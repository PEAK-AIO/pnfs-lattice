/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_apid.c -- Read-only HTTP/JSON metadata query service.
 *
 * Runs on a host that can reach the metadata backend and answers
 * find-style queries from hosts that cannot, so an NFS client can search
 * the namespace without an NFS mount and without any backend access.
 *
 * Strictly read-only: the only catalogue entry points reachable from a
 * request handler are the find_query scans and keyed reads.  There is no
 * route that mutates anything.
 *
 * Security model.  A bearer token gates every API route and is
 * compared in constant time.  The token is an ADMINISTRATIVE credential:
 * it can enumerate the entire namespace regardless of POSIX traversal
 * permissions, so it must be distributed accordingly.  Binding a
 * non-loopback address in cleartext is refused unless the operator opts
 * in explicitly, so an accidental --bind 0.0.0.0 fails fast instead of
 * serving the catalogue to the network.
 *
 * Concurrency: one listener, one request at a time.  Queries are scans;
 * serving them concurrently would multiply backend load rather than
 * reduce latency.  Put a proxy in front if concurrency is needed.
 */

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "mds_tls.h"
#include "find_query.h"
#include "apid_http.h"
#include "admin_util.h"

#define APID_DEFAULT_CONF   "/etc/pnfs-mds/mds.conf"
#define APID_DEFAULT_BIND   "127.0.0.1"
#define APID_DEFAULT_PORT   9810U

/** Largest request (request line + headers) we will read. */
#define APID_REQUEST_MAX    8192U
/** Hard ceiling on a response body, so a broad query cannot exhaust RAM. */
#define APID_BODY_MAX       ((size_t)32U * 1024U * 1024U)
/** Headroom reserved so the closing JSON suffix always fits. */
#define APID_TAIL_RESERVE   256U

/* Set from a signal handler; only ever assigned a constant. */
static volatile sig_atomic_t g_stop;

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

/* -----------------------------------------------------------------------
 * Connection abstraction (plain TCP or TLS)
 * ----------------------------------------------------------------------- */

struct conn {
    int                  fd;
    struct mds_tls_conn *tls;   /**< NULL for plaintext. */
};

static int conn_read(struct conn *c, void *buf, size_t len)
{
    if (c->tls != NULL) {
        return mds_tls_read(c->tls, buf, len);
    }
    return (int)recv(c->fd, buf, len, 0);
}

/** Write every byte or fail.  @return 0 on success, -1 on error. */
static int conn_write_all(struct conn *c, const char *buf, size_t len)
{
    while (len > 0U) {
        int w;

        if (c->tls != NULL) {
            w = mds_tls_write(c->tls, buf, len);
        } else {
            w = (int)send(c->fd, buf, len, MSG_NOSIGNAL);
        }
        if (w <= 0) {
            if (w < 0 && errno == EINTR) {
                continue;
            }
            return -1;
        }
        buf += (size_t)w;
        len -= (size_t)w;
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * Bounded growable response body
 * ----------------------------------------------------------------------- */

struct dynbuf {
    char  *data;
    size_t len;
    size_t cap;
    bool   overflow;    /**< hit APID_BODY_MAX; body is truncated. */
};

static void dynbuf_free(struct dynbuf *b)
{
    free(b->data);
    b->data = NULL;
    b->len = 0U;
    b->cap = 0U;
}

/** Ensure room for @p need more bytes plus a NUL. */
static bool dynbuf_reserve(struct dynbuf *b, size_t need)
{
    size_t want = b->len + need + 1U;
    size_t ncap;
    char  *nd;

    if (want <= b->cap) {
        return true;
    }
    if (want > APID_BODY_MAX) {
        b->overflow = true;
        return false;
    }
    ncap = (b->cap == 0U) ? 8192U : b->cap;
    while (ncap < want) {
        if (ncap > APID_BODY_MAX / 2U) {
            ncap = APID_BODY_MAX;
            break;
        }
        ncap *= 2U;
    }
    nd = realloc(b->data, ncap);
    if (nd == NULL) {
        b->overflow = true;
        return false;
    }
    b->data = nd;
    b->cap = ncap;
    return true;
}

static bool dynbuf_append(struct dynbuf *b, const char *s, size_t n)
{
    if (b->overflow || !dynbuf_reserve(b, n)) {
        return false;
    }
    memcpy(b->data + b->len, s, n);
    b->len += n;
    b->data[b->len] = '\0';
    return true;
}

/* -----------------------------------------------------------------------
 * HTTP helpers
 * ----------------------------------------------------------------------- */

static void send_simple(struct conn *c, const char *status,
                        const char *ctype, const char *body)
{
    char header[256];
    size_t blen = (body != NULL) ? strlen(body) : 0U;
    int n;

    n = snprintf(header, sizeof(header),
                 "HTTP/1.0 %s\r\n"
                 "Content-Type: %s\r\n"
                 "Content-Length: %zu\r\n"
                 "Cache-Control: no-store\r\n"
                 "Connection: close\r\n"
                 "\r\n",
                 status, ctype, blen);
    if (n < 0 || n >= (int)sizeof(header)) {
        return;
    }
    if (conn_write_all(c, header, (size_t)n) != 0) {
        return;
    }
    if (blen > 0U) {
        (void)conn_write_all(c, body, blen);
    }
}

/** Send a JSON error object with the matching status line. */
static void send_error(struct conn *c, const char *status, const char *msg)
{
    char body[256];
    char esc[192];

    if (json_escape_string(msg, esc, sizeof(esc)) < 0) {
        esc[0] = '\0';
    }
    (void)snprintf(body, sizeof(body), "{\"error\":\"%s\"}\n", esc);
    send_simple(c, status, "application/json", body);
}

/* -----------------------------------------------------------------------
 * /api/v1/find
 * ----------------------------------------------------------------------- */

struct find_emit_ctx {
    struct dynbuf *body;
    uint32_t       count;
    bool           truncated;   /**< stopped early to stay under the cap. */
};

static int api_emit(const struct find_result *r, void *arg)
{
    struct find_emit_ctx *ec = arg;
    char mode[FIND_MODE_STR_LEN];
    char esc[4 * (MDS_MAX_NAME + 1)];
    char row[1024];
    int  n;

    find_format_mode(r->mode, mode);
    if (json_escape_string(r->name, esc, sizeof(esc)) < 0) {
        esc[0] = '\0';
    }

    n = snprintf(row, sizeof(row),
                 "%s{\"fileid\":%" PRIu64 ",\"type\":\"%c\",\"mode\":\"%s\","
                 "\"nlink\":%u,\"uid\":%" PRIu64 ",\"gid\":%" PRIu64 ","
                 "\"size\":%" PRIu64 ",\"mtime_sec\":%" PRId64 ","
                 "\"ctime_sec\":%" PRId64 ",\"parent_fileid\":%" PRIu64 ","
                 "\"name\":\"%s\"}",
                 (ec->count == 0U) ? "" : ",",
                 r->fileid, find_type_char(r->type), mode, r->nlink,
                 r->uid, r->gid, r->size, r->mtime_sec, r->ctime_sec,
                 r->parent_fileid, esc);
    if (n < 0 || n >= (int)sizeof(row)) {
        return 0;   /* skip an unrepresentable row rather than fail */
    }
    /* Stop while there is still room for the closing suffix, so a
     * truncated result set is still valid JSON. */
    if (ec->body->len + (size_t)n + APID_TAIL_RESERVE > APID_BODY_MAX ||
        !dynbuf_append(ec->body, row, (size_t)n)) {
        ec->truncated = true;
        return 1;
    }
    ec->count++;
    return 0;
}

static void handle_find(struct conn *c, struct mds_catalogue *cat, char *qs)
{
    struct find_filter f;
    struct find_emit_ctx ec;
    struct dynbuf body;
    const char *why = "bad request";
    char header[256];
    char tail[64];
    enum mds_status st;
    int hn;

    memset(&f, 0, sizeof(f));
    memset(&ec, 0, sizeof(ec));
    memset(&body, 0, sizeof(body));

    if (!apid_parse_query(qs, &f, &why)) {
        send_error(c, "400 Bad Request", why);
        return;
    }

    ec.body = &body;
    if (!dynbuf_append(&body, "{\"results\":[", 12U)) {
        send_error(c, "500 Internal Server Error", "out of memory");
        dynbuf_free(&body);
        return;
    }

    st = find_query_run(cat, &f, api_emit, &ec);
    if (st != MDS_OK) {
        send_error(c, "500 Internal Server Error", mds_status_str(st));
        dynbuf_free(&body);
        return;
    }

    (void)snprintf(tail, sizeof(tail), "],\"count\":%u,\"truncated\":%s}\n",
                   ec.count, ec.truncated ? "true" : "false");
    if (!dynbuf_append(&body, tail, strlen(tail))) {
        send_error(c, "500 Internal Server Error", "response too large");
        dynbuf_free(&body);
        return;
    }

    hn = snprintf(header, sizeof(header),
                  "HTTP/1.0 200 OK\r\n"
                  "Content-Type: application/json\r\n"
                  "Content-Length: %zu\r\n"
                  "Cache-Control: no-store\r\n"
                  "Connection: close\r\n"
                  "\r\n", body.len);
    if (hn > 0 && hn < (int)sizeof(header) &&
        conn_write_all(c, header, (size_t)hn) == 0) {
        (void)conn_write_all(c, body.data, body.len);
    }
    dynbuf_free(&body);
}

/* -----------------------------------------------------------------------
 * Request dispatch
 * ----------------------------------------------------------------------- */

struct server {
    struct mds_catalogue *cat;
    char                  token[APID_TOKEN_MAX];
    bool                  auth_required;
};

static void handle_request(struct conn *c, struct server *srv)
{
    char  req[APID_REQUEST_MAX];
    char  tok[APID_TOKEN_MAX];
    size_t total = 0U;
    bool  have_token = false;
    enum apid_request_status rs;
    char *line_end;
    char *path = NULL;
    char *qs = NULL;

    /* Read until the header terminator or the cap. */
    for (;;) {
        int n = conn_read(c, req + total, sizeof(req) - total - 1U);

        if (n <= 0) {
            return;                     /* peer went away */
        }
        total += (size_t)n;
        req[total] = '\0';
        if (strstr(req, "\r\n\r\n") != NULL) {
            break;
        }
        if (total >= sizeof(req) - 1U) {
            send_error(c, "431 Request Header Fields Too Large",
                       "request headers too large");
            return;
        }
    }

    line_end = strstr(req, "\r\n");
    if (line_end == NULL) {
        send_error(c, "400 Bad Request", "malformed request line");
        return;
    }

    /* Read headers BEFORE splitting the request line: the split writes
     * NULs into req, which would hide the header block from any later
     * scan that starts at the beginning of the buffer. */
    if (srv->auth_required) {
        have_token = apid_extract_bearer(line_end, tok, sizeof(tok));
    }
    *line_end = '\0';

    rs = apid_split_request_line(req, &path, &qs);
    if (rs == APID_REQ_BAD_METHOD) {
        send_error(c, "405 Method Not Allowed", "only GET is supported");
        return;
    }
    if (rs != APID_REQ_OK) {
        send_error(c, "400 Bad Request", "malformed request line");
        return;
    }

    /* Liveness probes must work without a credential. */
    if (strcmp(path, "/healthz") == 0) {
        send_simple(c, "200 OK", "text/plain", "ok\n");
        return;
    }

    if (srv->auth_required &&
        (!have_token || !apid_secret_equal(tok, srv->token))) {
        send_error(c, "401 Unauthorized", "missing or invalid token");
        return;
    }

    if (strcmp(path, "/api/v1/find") == 0) {
        handle_find(c, srv->cat, qs);
        return;
    }
    send_error(c, "404 Not Found", "unknown endpoint");
}

/* -----------------------------------------------------------------------
 * Startup
 * ----------------------------------------------------------------------- */

static void usage(const char *prog, int rc)
{
    (void)fprintf(rc == 0 ? stdout : stderr,
        "mds-apid - read-only HTTP metadata query service.\n"
        "\n"
        "Usage:\n"
        "  %s [options]\n"
        "\n"
        "Options:\n"
        "  --bind ADDR           IPv4 address to listen on (default %s)\n"
        "  --port N              TCP port (default %u)\n"
        "  --config PATH         mds.conf path (default %s)\n"
        "  --token-file PATH     bearer token; required for /api/v1/*\n"
        "  --tls-cert PATH       server certificate (PEM)\n"
        "  --tls-key PATH        server private key (PEM)\n"
        "  --tls-ca PATH         CA bundle used to verify client certs\n"
        "  --require-client-cert require a client certificate (mutual TLS)\n"
        "  --insecure            allow a non-loopback bind without TLS\n"
        "  -h, --help            this help\n"
        "\n"
        "Endpoints:\n"
        "  GET /healthz          liveness probe (never requires a token)\n"
        "  GET /api/v1/find      search; see docs/find-api.md\n"
        "\n"
        "The token is an administrative credential: any holder can\n"
        "enumerate the whole namespace regardless of POSIX permissions.\n",
        prog, APID_DEFAULT_BIND, (unsigned)APID_DEFAULT_PORT,
        APID_DEFAULT_CONF);
    /* Option parsing runs before the accept loop, on the only thread. */
    exit(rc); /* NOLINT(concurrency-mt-unsafe) */
}

/**
 * Load a bearer token from a file.
 *
 * Refuses a world- or group-readable file: a token that anyone on the
 * host can read is not a credential.  Trailing newlines are stripped so
 * an ordinary `echo secret > file` works.
 *
 * @return 0 on success.
 */
static int load_token(const char *path, char *out, size_t cap)
{
    struct stat sb;
    FILE *fp;
    size_t n;
    char errbuf[128];

    if (stat(path, &sb) != 0) {
        (void)fprintf(stderr, "mds-apid: cannot stat %s: %s\n",
                      path, admin_errno_text(errno, errbuf, sizeof(errbuf)));
        return -1;
    }
    if ((sb.st_mode & (S_IRWXG | S_IRWXO)) != 0) {
        (void)fprintf(stderr,
            "mds-apid: refusing token file %s: mode %04o is accessible "
            "beyond its owner (chmod 600)\n",
            path, (unsigned)(sb.st_mode & 07777));
        return -1;
    }

    fp = fopen(path, "re");
    if (fp == NULL) {
        (void)fprintf(stderr, "mds-apid: cannot open %s: %s\n",
                      path, admin_errno_text(errno, errbuf, sizeof(errbuf)));
        return -1;
    }
    if (fgets(out, (int)cap, fp) == NULL) {
        (void)fprintf(stderr, "mds-apid: token file %s is empty\n", path);
        (void)fclose(fp);
        return -1;
    }
    (void)fclose(fp);

    n = strlen(out);
    while (n > 0U && (out[n - 1U] == '\n' || out[n - 1U] == '\r' ||
                      out[n - 1U] == ' '  || out[n - 1U] == '\t')) {
        out[--n] = '\0';
    }
    if (n == 0U) {
        (void)fprintf(stderr, "mds-apid: token file %s is empty\n", path);
        return -1;
    }
    return 0;
}

struct options {
    const char *bind_addr;
    const char *conf;
    const char *token_file;
    const char *tls_cert;
    const char *tls_key;
    const char *tls_ca;
    uint16_t    port;
    bool        require_client_cert;
    bool        insecure;
};

/* Flat argument dispatch; each branch is trivial.
 * NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void parse_opts(int argc, char **argv, struct options *o)
{
    int i;

    for (i = 1; i < argc; i++) {
        const char *a = argv[i];
        bool needs_value = (strcmp(a, "--bind") == 0 ||
                            strcmp(a, "--port") == 0 ||
                            strcmp(a, "--config") == 0 ||
                            strcmp(a, "--token-file") == 0 ||
                            strcmp(a, "--tls-cert") == 0 ||
                            strcmp(a, "--tls-key") == 0 ||
                            strcmp(a, "--tls-ca") == 0);

        if (needs_value && i + 1 >= argc) {
            (void)fprintf(stderr, "mds-apid: %s requires a value\n", a);
            usage(argv[0], 1);
        }

        if (strcmp(a, "-h") == 0 || strcmp(a, "--help") == 0) {
            usage(argv[0], 0);
        } else if (strcmp(a, "--bind") == 0) {
            o->bind_addr = argv[++i];
        } else if (strcmp(a, "--port") == 0) {
            uint64_t v = 0U;

            if (!find_parse_u64(argv[++i], &v) || v == 0U || v > 65535U) {
                (void)fprintf(stderr, "mds-apid: --port must be 1..65535\n");
                exit(1); /* NOLINT(concurrency-mt-unsafe) */
            }
            o->port = (uint16_t)v;
        } else if (strcmp(a, "--config") == 0) {
            o->conf = argv[++i];
        } else if (strcmp(a, "--token-file") == 0) {
            o->token_file = argv[++i];
        } else if (strcmp(a, "--tls-cert") == 0) {
            o->tls_cert = argv[++i];
        } else if (strcmp(a, "--tls-key") == 0) {
            o->tls_key = argv[++i];
        } else if (strcmp(a, "--tls-ca") == 0) {
            o->tls_ca = argv[++i];
        } else if (strcmp(a, "--require-client-cert") == 0) {
            o->require_client_cert = true;
        } else if (strcmp(a, "--insecure") == 0) {
            o->insecure = true;
        } else {
            (void)fprintf(stderr, "mds-apid: unknown option: %s\n", a);
            usage(argv[0], 1);
        }
    }
}

/** Validate option combinations that must fail before we bind. */
static int check_opts(const struct options *o, bool tls_enabled)
{
    if ((o->tls_cert != NULL) != (o->tls_key != NULL)) {
        (void)fprintf(stderr,
            "mds-apid: --tls-cert and --tls-key must be given together\n");
        return -1;
    }
    if (o->require_client_cert && o->tls_ca == NULL) {
        (void)fprintf(stderr,
            "mds-apid: --require-client-cert needs --tls-ca\n");
        return -1;
    }
    if (!apid_bind_allowed(o->bind_addr, tls_enabled, o->insecure)) {
        (void)fprintf(stderr,
            "mds-apid: refusing to serve %s in cleartext.\n"
            "  Enable TLS (--tls-cert/--tls-key), keep the bind on\n"
            "  loopback behind a TLS proxy, or pass --insecure if the\n"
            "  network is genuinely trusted.\n", o->bind_addr);
        return -1;
    }
    if (o->token_file == NULL) {
        (void)fprintf(stderr,
            "mds-apid: WARNING: no --token-file; /api/v1/* is unauthenticated\n");
    }
    return 0;
}

static int listen_on(const char *addr, uint16_t port)
{
    struct sockaddr_in sa;
    int fd;
    int one = 1;
    char errbuf[128];

    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    if (inet_pton(AF_INET, addr, &sa.sin_addr) != 1) {
        (void)fprintf(stderr,
            "mds-apid: --bind must be an IPv4 address, got '%s'\n", addr);
        return -1;
    }

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        (void)fprintf(stderr, "mds-apid: socket: %s\n",
                      admin_errno_text(errno, errbuf, sizeof(errbuf)));
        return -1;
    }
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
        (void)fprintf(stderr, "mds-apid: bind %s:%u: %s\n",
                      addr, (unsigned)port,
                      admin_errno_text(errno, errbuf, sizeof(errbuf)));
        close(fd);
        return -1;
    }
    if (listen(fd, 16) < 0) {
        (void)fprintf(stderr, "mds-apid: listen: %s\n",
                      admin_errno_text(errno, errbuf, sizeof(errbuf)));
        close(fd);
        return -1;
    }
    return fd;
}

int main(int argc, char **argv)
{
    struct options o;
    struct server srv;
    struct mds_config cfg;
    struct mds_tls_ctx *tls_ctx = NULL;
    struct sigaction sa;
    enum mds_status st;
    bool tls_enabled;
    int listen_fd;
    int rc = 0;
    char errbuf[128];

    memset(&o, 0, sizeof(o));
    memset(&srv, 0, sizeof(srv));
    o.bind_addr = APID_DEFAULT_BIND;
    o.conf = APID_DEFAULT_CONF;
    o.port = (uint16_t)APID_DEFAULT_PORT;

    parse_opts(argc, argv, &o);
    tls_enabled = (o.tls_cert != NULL && o.tls_key != NULL);
    if (check_opts(&o, tls_enabled) != 0) {
        return 1;
    }

    if (o.token_file != NULL) {
        if (load_token(o.token_file, srv.token, sizeof(srv.token)) != 0) {
            return 1;
        }
        srv.auth_required = true;
    }

    if (tls_enabled &&
        mds_tls_ctx_create(o.tls_ca, o.tls_cert, o.tls_key, true,
                           o.require_client_cert, &tls_ctx) != 0) {
        (void)fprintf(stderr, "mds-apid: TLS context setup failed\n");
        return 1;
    }

    st = mds_config_load(o.conf, &cfg);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "mds-apid: cannot load config %s: %s\n",
                      o.conf, mds_status_str(st));
        mds_tls_ctx_destroy(tls_ctx);
        return 1;
    }
    st = mds_catalogue_open(&cfg, &srv.cat);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "mds-apid: catalogue open failed: %s\n",
                      mds_status_str(st));
        mds_tls_ctx_destroy(tls_ctx);
        return 1;
    }

    listen_fd = listen_on(o.bind_addr, o.port);
    if (listen_fd < 0) {
        mds_catalogue_close(srv.cat);
        mds_tls_ctx_destroy(tls_ctx);
        return 1;
    }

    /* SIGPIPE would kill the process when a client hangs up mid-response;
     * writes report EPIPE instead.  SIGINT/SIGTERM stop the accept loop. */
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;
    (void)sigaction(SIGPIPE, &sa, NULL);
    sa.sa_handler = on_signal;
    (void)sigaction(SIGINT, &sa, NULL);
    (void)sigaction(SIGTERM, &sa, NULL);

    (void)fprintf(stderr,
        "mds-apid: listening on %s:%u tls=%s auth=%s\n",
        o.bind_addr, (unsigned)o.port,
        tls_enabled ? "on" : "off",
        srv.auth_required ? "token" : "none");

    while (g_stop == 0) {
        struct sockaddr_in cli;
        socklen_t clen = sizeof(cli);
        struct conn c;
        int fd = accept(listen_fd, (struct sockaddr *)&cli, &clen);

        if (fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            (void)fprintf(stderr, "mds-apid: accept: %s\n",
                          admin_errno_text(errno, errbuf, sizeof(errbuf)));
            rc = 1;
            break;
        }

        memset(&c, 0, sizeof(c));
        c.fd = fd;
        if (tls_ctx != NULL) {
            if (mds_tls_wrap(tls_ctx, fd, true, NULL, &c.tls) != 0) {
                /* Handshake failure (including a missing client cert
                 * under mTLS): drop before any request is parsed. */
                close(fd);
                continue;
            }
        }

        handle_request(&c, &srv);

        if (c.tls != NULL) {
            mds_tls_close(c.tls);
        }
        close(fd);
    }

    close(listen_fd);
    mds_catalogue_close(srv.cat);
    mds_tls_ctx_destroy(tls_ctx);
    return rc;
}

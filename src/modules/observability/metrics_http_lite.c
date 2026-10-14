/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * metrics_http_lite.c -- Minimal community HTTP /metrics endpoint.
 *
 * Replaces metrics_http_stub.c so community builds can actually
 * scrape Prometheus metrics without the enterprise observability
 * module.  Design goals:
 *
 *   - Single listener thread, sequential request handling.
 *     Prometheus scrapes once every 5-15 seconds; concurrency on
 *     the scrape path adds no value and a pile of complexity.
 *
 *   - HTTP/1.0 with explicit Connection: close so we never have
 *     to manage keep-alive state.
 *
 *   - Accept any path -- /metrics, /, /healthz, etc. all return
 *     the Prometheus body.  Saves clients (and operators
 *     curl'ing for sanity checks) from path mistakes.
 *
 *   - Hard 256 KiB response cap.  With ~140 histograms x ~14
 *     bucket lines each, the real expected size is ~30 KiB.  The
 *     cap exists to bound stack/heap pressure if observability is
 *     accidentally turned up.
 *
 *   - The request is read to the end of its headers BEFORE the
 *     response is written, and the connection is half-closed and
 *     drained to the peer's EOF before close().  Bytes left unread
 *     at close() make the kernel answer with RST and discard the
 *     unsent tail of the response; that is how a scrape whose GET
 *     arrived a moment after accept() used to receive ~13 KiB of a
 *     ~100 KiB body.  Every blocking call on a connection is bounded
 *     in bytes and time (see the METRICS_HTTP_* limits below).
 *
 *   - Shutdown is initiated by closing the listen socket from
 *     metrics_http_stop(); the accept() in the worker returns
 *     EBADF and the loop exits.
 *
 * Operators who want the endpoint OFF set `metrics_http_port = 0`
 * in mds.conf -- main.c skips metrics_http_start() entirely in
 * that case.
 */

#include "metrics_http.h"
#include "mds_metrics.h"
#include "mds_catalogue.h"
#include "catalog_stats.h"
#include "health.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#define METRICS_HTTP_BODY_CAP ((size_t)256 * 1024)

/*
 * Bounds on one client conversation.  The accept loop is sequential
 * (see the file comment), so these are also the bound on how long one
 * slow or silent client can delay every other scraper queued in the
 * listen backlog.
 *
 *   METRICS_HTTP_REQUEST_CAP: a scrape request is a few hundred bytes
 *     of request line + headers.  A client that has sent this much
 *     without a blank line is not speaking HTTP we want to serve and
 *     is dropped without a response.  The same cap bounds the bytes
 *     discarded while waiting for the peer's EOF after the response.
 *   METRICS_HTTP_IO_TIMEOUT_SEC: SO_RCVTIMEO on the accepted socket,
 *     so each blocking recv() -- request read, post-response drain --
 *     returns after this long without data.  A client that connects
 *     and never sends anything is dropped after one such timeout.
 *   METRICS_HTTP_SEND_TIMEOUT_SEC: SO_SNDTIMEO on the accepted socket.
 *     It only fires when the peer has stopped reading with its receive
 *     window full, so a scraper on a slow link is unaffected unless
 *     it stalls for this long.
 */
#define METRICS_HTTP_REQUEST_CAP      ((size_t)8 * 1024)
#define METRICS_HTTP_IO_TIMEOUT_SEC   1
#define METRICS_HTTP_SEND_TIMEOUT_SEC 5

struct metrics_http_ctx {
    int                   listen_fd;
    pthread_t             thread;
    _Atomic bool          shutdown;
    struct mds_catalogue *cat;
    uint16_t              port;
};

/* Thread-safe strerror(3) for the diagnostics below.  The build
 * defines _GNU_SOURCE, so this is the GNU strerror_r(): it returns
 * the message text, which may live in @buf or in a static string. */
static const char *errno_text(int err, char *buf, size_t cap)
{
    return strerror_r(err, buf, cap);
}

/* Append the backend client-side counters (round trips, transactions,
 * bytes) as Prometheus lines after the v2 body.  Best-effort: when the
 * backend has no instrumentation (NOSUPPORT) or the lines would not
 * fit, the base body is served unchanged -- a scrape must never fail
 * because the optional NDB block did not fit. */
static int append_backend_client_stats(struct mds_catalogue *cat,
                                       char *out, size_t cap, int used)
{
    struct mds_cat_backend_client_stats bs;
    int n;

    if (cat == NULL || used < 0 || (size_t)used >= cap) {
        return used;
    }
    if (mds_cat_backend_client_stats(cat, &bs) != MDS_OK) {
        return used;
    }

    n = snprintf(out + used, cap - (size_t)used,
        "# HELP pnfs_mds_ndb_exec_roundtrips_total Times a request thread "
        "blocked on an NDB execute round trip.\n"
        "# TYPE pnfs_mds_ndb_exec_roundtrips_total counter\n"
        "pnfs_mds_ndb_exec_roundtrips_total %llu\n"
        "# HELP pnfs_mds_ndb_scan_batch_waits_total Waits for the next NDB scan result batch.\n"
        "# TYPE pnfs_mds_ndb_scan_batch_waits_total counter\n"
        "pnfs_mds_ndb_scan_batch_waits_total %llu\n"
        "# HELP pnfs_mds_ndb_meta_waits_total Waits for NDB dictionary/meta operations.\n"
        "# TYPE pnfs_mds_ndb_meta_waits_total counter\n"
        "pnfs_mds_ndb_meta_waits_total %llu\n"
        "# HELP pnfs_mds_ndb_wait_nanoseconds_total Nanoseconds spent blocked on NDB responses.\n"
        "# TYPE pnfs_mds_ndb_wait_nanoseconds_total counter\n"
        "pnfs_mds_ndb_wait_nanoseconds_total %llu\n"
        "# HELP pnfs_mds_ndb_txn_started_total NDB transactions started.\n"
        "# TYPE pnfs_mds_ndb_txn_started_total counter\n"
        "pnfs_mds_ndb_txn_started_total %llu\n"
        "# HELP pnfs_mds_ndb_txn_committed_total NDB transactions committed.\n"
        "# TYPE pnfs_mds_ndb_txn_committed_total counter\n"
        "pnfs_mds_ndb_txn_committed_total %llu\n"
        "# HELP pnfs_mds_ndb_txn_aborted_total NDB transactions aborted.\n"
        "# TYPE pnfs_mds_ndb_txn_aborted_total counter\n"
        "pnfs_mds_ndb_txn_aborted_total %llu\n"
        "# HELP pnfs_mds_ndb_bytes_sent_total Bytes sent to NDB data nodes.\n"
        "# TYPE pnfs_mds_ndb_bytes_sent_total counter\n"
        "pnfs_mds_ndb_bytes_sent_total %llu\n"
        "# HELP pnfs_mds_ndb_bytes_received_total Bytes received from NDB data nodes.\n"
        "# TYPE pnfs_mds_ndb_bytes_received_total counter\n"
        "pnfs_mds_ndb_bytes_received_total %llu\n"
        "# HELP pnfs_mds_ndb_pk_ops_total NDB primary-key operations.\n"
        "# TYPE pnfs_mds_ndb_pk_ops_total counter\n"
        "pnfs_mds_ndb_pk_ops_total %llu\n"
        "# HELP pnfs_mds_ndb_range_scans_total NDB ordered-index range scans.\n"
        "# TYPE pnfs_mds_ndb_range_scans_total counter\n"
        "pnfs_mds_ndb_range_scans_total %llu\n"
        "# HELP pnfs_mds_ndb_read_rows_total Rows returned by NDB to this MDS.\n"
        "# TYPE pnfs_mds_ndb_read_rows_total counter\n"
        "pnfs_mds_ndb_read_rows_total %llu\n"
        "# HELP pnfs_mds_ndb_client_objects Ndb client objects aggregated into these counters.\n"
        "# TYPE pnfs_mds_ndb_client_objects gauge\n"
        "pnfs_mds_ndb_client_objects %llu\n",
        (unsigned long long)bs.exec_waits,
        (unsigned long long)bs.scan_waits,
        (unsigned long long)bs.meta_waits,
        (unsigned long long)bs.wait_nanos,
        (unsigned long long)bs.txn_started,
        (unsigned long long)bs.txn_committed,
        (unsigned long long)bs.txn_aborted,
        (unsigned long long)bs.bytes_sent,
        (unsigned long long)bs.bytes_recvd,
        (unsigned long long)bs.pk_ops,
        (unsigned long long)bs.range_scans,
        (unsigned long long)bs.read_rows,
        (unsigned long long)bs.client_objects);
    if (n < 0 || (size_t)n >= cap - (size_t)used) {
        /* Does not fit: serve the base body unchanged. */
        out[used] = '\0';
        return used;
    }
    return used + n;
}

/* Build a metrics snapshot and feed the v2 renderer into `out`.
 * Returns the number of bytes written (excluding NUL) or -1 on
 * truncation.  Buffer cap must be > 0. */
static int render_metrics_body(struct mds_catalogue *cat,
                               char *out, size_t cap)
{
    struct mds_metrics_snapshot snap = mds_metrics_snapshot();
    int n;

    if (cat != NULL) {
        const struct catalog_stats *cs = mds_catalogue_stats(cat);
        if (cs != NULL) {
            mds_metrics_snapshot_fill_catalog(&snap, cs);
        }
    }

    n = mds_metrics_prometheus_v2(&snap, &g_branch_metrics, out, cap);
    if (n >= 0) {
        n = append_backend_client_stats(cat, out, cap, n);
    }
    return n;
}

/* Send all bytes of `buf` (n bytes) to `fd`, resuming partial sends.
 * MSG_NOSIGNAL so a peer that hangs up mid-body yields EPIPE here
 * instead of a SIGPIPE that would take the daemon down when it is not
 * run under a service manager that ignores the signal; SO_SNDTIMEO
 * (set_socket_timeouts) bounds a peer that stops reading.  Returns 0
 * on success, -1 on any error including timeout and reset. */
static int write_all(int fd, const char *buf, size_t n)
{
    while (n > 0) {
        ssize_t w = send(fd, buf, n, MSG_NOSIGNAL);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        buf += w;
        n   -= (size_t)w;
    }
    return 0;
}

/* Bound every blocking call on the accepted socket (see the
 * METRICS_HTTP_*_TIMEOUT_SEC rationale).  Returns -1 when the kernel
 * refuses; the caller must then not block on the socket at all. */
static int set_socket_timeouts(int fd)
{
    struct timeval rcv = { .tv_sec = METRICS_HTTP_IO_TIMEOUT_SEC,
                           .tv_usec = 0 };
    struct timeval snd = { .tv_sec = METRICS_HTTP_SEND_TIMEOUT_SEC,
                           .tv_usec = 0 };

    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rcv, sizeof(rcv)) < 0 ||
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &snd, sizeof(snd)) < 0) {
        return -1;
    }
    return 0;
}

/* Read the request line + headers up to and including the first blank
 * line.  Nothing is parsed -- any path returns the metrics body -- but
 * the whole request MUST be consumed before the response is written:
 * bytes still unread when the socket is closed make the kernel answer
 * with RST and discard the unsent tail of the response.  A single
 * non-blocking recv() right after accept() used to miss a GET that
 * was still in flight, which is exactly that case.
 *
 * Returns 0 once the blank line has been seen; -1 when the peer closed
 * first, when SO_RCVTIMEO expired (EAGAIN), on a hard error, or when
 * METRICS_HTTP_REQUEST_CAP bytes arrived without a blank line.  In the
 * -1 cases the caller sends no response. */
static int read_request(int fd)
{
    char   buf[METRICS_HTTP_REQUEST_CAP];
    size_t used = 0;

    while (used < sizeof(buf)) {
        ssize_t n = recv(fd, buf + used, sizeof(buf) - used, 0);

        if (n == 0) {
            return -1;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        used += (size_t)n;
        if (memmem(buf, used, "\r\n\r\n", 4) != NULL) {
            return 0;
        }
    }
    return -1;
}

/* Half-close after the response and wait for the peer's EOF so that
 * close() never finds unread bytes (a pipelined second request, say)
 * and therefore never turns into RST.  Bounded by SO_RCVTIMEO per
 * recv() and by METRICS_HTTP_REQUEST_CAP discarded bytes in total.
 * On the timeout the receive queue is empty, so the caller's close()
 * is still a clean FIN and the kernel keeps delivering any unsent
 * response bytes in the background; on the byte cap the peer is
 * flooding a connection it should have closed and an RST is the
 * right answer. */
static void finish_connection(int fd)
{
    char   scratch[512];
    size_t discarded = 0;

    if (shutdown(fd, SHUT_WR) < 0) {
        return;
    }
    while (discarded < METRICS_HTTP_REQUEST_CAP) {
        ssize_t n = recv(fd, scratch, sizeof(scratch), 0);

        if (n == 0) {
            return;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return;
        }
        discarded += (size_t)n;
    }
}

/* Status-only reply for the two failure paths (no body). */
static void send_error_response(int fd, const char *status_line)
{
    char header[128];
    int  n = snprintf(header, sizeof(header),
        "HTTP/1.0 %s\r\n"
        "Content-Length: 0\r\n"
        "Connection: close\r\n\r\n", status_line);

    if (n > 0 && n < (int)sizeof(header)) {
        (void)write_all(fd, header, (size_t)n);
    }
}

static void handle_connection(int conn_fd, struct mds_catalogue *cat)
{
    char  *body;
    int    body_len;
    char   header[256];
    int    header_len;
    char   errbuf[128];

    if (set_socket_timeouts(conn_fd) < 0) {
        /* Without the timeouts nothing below is bounded; drop the
         * connection rather than risk wedging the listener. */
        (void)fprintf(stderr,
            "WARN: metrics_http: setsockopt(SO_RCVTIMEO/SO_SNDTIMEO) "
            "failed: %s; dropping connection\n",
            errno_text(errno, errbuf, sizeof(errbuf)));
        return;
    }

    /* The request is consumed in full before a single byte is written
     * (see read_request).  No complete request within the bounds means
     * no response: the peer simply sees the connection close. */
    if (read_request(conn_fd) < 0) {
        return;
    }

    body = malloc(METRICS_HTTP_BODY_CAP);
    if (body == NULL) {
        send_error_response(conn_fd, "500 Internal Server Error");
        finish_connection(conn_fd);
        return;
    }

    body_len = render_metrics_body(cat, body, METRICS_HTTP_BODY_CAP);
    if (body_len < 0) {
        /* Truncated: we have no usable length, so report 503
         * rather than serve a body that stops mid-line. */
        send_error_response(conn_fd, "503 Service Unavailable");
        free(body);
        finish_connection(conn_fd);
        return;
    }

    header_len = snprintf(header, sizeof(header),
        "HTTP/1.0 200 OK\r\n"
        "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
        "Content-Length: %d\r\n"
        "Connection: close\r\n"
        "\r\n", body_len);
    if (header_len < 0 || header_len >= (int)sizeof(header)) {
        free(body);
        return;
    }
    if (write_all(conn_fd, header, (size_t)header_len) == 0) {
        (void)write_all(conn_fd, body, (size_t)body_len);
    }
    free(body);
    finish_connection(conn_fd);
}

/* Sequential by design (file comment): while one connection is being
 * served, further scrapers wait in the listen backlog.  Each connection
 * holds this thread for a bounded time -- at most
 * METRICS_HTTP_IO_TIMEOUT_SEC to receive the request, the render, the
 * sends (each bounded by METRICS_HTTP_SEND_TIMEOUT_SEC only if the peer
 * stops reading) and at most METRICS_HTTP_IO_TIMEOUT_SEC waiting for
 * the peer's EOF -- so a silent or stalled client delays the others by
 * that bound, never indefinitely. */
static void *accept_loop(void *arg)
{
    struct metrics_http_ctx *ctx = arg;

    for (;;) {
        struct sockaddr_in cli;
        socklen_t          clen = sizeof(cli);
        int                conn_fd;

        if (atomic_load_explicit(&ctx->shutdown,
                                 memory_order_acquire)) {
            return NULL;
        }

        conn_fd = accept(ctx->listen_fd, (struct sockaddr *)&cli, &clen);
        if (conn_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            /* listen_fd closed (shutdown) or fatal error. */
            return NULL;
        }

        handle_connection(conn_fd, ctx->cat);
        close(conn_fd);
    }
}

int metrics_http_start(uint16_t port, struct mds_catalogue *cat,
                       struct metrics_http_ctx **out)
{
    struct metrics_http_ctx *ctx;
    struct sockaddr_in       addr;
    int                      one = 1;
    char                     errbuf[128];

    if (out == NULL) {
        return -1;
    }
    *out = NULL;

    if (port == 0) {
        return 0; /* operator opt-out */
    }

    ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        return -1;
    }
    ctx->cat  = cat;
    ctx->port = port;
    atomic_init(&ctx->shutdown, false);

    ctx->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ctx->listen_fd < 0) {
        (void)fprintf(stderr,
            "WARN: metrics_http: socket() failed: %s\n",
            errno_text(errno, errbuf, sizeof(errbuf)));
        free(ctx);
        return -1;
    }

    (void)setsockopt(ctx->listen_fd, SOL_SOCKET, SO_REUSEADDR,
                     &one, sizeof(one));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(port);

    if (bind(ctx->listen_fd, (struct sockaddr *)&addr,
             sizeof(addr)) < 0) {
        (void)fprintf(stderr,
            "WARN: metrics_http: bind(:%u) failed: %s\n",
            (unsigned)port, errno_text(errno, errbuf, sizeof(errbuf)));
        close(ctx->listen_fd);
        free(ctx);
        return -1;
    }

    if (listen(ctx->listen_fd, 8) < 0) {
        (void)fprintf(stderr,
            "WARN: metrics_http: listen() failed: %s\n",
            errno_text(errno, errbuf, sizeof(errbuf)));
        close(ctx->listen_fd);
        free(ctx);
        return -1;
    }

    if (pthread_create(&ctx->thread, NULL, accept_loop, ctx) != 0) {
        (void)fprintf(stderr,
            "WARN: metrics_http: pthread_create() failed\n");
        close(ctx->listen_fd);
        free(ctx);
        return -1;
    }

    (void)fprintf(stderr,
        "INFO: metrics_http: listening on 0.0.0.0:%u "
        "(GET /metrics)\n", (unsigned)port);

    *out = ctx;
    return 0;
}

void metrics_http_stop(struct metrics_http_ctx *ctx)
{
    if (ctx == NULL) {
        return;
    }

    atomic_store_explicit(&ctx->shutdown, true, memory_order_release);

    /* Wake the accept thread with shutdown(): a blocked accept() on a
     * shut-down listening socket returns an error, and the descriptor
     * number stays allocated, so no other thread can reuse it while
     * accept_loop may still pass it to a syscall.  The accept thread
     * reads listen_fd on every iteration; close() and the -1 store
     * therefore happen only after the join, when this thread is the
     * sole owner (same ordering as cluster_transport_server_stop). */
    if (ctx->listen_fd >= 0) {
        (void)shutdown(ctx->listen_fd, SHUT_RDWR);
    }
    (void)pthread_join(ctx->thread, NULL);
    if (ctx->listen_fd >= 0) {
        close(ctx->listen_fd);
        ctx->listen_fd = -1;
    }
    free(ctx);
}

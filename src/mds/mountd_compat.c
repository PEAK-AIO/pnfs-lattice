/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mountd_compat.c -- Compatibility responder for `showmount -e`.
 *
 * Implements ONC-RPC program 100005 (mountd) version 3 over UDP and
 * TCP, restricted to procedures NULL, EXPORT and DUMP.  Every other
 * procedure -- including MNT (proc 1) -- is rejected with PROC_UNAVAIL
 * at the RPC layer, which makes accidental NFSv3 mounts of the MDS
 * impossible by construction.
 *
 * The dispatch path is implemented as a pure function
 * (mountd_compat_handle_packet) so it can be unit-tested without
 * sockets.  The thread loop here only owns I/O: bind, accept,
 * record-marker reassembly, and writing replies.
 *
 * No allocations occur on the request path; all buffers are
 * stack-resident or owned by the per-connection state.  All input
 * length fields are bounds-checked before any data is read.
 *
 * See docs/mountd-compat.md for the full design and threat model.
 */

#include "mountd_compat.h"
#include "pnfs_mds.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>

/* -----------------------------------------------------------------------
 * RFC 5531 (ONC-RPC) constants
 * ----------------------------------------------------------------------- */

#define RPC_VERSION         2U
#define RPC_CALL            0U
#define RPC_REPLY           1U
#define MSG_ACCEPTED        0U
#define MSG_DENIED          1U

#define ACCEPT_SUCCESS        0U
#define ACCEPT_PROG_UNAVAIL   1U
#define ACCEPT_PROG_MISMATCH  2U
#define ACCEPT_PROC_UNAVAIL   3U
#define ACCEPT_GARBAGE_ARGS   4U
#define ACCEPT_SYSTEM_ERR     5U

#define REJECT_RPC_MISMATCH   0U
#define REJECT_AUTH_ERROR     1U

#define RPC_AUTH_NONE         0U
#define RPC_AUTH_UNIX         1U

/* RFC 5531 S8.2: rejected_reply.auth_stat values we actually emit. */
#define RPC_AUTH_TOOWEAK      5U

/* -----------------------------------------------------------------------
 * RFC 1813 Appendix I (mountd v3) constants
 * ----------------------------------------------------------------------- */

#define MOUNTD_PROG          100005U
#define MOUNTD_VERS3         3U

#define MOUNTPROC3_NULL      0U
#define MOUNTPROC3_MNT       1U
#define MOUNTPROC3_DUMP      2U
#define MOUNTPROC3_UMNT      3U
#define MOUNTPROC3_UMNTALL   4U
#define MOUNTPROC3_EXPORT    5U

/* RFC 1833 (rpcbind / portmap v2) -- used for self-registration. */
#define PMAP_PROG            100000U
#define PMAP_VERS2           2U
#define PMAPPROC_SET         1U
#define PMAPPROC_UNSET       2U
#define PMAP_PORT            111U
#define PMAP_IPPROTO_TCP     6U
#define PMAP_IPPROTO_UDP     17U

/* -----------------------------------------------------------------------
 * Local limits
 * ----------------------------------------------------------------------- */

#define MOUNTD_MAX_PKT          8192U
#define MOUNTD_MAX_TCP_CONNS    16
#define MOUNTD_TCP_IDLE_SEC     30
#define MOUNTD_AUTH_BODY_MAX    400U   /* RFC 5531 S8.1: opaque_auth body capped at 400 */
#define MOUNTD_RECV_TIMEOUT_MS  500    /* epoll wait quantum so we observe shutdown */

/* -----------------------------------------------------------------------
 * XDR primitive helpers -- big-endian on the wire.
 * ----------------------------------------------------------------------- */

static inline uint32_t mc_get_u32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] <<  8) |  (uint32_t)p[3];
}

static inline void mc_put_u32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >>  8);
    p[3] = (uint8_t) v;
}

/**
 * Emit an XDR opaque<>: 4-byte length followed by length bytes
 * padded with zeros to a 4-byte boundary.
 *
 * @return number of bytes written, or 0 if the encoded form would
 *         exceed @a cap or @a s is implausibly long.
 */
static size_t mc_put_xdr_string(uint8_t *out, size_t cap, const char *s)
{
    size_t len = strlen(s);
    if (len > MOUNTD_COMPAT_PATH_MAX) {
        return 0;
    }
    /* Pad to next multiple of 4. */
    size_t padded = (len + 3U) & ~(size_t)3U;
    if (4U + padded > cap) {
        return 0;
    }
    mc_put_u32(out, (uint32_t)len);
    if (len > 0) {
        memcpy(out + 4, s, len);
    }
    if (padded > len) {
        memset(out + 4 + len, 0, padded - len);
    }
    return 4U + padded;
}

/* -----------------------------------------------------------------------
 * Reply helpers -- every helper writes to a caller-provided buffer
 * and returns the number of bytes written, or 0 if @a cap is too
 * small.  None of them allocate.
 * ----------------------------------------------------------------------- */

/**
 * Emit a `MSG_ACCEPTED` reply header with the supplied accept_stat.
 * The verifier is AUTH_NONE / length 0, matching what every Linux
 * mountd implementation does.  Total: 24 bytes.
 */
static size_t mc_emit_accepted(uint8_t *out, size_t cap,
                               uint32_t xid, uint32_t accept_stat)
{
    if (cap < 24) {
        return 0;
    }
    mc_put_u32(out +  0, xid);
    mc_put_u32(out +  4, RPC_REPLY);
    mc_put_u32(out +  8, MSG_ACCEPTED);
    mc_put_u32(out + 12, RPC_AUTH_NONE);   /* verf flavor */
    mc_put_u32(out + 16, 0);               /* verf len    */
    mc_put_u32(out + 20, accept_stat);
    return 24;
}

/** Emit a `PROG_MISMATCH` reply (24 + 8 = 32 bytes). */
static size_t mc_emit_prog_mismatch(uint8_t *out, size_t cap,
                                    uint32_t xid, uint32_t low, uint32_t high)
{
    size_t off = mc_emit_accepted(out, cap, xid, ACCEPT_PROG_MISMATCH);
    if (off == 0 || off + 8 > cap) {
        return 0;
    }
    mc_put_u32(out + off, low);
    mc_put_u32(out + off + 4, high);
    return off + 8;
}

/** Emit an `RPC_MISMATCH`-rejected reply (24 bytes). */
static size_t mc_emit_rpc_mismatch(uint8_t *out, size_t cap, uint32_t xid)
{
    if (cap < 24) {
        return 0;
    }
    mc_put_u32(out +  0, xid);
    mc_put_u32(out +  4, RPC_REPLY);
    mc_put_u32(out +  8, MSG_DENIED);
    mc_put_u32(out + 12, REJECT_RPC_MISMATCH);
    mc_put_u32(out + 16, RPC_VERSION);   /* low  */
    mc_put_u32(out + 20, RPC_VERSION);   /* high */
    return 24;
}

/** Emit an `AUTH_ERROR`-rejected reply (20 bytes). */
static size_t mc_emit_auth_error(uint8_t *out, size_t cap,
                                 uint32_t xid, uint32_t auth_stat)
{
    if (cap < 20) {
        return 0;
    }
    mc_put_u32(out +  0, xid);
    mc_put_u32(out +  4, RPC_REPLY);
    mc_put_u32(out +  8, MSG_DENIED);
    mc_put_u32(out + 12, REJECT_AUTH_ERROR);
    mc_put_u32(out + 16, auth_stat);
    return 20;
}

/**
 * Emit the EXPORT result: a list of `exportnode { dirpath, groups }`,
 * with `groups` always set to the empty list (= world-readable).
 *
 * XDR linked-list encoding (RFC 4506 S4.19): each element is preceded
 * by a 4-byte boolean "value-follows".  TRUE = an element follows;
 * FALSE = end-of-list.
 *
 * @return bytes written into @a out, or 0 on overflow.
 */
static size_t mc_emit_exportlist(uint8_t *out, size_t cap,
                                 const struct mountd_compat_exports *exports)
{
    size_t off = 0;
    for (uint32_t i = 0; i < exports->count; i++) {
        if (off + 4 > cap) {
            return 0;
        }
        mc_put_u32(out + off, 1U);      /* value_follows = TRUE */
        off += 4;

        size_t s = mc_put_xdr_string(out + off, cap - off, exports->paths[i]);
        if (s == 0) {
            return 0;
        }
        off += s;

        /* groups: empty list -> single FALSE marker. */
        if (off + 4 > cap) {
            return 0;
        }
        mc_put_u32(out + off, 0U);
        off += 4;
    }
    if (off + 4 > cap) {
        return 0;
    }
    mc_put_u32(out + off, 0U);          /* end-of-list */
    return off + 4;
}

/**
 * Emit the DUMP result: an empty mountlist.  Always one 4-byte
 * `value_follows = FALSE`.
 */
static size_t mc_emit_empty_dump(uint8_t *out, size_t cap)
{
    if (cap < 4) {
        return 0;
    }
    mc_put_u32(out, 0U);
    return 4;
}

/* -----------------------------------------------------------------------
 * Call header parser
 *
 * Returns 0 on success and populates the out parameters with the
 * call's metadata.  Returns -1 on hopelessly truncated input (no
 * XID extractable -- drop), or -2 on RPC version mismatch (caller
 * should send an RPC_MISMATCH rejection).
 * ----------------------------------------------------------------------- */

static int mc_parse_call_header(const uint8_t *in, size_t in_len,
                                uint32_t *xid_out, uint32_t *prog_out,
                                uint32_t *vers_out, uint32_t *proc_out,
                                uint32_t *auth_flavor_out,
                                size_t   *args_off_out)
{
    /* Minimum fixed header: 10 * 4 = 40 bytes (xid, mtype, rpcvers,
     * prog, vers, proc, cred_flavor, cred_len, verf_flavor, verf_len). */
    if (in_len < 40U) {
        return -1;
    }

    uint32_t xid     = mc_get_u32(in +  0);
    uint32_t mtype   = mc_get_u32(in +  4);
    uint32_t rpcvers = mc_get_u32(in +  8);

    *xid_out = xid;

    if (mtype != RPC_CALL) {
        return -1;                              /* not a call: drop */
    }
    if (rpcvers != RPC_VERSION) {
        return -2;                              /* RPC mismatch */
    }

    *prog_out        = mc_get_u32(in + 12);
    *vers_out        = mc_get_u32(in + 16);
    *proc_out        = mc_get_u32(in + 20);
    *auth_flavor_out = mc_get_u32(in + 24);
    uint32_t cred_len = mc_get_u32(in + 28);

    if (cred_len > MOUNTD_AUTH_BODY_MAX) {
        return -1;
    }
    /* All XDR opaques are padded to a 4-byte boundary. */
    size_t cred_padded = (cred_len + 3U) & ~(size_t)3U;
    size_t off = 32U + cred_padded;
    if (off + 8U > in_len) {
        return -1;
    }

    /* uint32_t verf_flavor = mc_get_u32(in + off); */
    uint32_t verf_len = mc_get_u32(in + off + 4);
    if (verf_len > MOUNTD_AUTH_BODY_MAX) {
        return -1;
    }
    size_t verf_padded = (verf_len + 3U) & ~(size_t)3U;
    off += 8U + verf_padded;
    if (off > in_len) {
        return -1;
    }

    *args_off_out = off;
    return 0;
}

/* -----------------------------------------------------------------------
 * Public dispatch entry point.
 * ----------------------------------------------------------------------- */

int mountd_compat_handle_packet(const struct mountd_compat_exports *exports,
                                const uint8_t *in, size_t in_len,
                                uint8_t *out, size_t out_cap,
                                size_t *out_len)
{
    if (exports == NULL || in == NULL || out == NULL || out_len == NULL) {
        return -1;
    }
    if (in_len == 0U || in_len > MOUNTD_MAX_PKT) {
        return -1;
    }
    if (out_cap < 32U) {
        return -1;
    }

    uint32_t xid = 0, prog = 0, vers = 0, proc = 0, auth_flavor = 0;
    size_t args_off = 0;

    int hr = mc_parse_call_header(in, in_len,
                                  &xid, &prog, &vers, &proc,
                                  &auth_flavor, &args_off);
    if (hr == -2) {
        size_t n = mc_emit_rpc_mismatch(out, out_cap, xid);
        if (n == 0) {
            return -1;
        }
        *out_len = n;
        return 0;
    }
    if (hr != 0) {
        return -1;                              /* drop */
    }

    /* RFC 5531 S8: AUTH_NONE always permitted; AUTH_SYS (== AUTH_UNIX)
     * is permitted with no validation because this responder is
     * read-only and answers public discovery info.  Everything else
     * is rejected with AUTH_TOOWEAK so well-behaved clients can fall
     * back. */
    if (auth_flavor != RPC_AUTH_NONE && auth_flavor != RPC_AUTH_UNIX) {
        size_t n = mc_emit_auth_error(out, out_cap, xid, RPC_AUTH_TOOWEAK);
        if (n == 0) {
            return -1;
        }
        *out_len = n;
        return 0;
    }

    if (prog != MOUNTD_PROG) {
        size_t n = mc_emit_accepted(out, out_cap, xid, ACCEPT_PROG_UNAVAIL);
        if (n == 0) {
            return -1;
        }
        *out_len = n;
        return 0;
    }
    if (vers != MOUNTD_VERS3) {
        size_t n = mc_emit_prog_mismatch(out, out_cap, xid,
                                         MOUNTD_VERS3, MOUNTD_VERS3);
        if (n == 0) {
            return -1;
        }
        *out_len = n;
        return 0;
    }

    /* All MOUNT3 procedures except NULL, EXPORT and DUMP take args
     * we do not consume.  We do not even peek at args_off; rejecting
     * with PROC_UNAVAIL avoids any temptation to parse them. */
    (void)args_off;

    switch (proc) {
    case MOUNTPROC3_NULL: {
        size_t n = mc_emit_accepted(out, out_cap, xid, ACCEPT_SUCCESS);
        if (n == 0) {
            return -1;
        }
        *out_len = n;
        return 0;
    }
    case MOUNTPROC3_EXPORT: {
        size_t n = mc_emit_accepted(out, out_cap, xid, ACCEPT_SUCCESS);
        if (n == 0) {
            return -1;
        }
        size_t body = mc_emit_exportlist(out + n, out_cap - n, exports);
        if (body == 0) {
            return -1;
        }
        *out_len = n + body;
        return 0;
    }
    case MOUNTPROC3_DUMP: {
        size_t n = mc_emit_accepted(out, out_cap, xid, ACCEPT_SUCCESS);
        if (n == 0) {
            return -1;
        }
        size_t body = mc_emit_empty_dump(out + n, out_cap - n);
        if (body == 0) {
            return -1;
        }
        *out_len = n + body;
        return 0;
    }
    /* MNT, UMNT, UMNTALL, anything else -> PROC_UNAVAIL. */
    case MOUNTPROC3_MNT:
    case MOUNTPROC3_UMNT:
    case MOUNTPROC3_UMNTALL:
    default: {
        size_t n = mc_emit_accepted(out, out_cap, xid, ACCEPT_PROC_UNAVAIL);
        if (n == 0) {
            return -1;
        }
        *out_len = n;
        return 0;
    }
    }
}

/* -----------------------------------------------------------------------
 * I/O loop -- server context and thread.
 * ----------------------------------------------------------------------- */

/**
 * Per-TCP-connection state.  Each connection accumulates one
 * record-marked RPC message at a time.  We require the entire
 * record (including any continuation fragments) to fit in @a buf;
 * over-size records cause the connection to be closed.  This is
 * the same policy enforced by the main NFS server in rpc_server.c
 * but with a much smaller cap because mountd records are tiny.
 */
struct mc_tcp_conn {
    int      fd;                            /* -1 = slot unused */
    time_t   last_activity;
    uint32_t need_bytes;                    /* bytes still needed for current record body */
    uint32_t have_bytes;                    /* bytes accumulated in body so far */
    bool     have_rm;                       /* record-mark header read */
    bool     last_fragment;                 /* MSB of record mark for current fragment */
    uint8_t  rm[4];                         /* in-flight record-mark bytes */
    uint8_t  rm_have;                       /* bytes of @a rm read so far */
    uint8_t  body[MOUNTD_MAX_PKT];          /* accumulated record body */
};

struct mountd_compat_ctx {
    int                 udp_fd;
    int                 tcp_fd;
    int                 epoll_fd;
    int                 wake_fd;            /* eventfd-style: pipe[0] */
    int                 wake_write_fd;      /* pipe[1] */
    pthread_t           thread;
    atomic_int          running;
    bool                thread_started;

    uint16_t            bound_port;
    bool                registered;

    struct mc_tcp_conn  conns[MOUNTD_MAX_TCP_CONNS];

    struct mountd_compat_exports exports;
};

/* -----------------------------------------------------------------------
 * rpcbind self-registration (best-effort, UDP).
 *
 * RFC 1833 S3.2: PMAPPROC_SET takes a `mapping { prog, vers, prot,
 * port }` and returns a single boolean.  We do not need to inspect
 * the boolean result; if rpcbind is unreachable or rejects the
 * request, we just log and continue.
 *
 * This intentionally hand-rolls the request rather than pulling in
 * libntirpc's client API: the server is otherwise self-contained
 * and we want to avoid coupling the responder to libntirpc's
 * threading conventions.
 * ----------------------------------------------------------------------- */

static atomic_uint g_pmap_xid = ATOMIC_VAR_INIT(0xb0bcafeU);

static void mc_send_pmap_request(int fd,
                                 const struct sockaddr_in *dst,
                                 uint32_t proc, uint32_t prot, uint16_t port)
{
    uint8_t pkt[64];
    size_t  off = 0;

    uint32_t xid = atomic_fetch_add(&g_pmap_xid, 1);

    mc_put_u32(pkt + off, xid);             off += 4;
    mc_put_u32(pkt + off, RPC_CALL);        off += 4;
    mc_put_u32(pkt + off, RPC_VERSION);     off += 4;
    mc_put_u32(pkt + off, PMAP_PROG);       off += 4;
    mc_put_u32(pkt + off, PMAP_VERS2);      off += 4;
    mc_put_u32(pkt + off, proc);            off += 4;
    /* AUTH_NONE / AUTH_NONE */
    mc_put_u32(pkt + off, RPC_AUTH_NONE);   off += 4;
    mc_put_u32(pkt + off, 0);               off += 4;
    mc_put_u32(pkt + off, RPC_AUTH_NONE);   off += 4;
    mc_put_u32(pkt + off, 0);               off += 4;
    /* mapping */
    mc_put_u32(pkt + off, MOUNTD_PROG);     off += 4;
    mc_put_u32(pkt + off, MOUNTD_VERS3);    off += 4;
    mc_put_u32(pkt + off, prot);            off += 4;
    mc_put_u32(pkt + off, (uint32_t)port);  off += 4;

    (void)sendto(fd, pkt, off, MSG_NOSIGNAL,
                 (const struct sockaddr *)dst, sizeof(*dst));

    /* Best-effort drain of any reply so the kernel buffer doesn't
     * grow unbounded if the same fd is used multiple times. */
    uint8_t reply[64];
    (void)recv(fd, reply, sizeof(reply), MSG_DONTWAIT);
}

static void mc_rpcbind_set(uint16_t port, bool register_op)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        return;
    }

    struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };
    (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in dst;
    memset(&dst, 0, sizeof(dst));
    dst.sin_family      = AF_INET;
    dst.sin_port        = htons(PMAP_PORT);
    dst.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    uint32_t proc = register_op ? PMAPPROC_SET : PMAPPROC_UNSET;

    /* Register / unregister both transports. */
    mc_send_pmap_request(fd, &dst, proc, PMAP_IPPROTO_TCP, port);
    mc_send_pmap_request(fd, &dst, proc, PMAP_IPPROTO_UDP, port);

    close(fd);
}

/* -----------------------------------------------------------------------
 * Socket setup helpers.
 * ----------------------------------------------------------------------- */

static int mc_set_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return -1;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int mc_bind_addr(const char *bind_addr, uint16_t port,
                        struct sockaddr_in *out)
{
    memset(out, 0, sizeof(*out));
    out->sin_family = AF_INET;
    out->sin_port   = htons(port);

    if (bind_addr == NULL || bind_addr[0] == '\0') {
        out->sin_addr.s_addr = htonl(INADDR_ANY);
        return 0;
    }
    if (inet_pton(AF_INET, bind_addr, &out->sin_addr) != 1) {
        return -1;
    }
    return 0;
}

static int mc_open_udp(const char *bind_addr, uint16_t port, uint16_t *bound_out)
{
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        return -1;
    }
    int one = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    if (mc_bind_addr(bind_addr, port, &addr) != 0) {
        close(fd);
        return -1;
    }
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    socklen_t alen = sizeof(addr);
    if (getsockname(fd, (struct sockaddr *)&addr, &alen) == 0) {
        *bound_out = ntohs(addr.sin_port);
    }
    if (mc_set_nonblock(fd) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int mc_open_tcp(const char *bind_addr, uint16_t port, uint16_t *bound_out)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    int one = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr;
    if (mc_bind_addr(bind_addr, port, &addr) != 0) {
        close(fd);
        return -1;
    }
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, MOUNTD_MAX_TCP_CONNS) < 0) {
        close(fd);
        return -1;
    }
    socklen_t alen = sizeof(addr);
    if (getsockname(fd, (struct sockaddr *)&addr, &alen) == 0) {
        *bound_out = ntohs(addr.sin_port);
    }
    if (mc_set_nonblock(fd) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* -----------------------------------------------------------------------
 * TCP connection management
 * ----------------------------------------------------------------------- */

static void mc_conn_reset(struct mc_tcp_conn *c)
{
    c->fd             = -1;
    c->last_activity  = 0;
    c->need_bytes     = 0;
    c->have_bytes     = 0;
    c->have_rm        = false;
    c->last_fragment  = false;
    c->rm_have        = 0;
}

static void mc_conn_close(struct mountd_compat_ctx *ctx,
                          struct mc_tcp_conn *c)
{
    if (c->fd >= 0) {
        (void)epoll_ctl(ctx->epoll_fd, EPOLL_CTL_DEL, c->fd, NULL);
        close(c->fd);
    }
    mc_conn_reset(c);
}

static struct mc_tcp_conn *mc_conn_alloc(struct mountd_compat_ctx *ctx, int fd)
{
    for (int i = 0; i < MOUNTD_MAX_TCP_CONNS; i++) {
        if (ctx->conns[i].fd < 0) {
            mc_conn_reset(&ctx->conns[i]);
            ctx->conns[i].fd            = fd;
            ctx->conns[i].last_activity = time(NULL);
            return &ctx->conns[i];
        }
    }
    return NULL;
}

/**
 * Send a complete record-marked TCP RPC reply.
 * Returns 0 on success, -1 on error (caller should close).
 */
static int mc_tcp_write_record(int fd, const uint8_t *body, size_t body_len)
{
    if (body_len > 0x7FFFFFFFU) {
        return -1;
    }
    uint8_t rm[4];
    /* MSB set = last fragment. */
    uint32_t marker = (uint32_t)body_len | 0x80000000U;
    mc_put_u32(rm, marker);

    struct iovec iov[2];
    iov[0].iov_base = rm;
    iov[0].iov_len  = 4;
    iov[1].iov_base = (void *)body;
    iov[1].iov_len  = body_len;

    struct msghdr mh;
    memset(&mh, 0, sizeof(mh));
    mh.msg_iov    = iov;
    mh.msg_iovlen = 2;

    ssize_t n = sendmsg(fd, &mh, MSG_NOSIGNAL);
    if (n < 0 || (size_t)n != body_len + 4U) {
        return -1;
    }
    return 0;
}

/**
 * Read available bytes from @a c->fd into the pending record-mark
 * header / body, and dispatch any complete records.
 * Returns 0 if the connection should remain open, -1 to close.
 */
static int mc_tcp_drain(struct mountd_compat_ctx *ctx,
                        struct mc_tcp_conn *c)
{
    for (;;) {
        /* 1. Record-mark header. */
        if (!c->have_rm) {
            ssize_t n = recv(c->fd, c->rm + c->rm_have,
                             4U - c->rm_have, MSG_DONTWAIT);
            if (n == 0) {
                return -1;
            }
            if (n < 0) {
                return (errno == EAGAIN || errno == EWOULDBLOCK) ? 0 : -1;
            }
            c->rm_have += (uint8_t)n;
            if (c->rm_have < 4U) {
                return 0;
            }
            uint32_t marker = mc_get_u32(c->rm);
            c->last_fragment = (marker & 0x80000000U) != 0U;
            uint32_t frag_len = marker & 0x7FFFFFFFU;
            if ((uint64_t)c->have_bytes + frag_len > MOUNTD_MAX_PKT) {
                /* Record too large -- drop the connection. */
                return -1;
            }
            c->need_bytes = frag_len;
            c->have_rm    = true;
        }

        /* 2. Record body. */
        while (c->need_bytes > 0) {
            ssize_t n = recv(c->fd, c->body + c->have_bytes,
                             c->need_bytes, MSG_DONTWAIT);
            if (n == 0) {
                return -1;
            }
            if (n < 0) {
                return (errno == EAGAIN || errno == EWOULDBLOCK) ? 0 : -1;
            }
            c->have_bytes += (uint32_t)n;
            c->need_bytes -= (uint32_t)n;
        }

        /* 3. End of fragment.  If not the last, loop for the next
         *    record-mark; if last, dispatch and reset. */
        c->have_rm = false;
        c->rm_have = 0;
        if (!c->last_fragment) {
            continue;
        }

        /* Dispatch. */
        c->last_activity = time(NULL);

        uint8_t  reply[MOUNTD_MAX_PKT];
        size_t   reply_len = 0;
        int rc = mountd_compat_handle_packet(&ctx->exports,
                                             c->body, c->have_bytes,
                                             reply, sizeof(reply),
                                             &reply_len);
        c->have_bytes = 0;

        if (rc != 0) {
            /* Drop silently per RFC 5531 S10. */
            continue;
        }
        if (mc_tcp_write_record(c->fd, reply, reply_len) != 0) {
            return -1;
        }
    }
}

/* -----------------------------------------------------------------------
 * Worker thread
 * ----------------------------------------------------------------------- */

static void mc_handle_udp(struct mountd_compat_ctx *ctx)
{
    for (;;) {
        uint8_t in[MOUNTD_MAX_PKT];
        struct sockaddr_in src;
        socklen_t slen = sizeof(src);

        ssize_t n = recvfrom(ctx->udp_fd, in, sizeof(in), MSG_DONTWAIT,
                             (struct sockaddr *)&src, &slen);
        if (n < 0) {
            return;                                 /* EAGAIN or fatal */
        }
        if (n == 0 || (size_t)n > MOUNTD_MAX_PKT) {
            continue;
        }

        uint8_t out[MOUNTD_MAX_PKT];
        size_t  out_len = 0;
        int rc = mountd_compat_handle_packet(&ctx->exports,
                                             in, (size_t)n,
                                             out, sizeof(out), &out_len);
        if (rc != 0) {
            continue;                               /* drop */
        }
        (void)sendto(ctx->udp_fd, out, out_len, MSG_NOSIGNAL,
                     (const struct sockaddr *)&src, slen);
    }
}

static void mc_handle_accept(struct mountd_compat_ctx *ctx)
{
    for (;;) {
        struct sockaddr_in src;
        socklen_t slen = sizeof(src);
        int cfd = accept(ctx->tcp_fd, (struct sockaddr *)&src, &slen);
        if (cfd < 0) {
            return;
        }

        struct mc_tcp_conn *c = mc_conn_alloc(ctx, cfd);
        if (c == NULL) {
            /* No slot -- graceful close. */
            close(cfd);
            continue;
        }
        if (mc_set_nonblock(cfd) < 0) {
            mc_conn_close(ctx, c);
            continue;
        }
        struct epoll_event ev;
        memset(&ev, 0, sizeof(ev));
        ev.events   = EPOLLIN | EPOLLRDHUP;
        ev.data.ptr = c;
        if (epoll_ctl(ctx->epoll_fd, EPOLL_CTL_ADD, cfd, &ev) != 0) {
            mc_conn_close(ctx, c);
        }
    }
}

static void mc_sweep_idle(struct mountd_compat_ctx *ctx)
{
    time_t now = time(NULL);
    for (int i = 0; i < MOUNTD_MAX_TCP_CONNS; i++) {
        struct mc_tcp_conn *c = &ctx->conns[i];
        if (c->fd < 0) {
            continue;
        }
        if (now - c->last_activity > MOUNTD_TCP_IDLE_SEC) {
            mc_conn_close(ctx, c);
        }
    }
}

static void *mc_thread(void *arg)
{
    struct mountd_compat_ctx *ctx = arg;
    struct epoll_event events[8];

    while (atomic_load(&ctx->running)) {
        int n = epoll_wait(ctx->epoll_fd, events,
                           (int)(sizeof(events) / sizeof(events[0])),
                           MOUNTD_RECV_TIMEOUT_MS);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        for (int i = 0; i < n; i++) {
            void *p = events[i].data.ptr;
            if (p == &ctx->udp_fd) {
                mc_handle_udp(ctx);
                continue;
            }
            if (p == &ctx->tcp_fd) {
                mc_handle_accept(ctx);
                continue;
            }
            if (p == &ctx->wake_fd) {
                /* Drain wake pipe; the loop condition check above
                 * already saw 'running == 0' so we'll exit. */
                uint8_t drain[16];
                (void)read(ctx->wake_fd, drain, sizeof(drain));
                continue;
            }
            /* TCP connection event. */
            struct mc_tcp_conn *c = p;
            if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                mc_conn_close(ctx, c);
                continue;
            }
            if (events[i].events & EPOLLIN) {
                if (mc_tcp_drain(ctx, c) != 0) {
                    mc_conn_close(ctx, c);
                }
            }
        }
        mc_sweep_idle(ctx);
    }
    return NULL;
}

/* -----------------------------------------------------------------------
 * Public lifecycle
 * ----------------------------------------------------------------------- */

static int mc_validate_and_copy_exports(const struct mds_config *cfg,
                                        struct mountd_compat_exports *out)
{
    if (cfg->mountd_compat_export_count == 0U) {
        out->count = 1U;
        (void)snprintf(out->paths[0], sizeof(out->paths[0]), "/");
        return 0;
    }
    if (cfg->mountd_compat_export_count > MOUNTD_COMPAT_MAX_EXPORTS) {
        return -1;
    }
    out->count = cfg->mountd_compat_export_count;
    for (uint32_t i = 0; i < out->count; i++) {
        const char *src = cfg->mountd_compat_exports[i];
        if (src[0] == '\0') {
            return -1;
        }
        size_t len = strnlen(src, MOUNTD_COMPAT_PATH_MAX);
        if (len >= MOUNTD_COMPAT_PATH_MAX) {
            return -1;
        }
        memcpy(out->paths[i], src, len);
        out->paths[i][len] = '\0';
    }
    return 0;
}

int mountd_compat_start(const struct mds_config *cfg,
                        struct mountd_compat_ctx **out)
{
    if (cfg == NULL || out == NULL) {
        return -1;
    }
    *out = NULL;

    struct mountd_compat_ctx *ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        return -1;
    }
    ctx->udp_fd        = -1;
    ctx->tcp_fd        = -1;
    ctx->epoll_fd      = -1;
    ctx->wake_fd       = -1;
    ctx->wake_write_fd = -1;
    for (int i = 0; i < MOUNTD_MAX_TCP_CONNS; i++) {
        ctx->conns[i].fd = -1;
    }

    if (mc_validate_and_copy_exports(cfg, &ctx->exports) != 0) {
        MDS_LOG_ERROR(LOG_COMP_MDS,
            "mountd_compat: invalid export list");
        free(ctx);
        return -1;
    }

    uint16_t udp_port = 0;
    uint16_t tcp_port = 0;

    ctx->udp_fd = mc_open_udp(cfg->mountd_compat_bind_addr,
                              cfg->mountd_compat_port, &udp_port);
    if (ctx->udp_fd < 0) {
        MDS_LOG_WARN(LOG_COMP_MDS,
            "mountd_compat: UDP bind %s:%u failed: %s",
            cfg->mountd_compat_bind_addr,
            (unsigned)cfg->mountd_compat_port,
            strerror(errno));
        goto fail;
    }

    /* If port 0 was requested, the OS gave us one; bind TCP to the
     * same number so showmount sees a single registered port. */
    uint16_t requested_tcp = cfg->mountd_compat_port == 0U
                             ? udp_port
                             : cfg->mountd_compat_port;
    ctx->tcp_fd = mc_open_tcp(cfg->mountd_compat_bind_addr,
                              requested_tcp, &tcp_port);
    if (ctx->tcp_fd < 0) {
        MDS_LOG_WARN(LOG_COMP_MDS,
            "mountd_compat: TCP bind %s:%u failed: %s",
            cfg->mountd_compat_bind_addr,
            (unsigned)requested_tcp,
            strerror(errno));
        goto fail;
    }
    if (tcp_port != udp_port) {
        MDS_LOG_WARN(LOG_COMP_MDS,
            "mountd_compat: UDP and TCP bound to different "
            "ports (udp=%u tcp=%u); rpcbind clients may pick TCP",
            (unsigned)udp_port, (unsigned)tcp_port);
    }
    ctx->bound_port = tcp_port;

    /* Wake pipe for prompt shutdown without waiting on epoll timeout. */
    int wp[2];
    if (pipe(wp) != 0) {
        goto fail;
    }
    ctx->wake_fd       = wp[0];
    ctx->wake_write_fd = wp[1];
    if (mc_set_nonblock(ctx->wake_fd) < 0) {
        goto fail;
    }

    ctx->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (ctx->epoll_fd < 0) {
        goto fail;
    }

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events   = EPOLLIN;
    ev.data.ptr = &ctx->udp_fd;
    if (epoll_ctl(ctx->epoll_fd, EPOLL_CTL_ADD, ctx->udp_fd, &ev) != 0) {
        goto fail;
    }
    ev.data.ptr = &ctx->tcp_fd;
    if (epoll_ctl(ctx->epoll_fd, EPOLL_CTL_ADD, ctx->tcp_fd, &ev) != 0) {
        goto fail;
    }
    ev.data.ptr = &ctx->wake_fd;
    if (epoll_ctl(ctx->epoll_fd, EPOLL_CTL_ADD, ctx->wake_fd, &ev) != 0) {
        goto fail;
    }

    if (cfg->mountd_compat_register_rpcbind) {
        mc_rpcbind_set(ctx->bound_port, true);
        ctx->registered = true;
    }

    atomic_store(&ctx->running, 1);
    if (pthread_create(&ctx->thread, NULL, mc_thread, ctx) != 0) {
        atomic_store(&ctx->running, 0);
        goto fail;
    }
    ctx->thread_started = true;

    MDS_LOG_INFO(LOG_COMP_MDS,
        "mountd_compat: listening on %s:%u (udp+tcp)%s",
        cfg->mountd_compat_bind_addr,
        (unsigned)ctx->bound_port,
        ctx->registered ? ", registered with rpcbind" : "");

    *out = ctx;
    return 0;

fail:
    if (ctx->wake_write_fd >= 0) {
        close(ctx->wake_write_fd);
    }
    if (ctx->wake_fd >= 0) {
        close(ctx->wake_fd);
    }
    if (ctx->epoll_fd >= 0) {
        close(ctx->epoll_fd);
    }
    if (ctx->udp_fd >= 0) {
        close(ctx->udp_fd);
    }
    if (ctx->tcp_fd >= 0) {
        close(ctx->tcp_fd);
    }
    free(ctx);
    return -1;
}

void mountd_compat_stop(struct mountd_compat_ctx *ctx)
{
    if (ctx == NULL) {
        return;
    }
    atomic_store(&ctx->running, 0);

    if (ctx->wake_write_fd >= 0) {
        uint8_t b = 0;
        (void)write(ctx->wake_write_fd, &b, 1);
    }
    if (ctx->thread_started) {
        (void)pthread_join(ctx->thread, NULL);
    }

    /* Close client connections. */
    for (int i = 0; i < MOUNTD_MAX_TCP_CONNS; i++) {
        if (ctx->conns[i].fd >= 0) {
            close(ctx->conns[i].fd);
            ctx->conns[i].fd = -1;
        }
    }

    if (ctx->registered) {
        mc_rpcbind_set(ctx->bound_port, false);
    }

    if (ctx->wake_write_fd >= 0) {
        close(ctx->wake_write_fd);
    }
    if (ctx->wake_fd >= 0) {
        close(ctx->wake_fd);
    }
    if (ctx->epoll_fd >= 0) {
        close(ctx->epoll_fd);
    }
    if (ctx->udp_fd >= 0) {
        close(ctx->udp_fd);
    }
    if (ctx->tcp_fd >= 0) {
        close(ctx->tcp_fd);
    }
    free(ctx);
}

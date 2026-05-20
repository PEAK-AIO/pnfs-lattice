/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * rpc_server.c -- TCP-based ONC-RPC server for NFSv4.1.
 *
 * Epoll event loop with optional thread-pool COMPOUND dispatch.
 * Handles ONC-RPC record marking
 * (RFC 5531 S11), dispatches NFS NULL and COMPOUND procedures.
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/uio.h>
#include <netinet/tcp.h>

#include "rpc_server.h"
#include "nfs4_cb.h"    /* nfs4_cb_deliver_reply */
#include "xdr_codec.h"
#include "compound.h"
#include "session.h"
#include "proxy_io.h"
#include "health.h"
#include "io_tracker.h"
#include "quota.h"
#include "copy_offload.h"
#include "rename_2pc.h"
#include "cluster_transport.h"
#include "mds_gss.h"
#include "mds_shard.h"

/* -----------------------------------------------------------------------
 * Internal structures
 * ----------------------------------------------------------------------- */

/** Maximum pending send data per connection (1 MiB). */
#define SEND_QUEUE_MAX (1U << 20)

/** Per-connection state for record-marked message reassembly. */
struct rpc_conn {
    int       fd;
    uint8_t  *recv_buf;       /**< Incoming record assembly buffer. */
    uint32_t  recv_len;       /**< Bytes received so far. */
    uint32_t  recv_alloc;     /**< Allocated size of recv_buf. */
    uint32_t  frag_remain;    /**< Bytes remaining in current fragment. */
    bool      frag_last;      /**< Last-fragment flag of current frag. */
    bool      have_frag_hdr;  /**< Have we read the 4-byte frag header? */
    uint8_t   hdr_buf[4];    /**< Partial fragment header accumulator. */
    uint32_t  hdr_pos;        /**< Bytes of hdr_buf filled. */

    /* Output queue for EAGAIN handling -- circular buffer.
     * send_head is the read position; send_len is the count of
     * pending bytes.  No memmove on partial drain. */
    uint8_t  *send_buf;       /**< Circular output buffer. */
    uint32_t  send_head;      /**< Read position (circular). */
    uint32_t  send_len;       /**< Bytes pending. */
    uint32_t  send_alloc;     /**< Buffer capacity. */

    /* Thread-pool dispatch (3.3). */
    pthread_mutex_t send_lock; /**< Protects send_buf/send_len + writev. */
    _Atomic bool    busy;      /**< Request in flight on worker thread. */

    /* Embedded work item -- eliminates malloc per dispatch.
     * Safe because busy prevents concurrent dispatch per-conn. */
    struct rpc_server *wi_srv;        /**< Server backpointer for worker. */
    uint8_t           *wi_record;     /**< Points to recv_buf (zero-copy). */
    uint32_t           wi_record_len; /**< Record length at dispatch time. */
};

struct rpc_server {
    int                      listen_fd;
    int                      epoll_fd;
    int                      stop_pipe[2]; /**< Write end to wake epoll. */
    _Atomic int              running;

    uint16_t                 port;
    uint32_t                 mds_id;
    uint32_t                 stripe_unit;
    uint64_t                 write_verf;
    uint32_t                 max_conns;
    /* Phase 1: placement policy dispatcher for LAYOUTGET. */
    enum mds_placement_policy placement_policy;
    bool                     placement_policy_enabled;
    /* Phase 3: default stripe/mirror geometry. */
    uint32_t                 default_stripe_count;
    uint32_t                 default_mirror_count;
    /* Phase F of docs/hpc-nto1-plan.md -- HPC-Shared GETATTR
     * consistency mode. */
    enum mds_hpc_getattr_mode hpc_getattr_mode;
    /* Phase C / Step 5 of docs/hpc-nto1-plan.md -- wide pre-warm
     * stripe-count cap.  0 selects compile-time default 128. */
    uint32_t                 hpc_max_stripe_count;
    /* Phase C of docs/hpc-nto1-plan.md -- flex-files layout XDR
     * wire form for HPC-Shared inodes.  See enum mds_hpc_xdr_form. */
    enum mds_hpc_xdr_form    hpc_xdr_form;

    struct mds_catalogue    *cat;
    struct session_table    *st;
    struct open_state_table *ot;
    struct lock_table *lt;
    struct commit_queue    *cq;
    struct ds_health_monitor *ds_hm;
    struct subtree_map              *smap;
    const struct cluster_membership *membership;

    /* Extended compound_data context (Item 52). */
    bool gpudirect_required;
    bool skip_transient_ndb;
    enum nfs_auth_mode min_auth;
    struct mds_proxy_ctx            *proxy;
    struct health_monitor           *hm;
    struct io_tracker               *io_tracker;
    struct mds_quota_ctx            *quota;
    struct ds_prealloc_ctx           *prealloc;
    struct ds_prepare_ctx           *ds_prepare;
    struct ds_cache                 *ds_cache;
    struct inode_cache              *icache;
    struct dirent_cache             *dcache;
    /* Phase D of docs/hpc-nto1-plan.md -- HPC-Shared layout cache. */
    struct layout_cache             *lcache;
    /* Phase F of docs/hpc-nto1-plan.md -- HPC-Shared LAYOUTCOMMIT
     * aggregator.  NULL means synchronous LAYOUTCOMMIT (legacy). */
    struct layout_commit_aggregator *lcommit_agg;
    struct deleg_table              *dt;
    struct dir_deleg_table          *ddt;
    struct layout_recall            *lr;
    struct copy_offload_table       *cot;
    const struct rename_2pc_transport *transport;

    struct mds_shard_map     *shard_map;   /**< Shard routing map. */
    struct mds_gss_table    *gss_tbl;     /**< GSS context table. */

    /* Thread-pool dispatch (3.3). */
    struct threadpool       *tp;          /**< Worker pool (NULL = inline). */
    int                      comp_pipe[2]; /**< Completion pipe: workers write conn idx. */
    uint32_t            *free_list;
    uint32_t             free_top;   /**< eventfd to wake epoll. */

    struct rpc_conn         *conns;       /**< Array [max_conns]. */
    uint32_t                 conn_count;

    /* fd-index: O(1) lookup by fd instead of linear scan. */
    struct rpc_conn        **fd_to_conn;  /**< Sparse array [fd_index_size]. */
    uint32_t                 fd_index_size;
};

/* -----------------------------------------------------------------------
 * Helpers
 * ----------------------------------------------------------------------- */

static int set_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);

    if (flags < 0) {
        return -1;
}
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void conn_init(struct rpc_conn *c)
{
    memset(c, 0, sizeof(*c));
    c->fd = -1;
    pthread_mutex_init(&c->send_lock, NULL);
    atomic_store(&c->busy, false);
}

static void conn_reset(struct rpc_conn *c)
{
    if (c->fd >= 0) {
        close(c->fd);
}
    free(c->recv_buf);
    free(c->send_buf);
    pthread_mutex_destroy(&c->send_lock);
    conn_init(c);
}

/** Grow recv_buf to hold at least @need bytes total. */
static int conn_ensure_buf(struct rpc_conn *c, uint32_t need)
{
    if (need <= c->recv_alloc) {
        return 0;
}

    uint32_t newsize = c->recv_alloc ? c->recv_alloc : 4096;

    while (newsize < need) {
        newsize *= 2;
}
    if (newsize > RPC_MAX_RECORD_SIZE) {
        return -1;
}

    uint8_t *p = realloc(c->recv_buf, newsize);

    if (p == NULL) {
        return -1;
}
    c->recv_buf = p;
    c->recv_alloc = newsize;
    return 0;
}

/* -----------------------------------------------------------------------
 * RPC message processing
 * ----------------------------------------------------------------------- */

/**
 * Append data to the connection's circular send queue.
 * Returns 0 on success, -1 if the queue would exceed SEND_QUEUE_MAX.
 */
static int send_queue_append(struct rpc_conn *c,
                             const uint8_t *data, uint32_t len)
{
    uint32_t need = c->send_len + len;

    if (need > SEND_QUEUE_MAX) {
        return -1; /* Queue overflow -- drop connection. */
    }

    if (need > c->send_alloc) {
        uint32_t newsize = c->send_alloc ? c->send_alloc : 4096;

        while (newsize < need) {
            newsize *= 2;
        }
        if (newsize > SEND_QUEUE_MAX) {
            newsize = SEND_QUEUE_MAX;
        }

        /* Linearise existing data into the new buffer. */
        uint8_t *p = malloc(newsize);
        if (p == NULL) {
            return -1;
        }
        if (c->send_len > 0) {
            uint32_t contig = c->send_alloc - c->send_head;
            if (contig >= c->send_len) {
                memcpy(p, c->send_buf + c->send_head,
                       c->send_len);
            } else {
                memcpy(p, c->send_buf + c->send_head, contig);
                memcpy(p + contig, c->send_buf,
                       c->send_len - contig);
            }
        }
        free(c->send_buf);
        c->send_buf = p;
        c->send_head = 0;
        c->send_alloc = newsize;
    }

    /* Write at tail position, handling wrap. */
    uint32_t tail = (c->send_head + c->send_len) % c->send_alloc;
    uint32_t contig = c->send_alloc - tail;
    if (contig >= len) {
        memcpy(c->send_buf + tail, data, len);
    } else {
        memcpy(c->send_buf + tail, data, contig);
        memcpy(c->send_buf, data + contig, len - contig);
    }
    c->send_len += len;
    return 0;
}

/**
 * Try to drain the circular send queue.  Returns:
 *   0  -- queue fully drained (or was already empty).
 *   1  -- partial drain; EAGAIN hit, caller should register EPOLLOUT.
 *  -1  -- fatal write error.
 *
 * Advances send_head instead of memmove -- O(1) per write.
 */
static int send_queue_drain(struct rpc_conn *c)
{
    while (c->send_len > 0) {
        /* Write the contiguous chunk from head to min(end, alloc). */
        uint32_t contig = c->send_alloc - c->send_head;
        if (contig > c->send_len) {
            contig = c->send_len;
        }
        ssize_t n = write(c->fd, c->send_buf + c->send_head,
                          contig);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return 1; /* Partial; need EPOLLOUT. */
            }
            return -1;
        }
        if (n == 0) {
            return -1;
        }

        /* Advance head -- no memmove needed. */
        c->send_head = (c->send_head + (uint32_t)n) %
                       c->send_alloc;
        c->send_len -= (uint32_t)n;
    }
    /* Reset head to 0 when empty for clean re-use. */
    c->send_head = 0;
    return 0;
}

/**
 * Send a complete record-marked reply.
 *
 * Attempts immediate writev.  If EAGAIN is hit, remaining data is
 * queued in the connection's send_buf for EPOLLOUT-driven drain.
 * Returns 0 on success (or queued), -1 on fatal error.
 *
 * Thread safety: acquires c->send_lock for the duration of the
 * writev+queue_append sequence so that multiple workers cannot
 * interleave bytes on c->fd nor race on c->send_*.  Previously the
 * caller (rpc_work_fn) held send_lock across the whole compound;
 * now that's narrowed to here so EPOLLOUT drain and other workers
 * are not blocked while a compound is in the catalogue.
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static int send_record(struct rpc_conn *c, const uint8_t *data, uint32_t len)
{
    /* Single-fragment: set bit 31 (last-fragment). */
    uint32_t hdr = htonl(len | 0x80000000U);
    uint8_t hdr_buf[4];
    ssize_t total, sent;
    int rc = 0;

    memcpy(hdr_buf, &hdr, 4);
    total = 4 + (ssize_t)len;
    sent = 0;

    pthread_mutex_lock(&c->send_lock);
    while (sent < total) {
        struct iovec iov[2];
        int iovcnt = 0;
        ssize_t n;

        /* Recompute iov from the current offset each iteration. */
        if (sent < 4) {
            iov[0].iov_base = hdr_buf + sent;
            iov[0].iov_len = 4 - (size_t)sent;
            iov[1].iov_base = (void *)data;
            iov[1].iov_len = len;
            iovcnt = 2;
        } else {
            size_t data_off = (size_t)sent - 4;

            iov[0].iov_base = (void *)(data + data_off);
            iov[0].iov_len = len - data_off;
            iovcnt = 1;
        }

        n = writev(c->fd, iov, iovcnt);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
}
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* Queue the unsent remainder. */
                if (sent < 4) {
                    if (send_queue_append(c, hdr_buf + sent,
                                          4 - (uint32_t)sent) != 0) {
                        rc = -1;
                        goto out;
                    }
                    if (send_queue_append(c, data, len) != 0) {
                        rc = -1;
                        goto out;
                    }
                } else {
                    uint32_t data_off = (uint32_t)sent - 4;

                    if (send_queue_append(c, data + data_off,
                                          len - data_off) != 0) {
                        rc = -1;
                        goto out;
                    }
                }
                rc = 0; /* Queued; caller will register EPOLLOUT. */
                goto out;
            }
            rc = -1;
            goto out;
        }
        sent += n;
    }
out:
    pthread_mutex_unlock(&c->send_lock);
    return rc;
}

/** Reply buffer size -- 256 KB is plenty for any single NFS compound. */
#define REPLY_BUF_SIZE ((size_t)256 * 1024)

/** Maximum size of an inbound COMPOUND request body, in bytes.
 *
 * Matches the maxrequestsize advertised in CREATE_SESSION (1 MiB,
 * see encode_res_create_session in xdr_ops_core.c).  Records larger
 * than this trigger a COMPOUND reply with NFS4ERR_REQ_TOO_BIG
 * carried in a synthesised SEQUENCE result, per RFC 5661 S15.2 and
 * RFC 8881 S2.10.6.4 (the COMPOUND reply MUST contain at least one
 * resarray entry; an empty resarray crashes Linux and pynfs
 * clients on resarray[0]). */
#define MAX_COMPOUND_REQ_SIZE  ((uint32_t)(1024U * 1024U))

/* Helper: emit a single-element resarray containing a SEQUENCE result
 * with the given @inner status and the outer COMPOUND status set to
 * @outer.  Used by every decode-failure path so the wire reply always
 * carries resarray[0] = SEQUENCE_result(<error>) instead of an empty
 * resarray that confuses every interoperable client.
 *
 * The session_id and slot_id fields are zero-filled because the
 * SEQUENCE op was either not decoded (record too big / bad XDR) or
 * was decoded into a struct we no longer have access to (too many
 * ops following).  Per RFC 8881 S18.46.3, when SEQUENCE returns an
 * error the discriminator selects the void arm so the body is a
 * single status word -- exactly what encode_one_result produces for
 * a non-OK result. */
static int send_compound_decode_failure(struct rpc_conn *c,
                                        char *reply_buf,
                                        uint32_t xid,
                                        const char *tag,
                                        enum nfs4_status outer,
                                        enum nfs4_status inner)
{
    XDR enc;
    struct nfs4_result seq_err;

    memset(&seq_err, 0, sizeof(seq_err));
    seq_err.opnum = OP_SEQUENCE;
    seq_err.status = inner;

    xdrmem_ncreate(&enc, reply_buf, REPLY_BUF_SIZE, XDR_ENCODE);
    if (rpc_encode_accepted_reply(&enc, xid) != 0) {
        return -1;
    }
    if (nfs4_encode_compound_res(&enc, outer, tag, &seq_err, 1) != 0) {
        return -1;
    }
    return send_record(c, (uint8_t *)reply_buf, xdr_getpos(&enc));
}


/**
 * @brief Handle RPCSEC_GSS INIT or CONTINUE_INIT.
 *
 * Decodes the input token, calls mds_gss_accept_token, encodes
 * the reply with the output token and (on completion) a GSS
 * verifier containing MIC(seq_window).
 *
 * @return 0 on success (reply sent), -1 on failure.
 */
static int process_gss_init(struct rpc_server *srv,
                            struct rpc_conn *c,
                            const struct rpc_gss_cred *cred,
                            XDR *dec, const uint8_t *record,
                            uint32_t record_len, uint32_t xid)
{
    char reply_buf[REPLY_BUF_SIZE];
    XDR enc;
    xdrmem_create(&enc, reply_buf, sizeof(reply_buf), XDR_ENCODE);

    if (srv->gss_tbl == NULL) {
        return -1;
    }

    void *out_tok = NULL;
    size_t out_len = 0;
    uint8_t ctx_hnd[64];
    uint32_t ctx_len = 0;
    bool complete = false;

    /* Decode rpc_gss_init_arg: opaque token. */
    uint32_t tok_len = 0;
    if (!xdr_uint32_t(dec, &tok_len) || tok_len > 65536) {
        return -1;
    }
    uint32_t tok_pos = xdr_getpos(dec);
    if (tok_pos + tok_len > record_len) {
        return -1;
    }
    const void *in_tok = record + tok_pos;

    int ga = mds_gss_accept_token(
        srv->gss_tbl, cred->procedure,
        cred->ctx_handle, cred->ctx_handle_len,
        in_tok, tok_len,
        &out_tok, &out_len,
        ctx_hnd, &ctx_len, &complete);

    uint32_t gss_major =
        (ga == 0 && complete) ? 0
        : (ga == 0) ? 1 : 13; /* cppcheck-suppress knownConditionTrueFalse */

    /* Reply header with verifier. */
    uint32_t mt = 1, rs = 0, as_v = 0;
    (void)(xdr_uint32_t(&enc, &xid) &&
           xdr_uint32_t(&enc, &mt) &&
           xdr_uint32_t(&enc, &rs));

    if (ga == 0 && complete && srv->gss_tbl != NULL) {
        uint32_t sw_be = htobe32(64);
        void *sv_mic = NULL;
        size_t sv_len = 0;
        (void)mds_gss_get_mic(
            srv->gss_tbl, ctx_hnd, ctx_len,
            &sw_be, 4, &sv_mic, &sv_len);
        uint32_t svf = 6;
        uint32_t svl = (uint32_t)sv_len;
        (void)(xdr_uint32_t(&enc, &svf) &&
               xdr_uint32_t(&enc, &svl));
        if (sv_len > 0 && sv_mic) {
            (void)xdr_opaque_encode(&enc, sv_mic, svl);
        }
        free(sv_mic);
    } else {
        uint32_t nvf = 0, nvl = 0;
        (void)(xdr_uint32_t(&enc, &nvf) &&
               xdr_uint32_t(&enc, &nvl));
    }
    (void)xdr_uint32_t(&enc, &as_v);

    if (!xdr_uint32_t(&enc, &ctx_len) ||
        (ctx_len > 0 &&
         !xdr_opaque_encode(&enc, (char *)ctx_hnd,
                            ctx_len)) ||
        !xdr_uint32_t(&enc, &gss_major)) {
        free(out_tok);
        return -1;
    }

    uint32_t gss_minor = 0;
    uint32_t seq_window = 64;
    uint32_t tok32 = (uint32_t)out_len;
    if (!xdr_uint32_t(&enc, &gss_minor) ||
        !xdr_uint32_t(&enc, &seq_window) ||
        !xdr_uint32_t(&enc, &tok32) ||
        (out_len > 0 &&
         !xdr_opaque_encode(&enc, out_tok,
                            (uint32_t)out_len))) {
        free(out_tok);
        return -1;
    }
    free(out_tok);
    return send_record(c, (uint8_t *)reply_buf,
                       xdr_getpos(&enc));
}

/**
 * @brief Handle RPCSEC_GSS DESTROY.
 * @return 0 on success (reply sent), -1 on failure.
 */
static int process_gss_destroy(struct rpc_server *srv,
                               struct rpc_conn *c,
                               const struct rpc_gss_cred *cred,
                               uint32_t xid)
{
    char reply_buf[REPLY_BUF_SIZE];
    XDR enc;
    xdrmem_create(&enc, reply_buf, sizeof(reply_buf), XDR_ENCODE);

    if (srv->gss_tbl != NULL) {
        (void)mds_gss_destroy_context(
            srv->gss_tbl,
            cred->ctx_handle,
            cred->ctx_handle_len);
    }
    if (rpc_encode_accepted_reply(&enc, xid) != 0) {
        return -1;
    }
    return send_record(c, (uint8_t *)reply_buf,
                       xdr_getpos(&enc));
}

/** Process a complete RPC record. */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static int process_rpc_record(struct rpc_server *srv, struct rpc_conn *c,
                              uint8_t *record, uint32_t record_len)
{
    XDR dec;
    uint32_t xid, prog, vers, proc;
    char *reply_buf = NULL;
    struct nfs4_op *ops = NULL;
    struct nfs4_result *results = NULL;
    XDR enc;
    int rc = -1;

    /* Per-thread reusable scratch buffers: allocated once on first use,
     * reused across all subsequent requests on the same thread.
     * Eliminates malloc+calloc+free per RPC (~1MB per request).
     *
     * Phase C / Step 1 of docs/hpc-nto1-plan.md -- tl_results MUST be
     * calloc'd, not malloc'd.  compound_process() now calls
     * nfs4_result_destroy(&results[i]) BEFORE memset on each
     * iteration so prior compounds' heap state is freed before the
     * slot is reused.  On the very first compound on a fresh worker
     * thread, the slot is uninitialised; if the random byte at
     * results[i].opnum happens to equal OP_LAYOUTGET (50) the
     * dispatcher would call free() on uninitialised pointers, which
     * corrupts the heap and produces hard-to-diagnose hangs in
     * later RPC paths.  calloc zeroes the whole array once at
     * thread bring-up, which costs one mmap-backed memset and
     * removes the entire class of bug.
     *
     * tl_reply_buf and tl_ops do not own heap state, so plain
     * malloc is still safe for them.  Kept as-is to minimise the
     * surface of this fix. */
    static __thread char *tl_reply_buf = NULL;
    static __thread struct nfs4_op *tl_ops = NULL;
    static __thread struct nfs4_result *tl_results = NULL;

    if (tl_reply_buf == NULL) {
        tl_reply_buf = malloc(REPLY_BUF_SIZE);
        tl_ops = malloc((size_t)NFS4_MAX_OPS * sizeof(struct nfs4_op));
        tl_results = calloc((size_t)NFS4_MAX_OPS,
                            sizeof(struct nfs4_result));
        if (tl_reply_buf == NULL || tl_ops == NULL ||
            tl_results == NULL) {
            return -1;
        }
    }

    reply_buf = tl_reply_buf;

    xdrmem_ncreate(&dec, (char *)record, record_len, XDR_DECODE);

    uint32_t cred_flavor = 1; /* AUTH_SYS default */
    struct rpc_gss_cred gss_cred;
    memset(&gss_cred, 0, sizeof(gss_cred));
    uint32_t cred_uid = 0, cred_gid = 0;
    uint32_t cred_aux_gids[16] = {0};
    uint32_t cred_aux_gid_count = 0;
    if (rpc_decode_call_header(&dec, &xid, &prog, &vers, &proc,
                               &cred_flavor, &gss_cred,
                               &cred_uid, &cred_gid,
                               cred_aux_gids, &cred_aux_gid_count) != 0) {
        goto cleanup; /* Malformed -- drop connection. */
    }

    xdrmem_ncreate(&enc, reply_buf, REPLY_BUF_SIZE, XDR_ENCODE);

    /* Validate program/version. */
    if (prog != NFS_PROGRAM) {
        if (rpc_encode_error_reply(&enc, xid, 1) != 0) {
            goto cleanup;
}
        rc = send_record(c, (uint8_t *)reply_buf, xdr_getpos(&enc));
        goto cleanup;
    }
    if (vers != NFS_V4) {
        if (rpc_encode_error_reply(&enc, xid, 2) != 0) {
            goto cleanup;
}
        rc = send_record(c, (uint8_t *)reply_buf, xdr_getpos(&enc));
        goto cleanup;
    }

    /* --------------------------------------------------------
     * RPCSEC_GSS control messages (RFC 2203 s5.2).
     *
     * GSS INIT/CONTINUE/DESTROY are dispatched here,
     * BEFORE the proc check, because RFC 2203 uses
     * NULLPROC for control messages.  Only DATA falls
     * through to the normal proc dispatch below.
     * -------------------------------------------------------- */
    if (cred_flavor == 6 && gss_cred.version == 1 &&
            gss_cred.procedure != RPCSEC_GSS_DATA) {

        if (gss_cred.procedure == RPCSEC_GSS_INIT ||
                gss_cred.procedure ==
                    RPCSEC_GSS_CONTINUE_INIT) {
            rc = process_gss_init(srv, c, &gss_cred,
                                  &dec, record,
                                  record_len, xid);
            goto cleanup;
        }

        if (gss_cred.procedure == RPCSEC_GSS_DESTROY) {
            rc = process_gss_destroy(srv, c,
                                     &gss_cred, xid);
            goto cleanup;
        }

        /* Unknown GSS procedure -- drop. */
        goto cleanup;
    }

    /* --- Normal proc dispatch --- */
    if (proc == NFSPROC4_NULL) {
        if (rpc_encode_accepted_reply(&enc, xid) != 0) {
            goto cleanup;
        }
        rc = send_record(c, (uint8_t *)reply_buf,
                         xdr_getpos(&enc));
        goto cleanup;
    }

    if (proc != NFSPROC4_COMPOUND) {
        if (rpc_encode_error_reply(&enc, xid, 3) != 0) {
            goto cleanup;
        }
        rc = send_record(c, (uint8_t *)reply_buf, xdr_getpos(&enc));
        goto cleanup;
    }

    /* --- COMPOUND procedure --- */
    {
        uint32_t gss_svc_eff = 0;
        uint8_t *unwrapped_buf = NULL;
        char tag[NFS4_TAG_MAXLEN];
        tag[0] = '\0';
        uint32_t minorver = 0;
        uint32_t op_count = 0;

        /*
         * Heap-allocate ops[] and results[] -- their combined size
         * (~37 MB with current struct layouts) far exceeds the
         * default pthread stack.
         */
        /* Reuse thread-local scratch arrays.  Zero AFTER decode
         * so we only clear the ops actually used (not all 64). */
        ops = tl_ops;
        results = tl_results;

        /* --------------------------------------------------------
         * Auth enforcement -- whitelist gate.
         * Must run BEFORE request-path protection so that
         * gss_svc_eff is set before MIC/unwrap checks.
         * -------------------------------------------------------- */
        bool auth_ok = false;

        if (cred_flavor == 6) {
            if (gss_cred.version != 1) {
                auth_ok = false;
            } else if (gss_cred.procedure ==
                           RPCSEC_GSS_DATA) {
                if (srv->gss_tbl == NULL) {
                    goto wrongsec;
                }
                uint32_t eff_svc = 0;
                if (mds_gss_validate(srv->gss_tbl,
                                     &gss_cred,
                                     &eff_svc) != 0) {
                    goto wrongsec;
                }
                enum nfs_auth_mode eff_auth =
                    (enum nfs_auth_mode)(
                        NFS_AUTH_MODE_KRB5
                        + eff_svc - 1);
                if (eff_auth < srv->min_auth) {
                    goto wrongsec;
                }
                gss_svc_eff = eff_svc;
                auth_ok = true;
            }
        } else if (srv->min_auth <= NFS_AUTH_MODE_SYS) {
            if (cred_flavor == 0 || cred_flavor == 1) {
                auth_ok = true;
            }
        }

        if (!auth_ok) {
wrongsec:
            if (rpc_encode_accepted_reply(&enc, xid) != 0) {
                goto cleanup;
            }
            if (nfs4_encode_compound_res(&enc,
                    NFS4ERR_WRONGSEC,
                    tag, NULL, 0) != 0) {
                goto cleanup;
            }
            rc = send_record(c, (uint8_t *)reply_buf,
                             xdr_getpos(&enc));
            goto cleanup;
        }

        /* Verify header verifier (MIC over seq_num). */
        if (gss_svc_eff > 0) {
            if (mds_gss_verify_header(srv->gss_tbl,
                    &gss_cred, record,
                    gss_cred.cred_end_pos) != 0) {
                goto wrongsec;
            }
        }

        /* --- GSS data protection: request path --- */
        if (gss_svc_eff == RPCSEC_GSS_SVC_PRIVACY) {
            /* krb5p (RFC 2203 s5.3.3.4):
             * opaque databody_priv<>; unwrap gives
             * {seq_num, procedure_args}. */
            uint32_t priv_len = 0;
            if (!xdr_uint32_t(&dec, &priv_len) ||
                    priv_len > 4 * 1024 * 1024) {
                goto wrongsec;
            }
            uint32_t priv_pos = xdr_getpos(&dec);
            if (priv_pos + priv_len > record_len) {
                goto wrongsec;
            }
            void *plain = NULL;
            size_t plain_len = 0;
            if (mds_gss_unwrap_data(srv->gss_tbl,
                    gss_cred.ctx_handle,
                    gss_cred.ctx_handle_len,
                    record + priv_pos, priv_len,
                    &plain, &plain_len) != 0) {
                goto wrongsec;
            }
            unwrapped_buf = plain;
            /* Plaintext: {seq_num, procedure_args}. */
            XDR pdec;
            xdrmem_ncreate(&pdec, (char *)plain,
                           (uint32_t)plain_len,
                           XDR_DECODE);
            uint32_t body_seq = 0;
            if (!xdr_uint32_t(&pdec, &body_seq) ||
                    body_seq != gss_cred.seq_num) {
                free(unwrapped_buf);
                unwrapped_buf = NULL;
                goto wrongsec;
            }
            /* Re-init main decoder on args portion. */
            uint32_t args_off = xdr_getpos(&pdec);
            xdrmem_ncreate(&dec,
                (char *)plain + args_off,
                (uint32_t)(plain_len - args_off),
                XDR_DECODE);
        } else if (gss_svc_eff ==
                       RPCSEC_GSS_SVC_INTEGRITY) {
            /* krb5i (RFC 2203 s5.3.3.3):
             * opaque databody_integ<> then
             * opaque checksum<>. */
            uint32_t integ_len = 0;
            if (!xdr_uint32_t(&dec, &integ_len) ||
                    integ_len > 4 * 1024 * 1024) {
                goto wrongsec;
            }
            uint32_t integ_pos = xdr_getpos(&dec);
            if (integ_pos + integ_len > record_len) {
                goto wrongsec;
            }
            const void *integ_data =
                record + integ_pos;
            /* Skip past integ body to read checksum. */
            XDR skip;
            xdrmem_ncreate(&skip,
                (char *)record + integ_pos,
                integ_len, XDR_DECODE);
            uint32_t body_seq = 0;
            if (!xdr_uint32_t(&skip, &body_seq) ||
                    body_seq != gss_cred.seq_num) {
                goto wrongsec;
            }
            uint32_t args_off = xdr_getpos(&skip);

            /* Read checksum after databody_integ. */
            XDR ckdec;
            xdrmem_ncreate(&ckdec,
                (char *)record + integ_pos
                    + integ_len,
                record_len - integ_pos - integ_len,
                XDR_DECODE);
            uint32_t ck_len = 0;
            if (!xdr_uint32_t(&ckdec, &ck_len) ||
                    ck_len > 1024) {
                goto wrongsec;
            }
            uint32_t ck_pos =
                integ_pos + integ_len + 4;
            if (ck_pos + ck_len > record_len) {
                goto wrongsec;
            }
            if (mds_gss_verify_data(srv->gss_tbl,
                    gss_cred.ctx_handle,
                    gss_cred.ctx_handle_len,
                    integ_data, integ_len,
                    record + ck_pos,
                    ck_len) != 0) {
                goto wrongsec;
            }
            /* Re-init decoder on procedure_args
             * inside databody_integ. */
            xdrmem_ncreate(&dec,
                (char *)record + integ_pos
                    + args_off,
                integ_len - args_off,
                XDR_DECODE);
        }

        /*
         * Pre-check the wire record size against the maxrequestsize we
         * advertised in CREATE_SESSION.  pynfs SEQ6 (testRequestTooBig)
         * relies on this returning NFS4ERR_REQ_TOO_BIG carried by a
         * SEQUENCE result, not an empty resarray.  Doing this before
         * the XDR decoder runs avoids spending CPU parsing a request
         * we are about to reject. */
        if (record_len > MAX_COMPOUND_REQ_SIZE) {
            rc = send_compound_decode_failure(c, reply_buf, xid, "",
                NFS4ERR_REQ_TOO_BIG, NFS4ERR_REQ_TOO_BIG);
            goto cleanup;
        }

        /*
         * RFC 8881 S15.1.10.5 / S2.10.6.1.2 enforcement gate.  The
         * client MUST NOT send a request larger than
         * ca_maxrequestsize, and the server MUST reject any such
         * request with NFS4ERR_REQ_TOO_BIG before doing per-op
         * processing.  RFC 8881 S15.1.10.4 likewise mandates
         * NFS4ERR_TOO_MANY_OPS when the compound carries more
         * operations than ca_maxoperations.  Both checks MUST
         * happen ahead of fine-grained XDR validation: pynfs SEQ6
         * (testRequestTooBig) intentionally packs an oversize
         * component4 inside a LOOKUP that would otherwise fail
         * with NFS4ERR_BADXDR / NFS4ERR_NAMETOOLONG; the
         * RFC-mandated behaviour is REQ_TOO_BIG.
         *
         * To enforce this without parsing the entire compound, we
         * peek just enough of the wire bytes (tag + minorversion
         * + op_count + opnum[0] + session_id) to locate the
         * session, then restore the XDR cursor for the main
         * decoder.  xdr_getpos / xdr_setpos give us a non-
         * destructive look without rebuilding an XDR stream
         * (which would require knowing the underlying buffer
         * pointer, complicated by the GSS-unwrap rebind above).
         */
        if (srv->st != NULL) {
            uint32_t saved_pos = xdr_getpos(&dec);
            uint32_t peek_tag_len = 0;
            uint32_t peek_minor = 0;
            uint32_t peek_count = 0;
            uint32_t peek_op0 = 0;
            uint8_t peek_sid[SESSION_ID_SIZE];
            char tag_skip[NFS4_TAG_MAXLEN];
            bool peek_ok = false;

            if (xdr_uint32_t(&dec, &peek_tag_len) &&
                peek_tag_len < sizeof(tag_skip)) {
                bool tag_ok = (peek_tag_len == 0) ||
                    xdr_opaque_decode(&dec, tag_skip, peek_tag_len);
                if (tag_ok &&
                    xdr_uint32_t(&dec, &peek_minor) &&
                    xdr_uint32_t(&dec, &peek_count) &&
                    peek_count > 0 &&
                    xdr_uint32_t(&dec, &peek_op0) &&
                    peek_op0 == OP_SEQUENCE &&
                    xdr_opaque_decode(&dec, (char *)peek_sid,
                                      SESSION_ID_SIZE)) {
                    peek_ok = true;
                }
            }

            /* Always restore the decoder cursor so the main
             * decode pass starts at the same offset regardless of
             * how far the peek advanced. */
            xdr_setpos(&dec, saved_pos);

            if (peek_ok) {
                uint32_t sess_max_req = 0;
                uint32_t sess_max_ops = 0;

                if (session_get_limits(srv->st, peek_sid,
                        &sess_max_req, &sess_max_ops) == 0) {
                    if (sess_max_req > 0 &&
                        record_len > sess_max_req) {
                        rc = send_compound_decode_failure(c,
                            reply_buf, xid, "",
                            NFS4ERR_REQ_TOO_BIG,
                            NFS4ERR_REQ_TOO_BIG);
                        goto cleanup;
                    }
                    if (sess_max_ops > 0 &&
                        peek_count > sess_max_ops) {
                        rc = send_compound_decode_failure(c,
                            reply_buf, xid, "",
                            NFS4ERR_TOO_MANY_OPS,
                            NFS4ERR_TOO_MANY_OPS);
                        goto cleanup;
                    }
                }
            }
        }

        {
            int dec_rc = nfs4_decode_compound_args(
                &dec, tag, sizeof(tag), &minorver,
                ops, NFS4_MAX_OPS, &op_count);
            if (dec_rc != 0) {
                /* Map decoder error code to RFC-mandated NFS4ERR_*
                 * (RFC 5661 S15.2 / RFC 8881 S2.10.6.1.2):
                 *   -2 = count > NFS4_MAX_OPS  -> TOO_MANY_OPS
                 *   -1 = malformed wire bytes  -> BADXDR
                 * In every case emit a single-element resarray
                 * with a SEQUENCE result carrying the inner
                 * error code, so Linux / pynfs clients can read
                 * resarray[0] without crashing. */
                enum nfs4_status err_status =
                    (dec_rc == -2) ? NFS4ERR_TOO_MANY_OPS
                                   : NFS4ERR_BADXDR;
                rc = send_compound_decode_failure(c, reply_buf, xid,
                    tag, err_status, err_status);
                goto cleanup;
            }
        }

        /*
         * RFC 8881 S1.7 / S14.4: the COMPOUND tag is a utf8str_cs.
         * pynfs COMP3 (testBadTags) iterates get_invalid_utf8strings()
         * and expects NFS4ERR_INVAL for any tag that is not
         * well-formed UTF-8.  The decoder NUL-terminates @tag so
         * strlen() is safe; an embedded NUL inside the wire bytes
         * shows up as a too-short string, which the validator
         * accepts as legal UTF-8 -- we therefore additionally reject
         * any tag whose post-decode strlen does not match the
         * decoded length.  Sent back via send_compound_decode_failure
         * so the resarray-non-empty invariant (RFC 5661 S15.2) is
         * preserved for clients that expect resarray[0].
         */
        {
            size_t tlen = strlen(tag);
            if (!compound_is_valid_utf8(tag, tlen)) {
                rc = send_compound_decode_failure(c, reply_buf, xid,
                    "", NFS4ERR_INVAL, NFS4ERR_INVAL);
                goto cleanup;
            }
        }

        if (minorver < NFS4_MINOR_VERSION_MIN ||
            minorver > NFS4_MINOR_VERSION_MAX) {
            if (rpc_encode_accepted_reply(&enc, xid) != 0) {
                goto cleanup;
            }
            if (nfs4_encode_compound_res(&enc,
                    NFS4ERR_MINOR_VERS_MISMATCH, tag,
                    NULL, 0) != 0) {
                goto cleanup;
            }
            rc = send_record(c, (uint8_t *)reply_buf,
                             xdr_getpos(&enc));
            goto cleanup;
        }

        /* Execute the compound. */
        struct compound_data cd;
        uint32_t result_count;

        compound_init(&cd);
        cd.cat = srv->cat;
        cd.st = srv->st;
        cd.ot = srv->ot;
        cd.lt = srv->lt;
        cd.cq = srv->cq;
        cd.conn = c;
        cd.ds_hm = srv->ds_hm;
        cd.smap = srv->smap;
        cd.membership = srv->membership;
        cd.proxy = srv->proxy;
        cd.hm = srv->hm;
        cd.io_tracker = srv->io_tracker;
        cd.quota = srv->quota;
        cd.prealloc = srv->prealloc;
        cd.ds_prepare = srv->ds_prepare;
        cd.ds_cache = srv->ds_cache;
        cd.icache = srv->icache;
        cd.dcache = srv->dcache;
        cd.lcache = srv->lcache;
        cd.lcommit_agg = srv->lcommit_agg;
        cd.dt = srv->dt;
        cd.ddt = srv->ddt;
        cd.lr = srv->lr;
        cd.cot = srv->cot;
        cd.transport = srv->transport;
        cd.mds_id = srv->mds_id;
        cd.cfg_stripe_unit = srv->stripe_unit;
        cd.cfg_placement_policy = srv->placement_policy;
        cd.cfg_placement_policy_enabled = srv->placement_policy_enabled;
        cd.cfg_default_stripe_count = srv->default_stripe_count;
        cd.cfg_default_mirror_count = srv->default_mirror_count;
        cd.cfg_hpc_getattr_mode = srv->hpc_getattr_mode;
        cd.cfg_hpc_max_stripe_count = srv->hpc_max_stripe_count;
        cd.cfg_hpc_xdr_form = srv->hpc_xdr_form;
        cd.write_verf = srv->write_verf;
        cd.minorversion = minorver;
	cd.shard_map = srv->shard_map;
	/* Initialize request on the default (root) shard. */
	if (srv->shard_map != NULL) {
		const struct mds_shard *def_shard =
			mds_shard_map_get_default(srv->shard_map);
		if (def_shard != NULL) {
			cd.current_shard = def_shard;

			cd.cq = def_shard->cq;
			cd.root_cq = def_shard->cq;
		}
	}
	cd.gpudirect_required = srv->gpudirect_required;
	cd.skip_transient_ndb = srv->skip_transient_ndb;
        cd.auth_flavor = cred_flavor;
        cd.cred_uid = cred_uid;
        cd.cred_gid = cred_gid;
        cd.aux_gid_count = cred_aux_gid_count;
        memcpy(cd.aux_gids, cred_aux_gids,
               cred_aux_gid_count * sizeof(uint32_t));
        cd.gss_svc = gss_svc_eff;
        cd.min_auth = srv->min_auth;

        result_count = compound_process(&cd, ops, results, op_count);

        /* DRC: if replay_cached is set, send the cached reply directly.
         *
         * RFC 5661 S2.10.6.1.3 requires the replied bytes match what
         * the server originally sent for that (session, slot, seq_id),
         * EXCEPT for the RPC XID which MUST match the new request's
         * XID so the client's RPC layer can route the reply to the
         * outstanding call.  Per RFC 5531, the XID is the first
         * 4-byte field of the RPC reply header (ONC-RPC big-endian).
         * We copy the cached bytes into our reply scratch buffer and
         * overwrite [0..3] with the new request's XID before sending.
         *
         * NOTE: GSS-protected replies also embed MIC(seq_num) in the
         * reply verifier; that MIC was computed over the original
         * request's seq_num and will not validate against the
         * replay's new seq_num.  Skip the cached-replay fast path
         * for GSS for now -- the cache miss path returns
         * NFS4ERR_RETRY_UNCACHED_REP, which is RFC-conformant.
         * Drives pynfs SEQ9a. */
        if (cd.replay_cached && cd.st != NULL && gss_svc_eff == 0 &&
            op_count > 0 && ops[0].opnum == OP_SEQUENCE) {
            const uint8_t *cached = NULL;
            uint32_t cached_len = 0;
            if (session_slot_get_cached_reply(
                    cd.st, ops[0].arg.sequence.session_id,
                    ops[0].arg.sequence.slot_id,
                    &cached, &cached_len) == 0 &&
                cached_len >= 4 && cached_len <= REPLY_BUF_SIZE) {
                /* Defensive bounds: a malformed cache entry
                 * (somehow shorter than 4 bytes or larger than
                 * the reply buffer) MUST not be sent. */
                memcpy(reply_buf, cached, cached_len);
                /* Patch XID @ offset 0 (RFC 5531 S9 message header,
                 * uint32 big-endian).  htonl() converts from host
                 * order to network order for the wire. */
                uint32_t xid_be = htonl(xid);
                memcpy(reply_buf, &xid_be, 4);
                rc = send_record(c, (uint8_t *)reply_buf,
                                 cached_len);
                goto cleanup;
            }
            /* Cache miss / GSS / size sanity -- fall through to
             * normal encoding so the SEQUENCE result that
             * compound_process already populated is sent. */
        }

        /* Determine overall status (first error). */
        enum nfs4_status status = NFS4_OK;

        for (uint32_t i = 0; i < result_count; i++) {
            if (results[i].status != NFS4_OK) {
                status = results[i].status;
                break;
            }
        }

        /* Encode procedure results into a separate
         * buffer so GSS protection covers only the
         * results (proc_res_arg_t), not the RPC
         * reply header. */
        uint8_t *proc_res_buf = NULL;
        uint32_t proc_res_len = 0;

        if (gss_svc_eff > 0) {
            /* Encode COMPOUND result to temp buf. */
            XDR res_enc;
            /* NOLINTNEXTLINE(bugprone-implicit-widening-of-multiplication-result) */
            proc_res_buf = malloc(REPLY_BUF_SIZE);
            if (proc_res_buf == NULL) {
                goto cleanup;
            }
            xdrmem_ncreate(&res_enc,
                (char *)proc_res_buf,
                REPLY_BUF_SIZE, XDR_ENCODE);
            if (nfs4_encode_compound_res(&res_enc,
                    status, tag, results,
                    result_count) != 0) {
                free(proc_res_buf);
                goto cleanup;
            }
            proc_res_len = xdr_getpos(&res_enc);
        } else {
            /* Non-GSS: encode normally. */
            if (rpc_encode_accepted_reply(
                    &enc, xid) != 0) {
                goto cleanup;
            }
            if (nfs4_encode_compound_res(&enc,
                    status, tag, results,
                    result_count) != 0) {
                goto cleanup;
            }
        }

        /* DRC: defer caching until after GSS wrapping so cached
         * bytes match what was actually sent on the wire. */
        bool drc_needs_cache = (cd.sequence_done &&
            !cd.replay_cached && cd.st != NULL &&
            op_count > 0 && ops[0].opnum == OP_SEQUENCE);

        /* --- GSS data protection: reply path ---
         * RFC 2203: reply verifier = MIC(seq_num).
         * krb5i: body = databody_integ + checksum.
         * krb5p: body = databody_priv (wrapped). */
        if (gss_svc_eff > 0) {
            /* proc_res_buf already has the procedure
             * results. Build GSS-protected reply. */
            uint32_t compound_len = proc_res_len;
            uint8_t *compound_copy = proc_res_buf;
            proc_res_buf = NULL; /* Ownership moved. */

            /* Build seq_num for verifier MIC. */
            uint32_t seq_be =
                htobe32(gss_cred.seq_num);

            /* Compute verifier: MIC(seq_num). */
            void *verf_mic = NULL;
            size_t verf_mic_len = 0;
            (void)mds_gss_get_mic(srv->gss_tbl,
                gss_cred.ctx_handle,
                gss_cred.ctx_handle_len,
                &seq_be, 4,
                &verf_mic, &verf_mic_len);

            /* Re-encode reply header with GSS
             * verifier. */
            xdrmem_ncreate(&enc, reply_buf,
                REPLY_BUF_SIZE, XDR_ENCODE);
            uint32_t mt = 1, rs = 0;
            uint32_t vf = 6;
            uint32_t vl = (uint32_t)verf_mic_len;
            uint32_t as_val = 0;
            (void)(xdr_uint32_t(&enc, &xid) &&
                   xdr_uint32_t(&enc, &mt) &&
                   xdr_uint32_t(&enc, &rs) &&
                   xdr_uint32_t(&enc, &vf) &&
                   xdr_uint32_t(&enc, &vl));
            if (verf_mic_len > 0 && verf_mic) {
                (void)xdr_opaque_encode(&enc,
                    verf_mic, vl);
            }
            free(verf_mic);
            (void)xdr_uint32_t(&enc, &as_val);

            if (gss_svc_eff ==
                    RPCSEC_GSS_SVC_INTEGRITY) {
                /* Build {seq_num, reply_body}. */
                uint32_t integ_sz = 4 + compound_len;
                uint8_t *integ_buf =
                    malloc(integ_sz);
                if (integ_buf == NULL) {
                    free(compound_copy);
                    goto cleanup;
                }
                memcpy(integ_buf, &seq_be, 4);
                memcpy(integ_buf + 4,
                       compound_copy, compound_len);

                /* Emit databody_integ. */
                (void)(xdr_uint32_t(&enc,
                           &integ_sz) &&
                       xdr_opaque_encode(&enc,
                           (char *)integ_buf,
                           integ_sz));

                /* Compute + emit checksum. */
                void *ck = NULL;
                size_t ck_len = 0;
                (void)mds_gss_get_mic(
                    srv->gss_tbl,
                    gss_cred.ctx_handle,
                    gss_cred.ctx_handle_len,
                    integ_buf, integ_sz,
                    &ck, &ck_len);
                uint32_t cl = (uint32_t)ck_len;
                (void)(xdr_uint32_t(&enc, &cl) &&
                       (ck_len > 0 ?
                        xdr_opaque_encode(&enc,
                            ck, cl) : true));
                free(ck);
                free(integ_buf);
            } else if (gss_svc_eff ==
                    RPCSEC_GSS_SVC_PRIVACY) {
                /* Build {seq_num, reply_body},
                 * then wrap. */
                uint32_t plain_sz = 4 + compound_len;
                uint8_t *plain_buf =
                    malloc(plain_sz);
                if (plain_buf == NULL) {
                    free(compound_copy);
                    goto cleanup;
                }
                memcpy(plain_buf, &seq_be, 4);
                memcpy(plain_buf + 4,
                       compound_copy, compound_len);

                void *wrapped = NULL;
                size_t wrapped_len = 0;
                int wrc = mds_gss_wrap_data(
                    srv->gss_tbl,
                    gss_cred.ctx_handle,
                    gss_cred.ctx_handle_len,
                    plain_buf, plain_sz,
                    &wrapped, &wrapped_len);
                free(plain_buf);

                if (wrc == 0 && wrapped) {
                    uint32_t wl =
                        (uint32_t)wrapped_len;
                    (void)(xdr_uint32_t(&enc, &wl)
                        && xdr_opaque_encode(&enc,
                               wrapped, wl));
                    free(wrapped);
                }
            } else {
                /* krb5 (SVC_NONE): plain results,
                 * no length prefix (RFC 2203 s5.3.3.2
                 * -- same as AUTH_NONE format). */
                if (compound_len > 0) {
                    uint32_t pos = xdr_getpos(&enc);
                    if (pos + compound_len >
                            REPLY_BUF_SIZE) {
                        free(compound_copy);
                        goto cleanup;
                    }
                    memcpy(reply_buf + pos,
                           compound_copy,
                           compound_len);
                    xdr_setpos(&enc,
                        pos + compound_len);
                }
            }
            free(compound_copy);
        }
        free(proc_res_buf);

        free(unwrapped_buf);

        /* DRC: cache the final reply bytes (post-GSS-wrap). */
        if (drc_needs_cache) {
            (void)session_slot_cache_reply(
                cd.st, ops[0].arg.sequence.session_id,
                ops[0].arg.sequence.slot_id,
                (const uint8_t *)reply_buf, xdr_getpos(&enc));
        }
        rc = send_record(c, (uint8_t *)reply_buf, xdr_getpos(&enc));
    }

cleanup:
    /* Thread-local buffers: no free needed. */
    (void)ops;
    (void)results;
    (void)reply_buf;
    return rc;
}

/* -----------------------------------------------------------------------
 * Connection I/O
 * ----------------------------------------------------------------------- */

/**
 * Read data from a connection and process complete records.
 *
 * Returns 0 on success (may have processed 0 or more records),
 * -1 if the connection should be closed.
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */

/* -----------------------------------------------------------------------
 * Thread-pool COMPOUND dispatch (3.3)
 * ----------------------------------------------------------------------- */

/**
 * Worker function: process one RPC record on a pool thread.
 *
 * The work item is embedded in rpc_conn (zero-copy: wi_record points
 * directly into recv_buf).  No malloc/free on the hot path.
 */
static void rpc_work_fn(void *arg)
{
    struct rpc_conn   *conn = arg;
    struct rpc_server *srv  = conn->wi_srv;

    /* send_record now acquires conn->send_lock internally around its
     * writev / send_queue_append sequence (see send_record's docstring).
     * The outer lock that used to wrap process_rpc_record was overkill:
     * it serialised the entire catalogue-bound compound, blocking the
     * EPOLLOUT drain path and starving the conn's write side.
     *
     * Concurrent worker dispatch on this same conn is still bounded by
     * conn->busy (set above by dispatch_to_pool), so only one compound
     * is in flight per conn for now.  The remaining serialisation we
     * need -- bytes-on-wire ordering -- is provided by send_lock's
     * narrower scope inside send_record. */
    (void)process_rpc_record(srv, conn,
                             conn->wi_record, conn->wi_record_len);

    atomic_store(&conn->busy, false);

    /* Notify the epoll thread which connection completed. */
    uint32_t cidx = (uint32_t)(conn - srv->conns);
    (void)write(srv->comp_pipe[1], &cidx, sizeof(cidx));
}

/**
 * @brief Submit a completed RPC record to the worker pool.
 *
 * Copies the record, disables EPOLLIN, and submits to the threadpool.
 * Falls back to inline processing if the pool rejects the item.
 *
 * @return 0 on success (worker owns the connection), -1 on OOM.
 */
/**
 * @brief Non-blocking read into buffer, handling EAGAIN/EINTR/EOF.
 * @return  1 = got some bytes (may need more),
 *          0 = EAGAIN (no data now),
 *         -1 = error / EOF.
 */
static int nb_read(int fd, uint8_t *buf, uint32_t *pos, uint32_t want)
{
    while (*pos < want) {
        ssize_t n = read(fd, buf + *pos, want - *pos);
        if (n > 0) {
            *pos += (uint32_t)n;
            continue;
        }
        if (n == 0) {
            return -1;  /* EOF */
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        }
        if (errno == EINTR) {
            continue;
        }
        return -1;
    }
    return 1;  /* Got all requested bytes. */
}

/**
 * Submit a completed RPC record to the worker pool.
 *
 * Zero-copy: passes recv_buf directly to the worker (no malloc/memcpy).
 * Safe because EPOLLIN is disabled until the worker finishes, so no
 * new reads can overwrite recv_buf while the worker uses it.
 */
static int dispatch_to_pool(struct rpc_server *srv, struct rpc_conn *c)
{
    /* Set up embedded work item -- zero-copy from recv_buf. */
    c->wi_srv        = srv;
    c->wi_record     = c->recv_buf;
    c->wi_record_len = c->recv_len;
    c->recv_len      = 0;  /* Reset for next request. */

    atomic_store(&c->busy, true);

    /* Disable EPOLLIN -- no more reads until worker finishes. */
    struct epoll_event ev;
    ev.events = 0;
    ev.data.fd = c->fd;
    epoll_ctl(srv->epoll_fd, EPOLL_CTL_MOD, c->fd, &ev);

    if (threadpool_submit(srv->tp, rpc_work_fn, c) != 0) {
        /* Pool full -- re-enable EPOLLIN so the next epoll cycle
         * retries.  NEVER fall back to inline processing here:
         * the epoll thread must not block on NDB/NFS calls or
         * it can't process comp_pipe completions, causing a
         * permanent stall under concurrent load. */
        atomic_store(&c->busy, false);
        ev.events = EPOLLIN;
        epoll_ctl(srv->epoll_fd, EPOLL_CTL_MOD, c->fd, &ev);
    }
    return 0;
}

static int conn_read(struct rpc_server *srv, struct rpc_conn *c)
{
    for (;;) {
        /* Phase 1: read fragment header (4 bytes). */
        if (!c->have_frag_hdr) {
            int hr = nb_read(c->fd, c->hdr_buf, &c->hdr_pos, 4);
            if (hr <= 0) {
                return hr;
            }

            uint32_t raw = ((uint32_t)c->hdr_buf[0] << 24) |
                           ((uint32_t)c->hdr_buf[1] << 16) |
                           ((uint32_t)c->hdr_buf[2] << 8)  |
                           ((uint32_t)c->hdr_buf[3]);

            c->frag_last = (raw & 0x80000000U) != 0;
            c->frag_remain = raw & 0x7FFFFFFFU;
            c->have_frag_hdr = true;
            c->hdr_pos = 0;

            if (c->frag_remain > RPC_MAX_RECORD_SIZE) {
                return -1;
}
            if (conn_ensure_buf(c, c->recv_len + c->frag_remain) != 0) {
                return -1;
}
        }

        /* Phase 2: read fragment body. */
        {
            uint32_t want = c->frag_remain;
            uint32_t pos  = 0;
            int br = nb_read(c->fd, c->recv_buf + c->recv_len,
                             &pos, want);
            c->recv_len    += pos;
            c->frag_remain -= pos;
            if (br <= 0) {
                return br;
            }
        }

        /* Fragment body complete. */
        c->have_frag_hdr = false;

        if (c->frag_last) {
            /* Full record assembled.
             *
             * RFC 8881 §2.10.3.1: the backchannel shares the
             * forechannel TCP connection.  Callback REPLY records
             * (msg_type=1) arrive interleaved with client CALL
             * records.  Peek at msg_type (bytes 4-7) and route
             * replies to the pending-CB-reply handler instead of
             * the COMPOUND dispatcher.  Without this demux,
             * rpc_decode_call_header rejects msg_type!=0 and
             * drops the connection. */
            if (c->recv_len >= 8) {
                uint32_t msg_type =
                    ((uint32_t)c->recv_buf[4] << 24) |
                    ((uint32_t)c->recv_buf[5] << 16) |
                    ((uint32_t)c->recv_buf[6] << 8)  |
                    ((uint32_t)c->recv_buf[7]);
                if (msg_type == 1) { /* RPC REPLY */
                    nfs4_cb_deliver_reply(c->recv_buf,
                                          c->recv_len);
                    c->recv_len = 0;
                    continue; /* Next record. */
                }
            }

            if (srv->tp != NULL) {
                return dispatch_to_pool(srv, c);
            }

            /* Inline path (no threadpool or tests). */
            int rc = process_rpc_record(srv, c,
                                        c->recv_buf, c->recv_len);

            c->recv_len = 0;
            if (rc != 0) {
                return -1;
}
            /* Continue reading -- there may be pipelined requests. */
        }
        /* Else: more fragments expected. Loop back to read next header. */
    }
}

/* -----------------------------------------------------------------------
 * Server lifecycle
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int rpc_server_create(const struct rpc_server_config *cfg,
                      struct rpc_server **out)
{
    struct rpc_server *srv = NULL;
    int listen_fd = -1;
    int epoll_fd = -1;
    struct sockaddr_in addr;
    int opt = 1;
    uint32_t max_conns;

    if (cfg == NULL || out == NULL) {
        return -1;
}
    *out = NULL;

    max_conns = cfg->max_conns ? cfg->max_conns : RPC_MAX_CONNS;

    srv = calloc(1, sizeof(*srv));
    if (srv == NULL) {
        return -1;
}

    srv->listen_fd = -1;
    srv->epoll_fd = -1;
    srv->stop_pipe[0] = -1;
    srv->stop_pipe[1] = -1;
    srv->max_conns = max_conns;
    srv->mds_id = cfg->mds_id;
    srv->stripe_unit = cfg->stripe_unit;
    srv->placement_policy = cfg->placement_policy;
    srv->placement_policy_enabled = cfg->placement_policy_enabled;
    srv->default_stripe_count = cfg->default_stripe_count ?
                                 cfg->default_stripe_count : 1;
    srv->default_mirror_count = cfg->default_mirror_count ?
                                 cfg->default_mirror_count : 1;
    srv->hpc_getattr_mode = cfg->hpc_getattr_mode;
    srv->hpc_max_stripe_count = cfg->hpc_max_stripe_count;
    srv->hpc_xdr_form = cfg->hpc_xdr_form;
    srv->write_verf = cfg->write_verf;
    srv->cat = cfg->cat;
    srv->st = cfg->st;
    srv->ot = cfg->ot;
    srv->lt = cfg->lt;
    srv->cq = cfg->cq;
    srv->ds_hm = cfg->ds_hm;
    srv->smap = cfg->smap;
    srv->membership = cfg->membership;
    srv->proxy = cfg->proxy;
    srv->hm = cfg->hm;
    srv->io_tracker = cfg->io_tracker;
    srv->quota = cfg->quota;
    srv->prealloc = cfg->prealloc;
    srv->ds_prepare = cfg->ds_prepare;
    srv->ds_cache = cfg->ds_cache;
    srv->icache = cfg->icache;
    srv->dcache = cfg->dcache;
    srv->lcache = cfg->lcache;
    srv->lcommit_agg = cfg->lcommit_agg;
    srv->dt = cfg->dt;
    srv->ddt = cfg->ddt;
    srv->lr = cfg->lr;
    srv->cot = cfg->cot;
    srv->transport = cfg->transport;
    srv->gpudirect_required = cfg->gpudirect_required;
    srv->skip_transient_ndb = cfg->skip_transient_ndb;
    srv->min_auth = cfg->min_auth;
    srv->gss_tbl = cfg->gss_tbl;
    srv->tp = cfg->tp;
    srv->shard_map = cfg->shard_map;
    srv->comp_pipe[0] = -1;
    srv->comp_pipe[1] = -1;

    /* Allocate connection array. */
    srv->conns = calloc(max_conns, sizeof(struct rpc_conn));
    /* fd-index: pre-allocate sparse array for O(1) fd lookup. */
    srv->fd_index_size = max_conns + 64; /* headroom for listen+epoll fds */
    srv->fd_to_conn = (struct rpc_conn **)calloc(srv->fd_index_size,
                              sizeof(struct rpc_conn *));
    srv->free_list = calloc(max_conns, sizeof(uint32_t));
    /* All three allocations must succeed.  find_conn_by_fd() dereferences
     * srv->fd_to_conn using the bounds check against fd_index_size (which
     * is set non-zero above), so a NULL fd_to_conn would NULL-deref on
     * the first accept() instead of being caught here. */
    if (srv->conns == NULL || srv->fd_to_conn == NULL ||
        srv->free_list == NULL) {
        goto fail;
    }
    for (uint32_t fi = 0; fi < max_conns; fi++) {
        srv->free_list[fi] = fi;
    }
    srv->free_top = max_conns;
    for (uint32_t i = 0; i < max_conns; i++) {
        conn_init(&srv->conns[i]);
    }

    /* Create listening socket. */
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        goto fail;
}
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR,
                   &opt, sizeof(opt)) < 0) {
        goto fail;
}
#ifdef SO_REUSEPORT
    /* Allow multiple listeners on the same port for multi-listener
     * mode (kernel distributes connections across listeners).
     * Failure is non-fatal -- single-listener still works. */
    (void)setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT,
                     &opt, sizeof(opt));
#endif

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg->port ? cfg->port : RPC_DEFAULT_PORT);
    if (cfg->bind_addr != NULL) {
        if (inet_pton(AF_INET, cfg->bind_addr, &addr.sin_addr) != 1) {
            goto fail;
}
    } else {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    }

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        goto fail;
}
    if (listen(listen_fd, 128) < 0) {
        goto fail;
}

    /* Retrieve actual port (useful for ephemeral port). */
    {
        struct sockaddr_in bound;
        socklen_t slen = sizeof(bound);

        if (getsockname(listen_fd, (struct sockaddr *)&bound, &slen) == 0) {
            /* NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage) */
            srv->port = ntohs(bound.sin_port);
}
    }

    if (set_nonblock(listen_fd) != 0) {
        goto fail;
}

    /* Create epoll instance. */
    epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) {
        goto fail;
}

    /* Add listen socket to epoll. */
    {
        struct epoll_event ev;

        ev.events = EPOLLIN;
        ev.data.fd = listen_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) {
            goto fail;
}
    }

    /* Create stop pipe. */
    if (pipe(srv->stop_pipe) < 0) {
        goto fail;
}
    if (set_nonblock(srv->stop_pipe[0]) != 0) {
        goto fail;
}
    {
        struct epoll_event ev;

        ev.events = EPOLLIN;
        ev.data.fd = srv->stop_pipe[0];
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, srv->stop_pipe[0],
                      &ev) < 0) {
            goto fail;
}
    }

    /* Create completion pipe for threadpool notifications.
     * Workers write the 4-byte conn index; the epoll thread
     * reads only the completed indices -- O(completed) not
     * O(max_conns). */
    if (srv->tp != NULL) {
        if (pipe(srv->comp_pipe) < 0) {
            goto fail;
        }
        if (set_nonblock(srv->comp_pipe[0]) != 0) {
            goto fail;
        }
        struct epoll_event wev;
        wev.events = EPOLLIN;
        wev.data.fd = srv->comp_pipe[0];
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD,
                      srv->comp_pipe[0], &wev) < 0) {
            goto fail;
        }
    }

    srv->listen_fd = listen_fd;
    srv->epoll_fd = epoll_fd;
    *out = srv;
    return 0;

fail:
    if (listen_fd >= 0) {
        close(listen_fd);
}
    if (epoll_fd >= 0) {
        close(epoll_fd);
}
    if (srv != NULL) {
        if (srv->stop_pipe[0] >= 0) {
            close(srv->stop_pipe[0]);
}
        if (srv->stop_pipe[1] >= 0) {
            close(srv->stop_pipe[1]);
}
        if (srv->comp_pipe[0] >= 0) {
            close(srv->comp_pipe[0]);
        }
        if (srv->comp_pipe[1] >= 0) {
            close(srv->comp_pipe[1]);
        }
        free(srv->free_list);
        free((void *)srv->fd_to_conn);
        free(srv->conns);
        free(srv);
    }
    return -1;
}

/** Find a free connection slot. O(1) via free-list. */
static struct rpc_conn *find_free_conn(struct rpc_server *srv)
{
    if (srv->free_list != NULL && srv->free_top > 0) {
        uint32_t idx = srv->free_list[--srv->free_top];
        return &srv->conns[idx];
    }
    return NULL;
}

/** Find connection by fd -- O(1) via fd-index. */
static struct rpc_conn *find_conn_by_fd(struct rpc_server *srv, int fd)
{
    if (fd < 0 || (uint32_t)fd >= srv->fd_index_size) {
        return NULL;
    }
    return srv->fd_to_conn[fd];
}

static void close_conn(struct rpc_server *srv, struct rpc_conn *c)
{
    if (c->fd >= 0) {
        /* Unbind backchannel before closing the connection. */
        if (srv->st != NULL) {
            session_unbind_conn(srv->st, c);
        }
        epoll_ctl(srv->epoll_fd, EPOLL_CTL_DEL, c->fd, NULL);
        /* Clear fd-index entry before resetting the conn. */
        if ((uint32_t)c->fd < srv->fd_index_size) {
            srv->fd_to_conn[c->fd] = NULL;
        }
        conn_reset(c);
        srv->conn_count--;
    }
}


/**
 * Handle a new connection on the listen socket.
 */
static void handle_epoll_accept(struct rpc_server *srv)
{
    struct sockaddr_in client_addr;
    socklen_t clen = sizeof(client_addr);
    int cfd = accept(srv->listen_fd,
                     (struct sockaddr *)&client_addr, &clen);

    if (cfd < 0) {
        return;
}

    struct rpc_conn *c = find_free_conn(srv);

    if (c == NULL) {
        close(cfd); /* At capacity. */
        return;
    }

    if (set_nonblock(cfd) != 0) {
        close(cfd);
        return;
    }

    /* Disable Nagle for low-latency RPC. */
    {
        int nodelay = 1;

        if (setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY,
                       &nodelay,
                       sizeof(nodelay)) != 0) {
            char errbuf[64];
            (void)strerror_r(errno, errbuf,
                             sizeof(errbuf));
            (void)fprintf(stderr,
                "WARN: TCP_NODELAY failed on "
                "fd %d: %s\n", cfd, errbuf);
        }
    }

    c->fd = cfd;
    /* Wire fd-index for O(1) lookup. */
    if ((uint32_t)cfd < srv->fd_index_size) {
        srv->fd_to_conn[cfd] = c;
    }
    srv->conn_count++;

    struct epoll_event ev;

    ev.events = EPOLLIN;
    ev.data.fd = cfd;
    if (epoll_ctl(srv->epoll_fd, EPOLL_CTL_ADD,
                  cfd, &ev) < 0) {
        close_conn(srv, c);
    }
}

/**
 * Handle worker-completion notifications from the completion pipe.
 *
 * Workers write their 4-byte connection index when done.  We read
 * all available indices in one batch and re-enable EPOLLIN on each
 * completed connection.  Cost: O(completed) instead of O(max_conns).
 */
static void handle_epoll_wakeup(struct rpc_server *srv)
{
    uint32_t buf[64];
    ssize_t n;

    while ((n = read(srv->comp_pipe[0], buf, sizeof(buf))) > 0) {
        uint32_t count = (uint32_t)n / sizeof(uint32_t);
        for (uint32_t i = 0; i < count; i++) {
            if (buf[i] >= srv->max_conns) {
                continue; /* Defensive: invalid index. */
            }
            struct rpc_conn *rc = &srv->conns[buf[i]];
            if (rc->fd < 0) {
                continue;
            }
            struct epoll_event rev;
            rev.events = EPOLLIN;
            pthread_mutex_lock(&rc->send_lock);
            if (rc->send_len > 0) {
                rev.events |= EPOLLOUT;
            }
            pthread_mutex_unlock(&rc->send_lock);
            rev.data.fd = rc->fd;
            epoll_ctl(srv->epoll_fd, EPOLL_CTL_MOD,
                      rc->fd, &rev);
        }
    }
}

/**
 * Handle data / HUP / EPOLLOUT on a client connection.
 */
static void handle_epoll_conn(struct rpc_server *srv,
                               const struct epoll_event *ev)
{
    struct rpc_conn *c = find_conn_by_fd(srv, ev->data.fd);

    if (c == NULL) {
        return;
}

    if (ev->events & (EPOLLERR | EPOLLHUP)) {
        if (!atomic_load(&c->busy)) {
            close_conn(srv, c);
        }
        return;
    }

    /* Drain send queue on EPOLLOUT (Finding 3). */
    if (ev->events & EPOLLOUT) {
        pthread_mutex_lock(&c->send_lock);
        int dr = send_queue_drain(c);
        int empty = (dr == 0 && c->send_len == 0);
        pthread_mutex_unlock(&c->send_lock);

        if (dr < 0) {
            close_conn(srv, c);
            return;
        }
        if (empty) {
            struct epoll_event emod;

            emod.events = EPOLLIN;
            emod.data.fd = c->fd;
            epoll_ctl(srv->epoll_fd, EPOLL_CTL_MOD,
                      c->fd, &emod);
        }
    }

    if (ev->events & EPOLLIN) {
        if (conn_read(srv, c) != 0) {
            close_conn(srv, c);
            return;
        }
    }

    /* If send_record queued data, register EPOLLOUT. */
    pthread_mutex_lock(&c->send_lock);
    int has_pending = (c->fd >= 0 && c->send_len > 0);
    pthread_mutex_unlock(&c->send_lock);
    if (has_pending) {
        struct epoll_event emod;

        emod.events = EPOLLIN | EPOLLOUT;
        emod.data.fd = c->fd;
        epoll_ctl(srv->epoll_fd, EPOLL_CTL_MOD,
                  c->fd, &emod);
    }
}

int rpc_server_start(struct rpc_server *srv)
{
    struct epoll_event events[64];

    if (srv == NULL) {
        return -1;
}

    atomic_store(&srv->running, 1);

    while (atomic_load(&srv->running)) {
        int nfds = epoll_wait(srv->epoll_fd, events, 64, 1000);

        if (nfds < 0) {
            if (errno == EINTR) {
                continue;
}
            return -1;
        }

        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;

            /* Stop pipe? */
            if (fd == srv->stop_pipe[0]) {
                atomic_store(&srv->running, 0);
                break;
            }

            if (fd == srv->comp_pipe[0]) {
                handle_epoll_wakeup(srv);
                continue;
            }

            if (fd == srv->listen_fd) {
                handle_epoll_accept(srv);
                continue;
            }

            handle_epoll_conn(srv, &events[i]);
        }
    }
    return 0;
}

void rpc_server_stop(struct rpc_server *srv)
{
    if (srv == NULL) {
        return;
}

    /* Signal the epoll loop to exit. */
    atomic_store(&srv->running, 0);
    if (srv->stop_pipe[1] >= 0) {
        uint8_t b = 1;
        (void)write(srv->stop_pipe[1], &b, 1);
    }

    /* Wait for any in-flight worker threads to finish.
     * Workers set busy=false and write to comp_pipe when done.
     * We spin-wait briefly (bounded by pool drain time). */
    if (srv->tp != NULL) {
        for (int attempt = 0; attempt < 3000; attempt++) {
            bool any_busy = false;
            for (uint32_t i = 0; i < srv->max_conns; i++) {
                if (atomic_load(&srv->conns[i].busy)) {
                    any_busy = true;
                    break;
                }
            }
            if (!any_busy) {
                break;
            }
            usleep(1000);  /* 1 ms */
        }
    }
}

void rpc_server_destroy(struct rpc_server *srv)
{
    if (srv == NULL) {
        return;
}

    for (uint32_t i = 0; i < srv->max_conns; i++) {
        if (srv->conns[i].fd >= 0) {
            conn_reset(&srv->conns[i]);
}
    }
    free(srv->conns);
    free((void *)srv->fd_to_conn);
    free(srv->free_list);

    if (srv->listen_fd >= 0) {
        close(srv->listen_fd);
}
    if (srv->epoll_fd >= 0) {
        close(srv->epoll_fd);
}
    if (srv->stop_pipe[0] >= 0) {
        close(srv->stop_pipe[0]);
}
    if (srv->stop_pipe[1] >= 0) {
        close(srv->stop_pipe[1]);
}
    if (srv->comp_pipe[0] >= 0) {
        close(srv->comp_pipe[0]);
    }
    if (srv->comp_pipe[1] >= 0) {
        close(srv->comp_pipe[1]);
    }

    free(srv);
}

uint16_t rpc_server_port(const struct rpc_server *srv)
{
    if (srv == NULL) {
        return 0;
}
    return srv->port;
}

/* -----------------------------------------------------------------------
 * Public accessor for opaque rpc_conn fd
 * ----------------------------------------------------------------------- */

int rpc_conn_get_fd(const struct rpc_conn *c)
{
    if (c == NULL) {
        return -1;
}
    return c->fd;
}

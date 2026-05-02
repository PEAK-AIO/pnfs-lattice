/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * nfs4_cb.c — NFSv4.1 callback channel sender.
 *
 * Sends CB_COMPOUND { CB_SEQUENCE, CB_LAYOUTRECALL } on the session's
 * backchannel (the same TCP connection the client used for its
 * forechannel, per RFC 8881 §2.10.3.1).
 */

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <poll.h>
#include <arpa/inet.h>
#include <stdatomic.h>
#include <rpc/xdr.h>

#include "nfs4_cb.h"
#include "rpc_server.h"  /* rpc_conn_get_fd */
#include "compound.h"   /* OP_CB_SEQUENCE, OP_CB_LAYOUTRECALL */
#include "xdr_codec.h"  /* xdr_nfs4_stateid_encode */

/* -----------------------------------------------------------------------
 * Constants
 * ----------------------------------------------------------------------- */

#define CB_COMPOUND_TAG     "pnfs-mds-cb"
#define CB_MINOR_VERSION    1
#define CB_DEFAULT_TIMEOUT_BUILTIN 5000  /* ms (compile-time fallback) */
#define CB_MAX_MSG_SIZE     4096

/*
 * Runtime-configurable default timeout.  Initialised to the
 * compile-time fallback and may be overridden via
 * nfs4_cb_set_default_timeout() at daemon startup from the
 * `cb_recall_timeout_ms` INI key.  Read with atomic_load so any
 * thread sees a recent value; write with atomic_store from main.
 */
static _Atomic uint32_t cb_default_timeout_ms = CB_DEFAULT_TIMEOUT_BUILTIN;

void nfs4_cb_set_default_timeout(uint32_t timeout_ms)
{
    if (timeout_ms < 50) {
        timeout_ms = 50; /* floor — see header */
    }
    atomic_store_explicit(&cb_default_timeout_ms, timeout_ms,
                          memory_order_relaxed);
}

static uint32_t cb_default_timeout(void)
{
    return atomic_load_explicit(&cb_default_timeout_ms,
                                memory_order_relaxed);
}

/* RPC constants. */
#define RPC_MSG_VERSION     2
#define RPC_CALL            0
#define RPC_REPLY           1
#define RPC_MSG_ACCEPTED    0
#define RPC_ACCEPT_SUCCESS  0
#define AUTH_NONE           0

static _Atomic uint32_t cb_xid_counter = 1;

/* -----------------------------------------------------------------------
 * Backchannel slot management
 * ----------------------------------------------------------------------- */

/**
 * Acquire the next available backchannel slot.
 * Returns slot index, or -1 if no slots available.
 * Advances the slot's seq_id for CB_SEQUENCE.
 */
static int cb_slot_acquire(struct nfs4_session *s)
{
    uint32_t i, start;

    if (s->cb_slots == NULL || s->num_cb_slots == 0) {
        return -1;
    }

    /* Round-robin across available backchannel slots. */
    start = s->cb_next_slot % s->num_cb_slots;
    for (i = 0; i < s->num_cb_slots; i++) {
        uint32_t idx = (start + i) % s->num_cb_slots;
        s->cb_next_slot = idx + 1;
        s->cb_slots[idx].seq_id++;
        return (int)idx;
    }
    return -1;
}

static void cb_slot_release(struct nfs4_session *s, int slot_idx)
{
    (void)s;
    (void)slot_idx;
    /* No-op for now — single-threaded sender, slot reuse is implicit. */
}

/* -----------------------------------------------------------------------
 * Wire I/O helpers
 * ----------------------------------------------------------------------- */

/**
 * Send a complete RPC record-marked message on fd.
 * Returns 0 on success, -1 on error.
 */
static int send_record(int fd, const uint8_t *data, uint32_t len)
{
    uint32_t frag_hdr = htonl(len | 0x80000000u); /* last fragment */
    struct iovec iov[2];

    iov[0].iov_base = &frag_hdr;
    iov[0].iov_len = 4;
    iov[1].iov_base = (void *)data;
    iov[1].iov_len = len;

    struct msghdr msg;

    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = iov;
    msg.msg_iovlen = 2;

    ssize_t sent = sendmsg(fd, &msg, MSG_NOSIGNAL);

    if (sent < 0 || (uint32_t)sent != 4 + len) {
        return -1;
}
    return 0;
}

/**
 * Read a complete RPC record-marked message.
 * Caller must free(*out).  Returns 0 on success.
 */
static int recv_record(int fd, uint32_t timeout_ms,
                       uint8_t **out, uint32_t *out_len)
{
    struct pollfd pfd;
    uint8_t hdr[4];
    ssize_t n;

    pfd.fd = fd;
    pfd.events = POLLIN;

    /* Read 4-byte fragment header. */
    if (poll(&pfd, 1, (int)timeout_ms) <= 0) {
        return -ETIMEDOUT;
}
    n = recv(fd, hdr, 4, MSG_WAITALL);
    if (n != 4) {
        return -EIO;
}

    uint32_t raw = ntohl(*(uint32_t *)hdr);
    uint32_t frag_len = raw & 0x7FFFFFFFu;

    if (frag_len == 0 || frag_len > CB_MAX_MSG_SIZE) {
        return -EIO;
}

    uint8_t *buf = malloc(frag_len);

    if (buf == NULL) {
        return -ENOMEM;
}

    if (poll(&pfd, 1, (int)timeout_ms) <= 0) {
        free(buf);
        return -ETIMEDOUT;
    }
    n = recv(fd, buf, frag_len, MSG_WAITALL);
    if (n < 0 || (uint32_t)n != frag_len) {
        free(buf);
        return -EIO;
    }

    *out = buf;
    *out_len = frag_len;
    return 0;
}

/* -----------------------------------------------------------------------
 * XDR encoding: CB_COMPOUND { CB_SEQUENCE, CB_LAYOUTRECALL }
 * ----------------------------------------------------------------------- */

static bool encode_rpc_call_header(XDR *xdrs, uint32_t xid,
                                   uint32_t prog, uint32_t proc)
{
    uint32_t v;

    v = xid;           if (!xdr_uint32_t(xdrs, &v)) { return false;
}
    v = RPC_CALL;      if (!xdr_uint32_t(xdrs, &v)) { return false;
}
    v = RPC_MSG_VERSION; if (!xdr_uint32_t(xdrs, &v)) { return false;
}
    v = prog;          if (!xdr_uint32_t(xdrs, &v)) { return false;
}
    v = CB_MINOR_VERSION; if (!xdr_uint32_t(xdrs, &v)) { return false;
}
    v = proc;          if (!xdr_uint32_t(xdrs, &v)) { return false;
}
    /* Credential: AUTH_SYS (flavor=1, uid=0, gid=0). */
    v = 1; /* AUTH_SYS */
    if (!xdr_uint32_t(xdrs, &v)) { return false;
}
    {
        /* AUTH_SYS body: stamp + machinename + uid + gid + gids[] */
        uint32_t body_len = 20; /* 4+4+0+4+4+4 */
        uint32_t stamp = 0, mname_len = 0, uid = 0, gid = 0, ngids = 0;
        if (!xdr_uint32_t(xdrs, &body_len)) { return false; }
        if (!xdr_uint32_t(xdrs, &stamp)) { return false; }
        if (!xdr_uint32_t(xdrs, &mname_len)) { return false; }
        if (!xdr_uint32_t(xdrs, &uid)) { return false; }
        if (!xdr_uint32_t(xdrs, &gid)) { return false; }
        if (!xdr_uint32_t(xdrs, &ngids)) { return false; }
    }
    /* Verifier: AUTH_NONE (flavor=0, body_len=0). */
    v = AUTH_NONE;     if (!xdr_uint32_t(xdrs, &v)) { return false;
}
    v = 0;             if (!xdr_uint32_t(xdrs, &v)) { return false;
}
    return true;
}

static bool encode_cb_compound_header(XDR *xdrs, uint32_t op_count)
{
    /* tag (opaque<>) */
    uint32_t tag_len = (uint32_t)strlen(CB_COMPOUND_TAG);

    if (!xdr_uint32_t(xdrs, &tag_len)) {
        return false;
}
    if (!xdr_opaque(xdrs, (char *)CB_COMPOUND_TAG, tag_len)) {
        return false;
}

    /* minorversion */
    uint32_t minor = CB_MINOR_VERSION;

    if (!xdr_uint32_t(xdrs, &minor)) {
        return false;
}

    /* callback_ident (RFC 8881 §2.10.3.1 — 0 for NFSv4.1+) */
    uint32_t cb_ident = 0;

    if (!xdr_uint32_t(xdrs, &cb_ident)) {
        return false;
}

    /* argarray count */
    uint32_t count = op_count;

    if (!xdr_uint32_t(xdrs, &count)) {
        return false;
}

    return true;
}

static bool encode_cb_sequence(XDR *xdrs,
                               const uint8_t session_id[SESSION_ID_SIZE],
                               uint32_t slot_id,
                               uint32_t seq_id,
                               uint32_t highest_slot)
{
    uint32_t opnum = OP_CB_SEQUENCE;

    if (!xdr_uint32_t(xdrs, &opnum)) {
        return false;
}
    if (!xdr_opaque(xdrs, (char *)session_id, SESSION_ID_SIZE)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &seq_id)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &slot_id)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &highest_slot)) {
        return false;
}

    /* cachethis = false */
    uint32_t cachethis = 0;

    if (!xdr_uint32_t(xdrs, &cachethis)) {
        return false;
}

    /* referring_call_lists<> — empty */
    uint32_t ref_count = 0;

    if (!xdr_uint32_t(xdrs, &ref_count)) {
        return false;
}

    return true;
}

static bool encode_cb_recall(XDR *xdrs,
                             const struct nfs4_cb_recall_args *a)
{
    /* CB_RECALL4args per RFC 8881 §20.2:
     *   struct CB_RECALL4args {
     *       stateid4 stateid;
     *       bool     truncate;
     *       nfs_fh4  fh;
     *   };
     */
    uint32_t opnum = OP_CB_RECALL;

    if (!xdr_uint32_t(xdrs, &opnum)) {
        return false;
    }
    if (!xdr_nfs4_stateid_encode(xdrs, &a->stateid)) {
        return false;
    }

    uint32_t truncate = a->truncate ? 1u : 0u;

    if (!xdr_uint32_t(xdrs, &truncate)) {
        return false;
    }

    /* nfs_fh4 is opaque<NFS4_FHSIZE>; we use the 8-byte fileid
     * in network byte order, matching encode_cb_layoutrecall. */
    uint64_t fid_be = htobe64(a->fileid);
    uint32_t fh_len = 8;

    if (!xdr_uint32_t(xdrs, &fh_len)) {
        return false;
    }
    if (!xdr_opaque(xdrs, (char *)&fid_be, fh_len)) {
        return false;
    }

    return true;
}

/*
 * Encode one notify4 struct for CB_NOTIFY (RFC 8881 §20.4, §3.3.15).
 *
 * We emit exactly one notify4 per nfs4_cb_notify() call.  The
 * notify_mask is a 1-element bitmap with the bit for notify_type set;
 * the notify_vals opaque blob contains the per-type payload:
 *
 *   NOTIFY4_REMOVE_ENTRY      -> component4 old_name + uint64 prev_cookie
 *   NOTIFY4_ADD_ENTRY         -> component4 old_name (empty) + cookie
 *                                + new_entry4 { component4 new_name +
 *                                  cookie + entry_attrs_empty } +
 *                                + bool last_entry + uint64 prev_cookie
 *   NOTIFY4_RENAME_ENTRY      -> old_entry4 (old_name, 0) + new_entry4
 *                                (new_name, 0, empty attrs)
 *
 * Phase 8c does not advertise attrs, so the fattr4 inside entry4 is
 * an empty bitmap + zero-length opaque.  Phase 8d replaces the empty
 * attrs with real payloads per the GDD-negotiated bitmap.
 */
static bool encode_component4(XDR *xdrs, const char *name, uint32_t name_len)
{
    uint32_t len = name_len;
    if (!xdr_uint32_t(xdrs, &len)) { return false; }
    if (len > 0) {
        if (!xdr_opaque(xdrs, (char *)name, len)) { return false; }
    }
    return true;
}

static bool encode_empty_fattr4(XDR *xdrs)
{
    /* bitmap4 attr_mask<>: 0 words */
    uint32_t zero = 0;
    if (!xdr_uint32_t(xdrs, &zero)) { return false; }
    /* attrlist4 attr_vals<>: 0 bytes */
    if (!xdr_uint32_t(xdrs, &zero)) { return false; }
    return true;
}

static bool encode_entry4(XDR *xdrs, const char *name, uint32_t name_len,
                          uint64_t cookie)
{
    /*
     * entry4 per RFC 8881 §3.3.15 notify_entry4: name + cookie +
     * attrs (fattr4).  Phase 8c emits empty attrs.
     */
    if (!encode_component4(xdrs, name, name_len)) { return false; }
    if (!xdr_uint64_t(xdrs, &cookie)) { return false; }
    if (!encode_empty_fattr4(xdrs)) { return false; }
    return true;
}

/* notify_remove4 payload (RFC 8881 §3.3.15). */
static bool encode_notify_remove_body(XDR *sub,
                                      const struct nfs4_cb_notify_args *a)
{
    uint64_t cookie = 0;
    return encode_component4(sub, a->old_name, a->old_name_len) &&
           xdr_uint64_t(sub, &cookie);
}

/* notify_add4 payload (optional-old-entry empty, new_entry4, two optional bools). */
static bool encode_notify_add_body(XDR *sub,
                                   const struct nfs4_cb_notify_args *a)
{
    uint32_t nopt_old = 0;
    uint32_t nopt_last = 0;
    uint32_t nopt_prev = 0;

    return xdr_uint32_t(sub, &nopt_old) &&
           encode_entry4(sub, a->new_name, a->new_name_len, 0) &&
           xdr_uint32_t(sub, &nopt_last) &&
           xdr_uint32_t(sub, &nopt_prev);
}

/* notify_rename4 payload (old_entry4 + new_entry4). */
static bool encode_notify_rename_body(XDR *sub,
                                      const struct nfs4_cb_notify_args *a)
{
    return encode_entry4(sub, a->old_name, a->old_name_len, 0) &&
           encode_entry4(sub, a->new_name, a->new_name_len, 0);
}

/*
 * Emit the per-type notify_vals opaque.  Returns false on failure;
 * caller owns xdr_destroy on @sub regardless.
 */
static bool encode_notify_body(XDR *sub,
                               const struct nfs4_cb_notify_args *a)
{
    switch (a->notify_type) {
    case NOTIFY4_REMOVE_ENTRY: return encode_notify_remove_body(sub, a);
    case NOTIFY4_ADD_ENTRY:    return encode_notify_add_body(sub, a);
    case NOTIFY4_RENAME_ENTRY: return encode_notify_rename_body(sub, a);
    default:                   return false;
    }
}

static bool encode_cb_notify(XDR *xdrs,
                             const struct nfs4_cb_notify_args *a)
{
    uint32_t opnum = OP_CB_NOTIFY;
    uint32_t fh_len = 8;
    uint64_t fid_be = htobe64(a->dir_fileid);
    uint32_t nchanges = 1;
    uint32_t bm_words = 1;
    uint32_t mask = (uint32_t)1u << a->notify_type;
    char staging[1024];
    XDR sub;
    uint32_t sub_len;
    bool ok;

    if (!xdr_uint32_t(xdrs, &opnum)) { return false; }
    if (!xdr_nfs4_stateid_encode(xdrs, &a->stateid)) { return false; }
    if (!xdr_uint32_t(xdrs, &fh_len)) { return false; }
    if (!xdr_opaque(xdrs, (char *)&fid_be, fh_len)) { return false; }
    if (!xdr_uint32_t(xdrs, &nchanges)) { return false; }
    if (!xdr_uint32_t(xdrs, &bm_words)) { return false; }
    if (!xdr_uint32_t(xdrs, &mask)) { return false; }

    xdrmem_create(&sub, staging, sizeof(staging), XDR_ENCODE);
    ok = encode_notify_body(&sub, a);
    sub_len = xdr_getpos(&sub);
    xdr_destroy(&sub);
    if (!ok) { return false; }

    if (!xdr_uint32_t(xdrs, &sub_len)) { return false; }
    if (sub_len > 0) {
        if (!xdr_opaque(xdrs, staging, sub_len)) { return false; }
    }
    return true;
}

static bool encode_cb_layoutrecall(XDR *xdrs,
                                   const struct nfs4_cb_layoutrecall_args *a)
{
    uint32_t opnum = OP_CB_LAYOUTRECALL;

    if (!xdr_uint32_t(xdrs, &opnum)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, (uint32_t *)&a->layout_type)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, (uint32_t *)&a->iomode)) {
        return false;
}

    uint32_t changed = 1; /* layoutchanged = true */

    if (!xdr_uint32_t(xdrs, &changed)) {
        return false;
}

    uint32_t recall_type = (uint32_t)a->recall_type;

    if (!xdr_uint32_t(xdrs, &recall_type)) {
        return false;
}

    if (a->recall_type == LAYOUTRECALL4_FILE) {
        /* fh4 as opaque<NFS4_FHSIZE> — we encode fileid as 8-byte FH */
        uint64_t fid_be = htobe64(a->fileid);
        uint32_t fh_len = 8;

        if (!xdr_uint32_t(xdrs, &fh_len)) {
            return false;
}
        if (!xdr_opaque(xdrs, (char *)&fid_be, fh_len)) {
            return false;
}

        if (!xdr_uint64_t(xdrs, (uint64_t *)&a->offset)) {
            return false;
}
        if (!xdr_uint64_t(xdrs, (uint64_t *)&a->length)) {
            return false;
}
        if (!xdr_nfs4_stateid_encode(xdrs, &a->stateid)) {
            return false;
}
    }
    /* FSID and ALL have no additional fields for our use. */

    return true;
}

/* -----------------------------------------------------------------------
 * Decode CB_COMPOUND reply
 * ----------------------------------------------------------------------- */

/**
 * Minimal decode of CB_COMPOUND reply.
 * Returns the status of the first failed op, or 0 on all-success.
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static int decode_cb_compound_reply(const uint8_t *buf, uint32_t len)
{
    XDR xdrs;
    uint32_t xid, msg_type, reply_stat, accept_stat, v;

    xdrmem_create(&xdrs, (char *)buf, len, XDR_DECODE);

    /* RPC reply header. */
    if (!xdr_uint32_t(&xdrs, &xid)) { goto err;
}
    if (!xdr_uint32_t(&xdrs, &msg_type)) { goto err;
}
    if (msg_type != RPC_REPLY) { goto err;
}
    if (!xdr_uint32_t(&xdrs, &reply_stat)) { goto err;
}
    if (reply_stat != RPC_MSG_ACCEPTED) { goto err;
}

    /* Auth verifier (skip). */
    if (!xdr_uint32_t(&xdrs, &v)) { goto err; /* flavor */
}
    if (!xdr_uint32_t(&xdrs, &v)) { goto err; /* body len */
}

    if (!xdr_uint32_t(&xdrs, &accept_stat)) { goto err;
}
    if (accept_stat != RPC_ACCEPT_SUCCESS) { goto err;
}

    /* CB_COMPOUND reply: status, tag, resarray. */
    uint32_t status;

    if (!xdr_uint32_t(&xdrs, &status)) { goto err;
}
    if (status != 0) {
        xdr_destroy(&xdrs);
        return (int)status;
    }

    /* Skip tag. */
    uint32_t tag_len;

    if (!xdr_uint32_t(&xdrs, &tag_len)) { goto err;
}
    /* Skip tag bytes. */
    {
        char skip[256];
        uint32_t padded = (tag_len + 3) & ~3u;

        if (padded > sizeof(skip)) { goto err;
}
        if (!xdr_opaque(&xdrs, skip, padded)) { goto err;
}
    }

    /* Skip result array — overall status already checked. */
    xdr_destroy(&xdrs);
    return 0;

err:
    xdr_destroy(&xdrs);
    return -EIO;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int nfs4_cb_layoutrecall(struct nfs4_session *session,
                         const struct nfs4_cb_layoutrecall_args *args,
                         uint32_t timeout_ms)
{
    char msg_buf[CB_MAX_MSG_SIZE];
    XDR xdrs;
    uint32_t xid;
    int slot_idx;
    int fd;
    uint8_t *reply = NULL;
    uint32_t reply_len = 0;
    int rc;

    if (session == NULL || args == NULL) {
        return -EINVAL;
}

    if (session->cb_conn == NULL) {
        return -ENOTCONN;
}

    if (timeout_ms == 0) {
        timeout_ms = cb_default_timeout();
}

    fd = rpc_conn_get_fd(session->cb_conn);
    if (fd < 0) {
        return -ENOTCONN;
}

    /* Callbacks support AUTH_NONE and AUTH_SYS.  The RPC header
     * uses AUTH_NONE for the credential body; the security flavor
     * negotiation occurs at session creation, not per-callback. */

    /* Acquire a backchannel slot. */
    slot_idx = cb_slot_acquire(session);
    if (slot_idx < 0) {
        return -EAGAIN;
}

    xid = atomic_fetch_add(&cb_xid_counter, 1);

    /* Encode the full RPC message. */
    xdrmem_create(&xdrs, msg_buf, sizeof(msg_buf), XDR_ENCODE);

    if (!encode_rpc_call_header(&xdrs, xid, session->cb_prog, 1)) {
        rc = -EIO;
        goto out;
    }
    if (!encode_cb_compound_header(&xdrs, 2)) { /* CB_SEQUENCE + CB_LAYOUTRECALL */
        rc = -EIO;
        goto out;
    }
    if (!encode_cb_sequence(&xdrs, session->session_id,
                            (uint32_t)slot_idx,
                            session->cb_slots[slot_idx].seq_id,
                            session->num_cb_slots - 1)) {
        rc = -EIO;
        goto out;
    }
    if (!encode_cb_layoutrecall(&xdrs, args)) {
        rc = -EIO;
        goto out;
    }

    uint32_t msg_len = xdr_getpos(&xdrs);

    xdr_destroy(&xdrs);

    /* Send. */
    rc = send_record(fd, (uint8_t *)msg_buf, msg_len);
    if (rc != 0) {
        rc = -EIO;
        goto out;
    }

    /* Receive reply. */
    rc = recv_record(fd, timeout_ms, &reply, &reply_len);
    if (rc != 0) {
        goto out;
}

    /* Decode reply. */
    rc = decode_cb_compound_reply(reply, reply_len);

out:
    free(reply);
    cb_slot_release(session, slot_idx);
    return rc;
}

int nfs4_cb_notify(struct nfs4_session *session,
                   const struct nfs4_cb_notify_args *args,
                   uint32_t timeout_ms)
{
    char msg_buf[CB_MAX_MSG_SIZE];
    XDR xdrs;
    uint32_t xid;
    int slot_idx;
    int fd;
    uint8_t *reply = NULL;
    uint32_t reply_len = 0;
    int rc;

    if (session == NULL || args == NULL) {
        return -EINVAL;
    }
    if (args->notify_type != NOTIFY4_REMOVE_ENTRY &&
        args->notify_type != NOTIFY4_ADD_ENTRY &&
        args->notify_type != NOTIFY4_RENAME_ENTRY) {
        return -EINVAL;
    }
    if (session->cb_conn == NULL) {
        return -ENOTCONN;
    }
    if (timeout_ms == 0) {
        timeout_ms = cb_default_timeout();
    }

    fd = rpc_conn_get_fd(session->cb_conn);
    if (fd < 0) {
        return -ENOTCONN;
    }

    slot_idx = cb_slot_acquire(session);
    if (slot_idx < 0) {
        return -EAGAIN;
    }

    xid = atomic_fetch_add(&cb_xid_counter, 1);

    xdrmem_create(&xdrs, msg_buf, sizeof(msg_buf), XDR_ENCODE);

    if (!encode_rpc_call_header(&xdrs, xid, session->cb_prog, 1)) {
        rc = -EIO;
        goto out;
    }
    if (!encode_cb_compound_header(&xdrs, 2)) {
        rc = -EIO;
        goto out;
    }
    if (!encode_cb_sequence(&xdrs, session->session_id,
                            (uint32_t)slot_idx,
                            session->cb_slots[slot_idx].seq_id,
                            session->num_cb_slots - 1)) {
        rc = -EIO;
        goto out;
    }
    if (!encode_cb_notify(&xdrs, args)) {
        rc = -EIO;
        goto out;
    }

    uint32_t msg_len = xdr_getpos(&xdrs);

    xdr_destroy(&xdrs);

    rc = send_record(fd, (uint8_t *)msg_buf, msg_len);
    if (rc != 0) {
        rc = -EIO;
        goto out;
    }

    rc = recv_record(fd, timeout_ms, &reply, &reply_len);
    if (rc != 0) {
        goto out;
    }

    rc = decode_cb_compound_reply(reply, reply_len);

out:
    free(reply);
    cb_slot_release(session, slot_idx);
    return rc;
}

int nfs4_cb_notify_fd(int fd,
                      const uint8_t session_id[SESSION_ID_SIZE],
                      uint32_t cb_prog,
                      uint32_t slot_seq_id,
                      uint32_t num_cb_slots,
                      const struct nfs4_cb_notify_args *args,
                      uint32_t timeout_ms)
{
    char msg_buf[CB_MAX_MSG_SIZE];
    XDR xdrs;
    uint32_t xid;
    uint8_t *reply = NULL;
    uint32_t reply_len = 0;
    int rc;

    if (fd < 0 || args == NULL || session_id == NULL) {
        return -EINVAL;
    }
    if (args->notify_type != NOTIFY4_REMOVE_ENTRY &&
        args->notify_type != NOTIFY4_ADD_ENTRY &&
        args->notify_type != NOTIFY4_RENAME_ENTRY) {
        return -EINVAL;
    }
    if (timeout_ms == 0) {
        timeout_ms = cb_default_timeout();
    }

    xid = atomic_fetch_add(&cb_xid_counter, 1);

    xdrmem_create(&xdrs, msg_buf, sizeof(msg_buf), XDR_ENCODE);

    if (!encode_rpc_call_header(&xdrs, xid, cb_prog, 1)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }
    if (!encode_cb_compound_header(&xdrs, 2)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }

    uint32_t highest_slot = (num_cb_slots > 0) ? num_cb_slots - 1 : 0;

    if (!encode_cb_sequence(&xdrs, session_id, 0,
                            slot_seq_id, highest_slot)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }
    if (!encode_cb_notify(&xdrs, args)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }

    uint32_t msg_len = xdr_getpos(&xdrs);

    xdr_destroy(&xdrs);

    rc = send_record(fd, (uint8_t *)msg_buf, msg_len);
    if (rc != 0) {
        return -EIO;
    }

    rc = recv_record(fd, timeout_ms, &reply, &reply_len);
    if (rc != 0) {
        return rc;
    }

    rc = decode_cb_compound_reply(reply, reply_len);
    free(reply);
    return rc;
}

int nfs4_cb_recall(struct nfs4_session *session,
                   const struct nfs4_cb_recall_args *args,
                   uint32_t timeout_ms)
{
    char msg_buf[CB_MAX_MSG_SIZE];
    XDR xdrs;
    uint32_t xid;
    int slot_idx;
    int fd;
    uint8_t *reply = NULL;
    uint32_t reply_len = 0;
    int rc;

    if (session == NULL || args == NULL) {
        return -EINVAL;
    }
    if (session->cb_conn == NULL) {
        return -ENOTCONN;
    }
    if (timeout_ms == 0) {
        timeout_ms = cb_default_timeout();
    }

    fd = rpc_conn_get_fd(session->cb_conn);
    if (fd < 0) {
        return -ENOTCONN;
    }

    slot_idx = cb_slot_acquire(session);
    if (slot_idx < 0) {
        return -EAGAIN;
    }

    xid = atomic_fetch_add(&cb_xid_counter, 1);

    xdrmem_create(&xdrs, msg_buf, sizeof(msg_buf), XDR_ENCODE);

    if (!encode_rpc_call_header(&xdrs, xid, session->cb_prog, 1)) {
        rc = -EIO;
        goto out;
    }
    if (!encode_cb_compound_header(&xdrs, 2)) { /* CB_SEQUENCE + CB_RECALL */
        rc = -EIO;
        goto out;
    }
    if (!encode_cb_sequence(&xdrs, session->session_id,
                            (uint32_t)slot_idx,
                            session->cb_slots[slot_idx].seq_id,
                            session->num_cb_slots - 1)) {
        rc = -EIO;
        goto out;
    }
    if (!encode_cb_recall(&xdrs, args)) {
        rc = -EIO;
        goto out;
    }

    uint32_t msg_len = xdr_getpos(&xdrs);

    xdr_destroy(&xdrs);

    rc = send_record(fd, (uint8_t *)msg_buf, msg_len);
    if (rc != 0) {
        rc = -EIO;
        goto out;
    }

    rc = recv_record(fd, timeout_ms, &reply, &reply_len);
    if (rc != 0) {
        goto out;
    }

    rc = decode_cb_compound_reply(reply, reply_len);

out:
    free(reply);
    cb_slot_release(session, slot_idx);
    return rc;
}

/* -----------------------------------------------------------------------
 * fd-based CB_LAYOUTRECALL (no session/rpc_conn dependency)
 * ----------------------------------------------------------------------- */

int nfs4_cb_layoutrecall_fd(int fd,
                            const uint8_t session_id[SESSION_ID_SIZE],
                            uint32_t cb_prog,
                            uint32_t slot_seq_id,
                            uint32_t num_cb_slots,
                            const struct nfs4_cb_layoutrecall_args *args,
                            uint32_t timeout_ms)
{
    char msg_buf[CB_MAX_MSG_SIZE];
    XDR xdrs;
    uint32_t xid;
    uint8_t *reply = NULL;
    uint32_t reply_len = 0;
    int rc;

    if (fd < 0 || args == NULL || session_id == NULL) {
        return -EINVAL;
    }

    if (timeout_ms == 0) {
        timeout_ms = cb_default_timeout();
    }

    xid = atomic_fetch_add(&cb_xid_counter, 1);

    xdrmem_create(&xdrs, msg_buf, sizeof(msg_buf), XDR_ENCODE);

    if (!encode_rpc_call_header(&xdrs, xid, cb_prog, 1)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }
    if (!encode_cb_compound_header(&xdrs, 2)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }

    uint32_t highest_slot = (num_cb_slots > 0) ? num_cb_slots - 1 : 0;

    if (!encode_cb_sequence(&xdrs, session_id, 0,
                            slot_seq_id, highest_slot)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }
    if (!encode_cb_layoutrecall(&xdrs, args)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }

    uint32_t msg_len = xdr_getpos(&xdrs);

    xdr_destroy(&xdrs);

    rc = send_record(fd, (uint8_t *)msg_buf, msg_len);
    if (rc != 0) {
        return -EIO;
    }

    rc = recv_record(fd, timeout_ms, &reply, &reply_len);
    if (rc != 0) {
        return rc;
    }

    rc = decode_cb_compound_reply(reply, reply_len);
    free(reply);
    return rc;
}

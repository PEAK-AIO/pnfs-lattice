/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * nfs4_cb.c -- NFSv4.1 callback channel sender.
 *
 * Sends CB_COMPOUND { CB_SEQUENCE, CB_LAYOUTRECALL } on the session's
 * backchannel (the same TCP connection the client used for its
 * forechannel, per RFC 8881 S2.10.3.1).
 */

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <poll.h>
#include <arpa/inet.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>
#include <rpc/xdr.h>

#include "nfs4_cb.h"
#include "rpc_server.h"  /* rpc_conn_get_fd */
#include "compound.h"   /* OP_CB_SEQUENCE, OP_CB_LAYOUTRECALL */
#include "xdr_codec.h"  /* xdr_nfs4_stateid_encode */

/* -----------------------------------------------------------------------
 * Constants
 * ----------------------------------------------------------------------- */

#define CB_COMPOUND_TAG     "pnfs-mds-cb"
/*
 * RFC 8881 S2.10.5.2 / S20.1 -- callback program version registered
 * with the client via CREATE_SESSION's cb_program is hardcoded to 1
 * for both NFSv4.1 and NFSv4.2 (the program version on the wire is
 * separate from the protocol minorversion carried inside
 * CB_COMPOUND4args).  Pre-NFSv4.1 used a different program/version
 * scheme tied to SETCLIENTID; that path is not implemented.
 */
#define CB_PROG_VERSION     1
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
        timeout_ms = 50; /* floor -- see header */
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
    /* No-op for now -- single-threaded sender, slot reuse is implicit. */
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

/* -----------------------------------------------------------------------
 * XDR encoding: CB_COMPOUND { CB_SEQUENCE, CB_LAYOUTRECALL }
 * ----------------------------------------------------------------------- */

/*
 * RFC 8881 S2.10.8.3 / RFC 5531 S9 -- emit the RPC credential per the
 * captured callback security parameters.  AUTH_NONE (the RFC 8881
 * default for callbacks) emits flavor=0 with an empty (length-zero)
 * body.  AUTH_SYS emits flavor=1 with the authsys_parms tuple
 * (stamp, machinename, uid, gid, gids<16>) length-prefixed by the
 * total body size.  RPCSEC_GSS is not yet implemented; any other
 * flavor falls back to AUTH_NONE so the encoder always emits a
 * valid RPC header.
 *
 * @param sec  Captured parms (NULL is treated as AUTH_NONE for
 *             back-compat with code paths that never populated it).
 */
static bool encode_rpc_cred(XDR *xdrs, const struct nfs4_cb_sec *sec)
{
    uint32_t flavor = (sec != NULL) ? sec->flavor : NFS4_CB_AUTH_NONE;

    if (flavor != NFS4_CB_AUTH_SYS) {
        flavor = NFS4_CB_AUTH_NONE;
    }
    {
        uint32_t flavor_w = flavor;
        if (!xdr_uint32_t(xdrs, &flavor_w)) {
            return false;
        }
    }
    if (flavor == NFS4_CB_AUTH_NONE) {
        uint32_t body_len = 0;
        return xdr_uint32_t(xdrs, &body_len);
    }
    /* AUTH_SYS body. */
    uint32_t mname_len = sec->sys_machname_len;
    if (mname_len > NFS4_CB_MACHNAME_MAX) {
        mname_len = NFS4_CB_MACHNAME_MAX;
    }
    uint32_t mname_padded = (mname_len + 3) & ~3u;
    uint32_t ngids = sec->sys_ngids;
    if (ngids > NFS4_CB_AUX_GIDS_MAX) {
        ngids = NFS4_CB_AUX_GIDS_MAX;
    }
    /* body = stamp(4) + mname_len(4) + mname_padded + uid(4) + gid(4)
     *      + ngids(4) + ngids*4. */
    uint32_t body_len = 4 + 4 + mname_padded + 4 + 4 + 4 + (ngids * 4);
    uint32_t stamp = sec->sys_stamp;
    uint32_t uid = sec->sys_uid;
    uint32_t gid = sec->sys_gid;
    uint32_t mname_len_w = mname_len;
    uint32_t ngids_w = ngids;

    if (!xdr_uint32_t(xdrs, &body_len)) { return false; }
    if (!xdr_uint32_t(xdrs, &stamp)) { return false; }
    if (!xdr_uint32_t(xdrs, &mname_len_w)) { return false; }
    if (mname_len > 0) {
        if (!xdr_opaque_encode(xdrs, sec->sys_machname, mname_len)) {
            return false;
        }
    }
    if (!xdr_uint32_t(xdrs, &uid)) { return false; }
    if (!xdr_uint32_t(xdrs, &gid)) { return false; }
    if (!xdr_uint32_t(xdrs, &ngids_w)) { return false; }
    for (uint32_t i = 0; i < ngids; i++) {
        uint32_t g = sec->sys_gids[i];
        if (!xdr_uint32_t(xdrs, &g)) { return false; }
    }
    return true;
}

static bool encode_rpc_call_header(XDR *xdrs, uint32_t xid,
                                   uint32_t prog, uint32_t proc,
                                   const struct nfs4_cb_sec *sec)
{
    uint32_t v;

    v = xid;             if (!xdr_uint32_t(xdrs, &v)) { return false; }
    v = RPC_CALL;        if (!xdr_uint32_t(xdrs, &v)) { return false; }
    v = RPC_MSG_VERSION; if (!xdr_uint32_t(xdrs, &v)) { return false; }
    v = prog;            if (!xdr_uint32_t(xdrs, &v)) { return false; }
    v = CB_PROG_VERSION; if (!xdr_uint32_t(xdrs, &v)) { return false; }
    v = proc;            if (!xdr_uint32_t(xdrs, &v)) { return false; }
    /* Credential: per session sec parms (AUTH_NONE / AUTH_SYS). */
    if (!encode_rpc_cred(xdrs, sec)) { return false; }
    /* Verifier: AUTH_NONE (flavor=0, body_len=0).  Per RFC 5531
     * S9.5 the verifier is AUTH_NONE for AUTH_SYS calls. */
    v = AUTH_NONE;     if (!xdr_uint32_t(xdrs, &v)) { return false; }
    v = 0;             if (!xdr_uint32_t(xdrs, &v)) { return false; }
    return true;
}

static bool encode_cb_compound_header(XDR *xdrs, uint32_t minorversion,
                                      uint32_t op_count)
{
    /* tag (opaque<>) */
    uint32_t tag_len = (uint32_t)strlen(CB_COMPOUND_TAG);

    if (!xdr_uint32_t(xdrs, &tag_len)) {
        return false;
}
    if (!xdr_opaque(xdrs, (char *)CB_COMPOUND_TAG, tag_len)) {
        return false;
}

    /*
     * RFC 8881 S20.1 / RFC 7862 S20.1 -- minorversion in CB_COMPOUND4args
     * MUST match the session's negotiated minorversion (1 for v4.1,
     * 2 for v4.2).  A mismatch returns NFS4ERR_MINOR_VERS_MISMATCH at
     * the client's CB compound dispatcher (pynfs nfs4client.py:182).
     * Caller passes the value snapshot from struct nfs4_session via
     * struct session_cb_snap.minorversion.
     */
    uint32_t minor = minorversion;

    if (!xdr_uint32_t(xdrs, &minor)) {
        return false;
}

    /* callback_ident (RFC 8881 S2.10.3.1 -- 0 for NFSv4.1+) */
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

    /* referring_call_lists<> -- empty */
    uint32_t ref_count = 0;

    if (!xdr_uint32_t(xdrs, &ref_count)) {
        return false;
}

    return true;
}

static bool encode_cb_recall(XDR *xdrs,
                             const struct nfs4_cb_recall_args *a)
{
    /* CB_RECALL4args per RFC 8881 S20.2:
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
 * Encode one notify4 struct for CB_NOTIFY (RFC 8881 S20.4, S3.3.15).
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
     * entry4 per RFC 8881 S3.3.15 notify_entry4: name + cookie +
     * attrs (fattr4).  Phase 8c emits empty attrs.
     */
    if (!encode_component4(xdrs, name, name_len)) { return false; }
    if (!xdr_uint64_t(xdrs, &cookie)) { return false; }
    if (!encode_empty_fattr4(xdrs)) { return false; }
    return true;
}

/* notify_remove4 payload (RFC 8881 S3.3.15). */
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
        /* fh4 as opaque<NFS4_FHSIZE> -- we encode fileid as 8-byte FH */
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

    if (!encode_rpc_call_header(&xdrs, xid, session->cb_prog, 1,
                                &session->cb_sec)) {
        rc = -EIO;
        goto out;
    }
    if (!encode_cb_compound_header(&xdrs, session->minorversion, 2)) {
        /* CB_SEQUENCE + CB_LAYOUTRECALL */
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

    /*
     * Do NOT recv() the CB reply on this fd.  NFSv4.1 multiplexes
     * fore- and back-channel on the same TCP connection (RFC 8881
     * S2.10.3.1).  The epoll reader is the sole consumer of inbound
     * records; recv() here races with it and can steal the client's
     * next fore-channel record (e.g. LAYOUTRETURN), producing -EIO
     * on our decode and stranding the client's op without a reply.
     *
     * Treat a successful send as "callback delivered".  The recall
     * coordinator's transient-status logic handles cb_status == 0
     * correctly: it skips the preemptive revoke and waits for the
     * client's natural LAYOUTRETURN / DELEGRETURN.
     */
    rc = 0;

out:
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

    if (!encode_rpc_call_header(&xdrs, xid, session->cb_prog, 1,
                                &session->cb_sec)) {
        rc = -EIO;
        goto out;
    }
    if (!encode_cb_compound_header(&xdrs, session->minorversion, 2)) {
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

    /* See nfs4_cb_layoutrecall: do not recv on the shared fd. */
    rc = 0;

out:
    cb_slot_release(session, slot_idx);
    return rc;
}

int nfs4_cb_notify_fd(int fd,
                      const uint8_t session_id[SESSION_ID_SIZE],
                      uint32_t cb_prog,
                      uint32_t slot_seq_id,
                      uint32_t num_cb_slots,
                      uint32_t minorversion,
                      const struct nfs4_cb_sec *sec,
                      const struct nfs4_cb_notify_args *args,
                      uint32_t timeout_ms)
{
    char msg_buf[CB_MAX_MSG_SIZE];
    XDR xdrs;
    uint32_t xid;
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

    if (!encode_rpc_call_header(&xdrs, xid, cb_prog, 1, sec)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }
    if (!encode_cb_compound_header(&xdrs, minorversion, 2)) {
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

    /* See nfs4_cb_layoutrecall: do not recv on the shared fd. */
    return 0;
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

    if (!encode_rpc_call_header(&xdrs, xid, session->cb_prog, 1,
                                &session->cb_sec)) {
        rc = -EIO;
        goto out;
    }
    if (!encode_cb_compound_header(&xdrs, session->minorversion, 2)) {
        /* CB_SEQUENCE + CB_RECALL */
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

    /* See nfs4_cb_layoutrecall: do not recv on the shared fd. */
    rc = 0;

out:
    cb_slot_release(session, slot_idx);
    return rc;
}

/* -----------------------------------------------------------------------
 * fd-based senders (no session/rpc_conn dependency)
 *
 * These are used by the recall coordinators (delegation conflict-recall
 * on op_open/op_setattr/op_remove/op_rename/op_write, and layout
 * conflict-recall on op_layoutget).  The pattern is:
 *   1. Under the session table lock, snapshot the holder's cb
 *      metadata (session_id, cb_prog, slot_seq_id, num_cb_slots) and
 *      dup() the cb_conn fd.
 *   2. Release the lock.
 *   3. Send the CB on the dup'd fd; close the fd after the reply.
 * This avoids dereferencing a borrowed session pointer that may be
 * freed by DESTROY_SESSION or lease expiry while a recall is in flight.
 * ----------------------------------------------------------------------- */

int nfs4_cb_recall_fd(int fd,
                      const uint8_t session_id[SESSION_ID_SIZE],
                      uint32_t cb_prog,
                      uint32_t slot_seq_id,
                      uint32_t num_cb_slots,
                      uint32_t minorversion,
                      const struct nfs4_cb_sec *sec,
                      const struct nfs4_cb_recall_args *args,
                      uint32_t timeout_ms)
{
    char msg_buf[CB_MAX_MSG_SIZE];
    XDR xdrs;
    uint32_t xid;
    int rc;

    if (fd < 0 || args == NULL || session_id == NULL) {
        return -EINVAL;
    }

    if (timeout_ms == 0) {
        timeout_ms = cb_default_timeout();
    }

    xid = atomic_fetch_add(&cb_xid_counter, 1);

    xdrmem_create(&xdrs, msg_buf, sizeof(msg_buf), XDR_ENCODE);

    if (!encode_rpc_call_header(&xdrs, xid, cb_prog, 1, sec)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }
    if (!encode_cb_compound_header(&xdrs, minorversion, 2)) {
        /* CB_SEQUENCE + CB_RECALL */
        xdr_destroy(&xdrs);
        return -EIO;
    }

    uint32_t highest_slot = (num_cb_slots > 0) ? num_cb_slots - 1 : 0;

    if (!encode_cb_sequence(&xdrs, session_id, 0,
                            slot_seq_id, highest_slot)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }
    if (!encode_cb_recall(&xdrs, args)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }

    uint32_t msg_len = xdr_getpos(&xdrs);

    xdr_destroy(&xdrs);

    rc = send_record(fd, (uint8_t *)msg_buf, msg_len);
    if (rc != 0) {
        return -EIO;
    }

    /* See nfs4_cb_layoutrecall: do not recv on the shared fd. */
    return 0;
}

int nfs4_cb_layoutrecall_fd(int fd,
                            const uint8_t session_id[SESSION_ID_SIZE],
                            uint32_t cb_prog,
                            uint32_t slot_seq_id,
                            uint32_t num_cb_slots,
                            uint32_t minorversion,
                            const struct nfs4_cb_sec *sec,
                            const struct nfs4_cb_layoutrecall_args *args,
                            uint32_t timeout_ms)
{
    char msg_buf[CB_MAX_MSG_SIZE];
    XDR xdrs;
    uint32_t xid;
    int rc;

    if (fd < 0 || args == NULL || session_id == NULL) {
        return -EINVAL;
    }

    if (timeout_ms == 0) {
        timeout_ms = cb_default_timeout();
    }

    xid = atomic_fetch_add(&cb_xid_counter, 1);

    xdrmem_create(&xdrs, msg_buf, sizeof(msg_buf), XDR_ENCODE);

    if (!encode_rpc_call_header(&xdrs, xid, cb_prog, 1, sec)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }
    if (!encode_cb_compound_header(&xdrs, minorversion, 2)) {
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

    /* See nfs4_cb_layoutrecall: do not recv on the shared fd. */
    return 0;
}

/* -----------------------------------------------------------------------
 * Pending CB reply slot — multiplexed backchannel reply delivery
 *
 * RFC 8881 §2.10.3.1: the backchannel shares the forechannel TCP
 * connection (BIND_CONN_TO_SESSION).  The epoll record reader
 * (conn_read) is the sole consumer of incoming records.  A
 * synchronous CB like CB_GETATTR cannot recv() on a dup'd fd
 * because the reader would consume the reply first.  Instead,
 * the reader detects msg_type==1 (REPLY) and delivers the record
 * here; the CB sender blocks on the condvar.
 *
 * Single-slot: only one synchronous CB round-trip at a time.
 * This is fine because CB_GETATTR is the only synchronous CB,
 * and it is always called while processing a single COMPOUND.
 * ----------------------------------------------------------------------- */

static struct {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    uint32_t        xid;        /**< XID the waiter expects. */
    uint8_t        *buf;        /**< Caller-provided receive buffer. */
    uint32_t        buf_size;   /**< Capacity of buf. */
    uint32_t        reply_len;  /**< Actual reply length delivered. */
    bool            waiting;    /**< A sender is waiting for a reply. */
    bool            delivered;  /**< Reply has been delivered. */
} cb_pending_reply;

void nfs4_cb_pending_reply_init(void)
{
    pthread_mutex_init(&cb_pending_reply.lock, NULL);
    pthread_condattr_t attr;
    pthread_condattr_init(&attr);
    pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
    pthread_cond_init(&cb_pending_reply.cond, &attr);
    pthread_condattr_destroy(&attr);
    cb_pending_reply.waiting = false;
    cb_pending_reply.delivered = false;
}

static void cb_pending_register(uint32_t xid, uint8_t *buf, uint32_t bufsz)
{
    pthread_mutex_lock(&cb_pending_reply.lock);
    cb_pending_reply.xid = xid;
    cb_pending_reply.buf = buf;
    cb_pending_reply.buf_size = bufsz;
    cb_pending_reply.reply_len = 0;
    cb_pending_reply.delivered = false;
    cb_pending_reply.waiting = true;
    pthread_mutex_unlock(&cb_pending_reply.lock);
}

/** Block until the reply is delivered or timeout_ms expires.
 *  Returns reply length, or -1 on timeout. */
static int cb_pending_wait(uint32_t timeout_ms)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    ts.tv_sec  += timeout_ms / 1000;
    ts.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec  += 1;
        ts.tv_nsec -= 1000000000L;
    }

    int ret = -1;
    pthread_mutex_lock(&cb_pending_reply.lock);
    while (!cb_pending_reply.delivered) {
        int rc = pthread_cond_timedwait(&cb_pending_reply.cond,
                                       &cb_pending_reply.lock, &ts);
        if (rc != 0) { break; } /* ETIMEDOUT or error */
    }
    if (cb_pending_reply.delivered) {
        ret = (int)cb_pending_reply.reply_len;
    }
    pthread_mutex_unlock(&cb_pending_reply.lock);
    return ret;
}

static void cb_pending_unregister(void)
{
    pthread_mutex_lock(&cb_pending_reply.lock);
    cb_pending_reply.waiting = false;
    cb_pending_reply.delivered = false;
    cb_pending_reply.buf = NULL;
    pthread_mutex_unlock(&cb_pending_reply.lock);
}

void nfs4_cb_deliver_reply(const uint8_t *buf, uint32_t len)
{
    pthread_mutex_lock(&cb_pending_reply.lock);
    if (cb_pending_reply.waiting && !cb_pending_reply.delivered) {
        uint32_t copy = (len < cb_pending_reply.buf_size)
                            ? len : cb_pending_reply.buf_size;
        memcpy(cb_pending_reply.buf, buf, copy);
        cb_pending_reply.reply_len = copy;
        cb_pending_reply.delivered = true;
        pthread_cond_signal(&cb_pending_reply.cond);
    }
    /* If no waiter, silently discard (fire-and-forget CB reply). */
    pthread_mutex_unlock(&cb_pending_reply.lock);
}

/* -----------------------------------------------------------------------
 * CB_GETATTR (RFC 8881 §20.1) — synchronous round-trip
 *
 * Unlike the fire-and-forget CB_RECALL / CB_LAYOUTRECALL senders,
 * CB_GETATTR MUST read the reply to extract the delegation holder's
 * current size + change.  The reply is delivered via the pending
 * slot above, routed from the epoll record reader.
 * ----------------------------------------------------------------------- */

static bool encode_cb_getattr(XDR *xdrs, uint64_t fileid, uint32_t mds_id,
                              const struct nfs4_stateid *deleg_sid)
{
    uint32_t opnum = OP_CB_GETATTR;
    if (!xdr_uint32_t(xdrs, &opnum)) { return false; }
    /* nfs_fh4 — v1 cluster-global FH matching the delegation grant.
     * RFC 8881 §20.1: the FH MUST match the one the client holds. */
    if (!xdr_nfs4_fh_encode_v1(xdrs, mds_id, fileid, 1)) { return false; }
    /* bitmap4 requesting CHANGE(3) + SIZE(4). */
    uint32_t bm_words = 1;
    uint32_t bm_w0 = (1u << 3) | (1u << 4); /* CHANGE + SIZE */
    if (!xdr_uint32_t(xdrs, &bm_words)) { return false; }
    if (!xdr_uint32_t(xdrs, &bm_w0)) { return false; }
    (void)deleg_sid; /* CB_GETATTR does not carry a stateid on the wire */
    return true;
}

/**
 * Parse a CB_GETATTR reply to extract size + change.
 * The reply wire form is:
 *   RPC reply header, then CB_COMPOUND4res:
 *     status(4) tag(var) resarray_count(4)
 *     CB_SEQUENCE result (opnum+status + body)
 *     CB_GETATTR result (opnum+status + fattr4 on OK)
 */
static int decode_cb_getattr_reply(const uint8_t *buf, uint32_t len,
                                   struct nfs4_cb_getattr_result *out)
{
    XDR xdrs;
    uint32_t v;
    out->valid = false;

    xdrmem_create(&xdrs, (char *)(uintptr_t)buf, len, XDR_DECODE);

    /* RPC reply header: xid(4) msg_type(4)=1 reply_stat(4)=0
     *                   verf_flavor(4) verf_len(4) verf_body(var)
     *                   accept_stat(4)=0 */
    if (!xdr_uint32_t(&xdrs, &v)) { goto fail; } /* xid */
    if (!xdr_uint32_t(&xdrs, &v)) { goto fail; } /* msg_type */
    if (v != 1) { goto fail; } /* must be REPLY */
    if (!xdr_uint32_t(&xdrs, &v)) { goto fail; } /* reply_stat */
    if (v != 0) { goto fail; } /* MSG_ACCEPTED */
    /* verifier: flavor + len + body */
    if (!xdr_uint32_t(&xdrs, &v)) { goto fail; } /* verf flavor */
    { uint32_t vlen;
      if (!xdr_uint32_t(&xdrs, &vlen)) { goto fail; }
      if (vlen > 0) {
          char skip[400];
          if (vlen > sizeof(skip)) { goto fail; }
          if (!xdr_opaque_decode(&xdrs, skip, vlen)) { goto fail; }
      }
    }
    if (!xdr_uint32_t(&xdrs, &v)) { goto fail; } /* accept_stat */
    if (v != 0) { goto fail; } /* SUCCESS */

    /* CB_COMPOUND4res: status(4) + tag(var) + resarray_count(4) */
    if (!xdr_uint32_t(&xdrs, &v)) { goto fail; } /* compound status */
    if (v != 0) { goto fail; } /* NFS4_OK */
    { uint32_t tlen;
      if (!xdr_uint32_t(&xdrs, &tlen)) { goto fail; }
      if (tlen > 0) {
          char skip[64];
          if (tlen > sizeof(skip)) { goto fail; }
          if (!xdr_opaque_decode(&xdrs, skip, tlen)) { goto fail; }
      }
    }
    uint32_t res_count;
    if (!xdr_uint32_t(&xdrs, &res_count)) { goto fail; }
    if (res_count < 2) { goto fail; }

    /* Skip CB_SEQUENCE result: opnum(4) + status(4) + body. */
    if (!xdr_uint32_t(&xdrs, &v)) { goto fail; } /* opnum */
    if (!xdr_uint32_t(&xdrs, &v)) { goto fail; } /* status */
    if (v != 0) { goto fail; }
    /* CB_SEQUENCE4resok per RFC 8881 §20.9.3:
     *   session_id(16) + sequenceid(4) + slotid(4)
     *   + highest_slotid(4) + target_highest_slotid(4)
     *
     * NOTE: unlike the fore-channel SEQUENCE4resok (§18.46),
     * CB_SEQUENCE4resok does NOT include csr_status_flags.
     * The peek below is defensive: if an implementation adds
     * an extra field, we consume it instead of misaligning. */
    { char sid[16];
      if (!xdr_opaque_decode(&xdrs, sid, 16)) { goto fail; }
    }
    for (int i = 0; i < 4; i++) {
        if (!xdr_uint32_t(&xdrs, &v)) { goto fail; }
    }
    /* Peek: if the next word is NOT the CB_GETATTR opnum (3),
     * assume it is status_flags and consume it. */
    {
        uint32_t peek_pos = xdr_getpos(&xdrs);
        uint32_t peek_val;
        if (!xdr_uint32_t(&xdrs, &peek_val)) { goto fail; }
        if (peek_val != OP_CB_GETATTR) {
            /* Was status_flags — consumed; next read is opnum. */
        } else {
            /* Was the opnum itself — rewind so the outer
             * CB_GETATTR parse reads it. */
            xdr_setpos(&xdrs, peek_pos);
        }
    }

    /* CB_GETATTR result: opnum(4) + status(4) + fattr4 on OK. */
    if (!xdr_uint32_t(&xdrs, &v)) { goto fail; } /* opnum */
    if (!xdr_uint32_t(&xdrs, &v)) { goto fail; } /* status */
    if (v != 0) { goto fail; }

    /* fattr4 = bitmap4 + opaque attr_vals<> */
    uint32_t bm[3] = {0};
    uint32_t bm_count;
    if (!xdr_uint32_t(&xdrs, &bm_count)) { goto fail; }
    for (uint32_t i = 0; i < bm_count && i < 3; i++) {
        if (!xdr_uint32_t(&xdrs, &bm[i])) { goto fail; }
    }
    /* Skip any bitmap words beyond what we handle. */
    for (uint32_t i = 3; i < bm_count; i++) {
        if (!xdr_uint32_t(&xdrs, &v)) { goto fail; }
    }
    uint32_t attr_len;
    if (!xdr_uint32_t(&xdrs, &attr_len)) { goto fail; }
    /* Parse attr_vals in bitmap order. */
    bool has_change = (bm[0] & (1u << 3)) != 0;
    bool has_size   = (bm[0] & (1u << 4)) != 0;
    if (has_change) {
        if (!xdr_uint64_t(&xdrs, &out->change)) { goto fail; }
    }
    if (has_size) {
        if (!xdr_uint64_t(&xdrs, &out->size)) { goto fail; }
    }
    out->valid = true;
    xdr_destroy(&xdrs);
    return 0;

fail:
    xdr_destroy(&xdrs);
    return -1;
}

int nfs4_cb_getattr_fd(int fd,
                       const uint8_t session_id[SESSION_ID_SIZE],
                       uint32_t cb_prog,
                       uint32_t slot_seq_id,
                       uint32_t num_cb_slots,
                       uint32_t minorversion,
                       const struct nfs4_cb_sec *sec,
                       uint64_t fileid,
                       uint32_t mds_id,
                       const struct nfs4_stateid *deleg_stateid,
                       uint32_t timeout_ms,
                       struct nfs4_cb_getattr_result *out)
{
    char msg_buf[CB_MAX_MSG_SIZE];
    XDR xdrs;
    uint32_t xid;
    int rc;

    if (fd < 0 || session_id == NULL || out == NULL) {
        return -EINVAL;
    }
    memset(out, 0, sizeof(*out));
    if (timeout_ms == 0) {
        timeout_ms = cb_default_timeout();
    }

    xid = atomic_fetch_add(&cb_xid_counter, 1);

    /* Encode CB_COMPOUND { CB_SEQUENCE, CB_GETATTR }. */
    xdrmem_create(&xdrs, msg_buf, sizeof(msg_buf), XDR_ENCODE);
    if (!encode_rpc_call_header(&xdrs, xid, cb_prog, 1, sec)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }
    if (!encode_cb_compound_header(&xdrs, minorversion, 2)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }
    uint32_t highest_slot = (num_cb_slots > 0) ? num_cb_slots - 1 : 0;
    if (!encode_cb_sequence(&xdrs, session_id, 0,
                            slot_seq_id, highest_slot)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }
    if (!encode_cb_getattr(&xdrs, fileid, mds_id, deleg_stateid)) {
        xdr_destroy(&xdrs);
        return -EIO;
    }
    uint32_t msg_len = xdr_getpos(&xdrs);
    xdr_destroy(&xdrs);

    /* Send. */
    rc = send_record(fd, (uint8_t *)msg_buf, msg_len);
    if (rc != 0) {
        return -EIO;
    }

    /* Receive reply directly on the dup'd fd.
     *
     * The dup'd fd shares the underlying socket with the rpc_conn
     * monitored by the epoll thread.  When a threadpool is active,
     * dispatch_to_pool() disables EPOLLIN on the requesting
     * connection while its COMPOUND is being processed on the
     * worker.  If the backchannel is bound to the SAME connection
     * (BIND_CONN_TO_SESSION on the forechannel), the epoll thread
     * will NOT read from this socket until the worker finishes.
     * Direct recv on the dup'd fd is safe in that window.
     *
     * If the backchannel is on a DIFFERENT connection whose
     * EPOLLIN is still active, the reply is routed through
     * nfs4_cb_deliver_reply() instead (see conn_read demux).
     * We try the pending slot first (fast path for separate
     * connections), then fall back to direct recv. */
    uint8_t reply_buf[CB_MAX_MSG_SIZE];
    cb_pending_register(xid, reply_buf, sizeof(reply_buf));

    /* Short poll: give the epoll reader ~50 ms to deliver if the
     * reply is on a different connection. */
    int reply_len = cb_pending_wait(50);
    if (reply_len >= 0) {
        /* Fast path: epoll reader delivered the reply. */
        cb_pending_unregister();
        rc = decode_cb_getattr_reply(reply_buf, (uint32_t)reply_len, out);
        return (rc == 0 && out->valid) ? 0 : -EIO;
    }
    cb_pending_unregister();

    /* Slow path: epoll thread can't read (EPOLLIN disabled on our
     * connection, or the reply just hasn't arrived).  Do a blocking
     * recv with SO_RCVTIMEO on the dup'd fd.
     *
     * The dup'd fd shares the O_NONBLOCK flag with the original
     * connection fd (set by handle_epoll_accept).  SO_RCVTIMEO has
     * no effect on non-blocking sockets — recv returns EAGAIN
     * immediately.  Clear O_NONBLOCK so the recv actually blocks. */
    {
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags >= 0 && (flags & O_NONBLOCK)) {
            fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
        }
        struct timeval tv;
        tv.tv_sec  = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    /* Read the 4-byte record-mark header. */
    uint32_t frag_hdr_be;
    {
        size_t got = 0;
        while (got < 4) {
            ssize_t n = recv(fd, (uint8_t *)&frag_hdr_be + got,
                            4 - got, 0);
            if (n <= 0) { return -ETIMEDOUT; }
            got += (size_t)n;
        }
    }
    uint32_t payload_len = ntohl(frag_hdr_be) & 0x7FFFFFFFu;
    if (payload_len > sizeof(reply_buf)) {
        return -EIO;
    }
    {
        size_t got = 0;
        while (got < payload_len) {
            ssize_t n = recv(fd, reply_buf + got,
                            payload_len - got, 0);
            if (n <= 0) { return -ETIMEDOUT; }
            got += (size_t)n;
        }
    }
    rc = decode_cb_getattr_reply(reply_buf, payload_len, out);
    return (rc == 0 && out->valid) ? 0 : -EIO;
}

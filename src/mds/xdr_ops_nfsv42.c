/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 */
/*
 * xdr_ops_nfsv42.c — NFSv4.2 per-op XDR decoders and result encoders.
 *
 * These functions were extracted from inline case blocks in
 * decode_one_op() / encode_one_result() in xdr_codec.c.
 */

#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#include "xdr_codec.h"
#include "xdr_internal.h"

/* -----------------------------------------------------------------------
 * NFSv4.2 op decoders
 * ----------------------------------------------------------------------- */

bool decode_op_allocate(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_allocate *a = &op->arg.allocate;
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->offset)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->length)) { return false; }
    return true;
}

bool decode_op_deallocate(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_deallocate *a = &op->arg.deallocate;
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->offset)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->length)) { return false; }
    return true;
}

bool decode_op_copy(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_copy *a = &op->arg.copy;
    uint32_t bval;
    if (!xdr_nfs4_stateid_decode(xdrs, &a->src_stateid)) { return false; }
    if (!xdr_nfs4_stateid_decode(xdrs, &a->dst_stateid)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->src_offset)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->dst_offset)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->count)) { return false; }
    if (!xdr_uint32_t(xdrs, &bval)) { return false; }
    a->consecutive = (bval != 0);
    if (!xdr_uint32_t(xdrs, &bval)) { return false; }
    a->synchronous = (bval != 0);
    return true;
}

bool decode_op_copy_notify(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_copy_notify *a = &op->arg.copy_notify;
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) { return false; }
    return true;
}

bool decode_op_io_advise(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_io_advise *a = &op->arg.io_advise;
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->offset)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->count)) { return false; }
    if (!xdr_uint32_t(xdrs, &a->hints)) { return false; }
    return true;
}

bool decode_op_offload_cancel(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_offload_cancel *a = &op->arg.offload_cancel;
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) { return false; }
    return true;
}

bool decode_op_offload_status(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_offload_status *a = &op->arg.offload_status;
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) { return false; }
    return true;
}

bool decode_op_read_plus(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_read_plus *a = &op->arg.read_plus;
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->offset)) { return false; }
    if (!xdr_uint32_t(xdrs, &a->count)) { return false; }
    return true;
}

bool decode_op_seek(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_seek *a = &op->arg.seek;
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->offset)) { return false; }
    if (!xdr_uint32_t(xdrs, &a->what)) { return false; }
    return true;
}

bool decode_op_write_same(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_write_same *a = &op->arg.write_same;
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->offset)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->length)) { return false; }
    if (!xdr_uint32_t(xdrs, &a->data_len)) { return false; }
    if (a->data_len > MDS_XATTR_VAL_MAX) { return false; }
    if (a->data_len > 0) {
        if (!xdr_opaque_decode(xdrs, (char *)a->data, a->data_len)) {
            return false;
        }
    }
    return true;
}

bool decode_op_clone(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_clone *a = &op->arg.clone;
    if (!xdr_nfs4_stateid_decode(xdrs, &a->src_stateid)) { return false; }
    if (!xdr_nfs4_stateid_decode(xdrs, &a->dst_stateid)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->src_offset)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->dst_offset)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->count)) { return false; }
    return true;
}

/* -----------------------------------------------------------------------
 * NFSv4.2 result encoders
 * ----------------------------------------------------------------------- */

bool encode_res_io_advise(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_io_advise *ra = &r->res.io_advise;
    uint32_t hints = ra->hints;
    /* bitmap4 = uint32<> — must encode as variable-length array. */
    uint32_t bm_count = 1;
    if (!xdr_uint32_t(xdrs, &bm_count)) { return false; }
    return xdr_uint32_t(xdrs, &hints);
}

bool encode_res_seek(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_seek *rs = &r->res.seek;
    int32_t eof_val = rs->eof ? 1 : 0;
    uint64_t off = rs->offset;
    if (!xdr_putbool(xdrs, eof_val)) { return false; }
    if (!xdr_uint64_t(xdrs, &off)) { return false; }
    return true;
}

/**
 * Encode a single READ_PLUS data segment (NFS4_CONTENT_DATA).
 */
static bool encode_rp_data_seg(XDR *xdrs,
                               const struct nfs4_read_plus_content *seg)
{
    uint64_t doff = seg->offset;
    uint32_t dlen = seg->u.data.data_len;
    if (!xdr_uint64_t(xdrs, &doff)) { return false; }
    if (!xdr_uint32_t(xdrs, &dlen)) { return false; }
    if (dlen > 0) {
        if (!xdr_opaque_encode(xdrs,
                (char *)seg->u.data.data, dlen)) {
            return false;
        }
    }
    return true;
}

/**
 * Encode a single READ_PLUS hole segment (NFS4_CONTENT_HOLE).
 */
static bool encode_rp_hole_seg(XDR *xdrs,
                               const struct nfs4_read_plus_content *seg)
{
    uint64_t hoff = seg->offset;
    uint64_t hlen = seg->u.hole.length;
    if (!xdr_uint64_t(xdrs, &hoff)) { return false; }
    return xdr_uint64_t(xdrs, &hlen);
}

bool encode_res_read_plus(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_read_plus *rp = &r->res.read_plus;
    int32_t eof_val = rp->eof ? 1 : 0;
    uint32_t cnt = rp->seg_count;
    uint32_t si;
    if (!xdr_putbool(xdrs, eof_val)) { return false; }
    if (!xdr_uint32_t(xdrs, &cnt)) { return false; }
    for (si = 0; si < cnt && si < NFS4_READ_PLUS_MAX_SEGS; si++) {
        const struct nfs4_read_plus_content *seg = &rp->segs[si];
        uint32_t ct = seg->content_type;
        if (!xdr_uint32_t(xdrs, &ct)) { return false; }
        if (ct == NFS4_CONTENT_DATA) {
            if (!encode_rp_data_seg(xdrs, seg)) { return false; }
        } else {
            if (!encode_rp_hole_seg(xdrs, seg)) { return false; }
        }
    }
    return true;
}

bool encode_res_copy(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_copy *rc = &r->res.copy;
    uint64_t wc = rc->write_count;
    uint32_t comm = rc->committed;
    int32_t consec = rc->consecutive ? 1 : 0;
    int32_t sync = rc->synchronous ? 1 : 0;
    /*
     * RFC 7862 §15.2.3  write_response4:
     *   stateid4 *wr_callback_id<1>;  -- array count first
     *   length4 wr_count;
     *   stable_how4 wr_committed;
     *   verifier4 wr_writeverf;
     * Followed by copy_requirements4:
     *   bool cr_consecutive;
     *   bool cr_synchronous;
     */
    {
        uint32_t cb_count = rc->synchronous ? 0 : 1;

        if (!xdr_uint32_t(xdrs, &cb_count)) { return false; }
        if (cb_count > 0) {
            if (!xdr_nfs4_stateid_encode(xdrs, &rc->copy_stateid)) {
                return false;
            }
        }
    }
    if (!xdr_uint64_t(xdrs, &wc)) { return false; }
    if (!xdr_uint32_t(xdrs, &comm)) { return false; }
    if (!xdr_opaque_encode(xdrs, (const char *)rc->verifier,
                           NFS4_VERIFIER_SIZE)) {
        return false;
    }
    if (!xdr_putbool(xdrs, consec)) { return false; }
    if (!xdr_putbool(xdrs, sync)) { return false; }
    return true;
}

bool encode_res_copy_notify(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_copy_notify *rcn = &r->res.copy_notify;
    uint32_t src_count = 0; /* cnr_source_server<> — empty */
    /*
     * RFC 7862 §15.3.3  COPY_NOTIFY4resok:
     *   nfstime4 cnr_lease_time  {int64 sec, uint32 nsec}
     *   stateid4 cnr_stateid
     *   netloc4  cnr_source_server<>
     */
    {
        struct timespec ts;
        ts.tv_sec = (time_t)rcn->lease_time;
        ts.tv_nsec = 0;
        if (!xdr_nfs4_time_encode(xdrs, &ts)) { return false; }
    }
    if (!xdr_nfs4_stateid_encode(xdrs, &rcn->stateid)) { return false; }
    if (!xdr_uint32_t(xdrs, &src_count)) { return false; }
    return true;
}

bool encode_res_offload_status(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_offload_status *ros = &r->res.offload_status;
    uint64_t cnt = ros->count;
    int32_t comp = ros->complete ? 1 : 0;
    if (!xdr_uint64_t(xdrs, &cnt)) { return false; }
    if (!xdr_putbool(xdrs, comp)) { return false; }
    if (ros->complete) {
        uint32_t cst = (uint32_t)ros->complete_status;
        if (!xdr_uint32_t(xdrs, &cst)) { return false; }
    }
    return true;
}

/* -----------------------------------------------------------------------
 * RFC 8276 extended attribute decoders
 * ----------------------------------------------------------------------- */

/** Decode xattr_name (utf8str_mixed = opaque<>) into a NUL-terminated buf. */
static bool decode_xattr_name(XDR *xdrs, char *buf, uint32_t bufsize)
{
    uint32_t len = 0;

    if (!xdr_uint32_t(xdrs, &len)) { return false; }
    if (len == 0 || len >= bufsize) { return false; }
    if (!xdr_opaque_decode(xdrs, buf, len)) { return false; }
    buf[len] = '\0';
    return true;
}

bool decode_op_getxattr(XDR *xdrs, struct nfs4_op *op)
{
    return decode_xattr_name(xdrs, op->arg.getxattr.name,
                             sizeof(op->arg.getxattr.name));
}

bool decode_op_setxattr(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_setxattr *a = &op->arg.setxattr;
    uint32_t val_len;

    if (!xdr_uint32_t(xdrs, &a->option)) { return false; }
    if (a->option > SETXATTR4_REPLACE) { return false; }
    if (!decode_xattr_name(xdrs, a->name, sizeof(a->name))) {
        return false;
    }
    /* xattrvalue4 = opaque<> */
    if (!xdr_uint32_t(xdrs, &val_len)) { return false; }
    if (val_len > MDS_XATTR_VAL_MAX) { return false; }
    if (val_len > 0) {
        if (!xdr_opaque_decode(xdrs, (char *)a->value, val_len)) {
            return false;
        }
    }
    a->value_len = val_len;
    return true;
}

bool decode_op_listxattrs(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_listxattrs *a = &op->arg.listxattrs;

    if (!xdr_uint64_t(xdrs, &a->cookie)) { return false; }
    if (!xdr_uint32_t(xdrs, &a->maxcount)) { return false; }
    return true;
}

bool decode_op_removexattr(XDR *xdrs, struct nfs4_op *op)
{
    return decode_xattr_name(xdrs, op->arg.removexattr.name,
                             sizeof(op->arg.removexattr.name));
}

/* -----------------------------------------------------------------------
 * RFC 8276 extended attribute result encoders
 * ----------------------------------------------------------------------- */

bool encode_res_getxattr(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_getxattr *rx = &r->res.getxattr;
    uint32_t len = rx->value_len;

    if (!xdr_uint32_t(xdrs, &len)) { return false; }
    if (len > 0) {
        if (!xdr_opaque_encode(xdrs, (const char *)rx->value, len)) {
            return false;
        }
    }
    return true;
}

bool encode_res_setxattr(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_setxattr *rx = &r->res.setxattr;
    uint64_t before = rx->change_before;
    uint64_t after  = rx->change_after;

    return encode_change_info(xdrs, before, after);
}

bool encode_res_listxattrs(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_listxattrs *rx = &r->res.listxattrs;
    uint32_t cnt = rx->name_count;
    uint64_t cookie = rx->cookie;
    int32_t eof_val = rx->eof ? 1 : 0;
    uint32_t i;

    /*
     * RFC 8276 §8.4.3 / Linux kernel decode_listxattrs:
     *   cookie(8) + count(4) + [per-name: len(4)+data] + eof(4)
     */
    if (!xdr_uint64_t(xdrs, &cookie)) { return false; }
    if (!xdr_uint32_t(xdrs, &cnt)) { return false; }
    for (i = 0; i < cnt; i++) {
        uint32_t nlen = (uint32_t)strlen(rx->names[i]);

        if (!xdr_uint32_t(xdrs, &nlen)) { return false; }
        if (nlen > 0) {
            if (!xdr_opaque_encode(xdrs, rx->names[i], nlen)) {
                return false;
            }
        }
    }
    if (!xdr_putbool(xdrs, eof_val)) { return false; }
    return true;
}

bool encode_res_removexattr(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_removexattr *rx = &r->res.removexattr;
    uint64_t before = rx->change_before;
    uint64_t after  = rx->change_after;

    return encode_change_info(xdrs, before, after);
}

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * xdr_internal.h -- Private dispatch header for XDR op-codec modules.
 *
 * NOT a public API header.  Only included by xdr_codec.c and the
 * xdr_ops_*.c per-group modules.
 */

#ifndef XDR_INTERNAL_H
#define XDR_INTERNAL_H

#include "xdr_codec.h"

/* -----------------------------------------------------------------------
 * Shared helpers (defined in xdr_codec.c)
 * ----------------------------------------------------------------------- */

bool encode_change_info(XDR *xdrs, uint64_t before, uint64_t after);

void build_all_requested(uint32_t bm[NFS4_BITMAP_WORDS]);

/* -----------------------------------------------------------------------
 * NFSv4.1 core op decoders (xdr_ops_core.c)
 * ----------------------------------------------------------------------- */

bool decode_op_sequence(XDR *xdrs, struct nfs4_op *op);
bool decode_op_exchange_id(XDR *xdrs, struct nfs4_op *op);
bool decode_op_create_session(XDR *xdrs, struct nfs4_op *op);
bool decode_op_destroy_session(XDR *xdrs, struct nfs4_op *op);
bool decode_op_putfh(XDR *xdrs, struct nfs4_op *op);
bool decode_op_lookup(XDR *xdrs, struct nfs4_op *op);
bool decode_op_getattr(XDR *xdrs, struct nfs4_op *op);
bool decode_op_setattr(XDR *xdrs, struct nfs4_op *op);
bool decode_op_create(XDR *xdrs, struct nfs4_op *op);
bool decode_op_remove(XDR *xdrs, struct nfs4_op *op);
bool decode_op_rename(XDR *xdrs, struct nfs4_op *op);
bool decode_op_link(XDR *xdrs, struct nfs4_op *op);
bool decode_op_readdir(XDR *xdrs, struct nfs4_op *op);
bool decode_op_open(XDR *xdrs, struct nfs4_op *op);
bool decode_op_close(XDR *xdrs, struct nfs4_op *op);
bool decode_op_openattr(XDR *xdrs, struct nfs4_op *op);
bool decode_op_read(XDR *xdrs, struct nfs4_op *op);
bool decode_op_write(XDR *xdrs, struct nfs4_op *op);
bool decode_op_reclaim_complete(XDR *xdrs, struct nfs4_op *op);

/* NFSv4.1 core result encoders (xdr_ops_core.c) */

bool encode_res_sequence(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_exchange_id(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_create_session(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_getfh(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_getattr(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_create(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_readdir(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_open(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_close(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_read(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_write(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_remove(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_rename(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_link(XDR *xdrs, const struct nfs4_result *r);

/* -----------------------------------------------------------------------
 * pNFS layout op decoders/encoders (xdr_ops_layout.c)
 * ----------------------------------------------------------------------- */

bool decode_op_layoutget(XDR *xdrs, struct nfs4_op *op);
bool decode_op_getdeviceinfo(XDR *xdrs, struct nfs4_op *op);
bool decode_op_layoutreturn(XDR *xdrs, struct nfs4_op *op);
bool decode_op_layoutcommit(XDR *xdrs, struct nfs4_op *op);
bool decode_op_layouterror(XDR *xdrs, struct nfs4_op *op);
bool decode_op_layoutstats(XDR *xdrs, struct nfs4_op *op);

bool encode_res_layoutget(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_getdeviceinfo(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_layoutreturn(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_layoutcommit(XDR *xdrs, const struct nfs4_result *r);

/* -----------------------------------------------------------------------
 * Directory delegation op decoders/encoders (xdr_ops_dir_deleg.c)
 * ----------------------------------------------------------------------- */

bool decode_op_get_dir_delegation(XDR *xdrs, struct nfs4_op *op);
bool encode_res_get_dir_delegation(XDR *xdrs, const struct nfs4_result *r);
/* Non-fatal NFS4ERR_DIRDELEG_UNAVAIL body: a single bool. */
bool xdr_encode_gdd_unavail_body(XDR *xdrs, const struct nfs4_result *r);

/* -----------------------------------------------------------------------
 * NFSv4.2 op decoders/encoders (xdr_ops_nfsv42.c)
 * ----------------------------------------------------------------------- */

bool decode_op_allocate(XDR *xdrs, struct nfs4_op *op);
bool decode_op_deallocate(XDR *xdrs, struct nfs4_op *op);
bool decode_op_copy(XDR *xdrs, struct nfs4_op *op);
bool decode_op_copy_notify(XDR *xdrs, struct nfs4_op *op);
bool decode_op_io_advise(XDR *xdrs, struct nfs4_op *op);
bool decode_op_offload_cancel(XDR *xdrs, struct nfs4_op *op);
bool decode_op_offload_status(XDR *xdrs, struct nfs4_op *op);
bool decode_op_read_plus(XDR *xdrs, struct nfs4_op *op);
bool decode_op_seek(XDR *xdrs, struct nfs4_op *op);
bool decode_op_write_same(XDR *xdrs, struct nfs4_op *op);
bool decode_op_clone(XDR *xdrs, struct nfs4_op *op);

bool encode_res_io_advise(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_seek(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_read_plus(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_copy(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_copy_notify(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_offload_status(XDR *xdrs, const struct nfs4_result *r);

/* -----------------------------------------------------------------------
 * RFC 8276 xattr op decoders/encoders (xdr_ops_nfsv42.c)
 * ----------------------------------------------------------------------- */

bool decode_op_getxattr(XDR *xdrs, struct nfs4_op *op);
bool decode_op_setxattr(XDR *xdrs, struct nfs4_op *op);
bool decode_op_listxattrs(XDR *xdrs, struct nfs4_op *op);
bool decode_op_removexattr(XDR *xdrs, struct nfs4_op *op);

bool encode_res_getxattr(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_setxattr(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_listxattrs(XDR *xdrs, const struct nfs4_result *r);
bool encode_res_removexattr(XDR *xdrs, const struct nfs4_result *r);

#endif /* XDR_INTERNAL_H */

/* Exported from xdr_codec.c for READDIR inline attrs. */
uint32_t mds_type_to_nfs4(enum mds_file_type t);

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * xdr_codec.h -- NFSv4.1/4.2 XDR encode/decode for COMPOUND requests.
 *
 * Uses libntirpc XDR primitives (xdrmem_ncreate, xdr_uint32_t, etc.)
 * for the low-level encoding.  This module adds the NFSv4.1/4.2-specific
 * compound and per-operation codecs.
 *
 * See docs/architecture.md S18.3 and the Phase 1 plan.
 */

#ifndef XDR_CODEC_H
#define XDR_CODEC_H

#include <rpc/xdr.h>
#include <rpc/xdr_inline.h>

#include "compound.h"

/* -----------------------------------------------------------------------
 * NFSv4.1 constants
 * ----------------------------------------------------------------------- */

#define NFS_PROGRAM     100003
#define NFS_V4          4
#define NFS_CB_PROGRAM  0x40000000

/** COMPOUND procedure number within NFS_V4. */
#define NFSPROC4_NULL     0
#define NFSPROC4_COMPOUND 1

/** NFSv4 filehandle maximum size (RFC 8881 S3.1). */
#define NFS4_FHSIZE       128

/** Maximum compound tag length. */
#define NFS4_TAG_MAXLEN   64

/** Maximum operations in a single compound. */
#define NFS4_MAX_OPS      64

/** COMPOUND4 minor versions we accept (1 = NFSv4.1, 2 = NFSv4.2). */
#define NFS4_MINOR_VERSION_MIN 1  /* v4.1+ only; v4.0 lacks SEQUENCE */
#define NFS4_MINOR_VERSION_MAX 2

/* -----------------------------------------------------------------------
 * NFSv4 attribute bit positions (for bitmap4)
 *
 * We support a Phase-1 subset.  Bit numbers from RFC 8881 S5.8.
 * ----------------------------------------------------------------------- */

#define FATTR4_SUPPORTED_ATTRS  0
#define FATTR4_TYPE             1
#define FATTR4_FH_EXPIRE_TYPE  2
#define FATTR4_CHANGE           3
#define FATTR4_SIZE             4
#define FATTR4_LINK_SUPPORT     5
#define FATTR4_SYMLINK_SUPPORT  6
#define FATTR4_LEASE_TIME      10
#define FATTR4_FILEHANDLE      19
#define FATTR4_FILEID          20
#define FATTR4_FS_LOCATIONS    24
#define FATTR4_FSID             8
#define FATTR4_MAXREAD         30
#define FATTR4_MAXWRITE        31
#define FATTR4_MAXNAME         29
#define FATTR4_MODE            33
#define FATTR4_NUMLINKS        35
#define FATTR4_OWNER           36
#define FATTR4_OWNER_GROUP     37
#define FATTR4_SPACE_USED      45
#define FATTR4_TIME_ACCESS     47
#define FATTR4_TIME_METADATA   52
#define FATTR4_TIME_MODIFY     53
#define FATTR4_TIME_ACCESS_SET 48
#define FATTR4_TIME_MODIFY_SET 54
#define FATTR4_MOUNTED_ON_FILEID 55
#define FATTR4_SPACE_AVAIL     42
#define FATTR4_SPACE_FREE      43
#define FATTR4_SPACE_TOTAL     44
/* Word 1 (bits 32-63) */
#define FATTR4_FS_LAYOUT_TYPES 62
/* RFC 5662 S2 -- layouthint4 set-only pNFS creation hint. */
#define FATTR4_LAYOUT_HINT    63
/* Word 2 (bits 64-95) */
#define FATTR4_LAYOUT_BLKSIZE  66
#define FATTR4_MDSTHRESHOLD    68
/* RFC 7862 S10.2.3 -- change_attr_type (bit 79).
 *
 * When a client requests this attribute, we advertise that every
 * mutation strictly increases the change counter, so clients may
 * use (stored_change == current_change) as a sound cache-validity
 * test without re-reading any other attributes.  This is the
 * foundation for directory delegations and aggressive client-side
 * attribute caching.
 *
 * Our change counter is backed by NDB `interpretedUpdateTuple +
 * incValue(change, 1)` on every mutation path, which is atomic at
 * the data-node level and therefore monotonic across concurrent
 * mutations from any MDS.  See docs/change-counter-audit.md for the
 * per-shim-path audit that backs this claim.
 */
#define FATTR4_CHANGE_ATTR_TYPE 79
/* RFC 8276 extended attribute support (bit 82). */
#define FATTR4_XATTR_SUPPORT   82

/* RFC 7862 S10.2.3 -- values for change_attr_type. */
#define NFS4_CHANGE_TYPE_IS_MONOTONIC_INCR         0
#define NFS4_CHANGE_TYPE_IS_VERSION_COUNTER        1
#define NFS4_CHANGE_TYPE_IS_VERSION_COUNTER_NOPNFS 2
#define NFS4_CHANGE_TYPE_IS_TIME_METADATA          3
#define NFS4_CHANGE_TYPE_IS_UNDEFINED              4

/** Number of 32-bit words needed for our bitmap (bits 0-68 -> 3 words). */
#define NFS4_BITMAP_WORDS  3

/** Bitmap helper: set bit in a 3-word bitmap array. */
static inline void nfs4_bitmap_set(uint32_t bm[NFS4_BITMAP_WORDS],
                                   uint32_t bit)
{
    if (bit < 32) {
        bm[0] |= ((uint32_t)1 << bit);
    } else if (bit < 64) {
        bm[1] |= ((uint32_t)1 << (bit - 32));
    } else if (bit < 96) {
        bm[2] |= ((uint32_t)1 << (bit - 64));
    }
}

/** Bitmap helper: test bit in a 3-word bitmap array. */
static inline bool nfs4_bitmap_test(const uint32_t bm[NFS4_BITMAP_WORDS],
                                    uint32_t bit)
{
    if (bit < 32) {
        return (bm[0] & ((uint32_t)1 << bit)) != 0;
    }
    if (bit < 64) {
        return (bm[1] & ((uint32_t)1 << (bit - 32))) != 0;
    }
    if (bit < 96) {
        return (bm[2] & ((uint32_t)1 << (bit - 64))) != 0;
    }
    return false;
}

/* -----------------------------------------------------------------------
 * NFSv4.1 type codecs
 * ----------------------------------------------------------------------- */

/**
 * Encode/decode an nfs_fh4 (variable-length opaque filehandle).
 *
 * We represent filehandles as 8-byte big-endian fileids.
 * Encode: fileid -> 8-byte opaque on the wire.
 * Decode: 8-byte opaque from the wire -> fileid.
 *
 * @return true on success.
 */
struct nfs4_fh_desc;
bool xdr_nfs4_fh_encode(XDR *xdrs, uint64_t fileid);
bool xdr_nfs4_fh_encode_v1(XDR *xdrs, uint32_t owner_mds_id,
                            uint64_t fileid, uint32_t generation);
bool xdr_nfs4_fh_decode_full(XDR *xdrs, struct nfs4_fh_desc *desc);
bool xdr_nfs4_fh_decode(XDR *xdrs, uint64_t *fileid);

/**
 * Encode/decode a stateid4 (seqid + 12-byte other).
 */
bool xdr_nfs4_stateid_encode(XDR *xdrs, const struct nfs4_stateid *sid);
bool xdr_nfs4_stateid_decode(XDR *xdrs, struct nfs4_stateid *sid);

/**
 * Encode/decode a bitmap4 (counted array of uint32).
 */
bool xdr_nfs4_bitmap_encode(XDR *xdrs, const uint32_t *bm, uint32_t words);
bool xdr_nfs4_bitmap_decode(XDR *xdrs, uint32_t *bm, uint32_t max_words,
                            uint32_t *actual_words);

/**
 * Encode/decode an nfstime4 (int64 seconds + uint32 nanoseconds).
 */
bool xdr_nfs4_time_encode(XDR *xdrs, const struct timespec *ts);
bool xdr_nfs4_time_decode(XDR *xdrs, struct timespec *ts);

/**
 * Encode fattr4 for a GETATTR result.
 *
 * Encodes only the attributes present in both @requested and
 * our supported set.  The encoded format is:
 *   bitmap4 (which attrs are included) + opaque attr_vals.
 *
 * @param xdrs       XDR stream in ENCODE mode.
 * @param inode      Inode whose attributes to encode.
 * @param requested  Client's requested attribute bitmap.
 * @return true on success.
 */
bool xdr_nfs4_fattr_encode(XDR *xdrs, const struct mds_inode *inode,
                           const uint32_t requested[NFS4_BITMAP_WORDS]);

/** Optional FS-level space values for SPACE_AVAIL/FREE/TOTAL attrs. */
struct xdr_fattr_fs_space {
    uint64_t space_avail;
    uint64_t space_free;
    uint64_t space_total;
};

/**
 * Extended fattr4 encoder with optional FS-level space attributes.
 *
 * @param xdrs      XDR stream.
 * @param inode     Inode to encode attributes from.
 * @param requested Client's requested attribute bitmap.
 * @param fs_space  FS-level space values (may be NULL).
 * @return true on success.
 */
bool xdr_nfs4_fattr_encode_ex(XDR *xdrs, const struct mds_inode *inode,
                               const uint32_t requested[NFS4_BITMAP_WORDS],
                               const struct xdr_fattr_fs_space *fs_space,
                               const char *ref_server,
                               const char *ref_rootpath,
                               const char *ref_fs_root);

/**
 * Decode fattr4 from a SETATTR or GETATTR request.
 *
 * Decodes the bitmap and attribute values into @inode and @mask.
 * @mask receives which attributes were actually present.
 *
 * @param xdrs   XDR stream in DECODE mode.
 * @param inode  Receives decoded attribute values.
 * @param mask   Receives bitmask of which attrs were decoded.
 * @return true on success.
 */
bool xdr_nfs4_fattr_decode(XDR *xdrs, struct mds_inode *inode,
                           uint32_t *mask);

/**
 * Decode fattr4 and optionally capture a pNFS layouthint4.
 *
 * The layout hint is parsed into @layout_hint when FATTR4_LAYOUT_HINT
 * is present and carries this project's fixed-size HPC hint body.
 * Other layouthint4 bodies are consumed but leave @layout_hint absent.
 */
bool xdr_nfs4_fattr_decode_ex(XDR *xdrs, struct mds_inode *inode,
                              uint32_t *mask,
                              struct nfs4_layout_hint *layout_hint);

/* -----------------------------------------------------------------------
 * Compound-level codec
 * ----------------------------------------------------------------------- */

/**
 * Decode a COMPOUND4args from an XDR stream.
 *
 * Decodes the compound tag, minor version, and array of operations
 * into our typed C structures (nfs4_op from compound.h).
 *
 * @param xdrs          XDR stream in DECODE mode (positioned after RPC header).
 * @param tag           Buffer to receive the compound tag string.
 * @param tag_maxlen    Size of @tag buffer.
 * @param minorversion  Receives the minor version number.
 * @param ops           Array to receive decoded operations.
 * @param max_ops       Capacity of @ops array.
 * @param op_count      Receives the number of operations decoded.
 * @return 0 on success, -1 on decode error.
 */
int nfs4_decode_compound_args(XDR *xdrs,
                              char *tag, uint32_t tag_maxlen,
                              uint32_t *minorversion,
                              struct nfs4_op *ops, uint32_t max_ops,
                              uint32_t *op_count);

/**
 * Encode a COMPOUND4res into an XDR stream.
 *
 * Encodes the compound status, tag, and array of operation results
 * from our typed C structures (nfs4_result from compound.h).
 *
 * @param xdrs          XDR stream in ENCODE mode.
 * @param status        Overall compound status (first error or NFS4_OK).
 * @param tag           Compound tag string (echoed from request).
 * @param results       Array of operation results.
 * @param result_count  Number of results to encode.
 * @return 0 on success, -1 on encode error.
 */
int nfs4_encode_compound_res(XDR *xdrs,
                             enum nfs4_status status,
                             const char *tag,
                             const struct nfs4_result *results,
                             uint32_t result_count);

/* -----------------------------------------------------------------------
 * RPC header helpers (used by rpc_server.c)
 * ----------------------------------------------------------------------- */

/**
 * Decode an ONC-RPC call header.
 *
 * Reads xid, validates msg_type=CALL, rpcvers=2, and extracts
 * prog/vers/proc.  Skips credential and verifier.
 *
 * @param xdrs  XDR stream in DECODE mode.
 * @param xid   Receives the transaction ID.
 * @param prog  Receives the RPC program number.
 * @param vers  Receives the RPC version number.
 * @param proc  Receives the procedure number.
 * @return 0 on success, -1 on error.
 */
struct rpc_gss_cred;
int rpc_decode_call_header(XDR *xdrs, uint32_t *xid,
                           uint32_t *prog, uint32_t *vers, uint32_t *proc,
                           uint32_t *cred_flavor,
                           struct rpc_gss_cred *gss_cred,
                           uint32_t *out_uid, uint32_t *out_gid,
                           uint32_t *out_aux_gids, uint32_t *out_aux_gid_count);

/**
 * Encode an ONC-RPC accepted reply header.
 *
 * Writes xid, msg_type=REPLY, reply_stat=MSG_ACCEPTED,
 * verf=AUTH_NONE, accept_stat=SUCCESS.
 *
 * @param xdrs  XDR stream in ENCODE mode.
 * @param xid   Transaction ID (echoed from request).
 * @return 0 on success, -1 on error.
 */
int rpc_encode_accepted_reply(XDR *xdrs, uint32_t xid);

/**
 * Encode an ONC-RPC error reply (PROG_UNAVAIL, PROC_UNAVAIL, etc.).
 *
 * @param xdrs         XDR stream in ENCODE mode.
 * @param xid          Transaction ID.
 * @param accept_stat  One of the accept_stat enum values.
 * @return 0 on success, -1 on error.
 */
int rpc_encode_error_reply(XDR *xdrs, uint32_t xid, uint32_t accept_stat);

#endif /* XDR_CODEC_H */

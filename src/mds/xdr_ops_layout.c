/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 */
/*
 * xdr_ops_layout.c -- pNFS layout XDR decoders and result encoders.
 */

#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#include "xdr_codec.h"
#include "xdr_internal.h"

/* FF_FLAGS_STRIPE_LEASE lives in layout_types.h, but that header's
 * enum layout_iomode collides with compound.h's #define macros of the
 * same names.  Duplicate the single constant here to avoid the clash. */
#ifndef FF_FLAGS_STRIPE_LEASE
#define FF_FLAGS_STRIPE_LEASE 0x00000010
#endif

/* -----------------------------------------------------------------------
 * pNFS layout XDR decoders
 * ----------------------------------------------------------------------- */

bool decode_op_layoutget(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_layoutget *a = &op->arg.layoutget;
    uint32_t signal;

    /* signal_layout_avail (bool) */
    if (!xdr_uint32_t(xdrs, &signal)) {
        return false;
}
    a->signal_available = (signal != 0);
    if (!xdr_uint32_t(xdrs, &a->layout_type)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &a->iomode)) {
        return false;
}
    if (!xdr_uint64_t(xdrs, &a->offset)) {
        return false;
}
    if (!xdr_uint64_t(xdrs, &a->length)) {
        return false;
}
    if (!xdr_uint64_t(xdrs, &a->minlength)) {
        return false;
}
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) {
        return false;
}
    return xdr_uint32_t(xdrs, &a->maxcount);
}


bool decode_op_getdeviceinfo(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_getdeviceinfo *a = &op->arg.getdeviceinfo;
    uint32_t bm[NFS4_BITMAP_WORDS];
    uint32_t bm_words;

    if (!xdr_opaque_decode(xdrs, (char *)a->deviceid, NFS4_DEVICEID4_SIZE)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &a->layout_type)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &a->maxcount)) {
        return false;
}
    /* notify_types bitmap -- consume but ignore. */
    return xdr_nfs4_bitmap_decode(xdrs, bm, NFS4_BITMAP_WORDS, &bm_words);
}


bool decode_op_layoutreturn(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_layoutreturn *a = &op->arg.layoutreturn;
    uint32_t reclaim;

    if (!xdr_uint32_t(xdrs, &reclaim)) {
        return false;
}
    a->reclaim = (reclaim != 0);
    if (!xdr_uint32_t(xdrs, &a->layout_type)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &a->iomode)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &a->return_type)) {
        return false;
}

    if (a->return_type == LAYOUTRETURN4_FILE) {
        uint32_t body_len;

        if (!xdr_uint64_t(xdrs, &a->offset)) {
            return false;
}
        if (!xdr_uint64_t(xdrs, &a->length)) {
            return false;
}
        if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) {
            return false;
}
        /* lrf_body opaque -- consume length + skip. */
        if (!xdr_uint32_t(xdrs, &body_len)) {
            return false;
}
        if (body_len > 0) {
            char skip[4096];

            if (body_len > sizeof(skip)) {
                return false;
}
            if (!xdr_opaque_decode(xdrs, skip, body_len)) {
                return false;
}
        }
    }
    return true;
}


bool decode_op_layoutcommit(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_layoutcommit *a = &op->arg.layoutcommit;
    uint32_t reclaim, new_ofs, time_set;

    if (!xdr_uint64_t(xdrs, &a->offset)) {
        return false;
}
    if (!xdr_uint64_t(xdrs, &a->length)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &reclaim)) {
        return false;
}
    a->reclaim = (reclaim != 0);
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) {
        return false;
}

    /* newoffset4: bool + optional uint64 */
    if (!xdr_uint32_t(xdrs, &new_ofs)) {
        return false;
}
    a->new_offset = (new_ofs != 0);
    if (a->new_offset) {
        if (!xdr_uint64_t(xdrs, &a->last_write_offset)) {
            return false;
}
    }

    /* newtime4: bool + optional nfstime4 */
    if (!xdr_uint32_t(xdrs, &time_set)) {
        return false;
}
    a->time_modify_set = (time_set != 0);
    if (a->time_modify_set) {
        if (!xdr_nfs4_time_decode(xdrs, &a->time_modify)) {
            return false;
}
    }

    /* layoutupdate4: layout_type + body opaque */
    if (!xdr_uint32_t(xdrs, &a->layout_type)) {
        return false;
}
    {
        uint32_t body_len;

        if (!xdr_uint32_t(xdrs, &body_len)) {
            return false;
}
        if (body_len > 0) {
            char skip[4096];

            if (body_len > sizeof(skip)) {
                return false;
}
            if (!xdr_opaque_decode(xdrs, skip, body_len)) {
                return false;
}
        }
    }
    return true;
}

/* -----------------------------------------------------------------------
 * LAYOUTERROR decoder (extracted from inline case)
 * ----------------------------------------------------------------------- */

bool decode_op_layouterror(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_layouterror *a = &op->arg.layouterror;
    uint32_t st_val, op_val, de_count;
    /* RFC 7862 S15.6: stateid, offset, length, device_error<> */
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->offset)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->length)) { return false; }
    /* device_error4 array -- decode count + first entry only. */
    if (!xdr_uint32_t(xdrs, &de_count)) { return false; }
    if (de_count == 0) { return true; }
    if (!xdr_opaque_decode(xdrs, (char *)a->deviceid, NFS4_DEVICEID4_SIZE)) {
        return false;
    }
    if (!xdr_uint32_t(xdrs, &st_val)) { return false; }
    a->status = (enum nfs4_status)st_val;
    if (!xdr_uint32_t(xdrs, &op_val)) { return false; }
    a->opnum = (enum nfs_opnum4)op_val;
    /* Consume remaining device_error entries (decode-and-discard). */
    for (uint32_t dei = 1; dei < de_count; dei++) {
        char skip_devid[NFS4_DEVICEID4_SIZE];
        uint32_t skip_st, skip_op;
        if (!xdr_opaque_decode(xdrs, skip_devid, NFS4_DEVICEID4_SIZE)) {
            return false;
        }
        if (!xdr_uint32_t(xdrs, &skip_st)) { return false; }
        if (!xdr_uint32_t(xdrs, &skip_op)) { return false; }
    }
    return true;
}

/* -----------------------------------------------------------------------
 * LAYOUTSTATS decoder (extracted from inline case)
 * ----------------------------------------------------------------------- */

bool decode_op_layoutstats(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_layoutstats *a = &op->arg.layoutstats;
    if (!xdr_uint64_t(xdrs, &a->offset)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->length)) { return false; }
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->read_count)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->read_bytes)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->write_count)) { return false; }
    if (!xdr_uint64_t(xdrs, &a->write_bytes)) { return false; }
    if (!xdr_opaque_decode(xdrs, (char *)a->deviceid, NFS4_DEVICEID4_SIZE)) {
        return false;
    }
    return true;
}


/* -----------------------------------------------------------------------
 * ff_layout4 body encoder (RFC 8435, Phase 3)
 *
 * Wire: stripe_unit(4) mirror_count(4)
 *   per mirror: ds_count(4)
 *     per DS: deviceid(16) efficiency(4) stateid(16)
 *             fh_count(4) fh_len(4) fh
 *             ffl_user(4) ffl_group(4)
 *   ff_flags(4)
 *
 * Phase C / Step 6 of docs/hpc-nto1-plan.md -- the encoder splits into
 * two helpers selected by lg->ff_xdr_form:
 *
 *   _legacy  -- emits one ff_mirror4 per stripe, each with
 *              ds_count = mirror_count.  This is what every previous
 *              version of the daemon produced and is what pre-Linux-
 *              6.18 flex-files clients consume.
 *   _striped -- emits a single ff_mirror4 whose ds_count equals the
 *              stripe count, listing every DS in the layout.  Linux
 *              6.18+ clients consume this as the per-stripe DS
 *              dispatch table (dss_id = offset / stripe_unit) and
 *              fan out parallel WRITE / READ RPCs across all DSes.
 *
 * Both helpers emit the same RFC 8435 wire format -- only the
 * mirror_count and per-mirror ds_count differ.
 * ----------------------------------------------------------------------- */

/**
 * Emit one ff_data_server4 (deviceid + efficiency + stateid + fh
 * count + fh + ffl_user + ffl_group) into @p body_xdrs.  Returns
 * false on any XDR put failure.
 */
static bool encode_ff_data_server4(XDR *body_xdrs,
                                   const struct nfs4_ff_ds *fds)
{
    /* deviceid(16) */
    if (!xdr_opaque_encode(body_xdrs,
            (const char *)fds->deviceid,
            NFS4_DEVICEID4_SIZE)) {
        return false;
    }

    /* efficiency */
    uint32_t eff = fds->efficiency;
    if (!xdr_uint32_t(body_xdrs, &eff)) {
        return false;
    }

    /* stateid (seqid + other) */
    if (!xdr_nfs4_stateid_encode(body_xdrs, &fds->stateid)) {
        return false;
    }

    /* fh_count = 1, then fh */
    uint32_t fh_count = 1;
    if (!xdr_uint32_t(body_xdrs, &fh_count)) {
        return false;
    }
    uint32_t fh_len = fds->nfs_fh_len;
    if (!xdr_uint32_t(body_xdrs, &fh_len)) {
        return false;
    }
    if (fh_len > 0) {
        if (!xdr_opaque_encode(body_xdrs,
                (const char *)fds->nfs_fh, fh_len)) {
            return false;
        }
    }

    /* ffds_user, ffds_group: fattr4_owner = utf8str_mixed (string). */
    char ubuf[32], gbuf[32];
    int ulen = snprintf(ubuf, sizeof(ubuf), "%u", fds->ffl_user);
    int glen = snprintf(gbuf, sizeof(gbuf), "%u", fds->ffl_group);
    uint32_t ul = (uint32_t)ulen, gl = (uint32_t)glen;
    if (!xdr_uint32_t(body_xdrs, &ul)) { return false; }
    if (ul > 0 && !xdr_opaque_encode(body_xdrs, ubuf, ul)) { return false; }
    if (!xdr_uint32_t(body_xdrs, &gl)) { return false; }
    if (gl > 0 && !xdr_opaque_encode(body_xdrs, gbuf, gl)) { return false; }
    return true;
}

/**
 * Emit the trailing ff_flags + ffl_stats_collect_hint pair.  Both
 * forms share this footer.
 */
static bool encode_ff_layout4_footer(XDR *body_xdrs,
                                     const struct nfs4_res_layoutget *lg)
{
    uint32_t flags = lg->ff_flags;
    if (!xdr_uint32_t(body_xdrs, &flags)) {
        return false;
    }
    uint32_t hint = 0;
    if (!xdr_uint32_t(body_xdrs, &hint)) {
        return false;
    }
    /* Append stripe lease duration when FF_FLAGS_STRIPE_LEASE is set.
     * The client reads this immediately after ffl_stats_collect_hint
     * and uses it as the per-stripe lease window (ms). */
    if (flags & FF_FLAGS_STRIPE_LEASE) {
        uint32_t lease_ms = lg->stripe_lease_duration_ms;
        if (!xdr_uint32_t(body_xdrs, &lease_ms)) {
            return false;
        }
    }
    return true;
}

/**
 * Legacy form: ff_mirror_count outer entries, each with ds_count
 * mirror_count.  ff_mirrors[] is populated flat by compound_layout.c
 * as [stripe0/mirror0, stripe0/mirror1, stripe1/mirror0, ...].
 *
 * RFC 8435 says each ff_mirror4 groups all DSs for ONE stripe
 * (mirror_count DSs inside).  When mirror_count==1, flat and grouped
 * are identical.  For mirror_count>1, we must group by stripe: emit
 * stripe_count ff_mirror4 entries, each containing mirror_count
 * ff_data_server4 entries.
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static bool encode_ff_layout4_body_legacy(XDR *body_xdrs,
                                          const struct nfs4_res_layoutget *lg)
{
    uint32_t total = lg->ff_mirror_count;
    uint32_t mc_per_stripe = 1;
    uint32_t sc;

    /*
     * Determine stripe_count and mirror_count from the flat array.
     * ff_mirror_count = stripe_count * mirror_count.
     * If ds_count > 0, derive from layout metadata.
     */
    if (lg->ds_count > 0 && total > lg->ds_count) {
        mc_per_stripe = total / lg->ds_count;
        if (mc_per_stripe == 0) {
            mc_per_stripe = 1;
        }
    }
    sc = (mc_per_stripe > 0) ? total / mc_per_stripe : total;
    if (sc == 0) {
        sc = total;
        mc_per_stripe = 1;
    }

    /* stripe_unit */
    uint64_t su = (uint64_t)lg->stripe_unit;
    if (!xdr_uint64_t(body_xdrs, &su)) {
        return false;
    }

    /* mirror_count = number of ff_mirror4 entries = stripe_count. */
    if (!xdr_uint32_t(body_xdrs, &sc)) {
        return false;
    }

    for (uint32_t s = 0; s < sc; s++) {
        /* ds_count inside this mirror entry = mc_per_stripe */
        if (!xdr_uint32_t(body_xdrs, &mc_per_stripe)) {
            return false;
        }

        for (uint32_t m = 0; m < mc_per_stripe; m++) {
            uint32_t flat_idx = s * mc_per_stripe + m;
            const struct nfs4_ff_ds *fds;

            if (flat_idx >= total ||
                lg->ff_mirrors[flat_idx].ds == NULL) {
                /* Defensive: an unallocated mirror or
                 * geometry mismatch.  Skip the slot rather
                 * than dereferencing NULL; the wire frame
                 * stays well-formed because xdr_uint32_t
                 * still emitted ds_count above. */
                break;
            }
            fds = &lg->ff_mirrors[flat_idx].ds[0];
            if (!encode_ff_data_server4(body_xdrs, fds)) {
                return false;
            }
        }
    }

    return encode_ff_layout4_footer(body_xdrs, lg);
}

/**
 * Striped form (Phase C / Step 6): a single ff_mirror4 whose ds_count
 * equals the stripe_count.  Linux 6.18+ flex-files clients consume
 * this as the per-stripe DS dispatch table (dss_id = offset /
 * stripe_unit), enabling true per-client parallelism across all DSes.
 *
 * Producer contract: lg->ff_mirror_count == 1 and
 * lg->ff_mirrors[0].ds_count == stripe_count.  The encoder defends
 * against violations by trusting ds_count and tolerating a NULL
 * inner array (treated as zero entries, which produces an empty but
 * well-formed mirror).
 */
static bool encode_ff_layout4_body_striped(XDR *body_xdrs,
                                           const struct nfs4_res_layoutget *lg)
{
    /* stripe_unit -- the load-bearing field for the 6.18+ client's
     * dss_id dispatch.  See compound_layout.c's runtime BUG check. */
    uint64_t su = (uint64_t)lg->stripe_unit;
    if (!xdr_uint64_t(body_xdrs, &su)) {
        return false;
    }

    /* Striped form: exactly one ff_mirror4. */
    uint32_t outer = 1;
    if (!xdr_uint32_t(body_xdrs, &outer)) {
        return false;
    }

    /* Pull the inner (deeper) ds_count + ds[] from the first
     * mirror.  An unallocated mirror appears as ds_count == 0
     * and ds == NULL; the encoder still emits a valid frame. */
    uint32_t inner_count = 0;
    const struct nfs4_ff_ds *inner_ds = NULL;
    if (lg->ff_mirror_count > 0 && lg->ff_mirrors != NULL) {
        inner_count = lg->ff_mirrors[0].ds_count;
        inner_ds = lg->ff_mirrors[0].ds;
    }
    if (!xdr_uint32_t(body_xdrs, &inner_count)) {
        return false;
    }
    if (inner_ds != NULL) {
        for (uint32_t i = 0; i < inner_count; i++) {
            if (!encode_ff_data_server4(body_xdrs, &inner_ds[i])) {
                return false;
            }
        }
    }

    return encode_ff_layout4_footer(body_xdrs, lg);
}

/**
 * Compute the worst-case encoded size of one ff_data_server4 entry.
 * Bounded by deviceid(16) + efficiency(4) + stateid(16) + fh_count(4)
 * + fh_len(4) + fh(MDS_NFS_FH_MAX, padded) + user_len(4) + user_str(!~12)
 * + group_len(4) + group_str(!~12).  ~200 bytes upper bound; we round
 * up to 256 for headroom and 4-byte XDR alignment.
 */
#define FF_DATA_SERVER4_MAX_BYTES  256

/**
 * Body encoder dispatcher.  Selects the wire form by lg->ff_xdr_form
 * (default LEGACY) and emits the body opaque<> payload around the
 * chosen helper.
 *
 * Phase 2 of the QA plan -- heap-back the body buffer.  The legacy
 * fixed `char body_buf[8192]` overflowed silently for >~40 DSes;
 * heap-allocating sized to (ds_count * FF_DATA_SERVER4_MAX_BYTES) +
 * 64 bytes of frame overhead lets the same code path serve 4-stripe
 * and 1024-stripe layouts without truncation.  Allocation failure is
 * surfaced as a hard XDR encode error; the compound dispatcher in
 * rpc_server.c maps that to NFS4ERR_RESOURCE.
 */
static bool encode_ff_layout4_body(XDR *xdrs,
                                   const struct nfs4_res_layoutget *lg)
{
    XDR body_xdrs;
    uint32_t body_len;
    bool ok;
    char *body_buf = NULL;
    size_t body_cap;
    uint32_t worst_ds_count;

    /* Worst-case DS count across both wire forms.  Striped emits
     * inner ff_mirror[0].ds_count entries; legacy emits ff_mirror_count
     * (one per stripe*mirror).  Take the max so a producer that
     * filled both fields still encodes safely. */
    worst_ds_count = lg->ff_mirror_count;
    if (lg->ff_mirror_count > 0 && lg->ff_mirrors != NULL) {
        if (lg->ff_mirrors[0].ds_count > worst_ds_count) {
            worst_ds_count = lg->ff_mirrors[0].ds_count;
        }
    }
    if (worst_ds_count == 0) {
        worst_ds_count = 1;
    }

    /* Frame overhead: stripe_unit(8) + outer_count(4) + per-mirror
     * ds_count(4) repeated up to ff_mirror_count + ff_flags(4) +
     * stats_collect_hint(4) ~ 16 + 4*ff_mirror_count.  Cap at 64
     * bytes plus 4 bytes per mirror to bound the overhead. */
    body_cap = (size_t)worst_ds_count * FF_DATA_SERVER4_MAX_BYTES;
    body_cap += 64 + 4 * (size_t)lg->ff_mirror_count;

    body_buf = malloc(body_cap);
    if (body_buf == NULL) {
        return false;
    }

    xdrmem_ncreate(&body_xdrs, body_buf, (uint32_t)body_cap, XDR_ENCODE);

    if (lg->ff_xdr_form == NFS4_FF_XDR_FORM_STRIPED) {
        ok = encode_ff_layout4_body_striped(&body_xdrs, lg);
    } else {
        ok = encode_ff_layout4_body_legacy(&body_xdrs, lg);
    }
    if (!ok) {
        free(body_buf);
        return false;
    }

    body_len = xdr_getpos(&body_xdrs);

    /* Encode as opaque loc_body */
    if (!xdr_uint32_t(xdrs, &body_len)) {
        free(body_buf);
        return false;
    }
    if (!xdr_opaque_encode(xdrs, body_buf, body_len)) {
        free(body_buf);
        return false;
    }
    free(body_buf);
    return true;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
bool encode_res_layoutget(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_layoutget *lg = &r->res.layoutget;
    int32_t roc = lg->return_on_close ? 1 : 0;
    uint32_t count = 1; /* one layout segment */
    uint32_t layout_type = lg->layout_type;
    uint32_t iomode = lg->iomode;
    uint64_t offset = lg->offset;
    uint64_t length = lg->length;
    char *body_buf = NULL;
    size_t body_cap;
    XDR body_xdrs;
    uint32_t body_len;

    /* return_on_close */
    if (!xdr_putbool(xdrs, roc)) {
        return false;
}
    /* layout stateid */
    if (!xdr_nfs4_stateid_encode(xdrs, &lg->stateid)) {
        return false;
}
    /* layout count */
    if (!xdr_uint32_t(xdrs, &count)) {
        return false;
}
    /* layout4: offset, length, iomode, type */
    if (!xdr_uint64_t(xdrs, &offset)) {
        return false;
}
    if (!xdr_uint64_t(xdrs, &length)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &iomode)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &layout_type)) {
        return false;
}

    /* Branch on layout type for loc_body encoding. */
    if (layout_type == LAYOUT4_FLEX_FILES) {
        if (!encode_ff_layout4_body(xdrs, lg)) {
            return false;
        }
        return true;
    }

    /*
     * loc_body: nfsv4_1_file_layout4
     *   [deviceid 16][nfl_util u32][first_stripe_index u32]
     *   [pattern_offset u64][fh_list count + fhs]
     *
     * Phase 2 of the QA plan -- heap-back the body buffer.  Sized to
     * deviceid(16) + util(4) + first_idx(4) + pat_offset(8) +
     * fh_count(4) + per-DS (fh_len(4) + fh(MDS_NFS_FH_MAX, padded)).
     * Allocation failure surfaces as XDR encode error. */
    body_cap = 64 + (size_t)lg->ds_count * (4 + MDS_NFS_FH_MAX + 4);
    body_buf = malloc(body_cap);
    if (body_buf == NULL) {
        return false;
    }
    xdrmem_ncreate(&body_xdrs, body_buf, (uint32_t)body_cap, XDR_ENCODE);
    {
        /* RFC 5661 S13.4.2: nfl_util encodes stripe_unit in low
         * 31 bits, and NFL4_UFLG_COMMIT_THRU_MDS in bit 31. */
#define NFL4_UFLG_COMMIT_THRU_MDS 0x80000000U
        uint32_t nfl_util = lg->stripe_unit & ~NFL4_UFLG_COMMIT_THRU_MDS;
        uint32_t first_idx = 0;
        uint64_t pat_offset = 0;
        uint32_t fh_count = lg->ds_count;

        /* deviceid -- use first DS's deviceid. */
        if (lg->ds_count > 0) {
            if (!xdr_opaque_encode(&body_xdrs,
                    (const char *)lg->ds[0].deviceid,
                    NFS4_DEVICEID4_SIZE)) {
                free(body_buf);
                return false;
            }
        } else {
            uint8_t zeros[NFS4_DEVICEID4_SIZE] = {0};

            if (!xdr_opaque_encode(&body_xdrs,
                    (const char *)zeros, NFS4_DEVICEID4_SIZE)) {
                free(body_buf);
                return false;
            }
        }
        if (!xdr_uint32_t(&body_xdrs, &nfl_util)) {
            free(body_buf);
            return false;
        }
        if (!xdr_uint32_t(&body_xdrs, &first_idx)) {
            free(body_buf);
            return false;
        }
        if (!xdr_uint64_t(&body_xdrs, &pat_offset)) {
            free(body_buf);
            return false;
        }

        /* fh_list: count of nfs_fh4 */
        if (!xdr_uint32_t(&body_xdrs, &fh_count)) {
            free(body_buf);
            return false;
        }
        for (uint32_t i = 0; i < fh_count; i++) {
            uint32_t fh_len = lg->ds[i].nfs_fh_len;

            if (!xdr_uint32_t(&body_xdrs, &fh_len)) {
                free(body_buf);
                return false;
            }
            if (fh_len > 0) {
                if (!xdr_opaque_encode(&body_xdrs,
                        (const char *)lg->ds[i].nfs_fh, fh_len)) {
                    free(body_buf);
                    return false;
                }
            }
        }
    }
    body_len = xdr_getpos(&body_xdrs);

    /* Encode loc_body as opaque<> */
    if (!xdr_uint32_t(xdrs, &body_len)) {
        free(body_buf);
        return false;
}
    if (body_len > 0) {
        if (!xdr_opaque_encode(xdrs, body_buf, body_len)) {
            free(body_buf);
            return false;
}
    }
    free(body_buf);
    return true;
}


/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
bool encode_res_getdeviceinfo(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_getdeviceinfo *gdi = &r->res.getdeviceinfo;
    uint32_t layout_type = gdi->layout_type;
    char dev_buf[4096];
    XDR dev_xdrs;
    uint32_t dev_len;
    uint32_t stripe_indices_count = 0;
    const uint32_t empty_bm[NFS4_BITMAP_WORDS] = {0, 0};

    /* device_addr4: layout_type + da_addr_body opaque */
    if (!xdr_uint32_t(xdrs, &layout_type)) {
        return false;
}

    /*
     * For files layout: nfsv4_1_file_layout_ds_addr4
     * For flex-files: ff_device_addr4 (version prefix inside opaque body)
     *   [stripe_indices count + indices][multipath_list count + addrs]
     */
    xdrmem_ncreate(&dev_xdrs, dev_buf, sizeof(dev_buf), XDR_ENCODE);


    {
        /* stripe_indices: only for files layout, not flex-files. */
        if (layout_type != LAYOUT4_FLEX_FILES) {
            if (!xdr_uint32_t(&dev_xdrs, &stripe_indices_count)) {
                return false;
            }
        }

        /*
         * Flex-files (RFC 8435): ffda_netaddrs is a SINGLE
         * multipath_list4 = netaddr4<> -- a flat array of addresses.
         *
         * Files layout (RFC 5661): multipath_ds_list<> is an array
         * of multipath_list4 -- two-level: [DS count][per-DS count + addrs].
         */
        if (layout_type == LAYOUT4_FLEX_FILES) {
            /* Flat address list: total count, then all netaddr4 entries. */
            uint32_t total_addrs = 0;
            for (uint32_t i = 0; i < gdi->ds_count; i++) {
                total_addrs += (gdi->ds[i].endpoint_count > 0)
                               ? gdi->ds[i].endpoint_count : 1;
            }
            if (!xdr_uint32_t(&dev_xdrs, &total_addrs)) {
                return false;
            }
        } else {
            /* Files layout: outer count = number of DSs. */
            uint32_t mp_count = gdi->ds_count;
            if (!xdr_uint32_t(&dev_xdrs, &mp_count)) {
                return false;
            }
        }

        for (uint32_t i = 0; i < gdi->ds_count; i++) {
            uint32_t addr_count = (gdi->ds[i].endpoint_count > 0)
                                 ? gdi->ds[i].endpoint_count : 1;
            char netid_buf[8];
            if (gdi->ds[i].endpoint_count > 0 &&
                gdi->ds[i].endpoints[0].netid[0] != '\0') {
                (void)snprintf(netid_buf, sizeof(netid_buf),
                         "%s", gdi->ds[i].endpoints[0].netid);
            } else {
                (void)snprintf(netid_buf, sizeof(netid_buf), "tcp");
            }
            char uaddr[280];
            char *uaddr_p = uaddr;
            uint16_t port = gdi->ds[i].port;

            /* Files layout: per-DS inner multipath count.
             * Flex-files: skip -- already wrote flat count above. */
            if (layout_type != LAYOUT4_FLEX_FILES) {
                if (!xdr_uint32_t(&dev_xdrs, &addr_count)) {
                    return false;
                }
            }

            /* Encode each endpoint (netaddr4 = netid + uaddr). */
            for (uint32_t ep = 0; ep < addr_count; ep++) {
                const char *ep_netid;
                const char *ep_host;
                uint16_t ep_port;

                if (gdi->ds[i].endpoint_count > 0 &&
                    ep < gdi->ds[i].endpoint_count) {
                    ep_netid = gdi->ds[i].endpoints[ep].netid;
                    ep_host = gdi->ds[i].endpoints[ep].host;
                    ep_port = gdi->ds[i].endpoints[ep].port;
                } else {
                    ep_netid = netid_buf;
                    ep_host = gdi->ds[i].addr;
                    ep_port = port;
                }
                if (ep_host[0] == '\0') {
                    ep_host = gdi->ds[i].addr;
                }

                char ep_netid_buf[8];
                (void)snprintf(ep_netid_buf, sizeof(ep_netid_buf),
                         "%s", ep_netid);
                char *ep_np = ep_netid_buf;
                if (!xdr_string_encode(&dev_xdrs, &ep_np, 16)) {
                    return false;
                }

                (void)snprintf(uaddr, sizeof(uaddr), "%s.%u.%u",
                         ep_host,
                         (unsigned)(ep_port >> 8) & 0xFF,
                         (unsigned)(ep_port) & 0xFF);
                if (!xdr_string_encode(&dev_xdrs, &uaddr_p, 280)) {
                    return false;
                }
            }  /* end endpoint loop */
        }
    }

    /* DS version info (RFC 8435 ff_device_addr4).
     * Advertise NFSv4.2 to match the DS server capabilities and
     * avoid RPC client conflicts when the kernel already holds a
     * v4.2 connection to the DS from mount probing. */
    {
        uint32_t ver_count = 1;
        uint32_t ds_ver = 3, ds_min = 0;
        uint32_t ds_rsz = 1048576, ds_wsz = 1048576;
        uint32_t ds_tight = 0;
        if (!xdr_uint32_t(&dev_xdrs, &ver_count)) { return false; }
        if (!xdr_uint32_t(&dev_xdrs, &ds_ver)) { return false; }
        if (!xdr_uint32_t(&dev_xdrs, &ds_min)) { return false; }
        if (!xdr_uint32_t(&dev_xdrs, &ds_rsz)) { return false; }
        if (!xdr_uint32_t(&dev_xdrs, &ds_wsz)) { return false; }
        if (!xdr_uint32_t(&dev_xdrs, &ds_tight)) { return false; }
    }
    dev_len = xdr_getpos(&dev_xdrs);

    if (!xdr_uint32_t(xdrs, &dev_len)) {
        return false;
}
    if (dev_len > 0) {
        if (!xdr_opaque_encode(xdrs, dev_buf, dev_len)) {
            return false;
}
    }

    /* notification bitmap (empty). */
    return xdr_nfs4_bitmap_encode(xdrs, empty_bm, NFS4_BITMAP_WORDS);
}


bool encode_res_layoutreturn(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_layoutreturn *lr = &r->res.layoutreturn;
    int32_t present = lr->stateid_present ? 1 : 0;

    if (!xdr_putbool(xdrs, present)) {
        return false;
}
    if (lr->stateid_present) {
        return xdr_nfs4_stateid_encode(xdrs, &lr->stateid);
}
    return true;
}


bool encode_res_layoutcommit(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_layoutcommit *lc = &r->res.layoutcommit;
    int32_t has_size = lc->new_size ? 1 : 0;

    if (!xdr_putbool(xdrs, has_size)) {
        return false;
}
    if (lc->new_size) {
        uint64_t sz = lc->new_size_value;

        return xdr_uint64_t(xdrs, &sz);
    }
    return true;
}

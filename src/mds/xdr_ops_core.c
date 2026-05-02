/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 */
/*
 * xdr_ops_core.c — NFSv4.1 per-op XDR decoders and result encoders.
 */

#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#include "xdr_codec.h"
#include "xdr_internal.h"

/* From xdr_codec.c — needed for READDIR inline attr encoding. */
extern uint32_t mds_type_to_nfs4(enum mds_file_type t);

/* -----------------------------------------------------------------------
 * Per-operation argument decoders
 *
 * Each decodes from XDR into the corresponding nfs4_arg_* struct.
 * The opnum has already been consumed by the compound decoder.
 * ----------------------------------------------------------------------- */

bool decode_op_sequence(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_sequence *a = &op->arg.sequence;
    uint32_t cache_this;

    if (!xdr_opaque_decode(xdrs, (char *)a->session_id, SESSION_ID_SIZE)) {
        return false;
}
    /* RFC 8881 §18.46.1: sa_sequenceid before sa_slotid on wire. */
    if (!xdr_uint32_t(xdrs, &a->seq_id)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &a->slot_id)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &a->highest_slot_id)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &cache_this)) {
        return false;
}
    a->cache_this = (cache_this != 0);
    return true;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
bool decode_op_exchange_id(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_exchange_id *a = &op->arg.exchange_id;
    uint32_t co_len = 0;
    uint32_t impl_count;

    /* client_owner4: co_verifier + co_ownerid<NFS4_OPAQUE_LIMIT> */
    if (!xdr_opaque_decode(xdrs, (char *)a->verifier, NFS4_VERIFIER_SIZE)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &co_len)) {
        return false;
}
    if (co_len > CO_OWNERID_MAX) {
        return false;
}
    if (!xdr_opaque_decode(xdrs, (char *)a->co_ownerid, co_len)) {
        return false;
}
    a->co_ownerid_len = co_len;

    /* flags */
    if (!xdr_uint32_t(xdrs, &a->eia_flags)) {
        return false;
}

    /* state_protect4_a: sp_how (we expect SP4_NONE = 0). */
    {
        uint32_t sp_how;

        if (!xdr_uint32_t(xdrs, &sp_how)) {
            return false;
}
        /* We accept SP4_NONE only; other values we'd skip. */
    }

    /* nfs_impl_id4 array (optional, we skip). */
    if (!xdr_uint32_t(xdrs, &impl_count)) {
        return false;
}
    /* Skip impl_id entries by consuming raw bytes. */
    /* For Phase 1 we expect most clients send 0 or 1 entries. */
    for (uint32_t i = 0; i < impl_count; i++) {
        /* nfs_impl_id4: nii_domain (string) + nii_name (string) +
         * nii_date (nfstime4). We need to skip these. */
        uint32_t slen;

        /* nii_domain */
        if (!xdr_uint32_t(xdrs, &slen)) {
            return false;
}
        if (slen > 0) {
            char skip[1024];
            uint32_t padded = (slen + 3) & ~3U;

            if (padded > sizeof(skip)) {
                return false;
}
            if (!xdr_opaque_decode(xdrs, skip, slen)) {
                return false;
}
        }
        /* nii_name */
        if (!xdr_uint32_t(xdrs, &slen)) {
            return false;
}
        if (slen > 0) {
            char skip[1024];

            if (slen > sizeof(skip)) {
                return false;
}
            if (!xdr_opaque_decode(xdrs, skip, slen)) {
                return false;
}
        }
        /* nii_date: int64 + uint32 */
        {
            int64_t s;
            uint32_t ns;

            if (!xdr_int64_t(xdrs, &s)) {
                return false;
}
            if (!xdr_uint32_t(xdrs, &ns)) {
                return false;
}
        }
    }
    return true;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
bool decode_op_create_session(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_create_session *a = &op->arg.create_session;
    if (!xdr_uint64_t(xdrs, &a->clientid)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &a->seqid)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &a->csa_flags)) {
        return false;
}

    /* fore_chan_attrs: headerpadsize, maxrequestsize, maxresponsesize,
     * maxresponsesize_cached, maxoperations, maxrequests. */
    {
        uint32_t pad, maxreq, maxresp, maxcached, maxops;

        if (!xdr_uint32_t(xdrs, &pad)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxreq)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxresp)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxcached)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxops)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &a->fore_slots)) {
            return false;
        }
        /* ca_rdma_ird<1>: variable-length array. */
        {
            uint32_t ird_count;
            if (!xdr_uint32_t(xdrs, &ird_count)) { return false; }
            for (uint32_t ri = 0; ri < ird_count; ri++) {
                uint32_t ird_val;
                if (!xdr_uint32_t(xdrs, &ird_val)) { return false; }
            }
        }
    }

    /* back_chan_attrs: same 7 fields (6 + ca_rdma_ird). */
    {
        uint32_t pad, maxreq, maxresp, maxcached, maxops;

        if (!xdr_uint32_t(xdrs, &pad)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxreq)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxresp)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxcached)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxops)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &a->back_slots)) {
            return false;
        }
        /* ca_rdma_ird<1> */
        {
            uint32_t ird_count;
            if (!xdr_uint32_t(xdrs, &ird_count)) { return false; }
            for (uint32_t ri = 0; ri < ird_count; ri++) {
                uint32_t ird_val;
                if (!xdr_uint32_t(xdrs, &ird_val)) { return false; }
            }
        }
    }

    /* cb_program — store in arg for backchannel use. */
    if (!xdr_uint32_t(xdrs, &a->cb_prog)) {
        return false;
}

    /* cb_sec_parms4 array — capture first flavor, skip rest. */
    {
        uint32_t count;

        if (!xdr_uint32_t(xdrs, &count)) {
            return false;
}
        a->cb_sec_flavor = 0; /* default AUTH_NONE */
        for (uint32_t i = 0; i < count; i++) {
            uint32_t flavor;

            if (!xdr_uint32_t(xdrs, &flavor)) {
                return false;
}
            if (i == 0) {
                a->cb_sec_flavor = flavor;
}
            /* AUTH_NONE (0) has no additional data.
             * AUTH_SYS (1) has variable-length body — skip it. */
            if (flavor == 1) {
                /* authsys_parms: stamp(4) + machinename(string) +
                 * uid(4) + gid(4) + gids<>(uint32 array). */
                uint32_t stamp, mname_len, uid, gid, ngids;
                if (!xdr_uint32_t(xdrs, &stamp)) { return false; }
                if (!xdr_uint32_t(xdrs, &mname_len)) { return false; }
                if (mname_len > 0) {
                    uint32_t padded = (mname_len + 3) & ~3u;
                    char mskip[256];
                    if (padded > sizeof(mskip)) { return false; }
                    if (!xdr_opaque_decode(xdrs, mskip, padded)) { return false; }
                }
                if (!xdr_uint32_t(xdrs, &uid)) { return false; }
                if (!xdr_uint32_t(xdrs, &gid)) { return false; }
                if (!xdr_uint32_t(xdrs, &ngids)) { return false; }
                for (uint32_t gi = 0; gi < ngids; gi++) {
                    uint32_t aux;
                    if (!xdr_uint32_t(xdrs, &aux)) { return false; }
                }
            }
        }
    }
    return true;
}


bool decode_op_destroy_session(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_destroy_session *a = &op->arg.destroy_session;

    return xdr_opaque_decode(xdrs, (char *)a->session_id, SESSION_ID_SIZE);
}


bool decode_op_putfh(XDR *xdrs, struct nfs4_op *op)
{
    return xdr_nfs4_fh_decode_full(xdrs, &op->arg.putfh.fh);
}


bool decode_op_lookup(XDR *xdrs, struct nfs4_op *op)
{
    /* component4 = utf8str_cs = opaque<> */
    uint32_t len = 0;

    if (!xdr_uint32_t(xdrs, &len)) {
        return false;
}
    if (len >= MDS_MAX_NAME) {
        return false;
}
    if (!xdr_opaque_decode(xdrs, op->arg.lookup.name, len)) {
        return false;
}
    op->arg.lookup.name[len] = '\0';
    return true;
}


bool decode_op_getattr(XDR *xdrs, struct nfs4_op *op)
{
    /* GETATTR args = bitmap4 attr_request.
     * Store the requested bitmap so the encoder returns only
     * the attributes the client asked for. */
    uint32_t words;

    memset(op->arg.getattr.requested, 0, sizeof(op->arg.getattr.requested));
    return xdr_nfs4_bitmap_decode(xdrs, op->arg.getattr.requested,
                                  NFS4_BITMAP_WORDS, &words);
}


bool decode_op_setattr(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_setattr *a = &op->arg.setattr;

    /* stateid4 */
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) {
        return false;
    }
    /* Mark non-special stateids for validation. */
    {
        static const uint8_t zeros[12] = {0};
        static const uint8_t ones[12] = {
            0xff,0xff,0xff,0xff, 0xff,0xff,0xff,0xff,
            0xff,0xff,0xff,0xff
        };
        a->has_stateid = (memcmp(a->stateid.other, zeros, 12) != 0 &&
                          memcmp(a->stateid.other, ones, 12) != 0);
    }

    /* fattr4 */
    memset(&a->attrs, 0, sizeof(a->attrs));
    return xdr_nfs4_fattr_decode_ex(xdrs, &a->attrs, &a->mask,
                                    &a->layout_hint);
}


bool decode_op_create(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_create *a = &op->arg.create;
    uint32_t nfs_type;
    uint32_t name_len;

    /* createtype4: type (enum) + type-specific data. */
    if (!xdr_uint32_t(xdrs, &nfs_type)) {
        return false;
}

    switch (nfs_type) {
    case 1: a->type = MDS_FTYPE_REG;     break;
    case 2: a->type = MDS_FTYPE_DIR;     break;
    case 5: a->type = MDS_FTYPE_SYMLINK; break;
    default: a->type = MDS_FTYPE_REG;    break;
    }

    /* Symlinks carry linkdata (linktext4 = utf8str_cs) before the name. */
    a->link_target_len = 0;
    if (nfs_type == 5) {
        uint32_t lt_len;
        if (!xdr_uint32_t(xdrs, &lt_len)) {
            return false;
        }
        if (lt_len >= sizeof(a->link_target)) {
            return false;
        }
        if (lt_len > 0 && !xdr_opaque_decode(xdrs, a->link_target, lt_len)) {
            return false;
        }
        a->link_target[lt_len] = '\0';
        a->link_target_len = lt_len;
    }

    /* component4 objname */
    if (!xdr_uint32_t(xdrs, &name_len)) {
        return false;
}
    if (name_len >= MDS_MAX_NAME) {
        return false;
}
    if (!xdr_opaque_decode(xdrs, a->name, name_len)) {
        return false;
}
    a->name[name_len] = '\0';

    /* createattrs4 (fattr4) — we parse mode from it. */
    {
        struct mds_inode attrs;
        uint32_t mask;

        memset(&attrs, 0, sizeof(attrs));
        if (!xdr_nfs4_fattr_decode(xdrs, &attrs, &mask)) {
            return false;
}
        a->mode = attrs.mode;
        a->uid = attrs.uid;
        a->gid = attrs.gid;
    }
    return true;
}


bool decode_op_remove(XDR *xdrs, struct nfs4_op *op)
{
    uint32_t len = 0;

    if (!xdr_uint32_t(xdrs, &len)) {
        return false;
}
    if (len >= MDS_MAX_NAME) {
        return false;
}
    if (!xdr_opaque_decode(xdrs, op->arg.remove.name, len)) {
        return false;
}
    op->arg.remove.name[len] = '\0';
    return true;
}


bool decode_op_rename(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_rename *a = &op->arg.rename;
    uint32_t len;

    /* oldname */
    if (!xdr_uint32_t(xdrs, &len)) {
        return false;
}
    if (len >= MDS_MAX_NAME) {
        return false;
}
    if (!xdr_opaque_decode(xdrs, a->src_name, len)) {
        return false;
}
    a->src_name[len] = '\0';

    /* newname */
    if (!xdr_uint32_t(xdrs, &len)) {
        return false;
}
    if (len >= MDS_MAX_NAME) {
        return false;
}
    if (!xdr_opaque_decode(xdrs, a->dst_name, len)) {
        return false;
}
    a->dst_name[len] = '\0';
    return true;
}


bool decode_op_link(XDR *xdrs, struct nfs4_op *op)
{
    uint32_t len = 0;

    if (!xdr_uint32_t(xdrs, &len)) {
        return false;
}
    if (len >= MDS_MAX_NAME) {
        return false;
}
    if (!xdr_opaque_decode(xdrs, op->arg.link.name, len)) {
        return false;
}
    op->arg.link.name[len] = '\0';
    return true;
}


bool decode_op_readdir(XDR *xdrs, struct nfs4_op *op)
{
    /* READDIR4args: cookie(uint64), cookieverf(8 bytes),
     * dircount(uint32), maxcount(uint32), attr_request(bitmap4). */
    uint64_t cookie;
    char verf[8];
    uint32_t dircount, maxcount;
    uint32_t words;

    memset(op->arg.readdir.requested, 0, sizeof(op->arg.readdir.requested));
    if (!xdr_uint64_t(xdrs, &cookie)) {
        return false;
}
    if (!xdr_opaque_decode(xdrs, verf, 8)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &dircount)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &maxcount)) {
        return false;
}
    if (!xdr_nfs4_bitmap_decode(xdrs, op->arg.readdir.requested,
                                NFS4_BITMAP_WORDS, &words)) {
        return false;
}

    op->arg.readdir.cookie = cookie;
    op->arg.readdir.dircount = dircount;
    op->arg.readdir.maxcount = maxcount;
    return true;
}


/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
bool decode_op_open(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_open *a = &op->arg.open;
    uint32_t seqid_unused;
    uint32_t open_claim;
    uint32_t createmode;
    memset(&a->layout_hint, 0, sizeof(a->layout_hint));

    /* seqid (deprecated in v4.1 but still on wire) */
    if (!xdr_uint32_t(xdrs, &seqid_unused)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &a->share_access)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &a->share_deny)) {
        return false;
}

    /* open_owner4: clientid + owner<> — store for per-owner state. */
    {
        uint64_t clientid_wire;
        uint32_t owner_len;

        if (!xdr_uint64_t(xdrs, &clientid_wire)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &owner_len)) {
            return false;
}
        if (owner_len > NFS4_OPEN_OWNER_MAX) {
            return false;
}
        if (owner_len > 0) {
            if (!xdr_opaque_decode(xdrs, (char *)a->open_owner,
                                  owner_len)) {
                return false;
}
        }
        a->open_owner_len = owner_len;
    }

    /* openflag4: opentype (OPEN4_NOCREATE=0, OPEN4_CREATE=1) */
    {
        uint32_t opentype;

        if (!xdr_uint32_t(xdrs, &opentype)) {
            return false;
}
        a->create = (opentype == 1);
        if (a->create) {
            if (!xdr_uint32_t(xdrs, &createmode)) {
                return false;
}
            a->createmode = (enum nfs4_createmode)createmode;

            if (createmode == CREATEMODE_EXCLUSIVE4) {
                /*
                 * EXCLUSIVE4 (RFC 8881 §18.16.3): 8-byte create
                 * verifier for replay detection.  No fattr.
                 */
                if (!xdr_uint64_t(xdrs, &a->create_verf)) {
                    return false;
                }
                a->createmode = CREATEMODE_EXCLUSIVE4;
                a->mode = 0644;
                a->uid = 0;
                a->gid = 0;
            } else if (createmode == CREATEMODE_EXCLUSIVE4_1) {
                /*
                 * EXCLUSIVE4_1 (RFC 8881 §18.16.3): 8-byte verifier
                 * + fattr4.  Store verifier, then decode attrs.
                 */
                if (!xdr_uint64_t(xdrs, &a->create_verf)) {
                    return false;
                }
                a->createmode = CREATEMODE_EXCLUSIVE4_1;
                {
                    struct mds_inode attrs;
                    uint32_t mask;
                    memset(&attrs, 0, sizeof(attrs));
                    if (!xdr_nfs4_fattr_decode_ex(xdrs, &attrs, &mask,
                                                  &a->layout_hint)) {
                        return false;
                    }
                    a->mode = attrs.mode;
                    a->uid = attrs.uid;
                    a->gid = attrs.gid;
                }
            } else {
                /* UNCHECKED4 or GUARDED4: fattr4 follows. */
                if (createmode == 1) {
                    a->createmode = CREATEMODE_GUARDED4;
                } else {
                    a->createmode = CREATEMODE_UNCHECKED4;
                }
                {
                    struct mds_inode attrs;
                    uint32_t mask;
                    memset(&attrs, 0, sizeof(attrs));
                    if (!xdr_nfs4_fattr_decode_ex(xdrs, &attrs, &mask,
                                                  &a->layout_hint)) {
                        return false;
                    }
                    a->mode = attrs.mode;
                    a->uid = attrs.uid;
                    a->gid = attrs.gid;
                }
            }
        }
    }

    /* open_claim4 */
    if (!xdr_uint32_t(xdrs, &open_claim)) {
        return false;
}
    a->claim = (enum nfs4_claim_type)open_claim;

    switch (a->claim) {
    case CLAIM_NULL: {
        uint32_t name_len;

        if (!xdr_uint32_t(xdrs, &name_len)) {
            return false;
}
        if (name_len >= MDS_MAX_NAME) {
            return false;
}
        if (!xdr_opaque_decode(xdrs, a->name, name_len)) {
            return false;
}
        a->name[name_len] = '\0';
        break;
    }
    case CLAIM_FH:
        /* No additional args for CLAIM_FH. */
        break;
    default:
        return false;
    }
    return true;
}


bool decode_op_close(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_close *a = &op->arg.close;
    uint32_t seqid_unused;

    /* seqid (deprecated in v4.1 but on wire) */
    if (!xdr_uint32_t(xdrs, &seqid_unused)) {
        return false;
}
    return xdr_nfs4_stateid_decode(xdrs, &a->stateid);
}


bool decode_op_openattr(XDR *xdrs, struct nfs4_op *op)
{
    /* OPENATTR4args: createdir(bool). */
    uint32_t create;

    if (!xdr_uint32_t(xdrs, &create)) {
        return false;
}
    op->arg.openattr.create_dir = (create != 0);
    return true;
}


bool decode_op_read(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_read *a = &op->arg.read;

    /* READ4args: stateid4 + offset(uint64) + count(uint32). */
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) {
        return false;
}
    if (!xdr_uint64_t(xdrs, &a->offset)) {
        return false;
}
    return xdr_uint32_t(xdrs, &a->count);
}


bool decode_op_write(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_write *a = &op->arg.write;
    uint32_t stable_how;
    uint32_t data_len;

    /* WRITE4args: stateid4 + offset(uint64) + stable_how(uint32)
     *             + data(opaque<>). */
    if (!xdr_nfs4_stateid_decode(xdrs, &a->stateid)) {
        return false;
}
    if (!xdr_uint64_t(xdrs, &a->offset)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &stable_how)) {
        return false;
}
    /* stable_how: UNSTABLE4=0, DATA_SYNC4=1, FILE_SYNC4=2 — consumed. */

    /* data: length-prefixed opaque. */
    if (!xdr_uint32_t(xdrs, &data_len)) {
        return false;
}
    if (data_len > MDS_XATTR_VAL_MAX) {
        return false;
}
    if (data_len > 0) {
        if (!xdr_opaque_decode(xdrs, (char *)a->data, data_len)) {
            return false;
}
    }
    a->data_len = data_len;
    return true;
}

/* -----------------------------------------------------------------------
 * RECLAIM_COMPLETE decoder (extracted from inline case)
 * ----------------------------------------------------------------------- */

bool decode_op_reclaim_complete(XDR *xdrs, struct nfs4_op *op)
{
    struct nfs4_arg_reclaim_complete *a = &op->arg.reclaim_complete;
    uint32_t bval;
    if (!xdr_uint32_t(xdrs, &bval)) { return false; }
    a->rca_one_fs = (bval != 0);
    return true;
}


bool encode_res_sequence(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_sequence *s = &r->res.sequence;
    uint32_t status_flags = s->status_flags;

    if (!xdr_opaque_encode(xdrs, (const char *)s->session_id,
                           SESSION_ID_SIZE)) {
        return false;
}
    /* RFC 8881 §18.46.3: sr_sequenceid before sr_slotid */
    if (!xdr_uint32_t(xdrs, (uint32_t *)&s->seq_id)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, (uint32_t *)&s->slot_id)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, (uint32_t *)&s->highest_slot_id)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, (uint32_t *)&s->target_highest_slot_id)) {
        return false;
}
    return xdr_uint32_t(xdrs, &status_flags);
}


bool encode_res_exchange_id(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_exchange_id *e = &r->res.exchange_id;
    uint64_t clientid = e->clientid;
    uint32_t seqid = e->seqid;
    uint32_t flags = e->eir_flags;
    uint32_t sp_how = 0; /* SP4_NONE */

    if (!xdr_uint64_t(xdrs, &clientid)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &seqid)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &flags)) {
        return false;
}

    /* state_protect4_r: SP4_NONE */
    if (!xdr_uint32_t(xdrs, &sp_how)) {
        return false;
}

    /* server_owner4: so_minor_id(uint64) + so_major_id(opaque<>) */
    {
        uint64_t minor_id = 0;
        char major[] = "pnfs-mds";
        char *p = major;

        if (!xdr_uint64_t(xdrs, &minor_id)) {
            return false;
}
        if (!xdr_string_encode(xdrs, &p, 256)) {
            return false;
}
    }

    /* server_scope (opaque<>) */
    {
        char scope[] = "pnfs-mds.peakaio.com";
        char *p = scope;

        if (!xdr_string_encode(xdrs, &p, 256)) {
            return false;
}
    }

    /* server_impl_id (array of nfs_impl_id4 — we return 0 entries). */
    {
        uint32_t count = 0;

        if (!xdr_uint32_t(xdrs, &count)) {
            return false;
}
    }
    return true;
}


bool encode_res_create_session(XDR *xdrs,
                                      const struct nfs4_result *r)
{
    const struct nfs4_res_create_session *cs = &r->res.create_session;
    uint32_t csr_flags = cs->csr_flags;

    if (!xdr_opaque_encode(xdrs, (const char *)cs->session_id,
                           SESSION_ID_SIZE)) {
        return false;
}

    /* csr_sequence: RFC 8881 §18.36.3 — the sequence ID used for
     * this CREATE_SESSION (echoed back to the client). */
    {
        uint32_t csr_seq = cs->csr_sequence;
        if (!xdr_uint32_t(xdrs, &csr_seq)) {
            return false;
}
    }

    if (!xdr_uint32_t(xdrs, (uint32_t *)&cs->csr_flags)) {
        return false;
}

    /* fore_chan_attrs: pad, maxreq, maxresp, maxcached, maxops, maxreqs */
    {
        uint32_t pad = 0;
        uint32_t maxreq = 1048576;
        uint32_t maxresp = 1048576;
        uint32_t maxcached = 65536;
        uint32_t maxops = NFS4_MAX_OPS;
        uint32_t slots = cs->fore_slots;

        if (!xdr_uint32_t(xdrs, &pad)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxreq)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxresp)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxcached)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxops)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &slots)) {
            return false;
}
        /* ca_rdma_ird<1>: 0-length array (no RDMA). */
        {
            uint32_t rdma_ird_count = 0;
            if (!xdr_uint32_t(xdrs, &rdma_ird_count)) {
                return false;
}
        }
    }

    /* back_chan_attrs */
    {
        uint32_t pad = 0;
        uint32_t maxreq = 4096;
        uint32_t maxresp = 4096;
        uint32_t maxcached = 0;
        uint32_t maxops = 2;
        uint32_t slots = cs->back_slots;

        if (!xdr_uint32_t(xdrs, &pad)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxreq)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxresp)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxcached)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &maxops)) {
            return false;
}
        if (!xdr_uint32_t(xdrs, &slots)) {
            return false;
}
        /* ca_rdma_ird<1>: 0-length array (no RDMA). */
        {
            uint32_t rdma_ird_count = 0;
            if (!xdr_uint32_t(xdrs, &rdma_ird_count)) {
                return false;
}
        }
    }

    (void)csr_flags;
    return true;
}


bool encode_res_getfh(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_fh_desc *fh = &r->res.getfh.fh;
    if (fh->owner_mds_id != 0) {
        return xdr_nfs4_fh_encode_v1(xdrs, fh->owner_mds_id,
                                      fh->fileid, fh->generation);
    }
    return xdr_nfs4_fh_encode(xdrs, fh->fileid);
}


bool encode_res_getattr(XDR *xdrs, const struct nfs4_result *r)
{
    /* Use the client's requested bitmap, not all-supported. */
    const uint32_t *req = r->res.getattr.requested;

    if (r->res.getattr.has_fs_space) {
        struct xdr_fattr_fs_space fs = {
            .space_avail = r->res.getattr.space_avail,
            .space_free  = r->res.getattr.space_free,
            .space_total = r->res.getattr.space_total,
        };
        return xdr_nfs4_fattr_encode_ex(xdrs, &r->res.getattr.inode,
                                        req, &fs,
                                        r->res.getattr.has_referral
                                            ? r->res.getattr.referral_server
                                            : NULL,
                                        r->res.getattr.has_referral
                                            ? r->res.getattr.referral_rootpath
                                            : NULL,
                                        r->res.getattr.has_referral
                                            ? r->res.getattr.referral_fs_root
                                            : NULL);
    }
    return xdr_nfs4_fattr_encode_ex(xdrs, &r->res.getattr.inode, req,
                                    NULL,
                                    r->res.getattr.has_referral
                                        ? r->res.getattr.referral_server
                                        : NULL,
                                    r->res.getattr.has_referral
                                        ? r->res.getattr.referral_rootpath
                                        : NULL,
                                    r->res.getattr.has_referral
                                        ? r->res.getattr.referral_fs_root
                                        : NULL);
}


bool encode_res_create(XDR *xdrs, const struct nfs4_result *r)
{
    uint32_t all[NFS4_BITMAP_WORDS];

    /* Read change_info directly from the create result.  We must
     * NOT fall back to r->res.change_info.{before,after} here:
     * nfs4_res_create and nfs4_res_change_info share storage in
     * the nfs4_result union, so those fields alias parts of the
     * freshly-written create.inode (fileid, type|mode, etc.) and
     * would emit a nonsensical change_info4 (we observed values
     * like before=fileid, after=mode<<32|type pre-fix).  op_create
     * populates parent_change_before/after from the directory
     * inode, which is what RFC 8881 §18.4 requires. */
    if (!encode_change_info(xdrs,
        r->res.create.parent_change_before,
        r->res.create.parent_change_after)) {
        return false;
}
    /* cinfo followed by bitmap of attrs set. Return supported bitmap. */
    build_all_requested(all);
    return xdr_nfs4_bitmap_encode(xdrs, all, NFS4_BITMAP_WORDS);
}


static bool readdir_requested_any(const uint32_t requested[NFS4_BITMAP_WORDS])
{
    uint32_t i;

    for (i = 0; i < NFS4_BITMAP_WORDS; i++) {
        if (requested[i] != 0) {
            return true;
        }
    }
    return false;
}

static bool encode_readdir_minimal_attrs(
    XDR *xdrs,
    const struct mds_cat_dirent *entry,
    const uint32_t requested[NFS4_BITMAP_WORDS])
{
    struct mds_inode inode;
    uint32_t minimal[NFS4_BITMAP_WORDS];

    memset(&inode, 0, sizeof(inode));
    memset(minimal, 0, sizeof(minimal));
    inode.fileid = entry->fileid;
    inode.type = (enum mds_file_type)entry->type;

    if (!readdir_requested_any(requested)) {
        nfs4_bitmap_set(minimal, FATTR4_TYPE);
        nfs4_bitmap_set(minimal, FATTR4_FILEID);
    } else {
        if (nfs4_bitmap_test(requested, FATTR4_TYPE)) {
            nfs4_bitmap_set(minimal, FATTR4_TYPE);
        }
        if (nfs4_bitmap_test(requested, FATTR4_FILEID)) {
            nfs4_bitmap_set(minimal, FATTR4_FILEID);
        }
        if (nfs4_bitmap_test(requested, FATTR4_MOUNTED_ON_FILEID)) {
            nfs4_bitmap_set(minimal, FATTR4_MOUNTED_ON_FILEID);
        }
    }

    return xdr_nfs4_fattr_encode(xdrs, &inode, minimal);
}

bool encode_res_readdir(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_readdir *rd = &r->res.readdir;
    uint32_t i;
    uint8_t cookieverf[8];
    bool have_requested = readdir_requested_any(rd->requested);

    /* R1.1: cookieverf = dir inode change attribute (8 bytes, BE). */
    {
        uint64_t cv = htobe64(rd->dir_change);
        memcpy(cookieverf, &cv, 8);
    }
    if (!xdr_opaque_encode(xdrs, (char *)cookieverf, 8)) {
        return false;
    }

    /* entry4 linked list: each is value_follows(bool) + entry data.
     * Final entry followed by value_follows=false + eof(bool). */
    for (i = 0; i < rd->count; i++) {
        int32_t value_follows = 1; /* true */
        uint64_t cookie = rd->entries[i].fileid;
        uint32_t name_len = (uint32_t)strlen(rd->entries[i].name);

        if (!xdr_putbool(xdrs, value_follows)) {
            return false;
        }
        /* cookie */
        if (!xdr_uint64_t(xdrs, &cookie)) {
            return false;
        }
        /* name (component4 = utf8str_cs) */
        if (!xdr_uint32_t(xdrs, &name_len)) {
            return false;
        }
        if (!xdr_opaque_encode(xdrs, rd->entries[i].name, name_len)) {
            return false;
        }

        if (have_requested && rd->entry_attrs_valid[i]) {
            if (!xdr_nfs4_fattr_encode(xdrs, &rd->entry_attrs[i],
                                       rd->requested)) {
                return false;
            }
        } else {
            if (!encode_readdir_minimal_attrs(xdrs, &rd->entries[i],
                                              rd->requested)) {
                return false;
            }
        }
    }
    /* No more entries. */
    {
        int32_t value_follows = 0; /* false */

        if (!xdr_putbool(xdrs, value_follows)) {
            return false;
        }
    }
    /* eof */
    {
        int32_t eof_val = rd->eof ? 1 : 0;

        if (!xdr_putbool(xdrs, eof_val)) {
            return false;
        }
    }
    return true;
}


bool encode_res_open(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_open *o = &r->res.open;
    uint32_t rflags = 0;
    const uint32_t empty_bm[NFS4_BITMAP_WORDS] = {0, 0};
    uint32_t deleg_type = 0; /* OPEN_DELEGATE_NONE */

    /* stateid4 */
    if (!xdr_nfs4_stateid_encode(xdrs, &o->stateid)) {
        return false;
}
    /* change_info4 */
    if (!encode_change_info(xdrs,
        o->inode.change > 0 ? o->inode.change - 1 : 0,
        o->inode.change)) {
        return false;
}
    /* rflags */
    if (!xdr_uint32_t(xdrs, &rflags)) {
        return false;
}
    /* bitmap4 attrset (attrs that were set on create) */
    if (!xdr_nfs4_bitmap_encode(xdrs, empty_bm, NFS4_BITMAP_WORDS)) {
        return false;
}
    /* delegation: OPEN_DELEGATE_NONE */
    return xdr_uint32_t(xdrs, &deleg_type);
}


bool encode_res_close(XDR *xdrs, const struct nfs4_result *r)
{
    return xdr_nfs4_stateid_encode(xdrs, &r->res.close.stateid);
}


bool encode_res_read(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_read *rd = &r->res.read;
    int32_t eof_val = rd->eof ? 1 : 0;
    uint32_t len = rd->data_len;

    /* READ4resok: eof(bool) + data(opaque<>). */
    if (!xdr_putbool(xdrs, eof_val)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &len)) {
        return false;
}
    if (len > 0) {
        if (!xdr_opaque_encode(xdrs, (const char *)rd->data, len)) {
            return false;
}
    }
    return true;
}


bool encode_res_write(XDR *xdrs, const struct nfs4_result *r)
{
    const struct nfs4_res_write *wr = &r->res.write;
    uint32_t count = wr->count;
    uint32_t committed = wr->committed;
    uint64_t verf = wr->write_verf;

    /* WRITE4resok: count(uint32) + committed(uint32) + writeverf(8). */
    if (!xdr_uint32_t(xdrs, &count)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &committed)) {
        return false;
}
    return xdr_uint64_t(xdrs, &verf);
}


bool encode_res_remove(XDR *xdrs, const struct nfs4_result *r)
{
    return encode_change_info(xdrs,
        r->res.change_info.before ? r->res.change_info.before : 0,
        r->res.change_info.after ? r->res.change_info.after : 1);
}


bool encode_res_rename(XDR *xdrs, const struct nfs4_result *r)
{
    /* RENAME4res: source_cinfo + target_cinfo. */
    if (!encode_change_info(xdrs,
            r->res.change_info.src_before ? r->res.change_info.src_before : 0,
            r->res.change_info.src_after ? r->res.change_info.src_after : 1)) {
        return false;
    }
    return encode_change_info(xdrs,
        r->res.change_info.before ? r->res.change_info.before : 0,
        r->res.change_info.after ? r->res.change_info.after : 1);
}


bool encode_res_link(XDR *xdrs, const struct nfs4_result *r)
{
    return encode_change_info(xdrs,
        r->res.change_info.before ? r->res.change_info.before : 0,
        r->res.change_info.after ? r->res.change_info.after : 1);
}

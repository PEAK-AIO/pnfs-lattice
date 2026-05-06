/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * hpc_shared.c -- Phase B helpers for the HPC-Shared file mode.
 *
 * See hpc_shared.h for the public contract.
 */

#include <ctype.h>
#include <string.h>
#include <time.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "compound_internal.h"  /* cat_getattr, cat_setattr, compound_inode_invalidate */
#include "mds_catalogue.h"
#include "ds_prealloc.h"        /* ds_prealloc_batch + struct */
#include "hpc_shared.h"

/* Recognise a "true-like" value byte string per the documented set:
 * "1", "true", "yes", "on" (case-insensitive).  Empty value clears.
 * Anything else clears too (defensive default).  This mirrors common
 * boolean xattr conventions on Linux. */
static bool hpc_shared_value_is_truthy(const void *value, uint32_t value_len)
{
    const char *s;
    char buf[8];

    if (value == NULL || value_len == 0) {
        return false;
    }

    /* Trim trailing whitespace / NUL when copying so callers that
     * pass C-strings with a trailing newline (a common shell idiom:
     * `echo 1 | setfattr -v -`) still match. */
    s = (const char *)value;
    {
        uint32_t copy = value_len;
        uint32_t i;

        while (copy > 0) {
            char last = s[copy - 1];
            if (last == '\0' || last == '\n' || last == '\r' ||
                last == ' '  || last == '\t') {
                copy--;
                continue;
            }
            break;
        }
        if (copy == 0) {
            return false;
        }
        if (copy >= sizeof(buf)) {
            return false;  /* unrecognised long value -> clear */
        }
        for (i = 0; i < copy; i++) {
            buf[i] = (char)tolower((unsigned char)s[i]);
        }
        buf[copy] = '\0';
    }

    if (strcmp(buf, "1")    == 0) { return true; }
    if (strcmp(buf, "true") == 0) { return true; }
    if (strcmp(buf, "yes")  == 0) { return true; }
    if (strcmp(buf, "on")   == 0) { return true; }
    return false;
}

enum nfs4_status hpc_shared_xattr_apply(struct compound_data *cd,
                                        const void *value,
                                        uint32_t value_len,
                                        bool remove)
{
    struct mds_inode inode;
    enum mds_status st;
    bool want_set;
    uint32_t old_flags;

    if (cd == NULL) {
        return NFS4ERR_INVAL;
    }
    if (!cd->current_fh_set) {
        return NFS4ERR_NOFILEHANDLE;
    }
    /* Privilege gate: trusted.* xattrs are CAP_SYS_ADMIN-only on
     * Linux.  We mirror that here so an unprivileged user cannot
     * cause every file in their tree to occupy a wide HPC pre-warm
     * (DoS surface).  AUTH_SYS uid == 0 is the closest analogue
     * available to the MDS. */
    if (cd->cred_uid != 0) {
        return NFS4ERR_PERM;
    }

    if (remove) {
        want_set = false;
    } else {
        want_set = hpc_shared_value_is_truthy(value, value_len);
    }

    st = cat_getattr(cd, cd->current_fh.fileid, &inode);
    if (st != MDS_OK) {
        return mds_status_to_nfs4(st);
    }

    old_flags = inode.flags;
    if (want_set) {
        inode.flags |= MDS_IFLAG_HPC_SHARED;
    } else {
        inode.flags &= ~MDS_IFLAG_HPC_SHARED;
    }

    /* No-op fast path: do not bump change counter or hit the
     * catalogue when the bit is already in the desired state. */
    if (inode.flags == old_flags) {
        return NFS4_OK;
    }

    st = cat_setattr(cd, cd->current_fh.fileid, &inode, MDS_ATTR_FLAGS);
    if (st != MDS_OK) {
        return mds_status_to_nfs4(st);
    }
    compound_inode_invalidate(cd, cd->current_fh.fileid);
    return NFS4_OK;
}

void hpc_shared_inherit_from_parent(struct compound_data *cd,
                                    uint64_t parent_fileid,
                                    struct mds_inode *child)
{
    struct mds_inode parent;

    if (cd == NULL || child == NULL) {
        return;
    }
    if (parent_fileid == 0 || child->fileid == 0) {
        return;
    }
    if (child->flags & MDS_IFLAG_HPC_SHARED) {
        return;  /* already inherited (e.g. via fused create path) */
    }

    if (cat_getattr(cd, parent_fileid, &parent) != MDS_OK) {
        return;
    }
    if (!(parent.flags & MDS_IFLAG_HPC_SHARED)) {
        return;
    }

    child->flags |= MDS_IFLAG_HPC_SHARED;
    /* Best-effort: a transient catalogue error here only forfeits
     * the inheritance for this CREATE; the file is still created
     * and usable.  Operator can re-flag via setfattr. */
    (void)cat_setattr(cd, child->fileid, child, MDS_ATTR_FLAGS);
    compound_inode_invalidate(cd, child->fileid);
}

enum mds_status hpc_shared_xattr_synthesize_value(struct compound_data *cd,
                                                  uint64_t fileid,
                                                  void *out, uint32_t out_cap,
                                                  uint32_t *out_len)
{
    struct mds_inode inode;
    enum mds_status st;
    char *buf;

    if (cd == NULL || out == NULL || out_len == NULL || out_cap < 2) {
        return MDS_ERR_INVAL;
    }
    if (fileid == 0) {
        return MDS_ERR_INVAL;
    }

    st = cat_getattr(cd, fileid, &inode);
    if (st != MDS_OK) {
        return st;
    }

    buf = (char *)out;
    buf[0] = (inode.flags & MDS_IFLAG_HPC_SHARED) ? '1' : '0';
    buf[1] = '\0';
    *out_len = 1;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Phase C / Steps 4 + 5 of docs/hpc-nto1-plan.md -- wide HPC create.
 *
 * Implementation notes:
 *  - We bypass mds_cat_ns_create here because that path is wired to
 *    the single-DS prealloc-pop machinery (it expects a non-NULL
 *    prealloc ctx for REG files and persists a 1-DS stripe map as
 *    a side effect).  Passing prealloc=NULL behaves backend-
 *    specifically and would either fail outright or skip the inode
 *    initialisation we need.  Going through the lower-level
 *    primitives (alloc_fileid + inode_put + dirent_put) gives us full
 *    control: the inode is created with the HPC_SHARED flag already
 *    set, no spurious 1-DS stripe map ever exists, and the wide
 *    stripe map is a single mds_cat_stripe_map_put after FH capture.
 *
 *  - Two failure modes need explicit cleanup:
 *      (a) ds_prealloc_batch failure -- ds_prealloc_batch's internal
 *          rollback already enqueued GC for any DS-side state it
 *          partially produced.  We only need to remove the inode +
 *          dirent we created in step 2.
 *      (b) mds_cat_stripe_map_put failure -- ds_prealloc_batch
 *          succeeded so we own real DS files; we GC-enqueue every
 *          slot before removing the inode + dirent.  Without this
 *          the DS files leak forever.
 *
 *  - The catalogue's two-phase txn (begin / commit) groups the
 *    inode + dirent + parent touch atomically.  The wide stripe map
 *    is a separate write because it can fail independently and we
 *    want the cleanup path to run only on the second write.
 * ----------------------------------------------------------------------- */

/**
 * Step 2: allocate fileid + insert inode (with HPC_SHARED flag) +
 * insert parent dirent + touch parent, all in one catalogue write
 * transaction.  Returns the freshly-built inode and its fileid.
 *
 * Caller's responsibility: on subsequent step failure, call
 * mds_cat_ns_remove(parent_fileid, name) to undo this step.
 */
static enum mds_status hpc_create_inode_and_dirent(
    struct mds_catalogue *cat,
    uint64_t parent_fileid,
    const char *name,
    uint32_t mode,
    uint64_t uid, uint64_t gid,
    struct mds_inode *out_inode)
{
    struct mds_cat_txn *txn = NULL;
    struct mds_inode child;
    uint64_t child_fid = 0;
    struct timespec now;
    enum mds_status st;

    st = mds_cat_alloc_fileid(cat, NULL, &child_fid);
    if (st != MDS_OK || child_fid == 0) {
        return (st == MDS_OK) ? MDS_ERR_NOMEM : st;
    }

    st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn);
    if (st != MDS_OK) {
        return st;
    }

    clock_gettime(CLOCK_REALTIME, &now);
    memset(&child, 0, sizeof(child));
    child.fileid = child_fid;
    child.type = MDS_FTYPE_REG;
    child.mode = mode;
    child.nlink = 1;
    child.uid = uid;
    child.gid = gid;
    child.atime = now;
    child.mtime = now;
    child.ctime = now;
    child.change = 1;
    child.generation = 1;
    child.parent_fileid = parent_fileid;
    /* Phase 3: HPC-Shared wide CREATE is a multi-row operation
     * (inode + dirent + parent in this txn, then stripe map in a
     * follow-up write).  We persist the inode with the
     * MDS_IFLAG_HPC_CREATE_PENDING flag set so that, if the MDS
     * crashes after the inode commit but before the stripe map
     * commit, the read path filters the orphan file from clients
     * (compound_inode_get / compound_lookup_local_child both check
     * the flag and return MDS_ERR_NOTFOUND).  The flag is cleared by
     * the caller after mds_cat_stripe_map_put returns MDS_OK. */
    child.flags = MDS_IFLAG_HPC_SHARED | MDS_IFLAG_HPC_CREATE_PENDING;

    st = mds_cat_inode_put(cat, txn, &child);
    if (st != MDS_OK) {
        mds_cat_txn_abort(txn);
        return st;
    }

    st = mds_cat_dirent_put(cat, txn, parent_fileid, name,
                            child_fid, (uint8_t)MDS_FTYPE_REG);
    if (st != MDS_OK) {
        mds_cat_txn_abort(txn);
        return st;
    }

    /* Best-effort parent touch.  A failure here is not fatal -- the
     * inode + dirent are already in the txn and will commit; the
     * parent stays at its current change counter, which matches the
     * test_helpers.h convention for direct catalogue writes. */
    {
        struct mds_inode parent_inode;
        if (mds_cat_ns_getattr(cat, parent_fileid,
                               &parent_inode) == MDS_OK) {
            parent_inode.mtime = now;
            parent_inode.ctime = now;
            parent_inode.change++;
            (void)mds_cat_inode_put(cat, txn, &parent_inode);
        }
    }

    st = mds_cat_txn_commit(txn);
    if (st != MDS_OK) {
        return MDS_ERR_IO;
    }

    *out_inode = child;
    return MDS_OK;
}

/* GC-enqueue every captured slot of a wide pre-warm batch.  Used by
 * the wide-create rollback path after persistence (step 4) failed but
 * the DS files at @p entries are already on disk.  Best-effort: a
 * failing enqueue does not abort the rollback. */
static void hpc_create_gc_enqueue_entries(
    struct mds_catalogue *cat, uint64_t fileid,
    const struct ds_prealloc_batch_result *batch)
{
    if (cat == NULL || batch == NULL || batch->entries == NULL) {
        return;
    }
    uint32_t total = batch->stripe_count * batch->mirror_count;
    for (uint32_t i = 0; i < total; i++) {
        if (batch->entries[i].nfs_fh_len == 0) {
            continue;
        }
        (void)mds_cat_gc_enqueue(cat, NULL, fileid,
                                 batch->entries[i].ds_id,
                                 batch->entries[i].nfs_fh,
                                 batch->entries[i].nfs_fh_len);
    }
}

enum mds_status hpc_shared_create_wide_layout(
    struct mds_catalogue   *cat,
    struct ds_prealloc_ctx *prealloc,
    uint64_t                parent_fileid,
    const char             *name,
    uint32_t                mode,
    uint64_t                uid,
    uint64_t                gid,
    uint32_t                stripe_count,
    uint32_t                mirror_count,
    uint32_t                stripe_unit,
    uint8_t                 required_transport,
    uint8_t                 preferred_transport,
    uint32_t                preferred_caps,
    bool                    strict_unique_ds,
    struct mds_inode       *out)
{
    struct mds_inode child;
    enum mds_status  st;

    if (cat == NULL || prealloc == NULL || name == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }
    if (stripe_count == 0 || stripe_count > MDS_MAX_STRIPES ||
        mirror_count == 0 || mirror_count > MDS_MAX_MIRRORS) {
        return MDS_ERR_INVAL;
    }
    /* QA Phase 5: HPC-Shared wide pre-warm has only been validated
     * for mirror_count == 1.  The N-to-1 workloads this path exists
     * for write checkpoint data once and pay-as-you-go on durability
     * via async replication / backup; live mirroring on every WRITE
     * is explicitly out of scope (see docs/hpc-shared-files.md
     * "Limits").  Reject mirror_count > 1 with NOSUPPORT instead of
     * silently routing it through an unvalidated code path. */
    if (mirror_count > 1) {
        return MDS_ERR_NOSUPPORT;
    }
    if (stripe_unit == 0) {
        stripe_unit = 65536;
    }

    /* Step 2: create inode + dirent in one catalogue txn.  Inode
     * carries MDS_IFLAG_HPC_SHARED from the start -- no follow-up
     * setattr needed. */
    st = hpc_create_inode_and_dirent(cat, parent_fileid, name,
                                     mode, uid, gid, &child);
    if (st != MDS_OK) {
        return st;
    }

    /* Step 3: capture FHs in parallel, using the freshly-allocated
     * fileid so the returned entries[] line up with the catalogue
     * inode the previous step persisted. */
    struct ds_prealloc_batch_request req;
    struct ds_prealloc_batch_result   batch;
    memset(&req, 0, sizeof(req));
    req.stripe_count        = stripe_count;
    req.mirror_count        = mirror_count;
    req.stripe_unit         = stripe_unit;
    req.required_mode       = DS_MODE_GENERIC;
    req.required_transport  = (required_transport != 0)
                              ? required_transport : DS_TRANSPORT_TCP;
    req.preferred_transport = preferred_transport;
    req.preferred_caps      = preferred_caps;
    req.strict_unique_ds    = strict_unique_ds;
    req.fileid_hint         = child.fileid;

    st = ds_prealloc_batch(prealloc, &req, &batch);
    if (st != MDS_OK) {
        /* Step 3 failure: ds_prealloc_batch already GC-enqueued any
         * partial DS-side state.  We only need to remove the inode +
         * dirent we created in step 2. */
        (void)mds_cat_ns_remove(cat, NULL, parent_fileid, name);
        return st;
    }

    /* Step 4: persist the wide stripe map. */
    st = mds_cat_stripe_map_put(cat, NULL, child.fileid,
                                batch.stripe_count, batch.stripe_unit,
                                batch.mirror_count, batch.entries);
    if (st != MDS_OK) {
        /* Step 4 failure: we own real DS files (FH capture
         * succeeded).  GC-enqueue every slot before removing the
         * inode + dirent so the worker reclaims DS bytes on its
         * next pass.  The PENDING flag on the inode keeps it
         * filtered from clients until mds_cat_ns_remove succeeds. */
        hpc_create_gc_enqueue_entries(cat, child.fileid, &batch);
        ds_prealloc_batch_result_destroy(&batch);
        (void)mds_cat_ns_remove(cat, NULL, parent_fileid, name);
        return MDS_ERR_IO;
    }

    /* Step 5 (Phase 3 of the QA plan): clear MDS_IFLAG_HPC_CREATE_PENDING
     * now that the stripe map is durable.  This is the commit point
     * that makes the file visible to NFS clients.
     *
     * If the clear fails, the inode remains in PENDING state but the
     * stripe map has already been persisted -- we cannot leave that
     * combination behind, so we fall back to the same rollback as a
     * step 4 failure: delete the stripe map row, GC-enqueue every
     * captured DS FH, and remove the inode + dirent.  Returning IO
     * matches step 4's semantics so the client retries cleanly. */
    {
        struct mds_inode clear_attrs = child;
        clear_attrs.flags &= ~MDS_IFLAG_HPC_CREATE_PENDING;
        st = mds_cat_ns_setattr(cat, NULL, child.fileid, &clear_attrs,
                                MDS_ATTR_FLAGS);
        if (st != MDS_OK) {
            (void)mds_cat_stripe_map_del(cat, NULL, child.fileid);
            hpc_create_gc_enqueue_entries(cat, child.fileid, &batch);
            ds_prealloc_batch_result_destroy(&batch);
            (void)mds_cat_ns_remove(cat, NULL, parent_fileid, name);
            return MDS_ERR_IO;
        }
        child.flags = clear_attrs.flags;
    }

    /* Success: surface the geometry on the returned inode for
     * caller convenience.  Note that the catalogue's persisted
     * inode does not carry stripe_count / stripe_unit / mirror_count
     * (those live in the stripe_map row), so we set them locally
     * for the caller to consult during the same compound. */
    child.stripe_count = batch.stripe_count;
    child.stripe_unit  = batch.stripe_unit;
    child.mirror_count = batch.mirror_count;

    ds_prealloc_batch_result_destroy(&batch);
    *out = child;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Phase G -- client striping hint helpers (pure functions).
 *
 * No catalogue / compound dependencies; safe to test in isolation.
 * Threshold constants live at file scope so they are uniformly
 * referenced by the decoder, the geometry picker, and the unit tests.
 * ----------------------------------------------------------------------- */

/* Tier thresholds (master plan S5 Phase G). */
#define HPC_HINT_TIER1_SIZE_BYTES   (1ULL << 40)   /* 1 TiB */
#define HPC_HINT_TIER1_CLIENT_COUNT (1024U)
#define HPC_HINT_TIER1_STRIPE_UNIT  (1U << 20)     /* 1 MiB */

#define HPC_HINT_TIER2_SIZE_BYTES   (64ULL << 30)  /* 64 GiB */
#define HPC_HINT_TIER2_STRIPE_COUNT (64U)
#define HPC_HINT_TIER2_STRIPE_UNIT  (512U << 10)   /* 512 KiB */

enum mds_status hpc_hint_decode_xdr_body(const void *buf, uint32_t buf_len,
                                         struct pnfs_hpc_hint *out)
{
    const uint8_t *p;

    if (buf == NULL || out == NULL || buf_len != HPC_HINT_BODY_SIZE) {
        return MDS_ERR_INVAL;
    }
    p = (const uint8_t *)buf;

    out->expected_file_size =
        ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
        ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
        ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
        ((uint64_t)p[6] <<  8) |  (uint64_t)p[7];

    out->expected_client_count =
        ((uint32_t)p[8]  << 24) | ((uint32_t)p[9]  << 16) |
        ((uint32_t)p[10] <<  8) |  (uint32_t)p[11];

    out->flags =
        ((uint32_t)p[12] << 24) | ((uint32_t)p[13] << 16) |
        ((uint32_t)p[14] <<  8) |  (uint32_t)p[15];

    return MDS_OK;
}

bool hpc_hint_select_geometry(const struct pnfs_hpc_hint *hint,
                              uint32_t online_ds_count,
                              uint32_t *stripe_count,
                              uint32_t *stripe_unit)
{
    if (hint == NULL || stripe_count == NULL || stripe_unit == NULL) {
        return false;
    }
    if (online_ds_count == 0) {
        return false;
    }

    /* Tier 1: very large file or very many clients.  Stripe across
     * everything we have, capped by MDS_MAX_STRIPES so we never emit
     * a layout the catalogue cannot persist. */
    if (hint->expected_file_size >= HPC_HINT_TIER1_SIZE_BYTES ||
        hint->expected_client_count >= HPC_HINT_TIER1_CLIENT_COUNT) {
        uint32_t cap = online_ds_count;
        if (cap > MDS_MAX_STRIPES) {
            cap = MDS_MAX_STRIPES;
        }
        *stripe_count = cap;
        *stripe_unit  = HPC_HINT_TIER1_STRIPE_UNIT;
        return true;
    }

    /* Tier 2: large file. */
    if (hint->expected_file_size >= HPC_HINT_TIER2_SIZE_BYTES) {
        uint32_t target = HPC_HINT_TIER2_STRIPE_COUNT;
        if (target > online_ds_count) {
            target = online_ds_count;
        }
        *stripe_count = target;
        *stripe_unit  = HPC_HINT_TIER2_STRIPE_UNIT;
        return true;
    }

    /* Tier 0: caller's defaults stand. */
    return false;
}

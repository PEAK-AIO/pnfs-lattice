/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * hpc_shared.c -- Phase B helpers for the HPC-Shared file mode.
 *
 * See hpc_shared.h for the public contract.
 */

#include <ctype.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "compound_internal.h"  /* cat_getattr, cat_setattr, compound_inode_invalidate */
#include "mds_catalogue.h"
#include "ds_prealloc.h"        /* ds_prealloc_batch + struct */
#include "hpc_shared.h"
#include "migration.h"

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

static bool hpc_pending_map_is_complete(
    uint32_t stripe_count,
    uint32_t stripe_unit,
    uint32_t mirror_count,
    const struct mds_ds_map_entry *entries)
{
    uint64_t entry_count;

    if (stripe_count == 0 || stripe_count > MDS_MAX_STRIPES ||
        stripe_unit == 0 || mirror_count == 0 ||
        mirror_count > MDS_MAX_MIRRORS || entries == NULL) {
        return false;
    }
    entry_count = (uint64_t)stripe_count * mirror_count;
    for (uint64_t entry_index = 0; entry_index < entry_count;
         entry_index++) {
        if (entries[entry_index].nfs_fh_len == 0 ||
            entries[entry_index].nfs_fh_len > MDS_NFS_FH_MAX) {
            return false;
        }
    }
    return true;
}

static void hpc_enqueue_cleanup_entries(
    struct mds_catalogue *cat,
    uint64_t fileid,
    const struct mds_ds_map_entry *entries,
    uint64_t entry_count)
{
    if (cat == NULL || entries == NULL) {
        return;
    }
    for (uint64_t entry_index = 0; entry_index < entry_count;
         entry_index++) {
        if (entries[entry_index].nfs_fh_len == 0 ||
            entries[entry_index].nfs_fh_len > MDS_NFS_FH_MAX) {
            continue;
        }
        (void)mds_cat_gc_enqueue(
            cat, NULL, fileid, entries[entry_index].ds_id,
            entries[entry_index].nfs_fh,
            entries[entry_index].nfs_fh_len);
    }
}

/*
 * Reap grace for incomplete legacy PENDING rows, in seconds.  During a
 * rolling upgrade a peer MDS still running the pre-atomic release
 * commits the inode + dirent before the stripe map; reaping such a row
 * mid-create would delete the in-flight file and let the old node's
 * failure rollback remove a same-name successor by name.  Recovery
 * therefore only hides (never reaps) incomplete rows younger than this
 * window.  Wide-create FH capture takes minutes at worst, so 300 s
 * comfortably covers it.  Complete maps are promoted regardless of age.
 */
#define HPC_PENDING_REAP_GRACE_SEC_DEFAULT 300U
static uint32_t g_pending_reap_grace_sec =
    HPC_PENDING_REAP_GRACE_SEC_DEFAULT;

void hpc_shared_test_set_pending_reap_grace(uint32_t grace_sec)
{
    g_pending_reap_grace_sec = grace_sec;
}

enum mds_status hpc_shared_recover_pending(
    struct compound_data *cd,
    struct mds_inode *inode)
{
    struct mds_ds_map_entry *entries = NULL;
    struct mds_inode visible_inode;
    uint32_t stripe_count = 0;
    uint32_t stripe_unit = 0;
    uint32_t mirror_count = 0;
    uint64_t entry_count;
    char name[MDS_MAX_NAME + 1];
    enum mds_status st;

    if (cd == NULL || cd->cat == NULL || inode == NULL) {
        return MDS_ERR_INVAL;
    }
    if ((inode->flags & MDS_IFLAG_HPC_CREATE_PENDING) == 0) {
        return MDS_OK;
    }

    st = mds_cat_stripe_map_get(
        cd->cat, inode->fileid, &stripe_count, &stripe_unit, &mirror_count,
        &entries);
    if (st == MDS_OK && hpc_pending_map_is_complete(
            stripe_count, stripe_unit, mirror_count, entries)) {
        visible_inode = *inode;
        visible_inode.flags &= ~MDS_IFLAG_HPC_CREATE_PENDING;
        st = mds_cat_ns_setattr(
            cd->cat, NULL, inode->fileid, &visible_inode, MDS_ATTR_FLAGS);
        free(entries);
        if (st != MDS_OK) {
            return st;
        }
        *inode = visible_inode;
        compound_inode_invalidate(cd, inode->fileid);
        return MDS_OK;
    }
    if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
        free(entries);
        return st;
    }

    /*
     * Rolling-upgrade guard: hide (do not reap) an incomplete row
     * younger than the grace window -- it may be an in-flight create
     * on a not-yet-upgraded peer.  This matches the legacy runtime
     * filter; the row becomes reapable once it ages past the window
     * or visible once its creator commits the map and clears the flag.
     */
    if (g_pending_reap_grace_sec > 0) {
        struct timespec now;

        if (clock_gettime(CLOCK_REALTIME, &now) != 0 ||
            now.tv_sec < inode->ctime.tv_sec ||
            (uint64_t)(now.tv_sec - inode->ctime.tv_sec) <
                g_pending_reap_grace_sec) {
            free(entries);
            return MDS_ERR_NOTFOUND;
        }
    }

    entry_count = 0;
    if (stripe_count > 0 && stripe_count <= MDS_MAX_STRIPES &&
        mirror_count > 0 && mirror_count <= MDS_MAX_MIRRORS) {
        entry_count = (uint64_t)stripe_count * mirror_count;
    }
    hpc_enqueue_cleanup_entries(cd->cat, inode->fileid, entries, entry_count);
    free(entries);
    st = mds_cat_ns_dirent_name_for_child(
        cd->cat, inode->parent_fileid, inode->fileid, name, sizeof(name));
    if (st == MDS_OK) {
        st = mds_cat_ns_remove_known(
            cd->cat, NULL, inode->parent_fileid, name, inode, stripe_count);
        if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
            return st;
        }
        (void)mds_cat_stripe_map_del(cd->cat, NULL, inode->fileid);
        compound_dirent_invalidate(cd, inode->parent_fileid, name);
    } else if (st == MDS_ERR_NOTFOUND) {
        if (stripe_count > 0) {
            (void)mds_cat_stripe_map_del(cd->cat, NULL, inode->fileid);
        }
        st = mds_cat_inode_del(cd->cat, NULL, inode->fileid);
        if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
            return st;
        }
    } else {
        return st;
    }
    compound_inode_invalidate(cd, inode->fileid);
    return MDS_ERR_NOTFOUND;
}

struct hpc_pending_scan_ctx {
    struct compound_data compound;
    struct hpc_pending_recovery_stats stats;
    enum mds_status status;
};

static int hpc_recover_pending_scan_cb(
    const struct mig_inode_chunk *chunk,
    void *arg)
{
    struct hpc_pending_scan_ctx *ctx = arg;
    struct mds_inode inode;

    if ((chunk->inode.flags & MDS_IFLAG_HPC_CREATE_PENDING) == 0) {
        return 0;
    }
    inode = chunk->inode;
    ctx->status = hpc_shared_recover_pending(&ctx->compound, &inode);
    if (ctx->status == MDS_OK) {
        ctx->stats.promoted++;
        return 0;
    }
    if (ctx->status == MDS_ERR_NOTFOUND) {
        /* Reaped, or hidden by the rolling-upgrade reap grace; either
         * way the row is no longer client-visible. */
        ctx->stats.reaped++;
        ctx->status = MDS_OK;
        return 0;
    }
    return 1;
}

enum mds_status hpc_shared_recover_pending_scan(
    struct mds_catalogue *cat,
    struct hpc_pending_recovery_stats *stats)
{
    struct hpc_pending_scan_ctx ctx;
    enum mds_status st;

    if (cat == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&ctx, 0, sizeof(ctx));
    ctx.compound.cat = cat;
    st = mds_cat_subtree_iter(
        cat, MDS_FILEID_ROOT, hpc_recover_pending_scan_cb, &ctx);
    /* A callback-initiated stop may surface through the iterator's
     * return value; the recovery status is the authoritative cause. */
    if (ctx.status != MDS_OK) {
        return ctx.status;
    }
    if (st != MDS_OK) {
        return st;
    }
    if (stats != NULL) {
        *stats = ctx.stats;
    }
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Wide HPC create.
 *
 * DS object creation precedes the catalogue transaction because the DS
 * handles are part of the committed stripe map.  No namespace row is made
 * visible until mds_cat_ns_create_wide commits inode, dirent, parent update,
 * and the complete map together.
 * ----------------------------------------------------------------------- */

static enum mds_status hpc_prepare_wide_inode(
    struct mds_catalogue *cat,
    uint64_t parent_fileid,
    uint32_t mode,
    uint64_t uid,
    uint64_t gid,
    struct mds_inode *out_inode)
{
    struct mds_inode child;
    uint64_t child_fid = 0;
    struct timespec now;
    enum mds_status st;

    st = mds_cat_alloc_fileid(cat, NULL, &child_fid);
    if (st != MDS_OK || child_fid == 0) {
        return (st == MDS_OK) ? MDS_ERR_NOMEM : st;
    }

    if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
        return MDS_ERR_IO;
    }
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
    child.flags = MDS_IFLAG_HPC_SHARED;

    *out_inode = child;
    return MDS_OK;
}

/* GC-enqueue every captured slot of a failed wide pre-warm batch.
 * Best-effort: a failing enqueue does not change the create result. */
static void hpc_create_gc_enqueue_entries(
    struct mds_catalogue *cat, uint64_t fileid,
    const struct ds_prealloc_batch_result *batch)
{
    uint64_t entry_count;

    if (batch == NULL) {
        return;
    }
    entry_count = (uint64_t)batch->stripe_count * batch->mirror_count;
    if (entry_count > (uint64_t)MDS_MAX_STRIPES * MDS_MAX_MIRRORS) {
        return;
    }
    hpc_enqueue_cleanup_entries(
        cat, fileid, batch->entries, entry_count);
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
    struct ds_prealloc_batch_request request;
    struct ds_prealloc_batch_result batch;
    enum mds_status st;

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

    st = hpc_prepare_wide_inode(cat, parent_fileid, mode, uid, gid, &child);
    if (st != MDS_OK) {
        return st;
    }
    memset(&request, 0, sizeof(request));
    request.stripe_count = stripe_count;
    request.mirror_count = mirror_count;
    request.stripe_unit = stripe_unit;
    request.required_mode = DS_MODE_GENERIC;
    request.required_transport = required_transport != 0
        ? required_transport : DS_TRANSPORT_TCP;
    request.preferred_transport = preferred_transport;
    request.preferred_caps = preferred_caps;
    request.strict_unique_ds = strict_unique_ds;
    request.fileid_hint = child.fileid;

    st = ds_prealloc_batch(prealloc, &request, &batch);
    if (st != MDS_OK) {
        return st;
    }

    st = mds_cat_ns_create_wide(
        cat, parent_fileid, name, &child, batch.stripe_count,
        batch.stripe_unit, batch.mirror_count, batch.entries);
    if (st != MDS_OK) {
        hpc_create_gc_enqueue_entries(cat, child.fileid, &batch);
        ds_prealloc_batch_result_destroy(&batch);
        return st;
    }

    child.stripe_count = batch.stripe_count;
    child.stripe_unit = batch.stripe_unit;
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

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mover_util.c -- Shared file relocation helper.
 *
 * Extracted from rebalance.c to share between rebalance and tiering.
 * Supports mirror_count >= 1 (single-copy and mirrored files).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>

#include "mover_util.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "commit_queue.h"
#include "proxy_io.h"
#include "open_state.h"
#include "fh_util.h"
#include "mds_log.h"

/* ----------------------------------------------------------------------- *
 * Constants
 * ----------------------------------------------------------------------- */

#define MOVER_COPY_CHUNK  ((size_t)1024 * 1024)  /* 1 MiB */

/* ----------------------------------------------------------------------- *
 * mover_relocate_file
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int mover_relocate_file(const struct mover_ctx *ctx,
                        uint64_t fileid,
                        uint32_t source_ds,
                        uint32_t source_mirror,
                        uint32_t target_ds,
                        uint32_t stripe_idx)
{
    enum mds_status st;

    /* -- Step a: Revalidate stripe map via catalogue vtable. -- */
    uint32_t sc, su, mc;
    struct mds_ds_map_entry *entries = NULL;
    st = mds_cat_stripe_map_get(ctx->cat, fileid, &sc, &su, &mc, &entries);
    if (st != MDS_OK) {
        return -1;
    }

    if (stripe_idx >= sc || mc < 1 || source_mirror >= mc) {
        free(entries);
        return 0;
    }

    /* Entry index for the target (stripe, mirror) within the flat
     * entries array: entries[stripe * mirror_count + mirror]. */
    uint32_t eidx = stripe_idx * mc + source_mirror;

    /* Confirm source mirror still points at source_ds. */
    if (entries[eidx].ds_id != source_ds) {
        free(entries);
        return 0;
    }

    /* For mirrored files, confirm target is not already a mirror
     * of the same stripe. */
    if (mc > 1) {
        uint32_t base = stripe_idx * mc;
        for (uint32_t m = 0; m < mc; m++) {
            if (entries[base + m].ds_id == target_ds) {
                free(entries);
                return 0;
            }
        }
    }

    /* Check no active layouts (fencing). */
    bool has_layout = false;
    st = mds_coord_layout_scan_for_file(ctx->cat, fileid, &has_layout);
    if (st != MDS_OK || has_layout) {
        if (ctx->counters != NULL) {
            atomic_fetch_add(&ctx->counters->skipped_active_layout, 1);
        }
        free(entries);
        return 0;
    }

    /* Check no active opens (write fencing). */
    if (ctx->ot != NULL &&
        open_state_file_has_writers(ctx->ot, fileid)) {
        if (ctx->counters != NULL) {
            atomic_fetch_add(&ctx->counters->skipped_active_writer, 1);
        }
        free(entries);
        return 0;
    }

    /* -- Step b: Sparse check. -- */
    uint64_t hole_off;
    bool hole_eof = false;
    st = mds_proxy_seek_direct(ctx->proxy, source_ds,
                                fileid, stripe_idx, source_mirror,
                                0, 1 /* SEEK_HOLE */, &hole_off, &hole_eof);
    if (st == MDS_OK && !hole_eof) {
        struct mds_inode inode_chk;
        enum mds_status ist;
        ist = mds_cat_ns_getattr(ctx->cat, fileid, &inode_chk);
        if (ist == MDS_OK && hole_off < inode_chk.size) {
            if (ctx->counters != NULL) {
                atomic_fetch_add(&ctx->counters->skipped_sparse, 1);
            }
            free(entries);
            return 0;
        }
    }

    /* -- Step c: Read inode for file size. -- */
    struct mds_inode inode;
    st = mds_cat_ns_getattr(ctx->cat, fileid, &inode);
    if (st != MDS_OK) {
        free(entries);
        return -1;
    }

    /* Skip 0-byte files: no DS data to move. */
    if (inode.size == 0) {
        free(entries);
        return 0;
    }

    /* -- Step d: Truncate stale data on target. -- */
    (void)mds_proxy_truncate_ds_file(ctx->proxy, target_ds,
                                      fileid, stripe_idx, source_mirror);

    /* -- Step e: Create target file. -- */
    st = mds_proxy_ensure_ds_file(ctx->proxy, target_ds,
                                   fileid, stripe_idx, source_mirror);
    if (st != MDS_OK) {
        free(entries);
        return -1;
    }

    /* -- Step f: Copy data via copy_file_range().
     * The kernel NFS client may perform the copy server-side
     * (NFSv4.2 inter-server COPY) if both DS mounts support it.
     * Falls back to kernel splice-based copy (zero user-space
     * buffer allocation either way). */
    uint64_t copied = 0;
    st = mds_proxy_copy_direct(ctx->proxy, source_ds, target_ds,
                                fileid, stripe_idx, source_mirror,
                                inode.size, &copied);
    if (st != MDS_OK) {
        free(entries);
        return -1;
    }

    if (!atomic_load(ctx->running)) {
        free(entries);
        return -1;
    }

    /* -- Step g: Verify complete copy. -- */
    if (copied != inode.size) {
        MDS_LOG_INFO(LOG_COMP_MDS, "mover: incomplete copy for file %lu "
                "(%lu/%lu bytes)",
                (unsigned long)fileid,
                (unsigned long)copied, (unsigned long)inode.size);
        free(entries);
        return -1;
    }

    /* -- Step h: Revalidate stripe map before commit. -- */
    uint32_t sc2, su2, mc2;
    struct mds_ds_map_entry *entries2 = NULL;
    st = mds_cat_stripe_map_get(ctx->cat, fileid, &sc2, &su2, &mc2, &entries2);
    if (st != MDS_OK) {
        free(entries);
        return -1;
    }

    bool same = (sc2 == sc && su2 == su && mc2 == mc);
    uint32_t eidx2 = stripe_idx * mc2 + source_mirror;
    if (same && eidx2 < sc2 * mc2) {
        if (entries2[eidx2].ds_id != source_ds) {
            same = false;
        }
    } else {
        same = false;
    }

    if (!same) {
        free(entries);
        free(entries2);
        return 0;
    }

    /* -- Step i: Capture old FH for GC, update entries, commit. -- */
    uint8_t gc_fh_buf[MDS_NFS_FH_MAX];
    uint32_t gc_fh_len = entries2[eidx2].nfs_fh_len;
    if (gc_fh_len > MDS_NFS_FH_MAX) {
        gc_fh_len = MDS_NFS_FH_MAX;
    }
    memcpy(gc_fh_buf, entries2[eidx2].nfs_fh, gc_fh_len);

    entries2[eidx2].ds_id = target_ds;
    mds_synth_fh_into_entry(&entries2[eidx2],
                            fileid, stripe_idx, source_mirror);

    if (ctx->cq != NULL) {
        struct commit_op op;
        memset(&op, 0, sizeof(op));
        op.type = COMMIT_OP_REBALANCE_MOVE;
        op.args.rebalance_move.fileid = fileid;
        op.args.rebalance_move.stripe_count = sc2;
        op.args.rebalance_move.stripe_unit = su2;
        op.args.rebalance_move.mirror_count = mc2;
        op.args.rebalance_move.entries = entries2;
        op.args.rebalance_move.gc_fileid = fileid;
        op.args.rebalance_move.gc_ds_id = source_ds;
        if (gc_fh_len > 0) {
            memcpy(op.args.rebalance_move.gc_nfs_fh, gc_fh_buf, gc_fh_len);
        }
        op.args.rebalance_move.gc_fh_len = gc_fh_len;
        st = commit_queue_submit(ctx->cq, &op);
    } else if (ctx->cat != NULL) {
        /* RonDB mode: no commit queue -- write stripe map + GC
         * entry directly via catalogue vtable. */
        st = mds_cat_stripe_map_put(ctx->cat, NULL, fileid,
                                    sc2, su2, mc2, entries2);
        if (st == MDS_OK && gc_fh_len > 0) {
            (void)mds_cat_gc_enqueue(ctx->cat, NULL, fileid,
                                     source_ds, gc_fh_buf,
                                     gc_fh_len);
        }
    } else {
        st = MDS_ERR_IO;
    }

    free(entries);
    free(entries2);

    if (st != MDS_OK) {
        return -1;
    }

    if (ctx->counters != NULL) {
        atomic_fetch_add(&ctx->counters->bytes_copied, copied);
    }
    return 1;
}

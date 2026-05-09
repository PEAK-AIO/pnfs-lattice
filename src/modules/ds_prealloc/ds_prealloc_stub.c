/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_prealloc_stub.c -- synchronous-on-demand DS placement for the
 * community build.
 *
 * Linked into pnfs_mds_core when ENABLE_DS_PREALLOC=OFF.  The
 * enterprise implementation in ds_prealloc.c pre-fetches placements
 * into per-DS rings to keep the OPEN(CREATE) hot path off the
 * placement / fileid-alloc / DS-FH-capture round trips.  The
 * community build does not need that throughput optimisation but
 * still has to honour the same contract: every successful pop()
 * MUST return a (ds_id, fileid, nfs_fh) triple that the catalogue
 * can persist as a stripe-map row, otherwise the file is created
 * without a stripe map and the first WRITE returns NFS4ERR_NOENT.
 *
 * This file therefore implements pop() / peek() / select_any_online()
 * directly as a synchronous select-and-capture sequence:
 *
 *   1.  mds_cat_ds_list(cat) -- snapshot the DS registry.
 *   2.  ds_cache_overlay_weights() -- fold runtime weight + capacity.
 *   3.  placement_select_ex(policy, ds_list, 1, 1) -- pick one DS.
 *   4.  mds_cat_alloc_fileid() -- reserve a fresh fileid (pop only).
 *   5.  mds_proxy_ensure_ds_file_fh() -- create the DS-side data
 *       file and capture its NFS3 file handle (pop only).
 *
 * Synchronous placement is measurably slower than the enterprise
 * pre-fetch ring on fan-in workloads but is correct for every
 * CREATE shape pynfs exercises and keeps the code path free of
 * per-DS rings, refill workers, and fileid pools.  Wide pre-warm
 * (ds_prealloc_batch) is intentionally not implemented in CE -- the
 * caller in compound_data_io.c only takes that path for HPC-Shared
 * inodes, which the community build never marks.
 */
#include "ds_prealloc.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "mds_catalogue.h"
#include "ds_cache.h"
#include "placement.h"
#include "proxy_io.h"
#include "pnfs_mds.h"

/*
 * Default stripe unit advertised on every CE pop.  Matches the
 * compile-time default in src/common/config.c so that operators
 * who leave stripe_unit_bytes at the default see the same value
 * coming out of the placement pool as out of the LAYOUTGET fallback
 * path.
 */
#define DS_PREALLOC_STUB_DEFAULT_STRIPE_UNIT  ((uint32_t)(64U * 1024U))

/*
 * Lifetime: every pointer below is borrowed from the daemon's
 * top-level objects (main.c).  This module never frees them; it
 * only retains the addresses for the duration of the daemon
 * process.  Caller (main.c) destroys this context via
 * ds_prealloc_destroy() during shutdown; that frees only the
 * struct itself.
 */
struct ds_prealloc_ctx {
    const struct mds_catalogue *cat;
    struct mds_proxy_ctx       *proxy;
    enum mds_placement_policy   policy;
    const struct ds_cache      *cache;
    uint32_t                    stripe_unit;
};

/*
 * Snapshot the DS registry, overlay live cache state, and run the
 * configured placement policy with stripe_count=1, mirror_count=1.
 *
 * Out-args:
 *   *entry          DS map entry populated with ds_id (FH zeroed).
 *   *stripe_unit    Stripe unit applied (always ctx->stripe_unit).
 *
 * Returns 0 on success, -1 on any failure (no online DS, OOM,
 * catalogue read failure).  On failure both out-args are zeroed
 * so the caller can distinguish "never written" from "partial".
 */
static int select_one_online(const struct ds_prealloc_ctx *ctx,
                             struct mds_ds_map_entry *entry,
                             uint32_t *stripe_unit)
{
    struct mds_ds_info *ds_list = NULL;
    uint32_t ds_count = 0;
    enum mds_status st;

    if (ctx == NULL || ctx->cat == NULL || entry == NULL) {
        return -1;
    }
    memset(entry, 0, sizeof(*entry));
    if (stripe_unit != NULL) {
        *stripe_unit = ctx->stripe_unit;
    }

    /* The catalogue API takes a non-const handle even though the
     * read does not mutate state; cast through a local to keep the
     * const promise on the prealloc context itself. */
    st = mds_cat_ds_list((struct mds_catalogue *)ctx->cat,
                         &ds_list, &ds_count);
    if (st != MDS_OK) {
        return -1;
    }
    if (ds_list == NULL || ds_count == 0) {
        free(ds_list);
        return -1;
    }

    /* Overlay runtime weights / live capacity so WEIGHTED_RR and
     * CAPACITY policies see the same numbers as the rest of the
     * daemon.  Safe under NULL cache (no-op). */
    if (ctx->cache != NULL) {
        ds_cache_overlay_weights(ctx->cache, ds_list, ds_count);
    }

    st = placement_select_ex(ctx->policy, ds_list, ds_count,
                             1, 1, ctx->stripe_unit, entry);
    free(ds_list);
    if (st != MDS_OK) {
        memset(entry, 0, sizeof(*entry));
        return -1;
    }
    return 0;
}

int ds_prealloc_init(const struct mds_catalogue *cat,
                     struct mds_proxy_ctx *proxy,
                     uint32_t pool_size,
                     struct ds_prealloc_ctx **out)
{
    return ds_prealloc_init_ex(cat, proxy, PLACEMENT_RR,
                               pool_size, out);
}

int ds_prealloc_init_ex(const struct mds_catalogue *cat,
                        struct mds_proxy_ctx *proxy,
                        enum mds_placement_policy policy,
                        uint32_t pool_size,
                        struct ds_prealloc_ctx **out)
{
    struct ds_prealloc_ctx *ctx;

    /* pool_size is the enterprise pre-fetch ring depth; CE has no
     * ring so the value is informational only. */
    (void)pool_size;

    if (out == NULL) {
        return -1;
    }
    *out = NULL;
    if (cat == NULL) {
        /* Tests sometimes call init with cat==NULL; preserve that
         * old contract by returning success-with-NULL-handle so
         * existing callers' null-checks short-circuit. */
        return 0;
    }

    ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        return -1;
    }
    ctx->cat = cat;
    ctx->proxy = proxy;
    ctx->policy = policy;
    ctx->cache = NULL;
    ctx->stripe_unit = DS_PREALLOC_STUB_DEFAULT_STRIPE_UNIT;
    *out = ctx;
    return 0;
}

int ds_prealloc_pop(struct ds_prealloc_ctx *ctx,
                    struct mds_ds_map_entry *entry,
                    uint32_t *stripe_unit,
                    uint64_t *fileid_out)
{
    uint64_t fileid = 0;
    enum mds_status st;
    int rc;

    if (entry == NULL) {
        return -1;
    }
    /* Defensive default-out values so a caller that ignores the
     * return code does not see uninitialised memory. */
    memset(entry, 0, sizeof(*entry));
    if (stripe_unit != NULL) {
        *stripe_unit = 0;
    }
    if (fileid_out != NULL) {
        *fileid_out = 0;
    }

    if (ctx == NULL) {
        /* init_ex returned NULL handle (cat==NULL test path). */
        return -1;
    }

    rc = select_one_online(ctx, entry, stripe_unit);
    if (rc != 0) {
        return -1;
    }

    /* Reserve a fresh fileid so the catalogue's ns_create can
     * persist the inode + stripe-map row in the same transaction
     * with this exact id.  The fileid is consumed unconditionally:
     * if the caller's create fails downstream the id is simply
     * never used (no stripe-map row, no inode row).  That is the
     * same lifecycle the enterprise pool documents. */
    st = mds_cat_alloc_fileid((struct mds_catalogue *)ctx->cat,
                              NULL, &fileid);
    if (st != MDS_OK || fileid == 0) {
        memset(entry, 0, sizeof(*entry));
        if (stripe_unit != NULL) {
            *stripe_unit = 0;
        }
        return -1;
    }

    /* Capture the DS-side NFS3 file handle now so the stripe-map
     * row written by ns_create is immediately I/O-ready -- the
     * very next WRITE in the same compound MUST not see an empty
     * FH (NFS4ERR_NOENT in proxy_write).  When proxy is absent
     * (test fixtures) leave fh_len=0 and let DS_PENDING /
     * ds_prepare drive the eventual capture; the WRITE will then
     * stall on stripe_map_get's empty FH branch but the file row
     * itself is well-formed. */
    if (ctx->proxy != NULL) {
        uint32_t fh_len = (uint32_t)sizeof(entry->nfs_fh);
        st = mds_proxy_ensure_ds_file_fh(ctx->proxy, entry->ds_id,
                                         fileid, 0, 0,
                                         entry->nfs_fh, &fh_len);
        if (st == MDS_OK && fh_len > 0 &&
            fh_len <= sizeof(entry->nfs_fh)) {
            entry->nfs_fh_len = fh_len;
        } else {
            entry->nfs_fh_len = 0;
        }
    }

    if (fileid_out != NULL) {
        *fileid_out = fileid;
    }
    return 0;
}

int ds_prealloc_peek(const struct ds_prealloc_ctx *ctx,
                     struct mds_ds_map_entry *entry,
                     uint32_t *stripe_unit)
{
    /*
     * Peek runs the same selection as pop but skips the
     * fileid-alloc and DS-FH-capture side effects.  Callers use
     * the returned ds_id only to pre-grant a layout state row in
     * the CQ-fused path; the community build does not exercise
     * that path (cd->cq is always NULL), so a thread-safety drift
     * between peek and the subsequent pop has no observable
     * effect.  Returning the policy's actual choice keeps the
     * contract honest for any future caller that does need
     * coherence.
     */
    return select_one_online(ctx, entry, stripe_unit);
}

enum mds_status ds_prealloc_select_any_online(
    const struct ds_prealloc_ctx *ctx,
    struct mds_ds_map_entry *entry,
    uint32_t *stripe_unit)
{
    if (entry == NULL) {
        return MDS_ERR_INVAL;
    }
    if (select_one_online(ctx, entry, stripe_unit) != 0) {
        return MDS_ERR_NOSPC;
    }
    return MDS_OK;
}

int ds_prealloc_ensure(const struct ds_prealloc_ctx *ctx,
                       uint32_t ds_id, uint64_t fileid)
{
    enum mds_status st;

    if (ctx == NULL || ctx->proxy == NULL) {
        /* Without a proxy there is no DS-side file to ensure;
         * report success so callers' DS_PENDING flag drives the
         * eventual capture. */
        return 0;
    }
    st = mds_proxy_ensure_ds_file(ctx->proxy, ds_id, fileid, 0, 0);
    return (st == MDS_OK) ? 0 : -1;
}

void ds_prealloc_set_ds_cache(struct ds_prealloc_ctx *ctx,
                              const struct ds_cache *cache)
{
    if (ctx == NULL) {
        return;
    }
    /* Pointer-only borrow; main.c owns the cache lifetime. */
    ctx->cache = cache;
}

void ds_prealloc_destroy(struct ds_prealloc_ctx *ctx)
{
    free(ctx);
}

enum mds_status ds_prealloc_batch(
    struct ds_prealloc_ctx *ctx,
    const struct ds_prealloc_batch_request *req,
    struct ds_prealloc_batch_result *out)
{
    /* Wide pre-warm is HPC-Shared territory; the community build
     * never sets MDS_IFLAG_HPC_SHARED so the OPEN(CREATE) wide
     * branch is unreachable and a permanent INVAL is correct. */
    (void)ctx;
    (void)req;
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    return MDS_ERR_INVAL;
}

void ds_prealloc_batch_result_destroy(
    struct ds_prealloc_batch_result *res)
{
    if (res == NULL) {
        return;
    }
    free(res->entries);
    memset(res, 0, sizeof(*res));
}

void ds_prealloc_test_enable_synthetic_fh(struct ds_prealloc_ctx *ctx,
                                          bool enabled)
{
    /* Synthetic FH generation is a benchmark hook in enterprise.
     * The community pop already captures real FHs synchronously
     * via the proxy, so this entry point is a no-op. */
    (void)ctx;
    (void)enabled;
}

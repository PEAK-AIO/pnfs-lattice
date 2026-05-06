/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_prealloc_stub.c -- no-op stubs for the DS placement pre-allocation
 * pool.
 *
 * Linked into pnfs_mds_core when ENABLE_DS_PREALLOC=OFF (community
 * build).  The real implementation is in ds_prealloc.c.
 *
 * Semantics: pre-allocation is unavailable.
 *   - init() / init_ex() return success with a NULL handle so callers'
 *     null-checks short-circuit.
 *   - pop() / peek() return -1 (pool empty) -- every CREATE falls
 *     back to synchronous placement, which is the documented "slow
 *     path" of the real implementation.
 *   - select_any_online() returns MDS_ERR_NOSPC.
 *   - ensure() returns 0 (no-op; the proxy has nothing to ensure).
 *   - batch() returns MDS_ERR_INVAL -- wide pre-warm is unavailable;
 *     callers must use single-file CREATE+LAYOUTGET.
 *
 * The community daemon therefore performs synchronous placement on
 * every CREATE -- correct, just measurably slower for fan-in
 * workloads.
 */
#include "ds_prealloc.h"

#include <stddef.h>
#include <string.h>

int ds_prealloc_init(const struct mds_catalogue *cat,
                     struct mds_proxy_ctx *proxy,
                     uint32_t pool_size,
                     struct ds_prealloc_ctx **out)
{
    (void)cat;
    (void)proxy;
    (void)pool_size;

    if (out) {
        *out = NULL;
    }
    return 0;
}

int ds_prealloc_init_ex(const struct mds_catalogue *cat,
                        struct mds_proxy_ctx *proxy,
                        enum mds_placement_policy policy,
                        uint32_t pool_size,
                        struct ds_prealloc_ctx **out)
{
    (void)cat;
    (void)proxy;
    (void)policy;
    (void)pool_size;

    if (out) {
        *out = NULL;
    }
    return 0;
}

int ds_prealloc_pop(struct ds_prealloc_ctx *ctx,
                    struct mds_ds_map_entry *entry,
                    uint32_t *stripe_unit,
                    uint64_t *fileid_out)
{
    (void)ctx;
    (void)entry;
    if (stripe_unit) {
        *stripe_unit = 0;
    }
    if (fileid_out) {
        *fileid_out = 0;
    }
    return -1;   /* pool empty */
}

enum mds_status ds_prealloc_select_any_online(
    const struct ds_prealloc_ctx *ctx,
    struct mds_ds_map_entry *entry,
    uint32_t *stripe_unit)
{
    (void)ctx;
    (void)entry;
    if (stripe_unit) {
        *stripe_unit = 0;
    }
    return MDS_ERR_NOSPC;
}

int ds_prealloc_ensure(const struct ds_prealloc_ctx *ctx,
                       uint32_t ds_id, uint64_t fileid)
{
    (void)ctx;
    (void)ds_id;
    (void)fileid;
    return 0;
}

int ds_prealloc_peek(const struct ds_prealloc_ctx *ctx,
                     struct mds_ds_map_entry *entry,
                     uint32_t *stripe_unit)
{
    (void)ctx;
    (void)entry;
    if (stripe_unit) {
        *stripe_unit = 0;
    }
    return -1;
}

void ds_prealloc_set_ds_cache(struct ds_prealloc_ctx *ctx,
                              const struct ds_cache *cache)
{
    (void)ctx;
    (void)cache;
}

void ds_prealloc_destroy(struct ds_prealloc_ctx *ctx)
{
    (void)ctx;
}

enum mds_status ds_prealloc_batch(
    struct ds_prealloc_ctx *ctx,
    const struct ds_prealloc_batch_request *req,
    struct ds_prealloc_batch_result *out)
{
    (void)ctx;
    (void)req;
    if (out) {
        memset(out, 0, sizeof(*out));
    }
    return MDS_ERR_INVAL;
}

void ds_prealloc_batch_result_destroy(
    struct ds_prealloc_batch_result *res)
{
    (void)res;
}

void ds_prealloc_test_enable_synthetic_fh(struct ds_prealloc_ctx *ctx,
                                          bool enabled)
{
    (void)ctx;
    (void)enabled;
}

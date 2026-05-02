/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * layout_cache_stub.c — no-op stub implementations of the
 * MDS-side layout cache.
 *
 * Linked into pnfs_mds_core when ENABLE_LAYOUT_CACHE=OFF (community
 * build).  The full implementation is in layout_cache.c.
 *
 * Semantics: caching is disabled.
 *   - init() returns success with a NULL handle.
 *   - get()  always reports a miss (-1) — every LAYOUTGET goes
 *     straight to RonDB.  This is correct, just slower; it matches
 *     the behaviour of any deployment that hadn't configured a
 *     layout cache.
 *   - put()  silently drops the insert (0 = "success, kind of").
 *   - invalidate() / clear() are no-ops.
 *   - stats_get() zeros the output struct.
 *
 * The community daemon therefore has full LAYOUTGET correctness;
 * it just pays a RonDB round trip on every layout grant.
 */
#include "layout_cache.h"

#include <stddef.h>
#include <string.h>

int layout_cache_init(uint32_t max_entries, struct layout_cache **out)
{
    (void)max_entries;

    if (out) {
        *out = NULL;
    }
    return 0;
}

void layout_cache_destroy(struct layout_cache *lc)
{
    (void)lc;
}

int layout_cache_get(struct layout_cache *lc, uint64_t fileid,
                     uint32_t *stripe_count, uint32_t *stripe_unit,
                     uint32_t *mirror_count,
                     struct mds_ds_map_entry **entries)
{
    (void)lc;
    (void)fileid;

    if (stripe_count) {
        *stripe_count = 0;
    }
    if (stripe_unit) {
        *stripe_unit = 0;
    }
    if (mirror_count) {
        *mirror_count = 0;
    }
    if (entries) {
        *entries = NULL;
    }
    return -1;   /* miss */
}

int layout_cache_put(struct layout_cache *lc, uint64_t fileid,
                     uint32_t stripe_count, uint32_t stripe_unit,
                     uint32_t mirror_count,
                     const struct mds_ds_map_entry *entries)
{
    (void)lc;
    (void)fileid;
    (void)stripe_count;
    (void)stripe_unit;
    (void)mirror_count;
    (void)entries;
    return 0;
}

void layout_cache_invalidate(struct layout_cache *lc, uint64_t fileid)
{
    (void)lc;
    (void)fileid;
}

void layout_cache_clear(struct layout_cache *lc)
{
    (void)lc;
}

void layout_cache_stats_get(const struct layout_cache *lc,
                            struct layout_cache_stats *out)
{
    (void)lc;
    if (out) {
        memset(out, 0, sizeof(*out));
    }
}

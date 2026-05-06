/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * layout_commit_aggregator_stub.c -- no-op stubs.
 *
 * Linked into pnfs_mds_core when ENABLE_LAYOUT_COMMIT_AGGREGATOR=OFF
 * (community build).  The real implementation is in
 * layout_commit_aggregator.c.
 *
 * Semantics: the aggregator is unavailable.  init() returns success
 * with a NULL handle so callers' null-checks short-circuit; submit()
 * returns 0 (silently dropped -- the caller is expected to also
 * persist directly via the catalogue, which is what every existing
 * code path already does for durability), peek() / flush_fileid()
 * miss, flush_all_dirty() returns 0, drop() and stats_get() are
 * safe no-ops.
 *
 * The community daemon therefore performs one RonDB write per
 * LAYOUTCOMMIT instead of amortising -- correct, just less
 * efficient.
 */
#include "layout_commit_aggregator.h"

#include <stddef.h>
#include <string.h>

int layout_commit_aggregator_init(uint32_t max_buckets,
                                  uint32_t flush_interval_ms,
                                  struct layout_commit_aggregator **out)
{
    (void)max_buckets;
    (void)flush_interval_ms;

    if (out) {
        *out = NULL;
    }
    return 0;
}

void layout_commit_aggregator_destroy(struct layout_commit_aggregator *agg)
{
    (void)agg;
}

void layout_commit_aggregator_set_flush_fn(
    struct layout_commit_aggregator *agg,
    layout_commit_flush_fn fn, void *cookie)
{
    (void)agg;
    (void)fn;
    (void)cookie;
}

int layout_commit_aggregator_start(struct layout_commit_aggregator *agg)
{
    (void)agg;
    return 0;
}

void layout_commit_aggregator_stop(struct layout_commit_aggregator *agg)
{
    (void)agg;
}

int layout_commit_aggregator_submit(struct layout_commit_aggregator *agg,
                                    uint64_t fileid, uint64_t new_size,
                                    struct timespec mtime)
{
    (void)agg;
    (void)fileid;
    (void)new_size;
    (void)mtime;
    return 0;
}

int layout_commit_aggregator_peek(struct layout_commit_aggregator *agg,
                                  uint64_t fileid,
                                  uint64_t *out_size,
                                  struct timespec *out_mtime,
                                  bool *out_dirty)
{
    (void)agg;
    (void)fileid;
    (void)out_size;
    (void)out_mtime;
    (void)out_dirty;
    return -1;   /* miss */
}

int layout_commit_aggregator_flush_fileid(
    struct layout_commit_aggregator *agg, uint64_t fileid)
{
    (void)agg;
    (void)fileid;
    return 1;   /* miss */
}

uint32_t layout_commit_aggregator_flush_all_dirty(
    struct layout_commit_aggregator *agg)
{
    (void)agg;
    return 0;
}

void layout_commit_aggregator_drop(struct layout_commit_aggregator *agg,
                                   uint64_t fileid)
{
    (void)agg;
    (void)fileid;
}

void layout_commit_aggregator_stats_get(
    const struct layout_commit_aggregator *agg,
    struct layout_commit_aggregator_stats *out)
{
    (void)agg;
    if (out) {
        memset(out, 0, sizeof(*out));
    }
}

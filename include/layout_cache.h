/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * layout_cache.h — Phase D of docs/hpc-nto1-plan.md (v1 foundation).
 *
 * Per-inode in-memory cache of the stripe map keyed by fileid.  The
 * cache is intended to short-circuit `cat_stripe_map_get` on the
 * LAYOUTGET hot path for HPC-Shared inodes, where many clients each
 * issue an independent LAYOUTGET against the same fileid and the
 * underlying stripe map is stable for the file's lifetime.
 *
 * v1 scope (this file).  Pure data-structure module.  No callers
 * inside the MDS yet — the integration into op_layoutget /
 * op_remove / op_setattr lands in a follow-up patch alongside the
 * supporting unit-test scaffolding for those code paths.  Landing
 * the cache as an isolated, well-tested module first matches the
 * incremental shape we used for Phase G v1 (see
 * docs/hpc-shared-files.md).
 *
 * Design.
 *
 *   - 16 shards, sharded by `fileid % LAYOUT_CACHE_SHARDS`.  Each
 *     shard owns its own pthread_mutex_t, chained hash table, and
 *     LRU doubly-linked list.  16 mirrors the convention used by
 *     open_state.c, delegation.c, lock_state.c.
 *
 *   - Per-shard capacity = ceil(max_entries / 16), with a minimum of
 *     4 so very small caches still evict cleanly.
 *
 *   - The cache stores a heap-allocated copy of the caller's
 *     `mds_ds_map_entry[]` array.  `layout_cache_get` allocates a
 *     fresh copy for the caller, who then owns it and must free()
 *     it — this matches the convention used by `cat_stripe_map_get`
 *     and lets the cache mutate its own internal entry without
 *     racing with downstream readers.
 *
 *   - LRU is per-shard (not global).  Cross-shard LRU would need
 *     global coordination on every get/put and was traded away for
 *     read concurrency under contention.  The skew this introduces
 *     is bounded by the entry-count skew across shards, which is
 *     uniform for splitmix64-distributed fileids.
 *
 * Memory footprint.  Each cached entry holds
 *   sizeof(layout_cache_entry) + stripe_count * mirror_count *
 *   sizeof(struct mds_ds_map_entry).
 * For HPC tier-1 layouts with stripe_count == 1024 and mirror_count
 * == 1, that's ~136 KiB per entry (each mds_ds_map_entry is 136
 * bytes: 4 + 4 + 128).  At the default 1024-entry cache capacity
 * the upper bound is ~136 MiB.  Operators with smaller layouts pay
 * proportionally less; the cap is hard.
 *
 * Thread safety.  All operations are safe to call from concurrent
 * threads.  Each operation acquires exactly one shard's mutex; no
 * cross-shard locks are held simultaneously, so there is no
 * deadlock risk under any caller pattern.
 */

#ifndef LAYOUT_CACHE_H
#define LAYOUT_CACHE_H

#include <stdint.h>
#include "pnfs_mds.h"   /* struct mds_ds_map_entry */

/** Number of shards.  Must stay a power of two for the modulo
 *  shard selection to be a single AND on the hot path; current
 *  value is 16 to match the rest of the per-stripe-mutex pattern
 *  used by open_state / delegation / lock_state. */
#define LAYOUT_CACHE_SHARDS 16

/** Opaque cache handle. */
struct layout_cache;

/**
 * Aggregated per-cache statistics.  Read via layout_cache_stats_get.
 * All fields are monotonic counters except @c entry_count, which is
 * the current population summed across all shards.
 */
struct layout_cache_stats {
    uint64_t hits;
    uint64_t misses;
    uint64_t puts;
    uint64_t evictions;
    uint64_t invalidations;
    uint64_t entry_count;
};

/**
 * Create a layout cache with capacity for @p max_entries entries
 * total (split across LAYOUT_CACHE_SHARDS shards).
 *
 * @param max_entries Total capacity across all shards.  Must be > 0.
 *                    Each shard receives ceil(max_entries /
 *                    LAYOUT_CACHE_SHARDS) capacity, with a per-shard
 *                    minimum of 4.  The effective total may exceed
 *                    @p max_entries by up to LAYOUT_CACHE_SHARDS - 1
 *                    due to per-shard rounding.
 * @param out         Receives the cache handle.
 * @return 0 on success, -1 on bad args or allocation failure.
 */
int layout_cache_init(uint32_t max_entries, struct layout_cache **out);

/**
 * Destroy the cache and free all internal state.  Safe to call with
 * NULL.  Caller must ensure no other threads are using the cache.
 */
void layout_cache_destroy(struct layout_cache *lc);

/**
 * Look up the stripe map for @p fileid.
 *
 * On hit, the entry is promoted to the MRU end of its shard's LRU
 * list, and the four output parameters are populated:
 *
 *   *stripe_count  — number of stripes
 *   *stripe_unit   — stripe unit in bytes
 *   *mirror_count  — mirrors per stripe
 *   *entries       — heap-allocated array of size
 *                    (stripe_count * mirror_count); CALLER OWNS,
 *                    must free().  On miss this is left as NULL.
 *
 * @return 0 on hit, -1 on miss or bad args.  No allocation is
 *         performed on miss.
 */
int layout_cache_get(struct layout_cache *lc, uint64_t fileid,
                     uint32_t *stripe_count, uint32_t *stripe_unit,
                     uint32_t *mirror_count,
                     struct mds_ds_map_entry **entries);

/**
 * Insert or update the cached stripe map for @p fileid.
 *
 * If an entry with the same fileid already exists, it is replaced in
 * place (the previous heap-allocated entries array is freed) and
 * promoted to MRU.  If the shard is at capacity, the LRU tail is
 * evicted before the new entry is inserted.  The caller-supplied
 * @p entries array is COPIED internally; the caller retains
 * ownership of its own buffer.
 *
 * @param lc            Cache handle.
 * @param fileid        File identifier.
 * @param stripe_count  Number of stripes (must be > 0).
 * @param stripe_unit   Stripe unit in bytes.
 * @param mirror_count  Mirrors per stripe (must be > 0).
 * @param entries       Caller's array; sized stripe_count * mirror_count.
 *                      Must not be NULL.
 * @return 0 on success, -1 on bad args or allocation failure.  On
 *         allocation failure the previous entry (if any) is
 *         preserved unchanged.
 */
int layout_cache_put(struct layout_cache *lc, uint64_t fileid,
                     uint32_t stripe_count, uint32_t stripe_unit,
                     uint32_t mirror_count,
                     const struct mds_ds_map_entry *entries);

/**
 * Drop the entry for @p fileid if present.  Idempotent; safe to call
 * for a fileid that was never cached.  Used by the eventual
 * op_remove (final unlink) and op_setattr (shrink) integration sites
 * to keep the cache coherent with the catalogue.
 */
void layout_cache_invalidate(struct layout_cache *lc, uint64_t fileid);

/**
 * Drop ALL cached entries.  Used by tests; production callers should
 * prefer per-fileid invalidation.
 */
void layout_cache_clear(struct layout_cache *lc);

/**
 * Read aggregated counters and the live entry count.  Safe to call
 * concurrently with other operations; values are a snapshot, not a
 * consistent transaction across shards.
 */
void layout_cache_stats_get(const struct layout_cache *lc,
                            struct layout_cache_stats *out);

#endif /* LAYOUT_CACHE_H */

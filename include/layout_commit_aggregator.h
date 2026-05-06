/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * layout_commit_aggregator.h -- Phase F of docs/hpc-nto1-plan.md
 * (v1 foundation).
 *
 * In-memory write-back aggregator for LAYOUTCOMMIT updates issued by
 * HPC-Shared inodes.  Replaces the synchronous per-LAYOUTCOMMIT NDB
 * row write with a per-fileid bucket that holds the running maximum
 * file size and the latest mtime; a periodic timer thread (default
 * 200 ms) flushes dirty buckets through a caller-supplied callback,
 * and four forced-flush triggers (DELETE, final close, strict
 * GETATTR, admin) cover the cases where the size value must be
 * persisted before the next observer sees it.
 *
 * v1 scope (this file).  Pure data-structure module + timer thread.
 * No callers inside the MDS yet -- wiring into op_layoutcommit /
 * op_getattr / op_remove lands in a follow-up patch.  This matches
 * the v1 / integration split used for Phase D (layout_cache) and
 * Phase G (hpc_hint).
 *
 * Design.
 *
 *   - 16 shards, sharded by `fileid % LCA_SHARDS`.  Each shard owns
 *     its own pthread_mutex_t, chained hash table, and dirty list.
 *
 *   - Per-shard capacity = ceil(max_buckets / 16), with a per-shard
 *     floor of 4.  When the shard reaches capacity the LRU dirty
 *     bucket is force-flushed inline so a slow flush callback can
 *     never starve a hot bucket.  v1 keeps this simple -- eviction
 *     of a clean bucket is also allowed because the next submit
 *     will rebuild its state.
 *
 *   - Merge semantics on submit:
 *       max_size      := max(current_size,   new_size)
 *       latest_mtime  := max(current_mtime, new_mtime)  (by tv_sec
 *                          then tv_nsec)
 *       dirty         := true
 *
 *   - Flush callback signature: layout_commit_flush_fn.  Returning
 *     non-zero indicates the persistence step failed and the
 *     bucket should be re-marked dirty (the snapshot is preserved
 *     for the next flush attempt).  The callback runs WITHOUT the
 *     shard's mutex held.
 *
 *   - Periodic flush: a single timer thread iterates every shard,
 *     collects each shard's dirty fileids under its mutex (briefly),
 *     and invokes the callback per fileid outside the mutex.  This
 *     lets concurrent submits proceed against unrelated buckets
 *     while an NDB-bound flush is in flight.
 *
 *   - Cross-MDS coherence.  v1 is per-MDS; multi-MDS deployments
 *     get a divergence window for HPC-Shared files until the next
 *     flush on each MDS.  Documented in docs/hpc-shared-files.md
 *     and bounded by the configured flush interval.
 *
 *   - Crash recovery.  The aggregator is purely in-memory; on
 *     daemon restart any unflushed updates are lost.  Phase F's
 *     master plan documents this -- recovery is a DS-side scan
 *     against MDS_IFLAG_HPC_SHARED inodes (deferred to a focused
 *     follow-up).  v1 ships the aggregator behind an opt-in inode
 *     flag (HPC_SHARED) so non-HPC files continue to take the
 *     synchronous path.
 *
 * Thread safety.  All public functions are safe to call from
 * concurrent threads.  Each operation acquires exactly one shard's
 * mutex; the flush thread acquires shard mutexes in sequence (never
 * two at once).  Shutdown sequence: stop() joins the timer thread
 * before destroy() touches any shard state.
 */

#ifndef LAYOUT_COMMIT_AGGREGATOR_H
#define LAYOUT_COMMIT_AGGREGATOR_H

#include <stdbool.h>
#include <stdint.h>
#include <time.h>

/** Number of shards.  Must stay a power of two so the modulo is a
 *  single AND on the hot path; matches the convention used by
 *  layout_cache, open_state, delegation, lock_state. */
#define LCA_SHARDS 16

/** Default periodic flush interval in milliseconds.  Master plan
 *  S5 Phase F recommends 200 ms -- small enough that a `cp` from a
 *  GETATTR-issuing tool sees a near-current size, large enough to
 *  amortise hundreds of LAYOUTCOMMITs per file per second across
 *  one NDB write per flush. */
#define LCA_DEFAULT_FLUSH_INTERVAL_MS 200U

/** Default capacity (total dirty + clean buckets across all shards).
 *  Sized for ~4 K HPC-Shared files concurrently active per MDS. */
#define LCA_DEFAULT_MAX_BUCKETS 4096U

/** Opaque aggregator handle. */
struct layout_commit_aggregator;

/**
 * Aggregated counters.  Read via layout_commit_aggregator_stats_get.
 * All fields are monotonic counters except @c entry_count, which is
 * the live bucket population summed across all shards.  Snapshot
 * semantics -- values are not transactionally consistent across
 * shards.
 */
struct layout_commit_aggregator_stats {
    uint64_t submits;            /**< submit() calls (incl. merges). */
    uint64_t merges;             /**< submits that hit an existing bucket. */
    uint64_t flushes_periodic;   /**< buckets flushed by the timer. */
    uint64_t flushes_forced;     /**< buckets flushed by *_flush_fileid. */
    uint64_t flush_failures;     /**< callback returned non-zero. */
    uint64_t evictions;          /**< buckets evicted to make room. */
    uint64_t drops;              /**< buckets dropped via *_drop. */
    uint64_t entry_count;        /**< current bucket population. */
};

/**
 * Flush callback invoked when a bucket needs to be persisted.
 *
 * Called WITHOUT any aggregator-internal mutex held.  Caller MUST
 * NOT call any layout_commit_aggregator_* function from within the
 * callback against the same fileid -- that would attempt to acquire
 * the same shard mutex and deadlock.
 *
 * @return 0 on success (bucket is marked clean).  Non-zero on
 *         failure (bucket stays dirty for the next flush attempt;
 *         the same snapshot will NOT be retried until a new submit
 *         arrives or the next periodic tick fires).
 */
typedef int (*layout_commit_flush_fn)(uint64_t fileid,
                                       uint64_t size,
                                       struct timespec mtime,
                                       void *cookie);

/**
 * Create an aggregator.
 *
 * @param max_buckets        Total bucket capacity across all shards.
 *                           0 -> use LCA_DEFAULT_MAX_BUCKETS.
 * @param flush_interval_ms  Periodic timer interval in ms.  0 -> use
 *                           LCA_DEFAULT_FLUSH_INTERVAL_MS.
 * @param out                Receives the aggregator handle.
 * @return 0 on success, -1 on bad args / allocation failure.
 *
 * The timer thread is NOT started until layout_commit_aggregator_start
 * is called -- letting the caller wire the flush callback first.
 */
int layout_commit_aggregator_init(uint32_t max_buckets,
                                   uint32_t flush_interval_ms,
                                   struct layout_commit_aggregator **out);

/**
 * Destroy the aggregator.  Implicitly stops the timer thread if it
 * is still running, but does NOT flush remaining dirty buckets --
 * callers that need at-shutdown durability must call
 * layout_commit_aggregator_flush_all_dirty() first.  Safe with NULL.
 */
void layout_commit_aggregator_destroy(struct layout_commit_aggregator *agg);

/**
 * Wire (or rewire) the flush callback.  Safe to call before or
 * after start().  When the callback is NULL, periodic and forced
 * flushes are no-ops (buckets remain dirty in memory).
 */
void layout_commit_aggregator_set_flush_fn(
    struct layout_commit_aggregator *agg,
    layout_commit_flush_fn fn, void *cookie);

/**
 * Start the periodic timer thread.  Returns 0 on success, -1 if
 * the thread is already running or pthread_create failed.
 */
int layout_commit_aggregator_start(struct layout_commit_aggregator *agg);

/**
 * Stop the periodic timer thread.  Idempotent.  Joins the thread
 * before returning so the caller can safely tear down the
 * surrounding context (catalogue, shard map, etc.) immediately.
 */
void layout_commit_aggregator_stop(struct layout_commit_aggregator *agg);

/**
 * Submit a LAYOUTCOMMIT update for @p fileid.
 *
 * Merge semantics:
 *   - bucket.max_size     = max(bucket.max_size, @p new_size)
 *   - bucket.latest_mtime = newer of the two (tv_sec, then tv_nsec)
 *   - bucket.dirty        = true
 *
 * @return 0 on success, -1 on bad args / allocation failure under
 *         capacity pressure (eviction itself never fails -- but
 *         the new bucket allocation can OOM).
 */
int layout_commit_aggregator_submit(struct layout_commit_aggregator *agg,
                                     uint64_t fileid,
                                     uint64_t new_size,
                                     struct timespec mtime);

/**
 * Read the current aggregated state for @p fileid without flushing.
 *
 * On hit (bucket present), populates @p out_size, @p out_mtime, and
 * @p out_dirty.  On miss, returns -1; outputs are not touched.
 *
 * Used by op_getattr to satisfy `hpc_getattr_mode = optimistic`
 * -- a strict GETATTR forces a flush instead via flush_fileid.
 */
int layout_commit_aggregator_peek(struct layout_commit_aggregator *agg,
                                   uint64_t fileid,
                                   uint64_t *out_size,
                                   struct timespec *out_mtime,
                                   bool *out_dirty);

/**
 * Force-flush a single fileid.
 *
 * Invokes the configured flush callback under the bucket's snapshot.
 * If the callback returns 0, the bucket is marked clean.  If it
 * returns non-zero, the bucket stays dirty.
 *
 * @return 0 on hit + clean flush, 1 on miss (no bucket present),
 *         -1 on hit + failed flush (bucket stays dirty).
 */
int layout_commit_aggregator_flush_fileid(
    struct layout_commit_aggregator *agg, uint64_t fileid);

/**
 * Flush every dirty bucket across every shard once.  Used by the
 * periodic timer and by admin-triggered flush.  Non-fatal: failed
 * per-bucket flushes leave those buckets dirty without aborting
 * the sweep.
 *
 * @return total number of buckets attempted (success + failure).
 */
uint32_t layout_commit_aggregator_flush_all_dirty(
    struct layout_commit_aggregator *agg);

/**
 * Drop the bucket for @p fileid without flushing.  Used by
 * op_remove (final unlink) -- the size becomes meaningless once the
 * file is gone, so the in-memory state is discarded.  Idempotent
 * and NULL-safe.
 */
void layout_commit_aggregator_drop(struct layout_commit_aggregator *agg,
                                    uint64_t fileid);

/**
 * Read aggregated counters and the live bucket count.  Snapshot,
 * not transactionally consistent across shards.
 */
void layout_commit_aggregator_stats_get(
    const struct layout_commit_aggregator *agg,
    struct layout_commit_aggregator_stats *out);

#endif /* LAYOUT_COMMIT_AGGREGATOR_H */

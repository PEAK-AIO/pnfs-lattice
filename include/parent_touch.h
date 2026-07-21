/*
 * Copyright (c) 2026 PeakAIO. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-PeakAIO-Proprietary
 *
 * parent_touch.h — deferred parent-directory attribute aggregator.
 *
 * Regular-file CREATE / REMOVE traditionally bump the parent
 * directory's change counter + mtime/ctime via an interpreted update
 * INSIDE the same NDB transaction as the dirent mutation.  NDB's
 * strict 2PL holds that exclusive parent-row lock for the full 2PC
 * window, so all same-directory mutations serialize (~0.87 ms per op
 * measured on the lab; ~1,150 creates/s per directory with >= 2
 * client streams).
 *
 * This module moves the parent change/mtime maintenance for
 * regular-file create/remove OUT of the mutation transaction into a
 * per-MDS in-memory aggregator (the transaction keeps a SHARED-lock
 * read of the parent row — see the shim — preserving the
 * parent-existence guard and the rmdir/NOTEMPTY exclusion while
 * letting same-directory mutations commit in parallel).
 *
 * Bucket invariant (per parent fileid):
 *
 *     logical_change = persisted_change + pending_delta
 *
 *   - logical_change  what every local reader must observe (served
 *                     via parent_touch_overlay on the attr read
 *                     paths).
 *   - pending_delta   deferred increments not yet persisted; flushed
 *                     periodically as ONE interpreted
 *                     incValue(change, delta) + mtime/ctime write.
 *
 * Two-phase mutation protocol (infallible after commit):
 *
 *   1. parent_touch_prepare() BEFORE issuing the deferred NDB
 *      mutation.  Allocates and PINS the bucket (seeding
 *      logical_change from the caller's current logical view on
 *      first touch).  Any failure here means the caller must fall
 *      back to the synchronous in-transaction parent update — the
 *      mutation has not been issued yet, so nothing is lost.
 *   2a. parent_touch_commit_prepared() after the NDB commit
 *      succeeded.  Cannot fail: the pinned bucket is guaranteed to
 *      exist (pinned buckets are never evicted).  Returns the exact
 *      (before, after) change pair for change_info / OPEN cinfo.
 *   2b. parent_touch_abort_prepared() when the mutation failed.
 *
 * Synchronous writers (mkdir/rmdir/link/rename — they keep the
 * in-transaction interpreted update) call parent_touch_note_sync_bump
 * after success so logical_change tracks the persisted bump and the
 * invariant holds.  An explicit directory SETATTR must call
 * parent_touch_flush_and_drop first so client-set timestamps are not
 * overridden by the overlay afterwards.
 *
 * Timestamps: the flush callback receives flush-time "now", not the
 * submit-time stamp — absolute timestamps do not commute with racing
 * synchronous updates, and flush-time now can never regress a newer
 * synchronous write.  The persisted mtime may therefore LEAD the true
 * last-mutation time by up to one flush interval; local readers
 * always see the exact bucket mtime via the overlay.
 *
 * Crash semantics: purely in-memory.  Clean shutdown must call
 * parent_touch_flush_all_dirty() before destroy (main.c does).  On a
 * crash, up to one flush interval of parent change/mtime increments
 * is lost; dirents themselves are never lost (they committed).
 *
 * Thread safety: all public functions are safe from concurrent
 * threads.  Each operation takes exactly one shard mutex; the flush
 * thread never holds two shard mutexes at once and never invokes the
 * flush callback under a shard mutex.
 */

#ifndef PARENT_TOUCH_H
#define PARENT_TOUCH_H

#include <stdbool.h>
#include <stdint.h>
#include <time.h>

struct mds_inode;

/** Number of shards.  Power of two (mask, not modulo, on the hot
 *  path); matches layout_commit_aggregator / layout_cache. */
#define PT_SHARDS 16

/** Default periodic flush interval in milliseconds (config key
 *  parent_touch_flush_ms).  Small enough that non-owner readers and
 *  the crash window stay tight; large enough to amortise hundreds of
 *  same-directory creates into one NDB write. */
#define PT_DEFAULT_FLUSH_INTERVAL_MS 50U

/** Default bucket capacity across all shards (config key
 *  parent_touch_max_dirs) — the number of distinct parent
 *  directories with in-flight deferred attr state. */
#define PT_DEFAULT_MAX_DIRS 4096U

/** Opaque aggregator handle. */
struct parent_touch;

/**
 * Aggregated counters (parent_touch_stats_get).  Monotonic counters
 * except @c entry_count / @c pinned_count which are live populations.
 * Snapshot semantics — not transactionally consistent across shards.
 */
struct parent_touch_stats {
    uint64_t submits;           /**< commit_prepared() applications. */
    uint64_t sync_folds;        /**< note_sync_bump() hits. */
    uint64_t flushes_periodic;  /**< buckets flushed by the timer. */
    uint64_t flushes_forced;    /**< buckets flushed by forced paths. */
    uint64_t flush_failures;    /**< flush callback returned non-zero. */
    uint64_t evictions;         /**< buckets evicted to make room. */
    uint64_t prepare_fallbacks; /**< prepare() failures (caller went sync). */
    uint64_t drops;             /**< buckets dropped via flush_and_drop. */
    uint64_t entry_count;       /**< current bucket population. */
    uint64_t pinned_count;      /**< buckets with in-flight preparations. */
};

/**
 * Flush callback: persist @p change_delta increments + stamp
 * mtime/ctime = @p stamp on the parent inode row (one standalone
 * interpreted update — see mds_cat_ns_parent_touch).
 *
 * Called WITHOUT any aggregator mutex held.  MUST NOT call back into
 * parent_touch_* for the same fileid (shard-mutex deadlock).
 *
 * @return 0 on success (delta moved from pending to persisted),
 *         non-zero on failure (delta is restored to pending and the
 *         bucket stays dirty for the next attempt).
 */
typedef int (*parent_touch_flush_fn)(uint64_t fileid,
                                     uint64_t change_delta,
                                     struct timespec stamp,
                                     void *cookie);

/**
 * Create an aggregator.
 *
 * @param max_dirs          Total bucket capacity across all shards.
 *                          0 → PT_DEFAULT_MAX_DIRS.
 * @param flush_interval_ms Periodic flush interval.  0 → default.
 * @param out               Receives the handle.
 * @return 0 on success, -1 on bad args / allocation failure.
 *
 * The flush thread is NOT started until parent_touch_start() so the
 * caller can wire the flush callback first.
 */
int parent_touch_init(uint32_t max_dirs, uint32_t flush_interval_ms,
                      struct parent_touch **out);

/**
 * Destroy.  Implicitly stops the flush thread but does NOT flush
 * remaining dirty buckets — call parent_touch_flush_all_dirty()
 * first for at-shutdown durability.  NULL-safe.
 */
void parent_touch_destroy(struct parent_touch *pt);

/** Wire (or rewire) the flush callback.  NULL fn = flushes no-op
 *  (buckets stay dirty in memory). */
void parent_touch_set_flush_fn(struct parent_touch *pt,
                               parent_touch_flush_fn fn, void *cookie);

/** Start the periodic flush thread.  0 on success, -1 if already
 *  running / pthread_create failed. */
int parent_touch_start(struct parent_touch *pt);

/** Stop the flush thread.  Idempotent; joins before returning. */
void parent_touch_stop(struct parent_touch *pt);

/**
 * Phase 1 of a deferred mutation: pin (and if needed create) the
 * bucket for @p parent_fileid BEFORE issuing the deferred NDB
 * transaction.
 *
 * @param seed_change  The parent's CURRENT logical change value as
 *                     read by the caller through the overlay-aware
 *                     read path.  Used only on first touch (when no
 *                     bucket exists, logical == persisted, so the
 *                     caller's read IS the persisted value).
 * @return 0 on success (bucket pinned — commit_prepared/
 *         abort_prepared MUST follow), -1 when no room could be made
 *         (capacity full of pinned/unflushable buckets, or OOM) —
 *         the caller MUST take the synchronous parent-update path.
 */
int parent_touch_prepare(struct parent_touch *pt, uint64_t parent_fileid,
                         uint64_t seed_change);

/**
 * Phase 2a: the deferred NDB mutation committed.  Applies
 * logical_change++, pending_delta++, mtime = max(mtime, @p now),
 * marks dirty, unpins.  Cannot fail after a successful prepare().
 *
 * @param out_before  Optional: change value BEFORE this mutation.
 * @param out_after   Optional: change value AFTER (= before + 1).
 * @return 0; -1 only on API misuse (no prepared bucket — the pair is
 *         then synthesised from @p now-free defaults and must not be
 *         trusted; callers treat -1 as a bug).
 */
int parent_touch_commit_prepared(struct parent_touch *pt,
                                 uint64_t parent_fileid,
                                 struct timespec now,
                                 uint64_t *out_before,
                                 uint64_t *out_after);

/** Phase 2b: the deferred NDB mutation failed — unpin.  A clean,
 *  unpinned, zero-pending bucket created solely by this preparation
 *  is freed. */
void parent_touch_abort_prepared(struct parent_touch *pt,
                                 uint64_t parent_fileid);

/**
 * A SYNCHRONOUS in-transaction parent update committed (mkdir /
 * rmdir / link / rename / non-deferred create or remove) for a
 * directory that may have a live bucket: fold the persisted +1 into
 * logical_change (pending unchanged — the invariant holds) and keep
 * the bucket mtime monotonic.  No-op when no bucket exists.
 */
void parent_touch_note_sync_bump(struct parent_touch *pt,
                                 uint64_t parent_fileid,
                                 struct timespec now);

/**
 * Overlay the authoritative logical view onto @p ino (absolute and
 * idempotent — double application is harmless):
 *   ino->change = logical_change,
 *   ino->mtime / ino->ctime = bucket stamp when newer.
 * No-op (returns false) when no bucket exists.  Callers gate on
 * ino->type == DIR — only directories can have buckets.
 */
bool parent_touch_overlay(struct parent_touch *pt,
                          const uint64_t fileid,
                          struct mds_inode *ino);

/**
 * Force-flush a single fileid.
 * @return 0 on hit + clean flush (or already clean), 1 on miss,
 *         -1 on hit + failed flush (bucket stays dirty).
 */
int parent_touch_flush_fileid(struct parent_touch *pt, uint64_t fileid);

/**
 * Flush pending state for @p fileid, then drop the bucket so no
 * overlay applies afterwards.  Used by explicit directory SETATTR
 * (client-set timestamps must win) before it runs its own
 * synchronous write.  A pinned bucket is flushed but NOT dropped
 * (an in-flight preparation still owns it).
 * @return 0 flushed+dropped (or miss), -1 flush failed (bucket kept).
 */
int parent_touch_flush_and_drop(struct parent_touch *pt, uint64_t fileid);

/** Flush every dirty bucket once.  Returns buckets attempted. */
uint32_t parent_touch_flush_all_dirty(struct parent_touch *pt);

/** Read aggregated counters + live populations.  Snapshot. */
void parent_touch_stats_get(const struct parent_touch *pt,
                            struct parent_touch_stats *out);

#endif /* PARENT_TOUCH_H */

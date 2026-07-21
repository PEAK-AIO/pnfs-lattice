/*
 * Copyright (c) 2026 PeakAIO. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-PeakAIO-Proprietary
 *
 * parent_touch.c — deferred parent-directory attribute aggregator.
 *
 * Sharded per-parent-fileid buckets with a periodic flush thread.
 * See parent_touch.h for the public contract and the
 * logical = persisted + pending invariant.  Structure and locking
 * protocol mirror layout_commit_aggregator.c (the reviewed
 * precedent); differences are the additive pending counter, the
 * prepare/commit pin protocol, and flush-time timestamps.
 */

#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "parent_touch.h"
#include "pnfs_mds.h"

/* -----------------------------------------------------------------------
 * Internal data structures
 * ----------------------------------------------------------------------- */

struct pt_bucket {
    uint64_t            fileid;
    uint64_t            logical_change;  /* persisted + pending */
    uint64_t            pending_delta;   /* deferred, not yet flushed */
    struct timespec     stamp;           /* latest local mutation time */
    bool                dirty;           /* pending_delta > 0 */
    uint32_t            pin_count;       /* in-flight preparations */

    /* Hash chain (singly linked, head insertion). */
    struct pt_bucket   *hash_next;

    /* Dirty list (doubly linked, MRU at head). */
    struct pt_bucket   *dl_prev;
    struct pt_bucket   *dl_next;
};

struct pt_shard {
    pthread_mutex_t     lock;

    struct pt_bucket  **buckets;
    uint32_t            bucket_count;    /* hash table size */

    struct pt_bucket   *dl_head;         /* MRU dirty */
    struct pt_bucket   *dl_tail;         /* LRU dirty */

    uint32_t            entry_count;
    uint32_t            pinned_count;    /* buckets with pin_count > 0 */
    uint32_t            capacity;

    uint64_t            stat_submits;
    uint64_t            stat_sync_folds;
    uint64_t            stat_flushes_periodic;
    uint64_t            stat_flushes_forced;
    uint64_t            stat_flush_failures;
    uint64_t            stat_evictions;
    uint64_t            stat_prepare_fallbacks;
    uint64_t            stat_drops;
};

struct parent_touch {
    struct pt_shard        shards[PT_SHARDS];
    uint32_t               flush_interval_ms;

    /* Flush callback pair, updated atomically under cb_lock. */
    pthread_mutex_t        cb_lock;
    parent_touch_flush_fn  flush_fn;
    void                  *flush_cookie;

    /* Flush thread state. */
    pthread_t              thread;
    _Atomic int            running;
    int                    stop_pipe[2];
    bool                   thread_started;
};

/* -----------------------------------------------------------------------
 * Hashing (same scheme as layout_commit_aggregator)
 * ----------------------------------------------------------------------- */

static uint64_t splitmix64(uint64_t x)
{
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

static uint32_t shard_of(uint64_t fileid)
{
    return (uint32_t)(splitmix64(fileid) & (PT_SHARDS - 1U));
}

static uint32_t bucket_of(uint64_t fileid, uint32_t bucket_count)
{
    uint64_t h = splitmix64(fileid);
    h = (h >> 32) | (h << 32);
    return (uint32_t)(h % bucket_count);
}

/* -----------------------------------------------------------------------
 * Shard helpers — caller MUST hold the shard's lock
 * ----------------------------------------------------------------------- */

static struct pt_bucket *shard_find(const struct pt_shard *s,
                                    uint64_t fileid)
{
    uint32_t b = bucket_of(fileid, s->bucket_count);
    struct pt_bucket *e;

    for (e = s->buckets[b]; e != NULL; e = e->hash_next) {
        if (e->fileid == fileid) {
            return e;
        }
    }
    return NULL;
}

static void shard_hash_insert(struct pt_shard *s, struct pt_bucket *e)
{
    uint32_t b = bucket_of(e->fileid, s->bucket_count);
    e->hash_next = s->buckets[b];
    s->buckets[b] = e;
}

static void shard_hash_remove(struct pt_shard *s, struct pt_bucket *e)
{
    uint32_t b = bucket_of(e->fileid, s->bucket_count);
    struct pt_bucket **pp;

    for (pp = &s->buckets[b]; *pp != NULL; pp = &(*pp)->hash_next) {
        if (*pp == e) {
            *pp = e->hash_next;
            e->hash_next = NULL;
            return;
        }
    }
}

static void dl_unlink(struct pt_shard *s, struct pt_bucket *e)
{
    if (e->dl_next != NULL) {
        e->dl_next->dl_prev = e->dl_prev;
    } else if (s->dl_tail == e) {
        s->dl_tail = e->dl_prev;
    }
    if (e->dl_prev != NULL) {
        e->dl_prev->dl_next = e->dl_next;
    } else if (s->dl_head == e) {
        s->dl_head = e->dl_next;
    }
    e->dl_prev = NULL;
    e->dl_next = NULL;
}

static void dl_push_front(struct pt_shard *s, struct pt_bucket *e)
{
    e->dl_prev = NULL;
    e->dl_next = s->dl_head;
    if (s->dl_head != NULL) {
        s->dl_head->dl_prev = e;
    }
    s->dl_head = e;
    if (s->dl_tail == NULL) {
        s->dl_tail = e;
    }
}

/* Mark @e dirty and promote to MRU.  Idempotent. */
static void mark_dirty_mru(struct pt_shard *s, struct pt_bucket *e)
{
    if (e->dirty) {
        if (s->dl_head == e) {
            return;
        }
        dl_unlink(s, e);
    } else {
        e->dirty = true;
    }
    dl_push_front(s, e);
}

/* Mark @e clean and remove from the dirty list.  Idempotent. */
static void mark_clean(struct pt_shard *s, struct pt_bucket *e)
{
    if (!e->dirty) {
        return;
    }
    dl_unlink(s, e);
    e->dirty = false;
}

static void bucket_free(struct pt_bucket *e)
{
    free(e);
}

/* Pin bookkeeping.  Caller holds the shard lock. */
static void bucket_pin(struct pt_shard *s, struct pt_bucket *e)
{
    if (e->pin_count == 0) {
        s->pinned_count++;
    }
    e->pin_count++;
}

static void bucket_unpin(struct pt_shard *s, struct pt_bucket *e)
{
    if (e->pin_count > 0) {
        e->pin_count--;
        if (e->pin_count == 0 && s->pinned_count > 0) {
            s->pinned_count--;
        }
    }
}

/* -----------------------------------------------------------------------
 * Per-shard init / teardown
 * ----------------------------------------------------------------------- */

static uint32_t round_up_pow2(uint32_t n)
{
    uint32_t r = 4;
    while (r < n) {
        r <<= 1;
        if (r == 0) {
            return 1U << 30;
        }
    }
    return r;
}

static int shard_init(struct pt_shard *s, uint32_t capacity)
{
    if (capacity < 4) {
        capacity = 4;
    }
    uint32_t buckets = round_up_pow2(capacity * 2U);

    /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
    s->buckets = calloc(buckets, sizeof(struct pt_bucket *));
    if (s->buckets == NULL) {
        return -1;
    }
    s->bucket_count = buckets;
    s->dl_head      = NULL;
    s->dl_tail      = NULL;
    s->entry_count  = 0;
    s->pinned_count = 0;
    s->capacity     = capacity;
    s->stat_submits           = 0;
    s->stat_sync_folds        = 0;
    s->stat_flushes_periodic  = 0;
    s->stat_flushes_forced    = 0;
    s->stat_flush_failures    = 0;
    s->stat_evictions         = 0;
    s->stat_prepare_fallbacks = 0;
    s->stat_drops             = 0;
    pthread_mutex_init(&s->lock, NULL);
    return 0;
}

static void shard_destroy(struct pt_shard *s)
{
    for (uint32_t b = 0; b < s->bucket_count; b++) {
        struct pt_bucket *e = s->buckets[b];
        while (e != NULL) {
            struct pt_bucket *next = e->hash_next;
            bucket_free(e);
            e = next;
        }
    }
    /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
    free(s->buckets);
    s->buckets = NULL;
    pthread_mutex_destroy(&s->lock);
}

/* -----------------------------------------------------------------------
 * Timestamp helper
 * ----------------------------------------------------------------------- */

static bool stamp_newer(const struct timespec *a, const struct timespec *b)
{
    if (a->tv_sec != b->tv_sec) {
        return a->tv_sec > b->tv_sec;
    }
    return a->tv_nsec > b->tv_nsec;
}

/* -----------------------------------------------------------------------
 * Flush snapshot protocol
 *
 * snapshot: move pending to a local delta, mark clean.  A flush
 * failure RESTORES the delta additively — concurrent
 * commit_prepared() calls that landed while the callback ran are
 * preserved because they added to the (then zero) pending counter.
 * Caller holds the shard lock for both helpers.
 * ----------------------------------------------------------------------- */

static uint64_t snapshot_take(struct pt_shard *s, struct pt_bucket *e)
{
    uint64_t delta = e->pending_delta;

    e->pending_delta = 0;
    mark_clean(s, e);
    return delta;
}

static void snapshot_restore(struct pt_shard *s, struct pt_bucket *e,
                             uint64_t delta)
{
    e->pending_delta += delta;
    if (e->pending_delta > 0) {
        mark_dirty_mru(s, e);
    }
}

/* Read flush_fn + cookie atomically as a pair. */
static void load_flush_cb(struct parent_touch *pt,
                          parent_touch_flush_fn *out_fn,
                          void **out_cookie)
{
    pthread_mutex_lock(&pt->cb_lock);
    *out_fn     = pt->flush_fn;
    *out_cookie = pt->flush_cookie;
    pthread_mutex_unlock(&pt->cb_lock);
}

/* -----------------------------------------------------------------------
 * Public API — lifecycle
 * ----------------------------------------------------------------------- */

int parent_touch_init(uint32_t max_dirs, uint32_t flush_interval_ms,
                      struct parent_touch **out)
{
    if (out == NULL) {
        return -1;
    }
    if (max_dirs == 0) {
        max_dirs = PT_DEFAULT_MAX_DIRS;
    }
    if (flush_interval_ms == 0) {
        flush_interval_ms = PT_DEFAULT_FLUSH_INTERVAL_MS;
    }

    struct parent_touch *pt = calloc(1, sizeof(*pt));
    if (pt == NULL) {
        return -1;
    }

    uint32_t per_shard = (max_dirs + PT_SHARDS - 1U) / PT_SHARDS;

    for (uint32_t i = 0; i < PT_SHARDS; i++) {
        if (shard_init(&pt->shards[i], per_shard) != 0) {
            for (uint32_t j = 0; j < i; j++) {
                shard_destroy(&pt->shards[j]);
            }
            free(pt);
            return -1;
        }
    }

    pt->flush_interval_ms = flush_interval_ms;
    pthread_mutex_init(&pt->cb_lock, NULL);
    pt->flush_fn     = NULL;
    pt->flush_cookie = NULL;
    atomic_store(&pt->running, 0);
    pt->thread_started = false;

    if (pipe(pt->stop_pipe) != 0) {
        for (uint32_t i = 0; i < PT_SHARDS; i++) {
            shard_destroy(&pt->shards[i]);
        }
        pthread_mutex_destroy(&pt->cb_lock);
        free(pt);
        return -1;
    }

    *out = pt;
    return 0;
}

void parent_touch_destroy(struct parent_touch *pt)
{
    if (pt == NULL) {
        return;
    }

    parent_touch_stop(pt);

    if (pt->stop_pipe[0] >= 0) {
        close(pt->stop_pipe[0]);
    }
    if (pt->stop_pipe[1] >= 0) {
        close(pt->stop_pipe[1]);
    }

    for (uint32_t i = 0; i < PT_SHARDS; i++) {
        shard_destroy(&pt->shards[i]);
    }
    pthread_mutex_destroy(&pt->cb_lock);
    free(pt);
}

void parent_touch_set_flush_fn(struct parent_touch *pt,
                               parent_touch_flush_fn fn, void *cookie)
{
    if (pt == NULL) {
        return;
    }
    pthread_mutex_lock(&pt->cb_lock);
    pt->flush_fn     = fn;
    pt->flush_cookie = cookie;
    pthread_mutex_unlock(&pt->cb_lock);
}

/* -----------------------------------------------------------------------
 * prepare / commit_prepared / abort_prepared
 * ----------------------------------------------------------------------- */

/* Detach the LRU dirty UNPINNED bucket, or NULL.  Caller holds the
 * shard lock.  Walks from the tail so the oldest deferred state is
 * persisted first. */
static struct pt_bucket *shard_detach_lru_dirty_unpinned(struct pt_shard *s)
{
    struct pt_bucket *victim = s->dl_tail;

    while (victim != NULL && victim->pin_count > 0) {
        victim = victim->dl_prev;
    }
    if (victim == NULL) {
        return NULL;
    }
    shard_hash_remove(s, victim);
    dl_unlink(s, victim);
    s->entry_count--;
    s->stat_evictions++;
    return victim;
}

/* Detach any clean unpinned bucket, or NULL.  Caller holds lock. */
static struct pt_bucket *shard_detach_any_clean_unpinned(struct pt_shard *s)
{
    for (uint32_t b = 0; b < s->bucket_count; b++) {
        struct pt_bucket *e = s->buckets[b];
        while (e != NULL) {
            if (!e->dirty && e->pin_count == 0) {
                struct pt_bucket *victim = e;
                shard_hash_remove(s, victim);
                /* Clean → not on the dirty list. */
                s->entry_count--;
                s->stat_evictions++;
                return victim;
            }
            e = e->hash_next;
        }
    }
    return NULL;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity): bucket claim/evict ladder */
int parent_touch_prepare(struct parent_touch *pt, uint64_t parent_fileid,
                         uint64_t seed_change)
{
    if (pt == NULL) {
        return -1;
    }
    struct pt_shard *s = &pt->shards[shard_of(parent_fileid)];

    pthread_mutex_lock(&s->lock);

    struct pt_bucket *e = shard_find(s, parent_fileid);
    if (e != NULL) {
        /* Active-active: fold a fresher observed row value in so
         * the logical view (and change_info pairs served from it)
         * never regress below the persisted row. */
        if (seed_change > e->logical_change) {
            e->logical_change = seed_change;
        }
        bucket_pin(s, e);
        pthread_mutex_unlock(&s->lock);
        return 0;
    }

    /* Make room.  Prefer a clean victim (free — nothing to persist);
     * otherwise force-flush the LRU dirty unpinned bucket inline.
     * A failed inline flush aborts the preparation: the caller takes
     * the synchronous path and no deferred state is ever lost. */
    while (s->entry_count >= s->capacity) {
        struct pt_bucket *victim = shard_detach_any_clean_unpinned(s);

        if (victim != NULL) {
            bucket_free(victim);
            continue;
        }
        victim = shard_detach_lru_dirty_unpinned(s);
        if (victim == NULL) {
            /* Every bucket is pinned — capacity misconfiguration or
             * a preparation storm.  Fall back to sync. */
            s->stat_prepare_fallbacks++;
            pthread_mutex_unlock(&s->lock);
            return -1;
        }

        uint64_t        v_fileid  = victim->fileid;
        uint64_t        v_delta   = victim->pending_delta;
        uint64_t        v_logical = victim->logical_change;
        struct timespec v_stamp   = victim->stamp;

        pthread_mutex_unlock(&s->lock);

        parent_touch_flush_fn fn;
        void *cookie;
        struct timespec now;

        load_flush_cb(pt, &fn, &cookie);
        clock_gettime(CLOCK_REALTIME, &now);
        bool flush_ok = false;
        if (fn != NULL && v_delta > 0) {
            flush_ok = (fn(v_fileid, v_delta, now, cookie) == 0);
        } else if (v_delta == 0) {
            flush_ok = true;   /* nothing to persist */
        }

        pthread_mutex_lock(&s->lock);
        if (flush_ok) {
            s->stat_flushes_forced++;
            bucket_free(victim);
            continue;
        }

        /* Flush failed: preserve the victim's deferred state.  While
         * the lock was dropped, a racing prepare may have rebuilt a
         * bucket for the same fileid — merge into it (additive
         * pending, max logical/stamp) instead of inserting a
         * duplicate. */
        struct pt_bucket *existing = shard_find(s, v_fileid);
        if (existing != NULL) {
            existing->pending_delta += v_delta;
            if (v_logical > existing->logical_change) {
                existing->logical_change = v_logical;
            }
            if (stamp_newer(&v_stamp, &existing->stamp)) {
                existing->stamp = v_stamp;
            }
            if (existing->pending_delta > 0) {
                mark_dirty_mru(s, existing);
            }
            bucket_free(victim);
        } else {
            shard_hash_insert(s, victim);
            dl_push_front(s, victim);
            s->entry_count++;
        }
        s->stat_evictions--;
        s->stat_flush_failures++;
        s->stat_prepare_fallbacks++;
        pthread_mutex_unlock(&s->lock);
        return -1;
    }

    e = calloc(1, sizeof(*e));
    if (e == NULL) {
        s->stat_prepare_fallbacks++;
        pthread_mutex_unlock(&s->lock);
        return -1;
    }
    e->fileid         = parent_fileid;
    e->logical_change = seed_change;
    e->pending_delta  = 0;
    e->stamp.tv_sec   = 0;
    e->stamp.tv_nsec  = 0;
    e->dirty          = false;
    e->pin_count      = 0;
    shard_hash_insert(s, e);
    s->entry_count++;
    bucket_pin(s, e);

    pthread_mutex_unlock(&s->lock);
    return 0;
}

int parent_touch_commit_prepared(struct parent_touch *pt,
                                 uint64_t parent_fileid,
                                 struct timespec now,
                                 uint64_t *out_before,
                                 uint64_t *out_after)
{
    if (pt == NULL) {
        return -1;
    }
    struct pt_shard *s = &pt->shards[shard_of(parent_fileid)];
    int rc = -1;

    pthread_mutex_lock(&s->lock);
    struct pt_bucket *e = shard_find(s, parent_fileid);
    if (e != NULL) {
        e->logical_change++;
        e->pending_delta++;
        if (stamp_newer(&now, &e->stamp)) {
            e->stamp = now;
        }
        mark_dirty_mru(s, e);
        if (out_before != NULL) {
            *out_before = e->logical_change - 1;
        }
        if (out_after != NULL) {
            *out_after = e->logical_change;
        }
        bucket_unpin(s, e);
        s->stat_submits++;
        rc = 0;
    } else {
        /* API misuse — commit without a successful prepare.  The
         * pinned bucket cannot have been evicted, so this indicates
         * a caller bug.  Fail loudly via the return code. */
        if (out_before != NULL) {
            *out_before = 0;
        }
        if (out_after != NULL) {
            *out_after = 0;
        }
    }
    pthread_mutex_unlock(&s->lock);
    return rc;
}

void parent_touch_abort_prepared(struct parent_touch *pt,
                                 uint64_t parent_fileid)
{
    if (pt == NULL) {
        return;
    }
    struct pt_shard *s = &pt->shards[shard_of(parent_fileid)];

    pthread_mutex_lock(&s->lock);
    struct pt_bucket *e = shard_find(s, parent_fileid);
    if (e != NULL) {
        bucket_unpin(s, e);
        /* A clean, unpinned, zero-pending bucket carries no
         * information (logical == persisted) — free it so a
         * preparation that never committed leaves no residue. */
        if (e->pin_count == 0 && !e->dirty && e->pending_delta == 0) {
            shard_hash_remove(s, e);
            s->entry_count--;
            bucket_free(e);
        }
    }
    pthread_mutex_unlock(&s->lock);
}

/* -----------------------------------------------------------------------
 * note_sync_bump / overlay
 * ----------------------------------------------------------------------- */

void parent_touch_note_sync_bump(struct parent_touch *pt,
                                 uint64_t parent_fileid,
                                 struct timespec now)
{
    if (pt == NULL) {
        return;
    }
    struct pt_shard *s = &pt->shards[shard_of(parent_fileid)];

    pthread_mutex_lock(&s->lock);
    struct pt_bucket *e = shard_find(s, parent_fileid);
    if (e != NULL) {
        /* The synchronous transaction already bumped the persisted
         * counter by exactly one; fold it into the logical view so
         * logical = persisted + pending still holds.  pending is
         * untouched — nothing new to flush. */
        e->logical_change++;
        if (stamp_newer(&now, &e->stamp)) {
            e->stamp = now;
        }
        s->stat_sync_folds++;
    }
    pthread_mutex_unlock(&s->lock);
}

bool parent_touch_overlay(struct parent_touch *pt,
                          const uint64_t fileid,
                          struct mds_inode *ino)
{
    if (pt == NULL || ino == NULL) {
        return false;
    }
    struct pt_shard *s = &pt->shards[shard_of(fileid)];
    bool hit = false;

    pthread_mutex_lock(&s->lock);
    const struct pt_bucket *e = shard_find(s, fileid);
    if (e != NULL) {
        /* Absolute values — idempotent by construction.  change is
         * always overridden (logical >= any persisted/cached view on
         * the owning MDS); timestamps only move forward so a
         * concurrent explicit SETATTR that already dropped the
         * bucket cannot be overridden retroactively. */
        /* Raise-only (active-active): a peer MDS bumps the row
         * synchronously for directories it does not own, so the
         * freshly-read row can exceed this logical view.  Never
         * drag change backwards - a lowered change lets clients
         * keep negative dentries and peer-created names stay
         * invisible. */
        if (e->logical_change > ino->change) {
            ino->change = e->logical_change;
        }
        if (stamp_newer(&e->stamp, &ino->mtime)) {
            ino->mtime = e->stamp;
        }
        if (stamp_newer(&e->stamp, &ino->ctime)) {
            ino->ctime = e->stamp;
        }
        hit = true;
    }
    pthread_mutex_unlock(&s->lock);
    return hit;
}

/* -----------------------------------------------------------------------
 * flush_fileid / flush_and_drop
 * ----------------------------------------------------------------------- */

int parent_touch_flush_fileid(struct parent_touch *pt, uint64_t fileid)
{
    if (pt == NULL) {
        return -1;
    }
    parent_touch_flush_fn fn;
    void *cookie;

    load_flush_cb(pt, &fn, &cookie);

    struct pt_shard *s = &pt->shards[shard_of(fileid)];

    pthread_mutex_lock(&s->lock);
    struct pt_bucket *e = shard_find(s, fileid);
    if (e == NULL) {
        pthread_mutex_unlock(&s->lock);
        return 1;   /* miss */
    }
    if (!e->dirty || e->pending_delta == 0) {
        s->stat_flushes_forced++;
        pthread_mutex_unlock(&s->lock);
        return 0;   /* already clean */
    }
    uint64_t delta = snapshot_take(s, e);
    pthread_mutex_unlock(&s->lock);

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);

    int cb_rc = -1;
    if (fn != NULL) {
        cb_rc = fn(fileid, delta, now, cookie);
    }

    pthread_mutex_lock(&s->lock);
    if (cb_rc == 0) {
        s->stat_flushes_forced++;
        pthread_mutex_unlock(&s->lock);
        return 0;
    }
    e = shard_find(s, fileid);
    if (e != NULL) {
        snapshot_restore(s, e, delta);
    }
    s->stat_flush_failures++;
    pthread_mutex_unlock(&s->lock);
    return -1;
}

int parent_touch_flush_and_drop(struct parent_touch *pt, uint64_t fileid)
{
    if (pt == NULL) {
        return -1;
    }
    int frc = parent_touch_flush_fileid(pt, fileid);
    if (frc < 0) {
        return -1;   /* flush failed — keep the bucket + its state */
    }

    struct pt_shard *s = &pt->shards[shard_of(fileid)];

    pthread_mutex_lock(&s->lock);
    struct pt_bucket *e = shard_find(s, fileid);
    if (e != NULL && e->pin_count == 0 && e->pending_delta == 0) {
        shard_hash_remove(s, e);
        if (e->dirty) {
            dl_unlink(s, e);
        }
        s->entry_count--;
        s->stat_drops++;
        bucket_free(e);
    }
    pthread_mutex_unlock(&s->lock);
    return 0;
}

/* -----------------------------------------------------------------------
 * flush_all_dirty + periodic thread
 * ----------------------------------------------------------------------- */

#define PT_FLUSH_BATCH 64

struct pt_flush_item {
    uint64_t fileid;
    uint64_t delta;
};

/* Drain one shard.  Returns the number of buckets attempted. */
static uint32_t drain_shard(struct parent_touch *pt, struct pt_shard *s,
                            bool periodic)
{
    parent_touch_flush_fn fn;
    void *cookie;
    uint32_t attempted = 0;

    load_flush_cb(pt, &fn, &cookie);
    if (fn == NULL) {
        return 0;
    }

    for (;;) {
        struct pt_flush_item items[PT_FLUSH_BATCH];
        uint32_t n = 0;

        pthread_mutex_lock(&s->lock);
        while (n < PT_FLUSH_BATCH && s->dl_tail != NULL) {
            struct pt_bucket *e = s->dl_tail;

            items[n].fileid = e->fileid;
            items[n].delta  = snapshot_take(s, e);
            n++;
        }
        pthread_mutex_unlock(&s->lock);

        if (n == 0) {
            break;
        }
        attempted += n;

        for (uint32_t i = 0; i < n; i++) {
            struct timespec now;

            clock_gettime(CLOCK_REALTIME, &now);
            int cb_rc = fn(items[i].fileid, items[i].delta, now,
                           cookie);

            pthread_mutex_lock(&s->lock);
            if (cb_rc == 0) {
                if (periodic) {
                    s->stat_flushes_periodic++;
                } else {
                    s->stat_flushes_forced++;
                }
            } else {
                struct pt_bucket *e = shard_find(s, items[i].fileid);
                if (e != NULL) {
                    snapshot_restore(s, e, items[i].delta);
                }
                s->stat_flush_failures++;
            }
            pthread_mutex_unlock(&s->lock);
        }
    }

    return attempted;
}

uint32_t parent_touch_flush_all_dirty(struct parent_touch *pt)
{
    if (pt == NULL) {
        return 0;
    }
    uint32_t total = 0;
    for (uint32_t i = 0; i < PT_SHARDS; i++) {
        total += drain_shard(pt, &pt->shards[i], /*periodic=*/false);
    }
    return total;
}

/* Block on stop_pipe with a millisecond timeout.  1 = stop fired,
 * 0 = timeout (fire a tick), -1 = error. */
static int wait_for_tick_or_stop(int stop_fd, uint32_t ms)
{
    /* poll(), not select()/FD_SET: the stop-pipe fd can exceed
     * FD_SETSIZE (1024) on a busy MDS, and FD_SET() on such an fd
     * trips glibc's __fdelt_chk ("*** buffer overflow detected ***")
     * and aborts.  poll() has no descriptor-value limit. */
    struct pollfd pfd;

    pfd.fd = stop_fd;
    pfd.events = POLLIN;
    pfd.revents = 0;

    int rc = poll(&pfd, 1, (ms > (uint32_t)INT_MAX) ? INT_MAX : (int)ms);
    if (rc < 0) {
        if (errno == EINTR) {
            return 0;
        }
        return -1;
    }
    return rc > 0 ? 1 : 0;
}

static void *flush_thread_main(void *arg)
{
    struct parent_touch *pt = arg;

    while (atomic_load(&pt->running) != 0) {
        int rc = wait_for_tick_or_stop(pt->stop_pipe[0],
                                       pt->flush_interval_ms);
        if (rc != 0) {
            break;   /* stop fired or select error */
        }
        for (uint32_t i = 0; i < PT_SHARDS; i++) {
            (void)drain_shard(pt, &pt->shards[i], /*periodic=*/true);
            if (atomic_load(&pt->running) == 0) {
                break;
            }
        }
    }
    return NULL;
}

int parent_touch_start(struct parent_touch *pt)
{
    if (pt == NULL || pt->thread_started) {
        return -1;
    }
    atomic_store(&pt->running, 1);
    if (pthread_create(&pt->thread, NULL, flush_thread_main, pt) != 0) {
        atomic_store(&pt->running, 0);
        return -1;
    }
    pt->thread_started = true;
    return 0;
}

void parent_touch_stop(struct parent_touch *pt)
{
    if (pt == NULL || !pt->thread_started) {
        return;
    }
    atomic_store(&pt->running, 0);
    char b = 1;
    ssize_t wr = write(pt->stop_pipe[1], &b, 1);
    (void)wr;   /* best-effort wake-up */
    pthread_join(pt->thread, NULL);
    pt->thread_started = false;
}

/* -----------------------------------------------------------------------
 * Stats
 * ----------------------------------------------------------------------- */

void parent_touch_stats_get(const struct parent_touch *pt,
                            struct parent_touch_stats *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (pt == NULL) {
        return;
    }

    /* Cast away const to take shard mutexes; logically read-only. */
    struct parent_touch *pw = (struct parent_touch *)pt;

    for (uint32_t i = 0; i < PT_SHARDS; i++) {
        struct pt_shard *s = &pw->shards[i];

        pthread_mutex_lock(&s->lock);
        out->submits           += s->stat_submits;
        out->sync_folds        += s->stat_sync_folds;
        out->flushes_periodic  += s->stat_flushes_periodic;
        out->flushes_forced    += s->stat_flushes_forced;
        out->flush_failures    += s->stat_flush_failures;
        out->evictions         += s->stat_evictions;
        out->prepare_fallbacks += s->stat_prepare_fallbacks;
        out->drops             += s->stat_drops;
        out->entry_count       += s->entry_count;
        out->pinned_count      += s->pinned_count;
        pthread_mutex_unlock(&s->lock);
    }
}

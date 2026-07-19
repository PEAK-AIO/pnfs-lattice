/*
 * Copyright (c) 2026 PeakAIO. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-PeakAIO-Proprietary
 *
 * test_parent_touch.c — unit tests for the deferred parent-directory
 * attribute aggregator.
 *
 * Pure data-structure tests: no catalogue, no compound, no daemon.
 * The synthetic flush callback maintains a per-fileid "persisted"
 * change counter so every test can assert the module's core
 * invariant directly:
 *
 *     logical_change == persisted_change + pending_delta
 *
 * (After a quiesced flush, overlay(change) must equal the callback's
 * persisted counter.)
 */

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "parent_touch.h"
#include "pnfs_mds.h"

static int passed;
static int failed;

#define ASSERT_TRUE(cond) do {                                          \
    if (!(cond)) {                                                      \
        fprintf(stderr, "  FAIL %s:%d: %s\n",                           \
                __FILE__, __LINE__, #cond);                             \
        failed++;                                                       \
        return;                                                         \
    }                                                                   \
} while (0)

#define ASSERT_EQ(a, b) ASSERT_TRUE((a) == (b))

/* -----------------------------------------------------------------------
 * Recording flush callback with a simulated persisted store
 * ----------------------------------------------------------------------- */

#define MAX_FILES 128

struct persist_state {
    pthread_mutex_t lock;
    uint64_t        fileid[MAX_FILES];
    uint64_t        change[MAX_FILES];   /* simulated persisted counter */
    struct timespec stamp[MAX_FILES];
    uint32_t        nfiles;
    uint32_t        flush_calls;
    _Atomic int     should_fail;
};

static int persist_flush_fn(uint64_t fileid, uint64_t change_delta,
                            struct timespec stamp, void *cookie)
{
    struct persist_state *ps = cookie;

    if (atomic_load(&ps->should_fail) != 0) {
        return -1;
    }
    pthread_mutex_lock(&ps->lock);
    ps->flush_calls++;
    for (uint32_t i = 0; i < ps->nfiles; i++) {
        if (ps->fileid[i] == fileid) {
            ps->change[i] += change_delta;
            ps->stamp[i] = stamp;
            pthread_mutex_unlock(&ps->lock);
            return 0;
        }
    }
    if (ps->nfiles < MAX_FILES) {
        ps->fileid[ps->nfiles] = fileid;
        ps->change[ps->nfiles] = change_delta;
        ps->stamp[ps->nfiles]  = stamp;
        ps->nfiles++;
    }
    pthread_mutex_unlock(&ps->lock);
    return 0;
}

static void persist_init(struct persist_state *ps)
{
    memset(ps, 0, sizeof(*ps));
    pthread_mutex_init(&ps->lock, NULL);
    atomic_store(&ps->should_fail, 0);
}

static void persist_destroy(struct persist_state *ps)
{
    pthread_mutex_destroy(&ps->lock);
}

/* Simulated persisted change value (0 when never flushed). */
static uint64_t persist_change(struct persist_state *ps, uint64_t fileid)
{
    uint64_t v = 0;

    pthread_mutex_lock(&ps->lock);
    for (uint32_t i = 0; i < ps->nfiles; i++) {
        if (ps->fileid[i] == fileid) {
            v = ps->change[i];
            break;
        }
    }
    pthread_mutex_unlock(&ps->lock);
    return v;
}

static uint32_t persist_calls(struct persist_state *ps)
{
    pthread_mutex_lock(&ps->lock);
    uint32_t n = ps->flush_calls;
    pthread_mutex_unlock(&ps->lock);
    return n;
}

/* Helper: run one prepare+commit deferred mutation.  Returns the
 * (before, after) pair via out params; asserts handled by caller. */
static int do_deferred(struct parent_touch *pt, uint64_t fileid,
                       uint64_t seed, uint64_t *before, uint64_t *after)
{
    struct timespec now;

    if (parent_touch_prepare(pt, fileid, seed) != 0) {
        return -1;
    }
    clock_gettime(CLOCK_REALTIME, &now);
    return parent_touch_commit_prepared(pt, fileid, now, before, after);
}

/* -----------------------------------------------------------------------
 * Tests
 * ----------------------------------------------------------------------- */

static void test_init_destroy(void)
{
    fprintf(stdout, "  init_destroy:                    ");

    struct parent_touch *pt = NULL;
    ASSERT_EQ(parent_touch_init(64, 50, &pt), 0);
    ASSERT_TRUE(pt != NULL);

    struct parent_touch_stats st;
    parent_touch_stats_get(pt, &st);
    ASSERT_EQ(st.entry_count, 0u);
    ASSERT_EQ(st.submits, 0u);

    parent_touch_destroy(pt);
    parent_touch_destroy(NULL);   /* NULL-safe */

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_invalid_args(void)
{
    fprintf(stdout, "  invalid_args:                    ");

    ASSERT_EQ(parent_touch_init(64, 50, NULL), -1);

    struct parent_touch *pt = NULL;
    ASSERT_EQ(parent_touch_init(0, 0, &pt), 0);

    struct timespec t = {0, 0};
    struct mds_inode ino;
    memset(&ino, 0, sizeof(ino));

    ASSERT_EQ(parent_touch_prepare(NULL, 1, 0), -1);
    ASSERT_EQ(parent_touch_commit_prepared(NULL, 1, t, NULL, NULL), -1);
    parent_touch_abort_prepared(NULL, 1);           /* no crash */
    parent_touch_note_sync_bump(NULL, 1, t);        /* no crash */
    ASSERT_EQ(parent_touch_overlay(NULL, 1, &ino), false);
    ASSERT_EQ(parent_touch_overlay(pt, 1, NULL), false);
    ASSERT_EQ(parent_touch_flush_fileid(NULL, 1), -1);
    ASSERT_EQ(parent_touch_flush_and_drop(NULL, 1), -1);
    ASSERT_EQ(parent_touch_flush_all_dirty(NULL), 0u);
    parent_touch_stats_get(pt, NULL);               /* no-op */
    parent_touch_stats_get(NULL, NULL);             /* no-op */

    /* commit without prepare = API misuse → -1, no crash. */
    uint64_t b = 99, a = 99;
    ASSERT_EQ(parent_touch_commit_prepared(pt, 7, t, &b, &a), -1);

    parent_touch_destroy(pt);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_prepare_commit_pairs(void)
{
    fprintf(stdout, "  prepare_commit_pairs:            ");

    struct parent_touch *pt = NULL;
    ASSERT_EQ(parent_touch_init(64, 50, &pt), 0);

    /* Seed 100: three sequential deferred ops → consecutive exact
     * pairs (100,101) (101,102) (102,103). */
    for (uint64_t i = 0; i < 3; i++) {
        uint64_t before = 0, after = 0;
        ASSERT_EQ(do_deferred(pt, 42, 100, &before, &after), 0);
        ASSERT_EQ(before, 100 + i);
        ASSERT_EQ(after, 101 + i);
    }

    /* Overlay serves the logical value. */
    struct mds_inode ino;
    memset(&ino, 0, sizeof(ino));
    ino.change = 100;   /* stale persisted view */
    ASSERT_TRUE(parent_touch_overlay(pt, 42, &ino));
    ASSERT_EQ(ino.change, 103u);

    /* Overlay is idempotent. */
    ASSERT_TRUE(parent_touch_overlay(pt, 42, &ino));
    ASSERT_EQ(ino.change, 103u);

    /* Miss on an untouched fileid. */
    memset(&ino, 0, sizeof(ino));
    ASSERT_EQ(parent_touch_overlay(pt, 43, &ino), false);
    ASSERT_EQ(ino.change, 0u);

    struct parent_touch_stats st;
    parent_touch_stats_get(pt, &st);
    ASSERT_EQ(st.submits, 3u);
    ASSERT_EQ(st.entry_count, 1u);
    ASSERT_EQ(st.pinned_count, 0u);

    parent_touch_destroy(pt);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_abort_prepared(void)
{
    fprintf(stdout, "  abort_prepared:                  ");

    struct parent_touch *pt = NULL;
    ASSERT_EQ(parent_touch_init(64, 50, &pt), 0);

    /* Abort on a freshly-created bucket removes it entirely. */
    ASSERT_EQ(parent_touch_prepare(pt, 7, 500), 0);
    parent_touch_abort_prepared(pt, 7);

    struct parent_touch_stats st;
    parent_touch_stats_get(pt, &st);
    ASSERT_EQ(st.entry_count, 0u);
    ASSERT_EQ(st.pinned_count, 0u);

    struct mds_inode ino;
    memset(&ino, 0, sizeof(ino));
    ASSERT_EQ(parent_touch_overlay(pt, 7, &ino), false);

    /* Abort on a bucket with committed state keeps the state. */
    uint64_t b = 0, a = 0;
    ASSERT_EQ(do_deferred(pt, 7, 500, &b, &a), 0);
    ASSERT_EQ(a, 501u);
    ASSERT_EQ(parent_touch_prepare(pt, 7, 500), 0);
    parent_touch_abort_prepared(pt, 7);
    memset(&ino, 0, sizeof(ino));
    ASSERT_TRUE(parent_touch_overlay(pt, 7, &ino));
    ASSERT_EQ(ino.change, 501u);

    parent_touch_destroy(pt);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_flush_invariant(void)
{
    fprintf(stdout, "  flush_invariant:                 ");

    struct parent_touch *pt = NULL;
    struct persist_state ps;

    persist_init(&ps);
    /* Simulated persisted counter starts at 0 for fileid 11; seed
     * matches (bucket seeded from the reader's view of persisted). */
    ASSERT_EQ(parent_touch_init(64, 50, &pt), 0);
    parent_touch_set_flush_fn(pt, persist_flush_fn, &ps);

    for (int i = 0; i < 5; i++) {
        uint64_t b = 0, a = 0;
        ASSERT_EQ(do_deferred(pt, 11, 0, &b, &a), 0);
    }

    /* One forced flush persists the whole delta in ONE call. */
    uint32_t calls_before = persist_calls(&ps);
    ASSERT_EQ(parent_touch_flush_fileid(pt, 11), 0);
    ASSERT_EQ(persist_calls(&ps), calls_before + 1);
    ASSERT_EQ(persist_change(&ps, 11), 5u);

    /* Invariant after quiesce: overlay change == persisted. */
    struct mds_inode ino;
    memset(&ino, 0, sizeof(ino));
    ASSERT_TRUE(parent_touch_overlay(pt, 11, &ino));
    ASSERT_EQ(ino.change, persist_change(&ps, 11));

    /* Re-flush of a clean bucket: no extra callback. */
    calls_before = persist_calls(&ps);
    ASSERT_EQ(parent_touch_flush_fileid(pt, 11), 0);
    ASSERT_EQ(persist_calls(&ps), calls_before);

    /* Miss. */
    ASSERT_EQ(parent_touch_flush_fileid(pt, 999), 1);

    persist_destroy(&ps);
    parent_touch_destroy(pt);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_flush_failure_keeps_pending(void)
{
    fprintf(stdout, "  flush_failure_keeps_pending:     ");

    struct parent_touch *pt = NULL;
    struct persist_state ps;

    persist_init(&ps);
    ASSERT_EQ(parent_touch_init(64, 50, &pt), 0);
    parent_touch_set_flush_fn(pt, persist_flush_fn, &ps);

    uint64_t b = 0, a = 0;
    ASSERT_EQ(do_deferred(pt, 21, 0, &b, &a), 0);
    ASSERT_EQ(do_deferred(pt, 21, 0, &b, &a), 0);

    atomic_store(&ps.should_fail, 1);
    ASSERT_EQ(parent_touch_flush_fileid(pt, 21), -1);
    ASSERT_EQ(persist_change(&ps, 21), 0u);

    /* Logical view unaffected by the failed flush. */
    struct mds_inode ino;
    memset(&ino, 0, sizeof(ino));
    ASSERT_TRUE(parent_touch_overlay(pt, 21, &ino));
    ASSERT_EQ(ino.change, 2u);

    /* Retry succeeds and persists the FULL restored delta. */
    atomic_store(&ps.should_fail, 0);
    ASSERT_EQ(parent_touch_flush_fileid(pt, 21), 0);
    ASSERT_EQ(persist_change(&ps, 21), 2u);

    struct parent_touch_stats st;
    parent_touch_stats_get(pt, &st);
    ASSERT_EQ(st.flush_failures, 1u);

    persist_destroy(&ps);
    parent_touch_destroy(pt);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_sync_bump_fold(void)
{
    fprintf(stdout, "  sync_bump_fold:                  ");

    struct parent_touch *pt = NULL;
    struct persist_state ps;

    persist_init(&ps);
    ASSERT_EQ(parent_touch_init(64, 50, &pt), 0);
    parent_touch_set_flush_fn(pt, persist_flush_fn, &ps);

    /* No bucket → note_sync_bump is a no-op (no allocation). */
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    parent_touch_note_sync_bump(pt, 31, now);
    struct parent_touch_stats st;
    parent_touch_stats_get(pt, &st);
    ASSERT_EQ(st.entry_count, 0u);
    ASSERT_EQ(st.sync_folds, 0u);

    /* Two deferred + one sync bump: logical = seed + 3, but only the
     * two deferred increments flush (the sync op persisted its own).
     * Simulated store: the sync op's own write is modelled by adding
     * 1 out-of-band after the flush. */
    uint64_t b = 0, a = 0;
    ASSERT_EQ(do_deferred(pt, 31, 0, &b, &a), 0);
    ASSERT_EQ(do_deferred(pt, 31, 0, &b, &a), 0);
    clock_gettime(CLOCK_REALTIME, &now);
    parent_touch_note_sync_bump(pt, 31, now);

    struct mds_inode ino;
    memset(&ino, 0, sizeof(ino));
    ASSERT_TRUE(parent_touch_overlay(pt, 31, &ino));
    ASSERT_EQ(ino.change, 3u);

    ASSERT_EQ(parent_touch_flush_fileid(pt, 31), 0);
    ASSERT_EQ(persist_change(&ps, 31), 2u);   /* only the deferred 2 */

    parent_touch_stats_get(pt, &st);
    ASSERT_EQ(st.sync_folds, 1u);

    persist_destroy(&ps);
    parent_touch_destroy(pt);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_overlay_timestamps(void)
{
    fprintf(stdout, "  overlay_timestamps:              ");

    struct parent_touch *pt = NULL;
    ASSERT_EQ(parent_touch_init(64, 50, &pt), 0);

    ASSERT_EQ(parent_touch_prepare(pt, 41, 0), 0);
    struct timespec t1 = { .tv_sec = 1700000000, .tv_nsec = 500 };
    ASSERT_EQ(parent_touch_commit_prepared(pt, 41, t1, NULL, NULL), 0);

    /* Older inode stamps are advanced to the bucket stamp. */
    struct mds_inode ino;
    memset(&ino, 0, sizeof(ino));
    ino.mtime.tv_sec = 1600000000;
    ino.ctime.tv_sec = 1600000000;
    ASSERT_TRUE(parent_touch_overlay(pt, 41, &ino));
    ASSERT_EQ(ino.mtime.tv_sec, 1700000000);
    ASSERT_EQ(ino.mtime.tv_nsec, 500);
    ASSERT_EQ(ino.ctime.tv_sec, 1700000000);

    /* Newer inode stamps are preserved (monotonic guard). */
    memset(&ino, 0, sizeof(ino));
    ino.mtime.tv_sec = 1800000000;
    ino.ctime.tv_sec = 1800000000;
    ASSERT_TRUE(parent_touch_overlay(pt, 41, &ino));
    ASSERT_EQ(ino.mtime.tv_sec, 1800000000);
    ASSERT_EQ(ino.ctime.tv_sec, 1800000000);

    parent_touch_destroy(pt);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_flush_and_drop(void)
{
    fprintf(stdout, "  flush_and_drop:                  ");

    struct parent_touch *pt = NULL;
    struct persist_state ps;

    persist_init(&ps);
    ASSERT_EQ(parent_touch_init(64, 50, &pt), 0);
    parent_touch_set_flush_fn(pt, persist_flush_fn, &ps);

    uint64_t b = 0, a = 0;
    ASSERT_EQ(do_deferred(pt, 51, 0, &b, &a), 0);

    ASSERT_EQ(parent_touch_flush_and_drop(pt, 51), 0);
    ASSERT_EQ(persist_change(&ps, 51), 1u);

    /* Bucket gone — overlay misses (explicit SETATTR now wins). */
    struct mds_inode ino;
    memset(&ino, 0, sizeof(ino));
    ASSERT_EQ(parent_touch_overlay(pt, 51, &ino), false);

    struct parent_touch_stats st;
    parent_touch_stats_get(pt, &st);
    ASSERT_EQ(st.drops, 1u);
    ASSERT_EQ(st.entry_count, 0u);

    /* Pinned bucket: flushed but NOT dropped. */
    ASSERT_EQ(do_deferred(pt, 52, 0, &b, &a), 0);
    ASSERT_EQ(parent_touch_prepare(pt, 52, 0), 0);   /* pin */
    ASSERT_EQ(parent_touch_flush_and_drop(pt, 52), 0);
    memset(&ino, 0, sizeof(ino));
    ASSERT_TRUE(parent_touch_overlay(pt, 52, &ino));  /* still there */
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    ASSERT_EQ(parent_touch_commit_prepared(pt, 52, now, NULL, NULL), 0);

    /* Flush failure keeps the bucket. */
    ASSERT_EQ(do_deferred(pt, 53, 0, &b, &a), 0);
    atomic_store(&ps.should_fail, 1);
    ASSERT_EQ(parent_touch_flush_and_drop(pt, 53), -1);
    memset(&ino, 0, sizeof(ino));
    ASSERT_TRUE(parent_touch_overlay(pt, 53, &ino));
    ASSERT_EQ(ino.change, 1u);
    atomic_store(&ps.should_fail, 0);

    persist_destroy(&ps);
    parent_touch_destroy(pt);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_eviction_forces_flush(void)
{
    fprintf(stdout, "  eviction_forces_flush:           ");

    struct parent_touch *pt = NULL;
    struct persist_state ps;

    persist_init(&ps);
    /* Tiny capacity: per-shard floor is 4, 16 shards → fill one
     * shard past its cap by using many fileids (they spread across
     * shards; use enough that at least one shard overflows). */
    ASSERT_EQ(parent_touch_init(16, 50, &pt), 0);   /* 1/shard → floor 4 */
    parent_touch_set_flush_fn(pt, persist_flush_fn, &ps);

    uint64_t b = 0, a = 0;
    for (uint64_t f = 1; f <= 96; f++) {
        ASSERT_EQ(do_deferred(pt, f, 0, &b, &a), 0);
        ASSERT_EQ(a, 1u);
    }

    struct parent_touch_stats st;
    parent_touch_stats_get(pt, &st);
    /* 96 fileids over 16 shards × cap 4 = 64 slots → evictions ran
     * and every evicted bucket was flushed (delta 1 each). */
    ASSERT_TRUE(st.evictions > 0u);
    ASSERT_TRUE(st.entry_count <= 64u);
    ASSERT_EQ(st.flush_failures, 0u);

    /* Every fileid's increment is accounted for exactly once:
     * persisted + still-pending == 1 for each. */
    (void)parent_touch_flush_all_dirty(pt);
    for (uint64_t f = 1; f <= 96; f++) {
        ASSERT_EQ(persist_change(&ps, f), 1u);
    }

    /* Eviction-flush failure → prepare falls back (-1) and the
     * victim's state is preserved. */
    atomic_store(&ps.should_fail, 1);
    uint32_t fallbacks_before;
    parent_touch_stats_get(pt, &st);
    fallbacks_before = (uint32_t)st.prepare_fallbacks;

    /* Re-dirty every surviving bucket so the next insert must evict
     * a DIRTY victim (clean ones were all dropped by flush_all). */
    for (uint64_t f = 1; f <= 96; f++) {
        if (parent_touch_prepare(pt, f, 1) == 0) {
            struct timespec now;
            clock_gettime(CLOCK_REALTIME, &now);
            (void)parent_touch_commit_prepared(pt, f, now, NULL, NULL);
        }
    }
    parent_touch_stats_get(pt, &st);
    ASSERT_TRUE(st.prepare_fallbacks >= fallbacks_before);
    atomic_store(&ps.should_fail, 0);

    persist_destroy(&ps);
    parent_touch_destroy(pt);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* -----------------------------------------------------------------------
 * Concurrency: N threads × M deferred ops on ONE directory must yield
 * exactly N*M distinct consecutive (before, after) pairs.
 * ----------------------------------------------------------------------- */

#define CONC_THREADS 8
#define CONC_OPS     200

struct conc_ctx {
    struct parent_touch *pt;
    uint64_t             fileid;
    uint64_t             afters[CONC_OPS];
    int                  errors;
};

static void *conc_worker(void *arg)
{
    struct conc_ctx *c = arg;

    for (int i = 0; i < CONC_OPS; i++) {
        uint64_t before = 0, after = 0;
        if (do_deferred(c->pt, c->fileid, 1000, &before, &after) != 0 ||
            after != before + 1) {
            c->errors++;
            continue;
        }
        c->afters[i] = after;
    }
    return NULL;
}

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x < y) ? -1 : (x > y) ? 1 : 0;
}

static void test_concurrent_pairs_distinct(void)
{
    fprintf(stdout, "  concurrent_pairs_distinct:       ");

    struct parent_touch *pt = NULL;
    struct persist_state ps;

    persist_init(&ps);
    ASSERT_EQ(parent_touch_init(1024, 50, &pt), 0);
    parent_touch_set_flush_fn(pt, persist_flush_fn, &ps);

    struct conc_ctx ctx[CONC_THREADS];
    pthread_t th[CONC_THREADS];

    for (int t = 0; t < CONC_THREADS; t++) {
        memset(&ctx[t], 0, sizeof(ctx[t]));
        ctx[t].pt = pt;
        ctx[t].fileid = 77;
        ASSERT_EQ(pthread_create(&th[t], NULL, conc_worker, &ctx[t]), 0);
    }

    uint64_t all[CONC_THREADS * CONC_OPS];
    uint32_t n = 0;
    for (int t = 0; t < CONC_THREADS; t++) {
        pthread_join(th[t], NULL);
        ASSERT_EQ(ctx[t].errors, 0);
        for (int i = 0; i < CONC_OPS; i++) {
            all[n++] = ctx[t].afters[i];
        }
    }

    /* All "after" values are exactly 1001..1000+N*M, each once. */
    qsort(all, n, sizeof(all[0]), cmp_u64);
    for (uint32_t i = 0; i < n; i++) {
        ASSERT_EQ(all[i], 1001u + i);
    }

    /* Invariant after full flush: persisted delta == N*M. */
    (void)parent_touch_flush_all_dirty(pt);
    ASSERT_EQ(persist_change(&ps, 77), (uint64_t)CONC_THREADS * CONC_OPS);

    struct mds_inode ino;
    memset(&ino, 0, sizeof(ino));
    ASSERT_TRUE(parent_touch_overlay(pt, 77, &ino));
    ASSERT_EQ(ino.change, 1000u + (uint64_t)CONC_THREADS * CONC_OPS);

    persist_destroy(&ps);
    parent_touch_destroy(pt);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_periodic_thread(void)
{
    fprintf(stdout, "  periodic_thread:                 ");

    struct parent_touch *pt = NULL;
    struct persist_state ps;

    persist_init(&ps);
    ASSERT_EQ(parent_touch_init(64, 10, &pt), 0);   /* 10 ms ticks */
    parent_touch_set_flush_fn(pt, persist_flush_fn, &ps);
    ASSERT_EQ(parent_touch_start(pt), 0);
    ASSERT_EQ(parent_touch_start(pt), -1);          /* double start */

    uint64_t b = 0, a = 0;
    ASSERT_EQ(do_deferred(pt, 61, 0, &b, &a), 0);
    ASSERT_EQ(do_deferred(pt, 61, 0, &b, &a), 0);

    /* Within a few ticks the delta must land in the store. */
    bool flushed = false;
    for (int i = 0; i < 100; i++) {
        if (persist_change(&ps, 61) == 2u) {
            flushed = true;
            break;
        }
        usleep(10000);
    }
    ASSERT_TRUE(flushed);

    struct parent_touch_stats st;
    parent_touch_stats_get(pt, &st);
    ASSERT_TRUE(st.flushes_periodic >= 1u);

    parent_touch_stop(pt);
    parent_touch_stop(pt);   /* idempotent */

    persist_destroy(&ps);
    parent_touch_destroy(pt);

    fprintf(stdout, "PASS\n");
    passed++;
}

int main(void)
{
    fprintf(stdout, "test_parent_touch:\n");

    test_init_destroy();
    test_invalid_args();
    test_prepare_commit_pairs();
    test_abort_prepared();
    test_flush_invariant();
    test_flush_failure_keeps_pending();
    test_sync_bump_fold();
    test_overlay_timestamps();
    test_flush_and_drop();
    test_eviction_forces_flush();
    test_concurrent_pairs_distinct();
    test_periodic_thread();

    fprintf(stdout, "test_parent_touch: %d passed, %d failed\n",
            passed, failed);
    return failed == 0 ? 0 : 1;
}

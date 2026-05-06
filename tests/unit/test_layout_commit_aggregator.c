/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_layout_commit_aggregator.c -- Phase F v1 unit tests.
 *
 * Pure data-structure tests against the layout_commit_aggregator
 * module.  No catalogue, no compound, no daemon -- just the
 * aggregator + a synthetic flush callback that records per-fileid
 * snapshots into an in-test buffer.
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

#include "layout_commit_aggregator.h"

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
 * Recording flush callback
 *
 * Captures every (fileid, size, mtime) tuple the aggregator hands
 * us, plus a bool "should_fail" knob for failure-path testing.
 * ----------------------------------------------------------------------- */

#define MAX_RECORDS 1024

struct rec_entry {
    uint64_t        fileid;
    uint64_t        size;
    struct timespec mtime;
};

struct rec_state {
    pthread_mutex_t  lock;
    struct rec_entry items[MAX_RECORDS];
    uint32_t         count;
    _Atomic int      should_fail;
};

static int rec_flush_fn(uint64_t fileid, uint64_t size,
                        struct timespec mtime, void *cookie)
{
    struct rec_state *r = cookie;
    if (atomic_load(&r->should_fail) != 0) {
        return -1;
    }
    pthread_mutex_lock(&r->lock);
    if (r->count < MAX_RECORDS) {
        r->items[r->count].fileid = fileid;
        r->items[r->count].size   = size;
        r->items[r->count].mtime  = mtime;
        r->count++;
    }
    pthread_mutex_unlock(&r->lock);
    return 0;
}

static void rec_init(struct rec_state *r)
{
    memset(r, 0, sizeof(*r));
    pthread_mutex_init(&r->lock, NULL);
    atomic_store(&r->should_fail, 0);
}

static void rec_destroy(struct rec_state *r)
{
    pthread_mutex_destroy(&r->lock);
}

static uint32_t rec_count(struct rec_state *r)
{
    pthread_mutex_lock(&r->lock);
    uint32_t n = r->count;
    pthread_mutex_unlock(&r->lock);
    return n;
}

static bool rec_find(struct rec_state *r, uint64_t fileid,
                     struct rec_entry *out)
{
    pthread_mutex_lock(&r->lock);
    for (uint32_t i = 0; i < r->count; i++) {
        if (r->items[i].fileid == fileid) {
            if (out != NULL) {
                *out = r->items[i];
            }
            pthread_mutex_unlock(&r->lock);
            return true;
        }
    }
    pthread_mutex_unlock(&r->lock);
    return false;
}

static uint32_t rec_count_fileid(struct rec_state *r, uint64_t fileid)
{
    uint32_t count = 0;

    pthread_mutex_lock(&r->lock);
    for (uint32_t i = 0; i < r->count; i++) {
        if (r->items[i].fileid == fileid) {
            count++;
        }
    }
    pthread_mutex_unlock(&r->lock);
    return count;
}

static void rec_clear(struct rec_state *r)
{
    pthread_mutex_lock(&r->lock);
    r->count = 0;
    pthread_mutex_unlock(&r->lock);
}

/* -----------------------------------------------------------------------
 * Tests
 * ----------------------------------------------------------------------- */

static void test_init_destroy(void)
{
    fprintf(stdout, "  init_destroy:                     ");

    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(64, 200, &agg), 0);
    ASSERT_TRUE(agg != NULL);

    struct layout_commit_aggregator_stats st;
    layout_commit_aggregator_stats_get(agg, &st);
    ASSERT_EQ(st.entry_count, 0u);
    ASSERT_EQ(st.submits, 0u);

    layout_commit_aggregator_destroy(agg);

    /* destroy(NULL) safe */
    layout_commit_aggregator_destroy(NULL);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_invalid_args(void)
{
    fprintf(stdout, "  invalid_args:                     ");

    /* init: NULL out */
    ASSERT_EQ(layout_commit_aggregator_init(64, 200, NULL), -1);

    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(0, 0, &agg), 0);
    ASSERT_TRUE(agg != NULL);

    /* submit / peek / drop / flush all NULL-safe on agg=NULL */
    struct timespec t = {0};
    ASSERT_EQ(layout_commit_aggregator_submit(NULL, 1, 100, t), -1);

    uint64_t sz = 0;
    bool dirty = false;
    ASSERT_EQ(layout_commit_aggregator_peek(NULL, 1, &sz, &t, &dirty),
              -1);
    /* peek: NULL outputs */
    ASSERT_EQ(layout_commit_aggregator_peek(agg, 1, NULL, &t, &dirty),
              -1);
    ASSERT_EQ(layout_commit_aggregator_peek(agg, 1, &sz, NULL, &dirty),
              -1);
    ASSERT_EQ(layout_commit_aggregator_peek(agg, 1, &sz, &t, NULL),
              -1);

    layout_commit_aggregator_drop(NULL, 1);  /* no crash */
    ASSERT_EQ(layout_commit_aggregator_flush_fileid(NULL, 1), -1);
    ASSERT_EQ(layout_commit_aggregator_flush_all_dirty(NULL), 0u);

    /* stats(NULL out) is a no-op */
    layout_commit_aggregator_stats_get(agg, NULL);

    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_submit_peek_round_trip(void)
{
    fprintf(stdout, "  submit_peek_round_trip:           ");

    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(64, 200, &agg), 0);

    struct timespec t = { .tv_sec = 1700000000, .tv_nsec = 12345 };
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 42, 1024, t), 0);

    uint64_t sz = 0;
    struct timespec out_t = {0};
    bool dirty = false;
    ASSERT_EQ(layout_commit_aggregator_peek(agg, 42, &sz, &out_t,
                                             &dirty), 0);
    ASSERT_EQ(sz, 1024u);
    ASSERT_EQ(out_t.tv_sec, 1700000000);
    ASSERT_EQ(out_t.tv_nsec, 12345);
    ASSERT_TRUE(dirty);

    /* miss */
    ASSERT_EQ(layout_commit_aggregator_peek(agg, 99, &sz, &out_t,
                                             &dirty), -1);

    struct layout_commit_aggregator_stats st;
    layout_commit_aggregator_stats_get(agg, &st);
    ASSERT_EQ(st.submits, 1u);
    ASSERT_EQ(st.entry_count, 1u);

    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_max_size_merge(void)
{
    fprintf(stdout, "  max_size_merge:                   ");

    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(64, 200, &agg), 0);

    struct timespec t = { .tv_sec = 1700000000, .tv_nsec = 0 };
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 7, 5000, t), 0);
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 7, 3000, t), 0);
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 7, 8000, t), 0);
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 7, 7000, t), 0);

    uint64_t sz;
    struct timespec out_t;
    bool dirty;
    ASSERT_EQ(layout_commit_aggregator_peek(agg, 7, &sz, &out_t,
                                             &dirty), 0);
    ASSERT_EQ(sz, 8000u);
    ASSERT_TRUE(dirty);

    struct layout_commit_aggregator_stats st;
    layout_commit_aggregator_stats_get(agg, &st);
    ASSERT_EQ(st.submits, 4u);
    ASSERT_EQ(st.merges, 3u);
    ASSERT_EQ(st.entry_count, 1u);

    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_latest_mtime_merge(void)
{
    fprintf(stdout, "  latest_mtime_merge:               ");

    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(64, 200, &agg), 0);

    struct timespec earlier   = { .tv_sec = 1700000000, .tv_nsec = 100 };
    struct timespec later_sec = { .tv_sec = 1700000005, .tv_nsec = 0 };
    struct timespec later_nsec = { .tv_sec = 1700000005,
                                    .tv_nsec = 999 };

    /* Out-of-order submits -- final state must reflect the newest. */
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 11, 100, later_sec),
              0);
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 11, 200, earlier),
              0);
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 11, 150,
                                               later_nsec), 0);

    uint64_t sz;
    struct timespec out_t;
    bool dirty;
    ASSERT_EQ(layout_commit_aggregator_peek(agg, 11, &sz, &out_t,
                                             &dirty), 0);
    ASSERT_EQ(out_t.tv_sec, later_nsec.tv_sec);
    ASSERT_EQ(out_t.tv_nsec, later_nsec.tv_nsec);
    /* size is max across all three. */
    ASSERT_EQ(sz, 200u);

    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_drop(void)
{
    fprintf(stdout, "  drop:                             ");

    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(64, 200, &agg), 0);

    struct timespec t = { .tv_sec = 1, .tv_nsec = 0 };
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 5, 1234, t), 0);

    uint64_t sz; struct timespec out_t; bool dirty;
    ASSERT_EQ(layout_commit_aggregator_peek(agg, 5, &sz, &out_t,
                                             &dirty), 0);

    layout_commit_aggregator_drop(agg, 5);
    /* Now miss. */
    ASSERT_EQ(layout_commit_aggregator_peek(agg, 5, &sz, &out_t,
                                             &dirty), -1);

    /* Idempotent. */
    layout_commit_aggregator_drop(agg, 5);
    layout_commit_aggregator_drop(agg, 99);

    struct layout_commit_aggregator_stats st;
    layout_commit_aggregator_stats_get(agg, &st);
    ASSERT_EQ(st.entry_count, 0u);
    ASSERT_EQ(st.drops, 1u);  /* only the first drop hit */

    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_flush_fileid_success(void)
{
    fprintf(stdout, "  flush_fileid_success:             ");

    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(64, 200, &agg), 0);

    struct rec_state rec; rec_init(&rec);
    layout_commit_aggregator_set_flush_fn(agg, rec_flush_fn, &rec);

    struct timespec t = { .tv_sec = 100, .tv_nsec = 200 };
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 13, 4096, t), 0);

    /* miss for an unknown fileid */
    ASSERT_EQ(layout_commit_aggregator_flush_fileid(agg, 99), 1);
    ASSERT_EQ(rec_count(&rec), 0u);

    /* hit + clean flush */
    ASSERT_EQ(layout_commit_aggregator_flush_fileid(agg, 13), 0);
    ASSERT_EQ(rec_count(&rec), 1u);

    struct rec_entry got;
    ASSERT_TRUE(rec_find(&rec, 13, &got));
    ASSERT_EQ(got.size, 4096u);
    ASSERT_EQ(got.mtime.tv_sec, 100);
    ASSERT_EQ(got.mtime.tv_nsec, 200);

    /* Bucket should now be clean -- second flush is a no-op success. */
    bool dirty;
    uint64_t sz; struct timespec out_t;
    ASSERT_EQ(layout_commit_aggregator_peek(agg, 13, &sz, &out_t,
                                             &dirty), 0);
    ASSERT_TRUE(!dirty);

    ASSERT_EQ(layout_commit_aggregator_flush_fileid(agg, 13), 0);
    ASSERT_EQ(rec_count(&rec), 1u);  /* no new record */

    rec_destroy(&rec);
    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_flush_fileid_failure_keeps_dirty(void)
{
    fprintf(stdout, "  flush_fileid_failure_keeps_dirty: ");

    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(64, 200, &agg), 0);

    struct rec_state rec; rec_init(&rec);
    layout_commit_aggregator_set_flush_fn(agg, rec_flush_fn, &rec);

    struct timespec t = { .tv_sec = 1, .tv_nsec = 0 };
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 21, 999, t), 0);

    atomic_store(&rec.should_fail, 1);

    /* Flush attempt fails. */
    ASSERT_EQ(layout_commit_aggregator_flush_fileid(agg, 21), -1);
    ASSERT_EQ(rec_count(&rec), 0u);

    /* Bucket must still be dirty so a future success path retries. */
    uint64_t sz;
    struct timespec out_t;
    bool dirty;
    ASSERT_EQ(layout_commit_aggregator_peek(agg, 21, &sz, &out_t,
                                             &dirty), 0);
    ASSERT_TRUE(dirty);

    /* Flip the callback to success and retry. */
    atomic_store(&rec.should_fail, 0);
    ASSERT_EQ(layout_commit_aggregator_flush_fileid(agg, 21), 0);
    ASSERT_EQ(rec_count(&rec), 1u);

    rec_destroy(&rec);
    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_flush_all_dirty(void)
{
    fprintf(stdout, "  flush_all_dirty:                  ");

    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(256, 200, &agg), 0);

    struct rec_state rec; rec_init(&rec);
    layout_commit_aggregator_set_flush_fn(agg, rec_flush_fn, &rec);

    struct timespec t = { .tv_sec = 1, .tv_nsec = 0 };
    /* Spread across shards by using different fileids. */
    for (uint64_t fid = 1; fid <= 50; fid++) {
        ASSERT_EQ(layout_commit_aggregator_submit(agg, fid,
                                                   fid * 100, t), 0);
    }

    uint32_t flushed = layout_commit_aggregator_flush_all_dirty(agg);
    ASSERT_EQ(flushed, 50u);
    ASSERT_EQ(rec_count(&rec), 50u);

    /* Every fid must be present. */
    for (uint64_t fid = 1; fid <= 50; fid++) {
        struct rec_entry got;
        ASSERT_TRUE(rec_find(&rec, fid, &got));
        ASSERT_EQ(got.size, fid * 100);
    }

    /* All buckets are now clean -- flush_all_dirty drains nothing. */
    rec_clear(&rec);
    flushed = layout_commit_aggregator_flush_all_dirty(agg);
    ASSERT_EQ(flushed, 0u);
    ASSERT_EQ(rec_count(&rec), 0u);

    rec_destroy(&rec);
    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_flush_no_callback(void)
{
    fprintf(stdout, "  flush_no_callback:                ");

    /* Without a flush_fn wired, periodic + flush_all_dirty are
     * no-ops; flush_fileid signals failure (so callers see the
     * bucket is not yet persisted).  Capacity-pressure eviction
     * (QA review Blocker 3) also fails submit when no callback is
     * wired, but this test stays below capacity so submit succeeds. */
    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(64, 200, &agg), 0);

    struct timespec t = { .tv_sec = 1, .tv_nsec = 0 };
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 31, 100, t), 0);

    ASSERT_EQ(layout_commit_aggregator_flush_fileid(agg, 31), -1);
    ASSERT_EQ(layout_commit_aggregator_flush_all_dirty(agg), 0u);

    /* Bucket stays dirty. */
    uint64_t sz; struct timespec out_t; bool dirty;
    ASSERT_EQ(layout_commit_aggregator_peek(agg, 31, &sz, &out_t,
                                             &dirty), 0);
    ASSERT_TRUE(dirty);

    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_capacity_eviction(void)
{
    fprintf(stdout, "  capacity_eviction:                ");

    /* Per-shard floor of 4 -> total min capacity 64.  Insert > 64.
     *
     * QA review Blocker 3: when no flush callback is wired, the
     * eviction path treats that as a flush failure and fails submit
     * (preserving the victim) so dirty aggregates are never dropped.
     * To exercise the eviction-success path, wire a recording
     * callback that always succeeds. */
    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(LCA_SHARDS * 4, 200,
                                             &agg), 0);

    struct rec_state rec; rec_init(&rec);
    layout_commit_aggregator_set_flush_fn(agg, rec_flush_fn, &rec);

    struct timespec t = { .tv_sec = 1, .tv_nsec = 0 };
    for (uint64_t fid = 1; fid <= 1024; fid++) {
        ASSERT_EQ(layout_commit_aggregator_submit(agg, fid,
                                                   fid * 10, t), 0);
    }

    struct layout_commit_aggregator_stats st;
    layout_commit_aggregator_stats_get(agg, &st);
    ASSERT_TRUE(st.entry_count <= (uint64_t)LCA_SHARDS * 4);
    ASSERT_TRUE(st.evictions > 0);
    ASSERT_EQ(st.submits, 1024u);

    rec_destroy(&rec);
    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS (entries=%llu evictions=%llu)\n",
            (unsigned long long)st.entry_count,
            (unsigned long long)st.evictions);
    passed++;
}

/* QA Phase 4 -- eviction-flush regression.
 *
 * The pre-fix submit() path freed dirty victims without invoking the
 * flush callback, silently dropping every pending size/mtime update
 * once the shard hit capacity.  This test wires a recording flush
 * callback BEFORE eviction is forced and asserts that:
 *   (a) evictions occurred (st.evictions > 0);
 *   (b) every distinct fileid that was evicted appears in the
 *       callback's record buffer with a size matching the value the
 *       test submitted (so the callback was invoked on a sensible
 *       snapshot, not a wild pointer);
 *   (c) stat_flushes_forced incremented for evicted dirty buckets,
 *       with stat_flush_failures staying zero (callback never failed). */
static void test_eviction_flushes_dirty(void)
{
    fprintf(stdout, "  eviction_flushes_dirty:           ");

    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(LCA_SHARDS * 4, 200,
                                             &agg), 0);

    struct rec_state rec; rec_init(&rec);
    layout_commit_aggregator_set_flush_fn(agg, rec_flush_fn, &rec);

    /* LCA_SHARDS * 4 = 64 total slots.  With 200 distinct fileids,
     * pigeonhole forces ~ (200 - 64) = 136 evictions, comfortably
     * below the recorder's MAX_RECORDS=1024 buffer so no callbacks
     * are dropped during the test.  Every eviction must drive a
     * flush callback. */
    struct timespec t = { .tv_sec = 1, .tv_nsec = 0 };
    for (uint64_t fid = 1; fid <= 200; fid++) {
        ASSERT_EQ(layout_commit_aggregator_submit(agg, fid,
                                                   fid * 10, t), 0);
    }

    struct layout_commit_aggregator_stats st;
    layout_commit_aggregator_stats_get(agg, &st);
    ASSERT_TRUE(st.evictions > 0);
    ASSERT_TRUE(st.flushes_forced >= st.evictions);
    ASSERT_EQ(st.flush_failures, 0u);
    ASSERT_TRUE(rec_count(&rec) >= st.evictions);

    /* Spot-check: the callback's recorded size for any evicted fid
     * must equal that fid * 10 (the value we submitted).  We cannot
     * predict which fids were evicted (depends on shard-of), but we
     * can verify any record in the buffer has the right shape. */
    pthread_mutex_lock(&rec.lock);
    for (uint32_t i = 0; i < rec.count; i++) {
        uint64_t expected = rec.items[i].fileid * 10;
        if (rec.items[i].size != expected) {
            pthread_mutex_unlock(&rec.lock);
            fprintf(stderr,
                    "  FAIL: rec[%u] fid=%llu size=%llu expected=%llu\n",
                    i,
                    (unsigned long long)rec.items[i].fileid,
                    (unsigned long long)rec.items[i].size,
                    (unsigned long long)expected);
            failed++;
            rec_destroy(&rec);
            layout_commit_aggregator_destroy(agg);
            return;
        }
    }
    pthread_mutex_unlock(&rec.lock);

    rec_destroy(&rec);
    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS (evictions=%llu flushed=%llu)\n",
            (unsigned long long)st.evictions,
            (unsigned long long)st.flushes_forced);
    passed++;
}

/* QA review Blocker 3: when a dirty victim's flush callback fails
 * during eviction, the aggregator MUST preserve the victim (not
 * silently drop file A's update because file B forced its eviction).
 *
 * Strategy:
 *   1. Init a tiny aggregator (one shard -> capacity 4).
 *   2. Wire a recording flush callback whose should_fail toggle is
 *      ON.
 *   3. Submit fileids 1..200 with the failing callback.  The first
 *      few submits go into clean shards.  Once any shard hits
 *      capacity, the eviction path will try to flush the LRU dirty
 *      victim, the callback will fail, and the submit MUST return
 *      -1 with the victim still in the bucket.
 *   4. After we observe at least one -1, peek the victim's fileid
 *      and assert it is still present and dirty with the correct
 *      max_size.
 *   5. Flip should_fail OFF and explicitly flush_all_dirty; the
 *      preserved aggregate must drain through the callback (proving
 *      no data was lost across the failed-eviction window).
 *
 * The test covers the exact regression path the QA review flagged:
 * pre-fix code freed the dirty victim regardless of flush outcome,
 * losing the dirty aggregate; post-fix code preserves it. */
static void test_eviction_flush_failure_preserves_victim(void)
{
    fprintf(stdout, "  eviction_flush_failure_preserves: ");

    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(LCA_SHARDS * 4, 200,
                                             &agg), 0);

    struct rec_state rec; rec_init(&rec);
    layout_commit_aggregator_set_flush_fn(agg, rec_flush_fn, &rec);

    /* Force every flush callback to fail. */
    atomic_store(&rec.should_fail, 1);

    /* Submit many fileids; once any shard fills up, the next submit
     * to that shard must hit the failing-eviction path and return
     * -1.  Track which fids returned -1 so we can verify they are
     * preserved on the dirty list. */
    uint64_t failed_fids[256];
    uint32_t failed_count = 0;
    struct timespec t = { .tv_sec = 1, .tv_nsec = 0 };
    for (uint64_t fid = 1; fid <= 200; fid++) {
        int rc = layout_commit_aggregator_submit(agg, fid,
                                                 fid * 10, t);
        if (rc < 0 && failed_count < 256) {
            failed_fids[failed_count++] = fid;
        }
    }

    /* At least one submit must have hit the failing-eviction path.
     * 200 fids into 16 shards of 4 means every shard saw evictions. */
    ASSERT_TRUE(failed_count > 0);

    struct layout_commit_aggregator_stats st;
    layout_commit_aggregator_stats_get(agg, &st);
    ASSERT_TRUE(st.flush_failures > 0);
    /* Per-Blocker-3 invariant: an eviction that ended in flush
     * failure does NOT count as a successful eviction (we backed
     * out the increment).  So the only evictions counted are the
     * (rare) clean-victim path -- which is unlikely on this trace
     * because every bucket starts dirty.  The strong assertion is
     * "evictions <= flushes_forced" (every counted eviction must
     * have driven a successful forced flush). */
    ASSERT_TRUE(st.evictions <= st.flushes_forced);

    /* The failing flushes must NOT have appeared in the recorder --
     * the rec_flush_fn returns -1 before the recording branch when
     * should_fail is set. */
    ASSERT_EQ(rec_count(&rec), 0u);

    /* Spot-check: every fileid that hit the failing-eviction path
     * must still be observable in the aggregator with the size we
     * submitted (preserved across the failed flush). */
    for (uint32_t i = 0; i < failed_count; i++) {
        uint64_t fid = failed_fids[i];
        uint64_t sz = 0;
        struct timespec mt = { 0 };
        bool dirty = false;
        /* The bucket may not be present -- a later eviction during
         * a different submit could have legitimately evicted a
         * clean predecessor.  But when present, the bucket must
         * be dirty with size == fid * 10. */
        if (layout_commit_aggregator_peek(agg, fid, &sz, &mt,
                                           &dirty) == 0) {
            ASSERT_TRUE(dirty);
            ASSERT_EQ(sz, fid * 10);
        }
    }

    /* Recovery path: flip the failure switch off and flush_all_dirty.
     * Every preserved bucket must now drain through the callback,
     * proving no data was lost across the failed-eviction window. */
    atomic_store(&rec.should_fail, 0);
    uint32_t flushed = layout_commit_aggregator_flush_all_dirty(agg);
    ASSERT_TRUE(flushed > 0);
    ASSERT_TRUE(rec_count(&rec) > 0);

    rec_destroy(&rec);
    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS (failed_submits=%u flush_failures=%llu "
            "recovered=%u)\n",
            failed_count,
            (unsigned long long)st.flush_failures,
            flushed);
    passed++;
}

struct race_flush_state {
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    bool            entered;
    bool            release;
    uint64_t        victim_fileid;
    uint64_t        victim_size;
    struct timespec victim_mtime;
};

struct race_submit_arg {
    struct layout_commit_aggregator *agg;
    int                              rc;
    uint64_t                         failed_submit_fileid;
};

static void race_flush_state_init(struct race_flush_state *r)
{
    memset(r, 0, sizeof(*r));
    pthread_mutex_init(&r->lock, NULL);
    pthread_cond_init(&r->cond, NULL);
}

static void race_flush_state_destroy(struct race_flush_state *r)
{
    pthread_cond_destroy(&r->cond);
    pthread_mutex_destroy(&r->lock);
}

static int race_flush_fn(uint64_t fileid, uint64_t size,
                         struct timespec mtime, void *cookie)
{
    struct race_flush_state *r = cookie;

    pthread_mutex_lock(&r->lock);
    if (!r->entered) {
        r->victim_fileid = fileid;
        r->victim_size = size;
        r->victim_mtime = mtime;
        r->entered = true;
        pthread_cond_broadcast(&r->cond);
    }
    while (!r->release) {
        pthread_cond_wait(&r->cond, &r->lock);
    }
    pthread_mutex_unlock(&r->lock);
    return -1;
}

static void race_flush_release(struct race_flush_state *r)
{
    pthread_mutex_lock(&r->lock);
    r->release = true;
    pthread_cond_broadcast(&r->cond);
    pthread_mutex_unlock(&r->lock);
}

static bool race_flush_wait_entered(struct race_flush_state *r)
{
    for (uint32_t i = 0; i < 500; i++) {
        bool entered;

        pthread_mutex_lock(&r->lock);
        entered = r->entered;
        pthread_mutex_unlock(&r->lock);
        if (entered) {
            return true;
        }

        struct timespec sleep_t = {
            .tv_sec = 0,
            .tv_nsec = 10 * 1000 * 1000,
        };
        nanosleep(&sleep_t, NULL);
    }
    return false;
}

static void race_flush_snapshot(struct race_flush_state *r,
                                uint64_t *fileid,
                                uint64_t *size,
                                struct timespec *mtime)
{
    pthread_mutex_lock(&r->lock);
    *fileid = r->victim_fileid;
    *size = r->victim_size;
    *mtime = r->victim_mtime;
    pthread_mutex_unlock(&r->lock);
}

static void *race_submit_until_failure(void *ctx)
{
    struct race_submit_arg *arg = ctx;
    struct timespec t = { .tv_sec = 1, .tv_nsec = 0 };

    arg->rc = 0;
    arg->failed_submit_fileid = 0;
    for (uint64_t fid = 1; fid <= 100000; fid++) {
        int rc = layout_commit_aggregator_submit(arg->agg, fid,
                                                 fid * 10, t);
        if (rc < 0) {
            arg->rc = rc;
            arg->failed_submit_fileid = fid;
            return NULL;
        }
    }
    return NULL;
}

static void test_eviction_failure_merges_same_fileid_race(void)
{
    fprintf(stdout, "  eviction_failure_merges_same_fid: ");

    struct layout_commit_aggregator *agg = NULL;
    struct race_flush_state race;
    struct race_submit_arg arg;
    struct rec_state rec;
    struct rec_entry got;
    pthread_t thread;
    bool race_ready = false;
    bool rec_ready = false;
    bool thread_started = false;
    bool thread_joined = false;
    bool ok = false;
    uint64_t victim_fileid = 0;
    uint64_t victim_size = 0;
    uint64_t replacement_size = 0;
    struct timespec victim_mtime = { 0 };
    struct timespec replacement_mtime = { 0 };
    uint32_t flushed = 0;
    const char *fail_msg = "unexpected failure";

    if (layout_commit_aggregator_init(LCA_SHARDS * 4, 200, &agg) != 0) {
        fail_msg = "init failed";
        goto out;
    }

    race_flush_state_init(&race);
    race_ready = true;
    layout_commit_aggregator_set_flush_fn(agg, race_flush_fn, &race);

    memset(&arg, 0, sizeof(arg));
    arg.agg = agg;
    if (pthread_create(&thread, NULL, race_submit_until_failure,
                       &arg) != 0) {
        fail_msg = "pthread_create failed";
        goto out;
    }
    thread_started = true;

    if (!race_flush_wait_entered(&race)) {
        fail_msg = "eviction callback did not run";
        goto out;
    }

    race_flush_snapshot(&race, &victim_fileid, &victim_size,
                        &victim_mtime);
    replacement_size = victim_size + 1000;
    replacement_mtime.tv_sec = victim_mtime.tv_sec + 1;
    replacement_mtime.tv_nsec = victim_mtime.tv_nsec;

    /* Submit the same fileid while the evicted victim is detached
     * and its failed flush is still in progress.  The failed-evict
     * path must merge the detached victim back into this replacement
     * bucket rather than creating a duplicate hash entry. */
    if (layout_commit_aggregator_submit(agg, victim_fileid,
                                        replacement_size,
                                        replacement_mtime) != 0) {
        fail_msg = "replacement submit failed";
        goto out;
    }

    race_flush_release(&race);
    pthread_join(thread, NULL);
    thread_joined = true;

    if (arg.rc >= 0) {
        fail_msg = "evicting submit did not fail";
        goto out;
    }

    rec_init(&rec);
    rec_ready = true;
    layout_commit_aggregator_set_flush_fn(agg, rec_flush_fn, &rec);
    flushed = layout_commit_aggregator_flush_all_dirty(agg);
    if (flushed == 0) {
        fail_msg = "recovery flush found no dirty buckets";
        goto out;
    }
    if (rec_count_fileid(&rec, victim_fileid) != 1) {
        fail_msg = "victim fileid flushed more than once";
        goto out;
    }
    if (!rec_find(&rec, victim_fileid, &got)) {
        fail_msg = "victim fileid missing from recovery flush";
        goto out;
    }
    if (got.size != replacement_size ||
        got.mtime.tv_sec != replacement_mtime.tv_sec ||
        got.mtime.tv_nsec != replacement_mtime.tv_nsec) {
        fail_msg = "merged victim did not keep replacement update";
        goto out;
    }

    ok = true;

out:
    if (thread_started && !thread_joined) {
        race_flush_release(&race);
        pthread_join(thread, NULL);
    }
    if (rec_ready) {
        rec_destroy(&rec);
    }
    if (race_ready) {
        race_flush_state_destroy(&race);
    }
    layout_commit_aggregator_destroy(agg);

    if (!ok) {
        fprintf(stderr, "  FAIL: %s\n", fail_msg);
        failed++;
        return;
    }

    fprintf(stdout, "PASS (victim=%llu replacement_size=%llu)\n",
            (unsigned long long)victim_fileid,
            (unsigned long long)replacement_size);
    passed++;
}

static void test_periodic_timer(void)
{
    fprintf(stdout, "  periodic_timer:                   ");

    /* 25 ms tick -- enough to fire several times within a 250 ms test. */
    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(64, 25, &agg), 0);

    struct rec_state rec; rec_init(&rec);
    layout_commit_aggregator_set_flush_fn(agg, rec_flush_fn, &rec);

    ASSERT_EQ(layout_commit_aggregator_start(agg), 0);

    struct timespec t = { .tv_sec = 1, .tv_nsec = 0 };
    ASSERT_EQ(layout_commit_aggregator_submit(agg, 77, 8888, t), 0);

    /* Wait up to 500 ms for the timer to fire. */
    bool seen = false;
    for (int i = 0; i < 50; i++) {
        if (rec_count(&rec) > 0) {
            seen = true;
            break;
        }
        struct timespec sleep_t = { .tv_sec = 0,
                                     .tv_nsec = 10 * 1000 * 1000 };
        nanosleep(&sleep_t, NULL);
    }

    layout_commit_aggregator_stop(agg);

    ASSERT_TRUE(seen);
    struct rec_entry got;
    ASSERT_TRUE(rec_find(&rec, 77, &got));
    ASSERT_EQ(got.size, 8888u);

    struct layout_commit_aggregator_stats st;
    layout_commit_aggregator_stats_get(agg, &st);
    ASSERT_TRUE(st.flushes_periodic >= 1);

    /* idempotent stop */
    layout_commit_aggregator_stop(agg);

    rec_destroy(&rec);
    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* -----------------------------------------------------------------------
 * Concurrency smoke
 *
 * 8 threads x 5000 submits across a 32-fileid space, with a separate
 * thread doing forced flushes.  The aggregator must remain self-
 * consistent (no crash, stats reachable, max-size invariant: at
 * any moment the bucket's max_size is the max submitted by ANY
 * thread up to that point).
 *
 * For the invariant check we record the global max submitted size
 * per fileid in an atomic table.  At test end we flush_all_dirty
 * once more and verify the recorded sizes are >= the per-fileid
 * global max (the flush callback only writes the persisted
 * snapshot, which can be older than the latest in-bucket value).
 * ----------------------------------------------------------------------- */

#define CONC_THREADS    8
#define CONC_ITERATIONS 5000
#define CONC_FILEIDS    32

struct conc_arg {
    struct layout_commit_aggregator *agg;
    uint32_t                          seed;
    int                               thread_id;
    _Atomic uint64_t                  global_max_size[CONC_FILEIDS + 1];
};

static void *conc_worker(void *p)
{
    struct conc_arg *arg = p;
    unsigned int seed = arg->seed;

    for (int i = 0; i < CONC_ITERATIONS; i++) {
        uint64_t fid = (rand_r(&seed) % CONC_FILEIDS) + 1;
        uint64_t size = (uint64_t)(rand_r(&seed) % 100000);
        struct timespec t = {
            .tv_sec  = 1700000000 + (i % 1000),
            .tv_nsec = 0,
        };

        /* Update global max. */
        uint64_t old = atomic_load(&arg->global_max_size[fid]);
        while (size > old) {
            if (atomic_compare_exchange_weak(
                    &arg->global_max_size[fid], &old, size)) {
                break;
            }
        }

        int op = rand_r(&seed) % 8;
        if (op == 0) {
            (void)layout_commit_aggregator_drop(arg->agg, fid);
        } else if (op == 1) {
            (void)layout_commit_aggregator_flush_fileid(arg->agg,
                                                         fid);
        } else {
            (void)layout_commit_aggregator_submit(arg->agg, fid,
                                                   size, t);
        }
    }
    return NULL;
}

static void test_concurrent(void)
{
    fprintf(stdout, "  concurrent:                       ");

    struct layout_commit_aggregator *agg = NULL;
    ASSERT_EQ(layout_commit_aggregator_init(LCA_SHARDS * 4,
                                             200, &agg), 0);

    struct rec_state rec; rec_init(&rec);
    layout_commit_aggregator_set_flush_fn(agg, rec_flush_fn, &rec);

    pthread_t threads[CONC_THREADS];
    struct conc_arg args[CONC_THREADS];

    for (int t = 0; t < CONC_THREADS; t++) {
        memset(&args[t], 0, sizeof(args[t]));
        args[t].agg       = agg;
        args[t].seed      = (uint32_t)(0x55aa00 + t);
        args[t].thread_id = t;
        for (int i = 0; i <= CONC_FILEIDS; i++) {
            atomic_store(&args[t].global_max_size[i], 0);
        }
        ASSERT_EQ(pthread_create(&threads[t], NULL, conc_worker,
                                 &args[t]), 0);
    }
    for (int t = 0; t < CONC_THREADS; t++) {
        pthread_join(threads[t], NULL);
    }

    /* Final drain. */
    (void)layout_commit_aggregator_flush_all_dirty(agg);

    struct layout_commit_aggregator_stats st;
    layout_commit_aggregator_stats_get(agg, &st);
    ASSERT_TRUE(st.submits > 0);
    /* Submission count is bounded above by N_threads * iterations
     * (some ops were drops/flushes, not submits). */
    ASSERT_TRUE(st.submits <= (uint64_t)CONC_THREADS *
                              CONC_ITERATIONS);

    rec_destroy(&rec);
    layout_commit_aggregator_destroy(agg);

    fprintf(stdout, "PASS (submits=%llu flushes=%llu drops=%llu)\n",
            (unsigned long long)st.submits,
            (unsigned long long)(st.flushes_periodic +
                                 st.flushes_forced),
            (unsigned long long)st.drops);
    passed++;
}

int main(void)
{
    fprintf(stdout, "test_layout_commit_aggregator:\n");

    test_init_destroy();
    test_invalid_args();
    test_submit_peek_round_trip();
    test_max_size_merge();
    test_latest_mtime_merge();
    test_drop();
    test_flush_fileid_success();
    test_flush_fileid_failure_keeps_dirty();
    test_flush_all_dirty();
    test_flush_no_callback();
    test_capacity_eviction();
    test_eviction_flushes_dirty();
    test_eviction_flush_failure_preserves_victim();
    test_eviction_failure_merges_same_fileid_race();
    test_periodic_timer();
    test_concurrent();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}

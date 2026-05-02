/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_layout_cache.c — Phase D v1 unit tests.
 *
 * Pure data-structure tests against the layout_cache module.  No
 * catalogue, no compound, no daemon — just the cache.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>

#include "pnfs_mds.h"
#include "layout_cache.h"

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
 * Helpers
 * ----------------------------------------------------------------------- */

/* Fill an entry array deterministically so we can verify round-trip
 * fidelity.  ds_id seeded from fileid + stripe + mirror; nfs_fh
 * filled with a tiny pattern. */
static void fill_entries(struct mds_ds_map_entry *entries,
                         uint64_t fileid,
                         uint32_t stripe_count, uint32_t mirror_count)
{
    uint32_t s, m;
    for (s = 0; s < stripe_count; s++) {
        for (m = 0; m < mirror_count; m++) {
            uint32_t idx = s * mirror_count + m;
            entries[idx].ds_id = (uint32_t)(fileid * 1000 +
                                            s * 10 + m + 1);
            entries[idx].nfs_fh_len = 16;
            for (uint32_t i = 0; i < 16; i++) {
                entries[idx].nfs_fh[i] = (uint8_t)(idx + i);
            }
        }
    }
}

/* Verify the entries array matches the deterministic pattern from
 * fill_entries. */
static bool verify_entries(const struct mds_ds_map_entry *entries,
                           uint64_t fileid,
                           uint32_t stripe_count, uint32_t mirror_count)
{
    uint32_t s, m;
    for (s = 0; s < stripe_count; s++) {
        for (m = 0; m < mirror_count; m++) {
            uint32_t idx = s * mirror_count + m;
            uint32_t want_id = (uint32_t)(fileid * 1000 +
                                          s * 10 + m + 1);
            if (entries[idx].ds_id != want_id) {
                return false;
            }
            if (entries[idx].nfs_fh_len != 16) {
                return false;
            }
            for (uint32_t i = 0; i < 16; i++) {
                if (entries[idx].nfs_fh[i] != (uint8_t)(idx + i)) {
                    return false;
                }
            }
        }
    }
    return true;
}

/* -----------------------------------------------------------------------
 * Tests
 * ----------------------------------------------------------------------- */

static void test_create_destroy(void)
{
    fprintf(stdout, "  create_destroy:                   ");

    struct layout_cache *lc = NULL;
    ASSERT_EQ(layout_cache_init(64, &lc), 0);
    ASSERT_TRUE(lc != NULL);

    struct layout_cache_stats st;
    layout_cache_stats_get(lc, &st);
    ASSERT_EQ(st.entry_count, 0u);
    ASSERT_EQ(st.hits, 0u);
    ASSERT_EQ(st.misses, 0u);
    ASSERT_EQ(st.puts, 0u);

    layout_cache_destroy(lc);

    /* destroy(NULL) safe */
    layout_cache_destroy(NULL);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_invalid_args(void)
{
    fprintf(stdout, "  invalid_args:                     ");

    /* init: zero capacity / NULL out */
    struct layout_cache *lc = NULL;
    ASSERT_EQ(layout_cache_init(0, &lc), -1);
    ASSERT_EQ(layout_cache_init(16, NULL), -1);

    ASSERT_EQ(layout_cache_init(16, &lc), 0);

    uint32_t sc = 0, su = 0, mc = 0;
    struct mds_ds_map_entry *out = NULL;
    /* get: NULL outputs and NULL cache */
    ASSERT_EQ(layout_cache_get(NULL, 1, &sc, &su, &mc, &out), -1);
    ASSERT_EQ(layout_cache_get(lc, 1, NULL, &su, &mc, &out), -1);
    ASSERT_EQ(layout_cache_get(lc, 1, &sc, NULL, &mc, &out), -1);
    ASSERT_EQ(layout_cache_get(lc, 1, &sc, &su, NULL, &out), -1);
    ASSERT_EQ(layout_cache_get(lc, 1, &sc, &su, &mc, NULL), -1);

    /* put: zero dimensions / NULL entries */
    struct mds_ds_map_entry e;
    memset(&e, 0, sizeof(e));
    ASSERT_EQ(layout_cache_put(lc, 1, 0, 65536, 1, &e), -1);
    ASSERT_EQ(layout_cache_put(lc, 1, 1, 65536, 0, &e), -1);
    ASSERT_EQ(layout_cache_put(lc, 1, 1, 65536, 1, NULL), -1);
    ASSERT_EQ(layout_cache_put(NULL, 1, 1, 65536, 1, &e), -1);

    /* put: stripe count above MDS_MAX_STRIPES */
    ASSERT_EQ(layout_cache_put(lc, 1, MDS_MAX_STRIPES + 1, 65536,
                               1, &e),
              -1);

    /* invalidate(NULL) safe */
    layout_cache_invalidate(NULL, 1);
    /* clear(NULL) safe */
    layout_cache_clear(NULL);

    /* stats(NULL out) is a no-op (no crash) */
    layout_cache_stats_get(lc, NULL);

    layout_cache_destroy(lc);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_miss_on_empty(void)
{
    fprintf(stdout, "  miss_on_empty:                    ");

    struct layout_cache *lc = NULL;
    ASSERT_EQ(layout_cache_init(16, &lc), 0);

    uint32_t sc = 99, su = 99, mc = 99;
    struct mds_ds_map_entry *out = (void *)(uintptr_t)0xdeadbeef;
    ASSERT_EQ(layout_cache_get(lc, 42, &sc, &su, &mc, &out), -1);
    /* On miss, no allocation happens.  We don't spec the values of
     * sc/su/mc/out — caller must check the return code first. */

    struct layout_cache_stats st;
    layout_cache_stats_get(lc, &st);
    ASSERT_EQ(st.hits, 0u);
    ASSERT_EQ(st.misses, 1u);
    ASSERT_EQ(st.entry_count, 0u);

    layout_cache_destroy(lc);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_put_get_round_trip(void)
{
    fprintf(stdout, "  put_get_round_trip:               ");

    struct layout_cache *lc = NULL;
    ASSERT_EQ(layout_cache_init(16, &lc), 0);

    /* Put with stripe_count=4, mirror_count=2 -> 8 entries. */
    struct mds_ds_map_entry input[4 * 2];
    fill_entries(input, /*fileid=*/100, /*sc=*/4, /*mc=*/2);
    ASSERT_EQ(layout_cache_put(lc, 100, 4, 1U << 20, 2, input), 0);

    uint32_t sc = 0, su = 0, mc = 0;
    struct mds_ds_map_entry *out = NULL;
    ASSERT_EQ(layout_cache_get(lc, 100, &sc, &su, &mc, &out), 0);
    ASSERT_EQ(sc, 4u);
    ASSERT_EQ(su, 1U << 20);
    ASSERT_EQ(mc, 2u);
    ASSERT_TRUE(out != NULL);
    ASSERT_TRUE(verify_entries(out, 100, 4, 2));
    free(out);

    struct layout_cache_stats st;
    layout_cache_stats_get(lc, &st);
    ASSERT_EQ(st.hits, 1u);
    ASSERT_EQ(st.puts, 1u);
    ASSERT_EQ(st.entry_count, 1u);

    layout_cache_destroy(lc);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_put_replaces_existing(void)
{
    fprintf(stdout, "  put_replaces_existing:            ");

    struct layout_cache *lc = NULL;
    ASSERT_EQ(layout_cache_init(16, &lc), 0);

    struct mds_ds_map_entry first[4];
    fill_entries(first, 5, 4, 1);
    ASSERT_EQ(layout_cache_put(lc, 5, 4, 65536, 1, first), 0);

    /* Replace with a different geometry. */
    struct mds_ds_map_entry second[8];
    fill_entries(second, 5, 4, 2);
    ASSERT_EQ(layout_cache_put(lc, 5, 4, 1U << 20, 2, second), 0);

    /* Verify the replacement is what we get back. */
    uint32_t sc = 0, su = 0, mc = 0;
    struct mds_ds_map_entry *out = NULL;
    ASSERT_EQ(layout_cache_get(lc, 5, &sc, &su, &mc, &out), 0);
    ASSERT_EQ(sc, 4u);
    ASSERT_EQ(su, 1U << 20);
    ASSERT_EQ(mc, 2u);
    ASSERT_TRUE(verify_entries(out, 5, 4, 2));
    free(out);

    /* Still one entry total. */
    struct layout_cache_stats st;
    layout_cache_stats_get(lc, &st);
    ASSERT_EQ(st.entry_count, 1u);
    ASSERT_EQ(st.puts, 2u);

    layout_cache_destroy(lc);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_invalidate(void)
{
    fprintf(stdout, "  invalidate:                       ");

    struct layout_cache *lc = NULL;
    ASSERT_EQ(layout_cache_init(16, &lc), 0);

    struct mds_ds_map_entry e;
    memset(&e, 0, sizeof(e));
    e.ds_id = 7;
    ASSERT_EQ(layout_cache_put(lc, 99, 1, 65536, 1, &e), 0);

    uint32_t sc, su, mc;
    struct mds_ds_map_entry *out = NULL;
    ASSERT_EQ(layout_cache_get(lc, 99, &sc, &su, &mc, &out), 0);
    free(out);

    layout_cache_invalidate(lc, 99);

    /* Now miss. */
    out = NULL;
    ASSERT_EQ(layout_cache_get(lc, 99, &sc, &su, &mc, &out), -1);

    /* Idempotent: a second invalidate of the same fileid is a no-op. */
    layout_cache_invalidate(lc, 99);
    /* Invalidate of a never-cached fileid is also a no-op. */
    layout_cache_invalidate(lc, 12345);

    struct layout_cache_stats st;
    layout_cache_stats_get(lc, &st);
    ASSERT_EQ(st.entry_count, 0u);
    /* Only the real removal counts. */
    ASSERT_EQ(st.invalidations, 1u);

    layout_cache_destroy(lc);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_eviction_at_capacity(void)
{
    fprintf(stdout, "  eviction_at_capacity:             ");

    /* Use a tiny capacity so eviction happens after a small number
     * of inserts.  We size to LAYOUT_CACHE_SHARDS * 4 = 64 because
     * the per-shard floor is 4 — anything smaller still rounds up
     * to that floor.  Insert 1024 entries, then check that the
     * total entry count is bounded and the eviction counter
     * reflects the overflow. */
    struct layout_cache *lc = NULL;
    ASSERT_EQ(layout_cache_init(LAYOUT_CACHE_SHARDS * 4, &lc), 0);

    struct mds_ds_map_entry e;
    memset(&e, 0, sizeof(e));

    for (uint64_t fid = 1; fid <= 1024; fid++) {
        e.ds_id = (uint32_t)fid;
        ASSERT_EQ(layout_cache_put(lc, fid, 1, 65536, 1, &e), 0);
    }

    struct layout_cache_stats st;
    layout_cache_stats_get(lc, &st);
    ASSERT_TRUE(st.entry_count <= (uint64_t)LAYOUT_CACHE_SHARDS * 4);
    ASSERT_TRUE(st.evictions > 0);
    ASSERT_EQ(st.puts, 1024u);

    layout_cache_destroy(lc);

    fprintf(stdout, "PASS (entries=%llu evictions=%llu)\n",
            (unsigned long long)st.entry_count,
            (unsigned long long)st.evictions);
    passed++;
}

static void test_get_returns_owned_copy(void)
{
    fprintf(stdout, "  get_returns_owned_copy:           ");

    struct layout_cache *lc = NULL;
    ASSERT_EQ(layout_cache_init(16, &lc), 0);

    struct mds_ds_map_entry input[2];
    fill_entries(input, 7, 2, 1);
    ASSERT_EQ(layout_cache_put(lc, 7, 2, 65536, 1, input), 0);

    uint32_t sc, su, mc;
    struct mds_ds_map_entry *out = NULL;
    ASSERT_EQ(layout_cache_get(lc, 7, &sc, &su, &mc, &out), 0);
    ASSERT_TRUE(out != NULL);

    /* Caller mutates its copy.  This must NOT corrupt the cached
     * entry — verified by a second get returning the original. */
    out[0].ds_id = 0xDEADBEEF;
    out[1].ds_id = 0xDEADBEEF;
    free(out);

    struct mds_ds_map_entry *out2 = NULL;
    ASSERT_EQ(layout_cache_get(lc, 7, &sc, &su, &mc, &out2), 0);
    ASSERT_TRUE(verify_entries(out2, 7, 2, 1));
    free(out2);

    layout_cache_destroy(lc);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_large_stripe_count(void)
{
    fprintf(stdout, "  large_stripe_count:               ");

    struct layout_cache *lc = NULL;
    ASSERT_EQ(layout_cache_init(8, &lc), 0);

    /* MDS_MAX_STRIPES = 1024.  Verify the cache can carry a
     * tier-1 layout end to end. */
    const uint32_t big = MDS_MAX_STRIPES;
    struct mds_ds_map_entry *input =
        calloc(big, sizeof(*input));
    ASSERT_TRUE(input != NULL);
    fill_entries(input, /*fileid=*/0xC0DE, big, 1);
    ASSERT_EQ(layout_cache_put(lc, 0xC0DE, big, 1U << 20, 1, input),
              0);
    free(input);

    uint32_t sc = 0, su = 0, mc = 0;
    struct mds_ds_map_entry *out = NULL;
    ASSERT_EQ(layout_cache_get(lc, 0xC0DE, &sc, &su, &mc, &out), 0);
    ASSERT_EQ(sc, big);
    ASSERT_EQ(su, 1U << 20);
    ASSERT_EQ(mc, 1u);
    ASSERT_TRUE(verify_entries(out, 0xC0DE, big, 1));
    free(out);

    /* Stripe-count above the cap must be rejected by put. */
    struct mds_ds_map_entry one;
    memset(&one, 0, sizeof(one));
    ASSERT_EQ(layout_cache_put(lc, 0xC0DF, big + 1, 1U << 20, 1,
                               &one),
              -1);

    layout_cache_destroy(lc);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_clear(void)
{
    fprintf(stdout, "  clear:                            ");

    struct layout_cache *lc = NULL;
    ASSERT_EQ(layout_cache_init(64, &lc), 0);

    struct mds_ds_map_entry e;
    memset(&e, 0, sizeof(e));
    for (uint64_t fid = 1; fid <= 32; fid++) {
        ASSERT_EQ(layout_cache_put(lc, fid, 1, 65536, 1, &e), 0);
    }

    struct layout_cache_stats st;
    layout_cache_stats_get(lc, &st);
    ASSERT_EQ(st.entry_count, 32u);

    layout_cache_clear(lc);

    layout_cache_stats_get(lc, &st);
    ASSERT_EQ(st.entry_count, 0u);
    ASSERT_TRUE(st.invalidations >= 32u);

    /* All previously cached fileids must miss. */
    uint32_t sc, su, mc;
    struct mds_ds_map_entry *out = NULL;
    for (uint64_t fid = 1; fid <= 32; fid++) {
        out = NULL;
        ASSERT_EQ(layout_cache_get(lc, fid, &sc, &su, &mc, &out),
                  -1);
    }

    layout_cache_destroy(lc);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* -----------------------------------------------------------------------
 * Concurrency smoke test
 *
 * Run N threads doing random get/put/invalidate against a small
 * fileid space.  This is a smoke test, not a formal stress test:
 * it catches obvious data races and double-free / use-after-free
 * patterns under the per-shard mutex contract.  The fileid space
 * is intentionally larger than the cache capacity so eviction is
 * exercised continuously.
 * ----------------------------------------------------------------------- */

#define CONC_THREADS    8
#define CONC_ITERATIONS 5000
#define CONC_FILEIDS    32

struct conc_arg {
    struct layout_cache *lc;
    uint32_t             seed;
    int                  thread_id;
};

static void *conc_worker(void *p)
{
    struct conc_arg *arg = (struct conc_arg *)p;
    unsigned int seed = arg->seed;
    struct mds_ds_map_entry buf[2];
    int i;

    for (i = 0; i < CONC_ITERATIONS; i++) {
        uint64_t fid = (rand_r(&seed) % CONC_FILEIDS) + 1;
        int op = rand_r(&seed) % 4;

        if (op == 0) {
            uint32_t sc, su, mc;
            struct mds_ds_map_entry *out = NULL;
            int rc = layout_cache_get(arg->lc, fid,
                                      &sc, &su, &mc, &out);
            if (rc == 0) {
                /* Non-trivial ds_id so optimisers can't elide
                 * the read. */
                volatile uint32_t sink = out[0].ds_id;
                (void)sink;
                free(out);
            }
        } else if (op == 1) {
            fill_entries(buf, fid, 2, 1);
            (void)layout_cache_put(arg->lc, fid, 2, 65536, 1, buf);
        } else if (op == 2) {
            layout_cache_invalidate(arg->lc, fid);
        } else {
            struct layout_cache_stats st;
            layout_cache_stats_get(arg->lc, &st);
            (void)st;
        }
    }
    return NULL;
}

static void test_concurrent(void)
{
    fprintf(stdout, "  concurrent:                       ");

    struct layout_cache *lc = NULL;
    ASSERT_EQ(layout_cache_init(LAYOUT_CACHE_SHARDS * 4, &lc), 0);

    pthread_t threads[CONC_THREADS];
    struct conc_arg args[CONC_THREADS];

    for (int t = 0; t < CONC_THREADS; t++) {
        args[t].lc        = lc;
        args[t].seed      = (uint32_t)(0x12345 + t);
        args[t].thread_id = t;
        ASSERT_EQ(pthread_create(&threads[t], NULL,
                                 conc_worker, &args[t]),
                  0);
    }
    for (int t = 0; t < CONC_THREADS; t++) {
        pthread_join(threads[t], NULL);
    }

    /* Cache must be self-consistent: stats sum should be reachable
     * (no deadlock, no crash).  Entry count never exceeds capacity. */
    struct layout_cache_stats st;
    layout_cache_stats_get(lc, &st);
    ASSERT_TRUE(st.entry_count <= (uint64_t)LAYOUT_CACHE_SHARDS * 4);
    ASSERT_TRUE(st.hits + st.misses > 0);

    layout_cache_destroy(lc);

    fprintf(stdout, "PASS (hits=%llu misses=%llu evictions=%llu)\n",
            (unsigned long long)st.hits,
            (unsigned long long)st.misses,
            (unsigned long long)st.evictions);
    passed++;
}

int main(void)
{
    fprintf(stdout, "test_layout_cache:\n");

    test_create_destroy();
    test_invalid_args();
    test_miss_on_empty();
    test_put_get_round_trip();
    test_put_replaces_existing();
    test_invalidate();
    test_eviction_at_capacity();
    test_get_returns_owned_copy();
    test_large_stripe_count();
    test_clear();
    test_concurrent();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}

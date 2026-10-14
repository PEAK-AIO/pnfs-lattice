/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_lifecycle.c -- Contract C7: ownership at close.
 *
 * ops->close() releases backend-owned resources only and the
 * dispatcher frees struct mds_catalogue (mds_catalogue_close).  Two
 * things go wrong when a backend gets this wrong: a double free when
 * the backend frees the handle too, and a leak when it forgets its own
 * state.  Neither is visible to a plain run, so this test is also run
 * under -fsanitize=address,undefined (see the conformance CMakeLists
 * for the second configure); the plain run still catches crashes and
 * cross-handle interference.
 *
 *   1. open/use/close cycles on one thread: 200 by default, 20 on
 *      RonDB (see cycle_count()); CATALOGUE_TEST_CYCLES overrides
 *      either for soak runs.
 *   2. 8 handles open at the same time, each driven from its own
 *      thread, then closed in a shuffled (but reproducible) order.
 *      Handles are independent stores on memdb and independent
 *      connections on a shared store, so every thread only ever sees
 *      its own names.
 */

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "harness.h"
#include "check.h"

#define CYCLES_DEFAULT 200
/* Every RonDB open is a full cluster connect (several seconds), so the
 * default would run for tens of minutes there; 20 still crosses the
 * handle-reuse and connection-pool boundaries the test is after. */
#define CYCLES_RONDB   20
/* CATALOGUE_TEST_CYCLES bounds: a soak run of that size still finishes
 * in a bounded time on every backend. */
#define CYCLES_MIN     1
#define CYCLES_MAX     10000
#define CYCLES_ENV     "CATALOGUE_TEST_CYCLES"
#define HANDLES       8
#define OPS_PER_THREAD 64

/*
 * Open/close cycles for this run: CATALOGUE_TEST_CYCLES when set to an
 * integer in [CYCLES_MIN, CYCLES_MAX], else the backend default.  A
 * value outside the range or not a number is reported and ignored
 * rather than silently clamped, so a typo cannot pass for a soak run.
 */
static int cycle_count(void)
{
    const char *env = getenv(CYCLES_ENV);
    int fallback = conformance_backend_is("rondb") ? CYCLES_RONDB : CYCLES_DEFAULT;
    char *end = NULL;
    long v;

    if (env == NULL || env[0] == '\0') {
        return fallback;
    }
    errno = 0;
    v = strtol(env, &end, 10);
    if (errno != 0 || end == env || *end != '\0' || v < CYCLES_MIN || v > CYCLES_MAX) {
        (void)fprintf(stderr, "  %s='%s' ignored (want %d..%d); using %d\n", CYCLES_ENV,
                      env, CYCLES_MIN, CYCLES_MAX, fallback);
        return fallback;
    }
    return (int)v;
}

static void test_open_close_cycles(int cycles)
{
    int i;

    (void)printf("  open/close cycles: %d\n", cycles);
    for (i = 0; i < cycles; i++) {
        struct mds_catalogue *cat = NULL;
        struct mds_inode child, seen;
        uint64_t dir = 0;

        REQUIRE_EQ(conformance_open(&cat), MDS_OK);
        REQUIRE(cat != NULL);
        REQUIRE_EQ(conformance_scratch_dir(cat, &dir), MDS_OK);
        memset(&child, 0, sizeof(child));
        REQUIRE_EQ(mds_cat_ns_create(cat, NULL, dir, "cycle", MDS_FTYPE_REG,
                                     0644, 0, 0, NULL, &child), MDS_OK);
        CHECK_EQ(mds_cat_ns_lookup(cat, dir, "cycle", &seen), MDS_OK);
        CHECK_EQ(seen.fileid, child.fileid);
        if (i == 0) {
            /* A raw alias dirent whose inode is unlinked underneath it
             * (what the catalogue unit tests leave behind): the cleanup
             * must still take the scratch directory with it, or every
             * run on a shared store leaks one directory. */
            CHECK_EQ(mds_cat_dirent_insert(cat, NULL, dir, "alias",
                                           child.fileid,
                                           (uint8_t)MDS_FTYPE_REG), MDS_OK);
            CHECK_EQ(mds_cat_ns_remove(cat, NULL, dir, "cycle"), MDS_OK);
        }
        conformance_scratch_cleanup(cat, dir);
        CHECK_EQ(mds_cat_ns_getattr(cat, dir, &seen), MDS_ERR_NOTFOUND);
        mds_catalogue_close(cat);
    }
    /* NULL is always safe. */
    mds_catalogue_close(NULL);
}

struct worker {
    struct mds_catalogue *cat;
    uint64_t dir;
    int index;
    int failures;
};

static void *worker_main(void *arg)
{
    struct worker *w = arg;
    int i;

    for (i = 0; i < OPS_PER_THREAD; i++) {
        struct mds_inode child, seen;
        char name[64];

        (void)snprintf(name, sizeof(name), "h%d-f%d", w->index, i);
        memset(&child, 0, sizeof(child));
        if (mds_cat_ns_create(w->cat, NULL, w->dir, name, MDS_FTYPE_REG,
                              0644, 0, 0, NULL, &child) != MDS_OK) {
            w->failures++;
            continue;
        }
        if (mds_cat_ns_lookup(w->cat, w->dir, name, &seen) != MDS_OK ||
            seen.fileid != child.fileid) {
            w->failures++;
        }
        if (mds_cat_ns_remove(w->cat, NULL, w->dir, name) != MDS_OK) {
            w->failures++;
        }
        if (mds_cat_ns_lookup(w->cat, w->dir, name, &seen) !=
            MDS_ERR_NOTFOUND) {
            w->failures++;
        }
    }
    return NULL;
}

/* Fixed-seed shuffle so a failing order is reproducible from the log. */
static void shuffle(int *order, int n)
{
    uint32_t state = 0x9E3779B9U;
    int i;

    for (i = n - 1; i > 0; i--) {
        int j;
        int tmp;

        state = state * 1664525U + 1013904223U;
        j = (int)(state % (uint32_t)(i + 1));
        tmp = order[i];
        order[i] = order[j];
        order[j] = tmp;
    }
}

static void test_concurrent_handles(void)
{
    struct worker workers[HANDLES];
    pthread_t threads[HANDLES];
    int order[HANDLES];
    int opened = 0;
    int i;

    memset(workers, 0, sizeof(workers));
    for (i = 0; i < HANDLES; i++) {
        workers[i].index = i;
        if (conformance_open(&workers[i].cat) != MDS_OK ||
            workers[i].cat == NULL) {
            break;
        }
        opened++;
        if (conformance_scratch_dir(workers[i].cat, &workers[i].dir) !=
            MDS_OK) {
            workers[i].failures++;
        }
    }
    CHECK_EQ(opened, HANDLES);

    for (i = 0; i < opened; i++) {
        CHECK_EQ(pthread_create(&threads[i], NULL, worker_main,
                                &workers[i]), 0);
    }
    for (i = 0; i < opened; i++) {
        (void)pthread_join(threads[i], NULL);
        CHECK_EQ(workers[i].failures, 0);
    }

    for (i = 0; i < opened; i++) {
        order[i] = i;
    }
    shuffle(order, opened);
    (void)printf("  close order:");
    for (i = 0; i < opened; i++) {
        struct worker *w = &workers[order[i]];

        (void)printf(" %d", order[i]);
        conformance_scratch_cleanup(w->cat, w->dir);
        mds_catalogue_close(w->cat);
        w->cat = NULL;
    }
    (void)printf("\n");
}

int main(void)
{
    /* Probe the backend once so an unavailable backend skips before
     * any counting happens. */
    struct mds_catalogue *probe = conformance_open_checked();
    int rc;

    (void)printf("test_lifecycle (backend=%s):\n", conformance_backend_name());
    mds_catalogue_close(probe);

    test_open_close_cycles(cycle_count());
    test_concurrent_handles();

    rc = check_summary("test_lifecycle");
    conformance_shutdown();
    return rc;
}

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * bench_rondb_create.c -- Micro-benchmark: raw RonDB file create latency.
 *
 * Creates N files directly via the RonDB catalogue layer (no NFS, no XDR,
 * no RPC, no compound dispatch).  Measures the bare NDB transaction cost
 * for ns_create (dirent + inode + parent update + optional stripe).
 *
 * Usage:  bench_rondb_create <rondb_config> <count>
 *
 * Compare the ops/sec from this tool with mdtest to see how much
 * overhead the NFS/XDR/RPC wiring adds on top of pure NDB latency.
 *
 * Build: linked against pnfs_mds_core + rondb_shim (same as pnfs-mds).
 */

#ifdef HAVE_RONDB

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "catalogue_rondb.h"

static double elapsed_ms(const struct timespec *start,
                         const struct timespec *end)
{
    double s = (double)(end->tv_sec - start->tv_sec) * 1000.0;
    double ns = (double)(end->tv_nsec - start->tv_nsec) / 1000000.0;
    return s + ns;
}

/* CLOCK_MONOTONIC is always available on Linux, so the only failure
 * mode is a programming error; the benchmark aborts rather than
 * reporting timings taken from an unset timespec. */
static void stamp_now(struct timespec *ts)
{
    if (clock_gettime(CLOCK_MONOTONIC, ts) != 0) {
        (void)fprintf(stderr, "clock_gettime failed: %d\n", errno);
        abort();
    }
}

/* Parse a strictly decimal file count in 1..UINT32_MAX. */
static int parse_count(const char *s, uint32_t *out)
{
    char *end = NULL;
    unsigned long v;

    errno = 0;
    v = strtoul(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v == 0UL ||
        v > UINT32_MAX) {
        return -1;
    }
    *out = (uint32_t)v;
    return 0;
}

int main(int argc, char **argv)
{
    struct mds_config cfg;
    struct mds_catalogue *cat = NULL;
    enum mds_status st;
    uint32_t count = 200;
    struct timespec t_start, t_end;
    uint32_t success = 0;

    if (argc < 2) {
        (void)fprintf(stderr, "Usage: %s <mds.conf> [count]\n", argv[0]);
        return 1;
    }
    if (argc >= 3 && parse_count(argv[2], &count) != 0) {
        (void)fprintf(stderr, "count must be a decimal integer in 1..%u\n",
                      (unsigned)UINT32_MAX);
        return 1;
    }

    /* Load config and open catalogue. */
    memset(&cfg, 0, sizeof(cfg));
    st = mds_config_load(argv[1], &cfg);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Config load failed: %d\n", (int)st);
        return 1;
    }

    st = catalogue_rondb_open(&cfg, &cat);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "RonDB catalogue open failed: %d\n", (int)st);
        return 1;
    }

    /* Bootstrap schema if needed. */
    (void)mds_rondb_bootstrap(cat);

    (void)fprintf(stdout, "Creating %u files via raw RonDB ns_create...\n",
                  count);

    /* Warm up: 1 create to prime the NDB thread-local connection. */
    {
        struct mds_inode warmup;
        (void)mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT,
                                "__warmup__", MDS_FTYPE_REG,
                                0644, 0, 0, NULL, &warmup);
        (void)mds_cat_ns_remove(cat, NULL, MDS_FILEID_ROOT,
                                "__warmup__");
    }

    /* Timed loop: create N files in root directory. */
    stamp_now(&t_start);

    for (uint32_t i = 0; i < count; i++) {
        char name[64];
        struct mds_inode out;

        /* "bench_" + at most 10 digits always fits in 64 bytes. */
        (void)snprintf(name, sizeof(name), "bench_%06u", i);
        st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT,
                               name, MDS_FTYPE_REG,
                               0644, 1000, 1000, NULL, &out);
        if (st == MDS_OK) {
            success++;
        } else {
            (void)fprintf(stderr, "Create %s failed: %d\n", name, (int)st);
        }
    }

    stamp_now(&t_end);

    double total_ms = elapsed_ms(&t_start, &t_end);
    double ops_per_sec = (double)success / (total_ms / 1000.0);
    double avg_ms = total_ms / (double)count;

    (void)fprintf(stdout, "\n--- Raw RonDB ns_create benchmark ---\n");
    (void)fprintf(stdout, "Files created: %u / %u\n", success, count);
    (void)fprintf(stdout, "Total time:    %.1f ms\n", total_ms);
    (void)fprintf(stdout, "Avg latency:   %.2f ms/op\n", avg_ms);
    (void)fprintf(stdout, "Throughput:    %.1f ops/sec\n", ops_per_sec);

    /* Cleanup: remove the files. */
    (void)fprintf(stdout, "\nCleaning up...\n");

    stamp_now(&t_start);

    for (uint32_t i = 0; i < count; i++) {
        char name[64];

        (void)snprintf(name, sizeof(name), "bench_%06u", i);
        (void)mds_cat_ns_remove(cat, NULL, MDS_FILEID_ROOT, name);
    }

    stamp_now(&t_end);

    total_ms = elapsed_ms(&t_start, &t_end);
    ops_per_sec = (double)count / (total_ms / 1000.0);
    avg_ms = total_ms / (double)count;

    (void)fprintf(stdout, "\n--- Raw RonDB ns_remove benchmark ---\n");
    (void)fprintf(stdout, "Files removed: %u\n", count);
    (void)fprintf(stdout, "Total time:    %.1f ms\n", total_ms);
    (void)fprintf(stdout, "Avg latency:   %.2f ms/op\n", avg_ms);
    (void)fprintf(stdout, "Throughput:    %.1f ops/sec\n", ops_per_sec);

    mds_catalogue_close(cat);
    return 0;
}

#else /* !HAVE_RONDB */

#include <stdio.h>

int main(void)
{
    (void)fprintf(stderr, "This benchmark requires ENABLE_RONDB=ON\n");
    return 1;
}

#endif

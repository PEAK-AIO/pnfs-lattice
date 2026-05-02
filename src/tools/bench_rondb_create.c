/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * bench_rondb_create.c — Micro-benchmark: raw RonDB file create latency.
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "catalogue_rondb.h"

static double elapsed_ms(struct timespec *start, struct timespec *end)
{
    double s = (double)(end->tv_sec - start->tv_sec) * 1000.0;
    double ns = (double)(end->tv_nsec - start->tv_nsec) / 1000000.0;
    return s + ns;
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
        fprintf(stderr, "Usage: %s <mds.conf> [count]\n", argv[0]);
        return 1;
    }
    if (argc >= 3) {
        count = (uint32_t)atoi(argv[2]);
    }

    /* Load config and open catalogue. */
    memset(&cfg, 0, sizeof(cfg));
    st = mds_config_load(argv[1], &cfg);
    if (st != MDS_OK) {
        fprintf(stderr, "Config load failed: %d\n", (int)st);
        return 1;
    }

    st = catalogue_rondb_open(&cfg, &cat);
    if (st != MDS_OK) {
        fprintf(stderr, "RonDB catalogue open failed: %d\n", (int)st);
        return 1;
    }

    /* Bootstrap schema if needed. */
    (void)mds_rondb_bootstrap(cat);

    fprintf(stdout, "Creating %u files via raw RonDB ns_create...\n", count);

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
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    for (uint32_t i = 0; i < count; i++) {
        char name[64];
        struct mds_inode out;

        snprintf(name, sizeof(name), "bench_%06u", i);
        st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT,
                               name, MDS_FTYPE_REG,
                               0644, 1000, 1000, NULL, &out);
        if (st == MDS_OK) {
            success++;
        } else {
            fprintf(stderr, "Create %s failed: %d\n", name, (int)st);
        }
    }

    clock_gettime(CLOCK_MONOTONIC, &t_end);

    double total_ms = elapsed_ms(&t_start, &t_end);
    double ops_per_sec = (double)success / (total_ms / 1000.0);
    double avg_ms = total_ms / (double)count;

    fprintf(stdout, "\n--- Raw RonDB ns_create benchmark ---\n");
    fprintf(stdout, "Files created: %u / %u\n", success, count);
    fprintf(stdout, "Total time:    %.1f ms\n", total_ms);
    fprintf(stdout, "Avg latency:   %.2f ms/op\n", avg_ms);
    fprintf(stdout, "Throughput:    %.1f ops/sec\n", ops_per_sec);

    /* Cleanup: remove the files. */
    fprintf(stdout, "\nCleaning up...\n");

    clock_gettime(CLOCK_MONOTONIC, &t_start);

    for (uint32_t i = 0; i < count; i++) {
        char name[64];

        snprintf(name, sizeof(name), "bench_%06u", i);
        (void)mds_cat_ns_remove(cat, NULL, MDS_FILEID_ROOT, name);
    }

    clock_gettime(CLOCK_MONOTONIC, &t_end);

    total_ms = elapsed_ms(&t_start, &t_end);
    ops_per_sec = (double)count / (total_ms / 1000.0);
    avg_ms = total_ms / (double)count;

    fprintf(stdout, "\n--- Raw RonDB ns_remove benchmark ---\n");
    fprintf(stdout, "Files removed: %u\n", count);
    fprintf(stdout, "Total time:    %.1f ms\n", total_ms);
    fprintf(stdout, "Avg latency:   %.2f ms/op\n", avg_ms);
    fprintf(stdout, "Throughput:    %.1f ops/sec\n", ops_per_sec);

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

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_backend_probe.c — Backend connectivity and health probe tool.
 *
 * MDS-local tool for validating catalogue backend runtime:
 *   --probe      Health check (canary read/write)
 *   --bootstrap  One-time schema/subspace creation
 *   --cleanup    Explicit teardown (lab reset)
 *
 * No compound dispatch, no DS dependency, no NFS semantics.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <getopt.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#ifdef HAVE_RONDB
#include "catalogue_rondb.h"
#endif

static void usage(const char *prog)
{
    (void)fprintf(stderr,
        "Usage: %s --config <path> [--probe] [--bootstrap] [--cleanup]\n"
        "\n"
        "  --config <path>   MDS config file (required)\n"
        "  --probe           Run health probe against selected backend\n"
        "  --bootstrap       One-time schema/subspace creation\n"
        "  --cleanup         Teardown backend schema (lab reset)\n"
        "\n"
        "Backend is selected by CatalogueBackend in the config file.\n",
        prog);
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int main(int argc, char *argv[])
{
    const char *config_path = NULL;
    bool do_probe = false;
    bool do_bootstrap = false;
    bool do_cleanup = false;
    struct mds_config cfg;
    struct mds_catalogue *cat = NULL;
    enum mds_status st;
    enum mds_status final_st = MDS_OK;

    static struct option long_opts[] = {
        {"config",    required_argument, NULL, 'c'},
        {"probe",     no_argument,       NULL, 'p'},
        {"bootstrap", no_argument,       NULL, 'b'},
        {"cleanup",   no_argument,       NULL, 'x'},
        {"help",      no_argument,       NULL, 'h'},
        {NULL, 0, NULL, 0}
    };

    int opt;
    /* NOLINTNEXTLINE(concurrency-mt-unsafe) */
    while ((opt = getopt_long(argc, argv, "c:pbxh", long_opts, NULL)) != -1) {
        switch (opt) {
        case 'c': config_path = optarg; break;
        case 'p': do_probe = true; break;
        case 'b': do_bootstrap = true; break;
        case 'x': do_cleanup = true; break;
        case 'h':
        default:
            usage(argv[0]);
            return (opt == 'h') ? 0 : 1;
        }
    }

    if (config_path == NULL) {
        (void)fprintf(stderr, "ERROR: --config is required\n");
        usage(argv[0]);
        return 1;
    }

    if (!do_probe && !do_bootstrap && !do_cleanup) {
        (void)fprintf(stderr, "ERROR: specify at least one of "
                "--probe, --bootstrap, --cleanup\n");
        return 1;
    }

    /* Load config. */
    st = mds_config_load(config_path, &cfg);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "ERROR: config load failed: %d\n", (int)st);
        return 1;
    }

    (void)fprintf(stdout, "Backend: %s\n",
        /* removed */
        cfg.catalogue_backend == MDS_BACKEND_RONDB ? "rondb" :
        "unknown");

    /* Open catalogue. */
    st = mds_catalogue_open(&cfg, &cat);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "ERROR: catalogue open failed: %d (%s)\n",
                (int)st, mds_status_str(st));
        return 1;
    }
    (void)fprintf(stdout, "Catalogue opened successfully.\n");

    /* Bootstrap. */
    if (do_bootstrap) {
        enum mds_status cmd_st = MDS_OK;
        (void)fprintf(stdout, "Running bootstrap...\n");
        if (cfg.catalogue_backend == MDS_BACKEND_RONDB) {
            (void)fprintf(stdout, "Backend: no bootstrap needed "
                    "(root inode created on open).\n");
        }
#ifdef HAVE_RONDB
        else if (cfg.catalogue_backend == MDS_BACKEND_RONDB) {
            cmd_st = mds_rondb_bootstrap(cat);
            (void)fprintf(stdout, "RonDB bootstrap: %s\n",
                    cmd_st == MDS_OK ? "OK" : mds_status_str(cmd_st));
        }
#endif
        else {
            cmd_st = MDS_ERR_INVAL;
            (void)fprintf(stdout, "Bootstrap: unsupported backend.\n");
        }
        if (cmd_st != MDS_OK) {
            final_st = cmd_st;
        }
    }

    /* Probe. */
    if (do_probe) {
        struct timespec t0, t1;
        double latency_ms;

        (void)fprintf(stdout, "Running probe...\n");
        clock_gettime(CLOCK_MONOTONIC, &t0);

        st = mds_catalogue_probe(cat);

        clock_gettime(CLOCK_MONOTONIC, &t1);
        latency_ms = (double)(t1.tv_sec - t0.tv_sec) * 1000.0 +
                     (double)(t1.tv_nsec - t0.tv_nsec) / 1e6;

        (void)fprintf(stdout, "Probe result: %s (%.3f ms)\n",
                st == MDS_OK ? "OK" : mds_status_str(st),
                latency_ms);
        if (st != MDS_OK) {
            final_st = st;
        }
    }

    /* Cleanup. */
    if (do_cleanup) {
        enum mds_status cmd_st = MDS_OK;

        (void)fprintf(stdout, "Running cleanup...\n");
        if (cfg.catalogue_backend == MDS_BACKEND_RONDB) {
            (void)fprintf(stdout, "Backend: no cleanup needed.\n");
        }
#ifdef HAVE_RONDB
        else if (cfg.catalogue_backend == MDS_BACKEND_RONDB) {
            cmd_st = mds_rondb_cleanup(cat);
            (void)fprintf(stdout, "RonDB cleanup: %s\n",
                    cmd_st == MDS_OK ? "OK" : mds_status_str(cmd_st));
        }
#endif
        else {
            cmd_st = MDS_ERR_INVAL;
            (void)fprintf(stdout, "Cleanup: unsupported backend.\n");
        }

        if (cmd_st != MDS_OK) {
            final_st = cmd_st;
        }
    }

    mds_catalogue_close(cat);
    (void)fprintf(stdout, "Done.\n");
    return (final_st == MDS_OK) ? 0 : 1;
}

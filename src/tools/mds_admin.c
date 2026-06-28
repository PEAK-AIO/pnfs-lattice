/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_admin.c -- Administrative CLI tool.
 *
 * Usage: mds-admin <command> [options]
 *
 * Commands:
 *   subtree list           -- List subtree->MDS assignments
 *   subtree migrate <path> <dest_mds>
 *   ds list                -- List data servers and status
 *   ds add <host:port>     -- Add a data server
 *   resilver <ds_id>       -- Trigger resilvering for a DS
 *   rebalance <src> <tgt>  -- Move mirrors from source to target DS
 *   status                 -- Cluster health summary
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <time.h>

#include "pnfs_mds.h"
#include "cluster_transport.h"
#include "quota.h"
#include "resilver.h"
#include "rebalance.h"
#include "tiering.h"
#include "cluster_drain.h"
#include "subtree_split.h"
#include "backup.h"
#include "mds_catalogue.h"
#include "cluster_membership.h"

#include "admin_util.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "mds_metrics.h"

static void usage(const char *prog)
{
    (void)fprintf(stderr,
        "Usage: %s [--help] [--version] <command> [options]\n\n"
        "Global options:\n"
        "  -h, --help          Show this help\n"
        "  -V, --version       Show version\n\n"
        "Common per-command options:\n"
        "  --mds-host <host>   Admin target (default 127.0.0.1)\n"
        "  --mds-port <port>   Admin port   (default 9800)\n"
        "  --json              Machine-readable JSON output\n\n"
        "Commands:\n"
        "  status              Cluster health summary\n"
        "  metrics             Prometheus metrics\n\n"
        "  subtree list [--owner <mds_id>]\n"
        "  subtree owned <mds_id>\n"
        "  subtree migrate <path> <dest_mds_id>"
            " [--host <h>] [--port <p>]\n"
        "  subtree split <parent> <child> <dest_mds_id>"
            " [--host <h>] [--port <p>]\n"
        "  subtree assign <path> <dest_mds_id>"
            " [--host <h>] [--port <p>]\n\n"
        "  ds list             List registered data servers\n"
        "  ds add <ds_id> <host> <port> [--tier N]"
        " [--mode generic]"
        " [--transport tcp|rdma|tcp,rdma]"
        " [--export-path /path]\n"
        "  ds set-state <ds_id> <state>\n"
        "  ds remove <ds_id>\n"
        "  ds validate <ds_id> [--tool-version <str>] [--note <str>]\n"
        "  ds validate-clear <ds_id>\n"
        "  ds validate-info <ds_id>\n"
        "\n"
        "  ds import <ds_id> <source_dir> --db PATH"
        " --ds-mount PATH [--target-dir /path]\n"
        "  ds reattach <ds_id> --db PATH --ds-mount PATH\n"
        "  ds scan <ds_id> --db PATH --ds-mount PATH"
        " [--repair]\n\n"
        "  node join <mds_id> <hostname>"
            " [--nfs-port N] [--grpc-port N]\n"
        "  node leave <mds_id>\n"
        "  node drain <mds_id>\n"
        "  node undrain <mds_id>\n"
        "  node detach <mds_id>\n"
        "  node force-remove <mds_id>\n"
        "  node set-lifecycle <mds_id> <state>\n\n"
        "  standby attach <mds_id> <hostname> [...]\n\n"
        "  cluster status\n\n"
        "  migration progress\n\n"
        "  resilver start <ds_id>\n"
        "  resilver status [<ds_id>]\n\n"
        "  rebalance start <source_ds> <target_ds>\n"
        "  rebalance status\n\n"
        "  tier start [--interval SEC]"
            " [--promote-thresh N]"
            " [--demote-thresh N] [--cooldown N]\n"
        "  tier stop\n"
        "  tier status\n\n"
        "  quota set <scope_type> <scope_id>"
            " <max_bytes> <max_inodes>\n"
        "  quota report <scope_type> <scope_id>\n"
        "  quota del <scope_type> <scope_id>\n\n"
        "  backup create <path>\n"
        "  backup verify <path>\n"
        "  backup info <path>\n"
        "  backup restore <path>\n\n"
        "  upgrade failover <target_mds_id>\n"
        "  upgrade status\n\n"
        "  config show [<key>]       Live INI key=value dump\n"
        "  ds capacity show          Live DS total/used/weight\n"
        "  ds capacity probe-now     Force statvfs sweep now\n"
        "  ds set-weight <id> <w>    Set runtime WRR weight\n",
        prog);
}

/* -----------------------------------------------------------------------
 * subtree list [--owner <mds_id>] [--mds-host <host>] [--mds-port <port>]
 * ----------------------------------------------------------------------- */

static const char *subtree_state_name(int state)
{
    switch (state) {
    case 0: return "ACTIVE";
    case 1: return "MIGRATING";
    case 2: return "FROZEN";
    case 3: return "DRAINING";
    default: return "UNKNOWN";
    }
}

/* cppcheck-suppress constParameter */
static int cmd_subtree_list(int argc, char *const argv[])
{
    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    bool json = false;
    if (parse_admin_endpoint(argc, (const char *const *)argv,
                             &mds_host, &mds_port, &json) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    uint32_t owner_filter = 0;
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--owner") == 0 && i + 1 < argc) {
            owner_filter = (uint32_t)strtoul(argv[++i], NULL, 10);
        }
    }

    struct subtree_entry *entries = NULL;
    uint32_t count = 0;
    enum mds_status st = cluster_transport_request_subtree_list(
        mds_host, mds_port, owner_filter, &entries, &count);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Error: subtree list failed (%d)\n", (int)st);
        return 1;
    }

    if (json) {
        (void)printf("[\n");
        for (uint32_t i = 0; i < count; i++) {
            char esc_path[MDS_MAX_PATH * 6];
            if (json_escape_string(entries[i].path, esc_path,
                                   sizeof(esc_path)) < 0) {
                (void)fprintf(stderr,
                    "Error: path too long for JSON\n");
                free(entries);
                return 1;
            }
            (void)printf(
                "  {\n"
                "    \"path\": \"%s\",\n"
                "    \"owner\": %u,\n"
                "    \"state\": \"%s\",\n"
                "    \"ops\": %lu,\n"
                "    \"version\": %lu\n"
                "  }%s\n",
                esc_path, entries[i].owner_mds_id,
                subtree_state_name((int)entries[i].state),
                (unsigned long)atomic_load(&entries[i].op_count),
                (unsigned long)entries[i].version,
                (i + 1 < count) ? "," : "");
        }
        (void)printf("]\n");
    } else {
        (void)printf("%-40s  %-6s  %-10s  %-8s  %s\n",
                     "PATH", "OWNER", "STATE", "OPS", "VERSION");
        for (uint32_t i = 0; i < count; i++) {
            (void)printf("%-40s  %-6u  %-10s  %-8lu  %lu\n",
                         entries[i].path,
                         entries[i].owner_mds_id,
                         subtree_state_name((int)entries[i].state),
                         (unsigned long)atomic_load(&entries[i].op_count),
                         (unsigned long)entries[i].version);
        }
    }

    free(entries);
    return 0;
}

/**
 * cmd_subtree_migrate -- sends an admin migration request to the
 * running MDS daemon via its cluster transport port.
 *
 * The daemon performs path resolution, transport setup, and
 * migration_initiate() using its live db and subtree_map.
 */
static int cmd_subtree_migrate(int argc, char *argv[])
{
    const char *subtree_path = argv[0];
    const char *dest_str = argv[1];
    const char *mds_host  = DEFAULT_MDS_HOST;
    uint16_t    mds_port  = DEFAULT_MDS_PORT;
	const char *dest_host = NULL;
	uint16_t    dest_port = 0;
	bool have_host = false;
	bool have_port = false;
	int remaining = argc - 2;
	char **extra = argv + 2;

	/* Parse optional flags. */
	if (parse_admin_endpoint(remaining, (const char *const *)extra,
	                        &mds_host, &mds_port, NULL) != 0) {
		(void)fprintf(stderr, "Error: invalid --mds-port\n");
		return EXIT_FAILURE;
	}
	if (parse_dest_endpoint(remaining, (const char *const *)extra,
	                       &dest_host, &dest_port,
	                       &have_host, &have_port) != 0) {
		(void)fprintf(stderr, "Error: invalid --port\n");
		return EXIT_FAILURE;
	}
	if (have_host != have_port) {
		(void)fprintf(stderr, "--host and --port must be specified together.\n");
		return EXIT_FAILURE;
	}

    char *endptr = NULL;
    errno = 0;
    unsigned long dest_mds_id = strtoul(dest_str, &endptr, 10);
    if (errno != 0 || endptr == dest_str || dest_mds_id == 0) {
        (void)fprintf(stderr, "Invalid dest_mds_id: %s\n", dest_str);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Requesting migration of %s to MDS %lu via %s:%u ...\n",
            subtree_path, dest_mds_id, mds_host, (unsigned)mds_port);

    enum mds_status st = cluster_transport_request_migration(
        mds_host, mds_port, subtree_path, (uint32_t)dest_mds_id,
        dest_host, dest_port);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Migration request failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Migration of %s to MDS %lu completed successfully.\n",
            subtree_path, dest_mds_id);
    return EXIT_SUCCESS;
}

/* cppcheck-suppress constParameter */
static int cmd_resilver_start(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    if (argc < 1) {
        (void)fprintf(stderr, "Usage: resilver <ds_id> "
                "[--mds-host <host>] [--mds-port <port>]\n");
        return EXIT_FAILURE;
    }

    char *endptr = NULL;
    errno = 0;
    unsigned long ds_id = strtoul(argv[0], &endptr, 10);
    if (errno != 0 || endptr == argv[0] || ds_id > UINT32_MAX) {
        (void)fprintf(stderr, "Invalid ds_id: %s\n", argv[0]);
        return EXIT_FAILURE;
    }

    /* Parse optional flags. */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) {
                mds_port = (uint16_t)v;
}
        }
    }

    (void)fprintf(stdout, "Requesting resilver onto DS %lu via %s:%u ...\n",
            ds_id, mds_host, (unsigned)mds_port);

    enum mds_status st = cluster_transport_request_resilver_start(
        mds_host, mds_port, (uint32_t)ds_id);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Resilver start request failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Resilver started.\n");
    return EXIT_SUCCESS;
}

static const char *resilver_state_name(uint32_t state)
{
    switch (state) {
    case 0: return "IDLE";
    case 1: return "SCANNING";
    case 2: return "COPYING";
    case 3: return "DONE";
    case 4: return "ERROR";
    default: return "UNKNOWN";
    }
}

/* cppcheck-suppress constParameter */
static int cmd_resilver_status(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) {
                mds_port = (uint16_t)v;
}
        }
    }

    struct resilver_status_info info;

    enum mds_status st = cluster_transport_request_resilver_status_ext(
        mds_host, mds_port, &info);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Resilver status request failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "State:   %s\n", resilver_state_name(info.state));
    (void)fprintf(stdout, "Files:   %u / %u\n", info.files_done, info.files_total);
    (void)fprintf(stdout, "Copied:  %lu bytes\n", (unsigned long)info.bytes_copied);
    (void)fprintf(stdout, "Passes:  %u\n", info.pass_count);

    uint32_t total_skipped = info.skipped_active_layout
                           + info.skipped_active_writer
                           + info.skipped_sparse
                           + info.skipped_multi_stripe
                           + info.skipped_error;
    if (total_skipped > 0) {
        (void)fprintf(stdout, "Skipped: %u\n", total_skipped);
        if (info.skipped_active_layout > 0) {
            (void)fprintf(stdout, "  active-layout: %u\n",
                    info.skipped_active_layout);
        }
        if (info.skipped_active_writer > 0) {
            (void)fprintf(stdout, "  active-writer: %u\n",
                    info.skipped_active_writer);
        }
        if (info.skipped_sparse > 0) {
            (void)fprintf(stdout, "  sparse:        %u\n", info.skipped_sparse);
        }
        if (info.skipped_multi_stripe > 0) {
            (void)fprintf(stdout, "  multi-stripe:  %u\n",
                    info.skipped_multi_stripe);
        }
        if (info.skipped_error > 0) {
            (void)fprintf(stdout, "  error:         %u\n", info.skipped_error);
        }
    }
    return EXIT_SUCCESS;
}

/* -----------------------------------------------------------------------
 * status [--mds-host <host>] [--mds-port <port>]
 *
 * Compose a summary from cluster status + subtree list + migration progress.
 * ----------------------------------------------------------------------- */

/* cppcheck-suppress constParameter */
static int cmd_status(int argc, char *const argv[])
{
    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    if (parse_admin_endpoint(argc, (const char *const *)argv,
                             &mds_host, &mds_port, NULL) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    /* Cluster membership. */
    struct cluster_member *members = NULL;
    uint32_t mem_count = 0;
    enum mds_status st = cluster_transport_request_cluster_status(
        mds_host, mds_port, &members, &mem_count);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Error: cluster status failed (%d)\n", (int)st);
        return 1;
    }

    (void)printf("=== Cluster: %u node(s) ===\n", mem_count);
    for (uint32_t i = 0; i < mem_count; i++) {
        (void)printf("  MDS %-4u  %-20s  role=%u  lifecycle=%u\n",
                     members[i].mds_id, members[i].hostname,
                     (unsigned)members[i].role,
                     (unsigned)members[i].lifecycle);
    }
    free(members);

    /* Subtree list. */
    struct subtree_entry *entries = NULL;
    uint32_t sub_count = 0;
    st = cluster_transport_request_subtree_list(
        mds_host, mds_port, 0, &entries, &sub_count);
    if (st == MDS_OK) {
        (void)printf("\n=== Subtrees: %u ===\n", sub_count);
        for (uint32_t i = 0; i < sub_count; i++) {
            (void)printf("  %-40s  owner=%u  %s\n",
                         entries[i].path, entries[i].owner_mds_id,
                         subtree_state_name((int)entries[i].state));
        }
        free(entries);
    }

    /* Migration progress. */
    uint32_t mstate = 0, mtotal = 0, mdone = 0;
    char mpath[4096];
    mpath[0] = '\0';
    st = cluster_transport_request_mig_progress(
        mds_host, mds_port, &mstate, mpath, sizeof(mpath),
        &mtotal, &mdone);
    if (st == MDS_OK && mstate != 0) {
        (void)printf("\n=== Migration in progress ===\n");
        (void)printf("  path=%s  state=%u  done=%u/%u\n",
                     mpath, mstate, mdone, mtotal);
    } else {
        (void)printf("\n=== Migration: idle ===\n");
    }

    return 0;
}


/* -----------------------------------------------------------------------
 * Node membership commands
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static int cmd_node_join(int argc, char *argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;
    uint16_t    nfs_port = 2049;
    uint16_t    grpc_port = 50051;
    enum node_role      role = NODE_ACTIVE;
    uint32_t            partner_id = 0;
    const char         *cluster_addr = "";

    if (argc < 2) {
        (void)fprintf(stderr, "Usage: node join <mds_id> <hostname> "
                "[--nfs-port N] [--grpc-port N] "
                "[--role active|standby] [--partner-id N] "
                "[--cluster-addr ADDR] "
                "[--mds-host <host>] [--mds-port <port>]\n");
        return EXIT_FAILURE;
    }

    char *endptr = NULL;
    errno = 0;
    unsigned long mds_id = strtoul(argv[0], &endptr, 10);
    if (errno != 0 || endptr == argv[0] || mds_id == 0 || mds_id > UINT32_MAX) {
        (void)fprintf(stderr, "Invalid mds_id: %s\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *hostname = argv[1];

    /* Parse optional flags. */
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--nfs-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) { nfs_port = (uint16_t)v;
}
        } else if (strcmp(argv[i], "--grpc-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) { grpc_port = (uint16_t)v;
}
        } else if (strcmp(argv[i], "--role") == 0 && i + 1 < argc) {
            const char *rv = argv[++i];
            if (strcmp(rv, "standby") == 0) {
                role = NODE_STANDBY;
            } else if (strcmp(rv, "active") == 0) {
                role = NODE_ACTIVE;
            } else {
                (void)fprintf(stderr, "Unknown role: %s (use active|standby)\n", rv);
                return EXIT_FAILURE;
            }
        } else if (strcmp(argv[i], "--partner-id") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v <= UINT32_MAX) { partner_id = (uint32_t)v;
}
        } else if (strcmp(argv[i], "--cluster-addr") == 0 && i + 1 < argc) {
            cluster_addr = argv[++i];
        } else if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) { mds_port = (uint16_t)v;
}
        }
    }

    (void)fprintf(stdout, "Registering MDS %lu (%s, role=%s) via %s:%u ...\n",
            mds_id, hostname,
            (role == NODE_STANDBY) ? "standby" : "active",
            mds_host, (unsigned)mds_port);

    enum mds_status st = cluster_transport_request_node_join(
        mds_host, mds_port, (uint32_t)mds_id, hostname,
        nfs_port, grpc_port,
        role, NODE_IDLE, partner_id, cluster_addr);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Node join failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Node %lu joined successfully.\n", mds_id);
    return EXIT_SUCCESS;
}

/* cppcheck-suppress constParameter */
static int cmd_node_leave(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    if (argc < 1) {
        (void)fprintf(stderr, "Usage: node leave <mds_id> "
                "[--mds-host <host>] [--mds-port <port>]\n");
        return EXIT_FAILURE;
    }

    char *endptr = NULL;
    errno = 0;
    unsigned long mds_id = strtoul(argv[0], &endptr, 10);
    if (errno != 0 || endptr == argv[0] || mds_id == 0 || mds_id > UINT32_MAX) {
        (void)fprintf(stderr, "Invalid mds_id: %s\n", argv[0]);
        return EXIT_FAILURE;
    }

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) { mds_port = (uint16_t)v;
}
        }
    }

    (void)fprintf(stdout, "Requesting leave for MDS %lu via %s:%u ...\n",
            mds_id, mds_host, (unsigned)mds_port);

    enum mds_status st = cluster_transport_request_node_leave(
        mds_host, mds_port, (uint32_t)mds_id);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Node leave failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Node %lu removed.\n", mds_id);
    return EXIT_SUCCESS;
}



/* cppcheck-suppress constParameter */
static int cmd_node_drain(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;
    uint32_t    dest_id  = 0;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--dest") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= UINT32_MAX) { dest_id = (uint32_t)v;
}
        } else if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) { mds_port = (uint16_t)v;
}
        } else {
            (void)fprintf(stderr, "Unexpected argument: %s\n", argv[i]);
            (void)fprintf(stderr, "Usage: node drain --dest <dest_mds_id> "
                    "[--mds-host <host>] [--mds-port <port>]\n");
            return EXIT_FAILURE;
        }
    }

    if (dest_id == 0) {
        (void)fprintf(stderr, "Usage: node drain --dest <dest_mds_id> "
                "[--mds-host <host>] [--mds-port <port>]\n");
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Requesting drain-self to dest %u via %s:%u ...\n",
            (unsigned)dest_id, mds_host, (unsigned)mds_port);

    enum mds_status st = cluster_transport_request_drain_self(
        mds_host, mds_port, dest_id);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Drain failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Drain complete -- node is now DRAINED.\n");
    return EXIT_SUCCESS;
}

/* cppcheck-suppress constParameter */
static int cmd_node_detach(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    if (argc < 1) {
        (void)fprintf(stderr, "Usage: node detach <mds_id> "
                "[--mds-host <host>] [--mds-port <port>]\n");
        return EXIT_FAILURE;
    }

    char *endptr = NULL;
    errno = 0;
    unsigned long mds_id = strtoul(argv[0], &endptr, 10);
    if (errno != 0 || endptr == argv[0] || mds_id == 0 || mds_id > UINT32_MAX) {
        (void)fprintf(stderr, "Invalid mds_id: %s\n", argv[0]);
        return EXIT_FAILURE;
    }

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) { mds_port = (uint16_t)v;
}
        }
    }

    (void)fprintf(stdout, "Requesting standby detach for MDS %lu via %s:%u ...\n",
            mds_id, mds_host, (unsigned)mds_port);

    enum mds_status st = cluster_transport_request_standby_detach(
        mds_host, mds_port, (uint32_t)mds_id);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Detach failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Standby %lu detached.\n", mds_id);
    return EXIT_SUCCESS;
}

/* cppcheck-suppress constParameter */
static int cmd_node_force_remove(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    if (argc < 1) {
        (void)fprintf(stderr, "Usage: node force-remove <mds_id> "
                "[--mds-host <host>] [--mds-port <port>]\n");
        return EXIT_FAILURE;
    }

    char *endptr = NULL;
    errno = 0;
    unsigned long mds_id = strtoul(argv[0], &endptr, 10);
    if (errno != 0 || endptr == argv[0] || mds_id == 0 || mds_id > UINT32_MAX) {
        (void)fprintf(stderr, "Invalid mds_id: %s\n", argv[0]);
        return EXIT_FAILURE;
    }

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) { mds_port = (uint16_t)v;
}
        }
    }

    (void)fprintf(stdout, "Force-removing MDS %lu via %s:%u ...\n",
            mds_id, mds_host, (unsigned)mds_port);

    enum mds_status st = cluster_transport_request_force_remove(
        mds_host, mds_port, (uint32_t)mds_id);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Force-remove failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Node %lu force-removed.\n", mds_id);
    return EXIT_SUCCESS;
}

static const char *node_role_str(enum node_role r)
{
    switch (r) {
    case NODE_ACTIVE:  return "active";
    case NODE_STANDBY: return "standby";
    default:           return "unknown";
    }
}

static const char *node_lifecycle_str(enum node_lifecycle lc)
{
    switch (lc) {
    case NODE_JOINING:        return "joining";
    case NODE_IDLE:           return "idle";
    case NODE_ACTIVE_SERVING: return "serving";
    case NODE_DRAINING:       return "draining";
    case NODE_DRAINED:        return "drained";
    default:                  return "unknown";
    }
}
/* -----------------------------------------------------------------------
 * Cluster status command
 * ----------------------------------------------------------------------- */

/* cppcheck-suppress constParameter */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static int cmd_cluster_status(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) { mds_port = (uint16_t)v;
}
        }
    }

    struct cluster_member *members = NULL;
    uint32_t count = 0;

    enum mds_status st = cluster_transport_request_cluster_status(
        mds_host, mds_port, &members, &count);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Cluster status request failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Cluster members (%u):\n", count);
    (void)fprintf(stdout, "  %-6s  %-20s  %-8s  %-10s  %-16s  %-10s  %-9s  %-8s  %s\n",
            "ID", "Hostname", "Role", "Lifecycle", "Cluster Addr",
            "NFS Port", "GRPC Port", "Partner", "Joined");

    for (uint32_t i = 0; i < count; i++) {
        /* Format join time. */
        char time_buf[32];
        time_t t = (time_t)members[i].join_time_sec;
        struct tm tm_buf;
        if (gmtime_r(&t, &tm_buf) != NULL) {
            (void)strftime(time_buf, sizeof(time_buf),
                     "%Y-%m-%dT%H:%M:%SZ", &tm_buf);
        } else {
            (void)snprintf(time_buf, sizeof(time_buf), "%lu",
                     (unsigned long)members[i].join_time_sec);
        }

        /* Mark coordinator (lowest ID). */
        uint32_t lowest = UINT32_MAX;
        for (uint32_t j = 0; j < count; j++) {
            if (members[j].mds_id < lowest) {
                lowest = members[j].mds_id;
}
        }
        const char *marker = (members[i].mds_id == lowest)
                             ? " *coordinator" : "";

        char partner_buf[16];
        if (members[i].failover_partner_id != 0) {
            (void)snprintf(partner_buf, sizeof(partner_buf), "%u",
                     (unsigned)members[i].failover_partner_id);
        } else {
            (void)snprintf(partner_buf, sizeof(partner_buf), "-");
        }

        const char *caddr = members[i].cluster_addr[0] != '\0'
                          ? members[i].cluster_addr : "-";

        (void)fprintf(stdout,
                "  %-6u  %-20s  %-8s  %-10s  %-16s  %-10u  %-9u  %-8s  %s%s\n",
                members[i].mds_id,
                members[i].hostname,
                node_role_str(members[i].role),
                node_lifecycle_str(members[i].lifecycle),
                caddr,
                (unsigned)members[i].nfs_port,
                (unsigned)members[i].grpc_port,
                partner_buf,
                time_buf,
                marker);
    }

    free(members);
    return EXIT_SUCCESS;
}
/* -----------------------------------------------------------------------
 * Node set-lifecycle command
 * ----------------------------------------------------------------------- */

static enum node_lifecycle parse_lifecycle(const char *s)
{
    if (strcmp(s, "joining") == 0) {  return NODE_JOINING;
}
    if (strcmp(s, "idle") == 0) {     return NODE_IDLE;
}
    if (strcmp(s, "serving") == 0) {  return NODE_ACTIVE_SERVING;
}
    if (strcmp(s, "draining") == 0) { return NODE_DRAINING;
}
    if (strcmp(s, "drained") == 0) {  return NODE_DRAINED;
}
    /* NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange) */
    return (enum node_lifecycle)255; /* sentinel for invalid */
}

static int cmd_node_set_lifecycle(int argc, char *argv[])
{
    if (argc < 2) {
        (void)fprintf(stderr, "Usage: mds-admin node set-lifecycle <mds_id> <state>\n");
        (void)fprintf(stderr, "  states: joining, idle, serving, draining, drained\n");
        return EXIT_FAILURE;
    }

    unsigned long mds_id = strtoul(argv[0], NULL, 10);
    if (mds_id == 0 || mds_id > MDS_MAX_NODES) {
        (void)fprintf(stderr, "Invalid mds_id: %s\n", argv[0]);
        return EXIT_FAILURE;
    }

    enum node_lifecycle target = parse_lifecycle(argv[1]);
    if ((int)target == 255) {
        (void)fprintf(stderr, "Unknown lifecycle state: %s\n", argv[1]);
        (void)fprintf(stderr, "  valid: joining, idle, serving, draining, drained\n");
        return EXIT_FAILURE;
    }

    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) { mds_port = (uint16_t)v;
}
        }
    }

	 enum mds_status st = cluster_transport_request_set_lifecycle(
	     mds_host, mds_port, (uint32_t)mds_id, target);
	 if (st != MDS_OK) {
	     (void)fprintf(stderr, "set-lifecycle failed: %d\n", (int)st);
	     return EXIT_FAILURE;
	 }

	 (void)fprintf(stdout,
	         "set-lifecycle: node %lu -> %s OK\n",
	         mds_id, argv[1]);
	 return EXIT_SUCCESS;
}

/**
 * cmd_subtree_split -- sends a split admin request to the running MDS
 * daemon.  The daemon validates, resolves the child path to fileid,
 * registers the new child subtree, and migrates if remote.
 */
static int cmd_subtree_split(int argc, char *argv[])
{
	const char *parent_path = argv[0];
	const char *child_path  = argv[1];
	const char *dest_str    = argv[2];
	const char *mds_host    = DEFAULT_MDS_HOST;
	uint16_t    mds_port    = DEFAULT_MDS_PORT;
	const char *dest_host   = NULL;
	uint16_t    dest_port   = 0;
	bool have_host = false;
	bool have_port = false;
	int remaining = argc - 3;
	char **extra = argv + 3;

	if (parse_admin_endpoint(remaining, (const char *const *)extra,
	                        &mds_host, &mds_port, NULL) != 0) {
		(void)fprintf(stderr, "Error: invalid --mds-port\n");
		return EXIT_FAILURE;
	}
	if (parse_dest_endpoint(remaining, (const char *const *)extra,
	                       &dest_host, &dest_port,
	                       &have_host, &have_port) != 0) {
		(void)fprintf(stderr, "Error: invalid --port\n");
		return EXIT_FAILURE;
	}
	if (have_host != have_port) {
		(void)fprintf(stderr, "--host and --port must be specified together.\n");
		return EXIT_FAILURE;
	}

	char *endptr = NULL;
	errno = 0;
	unsigned long dest_mds_id = strtoul(dest_str, &endptr, 10);
	if (errno != 0 || endptr == dest_str || dest_mds_id == 0) {
		(void)fprintf(stderr, "Invalid dest_mds_id: %s\n", dest_str);
		return EXIT_FAILURE;
	}

	(void)fprintf(stdout,
	        "Requesting split of %s -> %s to MDS %lu via %s:%u ...\n",
	        parent_path, child_path, dest_mds_id,
	        mds_host, (unsigned)mds_port);

	enum mds_status st = cluster_transport_request_split(
	    mds_host, mds_port, parent_path, child_path,
	    (uint32_t)dest_mds_id, dest_host, dest_port);

	if (st != MDS_OK) {
		(void)fprintf(stderr, "Split request failed: %d\n", (int)st);
		return EXIT_FAILURE;
	}

	(void)fprintf(stdout,
	        "Split %s -> %s to MDS %lu completed.\n",
	        parent_path, child_path, dest_mds_id);
	return EXIT_SUCCESS;
}

/**
 * cmd_subtree_assign -- migrate an exact subtree root to a new owner.
 * Unlike 'subtree migrate', this requires the path to already be a
 * registered subtree root.  If not, the daemon returns an error.
 */
static int cmd_subtree_assign(int argc, char *argv[])
{
	const char *subtree_path = argv[0];
	const char *dest_str     = argv[1];
	const char *mds_host     = DEFAULT_MDS_HOST;
	uint16_t    mds_port     = DEFAULT_MDS_PORT;
	const char *dest_host    = NULL;
	uint16_t    dest_port    = 0;
	bool have_host = false;
	bool have_port = false;
	int remaining = argc - 2;
	char **extra = argv + 2;

	if (parse_admin_endpoint(remaining, (const char *const *)extra,
	                        &mds_host, &mds_port, NULL) != 0) {
		(void)fprintf(stderr, "Error: invalid --mds-port\n");
		return EXIT_FAILURE;
	}
	if (parse_dest_endpoint(remaining, (const char *const *)extra,
	                       &dest_host, &dest_port,
	                       &have_host, &have_port) != 0) {
		(void)fprintf(stderr, "Error: invalid --port\n");
		return EXIT_FAILURE;
	}
	if (have_host != have_port) {
		(void)fprintf(stderr, "--host and --port must be specified together.\n");
		return EXIT_FAILURE;
	}

	char *endptr = NULL;
	errno = 0;
	unsigned long dest_mds_id = strtoul(dest_str, &endptr, 10);
	if (errno != 0 || endptr == dest_str || dest_mds_id == 0) {
		(void)fprintf(stderr, "Invalid dest_mds_id: %s\n", dest_str);
		return EXIT_FAILURE;
	}

	(void)fprintf(stdout,
	        "Requesting assign of %s to MDS %lu via %s:%u ...\n",
	        subtree_path, dest_mds_id, mds_host, (unsigned)mds_port);

	enum mds_status st = cluster_transport_request_assign(
	    mds_host, mds_port, subtree_path, (uint32_t)dest_mds_id,
	    dest_host, dest_port);

	if (st != MDS_OK) {
		(void)fprintf(stderr, "Assign request failed: %d\n", (int)st);
		return EXIT_FAILURE;
	}

	(void)fprintf(stdout,
	        "Assign of %s to MDS %lu completed.\n",
	        subtree_path, dest_mds_id);
	return EXIT_SUCCESS;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
/* -----------------------------------------------------------------------
 * subtree owned [--mds-host <host>] [--mds-port <port>] <mds_id>
 *
 * Alias for subtree list --owner <mds_id>.
 * ----------------------------------------------------------------------- */

/* cppcheck-suppress constParameter */
static int cmd_subtree_owned(int argc, char *const argv[])
{
    if (argc < 1) {
        (void)fprintf(stderr, "Usage: mds-admin subtree owned <mds_id> "
                      "[--mds-host <host>] [--mds-port <port>]\n");
        return 1;
    }

    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t mds_port = DEFAULT_MDS_PORT;
    uint32_t mds_id = (uint32_t)strtoul(argv[0], NULL, 10);

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            mds_port = (uint16_t)strtoul(argv[++i], NULL, 10);
        }
    }

    struct subtree_entry *entries = NULL;
    uint32_t count = 0;
    enum mds_status st = cluster_transport_request_subtree_list(
        mds_host, mds_port, mds_id, &entries, &count);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Error: subtree list failed (%d)\n", (int)st);
        return 1;
    }

    (void)printf("%-40s  %-10s  %-8s  %s\n",
                 "PATH", "STATE", "OPS", "VERSION");
    for (uint32_t i = 0; i < count; i++) {
        (void)printf("%-40s  %-10s  %-8lu  %lu\n",
                     entries[i].path,
                     subtree_state_name((int)entries[i].state),
                     (unsigned long)atomic_load(&entries[i].op_count),
                     (unsigned long)entries[i].version);
    }

    free(entries);
    return 0;
}

/* -----------------------------------------------------------------------
 * node undrain <mds_id> [--mds-host <host>] [--mds-port <port>]
 *
 * Server-authoritative: sets lifecycle back to ACTIVE via set_lifecycle.
 * ----------------------------------------------------------------------- */

/* cppcheck-suppress constParameter */
static int cmd_node_undrain(int argc, char *const argv[])
{
    if (argc < 1) {
        (void)fprintf(stderr, "Usage: mds-admin node undrain <mds_id> "
                      "[--mds-host <host>] [--mds-port <port>]\n");
        return 1;
    }

    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t mds_port = DEFAULT_MDS_PORT;
    uint32_t mds_id = (uint32_t)strtoul(argv[0], NULL, 10);

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            mds_port = (uint16_t)strtoul(argv[++i], NULL, 10);
        }
    }

    enum mds_status st = cluster_transport_request_set_lifecycle(
        mds_host, mds_port, mds_id, NODE_ACTIVE_SERVING);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Error: undrain failed (%d)\n", (int)st);
        return 1;
    }

    (void)printf("Node %u lifecycle set to ACTIVE.\n", mds_id);
    return 0;
}

/* -----------------------------------------------------------------------
 * standby attach <mds_id> <hostname> [--nfs-port N] [--grpc-port N]
 *     [--mds-host <host>] [--mds-port <port>]
 *
 * CLI alias: node join with --role standby.
 * ----------------------------------------------------------------------- */

static int cmd_standby_attach(int argc, char *argv[])
{
    /* Rewrite args to inject --role standby and call cmd_node_join.
     * CLI argc is always small, so a fixed-size stack array is safe. */
    enum { MAX_ARGS = 64 };
    if (argc + 2 >= MAX_ARGS) {
        (void)fprintf(stderr, "Error: too many arguments\n");
        return 1;
    }

    int new_argc = argc + 2;
    char *new_argv[MAX_ARGS] = {0};

    for (int i = 0; i < argc; i++) {
        new_argv[i] = argv[i];
    }
    new_argv[argc] = (char *)"--role";
    new_argv[argc + 1] = (char *)"standby";

    return cmd_node_join(new_argc, new_argv);
}

/* -----------------------------------------------------------------------
 * migration progress [--mds-host <host>] [--mds-port <port>]
 * ----------------------------------------------------------------------- */

/* cppcheck-suppress constParameter */
static int cmd_migration_progress(int argc, char *const argv[])
{
    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    if (parse_admin_endpoint(argc, (const char *const *)argv,
                             &mds_host, &mds_port, NULL) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    uint32_t mstate = 0, mtotal = 0, mdone = 0;
    char mpath[4096];
    mpath[0] = '\0';
    enum mds_status st = cluster_transport_request_mig_progress(
        mds_host, mds_port, &mstate, mpath, sizeof(mpath),
        &mtotal, &mdone);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Error: migration progress failed (%d)\n",
                      (int)st);
        return 1;
    }

    if (mstate == 0) {
        (void)printf("Migration: idle\n");
    } else {
        (void)printf("Migration in progress:\n");
        (void)printf("  path:  %s\n", mpath);
        (void)printf("  state: %u\n", mstate);
        (void)printf("  done:  %u / %u\n", mdone, mtotal);
    }

    return 0;
}


/* -----------------------------------------------------------------------
 * Per-group dispatch helpers (keep main() cognitive complexity low)
 * ----------------------------------------------------------------------- */

/* -----------------------------------------------------------------------
 * Split evaluator admin commands (Tier 3 Phase 1)
 * ----------------------------------------------------------------------- */

static const char *proposal_state_str(enum split_proposal_state s)
{
	switch (s) {
	case PROPOSAL_PENDING:   return "PENDING";
	case PROPOSAL_APPROVED:  return "APPROVED";
	case PROPOSAL_REJECTED:  return "REJECTED";
	case PROPOSAL_EXECUTING: return "EXECUTING";
	case PROPOSAL_DONE:      return "DONE";
	case PROPOSAL_FAILED:    return "FAILED";
	case PROPOSAL_COOLDOWN:  return "COOLDOWN";
	default:                 return "UNKNOWN";
	}
}

static int cmd_subtree_proposals(int argc, char *argv[])
{
	const char *mds_host = DEFAULT_MDS_HOST;
	uint16_t    mds_port = DEFAULT_MDS_PORT;

	if (parse_admin_endpoint(argc, (const char *const *)argv,
	                        &mds_host, &mds_port, NULL) != 0) {
		(void)fprintf(stderr, "Error: invalid --mds-port\n");
		return EXIT_FAILURE;
	}

	struct split_proposal *arr = NULL;
	uint32_t count = 0;
	enum mds_status st = cluster_transport_request_split_proposals(
	    mds_host, mds_port, &arr, &count);

	if (st != MDS_OK) {
		(void)fprintf(stderr, "Error: %s\n", mds_status_str(st));
		return EXIT_FAILURE;
	}

	if (count == 0) {
		(void)fprintf(stdout, "No proposals.\n");
		return EXIT_SUCCESS;
	}

	(void)fprintf(stdout, "%-30s %8s %6s %10s\n",
	              "PATH", "OPS", "HOT#", "STATE");
	for (uint32_t i = 0; i < count; i++) {
		(void)fprintf(stdout, "%-30s %8lu %6u %10s\n",
		              arr[i].path,
		              (unsigned long)arr[i].sampled_ops,
		              (unsigned)arr[i].hot_intervals,
		              proposal_state_str(arr[i].state));
	}
	free(arr);
	return EXIT_SUCCESS;
}

static int cmd_subtree_approve(int argc, char *argv[])
{
	const char *path     = argv[0];
	const char *mds_host = DEFAULT_MDS_HOST;
	uint16_t    mds_port = DEFAULT_MDS_PORT;

	if (parse_admin_endpoint(argc - 1, (const char *const *)argv + 1,
	                        &mds_host, &mds_port, NULL) != 0) {
		(void)fprintf(stderr, "Error: invalid --mds-port\n");
		return EXIT_FAILURE;
	}

	enum mds_status st = cluster_transport_request_split_approve(
	    mds_host, mds_port, path);

	if (st != MDS_OK) {
		(void)fprintf(stderr, "Error: %s\n", mds_status_str(st));
		return EXIT_FAILURE;
	}
	(void)fprintf(stdout, "Approved: %s\n", path);
	return EXIT_SUCCESS;
}

static int cmd_subtree_auto(int argc, char *argv[], bool enable)
{
	const char *mds_host = DEFAULT_MDS_HOST;
	uint16_t    mds_port = DEFAULT_MDS_PORT;

	if (parse_admin_endpoint(argc, (const char *const *)argv,
	                        &mds_host, &mds_port, NULL) != 0) {
		(void)fprintf(stderr, "Error: invalid --mds-port\n");
		return EXIT_FAILURE;
	}

	enum mds_status st = cluster_transport_request_split_auto(
	    mds_host, mds_port, enable);

	if (st != MDS_OK) {
		(void)fprintf(stderr, "Error: %s\n", mds_status_str(st));
		return EXIT_FAILURE;
	}
	(void)fprintf(stdout, "Auto-execute %s.\n",
	              enable ? "enabled" : "disabled");
	return EXIT_SUCCESS;
}

static int dispatch_subtree(int argc, char *argv[])
{
    if (argc < 3) { return -1; }
    if (strcmp(argv[2], "list") == 0) {
        return cmd_subtree_list(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "owned") == 0 && argc >= 4) {
        return cmd_subtree_owned(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "migrate") == 0 && argc >= 5) {
        return cmd_subtree_migrate(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "split") == 0 && argc >= 6) {
        return cmd_subtree_split(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "proposals") == 0) {
        return cmd_subtree_proposals(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "approve") == 0 && argc >= 4) {
        return cmd_subtree_approve(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "auto-enable") == 0) {
        return cmd_subtree_auto(argc - 3, argv + 3, true);
    }
    if (strcmp(argv[2], "auto-disable") == 0) {
        return cmd_subtree_auto(argc - 3, argv + 3, false);
    }
    if (strcmp(argv[2], "assign") == 0 && argc >= 5) {
        return cmd_subtree_assign(argc - 3, argv + 3);
    }
    return -1;
}

static int dispatch_node(int argc, char *argv[])
{
    if (argc < 3) { return -1; }
    if (strcmp(argv[2], "join") == 0 && argc >= 5) {
        return cmd_node_join(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "leave") == 0 && argc >= 4) {
        return cmd_node_leave(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "drain") == 0) {
        return cmd_node_drain(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "detach") == 0 && argc >= 4) {
        return cmd_node_detach(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "force-remove") == 0 && argc >= 4) {
        return cmd_node_force_remove(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "set-lifecycle") == 0 && argc >= 5) {
        return cmd_node_set_lifecycle(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "undrain") == 0 && argc >= 4) {
        return cmd_node_undrain(argc - 3, argv + 3);
    }
    return -1;
}


/* -----------------------------------------------------------------------
 * rebalance <source_ds> <target_ds>
 * rebalance status
 * ----------------------------------------------------------------------- */

static const char *rebalance_state_name(uint32_t state)
{
    switch (state) {
    case 0: return "IDLE";
    case 1: return "SCANNING";
    case 2: return "COPYING";
    case 3: return "DONE";
    case 4: return "ERROR";
    default: return "UNKNOWN";
    }
}

/* cppcheck-suppress constParameter */
static int cmd_rebalance_start(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    if (argc < 2) {
        (void)fprintf(stderr, "Usage: rebalance <source_ds> <target_ds> "
                "[--mds-host <host>] [--mds-port <port>]\n");
        return EXIT_FAILURE;
    }

    char *end1 = NULL;
    char *end2 = NULL;
    errno = 0;
    unsigned long src = strtoul(argv[0], &end1, 10);
    unsigned long tgt = strtoul(argv[1], &end2, 10);
    if (errno != 0 || end1 == argv[0] || end2 == argv[1] ||
        src > UINT32_MAX || tgt > UINT32_MAX) {
        (void)fprintf(stderr, "Invalid ds_id(s): %s %s\n", argv[0], argv[1]);
        return EXIT_FAILURE;
    }

    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) {
                mds_port = (uint16_t)v;
            }
        }
    }

    (void)fprintf(stdout, "Requesting rebalance DS %lu -> DS %lu via %s:%u ...\n",
            src, tgt, mds_host, (unsigned)mds_port);

    enum mds_status st = cluster_transport_request_rebalance_start(
        mds_host, mds_port, (uint32_t)src, (uint32_t)tgt);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Rebalance start request failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Rebalance started.\n");
    return EXIT_SUCCESS;
}

/* cppcheck-suppress constParameter */
static int cmd_rebalance_status(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) {
                mds_port = (uint16_t)v;
            }
        }
    }

    struct rebalance_status_info info;
    enum mds_status st = cluster_transport_request_rebalance_status(
        mds_host, mds_port, &info);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Rebalance status request failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "State:      %s\n", rebalance_state_name(info.state));
    (void)fprintf(stdout, "Source DS:  %u\n", info.source_ds);
    (void)fprintf(stdout, "Target DS:  %u\n", info.target_ds);
    (void)fprintf(stdout, "Candidates: %u\n", info.candidates_in_pass);
    (void)fprintf(stdout, "Moved:      %u\n", info.files_moved);
    (void)fprintf(stdout, "Copied:     %lu bytes\n", (unsigned long)info.bytes_copied);
    (void)fprintf(stdout, "Passes:     %u\n", info.pass_count);

    uint32_t total_skipped = info.skipped_active_layout
                           + info.skipped_active_writer
                           + info.skipped_sparse
                           + info.skipped_multi_stripe
                           + info.skipped_error;
    if (total_skipped > 0) {
        (void)fprintf(stdout, "Skipped:    %u\n", total_skipped);
        if (info.skipped_active_layout > 0) {
            (void)fprintf(stdout, "  active-layout: %u\n",
                    info.skipped_active_layout);
        }
        if (info.skipped_active_writer > 0) {
            (void)fprintf(stdout, "  active-writer: %u\n",
                    info.skipped_active_writer);
        }
        if (info.skipped_sparse > 0) {
            (void)fprintf(stdout, "  sparse:        %u\n", info.skipped_sparse);
        }
        if (info.skipped_multi_stripe > 0) {
            (void)fprintf(stdout, "  multi-stripe:  %u\n",
                    info.skipped_multi_stripe);
        }
        if (info.skipped_error > 0) {
            (void)fprintf(stdout, "  error:         %u\n", info.skipped_error);
        }
    }
    return EXIT_SUCCESS;
}

/* -----------------------------------------------------------------------
 * Tiering CLI commands (S3.9 storage tiering V1)
 * ----------------------------------------------------------------------- */

static const char *tiering_state_name(uint32_t state)
{
    switch (state) {
    case 0: return "IDLE";
    case 1: return "EVALUATING";
    case 2: return "MOVING";
    case 3: return "DONE";
    case 4: return "ERROR";
    default: return "UNKNOWN";
    }
}

/* cppcheck-suppress constParameter */
static int cmd_tier_start(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    struct tiering_config cfg;
    memset(&cfg, 0, sizeof(cfg));

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) {
                mds_port = (uint16_t)v;
            }
        } else if (strcmp(argv[i], "--interval") == 0 && i + 1 < argc) {
            cfg.eval_interval_sec = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--promote-thresh") == 0 && i + 1 < argc) {
            cfg.promote_threshold = strtod(argv[++i], NULL);
        } else if (strcmp(argv[i], "--demote-thresh") == 0 && i + 1 < argc) {
            cfg.demote_threshold = strtod(argv[++i], NULL);
        } else if (strcmp(argv[i], "--cooldown") == 0 && i + 1 < argc) {
            cfg.cooldown_evals = (uint32_t)strtoul(argv[++i], NULL, 10);
        }
    }

    if (cfg.promote_threshold < 0.0 || cfg.demote_threshold < 0.0) {
        (void)fprintf(stderr,
                "Error: thresholds must be non-negative\n");
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Requesting tiering start via %s:%u ...\n",
            mds_host, (unsigned)mds_port);

    enum mds_status st = cluster_transport_request_tiering_start(
        mds_host, mds_port, &cfg);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Tiering start failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Tiering started.\n");
    return EXIT_SUCCESS;
}

/* cppcheck-suppress constParameter */
static int cmd_tier_stop(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) {
                mds_port = (uint16_t)v;
            }
        }
    }

    enum mds_status st = cluster_transport_request_tiering_stop(
        mds_host, mds_port);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Tiering stop failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "Tiering stopped.\n");
    return EXIT_SUCCESS;
}


static void print_skip_breakdown(const struct tiering_status_info *info)
{
    uint32_t total = info->skipped_active_layout
                   + info->skipped_active_writer
                   + info->skipped_sparse
                   + info->skipped_multi_stripe
                   + info->skipped_cooldown
                   + info->skipped_no_target_ds
                   + info->skipped_error;
    if (total == 0) {
        return;
    }
    (void)fprintf(stdout, "Skipped:    %u\n", total);
    if (info->skipped_active_layout > 0) {
        (void)fprintf(stdout, "  active-layout:  %u\n",
                info->skipped_active_layout);
    }
    if (info->skipped_active_writer > 0) {
        (void)fprintf(stdout, "  active-writer:  %u\n",
                info->skipped_active_writer);
    }
    if (info->skipped_sparse > 0) {
        (void)fprintf(stdout, "  sparse:         %u\n",
                info->skipped_sparse);
    }
    if (info->skipped_multi_stripe > 0) {
        (void)fprintf(stdout, "  multi-stripe:   %u\n",
                info->skipped_multi_stripe);
    }
    if (info->skipped_cooldown > 0) {
        (void)fprintf(stdout, "  cooldown:       %u\n",
                info->skipped_cooldown);
    }
    if (info->skipped_no_target_ds > 0) {
        (void)fprintf(stdout, "  no-target-ds:   %u\n",
                info->skipped_no_target_ds);
    }
    if (info->skipped_error > 0) {
        (void)fprintf(stdout, "  error:          %u\n",
                info->skipped_error);
    }
}

/* cppcheck-suppress constParameter */
static int cmd_tier_status(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) {
                mds_port = (uint16_t)v;
            }
        }
    }

    struct tiering_status_info info;
    enum mds_status st = cluster_transport_request_tiering_status(
        mds_host, mds_port, &info);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Tiering status request failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "State:      %s\n", tiering_state_name(info.state));
    (void)fprintf(stdout, "Evals:      %u\n", info.eval_count);
    (void)fprintf(stdout, "Candidates: %u\n", info.candidates_in_eval);
    (void)fprintf(stdout, "Promoted:   %u\n", info.files_promoted);
    (void)fprintf(stdout, "Demoted:    %u\n", info.files_demoted);
    (void)fprintf(stdout, "Moved:      %lu bytes\n", (unsigned long)info.bytes_moved);

    print_skip_breakdown(&info);
    return EXIT_SUCCESS;
}

/* -----------------------------------------------------------------------
 * Quota commands
 * ----------------------------------------------------------------------- */

static int cmd_quota_set(int argc, const char *const argv[])
{
    /* mds-admin quota set --host H --port P --user|--group ID \
     *   --hard-bytes N --soft-bytes N --hard-inodes N --soft-inodes N --grace S */
    const char *host = "127.0.0.1";
    uint16_t port = 9820;
    uint8_t stype = 0;
    uint64_t sid = 0;
    struct mds_quota_rule rule;
    memset(&rule, 0, sizeof(rule));

    {
        bool have_h = false, have_p = false;
        if (parse_dest_endpoint(argc, argv, &host, &port,
                                &have_h, &have_p) != 0) {
            (void)fprintf(stderr, "Error: invalid --port\n");
            return EXIT_FAILURE;
        }
    }
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--user") == 0 && i + 1 < argc) {
            stype = (uint8_t)'U';
            sid = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--group") == 0 && i + 1 < argc) {
            stype = (uint8_t)'G';
            sid = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--hard-bytes") == 0 && i + 1 < argc) {
            rule.hard_bytes = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--soft-bytes") == 0 && i + 1 < argc) {
            rule.soft_bytes = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--hard-inodes") == 0 && i + 1 < argc) {
            rule.hard_inodes = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--soft-inodes") == 0 && i + 1 < argc) {
            rule.soft_inodes = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--grace") == 0 && i + 1 < argc) {
            rule.grace_sec = (uint32_t)strtoul(argv[++i], NULL, 10);
        }
    }

    if (stype == 0) {
        (void)fprintf(stderr, "error: --user or --group required\n");
        return EXIT_FAILURE;
    }

    enum mds_status st = cluster_transport_request_quota_set(
        host, port, stype, sid, &rule);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "quota set failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }
    printf("OK\n");
    return 0;
}

static int cmd_quota_report(int argc, const char *const argv[])
{
    const char *host = "127.0.0.1";
    uint16_t port = 9820;
    uint8_t stype = 0;
    uint64_t sid = 0;

    {
        bool have_h = false, have_p = false;
        if (parse_dest_endpoint(argc, argv, &host, &port,
                                &have_h, &have_p) != 0) {
            (void)fprintf(stderr, "Error: invalid --port\n");
            return EXIT_FAILURE;
        }
    }
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--user") == 0 && i + 1 < argc) {
            stype = (uint8_t)'U';
            sid = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--group") == 0 && i + 1 < argc) {
            stype = (uint8_t)'G';
            sid = (uint64_t)strtoull(argv[++i], NULL, 10);
        }
    }

    if (stype == 0) {
        (void)fprintf(stderr, "error: --user or --group required\n");
        return EXIT_FAILURE;
    }

    struct mds_quota_rule rule;
    struct mds_quota_usage qusage;
    memset(&rule, 0, sizeof(rule));
    memset(&qusage, 0, sizeof(qusage));

    enum mds_status st = cluster_transport_request_quota_get(
        host, port, stype, sid, &rule, &qusage);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "quota get failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    printf("scope:        %c:%lu\n", (char)stype, (unsigned long)sid);
    printf("hard_bytes:   %lu\n", (unsigned long)rule.hard_bytes);
    printf("soft_bytes:   %lu\n", (unsigned long)rule.soft_bytes);
    printf("hard_inodes:  %lu\n", (unsigned long)rule.hard_inodes);
    printf("soft_inodes:  %lu\n", (unsigned long)rule.soft_inodes);
    printf("grace_sec:    %u\n", rule.grace_sec);
    printf("used_bytes:   %lu\n", (unsigned long)qusage.used_bytes);
    printf("used_inodes:  %lu\n", (unsigned long)qusage.used_inodes);
    return 0;
}

static int cmd_quota_del(int argc, const char *const argv[])
{
    const char *host = "127.0.0.1";
    uint16_t port = 9820;
    uint8_t stype = 0;
    uint64_t sid = 0;

    {
        bool have_h = false, have_p = false;
        if (parse_dest_endpoint(argc, argv, &host, &port,
                                &have_h, &have_p) != 0) {
            (void)fprintf(stderr, "Error: invalid --port\n");
            return EXIT_FAILURE;
        }
    }
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--user") == 0 && i + 1 < argc) {
            stype = (uint8_t)'U';
            sid = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--group") == 0 && i + 1 < argc) {
            stype = (uint8_t)'G';
            sid = (uint64_t)strtoull(argv[++i], NULL, 10);
        }
    }

    if (stype == 0) {
        (void)fprintf(stderr, "error: --user or --group required\n");
        return EXIT_FAILURE;
    }

    enum mds_status st = cluster_transport_request_quota_del(
        host, port, stype, sid);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "quota del failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }
    printf("OK\n");
    return 0;
}

static int dispatch_quota(int argc, char *argv[])
{
    if (argc >= 3 && strcmp(argv[2], "set") == 0) {
        return cmd_quota_set(argc - 3, (const char *const *)(argv + 3));
    }
    if (argc >= 3 && strcmp(argv[2], "report") == 0) {
        return cmd_quota_report(argc - 3, (const char *const *)(argv + 3));
    }
    if (argc >= 3 && strcmp(argv[2], "del") == 0) {
        return cmd_quota_del(argc - 3, (const char *const *)(argv + 3));
    }
    return -1;
}

static int dispatch_tier(int argc, char *argv[])
{
    if (argc >= 3 && strcmp(argv[2], "start") == 0) {
        return cmd_tier_start(argc - 3, argv + 3);
    }
    if (argc >= 3 && strcmp(argv[2], "stop") == 0) {
        return cmd_tier_stop(argc - 3, argv + 3);
    }
    if (argc >= 3 && strcmp(argv[2], "status") == 0) {
        return cmd_tier_status(argc - 3, argv + 3);
    }
    return -1;
}

static int dispatch_rebalance(int argc, char *argv[])
{
    if (argc >= 3 && strcmp(argv[2], "status") == 0) {
        return cmd_rebalance_status(argc - 3, argv + 3);
    }
    if (argc >= 5 && strcmp(argv[2], "start") == 0) {
        return cmd_rebalance_start(argc - 3, argv + 3);
    }
    return -1;
}

static int dispatch_resilver(int argc, char *argv[])
{
    if (argc >= 3 && strcmp(argv[2], "status") == 0) {
        return cmd_resilver_status(argc - 3, argv + 3);
    }
    if (argc >= 4 && strcmp(argv[2], "start") == 0) {
        return cmd_resilver_start(argc - 3, argv + 3);
    }
    return -1;
}


/* -----------------------------------------------------------------------
 * Backup commands (deprecated -- returns NOSUPPORT)
 * ----------------------------------------------------------------------- */

/** Default map size for admin tool (deprecated). */
#define MDS_ADMIN_DEFAULT_MAPSIZE  (1ULL << 30)

static int cmd_backup_create(int argc, const char *const argv[])
{
    const char *db_path = NULL;
    const char *output  = NULL;
    uint32_t flags = 0;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--db") == 0 && i + 1 < argc) {
            db_path = argv[++i];
        } else if (strcmp(argv[i], "--output") == 0 && i + 1 < argc) {
            output = argv[++i];
        } else if (strcmp(argv[i], "--compact") == 0) {
            flags |= MDS_BACKUP_COMPACT;
        } else if (strcmp(argv[i], "--fsync-before-copy") == 0) {
            flags |= MDS_BACKUP_FSYNC_BEFORE_COPY;
        }
    }
    if (db_path == NULL || output == NULL) {
        (void)fprintf(stderr, "Usage: backup create --db PATH --output PATH"
                " [--compact] [--fsync-before-copy]\n");
        return EXIT_FAILURE;
    }

    /* Backup create is not supported against the live catalogue: the
     * LMDB open path was removed with the LMDB-era code, and the RonDB
     * catalogue does not expose a raw-backup API.  Use a RonDB-native
     * snapshot/backup mechanism instead. */
    (void)flags;
    (void)fprintf(stderr,
            "ERROR: backup create is not supported in this build (%s)\n",
            db_path);
    return EXIT_FAILURE;
}

static int cmd_backup_verify(int argc, const char *const argv[])
{
    const char *file = NULL;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--file") == 0 && i + 1 < argc) {
            file = argv[++i];
        }
    }
    if (file == NULL) {
        (void)fprintf(stderr, "Usage: backup verify --file PATH\n");
        return EXIT_FAILURE;
    }

    struct mds_backup_report rpt;
    enum mds_status st = mds_backup_verify(file, &rpt);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "FAIL: %s\n",
                rpt.error_msg[0] ? rpt.error_msg : "verification failed");
        return EXIT_FAILURE;
    }
    printf("OK: backup verified\n");
    if (rpt.source_path[0] != '\0') {
        printf("  source:        %s\n", rpt.source_path);
    }
    printf("  schema:        %" PRIu32 "\n", rpt.schema_version);
    printf("  size:          %" PRIu64 " bytes\n", rpt.backup_size_bytes);
    return 0;
}

static int cmd_backup_info(int argc, const char *const argv[])
{
    const char *file = NULL;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--file") == 0 && i + 1 < argc) {
            file = argv[++i];
        }
    }
    if (file == NULL) {
        (void)fprintf(stderr, "Usage: backup info --file PATH\n");
        return EXIT_FAILURE;
    }

    struct mds_backup_report rpt;
    enum mds_status st = mds_backup_info(file, &rpt);
    if (st == MDS_ERR_NOTFOUND) {
        (void)fprintf(stderr, "No sidecar found for %s\n", file);
        return EXIT_FAILURE;
    }
    if (st != MDS_OK) {
        (void)fprintf(stderr, "ERROR: cannot read sidecar\n");
        return EXIT_FAILURE;
    }
    printf("Sidecar metadata for %s:\n", file);
    printf("  created:       %" PRId64 "\n", rpt.created_unix_ts);
    printf("  schema:        %" PRIu32 "\n", rpt.schema_version);
    printf("  size:          %" PRIu64 " bytes\n", rpt.backup_size_bytes);
    printf("  inodes:        %" PRIu64 "\n", rpt.inode_count);
    printf("  dirents:       %" PRIu64 "\n", rpt.dirent_count);
    printf("  stripe_maps:   %" PRIu64 "\n", rpt.stripe_map_count);
    if (rpt.source_path[0] != '\0') {
        printf("  source:        %s\n", rpt.source_path);
    }
    return 0;
}

static int cmd_backup_restore(int argc, const char *const argv[])
{
    const char *file = NULL;
    const char *dest = NULL;
    uint32_t flags = 0;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--file") == 0 && i + 1 < argc) {
            file = argv[++i];
        } else if (strcmp(argv[i], "--dest") == 0 && i + 1 < argc) {
            dest = argv[++i];
        } else if (strcmp(argv[i], "--force") == 0) {
            flags |= MDS_BACKUP_RESTORE_OVERWRITE;
        }
    }
    if (file == NULL || dest == NULL) {
        (void)fprintf(stderr,
                "Usage: backup restore --file PATH --dest PATH [--force]\n");
        return EXIT_FAILURE;
    }

    enum mds_status st = mds_backup_restore(file, dest, flags);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "ERROR: restore failed\n");
        return EXIT_FAILURE;
    }
    printf("Restored %s -> %s\n", file, dest);
    return 0;
}

static int dispatch_backup(int argc, char *argv[])
{
    if (argc >= 3 && strcmp(argv[2], "create") == 0) {
        return cmd_backup_create(argc - 3, (const char *const *)(argv + 3));
    }
    if (argc >= 3 && strcmp(argv[2], "verify") == 0) {
        return cmd_backup_verify(argc - 3, (const char *const *)(argv + 3));
    }
    if (argc >= 3 && strcmp(argv[2], "info") == 0) {
        return cmd_backup_info(argc - 3, (const char *const *)(argv + 3));
    }
    if (argc >= 3 && strcmp(argv[2], "restore") == 0) {
        return cmd_backup_restore(argc - 3, (const char *const *)(argv + 3));
    }
    return -1;
}

/* -----------------------------------------------------------------------
 * upgrade failover <target_mds_id> [--mds-host <h>] [--mds-port <p>]
 * upgrade status [--mds-host <h>] [--mds-port <p>]
 * ----------------------------------------------------------------------- */

/* cppcheck-suppress constParameter */
static int cmd_upgrade_failover(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    if (argc < 1) {
        (void)fprintf(stderr,
            "Usage: upgrade failover <target_mds_id>"
            " [--mds-host <host>] [--mds-port <port>]\n");
        return EXIT_FAILURE;
    }

    char *endptr = NULL;
    errno = 0;
    unsigned long target_id = strtoul(argv[0], &endptr, 10);
    if (errno != 0 || endptr == argv[0] || target_id == 0 ||
        target_id > UINT32_MAX) {
        (void)fprintf(stderr, "Invalid target_mds_id: %s\n", argv[0]);
        return EXIT_FAILURE;
    }

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) { mds_port = (uint16_t)v; }
        }
    }

    (void)fprintf(stdout,
        "Requesting controlled failover of MDS %lu via %s:%u ...\n",
        target_id, mds_host, (unsigned)mds_port);

    enum mds_status st = cluster_transport_request_failover(
        mds_host, mds_port, (uint32_t)target_id);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Failover failed: %d\n", (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout, "MDS %lu demoted successfully.\n", target_id);
    return EXIT_SUCCESS;
}

/* cppcheck-suppress constParameter */
static int cmd_upgrade_status(int argc, char *const argv[])
{
    const char *mds_host = DEFAULT_MDS_HOST;
    uint16_t    mds_port = DEFAULT_MDS_PORT;

    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--mds-host") == 0 && i + 1 < argc) {
            mds_host = argv[++i];
        } else if (strcmp(argv[i], "--mds-port") == 0 && i + 1 < argc) {
            unsigned long v = strtoul(argv[++i], NULL, 10);
            if (v > 0 && v <= 65535) { mds_port = (uint16_t)v; }
        }
    }

    struct upgrade_status_entry *entries = NULL;
    uint32_t count = 0;

    enum mds_status st = cluster_transport_request_upgrade_status(
        mds_host, mds_port, &entries, &count);

    if (st != MDS_OK) {
        (void)fprintf(stderr, "Upgrade status query failed: %d\n",
                      (int)st);
        return EXIT_FAILURE;
    }

    (void)fprintf(stdout,
        "%-6s  %-30s  %-12s  %-10s  %-10s\n",
        "MDS_ID", "SOFTWARE_VERSION", "WIRE_COMPAT",
        "ROLE", "LIFECYCLE");
    (void)fprintf(stdout,
        "------  ------------------------------"
        "  ------------  ----------  ----------\n");

    for (uint32_t i = 0; i < count; i++) {
        const char *role_name = "unknown";
        switch (entries[i].role) {
        case 0: role_name = "active";  break;
        case 1: role_name = "standby"; break;
        default: break;
        }
        const char *lc_name = "unknown";
        switch (entries[i].lifecycle) {
        case 0: lc_name = "joining";  break;
        case 1: lc_name = "idle";     break;
        case 2: lc_name = "serving";  break;
        case 3: lc_name = "draining"; break;
        case 4: lc_name = "drained";  break;
        default: break;
        }
        (void)fprintf(stdout,
            "%-6" PRIu32 "  %-30s  %-12" PRIu32 "  %-10s  %-10s\n",
            entries[i].mds_id,
            entries[i].software_version,
            entries[i].wire_compat_version,
            role_name, lc_name);
    }

    free(entries);
    return EXIT_SUCCESS;
}


/* -----------------------------------------------------------------------
 * DS registry commands (Item 47)
 * ----------------------------------------------------------------------- */

static const char *ds_state_name(uint32_t state)
{
    switch (state) {
    case DS_ONLINE:     return "ONLINE";
    case DS_OFFLINE:    return "OFFLINE";
    case DS_DRAINING:   return "DRAINING";
    case DS_REBALANCING: return "REBALANCING";
    default:            return "UNKNOWN";
    }
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static int cmd_ds_list(int argc, const char *const *argv)
{
    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    bool json = false;
    if (parse_admin_endpoint(argc, (const char *const *)argv,
                             &mds_host, &mds_port, &json) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    struct mds_ds_info *ds = NULL;
    uint32_t count = 0;
    enum mds_status st = cluster_transport_request_ds_list(
        mds_host, mds_port, &ds, &count);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Error: ds list failed (%d)\n", (int)st);
        return 1;
    }

    if (json) {
        (void)printf("[\n");
        for (uint32_t i = 0; i < count; i++) {
            char esc_addr[MDS_DS_ADDR_MAX * 6];
            if (json_escape_string(ds[i].addr, esc_addr,
                                   sizeof(esc_addr)) < 0) {
                (void)fprintf(stderr,
                    "Error: addr too long for JSON\n");
                free(ds);
                return 1;
            }
            (void)printf(
                "  {\n"
                "    \"ds_id\": %u,\n"
                "    \"addr\": \"%s\",\n"
                "    \"port\": %u,\n"
                "    \"state\": \"%s\",\n"
                "    \"tier\": %u,\n"
                "    \"used_bytes\": %lu,\n"
                "    \"total_bytes\": %lu\n"
                "  }%s\n",
                ds[i].ds_id, esc_addr, (unsigned)ds[i].port,
                ds_state_name(ds[i].state), ds[i].tier,
                (unsigned long)ds[i].used_bytes,
                (unsigned long)ds[i].total_bytes,
                (i + 1 < count) ? "," : "");
        }
        (void)printf("]\n");
    } else {
        (void)printf("%-6s  %-20s  %-6s  %-6s  %-10s  %-8s  %-5s  %-10s  %s\n",
                     "DS_ID", "HOST", "TCP", "RDMA",
                     "STATE", "MODE", "XPORT", "CAPS",
                     "USED/TOTAL");
        for (uint32_t i = 0; i < count; i++) {
            const char *mode_s = (ds[i].mode == DS_MODE_GENERIC)
                                 ? "generic" : "patched";
            const char *xport_s = "tcp";
            if (ds[i].transport == DS_TRANSPORT_RDMA) {
                xport_s = "rdma";
            } else if (ds[i].transport == (DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA)) {
                xport_s = "both";
            }
            char caps_buf[16];
            if (ds[i].capabilities != 0) {
                (void)snprintf(caps_buf,
                    sizeof(caps_buf), "0x%x",
                    ds[i].capabilities);
            } else {
                caps_buf[0] = '-';
                caps_buf[1] = '\0';
            }
            const char *caps_s = caps_buf;
            (void)printf("%-6u  %-20s  %-6u  %-6u  %-10s  %-8s  %-5s  %-10s  %lu/%lu\n",
                         ds[i].ds_id,
                         ds[i].host[0] ? ds[i].host : ds[i].addr,
                         (unsigned)ds[i].tcp_port,
                         (unsigned)ds[i].rdma_port,
                         ds_state_name(ds[i].state), mode_s, xport_s,
                         caps_s, (unsigned long)ds[i].used_bytes,
                         (unsigned long)ds[i].total_bytes);
        }
    }

    free(ds);
    return 0;
}


/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static int cmd_ds_add(int argc, const char *const *argv)
{
    if (argc < 3) {
        (void)fprintf(stderr,
            "Usage: mds-admin ds add <ds_id> <addr> <port>"
            " [--tier N] [--total-bytes N] [--used-bytes N]\n");
        return 1;
    }

    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    if (parse_admin_endpoint(argc - 3, argv + 3,
                             &mds_host, &mds_port, NULL) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }
    struct mds_ds_info info;

    memset(&info, 0, sizeof(info));

    char *endptr = NULL;
    info.ds_id = (uint32_t)strtoul(argv[0], &endptr, 10);
    if (endptr == argv[0] || *endptr != '\0') {
        (void)fprintf(stderr, "Error: invalid ds_id\n");
        return 1;
    }

    (void)snprintf(info.addr, sizeof(info.addr), "%s", argv[1]);
    {
        unsigned long pv = strtoul(argv[2], &endptr, 10);
        if (endptr == argv[2] || *endptr != '\0' ||
            pv == 0 || pv > 65535) {
            (void)fprintf(stderr, "Error: invalid port\n");
            return 1;
        }
        info.port = (uint16_t)pv;
        info.tcp_port = (uint16_t)pv;  /* positional port = tcp_port */
    }

    /* Default structured fields. */
    info.mode = DS_MODE_GENERIC;
    info.transport = DS_TRANSPORT_TCP;

    /* Parse "host:/export" format into structured host + export_path. */
    {
        const char *colon = strchr(argv[1], ':');
        if (colon != NULL && colon > argv[1]) {
            size_t hlen = (size_t)(colon - argv[1]);
            if (hlen >= sizeof(info.host)) {
                hlen = sizeof(info.host) - 1;
            }
            memcpy(info.host, argv[1], hlen);
            info.host[hlen] = '\0';
            (void)snprintf(info.export_path, sizeof(info.export_path),
                     "%s", colon + 1);
        } else {
            (void)snprintf(info.host, sizeof(info.host), "%s", argv[1]);
            (void)snprintf(info.export_path, sizeof(info.export_path), "/");
        }
    }

    /* Parse optional flags. */
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--tier") == 0 && i + 1 < argc) {
            info.tier = (uint32_t)strtoul(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--total-bytes") == 0 && i + 1 < argc) {
            info.total_bytes = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--used-bytes") == 0 && i + 1 < argc) {
            info.used_bytes = (uint64_t)strtoull(argv[++i], NULL, 10);
        } else if (strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            if (parse_ds_mode(argv[++i], &info.mode) != 0) {
                (void)fprintf(stderr,
                    "Error: invalid mode (generic)\n");
                return 1;
            }
        } else if (strcmp(argv[i], "--transport") == 0 && i + 1 < argc) {
            if (parse_ds_transport(argv[++i], &info.transport) != 0) {
                (void)fprintf(stderr,
                    "Error: invalid transport (tcp|rdma|tcp,rdma)\n");
                return 1;
            }
        } else if (strcmp(argv[i], "--export-path") == 0 && i + 1 < argc) {
            (void)snprintf(info.export_path, sizeof(info.export_path),
                     "%s", argv[++i]);
        } else if (strcmp(argv[i], "--tcp-port") == 0 && i + 1 < argc) {
            unsigned long tp = strtoul(argv[++i], NULL, 10);
            if (tp == 0 || tp > 65535) {
                (void)fprintf(stderr, "Error: invalid --tcp-port\n");
                return 1;
            }
            info.tcp_port = (uint16_t)tp;
        } else if (strcmp(argv[i], "--rdma-port") == 0 && i + 1 < argc) {
            unsigned long rp = strtoul(argv[++i], NULL, 10);
            if (rp == 0 || rp > 65535) {
                (void)fprintf(stderr, "Error: invalid --rdma-port\n");
                return 1;
            }
            info.rdma_port = (uint16_t)rp;
        }
    }

    enum mds_status st = cluster_transport_request_ds_add(
        mds_host, mds_port, &info);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Error: ds add failed (%d)\n", (int)st);
        return 1;
    }

    (void)printf("DS %u registered (state: OFFLINE).\n"
                 "Use 'ds set-state %u online' to make it eligible"
                 " for placement.\n",
                 info.ds_id, info.ds_id);
    return 0;
}

static int cmd_ds_set_state(int argc, const char *const *argv)
{
    if (argc < 2) {
        (void)fprintf(stderr,
            "Usage: mds-admin ds set-state <ds_id>"
            " <online|offline|draining|rebalancing>\n");
        return 1;
    }

    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    if (parse_admin_endpoint(argc - 2, argv + 2,
                             &mds_host, &mds_port, NULL) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    char *endptr = NULL;
    uint32_t ds_id = (uint32_t)strtoul(argv[0], &endptr, 10);
    if (endptr == argv[0] || *endptr != '\0') {
        (void)fprintf(stderr, "Error: invalid ds_id\n");
        return 1;
    }

    uint32_t new_state = 0;
    if (parse_ds_state(argv[1], &new_state) != 0) {
        (void)fprintf(stderr,
            "Error: unknown state '%s'"
            " (online|offline|draining|rebalancing)\n",
            argv[1]);
        return 1;
    }

    enum mds_status st = cluster_transport_request_ds_set_state(
        mds_host, mds_port, ds_id, new_state);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Error: ds set-state failed (%d)\n", (int)st);
        return 1;
    }

    (void)printf("DS %u state set to %s.\n", ds_id, argv[1]);
    return 0;
}

static int cmd_ds_remove(int argc, const char *const *argv)
{
    if (argc < 1) {
        (void)fprintf(stderr, "Usage: mds-admin ds remove <ds_id>\n");
        return 1;
    }

    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    if (parse_admin_endpoint(argc - 1, argv + 1,
                             &mds_host, &mds_port, NULL) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    char *endptr = NULL;
    uint32_t ds_id = (uint32_t)strtoul(argv[0], &endptr, 10);
    if (endptr == argv[0] || *endptr != '\0') {
        (void)fprintf(stderr, "Error: invalid ds_id\n");
        return 1;
    }

    enum mds_status st = cluster_transport_request_ds_remove(
        mds_host, mds_port, ds_id);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Error: ds remove failed (%d)\n", (int)st);
        return 1;
    }

    (void)printf("DS %u removed.\n", ds_id);
    return 0;
}


/* Patched DS commands (provision, patch-ready, patch-clear) removed.
 * Only generic (loosely coupled) DSes are supported. */

static int cmd_ds_validate(int argc, const char *const *argv)
{
    if (argc < 1) {
        (void)fprintf(stderr,
            "Usage: mds-admin ds validate <ds_id>"
            " [--tool-version <str>] [--note <str>]\n");
        return 1;
    }

    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    if (parse_admin_endpoint(argc - 1, argv + 1,
                             &mds_host, &mds_port, NULL) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    char *endptr = NULL;
    uint32_t ds_id = (uint32_t)strtoul(argv[0], &endptr, 10);
    if (endptr == argv[0] || *endptr != '\0') {
        (void)fprintf(stderr, "Error: invalid ds_id\n");
        return 1;
    }

    /* Parse optional flags. */
    const char *tool_ver = "manual";
    const char *note = "";
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--tool-version") == 0 && i + 1 < argc) {
            tool_ver = argv[++i];
        } else if (strcmp(argv[i], "--note") == 0 && i + 1 < argc) {
            note = argv[++i];
        }
    }

    /* Read DS to verify RDMA transport. */
    struct mds_ds_info *ds_list = NULL;
    uint32_t count = 0;
    enum mds_status st = cluster_transport_request_ds_list(
        mds_host, mds_port, &ds_list, &count);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Error: ds list failed\n");
        return 1;
    }

    bool found = false;
    bool has_rdma = false;
    for (uint32_t i = 0; i < count; i++) {
        if (ds_list[i].ds_id == ds_id) {
            found = true;
            has_rdma = (ds_list[i].transport & DS_TRANSPORT_RDMA) != 0;
            break;
        }
    }
    free(ds_list);

    if (!found) {
        (void)fprintf(stderr, "Error: DS %u not found\n", ds_id);
        return 1;
    }
    if (!has_rdma) {
        (void)fprintf(stderr,
            "Error: DS %u lacks RDMA transport\n", ds_id);
        return 1;
    }

    enum mds_status vst = cluster_transport_request_ds_validate(
        mds_host, mds_port, ds_id, tool_ver, note);
    if (vst != MDS_OK) {
        (void)fprintf(stderr, "Error: validate failed (%d)\n", (int)vst);
        return 1;
    }

    (void)printf("DS %u validated for GPUDirect (tool: %s).\n",
                 ds_id, tool_ver);
    return 0;
}

static int cmd_ds_validate_clear(int argc, const char *const *argv)
{
    if (argc < 1) {
        (void)fprintf(stderr,
            "Usage: mds-admin ds validate-clear <ds_id>\n");
        return 1;
    }

    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    if (parse_admin_endpoint(argc - 1, argv + 1,
                             &mds_host, &mds_port, NULL) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    uint32_t ds_id = (uint32_t)strtoul(argv[0], NULL, 10);
    enum mds_status st = cluster_transport_request_ds_validate_clear(
        mds_host, mds_port, ds_id);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Error: validate-clear failed (%d)\n",
                     (int)st);
        return 1;
    }
    (void)printf("DS %u GPUDirect validation cleared.\n", ds_id);
    return 0;
}

static int cmd_ds_validate_info(int argc, const char *const *argv)
{
    if (argc < 1) {
        (void)fprintf(stderr,
            "Usage: mds-admin ds validate-info <ds_id>\n");
        return 1;
    }

    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    if (parse_admin_endpoint(argc - 1, argv + 1,
                             &mds_host, &mds_port, NULL) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    uint32_t ds_id = (uint32_t)strtoul(argv[0], NULL, 10);

    struct mds_ds_info *ds_list = NULL;
    uint32_t count = 0;
    enum mds_status st = cluster_transport_request_ds_list(
        mds_host, mds_port, &ds_list, &count);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "Error: ds list failed\n");
        return 1;
    }

    bool found = false;
    for (uint32_t i = 0; i < count; i++) {
        if (ds_list[i].ds_id == ds_id) {
            found = true;
            (void)printf("DS %u validation info:\n", ds_id);
            (void)printf("  capabilities: 0x%x\n",
                         ds_list[i].capabilities);
            (void)printf("  gpudirect:    %s\n",
                         "not validated");
            (void)printf("  export-mode:  %s\n",
                         "not verified");
            break;
        }
    }
    free(ds_list);
    if (!found) {
        (void)fprintf(stderr, "Error: DS %u not found\n", ds_id);
        return 1;
    }
    return 0;
}


static int cmd_metrics(int argc, const char *const *argv)
{
    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    if (parse_admin_endpoint(argc, argv,
                             &mds_host, &mds_port, NULL) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    /* Connect and request metrics snapshot. */
    int fd = -1;
    /* Use a simple TCP request like other admin commands. */
    {
        struct mds_ds_info *dummy = NULL;
        uint32_t dummy_count = 0;
        /* Reuse the ds list connection pattern for metrics. */
        (void)dummy;
        (void)dummy_count;
    }

    /* Query daemon metrics via transport. */
    int conn_fd = -1;
    {
        /* Use a raw TCP connection to the admin port. */
        struct sockaddr_in sa;
        memset(&sa, 0, sizeof(sa));
        sa.sin_family = AF_INET;
        sa.sin_port = htons(mds_port);
        if (inet_pton(AF_INET, mds_host, &sa.sin_addr) != 1) {
            (void)fprintf(stderr, "Error: invalid host\n");
            return 1;
        }
        conn_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (conn_fd < 0 ||
            connect(conn_fd, (struct sockaddr *)&sa,
                    sizeof(sa)) != 0) {
            if (conn_fd >= 0) { close(conn_fd); }
            /* Fallback to local metrics if daemon not reachable. */
            goto local_fallback;
        }
    }

    /* Send metrics request. */
    {
        uint8_t hdr[5];
        hdr[0] = 69;  /* CT_MSG_METRICS_REQ */
        uint32_t len_be = 0;
        memcpy(hdr + 1, &len_be, 4);
        if (write(conn_fd, hdr, 5) != 5) {
            close(conn_fd);
            goto local_fallback;
        }
    }

    /* Read response. */
    {
        uint8_t resp_hdr[5];
        if (read(conn_fd, resp_hdr, 5) != 5 || resp_hdr[0] != 70) {
            close(conn_fd);
            goto local_fallback;
        }
        uint32_t resp_len_be;
        memcpy(&resp_len_be, resp_hdr + 1, 4);
        uint32_t resp_len = be32toh(resp_len_be);
        if (resp_len >= 50) {  /* 6*8 + 2 = 50 bytes */
            uint8_t mbuf[50];
            if (read(conn_fd, mbuf, 50) == 50) {
                struct mds_metrics_snapshot snap;
                size_t mo = 0;
                uint64_t fv;
                memcpy(&fv,mbuf+mo,8); mo+=8;
                snap.repl_deltas_sent = be64toh(fv);
                memcpy(&fv,mbuf+mo,8); mo+=8;
                snap.repl_bytes_sent = be64toh(fv);
                memcpy(&fv,mbuf+mo,8); mo+=8;
                snap.cat_commits_ok = be64toh(fv);
                memcpy(&fv,mbuf+mo,8); mo+=8;
                snap.cat_commits_fail = be64toh(fv);
                memcpy(&fv,mbuf+mo,8); mo+=8;
                snap.cat_flush_ns_sum = be64toh(fv);
                memcpy(&fv,mbuf+mo,8); mo+=8;
                snap.cat_flush_count = be64toh(fv);
                snap.repl_health_ok = mbuf[mo++];
                snap.repl_writes_blocked = mbuf[mo++];
                close(conn_fd);
                char buf[4096];
                int n = mds_metrics_prometheus(&snap, buf,
                                               sizeof(buf));
                if (n > 0) { (void)printf("%s", buf); }
                return 0;
            }
        }
        close(conn_fd);
    }

local_fallback: ;
    /* Fallback: show local CLI-process metrics. */
    struct mds_metrics_snapshot snap = mds_metrics_snapshot();
    char buf[4096];
    int n = mds_metrics_prometheus(&snap, buf, sizeof(buf));
    if (n > 0) {
        (void)printf("%s", buf);
    } else {
        (void)printf("(no metrics available)\n");
    }
    (void)fd;
    return 0;
}

/* Forward declarations for DS import/reattach/scan. */
static int cmd_ds_import(int argc, const char *const argv[]);
static int cmd_ds_reattach(int argc, const char *const argv[]);
static int cmd_ds_scan(int argc, const char *const argv[]);

/* -----------------------------------------------------------------------
 * C2: config show [<key>] / ds capacity show [--json]
 * ----------------------------------------------------------------------- */

/* Walk `line` to trim whitespace and split at '='.  Returns 0 on
 * success and sets *out_k / *out_v to in-buffer pointers (buffer
 * is mutated in place).  Returns -1 when no '=' is found. */
static int parse_kv_line(char *line, char **out_k, char **out_v)
{
    char *eq = strchr(line, '=');
    if (eq == NULL) { return -1; }
    char *k_end = eq;
    while (k_end > line && (k_end[-1] == ' ' || k_end[-1] == '\t')) {
        k_end--;
    }
    *k_end = '\0';
    char *v = eq + 1;
    while (*v == ' ' || *v == '\t') { v++; }
    *out_k = line;
    *out_v = v;
    return 0;
}

/* Emit the key=value dump from @text as a JSON object. */
static void config_show_emit_json(char *text)
{
    (void)printf("{\n");
    bool first = true;
    char *line = text;
    while (line != NULL && *line != '\0') {
        char *eol = strchr(line, '\n');
        if (eol != NULL) { *eol = '\0'; }
        char *k, *v;
        if (parse_kv_line(line, &k, &v) == 0) {
            (void)printf("%s  \"%s\": \"%s\"",
                         first ? "" : ",\n", k, v);
            first = false;
        }
        if (eol == NULL) { break; }
        line = eol + 1;
    }
    (void)printf("\n}\n");
}

/* Pick the first positional (non-flag, non-flag-value) argument. */
static const char *pick_filter_arg(int argc, const char *const argv[])
{
    for (int i = 0; i < argc; i++) {
        if (argv[i][0] == '-' || argv[i][0] == '\0') { continue; }
        if (i > 0 &&
            (strcmp(argv[i - 1], "--mds-host") == 0 ||
             strcmp(argv[i - 1], "--mds-port") == 0)) {
            continue;
        }
        return argv[i];
    }
    return NULL;
}

static int cmd_config_show(int argc, const char *const argv[])
{
    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    bool json = false;
    if (parse_admin_endpoint(argc, argv, &mds_host, &mds_port,
                             &json) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    const char *filter = pick_filter_arg(argc, argv);

    char *text = NULL;
    enum mds_status st = cluster_transport_request_config_show(
        mds_host, mds_port, filter, &text);
    if (st != MDS_OK) {
        (void)fprintf(stderr,
            "Error: config show failed (%d) -- is the daemon "
            "running and has --mds-port right?\n", (int)st);
        return 1;
    }

    if (json) {
        config_show_emit_json(text);
    } else {
        (void)printf("%s", text);
    }
    free(text);
    return 0;
}

static int cmd_ds_set_weight(int argc, const char *const argv[])
{
    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    if (parse_admin_endpoint(argc, argv, &mds_host, &mds_port, NULL) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    /* Positional args: <ds_id> <weight>. */
    const char *id_arg = NULL, *w_arg = NULL;
    for (int i = 0; i < argc; i++) {
        if (argv[i][0] == '-' || argv[i][0] == '\0') { continue; }
        if (i > 0 &&
            (strcmp(argv[i - 1], "--mds-host") == 0 ||
             strcmp(argv[i - 1], "--mds-port") == 0)) {
            continue;
        }
        if (id_arg == NULL) { id_arg = argv[i]; }
        else if (w_arg == NULL) { w_arg = argv[i]; }
    }
    if (id_arg == NULL || w_arg == NULL) {
        (void)fprintf(stderr,
            "Usage: mds-admin ds set-weight <ds_id> <weight> "
            "[--mds-host H] [--mds-port P]\n");
        return 1;
    }

    unsigned long id = strtoul(id_arg, NULL, 10);
    unsigned long w = strtoul(w_arg, NULL, 10);
    if (id >= MDS_MAX_DS_NODES || w > UINT32_MAX) {
        (void)fprintf(stderr, "Error: out of range\n");
        return 1;
    }

    enum mds_status st = cluster_transport_request_ds_set_weight(
        mds_host, mds_port, (uint32_t)id, (uint32_t)w);
    if (st != MDS_OK) {
        (void)fprintf(stderr,
            "Error: ds set-weight failed (%d)%s\n", (int)st,
            st == MDS_ERR_NOTFOUND ? " -- ds_id not in DS cache" : "");
        return 1;
    }
    (void)printf("DS %lu weight set to %lu (runtime only; "
                 "persist via ds_weight.%lu=%lu in INI).\n",
                 id, w, id, w);
    return 0;
}

static int cmd_ds_capacity_probe_now(int argc, const char *const argv[])
{
    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    if (parse_admin_endpoint(argc, argv, &mds_host, &mds_port, NULL) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }
    uint32_t probed = 0;
    enum mds_status st = cluster_transport_request_ds_capacity_probe(
        mds_host, mds_port, &probed);
    if (st != MDS_OK) {
        (void)fprintf(stderr,
            "Error: ds capacity probe-now failed (%d)\n", (int)st);
        return 1;
    }
    (void)printf("Probed %u DS%s.\n", (unsigned)probed,
                 probed == 1 ? "" : "es");
    return 0;
}

static int cmd_ds_capacity_show(int argc, const char *const argv[])
{
    const char *mds_host = NULL;
    uint16_t mds_port = 0;
    bool json = false;
    if (parse_admin_endpoint(argc, argv, &mds_host, &mds_port,
                             &json) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    struct ds_capacity_record *recs = NULL;
    uint32_t count = 0;
    enum mds_status st = cluster_transport_request_ds_capacity_show(
        mds_host, mds_port, &recs, &count);
    if (st != MDS_OK) {
        (void)fprintf(stderr,
            "Error: ds capacity show failed (%d)\n", (int)st);
        return 1;
    }

    if (json) {
        (void)printf("[\n");
        for (uint32_t i = 0; i < count; i++) {
            (void)printf(
                "  { \"ds_id\": %u, \"state\": \"%s\","
                " \"total_bytes\": %llu, \"used_bytes\": %llu,"
                " \"avail_bytes\": %llu, \"weight\": %u,"
                " \"auto_weight\": %u }%s\n",
                (unsigned)recs[i].ds_id,
                ds_state_name(recs[i].state),
                (unsigned long long)recs[i].total_bytes,
                (unsigned long long)recs[i].used_bytes,
                (unsigned long long)(recs[i].total_bytes >
                                     recs[i].used_bytes ?
                    recs[i].total_bytes - recs[i].used_bytes : 0),
                (unsigned)recs[i].weight,
                (unsigned)recs[i].auto_weight,
                (i + 1 < count) ? "," : "");
        }
        (void)printf("]\n");
    } else {
        (void)printf("%-6s  %-11s  %-18s  %-18s  %-18s  %-6s  %s\n",
                     "DS", "STATE", "TOTAL", "USED", "AVAIL",
                     "WEIGHT", "AUTO");
        for (uint32_t i = 0; i < count; i++) {
            uint64_t avail = recs[i].total_bytes > recs[i].used_bytes
                ? recs[i].total_bytes - recs[i].used_bytes : 0;
            (void)printf("%-6u  %-11s  %-18llu  %-18llu  %-18llu  %-6u  %u\n",
                         (unsigned)recs[i].ds_id,
                         ds_state_name(recs[i].state),
                         (unsigned long long)recs[i].total_bytes,
                         (unsigned long long)recs[i].used_bytes,
                         (unsigned long long)avail,
                         (unsigned)recs[i].weight,
                         (unsigned)recs[i].auto_weight);
        }
    }
    free(recs);
    return 0;
}

static int dispatch_config(int argc, const char *const argv[])
{
    if (argc < 3) { return -1; }
    if (strcmp(argv[2], "show") == 0) {
        return cmd_config_show(argc - 3, argv + 3);
    }
    return -1;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static int dispatch_ds(int argc, const char *const argv[])
{
    if (argc < 3) { return -1; }
    if (strcmp(argv[2], "list") == 0) {
        return cmd_ds_list(argc - 3, (const char *const *)(argv + 3));
    }
    if (strcmp(argv[2], "add") == 0 && argc >= 6) {
        return cmd_ds_add(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "set-state") == 0 && argc >= 5) {
        return cmd_ds_set_state(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "remove") == 0 && argc >= 4) {
        return cmd_ds_remove(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "capacity") == 0 && argc >= 4 &&
        strcmp(argv[3], "show") == 0) {
        return cmd_ds_capacity_show(argc - 4, argv + 4);
    }
    if (strcmp(argv[2], "capacity") == 0 && argc >= 4 &&
        strcmp(argv[3], "probe-now") == 0) {
        return cmd_ds_capacity_probe_now(argc - 4, argv + 4);
    }
    if (strcmp(argv[2], "set-weight") == 0 && argc >= 5) {
        return cmd_ds_set_weight(argc - 3, argv + 3);
    }
    /* patch-ready, patch-clear, provision, provision-info removed
     * (patched DS mode no longer supported). */
    if (strcmp(argv[2], "validate") == 0 && argc >= 4) {
        return cmd_ds_validate(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "validate-clear") == 0 && argc >= 4) {
        return cmd_ds_validate_clear(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "validate-info") == 0 && argc >= 4) {
        return cmd_ds_validate_info(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "import") == 0 && argc >= 5) {
        return cmd_ds_import(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "reattach") == 0 && argc >= 4) {
        return cmd_ds_reattach(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "scan") == 0 && argc >= 4) {
        return cmd_ds_scan(argc - 3, argv + 3);
    }
    return -1;
}

static int dispatch_upgrade(int argc, char *argv[])
{
    if (argc < 3) { return -1; }
    if (strcmp(argv[2], "failover") == 0 && argc >= 4) {
        return cmd_upgrade_failover(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "status") == 0) {
        return cmd_upgrade_status(argc - 3, argv + 3);
    }
    return -1;
}


/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    /* Global flags: --help / --version */
    if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
        usage(argv[0]);
        return EXIT_SUCCESS;
    }
    if (strcmp(argv[1], "-V") == 0 || strcmp(argv[1], "--version") == 0) {
        printf("mds-admin %s (commit %s)\n",
               PNFS_MDS_VERSION, PNFS_MDS_GIT_COMMIT);
        return EXIT_SUCCESS;
    }

    int rc = -1;
    const char *group = argv[1];

    if (strcmp(group, "subtree") == 0) {
        rc = dispatch_subtree(argc, argv);
    } else if (strcmp(group, "node") == 0) {
        rc = dispatch_node(argc, argv);
    } else if (strcmp(group, "standby") == 0) {
        if (argc >= 5 && strcmp(argv[2], "attach") == 0) {
            rc = cmd_standby_attach(argc - 3, argv + 3);
        }
    } else if (strcmp(group, "migration") == 0) {
        if (argc >= 3 && strcmp(argv[2], "progress") == 0) {
            rc = cmd_migration_progress(argc - 3, argv + 3);
        }
    } else if (strcmp(group, "cluster") == 0) {
        if (argc >= 3 && strcmp(argv[2], "status") == 0) {
            rc = cmd_cluster_status(argc - 3, argv + 3);
        }
    } else if (strcmp(group, "status") == 0) {
        rc = cmd_status(argc - 2, argv + 2);
    } else if (strcmp(group, "resilver") == 0) {
        rc = dispatch_resilver(argc, argv);
    } else if (strcmp(group, "tier") == 0) {
        rc = dispatch_tier(argc, argv);
    } else if (strcmp(group, "quota") == 0) {
        rc = dispatch_quota(argc, argv);
    } else if (strcmp(group, "rebalance") == 0) {
        rc = dispatch_rebalance(argc, argv);
    } else if (strcmp(group, "backup") == 0) {
        rc = dispatch_backup(argc, argv);
    } else if (strcmp(group, "metrics") == 0) {
        rc = cmd_metrics(argc - 2,
            (const char *const *)(argv + 2));
    } else if (strcmp(group, "ds") == 0) {
        rc = dispatch_ds(argc, (const char *const *)argv);
    } else if (strcmp(group, "upgrade") == 0) {
        rc = dispatch_upgrade(argc, argv);
    } else if (strcmp(group, "config") == 0) {
        rc = dispatch_config(argc, (const char *const *)argv);
    }

    if (rc < 0) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }
    return rc;
}


/* -----------------------------------------------------------------------
 * DS import / reattach / scan (deprecated)
 * ----------------------------------------------------------------------- */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"

#include <dirent.h>
#include <sys/stat.h>

static int cmd_ds_import(int argc, const char *const argv[])
{
    (void)argc;
    (void)argv;
    (void)fprintf(stderr, "ERROR: ds import requires catalogue (not implemented)\n");
    return EXIT_FAILURE;
}

static int cmd_ds_reattach(int argc, const char *const argv[])
{
    (void)argc;
    (void)argv;
    (void)fprintf(stderr,
        "ERROR: ds reattach requires catalogue (not implemented)\n");
    return EXIT_FAILURE;
}

static int cmd_ds_scan(int argc, const char *const argv[])
{
    (void)argc;
    (void)argv;
    (void)fprintf(stderr,
        "ERROR: ds scan requires catalogue (not implemented)\n");
    return EXIT_FAILURE;
}

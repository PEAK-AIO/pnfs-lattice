/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * config.c -- INI-style configuration parser.
 *
 * Populates struct mds_config from /etc/pnfs-mds/mds.conf.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>

#include "pnfs_mds.h"

/* Maximum config file line length */
#define CFG_LINE_MAX 512

/* -----------------------------------------------------------------------
 * Workload tuning profiles
 *
 * Each profile sets a coherent bundle of tuning parameters for a
 * specific workload class.  The "default" profile is a no-op (sets=0)
 * so existing auto-sizing and CQ-internal defaults still apply.
 *
 * Profile values are applied after initial defaults but before
 * explicit config keys, so operator overrides always win.
 * ----------------------------------------------------------------------- */

struct mds_tuning_profile {
    const char *name;
    uint64_t   sets;   /* MDS_CFG_SET_* bits for fields this profile touches */
    enum mds_workload_profile id;
    uint32_t   worker_threads;
    uint32_t   prealloc_pool_size;
    uint32_t   commit_batch_size;
    uint32_t   commit_flush_ms;
    uint32_t   commit_batch_max_bytes;
    uint32_t   commit_queue_depth;
    uint32_t   stripe_unit_bytes;
    uint32_t   inline_max_size;
    uint32_t   ds_prepare_queue_depth;
    uint32_t   lease_time_sec;
    bool       inline_enabled;
    /* Phase 1: DS placement policy for new files' layouts.  Only
     * consulted when cfg.placement_policy_enabled is true. */
    enum mds_placement_policy placement_policy;
    /* Phase 3: default stripe geometry.  Both default to 1.
     * Effective only when placement_policy_enabled is true. */
    uint32_t   default_stripe_count;
    uint32_t   default_mirror_count;
};

#define ALL_TUNING_BITS ( \
    MDS_CFG_SET_WORKER_THREADS | MDS_CFG_SET_PREALLOC_POOL_SIZE | \
    MDS_CFG_SET_COMMIT_BATCH_SIZE | MDS_CFG_SET_COMMIT_FLUSH_MS | \
    MDS_CFG_SET_COMMIT_BATCH_MAX_BYTES | MDS_CFG_SET_COMMIT_QUEUE_DEPTH | \
    MDS_CFG_SET_STRIPE_UNIT_BYTES | MDS_CFG_SET_INLINE_ENABLED | \
    MDS_CFG_SET_INLINE_MAX_SIZE | MDS_CFG_SET_DS_PREPARE_QUEUE_DEPTH | \
    MDS_CFG_SET_LEASE_TIME_SEC | MDS_CFG_SET_PLACEMENT_POLICY | \
    MDS_CFG_SET_DEFAULT_STRIPE_COUNT | MDS_CFG_SET_DEFAULT_MIRROR_COUNT)

static const struct mds_tuning_profile g_profiles[] = {
    /* default: no bits set -> auto-sizing and CQ internal defaults apply.
     * stripe_count=1, mirror_count=1 means no multi-DS layout. */
    /* name, sets, id, workers, prealloc, batch, flush, max_bytes,
     * queue, stripe, inline_max, ds_prep_q, lease, inline_en,
     * placement_policy, default_stripe_count, default_mirror_count */
    { "default", 0, MDS_PROFILE_DEFAULT,
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, PLACEMENT_RR, 1, 1 },

    /* hpc: MPI jobs, massive concurrent creates, large sequential files.
     * Inline stays enabled at 4 KiB for small marker/log/control files.
     * CAPACITY placement + stripe_count=4 targets multi-DS bandwidth. */
    { "hpc", ALL_TUNING_BITS, MDS_PROFILE_HPC,
      32, 4096, 512, 1, 4194304, 8192, 1048576, 4096, 8192, 90, true,
      PLACEMENT_CAPACITY, 4, 1 },

    /* ai_training: checkpoint-biased.  Burst creates for model saves,
     * large writes, mixed with read-heavy data loading.  stripe=4
     * matches hpc's bandwidth ambition on modern checkpoints. */
    { "ai_training", ALL_TUNING_BITS, MDS_PROFILE_AI_TRAINING,
      32, 2048, 256, 2, 2097152, 8192, 1048576, 4096, 4096, 90, true,
      PLACEMENT_CAPACITY, 4, 1 },

    /* genomics: bioinformatics pipelines -- many small files (FASTQ
     * chunks, BAM shards), high metadata rate, moderate file sizes.
     * WEIGHTED_RR + mirror=2 prioritises durability and load balance
     * on per-file reads across replicas. */
    { "genomics", ALL_TUNING_BITS, MDS_PROFILE_GENOMICS,
      24, 1024, 128, 1, 1048576, 4096, 65536, 16384, 4096, 90, true,
      PLACEMENT_WEIGHTED_RR, 1, 2 },

    /* media: video/render farms -- few very large files, low metadata
     * rate.  stripe=8 to push concurrent bandwidth; clamps to
     * min(stripe_count, ds_count) when the cluster has fewer DSes
     * (D5 fallback, bumps pnfs_mds_placement_degraded_total). */
    { "media", ALL_TUNING_BITS, MDS_PROFILE_MEDIA,
      16, 256, 64, 5, 1048576, 2048, 4194304, 4096, 2048, 120, true,
      PLACEMENT_CAPACITY, 8, 1 },
};

#define PROFILE_COUNT (sizeof(g_profiles) / sizeof(g_profiles[0]))

static const struct mds_tuning_profile *find_profile(const char *name)
{
    for (size_t i = 0; i < PROFILE_COUNT; i++) {
        if (strcmp(g_profiles[i].name, name) == 0) {
            return &g_profiles[i];
        }
    }
    return NULL;
}

static void apply_profile(struct mds_config *cfg,
                          const struct mds_tuning_profile *p)
{
    if (p == NULL || p->sets == 0) {
        return;
    }
    if (p->sets & MDS_CFG_SET_WORKER_THREADS) {
        cfg->worker_threads = p->worker_threads;
    }
    if (p->sets & MDS_CFG_SET_PREALLOC_POOL_SIZE) {
        cfg->prealloc_pool_size = p->prealloc_pool_size;
    }
    if (p->sets & MDS_CFG_SET_COMMIT_BATCH_SIZE) {
        cfg->commit_batch_size = p->commit_batch_size;
    }
    if (p->sets & MDS_CFG_SET_COMMIT_FLUSH_MS) {
        cfg->commit_flush_ms = p->commit_flush_ms;
    }
    if (p->sets & MDS_CFG_SET_COMMIT_BATCH_MAX_BYTES) {
        cfg->commit_batch_max_bytes = p->commit_batch_max_bytes;
    }
    if (p->sets & MDS_CFG_SET_COMMIT_QUEUE_DEPTH) {
        cfg->commit_queue_depth = p->commit_queue_depth;
    }
    if (p->sets & MDS_CFG_SET_STRIPE_UNIT_BYTES) {
        cfg->stripe_unit_bytes = p->stripe_unit_bytes;
    }
    if (p->sets & MDS_CFG_SET_INLINE_ENABLED) {
        cfg->inline_enabled = p->inline_enabled;
    }
    if (p->sets & MDS_CFG_SET_INLINE_MAX_SIZE) {
        cfg->inline_max_size = p->inline_max_size;
    }
    if (p->sets & MDS_CFG_SET_DS_PREPARE_QUEUE_DEPTH) {
        cfg->ds_prepare_queue_depth = p->ds_prepare_queue_depth;
    }
    if (p->sets & MDS_CFG_SET_LEASE_TIME_SEC) {
        cfg->lease_time_sec = p->lease_time_sec;
    }
    if (p->sets & MDS_CFG_SET_PLACEMENT_POLICY) {
        cfg->placement_policy = p->placement_policy;
    }
    if (p->sets & MDS_CFG_SET_DEFAULT_STRIPE_COUNT) {
        cfg->default_stripe_count = p->default_stripe_count;
    }
    if (p->sets & MDS_CFG_SET_DEFAULT_MIRROR_COUNT) {
        cfg->default_mirror_count = p->default_mirror_count;
    }
    cfg->workload_profile = p->id;
    cfg->tuning_set |= p->sets;
}

static char *strip_whitespace(char *s)
{
    while (isspace((unsigned char)*s)) {
        s++;
}
    if (*s == '\0') {
        return s;
}
    char *end = s + strlen(s) - 1;
    while (end > s && isspace((unsigned char)*end)) {
        *end-- = '\0';
}
    return s;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status mds_config_load(const char *path, struct mds_config *cfg)
{
    FILE *fp;
    char line[CFG_LINE_MAX];
    char selected_profile[CFG_LINE_MAX];
    bool have_profile = false;

    if (path == NULL || cfg == NULL) {
        return MDS_ERR_INVAL;
    }

    memset(cfg, 0, sizeof(*cfg));

    /* Set safe defaults */
    cfg->self.id = 1;
    (void)snprintf(cfg->self.hostname, sizeof(cfg->self.hostname), "localhost");
    cfg->self.nfs_port = 2049;
    cfg->self.grpc_port = 50051;
    cfg->catalogue_backend = MDS_BACKEND_RONDB;
    cfg->worker_threads = 16;
    cfg->max_inflight_per_conn = 0;  /* 0 = RPC_DEFAULT_MAX_INFLIGHT (8) */
    cfg->repl_mode = MDS_REPL_SYNC;
    cfg->self_role = 0;                 /* NODE_ACTIVE */
    cfg->self_failover_partner_id = 0;  /* no failover partner */
    cfg->ds_heartbeat_ms = 5000;
    cfg->stripe_unit_bytes = 65536;
    cfg->auto_widen_lease_on_4k = true;
    cfg->layout_grant_max_length_bytes = (1ULL << 36); /* 64 GiB */
    cfg->default_stripe_count = 1;
    cfg->default_mirror_count = 1;
    cfg->lease_time_sec = 90;
    cfg->grace_period_sec = 90;
    cfg->prealloc_pool_size = 128;
    cfg->ds_getdev_transport = 0;  /* FF_TRANSPORT_TCP: advertise tcp in GETDEVICEINFO */
    cfg->ds_rdma_port = 20049;     /* RDMA port advertised when ds_transport=rdma|both */
    cfg->prealloc_ring_count = 0;   /* 0 = ds_prealloc engine default. */
    cfg->repl_health_interval_ms = 5000;
    cfg->repl_refuse_writes_on_resync = false;
    cfg->repl_listen_port = 9401;        /* S11.4 default */
    cfg->gpudirect_required = false;
    cfg->nfs_auth_mode = NFS_AUTH_MODE_SYS;
    cfg->krb5_keytab_path[0] = '\0';
    cfg->krb5_principal[0] = '\0';
    cfg->cluster_ca_file[0] = '\0';
    cfg->node_cert_file[0] = '\0';
    cfg->node_key_file[0] = '\0';
    cfg->require_mtls = false;
    (void)snprintf(cfg->cluster_bind_addr, sizeof(cfg->cluster_bind_addr),
             "127.0.0.1");
    cfg->cluster_allowed_peer_count = 0;
    cfg->cluster_max_conns = 16;
    cfg->admin_allowed_host_count = 0;
    (void)snprintf(cfg->ds_mount_path_fmt, sizeof(cfg->ds_mount_path_fmt),
             "/mnt/ds%%u");

    /* DS prepare defaults (Phase 6) */
    cfg->ds_prepare_queue_depth = 0; /* 0 = default 4096 */
    cfg->ds_prepare_workers = 0;     /* 0 = 1 per DS */

    /* DS GC drainer parallelism (clamped by ds_gc_start_ex). */
    cfg->ds_gc_workers = 4;
    cfg->ds_gc_batch_size = 256;

    /* Sharding defaults (Tier 3 Phase 3) */
    cfg->shard_enabled = false;

    /* Cosmetic: keep referral junctions visible in READDIR by default. */
    cfg->hide_referral_junctions = false;

    /* POSIX DAC enforcement for AUTH_SYS requests (default OFF --
     * opt in with posix_dac=true; see pnfs_mds.h). */
    cfg->posix_dac = false;

    /* Transient state caching (default: off -- open/layout state
     * write-through to RonDB for cross-MDS correctness). */
    cfg->transient_state_cache = false;

    /* Stored synthetic DS owner (RFC 8435 S2.2).  Default off -> legacy
     * owner-aligned chown-on-LAYOUTGET path. */
    cfg->ds_synth_owner = false;

    /* Live DS capacity probe interval (ms).  60s by default. */
    cfg->ds_capacity_poll_ms = 60000;

    /* Phase B2: capacity-derived auto-weighting defaults OFF so a
     * fresh upgrade reproduces today's placement behaviour. */
    cfg->placement_capacity_weighting = CAP_WEIGHT_OFF;

    /* Inline-data / cache sizes -- previously had struct fields but
     * no INI parser, so operators could not actually tune them.
     * Apply sane defaults here; INI keys override below. */
    cfg->inline_max_size = 65536;          /* 64 KiB */
    cfg->inode_cache_size = 0;             /* 0 = disabled; set >0 to enable */
    cfg->dirent_cache_size = 0;     /* disabled by default: per-MDS namespace caches cannot stay coherent across the referral cluster */
    cfg->layout_cache_size = 0;     /* disabled by default */
    cfg->negative_cache_ttl_ms = 5000;

    /* Promoted-from-hardcode defaults. */
    cfg->ds_health_fail_threshold = 6;
    cfg->cb_recall_timeout_ms = 5000;
    cfg->dir_deleg_recall_timeout_ms = 5000;
    cfg->metrics_http_port = 9090;
    cfg->metrics_op_enabled = true;
    cfg->compound_perf_threshold_us = 0; /* disabled; was 2000 hardcoded */

    /* mountd compatibility responder (`showmount -e`).  Enabled
     * by default; binds UDP+TCP on 0.0.0.0:20048, registers with the
     * local rpcbind, and advertises a single synthetic export "/".
     * No DS interaction.  The MNT procedure is rejected at the RPC
     * layer with PROC_UNAVAIL so on-by-default still cannot expose
     * the MDS as an NFSv3 mount target.  Operators who want the
     * shim off (no extra port, no rpcbind entry) set
     * `mountd_compat_enabled = false` in mds.conf. */
    cfg->mountd_compat_enabled = true;
    cfg->mountd_compat_port = 20048;
    cfg->mountd_compat_register_rpcbind = true;
    (void)snprintf(cfg->mountd_compat_bind_addr,
                   sizeof(cfg->mountd_compat_bind_addr), "0.0.0.0");
    cfg->mountd_compat_export_count = 0;  /* 0 -> responder uses "/" */

    /* Directory delegations (Phase 8b) default off. */
    cfg->dir_delegations_enabled = false;
    /* File delegations (RFC 8881 S10.4) default ON.  Operators who
     * want zero CB_RECALL traffic (e.g. lab harnesses with
     * clientaddr=0.0.0.0) flip the INI key to false; main.c then
     * leaves rpc_cfg.dt = NULL and op_open() short-circuits the
     * deleg-grant arm via cd->dt == NULL. */
    cfg->file_delegations_enabled = false;

    /* AutoSplit defaults (Tier 3 Phase 1) */
    cfg->auto_split_enabled = false;
    cfg->auto_split_execute = false;
    cfg->auto_split_threshold = 10000;
    cfg->auto_split_interval = 300;
    cfg->auto_split_cooldown = 600;
    cfg->auto_split_sustained = 2;
    cfg->auto_split_min_children = 4;

    /*
     * Phase C of docs/hpc-nto1-plan.md -- wide-stripe HPC defaults.
     * memset() above already left hpc_xdr_form as MDS_HPC_XDR_FORM_AUTO
     * (= 0); set the stripe-count cap explicitly so the daemon falls
     * back to 128 when the operator omits the key.  The cap stays
     * compile-bounded by MDS_MAX_STRIPES (1024) below in the parser.
     */
    cfg->hpc_max_stripe_count = 128;

    /* Stripe lease duration (default 30s). */
    cfg->stripe_lease_duration_ms = 30000;

    /* Logging defaults: stderr output, INFO global verbosity, and every
     * component inheriting the global level (-1 sentinel).  memset()
     * above zeroed log_level_global to LOG_FATAL, so set INFO here. */
    cfg->log_file[0] = '\0';
    cfg->log_level_global = LOG_INFO;
    for (int i = 0; i < LOG_COMP_COUNT; i++) {
        cfg->log_level_by_component[i] = -1;
    }

    fp = fopen(path, "r");
    if (fp == NULL) {
        return MDS_ERR_IO;
    }
    selected_profile[0] = '\0';

    /* First pass: scan for workload_profile and remember the last
     * one so duplicate keys follow normal last-key-wins behavior.
     * Apply it before explicit keys so operator overrides win. */
    while (fgets(line, sizeof(line), fp) != NULL) {
        char *s = strip_whitespace(line);
        if (*s == '#' || *s == ';' || *s == '\0' || *s == '[') {
            continue;
        }
        char *eq = strchr(s, '=');
        if (eq == NULL) {
            continue;
        }
        *eq = '\0';
        const char *pk = strip_whitespace(s);
        const char *pv = strip_whitespace(eq + 1);
        if (strcmp(pk, "workload_profile") == 0) {
            (void)snprintf(selected_profile, sizeof(selected_profile),
                     "%s", pv);
            have_profile = true;
        }
    }
    if (have_profile) {
        const struct mds_tuning_profile *prof = find_profile(selected_profile);
        if (prof != NULL) {
            apply_profile(cfg, prof);
        } else {
            (void)fprintf(stderr,
                "WARN: unknown workload_profile '%s' "
                "(using defaults)\n", selected_profile);
        }
    }
    if (fseek(fp, 0, SEEK_SET) != 0) {
        (void)fclose(fp);
        return MDS_ERR_IO;
    }

    /* Second pass: parse all keys.  Explicit keys override profile. */
    while (fgets(line, sizeof(line), fp) != NULL) {
        char *s = strip_whitespace(line);

        /* Skip comments and empty lines */
        if (*s == '#' || *s == ';' || *s == '\0') {
            continue;
        }

        /* Skip section headers for now */
        if (*s == '[') {
            continue;
        }

        char *eq = strchr(s, '=');
        if (eq == NULL) {
            continue;
        }

        *eq = '\0';
        const char *key = strip_whitespace(s);
        char *val = strip_whitespace(eq + 1);

        /* Node identity */
        if (strcmp(key, "mds_id") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= UINT32_MAX) {
                cfg->self.id = (uint32_t)v;
}
        } else if (strcmp(key, "hostname") == 0) {
            (void)snprintf(cfg->self.hostname, sizeof(cfg->self.hostname),
                     "%s", val);
        } else if (strcmp(key, "nfs_port") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= 65535) {
                cfg->self.nfs_port = (uint16_t)v;
}
        } else if (strcmp(key, "grpc_port") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= 65535) {
                cfg->self.grpc_port = (uint16_t)v;
}

        /* Catalogue backend selection */
        } else if (strcmp(key, "workload_profile") == 0) {
            /* Already handled in first pass. */
        } else if (strcmp(key, "catalogue_backend") == 0) {
            if (strcmp(val, "rondb") == 0) {
#ifdef HAVE_RONDB
                cfg->catalogue_backend = MDS_BACKEND_RONDB;
#else
                (void)fprintf(stderr,
                    "ERROR: catalogue_backend=rondb but "
                    "binary built without ENABLE_RONDB\n");
                (void)fclose(fp);
                return MDS_ERR_INVAL;
#endif
            } else {
                (void)fprintf(stderr,
                    "ERROR: unknown catalogue_backend '%s' "
                    "(expected rondb)\n", val);
                (void)fclose(fp);
                return MDS_ERR_INVAL;
            }
        } else if (strcmp(key, "catalogue_backend_conf") == 0) {
            (void)snprintf(cfg->catalogue_backend_conf,
                sizeof(cfg->catalogue_backend_conf), "%s", val);

        /* Replication */
        } else if (strcmp(key, "standby_host") == 0) {
            (void)snprintf(cfg->standby_host, sizeof(cfg->standby_host),
                     "%s", val);
        } else if (strcmp(key, "standby_port") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= 65535) {
                cfg->standby_port = (uint16_t)v;
}
        } else if (strcmp(key, "repl_listen_port") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= 65535) {
                cfg->repl_listen_port = (uint16_t)v;
}
        } else if (strcmp(key, "repl_mode") == 0) {
            if (strcmp(val, "async") == 0) {
                cfg->repl_mode = MDS_REPL_ASYNC;
            } else if (strcmp(val, "semi_sync") == 0) {
                cfg->repl_mode = MDS_REPL_SEMI_SYNC;
            } else {
                cfg->repl_mode = MDS_REPL_SYNC;
}
        } else if (strcmp(key, "repl_semi_sync_n") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= UINT32_MAX) {
                cfg->repl_semi_sync_n = (uint32_t)v;
}

        } else if (strcmp(key, "repl_health_interval_ms") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= UINT32_MAX) {
                cfg->repl_health_interval_ms = (uint32_t)v;
}
        } else if (strcmp(key, "repl_refuse_writes_on_resync") == 0) {
            cfg->repl_refuse_writes_on_resync =
                (strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
        } else if (strcmp(key, "gpudirect_required") == 0) {
            cfg->gpudirect_required =
                (strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
        } else if (strcmp(key, "nfs_auth_mode") == 0) {
            if (strcmp(val, "sys") == 0) {
                cfg->nfs_auth_mode = NFS_AUTH_MODE_SYS;
            } else if (strcmp(val, "krb5") == 0) {
                cfg->nfs_auth_mode = NFS_AUTH_MODE_KRB5;
            } else if (strcmp(val, "krb5i") == 0) {
                cfg->nfs_auth_mode = NFS_AUTH_MODE_KRB5I;
            } else if (strcmp(val, "krb5p") == 0) {
                cfg->nfs_auth_mode = NFS_AUTH_MODE_KRB5P;
            } else {
                (void)fprintf(stderr,
                    "ERROR: invalid nfs_auth_mode '%s' "
                    "(expected sys|krb5|krb5i|krb5p)\n", val);
                /* Fatal-parse pattern: close the file and return a
                 * real mds_status (a bare -1 aliased MDS_ERR_NOMEM
                 * and leaked fp). */
                (void)fclose(fp);
                return MDS_ERR_INVAL;
            }
        } else if (strcmp(key, "krb5_keytab") == 0) {
            (void)snprintf(cfg->krb5_keytab_path,
                sizeof(cfg->krb5_keytab_path), "%s", val);
        } else if (strcmp(key, "krb5_principal") == 0) {
            (void)snprintf(cfg->krb5_principal,
                sizeof(cfg->krb5_principal), "%s", val);
        } else if (strcmp(key, "cluster_ca_file") == 0) {
            (void)snprintf(cfg->cluster_ca_file,
                sizeof(cfg->cluster_ca_file), "%s", val);
        } else if (strcmp(key, "node_cert_file") == 0) {
            (void)snprintf(cfg->node_cert_file,
                sizeof(cfg->node_cert_file), "%s", val);
        } else if (strcmp(key, "node_key_file") == 0) {
            (void)snprintf(cfg->node_key_file,
                sizeof(cfg->node_key_file), "%s", val);
        } else if (strcmp(key, "require_mtls") == 0) {
            cfg->require_mtls =
                (strcmp(val, "true") == 0 ||
                 strcmp(val, "1") == 0);

        /* Failover topology */
        } else if (strcmp(key, "self_role") == 0) {
            if (strcmp(val, "standby") == 0) {
                cfg->self_role = 1;  /* NODE_STANDBY */
            } else {
                cfg->self_role = 0;  /* NODE_ACTIVE (default) */
            }
        } else if (strcmp(key, "self_failover_partner_id") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= MDS_MAX_NODES) {
                cfg->self_failover_partner_id = (uint32_t)v;
            }

        /* Cluster */
        } else if (strcmp(key, "cluster_size") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= MDS_MAX_NODES) {
                cfg->cluster_size = (uint32_t)v;
}

        /* Tuning */
        } else if (strcmp(key, "worker_threads") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= 1024) {
                cfg->worker_threads = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_WORKER_THREADS;
}
        } else if (strcmp(key, "max_inflight_per_conn") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v >= 1 && v <= 1024) {
                cfg->max_inflight_per_conn = (uint32_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: max_inflight_per_conn=%lu out of range "
                    "(1..1024); using default\n", v);
            }
        } else if (strcmp(key, "ds_heartbeat_ms") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= UINT32_MAX) { /* 0 = disabled */
                cfg->ds_heartbeat_ms = (uint32_t)v;
}
        } else if (strcmp(key, "ds_transport") == 0) {
            if (strcmp(val, "rdma") == 0) {
                cfg->ds_getdev_transport = 1;       /* FF_TRANSPORT_RDMA */
            } else if (strcmp(val, "both") == 0) {
                cfg->ds_getdev_transport = 2;       /* FF_TRANSPORT_BOTH */
            } else {
                cfg->ds_getdev_transport = 0;       /* FF_TRANSPORT_TCP */
            }
        } else if (strcmp(key, "ds_rdma_port") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= 65535) {
                cfg->ds_rdma_port = (uint16_t)v;
            }
        } else if (strcmp(key, "stripe_unit_bytes") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            /* Bound before the uint32_t truncation: without the
             * upper check, 4294967296 silently became 0 and
             * 4294967297 became 1.  1 GiB is far above any sane
             * stripe unit (profiles use 64 KiB - 4 MiB). */
            if (v > 0 && v <= 1073741824UL) {
                cfg->stripe_unit_bytes = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_STRIPE_UNIT_BYTES;
} else {
                (void)fprintf(stderr,
                    "ERROR: stripe_unit_bytes=%s out of range "
                    "[1, 1073741824]\n", val);
                (void)fclose(fp);
                return MDS_ERR_INVAL;
            }
        } else if (strcmp(key, "auto_widen_lease_on_4k") == 0) {
            cfg->auto_widen_lease_on_4k =
                (strcmp(val, "true") == 0 ||
                 strcmp(val, "yes") == 0 ||
                 strcmp(val, "1") == 0);
        } else if (strcmp(key, "layout_grant_max_length_bytes") == 0) {
            uint64_t v = strtoull(val, NULL, 10);
            cfg->layout_grant_max_length_bytes =
                (v < 65536ULL) ? 65536ULL : v;
        } else if (strcmp(key, "default_mirror_count") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= MDS_MAX_MIRRORS) {
                cfg->default_mirror_count = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_DEFAULT_MIRROR_COUNT;
}
        } else if (strcmp(key, "default_stripe_count") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= MDS_MAX_STRIPES) {
                cfg->default_stripe_count = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_DEFAULT_STRIPE_COUNT;
} else {
                (void)fprintf(stderr,
                    "ERROR: default_stripe_count=%s out of range "
                    "[1, %u]\n", val, (unsigned)MDS_MAX_STRIPES);
                (void)fclose(fp);
                return MDS_ERR_INVAL;
            }
        } else if (strcmp(key, "lease_time_sec") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0) {
                cfg->lease_time_sec = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_LEASE_TIME_SEC;
}
        } else if (strcmp(key, "grace_period_sec") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= UINT32_MAX) { /* 0 = no grace period */
                cfg->grace_period_sec = (uint32_t)v;
}

        } else if (strcmp(key, "prealloc_pool_size") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= UINT32_MAX) {
                cfg->prealloc_pool_size = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_PREALLOC_POOL_SIZE;
}
        } else if (strcmp(key, "prealloc_ring_count") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= 64UL) {
                cfg->prealloc_ring_count = (uint32_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: prealloc_ring_count=%lu out of range "
                    "(0..64); ignoring\n", v);
            }

        /* Commit pipeline */
        } else if (strcmp(key, "CommitBatchSize") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= 4096) {
                cfg->commit_batch_size = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_COMMIT_BATCH_SIZE;
}
        } else if (strcmp(key, "CommitBatchMaxBytes") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= UINT32_MAX) {
                cfg->commit_batch_max_bytes = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_COMMIT_BATCH_MAX_BYTES;
}
        } else if (strcmp(key, "CommitFlushMs") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= 10000) {
                cfg->commit_flush_ms = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_COMMIT_FLUSH_MS;
}
        } else if (strcmp(key, "CommitQueueDepth") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= 65536) {
                cfg->commit_queue_depth = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_COMMIT_QUEUE_DEPTH;
}

        /* Cluster transport security */
        } else if (strcmp(key, "cluster_bind_addr") == 0) {
            (void)snprintf(cfg->cluster_bind_addr,
                     sizeof(cfg->cluster_bind_addr), "%s", val);
        } else if (strcmp(key, "cluster_max_conns") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= 256) {
                cfg->cluster_max_conns = (uint32_t)v;
}
        } else if (strncmp(key, "cluster_peer[", 13) == 0) {
            unsigned long idx = strtoul(key + 13, NULL, 10);
            if (idx < MDS_MAX_NODES) {
                (void)snprintf(cfg->cluster_allowed_peers[idx],
                         sizeof(cfg->cluster_allowed_peers[idx]),
                         "%s", val);
                if (idx + 1 > cfg->cluster_allowed_peer_count) {
                    cfg->cluster_allowed_peer_count = (uint32_t)(idx + 1);
}
            }
        } else if (strcmp(key, "admin_allowed_hosts") == 0) {
            /* Comma-separated list of IPv4 addresses. */
            char buf[CFG_LINE_MAX];
            (void)snprintf(buf, sizeof(buf), "%s", val);
            char *saveptr = NULL;
            char *tok = strtok_r(buf, ",", &saveptr);
            while (tok != NULL && cfg->admin_allowed_host_count < 32) {
                char *h = strip_whitespace(tok);
                if (*h != '\0') {
                    (void)snprintf(
                        cfg->admin_allowed_hosts[cfg->admin_allowed_host_count],
                        sizeof(cfg->admin_allowed_hosts[0]), "%s", h);
                    cfg->admin_allowed_host_count++;
                }
                tok = strtok_r(NULL, ",", &saveptr);
            }

        /* Data servers: ds_count and ds[N] = host:/export */
        } else if (strcmp(key, "ds_mount_path_fmt") == 0) {
            /* Validate: must contain exactly one %u, no other
             * format specifiers.  Reject anything else. */
            const char *p = val;
            int pct_u_count = 0;
            bool fmt_safe = true;
            while (*p != '\0') {
                if (*p == '%') {
                    p++;
                    if (*p == '%') { p++; continue; } /* %% literal */
                    if (*p == 'u') { pct_u_count++; p++; continue; }
                    fmt_safe = false; break;
                }
                p++;
            }
            if (fmt_safe && pct_u_count == 1) {
                (void)snprintf(cfg->ds_mount_path_fmt,
                         sizeof(cfg->ds_mount_path_fmt), "%s", val);
            } else {
                (void)fprintf(stderr, "WARN: ds_mount_path_fmt must contain"
                        " exactly one %%u and no other specifiers;"
                        " ignoring '%s'\n", val);
            }
        } else if (strcmp(key, "ds_count") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= MDS_MAX_DS_NODES) {
                cfg->ds_count = (uint32_t)v;
}
        } else if (strncmp(key, "ds[", 3) == 0) {
            /* Parse ds[N] = host:/export */
            unsigned long idx = strtoul(key + 3, NULL, 10);
            if (idx < MDS_MAX_DS_NODES) {
                (void)snprintf(cfg->ds_specs[idx],
                         sizeof(cfg->ds_specs[idx]), "%s", val);
}
        } else if (strncmp(key, "ds_weight.", 10) == 0) {
            /*
             * Parse `ds_weight.<ds_id> = <weight>`.  ds_id is the
             * runtime ID assigned when the DS was registered; weight
             * is a dimensionless non-negative integer consumed by the
             * WRR dispatcher.  Larger weight => proportionally more
             * placements.  Zero (or missing) leaves the DS at the
             * fallback (free-bytes) heuristic.
             */
            unsigned long id = strtoul(key + 10, NULL, 10);
            unsigned long w = strtoul(val, NULL, 10);
            if (id < MDS_MAX_DS_NODES && w <= UINT32_MAX) {
                cfg->ds_weight_by_id[id] = (uint32_t)w;
            } else {
                (void)fprintf(stderr,
                    "WARN: ds_weight.%lu=%lu out of range "
                    "(id < %u, w <= %u)\n",
                    id, w,
                    (unsigned)MDS_MAX_DS_NODES,
                    (unsigned)UINT32_MAX);
            }
        /* AutoSplit keys (Tier 3 Phase 1) */
        } else if (strcmp(key, "auto_split_enabled") == 0) {
            cfg->auto_split_enabled = (strcmp(val, "true") == 0 ||
                                       strcmp(val, "1") == 0);
        } else if (strcmp(key, "auto_split_execute") == 0) {
            cfg->auto_split_execute = (strcmp(val, "true") == 0 ||
                                       strcmp(val, "1") == 0);
        } else if (strcmp(key, "auto_split_threshold") == 0) {
            cfg->auto_split_threshold = (uint64_t)strtoull(val, NULL, 10);
        } else if (strcmp(key, "auto_split_interval") == 0) {
            cfg->auto_split_interval = (uint32_t)strtoul(val, NULL, 10);
        } else if (strcmp(key, "auto_split_cooldown") == 0) {
            cfg->auto_split_cooldown = (uint32_t)strtoul(val, NULL, 10);
        } else if (strcmp(key, "auto_split_sustained") == 0) {
            cfg->auto_split_sustained = (uint32_t)strtoul(val, NULL, 10);
        } else if (strcmp(key, "auto_split_min_children") == 0) {
            cfg->auto_split_min_children = (uint32_t)strtoul(val, NULL, 10);
        } else if (strcmp(key, "transient_state_cache") == 0) {
            cfg->transient_state_cache = (strcmp(val, "true") == 0 ||
                                          strcmp(val, "1") == 0);
        } else if (strcmp(key, "ds_synth_owner") == 0) {
            cfg->ds_synth_owner = (strcmp(val, "true") == 0 ||
                                   strcmp(val, "1") == 0);
        /*
         * Phase C of docs/hpc-nto1-plan.md -- wide-stripe pre-warm.
         * Operators on >128-DS clusters bump hpc_max_stripe_count up
         * to MDS_MAX_STRIPES; values outside [1, MDS_MAX_STRIPES] are
         * rejected loudly so a typo cannot silently degrade to a 1-DS
         * layout.
         */
        } else if (strcmp(key, "hpc_max_stripe_count") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= MDS_MAX_STRIPES) {
                cfg->hpc_max_stripe_count = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_HPC_MAX_STRIPE_COUNT;
            } else {
                (void)fprintf(stderr,
                    "ERROR: hpc_max_stripe_count=%s out of range "
                    "[1, %u]\n", val, (unsigned)MDS_MAX_STRIPES);
                (void)fclose(fp);
                return MDS_ERR_INVAL;
            }
        } else if (strcmp(key, "hpc_xdr_form") == 0) {
            /*
             * Three string tokens map to the three enum values.
             * Anything else is rejected; we deliberately do not
             * fall back to AUTO silently because operator intent
             * matters for fleet-wide compatibility (S14).
             */
            if (strcmp(val, "auto") == 0) {
                cfg->hpc_xdr_form = MDS_HPC_XDR_FORM_AUTO;
                cfg->tuning_set |= MDS_CFG_SET_HPC_XDR_FORM;
            } else if (strcmp(val, "legacy") == 0) {
                cfg->hpc_xdr_form = MDS_HPC_XDR_FORM_LEGACY;
                cfg->tuning_set |= MDS_CFG_SET_HPC_XDR_FORM;
            } else if (strcmp(val, "striped") == 0) {
                cfg->hpc_xdr_form = MDS_HPC_XDR_FORM_STRIPED;
                cfg->tuning_set |= MDS_CFG_SET_HPC_XDR_FORM;
            } else {
                (void)fprintf(stderr,
                    "ERROR: hpc_xdr_form='%s' invalid "
                    "(expected: auto, legacy, striped)\n", val);
                (void)fclose(fp);
                return MDS_ERR_INVAL;
            }
        } else if (strcmp(key, "ds_capacity_poll_ms") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            /* Cap at 24h; 0 disables the probe entirely. */
            if (v <= 86400000UL) {
                cfg->ds_capacity_poll_ms = (uint32_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: ds_capacity_poll_ms=%lu out of range "
                    "(0..86400000)\n", v);
            }
        } else if (strcmp(key, "ds_gc_workers") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            /* Hard clamp matches DS_GC_MAX_WORKERS in ds_gc.c. */
            if (v >= 1 && v <= 32) {
                cfg->ds_gc_workers = (uint32_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: ds_gc_workers=%lu out of range (1..32)\n",
                    v);
            }
        } else if (strcmp(key, "ds_gc_batch_size") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            /* Hard clamp matches DS_GC_MAX_BATCH_SIZE in ds_gc.c. */
            if (v >= 1 && v <= 4096) {
                cfg->ds_gc_batch_size = (uint32_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: ds_gc_batch_size=%lu out of range "
                    "(1..4096)\n", v);
            }
        } else if (strcmp(key, "ds_health_fail_threshold") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            /* Operationally useful range: 1..1024. */
            if (v >= 1 && v <= 1024) {
                cfg->ds_health_fail_threshold = (uint32_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: ds_health_fail_threshold=%lu out of range "
                    "(1..1024)\n", v);
            }
        } else if (strcmp(key, "cb_recall_timeout_ms") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            /* 50ms..5min; 0 is silly (would reject every callback). */
            if (v >= 50 && v <= 300000UL) {
                cfg->cb_recall_timeout_ms = (uint32_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: cb_recall_timeout_ms=%lu out of range "
                    "(50..300000)\n", v);
            }
        } else if (strcmp(key, "dir_deleg_recall_timeout_ms") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v >= 50 && v <= 300000UL) {
                cfg->dir_deleg_recall_timeout_ms = (uint32_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: dir_deleg_recall_timeout_ms=%lu out of range "
                    "(50..300000)\n", v);
            }
        } else if (strcmp(key, "metrics_http_port") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= 65535UL) {
                cfg->metrics_http_port = (uint16_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: metrics_http_port=%lu out of range "
                    "(0..65535)\n", v);
            }
        } else if (strcmp(key, "metrics_op_enabled") == 0) {
            /* Master kill-switch for per-op + per-cat-op + per-
             * op*phase latency histograms.  Default true.  Set to
             * false to take observability off the hot path entirely
             * without recompiling. */
            cfg->metrics_op_enabled =
                (strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
        } else if (strcmp(key, "compound_perf_threshold_us") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            cfg->compound_perf_threshold_us = (uint32_t)v;

        /* mountd_compat -- `showmount -e` compatibility responder.
         * See docs/mountd-compat.md.  All keys are optional; the
         * shim stays disabled unless mountd_compat_enabled=true. */
        } else if (strcmp(key, "mountd_compat_enabled") == 0) {
            cfg->mountd_compat_enabled =
                (strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
        } else if (strcmp(key, "mountd_compat_port") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= 65535UL) {
                cfg->mountd_compat_port = (uint16_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: mountd_compat_port=%lu out of range "
                    "(0..65535)\n", v);
            }
        } else if (strcmp(key, "mountd_compat_register_rpcbind") == 0) {
            cfg->mountd_compat_register_rpcbind =
                (strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
        } else if (strcmp(key, "mountd_compat_bind_addr") == 0) {
            (void)snprintf(cfg->mountd_compat_bind_addr,
                sizeof(cfg->mountd_compat_bind_addr), "%s", val);
        } else if (strcmp(key, "mountd_compat_exports") == 0) {
            /* Comma-separated list.  Empty entries and runs of
             * commas are ignored.  Each entry is bounded to 255
             * bytes; the list is bounded to 16 entries.  Excess
             * entries are dropped with a WARN. */
            char buf[1024];
            (void)snprintf(buf, sizeof(buf), "%s", val);
            cfg->mountd_compat_export_count = 0;
            char *saveptr = NULL;
            for (char *tok = strtok_r(buf, ",", &saveptr);
                 tok != NULL;
                 tok = strtok_r(NULL, ",", &saveptr)) {
                while (*tok == ' ' || *tok == '\t') {
                    tok++;
                }
                size_t tlen = strlen(tok);
                while (tlen > 0 &&
                       (tok[tlen - 1] == ' ' || tok[tlen - 1] == '\t')) {
                    tok[--tlen] = '\0';
                }
                if (tlen == 0) {
                    continue;
                }
                if (tlen >= sizeof(cfg->mountd_compat_exports[0])) {
                    (void)fprintf(stderr,
                        "WARN: mountd_compat_exports entry too long "
                        "(%zu bytes); truncating\n", tlen);
                    tlen = sizeof(cfg->mountd_compat_exports[0]) - 1;
                }
                if (cfg->mountd_compat_export_count >=
                    sizeof(cfg->mountd_compat_exports) /
                    sizeof(cfg->mountd_compat_exports[0])) {
                    (void)fprintf(stderr,
                        "WARN: mountd_compat_exports has more than %zu "
                        "entries; ignoring excess\n",
                        sizeof(cfg->mountd_compat_exports) /
                            sizeof(cfg->mountd_compat_exports[0]));
                    break;
                }
                memcpy(cfg->mountd_compat_exports[
                           cfg->mountd_compat_export_count],
                       tok, tlen);
                cfg->mountd_compat_exports[
                    cfg->mountd_compat_export_count][tlen] = '\0';
                cfg->mountd_compat_export_count++;
            }
        } else if (strcmp(key, "inline_max_size") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            /* Cap at the protocol maximum (64 KiB = MDS_XATTR_VAL_MAX
             * which also bounds single-chunk inline writes). */
            if (v > 0 && v <= 65536UL) {
                cfg->inline_max_size = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_INLINE_MAX_SIZE;
            } else {
                (void)fprintf(stderr,
                    "WARN: inline_max_size=%lu out of range "
                    "(1..65536)\n", v);
            }
        } else if (strcmp(key, "dirent_cache_size") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= 1000000UL) {
                cfg->dirent_cache_size = (uint32_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: dirent_cache_size=%lu out of range "
                    "(0..1000000)\n", v);
            }
        } else if (strcmp(key, "negative_cache_ttl_ms") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            /* Cap at 1h.  Zero falls back to built-in default at
             * dirent_cache_init time. */
            if (v <= 3600000UL) {
                cfg->negative_cache_ttl_ms = (uint32_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: negative_cache_ttl_ms=%lu out of range "
                    "(0..3600000)\n", v);
            }
        } else if (strcmp(key, "positive_cache_ttl_ms") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            /* Cap at 1h.  Zero = unset; main.c then picks a default by
             * cluster size (0 single-MDS, ~1000 ms multi-MDS). */
            if (v <= 3600000UL) {
                cfg->positive_cache_ttl_ms = (uint32_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: positive_cache_ttl_ms=%lu out of range "
                    "(0..3600000)\n", v);
            }
        } else if (strcmp(key, "dir_delegations_enabled") == 0) {
            cfg->dir_delegations_enabled =
                (strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
        } else if (strcmp(key, "file_delegations_enabled") == 0) {
            cfg->file_delegations_enabled =
                (strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
        } else if (strcmp(key, "ndb_conn_pool_size") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v > 0 && v <= 64) {
                cfg->ndb_conn_pool_size = (uint32_t)v;
            } else {
                (void)fprintf(stderr,
                    "WARN: ndb_conn_pool_size=%lu out of range "
                    "(1..64); using default\n", v);
            }
        } else if (strcmp(key, "ndb_async_writes") == 0) {
            cfg->ndb_async_writes = (strcmp(val, "true") == 0 ||
                                     strcmp(val, "1") == 0);
        } else if (strcmp(key, "placement_policy") == 0) {
            if (strcmp(val, "rr") == 0) {
                cfg->placement_policy = PLACEMENT_RR;
            } else if (strcmp(val, "wrr") == 0 ||
                       strcmp(val, "weighted_rr") == 0) {
                cfg->placement_policy = PLACEMENT_WEIGHTED_RR;
            } else if (strcmp(val, "capacity") == 0) {
                cfg->placement_policy = PLACEMENT_CAPACITY;
            } else {
                (void)fprintf(stderr,
                    "ERROR: invalid placement_policy '%s' "
                    "(expected rr|wrr|capacity)\n", val);
                (void)fclose(fp);
                return MDS_ERR_INVAL;
            }
            cfg->tuning_set |= MDS_CFG_SET_PLACEMENT_POLICY;
        } else if (strcmp(key, "placement_policy_enabled") == 0) {
            cfg->placement_policy_enabled =
                (strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
        } else if (strcmp(key, "placement_capacity_weighting") == 0) {
            if (strcmp(val, "off") == 0 ||
                strcmp(val, "false") == 0 ||
                strcmp(val, "0") == 0) {
                cfg->placement_capacity_weighting = CAP_WEIGHT_OFF;
            } else if (strcmp(val, "proportional") == 0 ||
                       strcmp(val, "on") == 0 ||
                       strcmp(val, "true") == 0 ||
                       strcmp(val, "1") == 0) {
                cfg->placement_capacity_weighting = CAP_WEIGHT_PROPORTIONAL;
            } else {
                (void)fprintf(stderr,
                    "WARN: placement_capacity_weighting='%s' unknown "
                    "(expected off|proportional); keeping default off\n",
                    val);
            }
        } else if (strcmp(key, "inode_cache_size") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= 1000000) {
                cfg->inode_cache_size = (uint32_t)v;
            }
        } else if (strcmp(key, "inline_enabled") == 0) {
            cfg->inline_enabled = (strcmp(val, "true") == 0 ||
                                   strcmp(val, "1") == 0);
            cfg->tuning_set |= MDS_CFG_SET_INLINE_ENABLED;
        /* Sharding keys (Tier 3 Phase 3) */
        /* DS prepare keys (Phase 6) */
        } else if (strcmp(key, "ds_prepare_queue_depth") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= 65536) {
                cfg->ds_prepare_queue_depth = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_DS_PREPARE_QUEUE_DEPTH;
            }
        } else if (strcmp(key, "ds_prepare_workers") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            if (v <= 64) {
                cfg->ds_prepare_workers = (uint32_t)v;
            }
        } else if (strcmp(key, "shard_enabled") == 0) {
            cfg->shard_enabled = (strcmp(val, "true") == 0 ||
                                  strcmp(val, "1") == 0);
        } else if (strcmp(key, "hide_referral_junctions") == 0) {
            cfg->hide_referral_junctions =
                (strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
        } else if (strcmp(key, "posix_dac") == 0) {
            cfg->posix_dac = (strcmp(val, "true") == 0 ||
                              strcmp(val, "1") == 0);

        /* Authority / image split */
        } else if (strcmp(key, "catalog_image_mode") == 0) {
            if (strcmp(val, "off") == 0) {
                cfg->catalog_image_mode = MDS_IMAGE_OFF;
            } else if (strcmp(val, "shadow") == 0) {
                cfg->catalog_image_mode = MDS_IMAGE_SHADOW;
            } else if (strcmp(val, "compare") == 0) {
                cfg->catalog_image_mode = MDS_IMAGE_COMPARE;
            } else if (strcmp(val, "primary") == 0) {
                cfg->catalog_image_mode = MDS_IMAGE_PRIMARY;
            } else {
                (void)fprintf(stderr,
                    "ERROR: invalid catalog_image_mode '%s' "
                    "(expected off|shadow|compare|primary)\n", val);
                (void)fclose(fp);
                return MDS_ERR_INVAL;
            }
        } else if (strcmp(key, "catalog_compare_reads") == 0) {
            cfg->catalog_compare_reads =
                (strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
        } else if (strcmp(key, "catalog_replay_mode") == 0) {
            if (strcmp(val, "off") == 0) {
                cfg->catalog_replay_mode = MDS_REPLAY_OFF;
            } else if (strcmp(val, "log") == 0) {
                cfg->catalog_replay_mode = MDS_REPLAY_LOG;
            } else if (strcmp(val, "journal") == 0) {
                cfg->catalog_replay_mode = MDS_REPLAY_JOURNAL;
            } else {
                (void)fprintf(stderr,
                    "ERROR: invalid catalog_replay_mode '%s' "
                    "(expected off|log|journal)\n", val);
                (void)fclose(fp);
                return MDS_ERR_INVAL;
            }
        } else if (strcmp(key, "catalog_replay_snapshot_path") == 0) {
            (void)snprintf(cfg->catalog_replay_snapshot_path,
                sizeof(cfg->catalog_replay_snapshot_path), "%s", val);
        } else if (strcmp(key, "catalog_replay_rebuild_on_start") == 0) {
            cfg->catalog_replay_rebuild_on_start =
                (strcmp(val, "true") == 0 || strcmp(val, "1") == 0);
        } else if (strcmp(key, "catalog_delta_log_path") == 0) {
            (void)snprintf(cfg->catalog_delta_log_path,
                sizeof(cfg->catalog_delta_log_path), "%s", val);

        /* DS synthetic-ID secret (RFC 8435 §2.2.1) */
        } else if (strcmp(key, "ds_synth_secret_file") == 0) {
            (void)snprintf(cfg->ds_synth_secret_file,
                sizeof(cfg->ds_synth_secret_file), "%s", val);

        /* Stripe lease */
        } else if (strcmp(key, "stripe_lease_duration_ms") == 0) {
            unsigned long v = strtoul(val, NULL, 10);
            /* 0 disables; cap at 5 min. */
            if (v <= 300000UL) {
                cfg->stripe_lease_duration_ms = (uint32_t)v;
                cfg->tuning_set |= MDS_CFG_SET_STRIPE_LEASE_DURATION;
            } else {
                (void)fprintf(stderr,
                    "WARN: stripe_lease_duration_ms=%lu out of range "
                    "(0..300000)\n", v);
            }

        /* Logging (src/common/log.c).  These keys are consumed by
         * main.c after mds_log_init(); the parser only records intent.
         * Unknown tokens warn and leave the default in place. */
        } else if (strcmp(key, "log_file") == 0) {
            (void)snprintf(cfg->log_file, sizeof(cfg->log_file), "%s", val);
        } else if (strcmp(key, "log_level") == 0) {
            int lvl = mds_log_level_from_str(val);
            if (lvl >= 0) {
                cfg->log_level_global = lvl;
            } else {
                (void)fprintf(stderr,
                    "WARN: invalid log_level '%s' "
                    "(expected fatal|error|warn|info|debug|trace); "
                    "keeping default\n", val);
            }
        } else if (strncmp(key, "log_level.", 10) == 0) {
            const char *comp_name = key + 10;
            int comp = mds_log_component_from_str(comp_name);
            int lvl = mds_log_level_from_str(val);
            if (comp >= 0 && lvl >= 0) {
                cfg->log_level_by_component[comp] = lvl;
            } else {
                (void)fprintf(stderr,
                    "WARN: invalid log_level.%s='%s' "
                    "(component mds|fsal|cluster|repl|cat|bpf|nfs, "
                    "level fatal|error|warn|info|debug|trace); ignored\n",
                    comp_name, val);
            }

        } else {
            /* Warn on unrecognized config keys. */
            (void)fprintf(stderr,
                "WARN: unknown config key '%s' "
                "(value '%s') -- ignored\n", key, val);
        }
    }

    (void)fclose(fp);

    /* Validate: require_mtls needs all TLS cert paths. */
    if (cfg->require_mtls) {
        if (cfg->cluster_ca_file[0] == '\0' ||
            cfg->node_cert_file[0] == '\0' ||
            cfg->node_key_file[0] == '\0') {
            (void)fprintf(stderr,
                "ERROR: require_mtls=true but "
                "cluster_ca_file, node_cert_file, or "
                "node_key_file is empty\n");
            return MDS_ERR_INVAL;
        }
    }

    /* Validate: catalog_image_mode != off requires durable journal. */
    if (cfg->catalog_image_mode != MDS_IMAGE_OFF &&
        cfg->catalog_replay_mode != MDS_REPLAY_JOURNAL) {
        (void)fprintf(stderr,
            "ERROR: catalog_image_mode=%s requires "
            "catalog_replay_mode=journal\n",
            cfg->catalog_image_mode == MDS_IMAGE_SHADOW ? "shadow" :
            cfg->catalog_image_mode == MDS_IMAGE_COMPARE ? "compare" :
            "primary");
        return MDS_ERR_INVAL;
    }

    /* Validate: compare_reads requires image >= shadow. */
    if (cfg->catalog_compare_reads &&
        cfg->catalog_image_mode < MDS_IMAGE_SHADOW) {
        (void)fprintf(stderr,
            "ERROR: catalog_compare_reads=true requires "
            "catalog_image_mode=shadow|compare|primary\n");
        return MDS_ERR_INVAL;
    }

    /* Load DS synthetic-ID secret if configured. */
    if (cfg->ds_synth_secret_file[0] != '\0') {
        FILE *sf = fopen(cfg->ds_synth_secret_file, "rb");
        if (sf == NULL) {
            (void)fprintf(stderr,
                "ERROR: cannot open ds_synth_secret_file '%s'\n",
                cfg->ds_synth_secret_file);
            return MDS_ERR_IO;
        }
        size_t nr = fread(cfg->ds_synth_secret,
                          1, sizeof(cfg->ds_synth_secret), sf);
        (void)fclose(sf);
        if (nr != sizeof(cfg->ds_synth_secret)) {
            (void)fprintf(stderr,
                "ERROR: ds_synth_secret_file '%s' must be exactly "
                "32 bytes (got %zu)\n",
                cfg->ds_synth_secret_file, nr);
            return MDS_ERR_INVAL;
        }
        cfg->ds_synth_secret_len = (uint32_t)sizeof(cfg->ds_synth_secret);
    }

    /* Auto-size prealloc_pool_size based on ds_count, but only if
     * neither a profile nor an explicit key set it deliberately. */
    if (!(cfg->tuning_set & MDS_CFG_SET_PREALLOC_POOL_SIZE) &&
        cfg->ds_count > 0) {
        cfg->prealloc_pool_size = cfg->ds_count * 64;
        if (cfg->prealloc_pool_size < 64) {
            cfg->prealloc_pool_size = 64;
        }
    }

    /* Auto-size NDB connection pool to match worker_threads when not
     * explicitly set.  RonDB catalogue dispatch is now concurrent
     * (no single-writer serialization), so each worker thread may
     * need its own NDB connection under peak load.  Capped at
     * NDB_CONN_POOL_MAX (64) in the shim.
     *
     * Override with ndb_conn_pool_size=N in mds.conf.
     * For multi-MDS clusters, ensure RonDB config.ini has enough
     * [api] slots: total = num_MDS x ndb_conn_pool_size. */
    if (cfg->ndb_conn_pool_size == 0) {
        uint32_t wt = cfg->worker_threads;
        if (wt == 0) { wt = 4; }
        cfg->ndb_conn_pool_size = (wt <= 64) ? wt : 64;
    }

    return MDS_OK;
}

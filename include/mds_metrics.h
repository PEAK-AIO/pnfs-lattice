/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_metrics.h -- Daemon-native metrics registry.
 *
 * Thread-safe atomic counters for replication, commit, and
 * health observability.  No libbpf dependency.
 */

#ifndef MDS_METRICS_H
#define MDS_METRICS_H

#include <stdint.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>

/* Live counters (written by hot paths, read by snapshot). */
struct mds_metrics {
    /* Replication throughput. */
    _Atomic uint64_t repl_deltas_sent;
    _Atomic uint64_t repl_bytes_sent;

    /* Catalogue commit outcomes. */
    _Atomic uint64_t cat_commits_ok;
    _Atomic uint64_t cat_commits_fail;

    /* Catalogue flush latency (daemon-native timing). */
    _Atomic uint64_t cat_flush_ns_sum;
    _Atomic uint64_t cat_flush_count;
};

/* Point-in-time snapshot (non-atomic, safe to pass around). */
struct mds_metrics_snapshot {
    uint64_t repl_deltas_sent;
    uint64_t repl_bytes_sent;
    uint64_t cat_commits_ok;
    uint64_t cat_commits_fail;
    uint64_t cat_flush_ns_sum;
    uint64_t cat_flush_count;

    /* Health gauges (populated from health_monitor at snapshot time). */
    uint8_t  repl_health_ok;     /* 1 = healthy, 0 = degraded */
    uint8_t  repl_writes_blocked; /* 1 = blocked, 0 = normal */

    /* Authority / image counters (populated from catalog_stats). */
    uint64_t cat_authority_writes;
    uint64_t cat_replay_journal_writes;
    uint64_t cat_replay_high_water;
    uint64_t cat_image_applied_high_water;
    uint64_t cat_image_hits;
    uint64_t cat_image_misses;
    uint64_t cat_image_fallbacks;
    uint64_t cat_compare_mismatches;
    uint64_t cat_compare_skipped_lag;
    uint64_t cat_replay_rebuild_completions;
};

/* -----------------------------------------------------------------------
 * Per-shard metrics (Phase 5D, Tier 3)
 *
 * Each mds_shard carries a pointer to its own mds_shard_metrics.
 * The CQ writer thread increments shard-local counters.
 * Global snapshot = sum across all shards.
 * ----------------------------------------------------------------------- */

struct mds_shard_metrics {
    _Atomic uint64_t cat_commits_ok;
    _Atomic uint64_t cat_commits_fail;
    _Atomic uint64_t cat_flush_ns_sum;
    _Atomic uint64_t cat_flush_count;
    _Atomic uint64_t ops_total;             /**< Total CQ ops committed. */
    _Atomic uint64_t queue_depth_sum;       /**< Cumulative queue depth samples. */
    _Atomic uint64_t queue_depth_count;     /**< Number of queue depth samples. */
};

/* Branch-specific counters (global, not per-shard). */
struct mds_branch_metrics {
    /* Generic async prepare. */
    _Atomic uint64_t ds_prepare_async_ok;     /**< Async FH captures completed. */
    _Atomic uint64_t ds_prepare_sync_fallback;/**< Sync fallback on queue miss. */
    _Atomic uint64_t ds_prepare_queue_depth;  /**< Current queue occupancy. */

    /* DS prealloc pool observability (Phase 12, Component C).
     * See docs/design-layoutget-decoupling.md S4.3.  These counters
     * let operators answer "is the pool warm enough?" without
     * instrumenting the hot path itself. */
    _Atomic uint64_t prealloc_pops_ok;        /**< Fast-path ring hit. */
    _Atomic uint64_t prealloc_pops_empty;     /**< Ring empty; slow-path fallback. */
    _Atomic uint64_t prealloc_pops_fh_missing;/**< Ring hit but FH not captured. */
    _Atomic uint64_t prealloc_refill_entries; /**< Entries added by refill thread. */
    _Atomic uint64_t prealloc_refill_batches; /**< Refill batch invocations. */
    _Atomic uint64_t gc_pending;              /**< GC-queue rows owned by this
                                                  *  MDS still awaiting DS
                                                  *  unlink (gauge; refreshed by
                                                  *  the ds_gc coordinator). */
    _Atomic uint64_t layoutget_sync_fallback; /**< LAYOUTGET took sync FH path. */
    _Atomic uint64_t layoutget_delay_count;   /**< LAYOUTGET returned NFS4ERR_DELAY
                                                  *  (reserved; used once
                                                  *  Component A lands). */

    /* NFS operation counters. */
    _Atomic uint64_t nfs_ops_total;     /**< All COMPOUND operations. */
    _Atomic uint64_t nfs_op_create;
    _Atomic uint64_t nfs_op_remove;
    _Atomic uint64_t nfs_op_lookup;
    _Atomic uint64_t nfs_op_getattr;
    _Atomic uint64_t nfs_op_setattr;
    _Atomic uint64_t nfs_op_readdir;
    _Atomic uint64_t nfs_op_open;
    _Atomic uint64_t nfs_op_close;
    _Atomic uint64_t nfs_op_read;
    _Atomic uint64_t nfs_op_write;
    _Atomic uint64_t nfs_op_layoutget;
    _Atomic uint64_t nfs_op_rename;
    _Atomic uint64_t nfs_moved;        /**< Ops rejected with NFS4ERR_MOVED
                                          *  by referral_strict routing. */

    /*
     * Per-phase latency instrumentation for OP_OPEN on the
     * CLAIM_NULL + create path.  Each field is a simple
     * nanosecond sum; the matching `_count` field records the
     * number of observations so operators can derive mean
     * latency per phase.
     *
     * The phases are only sampled when the request is the
     * CLAIM_NULL create path (i.e. OPEN with create=true that
     * resulted in an ns_create).  They answer "of the N ms per
     * CREATE, how much is lookup vs NDB commit vs state alloc
     * vs other."  Zero overhead on non-create OPENs.
     */
    _Atomic uint64_t open_create_parent_ns_sum;
    _Atomic uint64_t open_create_parent_count;
    _Atomic uint64_t open_create_lookup_ns_sum;
    _Atomic uint64_t open_create_lookup_count;
    _Atomic uint64_t open_create_ns_create_ns_sum;
    _Atomic uint64_t open_create_ns_create_count;
    _Atomic uint64_t open_create_ds_prepare_ns_sum;
    _Atomic uint64_t open_create_ds_prepare_count;
    _Atomic uint64_t open_create_state_open_ns_sum;
    _Atomic uint64_t open_create_state_open_count;
    _Atomic uint64_t open_create_total_ns_sum;
    _Atomic uint64_t open_create_total_count;

    /*
     * Phase 1.1: bumped every time placement_select_ex has to fall
     * back to the heap for its scratch arrays because the cluster
     * exceeds MDS_PLACEMENT_STACK_MAX (64) DSes.  Stays at 0 for
     * realistic clusters; a non-zero value means the stack cap
     * needs bumping or the stack frame budget has changed.
     */
    _Atomic uint64_t placement_heap_fallback_total;

    /*
     * Phase 3 (D5): bumped every time placement_select_ex has to
     * degrade the requested stripe_count * mirror_count because the
     * cluster has fewer ONLINE DSes than the profile requests.  Non-
     * zero is operationally interesting: it says "your profile is
     * bigger than your cluster."
     */
    _Atomic uint64_t placement_degraded_total;

    /*
     * IO_ADVISE accounting (RFC 7862 S15.5).  io_advise_total
     * counts every IO_ADVISE op successfully dispatched; the two
     * per-hint counters count only the hint flavours the server
     * actually acts on (WILLNEED bumps io_tracker to bias tiering
     * hot; DONTNEED / NOREUSE evict the inode cache entry to free
     * memory).  Hints that we do not act on (NORMAL, SEQUENTIAL,
     * RANDOM, INIT_PROXIMITY, directional READ/WRITE) do not bump
     * any sub-counter and are not echoed back in the response
     * mask, so clients see exactly what was honoured.
     */
    _Atomic uint64_t io_advise_total;
    _Atomic uint64_t io_advise_willneed;
    _Atomic uint64_t io_advise_dontneed;
};

extern struct mds_branch_metrics g_branch_metrics;

/** Metrics response version. */
#define MDS_METRICS_VERSION_V1  1  /**< Fixed 50-byte global snapshot. */
#define MDS_METRICS_VERSION_V2  2  /**< Global + branch + per-shard. */

/**
 * Register the RPC dispatcher threadpool for Prometheus export.
 *
 * Once set, `mds_metrics_prometheus_v2()` appends a
 * `pnfs_mds_rpc_worker_*` / `pnfs_mds_rpc_queue_*` section to its
 * output, so operators can see worker saturation, queue depth, and
 * queue-wait latency on /metrics without a separate endpoint.
 *
 * Pass NULL to unregister (e.g. during shutdown).  Thread-safe.
 */
struct threadpool;
void mds_metrics_set_rpc_threadpool(struct threadpool *tp);

/** Global metrics instance. */
extern struct mds_metrics g_metrics;

/** Reset all counters to zero. */
void mds_metrics_reset(void);

/** Take a point-in-time snapshot of all counters. */
struct mds_metrics_snapshot mds_metrics_snapshot(void);

/**
 * Render a snapshot as Prometheus text exposition format.
 *
 * @param snap  Snapshot to render.
 * @param buf   Output buffer.
 * @param cap   Buffer capacity.
 * @return Number of bytes written (excluding NUL), or -1 on truncation.
 */
int mds_metrics_prometheus(const struct mds_metrics_snapshot *snap,
                           char *buf, size_t cap);

/**
 * Render v2 metrics (global + branch) as Prometheus text.
 *
 * @param snap    Global snapshot.
 * @param branch  Branch-specific counters snapshot.
 * @param buf     Output buffer.
 * @param cap     Buffer capacity.
 * @return Bytes written (excluding NUL), or -1 on truncation.
 */
int mds_metrics_prometheus_v2(const struct mds_metrics_snapshot *snap,
                              const struct mds_branch_metrics *branch,
                              char *buf, size_t cap);

/**
 * Fill catalog_stats fields in a metrics snapshot.
 *
 * Called by the metrics export path after mds_metrics_snapshot().
 * @param snap  Snapshot to fill.
 * @param cs    Catalog stats (from mds_catalogue_stats(cat)).
 */
struct catalog_stats;
void mds_metrics_snapshot_fill_catalog(
    struct mds_metrics_snapshot *snap,
    const struct catalog_stats *cs);

#endif /* MDS_METRICS_H */

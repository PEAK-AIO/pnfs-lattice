/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_metrics.c -- Daemon-native metrics registry.
 */

#include <stdio.h>
#include <stdatomic.h>
#include <string.h>

#include "mds_metrics.h"
#include "mds_histogram.h"
#include "mds_op_metrics.h"
#include "pnfs_mds.h"
#include "catalog_stats.h"

/* Global metrics instance (zero-initialized). */
struct mds_metrics g_metrics;
struct mds_branch_metrics g_branch_metrics;

/*
 * Optional pointer to the RPC dispatcher threadpool.  Set by main.c
 * after the pool is created so the Prometheus renderer can include
 * dispatcher-saturation metrics.  The atomic store/load only makes
 * the POINTER hand-off race-safe; it does NOT extend the pool's
 * lifetime.  A scrape that loaded the pointer before the NULL store
 * may still be dereferencing the pool, so main.c must stop+join the
 * metrics HTTP server (metrics_http_stop) before threadpool_destroy.
 */
static _Atomic(struct threadpool *) g_metrics_rpc_tp = NULL;

void mds_metrics_set_rpc_threadpool(struct threadpool *tp)
{
    atomic_store_explicit(&g_metrics_rpc_tp, tp, memory_order_release);
}

/*
 * Render the RPC dispatcher section.  Returns bytes appended or -1
 * on truncation.  Safe to call with no pool registered; emits 0
 * bytes in that case.
 */
static int render_rpc_threadpool(char *buf, size_t cap)
{
    struct threadpool *tp;
    struct threadpool_stats st;
    int n;
    int total = 0;
    int rc;

    tp = atomic_load_explicit(&g_metrics_rpc_tp, memory_order_acquire);
    if (tp == NULL || buf == NULL || cap == 0) {
        return 0;
    }

    threadpool_get_stats(tp, &st);

    n = snprintf(buf, cap,
        "# HELP pnfs_mds_rpc_worker_total "
            "Total RPC dispatcher worker threads.\n"
        "# TYPE pnfs_mds_rpc_worker_total gauge\n"
        "pnfs_mds_rpc_worker_total %u\n"
        "# HELP pnfs_mds_rpc_worker_active "
            "Workers currently executing a request "
            "(== worker_total means saturated).\n"
        "# TYPE pnfs_mds_rpc_worker_active gauge\n"
        "pnfs_mds_rpc_worker_active %u\n"
        "# HELP pnfs_mds_rpc_queue_depth "
            "Requests waiting in the dispatcher queue.\n"
        "# TYPE pnfs_mds_rpc_queue_depth gauge\n"
        "pnfs_mds_rpc_queue_depth %u\n"
        "# HELP pnfs_mds_rpc_queue_capacity "
            "Maximum dispatcher queue capacity.\n"
        "# TYPE pnfs_mds_rpc_queue_capacity gauge\n"
        "pnfs_mds_rpc_queue_capacity %u\n"
        "# HELP pnfs_mds_rpc_submitted_total "
            "RPC requests accepted onto the dispatcher queue.\n"
        "# TYPE pnfs_mds_rpc_submitted_total counter\n"
        "pnfs_mds_rpc_submitted_total %lu\n"
        "# HELP pnfs_mds_rpc_completed_total "
            "RPC requests finished by worker threads.\n"
        "# TYPE pnfs_mds_rpc_completed_total counter\n"
        "pnfs_mds_rpc_completed_total %lu\n"
        "# HELP pnfs_mds_rpc_queue_full_total "
            "RPC submissions rejected because the queue was full.\n"
        "# TYPE pnfs_mds_rpc_queue_full_total counter\n"
        "pnfs_mds_rpc_queue_full_total %lu\n"
        "# HELP pnfs_mds_rpc_queue_wait_seconds "
            "Time each request spent queued before a worker picked "
            "it up (dispatcher backlog latency).\n",
        (unsigned)st.worker_total,
        (unsigned)st.worker_active,
        (unsigned)st.queue_depth,
        (unsigned)st.queue_capacity,
        (unsigned long)st.submitted_total,
        (unsigned long)st.completed_total,
        (unsigned long)st.queue_full_total);
    if (n < 0 || (size_t)n >= cap) {
        return -1;
    }
    total += n;

    rc = mds_histogram_render(st.queue_wait_hist,
                              "pnfs_mds_rpc_queue_wait_seconds",
                              buf + total, cap - (size_t)total);
    if (rc < 0) {
        return -1;
    }
    total += rc;
    return total;
}

void mds_metrics_reset(void)
{
    atomic_store(&g_metrics.repl_deltas_sent, 0);
    atomic_store(&g_metrics.repl_bytes_sent, 0);
    atomic_store(&g_metrics.cat_commits_ok, 0);
    atomic_store(&g_metrics.cat_commits_fail, 0);
    atomic_store(&g_metrics.cat_flush_ns_sum, 0);
    atomic_store(&g_metrics.cat_flush_count, 0);
}

struct mds_metrics_snapshot mds_metrics_snapshot(void)
{
    struct mds_metrics_snapshot s;
    memset(&s, 0, sizeof(s));

    s.repl_deltas_sent = atomic_load(&g_metrics.repl_deltas_sent);
    s.repl_bytes_sent  = atomic_load(&g_metrics.repl_bytes_sent);
    s.cat_commits_ok  = atomic_load(&g_metrics.cat_commits_ok);
    s.cat_commits_fail = atomic_load(&g_metrics.cat_commits_fail);
    s.cat_flush_ns_sum = atomic_load(&g_metrics.cat_flush_ns_sum);
    s.cat_flush_count = atomic_load(&g_metrics.cat_flush_count);

    /* Health gauges are populated by the caller from health_monitor
     * since this module doesn't depend on health.h directly. */

    /* catalog_stats fields are populated by the caller from
     * mds_catalogue_stats() since this module does not depend
     * on mds_catalogue.h directly.  Zero-init above is the
     * correct default when no catalogue handle is available. */

    return s;
}

void mds_metrics_snapshot_fill_catalog(
    struct mds_metrics_snapshot *snap,
    const struct catalog_stats *cs)
{
    if (snap == NULL || cs == NULL) {
        return;
    }
    snap->cat_authority_writes =
        catalog_stat_get(&cs->authority_writes);
    snap->cat_replay_journal_writes =
        catalog_stat_get(&cs->replay_journal_writes);
    snap->cat_replay_high_water =
        catalog_stat_get(&cs->replay_high_water);
    snap->cat_image_applied_high_water =
        catalog_stat_get(&cs->image_applied_high_water);
    snap->cat_image_hits =
        catalog_stat_get(&cs->image_hits);
    snap->cat_image_misses =
        catalog_stat_get(&cs->image_misses);
    snap->cat_image_fallbacks =
        catalog_stat_get(&cs->image_fallbacks);
    snap->cat_compare_mismatches =
        catalog_stat_get(&cs->compare_mismatches);
    snap->cat_compare_skipped_lag =
        catalog_stat_get(&cs->compare_skipped_lag);
    snap->cat_replay_rebuild_completions =
        catalog_stat_get(&cs->replay_rebuild_completions);
}

int mds_metrics_prometheus(const struct mds_metrics_snapshot *snap,
                           char *buf, size_t cap)
{
    if (snap == NULL || buf == NULL || cap == 0) {
        return -1;
    }

    int n = snprintf(buf, cap,
        "# HELP pnfs_mds_repl_deltas_sent "
            "Replication deltas sent.\n"
        "# TYPE pnfs_mds_repl_deltas_sent counter\n"
        "pnfs_mds_repl_deltas_sent %lu\n"
        "# HELP pnfs_mds_repl_bytes_sent "
            "Replication bytes sent.\n"
        "# TYPE pnfs_mds_repl_bytes_sent counter\n"
        "pnfs_mds_repl_bytes_sent %lu\n"
        "# HELP pnfs_mds_cat_commits_ok "
            "Successful catalogue commits.\n"
        "# TYPE pnfs_mds_cat_commits_ok counter\n"
        "pnfs_mds_cat_commits_ok %lu\n"
        "# HELP pnfs_mds_cat_commits_fail "
            "Failed catalogue commits.\n"
        "# TYPE pnfs_mds_cat_commits_fail counter\n"
        "pnfs_mds_cat_commits_fail %lu\n"
        "# HELP pnfs_mds_cat_flush_seconds_sum "
            "Total catalogue flush time.\n"
        "# TYPE pnfs_mds_cat_flush_seconds_sum counter\n"
        "pnfs_mds_cat_flush_seconds_sum %.9f\n"
        "# HELP pnfs_mds_cat_flush_count "
            "Catalogue flush count.\n"
        "# TYPE pnfs_mds_cat_flush_count counter\n"
        "pnfs_mds_cat_flush_count %lu\n"
        "# HELP pnfs_mds_repl_health_ok "
            "Replication health (1=ok, 0=degraded).\n"
        "# TYPE pnfs_mds_repl_health_ok gauge\n"
        "pnfs_mds_repl_health_ok %u\n"
        "# HELP pnfs_mds_repl_writes_blocked "
            "Writes blocked by repl health (1=blocked).\n"
        "# TYPE pnfs_mds_repl_writes_blocked gauge\n"
        "pnfs_mds_repl_writes_blocked %u\n",
        (unsigned long)snap->repl_deltas_sent,
        (unsigned long)snap->repl_bytes_sent,
        (unsigned long)snap->cat_commits_ok,
        (unsigned long)snap->cat_commits_fail,
        (double)snap->cat_flush_ns_sum / 1e9,
        (unsigned long)snap->cat_flush_count,
        (unsigned)snap->repl_health_ok,
        (unsigned)snap->repl_writes_blocked);

    if (n < 0 || (size_t)n >= cap) {
        return -1;
    }
    return n;
}

int mds_metrics_prometheus_v2(const struct mds_metrics_snapshot *snap,
                              const struct mds_branch_metrics *branch,
                              char *buf, size_t cap)
{
    int base;
    int extra;

    if (snap == NULL || branch == NULL || buf == NULL || cap == 0) {
        return -1;
    }

    /* Render v1 base metrics first. */
    base = mds_metrics_prometheus(snap, buf, cap);
    if (base < 0) {
        return -1;
    }

    /* Append authority/image counters. */
    extra = snprintf(buf + base, cap - (size_t)base,
        "# HELP pnfs_mds_cat_authority_writes "
            "Authority backend write commits.\n"
        "# TYPE pnfs_mds_cat_authority_writes counter\n"
        "pnfs_mds_cat_authority_writes %lu\n"
        "# HELP pnfs_mds_cat_replay_journal_writes "
            "Semantic replay journal records written.\n"
        "# TYPE pnfs_mds_cat_replay_journal_writes counter\n"
        "pnfs_mds_cat_replay_journal_writes %lu\n"
        "# HELP pnfs_mds_cat_replay_high_water "
            "Replay journal high-water seqno.\n"
        "# TYPE pnfs_mds_cat_replay_high_water gauge\n"
        "pnfs_mds_cat_replay_high_water %lu\n"
        "# HELP pnfs_mds_cat_image_applied_high_water "
            "Image applied high-water seqno.\n"
        "# TYPE pnfs_mds_cat_image_applied_high_water gauge\n"
        "pnfs_mds_cat_image_applied_high_water %lu\n"
        "# HELP pnfs_mds_cat_image_hits "
            "Image read hits.\n"
        "# TYPE pnfs_mds_cat_image_hits counter\n"
        "pnfs_mds_cat_image_hits %lu\n"
        "# HELP pnfs_mds_cat_image_misses "
            "Image read misses.\n"
        "# TYPE pnfs_mds_cat_image_misses counter\n"
        "pnfs_mds_cat_image_misses %lu\n"
        "# HELP pnfs_mds_cat_image_fallbacks "
            "Image-to-authority fallbacks.\n"
        "# TYPE pnfs_mds_cat_image_fallbacks counter\n"
        "pnfs_mds_cat_image_fallbacks %lu\n"
        "# HELP pnfs_mds_cat_compare_mismatches "
            "Compare-read mismatches (true divergence).\n"
        "# TYPE pnfs_mds_cat_compare_mismatches counter\n"
        "pnfs_mds_cat_compare_mismatches %lu\n"
        "# HELP pnfs_mds_cat_compare_skipped_lag "
            "Compare-reads skipped due to image lag.\n"
        "# TYPE pnfs_mds_cat_compare_skipped_lag counter\n"
        "pnfs_mds_cat_compare_skipped_lag %lu\n"
        "# HELP pnfs_mds_cat_replay_rebuild_completions "
            "Replay rebuild completions.\n"
        "# TYPE pnfs_mds_cat_replay_rebuild_completions counter\n"
        "pnfs_mds_cat_replay_rebuild_completions %lu\n",
        (unsigned long)snap->cat_authority_writes,
        (unsigned long)snap->cat_replay_journal_writes,
        (unsigned long)snap->cat_replay_high_water,
        (unsigned long)snap->cat_image_applied_high_water,
        (unsigned long)snap->cat_image_hits,
        (unsigned long)snap->cat_image_misses,
        (unsigned long)snap->cat_image_fallbacks,
        (unsigned long)snap->cat_compare_mismatches,
        (unsigned long)snap->cat_compare_skipped_lag,
        (unsigned long)snap->cat_replay_rebuild_completions);
    if (extra > 0 && (size_t)extra < cap - (size_t)base) { base += extra; }
    /* Async-REMOVE delete manifest (ported). */
    extra = snprintf(buf + base, cap - (size_t)base,
        "# TYPE pnfs_mds_remove_async_acked counter\n"
        "pnfs_mds_remove_async_acked %lu\n"
        "# TYPE pnfs_mds_remove_async_ack_unlinked counter\n"
        "pnfs_mds_remove_async_ack_unlinked %lu\n"
        "# TYPE pnfs_mds_remove_async_sync_fallback counter\n"
        "pnfs_mds_remove_async_sync_fallback %lu\n"
        "# TYPE pnfs_mds_remove_async_manifest_insert_fail counter\n"
        "pnfs_mds_remove_async_manifest_insert_fail %lu\n"
        "# TYPE pnfs_mds_remove_async_drained_ok counter\n"
        "pnfs_mds_remove_async_drained_ok %lu\n"
        "# TYPE pnfs_mds_remove_async_drain_mismatch counter\n"
        "pnfs_mds_remove_async_drain_mismatch %lu\n"
        "# TYPE pnfs_mds_remove_async_drain_fail counter\n"
        "pnfs_mds_remove_async_drain_fail %lu\n"
        "# TYPE pnfs_mds_remove_async_force_drain counter\n"
        "pnfs_mds_remove_async_force_drain %lu\n"
        "# TYPE pnfs_mds_remove_async_tombstone_hit counter\n"
        "pnfs_mds_remove_async_tombstone_hit %lu\n"
        "# TYPE pnfs_mds_remove_async_tombstone_scrubbed counter\n"
        "pnfs_mds_remove_async_tombstone_scrubbed %lu\n"
        "# TYPE pnfs_mds_remove_async_depth gauge\n"
        "pnfs_mds_remove_async_depth %lu\n",
        (unsigned long)atomic_load(&branch->remove_async_acked),
        (unsigned long)atomic_load(&branch->remove_async_ack_unlinked),
        (unsigned long)atomic_load(&branch->remove_async_sync_fallback),
        (unsigned long)atomic_load(&branch->remove_async_manifest_insert_fail),
        (unsigned long)atomic_load(&branch->remove_async_drained_ok),
        (unsigned long)atomic_load(&branch->remove_async_drain_mismatch),
        (unsigned long)atomic_load(&branch->remove_async_drain_fail),
        (unsigned long)atomic_load(&branch->remove_async_force_drain),
        (unsigned long)atomic_load(&branch->remove_async_tombstone_hit),
        (unsigned long)atomic_load(&branch->remove_async_tombstone_scrubbed),
        (unsigned long)atomic_load(&branch->remove_async_depth));
    if (extra < 0 || ((size_t)base + (size_t)extra) >= cap) {
        return -1;
    }
    base += extra;

    /* Append DS-prepare counters. */
    extra = snprintf(buf + base, cap - (size_t)base,
        "# HELP pnfs_mds_ds_prepare_async_ok "
            "Async DS FH captures completed.\n"
        "# TYPE pnfs_mds_ds_prepare_async_ok counter\n"
        "pnfs_mds_ds_prepare_async_ok %lu\n"
        "# HELP pnfs_mds_ds_prepare_sync_fallback "
            "Sync DS FH capture fallbacks.\n"
        "# TYPE pnfs_mds_ds_prepare_sync_fallback counter\n"
        "pnfs_mds_ds_prepare_sync_fallback %lu\n"
        "# HELP pnfs_mds_ds_prepare_queue_depth "
            "Current DS prepare queue occupancy.\n"
        "# TYPE pnfs_mds_ds_prepare_queue_depth gauge\n"
        "pnfs_mds_ds_prepare_queue_depth %lu\n",
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->ds_prepare_async_ok),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->ds_prepare_sync_fallback),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->ds_prepare_queue_depth));

    if (extra < 0 || ((size_t)base + (size_t)extra) >= cap) {
        return -1;
    }
    base += extra;

    /* Append DS prealloc + LAYOUTGET fallback counters (Phase 12 C). */
    extra = snprintf(buf + base, cap - (size_t)base,
        "# HELP pnfs_mds_prealloc_pops_ok "
            "DS prealloc pool fast-path hits.\n"
        "# TYPE pnfs_mds_prealloc_pops_ok counter\n"
        "pnfs_mds_prealloc_pops_ok %lu\n"
        "# HELP pnfs_mds_prealloc_pops_empty "
            "DS prealloc pool misses (slow-path fallback).\n"
        "# TYPE pnfs_mds_prealloc_pops_empty counter\n"
        "pnfs_mds_prealloc_pops_empty %lu\n"
        "# HELP pnfs_mds_prealloc_pops_fh_missing "
            "Prealloc hits with FH not yet captured.\n"
        "# TYPE pnfs_mds_prealloc_pops_fh_missing counter\n"
        "pnfs_mds_prealloc_pops_fh_missing %lu\n"
        "# HELP pnfs_mds_prealloc_refill_entries "
            "Entries added by the prealloc refill thread.\n"
        "# TYPE pnfs_mds_prealloc_refill_entries counter\n"
        "pnfs_mds_prealloc_refill_entries %lu\n"
        "# HELP pnfs_mds_prealloc_refill_batches "
            "Prealloc refill batch invocations.\n"
        "# TYPE pnfs_mds_prealloc_refill_batches counter\n"
        "pnfs_mds_prealloc_refill_batches %lu\n"
        "# HELP pnfs_mds_layoutget_sync_fallback "
            "LAYOUTGETs that fell back to the synchronous FH path.\n"
        "# TYPE pnfs_mds_layoutget_sync_fallback counter\n"
        "pnfs_mds_layoutget_sync_fallback %lu\n"
        "# HELP pnfs_mds_layoutget_delay_count "
            "LAYOUTGETs that returned NFS4ERR_DELAY.\n"
        "# TYPE pnfs_mds_layoutget_delay_count counter\n"
        "pnfs_mds_layoutget_delay_count %lu\n"
        "# HELP pnfs_mds_gc_pending "
            "Lazy-delete GC-queue entries owned by this MDS awaiting "
            "DS unlink.\n"
        "# TYPE pnfs_mds_gc_pending gauge\n"
        "pnfs_mds_gc_pending %lu\n",
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->prealloc_pops_ok),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->prealloc_pops_empty),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->prealloc_pops_fh_missing),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->prealloc_refill_entries),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->prealloc_refill_batches),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->layoutget_sync_fallback),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->layoutget_delay_count),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->gc_pending));

    if (extra < 0 || ((size_t)base + (size_t)extra) >= cap) {
        return -1;
    }
    base += extra;

    /* Append NFS operation counters. */
    extra = snprintf(buf + base, cap - (size_t)base,
        "# HELP pnfs_mds_nfs_ops_total Total NFS operations.\n"
        "# TYPE pnfs_mds_nfs_ops_total counter\n"
        "pnfs_mds_nfs_ops_total %lu\n"
        "# HELP pnfs_mds_nfs_op NFS operations by type.\n"
        "# TYPE pnfs_mds_nfs_op counter\n"
        "pnfs_mds_nfs_op{op=\"create\"} %lu\n"
        "pnfs_mds_nfs_op{op=\"remove\"} %lu\n"
        "pnfs_mds_nfs_op{op=\"lookup\"} %lu\n"
        "pnfs_mds_nfs_op{op=\"getattr\"} %lu\n"
        "pnfs_mds_nfs_op{op=\"setattr\"} %lu\n"
        "pnfs_mds_nfs_op{op=\"readdir\"} %lu\n"
        "pnfs_mds_nfs_op{op=\"open\"} %lu\n"
        "pnfs_mds_nfs_op{op=\"close\"} %lu\n"
        "pnfs_mds_nfs_op{op=\"read\"} %lu\n"
        "pnfs_mds_nfs_op{op=\"write\"} %lu\n"
        "pnfs_mds_nfs_op{op=\"layoutget\"} %lu\n"
        "pnfs_mds_nfs_op{op=\"rename\"} %lu\n",
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_ops_total),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_op_create),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_op_remove),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_op_lookup),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_op_getattr),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_op_setattr),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_op_readdir),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_op_open),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_op_close),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_op_read),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_op_write),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_op_layoutget),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_op_rename));
    if (extra < 0 || ((size_t)base + (size_t)extra) >= cap) {
        return -1;
    }
    base += extra;

    extra = snprintf(buf + base, cap - (size_t)base,
        "# HELP pnfs_mds_nfs_moved_total Operations rejected with "
        "NFS4ERR_MOVED by referral_strict routing.\n"
        "# TYPE pnfs_mds_nfs_moved_total counter\n"
        "pnfs_mds_nfs_moved_total %lu\n"
        "# HELP pnfs_mds_rpc_parks_total Requests parked under worker-pool "
        "backpressure (previously dropped).\n"
        "# TYPE pnfs_mds_rpc_parks_total counter\n"
        "pnfs_mds_rpc_parks_total %lu\n",
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->nfs_moved),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->rpc_parks));
    if (extra < 0 || ((size_t)base + (size_t)extra) >= cap) {
        return -1;
    }
    base += extra;

    /*
     * Per-phase latency for OP_OPEN on the CLAIM_NULL + create
     * path.  These are nanosecond sums with matching counts
     * (Phase 1 observability for the 14 ms / create hotspot).
     * Rendered as Prometheus summary-style (_sum + _count) so
     * operators can compute mean latency per phase.
     */
    /*
     * Phase 1.1: placement dispatcher heap fallback counter.
     * Reported first in this block because it is rarely non-zero;
     * operators scanning /metrics grep for it to spot unexpected
     * ds_count > 64 deployments.
     */
    extra = snprintf(buf + base, cap - (size_t)base,
        "# HELP pnfs_mds_placement_heap_fallback_total "
            "Placement dispatcher heap-fallback count "
            "(ds_count > MDS_PLACEMENT_STACK_MAX).\n"
        "# TYPE pnfs_mds_placement_heap_fallback_total counter\n"
        "pnfs_mds_placement_heap_fallback_total %lu\n"
        "# HELP pnfs_mds_placement_degraded_total "
            "Placement degraded count (stripe*mirror > ONLINE DSes).\n"
        "# TYPE pnfs_mds_placement_degraded_total counter\n"
        "pnfs_mds_placement_degraded_total %lu\n",
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->placement_heap_fallback_total),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->placement_degraded_total));
    if (extra < 0 || ((size_t)base + (size_t)extra) >= cap) {
        return -1;
    }
    base += extra;

    extra = snprintf(buf + base, cap - (size_t)base,
        "# HELP pnfs_mds_io_advise_total Total IO_ADVISE ops served.\n"
        "# TYPE pnfs_mds_io_advise_total counter\n"
        "pnfs_mds_io_advise_total %lu\n"
        "# HELP pnfs_mds_io_advise_hint "
            "IO_ADVISE hints actually acted on, by flavour.\n"
        "# TYPE pnfs_mds_io_advise_hint counter\n"
        "pnfs_mds_io_advise_hint{type=\"willneed\"} %lu\n"
        "pnfs_mds_io_advise_hint{type=\"dontneed\"} %lu\n",
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->io_advise_total),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->io_advise_willneed),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->io_advise_dontneed));
    if (extra < 0 || ((size_t)base + (size_t)extra) >= cap) {
        return -1;
    }
    base += extra;

    extra = snprintf(buf + base, cap - (size_t)base,
        "# HELP pnfs_mds_open_create_phase_ns_sum "
            "Sum of nanoseconds per CLAIM_NULL create phase.\n"
        "# TYPE pnfs_mds_open_create_phase_ns_sum counter\n"
        "pnfs_mds_open_create_phase_ns_sum{phase=\"parent_get\"} %lu\n"
        "pnfs_mds_open_create_phase_ns_sum{phase=\"lookup\"} %lu\n"
        "pnfs_mds_open_create_phase_ns_sum{phase=\"ns_create\"} %lu\n"
        "pnfs_mds_open_create_phase_ns_sum{phase=\"ds_prepare\"} %lu\n"
        "pnfs_mds_open_create_phase_ns_sum{phase=\"state_open\"} %lu\n"
        "pnfs_mds_open_create_phase_ns_sum{phase=\"total\"} %lu\n"
        "# HELP pnfs_mds_open_create_phase_count "
            "Count of CLAIM_NULL create phase observations.\n"
        "# TYPE pnfs_mds_open_create_phase_count counter\n"
        "pnfs_mds_open_create_phase_count{phase=\"parent_get\"} %lu\n"
        "pnfs_mds_open_create_phase_count{phase=\"lookup\"} %lu\n"
        "pnfs_mds_open_create_phase_count{phase=\"ns_create\"} %lu\n"
        "pnfs_mds_open_create_phase_count{phase=\"ds_prepare\"} %lu\n"
        "pnfs_mds_open_create_phase_count{phase=\"state_open\"} %lu\n"
        "pnfs_mds_open_create_phase_count{phase=\"total\"} %lu\n",
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->open_create_parent_ns_sum),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->open_create_lookup_ns_sum),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->open_create_ns_create_ns_sum),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->open_create_ds_prepare_ns_sum),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->open_create_state_open_ns_sum),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->open_create_total_ns_sum),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->open_create_parent_count),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->open_create_lookup_count),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->open_create_ns_create_count),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->open_create_ds_prepare_count),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->open_create_state_open_count),
        (unsigned long)atomic_load(
            (_Atomic uint64_t *)&branch->open_create_total_count));
    if (extra > 0 && (size_t)extra < cap - (size_t)base) { base += extra; }
    if (extra < 0 || ((size_t)base + (size_t)extra) >= cap) {
        return -1;
    }
    base += extra;

    /*
     * Append the RPC dispatcher section if a threadpool has been
     * registered (see mds_metrics_set_rpc_threadpool).  Emits 0
     * bytes when unregistered, so community builds are unaffected.
     */
    extra = render_rpc_threadpool(buf + base, cap - (size_t)base);
    if (extra < 0) {
        return -1;
    }
    base += extra;

    /*
     * Append per-NFS-op + per-catalogue-op + per-op*phase
     * latency histograms.  These are always-on; the renderer
     * skips (op, phase) cells that have no observations so the
     * output stays compact when nothing has happened yet.
     */
    extra = mds_op_metrics_render(buf + base, cap - (size_t)base);
    if (extra < 0) {
        return -1;
    }
    return base + extra;
}

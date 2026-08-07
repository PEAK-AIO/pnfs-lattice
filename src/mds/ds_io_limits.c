/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_io_limits.c -- per-DS I/O limits store and FSINFO prober
 * (Wave 5 T5.1).  See ds_io_limits.h for the policy contract.
 *
 * Concurrency: the per-DS table is guarded by one mutex (probe-rate
 * access only); the hot-path getters read pre-computed values -- the
 * per-DS records under the mutex for GETDEVICEINFO/LAYOUTGET, and
 * relaxed atomics for the min-across-DSes pair consumed by the
 * GETATTR MAXREAD/MAXWRITE encoder.
 *
 * Publish-before-recall: a capability decrease stores the new safe
 * values (and bumps the device-ID generation) under the table mutex
 * BEFORE layout_recall_for_ds() runs, so a client racing the recall
 * with LAYOUTGET/GETDEVICEINFO can only observe the new, safe state.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <signal.h>
#include <poll.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "ds_io_limits.h"
#include "ds_nfs_rpc.h"
#include "ds_health.h"      /* ds_addr_parse_host() */
#include "layout_recall.h"
#include "mds_metrics.h"

/** Per-probe RPC timeout (matches the FH-capture RPC callers). */
#define DS_IOLIMIT_PROBE_TIMEOUT_MS 3000U

struct ds_limit_rec {
    bool     present;    /* ds_id seen by a sweep at least once */
    bool     verified;   /* at least one successful probe */
    bool     eligible;   /* false: decoded limits < 4 KiB */
    uint32_t rsize;      /* effective advertised read limit */
    uint32_t wsize;      /* effective advertised write limit */
    uint32_t generation; /* device-ID generation (bumps on change) */
    uint32_t fail_count; /* consecutive probe failures */
};

static struct {
    pthread_mutex_t      lock;
    struct ds_limit_rec  recs[MDS_MAX_DS_NODES];
    _Atomic bool         enabled;
    _Atomic uint32_t     min_rsize;   /* legacy value when no entries */
    _Atomic uint32_t     min_wsize;
    ds_iolimit_probe_fn  probe_fn;    /* NULL = ds_nfs3_fsinfo */
    struct layout_recall *lr;         /* borrowed; may be NULL */

    /* Prober thread. */
    pthread_t            thread;
    _Atomic int          running;
    int                  stop_pipe[2];
    struct mds_catalogue *cat;        /* borrowed */
    uint32_t             interval_ms;
} g_dsl = {
    .lock = PTHREAD_MUTEX_INITIALIZER,
    .min_rsize = DS_IOLIMIT_LEGACY_BYTES,
    .min_wsize = DS_IOLIMIT_LEGACY_BYTES,
    .stop_pipe = { -1, -1 },
};

/* -----------------------------------------------------------------------
 * Policy helpers
 * ----------------------------------------------------------------------- */

/** Cap at 1 MiB and round down to a 4 KiB multiple. */
static uint32_t policy_effective(uint32_t raw)
{
    if (raw > DS_IOLIMIT_CAP_BYTES) {
        raw = DS_IOLIMIT_CAP_BYTES;
    }
    return raw - (raw % DS_IOLIMIT_ROUND_BYTES);
}

/** Recompute the min-across-eligible-DSes pair.  Caller holds lock. */
static void recompute_minima_locked(void)
{
    uint32_t min_r = 0;
    uint32_t min_w = 0;
    bool any = false;

    for (uint32_t i = 0; i < MDS_MAX_DS_NODES; i++) {
        const struct ds_limit_rec *rec = &g_dsl.recs[i];

        if (!rec->present || !rec->eligible) {
            continue;
        }
        if (!any || rec->rsize < min_r) {
            min_r = rec->rsize;
        }
        if (!any || rec->wsize < min_w) {
            min_w = rec->wsize;
        }
        any = true;
    }
    if (!any) {
        min_r = DS_IOLIMIT_LEGACY_BYTES;
        min_w = DS_IOLIMIT_LEGACY_BYTES;
    }
    atomic_store_explicit(&g_dsl.min_rsize, min_r, memory_order_relaxed);
    atomic_store_explicit(&g_dsl.min_wsize, min_w, memory_order_relaxed);
    atomic_store_explicit(&g_branch_metrics.ds_iolimit_min_read,
                          min_r, memory_order_relaxed);
    atomic_store_explicit(&g_branch_metrics.ds_iolimit_min_write,
                          min_w, memory_order_relaxed);
}

/**
 * Fold one probe outcome into the table.  Returns true when the
 * change is a capability DECREASE (or eligibility loss) that must
 * recall the DS's outstanding layouts.  The new state is published
 * before this returns -- the caller recalls strictly afterwards.
 */
static bool apply_probe_result(uint32_t ds_id, bool probe_ok,
                               uint32_t raw_rt, uint32_t raw_wt)
{
    struct ds_limit_rec *rec;
    uint32_t old_r;
    uint32_t old_w;
    bool old_elig;
    uint32_t eff_r;
    uint32_t eff_w;
    bool new_elig;
    bool need_recall = false;

    if (ds_id >= MDS_MAX_DS_NODES) {
        return false;
    }

    pthread_mutex_lock(&g_dsl.lock);
    rec = &g_dsl.recs[ds_id];

    if (!rec->present) {
        /* First sighting: what we have been advertising so far is
         * the unverified fallback. */
        rec->present = true;
        rec->verified = false;
        rec->eligible = true;
        rec->rsize = DS_IOLIMIT_FALLBACK_BYTES;
        rec->wsize = DS_IOLIMIT_FALLBACK_BYTES;
        rec->generation = 0;
        rec->fail_count = 0;
        recompute_minima_locked();
    }

    if (!probe_ok) {
        /* Keep last-known-good (or the fallback when never
         * verified); never shrink on a lost probe. */
        rec->fail_count++;
        atomic_fetch_add_explicit(
            &g_branch_metrics.ds_iolimit_probe_failures, 1,
            memory_order_relaxed);
        pthread_mutex_unlock(&g_dsl.lock);
        return false;
    }

    old_r = rec->rsize;
    old_w = rec->wsize;
    old_elig = rec->eligible;

    eff_r = policy_effective(raw_rt);
    eff_w = policy_effective(raw_wt);
    new_elig = (eff_r >= DS_IOLIMIT_ROUND_BYTES &&
                eff_w >= DS_IOLIMIT_ROUND_BYTES);

    rec->fail_count = 0;

    if (!new_elig) {
        /* Broken DS (decoded limits below 4 KiB).  Keep the last
         * advertised values for already-granted layouts, drop the
         * DS from new placement, and recall so clients re-drive
         * LAYOUTGET away from it. */
        if (old_elig) {
            rec->eligible = false;
            rec->generation++;
            recompute_minima_locked();
            need_recall = true;
            MDS_LOG_WARN(LOG_COMP_MDS,
                "DS %u FSINFO reports unusable I/O limits "
                "(rtmax=%u wtmax=%u) -- ineligible for new "
                "layouts (gen=%u)",
                (unsigned)ds_id, (unsigned)raw_rt,
                (unsigned)raw_wt, (unsigned)rec->generation);
        }
        pthread_mutex_unlock(&g_dsl.lock);
        return need_recall;
    }

    if (!rec->verified || eff_r != old_r || eff_w != old_w ||
        !old_elig) {
        bool shrink = rec->verified &&
                      (eff_r < old_r || eff_w < old_w);

        rec->verified = true;
        rec->eligible = true;
        rec->rsize = eff_r;
        rec->wsize = eff_w;
        rec->generation++;
        recompute_minima_locked();
        /* A decrease relative to what clients may already be using
         * must recall; growth and the first verification only need
         * the generation bump (new layouts pick up the new ID). */
        need_recall = shrink;
        MDS_LOG_INFO(LOG_COMP_MDS,
            "DS %u I/O limits: rsize=%u wsize=%u (raw %u/%u, "
            "gen=%u%s)",
            (unsigned)ds_id, (unsigned)eff_r, (unsigned)eff_w,
            (unsigned)raw_rt, (unsigned)raw_wt,
            (unsigned)rec->generation,
            shrink ? ", DECREASE -> recall" : "");
    }

    pthread_mutex_unlock(&g_dsl.lock);
    return need_recall;
}

/* -----------------------------------------------------------------------
 * Public getters
 * ----------------------------------------------------------------------- */

void ds_io_limits_get(uint32_t ds_id, uint32_t *rsize, uint32_t *wsize,
                      uint32_t *generation)
{
    uint32_t r = DS_IOLIMIT_LEGACY_BYTES;
    uint32_t w = DS_IOLIMIT_LEGACY_BYTES;
    uint32_t gen = 0;

    if (atomic_load_explicit(&g_dsl.enabled, memory_order_acquire) &&
        ds_id < MDS_MAX_DS_NODES) {
        pthread_mutex_lock(&g_dsl.lock);
        if (g_dsl.recs[ds_id].present) {
            r = g_dsl.recs[ds_id].rsize;
            w = g_dsl.recs[ds_id].wsize;
            gen = g_dsl.recs[ds_id].generation;
        } else {
            r = DS_IOLIMIT_FALLBACK_BYTES;
            w = DS_IOLIMIT_FALLBACK_BYTES;
        }
        pthread_mutex_unlock(&g_dsl.lock);
    }

    if (rsize != NULL) {
        *rsize = r;
    }
    if (wsize != NULL) {
        *wsize = w;
    }
    if (generation != NULL) {
        *generation = gen;
    }
}

void ds_io_limits_min(uint32_t *maxread, uint32_t *maxwrite)
{
    uint32_t r = DS_IOLIMIT_LEGACY_BYTES;
    uint32_t w = DS_IOLIMIT_LEGACY_BYTES;

    if (atomic_load_explicit(&g_dsl.enabled, memory_order_acquire)) {
        r = atomic_load_explicit(&g_dsl.min_rsize, memory_order_relaxed);
        w = atomic_load_explicit(&g_dsl.min_wsize, memory_order_relaxed);
    }
    if (maxread != NULL) {
        *maxread = r;
    }
    if (maxwrite != NULL) {
        *maxwrite = w;
    }
}

bool ds_io_limits_eligible(uint32_t ds_id)
{
    bool elig = true;

    if (!atomic_load_explicit(&g_dsl.enabled, memory_order_acquire) ||
        ds_id >= MDS_MAX_DS_NODES) {
        return true;
    }
    pthread_mutex_lock(&g_dsl.lock);
    if (g_dsl.recs[ds_id].present) {
        elig = g_dsl.recs[ds_id].eligible;
    }
    pthread_mutex_unlock(&g_dsl.lock);
    return elig;
}

/* -----------------------------------------------------------------------
 * Sweep
 * ----------------------------------------------------------------------- */

void ds_io_limits_sweep(struct mds_catalogue *cat)
{
    struct mds_ds_info *ds_list = NULL;
    uint32_t ds_count = 0;
    ds_iolimit_probe_fn probe;

    if (cat == NULL ||
        !atomic_load_explicit(&g_dsl.enabled, memory_order_acquire)) {
        return;
    }
    probe = g_dsl.probe_fn != NULL ? g_dsl.probe_fn : ds_nfs3_fsinfo;

    if (mds_cat_ds_list(cat, &ds_list, &ds_count) != MDS_OK) {
        return;
    }

    for (uint32_t i = 0; i < ds_count; i++) {
        char host[MDS_DS_ADDR_MAX];
        const char *export_path;
        uint16_t port;
        uint32_t raw_rt = 0;
        uint32_t raw_wt = 0;
        bool ok;
        bool recall;

        if (ds_list[i].state != DS_ONLINE ||
            ds_list[i].mode != DS_MODE_GENERIC) {
            continue;
        }
        if (ds_addr_parse_host(ds_list[i].addr, host,
                               sizeof(host)) != 0) {
            continue;
        }
        export_path = strchr(ds_list[i].addr, ':');
        if (export_path == NULL || export_path[1] != '/') {
            continue;
        }
        export_path++;
        port = (ds_list[i].port != 0) ? ds_list[i].port : 2049;

        ok = (probe(host, port, export_path, &raw_rt, &raw_wt,
                    DS_IOLIMIT_PROBE_TIMEOUT_MS) == 0);

        recall = apply_probe_result(ds_list[i].ds_id, ok,
                                    raw_rt, raw_wt);

        /* Publish-before-recall: the new state is already visible;
         * now force holders to re-drive LAYOUTGET/GETDEVICEINFO. */
        if (recall && g_dsl.lr != NULL) {
            atomic_fetch_add_explicit(
                &g_branch_metrics.ds_iolimit_capability_recalls, 1,
                memory_order_relaxed);
            (void)layout_recall_for_ds(g_dsl.lr, ds_list[i].ds_id);
        }
    }
    free(ds_list);
}

/* -----------------------------------------------------------------------
 * Prober thread
 * ----------------------------------------------------------------------- */

static void *iolimit_poll_thread(void *arg)
{
    (void)arg;

    /* Block all signals: the NDB API wakes internal threads with
     * signals that would interrupt our probe sockets (same rationale
     * as the DS health poll thread). */
    {
        sigset_t mask;

        sigfillset(&mask);
        pthread_sigmask(SIG_BLOCK, &mask, NULL);
    }

    /* Startup probe so the fallback window is one sweep, not one
     * interval. */
    ds_io_limits_sweep(g_dsl.cat);

    while (atomic_load(&g_dsl.running)) {
        struct pollfd pfd = {
            .fd = g_dsl.stop_pipe[0],
            .events = POLLIN,
        };
        int pr = poll(&pfd, 1, (int)g_dsl.interval_ms);

        if (pr > 0) {
            break; /* stop signal */
        }
        if (!atomic_load(&g_dsl.running)) {
            break;
        }
        ds_io_limits_sweep(g_dsl.cat);
    }
    return NULL;
}

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */

int ds_io_limits_enable(void)
{
    atomic_store_explicit(&g_dsl.enabled, true, memory_order_release);
    return 0;
}

bool ds_io_limits_enabled(void)
{
    return atomic_load_explicit(&g_dsl.enabled, memory_order_acquire);
}

void ds_io_limits_set_probe_fn(ds_iolimit_probe_fn fn)
{
    g_dsl.probe_fn = fn;
}

void ds_io_limits_set_recall(struct layout_recall *lr)
{
    g_dsl.lr = lr;
}

int ds_io_limits_start(struct mds_catalogue *cat, uint32_t interval_ms)
{
    if (cat == NULL || interval_ms == 0 || !ds_io_limits_enabled()) {
        return -1;
    }
    if (atomic_load(&g_dsl.running)) {
        return 0; /* already running */
    }
    if (pipe(g_dsl.stop_pipe) != 0) {
        return -1;
    }
    g_dsl.cat = cat;
    g_dsl.interval_ms = interval_ms;
    atomic_store(&g_dsl.running, 1);
    if (pthread_create(&g_dsl.thread, NULL, iolimit_poll_thread,
                       NULL) != 0) {
        atomic_store(&g_dsl.running, 0);
        close(g_dsl.stop_pipe[0]);
        close(g_dsl.stop_pipe[1]);
        g_dsl.stop_pipe[0] = -1;
        g_dsl.stop_pipe[1] = -1;
        return -1;
    }
    return 0;
}

void ds_io_limits_stop(void)
{
    if (!atomic_load(&g_dsl.running)) {
        return;
    }
    atomic_store(&g_dsl.running, 0);
    if (g_dsl.stop_pipe[1] >= 0) {
        uint8_t b = 1;

        (void)!write(g_dsl.stop_pipe[1], &b, 1);
    }
    pthread_join(g_dsl.thread, NULL);
    if (g_dsl.stop_pipe[0] >= 0) {
        close(g_dsl.stop_pipe[0]);
        close(g_dsl.stop_pipe[1]);
        g_dsl.stop_pipe[0] = -1;
        g_dsl.stop_pipe[1] = -1;
    }
}

void ds_io_limits_shutdown(void)
{
    ds_io_limits_stop();
    pthread_mutex_lock(&g_dsl.lock);
    memset(g_dsl.recs, 0, sizeof(g_dsl.recs));
    pthread_mutex_unlock(&g_dsl.lock);
    atomic_store_explicit(&g_dsl.enabled, false, memory_order_release);
    atomic_store_explicit(&g_dsl.min_rsize, DS_IOLIMIT_LEGACY_BYTES,
                          memory_order_relaxed);
    atomic_store_explicit(&g_dsl.min_wsize, DS_IOLIMIT_LEGACY_BYTES,
                          memory_order_relaxed);
    g_dsl.probe_fn = NULL;
    g_dsl.lr = NULL;
    g_dsl.cat = NULL;
}

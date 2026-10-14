/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * failover_watchdog.c -- Partner-liveness watchdog.
 *
 * Replaces the removed LMDB-delta-shipping health signal that used
 * to tell the standby when its partner died.  Every MDS runs a
 * heartbeat thread (cluster_hb_fn in main.c, over
 * mds_cluster_node_heartbeat) that refreshes its node-registry row's
 * last_heartbeat_ns every 5 seconds.  The watchdog here is the reader
 * side: on the standby, a background thread periodically scans the
 * registry for stale rows and fires failover_promote when the partner
 * has missed ceil(stale_timeout_ms / heartbeat_interval_ms) intervals.
 *
 * Design
 *
 *   - One detached-joinable pthread per standby MDS.  Runs for the
 *     life of the daemon unless failover_watchdog_stop() is called.
 *   - Poll cadence defaults to 2 seconds (faster than the heartbeat
 *     interval so a dead partner is observed within ~5-15 s end-to-end).
 *   - Stale threshold defaults to 15 seconds = 3 missed heartbeats.
 *     Configurable via failover_watchdog_cfg.stale_timeout_ms.
 *   - After a successful promotion, the thread self-exits -- there is
 *     nothing left to watch (a promoted node is the primary and has
 *     no partner).
 *   - Uses the backend-neutral mds_cluster_node_scan_stale(threshold_ns)
 *     dispatcher (mds_cluster.h), which reports rows where
 *     last_heartbeat_ns < threshold.  A backend without that cluster
 *     slot cannot host the watchdog: failover_watchdog_start refuses
 *     with MDS_ERR_NOSUPPORT instead of starting a thread that would
 *     never observe anything.
 *
 * Safety
 *
 *   - Self-fencing + replication-health gates live inside
 *     failover_promote(); the watchdog only triggers it, never
 *     bypasses the checks.
 *   - If clock_gettime fails or the scan returns an error, the
 *     watchdog skips the tick rather than guessing that the partner
 *     is dead.  Guessing wrong fires a spurious promotion, which is
 *     more expensive than waiting one more cycle.
 *   - Bootup grace: the first watchdog_min_observe_ms after startup
 *     we refuse to promote even if the partner's row is missing or
 *     stale.  Covers the case where the standby came up before the
 *     primary finished initial heartbeat insertion.
 *   - Clock domain: the threshold is derived from this host's
 *     CLOCK_REALTIME and the writers stamp their CLOCK_REALTIME
 *     (mds_cluster.h), so the stale threshold must exceed the
 *     deployment's NTP skew bound.  A partner row whose timestamp is
 *     below FAILOVER_HB_REALTIME_FLOOR_NS (failover_watchdog.h) was
 *     written in the old CLOCK_MONOTONIC domain by a not-yet-upgraded
 *     primary; it is INDETERMINATE -- the tick is skipped and the
 *     partner is never declared stale on its account -- so a rolling
 *     upgrade cannot promote the standby against a live primary.
 */

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "pnfs_mds.h"
#include "failover.h"
#include "failover_watchdog.h"
#include "mds_cluster.h"

/* -----------------------------------------------------------------------
 * Tunables (compile-time defaults; can be overridden via the cfg struct)
 * ----------------------------------------------------------------------- */

#define WATCHDOG_POLL_INTERVAL_MS_DEFAULT      2000u
#define WATCHDOG_MIN_OBSERVE_MS_DEFAULT       20000u

/* -----------------------------------------------------------------------
 * Handle layout
 * ----------------------------------------------------------------------- */

struct failover_watchdog {
	struct failover_ctx   *fo;
	struct mds_catalogue  *cat;
	uint32_t               partner_id;
	uint32_t               poll_interval_ms;
	uint32_t               stale_timeout_ms;
	uint32_t               min_observe_ms;
	pthread_t              thread;
	_Atomic bool           running;
	_Atomic bool           started;
	uint64_t               start_ts_ns;
	/* Indeterminate-partner log edge; touched by the watchdog thread
	 * only, so it needs no synchronisation. */
	bool                   indeterminate_logged;
};

/* Per-tick scan context. */
struct scan_ctx {
	uint32_t partner_id;
	bool     partner_stale_found;   /* stale AND in the realtime domain */
	bool     partner_indeterminate; /* below the realtime floor */
	uint64_t partner_hb_ns;         /* for the indeterminate log line */
};

bool failover_heartbeat_plausible(uint64_t last_heartbeat_ns)
{
	return last_heartbeat_ns >= FAILOVER_HB_REALTIME_FLOOR_NS;
}

static uint64_t clock_now_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
		return 0;
	}
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* The store reports every row below the threshold, so a partner row
 * still stamped in the old CLOCK_MONOTONIC domain is always reported;
 * classify it here rather than count it as stale. */
static int watchdog_scan_cb(uint32_t mds_id, uint64_t boot_epoch,
			    uint64_t last_heartbeat_ns, void *ctx_void)
{
	struct scan_ctx *ctx = ctx_void;

	(void)boot_epoch;

	if (ctx == NULL) {
		return 1;
	}
	if (mds_id == ctx->partner_id) {
		ctx->partner_hb_ns = last_heartbeat_ns;
		if (failover_heartbeat_plausible(last_heartbeat_ns)) {
			ctx->partner_stale_found = true;
		} else {
			ctx->partner_indeterminate = true;
		}
		return 1; /* found what we need; stop scanning */
	}
	return 0;
}

/* Sleep one poll interval.  An interrupted sleep only shortens the
 * wait; the loop re-checks `running` right after. */
static void watchdog_sleep(const struct failover_watchdog *wd)
{
	struct timespec sleep_ts;

	sleep_ts.tv_sec  = (time_t)(wd->poll_interval_ms / 1000u);
	sleep_ts.tv_nsec = (long)((wd->poll_interval_ms % 1000u) *
				  1000000u);
	(void)nanosleep(&sleep_ts, NULL);
}

/* Boot-up grace: give the primary time to insert its own initial
 * heartbeat row. */
static bool watchdog_in_grace(const struct failover_watchdog *wd,
			      uint64_t now_ns)
{
	return wd->start_ts_ns != 0 &&
	       now_ns - wd->start_ts_ns <
		       (uint64_t)wd->min_observe_ms * 1000000ULL;
}

/* Rows older than this are stale; clamps at 0 so an early clock can
 * never underflow. */
static uint64_t watchdog_threshold_ns(const struct failover_watchdog *wd,
				      uint64_t now_ns)
{
	uint64_t stale_ns = (uint64_t)wd->stale_timeout_ms * 1000000ULL;

	return (now_ns > stale_ns) ? now_ns - stale_ns : 0;
}

/* One observation of the partner.  True only when the partner's row is
 * stale AND stamped in the realtime domain.  A scan failure skips the
 * tick silently (transient); an indeterminate row skips it and is
 * logged once per transition -- guessing wrong here fires a spurious
 * promotion, which is more expensive than waiting one more cycle. */
static bool watchdog_partner_is_stale(struct failover_watchdog *wd,
				      uint64_t now_ns)
{
	struct scan_ctx sctx;
	enum mds_status st;

	memset(&sctx, 0, sizeof(sctx));
	sctx.partner_id = wd->partner_id;

	st = mds_cluster_node_scan_stale(wd->cat,
					 watchdog_threshold_ns(wd, now_ns),
					 watchdog_scan_cb, &sctx);
	if (st != MDS_OK) {
		return false;
	}
	if (sctx.partner_indeterminate) {
		if (!wd->indeterminate_logged) {
			MDS_LOG_WARN(LOG_COMP_CLUSTER,
				"failover_watchdog: partner %u heartbeat "
				"%llu ns is below the realtime floor "
				"(pre-upgrade writer clock?); tick skipped, "
				"partner not declared stale",
				(unsigned)wd->partner_id,
				(unsigned long long)sctx.partner_hb_ns);
			wd->indeterminate_logged = true;
		}
		return false;
	}
	if (wd->indeterminate_logged) {
		MDS_LOG_INFO(LOG_COMP_CLUSTER,
			"failover_watchdog: partner %u heartbeat is back in "
			"the realtime domain", (unsigned)wd->partner_id);
		wd->indeterminate_logged = false;
	}
	return sctx.partner_stale_found;
}

/* The partner is stale: attempt the promotion.  True when it succeeded
 * and the watchdog is done; false to loop and re-poll -- the precheck
 * guards in failover_promote (self-fencing, repl health, wire compat)
 * are the right place for the retry decision; the watchdog's job is
 * just to keep observing. */
static bool watchdog_try_promote(const struct failover_watchdog *wd)
{
	enum mds_status st;

	MDS_LOG_INFO(LOG_COMP_CLUSTER,
		"failover_watchdog: partner %u heartbeat stale > %u ms, "
		"attempting promotion",
		(unsigned)wd->partner_id,
		(unsigned)wd->stale_timeout_ms);

	st = failover_promote(wd->fo);
	if (st == MDS_OK) {
		MDS_LOG_INFO(LOG_COMP_CLUSTER,
			"failover_watchdog: promotion succeeded; "
			"watchdog exiting");
		return true;
	}

	MDS_LOG_INFO(LOG_COMP_CLUSTER,
		"failover_watchdog: promotion refused (st=%d); "
		"will retry next tick",
		(int)st);
	return false;
}

static void *watchdog_fn(void *arg)
{
	struct failover_watchdog *wd = arg;

	while (atomic_load(&wd->running)) {
		uint64_t now_ns;

		/* Sleep first so the initial tick respects min_observe_ms. */
		watchdog_sleep(wd);

		if (!atomic_load(&wd->running)) {
			break;
		}

		/* Only the STANDBY role is eligible for promotion. */
		if (failover_get_role(wd->fo) != FAILOVER_STANDBY) {
			break;
		}

		now_ns = clock_now_ns();
		if (now_ns == 0) {
			continue;
		}
		if (watchdog_in_grace(wd, now_ns)) {
			continue;
		}
		if (!watchdog_partner_is_stale(wd, now_ns)) {
			continue;
		}
		if (watchdog_try_promote(wd)) {
			break;
		}
	}

	return NULL;
}

enum mds_status failover_watchdog_start(const struct failover_watchdog_cfg *cfg,
					struct failover_watchdog **out)
{
	struct failover_watchdog *wd;

	if (cfg == NULL || out == NULL) {
		return MDS_ERR_INVAL;
	}
	*out = NULL;
	if (cfg->fo == NULL || cfg->cat == NULL || cfg->partner_id == 0) {
		return MDS_ERR_INVAL;
	}
	/* The only catalogue dependency is the stale-node scan.  Without
	 * it every tick would be a NOSUPPORT skip and the standby would
	 * silently never promote, so refuse up front (C5: no fallback
	 * that weakens what the caller believes is armed). */
	if (!mds_cluster_stale_scan_supported(cfg->cat)) {
		return MDS_ERR_NOSUPPORT;
	}

	wd = calloc(1, sizeof(*wd));
	if (wd == NULL) {
		return MDS_ERR_NOMEM;
	}

	wd->fo               = cfg->fo;
	wd->cat              = cfg->cat;
	wd->partner_id       = cfg->partner_id;
	wd->poll_interval_ms = cfg->poll_interval_ms > 0
			     ? cfg->poll_interval_ms
			     : WATCHDOG_POLL_INTERVAL_MS_DEFAULT;
	wd->stale_timeout_ms = cfg->stale_timeout_ms > 0
			     ? cfg->stale_timeout_ms
			     : FAILOVER_WATCHDOG_STALE_TIMEOUT_MS_DEFAULT;
	wd->min_observe_ms   = cfg->min_observe_ms > 0
			     ? cfg->min_observe_ms
			     : WATCHDOG_MIN_OBSERVE_MS_DEFAULT;

	atomic_store(&wd->running, true);
	wd->start_ts_ns = clock_now_ns();

	if (pthread_create(&wd->thread, NULL, watchdog_fn, wd) != 0) {
		free(wd);
		return MDS_ERR_NOMEM;
	}
	atomic_store(&wd->started, true);

	*out = wd;
	return MDS_OK;
}

void failover_watchdog_stop(struct failover_watchdog *wd)
{
	if (wd == NULL) {
		return;
	}
	if (atomic_load(&wd->started)) {
		atomic_store(&wd->running, false);
		(void)pthread_join(wd->thread, NULL);
	}
	free(wd);
}

/* -----------------------------------------------------------------------
 * Writer-side heartbeat tick (see failover_watchdog.h)
 * ----------------------------------------------------------------------- */

struct superseder_lookup {
	uint32_t mds_id;
	uint64_t boot_epoch;
};

static int superseder_cb(uint32_t mds_id, uint64_t boot_epoch,
			 const char *hostname, uint16_t nfs_port,
			 uint16_t grpc_port, uint64_t last_heartbeat_ns,
			 void *ctx_void)
{
	struct superseder_lookup *ctx = ctx_void;

	(void)hostname;
	(void)nfs_port;
	(void)grpc_port;
	(void)last_heartbeat_ns;
	if (mds_id != ctx->mds_id) {
		return 0;
	}
	ctx->boot_epoch = boot_epoch;
	return 1;
}

enum mds_status cluster_heartbeat_tick(struct mds_catalogue *cat,
				       uint32_t mds_id, uint64_t boot_epoch,
				       uint64_t *superseding_epoch)
{
	enum mds_status st;

	if (superseding_epoch != NULL) {
		*superseding_epoch = 0;
	}
	if (cat == NULL) {
		return MDS_ERR_INVAL;
	}
	st = mds_cluster_node_heartbeat(cat, mds_id, boot_epoch);
	if (st == MDS_ERR_STALE && superseding_epoch != NULL) {
		struct superseder_lookup ctx = { .mds_id = mds_id,
						 .boot_epoch = 0 };

		/* Best effort: the fence decision is already made by the
		 * STALE; the read-back only names the row for the log. */
		(void)mds_cluster_node_list(cat, superseder_cb, &ctx);
		*superseding_epoch = ctx.boot_epoch;
	}
	return st;
}

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * failover_rondb.c -- RonDB-native partner-liveness watchdog.
 *
 * Replaces the removed LMDB-delta-shipping health signal that used
 * to tell the standby when its partner died.  RonDB already has a
 * per-MDS heartbeat thread (see catalogue_rondb_mds_heartbeat +
 * rondb_hb_fn in main.c) that updates mds_node_registry.last_heartbeat_ns
 * every 5 seconds.  The watchdog here is the reader side: on the
 * standby, a background thread periodically scans the registry for
 * stale rows and fires failover_promote when the partner has missed
 * ceil(stale_timeout_ms / heartbeat_interval_ms) intervals.
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
 *   - Uses the existing catalogue_rondb_mds_scan_stale(threshold_ns)
 *     shim which returns rows where last_heartbeat_ns < threshold.
 *     No new RonDB wire calls required.
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
 */

#ifdef HAVE_RONDB

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
#include "failover_rondb.h"
#include "catalogue_rondb.h"

/* -----------------------------------------------------------------------
 * Tunables (compile-time defaults; can be overridden via the cfg struct)
 * ----------------------------------------------------------------------- */

#define WATCHDOG_POLL_INTERVAL_MS_DEFAULT      2000u
#define WATCHDOG_STALE_TIMEOUT_MS_DEFAULT     15000u
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
};

/* Per-tick scan context. */
struct scan_ctx {
	uint32_t partner_id;
	bool     partner_stale_found;
};

static uint64_t clock_now_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
		return 0;
	}
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static int watchdog_scan_cb(uint32_t mds_id, uint64_t boot_epoch,
			    uint64_t last_heartbeat_ns, void *ctx_void)
{
	struct scan_ctx *ctx = ctx_void;

	(void)boot_epoch;
	(void)last_heartbeat_ns;

	if (ctx == NULL) {
		return 1;
	}
	if (mds_id == ctx->partner_id) {
		ctx->partner_stale_found = true;
		return 1; /* found what we need; stop scanning */
	}
	return 0;
}

static void *watchdog_fn(void *arg)
{
	struct failover_watchdog *wd = arg;

	while (atomic_load(&wd->running)) {
		struct timespec sleep_ts;
		uint64_t now_ns;
		uint64_t threshold_ns;
		struct scan_ctx sctx;
		enum mds_status st;

		/* Sleep first so the initial tick respects min_observe_ms. */
		sleep_ts.tv_sec  = (time_t)(wd->poll_interval_ms / 1000u);
		sleep_ts.tv_nsec = (long)((wd->poll_interval_ms % 1000u) *
					  1000000u);
		(void)nanosleep(&sleep_ts, NULL);

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

		/* Boot-up grace: give the primary time to insert its own
		 * initial heartbeat row. */
		if (wd->start_ts_ns != 0 &&
		    now_ns - wd->start_ts_ns <
			    (uint64_t)wd->min_observe_ms * 1000000ULL) {
			continue;
		}

		threshold_ns = (now_ns > (uint64_t)wd->stale_timeout_ms *
					 1000000ULL)
			     ? now_ns - (uint64_t)wd->stale_timeout_ms *
					1000000ULL
			     : 0;

		memset(&sctx, 0, sizeof(sctx));
		sctx.partner_id = wd->partner_id;

		st = catalogue_rondb_mds_scan_stale(wd->cat, threshold_ns,
						    watchdog_scan_cb, &sctx);
		if (st != MDS_OK) {
			/* Transient scan failure: skip this tick. */
			continue;
		}

		if (!sctx.partner_stale_found) {
			continue;
		}

		(void)fprintf(stderr,
			"failover_rondb: partner %u heartbeat stale > %u ms, "
			"attempting promotion\n",
			(unsigned)wd->partner_id,
			(unsigned)wd->stale_timeout_ms);

		st = failover_promote(wd->fo);
		if (st == MDS_OK) {
			(void)fprintf(stderr,
				"failover_rondb: promotion succeeded; "
				"watchdog exiting\n");
			break;
		}

		(void)fprintf(stderr,
			"failover_rondb: promotion refused (st=%d); "
			"will retry next tick\n",
			(int)st);
		/* Loop and re-poll.  The precheck guards in
		 * failover_promote (self-fencing, repl health, wire
		 * compat) are the right place for the retry decision;
		 * the watchdog's job is just to keep observing. */
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
	if (cfg->fo == NULL || cfg->cat == NULL || cfg->partner_id == 0) {
		return MDS_ERR_INVAL;
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
			     : WATCHDOG_STALE_TIMEOUT_MS_DEFAULT;
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

#else /* !HAVE_RONDB */

/* ISO C forbids an empty TU; keep this anchor for ENABLE_RONDB=OFF
 * builds.  Callers never reach these stubs at runtime because main.c
 * guards the watchdog wire-up with #ifdef HAVE_RONDB. */
#include <stddef.h>
#include "pnfs_mds.h"
#include "failover_rondb.h"

enum mds_status failover_watchdog_start(
	const struct failover_watchdog_cfg *cfg,
	struct failover_watchdog **out)
{
	(void)cfg;
	if (out != NULL) {
		*out = NULL;
	}
	return MDS_ERR_NOSUPPORT;
}

void failover_watchdog_stop(struct failover_watchdog *wd)
{
	(void)wd;
}

#endif /* HAVE_RONDB */

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * failover_rondb.h -- Partner-liveness watchdog (RonDB backend).
 *
 * Starts a background thread on the standby MDS that polls
 * mds_node_registry.last_heartbeat_ns and triggers failover_promote()
 * when the partner misses enough heartbeats.  See
 * src/cluster/failover_rondb.c for the full design note.
 */

#ifndef FAILOVER_RONDB_H
#define FAILOVER_RONDB_H

#include <stdint.h>

#include "pnfs_mds.h"

struct failover_ctx;
struct mds_catalogue;
struct failover_watchdog;

struct failover_watchdog_cfg {
	struct failover_ctx  *fo;            /**< Existing failover context. */
	struct mds_catalogue *cat;           /**< Catalogue (must be RonDB). */
	uint32_t              partner_id;    /**< MDS ID to watch. */
	uint32_t              poll_interval_ms;
	uint32_t              stale_timeout_ms;
	uint32_t              min_observe_ms;
};

/**
 * Start the partner-liveness watchdog.
 *
 * @param cfg Config (see struct).  poll_interval_ms,
 *            stale_timeout_ms, and min_observe_ms are optional
 *            (0 = compile-time defaults).
 * @param out Receives the watchdog handle.
 * @return MDS_OK on success.
 */
enum mds_status failover_watchdog_start(
	const struct failover_watchdog_cfg *cfg,
	struct failover_watchdog **out);

/**
 * Stop the watchdog.  Signals the thread, joins, frees.  NULL-safe.
 */
void failover_watchdog_stop(struct failover_watchdog *wd);

#endif /* FAILOVER_RONDB_H */

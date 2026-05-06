/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * health.h -- Cluster health monitoring API.
 *
 * Provides periodic replication health checks with operator alerting
 * and optional write refusal when the standby requires resync.
 *
 * See docs/architecture.md S23 item 38a.
 */

#ifndef HEALTH_H
#define HEALTH_H

#include <stdbool.h>
#include <stdint.h>

struct health_monitor;
struct repl_conn;

/**
 * Initialise the health monitor.
 *
 * @param conn           Replication connection (NULL = no standby,
 *                       health_repl_is_ok always returns true).
 * @param interval_ms    Poll interval in milliseconds (0 = default 5000).
 * @param refuse_writes  If true, health_writes_blocked() returns true
 *                       when resync is needed.
 * @param out            Receives allocated monitor.  Caller must call
 *                       health_monitor_destroy() to free.
 * @return 0 on success, -1 on failure.
 *
 * Ownership: caller retains ownership of @conn.  The monitor only
 * reads from the connection; it does not close or modify it.
 */
int  health_monitor_init(struct repl_conn *conn, uint32_t interval_ms,
			 bool refuse_writes, struct health_monitor **out);

/**
 * Start the health polling thread.
 *
 * @param hm  Monitor from health_monitor_init().
 * @return 0 on success, -1 on failure.
 *
 * Thread safety: safe to call once per monitor instance.
 */
int  health_monitor_start(struct health_monitor *hm);

/**
 * Stop the polling thread and release resources.
 *
 * @param hm  Monitor (NULL-safe).
 */
void health_monitor_stop(struct health_monitor *hm);

/**
 * Destroy the health monitor and free memory.
 *
 * Calls health_monitor_stop() if still running.
 *
 * @param hm  Monitor (NULL-safe).
 */
void health_monitor_destroy(struct health_monitor *hm);

/**
 * Check if replication is healthy.
 *
 * @param hm  Monitor (NULL = always returns true).
 * @return true if replication is healthy or no standby configured.
 *
 * Thread safety: safe to call from any thread (atomic read).
 */
bool health_repl_is_ok(const struct health_monitor *hm);

/**
 * Check if metadata writes should be blocked.
 *
 * Returns true only when both conditions hold:
 * 1. Replication is unhealthy (resync needed).
 * 2. refuse_writes was set to true at init time.
 *
 * @param hm  Monitor (NULL = always returns false).
 * @return true if writes should be blocked.
 *
 * Thread safety: safe to call from any thread (atomic read).
 */
bool health_writes_blocked(const struct health_monitor *hm);

/**
 * Force the replication health status.  Test-only.
 *
 * @param hm  Monitor.
 * @param ok  1 = healthy, 0 = resync needed.
 */
void health_monitor_test_set_repl_ok(struct health_monitor *hm, int ok);

#endif /* HEALTH_H */

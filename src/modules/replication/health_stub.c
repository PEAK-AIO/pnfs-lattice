/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * health_stub.c — no-op stub implementations of the replication
 * health monitor API.
 *
 * Linked into pnfs_mds_core when ENABLE_REPLICATION=OFF (community
 * build).  The real implementation is in health.c in this same
 * directory.
 *
 * Semantics: replication health monitoring is unavailable.
 *   - health_monitor_init() returns success but yields a NULL monitor.
 *   - health_repl_is_ok() returns true (matches the documented
 *     "NULL = always healthy" contract).
 *   - health_writes_blocked() returns false ("never block writes").
 *   - start/stop/destroy/test_set are safe no-ops.
 *
 * The stub semantics are deliberately the same as "no standby
 * configured" in the real implementation; that path is already
 * exercised by every single-MDS deployment, so the community build
 * gets well-tested behaviour for free.
 */
#include "health.h"

#include <stddef.h>   /* NULL */

int health_monitor_init(struct repl_conn *conn, uint32_t interval_ms,
                        bool refuse_writes, struct health_monitor **out)
{
    (void)conn;
    (void)interval_ms;
    (void)refuse_writes;

    if (out) {
        *out = NULL;
    }
    return 0;
}

int health_monitor_start(struct health_monitor *hm)
{
    (void)hm;
    return 0;
}

void health_monitor_stop(struct health_monitor *hm)
{
    (void)hm;
}

void health_monitor_destroy(struct health_monitor *hm)
{
    (void)hm;
}

bool health_repl_is_ok(const struct health_monitor *hm)
{
    (void)hm;
    return true;
}

bool health_writes_blocked(const struct health_monitor *hm)
{
    (void)hm;
    return false;
}

void health_monitor_test_set_repl_ok(struct health_monitor *hm, int ok)
{
    (void)hm;
    (void)ok;
}

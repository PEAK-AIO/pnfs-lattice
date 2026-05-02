/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_health.h — Data server health monitoring.
 *
 * Periodically probes each DS_ONLINE data server with NFS NULL RPCs.
 * After fail_threshold consecutive failures, marks the DS offline
 * (replicated via commit queue) and invokes a failure callback.
 *
 * See docs/architecture.md §15 for design.
 */

#ifndef DS_HEALTH_H
#define DS_HEALTH_H

#include <stdint.h>

struct mds_catalogue;
struct commit_queue;
struct ds_health_monitor;

/** Callback invoked when a DS transitions to DS_OFFLINE. */
typedef void (*ds_fail_cb)(uint32_t ds_id, void *ctx);

/**
 * Parse hostname from DS registry addr field ("host:/export").
 *
 * @param addr      Full DS address string.
 * @param host      Output buffer for hostname.
 * @param host_len  Size of output buffer.
 * @return 0 on success, -1 on parse error.
 */
int ds_addr_parse_host(const char *addr, char *host, size_t host_len);

/**
 * Send NFS NULL RPC to a data server.
 *
 * @param host       Hostname or IP.
 * @param port       TCP port.
 * @param timeout_ms Timeout in milliseconds.
 * @return 0 on success, -1 on failure.
 */
int ds_probe_null(const char *host, uint16_t port, uint32_t timeout_ms);

/**
 * Initialise the DS health monitor.
 *
 * DS registry reads/writes go through the catalogue vtable
 * (mds_cat_ds_list / mds_cat_ds_put), so this works for both
 * the RonDB backend.
 *
 * @param cat            Catalogue handle (must not be NULL).
 * @param cq             Commit queue for replicated state transitions (may be NULL).
 * @param interval_ms    Poll interval (0 = default 5000).
 * @param fail_threshold Consecutive failures before marking offline (0 = default 3).
 * @param cb             Failure callback (may be NULL).
 * @param cb_ctx         Opaque context for callback.
 * @param out            Receives allocated monitor.
 * @return 0 on success, -1 on failure.
 */
int ds_health_init(const struct mds_catalogue *cat, struct commit_queue *cq,
                   uint32_t interval_ms, uint32_t fail_threshold,
                   ds_fail_cb cb, void *cb_ctx,
                   struct ds_health_monitor **out);

/** Start the health polling thread. */
int ds_health_start(struct ds_health_monitor *hm);

/** Stop the polling thread. */
void ds_health_stop(struct ds_health_monitor *hm);

/** Destroy the monitor and free resources. */
void ds_health_destroy(struct ds_health_monitor *hm);

/**
 * Report a client-observed DS error (from LAYOUTERROR).
 * Increments the DS's failure counter by 1.
 */
void ds_health_report_error(struct ds_health_monitor *hm, uint32_t ds_id);

/** Test-only: immediately trigger failure path for a DS. */
void ds_health_test_force_fail(struct ds_health_monitor *hm, uint32_t ds_id);

struct mds_shard;

/**
 * Bind to a default shard (sets db and cq from shard).
 *
 * @param hm     DS health monitor.
 * @param shard  Default shard (NULL tolerated).
 */
void ds_health_set_shard(struct ds_health_monitor *hm,
                         const struct mds_shard *shard);

#endif /* DS_HEALTH_H */

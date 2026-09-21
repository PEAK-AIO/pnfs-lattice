/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * repl_monitor.h -- Userspace eBPF replication-latency monitor.
 *
 * Lifecycle: bpf_monitor_init() -> bpf_monitor_start() ->
 * bpf_monitor_stop() -> bpf_monitor_destroy().  The monitor is
 * built only when ENABLE_EBPF is set and libbpf is available; the
 * implementation in src/bpf/repl_monitor.c is a scaffold that
 * reports failure from init/start until the loader is written.
 */

#ifndef REPL_MONITOR_H
#define REPL_MONITOR_H

/** Opaque monitor handle; owned by the caller after a successful init. */
struct bpf_monitor;

/**
 * @brief Allocate a monitor and load the eBPF programs.
 *
 * @param[out] out  Receives the monitor handle on success.
 * @return 0 on success, -1 on failure (nothing allocated).
 */
int bpf_monitor_init(struct bpf_monitor **out);

/**
 * @brief Start polling the ring buffer on a background thread.
 *
 * @param mon  Monitor from bpf_monitor_init().
 * @return 0 on success, -1 on failure.
 */
int bpf_monitor_start(struct bpf_monitor *mon);

/**
 * @brief Request the polling thread to stop.  Safe on NULL.
 */
void bpf_monitor_stop(struct bpf_monitor *mon);

/**
 * @brief Stop (if running), detach programs and free the monitor.
 *        Safe on NULL.
 */
void bpf_monitor_destroy(struct bpf_monitor *mon);

#endif /* REPL_MONITOR_H */

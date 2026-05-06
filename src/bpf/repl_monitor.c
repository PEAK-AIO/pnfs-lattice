/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * repl_monitor.c -- Userspace eBPF loader and metrics exporter.
 *
 * Loads repl_monitor.bpf.o, attaches programs, reads ring buffer,
 * and exports replication latency metrics.
 */

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#include "pnfs_mds.h"

struct bpf_monitor {
    bool running;
    /* TODO: struct bpf_object*, ring_buffer*, fds */
};

int bpf_monitor_init(struct bpf_monitor **out)
{
    /* TODO: implement
     * 1. bpf_object__open("repl_monitor.bpf.o")
     * 2. bpf_object__load()
     * 3. Attach fentry programs
     * 4. Open ring buffer
     */
    (void)out;
    return -1;
}

int bpf_monitor_start(struct bpf_monitor *mon)
{
    /* TODO: implement
     * Spawn thread that polls ring buffer for repl_latency_event.
     * Compute running avg/p99 latency, export via /metrics endpoint
     * or shared memory for health.c to read.
     */
    (void)mon;
    return -1;
}

void bpf_monitor_stop(struct bpf_monitor *mon)
{
    if (mon != NULL) {
        mon->running = false;
    }
}

void bpf_monitor_destroy(struct bpf_monitor *mon)
{
    if (mon == NULL) {
        return;
    }
    bpf_monitor_stop(mon);
    /* TODO: detach programs, close bpf_object */
    free(mon);
}

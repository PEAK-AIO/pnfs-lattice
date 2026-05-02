/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_gc_stub.c — no-op stubs for the DS garbage collector worker.
 *
 * Linked into pnfs_mds_core when ENABLE_DS_GC=OFF (community build).
 * The real implementation is in ds_gc.c.
 *
 * Semantics: no background drainer runs.  GC entries persist in the
 * catalogue queue and accumulate; an operator running an enterprise
 * binary against the same catalogue will drain them on the next
 * tick.  Community sites that need DS-side reclaim should run a
 * periodic admin command instead.
 *
 * Both ds_gc_start variants honour the documented "poll_ms == 0
 * disables drainer" contract by returning 0 with *out = NULL — the
 * stub always behaves as if the operator passed poll_ms=0.
 */
#include "ds_gc.h"

#include <stddef.h>

int ds_gc_start(struct mds_catalogue *cat,
                struct mds_proxy_ctx *proxy,
                uint32_t poll_ms,
                struct ds_gc **out)
{
    (void)cat;
    (void)proxy;
    (void)poll_ms;

    if (out) {
        *out = NULL;
    }
    return 0;
}

int ds_gc_start_ex(struct mds_catalogue *cat,
                   struct mds_proxy_ctx *proxy,
                   uint32_t poll_ms,
                   uint32_t workers,
                   uint32_t batch_size,
                   struct ds_gc **out)
{
    (void)cat;
    (void)proxy;
    (void)poll_ms;
    (void)workers;
    (void)batch_size;

    if (out) {
        *out = NULL;
    }
    return 0;
}

void ds_gc_stop(struct ds_gc *gc)
{
    (void)gc;
}

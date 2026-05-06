/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * heartbeat.c -- MDS<->DS and MDS<->MDS heartbeat.
 */

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#include "pnfs_mds.h"

struct heartbeat_ctx {
    uint32_t self_id;
    uint64_t epoch;
    uint32_t interval_ms;
    uint32_t timeout_ms;
    bool     running;
    /* TODO: peer list, gRPC stubs, timer thread */
};

int heartbeat_init(uint32_t self_id, uint64_t epoch,
                   uint32_t interval_ms, uint32_t timeout_ms,
                   struct heartbeat_ctx **out)
{
    if (out == NULL) {
        return -1;
    }
    struct heartbeat_ctx *ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        return -1;
    }
    ctx->self_id = self_id;
    ctx->epoch = epoch;
    ctx->interval_ms = interval_ms;
    ctx->timeout_ms = timeout_ms;
    *out = ctx;
    return 0;
}

int heartbeat_start(struct heartbeat_ctx *ctx)
{
    /* TODO: spawn heartbeat timer thread that sends HeartbeatMsg
     * via gRPC to all known peers every ctx->interval_ms.
     * If any peer fails to respond within ctx->timeout_ms,
     * invoke the failure callback. */
    (void)ctx;
    return -1;
}

int heartbeat_stop(struct heartbeat_ctx *ctx)
{
    /* TODO: signal timer thread to exit, join */
    if (ctx == NULL) {
        return 0;
    }
    ctx->running = false;
    return 0;
}

void heartbeat_destroy(struct heartbeat_ctx *ctx)
{
    if (ctx == NULL) {
        return;
    }
    heartbeat_stop(ctx);
    free(ctx);
}

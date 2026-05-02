/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * rebalance_stub.c — no-op stub implementations of the rebalance API.
 *
 * Linked into pnfs_mds_core when ENABLE_REBALANCE=OFF (community
 * build). The full implementation is in rebalance.c in this same
 * directory.
 *
 * Semantics: rebalance is unavailable.  rebalance_init() returns
 * success but yields a NULL worker handle so callers that null-check
 * before use (cluster_transport handlers) short-circuit cleanly.
 * rebalance_start() returns MDS_ERR_INVAL — "rebalance is not
 * configured on this host" — which is what callers already handle
 * when source==target or DSes are offline.
 */
#include "rebalance.h"

#include <string.h>

int rebalance_init(const struct mds_catalogue *cat, struct commit_queue *cq,
                   struct mds_proxy_ctx *proxy_ctx,
                   struct open_state_table *ot,
                   struct rebalance_worker **out)
{
    (void)cat;
    (void)cq;
    (void)proxy_ctx;
    (void)ot;

    if (out) {
        *out = NULL;
    }
    return 0;
}

enum mds_status rebalance_start(struct rebalance_worker *rw,
                                uint32_t source_ds, uint32_t target_ds)
{
    (void)rw;
    (void)source_ds;
    (void)target_ds;
    return MDS_ERR_INVAL;
}

void rebalance_stop(struct rebalance_worker *rw)
{
    (void)rw;
}

void rebalance_destroy(struct rebalance_worker *rw)
{
    (void)rw;
}

void rebalance_status_ext(const struct rebalance_worker *rw,
                          struct rebalance_status_info *info)
{
    (void)rw;
    if (info) {
        memset(info, 0, sizeof(*info));
        info->state = REBALANCE_IDLE;
    }
}

void rebalance_set_retry_delay(struct rebalance_worker *rw, uint32_t secs)
{
    (void)rw;
    (void)secs;
}

void rebalance_set_shard(struct rebalance_worker *rw,
                         const struct mds_shard *shard)
{
    (void)rw;
    (void)shard;
}

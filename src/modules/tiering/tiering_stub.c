/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * tiering_stub.c -- no-op stub implementations of the tiering API.
 *
 * Linked into pnfs_mds_core when ENABLE_TIERING=OFF (community build).
 * The full implementation is in tiering.c in this same directory.
 *
 * Semantics: tiering is unavailable.  tiering_init() returns success
 * but yields a NULL worker handle so callers that null-check before
 * use short-circuit cleanly.  tiering_start() returns MDS_ERR_INVAL
 * -- "tiering is not configured on this host".
 */
#include "tiering.h"

#include <string.h>

int tiering_init(const struct mds_catalogue *cat, struct commit_queue *cq,
                 struct mds_proxy_ctx *proxy_ctx,
                 struct open_state_table *ot,
                 struct io_tracker *iot,
                 struct tiering_worker **out)
{
    (void)cat;
    (void)cq;
    (void)proxy_ctx;
    (void)ot;
    (void)iot;

    if (out) {
        *out = NULL;
    }
    return 0;
}

enum mds_status tiering_start(struct tiering_worker *tw,
                              const struct tiering_config *cfg)
{
    (void)tw;
    (void)cfg;
    return MDS_ERR_INVAL;
}

void tiering_stop(struct tiering_worker *tw)
{
    (void)tw;
}

void tiering_destroy(struct tiering_worker *tw)
{
    (void)tw;
}

void tiering_status(const struct tiering_worker *tw,
                    struct tiering_status_info *info)
{
    (void)tw;
    if (info) {
        memset(info, 0, sizeof(*info));
        info->state = TIERING_IDLE;
    }
}

void tiering_set_eval_interval(struct tiering_worker *tw, uint32_t secs)
{
    (void)tw;
    (void)secs;
}

void tiering_set_max_evals(struct tiering_worker *tw, uint32_t n)
{
    (void)tw;
    (void)n;
}

void tiering_set_shard(struct tiering_worker *tw,
                       const struct mds_shard *shard)
{
    (void)tw;
    (void)shard;
}

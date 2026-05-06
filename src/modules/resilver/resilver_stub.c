/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * resilver_stub.c -- no-op stub implementations of the resilver API.
 *
 * Linked into pnfs_mds_core when ENABLE_RESILVER=OFF (community
 * build). The full implementation is in resilver.c in this same
 * directory.
 *
 * Semantics: resilver is unavailable.  resilver_init() returns
 * success but yields a NULL worker handle so that:
 *   - callers that null-check before use (cluster_transport handlers)
 *     short-circuit cleanly,
 *   - all subsequent operations on the handle are no-ops,
 *   - status queries return an idle/zeroed state.
 *
 * No background thread is started; no DS scan is performed; no
 * memory is allocated.  The daemon binary stays smaller and the
 * resilver-related cluster-transport RPC handlers can still link.
 */
#include "resilver.h"

#include <string.h>

int resilver_init(const struct mds_catalogue *cat, struct commit_queue *cq,
                  struct mds_proxy_ctx *proxy_ctx,
                  struct open_state_table *ot,
                  struct resilver_worker **out)
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

int resilver_start(struct resilver_worker *rw, uint32_t target_ds)
{
    (void)rw;
    (void)target_ds;
    /*
     * Mirror the real implementation's contract for "not initialised":
     * return -1 so callers know nothing started.
     */
    return -1;
}

void resilver_stop(struct resilver_worker *rw)
{
    (void)rw;
}

void resilver_destroy(struct resilver_worker *rw)
{
    (void)rw;
}

void resilver_status(const struct resilver_worker *rw,
                     uint32_t *files_total, uint32_t *files_done,
                     uint64_t *bytes_copied, uint32_t *state)
{
    (void)rw;
    if (files_total) {
        *files_total = 0;
    }
    if (files_done) {
        *files_done = 0;
    }
    if (bytes_copied) {
        *bytes_copied = 0;
    }
    if (state) {
        *state = RESILVER_IDLE;
    }
}

void resilver_status_ext(const struct resilver_worker *rw,
                         struct resilver_status_info *info)
{
    (void)rw;
    if (info) {
        memset(info, 0, sizeof(*info));
        info->state = RESILVER_IDLE;
    }
}

void resilver_set_retry_delay(struct resilver_worker *rw, uint32_t secs)
{
    (void)rw;
    (void)secs;
}

void resilver_set_shard(struct resilver_worker *rw,
                        const struct mds_shard *shard)
{
    (void)rw;
    (void)shard;
}

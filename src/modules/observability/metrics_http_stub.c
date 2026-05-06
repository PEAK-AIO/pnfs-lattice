/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * metrics_http_stub.c -- no-op stub for the Prometheus HTTP server.
 *
 * Linked into pnfs_mds_core when ENABLE_OBSERVABILITY=OFF (community
 * build). The full implementation is in metrics_http.c in this same
 * directory.
 *
 * Semantics: the metrics endpoint is not started.  metrics_http_start
 * returns 0 (success) but yields a NULL ctx so callers that null-check
 * before stopping (main.c does) short-circuit cleanly.  No port is
 * bound, no thread is spawned, no metrics are exposed.
 *
 * Operators on community builds get observability via logs and the
 * mds-admin CLI introspection commands; the Prometheus integration is
 * an enterprise-only convenience.
 */
#include "metrics_http.h"

#include <stddef.h>   /* NULL */

int metrics_http_start(uint16_t port, struct mds_catalogue *cat,
                       struct metrics_http_ctx **out)
{
    (void)port;
    (void)cat;

    if (out) {
        *out = NULL;
    }
    return 0;
}

void metrics_http_stop(struct metrics_http_ctx *ctx)
{
    (void)ctx;
}

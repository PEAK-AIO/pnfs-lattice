/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 */

#ifndef METRICS_HTTP_H
#define METRICS_HTTP_H

#include <stdint.h>

struct mds_catalogue;
struct metrics_http_ctx;

/**
 * Start a minimal HTTP server for Prometheus /metrics scraping.
 *
 * @param port  TCP port (e.g. 9090).
 * @param cat   Catalogue handle for catalog_stats.
 * @param out   Receives the context handle.
 * @return 0 on success.
 */
int metrics_http_start(uint16_t port, struct mds_catalogue *cat,
                       struct metrics_http_ctx **out);

/** Stop the metrics HTTP server and free resources. */
void metrics_http_stop(struct metrics_http_ctx *ctx);

#endif /* METRICS_HTTP_H */

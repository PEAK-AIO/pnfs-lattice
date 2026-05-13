/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * metrics_http_lite.c -- Minimal community HTTP /metrics endpoint.
 *
 * Replaces metrics_http_stub.c so community builds can actually
 * scrape Prometheus metrics without the enterprise observability
 * module.  Design goals:
 *
 *   - Single listener thread, sequential request handling.
 *     Prometheus scrapes once every 5-15 seconds; concurrency on
 *     the scrape path adds no value and a pile of complexity.
 *
 *   - HTTP/1.0 with explicit Connection: close so we never have
 *     to manage keep-alive state.
 *
 *   - Accept any path -- /metrics, /, /healthz, etc. all return
 *     the Prometheus body.  Saves clients (and operators
 *     curl'ing for sanity checks) from path mistakes.
 *
 *   - Hard 256 KiB response cap.  With ~140 histograms x ~14
 *     bucket lines each, the real expected size is ~30 KiB.  The
 *     cap exists to bound stack/heap pressure if observability is
 *     accidentally turned up.
 *
 *   - Shutdown is initiated by closing the listen socket from
 *     metrics_http_stop(); the accept() in the worker returns
 *     EBADF and the loop exits.
 *
 * Operators who want the endpoint OFF set `metrics_http_port = 0`
 * in mds.conf -- main.c skips metrics_http_start() entirely in
 * that case.
 */

#include "metrics_http.h"
#include "mds_metrics.h"
#include "mds_catalogue.h"
#include "catalog_stats.h"
#include "health.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define METRICS_HTTP_BODY_CAP (256 * 1024)

struct metrics_http_ctx {
    int                   listen_fd;
    pthread_t             thread;
    _Atomic bool          shutdown;
    struct mds_catalogue *cat;
    uint16_t              port;
};

/* Build a metrics snapshot and feed the v2 renderer into `out`.
 * Returns the number of bytes written (excluding NUL) or -1 on
 * truncation.  Buffer cap must be > 0. */
static int render_metrics_body(struct mds_catalogue *cat,
                               char *out, size_t cap)
{
    struct mds_metrics_snapshot snap = mds_metrics_snapshot();

    if (cat != NULL) {
        struct catalog_stats *cs = mds_catalogue_stats(cat);
        if (cs != NULL) {
            mds_metrics_snapshot_fill_catalog(&snap, cs);
        }
    }

    return mds_metrics_prometheus_v2(&snap, &g_branch_metrics, out, cap);
}

/* Write all bytes of `buf` (n bytes) to `fd`, ignoring partial
 * writes.  Returns 0 on success, -1 on error (including
 * connection reset). */
static int write_all(int fd, const char *buf, size_t n)
{
    while (n > 0) {
        ssize_t w = write(fd, buf, n);
        if (w < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        buf += w;
        n   -= (size_t)w;
    }
    return 0;
}

/* Consume the request line + headers up to the first blank line.
 * We do not parse anything (any path returns metrics); we just
 * need to drain enough that the client's send buffer can flush
 * before we reply.  Reads cap out at 8 KiB; oversize requests
 * are dropped. */
static void drain_request(int fd)
{
    char    buf[2048];
    ssize_t n;
    int     attempts = 0;

    while (attempts++ < 4) {
        n = recv(fd, buf, sizeof(buf), MSG_DONTWAIT);
        if (n <= 0) {
            return;
        }
        if (memmem(buf, (size_t)n, "\r\n\r\n", 4) != NULL) {
            return;
        }
    }
}

static void handle_connection(int conn_fd, struct mds_catalogue *cat)
{
    char  *body;
    int    body_len;
    char   header[256];
    int    header_len;

    body = malloc(METRICS_HTTP_BODY_CAP);
    if (body == NULL) {
        const char *msg =
            "HTTP/1.0 500 Internal Server Error\r\n"
            "Content-Length: 0\r\n"
            "Connection: close\r\n\r\n";
        (void)write_all(conn_fd, msg, strlen(msg));
        return;
    }

    drain_request(conn_fd);

    body_len = render_metrics_body(cat, body, METRICS_HTTP_BODY_CAP);
    if (body_len < 0) {
        /* Truncated: still serve what we have minus the last
         * line.  We have no length here; report 503 instead. */
        const char *msg =
            "HTTP/1.0 503 Service Unavailable\r\n"
            "Content-Length: 0\r\n"
            "Connection: close\r\n\r\n";
        (void)write_all(conn_fd, msg, strlen(msg));
        free(body);
        return;
    }

    header_len = snprintf(header, sizeof(header),
        "HTTP/1.0 200 OK\r\n"
        "Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"
        "Content-Length: %d\r\n"
        "Connection: close\r\n"
        "\r\n", body_len);
    if (header_len < 0 || header_len >= (int)sizeof(header)) {
        free(body);
        return;
    }
    (void)write_all(conn_fd, header, (size_t)header_len);
    (void)write_all(conn_fd, body, (size_t)body_len);
    free(body);
}

static void *accept_loop(void *arg)
{
    struct metrics_http_ctx *ctx = arg;

    for (;;) {
        struct sockaddr_in cli;
        socklen_t          clen = sizeof(cli);
        int                conn_fd;

        if (atomic_load_explicit(&ctx->shutdown,
                                 memory_order_acquire)) {
            return NULL;
        }

        conn_fd = accept(ctx->listen_fd, (struct sockaddr *)&cli, &clen);
        if (conn_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            /* listen_fd closed (shutdown) or fatal error. */
            return NULL;
        }

        handle_connection(conn_fd, ctx->cat);
        close(conn_fd);
    }
}

int metrics_http_start(uint16_t port, struct mds_catalogue *cat,
                       struct metrics_http_ctx **out)
{
    struct metrics_http_ctx *ctx;
    struct sockaddr_in       addr;
    int                      one = 1;

    if (out == NULL) {
        return -1;
    }
    *out = NULL;

    if (port == 0) {
        return 0; /* operator opt-out */
    }

    ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        return -1;
    }
    ctx->cat  = cat;
    ctx->port = port;
    atomic_init(&ctx->shutdown, false);

    ctx->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ctx->listen_fd < 0) {
        (void)fprintf(stderr,
            "WARN: metrics_http: socket() failed: %s\n",
            strerror(errno));
        free(ctx);
        return -1;
    }

    (void)setsockopt(ctx->listen_fd, SOL_SOCKET, SO_REUSEADDR,
                     &one, sizeof(one));

    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(port);

    if (bind(ctx->listen_fd, (struct sockaddr *)&addr,
             sizeof(addr)) < 0) {
        (void)fprintf(stderr,
            "WARN: metrics_http: bind(:%u) failed: %s\n",
            (unsigned)port, strerror(errno));
        close(ctx->listen_fd);
        free(ctx);
        return -1;
    }

    if (listen(ctx->listen_fd, 8) < 0) {
        (void)fprintf(stderr,
            "WARN: metrics_http: listen() failed: %s\n",
            strerror(errno));
        close(ctx->listen_fd);
        free(ctx);
        return -1;
    }

    if (pthread_create(&ctx->thread, NULL, accept_loop, ctx) != 0) {
        (void)fprintf(stderr,
            "WARN: metrics_http: pthread_create() failed\n");
        close(ctx->listen_fd);
        free(ctx);
        return -1;
    }

    (void)fprintf(stderr,
        "INFO: metrics_http: listening on 0.0.0.0:%u "
        "(GET /metrics)\n", (unsigned)port);

    *out = ctx;
    return 0;
}

void metrics_http_stop(struct metrics_http_ctx *ctx)
{
    if (ctx == NULL) {
        return;
    }

    atomic_store_explicit(&ctx->shutdown, true, memory_order_release);

    /* Closing the listen fd kicks accept() out of its block. */
    if (ctx->listen_fd >= 0) {
        shutdown(ctx->listen_fd, SHUT_RDWR);
        close(ctx->listen_fd);
        ctx->listen_fd = -1;
    }

    (void)pthread_join(ctx->thread, NULL);
    free(ctx);
}

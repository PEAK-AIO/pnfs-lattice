/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_mds_metrics.c -- Unit tests for metrics registry and the
 * community /metrics HTTP listener (metrics_http_lite.c).
 */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "mds_metrics.h"
#include "mds_op_metrics.h"
#include "metrics_http.h"

static int passed = 0;
static int failed = 0;

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: %s\n", \
                __FILE__, __LINE__, #cond); \
        failed++; return; \
    } \
} while (0)

#define ASSERT_EQ(a, b) ASSERT_TRUE((a) == (b))

/* Client-side bound on every blocking call in the listener tests: a
 * regression must fail the test, not hang it.  Generous next to the
 * listener's own 1 s request timeout. */
#define HTTP_TEST_CLIENT_TIMEOUT_SEC 5
/* Largest response the tests accept; the listener caps at 256 KiB. */
#define HTTP_TEST_RESPONSE_CAP ((size_t)512 * 1024)
/* Delay between connect() and sending the GET: long enough that the
 * listener has certainly accept()ed and looked for the request first,
 * which is the window the old single non-blocking recv() missed. */
#define HTTP_TEST_LATE_GET_DELAY_MS 50
/* Client receive buffer, far below the ~50 KiB+ body.  On loopback the
 * whole body would otherwise land in the client's buffer before the
 * late GET is even sent, hiding the bug this file guards against: a
 * small window keeps the listener's body write in progress when the
 * GET arrives, so a listener that has not consumed the request by the
 * time it close()s sends RST and the client sees a truncated body --
 * the LAN symptom.  The kernel doubles the value and floors it. */
#define HTTP_TEST_CLIENT_RCVBUF 4096

/* Sleep for a bounded number of milliseconds (EINTR-tolerant). */
static void sleep_ms(long ms)
{
    struct timespec ts = { .tv_sec = ms / 1000,
                           .tv_nsec = (ms % 1000) * 1000000L };

    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
        continue;
    }
}

/* Ask the kernel for a free loopback port: bind :0, read it back, and
 * release it for metrics_http_start() to bind.  Returns 0 on success. */
static int pick_free_port(uint16_t *port_out)
{
    struct sockaddr_in addr;
    socklen_t alen = sizeof(addr);
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0) {
        return -1;
    }
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
        getsockname(fd, (struct sockaddr *)&addr, &alen) < 0) {
        close(fd);
        return -1;
    }
    *port_out = ntohs(addr.sin_port);
    close(fd);
    return 0;
}

/* Start the listener on a fresh loopback port.  Retries because the
 * port picked above can, in principle, be taken by another process
 * between the probe and the real bind. */
static struct metrics_http_ctx *start_listener(uint16_t *port_out)
{
    int attempt;

    for (attempt = 0; attempt < 3; attempt++) {
        struct metrics_http_ctx *ctx = NULL;

        if (pick_free_port(port_out) != 0) {
            return NULL;
        }
        if (metrics_http_start(*port_out, NULL, &ctx) == 0 && ctx != NULL) {
            return ctx;
        }
    }
    return NULL;
}

/* Connect to the listener with client-side timeouts armed and the
 * small receive window described at HTTP_TEST_CLIENT_RCVBUF (it must
 * be set before connect() to take effect on the negotiated window). */
static int connect_listener(uint16_t port)
{
    struct sockaddr_in addr;
    struct timeval tv = { .tv_sec = HTTP_TEST_CLIENT_TIMEOUT_SEC,
                          .tv_usec = 0 };
    int rcvbuf = HTTP_TEST_CLIENT_RCVBUF;
    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0) {
        return -1;
    }
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) < 0 ||
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0 ||
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0 ||
        connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int send_get(int fd)
{
    static const char req[] =
        "GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n";
    size_t off = 0;

    while (off < sizeof(req) - 1) {
        ssize_t w = send(fd, req + off, sizeof(req) - 1 - off, MSG_NOSIGNAL);

        if (w <= 0) {
            return -1;
        }
        off += (size_t)w;
    }
    return 0;
}

/* Read until the peer's EOF.  Returns the byte count, or -1 on a
 * timeout, a reset or an oversize response. */
static ssize_t read_to_eof(int fd, char *buf, size_t cap)
{
    size_t used = 0;

    for (;;) {
        ssize_t n;

        if (used == cap) {
            return -1;
        }
        n = recv(fd, buf + used, cap - used, 0);
        if (n == 0) {
            return (ssize_t)used;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        used += (size_t)n;
    }
}

/* Split an HTTP/1.0 response: status code, Content-Length header value
 * and the number of body bytes actually present after the blank line.
 * Returns 0 when all three were found. */
static int parse_response(const char *buf, size_t len, int *status,
                          long *content_length, size_t *body_len)
{
    const char *hdr_end;
    const char *cl;

    /* Validate before the searches: the buffer comes from the caller's
     * malloc, and gcc's -Wnonnull (under -fsanitize=undefined) wants the
     * NULL case decided before memmem() sees the pointer. */
    if (buf == NULL || len < 12 || memcmp(buf, "HTTP/1.0 ", 9) != 0) {
        return -1;
    }
    *status = atoi(buf + 9);
    hdr_end = memmem(buf, len, "\r\n\r\n", 4);
    if (hdr_end == NULL) {
        return -1;
    }
    cl = memmem(buf, (size_t)(hdr_end - buf), "Content-Length: ", 16);
    if (cl == NULL) {
        return -1;
    }
    *content_length = atol(cl + 16);
    *body_len = len - (size_t)(hdr_end + 4 - buf);
    return 0;
}

/* Scrape whose GET arrives only after the listener has accepted the
 * connection: the reply must still be a complete 200 whose body is
 * exactly Content-Length bytes long.  Before the request was read
 * before replying, the unread GET made close() send RST and the peer
 * saw a truncated body. */
static void test_metrics_http_late_get_full_body(void)
{
    struct metrics_http_ctx *ctx;
    uint16_t port = 0;
    char *resp;
    ssize_t got;
    int fd;
    int status = 0;
    long content_length = -1;
    size_t body_len = 0;

    fprintf(stdout, "  test_metrics_http_late_get:        ");

    resp = malloc(HTTP_TEST_RESPONSE_CAP);
    ASSERT_TRUE(resp != NULL);
    ctx = start_listener(&port);
    ASSERT_TRUE(ctx != NULL);

    fd = connect_listener(port);
    ASSERT_TRUE(fd >= 0);
    sleep_ms(HTTP_TEST_LATE_GET_DELAY_MS);
    ASSERT_EQ(send_get(fd), 0);
    got = read_to_eof(fd, resp, HTTP_TEST_RESPONSE_CAP);
    close(fd);
    metrics_http_stop(ctx);

    ASSERT_TRUE(got > 0);
    ASSERT_EQ(parse_response(resp, (size_t)got, &status,
                             &content_length, &body_len), 0);
    ASSERT_EQ(status, 200);
    ASSERT_TRUE(content_length > 0);
    ASSERT_EQ((long)body_len, content_length);
    free(resp);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* A client that connects and never sends anything must be dropped
 * after the listener's request timeout, and a well-behaved client
 * queued behind it must still be served in full. */
static void test_metrics_http_silent_client_bounded(void)
{
    struct metrics_http_ctx *ctx;
    uint16_t port = 0;
    char *resp;
    char probe[16];
    ssize_t got;
    int silent_fd;
    int fd;
    int status = 0;
    long content_length = -1;
    size_t body_len = 0;

    fprintf(stdout, "  test_metrics_http_silent_client:   ");

    resp = malloc(HTTP_TEST_RESPONSE_CAP);
    ASSERT_TRUE(resp != NULL);
    ctx = start_listener(&port);
    ASSERT_TRUE(ctx != NULL);

    /* The silent client is accepted first and holds the sequential
     * listener until its request timeout expires. */
    silent_fd = connect_listener(port);
    ASSERT_TRUE(silent_fd >= 0);
    sleep_ms(HTTP_TEST_LATE_GET_DELAY_MS);

    fd = connect_listener(port);
    ASSERT_TRUE(fd >= 0);
    ASSERT_EQ(send_get(fd), 0);
    got = read_to_eof(fd, resp, HTTP_TEST_RESPONSE_CAP);
    close(fd);

    /* By now the listener has moved on, so the silent connection has
     * been closed on it: EOF, not a hang and not a body. */
    ASSERT_EQ(recv(silent_fd, probe, sizeof(probe), 0), 0);
    close(silent_fd);
    metrics_http_stop(ctx);

    ASSERT_TRUE(got > 0);
    ASSERT_EQ(parse_response(resp, (size_t)got, &status,
                             &content_length, &body_len), 0);
    ASSERT_EQ(status, 200);
    ASSERT_EQ((long)body_len, content_length);
    free(resp);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_metrics_reset(void)
{
    fprintf(stdout, "  test_metrics_reset:                ");

    atomic_fetch_add(&g_metrics.cat_commits_ok, 10);
    mds_metrics_reset();

    struct mds_metrics_snapshot s = mds_metrics_snapshot();
    ASSERT_EQ(s.cat_commits_ok, 0);
    ASSERT_EQ(s.repl_deltas_sent, 0);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_metrics_increment(void)
{
    fprintf(stdout, "  test_metrics_increment:            ");

    mds_metrics_reset();
    atomic_fetch_add(&g_metrics.repl_deltas_sent, 5);
    atomic_fetch_add(&g_metrics.repl_bytes_sent, 1024);
    atomic_fetch_add(&g_metrics.cat_commits_ok, 3);
    atomic_fetch_add(&g_metrics.cat_commits_fail, 1);

    struct mds_metrics_snapshot s = mds_metrics_snapshot();
    ASSERT_EQ(s.repl_deltas_sent, 5);
    ASSERT_EQ(s.repl_bytes_sent, 1024);
    ASSERT_EQ(s.cat_commits_ok, 3);
    ASSERT_EQ(s.cat_commits_fail, 1);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_metrics_prometheus(void)
{
    fprintf(stdout, "  test_metrics_prometheus:           ");

    mds_metrics_reset();
    atomic_fetch_add(&g_metrics.cat_commits_ok, 42);

    struct mds_metrics_snapshot s = mds_metrics_snapshot();
    s.repl_health_ok = 1;

    char buf[4096];
    int n = mds_metrics_prometheus(&s, buf, sizeof(buf));
    ASSERT_TRUE(n > 0);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_cat_commits_ok 42") != NULL);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_repl_health_ok 1") != NULL);
    ASSERT_TRUE(strstr(buf, "# TYPE") != NULL);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_metrics_prometheus_truncation(void)
{
    fprintf(stdout, "  test_metrics_prometheus_truncate:  ");

    struct mds_metrics_snapshot s;
    memset(&s, 0, sizeof(s));

    char buf[10];
    int n = mds_metrics_prometheus(&s, buf, sizeof(buf));
    ASSERT_EQ(n, -1);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* Wave 6: the branch (v2) render must expose the decision
 * instrumentation counters the lab reads via mds-metrics-diff. */
static void test_metrics_prometheus_v2_wave6(void)
{
    fprintf(stdout, "  test_metrics_prometheus_v2_wave6:  ");

    /* The v2 render includes the always-on per-op and per-cat-op
     * histogram families (~50 KB with zero observations), so the
     * buffer must be sized like the real scrape consumer's. */
    struct mds_metrics_snapshot s;
    static char buf[131072];
    int n;

    memset(&s, 0, sizeof(s));
    atomic_fetch_add(&g_branch_metrics.cat_transient_retries, 3);
    atomic_fetch_add(&g_branch_metrics.cat_transient_backoff_us, 1500);
    atomic_fetch_add(&g_branch_metrics.cat_transient_retry_exhausted, 1);
    atomic_fetch_add(&g_branch_metrics.ds_fh_cache_hits, 7);
    atomic_fetch_add(&g_branch_metrics.ds_fh_cache_misses, 2);

    n = mds_metrics_prometheus_v2(&s, &g_branch_metrics,
                                  buf, sizeof(buf));
    ASSERT_TRUE(n > 0);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_cat_transient_retries 3") != NULL);
    ASSERT_TRUE(strstr(buf,
        "pnfs_mds_cat_transient_backoff_us 1500") != NULL);
    ASSERT_TRUE(strstr(buf,
        "pnfs_mds_cat_transient_retry_exhausted 1") != NULL);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_ds_fh_cache_hits 7") != NULL);
    ASSERT_TRUE(strstr(buf, "pnfs_mds_ds_fh_cache_misses 2") != NULL);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* Every op/cat-op enum entry must have a name-table row: designated
 * initializers silently leave gaps as NULL, which would put "(null)"
 * into /metrics label values.  Also pins the Wave 6 addition. */
static void test_op_metrics_name_tables_complete(void)
{
    fprintf(stdout, "  test_op_metrics_name_tables:       ");

    for (int i = 0; i < (int)MDS_OPC__COUNT; i++) {
        ASSERT_TRUE(mds_op_class_name((enum mds_op_class)i) != NULL);
    }
    for (int i = 0; i < (int)MDS_CATOP__COUNT; i++) {
        ASSERT_TRUE(mds_cat_op_name((enum mds_cat_op)i) != NULL);
    }
    ASSERT_TRUE(strcmp(mds_cat_op_name(MDS_CATOP_UNLINK_RECALL),
                       "unlink_recall") == 0);

    fprintf(stdout, "PASS\n");
    passed++;
}

int main(void)
{
    fprintf(stdout, "test_mds_metrics:\n");

    test_metrics_reset();
    test_metrics_increment();
    test_metrics_prometheus();
    test_metrics_prometheus_truncation();
    test_metrics_prometheus_v2_wave6();
    test_op_metrics_name_tables_complete();
    test_metrics_http_late_get_full_body();
    test_metrics_http_silent_client_bounded();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}

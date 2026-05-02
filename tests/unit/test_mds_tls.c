/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_mds_tls.c — Unit tests for TLS helper.
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

#include "mds_tls.h"

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

static void test_ctx_create_null(void)
{
    fprintf(stdout, "  test_ctx_create_null:              ");
    ASSERT_EQ(mds_tls_ctx_create(NULL, NULL, NULL,
                                  false, false, NULL), -1);
    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_ctx_create_no_certs(void)
{
    fprintf(stdout, "  test_ctx_create_no_certs:          ");
    struct mds_tls_ctx *ctx = NULL;
    /* Create with no cert files — should succeed (for testing). */
    int rc = mds_tls_ctx_create("", "", "", false, false, &ctx);
    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(ctx != NULL);
    mds_tls_ctx_destroy(ctx);
    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_ctx_create_bad_ca(void)
{
    fprintf(stdout, "  test_ctx_create_bad_ca:            ");
    struct mds_tls_ctx *ctx = NULL;
    int rc = mds_tls_ctx_create("/nonexistent/ca.crt", "", "",
                                 false, false, &ctx);
    ASSERT_EQ(rc, -1);
    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_wrap_invalid_fd(void)
{
    fprintf(stdout, "  test_wrap_invalid_fd:              ");
    struct mds_tls_ctx *ctx = NULL;
    (void)mds_tls_ctx_create("", "", "", false, false, &ctx);
    struct mds_tls_conn *conn = NULL;
    int rc = mds_tls_wrap(ctx, -1, false, NULL, &conn);
    ASSERT_EQ(rc, -1);
    mds_tls_ctx_destroy(ctx);
    fprintf(stdout, "PASS\n");
    passed++;
}


static void test_pending_null(void)
{
    fprintf(stdout, "  test_pending_null:                 ");
    /* NULL conn → 0 pending bytes. */
    ASSERT_EQ(mds_tls_pending(NULL), (size_t)0);
    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_peek_null(void)
{
    fprintf(stdout, "  test_peek_null:                    ");
    char buf[4];
    ASSERT_EQ(mds_tls_peek(NULL, buf, sizeof(buf)), -1);
    fprintf(stdout, "PASS\n");
    passed++;
}


int main(void)
{
    fprintf(stdout, "test_mds_tls:\n");

    test_ctx_create_null();
    test_ctx_create_no_certs();
    test_ctx_create_bad_ca();
    test_wrap_invalid_fd();
    test_pending_null();
    test_peek_null();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}

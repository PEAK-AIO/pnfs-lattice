/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_mds_admin.c -- Unit tests for admin_util helpers (Item 47).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "admin_util.h"

static int passed = 0;
static int failed = 0;

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stdout, "FAIL (%s:%d: %s)\n", __FILE__, __LINE__, #cond); \
        failed++; return; \
    } \
} while (0)

#define ASSERT_EQ(a, b) ASSERT_TRUE((a) == (b))
#define ASSERT_STREQ(a, b) ASSERT_TRUE(strcmp((a), (b)) == 0)

/* ----------------------------------------------------------------------- */

static void test_parse_admin_endpoint_defaults(void)
{
    fprintf(stdout, "  test_parse_admin_endpoint_defaults: ");

    const char *argv[] = {"--owner", "42"};
    const char *host = NULL;
    uint16_t port = 0;
    bool json = false;
    parse_admin_endpoint(2, argv, &host, &port, &json);

    ASSERT_STREQ(host, DEFAULT_MDS_HOST);
    ASSERT_EQ(port, DEFAULT_MDS_PORT);
    ASSERT_TRUE(!json);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_parse_admin_endpoint_custom(void)
{
    fprintf(stdout, "  test_parse_admin_endpoint_custom:   ");

    const char *argv[] = {"--mds-host", "10.0.0.1", "--mds-port", "9999",
                          "--json"};
    const char *host = NULL;
    uint16_t port = 0;
    bool json = false;
    parse_admin_endpoint(5, argv, &host, &port, &json);

    ASSERT_STREQ(host, "10.0.0.1");
    ASSERT_EQ(port, 9999);
    ASSERT_TRUE(json);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_parse_admin_endpoint_json_null(void)
{
    fprintf(stdout, "  test_parse_admin_endpoint_json_null:");

    const char *argv[] = {"--json"};
    const char *host = NULL;
    uint16_t port = 0;
    /* json pointer is NULL -- should not crash. */
    parse_admin_endpoint(1, argv, &host, &port, NULL);

    ASSERT_STREQ(host, DEFAULT_MDS_HOST);
    ASSERT_EQ(port, DEFAULT_MDS_PORT);

    fprintf(stdout, " PASS\n");
    passed++;
}

static void test_parse_dest_endpoint_defaults(void)
{
    fprintf(stdout, "  test_parse_dest_endpoint_defaults:  ");

    const char *argv[] = {"--mds-host", "10.0.0.1"};
    const char *host = NULL;
    uint16_t port = 0;
    bool have_host = false;
    bool have_port = false;
    int rc = parse_dest_endpoint(2, argv, &host, &port,
                                  &have_host, &have_port);

    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(!have_host);
    ASSERT_TRUE(!have_port);
    ASSERT_STREQ(host, DEFAULT_DEST_HOST);
    ASSERT_EQ(port, DEFAULT_DEST_PORT);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_parse_dest_endpoint_custom(void)
{
    fprintf(stdout, "  test_parse_dest_endpoint_custom:    ");

    const char *argv[] = {"--host", "192.168.1.1", "--port", "8080"};
    const char *host = NULL;
    uint16_t port = 0;
    bool have_host = false;
    bool have_port = false;
    int rc = parse_dest_endpoint(4, argv, &host, &port,
                                  &have_host, &have_port);

    ASSERT_EQ(rc, 0);
    ASSERT_TRUE(have_host);
    ASSERT_TRUE(have_port);
    ASSERT_STREQ(host, "192.168.1.1");
    ASSERT_EQ(port, 8080);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_parse_dest_endpoint_invalid_port(void)
{
    fprintf(stdout, "  test_parse_dest_endpoint_invalid:   ");

    const char *argv[] = {"--host", "h", "--port", "0"};
    const char *host = NULL;
    uint16_t port = 0;
    bool have_host = false;
    bool have_port = false;
    int rc = parse_dest_endpoint(4, argv, &host, &port,
                                  &have_host, &have_port);

    ASSERT_EQ(rc, -1);

    fprintf(stdout, "PASS\n");
    passed++;
}


static void test_parse_admin_endpoint_bad_port_zero(void)
{
    fprintf(stdout, "  test_parse_admin_bad_port_zero:     ");

    const char *argv[] = {"--mds-port", "0"};
    const char *host = NULL;
    uint16_t port = 0;
    int rc = parse_admin_endpoint(2, argv, &host, &port, NULL);
    ASSERT_EQ(rc, -1);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_parse_admin_endpoint_bad_port_alpha(void)
{
    fprintf(stdout, "  test_parse_admin_bad_port_alpha:    ");

    const char *argv[] = {"--mds-port", "abc"};
    const char *host = NULL;
    uint16_t port = 0;
    int rc = parse_admin_endpoint(2, argv, &host, &port, NULL);
    ASSERT_EQ(rc, -1);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_parse_admin_endpoint_bad_port_overflow(void)
{
    fprintf(stdout, "  test_parse_admin_bad_port_overflow: ");

    const char *argv[] = {"--mds-port", "70000"};
    const char *host = NULL;
    uint16_t port = 0;
    int rc = parse_admin_endpoint(2, argv, &host, &port, NULL);
    ASSERT_EQ(rc, -1);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_parse_admin_endpoint_port_missing_value(void)
{
    fprintf(stdout, "  test_parse_admin_port_missing_val:  ");

    const char *argv[] = {"--mds-port"};
    const char *host = NULL;
    uint16_t port = 0;
    int rc = parse_admin_endpoint(1, argv, &host, &port, NULL);
    ASSERT_EQ(rc, -1);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* ----------------------------------------------------------------------- */

static void test_json_escape_plain(void)
{
    fprintf(stdout, "  test_json_escape_plain:             ");

    char out[256];
    int n = json_escape_string("hello world", out, sizeof(out));

    ASSERT_EQ(n, 11);
    ASSERT_STREQ(out, "hello world");

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_json_escape_special(void)
{
    fprintf(stdout, "  test_json_escape_special:           ");

    char out[256];
    int n = json_escape_string("a\"b\\c\nd", out, sizeof(out));

    /* Expected: a\"b\\c\nd */
    ASSERT_TRUE(n > 0);
    ASSERT_STREQ(out, "a\\\"b\\\\c\\nd");

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_json_escape_control(void)
{
    fprintf(stdout, "  test_json_escape_control:           ");

    char in[4] = {0x01, 0x1f, 'x', '\0'};
    char out[256];
    int n = json_escape_string(in, out, sizeof(out));

    /* \u0001\u001fx */
    ASSERT_TRUE(n > 0);
    ASSERT_STREQ(out, "\\u0001\\u001fx");

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_json_escape_truncation(void)
{
    fprintf(stdout, "  test_json_escape_truncation:        ");

    char out[4];
    int n = json_escape_string("hello", out, sizeof(out));

    /* Buffer too small -- should return -1. */
    ASSERT_EQ(n, -1);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_json_escape_null_input(void)
{
    fprintf(stdout, "  test_json_escape_null_input:        ");

    char out[16];
    int n = json_escape_string(NULL, out, sizeof(out));
    ASSERT_EQ(n, -1);

    n = json_escape_string("x", NULL, 16);
    ASSERT_EQ(n, -1);

    n = json_escape_string("x", out, 0);
    ASSERT_EQ(n, -1);

    fprintf(stdout, "PASS\n");
    passed++;
}

/* ----------------------------------------------------------------------- */

static void test_parse_ds_state_valid(void)
{
    fprintf(stdout, "  test_parse_ds_state_valid:          ");

    uint32_t out = 99;
    ASSERT_EQ(parse_ds_state("online", &out), 0);
    ASSERT_EQ(out, 0);
    ASSERT_EQ(parse_ds_state("offline", &out), 0);
    ASSERT_EQ(out, 1);
    ASSERT_EQ(parse_ds_state("draining", &out), 0);
    ASSERT_EQ(out, 2);
    ASSERT_EQ(parse_ds_state("rebalancing", &out), 0);
    ASSERT_EQ(out, 3);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_parse_ds_state_invalid(void)
{
    fprintf(stdout, "  test_parse_ds_state_invalid:        ");

    uint32_t out = 0;
    ASSERT_EQ(parse_ds_state("bogus", &out), -1);
    ASSERT_EQ(parse_ds_state(NULL, &out), -1);
    ASSERT_EQ(parse_ds_state("online", NULL), -1);

    fprintf(stdout, "PASS\n");
    passed++;
}


static void test_parse_ds_mode_valid(void)
{
    fprintf(stdout, "  test_parse_ds_mode_valid:           ");
    uint8_t out = 99;
    /* Patched mode removed -- only generic is accepted. */
    ASSERT_EQ(parse_ds_mode("patched", &out), -1);
    ASSERT_EQ(parse_ds_mode("generic", &out), 0);
    ASSERT_EQ(out, 1);
    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_parse_ds_mode_invalid(void)
{
    fprintf(stdout, "  test_parse_ds_mode_invalid:         ");
    uint8_t out = 0;
    ASSERT_EQ(parse_ds_mode("bogus", &out), -1);
    ASSERT_EQ(parse_ds_mode(NULL, &out), -1);
    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_parse_ds_transport_valid(void)
{
    fprintf(stdout, "  test_parse_ds_transport_valid:      ");
    uint8_t out = 0;
    ASSERT_EQ(parse_ds_transport("tcp", &out), 0);
    ASSERT_EQ(out, 0x01);
    ASSERT_EQ(parse_ds_transport("rdma", &out), 0);
    ASSERT_EQ(out, 0x02);
    ASSERT_EQ(parse_ds_transport("tcp,rdma", &out), 0);
    ASSERT_EQ(out, 0x03);
    ASSERT_EQ(parse_ds_transport("rdma,tcp", &out), 0);
    ASSERT_EQ(out, 0x03);
    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_parse_ds_transport_invalid(void)
{
    fprintf(stdout, "  test_parse_ds_transport_invalid:    ");
    uint8_t out = 0;
    ASSERT_EQ(parse_ds_transport("udp", &out), -1);
    ASSERT_EQ(parse_ds_transport(NULL, &out), -1);
    fprintf(stdout, "PASS\n");
    passed++;
}


int main(void)
{
    fprintf(stdout, "test_mds_admin:\n");

    test_parse_admin_endpoint_defaults();
    test_parse_admin_endpoint_custom();
    test_parse_admin_endpoint_json_null();
    test_parse_dest_endpoint_defaults();
    test_parse_dest_endpoint_custom();
    test_parse_dest_endpoint_invalid_port();
    test_parse_admin_endpoint_bad_port_zero();
    test_parse_admin_endpoint_bad_port_alpha();
    test_parse_admin_endpoint_bad_port_overflow();
    test_parse_admin_endpoint_port_missing_value();
    test_json_escape_plain();
    test_json_escape_special();
    test_json_escape_control();
    test_json_escape_truncation();
    test_json_escape_null_input();
    test_parse_ds_state_valid();
    test_parse_ds_state_invalid();
    test_parse_ds_mode_valid();
    test_parse_ds_mode_invalid();
    test_parse_ds_transport_valid();
    test_parse_ds_transport_invalid();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}

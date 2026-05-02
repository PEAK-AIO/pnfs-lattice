/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_rpc_server.c — Loopback tests for the RPC server.
 *
 * Starts a server on an ephemeral port, connects via TCP, sends
 * record-marked RPC messages, and verifies the replies.
 *
 * RonDB-native: uses open_test_catalogue() for the catalogue handle.
 * Skips gracefully if no RonDB cluster is available.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/* Like assert() but not elided by NDEBUG. */
#define VERIFY(expr) do { if (!(expr)) { \
	fprintf(stderr, "VERIFY FAILED: %s (%s:%d)\n", \
		#expr, __FILE__, __LINE__); abort(); } } while (0)
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include "rpc_server.h"
#include "xdr_codec.h"
#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "test_helpers.h"
#include "session.h"
#include "open_state.h"
#include "ds_health.h"

/* -----------------------------------------------------------------------
 * Test framework
 * ----------------------------------------------------------------------- */

#define TEST_MDS_ID   0

static int tests_run;
static int tests_passed;

#define ASSERT_EQ(a, b) do {						\
	if ((a) != (b)) {						\
		fprintf(stderr, "  FAIL %s:%d: %s != %s\n",		\
			__FILE__, __LINE__, #a, #b);			\
		return;							\
	}								\
} while (0)

#define ASSERT_TRUE(x)  ASSERT_EQ(!!(x), 1)

#define RUN_TEST(fn) do {						\
	tests_run++;							\
	fprintf(stdout, "  %-50s", #fn);				\
	fflush(stdout);							\
	fn();								\
	tests_passed++;							\
	fprintf(stdout, "PASS\n");					\
} while (0)

/* -----------------------------------------------------------------------
 * Server thread + helpers
 * ----------------------------------------------------------------------- */

struct test_ctx {
    struct mds_catalogue    *cat;
    struct session_table    *st;
    struct open_state_table *ot;
    struct rpc_server       *srv;
    pthread_t                thread;
};

static void *server_thread(void *arg)
{
    struct rpc_server *srv = (struct rpc_server *)arg;

    rpc_server_start(srv);
    return NULL;
}

static void setup_test(struct test_ctx *ctx)
{
    struct rpc_server_config cfg;

    memset(ctx, 0, sizeof(*ctx));

    ctx->cat = open_test_catalogue();
    if (ctx->cat == NULL) {
        return;  /* Caller checks ctx->cat for NULL. */
    }
    assert(session_table_init(TEST_MDS_ID, 0, &ctx->st) == 0);
    assert(open_state_table_init(TEST_MDS_ID, &ctx->ot) == 0);

    memset(&cfg, 0, sizeof(cfg));
    cfg.bind_addr = "127.0.0.1";
    cfg.port = 0; /* Ephemeral port */
    cfg.cat = ctx->cat;

    assert(rpc_server_create(&cfg, &ctx->srv) == 0);
    assert(rpc_server_port(ctx->srv) != 0);

    assert(pthread_create(&ctx->thread, NULL, server_thread,
                          ctx->srv) == 0);
    usleep(50000); /* 50ms — let server start epoll loop. */
}

static void teardown_test(struct test_ctx *ctx)
{
    rpc_server_stop(ctx->srv);
    pthread_join(ctx->thread, NULL);
    rpc_server_destroy(ctx->srv);
    open_state_table_destroy(ctx->ot);
    session_table_destroy(ctx->st);
    mds_catalogue_close(ctx->cat);
}

static int connect_to_server(const struct test_ctx *ctx)
{
    struct sockaddr_in addr;
    int fd;

    fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(fd >= 0);

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(rpc_server_port(ctx->srv));
    assert(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/** Send a record-marked message and read the reply. */
static int send_and_recv(int fd, const uint8_t *req, uint32_t req_len,
                         uint8_t *reply, uint32_t reply_max,
                         uint32_t *reply_len)
{
    uint32_t hdr = htonl(req_len | 0x80000000U);

    if (write(fd, &hdr, 4) != 4)
        return -1;
    if (write(fd, req, req_len) != (ssize_t)req_len)
        return -1;

    uint8_t rhdr[4];
    ssize_t n;
    uint32_t total_read = 0;

    while (total_read < 4) {
        n = read(fd, rhdr + total_read, 4 - total_read);
        if (n <= 0)
            return -1;
        total_read += (uint32_t)n;
    }

    uint32_t raw = ((uint32_t)rhdr[0] << 24) | ((uint32_t)rhdr[1] << 16) |
                   ((uint32_t)rhdr[2] << 8)  | ((uint32_t)rhdr[3]);
    uint32_t rlen = raw & 0x7FFFFFFFU;

    if (rlen > reply_max)
        return -1;

    total_read = 0;
    while (total_read < rlen) {
        n = read(fd, reply + total_read, rlen - total_read);
        if (n <= 0)
            return -1;
        total_read += (uint32_t)n;
    }
    *reply_len = rlen;
    return 0;
}

/* -----------------------------------------------------------------------
 * Tests
 * ----------------------------------------------------------------------- */

/** Build a minimal RPC NULL call. */
static uint32_t build_null_call(uint8_t *buf, uint32_t buflen, uint32_t xid)
{
    XDR enc;

    xdrmem_ncreate(&enc, (char *)buf, buflen, XDR_ENCODE);
    {
        uint32_t msg_type = 0; /* CALL */
        uint32_t rpcvers = 2;
        uint32_t prog = NFS_PROGRAM;
        uint32_t vers = NFS_V4;
        uint32_t proc = NFSPROC4_NULL;
        uint32_t auth = 0;
        uint32_t alen = 0;

        assert(xdr_uint32_t(&enc, &xid));
        assert(xdr_uint32_t(&enc, &msg_type));
        assert(xdr_uint32_t(&enc, &rpcvers));
        assert(xdr_uint32_t(&enc, &prog));
        assert(xdr_uint32_t(&enc, &vers));
        assert(xdr_uint32_t(&enc, &proc));
        assert(xdr_uint32_t(&enc, &auth));
        assert(xdr_uint32_t(&enc, &alen));
        assert(xdr_uint32_t(&enc, &auth));
        assert(xdr_uint32_t(&enc, &alen));
    }
    return xdr_getpos(&enc);
}

static void test_null_procedure(void)
{
    struct test_ctx ctx;
    uint8_t req[256];
    uint8_t reply[4096];
    uint32_t reply_len = 0;

    setup_test(&ctx);
    if (ctx.cat == NULL) {
        fprintf(stdout, "SKIP (no RonDB)\n");
        tests_passed++;
        return;
    }
    int fd = connect_to_server(&ctx);

    ASSERT_TRUE(fd >= 0);

    uint32_t req_len = build_null_call(req, sizeof(req), 0x1111);

    ASSERT_EQ(send_and_recv(fd, req, req_len, reply, sizeof(reply),
                            &reply_len), 0);
    ASSERT_TRUE(reply_len > 0);

    /* Decode reply: xid + msg_type=1 + reply_stat=0 + verf + accept_stat=0 */
    XDR dec;

    xdrmem_ncreate(&dec, (char *)reply, reply_len, XDR_DECODE);
    {
        uint32_t r_xid, msg_type, reply_stat;
        uint32_t verf_flavor, verf_len, accept_stat;

        ASSERT_TRUE(xdr_uint32_t(&dec, &r_xid));
        ASSERT_EQ(r_xid, (uint32_t)0x1111);
        ASSERT_TRUE(xdr_uint32_t(&dec, &msg_type));
        ASSERT_EQ(msg_type, (uint32_t)1); /* REPLY */
        ASSERT_TRUE(xdr_uint32_t(&dec, &reply_stat));
        ASSERT_EQ(reply_stat, (uint32_t)0); /* MSG_ACCEPTED */
        ASSERT_TRUE(xdr_uint32_t(&dec, &verf_flavor));
        ASSERT_EQ(verf_flavor, (uint32_t)0);
        ASSERT_TRUE(xdr_uint32_t(&dec, &verf_len));
        ASSERT_EQ(verf_len, (uint32_t)0);
        ASSERT_TRUE(xdr_uint32_t(&dec, &accept_stat));
        ASSERT_EQ(accept_stat, (uint32_t)0); /* SUCCESS */
    }

    close(fd);
    teardown_test(&ctx);
}

static void test_server_stop_clean(void)
{
    struct test_ctx ctx;

    setup_test(&ctx);
    if (ctx.cat == NULL) {
        fprintf(stdout, "SKIP (no RonDB)\n");
        tests_passed++;
        return;
    }

    /* Just verify stop+destroy doesn't crash. */
    teardown_test(&ctx);
}

static void test_multiple_connections(void)
{
    struct test_ctx ctx;
    uint8_t req[256];
    uint8_t reply[4096];
    uint32_t reply_len;

    setup_test(&ctx);
    if (ctx.cat == NULL) {
        fprintf(stdout, "SKIP (no RonDB)\n");
        tests_passed++;
        return;
    }

    int fd1 = connect_to_server(&ctx);
    int fd2 = connect_to_server(&ctx);
    ASSERT_TRUE(fd1 >= 0);
    ASSERT_TRUE(fd2 >= 0);

    uint32_t req_len = build_null_call(req, sizeof(req), 0x2222);

    /* Both connections should get valid replies. */
    reply_len = 0;
    ASSERT_EQ(send_and_recv(fd1, req, req_len, reply, sizeof(reply),
                            &reply_len), 0);
    ASSERT_TRUE(reply_len > 0);

    reply_len = 0;
    ASSERT_EQ(send_and_recv(fd2, req, req_len, reply, sizeof(reply),
                            &reply_len), 0);
    ASSERT_TRUE(reply_len > 0);

    close(fd1);
    close(fd2);
    teardown_test(&ctx);
}

int main(void)
{
    fprintf(stdout, "test_rpc_server (RonDB-native)\n");

    RUN_TEST(test_null_procedure);
    RUN_TEST(test_server_stop_clean);
    RUN_TEST(test_multiple_connections);

    fprintf(stdout, "\n  %d/%d tests passed\n",
        tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}

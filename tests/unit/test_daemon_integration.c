/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_daemon_integration.c -- Item 52 tests: RPC listener lifecycle,
 * real COMPOUND (PUTROOTFH + GETATTR), multi-listener SO_REUSEPORT.
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
#include <netinet/in.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "rpc_server.h"
#include "mds_catalogue.h"
#include "session.h"
#include "open_state.h"
#include "xdr_codec.h"
#include "compound.h"
#include "grace.h"

/* -------------------------------------------------------------------
 * Test infrastructure
 * ------------------------------------------------------------------- */

static int tests_run;
static int tests_passed;
static int test_failed;

#define ASSERT_EQ(a, b) do {                                    \
    if ((a) != (b)) {                                           \
        fprintf(stderr, "  FAIL %s:%d: %s != %s\n",            \
                __FILE__, __LINE__, #a, #b);                    \
        test_failed = 1;                                        \
        return;                                                 \
    }                                                           \
} while (0)

#define ASSERT_TRUE(cond) do {                                  \
    if (!(cond)) {                                              \
        fprintf(stderr, "  FAIL %s:%d: %s\n",                  \
                __FILE__, __LINE__, #cond);                     \
        test_failed = 1;                                        \
        return;                                                 \
    }                                                           \
} while (0)

#define RUN_TEST(fn) do {                                       \
    test_failed = 0;                                            \
    tests_run++;                                                \
    fn();                                                       \
    if (!test_failed) {                                         \
        tests_passed++;                                         \
        fprintf(stdout, "  %-55s PASS\n", #fn);                \
    }                                                           \
} while (0)

#define TEST_MAP_SIZE (10ULL * 1024 * 1024)

/* ------------------------------------------------------------------- */

static char *make_temp_db_path(void)
{
    char tmpl[] = "/tmp/pnfs-di-test-XXXXXX";
    const char *dir = mkdtemp(tmpl);
    if (dir == NULL) { return NULL; }

    size_t len = strlen(dir) + sizeof("/data.mdb");
    char *path = malloc(len);
    if (path == NULL) { return NULL; }
    (void)snprintf(path, len, "%s/data.mdb", dir);
    return path;
}

static void cleanup_temp_db(const char *path)
{
    char lock_path[512];
    (void)unlink(path);
    (void)snprintf(lock_path, sizeof(lock_path), "%s-lock", path);
    (void)unlink(lock_path);
    char dir[512];
    (void)snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash != NULL) {
        *slash = '\0';
        (void)rmdir(dir);
    }
}

static void *server_thread(void *arg)
{
    struct rpc_server *srv = arg;
    (void)rpc_server_start(srv);
    return NULL;
}

static int connect_to_port(uint16_t port)
{
    struct sockaddr_in addr;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { return -1; }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    (void)inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int send_and_recv(int fd, const uint8_t *req, uint32_t req_len,
                         uint8_t *reply, uint32_t reply_max,
                         uint32_t *reply_len)
{
    uint32_t hdr = htonl(req_len | 0x80000000U);
    if (write(fd, &hdr, 4) != 4) { return -1; }
    if (write(fd, req, req_len) != (ssize_t)req_len) { return -1; }

    uint8_t rhdr[4];
    uint32_t total_read = 0;
    while (total_read < 4) {
        ssize_t n = read(fd, rhdr + total_read, 4 - total_read);
        if (n <= 0) { return -1; }
        total_read += (uint32_t)n;
    }

    uint32_t raw = ((uint32_t)rhdr[0] << 24) | ((uint32_t)rhdr[1] << 16) |
                   ((uint32_t)rhdr[2] << 8)  | ((uint32_t)rhdr[3]);
    uint32_t rlen = raw & 0x7FFFFFFFU;
    if (rlen > reply_max) { return -1; }

    total_read = 0;
    while (total_read < rlen) {
        ssize_t n = read(fd, reply + total_read, rlen - total_read);
        if (n <= 0) { return -1; }
        total_read += (uint32_t)n;
    }
    *reply_len = rlen;
    return 0;
}

static uint32_t build_compound_putrootfh_getattr(uint8_t *buf,
                                                  uint32_t buflen,
                                                  uint32_t xid)
{
    XDR enc;
    xdrmem_ncreate(&enc, (char *)buf, buflen, XDR_ENCODE);

    /* RPC call header */
    {
        uint32_t msg_type = 0, rpcvers = 2;
        uint32_t prog = NFS_PROGRAM, vers = NFS_V4;
        uint32_t proc = NFSPROC4_COMPOUND;
        uint32_t auth = 0, alen = 0;

        VERIFY(xdr_uint32_t(&enc, &xid));
        VERIFY(xdr_uint32_t(&enc, &msg_type));
        VERIFY(xdr_uint32_t(&enc, &rpcvers));
        VERIFY(xdr_uint32_t(&enc, &prog));
        VERIFY(xdr_uint32_t(&enc, &vers));
        VERIFY(xdr_uint32_t(&enc, &proc));
        VERIFY(xdr_uint32_t(&enc, &auth));
        VERIFY(xdr_uint32_t(&enc, &alen));
        VERIFY(xdr_uint32_t(&enc, &auth));
        VERIFY(xdr_uint32_t(&enc, &alen));
    }

    /* COMPOUND4args: PUTROOTFH + GETATTR */
    {
        uint32_t tag_len = 0, minorver = 1, count = 2;
        VERIFY(xdr_uint32_t(&enc, &tag_len));
        VERIFY(xdr_uint32_t(&enc, &minorver));
        VERIFY(xdr_uint32_t(&enc, &count));

        /* OP_PUTROOTFH */
        {
            uint32_t op = OP_PUTROOTFH;
            VERIFY(xdr_uint32_t(&enc, &op));
        }

        /* OP_GETATTR */
        {
            uint32_t op = OP_GETATTR;
            uint32_t words = NFS4_BITMAP_WORDS;
            uint32_t w = 0xFFFFFFFF;
            VERIFY(xdr_uint32_t(&enc, &op));
            VERIFY(xdr_uint32_t(&enc, &words));
            for (uint32_t bi = 0; bi < NFS4_BITMAP_WORDS; bi++) {
                VERIFY(xdr_uint32_t(&enc, &w));
            }
        }
    }
    return xdr_getpos(&enc);
}

/* ===================================================================
 * Test 1: Lifecycle smoke -- create, start, stop, destroy.
 * =================================================================== */
static void test_lifecycle_smoke(void)
{
    struct mds_catalogue *db = NULL;
    char *db_path = make_temp_db_path();
    ASSERT_TRUE(db_path != NULL);
    db = open_test_catalogue(); ASSERT_TRUE(db != NULL);

    struct rpc_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.bind_addr = "127.0.0.1";
    cfg.port = 0; /* ephemeral */
    cfg.db = db;

    struct rpc_server *srv = NULL;
    ASSERT_EQ(rpc_server_create(&cfg, &srv), 0);
    ASSERT_TRUE(rpc_server_port(srv) != 0);

    pthread_t tid;
    ASSERT_EQ(pthread_create(&tid, NULL, server_thread, srv), 0);
    usleep(50000); /* 50ms for epoll to start */

    rpc_server_stop(srv);
    (void)pthread_join(tid, NULL);
    rpc_server_destroy(srv);

    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* ===================================================================
 * Test 2: Real COMPOUND -- PUTROOTFH + GETATTR over live server.
 * =================================================================== */
static void test_compound_putrootfh_getattr(void)
{
    struct mds_catalogue *db = NULL;
    char *db_path = make_temp_db_path();
    ASSERT_TRUE(db_path != NULL);
    db = open_test_catalogue(); ASSERT_TRUE(db != NULL);

    struct mds_catalogue *cat = NULL;
    cat = db;

    grace_init();

    struct rpc_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.bind_addr = "127.0.0.1";
    cfg.port = 0;
    cfg.db = db;
    cfg.cat = cat;

    struct rpc_server *srv = NULL;
    ASSERT_EQ(rpc_server_create(&cfg, &srv), 0);

    pthread_t tid;
    ASSERT_EQ(pthread_create(&tid, NULL, server_thread, srv), 0);
    usleep(50000);

    uint16_t port = rpc_server_port(srv);

    /* Connect and send PUTROOTFH + GETATTR */
    int fd = connect_to_port(port);
    ASSERT_TRUE(fd >= 0);

    uint8_t req[512];
    uint32_t req_len = build_compound_putrootfh_getattr(
        req, sizeof(req), 0x4242);

    uint8_t reply[8192];
    uint32_t reply_len = 0;
    ASSERT_EQ(send_and_recv(fd, req, req_len, reply, sizeof(reply),
                            &reply_len), 0);
    ASSERT_TRUE(reply_len > 0);

    /* Decode reply: skip RPC header, read COMPOUND4res status. */
    XDR dec;
    xdrmem_ncreate(&dec, (char *)reply, reply_len, XDR_DECODE);
    {
        uint32_t xid, msg_type, reply_stat, auth_flavor, auth_len;
        uint32_t accept_stat;
        ASSERT_TRUE(xdr_uint32_t(&dec, &xid));
        ASSERT_EQ(xid, (uint32_t)0x4242);
        ASSERT_TRUE(xdr_uint32_t(&dec, &msg_type));
        ASSERT_EQ(msg_type, (uint32_t)1); /* REPLY */
        ASSERT_TRUE(xdr_uint32_t(&dec, &reply_stat));
        ASSERT_EQ(reply_stat, (uint32_t)0); /* MSG_ACCEPTED */
        ASSERT_TRUE(xdr_uint32_t(&dec, &auth_flavor));
        ASSERT_TRUE(xdr_uint32_t(&dec, &auth_len));
        ASSERT_TRUE(xdr_uint32_t(&dec, &accept_stat));
        ASSERT_EQ(accept_stat, (uint32_t)0); /* SUCCESS */

        /* COMPOUND4res: status + tag + op_count */
        uint32_t cstatus, tag_len, op_count;
        ASSERT_TRUE(xdr_uint32_t(&dec, &cstatus));
        ASSERT_EQ(cstatus, (uint32_t)NFS4_OK);
        ASSERT_TRUE(xdr_uint32_t(&dec, &tag_len));
        ASSERT_TRUE(xdr_uint32_t(&dec, &op_count));
        ASSERT_EQ(op_count, (uint32_t)2);

        /* Result 0: PUTROOTFH */
        uint32_t op0, st0;
        ASSERT_TRUE(xdr_uint32_t(&dec, &op0));
        ASSERT_EQ(op0, (uint32_t)OP_PUTROOTFH);
        ASSERT_TRUE(xdr_uint32_t(&dec, &st0));
        ASSERT_EQ(st0, (uint32_t)NFS4_OK);

        /* Result 1: GETATTR */
        uint32_t op1, st1;
        ASSERT_TRUE(xdr_uint32_t(&dec, &op1));
        ASSERT_EQ(op1, (uint32_t)OP_GETATTR);
        ASSERT_TRUE(xdr_uint32_t(&dec, &st1));
        ASSERT_EQ(st1, (uint32_t)NFS4_OK);
    }

    close(fd);
    rpc_server_stop(srv);
    (void)pthread_join(tid, NULL);
    rpc_server_destroy(srv);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
}

/* ===================================================================
 * Test 3: Multi-listener -- two listeners on same port (SO_REUSEPORT).
 * =================================================================== */
static void test_multi_listener(void)
{
#ifndef SO_REUSEPORT
    /* SO_REUSEPORT not available -- skip. */
    fprintf(stdout, "  %-55s SKIP (no SO_REUSEPORT)\n",
            "test_multi_listener");
    return;
#else
    struct mds_catalogue *db = NULL;
    char *db_path = make_temp_db_path();
    ASSERT_TRUE(db_path != NULL);
    db = open_test_catalogue(); ASSERT_TRUE(db != NULL);

    struct rpc_server_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.bind_addr = "127.0.0.1";
    cfg.port = 0; /* ephemeral -- OS picks */
    cfg.db = db;

    /* Listener 1: ephemeral port. */
    struct rpc_server *srv1 = NULL;
    ASSERT_EQ(rpc_server_create(&cfg, &srv1), 0);
    uint16_t port = rpc_server_port(srv1);
    ASSERT_TRUE(port != 0);

    /* Listener 2: bind to same explicit port. */
    cfg.port = port;
    struct rpc_server *srv2 = NULL;
    ASSERT_EQ(rpc_server_create(&cfg, &srv2), 0);
    ASSERT_EQ(rpc_server_port(srv2), port);

    /* Start both. */
    pthread_t t1, t2;
    ASSERT_EQ(pthread_create(&t1, NULL, server_thread, srv1), 0);
    ASSERT_EQ(pthread_create(&t2, NULL, server_thread, srv2), 0);
    usleep(50000);

    /* Verify at least one connection succeeds. */
    int fd = connect_to_port(port);
    ASSERT_TRUE(fd >= 0);
    close(fd);

    rpc_server_stop(srv1);
    rpc_server_stop(srv2);
    (void)pthread_join(t1, NULL);
    (void)pthread_join(t2, NULL);
    rpc_server_destroy(srv1);
    rpc_server_destroy(srv2);
    mds_catalogue_close(db);
    cleanup_temp_db(db_path);
    free(db_path);
#endif
}

/* -------------------------------------------------------------------
 * Main
 * ------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "test_daemon_integration:\n");

    RUN_TEST(test_lifecycle_smoke);
    RUN_TEST(test_compound_putrootfh_getattr);
    RUN_TEST(test_multi_listener);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? EXIT_SUCCESS : EXIT_FAILURE;
}

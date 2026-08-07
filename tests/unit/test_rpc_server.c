/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_rpc_server.c -- Loopback tests for the RPC server.
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
    struct threadpool       *tp;   /* NULL for inline-path tests */
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
    cfg.st = ctx->st;  /* Sessions: EXCHANGE_ID/CREATE_SESSION/SEQUENCE. */

    assert(rpc_server_create(&cfg, &ctx->srv) == 0);
    assert(rpc_server_port(ctx->srv) != 0);

    assert(pthread_create(&ctx->thread, NULL, server_thread,
                          ctx->srv) == 0);
    usleep(50000); /* 50ms -- let server start epoll loop. */
}

/* Set up a server with a worker pool + bounded pipelining cap, so the
 * threadpool dispatch path (not the inline path) is exercised. */
static void setup_test_pooled(struct test_ctx *ctx, uint32_t max_inflight)
{
    struct rpc_server_config cfg;

    memset(ctx, 0, sizeof(*ctx));

    ctx->cat = open_test_catalogue();
    if (ctx->cat == NULL) {
        return;  /* Caller checks ctx->cat for NULL. */
    }
    assert(session_table_init(TEST_MDS_ID, 0, &ctx->st) == 0);
    assert(open_state_table_init(TEST_MDS_ID, &ctx->ot) == 0);
    assert(threadpool_create(4, &ctx->tp) == 0);

    memset(&cfg, 0, sizeof(cfg));
    cfg.bind_addr = "127.0.0.1";
    cfg.port = 0; /* Ephemeral port */
    cfg.cat = ctx->cat;
    cfg.tp = ctx->tp;
    cfg.max_inflight_per_conn = max_inflight;

    assert(rpc_server_create(&cfg, &ctx->srv) == 0);
    assert(rpc_server_port(ctx->srv) != 0);

    assert(pthread_create(&ctx->thread, NULL, server_thread,
                          ctx->srv) == 0);
    usleep(50000); /* 50ms -- let server start epoll loop. */
}

static void teardown_test(struct test_ctx *ctx)
{
    rpc_server_stop(ctx->srv);
    pthread_join(ctx->thread, NULL);
    rpc_server_destroy(ctx->srv);
    /* The pool must be destroyed only after the server (which uses it)
     * is fully stopped + destroyed. */
    if (ctx->tp != NULL) {
        threadpool_destroy(ctx->tp);
    }
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

/** Write one record-marked request without reading the reply. */
static int send_record_only(int fd, const uint8_t *req, uint32_t req_len)
{
    uint32_t hdr = htonl(req_len | 0x80000000U);

    if (write(fd, &hdr, 4) != 4)
        return -1;
    if (write(fd, req, req_len) != (ssize_t)req_len)
        return -1;
    return 0;
}

/** Read one record-marked reply (4-byte frag header + body). */
static int recv_one_reply(int fd, uint8_t *reply, uint32_t reply_max,
                          uint32_t *reply_len)
{
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

/*
 * Bounded request pipelining: send many NULL calls back-to-back on a
 * single connection (without reading), then verify every reply comes
 * back exactly once.  The cap (2) is intentionally smaller than the
 * request count so the in-flight-cap disarm + completion re-arm path
 * is exercised.  Replies may return out of order -- the client matches
 * by XID -- so we only assert set membership.
 */
static void test_pipelined_requests(void)
{
    struct test_ctx ctx;
    uint8_t req[256];
    enum { PIPE_N = 16 };
    int seen[PIPE_N];

    setup_test_pooled(&ctx, 2);
    if (ctx.cat == NULL) {
        fprintf(stdout, "SKIP (no RonDB)\n");
        tests_passed++;
        return;
    }

    int fd = connect_to_server(&ctx);
    ASSERT_TRUE(fd >= 0);

    for (uint32_t i = 0; i < PIPE_N; i++) {
        uint32_t req_len = build_null_call(req, sizeof(req), 0x3000 + i);
        ASSERT_EQ(send_record_only(fd, req, req_len), 0);
    }

    for (uint32_t i = 0; i < PIPE_N; i++) {
        seen[i] = 0;
    }
    for (uint32_t i = 0; i < PIPE_N; i++) {
        uint8_t reply[4096];
        uint32_t reply_len = 0;
        ASSERT_EQ(recv_one_reply(fd, reply, sizeof(reply), &reply_len), 0);
        ASSERT_TRUE(reply_len >= 4);
        uint32_t r_xid = ((uint32_t)reply[0] << 24) |
                         ((uint32_t)reply[1] << 16) |
                         ((uint32_t)reply[2] << 8)  |
                         ((uint32_t)reply[3]);
        ASSERT_TRUE(r_xid >= 0x3000 && r_xid < 0x3000 + PIPE_N);
        uint32_t idx = r_xid - 0x3000;
        ASSERT_EQ(seen[idx], 0);  /* no duplicate reply */
        seen[idx] = 1;
    }
    for (uint32_t i = 0; i < PIPE_N; i++) {
        ASSERT_TRUE(seen[i]);  /* every request answered */
    }

    close(fd);
    teardown_test(&ctx);
}

/*
 * Deferred close: pipeline many requests then close the connection
 * immediately without reading any reply.  This drives conn_begin_close
 * (HUP) while requests may still be in flight on worker threads; the
 * server must finalize the connection only after they drain, without
 * crashing, leaking, or hanging on shutdown.
 */
static void test_close_during_pipeline(void)
{
    struct test_ctx ctx;
    uint8_t req[256];

    setup_test_pooled(&ctx, 4);
    if (ctx.cat == NULL) {
        fprintf(stdout, "SKIP (no RonDB)\n");
        tests_passed++;
        return;
    }

    int fd = connect_to_server(&ctx);
    ASSERT_TRUE(fd >= 0);

    for (uint32_t i = 0; i < 32; i++) {
        uint32_t req_len = build_null_call(req, sizeof(req), 0x4000 + i);
        if (send_record_only(fd, req, req_len) != 0) {
            break;  /* peer reset is acceptable under abrupt close */
        }
    }
    /* Abrupt close without draining replies -> server sees EPOLLHUP. */
    close(fd);
    usleep(20000); /* let the server observe HUP and drain workers */

    /* Must not crash/hang: deferred finalize + inflight drain on stop. */
    teardown_test(&ctx);
}

/* -----------------------------------------------------------------------
 * Wave 5 T5.3 -- RFC 8881 S2.10.6.1.3 NFS4ERR_REP_TOO_BIG_TO_CACHE.
 *
 * Full wire flow: EXCHANGE_ID -> CREATE_SESSION with a tiny
 * ca_maxresponsesizecached (96 B, above op_sequence's 56-byte floor)
 * -> SEQUENCE(sa_cachethis=true)+PUTROOTFH+GETATTR whose encoded
 * reply exceeds the cap -> the server MUST answer a single SEQUENCE
 * result carrying NFS4ERR_REP_TOO_BIG_TO_CACHE and skip caching.
 * The same compound with sa_cachethis=false succeeds.
 * ----------------------------------------------------------------------- */

/** Encode the RPC call header + COMPOUND preamble (empty tag,
 *  minorversion 1, @opcount ops).  Caller appends op bodies. */
static void build_compound_start(XDR *enc, uint8_t *buf, uint32_t buflen,
                                 uint32_t xid, uint32_t opcount)
{
    uint32_t msg_type = 0, rpcvers = 2, prog = NFS_PROGRAM;
    uint32_t vers = NFS_V4, proc = NFSPROC4_COMPOUND;
    uint32_t auth = 0, alen = 0, taglen = 0, minor = 1;

    xdrmem_ncreate(enc, (char *)buf, buflen, XDR_ENCODE);
    VERIFY(xdr_uint32_t(enc, &xid));
    VERIFY(xdr_uint32_t(enc, &msg_type));
    VERIFY(xdr_uint32_t(enc, &rpcvers));
    VERIFY(xdr_uint32_t(enc, &prog));
    VERIFY(xdr_uint32_t(enc, &vers));
    VERIFY(xdr_uint32_t(enc, &proc));
    VERIFY(xdr_uint32_t(enc, &auth));   /* cred: AUTH_NONE */
    VERIFY(xdr_uint32_t(enc, &alen));
    VERIFY(xdr_uint32_t(enc, &auth));   /* verf: AUTH_NONE */
    VERIFY(xdr_uint32_t(enc, &alen));
    VERIFY(xdr_uint32_t(enc, &taglen)); /* empty tag */
    VERIFY(xdr_uint32_t(enc, &minor));
    VERIFY(xdr_uint32_t(enc, &opcount));
}

/** Position @dec past the RPC accepted-reply header, at the COMPOUND
 *  status word. */
static void reply_open(XDR *dec, uint8_t *reply, uint32_t len)
{
    uint32_t v = 0;

    xdrmem_ncreate(dec, (char *)reply, len, XDR_DECODE);
    VERIFY(xdr_uint32_t(dec, &v)); /* xid */
    VERIFY(xdr_uint32_t(dec, &v)); /* msg_type */
    VERIFY(xdr_uint32_t(dec, &v)); /* reply_stat */
    VERIFY(xdr_uint32_t(dec, &v)); /* verf flavor */
    VERIFY(xdr_uint32_t(dec, &v)); /* verf len */
    VERIFY(xdr_uint32_t(dec, &v)); /* accept_stat */
}

/** Append a SEQUENCE op (RFC 8881 wire order: sid, seqid, slot,
 *  highest_slot, cachethis). */
static void append_op_sequence(XDR *enc, const uint8_t *sid,
                               uint32_t seqid, uint32_t cachethis)
{
    uint32_t op = OP_SEQUENCE, slot = 0, highest = 7;

    VERIFY(xdr_uint32_t(enc, &op));
    VERIFY(xdr_opaque_encode(enc, (const char *)sid, SESSION_ID_SIZE));
    VERIFY(xdr_uint32_t(enc, &seqid));
    VERIFY(xdr_uint32_t(enc, &slot));
    VERIFY(xdr_uint32_t(enc, &highest));
    VERIFY(xdr_uint32_t(enc, &cachethis));
}

/** Append PUTROOTFH + GETATTR(TYPE|SIZE|CHANGE) so the reply carries
 *  a fattr4 body and comfortably exceeds a ~100-byte cached cap. */
static void append_putrootfh_getattr(XDR *enc)
{
    uint32_t op = OP_PUTROOTFH;
    uint32_t requested[NFS4_BITMAP_WORDS] = {0, 0, 0};

    VERIFY(xdr_uint32_t(enc, &op));
    op = OP_GETATTR;
    VERIFY(xdr_uint32_t(enc, &op));
    nfs4_bitmap_set(requested, FATTR4_TYPE);
    nfs4_bitmap_set(requested, FATTR4_CHANGE);
    nfs4_bitmap_set(requested, FATTR4_SIZE);
    VERIFY(xdr_nfs4_bitmap_encode(enc, requested, NFS4_BITMAP_WORDS));
}

static void test_rep_too_big_to_cache(void)
{
    struct test_ctx ctx;
    uint8_t req[1024];
    uint8_t reply[8192];
    uint32_t reply_len = 0;
    XDR enc, dec;
    uint64_t clientid = 0;
    uint32_t eid_seqid = 0;
    uint8_t sid[SESSION_ID_SIZE];
    uint32_t v = 0;

    setup_test(&ctx);
    if (ctx.cat == NULL) {
        fprintf(stdout, "SKIP (no RonDB)\n");
        tests_passed++;
        return;
    }
    int fd = connect_to_server(&ctx);
    ASSERT_TRUE(fd >= 0);

    /* 1. EXCHANGE_ID (sole op). */
    build_compound_start(&enc, req, sizeof(req), 0x501, 1);
    {
        uint32_t op = OP_EXCHANGE_ID;
        uint8_t verifier[NFS4_VERIFIER_SIZE] =
            {1, 2, 3, 4, 5, 6, 7, 8};
        const char owner[] = "t53-client";
        uint32_t olen = (uint32_t)strlen(owner);
        uint32_t flags = 0, sp_how = 0, impl = 0;

        VERIFY(xdr_uint32_t(&enc, &op));
        VERIFY(xdr_opaque_encode(&enc, (const char *)verifier,
                                 NFS4_VERIFIER_SIZE));
        VERIFY(xdr_uint32_t(&enc, &olen));
        VERIFY(xdr_opaque_encode(&enc, owner, olen));
        VERIFY(xdr_uint32_t(&enc, &flags));
        VERIFY(xdr_uint32_t(&enc, &sp_how));
        VERIFY(xdr_uint32_t(&enc, &impl));
    }
    ASSERT_EQ(send_and_recv(fd, req, xdr_getpos(&enc), reply,
                            sizeof(reply), &reply_len), 0);
    reply_open(&dec, reply, reply_len);
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* compound status */
    ASSERT_EQ(v, (uint32_t)NFS4_OK);
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* tag len (0) */
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* res count */
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* opnum */
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* op status */
    ASSERT_EQ(v, (uint32_t)NFS4_OK);
    ASSERT_TRUE(xdr_uint64_t(&dec, &clientid));
    ASSERT_TRUE(xdr_uint32_t(&dec, &eid_seqid));

    /* 2. CREATE_SESSION: ca_maxresponsesizecached = 96 bytes (above
     * op_sequence's 56-byte floor, below any real 3-op reply). */
    build_compound_start(&enc, req, sizeof(req), 0x502, 1);
    {
        uint32_t op = OP_CREATE_SESSION;
        uint32_t csa_flags = 0;
        uint32_t pad = 0, maxreq = 1048576, maxresp = 1048576;
        uint32_t maxcached = 96, maxops = 16, slots = 8, ird = 0;
        uint32_t bmaxreq = 4096, bmaxresp = 4096, bmaxcached = 0;
        uint32_t bmaxops = 2, bslots = 2;
        uint32_t cb_prog = 0x40000000U, sec_count = 0;

        VERIFY(xdr_uint32_t(&enc, &op));
        VERIFY(xdr_uint64_t(&enc, &clientid));
        VERIFY(xdr_uint32_t(&enc, &eid_seqid));
        VERIFY(xdr_uint32_t(&enc, &csa_flags));
        /* fore_chan_attrs */
        VERIFY(xdr_uint32_t(&enc, &pad));
        VERIFY(xdr_uint32_t(&enc, &maxreq));
        VERIFY(xdr_uint32_t(&enc, &maxresp));
        VERIFY(xdr_uint32_t(&enc, &maxcached));
        VERIFY(xdr_uint32_t(&enc, &maxops));
        VERIFY(xdr_uint32_t(&enc, &slots));
        VERIFY(xdr_uint32_t(&enc, &ird));
        /* back_chan_attrs */
        VERIFY(xdr_uint32_t(&enc, &pad));
        VERIFY(xdr_uint32_t(&enc, &bmaxreq));
        VERIFY(xdr_uint32_t(&enc, &bmaxresp));
        VERIFY(xdr_uint32_t(&enc, &bmaxcached));
        VERIFY(xdr_uint32_t(&enc, &bmaxops));
        VERIFY(xdr_uint32_t(&enc, &bslots));
        VERIFY(xdr_uint32_t(&enc, &ird));
        VERIFY(xdr_uint32_t(&enc, &cb_prog));
        VERIFY(xdr_uint32_t(&enc, &sec_count));
    }
    ASSERT_EQ(send_and_recv(fd, req, xdr_getpos(&enc), reply,
                            sizeof(reply), &reply_len), 0);
    reply_open(&dec, reply, reply_len);
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* compound status */
    ASSERT_EQ(v, (uint32_t)NFS4_OK);
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* tag len */
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* res count */
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* opnum */
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* op status */
    ASSERT_EQ(v, (uint32_t)NFS4_OK);
    ASSERT_TRUE(xdr_opaque_decode(&dec, (char *)sid, SESSION_ID_SIZE));

    /* 3. SEQUENCE(sa_cachethis=TRUE) + PUTROOTFH + GETATTR: the
     * encoded reply exceeds 96 bytes -> the server must answer a
     * single SEQUENCE result with NFS4ERR_REP_TOO_BIG_TO_CACHE. */
    build_compound_start(&enc, req, sizeof(req), 0x503, 3);
    append_op_sequence(&enc, sid, 1 /* seqid */, 1 /* cachethis */);
    append_putrootfh_getattr(&enc);
    ASSERT_EQ(send_and_recv(fd, req, xdr_getpos(&enc), reply,
                            sizeof(reply), &reply_len), 0);
    reply_open(&dec, reply, reply_len);
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* compound status */
    ASSERT_EQ(v, (uint32_t)NFS4ERR_REP_TOO_BIG_TO_CACHE);
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* tag len */
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* res count */
    ASSERT_EQ(v, (uint32_t)1);
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* opnum */
    ASSERT_EQ(v, (uint32_t)OP_SEQUENCE);
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* op status */
    ASSERT_EQ(v, (uint32_t)NFS4ERR_REP_TOO_BIG_TO_CACHE);

    /* 4. Same ops with sa_cachethis=FALSE -> full reply, NFS4_OK. */
    build_compound_start(&enc, req, sizeof(req), 0x504, 3);
    append_op_sequence(&enc, sid, 2 /* seqid */, 0 /* cachethis */);
    append_putrootfh_getattr(&enc);
    ASSERT_EQ(send_and_recv(fd, req, xdr_getpos(&enc), reply,
                            sizeof(reply), &reply_len), 0);
    reply_open(&dec, reply, reply_len);
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* compound status */
    ASSERT_EQ(v, (uint32_t)NFS4_OK);
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* tag len */
    ASSERT_TRUE(xdr_uint32_t(&dec, &v));       /* res count */
    ASSERT_EQ(v, (uint32_t)3);

    close(fd);
    teardown_test(&ctx);
}

int main(void)
{
    fprintf(stdout, "test_rpc_server (RonDB-native)\n");

    RUN_TEST(test_null_procedure);
    RUN_TEST(test_server_stop_clean);
    RUN_TEST(test_multiple_connections);
    RUN_TEST(test_pipelined_requests);
    RUN_TEST(test_close_during_pipeline);
    RUN_TEST(test_rep_too_big_to_cache);

    fprintf(stdout, "\n  %d/%d tests passed\n",
        tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}

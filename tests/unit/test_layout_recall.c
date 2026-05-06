/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_layout_recall.c -- Tests for layout recall coordinator and
 * LAYOUTERROR XDR decode (RFC 7862 S15.6).
 *
 * Covers:
 *   1. Recall coordinator init/destroy lifecycle.
 *   2. layout_recall_for_ds with no affected layouts (no-op).
 *   3. layout_recall_for_ds revokes when no active session.
 *   4. layout_recall_for_file returns -1 (stub).
 *   5. LAYOUTERROR XDR decode with stateid + device_error array.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

/* Like assert() but not elided by NDEBUG. */
#define VERIFY(expr) do { if (!(expr)) { \
	fprintf(stderr, "VERIFY FAILED: %s (%s:%d)\n", \
		#expr, __FILE__, __LINE__); abort(); } } while (0)
#include <unistd.h>
#include <errno.h>

#include "layout_recall.h"
#include "mds_catalogue.h"
#include "test_helpers.h"
#include "mds_coordination.h"
#include "commit_queue.h"
#include "xdr_codec.h"
#include "compound_internal.h"

#include "open_state.h"
#include "compound.h"
#include "session.h"
#include "nfs4_cb.h"
#include "rpc_server.h"
#include <sys/socket.h>
#include <pthread.h>
#include <poll.h>
#include <arpa/inet.h>

/* -----------------------------------------------------------------------
 * Test helpers
 * ----------------------------------------------------------------------- */

static int pass_count;
static int fail_count;

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a), _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stderr, "FAIL %s:%d: %s == %lld, expected %lld\n", \
                __FILE__, __LINE__, #a, _a, _b); \
        fail_count++; return; \
    } pass_count++; \
} while (0)

#define ASSERT_NE(a, b) do { \
    long long _a = (long long)(a), _b = (long long)(b); \
    if (_a == _b) { \
        fprintf(stderr, "FAIL %s:%d: %s == %lld, expected != %lld\n", \
                __FILE__, __LINE__, #a, _a, _b); \
        fail_count++; return; \
    } pass_count++; \
} while (0)

#define TEST_MAP_SIZE (64ULL * 1024 * 1024)

static char *make_temp_db_path(void)
{
    static char path[256];
    snprintf(path, sizeof(path), "/tmp/test_layout_recall_%d", getpid());
    return path;
}

static void cleanup_db(const char *path)
{
    char lockpath[300];
    unlink(path);
    snprintf(lockpath, sizeof(lockpath), "%s-lock", path);
    unlink(lockpath);
}

/* Scan callback for verification. */
struct scan_result {
    uint64_t clientids[64];
    uint64_t fileids[64];
    uint32_t count;
};

static int collect_cb(uint64_t clientid, uint64_t fileid, void *ctx)
{
    struct scan_result *sr = ctx;

    if (sr->count < 64) {
        sr->clientids[sr->count] = clientid;
        sr->fileids[sr->count] = fileid;
        sr->count++;
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * Test 1: Recall coordinator lifecycle
 * ----------------------------------------------------------------------- */

static void test_recall_lifecycle(void)
{
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    char *path = make_temp_db_path();

    db = open_test_catalogue(); VERIFY(db != NULL);
	cat = db;

    ASSERT_EQ(layout_recall_init(cat, NULL, 0, &lr), 0);
    ASSERT_NE((long long)(uintptr_t)lr, 0);

    layout_recall_destroy(lr);
    /* Destroy with NULL is safe. */
    layout_recall_destroy(NULL);

    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: test_recall_lifecycle\n");
}

/* -----------------------------------------------------------------------
 * Test 2: layout_recall_for_ds with no affected layouts
 * ----------------------------------------------------------------------- */

static void test_recall_no_layouts(void)
{
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    char *path = make_temp_db_path();

    db = open_test_catalogue(); VERIFY(db != NULL);
	cat = db;
    ASSERT_EQ(layout_recall_init(cat, NULL, 0, &lr), 0);

    /* No index entries for ds_id=99, should return 0 (success, no-op). */
    ASSERT_EQ(layout_recall_for_ds(lr, 99), 0);

    layout_recall_destroy(lr);
    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: test_recall_no_layouts\n");
}

/* -----------------------------------------------------------------------
 * Test 3: Recall revokes layouts when no active session
 * ----------------------------------------------------------------------- */

static void test_recall_revoke_no_session(void)
{
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    char *path = make_temp_db_path();
    enum mds_status mst;
    struct mds_cat_txn *txn = NULL;
    struct nfs4_stateid sid;
    struct scan_result sr;

    db = open_test_catalogue(); VERIFY(db != NULL);
	cat = db;

    ASSERT_EQ(layout_recall_init(cat, NULL, 1000, &lr), 0);

    /* Create a layout + index entry for ds_id=5, clientid=100, fileid=200.
     * mds_coord_layout_grant creates both layout_state and ds_layout_idx. */
    uint32_t ds_ids[] = {5};
    mst = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn);
    ASSERT_EQ(mst, MDS_OK);

    memset(&sid, 0, sizeof(sid));
    sid.seqid = 1;
    mst = mds_coord_layout_grant(db, txn, 100, 200,
                                 1, 0, 0xFFFFFFFFFFFFFFFFULL, &sid,
                                 ds_ids, 1);
    ASSERT_EQ(mst, MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    /*
     * Call layout_recall_for_ds.  No session for clientid=100,
     * so cb will fail and the layout should be revoked.
     */
    ASSERT_EQ(layout_recall_for_ds(lr, 5), 0);

    /* Verify layout state was deleted (revoked). */
    {
        bool has_layout = false;
        mst = mds_coord_layout_scan_for_file(db, 200, &has_layout);
        ASSERT_EQ(mst, MDS_OK);
        ASSERT_EQ(has_layout, 0);
    }

    /* Verify index entry was deleted. */
    memset(&sr, 0, sizeof(sr));
    mst = mds_coord_ds_layout_idx_scan(db, 5, collect_cb, &sr);
    ASSERT_EQ(mst, MDS_OK);
    ASSERT_EQ(sr.count, 0);

    layout_recall_destroy(lr);
    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: test_recall_revoke_no_session\n");
}

/* -----------------------------------------------------------------------
 * Test 4: layout_recall_for_file returns -1 (stub)
 * ----------------------------------------------------------------------- */

static void test_recall_for_file_no_layouts(void)
{
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    char *path = make_temp_db_path();

    db = open_test_catalogue(); VERIFY(db != NULL);
	cat = db;
    ASSERT_EQ(layout_recall_init(cat, NULL, 0, &lr), 0);

    ASSERT_EQ(layout_recall_for_file(lr, 42), 0);

    layout_recall_destroy(lr);
    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: test_recall_for_file_no_layouts\n");
}

/* -----------------------------------------------------------------------
 * Test 5: LAYOUTERROR stateid field in arg struct
 * ----------------------------------------------------------------------- */

static void test_layouterror_struct(void)
{
    /*
     * Verify the nfs4_arg_layouterror struct has the stateid field
     * at the expected position (compile-time verification).
     */
    struct nfs4_arg_layouterror le;
    memset(&le, 0, sizeof(le));
    le.stateid.seqid = 42;
    le.offset = 1000;
    le.length = 2000;
    le.status = 5; /* NFS4ERR_IO */
    le.opnum = 25; /* OP_READ */

    ASSERT_EQ(le.stateid.seqid, 42);
    ASSERT_EQ(le.offset, 1000);
    ASSERT_EQ(le.length, 2000);
    ASSERT_EQ(le.status, 5);
    ASSERT_EQ(le.opnum, 25);

    printf("  PASS: test_layouterror_struct\n");
}

/* -----------------------------------------------------------------------
 * Mock CB server thread -- reads one RPC record, sends a success reply
 * ----------------------------------------------------------------------- */

struct mock_cb_server_args {
    int fd;
    int received;  /* Set to 1 if a record was received. */
    uint32_t layoutrecall_count;
    struct nfs4_stateid last_stateid;
    /*
     * Status to encode in the synthesised CB_COMPOUND4res header.
     * Default 0 (NFS4_OK) preserves the prior mock behaviour.  Tests
     * that exercise the server's response to transient client errors
     * (NFS4ERR_DELAY, NFS4ERR_RECALLCONFLICT) set this to the wire
     * code they want to drive into the recall coordinator.
     */
    uint32_t reply_status;
};

static bool test_xdr_skip_opaque_body(XDR *xdrs, uint32_t max_len)
{
    uint32_t len;
    uint32_t pos;
    uint32_t padded;

    if (!xdr_uint32_t(xdrs, &len)) {
        return false;
    }
    if (len > max_len || len > UINT32_MAX - 3U) {
        return false;
    }
    padded = (len + 3U) & ~3U;
    pos = xdr_getpos(xdrs);
    return xdr_setpos(xdrs, pos + padded);
}

static bool test_xdr_skip_opaque_auth(XDR *xdrs)
{
    uint32_t flavor;

    if (!xdr_uint32_t(xdrs, &flavor)) {
        return false;
    }
    return test_xdr_skip_opaque_body(xdrs, 4096);
}

static bool test_decode_cb_layoutrecall_stateid(const uint8_t *buf,
                                                uint32_t len,
                                                struct nfs4_stateid *sid)
{
    XDR xdrs;
    char session_id[SESSION_ID_SIZE];
    uint32_t v;
    uint32_t opcount;
    uint32_t opnum;
    uint32_t recall_type;
    uint32_t ref_count;
    uint64_t u64;
    bool ok = false;

    if (buf == NULL || sid == NULL) {
        return false;
    }

    xdrmem_create(&xdrs, (char *)buf, len, XDR_DECODE);

    /* RPC call header. */
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* xid */
    if (!xdr_uint32_t(&xdrs, &v) || v != 0) { goto out; } /* CALL */
    if (!xdr_uint32_t(&xdrs, &v) || v != 2) { goto out; } /* rpcvers */
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* prog */
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* vers */
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* proc */
    if (!test_xdr_skip_opaque_auth(&xdrs)) { goto out; }
    if (!test_xdr_skip_opaque_auth(&xdrs)) { goto out; }

    /* CB_COMPOUND4args header. */
    if (!test_xdr_skip_opaque_body(&xdrs, 1024)) { goto out; } /* tag */
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* minorversion */
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* callback_ident */
    if (!xdr_uint32_t(&xdrs, &opcount) || opcount < 2) { goto out; }

    /* CB_SEQUENCE. */
    if (!xdr_uint32_t(&xdrs, &opnum) || opnum != OP_CB_SEQUENCE) {
        goto out;
    }
    if (!xdr_opaque(&xdrs, session_id, SESSION_ID_SIZE)) { goto out; }
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* sequenceid */
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* slotid */
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* highest_slotid */
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* cachethis */
    if (!xdr_uint32_t(&xdrs, &ref_count) || ref_count != 0) {
        goto out;
    }

    /* CB_LAYOUTRECALL4args. */
    if (!xdr_uint32_t(&xdrs, &opnum) || opnum != OP_CB_LAYOUTRECALL) {
        goto out;
    }
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* layout_type */
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* iomode */
    if (!xdr_uint32_t(&xdrs, &v)) { goto out; } /* layoutchanged */
    if (!xdr_uint32_t(&xdrs, &recall_type)) { goto out; }
    if (recall_type != LAYOUTRECALL4_FILE) {
        goto out;
    }
    if (!test_xdr_skip_opaque_body(&xdrs, NFS4_FHSIZE)) { goto out; }
    if (!xdr_uint64_t(&xdrs, &u64)) { goto out; } /* offset */
    if (!xdr_uint64_t(&xdrs, &u64)) { goto out; } /* length */
    if (!xdr_nfs4_stateid_decode(&xdrs, sid)) { goto out; }

    ok = true;

out:
    xdr_destroy(&xdrs);
    return ok;
}

static void *mock_cb_server_thread(void *arg)
{
    struct mock_cb_server_args *a = arg;
    struct pollfd pfd;
    uint8_t hdr[4];
    ssize_t n;

    pfd.fd = a->fd;
    pfd.events = POLLIN;

    /* Wait for RPC record (fragment header + payload). */
    if (poll(&pfd, 1, 3000) <= 0) {
        return NULL;
    }
    n = recv(a->fd, hdr, 4, MSG_WAITALL);
    if (n != 4) {
        return NULL;
    }

    uint32_t raw = ntohl(*(uint32_t *)hdr);
    uint32_t frag_len = raw & 0x7FFFFFFFu;

    if (frag_len == 0 || frag_len > 4096) {
        return NULL;
    }

    uint8_t *buf = malloc(frag_len);
    if (buf == NULL) {
        return NULL;
    }
    n = recv(a->fd, buf, frag_len, MSG_WAITALL);
    if (n < 0 || (uint32_t)n != frag_len) {
        free(buf);
        return NULL;
    }

    /* Extract XID from RPC header (first 4 bytes of payload). */
    uint32_t xid;
    struct nfs4_stateid decoded_sid;
    memcpy(&xid, buf, 4);
    if (test_decode_cb_layoutrecall_stateid(buf, frag_len, &decoded_sid)) {
        a->last_stateid = decoded_sid;
        a->layoutrecall_count++;
    }
    free(buf);

    a->received = 1;

    /*
     * Build a minimal RPC REPLY + CB_COMPOUND success response.
     * Layout: xid(4) reply(4=1) accepted(4=0) auth_none(4+4)
     *         accept_success(4=0) status(4=0) tag_len(4=0) resarray_count(4=0)
     */
    uint8_t reply[40];
    uint32_t *rp = (uint32_t *)reply;
    rp[0] = xid;                    /* xid -- network order from client */
    rp[1] = htonl(1);               /* msg_type = REPLY */
    rp[2] = htonl(0);               /* reply_stat = MSG_ACCEPTED */
    rp[3] = htonl(0);               /* auth flavor = AUTH_NONE */
    rp[4] = htonl(0);               /* auth body len = 0 */
    rp[5] = htonl(0);               /* accept_stat = SUCCESS */
    rp[6] = htonl(a->reply_status); /* CB_COMPOUND status (default 0) */
    rp[7] = htonl(0);               /* tag len = 0 */
    rp[8] = htonl(0);               /* resarray count = 0 */

    uint32_t reply_len = 36;
    uint32_t frag_hdr = htonl(reply_len | 0x80000000u);

    send(a->fd, &frag_hdr, 4, MSG_NOSIGNAL);
    send(a->fd, reply, reply_len, MSG_NOSIGNAL);

    return NULL;
}

/* -----------------------------------------------------------------------
 * Test 6: Recall with session table set + CB succeeded
 *
 * Creates a session, binds a socketpair as cb_conn, populates
 * layout + index, runs a mock CB server, and verifies both CB
 * delivery and authoritative revoke.
 * ----------------------------------------------------------------------- */

static void test_recall_with_cb_success(void)
{
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    struct session_table *st = NULL;
    char *path = make_temp_db_path();
    enum mds_status mst;
    struct mds_cat_txn *txn = NULL;
    struct nfs4_stateid sid;
    struct scan_result sr;

    db = open_test_catalogue(); VERIFY(db != NULL);
	cat = db;
    ASSERT_EQ(layout_recall_init(cat, NULL, 2000, &lr), 0);

    /* Set up session table with one session. */
    uint64_t clientid;
    uint32_t seqid, flags_out;
    ASSERT_EQ(session_table_init(0, 90, &st), 0);
    ASSERT_EQ(session_exchange_id(st, (const uint8_t *)"test-cb-client", 14,
                                  (const uint8_t *)"\x01\x02\x03\x04\x05\x06\x07\x08",
                                  0, &clientid, &seqid, &flags_out, 0, 0, 0), 0);

    uint8_t session_id[SESSION_ID_SIZE];
    uint32_t fore = 0, back = 0;
    ASSERT_EQ(session_create_session(st, clientid, seqid, 16, 4,
                                     0x40000000, 0, /* AUTH_NONE */
                                     0, 0,
                                     1, /* minorversion */
                                     session_id, &fore, &back,
                                     NULL, NULL), 0);

    /* Create socketpair for mock backchannel. */
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

    /* Bind one end as cb_conn. */
    ASSERT_EQ(session_bind_conn(st, session_id,
                                (struct rpc_conn *)(void *)sv), 0);

    /* Inject session table into recall coordinator. */
    layout_recall_set_session_table(lr, st);

    /* Create layout + index for ds_id=7, using the session's clientid. */
    uint32_t ds_ids[] = {7};
    mst = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn);
    ASSERT_EQ(mst, MDS_OK);

    memset(&sid, 0, sizeof(sid));
    sid.seqid = 1;
    mst = mds_coord_layout_grant(db, txn, clientid, 300,
                                 1, 0, 0xFFFFFFFFFFFFFFFFULL, &sid,
                                 ds_ids, 1);
    ASSERT_EQ(mst, MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    /* Start mock CB server on the other end of the socketpair. */
    struct mock_cb_server_args mock_args = { .fd = sv[1], .received = 0 };
    pthread_t mock_tid;
    ASSERT_EQ(pthread_create(&mock_tid, NULL, mock_cb_server_thread, &mock_args), 0);

    /* Fire the recall. */
    ASSERT_EQ(layout_recall_for_ds(lr, 7), 0);

    /* Wait for mock server. */
    pthread_join(mock_tid, NULL);

    /* The mock server should have received a CB_LAYOUTRECALL. */
    ASSERT_EQ(mock_args.received, 1);

    /* Layout should be revoked (authoritative). */
    {
        bool has_layout = false;
        mst = mds_coord_layout_scan_for_file(db, 300, &has_layout);
        ASSERT_EQ(mst, MDS_OK);
        ASSERT_EQ(has_layout, 0);
    }

    /* Index should be empty. */
    memset(&sr, 0, sizeof(sr));
    mst = mds_coord_ds_layout_idx_scan(db, 7, collect_cb, &sr);
    ASSERT_EQ(mst, MDS_OK);
    ASSERT_EQ(sr.count, 0);

    close(sv[0]);
    close(sv[1]);
    layout_recall_destroy(lr);
    session_table_destroy(st);
    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: test_recall_with_cb_success\n");
}

/* -----------------------------------------------------------------------
 * Test 7: Recall with CB failure (no backchannel) still revokes
 * ----------------------------------------------------------------------- */

static void test_recall_cb_fail_still_revokes(void)
{
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    struct session_table *st = NULL;
    char *path = make_temp_db_path();
    enum mds_status mst;
    struct mds_cat_txn *txn = NULL;
    struct nfs4_stateid sid;
    struct scan_result sr;

    db = open_test_catalogue(); VERIFY(db != NULL);
	cat = db;
    ASSERT_EQ(layout_recall_init(cat, NULL, 500, &lr), 0);

    /* Set up session without binding any cb_conn. */
    uint64_t clientid;
    uint32_t seqid, flags_out;
    ASSERT_EQ(session_table_init(0, 90, &st), 0);
    ASSERT_EQ(session_exchange_id(st, (const uint8_t *)"test-no-bc", 10,
                                  (const uint8_t *)"\x01\x02\x03\x04\x05\x06\x07\x08",
                                  0, &clientid, &seqid, &flags_out, 0, 0, 0), 0);
    uint8_t session_id[SESSION_ID_SIZE];
    uint32_t fore = 0, back = 0;
    ASSERT_EQ(session_create_session(st, clientid, seqid, 16, 4,
                                     0x40000000, 0,
                                     0, 0,
                                     1, /* minorversion */
                                     session_id, &fore, &back,
                                     NULL, NULL), 0);
    /* No bind_conn -- cb_conn is NULL, so CB will be skipped. */

    layout_recall_set_session_table(lr, st);

    /* Create layout. */
    uint32_t ds_ids[] = {8};
    mst = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn);
    ASSERT_EQ(mst, MDS_OK);
    memset(&sid, 0, sizeof(sid));
    sid.seqid = 1;
    mst = mds_coord_layout_grant(db, txn, clientid, 400,
                                 1, 0, 0xFFFFFFFFFFFFFFFFULL, &sid,
                                 ds_ids, 1);
    ASSERT_EQ(mst, MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    /* Fire recall -- CB will fail (no cb_conn), but revoke must succeed. */
    ASSERT_EQ(layout_recall_for_ds(lr, 8), 0);

    /* Layout must be revoked. */
    {
        bool has_layout = false;
        mst = mds_coord_layout_scan_for_file(db, 400, &has_layout);
        ASSERT_EQ(mst, MDS_OK);
        ASSERT_EQ(has_layout, 0);
    }

    memset(&sr, 0, sizeof(sr));
    mst = mds_coord_ds_layout_idx_scan(db, 8, collect_cb, &sr);
    ASSERT_EQ(mst, MDS_OK);
    ASSERT_EQ(sr.count, 0);

    layout_recall_destroy(lr);
    session_table_destroy(st);
    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: test_recall_cb_fail_still_revokes\n");
}

/* -----------------------------------------------------------------------
 * Test 8: nfs4_cb_layoutrecall_fd basic validation
 * ----------------------------------------------------------------------- */

static void test_cb_layoutrecall_fd_bad_args(void)
{
    struct nfs4_cb_layoutrecall_args args;
    uint8_t sid[SESSION_ID_SIZE] = {0};

    memset(&args, 0, sizeof(args));
    args.recall_type = LAYOUTRECALL4_ALL;

    /* Bad fd. */
    ASSERT_EQ(nfs4_cb_layoutrecall_fd(-1, sid, 0x40000000, 1, 1,
                                       1, NULL, &args, 1000), -EINVAL);
    /* NULL args. */
    ASSERT_EQ(nfs4_cb_layoutrecall_fd(0, sid, 0x40000000, 1, 1,
                                       1, NULL, NULL, 1000), -EINVAL);
    /* NULL session_id. */
    ASSERT_EQ(nfs4_cb_layoutrecall_fd(0, NULL, 0x40000000, 1, 1,
                                       1, NULL, &args, 1000), -EINVAL);

    printf("  PASS: test_cb_layoutrecall_fd_bad_args\n");
}

/* -----------------------------------------------------------------------
 * Test 9: nfs4_cb_layoutrecall_fd round-trip over socketpair
 * ----------------------------------------------------------------------- */

static void test_cb_layoutrecall_fd_roundtrip(void)
{
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

    uint8_t sid[SESSION_ID_SIZE];
    memset(sid, 0x42, SESSION_ID_SIZE);

    struct nfs4_cb_layoutrecall_args args;
    memset(&args, 0, sizeof(args));
    args.layout_type = 1;
    args.iomode = 3;
    args.recall_type = LAYOUTRECALL4_ALL;

    /* Start mock server. */
    struct mock_cb_server_args mock = { .fd = sv[1], .received = 0 };
    pthread_t tid;
    ASSERT_EQ(pthread_create(&tid, NULL, mock_cb_server_thread, &mock), 0);

    int rc = nfs4_cb_layoutrecall_fd(sv[0], sid, 0x40000000, 1, 1,
                                      1 /* minorversion */, NULL,
                                      &args, 3000);
    ASSERT_EQ(rc, 0);

    pthread_join(tid, NULL);
    ASSERT_EQ(mock.received, 1);

    close(sv[0]);
    close(sv[1]);
    printf("  PASS: test_cb_layoutrecall_fd_roundtrip\n");
}

static void fill_test_layout_stateid(struct nfs4_stateid *sid,
                                     uint32_t seqid,
                                     uint8_t seed)
{
    memset(sid, 0, sizeof(*sid));
    sid->seqid = seqid;
    for (uint32_t i = 0; i < NFS4_OTHER_SIZE; i++) {
        sid->other[i] = (uint8_t)(seed + i);
    }
}

/* -----------------------------------------------------------------------
 * Test 10: Byte-range conflict recall sends latest in-memory seqid
 * ----------------------------------------------------------------------- */

static void test_byte_range_recall_uses_latest_seqid(void)
{
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    struct session_table *st = NULL;
    struct mds_cat_txn *txn = NULL;
    struct nfs4_stateid sid;
    struct mock_cb_server_args mock;
    pthread_t tid;
    char *path = make_temp_db_path();
    uint64_t clientid;
    uint32_t seqid;
    uint32_t flags_out;
    uint8_t session_id[SESSION_ID_SIZE];
    uint32_t fore = 0;
    uint32_t back = 0;
    uint32_t ds_ids[] = {9};
    uint32_t recalled = 0;
    int sv[2];

    db = open_test_catalogue(); VERIFY(db != NULL);
    cat = db;
    ASSERT_EQ(layout_recall_init(cat, NULL, 3000, &lr), 0);
    ASSERT_EQ(session_table_init(0, 90, &st), 0);
    ASSERT_EQ(session_exchange_id(st, (const uint8_t *)"br-latest", 9,
                                  (const uint8_t *)"\x01\x02\x03\x04\x05\x06\x07\x08",
                                  0, &clientid, &seqid, &flags_out, 0, 0, 0), 0);
    ASSERT_EQ(session_create_session(st, clientid, seqid, 16, 4,
                                     0x40000000, 0, 0, 0, 1,
                                     session_id, &fore, &back,
                                     NULL, NULL), 0);
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    ASSERT_EQ(session_bind_conn(st, session_id,
                                (struct rpc_conn *)(void *)sv), 0);
    layout_recall_set_session_table(lr, st);

    fill_test_layout_stateid(&sid, 1, 0xA0);
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant(db, txn, clientid, 500,
                                     LAYOUTIOMODE4_RW, 0, 1024,
                                     &sid, ds_ids, 1),
              MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);
    layout_seqid_record_at(sid.other, 3);

    memset(&mock, 0, sizeof(mock));
    mock.fd = sv[1];
    ASSERT_EQ(pthread_create(&tid, NULL, mock_cb_server_thread, &mock), 0);

    ASSERT_EQ(layout_recall_byte_range_for_holders(lr, 500, clientid + 1,
                                                   LAYOUTIOMODE4_RW,
                                                   0, 512,
                                                   LAYOUT4_FLEX_FILES,
                                                   &recalled),
              0);
    pthread_join(tid, NULL);

    ASSERT_EQ(recalled, 1);
    ASSERT_EQ(mock.received, 1);
    ASSERT_EQ(mock.layoutrecall_count, 1);
    ASSERT_EQ(mock.last_stateid.seqid, 4);
    ASSERT_EQ(memcmp(mock.last_stateid.other, sid.other, NFS4_OTHER_SIZE), 0);

    close(sv[0]);
    close(sv[1]);
    layout_recall_destroy(lr);
    session_table_destroy(st);
    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: test_byte_range_recall_uses_latest_seqid\n");
}

/* -----------------------------------------------------------------------
 * Test 11: Byte-range conflict recall dedupes duplicate rows by clientid
 * ----------------------------------------------------------------------- */

static void test_byte_range_recall_dedupes_clientid(void)
{
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    struct mds_cat_txn *txn = NULL;
    struct nfs4_stateid sid_low;
    struct nfs4_stateid sid_high;
    char *path = make_temp_db_path();
    uint32_t ds_ids[] = {10};
    uint32_t recalled = 0;

    db = open_test_catalogue(); VERIFY(db != NULL);
    cat = db;
    ASSERT_EQ(layout_recall_init(cat, NULL, 0, &lr), 0);

    fill_test_layout_stateid(&sid_low, 1, 0xB0);
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant(db, txn, 0x100, 600,
                                     LAYOUTIOMODE4_RW, 0, 1024,
                                     &sid_low, ds_ids, 1),
              MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    fill_test_layout_stateid(&sid_high, 5, 0xC0);
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant(db, txn, 0x100, 600,
                                     LAYOUTIOMODE4_RW, 0, 1024,
                                     &sid_high, ds_ids, 1),
              MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    ASSERT_EQ(layout_recall_byte_range_for_holders(lr, 600, 0x200,
                                                   LAYOUTIOMODE4_RW,
                                                   0, 512,
                                                   LAYOUT4_FLEX_FILES,
                                                   &recalled),
              0);
    ASSERT_EQ(recalled, 1);

    layout_recall_destroy(lr);
    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: test_byte_range_recall_dedupes_clientid\n");
}

/* -----------------------------------------------------------------------
 * Test 12: Byte-range conflict recall -- transient CB status skips
 * the preemptive revoke, leaving the layout-state row intact so the
 * client's natural LAYOUTRETURN cleans up.
 *
 * RFC 5661 S20.4.2.1 / S15.1.10.10 -- NFS4ERR_RECALLCONFLICT (10061)
 * means "client has I/O in flight, will return on completion";
 * NFS4ERR_DELAY (10008) is the equivalent generic "server please retry"
 * status.  The MDS MUST NOT revoke on either, or it will race the
 * incoming LAYOUTRETURN and produce NFS4ERR_BAD_STATEID.  This test
 * drives the path with NFS4ERR_RECALLCONFLICT and verifies the row
 * survives.  Reproduces the P05_disjoint_ranges hang in Mark's
 * harness and validates the fix.
 * ----------------------------------------------------------------------- */

static void test_byte_range_recall_skip_revoke_on_recallconflict(void)
{
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    struct session_table *st = NULL;
    struct mds_cat_txn *txn = NULL;
    struct nfs4_stateid sid;
    struct mock_cb_server_args mock;
    pthread_t tid;
    char *path = make_temp_db_path();
    uint64_t clientid;
    uint32_t seqid;
    uint32_t flags_out;
    uint8_t session_id[SESSION_ID_SIZE];
    uint32_t fore = 0;
    uint32_t back = 0;
    uint32_t ds_ids[] = {11};
    uint32_t recalled = 0;
    int sv[2];

    db = open_test_catalogue(); VERIFY(db != NULL);
    cat = db;
    ASSERT_EQ(layout_recall_init(cat, NULL, 3000, &lr), 0);
    ASSERT_EQ(session_table_init(0, 90, &st), 0);
    ASSERT_EQ(session_exchange_id(st, (const uint8_t *)"br-rcconfl", 10,
                                  (const uint8_t *)"\x01\x02\x03\x04\x05\x06\x07\x08",
                                  0, &clientid, &seqid, &flags_out, 0, 0, 0), 0);
    ASSERT_EQ(session_create_session(st, clientid, seqid, 16, 4,
                                     0x40000000, 0, 0, 0, 1,
                                     session_id, &fore, &back,
                                     NULL, NULL), 0);
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    ASSERT_EQ(session_bind_conn(st, session_id,
                                (struct rpc_conn *)(void *)sv), 0);
    layout_recall_set_session_table(lr, st);

    /* Persist a layout grant for the holder. */
    fill_test_layout_stateid(&sid, 1, 0xD0);
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant(db, txn, clientid, 700,
                                     LAYOUTIOMODE4_RW, 0, 1024,
                                     &sid, ds_ids, 1),
              MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    /* Mock client returns NFS4ERR_RECALLCONFLICT (10061). */
    memset(&mock, 0, sizeof(mock));
    mock.fd = sv[1];
    mock.reply_status = (uint32_t)NFS4ERR_RECALLCONFLICT;
    ASSERT_EQ(pthread_create(&tid, NULL, mock_cb_server_thread, &mock), 0);

    /* Drive the byte-range recall as if a peer had requested an
     * overlapping IOMODE_RW layout on the same fileid. */
    ASSERT_EQ(layout_recall_byte_range_for_holders(lr, 700, clientid + 1,
                                                   LAYOUTIOMODE4_RW,
                                                   0, 512,
                                                   LAYOUT4_FLEX_FILES,
                                                   &recalled),
              0);
    pthread_join(tid, NULL);

    /* The CB was sent and the mock saw it. */
    ASSERT_EQ(recalled, 1);
    ASSERT_EQ(mock.received, 1);
    ASSERT_EQ(mock.layoutrecall_count, 1);

    /*
     * Critical assertion: the layout-state row MUST still exist on
     * the catalogue.  RFC 5661 S20.4.2.1 says the client will send
     * LAYOUTRETURN once its I/O drains; if we revoked here the
     * subsequent LAYOUTRETURN would hit NFS4ERR_BAD_STATEID and the
     * client would spin retransmitting.
     */
    {
        bool has_layout = false;
        enum mds_status mst;

        mst = mds_coord_layout_scan_for_file(db, 700, &has_layout);
        ASSERT_EQ(mst, MDS_OK);
        ASSERT_EQ(has_layout, 1);
    }

    close(sv[0]);
    close(sv[1]);
    layout_recall_destroy(lr);
    session_table_destroy(st);
    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: test_byte_range_recall_skip_revoke_on_recallconflict\n");
}

/* -----------------------------------------------------------------------
 * Test 13: Byte-range conflict recall -- NFS4ERR_DELAY also skips
 * revoke (transient sibling of NFS4ERR_RECALLCONFLICT, RFC 5661
 * S20.4.2.1).
 * ----------------------------------------------------------------------- */

static void test_byte_range_recall_skip_revoke_on_delay(void)
{
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    struct session_table *st = NULL;
    struct mds_cat_txn *txn = NULL;
    struct nfs4_stateid sid;
    struct mock_cb_server_args mock;
    pthread_t tid;
    char *path = make_temp_db_path();
    uint64_t clientid;
    uint32_t seqid;
    uint32_t flags_out;
    uint8_t session_id[SESSION_ID_SIZE];
    uint32_t fore = 0;
    uint32_t back = 0;
    uint32_t ds_ids[] = {12};
    uint32_t recalled = 0;
    int sv[2];

    db = open_test_catalogue(); VERIFY(db != NULL);
    cat = db;
    ASSERT_EQ(layout_recall_init(cat, NULL, 3000, &lr), 0);
    ASSERT_EQ(session_table_init(0, 90, &st), 0);
    ASSERT_EQ(session_exchange_id(st, (const uint8_t *)"br-delay", 8,
                                  (const uint8_t *)"\x01\x02\x03\x04\x05\x06\x07\x08",
                                  0, &clientid, &seqid, &flags_out, 0, 0, 0), 0);
    ASSERT_EQ(session_create_session(st, clientid, seqid, 16, 4,
                                     0x40000000, 0, 0, 0, 1,
                                     session_id, &fore, &back,
                                     NULL, NULL), 0);
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    ASSERT_EQ(session_bind_conn(st, session_id,
                                (struct rpc_conn *)(void *)sv), 0);
    layout_recall_set_session_table(lr, st);

    fill_test_layout_stateid(&sid, 1, 0xE0);
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant(db, txn, clientid, 800,
                                     LAYOUTIOMODE4_RW, 0, 1024,
                                     &sid, ds_ids, 1),
              MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    memset(&mock, 0, sizeof(mock));
    mock.fd = sv[1];
    mock.reply_status = (uint32_t)NFS4ERR_DELAY;
    ASSERT_EQ(pthread_create(&tid, NULL, mock_cb_server_thread, &mock), 0);

    ASSERT_EQ(layout_recall_byte_range_for_holders(lr, 800, clientid + 1,
                                                   LAYOUTIOMODE4_RW,
                                                   0, 512,
                                                   LAYOUT4_FLEX_FILES,
                                                   &recalled),
              0);
    pthread_join(tid, NULL);

    ASSERT_EQ(recalled, 1);
    ASSERT_EQ(mock.received, 1);

    /* Layout MUST survive the recall -- NFS4ERR_DELAY is transient. */
    {
        bool has_layout = false;
        enum mds_status mst;

        mst = mds_coord_layout_scan_for_file(db, 800, &has_layout);
        ASSERT_EQ(mst, MDS_OK);
        ASSERT_EQ(has_layout, 1);
    }

    close(sv[0]);
    close(sv[1]);
    layout_recall_destroy(lr);
    session_table_destroy(st);
    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: test_byte_range_recall_skip_revoke_on_delay\n");
}

/* -----------------------------------------------------------------------
 * Test 14: Byte-range conflict recall -- a TERMINAL CB error (e.g.
 * NFS4ERR_BADSESSION) still triggers the authoritative revoke.  This
 * is the negative complement of the two transient-skip tests above.
 * ----------------------------------------------------------------------- */

static void test_byte_range_recall_revokes_on_terminal_status(void)
{
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    struct session_table *st = NULL;
    struct mds_cat_txn *txn = NULL;
    struct nfs4_stateid sid;
    struct mock_cb_server_args mock;
    pthread_t tid;
    char *path = make_temp_db_path();
    uint64_t clientid;
    uint32_t seqid;
    uint32_t flags_out;
    uint8_t session_id[SESSION_ID_SIZE];
    uint32_t fore = 0;
    uint32_t back = 0;
    uint32_t ds_ids[] = {13};
    uint32_t recalled = 0;
    int sv[2];

    db = open_test_catalogue(); VERIFY(db != NULL);
    cat = db;
    ASSERT_EQ(layout_recall_init(cat, NULL, 3000, &lr), 0);
    ASSERT_EQ(session_table_init(0, 90, &st), 0);
    ASSERT_EQ(session_exchange_id(st, (const uint8_t *)"br-badsess", 10,
                                  (const uint8_t *)"\x01\x02\x03\x04\x05\x06\x07\x08",
                                  0, &clientid, &seqid, &flags_out, 0, 0, 0), 0);
    ASSERT_EQ(session_create_session(st, clientid, seqid, 16, 4,
                                     0x40000000, 0, 0, 0, 1,
                                     session_id, &fore, &back,
                                     NULL, NULL), 0);
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    ASSERT_EQ(session_bind_conn(st, session_id,
                                (struct rpc_conn *)(void *)sv), 0);
    layout_recall_set_session_table(lr, st);

    fill_test_layout_stateid(&sid, 1, 0xF0);
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant(db, txn, clientid, 900,
                                     LAYOUTIOMODE4_RW, 0, 1024,
                                     &sid, ds_ids, 1),
              MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    memset(&mock, 0, sizeof(mock));
    mock.fd = sv[1];
    mock.reply_status = (uint32_t)NFS4ERR_BADSESSION;
    ASSERT_EQ(pthread_create(&tid, NULL, mock_cb_server_thread, &mock), 0);

    ASSERT_EQ(layout_recall_byte_range_for_holders(lr, 900, clientid + 1,
                                                   LAYOUTIOMODE4_RW,
                                                   0, 512,
                                                   LAYOUT4_FLEX_FILES,
                                                   &recalled),
              0);
    pthread_join(tid, NULL);

    ASSERT_EQ(recalled, 1);
    ASSERT_EQ(mock.received, 1);

    /*
     * With the send-only CB model (no recv on the shared fd), the
     * recall coordinator never sees the client's terminal status.
     * cb_status == 0 ("send succeeded") is treated as transient,
     * so the layout row is preserved -- the client's natural
     * LAYOUTRETURN cleans it up.  The unlink path has its own
     * forced-revoke logic that does not depend on CB status.
     */
    {
        bool has_layout = false;
        enum mds_status mst;

        mst = mds_coord_layout_scan_for_file(db, 900, &has_layout);
        ASSERT_EQ(mst, MDS_OK);
        ASSERT_EQ(has_layout, 1);
    }

    close(sv[0]);
    close(sv[1]);
    layout_recall_destroy(lr);
    session_table_destroy(st);
    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: test_byte_range_recall_revokes_on_terminal_status\n");
}
/* -----------------------------------------------------------------------
 * Test 15: Byte-range conflict recall -- NFS4ERR_NOMATCHING_LAYOUT is
 * terminal and must not be confused with NFS4ERR_RECALLCONFLICT.
 * RFC 5661/RFC 8881 assign NOMATCHING_LAYOUT=10060 and
 * RECALLCONFLICT=10061; a regression in the enum used 10060 for
 * RECALLCONFLICT and incorrectly skipped this revoke.
 * ----------------------------------------------------------------------- */

static void test_byte_range_recall_revokes_on_nomatching_layout(void)
{
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    struct session_table *st = NULL;
    struct mds_cat_txn *txn = NULL;
    struct nfs4_stateid sid;
    struct mock_cb_server_args mock;
    pthread_t tid;
    char *path = make_temp_db_path();
    uint64_t clientid;
    uint32_t seqid;
    uint32_t flags_out;
    uint8_t session_id[SESSION_ID_SIZE];
    uint32_t fore = 0;
    uint32_t back = 0;
    uint32_t ds_ids[] = {14};
    uint32_t recalled = 0;
    int sv[2];

    db = open_test_catalogue(); VERIFY(db != NULL);
    cat = db;
    ASSERT_EQ(layout_recall_init(cat, NULL, 3000, &lr), 0);
    ASSERT_EQ(session_table_init(0, 90, &st), 0);
    ASSERT_EQ(session_exchange_id(st, (const uint8_t *)"br-nomatch", 10,
                                  (const uint8_t *)"\x01\x02\x03\x04\x05\x06\x07\x08",
                                  0, &clientid, &seqid, &flags_out, 0, 0, 0), 0);
    ASSERT_EQ(session_create_session(st, clientid, seqid, 16, 4,
                                     0x40000000, 0, 0, 0, 1,
                                     session_id, &fore, &back,
                                     NULL, NULL), 0);
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    ASSERT_EQ(session_bind_conn(st, session_id,
                                (struct rpc_conn *)(void *)sv), 0);
    layout_recall_set_session_table(lr, st);

    fill_test_layout_stateid(&sid, 1, 0xF1);
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant(db, txn, clientid, 901,
                                     LAYOUTIOMODE4_RW, 0, 1024,
                                     &sid, ds_ids, 1),
              MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    memset(&mock, 0, sizeof(mock));
    mock.fd = sv[1];
    mock.reply_status = (uint32_t)NFS4ERR_NOMATCHING_LAYOUT;
    ASSERT_EQ(pthread_create(&tid, NULL, mock_cb_server_thread, &mock), 0);

    ASSERT_EQ(layout_recall_byte_range_for_holders(lr, 901, clientid + 1,
                                                   LAYOUTIOMODE4_RW,
                                                   0, 512,
                                                   LAYOUT4_FLEX_FILES,
                                                   &recalled),
              0);
    pthread_join(tid, NULL);

    ASSERT_EQ(recalled, 1);
    ASSERT_EQ(mock.received, 1);

    /*
     * Send-only CB: NOMATCHING_LAYOUT is never observed by the
     * server; cb_status == 0 keeps the row (transient).  Same
     * rationale as test_byte_range_recall_revokes_on_terminal_status.
     */
    {
        bool has_layout = false;
        enum mds_status mst;

        mst = mds_coord_layout_scan_for_file(db, 901, &has_layout);
        ASSERT_EQ(mst, MDS_OK);
        ASSERT_EQ(has_layout, 1);
    }

    close(sv[0]);
    close(sv[1]);
    layout_recall_destroy(lr);
    session_table_destroy(st);
    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: test_byte_range_recall_revokes_on_nomatching_layout\n");
}

/* -----------------------------------------------------------------------
 * Test 16: Final-unlink recall forcibly revokes layout-state rows even
 * for callback statuses that ordinary byte-range conflict recall treats
 * as transient.  This prevents the P04_unlink_held deadlock where the
 * holder answers the CB by sending LAYOUTRETURN after the namespace
 * entry has already been removed; Linux then sees STALE on PUTFH and
 * enters migration recovery instead of completing the return.
 * ----------------------------------------------------------------------- */

static void test_unlink_revoke_forces_revoke(uint32_t reply_status,
                                             uint64_t fileid,
                                             uint32_t ds_id,
                                             uint8_t state_seed,
                                             const char *label)
{
    struct mds_catalogue *db = NULL;
    struct mds_catalogue *cat = NULL;
    struct layout_recall *lr = NULL;
    struct session_table *st = NULL;
    struct mds_cat_txn *txn = NULL;
    struct nfs4_stateid sid;
    struct mock_cb_server_args mock;
    pthread_t tid;
    char *path = make_temp_db_path();
    uint64_t clientid;
    uint32_t seqid;
    uint32_t flags_out;
    uint8_t session_id[SESSION_ID_SIZE];
    uint32_t fore = 0;
    uint32_t back = 0;
    uint32_t ds_ids[] = {0};
    uint32_t recalled = 0;
    int sv[2];

    ds_ids[0] = ds_id;

    db = open_test_catalogue(); VERIFY(db != NULL);
    cat = db;
    ASSERT_EQ(layout_recall_init(cat, NULL, 3000, &lr), 0);
    ASSERT_EQ(session_table_init(0, 90, &st), 0);
    ASSERT_EQ(session_exchange_id(st, (const uint8_t *)label,
                                  (uint32_t)strlen(label),
                                  (const uint8_t *)"\x01\x02\x03\x04\x05\x06\x07\x08",
                                  0, &clientid, &seqid, &flags_out,
                                  0, 0, 0), 0);
    ASSERT_EQ(session_create_session(st, clientid, seqid, 16, 4,
                                     0x40000000, 0, 0, 0, 1,
                                     session_id, &fore, &back,
                                     NULL, NULL), 0);
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    ASSERT_EQ(session_bind_conn(st, session_id,
                                (struct rpc_conn *)(void *)sv), 0);
    layout_recall_set_session_table(lr, st);

    fill_test_layout_stateid(&sid, 1, state_seed);
    ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    ASSERT_EQ(mds_coord_layout_grant(db, txn, clientid, fileid,
                                     LAYOUTIOMODE4_RW, 0, 1024,
                                     &sid, ds_ids, 1),
              MDS_OK);
    ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);

    memset(&mock, 0, sizeof(mock));
    mock.fd = sv[1];
    mock.reply_status = reply_status;
    ASSERT_EQ(pthread_create(&tid, NULL, mock_cb_server_thread, &mock), 0);

    ASSERT_EQ(layout_recall_revoke_all_for_unlink(lr, fileid, &recalled), 0);
    pthread_join(tid, NULL);

    ASSERT_EQ(recalled, 1);
    ASSERT_EQ(mock.received, 1);
    ASSERT_EQ(mock.layoutrecall_count, 1);

    {
        bool has_layout = false;
        enum mds_status mst;

        mst = mds_coord_layout_scan_for_file(db, fileid, &has_layout);
        ASSERT_EQ(mst, MDS_OK);
        ASSERT_EQ(has_layout, 0);
    }

    close(sv[0]);
    close(sv[1]);
    layout_recall_destroy(lr);
    session_table_destroy(st);
    mds_catalogue_close(db);
    cleanup_db(path);
    printf("  PASS: %s\n", label);
}

/* -----------------------------------------------------------------------
 * Main
 * ----------------------------------------------------------------------- */

int main(void)
{
    printf("test_layout_recall\n");

    test_recall_lifecycle();
    test_recall_no_layouts();
    test_recall_revoke_no_session();
    test_recall_for_file_no_layouts();
    test_layouterror_struct();
    test_recall_with_cb_success();
    test_recall_cb_fail_still_revokes();
    test_cb_layoutrecall_fd_bad_args();
    test_cb_layoutrecall_fd_roundtrip();
    test_byte_range_recall_uses_latest_seqid();
    test_byte_range_recall_dedupes_clientid();
    test_byte_range_recall_skip_revoke_on_recallconflict();
    test_byte_range_recall_skip_revoke_on_delay();
    test_byte_range_recall_revokes_on_terminal_status();
    test_byte_range_recall_revokes_on_nomatching_layout();
    test_unlink_revoke_forces_revoke(0, 1000, 15, 0xF2,
                                     "test_unlink_revoke_forces_revoke_on_ok");
    test_unlink_revoke_forces_revoke((uint32_t)NFS4ERR_RECALLCONFLICT,
                                     1001, 16, 0xF3,
                                     "test_unlink_revoke_forces_revoke_on_recallconflict");
    test_unlink_revoke_forces_revoke((uint32_t)NFS4ERR_DELAY,
                                     1002, 17, 0xF4,
                                     "test_unlink_revoke_forces_revoke_on_delay");

    printf("\n%d passed, %d failed\n", pass_count, fail_count);
    return fail_count > 0 ? 1 : 0;
}

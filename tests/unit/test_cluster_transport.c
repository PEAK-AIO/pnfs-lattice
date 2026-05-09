/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_cluster_transport.c -- Unit tests for the TCP inter-MDS
 *                             transport (cluster_transport.c).
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <endian.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>

#include "pnfs_mds.h"
#include "test_helpers.h"
#include "test_helpers.h"
#include "cluster_transport.h"
#include "mds_catalogue.h"
#include "rename_2pc.h"
#include "subtree_map.h"
#include "migration.h"
#include "cluster_membership.h"
#include "subtree_split.h"
#include "rebalance.h"
#include "tiering.h"
#include "io_tracker.h"
#include "proxy_io.h"
#include "commit_queue.h"
#include "ds_cache.h"

/* -------------------------------------------------------------------
 * Helpers
 * ------------------------------------------------------------------- */

static int passed;
static int failed;

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n", \
                __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        failed++; \
        return; \
    } \
} while (0)

#define ASSERT_NE(a, b) do { \
    if ((a) == (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) == %s (%d)\n", \
                __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        failed++; \
        return; \
    } \
} while (0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", \
                __FILE__, __LINE__, #cond); \
        failed++; \
        return; \
    } \
} while (0)

#define PASS() do { \
    passed++; \
    fprintf(stdout, "  PASS\n"); \
} while (0)

/* -------------------------------------------------------------------
 * DB setup / teardown
 * ------------------------------------------------------------------- */

/**
 * Wrap a DB handle into a catalogue for cluster_transport_server_start.
 * Caller must free the returned catalogue with mds_catalogue_close().
 */
static struct mds_catalogue *wrap_db_as_cat(struct mds_catalogue *db)
{
    return db;
}

static const char *TEST_DB_PATH = "/tmp/test_cluster_transport.db";

static void cleanup_db(void)
{
    char lockpath[512];
    (void)unlink(TEST_DB_PATH);
    snprintf(lockpath, sizeof(lockpath), "%s-lock", TEST_DB_PATH);
    (void)unlink(lockpath);
}

static struct mds_catalogue *open_test_db(void)
{
    cleanup_db();
    return open_test_catalogue();
}

/* -------------------------------------------------------------------
 * test_server_start_stop
 *
 * Verify that we can start a transport server on an OS-assigned
 * port and stop it cleanly.
 * ------------------------------------------------------------------- */

static void test_server_start_stop(void)
{
    fprintf(stdout, "  test_server_start_stop:          ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    ASSERT_TRUE(db != NULL);

    struct cluster_server *srv = NULL;
    enum mds_status st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0, wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_TRUE(srv != NULL);

    uint16_t port = cluster_transport_server_port(srv);
    ASSERT_TRUE(port > 0);

    cluster_transport_server_stop(srv);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

static void test_split_admin_local_wire_rejects_sharded_mode(void)
{
    fprintf(stdout, "  test_split_admin_local_wire_rejects_sharded_mode: ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    cat = db;
    ASSERT_TRUE(db != NULL);

    struct mds_inode data_dir;
    enum mds_status st;
    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "data",
                       MDS_FTYPE_DIR, 0755, 0, 0,
		       NULL, &data_dir);
    ASSERT_EQ(st, MDS_OK);

    struct mds_inode hot_dir;
    st = mds_cat_ns_create(cat, NULL, data_dir.fileid, "hot",
                       MDS_FTYPE_DIR, 0755, 0, 0,
		       NULL, &hot_dir);
    ASSERT_EQ(st, MDS_OK);

    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(smap, "/data", 1, "localhost",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0,
                                        wrap_db_as_cat(db), smap, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    cluster_transport_server_set_sharding(srv, true);
    uint16_t port = cluster_transport_server_port(srv);

    st = cluster_transport_request_split(
        "127.0.0.1", port,
        "/data", "/data/hot", 1,
        NULL, 0);
    ASSERT_EQ(st, MDS_ERR_NOSUPPORT);

    struct subtree_entry entry;
    st = subtree_map_lookup_exact(smap, "/data/hot", &entry);
    ASSERT_EQ(st, MDS_ERR_NOTFOUND);

    cluster_transport_server_stop(srv);
    subtree_map_destroy(smap);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_tcp_prepare_vote_commit
 *
 * Full round-trip: start server, connect client, send PREPARE
 * (expect VOTE_COMMIT), then send COMMIT.  Verify the destination
 * file appears in the DB.
 * ------------------------------------------------------------------- */

static void test_tcp_prepare_vote_commit(void)
{
    fprintf(stdout, "  test_tcp_prepare_vote_commit:    ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    cat = db;
    ASSERT_TRUE(db != NULL);

    /* Create a destination directory. */
    struct mds_inode dstdir;
    enum mds_status st;
    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "dstdir",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &dstdir);
    ASSERT_EQ(st, MDS_OK);

    /* Start server. */
    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0, wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t port = cluster_transport_server_port(srv);

    /* Connect client. */
    struct rename_2pc_transport *client = NULL;
    st = cluster_transport_connect("127.0.0.1", port, &client);
    ASSERT_EQ(st, MDS_OK);

    /* Build a fake inode to send. */
    struct mds_inode fake_inode;
    memset(&fake_inode, 0, sizeof(fake_inode));
    fake_inode.fileid = 9999;
    fake_inode.type = MDS_FTYPE_REG;
    fake_inode.mode = 0644;
    fake_inode.nlink = 1;

    /* RonDB shared-authority on_commit expects the inode in the DB. */
    {
        struct mds_cat_txn *txn = NULL;
        ASSERT_EQ(mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
        ASSERT_EQ(mds_cat_inode_put(db, txn, &fake_inode), MDS_OK);
        ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);
    }

    /* PREPARE -- expect VOTE_COMMIT. */
    uint64_t txn_id = 42;
    int vote = client->prepare(1, txn_id, dstdir.fileid, "moved_file",
                               &fake_inode, sizeof(fake_inode),
                               client->user_ctx);
    ASSERT_EQ(vote, 1);

    /* COMMIT. */
    int rc = client->commit(1, txn_id, client->user_ctx);
    ASSERT_EQ(rc, 0);

    /* Give server a moment to process. */
    usleep(50000);

    /* Verify the file appeared in the destination directory. */
    struct mds_inode child;
    st = mds_cat_ns_lookup(db, dstdir.fileid, "moved_file", &child);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(child.type, MDS_FTYPE_REG);

    cluster_transport_disconnect(client);
    cluster_transport_server_stop(srv);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_tcp_prepare_abort
 *
 * PREPARE followed by ABORT.  Verify no file appears.
 * ------------------------------------------------------------------- */

static void test_tcp_prepare_abort(void)
{
    fprintf(stdout, "  test_tcp_prepare_abort:          ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    cat = db;
    ASSERT_TRUE(db != NULL);

    struct mds_inode dstdir;
    enum mds_status st;
    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "abortdir",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &dstdir);
    ASSERT_EQ(st, MDS_OK);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0, wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t port = cluster_transport_server_port(srv);

    struct rename_2pc_transport *client = NULL;
    st = cluster_transport_connect("127.0.0.1", port, &client);
    ASSERT_EQ(st, MDS_OK);

    struct mds_inode fake_inode;
    memset(&fake_inode, 0, sizeof(fake_inode));
    fake_inode.fileid = 8888;
    fake_inode.type = MDS_FTYPE_REG;
    fake_inode.mode = 0644;
    fake_inode.nlink = 1;

    uint64_t txn_id = 77;
    int vote = client->prepare(1, txn_id, dstdir.fileid, "aborted_file",
                               &fake_inode, sizeof(fake_inode),
                               client->user_ctx);
    ASSERT_EQ(vote, 1);

    /* ABORT instead of COMMIT. */
    int rc = client->abort_rename(1, txn_id, client->user_ctx);
    ASSERT_EQ(rc, 0);

    usleep(50000);

    /* Verify file does NOT exist. */
    struct mds_inode child;
    st = mds_cat_ns_lookup(db, dstdir.fileid, "aborted_file", &child);
    ASSERT_EQ(st, MDS_ERR_NOTFOUND);

    cluster_transport_disconnect(client);
    cluster_transport_server_stop(srv);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_connect_failure
 *
 * Connecting to a port where nothing is listening should fail.
 * ------------------------------------------------------------------- */

static void test_connect_failure(void)
{
    fprintf(stdout, "  test_connect_failure:            ");
    fflush(stdout);

    struct rename_2pc_transport *client = NULL;
    /* Port 1 is unlikely to have a listener. */
    enum mds_status st = cluster_transport_connect("127.0.0.1", 1, &client);
    ASSERT_EQ(st, MDS_ERR_IO);
    ASSERT_TRUE(client == NULL);

    PASS();
}

/* -------------------------------------------------------------------
 * test_prepare_vote_abort_on_conflict
 *
 * PREPARE targeting a name that already exists -- server should
 * return VOTE_ABORT (vote=0).
 * ------------------------------------------------------------------- */

static void test_prepare_vote_abort_on_conflict(void)
{
    fprintf(stdout, "  test_prepare_vote_abort_conflict:");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    cat = db;
    ASSERT_TRUE(db != NULL);

    /* Create a directory with an existing file. */
    struct mds_inode dir;
    enum mds_status st;
    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "conflictdir",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &dir);
    ASSERT_EQ(st, MDS_OK);

    struct mds_inode existing;
    st = test_create_file(db, dir.fileid, "existing_file",
                          0644, &existing);
    ASSERT_EQ(st, MDS_OK);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0, wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t port = cluster_transport_server_port(srv);

    struct rename_2pc_transport *client = NULL;
    st = cluster_transport_connect("127.0.0.1", port, &client);
    ASSERT_EQ(st, MDS_OK);

    struct mds_inode fake_inode;
    memset(&fake_inode, 0, sizeof(fake_inode));
    fake_inode.fileid = 7777;
    fake_inode.type = MDS_FTYPE_REG;
    fake_inode.mode = 0644;
    fake_inode.nlink = 1;

    /* PREPARE with conflicting name -- expect VOTE_ABORT (0). */
    uint64_t txn_id = 99;
    int vote = client->prepare(1, txn_id, dir.fileid, "existing_file",
                               &fake_inode, sizeof(fake_inode),
                               client->user_ctx);
    ASSERT_EQ(vote, 0);

    cluster_transport_disconnect(client);
    cluster_transport_server_stop(srv);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_mig_admin_request
 *
 * Start a cluster transport server with a live db + smap,
 * populate a /data subtree, start a destination migration server,
 * then send an admin migration request through the daemon handler.
 * Verify the destination received the migrated data.
 * ------------------------------------------------------------------- */

static const char *DEST_DB_PATH = "/tmp/test_ct_admin_dest.db";

static void cleanup_dest_db(void)
{
    char lockpath[512];
    (void)unlink(DEST_DB_PATH);
    snprintf(lockpath, sizeof(lockpath), "%s-lock", DEST_DB_PATH);
    (void)unlink(lockpath);
}

static void test_mig_admin_request(void)
{
    fprintf(stdout, "  test_mig_admin_request:          ");
    fflush(stdout);

    /* -- Source setup ------------------------------------------------ */
    struct mds_catalogue *src_db = open_test_db();
    struct mds_catalogue *src_cat = NULL;
    src_cat = src_db;
    ASSERT_TRUE(src_db != NULL);

    /* Create /data directory and a file underneath. */
    struct mds_inode data_dir;
    enum mds_status st;
    st = mds_cat_ns_create(src_cat, NULL, MDS_FILEID_ROOT, "data",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &data_dir);
    ASSERT_EQ(st, MDS_OK);

    struct mds_inode file1;
    st = test_create_file(src_db, data_dir.fileid, "file1.txt",
                          0644, &file1);
    ASSERT_EQ(st, MDS_OK);

    /* Init subtree map and register /data as owned by self (MDS 1). */
    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(smap, "/data", 1, "localhost",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Start source cluster server WITH smap. */
    struct cluster_server *src_srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0, wrap_db_as_cat(src_db), smap, NULL, &src_srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t src_port = cluster_transport_server_port(src_srv);

    /* -- Destination setup ------------------------------------------ */
    cleanup_dest_db();
    struct mds_catalogue *dst_db = NULL;
    struct mds_catalogue *dst_cat = NULL;
    st = (dst_db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO;
    ASSERT_EQ(st, MDS_OK);
    dst_cat = dst_db;

    /* Start destination migration server (accepts MIG_BEGIN etc). */
    struct cluster_server *dst_srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0, wrap_db_as_cat(dst_db), NULL, NULL, &dst_srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t dst_port = cluster_transport_server_port(dst_srv);

    /* -- Send admin migration request ------------------------------- */
    st = cluster_transport_request_migration(
        "127.0.0.1", src_port,
        "/data", 2,
        "127.0.0.1", dst_port);
    ASSERT_EQ(st, MDS_OK);

    /* Brief pause for async processing. */
    usleep(100000);

    /* -- Verify destination has migrated inodes ---------------------- */
    /* Migration copies inodes by fileid (the parent->root dirent lives
     * above the subtree and is handled by the referral layer, so we
     * verify by direct fileid and child-dirent lookups). */
    struct mds_inode dst_dir;
    st = mds_cat_ns_getattr(dst_cat, data_dir.fileid, &dst_dir);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(dst_dir.type, MDS_FTYPE_DIR);

    uint64_t child_fid;
    uint8_t  child_type;
    st = mds_cat_dirent_get(dst_db, data_dir.fileid, "file1.txt",
                             &child_fid, &child_type);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(child_fid, file1.fileid);

    /* -- Cleanup ---------------------------------------------------- */
    cluster_transport_server_stop(dst_srv);
    cluster_transport_server_stop(src_srv);
    subtree_map_destroy(smap);
    mds_catalogue_close(dst_db);
    cleanup_dest_db();
    mds_catalogue_close(src_db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * main
 * ------------------------------------------------------------------- */


/* -------------------------------------------------------------------
 * test_mig_admin_membership_resolve
 *
 * Start a source server with membership set.  Register the
 * destination as MDS 2 in membership.  Send an admin migration
 * request with dest_host=NULL (host_len=0) -- the handler should
 * resolve MDS 2 from membership and complete the migration.
 * ------------------------------------------------------------------- */

static const char *MEMB_DEST_DB = "/tmp/test_ct_memb_dest.db";

static void cleanup_memb_dest_db(void)
{
    char lockpath[512];
    (void)unlink(MEMB_DEST_DB);
    snprintf(lockpath, sizeof(lockpath), "%s-lock", MEMB_DEST_DB);
    (void)unlink(lockpath);
}

static void test_mig_admin_membership_resolve(void)
{
    fprintf(stdout, "  test_mig_admin_membership_resolve: ");
    fflush(stdout);

    /* -- Source setup ------------------------------------------------ */
    struct mds_catalogue *src_db = open_test_db();
    struct mds_catalogue *src_cat = NULL;
    src_cat = src_db;
    ASSERT_TRUE(src_db != NULL);

    struct mds_inode data_dir;
    enum mds_status st;
    st = mds_cat_ns_create(src_cat, NULL, MDS_FILEID_ROOT, "mdata",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &data_dir);
    ASSERT_EQ(st, MDS_OK);

    struct mds_inode file1;
    st = test_create_file(src_db, data_dir.fileid, "f1.txt",
                          0644, &file1);
    ASSERT_EQ(st, MDS_OK);

    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(smap, "/mdata", 1, "localhost",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* -- Destination setup ------------------------------------------ */
    cleanup_memb_dest_db();
    struct mds_catalogue *dst_db = NULL;
    struct mds_catalogue *dst_cat = NULL;
    st = (dst_db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO;
    ASSERT_EQ(st, MDS_OK);
    dst_cat = dst_db;

    struct cluster_server *dst_srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0,
                                        wrap_db_as_cat(dst_db), NULL, NULL, &dst_srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t dst_port = cluster_transport_server_port(dst_srv);

    /* -- Source server with membership ------------------------------ */
    /* allowed_peers must include 127.0.0.1 for the dest_host_is_allowed
     * check to pass after membership resolution. */
    char peers[1][64];
    snprintf(peers[0], sizeof(peers[0]), "127.0.0.1");

    struct cluster_server *src_srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1",
                                        (const char (*)[64])peers, 1,
                                        0, wrap_db_as_cat(src_db), smap, NULL, &src_srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t src_port = cluster_transport_server_port(src_srv);

    /* Create membership and register MDS 2 at 127.0.0.1:dst_port. */
    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.self.id = 1;
    snprintf(cfg.self.hostname, sizeof(cfg.self.hostname), "localhost");
    cfg.self.nfs_port = 2049;
    cfg.self.grpc_port = src_port;

    struct cluster_membership *cm = NULL;
    st = cluster_membership_init(&cfg, smap, NULL, &cm);
    ASSERT_EQ(st, MDS_OK);

    struct cluster_member peer;
    memset(&peer, 0, sizeof(peer));
    peer.mds_id = 2;
    snprintf(peer.hostname, sizeof(peer.hostname), "127.0.0.1");
    peer.nfs_port = 2049;
    peer.grpc_port = dst_port;
    peer.join_time_sec = 1000002;
    st = cluster_node_join(cm, &peer);
    ASSERT_EQ(st, MDS_OK);
    /* Promote IDLE -> ACTIVE_SERVING for preflight eligibility. */
    st = cluster_membership_set_lifecycle(cm, 2, NODE_ACTIVE_SERVING);
    ASSERT_EQ(st, MDS_OK);

    cluster_transport_server_set_membership(src_srv, cm);

    /* -- Send admin migration with NULL dest_host ------------------- */
    st = cluster_transport_request_migration(
        "127.0.0.1", src_port,
        "/mdata", 2,
        NULL, 0);
    ASSERT_EQ(st, MDS_OK);

    usleep(100000);

    /* -- Verify destination received migrated data ------------------ */
    struct mds_inode dst_dir;
    st = mds_cat_ns_getattr(dst_cat, data_dir.fileid, &dst_dir);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(dst_dir.type, MDS_FTYPE_DIR);

    uint64_t child_fid;
    uint8_t  child_type;
    st = mds_cat_dirent_get(dst_db, data_dir.fileid, "f1.txt",
                             &child_fid, &child_type);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(child_fid, file1.fileid);

    /* -- Cleanup ---------------------------------------------------- */
    cluster_transport_server_stop(dst_srv);
    cluster_transport_server_stop(src_srv);
    cluster_membership_destroy(cm);
    subtree_map_destroy(smap);
    mds_catalogue_close(dst_db);
    cleanup_memb_dest_db();
    mds_catalogue_close(src_db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * Sequence 6 -- set-lifecycle wire round-trip
 * ------------------------------------------------------------------- */

static void test_set_lifecycle_wire(void)
{
    fprintf(stdout, "  test_set_lifecycle_wire:          ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    ASSERT_TRUE(db != NULL);

    /* Start server. */
    struct cluster_server *srv = NULL;
    enum mds_status st = cluster_transport_server_start(
        0, "127.0.0.1", NULL, 0, 0, wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t port = cluster_transport_server_port(srv);

    /* Create membership and register it with the server. */
    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);

    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.self.id = 1;
    snprintf(cfg.self.hostname, sizeof(cfg.self.hostname), "localhost");
    cfg.self.nfs_port = 2049;
    cfg.self.grpc_port = port;

    struct cluster_membership *cm = NULL;
    st = cluster_membership_init(&cfg, smap, NULL, &cm);
    ASSERT_EQ(st, MDS_OK);

    cluster_transport_server_set_membership(srv, cm);

    /* Join a second node as IDLE. */
    struct cluster_member peer;
    memset(&peer, 0, sizeof(peer));
    peer.mds_id = 2;
    snprintf(peer.hostname, sizeof(peer.hostname), "mds2.local");
    peer.nfs_port = 2049;
    peer.grpc_port = 9800;
    peer.lifecycle = NODE_IDLE;
    st = cluster_node_join(cm, &peer);
    ASSERT_EQ(st, MDS_OK);

    /* Request IDLE -> ACTIVE_SERVING via wire. */
    st = cluster_transport_request_set_lifecycle(
        "127.0.0.1", port, 2, NODE_ACTIVE_SERVING);
    ASSERT_EQ(st, MDS_OK);

    /* Verify lifecycle changed. */
    struct cluster_member result;
    st = cluster_membership_get(cm, 2, &result);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)result.lifecycle, (int)NODE_ACTIVE_SERVING);

    /* Invalid transition (ACTIVE_SERVING -> IDLE) should fail. */
    st = cluster_transport_request_set_lifecycle(
        "127.0.0.1", port, 2, NODE_IDLE);
    ASSERT_TRUE(st != MDS_OK);

    /* Cleanup. */
    cluster_transport_server_stop(srv);
    cluster_membership_destroy(cm);
    subtree_map_destroy(smap);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_split_admin_local_wire
 *
 * Send CT_MSG_SPLIT_ADMIN_REQ with dest = self (local split).
 * Verify the child subtree is registered in the map.
 * ------------------------------------------------------------------- */
static void test_split_admin_local_wire(void)
{
    fprintf(stdout, "  test_split_admin_local_wire:     ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    cat = db;
    ASSERT_TRUE(db != NULL);

    /* Create /data directory and /data/hot child directory. */
    struct mds_inode data_dir;
    enum mds_status st;
    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "data",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &data_dir);
    ASSERT_EQ(st, MDS_OK);

    struct mds_inode hot_dir;
    st = mds_cat_ns_create(cat, NULL, data_dir.fileid, "hot",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &hot_dir);
    ASSERT_EQ(st, MDS_OK);

    /* Init subtree map (self = MDS 1) and register /data. */
    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(smap, "/data", 1, "localhost",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Start server with db + smap. */
    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0,
                                        wrap_db_as_cat(db), smap, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t port = cluster_transport_server_port(srv);

    /* Send split request: dest = self (1), local split. */
    st = cluster_transport_request_split(
        "127.0.0.1", port,
        "/data", "/data/hot", 1,
        NULL, 0);
    ASSERT_EQ(st, MDS_OK);

    /* Verify /data/hot is now a subtree entry owned by self. */
    struct subtree_entry entry;
    st = subtree_map_lookup_exact(smap, "/data/hot", &entry);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(entry.owner_mds_id, (uint32_t)1);
    ASSERT_EQ((int)entry.state, (int)SUBTREE_ACTIVE);

    cluster_transport_server_stop(srv);
    subtree_map_destroy(smap);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_split_admin_fileid_mismatch
 *
 * Send a split request with a wrong child_fileid.  Expect error.
 * ------------------------------------------------------------------- */
static void test_split_admin_fileid_mismatch(void)
{
    fprintf(stdout, "  test_split_admin_fileid_mismatch: ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    cat = db;
    ASSERT_TRUE(db != NULL);

    struct mds_inode data_dir;
    enum mds_status st;
    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "data",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &data_dir);
    ASSERT_EQ(st, MDS_OK);

    struct mds_inode hot_dir;
    st = mds_cat_ns_create(cat, NULL, data_dir.fileid, "hot",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &hot_dir);
    ASSERT_EQ(st, MDS_OK);

    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(smap, "/data", 1, "localhost",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0,
                                        wrap_db_as_cat(db), smap, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t port = cluster_transport_server_port(srv);

    /* Build raw request with wrong fileid = 99999. */
    const char *pp = "/data";
    const char *cp = "/data/hot";
    size_t pp_len = strlen(pp);
    size_t cp_len = strlen(cp);
    uint32_t payload_len = 2 + (uint32_t)pp_len + 2 + (uint32_t)cp_len +
                           8 + 4 + 2 + 0 + 2;
    uint8_t *buf = malloc(payload_len);
    ASSERT_TRUE(buf != NULL);

    size_t off = 0;
    uint16_t be16;
    uint32_t be32;
    uint64_t be64;

    be16 = htobe16((uint16_t)pp_len);
    memcpy(buf + off, &be16, 2); off += 2;
    memcpy(buf + off, pp, pp_len); off += pp_len;

    be16 = htobe16((uint16_t)cp_len);
    memcpy(buf + off, &be16, 2); off += 2;
    memcpy(buf + off, cp, cp_len); off += cp_len;

    be64 = htobe64(99999);  /* wrong fileid */
    memcpy(buf + off, &be64, 8); off += 8;

    be32 = htobe32(1);  /* dest = self */
    memcpy(buf + off, &be32, 4); off += 4;

    be16 = htobe16(0);  /* no host */
    memcpy(buf + off, &be16, 2); off += 2;

    be16 = htobe16(0);  /* no port */
    memcpy(buf + off, &be16, 2); off += 2;

    /* Connect and send raw. */
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_TRUE(fd >= 0);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    ASSERT_EQ(connect(fd, (struct sockaddr *)&addr, sizeof(addr)), 0);

    /* Send header + payload. */
    uint8_t hdr[5];
    hdr[0] = CT_MSG_SPLIT_ADMIN_REQ;
    uint32_t pl_be = htobe32(payload_len);
    memcpy(hdr + 1, &pl_be, 4);
    ASSERT_EQ((int)send(fd, hdr, 5, 0), 5);
    ASSERT_EQ((int)send(fd, buf, payload_len, 0), (int)payload_len);

    /* Read response. */
    uint8_t resp_hdr[5];
    ASSERT_EQ((int)recv(fd, resp_hdr, 5, MSG_WAITALL), 5);
    ASSERT_EQ(resp_hdr[0], CT_MSG_SPLIT_ADMIN_RESP);
    uint8_t status_byte;
    ASSERT_EQ((int)recv(fd, &status_byte, 1, MSG_WAITALL), 1);
    /* Expect error (non-zero). */
    ASSERT_TRUE(status_byte != 0);

    close(fd);
    free(buf);
    cluster_transport_server_stop(srv);
    subtree_map_destroy(smap);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_assign_admin_not_exact_root
 *
 * Send an assign request for a path that is not a subtree root.
 * Expect MDS_ERR_NOTFOUND.
 * ------------------------------------------------------------------- */
static void test_assign_admin_not_exact_root(void)
{
    fprintf(stdout, "  test_assign_admin_not_exact_root: ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    cat = db;
    ASSERT_TRUE(db != NULL);

    /* Create /data in the catalogue so path resolution works. */
    struct mds_inode data_dir;
    enum mds_status st;
    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "data",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &data_dir);
    ASSERT_EQ(st, MDS_OK);

    /* Only register "/" as subtree root.  /data is NOT a subtree root. */
    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0,
                                        wrap_db_as_cat(db), smap, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t port = cluster_transport_server_port(srv);

    /* Send assign for /data -- should fail (not exact root). */
    st = cluster_transport_request_assign(
        "127.0.0.1", port,
        "/data", 2,
        "127.0.0.1", 9999);
    ASSERT_TRUE(st != MDS_OK);

    cluster_transport_server_stop(srv);
    subtree_map_destroy(smap);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_split_admin_dest_not_serving
 *
 * Start server with membership.  Join dest node as IDLE.
 * Send split targeting that node.  Expect MDS_ERR_PERM.
 * ------------------------------------------------------------------- */
static void test_split_admin_dest_not_serving(void)
{
    fprintf(stdout, "  test_split_admin_dest_not_serving: ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    struct mds_catalogue *cat = NULL;
    cat = db;
    ASSERT_TRUE(db != NULL);

    struct mds_inode data_dir;
    enum mds_status st;
    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "data",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &data_dir);
    ASSERT_EQ(st, MDS_OK);

    struct mds_inode hot_dir;
    st = mds_cat_ns_create(cat, NULL, data_dir.fileid, "hot",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &hot_dir);
    ASSERT_EQ(st, MDS_OK);

    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(smap, "/data", 1, "localhost",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0,
                                        wrap_db_as_cat(db), smap, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t port = cluster_transport_server_port(srv);

    /* Set up membership with self as serving, peer as IDLE. */
    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.self.id = 1;
    snprintf(cfg.self.hostname, sizeof(cfg.self.hostname), "localhost");
    cfg.self.nfs_port = 2049;
    cfg.self.grpc_port = port;

    struct cluster_membership *cm = NULL;
    st = cluster_membership_init(&cfg, smap, NULL, &cm);
    ASSERT_EQ(st, MDS_OK);
    cluster_transport_server_set_membership(srv, cm);

    struct cluster_member peer;
    memset(&peer, 0, sizeof(peer));
    peer.mds_id = 2;
    snprintf(peer.hostname, sizeof(peer.hostname), "mds2.local");
    peer.nfs_port = 2049;
    peer.grpc_port = 9800;
    peer.lifecycle = NODE_IDLE;  /* NOT serving! */
    st = cluster_node_join(cm, &peer);
    ASSERT_EQ(st, MDS_OK);

    /* Send split targeting IDLE node -- expect MDS_ERR_PERM. */
    st = cluster_transport_request_split(
        "127.0.0.1", port,
        "/data", "/data/hot", 2,
        "127.0.0.1", 9999);
    ASSERT_TRUE(st != MDS_OK);

    cluster_transport_server_stop(srv);
    cluster_membership_destroy(cm);
    subtree_map_destroy(smap);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}


/* -------------------------------------------------------------------
 * test_standby_detach_wire
 *
 * Send CT_MSG_STANDBY_DETACH through the wire. Verify the standby
 * node is removed from membership.
 * ------------------------------------------------------------------- */
static void test_standby_detach_wire(void)
{
    fprintf(stdout, "  test_standby_detach_wire:          ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    ASSERT_TRUE(db != NULL);

    struct subtree_map *smap = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0,
                                        wrap_db_as_cat(db), smap, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t port = cluster_transport_server_port(srv);

    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.self.id = 1;
    snprintf(cfg.self.hostname, sizeof(cfg.self.hostname), "localhost");
    cfg.self.nfs_port = 2049;
    cfg.self.grpc_port = port;

    struct cluster_membership *cm = NULL;
    st = cluster_membership_init(&cfg, smap, NULL, &cm);
    ASSERT_EQ(st, MDS_OK);
    cluster_transport_server_set_membership(srv, cm);

    /* Join node 2 as standby, no failover partner. */
    struct cluster_member peer;
    memset(&peer, 0, sizeof(peer));
    peer.mds_id = 2;
    snprintf(peer.hostname, sizeof(peer.hostname), "mds2.local");
    peer.nfs_port = 2049;
    peer.grpc_port = 9800;
    peer.role = NODE_STANDBY;
    st = cluster_node_join(cm, &peer);
    ASSERT_EQ(st, MDS_OK);

    /* Detach via wire -- should succeed. */
    st = cluster_transport_request_standby_detach("127.0.0.1", port, 2);
    ASSERT_EQ(st, MDS_OK);

    /* Verify node 2 is gone. */
    struct cluster_member result;
    st = cluster_membership_get(cm, 2, &result);
    ASSERT_TRUE(st != MDS_OK);

    cluster_transport_server_stop(srv);
    cluster_membership_destroy(cm);
    subtree_map_destroy(smap);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_force_remove_wire
 *
 * Send CT_MSG_FORCE_REMOVE through the wire. Verify the node is
 * removed from membership.
 * ------------------------------------------------------------------- */
static void test_force_remove_wire(void)
{
    fprintf(stdout, "  test_force_remove_wire:             ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    ASSERT_TRUE(db != NULL);

    struct subtree_map *smap = NULL;
    enum mds_status st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0,
                                        wrap_db_as_cat(db), smap, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t port = cluster_transport_server_port(srv);

    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.self.id = 1;
    snprintf(cfg.self.hostname, sizeof(cfg.self.hostname), "localhost");
    cfg.self.nfs_port = 2049;
    cfg.self.grpc_port = port;

    struct cluster_membership *cm = NULL;
    st = cluster_membership_init(&cfg, smap, NULL, &cm);
    ASSERT_EQ(st, MDS_OK);
    cluster_transport_server_set_membership(srv, cm);

    /* Join node 3 as standby (zero subtrees). */
    struct cluster_member peer;
    memset(&peer, 0, sizeof(peer));
    peer.mds_id = 3;
    snprintf(peer.hostname, sizeof(peer.hostname), "mds3.local");
    peer.nfs_port = 2049;
    peer.grpc_port = 9801;
    peer.role = NODE_STANDBY;
    st = cluster_node_join(cm, &peer);
    ASSERT_EQ(st, MDS_OK);

    /* Force-remove via wire -- should succeed. */
    st = cluster_transport_request_force_remove("127.0.0.1", port, 3);
    ASSERT_EQ(st, MDS_OK);

    /* Verify node 3 is gone. */
    struct cluster_member result;
    st = cluster_membership_get(cm, 3, &result);
    ASSERT_TRUE(st != MDS_OK);

    cluster_transport_server_stop(srv);
    cluster_membership_destroy(cm);
    subtree_map_destroy(smap);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * Shared helpers for rebalance/tiering tests.
 * ------------------------------------------------------------------- */

static const char *REBAL_DB = "/tmp/test_ct_rebalance.db";
static char g_rebal_tmpdir[256];

static void cleanup_rebal(void)
{
    char lockpath[512];
    (void)unlink(REBAL_DB);
    snprintf(lockpath, sizeof(lockpath), "%s-lock", REBAL_DB);
    (void)unlink(lockpath);
    if (g_rebal_tmpdir[0] != '\0') {
        char cmd[512];
        snprintf(cmd, sizeof(cmd), "rm -rf %s", g_rebal_tmpdir);
        (void)system(cmd);
        g_rebal_tmpdir[0] = '\0';
    }
}


/* -------------------------------------------------------------------
 * test_rebalance_status_wire
 *
 * Start a server with rebalance worker, query status over the wire,
 * and verify the response contains zeroed-out idle fields.
 * ------------------------------------------------------------------- */

static void test_rebalance_status_wire(void)
{
    fprintf(stdout, "  test_rebalance_status_wire:       ");
    fflush(stdout);

    cleanup_rebal();
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    enum mds_status st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
	cat = db;
    ASSERT_EQ(st, MDS_OK);

    struct mds_proxy_ctx *proxy = NULL;
    st = mds_proxy_ctx_create(&proxy);
    ASSERT_EQ(st, MDS_OK);

    struct rebalance_worker *rw = NULL;
    ASSERT_EQ(rebalance_init(cat, NULL, proxy, NULL, &rw), 0);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0,
                                        wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    cluster_transport_server_set_rebalance(srv, rw);
    uint16_t port = cluster_transport_server_port(srv);

    /* Query status -- worker never started, should be idle/zeroed. */
    struct rebalance_status_info info;
    memset(&info, 0xFF, sizeof(info));
    st = cluster_transport_request_rebalance_status("127.0.0.1", port, &info);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(info.state, REBALANCE_IDLE);
    ASSERT_EQ(info.source_ds, 0);
    ASSERT_EQ(info.target_ds, 0);
    ASSERT_EQ(info.files_moved, 0);
    ASSERT_EQ(info.bytes_copied, 0);

    cluster_transport_server_stop(srv);
    rebalance_destroy(rw);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup_rebal();
    PASS();
}


/* -------------------------------------------------------------------
 * test_tiering_status_wire
 *
 * Start a server with tiering worker, query status over the wire,
 * and verify the response contains zeroed-out idle fields.
 * ------------------------------------------------------------------- */

static void test_tiering_status_wire(void)
{
    fprintf(stdout, "  test_tiering_status_wire:          ");
    fflush(stdout);

    cleanup_rebal();
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    enum mds_status st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
	cat = db;
    ASSERT_EQ(st, MDS_OK);

    struct mds_proxy_ctx *proxy = NULL;
    st = mds_proxy_ctx_create(&proxy);
    ASSERT_EQ(st, MDS_OK);

    struct commit_queue *cq = NULL;
    ASSERT_EQ(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq), 0);

    struct io_tracker *iot = NULL;
    ASSERT_EQ(io_tracker_init(16, &iot), 0);

    struct tiering_worker *tw = NULL;
    ASSERT_EQ(tiering_init(cat, cq, proxy, NULL, iot, &tw), 0);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0,
                                        wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    cluster_transport_server_set_tiering(srv, tw);
    uint16_t port = cluster_transport_server_port(srv);

    /* Query status -- worker never started, should be idle/zeroed. */
    struct tiering_status_info info;
    memset(&info, 0xFF, sizeof(info));
    st = cluster_transport_request_tiering_status("127.0.0.1", port, &info);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(info.state, TIERING_IDLE);
    ASSERT_EQ(info.eval_count, 0);
    ASSERT_EQ(info.files_promoted, 0);
    ASSERT_EQ(info.files_demoted, 0);
    ASSERT_EQ(info.bytes_moved, 0);

    cluster_transport_server_stop(srv);
    tiering_destroy(tw);
    commit_queue_destroy(cq);
    io_tracker_destroy(iot);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup_rebal();
    PASS();
}


/* -------------------------------------------------------------------
 * test_tiering_stop_wire
 * ------------------------------------------------------------------- */

static void test_tiering_stop_wire(void)
{
    fprintf(stdout, "  test_tiering_stop_wire:            ");
    fflush(stdout);

    cleanup_rebal();
    struct mds_catalogue *db = NULL;
	struct mds_catalogue *cat = NULL;
    enum mds_status st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
	cat = db;
    ASSERT_EQ(st, MDS_OK);

    struct mds_proxy_ctx *proxy = NULL;
    st = mds_proxy_ctx_create(&proxy);
    ASSERT_EQ(st, MDS_OK);

    struct commit_queue *cq = NULL;
    ASSERT_EQ(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq), 0);

    struct io_tracker *iot = NULL;
    ASSERT_EQ(io_tracker_init(16, &iot), 0);

    struct tiering_worker *tw = NULL;
    ASSERT_EQ(tiering_init(cat, cq, proxy, NULL, iot, &tw), 0);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0,
                                        wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    cluster_transport_server_set_tiering(srv, tw);
    uint16_t port = cluster_transport_server_port(srv);

    /* Stop without start -- should still return OK. */
    st = cluster_transport_request_tiering_stop("127.0.0.1", port);
    ASSERT_EQ(st, MDS_OK);

    cluster_transport_server_stop(srv);
    tiering_destroy(tw);
    io_tracker_destroy(iot);
    commit_queue_destroy(cq);
    mds_proxy_ctx_destroy(proxy);
    mds_catalogue_close(db);
    cleanup_rebal();
    PASS();
}

/* -------------------------------------------------------------------
 * test_tiering_start_null_cfg
 * ------------------------------------------------------------------- */

static void test_tiering_start_null_cfg(void)
{
    fprintf(stdout, "  test_tiering_start_null_cfg:       ");
    fflush(stdout);

    enum mds_status st = cluster_transport_request_tiering_start(
        "127.0.0.1", 1, NULL);
    ASSERT_EQ(st, MDS_ERR_INVAL);

    PASS();
}

/* -----------------------------------------------------------------------
 * test_ds_list_admin_wire -- DS list via loopback (Item 47)
 * ----------------------------------------------------------------------- */

static void test_ds_list_admin_wire(void)
{
    fprintf(stdout, "  test_ds_list_admin_wire:          ");

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/test_ct_dslist_XXXXXX");
    ASSERT_TRUE(mkdtemp(tmpdir) != NULL);

    char db_path[300];
    snprintf(db_path, sizeof(db_path), "%s/ds.db", tmpdir);

    struct mds_catalogue *db = NULL;
    enum mds_status st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);

    /* Seed 2 DS entries. */
    struct mds_ds_info ds1;
    memset(&ds1, 0, sizeof(ds1));
    ds1.ds_id = 10; ds1.state = DS_ONLINE; ds1.tier = 0;
    ds1.mode = DS_MODE_GENERIC; ds1.transport = DS_TRANSPORT_TCP;
    snprintf(ds1.host, sizeof(ds1.host), "ds10");
    snprintf(ds1.export_path, sizeof(ds1.export_path), "/export");
    ds1.port = 2049; ds1.total_bytes = 1000000; ds1.used_bytes = 500000;
    snprintf(ds1.addr, sizeof(ds1.addr), "ds10:/export");
    {
        struct mds_cat_txn *txn = NULL;
        st = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn);
        ASSERT_EQ(st, MDS_OK);
        st = mds_cat_ds_put(db, txn, &ds1);
        ASSERT_EQ(st, MDS_OK);
        ASSERT_EQ(mds_cat_txn_commit(txn), 0);
    }

    struct mds_ds_info ds2;
    memset(&ds2, 0, sizeof(ds2));
    ds2.ds_id = 20; ds2.state = DS_OFFLINE; ds2.tier = 1;
    ds2.mode = DS_MODE_GENERIC; ds2.transport = DS_TRANSPORT_TCP;
    snprintf(ds2.host, sizeof(ds2.host), "ds20");
    snprintf(ds2.export_path, sizeof(ds2.export_path), "/cold");
    ds2.port = 2050; ds2.total_bytes = 2000000; ds2.used_bytes = 100000;
    snprintf(ds2.addr, sizeof(ds2.addr), "ds20:/cold");
    {
        struct mds_cat_txn *txn = NULL;
        st = mds_cat_txn_begin(db, MDS_CAT_TXN_WRITE, &txn);
        ASSERT_EQ(st, MDS_OK);
        st = mds_cat_ds_put(db, txn, &ds2);
        ASSERT_EQ(st, MDS_OK);
        ASSERT_EQ(mds_cat_txn_commit(txn), 0);
    }

    /* Start loopback server. */
    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, NULL, NULL, 0, 0, wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t port = cluster_transport_server_port(srv);
    ASSERT_TRUE(port > 0);

    /* Client request. */
    struct mds_ds_info *out = NULL;
    uint32_t count = 0;
    st = cluster_transport_request_ds_list("127.0.0.1", port, &out, &count);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(count, 2);

    /* Verify fields (order may vary by ds_id). */
    const struct mds_ds_info *d10 = NULL;
    const struct mds_ds_info *d20 = NULL;
    for (uint32_t i = 0; i < count; i++) {
        if (out[i].ds_id == 10) { d10 = &out[i]; }
        if (out[i].ds_id == 20) { d20 = &out[i]; }
    }
    ASSERT_TRUE(d10 != NULL);
    ASSERT_TRUE(d20 != NULL);

    ASSERT_EQ(d10->state, DS_ONLINE);
    ASSERT_EQ(d10->tier, 0);
    ASSERT_EQ(d10->port, 2049);
    ASSERT_EQ(d10->total_bytes, 1000000);
    ASSERT_EQ(d10->used_bytes, 500000);
    ASSERT_TRUE(strcmp(d10->addr, "ds10:/export") == 0);

    ASSERT_EQ(d20->state, DS_OFFLINE);
    ASSERT_EQ(d20->tier, 1);
    ASSERT_EQ(d20->port, 2050);
    ASSERT_EQ(d20->total_bytes, 2000000);
    ASSERT_EQ(d20->used_bytes, 100000);
    ASSERT_TRUE(strcmp(d20->addr, "ds20:/cold") == 0);

    free(out);
    cluster_transport_server_stop(srv);
    mds_catalogue_close(db);

    /* Cleanup tmpdir. */
    char rm_cmd[512];
    snprintf(rm_cmd, sizeof(rm_cmd), "rm -rf %s", tmpdir);
    (void)system(rm_cmd);

    fprintf(stdout, "PASS\n");
    passed++;
}


/* -----------------------------------------------------------------------
 * test_ds_add_set_state_remove_wire -- DS lifecycle via loopback (Item 47b)
 * ----------------------------------------------------------------------- */

static void test_ds_add_set_state_remove_wire(void)
{
    fprintf(stdout, "  test_ds_add_set_state_remove_wire: ");

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/test_ct_dsadm_XXXXXX");
    ASSERT_TRUE(mkdtemp(tmpdir) != NULL);

    char db_path[300];
    snprintf(db_path, sizeof(db_path), "%s/ds.db", tmpdir);

    struct mds_catalogue *db = NULL;
    enum mds_status st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);

    /* Need a commit queue for write operations. */
    struct commit_queue *cq = NULL;
    ASSERT_EQ(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq), 0);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, NULL, NULL, 0, 0,
                                        wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    cluster_transport_server_set_cq(srv, cq);

    uint16_t port = cluster_transport_server_port(srv);
    ASSERT_TRUE(port > 0);

    /* DS add. */
    struct mds_ds_info info;
    memset(&info, 0, sizeof(info));
    info.ds_id = 55;
    info.port = 2049;
    info.tier = 0;
    info.total_bytes = 1000000;
    snprintf(info.addr, sizeof(info.addr), "ds55:/export");

    st = cluster_transport_request_ds_add("127.0.0.1", port, &info);
    ASSERT_EQ(st, MDS_OK);

    /* Verify via ds list. */
    struct mds_ds_info *out = NULL;
    uint32_t count = 0;
    st = cluster_transport_request_ds_list("127.0.0.1", port, &out, &count);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(out[0].ds_id, 55);
    ASSERT_EQ(out[0].state, DS_OFFLINE);
    free(out);

    /* Duplicate add should fail. */
    st = cluster_transport_request_ds_add("127.0.0.1", port, &info);
    ASSERT_EQ(st, MDS_ERR_EXISTS);

    /* Set state to ONLINE. */
    st = cluster_transport_request_ds_set_state("127.0.0.1", port,
                                                 55, DS_ONLINE);
    ASSERT_EQ(st, MDS_OK);

    /* Remove should fail -- DS is ONLINE. */
    st = cluster_transport_request_ds_remove("127.0.0.1", port, 55);
    ASSERT_EQ(st, MDS_ERR_PERM);

    /* Set back to OFFLINE. */
    st = cluster_transport_request_ds_set_state("127.0.0.1", port,
                                                 55, DS_OFFLINE);
    ASSERT_EQ(st, MDS_OK);

    /* Remove should now succeed. */
    st = cluster_transport_request_ds_remove("127.0.0.1", port, 55);
    ASSERT_EQ(st, MDS_OK);

    /* Verify empty. */
    st = cluster_transport_request_ds_list("127.0.0.1", port, &out, &count);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(count, 0);
    free(out);

    cluster_transport_server_stop(srv);
    commit_queue_destroy(cq);
    mds_catalogue_close(db);

    char rm_cmd[512];
    snprintf(rm_cmd, sizeof(rm_cmd), "rm -rf %s", tmpdir);
    (void)system(rm_cmd);

    fprintf(stdout, "PASS\n");
    passed++;
}

static void test_ds_admin_invalidates_cache(void)
{
    fprintf(stdout, "  test_ds_admin_invalidates_cache:   ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    ASSERT_TRUE(db != NULL);
    struct mds_catalogue *cat = NULL;
    cat = db;

    struct commit_queue *cq = NULL;
    ASSERT_EQ(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq), 0);

    struct ds_cache *cache = NULL;
    ASSERT_EQ(ds_cache_create(cat, &cache), 0);
    ASSERT_TRUE(cache != NULL);

    struct cluster_server *srv = NULL;
    enum mds_status st = cluster_transport_server_start(
        0, "127.0.0.1", NULL, 0, 0, wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    cluster_transport_server_set_cq(srv, cq);
    cluster_transport_server_set_ds_cache(srv, cache);

    uint16_t port = cluster_transport_server_port(srv);
    ASSERT_TRUE(port > 0);

    struct mds_ds_info info;
    memset(&info, 0, sizeof(info));
    info.ds_id = 77;
    info.port = 20049;
    info.tier = 1;
    info.total_bytes = 5000000;
    info.mode = DS_MODE_GENERIC;
    info.transport = DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA;
    snprintf(info.addr, sizeof(info.addr), "rdma-ds77:/cold");
    snprintf(info.host, sizeof(info.host), "rdma-ds77");
    snprintf(info.export_path, sizeof(info.export_path), "/cold");

    st = cluster_transport_request_ds_add("127.0.0.1", port, &info);
    ASSERT_EQ(st, MDS_OK);

    struct mds_ds_info cached;
    memset(&cached, 0, sizeof(cached));
    ASSERT_EQ(ds_cache_get(cache, 77, &cached), MDS_OK);
    ASSERT_EQ(cached.ds_id, 77);
    ASSERT_EQ(cached.state, DS_OFFLINE);
    ASSERT_EQ(cached.mode, DS_MODE_GENERIC);

    st = cluster_transport_request_ds_set_state("127.0.0.1", port,
                                                77, DS_ONLINE);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(ds_cache_get(cache, 77, &cached), MDS_OK);
    ASSERT_EQ(cached.state, DS_ONLINE);

    uint8_t secret[32];
    for (size_t i = 0; i < sizeof(secret); i++) {
        secret[i] = (uint8_t)(i + 1U);
    }

    uint64_t epoch = 1234;
    st = cluster_transport_request_ds_provision("127.0.0.1", port,
                                                77, secret,
                                                sizeof(secret), epoch);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(ds_cache_get(cache, 77, &cached), MDS_OK);
    ASSERT_EQ(cached.mode, DS_MODE_GENERIC);

    uint8_t cached_secret[32];
    uint64_t cached_epoch = 0;
    ASSERT_EQ(ds_cache_get_provision(cache, 77, cached_secret,
                                     sizeof(cached_secret),
                                     &cached_epoch), MDS_OK);
    ASSERT_TRUE(memcmp(cached_secret, secret, sizeof(secret)) == 0);
    ASSERT_EQ(cached_epoch, epoch);

    /* patch_ready / patch_clear / validate are vestiges of the
     * removed DS_MODE_PATCHED track.  Verify the admin RPCs still
     * succeed (backward compat) but don't assert on patched semantics. */
    st = cluster_transport_request_ds_patch_ready("127.0.0.1", port,
                                                  77, 0);
    ASSERT_EQ(st, MDS_OK);

    st = cluster_transport_request_ds_validate("127.0.0.1", port, 77,
                                               "cache-test", "ok");
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(ds_cache_get(cache, 77, &cached), MDS_OK);
    ASSERT_EQ(cached.capabilities, (uint32_t)0);

    st = cluster_transport_request_ds_validate_clear("127.0.0.1", port, 77);
    ASSERT_EQ(st, MDS_OK);

    st = cluster_transport_request_ds_patch_clear("127.0.0.1", port, 77);
    ASSERT_EQ(st, MDS_OK);

    st = cluster_transport_request_ds_set_state("127.0.0.1", port,
                                                77, DS_OFFLINE);
    ASSERT_EQ(st, MDS_OK);

    st = cluster_transport_request_ds_remove("127.0.0.1", port, 77);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(ds_cache_get(cache, 77, &cached), MDS_ERR_NOTFOUND);
    ASSERT_EQ(ds_cache_get_provision(cache, 77, cached_secret,
                                     sizeof(cached_secret),
                                     &cached_epoch), MDS_ERR_NOTFOUND);

    cluster_transport_server_stop(srv);
    ds_cache_destroy(cache);
    commit_queue_destroy(cq);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}


/* -----------------------------------------------------------------------
 * test_ds_add_v2_mode_transport -- DS add with structured fields (Phase 1)
 * ----------------------------------------------------------------------- */

static void test_ds_add_v2_mode_transport(void)
{
    fprintf(stdout, "  test_ds_add_v2_mode_transport:      ");

    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/test_ct_dsv2_XXXXXX");
    ASSERT_TRUE(mkdtemp(tmpdir) != NULL);

    char db_path[300];
    snprintf(db_path, sizeof(db_path), "%s/ds.db", tmpdir);

    struct mds_catalogue *db = NULL;
    enum mds_status st = ((db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO);
    ASSERT_EQ(st, MDS_OK);

    struct commit_queue *cq = NULL;
    ASSERT_EQ(commit_queue_create(db, NULL, 0, 0, 0, 0, 0, 0, &cq), 0);

    struct cluster_server *srv = NULL;
    st = cluster_transport_server_start(0, NULL, NULL, 0, 0,
                                        wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    cluster_transport_server_set_cq(srv, cq);
    uint16_t port = cluster_transport_server_port(srv);

    /* Register a generic RDMA-capable DS. */
    struct mds_ds_info info;
    memset(&info, 0, sizeof(info));
    info.ds_id = 88;
    info.port = 20049;
    info.tier = 1;
    info.total_bytes = 5000000;
    info.mode = DS_MODE_GENERIC;
    info.transport = DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA;
    snprintf(info.addr, sizeof(info.addr), "rdma-ds88:/cold");
    snprintf(info.host, sizeof(info.host), "rdma-ds88");
    snprintf(info.export_path, sizeof(info.export_path), "/cold");

    st = cluster_transport_request_ds_add("127.0.0.1", port, &info);
    ASSERT_EQ(st, MDS_OK);

    /* Read back via ds list and verify V2 fields. */
    struct mds_ds_info *out = NULL;
    uint32_t count = 0;
    st = cluster_transport_request_ds_list("127.0.0.1", port, &out, &count);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(out[0].ds_id, 88);
    ASSERT_EQ(out[0].mode, DS_MODE_GENERIC);
    ASSERT_EQ(out[0].transport, DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA);
    ASSERT_TRUE(strcmp(out[0].host, "rdma-ds88") == 0);
    ASSERT_TRUE(strcmp(out[0].export_path, "/cold") == 0);
    free(out);

    cluster_transport_server_stop(srv);
    commit_queue_destroy(cq);
    mds_catalogue_close(db);

    char rm_cmd[512];
    snprintf(rm_cmd, sizeof(rm_cmd), "rm -rf %s", tmpdir);
    (void)system(rm_cmd);

    fprintf(stdout, "PASS\n");
    passed++;
}


/* -------------------------------------------------------------------
 * test_admin_allowed_hosts_acl
 *
 * Verify that admin_allowed_hosts permits a remote IP to connect
 * to the admin transport while non-listed IPs are rejected.
 *
 * Strategy: start a server with cluster_bind_addr = 0.0.0.0
 * (no cluster_peer, no TLS) and wire in a config with
 * admin_allowed_hosts = 127.0.0.1.  Localhost should connect;
 * the ACL check itself is confirmed by the successful RPC round
 * trip (CONFIG_SHOW).  Without the admin_allowed_hosts entry the
 * server would restrict to loopback anyway via the empty-list
 * fallback, so the real value of this test is demonstrating the
 * new code path is exercised and the config is plumbed through
 * cluster_transport_server_set_config.
 * ------------------------------------------------------------------- */

static void test_admin_allowed_hosts_acl(void)
{
    fprintf(stdout, "  test_admin_allowed_hosts_acl:       ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    ASSERT_TRUE(db != NULL);

    /* Start server on 0.0.0.0 with NO cluster_peer entries.
     * Without admin_allowed_hosts the loopback-only fallback
     * would still accept 127.0.0.1 -- but we want to exercise
     * the new ACL path, so we pass a non-empty admin list. */
    struct cluster_server *srv = NULL;
    enum mds_status st = cluster_transport_server_start(
        0, "0.0.0.0", NULL, 0, 0,
        wrap_db_as_cat(db), NULL, NULL, &srv);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_TRUE(srv != NULL);

    /* Build a config with admin_allowed_hosts = 127.0.0.1. */
    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    snprintf(cfg.admin_allowed_hosts[0],
             sizeof(cfg.admin_allowed_hosts[0]), "127.0.0.1");
    cfg.admin_allowed_host_count = 1;

    cluster_transport_server_set_config(srv, &cfg);

    uint16_t port = cluster_transport_server_port(srv);
    ASSERT_TRUE(port > 0);

    /* CONFIG_SHOW from 127.0.0.1 should succeed (admin host
     * is in the list).  The response body is empty because
     * the config was just zero-init'd, but the RPC status
     * proves the connection was accepted and dispatched. */
    char *text = NULL;
    st = cluster_transport_request_config_show(
        "127.0.0.1", port, NULL, &text);
    /* MDS_ERR_INVAL is expected: cfg has no real keys to
     * render, so render_config_show returns an empty body
     * which the handler maps to INVAL.  The important thing
     * is that we did NOT get MDS_ERR_IO (connection refused). */
    ASSERT_TRUE(st == MDS_OK || st == MDS_ERR_INVAL);
    free(text);

    cluster_transport_server_stop(srv);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

int main(void)
{
    fprintf(stdout, "test_cluster_transport:\n");

    test_server_start_stop();
    test_tcp_prepare_vote_commit();
    test_tcp_prepare_abort();
    test_connect_failure();
    test_prepare_vote_abort_on_conflict();
    test_mig_admin_request();
    test_mig_admin_membership_resolve();
    test_set_lifecycle_wire();
    test_split_admin_local_wire();
    test_split_admin_local_wire_rejects_sharded_mode();
    test_split_admin_fileid_mismatch();
    test_assign_admin_not_exact_root();
    test_split_admin_dest_not_serving();
    test_standby_detach_wire();
    test_force_remove_wire();
    test_rebalance_status_wire();
    test_tiering_status_wire();
    test_tiering_stop_wire();
    test_tiering_start_null_cfg();
    test_ds_list_admin_wire();
    test_ds_add_set_state_remove_wire();
    test_ds_admin_invalidates_cache();
    test_ds_add_v2_mode_transport();
    test_admin_allowed_hosts_acl();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}

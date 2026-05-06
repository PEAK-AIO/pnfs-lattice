/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_compound_cluster.c -- Tests for compound dispatch referral hooks
 *                            and cross-subtree rename routing.
 *
 * These tests exercise the cluster integration in compound.c:
 *   - PUTFH on a junction -> NFS4_OK
 *   - LOOKUP into a junction -> NFS4ERR_MOVED
 *   - LOOKUP + GETATTR(fs_locations) on a junction -> NFS4_OK
 *   - RENAME cross-subtree without transport -> NFS4ERR_XDEV
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "subtree_map.h"
#include "referral.h"
#include "cluster_transport.h"
#include "mds_catalogue.h"
#include "cluster_membership.h"
#include "xdr_codec.h"
#include "test_helpers.h"

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

static const char *TEST_DB_PATH = "/tmp/test_compound_cluster.db";

static void cleanup_db(void)
{
    char lockpath[512];
    (void)unlink(TEST_DB_PATH);
    snprintf(lockpath, sizeof(lockpath), "%s-lock", TEST_DB_PATH);
    (void)unlink(lockpath);
}

static struct mds_catalogue *wrap_db_as_cat(struct mds_catalogue *db)
{
    return db;
}

static struct mds_catalogue *open_test_db(void)
{
    cleanup_db();
    return open_test_catalogue();
}

/* -------------------------------------------------------------------
 * Op builder helpers
 * ------------------------------------------------------------------- */

static struct nfs4_op mk_putrootfh(void)
{
    struct nfs4_op op;
    memset(&op, 0, sizeof(op));
    op.opnum = OP_PUTROOTFH;
    return op;
}

static struct nfs4_op mk_putfh(uint64_t fileid)
{
    struct nfs4_op op;
    memset(&op, 0, sizeof(op));
    op.opnum = OP_PUTFH;
    op.arg.putfh.fh.fileid = fileid;
    return op;
}

static struct nfs4_op mk_lookup(const char *name)
{
    struct nfs4_op op;
    memset(&op, 0, sizeof(op));
    op.opnum = OP_LOOKUP;
    snprintf(op.arg.lookup.name, sizeof(op.arg.lookup.name), "%s", name);
    return op;
}

static struct nfs4_op mk_savefh(void)
{
    struct nfs4_op op;
    memset(&op, 0, sizeof(op));
    op.opnum = OP_SAVEFH;
    return op;
}

static struct nfs4_op mk_getattr_fs_locations(void)
{
    struct nfs4_op op;

    memset(&op, 0, sizeof(op));
    op.opnum = OP_GETATTR;
    nfs4_bitmap_set(op.arg.getattr.requested, FATTR4_FS_LOCATIONS);
    nfs4_bitmap_set(op.arg.getattr.requested, FATTR4_FSID);
    return op;
}

static struct nfs4_op mk_rename(const char *src, const char *dst)
{
    struct nfs4_op op;
    memset(&op, 0, sizeof(op));
    op.opnum = OP_RENAME;
    snprintf(op.arg.rename.src_name, sizeof(op.arg.rename.src_name),
             "%s", src);
    snprintf(op.arg.rename.dst_name, sizeof(op.arg.rename.dst_name),
             "%s", dst);
    return op;
}


/* -------------------------------------------------------------------
 * test_putfh_junction_succeeds
 *
 * Create a junction directory, then PUTFH to it with a subtree_map
 * configured.  PUTFH should succeed so GETATTR(fs_locations) can
 * later be issued on the junction filehandle.
 * ------------------------------------------------------------------- */

static void test_putfh_junction_succeeds(void)
{
    fprintf(stdout, "  test_putfh_junction_ok:       ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    ASSERT_TRUE(db != NULL);
    struct mds_catalogue *cat = wrap_db_as_cat(db);
    ASSERT_TRUE(cat != NULL);

    /* Create a junction directory. */
    enum mds_status st;
    st = referral_create_junction(cat, MDS_FILEID_ROOT, "remote_home", 2);
    ASSERT_EQ(st, MDS_OK);

    /* Find the junction's fileid. */
    struct mds_inode junction;
    st = mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "remote_home", &junction);
    ASSERT_EQ(st, MDS_OK);

    /* Create a minimal subtree map (enables referral checking). */
    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);

    /* Build compound: PUTFH(junction_fileid). */
    struct compound_data cd;
    compound_init(&cd);
    cd.cat = cat;
    cd.smap = smap;

    struct nfs4_op ops[1];
    struct nfs4_result results[1];
    ops[0] = mk_putfh(junction.fileid);

    uint32_t n = compound_process(&cd, ops, results, 1);
    ASSERT_EQ(n, 1);
    ASSERT_EQ(results[0].status, NFS4_OK); /* PUTFH on junction succeeds -- RFC 8881 S8.5.1 */

    mds_catalogue_close(db);
    subtree_map_destroy(smap);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_lookup_into_junction_returns_moved
 *
 * PUTROOTFH + LOOKUP("junction_dir") where junction_dir is a junction.
 * Should return NFS4ERR_MOVED on the LOOKUP.
 * ------------------------------------------------------------------- */

static void test_lookup_junction_returns_moved(void)
{
    fprintf(stdout, "  test_lookup_junction_ok:      ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    ASSERT_TRUE(db != NULL);
    struct mds_catalogue *cat = wrap_db_as_cat(db);
    ASSERT_TRUE(cat != NULL);

    enum mds_status st;
    st = referral_create_junction(cat, MDS_FILEID_ROOT, "foreign", 3);
    ASSERT_EQ(st, MDS_OK);

    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);

    struct compound_data cd;
    compound_init(&cd);
    cd.cat = cat;
    cd.smap = smap;

    struct nfs4_op ops[2];
    struct nfs4_result results[2];
    ops[0] = mk_putrootfh();
    ops[1] = mk_lookup("foreign");

    uint32_t n = compound_process(&cd, ops, results, 2);
    /* PUTROOTFH succeeds, LOOKUP fails with MOVED. */
    ASSERT_EQ(n, 2);
    ASSERT_EQ(results[0].status, NFS4_OK);
    ASSERT_EQ(results[1].status, NFS4ERR_MOVED);

    subtree_map_destroy(smap);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_lookup_junction_getattr_fs_locations
 *
 * Linux follows a referral by retrying with
 * PUTFH(parent) + LOOKUP(name) + GETATTR(fs_locations).  That LOOKUP
 * must succeed so GETATTR can return referral data.
 * ------------------------------------------------------------------- */

static void test_lookup_junction_getattr_fs_locations(void)
{
    fprintf(stdout, "  test_lookup_junction_fs_locations: ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    ASSERT_TRUE(db != NULL);
    struct mds_catalogue *cat = wrap_db_as_cat(db);
    ASSERT_TRUE(cat != NULL);

    enum mds_status st;
    st = referral_create_junction(cat, MDS_FILEID_ROOT, "foreign", 3);
    ASSERT_EQ(st, MDS_OK);

    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(smap, "/foreign", 3, "10.10.10.51",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    struct compound_data cd;
    compound_init(&cd);
    cd.cat = cat;
    cd.smap = smap;

    struct nfs4_op ops[3];
    struct nfs4_result results[3];
    ops[0] = mk_putrootfh();
    ops[1] = mk_lookup("foreign");
    ops[2] = mk_getattr_fs_locations();

    uint32_t n = compound_process(&cd, ops, results, 3);
    ASSERT_EQ(n, 3);
    ASSERT_EQ(results[0].status, NFS4_OK);
    ASSERT_EQ(results[1].status, NFS4_OK);
    ASSERT_EQ(results[2].status, NFS4_OK);
    ASSERT_TRUE(results[2].res.getattr.has_referral);
    ASSERT_TRUE(strcmp(results[2].res.getattr.referral_server,
                       "10.10.10.51") == 0);
    ASSERT_TRUE(strcmp(results[2].res.getattr.referral_rootpath,
                       "/") == 0);
    ASSERT_TRUE(strcmp(results[2].res.getattr.referral_fs_root,
                       "/foreign") == 0);

    subtree_map_destroy(smap);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_rename_cross_subtree_local_to_remote_fastpath
 *
 * In the single-RonDB-cluster deployment model every MDS shares the
 * same NDB cluster, so a RENAME whose source subtree is owned by us
 * and whose destination subtree is owned by another MDS no longer
 * needs a multi-MDS gRPC 2PC: rondb_shim_rename batches the dirent
 * delete + dirent insert + parent updates + child parent_fileid
 * change into a single NdbTransaction whose cross-partition atomicity
 * is guaranteed by NDB's internal 2PC (see compound_namespace.c
 * src_local && !dst_local block, ~6 RTs collapse to one NDB commit).
 *
 * The old expectation here was NFS4ERR_XDEV -- a give-up signal that
 * predates the fused-rename optimisation and has been moot since the
 * RonDB-only catalogue migration.  This rewrite verifies the new
 * contract:
 *
 *   1. RENAME succeeds (NFS4_OK).
 *   2. The dirent under /local is gone.
 *   3. The dirent under /remote points at the original fileid.
 * ------------------------------------------------------------------- */

static void test_rename_cross_subtree_local_to_remote_fastpath(void)
{
    fprintf(stdout,
            "  test_rename_cross_subtree_local_to_remote_fastpath: ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    ASSERT_TRUE(db != NULL);

    enum mds_status st;

    /* /local and /remote dirs + a file under /local. */
    struct mds_inode local_dir, remote_dir;
    struct mds_catalogue *cat = wrap_db_as_cat(db);

    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "local",
                           MDS_FTYPE_DIR, 0755, 0, 0,
                           NULL, &local_dir);
    ASSERT_EQ(st, MDS_OK);

    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "remote",
                           MDS_FTYPE_DIR, 0755, 0, 0,
                           NULL, &remote_dir);
    ASSERT_EQ(st, MDS_OK);

    struct mds_inode file;
    st = test_create_file(db, local_dir.fileid, "data.txt",
                          0644, &file);
    ASSERT_EQ(st, MDS_OK);

    /* Subtree map: /local -> self (MDS 1), /remote -> MDS 2.
     * Source is local, destination is non-local -- the path the
     * fast-path block in op_rename targets. */
    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                          NULL, &smap);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(smap, "/local", 1, "localhost",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);
    st = subtree_map_add(smap, "/remote", 2, "remote-host",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* PUTROOTFH -> LOOKUP(local) -> SAVEFH -> PUTROOTFH ->
     * LOOKUP(remote) -> RENAME("data.txt" -> "data.txt"). */
    struct compound_data cd;
    compound_init(&cd);
    cd.cat = cat;
    cd.smap = smap;

    struct nfs4_op ops[6];
    struct nfs4_result results[6];
    ops[0] = mk_putrootfh();
    ops[1] = mk_lookup("local");
    ops[2] = mk_savefh();
    ops[3] = mk_putrootfh();
    ops[4] = mk_lookup("remote");
    ops[5] = mk_rename("data.txt", "data.txt");

    uint32_t n = compound_process(&cd, ops, results, 6);
    ASSERT_EQ(n, 6);
    ASSERT_EQ(results[0].status, NFS4_OK);
    ASSERT_EQ(results[1].status, NFS4_OK);
    ASSERT_EQ(results[2].status, NFS4_OK);
    ASSERT_EQ(results[3].status, NFS4_OK);
    ASSERT_EQ(results[4].status, NFS4_OK);
    ASSERT_EQ(results[5].status, NFS4_OK);

    /* Source dirent gone, destination dirent points at the same
     * inode the original create returned. */
    {
        struct mds_inode looked_up;
        st = mds_cat_ns_lookup(cat, local_dir.fileid, "data.txt",
                               &looked_up);
        ASSERT_EQ(st, MDS_ERR_NOTFOUND);

        st = mds_cat_ns_lookup(cat, remote_dir.fileid, "data.txt",
                               &looked_up);
        ASSERT_EQ(st, MDS_OK);
        ASSERT_EQ(looked_up.fileid, file.fileid);
    }

    subtree_map_destroy(smap);
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

/* -------------------------------------------------------------------
 * test_rename_cross_subtree_membership
 *
 * Like test_rename_cross_subtree_xdev, but with membership set.
 * The rename should resolve the destination transport on-demand
 * via membership and attempt the 2PC.  With a loopback destination
 * server running, the rename succeeds.
 * ------------------------------------------------------------------- */

static const char *CC_DEST_DB = "/tmp/test_cc_rename_dest.db";

static void cleanup_cc_dest_db(void)
{
    char lockpath[512];
    (void)unlink(CC_DEST_DB);
    snprintf(lockpath, sizeof(lockpath), "%s-lock", CC_DEST_DB);
    (void)unlink(lockpath);
}

static void test_rename_cross_subtree_membership(void)
{
    fprintf(stdout, "  test_rename_cross_subtree_membership: ");
    fflush(stdout);

    struct mds_catalogue *db = open_test_db();
    ASSERT_TRUE(db != NULL);
    struct mds_catalogue *cat = wrap_db_as_cat(db);

    enum mds_status st;

    /* Create /local and /remote directories + a file in /local. */
    struct mds_inode local_dir, remote_dir;
    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "local",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &local_dir);
    ASSERT_EQ(st, MDS_OK);

    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "remote",
                       MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &remote_dir);
    ASSERT_EQ(st, MDS_OK);

    struct mds_inode file;
    st = test_create_file(db, local_dir.fileid, "data.txt",
                          0644, &file);
    ASSERT_EQ(st, MDS_OK);

    /* Subtree map: /local -> self (MDS 1), /remote -> MDS 2. */
    struct subtree_map *smap = NULL;
    st = subtree_map_init(NULL, NULL, 1, "localhost",
                                 NULL, &smap);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(smap, "/local", 1, "localhost",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    st = subtree_map_add(smap, "/remote", 2, "127.0.0.1",
                         SUBTREE_ACTIVE, 1);
    ASSERT_EQ(st, MDS_OK);

    /* Start a destination transport server for the 2PC. */
    cleanup_cc_dest_db();
    struct mds_catalogue *dst_db = NULL;
    st = (dst_db = open_test_catalogue()) != NULL ? MDS_OK : MDS_ERR_IO;
    ASSERT_EQ(st, MDS_OK);

    /* Create matching directories in dst_db so the 2PC PREPARE
     * handler finds dst_parent.  Both fresh DBs allocate fileids
     * from the same starting point, so same-order creation yields
     * matching fileids. */
    struct mds_catalogue *dst_cat = wrap_db_as_cat(dst_db);
    {
        struct mds_inode d1, d2;
        st = mds_cat_ns_create(dst_cat, NULL, MDS_FILEID_ROOT, "local",
                           MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &d1);
        ASSERT_EQ(st, MDS_OK);
        st = mds_cat_ns_create(dst_cat, NULL, MDS_FILEID_ROOT, "remote",
                           MDS_FTYPE_DIR, 0755, 0, 0,
			       NULL, &d2);
        ASSERT_EQ(st, MDS_OK);
        /* Sanity: fileids must match. */
        ASSERT_EQ(d2.fileid, remote_dir.fileid);

        /* RonDB shared-authority: on_commit expects the inode in the
         * destination DB.  Insert the source file's inode. */
        struct mds_cat_txn *txn = NULL;
        ASSERT_EQ(mds_cat_txn_begin(dst_cat, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
        ASSERT_EQ(mds_cat_inode_put(dst_cat, txn, &file), MDS_OK);
        ASSERT_EQ(mds_cat_txn_commit(txn), MDS_OK);
    }

    struct cluster_server *dst_srv = NULL;
    st = cluster_transport_server_start(0, "127.0.0.1", NULL, 0, 0,
                                        dst_cat, NULL, NULL, &dst_srv);
    ASSERT_EQ(st, MDS_OK);
    uint16_t dst_port = cluster_transport_server_port(dst_srv);

    /* Create membership with MDS 2 at 127.0.0.1:dst_port. */
    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.self.id = 1;
    snprintf(cfg.self.hostname, sizeof(cfg.self.hostname), "localhost");
    cfg.self.nfs_port = 2049;
    cfg.self.grpc_port = 50051;

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

    /* Compound with membership set: rename should resolve on-demand. */
    struct compound_data cd;
    compound_init(&cd);
    cd.cat = cat;
    cd.smap = smap;
    cd.membership = cm;

    struct nfs4_op ops[6];
    struct nfs4_result results[6];
    ops[0] = mk_putrootfh();
    ops[1] = mk_lookup("local");
    ops[2] = mk_savefh();
    ops[3] = mk_putrootfh();
    ops[4] = mk_lookup("remote");
    ops[5] = mk_rename("data.txt", "data.txt");

    uint32_t n = compound_process(&cd, ops, results, 6);
    /* All ops should succeed -- rename resolves via membership. */
    ASSERT_EQ(n, 6);
    ASSERT_EQ(results[0].status, NFS4_OK);
    ASSERT_EQ(results[1].status, NFS4_OK);
    ASSERT_EQ(results[2].status, NFS4_OK);
    ASSERT_EQ(results[3].status, NFS4_OK);
    ASSERT_EQ(results[4].status, NFS4_OK);
    ASSERT_EQ(results[5].status, NFS4_OK);

    /* Cleanup. */
    cluster_transport_server_stop(dst_srv);
    cluster_membership_destroy(cm);
    subtree_map_destroy(smap);
    mds_catalogue_close(dst_db);
    cleanup_cc_dest_db();
    mds_catalogue_close(db);
    cleanup_db();
    PASS();
}

int main(void)
{
    fprintf(stdout, "test_compound_cluster:\n");

    test_putfh_junction_succeeds();
    test_lookup_junction_returns_moved();
    test_lookup_junction_getattr_fs_locations();
    test_rename_cross_subtree_local_to_remote_fastpath();
    test_rename_cross_subtree_membership();

    fprintf(stdout, "\n  %d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}

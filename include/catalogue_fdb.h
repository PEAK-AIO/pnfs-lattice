/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_fdb.h -- FoundationDB catalogue backend (catalogue_backend = fdb).
 *
 * Built into pnfs_mds_core when ENABLE_FDB is ON (HAVE_FDB=1).  The
 * backend is a set of translation units under src/catalogue/:
 *   catalogue_fdb.c         lifecycle: client network, open/close/probe/
 *                           bootstrap, process shutdown, vtable assembly
 *   catalogue_fdb_ns.c      namespace authority slots
 *   catalogue_fdb_ext.c     extended authority slots (inline, xattr,
 *                           stripe map, DS registry, quota, GC, delete
 *                           manifest, shard / ext_dirent / link_anchor)
 *   catalogue_fdb_coord.c   coordination slots (layout state, open /
 *                           lock / delegation write-through, client,
 *                           session, DRC slots, recovery, 2PC journal)
 *   catalogue_fdb_cluster.c cluster slots (node registry, partition map)
 *   fdb_keys.h              key space registry
 *   fdb_codec.[ch]          value formats
 *   fdb_txn.[ch]            transaction runner and commit-outcome protocol
 * The authority slots are filled through the *_register hooks declared
 * below; the coordination and cluster tables are exported constants.
 *
 * Process lifecycle.  The fdb_c client runs ONE network thread per
 * process and fdb_stop_network() is terminal.  The first open() starts
 * the network and registers catalogue_fdb_process_shutdown() with the
 * dispatcher (mds_catalogue_register_process_shutdown); every handle
 * counts against it.  mds_catalogue_process_shutdown() -- called once
 * by the daemon after the last close and by every test main through
 * conformance_shutdown() -- stops and joins the thread; it refuses
 * while handles are open and no open() succeeds afterwards.  A process
 * that exits without the call is covered by an atexit() safety net
 * that runs the same routine.
 */

#ifndef CATALOGUE_FDB_H
#define CATALOGUE_FDB_H

#include <stdint.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"

struct mds_config;
struct mds_authority_ops;
struct FDB_transaction;
struct fdb_key_prefix;

/** Schema stamp stored under META/FDB_META_SCHEMA_VERSION; open() and
 *  probe() refuse a keyspace stamped with any other value. */
#define FDB_CAT_SCHEMA_VERSION 2U

/**
 * Factory-shaped constructor for `catalogue_backend = fdb`.
 *
 * Connects to the cluster named by cfg->fdb_cluster_file (empty: the
 * FDB_CLUSTER_FILE environment variable, then
 * /etc/foundationdb/fdb.cluster), starts the client network on the
 * first open of the process, clears the witness keys of this MDS id's
 * dead incarnations and checks the schema stamp under
 * cfg->fdb_key_prefix.  An unbootstrapped keyspace opens successfully;
 * probe() then fails until bootstrap() has run (the daemon's startup
 * loop and the test harness both do that).
 *
 * @param cfg  Configuration (non-NULL).  cfg->self.id is the MDS id
 *             stamped on witness keys; cfg->fdb_op_deadline_ms and
 *             cfg->fdb_txn_timeout_ms take their defaults when 0.
 * @param out  Receives the handle (non-NULL); NULL on failure.
 * @return MDS_OK; MDS_ERR_INVAL on a NULL argument, a prefix longer
 *         than FDB_KEY_PREFIX_MAX, a schema stamp other than
 *         FDB_CAT_SCHEMA_VERSION, or after the process network was
 *         stopped; MDS_ERR_NOMEM; MDS_ERR_IO when the client network
 *         cannot start, the database handle cannot be created or the
 *         cluster does not answer within the operation deadline.
 */
enum mds_status catalogue_fdb_open(const struct mds_config *cfg,
                                   struct mds_catalogue **out);

/**
 * Per-backend process shutdown hook (registered with the dispatcher by
 * the first open).  Stops and joins the client network thread when no
 * handle is open; logs an error and leaves the network running when
 * handles are still open.  Idempotent.
 */
void catalogue_fdb_process_shutdown(void);

/**
 * Record the cluster registry boot epoch of this daemon on the handle
 * (rows that carry an owner epoch use it).  Witness keys do not depend
 * on it (see fdb_txn.h), so it may be set at any time.
 *
 * @return MDS_OK; MDS_ERR_INVAL when @p cat is not an fdb handle.
 */
enum mds_status fdb_backend_set_boot_epoch(const struct mds_catalogue *cat,
                                           uint64_t boot_epoch);

/**
 * Range-clear every key under the handle's prefix except the WITNESS
 * table (test and admin helper).  The live incarnation's witness and
 * fence-anchor rows must survive any clear that runs through the
 * transaction runner (fdb_txn.h, body rule); dead incarnations' rows
 * are swept by a later open of their mds_id (catalogue_fdb.c,
 * witness_clear_body).  Refuses an empty prefix: that would erase the
 * whole database.
 *
 * @return MDS_OK; MDS_ERR_INVAL for a NULL / non-fdb handle or an empty
 *         prefix; the transaction runner's status otherwise.
 */
enum mds_status catalogue_fdb_keyspace_clear(const struct mds_catalogue *cat);

/* -----------------------------------------------------------------------
 * Backend-internal (shared between the backend's translation units)
 * ----------------------------------------------------------------------- */

/**
 * Fill the namespace slots of @p ops (catalogue_fdb_ns.c).  Called once
 * by catalogue_fdb.c while assembling the authority table, before
 * catalogue_fdb_ext_register().
 */
void catalogue_fdb_ns_register(struct mds_authority_ops *ops);

/**
 * Write @p ino as one logical inode: the blob and, for a directory,
 * the four side keys (fdb_keys.h INODE layout).  Used by inode_put,
 * create paths and the bootstrap root.
 *
 * @return 0, or an fdb_error_t-compatible code for a body to return
 *         (FDB_ERR_PLATFORM_ERROR when the inode cannot be encoded).
 */
int catalogue_fdb_inode_write(struct FDB_transaction *tr, const struct fdb_key_prefix *p,
                              const struct mds_inode *ino);

/**
 * Fill the extended authority slots of @p ops (catalogue_fdb_ext.c:
 * inline data, xattrs, stripe maps, DS registry and provisioning,
 * quota, GC queue, delete manifest, shard routing, cross-shard dirents,
 * link anchors).  Called once by catalogue_fdb.c after
 * catalogue_fdb_ns_register(); the prealloc pool slots are left
 * untouched (NULL) and backend_client_stats is filled by
 * catalogue_fdb.c from the transaction runner's counters.
 */
void catalogue_fdb_ext_register(struct mds_authority_ops *ops);

/* --- fdb-coord track: coordination and cluster tables ------------------- */

struct mds_coordination_ops;
struct mds_cluster_ops;

/** Coordination slots (catalogue_fdb_coord.c); installed as
 *  cat->coord_ops by catalogue_fdb_open(). */
extern const struct mds_coordination_ops fdb_coordination_ops;

/** Cluster slots (catalogue_fdb_cluster.c); installed as
 *  cat->cluster_ops by catalogue_fdb_open().  Every slot, partition_cas
 *  included, is reached through the mds_cluster_* dispatchers. */
extern const struct mds_cluster_ops fdb_cluster_ops;

#endif /* CATALOGUE_FDB_H */

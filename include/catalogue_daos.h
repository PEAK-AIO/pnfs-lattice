/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_daos.h -- DAOS catalogue backend (catalogue_backend = daos).
 *
 * Built into pnfs_mds_core when ENABLE_DAOS is ON (HAVE_DAOS=1).  The
 * backend is a set of translation units under src/catalogue/:
 *   catalogue_daos.c         lifecycle: library init, pool / container
 *                            handles, container attribute stamps, probe,
 *                            bootstrap, process shutdown, vtable assembly
 *   catalogue_daos_ns.c      namespace authority slots and the shared
 *                            inode / dirent / shard / marker primitives
 *   catalogue_daos_ext.c     extended authority slots (inline, xattr,
 *                            stripe map, DS registry, quota, GC, delete
 *                            manifest, shard / ext_dirent / link_anchor)
 *   catalogue_daos_coord.c   coordination slots (layout state, open /
 *                            lock / delegation write-through, client,
 *                            session, DRC slots, recovery, 2PC journal)
 *   catalogue_daos_cluster.c cluster slots (node registry, partition map)
 *   daos_keys.h              object model and key space
 *   daos_codec.[ch]          DAOS-only value formats (fdb_codec.[ch] holds
 *                            the rest)
 *   daos_txn.[ch]            transaction runner, event-queue pool, body
 *                            helpers, id allocators
 * The authority slots are filled through the *_register hooks declared
 * below; the coordination and cluster tables are exported constants.
 *
 * Process lifecycle.  libdaos is initialised once per process by the
 * first open() (daos_init is reference counted and may be called again
 * after daos_fini, so a later open after a full shutdown works).  Every
 * handle counts against it; the first open registers
 * catalogue_daos_process_shutdown() with the dispatcher
 * (mds_catalogue_register_process_shutdown) and as an atexit() safety
 * net.  mds_catalogue_process_shutdown() -- called once by the daemon
 * after the last close and by every test main through
 * conformance_shutdown() -- runs daos_fini when no handle is open and
 * refuses (logs) while handles are open.
 *
 * One DAOS container is one catalogue.  Both object class names, their
 * resolved encodings, the directory counter shard count and the schema
 * version are stamped as container attributes by bootstrap and verified
 * at open BEFORE any object is touched: the class is encoded in every
 * object id, so a mismatch would address a second, empty namespace.
 */

#ifndef CATALOGUE_DAOS_H
#define CATALOGUE_DAOS_H

#include <stdint.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"

struct mds_config;
struct mds_authority_ops;
struct mds_coordination_ops;
struct mds_cluster_ops;

/** Schema stamp (container attribute lattice.schema and META "schema");
 *  open(), probe() and bootstrap() refuse any other value. */
#define DAOS_CAT_SCHEMA_VERSION 1U

/**
 * Factory-shaped constructor for `catalogue_backend = daos`.
 *
 * Initialises libdaos on the first open of the process, connects to
 * cfg->daos_pool in the system cfg->daos_sys (empty: "daos_server"),
 * opens cfg->daos_cont read-write and verifies the container's stamps
 * against the configuration (cfg->daos_oclass, default RP_3G1;
 * cfg->daos_oclass_spread, default RP_3GX; cfg->daos_dir_counter_shards,
 * default 16; the schema version).  A container without stamps opens
 * successfully; probe() then fails until bootstrap() has run (the
 * daemon's startup loop and the test harness both do that).
 *
 * @param cfg  Configuration (non-NULL).  cfg->self.id is the MDS id
 *             stamped on GC and recovery index rows; cfg->worker_threads
 *             sizes the event-queue pool; cfg->daos_op_deadline_ms takes
 *             its default (8000) when 0.
 * @param out  Receives the handle (non-NULL); NULL on failure.
 * @return MDS_OK; MDS_ERR_INVAL on a NULL argument, an empty pool or
 *         container, an unknown class name, a stamp that does not match
 *         the configuration or a schema other than
 *         DAOS_CAT_SCHEMA_VERSION; MDS_ERR_NOMEM; MDS_ERR_IO when the
 *         library cannot initialise, the pool or container cannot be
 *         opened or the attributes cannot be read.
 */
enum mds_status catalogue_daos_open(const struct mds_config *cfg, struct mds_catalogue **out);

/**
 * Per-backend process shutdown hook (registered with the dispatcher by
 * the first open).  Finalises libdaos when no handle is open; logs an
 * error and leaves it initialised while handles are still open.
 * Idempotent.
 */
void catalogue_daos_process_shutdown(void);

/**
 * Record the cluster registry boot epoch of this daemon on the handle
 * (rows that carry an owner epoch use it).  May be set at any time.
 *
 * @return MDS_OK; MDS_ERR_INVAL when @p cat is not a daos handle.
 */
enum mds_status daos_backend_set_boot_epoch(const struct mds_catalogue *cat,
                                            uint64_t boot_epoch);

/* -----------------------------------------------------------------------
 * Backend-internal (shared between the backend's translation units)
 * ----------------------------------------------------------------------- */

/**
 * Fill the namespace slots of @p ops (catalogue_daos_ns.c).  Called once
 * by catalogue_daos.c while assembling the authority table, before
 * catalogue_daos_ext_register().
 */
void catalogue_daos_ns_register(struct mds_authority_ops *ops);

/**
 * Fill the extended authority slots of @p ops (catalogue_daos_ext.c:
 * inline data, xattrs, stripe maps, DS registry and provisioning, quota,
 * GC queue, delete manifest, shard routing, cross-shard dirents, link
 * anchors).  Called once by catalogue_daos.c after
 * catalogue_daos_ns_register(); the prealloc pool slots are left
 * untouched (NULL) and backend_client_stats is filled by
 * catalogue_daos.c from the transaction runner's counters.
 */
void catalogue_daos_ext_register(struct mds_authority_ops *ops);

/** Coordination slots (catalogue_daos_coord.c); installed as
 *  cat->coord_ops by catalogue_daos_open(). */
extern const struct mds_coordination_ops daos_coordination_ops;

/** Cluster slots (catalogue_daos_cluster.c); installed as
 *  cat->cluster_ops by catalogue_daos_open().  Every slot, partition_cas
 *  included, is reached through the mds_cluster_* dispatchers. */
extern const struct mds_cluster_ops daos_cluster_ops;

#endif /* CATALOGUE_DAOS_H */

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_cluster.h -- Backend-neutral cluster services interface.
 *
 * Multi-MDS operation needs four services from the catalogue store
 * besides the catalogue data itself: a node registry (which MDS is
 * alive, where, and since which boot epoch), a heartbeat on that
 * registry, a stale-peer scan for the failover watchdog, and the
 * partition map (which MDS owns which namespace subtree).  This header
 * is the backend-neutral surface for those services.  A backend
 * implements them through struct mds_cluster_ops (catalogue_internal.h)
 * and the dispatchers below forward to it.
 *
 * Every slot is optional.  A backend that leaves a slot NULL makes the
 * matching dispatcher return MDS_ERR_NOSUPPORT, and every status a slot
 * does return reaches the caller unchanged.  mds_cluster_supported()
 * tells the daemon whether the store can carry a multi-process cluster
 * at all; an in-process store may populate the slots for tests without
 * ever claiming that.
 *
 * Contract notes shared by every slot:
 *   - A scan callback returning non-zero stops the scan; the scan then
 *     returns MDS_OK.
 *   - Callbacks may call back into the same catalogue handle (C1 in
 *     catalogue_internal.h), so a backend never holds a non-reentrant
 *     lock or an open retry body across a callback.
 *   - Rows are bounded by MDS_MAX_NODES (node registry) and the
 *     configured partition count, so every scan is small.
 */

#ifndef MDS_CLUSTER_H
#define MDS_CLUSTER_H

#include <stdbool.h>
#include <stdint.h>

#include "pnfs_mds.h"

struct mds_catalogue;

/* -----------------------------------------------------------------------
 * Partition (subtree) ownership state, as stored in the partition map.
 *
 * The values are the row encoding every backend must use so that a map
 * written by one backend reads back identically through another (the
 * RonDB column encoding RONDB_PM_STATE_* is pinned to these values by
 * _Static_assert in catalogue_rondb.c).
 * ----------------------------------------------------------------------- */

#define MDS_PARTITION_STATE_ACTIVE     0  /**< Owner serves the subtree. */
#define MDS_PARTITION_STATE_MIGRATING  1  /**< Ownership moving; writes held. */
#define MDS_PARTITION_STATE_FROZEN     2  /**< Subtree frozen (no mutation). */

/* -----------------------------------------------------------------------
 * Callback types
 * ----------------------------------------------------------------------- */

/**
 * Node-registry row callback for mds_cluster_node_list().
 *
 * @param mds_id             Registered MDS id (1..MDS_MAX_NODES).
 * @param boot_epoch         Incarnation of that MDS (see node_register).
 * @param hostname           NUL-terminated hostname or address; valid
 *                           only for the duration of the callback.
 * @param nfs_port           NFS listener port advertised by the node.
 * @param grpc_port          Inter-MDS control port advertised by the node.
 * @param last_heartbeat_ns  Timestamp of the node's last heartbeat.
 * @param ctx                Caller context.
 * @return 0 to continue, non-zero to stop the scan.
 */
typedef int (*mds_cluster_node_cb)(uint32_t mds_id, uint64_t boot_epoch,
                                   const char *hostname,
                                   uint16_t nfs_port, uint16_t grpc_port,
                                   uint64_t last_heartbeat_ns, void *ctx);

/**
 * Stale-node callback for mds_cluster_node_scan_stale().
 *
 * @param mds_id             Registered MDS id whose heartbeat is stale.
 * @param boot_epoch         Incarnation recorded in the registry row.
 * @param last_heartbeat_ns  The stale heartbeat timestamp.
 * @param ctx                Caller context.
 * @return 0 to continue, non-zero to stop the scan.
 */
typedef int (*mds_cluster_stale_cb)(uint32_t mds_id, uint64_t boot_epoch,
                                    uint64_t last_heartbeat_ns, void *ctx);

/**
 * Partition-map row callback for mds_cluster_partition_list().
 *
 * @param partition_id  Partition (subtree) id.
 * @param owner_mds_id  MDS that owns the subtree.
 * @param state         One of MDS_PARTITION_STATE_*.
 * @param subtree_path  NUL-terminated absolute path of the subtree root;
 *                      valid only for the duration of the callback.
 * @param ctx           Caller context.
 * @return 0 to continue, non-zero to stop the scan.
 */
typedef int (*mds_cluster_partition_cb)(uint32_t partition_id,
                                        uint32_t owner_mds_id,
                                        uint8_t state,
                                        const char *subtree_path,
                                        void *ctx);

/* -----------------------------------------------------------------------
 * Node registry
 *
 * One row per mds_id.  boot_epoch identifies one incarnation of an MDS
 * and must be monotonic across restarts of the same mds_id; it is what
 * lets the store tell a restarted node from a stale one.  The daemon
 * derives it from CLOCK_REALTIME nanoseconds at startup (main.c), so
 * it stays monotonic across host reboots as long as the wall clock is
 * not stepped back below the previous incarnation's value; when it is,
 * node_register refuses with MDS_ERR_EXISTS and the daemon exits with
 * a message naming the row and both epochs rather than continuing
 * unregistered.
 *
 * Contract (binding on every backend; RonDB and memdb implement it):
 *
 *   node_register    conditional upsert: insert when absent; replace
 *                    when the existing row's boot_epoch is lower;
 *                    MDS_ERR_EXISTS when a row with an equal or higher
 *                    boot_epoch exists (a duplicate live mds_id is
 *                    split-brain and is refused).
 *   node_heartbeat   update the heartbeat timestamp only when the row
 *                    exists AND its boot_epoch equals @boot_epoch;
 *                    MDS_ERR_NOTFOUND when absent; MDS_ERR_STALE on an
 *                    epoch mismatch (an old incarnation stops
 *                    heartbeating and never overwrites its replacement).
 *                    One conditional write, no read-before-write.
 *   node_deregister  delete only when the row's boot_epoch equals
 *                    @boot_epoch; MDS_OK when the row is already absent
 *                    (a retried shutdown is harmless); MDS_ERR_STALE on
 *                    an epoch mismatch, nothing deleted.
 *   timestamps       the writer's CLOCK_REALTIME in nanoseconds, so the
 *                    watchdog's threshold (its own CLOCK_REALTIME) is in
 *                    the same clock domain across hosts; the stale
 *                    threshold (15 s default) must therefore exceed the
 *                    deployment's NTP skew bound.
 *
 * Consumer rule for last_heartbeat_ns (rolling upgrade): RonDB writers
 * that predate this contract stamped CLOCK_MONOTONIC, a value that can
 * never reach FAILOVER_HB_REALTIME_FLOOR_NS (failover_watchdog.h,
 * 2020-01-01 UTC) in the realtime domain.  A row below that floor is
 * INDETERMINATE: the failover watchdog skips its tick for that partner
 * and never declares it stale, so an upgraded standby cannot promote
 * against a not-yet-upgraded primary; the node_list consumer
 * (cluster_membership_populate) does not interpret the timestamp at
 * all.  The store applies no such rule -- it reports rows below the
 * threshold as asked.
 * ----------------------------------------------------------------------- */

/**
 * Register this MDS incarnation in the node registry.
 *
 * @param cat         Catalogue handle.
 * @param mds_id      This MDS's id.
 * @param boot_epoch  This incarnation's boot epoch.
 * @param hostname    Address peers use to reach this node (non-NULL).
 * @param nfs_port    NFS listener port.
 * @param grpc_port   Inter-MDS control port.
 * @return MDS_OK; MDS_ERR_EXISTS per the contract above;
 *         MDS_ERR_NOSUPPORT when the backend has no registry;
 *         MDS_ERR_INVAL on a NULL handle or hostname; or the backend's
 *         status unchanged.
 */
enum mds_status mds_cluster_node_register(struct mds_catalogue *cat,
                                          uint32_t mds_id,
                                          uint64_t boot_epoch,
                                          const char *hostname,
                                          uint16_t nfs_port,
                                          uint16_t grpc_port);

/**
 * Refresh this incarnation's heartbeat timestamp.
 *
 * @return MDS_OK; MDS_ERR_NOTFOUND when no row exists for @mds_id;
 *         MDS_ERR_STALE on a boot_epoch mismatch;
 *         MDS_ERR_NOSUPPORT when the backend has no registry;
 *         MDS_ERR_INVAL on a NULL handle; or the backend's status.
 */
enum mds_status mds_cluster_node_heartbeat(struct mds_catalogue *cat,
                                           uint32_t mds_id,
                                           uint64_t boot_epoch);

/**
 * Remove this incarnation's registry row on clean shutdown.
 *
 * @return MDS_OK (also when the row is already absent);
 *         MDS_ERR_STALE on a boot_epoch mismatch (nothing deleted);
 *         MDS_ERR_NOSUPPORT when the backend has no registry;
 *         MDS_ERR_INVAL on a NULL handle; or the backend's status.
 */
enum mds_status mds_cluster_node_deregister(struct mds_catalogue *cat,
                                            uint32_t mds_id,
                                            uint64_t boot_epoch);

/**
 * Enumerate every node-registry row.
 *
 * @return MDS_OK (including an early stop by the callback);
 *         MDS_ERR_NOSUPPORT when the backend has no registry;
 *         MDS_ERR_INVAL on a NULL handle or callback; or the backend's
 *         status.
 */
enum mds_status mds_cluster_node_list(struct mds_catalogue *cat,
                                      mds_cluster_node_cb cb, void *ctx);

/**
 * Report every registry row whose last_heartbeat_ns is older than
 * @threshold_ns (same clock domain as the heartbeat writer).
 *
 * @return MDS_OK (including an early stop by the callback);
 *         MDS_ERR_NOSUPPORT when the backend has no registry;
 *         MDS_ERR_INVAL on a NULL handle or callback; or the backend's
 *         status.
 */
enum mds_status mds_cluster_node_scan_stale(struct mds_catalogue *cat,
                                            uint64_t threshold_ns,
                                            mds_cluster_stale_cb cb,
                                            void *ctx);

/* -----------------------------------------------------------------------
 * Partition map
 *
 * One row per partition_id: (owner_mds_id, state, subtree_path).
 *
 * Contract (RonDB and memdb implement it): partition_put with
 * insert_only == true inserts the row only when @partition_id is absent
 * and returns MDS_ERR_EXISTS otherwise (the root claim at startup, so a
 * transient error can never rewrite the real owner); insert_only ==
 * false is an upsert and is reserved for seeding a never-owned initial
 * shard layout.  partition_cas moves ownership of an EXISTING row and
 * is the only way an owned row changes hands: the write lands only
 * while the row's owner still equals the caller's expected owner
 * (MDS_ERR_STALE otherwise, MDS_ERR_NOTFOUND for an absent row, nothing
 * written in either case), so two standbys promoting against the same
 * dead primary cannot both believe they own its partitions, and a
 * takeover the store refused is never held in memory.  A failed
 * partition_list at startup is fatal for the caller after a bounded
 * retry (subtree_map_init_from_catalogue), never "empty map".
 *
 * Pre-seeded root.  A store MAY create partition 0 = (owner 0, "/")
 * when it bootstraps its schema; owner 0 means "unowned: every head
 * serves root" (compound.c never gates owner 0).  A node starting on
 * such a store finds root in partition_list and never claims it; if
 * two nodes race the claim on a store that does not seed, the loser's
 * insert-only put sees MDS_ERR_EXISTS and adopts the winner's row --
 * the claim path exists for stores that do not seed.  RonDB seeds
 * (rondb_shim_bootstrap_metadata), so on RonDB root is always unowned
 * and only the /shardN rows carry ownership; memdb does not seed, so
 * the first opener of a memdb instance owns root.  The two therefore
 * differ in who owns "/": deployed semantics on RonDB, in-process
 * test semantics on memdb.
 * ----------------------------------------------------------------------- */

/**
 * Enumerate every partition-map row.
 *
 * @return MDS_OK (including an early stop by the callback);
 *         MDS_ERR_NOSUPPORT when the backend has no partition map;
 *         MDS_ERR_INVAL on a NULL handle or callback; or the backend's
 *         status.
 */
enum mds_status mds_cluster_partition_list(struct mds_catalogue *cat,
                                           mds_cluster_partition_cb cb,
                                           void *ctx);

/**
 * Write one partition-map row.
 *
 * @param cat           Catalogue handle.
 * @param partition_id  Partition id (row key).
 * @param owner_mds_id  Owning MDS.
 * @param state         One of MDS_PARTITION_STATE_*.
 * @param subtree_path  Absolute subtree root path (non-NULL).
 * @param insert_only   true: fail with MDS_ERR_EXISTS when the row
 *                      exists; false: upsert.
 * @return MDS_OK; MDS_ERR_EXISTS per @insert_only; MDS_ERR_NOSUPPORT
 *         when the backend has no partition map; MDS_ERR_INVAL on a
 *         NULL handle or path; or the backend's status.
 */
enum mds_status mds_cluster_partition_put(struct mds_catalogue *cat,
                                          uint32_t partition_id,
                                          uint32_t owner_mds_id,
                                          uint8_t state,
                                          const char *subtree_path,
                                          bool insert_only);

/**
 * Compare-and-swap the owner of one partition-map row.
 *
 * @param cat             Catalogue handle.
 * @param partition_id    Partition id (row key).
 * @param expected_owner  Owner the row must currently record.
 * @param new_owner       Owner to record.
 * @param new_state       One of MDS_PARTITION_STATE_*, written with it.
 * @return MDS_OK when the row was rewritten; MDS_ERR_NOTFOUND when no
 *         row exists for @partition_id; MDS_ERR_STALE when the row's
 *         owner is not @expected_owner (nothing written);
 *         MDS_ERR_NOSUPPORT when the backend has no CAS; MDS_ERR_INVAL
 *         on a NULL handle; or the backend's status.
 */
enum mds_status mds_cluster_partition_cas(struct mds_catalogue *cat,
                                          uint32_t partition_id,
                                          uint32_t expected_owner,
                                          uint32_t new_owner,
                                          uint8_t new_state);

/**
 * True when the store can carry a multi-process MDS cluster: the
 * node_register, node_heartbeat, node_list, partition_list and
 * partition_put slots are all present AND the handle carries
 * MDS_CAT_CAP_MULTI_PROCESS (the store is reachable from more than one
 * process).  An in-process store that populates the slots for tests
 * never sets the capability, so the daemon refuses cluster_size > 1 on
 * it by construction.  False for a NULL handle.
 *
 * This predicate asserts the presence of the cluster services only; it
 * says nothing about cross-MDS OPEN/LOCK exclusivity.
 */
bool mds_cluster_supported(const struct mds_catalogue *cat);

/**
 * True when the backend implements node_scan_stale -- the failover
 * watchdog's only catalogue dependency (failover_watchdog_start
 * refuses with MDS_ERR_NOSUPPORT when this is false).  Unlike
 * mds_cluster_supported() this does not require
 * MDS_CAT_CAP_MULTI_PROCESS, so an in-process store that populates
 * the slot for tests can exercise the watchdog.  False for a NULL
 * handle.
 */
bool mds_cluster_stale_scan_supported(const struct mds_catalogue *cat);

#endif /* MDS_CLUSTER_H */

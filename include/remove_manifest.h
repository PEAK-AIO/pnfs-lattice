/*
 * Copyright (c) 2026 PeakAIO. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-PeakAIO-Proprietary
 *
 * remove_manifest.h — Async-REMOVE delete manifest (mds.conf
 * `remove_async`, RonDB schema v18).
 *
 * Design summary
 * --------------
 * op_remove's synchronous cost floor is the ns_remove NDB transaction
 * (~0.65 ms measured on the lab).  With `remove_async = true`,
 * eligible REMOVEs are acked after a much cheaper durable step:
 *
 *   1. an in-memory TOMBSTONE keyed (dir_fileid, name) hides the
 *      dirent from every read path (LOOKUP/OPEN/READDIR) immediately;
 *   2. one mds_remove_pending row (single-row NDB insert, ~0.3 ms)
 *      records the pending delete durably — RonDB replication makes
 *      it rack-safe and any-MDS resumable;
 *   3. the reply is sent; the manifest DRAINER executes the real
 *      guarded ns_remove + final-unlink cleanup off the request
 *      thread (batched claims, ownership leases, crash re-claim —
 *      exactly the mds_unlink_pending drainer pattern).
 *
 * Because the manifest row commits BEFORE the client is acked, an
 * MDS crash never resurrects an acknowledged remove: the row is
 * re-loaded into tombstones at startup (remove_manifest_load) and
 * drained by this or any peer MDS.
 *
 * Invariants
 * ----------
 *   1. A tombstone exists for every manifest row this node serves
 *      reads for (submit inserts the tombstone first; load rebuilds
 *      them before RPC listeners start).
 *   2. Exactly one executor per entry: the PENDING -> DRAINING
 *      transition under the stripe lock elects either a drainer
 *      worker or a force-drain caller; the loser skips or waits.
 *   3. Mutations that would collide with a pending delete
 *      (CREATE/RENAME onto the name, RMDIR of the parent) must call
 *      the force-drain helpers first, so the catalogue never sees a
 *      double dirent and RMDIR emptiness stays truthful.
 *   4. destroy() drains every remaining tombstone synchronously —
 *      a graceful shutdown leaves no pending manifest work behind.
 *
 * v1 deployment gate (enforced by main.c, not here): single-MDS or
 * strict-subtree-sharded clusters only, same convention as
 * inode_cache_multi_mds_unsafe — tombstone visibility is node-local.
 */

#ifndef REMOVE_MANIFEST_H
#define REMOVE_MANIFEST_H

#include <stdbool.h>
#include <stdint.h>

struct mds_catalogue;
struct mds_proxy_ctx;
struct layout_cache;
struct layout_commit_aggregator;
struct mds_quota_ctx;
struct remove_manifest;

/* Hard caps on the configurable knobs (style of deferred_unlink.h). */
#define REMOVE_MANIFEST_MAX_WORKERS   32U
#define REMOVE_MANIFEST_MAX_BATCH   4096U
#define REMOVE_MANIFEST_MAX_PENDING (1U << 20)

/**
 * @brief Initialise the manifest module (tombstone table + drainer).
 *
 * Fails with -1 when the catalogue backend lacks the
 * mds_remove_pending ops (MDS_ERR_NOSUPPORT probe) or on allocation
 * failure; *out is NULL in that case and the caller must leave
 * `remove_async` disabled.
 *
 * Threads are NOT started here — call remove_manifest_load() first
 * (before RPC listeners), then remove_manifest_start().
 *
 * @param cat          Catalogue handle (borrowed; must outlive destroy).
 * @param lcache       Layout cache (may be NULL).
 * @param lcommit_agg  LAYOUTCOMMIT aggregator (may be NULL).
 * @param quota        Quota ctx for drain-time accounting (may be NULL).
 * @param mds_id       This node's MDS id (claim identity).
 * @param boot_epoch   This node's boot epoch (claim identity).
 * @param max_pending  Tombstone-table capacity; submit falls back to
 *                     the synchronous path beyond it.  Clamped to
 *                     REMOVE_MANIFEST_MAX_PENDING.
 * @param workers      Drainer worker threads (>= 1; clamped).
 * @param batch_size   Coordinator claim batch (clamped).
 * @param poll_ms      Coordinator poll interval (>= 10 enforced).
 * @param claim_ttl_ns Ownership-lease duration for claimed rows.
 * @param[out] out     Receives the handle.
 * @return 0 on success, -1 on failure (*out == NULL).
 */
int remove_manifest_init(struct mds_catalogue *cat,
			 struct mds_proxy_ctx *proxy,
			 struct layout_cache *lcache,
			 struct layout_commit_aggregator *lcommit_agg,
			 struct mds_quota_ctx *quota,
			 uint32_t mds_id, uint64_t boot_epoch,
			 uint32_t max_pending,
			 uint32_t workers, uint32_t batch_size,
			 uint32_t poll_ms, uint64_t claim_ttl_ns,
			 struct remove_manifest **out);

/**
 * @brief Load every persisted manifest row into the tombstone table.
 *
 * MUST run after the catalogue is ready and BEFORE RPC listeners
 * start serving compounds, so a restart cannot expose acked-removed
 * names.  Returns the number of rows loaded, or -1 on backend error.
 */
int remove_manifest_load(struct remove_manifest *rm);

/** @brief Start the drainer threads (after load). 0 on success. */
int remove_manifest_start(struct remove_manifest *rm);

/**
 * @brief Stop the drainer, synchronously drain every remaining
 *        tombstone (graceful-shutdown flush), and free the handle.
 *
 * Must be invoked BEFORE the catalogue / caches / quota ctx given at
 * init time are torn down.  NULL is tolerated.
 */
void remove_manifest_destroy(struct remove_manifest *rm);

/**
 * @brief Ack-path submission (op_remove fast path).
 *
 * Inserts the tombstone, then commits the manifest row
 * (durable-before-ack).  On any failure the tombstone is rolled back
 * and -1 is returned — the caller MUST fall through to the
 * synchronous remove path so the REMOVE never silently drops.
 *
 * @return 0 accepted (caller replies NFS4_OK), -1 fall back.
 */
int remove_manifest_submit(struct remove_manifest *rm,
			   uint64_t dir_fileid, const char *name,
			   uint64_t child_fileid,
			   uint64_t child_generation,
			   bool unlink_at_ack);

/**
 * @brief Read-path filter: true when (dir_fileid, name) has a
 *        pending (acked but not yet executed) remove.
 *
 * O(1); safe from any request thread.  NULL rm returns false.
 */
bool remove_manifest_is_tombstoned(struct remove_manifest *rm,
				   uint64_t dir_fileid,
				   const char *name);

/**
 * @brief Synchronously execute (or wait out) the pending remove of
 *        one name, so a CREATE / RENAME-target / LINK-target on that
 *        name can proceed against a clean catalogue.
 *
 * @return 0 when no entry remains for the name (executed, already
 *         drained, or never present); -1 on hard failure or wait
 *         timeout — the caller should surface NFS4ERR_DELAY.
 */
/*
 * Reconcile in-memory tombstones against manifest rows: entries in
 * PENDING state older than min_age_ns whose row no longer exists
 * (a peer drained it) are dropped; rows that still exist are
 * claimed and drained inline.  Returns the number of stale
 * tombstones dropped.  Called periodically by the coordinator;
 * exposed for tests.
 */
int remove_manifest_scrub_orphans(struct remove_manifest *rm,
				  uint64_t min_age_ns);

int remove_manifest_force_drain_entry(struct remove_manifest *rm,
				      uint64_t dir_fileid,
				      const char *name);

/**
 * @brief Synchronously drain every pending remove under one
 *        directory (RMDIR emptiness-check pre-pass).
 *
 * @return 0 when the directory has no pending entries left,
 *         -1 on hard failure / timeout (caller: NFS4ERR_DELAY).
 */
int remove_manifest_force_drain_dir(struct remove_manifest *rm,
				    uint64_t dir_fileid);

/** @brief Current tombstone count (depth gauge). NULL-safe (0). */
uint32_t remove_manifest_pending(const struct remove_manifest *rm);

#endif /* REMOVE_MANIFEST_H */

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * failover.h — Standby promotion for MDS failover.
 *
 * When the primary MDS fails, the standby (which has been receiving
 * txn_delta replicas) promotes itself by taking over the primary's
 * subtrees and entering a grace period.
 *
 * Promotion follows a strict two-phase ordering:
 *   1. Internal PROMOTING state
 *   2. Self-fencing guard + replication health gate
 *   3. detect_cb confirmation (if non-NULL)
 *   4. Transfer subtree ownership (authoritative CAS)
 *   5. Load client recovery records
 *   6. Enter grace period
 *   7. Publish self as ACTIVE+ACTIVE_SERVING
 *   8. Transition to PRIMARY role
 *
 * Demotion/failback is NOT implemented in this version — it requires
 * an explicit failback protocol with resync guarantees.
 */

#ifndef FAILOVER_H
#define FAILOVER_H

#include <stdint.h>
#include <stdbool.h>
#include "pnfs_mds.h"

struct subtree_map;
struct mds_catalogue;
struct repl_conn;
struct cluster_membership;
struct health_monitor;

/* -----------------------------------------------------------------------
 * Failover role
 * ----------------------------------------------------------------------- */

enum failover_role {
    FAILOVER_STANDBY   = 0,
    FAILOVER_PROMOTING = 1,
    FAILOVER_PRIMARY   = 2,
    FAILOVER_DEMOTING  = 3,
};

/* -----------------------------------------------------------------------
 * Failover configuration
 * ----------------------------------------------------------------------- */

/**
 * Configuration for failover_init().
 *
 * detect_cb: returns 0 if partner alive, non-zero if dead.
 *   - In authoritative (etcd) mode: may be NULL — the watch-driven
 *     partner-loss callback triggers promotion directly.
 *   - In local/test mode: injectable callback for deterministic control.
 *
 * membership: required for promote_standby() and self-fencing checks.
 *             May be NULL only in legacy test configurations.
 *
 * hm: health monitor for replication health gate.  May be NULL
 *     (promotion proceeds without replication check).
 */
struct failover_cfg {
    uint32_t                  self_id;
    uint32_t                  partner_id;
    struct subtree_map       *map;
    struct mds_catalogue     *cat;
    uint32_t                  grace_period_sec;
    int                     (*detect_cb)(uint32_t partner_id, void *arg);
    void                     *detect_arg;
    struct cluster_membership *membership;
    struct health_monitor    *hm;
};

/* -----------------------------------------------------------------------
 * Failover context (opaque)
 * ----------------------------------------------------------------------- */

struct failover_ctx;

/* -----------------------------------------------------------------------
 * API
 * ----------------------------------------------------------------------- */

/**
 * Initialise failover context.
 *
 * detect_cb may be NULL (authoritative mode).  When NULL, the caller
 * is responsible for triggering promotion via failover_promote()
 * from the partner-loss watch callback.
 *
 * @param cfg  Configuration (copied internally).
 * @param out  Receives allocated context.
 * @return MDS_OK on success, MDS_ERR_INVAL/MDS_ERR_NOMEM on failure.
 */
enum mds_status failover_init(const struct failover_cfg *cfg,
                              struct failover_ctx **out);

/**
 * Promote this standby to primary.
 *
 * Two-phase procedure (see header comment for ordering).
 * Returns MDS_OK on success.  Named error codes:
 *   MDS_ERR_PERM  — not in STANDBY state, self-fenced,
 *                    replication unhealthy, or partner alive.
 *   MDS_ERR_IO    — subtree takeover or recovery load failed.
 *
 * @param ctx  Failover context (must be in STANDBY role).
 * @return MDS_OK on success, or error code.
 */
enum mds_status failover_promote(struct failover_ctx *ctx);

/**
 * Controlled demotion: gracefully hand off the active role.
 *
 * Orchestrates: compat check -> quiesce (freeze subtrees) ->
 * replication sync barrier -> self-remove from membership.
 *
 * The membership entry is deleted while still ACTIVE + ACTIVE_SERVING
 * so that the standby's partner-loss watcher fires and triggers
 * promotion.  No lifecycle transitions (DRAINING/DRAINED) occur —
 * the freeze + barrier handle the actual quiescing.
 *
 * On failure (barrier timeout, compat mismatch, self-remove error),
 * subtrees are unfrozen and role is restored to PRIMARY.  No state
 * mutation occurs on compat rejection (MDS_ERR_PERM returned before
 * any changes).
 *
 * After success the node is ready to stop.  It does NOT remain as
 * an in-cluster standby in the same process.
 *
 * @param ctx   Failover context (must be PRIMARY role).
 * @param repl  Replication connection (must NOT be NULL).
 * @return MDS_OK on success, MDS_ERR_PERM (wrong role, compat mismatch,
 *         or NULL repl), MDS_ERR_IO (barrier or self-remove failure).
 */
enum mds_status failover_controlled_demote(struct failover_ctx *ctx,
                                           struct repl_conn *repl);

/**
 * Query the current failover role.
 */
enum failover_role failover_get_role(const struct failover_ctx *ctx);

/**
 * @brief Return the self MDS ID from a failover context.
 */
uint32_t failover_get_self_id(const struct failover_ctx *ctx);

/**
 * Destroy failover context and free resources.
 */
void failover_destroy(struct failover_ctx *ctx);

#endif /* FAILOVER_H */

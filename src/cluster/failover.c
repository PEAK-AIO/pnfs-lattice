/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * failover.c -- Standby promotion and failover logic.
 *
 * Two-phase promotion ordering (Seq 9):
 *   1. Set internal role -> PROMOTING
 *   2. Self-fencing guard + replication health gate
 *   3. detect_cb confirmation (if non-NULL)
 *   4. Transfer subtree ownership via failover_take_over; a takeover
 *      that wins no partition (another standby's CAS landed first)
 *      returns the node to STANDBY so its watchdog keeps observing
 *   5. Load the partner's client recovery records from the
 *      coordination backend and re-own them (best-effort)
 *   6. Enter grace period
 *   7. Publish self as ACTIVE + ACTIVE_SERVING via promote_standby
 *   8. Set internal role -> PRIMARY
 *
 * Demotion/failback is NOT implemented -- requires explicit failback
 * protocol with resync guarantees (see architecture.md SDeferred).
 */

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>

#include "pnfs_mds.h"
#include "failover.h"
#include "subtree_map.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "grace.h"
#include "cluster_membership.h"
#include "health.h"

/* -----------------------------------------------------------------------
 * Internal structure
 * ----------------------------------------------------------------------- */

struct failover_ctx {
    uint32_t           self_id;
    uint32_t           partner_id;
    enum failover_role role;
    struct subtree_map *map;
    struct mds_catalogue *cat;
    uint32_t           grace_period_sec;

    /* Detection callback (optional -- NULL in authoritative mode). */
    int              (*detect_cb)(uint32_t partner_id, void *arg);
    void              *detect_arg;

    /* Membership handle for promote_standby + self-fencing. */
    struct cluster_membership *membership;

    /* Health monitor for replication health gate. */
    struct health_monitor *hm;

    /* Record of taken-over subtrees for targeted rollback. */
    uint32_t           taken_count;
    uint32_t           taken_cap;
    char             (*taken_paths)[MDS_MAX_PATH];
};

/* -----------------------------------------------------------------------
 * Private helpers
 * ----------------------------------------------------------------------- */

/** Clear taken-paths state and free the array. */
static void clear_taken_state(struct failover_ctx *ctx)
{
    free(ctx->taken_paths);
    ctx->taken_paths = NULL;
    ctx->taken_count = 0;
    ctx->taken_cap   = 0;
}

/**
 * Pre-flight checks (phases 2--3): self-fencing, replication health,
 * and detect_cb.  Returns MDS_OK to proceed, or an error code.
 */
static enum mds_status promote_prechecks(struct failover_ctx *ctx)
{
    /* Phase 2a: Self-fencing guard. */
    if (ctx->membership != NULL &&
        cluster_membership_is_self_fenced(ctx->membership)) {
        return MDS_ERR_PERM;
    }

    /* Phase 2b: Replication health gate. */
    if (ctx->hm != NULL && !health_repl_is_ok(ctx->hm)) {
        return MDS_ERR_PERM;
    }

    /* Phase 3: Confirm partner is actually dead (if detect_cb set). */
    if (ctx->detect_cb != NULL &&
        ctx->detect_cb(ctx->partner_id, ctx->detect_arg) == 0) {
        return MDS_ERR_PERM;
    }

    /* Phase 3a (Item 46): Wire-compat check -- reject promotion if
     * self and partner have different wire_compat_version.  Safety
     * backstop even if operator bypasses CLI. */
    if (ctx->membership != NULL) {
        struct cluster_member self_m;
        struct cluster_member partner_m;
        if (cluster_membership_get(ctx->membership, ctx->self_id,
                                   &self_m) == MDS_OK &&
            cluster_membership_get(ctx->membership, ctx->partner_id,
                                   &partner_m) == MDS_OK) {
            if (self_m.wire_compat_version !=
                partner_m.wire_compat_version) {
                return MDS_ERR_PERM;
            }
        }
    }

    return MDS_OK;
}

/**
 * Snapshot paths currently owned by the partner so we can do a
 * targeted rollback on failure.  Allocates ctx->taken_paths.
 */
static enum mds_status snapshot_partner_paths(struct failover_ctx *ctx)
{
    uint32_t map_n = subtree_map_count(ctx->map);

    clear_taken_state(ctx);

    if (map_n == 0) {
        return MDS_OK;
    }

    ctx->taken_paths = calloc(map_n, sizeof(*ctx->taken_paths));
    if (ctx->taken_paths == NULL) {
        return MDS_ERR_NOMEM;
    }
    ctx->taken_cap = map_n;

    for (uint32_t i = 0; i < map_n; i++) {
        struct subtree_entry ent;
        if (subtree_map_get_entry(ctx->map, i, &ent) != MDS_OK) {
            continue;
        }
        if (ent.owner_mds_id == ctx->partner_id &&
            ctx->taken_count < ctx->taken_cap) {
            (void)snprintf(ctx->taken_paths[ctx->taken_count],
                     MDS_MAX_PATH, "%s", ent.path);
            ctx->taken_count++;
        }
    }

    return MDS_OK;
}

/** Roll back taken-over subtrees to the original partner owner --
 *  store first (the CAS that took them is reversed, expected = self),
 *  then memory, through the same transfer path as the takeover.  An
 *  entry the takeover skipped is still the partner's and answers
 *  STALE here, which is the intended no-op. */
static void rollback_taken_paths(struct failover_ctx *ctx)
{
    for (uint32_t i = 0; i < ctx->taken_count; i++) {
        enum mds_status st;

        st = subtree_map_failover_transfer(ctx->map, ctx->cat,
                                           ctx->taken_paths[i],
                                           ctx->self_id, ctx->partner_id);
        if (st != MDS_OK && st != MDS_ERR_STALE &&
            st != MDS_ERR_NOTFOUND) {
            MDS_LOG_WARN(LOG_COMP_CLUSTER,
                    "failover rollback: %s could not be returned to "
                    "MDS %u: %s", ctx->taken_paths[i],
                    (unsigned)ctx->partner_id, mds_status_str(st));
        }
    }
    clear_taken_state(ctx);
}

/** Phase 4b took no partition, so there is nothing to roll back: drop
 *  the snapshot and return to STANDBY.  MDS_ERR_STALE means the store
 *  gave the partner's partitions to another node -- that node is the
 *  new primary and this one must keep watching, not publish
 *  ACTIVE_SERVING over nothing -- and is passed through so the caller
 *  can tell a lost race from a store failure.  Every other status is
 *  reported as MDS_ERR_IO, as before. */
static enum mds_status promote_takeover_failed(struct failover_ctx *ctx,
                                               enum mds_status st)
{
    if (st == MDS_ERR_STALE) {
        MDS_LOG_INFO(LOG_COMP_CLUSTER,
                "failover: another node took over the partitions of "
                "MDS %u; staying standby", (unsigned)ctx->partner_id);
    } else {
        st = MDS_ERR_IO;
    }
    clear_taken_state(ctx);
    ctx->role = FAILOVER_STANDBY;
    return st;
}

struct failover_recovery_list_ctx {
    struct client_recovery_rec *recs;
    uint32_t count;
    uint32_t capacity;
    enum mds_status st;
};

static int failover_collect_recovery_cb(uint64_t clientid,
                                        uint32_t owner_mds_id,
                                        uint64_t owner_boot_epoch,
                                        void *arg)
{
    struct failover_recovery_list_ctx *ctx = arg;

    (void)owner_mds_id;
    (void)owner_boot_epoch;

    if (ctx == NULL) {
        return 1;
    }

    if (ctx->count >= ctx->capacity) {
        uint32_t new_cap = (ctx->capacity == 0) ? 16 : ctx->capacity * 2;
        void *tmp = realloc(ctx->recs,
                            (size_t)new_cap * sizeof(*ctx->recs));
        if (tmp == NULL) {
            ctx->st = MDS_ERR_NOMEM;
            return 1;
        }
        ctx->recs = tmp;
        ctx->capacity = new_cap;
    }

    memset(&ctx->recs[ctx->count], 0, sizeof(ctx->recs[ctx->count]));
    ctx->recs[ctx->count].clientid = clientid;
    ctx->count++;
    return 0;
}

/**
 * Re-own the partner's recovery rows.
 *
 * Recovery rows record the MDS that served the client, and
 * mds_coord_recovery_list(owner) returns only that owner's rows.  The
 * grace-period EXCHANGE_ID matching in session.c lists the rows owned
 * by THIS node, so after promotion the partner's clients would never
 * be matched to their old clientids (and CLAIM_PREVIOUS would fail)
 * unless their rows are re-owned.  Rewriting a row through this node's
 * catalogue handle stamps this node as its owner; the payload is
 * unchanged.  Exceptional path only (one read + one write per client
 * at promotion), best-effort: a row that cannot be adopted is left to
 * the partner's ownership and its client simply gets a fresh clientid,
 * exactly as before this step existed.
 *
 * @return Number of rows that could NOT be adopted.
 */
static uint32_t failover_adopt_recovery_rows(
    struct mds_catalogue *cat,
    const struct client_recovery_rec *recs, uint32_t count)
{
    uint32_t failed = 0;

    for (uint32_t i = 0; i < count; i++) {
        uint8_t co_ownerid[1024];
        uint32_t co_len = 0;
        uint8_t verifier[8];

        if (mds_coord_recovery_get(cat, recs[i].clientid, co_ownerid,
                                   &co_len, verifier) != MDS_OK ||
            co_len > sizeof(co_ownerid) ||
            mds_coord_recovery_put(cat, NULL, recs[i].clientid,
                                   co_ownerid, co_len, verifier) != MDS_OK) {
            failed++;
        }
    }
    return failed;
}

static enum mds_status failover_load_recovery_clients(
    struct mds_catalogue *cat, uint32_t owner_mds_id,
    struct client_recovery_rec **out_recs, uint32_t *out_count)
{
    struct failover_recovery_list_ctx ctx;
    enum mds_status st;

    if (cat == NULL || out_recs == NULL || out_count == NULL) {
        return MDS_ERR_INVAL;
    }

    memset(&ctx, 0, sizeof(ctx));
    ctx.st = MDS_OK;

    st = mds_coord_recovery_list(cat, owner_mds_id,
                                 failover_collect_recovery_cb, &ctx);
    if (ctx.st != MDS_OK) {
        free(ctx.recs);
        return ctx.st;
    }
    if (st != MDS_OK) {
        free(ctx.recs);
        return st;
    }

    *out_recs = ctx.recs;
    *out_count = ctx.count;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * API
 * ----------------------------------------------------------------------- */

enum mds_status failover_init(const struct failover_cfg *cfg,
                              struct failover_ctx **out)
{
    struct failover_ctx *ctx;

    if (cfg == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }
    if (cfg->map == NULL || cfg->cat == NULL) {
        return MDS_ERR_INVAL;
    }

    ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        return MDS_ERR_NOMEM;
    }

    ctx->self_id          = cfg->self_id;
    ctx->partner_id       = cfg->partner_id;
    ctx->role             = FAILOVER_STANDBY;
    ctx->map              = cfg->map;
    ctx->cat              = cfg->cat;
    ctx->grace_period_sec = cfg->grace_period_sec;
    ctx->detect_cb        = cfg->detect_cb;
    ctx->detect_arg       = cfg->detect_arg;
    ctx->membership       = cfg->membership;
    ctx->hm               = cfg->hm;
    ctx->taken_count      = 0;

    *out = ctx;
    return MDS_OK;
}

enum mds_status failover_promote(struct failover_ctx *ctx)
{
    struct client_recovery_rec *recs = NULL;
    uint32_t rec_count = 0;
    uint32_t taken = 0;
    enum mds_status st;

    if (ctx == NULL) {
        return MDS_ERR_INVAL;
    }

    /* Phase 1: Must be in STANDBY state. */
    if (ctx->role != FAILOVER_STANDBY) {
        return MDS_ERR_PERM;
    }

    ctx->role = FAILOVER_PROMOTING;

    /* Phases 2--3: Self-fencing, health, detect_cb. */
    st = promote_prechecks(ctx);
    if (st != MDS_OK) {
        ctx->role = FAILOVER_STANDBY;
        return st;
    }

    /* Phase 4a: Snapshot partner-owned paths for rollback. */
    st = snapshot_partner_paths(ctx);
    if (st != MDS_OK) {
        ctx->role = FAILOVER_STANDBY;
        return st;
    }

    /* Phase 4b: Transfer subtree ownership (failover-specific path
     * that bypasses owner_role_ok).  Each partition row is CAS'd from
     * the partner to self in the catalogue before the in-memory map
     * follows, so the takeover survives the next partition-map
     * refresh and a second standby racing for the same partner loses
     * the CAS instead of also believing it owns the subtrees.  A
     * takeover that lost every CAS (MDS_ERR_STALE) never reaches
     * PRIMARY: the loser stays standby. */
    st = subtree_map_failover_take_over(ctx->map, ctx->cat,
                                        ctx->partner_id, ctx->self_id,
                                        &taken);
    if (st != MDS_OK) {
        return promote_takeover_failed(ctx, st);
    }

    /* Phase 5: Load the partner's recovering clientids via the
     * coordination API (rows are owned by the MDS that served the
     * client).  grace_enter_with_clients() currently tracks only
     * clientid; the full recovery payload remains in the coordination
     * backend for EXCHANGE_ID matching during grace. */
    st = failover_load_recovery_clients(ctx->cat, ctx->partner_id,
                                        &recs, &rec_count);
    if (st != MDS_OK) {
        rollback_taken_paths(ctx);
        ctx->role = FAILOVER_STANDBY;
        return st;
    }

    /* Phase 5b: adopt those rows so this node's own grace matching
     * (which lists rows owned by self) sees them.  Best-effort. */
    {
        uint32_t not_adopted =
            failover_adopt_recovery_rows(ctx->cat, recs, rec_count);

        if (not_adopted != 0) {
            MDS_LOG_WARN(LOG_COMP_CLUSTER,
                    "failover: %u of %u recovery rows of partner %u "
                    "could not be re-owned; those clients will not "
                    "be matched to their old clientids in grace",
                    (unsigned)not_adopted, (unsigned)rec_count,
                    (unsigned)ctx->partner_id);
        }
    }

    /* Phase 6: Enter grace period with client list. */
    grace_enter_with_clients(ctx->grace_period_sec, recs, rec_count);
    free(recs);

    /* Phase 7: Publish self as ACTIVE + ACTIVE_SERVING.
     * Only done AFTER takeover + recovery + grace so peers never see
     * ACTIVE_SERVING before the promoting node is ready. */
    if (ctx->membership != NULL) {
        st = cluster_membership_promote_standby(
            ctx->membership, ctx->self_id);
        if (st != MDS_OK) {
            MDS_LOG_WARN(LOG_COMP_CLUSTER,
                    "promote_standby publish failed: %d "
                    "(proceeding anyway -- subtrees already taken)",
                    (int)st);
        }
    }

    /* Phase 8: Transition to PRIMARY. */
    ctx->role = FAILOVER_PRIMARY;
    return MDS_OK;
}

enum mds_status failover_controlled_demote(struct failover_ctx *ctx,
                                           struct repl_conn *repl)
{
    enum mds_status st;

    if (ctx == NULL) {
        return MDS_ERR_INVAL;
    }

    /* Step 1: Role guard -- must be PRIMARY. */
    if (ctx->role != FAILOVER_PRIMARY) {
        return MDS_ERR_PERM;
    }

    /* Step 2: Require live replication for controlled demote.
     * Without replication we cannot guarantee standby is caught up,
     * so we fail closed rather than risk data loss. */
    if (repl == NULL) {
        return MDS_ERR_PERM;
    }

    /* Step 3: Compat check -- before any state mutation. */
    if (ctx->membership != NULL) {
        struct cluster_member self_m;
        struct cluster_member partner_m;

        if (cluster_membership_get(ctx->membership, ctx->self_id,
                                   &self_m) == MDS_OK &&
            cluster_membership_get(ctx->membership, ctx->partner_id,
                                   &partner_m) == MDS_OK) {
            if (self_m.wire_compat_version !=
                partner_m.wire_compat_version) {
                return MDS_ERR_PERM;
            }
        }
    }

    /* Step 4: Transition to DEMOTING. */
    ctx->role = FAILOVER_DEMOTING;

    /* Step 5: Quiesce -- freeze all locally-owned subtrees.
     * NFS clients hitting frozen subtrees get NFS4ERR_DELAY
     * via the existing compound dispatch check.
     *
     * No membership lifecycle transitions here -- the entry must
     * remain ACTIVE + ACTIVE_SERVING so that self-removal in
     * step 7 triggers the partner-loss watcher on the standby. */
    st = subtree_map_freeze_owned(ctx->map, ctx->self_id);
    if (st != MDS_OK) {
        /* Rollback: unfreeze any partially-frozen subtrees, restore. */
        (void)subtree_map_unfreeze_owned(ctx->map, ctx->self_id);
        ctx->role = FAILOVER_PRIMARY;
        return MDS_ERR_IO;
    }

    /* Step 6: Replication sync barrier removed -- RonDB has native
     * multi-node consistency, so no standby catch-up step is needed. */
    (void)repl;

    /* Step 7: Self-remove from membership while still ACTIVE_SERVING.
     * The partner-loss watcher on the standby fires on deletion of
     * an ACTIVE + ACTIVE_SERVING member, triggering promotion.
     * cluster_membership_remove_self() is the only removal API that
     * permits self-deletion (cluster_node_leave and
     * cluster_force_remove_node both reject it). */
    if (ctx->membership != NULL) {
        st = cluster_membership_remove_self(ctx->membership);
        if (st != MDS_OK) {
            (void)subtree_map_unfreeze_owned(ctx->map, ctx->self_id);
            ctx->role = FAILOVER_PRIMARY;
            return MDS_ERR_IO;
        }
    }

    /* Step 8: Terminal state -- node is drained and ready to stop.
     * The operator stops the daemon, upgrades, and restarts as
     * standby.  Subtrees remain frozen (daemon is shutting down). */
    ctx->role = FAILOVER_STANDBY;
    return MDS_OK;
}

enum failover_role failover_get_role(const struct failover_ctx *ctx)
{
    if (ctx == NULL) {
        return FAILOVER_STANDBY;
    }
    return ctx->role;
}

uint32_t failover_get_self_id(const struct failover_ctx *ctx)
{
    if (ctx == NULL) {
        return 0;
    }
    return ctx->self_id;
}

void failover_destroy(struct failover_ctx *ctx)
{
    if (ctx != NULL) {
        free(ctx->taken_paths);
    }
    free(ctx);
}

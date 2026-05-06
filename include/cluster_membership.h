/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * cluster_membership.h -- MDS cluster membership table.
 *
 * Write operations are dispatched through an internal backend vtable
 * (local in-memory).  All operations are explicit
 * (no background heartbeats or liveness).
 * Multi-node configurations use RonDB node_registry for peer
 * discovery.
 *
 * Thread safety: all public API calls are internally synchronised
 * via pthread_rwlock_t.  Transport handler threads and the main
 * thread may call concurrently.
 */

#ifndef CLUSTER_MEMBERSHIP_H
#define CLUSTER_MEMBERSHIP_H

#include <stdint.h>
#include <stdbool.h>

/* Forward declarations. */
struct mds_config;
struct subtree_map;
struct mds_tls_ctx;

/* -----------------------------------------------------------------------
 * Node roles and lifecycle
 * ----------------------------------------------------------------------- */

enum node_role {
    NODE_ACTIVE  = 0,  /**< May own subtrees (after activation). */
    NODE_STANDBY = 1,  /**< Mirrors a paired active; cannot own subtrees. */
};

enum node_lifecycle {
    NODE_IDLE           = 0,  /**< Converged, eligible for assignment (zero-init default). */
    NODE_JOINING        = 1,  /**< Published but not yet converged. */
    NODE_ACTIVE_SERVING = 2,  /**< Serving subtrees. */
    NODE_DRAINING       = 3,  /**< Migrating subtrees away before leave. */
    NODE_DRAINED        = 4,  /**< All subtrees evacuated; awaiting removal. */
};

/* -----------------------------------------------------------------------
 * Member record
 * ----------------------------------------------------------------------- */

struct cluster_member {
    uint32_t            mds_id;
    char                hostname[256];
    uint16_t            nfs_port;
    uint16_t            grpc_port;       /**< Matches cfg.self.grpc_port naming. */
    uint64_t            join_time_sec;   /**< Wall-clock, seconds since Unix epoch. */
    enum node_role      role;            /**< Active or standby. */
    enum node_lifecycle lifecycle;       /**< Current lifecycle state. */
    uint32_t            failover_partner_id; /**< Paired partner (0 = none). */
    char                cluster_addr[64];    /**< Inter-MDS address. */

    /* Rolling upgrade fields (Item 46). */
    uint32_t            wire_compat_version;     /**< PNFS_MDS_WIRE_COMPAT_VERSION at join. */
};

/* -----------------------------------------------------------------------
 * Opaque handle
 * ----------------------------------------------------------------------- */

struct cluster_membership;

/* -----------------------------------------------------------------------
 * Change callback
 *
 * Fired on successful join or leave only (NOT on rejected operations).
 * Informational -- must not call back into cluster_membership (deadlock).
 * ----------------------------------------------------------------------- */

typedef void (*cluster_change_cb)(uint32_t mds_id, bool joined, void *arg);

/* -----------------------------------------------------------------------
 * API
 * ----------------------------------------------------------------------- */

/**
 * @brief Initialise cluster membership.
 *
 * Creates the membership context, auto-registers self from cfg->self,
 * and registers self's hostname in the subtree map.
 *
 * If cfg->etcd_endpoints is non-empty AND cfg->cluster_size > 1,
 * Single-node with etcd configured logs a WARN and uses local mode.
 *
 * @param cfg   MDS configuration (read-only, not stored).
 * @param smap  Subtree map handle (retained for node registration).
 * @param[out] out  Receives the membership handle.
 * @return MDS_OK on success, MDS_ERR_NOSUPPORT if etcd required.
 */
enum mds_status cluster_membership_init(const struct mds_config *cfg,
                                        struct subtree_map *smap,
                                        struct mds_tls_ctx *etcd_tls,
                                        struct cluster_membership **out);

/**
 * @brief Destroy the membership context and free resources.
 */
void cluster_membership_destroy(struct cluster_membership *ctx);

/**
 * @brief Add a node to the membership table.
 *
 * Also registers the node's hostname in the subtree map for referral
 * encoding.  The new node starts with no subtrees (idle).
 *
 * @param ctx     Membership handle.
 * @param member  Member record to add.
 * @return MDS_OK, MDS_ERR_EXISTS (duplicate), MDS_ERR_INVAL, MDS_ERR_NOMEM.
 */
enum mds_status cluster_node_join(struct cluster_membership *ctx,
                                  const struct cluster_member *member);

/**
 * @brief Remove a node from the membership table.
 *
 * Rejects with MDS_ERR_PERM if the node still owns subtrees
 * (caller must drain subtrees first via migration).
 * Rejects with MDS_ERR_INVAL if mds_id == self.
 *
 * NOTE: The subtree-ownership check is local-only and is NOT
 * race-free against concurrent migration or ownership changes
 * on remote nodes.  Acceptable for admin-only usage.
 *
 * @param ctx     Membership handle.
 * @param mds_id  Node ID to remove.
 * @return MDS_OK, MDS_ERR_PERM, MDS_ERR_INVAL, MDS_ERR_NOTFOUND.
 */
enum mds_status cluster_node_leave(struct cluster_membership *ctx,
                                   uint32_t mds_id);

/**
 * @brief List all members.
 *
 * Returns a caller-owned array (free with free()).
 *
 * @param ctx        Membership handle.
 * @param[out] out   Receives allocated array of members.
 * @param[out] count Receives number of members.
 * @return MDS_OK, MDS_ERR_NOMEM.
 */
enum mds_status cluster_membership_list(const struct cluster_membership *ctx,
                                        struct cluster_member **out,
                                        uint32_t *count);

/**
 * @brief Get a single member by ID.
 *
 * @param ctx        Membership handle.
 * @param mds_id     Node ID.
 * @param[out] out   Receives member record.
 * @return MDS_OK, MDS_ERR_NOTFOUND.
 */
enum mds_status cluster_membership_get(const struct cluster_membership *ctx,
                                       uint32_t mds_id,
                                       struct cluster_member *out);

/**
 * @brief Check if this node is the coordinator.
 *
 * The coordinator is the member with the lowest mds_id.
 *
 * @param ctx  Membership handle.
 * @return true if self has the lowest mds_id among all members.
 */
bool cluster_membership_is_coordinator(const struct cluster_membership *ctx);

/**
 * @brief Set the change callback.
 *
 * At most one callback can be registered.  Pass NULL to clear.
 *
 * @param ctx  Membership handle.
 * @param cb   Callback function (or NULL).
 * @param arg  User argument passed to cb.
 */
void cluster_membership_set_change_cb(struct cluster_membership *ctx,
                                      cluster_change_cb cb,
                                      void *arg);

/**
 * @brief Return the number of members.
 */
uint32_t cluster_membership_count(const struct cluster_membership *ctx);

/**
 * @brief Return this node's mds_id.
 */
uint32_t cluster_membership_self_id(const struct cluster_membership *ctx);

/**
 * @brief Check whether a node has an active (non-standby) role.
 *
 * @param ctx     Membership handle.
 * @param mds_id  Node ID.
 * @return true if NODE_ACTIVE, false if NODE_STANDBY or not found.
 */
bool cluster_membership_is_active_role(const struct cluster_membership *ctx,
                                       uint32_t mds_id);

/**
 * @brief Check whether a node may own subtrees.
 *
 * Returns true only when the node has an active role AND its lifecycle
 * is NODE_ACTIVE_SERVING.  Nodes that are JOINING, IDLE, or DRAINING
 * are not eligible.
 *
 * @param ctx     Membership handle (NULL => true for single-node).
 * @param mds_id  Node ID.
 * @return true if eligible to own subtrees.
 */
bool cluster_membership_can_own_subtrees(
    const struct cluster_membership *ctx, uint32_t mds_id);

/**
 * @brief Transition a node's lifecycle state.
 *
 * Valid transitions:
 *   JOINING -> IDLE
 *   IDLE -> ACTIVE_SERVING
 *   ACTIVE_SERVING -> DRAINING
 *   DRAINING -> ACTIVE_SERVING  (undrain)
 *
 * @param ctx       Membership handle.
 * @param mds_id    Node ID to transition.
 * @param new_state Target lifecycle state.
 * @return MDS_OK, MDS_ERR_INVAL (bad transition), MDS_ERR_NOTFOUND,
 *         MDS_ERR_PERM (standby cannot serve/drain).
 */
enum mds_status cluster_membership_set_lifecycle(
    struct cluster_membership *ctx, uint32_t mds_id,
    enum node_lifecycle new_state);

/**
 * @brief Check whether the membership backend is authoritative.
 *
 * Since membership now lives in RonDB's node_registry table, the
 * membership view is always authoritative; this predicate
 * unconditionally returns true and is retained for API stability.
 */
bool cluster_membership_is_authoritative(const struct cluster_membership *ctx);

/**
 * @brief Check if this node has self-fenced due to keepalive failure.
 *
 * Consumers should check this before serving requests.
 * Actual fencing behavior is wired in Seq 4+.
 */
bool cluster_membership_is_self_fenced(const struct cluster_membership *ctx);


/**
 * @brief Resolve a peer's cluster transport endpoint from membership.
 *
 * Looks up mds_id in the membership cache and returns the address
 * and port suitable for cluster_transport_connect().  Prefers
 * cluster_addr if non-empty and not a wildcard (0.0.0.0, ::);
 * otherwise falls back to hostname.
 *
 * @param ctx       Membership handle.
 * @param mds_id    Peer node ID.
 * @param host_out  Buffer to receive the host string (NUL-terminated).
 * @param host_cap  Capacity of host_out in bytes (must be > 0).
 * @param port_out  Receives the cluster transport port (grpc_port).
 * @return MDS_OK, MDS_ERR_NOTFOUND (unknown mds_id),
 *         MDS_ERR_INVAL (bad args or grpc_port == 0 on the peer).
 */
enum mds_status cluster_membership_resolve_peer(
    const struct cluster_membership *ctx,
    uint32_t mds_id,
    char *host_out, size_t host_cap,
    uint16_t *port_out);
/* JSON serialisation (used by tests). */
int member_to_json(const struct cluster_member *m,
                   char *buf, size_t cap);

/* -----------------------------------------------------------------------
 * Standby detach (Seq 8)
 *
 * Safe removal of a standby node.  Validates role, rejects self-detach,
 * and checks that the paired active partner is present and ACTIVE_SERVING
 * before removing the standby.
 *
 * @param ctx     Membership handle.
 * @param mds_id  Standby node to detach.
 * @return MDS_OK on success, MDS_ERR_PERM if unsafe,
 *         MDS_ERR_NOTFOUND if unknown.
 */
enum mds_status cluster_standby_detach(struct cluster_membership *ctx,
                                       uint32_t mds_id);

/**
 * @brief Force-remove a node (admin escape hatch, Seq 8).
 *
 * Skips lifecycle checks.  Rejects self-removal.  Rejects active
 * nodes that still own subtrees (no in-tree recovery path).
 * Allowed for standby nodes or zero-subtree active nodes.
 *
 * @param ctx     Membership handle.
 * @param mds_id  Node to remove.
 * @return MDS_OK, MDS_ERR_PERM, MDS_ERR_NOTFOUND.
 */
enum mds_status cluster_force_remove_node(struct cluster_membership *ctx,
                                          uint32_t mds_id);

/**
 * @brief Remove this node's own membership entry (controlled demote).
 *
 * Unlike cluster_node_leave() and cluster_force_remove_node(),
 * self-removal IS allowed.  Call while the member entry is still
 * ACTIVE + ACTIVE_SERVING so the partner-loss watcher fires on
 * the standby.
 *
 * In RonDB mode the key is deleted; the watch handler fires the
 * partner_loss_cb
 *
 *
 *
 * @param ctx  Membership handle.
 * @return MDS_OK on success, MDS_ERR_INVAL (NULL ctx),
 *         MDS_ERR_NOTFOUND (self not in table).
 */
enum mds_status cluster_membership_remove_self(
    struct cluster_membership *ctx);

/* -----------------------------------------------------------------------
 * Partner-loss observer (Seq 9)
 *
 * Dedicated callback for failover -- NOT the generic change_cb.
 * Fired by membership_watch_handler on DELETE of the configured
 * partner, only when the removed member's last cached role was
 * NODE_ACTIVE and lifecycle was NODE_ACTIVE_SERVING.
 * ----------------------------------------------------------------------- */

/**
 * @brief Callback type for partner-loss notification.
 *
 * @param removed_member  Snapshot of the member BEFORE removal.
 * @param arg             User argument from set_partner_loss_cb.
 */
typedef void (*cluster_partner_loss_cb)(
    const struct cluster_member *removed_member, void *arg);

/**
 * @brief Register a partner-loss observer.
 *
 * At most one observer can be registered.  Pass NULL cb to clear.
 * When a DELETE event for partner_id is observed and the member's
 * last cached state is NODE_ACTIVE + NODE_ACTIVE_SERVING, the
 * callback is invoked with a snapshot of the removed member.
 *
 * @param ctx         Membership handle.
 * @param partner_id  MDS ID of the paired partner to watch.
 * @param cb          Callback (or NULL to clear).
 * @param arg         User argument passed to cb.
 */
void cluster_membership_set_partner_loss_cb(
    struct cluster_membership *ctx,
    uint32_t partner_id,
    cluster_partner_loss_cb cb,
    void *arg);

/* -----------------------------------------------------------------------
 * Standby promotion (Seq 9)
 *
 * Atomically transitions a NODE_STANDBY member to NODE_ACTIVE +
 * NODE_ACTIVE_SERVING.  Does NOT fire change_cb (not a join/leave).
 * Peers learn about the update via the normal coordination PUT event.
 * ----------------------------------------------------------------------- */

/**
 * @brief Promote a standby member to active+serving.
 *
 * Validates that the member exists and has role == NODE_STANDBY.
 * In RonDB mode: CAS-updates the member JSON.
 * Updates in-place under write lock.
 *
 * @param ctx     Membership handle.
 * @param mds_id  Node ID to promote.
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_PERM (not standby).
 */
enum mds_status cluster_membership_promote_standby(
    struct cluster_membership *ctx, uint32_t mds_id);

/* -----------------------------------------------------------------------
 * RonDB-native membership population (Phase 9)
 *
 * Scans mds_node_registry and upserts all registered nodes into the
 * local membership array.  Called once at startup and periodically
 * from the heartbeat thread to discover newly-joined peers.
 * ----------------------------------------------------------------------- */

struct mds_catalogue;

/**
 * @brief Populate membership from RonDB node_registry.
 *
 * Scans all rows in mds_node_registry and upserts each as a
 * cluster_member.  Existing entries are updated; new entries
 * are inserted.  Self is skipped (already registered by init).
 *
 * @param ctx  Membership handle.
 * @param cat  Catalogue handle (RonDB backend).
 * @return MDS_OK on success, MDS_ERR_INVAL, MDS_ERR_IO.
 */
enum mds_status cluster_membership_populate_rondb(
    struct cluster_membership *ctx, struct mds_catalogue *cat);

#endif /* CLUSTER_MEMBERSHIP_H */

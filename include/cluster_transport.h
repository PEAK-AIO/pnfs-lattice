/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * cluster_transport.h — Inter-MDS transport for 2PC rename messages.
 *
 * Defines the transport abstraction (function pointers) and a
 * lightweight TCP implementation.  The same abstraction supports
 * in-process loopback for unit tests.
 */

#ifndef CLUSTER_TRANSPORT_H
#define CLUSTER_TRANSPORT_H

#include <stdint.h>
#include <stddef.h>

#include "pnfs_mds.h"
#include "cluster_membership.h"
#include "subtree_map.h"

/* Forward declarations. */
struct mds_catalogue;
struct subtree_map;

/* -----------------------------------------------------------------------
 * Transport abstraction
 *
 * Callers set these function pointers to route 2PC RPCs either via
 * TCP (production), gRPC (future), or in-process loopback (tests).
 * ----------------------------------------------------------------------- */

struct rename_2pc_transport {
    /**
     * Send PrepareRename to the remote MDS.
     * @return 1 for VOTE_COMMIT, 0 for VOTE_ABORT, -1 for transport error.
     */
    int (*prepare)(uint32_t remote_mds_id, uint64_t txn_id,
                   uint64_t dst_parent, const char *dst_name,
                   const void *inode_data, size_t data_len,
                   void *user_ctx);

    /** Send CommitRename.  @return 0 on success, -1 on failure. */
    int (*commit)(uint32_t remote_mds_id, uint64_t txn_id,
                  void *user_ctx);

    /** Send AbortRename.  @return 0 on success, -1 on failure. */
    int (*abort_rename)(uint32_t remote_mds_id, uint64_t txn_id,
                        void *user_ctx);

    void *user_ctx;
};

/* -----------------------------------------------------------------------
 * Wire protocol message types (TCP transport)
 * ----------------------------------------------------------------------- */

#define CT_MSG_PREPARE  1
#define CT_MSG_VOTE     2
#define CT_MSG_COMMIT   3
#define CT_MSG_ABORT    4
#define CT_MSG_ACK      5

/* Migration message types (§13 subtree migration). */
#define CT_MSG_MIG_BEGIN   6
#define CT_MSG_MIG_INODE   7
#define CT_MSG_MIG_FINISH  8
#define CT_MSG_MIG_ACK     9
#define CT_MSG_MIG_NACK   10

/* Admin-triggered migration (source MDS daemon-internal). */
#define CT_MSG_MIG_ADMIN_REQ   11
#define CT_MSG_MIG_ADMIN_RESP  12

/* Resilver admin messages (§15.3 resilvering). */
#define CT_MSG_RESILVER_START   13
#define CT_MSG_RESILVER_STATUS  14
#define CT_MSG_RESILVER_RESP    15

/* Cluster membership admin messages (§13.5 MDS scale-out scaffold). */
#define CT_MSG_NODE_JOIN            16
#define CT_MSG_NODE_LEAVE           17
#define CT_MSG_CLUSTER_STATUS_REQ   18
#define CT_MSG_CLUSTER_STATUS_RESP  19
#define CT_MSG_MEMBERSHIP_RESP      20  /* generic OK/ERR for join/leave */
#define CT_MSG_SET_LIFECYCLE        21  /* set node lifecycle state */

/* Split admin messages (§7.1 subtree split scale-out). */
#define CT_MSG_SPLIT_ADMIN_REQ      22
#define CT_MSG_SPLIT_ADMIN_RESP     23

/* Assign admin messages (§7.3 exact-root migrate for scale-out). */
#define CT_MSG_ASSIGN_ADMIN_REQ     24
#define CT_MSG_ASSIGN_ADMIN_RESP    25

/* Cross-subtree hard link nlink management (§ cross-MDS links). */
/* Split evaluator admin messages (Tier 3 Phase 1). */
#define CT_MSG_SPLIT_PROPOSALS_REQ   75
#define CT_MSG_SPLIT_PROPOSALS_RESP  76
#define CT_MSG_SPLIT_APPROVE_REQ     77
#define CT_MSG_SPLIT_APPROVE_RESP    78
#define CT_MSG_SPLIT_AUTO_REQ        79
#define CT_MSG_SPLIT_AUTO_RESP       80

#define CT_MSG_NLINK_INC_REQ   71
#define CT_MSG_NLINK_INC_RESP  72
#define CT_MSG_NLINK_DEC_REQ   73
#define CT_MSG_NLINK_DEC_RESP  74

/* Drain, detach, force-remove admin messages (Seq 8). */
#define CT_MSG_DRAIN_SELF           26
#define CT_MSG_STANDBY_DETACH       27
#define CT_MSG_FORCE_REMOVE         28

/* Admin query messages (Seq 10). */
#define CT_MSG_SUBTREE_LIST_ADMIN_REQ   29
#define CT_MSG_SUBTREE_LIST_ADMIN_RESP  30
#define CT_MSG_MIG_PROGRESS_ADMIN_REQ   31
#define CT_MSG_MIG_PROGRESS_ADMIN_RESP  32

/* Extended resilver status (Seq 11 — includes skip counters). */
#define CT_MSG_RESILVER_STATUS_EXT  33

/* Rebalance admin messages (§3.8 mirror relocation V1). */
#define CT_MSG_REBALANCE_START       34
#define CT_MSG_REBALANCE_STATUS_REQ  35
#define CT_MSG_REBALANCE_RESP        36

/* Tiering admin messages (§3.9 storage tiering V1). */
#define CT_MSG_TIERING_START         37
#define CT_MSG_TIERING_STOP          38
#define CT_MSG_TIERING_STATUS_REQ    39
#define CT_MSG_TIERING_RESP          40

/* Quota admin messages (§4.5 quota management V1). */
#define CT_MSG_QUOTA_SET             41
#define CT_MSG_QUOTA_GET_REQ         42
#define CT_MSG_QUOTA_GET_RESP        43
#define CT_MSG_QUOTA_DEL             44

/* Rolling upgrade admin messages (§8.5 rolling upgrade V1). */
#define CT_MSG_FAILOVER_REQ          45
#define CT_MSG_FAILOVER_RESP         46
#define CT_MSG_UPGRADE_STATUS_REQ    47
#define CT_MSG_UPGRADE_STATUS_RESP   48

/* DS registry admin messages (§7 admin CLI V1). */
#define CT_MSG_DS_LIST_REQ           49
#define CT_MSG_DS_LIST_RESP          50

/* DS admin write messages (§7 admin CLI V1, item 47b). */
#define CT_MSG_DS_ADD_REQ            51
#define CT_MSG_DS_ADD_RESP           52
#define CT_MSG_DS_SET_STATE_REQ      53
#define CT_MSG_DS_SET_STATE_RESP     54
#define CT_MSG_DS_REMOVE_REQ         55
#define CT_MSG_DS_REMOVE_RESP        56

/* DS validation admin messages (Phase 6). */
#define CT_MSG_DS_VALIDATE_REQ       57
#define CT_MSG_DS_VALIDATE_RESP      58
#define CT_MSG_DS_VALIDATE_CLR_REQ   59
#define CT_MSG_DS_VALIDATE_CLR_RESP  60

/* DS provisioning admin messages (Phase 5). */
#define CT_MSG_DS_PROVISION_REQ      61
#define CT_MSG_DS_PROVISION_RESP     62
#define CT_MSG_DS_PROVISION_INFO_REQ 63
#define CT_MSG_DS_PROVISION_INFO_RESP 64

/* Patched kernel-profile readiness (Phase 7). */
#define CT_MSG_DS_PATCH_READY_REQ    65
#define CT_MSG_DS_PATCH_READY_RESP   66
#define CT_MSG_DS_PATCH_CLEAR_REQ    67
#define CT_MSG_DS_PATCH_CLEAR_RESP   68

/* Metrics query (Item 48). */
#define CT_MSG_METRICS_REQ           69
#define CT_MSG_METRICS_RESP          70

/* C2: read-only admin introspection.
 *   CONFIG_SHOW:  request a rendered INI-key=value dump (optional
 *                 key filter as a UTF-8 payload).  Response is a
 *                 UTF-8 text blob the client prints verbatim.
 *   DS_CAPACITY_SHOW: one record per DS (ds_id, total, used,
 *                     weight).  Consumed by `mds-admin ds capacity
 *                     show`. */
#define CT_MSG_CONFIG_SHOW_REQ       81
#define CT_MSG_CONFIG_SHOW_RESP      82
#define CT_MSG_DS_CAPACITY_SHOW_REQ  83
#define CT_MSG_DS_CAPACITY_SHOW_RESP 84

/* C3: runtime admin write-path knobs (see docs/config-keys.md). */
#define CT_MSG_DS_SET_WEIGHT_REQ     85
#define CT_MSG_DS_SET_WEIGHT_RESP    86
#define CT_MSG_DS_CAPACITY_PROBE_REQ  87
#define CT_MSG_DS_CAPACITY_PROBE_RESP 88

/** Wire header: [msg_type u8][payload_len u32 BE] = 5 bytes. */
#define CT_HEADER_SIZE  5

/* -----------------------------------------------------------------------
 * TCP transport server
 * ----------------------------------------------------------------------- */

struct mds_tls_ctx;
struct cluster_server;

/**
 * @brief Start the cluster transport server.
 *
 * Spawns a listener thread on the given port.  Incoming 2PC
 * messages are dispatched to rename_2pc_on_prepare / on_commit /
 * on_abort using the provided catalogue handle.  If @smap is non-NULL,
 * admin migration requests (CT_MSG_MIG_ADMIN_REQ) are also handled.
 *
 * @param port              TCP port to listen on.
 * @param bind_addr        Address to bind (NULL → 127.0.0.1).
 * @param allowed_peers    Array of allowed peer IPs (NULL → accept all).
 * @param allowed_peer_count  Number of entries in allowed_peers.
 * @param max_conns         Max concurrent connections (0 → default 16).
 * @param cat              Catalogue handle (must not be NULL).
 * @param smap             Subtree map (may be NULL if admin migration not needed).
 * @param[out] out  Receives the server handle.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_server_start(uint16_t port,
                                               const char *bind_addr,
                                               const char (*allowed_peers)[64],
                                               uint32_t allowed_peer_count,
                                               uint32_t max_conns,
                                               struct mds_catalogue *cat,
                                               struct subtree_map *smap,
                                               struct mds_tls_ctx *tls,
                                               struct cluster_server **out);

/**
 * @brief Stop the server and free resources.
 */
void cluster_transport_server_stop(struct cluster_server *srv);

/**
 * @brief Return the actual port the server is listening on.
 *
 * Useful when port 0 was passed to server_start (OS-assigned).
 */
uint16_t cluster_transport_server_port(const struct cluster_server *srv);

/* -----------------------------------------------------------------------
 * TCP transport client
 * ----------------------------------------------------------------------- */

/**
 * @brief Create a TCP-backed transport connected to a remote MDS.
 *
 * The returned transport implements the rename_2pc_transport
 * interface.  Caller must eventually call cluster_transport_disconnect().
 *
 * @param host  Remote hostname or IP.
 * @param port  Remote port.
 * @param[out] out  Receives the transport.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_connect(const char *host, uint16_t port,
                                          struct rename_2pc_transport **out);

/**
 * @brief Disconnect and free a TCP transport.
 */
void cluster_transport_disconnect(struct rename_2pc_transport *t);

/* -----------------------------------------------------------------------
 * TCP migration transport
 * ----------------------------------------------------------------------- */

struct migration_transport;

/**
 * @brief Create a TCP-backed migration transport connected to a remote MDS.
 *
 * The returned transport implements the migration_transport interface
 * (migration.h).  Caller must call cluster_transport_disconnect_migration()
 * when done.
 *
 * @param host  Remote hostname or IP.
 * @param port  Remote port.
 * @param[out] out  Receives the transport.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_connect_migration(
    const char *host, uint16_t port,
    struct migration_transport **out);

/**
 * @brief Disconnect and free a TCP migration transport.
 */
void cluster_transport_disconnect_migration(struct migration_transport *t);

/* -----------------------------------------------------------------------
 * Admin migration request (sent by mds-admin CLI to running daemon)
 * ----------------------------------------------------------------------- */

/**
 * @brief Send an admin migration request to a running MDS daemon.
 *
 * Connects to the daemon's cluster transport port, sends a
 * CT_MSG_MIG_ADMIN_REQ, and waits for CT_MSG_MIG_ADMIN_RESP.
 * The daemon performs the migration using its live db and smap.
 *
 * @param mds_host       Daemon cluster transport host.
 * @param mds_port       Daemon cluster transport port.
 * @param subtree_path   Subtree to migrate (e.g. "/data").
 * @param dest_mds_id    Destination MDS node ID.
 * @param dest_host      Destination MDS host for migration transport.
 * @param dest_port      Destination MDS migration transport port.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_migration(
    const char *mds_host, uint16_t mds_port,
    const char *subtree_path, uint32_t dest_mds_id,
    const char *dest_host, uint16_t dest_port);

/* -----------------------------------------------------------------------
 * Resilver worker registration (called from main after init)
 * ----------------------------------------------------------------------- */

struct resilver_worker;

/**
 * @brief Register the resilver worker with the cluster transport server.
 *
 * Must be called after both cluster_transport_server_start() and
 * resilver_init().  Allows the server to dispatch CT_MSG_RESILVER_*
 * messages to the worker.
 *
 * @param srv     Cluster transport server.
 * @param rw      Resilver worker handle (may be NULL to clear).
 */
void cluster_transport_server_set_resilver(struct cluster_server *srv,
                                           struct resilver_worker *rw);

/* -----------------------------------------------------------------------
 * Resilver admin requests (sent by mds-admin CLI to running daemon)
 * ----------------------------------------------------------------------- */

/**
 * @brief Request the daemon to start resilvering onto a specific DS.
 *
 * @param mds_host  Daemon cluster transport host.
 * @param mds_port  Daemon cluster transport port.
 * @param ds_id     Target data server ID.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_resilver_start(
    const char *mds_host, uint16_t mds_port, uint32_t ds_id);

/**
 * @brief Query resilvering progress from the running daemon.
 *
 * @param mds_host    Daemon cluster transport host.
 * @param mds_port    Daemon cluster transport port.
 * @param[out] state  Receives resilver state (0=idle..4=error).
 * @param[out] files_total  Receives total files to process.
 * @param[out] files_done   Receives files completed.
 * @param[out] bytes_copied Receives bytes copied.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_resilver_status(
    const char *mds_host, uint16_t mds_port,
    uint32_t *state, uint32_t *files_total,
    uint32_t *files_done, uint64_t *bytes_copied);


struct resilver_status_info;

/**
 * @brief Request extended resilver status including skip-reason counters.
 *
 * @param mds_host  Daemon host.
 * @param mds_port  Daemon port.
 * @param[out] info Receives all status fields.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_resilver_status_ext(
    const char *mds_host, uint16_t mds_port,
    struct resilver_status_info *info);

/* -----------------------------------------------------------------------
 * Rebalance admin (§3.8 mirror relocation V1)
 * ----------------------------------------------------------------------- */

struct rebalance_worker;

/**
 * @brief Register the rebalance worker with the transport server.
 *
 * @param srv  Cluster transport server.
 * @param rw   Rebalance worker handle (may be NULL to clear).
 */
void cluster_transport_server_set_rebalance(struct cluster_server *srv,
                                             struct rebalance_worker *rw);

/**
 * @brief Request the daemon to start rebalancing source → target.
 *
 * @param mds_host   Daemon cluster transport host.
 * @param mds_port   Daemon cluster transport port.
 * @param source_ds  Source data server ID.
 * @param target_ds  Target data server ID.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_rebalance_start(
    const char *mds_host, uint16_t mds_port,
    uint32_t source_ds, uint32_t target_ds);

struct rebalance_status_info;

/**
 * @brief Request rebalance status from the running daemon.
 *
 * @param mds_host  Daemon host.
 * @param mds_port  Daemon port.
 * @param[out] info Receives all status fields.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_rebalance_status(
    const char *mds_host, uint16_t mds_port,
    struct rebalance_status_info *info);

/* -----------------------------------------------------------------------
 * Tiering admin (§3.9 storage tiering V1)
 * ----------------------------------------------------------------------- */

struct tiering_worker;
struct tiering_config;

/**
 * @brief Register the tiering worker with the transport server.
 */
void cluster_transport_server_set_tiering(struct cluster_server *srv,
                                           struct tiering_worker *tw);

/**
 * @brief Request the daemon to start tiering with the given config.
 */
enum mds_status cluster_transport_request_tiering_start(
    const char *mds_host, uint16_t mds_port,
    const struct tiering_config *cfg);

/**
 * @brief Request the daemon to stop tiering.
 */
enum mds_status cluster_transport_request_tiering_stop(
    const char *mds_host, uint16_t mds_port);

struct tiering_status_info;

/**
 * @brief Request tiering status from the running daemon.
 */
enum mds_status cluster_transport_request_tiering_status(
    const char *mds_host, uint16_t mds_port,
    struct tiering_status_info *info);

/* -----------------------------------------------------------------------
 * Cluster membership registration (called from main after init)
 * ----------------------------------------------------------------------- */

struct cluster_membership;

/**
 * @brief Register the cluster membership handle with the transport server.
 *
 * Must be called after both cluster_transport_server_start() and
 * cluster_membership_init().  Allows the server to dispatch
 * CT_MSG_NODE_* and CT_MSG_CLUSTER_STATUS_* messages.
 *
 * @param srv   Cluster transport server.
 * @param mem   Membership handle (may be NULL to clear).
 */
void cluster_transport_server_set_membership(struct cluster_server *srv,
                                              struct cluster_membership *mem);

/* -----------------------------------------------------------------------
 * Cluster membership client requests (mds-admin CLI -> daemon)
 * ----------------------------------------------------------------------- */

/**
 * @brief Request the daemon to register a new node.
 */
enum mds_status cluster_transport_request_node_join(
    const char *mds_host, uint16_t mds_port,
    uint32_t mds_id, const char *hostname,
    uint16_t nfs_port, uint16_t grpc_port,
    enum node_role role, enum node_lifecycle lifecycle,
    uint32_t failover_partner_id, const char *cluster_addr);

/**
 * @brief Request the daemon to remove a node.
 */
enum mds_status cluster_transport_request_node_leave(
    const char *mds_host, uint16_t mds_port, uint32_t mds_id);

/**
 * @brief Request the daemon to change a node's lifecycle state.
 *
 * Sends CT_MSG_SET_LIFECYCLE with the target mds_id and lifecycle.
 * The daemon validates the transition and responds with
 * CT_MSG_MEMBERSHIP_RESP.
 *
 * @param mds_host  Daemon cluster transport host.
 * @param mds_port  Daemon cluster transport port.
 * @param mds_id    Node whose lifecycle to change.
 * @param state     Target lifecycle state.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_set_lifecycle(
    const char *mds_host, uint16_t mds_port,
    uint32_t mds_id, enum node_lifecycle state);

/**
 * @brief Query cluster membership from the daemon.
 *
 * Returns a caller-owned array (free with free()).
 *
 * @param[out] out    Receives allocated array of cluster_member.
 * @param[out] count  Receives number of members.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_cluster_status(
    const char *mds_host, uint16_t mds_port,
    struct cluster_member **out, uint32_t *count);
/* -----------------------------------------------------------------------
 * Membership-backed transport factories (Seq 5)
 *
 * Resolve peer endpoint from cluster_membership, then delegate to the
 * existing host:port connect functions.  Callers disconnect as usual.
 * ----------------------------------------------------------------------- */

/**
 * @brief Create a rename-2PC transport by resolving mds_id from membership.
 *
 * @param mem     Membership handle (must not be NULL).
 * @param mds_id  Peer node ID.
 * @param[out] out  Receives the transport.
 * @return MDS_OK on success, or resolve/connect error.
 */
enum mds_status cluster_transport_connect_by_id(
    const struct cluster_membership *mem,
    uint32_t mds_id,
    struct rename_2pc_transport **out);

/**
 * @brief Create a migration transport by resolving mds_id from membership.
 *
 * @param mem     Membership handle (must not be NULL).
 * @param mds_id  Peer node ID.
 * @param[out] out  Receives the transport.
 * @return MDS_OK on success, or resolve/connect error.
 */
enum mds_status cluster_transport_connect_migration_by_id(
    const struct cluster_membership *mem,
    uint32_t mds_id,
    struct migration_transport **out);


/* -----------------------------------------------------------------------
 * Split admin request (sent by mds-admin CLI to running daemon)
 * ----------------------------------------------------------------------- */

/**
 * @brief Send an admin split request to a running MDS daemon.
 *
 * Connects to the daemon's cluster transport port, sends a
 * CT_MSG_SPLIT_ADMIN_REQ, and waits for CT_MSG_SPLIT_ADMIN_RESP.
 * The daemon validates, optionally resolves child_path to fileid,
 * performs the split (and remote migration if dest != self).
 *
 * @param mds_host       Daemon cluster transport host.
 * @param mds_port       Daemon cluster transport port.
 * @param parent_path    Parent subtree root path.
 * @param child_path     Child path to split off.
 * @param dest_mds_id    Destination MDS node ID.
 * @param dest_host      Destination MDS host (may be NULL if membership resolves).
 * @param dest_port      Destination MDS port (0 if membership resolves).
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_split(
    const char *mds_host, uint16_t mds_port,
    const char *parent_path, const char *child_path,
    uint32_t dest_mds_id,
    const char *dest_host, uint16_t dest_port);

/* -----------------------------------------------------------------------
 * Assign admin request (exact-root migrate, sent by mds-admin CLI)
 * ----------------------------------------------------------------------- */

/**
 * @brief Send an admin assign request to a running MDS daemon.
 *
 * Requires the path to be an exact subtree root.  The daemon validates
 * via subtree_map_lookup_exact(), then migrates to the destination.
 *
 * @param mds_host       Daemon cluster transport host.
 * @param mds_port       Daemon cluster transport port.
 * @param subtree_path   Exact subtree root path.
 * @param dest_mds_id    Destination MDS node ID.
 * @param dest_host      Destination MDS host (may be NULL).
 * @param dest_port      Destination MDS port (0 if membership resolves).
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_assign(
    const char *mds_host, uint16_t mds_port,
    const char *subtree_path, uint32_t dest_mds_id,
    const char *dest_host, uint16_t dest_port);

/* -----------------------------------------------------------------------
 * Drain, detach, force-remove admin requests (Seq 8)
 * ----------------------------------------------------------------------- */

/**
 * @brief Request the target daemon to drain itself.
 *
 * Must be sent to the daemon on the node being drained.
 *
 * @param mds_host      Target daemon host.
 * @param mds_port      Target daemon cluster port.
 * @param dest_mds_id   Destination MDS for migrated subtrees.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_drain_self(
    const char *mds_host, uint16_t mds_port,
    uint32_t dest_mds_id);

/**
 * @brief Request standby detach.
 */
enum mds_status cluster_transport_request_standby_detach(
    const char *mds_host, uint16_t mds_port,
    uint32_t mds_id);

/**
 * @brief Request forced node removal.
 */
enum mds_status cluster_transport_request_force_remove(
    const char *mds_host, uint16_t mds_port,
    uint32_t mds_id);

/* -----------------------------------------------------------------------
 * C2: Config / DS-capacity introspection
 * ----------------------------------------------------------------------- */

/**
 * @brief Request the daemon's rendered INI-key=value listing.
 *
 * @param mds_host   Daemon cluster-transport host.
 * @param mds_port   Daemon cluster-transport port.
 * @param key_filter Optional single key (NULL / empty = all keys).
 *                   When set, the response lists only that key.
 * @param[out] out_text  Heap-allocated UTF-8 blob (caller frees with
 *                       free()).  NUL-terminated.  On MDS_OK and
 *                       empty response set to a 1-byte empty string
 *                       to simplify callers.
 * @return MDS_OK on success; MDS_ERR_IO on transport failure;
 *         MDS_ERR_INVAL when the daemon has no config attached.
 */
enum mds_status cluster_transport_request_config_show(
    const char *mds_host, uint16_t mds_port,
    const char *key_filter, char **out_text);

/** One record returned by DS_CAPACITY_SHOW.
 *
 * Wire layout (big-endian): ds_id(4) + total(8) + used(8) + weight(4)
 *                         + state(4) + auto_weight(4) = 32 bytes.
 *
 * `weight` is the operator override (ds_weight.<id> or mds-admin ds
 * set-weight).  `auto_weight` is the capacity-derived value stamped
 * by the statvfs probe when placement_capacity_weighting is
 * CAP_WEIGHT_PROPORTIONAL; zero means the probe has not written a
 * derived value (feature off or no successful probe yet).  WRR
 * effective weight = auto_weight only when `weight` is zero.
 */
struct ds_capacity_record {
    uint32_t ds_id;
    uint64_t total_bytes;
    uint64_t used_bytes;
    uint32_t weight;
    uint32_t state;     /* DS_ONLINE / DS_OFFLINE / ... */
    uint32_t auto_weight;
};

/**
 * @brief Request the daemon's live DS capacity snapshot.
 *
 * The records come from the live in-memory ds_cache, which is
 * populated at startup from the catalogue, stamped with admin
 * weights from `ds_weight.<id>`, and refreshed by the statvfs
 * capacity probe when enabled.  Reflects what WRR currently sees.
 *
 * @param[out] out_records  Heap array; caller frees with free().
 * @param[out] out_count    Number of records written.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_ds_capacity_show(
    const char *mds_host, uint16_t mds_port,
    struct ds_capacity_record **out_records, uint32_t *out_count);

/**
 * @brief Update a single DS's runtime WRR weight.
 *
 * Ephemeral: new value survives only until the next daemon
 * restart.  Persist by editing `ds_weight.<id>` in the INI.
 *
 * @return MDS_OK on success; MDS_ERR_NOTFOUND if ds_id is not in
 *         the daemon's DS cache; MDS_ERR_IO on transport failure.
 */
enum mds_status cluster_transport_request_ds_set_weight(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id, uint32_t weight);

/**
 * @brief Trigger an immediate statvfs() sweep of all DSes.
 *
 * Returns when the sweep is complete on the daemon side (no
 * background wait).  Useful after attaching a new DS so the
 * next LAYOUTGET sees live capacity without waiting for the
 * regular poll interval.
 *
 * @param[out] probed_count  Number of DSes successfully probed
 *                           (NULL tolerated).
 */
enum mds_status cluster_transport_request_ds_capacity_probe(
    const char *mds_host, uint16_t mds_port,
    uint32_t *probed_count);

/* -----------------------------------------------------------------------
 * Migration tracker integration (Seq 10)
 * ----------------------------------------------------------------------- */

struct migration_tracker;

/**
 * @brief Attach a migration tracker to the server.
 *
 * Must be called after server_start, before any admin progress queries.
 */
void cluster_transport_server_set_migration_tracker(
    struct cluster_server *srv, struct migration_tracker *tracker);

/**
 * @brief Attach a borrowed pointer to the daemon's live config.
 *
 * Used by the CONFIG_SHOW handler to render current INI key=value
 * pairs.  The pointer must outlive the server.  Safe to pass NULL
 * to clear; when cleared, CONFIG_SHOW responds with MDS_ERR_INVAL.
 */
void cluster_transport_server_set_config(
    struct cluster_server *srv, const struct mds_config *cfg);

/**
 * @brief Attach the DS cache so DS_CAPACITY_SHOW can iterate it.
 *
 * Already stored via cluster_transport_server_set_ds_cache for DS
 * lifecycle handlers; this is just a re-exposure for clarity.  The
 * handler reads cache entries under the cache's own rwlock.
 */

/* -----------------------------------------------------------------------
 * Admin query client requests (Seq 10)
 * ----------------------------------------------------------------------- */

/**
 * @brief Request subtree list from a running MDS daemon.
 *
 * Caller frees *out with free().
 * Wire: CT_MSG_SUBTREE_LIST_ADMIN_REQ (empty payload) / RESP.
 *
 * @param mds_host   Target daemon host.
 * @param mds_port   Target daemon cluster port.
 * @param owner_filter  Filter by owner MDS ID (0 = all).
 * @param[out] out   Receives array of subtree_entry.
 * @param[out] count Number of entries.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_subtree_list(
    const char *mds_host, uint16_t mds_port,
    uint32_t owner_filter,
    struct subtree_entry **out, uint32_t *count);

/**
 * @brief Request migration progress from a running MDS daemon.
 *
 * Wire: CT_MSG_MIG_PROGRESS_ADMIN_REQ (empty) / RESP.
 *
 * @param mds_host   Target daemon host.
 * @param mds_port   Target daemon cluster port.
 * @param[out] state Migration state.
 * @param[out] path  Subtree path being migrated.
 * @param path_cap   Capacity of path buffer.
 * @param[out] total Total inodes.
 * @param[out] done  Inodes completed.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_mig_progress(
    const char *mds_host, uint16_t mds_port,
    uint32_t *state, char *path, size_t path_cap,
    uint32_t *total, uint32_t *done);


/* ---- Quota wire protocol ---- */

struct mds_quota_rule;
struct mds_quota_usage;
struct mds_quota_ctx;

void cluster_transport_server_set_quota(struct cluster_server *srv,
                                        struct mds_quota_ctx *quota);

enum mds_status cluster_transport_request_quota_set(
    const char *mds_host, uint16_t mds_port,
    uint8_t scope_type, uint64_t scope_id,
    const struct mds_quota_rule *rule);

enum mds_status cluster_transport_request_quota_get(
    const char *mds_host, uint16_t mds_port,
    uint8_t scope_type, uint64_t scope_id,
    struct mds_quota_rule *rule, struct mds_quota_usage *usage);

enum mds_status cluster_transport_request_quota_del(
    const char *mds_host, uint16_t mds_port,
    uint8_t scope_type, uint64_t scope_id);
/* ---- Rolling upgrade wire protocol (Item 46) ---- */

/**
 * @brief Request controlled failover (demote) of a target MDS.
 *
 * Sends CT_MSG_FAILOVER_REQ to the target and waits for RESP.
 *
 * @param mds_host  Target daemon host.
 * @param mds_port  Target daemon cluster port.
 * @return MDS_OK on success, error code otherwise.
 */
enum mds_status cluster_transport_request_failover(
    const char *mds_host, uint16_t mds_port,
    uint32_t target_mds_id);

/**
 * @brief Upgrade status entry returned per node.
 */
struct upgrade_status_entry {
    uint32_t mds_id;
    uint32_t wire_compat_version;
    char     software_version[32];
    uint8_t  role;
    uint8_t  lifecycle;
};

/**
 * @brief Request upgrade status table from a running MDS daemon.
 *
 * Caller frees *out with free().
 *
 * @param mds_host   Target daemon host.
 * @param mds_port   Target daemon cluster port.
 * @param[out] out   Receives array of upgrade_status_entry.
 * @param[out] count Number of entries.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_upgrade_status(
    const char *mds_host, uint16_t mds_port,
    struct upgrade_status_entry **out, uint32_t *count);

/* -----------------------------------------------------------------------
 * DS admin: commit queue and proxy context attachment
 * ----------------------------------------------------------------------- */

struct commit_queue;
struct mds_proxy_ctx;
struct ds_cache;

/**
 * @brief Attach a commit queue for DS admin write operations.
 */
void cluster_transport_server_set_cq(struct cluster_server *srv,
                                     struct commit_queue *cq);

/**
 * @brief Attach a proxy context and DS mount path format for live
 *        proxy mount management during DS state transitions.
 *
 * @param srv              Server handle.
 * @param proxy            Proxy I/O context.
 * @param mount_path_fmt   Format string for DS mount paths
 *                         (e.g. "/mnt/ds%u").
 */
void cluster_transport_server_set_proxy(struct cluster_server *srv,
                                        struct mds_proxy_ctx *proxy,
                                        const char *mount_path_fmt);

/**
 * @brief Attach the in-memory DS cache for admin-triggered refreshes.
 */
void cluster_transport_server_set_ds_cache(struct cluster_server *srv,
                                           struct ds_cache *cache);

/* -----------------------------------------------------------------------
 * DS registry admin (read-only)
 * ----------------------------------------------------------------------- */

/**
 * @brief Query the running MDS for its registered data servers.
 *
 * Wire: CT_MSG_DS_LIST_REQ (empty payload) / CT_MSG_DS_LIST_RESP.
 *
 * @param mds_host  Admin endpoint host.
 * @param mds_port  Admin endpoint port.
 * @param out       Receives malloc'd array of mds_ds_info.
 * @param count     Receives number of entries.
 * @return MDS_OK or error.
 */
enum mds_status cluster_transport_request_ds_list(
    const char *mds_host, uint16_t mds_port,
    struct mds_ds_info **out, uint32_t *count);

/**
 * @brief Register a new data server (starts in DS_OFFLINE).
 *
 * Wire: CT_MSG_DS_ADD_REQ / CT_MSG_DS_ADD_RESP.
 */
enum mds_status cluster_transport_request_ds_add(
    const char *mds_host, uint16_t mds_port,
    const struct mds_ds_info *info);

/**
 * @brief Change a data server's state.
 *
 * Wire: CT_MSG_DS_SET_STATE_REQ / CT_MSG_DS_SET_STATE_RESP.
 */
enum mds_status cluster_transport_request_ds_set_state(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id, uint32_t new_state);

/**
 * @brief Remove an offline, unreferenced data server.
 *
 * Wire: CT_MSG_DS_REMOVE_REQ / CT_MSG_DS_REMOVE_RESP.
 */
enum mds_status cluster_transport_request_ds_remove(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id);

/**
 * @brief Mark a DS as GPUDirect-validated.
 * Requires DS to exist and have RDMA transport.
 */
enum mds_status cluster_transport_request_ds_validate(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id, const char *tool_version, const char *note);

/**
 * @brief Clear GPUDirect validation from a DS.
 */
enum mds_status cluster_transport_request_ds_validate_clear(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id);

/**
 * @brief Provision a patched DS with a secret.
 */
enum mds_status cluster_transport_request_ds_provision(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id, const uint8_t *secret, uint32_t secret_len,
    uint64_t epoch);

/**
 * @brief Set patched kernel-profile readiness bits.
 */
enum mds_status cluster_transport_request_ds_patch_ready(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id, uint32_t bits);

/**
 * @brief Clear all patched readiness bits.
 */
enum mds_status cluster_transport_request_ds_patch_clear(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id);

struct split_evaluator;
struct split_proposal;

/**
 * @brief Attach the split evaluator to the transport server.
 */
void cluster_transport_server_set_evaluator(struct cluster_server *srv,
                                            struct split_evaluator *eval);

/**
 * @brief Tell the transport server whether same-MDS shard splits are active.
 *
 * When enabled, CT_MSG_SPLIT_ADMIN_REQ with dest=self is rejected until the
 * owned child-shard lifecycle is fully wired; this avoids advertising a
 * map-only split as a working local shard cutover.
 */
void cluster_transport_server_set_sharding(struct cluster_server *srv,
                                           bool enabled);

/**
 * @brief List split proposals from a running MDS daemon.
 * Caller frees *out with free().
 */
enum mds_status cluster_transport_request_split_proposals(
    const char *mds_host, uint16_t mds_port,
    struct split_proposal **out, uint32_t *count);

/**
 * @brief Approve a split proposal on a running MDS daemon.
 */
enum mds_status cluster_transport_request_split_approve(
    const char *mds_host, uint16_t mds_port,
    const char *path);

/**
 * @brief Set auto-execute on a running MDS daemon.
 */
enum mds_status cluster_transport_request_split_auto(
    const char *mds_host, uint16_t mds_port, bool enable);

struct failover_ctx;
struct repl_conn;

/**
 * @brief Attach failover context for rolling upgrade support.
 */
void cluster_transport_server_set_failover(struct cluster_server *srv,
                                           struct failover_ctx *fo,
                                           struct repl_conn *repl);

#endif /* CLUSTER_TRANSPORT_H */
/**
 * Send a cross-subtree nlink increment to the inode-owning MDS.
 * @param membership  Cluster membership handle.
 * @param target_mds  MDS that owns the inode.
 * @param fileid      Inode fileid to increment nlink on.
 * @return MDS_OK on success.
 */
enum mds_status cluster_transport_request_nlink_inc(
    const struct cluster_membership *membership,
    uint32_t target_mds, uint64_t fileid);

/**
 * Send a cross-subtree nlink decrement to the inode-owning MDS.
 * On nlink=0, the remote MDS handles GC.
 */
enum mds_status cluster_transport_request_nlink_dec(
    const struct cluster_membership *membership,
    uint32_t target_mds, uint64_t fileid);

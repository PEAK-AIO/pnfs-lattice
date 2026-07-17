/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * rpc_server.h -- TCP-based ONC-RPC server for NFSv4.1.
 *
 * Accepts TCP connections, reassembles ONC-RPC record-marked messages
 * (RFC 5531 S11), and dispatches NFS NULL (proc 0) and COMPOUND
 * (proc 1) procedures via the XDR codec and compound dispatch layers.
 *
 * Epoll event loop with optional thread-pool COMPOUND dispatch
 * (worker_threads > 1).  RDMA via libntirpc SVCXPRT deferred.
 *
 * See docs/architecture.md S4.1 for design overview.
 */

#ifndef RPC_SERVER_H
#define RPC_SERVER_H

#include "pnfs_mds.h"

#include <stdint.h>
#include <stdbool.h>

struct mds_catalogue;
struct session_table;
struct open_state_table;
struct lock_table;
struct mds_proxy_ctx;
struct health_monitor;
struct io_tracker;
struct mds_quota_ctx;
struct ds_prealloc_ctx;
struct ds_prepare_ctx;
struct ds_cache;
struct copy_offload_table;
struct rename_2pc_transport;
struct inode_cache;
struct dirent_cache;
struct layout_cache;
struct layout_commit_aggregator;
struct deleg_table;
struct dir_deleg_table;
struct layout_recall;

/* -----------------------------------------------------------------------
 * Configuration
 * ----------------------------------------------------------------------- */

/** Default NFS TCP port. */
#define RPC_DEFAULT_PORT     2049

/** Maximum simultaneous TCP connections.
 *  With 64+ MDS nodes and clients using nconnect=16, the default
 *  must be large enough for high fan-in.  Tunable via max_conns. */
#define RPC_MAX_CONNS        1024

/** Maximum single RPC record size (1 MB). */
#define RPC_MAX_RECORD_SIZE  (1U << 20)

/** Default per-connection in-flight request cap (bounded pipelining).
 *  Used when rpc_server_config.max_inflight_per_conn is 0. */
#define RPC_DEFAULT_MAX_INFLIGHT  8

/* Forward declarations for config member types. */
struct mds_gss_table;
struct commit_queue;
struct ds_health_monitor;
struct subtree_map;
struct cluster_membership;
struct threadpool;
struct mds_shard_map;

/**
 * RPC server configuration.
 *
 * Pass to rpc_server_create().  The db/st/ot pointers must remain
 * valid for the lifetime of the server.
 */
struct rpc_server_config {
    const char *bind_addr;      /**< Bind address (NULL = "0.0.0.0"). */
    uint16_t    port;           /**< TCP port (0 = RPC_DEFAULT_PORT). */
    uint32_t    mds_id;         /**< MDS node ID for stateid/deviceid. */
    uint64_t    write_verf;     /**< Server boot epoch for writeverf4. */
    uint32_t    stripe_unit;    /**< Stripe unit (0 = 64 KiB default). */
    uint8_t     ds_getdev_transport; /**< GETDEVICEINFO ds_transport: 0=tcp,1=rdma,2=both. */
    uint16_t    ds_rdma_port;       /**< RDMA port advertised (0 -> 20049). */
    bool        auto_widen_lease_on_4k; /**< Widen generic 4 KiB leases. */
    uint32_t    max_conns;      /**< Max connections (0 = RPC_MAX_CONNS). */
    /* Bounded request pipelining: max COMPOUNDs processed concurrently
     * per connection (0 = RPC_DEFAULT_MAX_INFLIGHT).  Lets NFSv4.1
     * session-slot parallelism use the worker pool instead of being
     * serialized one-at-a-time per TCP connection. */
    uint32_t    max_inflight_per_conn;
    /* Phase 1: placement policy for new files' layouts.  Ignored
     * unless placement_policy_enabled is true. */
    enum mds_placement_policy placement_policy;
    bool        placement_policy_enabled;
    /* Phase 3: default stripe geometry.  Ignored unless the
     * flag above is true.  Each defaults to 1. */
    uint32_t    default_stripe_count;
    uint32_t    default_mirror_count;
    /* Phase F of docs/hpc-nto1-plan.md -- HPC-Shared GETATTR
     * consistency mode.  Default MDS_HPC_GETATTR_STRICT preserves
     * POSIX stat() semantics by forcing a flush of any in-memory
     * LAYOUTCOMMIT aggregate before serving the reply. */
    enum mds_hpc_getattr_mode hpc_getattr_mode;
    /* Phase C / Step 5 of docs/hpc-nto1-plan.md -- wide pre-warm
     * stripe-count cap for HPC-Shared CREATEs.  0 selects the
     * compile-time default (128, matches src/common/config.c).
     * Clamped at use site by MDS_MAX_STRIPES and the count of
     * ONLINE GENERIC DSes; an HPC CREATE with 0 ONLINE DSes
     * surfaces NFS4ERR_NOSPC to the client. */
    uint32_t    hpc_max_stripe_count;
    /* Phase C of docs/hpc-nto1-plan.md -- flex-files layout XDR wire
     * form for HPC-Shared inodes.  AUTO emits striped form for
     * HPC-Shared inodes with mirror_count == 1 && stripe_count > 1
     * and legacy form for everything else; LEGACY / STRIPED force
     * the form unconditionally (used by lab and by operators with
     * mixed-kernel fleets).  See enum mds_hpc_xdr_form. */
    enum mds_hpc_xdr_form hpc_xdr_form;
    bool hpc_serve_layouts;
    bool serve_layouts;

    struct mds_catalogue    *cat; /**< Catalogue handle (authoritative). */
    struct session_table    *st;  /**< Session/clientid manager. */
    struct open_state_table *ot;
    struct lock_table *lt;  /**< Open state manager. */
    struct commit_queue    *cq;  /**< Commit queue (NULL = direct txns). */
    struct ds_health_monitor *ds_hm; /**< DS health monitor (may be NULL). */
    struct subtree_map              *smap; /**< Subtree map (may be NULL). */
    const struct cluster_membership *membership; /**< Membership (may be NULL). */

    /* Extended compound_data context (Item 52). */
    struct mds_proxy_ctx    *proxy;     /**< Proxy I/O (may be NULL). */
    struct health_monitor   *hm;        /**< Replication health (may be NULL). */
    struct io_tracker       *io_tracker; /**< Tiering I/O heat (may be NULL). */
    struct mds_quota_ctx    *quota;     /**< Quota enforcement (may be NULL). */
    struct ds_prealloc_ctx  *prealloc;  /**< DS pre-alloc pool (NULL = inline mode). */
    struct ds_prepare_ctx   *ds_prepare; /**< Generic DS async prepare queue. */
    struct ds_cache         *ds_cache;   /**< In-memory DS registry cache. */
    struct copy_offload_table *cot;     /**< Async COPY state (may be NULL). */
    const struct rename_2pc_transport *transport; /**< Rename 2PC (may be NULL). */
    bool gpudirect_required; /**< Require 0 for placement. */
    bool skip_transient_ndb; /**< Skip NDB for open/layout state. */
    bool hide_referral_junctions; /**< Hide /shardN referral junctions from root READDIR. */
    bool posix_dac; /**< Enforce POSIX permission semantics for AUTH_SYS. */
    bool referral_strict; /**< NFS4ERR_MOVED for foreign-shard FHs. */
    enum nfs_auth_mode min_auth; /**< Minimum NFS auth level. */
    struct mds_gss_table *gss_tbl; /**< GSS context table (NULL if SYS). */
    struct threadpool *tp; /**< Worker pool for COMPOUND dispatch (NULL = inline). */
    struct mds_shard_map *shard_map; /**< Shard routing map (NULL = no routing). */
    struct inode_cache *icache; /**< Global inode LRU cache (NULL = no caching). */
    struct dirent_cache *dcache; /**< Global dirent+negative LRU cache (NULL = no caching). */
    /* Phase D of docs/hpc-nto1-plan.md -- per-inode stripe-map cache,
     * exclusively for HPC-Shared inodes.  NULL = no caching (every
     * LAYOUTGET reads the catalogue, matching pre-Phase-D
     * behaviour). */
    struct layout_cache *lcache;
    /* Phase F of docs/hpc-nto1-plan.md -- LAYOUTCOMMIT aggregator,
     * exclusively for HPC-Shared inodes.  NULL = no aggregator
     * (every LAYOUTCOMMIT writes synchronously, matching
     * pre-Phase-F behaviour). */
    struct layout_commit_aggregator *lcommit_agg;
    struct deleg_table *dt;
    struct dir_deleg_table *ddt; /**< Dir delegation table (NULL when feature off). */
    /*
     * Layout recall coordinator (RFC 8881 S12.5.5 / DS-failure /
     * admin recall / Mark's byte-range partial recall on op_layoutget).
     * NULL means "no conflict-recall"; the LAYOUTGET path skips the
     * holder scan and grants without consulting other clients,
     * matching pre-byte-range-fix behaviour.
     */
    struct layout_recall *lr;
};

/* -----------------------------------------------------------------------
 * Opaque server handle
 * ----------------------------------------------------------------------- */

struct rpc_conn;    /* Defined in rpc_server.c */
struct rpc_server;  /* Defined in rpc_server.c */

/* -----------------------------------------------------------------------
 * Lifecycle API
 * ----------------------------------------------------------------------- */

/**
 * Create an RPC server.
 *
 * Allocates the server, creates the listening socket, and binds it.
 * Does not start accepting connections -- call rpc_server_start() for that.
 *
 * @param cfg  Configuration.  The struct is copied; the caller may free it.
 * @param out  Receives the server handle.
 * @return 0 on success, -1 on error (socket/bind failure, allocation).
 */
int rpc_server_create(const struct rpc_server_config *cfg,
                      struct rpc_server **out);

/**
 * Run the server event loop (blocking).
 *
 * Accepts connections and processes NFS RPCs until rpc_server_stop()
 * is called from another thread or a signal handler.
 *
 * @param srv  Server handle from rpc_server_create().
 * @return 0 on clean shutdown, -1 on error.
 */
int rpc_server_start(struct rpc_server *srv);

/**
 * Signal the server to stop.
 *
 * Safe to call from another thread or a signal handler.  The event
 * loop in rpc_server_start() will return shortly after this call.
 *
 * @param srv  Server handle.
 */
void rpc_server_stop(struct rpc_server *srv);

/**
 * Destroy the server and free all resources.
 *
 * Closes the listening socket, all client connections, and frees
 * memory.  Must not be called while the event loop is running
 * (call rpc_server_stop() first and wait for rpc_server_start()
 * to return).
 *
 * @param srv  Server handle.  NULL is tolerated.
 */
void rpc_server_destroy(struct rpc_server *srv);

/**
 * Return the actual port the server is listening on.
 *
 * Useful when port=0 was passed (OS-assigned ephemeral port) or
 * for diagnostic purposes.
 *
 * @param srv  Server handle.
 * @return Port number in host byte order, or 0 if not yet bound.
 */
uint16_t rpc_server_port(const struct rpc_server *srv);

/**
 * Get the file descriptor from an opaque rpc_conn.
 *
 * @param c  Connection handle.
 * @return fd, or -1 if c is NULL.
 */
int rpc_conn_get_fd(const struct rpc_conn *c);

#endif /* RPC_SERVER_H */

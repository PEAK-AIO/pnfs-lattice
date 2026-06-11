/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * cluster_transport.c -- Lightweight TCP transport for inter-MDS 2PC.
 *
 * Wire format (all multi-byte fields big-endian):
 *   Header: [msg_type u8][payload_len u32]  (5 bytes)
 *   PREPARE (1): [txn_id 8][dst_parent 8][dst_name_len 2][dst_name ...]
 *                [inode_data_len 4][inode_data ...]
 *   VOTE    (2): [txn_id 8][vote u8]  (1=commit, 0=abort)
 *   COMMIT  (3): [txn_id 8]
 *   ABORT   (4): [txn_id 8]
 *   ACK     (5): [txn_id 8][status u8]  (0=ok, 1=error)
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>
#include <endian.h>
#include <pthread.h>
#include <time.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "pnfs_mds.h"
#include "subtree_map.h"
#include "cluster_transport.h"
#include "mds_catalogue.h"
#include "rename_2pc.h"
#include "mds_tls.h"
#include "mds_metrics.h"
#include "commit_queue.h"
#include "proxy_io.h"
#include "ds_cache.h"
#include "ds_capacity.h"
#include "resilver.h"
#include "rebalance.h"
#include "tiering.h"
#include "quota.h"
#include "migration.h"
#include "cluster_membership.h"
#include "cluster_drain.h"
#include "subtree_split.h"
#include "failover.h"

/* rename_2pc catalogue-aware wrappers are declared in rename_2pc.h
 * (included above).  They use the catalogue handle internally
 * so this file operates through the catalogue abstraction. */

/* -----------------------------------------------------------------------
 * Wire helpers
 *
 * When tl_tls_conn is non-NULL (set by the per-connection thread) the
 * helpers route I/O through the TLS layer.  Otherwise they fall back
 * to raw send()/recv().  This avoids changing every handler function
 * signature while enabling encrypted transport on the server side.
 * ----------------------------------------------------------------------- */

static _Thread_local struct mds_tls_conn *tl_tls_conn = NULL;

static int send_all(int fd, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    size_t sent = 0;

    while (sent < len) {
        ssize_t n;
        if (tl_tls_conn != NULL) {
            n = mds_tls_write(tl_tls_conn, p + sent, len - sent);
        } else {
            n = send(fd, p + sent, len - sent, MSG_NOSIGNAL);
        }
        if (n <= 0) {
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

static int recv_all(int fd, void *buf, size_t len)
{
    uint8_t *p = buf;
    size_t got = 0;

    while (got < len) {
        ssize_t n;
        if (tl_tls_conn != NULL) {
            n = mds_tls_read(tl_tls_conn, p + got, len - got);
        } else {
            n = recv(fd, p + got, len - got, 0);
        }
        if (n <= 0) {
            return -1;
        }
        got += (size_t)n;
    }
    return 0;
}

static int send_header(int fd, uint8_t msg_type, uint32_t payload_len)
{
    uint8_t hdr[CT_HEADER_SIZE];
    uint32_t be_len = htobe32(payload_len);

    hdr[0] = msg_type;
    memcpy(hdr + 1, &be_len, 4);
    return send_all(fd, hdr, CT_HEADER_SIZE);
}

static int recv_header(int fd, uint8_t *msg_type, uint32_t *payload_len)
{
    uint8_t hdr[CT_HEADER_SIZE];

    if (recv_all(fd, hdr, CT_HEADER_SIZE) != 0) {
        return -1;
    }
    *msg_type = hdr[0];
    uint32_t be_len;
    memcpy(&be_len, hdr + 1, 4);
    *payload_len = be32toh(be_len);
    return 0;
}

/**
 * Decode a 1-byte wire status into mds_status.
 * Wire encoding: 0 = MDS_OK, 1..19 = negated mds_status.
 * Out-of-range values map to MDS_ERR_IO.
 */
static enum mds_status decode_wire_status(uint8_t wire_byte)
{
    if (wire_byte == 0) {
        return MDS_OK;
}
    if (wire_byte <= 19) {
        /* NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange) */
        return (enum mds_status)(-(int)wire_byte);
}
    return MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Server
 * ----------------------------------------------------------------------- */

#define CT_MAX_PEERS 128  /* Must be >= MDS_MAX_NODES for full-mesh clusters */

struct cluster_server {
    int                listen_fd;
    uint16_t           port;
    struct mds_catalogue *cat;
    /* catalogue handle -- all handlers use srv->cat. */
    struct subtree_map *smap;
    pthread_t          thread;
    _Atomic bool      running;

    /* Peer allowlist (Finding 1 remediation). */
    struct in_addr     allowed_peers[CT_MAX_PEERS];
    uint32_t           allowed_peer_count;

    /* Admin-only allowed hosts (web UI / monitoring). */
    struct in_addr     admin_allowed_hosts[32];
    uint32_t           admin_allowed_host_count;

    /* Per-connection threading (Finding 2 remediation). */
    atomic_uint        conn_count;
    uint32_t           max_conns;

    /* Active connection FD tracking for clean shutdown. */
    int               *conn_fds;
    uint32_t           conn_fd_count;
    pthread_mutex_t    conn_fds_lock;

    /* Resilver worker (set via cluster_transport_server_set_resilver). */
    struct resilver_worker *resilver;

    /* Rebalance worker (set via cluster_transport_server_set_rebalance). */
    struct rebalance_worker *rebalance;

    /* Tiering worker (set via cluster_transport_server_set_tiering). */
    struct tiering_worker *tiering;

    /* Cluster membership (set via cluster_transport_server_set_membership). */
    struct cluster_membership *membership;

    /* Migration tracker (set via cluster_transport_server_set_migration_tracker). */
    struct migration_tracker *tracker;

    /* Quota context (set via cluster_transport_server_set_quota). */
    struct mds_quota_ctx *quota;

    /* Failover context for rolling upgrade (set via
     * cluster_transport_server_set_failover). */
    struct failover_ctx *fo_ctx;
    struct repl_conn    *fo_repl;

    /* TLS context for inter-MDS connections (NULL = plaintext). */
    struct mds_tls_ctx  *tls;

    /* Commit queue for DS admin writes (Item 47b). */
    struct commit_queue *cq;

    /* Proxy I/O context + mount path format for DS lifecycle. */
    struct mds_proxy_ctx *proxy;
    struct ds_cache     *ds_cache;
    char ds_mount_path_fmt[512];

    /* Split evaluator (Tier 3 Phase 1). */
    struct split_evaluator *evaluator;

    /* True when same-MDS split requests would imply child-shard cutover. */
    bool sharding_enabled;

    /* C2: borrowed pointer to the daemon's live config.
     * NULL disables CONFIG_SHOW (responds with MDS_ERR_INVAL). */
    const struct mds_config *cfg;
};

/**
 * Check if a peer address is in the allowlist.
 * Returns true if allowed (empty list means accept all).
 *
 * Two ACLs are checked:
 *   1. cluster_peer[] -- inter-MDS peers (populated from cluster_peer[N]).
 *   2. admin_allowed_hosts -- monitoring / web-UI hosts (populated from
 *      admin_allowed_hosts INI key).  Separated so operators can grant
 *      admin-port access without adding a host as a cluster member.
 *
 * If both lists are empty and TLS is off, loopback-only is enforced.
 */
static bool peer_is_allowed(const struct cluster_server *srv,
                            const struct sockaddr_in *peer)
{
    if (srv->allowed_peer_count == 0 &&
        srv->admin_allowed_host_count == 0) {
        /* No explicit allowlist.  If TLS is also not configured,
         * restrict to loopback only (3.4: unauthenticated admin
         * transport must not be reachable from the network). */
        if (srv->tls == NULL) {
            uint32_t addr = ntohl(peer->sin_addr.s_addr);
            return (addr >> 24) == 127;
        }
        return true;
    }

    /* Check cluster peers. */
    for (uint32_t i = 0; i < srv->allowed_peer_count; i++) {
        /* NOLINTNEXTLINE(clang-analyzer-core.UndefinedBinaryOperatorResult) */
        if (peer->sin_addr.s_addr == srv->allowed_peers[i].s_addr) {
            return true;
        }
    }

    /* Check admin allowed hosts. */
    for (uint32_t i = 0; i < srv->admin_allowed_host_count; i++) {
        if (peer->sin_addr.s_addr == srv->admin_allowed_hosts[i].s_addr) {
            return true;
        }
    }
    return false;
}

/**
 * Register a connection FD so server_stop can shutdown() it.
 */
static void register_conn_fd(struct cluster_server *srv, int fd)
{
    pthread_mutex_lock(&srv->conn_fds_lock);
    if (srv->conn_fd_count < srv->max_conns) {
        srv->conn_fds[srv->conn_fd_count++] = fd;
    }
    pthread_mutex_unlock(&srv->conn_fds_lock);
}

/**
 * Unregister a connection FD (must be called before close()).
 */
static void unregister_conn_fd(struct cluster_server *srv, int fd)
{
    pthread_mutex_lock(&srv->conn_fds_lock);
    for (uint32_t i = 0; i < srv->conn_fd_count; i++) {
        if (srv->conn_fds[i] == fd) {
            srv->conn_fds[i] = srv->conn_fds[srv->conn_fd_count - 1];
            srv->conn_fd_count--;
            break;
        }
    }
    pthread_mutex_unlock(&srv->conn_fds_lock);
}

/**
 * Check if a destination host is in the allowed peer list.
 * Used by handle_mig_admin to validate outbound connections.
 * Returns true if the host resolves to an allowed peer address.
 */
static bool dest_host_is_allowed(const struct cluster_server *srv,
                                 const char *host)
{
    if (srv->allowed_peer_count == 0) {
        return true; /* No allowlist -- allow all destinations. */
}

    struct in_addr addr;

    if (inet_pton(AF_INET, host, &addr) == 1) {
        for (uint32_t i = 0; i < srv->allowed_peer_count; i++) {
            if (addr.s_addr == srv->allowed_peers[i].s_addr) {
                return true;
}
        }
        return false;
    }

    /* Try hostname resolution. */
    struct addrinfo hints = {0}, *res = NULL;

    hints.ai_family = AF_INET;
    if (getaddrinfo(host, NULL, &hints, &res) != 0 || res == NULL) {
        return false;
}

    bool found = false;

    for (struct addrinfo *r = res; r != NULL; r = r->ai_next) {
        const struct sockaddr_in *sin = (struct sockaddr_in *)r->ai_addr;

        for (uint32_t i = 0; i < srv->allowed_peer_count; i++) {
            if (sin->sin_addr.s_addr == srv->allowed_peers[i].s_addr) {
                found = true;
                break;
            }
        }
        if (found) {
            break;
}
    }
    freeaddrinfo(res);
    return found;
}

/**
 * Handle one PREPARE message on a connection.
 */
/* Forward declarations for membership handlers (defined at end of file). */
static void handle_node_join(struct cluster_server *srv, int conn_fd,
                             const uint8_t *payload, uint32_t plen);
static void handle_node_leave(struct cluster_server *srv, int conn_fd,
                              const uint8_t *payload, uint32_t plen);
static void handle_cluster_status(const struct cluster_server *srv, int conn_fd,
                                  const uint8_t *payload, uint32_t plen);
static void handle_set_lifecycle(struct cluster_server *srv, int conn_fd,
                                 const uint8_t *payload, uint32_t plen);
static void handle_subtree_list_admin(const struct cluster_server *srv,
                                      int conn_fd,
                                      const uint8_t *payload, uint32_t plen);
static void handle_mig_progress_admin(const struct cluster_server *srv,
                                      int conn_fd,
                                      const uint8_t *payload, uint32_t plen);

static void handle_prepare(struct cluster_server *srv, int conn_fd,
                           const uint8_t *payload, uint32_t plen)
{
    /* Parse: [txn_id 8][dst_parent 8][name_len 2][name ...][idata_len 4][idata ...] */
    if (plen < 8 + 8 + 2) {
        return;
    }

    size_t off = 0;
    uint64_t txn_id_be, dst_parent_be;

    memcpy(&txn_id_be, payload + off, 8);     off += 8;
    memcpy(&dst_parent_be, payload + off, 8); off += 8;

    uint64_t txn_id = be64toh(txn_id_be);
    uint64_t dst_parent = be64toh(dst_parent_be);

    uint16_t name_len_be;
    memcpy(&name_len_be, payload + off, 2); off += 2;
    uint16_t name_len = be16toh(name_len_be);

    if (off + name_len + 4 > plen || name_len >= 256) {
        return;
    }

    char dst_name[256];
    memcpy(dst_name, payload + off, name_len); off += name_len;
    dst_name[name_len] = '\0';

    uint32_t idata_len_be;
    memcpy(&idata_len_be, payload + off, 4); off += 4;
    uint32_t idata_len = be32toh(idata_len_be);

    if (off + idata_len > plen) {
        return;
    }

    const void *inode_data = payload + off;

    /* Dispatch to participant via catalogue wrapper. */
    int vote = rename_2pc_on_prepare(srv->cat, txn_id, dst_parent,
                                     dst_name, inode_data, idata_len, 0);

    /* Send VOTE response. */
    uint8_t vote_payload[9];
    uint64_t txn_be = htobe64(txn_id);
    memcpy(vote_payload, &txn_be, 8);
    vote_payload[8] = (uint8_t)(vote == 1 ? 1 : 0);

    send_header(conn_fd, CT_MSG_VOTE, 9);
    send_all(conn_fd, vote_payload, 9);
}

static void handle_commit(struct cluster_server *srv, int conn_fd,
                          const uint8_t *payload, uint32_t plen)
{
    if (plen < 8) {
        return;
    }

    uint64_t txn_id_be;
    memcpy(&txn_id_be, payload, 8);
    uint64_t txn_id = be64toh(txn_id_be);

    enum mds_status st = rename_2pc_on_commit(srv->cat, txn_id);

    /* Send ACK. */
    uint8_t ack[9];
    uint64_t txn_be = htobe64(txn_id);
    memcpy(ack, &txn_be, 8);
    ack[8] = (st == MDS_OK) ? 0 : 1;

    send_header(conn_fd, CT_MSG_ACK, 9);
    send_all(conn_fd, ack, 9);
}

static void handle_abort(struct cluster_server *srv, int conn_fd,
                         const uint8_t *payload, uint32_t plen)
{
    if (plen < 8) {
        return;
    }

    uint64_t txn_id_be;
    memcpy(&txn_id_be, payload, 8);
    uint64_t txn_id = be64toh(txn_id_be);

    enum mds_status st = rename_2pc_on_abort(srv->cat, txn_id);

    /* Send ACK. */
    uint8_t ack[9];
    uint64_t txn_be = htobe64(txn_id);
    memcpy(ack, &txn_be, 8);
    ack[8] = (st == MDS_OK) ? 0 : 1;

    send_header(conn_fd, CT_MSG_ACK, 9);
    send_all(conn_fd, ack, 9);
}

/* -----------------------------------------------------------------------
 * Migration server-side handlers
 *
 * Wire format:
 *   MIG_BEGIN  (6): [path_len 2 BE][path ...]
 *   MIG_INODE  (7): [chunk_data ...]  (serialised mig_inode_chunk)
 *   MIG_FINISH (8): [total_inodes 4 BE]
 *   MIG_ACK    (9): [status u8]  (0=ok, 1=error)
 *   MIG_NACK  (10): [status u8]
 * ----------------------------------------------------------------------- */

static void handle_mig_begin(struct cluster_server *srv, int conn_fd,
                             /* NOLINTNEXTLINE(misc-unused-parameters) */
                             const uint8_t *payload, uint32_t plen)
{
    (void)srv;
    /* Parse path (informational only -- dest just ACKs). */
    uint8_t ack_status = 0;  /* 0 = OK */
    if (plen < 2) {
        ack_status = 1;
}

    send_header(conn_fd, CT_MSG_MIG_ACK, 1);
    send_all(conn_fd, &ack_status, 1);
}

static void handle_mig_inode(struct cluster_server *srv, int conn_fd,
                             const uint8_t *payload, uint32_t plen)
{
    struct mig_inode_chunk chunk;
    int rc;

    rc = mig_chunk_deserialise(payload, plen, &chunk);
    if (rc != 0) {
        uint8_t nack = 1;
        send_header(conn_fd, CT_MSG_MIG_NACK, 1);
        send_all(conn_fd, &nack, 1);
        return;
    }

    enum mds_status st = migration_apply_chunk(srv->cat, &chunk);
    mig_chunk_free(&chunk);

    uint8_t status = (st == MDS_OK) ? 0 : 1;
    uint8_t resp_type = (st == MDS_OK) ? CT_MSG_MIG_ACK : CT_MSG_MIG_NACK;
    send_header(conn_fd, resp_type, 1);
    send_all(conn_fd, &status, 1);
}

static void handle_mig_finish(struct cluster_server *srv, int conn_fd,
                              const uint8_t *payload, uint32_t plen)
{
    (void)srv;
    (void)payload;
    (void)plen;

    uint8_t ack_status = 0;
    send_header(conn_fd, CT_MSG_MIG_ACK, 1);
    send_all(conn_fd, &ack_status, 1);
}

/* -----------------------------------------------------------------------
 * Admin-triggered migration handler
 *
 * Wire format (CT_MSG_MIG_ADMIN_REQ):
 *   [path_len 2 BE][path ...][dest_mds_id 4 BE]
 *   [dest_host_len 2 BE][dest_host ...][dest_port 2 BE]
 *
 * Response (CT_MSG_MIG_ADMIN_RESP):
 *   [status u8]  (0=ok, non-zero=mds_status error code)
 * ----------------------------------------------------------------------- */


/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void handle_mig_admin(struct cluster_server *srv, int conn_fd,
                             const uint8_t *payload, uint32_t plen)
{
    enum mds_status st = MDS_ERR_INVAL;

    /* Need smap and catalogue for admin migration. */
    if (srv->smap == NULL || srv->cat == NULL) {
        goto respond;
    }

    /* Parse request. */
    if (plen < 2) {
        goto respond;
}

    size_t off = 0;
    uint16_t path_len_be;
    memcpy(&path_len_be, payload + off, 2); off += 2;
    uint16_t path_len = be16toh(path_len_be);
    if (path_len == 0 || off + path_len + 4 + 2 + 2 > plen) {
        goto respond;
}

    char subtree_path[MDS_MAX_PATH];
    if (path_len >= MDS_MAX_PATH) {
        goto respond;
}
    memcpy(subtree_path, payload + off, path_len); off += path_len;
    subtree_path[path_len] = '\0';

    uint32_t dest_id_be;
    memcpy(&dest_id_be, payload + off, 4); off += 4;
    uint32_t dest_mds_id = be32toh(dest_id_be);

    if (off + 2 > plen) {
        goto respond;
}
    uint16_t host_len_be;
    memcpy(&host_len_be, payload + off, 2); off += 2;
    uint16_t host_len = be16toh(host_len_be);
    if (off + host_len + 2 > plen || host_len >= 256) {
        goto respond;
}

    char dest_host[256];
    if (host_len > 0) {
        memcpy(dest_host, payload + off, host_len);
        dest_host[host_len] = '\0';
    } else {
        dest_host[0] = '\0';
    }
    off += host_len;

    uint16_t dest_port_be;
    memcpy(&dest_port_be, payload + off, 2); off += 2;
    uint16_t dest_port = be16toh(dest_port_be);
    (void)off;

    /* Resolve destination via membership if available. */
    if (srv->membership != NULL) {
        st = cluster_membership_resolve_peer(
            srv->membership, dest_mds_id,
            dest_host, sizeof(dest_host), &dest_port);
        if (st != MDS_OK) {
            goto respond;
}
    } else if (host_len == 0) {
        /* No membership and no wire host -- cannot resolve. */
        goto respond;
    }

    /* Preflight: ensure destination can own subtrees before streaming. */
    if (srv->membership != NULL &&
        !cluster_membership_can_own_subtrees(srv->membership, dest_mds_id)) {
        st = MDS_ERR_PERM;
        goto respond;
    }

    /* Resolve subtree path to root fileid. */
    uint64_t root_fid = 0;
    st = mds_cat_resolve_path(srv->cat, subtree_path, &root_fid);
    if (st != MDS_OK) {
        goto respond;
    }

    /* Exact-root check: path must already be a subtree root.
     * The legacy auto-register behaviour was removed in Seq 7 to
     * enforce the discipline that only subtree split creates new
     * subtree entries.  Use 'subtree split' to carve a new child
     * or 'subtree assign' for exact-root migration. */
    {
        struct subtree_entry check;
        st = subtree_map_lookup_exact(srv->smap, subtree_path, &check);
        if (st != MDS_OK) {
            st = MDS_ERR_NOTFOUND;
            goto respond;
        }
    }

    /* Destination validation (Finding 1 remediation):
     * refuse to connect to addresses outside the known peer set. */
    if (!dest_host_is_allowed(srv, dest_host)) {
        st = MDS_ERR_PERM;
        goto respond;
    }

    /* Connect migration transport to destination MDS. */
    struct migration_transport *mt = NULL;
    st = cluster_transport_connect_migration(
        dest_host, dest_port, &mt);
    if (st != MDS_OK) {
        goto respond;
}

    /* Execute migration using daemon's live db and smap. */
    st = migration_initiate(srv->cat, srv->smap, mt,
                            subtree_path, root_fid, dest_mds_id,
                            NULL);
    cluster_transport_disconnect_migration(mt);

respond:;
    /* Encode: 0=OK, else negated mds_status (positive byte on wire). */
    uint8_t resp_status = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);
    send_header(conn_fd, CT_MSG_MIG_ADMIN_RESP, 1);
    send_all(conn_fd, &resp_status, 1);
}

/* -----------------------------------------------------------------------
 * Resilver admin handlers
 * ----------------------------------------------------------------------- */

void cluster_transport_server_set_resilver(struct cluster_server *srv,
                                           struct resilver_worker *rw)
{
    if (srv != NULL) {
        srv->resilver = rw;
}
}

static void handle_resilver_start(struct cluster_server *srv, int conn_fd,
                                  const uint8_t *payload, uint32_t plen)
{
    uint8_t resp_status = 1; /* error by default */

    if (srv->resilver == NULL || plen < 4) {
        goto respond;
}

    uint32_t ds_id_be;
    memcpy(&ds_id_be, payload, 4);
    uint32_t ds_id = be32toh(ds_id_be);

    int rc = resilver_start(srv->resilver, ds_id);
    resp_status = (rc == 0) ? 0 : 1;

respond:
    send_header(conn_fd, CT_MSG_RESILVER_RESP, 1);
    send_all(conn_fd, &resp_status, 1);
}

static void handle_resilver_status(const struct cluster_server *srv, int conn_fd,
                                   const uint8_t *payload, uint32_t plen)
{
    (void)payload;
    (void)plen;

    uint8_t resp[17]; /* state(1) + files_total(4) + files_done(4) + bytes_copied(8) */
    memset(resp, 0, sizeof(resp));

    if (srv->resilver != NULL) {
        uint32_t state, total, done;
        uint64_t bytes;
        resilver_status(srv->resilver, &total, &done, &bytes, &state);
        resp[0] = (uint8_t)state;
        uint32_t be32;
        uint64_t be64;
        be32 = htobe32(total);
        memcpy(resp + 1, &be32, 4);
        be32 = htobe32(done);
        memcpy(resp + 5, &be32, 4);
        be64 = htobe64(bytes);
        memcpy(resp + 9, &be64, 8);
    }

    send_header(conn_fd, CT_MSG_RESILVER_RESP, sizeof(resp));
    send_all(conn_fd, resp, sizeof(resp));
}

/* -----------------------------------------------------------------------
 * Extended resilver status handler (Seq 11 -- skip counters)
 * ----------------------------------------------------------------------- */

static void handle_resilver_status_ext(const struct cluster_server *srv,
                                       int conn_fd,
                                       const uint8_t *payload, uint32_t plen)
{
    (void)payload;
    (void)plen;

    /* Response: state(1) + files_total(4) + files_done(4) + bytes_copied(8)
     *         + pass_count(4) + 5 skip counters * 4 = 41 bytes. */
    uint8_t resp[41];
    memset(resp, 0, sizeof(resp));

    if (srv->resilver != NULL) {
        struct resilver_status_info info;
        resilver_status_ext(srv->resilver, &info);

        resp[0] = (uint8_t)info.state;

        uint32_t be32;
        uint64_t be64;

        be32 = htobe32(info.files_total);
        memcpy(resp + 1, &be32, 4);
        be32 = htobe32(info.files_done);
        memcpy(resp + 5, &be32, 4);
        be64 = htobe64(info.bytes_copied);
        memcpy(resp + 9, &be64, 8);
        be32 = htobe32(info.pass_count);
        memcpy(resp + 17, &be32, 4);
        be32 = htobe32(info.skipped_active_layout);
        memcpy(resp + 21, &be32, 4);
        be32 = htobe32(info.skipped_active_writer);
        memcpy(resp + 25, &be32, 4);
        be32 = htobe32(info.skipped_sparse);
        memcpy(resp + 29, &be32, 4);
        be32 = htobe32(info.skipped_multi_stripe);
        memcpy(resp + 33, &be32, 4);
        be32 = htobe32(info.skipped_error);
        memcpy(resp + 37, &be32, 4);
    }

    send_header(conn_fd, CT_MSG_RESILVER_RESP, sizeof(resp));
    send_all(conn_fd, resp, sizeof(resp));
}


/* -----------------------------------------------------------------------
 * Rebalance admin handlers (S3.8 mirror relocation V1)
 * ----------------------------------------------------------------------- */

void cluster_transport_server_set_rebalance(struct cluster_server *srv,
                                             struct rebalance_worker *rw)
{
    if (srv != NULL) {
        srv->rebalance = rw;
    }
}

static void handle_rebalance_start(struct cluster_server *srv, int conn_fd,
                                   const uint8_t *payload, uint32_t plen)
{
    enum mds_status st = MDS_ERR_INVAL;

    if (srv->rebalance == NULL || plen < 8) {
        goto respond;
    }

    uint32_t src_be, tgt_be;
    memcpy(&src_be, payload, 4);
    memcpy(&tgt_be, payload + 4, 4);
    uint32_t src_ds = be32toh(src_be);
    uint32_t tgt_ds = be32toh(tgt_be);

    st = rebalance_start(srv->rebalance, src_ds, tgt_ds);

respond:;
    int32_t st_be = (int32_t)htobe32((uint32_t)(int32_t)st);
    send_header(conn_fd, CT_MSG_REBALANCE_RESP, 4);
    send_all(conn_fd, &st_be, 4);
}

static void handle_rebalance_status(const struct cluster_server *srv,
                                    int conn_fd,
                                    const uint8_t *payload, uint32_t plen)
{
    (void)payload;
    (void)plen;

    /* Response: state(1) + source_ds(4) + target_ds(4) +
     *           candidates_in_pass(4) + files_moved(4) + bytes_copied(8) +
     *           pass_count(4) + 5 skip counters * 4 = 49 bytes. */
    uint8_t resp[49];
    memset(resp, 0, sizeof(resp));

    if (srv->rebalance != NULL) {
        struct rebalance_status_info info;
        rebalance_status_ext(srv->rebalance, &info);

        resp[0] = (uint8_t)info.state;
        uint32_t be32;
        uint64_t be64;

        be32 = htobe32(info.source_ds);
        memcpy(resp + 1, &be32, 4);
        be32 = htobe32(info.target_ds);
        memcpy(resp + 5, &be32, 4);
        be32 = htobe32(info.candidates_in_pass);
        memcpy(resp + 9, &be32, 4);
        be32 = htobe32(info.files_moved);
        memcpy(resp + 13, &be32, 4);
        be64 = htobe64(info.bytes_copied);
        memcpy(resp + 17, &be64, 8);
        be32 = htobe32(info.pass_count);
        memcpy(resp + 25, &be32, 4);
        be32 = htobe32(info.skipped_active_layout);
        memcpy(resp + 29, &be32, 4);
        be32 = htobe32(info.skipped_active_writer);
        memcpy(resp + 33, &be32, 4);
        be32 = htobe32(info.skipped_sparse);
        memcpy(resp + 37, &be32, 4);
        be32 = htobe32(info.skipped_multi_stripe);
        memcpy(resp + 41, &be32, 4);
        be32 = htobe32(info.skipped_error);
        memcpy(resp + 45, &be32, 4);
    }

    send_header(conn_fd, CT_MSG_REBALANCE_RESP, sizeof(resp));
    send_all(conn_fd, resp, sizeof(resp));
}

/* -----------------------------------------------------------------------
 * Tiering admin handlers (S3.9 storage tiering V1)
 * ----------------------------------------------------------------------- */

void cluster_transport_server_set_tiering(struct cluster_server *srv,
                                           struct tiering_worker *tw)
{
    if (srv != NULL) {
        srv->tiering = tw;
    }
}

static void handle_tiering_start(struct cluster_server *srv, int conn_fd,
                                  const uint8_t *payload, uint32_t plen)
{
    enum mds_status st = MDS_ERR_INVAL;

    if (srv->tiering == NULL || plen < 24) {
        goto respond;
    }

    struct tiering_config cfg;
    memset(&cfg, 0, sizeof(cfg));

    uint32_t be32;
    uint64_t be64;
    memcpy(&be32, payload, 4);
    cfg.eval_interval_sec = be32toh(be32);
    memcpy(&be64, payload + 4, 8);
    uint64_t prom_milli = be64toh(be64);
    cfg.promote_threshold = (double)prom_milli / 1000.0;
    memcpy(&be64, payload + 12, 8);
    uint64_t dem_milli = be64toh(be64);
    cfg.demote_threshold = (double)dem_milli / 1000.0;
    memcpy(&be32, payload + 20, 4);
    cfg.cooldown_evals = be32toh(be32);

    st = tiering_start(srv->tiering, &cfg);

respond:;
    int32_t st_be = (int32_t)htobe32((uint32_t)(int32_t)st);
    send_header(conn_fd, CT_MSG_TIERING_RESP, 4);
    send_all(conn_fd, &st_be, 4);
}

static void handle_tiering_stop(struct cluster_server *srv, int conn_fd,
                                 const uint8_t *payload, uint32_t plen)
{
    enum mds_status st = MDS_ERR_INVAL;

    (void)payload;
    (void)plen;

    /* srv->tiering is NULL until the daemon attaches a worker;
     * mirror handle_tiering_start's guard to avoid a NULL deref. */
    if (srv->tiering != NULL) {
        tiering_stop(srv->tiering);
        st = MDS_OK;
    }

    int32_t st_be = (int32_t)htobe32((uint32_t)(int32_t)st);
    send_header(conn_fd, CT_MSG_TIERING_RESP, 4);
    send_all(conn_fd, &st_be, 4);
}

static void handle_tiering_status(const struct cluster_server *srv,
                                   int conn_fd,
                                   const uint8_t *payload, uint32_t plen)
{
    (void)payload;
    (void)plen;

    /* Response layout (56 bytes):
     * state(4) + eval_count(4) + candidates_in_eval(4) +
     * files_promoted(4) + files_demoted(4) + bytes_moved(8) +
     * 7 skip counters * 4 = 56 bytes. */
    uint8_t resp[56];
    memset(resp, 0, sizeof(resp));

    if (srv->tiering != NULL) {
        struct tiering_status_info info;
        tiering_status(srv->tiering, &info);

        uint32_t be32;
        uint64_t be64;

        be32 = htobe32(info.state);
        memcpy(resp, &be32, 4);
        be32 = htobe32(info.eval_count);
        memcpy(resp + 4, &be32, 4);
        be32 = htobe32(info.candidates_in_eval);
        memcpy(resp + 8, &be32, 4);
        be32 = htobe32(info.files_promoted);
        memcpy(resp + 12, &be32, 4);
        be32 = htobe32(info.files_demoted);
        memcpy(resp + 16, &be32, 4);
        be64 = htobe64(info.bytes_moved);
        memcpy(resp + 20, &be64, 8);
        be32 = htobe32(info.skipped_active_layout);
        memcpy(resp + 28, &be32, 4);
        be32 = htobe32(info.skipped_active_writer);
        memcpy(resp + 32, &be32, 4);
        be32 = htobe32(info.skipped_sparse);
        memcpy(resp + 36, &be32, 4);
        be32 = htobe32(info.skipped_multi_stripe);
        memcpy(resp + 40, &be32, 4);
        be32 = htobe32(info.skipped_cooldown);
        memcpy(resp + 44, &be32, 4);
        be32 = htobe32(info.skipped_no_target_ds);
        memcpy(resp + 48, &be32, 4);
        be32 = htobe32(info.skipped_error);
        memcpy(resp + 52, &be32, 4);
    }

    send_header(conn_fd, CT_MSG_TIERING_RESP, sizeof(resp));
    send_all(conn_fd, resp, sizeof(resp));
}

/* -----------------------------------------------------------------------
 * Split admin handler (Seq 7.1)
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void handle_split_admin(struct cluster_server *srv, int conn_fd,
                               const uint8_t *payload, uint32_t plen)
{
    enum mds_status st = MDS_ERR_INVAL;

    /* Need smap and catalogue for split. */
    if (srv->smap == NULL || srv->cat == NULL) {
        goto respond;
    }

    /* Parse request:
     * [parent_path_len 2 BE][parent_path N]
     * [child_path_len  2 BE][child_path  M]
     * [child_fileid    8 BE]
     * [dest_mds_id     4 BE]
     * [host_len        2 BE][dest_host   H]
     * [dest_port       2 BE]
     */
    if (plen < 2) {
        goto respond;
}

    size_t off = 0;

    /* parent_path */
    uint16_t pp_len_be;
    memcpy(&pp_len_be, payload + off, 2); off += 2;
    uint16_t pp_len = be16toh(pp_len_be);
    if (pp_len == 0 || pp_len >= MDS_MAX_PATH || off + pp_len > plen) {
        goto respond;
}

    char parent_path[MDS_MAX_PATH];
    memcpy(parent_path, payload + off, pp_len); off += pp_len;
    parent_path[pp_len] = '\0';

    /* child_path */
    if (off + 2 > plen) {
        goto respond;
}
    uint16_t cp_len_be;
    memcpy(&cp_len_be, payload + off, 2); off += 2;
    uint16_t cp_len = be16toh(cp_len_be);
    if (cp_len == 0 || cp_len >= MDS_MAX_PATH || off + cp_len > plen) {
        goto respond;
}

    char child_path[MDS_MAX_PATH];
    memcpy(child_path, payload + off, cp_len); off += cp_len;
    child_path[cp_len] = '\0';

    /* child_fileid (8 BE) */
    if (off + 8 > plen) {
        goto respond;
}
    uint64_t wire_fileid_be;
    memcpy(&wire_fileid_be, payload + off, 8); off += 8;
    uint64_t wire_fileid = be64toh(wire_fileid_be);

    /* dest_mds_id (4 BE) */
    if (off + 4 > plen) {
        goto respond;
}
    uint32_t dest_id_be;
    memcpy(&dest_id_be, payload + off, 4); off += 4;
    uint32_t dest_mds_id = be32toh(dest_id_be);

    /* dest host (len-prefixed) */
    if (off + 2 > plen) {
        goto respond;
}
    uint16_t hl_be;
    memcpy(&hl_be, payload + off, 2); off += 2;
    uint16_t host_len = be16toh(hl_be);
    if (off + host_len + 2 > plen || host_len >= 256) {
        goto respond;
}

    char dest_host[256];
    if (host_len > 0) {
        memcpy(dest_host, payload + off, host_len);
        dest_host[host_len] = '\0';
    } else {
        dest_host[0] = '\0';
    }
    off += host_len;

    uint16_t dp_be;
    memcpy(&dp_be, payload + off, 2); off += 2;
    uint16_t dest_port = be16toh(dp_be);
    (void)off;

    /* Always resolve child_path to fileid. */
    uint64_t resolved_fid = 0;
    st = mds_cat_resolve_path(srv->cat, child_path, &resolved_fid);
    if (st != MDS_OK) {
        goto respond;
    }

    /* Cross-check: if caller supplied a fileid, it must match. */
    if (wire_fileid != 0 && wire_fileid != resolved_fid) {
        st = MDS_ERR_INVAL;
        goto respond;
    }

    /* Preflight: ensure destination can own subtrees before streaming. */
    if (srv->membership != NULL &&
        !cluster_membership_can_own_subtrees(srv->membership, dest_mds_id)) {
        st = MDS_ERR_PERM;
        goto respond;
    }

    /* Local split (dest == self): no transport needed. */
    uint32_t self_id = subtree_map_self_id(srv->smap);
    struct migration_transport *mt = NULL;

    if (dest_mds_id == self_id && srv->sharding_enabled) {
        /*
         * Phase 3 safe subset: same-MDS child-shard split is not yet wired
         * into owned shard lifecycle + metadata migration. Refuse instead of
         * registering a map-only split that would misroute follow-up traffic.
         */
        st = MDS_ERR_NOSUPPORT;
        goto respond;
    }

    if (dest_mds_id != self_id) {
        /* Resolve destination endpoint. */
        if (srv->membership != NULL) {
            st = cluster_membership_resolve_peer(
                srv->membership, dest_mds_id,
                dest_host, sizeof(dest_host), &dest_port);
            if (st != MDS_OK) {
                goto respond;
}
        } else if (host_len == 0) {
            goto respond;
        }

        if (!dest_host_is_allowed(srv, dest_host)) {
            st = MDS_ERR_PERM;
            goto respond;
        }

        st = cluster_transport_connect_migration(
            dest_host, dest_port, &mt);
        if (st != MDS_OK) {
            goto respond;
}
    }

    st = subtree_split_execute(srv->smap, srv->cat,
                               parent_path, child_path,
                               resolved_fid, dest_mds_id,
                               (const struct migration_transport *)mt, 0);

    if (mt != NULL) {
        cluster_transport_disconnect_migration(mt);
}

respond:;
    uint8_t resp_status = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);
    send_header(conn_fd, CT_MSG_SPLIT_ADMIN_RESP, 1);
    send_all(conn_fd, &resp_status, 1);
}

/* -----------------------------------------------------------------------
 * Assign admin handler (Seq 7.3 -- exact-root migrate)
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void handle_assign_admin(struct cluster_server *srv, int conn_fd,
                                const uint8_t *payload, uint32_t plen)
{
    enum mds_status st = MDS_ERR_INVAL;

    if (srv->smap == NULL || srv->cat == NULL) {
        goto respond;
    }

    /* Parse: same wire format as MIG_ADMIN_REQ.
     * [path_len 2 BE][path N][dest_id 4 BE]
     * [host_len 2 BE][host H][dest_port 2 BE]
     */
    if (plen < 2) {
        goto respond;
}

    size_t off = 0;
    uint16_t path_len_be;
    memcpy(&path_len_be, payload + off, 2); off += 2;
    uint16_t path_len = be16toh(path_len_be);
    if (path_len == 0 || path_len >= MDS_MAX_PATH || off + path_len + 4 + 2 + 2 > plen) {
        goto respond;
}

    char subtree_path[MDS_MAX_PATH];
    memcpy(subtree_path, payload + off, path_len); off += path_len;
    subtree_path[path_len] = '\0';

    uint32_t dest_id_be;
    memcpy(&dest_id_be, payload + off, 4); off += 4;
    uint32_t dest_mds_id = be32toh(dest_id_be);

    if (off + 2 > plen) {
        goto respond;
}
    uint16_t hl_be;
    memcpy(&hl_be, payload + off, 2); off += 2;
    uint16_t host_len = be16toh(hl_be);
    if (off + host_len + 2 > plen || host_len >= 256) {
        goto respond;
}

    char dest_host[256];
    if (host_len > 0) {
        memcpy(dest_host, payload + off, host_len);
        dest_host[host_len] = '\0';
    } else {
        dest_host[0] = '\0';
    }
    off += host_len;

    uint16_t dp_be;
    memcpy(&dp_be, payload + off, 2); off += 2;
    uint16_t dest_port = be16toh(dp_be);
    (void)off;

    /* Exact-root check: path must already be a subtree root. */
    struct subtree_entry exact_entry;
    st = subtree_map_lookup_exact(srv->smap, subtree_path, &exact_entry);
    if (st != MDS_OK) {
        st = MDS_ERR_NOTFOUND;
        goto respond;
    }

    /* Preflight: ensure destination can own subtrees. */
    if (srv->membership != NULL &&
        !cluster_membership_can_own_subtrees(srv->membership, dest_mds_id)) {
        st = MDS_ERR_PERM;
        goto respond;
    }

    /* Resolve destination. */
    if (srv->membership != NULL) {
        st = cluster_membership_resolve_peer(
            srv->membership, dest_mds_id,
            dest_host, sizeof(dest_host), &dest_port);
        if (st != MDS_OK) {
            goto respond;
}
    } else if (host_len == 0) {
        goto respond;
    }

    if (!dest_host_is_allowed(srv, dest_host)) {
        st = MDS_ERR_PERM;
        goto respond;
    }

    /* Resolve path to fileid. */
    uint64_t root_fid = 0;
    st = mds_cat_resolve_path(srv->cat, subtree_path, &root_fid);
    if (st != MDS_OK) {
        goto respond;
    }

    /* Connect and migrate. */
    struct migration_transport *mt = NULL;
    st = cluster_transport_connect_migration(
        dest_host, dest_port, &mt);
    if (st != MDS_OK) {
        goto respond;
    }

    st = migration_initiate(srv->cat, srv->smap, mt,
                            subtree_path, root_fid, dest_mds_id,
                            NULL);
    cluster_transport_disconnect_migration(mt);

respond:;
    uint8_t resp_status = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);
    send_header(conn_fd, CT_MSG_ASSIGN_ADMIN_RESP, 1);
    send_all(conn_fd, &resp_status, 1);
}

/**
 * Per-connection thread argument.
 */
struct conn_thread_arg {
    struct cluster_server *srv;
    int conn_fd;
};

/**
 * Handle one client connection (message loop).
 */

/* -----------------------------------------------------------------------
 * Drain self handler (Seq 8)
 *
 * CT_MSG_DRAIN_SELF payload: dest_mds_id(4 BE).
 * ----------------------------------------------------------------------- */

static void handle_drain_self(struct cluster_server *srv, int conn_fd,
                              const uint8_t *payload, uint32_t plen)
{
    uint8_t resp_status = 1;

    if (srv->membership == NULL || srv->cat == NULL ||
        srv->smap == NULL || plen != 4) {
        goto respond;
    }

    uint32_t dest_be;
    memcpy(&dest_be, payload, 4);
    uint32_t dest_mds_id = be32toh(dest_be);

    enum mds_status st = cluster_drain_self(
        srv->membership, srv->smap, srv->cat, dest_mds_id);
    resp_status = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);

respond:
    send_header(conn_fd, CT_MSG_MEMBERSHIP_RESP, 1);
    send_all(conn_fd, &resp_status, 1);
}

/* -----------------------------------------------------------------------
 * Standby detach handler (Seq 8)
 *
 * CT_MSG_STANDBY_DETACH payload: mds_id(4 BE).
 * ----------------------------------------------------------------------- */

static void handle_standby_detach(struct cluster_server *srv, int conn_fd,
                                  const uint8_t *payload, uint32_t plen)
{
    uint8_t resp_status = 1;

    if (srv->membership == NULL || plen != 4) {
        goto respond;
    }

    uint32_t id_be;
    memcpy(&id_be, payload, 4);
    uint32_t mds_id = be32toh(id_be);

    enum mds_status st = cluster_standby_detach(srv->membership, mds_id);
    resp_status = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);

respond:
    send_header(conn_fd, CT_MSG_MEMBERSHIP_RESP, 1);
    send_all(conn_fd, &resp_status, 1);
}

/* -----------------------------------------------------------------------
 * Force-remove handler (Seq 8)
 *
 * CT_MSG_FORCE_REMOVE payload: mds_id(4 BE).
 * ----------------------------------------------------------------------- */

static void handle_force_remove(struct cluster_server *srv, int conn_fd,
                                const uint8_t *payload, uint32_t plen)
{
    uint8_t resp_status = 1;

    if (srv->membership == NULL || plen != 4) {
        goto respond;
    }

    uint32_t id_be;
    memcpy(&id_be, payload, 4);
    uint32_t mds_id = be32toh(id_be);

    enum mds_status st = cluster_force_remove_node(srv->membership, mds_id);
    resp_status = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);

respond:
    send_header(conn_fd, CT_MSG_MEMBERSHIP_RESP, 1);
    send_all(conn_fd, &resp_status, 1);
}

/* Forward declarations for quota handlers. */
static void handle_quota_set(struct cluster_server *srv, int conn_fd,
                              const uint8_t *payload, uint32_t plen);
static void handle_quota_get(struct cluster_server *srv, int conn_fd,
                              const uint8_t *payload, uint32_t plen);
static void handle_quota_del(struct cluster_server *srv, int conn_fd,
                              const uint8_t *payload, uint32_t plen);

/* -----------------------------------------------------------------------
 * Rolling upgrade handlers (Item 46)
 * ----------------------------------------------------------------------- */

static void handle_failover_req(struct cluster_server *srv, int conn_fd,
                                const uint8_t *payload, uint32_t plen)
{
    uint8_t resp[4];
    uint32_t status_val = (uint32_t)MDS_ERR_PERM;

    /* Payload must contain target_mds_id (4 bytes BE). */
    if (plen < 4 || payload == NULL) {
        goto respond;
    }

    /* Validate target matches this node's self_id. */
    uint32_t id_be;
    memcpy(&id_be, payload, 4);
    uint32_t target_id = be32toh(id_be);

    if (srv->fo_ctx != NULL) {
        uint32_t self_id = failover_get_self_id(srv->fo_ctx);
        if (target_id != self_id) {
            status_val = (uint32_t)MDS_ERR_INVAL;
            goto respond;
        }
        enum mds_status st = failover_controlled_demote(
            srv->fo_ctx, srv->fo_repl);
        status_val = (uint32_t)st;
    }

respond:
    {
        uint32_t be_st = htobe32(status_val);
        memcpy(resp, &be_st, 4);
        (void)send_header(conn_fd, CT_MSG_FAILOVER_RESP, sizeof(resp));
        (void)send_all(conn_fd, resp, sizeof(resp));
    }
}

static void handle_upgrade_status(const struct cluster_server *srv, int conn_fd,
                                  const uint8_t *payload, uint32_t plen)
{
    (void)payload;
    (void)plen;

    uint8_t resp_buf[4 + MDS_MAX_NODES * 42];
    uint32_t node_count = 0;
    uint32_t off = 4;  /* reserve space for node_count header */

    if (srv->membership != NULL) {
        for (uint32_t mid = 1; mid <= MDS_MAX_NODES; mid++) {
            struct cluster_member m;
            if (cluster_membership_get(srv->membership, mid,
                                       &m) != MDS_OK) {
                continue;
            }
            if (off + 42 > sizeof(resp_buf)) {
                break;
            }
            /* mds_id (4) + wire_compat (4) + version_str (32) + role (1)
             * + lifecycle (1) = 42 bytes per node. */
            uint32_t be_val;
            be_val = htobe32(m.mds_id);
            memcpy(resp_buf + off, &be_val, 4);
            off += 4;
            be_val = htobe32(m.wire_compat_version);
            memcpy(resp_buf + off, &be_val, 4);
            off += 4;
            memset(resp_buf + off, 0, 32);
            (void)snprintf((char *)(resp_buf + off), 32,
                           "%s", PNFS_MDS_VERSION);
            off += 32;
            resp_buf[off++] = (uint8_t)m.role;
            resp_buf[off++] = (uint8_t)m.lifecycle;
            node_count++;
        }
    }

    uint32_t be_count = htobe32(node_count);
    memcpy(resp_buf, &be_count, 4);
    (void)send_header(conn_fd, CT_MSG_UPGRADE_STATUS_RESP, off);
    (void)send_all(conn_fd, resp_buf, off);
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void handle_ds_patch_ready_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);
static void handle_ds_patch_clear_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);

static void handle_ds_provision_admin(const struct cluster_server *srv,
                                      int conn_fd,
                                      const uint8_t *payload, uint32_t plen);

static void handle_ds_validate_admin(const struct cluster_server *srv,
                                     int conn_fd,
                                     const uint8_t *payload, uint32_t plen);
static void handle_ds_validate_clr_admin(const struct cluster_server *srv,
                                         int conn_fd,
                                         const uint8_t *payload, uint32_t plen);

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void handle_ds_add_admin(const struct cluster_server *srv,
                                int conn_fd,
                                const uint8_t *payload, uint32_t plen);
static void handle_ds_set_state_admin(const struct cluster_server *srv,
                                      int conn_fd,
                                      const uint8_t *payload, uint32_t plen);
static void handle_ds_remove_admin(const struct cluster_server *srv,
                                    int conn_fd,
                                    const uint8_t *payload, uint32_t plen);

static void handle_ds_list_admin(const struct cluster_server *srv,
                                 int conn_fd,
                                 const uint8_t *payload, uint32_t plen);

static void handle_split_proposals(const struct cluster_server *srv,
                                   int conn_fd);
static void handle_split_approve(const struct cluster_server *srv,
                                 int conn_fd,
                                 const uint8_t *payload, uint32_t plen);
static void handle_split_auto(const struct cluster_server *srv,
                              int conn_fd,
                              const uint8_t *payload, uint32_t plen);

/* C2: introspection handlers (impl near end of file). */
static void handle_config_show_admin(const struct cluster_server *srv,
                                     int conn_fd,
                                     const uint8_t *payload, uint32_t plen);
static void handle_ds_capacity_show_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);

/* C3: write-path handlers. */
static void handle_ds_set_weight_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);
static void handle_ds_capacity_probe_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);

/* Cross-subtree nlink handlers (impl near the nlink client code). */
static void handle_nlink_inc(const struct cluster_server *srv, int conn_fd,
                             const uint8_t *payload, uint32_t plen);
static void handle_nlink_dec(const struct cluster_server *srv, int conn_fd,
                             const uint8_t *payload, uint32_t plen);

static void handle_connection(struct cluster_server *srv, int conn_fd)
{
    while (atomic_load(&srv->running)) {
        uint8_t  msg_type;
        uint32_t payload_len;

        /* H2 fix: poll with timeout to prevent single-client DoS.
         * When TLS is active, check for buffered data first --
         * SSL may have decrypted a full record already. */
        if (tl_tls_conn == NULL ||
            mds_tls_pending(tl_tls_conn) == 0) {
            struct pollfd pfd_conn = {
                .fd = conn_fd, .events = POLLIN
            };
            int pr = poll(&pfd_conn, 1, 30000);  /* 30 s */
            if (pr <= 0) {
                break;  /* timeout or error */
            }
        }

        if (recv_header(conn_fd, &msg_type, &payload_len) != 0) {
            break;
        }

        /* Limit payload to 4 MiB (migration chunks may be large). */
        if (payload_len > 4 * 1024 * 1024) {
            break;
        }

        uint8_t *payload = NULL;
        if (payload_len > 0) {
            payload = malloc(payload_len);
            if (payload == NULL) {
                break;
            }
            if (recv_all(conn_fd, payload, payload_len) != 0) {
                free(payload);
                break;
            }
        }

        switch (msg_type) {
        case CT_MSG_PREPARE:
            handle_prepare(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_COMMIT:
            handle_commit(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_ABORT:
            handle_abort(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_MIG_BEGIN:
            handle_mig_begin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_MIG_INODE:
            handle_mig_inode(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_MIG_FINISH:
            handle_mig_finish(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_MIG_ADMIN_REQ:
            handle_mig_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_RESILVER_START:
            handle_resilver_start(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_RESILVER_STATUS:
            handle_resilver_status(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_RESILVER_STATUS_EXT:
            handle_resilver_status_ext(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_REBALANCE_START:
            handle_rebalance_start(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_REBALANCE_STATUS_REQ:
            handle_rebalance_status(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_TIERING_START:
            handle_tiering_start(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_TIERING_STOP:
            handle_tiering_stop(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_TIERING_STATUS_REQ:
            handle_tiering_status(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_QUOTA_SET:
            handle_quota_set(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_QUOTA_GET_REQ:
            handle_quota_get(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_QUOTA_DEL:
            handle_quota_del(srv, conn_fd, payload, payload_len);
            break;


        case CT_MSG_NODE_JOIN:
            handle_node_join(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_NODE_LEAVE:
            handle_node_leave(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_CLUSTER_STATUS_REQ:
            handle_cluster_status(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_SET_LIFECYCLE:
            handle_set_lifecycle(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_SPLIT_ADMIN_REQ:
            handle_split_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_SPLIT_PROPOSALS_REQ:
            handle_split_proposals(srv, conn_fd);
            break;
        case CT_MSG_SPLIT_APPROVE_REQ:
            handle_split_approve(srv, conn_fd, payload, payload_len);
            break;
        case CT_MSG_SPLIT_AUTO_REQ:
            handle_split_auto(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_ASSIGN_ADMIN_REQ:
            handle_assign_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_DRAIN_SELF:
            handle_drain_self(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_STANDBY_DETACH:
            handle_standby_detach(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_FORCE_REMOVE:
            handle_force_remove(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_SUBTREE_LIST_ADMIN_REQ:
            handle_subtree_list_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_MIG_PROGRESS_ADMIN_REQ:
            handle_mig_progress_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_FAILOVER_REQ:
            handle_failover_req(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_UPGRADE_STATUS_REQ:
            handle_upgrade_status(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_DS_LIST_REQ:
            handle_ds_list_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_DS_ADD_REQ:
            handle_ds_add_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_DS_SET_STATE_REQ:
            handle_ds_set_state_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_DS_REMOVE_REQ:
            handle_ds_remove_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_DS_PROVISION_REQ:
            handle_ds_provision_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_CONFIG_SHOW_REQ:
            handle_config_show_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_DS_CAPACITY_SHOW_REQ:
            handle_ds_capacity_show_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_DS_SET_WEIGHT_REQ:
            handle_ds_set_weight_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_DS_CAPACITY_PROBE_REQ:
            handle_ds_capacity_probe_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_METRICS_REQ:
            {
                struct mds_metrics_snapshot snap =
                    mds_metrics_snapshot();
                if (srv->fo_ctx != NULL) {
                    snap.repl_health_ok = 1;
                }
                /* Serialize field-by-field in network order. */
                uint8_t mbuf[8*6 + 2];
                size_t moff = 0;
                uint64_t f;
                f = htobe64(snap.repl_deltas_sent);
                memcpy(mbuf+moff,&f,8); moff+=8;
                f = htobe64(snap.repl_bytes_sent);
                memcpy(mbuf+moff,&f,8); moff+=8;
                f = htobe64(snap.cat_commits_ok);
                memcpy(mbuf+moff,&f,8); moff+=8;
                f = htobe64(snap.cat_commits_fail);
                memcpy(mbuf+moff,&f,8); moff+=8;
                f = htobe64(snap.cat_flush_ns_sum);
                memcpy(mbuf+moff,&f,8); moff+=8;
                f = htobe64(snap.cat_flush_count);
                memcpy(mbuf+moff,&f,8); moff+=8;
                mbuf[moff++] = snap.repl_health_ok;
                mbuf[moff++] = snap.repl_writes_blocked;
                send_header(conn_fd, CT_MSG_METRICS_RESP,
                            (uint32_t)moff);
                send_all(conn_fd, mbuf, moff);
            }
            break;

        case CT_MSG_DS_PATCH_READY_REQ:
            handle_ds_patch_ready_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_DS_PATCH_CLEAR_REQ:
            handle_ds_patch_clear_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_DS_VALIDATE_REQ:
            handle_ds_validate_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_DS_VALIDATE_CLR_REQ:
            handle_ds_validate_clr_admin(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_NLINK_INC_REQ:
            handle_nlink_inc(srv, conn_fd, payload, payload_len);
            break;

        case CT_MSG_NLINK_DEC_REQ:
            handle_nlink_dec(srv, conn_fd, payload, payload_len);
            break;

        default:
            /* Unknown message -- disconnect. */
            free(payload);
            goto done;
        }

        free(payload);
    }

done:
    ;  /* FD lifetime managed by conn_thread_entry. */
}

/**
 * Thread entry point: run handle_connection, unregister FD, close,
 * then decrement conn_count.  Unregister before close so that
 * server_stop cannot shutdown() a potentially-reused descriptor.
 */
static void *conn_thread_entry(void *arg)
{
    struct conn_thread_arg *cta = arg;
    struct cluster_server *srv = cta->srv;
    int fd = cta->conn_fd;

    /* TLS handshake for inbound connections. */
    struct mds_tls_conn *tc = NULL;
    if (srv->tls != NULL) {
        if (mds_tls_wrap(srv->tls, fd, true,
                         NULL, &tc) != 0) {
            /* Handshake failed -- drop connection. */
            unregister_conn_fd(srv, fd);
            close(fd);
            atomic_fetch_sub(&srv->conn_count, 1);
            free(cta);
            return NULL;
        }
    }

    tl_tls_conn = tc;
    handle_connection(srv, fd);
    tl_tls_conn = NULL;

    if (tc != NULL) {
        mds_tls_close(tc);
    }
    unregister_conn_fd(srv, fd);
    close(fd);
    atomic_fetch_sub(&srv->conn_count, 1);
    free(cta);
    return NULL;
}

static void *server_thread(void *arg)
{
    struct cluster_server *srv = arg;

    while (atomic_load(&srv->running)) {
        /* Poll with a short timeout so we can re-check running. */
        struct pollfd pfd = {
            .fd = srv->listen_fd,
            .events = POLLIN,
        };
        int pr = poll(&pfd, 1, 200);  /* 200 ms */
        if (pr <= 0) {
            continue;  /* Timeout or error -- re-check running flag. */
        }

        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);

        int conn = accept(srv->listen_fd,
                          (struct sockaddr *)&client_addr, &addr_len);
        if (conn < 0) {
            if (!atomic_load(&srv->running)) {
                break;
            }
            continue;
        }

        /* Peer allowlist check (Finding 1 remediation). */
        if (!peer_is_allowed(srv, &client_addr)) {
            close(conn);
            continue;
        }

        /* Connection limit check (Finding 2 remediation). */
        if (atomic_load(&srv->conn_count) >= srv->max_conns) {
            close(conn);
            continue;
        }

        int flag = 1;
        setsockopt(conn, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        /* Spawn a detached thread per connection (Finding 2).
         * The listener returns to accept() immediately. */
        {
            struct conn_thread_arg *cta = malloc(sizeof(*cta));
            if (cta == NULL) {
                close(conn);
                continue;
            }
            cta->srv = srv;
            cta->conn_fd = conn;

            register_conn_fd(srv, conn);
            atomic_fetch_add(&srv->conn_count, 1);

            pthread_t tid;
            pthread_attr_t attr;

            pthread_attr_init(&attr);
            pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
            if (pthread_create(&tid, &attr, conn_thread_entry, cta) != 0) {
                unregister_conn_fd(srv, conn);
                atomic_fetch_sub(&srv->conn_count, 1);
                close(conn);
                free(cta);
            }
            pthread_attr_destroy(&attr);
        }
    }

    return NULL;
}

enum mds_status cluster_transport_server_start(uint16_t port,
                                               const char *bind_addr,
                                               const char (*allowed_peers)[64],
                                               uint32_t allowed_peer_count,
                                               uint32_t max_conns,
                                               struct mds_catalogue *cat,
                                               struct subtree_map *smap,
                                               struct mds_tls_ctx *tls,
                                               struct cluster_server **out)
{
    if (cat == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }

    struct cluster_server *srv = calloc(1, sizeof(*srv));
    if (srv == NULL) {
        return MDS_ERR_NOMEM;
    }

    srv->cat = cat;
    /* All handlers use srv->cat directly. */
    srv->smap = smap;
    srv->tls = tls;
    atomic_store(&srv->running, true);
    atomic_init(&srv->conn_count, 0);
    srv->max_conns = (max_conns > 0) ? max_conns : 16;

    srv->conn_fds = malloc(srv->max_conns * sizeof(int));
    if (srv->conn_fds == NULL) {
        free(srv);
        return MDS_ERR_NOMEM;
    }
    srv->conn_fd_count = 0;
    pthread_mutex_init(&srv->conn_fds_lock, NULL);

    /* Populate peer allowlist. */
    srv->allowed_peer_count = 0;
    if (allowed_peers != NULL && allowed_peer_count > 0) {
        uint32_t n = allowed_peer_count;

        if (n > CT_MAX_PEERS) {
            n = CT_MAX_PEERS;
}
        for (uint32_t i = 0; i < n; i++) {
            if (inet_pton(AF_INET, allowed_peers[i],
                          &srv->allowed_peers[i]) == 1) {
                srv->allowed_peer_count++;
}
        }
    }

    /* Populate admin allowed hosts from config (if attached later
     * via cluster_transport_server_set_config). */
    srv->admin_allowed_host_count = 0;

    srv->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (srv->listen_fd < 0) {
        pthread_mutex_destroy(&srv->conn_fds_lock);
        free(srv->conn_fds);
        free(srv);
        return MDS_ERR_IO;
    }

    int opt = 1;
    setsockopt(srv->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    /* Bind restriction (Finding 1): default to loopback. */
    if (bind_addr != NULL && bind_addr[0] != '\0') {
        if (inet_pton(AF_INET, bind_addr, &addr.sin_addr) != 1) {
            addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
}
    } else {
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    }
    addr.sin_port = htons(port);

    if (bind(srv->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(srv->listen_fd);
        pthread_mutex_destroy(&srv->conn_fds_lock);
        free(srv->conn_fds);
        free(srv);
        return MDS_ERR_IO;
    }

    /* Retrieve assigned port (for port=0 case). */
    socklen_t slen = sizeof(addr);
    if (getsockname(srv->listen_fd, (struct sockaddr *)&addr, &slen) == 0) {
        srv->port = ntohs(addr.sin_port);
    }

    if (listen(srv->listen_fd, 16) != 0) {
        close(srv->listen_fd);
        pthread_mutex_destroy(&srv->conn_fds_lock);
        free(srv->conn_fds);
        free(srv);
        return MDS_ERR_IO;
    }

    if (pthread_create(&srv->thread, NULL, server_thread, srv) != 0) {
        close(srv->listen_fd);
        pthread_mutex_destroy(&srv->conn_fds_lock);
        free(srv->conn_fds);
        free(srv);
        return MDS_ERR_IO;
    }

    *out = srv;
    return MDS_OK;
}

void cluster_transport_server_stop(struct cluster_server *srv)
{
    if (srv == NULL) {
        return;
    }

    atomic_store(&srv->running, false);
    /*
     * Shut down the listen socket so poll() returns immediately,
     * then close and join.  shutdown() is more reliable than bare
     * close() for unblocking poll/accept under valgrind.
     */
    if (srv->listen_fd >= 0) {
        shutdown(srv->listen_fd, SHUT_RDWR);
        close(srv->listen_fd);
        srv->listen_fd = -1;
    }
    pthread_join(srv->thread, NULL);

    /*
     * Force-unwind any connection threads blocked in recv() by
     * shutting down their sockets.  After shutdown(), recv() returns
     * 0 or -1, causing the message loop to exit promptly.
     */
    pthread_mutex_lock(&srv->conn_fds_lock);
    for (uint32_t i = 0; i < srv->conn_fd_count; i++) {
        shutdown(srv->conn_fds[i], SHUT_RDWR);
    }
    pthread_mutex_unlock(&srv->conn_fds_lock);

    /*
     * Wait unconditionally for all connection threads to finish.
     * With their sockets shut down, threads will unwind within
     * one message-loop iteration.
     */
    while (atomic_load(&srv->conn_count) > 0) {
        /* NOLINTNEXTLINE(bugprone-implicit-widening-of-multiplication-result) */
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 100 * 1000 * 1000 };
        nanosleep(&ts, NULL);  /* 100 ms */
    }

    /* Brief settle time: detached connection threads may still be
     * unwinding their stacks after decrementing conn_count.  Without
     * this, Valgrind reports the thread-internal pthread state as
     * "possibly lost" because glibc hasn't reclaimed it yet. */
    {
        struct timespec settle = { .tv_sec = 0, .tv_nsec = 50L * 1000 * 1000 };
        nanosleep(&settle, NULL);  /* 50 ms */
    }

    pthread_mutex_destroy(&srv->conn_fds_lock);
    free(srv->conn_fds);
    free(srv);
}

uint16_t cluster_transport_server_port(const struct cluster_server *srv)
{
    if (srv == NULL) {
        return 0;
    }
    return srv->port;
}

/* -----------------------------------------------------------------------
 * Client
 * ----------------------------------------------------------------------- */

struct tcp_client_ctx {
    int fd;
};

static int tcp_prepare(uint32_t remote_mds_id, uint64_t txn_id,
                       uint64_t dst_parent, const char *dst_name,
                       const void *inode_data, size_t data_len,
                       void *user_ctx)
{
    (void)remote_mds_id;
    const struct tcp_client_ctx *ctx = user_ctx;

    uint16_t name_len = (uint16_t)strlen(dst_name);
    uint32_t payload_len = 8 + 8 + 2 + name_len + 4 + (uint32_t)data_len;

    if (send_header(ctx->fd, CT_MSG_PREPARE, payload_len) != 0) {
        return -1;
    }

    uint8_t buf[8 + 8 + 2];
    uint64_t txn_be = htobe64(txn_id);
    uint64_t dst_be = htobe64(dst_parent);
    uint16_t nlen_be = htobe16(name_len);

    memcpy(buf, &txn_be, 8);
    memcpy(buf + 8, &dst_be, 8);
    memcpy(buf + 16, &nlen_be, 2);
    if (send_all(ctx->fd, buf, 18) != 0) {
        return -1;
    }
    if (send_all(ctx->fd, dst_name, name_len) != 0) {
        return -1;
    }

    uint32_t dlen_be = htobe32((uint32_t)data_len);
    if (send_all(ctx->fd, &dlen_be, 4) != 0) {
        return -1;
    }
    if (data_len > 0 && send_all(ctx->fd, inode_data, data_len) != 0) {
        return -1;
    }

    /* Read VOTE response. */
    uint8_t  resp_type;
    uint32_t resp_len;
    if (recv_header(ctx->fd, &resp_type, &resp_len) != 0) {
        return -1;
    }
    if (resp_type != CT_MSG_VOTE || resp_len != 9) {
        return -1;
    }

    uint8_t vote_buf[9];
    if (recv_all(ctx->fd, vote_buf, 9) != 0) {
        return -1;
    }

    return vote_buf[8] ? 1 : 0;
}

static int tcp_commit(uint32_t remote_mds_id, uint64_t txn_id,
                      void *user_ctx)
{
    (void)remote_mds_id;
    const struct tcp_client_ctx *ctx = user_ctx;

    if (send_header(ctx->fd, CT_MSG_COMMIT, 8) != 0) {
        return -1;
    }

    uint64_t txn_be = htobe64(txn_id);
    if (send_all(ctx->fd, &txn_be, 8) != 0) {
        return -1;
    }

    /* Read ACK. */
    uint8_t  resp_type;
    uint32_t resp_len;
    if (recv_header(ctx->fd, &resp_type, &resp_len) != 0) {
        return -1;
    }
    if (resp_type != CT_MSG_ACK || resp_len != 9) {
        return -1;
    }

    uint8_t ack[9];
    if (recv_all(ctx->fd, ack, 9) != 0) {
        return -1;
    }

    return ack[8] == 0 ? 0 : -1;
}

static int tcp_abort(uint32_t remote_mds_id, uint64_t txn_id,
                     void *user_ctx)
{
    (void)remote_mds_id;
    const struct tcp_client_ctx *ctx = user_ctx;

    if (send_header(ctx->fd, CT_MSG_ABORT, 8) != 0) {
        return -1;
    }

    uint64_t txn_be = htobe64(txn_id);
    if (send_all(ctx->fd, &txn_be, 8) != 0) {
        return -1;
    }

    /* Read ACK. */
    uint8_t  resp_type;
    uint32_t resp_len;
    if (recv_header(ctx->fd, &resp_type, &resp_len) != 0) {
        return -1;
    }
    if (resp_type != CT_MSG_ACK || resp_len != 9) {
        return -1;
    }

    uint8_t ack[9];
    if (recv_all(ctx->fd, ack, 9) != 0) {
        return -1;
    }

    return ack[8] == 0 ? 0 : -1;
}

enum mds_status cluster_transport_connect(const char *host, uint16_t port,
                                          struct rename_2pc_transport **out)
{
    if (host == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        /* Try hostname resolution. */
        struct addrinfo hints_c = {0}, *res_c = NULL;
        hints_c.ai_family = AF_INET;
        if (getaddrinfo(host, NULL, &hints_c, &res_c) != 0 || res_c == NULL) {
            close(fd);
            return MDS_ERR_IO;
        }
        memcpy(&addr.sin_addr,
               &((struct sockaddr_in *)res_c->ai_addr)->sin_addr,
               sizeof(addr.sin_addr));
        freeaddrinfo(res_c);
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    struct tcp_client_ctx *ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        close(fd);
        return MDS_ERR_NOMEM;
    }
    ctx->fd = fd;

    struct rename_2pc_transport *t = calloc(1, sizeof(*t));
    if (t == NULL) {
        close(fd);
        free(ctx);
        return MDS_ERR_NOMEM;
    }

    t->prepare = tcp_prepare;
    t->commit = tcp_commit;
    t->abort_rename = tcp_abort;
    t->user_ctx = ctx;

    *out = t;
    return MDS_OK;
}

void cluster_transport_disconnect(struct rename_2pc_transport *t)
{
    if (t == NULL) {
        return;
    }

    struct tcp_client_ctx *ctx = t->user_ctx;
    if (ctx != NULL) {
        if (ctx->fd >= 0) {
            close(ctx->fd);
        }
        free(ctx);
    }
    free(t);
}

/* -----------------------------------------------------------------------
 * Migration TCP client
 *
 * Implements migration_transport callbacks over TCP.
 * ----------------------------------------------------------------------- */

static int ct_mig_send_begin(uint32_t dest_mds_id,
                             const char *subtree_path,
                             uint32_t estimated_inodes,
                             void *user_ctx)
{
    (void)dest_mds_id;
    (void)estimated_inodes;
    const struct tcp_client_ctx *ctx = user_ctx;

    uint16_t path_len = (uint16_t)strlen(subtree_path);
    uint32_t payload_len = 2 + path_len;

    if (send_header(ctx->fd, CT_MSG_MIG_BEGIN, payload_len) != 0) {
        return -1;
}

    uint16_t plen_be = htobe16(path_len);
    if (send_all(ctx->fd, &plen_be, 2) != 0) {
        return -1;
}
    if (path_len > 0 && send_all(ctx->fd, subtree_path, path_len) != 0) {
        return -1;
}

    /* Wait for ACK. */
    uint8_t  resp_type;
    uint32_t resp_len;
    if (recv_header(ctx->fd, &resp_type, &resp_len) != 0) {
        return -1;
}
    if (resp_type == CT_MSG_MIG_NACK) {
        return -1;
}
    if (resp_type != CT_MSG_MIG_ACK || resp_len != 1) {
        return -1;
}

    uint8_t status;
    if (recv_all(ctx->fd, &status, 1) != 0) {
        return -1;
}

    return (status == 0) ? 0 : -1;
}

static int ct_mig_send_inode(uint32_t dest_mds_id,
                             const void *data, size_t len,
                             void *user_ctx)
{
    (void)dest_mds_id;
    const struct tcp_client_ctx *ctx = user_ctx;

    if (send_header(ctx->fd, CT_MSG_MIG_INODE, (uint32_t)len) != 0) {
        return -1;
}
    if (len > 0 && send_all(ctx->fd, data, len) != 0) {
        return -1;
}

    /* Wait for ACK/NACK. */
    uint8_t  resp_type;
    uint32_t resp_len;
    if (recv_header(ctx->fd, &resp_type, &resp_len) != 0) {
        return -1;
}
    if (resp_type == CT_MSG_MIG_NACK) {
        return -1;
}
    if (resp_type != CT_MSG_MIG_ACK || resp_len != 1) {
        return -1;
}

    uint8_t status;
    if (recv_all(ctx->fd, &status, 1) != 0) {
        return -1;
}

    return (status == 0) ? 0 : -1;
}

static int ct_mig_send_finish(uint32_t dest_mds_id,
                              uint32_t total_inodes,
                              void *user_ctx)
{
    (void)dest_mds_id;
    const struct tcp_client_ctx *ctx = user_ctx;

    uint32_t ti_be = htobe32(total_inodes);
    if (send_header(ctx->fd, CT_MSG_MIG_FINISH, 4) != 0) {
        return -1;
}
    if (send_all(ctx->fd, &ti_be, 4) != 0) {
        return -1;
}

    /* Wait for ACK. */
    uint8_t  resp_type;
    uint32_t resp_len;
    if (recv_header(ctx->fd, &resp_type, &resp_len) != 0) {
        return -1;
}
    if (resp_type == CT_MSG_MIG_NACK) {
        return -1;
}
    if (resp_type != CT_MSG_MIG_ACK || resp_len != 1) {
        return -1;
}

    uint8_t status;
    if (recv_all(ctx->fd, &status, 1) != 0) {
        return -1;
}

    return (status == 0) ? 0 : -1;
}

enum mds_status cluster_transport_connect_migration(
    const char *host, uint16_t port,
    struct migration_transport **out)
{
    if (host == NULL || out == NULL) {
        return MDS_ERR_INVAL;
}

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return MDS_ERR_IO;
}

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        struct addrinfo hints_m = {0}, *res_m = NULL;
        hints_m.ai_family = AF_INET;
        if (getaddrinfo(host, NULL, &hints_m, &res_m) != 0 || res_m == NULL) {
            close(fd);
            return MDS_ERR_IO;
        }
        memcpy(&addr.sin_addr,
               &((struct sockaddr_in *)res_m->ai_addr)->sin_addr,
               sizeof(addr.sin_addr));
        freeaddrinfo(res_m);
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    struct tcp_client_ctx *ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        close(fd);
        return MDS_ERR_NOMEM;
    }
    ctx->fd = fd;

    struct migration_transport *t = calloc(1, sizeof(*t));
    if (t == NULL) {
        close(fd);
        free(ctx);
        return MDS_ERR_NOMEM;
    }

    t->send_begin  = ct_mig_send_begin;
    t->send_inode  = ct_mig_send_inode;
    t->send_finish = ct_mig_send_finish;
    t->user_ctx    = ctx;

    *out = t;
    return MDS_OK;
}

void cluster_transport_disconnect_migration(struct migration_transport *t)
{
    if (t == NULL) {
        return;
}

    struct tcp_client_ctx *ctx = t->user_ctx;
    if (ctx != NULL) {
        if (ctx->fd >= 0) {
            close(ctx->fd);
}
        free(ctx);
    }
    free(t);
}

/* -----------------------------------------------------------------------
 * Admin migration request (mds-admin CLI -> running daemon)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_migration(
    const char *mds_host, uint16_t mds_port,
    const char *subtree_path, uint32_t dest_mds_id,
    const char *dest_host, uint16_t dest_port)
{
    if (mds_host == NULL || subtree_path == NULL) {
        return MDS_ERR_INVAL;
}

    size_t path_len = strlen(subtree_path);
    size_t host_len = (dest_host != NULL) ? strlen(dest_host) : 0;
    if (path_len == 0 || path_len > UINT16_MAX || host_len > 255) {
        return MDS_ERR_INVAL;
}

    /* Connect to daemon cluster transport port. */
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return MDS_ERR_IO;
}

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(mds_port);

    if (inet_pton(AF_INET, mds_host, &addr.sin_addr) != 1) {
        struct addrinfo hints_r = {0}, *res_r = NULL;
        hints_r.ai_family = AF_INET;
        if (getaddrinfo(mds_host, NULL, &hints_r, &res_r) != 0 || res_r == NULL) {
            close(fd);
            return MDS_ERR_IO;
        }
        memcpy(&addr.sin_addr,
               &((struct sockaddr_in *)res_r->ai_addr)->sin_addr,
               sizeof(addr.sin_addr));
        freeaddrinfo(res_r);
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    /* Build payload: [path_len 2 BE][path][dest_id 4 BE]
     *                [host_len 2 BE][host][dest_port 2 BE] */
    uint32_t payload_len = 2 + (uint32_t)path_len + 4 + 2 +
                           (uint32_t)host_len + 2;
    uint8_t *buf = malloc(payload_len);
    if (buf == NULL) {
        close(fd);
        return MDS_ERR_NOMEM;
    }

    size_t off = 0;
    uint16_t pl_be = htobe16((uint16_t)path_len);
    memcpy(buf + off, &pl_be, 2);           off += 2;
    memcpy(buf + off, subtree_path, path_len); off += path_len;

    uint32_t did_be = htobe32(dest_mds_id);
    memcpy(buf + off, &did_be, 4);          off += 4;

    uint16_t hl_be = htobe16((uint16_t)host_len);
    memcpy(buf + off, &hl_be, 2);           off += 2;
    if (host_len > 0) {
        memcpy(buf + off, dest_host, host_len);
}
    off += host_len;

    uint16_t dp_be = htobe16(dest_port);
    memcpy(buf + off, &dp_be, 2);           off += 2;
    (void)off;

    /* Send request. */
    enum mds_status ret = MDS_ERR_IO;
    if (send_header(fd, CT_MSG_MIG_ADMIN_REQ, payload_len) != 0) {
        goto out;
}
    if (send_all(fd, buf, payload_len) != 0) {
        goto out;
}

    /* Read response. */
    uint8_t  resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0) {
        goto out;
}
    if (resp_type != CT_MSG_MIG_ADMIN_RESP || resp_len != 1) {
        goto out;
}

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        goto out;
}

    /* Decode: 0=OK, else negated mds_status (positive wire byte). */
    ret = decode_wire_status(status);

out:
    free(buf);
    close(fd);
    return ret;
}


/* -----------------------------------------------------------------------
 * Split admin client (Seq 7.2)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_split(
    const char *mds_host, uint16_t mds_port,
    const char *parent_path, const char *child_path,
    uint32_t dest_mds_id,
    const char *dest_host, uint16_t dest_port)
{
    if (mds_host == NULL || parent_path == NULL || child_path == NULL) {
        return MDS_ERR_INVAL;
}

    size_t pp_len = strlen(parent_path);
    size_t cp_len = strlen(child_path);
    size_t host_len = (dest_host != NULL) ? strlen(dest_host) : 0;
    if (pp_len == 0 || pp_len > UINT16_MAX ||
        cp_len == 0 || cp_len > UINT16_MAX || host_len > 255) {
        return MDS_ERR_INVAL;
}

    /* Connect to daemon. */
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return MDS_ERR_IO;
}

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(mds_port);

    if (inet_pton(AF_INET, mds_host, &addr.sin_addr) != 1) {
        struct addrinfo hints_r = {0}, *res_r = NULL;
        hints_r.ai_family = AF_INET;
        if (getaddrinfo(mds_host, NULL, &hints_r, &res_r) != 0 || res_r == NULL) {
            close(fd);
            return MDS_ERR_IO;
        }
        memcpy(&addr.sin_addr,
               &((struct sockaddr_in *)res_r->ai_addr)->sin_addr,
               sizeof(addr.sin_addr));
        freeaddrinfo(res_r);
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    /* Build payload:
     * [pp_len 2 BE][parent_path]
     * [cp_len 2 BE][child_path]
     * [child_fileid 8 BE]  (always 0 -- daemon resolves)
     * [dest_id 4 BE]
     * [host_len 2 BE][host][dest_port 2 BE]
     */
    uint32_t payload_len = 2 + (uint32_t)pp_len + 2 + (uint32_t)cp_len +
                           8 + 4 + 2 + (uint32_t)host_len + 2;
    uint8_t *buf = malloc(payload_len);
    if (buf == NULL) {
        close(fd);
        return MDS_ERR_NOMEM;
    }

    size_t off = 0;
    uint16_t be16;
    uint32_t be32;
    uint64_t be64;

    be16 = htobe16((uint16_t)pp_len);
    memcpy(buf + off, &be16, 2); off += 2;
    memcpy(buf + off, parent_path, pp_len); off += pp_len;

    be16 = htobe16((uint16_t)cp_len);
    memcpy(buf + off, &be16, 2); off += 2;
    memcpy(buf + off, child_path, cp_len); off += cp_len;

    be64 = htobe64(0);  /* child_fileid = 0 (daemon resolves) */
    memcpy(buf + off, &be64, 8); off += 8;

    be32 = htobe32(dest_mds_id);
    memcpy(buf + off, &be32, 4); off += 4;

    be16 = htobe16((uint16_t)host_len);
    memcpy(buf + off, &be16, 2); off += 2;
    if (host_len > 0) {
        memcpy(buf + off, dest_host, host_len);
}
    off += host_len;

    be16 = htobe16(dest_port);
    memcpy(buf + off, &be16, 2); off += 2;
    (void)off;

    enum mds_status ret = MDS_ERR_IO;
    if (send_header(fd, CT_MSG_SPLIT_ADMIN_REQ, payload_len) != 0) {
        goto out;
}
    if (send_all(fd, buf, payload_len) != 0) {
        goto out;
}

    uint8_t  resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0) {
        goto out;
}
    if (resp_type != CT_MSG_SPLIT_ADMIN_RESP || resp_len != 1) {
        goto out;
}

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        goto out;
}

    ret = decode_wire_status(status);

out:
    free(buf);
    close(fd);
    return ret;
}

/* -----------------------------------------------------------------------
 * Assign admin client (Seq 7.3)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_assign(
    const char *mds_host, uint16_t mds_port,
    const char *subtree_path, uint32_t dest_mds_id,
    const char *dest_host, uint16_t dest_port)
{
    if (mds_host == NULL || subtree_path == NULL) {
        return MDS_ERR_INVAL;
}

    size_t path_len = strlen(subtree_path);
    size_t host_len = (dest_host != NULL) ? strlen(dest_host) : 0;
    if (path_len == 0 || path_len > UINT16_MAX || host_len > 255) {
        return MDS_ERR_INVAL;
}

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return MDS_ERR_IO;
}

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(mds_port);

    if (inet_pton(AF_INET, mds_host, &addr.sin_addr) != 1) {
        struct addrinfo hints_r = {0}, *res_r = NULL;
        hints_r.ai_family = AF_INET;
        if (getaddrinfo(mds_host, NULL, &hints_r, &res_r) != 0 || res_r == NULL) {
            close(fd);
            return MDS_ERR_IO;
        }
        memcpy(&addr.sin_addr,
               &((struct sockaddr_in *)res_r->ai_addr)->sin_addr,
               sizeof(addr.sin_addr));
        freeaddrinfo(res_r);
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    /* Build payload: same format as MIG_ADMIN_REQ.
     * [path_len 2 BE][path][dest_id 4 BE]
     * [host_len 2 BE][host][dest_port 2 BE]
     */
    uint32_t payload_len = 2 + (uint32_t)path_len + 4 + 2 +
                           (uint32_t)host_len + 2;
    uint8_t *buf = malloc(payload_len);
    if (buf == NULL) {
        close(fd);
        return MDS_ERR_NOMEM;
    }

    size_t off = 0;
    uint16_t be16;
    uint32_t be32;

    be16 = htobe16((uint16_t)path_len);
    memcpy(buf + off, &be16, 2); off += 2;
    memcpy(buf + off, subtree_path, path_len); off += path_len;

    be32 = htobe32(dest_mds_id);
    memcpy(buf + off, &be32, 4); off += 4;

    be16 = htobe16((uint16_t)host_len);
    memcpy(buf + off, &be16, 2); off += 2;
    if (host_len > 0) {
        memcpy(buf + off, dest_host, host_len);
}
    off += host_len;

    be16 = htobe16(dest_port);
    memcpy(buf + off, &be16, 2); off += 2;
    (void)off;

    enum mds_status ret = MDS_ERR_IO;
    if (send_header(fd, CT_MSG_ASSIGN_ADMIN_REQ, payload_len) != 0) {
        goto out;
}
    if (send_all(fd, buf, payload_len) != 0) {
        goto out;
}

    uint8_t  resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0) {
        goto out;
}
    if (resp_type != CT_MSG_ASSIGN_ADMIN_RESP || resp_len != 1) {
        goto out;
}

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        goto out;
}

    ret = decode_wire_status(status);

out:
    free(buf);
    close(fd);
    return ret;
}

/* -----------------------------------------------------------------------
 * Internal: open a TCP connection to a cluster transport peer.
 * Returns fd >= 0 on success, -1 on failure.
 * ----------------------------------------------------------------------- */
static int ct_client_connect(const char *host, uint16_t port)
{
    if (host == NULL) {
        return -1;
}

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
}

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);

    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        struct addrinfo hints_r = {0}, *res_r = NULL;
        hints_r.ai_family = AF_INET;
        if (getaddrinfo(host, NULL, &hints_r, &res_r) != 0 ||
            res_r == NULL) {
            close(fd);
            return -1;
        }
        memcpy(&addr.sin_addr,
               &((struct sockaddr_in *)res_r->ai_addr)->sin_addr,
               sizeof(addr.sin_addr));
        freeaddrinfo(res_r);
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }

    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    return fd;
}

/* -----------------------------------------------------------------------
 * Resilver client requests (mds-admin CLI -> daemon)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_resilver_start(
    const char *mds_host, uint16_t mds_port, uint32_t ds_id)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
}

    uint32_t ds_id_be = htobe32(ds_id);
    if (send_header(fd, CT_MSG_RESILVER_START, 4) != 0 ||
        send_all(fd, &ds_id_be, 4) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_RESILVER_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);
    return (status == 0) ? MDS_OK : MDS_ERR_IO;
}

enum mds_status cluster_transport_request_resilver_status(
    const char *mds_host, uint16_t mds_port,
    uint32_t *state, uint32_t *files_total,
    uint32_t *files_done, uint64_t *bytes_copied)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
}

    if (send_header(fd, CT_MSG_RESILVER_STATUS, 0) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_RESILVER_RESP || resp_len < 17) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp[17];
    if (recv_all(fd, resp, sizeof(resp)) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);

    if (state != NULL) {
        *state = resp[0];
}
    if (files_total != NULL) {
        uint32_t be32;
        memcpy(&be32, resp + 1, 4);
        *files_total = be32toh(be32);
    }
    if (files_done != NULL) {
        uint32_t be32;
        memcpy(&be32, resp + 5, 4);
        *files_done = be32toh(be32);
    }
    if (bytes_copied != NULL) {
        uint64_t be64;
        memcpy(&be64, resp + 9, 8);
        *bytes_copied = be64toh(be64);
    }

    return MDS_OK;
}

enum mds_status cluster_transport_request_resilver_status_ext(
    const char *mds_host, uint16_t mds_port,
    struct resilver_status_info *info)
{
    if (info == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(info, 0, sizeof(*info));

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    if (send_header(fd, CT_MSG_RESILVER_STATUS_EXT, 0) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_RESILVER_RESP || resp_len < 41) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp[41];
    if (recv_all(fd, resp, sizeof(resp)) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);

    info->state = resp[0];

    uint32_t be32;
    uint64_t be64;

    memcpy(&be32, resp + 1, 4);
    info->files_total = be32toh(be32);
    memcpy(&be32, resp + 5, 4);
    info->files_done = be32toh(be32);
    memcpy(&be64, resp + 9, 8);
    info->bytes_copied = be64toh(be64);
    memcpy(&be32, resp + 17, 4);
    info->pass_count = be32toh(be32);
    memcpy(&be32, resp + 21, 4);
    info->skipped_active_layout = be32toh(be32);
    memcpy(&be32, resp + 25, 4);
    info->skipped_active_writer = be32toh(be32);
    memcpy(&be32, resp + 29, 4);
    info->skipped_sparse = be32toh(be32);
    memcpy(&be32, resp + 33, 4);
    info->skipped_multi_stripe = be32toh(be32);
    memcpy(&be32, resp + 37, 4);
    info->skipped_error = be32toh(be32);

    return MDS_OK;
}


/* -----------------------------------------------------------------------
 * Rebalance client requests (mds-admin CLI -> daemon)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_rebalance_start(
    const char *mds_host, uint16_t mds_port,
    uint32_t source_ds, uint32_t target_ds)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    uint8_t payload[8];
    uint32_t be32;
    be32 = htobe32(source_ds);
    memcpy(payload, &be32, 4);
    be32 = htobe32(target_ds);
    memcpy(payload + 4, &be32, 4);

    if (send_header(fd, CT_MSG_REBALANCE_START, 8) != 0 ||
        send_all(fd, payload, 8) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_REBALANCE_RESP || resp_len < 4) {
        close(fd);
        return MDS_ERR_IO;
    }

    int32_t st_be;
    if (recv_all(fd, &st_be, 4) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);
    return (enum mds_status)(int32_t)be32toh((uint32_t)st_be);
}

enum mds_status cluster_transport_request_rebalance_status(
    const char *mds_host, uint16_t mds_port,
    struct rebalance_status_info *info)
{
    if (info == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(info, 0, sizeof(*info));

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    if (send_header(fd, CT_MSG_REBALANCE_STATUS_REQ, 0) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_REBALANCE_RESP || resp_len < 49) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp[49];
    if (recv_all(fd, resp, sizeof(resp)) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);

    info->state = resp[0];

    uint32_t be32;
    uint64_t be64;

    memcpy(&be32, resp + 1, 4);
    info->source_ds = be32toh(be32);
    memcpy(&be32, resp + 5, 4);
    info->target_ds = be32toh(be32);
    memcpy(&be32, resp + 9, 4);
    info->candidates_in_pass = be32toh(be32);
    memcpy(&be32, resp + 13, 4);
    info->files_moved = be32toh(be32);
    memcpy(&be64, resp + 17, 8);
    info->bytes_copied = be64toh(be64);
    memcpy(&be32, resp + 25, 4);
    info->pass_count = be32toh(be32);
    memcpy(&be32, resp + 29, 4);
    info->skipped_active_layout = be32toh(be32);
    memcpy(&be32, resp + 33, 4);
    info->skipped_active_writer = be32toh(be32);
    memcpy(&be32, resp + 37, 4);
    info->skipped_sparse = be32toh(be32);
    memcpy(&be32, resp + 41, 4);
    info->skipped_multi_stripe = be32toh(be32);
    memcpy(&be32, resp + 45, 4);
    info->skipped_error = be32toh(be32);

    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Tiering client requests (mds-admin CLI -> daemon)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_tiering_start(
    const char *mds_host, uint16_t mds_port,
    const struct tiering_config *cfg)
{
    if (cfg == NULL) {
        return MDS_ERR_INVAL;
    }
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    uint8_t payload[24];
    uint32_t be32;
    uint64_t be64;

    be32 = htobe32(cfg->eval_interval_sec);
    memcpy(payload, &be32, 4);
    be64 = htobe64((uint64_t)(cfg->promote_threshold * 1000.0));
    memcpy(payload + 4, &be64, 8);
    be64 = htobe64((uint64_t)(cfg->demote_threshold * 1000.0));
    memcpy(payload + 12, &be64, 8);
    be32 = htobe32(cfg->cooldown_evals);
    memcpy(payload + 20, &be32, 4);

    if (send_header(fd, CT_MSG_TIERING_START, 24) != 0 ||
        send_all(fd, payload, 24) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_TIERING_RESP || resp_len < 4) {
        close(fd);
        return MDS_ERR_IO;
    }

    int32_t st_be;
    if (recv_all(fd, &st_be, 4) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);
    return (enum mds_status)(int32_t)be32toh((uint32_t)st_be);
}

enum mds_status cluster_transport_request_tiering_stop(
    const char *mds_host, uint16_t mds_port)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    if (send_header(fd, CT_MSG_TIERING_STOP, 0) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_TIERING_RESP || resp_len < 4) {
        close(fd);
        return MDS_ERR_IO;
    }

    int32_t st_be;
    if (recv_all(fd, &st_be, 4) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);
    return (enum mds_status)(int32_t)be32toh((uint32_t)st_be);
}

enum mds_status cluster_transport_request_tiering_status(
    const char *mds_host, uint16_t mds_port,
    struct tiering_status_info *info)
{
    if (info == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(info, 0, sizeof(*info));

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    if (send_header(fd, CT_MSG_TIERING_STATUS_REQ, 0) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_TIERING_RESP || resp_len < 56) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp[56];
    if (recv_all(fd, resp, sizeof(resp)) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);

    uint32_t be32;
    uint64_t be64;

    memcpy(&be32, resp, 4);
    info->state = be32toh(be32);
    memcpy(&be32, resp + 4, 4);
    info->eval_count = be32toh(be32);
    memcpy(&be32, resp + 8, 4);
    info->candidates_in_eval = be32toh(be32);
    memcpy(&be32, resp + 12, 4);
    info->files_promoted = be32toh(be32);
    memcpy(&be32, resp + 16, 4);
    info->files_demoted = be32toh(be32);
    memcpy(&be64, resp + 20, 8);
    info->bytes_moved = be64toh(be64);
    memcpy(&be32, resp + 28, 4);
    info->skipped_active_layout = be32toh(be32);
    memcpy(&be32, resp + 32, 4);
    info->skipped_active_writer = be32toh(be32);
    memcpy(&be32, resp + 36, 4);
    info->skipped_sparse = be32toh(be32);
    memcpy(&be32, resp + 40, 4);
    info->skipped_multi_stripe = be32toh(be32);
    memcpy(&be32, resp + 44, 4);
    info->skipped_cooldown = be32toh(be32);
    memcpy(&be32, resp + 48, 4);
    info->skipped_no_target_ds = be32toh(be32);
    memcpy(&be32, resp + 52, 4);
    info->skipped_error = be32toh(be32);

    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Cluster membership admin handlers
 * ----------------------------------------------------------------------- */

void cluster_transport_server_set_membership(struct cluster_server *srv,
                                              struct cluster_membership *mem)
{
    if (srv != NULL) {
        srv->membership = mem;
}
}

/*
 * NODE_JOIN payload (v2):
 *   mds_id(4 BE) + nfs_port(2 BE) + grpc_port(2 BE) +
 *   hostname_len(2 BE) + hostname(N) +
 *   role(1) + lifecycle(1) + partner_id(4 BE) + addr_len(2 BE) + cluster_addr(M).
 *
 * Minimum: 10 + 1(hostname) + 8 = 19 bytes.
 */
static void handle_node_join(struct cluster_server *srv, int conn_fd,
                             const uint8_t *payload, uint32_t plen)
{
    uint8_t resp_status = 1; /* error by default */

    if (srv->membership == NULL || plen < 19) {
        goto respond;
}

    uint32_t mds_id_be;
    memcpy(&mds_id_be, payload, 4);
    uint32_t mds_id = be32toh(mds_id_be);

    uint16_t nfs_port_be, grpc_port_be, hlen_be;
    memcpy(&nfs_port_be, payload + 4, 2);
    memcpy(&grpc_port_be, payload + 6, 2);
    memcpy(&hlen_be, payload + 8, 2);

    uint16_t nfs_port  = be16toh(nfs_port_be);
    uint16_t grpc_port = be16toh(grpc_port_be);
    uint16_t hlen      = be16toh(hlen_be);

    if (hlen == 0 || hlen >= 256) {
        goto respond;
}

    /* After hostname: role(1)+lifecycle(1)+partner_id(4)+alen(2)+addr(M). */
    uint32_t tail_off = 10 + (uint32_t)hlen;
    if (tail_off + 8 > plen) {
        goto respond;
}

    uint8_t role_byte = payload[tail_off];
    uint8_t lc_byte   = payload[tail_off + 1];

    uint32_t partner_be;
    memcpy(&partner_be, payload + tail_off + 2, 4);
    uint32_t partner_id = be32toh(partner_be);

    uint16_t alen_be;
    memcpy(&alen_be, payload + tail_off + 6, 2);
    uint16_t alen = be16toh(alen_be);

    if (alen >= 64) {
        goto respond;
}
    if (tail_off + 8 + (uint32_t)alen != plen) {
        goto respond;
}

    struct cluster_member member;
    memset(&member, 0, sizeof(member));
    member.mds_id = mds_id;
    member.nfs_port = nfs_port;
    member.grpc_port = grpc_port;
    member.join_time_sec = (uint64_t)time(NULL);
    memcpy(member.hostname, payload + 10, hlen);
    member.hostname[hlen] = '\0';
    member.role = (enum node_role)role_byte;
    member.lifecycle = (enum node_lifecycle)lc_byte;
    member.failover_partner_id = partner_id;
    if (alen > 0) {
        memcpy(member.cluster_addr, payload + tail_off + 8, alen);
    }
    member.cluster_addr[alen] = '\0';

    enum mds_status st = cluster_node_join(srv->membership, &member);
    resp_status = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);

respond:
    send_header(conn_fd, CT_MSG_MEMBERSHIP_RESP, 1);
    send_all(conn_fd, &resp_status, 1);
}

/*
 * NODE_LEAVE payload: mds_id(4 BE).
 */
static void handle_node_leave(struct cluster_server *srv, int conn_fd,
                              const uint8_t *payload, uint32_t plen)
{
    uint8_t resp_status = 1;

    if (srv->membership == NULL || plen != 4) {
        goto respond;
}

    uint32_t mds_id_be;
    memcpy(&mds_id_be, payload, 4);
    uint32_t mds_id = be32toh(mds_id_be);

    enum mds_status st = cluster_node_leave(srv->membership, mds_id);
    resp_status = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);

respond:
    send_header(conn_fd, CT_MSG_MEMBERSHIP_RESP, 1);
    send_all(conn_fd, &resp_status, 1);
}

/*
 * SET_LIFECYCLE payload: [mds_id(4 BE)][lifecycle(1)] = 5 bytes.
 * Response: CT_MSG_MEMBERSHIP_RESP with status byte.
 */
static void handle_set_lifecycle(struct cluster_server *srv, int conn_fd,
                                 const uint8_t *payload, uint32_t plen)
{
    enum mds_status st = MDS_ERR_INVAL;

    if (srv->membership == NULL || plen != 5) {
        goto respond;
}

    uint32_t mds_id_be;
    memcpy(&mds_id_be, payload, 4);
    uint32_t mds_id = be32toh(mds_id_be);

    uint8_t lc_byte = payload[4];
    switch (lc_byte) {
    case NODE_IDLE:
    case NODE_JOINING:
    case NODE_ACTIVE_SERVING:
    case NODE_DRAINING:
    case NODE_DRAINED:
        break;
    default:
        goto respond;
    }

    st = cluster_membership_set_lifecycle(
        srv->membership, mds_id, (enum node_lifecycle)lc_byte);

respond:;
    uint8_t resp_status = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);
    send_header(conn_fd, CT_MSG_MEMBERSHIP_RESP, 1);
    send_all(conn_fd, &resp_status, 1);
}

/*
 * CLUSTER_STATUS response payload (v2):
 *   count(4 BE) + per-member:
 *     mds_id(4) + nfs_port(2) + grpc_port(2) + join_time(8) +
 *     hlen(2) + hostname(N) +
 *     role(1) + lifecycle(1) + partner_id(4) + alen(2) + cluster_addr(M).
 */
static void handle_cluster_status(const struct cluster_server *srv, int conn_fd,
                                  const uint8_t *payload, uint32_t plen)
{
    (void)payload;
    (void)plen;

    struct cluster_member *members = NULL;
    uint32_t count = 0;
    enum mds_status st = MDS_ERR_INVAL;

    if (srv->membership != NULL) {
        st = cluster_membership_list(srv->membership, &members, &count);
    }

    if (st != MDS_OK) {
        /* Send error: status(1) = error code, no payload. */
        uint8_t err = (uint8_t)(-(int)st);
        send_header(conn_fd, CT_MSG_CLUSTER_STATUS_RESP, 1);
        send_all(conn_fd, &err, 1);
        return;
    }

    /* Compute response size: status(1) + count(4) + per-member data. */
    size_t total = 1 + 4; /* status + count */
    for (uint32_t i = 0; i < count; i++) {
        total += 4 + 2 + 2 + 8 + 2 + strlen(members[i].hostname)
               + 1 + 1 + 4 + 2 + strlen(members[i].cluster_addr);
    }

    uint8_t *resp = malloc(total);
    if (resp == NULL) {
        free(members);
        uint8_t err = (uint8_t)(-(int)MDS_ERR_NOMEM);
        send_header(conn_fd, CT_MSG_CLUSTER_STATUS_RESP, 1);
        send_all(conn_fd, &err, 1);
        return;
    }

    size_t off = 0;
    resp[off++] = 0; /* status = OK */
    uint32_t count_be = htobe32(count);
    memcpy(resp + off, &count_be, 4); off += 4;

    for (uint32_t i = 0; i < count; i++) {
        uint32_t id_be = htobe32(members[i].mds_id);
        memcpy(resp + off, &id_be, 4); off += 4;

        uint16_t np_be = htobe16(members[i].nfs_port);
        memcpy(resp + off, &np_be, 2); off += 2;

        uint16_t gp_be = htobe16(members[i].grpc_port);
        memcpy(resp + off, &gp_be, 2); off += 2;

        uint64_t jt_be = htobe64(members[i].join_time_sec);
        memcpy(resp + off, &jt_be, 8); off += 8;

        uint16_t hlen = (uint16_t)strlen(members[i].hostname);
        uint16_t hlen_be = htobe16(hlen);
        memcpy(resp + off, &hlen_be, 2); off += 2;

        memcpy(resp + off, members[i].hostname, hlen); off += hlen;

        resp[off++] = (uint8_t)members[i].role;
        resp[off++] = (uint8_t)members[i].lifecycle;

        uint32_t part_be = htobe32(members[i].failover_partner_id);
        memcpy(resp + off, &part_be, 4); off += 4;

        uint16_t alen = (uint16_t)strlen(members[i].cluster_addr);
        uint16_t alen_be = htobe16(alen);
        memcpy(resp + off, &alen_be, 2); off += 2;

        if (alen > 0) {
            memcpy(resp + off, members[i].cluster_addr, alen);
            off += alen;
        }
    }

    send_header(conn_fd, CT_MSG_CLUSTER_STATUS_RESP, (uint32_t)total);
    send_all(conn_fd, resp, total);

    free(resp);
    free(members);
}

/* -----------------------------------------------------------------------
 * Cluster membership client requests (mds-admin CLI -> daemon)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_node_join(
    const char *mds_host, uint16_t mds_port,
    uint32_t mds_id, const char *hostname,
    uint16_t nfs_port, uint16_t grpc_port,
    enum node_role role, enum node_lifecycle lifecycle,
    uint32_t failover_partner_id, const char *cluster_addr)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
}

    size_t hlen = strlen(hostname);
    if (hlen >= 256) {
        close(fd);
        return MDS_ERR_INVAL;
    }

    size_t alen = (cluster_addr != NULL) ? strlen(cluster_addr) : 0;
    if (alen >= 64) {
        close(fd);
        return MDS_ERR_INVAL;
    }

    /* mds_id(4)+nfs(2)+grpc(2)+hlen(2)+host(N)+role(1)+lc(1)+partner(4)+alen(2)+addr(M) */
    size_t plen = 10 + hlen + 8 + alen;
    uint8_t buf[10 + 256 + 8 + 64];

    uint32_t id_be = htobe32(mds_id);
    memcpy(buf, &id_be, 4);

    uint16_t np_be = htobe16(nfs_port);
    memcpy(buf + 4, &np_be, 2);

    uint16_t gp_be = htobe16(grpc_port);
    memcpy(buf + 6, &gp_be, 2);

    uint16_t hl_be = htobe16((uint16_t)hlen);
    memcpy(buf + 8, &hl_be, 2);

    memcpy(buf + 10, hostname, hlen);

    size_t off = 10 + hlen;
    buf[off]     = (uint8_t)role;
    buf[off + 1] = (uint8_t)lifecycle;
    off += 2;

    uint32_t partner_be = htobe32(failover_partner_id);
    memcpy(buf + off, &partner_be, 4);
    off += 4;

    uint16_t al_be = htobe16((uint16_t)alen);
    memcpy(buf + off, &al_be, 2);
    off += 2;

    if (alen > 0) {
        memcpy(buf + off, cluster_addr, alen);
        off += alen;
    }
    (void)off;

    if (send_header(fd, CT_MSG_NODE_JOIN, (uint32_t)plen) != 0 ||
        send_all(fd, buf, plen) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_MEMBERSHIP_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);
    return decode_wire_status(status);
}

enum mds_status cluster_transport_request_node_leave(
    const char *mds_host, uint16_t mds_port, uint32_t mds_id)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
}

    uint32_t id_be = htobe32(mds_id);
    if (send_header(fd, CT_MSG_NODE_LEAVE, 4) != 0 ||
        send_all(fd, &id_be, 4) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_MEMBERSHIP_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);
    return decode_wire_status(status);
}

enum mds_status cluster_transport_request_set_lifecycle(
    const char *mds_host, uint16_t mds_port,
    uint32_t mds_id, enum node_lifecycle state)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
}

    uint8_t payload[5];
    uint32_t id_be = htobe32(mds_id);
    memcpy(payload, &id_be, 4);
    payload[4] = (uint8_t)state;

    if (send_header(fd, CT_MSG_SET_LIFECYCLE, 5) != 0 ||
        send_all(fd, payload, 5) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_MEMBERSHIP_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);
    return decode_wire_status(status);
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status cluster_transport_request_cluster_status(
    const char *mds_host, uint16_t mds_port,
    struct cluster_member **out, uint32_t *count)
{
    if (out == NULL || count == NULL) {
        return MDS_ERR_INVAL;
}

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
}

    if (send_header(fd, CT_MSG_CLUSTER_STATUS_REQ, 0) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_CLUSTER_STATUS_RESP) {
        close(fd);
        return MDS_ERR_IO;
    }

    if (resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t *resp = malloc(resp_len);
    if (resp == NULL) {
        close(fd);
        return MDS_ERR_NOMEM;
    }
    if (recv_all(fd, resp, resp_len) != 0) {
        free(resp);
        close(fd);
        return MDS_ERR_IO;
    }
    close(fd);

    /* First byte is status: 0 = OK, non-zero = error code. */
    uint8_t status = resp[0];
    if (status != 0) {
        free(resp);
        /* NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange) */
        /* NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange) */
        return (enum mds_status)(-(int)status);
    }

    if (resp_len < 5) { /* status(1) + count(4) */
        free(resp);
        return MDS_ERR_IO;
    }

    uint32_t cnt_be;
    memcpy(&cnt_be, resp + 1, 4);
    uint32_t cnt = be32toh(cnt_be);

    /* Reject obviously bogus counts to bound allocation. */
    if (cnt > MDS_MAX_NODES) {
        free(resp);
        return MDS_ERR_IO;
    }

    struct cluster_member *arr = NULL;
    if (cnt > 0) {
        arr = calloc(cnt, sizeof(*arr));
        if (arr == NULL) {
            free(resp);
            return MDS_ERR_NOMEM;
        }
    }

    size_t off = 5; /* past status(1) + count(4) */
    for (uint32_t i = 0; i < cnt; i++) {
        if (off + 26 > resp_len) {
            free(arr);
            free(resp);
            return MDS_ERR_IO;
        }

        uint32_t id_be;
        memcpy(&id_be, resp + off, 4); off += 4;
        arr[i].mds_id = be32toh(id_be);

        uint16_t np_be;
        memcpy(&np_be, resp + off, 2); off += 2;
        arr[i].nfs_port = be16toh(np_be);

        uint16_t gp_be;
        memcpy(&gp_be, resp + off, 2); off += 2;
        arr[i].grpc_port = be16toh(gp_be);

        uint64_t jt_be;
        memcpy(&jt_be, resp + off, 8); off += 8;
        arr[i].join_time_sec = be64toh(jt_be);

        uint16_t hl_be;
        memcpy(&hl_be, resp + off, 2); off += 2;
        uint16_t hlen = be16toh(hl_be);

        if (hlen >= 256 || off + hlen > resp_len) {
            free(arr);
            free(resp);
            return MDS_ERR_IO;
        }
        memcpy(arr[i].hostname, resp + off, hlen);
        arr[i].hostname[hlen] = '\0';
        off += hlen;

        /* role(1) + lifecycle(1) + partner_id(4) + alen(2) + addr(M). */
        if (off + 8 > resp_len) {
            free(arr);
            free(resp);
            return MDS_ERR_IO;
        }
        arr[i].role = (enum node_role)resp[off];
        arr[i].lifecycle = (enum node_lifecycle)resp[off + 1];
        off += 2;

        uint32_t part_be;
        memcpy(&part_be, resp + off, 4); off += 4;
        arr[i].failover_partner_id = be32toh(part_be);

        uint16_t al_be;
        memcpy(&al_be, resp + off, 2); off += 2;
        uint16_t alen = be16toh(al_be);

        if (alen >= 64 || off + alen > resp_len) {
            free(arr);
            free(resp);
            return MDS_ERR_IO;
        }
        if (alen > 0) {
            memcpy(arr[i].cluster_addr, resp + off, alen);
            off += alen;
        }
        arr[i].cluster_addr[alen] = '\0';
    }

    free(resp);
    *out = arr;
    *count = cnt;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Membership-backed transport factories (Seq 5)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_connect_by_id(
    const struct cluster_membership *mem,
    uint32_t mds_id,
    struct rename_2pc_transport **out)
{
    if (mem == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }

    char host[256];
    uint16_t port = 0;
    enum mds_status st = cluster_membership_resolve_peer(
        mem, mds_id, host, sizeof(host), &port);
    if (st != MDS_OK) {
        return st;
    }

    return cluster_transport_connect(host, port, out);
}

enum mds_status cluster_transport_connect_migration_by_id(
    const struct cluster_membership *mem,
    uint32_t mds_id,
    struct migration_transport **out)
{
    if (mem == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }

    char host[256];
    uint16_t port = 0;
    enum mds_status st = cluster_membership_resolve_peer(
        mem, mds_id, host, sizeof(host), &port);
    if (st != MDS_OK) {
        return st;
    }

    return cluster_transport_connect_migration(host, port, out);
}

/* -----------------------------------------------------------------------
 * Client requests: drain, detach, force-remove (Seq 8)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_drain_self(
    const char *mds_host, uint16_t mds_port,
    uint32_t dest_mds_id)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    uint32_t dest_be = htobe32(dest_mds_id);
    if (send_header(fd, CT_MSG_DRAIN_SELF, 4) != 0 ||
        send_all(fd, &dest_be, 4) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }
    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_MEMBERSHIP_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);
    return decode_wire_status(status);
}

enum mds_status cluster_transport_request_standby_detach(
    const char *mds_host, uint16_t mds_port,
    uint32_t mds_id)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    uint32_t id_be = htobe32(mds_id);
    if (send_header(fd, CT_MSG_STANDBY_DETACH, 4) != 0 ||
        send_all(fd, &id_be, 4) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_MEMBERSHIP_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);
    return decode_wire_status(status);
}

enum mds_status cluster_transport_request_force_remove(
    const char *mds_host, uint16_t mds_port,
    uint32_t mds_id)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    uint32_t id_be = htobe32(mds_id);
    if (send_header(fd, CT_MSG_FORCE_REMOVE, 4) != 0 ||
        send_all(fd, &id_be, 4) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_MEMBERSHIP_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);
    return decode_wire_status(status);
}


/* -----------------------------------------------------------------------
 * Migration tracker setter (Seq 10)
 * ----------------------------------------------------------------------- */

void cluster_transport_server_set_migration_tracker(
    struct cluster_server *srv, struct migration_tracker *tracker)
{
    if (srv != NULL) {
        srv->tracker = tracker;
    }
}

void cluster_transport_server_set_config(struct cluster_server *srv,
                                         const struct mds_config *cfg)
{
    if (srv == NULL) {
        return;
    }
    srv->cfg = cfg;

    /* Populate admin allowed hosts ACL from config.
     * This is separate from cluster_peer[] so operators can grant
     * web-UI / monitoring access without adding an MDS peer. */
    if (cfg != NULL) {
        srv->admin_allowed_host_count = 0;
        for (uint32_t i = 0; i < cfg->admin_allowed_host_count && i < 32; i++) {
            if (inet_pton(AF_INET, cfg->admin_allowed_hosts[i],
                          &srv->admin_allowed_hosts[srv->admin_allowed_host_count]) == 1) {
                srv->admin_allowed_host_count++;
            }
        }
        if (srv->admin_allowed_host_count > 0) {
            MDS_LOG_INFO(LOG_COMP_CLUSTER,
                "admin_allowed_hosts: %u host(s) permitted",
                (unsigned)srv->admin_allowed_host_count);
        }
    }
}



/* -----------------------------------------------------------------------
 * DS add/set-state/remove admin handlers (Item 47b)
 *
 * All require srv->cq.  Writes go through the commit queue for
 * replication.  State transitions to ONLINE install proxy mounts;
 * removal clears proxy mounts after CQ delete.
 * ----------------------------------------------------------------------- */

static void send_status_resp(int conn_fd, uint8_t msg_type,
                             enum mds_status st)
{
    uint8_t status = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);
    send_header(conn_fd, msg_type, 1);
    send_all(conn_fd, &status, 1);
}

static enum mds_status refresh_ds_cache(const struct cluster_server *srv)
{
    if (srv == NULL || srv->ds_cache == NULL || srv->cat == NULL) {
        return MDS_OK;
    }
    if (ds_cache_invalidate(srv->ds_cache, srv->cat) != 0) {
        return MDS_ERR_IO;
    }
    return MDS_OK;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void handle_ds_patch_ready_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);
static void handle_ds_patch_clear_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);

static void handle_ds_provision_admin(const struct cluster_server *srv,
                                      int conn_fd,
                                      const uint8_t *payload, uint32_t plen);

static void handle_ds_validate_admin(const struct cluster_server *srv,
                                     int conn_fd,
                                     const uint8_t *payload, uint32_t plen);
static void handle_ds_validate_clr_admin(const struct cluster_server *srv,
                                         int conn_fd,
                                         const uint8_t *payload, uint32_t plen);

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void handle_ds_add_admin(const struct cluster_server *srv,
                                int conn_fd,
                                const uint8_t *payload, uint32_t plen)
{
    /* ds_id(4) tier(4) total(8) used(8) port(2) addr_len(2) addr */
    if (srv->cq == NULL || plen < 28) {
        send_status_resp(conn_fd, CT_MSG_DS_ADD_RESP, MDS_ERR_INVAL);
        return;
    }

    struct commit_op cop;
    memset(&cop, 0, sizeof(cop));
    cop.type = COMMIT_OP_DS_REGISTER;

    struct mds_ds_info *info = &cop.args.ds_register.info;
    size_t off = 0;

    uint32_t id_be;
    memcpy(&id_be, payload + off, 4); off += 4;
    info->ds_id = be32toh(id_be);

    uint32_t tier_be;
    memcpy(&tier_be, payload + off, 4); off += 4;
    info->tier = be32toh(tier_be);

    uint64_t total_be;
    memcpy(&total_be, payload + off, 8); off += 8;
    info->total_bytes = be64toh(total_be);

    uint64_t used_be;
    memcpy(&used_be, payload + off, 8); off += 8;
    info->used_bytes = be64toh(used_be);

    uint16_t port_be;
    memcpy(&port_be, payload + off, 2); off += 2;
    info->port = be16toh(port_be);

    uint16_t al_be;
    memcpy(&al_be, payload + off, 2); off += 2;
    uint16_t alen = be16toh(al_be);

    if (alen >= MDS_DS_ADDR_MAX || off + alen > plen) {
        send_status_resp(conn_fd, CT_MSG_DS_ADD_RESP, MDS_ERR_INVAL);
        return;
    }
    memcpy(info->addr, payload + off, alen);
    info->addr[alen] = '\0';
    off += alen;

    /* V2 fields: mode(1) transport(1) host_len(2) host export_len(2) export */
    if (off + 2 <= plen) {
        info->mode = payload[off++];
        info->transport = payload[off++];

        if (off + 2 <= plen) {
            uint16_t hl_be;
            memcpy(&hl_be, payload + off, 2); off += 2;
            uint16_t hlen = be16toh(hl_be);
            if (hlen < MDS_DS_HOST_MAX && off + hlen <= plen) {
                memcpy(info->host, payload + off, hlen);
                info->host[hlen] = '\0';
                off += hlen;
            }
        }
        if (off + 2 <= plen) {
            uint16_t el_be;
            memcpy(&el_be, payload + off, 2); off += 2;
            uint16_t elen = be16toh(el_be);
            if (elen < MDS_DS_EXPORT_MAX && off + elen <= plen) {
                memcpy(info->export_path, payload + off, elen);
                info->export_path[elen] = '\0';
                off += elen;
            }
        }
    (void)off;  /* consumed all fields */
    } else {
        /* Legacy payload -- default to patched/tcp, parse addr. */
        info->mode = DS_MODE_GENERIC;
        info->transport = DS_TRANSPORT_TCP;
        /* Parse "host:/export" from legacy addr. */
        {
            const char *c = strchr(info->addr, ':');
            if (c != NULL && c > info->addr) {
                size_t hl = (size_t)(c - info->addr);
                if (hl >= MDS_DS_HOST_MAX) { hl = MDS_DS_HOST_MAX - 1; }
                memcpy(info->host, info->addr, hl);
                info->host[hl] = '\0';
                (void)snprintf(info->export_path,
                         sizeof(info->export_path), "%s", c + 1);
            } else {
                (void)snprintf(info->host, sizeof(info->host),
                         "%s", info->addr);
                (void)snprintf(info->export_path,
                         sizeof(info->export_path), "/");
            }
        }
    }

    /* V3: per-transport ports if present. */
    if (off + 4 <= plen) {
        uint16_t tp_be, rp_be;
        memcpy(&tp_be, payload + off, 2); off += 2;
        memcpy(&rp_be, payload + off, 2); off += 2;
        info->tcp_port = be16toh(tp_be);
        info->rdma_port = be16toh(rp_be);
    }
    (void)off;

    /* Phase 6B: tier-mode validation.
     * Reject adding a DS if its mode conflicts with existing DSes
     * in the same tier.  No mixed patched + generic within a tier. */
    if (srv->cat != NULL) {
        struct mds_ds_info *existing = NULL;
        uint32_t ecnt = 0;
        if (mds_cat_ds_list(srv->cat, &existing, &ecnt) == MDS_OK) {
            for (uint32_t ei = 0; ei < ecnt; ei++) {
                if (existing[ei].tier == info->tier &&
                    existing[ei].ds_id != info->ds_id &&
                    existing[ei].mode != info->mode &&
                    existing[ei].state != DS_OFFLINE) {
                    free(existing);
                    send_status_resp(conn_fd, CT_MSG_DS_ADD_RESP,
                                     MDS_ERR_INVAL);
                    return;
                }
            }
            free(existing);
        }
    }

    /* State forced to OFFLINE by CQ apply_ds_register. */
    enum mds_status st;
    /* cppcheck-suppress knownConditionTrueFalse */
    if (srv->cq != NULL) {
        st = commit_queue_submit(
            (struct commit_queue *)srv->cq, &cop);
    } else if (srv->cat != NULL) {
        /* RonDB mode: direct catalogue dispatch. */
        info->state = DS_OFFLINE;
        st = mds_cat_ds_put(srv->cat, NULL, info);
    } else {
        st = MDS_ERR_INVAL;
    }
    if (st == MDS_OK) {
        (void)refresh_ds_cache(srv);
    }
    send_status_resp(conn_fd, CT_MSG_DS_ADD_RESP, st);
}

static void handle_ds_set_state_admin(const struct cluster_server *srv,
                                      int conn_fd,
                                      const uint8_t *payload, uint32_t plen)
{
    if (plen < 8) {
        send_status_resp(conn_fd, CT_MSG_DS_SET_STATE_RESP, MDS_ERR_INVAL);
        return;
    }
    if (srv->cq == NULL && srv->cat == NULL) {
        send_status_resp(conn_fd, CT_MSG_DS_SET_STATE_RESP, MDS_ERR_INVAL);
        return;
    }

    uint32_t ds_id_be, state_be;
    memcpy(&ds_id_be, payload, 4);
    memcpy(&state_be, payload + 4, 4);
    uint32_t ds_id = be32toh(ds_id_be);
    uint32_t new_state = be32toh(state_be);

    /* Read current state to detect no-op transitions for safe rollback. */
    bool was_already_online = false;
    {
        struct mds_ds_info cur;
        if (mds_cat_ds_get(srv->cat, ds_id, &cur) == MDS_OK) {
            was_already_online = (cur.state == DS_ONLINE);
        }
    }

    /* If transitioning to ONLINE, ensure proxy mount can be installed. */
    if (new_state == DS_ONLINE && srv->proxy != NULL &&
        srv->ds_mount_path_fmt[0] != '\0') {
        char mount_path[512];
        /* NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling) */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
        (void)snprintf(mount_path, sizeof(mount_path),
                 srv->ds_mount_path_fmt, (unsigned)ds_id);
#pragma GCC diagnostic pop
        enum mds_status pst = mds_proxy_mount_set(srv->proxy,
                                                   ds_id, mount_path);
        if (pst != MDS_OK) {
            send_status_resp(conn_fd, CT_MSG_DS_SET_STATE_RESP, pst);
            return;
        }
    }

    enum mds_status st;
    if (srv->cq != NULL) {
        struct commit_op cop;
        memset(&cop, 0, sizeof(cop));
        cop.type = COMMIT_OP_DS_STATE;
        cop.args.ds_state.ds_id = ds_id;
        cop.args.ds_state.new_state = new_state;
        st = commit_queue_submit((struct commit_queue *)srv->cq, &cop);
    } else {
        /* RonDB mode: direct catalogue dispatch (no CQ). */
        struct mds_ds_info info;
        st = mds_cat_ds_get(srv->cat, ds_id, &info);
        if (st == MDS_OK) {
            info.state = new_state;
            st = mds_cat_ds_put(srv->cat, NULL, &info);
        }
    }

    /* Roll back proxy mount only if CQ failed AND the DS was not
     * already online (avoid clearing a pre-existing valid mount). */
    if (st != MDS_OK && new_state == DS_ONLINE &&
        !was_already_online && srv->proxy != NULL) {
        mds_proxy_mount_clear(srv->proxy, ds_id);
    }
    if (st == MDS_OK) {
        (void)refresh_ds_cache(srv);
    }

    send_status_resp(conn_fd, CT_MSG_DS_SET_STATE_RESP, st);
}

static void handle_ds_remove_admin(const struct cluster_server *srv,
                                    int conn_fd,
                                    const uint8_t *payload, uint32_t plen)
{
    if (plen < 4) {
        send_status_resp(conn_fd, CT_MSG_DS_REMOVE_RESP, MDS_ERR_INVAL);
        return;
    }
    if (srv->cq == NULL && srv->cat == NULL) {
        send_status_resp(conn_fd, CT_MSG_DS_REMOVE_RESP, MDS_ERR_INVAL);
        return;
    }

    uint32_t ds_id_be;
    memcpy(&ds_id_be, payload, 4);
    uint32_t ds_id = be32toh(ds_id_be);

    enum mds_status st;
    if (srv->cq != NULL) {
        struct commit_op cop;
        memset(&cop, 0, sizeof(cop));
        cop.type = COMMIT_OP_DS_REMOVE;
        cop.args.ds_remove.ds_id = ds_id;
        st = commit_queue_submit((struct commit_queue *)srv->cq, &cop);
    } else {
        /* RonDB mode: direct catalogue dispatch. */
        st = mds_cat_ds_del(srv->cat, NULL, ds_id);
    }

    /* Clear proxy mapping only after successful CQ delete. */
    if (st == MDS_OK && srv->proxy != NULL) {
        mds_proxy_mount_clear(srv->proxy, ds_id);
    }
    if (st == MDS_OK) {
        (void)refresh_ds_cache(srv);
    }

    send_status_resp(conn_fd, CT_MSG_DS_REMOVE_RESP, st);
}




/* -----------------------------------------------------------------------
 * DS patch-ready/patch-clear handlers (Phase 7)
 * ----------------------------------------------------------------------- */

static void handle_ds_patch_ready_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen)
{
    /* ds_id(4) + bits(4) = 8 bytes */
    if (srv->cat == NULL || plen < 8) {
        send_status_resp(conn_fd, CT_MSG_DS_PATCH_READY_RESP,
                         MDS_ERR_INVAL);
        return;
    }

    uint32_t ds_id_be, bits_be;
    memcpy(&ds_id_be, payload, 4);
    memcpy(&bits_be, payload + 4, 4);
    uint32_t ds_id = be32toh(ds_id_be);
    uint32_t bits = be32toh(bits_be);

    struct mds_ds_info info;
    enum mds_status st = mds_cat_ds_get(srv->cat, ds_id, &info);
    if (st != MDS_OK) {
        send_status_resp(conn_fd, CT_MSG_DS_PATCH_READY_RESP, st);
        return;
    }
    if (info.mode != DS_MODE_GENERIC) {
        send_status_resp(conn_fd, CT_MSG_DS_PATCH_READY_RESP,
                         MDS_ERR_INVAL);
        return;
    }

    info.capabilities |= bits;

    st = mds_cat_ds_put(srv->cat, NULL, &info);
    if (st != MDS_OK) {
        send_status_resp(conn_fd, CT_MSG_DS_PATCH_READY_RESP, st);
        return;
    }
    (void)refresh_ds_cache(srv);
    send_status_resp(conn_fd, CT_MSG_DS_PATCH_READY_RESP, MDS_OK);
}

static void handle_ds_patch_clear_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen)
{
    if (srv->cat == NULL || plen < 4) {
        send_status_resp(conn_fd, CT_MSG_DS_PATCH_CLEAR_RESP,
                         MDS_ERR_INVAL);
        return;
    }

    uint32_t ds_id_be2;
    memcpy(&ds_id_be2, payload, 4);
    uint32_t ds_id = be32toh(ds_id_be2);

    struct mds_ds_info info;
    enum mds_status st = mds_cat_ds_get(srv->cat, ds_id, &info);
    if (st != MDS_OK) {
        send_status_resp(conn_fd, CT_MSG_DS_PATCH_CLEAR_RESP, st);
        return;
    }

    /* The request carries no bitmask (client sends ds_id only) and is
     * documented as "clear all patched readiness bits": reset the
     * whole capability word. */
    info.capabilities = 0;

    st = mds_cat_ds_put(srv->cat, NULL, &info);
    if (st != MDS_OK) {
        send_status_resp(conn_fd, CT_MSG_DS_PATCH_CLEAR_RESP, st);
        return;
    }
    (void)refresh_ds_cache(srv);
    send_status_resp(conn_fd, CT_MSG_DS_PATCH_CLEAR_RESP, MDS_OK);
}

/* -----------------------------------------------------------------------
 * DS provision handler (Phase 5)
 * ----------------------------------------------------------------------- */

static void handle_ds_patch_ready_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);
static void handle_ds_patch_clear_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);

static void handle_ds_provision_admin(const struct cluster_server *srv,
                                      int conn_fd,
                                      const uint8_t *payload, uint32_t plen)
{
    /* ds_id(4) + secret(32) + epoch(8) = 44 bytes */
    if (srv->cat == NULL || plen < 44) {
        send_status_resp(conn_fd, CT_MSG_DS_PROVISION_RESP, MDS_ERR_INVAL);
        return;
    }

    uint32_t ds_id_be;
    memcpy(&ds_id_be, payload, 4);
    uint32_t ds_id = be32toh(ds_id_be);

    const uint8_t *secret = payload + 4;
    uint64_t epoch_be;
    memcpy(&epoch_be, payload + 36, 8);
    uint64_t epoch = be64toh(epoch_be);

    /*
     * Validate: DS must already exist in the registry.
     * Reject unknown ds_id before writing any provisioning state
     * to prevent orphan records.
     */
    struct mds_ds_info info;
    enum mds_status st = mds_cat_ds_get(srv->cat, ds_id, &info);
    if (st != MDS_OK) {
        send_status_resp(conn_fd, CT_MSG_DS_PROVISION_RESP,
                         (st == MDS_ERR_NOTFOUND) ? MDS_ERR_NOTFOUND
                                                  : MDS_ERR_IO);
        return;
    }

    /* Store provisioning secret + epoch, then upgrade mode. */
    st = mds_cat_ds_provision_put(srv->cat, NULL, ds_id,
                                  secret, 32, epoch);
    if (st != MDS_OK) {
        send_status_resp(conn_fd, CT_MSG_DS_PROVISION_RESP, st);
        return;
    }

    /* Upgrade DS mode to DS_MODE_GENERIC if currently generic. */
    if (info.mode != DS_MODE_GENERIC) {
        info.mode = DS_MODE_GENERIC;
        st = mds_cat_ds_put(srv->cat, NULL, &info);
        if (st != MDS_OK) {
            send_status_resp(conn_fd, CT_MSG_DS_PROVISION_RESP, st);
            return;
        }
    }

    (void)refresh_ds_cache(srv);
    send_status_resp(conn_fd, CT_MSG_DS_PROVISION_RESP, MDS_OK);
}

/* -----------------------------------------------------------------------
 * DS validate/validate-clear handlers (Phase 6)
 * ----------------------------------------------------------------------- */

static void handle_ds_patch_ready_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);
static void handle_ds_patch_clear_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);

static void handle_ds_provision_admin(const struct cluster_server *srv,
                                      int conn_fd,
                                      const uint8_t *payload, uint32_t plen);

static void handle_ds_validate_admin(const struct cluster_server *srv,
                                     int conn_fd,
                                     const uint8_t *payload, uint32_t plen)
{
    if (srv->cat == NULL || plen < 4) {
        send_status_resp(conn_fd, CT_MSG_DS_VALIDATE_RESP, MDS_ERR_INVAL);
        return;
    }

    uint32_t ds_id_be;
    memcpy(&ds_id_be, payload, 4);
    uint32_t ds_id = be32toh(ds_id_be);

    /* Read DS, verify RDMA, set capability, write back. */
    struct mds_ds_info info;
    enum mds_status st = mds_cat_ds_get(srv->cat, ds_id, &info);
    if (st != MDS_OK) {
        send_status_resp(conn_fd, CT_MSG_DS_VALIDATE_RESP, st);
        return;
    }
    if ((info.transport & DS_TRANSPORT_RDMA) == 0) {
        send_status_resp(conn_fd, CT_MSG_DS_VALIDATE_RESP, MDS_ERR_INVAL);
        return;
    }

    /* Mark GPUDirect-validated; pairs with handle_ds_validate_clr_admin
     * which clears exactly this bit. */
    info.capabilities |= DS_CAP_GPUDIRECT;

    st = mds_cat_ds_put(srv->cat, NULL, &info);
    if (st != MDS_OK) {
        send_status_resp(conn_fd, CT_MSG_DS_VALIDATE_RESP, st);
        return;
    }

    /* Validation metadata was stored in a separate backend DBI but
     * never read back by any production code path.  The DS info
     * update via mds_cat_ds_put above is the authoritative state. */
    (void)refresh_ds_cache(srv);
    send_status_resp(conn_fd, CT_MSG_DS_VALIDATE_RESP, MDS_OK);
}

static void handle_ds_validate_clr_admin(const struct cluster_server *srv,
                                         int conn_fd,
                                         const uint8_t *payload, uint32_t plen)
{
    if (srv->cat == NULL || plen < 4) {
        send_status_resp(conn_fd, CT_MSG_DS_VALIDATE_CLR_RESP, MDS_ERR_INVAL);
        return;
    }

    uint32_t ds_id_be;
    memcpy(&ds_id_be, payload, 4);
    uint32_t ds_id = be32toh(ds_id_be);

    struct mds_ds_info info;
    enum mds_status st = mds_cat_ds_get(srv->cat, ds_id, &info);
    if (st != MDS_OK) {
        send_status_resp(conn_fd, CT_MSG_DS_VALIDATE_CLR_RESP, st);
        return;
    }

    /* Clear only the GPUDirect validation bit; other capability bits
     * (e.g. patch-readiness) are managed by separate requests. */
    info.capabilities &= ~(uint32_t)DS_CAP_GPUDIRECT;

    st = mds_cat_ds_put(srv->cat, NULL, &info);
    if (st != MDS_OK) {
        send_status_resp(conn_fd, CT_MSG_DS_VALIDATE_CLR_RESP, st);
        return;
    }

    /* Validation metadata cleared -- DS info update above is
     * the authoritative state. */
    (void)refresh_ds_cache(srv);
    send_status_resp(conn_fd, CT_MSG_DS_VALIDATE_CLR_RESP, MDS_OK);
}

/* -----------------------------------------------------------------------
 * DS registry list admin handler (Item 47)
 *
 * CT_MSG_DS_LIST_REQ payload: empty.
 * CT_MSG_DS_LIST_RESP payload:
 *     status(1) count(4 BE) { ds_id(4 BE) state(4 BE) tier(4 BE)
 *       total_bytes(8 BE) used_bytes(8 BE) port(2 BE)
 *       addr_len(2 BE) addr }*
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void handle_ds_patch_ready_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);
static void handle_ds_patch_clear_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen);

static void handle_ds_provision_admin(const struct cluster_server *srv,
                                      int conn_fd,
                                      const uint8_t *payload, uint32_t plen);

static void handle_ds_validate_admin(const struct cluster_server *srv,
                                     int conn_fd,
                                     const uint8_t *payload, uint32_t plen);
static void handle_ds_validate_clr_admin(const struct cluster_server *srv,
                                         int conn_fd,
                                         const uint8_t *payload, uint32_t plen);

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void handle_ds_add_admin(const struct cluster_server *srv,
                                int conn_fd,
                                const uint8_t *payload, uint32_t plen);
static void handle_ds_set_state_admin(const struct cluster_server *srv,
                                      int conn_fd,
                                      const uint8_t *payload, uint32_t plen);
static void handle_ds_remove_admin(const struct cluster_server *srv,
                                    int conn_fd,
                                    const uint8_t *payload, uint32_t plen);

static void handle_ds_list_admin(const struct cluster_server *srv,
                                 int conn_fd,
                                 const uint8_t *payload, uint32_t plen)
{
    (void)payload;
    (void)plen;

    if (srv->cat == NULL) {
        uint8_t err = 1;
        send_header(conn_fd, CT_MSG_DS_LIST_RESP, 1);
        send_all(conn_fd, &err, 1);
        return;
    }

    struct mds_ds_info *ds = NULL;
    uint32_t count = 0;
    enum mds_status st = mds_cat_ds_list(srv->cat, &ds, &count);
    if (st != MDS_OK) {
        uint8_t err = (uint8_t)(-(int)st);
        send_header(conn_fd, CT_MSG_DS_LIST_RESP, 1);
        send_all(conn_fd, &err, 1);
        return;
    }

    /* Calculate response size. */
    size_t total = 1 + 1 + 4;  /* status(1) + version(1) + count(4) */
    for (uint32_t i = 0; i < count; i++) {
        size_t alen = strnlen(ds[i].addr, MDS_DS_ADDR_MAX);
        size_t hlen = strnlen(ds[i].host, MDS_DS_HOST_MAX);
        size_t elen = strnlen(ds[i].export_path, MDS_DS_EXPORT_MAX);
        total += 4 + 4 + 4 + 8 + 8 + 2 + 2 + alen
               + 1 + 1 + 2 + hlen + 2 + elen;
    }

    uint8_t *resp = malloc(total);
    if (resp == NULL) {
        free(ds);
        uint8_t err = (uint8_t)(-(int)MDS_ERR_NOMEM);
        send_header(conn_fd, CT_MSG_DS_LIST_RESP, 1);
        send_all(conn_fd, &err, 1);
        return;
    }

    size_t off = 0;
    resp[off++] = 0;  /* status = OK */
    resp[off++] = 2;  /* wire format version = 2 */

    uint32_t cnt_be = htobe32(count);
    memcpy(resp + off, &cnt_be, 4); off += 4;

    for (uint32_t i = 0; i < count; i++) {
        uint32_t id_be = htobe32(ds[i].ds_id);
        memcpy(resp + off, &id_be, 4); off += 4;

        uint32_t state_be = htobe32(ds[i].state);
        memcpy(resp + off, &state_be, 4); off += 4;

        uint32_t tier_be = htobe32(ds[i].tier);
        memcpy(resp + off, &tier_be, 4); off += 4;

        uint64_t total_be = htobe64(ds[i].total_bytes);
        memcpy(resp + off, &total_be, 8); off += 8;

        uint64_t used_be = htobe64(ds[i].used_bytes);
        memcpy(resp + off, &used_be, 8); off += 8;

        uint16_t port_be = htobe16(ds[i].port);
        memcpy(resp + off, &port_be, 2); off += 2;

        size_t alen = strnlen(ds[i].addr, MDS_DS_ADDR_MAX);
        uint16_t al_be = htobe16((uint16_t)alen);
        memcpy(resp + off, &al_be, 2); off += 2;
        memcpy(resp + off, ds[i].addr, alen); off += alen;

        /* V2 structured fields. */
        resp[off++] = ds[i].mode;
        resp[off++] = ds[i].transport;
        size_t hlen = strnlen(ds[i].host, MDS_DS_HOST_MAX);
        uint16_t hl_be = htobe16((uint16_t)hlen);
        memcpy(resp + off, &hl_be, 2); off += 2;
        memcpy(resp + off, ds[i].host, hlen); off += hlen;
        size_t elen = strnlen(ds[i].export_path, MDS_DS_EXPORT_MAX);
        uint16_t el_be = htobe16((uint16_t)elen);
        memcpy(resp + off, &el_be, 2); off += 2;
        memcpy(resp + off, ds[i].export_path, elen); off += elen;
    }

    send_header(conn_fd, CT_MSG_DS_LIST_RESP, (uint32_t)off);
    send_all(conn_fd, resp, off);

    free(resp);
    free(ds);
}

/* -----------------------------------------------------------------------
 * Subtree list admin handler (Seq 10)
 *
 * CT_MSG_SUBTREE_LIST_ADMIN_REQ payload: owner_filter(4 BE).  0 = all.
 * CT_MSG_SUBTREE_LIST_ADMIN_RESP payload:
 *     status(1) count(4 BE) { path_len(2 BE) path owner(4 BE)
 *                              state(1) version(8 BE) op_count(8 BE) }*
 * ----------------------------------------------------------------------- */

static void handle_subtree_list_admin(const struct cluster_server *srv,
                                      int conn_fd,
                                      const uint8_t *payload, uint32_t plen)
{
    if (srv->smap == NULL || plen < 4) {
        uint8_t err = 1;
        send_header(conn_fd, CT_MSG_SUBTREE_LIST_ADMIN_RESP, 1);
        send_all(conn_fd, &err, 1);
        return;
    }

    uint32_t filter_be;
    memcpy(&filter_be, payload, 4);
    uint32_t owner_filter = be32toh(filter_be);

    uint32_t snap_count = 0;
    struct subtree_entry *snap = NULL;
    enum mds_status snap_st = subtree_map_list_snapshot(
        srv->smap, &snap, &snap_count);
    if (snap_st != MDS_OK) {
        uint8_t err = (uint8_t)(-(int)snap_st);
        send_header(conn_fd, CT_MSG_SUBTREE_LIST_ADMIN_RESP, 1);
        send_all(conn_fd, &err, 1);
        return;
    }

    /* Count matching entries and calculate response size. */
    uint32_t match_count = 0;
    size_t total = 1 + 4;  /* status(1) + count(4) */
    for (uint32_t i = 0; i < snap_count; i++) {
        if (owner_filter != 0 && snap[i].owner_mds_id != owner_filter) {
            continue;
        }
        size_t plen_i = strnlen(snap[i].path, MDS_MAX_PATH);
        total += 2 + plen_i + 4 + 1 + 8 + 8;
        match_count++;
    }

    uint8_t *resp = malloc(total);
    if (resp == NULL) {
        free(snap);
        uint8_t err = (uint8_t)(-(int)MDS_ERR_NOMEM);
        send_header(conn_fd, CT_MSG_SUBTREE_LIST_ADMIN_RESP, 1);
        send_all(conn_fd, &err, 1);
        return;
    }

    size_t off = 0;
    resp[off++] = 0;  /* status = OK */

    uint32_t cnt_be = htobe32(match_count);
    memcpy(resp + off, &cnt_be, 4); off += 4;

    for (uint32_t i = 0; i < snap_count; i++) {
        if (owner_filter != 0 && snap[i].owner_mds_id != owner_filter) {
            continue;
        }

        size_t plen_i = strnlen(snap[i].path, MDS_MAX_PATH);
        uint16_t pl_be = htobe16((uint16_t)plen_i);
        memcpy(resp + off, &pl_be, 2); off += 2;
        memcpy(resp + off, snap[i].path, plen_i); off += plen_i;

        uint32_t owner_be = htobe32(snap[i].owner_mds_id);
        memcpy(resp + off, &owner_be, 4); off += 4;

        resp[off++] = (uint8_t)snap[i].state;

        uint64_t ver_be = htobe64(snap[i].version);
        memcpy(resp + off, &ver_be, 8); off += 8;

        uint64_t ops_be = htobe64(atomic_load(&snap[i].op_count));
        memcpy(resp + off, &ops_be, 8); off += 8;
    }

    send_header(conn_fd, CT_MSG_SUBTREE_LIST_ADMIN_RESP, (uint32_t)off);
    send_all(conn_fd, resp, off);

    free(resp);
    free(snap);
}

/* -----------------------------------------------------------------------
 * Migration progress admin handler (Seq 10)
 *
 * CT_MSG_MIG_PROGRESS_ADMIN_REQ payload: empty.
 * CT_MSG_MIG_PROGRESS_ADMIN_RESP payload:
 *     status(1) state(4 BE) total(4 BE) done(4 BE)
 *     path_len(2 BE) path
 * ----------------------------------------------------------------------- */

static void handle_mig_progress_admin(const struct cluster_server *srv,
                                      int conn_fd,
                                      const uint8_t *payload, uint32_t plen)
{
    (void)payload;
    (void)plen;

    enum migration_state mstate = MIG_IDLE;
    char mpath[MDS_MAX_PATH];
    uint32_t mtotal = 0, mdone = 0;

    /* Defensive: tracker may not be attached yet; reply with the
     * handler's 1-byte error pattern instead of relying on the
     * tracker API tolerating NULL. */
    if (srv->tracker == NULL) {
        uint8_t err = (uint8_t)(-(int)MDS_ERR_INVAL);
        send_header(conn_fd, CT_MSG_MIG_PROGRESS_ADMIN_RESP, 1);
        send_all(conn_fd, &err, 1);
        return;
    }

    mpath[0] = '\0';
    migration_tracker_get_progress(srv->tracker, &mstate,
                                   mpath, sizeof(mpath),
                                   &mtotal, &mdone);

    size_t plen_m = strnlen(mpath, MDS_MAX_PATH);
    size_t resp_sz = 1 + 4 + 4 + 4 + 2 + plen_m;  /* status + state + total + done + len + path */
    uint8_t *resp = malloc(resp_sz);
    if (resp == NULL) {
        uint8_t err = (uint8_t)(-(int)MDS_ERR_NOMEM);
        send_header(conn_fd, CT_MSG_MIG_PROGRESS_ADMIN_RESP, 1);
        send_all(conn_fd, &err, 1);
        return;
    }

    size_t off = 0;
    resp[off++] = 0;  /* status = OK */

    uint32_t st_be = htobe32((uint32_t)mstate);
    memcpy(resp + off, &st_be, 4); off += 4;

    uint32_t tot_be = htobe32(mtotal);
    memcpy(resp + off, &tot_be, 4); off += 4;

    uint32_t done_be = htobe32(mdone);
    memcpy(resp + off, &done_be, 4); off += 4;

    uint16_t pl_be = htobe16((uint16_t)plen_m);
    memcpy(resp + off, &pl_be, 2); off += 2;
    memcpy(resp + off, mpath, plen_m); off += plen_m;

    send_header(conn_fd, CT_MSG_MIG_PROGRESS_ADMIN_RESP, (uint32_t)off);
    send_all(conn_fd, resp, off);
    free(resp);
}

/* -----------------------------------------------------------------------
 * Subtree list admin client request (Seq 10)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_subtree_list(
    const char *mds_host, uint16_t mds_port,
    uint32_t owner_filter,
    struct subtree_entry **out, uint32_t *count)
{
    if (out == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    uint32_t filter_be = htobe32(owner_filter);
    if (send_header(fd, CT_MSG_SUBTREE_LIST_ADMIN_REQ, 4) != 0 ||
        send_all(fd, &filter_be, 4) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_SUBTREE_LIST_ADMIN_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t *resp = malloc(resp_len);
    if (resp == NULL) {
        close(fd);
        return MDS_ERR_NOMEM;
    }
    if (recv_all(fd, resp, resp_len) != 0) {
        free(resp);
        close(fd);
        return MDS_ERR_IO;
    }
    close(fd);

    if (resp[0] != 0) {
        enum mds_status st = decode_wire_status(resp[0]);
        free(resp);
        return st;
    }


    if (resp_len < 5) {
        free(resp);
        return MDS_ERR_IO;
    }

    uint32_t cnt_be;
    memcpy(&cnt_be, resp + 1, 4);
    uint32_t cnt = be32toh(cnt_be);

    if (cnt > 65536) {
        free(resp);
        return MDS_ERR_IO;
    }

    struct subtree_entry *arr = NULL;
    if (cnt > 0) {
        arr = calloc(cnt, sizeof(*arr));
        if (arr == NULL) {
            free(resp);
            return MDS_ERR_NOMEM;
        }
    }

    size_t off = 5;
    for (uint32_t i = 0; i < cnt; i++) {
        if (off + 2 > resp_len) {
            free(arr);
            free(resp);
            return MDS_ERR_IO;
        }

        uint16_t pl_be;
        memcpy(&pl_be, resp + off, 2); off += 2;
        uint16_t plen_i = be16toh(pl_be);

        if (plen_i >= MDS_MAX_PATH || off + plen_i + 4 + 1 + 8 + 8 > resp_len) {
            free(arr);
            free(resp);
            return MDS_ERR_IO;
        }
        memcpy(arr[i].path, resp + off, plen_i);
        arr[i].path[plen_i] = '\0';
        off += plen_i;

        uint32_t own_be;
        memcpy(&own_be, resp + off, 4); off += 4;
        arr[i].owner_mds_id = be32toh(own_be);

        arr[i].state = (enum subtree_state)resp[off]; off += 1;

        uint64_t ver_be;
        memcpy(&ver_be, resp + off, 8); off += 8;
        arr[i].version = be64toh(ver_be);

        uint64_t ops_be;
        memcpy(&ops_be, resp + off, 8); off += 8;
        atomic_store(&arr[i].op_count, be64toh(ops_be));
    }

    *out = arr;
    *count = cnt;
    free(resp);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Migration progress admin client request (Seq 10)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_mig_progress(
    const char *mds_host, uint16_t mds_port,
    uint32_t *state, char *path, size_t path_cap,
    uint32_t *total, uint32_t *done)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    if (send_header(fd, CT_MSG_MIG_PROGRESS_ADMIN_REQ, 0) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_MIG_PROGRESS_ADMIN_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t *resp = malloc(resp_len);
    if (resp == NULL) {
        close(fd);
        return MDS_ERR_NOMEM;
    }
    if (recv_all(fd, resp, resp_len) != 0) {
        free(resp);
        close(fd);
        return MDS_ERR_IO;
    }
    close(fd);

    if (resp[0] != 0) {
        enum mds_status st = decode_wire_status(resp[0]);
        free(resp);
        return st;
    }

    if (resp_len < 15) {  /* 1 + 4 + 4 + 4 + 2 */
        free(resp);
        return MDS_ERR_IO;
    }

    size_t off = 1;

    uint32_t st_be;
    memcpy(&st_be, resp + off, 4); off += 4;
    if (state != NULL) { *state = be32toh(st_be); }

    uint32_t tot_be;
    memcpy(&tot_be, resp + off, 4); off += 4;
    if (total != NULL) { *total = be32toh(tot_be); }

    uint32_t done_be;
    memcpy(&done_be, resp + off, 4); off += 4;
    if (done != NULL) { *done = be32toh(done_be); }

    uint16_t pl_be;
    memcpy(&pl_be, resp + off, 2); off += 2;
    uint16_t plen_m = be16toh(pl_be);

    if (off + plen_m > resp_len || plen_m >= MDS_MAX_PATH) {
        free(resp);
        return MDS_ERR_IO;
    }
    if (path != NULL && path_cap > 0) {
        size_t copy = (plen_m < path_cap - 1) ? plen_m : path_cap - 1;
        memcpy(path, resp + off, copy);
        path[copy] = '\0';
    }

    free(resp);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Quota wire protocol handlers (S4.5)
 *
 * Payload formats:
 *   QUOTA_SET:     [scope_type u8][scope_id u64 BE][hard_bytes u64 BE]
 *                  [soft_bytes u64 BE][hard_inodes u64 BE]
 *                  [soft_inodes u64 BE][grace_sec u32 BE] = 41 bytes
 *   QUOTA_GET_REQ: [scope_type u8][scope_id u64 BE] = 9 bytes
 *   QUOTA_GET_RESP:[status u8][hard_bytes u64 BE][soft_bytes u64 BE]
 *                  [hard_inodes u64 BE][soft_inodes u64 BE]
 *                  [grace_sec u32 BE][used_bytes u64 BE]
 *                  [used_inodes u64 BE] = 57 bytes
 *   QUOTA_DEL:     [scope_type u8][scope_id u64 BE] = 9 bytes
 * ----------------------------------------------------------------------- */

static void handle_quota_set(struct cluster_server *srv, int conn_fd,
                              const uint8_t *payload, uint32_t plen)
{
    enum mds_status st = MDS_ERR_INVAL;

    if (srv->quota == NULL || plen < 45) {
        goto respond;
    }

    {
        uint8_t stype = payload[0];
        uint64_t sid;
        memcpy(&sid, payload + 1, 8);
        sid = be64toh(sid);

        struct mds_quota_rule rule;
        memset(&rule, 0, sizeof(rule));

        uint64_t be64;
        uint32_t be32;
        memcpy(&be64, payload + 9, 8);  rule.hard_bytes  = be64toh(be64);
        memcpy(&be64, payload + 17, 8); rule.soft_bytes  = be64toh(be64);
        memcpy(&be64, payload + 25, 8); rule.hard_inodes = be64toh(be64);
        memcpy(&be64, payload + 33, 8); rule.soft_inodes = be64toh(be64);
        memcpy(&be32, payload + 41, 4); rule.grace_sec = be32toh(be32);

        st = mds_quota_set(srv->quota, stype, sid, &rule);
    }

respond:
    {
        uint8_t resp[5];
        uint32_t be_st = htobe32((uint32_t)(int32_t)st);
        resp[0] = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);
        memcpy(resp + 1, &be_st, 4);
        (void)send_header(conn_fd, CT_MSG_QUOTA_GET_RESP, sizeof(resp));
        (void)send_all(conn_fd, resp, sizeof(resp));
    }
}

static void handle_quota_get(struct cluster_server *srv, int conn_fd,
                              const uint8_t *payload, uint32_t plen)
{
    enum mds_status st = MDS_ERR_INVAL;
    struct mds_quota_rule rule;
    struct mds_quota_usage usage;

    memset(&rule, 0, sizeof(rule));
    memset(&usage, 0, sizeof(usage));

    if (srv->quota == NULL || plen < 9) {
        goto respond;
    }

    {
        uint8_t stype = payload[0];
        uint64_t sid;
        memcpy(&sid, payload + 1, 8);
        sid = be64toh(sid);

        st = mds_quota_get(srv->quota, stype, sid, &rule, &usage);
    }

respond:
    {
        uint8_t resp[57];
        memset(resp, 0, sizeof(resp));
        resp[0] = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);

        uint64_t be64;
        uint32_t be32;
        be64 = htobe64(rule.hard_bytes);  memcpy(resp + 1, &be64, 8);
        be64 = htobe64(rule.soft_bytes);  memcpy(resp + 9, &be64, 8);
        be64 = htobe64(rule.hard_inodes); memcpy(resp + 17, &be64, 8);
        be64 = htobe64(rule.soft_inodes); memcpy(resp + 25, &be64, 8);
        be32 = htobe32(rule.grace_sec);   memcpy(resp + 33, &be32, 4);
        be64 = htobe64(usage.used_bytes);  memcpy(resp + 37, &be64, 8);
        be64 = htobe64(usage.used_inodes); memcpy(resp + 45, &be64, 8);

        (void)send_header(conn_fd, CT_MSG_QUOTA_GET_RESP, sizeof(resp));
        (void)send_all(conn_fd, resp, sizeof(resp));
    }
}

static void handle_quota_del(struct cluster_server *srv, int conn_fd,
                              const uint8_t *payload, uint32_t plen)
{
    enum mds_status st = MDS_ERR_INVAL;

    if (srv->quota == NULL || plen < 9) {
        goto respond;
    }

    {
        uint8_t stype = payload[0];
        uint64_t sid;
        memcpy(&sid, payload + 1, 8);
        sid = be64toh(sid);

        st = mds_quota_del(srv->quota, stype, sid);
    }

respond:
    {
        uint8_t resp[5];
        uint32_t be_st = htobe32((uint32_t)(int32_t)st);
        resp[0] = (st == MDS_OK) ? 0 : (uint8_t)(-(int)st);
        memcpy(resp + 1, &be_st, 4);
        (void)send_header(conn_fd, CT_MSG_QUOTA_GET_RESP, sizeof(resp));
        (void)send_all(conn_fd, resp, sizeof(resp));
    }
}

/* ---- Setter ---- */

void cluster_transport_server_set_quota(struct cluster_server *srv,
                                        struct mds_quota_ctx *quota)
{
    if (srv != NULL) {
        srv->quota = quota;
    }
}

void cluster_transport_server_set_failover(struct cluster_server *srv,
                                           struct failover_ctx *fo,
                                           struct repl_conn *repl)
{
    if (srv == NULL) {
        return;
    }
    srv->fo_ctx  = fo;
    srv->fo_repl = repl;
}

void cluster_transport_server_set_cq(struct cluster_server *srv,
                                     struct commit_queue *cq)
{
    if (srv != NULL) {
        srv->cq = cq;
    }
}

void cluster_transport_server_set_proxy(struct cluster_server *srv,
                                        struct mds_proxy_ctx *proxy,
                                        const char *mount_path_fmt)
{
    if (srv != NULL) {
        srv->proxy = proxy;
        if (mount_path_fmt != NULL) {
            (void)snprintf(srv->ds_mount_path_fmt,
                     sizeof(srv->ds_mount_path_fmt),
                     "%s", mount_path_fmt);
        }
    }
}

void cluster_transport_server_set_ds_cache(struct cluster_server *srv,
                                           struct ds_cache *cache)
{
    if (srv != NULL) {
        srv->ds_cache = cache;
    }
}

/* -----------------------------------------------------------------------
 * Rolling upgrade client request functions (Item 46)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_failover(
    const char *mds_host, uint16_t mds_port,
    uint32_t target_mds_id)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    /* Send request with target_mds_id (4 bytes BE). */
    uint32_t id_be = htobe32(target_mds_id);
    if (send_header(fd, CT_MSG_FAILOVER_REQ, 4) != 0 ||
        send_all(fd, &id_be, 4) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_FAILOVER_RESP || resp_len < 4) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t rbuf[4];
    if (recv_all(fd, rbuf, 4) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint32_t be_st;
    memcpy(&be_st, rbuf, 4);
    uint32_t status = be32toh(be_st);

    close(fd);
    return (enum mds_status)status;
}

enum mds_status cluster_transport_request_upgrade_status(
    const char *mds_host, uint16_t mds_port,
    struct upgrade_status_entry **out, uint32_t *count)
{
    if (out == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }
    *out = NULL;
    *count = 0;

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    /* Send request with empty payload. */
    if (send_header(fd, CT_MSG_UPGRADE_STATUS_REQ, 0) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_UPGRADE_STATUS_RESP || resp_len < 4) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t *payload = malloc(resp_len);
    if (payload == NULL) {
        close(fd);
        return MDS_ERR_NOMEM;
    }
    if (recv_all(fd, payload, resp_len) != 0) {
        free(payload);
        close(fd);
        return MDS_ERR_IO;
    }
    close(fd);

    uint32_t be_cnt;
    memcpy(&be_cnt, payload, 4);
    uint32_t n = be32toh(be_cnt);

    /* Validate: n must be sane and payload must hold exactly
     * 4-byte header + n * 42 bytes of node entries. */
    if (n > MDS_MAX_NODES || (resp_len - 4) / 42 < n) {
        free(payload);
        return MDS_ERR_IO;
    }

    struct upgrade_status_entry *arr = NULL;
    if (n > 0) {
        arr = calloc(n, sizeof(*arr));
        if (arr == NULL) {
            free(payload);
            return MDS_ERR_NOMEM;
        }
        uint32_t off = 4;
        for (uint32_t i = 0; i < n; i++) {
            uint32_t be_val;
            memcpy(&be_val, payload + off, 4);
            arr[i].mds_id = be32toh(be_val);
            off += 4;
            memcpy(&be_val, payload + off, 4);
            arr[i].wire_compat_version = be32toh(be_val);
            off += 4;
            memcpy(arr[i].software_version, payload + off, 32);
            arr[i].software_version[31] = '\0';
            off += 32;
            arr[i].role = payload[off++];
            arr[i].lifecycle = payload[off++];
        }
    }

    free(payload);
    *out = arr;
    *count = n;
    return MDS_OK;
}

/* ---- Client request functions ---- */

enum mds_status cluster_transport_request_quota_set(
    const char *mds_host, uint16_t mds_port,
    uint8_t scope_type, uint64_t scope_id,
    const struct mds_quota_rule *rule)
{
    if (rule == NULL) {
        return MDS_ERR_INVAL;
    }

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    uint8_t payload[45];
    uint64_t be64;
    uint32_t be32;

    payload[0] = scope_type;
    be64 = htobe64(scope_id);  memcpy(payload + 1, &be64, 8);
    be64 = htobe64(rule->hard_bytes);  memcpy(payload + 9, &be64, 8);
    be64 = htobe64(rule->soft_bytes);  memcpy(payload + 17, &be64, 8);
    be64 = htobe64(rule->hard_inodes); memcpy(payload + 25, &be64, 8);
    be64 = htobe64(rule->soft_inodes); memcpy(payload + 33, &be64, 8);
    be32 = htobe32(rule->grace_sec);   memcpy(payload + 41, &be32, 4);

    if (send_header(fd, CT_MSG_QUOTA_SET, 45) != 0 ||
        send_all(fd, payload, 45) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_QUOTA_GET_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }
    /* Drain remaining. */
    if (resp_len > 1) {
        uint8_t drain[256];
        uint32_t left = resp_len - 1;
        while (left > 0) {
            uint32_t chunk = (left > sizeof(drain)) ? sizeof(drain) : left;
            if (recv_all(fd, drain, chunk) != 0) {
                break;
            }
            left -= chunk;
        }
    }

    close(fd);
    return (enum mds_status)(-(int)status);
}

enum mds_status cluster_transport_request_quota_get(
    const char *mds_host, uint16_t mds_port,
    uint8_t scope_type, uint64_t scope_id,
    struct mds_quota_rule *rule, struct mds_quota_usage *usage)
{
    if (rule == NULL || usage == NULL) {
        return MDS_ERR_INVAL;
    }

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    uint8_t payload[9];
    uint64_t be64 = htobe64(scope_id);
    payload[0] = scope_type;
    memcpy(payload + 1, &be64, 8);

    if (send_header(fd, CT_MSG_QUOTA_GET_REQ, 9) != 0 ||
        send_all(fd, payload, 9) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_QUOTA_GET_RESP || resp_len < 57) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp[57];
    if (recv_all(fd, resp, 57) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }
    close(fd);

    uint8_t status = resp[0];
    if (status != 0) {
    /* NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange) */
        return (enum mds_status)(-(int)status);
    }

    memcpy(&be64, resp + 1, 8);  rule->hard_bytes  = be64toh(be64);
    memcpy(&be64, resp + 9, 8);  rule->soft_bytes  = be64toh(be64);
    memcpy(&be64, resp + 17, 8); rule->hard_inodes = be64toh(be64);
    memcpy(&be64, resp + 25, 8); rule->soft_inodes = be64toh(be64);
    uint32_t be32;
    memcpy(&be32, resp + 33, 4); rule->grace_sec   = be32toh(be32);
    memcpy(&be64, resp + 37, 8); usage->used_bytes  = be64toh(be64);
    memcpy(&be64, resp + 45, 8); usage->used_inodes = be64toh(be64);

    return MDS_OK;
}

enum mds_status cluster_transport_request_quota_del(
    const char *mds_host, uint16_t mds_port,
    uint8_t scope_type, uint64_t scope_id)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    uint8_t payload[9];
    uint64_t be64 = htobe64(scope_id);
    payload[0] = scope_type;
    memcpy(payload + 1, &be64, 8);

    if (send_header(fd, CT_MSG_QUOTA_DEL, 9) != 0 ||
        send_all(fd, payload, 9) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_QUOTA_GET_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }
    if (resp_len > 1) {
        uint8_t drain[256];
        uint32_t left = resp_len - 1;
        while (left > 0) {
            uint32_t chunk = (left > sizeof(drain)) ? sizeof(drain) : left;
            if (recv_all(fd, drain, chunk) != 0) {
                break;
            }
            left -= chunk;
        }
    }

    close(fd);
    return (enum mds_status)(-(int)status);
}

/* -----------------------------------------------------------------------
 * DS registry list admin client request (Item 47)
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status cluster_transport_request_ds_list(
    const char *mds_host, uint16_t mds_port,
    struct mds_ds_info **out, uint32_t *count)
{
    if (out == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    /* Empty payload request. */
    if (send_header(fd, CT_MSG_DS_LIST_REQ, 0) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_DS_LIST_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t *resp = malloc(resp_len);
    if (resp == NULL) {
        close(fd);
        return MDS_ERR_NOMEM;
    }
    if (recv_all(fd, resp, resp_len) != 0) {
        free(resp);
        close(fd);
        return MDS_ERR_IO;
    }
    close(fd);

    if (resp[0] != 0) {
        enum mds_status st = decode_wire_status(resp[0]);
        free(resp);
        return st;
    }


    /* Detect wire version: V2 has version byte after status. */
    uint8_t wire_ver = 1;
    size_t hdr_off = 1;  /* past status */
    if (resp_len >= 6 && resp[1] == 2) {
        wire_ver = 2;
        hdr_off = 2;  /* past status + version */
    }

    if (resp_len < hdr_off + 4) {
        free(resp);
        return MDS_ERR_IO;
    }

    uint32_t cnt_be;
    memcpy(&cnt_be, resp + hdr_off, 4);
    uint32_t cnt = be32toh(cnt_be);

    if (cnt > MDS_MAX_DS_NODES) {
        free(resp);
        return MDS_ERR_IO;
    }

    struct mds_ds_info *arr = NULL;
    if (cnt > 0) {
        arr = calloc(cnt, sizeof(*arr));
        if (arr == NULL) {
            free(resp);
            return MDS_ERR_NOMEM;
        }
    }

    size_t off = hdr_off + 4;
    for (uint32_t i = 0; i < cnt; i++) {
        /* ds_id(4) + state(4) + tier(4) + total(8) + used(8) + port(2) + addr_len(2) */
        if (off + 32 > resp_len) {
            free(arr);
            free(resp);
            return MDS_ERR_IO;
        }

        uint32_t id_be;
        memcpy(&id_be, resp + off, 4); off += 4;
        arr[i].ds_id = be32toh(id_be);

        uint32_t state_be;
        memcpy(&state_be, resp + off, 4); off += 4;
        arr[i].state = be32toh(state_be);

        uint32_t tier_be;
        memcpy(&tier_be, resp + off, 4); off += 4;
        arr[i].tier = be32toh(tier_be);

        uint64_t total_be;
        memcpy(&total_be, resp + off, 8); off += 8;
        arr[i].total_bytes = be64toh(total_be);

        uint64_t used_be;
        memcpy(&used_be, resp + off, 8); off += 8;
        arr[i].used_bytes = be64toh(used_be);

        uint16_t port_be;
        memcpy(&port_be, resp + off, 2); off += 2;
        arr[i].port = be16toh(port_be);

        uint16_t al_be;
        memcpy(&al_be, resp + off, 2); off += 2;
        uint16_t alen = be16toh(al_be);

        if (alen >= MDS_DS_ADDR_MAX || off + alen > resp_len) {
            free(arr);
            free(resp);
            return MDS_ERR_IO;
        }
        memcpy(arr[i].addr, resp + off, alen);
        arr[i].addr[alen] = '\0';
        off += alen;

        /* V2 structured fields -- only present when wire_ver >= 2. */
        if (wire_ver >= 2 && off + 2 <= resp_len) {
            arr[i].mode = resp[off++];
            arr[i].transport = resp[off++];

            if (off + 2 <= resp_len) {
                uint16_t hl_be;
                memcpy(&hl_be, resp + off, 2); off += 2;
                uint16_t hlen_v = be16toh(hl_be);
                if (hlen_v < MDS_DS_HOST_MAX && off + hlen_v <= resp_len) {
                    memcpy(arr[i].host, resp + off, hlen_v);
                    arr[i].host[hlen_v] = '\0';
                    off += hlen_v;
                }
            }
            if (off + 2 <= resp_len) {
                uint16_t el_be;
                memcpy(&el_be, resp + off, 2); off += 2;
                uint16_t elen_v = be16toh(el_be);
                if (elen_v < MDS_DS_EXPORT_MAX && off + elen_v <= resp_len) {
                    memcpy(arr[i].export_path, resp + off, elen_v);
                    arr[i].export_path[elen_v] = '\0';
                    off += elen_v;
                }
            }
        }
    }

    *out = arr;
    *count = cnt;
    free(resp);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * DS add client request (Item 47b)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_ds_add(
    const char *mds_host, uint16_t mds_port,
    const struct mds_ds_info *info)
{
    if (info == NULL) {
        return MDS_ERR_INVAL;
    }

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    /* ds_id(4) tier(4) total(8) used(8) port(2) addr_len(2) addr */
    size_t alen = strnlen(info->addr, MDS_DS_ADDR_MAX - 1);
    uint32_t plen = 0; (void)plen; /* set after V2 append */
    uint8_t buf[28 + MDS_DS_ADDR_MAX + 6 + MDS_DS_HOST_MAX +
               MDS_DS_EXPORT_MAX];
    size_t off = 0;

    uint32_t id_be = htobe32(info->ds_id);
    memcpy(buf + off, &id_be, 4); off += 4;
    uint32_t tier_be = htobe32(info->tier);
    memcpy(buf + off, &tier_be, 4); off += 4;
    uint64_t total_be = htobe64(info->total_bytes);
    memcpy(buf + off, &total_be, 8); off += 8;
    uint64_t used_be = htobe64(info->used_bytes);
    memcpy(buf + off, &used_be, 8); off += 8;
    uint16_t port_be = htobe16(info->port);
    memcpy(buf + off, &port_be, 2); off += 2;
    uint16_t al_be = htobe16((uint16_t)alen);
    memcpy(buf + off, &al_be, 2); off += 2;
    memcpy(buf + off, info->addr, alen); off += alen;

    /* V2 structured fields. */
    buf[off++] = info->mode;
    buf[off++] = info->transport;
    {
        size_t hlen = strnlen(info->host, MDS_DS_HOST_MAX - 1);
        uint16_t hl_be = htobe16((uint16_t)hlen);
        memcpy(buf + off, &hl_be, 2); off += 2;
        memcpy(buf + off, info->host, hlen); off += hlen;

        size_t elen = strnlen(info->export_path, MDS_DS_EXPORT_MAX - 1);
        uint16_t el_be = htobe16((uint16_t)elen);
        memcpy(buf + off, &el_be, 2); off += 2;
        memcpy(buf + off, info->export_path, elen); off += elen;
    }
    /* V3: per-transport ports. */
    {
        uint16_t tp_be = htobe16(info->tcp_port);
        uint16_t rp_be = htobe16(info->rdma_port);
        memcpy(buf + off, &tp_be, 2); off += 2;
        memcpy(buf + off, &rp_be, 2); off += 2;
    }
    plen = (uint32_t)off;

    if (send_header(fd, CT_MSG_DS_ADD_REQ, plen) != 0 ||
        send_all(fd, buf, off) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_DS_ADD_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }
    close(fd);
    return decode_wire_status(status);
}

/* -----------------------------------------------------------------------
 * DS set-state client request (Item 47b)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_ds_set_state(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id, uint32_t new_state)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    uint8_t buf[8];
    uint32_t id_be = htobe32(ds_id);
    uint32_t st_be = htobe32(new_state);
    memcpy(buf, &id_be, 4);
    memcpy(buf + 4, &st_be, 4);

    if (send_header(fd, CT_MSG_DS_SET_STATE_REQ, 8) != 0 ||
        send_all(fd, buf, 8) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_DS_SET_STATE_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }
    close(fd);
    return decode_wire_status(status);
}

/* -----------------------------------------------------------------------
 * DS remove client request (Item 47b)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_ds_remove(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    uint32_t id_be = htobe32(ds_id);
    if (send_header(fd, CT_MSG_DS_REMOVE_REQ, 4) != 0 ||
        send_all(fd, &id_be, 4) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_DS_REMOVE_RESP || resp_len < 1) {
        close(fd);
        return MDS_ERR_IO;
    }

    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd);
        return MDS_ERR_IO;
    }
    close(fd);
    return decode_wire_status(status);
}

/* -----------------------------------------------------------------------
 * DS validate client (Phase 6)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_ds_validate(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id, const char *tool_version, const char *note)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) { return MDS_ERR_IO; }

    uint8_t buf[4 + 2 + 64 + 2 + 256];
    size_t off = 0;
    uint32_t id_be = htobe32(ds_id);
    memcpy(buf + off, &id_be, 4); off += 4;

    size_t tl = tool_version ? strnlen(tool_version, 63) : 0;
    uint16_t tl16_be = htobe16((uint16_t)tl);
    memcpy(buf + off, &tl16_be, 2); off += 2;
    if (tl > 0) { memcpy(buf + off, tool_version, tl); off += tl; }

    size_t nl = note ? strnlen(note, 255) : 0;
    uint16_t nl16_be = htobe16((uint16_t)nl);
    memcpy(buf + off, &nl16_be, 2); off += 2;
    if (nl > 0) { memcpy(buf + off, note, nl); off += nl; }

    if (send_header(fd, CT_MSG_DS_VALIDATE_REQ, (uint32_t)off) != 0 ||
        send_all(fd, buf, off) != 0) {
        close(fd); return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_DS_VALIDATE_RESP || resp_len < 1) {
        close(fd); return MDS_ERR_IO;
    }
    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) { close(fd); return MDS_ERR_IO; }
    close(fd);
    return decode_wire_status(status);
}

enum mds_status cluster_transport_request_ds_validate_clear(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) { return MDS_ERR_IO; }

    uint32_t id_be = htobe32(ds_id);
    if (send_header(fd, CT_MSG_DS_VALIDATE_CLR_REQ, 4) != 0 ||
        send_all(fd, &id_be, 4) != 0) {
        close(fd); return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_DS_VALIDATE_CLR_RESP || resp_len < 1) {
        close(fd); return MDS_ERR_IO;
    }
    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) { close(fd); return MDS_ERR_IO; }
    close(fd);
    return decode_wire_status(status);
}

/* -----------------------------------------------------------------------
 * DS provision client (Phase 5)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_ds_provision(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id, const uint8_t *secret, uint32_t secret_len,
    uint64_t epoch)
{
    if (secret == NULL || secret_len != 32) {
        return MDS_ERR_INVAL;
    }

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) { return MDS_ERR_IO; }

    uint8_t buf[44];
    uint32_t id_be = htobe32(ds_id);
    memcpy(buf, &id_be, 4);
    memcpy(buf + 4, secret, 32);
    uint64_t ep_be = htobe64(epoch);
    memcpy(buf + 36, &ep_be, 8);

    if (send_header(fd, CT_MSG_DS_PROVISION_REQ, 44) != 0 ||
        send_all(fd, buf, 44) != 0) {
        close(fd); return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_DS_PROVISION_RESP || resp_len < 1) {
        close(fd); return MDS_ERR_IO;
    }
    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) { close(fd); return MDS_ERR_IO; }
    close(fd);
    return decode_wire_status(status);
}

/* -----------------------------------------------------------------------
 * DS patch-ready/clear client (Phase 7)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_transport_request_ds_patch_ready(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id, uint32_t bits)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) { return MDS_ERR_IO; }

    uint8_t buf[8];
    uint32_t id_be = htobe32(ds_id);
    uint32_t bits_be = htobe32(bits);
    memcpy(buf, &id_be, 4);
    memcpy(buf + 4, &bits_be, 4);

    if (send_header(fd, CT_MSG_DS_PATCH_READY_REQ, 8) != 0 ||
        send_all(fd, buf, 8) != 0) {
        close(fd); return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_DS_PATCH_READY_RESP || resp_len < 1) {
        close(fd); return MDS_ERR_IO;
    }
    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) { close(fd); return MDS_ERR_IO; }
    close(fd);
    return decode_wire_status(status);
}

enum mds_status cluster_transport_request_ds_patch_clear(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) { return MDS_ERR_IO; }

    uint32_t id_be = htobe32(ds_id);
    if (send_header(fd, CT_MSG_DS_PATCH_CLEAR_REQ, 4) != 0 ||
        send_all(fd, &id_be, 4) != 0) {
        close(fd); return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_DS_PATCH_CLEAR_RESP || resp_len < 1) {
        close(fd); return MDS_ERR_IO;
    }
    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) { close(fd); return MDS_ERR_IO; }
    close(fd);
    return decode_wire_status(status);
}

/* -----------------------------------------------------------------------
 * Cross-subtree nlink management (hard links across MDS boundaries)
 *
 * Option C: fully distributed inode reference table.
 * Each MDS tracks its local dirent count per remote fileid.
 * nlink_inc/dec adjust the authoritative nlink on the inode-owning MDS.
 * ----------------------------------------------------------------------- */

static enum mds_status nlink_request(
    const struct cluster_membership *membership,
    uint32_t target_mds, uint64_t fileid, int32_t delta)
{
    char host[256];
    uint16_t port = 0;
    enum mds_status st;
    int fd;

    st = cluster_membership_resolve_peer(membership, target_mds,
                                          host, sizeof(host), &port);
    if (st != MDS_OK) { return st; }

    fd = ct_client_connect(host, port);
    if (fd < 0) { return MDS_ERR_IO; }

    uint8_t msg_type = (delta > 0) ? CT_MSG_NLINK_INC_REQ
                                    : CT_MSG_NLINK_DEC_REQ;
    uint8_t exp_resp = (delta > 0) ? CT_MSG_NLINK_INC_RESP
                                    : CT_MSG_NLINK_DEC_RESP;

    /* Wire: fileid(8 BE) + delta(4 BE). */
    {
        uint64_t fid_be = htobe64(fileid);
        uint32_t d_be = htobe32((uint32_t)delta);

        if (send_header(fd, msg_type, 12) != 0 ||
            send_all(fd, &fid_be, 8) != 0 ||
            send_all(fd, &d_be, 4) != 0) {
            close(fd);
            return MDS_ERR_IO;
        }
    }

    /* Read response. */
    {
        uint8_t rt;
        uint32_t rlen;
        uint32_t remote_status;

        if (recv_header(fd, &rt, &rlen) != 0 || rt != exp_resp) {
            close(fd);
            return MDS_ERR_IO;
        }
        if (rlen < 4 || recv_all(fd, &remote_status, 4) != 0) {
            close(fd);
            return MDS_ERR_IO;
        }
        remote_status = be32toh(remote_status);
        close(fd);
        return (enum mds_status)remote_status;
    }
}

enum mds_status cluster_transport_request_nlink_inc(
    const struct cluster_membership *membership,
    uint32_t target_mds, uint64_t fileid)
{
    return nlink_request(membership, target_mds, fileid, 1);
}

enum mds_status cluster_transport_request_nlink_dec(
    const struct cluster_membership *membership,
    uint32_t target_mds, uint64_t fileid)
{
    return nlink_request(membership, target_mds, fileid, -1);
}

/*
 * Server side of nlink_request().
 *
 * Request wire format (must match the client sender above):
 *   [fileid u64 BE][delta i32 BE]   (12 bytes)
 * Response: matching *_RESP carrying [status i32 BE] (mds_status),
 * which the client casts back to enum mds_status.
 */
static void handle_nlink_adjust(const struct cluster_server *srv,
                                int conn_fd, uint8_t resp_type,
                                int32_t expect_delta,
                                const uint8_t *payload, uint32_t plen)
{
    enum mds_status st = MDS_ERR_INVAL;

    /* Validate untrusted payload length before parsing. */
    if (srv->cat != NULL && payload != NULL && plen >= 12) {
        uint64_t fid_be;
        uint32_t d_be;

        memcpy(&fid_be, payload, 8);
        memcpy(&d_be, payload + 8, 4);
        uint64_t fileid = be64toh(fid_be);
        int32_t delta = (int32_t)be32toh(d_be);

        /* The client only ever sends +1 (INC) or -1 (DEC); reject any
         * other delta so a malformed/hostile peer cannot skew the
         * authoritative nlink count by arbitrary amounts. */
        if (delta == expect_delta) {
            st = mds_cat_ns_nlink_adjust(srv->cat, fileid, delta);
        }
    }

    int32_t st_be = (int32_t)htobe32((uint32_t)(int32_t)st);
    send_header(conn_fd, resp_type, 4);
    send_all(conn_fd, &st_be, 4);
}

static void handle_nlink_inc(const struct cluster_server *srv, int conn_fd,
                             const uint8_t *payload, uint32_t plen)
{
    handle_nlink_adjust(srv, conn_fd, CT_MSG_NLINK_INC_RESP, 1,
                        payload, plen);
}

static void handle_nlink_dec(const struct cluster_server *srv, int conn_fd,
                             const uint8_t *payload, uint32_t plen)
{
    handle_nlink_adjust(srv, conn_fd, CT_MSG_NLINK_DEC_RESP, -1,
                        payload, plen);
}

/* -----------------------------------------------------------------------
 * Split evaluator admin handlers (Tier 3 Phase 1)
 * ----------------------------------------------------------------------- */

static void handle_split_proposals(const struct cluster_server *srv,
                                   int conn_fd)
{
    struct split_proposal *arr = NULL;
    uint32_t count = 0;

    if (srv->evaluator == NULL) {
        send_status_resp(conn_fd, CT_MSG_SPLIT_PROPOSALS_RESP, MDS_OK);
        return;
    }
    if (split_evaluator_list_proposals(srv->evaluator, &arr, &count) != 0) {
        send_status_resp(conn_fd, CT_MSG_SPLIT_PROPOSALS_RESP, MDS_ERR_IO);
        return;
    }
    uint32_t per_prop = MDS_MAX_PATH + 8 + 8 + 4 + 4 + 4 + 8 + SPLIT_REJECT_REASON_MAX;
    uint32_t total = 8 + count * per_prop;
    uint8_t *buf = calloc(1, total);
    if (buf == NULL) {
        free(arr);
        send_status_resp(conn_fd, CT_MSG_SPLIT_PROPOSALS_RESP, MDS_ERR_NOMEM);
        return;
    }
    { uint32_t v = htobe32((uint32_t)MDS_OK); memcpy(buf, &v, 4); }
    { uint32_t v = htobe32(count); memcpy(buf + 4, &v, 4); }
    uint32_t off = 8;
    for (uint32_t i = 0; i < count; i++) {
        memcpy(buf + off, arr[i].path, MDS_MAX_PATH); off += MDS_MAX_PATH;
        { uint64_t v = htobe64(arr[i].sampled_ops); memcpy(buf + off, &v, 8); off += 8; }
        { uint64_t v = htobe64(arr[i].first_seen_sec); memcpy(buf + off, &v, 8); off += 8; }
        { uint32_t v = htobe32(arr[i].hot_intervals); memcpy(buf + off, &v, 4); off += 4; }
        { uint32_t v = htobe32(arr[i].target_mds_id); memcpy(buf + off, &v, 4); off += 4; }
        { uint32_t v = htobe32((uint32_t)arr[i].state); memcpy(buf + off, &v, 4); off += 4; }
        { uint64_t v = htobe64(arr[i].cooldown_expiry); memcpy(buf + off, &v, 8); off += 8; }
        memcpy(buf + off, arr[i].reject_reason,
               SPLIT_REJECT_REASON_MAX);
        off += SPLIT_REJECT_REASON_MAX;
    }
    (void)send_header(conn_fd, CT_MSG_SPLIT_PROPOSALS_RESP, off);
    (void)send_all(conn_fd, buf, off);
    free(buf); free(arr);
}

static void handle_split_approve(const struct cluster_server *srv,
                                 int conn_fd,
                                 const uint8_t *payload, uint32_t plen)
{
    if (srv->evaluator == NULL || plen < 1) {
        send_status_resp(conn_fd, CT_MSG_SPLIT_APPROVE_RESP, MDS_ERR_INVAL);
        return;
    }
    char path[MDS_MAX_PATH];
    uint32_t clen = plen < sizeof(path) ? plen : sizeof(path) - 1;
    memcpy(path, payload, clen);
    path[clen] = '\0';
    int rc = split_evaluator_approve(srv->evaluator, path);
    send_status_resp(conn_fd, CT_MSG_SPLIT_APPROVE_RESP,
                     (rc == 0) ? MDS_OK : MDS_ERR_NOTFOUND);
}

static void handle_split_auto(const struct cluster_server *srv,
                              int conn_fd,
                              const uint8_t *payload, uint32_t plen)
{
    if (srv->evaluator == NULL || plen < 1) {
        send_status_resp(conn_fd, CT_MSG_SPLIT_AUTO_RESP, MDS_ERR_INVAL);
        return;
    }
    split_evaluator_set_auto_execute(srv->evaluator, payload[0] != 0);
    send_status_resp(conn_fd, CT_MSG_SPLIT_AUTO_RESP, MDS_OK);
}

void cluster_transport_server_set_evaluator(struct cluster_server *srv,
                                            struct split_evaluator *eval)
{
    if (srv != NULL) { srv->evaluator = eval; }
}

void cluster_transport_server_set_sharding(struct cluster_server *srv,
                                           bool enabled)
{
    if (srv != NULL) {
        srv->sharding_enabled = enabled;
    }
}

/* -----------------------------------------------------------------------
 * Split evaluator client requests (Tier 3 Phase 1)
 * ----------------------------------------------------------------------- */

static int admin_connect(const char *host, uint16_t port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;

    if (fd < 0) { return -1; }
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        struct addrinfo hints = {0}, *res = NULL;
        hints.ai_family = AF_INET;
        if (getaddrinfo(host, NULL, &hints, &res) != 0 || res == NULL) {
            close(fd); return -1;
        }
        memcpy(&addr.sin_addr,
               &((struct sockaddr_in *)res->ai_addr)->sin_addr,
               sizeof(addr.sin_addr));
        freeaddrinfo(res);
    }
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd); return -1;
    }
    { int flag = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)); }
    return fd;
}

enum mds_status cluster_transport_request_split_proposals(
    const char *mds_host, uint16_t mds_port,
    struct split_proposal **out, uint32_t *count)
{
    int fd;
    uint8_t *resp = NULL;
    uint8_t resp_type;
    uint32_t rlen;

    if (out == NULL || count == NULL) { return MDS_ERR_INVAL; }
    *out = NULL; *count = 0;

    fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) { return MDS_ERR_IO; }

    if (send_header(fd, CT_MSG_SPLIT_PROPOSALS_REQ, 0) != 0) {
        close(fd); return MDS_ERR_IO;
    }

    /* Read response header. */
    if (recv_header(fd, &resp_type, &rlen) != 0 ||
        resp_type != CT_MSG_SPLIT_PROPOSALS_RESP) {
        close(fd); return MDS_ERR_IO;
    }
    /* Short response (1 byte) = status-only. */
    if (rlen == 1) {
        uint8_t status;

        if (recv_all(fd, &status, 1) != 0) {
            close(fd);
            return MDS_ERR_IO;
        }
        close(fd);
        return decode_wire_status(status);
    }
    if (rlen < 8) { close(fd); return MDS_ERR_IO; }

    resp = calloc(1, rlen);
    if (resp == NULL) { close(fd); return MDS_ERR_NOMEM; }
    if (recv_all(fd, resp, rlen) != 0) {
        free(resp); close(fd); return MDS_ERR_IO;
    }
    close(fd);

    uint32_t st_be, cnt_be;
    memcpy(&st_be, resp, 4);
    memcpy(&cnt_be, resp + 4, 4);
    enum mds_status st = (enum mds_status)(int32_t)be32toh(st_be);
    uint32_t n = be32toh(cnt_be);
    if (st != MDS_OK || n == 0) { free(resp); return st; }

    uint32_t per_prop = MDS_MAX_PATH + 8 + 8 + 4 + 4 + 4 + 8 + SPLIT_REJECT_REASON_MAX;
    if (rlen < 8 + n * per_prop) { free(resp); return MDS_ERR_IO; }

    struct split_proposal *arr = calloc(n, sizeof(*arr));
    if (arr == NULL) { free(resp); return MDS_ERR_NOMEM; }

    uint32_t off = 8;
    for (uint32_t i = 0; i < n; i++) {
        memcpy(arr[i].path, resp + off, MDS_MAX_PATH); off += MDS_MAX_PATH;
        { uint64_t v; memcpy(&v, resp + off, 8); arr[i].sampled_ops = be64toh(v); off += 8; }
        { uint64_t v; memcpy(&v, resp + off, 8); arr[i].first_seen_sec = be64toh(v); off += 8; }
        { uint32_t v; memcpy(&v, resp + off, 4); arr[i].hot_intervals = be32toh(v); off += 4; }
        { uint32_t v; memcpy(&v, resp + off, 4); arr[i].target_mds_id = be32toh(v); off += 4; }
        { uint32_t v; memcpy(&v, resp + off, 4);
          arr[i].state = (enum split_proposal_state)be32toh(v);
          off += 4; }
        { uint64_t v; memcpy(&v, resp + off, 8); arr[i].cooldown_expiry = be64toh(v); off += 8; }
        memcpy(arr[i].reject_reason, resp + off,
               SPLIT_REJECT_REASON_MAX);
        off += SPLIT_REJECT_REASON_MAX;
    }
    free(resp);
    *out = arr; *count = n;
    return MDS_OK;
}

enum mds_status cluster_transport_request_split_approve(
    const char *mds_host, uint16_t mds_port, const char *path)
{
    int fd;
    uint8_t resp_type;
    uint32_t resp_len;
    uint8_t status;

    if (path == NULL) { return MDS_ERR_INVAL; }
    uint32_t plen = (uint32_t)strlen(path) + 1;

    fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) { return MDS_ERR_IO; }

    if (send_header(fd, CT_MSG_SPLIT_APPROVE_REQ, plen) != 0 ||
        send_all(fd, path, plen) != 0) {
        close(fd); return MDS_ERR_IO;
    }

    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_SPLIT_APPROVE_RESP || resp_len < 1) {
        close(fd); return MDS_ERR_IO;
    }
    if (recv_all(fd, &status, 1) != 0) {
        close(fd); return MDS_ERR_IO;
    }
    close(fd);
    return decode_wire_status(status);
}

enum mds_status cluster_transport_request_split_auto(
    const char *mds_host, uint16_t mds_port, bool enable)
{
    int fd;
    uint8_t flag = enable ? 1 : 0;
    uint8_t resp_type;
    uint32_t resp_len;
    uint8_t status;

    fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) { return MDS_ERR_IO; }

    if (send_header(fd, CT_MSG_SPLIT_AUTO_REQ, 1) != 0 ||
        send_all(fd, &flag, 1) != 0) {
        close(fd); return MDS_ERR_IO;
    }

    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_SPLIT_AUTO_RESP || resp_len < 1) {
        close(fd); return MDS_ERR_IO;
    }
    if (recv_all(fd, &status, 1) != 0) {
        close(fd); return MDS_ERR_IO;
    }
    close(fd);
    return decode_wire_status(status);
}

/* -----------------------------------------------------------------------
 * C2: Config / DS capacity introspection
 * ----------------------------------------------------------------------- */

/*
 * Render a single "key = value" line into buf (with bounds check).
 * Skips entirely when @filter is non-empty and doesn't match @key.
 * Returns bytes appended (0 on skip or overflow).
 */
static size_t render_line(char *buf, size_t cap, size_t off,
                          const char *filter, const char *key,
                          const char *fmt, ...)
__attribute__((format(printf, 6, 7)));

static size_t render_line(char *buf, size_t cap, size_t off,
                          const char *filter, const char *key,
                          const char *fmt, ...)
{
    if (filter != NULL && filter[0] != '\0' &&
        strcmp(filter, key) != 0) {
        return 0;
    }
    if (off >= cap) {
        return 0;
    }
    int n = snprintf(buf + off, cap - off, "%s = ", key);
    if (n < 0 || (size_t)n >= cap - off) {
        return 0;
    }
    off += (size_t)n;

    va_list ap;
    va_start(ap, fmt);
    /* NOLINTNEXTLINE(clang-analyzer-valist.Uninitialized) */
    n = vsnprintf(buf + off, cap - off, fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= cap - off) {
        return 0;
    }
    off += (size_t)n;
    if (off + 1 >= cap) {
        return 0;
    }
    buf[off++] = '\n';
    buf[off] = '\0';
    return off;
}

/*
 * Append @key's line via render_line() and bump `off` on success.
 * Zero-return from render_line means the line was skipped (filter
 * mismatch) or it would have overflowed; in either case we keep the
 * previous `off`.
 */
#define RENDER_KEY(k, ...) do { \
    size_t _n = render_line(buf, cap, off, filter, (k), __VA_ARGS__); \
    if (_n != 0) { off = _n; } \
} while (0)

/*
 * Per-subsystem helpers keep the top-level render function under
 * the clang-tidy cognitive-complexity threshold.  Each helper takes
 * the running offset by pointer so it can bump it via the macro and
 * the outer function stays a simple sequence of calls.
 */
static void render_cfg_identity(const struct mds_config *cfg,
                                const char *filter,
                                char *buf, size_t cap, size_t *offp)
{
    size_t off = *offp;
    RENDER_KEY("mds_id",       "%u", (unsigned)cfg->self.id);
    RENDER_KEY("hostname",     "%s", cfg->self.hostname);
    RENDER_KEY("nfs_port",     "%u", (unsigned)cfg->self.nfs_port);
    RENDER_KEY("grpc_port",    "%u", (unsigned)cfg->self.grpc_port);
    RENDER_KEY("cluster_size", "%u", (unsigned)cfg->cluster_size);
    RENDER_KEY("ds_count",     "%u", (unsigned)cfg->ds_count);
    *offp = off;
}

static void render_cfg_tuning_core(const struct mds_config *cfg,
                                   const char *filter,
                                   char *buf, size_t cap, size_t *offp)
{
    size_t off = *offp;
    RENDER_KEY("worker_threads",       "%u", (unsigned)cfg->worker_threads);
    RENDER_KEY("stripe_unit_bytes",    "%u", (unsigned)cfg->stripe_unit_bytes);
    RENDER_KEY("default_stripe_count", "%u", (unsigned)cfg->default_stripe_count);
    RENDER_KEY("default_mirror_count", "%u", (unsigned)cfg->default_mirror_count);
    *offp = off;
}

static void render_cfg_tuning_sec(const struct mds_config *cfg,
                                  const char *filter,
                                  char *buf, size_t cap, size_t *offp)
{
    size_t off = *offp;
    RENDER_KEY("lease_time_sec",       "%u", (unsigned)cfg->lease_time_sec);
    RENDER_KEY("grace_period_sec",     "%u", (unsigned)cfg->grace_period_sec);
    RENDER_KEY("prealloc_pool_size",   "%u", (unsigned)cfg->prealloc_pool_size);
    RENDER_KEY("inline_enabled", "%s",
               cfg->inline_enabled ? "true" : "false");
    RENDER_KEY("inline_max_size",      "%u", (unsigned)cfg->inline_max_size);
    *offp = off;
}

static void render_cfg_tuning(const struct mds_config *cfg,
                              const char *filter,
                              char *buf, size_t cap, size_t *offp)
{
    render_cfg_tuning_core(cfg, filter, buf, cap, offp);
    render_cfg_tuning_sec(cfg, filter, buf, cap, offp);
}

static void render_cfg_caches(const struct mds_config *cfg,
                              const char *filter,
                              char *buf, size_t cap, size_t *offp)
{
    size_t off = *offp;
    RENDER_KEY("inode_cache_size",     "%u", (unsigned)cfg->inode_cache_size);
    RENDER_KEY("dirent_cache_size",    "%u", (unsigned)cfg->dirent_cache_size);
    RENDER_KEY("negative_cache_ttl_ms", "%u",
               (unsigned)cfg->negative_cache_ttl_ms);
    *offp = off;
}

static void render_cfg_placement(const struct mds_config *cfg,
                                 const char *filter,
                                 char *buf, size_t cap, size_t *offp)
{
    size_t off = *offp;
    const char *p = "rr";
    if (cfg->placement_policy == PLACEMENT_WEIGHTED_RR) {
        p = "wrr";
    } else if (cfg->placement_policy == PLACEMENT_CAPACITY) {
        p = "capacity";
    }
    RENDER_KEY("placement_policy", "%s", p);
    RENDER_KEY("placement_policy_enabled", "%s",
               cfg->placement_policy_enabled ? "true" : "false");
    {
        const char *w =
            (cfg->placement_capacity_weighting == CAP_WEIGHT_PROPORTIONAL)
                ? "proportional" : "off";
        RENDER_KEY("placement_capacity_weighting", "%s", w);
    }
    *offp = off;
}

static void render_cfg_ds(const struct mds_config *cfg,
                          const char *filter,
                          char *buf, size_t cap, size_t *offp)
{
    size_t off = *offp;
    RENDER_KEY("ds_heartbeat_ms",        "%u", (unsigned)cfg->ds_heartbeat_ms);
    RENDER_KEY("ds_health_fail_threshold", "%u",
               (unsigned)cfg->ds_health_fail_threshold);
    RENDER_KEY("ds_capacity_poll_ms",    "%u", (unsigned)cfg->ds_capacity_poll_ms);
    RENDER_KEY("ds_mount_path_fmt",      "%s", cfg->ds_mount_path_fmt);
    for (uint32_t i = 0; i < MDS_MAX_DS_NODES; i++) {
        if (cfg->ds_weight_by_id[i] != 0) {
            char keybuf[32];
            (void)snprintf(keybuf, sizeof(keybuf), "ds_weight.%u",
                           (unsigned)i);
            RENDER_KEY(keybuf, "%u", (unsigned)cfg->ds_weight_by_id[i]);
        }
    }
    *offp = off;
}

static void render_cfg_misc_callbacks(const struct mds_config *cfg,
                                      const char *filter,
                                      char *buf, size_t cap, size_t *offp)
{
    size_t off = *offp;
    RENDER_KEY("dir_delegations_enabled", "%s",
               cfg->dir_delegations_enabled ? "true" : "false");
    RENDER_KEY("dir_deleg_recall_timeout_ms", "%u",
               (unsigned)cfg->dir_deleg_recall_timeout_ms);
    RENDER_KEY("cb_recall_timeout_ms", "%u",
               (unsigned)cfg->cb_recall_timeout_ms);
    RENDER_KEY("metrics_http_port", "%u",
               (unsigned)cfg->metrics_http_port);
    *offp = off;
}

static void render_cfg_misc_state(const struct mds_config *cfg,
                                  const char *filter,
                                  char *buf, size_t cap, size_t *offp)
{
    size_t off = *offp;
    RENDER_KEY("transient_state_cache", "%s",
               cfg->transient_state_cache ? "true" : "false");
    RENDER_KEY("shard_enabled", "%s",
               cfg->shard_enabled ? "true" : "false");
    RENDER_KEY("ndb_async_writes", "%s",
               cfg->ndb_async_writes ? "true" : "false");
    RENDER_KEY("ndb_conn_pool_size", "%u",
               (unsigned)cfg->ndb_conn_pool_size);
    *offp = off;
}

static void render_cfg_misc(const struct mds_config *cfg,
                            const char *filter,
                            char *buf, size_t cap, size_t *offp)
{
    render_cfg_misc_callbacks(cfg, filter, buf, cap, offp);
    render_cfg_misc_state(cfg, filter, buf, cap, offp);
}

/*
 * Render a compact snapshot of the live mds_config as INI-style
 * key=value lines.  Output is a single NUL-terminated UTF-8 blob
 * suitable for printing on the admin CLI.  When @filter is
 * non-empty only matching keys are emitted.
 */
static size_t render_config_show(const struct mds_config *cfg,
                                 const char *filter,
                                 char *buf, size_t cap)
{
    if (cap == 0) { return 0; }
    size_t off = 0;
    buf[0] = '\0';

    render_cfg_identity(cfg, filter, buf, cap, &off);
    render_cfg_tuning(cfg, filter, buf, cap, &off);
    render_cfg_caches(cfg, filter, buf, cap, &off);
    render_cfg_placement(cfg, filter, buf, cap, &off);
    render_cfg_ds(cfg, filter, buf, cap, &off);
    render_cfg_misc(cfg, filter, buf, cap, &off);
    return off;
}

#undef RENDER_KEY

static void handle_config_show_admin(const struct cluster_server *srv,
                                     int conn_fd,
                                     const uint8_t *payload, uint32_t plen)
{
    /* Payload is an optional UTF-8 key filter.  zero-length = all. */
    char filter[128];
    if (plen >= sizeof(filter)) {
        (void)send_header(conn_fd, CT_MSG_CONFIG_SHOW_RESP, 0);
        return;
    }
    filter[0] = '\0';
    if (plen > 0) {
        memcpy(filter, payload, plen);
        filter[plen] = '\0';
    }

    if (srv->cfg == NULL) {
        /* Daemon did not attach a config; reply with empty body so
         * the client reports "no config attached" without parsing
         * a status opcode we do not have. */
        (void)send_header(conn_fd, CT_MSG_CONFIG_SHOW_RESP, 0);
        return;
    }

    /*
     * 8 KiB holds every key currently emitted by render_config_show
     * with plenty of headroom (~32 keys at <256 bytes each).
     * Rendered text is bounded and truncation-safe.
     */
    char buf[8192];
    size_t used = render_config_show(srv->cfg, filter, buf, sizeof(buf));

    (void)send_header(conn_fd, CT_MSG_CONFIG_SHOW_RESP, (uint32_t)used);
    if (used > 0) {
        (void)send_all(conn_fd, buf, used);
    }
}

static void handle_ds_capacity_show_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen)
{
    (void)payload; (void)plen;

    /*
     * Snapshot ids under the cache rwlock, then read each entry
     * via ds_cache_get which copies the full mds_ds_info struct.
     * This avoids holding the lock across the send() syscalls.
     */
    uint32_t ids[MDS_MAX_DS_NODES];
    uint32_t n = 0;

    if (srv->ds_cache != NULL) {
        n = ds_cache_snapshot_ids(srv->ds_cache, ids,
                                  MDS_MAX_DS_NODES);
    }

    /* Record size on the wire: ds_id(4) + total(8) + used(8) +
     * weight(4) + state(4) + auto_weight(4) = 32 bytes.  Always
     * allocate the header word so `n == 0` still produces a valid
     * response.  auto_weight is the capacity-derived value stamped
     * by the statvfs probe; zero = feature off or no probe yet. */
    const uint32_t rec_size = 32;
    uint32_t total = 4 + n * rec_size;

    uint8_t *buf = malloc(total);
    if (buf == NULL) {
        (void)send_header(conn_fd, CT_MSG_DS_CAPACITY_SHOW_RESP, 0);
        return;
    }

    uint32_t cnt_be = htobe32(n);
    memcpy(buf, &cnt_be, 4);
    uint32_t off = 4;
    uint32_t written = 0;

    for (uint32_t i = 0; i < n; i++) {
        struct mds_ds_info info;
        uint32_t op_w = 0, auto_w = 0;
        if (ds_cache_get(srv->ds_cache, ids[i], &info) != MDS_OK) {
            continue;
        }
        /* Invariant: never write a record past the allocation sized
         * for `n` entries; if it would overflow, stop and let the
         * count fix-up below report only the records written. */
        if (off + rec_size > total) {
            break;
        }
        (void)ds_cache_get_weights(srv->ds_cache, ids[i],
                                   &op_w, &auto_w);
        uint32_t id_be = htobe32(info.ds_id);
        uint64_t tot_be = htobe64(info.total_bytes);
        uint64_t use_be = htobe64(info.used_bytes);
        uint32_t w_be = htobe32(op_w);
        uint32_t st_be = htobe32(info.state);
        uint32_t aw_be = htobe32(auto_w);
        memcpy(buf + off, &id_be, 4);  off += 4;
        memcpy(buf + off, &tot_be, 8); off += 8;
        memcpy(buf + off, &use_be, 8); off += 8;
        memcpy(buf + off, &w_be, 4);   off += 4;
        memcpy(buf + off, &st_be, 4);  off += 4;
        memcpy(buf + off, &aw_be, 4);  off += 4;
        written++;
    }

    /* Fix up count if some entries vanished between snapshot and
     * read (race with an admin CLI remove). */
    if (written != n) {
        uint32_t fixed_be = htobe32(written);
        memcpy(buf, &fixed_be, 4);
    }

    (void)send_header(conn_fd, CT_MSG_DS_CAPACITY_SHOW_RESP,
                      4 + written * rec_size);
    (void)send_all(conn_fd, buf, 4 + written * rec_size);
    free(buf);
}

enum mds_status cluster_transport_request_config_show(
    const char *mds_host, uint16_t mds_port,
    const char *key_filter, char **out_text)
{
    if (out_text == NULL) { return MDS_ERR_INVAL; }
    *out_text = NULL;

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) { return MDS_ERR_IO; }

    uint32_t filter_len = 0;
    if (key_filter != NULL) { filter_len = (uint32_t)strlen(key_filter); }
    if (filter_len > 127) { close(fd); return MDS_ERR_INVAL; }

    if (send_header(fd, CT_MSG_CONFIG_SHOW_REQ, filter_len) != 0) {
        close(fd); return MDS_ERR_IO;
    }
    if (filter_len > 0 &&
        send_all(fd, key_filter, filter_len) != 0) {
        close(fd); return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_CONFIG_SHOW_RESP) {
        close(fd); return MDS_ERR_IO;
    }
    if (resp_len > 1024 * 1024) { close(fd); return MDS_ERR_IO; }

    char *text = malloc((size_t)resp_len + 1);
    if (text == NULL) { close(fd); return MDS_ERR_NOMEM; }
    if (resp_len > 0 && recv_all(fd, text, resp_len) != 0) {
        free(text); close(fd); return MDS_ERR_IO;
    }
    text[resp_len] = '\0';
    close(fd);

    /* Empty response => daemon has no config attached. */
    if (resp_len == 0) {
        free(text);
        return MDS_ERR_INVAL;
    }
    *out_text = text;
    return MDS_OK;
}

static void handle_ds_set_weight_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen)
{
    /* Payload: ds_id(u32 BE) + weight(u32 BE). */
    if (plen != 8) {
        send_status_resp(conn_fd, CT_MSG_DS_SET_WEIGHT_RESP,
                         MDS_ERR_INVAL);
        return;
    }
    if (srv->ds_cache == NULL) {
        send_status_resp(conn_fd, CT_MSG_DS_SET_WEIGHT_RESP,
                         MDS_ERR_INVAL);
        return;
    }
    uint32_t id_be, w_be;
    memcpy(&id_be, payload, 4);
    memcpy(&w_be, payload + 4, 4);
    uint32_t ds_id = be32toh(id_be);
    uint32_t weight = be32toh(w_be);

    if (ds_cache_set_weight(srv->ds_cache, ds_id, weight) != 0) {
        send_status_resp(conn_fd, CT_MSG_DS_SET_WEIGHT_RESP,
                         MDS_ERR_NOTFOUND);
        return;
    }
    send_status_resp(conn_fd, CT_MSG_DS_SET_WEIGHT_RESP, MDS_OK);
}

static void handle_ds_capacity_probe_admin(
    const struct cluster_server *srv, int conn_fd,
    const uint8_t *payload, uint32_t plen)
{
    (void)payload; (void)plen;

    uint32_t probed = 0;
    if (srv->ds_cache != NULL && srv->ds_mount_path_fmt[0] != '\0') {
        /* Synchronous sweep via the public probe helper.  The
         * same helper the background ds_capacity thread calls,
         * so concurrent access is already locked appropriately.
         * Pass the live capacity-weighting mode so an admin-driven
         * probe-now stamps auto_weight the same way the background
         * loop would (and honours a flip back to OFF by clearing
         * stale auto-weights). */
        enum mds_placement_capacity_weighting mode = CAP_WEIGHT_OFF;
        if (srv->cfg != NULL) {
            mode = srv->cfg->placement_capacity_weighting;
        }
        int n = ds_capacity_probe_once(srv->ds_cache,
                                       srv->ds_mount_path_fmt,
                                       mode);
        if (n > 0) {
            probed = (uint32_t)n;
        }
    }

    /* Response: status(u8) + probed(u32). */
    uint8_t resp[5];
    resp[0] = 0; /* MDS_OK */
    uint32_t be = htobe32(probed);
    memcpy(resp + 1, &be, 4);
    (void)send_header(conn_fd, CT_MSG_DS_CAPACITY_PROBE_RESP,
                      (uint32_t)sizeof(resp));
    (void)send_all(conn_fd, resp, sizeof(resp));
}

enum mds_status cluster_transport_request_ds_set_weight(
    const char *mds_host, uint16_t mds_port,
    uint32_t ds_id, uint32_t weight)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) { return MDS_ERR_IO; }

    uint8_t payload[8];
    uint32_t be = htobe32(ds_id);
    memcpy(payload, &be, 4);
    be = htobe32(weight);
    memcpy(payload + 4, &be, 4);

    if (send_header(fd, CT_MSG_DS_SET_WEIGHT_REQ, 8) != 0 ||
        send_all(fd, payload, 8) != 0) {
        close(fd); return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_DS_SET_WEIGHT_RESP || resp_len < 1) {
        close(fd); return MDS_ERR_IO;
    }
    uint8_t status;
    if (recv_all(fd, &status, 1) != 0) {
        close(fd); return MDS_ERR_IO;
    }
    close(fd);
    return decode_wire_status(status);
}

enum mds_status cluster_transport_request_ds_capacity_probe(
    const char *mds_host, uint16_t mds_port,
    uint32_t *probed_count)
{
    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) { return MDS_ERR_IO; }

    if (send_header(fd, CT_MSG_DS_CAPACITY_PROBE_REQ, 0) != 0) {
        close(fd); return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_DS_CAPACITY_PROBE_RESP || resp_len < 5) {
        close(fd); return MDS_ERR_IO;
    }
    uint8_t resp[5];
    if (recv_all(fd, resp, 5) != 0) {
        close(fd); return MDS_ERR_IO;
    }
    close(fd);

    enum mds_status st = decode_wire_status(resp[0]);
    if (st != MDS_OK) { return st; }
    if (probed_count != NULL) {
        uint32_t be;
        memcpy(&be, resp + 1, 4);
        *probed_count = be32toh(be);
    }
    return MDS_OK;
}

enum mds_status cluster_transport_request_ds_capacity_show(
    const char *mds_host, uint16_t mds_port,
    struct ds_capacity_record **out_records, uint32_t *out_count)
{
    if (out_records == NULL || out_count == NULL) {
        return MDS_ERR_INVAL;
    }
    *out_records = NULL;
    *out_count = 0;

    int fd = ct_client_connect(mds_host, mds_port);
    if (fd < 0) { return MDS_ERR_IO; }

    if (send_header(fd, CT_MSG_DS_CAPACITY_SHOW_REQ, 0) != 0) {
        close(fd); return MDS_ERR_IO;
    }

    uint8_t resp_type;
    uint32_t resp_len;
    if (recv_header(fd, &resp_type, &resp_len) != 0 ||
        resp_type != CT_MSG_DS_CAPACITY_SHOW_RESP) {
        close(fd); return MDS_ERR_IO;
    }
    if (resp_len < 4 || resp_len > 4 + MDS_MAX_DS_NODES * 32) {
        close(fd); return MDS_ERR_IO;
    }

    uint8_t *buf = malloc(resp_len);
    if (buf == NULL) { close(fd); return MDS_ERR_NOMEM; }
    if (recv_all(fd, buf, resp_len) != 0) {
        free(buf); close(fd); return MDS_ERR_IO;
    }
    close(fd);

    uint32_t cnt_be;
    memcpy(&cnt_be, buf, 4);
    uint32_t n = be32toh(cnt_be);
    if (resp_len != 4 + n * 32 || n > MDS_MAX_DS_NODES) {
        free(buf); return MDS_ERR_IO;
    }
    if (n == 0) {
        free(buf);
        return MDS_OK;
    }

    struct ds_capacity_record *recs = calloc(n, sizeof(*recs));
    if (recs == NULL) { free(buf); return MDS_ERR_NOMEM; }
    uint32_t off = 4;
    for (uint32_t i = 0; i < n; i++) {
        uint32_t id_be, w_be, st_be, aw_be;
        uint64_t tot_be, use_be;
        memcpy(&id_be, buf + off, 4);  off += 4;
        memcpy(&tot_be, buf + off, 8); off += 8;
        memcpy(&use_be, buf + off, 8); off += 8;
        memcpy(&w_be, buf + off, 4);   off += 4;
        memcpy(&st_be, buf + off, 4);  off += 4;
        memcpy(&aw_be, buf + off, 4);  off += 4;
        recs[i].ds_id = be32toh(id_be);
        recs[i].total_bytes = be64toh(tot_be);
        recs[i].used_bytes = be64toh(use_be);
        recs[i].weight = be32toh(w_be);
        recs[i].state = be32toh(st_be);
        recs[i].auto_weight = be32toh(aw_be);
    }
    free(buf);
    *out_records = recs;
    *out_count = n;
    return MDS_OK;
}

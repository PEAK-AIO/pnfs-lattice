/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * cluster_membership.c -- MDS cluster membership table.
 *
 * Public API dispatches write operations through a backend vtable
 * (local in-memory).
 *
 * Lock contract:
 *   - apply_registry_member() is a private helper that takes the
 *     write lock internally and merges one node-registry row into the
 *     local cache.  It is called by cluster_membership_populate().
 *   - Local backend join/leave are called with write lock held
 *     (unchanged from before).
 *   - cluster_membership_populate() reads the node registry through
 *     the backend-neutral mds_cluster_node_list() dispatcher and
 *     never holds the rwlock across that call.
 *
 * Thread safety: all public API calls are internally synchronised
 * via pthread_rwlock_t.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "cluster_membership.h"
#include "subtree_map.h"
#include "mds_cluster.h"

/* -----------------------------------------------------------------------
 * Tunables
 * ----------------------------------------------------------------------- */

#define MEMBERSHIP_INIT_CAP     8

/* -----------------------------------------------------------------------
 * Backend vtable
 *
 * Local backend: called with write lock held.
 * ----------------------------------------------------------------------- */

struct membership_backend {
    enum mds_status (*join)(struct cluster_membership *ctx,
                            const struct cluster_member *member);
    enum mds_status (*leave)(struct cluster_membership *ctx,
                             uint32_t mds_id);
    void (*destroy)(struct cluster_membership *ctx);
};

/* -----------------------------------------------------------------------
 * Internal structure
 * ----------------------------------------------------------------------- */

struct cluster_membership {
    struct cluster_member *members;
    uint32_t               count;
    uint32_t               capacity;
    uint32_t               self_id;

    struct subtree_map    *smap;       /**< Retained, not owned. */

    cluster_change_cb      change_cb;
    void                  *change_arg;

    pthread_rwlock_t       lock;

    const struct membership_backend *backend;

    /* Partner-loss observer (Seq 9) */
    uint32_t               partner_loss_id;
    cluster_partner_loss_cb partner_loss_cb;
    void                  *partner_loss_arg;
};

/* -----------------------------------------------------------------------
 * Helpers (caller must hold appropriate lock)
 * ----------------------------------------------------------------------- */

static int find_member(const struct cluster_membership *ctx, uint32_t mds_id)
{
    for (uint32_t i = 0; i < ctx->count; i++) {
        if (ctx->members[i].mds_id == mds_id) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status grow_members(struct cluster_membership *ctx)
{
    if (ctx->count < ctx->capacity) {
        return MDS_OK;
    }
    uint32_t new_cap = ctx->capacity * 2;
    /* NOLINTNEXTLINE(clang-analyzer-optin.portability.UnixAPI) */
    struct cluster_member *tmp = realloc(
        ctx->members, (size_t)new_cap * sizeof(*tmp));
    if (tmp == NULL) {
        return MDS_ERR_NOMEM;
    }
    ctx->members = tmp;
    ctx->capacity = new_cap;
    return MDS_OK;
}

static uint32_t lowest_mds_id(const struct cluster_membership *ctx)
{
    if (ctx->count == 0) {
        return 0;
    }
    uint32_t low = ctx->members[0].mds_id;
    for (uint32_t i = 1; i < ctx->count; i++) {
        if (ctx->members[i].mds_id < low) {
            low = ctx->members[i].mds_id;
        }
    }
    return low;
}

/* -----------------------------------------------------------------------
 * apply_registry_member
 *
 * Merge one node-registry row into the local array under write lock.
 *
 * The registry is authoritative for a node's address only: mds_id,
 * hostname, nfs_port, grpc_port (the row carries nothing else the
 * callback exposes).  Role, lifecycle, failover partner, cluster
 * address, wire-compat version and join time are LOCAL state -- set by
 * cluster_membership_init for self from the configuration and by
 * cluster_node_join (transport JOIN) for peers -- so an existing member
 * keeps them and only its address fields are refreshed.  Replacing the
 * whole entry, as the previous populate did, stamped this node's own
 * row ACTIVE/ACTIVE_SERVING/partner 0, so a configured standby never
 * matched main.c's `role == NODE_STANDBY` check and failover never
 * armed; it likewise flipped transport-joined standby peers to ACTIVE.
 *
 * A node seen only through the registry is inserted with the defaults
 * the registry path always gave it (ACTIVE, ACTIVE_SERVING, no partner,
 * no cluster address), except wire_compat_version: the registry does
 * not carry it, and this codebase's convention for an unknown version
 * is the legacy value 1 (test_rolling_upgrade: a member record without
 * the field decodes as 1), not 0 -- with self now keeping its real
 * PNFS_MDS_WIRE_COMPAT_VERSION, a 0 here would make
 * failover_promote's compat gate refuse every promotion.
 * ----------------------------------------------------------------------- */

/* Legacy wire-compat version assumed for a peer whose record does not
 * carry one (see above). */
#define REGISTRY_WIRE_COMPAT_UNKNOWN 1U

static void apply_registry_member(struct cluster_membership *ctx,
                                  const struct cluster_member *reg)
{
    pthread_rwlock_wrlock(&ctx->lock);

    int idx = find_member(ctx, reg->mds_id);
    if (idx >= 0) {
        struct cluster_member *m = &ctx->members[idx];

        if (reg->hostname[0] != '\0') {
            memcpy(m->hostname, reg->hostname, sizeof(m->hostname));
        }
        m->nfs_port = reg->nfs_port;
        m->grpc_port = reg->grpc_port;
    } else if (grow_members(ctx) == MDS_OK) {
        struct cluster_member *m = &ctx->members[ctx->count];

        *m = *reg;
        m->role = NODE_ACTIVE;
        m->lifecycle = NODE_ACTIVE_SERVING;
        m->failover_partner_id = 0;
        m->cluster_addr[0] = '\0';
        m->wire_compat_version = REGISTRY_WIRE_COMPAT_UNKNOWN;
        m->join_time_sec = (uint64_t)time(NULL);
        ctx->count++;
    }

    pthread_rwlock_unlock(&ctx->lock);

    /* Register hostname in subtree map (outside our lock). */
    if (ctx->smap != NULL && reg->hostname[0] != '\0') {
        (void)subtree_map_register_node(ctx->smap, reg->mds_id,
                                        reg->hostname);
    }
}

/* -----------------------------------------------------------------------
 * JSON serialisation (used by tests)
 * ----------------------------------------------------------------------- */

/**
 * Escape a string for safe embedding inside a JSON string literal.
 *
 * Backslash and double quote are prefixed with a backslash; control
 * characters (< 0x20) are emitted as \u00XX.  Reads at most src_max
 * bytes from src (stopping at NUL) and always NUL-terminates dst.
 * Output that would not fit is truncated at the last whole escape
 * sequence -- never mid-sequence.
 *
 * @param dst      Destination buffer (always NUL-terminated on return).
 * @param dst_cap  Capacity of dst in bytes (must be > 0).
 * @param src      Source string (may lack NUL within src_max).
 * @param src_max  Maximum bytes to read from src.
 */
static void json_escape(char *dst, size_t dst_cap,
                        const char *src, size_t src_max)
{
    static const char hex[] = "0123456789abcdef";
    size_t out = 0;

    if (dst == NULL || dst_cap == 0) {
        return;
    }
    if (src == NULL) {
        dst[0] = '\0';
        return;
    }

    for (size_t i = 0; i < src_max && src[i] != '\0'; i++) {
        unsigned char c = (unsigned char)src[i];

        if (c == '\\' || c == '"') {
            if (out + 2 >= dst_cap) {
                break;
            }
            dst[out++] = '\\';
            dst[out++] = (char)c;
        } else if (c < 0x20) {
            if (out + 6 >= dst_cap) {
                break;
            }
            dst[out++] = '\\';
            dst[out++] = 'u';
            dst[out++] = '0';
            dst[out++] = '0';
            dst[out++] = hex[(c >> 4) & 0x0F];
            dst[out++] = hex[c & 0x0F];
        } else {
            if (out + 1 >= dst_cap) {
                break;
            }
            dst[out++] = (char)c;
        }
    }
    dst[out] = '\0';
}

int member_to_json(const struct cluster_member *m,
                           char *buf, size_t cap)
{
    /* hostname and cluster_addr originate from remote peers; escape
     * them so a crafted value cannot break out of the JSON string.
     * Worst case expansion is 6x (every byte becomes \u00XX). */
    char host_esc[sizeof(m->hostname) * 6 + 1];
    char addr_esc[sizeof(m->cluster_addr) * 6 + 1];

    if (m == NULL) {
        return -1;
    }

    json_escape(host_esc, sizeof(host_esc),
                m->hostname, sizeof(m->hostname));
    json_escape(addr_esc, sizeof(addr_esc),
                m->cluster_addr, sizeof(m->cluster_addr));

    return snprintf(buf, cap,
        "{\"hostname\":\"%s\","
        "\"nfs_port\":%" PRIu16 ","
        "\"grpc_port\":%" PRIu16 ","
        "\"role\":%d,"
        "\"lifecycle\":%d,"
        "\"partner_id\":%" PRIu32 ","
        "\"cluster_addr\":\"%s\","
        "\"wire_compat\":%" PRIu32 "}",
        host_esc, m->nfs_port, m->grpc_port,
        (int)m->role, (int)m->lifecycle,
        m->failover_partner_id, addr_esc,
        m->wire_compat_version);
}


/* -----------------------------------------------------------------------
 * Local backend (in-memory array, no persistence)
 *
 * Called with write lock held by the public API.
 * ----------------------------------------------------------------------- */

static enum mds_status local_backend_join(struct cluster_membership *ctx,
                                          const struct cluster_member *member)
{
    enum mds_status st = grow_members(ctx);
    if (st != MDS_OK) {
        return st;
    }
    ctx->members[ctx->count] = *member;
    ctx->count++;
    return MDS_OK;
}

static enum mds_status local_backend_leave(struct cluster_membership *ctx,
                                           uint32_t mds_id)
{
    int idx = find_member(ctx, mds_id);
    if (idx < 0) {
        return MDS_ERR_NOTFOUND;
    }
    if ((uint32_t)(idx + 1) < ctx->count) {
        memmove(&ctx->members[idx], &ctx->members[idx + 1],
                (ctx->count - (uint32_t)idx - 1) * sizeof(ctx->members[0]));
    }
    ctx->count--;
    return MDS_OK;
}

static void local_backend_destroy(struct cluster_membership *ctx)
{
    (void)ctx;
}

static const struct membership_backend local_membership_backend = {
    .join    = local_backend_join,
    .leave   = local_backend_leave,
    .destroy = local_backend_destroy,
};

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

enum mds_status cluster_membership_init(const struct mds_config *cfg,
                                        struct subtree_map *smap,
                                        struct mds_tls_ctx *etcd_tls,
                                        struct cluster_membership **out)
{
    (void)etcd_tls;
    if (cfg == NULL || smap == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }
    if (cfg->self.id == 0 || cfg->self.id > MDS_MAX_NODES) {
        return MDS_ERR_INVAL;
    }

    struct cluster_membership *ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        return MDS_ERR_NOMEM;
    }

    ctx->members = calloc(MEMBERSHIP_INIT_CAP, sizeof(*ctx->members));
    if (ctx->members == NULL) {
        free(ctx);
        return MDS_ERR_NOMEM;
    }
    ctx->capacity = MEMBERSHIP_INIT_CAP;
    ctx->self_id = cfg->self.id;
    ctx->smap = smap;

    if (pthread_rwlock_init(&ctx->lock, NULL) != 0) {
        free(ctx->members);
        free(ctx);
        return MDS_ERR_NOMEM;
    }

    ctx->backend = &local_membership_backend;

    /* Auto-register self. */
    struct cluster_member self_member;
    memset(&self_member, 0, sizeof(self_member));
    self_member.mds_id = cfg->self.id;
    (void)snprintf(self_member.hostname, sizeof(self_member.hostname),
             "%s", cfg->self.hostname);
    self_member.nfs_port = cfg->self.nfs_port;
    self_member.grpc_port = cfg->self.grpc_port;
    self_member.join_time_sec = (uint64_t)time(NULL);
    self_member.role = (enum node_role)cfg->self_role;
    self_member.lifecycle = (cfg->self_role == NODE_STANDBY)
                            ? NODE_IDLE : NODE_ACTIVE_SERVING;
    self_member.failover_partner_id = cfg->self_failover_partner_id;
    self_member.wire_compat_version = PNFS_MDS_WIRE_COMPAT_VERSION;
    if (cfg->cluster_bind_addr[0] != '\0') {
        (void)snprintf(self_member.cluster_addr,
                 sizeof(self_member.cluster_addr),
                 "%s", cfg->cluster_bind_addr);
    }

    ctx->members[0] = self_member;
    ctx->count = 1;

    (void)subtree_map_register_node(smap, cfg->self.id, cfg->self.hostname);

    *out = ctx;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Membership population from the catalogue's node registry
 * ----------------------------------------------------------------------- */

struct registry_populate_ctx {
    struct cluster_membership *cm;
    uint32_t upserted;
};

/* boot_epoch and last_heartbeat_ns are deliberately not interpreted:
 * liveness is the failover watchdog's business (failover_watchdog.h),
 * and the timestamp's clock domain is not this consumer's to judge. */
static int registry_member_cb(uint32_t mds_id, uint64_t boot_epoch,
                              const char *hostname,
                              uint16_t nfs_port, uint16_t grpc_port,
                              uint64_t last_heartbeat_ns, void *ctx)
{
    struct registry_populate_ctx *rc = ctx;
    (void)boot_epoch;
    (void)last_heartbeat_ns;

    struct cluster_member reg;
    memset(&reg, 0, sizeof(reg));
    reg.mds_id = mds_id;
    if (hostname != NULL) {
        (void)snprintf(reg.hostname, sizeof(reg.hostname), "%s", hostname);
    }
    reg.nfs_port = nfs_port;
    reg.grpc_port = grpc_port;

    apply_registry_member(rc->cm, &reg);
    rc->upserted++;
    return 0;
}

enum mds_status cluster_membership_populate(struct cluster_membership *ctx,
                                            struct mds_catalogue *cat)
{
    if (ctx == NULL || cat == NULL) {
        return MDS_ERR_INVAL;
    }

    struct registry_populate_ctx rc = { .cm = ctx, .upserted = 0 };
    /* The dispatcher's status passes through unchanged: a backend
     * without a node registry yields MDS_ERR_NOSUPPORT. */
    return mds_cluster_node_list(cat, registry_member_cb, &rc);
}

void cluster_membership_destroy(struct cluster_membership *ctx)
{
    if (ctx == NULL) {
        return;
    }
    if (ctx->backend != NULL) {
        ctx->backend->destroy(ctx);
    }
    pthread_rwlock_destroy(&ctx->lock);
    free(ctx->members);
    free(ctx);
}

enum mds_status cluster_node_join(struct cluster_membership *ctx,
                                  const struct cluster_member *member)
{
    if (ctx == NULL || member == NULL) {
        return MDS_ERR_INVAL;
    }
    if (member->mds_id == 0 || member->mds_id > MDS_MAX_NODES) {
        return MDS_ERR_INVAL;
    }

    /* Stamp role/lifecycle defaults. */
    struct cluster_member copy = *member;
    if (copy.role != NODE_STANDBY) {
        copy.role = NODE_ACTIVE;
    }
    if (copy.lifecycle != NODE_IDLE &&
        copy.lifecycle != NODE_JOINING) {
        copy.lifecycle = NODE_IDLE;
    }


    /* Local backend: called WITH lock */
    pthread_rwlock_wrlock(&ctx->lock);

    if (ctx->count >= MDS_MAX_NODES) {
        pthread_rwlock_unlock(&ctx->lock);
        return MDS_ERR_NOSPC;
    }

    if (find_member(ctx, member->mds_id) >= 0) {
        pthread_rwlock_unlock(&ctx->lock);
        return MDS_ERR_EXISTS;
    }

    enum mds_status st = ctx->backend->join(ctx, &copy);
    if (st != MDS_OK) {
        pthread_rwlock_unlock(&ctx->lock);
        return st;
    }

    cluster_change_cb cb = ctx->change_cb;
    void *arg = ctx->change_arg;

    pthread_rwlock_unlock(&ctx->lock);

    /* Register hostname in subtree map (outside our lock). */
    (void)subtree_map_register_node(ctx->smap, member->mds_id,
                                    member->hostname);

    if (cb != NULL) {
        cb(member->mds_id, true, arg);
    }

    return MDS_OK;
}

enum mds_status cluster_node_leave(struct cluster_membership *ctx,
                                   uint32_t mds_id)
{
    if (ctx == NULL) {
        return MDS_ERR_INVAL;
    }
    if (mds_id == 0) {
        return MDS_ERR_INVAL;
    }

    if (mds_id == ctx->self_id) {
        return MDS_ERR_INVAL;
    }

    /* Role-aware leave policy (Seq 8). */
    pthread_rwlock_rdlock(&ctx->lock);
    int idx = find_member(ctx, mds_id);
    if (idx < 0) {
        pthread_rwlock_unlock(&ctx->lock);
        return MDS_ERR_NOTFOUND;
    }

    enum node_role      role = ctx->members[idx].role;
    enum node_lifecycle lc   = ctx->members[idx].lifecycle;
    pthread_rwlock_unlock(&ctx->lock);

    /* Standbys must use cluster_standby_detach(). */
    if (role == NODE_STANDBY) {
        return MDS_ERR_PERM;
    }

    /* Active nodes: only JOINING/IDLE/DRAINED with zero subtrees. */
    if (lc == NODE_ACTIVE_SERVING || lc == NODE_DRAINING) {
        return MDS_ERR_PERM;
    }

    if (subtree_map_node_owns_subtrees(ctx->smap, mds_id)) {
        return MDS_ERR_PERM;
    }


    /* Local backend: called WITH lock */
    pthread_rwlock_wrlock(&ctx->lock);

    enum mds_status st = ctx->backend->leave(ctx, mds_id);
    if (st != MDS_OK) {
        pthread_rwlock_unlock(&ctx->lock);
        return st;
    }

    cluster_change_cb cb = ctx->change_cb;
    void *arg = ctx->change_arg;

    pthread_rwlock_unlock(&ctx->lock);

    (void)subtree_map_unregister_node(ctx->smap, mds_id);

    if (cb != NULL) {
        cb(mds_id, false, arg);
    }

    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Standby detach (Seq 8)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_standby_detach(struct cluster_membership *ctx,
                                       uint32_t mds_id)
{
    if (ctx == NULL) {
        return MDS_ERR_INVAL;
    }
    if (mds_id == 0) {
        return MDS_ERR_INVAL;
    }
    if (mds_id == ctx->self_id) {
        return MDS_ERR_INVAL;
    }

    /* Lookup member and validate role + replica safety. */
    pthread_rwlock_rdlock(&ctx->lock);

    int idx = find_member(ctx, mds_id);
    if (idx < 0) {
        pthread_rwlock_unlock(&ctx->lock);
        return MDS_ERR_NOTFOUND;
    }
    if (ctx->members[idx].role != NODE_STANDBY) {
        pthread_rwlock_unlock(&ctx->lock);
        return MDS_ERR_PERM;
    }

    uint32_t partner = ctx->members[idx].failover_partner_id;
    if (partner != 0) {
        int pidx = find_member(ctx, partner);
        if (pidx < 0 ||
            ctx->members[pidx].role != NODE_ACTIVE ||
            ctx->members[pidx].lifecycle != NODE_ACTIVE_SERVING) {
            pthread_rwlock_unlock(&ctx->lock);
            return MDS_ERR_PERM;
        }
    }

    pthread_rwlock_unlock(&ctx->lock);

    /* Safe to remove -- delegate to backend. */

    pthread_rwlock_wrlock(&ctx->lock);

    enum mds_status st = ctx->backend->leave(ctx, mds_id);
    if (st != MDS_OK) {
        pthread_rwlock_unlock(&ctx->lock);
        return st;
    }

    cluster_change_cb cb = ctx->change_cb;
    void *arg = ctx->change_arg;

    pthread_rwlock_unlock(&ctx->lock);

    (void)subtree_map_unregister_node(ctx->smap, mds_id);

    if (cb != NULL) {
        cb(mds_id, false, arg);
    }

    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Forced removal (Seq 8)
 * ----------------------------------------------------------------------- */

enum mds_status cluster_force_remove_node(struct cluster_membership *ctx,
                                          uint32_t mds_id)
{
    if (ctx == NULL) {
        return MDS_ERR_INVAL;
    }
    if (mds_id == 0) {
        return MDS_ERR_INVAL;
    }
    if (mds_id == ctx->self_id) {
        return MDS_ERR_INVAL;
    }

    /* Reject active nodes that still own subtrees. */
    pthread_rwlock_rdlock(&ctx->lock);

    int idx = find_member(ctx, mds_id);
    if (idx < 0) {
        pthread_rwlock_unlock(&ctx->lock);
        return MDS_ERR_NOTFOUND;
    }

    bool is_active = (ctx->members[idx].role == NODE_ACTIVE);
    pthread_rwlock_unlock(&ctx->lock);

    if (is_active && subtree_map_node_owns_subtrees(ctx->smap, mds_id)) {
        return MDS_ERR_PERM;
    }

    /* Bypass lifecycle checks -- straight removal. */

    pthread_rwlock_wrlock(&ctx->lock);

    enum mds_status st = ctx->backend->leave(ctx, mds_id);
    if (st != MDS_OK) {
        pthread_rwlock_unlock(&ctx->lock);
        return st;
    }

    cluster_change_cb cb = ctx->change_cb;
    void *arg = ctx->change_arg;

    pthread_rwlock_unlock(&ctx->lock);

    (void)subtree_map_unregister_node(ctx->smap, mds_id);

    if (cb != NULL) {
        cb(mds_id, false, arg);
    }

    return MDS_OK;
}


/* -----------------------------------------------------------------------
 * Self-removal for controlled demote (Item 46)
 *
 * Unlike cluster_node_leave / cluster_force_remove_node, self-removal
 * IS permitted.  The caller is responsible for calling this while the
 * entry still shows ACTIVE + ACTIVE_SERVING so that the partner-loss
 * watcher on the standby fires correctly.
 *
 * etcd mode  -- backend->leave deletes the key; the etcd watch handler
 *              picks up the DELETE and fires partner_loss_cb.
 * local mode -- remove from the array, fire change_cb + partner_loss_cb
 *              inline (no watch loop).
 * ----------------------------------------------------------------------- */

enum mds_status cluster_membership_remove_self(struct cluster_membership *ctx)
{
    if (ctx == NULL) {
        return MDS_ERR_INVAL;
    }

    uint32_t self_id = ctx->self_id;

    /* Snapshot the member BEFORE removal (for partner-loss callback). */
    struct cluster_member removed_snap;
    bool have_snap = false;

    pthread_rwlock_rdlock(&ctx->lock);
    int idx = find_member(ctx, self_id);
    if (idx >= 0) {
        removed_snap = ctx->members[idx];
        have_snap = true;
    }
    pthread_rwlock_unlock(&ctx->lock);

    if (!have_snap) {
        return MDS_ERR_NOTFOUND;
    }

    /* etcd mode: backend delete triggers the watch handler which
     * fires partner_loss_cb automatically.  No inline callback. */

    /* Local mode: remove + fire callbacks inline. */
    pthread_rwlock_wrlock(&ctx->lock);

    enum mds_status st = ctx->backend->leave(ctx, self_id);
    if (st != MDS_OK) {
        pthread_rwlock_unlock(&ctx->lock);
        return st;
    }

    /* Re-read callbacks under lock (could have changed). */
    cluster_change_cb cb = ctx->change_cb;
    void *cb_arg = ctx->change_arg;

    uint32_t ploss_id = ctx->partner_loss_id;
    cluster_partner_loss_cb ploss_cb = ctx->partner_loss_cb;
    void *ploss_arg = ctx->partner_loss_arg;

    pthread_rwlock_unlock(&ctx->lock);

    /* Unregister from subtree map. */
    if (ctx->smap != NULL) {
        (void)subtree_map_unregister_node(ctx->smap, self_id);
    }

    /* Fire generic change_cb (leave notification). */
    if (cb != NULL) {
        cb(self_id, false, cb_arg);
    }

    /* Fire partner-loss callback if configured and the snapshot
     * matches the ACTIVE + ACTIVE_SERVING condition. */
    if (ploss_cb != NULL &&
        self_id == ploss_id &&
        removed_snap.role == NODE_ACTIVE &&
        removed_snap.lifecycle == NODE_ACTIVE_SERVING) {
        ploss_cb(&removed_snap, ploss_arg);
    }

    return MDS_OK;
}


enum mds_status cluster_membership_list(const struct cluster_membership *ctx,
                                        struct cluster_member **out,
                                        uint32_t *count)
{
    if (ctx == NULL || out == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }

    pthread_rwlock_rdlock((pthread_rwlock_t *)&ctx->lock);

    uint32_t n = ctx->count;
    struct cluster_member *arr = NULL;

    if (n > 0) {
        arr = malloc((size_t)n * sizeof(*arr));
        if (arr == NULL) {
            pthread_rwlock_unlock((pthread_rwlock_t *)&ctx->lock);
            return MDS_ERR_NOMEM;
        }
        memcpy(arr, ctx->members, (size_t)n * sizeof(*arr));
    }

    pthread_rwlock_unlock((pthread_rwlock_t *)&ctx->lock);

    *out = arr;
    *count = n;
    return MDS_OK;
}

enum mds_status cluster_membership_get(const struct cluster_membership *ctx,
                                       uint32_t mds_id,
                                       struct cluster_member *out)
{
    if (ctx == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }

    pthread_rwlock_rdlock((pthread_rwlock_t *)&ctx->lock);

    int idx = find_member(ctx, mds_id);
    if (idx < 0) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&ctx->lock);
        return MDS_ERR_NOTFOUND;
    }

    *out = ctx->members[idx];

    pthread_rwlock_unlock((pthread_rwlock_t *)&ctx->lock);
    return MDS_OK;
}

bool cluster_membership_is_coordinator(const struct cluster_membership *ctx)
{
    if (ctx == NULL) {
        return false;
    }

    pthread_rwlock_rdlock((pthread_rwlock_t *)&ctx->lock);
    uint32_t low = lowest_mds_id(ctx);
    pthread_rwlock_unlock((pthread_rwlock_t *)&ctx->lock);

    return low == ctx->self_id;
}

void cluster_membership_set_change_cb(struct cluster_membership *ctx,
                                      cluster_change_cb cb,
                                      void *arg)
{
    if (ctx == NULL) {
        return;
    }

    pthread_rwlock_wrlock(&ctx->lock);
    ctx->change_cb = cb;
    ctx->change_arg = arg;
    pthread_rwlock_unlock(&ctx->lock);
}

uint32_t cluster_membership_count(const struct cluster_membership *ctx)
{
    if (ctx == NULL) {
        return 0;
    }

    pthread_rwlock_rdlock((pthread_rwlock_t *)&ctx->lock);
    uint32_t n = ctx->count;
    pthread_rwlock_unlock((pthread_rwlock_t *)&ctx->lock);
    return n;
}

uint32_t cluster_membership_self_id(const struct cluster_membership *ctx)
{
    if (ctx == NULL) {
        return 0;
    }
    return ctx->self_id;
}

bool cluster_membership_is_active_role(const struct cluster_membership *ctx,
                                       uint32_t mds_id)
{
    if (ctx == NULL || mds_id == 0) {
        return false;
    }

    pthread_rwlock_rdlock((pthread_rwlock_t *)&ctx->lock);

    int idx = find_member(ctx, mds_id);
    bool active = (idx >= 0 && ctx->members[idx].role == NODE_ACTIVE);

    pthread_rwlock_unlock((pthread_rwlock_t *)&ctx->lock);
    return active;
}

bool cluster_membership_can_own_subtrees(
    const struct cluster_membership *ctx, uint32_t mds_id)
{
    if (ctx == NULL) {
        return true;  /* single-node / no membership */
    }
    if (mds_id == 0) {
        return false;
    }

    pthread_rwlock_rdlock((pthread_rwlock_t *)&ctx->lock);

    int idx = find_member(ctx, mds_id);
    bool ok = (idx >= 0 &&
               ctx->members[idx].role == NODE_ACTIVE &&
               ctx->members[idx].lifecycle == NODE_ACTIVE_SERVING);

    pthread_rwlock_unlock((pthread_rwlock_t *)&ctx->lock);
    return ok;
}

/* -----------------------------------------------------------------------
 * Lifecycle transitions
 * ----------------------------------------------------------------------- */

static bool lifecycle_transition_ok(enum node_lifecycle cur,
                                    enum node_lifecycle next)
{
    if (cur == NODE_JOINING && next == NODE_IDLE) { return true;
}
    if (cur == NODE_IDLE && next == NODE_ACTIVE_SERVING) { return true;
}
    if (cur == NODE_ACTIVE_SERVING && next == NODE_DRAINING) { return true;
}
    if (cur == NODE_DRAINING && next == NODE_ACTIVE_SERVING) { return true;
}
    if (cur == NODE_DRAINING && next == NODE_DRAINED) { return true;
}
    return false;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status cluster_membership_set_lifecycle(
    struct cluster_membership *ctx, uint32_t mds_id,
    enum node_lifecycle new_state)
{
    if (ctx == NULL) {
        return MDS_ERR_INVAL;
    }
    if (mds_id == 0 || mds_id > MDS_MAX_NODES) {
        return MDS_ERR_INVAL;
    }


    /* Local mode -- under write lock */
    pthread_rwlock_wrlock(&ctx->lock);

    int idx = find_member(ctx, mds_id);
    if (idx < 0) {
        pthread_rwlock_unlock(&ctx->lock);
        return MDS_ERR_NOTFOUND;
    }

    if (ctx->members[idx].role == NODE_STANDBY &&
        (new_state == NODE_ACTIVE_SERVING || new_state == NODE_DRAINING)) {
        pthread_rwlock_unlock(&ctx->lock);
        return MDS_ERR_PERM;
    }

    enum node_lifecycle cur = ctx->members[idx].lifecycle;
    if (!lifecycle_transition_ok(cur, new_state)) {
        pthread_rwlock_unlock(&ctx->lock);
        return MDS_ERR_INVAL;
    }

    ctx->members[idx].lifecycle = new_state;

    pthread_rwlock_unlock(&ctx->lock);
    return MDS_OK;
}

bool cluster_membership_is_authoritative(const struct cluster_membership *ctx)
{
    (void)ctx;
    /* RonDB mode is always authoritative. */
    return true;
}

bool cluster_membership_is_self_fenced(const struct cluster_membership *ctx)
{
    (void)ctx;
    return false;
}

void cluster_membership_set_partner_loss_cb(
    struct cluster_membership *ctx,
    uint32_t partner_id,
    cluster_partner_loss_cb cb,
    void *arg)
{
    if (ctx == NULL) { return; }
    pthread_rwlock_wrlock(&ctx->lock);
    ctx->partner_loss_id  = partner_id;
    ctx->partner_loss_cb  = cb;
    ctx->partner_loss_arg = arg;
    pthread_rwlock_unlock(&ctx->lock);
}

/* -----------------------------------------------------------------------
 * Standby promotion (Seq 9)
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status cluster_membership_promote_standby(
    struct cluster_membership *ctx, uint32_t mds_id)
{
    if (ctx == NULL) {
        return MDS_ERR_INVAL;
    }
    if (mds_id == 0 || mds_id > MDS_MAX_NODES) {
        return MDS_ERR_INVAL;
    }


    /* Local mode: under write lock.  Does NOT fire change_cb. */
    pthread_rwlock_wrlock(&ctx->lock);

    int idx = find_member(ctx, mds_id);
    if (idx < 0) {
        pthread_rwlock_unlock(&ctx->lock);
        return MDS_ERR_NOTFOUND;
    }
    if (ctx->members[idx].role != NODE_STANDBY) {
        pthread_rwlock_unlock(&ctx->lock);
        return MDS_ERR_PERM;
    }

    ctx->members[idx].role = NODE_ACTIVE;
    ctx->members[idx].lifecycle = NODE_ACTIVE_SERVING;

    pthread_rwlock_unlock(&ctx->lock);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Peer resolution
 * ----------------------------------------------------------------------- */

/** Return true if @addr is empty or a wildcard bind address. */
static bool is_wildcard_addr(const char *addr)
{
    if (addr[0] == '\0') {
        return true;
}
    if (strcmp(addr, "0.0.0.0") == 0) {
        return true;
}
    if (strcmp(addr, "::") == 0) {
        return true;
}
    return false;
}

enum mds_status cluster_membership_resolve_peer(
    const struct cluster_membership *ctx,
    uint32_t mds_id,
    char *host_out, size_t host_cap,
    uint16_t *port_out)
{
    if (ctx == NULL || host_out == NULL || host_cap == 0 ||
        port_out == NULL) {
        return MDS_ERR_INVAL;
    }

    pthread_rwlock_rdlock((pthread_rwlock_t *)&ctx->lock);

    int idx = find_member(ctx, mds_id);
    if (idx < 0) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&ctx->lock);
        return MDS_ERR_NOTFOUND;
    }

    const struct cluster_member *m = &ctx->members[idx];

    if (m->grpc_port == 0) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&ctx->lock);
        return MDS_ERR_INVAL;
    }

    /* Prefer cluster_addr unless it is empty or a wildcard. */
    const char *src = is_wildcard_addr(m->cluster_addr)
                      ? m->hostname : m->cluster_addr;

    size_t len = strlen(src);
    if (len == 0 || len >= host_cap) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&ctx->lock);
        return MDS_ERR_INVAL;
    }

    memcpy(host_out, src, len + 1); /* includes NUL */
    *port_out = m->grpc_port;

    pthread_rwlock_unlock((pthread_rwlock_t *)&ctx->lock);
    return MDS_OK;
}

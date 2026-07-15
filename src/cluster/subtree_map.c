/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * subtree_map.c -- Subtree ownership map.
 *
 * Lock contract:
 *   - apply_subtree_upsert() / apply_subtree_remove() are private
 *     helpers that take the write lock internally and mutate the
 *     local cache.  Called by watch handlers and init.
 *   - Backend vtable set_owner/set_state/add_entry/remove_entry
 *     do RonDB I/O OUTSIDE the rwlock, then wait for the watch
 *     handler to apply the change locally.
 *   - Local backend vtable functions are called with write lock
 *     held (unchanged).
 *
 * Thread safety: all public API calls acquire the internal rwlock.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "subtree_map.h"
#include "cluster_membership.h"

/* -----------------------------------------------------------------------
 * Tunables
 * ----------------------------------------------------------------------- */

#define SUBTREE_MAP_INIT_CAP   16
#define NODE_INFO_INIT_CAP     8
#define WAIT_FOR_REV_TIMEOUT_SEC 10
#define SUBTREE_JSON_BUF       512

/* -----------------------------------------------------------------------
 * Backend vtable
 *
 * Local backend: called with write lock held, idx valid.
 * ----------------------------------------------------------------------- */

struct subtree_backend {
    enum mds_status (*set_owner)(struct subtree_map *map, int idx,
                                 uint32_t new_owner, uint64_t expected_ver);
    enum mds_status (*set_state)(struct subtree_map *map, int idx,
                                 enum subtree_state state,
                                 uint64_t expected_ver);
    enum mds_status (*add_entry)(struct subtree_map *map,
                                 const struct subtree_entry *entry);
    enum mds_status (*remove_entry)(struct subtree_map *map, int idx);
    void (*destroy)(struct subtree_map *map);
};

/* -----------------------------------------------------------------------
 * Internal structures
 * ----------------------------------------------------------------------- */

struct subtree_map {
    struct subtree_entry *entries;
    uint32_t              count;
    uint32_t              capacity;
    uint32_t              self_id;
    pthread_rwlock_t      lock;

    /* Node hostname table (for referral encoding). */
    struct mds_node_info *nodes;
    uint32_t              node_count;
    uint32_t              node_capacity;

    /* Frozen fileid set. */
    uint64_t             *frozen_fids;
    uint32_t              frozen_count;
    uint32_t              frozen_cap;

    const struct subtree_backend *backend;
    enum cluster_mode     mode;
    char                  key_prefix[256];    /**< key namespace (legacy, unused). */
    char                  subtrees_prefix[280]; /**< "{prefix}/subtrees" */
    char                  subtrees_scan[284];   /**< "{prefix}/subtrees/" (for prefix scans) */

    /* Optional membership backlink for role enforcement. */
    const struct cluster_membership *membership;

    /* reserved (legacy) */

    /* Change callback */
    subtree_change_cb     change_cb;
    void                 *change_arg;

    /* Wait-for-revision */
    int64_t               applied_revision;
    pthread_mutex_t       rev_mutex;
    pthread_cond_t        rev_cond;
};

/* -----------------------------------------------------------------------
 * Helpers
 * ----------------------------------------------------------------------- */

static enum mds_status register_node(struct subtree_map *m,
                                     uint32_t mds_id,
                                     const char *hostname)
{
    if (hostname == NULL) { return MDS_OK;
}

    for (uint32_t i = 0; i < m->node_count; i++) {
        if (m->nodes[i].mds_id == mds_id) {
            (void)snprintf(m->nodes[i].hostname,
                     sizeof(m->nodes[i].hostname), "%s", hostname);
            return MDS_OK;
        }
    }

    if (m->node_count >= m->node_capacity) {
        uint32_t new_cap = m->node_capacity * 2;
        /* NOLINTNEXTLINE(clang-analyzer-optin.portability.UnixAPI) */
        struct mds_node_info *tmp = realloc(
            m->nodes, (size_t)new_cap * sizeof(*tmp));
        if (tmp == NULL) { return MDS_ERR_NOMEM;
}
        m->nodes = tmp;
        m->node_capacity = new_cap;
    }

    m->nodes[m->node_count].mds_id = mds_id;
    (void)snprintf(m->nodes[m->node_count].hostname,
             sizeof(m->nodes[m->node_count].hostname), "%s", hostname);
    m->node_count++;
    return MDS_OK;
}

static enum mds_status grow_entries(struct subtree_map *m)
{
    if (m->count < m->capacity) { return MDS_OK;
}
    uint32_t new_cap = m->capacity * 2;
    /* NOLINTNEXTLINE(clang-analyzer-optin.portability.UnixAPI) */
    struct subtree_entry *tmp = realloc(
        m->entries, (size_t)new_cap * sizeof(*tmp));
    if (tmp == NULL) { return MDS_ERR_NOMEM;
}
    m->entries = tmp;
    m->capacity = new_cap;
    return MDS_OK;
}

static int longest_prefix_match(const struct subtree_map *m,
                                const char *path)
{
    int best = -1;
    size_t best_len = 0;

    for (uint32_t i = 0; i < m->count; i++) {
        size_t elen = strlen(m->entries[i].path);
        if (elen == 1 && m->entries[i].path[0] == '/') {
            if (best_len == 0) { best = (int)i; best_len = 1; }
            continue;
        }
        if (strncmp(path, m->entries[i].path, elen) != 0) { continue;
}
        char next = path[elen];
        if (next != '/' && next != '\0') { continue;
}
        if (elen > best_len) { best = (int)i; best_len = elen; }
    }
    return best;
}

static int find_exact(const struct subtree_map *m, const char *path)
{
    for (uint32_t i = 0; i < m->count; i++) {
        if (strcmp(m->entries[i].path, path) == 0) { return (int)i;
}
    }
    return -1;
}

enum mds_status subtree_map_set_root_fileid(struct subtree_map *map,
                                            const char *path,
                                            uint64_t fileid)
{
    int idx;

    if (map == NULL || path == NULL) { return MDS_ERR_INVAL;
}
    pthread_rwlock_wrlock(&map->lock);
    idx = find_exact(map, path);
    if (idx < 0) {
        pthread_rwlock_unlock(&map->lock);
        return MDS_ERR_NOTFOUND;
    }
    map->entries[idx].root_fileid = fileid;
    pthread_rwlock_unlock(&map->lock);
    return MDS_OK;
}

int subtree_map_owner_for_root_fileid(const struct subtree_map *map,
                                      uint64_t fileid,
                                      uint32_t *owner_out)
{
    struct subtree_map *m = (struct subtree_map *)(uintptr_t)map;
    int found = 0;

    if (m == NULL || fileid == 0) { return 0;
}
    pthread_rwlock_rdlock(&m->lock);
    for (uint32_t i = 0; i < m->count; i++) {
        if (m->entries[i].root_fileid != fileid) { continue;
}
        if (m->entries[i].path[0] == '/' &&
            m->entries[i].path[1] == '\0') { continue;
}
        if (owner_out != NULL) { *owner_out = m->entries[i].owner_mds_id;
}
        found = 1;
        break;
    }
    pthread_rwlock_unlock(&m->lock);
    return found;
}

/* -----------------------------------------------------------------------
 * apply_subtree_upsert / apply_subtree_remove
 *
 * Private helpers that mutate the local array under write lock.
 * Called by: watch handlers, init snapshot loading.
 * ----------------------------------------------------------------------- */

static void apply_subtree_upsert(struct subtree_map *m,
                                 const char *path,
                                 uint32_t owner_mds_id,
                                 enum subtree_state state,
                                 uint64_t version)
{
    pthread_rwlock_wrlock(&m->lock);

    int idx = find_exact(m, path);
    if (idx >= 0) {
        m->entries[idx].owner_mds_id = owner_mds_id;
        m->entries[idx].state = state;
        m->entries[idx].version = version;
    } else {
        if (grow_entries(m) == MDS_OK) {
            struct subtree_entry *e = &m->entries[m->count];
            memset(e, 0, sizeof(*e));
            (void)snprintf(e->path, sizeof(e->path), "%s", path);
            e->owner_mds_id = owner_mds_id;
            e->state = state;
            e->version = version;
            m->count++;
        }
    }

    pthread_rwlock_unlock(&m->lock);
}

static void apply_subtree_remove(struct subtree_map *m, const char *path)
{
    pthread_rwlock_wrlock(&m->lock);

    int idx = find_exact(m, path);
    if (idx >= 0) {
        if ((uint32_t)(idx + 1) < m->count) {
            memmove(&m->entries[idx], &m->entries[idx + 1],
                    (m->count - (uint32_t)idx - 1) *
                    sizeof(m->entries[0]));
        }
        m->count--;
    }

    pthread_rwlock_unlock(&m->lock);
}

/* -----------------------------------------------------------------------
 * Wait-for-revision (subtree)
 * ----------------------------------------------------------------------- */

static void subtree_signal_revision(struct subtree_map *m, int64_t rev)
{
    pthread_mutex_lock(&m->rev_mutex);
    if (rev > m->applied_revision) {
        m->applied_revision = rev;
    }
    pthread_cond_broadcast(&m->rev_cond);
    pthread_mutex_unlock(&m->rev_mutex);
}

static enum mds_status subtree_wait_for_revision(struct subtree_map *m,
                                                  int64_t target_rev)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += WAIT_FOR_REV_TIMEOUT_SEC;

    pthread_mutex_lock(&m->rev_mutex);
    while (m->applied_revision < target_rev) {
        int rc = pthread_cond_timedwait(&m->rev_cond, &m->rev_mutex, &ts);
        if (rc != 0) {
            pthread_mutex_unlock(&m->rev_mutex);
            return MDS_ERR_IO;
        }
    }
    pthread_mutex_unlock(&m->rev_mutex);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * JSON serialisation
 * ----------------------------------------------------------------------- */

int subtree_to_json(const struct subtree_entry *e,
                            char *buf, size_t cap)
{
    return snprintf(buf, cap,
        "{\"owner_mds_id\":%" PRIu32 ",\"state\":%d}",
        e->owner_mds_id, (int)e->state);
}


/* -----------------------------------------------------------------------
 * Membership backlink
 * ----------------------------------------------------------------------- */

void subtree_map_set_membership(struct subtree_map *map,
                                const struct cluster_membership *membership)
{
    if (map == NULL) { return;
}
    map->membership = membership;
}

static bool owner_role_ok(const struct subtree_map *map, uint32_t owner_id)
{
    return cluster_membership_can_own_subtrees(map->membership, owner_id);
}


/* -----------------------------------------------------------------------
 * Local backend
 * ----------------------------------------------------------------------- */

static enum mds_status local_set_owner(struct subtree_map *map, int idx,
                                       uint32_t new_owner,
                                       uint64_t expected_ver)
{
    if (map->entries[idx].version != expected_ver) { return MDS_ERR_STALE;
}
    map->entries[idx].owner_mds_id = new_owner;
    map->entries[idx].version++;
    return MDS_OK;
}

static enum mds_status local_set_state(struct subtree_map *map, int idx,
                                       enum subtree_state state,
                                       uint64_t expected_ver)
{
    if (map->entries[idx].version != expected_ver) { return MDS_ERR_STALE;
}
    map->entries[idx].state = state;
    map->entries[idx].version++;
    return MDS_OK;
}

static enum mds_status local_add_entry(struct subtree_map *map,
                                       const struct subtree_entry *entry)
{
    enum mds_status st = grow_entries(map);
    if (st != MDS_OK) { return st;
}
    map->entries[map->count] = *entry;
    map->count++;
    return MDS_OK;
}

static enum mds_status local_remove_entry(struct subtree_map *map, int idx)
{
    if ((uint32_t)(idx + 1) < map->count) {
        memmove(&map->entries[idx], &map->entries[idx + 1],
                (map->count - (uint32_t)idx - 1) * sizeof(map->entries[0]));
    }
    map->count--;
    return MDS_OK;
}

static void local_subtree_backend_destroy(struct subtree_map *map)
{
    (void)map;
}

static const struct subtree_backend local_subtree_backend = {
    .set_owner    = local_set_owner,
    .set_state    = local_set_state,
    .add_entry    = local_add_entry,
    .remove_entry = local_remove_entry,
    .destroy      = local_subtree_backend_destroy,
};

enum mds_status subtree_map_init(const char *etcd_endpoints,
                                 const char *etcd_key_prefix,
                                 uint32_t self_id,
                                 const char *self_hostname,
                                 struct mds_tls_ctx *etcd_tls,
                                 struct subtree_map **out)
{
    /* etcd_endpoints / etcd_tls are retained for ABI stability only:
     * the etcd backend has been removed, and these parameters are no
     * longer read. */
    (void)etcd_endpoints;
    (void)etcd_tls;

    if (out == NULL) { return MDS_ERR_INVAL;
}

    struct subtree_map *m = calloc(1, sizeof(*m));
    if (m == NULL) { return MDS_ERR_NOMEM;
}

    m->entries = calloc(SUBTREE_MAP_INIT_CAP, sizeof(*m->entries));
    if (m->entries == NULL) { free(m); return MDS_ERR_NOMEM; }
    m->capacity = SUBTREE_MAP_INIT_CAP;

    m->nodes = calloc(NODE_INFO_INIT_CAP, sizeof(*m->nodes));
    if (m->nodes == NULL) {
        free(m->entries); free(m); return MDS_ERR_NOMEM;
    }
    m->node_capacity = NODE_INFO_INIT_CAP;

    m->self_id = self_id;

    /* Set key prefix (legacy parameter, unused) (default "/pnfs-mds"). */
    const char *pfx = (etcd_key_prefix != NULL && etcd_key_prefix[0] != '\0')
                     ? etcd_key_prefix : "/pnfs-mds";
    (void)snprintf(m->key_prefix, sizeof(m->key_prefix), "%s", pfx);
    (void)snprintf(m->subtrees_prefix, sizeof(m->subtrees_prefix),
             "%s/subtrees", m->key_prefix);
    (void)snprintf(m->subtrees_scan, sizeof(m->subtrees_scan),
             "%s/subtrees/", m->key_prefix);

    int rc = pthread_rwlock_init(&m->lock, NULL);
    if (rc != 0) {
        free(m->nodes); free(m->entries); free(m);
        return MDS_ERR_NOMEM;
    }

    pthread_mutex_init(&m->rev_mutex, NULL);
    pthread_cond_init(&m->rev_cond, NULL);

    /* --- Local path --- */
    m->backend = &local_subtree_backend;
    m->mode = CLUSTER_MODE_LOCAL;

    /* Seed root entry: "/" owned by self. */
    (void)snprintf(m->entries[0].path, sizeof(m->entries[0].path), "/");
    m->entries[0].owner_mds_id = self_id;
    m->entries[0].version = 1;
    m->entries[0].state = SUBTREE_ACTIVE;
    m->count = 1;

    if (self_hostname != NULL) {
        enum mds_status st = register_node(m, self_id, self_hostname);
        if (st != MDS_OK) {
            pthread_mutex_destroy(&m->rev_mutex);
            pthread_cond_destroy(&m->rev_cond);
            pthread_rwlock_destroy(&m->lock);
            free(m->nodes); free(m->entries); free(m);
            return st;
        }
    }

    *out = m;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * RonDB backend
 * ----------------------------------------------------------------------- */

#ifdef HAVE_RONDB
#include "catalogue_rondb.h"
/* RONDB_PM_STATE_ACTIVE from rondb_schema.h -- inlined to avoid
 * adding src/catalogue to the cluster include path. */
#define PM_STATE_ACTIVE 0

struct pm_load_ctx {
    struct subtree_map *map;
    uint32_t            loaded;
};

static int pm_load_cb(uint32_t partition_id, uint32_t owner_mds_id,
                      uint8_t state, const char *subtree_path, void *ctx)
{
    struct pm_load_ctx *lc = ctx;
    struct subtree_map *m = lc->map;

    (void)partition_id;
    if (subtree_path == NULL || subtree_path[0] == '\0') {
        return 0;
    }
    apply_subtree_upsert(m, subtree_path, owner_mds_id,
                         (enum subtree_state)state, 1);
    lc->loaded++;
    return 0;
}

enum mds_status subtree_map_init_rondb(struct mds_catalogue *cat,
                                      uint32_t self_id,
                                      const char *self_hostname,
                                      struct subtree_map **out)
{
    struct subtree_map *m;
    struct pm_load_ctx lc;
    enum mds_status st;

    if (cat == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }

    m = calloc(1, sizeof(*m));
    if (m == NULL) { return MDS_ERR_NOMEM; }

    m->entries = calloc(SUBTREE_MAP_INIT_CAP, sizeof(*m->entries));
    if (m->entries == NULL) { free(m); return MDS_ERR_NOMEM; }
    m->capacity = SUBTREE_MAP_INIT_CAP;

    m->nodes = calloc(NODE_INFO_INIT_CAP, sizeof(*m->nodes));
    if (m->nodes == NULL) {
        free(m->entries); free(m); return MDS_ERR_NOMEM;
    }
    m->node_capacity = NODE_INFO_INIT_CAP;
    m->self_id = self_id;

    if (pthread_rwlock_init(&m->lock, NULL) != 0) {
        free(m->nodes); free(m->entries); free(m);
        return MDS_ERR_NOMEM;
    }
    pthread_mutex_init(&m->rev_mutex, NULL);
    pthread_cond_init(&m->rev_cond, NULL);

    /* Load subtree entries from RonDB partition_map. */
    lc.map = m;
    lc.loaded = 0;
    st = catalogue_rondb_partition_map_list(cat, pm_load_cb, &lc);
    if (st != MDS_OK) {
        MDS_LOG_WARN(LOG_COMP_CLUSTER,
            "partition_map load failed (%d), "
            "seeding root entry", (int)st);
    }

    /* Ensure root "/" exists. */
    if (find_exact(m, "/") < 0) {
        (void)snprintf(m->entries[m->count].path,
                 sizeof(m->entries[0].path), "/");
        m->entries[m->count].owner_mds_id = self_id;
        m->entries[m->count].version = 1;
        m->entries[m->count].state = SUBTREE_ACTIVE;
        m->count++;

        /* Claim root in RonDB. */
        (void)catalogue_rondb_partition_map_put(
            cat, 0, self_id, PM_STATE_ACTIVE, "/");
    }

    /* Load node hostnames from node_registry. */
    /* (done separately via main.c heartbeat registration) */

    m->backend = &local_subtree_backend;
    m->mode = CLUSTER_MODE_RONDB;

    if (self_hostname != NULL) {
        (void)register_node(m, self_id, self_hostname);
    }

    MDS_LOG_INFO(LOG_COMP_CLUSTER,
        "subtree_map_init_rondb: loaded %u entries from "
        "partition_map", lc.loaded);

    *out = m;
    return MDS_OK;
}
enum mds_status subtree_map_refresh_rondb(struct subtree_map *map,
                                          struct mds_catalogue *cat)
{
    struct pm_load_ctx lc;

    if (map == NULL || cat == NULL) {
        return MDS_ERR_INVAL;
    }

    lc.map = map;
    lc.loaded = 0;
    return catalogue_rondb_partition_map_list(cat, pm_load_cb, &lc);
}

enum mds_status subtree_map_seed_shards_rondb(
	struct subtree_map *map,
	struct mds_catalogue *cat,
	uint32_t cluster_size,
	const char *const *peer_hosts,
	uint32_t peer_count)
{
	if (map == NULL || cat == NULL) {
		return MDS_ERR_INVAL;
	}
	if (cluster_size <= 1 || subtree_map_count(map) > 1) {
		return MDS_OK;
	}

	for (uint32_t si = 0; si < cluster_size; si++) {
		char spath[64];
		uint32_t mds_id = si + 1;
		const char *host = NULL;
		enum mds_status ast;
		enum mds_status pst;

		(void)snprintf(spath, sizeof(spath),
			       "/shard%u", (unsigned)mds_id);
		if (peer_hosts != NULL && si < peer_count &&
		    peer_hosts[si] != NULL && peer_hosts[si][0] != '\0') {
			host = peer_hosts[si];
		}

		/*
		 * Membership is not wired yet, so owner_role_ok allows
		 * any owner_id.  Add to the local cache first so this
		 * boot can serve referrals even if RonDB put fails.
		 */
		ast = subtree_map_add(map, spath, mds_id, host,
				      SUBTREE_ACTIVE, 1);
		if (ast != MDS_OK && ast != MDS_ERR_EXISTS) {
			MDS_LOG_WARN(LOG_COMP_CLUSTER,
				"partition seed %s failed: %d",
				spath, (int)ast);
			continue;
		}

		/*
		 * Persist with partition_id == mds_id (root uses 0).
		 * writeTuple upsert — safe if every MDS races the seed.
		 */
		pst = catalogue_rondb_partition_map_put(
			cat, mds_id, mds_id, PM_STATE_ACTIVE, spath);
		if (pst != MDS_OK) {
			MDS_LOG_WARN(LOG_COMP_CLUSTER,
				"partition_map put %s (id=%u) failed: %d "
				"(in-memory seed kept for this boot)",
				spath, (unsigned)mds_id, (int)pst);
			if (ast == MDS_OK) {
				MDS_LOG_INFO(LOG_COMP_CLUSTER,
					"seeded partition %s -> MDS %u "
					"(memory only)",
					spath, (unsigned)mds_id);
			}
			continue;
		}

		if (ast == MDS_OK) {
			MDS_LOG_INFO(LOG_COMP_CLUSTER,
				"seeded partition %s -> MDS %u "
				"(persisted to partition_map)",
				spath, (unsigned)mds_id);
		} else {
			MDS_LOG_INFO(LOG_COMP_CLUSTER,
				"persisted existing partition %s -> "
				"MDS %u to partition_map",
				spath, (unsigned)mds_id);
		}
	}

	return MDS_OK;
}
#endif /* HAVE_RONDB */

/* -----------------------------------------------------------------------
 * Change callback setter
 * ----------------------------------------------------------------------- */

void subtree_map_set_change_cb(struct subtree_map *map,
                               subtree_change_cb cb, void *arg)
{
    if (map == NULL) { return;
}
    pthread_rwlock_wrlock(&map->lock);
    map->change_cb = cb;
    map->change_arg = arg;
    pthread_rwlock_unlock(&map->lock);
}

/* -----------------------------------------------------------------------
 * Public API -- reads
 * ----------------------------------------------------------------------- */

enum mds_status subtree_map_lookup(const struct subtree_map *map,
                                   const char *path,
                                   struct subtree_entry *entry)
{
    if (map == NULL || path == NULL || entry == NULL) { return MDS_ERR_INVAL;
}

    pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);
    int idx = longest_prefix_match(map, path);
    if (idx < 0) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
        return MDS_ERR_NOTFOUND;
    }
    *entry = map->entries[idx];
    pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
    return MDS_OK;
}

enum mds_status subtree_map_lookup_exact(const struct subtree_map *map,
                                         const char *path,
                                         struct subtree_entry *entry)
{
    if (map == NULL || path == NULL || entry == NULL) { return MDS_ERR_INVAL;
}

    pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);
    int idx = find_exact(map, path);
    if (idx < 0) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
        return MDS_ERR_NOTFOUND;
    }
    *entry = map->entries[idx];
    pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
    return MDS_OK;
}

bool subtree_map_is_local(const struct subtree_map *map, const char *path)
{
    if (map == NULL || path == NULL) { return false;
}
    struct subtree_entry entry;
    if (subtree_map_lookup(map, path, &entry) != MDS_OK) { return false;
}
    return entry.owner_mds_id == map->self_id;
}

/* -----------------------------------------------------------------------
 * Public API -- writes
 * ----------------------------------------------------------------------- */

enum mds_status subtree_map_set_owner(struct subtree_map *map,
                                      const char *path,
                                      uint32_t new_owner,
                                      uint64_t expected_ver)
{
    if (map == NULL || path == NULL) { return MDS_ERR_INVAL;
}

    if (!owner_role_ok(map, new_owner)) { return MDS_ERR_PERM;
}

    pthread_rwlock_wrlock(&map->lock);
    int idx = find_exact(map, path);
    if (idx < 0) {
        pthread_rwlock_unlock(&map->lock);
        return MDS_ERR_NOTFOUND;
    }
    enum mds_status st = map->backend->set_owner(map, idx, new_owner,
                                                  expected_ver);
    pthread_rwlock_unlock(&map->lock);
    return st;
}

enum mds_status subtree_map_transfer_owner_if_migrating(
    struct subtree_map *map, const char *path,
    uint32_t new_owner, uint64_t expected_ver)
{
    if (map == NULL || path == NULL) { return MDS_ERR_INVAL;
}

    /* State pre-check: entry must be SUBTREE_MIGRATING. */
    pthread_rwlock_rdlock(&map->lock);
    int idx = find_exact(map, path);
    if (idx < 0) {
        pthread_rwlock_unlock(&map->lock);
        return MDS_ERR_NOTFOUND;
    }
    if (map->entries[idx].state != SUBTREE_MIGRATING) {
        pthread_rwlock_unlock(&map->lock);
        return MDS_ERR_INVAL;
    }
    pthread_rwlock_unlock(&map->lock);

    /* Delegate to the same backend path as set_owner. */
    if (!owner_role_ok(map, new_owner)) { return MDS_ERR_PERM;
}


    pthread_rwlock_wrlock(&map->lock);
    idx = find_exact(map, path);
    if (idx < 0) {
        pthread_rwlock_unlock(&map->lock);
        return MDS_ERR_NOTFOUND;
    }
    /* Re-check state under write lock. */
    if (map->entries[idx].state != SUBTREE_MIGRATING) {
        pthread_rwlock_unlock(&map->lock);
        return MDS_ERR_INVAL;
    }
    enum mds_status st = map->backend->set_owner(map, idx, new_owner,
                                                  expected_ver);
    pthread_rwlock_unlock(&map->lock);
    return st;
}

enum mds_status subtree_map_set_state(struct subtree_map *map,
                                      const char *path,
                                      enum subtree_state state,
                                      uint64_t expected_ver)
{
    if (map == NULL || path == NULL) { return MDS_ERR_INVAL;
}


    pthread_rwlock_wrlock(&map->lock);
    int idx = find_exact(map, path);
    if (idx < 0) {
        pthread_rwlock_unlock(&map->lock);
        return MDS_ERR_NOTFOUND;
    }
    enum mds_status st = map->backend->set_state(map, idx, state,
                                                  expected_ver);
    pthread_rwlock_unlock(&map->lock);
    return st;
}

enum mds_status subtree_map_add(struct subtree_map *map,
                                const char *path,
                                uint32_t owner_id,
                                const char *hostname,
                                enum subtree_state state,
                                uint64_t version)
{
    if (map == NULL || path == NULL) { return MDS_ERR_INVAL;
}

    if (!owner_role_ok(map, owner_id)) { return MDS_ERR_PERM;
}

    pthread_rwlock_wrlock(&map->lock);

    if (find_exact(map, path) >= 0) {
        pthread_rwlock_unlock(&map->lock);
        return MDS_ERR_EXISTS;
    }

    struct subtree_entry new_entry;
    memset(&new_entry, 0, sizeof(new_entry));
    (void)snprintf(new_entry.path, sizeof(new_entry.path), "%s", path);
    new_entry.owner_mds_id = owner_id;
    new_entry.version = version;
    new_entry.state = state;

    enum mds_status st = map->backend->add_entry(map, &new_entry);
    if (st != MDS_OK) {
        pthread_rwlock_unlock(&map->lock);
        return st;
    }

    if (hostname != NULL) {
        (void)register_node(map, owner_id, hostname);
    }

    pthread_rwlock_unlock(&map->lock);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Accessors
 * ----------------------------------------------------------------------- */

uint32_t subtree_map_self_id(const struct subtree_map *map)
{
    return map ? map->self_id : 0;
}

uint32_t subtree_map_count(const struct subtree_map *map)
{
    if (map == NULL) { return 0;
}
    pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);
    uint32_t n = map->count;
    pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
    return n;
}

enum mds_status subtree_map_get_entry(const struct subtree_map *map,
                                      uint32_t idx,
                                      struct subtree_entry *out)
{
    if (map == NULL || out == NULL) { return MDS_ERR_INVAL;
}
    pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);
    if (idx >= map->count) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
        return MDS_ERR_INVAL;
    }
    *out = map->entries[idx];
    pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
    return MDS_OK;
}

enum mds_status subtree_map_node_hostname(const struct subtree_map *map,
                                          uint32_t mds_id,
                                          char *hostname, size_t len)
{
    if (map == NULL || hostname == NULL || len == 0) { return MDS_ERR_INVAL;
}

    pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);
    for (uint32_t i = 0; i < map->node_count; i++) {
        if (map->nodes[i].mds_id == mds_id) {
            (void)snprintf(hostname, len, "%s", map->nodes[i].hostname);
            pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
            return MDS_OK;
        }
    }
    pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
    return MDS_ERR_NOTFOUND;
}

/* -----------------------------------------------------------------------
 * Frozen fileid set
 * ----------------------------------------------------------------------- */

static int cmp_u64(const void *a, const void *b)
{
    uint64_t va = *(const uint64_t *)a;
    uint64_t vb = *(const uint64_t *)b;
    if (va < vb) { return -1;
}
    if (va > vb) { return  1;
}
    return 0;
}

enum mds_status subtree_map_freeze_fileids(struct subtree_map *map,
                                           const uint64_t *fids,
                                           uint32_t count)
{
    if (map == NULL || (count > 0 && fids == NULL)) { return MDS_ERR_INVAL;
}
    if (count == 0) { return MDS_OK;
}

    pthread_rwlock_wrlock(&map->lock);

    uint32_t new_total = map->frozen_count + count;
    if (new_total > map->frozen_cap) {
        uint32_t new_cap = map->frozen_cap;
        if (new_cap == 0) { new_cap = 64;
}
        while (new_cap < new_total) { new_cap *= 2;
}
        uint64_t *tmp = realloc(map->frozen_fids,
                                (size_t)new_cap * sizeof(*tmp));
        if (tmp == NULL) {
            pthread_rwlock_unlock(&map->lock);
            return MDS_ERR_NOMEM;
        }
        map->frozen_fids = tmp;
        map->frozen_cap = new_cap;
    }

    memcpy(map->frozen_fids + map->frozen_count, fids,
           (size_t)count * sizeof(*fids));
    map->frozen_count = new_total;
    qsort(map->frozen_fids, map->frozen_count,
          sizeof(*map->frozen_fids), cmp_u64);

    pthread_rwlock_unlock(&map->lock);
    return MDS_OK;
}

bool subtree_map_is_fileid_frozen(const struct subtree_map *map,
                                  uint64_t fileid)
{
    if (map == NULL) { return false;
}
    pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);
    bool found = false;
    if (map->frozen_count > 0) {
        found = bsearch(&fileid, map->frozen_fids, map->frozen_count,
                        sizeof(*map->frozen_fids), cmp_u64) != NULL;
    }
    pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
    return found;
}

void subtree_map_unfreeze_fileids(struct subtree_map *map,
                                  const uint64_t *fids,
                                  uint32_t count)
{
    if (map == NULL || count == 0 || fids == NULL) { return;
}

    pthread_rwlock_wrlock(&map->lock);
    for (uint32_t i = 0; i < count; i++) {
        uint64_t *found = bsearch(&fids[i], map->frozen_fids,
                                  map->frozen_count,
                                  sizeof(*map->frozen_fids), cmp_u64);
        if (found != NULL) {
            size_t fidx = (size_t)(found - map->frozen_fids);
            size_t tail = map->frozen_count - fidx - 1;
            if (tail > 0) { memmove(found, found + 1, tail * sizeof(*found));
}
            map->frozen_count--;
        }
    }
    pthread_rwlock_unlock(&map->lock);
}

void subtree_map_destroy(struct subtree_map *map)
{
    if (map == NULL) { return;
}
    if (map->backend != NULL) { map->backend->destroy(map);
}
    pthread_rwlock_destroy(&map->lock);
    if (map->mode == CLUSTER_MODE_LOCAL) {
        pthread_mutex_destroy(&map->rev_mutex);
        pthread_cond_destroy(&map->rev_cond);
    }
    free(map->frozen_fids);
    free(map->nodes);
    free(map->entries);
    free(map);
}

/* -----------------------------------------------------------------------
 * Referral helper
 * ----------------------------------------------------------------------- */

enum mds_status referral_build(const struct subtree_map *map,
                               const char *path,
                               struct mds_fs_location *loc)
{
    if (map == NULL || path == NULL || loc == NULL) { return MDS_ERR_INVAL;
}

    struct subtree_entry entry;
    enum mds_status st = subtree_map_lookup(map, path, &entry);
    if (st != MDS_OK) { return st;
}

    st = subtree_map_node_hostname(map, entry.owner_mds_id,
                                   loc->server, sizeof(loc->server));
    if (st != MDS_OK) { return st;
}

    /* rootpath is the subtree path itself: every MDS exports the
     * full shared namespace at "/", so the fs_location must direct
     * the client to <owner>:<subtree-path> (RFC 8881 S11.11).  The
     * previous hard-coded "/" pointed clients at the owner's global
     * root, silently aliasing every referral submount to the root
     * directory: all subtrees shared one server directory (cross-
     * subtree collisions) and the real subtree dirs stayed empty. */
    (void)snprintf(loc->rootpath, sizeof(loc->rootpath),
                   "%s", entry.path);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Per-subtree operation counters
 * ----------------------------------------------------------------------- */

enum mds_status subtree_map_inc_ops(struct subtree_map *map,
                                    const char *path)
{
    if (map == NULL || path == NULL) { return MDS_ERR_INVAL;
}

    pthread_rwlock_rdlock(&map->lock);
    for (uint32_t i = 0; i < map->count; i++) {
        if (strcmp(map->entries[i].path, path) == 0) {
            atomic_fetch_add_explicit(&map->entries[i].op_count,
                                      1, memory_order_relaxed);
            pthread_rwlock_unlock(&map->lock);
            return MDS_OK;
        }
    }
    pthread_rwlock_unlock(&map->lock);
    return MDS_ERR_NOTFOUND;
}

enum mds_status subtree_map_get_stats(const struct subtree_map *map,
                                      const char *path,
                                      uint64_t *count_out)
{
    if (map == NULL || path == NULL || count_out == NULL) { return MDS_ERR_INVAL;
}

    pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);
    for (uint32_t i = 0; i < map->count; i++) {
        if (strcmp(map->entries[i].path, path) == 0) {
            *count_out = atomic_load_explicit(
                (_Atomic uint64_t *)&map->entries[i].op_count,
                memory_order_relaxed);
            pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
            return MDS_OK;
        }
    }
    pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
    return MDS_ERR_NOTFOUND;
}

enum mds_status subtree_map_reset_stats(struct subtree_map *map,
                                        const char *path)
{
    if (map == NULL || path == NULL) { return MDS_ERR_INVAL;
}

    pthread_rwlock_rdlock(&map->lock);
    for (uint32_t i = 0; i < map->count; i++) {
        if (strcmp(map->entries[i].path, path) == 0) {
            atomic_store_explicit(&map->entries[i].op_count,
                                  0, memory_order_relaxed);
            pthread_rwlock_unlock(&map->lock);
            return MDS_OK;
        }
    }
    pthread_rwlock_unlock(&map->lock);
    return MDS_ERR_NOTFOUND;
}

/* -----------------------------------------------------------------------
 * Failover: take over partner's subtrees
 * ----------------------------------------------------------------------- */

enum mds_status subtree_map_take_over(struct subtree_map *map,
                                      uint32_t old_owner,
                                      uint32_t new_owner,
                                      uint32_t *count_out)
{
    uint32_t taken = 0;

    if (map == NULL || count_out == NULL) { return MDS_ERR_INVAL;
}

    pthread_rwlock_wrlock(&map->lock);

    if (!owner_role_ok(map, new_owner)) {
        pthread_rwlock_unlock(&map->lock);
        *count_out = 0;
        return MDS_ERR_PERM;
    }

    for (uint32_t i = 0; i < map->count; i++) {
        if (map->entries[i].owner_mds_id == old_owner) {
            map->entries[i].owner_mds_id = new_owner;
            map->entries[i].version++;
            taken++;
        }
    }

    pthread_rwlock_unlock(&map->lock);
    *count_out = taken;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Failover-specific subtree takeover
 *
 * Bypasses owner_role_ok() because the promoting standby is not yet
 * ACTIVE_SERVING.
 * ----------------------------------------------------------------------- */

enum mds_status subtree_map_failover_take_over(struct subtree_map *map,
                                               uint32_t old_owner,
                                               uint32_t new_owner,
                                               uint32_t *count_out)
{
    uint32_t taken = 0;

    if (map == NULL || count_out == NULL) {
        return MDS_ERR_INVAL;
    }

    pthread_rwlock_wrlock(&map->lock);

    for (uint32_t i = 0; i < map->count; i++) {
        if (map->entries[i].owner_mds_id == old_owner) {
            map->entries[i].owner_mds_id = new_owner;
            map->entries[i].version++;
            taken++;
        }
    }

    pthread_rwlock_unlock(&map->lock);
    *count_out = taken;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Split/merge map-level primitives
 * ----------------------------------------------------------------------- */

static uint32_t path_depth(const char *path)
{
    uint32_t depth = 0;
    for (const char *p = path; *p != '\0'; p++) {
        if (*p == '/') { depth++;
}
    }
    return (depth > 0) ? depth - 1 : 0;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status subtree_map_validate_split(const struct subtree_map *map,
                                           const char *parent_path,
                                           const char *child_path,
                                           uint32_t max_depth)
{
    size_t plen;
    uint32_t depth;
    bool parent_found = false;
    bool parent_local = false;

    if (map == NULL || parent_path == NULL || child_path == NULL) {
        return MDS_ERR_INVAL;
}

    if (max_depth == 0) { max_depth = SUBTREE_MAX_SPLIT_DEPTH;
}

    plen = strlen(parent_path);

    if (strcmp(parent_path, "/") == 0) {
        if (child_path[0] != '/') { return MDS_ERR_INVAL;
}
    } else {
        if (strncmp(child_path, parent_path, plen) != 0) { return MDS_ERR_INVAL;
}
        if (child_path[plen] != '/') { return MDS_ERR_INVAL;
}
    }

    depth = path_depth(child_path);
    if (depth > max_depth) { return MDS_ERR_INVAL;
}

    size_t clen = strlen(child_path);

    pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);

    for (uint32_t i = 0; i < map->count; i++) {
        const char *epath = map->entries[i].path;
        size_t elen = strlen(epath);

        if (strcmp(epath, child_path) == 0) {
            pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
            return MDS_ERR_EXISTS;
        }
        if (strcmp(epath, parent_path) == 0) {
            parent_found = true;
            if (map->entries[i].owner_mds_id == map->self_id) {
                parent_local = true;
}
        }

        if (elen > plen && elen < clen &&
            strncmp(epath, parent_path, plen) == 0 &&
            (plen == 1 || epath[plen] == '/') &&
            strncmp(child_path, epath, elen) == 0 &&
            child_path[elen] == '/') {
            if (map->entries[i].owner_mds_id != map->self_id) {
                pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
                return MDS_ERR_PERM;
            }
        }
    }

    pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);

    if (!parent_found) { return MDS_ERR_NOTFOUND;
}
    if (!parent_local) { return MDS_ERR_PERM;
}
    return MDS_OK;
}

enum mds_status subtree_map_register_split(struct subtree_map *map,
                                           const char *parent_path,
                                           const char *child_path,
                                           uint32_t owner)
{
    if (map == NULL || parent_path == NULL || child_path == NULL) {
        return MDS_ERR_INVAL;
}
    return subtree_map_add(map, child_path, owner, NULL,
                           SUBTREE_ACTIVE, 1);
}

enum mds_status subtree_map_remove_subtree(struct subtree_map *map,
                                           const char *path)
{
    if (map == NULL || path == NULL) { return MDS_ERR_INVAL;
}
    if (strcmp(path, "/") == 0) { return MDS_ERR_INVAL;
}


    pthread_rwlock_wrlock(&map->lock);
    int idx = find_exact(map, path);
    if (idx < 0) {
        pthread_rwlock_unlock(&map->lock);
        return MDS_ERR_NOTFOUND;
    }
    enum mds_status st = map->backend->remove_entry(map, idx);
    pthread_rwlock_unlock(&map->lock);
    return st;
}

/* -----------------------------------------------------------------------
 * Node registration -- public wrappers
 * ----------------------------------------------------------------------- */

enum mds_status subtree_map_register_node(struct subtree_map *map,
                                          uint32_t mds_id,
                                          const char *hostname)
{
    if (map == NULL || hostname == NULL) { return MDS_ERR_INVAL;
}
    pthread_rwlock_wrlock(&map->lock);
    enum mds_status st = register_node(map, mds_id, hostname);
    pthread_rwlock_unlock(&map->lock);
    return st;
}

enum mds_status subtree_map_unregister_node(struct subtree_map *map,
                                            uint32_t mds_id)
{
    if (map == NULL) { return MDS_ERR_INVAL;
}
    pthread_rwlock_wrlock(&map->lock);
    for (uint32_t i = 0; i < map->node_count; i++) {
        if (map->nodes[i].mds_id == mds_id) {
            if (i + 1 < map->node_count) {
                memmove(&map->nodes[i], &map->nodes[i + 1],
                        (map->node_count - i - 1) * sizeof(map->nodes[0]));
            }
            map->node_count--;
            pthread_rwlock_unlock(&map->lock);
            return MDS_OK;
        }
    }
    pthread_rwlock_unlock(&map->lock);
    return MDS_ERR_NOTFOUND;
}

bool subtree_map_node_owns_subtrees(const struct subtree_map *map,
                                    uint32_t mds_id)
{
    if (map == NULL) { return false;
}
    pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);
    for (uint32_t i = 0; i < map->count; i++) {
        if (map->entries[i].owner_mds_id == mds_id) {
            pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
            return true;
        }
    }
    pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
    return false;
}

enum mds_status subtree_map_get_node_subtrees(
    const struct subtree_map *map,
    uint32_t mds_id,
    struct subtree_entry **out,
    uint32_t *count)
{
    if (map == NULL || out == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }

    *out = NULL;
    *count = 0;

    pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);

    /* First pass: count matching entries. */
    uint32_t n = 0;
    for (uint32_t i = 0; i < map->count; i++) {
        if (map->entries[i].owner_mds_id == mds_id) {
            n++;
        }
    }

    if (n == 0) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
        return MDS_OK;
    }

    struct subtree_entry *arr = calloc(n, sizeof(*arr));
    if (arr == NULL) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
        return MDS_ERR_NOMEM;
    }

    /* Second pass: copy matching entries. */
    uint32_t idx = 0;
    for (uint32_t i = 0; i < map->count && idx < n; i++) {
        if (map->entries[i].owner_mds_id == mds_id) {
            arr[idx] = map->entries[i];
            idx++;
        }
    }

    pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);

    *out = arr;
    *count = idx;
    return MDS_OK;
}

bool subtree_map_is_authoritative(const struct subtree_map *map)
{
    if (map == NULL) { return false;
}
    return false;
}

/* -----------------------------------------------------------------------
 * Snapshot helper (Seq 10)
 * ----------------------------------------------------------------------- */

enum mds_status subtree_map_list_snapshot(
    const struct subtree_map *map,
    struct subtree_entry **out,
    uint32_t *count)
{
    if (map == NULL || out == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }

    *out = NULL;
    *count = 0;

    pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);

    uint32_t n = map->count;
    if (n == 0) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
        return MDS_OK;
    }

    struct subtree_entry *arr = calloc(n, sizeof(*arr));
    if (arr == NULL) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
        return MDS_ERR_NOMEM;
    }

    for (uint32_t i = 0; i < n; i++) {
        arr[i] = map->entries[i];
    }

    pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);

    *out = arr;
    *count = n;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Rolling upgrade: freeze/unfreeze owned subtrees (Item 46)
 * ----------------------------------------------------------------------- */

enum mds_status subtree_map_freeze_owned(struct subtree_map *map,
                                         uint32_t owner_id)
{
    uint32_t n;
    uint32_t i;

    if (map == NULL) {
        return MDS_ERR_INVAL;
    }

    n = subtree_map_count(map);
    for (i = 0; i < n; i++) {
        struct subtree_entry ent;
        enum mds_status st = subtree_map_get_entry(map, i, &ent);
        if (st != MDS_OK) {
            continue;
        }
        if (ent.owner_mds_id != owner_id) {
            continue;
        }
        if (ent.state != SUBTREE_ACTIVE) {
            continue;
        }
        st = subtree_map_set_state(map, ent.path,
                                   SUBTREE_FROZEN, ent.version);
        if (st != MDS_OK) {
            return st;
        }
    }
    return MDS_OK;
}

enum mds_status subtree_map_unfreeze_owned(struct subtree_map *map,
                                           uint32_t owner_id)
{
    uint32_t n;
    uint32_t i;

    if (map == NULL) {
        return MDS_ERR_INVAL;
    }

    n = subtree_map_count(map);
    for (i = 0; i < n; i++) {
        struct subtree_entry ent;
        enum mds_status st = subtree_map_get_entry(map, i, &ent);
        if (st != MDS_OK) {
            continue;
        }
        if (ent.owner_mds_id != owner_id) {
            continue;
        }
        if (ent.state != SUBTREE_FROZEN) {
            continue;
        }
        st = subtree_map_set_state(map, ent.path,
                                   SUBTREE_ACTIVE, ent.version);
        if (st != MDS_OK) {
            return st;
        }
    }
    return MDS_OK;
}

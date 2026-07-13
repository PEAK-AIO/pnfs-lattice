/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * subtree_map.h -- Subtree ownership map and fs_locations referrals.
 */

#ifndef SUBTREE_MAP_H
#define SUBTREE_MAP_H

#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include "pnfs_mds.h"

/* -----------------------------------------------------------------------
 * Subtree states
 * ----------------------------------------------------------------------- */

enum subtree_state {
    SUBTREE_ACTIVE    = 0,
    SUBTREE_MIGRATING = 1,
    SUBTREE_FROZEN    = 2,
};

/* -----------------------------------------------------------------------
 * Subtree entry
 * ----------------------------------------------------------------------- */

struct subtree_entry {
    char                path[MDS_MAX_PATH];
    uint32_t            owner_mds_id;
    uint64_t            version;
    enum subtree_state  state;
    _Atomic uint64_t    op_count;  /**< Advisory per-subtree op counter. */
    uint64_t            root_fileid; /**< Fileid of the subtree root dir
                                       *  (0 = unresolved).  Lets FH-based
                                       *  ancestry walks recognise a
                                       *  junction without a path. */
};

/* -----------------------------------------------------------------------
 * MDS node info (for referral encoding)
 * ----------------------------------------------------------------------- */

struct mds_node_info {
    uint32_t mds_id;
    char     hostname[256];
};

/* -----------------------------------------------------------------------
 * Subtree map handle
 * ----------------------------------------------------------------------- */

struct subtree_map;
struct mds_tls_ctx;

/* -----------------------------------------------------------------------
 * Subtree change callback
 *
 * Fired by the etcd watch handler on PUT/DELETE of subtree entries.
 * Informational -- must not call back into subtree_map (deadlock).
 * ----------------------------------------------------------------------- */

typedef void (*subtree_change_cb)(const char *path, bool exists, void *arg);

void subtree_map_set_change_cb(struct subtree_map *map,
                               subtree_change_cb cb, void *arg);

/**
 * @brief Initialise the subtree map.
 *
 * Seeds a root subtree entry (path="/", owner=self_id, ACTIVE, v=1).
 * If etcd_endpoints is NULL, operates in local-only mode (no etcd sync).
 *
 * @param etcd_endpoints  Comma-separated etcd endpoints, or NULL.
 * @param self_id         This MDS node's ID.
 * @param self_hostname   This node's hostname (for referrals).
 * @param[out] map        Receives the map handle.
 * @return MDS_OK on success.
 */
/* NOLINTNEXTLINE(readability-inconsistent-declaration-parameter-name) */
enum mds_status subtree_map_init(const char *etcd_endpoints,
                                 const char *etcd_key_prefix,
                                 uint32_t self_id,
                                 const char *self_hostname,
                                 struct mds_tls_ctx *etcd_tls,
                                 struct subtree_map **map);

struct mds_catalogue;

/**
 * @brief Initialise the subtree map from RonDB partition_map.
 *
 * Loads subtree entries from mds_partition_map NDB table.  No etcd.
 *
 * @param cat             Catalogue handle (RonDB backend).
 * @param self_id         This MDS node's ID.
 * @param self_hostname   This node's hostname (for referrals).
 * @param[out] map        Receives the map handle.
 * @return MDS_OK on success.
 */
enum mds_status subtree_map_init_rondb(struct mds_catalogue *cat,
                                      uint32_t self_id,
                                      const char *self_hostname,
                                      struct subtree_map **out);

/**
 * @brief Refresh the subtree map from RonDB partition_map.
 *
 * Re-reads all entries from the partition_map table and upserts
 * them into the local cache.  New entries are added, changed
 * owners are updated.  Safe to call from background threads.
 *
 * @param map  Subtree map (must have been init'd with _rondb).
 * @param cat  Catalogue handle (must be RonDB backend).
 * @return MDS_OK on success.
 */
enum mds_status subtree_map_refresh_rondb(struct subtree_map *map,
                                          struct mds_catalogue *cat);

/**
 * @brief Look up the owning MDS for a given path.
 * @param map   Map handle.
 * @param path  Absolute path to look up.
 * @param[out] entry  Receives the matching subtree entry.
 * @return MDS_OK if found, MDS_ERR_NOTFOUND otherwise.
 */
enum mds_status subtree_map_lookup(const struct subtree_map *map,
                                   const char *path,
                                   struct subtree_entry *entry);


/**
 * @brief Exact-path subtree lookup (no longest-prefix fallback).
 * @param map   Map handle.
 * @param path  Absolute path to look up.
 * @param[out] entry  Receives the matching subtree entry.
 * @return MDS_OK if an entry with exactly this path exists, MDS_ERR_NOTFOUND otherwise.
 */
enum mds_status subtree_map_lookup_exact(const struct subtree_map *map,
                                         const char *path,
                                         struct subtree_entry *entry);
/**
 * @brief Check if a path is owned by this MDS.
 * @param map   Map handle.
 * @param path  Absolute path.
 * @return true if this MDS owns the subtree containing path.
 */
bool subtree_map_is_local(const struct subtree_map *map, const char *path);

/**
 * @brief Update subtree ownership (CAS on etcd).
 * @param map            Map handle.
 * @param path           Subtree root path.
 * @param new_owner      New owning MDS ID.
 * @param expected_ver   Expected version for CAS.
 * @return MDS_OK on success, MDS_ERR_STALE if version mismatch.
 */
enum mds_status subtree_map_set_owner(struct subtree_map *map,
                                      const char *path,
                                      uint32_t new_owner,
                                      uint64_t expected_ver);

/**
 * @brief Update subtree state (CAS on etcd).
 */
enum mds_status subtree_map_set_state(struct subtree_map *map,
                                      const char *path,
                                      enum subtree_state state,
                                      uint64_t expected_ver);

/**
 * @brief Add a subtree entry to the map.
 *
 * Used during initialisation (node join) and by tests.  The entry
 * is inserted with the given owner, state, and version.  If a hostname
 * is provided, it is registered in the node info table for referral
 * encoding.
 *
 * @param map       Map handle.
 * @param path      Subtree root path (e.g. "/home").
 * @param owner_id  Owning MDS node ID.
 * @param hostname  Owner's hostname (may be NULL for self).
 * @param state     Initial state.
 * @param version   Initial version.
 * @return MDS_OK on success, MDS_ERR_EXISTS if path already registered.
 */
enum mds_status subtree_map_add(struct subtree_map *map,
                                const char *path,
                                uint32_t owner_id,
                                const char *hostname,
                                enum subtree_state state,
                                uint64_t version);

/**
 * @brief Return the self_id of the map.
 */
uint32_t subtree_map_self_id(const struct subtree_map *map);

/**
 * @brief Return the number of subtree entries in the map.
 */
uint32_t subtree_map_count(const struct subtree_map *map);

/**
 * @brief Read a subtree entry by index (for iteration).
 *
 * @param map   Map handle.
 * @param idx   Zero-based index (0 .. subtree_map_count()-1).
 * @param out   Receives a copy of the entry.
 * @return MDS_OK, MDS_ERR_INVAL if out of range.
 */
enum mds_status subtree_map_get_entry(const struct subtree_map *map,
                                      uint32_t idx,
                                      struct subtree_entry *out);

/**
 * @brief Record the root-directory fileid for a subtree entry.
 *
 * Resolved once at startup (and again on partition reload) by walking
 * the entry's path through the catalogue.  Enables fileid-based
 * junction identification for FHs presented without a path.
 */
enum mds_status subtree_map_set_root_fileid(struct subtree_map *map,
                                            const char *path,
                                            uint64_t fileid);

/**
 * @brief Look up the owning MDS of a junction by its root fileid.
 *
 * Only matches non-root entries whose root_fileid has been resolved.
 *
 * @return 1 and sets *owner_out when fileid is a registered junction
 *         root; 0 otherwise.
 */
int subtree_map_owner_for_root_fileid(const struct subtree_map *map,
                                      uint64_t fileid,
                                      uint32_t *owner_out);


/**
 * @brief Look up node hostname by mds_id.
 * @return MDS_OK on success, MDS_ERR_NOTFOUND if unknown.
 */
enum mds_status subtree_map_node_hostname(const struct subtree_map *map,
                                          uint32_t mds_id,
                                          char *hostname, size_t len);

/**
 * @brief Register a set of fileids as frozen (migrating).
 *
 * Called by migration_initiate() after freezing a subtree.  The fileids
 * are inserted into a sorted in-memory set for O(log n) lookup.
 *
 * @param map    Map handle.
 * @param fids   Array of fileids to freeze.
 * @param count  Number of entries in @fids.
 * @return MDS_OK on success, MDS_ERR_NOMEM on allocation failure.
 */
enum mds_status subtree_map_freeze_fileids(struct subtree_map *map,
                                           const uint64_t *fids,
                                           uint32_t count);

/**
 * @brief Check if a fileid is in the frozen set.
 *
 * Used by the compound dispatcher when the current path is unknown
 * (e.g. raw PUTFH) to precisely block only mutations to fileids
 * within a migrating subtree.
 *
 * @param map    Map handle.
 * @param fileid Fileid to check.
 * @return true if the fileid is frozen.
 */
bool subtree_map_is_fileid_frozen(const struct subtree_map *map,
                                  uint64_t fileid);

/**
 * @brief Remove a set of fileids from the frozen set.
 *
 * Called by migration_initiate() after migration completes or aborts.
 *
 * @param map    Map handle.
 * @param fids   Array of fileids to unfreeze.
 * @param count  Number of entries in @fids.
 */
void subtree_map_unfreeze_fileids(struct subtree_map *map,
                                  const uint64_t *fids,
                                  uint32_t count);

/**
 * @brief Shut down the subtree map and free resources.
 */
void subtree_map_destroy(struct subtree_map *map);

/* -----------------------------------------------------------------------
 * Referral helpers
 * ----------------------------------------------------------------------- */

struct mds_fs_location {
    char server[256];
    char rootpath[MDS_MAX_PATH];
};

/**
 * @brief Build an fs_locations referral for a foreign subtree.
 * @param map   Subtree map handle.
 * @param path  Path of the junction directory.
 * @param[out] loc  Receives the referral location.
 * @return MDS_OK on success.
 */
enum mds_status referral_build(const struct subtree_map *map,
                               const char *path,
                               struct mds_fs_location *loc);


/**
 * @brief Take over all subtrees owned by old_owner.
 *
 * Iterates all entries, CAS-updates those with owner_mds_id == old_owner
 * to new_owner.  Used during failover promotion.
 *
 * @param map        Map handle.
 * @param old_owner  MDS ID of the failed primary.
 * @param new_owner  MDS ID of the promoting standby.
 * @param count_out  Receives number of subtrees taken over.
 * @return MDS_OK on success (even if count_out == 0).
 */
enum mds_status subtree_map_take_over(struct subtree_map *map,
                                      uint32_t old_owner,
                                      uint32_t new_owner,
                                      uint32_t *count_out);



/* -----------------------------------------------------------------------
 * Per-subtree operation counters (advisory)
 *
 * Counters are advisory -- they track resolved subtree ownership,
 * not transient path strings.  Approximate due to concurrent access.
 * ----------------------------------------------------------------------- */

/**
 * @brief Atomically increment the op counter for a subtree.
 *
 * Called from compound dispatch on each operation after subtree
 * resolution.  Uses relaxed memory ordering for performance.
 *
 * @param map   Map handle.
 * @param path  Subtree path.
 * @return MDS_OK, MDS_ERR_NOTFOUND.
 */
enum mds_status subtree_map_inc_ops(struct subtree_map *map,
                                    const char *path);

/**
 * @brief Read the current op counter for a subtree (non-destructive).
 *
 * @param map        Map handle.
 * @param path       Subtree path.
 * @param count_out  Receives current op count.
 * @return MDS_OK, MDS_ERR_NOTFOUND.
 */
enum mds_status subtree_map_get_stats(const struct subtree_map *map,
                                      const char *path,
                                      uint64_t *count_out);

/**
 * @brief Atomically reset the op counter for a subtree to 0.
 *
 * @param map   Map handle.
 * @param path  Subtree path.
 * @return MDS_OK, MDS_ERR_NOTFOUND.
 */
enum mds_status subtree_map_reset_stats(struct subtree_map *map,
                                        const char *path);

/* -----------------------------------------------------------------------
 * Split/merge map-level primitives
 * ----------------------------------------------------------------------- */

/** Default maximum subtree split depth. */
#define SUBTREE_MAX_SPLIT_DEPTH 3

/**
 * @brief Validate whether a subtree split is allowed (map-level checks only).
 *
 * Checks:
 *   - child_path is not already a subtree root
 *   - child_path starts with parent_path (is a descendant)
 *   - parent_path is owned locally (by this MDS)
 *   - Depth from "/" to child_path does not exceed max_depth
 *
 * Does NOT check catalogue (existence, directory type, junction status).
 * Those checks belong in the orchestration layer (subtree_split.c).
 *
 * @param map         Map handle.
 * @param parent_path Parent subtree root (e.g. "/data").
 * @param child_path  Proposed child subtree root (e.g. "/data/hot").
 * @param max_depth   Maximum allowed depth (0 = use default).
 * @return MDS_OK if split is valid, MDS_ERR_EXISTS / MDS_ERR_INVAL /
 *         MDS_ERR_PERM on validation failure.
 */
enum mds_status subtree_map_validate_split(const struct subtree_map *map,
                                           const char *parent_path,
                                           const char *child_path,
                                           uint32_t max_depth);

/**
 * @brief Register a new subtree entry for a split child (map-only, no migration).
 *
 * Atomic insertion: inserts child_path with given owner, version=1,
 * state=SUBTREE_ACTIVE.  Fails if already present.
 *
 * @param map         Map handle.
 * @param parent_path Parent subtree (must exist and be locally owned).
 * @param child_path  New child subtree path.
 * @param owner       Owner MDS ID for the child.
 * @return MDS_OK, MDS_ERR_EXISTS, MDS_ERR_NOMEM.
 */
enum mds_status subtree_map_register_split(struct subtree_map *map,
                                           const char *parent_path,
                                           const char *child_path,
                                           uint32_t owner);

/**
 * @brief Remove a subtree entry from the map.
 *
 * Used by merge and split rollback.  Cannot remove the root subtree "/".
 *
 * @param map   Map handle.
 * @param path  Subtree path to remove.
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_INVAL (if path == "/").
 */
enum mds_status subtree_map_remove_subtree(struct subtree_map *map,
                                           const char *path);


/* -----------------------------------------------------------------------
 * Node registration (cluster membership integration)
 * ----------------------------------------------------------------------- */

/**
 * @brief Register or update a node's hostname in the node info table.
 *
 * Public wrapper around the internal register_node() helper.
 * Called by cluster_membership on node join to enable referral encoding
 * for that node's subtrees.
 *
 * @param map       Map handle.
 * @param mds_id    Node ID.
 * @param hostname  Node hostname.
 * @return MDS_OK, MDS_ERR_INVAL, MDS_ERR_NOMEM.
 */
enum mds_status subtree_map_register_node(struct subtree_map *map,
                                          uint32_t mds_id,
                                          const char *hostname);

/* Forward declaration for role-based ownership enforcement. */
struct cluster_membership;

/**
 * @brief Set the membership handle for role-based ownership enforcement.
 *
 * When set, subtree ownership mutations (set_owner, add, take_over)
 * reject standby nodes as owners.
 *
 * @param map         Map handle.
 * @param membership  Membership handle (may be NULL to disable checks).
 */
void subtree_map_set_membership(struct subtree_map *map,
                                const struct cluster_membership *membership);

/**
 * @brief Remove a node from the node info table (low-level helper).
 *
 * Called by cluster_membership on node leave.  Does not affect
 * subtree ownership entries.
 *
 * @pre Caller MUST have already drained all subtrees owned by
 *      @p mds_id (via migration) before calling this function.
 *      Calling with owned subtrees still present leaves orphaned
 *      ownership entries.
 *
 * @param map     Map handle.
 * @param mds_id  Node ID to remove.
 * @return MDS_OK, MDS_ERR_NOTFOUND.
 */
enum mds_status subtree_map_unregister_node(struct subtree_map *map,
                                            uint32_t mds_id);

/**
 * @brief Check if a node owns any subtree entries.
 *
 * Used by cluster_node_leave() to enforce the drain-first invariant:
 * a node cannot leave the cluster while it still owns subtrees.
 *
 * @param map     Map handle.
 * @param mds_id  Node ID to check.
 * @return true if at least one subtree has owner_mds_id == mds_id.
 */
bool subtree_map_node_owns_subtrees(const struct subtree_map *map,
                                    uint32_t mds_id);

/**
 * @brief Collect all subtree entries owned by a given node.
 *
 * Allocates and returns a malloc'd array of subtree_entry structs
 * whose owner_mds_id matches @p mds_id.  Caller must free the
 * returned array (may be NULL if count is 0).
 *
 * @param map       Map handle.
 * @param mds_id    Node ID to query.
 * @param[out] out  Receives malloc'd array (NULL if none).
 * @param[out] count Receives number of entries.
 * @return MDS_OK on success, MDS_ERR_INVAL on bad args,
 *         MDS_ERR_NOMEM on allocation failure.
 */
enum mds_status subtree_map_get_node_subtrees(
    const struct subtree_map *map,
    uint32_t mds_id,
    struct subtree_entry **out,
    uint32_t *count);

/**
 * @brief Snapshot all subtree entries.
 *
 * Allocates and returns a caller-owned array of all subtree entries
 * copied under the read lock in a single atomic snapshot.
 * Caller must free the returned array with free().
 *
 * @param map       Map handle.
 * @param[out] out  Receives malloc'd array (NULL if count==0).
 * @param[out] count Receives number of entries.
 * @return MDS_OK on success, MDS_ERR_INVAL on bad args,
 *         MDS_ERR_NOMEM on allocation failure.
 */
enum mds_status subtree_map_list_snapshot(
    const struct subtree_map *map,
    struct subtree_entry **out,
    uint32_t *count);

/**
 * @brief Check whether the subtree map backend is authoritative.
 *
 * Always returns false in RonDB mode.
 *
 */
bool subtree_map_is_authoritative(const struct subtree_map *map);

/* JSON serialisation (used by tests). */
int subtree_to_json(const struct subtree_entry *e,
                    char *buf, size_t cap);


/* -----------------------------------------------------------------------
 * Migration-specific ownership transfer
 *
 * Only succeeds when the subtree is in SUBTREE_MIGRATING state.
 * Used by migration_initiate() to commit ownership at the end of
 * a data-streaming migration.  Does NOT affect the general-purpose
 * subtree_map_set_owner(), which remains available for failover
 * and fenced-takeover paths.
 * ----------------------------------------------------------------------- */

/**
 * @brief Transfer subtree ownership, requiring SUBTREE_MIGRATING state.
 *
 * Looks up the entry by path; if its state is not SUBTREE_MIGRATING,
 * returns MDS_ERR_INVAL.  Otherwise delegates to the same backend as
 * subtree_map_set_owner() (local or etcd).
 *
 * @param map           Map handle.
 * @param path          Subtree root path.
 * @param new_owner     New owning MDS ID.
 * @param expected_ver  Expected version for CAS.
 * @return MDS_OK on success, MDS_ERR_INVAL if not migrating,
 *         MDS_ERR_NOTFOUND if path unknown, MDS_ERR_STALE on CAS fail.
 */
enum mds_status subtree_map_transfer_owner_if_migrating(
    struct subtree_map *map, const char *path,
    uint32_t new_owner, uint64_t expected_ver);


/**
 * @brief Failover-specific subtree takeover (supports etcd mode).
 *
 * Unlike subtree_map_take_over(), this function works in etcd mode
 * by enumerating entries owned by old_owner and CAS-updating each
 * one individually via etcd_set_owner_by_path().  It also bypasses
 * the owner_role_ok() check because the promoting standby is not
 * yet ACTIVE_SERVING at the time of takeover.
 *
 * In local mode, delegates to the existing subtree_map_take_over()
 * with role checks disabled.
 *
 * @param map        Map handle.
 * @param old_owner  MDS ID of the failed primary.
 * @param new_owner  MDS ID of the promoting standby.
 * @param count_out  Receives number of subtrees taken over.
 * @return MDS_OK on success, MDS_ERR_IO on etcd CAS failure.
 */
enum mds_status subtree_map_failover_take_over(struct subtree_map *map,
                                               uint32_t old_owner,
                                               uint32_t new_owner,
                                               uint32_t *count_out);

/**
 * @brief Freeze all subtrees owned by @a owner_id.
 *
 * Sets state -> SUBTREE_FROZEN on each ACTIVE subtree owned by the
 * given node.  Used during controlled failover to quiesce I/O before
 * the replication sync barrier.
 *
 * @param map       Map handle.
 * @param owner_id  MDS node ID whose subtrees to freeze.
 * @return MDS_OK on success, MDS_ERR_IO on CAS failure.
 */
enum mds_status subtree_map_freeze_owned(struct subtree_map *map,
                                         uint32_t owner_id);

/**
 * @brief Unfreeze all subtrees owned by @a owner_id.
 *
 * Restores state SUBTREE_FROZEN -> SUBTREE_ACTIVE on each frozen
 * subtree owned by the given node.  Used for rollback if the sync
 * barrier fails during controlled failover.
 *
 * @param map       Map handle.
 * @param owner_id  MDS node ID whose subtrees to unfreeze.
 * @return MDS_OK on success, MDS_ERR_IO on CAS failure.
 */
enum mds_status subtree_map_unfreeze_owned(struct subtree_map *map,
                                           uint32_t owner_id);

#endif /* SUBTREE_MAP_H */

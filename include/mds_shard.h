/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_shard.h — Request-local shard routing for metadata operations.
 *
 * A shard owns one catalogue handle and one commit queue, covering
 * a subtree of the global namespace.  The shard map provides
 * longest-prefix-match lookup: given a file path, it returns the
 * shard whose subtree_path is the longest prefix of that path.
 *
 * Phase 2: single root shard wrapping the daemon-global cat+cq.
 * Phase 3: multi-shard with owned child shards created on split.
 *
 * See docs/architecture.md §4.7.7 for the sharding roadmap.
 */

#ifndef MDS_SHARD_H
#define MDS_SHARD_H
#include <stddef.h>

#include <stdint.h>
#include <stdbool.h>

#include "pnfs_mds.h"

struct mds_catalogue;
struct commit_queue;
struct repl_conn;

/* Maximum number of shards per MDS node. */
#define MDS_SHARD_MAX  64

/**
 * A single metadata shard.
 *
 * Heap-allocated and pointer-stable: once created, the address does
 * not change even if other shards are added or removed from the map.
 * This makes it safe for compound_data.current_shard to hold a pointer
 * across an entire COMPOUND while admin operations modify the map.
 *
 * Root shard: owned == false (daemon manages db/cq lifecycle).
 * Child shards: owned == true (shard map manages db/cq lifecycle).
 */
struct mds_shard {
	struct mds_catalogue *cat;  /**< Catalogue wrapper for this shard. */
	struct commit_queue  *cq;   /**< Commit queue for this shard. */
	char  subtree_path[MDS_MAX_PATH]; /**< Subtree root (e.g. "/", "/data"). */
	uint32_t shard_id;          /**< Monotonically increasing shard identifier. */
	bool     owned;             /**< true = shard map owns cat/cq lifecycle. */
};

/**
 * Opaque shard map handle.
 *
 * Thread-safe: all operations are protected by a pthread rwlock.
 * Readers (lookup) run concurrently; writers (add/remove) are exclusive.
 */
struct mds_shard_map;

/**
 * @brief Create an empty shard map.
 *
 * @param[out] out  Receives the new shard map handle.
 * @return 0 on success, -1 on allocation failure.
 */
int mds_shard_map_create(struct mds_shard_map **out);

/**
 * @brief Destroy a shard map and free its memory.
 *
 * For owned shards (owned == true), destroys their CQ and closes
 * their catalogue.  Non-owned shards (root shard) are left alone.
 *
 * @param map  Shard map handle (NULL tolerated).
 */
void mds_shard_map_destroy(struct mds_shard_map *map);

/**
 * @brief Add a non-owning shard for a subtree path.
 *
 * The shard map borrows db and cq — it does NOT free them on
 * destruction.  Used for the root shard and test shards.
 *
 * If a shard with the same subtree_path already exists, it is replaced.
 *
 * @param map   Shard map handle.
 * @param path  Subtree root path (e.g. "/", "/data").
 * @param cq    Commit queue (borrowed, not owned; may be NULL).
 * @return 0 on success, -1 on failure (full, or NULL args).
 */
int mds_shard_map_add(struct mds_shard_map *map,
		      const char *path,
		      struct commit_queue *cq);

/**
 * @brief Open and add an owned child shard.
 *
 * Creates a new catalogue + CQ, inserts into the map.  The shard map
 * owns these resources and will close them on mds_shard_map_close()
 * or mds_shard_map_destroy().
 *
 * @param map        Shard map handle.
 * @param path       Subtree root path for routing.
 * @param repl       Replication connection for the CQ (NULL = no repl).
 * @param repl_mode  Replication mode (enum mds_repl_mode).
 * @param[out] out   Receives pointer to the newly created shard.
 * @return 0 on success, -1 on failure.
 */
int mds_shard_map_open(struct mds_shard_map *map,
		       const char *path,
		       struct repl_conn *repl,
		       int repl_mode,
		       const struct mds_shard **out);
int mds_shard_map_open_with_id(struct mds_shard_map *map,
			       const char *path,
			       struct repl_conn *repl,
			       int repl_mode,
			       uint32_t shard_id,
			       const struct mds_shard **out);

/**
 * @brief Close and remove an owned child shard.
 *
 * Destroys the CQ, closes the catalogue, removes from the map, and
 * frees the shard object.  Only works for shards with owned == true.
 *
 * @param map   Shard map handle.
 * @param path  Subtree root path to close.
 * @return 0 on success, -1 if not found or not owned.
 */
int mds_shard_map_close(struct mds_shard_map *map, const char *path);

/**
 * @brief Remove a shard from the routing map without closing resources.
 *
 * For non-owned shards, this just removes routing.  For owned shards,
 * use mds_shard_map_close() instead.
 *
 * @param map   Shard map handle.
 * @param path  Subtree root path to remove.
 * @return 0 on success, -1 if not found.
 */
int mds_shard_map_remove(struct mds_shard_map *map, const char *path);

/**
 * @brief Lookup the shard for a given file path (longest prefix match).
 *
 * Returns a stable heap-allocated pointer that survives add/remove of
 * other shards.  Safe for compound_data to hold across a COMPOUND.
 *
 * @param map   Shard map handle (NULL returns NULL).
 * @param path  File path to resolve (e.g. "/data/foo/bar").
 * @return Pointer to the matching shard, or NULL if no match.
 */
const struct mds_shard *mds_shard_map_lookup(const struct mds_shard_map *map,
					     const char *path);

/**
 * @brief Lookup a shard by shard_id.
 *
 * Used for fileid->shard resolution (via shard_fileid_map DBI).
 *
 * @param map       Shard map handle (NULL returns NULL).
 * @param shard_id  Shard identifier to find.
 * @return Pointer to the shard, or NULL if not found.
 */
const struct mds_shard *mds_shard_map_lookup_by_id(
	const struct mds_shard_map *map,
	uint32_t shard_id);

/**
 * @brief Return the root ("/") shard.
 *
 * Convenience for callers that perform cluster-global operations
 * (DS registry, client recovery) not tied to a specific namespace path.
 *
 * @param map  Shard map handle.
 * @return Pointer to the root shard, or NULL if no root shard exists.
 */
const struct mds_shard *mds_shard_map_get_default(const struct mds_shard_map *map);

/**
 * @brief Return the number of shards in the map.
 *
 * @param map  Shard map handle (NULL returns 0).
 * @return Number of shards.
 */
uint32_t mds_shard_map_count(const struct mds_shard_map *map);

/**
 * Callback for mds_shard_map_for_each().
 *
 * @param shard  Current shard.
 * @param ctx    Opaque user context.
 * @return 0 to continue, non-zero to stop.
 */
typedef int (*mds_shard_cb)(const struct mds_shard *shard, void *ctx);

/**
 * @brief Iterate all shards in the map.
 *
 * Holds a read lock for the duration.  Callback must not modify the map.
 *
 * @param map  Shard map handle (NULL tolerated).
 * @param cb   Per-shard callback.
 * @param ctx  Opaque context.
 */
void mds_shard_map_for_each(const struct mds_shard_map *map,
			    mds_shard_cb cb, void *ctx);

/**
 * @brief Format the local data path for a shard ID.
 *
 * Uses the configured shard directory and a stable filename derived
 * from @a shard_id so both active restart and standby lazy-open can
 * materialise the same child shard locally.
 *
 * @param shard_dir  Base shard directory from config.
 * @param shard_id   Durable shard identifier (must be non-zero).
 * @param out        Receives the formatted path.
 * @param out_len    Size of @a out in bytes.
 * @return 0 on success, -1 on invalid input or truncation.
 */
int mds_shard_format_local_path(const char *shard_dir,
				uint32_t shard_id,
				char *out,
				size_t out_len);

/* -----------------------------------------------------------------------
 * Local shard data migration (Phase 3)
 * ----------------------------------------------------------------------- */

/**
 * @brief Migrate namespace data for a subtree from one local shard to another.
 *
 * Uses DFS iteration on src_shard catalogue and migration_apply_chunk()
 * on dst_shard catalogue.  After successful copy, deletes migrated
 * records from src_shard.  Populates root catalogue shard_fileid_map
 * for each fileid.
 *
 * @param src_shard          Source shard (namespace data read from here).
 * @param dst_shard          Destination shard (data written here).
 * @param root_cat           Root catalogue (for shard_fileid_map entries).
 * @param subtree_root_fileid Fileid of the subtree root directory.
 * @param dst_shard_id       Shard ID for fileid map entries.
 * @return MDS_OK on success, error code on failure.
 */
enum mds_status mds_shard_migrate_subtree(
	const struct mds_shard *src_shard,
	const struct mds_shard *dst_shard,
	struct mds_catalogue *root_cat,
	uint64_t subtree_root_fileid,
	uint32_t dst_shard_id);

#endif /* MDS_SHARD_H */

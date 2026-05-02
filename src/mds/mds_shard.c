/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_shard.c — Request-local shard routing for metadata operations.
 *
 * Implements the shard map: an array of heap-allocated mds_shard pointers
 * sorted by descending path length for longest-prefix-first lookup.
 * Shard pointers are stable across add/remove of other shards.
 *
 * See docs/architecture.md §4.7.7 for design context.
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>

#include "mds_shard.h"
#include "mds_catalogue.h"
#include "commit_queue.h"

/* -----------------------------------------------------------------------
 * Internal structure
 * ----------------------------------------------------------------------- */

struct mds_shard_map {
	pthread_rwlock_t    lock;
	struct mds_shard   *shards[MDS_SHARD_MAX]; /* Heap-allocated pointers. */
	uint32_t            count;
	uint32_t            next_id; /* Monotonic shard_id allocator. */
};

/* -----------------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------------- */

/**
 * Compare two shard pointers by descending path length (for qsort).
 * Longer paths sort first so that lookup finds the longest prefix
 * match on the first hit.
 */
static int cmp_ptr_path_len_desc(const void *a, const void *b)
{
	const struct mds_shard *sa = *(const struct mds_shard *const *)a;
	const struct mds_shard *sb = *(const struct mds_shard *const *)b;
	size_t la = strlen(sa->subtree_path);
	size_t lb = strlen(sb->subtree_path);

	if (la > lb) {
		return -1;
	}
	if (la < lb) {
		return 1;
	}
	return 0;
}

static int mds_shard_map_open_internal(struct mds_shard_map *map,
				       const char *path,


				       struct repl_conn *repl,
				       int repl_mode,
				       bool use_explicit_id,
				       uint32_t shard_id,
				       const struct mds_shard **out);

int mds_shard_map_open(struct mds_shard_map *map,
		       const char *path,


		       struct repl_conn *repl,
		       int repl_mode,
		       const struct mds_shard **out)
{
	return mds_shard_map_open_internal(map, path,
					   repl, repl_mode, false, 0, out);
}

int mds_shard_map_open_with_id(struct mds_shard_map *map,
			       const char *path,


			       struct repl_conn *repl,
			       int repl_mode,
			       uint32_t shard_id,
			       const struct mds_shard **out)
{
	if (shard_id == 0) {
		return -1;
	}
	return mds_shard_map_open_internal(map, path,
					   repl, repl_mode, true, shard_id,
					   out);
}

/**
 * Check whether @a prefix is a path prefix of @a path.
 *
 * "/" is a prefix of everything.  "/data" is a prefix of "/data",
 * "/data/", and "/data/foo" but NOT "/datafile".
 */
static bool is_path_prefix(const char *prefix, const char *path)
{
	size_t plen = strlen(prefix);

	if (plen == 0) {
		return false;
	}

	/* Root "/" matches everything. */
	if (plen == 1 && prefix[0] == '/') {
		return true;
	}

	if (strncmp(prefix, path, plen) != 0) {
		return false;
	}

	/* Exact match, or path continues with '/'. */
	if (path[plen] == '\0' || path[plen] == '/') {
		return true;
	}

	return false;
}

/** Re-sort the pointer array by descending path length. */
static void resort(struct mds_shard_map *map)
{
	if (map->count > 1) {
		qsort(/* NOLINT(bugprone-multi-level-implicit-pointer-conversion,bugprone-sizeof-expression) */
		      (void *)map->shards, map->count,
		      sizeof(struct mds_shard *),
		      cmp_ptr_path_len_desc);
	}
}

/** Find the index of a shard by subtree_path.  Returns count if not found. */
static uint32_t find_index(const struct mds_shard_map *map, const char *path)
{
	uint32_t i;

	for (i = 0; i < map->count; i++) {
		if (strcmp(map->shards[i]->subtree_path, path) == 0) {
			return i;
		}
	}
	return map->count;
}

/** Free a shard's owned resources (CQ + catalogue) if owned, then free the object. */
static void free_shard(struct mds_shard *s)
{
	if (s == NULL) {
		return;
	}
	if (s->owned) {
		commit_queue_destroy(s->cq);
		s->cq = NULL;
	}
	/* Free catalogue wrapper (struct only — db is freed above or
	 * by the caller for non-owned shards). */
	free(s->cat);
	s->cat = NULL;
	free(s);
}

int mds_shard_format_local_path(const char *shard_dir,
				uint32_t shard_id,
				char *out,
				size_t out_len)
{
	const char *sep = "/";
	size_t dir_len;
	int n;

	if (shard_dir == NULL || out == NULL || out_len == 0 || shard_id == 0) {
		return -1;
	}

	dir_len = strlen(shard_dir);
	if (dir_len > 0 && shard_dir[dir_len - 1] == '/') {
		sep = "";
	}

	n = snprintf(out, out_len, "%s%sshard-%u.mdb",
		     shard_dir, sep, shard_id);
	if (n < 0 || (size_t)n >= out_len) {
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int mds_shard_map_create(struct mds_shard_map **out)
{
	struct mds_shard_map *map;

	if (out == NULL) {
		return -1;
	}

	map = calloc(1, sizeof(*map));
	if (map == NULL) {
		return -1;
	}

	pthread_rwlock_init(&map->lock, NULL);
	map->next_id = 1;
	*out = map;
	return 0;
}

void mds_shard_map_destroy(struct mds_shard_map *map)
{
	uint32_t i;

	if (map == NULL) {
		return;
	}

	/* Free all shard objects.  Owned shards get their CQ+catalogue closed. */
	for (i = 0; i < map->count; i++) {
		free_shard(map->shards[i]);
		map->shards[i] = NULL;
	}
	map->count = 0;

	pthread_rwlock_destroy(&map->lock);
	free(map);
}

int mds_shard_map_add(struct mds_shard_map *map,
		      const char *path,
		      struct commit_queue *cq)
{
	uint32_t idx;

	if (map == NULL || path == NULL || path[0] == '\0') {
		return -1;
	}

	pthread_rwlock_wrlock(&map->lock);

	/* Replace existing entry with the same path. */
	idx = find_index(map, path);
	if (idx < map->count) {
		struct mds_shard *existing = map->shards[idx];
		existing->cq = cq;
		/* owned stays as-is for replacement; non-owned stays non-owned. */
		resort(map);
		pthread_rwlock_unlock(&map->lock);
		return 0;
	}

	/* Insert new entry. */
	if (map->count >= MDS_SHARD_MAX) {
		pthread_rwlock_unlock(&map->lock);
		return -1;
	}

	struct mds_shard *s = calloc(1, sizeof(*s));
	if (s == NULL) {
		pthread_rwlock_unlock(&map->lock);
		return -1;
	}

	s->cat = NULL;
	s->cq = cq;
	s->owned = false;
	s->shard_id = map->next_id++;
	(void)snprintf(s->subtree_path, sizeof(s->subtree_path),
		       "%s", path);

	map->shards[map->count] = s;
	map->count++;
	resort(map);

	pthread_rwlock_unlock(&map->lock);
	return 0;
}

static int mds_shard_map_open_internal(struct mds_shard_map *map,
				       const char *path,
				       struct repl_conn *repl,
				       int repl_mode,
				       bool use_explicit_id,
				       uint32_t shard_id,
				       const struct mds_shard **out)
{
	/* Shard open is not implemented (RonDB-only feature, not wired). */
	(void)map;
	(void)path;
	(void)repl;
	(void)repl_mode;
	(void)use_explicit_id;
	(void)shard_id;
	(void)out;
	return -1;
}

int mds_shard_map_close(struct mds_shard_map *map, const char *path)
{
	uint32_t idx;

	if (map == NULL || path == NULL) {
		return -1;
	}

	pthread_rwlock_wrlock(&map->lock);

	idx = find_index(map, path);
	if (idx >= map->count) {
		pthread_rwlock_unlock(&map->lock);
		return -1;
	}

	struct mds_shard *s = map->shards[idx];
	if (!s->owned) {
		pthread_rwlock_unlock(&map->lock);
		return -1; /* Cannot close a non-owned shard. */
	}

	/* Shift remaining entries down. Explicit byte-level
	 * arithmetic avoids void-pointer and multi-level
	 * implicit-pointer-conversion warnings. */
	if (idx + 1 < map->count) {
		uint8_t *base = (uint8_t *)map->shards;
		size_t sz = sizeof(struct mds_shard *);

		memmove(base + idx * sz,
			base + (idx + 1) * sz,
			(map->count - idx - 1) * sz);
	}
	map->shards[map->count - 1] = NULL;
	map->count--;

	pthread_rwlock_unlock(&map->lock);

	/* Free resources outside the lock. */
	free_shard(s);
	return 0;
}

int mds_shard_map_remove(struct mds_shard_map *map, const char *path)
{
	uint32_t idx;

	if (map == NULL || path == NULL) {
		return -1;
	}

	pthread_rwlock_wrlock(&map->lock);

	idx = find_index(map, path);
	if (idx >= map->count) {
		pthread_rwlock_unlock(&map->lock);
		return -1;
	}

	struct mds_shard *s = map->shards[idx];

	if (idx + 1 < map->count) {
		uint8_t *base = (uint8_t *)map->shards;
		size_t sz = sizeof(struct mds_shard *);

		memmove(base + idx * sz,
			base + (idx + 1) * sz,
			(map->count - idx - 1) * sz);
	}
	map->shards[map->count - 1] = NULL;
	map->count--;

	pthread_rwlock_unlock(&map->lock);

	/* Free the shard object.  For non-owned, this just frees the
	 * struct without touching db/cq. */
	free_shard(s);
	return 0;
}

const struct mds_shard *mds_shard_map_lookup(const struct mds_shard_map *map,
					     const char *path)
{
	uint32_t i;

	if (map == NULL || path == NULL || path[0] == '\0') {
		return NULL;
	}

	pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);

	/* Linear scan — entries are sorted longest-first, so the first
	 * match is the longest-prefix match. */
	for (i = 0; i < map->count; i++) {
		if (is_path_prefix(map->shards[i]->subtree_path, path)) {
			const struct mds_shard *result = map->shards[i];
			pthread_rwlock_unlock(
				(pthread_rwlock_t *)&map->lock);
			return result;
		}
	}

	pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
	return NULL;
}

const struct mds_shard *mds_shard_map_lookup_by_id(
	const struct mds_shard_map *map,
	uint32_t shard_id)
{
	uint32_t i;

	if (map == NULL) {
		return NULL;
	}

	pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);

	for (i = 0; i < map->count; i++) {
		if (map->shards[i]->shard_id == shard_id) {
			const struct mds_shard *result = map->shards[i];
			pthread_rwlock_unlock(
				(pthread_rwlock_t *)&map->lock);
			return result;
		}
	}

	pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
	return NULL;
}

const struct mds_shard *mds_shard_map_get_default(
	const struct mds_shard_map *map)
{
	if (map == NULL) {
		return NULL;
	}
	return mds_shard_map_lookup(map, "/");
}

uint32_t mds_shard_map_count(const struct mds_shard_map *map)
{
	uint32_t n;

	if (map == NULL) {
		return 0;
	}

	pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);
	n = map->count;
	pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
	return n;
}

void mds_shard_map_for_each(const struct mds_shard_map *map,
			    mds_shard_cb cb, void *ctx)
{
	uint32_t i;

	if (map == NULL || cb == NULL) {
		return;
	}

	pthread_rwlock_rdlock((pthread_rwlock_t *)&map->lock);

	for (i = 0; i < map->count; i++) {
		if (cb(map->shards[i], ctx) != 0) {
			break;
		}
	}

	pthread_rwlock_unlock((pthread_rwlock_t *)&map->lock);
}

/* -----------------------------------------------------------------------
 * Local shard data migration (Phase 3)
 *
 * Copies namespace tables (inodes, dirents, stripe_map, xattrs,
 * inline_data) from src to dst shard using the migration chunk
 * serialization format.  Populates the root-global shard_fileid_map
 * for each migrated fileid.
 *
 * This is the same-process equivalent of cross-MDS migration:
 * no network transport, same DFS iteration + apply pattern.
 * ----------------------------------------------------------------------- */

#include "migration.h"

/** DFS iteration callback context for local shard migration. */
struct local_mig_ctx {
	struct mds_catalogue *dst_cat;
	struct mds_catalogue *root_cat;
	uint32_t         dst_shard_id;
	uint32_t         count;
	enum mds_status  last_err;
};

/**
 * Per-inode callback: build a migration chunk from the source inode,
 * apply it to the destination shard, and record the fileid mapping.
 */
static int local_mig_inode_cb(const struct mig_inode_chunk *chunk, void *arg)
{
	struct local_mig_ctx *ctx = arg;
	enum mds_status st;

	/* Write inode + dirents + stripe_map + xattrs + inline_data to dst
	 * via the destination shard's catalogue handle. */
	st = migration_apply_chunk(ctx->dst_cat, chunk);
	if (st != MDS_OK) {
		ctx->last_err = st;
		return -1;
	}

	/* Record fileid → shard mapping in root-global index. */
	if (ctx->root_cat != NULL) {
		(void)mds_cat_shard_fileid_put(ctx->root_cat, NULL,
					      chunk->fileid,
					      ctx->dst_shard_id);
	}

	ctx->count++;
	return 0;
}

enum mds_status mds_shard_migrate_subtree(
	const struct mds_shard *src_shard,
	const struct mds_shard *dst_shard,
	struct mds_catalogue *root_cat,
	uint64_t subtree_root_fileid,
	uint32_t dst_shard_id)
{
	if (src_shard == NULL || dst_shard == NULL) {
		return MDS_ERR_INVAL;
	}

	struct local_mig_ctx ctx;
	memset(&ctx, 0, sizeof(ctx));
	ctx.dst_cat = dst_shard->cat;
	ctx.root_cat = root_cat;
	ctx.dst_shard_id = dst_shard_id;
	ctx.last_err = MDS_OK;

	/* DFS iteration over the source subtree via catalogue.
	 * mds_cat_subtree_iter reads inodes/dirents/stripe_map/xattrs/
	 * inline_data and calls the callback with a complete chunk. */
	if (src_shard->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	enum mds_status st = mds_cat_subtree_iter(
		src_shard->cat, subtree_root_fileid,
		local_mig_inode_cb, &ctx);

	if (st != MDS_OK && ctx.last_err == MDS_OK) {
		ctx.last_err = st;
	}

	return ctx.last_err;
}

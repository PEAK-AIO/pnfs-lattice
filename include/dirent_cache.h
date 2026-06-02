/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * dirent_cache.h -- In-memory directory entry LRU cache.
 *
 * Caches (parent_fileid, name) -> (child_fileid, child_type) to avoid
 * NDB reads on repeated LOOKUPs.  Supports negative entries (NOTFOUND)
 * with configurable TTL to suppress NDB reads for missing names
 * (e.g. .git/, .DS_Store probes).
 *
 * Thread-safe: 16 striped mutexes partitioned by hash of
 * (parent_fileid, name).  Each stripe has its own hash table
 * and LRU list, eliminating cross-stripe contention.
 */

#ifndef DIRENT_CACHE_H
#define DIRENT_CACHE_H

#include <stdint.h>
#include <stdbool.h>

/** Opaque cache handle. */
struct dirent_cache;

/**
 * Create a dirent cache.
 *
 * @param max_entries    Maximum cached entries (positive + negative).
 * @param neg_ttl_ms     TTL for negative entries in milliseconds (0 = 5000).
 * @param out            Receives the cache handle.
 * @return 0 on success, -1 on error.
 */
int dirent_cache_init(uint32_t max_entries, uint32_t neg_ttl_ms,
                      struct dirent_cache **out);

/**
 * Set the positive-entry TTL in milliseconds (0 = disabled, default).
 *
 * When non-zero, dirent_cache_get() treats a positive (name->fileid)
 * entry older than @pos_ttl_ms as a miss and evicts it, bounding how
 * long a stale name->fileid mapping can be served.  Negative entries
 * keep using neg_ttl_ms from dirent_cache_init().  Intended for
 * active-active (multi-MDS) deployments where a CREATE/REMOVE/RENAME
 * on a peer MDS does not invalidate this daemon's in-memory cache.
 *
 * Must be called at startup, before the cache is shared with worker
 * threads.
 */
void dirent_cache_set_pos_ttl_ms(struct dirent_cache *dc,
                                 uint32_t pos_ttl_ms);

/**
 * Look up a dirent by (parent, name).
 *
 * On hit: copies child_fileid and child_type to out params, returns 0.
 * On negative hit (cached NOTFOUND, within TTL): returns 1.
 * On miss (not cached or negative TTL expired): returns -1.
 *
 * @param dc              Cache handle.
 * @param parent_fileid   Parent directory fileid.
 * @param name            Entry name (null-terminated).
 * @param child_fileid    Out: child fileid (valid only when return == 0).
 * @param child_type      Out: child type (valid only when return == 0).
 * @return 0 = positive hit, 1 = negative hit, -1 = miss.
 */
int dirent_cache_get(struct dirent_cache *dc,
                     uint64_t parent_fileid, const char *name,
                     uint64_t *child_fileid, uint8_t *child_type);

/**
 * Insert or update a positive dirent entry.
 *
 * @return 0 on success, -1 on error.
 */
int dirent_cache_put(struct dirent_cache *dc,
                     uint64_t parent_fileid, const char *name,
                     uint64_t child_fileid, uint8_t child_type);

/**
 * Insert a negative (NOTFOUND) entry with TTL.
 *
 * @return 0 on success, -1 on error.
 */
int dirent_cache_put_negative(struct dirent_cache *dc,
                              uint64_t parent_fileid, const char *name);

/**
 * Read the current invalidation generation counter.
 *
 * The counter is bumped on every dirent_cache_invalidate and
 * dirent_cache_invalidate_parent call.  Callers that race a backend
 * lookup against a concurrent CREATE/REMOVE use it to detect that
 * the cache state they observed before the lookup may already be
 * stale.  See dirent_cache_put_negative_if_unchanged for the
 * intended pattern.
 */
uint64_t dirent_cache_read_gen(const struct dirent_cache *dc);

/**
 * Insert a negative (NOTFOUND) entry only when no invalidation has
 * happened since @gen_snapshot was taken.
 *
 * Closes the TOCTOU race where a concurrent CREATE commits to the
 * authoritative store between a backend NOTFOUND and the caller's
 * cache insertion -- without this guard the stale negative entry
 * would survive the CREATE's invalidate (because the invalidate
 * runs before the put, and finds no entry to remove) and would
 * shadow the new file for the negative-cache TTL window.
 *
 * Intended use:
 *
 *   uint64_t gen = dirent_cache_read_gen(dc);
 *   st = cat_lookup(...);
 *   if (st == NOTFOUND) {
 *       (void)dirent_cache_put_negative_if_unchanged(
 *           dc, parent, name, gen);
 *   }
 *
 * @return 0 if the entry was inserted, -1 if the generation
 *         changed (insert skipped) or any input was invalid.
 */
int dirent_cache_put_negative_if_unchanged(struct dirent_cache *dc,
                                           uint64_t parent_fileid,
                                           const char *name,
                                           uint64_t gen_snapshot);

/**
 * Remove a specific dirent entry (positive or negative).
 *
 * Called on CREATE, REMOVE, LINK for the affected (parent, name).
 */
void dirent_cache_invalidate(struct dirent_cache *dc,
                             uint64_t parent_fileid, const char *name);

/**
 * Remove ALL entries for a parent directory.
 *
 * Called on directory mutations that may affect ordering or
 * completeness (e.g. cross-MDS invalidation).
 *
 * Implementation: linear scan of the LRU list, O(n) in cache size.
 * Acceptable because parent-level invalidation is rare (only on
 * cross-MDS delta receipt).
 */
void dirent_cache_invalidate_parent(struct dirent_cache *dc,
                                    uint64_t parent_fileid);

/**
 * Return the current number of cached entries.
 */
uint32_t dirent_cache_count(const struct dirent_cache *dc);

/**
 * Destroy the cache and free all memory.  NULL-safe.
 */
void dirent_cache_destroy(struct dirent_cache *dc);

#endif /* DIRENT_CACHE_H */

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

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * inode_cache.h -- In-memory inode LRU cache (16-way striped).
 *
 * Hot inodes are cached to avoid catalogue reads on every operation.
 * The cache is write-through: callers are responsible for persisting
 * inode changes to the catalogue before (or after) calling inode_cache_put().
 *
 * Thread-safe: the cache is partitioned into 16 independent stripes
 * (selected by fileid hash), each guarded by its own mutex, so
 * concurrent operations on different stripes never contend.
 */

#ifndef INODE_CACHE_H
#define INODE_CACHE_H

#include <stdint.h>
#include "pnfs_mds.h"

/** Opaque cache handle. */
struct inode_cache;

/**
 * Create an inode cache.
 *
 * @param max_entries  Total cache budget in inodes.  The budget is
 *                     divided across 16 stripes (ceil(max/16) each);
 *                     when a stripe is full, its least-recently-used
 *                     entry is evicted.  Eviction is therefore
 *                     stripe-local: a skewed fileid distribution can
 *                     evict from a full stripe while others have room.
 * @param out          Receives the cache handle.
 * @return 0 on success, -1 on error (ENOMEM or invalid args).
 */
int inode_cache_init(uint32_t max_entries, struct inode_cache **out);

/**
 * Set the positive-entry TTL in milliseconds (0 = disabled, default).
 *
 * When non-zero, inode_cache_get() treats an entry older than @ttl_ms
 * as a miss (and evicts it), bounding how long a stale inode can be
 * served.  Intended for active-active (multi-MDS) deployments where a
 * mutation on a peer MDS does not invalidate this daemon's in-memory
 * cache; single-MDS deployments leave it 0 and keep entries valid
 * until explicit invalidation or LRU eviction.
 *
 * Must be called at startup, before the cache is shared with worker
 * threads; it is not intended to be called concurrently with
 * inode_cache_get/put.
 */
void inode_cache_set_ttl_ms(struct inode_cache *ic, uint32_t ttl_ms);

/**
 * Look up an inode by fileid.
 *
 * On a hit the entry is promoted to MRU position within its stripe
 * and the inode is copied into @inode.
 *
 * @return 0 on cache hit, -1 on miss.
 */
int inode_cache_get(struct inode_cache *ic, uint64_t fileid,
		    struct mds_inode *inode);

/**
 * Insert or update an inode in the cache.
 *
 * If an entry with the same fileid already exists it is updated and
 * promoted to MRU position.  If the target stripe is full its LRU
 * entry is evicted first.
 *
 * @return 0 on success, -1 on error.
 */
int inode_cache_put(struct inode_cache *ic, const struct mds_inode *inode);

/**
 * Remove an inode from the cache (if present).
 *
 * Used when a write operation has deleted the inode or when a
 * cross-MDS invalidation message is received.
 */
void inode_cache_invalidate(struct inode_cache *ic, uint64_t fileid);

/**
 * Return the current number of cached entries.
 */
uint32_t inode_cache_count(const struct inode_cache *ic);

/**
 * Destroy the cache and free all memory.
 *
 * Safe to call with NULL.
 */
void inode_cache_destroy(struct inode_cache *ic);

#endif /* INODE_CACHE_H */

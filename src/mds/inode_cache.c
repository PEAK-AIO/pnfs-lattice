/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * inode_cache.c — In-memory inode LRU cache.
 *
 * Hot inodes are cached to avoid catalogue reads on every operation.
 * Cache is invalidated on write-through updates and cross-MDS
 * invalidation messages.
 *
 * Implementation: chained hash table + doubly-linked LRU list.
 * Thread-safe via a single pthread_mutex (sufficient for the
 * expected contention level; shard later if profiling warrants).
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>

#include "pnfs_mds.h"
#include "inode_cache.h"

/* -----------------------------------------------------------------------
 * Internal data structures
 * ----------------------------------------------------------------------- */

struct cache_entry {
	uint64_t            fileid;
	struct mds_inode    inode;
	struct cache_entry *prev;      /* LRU list — towards tail (older) */
	struct cache_entry *next;      /* LRU list — towards head (newer) */
	struct cache_entry *hash_next; /* hash chain (singly linked) */
};

struct inode_cache {
	struct cache_entry **hash_table;
	uint32_t             hash_size;
	struct cache_entry  *lru_head; /* most recently used */
	struct cache_entry  *lru_tail; /* least recently used */
	uint32_t             count;
	uint32_t             max_entries;
	/* Single global mutex: the LRU list and count are shared state
	 * that cannot be protected by per-stripe locks without corruption.
	 * Shard the entire cache (separate hash+LRU per stripe) for real
	 * concurrency — tracked as a future optimization. */
	pthread_mutex_t      lock;
};

/* -----------------------------------------------------------------------
 * Hash helpers
 * ----------------------------------------------------------------------- */

static uint32_t hash_fileid(uint64_t fileid, uint32_t size)
{
	/* splitmix64 — same hash used in session.c and open_state.c. */
	uint64_t h = fileid;

	h ^= h >> 30;
	h *= 0xbf58476d1ce4e5b9ULL;
	h ^= h >> 27;
	h *= 0x94d049bb133111ebULL;
	h ^= h >> 31;
	return (uint32_t)(h % size);
}

/**
 * Walk hash chain for @fileid.  Returns the entry or NULL.
 * Caller must hold ic->lock.
 */
static struct cache_entry *hash_find(const struct inode_cache *ic,
				     uint64_t fileid)
{
	uint32_t bucket = hash_fileid(fileid, ic->hash_size);
	struct cache_entry *e;

	for (e = ic->hash_table[bucket]; e != NULL; e = e->hash_next) {
		if (e->fileid == fileid) {
			return e;
}
	}
	return NULL;
}

/** Insert @e at the head of its hash bucket.  Caller holds lock. */
static void hash_insert(struct inode_cache *ic, struct cache_entry *e)
{
	uint32_t bucket = hash_fileid(e->fileid, ic->hash_size);

	e->hash_next = ic->hash_table[bucket];
	ic->hash_table[bucket] = e;
}

/** Remove @e from its hash bucket.  Caller holds lock. */
static void hash_remove(struct inode_cache *ic, struct cache_entry *e)
{
	uint32_t bucket = hash_fileid(e->fileid, ic->hash_size);
	struct cache_entry **pp;

	for (pp = &ic->hash_table[bucket]; *pp != NULL;
	     pp = &(*pp)->hash_next) {
		if (*pp == e) {
			*pp = e->hash_next;
			e->hash_next = NULL;
			return;
		}
	}
}

/* -----------------------------------------------------------------------
 * LRU list helpers — caller must hold ic->lock
 * ----------------------------------------------------------------------- */

/** Unlink @e from the LRU doubly-linked list. */
static void lru_unlink(struct inode_cache *ic, struct cache_entry *e)
{
	if (e->next != NULL) {
		e->next->prev = e->prev;
	} else {
		ic->lru_tail = e->prev;
}

	if (e->prev != NULL) {
		e->prev->next = e->next;
	} else {
		ic->lru_head = e->next;
}

	e->prev = NULL;
	e->next = NULL;
}

/** Push @e to the front (MRU position). */
static void lru_push_front(struct inode_cache *ic, struct cache_entry *e)
{
	e->prev = NULL;
	e->next = ic->lru_head;

	if (ic->lru_head != NULL) {
		ic->lru_head->prev = e;
}
	ic->lru_head = e;

	if (ic->lru_tail == NULL) {
		ic->lru_tail = e;
}
}

/** Promote @e to MRU position (unlink + push front). */
static void lru_promote(struct inode_cache *ic, struct cache_entry *e)
{
	if (ic->lru_head == e) {
		return; /* already at front */
}
	lru_unlink(ic, e);
	lru_push_front(ic, e);
}

/**
 * Evict the LRU tail entry.  Removes from hash and LRU, frees memory.
 * Returns 0 on success, -1 if cache is empty.
 */
static int lru_evict_tail(struct inode_cache *ic)
{
	struct cache_entry *victim;

	victim = ic->lru_tail;
	if (victim == NULL) {
		return -1;
}

	hash_remove(ic, victim);
	lru_unlink(ic, victim);
	free(victim);
	ic->count--;
	return 0;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int inode_cache_init(uint32_t max_entries, struct inode_cache **out)
{
	struct inode_cache *ic;

	if (out == NULL || max_entries == 0) {
		return -1;
}

	ic = calloc(1, sizeof(*ic));
	if (ic == NULL) {
		return -1;
}

	ic->max_entries = max_entries;
	ic->hash_size   = max_entries * 2; /* load factor ~0.5 */
	/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
	ic->hash_table  = calloc(ic->hash_size, sizeof(struct cache_entry *));
	if (ic->hash_table == NULL) {
		free(ic);
		return -1;
	}

	pthread_mutex_init(&ic->lock, NULL);
	*out = ic;
	return 0;
}

int inode_cache_get(struct inode_cache *ic, uint64_t fileid,
		    struct mds_inode *inode)
{
	struct cache_entry *e;

	if (ic == NULL || inode == NULL) {
		return -1;
	}

	pthread_mutex_lock(&ic->lock);

	e = hash_find(ic, fileid);
	if (e == NULL) {
		pthread_mutex_unlock(&ic->lock);
		return -1; /* miss */
	}

	*inode = e->inode;
	lru_promote(ic, e);

	pthread_mutex_unlock(&ic->lock);
	return 0; /* hit */
}

int inode_cache_put(struct inode_cache *ic, const struct mds_inode *inode)
{
	struct cache_entry *e;

	if (ic == NULL || inode == NULL) {
		return -1;
	}

	pthread_mutex_lock(&ic->lock);

	/* Check if already cached — update + promote. */
	e = hash_find(ic, inode->fileid);
	if (e != NULL) {
		e->inode = *inode;
		lru_promote(ic, e);
		pthread_mutex_unlock(&ic->lock);
		return 0;
	}

	/* Evict LRU tail if at capacity. */
	if (ic->count >= ic->max_entries) {
		lru_evict_tail(ic);
	}

	/* Allocate new entry. */
	e = calloc(1, sizeof(*e));
	if (e == NULL) {
		pthread_mutex_unlock(&ic->lock);
		return -1;
	}

	e->fileid = inode->fileid;
	e->inode  = *inode;

	hash_insert(ic, e);
	lru_push_front(ic, e);
	ic->count++;

	pthread_mutex_unlock(&ic->lock);
	return 0;
}

void inode_cache_invalidate(struct inode_cache *ic, uint64_t fileid)
{
	struct cache_entry *e;

	if (ic == NULL) {
		return;
	}

	pthread_mutex_lock(&ic->lock);

	e = hash_find(ic, fileid);
	if (e != NULL) {
		hash_remove(ic, e);
		lru_unlink(ic, e);
		free(e);
		ic->count--;
	}

	pthread_mutex_unlock(&ic->lock);
}

uint32_t inode_cache_count(const struct inode_cache *ic)
{
	if (ic == NULL) {
		return 0;
}
	/* count is only modified under lock; reading uint32_t is atomic
	 * on all supported platforms (informational / test use only). */
	return ic->count;
}

void inode_cache_destroy(struct inode_cache *ic)
{
	struct cache_entry *e;
	struct cache_entry *next;

	if (ic == NULL) {
		return;
}

	e = ic->lru_head;
	while (e != NULL) {
		next = e->next;
		free(e);
		e = next;
	}

	pthread_mutex_destroy(&ic->lock);
	/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
	free(ic->hash_table);
	free(ic);
}

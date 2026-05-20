/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * inode_cache.c -- In-memory inode LRU cache.
 *
 * Hot inodes are cached to avoid catalogue reads on every operation.
 * Cache is invalidated on write-through updates and cross-MDS
 * invalidation messages.
 *
 * Implementation: 16-stripe hash table + per-stripe doubly-linked
 * LRU list, mirroring the dirent_cache layout.  Each stripe has its
 * own mutex, hash sub-table, LRU list, and entry count, so concurrent
 * operations on different stripes never contend.  Stripe selection
 * is by hash of fileid.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>

#include "pnfs_mds.h"
#include "inode_cache.h"

#define IC_STRIPES 16

/* -----------------------------------------------------------------------
 * Internal data structures
 * ----------------------------------------------------------------------- */

struct cache_entry {
	uint64_t            fileid;
	struct mds_inode    inode;
	struct cache_entry *prev;      /* LRU list -- towards tail (older) */
	struct cache_entry *next;      /* LRU list -- towards head (newer) */
	struct cache_entry *hash_next; /* hash chain (singly linked) */
};

/** Per-stripe partition.  Each stripe is fully independent. */
struct ic_stripe {
	struct cache_entry **hash_table;
	uint32_t             hash_size;
	struct cache_entry  *lru_head; /* most recently used */
	struct cache_entry  *lru_tail; /* least recently used */
	uint32_t             count;
	uint32_t             max_entries;
	pthread_mutex_t      lock;
};

struct inode_cache {
	struct ic_stripe stripes[IC_STRIPES];
};

/* -----------------------------------------------------------------------
 * Hash helpers
 * ----------------------------------------------------------------------- */

/** splitmix64 -- same hash used in dirent_cache.c / open_state.c. */
static uint64_t splitmix64(uint64_t x)
{
	x ^= x >> 30;
	x *= 0xbf58476d1ce4e5b9ULL;
	x ^= x >> 27;
	x *= 0x94d049bb133111ebULL;
	x ^= x >> 31;
	return x;
}

/** Select stripe from fileid. */
static uint32_t ic_stripe_idx(uint64_t fileid)
{
	return (uint32_t)(splitmix64(fileid) % IC_STRIPES);
}

/** Bucket within a stripe's hash table. */
static uint32_t ic_bucket(uint64_t fileid, uint32_t hash_size)
{
	return (uint32_t)(splitmix64(fileid) % hash_size);
}

/**
 * Walk hash chain for @fileid.  Returns the entry or NULL.
 * Caller must hold the stripe lock.
 */
static struct cache_entry *hash_find(const struct ic_stripe *st,
				     uint64_t fileid)
{
	uint32_t bucket = ic_bucket(fileid, st->hash_size);
	struct cache_entry *e;

	for (e = st->hash_table[bucket]; e != NULL; e = e->hash_next) {
		if (e->fileid == fileid) {
			return e;
		}
	}
	return NULL;
}

/** Insert @e at the head of its hash bucket.  Caller holds stripe lock. */
static void hash_insert(struct ic_stripe *st, struct cache_entry *e)
{
	uint32_t bucket = ic_bucket(e->fileid, st->hash_size);

	e->hash_next = st->hash_table[bucket];
	st->hash_table[bucket] = e;
}

/** Remove @e from its hash bucket.  Caller holds stripe lock. */
static void hash_remove(struct ic_stripe *st, struct cache_entry *e)
{
	uint32_t bucket = ic_bucket(e->fileid, st->hash_size);
	struct cache_entry **pp;

	for (pp = &st->hash_table[bucket]; *pp != NULL;
	     pp = &(*pp)->hash_next) {
		if (*pp == e) {
			*pp = e->hash_next;
			e->hash_next = NULL;
			return;
		}
	}
}

/* -----------------------------------------------------------------------
 * LRU list helpers -- caller must hold stripe lock
 * ----------------------------------------------------------------------- */

/** Unlink @e from the LRU doubly-linked list. */
static void lru_unlink(struct ic_stripe *st, struct cache_entry *e)
{
	if (e->next != NULL) {
		e->next->prev = e->prev;
	} else {
		st->lru_tail = e->prev;
	}

	if (e->prev != NULL) {
		e->prev->next = e->next;
	} else {
		st->lru_head = e->next;
	}

	e->prev = NULL;
	e->next = NULL;
}

/** Push @e to the front (MRU position). */
static void lru_push_front(struct ic_stripe *st, struct cache_entry *e)
{
	e->prev = NULL;
	e->next = st->lru_head;

	if (st->lru_head != NULL) {
		st->lru_head->prev = e;
	}
	st->lru_head = e;

	if (st->lru_tail == NULL) {
		st->lru_tail = e;
	}
}

/** Promote @e to MRU position (unlink + push front). */
static void lru_promote(struct ic_stripe *st, struct cache_entry *e)
{
	if (st->lru_head == e) {
		return; /* already at front */
	}
	lru_unlink(st, e);
	lru_push_front(st, e);
}

/**
 * Evict the LRU tail entry.  Removes from hash and LRU, frees memory.
 * Returns 0 on success, -1 if stripe is empty.
 */
static int lru_evict_tail(struct ic_stripe *st)
{
	struct cache_entry *victim;

	victim = st->lru_tail;
	if (victim == NULL) {
		return -1;
	}

	hash_remove(st, victim);
	lru_unlink(st, victim);
	free(victim);
	st->count--;
	return 0;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int inode_cache_init(uint32_t max_entries, struct inode_cache **out)
{
	struct inode_cache *ic;
	uint32_t per_stripe;
	uint32_t hash_per_stripe;

	if (out == NULL || max_entries == 0) {
		return -1;
	}

	ic = calloc(1, sizeof(*ic));
	if (ic == NULL) {
		return -1;
	}

	per_stripe = (max_entries + IC_STRIPES - 1) / IC_STRIPES;
	if (per_stripe == 0) {
		per_stripe = 1;
	}
	hash_per_stripe = per_stripe * 2; /* load factor ~0.5 */
	if (hash_per_stripe == 0) {
		hash_per_stripe = 1;
	}

	for (uint32_t i = 0; i < IC_STRIPES; i++) {
		struct ic_stripe *st = &ic->stripes[i];

		st->max_entries = per_stripe;
		st->hash_size = hash_per_stripe;
		/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
		st->hash_table = calloc(hash_per_stripe,
					sizeof(struct cache_entry *));
		if (st->hash_table == NULL) {
			/* Roll back already-allocated stripes. */
			for (uint32_t j = 0; j < i; j++) {
				/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
				free(ic->stripes[j].hash_table);
				pthread_mutex_destroy(&ic->stripes[j].lock);
			}
			free(ic);
			return -1;
		}
		pthread_mutex_init(&st->lock, NULL);
	}

	*out = ic;
	return 0;
}

int inode_cache_get(struct inode_cache *ic, uint64_t fileid,
		    struct mds_inode *inode)
{
	struct cache_entry *e;
	struct ic_stripe *st;
	uint32_t si;

	if (ic == NULL || inode == NULL) {
		return -1;
	}

	si = ic_stripe_idx(fileid);
	st = &ic->stripes[si];
	pthread_mutex_lock(&st->lock);

	e = hash_find(st, fileid);
	if (e == NULL) {
		pthread_mutex_unlock(&st->lock);
		return -1; /* miss */
	}

	*inode = e->inode;
	lru_promote(st, e);

	pthread_mutex_unlock(&st->lock);
	return 0; /* hit */
}

int inode_cache_put(struct inode_cache *ic, const struct mds_inode *inode)
{
	struct cache_entry *e;
	struct ic_stripe *st;
	uint32_t si;

	if (ic == NULL || inode == NULL) {
		return -1;
	}

	si = ic_stripe_idx(inode->fileid);
	st = &ic->stripes[si];
	pthread_mutex_lock(&st->lock);

	/* Check if already cached -- update + promote. */
	e = hash_find(st, inode->fileid);
	if (e != NULL) {
		e->inode = *inode;
		lru_promote(st, e);
		pthread_mutex_unlock(&st->lock);
		return 0;
	}

	/* Evict LRU tail if stripe is at capacity. */
	if (st->count >= st->max_entries) {
		lru_evict_tail(st);
	}

	/* Allocate new entry. */
	e = calloc(1, sizeof(*e));
	if (e == NULL) {
		pthread_mutex_unlock(&st->lock);
		return -1;
	}

	e->fileid = inode->fileid;
	e->inode  = *inode;

	hash_insert(st, e);
	lru_push_front(st, e);
	st->count++;

	pthread_mutex_unlock(&st->lock);
	return 0;
}

void inode_cache_invalidate(struct inode_cache *ic, uint64_t fileid)
{
	struct cache_entry *e;
	struct ic_stripe *st;
	uint32_t si;

	if (ic == NULL) {
		return;
	}

	si = ic_stripe_idx(fileid);
	st = &ic->stripes[si];
	pthread_mutex_lock(&st->lock);

	e = hash_find(st, fileid);
	if (e != NULL) {
		hash_remove(st, e);
		lru_unlink(st, e);
		free(e);
		st->count--;
	}

	pthread_mutex_unlock(&st->lock);
}

uint32_t inode_cache_count(const struct inode_cache *ic)
{
	uint32_t total = 0;

	if (ic == NULL) {
		return 0;
	}
	/* Per-stripe count is only modified under that stripe's lock;
	 * a sloppy read across all stripes without locking is fine for
	 * informational / test use (matches the legacy single-mutex
	 * behaviour where count was read without the lock too). */
	for (uint32_t i = 0; i < IC_STRIPES; i++) {
		total += ic->stripes[i].count;
	}
	return total;
}

void inode_cache_destroy(struct inode_cache *ic)
{
	if (ic == NULL) {
		return;
	}

	for (uint32_t si = 0; si < IC_STRIPES; si++) {
		struct ic_stripe *st = &ic->stripes[si];
		struct cache_entry *e = st->lru_head;

		while (e != NULL) {
			struct cache_entry *next = e->next;
			free(e);
			e = next;
		}
		pthread_mutex_destroy(&st->lock);
		/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
		free(st->hash_table);
	}
	free(ic);
}

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * inode_cache.c -- In-memory inode LRU cache (16-way striped).
 *
 * Hot inodes are cached to avoid catalogue reads on every operation.
 * Entries are dropped on write-through invalidation; an optional
 * positive-entry TTL (inode_cache_set_ttl_ms) additionally bounds how
 * long a stale inode can be served in active-active deployments, where
 * a peer MDS mutates the shared catalogue without notifying this cache.
 *
 * Implementation (Wave 3 T3.2): 16-stripe hash table + per-stripe
 * doubly-linked LRU list, mirroring dirent_cache.c.  Each stripe has
 * its own mutex, hash sub-table, LRU list, entry count, and capacity
 * (ceil(max_entries / IC_STRIPES)), so concurrent operations on
 * different stripes never contend.  Stripe selection hashes the fileid
 * with splitmix64.  The pre-Wave-3 implementation serialized every
 * lookup on one global mutex AND wrote the shared LRU list on every
 * hit; per-stripe locking removes that hot-path contention without
 * changing the write-through or TTL contract.
 *
 * Capacity note: the budget is divided across stripes, so a skewed
 * fileid distribution can evict from a full stripe while another
 * stripe has room -- total occupancy is bounded by
 * IC_STRIPES * ceil(max_entries / IC_STRIPES).  Acceptable for a
 * cache whose only correctness requirement is write-through
 * consistency (same trade-off as dirent_cache.c / layout_cache.c).
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>

#include "pnfs_mds.h"
#include "inode_cache.h"

#define IC_STRIPES 16

/* -----------------------------------------------------------------------
 * Internal data structures
 * ----------------------------------------------------------------------- */

struct cache_entry {
	uint64_t            fileid;
	struct mds_inode    inode;
	uint64_t            insert_ms;  /* monotonic ms at insert (TTL base) */
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
	/* Positive-entry TTL in ms (0 = disabled).  Set once at startup
	 * via inode_cache_set_ttl_ms() (documented as not concurrent
	 * with get/put); read under a stripe lock in _get. */
	uint32_t         ttl_ms;
};

/* -----------------------------------------------------------------------
 * Hash helpers
 * ----------------------------------------------------------------------- */

/** Monotonic millisecond clock for TTL accounting. */
static uint64_t monotonic_ms(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/** splitmix64 -- same hash used in session.c and dirent_cache.c. */
static uint64_t splitmix64(uint64_t x)
{
	x ^= x >> 30;
	x *= 0xbf58476d1ce4e5b9ULL;
	x ^= x >> 27;
	x *= 0x94d049bb133111ebULL;
	x ^= x >> 31;
	return x;
}

/** Select stripe from the fileid hash. */
static uint32_t ic_stripe_idx(uint64_t fileid)
{
	return (uint32_t)(splitmix64(fileid) % IC_STRIPES);
}

/** Bucket within a stripe's hash table. */
static uint32_t ic_bucket(uint64_t fileid, uint32_t hash_size)
{
	return (uint32_t)(splitmix64(fileid) % hash_size);
}

/* -----------------------------------------------------------------------
 * Per-stripe helpers -- caller must hold the stripe lock
 * ----------------------------------------------------------------------- */

/** Walk hash chain for @fileid.  Returns the entry or NULL. */
static struct cache_entry *ic_find(const struct ic_stripe *st,
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

/** Insert @e at the head of its hash bucket. */
static void ic_hash_insert(struct ic_stripe *st, struct cache_entry *e)
{
	uint32_t bucket = ic_bucket(e->fileid, st->hash_size);

	e->hash_next = st->hash_table[bucket];
	st->hash_table[bucket] = e;
}

/** Remove @e from its hash bucket. */
static void ic_hash_remove(struct ic_stripe *st, struct cache_entry *e)
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

/** Unlink @e from the stripe's LRU doubly-linked list. */
static void ic_lru_unlink(struct ic_stripe *st, struct cache_entry *e)
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
static void ic_lru_push_front(struct ic_stripe *st, struct cache_entry *e)
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
static void ic_lru_promote(struct ic_stripe *st, struct cache_entry *e)
{
	if (st->lru_head == e) {
		return; /* already at front */
	}
	ic_lru_unlink(st, e);
	ic_lru_push_front(st, e);
}

/** Remove @e from hash + LRU and free it. */
static void ic_evict_entry(struct ic_stripe *st, struct cache_entry *e)
{
	ic_hash_remove(st, e);
	ic_lru_unlink(st, e);
	free(e);
	st->count--;
}

/** Evict the stripe's LRU tail entry (no-op on an empty stripe). */
static void ic_evict_tail(struct ic_stripe *st)
{
	if (st->lru_tail != NULL) {
		ic_evict_entry(st, st->lru_tail);
	}
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int inode_cache_init(uint32_t max_entries, struct inode_cache **out)
{
	struct inode_cache *ic;
	uint32_t per_stripe;
	uint32_t hash_per_stripe;
	uint32_t i;

	if (out == NULL || max_entries == 0) {
		return -1;
	}

	ic = calloc(1, sizeof(*ic));
	if (ic == NULL) {
		return -1;
	}

	/* Divide the budget across stripes (ceil so max_entries=1
	 * still yields a usable one-entry stripe). */
	per_stripe = (max_entries + IC_STRIPES - 1) / IC_STRIPES;
	hash_per_stripe = per_stripe * 2; /* load factor ~0.5 */
	if (hash_per_stripe == 0) {
		hash_per_stripe = 1;
	}

	for (i = 0; i < IC_STRIPES; i++) {
		struct ic_stripe *st = &ic->stripes[i];

		st->max_entries = per_stripe;
		st->hash_size = hash_per_stripe;
		/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
		st->hash_table = calloc(hash_per_stripe,
					sizeof(struct cache_entry *));
		if (st->hash_table == NULL) {
			uint32_t j;

			/* Rollback already-allocated stripes. */
			for (j = 0; j < i; j++) {
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
	struct ic_stripe *st;
	struct cache_entry *e;

	if (ic == NULL || inode == NULL) {
		return -1;
	}

	st = &ic->stripes[ic_stripe_idx(fileid)];
	pthread_mutex_lock(&st->lock);

	e = ic_find(st, fileid);
	if (e == NULL) {
		pthread_mutex_unlock(&st->lock);
		return -1; /* miss */
	}

	/* Positive-entry TTL: bound cross-MDS staleness.  When ttl_ms is
	 * set and this entry has aged past it, evict and report a miss so
	 * the caller re-reads the authoritative catalogue (which a peer
	 * MDS may have mutated without invalidating this local cache). */
	if (ic->ttl_ms != 0 &&
	    (monotonic_ms() - e->insert_ms) > (uint64_t)ic->ttl_ms) {
		ic_evict_entry(st, e);
		pthread_mutex_unlock(&st->lock);
		return -1; /* expired -> miss */
	}

	*inode = e->inode;
	ic_lru_promote(st, e);

	pthread_mutex_unlock(&st->lock);
	return 0; /* hit */
}

int inode_cache_put(struct inode_cache *ic, const struct mds_inode *inode)
{
	struct ic_stripe *st;
	struct cache_entry *e;

	if (ic == NULL || inode == NULL) {
		return -1;
	}

	st = &ic->stripes[ic_stripe_idx(inode->fileid)];
	pthread_mutex_lock(&st->lock);

	/* Check if already cached -- update + promote. */
	e = ic_find(st, inode->fileid);
	if (e != NULL) {
		e->inode = *inode;
		e->insert_ms = monotonic_ms();
		ic_lru_promote(st, e);
		pthread_mutex_unlock(&st->lock);
		return 0;
	}

	/* Evict the stripe's LRU tail if the stripe is at capacity. */
	if (st->count >= st->max_entries) {
		ic_evict_tail(st);
	}

	/* Allocate new entry. */
	e = calloc(1, sizeof(*e));
	if (e == NULL) {
		pthread_mutex_unlock(&st->lock);
		return -1;
	}

	e->fileid = inode->fileid;
	e->inode  = *inode;
	e->insert_ms = monotonic_ms();

	ic_hash_insert(st, e);
	ic_lru_push_front(st, e);
	st->count++;

	pthread_mutex_unlock(&st->lock);
	return 0;
}

void inode_cache_invalidate(struct inode_cache *ic, uint64_t fileid)
{
	struct ic_stripe *st;
	struct cache_entry *e;

	if (ic == NULL) {
		return;
	}

	st = &ic->stripes[ic_stripe_idx(fileid)];
	pthread_mutex_lock(&st->lock);

	e = ic_find(st, fileid);
	if (e != NULL) {
		ic_evict_entry(st, e);
	}

	pthread_mutex_unlock(&st->lock);
}

void inode_cache_set_ttl_ms(struct inode_cache *ic, uint32_t ttl_ms)
{
	if (ic == NULL) {
		return;
	}

	/* Startup-only by contract (see inode_cache.h): callers set the
	 * TTL before the cache is shared with worker threads, so a plain
	 * store is safe -- there is no single global lock to take in the
	 * striped layout.  Readers observe it under their stripe lock. */
	ic->ttl_ms = ttl_ms;
}

uint32_t inode_cache_count(const struct inode_cache *ic)
{
	uint32_t total = 0;
	uint32_t i;

	if (ic == NULL) {
		return 0;
	}
	/* Per-stripe counts are only modified under the stripe lock;
	 * the unlocked sum is a point-in-time approximation
	 * (informational / test use only, as before). */
	for (i = 0; i < IC_STRIPES; i++) {
		total += ic->stripes[i].count;
	}
	return total;
}

void inode_cache_destroy(struct inode_cache *ic)
{
	uint32_t i;

	if (ic == NULL) {
		return;
	}

	for (i = 0; i < IC_STRIPES; i++) {
		struct ic_stripe *st = &ic->stripes[i];
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

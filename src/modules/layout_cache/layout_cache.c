/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * layout_cache.c -- per-inode stripe map cache.
 *
 * 16 shards, each protected by a pthread_mutex.  Within each shard:
 *   - a chained hash table (SHARD_HASH_BUCKETS=256 slots) keyed by fileid
 *   - a doubly-linked LRU list with a sentinel node
 *
 * Shard selection: fileid & (LAYOUT_CACHE_SHARDS - 1) (power-of-two mask).
 * Bucket selection: splitmix64(fileid) & (SHARD_HASH_BUCKETS - 1).
 *
 * On get: copy entry to a fresh heap buffer (caller owns, must free()).
 * On put: copy caller's buffer internally (caller retains its own).
 * On eviction: free the internal copy.
 */
#include "layout_cache.h"

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define SHARD_HASH_BUCKETS  256u
#define SHARD_HASH_MASK     (SHARD_HASH_BUCKETS - 1u)
#define SHARD_IDX(fid)      ((uint32_t)((fid) & (LAYOUT_CACHE_SHARDS - 1u)))

static uint32_t bucket_of(uint64_t fid)
{
    uint64_t h = fid ^ (fid >> 30);
    h *= 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 27;
    h *= 0x94d049bb133111ebULL;
    h ^= h >> 31;
    return (uint32_t)(h & SHARD_HASH_MASK);
}

struct lce {
    uint64_t                  fileid;
    uint32_t                  stripe_count;
    uint32_t                  stripe_unit;
    uint32_t                  mirror_count;
    struct mds_ds_map_entry  *entries;   /* heap copy; size = stripe_count * mirror_count */
    struct lce               *hash_next; /* chained hash collision list */
    struct lce               *lru_prev;  /* doubly-linked LRU; MRU end = after sentinel */
    struct lce               *lru_next;
};

struct shard {
    pthread_mutex_t  mu;
    struct lce      *buckets[SHARD_HASH_BUCKETS];
    struct lce       lru_sentinel;  /* sentinel.next -> MRU; sentinel.prev -> LRU */
    uint32_t         capacity;
    uint32_t         count;
    uint64_t         hits;
    uint64_t         misses;
    uint64_t         puts;
    uint64_t         evictions;
    uint64_t         invalidations;
};

struct layout_cache {
    struct shard shards[LAYOUT_CACHE_SHARDS];
};

/* ---- LRU helpers ---- */

static void lru_unlink(struct lce *e)
{
    e->lru_prev->lru_next = e->lru_next;
    e->lru_next->lru_prev = e->lru_prev;
}

/* Insert e immediately after pos (makes e the new MRU when pos = sentinel). */
static void lru_insert_after(struct lce *pos, struct lce *e)
{
    e->lru_next          = pos->lru_next;
    e->lru_prev          = pos;
    pos->lru_next->lru_prev = e;
    pos->lru_next        = e;
}

static void lru_promote(struct shard *sh, struct lce *e)
{
    lru_unlink(e);
    lru_insert_after(&sh->lru_sentinel, e);
}

/* Returns the LRU tail, or NULL if the list is empty. */
static struct lce *lru_tail(struct shard *sh)
{
    struct lce *tail = sh->lru_sentinel.lru_prev;
    return (tail == &sh->lru_sentinel) ? NULL : tail;
}

/* ---- hash helpers ---- */

static struct lce *hash_find(struct shard *sh, uint64_t fileid)
{
    struct lce *e = sh->buckets[bucket_of(fileid)];
    for (; e != NULL; e = e->hash_next) {
        if (e->fileid == fileid) {
            return e;
        }
    }
    return NULL;
}

static void hash_remove(struct shard *sh, struct lce *e)
{
    struct lce **p = &sh->buckets[bucket_of(e->fileid)];
    while (*p != NULL && *p != e) {
        p = &(*p)->hash_next;
    }
    if (*p != NULL) {
        *p = e->hash_next;
    }
    e->hash_next = NULL;
}

static void hash_insert(struct shard *sh, struct lce *e)
{
    uint32_t b = bucket_of(e->fileid);
    e->hash_next   = sh->buckets[b];
    sh->buckets[b] = e;
}

/* ---- shard helpers ---- */

static void shard_evict_lru(struct shard *sh)
{
    struct lce *v = lru_tail(sh);
    if (v == NULL) {
        return;
    }
    lru_unlink(v);
    hash_remove(sh, v);
    free(v->entries);
    free(v);
    sh->count--;
    sh->evictions++;
}

static void shard_free_entry(struct shard *sh, struct lce *e)
{
    lru_unlink(e);
    hash_remove(sh, e);
    free(e->entries);
    free(e);
    sh->count--;
}

/* ---- public API ---- */

int layout_cache_init(uint32_t max_entries, struct layout_cache **out)
{
    struct layout_cache *lc;
    uint32_t per_shard, i;

    if (out == NULL || max_entries == 0) {
        return -1;
    }
    *out = NULL;

    lc = calloc(1, sizeof(*lc));
    if (lc == NULL) {
        return -1;
    }

    per_shard = (max_entries + LAYOUT_CACHE_SHARDS - 1u) / LAYOUT_CACHE_SHARDS;
    if (per_shard < 4u) {
        per_shard = 4u;
    }

    for (i = 0; i < LAYOUT_CACHE_SHARDS; i++) {
        struct shard *sh = &lc->shards[i];
        if (pthread_mutex_init(&sh->mu, NULL) != 0) {
            uint32_t j;
            for (j = 0; j < i; j++) {
                pthread_mutex_destroy(&lc->shards[j].mu);
            }
            free(lc);
            return -1;
        }
        sh->lru_sentinel.lru_prev = &sh->lru_sentinel;
        sh->lru_sentinel.lru_next = &sh->lru_sentinel;
        sh->capacity = per_shard;
    }

    *out = lc;
    return 0;
}

void layout_cache_destroy(struct layout_cache *lc)
{
    uint32_t i;
    if (lc == NULL) {
        return;
    }
    for (i = 0; i < LAYOUT_CACHE_SHARDS; i++) {
        struct shard *sh = &lc->shards[i];
        struct lce *e = sh->lru_sentinel.lru_next;
        while (e != &sh->lru_sentinel) {
            struct lce *next = e->lru_next;
            free(e->entries);
            free(e);
            e = next;
        }
        pthread_mutex_destroy(&sh->mu);
    }
    free(lc);
}

int layout_cache_get(struct layout_cache *lc, uint64_t fileid,
                     uint32_t *stripe_count, uint32_t *stripe_unit,
                     uint32_t *mirror_count,
                     struct mds_ds_map_entry **entries)
{
    struct shard *sh;
    struct lce *e;
    size_t entry_bytes;
    struct mds_ds_map_entry *copy;

    if (lc == NULL || entries == NULL) {
        return -1;
    }
    *entries = NULL;

    sh = &lc->shards[SHARD_IDX(fileid)];
    pthread_mutex_lock(&sh->mu);

    e = hash_find(sh, fileid);
    if (e == NULL) {
        sh->misses++;
        pthread_mutex_unlock(&sh->mu);
        return -1;
    }

    entry_bytes = (size_t)e->stripe_count * (size_t)e->mirror_count
                  * sizeof(struct mds_ds_map_entry);
    copy = malloc(entry_bytes);
    if (copy == NULL) {
        sh->misses++;
        pthread_mutex_unlock(&sh->mu);
        return -1;
    }
    memcpy(copy, e->entries, entry_bytes);

    if (stripe_count != NULL) { *stripe_count = e->stripe_count; }
    if (stripe_unit  != NULL) { *stripe_unit  = e->stripe_unit;  }
    if (mirror_count != NULL) { *mirror_count = e->mirror_count; }
    *entries = copy;

    lru_promote(sh, e);
    sh->hits++;

    pthread_mutex_unlock(&sh->mu);
    return 0;
}

int layout_cache_put(struct layout_cache *lc, uint64_t fileid,
                     uint32_t stripe_count, uint32_t stripe_unit,
                     uint32_t mirror_count,
                     const struct mds_ds_map_entry *entries)
{
    struct shard *sh;
    struct lce *e;
    size_t entry_bytes;
    struct mds_ds_map_entry *copy;

    if (lc == NULL || entries == NULL || stripe_count == 0 ||
        stripe_count > MDS_MAX_STRIPES || mirror_count == 0 ||
        mirror_count > MDS_MAX_MIRRORS) {
        return -1;
    }

    entry_bytes = (size_t)stripe_count * (size_t)mirror_count
                  * sizeof(struct mds_ds_map_entry);
    copy = malloc(entry_bytes);
    if (copy == NULL) {
        return -1;
    }
    memcpy(copy, entries, entry_bytes);

    sh = &lc->shards[SHARD_IDX(fileid)];
    pthread_mutex_lock(&sh->mu);

    e = hash_find(sh, fileid);
    if (e != NULL) {
        free(e->entries);
        e->entries      = copy;
        e->stripe_count = stripe_count;
        e->stripe_unit  = stripe_unit;
        e->mirror_count = mirror_count;
        lru_promote(sh, e);
        sh->puts++;
        pthread_mutex_unlock(&sh->mu);
        return 0;
    }

    if (sh->count >= sh->capacity) {
        shard_evict_lru(sh);
    }

    e = calloc(1, sizeof(*e));
    if (e == NULL) {
        pthread_mutex_unlock(&sh->mu);
        free(copy);
        return -1;
    }
    e->fileid       = fileid;
    e->stripe_count = stripe_count;
    e->stripe_unit  = stripe_unit;
    e->mirror_count = mirror_count;
    e->entries      = copy;

    hash_insert(sh, e);
    lru_insert_after(&sh->lru_sentinel, e);
    sh->count++;
    sh->puts++;

    pthread_mutex_unlock(&sh->mu);
    return 0;
}

void layout_cache_invalidate(struct layout_cache *lc, uint64_t fileid)
{
    struct shard *sh;
    struct lce *e;

    if (lc == NULL) {
        return;
    }
    sh = &lc->shards[SHARD_IDX(fileid)];
    pthread_mutex_lock(&sh->mu);
    e = hash_find(sh, fileid);
    if (e != NULL) {
        shard_free_entry(sh, e);
        sh->invalidations++;
    }
    pthread_mutex_unlock(&sh->mu);
}

void layout_cache_clear(struct layout_cache *lc)
{
    uint32_t i;
    if (lc == NULL) {
        return;
    }
    for (i = 0; i < LAYOUT_CACHE_SHARDS; i++) {
        struct shard *sh = &lc->shards[i];
        pthread_mutex_lock(&sh->mu);
        while (sh->count > 0) {
            shard_evict_lru(sh);
        }
        pthread_mutex_unlock(&sh->mu);
    }
}

void layout_cache_stats_get(const struct layout_cache *lc,
                            struct layout_cache_stats *out)
{
    uint32_t i;
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (lc == NULL) {
        return;
    }
    for (i = 0; i < LAYOUT_CACHE_SHARDS; i++) {
        struct shard *sh = (struct shard *)&lc->shards[i];
        pthread_mutex_lock(&sh->mu);
        out->hits          += sh->hits;
        out->misses        += sh->misses;
        out->puts          += sh->puts;
        out->evictions     += sh->evictions;
        out->invalidations += sh->invalidations;
        out->entry_count   += sh->count;
        pthread_mutex_unlock(&sh->mu);
    }
}

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * dirent_cache.c -- In-memory directory entry LRU cache.
 *
 * Caches (parent_fileid, name) -> (child_fileid, child_type).
 * Negative entries (NOTFOUND) use child_fileid == 0 with a
 * monotonic timestamp for TTL-based expiry.
 *
 * Implementation: 16-stripe hash table + per-stripe doubly-linked
 * LRU list.  Each stripe has its own mutex, hash sub-table, LRU
 * list, and entry count, so concurrent operations on different
 * stripes never contend.  Stripe selection is by hash of
 * (parent_fileid, name).
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>

#include "pnfs_mds.h"
#include "dirent_cache.h"

#define DIRENT_CACHE_DEFAULT_NEG_TTL_MS 5000
#define DC_STRIPES 16

/* -----------------------------------------------------------------------
 * Internal data structures
 * ----------------------------------------------------------------------- */

struct dc_entry {
    uint64_t         parent_fileid;
    char             name[MDS_MAX_NAME + 1];
    uint64_t         child_fileid;  /* 0 = negative entry */
    uint8_t          child_type;
    uint64_t         insert_ms;     /* monotonic ms (for negative TTL) */
    struct dc_entry *prev;          /* LRU list -- towards tail (older) */
    struct dc_entry *next;          /* LRU list -- towards head (newer) */
    struct dc_entry *hash_next;     /* hash chain (singly linked) */
};

/** Per-stripe partition.  Each stripe is fully independent. */
struct dc_stripe {
    struct dc_entry **hash_table;
    uint32_t          hash_size;
    struct dc_entry  *lru_head;     /* most recently used */
    struct dc_entry  *lru_tail;     /* least recently used */
    uint32_t          count;
    uint32_t          max_entries;
    pthread_mutex_t   lock;
};

struct dirent_cache {
    struct dc_stripe  stripes[DC_STRIPES];
    uint32_t          neg_ttl_ms;
};

/* -----------------------------------------------------------------------
 * Hash helpers
 * ----------------------------------------------------------------------- */

/** splitmix64 -- same hash used in inode_cache.c. */
static uint64_t splitmix64(uint64_t x)
{
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
}

/** FNV-1a for short strings. */
static uint64_t fnv1a(const char *s)
{
    uint64_t h = 0xcbf29ce484222325ULL;

    for (; *s != '\0'; s++) {
        h ^= (uint64_t)(uint8_t)*s;
        h *= 0x100000001b3ULL;
    }
    return h;
}

/** Combined hash of (parent, name). */
static uint64_t dc_combined_hash(uint64_t parent, const char *name)
{
    return splitmix64(parent) ^ fnv1a(name);
}

/** Select stripe from combined hash. */
static uint32_t dc_stripe_idx(uint64_t parent, const char *name)
{
    return (uint32_t)(dc_combined_hash(parent, name) % DC_STRIPES);
}

/** Bucket within a stripe's hash table. */
static uint32_t dc_bucket(uint64_t parent, const char *name,
                          uint32_t hash_size)
{
    return (uint32_t)(dc_combined_hash(parent, name) % hash_size);
}

static uint64_t monotonic_ms(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

/* -----------------------------------------------------------------------
 * Per-stripe helpers -- caller must hold stripe lock
 * ----------------------------------------------------------------------- */

static struct dc_entry *dc_find(const struct dc_stripe *st,
                                uint64_t parent, const char *name)
{
    uint32_t bucket = dc_bucket(parent, name, st->hash_size);
    struct dc_entry *e;

    for (e = st->hash_table[bucket]; e != NULL; e = e->hash_next) {
        if (e->parent_fileid == parent &&
            strcmp(e->name, name) == 0) {
            return e;
        }
    }
    return NULL;
}

static void dc_hash_insert(struct dc_stripe *st, struct dc_entry *e)
{
    uint32_t bucket = dc_bucket(e->parent_fileid, e->name,
                                st->hash_size);

    e->hash_next = st->hash_table[bucket];
    st->hash_table[bucket] = e;
}

static void dc_hash_remove(struct dc_stripe *st, struct dc_entry *e)
{
    uint32_t bucket = dc_bucket(e->parent_fileid, e->name,
                                st->hash_size);
    struct dc_entry **pp;

    for (pp = &st->hash_table[bucket]; *pp != NULL;
         pp = &(*pp)->hash_next) {
        if (*pp == e) {
            *pp = e->hash_next;
            e->hash_next = NULL;
            return;
        }
    }
}

static void dc_lru_unlink(struct dc_stripe *st, struct dc_entry *e)
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

static void dc_lru_push_front(struct dc_stripe *st, struct dc_entry *e)
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

static void dc_lru_promote(struct dc_stripe *st, struct dc_entry *e)
{
    if (st->lru_head == e) {
        return;
    }
    dc_lru_unlink(st, e);
    dc_lru_push_front(st, e);
}

static void dc_evict_entry(struct dc_stripe *st, struct dc_entry *e)
{
    dc_hash_remove(st, e);
    dc_lru_unlink(st, e);
    free(e);
    st->count--;
}

static void dc_evict_tail(struct dc_stripe *st)
{
    if (st->lru_tail != NULL) {
        dc_evict_entry(st, st->lru_tail);
    }
}

/* -----------------------------------------------------------------------
 * Internal put helper (positive or negative)
 * ----------------------------------------------------------------------- */

static int dc_put_internal(struct dirent_cache *dc,
                           uint64_t parent, const char *name,
                           uint64_t child_fid, uint8_t child_type)
{
    uint32_t si = dc_stripe_idx(parent, name);
    struct dc_stripe *st = &dc->stripes[si];
    struct dc_entry *e;

    pthread_mutex_lock(&st->lock);

    /* Update existing entry if present. */
    e = dc_find(st, parent, name);
    if (e != NULL) {
        e->child_fileid = child_fid;
        e->child_type   = child_type;
        e->insert_ms    = (child_fid == 0) ? monotonic_ms() : 0;
        dc_lru_promote(st, e);
        pthread_mutex_unlock(&st->lock);
        return 0;
    }

    /* Evict LRU if at capacity. */
    if (st->count >= st->max_entries) {
        dc_evict_tail(st);
    }

    e = calloc(1, sizeof(*e));
    if (e == NULL) {
        pthread_mutex_unlock(&st->lock);
        return -1;
    }

    e->parent_fileid = parent;
    (void)snprintf(e->name, sizeof(e->name), "%s", name);
    e->child_fileid = child_fid;
    e->child_type   = child_type;
    e->insert_ms    = (child_fid == 0) ? monotonic_ms() : 0;

    dc_hash_insert(st, e);
    dc_lru_push_front(st, e);
    st->count++;

    pthread_mutex_unlock(&st->lock);
    return 0;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int dirent_cache_init(uint32_t max_entries, uint32_t neg_ttl_ms,
                      struct dirent_cache **out)
{
    struct dirent_cache *dc;
    uint32_t per_stripe;
    uint32_t hash_per_stripe;

    if (out == NULL || max_entries == 0) {
        return -1;
    }

    dc = calloc(1, sizeof(*dc));
    if (dc == NULL) {
        return -1;
    }

    dc->neg_ttl_ms = (neg_ttl_ms > 0)
                    ? neg_ttl_ms
                    : DIRENT_CACHE_DEFAULT_NEG_TTL_MS;

    per_stripe = (max_entries + DC_STRIPES - 1) / DC_STRIPES;
    hash_per_stripe = per_stripe * 2; /* load factor ~0.5 */
    if (hash_per_stripe == 0) {
        hash_per_stripe = 1;
    }

    for (uint32_t i = 0; i < DC_STRIPES; i++) {
        struct dc_stripe *st = &dc->stripes[i];
        st->max_entries = per_stripe;
        st->hash_size = hash_per_stripe;
        /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
        st->hash_table = calloc(hash_per_stripe,
                                sizeof(struct dc_entry *));
        if (st->hash_table == NULL) {
            /* Rollback already-allocated stripes. */
            for (uint32_t j = 0; j < i; j++) {
                /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
                free(dc->stripes[j].hash_table);
                pthread_mutex_destroy(&dc->stripes[j].lock);
            }
            free(dc);
            return -1;
        }
        pthread_mutex_init(&st->lock, NULL);
    }

    *out = dc;
    return 0;
}

int dirent_cache_get(struct dirent_cache *dc,
                     uint64_t parent_fileid, const char *name,
                     uint64_t *child_fileid, uint8_t *child_type)
{
    struct dc_entry *e;
    uint32_t si;
    struct dc_stripe *st;

    if (dc == NULL || name == NULL) {
        return -1;
    }

    si = dc_stripe_idx(parent_fileid, name);
    st = &dc->stripes[si];
    pthread_mutex_lock(&st->lock);

    e = dc_find(st, parent_fileid, name);
    if (e == NULL) {
        pthread_mutex_unlock(&st->lock);
        return -1; /* miss */
    }

    /* Negative entry -- check TTL. */
    if (e->child_fileid == 0) {
        uint64_t now = monotonic_ms();
        if ((now - e->insert_ms) > dc->neg_ttl_ms) {
            /* Expired: evict and treat as miss. */
            dc_evict_entry(st, e);
            pthread_mutex_unlock(&st->lock);
            return -1;
        }
        dc_lru_promote(st, e);
        pthread_mutex_unlock(&st->lock);
        return 1; /* negative hit */
    }

    /* Positive hit. */
    if (child_fileid != NULL) {
        *child_fileid = e->child_fileid;
    }
    if (child_type != NULL) {
        *child_type = e->child_type;
    }
    dc_lru_promote(st, e);

    pthread_mutex_unlock(&st->lock);
    return 0;
}

int dirent_cache_put(struct dirent_cache *dc,
                     uint64_t parent_fileid, const char *name,
                     uint64_t child_fileid, uint8_t child_type)
{
    if (dc == NULL || name == NULL || child_fileid == 0) {
        return -1;
    }
    return dc_put_internal(dc, parent_fileid, name,
                           child_fileid, child_type);
}

int dirent_cache_put_negative(struct dirent_cache *dc,
                              uint64_t parent_fileid, const char *name)
{
    if (dc == NULL || name == NULL) {
        return -1;
    }
    return dc_put_internal(dc, parent_fileid, name, 0, 0);
}

void dirent_cache_invalidate(struct dirent_cache *dc,
                             uint64_t parent_fileid, const char *name)
{
    struct dc_entry *e;
    uint32_t si;
    struct dc_stripe *st;

    if (dc == NULL || name == NULL) {
        return;
    }

    si = dc_stripe_idx(parent_fileid, name);
    st = &dc->stripes[si];
    pthread_mutex_lock(&st->lock);

    e = dc_find(st, parent_fileid, name);
    if (e != NULL) {
        dc_evict_entry(st, e);
    }

    pthread_mutex_unlock(&st->lock);
}

void dirent_cache_invalidate_parent(struct dirent_cache *dc,
                                    uint64_t parent_fileid)
{
    if (dc == NULL) {
        return;
    }

    /* Scan each stripe independently (one lock at a time). */
    for (uint32_t si = 0; si < DC_STRIPES; si++) {
        struct dc_stripe *st = &dc->stripes[si];
        struct dc_entry *e;
        struct dc_entry *next;

        pthread_mutex_lock(&st->lock);
        for (e = st->lru_head; e != NULL; e = next) {
            next = e->next;
            if (e->parent_fileid == parent_fileid) {
                dc_evict_entry(st, e);
            }
        }
        pthread_mutex_unlock(&st->lock);
    }
}

uint32_t dirent_cache_count(const struct dirent_cache *dc)
{
    uint32_t total = 0;

    if (dc == NULL) {
        return 0;
    }
    for (uint32_t i = 0; i < DC_STRIPES; i++) {
        total += dc->stripes[i].count;
    }
    return total;
}

void dirent_cache_destroy(struct dirent_cache *dc)
{
    if (dc == NULL) {
        return;
    }

    for (uint32_t si = 0; si < DC_STRIPES; si++) {
        struct dc_stripe *st = &dc->stripes[si];
        struct dc_entry *e = st->lru_head;
        while (e != NULL) {
            struct dc_entry *next = e->next;
            free(e);
            e = next;
        }
        pthread_mutex_destroy(&st->lock);
        /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
        free(st->hash_table);
    }
    free(dc);
}

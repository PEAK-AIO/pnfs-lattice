/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * lease_table.c -- Stripe lease table implementation.
 *
 * Sharded hash table (16 shards) keyed on fileid.  Each shard is
 * protected by a pthread_mutex.  Entries are stored in a singly-linked
 * bucket chain; expired entries are lazily evicted during lookup and
 * conflict-check operations.
 *
 * Memory:
 *   sizeof(stripe_lease_table) = 16 * (mutex + pointer)  ~1 KiB
 *   Each lease entry is ~56 bytes.  At 10K concurrent leases the
 *   table consumes ~600 KiB -- negligible for an MDS process.
 */

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#include "lease_table.h"

/* -----------------------------------------------------------------------
 * Internal types
 * ----------------------------------------------------------------------- */

#define SLT_SHARD_COUNT 16
#define SLT_SHARD_MASK  (SLT_SHARD_COUNT - 1)

struct slt_entry {
    struct slt_entry *next;
    uint64_t fileid;
    uint64_t clientid;
    uint64_t offset;
    uint64_t length;
    uint64_t expiry_ns;  /* CLOCK_MONOTONIC nanoseconds */
};

struct slt_shard {
    pthread_mutex_t   lock;
    struct slt_entry *head;
};

struct stripe_lease_table {
    struct slt_shard shards[SLT_SHARD_COUNT];
};

/* -----------------------------------------------------------------------
 * Helpers
 * ----------------------------------------------------------------------- */

static uint32_t shard_index(uint64_t fileid)
{
    /* Mix fileid bits for a reasonable distribution across 16 shards. */
    return (uint32_t)((fileid ^ (fileid >> 16)) & SLT_SHARD_MASK);
}

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/** Check whether two byte ranges overlap. */
static bool ranges_overlap(uint64_t a_off, uint64_t a_len,
                           uint64_t b_off, uint64_t b_len)
{
    /* Whole-file sentinel: UINT64_MAX length means "everything". */
    if (a_len == UINT64_MAX || b_len == UINT64_MAX) {
        return true;
    }
    uint64_t a_end = a_off + a_len;
    uint64_t b_end = b_off + b_len;

    /* Guard against wraparound. */
    if (a_end < a_off) { a_end = UINT64_MAX; }
    if (b_end < b_off) { b_end = UINT64_MAX; }

    return (a_off < b_end) && (b_off < a_end);
}

/**
 * Evict expired entries from the shard's chain.  Must be called with
 * the shard lock held.  Returns the number of entries evicted.
 */
static uint32_t shard_evict_expired(struct slt_shard *sh, uint64_t now)
{
    uint32_t evicted = 0;
    struct slt_entry **pp = &sh->head;
    while (*pp != NULL) {
        struct slt_entry *e = *pp;
        if (e->expiry_ns <= now) {
            *pp = e->next;
            free(e);
            evicted++;
        } else {
            pp = &e->next;
        }
    }
    return evicted;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int stripe_lease_table_init(struct stripe_lease_table **out)
{
    if (out == NULL) {
        return -1;
    }
    struct stripe_lease_table *tbl = calloc(1, sizeof(*tbl));
    if (tbl == NULL) {
        return -1;
    }
    for (uint32_t i = 0; i < SLT_SHARD_COUNT; i++) {
        pthread_mutex_init(&tbl->shards[i].lock, NULL);
        tbl->shards[i].head = NULL;
    }
    *out = tbl;
    return 0;
}

void stripe_lease_table_destroy(struct stripe_lease_table *tbl)
{
    if (tbl == NULL) {
        return;
    }
    for (uint32_t i = 0; i < SLT_SHARD_COUNT; i++) {
        struct slt_shard *sh = &tbl->shards[i];
        pthread_mutex_lock(&sh->lock);
        struct slt_entry *e = sh->head;
        while (e != NULL) {
            struct slt_entry *next = e->next;
            free(e);
            e = next;
        }
        sh->head = NULL;
        pthread_mutex_unlock(&sh->lock);
        pthread_mutex_destroy(&sh->lock);
    }
    free(tbl);
}

bool stripe_lease_check_conflict(struct stripe_lease_table *tbl,
                                 uint64_t fileid,
                                 uint64_t clientid,
                                 uint64_t offset,
                                 uint64_t length)
{
    if (tbl == NULL) {
        return false;
    }
    uint32_t idx = shard_index(fileid);
    struct slt_shard *sh = &tbl->shards[idx];
    uint64_t ts = now_ns();
    bool conflict = false;

    pthread_mutex_lock(&sh->lock);
    (void)shard_evict_expired(sh, ts);

    for (struct slt_entry *e = sh->head; e != NULL; e = e->next) {
        if (e->fileid != fileid) {
            continue;
        }
        /* Same client: no conflict (renewal path). */
        if (e->clientid == clientid) {
            continue;
        }
        if (ranges_overlap(e->offset, e->length, offset, length)) {
            conflict = true;
            break;
        }
    }
    pthread_mutex_unlock(&sh->lock);
    return conflict;
}

int stripe_lease_acquire(struct stripe_lease_table *tbl,
                         uint64_t fileid,
                         uint64_t clientid,
                         uint64_t offset,
                         uint64_t length,
                         uint32_t duration_ms)
{
    if (tbl == NULL || duration_ms == 0) {
        return -1;
    }
    uint32_t idx = shard_index(fileid);
    struct slt_shard *sh = &tbl->shards[idx];
    uint64_t ts = now_ns();
    uint64_t expiry = ts + (uint64_t)duration_ms * 1000000ULL;

    pthread_mutex_lock(&sh->lock);
    (void)shard_evict_expired(sh, ts);

    /* Upsert: look for existing entry with same key. */
    for (struct slt_entry *e = sh->head; e != NULL; e = e->next) {
        if (e->fileid == fileid && e->offset == offset) {
            /* Replace in-place (renew or ownership change). */
            e->clientid  = clientid;
            e->length    = length;
            e->expiry_ns = expiry;
            pthread_mutex_unlock(&sh->lock);
            return 0;
        }
    }

    /* Insert new entry at head. */
    struct slt_entry *ne = calloc(1, sizeof(*ne));
    if (ne == NULL) {
        pthread_mutex_unlock(&sh->lock);
        return -1;
    }
    ne->fileid    = fileid;
    ne->clientid  = clientid;
    ne->offset    = offset;
    ne->length    = length;
    ne->expiry_ns = expiry;
    ne->next      = sh->head;
    sh->head      = ne;

    pthread_mutex_unlock(&sh->lock);
    return 0;
}

void stripe_lease_release(struct stripe_lease_table *tbl,
                          uint64_t fileid,
                          uint64_t clientid,
                          uint64_t offset)
{
    if (tbl == NULL) {
        return;
    }
    uint32_t idx = shard_index(fileid);
    struct slt_shard *sh = &tbl->shards[idx];

    pthread_mutex_lock(&sh->lock);
    struct slt_entry **pp = &sh->head;
    while (*pp != NULL) {
        struct slt_entry *e = *pp;
        if (e->fileid == fileid &&
            e->clientid == clientid &&
            e->offset == offset) {
            *pp = e->next;
            free(e);
            break;
        }
        pp = &e->next;
    }
    pthread_mutex_unlock(&sh->lock);
}

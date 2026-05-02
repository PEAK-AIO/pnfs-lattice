/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * io_tracker.c — Per-file I/O frequency tracker.
 *
 * Open-addressing hash table with atomic per-bucket counters.
 * record()/bump() take a read lock (concurrent updates OK via atomics).
 * snapshot_and_reset() takes a write lock (exclusive, brief).
 */

#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>

#include "io_tracker.h"

/* ----------------------------------------------------------------------- *
 * Constants
 * ----------------------------------------------------------------------- */

#define IOT_EMPTY_FILEID  0   /* Sentinel: slot is unused. */
#define IOT_LOAD_FACTOR   75  /* Resize at 75% occupancy. */

/* ----------------------------------------------------------------------- *
 * Bucket
 * ----------------------------------------------------------------------- */

struct iot_bucket {
    _Atomic uint64_t fileid;
    _Atomic uint64_t io_bytes;
    _Atomic uint32_t bumps;
};

/* ----------------------------------------------------------------------- *
 * Tracker
 * ----------------------------------------------------------------------- */

struct io_tracker {
    pthread_rwlock_t    lock;
    struct iot_bucket  *buckets;
    uint32_t            capacity;  /* Always a power of 2. */
    uint32_t            mask;      /* capacity - 1. */
    _Atomic uint32_t    count;     /* Occupied slots. */
};

/* ----------------------------------------------------------------------- *
 * Helpers
 * ----------------------------------------------------------------------- */

static uint32_t next_pow2(uint32_t v)
{
    if (v == 0) {
        return 16;
    }
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
}

static uint64_t fid_hash(uint64_t fid)
{
    /* Splitmix-style mix. */
    fid ^= fid >> 30;
    fid *= 0xbf58476d1ce4e5b9ULL;
    fid ^= fid >> 27;
    fid *= 0x94d049bb133111ebULL;
    fid ^= fid >> 31;
    return fid;
}

/**
 * Find or insert a slot for @p fileid.
 * Caller must hold at least a read lock.
 * Returns NULL if the table is full (should not happen at <75% load).
 */
static struct iot_bucket *find_or_insert(struct io_tracker *iot,
                                         uint64_t fileid)
{
    uint32_t idx = (uint32_t)(fid_hash(fileid) & iot->mask);
    for (uint32_t i = 0; i < iot->capacity; i++) {
        struct iot_bucket *b = &iot->buckets[idx];
        uint64_t cur = atomic_load_explicit(&b->fileid,
                                             memory_order_relaxed);
        if (cur == fileid) {
            return b;
        }
        if (cur == IOT_EMPTY_FILEID) {
            /* Try to claim this slot. */
            uint64_t expected = IOT_EMPTY_FILEID;
            if (atomic_compare_exchange_strong_explicit(
                    &b->fileid, &expected, fileid,
                    memory_order_acq_rel, memory_order_relaxed)) {
                atomic_fetch_add_explicit(&iot->count, 1,
                                           memory_order_relaxed);
                return b;
            }
            /* Someone else claimed it — re-check. */
            if (atomic_load_explicit(&b->fileid,
                                      memory_order_relaxed) == fileid) {
                return b;
            }
        }
        idx = (idx + 1) & iot->mask;
    }
    return NULL; /* Table full — should not happen. */
}

/* ----------------------------------------------------------------------- *
 * Resize (called under write lock when load factor exceeded)
 * ----------------------------------------------------------------------- */

static int resize(struct io_tracker *iot)
{
    uint32_t new_cap = iot->capacity * 2;
    struct iot_bucket *new_buckets = calloc(new_cap, sizeof(*new_buckets));
    if (new_buckets == NULL) {
        return -1;
    }
    uint32_t new_mask = new_cap - 1;

    for (uint32_t i = 0; i < iot->capacity; i++) {
        uint64_t fid = atomic_load(&iot->buckets[i].fileid);
        if (fid == IOT_EMPTY_FILEID) {
            continue;
        }
        uint32_t idx = (uint32_t)(fid_hash(fid) & new_mask);
        while (atomic_load(&new_buckets[idx].fileid) != IOT_EMPTY_FILEID) {
            idx = (idx + 1) & new_mask;
        }
        atomic_store(&new_buckets[idx].fileid, fid);
        atomic_store(&new_buckets[idx].io_bytes,
                     atomic_load(&iot->buckets[i].io_bytes));
        atomic_store(&new_buckets[idx].bumps,
                     atomic_load(&iot->buckets[i].bumps));
    }

    free(iot->buckets);
    iot->buckets = new_buckets;
    iot->capacity = new_cap;
    iot->mask = new_mask;
    return 0;
}

/* ----------------------------------------------------------------------- *
 * Public API
 * ----------------------------------------------------------------------- */

int io_tracker_init(uint32_t initial_capacity, struct io_tracker **out)
{
    if (out == NULL) {
        return -1;
    }
    *out = NULL;

    struct io_tracker *iot = calloc(1, sizeof(*iot));
    if (iot == NULL) {
        return -1;
    }

    iot->capacity = next_pow2(initial_capacity < 64 ? 64 : initial_capacity);
    iot->mask = iot->capacity - 1;
    iot->buckets = calloc(iot->capacity, sizeof(*iot->buckets));
    if (iot->buckets == NULL) {
        free(iot);
        return -1;
    }

    if (pthread_rwlock_init(&iot->lock, NULL) != 0) {
        free(iot->buckets);
        free(iot);
        return -1;
    }

    atomic_store(&iot->count, 0);
    *out = iot;
    return 0;
}

void io_tracker_destroy(struct io_tracker *iot)
{
    if (iot == NULL) {
        return;
    }
    pthread_rwlock_destroy(&iot->lock);
    free(iot->buckets);
    free(iot);
}

void io_tracker_record(struct io_tracker *iot, uint64_t fileid,
                       uint64_t read_bytes, uint64_t write_bytes)
{
    if (iot == NULL || fileid == IOT_EMPTY_FILEID) {
        return;
    }

    pthread_rwlock_rdlock(&iot->lock);

    /* Check load factor — if near limit, upgrade to write lock and resize. */
    uint32_t cnt = atomic_load_explicit(&iot->count, memory_order_relaxed);
    if (cnt * 100 >= iot->capacity * IOT_LOAD_FACTOR) {
        pthread_rwlock_unlock(&iot->lock);
        pthread_rwlock_wrlock(&iot->lock);
        cnt = atomic_load_explicit(&iot->count, memory_order_relaxed);
        if (cnt * 100 >= iot->capacity * IOT_LOAD_FACTOR) {
            (void)resize(iot);
        }
        pthread_rwlock_unlock(&iot->lock);
        pthread_rwlock_rdlock(&iot->lock);
    }

    struct iot_bucket *b = find_or_insert(iot, fileid);
    if (b != NULL) {
        atomic_fetch_add_explicit(&b->io_bytes, read_bytes + write_bytes,
                                   memory_order_relaxed);
    }

    pthread_rwlock_unlock(&iot->lock);
}

void io_tracker_bump(struct io_tracker *iot, uint64_t fileid)
{
    if (iot == NULL || fileid == IOT_EMPTY_FILEID) {
        return;
    }

    pthread_rwlock_rdlock(&iot->lock);

    uint32_t cnt = atomic_load_explicit(&iot->count, memory_order_relaxed);
    if (cnt * 100 >= iot->capacity * IOT_LOAD_FACTOR) {
        pthread_rwlock_unlock(&iot->lock);
        pthread_rwlock_wrlock(&iot->lock);
        cnt = atomic_load_explicit(&iot->count, memory_order_relaxed);
        if (cnt * 100 >= iot->capacity * IOT_LOAD_FACTOR) {
            (void)resize(iot);
        }
        pthread_rwlock_unlock(&iot->lock);
        pthread_rwlock_rdlock(&iot->lock);
    }

    struct iot_bucket *b = find_or_insert(iot, fileid);
    if (b != NULL) {
        atomic_fetch_add_explicit(&b->bumps, 1, memory_order_relaxed);
    }

    pthread_rwlock_unlock(&iot->lock);
}

int io_tracker_snapshot_and_reset(struct io_tracker *iot,
                                  struct io_tracker_entry **out,
                                  uint32_t *count)
{
    if (iot == NULL || out == NULL || count == NULL) {
        return -1;
    }

    pthread_rwlock_wrlock(&iot->lock);

    uint32_t n = atomic_load(&iot->count);
    if (n == 0) {
        pthread_rwlock_unlock(&iot->lock);
        *out = NULL;
        *count = 0;
        return 0;
    }

    struct io_tracker_entry *entries = malloc(n * sizeof(*entries));
    if (entries == NULL) {
        pthread_rwlock_unlock(&iot->lock);
        return -1;
    }

    uint32_t idx = 0;
    for (uint32_t i = 0; i < iot->capacity && idx < n; i++) {
        uint64_t fid = atomic_load(&iot->buckets[i].fileid);
        if (fid == IOT_EMPTY_FILEID) {
            continue;
        }
        entries[idx].fileid = fid;
        entries[idx].io_bytes = atomic_load(&iot->buckets[i].io_bytes);
        entries[idx].layoutget_bumps = atomic_load(&iot->buckets[i].bumps);
        idx++;

        /* Reset slot. */
        atomic_store(&iot->buckets[i].fileid, IOT_EMPTY_FILEID);
        atomic_store(&iot->buckets[i].io_bytes, (uint64_t)0);
        atomic_store(&iot->buckets[i].bumps, (uint32_t)0);
    }

    atomic_store(&iot->count, 0);
    pthread_rwlock_unlock(&iot->lock);

    *out = entries;
    *count = idx;
    return 0;
}

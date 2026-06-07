/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * lease_table.c -- Per-(DS-stripe) lease table implementation.
 *
 * Sharded hash table (16 shards) keyed by mix(fileid, stripe_index).
 * Each shard is protected by a pthread_mutex.  Entries are kept in a
 * singly-linked bucket chain; expired entries are lazily evicted
 * during lookup and conflict-check operations.
 *
 * Shard hash:
 *   mix = fileid ^ (stripe_index * 0x9e3779b97f4a7c15)
 *   idx = (mix ^ (mix >> 16)) & SLT_SHARD_MASK
 *
 * Spreading a single fileid across multiple shards (one per stripe
 * index) gives wide-stripe layouts shard-level concurrency on the
 * table itself.
 */

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#include "lease_table.h"

#define SLT_SHARD_COUNT 16
#define SLT_SHARD_MASK  (SLT_SHARD_COUNT - 1)

struct slt_entry {
    struct slt_entry *next;
    uint64_t fileid;
    uint64_t clientid;
    uint64_t ds_offset;
    uint64_t ds_length;
    uint64_t expiry_ns;
    uint32_t stripe_index;
    uint32_t ds_id;
};

struct slt_shard {
    pthread_mutex_t   lock;
    struct slt_entry *head;
};

struct stripe_lease_table {
    struct slt_shard shards[SLT_SHARD_COUNT];
};

static uint32_t shard_index(uint64_t fileid, uint32_t stripe_index)
{
    const uint64_t GR = 0x9e3779b97f4a7c15ULL;
    uint64_t mix = fileid ^ ((uint64_t)stripe_index * GR);
    return (uint32_t)((mix ^ (mix >> 16)) & SLT_SHARD_MASK);
}

static uint64_t now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static bool ranges_overlap(uint64_t a_off, uint64_t a_len,
                           uint64_t b_off, uint64_t b_len)
{
    uint64_t a_end = a_off + a_len;
    uint64_t b_end = b_off + b_len;
    if (a_end < a_off) { a_end = UINT64_MAX; }
    if (b_end < b_off) { b_end = UINT64_MAX; }
    return (a_off < b_end) && (b_off < a_end);
}

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

int stripe_lease_table_init(struct stripe_lease_table **out)
{
    if (out == NULL) { return -1; }
    struct stripe_lease_table *tbl = calloc(1, sizeof(*tbl));
    if (tbl == NULL) { return -1; }
    for (uint32_t i = 0; i < SLT_SHARD_COUNT; i++) {
        pthread_mutex_init(&tbl->shards[i].lock, NULL);
        tbl->shards[i].head = NULL;
    }
    *out = tbl;
    return 0;
}

void stripe_lease_table_destroy(struct stripe_lease_table *tbl)
{
    if (tbl == NULL) { return; }
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
                                 uint32_t stripe_index,
                                 uint64_t ds_offset,
                                 uint64_t ds_length)
{
    if (tbl == NULL || ds_length == 0) { return false; }
    uint32_t idx = shard_index(fileid, stripe_index);
    struct slt_shard *sh = &tbl->shards[idx];
    uint64_t ts = now_ns();
    bool conflict = false;

    pthread_mutex_lock(&sh->lock);
    (void)shard_evict_expired(sh, ts);

    for (struct slt_entry *e = sh->head; e != NULL; e = e->next) {
        if (e->fileid != fileid) { continue; }
        if (e->stripe_index != stripe_index) { continue; }
        if (e->clientid == clientid) { continue; }
        if (ranges_overlap(e->ds_offset, e->ds_length, ds_offset, ds_length)) {
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
                         uint32_t ds_id,
                         uint32_t stripe_index,
                         uint64_t ds_offset,
                         uint64_t ds_length,
                         uint32_t duration_ms)
{
    if (tbl == NULL || duration_ms == 0 || ds_length == 0) { return -1; }
    uint32_t idx = shard_index(fileid, stripe_index);
    struct slt_shard *sh = &tbl->shards[idx];
    uint64_t ts = now_ns();
    uint64_t expiry = ts + (uint64_t)duration_ms * 1000000ULL;

    pthread_mutex_lock(&sh->lock);
    (void)shard_evict_expired(sh, ts);

    for (struct slt_entry *e = sh->head; e != NULL; e = e->next) {
        if (e->fileid       == fileid       &&
            e->clientid     == clientid     &&
            e->stripe_index == stripe_index &&
            e->ds_offset    == ds_offset) {
            e->ds_length = ds_length;
            e->ds_id     = ds_id;
            e->expiry_ns = expiry;
            pthread_mutex_unlock(&sh->lock);
            return 0;
        }
    }

    struct slt_entry *ne = calloc(1, sizeof(*ne));
    if (ne == NULL) {
        pthread_mutex_unlock(&sh->lock);
        return -1;
    }
    ne->fileid       = fileid;
    ne->clientid     = clientid;
    ne->stripe_index = stripe_index;
    ne->ds_offset    = ds_offset;
    ne->ds_length    = ds_length;
    ne->ds_id        = ds_id;
    ne->expiry_ns    = expiry;
    ne->next         = sh->head;
    sh->head         = ne;

    pthread_mutex_unlock(&sh->lock);
    return 0;
}

void stripe_lease_release(struct stripe_lease_table *tbl,
                          uint64_t fileid,
                          uint64_t clientid,
                          uint32_t stripe_index,
                          uint64_t ds_offset)
{
    if (tbl == NULL) { return; }
    uint32_t idx = shard_index(fileid, stripe_index);
    struct slt_shard *sh = &tbl->shards[idx];

    pthread_mutex_lock(&sh->lock);
    struct slt_entry **pp = &sh->head;
    while (*pp != NULL) {
        struct slt_entry *e = *pp;
        if (e->fileid       == fileid       &&
            e->clientid     == clientid     &&
            e->stripe_index == stripe_index &&
            e->ds_offset    == ds_offset) {
            *pp = e->next;
            free(e);
            break;
        }
        pp = &e->next;
    }
    pthread_mutex_unlock(&sh->lock);
}

void stripe_lease_release_all_for(struct stripe_lease_table *tbl,
                                  uint64_t fileid,
                                  uint64_t clientid)
{
    if (tbl == NULL) { return; }
    for (uint32_t i = 0; i < SLT_SHARD_COUNT; i++) {
        struct slt_shard *sh = &tbl->shards[i];
        pthread_mutex_lock(&sh->lock);
        struct slt_entry **pp = &sh->head;
        while (*pp != NULL) {
            struct slt_entry *e = *pp;
            if (e->fileid == fileid && e->clientid == clientid) {
                *pp = e->next;
                free(e);
                continue;
            }
            pp = &e->next;
        }
        pthread_mutex_unlock(&sh->lock);
    }
}

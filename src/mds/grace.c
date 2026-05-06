/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * grace.c -- NFSv4.1 grace period management.
 *
 * After startup or failover, the MDS enters a grace period during
 * which only reclaim operations (OPEN with CLAIM_PREVIOUS,
 * LOCK with reclaim) are accepted.
 *
 * Client tracking: when grace_enter_with_clients() is used, we
 * store a sorted array of pending clientids.  Reclaim marks them
 * done; when all are done, grace exits early.
 */

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#include "pnfs_mds.h"
#include "grace.h"

/* Maximum tracked clients (heap-allocated, so this is generous). */
#define GRACE_MAX_CLIENTS  65536

struct grace_client_entry {
    uint64_t clientid;
    bool     reclaimed;
};

struct grace_state {
    pthread_mutex_t lock;
    bool     in_grace;
    uint32_t duration_sec;
    time_t   start_time;

    /* Client tracking (heap-allocated sorted array). */
    struct grace_client_entry *clients;
    uint32_t client_count;
    uint32_t reclaimed_count;
};

static struct grace_state g_grace;

/* --- Binary search on sorted clientid array --- */

static int cmp_clientid(const void *a, const void *b)
{
    const struct grace_client_entry *ea = a;
    const struct grace_client_entry *eb = b;

    if (ea->clientid < eb->clientid) {
        return -1;
}
    if (ea->clientid > eb->clientid) {
        return 1;
}
    return 0;
}

static struct grace_client_entry *
find_client(uint64_t clientid)
{
    struct grace_client_entry needle = { .clientid = clientid };

    if (g_grace.clients == NULL || g_grace.client_count == 0) {
        return NULL;
}

    return bsearch(&needle, g_grace.clients, g_grace.client_count,
                   sizeof(*g_grace.clients), cmp_clientid);
}

/* --- Internal: free client array --- */

static void clear_clients(void)
{
    free(g_grace.clients);
    g_grace.clients = NULL;
    g_grace.client_count = 0;
    g_grace.reclaimed_count = 0;
}

/* --- Public API --- */

void grace_init(void)
{
    pthread_mutex_init(&g_grace.lock, NULL);
    g_grace.in_grace = false;
    g_grace.duration_sec = 0;
    g_grace.start_time = 0;
    g_grace.clients = NULL;
    g_grace.client_count = 0;
    g_grace.reclaimed_count = 0;
}

void grace_enter(uint32_t duration_sec)
{
    pthread_mutex_lock(&g_grace.lock);
    clear_clients();
    g_grace.in_grace = true;
    g_grace.duration_sec = duration_sec;
    g_grace.start_time = time(NULL);
    pthread_mutex_unlock(&g_grace.lock);
}

void grace_enter_with_clients(uint32_t duration_sec,
                              const struct client_recovery_rec *recs,
                              uint32_t count)
{
    pthread_mutex_lock(&g_grace.lock);
    clear_clients();

    g_grace.in_grace = true;
    g_grace.duration_sec = duration_sec;
    g_grace.start_time = time(NULL);

    if (count > 0 && recs != NULL && count <= GRACE_MAX_CLIENTS) {
        g_grace.clients = calloc(count, sizeof(*g_grace.clients));
        if (g_grace.clients != NULL) {
            for (uint32_t i = 0; i < count; i++) {
                g_grace.clients[i].clientid = recs[i].clientid;
                g_grace.clients[i].reclaimed = false;
            }
            g_grace.client_count = count;
            g_grace.reclaimed_count = 0;

            /* Sort for binary search. */
            qsort(g_grace.clients, g_grace.client_count,
                  sizeof(*g_grace.clients), cmp_clientid);
        }
    }

    pthread_mutex_unlock(&g_grace.lock);
}

bool grace_is_active(void)
{
    bool active;

    pthread_mutex_lock(&g_grace.lock);
    if (!g_grace.in_grace) {
        pthread_mutex_unlock(&g_grace.lock);
        return false;
    }
    time_t now = time(NULL);
    if ((uint32_t)(now - g_grace.start_time) >= g_grace.duration_sec) {
        g_grace.in_grace = false;
        clear_clients();
        active = false;
    } else {
        active = true;
    }
    pthread_mutex_unlock(&g_grace.lock);
    return active;
}

int grace_client_reclaimed(uint64_t clientid)
{
    int rc = -1;

    pthread_mutex_lock(&g_grace.lock);
    if (!g_grace.in_grace) {
        pthread_mutex_unlock(&g_grace.lock);
        return -1;
    }

    struct grace_client_entry *e = find_client(clientid);
    if (e != NULL && !e->reclaimed) {
        e->reclaimed = true;
        g_grace.reclaimed_count++;
        rc = 0;

        /* Auto-exit grace if all tracked clients have reclaimed. */
        if (g_grace.client_count > 0 &&
            g_grace.reclaimed_count >= g_grace.client_count) {
            g_grace.in_grace = false;
            clear_clients();
        }
    }

    pthread_mutex_unlock(&g_grace.lock);
    return rc;
}

bool grace_client_is_recovering(uint64_t clientid)
{
    bool result = false;

    pthread_mutex_lock(&g_grace.lock);
    if (!g_grace.in_grace) {
        pthread_mutex_unlock(&g_grace.lock);
        return false;
    }

    const struct grace_client_entry *e = find_client(clientid);
    if (e != NULL && !e->reclaimed) {
        result = true;
}

    pthread_mutex_unlock(&g_grace.lock);
    return result;
}

uint32_t grace_pending_count(void)
{
    uint32_t pending = 0;

    pthread_mutex_lock(&g_grace.lock);
    if (g_grace.in_grace && g_grace.client_count > 0) {
        pending = g_grace.client_count - g_grace.reclaimed_count;
}
    pthread_mutex_unlock(&g_grace.lock);
    return pending;
}

void grace_exit(void)
{
    pthread_mutex_lock(&g_grace.lock);
    g_grace.in_grace = false;
    clear_clients();
    pthread_mutex_unlock(&g_grace.lock);
}

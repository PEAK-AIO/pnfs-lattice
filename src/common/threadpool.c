/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * threadpool.c -- Fixed-size worker pool for RPC dispatch.
 *
 * Work items are drawn from a pre-allocated freelist (slab) sized
 * to max_pending, eliminating malloc/free on the submit/complete
 * hot path.  Both the work queue and freelist are managed under
 * the same mutex that protects the pending queue.
 */

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdbool.h>

#include "pnfs_mds.h"


struct tp_work_item {
    tp_work_fn             fn;
    void                  *arg;
    struct tp_work_item   *next;
};

struct threadpool {
    pthread_t        *threads;
    uint32_t          thread_count;
    pthread_mutex_t   lock;
    pthread_cond_t    cond;
    struct tp_work_item *head;         /**< Pending work queue head. */
    struct tp_work_item *tail;         /**< Pending work queue tail. */
    uint32_t          pending;
    uint32_t          max_pending;
    bool              shutdown;

    /* Pre-allocated work-item slab + freelist. */
    struct tp_work_item *slab;         /**< Contiguous item array. */
    struct tp_work_item *free_head;    /**< Freelist stack (LIFO). */
};

/** Return a work item to the freelist.  Caller must hold tp->lock. */
static void freelist_put(struct threadpool *tp, struct tp_work_item *item)
{
    item->fn = NULL;
    item->arg = NULL;
    item->next = tp->free_head;
    tp->free_head = item;
}

/** Pop a work item from the freelist.  Caller must hold tp->lock. */
static struct tp_work_item *freelist_get(struct threadpool *tp)
{
    struct tp_work_item *item = tp->free_head;
    if (item != NULL) {
        tp->free_head = item->next;
        item->next = NULL;
    }
    return item;
}

static void *worker_loop(void *arg)
{
    struct threadpool *tp = arg;

    for (;;) {
        pthread_mutex_lock(&tp->lock);
        while (tp->head == NULL && !tp->shutdown) {
            pthread_cond_wait(&tp->cond, &tp->lock);
        }
        if (tp->shutdown && tp->head == NULL) {
            pthread_mutex_unlock(&tp->lock);
            return NULL;
        }

        struct tp_work_item *item = tp->head;
        tp->head = item->next;
        if (tp->head == NULL) {
            tp->tail = NULL;
        }
        tp->pending--;

        /* Capture fn/arg before returning item to freelist. */
        tp_work_fn fn = item->fn;
        void *fn_arg = item->arg;
        freelist_put(tp, item);
        pthread_mutex_unlock(&tp->lock);

        fn(fn_arg);
    }
}

int threadpool_create(uint32_t count, struct threadpool **out)
{
    if (out == NULL || count == 0) {
        return -1;
    }

    struct threadpool *tp = calloc(1, sizeof(*tp));
    if (tp == NULL) {
        return -1;
    }

    tp->threads = calloc(count, sizeof(pthread_t));
    if (tp->threads == NULL) {
        free(tp);
        return -1;
    }

    tp->thread_count = count;
    tp->max_pending = count * 64;  /* Allow up to 64 queued items per worker. */

    /* Pre-allocate the work-item slab and build the freelist. */
    tp->slab = calloc(tp->max_pending, sizeof(struct tp_work_item));
    if (tp->slab == NULL) {
        free(tp->threads);
        free(tp);
        return -1;
    }
    tp->free_head = NULL;
    for (uint32_t i = 0; i < tp->max_pending; i++) {
        tp->slab[i].next = tp->free_head;
        tp->free_head = &tp->slab[i];
    }

    pthread_mutex_init(&tp->lock, NULL);
    pthread_cond_init(&tp->cond, NULL);

    for (uint32_t i = 0; i < count; i++) {
        int rc = pthread_create(&tp->threads[i], NULL, worker_loop, tp);
        if (rc != 0) {
            /* Partial creation -- shut down what we started */
            tp->shutdown = true;
            pthread_cond_broadcast(&tp->cond);
            for (uint32_t j = 0; j < i; j++) {
                pthread_join(tp->threads[j], NULL);
            }
            pthread_mutex_destroy(&tp->lock);
            pthread_cond_destroy(&tp->cond);
            free(tp->slab);
            free(tp->threads);
            free(tp);
            return -1;
        }
    }

    *out = tp;
    return 0;
}

int threadpool_submit(struct threadpool *tp, tp_work_fn fn, void *arg)
{
    if (tp == NULL || fn == NULL) {
        return -1;
    }

    pthread_mutex_lock(&tp->lock);
    if (tp->shutdown) {
        pthread_mutex_unlock(&tp->lock);
        return -1;
    }
    /* Backpressure: reject if queue is full. */
    if (tp->pending >= tp->max_pending) {
        pthread_mutex_unlock(&tp->lock);
        return -1;
    }

    struct tp_work_item *item = freelist_get(tp);
    if (item == NULL) {
        /* Should not happen (pending < max_pending), but be safe. */
        pthread_mutex_unlock(&tp->lock);
        return -1;
    }
    item->fn = fn;
    item->arg = arg;
    item->next = NULL;

    if (tp->tail != NULL) {
        tp->tail->next = item;
    } else {
        tp->head = item;
    }
    tp->tail = item;
    tp->pending++;
    pthread_cond_signal(&tp->cond);
    pthread_mutex_unlock(&tp->lock);
    return 0;
}

void threadpool_destroy(struct threadpool *tp)
{
    if (tp == NULL) {
        return;
    }

    pthread_mutex_lock(&tp->lock);
    tp->shutdown = true;
    pthread_cond_broadcast(&tp->cond);
    pthread_mutex_unlock(&tp->lock);

    for (uint32_t i = 0; i < tp->thread_count; i++) {
        pthread_join(tp->threads[i], NULL);
    }

    /* Slab owns all work items -- single free covers everything. */
    pthread_mutex_destroy(&tp->lock);
    pthread_cond_destroy(&tp->cond);
    free(tp->slab);
    free(tp->threads);
    free(tp);
}

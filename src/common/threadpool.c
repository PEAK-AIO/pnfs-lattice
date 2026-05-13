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
 *
 * Observability
 * -------------
 * Each work item carries an enqueue timestamp; the worker computes
 * queue-wait nanoseconds at dequeue and feeds them into a
 * lock-free 12-bucket histogram.  Submitted / completed / queue-
 * full / active-worker counters live on the pool struct as
 * `_Atomic uint64_t` and are read by `threadpool_stats()`.  All
 * observability writes are relaxed-order: the mutex around the
 * queue itself provides whatever ordering callers need.
 */

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdbool.h>
#include <time.h>

#include "pnfs_mds.h"
#include "mds_histogram.h"


struct tp_work_item {
    tp_work_fn             fn;
    void                  *arg;
    struct tp_work_item   *next;
    uint64_t               enqueue_ns;  /**< CLOCK_MONOTONIC at submit. */
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

    /* Live observability counters (relaxed-order). */
    _Atomic uint64_t  submitted_total;   /**< Submissions accepted. */
    _Atomic uint64_t  completed_total;   /**< Items finished by workers. */
    _Atomic uint64_t  queue_full_total;  /**< Submissions rejected (full). */
    _Atomic uint64_t  active_workers;    /**< Workers currently in fn(). */
    _Atomic uint64_t  queue_wait_ns_sum; /**< Cumulative wait ns. */
    _Atomic uint64_t  queue_wait_count;  /**< Wait observations. */

    struct mds_histogram queue_wait_hist;
};

/** Read CLOCK_MONOTONIC as a single 64-bit nanosecond stamp. */
static uint64_t monotonic_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/** Return a work item to the freelist.  Caller must hold tp->lock. */
static void freelist_put(struct threadpool *tp, struct tp_work_item *item)
{
    item->fn = NULL;
    item->arg = NULL;
    item->enqueue_ns = 0;
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

        /* Capture fn/arg/enqueue stamp before returning item to freelist. */
        tp_work_fn fn = item->fn;
        void *fn_arg = item->arg;
        uint64_t enqueue_ns = item->enqueue_ns;
        freelist_put(tp, item);
        pthread_mutex_unlock(&tp->lock);

        /*
         * Record queue-wait latency.  Using the dequeue clock as
         * close to the unlock as possible biases the sample toward
         * "time the request sat queued" rather than "time spent
         * acquiring the lock to dequeue", which is what operators
         * actually want when chasing dispatcher saturation.
         */
        uint64_t now_ns = monotonic_ns();
        uint64_t wait_ns = (enqueue_ns != 0 && now_ns >= enqueue_ns)
            ? (now_ns - enqueue_ns) : 0;
        atomic_fetch_add_explicit(&tp->queue_wait_ns_sum, wait_ns,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&tp->queue_wait_count, 1U,
                                  memory_order_relaxed);
        mds_histogram_observe(&tp->queue_wait_hist, wait_ns);

        atomic_fetch_add_explicit(&tp->active_workers, 1U,
                                  memory_order_relaxed);
        fn(fn_arg);
        atomic_fetch_sub_explicit(&tp->active_workers, 1U,
                                  memory_order_relaxed);
        atomic_fetch_add_explicit(&tp->completed_total, 1U,
                                  memory_order_relaxed);
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

    /* `_Atomic uint64_t` counters and the embedded histogram are
     * zero-initialised by calloc, which is a valid initial state
     * for relaxed-order atomic integers. */

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
        atomic_fetch_add_explicit(&tp->queue_full_total, 1U,
                                  memory_order_relaxed);
        return -1;
    }

    struct tp_work_item *item = freelist_get(tp);
    if (item == NULL) {
        /* Should not happen (pending < max_pending), but be safe. */
        pthread_mutex_unlock(&tp->lock);
        atomic_fetch_add_explicit(&tp->queue_full_total, 1U,
                                  memory_order_relaxed);
        return -1;
    }
    item->fn = fn;
    item->arg = arg;
    item->enqueue_ns = monotonic_ns();
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

    atomic_fetch_add_explicit(&tp->submitted_total, 1U,
                              memory_order_relaxed);
    return 0;
}

void threadpool_get_stats(struct threadpool *tp, struct threadpool_stats *out)
{
    if (out == NULL) {
        return;
    }
    memset(out, 0, sizeof(*out));
    if (tp == NULL) {
        return;
    }

    pthread_mutex_lock(&tp->lock);
    out->worker_total   = tp->thread_count;
    out->queue_depth    = tp->pending;
    out->queue_capacity = tp->max_pending;
    pthread_mutex_unlock(&tp->lock);

    out->worker_active = (uint32_t)atomic_load_explicit(
        &tp->active_workers, memory_order_relaxed);
    out->submitted_total = atomic_load_explicit(
        &tp->submitted_total, memory_order_relaxed);
    out->completed_total = atomic_load_explicit(
        &tp->completed_total, memory_order_relaxed);
    out->queue_full_total = atomic_load_explicit(
        &tp->queue_full_total, memory_order_relaxed);
    out->queue_wait_ns_sum = atomic_load_explicit(
        &tp->queue_wait_ns_sum, memory_order_relaxed);
    out->queue_wait_count = atomic_load_explicit(
        &tp->queue_wait_count, memory_order_relaxed);
    out->queue_wait_hist = &tp->queue_wait_hist;
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

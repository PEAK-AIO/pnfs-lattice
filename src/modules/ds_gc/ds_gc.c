/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_gc.c -- Background drainer for the catalogue GC queue.
 *
 * Community-edition implementation: coordinator thread batch-peeks
 * gc_queue rows and feeds a bounded work queue consumed by N worker
 * threads.  Each worker issues path-based unlinks on the DS mounts,
 * best-effort stripe_map catalogue cleanup, then dequeues the row.
 */

#include "ds_gc.h"

#include "mds_catalogue.h"
#include "pnfs_mds.h"
#include "proxy_io.h"

#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define DS_GC_MIN_POLL_MS     100U
#define DS_GC_MAX_WORKERS     32U
#define DS_GC_MAX_BATCH_SIZE  4096U
#define DS_GC_QUEUE_CAP       4096U

struct ds_gc_work_item {
	struct mds_gc_entry entry;
	bool                valid;
};

struct ds_gc {
	struct mds_catalogue    *cat;
	struct mds_proxy_ctx    *proxy;
	uint32_t                 poll_ms;
	uint32_t                 workers;
	uint32_t                 batch_size;

	_Atomic bool             stop;
	int                      wake_pipe[2];
	pthread_t                coordinator;

	pthread_mutex_t          q_mutex;
	pthread_cond_t           q_not_empty;
	pthread_cond_t           q_not_full;
	struct ds_gc_work_item  *queue;
	uint32_t                 q_head;
	uint32_t                 q_tail;
	uint32_t                 q_count;
	uint32_t                 q_cap;

	pthread_t               *worker_threads;
	uint32_t                 worker_count;

	struct mds_gc_entry     *batch_buf;
};

static uint32_t clamp_u32(uint32_t v, uint32_t lo, uint32_t hi)
{
	if (v < lo) {
		return lo;
	}
	if (v > hi) {
		return hi;
	}
	return v;
}

static bool process_one_entry(struct ds_gc *gc,
			      const struct mds_gc_entry *entry)
{
	bool had_any_existed = false;
	bool blocked = false;

	for (uint32_t stripe = 0; stripe < MDS_MAX_STRIPES && !blocked;
	     stripe++) {
		for (uint32_t mirror = 0; mirror < MDS_MAX_MIRRORS; mirror++) {
			bool existed = false;
			enum mds_status st;

			st = mds_proxy_unlink_ds_file(gc->proxy,
						      entry->ds_id,
						      entry->fileid,
						      stripe, mirror,
						      &existed);
			if (st == MDS_ERR_NOTFOUND) {
				/* DS mount missing -- retry later. */
				blocked = true;
				break;
			}
			if (st != MDS_OK) {
				blocked = true;
				break;
			}
			if (!existed) {
				if (!had_any_existed) {
					goto dequeue_ok;
				}
			} else {
				had_any_existed = true;
			}
		}
	}

dequeue_ok:
	if (blocked) {
		return false;
	}
	/* Best-effort: drop catalogue stripe rows after DS bytes are gone.
	 * Idempotent when ns_remove already deleted them in-txn. */
	(void)mds_cat_stripe_map_del(gc->cat, NULL, entry->fileid);
	return mds_cat_gc_dequeue(gc->cat, NULL, entry->gc_seq) == MDS_OK;
}

static void q_signal_stop(struct ds_gc *gc)
{
	(void)write(gc->wake_pipe[1], "x", 1);
}

static bool q_push(struct ds_gc *gc, const struct mds_gc_entry *entry)
{
	pthread_mutex_lock(&gc->q_mutex);
	while (gc->q_count >= gc->q_cap && !atomic_load_explicit(&gc->stop,
							       memory_order_relaxed)) {
		pthread_cond_wait(&gc->q_not_full, &gc->q_mutex);
	}
	if (atomic_load_explicit(&gc->stop, memory_order_relaxed)) {
		pthread_mutex_unlock(&gc->q_mutex);
		return false;
	}
	gc->queue[gc->q_tail].entry = *entry;
	gc->queue[gc->q_tail].valid = true;
	gc->q_tail = (gc->q_tail + 1U) % gc->q_cap;
	gc->q_count++;
	pthread_cond_signal(&gc->q_not_empty);
	pthread_mutex_unlock(&gc->q_mutex);
	return true;
}

static bool q_pop(struct ds_gc *gc, struct mds_gc_entry *entry)
{
	pthread_mutex_lock(&gc->q_mutex);
	while (gc->q_count == 0 &&
	       !atomic_load_explicit(&gc->stop, memory_order_relaxed)) {
		pthread_cond_wait(&gc->q_not_empty, &gc->q_mutex);
	}
	if (gc->q_count == 0) {
		pthread_mutex_unlock(&gc->q_mutex);
		return false;
	}
	*entry = gc->queue[gc->q_head].entry;
	gc->queue[gc->q_head].valid = false;
	gc->q_head = (gc->q_head + 1U) % gc->q_cap;
	gc->q_count--;
	pthread_cond_signal(&gc->q_not_full);
	pthread_mutex_unlock(&gc->q_mutex);
	return true;
}

static void *ds_gc_worker_main(void *arg)
{
	struct ds_gc *gc = arg;
	struct mds_gc_entry entry;

	while (!atomic_load_explicit(&gc->stop, memory_order_relaxed)) {
		if (!q_pop(gc, &entry)) {
			continue;
		}
		(void)process_one_entry(gc, &entry);
	}
	return NULL;
}

static void coordinator_drain_queue(struct ds_gc *gc)
{
	pthread_mutex_lock(&gc->q_mutex);
	while (gc->q_count > 0) {
		pthread_mutex_unlock(&gc->q_mutex);
		usleep(1000U);
		pthread_mutex_lock(&gc->q_mutex);
	}
	pthread_mutex_unlock(&gc->q_mutex);
}

static void *ds_gc_coordinator_main(void *arg)
{
	struct ds_gc *gc = arg;

	while (!atomic_load_explicit(&gc->stop, memory_order_relaxed)) {
		uint32_t n = 0;
		enum mds_status st;

		st = mds_cat_gc_peek_batch(gc->cat, gc->batch_buf,
					   gc->batch_size, &n);
		if (st == MDS_OK && n > 0) {
			for (uint32_t i = 0; i < n; i++) {
				if (!q_push(gc, &gc->batch_buf[i])) {
					break;
				}
			}
			coordinator_drain_queue(gc);
		}

		{
			struct pollfd pfd = {
				.fd = gc->wake_pipe[0],
				.events = POLLIN,
			};
			(void)poll(&pfd, 1, (int)gc->poll_ms);
		}
	}
	return NULL;
}

static void ds_gc_free(struct ds_gc *gc)
{
	if (gc == NULL) {
		return;
	}
	if (gc->wake_pipe[0] >= 0) {
		(void)close(gc->wake_pipe[0]);
	}
	if (gc->wake_pipe[1] >= 0) {
		(void)close(gc->wake_pipe[1]);
	}
	pthread_mutex_destroy(&gc->q_mutex);
	pthread_cond_destroy(&gc->q_not_empty);
	pthread_cond_destroy(&gc->q_not_full);
	free(gc->queue);
	free(gc->batch_buf);
	free(gc->worker_threads);
	free(gc);
}

int ds_gc_start(struct mds_catalogue *cat,
                struct mds_proxy_ctx *proxy,
                uint32_t poll_ms,
                struct ds_gc **out)
{
	return ds_gc_start_ex(cat, proxy, poll_ms, 1U, 256U, out);
}

int ds_gc_start_ex(struct mds_catalogue *cat,
                   struct mds_proxy_ctx *proxy,
                   uint32_t poll_ms,
                   uint32_t workers,
                   uint32_t batch_size,
                   struct ds_gc **out)
{
	struct ds_gc *gc = NULL;
	int pipe_rc;

	if (out == NULL) {
		return -1;
	}
	*out = NULL;

	if (cat == NULL || proxy == NULL || poll_ms == 0U) {
		return 0;
	}

	poll_ms = clamp_u32(poll_ms, DS_GC_MIN_POLL_MS, UINT32_MAX);
	workers = clamp_u32(workers, 1U, DS_GC_MAX_WORKERS);
	batch_size = clamp_u32(batch_size, 1U, DS_GC_MAX_BATCH_SIZE);

	gc = calloc(1, sizeof(*gc));
	if (gc == NULL) {
		return -1;
	}
	gc->cat = cat;
	gc->proxy = proxy;
	gc->poll_ms = poll_ms;
	gc->workers = workers;
	gc->batch_size = batch_size;
	gc->wake_pipe[0] = -1;
	gc->wake_pipe[1] = -1;
	atomic_store_explicit(&gc->stop, false, memory_order_relaxed);

	gc->q_cap = DS_GC_QUEUE_CAP;
	gc->queue = calloc(gc->q_cap, sizeof(*gc->queue));
	gc->batch_buf = calloc(batch_size, sizeof(*gc->batch_buf));
	gc->worker_threads = calloc(workers, sizeof(*gc->worker_threads));
	if (gc->queue == NULL || gc->batch_buf == NULL ||
	    gc->worker_threads == NULL) {
		ds_gc_free(gc);
		return -1;
	}

	if (pthread_mutex_init(&gc->q_mutex, NULL) != 0 ||
	    pthread_cond_init(&gc->q_not_empty, NULL) != 0 ||
	    pthread_cond_init(&gc->q_not_full, NULL) != 0) {
		ds_gc_free(gc);
		return -1;
	}

	pipe_rc = pipe(gc->wake_pipe);
	if (pipe_rc != 0) {
		ds_gc_free(gc);
		return -1;
	}

	for (uint32_t i = 0; i < workers; i++) {
		if (pthread_create(&gc->worker_threads[i], NULL,
				   ds_gc_worker_main, gc) != 0) {
			atomic_store_explicit(&gc->stop, true,
					      memory_order_relaxed);
			q_signal_stop(gc);
			for (uint32_t j = 0; j < i; j++) {
				(void)pthread_join(gc->worker_threads[j], NULL);
			}
			ds_gc_free(gc);
			return -1;
		}
		gc->worker_count++;
	}

	if (pthread_create(&gc->coordinator, NULL,
			   ds_gc_coordinator_main, gc) != 0) {
		atomic_store_explicit(&gc->stop, true, memory_order_relaxed);
		q_signal_stop(gc);
		for (uint32_t i = 0; i < gc->worker_count; i++) {
			(void)pthread_join(gc->worker_threads[i], NULL);
		}
		ds_gc_free(gc);
		return -1;
	}

	*out = gc;
	return 0;
}

void ds_gc_stop(struct ds_gc *gc)
{
	if (gc == NULL) {
		return;
	}

	atomic_store_explicit(&gc->stop, true, memory_order_relaxed);
	q_signal_stop(gc);
	pthread_cond_broadcast(&gc->q_not_empty);
	pthread_cond_broadcast(&gc->q_not_full);

	(void)pthread_join(gc->coordinator, NULL);
	for (uint32_t i = 0; i < gc->worker_count; i++) {
		(void)pthread_join(gc->worker_threads[i], NULL);
	}
	ds_gc_free(gc);
}

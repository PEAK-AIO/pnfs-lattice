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
#include "mds_metrics.h"
#include "open_state.h"
#include "pnfs_mds.h"
#include "proxy_io.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define DS_GC_MIN_POLL_MS     100U
#define DS_GC_MAX_WORKERS     32U
#define DS_GC_MAX_BATCH_SIZE  4096U
#define DS_GC_QUEUE_CAP       4096U
#define DS_GC_GAUGE_EVERY     6U   /* refresh gc_pending every 6th tick */
#define DS_GC_LEASE_MS        30000U
#define DS_GC_STALE_OWNER_MS  15000U

struct ds_gc_work_item {
	struct mds_gc_task  task;
	bool                valid;
};

struct ds_gc {
	struct mds_catalogue    *cat;
	struct mds_proxy_ctx    *proxy;
	uint32_t                 poll_ms;
	uint32_t                 workers;
	uint32_t                 batch_size;
	uint32_t                 mds_id;
	uint64_t                 boot_epoch;
	_Atomic(struct open_state_table *) open_state;
	_Atomic uint32_t         async_high_watermark;
	_Atomic uint32_t         async_low_watermark;
	_Atomic bool             async_backpressure;

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

	struct mds_gc_task      *batch_buf;
};

static uint64_t gc_now_ns(void)
{
	struct timespec now;

	if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
		return 0;
	}
	return (uint64_t)now.tv_sec * 1000000000ULL +
		(uint64_t)now.tv_nsec;
}

static void gc_metric_subtract(_Atomic uint64_t *metric, uint64_t amount)
{
	uint64_t current;
	uint64_t next;

	if (amount == 0) {
		return;
	}
	current = atomic_load_explicit(metric, memory_order_relaxed);
	do {
		next = current > amount ? current - amount : 0;
	} while (!atomic_compare_exchange_weak_explicit(
		metric, &current, next, memory_order_relaxed,
		memory_order_relaxed));
}

static void gc_refresh_task_metrics(struct ds_gc *gc)
{
	struct mds_gc_task_stats stats;
	uint64_t now_ns;
	uint64_t age_seconds = 0;

	if (mds_cat_gc_task_stats(gc->cat, &stats) != MDS_OK) {
		return;
	}
	now_ns = gc_now_ns();
	if (stats.oldest_file_task_created_ns > 0 &&
	    now_ns >= stats.oldest_file_task_created_ns) {
		age_seconds = (now_ns - stats.oldest_file_task_created_ns) /
			1000000000ULL;
	}
	atomic_store_explicit(&g_branch_metrics.gc_pending,
		stats.active_file_tasks, memory_order_relaxed);
	atomic_store_explicit(&g_branch_metrics.gc_claimed,
		stats.claimed_file_tasks, memory_order_relaxed);
	atomic_store_explicit(&g_branch_metrics.gc_oldest_age_seconds,
		age_seconds, memory_order_relaxed);
}

static void gc_note_task_completed(
	struct ds_gc *gc, const struct mds_gc_task *task,
	uint64_t deferred_quota_bytes)
{
	(void)gc;
	atomic_fetch_add_explicit(&g_branch_metrics.gc_completed_total, 1,
		memory_order_relaxed);
	if (task->task_kind != MDS_GC_TASK_FILE_UNLINK) {
		return;
	}
	gc_metric_subtract(&g_branch_metrics.gc_pending, 1);
	gc_metric_subtract(&g_branch_metrics.gc_deferred_quota_bytes,
		deferred_quota_bytes);
	gc_metric_subtract(&g_branch_metrics.gc_deferred_quota_inodes, 1);
}

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

static void process_legacy_task(struct ds_gc *gc,
				 const struct mds_gc_task *task)
{
	bool had_any_existed = false;
	bool blocked = false;

	/*
	 * A file's stripes (0..stripe_count-1) and mirrors (0..mirror_count-1)
	 * are dense from 0, so the first absent slot means there are no higher
	 * ones.  Stop probing there instead of brute-forcing all
	 * MDS_MAX_STRIPES * MDS_MAX_MIRRORS combinations -- that was ~4096 DS
	 * round-trips per single-stripe file, which made the drain unable to
	 * keep up with a heavy-delete backlog.
	 */
	enum mds_status block_st = MDS_OK;

	for (uint32_t stripe = 0; stripe < MDS_MAX_STRIPES && !blocked;
	     stripe++) {
		bool stripe_had_existing = false;

		for (uint32_t mirror = 0; mirror < MDS_MAX_MIRRORS; mirror++) {
			bool existed = false;
			enum mds_status st;

			st = mds_proxy_unlink_ds_file(gc->proxy,
						      task->ds_id,
						      task->fileid,
						      stripe, mirror,
						      &existed);
			if (st != MDS_OK) {
				/* DS mount missing / I/O error -- retry later. */
				blocked = true;
				block_st = st;
				break;
			}
			if (existed) {
				had_any_existed = true;
				stripe_had_existing = true;
				continue;
			}
			/* First absent mirror in this stripe: no higher
			 * mirrors exist for it. */
			break;
		}
		/* First stripe with no mirror at all: stripes exhausted. */
		if (!blocked && !stripe_had_existing) {
			break;
		}
	}

	if (blocked) {
		/* An unavailable mount is never permission to drop cleanup work.
		 * Release the lease with a retry delay so later tasks can run and
		 * another MDS can take over after the delay or lease expiry. */
		static _Atomic unsigned long blk_n = 0;
		if ((atomic_fetch_add_explicit(&blk_n, 1UL,
					       memory_order_relaxed) & 0x3FFUL) == 0UL) {
			fprintf(stderr,
				"WARN: ds_gc retrying task: fileid=%llu ds_id=%u "
				"status=%d\n",
				(unsigned long long)task->fileid, task->ds_id,
				(int)block_st);
		}
		(void)mds_cat_gc_task_reschedule(
			gc->cat, task->task_kind, task->task_id, gc->mds_id,
			gc->boot_epoch, (int32_t)block_st, gc->poll_ms);
		return;
	}
	(void)had_any_existed;
	/* Legacy rows have no retained inode.  Future file-unlink tasks keep
	 * their stripe map and are finalized by Phase 4, rather than here. */
	(void)mds_cat_stripe_map_del(gc->cat, NULL, task->fileid);
	(void)mds_cat_gc_task_complete(
		gc->cat, task->task_kind, task->task_id, gc->mds_id,
		gc->boot_epoch);
}

static void reschedule_file_task(
	struct ds_gc *gc, const struct mds_gc_task *task,
	enum mds_status status)
{
	(void)mds_cat_gc_task_reschedule(
		gc->cat, task->task_kind, task->task_id, gc->mds_id,
		gc->boot_epoch, (int32_t)status, gc->poll_ms);
}

static void quarantine_file_task(
	struct ds_gc *gc, const struct mds_gc_task *task,
	enum mds_status status)
{
	if (mds_cat_gc_task_quarantine(
		gc->cat, task->task_kind, task->task_id, gc->mds_id,
		gc->boot_epoch, (int32_t)status) == MDS_OK) {
		atomic_fetch_add_explicit(
			&g_branch_metrics.gc_permanent_failures_total, 1,
			memory_order_relaxed);
		if (task->task_kind == MDS_GC_TASK_FILE_UNLINK) {
			gc_metric_subtract(&g_branch_metrics.gc_pending, 1);
		}
	}
}

static enum mds_status process_file_object(
	struct ds_gc *gc, const struct mds_gc_task *task,
	uint32_t ds_id, uint32_t stripe, uint32_t mirror)
{
	enum mds_status status;
	bool existed;

	status = mds_cat_gc_task_renew(
		gc->cat, task->task_kind, task->task_id, gc->mds_id,
		gc->boot_epoch, DS_GC_LEASE_MS);
	if (status != MDS_OK) {
		return status;
	}
	status = mds_proxy_fence_ds_file(
		gc->proxy, ds_id, task->fileid, stripe, mirror);
	if (status != MDS_OK) {
		atomic_fetch_add_explicit(
			&g_branch_metrics.gc_unavailable_ds_total, 1,
			memory_order_relaxed);
		return status;
	}
	existed = false;
	status = mds_proxy_unlink_ds_file(
		gc->proxy, ds_id, task->fileid, stripe, mirror, &existed);
	if (status != MDS_OK) {
		atomic_fetch_add_explicit(
			&g_branch_metrics.gc_unavailable_ds_total, 1,
			memory_order_relaxed);
	}
	return status;
}

static void process_file_unlink_task(
	struct ds_gc *gc, const struct mds_gc_task *task)
{
	struct mds_inode inode;
	struct mds_ds_map_entry *entries = NULL;
	struct open_state_table *open_state;
	uint32_t stripe_count = 0;
	uint32_t stripe_unit = 0;
	uint32_t mirror_count = 0;
	uint32_t entry_count;
	enum mds_status status;
	int has_opens;

	open_state = atomic_load_explicit(
		&gc->open_state, memory_order_acquire);
	has_opens = open_state_file_has_opens(open_state, task->fileid);
	if (has_opens != 0) {
		if (has_opens > 0) {
			atomic_fetch_add_explicit(
				&g_branch_metrics.gc_open_blocked_total, 1,
				memory_order_relaxed);
		}
		reschedule_file_task(
			gc, task, has_opens > 0 ? MDS_ERR_DELAY : MDS_ERR_IO);
		return;
	}

	status = mds_cat_ns_getattr(gc->cat, task->fileid, &inode);
	if (status == MDS_ERR_NOTFOUND) {
		if (mds_cat_gc_task_complete(
			gc->cat, task->task_kind, task->task_id, gc->mds_id,
			gc->boot_epoch) == MDS_OK) {
			gc_note_task_completed(gc, task, 0);
		}
		return;
	}
	if (status != MDS_OK) {
		reschedule_file_task(gc, task, status);
		return;
	}
	if (inode.type != MDS_FTYPE_REG ||
	    inode.generation != task->inode_generation ||
	    inode.nlink != 0 ||
	    !(inode.flags & MDS_IFLAG_DELETE_PENDING)) {
		quarantine_file_task(gc, task, MDS_ERR_STALE);
		return;
	}

	if (inode.flags & MDS_IFLAG_INLINE_STRIPE) {
		if (inode.inline_ds_id == 0) {
			quarantine_file_task(gc, task, MDS_ERR_STALE);
			return;
		}
		status = process_file_object(
			gc, task, inode.inline_ds_id, 0, 0);
		if (status != MDS_OK) {
			reschedule_file_task(gc, task, status);
			return;
		}
	} else {
		status = mds_cat_stripe_map_get(
			gc->cat, task->fileid, &stripe_count, &stripe_unit,
			&mirror_count, &entries);
		if (status != MDS_OK && status != MDS_ERR_NOTFOUND) {
			reschedule_file_task(gc, task, status);
			return;
		}
		if (status == MDS_OK) {
			if (stripe_count == 0 || mirror_count == 0 ||
			    stripe_count > MDS_MAX_STRIPES ||
			    mirror_count > MDS_MAX_MIRRORS ||
			    stripe_count > UINT32_MAX / mirror_count) {
				free(entries);
				quarantine_file_task(gc, task, MDS_ERR_STALE);
				return;
			}
			entry_count = stripe_count * mirror_count;
			for (uint32_t stripe = 0; stripe < stripe_count; stripe++) {
				for (uint32_t mirror = 0; mirror < mirror_count;
				     mirror++) {
					uint32_t entry_index =
						stripe * mirror_count + mirror;

					if (entry_index >= entry_count ||
					    entries[entry_index].ds_id == 0) {
						free(entries);
						quarantine_file_task(
							gc, task, MDS_ERR_STALE);
						return;
					}
					status = process_file_object(
						gc, task,
						entries[entry_index].ds_id,
						stripe, mirror);
					if (status != MDS_OK) {
						free(entries);
						reschedule_file_task(
							gc, task, status);
						return;
					}
				}
			}
			free(entries);
		}
	}

	status = mds_cat_gc_task_renew(
		gc->cat, task->task_kind, task->task_id, gc->mds_id,
		gc->boot_epoch, DS_GC_LEASE_MS);
	if (status != MDS_OK) {
		return;
	}
	status = mds_cat_gc_task_finalize_file(
		gc->cat, task->fileid, task->inode_generation, gc->mds_id,
		gc->boot_epoch);
	if (status == MDS_OK) {
		gc_note_task_completed(gc, task, inode.size);
		return;
	}
	if (status == MDS_ERR_NOTFOUND) {
		if (mds_cat_gc_task_complete(
			gc->cat, task->task_kind, task->task_id, gc->mds_id,
			gc->boot_epoch) == MDS_OK) {
			gc_note_task_completed(gc, task, 0);
		}
		return;
	}
	if (status == MDS_ERR_STALE) {
		quarantine_file_task(gc, task, status);
		return;
	}
	reschedule_file_task(gc, task, status);
}

static void q_signal_wake(struct ds_gc *gc)
{
	if (gc != NULL && gc->wake_pipe[1] >= 0) {
		(void)write(gc->wake_pipe[1], "x", 1);
	}
}

static bool q_push(struct ds_gc *gc, const struct mds_gc_task *task)
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
	gc->queue[gc->q_tail].task = *task;
	gc->queue[gc->q_tail].valid = true;
	gc->q_tail = (gc->q_tail + 1U) % gc->q_cap;
	gc->q_count++;
	pthread_cond_signal(&gc->q_not_empty);
	pthread_mutex_unlock(&gc->q_mutex);
	return true;
}

static bool q_pop(struct ds_gc *gc, struct mds_gc_task *task)
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
	*task = gc->queue[gc->q_head].task;
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
	struct mds_gc_task task;

	while (!atomic_load_explicit(&gc->stop, memory_order_relaxed)) {
		if (!q_pop(gc, &task)) {
			continue;
		}
		if (task.task_kind == MDS_GC_TASK_LEGACY_DS_UNLINK) {
			process_legacy_task(gc, &task);
		} else if (task.task_kind == MDS_GC_TASK_FILE_UNLINK) {
			process_file_unlink_task(gc, &task);
		} else {
			quarantine_file_task(gc, &task, MDS_ERR_NOSUPPORT);
		}
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
	uint32_t gauge_tick = 0;

	while (!atomic_load_explicit(&gc->stop, memory_order_relaxed)) {
		uint32_t n = 0;
		enum mds_status st;

		st = mds_cat_gc_task_claim_batch(
			gc->cat, gc->batch_buf, gc->batch_size, &n, gc->mds_id,
			gc->boot_epoch, DS_GC_LEASE_MS, DS_GC_STALE_OWNER_MS);
		if (st == MDS_OK && n > 0) {
			for (uint32_t i = 0; i < n; i++) {
				if (gc->batch_buf[i].task_kind ==
				    MDS_GC_TASK_FILE_UNLINK) {
					atomic_fetch_add_explicit(
						&g_branch_metrics.gc_claimed_total, 1,
						memory_order_relaxed);
					if (gc->batch_buf[i].attempt_count > 1) {
						atomic_fetch_add_explicit(
							&g_branch_metrics.gc_retries_total, 1,
							memory_order_relaxed);
					}
					if (gc->batch_buf[i].claim_flags &
					    MDS_GC_TASK_CLAIM_F_TAKEOVER) {
						atomic_fetch_add_explicit(
							&g_branch_metrics
								.gc_dead_owner_takeovers_total,
							1, memory_order_relaxed);
					}
				}
				if (!q_push(gc, &gc->batch_buf[i])) {
					break;
				}
			}
			coordinator_drain_queue(gc);
		}

		/* Refresh durable task gauges infrequently.  The request path
		 * reads these cached values and never scans the catalogue. */
		if ((gauge_tick++ % DS_GC_GAUGE_EVERY) == 0) {
			gc_refresh_task_metrics(gc);
		}

		/*
		 * Back-to-back batches while there is a backlog: a full batch
		 * means there is almost certainly more, so re-peek immediately
		 * instead of sleeping poll_ms.  Without this the drain is capped
		 * at batch_size/poll_ms (~50/s with the defaults), which can't
		 * clear a million-row backlog.  Only fall through to the
		 * poll-sleep when the queue has drained below a full batch.
		 */
		if (st == MDS_OK && n >= gc->batch_size &&
		    !atomic_load_explicit(&gc->stop, memory_order_relaxed)) {
			continue;
		}

		{
			struct pollfd pfd = {
				.fd = gc->wake_pipe[0],
				.events = POLLIN,
			};
			if (poll(&pfd, 1, (int)gc->poll_ms) > 0 &&
			    (pfd.revents & POLLIN) != 0) {
				char discard[64];

				while (read(gc->wake_pipe[0], discard,
					    sizeof(discard)) > 0) {
				}
			}
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
	return ds_gc_start_ex_with_identity(cat, proxy, poll_ms, workers,
					    batch_size, 0, 0, out);
}

int ds_gc_start_ex_with_identity(struct mds_catalogue *cat,
                                 struct mds_proxy_ctx *proxy,
                                 uint32_t poll_ms,
                                 uint32_t workers,
                                 uint32_t batch_size,
                                 uint32_t mds_id,
                                 uint64_t boot_epoch,
                                 struct ds_gc **out)
{
	struct ds_gc *gc = NULL;
	int pipe_rc;
	int read_flags;
	int write_flags;

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
	gc->mds_id = mds_id;
	gc->boot_epoch = boot_epoch;
	atomic_store_explicit(&gc->open_state, NULL, memory_order_relaxed);
	atomic_store_explicit(
		&gc->async_high_watermark, 0, memory_order_relaxed);
	atomic_store_explicit(
		&gc->async_low_watermark, 0, memory_order_relaxed);
	atomic_store_explicit(
		&gc->async_backpressure, false, memory_order_relaxed);
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
	read_flags = fcntl(gc->wake_pipe[0], F_GETFL);
	write_flags = fcntl(gc->wake_pipe[1], F_GETFL);
	if (read_flags < 0 || write_flags < 0 ||
	    fcntl(gc->wake_pipe[0], F_SETFL, read_flags | O_NONBLOCK) != 0 ||
	    fcntl(gc->wake_pipe[1], F_SETFL, write_flags | O_NONBLOCK) != 0) {
		ds_gc_free(gc);
		return -1;
	}

	for (uint32_t i = 0; i < workers; i++) {
		if (pthread_create(&gc->worker_threads[i], NULL,
				   ds_gc_worker_main, gc) != 0) {
			atomic_store_explicit(&gc->stop, true,
					      memory_order_relaxed);
			q_signal_wake(gc);
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
		q_signal_wake(gc);
		for (uint32_t i = 0; i < gc->worker_count; i++) {
			(void)pthread_join(gc->worker_threads[i], NULL);
		}
		ds_gc_free(gc);
		return -1;
	}

	*out = gc;
	return 0;
}

void ds_gc_set_open_state(struct ds_gc *gc, struct open_state_table *ot)
{
	if (gc != NULL) {
		atomic_store_explicit(&gc->open_state, ot, memory_order_release);
	}
}
void ds_gc_wake(struct ds_gc *gc)
{
	q_signal_wake(gc);
}

void ds_gc_set_backpressure(
	struct ds_gc *gc, uint32_t high_watermark, uint32_t low_watermark)
{
	if (gc == NULL) {
		return;
	}
	atomic_store_explicit(&gc->async_high_watermark, high_watermark,
		memory_order_release);
	atomic_store_explicit(&gc->async_low_watermark, low_watermark,
		memory_order_release);
	gc_refresh_task_metrics(gc);
}

bool ds_gc_should_backpressure(struct ds_gc *gc)
{
	uint32_t high_watermark;
	uint32_t low_watermark;
	uint64_t pending;
	bool active;

	if (gc == NULL) {
		return false;
	}
	high_watermark = atomic_load_explicit(
		&gc->async_high_watermark, memory_order_acquire);
	if (high_watermark == 0) {
		return false;
	}
	low_watermark = atomic_load_explicit(
		&gc->async_low_watermark, memory_order_acquire);
	pending = atomic_load_explicit(
		&g_branch_metrics.gc_pending, memory_order_relaxed);
	active = atomic_load_explicit(
		&gc->async_backpressure, memory_order_acquire);
	if (active && pending <= low_watermark) {
		atomic_store_explicit(
			&gc->async_backpressure, false, memory_order_release);
		active = false;
	} else if (!active && pending >= high_watermark) {
		atomic_store_explicit(
			&gc->async_backpressure, true, memory_order_release);
		active = true;
	}
	atomic_store_explicit(
		&g_branch_metrics.remove_async_backpressure_active,
		active ? 1 : 0, memory_order_relaxed);
	return active;
}

void ds_gc_note_file_unlink(struct ds_gc *gc, uint64_t inode_size)
{
	if (gc == NULL) {
		return;
	}
	atomic_fetch_add_explicit(
		&g_branch_metrics.gc_pending, 1, memory_order_relaxed);
	atomic_fetch_add_explicit(
		&g_branch_metrics.gc_deferred_quota_bytes, inode_size,
		memory_order_relaxed);
	atomic_fetch_add_explicit(
		&g_branch_metrics.gc_deferred_quota_inodes, 1,
		memory_order_relaxed);
	(void)ds_gc_should_backpressure(gc);
}

void ds_gc_stop(struct ds_gc *gc)
{
	if (gc == NULL) {
		return;
	}

	atomic_store_explicit(&gc->stop, true, memory_order_relaxed);
	q_signal_wake(gc);
	pthread_cond_broadcast(&gc->q_not_empty);
	pthread_cond_broadcast(&gc->q_not_full);

	(void)pthread_join(gc->coordinator, NULL);
	for (uint32_t i = 0; i < gc->worker_count; i++) {
		(void)pthread_join(gc->worker_threads[i], NULL);
	}
	ds_gc_free(gc);
}

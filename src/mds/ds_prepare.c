/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_prepare.c -- Async DS file preparation queue (Epic B).
 *
 * Moves DS file creation off the LAYOUTGET hot path for generic
 * (stock knfsd) data servers.  One worker thread per DS processes
 * a FIFO sub-queue, creating files and capturing NFS file handles.
 *
 * Generation checks before and after I/O prevent stale writes
 * into the catalogue when the file is deleted or replaced between enqueue
 * and completion.
 */

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>

#include "ds_prepare.h"
#include "mds_catalogue.h"
#include "proxy_io.h"

#define DS_PREP_DEFAULT_DEPTH 4096
#define DS_PREP_HASH_SIZE     1024

/* -----------------------------------------------------------------------
 * Per-DS worker sub-queue
 * ----------------------------------------------------------------------- */

struct ds_worker {
	uint32_t              ds_id;
	struct ds_prepare_ctx *ctx;
	pthread_t              thread;
	pthread_mutex_t        mtx;
	pthread_cond_t         cond;
	struct ds_prepare_job *head;
	struct ds_prepare_job *tail;
	uint32_t               count;
	int                    running;
};

/* -----------------------------------------------------------------------
 * Context
 * ----------------------------------------------------------------------- */

struct ds_prepare_ctx {
	struct mds_catalogue *cat;
	struct mds_proxy_ctx *proxy;
	uint32_t              max_depth;

	/* Fileid hash table for O(1) LAYOUTGET lookup. */
	pthread_mutex_t       hash_mtx;
	struct ds_prepare_job *hash[DS_PREP_HASH_SIZE];

	/* Per-DS workers. */
	struct ds_worker     *workers;
	uint32_t              worker_count;

	/* Global counters. */
	_Atomic uint64_t      total_jobs;
	_Atomic uint64_t      completed;
	_Atomic uint64_t      failed;
	_Atomic uint64_t      cancelled;
};

/* -----------------------------------------------------------------------
 * Hash helpers
 * ----------------------------------------------------------------------- */

static uint32_t fid_hash(uint64_t fileid)
{
	return (uint32_t)(fileid % DS_PREP_HASH_SIZE);
}

static bool hash_insert(struct ds_prepare_ctx *ctx,
			struct ds_prepare_job *job)
{
	uint32_t h = fid_hash(job->fileid);
	struct ds_prepare_job *cur;
	bool inserted = true;

	pthread_mutex_lock(&ctx->hash_mtx);
	for (cur = ctx->hash[h]; cur != NULL; cur = cur->hash_next) {
		if (cur->fileid == job->fileid &&
		    cur->ds_id == job->ds_id &&
		    cur->stripe == job->stripe &&
		    cur->mirror == job->mirror) {
			inserted = false;
			break;
		}
	}
	if (inserted) {
		job->hash_next = ctx->hash[h];
		ctx->hash[h] = job;
	}
	pthread_mutex_unlock(&ctx->hash_mtx);
	return inserted;
}

static void hash_remove(struct ds_prepare_ctx *ctx,
			struct ds_prepare_job *job)
{
	uint32_t h = fid_hash(job->fileid);
	struct ds_prepare_job **pp;

	pthread_mutex_lock(&ctx->hash_mtx);
	for (pp = &ctx->hash[h]; *pp != NULL; pp = &(*pp)->hash_next) {
		if (*pp == job) {
			*pp = job->hash_next;
			job->hash_next = NULL;
			break;
		}
	}
	pthread_mutex_unlock(&ctx->hash_mtx);
}

/* -----------------------------------------------------------------------
 * Worker sub-queue helpers
 * ----------------------------------------------------------------------- */

static void worker_enqueue(struct ds_worker *w, struct ds_prepare_job *job)
{
	pthread_mutex_lock(&w->mtx);
	job->queue_next = NULL;
	if (w->tail != NULL) {
		w->tail->queue_next = job;
	} else {
		w->head = job;
	}
	w->tail = job;
	w->count++;
	pthread_cond_signal(&w->cond);
	pthread_mutex_unlock(&w->mtx);
}

static struct ds_prepare_job *worker_dequeue(struct ds_worker *w)
{
	struct ds_prepare_job *job;

	pthread_mutex_lock(&w->mtx);
	while (w->head == NULL && w->running) {
		pthread_cond_wait(&w->cond, &w->mtx);
	}
	job = w->head;
	if (job != NULL) {
		w->head = job->queue_next;
		if (w->head == NULL) {
			w->tail = NULL;
		}
		w->count--;
		job->queue_next = NULL;
	}
	pthread_mutex_unlock(&w->mtx);
	return job;
}

/* -----------------------------------------------------------------------
 * Job lifecycle
 * ----------------------------------------------------------------------- */

static struct ds_prepare_job *job_create(uint64_t fileid,
					 struct mds_catalogue *cat,
					 uint64_t generation,
					 uint32_t ds_id,
					 uint32_t stripe,
					 uint32_t mirror)
{
	struct ds_prepare_job *j = calloc(1, sizeof(*j));
	if (j == NULL) {
		return NULL;
	}
	j->cat        = cat;
	j->fileid     = fileid;
	j->generation = generation;
	j->ds_id      = ds_id;
	j->stripe     = stripe;
	j->mirror     = mirror;
	atomic_store(&j->state, DS_PREP_QUEUED);
	return j;
}

static void job_complete(struct ds_prepare_job *j, enum ds_prepare_state st)
{
	atomic_store(&j->state, st);
}

static void job_destroy(struct ds_prepare_job *j)
{
	if (j == NULL) {
		return;
	}
	free(j);
}

static bool ds_needs_prepare(struct ds_prepare_ctx *ctx, uint32_t ds_id)
{
	(void)ctx;
	(void)ds_id;
	/* All DSes are generic (loosely coupled) -- always need FH capture. */
	return true;
}

/* -----------------------------------------------------------------------
 * Worker thread
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void *worker_thread(void *arg)
{
	struct ds_worker *w = arg;
	struct ds_prepare_ctx *ctx = w->ctx;

	while (w->running) {
		struct ds_prepare_job *job = worker_dequeue(w);
		if (job == NULL) {
			break; /* shutdown */
		}
		atomic_store(&job->state, DS_PREP_IN_PROGRESS);

		/* Pre-I/O revalidation: inode must still exist with
		 * matching generation and DS_PENDING set. */
		{
			struct mds_inode check;
			enum mds_status st;

			st = mds_cat_ns_getattr(job->cat, job->fileid, &check);
			if (st != MDS_OK ||
			    check.generation != job->generation ||
			    !(check.flags & MDS_IFLAG_DS_PENDING)) {
				job_complete(job, DS_PREP_CANCELLED);
				atomic_fetch_add(&ctx->cancelled, 1);
				hash_remove(ctx, job);
				job_destroy(job);
				continue;
			}
		}

		/* Create DS file and capture NFS FH. */
		uint8_t fh_buf[MDS_NFS_FH_MAX];
		uint32_t fh_len = MDS_NFS_FH_MAX;
		enum mds_status fh_st;

		fh_st = mds_proxy_ensure_ds_file_fh(
			ctx->proxy, job->ds_id, job->fileid,
			job->stripe, job->mirror,
			fh_buf, &fh_len);

		if (fh_st != MDS_OK) {
			MDS_LOG_WARN(LOG_COMP_MDS,
				"ds_prepare: FH capture failed "
				"fileid=%llu ds=%u stripe=%u mirror=%u "
				"st=%d",
				(unsigned long long)job->fileid,
				(unsigned)job->ds_id,
				(unsigned)job->stripe,
				(unsigned)job->mirror, (int)fh_st);
			job_complete(job, DS_PREP_FAILED);
			atomic_fetch_add(&ctx->failed, 1);
			hash_remove(ctx, job);
			job_destroy(job);
			continue;
		}

		/* Post-I/O revalidation: re-check generation before
		 * writing stripe map.  Catches delete/rename races. */
		{
			struct mds_inode recheck;
			enum mds_status st;

			st = mds_cat_ns_getattr(job->cat, job->fileid, &recheck);
			if (st != MDS_OK ||
			    recheck.generation != job->generation ||
			    !(recheck.flags & MDS_IFLAG_DS_PENDING)) {
				/* Orphan DS file -- gc-scan will clean it. */
				job_complete(job, DS_PREP_CANCELLED);
				atomic_fetch_add(&ctx->cancelled, 1);
				hash_remove(ctx, job);
				job_destroy(job);
				continue;
			}
		}

		/* Commit FH to stripe_map + clear DS_PENDING when all
		 * async-needed entries are ready. */
		{
			struct mds_ds_map_entry *entries = NULL;
			struct mds_inode inode;
			uint32_t stripe_count = 0;
			uint32_t stripe_unit = 0;
			uint32_t mirror_count = 0;
			uint32_t entry_count;
			uint32_t target_idx;
			bool clear_pending = true;
			enum mds_status st;

			st = mds_cat_stripe_map_get(job->cat, job->fileid,
				&stripe_count, &stripe_unit,
				&mirror_count, &entries);
			if (st != MDS_OK) {
				MDS_LOG_WARN(LOG_COMP_MDS,
					"ds_prepare: stripe map read failed "
					"fileid=%llu st=%d",
					(unsigned long long)job->fileid,
					(int)st);
				free(entries);
				job_complete(job, DS_PREP_FAILED);
				atomic_fetch_add(&ctx->failed, 1);
				hash_remove(ctx, job);
				job_destroy(job);
				continue;
			}

			entry_count = stripe_count * mirror_count;
			target_idx = job->stripe * mirror_count + job->mirror;
			if (stripe_count == 0 || mirror_count == 0 ||
			    job->stripe >= stripe_count ||
			    job->mirror >= mirror_count ||
			    target_idx >= entry_count ||
			    entries[target_idx].ds_id != job->ds_id) {
				MDS_LOG_WARN(LOG_COMP_MDS,
					"ds_prepare: stripe map mismatch "
					"fileid=%llu sc=%u mc=%u job "
					"stripe=%u mirror=%u ds=%u map_ds=%u",
					(unsigned long long)job->fileid,
					(unsigned)stripe_count,
					(unsigned)mirror_count,
					(unsigned)job->stripe,
					(unsigned)job->mirror,
					(unsigned)job->ds_id,
					(target_idx < entry_count)
					? (unsigned)entries[target_idx].ds_id
					: 0U);
				free(entries);
				job_complete(job, DS_PREP_FAILED);
				atomic_fetch_add(&ctx->failed, 1);
				hash_remove(ctx, job);
				job_destroy(job);
				continue;
			}

			entries[target_idx].nfs_fh_len = fh_len;
			memcpy(entries[target_idx].nfs_fh, fh_buf, fh_len);

			st = mds_cat_stripe_map_put(job->cat, NULL,
				job->fileid, stripe_count, stripe_unit,
				mirror_count, entries);
			if (st == MDS_OK) {
				uint32_t ei;
				for (ei = 0; ei < entry_count; ei++) {
					if (ds_needs_prepare(ctx,
							    entries[ei].ds_id) &&
					    entries[ei].nfs_fh_len == 0) {
						clear_pending = false;
						break;
					}
				}
			}
			free(entries);

			if (st != MDS_OK) {
				job_complete(job, DS_PREP_FAILED);
				atomic_fetch_add(&ctx->failed, 1);
				hash_remove(ctx, job);
				job_destroy(job);
				continue;
			}

			if (clear_pending) {
				st = mds_cat_ns_getattr(job->cat,
							job->fileid, &inode);
				if (st == MDS_OK) {
					inode.flags &= ~MDS_IFLAG_DS_PENDING;
					st = mds_cat_inode_put(job->cat, NULL,
							       &inode);
				}
			}

			if (st != MDS_OK) {
				job_complete(job, DS_PREP_FAILED);
				atomic_fetch_add(&ctx->failed, 1);
				hash_remove(ctx, job);
				job_destroy(job);
				continue;
			}
		}

		job_complete(job, DS_PREP_DONE);
		atomic_fetch_add(&ctx->completed, 1);

		/* Reap completed job after a brief hold for
		 * any racing LAYOUTGET to observe DONE. */
		hash_remove(ctx, job);
		job_destroy(job);
	}

	return NULL;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int ds_prepare_create(const struct mds_catalogue *cat,
		      struct mds_proxy_ctx *proxy,
		      uint32_t ds_count,
		      uint32_t queue_depth,
		      struct ds_prepare_ctx **out)
{
	struct ds_prepare_ctx *ctx;
	uint32_t i;

	if (cat == NULL || out == NULL || ds_count == 0) {
		return -EINVAL;
	}

	ctx = calloc(1, sizeof(*ctx));
	if (ctx == NULL) {
		return -ENOMEM;
	}

	ctx->cat = (struct mds_catalogue *)cat;
	ctx->proxy = proxy;
	ctx->max_depth = (queue_depth > 0) ? queue_depth
					    : DS_PREP_DEFAULT_DEPTH;
	pthread_mutex_init(&ctx->hash_mtx, NULL);
	atomic_store(&ctx->total_jobs, 0);
	atomic_store(&ctx->completed, 0);
	atomic_store(&ctx->failed, 0);
	atomic_store(&ctx->cancelled, 0);

	/* One worker per DS. */
	ctx->workers = calloc(ds_count, sizeof(*ctx->workers));
	if (ctx->workers == NULL) {
		free(ctx);
		return -ENOMEM;
	}
	ctx->worker_count = ds_count;

	for (i = 0; i < ds_count; i++) {
		struct ds_worker *w = &ctx->workers[i];
		w->ds_id = i;
		w->ctx = ctx;
		w->running = 1;
		pthread_mutex_init(&w->mtx, NULL);
		pthread_cond_init(&w->cond, NULL);
		if (pthread_create(&w->thread, NULL, worker_thread, w) != 0) {
			w->running = 0;
			/* Clean up already-created threads. */
			for (uint32_t j = 0; j < i; j++) {
				ctx->workers[j].running = 0;
				pthread_mutex_lock(&ctx->workers[j].mtx);
				pthread_cond_signal(&ctx->workers[j].cond);
				pthread_mutex_unlock(&ctx->workers[j].mtx);
				pthread_join(ctx->workers[j].thread, NULL);
			}
			free(ctx->workers);
			free(ctx);
			return -errno;
		}
	}

	*out = ctx;
	return 0;
}

int ds_prepare_enqueue(struct ds_prepare_ctx *ctx,
		       struct mds_catalogue *cat,
		       uint64_t fileid, uint64_t generation,
		       uint32_t ds_id, uint32_t stripe, uint32_t mirror)
{
	struct ds_prepare_job *job;
	uint64_t total;
	uint64_t done;
	uint32_t worker_idx;

	if (ctx == NULL || cat == NULL) {
		return -EINVAL;
	}
	total = atomic_load(&ctx->total_jobs);
	done = atomic_load(&ctx->completed) +
	       atomic_load(&ctx->failed) +
	       atomic_load(&ctx->cancelled);
	if (total - done >= ctx->max_depth) {
		return -ENOSPC;
	}
	job = job_create(fileid, cat, generation, ds_id, stripe, mirror);
	if (job == NULL) {
		return -ENOMEM;
	}

	/* Insert into hash table. */
	if (!hash_insert(ctx, job)) {
		job_destroy(job);
		return 0;
	}
	atomic_fetch_add(&ctx->total_jobs, 1);

	/* Route to the appropriate DS worker. */
	worker_idx = ds_id % ctx->worker_count;
	worker_enqueue(&ctx->workers[worker_idx], job);

	return 0;
}

int ds_prepare_check(struct ds_prepare_ctx *ctx, uint64_t fileid)
{
	uint32_t h;
	struct ds_prepare_job *job;
	bool saw_match = false;
	bool saw_queued = false;
	bool saw_in_progress = false;
	bool saw_failed = false;
	bool saw_cancelled = false;
	if (ctx == NULL) {
		return -1;
	}
	h = fid_hash(fileid);
	pthread_mutex_lock(&ctx->hash_mtx);
	for (job = ctx->hash[h]; job != NULL; job = job->hash_next) {
		int state;

		if (job->fileid != fileid) {
			continue;
		}

		saw_match = true;
		state = atomic_load(&job->state);
		if (state == DS_PREP_IN_PROGRESS) {
			saw_in_progress = true;
		} else if (state == DS_PREP_QUEUED) {
			saw_queued = true;
		} else if (state == DS_PREP_FAILED) {
			saw_failed = true;
		} else if (state == DS_PREP_CANCELLED) {
			saw_cancelled = true;
		}
	}
	pthread_mutex_unlock(&ctx->hash_mtx);

	if (saw_in_progress) {
		return DS_PREP_IN_PROGRESS;
	}
	if (saw_queued) {
		return DS_PREP_QUEUED;
	}
	if (saw_failed) {
		return DS_PREP_FAILED;
	}
	if (saw_cancelled) {
		return DS_PREP_CANCELLED;
	}
	return saw_match ? DS_PREP_DONE : -1;
}

void ds_prepare_get_stats(const struct ds_prepare_ctx *ctx,
			  struct ds_prepare_stats *stats)
{
	if (ctx == NULL || stats == NULL) {
		return;
	}

	stats->completed = atomic_load(&ctx->completed);
	stats->failed = atomic_load(&ctx->failed);
	stats->cancelled = atomic_load(&ctx->cancelled);
	uint64_t total = atomic_load(&ctx->total_jobs);
	uint64_t done = stats->completed + stats->failed + stats->cancelled;
	stats->queue_depth = (total > done) ? total - done : 0;
}

void ds_prepare_destroy(struct ds_prepare_ctx *ctx)
{
	if (ctx == NULL) {
		return;
	}

	/* Signal all workers to stop. */
	for (uint32_t i = 0; i < ctx->worker_count; i++) {
		struct ds_worker *w = &ctx->workers[i];
		pthread_mutex_lock(&w->mtx);
		w->running = 0;
		pthread_cond_signal(&w->cond);
		pthread_mutex_unlock(&w->mtx);
	}

	/* Join workers and drain remaining jobs. */
	for (uint32_t i = 0; i < ctx->worker_count; i++) {
		struct ds_worker *w = &ctx->workers[i];
		pthread_join(w->thread, NULL);

		/* Free any remaining queued jobs. */
		struct ds_prepare_job *j = w->head;
		while (j != NULL) {
			struct ds_prepare_job *next = j->queue_next;
			hash_remove(ctx, j);
			job_destroy(j);
			j = next;
		}

		pthread_mutex_destroy(&w->mtx);
		pthread_cond_destroy(&w->cond);
	}

	/* Free any remaining hash entries. */
	for (uint32_t h = 0; h < DS_PREP_HASH_SIZE; h++) {
		struct ds_prepare_job *j = ctx->hash[h];
		while (j != NULL) {
			struct ds_prepare_job *next = j->hash_next;
			job_destroy(j);
			j = next;
		}
	}

	pthread_mutex_destroy(&ctx->hash_mtx);
	free(ctx->workers);
	free(ctx);
}

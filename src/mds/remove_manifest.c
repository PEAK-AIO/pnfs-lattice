/*
 * Copyright (c) 2026 PeakAIO. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-PeakAIO-Proprietary
 *
 * remove_manifest.c — Async-REMOVE delete manifest (mds.conf
 * `remove_async`).  See include/remove_manifest.h for the design
 * summary and invariants; the short version:
 *
 *   - op_remove's fast path inserts an in-memory TOMBSTONE
 *     (dir_fileid, name) and one durable mds_remove_pending row, then
 *     acks the client.  The tombstone hides the name from every read
 *     path until the real ns_remove runs.
 *   - The DRAINER (coordinator + workers, cloned from
 *     unlink_pending_drainer.c) claims rows via the ownership-lease
 *     protocol and executes the guarded ns_remove + final-unlink
 *     cleanup off the request thread.
 *   - Exactly-once execution is decided by the tombstone state
 *     machine (PENDING -> DRAINING under the stripe lock), NOT by the
 *     NDB claim: the claim only provides crash re-claim across MDSes.
 *   - CREATE / RENAME-target collisions and RMDIR call the
 *     force-drain helpers, which either take over execution or wait
 *     for the in-flight executor, so the catalogue never sees a
 *     colliding insert and RMDIR emptiness stays truthful.
 *
 * Locking: 16 stripes, each a mutex + cond + chained hash table.
 * The cond broadcasts on entry removal; force-drain waiters and the
 * submit-initialisation window (state DRAINING until the manifest
 * row commits) are its only consumers.  No lock is held across NDB
 * round-trips.
 */

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "remove_manifest.h"
#include "mds_catalogue.h"
#include "proxy_io.h"
#include "layout_cache.h"
#include "layout_commit_aggregator.h"
#include "quota.h"
#include "mds_metrics.h"

/* MDS_BRANCH_* counter macros are open-coded atomics here. */
#ifndef MDS_BRANCH_ADD
#define MDS_BRANCH_ADD(field, n) \
	atomic_fetch_add_explicit(&g_branch_metrics.field, (n), \
				  memory_order_relaxed)
#endif
#define RM_DEPTH_SET(rm) \
	atomic_store_explicit(&g_branch_metrics.remove_async_depth, \
			      atomic_load(&(rm)->total), \
			      memory_order_relaxed)

#define RM_STRIPES        16U
#define RM_MIN_POLL_MS    10U
/* Force-drain wait budget: 500 x 10 ms = 5 s, far above the worst
 * observed drain latency for a single entry (~2 ms). */
#define RM_WAIT_SLICE_MS  10U
#define RM_WAIT_SLICES    500U
/* Orphan-tombstone scrub cadence (F3): reconcile in-memory notes
 * against manifest rows so peer-drained rows do not leave stale
 * entries behind.  Only runs while notes exist. */
#define RM_SCRUB_INTERVAL_NS (30ULL * 1000000000ULL)

static uint64_t rm_now_ns(void);

enum rm_state {
	RM_PENDING  = 0,	/* eligible for an executor */
	RM_DRAINING = 1,	/* executor elected (or row insert in flight) */
};

struct rm_tombstone {
	uint64_t dir_fileid;
	uint64_t child_fileid;
	uint64_t child_generation;
	uint64_t seq;		/* manifest row PK; 0 while insert in flight */
	uint64_t created_ns;	/* CLOCK_MONOTONIC at insert (scrubber) */
	uint8_t  state;		/* enum rm_state */
	struct rm_tombstone *hash_next;
	char     name[MDS_MAX_NAME + 1];
};

struct rm_stripe {
	pthread_mutex_t     lock;
	pthread_cond_t      cond;	/* broadcast on entry removal */
	struct rm_tombstone **buckets;
	uint32_t            bucket_count;
};

/* Bounded single-batch work ring (unlink_pending_drainer pattern). */
struct rm_queue {
	struct mds_remove_pending_entry *slots;	/* [cap] */
	uint32_t cap;
	uint32_t head;
	uint32_t tail;
	uint32_t pending;
	uint32_t inflight;
	pthread_mutex_t mtx;
	pthread_cond_t  cond_not_empty;
	pthread_cond_t  cond_batch_done;
};

struct remove_manifest {
	struct mds_catalogue            *cat;
	struct mds_proxy_ctx *proxy;
	struct layout_cache             *lcache;
	struct layout_commit_aggregator *lcommit_agg;
	struct mds_quota_ctx            *quota;

	uint32_t mds_id;
	uint64_t boot_epoch;
	uint32_t max_pending;
	uint32_t workers;
	uint32_t batch_size;
	uint32_t poll_ms;
	uint64_t claim_ttl_ns;

	struct rm_stripe stripes[RM_STRIPES];
	_Atomic uint32_t total;

	_Atomic bool running;
	int stop_pipe[2];

	struct rm_queue q;
	struct mds_remove_pending_entry *peek_buf;

	pthread_t  coord_thread;
	bool       coord_started;
	pthread_t *worker_threads;
	uint32_t   workers_started;
};

/* -----------------------------------------------------------------------
 * Hash helpers (same functions as dirent_cache.c so tombstone and
 * dirent-cache keys shard identically under mixed workloads)
 * ----------------------------------------------------------------------- */

static uint64_t rm_splitmix64(uint64_t x)
{
	x ^= x >> 30;
	x *= 0xbf58476d1ce4e5b9ULL;
	x ^= x >> 27;
	x *= 0x94d049bb133111ebULL;
	x ^= x >> 31;
	return x;
}

static uint64_t rm_fnv1a(const char *s)
{
	uint64_t h = 0xcbf29ce484222325ULL;

	for (; *s != '\0'; s++) {
		h ^= (uint64_t)(uint8_t)*s;
		h *= 0x100000001b3ULL;
	}
	return h;
}

static uint64_t rm_hash(uint64_t dir_fileid, const char *name)
{
	return rm_splitmix64(dir_fileid) ^ rm_fnv1a(name);
}

static struct rm_stripe *rm_stripe_for(struct remove_manifest *rm,
				       uint64_t dir_fileid,
				       const char *name)
{
	return &rm->stripes[rm_hash(dir_fileid, name) % RM_STRIPES];
}

/* Caller holds the stripe lock. */
static struct rm_tombstone **rm_bucket_head(struct rm_stripe *st,
					    uint64_t dir_fileid,
					    const char *name)
{
	uint64_t h = rm_hash(dir_fileid, name);

	return &st->buckets[(uint32_t)(h % st->bucket_count)];
}

/* Caller holds the stripe lock. */
static struct rm_tombstone *rm_find_locked(struct rm_stripe *st,
					   uint64_t dir_fileid,
					   const char *name)
{
	struct rm_tombstone *e;

	for (e = *rm_bucket_head(st, dir_fileid, name); e != NULL;
	     e = e->hash_next) {
		if (e->dir_fileid == dir_fileid &&
		    strcmp(e->name, name) == 0) {
			return e;
		}
	}
	return NULL;
}

/* Caller holds the stripe lock.  Unlinks + frees + broadcasts. */
static void rm_remove_locked(struct remove_manifest *rm,
			     struct rm_stripe *st,
			     uint64_t dir_fileid, const char *name)
{
	struct rm_tombstone **pp;

	for (pp = rm_bucket_head(st, dir_fileid, name); *pp != NULL;
	     pp = &(*pp)->hash_next) {
		if ((*pp)->dir_fileid == dir_fileid &&
		    strcmp((*pp)->name, name) == 0) {
			struct rm_tombstone *dead = *pp;

			*pp = dead->hash_next;
			free(dead);
			atomic_fetch_sub(&rm->total, 1U);
	RM_DEPTH_SET(rm);
			pthread_cond_broadcast(&st->cond);
			return;
		}
	}
}


/* -----------------------------------------------------------------------
 * Lattice-native finalize for an acked (delete-at-ack) remove.
 *
 * The ack transaction already removed the dirent and flagged the inode
 * MDS_IFLAG_DELETE_PENDING; this runs the remaining half off the
 * request thread: best-effort DS fence, GC enqueue per unique DS from
 * the inode snapshot (inline 1x1 binding or the stripe map), stripe
 * rows, the inode row itself, quota, and this node's in-memory layout
 * state.  Idempotent: re-running after a crash no-ops on NOTFOUND and
 * the GC drainer tolerates missing DS files.
 * ----------------------------------------------------------------------- */
static void rm_finalize_inode(struct remove_manifest *rm,
			      const struct mds_inode *ino)
{
	struct mds_ds_map_entry *sm = NULL;
	struct mds_ds_map_entry inline_sm;
	uint32_t sc = 0;
	uint32_t su = 0;
	uint32_t mc = 0;
	bool own_sm = false;

	if (ino == NULL || ino->fileid == 0) {
		return;
	}
	if (rm->lcache != NULL) {
		layout_cache_invalidate(rm->lcache, ino->fileid);
	}
	if (rm->lcommit_agg != NULL) {
		layout_commit_aggregator_drop(rm->lcommit_agg, ino->fileid);
	}

	if ((ino->flags & MDS_IFLAG_INLINE_STRIPE) != 0U) {
		uint32_t fhl = ino->inline_fh_len;

		memset(&inline_sm, 0, sizeof(inline_sm));
		if (fhl > MDS_NFS_FH_MAX) {
			fhl = MDS_NFS_FH_MAX;
		}
		inline_sm.ds_id = ino->inline_ds_id;
		inline_sm.nfs_fh_len = fhl;
		if (fhl > 0) {
			memcpy(inline_sm.nfs_fh, ino->inline_fh, fhl);
		}
		sm = &inline_sm;
		sc = 1;
		mc = 1;
		su = ino->stripe_unit;
	} else if (mds_cat_stripe_map_get(rm->cat, ino->fileid, &sc, &su,
					  &mc, &sm) == MDS_OK && sm != NULL) {
		own_sm = true;
	}
	(void)su;

	if (sm != NULL && sc != 0 && mc != 0) {
		uint32_t total = sc * mc;
		uint32_t seen[64];
		uint32_t seen_n = 0;
		uint32_t i;

		for (i = 0; i < total && i < 64U * 64U; i++) {
			uint32_t ds_id = sm[i % (sc * mc)].ds_id;
			bool dup = false;
			uint32_t k;

			for (k = 0; k < seen_n; k++) {
				if (seen[k] == ds_id) {
					dup = true;
					break;
				}
			}
			if (dup || seen_n >= 64U) {
				continue;
			}
			seen[seen_n++] = ds_id;
			{
				uint32_t fhl = sm[i].nfs_fh_len;

				if (fhl > MDS_NFS_FH_MAX) {
					fhl = MDS_NFS_FH_MAX;
				}
				(void)mds_cat_gc_enqueue(rm->cat, NULL,
							 ino->fileid, ds_id,
							 sm[i].nfs_fh, fhl);
			}
		}
	}

	/* Inline-stripe inodes have no side-table rows: the stripe map
	 * lives in the inode row that inode_del below removes.  Skip the
	 * catalogue round-trip; ds_gc still sweeps legacy strays. */
	if ((ino->flags & MDS_IFLAG_INLINE_STRIPE) == 0U) {
		(void)mds_cat_stripe_map_del(rm->cat, NULL, ino->fileid);
	}
	(void)mds_cat_inode_del(rm->cat, NULL, ino->fileid);
	if (rm->quota != NULL) {
		(void)mds_quota_update_remove(rm->quota, ino->uid,
					      ino->gid, ino->size);
	}
	if (own_sm) {
		free(sm);
	}
}

/* -----------------------------------------------------------------------
 * Execution body — the real remove, shared by drainer workers,
 * force-drain callers, and the shutdown flush.  The caller has
 * already won the PENDING -> DRAINING election for this entry (or
 * knows no tombstone exists, e.g. a crashed peer's reloaded row).
 *
 * Returns true when the manifest row was completed (success OR
 * benign guard mismatch); false on a retryable backend failure, in
 * which case the row keeps its claim until the lease lapses and the
 * tombstone (if any) has been reverted to PENDING by the caller.
 * ----------------------------------------------------------------------- */

static bool rm_execute(struct remove_manifest *rm,
		       const struct mds_remove_pending_entry *w)
{
	struct mds_ns_remove_info info;
	enum mds_status st;

	memset(&info, 0, sizeof(info));

	/* Guarded remove: mutate only while the dirent still resolves
	 * to the acked (fileid, generation).  DEFER_PARENT because the
	 * ack path already folded the parent change/mtime delta into
	 * the parent_touch aggregator — bumping again here would
	 * double-count. */
	st = mds_cat_ns_remove_info_verified_flags(
		rm->cat, NULL, w->dir_fileid, w->name,
		w->child_fileid, w->child_generation, &info,
		(uint32_t)MDS_CAT_NSF_DEFER_PARENT);
	if (st == MDS_ERR_NOSUPPORT) {
		/* Backend without the guarded op: two-step fallback.
		 * The recreate race is closed by the force-drain hooks
		 * (CREATE / RENAME / LINK drain the tombstone before
		 * inserting), so a fileid match here is authoritative
		 * enough for the unguarded remove. */
		uint64_t cur_fid = 0;
		uint8_t cur_type = 0;
		enum mds_status lst;

		lst = mds_cat_dirent_get(rm->cat, w->dir_fileid, w->name,
					 &cur_fid, &cur_type);
		if (lst == MDS_OK && cur_fid == w->child_fileid) {
			st = mds_cat_ns_remove_info_flags(
				rm->cat, NULL, w->dir_fileid, w->name,
				&info,
				(uint32_t)MDS_CAT_NSF_DEFER_PARENT);
		} else if (lst == MDS_OK || lst == MDS_ERR_NOTFOUND) {
			st = MDS_ERR_STALE;
		} else {
			st = lst;
		}
	}

	if (st == MDS_OK) {
		const struct mds_inode *ino = &info.child_pre;

		if (ino->fileid != 0 && ino->type == MDS_FTYPE_REG &&
		    ino->nlink == 1) {
			/* Final unlink: same cleanup body as op_remove.
			 * Local in-memory layout state first (snapshot
			 * is the only reliable source), then the
			 * inline fast path, then the persistent
			 * unlink_pending handoff with inline-run
			 * fallback so no unlink ever leaks. */
			rm_finalize_inode(rm, ino);
		}
		if (rm->quota != NULL && ino->fileid != 0) {
			(void)mds_quota_update_remove(rm->quota,
						      ino->uid, ino->gid,
						      ino->size);
		}
		MDS_BRANCH_ADD(remove_async_drained_ok, 1U);
	} else if (st == MDS_ERR_STALE || st == MDS_ERR_NOTFOUND) {
		/* Dirent no longer resolves to the acked child.
		 * Legacy: recreate after force-drain — benign, done.
		 * PHASE-R unlink-at-ack (and its crash-reload shape):
		 * the ack txn already removed the dirent and flagged
		 * the inode; the inode/stripe/DS/quota half still
		 * must run.  Disambiguate via the inode signature —
		 * DELETE_PENDING + matching generation.  Keep-orphan
		 * inodes never have a manifest row, so no false
		 * positive. */
		struct mds_inode ino2;

		memset(&ino2, 0, sizeof(ino2));
		if (mds_cat_ns_getattr(rm->cat, w->child_fileid,
			       &ino2) == MDS_OK &&
		    (ino2.flags & MDS_IFLAG_DELETE_PENDING) != 0U &&
		    ino2.generation == w->child_generation &&
		    ino2.type == MDS_FTYPE_REG) {
			rm_finalize_inode(rm, &ino2);
			free(ino2.ds_map);
			MDS_BRANCH_ADD(remove_async_drained_ok, 1U);
		} else {
			free(ino2.ds_map);
			MDS_BRANCH_ADD(remove_async_drain_mismatch, 1U);
		}
	} else {
		/* Retryable backend failure (NDB unavailable, timeout,
		 * NOSUPPORT on a downgraded backend).  Leave the row;
		 * the claim lease lapse re-queues it here or on a
		 * peer. */
		MDS_BRANCH_ADD(remove_async_drain_fail, 1U);
		(void)mds_cat_remove_pending_bump_retry(rm->cat,
							w->remove_seq);
		free(info.child_pre.ds_map);
		return false;
	}

	free(info.child_pre.ds_map);
	(void)mds_cat_remove_pending_complete(rm->cat, w->remove_seq);
	return true;
}

/* Execute + tombstone teardown for an entry this thread has moved to
 * RM_DRAINING.  On retryable failure the tombstone reverts to
 * RM_PENDING so the name stays hidden and a later pass retries. */
static bool rm_execute_and_clear(struct remove_manifest *rm,
				 const struct mds_remove_pending_entry *w)
{
	struct rm_stripe *st = rm_stripe_for(rm, w->dir_fileid, w->name);
	bool done = rm_execute(rm, w);

	pthread_mutex_lock(&st->lock);
	if (done) {
		rm_remove_locked(rm, st, w->dir_fileid, w->name);
	} else {
		struct rm_tombstone *e =
			rm_find_locked(st, w->dir_fileid, w->name);

		if (e != NULL) {
			e->state = RM_PENDING;
			pthread_cond_broadcast(&st->cond);
		}
	}
	pthread_mutex_unlock(&st->lock);
	return done;
}

/* -----------------------------------------------------------------------
 * Public: ack path
 * ----------------------------------------------------------------------- */

int remove_manifest_submit(struct remove_manifest *rm,
			   uint64_t dir_fileid, const char *name,
			   uint64_t child_fileid,
			   uint64_t child_generation,
			   bool unlink_at_ack)
{
	struct rm_stripe *st;
	struct rm_tombstone *e;
	uint64_t seq = 0;
	size_t nlen;
	enum mds_status cst;

	if (rm == NULL || name == NULL || name[0] == '\0' ||
	    dir_fileid == 0 || child_fileid == 0) {
		return -1;
	}
	nlen = strlen(name);
	if (nlen > MDS_MAX_NAME) {
		return -1;
	}
	if (atomic_load(&rm->total) >= rm->max_pending) {
		MDS_BRANCH_ADD(remove_async_sync_fallback, 1U);
		return -1;
	}

	st = rm_stripe_for(rm, dir_fileid, name);

	/* Insert in DRAINING state: invisible to executors (drainer /
	 * force-drain waits) until the manifest row is durable and the
	 * seq is published below. */
	e = calloc(1, sizeof(*e));
	if (e == NULL) {
		MDS_BRANCH_ADD(remove_async_sync_fallback, 1U);
		return -1;
	}
	e->dir_fileid = dir_fileid;
	e->child_fileid = child_fileid;
	e->child_generation = child_generation;
	e->created_ns = rm_now_ns();
	e->seq = 0;
	e->state = RM_DRAINING;
	memcpy(e->name, name, nlen + 1);

	pthread_mutex_lock(&st->lock);
	if (rm_find_locked(st, dir_fileid, name) != NULL) {
		/* A pending remove already exists for this name — the
		 * caller must not have force-drained.  Refuse; the
		 * synchronous path resolves it authoritatively. */
		pthread_mutex_unlock(&st->lock);
		free(e);
		MDS_BRANCH_ADD(remove_async_sync_fallback, 1U);
		return -1;
	}
	e->hash_next = *rm_bucket_head(st, dir_fileid, name);
	*rm_bucket_head(st, dir_fileid, name) = e;
	atomic_fetch_add(&rm->total, 1U);
	RM_DEPTH_SET(rm);
	pthread_mutex_unlock(&st->lock);

	/* Durable-before-ack.  PHASE-R TEST: in unlink-at-ack
	 * mode the SAME committed txn also deletes the dirent
	 * and flags the inode DELETE_PENDING, so every MDS
	 * misses the name via ordinary catalogue reads. */
	if (unlink_at_ack) {
		cst = mds_cat_remove_pending_enqueue_unlink(rm->cat,
							NULL, dir_fileid,
							name, child_fileid,
							child_generation,
							&seq);
	} else {
		cst = mds_cat_remove_pending_enqueue(rm->cat, NULL,
						     dir_fileid,
						     name, child_fileid,
						     child_generation,
						     &seq);
	}
	pthread_mutex_lock(&st->lock);
	if (cst != MDS_OK) {
		rm_remove_locked(rm, st, dir_fileid, name);
		pthread_mutex_unlock(&st->lock);
		MDS_BRANCH_ADD(remove_async_manifest_insert_fail, 1U);
		return -1;
	}
	e = rm_find_locked(st, dir_fileid, name);
	if (e != NULL) {
		e->seq = seq;
		e->state = RM_PENDING;
		pthread_cond_broadcast(&st->cond);
	}
	pthread_mutex_unlock(&st->lock);

	MDS_BRANCH_ADD(remove_async_acked, 1U);
	if (unlink_at_ack) {
		MDS_BRANCH_ADD(remove_async_ack_unlinked, 1U);
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Public: read-path filter
 * ----------------------------------------------------------------------- */

bool remove_manifest_is_tombstoned(struct remove_manifest *rm,
				   uint64_t dir_fileid,
				   const char *name)
{
	struct rm_stripe *st;
	bool hit;

	if (rm == NULL || name == NULL || name[0] == '\0' ||
	    atomic_load(&rm->total) == 0U) {
		return false;
	}

	st = rm_stripe_for(rm, dir_fileid, name);
	pthread_mutex_lock(&st->lock);
	hit = (rm_find_locked(st, dir_fileid, name) != NULL);
	pthread_mutex_unlock(&st->lock);
	if (hit) {
		MDS_BRANCH_ADD(remove_async_tombstone_hit, 1U);
	}
	return hit;
}

/* -----------------------------------------------------------------------
 * Public: force-drain (mutation-collision hooks)
 * ----------------------------------------------------------------------- */

static void rm_wait_slice(struct rm_stripe *st)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
		return;
	}
	ts.tv_nsec += (long)RM_WAIT_SLICE_MS * 1000000L;
	if (ts.tv_nsec >= 1000000000L) {
		ts.tv_sec += 1;
		ts.tv_nsec -= 1000000000L;
	}
	(void)pthread_cond_timedwait(&st->cond, &st->lock, &ts);
}

int remove_manifest_force_drain_entry(struct remove_manifest *rm,
				      uint64_t dir_fileid,
				      const char *name)
{
	struct rm_stripe *st;
	uint32_t slices;

	if (rm == NULL || name == NULL || name[0] == '\0') {
		return 0;
	}
	if (atomic_load(&rm->total) == 0U) {
		return 0;
	}

	st = rm_stripe_for(rm, dir_fileid, name);
	pthread_mutex_lock(&st->lock);
	for (slices = 0; slices < RM_WAIT_SLICES; slices++) {
		struct rm_tombstone *e =
			rm_find_locked(st, dir_fileid, name);

		if (e == NULL) {
			pthread_mutex_unlock(&st->lock);
			return 0;
		}
		if (e->state == RM_PENDING) {
			struct mds_remove_pending_entry w;

			memset(&w, 0, sizeof(w));
			w.remove_seq = e->seq;
			w.dir_fileid = e->dir_fileid;
			w.child_fileid = e->child_fileid;
			w.child_generation = e->child_generation;
			memcpy(w.name, e->name, sizeof(w.name));
			e->state = RM_DRAINING;
			pthread_mutex_unlock(&st->lock);

			MDS_BRANCH_ADD(remove_async_force_drain, 1U);
			return rm_execute_and_clear(rm, &w) ? 0 : -1;
		}
		/* Executor in flight (drainer worker or the submit
		 * initialisation window) — wait for removal or the
		 * PENDING revert. */
		rm_wait_slice(st);
	}
	pthread_mutex_unlock(&st->lock);
	return -1;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity): stripe scan + drain wait loop */
int remove_manifest_force_drain_dir(struct remove_manifest *rm,
				    uint64_t dir_fileid)
{
	uint32_t si;
	int rc = 0;

	if (rm == NULL || atomic_load(&rm->total) == 0U) {
		return 0;
	}

	/* Snapshot-and-drain per stripe: collect the names first (the
	 * execute path takes the same stripe lock), then drain each
	 * through the single-entry helper. */
	for (si = 0; si < RM_STRIPES; si++) {
		struct rm_stripe *st = &rm->stripes[si];

		for (;;) {
			char name[MDS_MAX_NAME + 1];
			struct rm_tombstone *e = NULL;
			uint32_t b;

			name[0] = '\0';
			pthread_mutex_lock(&st->lock);
			for (b = 0; b < st->bucket_count && e == NULL;
			     b++) {
				for (e = st->buckets[b]; e != NULL;
				     e = e->hash_next) {
					if (e->dir_fileid == dir_fileid) {
						break;
					}
				}
			}
			if (e != NULL) {
				memcpy(name, e->name, sizeof(name));
			}
			pthread_mutex_unlock(&st->lock);

			if (name[0] == '\0') {
				break;
			}
			if (remove_manifest_force_drain_entry(
				    rm, dir_fileid, name) != 0) {
				rc = -1;
				break;
			}
		}
	}
	return rc;
}

uint32_t remove_manifest_pending(const struct remove_manifest *rm)
{
	if (rm == NULL) {
		return 0;
	}
	/* C11 atomic_load lacks a const-qualified form; cast the const
	 * away directly (C17 relaxed this) — the load itself is read-only. */
	return atomic_load(&((struct remove_manifest *)rm)->total);
}

/* -----------------------------------------------------------------------
 * Startup load
 * ----------------------------------------------------------------------- */

struct rm_load_ctx {
	struct remove_manifest *rm;
	int loaded;
	bool failed;
};

static int rm_load_cb(const struct mds_remove_pending_entry *entry,
		      void *ctx)
{
	struct rm_load_ctx *lc = ctx;
	struct remove_manifest *rm = lc->rm;
	struct rm_stripe *st;
	struct rm_tombstone *e;

	if (entry->dir_fileid == 0 || entry->name[0] == '\0') {
		return 0;
	}

	e = calloc(1, sizeof(*e));
	if (e == NULL) {
		lc->failed = true;
		return 1;
	}
	e->dir_fileid = entry->dir_fileid;
	e->child_fileid = entry->child_fileid;
	e->child_generation = entry->child_generation;
	e->created_ns = rm_now_ns();
	e->seq = entry->remove_seq;
	e->state = RM_PENDING;
	(void)snprintf(e->name, sizeof(e->name), "%s", entry->name);

	st = rm_stripe_for(rm, e->dir_fileid, e->name);
	pthread_mutex_lock(&st->lock);
	if (rm_find_locked(st, e->dir_fileid, e->name) != NULL) {
		/* Duplicate rows for one name cannot happen in normal
		 * operation (submit refuses while a tombstone exists);
		 * tolerate anyway — first row wins, the loser is
		 * completed as a mismatch by the drainer. */
		pthread_mutex_unlock(&st->lock);
		free(e);
		return 0;
	}
	e->hash_next = *rm_bucket_head(st, e->dir_fileid, e->name);
	*rm_bucket_head(st, e->dir_fileid, e->name) = e;
	atomic_fetch_add(&rm->total, 1U);
	RM_DEPTH_SET(rm);
	pthread_mutex_unlock(&st->lock);

	lc->loaded++;
	return 0;
}

int remove_manifest_load(struct remove_manifest *rm)
{
	struct rm_load_ctx lc;
	enum mds_status st;

	if (rm == NULL) {
		return -1;
	}
	memset(&lc, 0, sizeof(lc));
	lc.rm = rm;

	st = mds_cat_remove_pending_scan_all(rm->cat, rm_load_cb, &lc);
	if (st != MDS_OK || lc.failed) {
		return -1;
	}
	if (lc.loaded > 0) {
		(void)fprintf(stderr,
			"INFO: remove_manifest: loaded %d pending "
			"remove(s) from the delete manifest; names stay "
			"hidden until drained\n", lc.loaded);
	}
	return lc.loaded;
}

/* -----------------------------------------------------------------------
 * Drainer — coordinator + workers (unlink_pending_drainer pattern)
 * ----------------------------------------------------------------------- */

static uint64_t rm_now_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
		return 0;
	}
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static bool rm_sleep_or_stop(int wake_fd, uint32_t ms)
{
	/* poll(), not pselect()/FD_SET: a busy MDS can hold more than
	 * FD_SETSIZE (1024) open descriptors, so the wake-pipe fd may
	 * exceed that ceiling.  FD_SET() on such an fd trips glibc's
	 * __fdelt_chk ("*** buffer overflow detected ***") and aborts the
	 * process.  poll() imposes no descriptor-value limit. */
	struct pollfd pfd;
	int rc;

	pfd.fd = wake_fd;
	pfd.events = POLLIN;
	pfd.revents = 0;

	rc = poll(&pfd, 1, (ms > (uint32_t)INT_MAX) ? INT_MAX : (int)ms);
	if (rc > 0 && (pfd.revents & (POLLIN | POLLHUP | POLLERR))) {
		char drain;

		(void)(read(wake_fd, &drain, 1) >= 0);
		return false;
	}
	return true;
}

static bool rm_running(const struct remove_manifest *rm)
{
	return atomic_load_explicit(&rm->running, memory_order_acquire);
}

static void *rm_worker_thread(void *arg)
{
	struct remove_manifest *rm = arg;

	for (;;) {
		struct mds_remove_pending_entry w;
		struct rm_stripe *st;
		struct rm_tombstone *e;
		bool execute = false;

		pthread_mutex_lock(&rm->q.mtx);
		while (rm_running(rm) && rm->q.pending == 0) {
			pthread_cond_wait(&rm->q.cond_not_empty,
					  &rm->q.mtx);
		}
		if (rm->q.pending == 0) {
			pthread_mutex_unlock(&rm->q.mtx);
			break;
		}
		w = rm->q.slots[rm->q.head];
		rm->q.head++;
		rm->q.pending--;
		rm->q.inflight++;
		pthread_mutex_unlock(&rm->q.mtx);

		/* Elect this worker as the entry's executor.  A missing
		 * tombstone means either a force-drain completed the
		 * entry between claim and now (row already gone;
		 * rm_execute turns into a benign mismatch/idempotent
		 * complete) or the row belongs to a crashed peer whose
		 * names this node never served — execute either way.
		 * A DRAINING tombstone means someone else is executing:
		 * skip; they will complete the row. */
		st = rm_stripe_for(rm, w.dir_fileid, w.name);
		pthread_mutex_lock(&st->lock);
		e = rm_find_locked(st, w.dir_fileid, w.name);
		if (e == NULL) {
			execute = true;
		} else if (e->state == RM_PENDING && e->seq == w.remove_seq) {
			e->state = RM_DRAINING;
			execute = true;
		}
		pthread_mutex_unlock(&st->lock);

		if (execute) {
			(void)rm_execute_and_clear(rm, &w);
		}

		pthread_mutex_lock(&rm->q.mtx);
		rm->q.inflight--;
		if (rm->q.pending == 0 && rm->q.inflight == 0) {
			pthread_cond_signal(&rm->q.cond_batch_done);
		}
		pthread_mutex_unlock(&rm->q.mtx);
	}
	return NULL;
}

/* -----------------------------------------------------------------------
 * Orphan-tombstone scrubber (F3).
 *
 * When a PEER claims and drains this node's manifest row (crash
 * takeover or cross-node claim), the origin's in-memory tombstone
 * is never cleared: the local drainer only clears entries it
 * executes itself.  The stale note is harmless for correctness
 * (row and inode are gone) but leaks memory, pollutes the depth
 * gauge, and forces a pointless force-drain wait if the same name
 * is recreated.  For every PENDING entry older than min_age_ns,
 * probe its row with a claim: NOTFOUND drops the note; a
 * successful claim means the row is overdue and is drained right
 * here; anything else (lease held elsewhere, transient error)
 * leaves the entry for a later pass.
 * ----------------------------------------------------------------------- */
int remove_manifest_scrub_orphans(struct remove_manifest *rm,
				  uint64_t min_age_ns)
{
	int scrubbed = 0;
	uint64_t now;
	uint32_t si;

	if (rm == NULL || rm->cat == NULL) {
		return 0;
	}
	now = rm_now_ns();

	for (si = 0; si < RM_STRIPES; si++) {
		struct rm_stripe *st = &rm->stripes[si];
		struct mds_remove_pending_entry cand[32];
		uint32_t n = 0;
		uint32_t b;
		uint32_t k;
		struct rm_tombstone *e;

		pthread_mutex_lock(&st->lock);
		for (b = 0; b < st->bucket_count && n < 32U; b++) {
			for (e = st->buckets[b];
			     e != NULL && n < 32U;
			     e = e->hash_next) {
				if (e->state != RM_PENDING ||
				    e->seq == 0 ||
				    now - e->created_ns < min_age_ns) {
					continue;
				}
				memset(&cand[n], 0, sizeof(cand[n]));
				cand[n].remove_seq = e->seq;
				cand[n].dir_fileid = e->dir_fileid;
				cand[n].child_fileid = e->child_fileid;
				cand[n].child_generation =
					e->child_generation;
				memcpy(cand[n].name, e->name,
				       sizeof(cand[n].name));
				e->state = RM_DRAINING;
				n++;
			}
		}
		pthread_mutex_unlock(&st->lock);

		for (k = 0; k < n; k++) {
			enum mds_status cst;

			cst = mds_cat_remove_pending_claim(rm->cat,
					cand[k].remove_seq, rm->mds_id,
					rm->boot_epoch, now,
					rm->claim_ttl_ns);
			if (cst == MDS_OK) {
				/* Row exists and is overdue: drain it. */
				(void)rm_execute_and_clear(rm, &cand[k]);
				continue;
			}
			pthread_mutex_lock(&st->lock);
			if (cst == MDS_ERR_NOTFOUND) {
				/* Row gone: a peer drained it. */
				rm_remove_locked(rm, st,
						 cand[k].dir_fileid,
						 cand[k].name);
				MDS_BRANCH_ADD(
					remove_async_tombstone_scrubbed,
					1U);
				scrubbed++;
			} else {
				/* Lease held elsewhere / transient:
				 * revert for a later pass. */
				e = rm_find_locked(st,
						   cand[k].dir_fileid,
						   cand[k].name);
				if (e != NULL) {
					e->state = RM_PENDING;
					pthread_cond_broadcast(&st->cond);
				}
			}
			pthread_mutex_unlock(&st->lock);
		}
	}
	return scrubbed;
}

static void *rm_coord_thread(void *arg)
{
	uint64_t last_scrub_ns = rm_now_ns();
	struct remove_manifest *rm = arg;

	while (rm_running(rm)) {
		uint32_t n = 0;
		uint32_t staged = 0;
		uint64_t now_ns = rm_now_ns();
		enum mds_status st;
		uint32_t i;

		st = mds_cat_remove_pending_peek_batch(rm->cat, now_ns,
						       rm->peek_buf,
						       rm->batch_size,
						       &n);
		if (st != MDS_OK) {
			n = 0;
		}

		for (i = 0; i < n && rm_running(rm); i++) {
			enum mds_status cst;

			cst = mds_cat_remove_pending_claim(
				rm->cat, rm->peek_buf[i].remove_seq,
				rm->mds_id, rm->boot_epoch, now_ns,
				rm->claim_ttl_ns);
			if (cst != MDS_OK) {
				continue; /* raced; skip */
			}
			pthread_mutex_lock(&rm->q.mtx);
			rm->q.slots[rm->q.tail] = rm->peek_buf[i];
			rm->q.tail++;
			rm->q.pending++;
			pthread_cond_signal(&rm->q.cond_not_empty);
			pthread_mutex_unlock(&rm->q.mtx);
			staged++;
		}

		/* Depth gauge: cheap (atomic read of the tombstone
		 * count), refreshed every tick.  Legacy-global store,
		 * same convention as unlink_pending_depth. */
		atomic_store_explicit(&g_branch_metrics.remove_async_depth,
				      (uint64_t)atomic_load(&rm->total),
				      memory_order_relaxed);

		/* Orphan scrub (F3): reconcile stale in-memory tombstones
		 * left behind when a peer drained our rows. */
		if (atomic_load(&rm->total) > 0 &&
		    rm_now_ns() - last_scrub_ns >= RM_SCRUB_INTERVAL_NS) {
			last_scrub_ns = rm_now_ns();
			(void)remove_manifest_scrub_orphans(
				rm, 2ULL * rm->claim_ttl_ns);
		}

		if (staged > 0) {
			/* Wait for the batch to drain so the next peek
			 * strictly advances (single-peeker property of
			 * the bounded ring). */
			pthread_mutex_lock(&rm->q.mtx);
			while (rm_running(rm) &&
			       (rm->q.pending > 0 || rm->q.inflight > 0)) {
				pthread_cond_wait(&rm->q.cond_batch_done,
						  &rm->q.mtx);
			}
			rm->q.head = 0;
			rm->q.tail = 0;
			pthread_mutex_unlock(&rm->q.mtx);
			continue; /* immediately re-peek while busy */
		}

		if (!rm_sleep_or_stop(rm->stop_pipe[0], rm->poll_ms)) {
			break;
		}
	}
	return NULL;
}

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */

int remove_manifest_init(struct mds_catalogue *cat,
			 struct mds_proxy_ctx *proxy,
			 struct layout_cache *lcache,
			 struct layout_commit_aggregator *lcommit_agg,
			 struct mds_quota_ctx *quota,
			 uint32_t mds_id, uint64_t boot_epoch,
			 uint32_t max_pending,
			 uint32_t workers, uint32_t batch_size,
			 uint32_t poll_ms, uint64_t claim_ttl_ns,
			 struct remove_manifest **out)
{
	struct remove_manifest *rm;
	uint32_t per_stripe;
	uint32_t i;
	uint32_t count_probe = 0;
	enum mds_status pst;

	if (out == NULL) {
		return -1;
	}
	*out = NULL;
	if (cat == NULL || max_pending == 0 || workers == 0 ||
	    batch_size == 0) {
		return -1;
	}

	/* Capability probe: a backend without the manifest table must
	 * fail init so main.c refuses to arm `remove_async`. */
	pst = mds_cat_remove_pending_count(cat, &count_probe);
	if (pst == MDS_ERR_NOSUPPORT) {
		(void)fprintf(stderr,
			"WARN: remove_async requested but the catalogue "
			"backend has no mds_remove_pending support; "
			"keeping the synchronous REMOVE path\n");
		return -1;
	}
	if (pst != MDS_OK) {
		return -1;
	}

	if (max_pending > REMOVE_MANIFEST_MAX_PENDING) {
		max_pending = REMOVE_MANIFEST_MAX_PENDING;
	}
	if (workers > REMOVE_MANIFEST_MAX_WORKERS) {
		workers = REMOVE_MANIFEST_MAX_WORKERS;
	}
	if (batch_size > REMOVE_MANIFEST_MAX_BATCH) {
		batch_size = REMOVE_MANIFEST_MAX_BATCH;
	}
	if (poll_ms < RM_MIN_POLL_MS) {
		poll_ms = RM_MIN_POLL_MS;
	}

	rm = calloc(1, sizeof(*rm));
	if (rm == NULL) {
		return -1;
	}
	rm->cat = cat;
	rm->proxy = proxy;
	rm->lcache = lcache;
	rm->lcommit_agg = lcommit_agg;
	rm->quota = quota;
	rm->mds_id = mds_id;
	rm->boot_epoch = boot_epoch;
	rm->max_pending = max_pending;
	rm->workers = workers;
	rm->batch_size = batch_size;
	rm->poll_ms = poll_ms;
	rm->claim_ttl_ns = claim_ttl_ns;
	rm->stop_pipe[0] = -1;
	rm->stop_pipe[1] = -1;

	per_stripe = (max_pending * 2U) / RM_STRIPES;
	if (per_stripe < 64U) {
		per_stripe = 64U;
	}
	for (i = 0; i < RM_STRIPES; i++) {
		struct rm_stripe *st = &rm->stripes[i];

		st->bucket_count = per_stripe;
		/* NOLINTNEXTLINE(bugprone-sizeof-expression): pointer-array element size is intentional */
		st->buckets = (struct rm_tombstone **)calloc(per_stripe, sizeof(*st->buckets));
		if (st->buckets == NULL) {
			goto fail;
		}
		pthread_mutex_init(&st->lock, NULL);
		pthread_cond_init(&st->cond, NULL);
	}

	rm->q.cap = batch_size;
	rm->q.slots = calloc(batch_size, sizeof(*rm->q.slots));
	rm->peek_buf = calloc(batch_size, sizeof(*rm->peek_buf));
	if (rm->q.slots == NULL || rm->peek_buf == NULL) {
		goto fail;
	}
	pthread_mutex_init(&rm->q.mtx, NULL);
	pthread_cond_init(&rm->q.cond_not_empty, NULL);
	pthread_cond_init(&rm->q.cond_batch_done, NULL);

	rm->worker_threads = calloc(workers, sizeof(*rm->worker_threads));
	if (rm->worker_threads == NULL) {
		goto fail;
	}
	if (pipe(rm->stop_pipe) != 0) {
		goto fail;
	}

	*out = rm;
	return 0;

fail:
	for (i = 0; i < RM_STRIPES; i++) {
		free((void *)rm->stripes[i].buckets);
	}
	free(rm->q.slots);
	free(rm->peek_buf);
	free(rm->worker_threads);
	if (rm->stop_pipe[0] >= 0) {
		close(rm->stop_pipe[0]);
	}
	if (rm->stop_pipe[1] >= 0) {
		close(rm->stop_pipe[1]);
	}
	free(rm);
	return -1;
}

int remove_manifest_start(struct remove_manifest *rm)
{
	uint32_t i;

	if (rm == NULL) {
		return -1;
	}
	atomic_store_explicit(&rm->running, true, memory_order_release);

	for (i = 0; i < rm->workers; i++) {
		if (pthread_create(&rm->worker_threads[i], NULL,
				   rm_worker_thread, rm) != 0) {
			break;
		}
		rm->workers_started++;
	}
	if (rm->workers_started == 0) {
		atomic_store_explicit(&rm->running, false,
				      memory_order_release);
		return -1;
	}
	if (pthread_create(&rm->coord_thread, NULL, rm_coord_thread,
			   rm) != 0) {
		atomic_store_explicit(&rm->running, false,
				      memory_order_release);
		pthread_mutex_lock(&rm->q.mtx);
		pthread_cond_broadcast(&rm->q.cond_not_empty);
		pthread_mutex_unlock(&rm->q.mtx);
		for (i = 0; i < rm->workers_started; i++) {
			(void)pthread_join(rm->worker_threads[i], NULL);
		}
		rm->workers_started = 0;
		return -1;
	}
	rm->coord_started = true;
	return 0;
}

/* Graceful-shutdown flush: single-threaded drain of every remaining
 * tombstone after the drainer threads are stopped.  A failed entry is
 * left in the manifest (durable) for the next boot / a peer. */
static void rm_shutdown_flush(struct remove_manifest *rm)
{
	uint32_t si;
	uint32_t flushed = 0;
	uint32_t left = 0;

	for (si = 0; si < RM_STRIPES; si++) {
		struct rm_stripe *st = &rm->stripes[si];

		for (;;) {
			struct mds_remove_pending_entry w;
			struct rm_tombstone *e = NULL;
			uint32_t b;
			bool found = false;

			memset(&w, 0, sizeof(w));
			pthread_mutex_lock(&st->lock);
			for (b = 0; b < st->bucket_count && e == NULL;
			     b++) {
				for (e = st->buckets[b];
				     e != NULL && e->state != RM_PENDING;
				     e = e->hash_next) {
					;
				}
			}
			if (e != NULL) {
				w.remove_seq = e->seq;
				w.dir_fileid = e->dir_fileid;
				w.child_fileid = e->child_fileid;
				w.child_generation = e->child_generation;
				memcpy(w.name, e->name, sizeof(w.name));
				e->state = RM_DRAINING;
				found = true;
			}
			pthread_mutex_unlock(&st->lock);

			if (!found) {
				break;
			}
			if (rm_execute_and_clear(rm, &w)) {
				flushed++;
			} else {
				/* Leave it durable; stop retrying this
				 * stripe to avoid a shutdown livelock
				 * against a dead backend. */
				left++;
				break;
			}
		}
	}
	if (flushed > 0 || left > 0) {
		(void)fprintf(stderr,
			"INFO: remove_manifest: shutdown flush drained "
			"%u pending remove(s), %u left durable in the "
			"manifest\n", flushed, left);
	}
}

void remove_manifest_destroy(struct remove_manifest *rm)
{
	uint32_t i;

	if (rm == NULL) {
		return;
	}

	if (rm->coord_started || rm->workers_started > 0) {
		atomic_store_explicit(&rm->running, false,
				      memory_order_release);
		if (rm->stop_pipe[1] >= 0) {
			(void)(write(rm->stop_pipe[1], "x", 1) == 1);
		}
		pthread_mutex_lock(&rm->q.mtx);
		pthread_cond_broadcast(&rm->q.cond_not_empty);
		pthread_cond_broadcast(&rm->q.cond_batch_done);
		pthread_mutex_unlock(&rm->q.mtx);

		if (rm->coord_started) {
			(void)pthread_join(rm->coord_thread, NULL);
		}
		for (i = 0; i < rm->workers_started; i++) {
			(void)pthread_join(rm->worker_threads[i], NULL);
		}
	}

	rm_shutdown_flush(rm);

	for (i = 0; i < RM_STRIPES; i++) {
		struct rm_stripe *st = &rm->stripes[i];
		uint32_t b;

		for (b = 0; b < st->bucket_count; b++) {
			struct rm_tombstone *e = st->buckets[b];

			while (e != NULL) {
				struct rm_tombstone *next = e->hash_next;

				free(e);
				e = next;
			}
		}
		free((void *)st->buckets);
		pthread_mutex_destroy(&st->lock);
		pthread_cond_destroy(&st->cond);
	}
	pthread_mutex_destroy(&rm->q.mtx);
	pthread_cond_destroy(&rm->q.cond_not_empty);
	pthread_cond_destroy(&rm->q.cond_batch_done);
	free(rm->q.slots);
	free(rm->peek_buf);
	free(rm->worker_threads);
	if (rm->stop_pipe[0] >= 0) {
		close(rm->stop_pipe[0]);
	}
	if (rm->stop_pipe[1] >= 0) {
		close(rm->stop_pipe[1]);
	}
	free(rm);
}

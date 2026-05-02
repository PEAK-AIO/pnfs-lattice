/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_capacity.c — Live capacity probe for data servers.
 *
 * Background thread that calls statvfs() on each DS's mount path
 * periodically and writes the resulting capacity back to the DS
 * cache.  WRR then picks up the live capacity via the existing
 * ds_cache_overlay_weights() call before placement_select_ex.
 *
 * statvfs() was chosen over a bespoke NFSv3 FSSTAT RPC because it:
 *   - works on any mounted filesystem (NFSv3, NFSv4, local, whatever)
 *   - requires no vendor-specific behaviour from the DS
 *   - reuses the existing DS mount that proxy I/O already sets up
 *   - avoids parsing RPC on a slow path where we already know the
 *     mount is valid (we just mounted it)
 *
 * Limitations:
 *   - If the MDS is not running proxy I/O, the DS is not mounted on
 *     this host and the probe will always fail.  Operators who want
 *     WRR in that case must use static weights (Phase A,
 *     ds_weight.<id>).  The probe failure is silent per-DS so the
 *     fallback path takes over seamlessly.
 *   - statvfs() is a blocking syscall; the polling interval should
 *     comfortably exceed the worst-case statvfs round-trip (typically
 *     sub-millisecond on a cached mount, tens of ms when the DS is
 *     under load).  Default 60s interval is safe.
 */
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>

#include "pnfs_mds.h"
#include "ds_cache.h"
#include "ds_capacity.h"
#include "mds_catalogue.h"

/* -----------------------------------------------------------------------
 * Internal structures
 * ----------------------------------------------------------------------- */

struct ds_capacity {
	struct ds_cache *cache;
	struct mds_catalogue *cat; /* NULL = no shared persistence/reload */
	uint32_t         mds_id;   /* observer id stamped on persisted rows */
	char             mount_path_fmt[128];
	uint32_t         poll_ms;
	enum mds_placement_capacity_weighting mode;

	pthread_t        thread;
	_Atomic bool     running;
	int              stop_pipe[2]; /* write to wake the sleeping thread */
};

/* -----------------------------------------------------------------------
 * Cluster-shared capacity persistence (Phase: multi-MDS df consistency).
 *
 * Each successful local statvfs probe persists total/used onto the
 * DS registry row in RonDB via mds_cat_ds_get + mds_cat_ds_put.
 * Read-modify-write preserves admin-controlled fields (state, weight,
 * addr, mode, capabilities, ...) so two MDSes racing on the probe
 * interval cannot trash each other's admin state — they only race on
 * the four capacity columns, which are within bytes of each other
 * within any probe interval (LWW is benign).
 * ----------------------------------------------------------------------- */
static void capacity_persist_to_catalogue(struct mds_catalogue *cat,
					  uint32_t ds_id,
					  uint64_t total_bytes,
					  uint64_t used_bytes)
{
	struct mds_ds_info info;
	enum mds_status st;

	if (cat == NULL) {
		return;
	}

	st = mds_cat_ds_get(cat, ds_id, &info);
	if (st != MDS_OK) {
		/*
		 * Row missing or transient catalogue error.  Best-
		 * effort: skip this cycle's persistence; the in-memory
		 * cache still reflects the local probe, so this MDS
		 * keeps serving correct df.  Other MDSes that do see
		 * the row will publish their own observations.
		 */
		return;
	}

	info.total_bytes = total_bytes;
	info.used_bytes  = used_bytes;
	(void)mds_cat_ds_put(cat, NULL, &info);
}

/*
 * Pull the current cluster-shared DS view from the catalogue and merge
 * any per-DS observations whose total_bytes is non-zero into the local
 * cache (max-by-merge-time semantics: each merge restamps
 * last_observed_unix_sec to NOW so the staleness gate treats the
 * value as fresh for stale_sec budget).  Called once per probe
 * interval, after the per-DS local probe loop.
 */
static void capacity_reload_cluster_view(struct ds_cache *cache,
					 struct mds_catalogue *cat)
{
	struct mds_ds_info *list = NULL;
	uint32_t count = 0;
	enum mds_status st;

	if (cache == NULL || cat == NULL) {
		return;
	}

	st = mds_cat_ds_list(cat, &list, &count);
	if (st != MDS_OK || list == NULL) {
		free(list);
		return;
	}
	ds_cache_apply_remote_observations(cache, list, count);
	free(list);
}

/* -----------------------------------------------------------------------
 * Pure derivation helper (unit-testable, no cache, no syscalls).
 *
 * Formula is documented in ds_capacity.h; kept linear and branch-free
 * except for the clamp so a future caller (rebalance) can audit the
 * exact value it would see under any (total, used) pair.
 * ----------------------------------------------------------------------- */
uint32_t ds_capacity_derive_auto_weight(uint64_t total_bytes,
					uint64_t used_bytes)
{
	uint64_t free_bytes;
	uint64_t weight;

	if (total_bytes == 0) {
		return 0;
	}
	free_bytes = (used_bytes >= total_bytes)
		? 0
		: total_bytes - used_bytes;

	/* (free * 100) / total fits in u64 because free <= total and
	 * total <= UINT64_MAX / 100 on any realistic DS (> 184 EiB).
	 * On the unlikely >184 EiB case, fall back to a mul-free form
	 * that loses one digit of precision but stays in range. */
	if (total_bytes > UINT64_MAX / 100ULL) {
		weight = (free_bytes / (total_bytes / 100ULL));
	} else {
		weight = (free_bytes * 100ULL) / total_bytes;
	}

	if (weight == 0) {
		weight = 1; /* near-full: stay selectable */
	}
	if (weight > 100) {
		weight = 100; /* defensive; the formula caps at 100 already */
	}
	return (uint32_t)weight;
}

/* -----------------------------------------------------------------------
 * Probe primitive
 * ----------------------------------------------------------------------- */

/*
 * statvfs() the mount path for a single ds_id, compute total and
 * used bytes, and stamp the result onto the cache.  Returns 1 on a
 * successful probe (capacity updated), 0 on probe failure (skip this
 * cycle).  Never holds the cache lock across the syscall.
 *
 * When @p mode is CAP_WEIGHT_PROPORTIONAL, the derived auto_weight
 * is also written to the cache via ds_cache_set_auto_weight.  The
 * operator weight (info.weight) is never touched here — that path
 * stays under mds-admin ds set-weight / ds_weight.<id>.
 *
 * When @p cat is non-NULL, the probe additionally persists
 * total/used onto the DS registry row via read-modify-write so other
 * MDSes (including pure-metadata MDSes that don't proxy-mount the DS)
 * can pick up the observation through the periodic cluster reload.
 */
static int probe_one(struct ds_cache *cache,
		     const char *fmt, uint32_t ds_id,
		     enum mds_placement_capacity_weighting mode,
		     struct mds_catalogue *cat)
{
	char path[256];
	struct statvfs sv;
	uint64_t total_bytes;
	uint64_t avail_bytes;
	uint64_t used_bytes;

	/*
	 * The format is validated by config.c to contain exactly one
	 * %u and no other specifiers.  We silence the warning for this
	 * call because we cannot prove the invariant at this layer.
	 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
	(void)snprintf(path, sizeof(path), fmt, (unsigned)ds_id);
#pragma GCC diagnostic pop

	if (statvfs(path, &sv) != 0) {
		return 0;
	}

	/*
	 * f_frsize is the fundamental block size (bytes); f_blocks the
	 * total block count; f_bavail the blocks available to non-
	 * privileged users.  "Used" is total - available so WRR sees
	 * what the client would see if it tried to write.
	 */
	if (sv.f_frsize == 0 || sv.f_blocks == 0) {
		return 0;
	}
	total_bytes = (uint64_t)sv.f_blocks * (uint64_t)sv.f_frsize;
	avail_bytes = (uint64_t)sv.f_bavail * (uint64_t)sv.f_frsize;
	if (avail_bytes > total_bytes) {
		avail_bytes = total_bytes;
	}
	used_bytes = total_bytes - avail_bytes;

	if (ds_cache_set_capacity(cache, ds_id, total_bytes,
				  used_bytes) != 0) {
		return 0;
	}

	/*
	 * Cluster-shared persistence: stamp the live values onto the
	 * persistent DS registry row so other MDSes can pick them up
	 * via the periodic reload.  Best-effort — a failure here
	 * doesn't roll back the local-cache write that already
	 * succeeded above.  See capacity_persist_to_catalogue for the
	 * RMW-safety rationale.
	 */
	capacity_persist_to_catalogue(cat, ds_id,
				      total_bytes, used_bytes);

	/*
	 * Derive and stamp auto_weight when the policy is on.  A zero
	 * from ds_capacity_derive_auto_weight() maps to "no signal
	 * yet" and the overlay falls back to operator weight / free
	 * bytes, so it's safe to always write.
	 */
	if (mode == CAP_WEIGHT_PROPORTIONAL) {
		uint32_t aw = ds_capacity_derive_auto_weight(total_bytes,
							     used_bytes);
		(void)ds_cache_set_auto_weight(cache, ds_id, aw);
	} else {
		/* Ensure a mode flip from PROPORTIONAL -> OFF clears
		 * stale auto-weights; otherwise a stale value would
		 * keep overlaying. */
		(void)ds_cache_set_auto_weight(cache, ds_id, 0);
	}
	return 1;
}

int ds_capacity_probe_once(struct ds_cache *cache,
			   const char *mount_path_fmt,
			   enum mds_placement_capacity_weighting mode)
{
	uint32_t ids[MDS_MAX_DS_NODES];
	uint32_t n;
	uint32_t i;
	int probed = 0;

	if (cache == NULL || mount_path_fmt == NULL) {
		return 0;
	}
	n = ds_cache_snapshot_ids(cache, ids, MDS_MAX_DS_NODES);
	for (i = 0; i < n; i++) {
		/* Test entry point: no catalogue persistence. */
		probed += probe_one(cache, mount_path_fmt, ids[i],
				    mode, NULL);
	}
	return probed;
}

/* -----------------------------------------------------------------------
 * Polling thread
 * ----------------------------------------------------------------------- */

/*
 * Sleep for up to `ms` milliseconds, but wake early if the stop
 * pipe receives any byte.  Returns true on normal timeout, false on
 * stop signal (thread should exit).
 */
static bool sleep_or_stop(int wake_fd, uint32_t ms)
{
	struct timespec ts;
	fd_set rfds;
	int rc;

	ts.tv_sec = (time_t)(ms / 1000U);
	ts.tv_nsec = (long)(ms % 1000U) * 1000000L;

	FD_ZERO(&rfds);
	FD_SET(wake_fd, &rfds);
	rc = pselect(wake_fd + 1, &rfds, NULL, NULL, &ts, NULL);
	if (rc > 0 && FD_ISSET(wake_fd, &rfds)) {
		char drain;
		(void)read(wake_fd, &drain, 1);
		return false;
	}
	return true;
}

static void *capacity_thread(void *arg)
{
	struct ds_capacity *cap = arg;

	while (atomic_load_explicit(&cap->running, memory_order_acquire)) {
		/*
		 * Per-DS probe loop — each successful local statvfs
		 * also persists to the cluster-shared DS row so peer
		 * MDSes can pick up the observation.  Pure-metadata
		 * MDSes (no proxy I/O, no DS mounts) skip this loop
		 * entirely and depend solely on the reload below.
		 */
		{
			uint32_t ids[MDS_MAX_DS_NODES];
			uint32_t n;
			uint32_t i;

			n = ds_cache_snapshot_ids(cap->cache, ids,
						  MDS_MAX_DS_NODES);
			for (i = 0; i < n; i++) {
				(void)probe_one(cap->cache,
						cap->mount_path_fmt,
						ids[i], cap->mode,
						cap->cat);
			}
		}

		/*
		 * Cluster-view reload: pull peers' freshly-persisted
		 * observations into the local cache.  Lets a metadata-
		 * only MDS (or one whose proxy mount is wedged) still
		 * report a real df by piggybacking on a healthy peer's
		 * statvfs results.  No-op when cap->cat is NULL
		 * (legacy local-only mode) or in the unit-test entry
		 * point above.
		 */
		capacity_reload_cluster_view(cap->cache, cap->cat);

		if (!sleep_or_stop(cap->stop_pipe[0], cap->poll_ms)) {
			break;
		}
	}
	return NULL;
}

/* -----------------------------------------------------------------------
 * Public lifecycle
 * ----------------------------------------------------------------------- */

int ds_capacity_start(struct ds_cache *cache,
		      struct mds_catalogue *cat,
		      uint32_t mds_id,
		      const char *mount_path_fmt,
		      uint32_t poll_ms,
		      enum mds_placement_capacity_weighting mode,
		      struct ds_capacity **out)
{
	struct ds_capacity *cap;

	if (out == NULL) {
		return -1;
	}
	*out = NULL;

	if (cache == NULL || mount_path_fmt == NULL || poll_ms == 0) {
		return 0; /* disabled */
	}

	cap = calloc(1, sizeof(*cap));
	if (cap == NULL) {
		return -1;
	}

	cap->cache = cache;
	cap->cat = cat;
	cap->mds_id = mds_id;
	(void)snprintf(cap->mount_path_fmt, sizeof(cap->mount_path_fmt),
		       "%s", mount_path_fmt);
	cap->poll_ms = poll_ms;
	cap->mode = mode;
	cap->stop_pipe[0] = -1;
	cap->stop_pipe[1] = -1;

	if (pipe(cap->stop_pipe) != 0) {
		free(cap);
		return -1;
	}
	/* Non-blocking read end: the drain in sleep_or_stop() reads
	 * only what was written; defensive against spurious wakes. */
	(void)fcntl(cap->stop_pipe[0], F_SETFL, O_NONBLOCK);

	atomic_store_explicit(&cap->running, true, memory_order_release);
	if (pthread_create(&cap->thread, NULL, capacity_thread, cap) != 0) {
		close(cap->stop_pipe[0]);
		close(cap->stop_pipe[1]);
		free(cap);
		return -1;
	}

	*out = cap;
	return 0;
}

void ds_capacity_stop(struct ds_capacity *cap)
{
	char wake = 'x';

	if (cap == NULL) {
		return;
	}
	atomic_store_explicit(&cap->running, false, memory_order_release);
	(void)write(cap->stop_pipe[1], &wake, 1);
	(void)pthread_join(cap->thread, NULL);
	close(cap->stop_pipe[0]);
	close(cap->stop_pipe[1]);
	free(cap);
}

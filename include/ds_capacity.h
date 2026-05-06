/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_capacity.h -- Live capacity probe for data servers.
 *
 * Periodically calls statvfs() on each DS's mount path (as configured
 * via ds_mount_path_fmt) and stamps the results onto the in-memory DS
 * cache so the WRR placement dispatcher sees real capacity instead of
 * the zero that 3rd-party DSes (NetApp, Isilon, Ceph, plain NFS)
 * report via the catalogue.
 *
 * Works on any mounted filesystem \u2014 statvfs() is the same syscall df(1)
 * uses.  No vendor assumptions, no protocol peeking: if the DS is
 * NFS-mountable (which it is by definition of being a DS) and the MDS
 * has it mounted (the default when proxy I/O is enabled), the probe
 * works.  On probe failure the DS capacity is left untouched and WRR
 * falls back to admin weights (Phase A) or uniform.
 *
 * Runs a single background thread that polls the whole list every
 * `poll_ms`.  Thread-safe; the cache writeback uses the cache's
 * existing rwlock.
 */
#ifndef DS_CAPACITY_H
#define DS_CAPACITY_H

#include <stdint.h>

#include "pnfs_mds.h"  /* enum mds_placement_capacity_weighting */

struct ds_capacity;
struct ds_cache;
struct mds_catalogue;

/**
 * @brief Derive a WRR auto-weight from the fullness ratio.
 *
 * Pure function (no side effects).  Exposed so unit tests and any
 * future rebalance heuristic can reuse the same derivation the
 * probe applies.
 *
 * Formula:
 *   free  = max(0, total - used)
 *   auto  = max(1, floor((free * 100) / total))
 *   clamp to [1, 100].
 *
 * Boundary contract:
 *   total == 0          -> 0 (unknown capacity; overlay falls back)
 *   used  >= total      -> 1 (near-full DS stays selectable)
 *   used  == 0          -> 100 (empty DS; capped)
 *
 * @return A value in {0} U [1, 100].
 */
uint32_t ds_capacity_derive_auto_weight(uint64_t total_bytes,
					uint64_t used_bytes);

/**
 * @brief Start the capacity probe thread.
 *
 * @param cache            DS cache to update on each probe cycle.
 * @param cat              Catalogue handle for the cluster-shared
 *                         capacity persistence (RonDB read-modify-
 *                         write).  May be NULL -- the probe then
 *                         operates in legacy local-only mode and
 *                         neither persists nor reloads from peers.
 * @param mds_id           This MDS's numeric id; recorded as the
 *                         observer when persisting.
 * @param mount_path_fmt   printf-style format with exactly one %u
 *                         placeholder for ds_id.  Same format the
 *                         proxy I/O module uses (`/mnt/ds%u` by
 *                         default).  Already validated by config.c.
 * @param poll_ms          Interval in milliseconds between full
 *                         sweeps.  0 disables the probe; the caller
 *                         gets a NULL @p *out and no thread runs.
 * @param out              Receives the opaque handle (NULL if
 *                         disabled).  NULL-safe to destroy.
 * @return 0 on success (including the disabled case), -1 on a hard
 *         failure (allocation, pthread_create).
 */
int ds_capacity_start(struct ds_cache *cache,
		      struct mds_catalogue *cat,
		      uint32_t mds_id,
		      const char *mount_path_fmt,
		      uint32_t poll_ms,
		      enum mds_placement_capacity_weighting mode,
		      struct ds_capacity **out);

/**
 * @brief Stop the probe thread and release resources.
 *
 * NULL-safe.  Blocks until the thread has joined.
 */
void ds_capacity_stop(struct ds_capacity *cap);

/**
 * @brief Run one synchronous probe cycle.
 *
 * Exposed for unit tests so they do not need to wait for the
 * background thread.  Safe to call while the background thread is
 * also running; both take the cache write-lock for the writeback.
 *
 * @param mode  CAP_WEIGHT_OFF writes only total/used.
 *              CAP_WEIGHT_PROPORTIONAL additionally derives and
 *              writes auto_weight via ds_capacity_derive_auto_weight().
 *
 * @return Number of DSes successfully probed.
 */
int ds_capacity_probe_once(struct ds_cache *cache,
			   const char *mount_path_fmt,
			   enum mds_placement_capacity_weighting mode);

#endif /* DS_CAPACITY_H */

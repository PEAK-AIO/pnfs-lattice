/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_cache.h -- In-memory DS registry cache.
 *
 * Eliminates per-request catalogue reads for DS info and provisioning
 * data on hot paths (LAYOUTGET, CREATE, ds_prealloc, ds_prepare).
 * Populated from catalogue at startup; invalidated on admin commands
 * (ds add/remove/set-state/patch-ready/provision).
 */

#ifndef DS_CACHE_H
#define DS_CACHE_H

#include <stdint.h>
#include <stdbool.h>

#include "pnfs_mds.h"

struct ds_cache;
struct mds_catalogue;

/**
 * @brief Create and populate the DS cache from the catalogue.
 *
 * Reads the full DS registry + provisioning data into memory
 * via the authority vtable.
 *
 * @param cat  Catalogue handle (NULL = create empty cache).
 * @param out  Receives the cache handle.
 * @return 0 on success, -1 on error.
 */
int ds_cache_create(struct mds_catalogue *cat, struct ds_cache **out);

/**
 * @brief Look up a DS by ID.
 *
 * Thread-safe (read-lock).
 *
 * @param cache  Cache handle.
 * @param ds_id  Data server ID.
 * @param info   Receives a copy of the DS info.
 * @return MDS_OK if found, MDS_ERR_NOTFOUND otherwise.
 */
enum mds_status ds_cache_get(const struct ds_cache *cache,
			     uint32_t ds_id,
			     struct mds_ds_info *info);

/**
 * @brief Check if a DS is online.
 *
 * @return true if DS exists and state == DS_ONLINE.
 */
bool ds_cache_is_online(const struct ds_cache *cache, uint32_t ds_id);

/**
 * @brief Check if a DS is patched-ready.
 *
 * @return true if mode == DS_MODE_GENERIC and
 *         capabilities includes 0.
 */
bool ds_cache_is_patched_ready(const struct ds_cache *cache,
			       uint32_t ds_id);

/**
 * @brief Look up provisioning data (secret + epoch) for a DS.
 *
 * @param cache      Cache handle.
 * @param ds_id      Data server ID.
 * @param secret     Receives secret (32 bytes).
 * @param secret_cap Capacity of secret buffer.
 * @param epoch      Receives provisioning epoch.
 * @return MDS_OK if found, MDS_ERR_NOTFOUND otherwise.
 */
enum mds_status ds_cache_get_provision(const struct ds_cache *cache,
				       uint32_t ds_id,
				       uint8_t *secret,
				       uint32_t secret_cap,
				       uint64_t *epoch);

/**
 * @brief Reload the cache from the catalogue.
 *
 * Call after any admin command that modifies DS registry or
 * provisioning data (ds add, remove, set-state, patch-ready,
 * provision).  Acquires write-lock briefly.
 *
 * @param cache  Cache handle.
 * @param cat    Catalogue handle.
 * @return 0 on success, -1 on error (cache remains stale).
 */
int ds_cache_invalidate(struct ds_cache *cache, struct mds_catalogue *cat);

/**
 * @brief Apply operator-configured WRR weights to the cache.
 *
 * Called from main.c after ds_cache_create() to stamp per-DS
 * placement weights from `struct mds_config.ds_weight_by_id`.  The
 * weight field is runtime-only (not persisted in RonDB), so each
 * MDS re-applies its own config on start -- safe because the array
 * is keyed by ds_id and every MDS sees the same ds_id namespace.
 *
 * @param cache    Cache handle (NULL-safe).
 * @param weights  Array of size MDS_MAX_DS_NODES; entries with
 *                 value 0 clear any prior weight (i.e. fall back
 *                 to the free-bytes heuristic).
 */
void ds_cache_apply_weights(struct ds_cache *cache,
			    const uint32_t *weights);

/**
 * @brief Overlay the cached weight field onto a freshly-read DS list.
 *
 * `mds_cat_ds_list` returns DS info straight from the catalogue,
 * which does not persist the runtime `weight` field.  Call this
 * helper after a list read and before handing the array to
 * placement_select_ex so WRR sees operator-configured weights.
 *
 * Entries whose ds_id is not in the cache are left untouched
 * (weight stays zero, i.e. fall back to free-bytes heuristics).
 *
 * @param cache  Cache handle (NULL-safe -- no-op).
 * @param list   DS list to patch.
 * @param count  Number of entries in @list.
 */
void ds_cache_overlay_weights(const struct ds_cache *cache,
			      struct mds_ds_info *list, uint32_t count);

/**
 * @brief Update the live capacity fields for a DS.
 *
 * Called by the ds_capacity probe after a successful statvfs() to
 * stamp `total_bytes` and `used_bytes` on the cached entry.  The
 * cached values are then overlaid onto the next ds_list read via
 * `ds_cache_overlay_weights()` so WRR sees live capacity.
 *
 * @return 0 on success, -1 if @p ds_id is absent from the cache.
 */
int ds_cache_set_capacity(struct ds_cache *cache, uint32_t ds_id,
			  uint64_t total_bytes, uint64_t used_bytes);

/**
 * @brief Update the WRR weight for a single DS.
 *
 * Used by `mds-admin ds set-weight` to retune WRR on a running
 * daemon without a restart.  Runtime-only: the new value is kept
 * in the cache only, so a daemon restart will re-apply the
 * persistent `ds_weight.<id>` INI key.
 *
 * @return 0 on success, -1 if @p ds_id is absent from the cache.
 */
int ds_cache_set_weight(struct ds_cache *cache, uint32_t ds_id,
			uint32_t weight);

/**
 * @brief Set the capacity-derived auto-weight for a DS.
 *
 * Called by the capacity probe when placement_capacity_weighting
 * is non-OFF.  The auto-weight is overlaid onto the DS list only
 * when the operator weight (ds_weight.<id>) is zero, so the
 * operator override always wins.  A zero auto_weight clears any
 * prior derived value and falls back to the free-bytes heuristic.
 *
 * @return 0 on success, -1 if @p ds_id is absent from the cache.
 */
int ds_cache_set_auto_weight(struct ds_cache *cache, uint32_t ds_id,
			     uint32_t auto_weight);

/**
 * @brief Read both weight fields for a DS.
 *
 * Exposed so the admin `ds capacity show` response can report
 * operator weight and derived auto-weight side-by-side.  Either
 * output pointer may be NULL.
 *
 * @return MDS_OK if the entry is present, MDS_ERR_NOTFOUND
 *         otherwise.  Both outputs are set to 0 on NOTFOUND.
 */
enum mds_status ds_cache_get_weights(const struct ds_cache *cache,
				     uint32_t ds_id,
				     uint32_t *operator_weight,
				     uint32_t *auto_weight);

/**
 * @brief Snapshot the list of present ds_ids.
 *
 * Used by the capacity probe to iterate the DS set without holding
 * the cache lock during each statvfs() (which can be slow).
 *
 * @param cache   Cache handle.
 * @param ids     Caller-provided buffer.
 * @param cap     Capacity of @p ids (in elements).
 * @return Number of IDs written (<= min(cap, MDS_MAX_DS_NODES)).
 */
uint32_t ds_cache_snapshot_ids(const struct ds_cache *cache,
			       uint32_t *ids, uint32_t cap);

/**
 * @brief Aggregate live capacity across all online DSes.
 *
 * Iterates the cached DS set and sums total/used bytes for entries
 * whose state == DS_ONLINE and whose capacity probe has produced a
 * non-zero total (i.e. the statvfs probe has actually run).  Any
 * DS without live capacity data is skipped -- it must not contribute
 * a phantom 0 to the sum.
 *
 * Staleness gate: entries whose last in-memory observation is older
 * than @p stale_sec wall-clock seconds are also skipped.  This
 * prevents a DS whose only prober has died from contributing a
 * frozen reading forever.  Pass 0 to disable the gate (legacy
 * behaviour).
 *
 * Used by GETATTR's SPACE_AVAIL/FREE/TOTAL fill-in to report a
 * physically meaningful capacity to the client (instead of an
 * unlimited sentinel) when no quota rules are installed.
 *
 * Saturating arithmetic: if the sum would overflow uint64_t, the
 * result is clamped to UINT64_MAX.  Practical capacities are far
 * below this; the clamp is purely defensive against pathological
 * configurations.
 *
 * @param cache         Cache handle (NULL-safe -- returns zero counts).
 * @param stale_sec     Maximum age (wall-clock seconds) of the most
 *                      recent observation before the entry is
 *                      ignored.  0 = no staleness gate.
 * @param total_bytes   Receives sum of total_bytes (may be NULL).
 * @param used_bytes    Receives sum of used_bytes (may be NULL).
 * @param ds_count_out  Receives number of online DSes contributing
 *                      to the sum (may be NULL).  Zero means no DS
 *                      had live capacity data and the totals are
 *                      not meaningful.
 */
void ds_cache_aggregate_capacity(const struct ds_cache *cache,
				 uint32_t stale_sec,
				 uint64_t *total_bytes,
				 uint64_t *used_bytes,
				 uint32_t *ds_count_out);

/**
 * @brief Merge a freshly-read DS list (from RonDB) into the local
 *        capacity cache.
 *
 * For each entry in @p list whose total_bytes is non-zero, copies
 * total_bytes/used_bytes into the local cache row and stamps the
 * "last observed" wall-clock timestamp to NOW so the staleness gate
 * in ds_cache_aggregate_capacity treats the merged value as fresh.
 *
 * Rows whose total_bytes is zero in @p list are NOT merged -- a zero
 * means the source MDS has not yet probed the DS, and we don't want
 * to overwrite a fresher local observation with a phantom zero.
 *
 * Used by the periodic cluster-view reload in src/mds/ds_capacity.c
 * so MDSes that don't proxy-mount a given DS still see the latest
 * observation written by an MDS that does.
 *
 * @param cache  Cache handle (NULL-safe -- no-op).
 * @param list   DS list freshly read from the catalogue.
 * @param count  Number of entries in @p list.
 */
void ds_cache_apply_remote_observations(struct ds_cache *cache,
					const struct mds_ds_info *list,
					uint32_t count);

/**
 * @brief Destroy the cache and free all resources.
 */
void ds_cache_destroy(struct ds_cache *cache);

#endif /* DS_CACHE_H */

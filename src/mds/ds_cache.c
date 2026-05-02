/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_cache.c — In-memory DS registry cache.
 *
 * Indexed by ds_id (0..MDS_MAX_DS_NODES-1).  Populated from catalogue
 * at startup, reloaded on admin commands.  All hot-path lookups
 * use read-locks (multiple concurrent readers, no catalogue I/O).
 */

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>

#include "ds_cache.h"
#include "pnfs_mds.h"
#include "mds_catalogue.h"

struct ds_cache_entry {
	struct mds_ds_info info;
	uint8_t            secret[32];
	uint64_t           provision_epoch;
	bool               present;          /* ds_id registered */
	bool               provisioned;      /* secret + epoch loaded */
	/*
	 * Capacity-derived WRR weight (Phase B2).  Written by the
	 * statvfs probe when placement_capacity_weighting !=
	 * CAP_WEIGHT_OFF.  Layered *under* info.weight in
	 * ds_cache_overlay_weights so an operator override always wins.
	 * 0 = unset (either the feature is off or the probe has not run
	 * yet) and overlay leaves the list entry alone, letting the
	 * placement path's free-bytes heuristic take over.
	 */
	uint32_t           auto_weight;
	/*
	 * Wall-clock (CLOCK_REALTIME) seconds at which info.total_bytes /
	 * info.used_bytes were last refreshed — either by a successful
	 * local statvfs probe or by a remote-observation merge from
	 * ds_cache_apply_remote_observations().  Zero means "never
	 * observed".  Read by ds_cache_aggregate_capacity to gate stale
	 * readings out of the GETATTR fill-in.  Internal-only — never
	 * persisted, never exposed to clients or other subsystems; the
	 * mds_ds_info struct (which IS persisted via mds_cat_ds_put)
	 * deliberately does not carry this field.
	 */
	uint64_t           last_observed_unix_sec;
};

static uint64_t cache_now_unix_sec(void)
{
	struct timespec ts;
	if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
		return 0;
	}
	return (uint64_t)ts.tv_sec;
}

struct ds_cache {
	pthread_rwlock_t     lock;
	struct ds_cache_entry entries[MDS_MAX_DS_NODES];
};

/* -----------------------------------------------------------------------
 * Internal: load all DS entries from catalogue into the cache.
 * Caller must hold write-lock.
 * ----------------------------------------------------------------------- */

static int cache_reload_locked(struct ds_cache *cache,
                               struct mds_catalogue *cat)
{
	struct ds_cache_entry *entries = NULL;
	struct mds_ds_info *list = NULL;
	uint32_t count = 0;
	uint32_t i;
	enum mds_status st;
	entries = calloc(MDS_MAX_DS_NODES, sizeof(*entries));
	if (entries == NULL) {
		return -1;
	}

	st = mds_cat_ds_list(cat, &list, &count);
	if (st != MDS_OK) {
		free(entries);
		free(list);
		return -1;
	}

	for (i = 0; i < count; i++) {
		uint32_t id = list[i].ds_id;

		if (id >= MDS_MAX_DS_NODES) {
			continue;
		}
		entries[id].info = list[i];
		entries[id].present = true;

		/* Try to load provisioning data. */
		st = mds_cat_ds_provision_get(
			cat, id,
			entries[id].secret,
			sizeof(entries[id].secret),
			&entries[id].provision_epoch);
		entries[id].provisioned = (st == MDS_OK);
	}

	memcpy(cache->entries, entries, sizeof(cache->entries));
	free(entries);
	free(list);
	return 0;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int ds_cache_create(struct mds_catalogue *cat, struct ds_cache **out)
{
	struct ds_cache *c;

	if (out == NULL) {
		return -1;
	}
	*out = NULL;

	c = calloc(1, sizeof(*c));
	if (c == NULL) {
		return -1;
	}

	if (pthread_rwlock_init(&c->lock, NULL) != 0) {
		free(c);
		return -1;
	}

	if (cat != NULL && cache_reload_locked(c, cat) != 0) {
		pthread_rwlock_destroy(&c->lock);
		free(c);
		return -1;
	}

	*out = c;
	return 0;
}

enum mds_status ds_cache_get(const struct ds_cache *cache,
			     uint32_t ds_id,
			     struct mds_ds_info *info)
{
	enum mds_status st;

	if (cache == NULL || info == NULL || ds_id >= MDS_MAX_DS_NODES) {
		return MDS_ERR_NOTFOUND;
	}

	pthread_rwlock_rdlock((pthread_rwlock_t *)&cache->lock);
	if (cache->entries[ds_id].present) {
		*info = cache->entries[ds_id].info;
		st = MDS_OK;
	} else {
		st = MDS_ERR_NOTFOUND;
	}
	pthread_rwlock_unlock((pthread_rwlock_t *)&cache->lock);
	return st;
}

bool ds_cache_is_online(const struct ds_cache *cache, uint32_t ds_id)
{
	bool online = false;

	if (cache == NULL || ds_id >= MDS_MAX_DS_NODES) {
		return false;
	}

	pthread_rwlock_rdlock((pthread_rwlock_t *)&cache->lock);
	if (cache->entries[ds_id].present) {
		online = (cache->entries[ds_id].info.state == DS_ONLINE);
	}
	pthread_rwlock_unlock((pthread_rwlock_t *)&cache->lock);
	return online;
}

bool ds_cache_is_patched_ready(const struct ds_cache *cache,
			       uint32_t ds_id)
{
	bool ready = false;

	if (cache == NULL || ds_id >= MDS_MAX_DS_NODES) {
		return false;
	}

	pthread_rwlock_rdlock((pthread_rwlock_t *)&cache->lock);
	if (cache->entries[ds_id].present) {
		const struct mds_ds_info *info = &cache->entries[ds_id].info;
		ready = (info->mode == DS_MODE_GENERIC &&
			 info->state == DS_ONLINE);
	}
	pthread_rwlock_unlock((pthread_rwlock_t *)&cache->lock);
	return ready;
}

enum mds_status ds_cache_get_provision(const struct ds_cache *cache,
				       uint32_t ds_id,
				       uint8_t *secret,
				       uint32_t secret_cap,
				       uint64_t *epoch)
{
	enum mds_status st;

	if (cache == NULL || secret == NULL || epoch == NULL ||
	    secret_cap < 32 || ds_id >= MDS_MAX_DS_NODES) {
		return MDS_ERR_NOTFOUND;
	}

	pthread_rwlock_rdlock((pthread_rwlock_t *)&cache->lock);
	if (cache->entries[ds_id].present &&
	    cache->entries[ds_id].provisioned) {
		memcpy(secret, cache->entries[ds_id].secret,
		       32);
		*epoch = cache->entries[ds_id].provision_epoch;
		st = MDS_OK;
	} else {
		st = MDS_ERR_NOTFOUND;
	}
	pthread_rwlock_unlock((pthread_rwlock_t *)&cache->lock);
	return st;
}

int ds_cache_invalidate(struct ds_cache *cache, struct mds_catalogue *cat)
{
	int rc;

	if (cache == NULL || cat == NULL) {
		return -1;
	}

	pthread_rwlock_wrlock(&cache->lock);
	rc = cache_reload_locked(cache, cat);
	pthread_rwlock_unlock(&cache->lock);
	return rc;
}

void ds_cache_destroy(struct ds_cache *cache)
{
	if (cache == NULL) {
		return;
	}
	pthread_rwlock_destroy(&cache->lock);
	free(cache);
}

void ds_cache_apply_weights(struct ds_cache *cache,
			    const uint32_t *weights)
{
	uint32_t i;

	if (cache == NULL || weights == NULL) {
		return;
	}

	pthread_rwlock_wrlock(&cache->lock);
	for (i = 0; i < MDS_MAX_DS_NODES; i++) {
		if (cache->entries[i].present) {
			cache->entries[i].info.weight = weights[i];
		}
	}
	pthread_rwlock_unlock(&cache->lock);
}

void ds_cache_overlay_weights(const struct ds_cache *cache,
			      struct mds_ds_info *list, uint32_t count)
{
	uint32_t i;

	if (cache == NULL || list == NULL || count == 0) {
		return;
	}

	pthread_rwlock_rdlock((pthread_rwlock_t *)&cache->lock);
	for (i = 0; i < count; i++) {
		uint32_t id = list[i].ds_id;

		if (id < MDS_MAX_DS_NODES &&
			    cache->entries[id].present) {
			const struct ds_cache_entry *e = &cache->entries[id];
			const struct mds_ds_info *c = &e->info;

			/*
			 * Weight precedence (highest first):
			 *   1. Operator override c->weight (ds_weight.<id>
			 *      or mds-admin ds set-weight).  Non-zero wins
			 *      unconditionally.
			 *   2. Capacity-derived auto_weight from the probe
			 *      (only when placement_capacity_weighting !=
			 *      OFF; else it stays 0).
			 *   3. Free-bytes heuristic inside the placement
			 *      path (see fill_online_with_free), consulted
			 *      when both of the above are zero.
			 */
			if (c->weight != 0) {
				list[i].weight = c->weight;
			} else if (e->auto_weight != 0) {
				list[i].weight = e->auto_weight;
			} else {
				list[i].weight = 0;
			}
			/*
			 * Overlay live capacity when the probe has
			 * collected it (total_bytes != 0).  A zero means
			 * the probe has not run or failed; leave the list
			 * entry's catalogue value intact so WRR still has
			 * something to work with (may also be zero on 3rd-
			 * party DSes, in which case the admin-weight path
			 * takes over).
			 */
			if (c->total_bytes != 0) {
				list[i].total_bytes = c->total_bytes;
				list[i].used_bytes = c->used_bytes;
			}
		}
	}
	pthread_rwlock_unlock((pthread_rwlock_t *)&cache->lock);
}

int ds_cache_set_capacity(struct ds_cache *cache, uint32_t ds_id,
			  uint64_t total_bytes, uint64_t used_bytes)
{
	int rc = -1;
	uint64_t now_sec;

	if (cache == NULL || ds_id >= MDS_MAX_DS_NODES) {
		return -1;
	}

	now_sec = cache_now_unix_sec();

	pthread_rwlock_wrlock(&cache->lock);
	if (cache->entries[ds_id].present) {
		cache->entries[ds_id].info.total_bytes = total_bytes;
		cache->entries[ds_id].info.used_bytes = used_bytes;
		cache->entries[ds_id].last_observed_unix_sec = now_sec;
		rc = 0;
	}
	pthread_rwlock_unlock(&cache->lock);
	return rc;
}

int ds_cache_set_weight(struct ds_cache *cache, uint32_t ds_id,
			uint32_t weight)
{
	int rc = -1;

	if (cache == NULL || ds_id >= MDS_MAX_DS_NODES) {
		return -1;
	}

	pthread_rwlock_wrlock(&cache->lock);
	if (cache->entries[ds_id].present) {
		cache->entries[ds_id].info.weight = weight;
		rc = 0;
	}
	pthread_rwlock_unlock(&cache->lock);
	return rc;
}

int ds_cache_set_auto_weight(struct ds_cache *cache, uint32_t ds_id,
			     uint32_t auto_weight)
{
	int rc = -1;

	if (cache == NULL || ds_id >= MDS_MAX_DS_NODES) {
		return -1;
	}

	pthread_rwlock_wrlock(&cache->lock);
	if (cache->entries[ds_id].present) {
		cache->entries[ds_id].auto_weight = auto_weight;
		rc = 0;
	}
	pthread_rwlock_unlock(&cache->lock);
	return rc;
}

enum mds_status ds_cache_get_weights(const struct ds_cache *cache,
				     uint32_t ds_id,
				     uint32_t *operator_weight,
				     uint32_t *auto_weight)
{
	enum mds_status st = MDS_ERR_NOTFOUND;

	if (operator_weight != NULL) { *operator_weight = 0; }
	if (auto_weight != NULL)     { *auto_weight = 0; }

	if (cache == NULL || ds_id >= MDS_MAX_DS_NODES) {
		return MDS_ERR_NOTFOUND;
	}

	pthread_rwlock_rdlock((pthread_rwlock_t *)&cache->lock);
	if (cache->entries[ds_id].present) {
		if (operator_weight != NULL) {
			*operator_weight =
				cache->entries[ds_id].info.weight;
		}
		if (auto_weight != NULL) {
			*auto_weight =
				cache->entries[ds_id].auto_weight;
		}
		st = MDS_OK;
	}
	pthread_rwlock_unlock((pthread_rwlock_t *)&cache->lock);
	return st;
}

/*
 * Decide whether @p e contributes to the live aggregate.
 *
 * Eligibility rules (must all hold):
 *   - entry is present in the cache
 *   - DS is administratively ONLINE
 *   - the entry has a non-zero total_bytes (skip phantom-0 readings
 *     so a never-probed DS doesn't drag the aggregate)
 *   - if @p stale_sec > 0, the entry's last_observed_unix_sec
 *     is recent enough (a never-observed entry, with
 *     last_observed_unix_sec == 0, is treated as stale)
 *
 * Caller must already hold the cache lock; this function does
 * not touch shared state.
 */
static bool aggregate_entry_eligible(const struct ds_cache_entry *e,
				     uint32_t stale_sec,
				     uint64_t now_sec)
{
	uint64_t obs;

	if (!e->present) {
		return false;
	}
	if (e->info.state != DS_ONLINE) {
		return false;
	}
	if (e->info.total_bytes == 0) {
		return false;
	}
	if (stale_sec == 0) {
		return true;
	}
	obs = e->last_observed_unix_sec;
	if (obs == 0) {
		return false;
	}
	if (now_sec > obs && (now_sec - obs) > (uint64_t)stale_sec) {
		return false;
	}
	return true;
}

/* Saturating add into *acc. */
static void sat_add_u64(uint64_t *acc, uint64_t v)
{
	if (*acc > UINT64_MAX - v) {
		*acc = UINT64_MAX;
	} else {
		*acc += v;
	}
}

void ds_cache_aggregate_capacity(const struct ds_cache *cache,
				 uint32_t stale_sec,
				 uint64_t *total_bytes,
				 uint64_t *used_bytes,
				 uint32_t *ds_count_out)
{
	uint64_t total = 0;
	uint64_t used = 0;
	uint32_t count = 0;
	uint32_t i;
	uint64_t now_sec;

	if (total_bytes != NULL) { *total_bytes = 0; }
	if (used_bytes != NULL)  { *used_bytes = 0; }
	if (ds_count_out != NULL){ *ds_count_out = 0; }

	if (cache == NULL) {
		return;
	}

	now_sec = (stale_sec > 0) ? cache_now_unix_sec() : 0;

	pthread_rwlock_rdlock((pthread_rwlock_t *)&cache->lock);
	for (i = 0; i < MDS_MAX_DS_NODES; i++) {
		const struct ds_cache_entry *e = &cache->entries[i];
		uint64_t add_total;
		uint64_t add_used;

		if (!aggregate_entry_eligible(e, stale_sec, now_sec)) {
			continue;
		}

		add_total = e->info.total_bytes;
		add_used  = (e->info.used_bytes <= add_total)
				? e->info.used_bytes
				: add_total;

		sat_add_u64(&total, add_total);
		sat_add_u64(&used, add_used);
		count++;
	}
	pthread_rwlock_unlock((pthread_rwlock_t *)&cache->lock);

	if (total_bytes != NULL) { *total_bytes = total; }
	if (used_bytes != NULL)  { *used_bytes  = used; }
	if (ds_count_out != NULL){ *ds_count_out = count; }
}

void ds_cache_apply_remote_observations(struct ds_cache *cache,
					const struct mds_ds_info *list,
					uint32_t count)
{
	uint32_t i;
	uint64_t now_sec;

	if (cache == NULL || list == NULL || count == 0) {
		return;
	}

	now_sec = cache_now_unix_sec();

	pthread_rwlock_wrlock(&cache->lock);
	for (i = 0; i < count; i++) {
		uint32_t id = list[i].ds_id;

		if (id >= MDS_MAX_DS_NODES) {
			continue;
		}
		if (!cache->entries[id].present) {
			continue;
		}
		/* Skip phantom-0 rows: a row whose persisted total_bytes
		 * is zero either represents a DS no MDS has ever probed
		 * yet, or a legacy registry seed without capacity info.
		 * Either way, merging zero would clobber a possibly
		 * fresher local observation. */
		if (list[i].total_bytes == 0) {
			continue;
		}
		cache->entries[id].info.total_bytes = list[i].total_bytes;
		cache->entries[id].info.used_bytes  = list[i].used_bytes;
		cache->entries[id].last_observed_unix_sec = now_sec;
	}
	pthread_rwlock_unlock(&cache->lock);
}

uint32_t ds_cache_snapshot_ids(const struct ds_cache *cache,
			       uint32_t *ids, uint32_t cap)
{
	uint32_t n = 0;
	uint32_t i;

	if (cache == NULL || ids == NULL || cap == 0) {
		return 0;
	}

	pthread_rwlock_rdlock((pthread_rwlock_t *)&cache->lock);
	for (i = 0; i < MDS_MAX_DS_NODES && n < cap; i++) {
		if (cache->entries[i].present) {
			ids[n++] = cache->entries[i].info.ds_id;
		}
	}
	pthread_rwlock_unlock((pthread_rwlock_t *)&cache->lock);
	return n;
}

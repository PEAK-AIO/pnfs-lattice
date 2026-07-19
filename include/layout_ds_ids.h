/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * layout_ds_ids.h -- Helpers for durable pNFS layout-state DS ID lists.
 */

#ifndef LAYOUT_DS_IDS_H
#define LAYOUT_DS_IDS_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "pnfs_mds.h"

/*
 * Durable layout-state indexes need one row per DS that can host any
 * stripe of the grant.  Bound the caller-supplied list by the same
 * software geometry limit that bounds stripe maps.
 *
 * MDS_LAYOUT_DS_ID_MAX is the GLOBAL ceiling enforced by the catalogue
 * dispatch layer (mds_coord_layout_grant / _return in
 * src/catalogue/catalogue_dispatch.c).  Individual backends MAY apply
 * a narrower cap when their on-disk row format or transaction size
 * cannot accommodate the full ceiling: the in-memory test backend
 * (tests/catalogue_memdb.c) deep-copies the full list, while the
 * RonDB shim is bounded by the per-transaction op-count limit
 * (~4096) and is documented as a follow-up at
 * src/catalogue/catalogue_rondb.c::catalogue_rondb_stripe_map_get.
 * Backends that reject a grant must return MDS_ERR_NOSUPP /
 * MDS_ERR_NOSPC so the caller can surface the limit to operators
 * rather than silently truncating.
 */
#define MDS_LAYOUT_DS_ID_MAX \
	((uint32_t)(MDS_MAX_STRIPES * MDS_MAX_MIRRORS))

_Static_assert((uint64_t)MDS_MAX_STRIPES * (uint64_t)MDS_MAX_MIRRORS <=
	       UINT32_MAX,
	       "layout DS ID ceiling overflows uint32_t");

struct layout_ds_id_list {
	uint32_t *ids;		/* heap, owned by the list */
	uint32_t count;
};

/**
 * Check whether a layout-state DS count is within the software limit.
 *
 * @param count  Number of DS IDs supplied by the caller.
 * @return true when count is valid.
 */
static inline bool layout_ds_id_count_valid(uint32_t count)
{
	return count <= MDS_LAYOUT_DS_ID_MAX;
}

/*
 * Linear membership scan used by the dedup loop in
 * layout_ds_id_list_from_entries() below.  This is intentionally a
 * straight-line search: typical layouts have stripe_count <= 128
 * so the O(n^2) total cost stays under ~16 K comparisons.  At
 * MDS_MAX_STRIPES = 1024 the cost climbs to ~1 M comparisons per
 * grant; if 1024-stripe layouts ever become hot enough to matter,
 * replace this with a sort + unique pass or a small hashset.
 * Tracked as a follow-up in docs/hpc-nto1-plan.md.
 */
static inline bool layout_ds_id_seen(const uint32_t *ids, uint32_t count,
				     uint32_t ds_id)
{
	uint32_t i;

	for (i = 0; i < count; i++) {
		if (ids[i] == ds_id) {
			return true;
		}
	}
	return false;
}

/**
 * Build a heap-owned unique DS-ID list from stripe-map entries.
 *
 * The returned list owns @c ids and the caller must release it with
 * layout_ds_id_list_destroy().  Duplicate DS IDs are collapsed because
 * the durable reverse index is keyed per DS/client/file, not per stripe.
 *
 * @param list         Output list; reset to empty before population.
 * @param entries      Stripe-map entries to scan.
 * @param entry_count  Number of entries in @p entries.
 * @return MDS_OK on success, MDS_ERR_INVAL for invalid input, or
 *         MDS_ERR_NOMEM on allocation failure.
 */
static inline enum mds_status layout_ds_id_list_from_entries(
	struct layout_ds_id_list *list,
	const struct mds_ds_map_entry *entries,
	uint32_t entry_count)
{
	uint32_t *ids;
	uint32_t i;
	uint32_t count = 0;

	if (list == NULL) {
		return MDS_ERR_INVAL;
	}
	list->ids = NULL;
	list->count = 0;

	if (entry_count == 0) {
		return MDS_OK;
	}
	if (entries == NULL || !layout_ds_id_count_valid(entry_count)) {
		return MDS_ERR_INVAL;
	}

	ids = calloc(entry_count, sizeof(*ids));
	if (ids == NULL) {
		return MDS_ERR_NOMEM;
	}

	for (i = 0; i < entry_count; i++) {
		/* DS registration is 0-based (main.c seeds info.ds_id = di
		 * from di=0), so ds_id 0 is a legitimate data server -- the
		 * first one.  Only a genuinely unfilled/phantom stripe (no
		 * ds_id AND no captured filehandle) is invalid; a real
		 * stripe that happens to land on DS 0 carries a filehandle
		 * and must be kept.  Rejecting ds_id==0 outright made every
		 * wide layout that spans all DS (which necessarily includes
		 * DS 0) fail with NFS4ERR_IO. */
		if (entries[i].ds_id == 0 && entries[i].nfs_fh_len == 0) {
			free(ids);
			return MDS_ERR_INVAL;
		}
		if (!layout_ds_id_seen(ids, count, entries[i].ds_id)) {
			ids[count] = entries[i].ds_id;
			count++;
		}
	}

	list->ids = ids;
	list->count = count;
	return MDS_OK;
}

/**
 * Free a layout DS-ID list.
 *
 * @param list  List returned by layout_ds_id_list_from_entries().
 */
static inline void layout_ds_id_list_destroy(struct layout_ds_id_list *list)
{
	if (list == NULL) {
		return;
	}
	free(list->ids);
	list->ids = NULL;
	list->count = 0;
}

#endif /* LAYOUT_DS_IDS_H */

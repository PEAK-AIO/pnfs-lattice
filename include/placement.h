/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * placement.h — Stripe placement policy API.
 *
 * Selects which data servers host the stripes + mirrors for a new file.
 * Phase 2 provides round-robin; capacity-weighted and tier-aware
 * policies are planned for Phase 3.
 */

#ifndef PLACEMENT_H
#define PLACEMENT_H

#include <stdint.h>
#include "pnfs_mds.h"       /* struct mds_ds_map_entry */

/**
 * Select DS nodes for a new file's stripe layout.
 *
 * Filters the DS registry for ONLINE servers, then assigns
 * stripe_count × mirror_count entries via round-robin.  No two
 * mirrors of the same stripe will land on the same DS.
 *
 * @param ds_list       Array of registered data servers (from
 *                      mds_cat_ds_list()).
 * @param ds_count      Number of entries in @ds_list.
 * @param stripe_count  Number of stripes for the file.
 * @param mirror_count  Number of mirrors per stripe (≥ 1).
 * @param stripe_unit   Stripe unit size in bytes (written into each
 *                      entry for the caller's convenience — not used
 *                      by this function).
 * @param entries       Caller-allocated output array of size
 *                      stripe_count × mirror_count.
 * @return MDS_OK on success, MDS_ERR_NOSPC if insufficient ONLINE DSs,
 *         MDS_ERR_INVAL on bad parameters.
 */
enum mds_status placement_select(const struct mds_ds_info *ds_list,
                                 uint32_t ds_count,
                                 uint32_t stripe_count,
                                 uint32_t mirror_count,
                                 uint32_t stripe_unit,
                                 struct mds_ds_map_entry *entries);

/**
 * Select DS nodes for a new file's stripe layout under a specific
 * policy.  Thin superset of `placement_select`:
 *
 *   PLACEMENT_RR          : round-robin (delegates to placement_select).
 *   PLACEMENT_WEIGHTED_RR : reservoir pick weighted by (total - used).
 *   PLACEMENT_CAPACITY    : pick the DS with the most free bytes.
 *
 * In Phase 1 all policies operate with `stripe_count=1,
 * mirror_count=1` exactly - multi-DS striping and multi-mirror
 * placement land in Phase 3.  Passing larger values falls back to
 * the Phase 1 single-DS path via placement_select's graceful-degrade
 * clause.  The "no two mirrors of the same stripe on the same DS"
 * invariant is preserved by the underlying RR helper.
 *
 * Bumps `pnfs_mds_placement_degraded_total` when a caller asks for
 * more stripes / mirrors than the cluster can host (D5).
 */
enum mds_status placement_select_ex(enum mds_placement_policy policy,
                                    const struct mds_ds_info *ds_list,
                                    uint32_t ds_count,
                                    uint32_t stripe_count,
                                    uint32_t mirror_count,
                                    uint32_t stripe_unit,
                                    struct mds_ds_map_entry *entries);

/**
 * Select a replacement DS for a degraded mirror slot.
 *
 * If target_ds_id != 0, validates it is ONLINE and not in the
 * exclude list.  If target_ds_id == 0, picks any eligible ONLINE DS
 * not in the exclude list (first-eligible).
 *
 * @param ds_list        Registered DSes.
 * @param ds_count       Number of DSes.
 * @param exclude_ds_ids DS IDs to exclude (existing mirrors).
 * @param exclude_count  Number of exclusions.
 * @param target_ds_id   Preferred target (0 = auto-select).
 * @param out_ds_id      Receives chosen DS ID.
 * @return MDS_OK, MDS_ERR_NOSPC (no eligible DS), MDS_ERR_INVAL.
 */
enum mds_status placement_select_replacement(
    const struct mds_ds_info *ds_list, uint32_t ds_count,
    const uint32_t *exclude_ds_ids, uint32_t exclude_count,
    uint32_t target_ds_id, uint32_t *out_ds_id);

/**
 * Select a DS from a specific tier for tiering data movement.
 *
 * Filters to ONLINE DSs in @p target_tier that are not in the
 * exclude list.  Returns the first eligible DS (no weighting in V1).
 *
 * @param ds_list        Registered DSes.
 * @param ds_count       Number of DSes.
 * @param exclude_ds_ids DS IDs to exclude (existing mirrors).
 * @param exclude_count  Number of exclusions.
 * @param target_tier    Desired tier (0=hot, 1=warm, 2=cold).
 * @param out_ds_id      Receives chosen DS ID.
 * @return MDS_OK, MDS_ERR_NOSPC (no eligible DS in tier), MDS_ERR_INVAL.
 */
enum mds_status placement_select_for_tier(
    const struct mds_ds_info *ds_list, uint32_t ds_count,
    const uint32_t *exclude_ds_ids, uint32_t exclude_count,
    uint32_t target_tier, uint32_t *out_ds_id);


/**
 * Check if a DS's mode/transport are compatible with the requested profile.
 *
 * Phase 1 stub: always returns true.
 *
 * @param ds                  DS record to check.
 * @param required_mode       Required mode (DS_MODE_GENERIC, DS_MODE_GENERIC,
 *                            or 0xFF for any).
 * @param required_transport  Required transport bitmask.
 * @return true if compatible.
 */

/**
 * Build a filtered DS list containing only ONLINE DSes matching
 * the required mode and transport profile.
 *
 * Caller must free(*out) when done.
 *
 * @param in                  Input DS array (from registry list).
 * @param in_count            Input count.
 * @param required_mode       Required mode (or 0xFF for any).
 * @param required_transport  Required transport bitmask.
 * @param out                 Receives filtered array (may be NULL if 0).
 * @param out_count           Receives count of matching DSes.
 * @return MDS_OK or error.
 */
enum mds_status ds_filter_compatible(
    const struct mds_ds_info *in, uint32_t in_count,
    uint8_t required_mode, uint8_t required_transport,
    struct mds_ds_info **out, uint32_t *out_count);


/**
 * Check if a DS has the required capabilities.
 * @return true if (ds->capabilities & required_caps) == required_caps.
 */

/**
 * Like ds_filter_compatible but also requires capabilities.
 */
enum mds_status ds_filter_with_caps(
    const struct mds_ds_info *in, uint32_t in_count,
    uint8_t required_mode, uint8_t required_transport,
    uint32_t required_caps,
    struct mds_ds_info **out, uint32_t *out_count);

/**
 * Two-tier compatibility filter with a soft preference (Phase H of
 * docs/hpc-nto1-plan.md).
 *
 * Behaviour:
 *   1. Build the set of ONLINE DSes that match @p required_mode
 *      and @p required_transport (the hard floor — same semantics
 *      as ds_filter_compatible).
 *   2. Within that set, build the subset that ALSO matches the
 *      preference: at least one bit of @p preferred_transport AND
 *      all bits of @p preferred_caps.
 *   3. If the preferred subset is non-empty, return it.  Otherwise
 *      return the full required-matching set (graceful fallback).
 *
 * Passing preferred_transport == 0 and preferred_caps == 0 is
 * equivalent to ds_filter_compatible — the function collapses into
 * the no-preference path.  Caller must free(*out) when done.
 *
 * Concrete use: HPC-shared files prefer RDMA|GPUDirect DSes when the
 * fleet has any, but still get layouts on plain TCP DSes when
 * the cluster has no RDMA hardware.  A cluster mid-rollout (some
 * RDMA, some TCP-only) ends up serving HPC files only on the RDMA
 * subset, which is the desired locality property.
 *
 * @param in                  Input DS array (from registry list).
 * @param in_count            Input count.
 * @param required_mode       Required mode (or 0xFF for any).
 * @param required_transport  Required transport bitmask (hard floor).
 * @param preferred_transport Soft preference: at least one bit must
 *                            match for a DS to count as preferred.
 *                            Pass 0 to disable transport preference.
 * @param preferred_caps      Soft preference: all bits must match
 *                            for a DS to count as preferred.  Pass
 *                            0 to disable capability preference.
 * @param out                 Receives filtered array (may be NULL
 *                            if 0).
 * @param out_count           Receives count of matching DSes.
 * @return MDS_OK or error.
 */
enum mds_status ds_filter_compatible_preferred(
    const struct mds_ds_info *in, uint32_t in_count,
    uint8_t  required_mode,
    uint8_t  required_transport,
    uint8_t  preferred_transport,
    uint32_t preferred_caps,
    struct mds_ds_info **out, uint32_t *out_count);

bool ds_capability_match(const struct mds_ds_info *ds, uint32_t required_caps);

bool ds_profile_match(const struct mds_ds_info *ds,
                      uint8_t required_mode,
                      uint8_t required_transport);

#endif /* PLACEMENT_H */



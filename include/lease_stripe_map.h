/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * lease_stripe_map.h -- Pure helper to decompose a logical (offset,
 * length) range into per-stripe slices.
 *
 * Companion to lease_table.h.  No allocation, no I/O, no locks; safe
 * to call from any context.  Designed for unit-testable isolation.
 *
 *   logical range:   [logical_offset, logical_offset + logical_length)
 *   stripe size:     stripe_unit bytes
 *   stripe_index:    logical_offset / stripe_unit (and 1, 2, ... for
 *                    subsequent stripes intersected)
 *   ds_offset:       offset within the stripe at which the slice begins
 *   ds_length:       slice length within the stripe
 *
 * Sentinel: logical_length == UINT64_MAX (whole-file) is rejected -- the
 * caller should use stripe_lease_release_all_for() for whole-file paths
 * and decompose finite ranges here.
 */

#ifndef LEASE_STRIPE_MAP_H
#define LEASE_STRIPE_MAP_H

#include <stdint.h>

struct stripe_slice {
    uint32_t stripe_index;
    uint64_t ds_offset;
    uint64_t ds_length;
};

/**
 * @brief Decompose a logical byte range into one or more per-stripe slices.
 *
 * @param logical_offset Starting byte of the range.
 * @param logical_length Length in bytes (must not be 0 or UINT64_MAX).
 * @param stripe_unit    Stripe unit in bytes (must be > 0).
 * @param out            Caller-owned output buffer.
 * @param max_slices     Capacity of @p out (must be >= 1).
 *
 * @return  On success, the number of slices written (>= 1).
 *          -EINVAL if any argument is invalid (NULL out, zero stripe_unit,
 *          zero length, UINT64_MAX length, or max_slices == 0).
 *          -ENOSPC if the range straddles more stripes than @p max_slices.
 *
 * Notes:
 *  - stripe_index is monotonic increasing across the returned slices.
 *  - For each slice s: 0 <= s.ds_offset < stripe_unit
 *                      and 0 < s.ds_length <= stripe_unit.
 */
int lease_range_to_stripe_slices(uint64_t logical_offset,
                                 uint64_t logical_length,
                                 uint32_t stripe_unit,
                                 struct stripe_slice *out,
                                 uint32_t max_slices);

#endif /* LEASE_STRIPE_MAP_H */

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * lease_stripe_map.c -- Pure helper: decompose a logical byte range
 * into per-stripe slices.
 *
 * No allocation, no I/O, no locks.  Single-pass iteration; arithmetic
 * stays overflow-safe by consuming "remaining" rather than computing
 * an end offset that could wrap UINT64_MAX.
 */

#include <errno.h>
#include <stddef.h>
#include <stdint.h>

#include "lease_stripe_map.h"

int lease_range_to_stripe_slices(uint64_t logical_offset,
                                 uint64_t logical_length,
                                 uint32_t stripe_unit,
                                 struct stripe_slice *out,
                                 uint32_t max_slices)
{
    /* Argument validation -- fail fast and explicit. */
    if (out == NULL)               { return -EINVAL; }
    if (stripe_unit == 0)          { return -EINVAL; }
    if (max_slices == 0)           { return -EINVAL; }
    if (logical_length == 0)       { return -EINVAL; }
    if (logical_length == UINT64_MAX) { return -EINVAL; }

    /* First stripe index must fit in uint32_t. */
    uint64_t first_idx64 = logical_offset / (uint64_t)stripe_unit;
    if (first_idx64 > (uint64_t)UINT32_MAX) {
        return -EINVAL;
    }

    uint32_t cur_idx   = (uint32_t)first_idx64;
    uint64_t cur_off   = logical_offset % (uint64_t)stripe_unit;
    uint64_t remaining = logical_length;
    uint32_t count     = 0;

    while (remaining > 0) {
        if (count == max_slices) {
            return -ENOSPC;
        }
        /* Slice length is the lesser of (stripe_unit - cur_off) and
         * the remaining bytes in the range.  Both operands are
         * bounded so no wraparound is possible. */
        uint64_t slice_len = (uint64_t)stripe_unit - cur_off;
        if (slice_len > remaining) {
            slice_len = remaining;
        }
        out[count].stripe_index = cur_idx;
        out[count].ds_offset    = cur_off;
        out[count].ds_length    = slice_len;
        count++;

        remaining -= slice_len;
        cur_off    = 0;

        /* Detect monotonic uint32_t overflow on stripe_index. */
        if (remaining > 0) {
            if (cur_idx == UINT32_MAX) {
                return -EINVAL;
            }
            cur_idx++;
        }
    }
    return (int)count;
}

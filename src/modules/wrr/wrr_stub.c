/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * wrr_stub.c -- no-op stub for WRR + capacity placement kernels.
 *
 * Linked into pnfs_mds_core when ENABLE_WRR=OFF (community build).
 *
 * Semantics: both pickers return 0 (the first online slot).  The
 * surrounding walk-forward logic in placement_select_ex() then
 * advances to the next free slot when 0 is already taken, which
 * gives an RR-equivalent placement across stripes.  WEIGHTED_RR
 * and CAPACITY policies therefore degrade gracefully to plain RR
 * on community builds -- operators that explicitly request a
 * weighted policy still get a valid placement, just not weighted.
 */

#include "wrr.h"

uint32_t mds_wrr_weighted_pick(const uint64_t *free_bytes, uint32_t n)
{
    (void)free_bytes;
    (void)n;
    return 0;
}

uint32_t mds_wrr_capacity_pick(const uint64_t *free_bytes, uint32_t n)
{
    (void)free_bytes;
    (void)n;
    return 0;
}

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * wrr.h — Weighted-round-robin and capacity-derived placement
 * selection kernels.
 *
 * These two routines are the policy-aware decision functions
 * called from placement_select_ex() when the operator has selected
 * PLACEMENT_WEIGHTED_RR or PLACEMENT_CAPACITY.  They live in the
 * `wrr` opt-in module so the community build can compile them out
 * (ENABLE_WRR=OFF), in which case the stub variant returns 0 and
 * the surrounding "walk-forward to next free slot" logic in
 * placement_select_ex() degrades the call to plain round-robin
 * over the online DS set.
 *
 * The plain-RR path in placement_select() stays in placement.c
 * unconditionally — it is the always-available baseline.
 */

#ifndef WRR_H
#define WRR_H

#include <stdint.h>

/**
 * Weighted random selection over @a n online DSes by free-byte
 * weight.  Larger free_bytes → higher selection probability.
 *
 * @param free_bytes  Per-online-DS free-byte (or weight) values.
 * @param n           Number of online DSes (length of free_bytes).
 * @return Index in [0, n) of the chosen DS.  When n == 0 returns 0.
 *
 * Stub semantics (community build): always returns 0 — the
 * surrounding logic walks forward to the next free slot, which
 * gives an RR-equivalent placement.
 */
uint32_t mds_wrr_weighted_pick(const uint64_t *free_bytes, uint32_t n);

/**
 * Largest-free-bytes (top-1 by free_bytes) deterministic selection
 * over @a n online DSes.
 *
 * @param free_bytes  Per-online-DS free-byte values.
 * @param n           Number of online DSes.
 * @return Index in [0, n) of the DS with the largest free_bytes.
 *         Returns 0 on empty input.
 *
 * Stub semantics (community build): always returns 0.
 */
uint32_t mds_wrr_capacity_pick(const uint64_t *free_bytes, uint32_t n);

#endif /* WRR_H */

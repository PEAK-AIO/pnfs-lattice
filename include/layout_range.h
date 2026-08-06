/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * layout_range.h -- Layout byte-range helpers.
 *
 * Deliberately self-contained (stdint only) so it can be included
 * from both the C compound/layout code and the C++ RonDB shim
 * (layout_types.h itself is C-only: it carries a flexible array
 * member that C++ rejects under -Wpedantic).
 */

#ifndef LAYOUT_RANGE_H
#define LAYOUT_RANGE_H

#include <stdint.h>
#include <stddef.h>

/**
 * Saturating union of two layout byte ranges.
 *
 * A length of UINT64_MAX is the RFC 8881 "to EOF" sentinel: the union
 * with anything keeps the sentinel.  A zero-length range is empty and
 * contributes nothing.  End offsets saturate at UINT64_MAX, in which
 * case the resulting length degrades to the EOF sentinel.
 *
 * Used by the layout-state renewal path: the persisted row must stay a
 * SUPERSET of every range granted under the stateid, because the
 * byte-range recall scanner skips holders whose row is disjoint from a
 * recalled range.  Over-recall is safe; under-recall silently drops
 * conflict recalls.
 *
 * @param off_a/len_a  First range.
 * @param off_b/len_b  Second range.
 * @param off_out      Receives the union's start offset.
 * @param len_out      Receives the union's length (may be UINT64_MAX).
 */
static inline void layout_range_union_saturating(
    uint64_t off_a, uint64_t len_a,
    uint64_t off_b, uint64_t len_b,
    uint64_t *off_out, uint64_t *len_out)
{
    uint64_t off;
    uint64_t end_a;
    uint64_t end_b;
    uint64_t end;

    if (off_out == NULL || len_out == NULL) {
        return;
    }
    if (len_a == 0) {
        *off_out = off_b;
        *len_out = len_b;
        return;
    }
    if (len_b == 0) {
        *off_out = off_a;
        *len_out = len_a;
        return;
    }

    off = (off_a < off_b) ? off_a : off_b;

    if (len_a == UINT64_MAX || len_b == UINT64_MAX) {
        *off_out = off;
        *len_out = UINT64_MAX; /* to-EOF sentinel dominates */
        return;
    }

    end_a = (len_a > UINT64_MAX - off_a) ? UINT64_MAX : off_a + len_a;
    end_b = (len_b > UINT64_MAX - off_b) ? UINT64_MAX : off_b + len_b;
    end = (end_a > end_b) ? end_a : end_b;

    *off_out = off;
    if (end == UINT64_MAX) {
        *len_out = UINT64_MAX;
    } else {
        *len_out = end - off;
    }
}

#endif /* LAYOUT_RANGE_H */

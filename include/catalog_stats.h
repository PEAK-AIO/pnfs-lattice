/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalog_stats.h — Atomic counters for authority/image observability.
 *
 * All counters use relaxed atomics for low-overhead instrumentation.
 * The stats struct is embedded in struct mds_catalogue and exposed
 * via mds_catalogue_stats() for metrics export.
 */

#ifndef CATALOG_STATS_H
#define CATALOG_STATS_H

#include <stdatomic.h>
#include <stdint.h>

struct catalog_stats {
    /* Authority backend */
    _Atomic(uint64_t) authority_writes;

    /* Replay journal */
    _Atomic(uint64_t) replay_journal_writes;
    _Atomic(uint64_t) replay_high_water;

    /* Materialized image */
    _Atomic(uint64_t) image_applied_high_water;
    _Atomic(uint64_t) image_hits;
    _Atomic(uint64_t) image_misses;
    _Atomic(uint64_t) image_fallbacks;

    /* Compare-read validation */
    _Atomic(uint64_t) compare_mismatches;
    _Atomic(uint64_t) compare_skipped_lag;

    /* Rebuild */
    _Atomic(uint64_t) replay_rebuild_completions;
};

/** Increment a counter by 1 (relaxed ordering). */
static inline void catalog_stat_inc(volatile _Atomic(uint64_t) *counter)
{
    atomic_fetch_add_explicit(counter, 1, memory_order_relaxed);
}

/** Set a counter to an absolute value (relaxed ordering). */
static inline void catalog_stat_set(volatile _Atomic(uint64_t) *counter,
                                     uint64_t value)
{
    atomic_store_explicit(counter, value, memory_order_relaxed);
}

/** Read a counter (relaxed ordering). */
static inline uint64_t catalog_stat_get(
    const volatile _Atomic(uint64_t) *counter)
{
    return atomic_load_explicit(counter, memory_order_relaxed);
}

/** Zero all counters. */
static inline void catalog_stats_reset(struct catalog_stats *stats)
{
    if (stats == NULL) {
        return;
    }
    atomic_store_explicit(&stats->authority_writes, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&stats->replay_journal_writes, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&stats->replay_high_water, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&stats->image_applied_high_water, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&stats->image_hits, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&stats->image_misses, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&stats->image_fallbacks, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&stats->compare_mismatches, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&stats->compare_skipped_lag, 0,
                          memory_order_relaxed);
    atomic_store_explicit(&stats->replay_rebuild_completions, 0,
                          memory_order_relaxed);
}

#endif /* CATALOG_STATS_H */

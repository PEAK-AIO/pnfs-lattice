/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_histogram.h -- Fixed 12-bucket latency histogram.
 *
 * Cheap, lock-free observation primitive aimed at NFS-server
 * latency tracking.  Each observation is a single relaxed-order
 * atomic increment in one of 12 fixed buckets plus relaxed adds
 * to _sum_ns and _count.  Bucket bounds are pre-tuned for the
 * 100 us .. 250 ms range that covers everything from "cache hit"
 * to "we are on fire".
 *
 * Bucket upper bounds:
 *   100 us, 250 us, 500 us, 1 ms, 2.5 ms, 5 ms, 10 ms, 25 ms,
 *   50 ms, 100 ms, 250 ms, +Inf
 *
 * The renderer emits Prometheus histogram exposition format
 * (cumulative buckets + _sum + _count) so dashboards can compute
 * p50/p95/p99 with histogram_quantile().
 */

#ifndef MDS_HISTOGRAM_H
#define MDS_HISTOGRAM_H

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

/** Number of buckets (11 finite + 1 +Inf). */
#define MDS_HIST_BUCKETS 12

/**
 * Live histogram.  All fields are atomic so the hot path is
 * lock-free.  Zero-initialised state is valid (use calloc or
 * static storage).
 */
struct mds_histogram {
	_Atomic uint64_t buckets[MDS_HIST_BUCKETS];
	_Atomic uint64_t sum_ns;
	_Atomic uint64_t count;
};

/** Bucket upper bounds in nanoseconds (last entry is +Inf == UINT64_MAX). */
extern const uint64_t mds_hist_bucket_ns[MDS_HIST_BUCKETS];

/** Bucket upper bounds rendered as Prometheus `le="..."` labels (seconds). */
extern const char *const mds_hist_bucket_label[MDS_HIST_BUCKETS];

/**
 * Record a single observation.
 *
 * @param h   Histogram to update.  May be NULL (no-op).
 * @param ns  Sample value in nanoseconds.
 */
void mds_histogram_observe(struct mds_histogram *h, uint64_t ns);

/** Reset all buckets and sum/count to zero. */
void mds_histogram_reset(struct mds_histogram *h);

/**
 * Render the histogram as Prometheus histogram exposition text.
 *
 * Emits:
 *   # TYPE <metric_name> histogram
 *   <metric_name>_bucket{le="0.0001"} N
 *   ...
 *   <metric_name>_bucket{le="+Inf"} N
 *   <metric_name>_sum   <seconds>
 *   <metric_name>_count <count>
 *
 * The caller is responsible for emitting `# HELP` before calling.
 *
 * @return Bytes written (excluding NUL), or -1 on truncation / bad args.
 */
int mds_histogram_render(const struct mds_histogram *h,
			 const char *metric_name,
			 char *buf, size_t cap);

#endif /* MDS_HISTOGRAM_H */

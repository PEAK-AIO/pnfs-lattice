/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_histogram.c -- 12-bucket latency histogram.
 *
 * Hot path is three relaxed atomic adds (bucket, sum, count).
 * The bucket lookup is a linear scan over 11 finite bounds; this
 * compiles to a short branch chain that branch-predicts well in
 * the steady state (most samples fall in 2-3 adjacent buckets).
 */

#include "mds_histogram.h"

#include <stdio.h>
#include <string.h>

/* Bucket upper bounds, nanoseconds.  Last entry must be UINT64_MAX
 * so the +Inf bucket catches all overflow. */
const uint64_t mds_hist_bucket_ns[MDS_HIST_BUCKETS] = {
	    100ULL * 1000ULL,        /* 100 us */
	    250ULL * 1000ULL,        /* 250 us */
	    500ULL * 1000ULL,        /* 500 us */
	   1000ULL * 1000ULL,        /*   1 ms */
	   2500ULL * 1000ULL,        /* 2.5 ms */
	   5000ULL * 1000ULL,        /*   5 ms */
	  10000ULL * 1000ULL,        /*  10 ms */
	  25000ULL * 1000ULL,        /*  25 ms */
	  50000ULL * 1000ULL,        /*  50 ms */
	 100000ULL * 1000ULL,        /* 100 ms */
	 250000ULL * 1000ULL,        /* 250 ms */
	UINT64_MAX,                  /* +Inf  */
};

/* Same bounds rendered as seconds for Prometheus `le="..."` labels. */
const char *const mds_hist_bucket_label[MDS_HIST_BUCKETS] = {
	"0.0001", "0.00025", "0.0005",
	"0.001",  "0.0025",  "0.005",
	"0.01",   "0.025",   "0.05",
	"0.1",    "0.25",    "+Inf",
};

void mds_histogram_observe(struct mds_histogram *h, uint64_t ns)
{
	unsigned idx;

	if (h == NULL) {
		return;
	}

	for (idx = 0; idx < MDS_HIST_BUCKETS - 1; idx++) {
		if (ns <= mds_hist_bucket_ns[idx]) {
			break;
		}
	}

	atomic_fetch_add_explicit(&h->buckets[idx], 1U,
				  memory_order_relaxed);
	atomic_fetch_add_explicit(&h->sum_ns, ns,
				  memory_order_relaxed);
	atomic_fetch_add_explicit(&h->count, 1U,
				  memory_order_relaxed);
}

void mds_histogram_reset(struct mds_histogram *h)
{
	unsigned i;

	if (h == NULL) {
		return;
	}
	for (i = 0; i < MDS_HIST_BUCKETS; i++) {
		atomic_store_explicit(&h->buckets[i], 0U,
				      memory_order_relaxed);
	}
	atomic_store_explicit(&h->sum_ns, 0U, memory_order_relaxed);
	atomic_store_explicit(&h->count,  0U, memory_order_relaxed);
}

int mds_histogram_render(const struct mds_histogram *h,
			 const char *metric_name,
			 char *buf, size_t cap)
{
	size_t   off = 0;
	uint64_t cumulative = 0;
	uint64_t sum_ns;
	uint64_t count;
	int      n;
	unsigned i;

	if (h == NULL || metric_name == NULL || buf == NULL || cap == 0) {
		return -1;
	}

	n = snprintf(buf + off, cap - off,
		"# TYPE %s histogram\n", metric_name);
	if (n < 0 || (size_t)n >= cap - off) {
		return -1;
	}
	off += (size_t)n;

	for (i = 0; i < MDS_HIST_BUCKETS; i++) {
		uint64_t v = atomic_load_explicit(
			(_Atomic uint64_t *)&h->buckets[i],
			memory_order_relaxed);
		cumulative += v;
		n = snprintf(buf + off, cap - off,
			"%s_bucket{le=\"%s\"} %lu\n",
			metric_name,
			mds_hist_bucket_label[i],
			(unsigned long)cumulative);
		if (n < 0 || (size_t)n >= cap - off) {
			return -1;
		}
		off += (size_t)n;
	}

	sum_ns = atomic_load_explicit(
		(_Atomic uint64_t *)&h->sum_ns, memory_order_relaxed);
	count  = atomic_load_explicit(
		(_Atomic uint64_t *)&h->count,  memory_order_relaxed);

	n = snprintf(buf + off, cap - off,
		"%s_sum %.9f\n"
		"%s_count %lu\n",
		metric_name, (double)sum_ns / 1e9,
		metric_name, (unsigned long)count);
	if (n < 0 || (size_t)n >= cap - off) {
		return -1;
	}
	off += (size_t)n;

	return (int)off;
}

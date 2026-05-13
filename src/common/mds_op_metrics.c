/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_op_metrics.c -- Per-op latency + phase split histograms.
 *
 * Three families: per NFS op total, per catalogue op, and per
 * NFS op * phase.  The phase tracker is a tiny per-thread stack
 * driven by mds_phase_enter/leave at backend entry points.
 *
 * Hot-path cost per observation:
 *   - one CLOCK_MONOTONIC read
 *   - three relaxed atomic adds (bucket, sum, count) into one
 *     fixed histogram chosen by enum index
 *
 * The phase tracker adds two CLOCK_MONOTONIC reads per
 * enter/leave pair (~50 ns on modern CPUs) and a few stack
 * accesses; nothing touches global state on the hot path.
 */

#include "mds_op_metrics.h"

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "compound.h"
#include "mds_histogram.h"

/*
 * Master kill-switch.  Default ON.  Toggled at startup via
 * cfg.metrics_op_enabled and at runtime via the setter below.
 * Stored as relaxed atomic; observations make a single
 * acquire-free load, which inlines into a 1-2 ns no-op when the
 * flag is false.
 */
_Atomic bool g_mds_op_metrics_enabled = true;

void mds_op_metrics_set_enabled(bool enabled)
{
	atomic_store_explicit(&g_mds_op_metrics_enabled, enabled,
			      memory_order_relaxed);
}

/* -----------------------------------------------------------------------
 * Static names + storage
 * ----------------------------------------------------------------------- */

static const char *const op_class_name[MDS_OPC__COUNT] = {
	[MDS_OPC_LOOKUP]        = "lookup",
	[MDS_OPC_GETATTR]       = "getattr",
	[MDS_OPC_SETATTR]       = "setattr",
	[MDS_OPC_ACCESS]        = "access",
	[MDS_OPC_CREATE]        = "create",
	[MDS_OPC_REMOVE]        = "remove",
	[MDS_OPC_RENAME]        = "rename",
	[MDS_OPC_READDIR]       = "readdir",
	[MDS_OPC_OPEN]          = "open",
	[MDS_OPC_CLOSE]         = "close",
	[MDS_OPC_READ]          = "read",
	[MDS_OPC_WRITE]         = "write",
	[MDS_OPC_COMMIT]        = "commit",
	[MDS_OPC_LAYOUTGET]     = "layoutget",
	[MDS_OPC_LAYOUTCOMMIT]  = "layoutcommit",
	[MDS_OPC_LAYOUTRETURN]  = "layoutreturn",
	[MDS_OPC_LOCK]          = "lock",
	[MDS_OPC_LOCKU]         = "locku",
	[MDS_OPC_SEQUENCE]      = "sequence",
	[MDS_OPC_OTHER]         = "other",
};

static const char *const cat_op_name[MDS_CATOP__COUNT] = {
	[MDS_CATOP_NS_CREATE]        = "ns_create",
	[MDS_CATOP_NS_REMOVE]        = "ns_remove",
	[MDS_CATOP_NS_RENAME]        = "ns_rename",
	[MDS_CATOP_NS_LINK]          = "ns_link",
	[MDS_CATOP_NS_LOOKUP]        = "ns_lookup",
	[MDS_CATOP_NS_GETATTR]       = "ns_getattr",
	[MDS_CATOP_NS_SETATTR]       = "ns_setattr",
	[MDS_CATOP_NS_READDIR]       = "ns_readdir",
	[MDS_CATOP_NS_READDIR_PLUS]  = "ns_readdir_plus",
	[MDS_CATOP_NS_NLINK_ADJUST]  = "ns_nlink_adjust",
	[MDS_CATOP_ALLOC_FILEID]     = "alloc_fileid",
	[MDS_CATOP_INODE_PUT]        = "inode_put",
	[MDS_CATOP_INODE_DEL]        = "inode_del",
	[MDS_CATOP_DIRENT_PUT]       = "dirent_put",
	[MDS_CATOP_DIRENT_DEL]       = "dirent_del",
	[MDS_CATOP_INLINE_GET]       = "inline_get",
	[MDS_CATOP_INLINE_PUT]       = "inline_put",
	[MDS_CATOP_LAYOUTCOMMIT]     = "layoutcommit",
	[MDS_CATOP_STRIPE_MAP_GET]   = "stripe_map_get",
	[MDS_CATOP_STRIPE_MAP_PUT]   = "stripe_map_put",
	[MDS_CATOP_STRIPE_MAP_DEL]   = "stripe_map_del",
	[MDS_CATOP_LAYOUT_GRANT]     = "layout_grant",
	[MDS_CATOP_LAYOUT_RETURN]    = "layout_return",
	[MDS_CATOP_LAYOUT_LOOKUP]    = "layout_lookup",
	[MDS_CATOP_LAYOUTGET_FUSED]  = "layoutget_fused",
	[MDS_CATOP_INODE_GET]            = "inode_get",
	[MDS_CATOP_DS_PREPARE_CHECK]     = "ds_prepare_check",
	[MDS_CATOP_LAYOUT_RECALL_SCAN]   = "layout_recall_scan",
	[MDS_CATOP_LAYOUT_REVOKE_GRANT]  = "layout_revoke_grant",
	[MDS_CATOP_OTHER]            = "other",
};

static const char *const phase_name[MDS_PHASE__COUNT] = {
	[MDS_PHASE_PROTOCOL]  = "protocol",
	[MDS_PHASE_STATE]     = "state",
	[MDS_PHASE_CATALOGUE] = "catalogue",
	[MDS_PHASE_DS_IO]     = "ds_io",
};

const char *mds_op_class_name(enum mds_op_class c)
{
	return (c < MDS_OPC__COUNT) ? op_class_name[c] : "?";
}

const char *mds_cat_op_name(enum mds_cat_op c)
{
	return (c < MDS_CATOP__COUNT) ? cat_op_name[c] : "?";
}

const char *mds_phase_name(enum mds_phase p)
{
	return (p < MDS_PHASE__COUNT) ? phase_name[p] : "?";
}

/* -----------------------------------------------------------------------
 * Global histogram storage
 * ----------------------------------------------------------------------- */

static struct mds_histogram g_op_total_hist[MDS_OPC__COUNT];
static struct mds_histogram g_cat_op_hist  [MDS_CATOP__COUNT];
static struct mds_histogram g_op_phase_hist[MDS_OPC__COUNT][MDS_PHASE__COUNT];

/* -----------------------------------------------------------------------
 * Op-class mapping
 * ----------------------------------------------------------------------- */

enum mds_op_class mds_op_class_from_opnum(uint32_t opnum)
{
	switch (opnum) {
	case OP_LOOKUP:          return MDS_OPC_LOOKUP;
	case OP_GETATTR:         return MDS_OPC_GETATTR;
	case OP_SETATTR:         return MDS_OPC_SETATTR;
	case OP_ACCESS:          return MDS_OPC_ACCESS;
	case OP_CREATE:          return MDS_OPC_CREATE;
	case OP_REMOVE:          return MDS_OPC_REMOVE;
	case OP_RENAME:          return MDS_OPC_RENAME;
	case OP_READDIR:         return MDS_OPC_READDIR;
	case OP_OPEN:            return MDS_OPC_OPEN;
	case OP_CLOSE:           return MDS_OPC_CLOSE;
	case OP_READ:
	case OP_READ_PLUS:       return MDS_OPC_READ;
	case OP_WRITE:           return MDS_OPC_WRITE;
	case OP_COMMIT:          return MDS_OPC_COMMIT;
	case OP_LAYOUTGET:       return MDS_OPC_LAYOUTGET;
	case OP_LAYOUTCOMMIT:    return MDS_OPC_LAYOUTCOMMIT;
	case OP_LAYOUTRETURN:    return MDS_OPC_LAYOUTRETURN;
	case OP_LOCK:            return MDS_OPC_LOCK;
	case OP_LOCKU:           return MDS_OPC_LOCKU;
	case OP_SEQUENCE:        return MDS_OPC_SEQUENCE;
	default:                 return MDS_OPC_OTHER;
	}
}

void mds_op_observe_total(enum mds_op_class c, uint64_t ns)
{
	if (!mds_op_metrics_enabled() || c >= MDS_OPC__COUNT) {
		return;
	}
	mds_histogram_observe(&g_op_total_hist[c], ns);
}

void mds_cat_op_observe(enum mds_cat_op c, uint64_t ns)
{
	if (!mds_op_metrics_enabled() || c >= MDS_CATOP__COUNT) {
		return;
	}
	mds_histogram_observe(&g_cat_op_hist[c], ns);
}

/* -----------------------------------------------------------------------
 * Thread-local phase tracker
 * ----------------------------------------------------------------------- */

#define MDS_PHASE_STACK_MAX 8

struct mds_phase_state {
	int       active;                       /**< begin_op was called. */
	int       sp;                           /**< Stack pointer. */
	enum mds_phase stack[MDS_PHASE_STACK_MAX];
	uint64_t  mark_ns;                      /**< Last clock sample. */
	uint64_t  ns_per_phase[MDS_PHASE__COUNT];
};

static __thread struct mds_phase_state g_phase;

static inline uint64_t monotonic_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
		return 0;
	}
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

uint64_t mds_op_metrics_now_ns(void)
{
	return monotonic_ns();
}

void mds_phase_begin_op(void)
{
	if (!mds_op_metrics_enabled()) {
		g_phase.active = 0;
		return;
	}
	g_phase.active = 1;
	g_phase.sp     = 0;
	g_phase.stack[0] = MDS_PHASE_PROTOCOL;
	g_phase.mark_ns = monotonic_ns();
	memset(g_phase.ns_per_phase, 0, sizeof(g_phase.ns_per_phase));
}

void mds_phase_enter(enum mds_phase p)
{
	uint64_t now;
	enum mds_phase top;

	if (!g_phase.active || p >= MDS_PHASE__COUNT) {
		return;
	}
	if (g_phase.sp + 1 >= MDS_PHASE_STACK_MAX) {
		/* Stack overflow -- silently ignore so we never crash the
		 * dispatcher.  The caller's matching leave will be ignored
		 * too because sp won't increase. */
		return;
	}

	now = monotonic_ns();
	top = g_phase.stack[g_phase.sp];
	g_phase.ns_per_phase[top] += (now - g_phase.mark_ns);
	g_phase.mark_ns = now;
	g_phase.sp++;
	g_phase.stack[g_phase.sp] = p;
}

void mds_phase_leave(void)
{
	uint64_t now;
	enum mds_phase top;

	if (!g_phase.active || g_phase.sp == 0) {
		return;
	}

	now = monotonic_ns();
	top = g_phase.stack[g_phase.sp];
	g_phase.ns_per_phase[top] += (now - g_phase.mark_ns);
	g_phase.mark_ns = now;
	g_phase.sp--;
}

void mds_phase_end_op(enum mds_op_class c)
{
	uint64_t now;
	enum mds_phase top;
	unsigned p;

	if (!g_phase.active) {
		return;
	}

	now = monotonic_ns();
	top = g_phase.stack[g_phase.sp];
	g_phase.ns_per_phase[top] += (now - g_phase.mark_ns);
	g_phase.active = 0;

	if (c >= MDS_OPC__COUNT) {
		return;
	}
	for (p = 0; p < MDS_PHASE__COUNT; p++) {
		uint64_t v = g_phase.ns_per_phase[p];
		if (v == 0) {
			continue;
		}
		mds_histogram_observe(&g_op_phase_hist[c][p], v);
	}
}

/* -----------------------------------------------------------------------
 * Rendering
 *
 * Family 1 + 3 share a per-op-class loop.  Family 2 has its own loop.
 * We emit each histogram as a distinct Prometheus metric so that the
 * `op="..."` and `phase="..."` labels appear on the bucket lines as
 * extra dimensions (Prometheus aggregates over them naturally).
 *
 * To keep the renderer simple we render each family as N parallel
 * histograms named after the metric; each histogram already emits
 * `_bucket{le="..."}` lines, and we just inject the op/phase label
 * by composing the per-call metric name with the label embedded.
 * That keeps the existing mds_histogram_render contract clean.
 *
 * The label injection is done by rendering each (op, phase) histogram
 * with a synthetic metric name that includes the labels.  Prometheus
 * accepts this form because the parser is keyed on the metric name
 * before the first `{`.
 *
 * Implementation: we use a small "labeled" render helper that emits
 *      "<metric>_bucket{op=\"..\"[,phase=\"..\"],le=\"..\"} N\n"
 *      "<metric>_sum{op=\"..\"[,phase=\"..\"]} N\n"
 *      "<metric>_count{op=\"..\"[,phase=\"..\"]} N\n"
 * directly instead of routing through mds_histogram_render(), which
 * does not know about labels.
 * ----------------------------------------------------------------------- */

static int render_labeled_hist(const struct mds_histogram *h,
			       const char *metric_name,
			       const char *label,
			       char *buf, size_t cap)
{
	size_t   off = 0;
	uint64_t cumulative = 0;
	uint64_t sum_ns;
	uint64_t count;
	int      n;
	unsigned i;

	for (i = 0; i < MDS_HIST_BUCKETS; i++) {
		uint64_t v = atomic_load_explicit(
			(_Atomic uint64_t *)&h->buckets[i],
			memory_order_relaxed);
		cumulative += v;
		n = snprintf(buf + off, cap - off,
			"%s_bucket{%s,le=\"%s\"} %lu\n",
			metric_name, label,
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
		"%s_sum{%s} %.9f\n"
		"%s_count{%s} %lu\n",
		metric_name, label, (double)sum_ns / 1e9,
		metric_name, label, (unsigned long)count);
	if (n < 0 || (size_t)n >= cap - off) {
		return -1;
	}
	off += (size_t)n;
	return (int)off;
}

int mds_op_metrics_render(char *buf, size_t cap)
{
	size_t off = 0;
	int    n;
	unsigned i;
	unsigned p;

	if (buf == NULL || cap == 0) {
		return -1;
	}

	/* When the kill-switch is engaged we emit nothing -- this
	 * keeps /metrics compact (no all-zero noise) and lets
	 * scrapers detect the disabled state via the absence of
	 * pnfs_mds_op_latency_seconds. */
	if (!mds_op_metrics_enabled()) {
		return 0;
	}

	/* Family 1: per-op total latency. */
	n = snprintf(buf + off, cap - off,
		"# HELP pnfs_mds_op_latency_seconds "
			"End-to-end latency of one NFS op handled by "
			"COMPOUND dispatch (excludes RPC decode/encode).\n"
		"# TYPE pnfs_mds_op_latency_seconds histogram\n");
	if (n < 0 || (size_t)n >= cap - off) return -1;
	off += (size_t)n;

	for (i = 0; i < MDS_OPC__COUNT; i++) {
		char label[64];
		int rc;

		(void)snprintf(label, sizeof(label),
			"op=\"%s\"", mds_op_class_name((enum mds_op_class)i));
		rc = render_labeled_hist(&g_op_total_hist[i],
			"pnfs_mds_op_latency_seconds",
			label, buf + off, cap - off);
		if (rc < 0) return -1;
		off += (size_t)rc;
	}

	/* Family 2: per-catalogue-op latency. */
	n = snprintf(buf + off, cap - off,
		"# HELP pnfs_mds_cat_op_latency_seconds "
			"Latency of a single catalogue vtable call "
			"(== one RonDB roundtrip in the RonDB backend).\n"
		"# TYPE pnfs_mds_cat_op_latency_seconds histogram\n");
	if (n < 0 || (size_t)n >= cap - off) return -1;
	off += (size_t)n;

	for (i = 0; i < MDS_CATOP__COUNT; i++) {
		char label[64];
		int rc;

		/* Use cat_op="..." (not op="...") so dashboards never
		 * confuse the NFS-level op LOOKUP with the catalogue
		 * call ns_lookup. */
		(void)snprintf(label, sizeof(label),
			"cat_op=\"%s\"",
			mds_cat_op_name((enum mds_cat_op)i));
		rc = render_labeled_hist(&g_cat_op_hist[i],
			"pnfs_mds_cat_op_latency_seconds",
			label, buf + off, cap - off);
		if (rc < 0) return -1;
		off += (size_t)rc;
	}

	/* Family 3: per-op phase split.  Skip phases that never fired
	 * (count == 0) to keep the output compact. */
	n = snprintf(buf + off, cap - off,
		"# HELP pnfs_mds_op_phase_seconds "
			"Time each NFS op spent in a given phase. "
			"phase=protocol (default, includes RPC + state "
			"hash lookups), catalogue (RonDB), state, ds_io.\n"
		"# TYPE pnfs_mds_op_phase_seconds histogram\n");
	if (n < 0 || (size_t)n >= cap - off) return -1;
	off += (size_t)n;

	for (i = 0; i < MDS_OPC__COUNT; i++) {
		for (p = 0; p < MDS_PHASE__COUNT; p++) {
			char label[96];
			int rc;
			uint64_t count = atomic_load_explicit(
				(_Atomic uint64_t *)
				&g_op_phase_hist[i][p].count,
				memory_order_relaxed);
			if (count == 0) {
				continue;
			}
			(void)snprintf(label, sizeof(label),
				"op=\"%s\",phase=\"%s\"",
				mds_op_class_name((enum mds_op_class)i),
				mds_phase_name((enum mds_phase)p));
			rc = render_labeled_hist(&g_op_phase_hist[i][p],
				"pnfs_mds_op_phase_seconds",
				label, buf + off, cap - off);
			if (rc < 0) return -1;
			off += (size_t)rc;
		}
	}

	return (int)off;
}

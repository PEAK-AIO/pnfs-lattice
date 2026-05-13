/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_op_metrics.h -- Per-operation latency observability.
 *
 * Three coordinated histogram families:
 *
 *   1) Per NFS op total latency
 *      pnfs_mds_op_latency_seconds{op="open"}     histogram
 *      ... computed by the COMPOUND dispatcher around dispatch_op().
 *
 *   2) Per catalogue op latency
 *      pnfs_mds_cat_op_latency_seconds{op="ns_lookup"} histogram
 *      ... computed by catalogue_dispatch wrappers around the vtable
 *      call.  Tells you which RonDB call(s) dominate.
 *
 *   3) Per NFS op phase split
 *      pnfs_mds_op_phase_seconds{op="open",phase="catalogue"} histogram
 *      ... computed via a thread-local phase tracker.  Catalogue
 *      dispatch enters CATALOGUE on entry; everything else stays in
 *      PROTOCOL.  Hot state-mgmt and DS-I/O sites can also call
 *      mds_phase_enter() / mds_phase_leave() to claim their phase.
 *
 * Together these let an operator answer:
 *   - Which NFS op is slow?                (family 1)
 *   - Is the slow op slow because of RonDB?(family 3 -- catalogue%)
 *   - Which RonDB call is the culprit?     (family 2)
 *
 * The thread-local tracker is zero-cost when no phase is active
 * (NFS_PHASE_INACTIVE).  COMPOUND dispatch arms it via
 * mds_phase_begin_op() and disarms it via mds_phase_end_op().
 */

#ifndef MDS_OP_METRICS_H
#define MDS_OP_METRICS_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* -----------------------------------------------------------------------
 * Build-time kill-switch
 *
 * Define MDS_OP_METRICS_DISABLE in CFLAGS (or via the CMake option
 * ENABLE_OP_METRICS=OFF) to compile every observation site out of
 * the binary entirely.  When disabled at build time:
 *   - All public observe / phase / render functions become empty.
 *   - mds_op_metrics_enabled() is a constant `false`.
 *   - The CAT_TIMED and MDS_PHASE_SCOPE macros expand to direct
 *     execution of the wrapped expression with no extra work.
 *
 * The default (no define) keeps everything compiled in; runtime
 * gating via mds_op_metrics_set_enabled() still works.
 * ----------------------------------------------------------------------- */
#ifdef MDS_OP_METRICS_DISABLE
#  define MDS_OP_METRICS_BUILD_ENABLED 0
#else
#  define MDS_OP_METRICS_BUILD_ENABLED 1
#endif

/* -----------------------------------------------------------------------
 * Runtime kill-switch
 *
 * Hot-path test: a single relaxed atomic load + branch.  Inlined so
 * the compiler can hoist it across adjacent observations.
 * ----------------------------------------------------------------------- */
extern _Atomic bool g_mds_op_metrics_enabled;

static inline bool mds_op_metrics_enabled(void)
{
#if MDS_OP_METRICS_BUILD_ENABLED
	return atomic_load_explicit(&g_mds_op_metrics_enabled,
				    memory_order_relaxed);
#else
	return false;
#endif
}

/** Enable or disable all op-metrics observability at runtime. */
void mds_op_metrics_set_enabled(bool enabled);

/* -----------------------------------------------------------------------
 * NFS op classes
 *
 * Coarser than RFC 8881 opnums -- only the ops worth a histogram.
 * Anything else maps to MDS_OPC_OTHER (cheap counter, no histogram).
 * ----------------------------------------------------------------------- */
enum mds_op_class {
	MDS_OPC_LOOKUP = 0,
	MDS_OPC_GETATTR,
	MDS_OPC_SETATTR,
	MDS_OPC_ACCESS,
	MDS_OPC_CREATE,
	MDS_OPC_REMOVE,
	MDS_OPC_RENAME,
	MDS_OPC_READDIR,
	MDS_OPC_OPEN,
	MDS_OPC_CLOSE,
	MDS_OPC_READ,
	MDS_OPC_WRITE,
	MDS_OPC_COMMIT,
	MDS_OPC_LAYOUTGET,
	MDS_OPC_LAYOUTCOMMIT,
	MDS_OPC_LAYOUTRETURN,
	MDS_OPC_LOCK,
	MDS_OPC_LOCKU,
	MDS_OPC_SEQUENCE,
	MDS_OPC_OTHER,
	MDS_OPC__COUNT,
};

const char *mds_op_class_name(enum mds_op_class c);

/** Map an NFSv4 opnum to the histogram class. */
enum mds_op_class mds_op_class_from_opnum(uint32_t opnum);

/** Record the total wall-clock latency of one NFS op. */
void mds_op_observe_total(enum mds_op_class c, uint64_t ns);

/* -----------------------------------------------------------------------
 * Catalogue op classes -- mirror the catalogue_dispatch vtable.
 * ----------------------------------------------------------------------- */
enum mds_cat_op {
	MDS_CATOP_NS_CREATE = 0,
	MDS_CATOP_NS_REMOVE,
	MDS_CATOP_NS_RENAME,
	MDS_CATOP_NS_LINK,
	MDS_CATOP_NS_LOOKUP,
	MDS_CATOP_NS_GETATTR,
	MDS_CATOP_NS_SETATTR,
	MDS_CATOP_NS_READDIR,
	MDS_CATOP_NS_READDIR_PLUS,
	MDS_CATOP_NS_NLINK_ADJUST,
	MDS_CATOP_ALLOC_FILEID,
	MDS_CATOP_INODE_PUT,
	MDS_CATOP_INODE_DEL,
	MDS_CATOP_DIRENT_PUT,
	MDS_CATOP_DIRENT_DEL,
	MDS_CATOP_INLINE_GET,
	MDS_CATOP_INLINE_PUT,
	MDS_CATOP_LAYOUTCOMMIT,
	/* Layout / stripe-map plane (the LAYOUTGET hot path lives
	 * here -- separate buckets let us tell stripe-map read,
	 * layout grant write, and the fused RonDB combo apart). */
	MDS_CATOP_STRIPE_MAP_GET,
	MDS_CATOP_STRIPE_MAP_PUT,
	MDS_CATOP_STRIPE_MAP_DEL,
	MDS_CATOP_LAYOUT_GRANT,
	MDS_CATOP_LAYOUT_RETURN,
	MDS_CATOP_LAYOUT_LOOKUP,
	MDS_CATOP_LAYOUTGET_FUSED,
	/* LAYOUTGET hot-path probes: not strict catalogue calls,
	 * but they are the candidate sources of the unaccounted
	 * "protocol" time inside op_layoutget.  Each one wraps a
	 * single named code region so the per-op breakdown is
	 * visible in cat_op_latency without changing the phase
	 * tracker semantics. */
	MDS_CATOP_INODE_GET,            /* compound_inode_get          */
	MDS_CATOP_DS_PREPARE_CHECK,     /* compound_ds_prepare_check   */
	MDS_CATOP_LAYOUT_RECALL_SCAN,   /* byte-range recall scan      */
	MDS_CATOP_LAYOUT_REVOKE_GRANT,  /* layout_revoke_unready_grant */
	MDS_CATOP_OTHER,
	MDS_CATOP__COUNT,
};

const char *mds_cat_op_name(enum mds_cat_op c);
void mds_cat_op_observe(enum mds_cat_op c, uint64_t ns);

/** CLOCK_MONOTONIC reading, ns resolution.  Cheap helper that callers
 * outside catalogue_dispatch.c use to time isolated catalogue-class
 * calls (e.g. the direct catalogue_rondb_layoutget_fused entry from
 * compound_layout.c, which bypasses the vtable). */
uint64_t mds_op_metrics_now_ns(void);

/* Inline timing macro for ad-hoc probes outside catalogue_dispatch.c.
 *
 * Times `expr`, observes the elapsed ns into the named cat-op
 * histogram, and does NOT enter the CATALOGUE phase (callers stay in
 * whichever phase was current).  Use this for code regions that may
 * or may not be NDB-bound -- the histogram tells you the cost; the
 * phase tracker keeps its high-level attribution intact.
 *
 * Build-disabled: degrades to a bare evaluation of `expr` with a
 * (void)(catop) so unused-variable warnings stay quiet. */
#if MDS_OP_METRICS_BUILD_ENABLED
#define MDS_TIME_CAT_OP(catop, expr) do {                              \
	uint64_t _mtco_t0 = 0;                                          \
	bool     _mtco_en = mds_op_metrics_enabled();                   \
	if (_mtco_en) {                                                 \
		_mtco_t0 = mds_op_metrics_now_ns();                     \
	}                                                               \
	expr;                                                           \
	if (_mtco_en) {                                                 \
		mds_cat_op_observe((catop),                             \
			mds_op_metrics_now_ns() - _mtco_t0);            \
	}                                                               \
} while (0)
#else
#define MDS_TIME_CAT_OP(catop, expr) do { (void)(catop); expr; } while (0)
#endif

/* -----------------------------------------------------------------------
 * Phase tracker (thread-local)
 *
 * Each worker has its own phase stack.  Phases form a small stack so
 * nested calls (a catalogue function that itself enters another phase)
 * accrue time correctly.  Time is always charged to the phase currently
 * on top of the stack.
 *
 * The PROTOCOL phase is the bottom of the stack and covers everything
 * not explicitly claimed by another phase (XDR encode/decode, COMPOUND
 * bookkeeping, slot validation, hash-table lookups, etc.).
 * ----------------------------------------------------------------------- */
enum mds_phase {
	MDS_PHASE_PROTOCOL = 0,
	MDS_PHASE_STATE,
	MDS_PHASE_CATALOGUE,
	MDS_PHASE_DS_IO,
	MDS_PHASE__COUNT,
};

const char *mds_phase_name(enum mds_phase p);

/** Arm the per-thread phase tracker.  PROTOCOL is the default. */
void mds_phase_begin_op(void);

/** Push a phase.  All time until matching leave charges to it. */
void mds_phase_enter(enum mds_phase p);

/** Pop the current phase.  Pairs with the most recent enter. */
void mds_phase_leave(void);

/**
 * Disarm the tracker and flush per-phase totals into the
 * op-class * phase histograms.  Idempotent if op is OTHER.
 */
void mds_phase_end_op(enum mds_op_class c);

/* -----------------------------------------------------------------------
 * Scope helper -- RAII-style phase enter/leave.
 *
 * MDS_PHASE_SCOPE(MDS_PHASE_STATE);
 *
 * Place at the top of a function (after any cheap argument-validation
 * returns).  The cleanup attribute fires mds_phase_leave() on every
 * exit path, including early returns and (in C, modulo longjmp) scope
 * end.  GCC and Clang ≥ 3 both support __attribute__((cleanup)); the
 * codebase mandates GCC ≥ 11.1 in CMakeLists.txt.
 *
 * Disabled-mode cost: the cleanup variable still exists, but the
 * cleanup function and mds_phase_enter() each early-return on a
 * single atomic load.  The compiler elides the unused variable in
 * optimised builds, leaving only two predicted-not-taken branches.
 *
 * Build-disabled (MDS_OP_METRICS_DISABLE): macro expands to (void)0
 * with no variable, no cleanup, no function calls.  Zero overhead.
 * ----------------------------------------------------------------------- */
#if MDS_OP_METRICS_BUILD_ENABLED

static inline void mds_phase_scope_end(int *unused)
{
	(void)unused;
	mds_phase_leave();
}

#define MDS_PHASE_SCOPE_CONCAT_(a, b) a##b
#define MDS_PHASE_SCOPE_CONCAT(a, b)  MDS_PHASE_SCOPE_CONCAT_(a, b)

#define MDS_PHASE_SCOPE(p)                                              \
	int MDS_PHASE_SCOPE_CONCAT(_mds_phase_scope_, __LINE__)         \
		__attribute__((cleanup(mds_phase_scope_end))) = 0;      \
	mds_phase_enter((p))

#else  /* !MDS_OP_METRICS_BUILD_ENABLED */

#define MDS_PHASE_SCOPE(p)  ((void)0)

#endif

/* -----------------------------------------------------------------------
 * Rendering
 * ----------------------------------------------------------------------- */

/**
 * Render all three histogram families as Prometheus text.
 *
 * @return Bytes written (excluding NUL), or -1 on truncation.
 */
int mds_op_metrics_render(char *buf, size_t cap);

#endif /* MDS_OP_METRICS_H */

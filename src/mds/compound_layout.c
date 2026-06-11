/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 */
/*
 * compound_layout.c -- pNFS layout ops (LAYOUTGET/RETURN/COMMIT, GETDEVICEINFO, LAYOUTERROR/STATS).
 */

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <arpa/inet.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "compound_internal.h"
#include "placement.h"
#include "proxy_io.h"
#include <stdatomic.h>
#include "ds_prepare.h"
#include "ds_cache.h"
#include "proxy_io.h"
#include "open_state.h"
#include "ds_health.h"
#include "commit_queue.h"
#include "io_tracker.h"
#include "catalogue_rondb.h"
#include "mds_metrics.h"
#include "layout_ds_ids.h"
#include "layout_cache.h"  /* Phase D of docs/hpc-nto1-plan.md */
#include "layout_commit_aggregator.h"  /* Phase F of docs/hpc-nto1-plan.md */
#include "layout_recall.h"  /* byte-range conflict-recall on op_layoutget */
#include "lease_table.h"
#include "lease_stripe_map.h"    /* stripe lease table (Phase 2) */
#include "mds_op_metrics.h" /* CAT_TIMED-equivalent for direct fused call */


/* -----------------------------------------------------------------------
 * pNFS layout handlers
 * ----------------------------------------------------------------------- */

/**
 * Build a device ID from mds_id=1 and ds_id.
 * Format: [mds_id BE 4][ds_id BE 4][zeros 8] = 16 bytes.
 */
static void build_deviceid(uint32_t mds_id, uint32_t ds_id,
			   uint8_t out[NFS4_DEVICEID4_SIZE])
{
	uint32_t mds = htonl(mds_id);
	uint32_t ds  = htonl(ds_id);

	memset(out, 0, NFS4_DEVICEID4_SIZE);
	memcpy(out, &mds, 4);
	memcpy(out + 4, &ds, 4);
}

/**
 * Extract ds_id from a 16-byte device ID.
 */
static uint32_t deviceid_to_ds_id(const uint8_t did[NFS4_DEVICEID4_SIZE])
{
	uint32_t ds;

	memcpy(&ds, did + 4, 4);
	return ntohl(ds);
}


/** Map flex transport policy to required transport bitmask. */
static uint8_t ff_policy_transport(enum ff_transport_policy pol)
{
	switch (pol) {
	case FF_TRANSPORT_RDMA: return DS_TRANSPORT_RDMA;
	case FF_TRANSPORT_BOTH:
		return DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA;
	case FF_TRANSPORT_TCP:
	default:
		return DS_TRANSPORT_TCP;
	}
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
/* -----------------------------------------------------------------------
 * Layout stateid generation -- globally unique per layout grant.
 *
 * Wire format (12 bytes of stateid.other):
 *     [mds_id BE 4B][counter BE 8B]
 *
 * The counter is a single 64-bit monotonic integer.  It MUST never
 * repeat across MDS restarts, because the catalogue keeps layout_state
 * rows from previous runs (placement, stripe map, and -- when
 * transient_state_cache is off or the CQ pregrant path fires -- layout
 * grants themselves) until the client LAYOUTRETURNs or the server GCs.
 * If the counter restarts at 1 after a daemon restart, the new run's
 * LAYOUTGET issues a stateid that already exists in layout_state for a
 * different fileid.  The next LAYOUTCOMMIT looks the stateid up, sees
 * the stale row with the OLD fileid, compares it against the current
 * compound's FH (the NEW fileid), and returns NFS4ERR_BAD_STATEID.
 * Because the Linux client swallows BAD_STATEID on LAYOUTCOMMIT
 * silently (just drops the pending size update and moves on), the
 * write looks successful to userspace but the MDS never updates the
 * file size -- resulting in an apparently-successful cp producing a
 * 0-byte destination.  See git log for the lab reproduction.
 *
 * Fix: seed the counter from CLOCK_REALTIME nanoseconds at the first
 * call.  The seed is strictly larger than any counter value a
 * previous boot of this MDS could have reached (a single MDS issuing
 * stateids at one-per-nanosecond for the lifetime of the universe
 * would still leave headroom), so collisions with persisted rows are
 * impossible.
 *
 * Same-shape generators exist in open_state.c and lock_state.c;
 * those paths are being audited separately -- both currently rely on
 * the same counter pattern but their rows are cleared on restart
 * under transient_state_cache, so the collision window is narrower.
 * ----------------------------------------------------------------------- */

_Atomic uint64_t g_layout_sid_counter = 0;

/* One-shot initialiser run on first make_layout_stateid() call.
 * pthread_once gives us a thread-safe, no-dependency initialiser
 * without needing a daemon-startup hook. */
static pthread_once_t g_layout_sid_seed_once = PTHREAD_ONCE_INIT;

/* -----------------------------------------------------------------------
 * In-memory layout-stateid seqid tracker.
 *
 * RFC 8881 S12.5.3 requires a layout stateid's seqid to advance
 * monotonically across LAYOUTGET / LAYOUTRETURN / LAYOUTCOMMIT.  When
 * transient_state_cache=true (lab default) the layout_state row is
 * never written to NDB, so mds_coord_layout_get_by_stateid() always
 * misses and the server cannot distinguish a renewal from a new grant.
 *
 * This module-local table records every layout `other` we issue and
 * the latest seqid we returned for it.  Lookup is keyed on the 12-byte
 * `other`; chained hash with a striped mutex.  Entries persist for
 * the lifetime of the daemon -- same lifecycle as the layout itself
 * for transient mode.  Memory is bounded by the number of distinct
 * granted stateids (one per (clientid, fileid) pair on most paths);
 * the hash overhead is sizeof(struct layout_seqid_entry) per entry.
 *
 * Persistent mode (transient_state_cache=false) ALSO updates this
 * table so the in-memory and NDB views stay coherent across daemon
 * runs -- but the catalogue probe in layout_pick_stateid() runs first
 * and short-circuits on hit, so this table is the fallback path.
 * ----------------------------------------------------------------------- */

#define LAYOUT_SEQID_BUCKETS 1024
#define LAYOUT_SEQID_STRIPES 32

struct layout_seqid_entry {
	uint8_t  other[NFS4_OTHER_SIZE];
	uint32_t seqid;
	struct layout_seqid_entry *hash_next;
};

static struct layout_seqid_entry *g_layout_seqid_buckets[LAYOUT_SEQID_BUCKETS];
static pthread_mutex_t g_layout_seqid_locks[LAYOUT_SEQID_STRIPES];
static pthread_once_t  g_layout_seqid_init_once = PTHREAD_ONCE_INIT;

static void layout_seqid_init(void)
{
	for (uint32_t i = 0; i < LAYOUT_SEQID_STRIPES; i++) {
		pthread_mutex_init(&g_layout_seqid_locks[i], NULL);
	}
}

static uint32_t layout_seqid_hash(const uint8_t other[NFS4_OTHER_SIZE])
{
	uint64_t v = 0;
	memcpy(&v, other + 4, sizeof(v));
	v ^= v >> 33;
	v *= 0xff51afd7ed558ccdULL;
	v ^= v >> 33;
	return (uint32_t)(v % LAYOUT_SEQID_BUCKETS);
}

/*
 * Insert a freshly-allocated layout `other` with seqid = 1.  Idempotent
 * -- a duplicate call (same `other`) leaves the existing entry alone
 * because make_layout_stateid uses a unique counter and the `other`
 * collision space is 2^96 wide.
 */
static void layout_seqid_record_new(const uint8_t other[NFS4_OTHER_SIZE])
{
	uint32_t bucket;
	uint32_t stripe;
	struct layout_seqid_entry *e;

	(void)pthread_once(&g_layout_seqid_init_once, layout_seqid_init);

	bucket = layout_seqid_hash(other);
	stripe = bucket % LAYOUT_SEQID_STRIPES;
	pthread_mutex_lock(&g_layout_seqid_locks[stripe]);
	for (e = g_layout_seqid_buckets[bucket]; e != NULL; e = e->hash_next) {
		if (memcmp(e->other, other, NFS4_OTHER_SIZE) == 0) {
			pthread_mutex_unlock(
				&g_layout_seqid_locks[stripe]);
			return;
		}
	}
	e = calloc(1, sizeof(*e));
	if (e != NULL) {
		memcpy(e->other, other, NFS4_OTHER_SIZE);
		e->seqid = 1;
		e->hash_next = g_layout_seqid_buckets[bucket];
		g_layout_seqid_buckets[bucket] = e;
	}
	pthread_mutex_unlock(&g_layout_seqid_locks[stripe]);
}

/*
 * Upsert: set the in-memory tracker's seqid for @other to exactly
 * @seqid.  Inserts a fresh entry if none exists; updates the existing
 * entry's seqid if it does.  Used by layout_recall.c for
 * CB_LAYOUTRECALL stateid management.
 */
void layout_seqid_record_at(const uint8_t other[NFS4_OTHER_SIZE],
			    uint32_t seqid)
{
	uint32_t bucket;
	uint32_t stripe;
	struct layout_seqid_entry *e;

	if (seqid == 0U) {
		seqid = 1U;
	}

	(void)pthread_once(&g_layout_seqid_init_once, layout_seqid_init);

	bucket = layout_seqid_hash(other);
	stripe = bucket % LAYOUT_SEQID_STRIPES;
	pthread_mutex_lock(&g_layout_seqid_locks[stripe]);
	for (e = g_layout_seqid_buckets[bucket]; e != NULL; e = e->hash_next) {
		if (memcmp(e->other, other, NFS4_OTHER_SIZE) == 0) {
			if (seqid > e->seqid) {
				e->seqid = seqid;
			}
			pthread_mutex_unlock(
				&g_layout_seqid_locks[stripe]);
			return;
		}
	}
	e = calloc(1, sizeof(*e));
	if (e != NULL) {
		memcpy(e->other, other, NFS4_OTHER_SIZE);
		e->seqid = seqid;
		e->hash_next = g_layout_seqid_buckets[bucket];
		g_layout_seqid_buckets[bucket] = e;
	}
	pthread_mutex_unlock(&g_layout_seqid_locks[stripe]);
}

/*
 * Look up @other in the table.  On hit, advance the stored seqid by
 * one (with 0xFFFFFFFF -> 1 wrap per RFC 8881 S8.2.2) and return the
 * NEW seqid via @next_seqid; sets *@hit = true.  On miss, *@hit =
 * false and @next_seqid is untouched.
 */
void layout_seqid_advance(const uint8_t other[NFS4_OTHER_SIZE],
			  bool *hit, uint32_t *next_seqid)
{
	uint32_t bucket;
	uint32_t stripe;
	struct layout_seqid_entry *e;

	*hit = false;
	(void)pthread_once(&g_layout_seqid_init_once, layout_seqid_init);

	bucket = layout_seqid_hash(other);
	stripe = bucket % LAYOUT_SEQID_STRIPES;
	pthread_mutex_lock(&g_layout_seqid_locks[stripe]);
	for (e = g_layout_seqid_buckets[bucket]; e != NULL; e = e->hash_next) {
		if (memcmp(e->other, other, NFS4_OTHER_SIZE) == 0) {
			uint32_t next = e->seqid + 1U;
			if (next == 0U) {
				next = 1U;
			}
			e->seqid = next;
			*next_seqid = next;
			*hit = true;
			break;
		}
	}
	pthread_mutex_unlock(&g_layout_seqid_locks[stripe]);
}

/*
 * Peek: return the current (last-issued) seqid for @other without
 * advancing it.  Used by LAYOUTRETURN seqid validation -- RFC 8881
 * S8.2.2 says a stateid presented with seqid < current is OLD_STATEID,
 * with seqid > current is BAD_STATEID, and with seqid == current is
 * the latest copy and acceptable.
 *
 * Returns true on hit (sets *cur_seqid); false on miss (caller decides
 * whether to treat that as BAD_STATEID or accept by other validation).
 */
bool layout_seqid_peek(const uint8_t other[NFS4_OTHER_SIZE],
		       uint32_t *cur_seqid)
{
	uint32_t bucket;
	uint32_t stripe;
	struct layout_seqid_entry *e;
	bool hit = false;

	(void)pthread_once(&g_layout_seqid_init_once, layout_seqid_init);

	bucket = layout_seqid_hash(other);
	stripe = bucket % LAYOUT_SEQID_STRIPES;
	pthread_mutex_lock(&g_layout_seqid_locks[stripe]);
	for (e = g_layout_seqid_buckets[bucket]; e != NULL;
	     e = e->hash_next) {
		if (memcmp(e->other, other, NFS4_OTHER_SIZE) == 0) {
			*cur_seqid = e->seqid;
			hit = true;
			break;
		}
	}
	pthread_mutex_unlock(&g_layout_seqid_locks[stripe]);
	return hit;
}

/* Remove an entry from the in-memory tracker.  Idempotent / NULL-safe.
 * Used by op_layoutreturn after the stateid has been accepted so a
 * subsequent op presenting the now-returned stateid sees a clean
 * miss -- the caller decides whether that is BAD_STATEID or a fresh
 * grant. */
void layout_seqid_remove(const uint8_t other[NFS4_OTHER_SIZE])
{
	uint32_t bucket;
	uint32_t stripe;
	struct layout_seqid_entry **pp;

	(void)pthread_once(&g_layout_seqid_init_once, layout_seqid_init);

	bucket = layout_seqid_hash(other);
	stripe = bucket % LAYOUT_SEQID_STRIPES;
	pthread_mutex_lock(&g_layout_seqid_locks[stripe]);
	for (pp = &g_layout_seqid_buckets[bucket]; *pp != NULL;
	     pp = &(*pp)->hash_next) {
		if (memcmp((*pp)->other, other, NFS4_OTHER_SIZE) == 0) {
			struct layout_seqid_entry *dead = *pp;
			*pp = dead->hash_next;
			free(dead);
			break;
		}
	}
	pthread_mutex_unlock(&g_layout_seqid_locks[stripe]);
}

static void seed_layout_sid_counter(void)
{
	struct timespec ts;
	uint64_t seed;

	if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
		/* Fallback: CLOCK_MONOTONIC still beats starting from 1. */
		(void)clock_gettime(CLOCK_MONOTONIC, &ts);
	}
	seed = (uint64_t)ts.tv_sec * 1000000000ULL +
	       (uint64_t)ts.tv_nsec;
	/* Guarantee we never start at zero (which some layers could
	 * interpret as "unset") even on a clock skew to the epoch. */
	if (seed == 0) {
		seed = 1;
	}
	atomic_store(&g_layout_sid_counter, seed);
}

void make_layout_stateid(uint32_t mds_id,
			struct nfs4_stateid *out)
{
	uint32_t mds_be;
	uint64_t seq_be;

	(void)pthread_once(&g_layout_sid_seed_once, seed_layout_sid_counter);

	mds_be = htobe32(mds_id);
	seq_be = htobe64(atomic_fetch_add(&g_layout_sid_counter, 1));

	memset(out, 0, sizeof(*out));
	out->seqid = 1;
	memcpy(out->other, &mds_be, 4);
	memcpy(out->other + 4, &seq_be, 8);

	/* Register the new `other` so subsequent LAYOUTGET / LAYOUTCOMMIT
	 * / LAYOUTRETURN that pass this stateid back can advance the
	 * seqid (RFC 8881 S12.5.3).  See layout_seqid_advance(). */
	layout_seqid_record_new(out->other);
}

/*
 * Hard upper bound on the per-LAYOUTGET grant range length.
 *
 * Linux pNFS clients legitimately send LAYOUTGET with
 * (length == UINT64_MAX, minlength == UINT64_MAX) to mean "give me
 * access starting at the offset onward".  Granting an actual
 * UINT64_MAX-byte window forces every CB_LAYOUTRECALL on a conflict
 * to be whole-file because there is no narrower range to intersect
 * against.
 *
 * Cap the per-grant range so the wire response and persisted row are
 * always strictly bounded.  Linux clients re-issue LAYOUTGET when
 * their I/O extends past the granted window, so a bounded grant is
 * functionally equivalent for finite workloads while letting the
 * server emit byte-range CB_LAYOUTRECALLs.
 */
#define MDS_LAYOUT_GRANT_MAX_LENGTH_DEFAULT (1ULL << 36) /* 64 GiB */

static _Atomic(uint64_t) mds_layout_grant_max_length =
	MDS_LAYOUT_GRANT_MAX_LENGTH_DEFAULT;

void compound_layout_set_grant_max_length(uint64_t bytes)
{
	uint64_t cap = bytes;

	if (cap < 65536ULL) {
		cap = 65536ULL;
	}
	atomic_store(&mds_layout_grant_max_length, cap);
}

static uint64_t layout_clamp_grant_length(uint64_t offset, uint64_t length)
{
	uint64_t cap = atomic_load(&mds_layout_grant_max_length);

	if (offset >= UINT64_MAX - 1ULL) {
		return 0;
	}
	if (cap > UINT64_MAX - 1ULL - offset) {
		cap = UINT64_MAX - 1ULL - offset;
	}
	if (length > cap) {
		return cap;
	}
	return length;
}

static enum nfs4_status layout_select_grant_range(
	const struct nfs4_arg_layoutget *a,
	const struct mds_inode *inode,
	uint32_t configured_stripe_unit,
	uint64_t *grant_offset,
	uint64_t *grant_length)
{
	uint64_t window;

	if (a == NULL || grant_offset == NULL || grant_length == NULL) {
		return NFS4ERR_IO;
	}

	*grant_offset = a->offset;

	/* Overflow check for finite requests. */
	if (a->length != UINT64_MAX &&
	    a->length > UINT64_MAX - a->offset) {
		return NFS4ERR_INVAL;
	}
	if (a->offset == UINT64_MAX) {
		return NFS4ERR_INVAL;
	}

	/*
	 * RFC 8881 S18.43.3: the server MAY return a layout that covers
	 * a byte range larger than requested.  Always widen the grant to
	 * at least the stripe unit or the remaining file size so a
	 * single LAYOUTGET during writeback (which requests exactly one
	 * 4KiB page) covers the entire dirty range.  Without this the
	 * kernel client sends one LAYOUTGET per dirty page, creating a
	 * storm of ~65,000 serialized RPCs for a 250MB file that
	 * overwhelms the MDS and causes close() to hang.
	 *
	 * The floor is the client's requested length (may be 4K or
	 * UINT64_MAX); the ceiling is layout_grant_max_length_bytes
	 * (1 GiB) applied by layout_clamp_grant_length() below.
	 */
	window = configured_stripe_unit > 0 ? configured_stripe_unit : 65536ULL;
	/* Use the client's requested length as a floor so we never
	 * grant less than what was asked. */
	if (a->length != UINT64_MAX && a->length > window) {
		window = a->length;
	}
	if (a->minlength != UINT64_MAX && a->minlength > window) {
		window = a->minlength;
	}
	if (inode != NULL && inode->size > a->offset) {
		uint64_t remaining = inode->size - a->offset;

		if (remaining > window) {
			window = remaining;
		}
	}
	if (window == 0) {
		window = 1;
	}
	window = layout_clamp_grant_length(a->offset, window);
	if (window == 0) {
		return NFS4ERR_INVAL;
	}

	*grant_length = window;
	return NFS4_OK;
}

/*
 * RFC 8881 S12.5.3 / S8.2.2 -- layout stateid renewal.
 *
 * When a client passes the previously-issued layout stateid as input
 * to LAYOUTGET, the server MUST return the SAME `other` with seqid
 * advanced by one (wrapping 0xFFFFFFFF -> 1, since 0 is reserved).
 * Non-layout inputs (open stateid, CLAIM_NULL, special-zero) get a
 * fresh stateid with seqid = 1.
 *
 * pynfs FFST1 (testStateid1), FFLG2 (testFlexLayoutStress),
 * FFLA1 (testFlexLayoutTestAccess), FFLOOS (testFlexLayoutOldSeqid)
 * all rely on this monotonic advance.  The previous behaviour
 * (always allocating a fresh `other` via make_layout_stateid)
 * leaked a layout_state row per LAYOUTGET and caused the client to
 * see seqid stuck at 1 forever.
 *
 * Detection: probe mds_coord_layout_get_by_stateid for the incoming
 * stateid_other.  A hit whose persisted fileid matches the current
 * compound's FH is treated as renewal.  A miss (or fileid mismatch)
 * is treated as a new grant.  When @cat is NULL (e.g. unit-test
 * harness without a coordination backend), we conservatively fall
 * back to allocating a fresh stateid -- the legacy behaviour.
 */
static void layout_pick_stateid(struct compound_data *cd,
				const struct nfs4_stateid *client_sid,
				struct nfs4_stateid *out)
{
	/* Step 1: in-memory layout-stateid table.  This is the only path
	 * that works under transient_state_cache=true (lab default) where
	 * the layout_state row never reaches NDB. */
	if (client_sid != NULL && client_sid->seqid != 0) {
		bool hit = false;
		uint32_t next_seqid = 0;

		layout_seqid_advance(client_sid->other, &hit, &next_seqid);
		if (hit) {
			memset(out, 0, sizeof(*out));
			out->seqid = next_seqid;
			memcpy(out->other, client_sid->other,
			       NFS4_OTHER_SIZE);
			return;
		}
	}

	/* Step 2: persistent path.  When the layout_state row IS in NDB
	 * (transient_state_cache=false), the in-memory table can lose
	 * entries on daemon restart; fall through to the catalogue probe
	 * before allocating fresh.  Match on fileid prevents replaying
	 * a stale stateid against the wrong file. */
	if (cd != NULL && cd->cat != NULL && client_sid != NULL &&
	    client_sid->seqid != 0) {
		uint64_t row_clientid = 0;
		uint64_t row_fileid = 0;
		uint32_t row_iomode = 0;
		uint64_t row_offset = 0;
		uint64_t row_length = 0;
		uint32_t row_seqid = 0;
		enum mds_status st;

		st = mds_coord_layout_get_by_stateid(
			cd->cat, client_sid->other,
			&row_clientid, &row_fileid,
			&row_iomode, &row_offset, &row_length,
			&row_seqid);
		if (st == MDS_OK && cd->current_fh_set &&
		    row_fileid == cd->current_fh.fileid) {
			uint32_t next = row_seqid + 1U;
			if (next == 0U) {
				next = 1U; /* RFC: skip reserved 0. */
			}
			memset(out, 0, sizeof(*out));
			out->seqid = next;
			memcpy(out->other, client_sid->other,
			       NFS4_OTHER_SIZE);
			/* Sync the in-memory table so the next call hits
			 * the fast path. */
			layout_seqid_record_new(client_sid->other);
			return;
		}
	}

	/* Step 3: not a known layout stateid (open stateid, special-zero,
	 * or first LAYOUTGET on a fresh client).  Allocate a fresh one. */
	make_layout_stateid(cd != NULL ? cd->mds_id : 0, out);
}

static bool layout_state_is_root_global(const struct compound_data *cd)
{
	if (cd == NULL) {
		return false;
	}
	/* RonDB mode: catalogue is set.
	 * Always use the catalogue/root-global path. */
	return cd->cat != NULL;
}

/*
 * Phase H of docs/hpc-nto1-plan.md -- compatibility filter that
 * prefers RDMA / GPUDirect DSes for HPC-shared inodes when the
 * cluster has any, and falls back to the full required-transport
 * set otherwise.  Plain (non-HPC) inodes go through
 * ds_filter_compatible unchanged so the legacy path is bit-for-bit
 * identical for the common workload.
 *
 * Why a thin wrapper rather than threading the inode flag down into
 * placement.c: the placement layer is intentionally policy-agnostic.
 * Keeping the HPC preference here keeps placement.c free of NFSv4 /
 * compound concerns and means a future Phase J / H+ topology hint
 * lands in this same helper without churn in the placement library.
 */
static enum mds_status layout_filter_for_inode(
	const struct mds_inode *inode,
	const struct mds_ds_info *in, uint32_t in_count,
	uint8_t required_mode, uint8_t required_transport,
	struct mds_ds_info **out, uint32_t *out_count)
{
	bool hpc = (inode != NULL) &&
		   ((inode->flags & MDS_IFLAG_HPC_SHARED) != 0);
	if (!hpc) {
		return ds_filter_compatible(in, in_count,
					    required_mode,
					    required_transport,
					    out, out_count);
	}
	return ds_filter_compatible_preferred(
		in, in_count,
		required_mode, required_transport,
		DS_TRANSPORT_RDMA, DS_CAP_GPUDIRECT,
		out, out_count);
}


static bool layout_entry_ready_for_grant(const struct compound_data *cd,
					 const struct mds_ds_map_entry *entry)
{
	(void)cd;

	if (entry == NULL) {
		return false;
	}

	return entry->nfs_fh_len > 0;
}

static bool layout_entries_ready_for_grant(const struct compound_data *cd,
					   const struct mds_ds_map_entry *entries,
					   uint32_t total)
{
	uint32_t i;

	if (entries == NULL || total == 0) {
		return false;
	}

	for (i = 0; i < total; i++) {
		if (!layout_entry_ready_for_grant(cd, &entries[i])) {
			return false;
		}
	}

	return true;
}

static enum nfs4_status layout_make_ds_list(
	const struct mds_ds_map_entry *entries,
	uint32_t total,
	struct layout_ds_id_list *out)
{
	enum mds_status st;
	if (out == NULL || entries == NULL || total == 0) {
		return NFS4ERR_IO;
	}
	st = layout_ds_id_list_from_entries(out, entries, total);
	if (st == MDS_ERR_NOMEM) {
		return NFS4ERR_RESOURCE;
	}
	if (st != MDS_OK) {
		return NFS4ERR_IO;
	}
	if (out->count == 0) {
		layout_ds_id_list_destroy(out);
		return NFS4ERR_IO;
	}
	return NFS4_OK;
}

static enum nfs4_status layout_revoke_grant_entries(
	struct compound_data *cd,
	const struct nfs4_stateid *stateid,
	const struct mds_ds_map_entry *entries,
	uint32_t total)
{
	struct layout_ds_id_list ds_list = { 0 };
	enum nfs4_status nst;
	enum mds_status st;

	if (cd == NULL || cd->cat == NULL ||
	    stateid == NULL || entries == NULL || total == 0) {
		return NFS4ERR_IO;
	}
	nst = layout_make_ds_list(entries, total, &ds_list);
	if (nst != NFS4_OK) {
		return nst;
	}
	st = mds_coord_layout_return(
		cd->cat, NULL,
		stateid->other,
		cd->clientid,
		cd->current_fh.fileid,
		ds_list.ids, ds_list.count);
	layout_ds_id_list_destroy(&ds_list);

	if (st == MDS_OK || st == MDS_ERR_NOTFOUND) {
		return NFS4_OK;
	}
	return mds_status_to_nfs4(st);
}

static void layout_clear_ds_pending(struct compound_data *cd, uint64_t fileid)
{
	struct mds_inode inode;
	enum mds_status st;

	if (cd == NULL || cd->cat == NULL) {
		return;
	}

	st = cat_getattr(cd, fileid, &inode);
	if (st != MDS_OK || !(inode.flags & MDS_IFLAG_DS_PENDING)) {
		return;
	}
	inode.flags &= ~MDS_IFLAG_DS_PENDING;
	(void)cat_setattr(cd, fileid, &inode, MDS_ATTR_FLAGS);
	compound_inode_invalidate(cd, fileid);
}

/**
 * Revoke a layout grant whose entries have unready FHs, and tell the
 * client to retry.
 *
 * Phase 12 Component A (see docs/design-layoutget-decoupling.md S4.1):
 * LAYOUTGET no longer runs synchronous DS NFS RPCs on the hot path.
 * When a placement produces entries with nfs_fh_len == 0, we:
 *
 *   1. Return the already-granted layout state via mds_coord_layout_return
 *      so the per-(clientid, fileid) state is not leaked.
 *   2. Mark the inode DS_PENDING so subsequent LAYOUTGETs take the
 *      ds_prepare fast-path.
 *   3. Enqueue the fileid into ds_prepare so the background worker
 *      populates the FH before the client retries.
 *   4. Return NFS4ERR_DELAY (RFC 8881 S15.1.1.3) -- client retries
 *      with backoff.  We deliberately do NOT return
 *      NFS4ERR_LAYOUTUNAVAILABLE because the Linux client treats
 *      that as a dead path and falls back to proxy I/O through the
 *      MDS, which violates the no-inline-writes rule.
 */
static enum nfs4_status layout_revoke_unready_grant(
	struct compound_data *cd,
	const struct nfs4_stateid *stateid,
	const struct mds_ds_map_entry *entries,
	uint32_t total)
{
	struct mds_inode inode;
	enum mds_status gst;
	enum nfs4_status nst;

	if (cd == NULL || cd->cat == NULL ||
	    stateid == NULL || entries == NULL || total == 0) {
		return NFS4ERR_IO;
	}
	nst = layout_revoke_grant_entries(cd, stateid, entries, total);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* Mark DS_PENDING + enqueue async prepare. */
	gst = cat_getattr(cd, cd->current_fh.fileid, &inode);
	if (gst == MDS_OK) {
		if (!(inode.flags & MDS_IFLAG_DS_PENDING)) {
			inode.flags |= MDS_IFLAG_DS_PENDING;
			(void)cat_setattr(cd, cd->current_fh.fileid,
					  &inode, MDS_ATTR_FLAGS);
			compound_inode_invalidate(
				cd, cd->current_fh.fileid);
		}
		compound_maybe_enqueue_ds_prepare(cd, &inode);
	}

	atomic_fetch_add_explicit(
		&g_branch_metrics.layoutget_delay_count, 1,
		memory_order_relaxed);
	return NFS4ERR_DELAY;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_layoutget(struct compound_data *cd,
				     const struct nfs4_op *op,
				     struct nfs4_result *res)
{
	const struct nfs4_arg_layoutget *a = &op->arg.layoutget;
	struct nfs4_res_layoutget *r = &res->res.layoutget;
	struct mds_inode inode;
	struct mds_ds_info *ds_list = NULL;
	uint32_t ds_count = 0;
	struct mds_ds_map_entry *entries = NULL;
	uint32_t stripe_count, stripe_unit, mirror_count;
	enum mds_status st;
	enum nfs4_status nst;
	int prep_state;
	bool clear_ds_pending_on_success = false;
	uint32_t i;
	struct nfs4_stateid client_sid;

	/* RFC 8881 S16.2.4 -- resolve CURRENT_STATEID4 marker.  Pynfs
	 * CSID7 testOpenLayoutGet bundles LAYOUTGET right after OPEN with
	 * the marker as the layout stateid argument.  Resolve once here
	 * so all six call sites of layout_pick_stateid below see the same
	 * resolved input. */
	nst = compound_resolve_stateid(cd, &a->stateid, &client_sid);
	if (nst != NFS4_OK) {
		return nst;
	}

	/*
	 * Phase 6 -- wide long-lived layout grant.
	 *
	 * Rather than echo the client's requested range and iomode,
	 * we always grant a read-write, whole-file, long-lived layout.
	 * RFC 8881 S12.5.3 explicitly allows the server to grant a
	 * wider iomode / range than requested; the client may use the
	 * wider grant for any subsequent I/O without issuing another
	 * LAYOUTGET.  Combined with return_on_close=false, this lets a
	 * client reuse the same layout across close/reopen cycles until
	 * the server recalls it.
	 *
	 * Why this is safe now: the CB_RECALL path is production-hard
	 * (dedupe window in deleg_recall_file, tested via null-arg
	 * suites in test_nfs4_cb), so a long-lived grant will actually
	 * get recalled when another client needs conflicting access.
	 *
	 * Concrete effect: a mdtest-style workload that opens a file,
	 * does one LAYOUTGET, writes some bytes, closes, then reopens
	 * and writes again no longer needs a second LAYOUTGET -- one
	 * NDB commit saved per file.  Measured wins come from any
	 * workload where the same file is re-opened within its lease
	 * window.
	 */
	const uint32_t grant_iomode = LAYOUTIOMODE4_RW;
	uint64_t grant_offset = 0;
	uint64_t grant_length = UINT64_MAX;
	uint64_t lease_offset = 0;
	uint64_t lease_length = UINT64_MAX;
	uint64_t recall_offset = 0;
	uint64_t recall_length = UINT64_MAX;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* Only support files layout type. */
	if (a->layout_type != LAYOUT4_NFSV4_1_FILES && a->layout_type != LAYOUT4_FLEX_FILES) {
		return NFS4ERR_NOTSUPP;
}

	/* Must be a regular file. */
	MDS_TIME_CAT_OP(MDS_CATOP_INODE_GET,
		st = compound_inode_get(cd, cd->current_fh.fileid, &inode));
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
}
	if (inode.type != MDS_FTYPE_REG) {
		return NFS4ERR_INVAL;
}

	/* Inline files are served via MDS proxy I/O, not pNFS layouts.
	 * DS_PENDING files have stripe_maps and can serve layouts. */
	if ((inode.flags & MDS_IFLAG_INLINE) &&
	    !(inode.flags & MDS_IFLAG_DS_PENDING)) {
		return NFS4ERR_LAYOUTUNAVAILABLE;
}
	nst = layout_select_grant_range(
		a, &inode, cd->cfg_stripe_unit,
		&grant_offset, &grant_length);
	if (nst != NFS4_OK) {
		return nst;
	}
	/* Decouple stripe-lease and recall scope from the grant scope.
	 *
	 * The grant range (returned to the client on the wire) is widened
	 * by layout_select_grant_range() up to layout_grant_max_length_bytes
	 * (1 GiB) to avoid per-page LAYOUTGET storms during writeback.
	 *
	 * The stripe-lease range, however, should track what the client
	 * actually intends to write -- taken from loga_minlength
	 * (RFC 5661 18.43.2, RFC 8881 18.43.3).  Without this decoupling,
	 * two clients writing disjoint sub-GiB regions of the same
	 * logical file would always see a lease-scope conflict even
	 * though the underlying byte ranges do not overlap.
	 *
	 * Fall back to the configured stripe unit when the client did
	 * not supply a meaningful minlength.  Never let the lease exceed
	 * the grant. */
	lease_offset = a->offset;
	if (a->minlength != UINT64_MAX && a->minlength > 0) {
		lease_length = a->minlength;
	} else {
		lease_length = cd->cfg_stripe_unit > 0
			? cd->cfg_stripe_unit
			: 65536ULL;
	}
	if (cd->cfg_auto_widen_lease_on_4k &&
	    a->minlength == 4096 &&
	    a->length == UINT64_MAX) {
		lease_length = UINT64_MAX; /* capped to grant_length below */
	}
	/*
	 * Floor: clamp lease_length up to the configured stripe
	 * unit (or 64 KiB if unconfigured) so the smallest possible
	 * lease still aligns with the smallest sensible on-wire
	 * I/O unit.  Strict-N-to-1 clients already send
	 * loga_minlength >= stripe_unit, so this floor is a no-op
	 * for them.  Defends the non-strict path (e.g. Linux
	 * page-cache writeback issuing loga_minlength = 4096)
	 * from creating one-page leases that fragment the lease
	 * table.
	 */
	{
		uint64_t lease_floor = cd->cfg_stripe_unit > 0
			? cd->cfg_stripe_unit
			: 65536ULL;
		if (lease_length < lease_floor) {
			lease_length = lease_floor;
		}
	}
	if (lease_length > grant_length) {
		lease_length = grant_length;
	}
	recall_offset = lease_offset;
	recall_length = lease_length;

	/*
	 * Mark's bug -- byte-range CB_LAYOUTRECALL.
	 *
	 * Before granting a new layout, scan existing holders on this
	 * fileid and emit byte-range CB_LAYOUTRECALL to any holder
	 * whose iomode conflicts with the requesting iomode AND whose
	 * range overlaps the requested (offset, length).  The kernel
	 * client (Linux 6.18+ Slice 3) consumes the byte-range recall
	 * by invalidating only the recalled sub-range of its layout
	 * cache, instead of dropping the whole layout as it would for
	 * a whole-file CB_LAYOUTRECALL.
	 *
	 * Best-effort: any CB delivery error or catalogue scan failure
	 * still leaves the catalogue in a coherent state -- holders
	 * whose row was revoked observe NFS4ERR_BAD_STATEID on their
	 * next LAYOUT*, and the requesting LAYOUTGET below proceeds
	 * normally.  Skipped when:
	 *   - cd->lr is unset (test compat / no recall coordinator),
	 *   - the request's stateid matches the requesting client's
	 *     existing layout (idempotent renew -- no recall needed).
	 * The same-client case is handled inside the helper by the
	 * clientid filter, but we early-out here to avoid the catalogue
	 * round trip for the common renew path.
	 */
	/*
	 * When transient_state_cache=on, this MDS does not persist
	 * layout grants to NDB (every `mds_coord_layout_grant` call
	 * site below is gated on `!cd->skip_transient_ndb`).  As a
	 * direct consequence the `layout_state` table -- which is
	 * the only thing layout_recall_byte_range_for_holders ->
	 * mds_coord_layout_iter_file scans -- is empty from this
	 * MDS's perspective, so the recall scan is a guaranteed-
	 * miss NDB round-trip on the LAYOUTGET hot path (measured
	 * at ~37 ms p50 on the lab cluster, ~100% of LAYOUTGET's
	 * total latency).  Skip the scan when the same flag that
	 * gates the writes is set; that keeps reads and writes
	 * consistent and trims LAYOUTGET from ~42 ms back to ~1 ms
	 * for the create-write-close pattern an untar issues.
	 *
	 * Caveat for heterogeneous deployments: if a different MDS
	 * in the cluster runs with transient_state_cache=off and
	 * writes its grants to NDB, those holders are invisible to
	 * us here.  Today's lab + production configs run the same
	 * flag across all MDSes, so the trade-off is currently
	 * vacuous.  If/when we mix modes we should fall back to the
	 * full scan whenever `cd->skip_transient_ndb` is false.
	 */
	if (cd->lr != NULL && !cd->skip_transient_ndb) {
		uint32_t recalled = 0;
		uint32_t req_iomode_for_recall = a->iomode;

		/* Promote READ to RW for the conflict scan when the
		 * server is granting RW upgrades unconditionally
		 * (matches the long-lived grant policy below): if we
		 * are about to grant RW, any conflicting holder must
		 * see a recall regardless of what the client asked
		 * for. */
		if (grant_iomode == LAYOUTIOMODE4_RW) {
			req_iomode_for_recall = LAYOUTIOMODE4_RW;
		}

		MDS_TIME_CAT_OP(MDS_CATOP_LAYOUT_RECALL_SCAN,
			(void)layout_recall_byte_range_for_holders(
				cd->lr,
				cd->current_fh.fileid,
				cd->clientid,
				req_iomode_for_recall,
				recall_offset,
				recall_length,
				a->layout_type,
				&recalled));
		(void)recalled; /* tracked via per-holder log; metrics tbd */
	}

	/* Stripe lease conflict check.
	 *
	 * When the stripe lease table is active (cd->slt != NULL and
	 * cd->cfg_stripe_lease_duration_ms > 0), check whether another
	 * client already holds an unexpired lease on an overlapping
	 * range of this fileid.  If so, return NFS4ERR_LAYOUTTRYLATER
	 * so the requesting client retries after a backoff.
	 *
	 * Same-client requests are allowed (renewal path). */
	if (cd->slt != NULL && cd->cfg_stripe_lease_duration_ms > 0 &&
	    (inode.flags & MDS_IFLAG_HPC_SHARED) != 0) {
		/* Patch 0006: per-stripe lease arbitration is gated by
		 * MDS_IFLAG_HPC_SHARED on the inode -- non-HPC files never
		 * enter the SLT, preserving baseline (non-arbitrated)
		 * behaviour for regular workloads.
		 *
		 * Patch 0005: per-DS-stripe lease keying.  Decompose the
		 * logical lease range into per-stripe slices and conflict-
		 * check each one against the lease table.  Slicing fails
		 * closed -- on error we return TRYLATER. */
		struct stripe_slice _slt_slices[MDS_MAX_STRIPES];
		uint32_t _slt_unit = cd->cfg_stripe_unit ? cd->cfg_stripe_unit : 65536U;
		int _slt_n = lease_range_to_stripe_slices(
			lease_offset, lease_length, _slt_unit,
			_slt_slices, MDS_MAX_STRIPES);
		if (_slt_n < 0) {
			return NFS4ERR_LAYOUTTRYLATER;
		}
		/* Patch 0007: contention-aware grant narrowing.
		 * Find the longest conflict-free file-byte prefix.  If
		 * 0 < prefix < lease_length, narrow lease+grant to the
		 * prefix so the client gets a smaller-but-usable layout
		 * instead of NFS4ERR_LAYOUTTRYLATER.  prefix == 0 keeps
		 * the legacy TRYLATER behaviour. */
		uint64_t _slt_prefix = stripe_lease_prefix_conflict_free_length(
			cd->slt,
			_slt_slices, (uint32_t)_slt_n,
			_slt_unit,
			lease_offset, lease_length,
			cd->current_fh.fileid, cd->clientid);
		if (_slt_prefix == 0) {
			return NFS4ERR_LAYOUTTRYLATER;
		}
		if (_slt_prefix < lease_length) {
			lease_length = _slt_prefix;
			if (grant_length > _slt_prefix) {
				grant_length = _slt_prefix;
			}
		}
	}

	/*
	 * If the CQ fused a layout pregrant onto the preceding
	 * OPEN(CREATE), consume it now -- before path branching.
	 * Scope is this single compound; layout_pregranted is cleared
	 * so compound_process skips revoke_unused_pregrant on any
	 * error return below.  Individual paths reuse
	 * pregrant_consumed to avoid re-issuing mds_coord_layout_grant
	 * for a stateid that is already either persisted (by the CQ)
	 * or intentionally in-memory-only (skip_transient_ndb).
	 */
	bool pregrant_consumed = false;
	struct nfs4_stateid pregrant_sid = { 0 };

	if (cd->layout_pregranted &&
	    cd->layout_pregrant_fileid == cd->current_fh.fileid) {
		pregrant_sid = cd->layout_pregrant_stateid;
		pregrant_consumed = true;
		cd->layout_pregranted = false;
	}

	/* DS_PENDING lifecycle (Phase 5A/5B unified model).
	 *
	 * DS_PENDING means the stripe map exists, but at least one
	 * async-needed DS entry is not yet ready for direct client I/O.
	 * If async prepare is queued or in progress, report temporary
	 * unavailability. Otherwise fall back to the synchronous path
	 * below and clear the flag only after a successful grant.
	 */
	if (inode.flags & MDS_IFLAG_DS_PENDING) {
		clear_ds_pending_on_success = true;
		MDS_TIME_CAT_OP(MDS_CATOP_DS_PREPARE_CHECK,
			prep_state = compound_ds_prepare_check(
				cd, cd->current_fh.fileid));
		if (prep_state == -1 && cd->ds_prepare != NULL) {
			compound_maybe_enqueue_ds_prepare(cd, &inode);
			/* Re-check after enqueue; result consumed
			 * only for side-effect (queue priming). */
			MDS_TIME_CAT_OP(MDS_CATOP_DS_PREPARE_CHECK,
				(void)compound_ds_prepare_check(
					cd, cd->current_fh.fileid));
		}
		/* Fall through to the LAYOUTGET path.  FH capture
		 * happens during placement (new files only).
		 * Returning LAYOUTUNAVAILABLE here would cause the
		 * Linux kernel client to cache the failure
		 * permanently and never retry pNFS for this file. */
	}

	/*
	 * Phase D of docs/hpc-nto1-plan.md -- HPC-Shared layout cache
	 * fast path.
	 *
	 * For inodes with MDS_IFLAG_HPC_SHARED, look the stripe map up
	 * in the per-MDS layout cache before touching the catalogue.
	 * On hit we still mint a fresh per-client layout stateid (v1
	 * keeps per-client stateids -- no shared stateid until a
	 * future revision pairs it with the recall-path skip), but we
	 * skip the cat_stripe_map_get / RonDB-fused read and the
	 * placement fallback entirely.
	 *
	 * The cache is only consulted when DS_PENDING is clear: a
	 * file in the DS_PENDING window has no FH-ready entries to
	 * serve, and falling through to the existing path keeps the
	 * ds_prepare hand-off coherent.
	 *
	 * Plain (non-HPC) inodes never touch the cache, keeping the
	 * legacy LAYOUTGET path bit-for-bit identical for the common
	 * workload.
	 */
	bool layout_cache_was_hit = false;
	const bool inode_is_hpc_shared =
		(inode.flags & MDS_IFLAG_HPC_SHARED) != 0;

	if (inode_is_hpc_shared && cd->lcache != NULL &&
	    !(inode.flags & MDS_IFLAG_DS_PENDING)) {
		struct mds_ds_map_entry *cached_entries = NULL;
		uint32_t c_sc = 0;
		uint32_t c_su = 0;
		uint32_t c_mc = 0;

		if (layout_cache_get(cd->lcache,
				     cd->current_fh.fileid,
				     &c_sc, &c_su, &c_mc,
				     &cached_entries) == 0) {
			struct nfs4_stateid layout_sid;

			stripe_count = c_sc;
			stripe_unit  = c_su;
			mirror_count = c_mc;
			entries      = cached_entries;

			if (pregrant_consumed) {
				layout_sid = pregrant_sid;
			} else {
				layout_pick_stateid(cd, &client_sid,
						    &layout_sid);
				if (!cd->skip_transient_ndb &&
				    cd->cat != NULL) {
					uint32_t ds_total = stripe_count *
							    mirror_count;
					struct layout_ds_id_list ds_ids = { 0 };
					enum nfs4_status ds_nst;

					ds_nst = layout_make_ds_list(
						entries, ds_total, &ds_ids);
					if (ds_nst != NFS4_OK) {
						free(entries);
						return ds_nst;
					}
					(void)mds_coord_layout_grant(
						cd->cat, NULL,
						cd->clientid,
						cd->current_fh.fileid,
						grant_iomode,
						grant_offset,
						grant_length,
						&layout_sid,
						ds_ids.ids, ds_ids.count);
					layout_ds_id_list_destroy(&ds_ids);
				}
			}
			r->stateid = layout_sid;
			layout_cache_was_hit = true;
			goto fill_layoutget_result;
		}
	}

	if (layout_state_is_root_global(cd)) {
		/*
		 * Keep the root-global layout-state path separate
		 * map reads/writes stay in the namespace shard while layout_state and
		 * ds_layout_idx stay in root_db/root_cq. This intentionally duplicates
		 * some of the single-shard LAYOUTGET flow; refactor once the split-db
		 * plumbing is centralized.
		 */
		/* Fast path: check if this file's stripe was just created
		 * in the same compound (stashed by op_open CREATE).
		 * Eliminates 2 NDB reads from LAYOUTGET for new files. */
		if (cd->stripe_cached &&
		    cd->stripe_cached_fileid == cd->current_fh.fileid &&
		    cd->stripe_cached_ds_id != 0) {
			struct nfs4_stateid fast_sid;

			stripe_count = 1;
			mirror_count = 1;
			stripe_unit = cd->stripe_cached_stripe_unit > 0
				    ? cd->stripe_cached_stripe_unit : 65536;

			entries = calloc(1, sizeof(*entries));
			if (entries != NULL) {
				entries[0].ds_id = cd->stripe_cached_ds_id;
				/*
				 * Populate the captured NFS file handle when
				 * op_open's fused create surfaced one
				 * (Fix 3 of the May-13 perf pass).  With a
				 * real FH in place, layout_entries_ready_for_grant
				 * below returns true and we both (a) skip the
				 * cat_stripe_map_get NDB round-trip the slow
				 * path would issue and (b) avoid the
				 * layout_revoke_unready_grant DELAY round-trip
				 * the legacy DS_PENDING path takes.  When no FH
				 * was captured (community / proxy-less build)
				 * the cache leaves nfs_fh_len at 0 and we fall
				 * through to that legacy DS_PENDING flow.
				 */
				if (cd->stripe_cached_nfs_fh_len > 0 &&
				    cd->stripe_cached_nfs_fh_len <=
					sizeof(entries[0].nfs_fh)) {
					entries[0].nfs_fh_len =
						cd->stripe_cached_nfs_fh_len;
					memcpy(entries[0].nfs_fh,
					       cd->stripe_cached_nfs_fh,
					       cd->stripe_cached_nfs_fh_len);
				} else {
					entries[0].nfs_fh_len = 0; /* FH pending */
				}

				/*
				 * Reuse the CQ pregrant stateid if OPEN(CREATE)
				 * produced one for this fileid in this compound.
				 * Its consumption above cleared layout_pregranted
				 * so compound_process skips
				 * revoke_unused_pregrant on any subsequent error
				 * return -- the previously observed fused
				 * regression (see
				 * docs/design-post-phase3-candidates.md).
				 */
				if (pregrant_consumed) {
					fast_sid = pregrant_sid;
				} else {
					/* Write layout_state (skip for single-MDS). */
					layout_pick_stateid(cd, &client_sid,
							    &fast_sid);
					if (!cd->skip_transient_ndb) {
						(void)mds_coord_layout_grant(
							cd->cat, NULL,
							cd->clientid,
							cd->current_fh.fileid,
							grant_iomode, grant_offset,
							grant_length, &fast_sid,
							&cd->stripe_cached_ds_id,
							1);
					}
				}
				r->stateid = fast_sid;

				cd->stripe_cached = false;

				/* Phase 12 Component A: no sync FH capture on
				 * the LAYOUTGET hot path.  If the pre-captured
				 * FH is missing, route through the ds_prepare
				 * async capture + NFS4ERR_DELAY helper.  See
				 * docs/design-layoutget-decoupling.md S4.1. */
				if (!layout_entries_ready_for_grant(
					    cd, entries,
					    stripe_count * mirror_count)) {
					enum nfs4_status ds;

					MDS_TIME_CAT_OP(
						MDS_CATOP_LAYOUT_REVOKE_GRANT,
						ds = layout_revoke_unready_grant(
							cd, &fast_sid, entries,
							stripe_count * mirror_count));
					free(entries);
					return ds;
				}
				if (clear_ds_pending_on_success) {
					layout_clear_ds_pending(
						cd, cd->current_fh.fileid);
				}
				goto fill_layoutget_result;
			}
		}

		/* Phase 2: Use fused stripe_get + layout_grant when
		 * RonDB backend is active.  Saves 1 NDB round-trip. */
#ifdef HAVE_RONDB
		if (cd->cat != NULL &&
		    mds_catalogue_backend_type(cd->cat) == MDS_BACKEND_RONDB &&
		    !cd->skip_transient_ndb) {
			struct nfs4_stateid fused_sid;
			layout_pick_stateid(cd, &client_sid, &fused_sid);

		/* Direct-from-compound call: not in the catalogue vtable
		 * (CAT_TIMED only wraps vtable dispatch), so we time it
		 * inline.  Skipped cleanly when observability is off. */
		{
			bool _t = mds_op_metrics_enabled();
			uint64_t _t0 = 0;

			if (_t) {
				_t0 = mds_op_metrics_now_ns();
				mds_phase_enter(MDS_PHASE_CATALOGUE);
			}
			st = catalogue_rondb_layoutget_fused(
				cd->cat, cd->current_fh.fileid,
				&stripe_count, &stripe_unit,
				&mirror_count, &entries,
				&fused_sid, cd->clientid,
				grant_iomode, grant_offset, grant_length,
				cd->mds_id);
			if (_t) {
				mds_phase_leave();
				mds_cat_op_observe(
					MDS_CATOP_LAYOUTGET_FUSED,
					mds_op_metrics_now_ns() - _t0);
			}
		}

		if (st == MDS_OK) {
			/* Layout grant already persisted in the fused txn. */
			r->stateid = fused_sid;

			/* No backfill -- if FH is missing, return
			 * LAYOUTUNAVAILABLE and let client use proxy I/O. */
		if (!layout_entries_ready_for_grant(
			    cd, entries,
			    stripe_count * mirror_count)) {
			nst = layout_revoke_grant_entries(
				cd, &fused_sid, entries,
				stripe_count * mirror_count);
			free(entries);
			if (nst != NFS4_OK) {
				return nst;
			}
			return NFS4ERR_LAYOUTUNAVAILABLE;
		}
			if (clear_ds_pending_on_success) {
				layout_clear_ds_pending(
					cd, cd->current_fh.fileid);
			}
			goto fill_layoutget_result;
		}
		/* Fused path failed.  Fall back to non-fused
		 * (separate txns) for any transient or non-fatal error. */
		if (st == MDS_ERR_DELAY || st == MDS_ERR_IO ||
		    st == MDS_ERR_INVAL) {
			st = cat_stripe_map_get(
				cd, cd->current_fh.fileid,
				&stripe_count, &stripe_unit,
				&mirror_count, &entries);
			/* continue to normal path below */
		} else if (st != MDS_ERR_NOTFOUND) {
			/* NOTFOUND: fall through to placement. */
			return mds_status_to_nfs4(st);
		}
		} else
#endif /* HAVE_RONDB */
		{
			st = cat_stripe_map_get(cd, cd->current_fh.fileid,
						     &stripe_count, &stripe_unit,
						     &mirror_count, &entries);
		}
		if (st == MDS_ERR_NOTFOUND) {
			st = cat_ds_list(cd,
						       &ds_list, &ds_count);
			if (st != MDS_OK || ds_count == 0) {
				free(ds_list);
				return NFS4ERR_NOSPC;
			}

			{
				struct mds_ds_info *compat = NULL;
				uint32_t compat_count = 0;

				st = layout_filter_for_inode(
					&inode,
					ds_list, ds_count,
					DS_MODE_GENERIC,
					(a->layout_type == LAYOUT4_FLEX_FILES)
						? ff_policy_transport(cd->ff_policy)
						: DS_TRANSPORT_TCP,
					&compat, &compat_count);
				free(ds_list);
				ds_list = compat;
				ds_count = compat_count;
				if (st != MDS_OK || ds_count == 0) {
					free(ds_list);
					return NFS4ERR_NOSPC;
				}
			}

			/*
			 * Phase 3: honour the daemon's configured default
			 * stripe/mirror geometry when the placement policy
			 * dispatcher is active.  Both fields default to 1
			 * in Phase 1/1.1, so flag-on + default profile still
			 * produces the single-DS layout that pre-Phase-3
			 * builds shipped.  A profile or explicit INI key
			 * (default_stripe_count / default_mirror_count) opts
			 * a deployment into multi-DS striping.  Flag-off keeps
			 * the legacy 1/1 hardcode and the legacy RR path
			 * below.
			 */
			if (cd->cfg_placement_policy_enabled &&
			    cd->cfg_default_stripe_count > 0 &&
			    cd->cfg_default_mirror_count > 0) {
				stripe_count = cd->cfg_default_stripe_count;
				mirror_count = cd->cfg_default_mirror_count;
			} else {
				stripe_count = 1;
				mirror_count = 1;
			}
			stripe_unit = (cd->cfg_stripe_unit > 0)
				    ? cd->cfg_stripe_unit : 65536;

			/* NOLINTNEXTLINE(bugprone-implicit-widening-of-multiplication-result) */
			entries = calloc(stripe_count * mirror_count,
					 sizeof(*entries));
			if (entries == NULL) {
				free(ds_list);
				return NFS4ERR_RESOURCE;
			}

			/*
			 * Phase 1: policy dispatcher.  When
			 * cfg_placement_policy_enabled is true we call
			 * placement_select_ex with the daemon's effective
			 * policy; otherwise the legacy placement_select
			 * (plain round-robin) runs unchanged so the
			 * no-op path is bit-identical to pre-feature
			 * behaviour.
			 */
			if (cd->cfg_placement_policy_enabled) {
				/* Phase A WRR: overlay operator-configured
				 * weights from the DS cache so the WRR
				 * dispatcher does not collapse to uniform on
				 * 3rd-party DSes that never populate
				 * total_bytes / used_bytes. */
				if (cd->ds_cache != NULL) {
					ds_cache_overlay_weights(
						cd->ds_cache,
						ds_list, ds_count);
				}
				st = placement_select_ex(
					cd->cfg_placement_policy,
					ds_list, ds_count,
					stripe_count, mirror_count,
					stripe_unit, entries);
			} else {
				st = placement_select(ds_list, ds_count,
					      stripe_count, mirror_count,
					      stripe_unit, entries);
			}
			free(ds_list);
			ds_list = NULL;
			if (st != MDS_OK) {
				free(entries);
				return NFS4ERR_NOSPC;
			}

			/* Phase 12 Component A: no sync FH capture on the
			 * LAYOUTGET hot path.  Placement produces entries
			 * with nfs_fh_len == 0; the post-grant check below
			 * routes through layout_revoke_unready_grant, which
			 * enqueues ds_prepare and returns NFS4ERR_DELAY.
			 * The variable 'i' is no longer used in this branch
			 * but remains declared at the function scope. */
			(void)i;

			/* Persist newly created stripe map via catalogue. */
			if (cd->cat == NULL) {
				free(entries);
				free(ds_list);
				return NFS4ERR_IO;
			}
			st = mds_cat_stripe_map_put(cd->cat, NULL,
				cd->current_fh.fileid,
				stripe_count, stripe_unit,
				mirror_count, entries);
			if (st != MDS_OK) {
				free(entries);
				return NFS4ERR_IO;
			}
		} else if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}

		/* Phase 12 Component A: if entries still have unready
		 * FHs (prealloc pool was empty at CREATE, or legacy
		 * stripe_map), enqueue async capture + return DELAY
		 * instead of LAYOUTUNAVAILABLE.  Done inline because
		 * no grant has been issued yet -- no layout_return to
		 * clean up. */
		if (!layout_entries_ready_for_grant(
			    cd, entries, stripe_count * mirror_count)) {
			struct mds_inode pend_inode;
			enum mds_status gst;

			gst = cat_getattr(cd, cd->current_fh.fileid,
					  &pend_inode);
			if (gst == MDS_OK) {
				if (!(pend_inode.flags & MDS_IFLAG_DS_PENDING)) {
					pend_inode.flags |= MDS_IFLAG_DS_PENDING;
					(void)cat_setattr(cd,
						cd->current_fh.fileid,
						&pend_inode, MDS_ATTR_FLAGS);
					compound_inode_invalidate(
						cd, cd->current_fh.fileid);
				}
				compound_maybe_enqueue_ds_prepare(
					cd, &pend_inode);
			}
			atomic_fetch_add_explicit(
				&g_branch_metrics.layoutget_delay_count,
				1, memory_order_relaxed);
			free(entries);
			return NFS4ERR_DELAY;
		}

		/* Persist layout_state (skip for single-MDS). */
		if (cd->cat != NULL) {
			struct nfs4_stateid layout_sid;
			uint32_t ds_total = stripe_count * mirror_count;
			struct layout_ds_id_list ds_ids = { 0 };

			if (pregrant_consumed) {
				layout_sid = pregrant_sid;
			} else {
				layout_pick_stateid(cd, &client_sid,
						    &layout_sid);
				if (!cd->skip_transient_ndb) {
					nst = layout_make_ds_list(
						entries, ds_total, &ds_ids);
					if (nst != NFS4_OK) {
						free(entries);
						return nst;
					}
					(void)mds_coord_layout_grant(
						cd->cat, NULL,
						cd->clientid,
						cd->current_fh.fileid,
						grant_iomode, grant_offset,
						grant_length,
						&layout_sid,
						ds_ids.ids, ds_ids.count);
					layout_ds_id_list_destroy(&ds_ids);
				}
			}
			r->stateid = layout_sid;
		} else {
			free(entries);
			return NFS4ERR_IO;
		}

		if (clear_ds_pending_on_success) {
			layout_clear_ds_pending(
				cd, cd->current_fh.fileid);
		}

		goto fill_layoutget_result;
	}

	/*
	 * Catalogue-only path: stripe map via cat_stripe_map_get,
	 * layout state via mds_coord_layout_grant.
	 */
	{
		st = cat_stripe_map_get(cd, cd->current_fh.fileid,
					&stripe_count, &stripe_unit,
					&mirror_count, &entries);
		if (st == MDS_ERR_NOTFOUND) {
			/* Need placement -- read DS registry. */
			st = cat_ds_list(cd, &ds_list, &ds_count);
			if (st != MDS_OK || ds_count == 0) {
				free(ds_list);
				return NFS4ERR_NOSPC;
			}

			{
				struct mds_ds_info *compat = NULL;
				uint32_t compat_count = 0;
				st = layout_filter_for_inode(&inode,
					ds_list, ds_count,
					DS_MODE_GENERIC,
					(a->layout_type == LAYOUT4_FLEX_FILES)
						? ff_policy_transport(cd->ff_policy)
						: DS_TRANSPORT_TCP,
					&compat, &compat_count);
				free(ds_list);
				ds_list = compat;
				ds_count = compat_count;
				if (st != MDS_OK || ds_count == 0) {
					free(ds_list);
					return NFS4ERR_NOSPC;
				}
			}

			stripe_count = 1;
			mirror_count = 1;
			stripe_unit = (cd->cfg_stripe_unit > 0)
				    ? cd->cfg_stripe_unit : 65536;

			/* NOLINTNEXTLINE(bugprone-implicit-widening-of-multiplication-result) */
			entries = calloc(stripe_count * mirror_count,
					 sizeof(*entries));
			if (entries == NULL) {
				free(ds_list);
				return NFS4ERR_RESOURCE;
			}

			st = placement_select(ds_list, ds_count,
					      stripe_count, mirror_count,
					      stripe_unit, entries);
			free(ds_list);
			ds_list = NULL;
			if (st != MDS_OK) {
				free(entries);
				return NFS4ERR_NOSPC;
			}

			/* Phase 12 Component A: no sync FH capture.  The
			 * post-grant check returns NFS4ERR_DELAY when
			 * entries have nfs_fh_len == 0.  See
			 * docs/design-layoutget-decoupling.md S4.1. */
			(void)i;

			/* Persist new stripe map via catalogue. */
			if (cd->cat == NULL) {
				free(entries);
				return NFS4ERR_IO;
			}
			st = mds_cat_stripe_map_put(cd->cat, NULL,
				cd->current_fh.fileid,
				stripe_count, stripe_unit,
				mirror_count, entries);
			if (st != MDS_OK) {
				free(entries);
				return NFS4ERR_IO;
			}
		} else if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}

		/* Check pre-grant from fused CREATE+LAYOUTGET. */
		if (pregrant_consumed) {
			r->stateid = pregrant_sid;
		} else {
			/* Persist layout state (skip for single-MDS). */
			struct nfs4_stateid layout_sid;
			uint32_t ds_total = stripe_count * mirror_count;
			struct layout_ds_id_list ds_ids = { 0 };

			if (cd->cat == NULL) {
				free(entries);
				return NFS4ERR_IO;
			}
			layout_pick_stateid(cd, &client_sid, &layout_sid);
			if (!cd->skip_transient_ndb) {
				nst = layout_make_ds_list(
					entries, ds_total, &ds_ids);
				if (nst != NFS4_OK) {
					free(entries);
					return nst;
				}
				(void)mds_coord_layout_grant(
					cd->cat, NULL,
					cd->clientid, cd->current_fh.fileid,
					grant_iomode, grant_offset, grant_length,
					&layout_sid,
					ds_ids.ids, ds_ids.count);
				layout_ds_id_list_destroy(&ds_ids);
			}
			r->stateid = layout_sid;
		}
	}

	/*
	 * NOTE (Issue #6): Existing files retain their original stripe
	 * geometry.  Adding DSs to the cluster does not re-stripe
	 * already-created files.  Stripe expansion would require data
	 * migration and is not supported -- document as a limitation.
	 */

	/* No backfill -- LAYOUTGET must never block on network I/O.
	 * If entries have nfs_fh_len == 0 (legacy or failed capture),
	 * return LAYOUTUNAVAILABLE.  The client falls back to proxy
	 * I/O through the MDS for these files. */
	if (!layout_entries_ready_for_grant(
		    cd, entries, stripe_count * mirror_count)) {
		enum nfs4_status revoke_status;

		MDS_TIME_CAT_OP(MDS_CATOP_LAYOUT_REVOKE_GRANT,
			revoke_status = layout_revoke_unready_grant(
				cd, &r->stateid, entries,
				stripe_count * mirror_count));
		free(entries);
		return revoke_status;
	}
	if (clear_ds_pending_on_success &&
	    layout_entries_ready_for_grant(
		    cd, entries, stripe_count * mirror_count)) {
		layout_clear_ds_pending(cd, cd->current_fh.fileid);
	}

fill_layoutget_result:
	/* Phase D of docs/hpc-nto1-plan.md -- populate the HPC layout
	 * cache once per LAYOUTGET that produced a fresh stripe map.
	 * Skipped when the path was a cache hit (the entry is already
	 * present and this would be a redundant copy).  All branches
	 * that reach this label have entries[] verified FH-ready by
	 * their own (per-branch) layout_entries_ready_for_grant
	 * checks, so the cached snapshot is always servable. */
	if (inode_is_hpc_shared && cd->lcache != NULL &&
	    !layout_cache_was_hit && entries != NULL &&
	    stripe_count > 0 && mirror_count > 0) {
		(void)layout_cache_put(cd->lcache,
				       cd->current_fh.fileid,
				       stripe_count, stripe_unit,
				       mirror_count, entries);
	}
	/* Fill result.
	 *
	 * Advertise the persisted long-lived grant to the client:
	 *   - iomode = RW  (server may widen per RFC 8881 S12.5.3)
	 *   - offset/length = bounded grant selected above
	 *   - return_on_close = false  (client keeps the layout across
	 *     close; returned only on explicit LAYOUTRETURN or when
	 *     the server CB_RECALLs it)
	 *
	 * The widened (iomode, offset, length) triple matches what the
	 * layout_state rows in NDB hold (see grant_* locals above), so
	 * the client's stored stateid is coherent with the server's
	 * authoritative record.
	 */
	r->return_on_close = false;
	r->layout_type = a->layout_type;
	r->iomode = grant_iomode;
	r->offset = grant_offset;
	r->length = grant_length;
	r->stripe_unit = stripe_unit;

	/* Phase E + E+ of docs/hpc-nto1-plan.md -- byte-range stripe
	 * subsetting with stripe-aligned boundaries.
	 *
	 * If the client asked for a narrow byte range (length !=
	 * UINT64_MAX) and we have a real stripe geometry, restrict the
	 * returned DS list to the stripes that actually cover it.  The
	 * authoritative grant range stays whole-file (grant_offset /
	 * grant_length above) -- we only narrow what we emit on the
	 * wire so the client's per-DS dispatch table is stripe-aligned
	 * and proportional to the request.
	 *
	 * Selection:
	 *   start_stripe = floor(offset / stripe_unit)
	 *   end_stripe   = floor((offset + length - 1) / stripe_unit)
	 *   clamped to [0, stripe_count - 1]
	 *
	 * The returned (offset, length) pair stays the granted range:
	 * RFC 8881 S12.5.3 lets the server widen, and Phase D's per-
	 * inode layout cache will rely on the cached layout being
	 * authoritative across narrowed requests.  Only ds[] is
	 * trimmed.  ffl_stripe_unit (== r->stripe_unit) tells the
	 * Linux 6.18+ flex-files client which entry covers which byte;
	 * see RFC 8435 S5.1 and the kernel `dss_id = offset /
	 * stripe_unit` dispatcher.
	 *
	 * Single-stripe layouts skip the trim entirely -- the legacy
	 * stripe_count=1 case stays bit-for-bit identical. */
	uint32_t emit_start_stripe = 0;
	uint32_t emit_end_stripe   = (stripe_count > 0)
		? (stripe_count - 1) : 0;
	if (stripe_count > 1 && stripe_unit > 0 &&
	    a->length != UINT64_MAX) {
		/* Guard against zero-length requests (no stripes
		 * cover them; emit just the start stripe so the
		 * client has somewhere to send the eventual write). */
		if (a->length == 0) {
			emit_start_stripe = (uint32_t)(a->offset /
						       stripe_unit);
			emit_end_stripe   = emit_start_stripe;
		} else {
			uint64_t end_byte;

			emit_start_stripe = (uint32_t)(a->offset /
						       stripe_unit);
			if (a->offset > UINT64_MAX - (a->length - 1)) {
				end_byte = UINT64_MAX;
			} else {
				end_byte = a->offset + a->length - 1;
			}
			emit_end_stripe = (uint32_t)(end_byte / stripe_unit);
		}
		if (emit_start_stripe >= stripe_count) {
			emit_start_stripe = stripe_count - 1;
		}
		if (emit_end_stripe >= stripe_count) {
			emit_end_stripe = stripe_count - 1;
		}
	}

	uint32_t emit_stripes = (emit_end_stripe - emit_start_stripe) + 1;
	uint32_t want_ds_count = emit_stripes * mirror_count;
	/* No clamp: the wire-path arrays are heap-allocated by
	 * nfs4_res_layoutget_alloc() below and bounded only by
	 * MDS_MAX_STRIPES * MDS_MAX_MIRRORS.  The legacy 16-DS clamp
	 * was a leftover from the value-type result struct and silently
	 * truncated 128 / 1024-stripe HPC layouts on the wire. */

	/* The flex-files form populated below depends on ff_xdr_form:
	 *
	 *   FF_XDR_FORM_LEGACY  -- N ff_mirror4 entries, each with
	 *                         ds_count = mirror_count.  Pre-6.18
	 *                         Linux flex-files clients consume
	 *                         this shape.
	 *   FF_XDR_FORM_STRIPED -- 1 ff_mirror4 entry whose ds_count
	 *                         == stripe_count.  Linux 6.18+
	 *                         clients consume this as the per-
	 *                         stripe DS dispatch table (RFC 8435
	 *                         S5.1).
	 *
	 * The actual selection of ff_xdr_form is done after stripe
	 * geometry is known but before nfs4_res_layoutget_alloc(); see
	 * the assignment to r->ff_xdr_form a few lines below. */
	uint32_t want_ff_mirror_count = 0;
	if (r->layout_type == LAYOUT4_FLEX_FILES) {
		/* Provisional: assume legacy form.  Re-set to 1 below
		 * if the auto/explicit selector picks STRIPED. */
		want_ff_mirror_count = stripe_count * mirror_count;
	}

	/* Phase C / Step 6 of docs/hpc-nto1-plan.md -- ff_xdr_form
	 * selector.  Plain (non-HPC) inodes always emit the legacy
	 * one-DS-per-mirror form so existing clients see bit-for-bit
	 * identical wire output.  HPC-Shared inodes consult
	 * cd->cfg_hpc_xdr_form:
	 *
	 *   AUTO     -- striped iff mirror_count == 1 && stripe_count
	 *              > 1 (the only shape RFC 8435 S5.1 lets us emit
	 *              for multi-DS-per-mirror without violating the
	 *              one-mirror-per-replica constraint).
	 *   STRIPED  -- force striped (lab / 6.18+-only fleets).
	 *   LEGACY   -- force legacy (mixed-kernel fleets).
	 */
	r->ff_xdr_form = NFS4_FF_XDR_FORM_LEGACY;
	if (r->layout_type == LAYOUT4_FLEX_FILES && inode_is_hpc_shared) {
		bool pick_striped = false;
		switch (cd->cfg_hpc_xdr_form) {
		case MDS_HPC_XDR_FORM_STRIPED:
			pick_striped = true;
			break;
		case MDS_HPC_XDR_FORM_LEGACY:
			pick_striped = false;
			break;
		case MDS_HPC_XDR_FORM_AUTO:
		default:
			pick_striped = (mirror_count == 1 &&
					stripe_count > 1);
			break;
		}
		if (pick_striped) {
			r->ff_xdr_form = NFS4_FF_XDR_FORM_STRIPED;
			/* Striped form: exactly one ff_mirror4 carrying
			 * stripe_count DSes. */
			want_ff_mirror_count = 1;
		}
	}
	/* QA review Blocker 1 -- honor loga_maxcount per RFC 8881 S18.43.4.
	 *
	 * Estimate the encoded LAYOUTGET reply size from the geometry we
	 * are about to emit.  If it would exceed a->maxcount, return
	 * NFS4ERR_TOOSMALL so the client can retry with a larger
	 * maxcount instead of receiving a truncated body.
	 *
	 * Outer wrapper bytes (constant): return_on_close (4) +
	 * stateid (16) + layout_count (4) + layout4 header
	 * (offset 8 + length 8 + iomode 4 + type 4) + body_len (4) = 52.
	 *
	 * Body bytes (worst case, derived from xdr_ops_layout.c):
	 *   files layout : 64 + want_ds_count * (4 + MDS_NFS_FH_MAX + 4)
	 *   flex-files   : 64 + 4 * want_ff_mirror_count
	 *                  + worst_ds_count * FF_DATA_SERVER4_MAX_BYTES
	 *                  where FF_DATA_SERVER4_MAX_BYTES = 256.
	 *
	 * We over-estimate to leave headroom for GSS wrapping / outer
	 * compound framing, since the same client maxcount budget has to
	 * cover the entire RPC reply.  An over-estimate that triggers
	 * NFS4ERR_TOOSMALL on a marginal layout is safe: the client
	 * retries with a bumped budget, and Linux clients send
	 * maxcount=0xFFFFFFFF in practice so the path is rarely taken. */
	if (a->maxcount > 0 && a->maxcount < UINT32_MAX) {
		uint64_t outer_bytes = 52;
		uint64_t body_bytes;
		if (r->layout_type == LAYOUT4_FLEX_FILES) {
			/* Striped form (want_ff_mirror_count == 1) and legacy
			 * form both place want_ds_count DS entries on the wire
			 * -- striped puts them inside one ff_mirror4, legacy
			 * spreads them across one DS per mirror.  Either way
			 * the body cost scales with want_ds_count. */
			body_bytes = 64
				   + 4ULL * want_ff_mirror_count
				   + (uint64_t)want_ds_count * 256ULL;
		} else {
			body_bytes = 64
				   + (uint64_t)want_ds_count *
				     (4 + MDS_NFS_FH_MAX + 4);
		}
		uint64_t estimated = outer_bytes + body_bytes;
		if (estimated > (uint64_t)a->maxcount) {
			nst = layout_revoke_grant_entries(
				cd, &r->stateid, entries,
				stripe_count * mirror_count);
			free(entries);
			if (nst != NFS4_OK) {
				return nst;
			}
			return NFS4ERR_TOOSMALL;
		}
	}

	if (nfs4_res_layoutget_alloc(r, want_ds_count,
				      want_ff_mirror_count) != 0) {
		nst = layout_revoke_grant_entries(
			cd, &r->stateid, entries,
			stripe_count * mirror_count);
		free(entries);
		if (nst != NFS4_OK) {
			return nst;
		}
		return NFS4ERR_RESOURCE;
	}

	/* Defensive: mirror_count == 0 would make the divisions below
	 * undefined (clang-tidy clang-analyzer-core.DivideZero).  In
	 * practice the catalogue never persists a stripe map with
	 * mirror_count == 0; if it ever does, treat the layout as
	 * unavailable so the client retries instead of getting a
	 * malformed grant. */
	if (mirror_count == 0) {
		free(entries);
		return NFS4ERR_LAYOUTUNAVAILABLE;
	}

	for (i = 0; i < r->ds_count; i++) {
		/* Map the i-th emitted slot back to the right entry in
		 * the (stripe, mirror) catalogue array.  Layout is
		 * row-major by stripe then mirror, matching the
		 * catalogue's serialisation order. */
		uint32_t emit_stripe = emit_start_stripe +
			(i / mirror_count);
		uint32_t emit_mirror = i % mirror_count;
		uint32_t src_idx     = emit_stripe * mirror_count +
			emit_mirror;

		r->ds[i].ds_id = entries[src_idx].ds_id;
		r->ds[i].nfs_fh_len = entries[src_idx].nfs_fh_len;
		if (entries[src_idx].nfs_fh_len > 0) {
			memcpy(r->ds[i].nfs_fh, entries[src_idx].nfs_fh,
			       entries[src_idx].nfs_fh_len);
		}
		build_deviceid(cd->mds_id, entries[src_idx].ds_id,
			       r->ds[i].deviceid);
	}

	/* Phase E+ runtime assert: any layout with more than one DS
	 * MUST advertise a non-zero stripe_unit, otherwise the Linux
	 * 6.18+ flex-files client cannot compute per-DS dispatch and
	 * will collapse back to a single-DS path.  This is a load-
	 * bearing field, not advisory; treat a violation as a server
	 * bug we want to catch in tests, not a wire condition. */
	if (r->ds_count > 1 && r->stripe_unit == 0) {
		MDS_LOG_INFO(LOG_COMP_NFS,
			"layoutget: BUG \u2014 dss_count=%u with stripe_unit=0; "
			"client will collapse to single-DS",
			r->ds_count);
	}

	/* Populate flex-files mirror model if flex layout requested.
	 * Two shapes per RFC 8435 S5.1:
	 *
	 *   STRIPED -- exactly one ff_mirror4 with ds_count == stripe_count.
	 *             The Linux 6.18+ flex-files client treats this as the
	 *             per-stripe DS dispatch table (dss_id =
	 *             offset / stripe_unit).  Used for HPC-Shared inodes
	 *             whose mirror_count == 1 && stripe_count > 1.
	 *
	 *   LEGACY  -- stripe_count * mirror_count ff_mirror4 entries, each
	 *             with ds_count == 1.  Pre-6.18 Linux clients consume
	 *             this as one DS per mirror per layout segment. */
	if (r->layout_type == LAYOUT4_FLEX_FILES) {
		r->ff_flags = 0;

		/* Stripe lease: acquire the in-memory lease only.
		 * The historical FF_FLAGS_STRIPE_LEASE wire flag and the
		 * trailing ffl_stripe_lease_duration_ms XDR field were a
		 * PEAK-only extension of RFC 8435 Flex Files v1; removed
		 * so the LAYOUTGET reply is bit-for-bit RFC compliant.
		 * Cross-client coordination still happens server-side
		 * via stripe_lease_check_conflict at LAYOUTGET entry. */
		if (cd->slt != NULL &&
		    cd->cfg_stripe_lease_duration_ms > 0 &&
		    (inode.flags & MDS_IFLAG_HPC_SHARED) != 0) {
			/* Patch 0006: HPC-gated.  Only HPC_SHARED files enter
			 * the SLT (matches the conflict-check gate above).
			 *
			 * Patch 0005: per-DS-stripe lease keying.  Acquire one
			 * lease entry per stripe slice; best-effort -- the
			 * conflict check at LAYOUTGET entry already ruled out
			 * cross-client collisions. */
			struct stripe_slice _slt_slices[MDS_MAX_STRIPES];
			uint32_t _slt_unit = cd->cfg_stripe_unit
				? cd->cfg_stripe_unit : 65536U;
			int _slt_n = lease_range_to_stripe_slices(
				lease_offset, lease_length, _slt_unit,
				_slt_slices, MDS_MAX_STRIPES);
			for (int _slt_i = 0; _slt_i < _slt_n; _slt_i++) {
				(void)stripe_lease_acquire(
					cd->slt,
					cd->current_fh.fileid,
					cd->clientid,
					0U, /* ds_id: tracing only */
					_slt_slices[_slt_i].stripe_index,
					_slt_slices[_slt_i].ds_offset,
					_slt_slices[_slt_i].ds_length,
					cd->cfg_stripe_lease_duration_ms);
			}
		}

		/*
		 * RFC 8435 S5.1: ffl_user / ffl_group identify the user
		 * and group the DS must use to access the data on behalf
		 * of the client.  Pynfs FFLA1 (testFlexLayoutTestAccess)
		 * requires READ vs RW grants to advertise *different*
		 * uids and the *same* gid -- the canonical RFC 8435
		 * pattern is to use squash semantics for READ grants
		 * (uid 0 / nobody) and the AUTH_SYS caller's uid for RW
		 * grants, while group identity stays tied to the inode.
		 * Plain inodes pre-feature shipped with both fields = 0;
		 * keying off a->iomode preserves that for any client that
		 * never asks for a READ-only grant. */
		const uint32_t ffl_user_value =
			(a->iomode == LAYOUTIOMODE4_READ)
				? cd->cred_uid
				: 0U;
		const uint32_t ffl_group_value = (uint32_t)inode.gid;

		/*
		 * Align DS backing-file ownership with the credentials
		 * we are about to advertise in ffl_user / ffl_group.
		 * Without this, files created by the MDS sit as
		 * root:root 0600 and the DS denies any READ that
		 * arrives with the caller's real uid (RFC 8435 S2.2.1
		 * loosely-coupled model).  Best-effort: a chown failure
		 * is logged but does not fail the layout grant -- the
		 * client will simply observe NFS4ERR_ACCESS on the DS
		 * READ/WRITE and retry, matching today's behaviour.
		 */
		if (cd->proxy != NULL && entries != NULL) {
			const uint32_t total =
				stripe_count * mirror_count;
			for (uint32_t idx = 0; idx < total; idx++) {
				const uint32_t stripe = idx / mirror_count;
				const uint32_t mirror = idx % mirror_count;
				enum mds_status own_st;

				own_st = mds_proxy_set_ds_owner_explicit(
					cd->proxy, entries[idx].ds_id,
					cd->current_fh.fileid,
					stripe, mirror,
					(uid_t)ffl_user_value,
					(gid_t)ffl_group_value);
				if (own_st != MDS_OK) {
					MDS_LOG_WARN(LOG_COMP_NFS,
						"layout chown failed "
						"ds=%u fid=%lu s=%u m=%u "
						"uid=%u gid=%u: %d",
						(unsigned)entries[idx].ds_id,
						(unsigned long)
						cd->current_fh.fileid,
						stripe, mirror,
						(unsigned)ffl_user_value,
						(unsigned)ffl_group_value,
						(int)own_st);
				}
			}
		}

		if (r->ff_xdr_form == NFS4_FF_XDR_FORM_STRIPED &&
		    r->ff_mirror_count == 1 && r->ds_count > 0) {
			/* Striped form: one ff_mirror4 carrying every DS
			 * already in r->ds[].  Allocation failure is
			 * treated as RESOURCE exhaustion; partial mirrors
			 * are freed by nfs4_res_layoutget_destroy() at
			 * compound teardown. */
			if (nfs4_ff_mirror_alloc(&r->ff_mirrors[0],
						 r->ds_count) != 0) {
				nst = layout_revoke_grant_entries(
					cd, &r->stateid, entries,
					stripe_count * mirror_count);
				free(entries);
				if (nst != NFS4_OK) {
					return nst;
				}
				return NFS4ERR_RESOURCE;
			}
			for (uint32_t k = 0; k < r->ds_count; k++) {
				r->ff_mirrors[0].ds[k].ds_id =
					r->ds[k].ds_id;
				memcpy(r->ff_mirrors[0].ds[k].deviceid,
				       r->ds[k].deviceid,
				       NFS4_DEVICEID4_SIZE);
				r->ff_mirrors[0].ds[k].efficiency = 0;
				r->ff_mirrors[0].ds[k].stateid =
					r->stateid;
				r->ff_mirrors[0].ds[k].nfs_fh_len =
					r->ds[k].nfs_fh_len;
				memcpy(r->ff_mirrors[0].ds[k].nfs_fh,
				       r->ds[k].nfs_fh,
				       r->ds[k].nfs_fh_len);
				r->ff_mirrors[0].ds[k].ffl_user =
					ffl_user_value;
				r->ff_mirrors[0].ds[k].ffl_group =
					ffl_group_value;
			}
		} else {
			/* Legacy one-DS-per-mirror form. */
			for (uint32_t m = 0; m < r->ff_mirror_count; m++) {
				if (nfs4_ff_mirror_alloc(&r->ff_mirrors[m],
							 1) != 0) {
					nst = layout_revoke_grant_entries(
						cd, &r->stateid, entries,
						stripe_count * mirror_count);
					free(entries);
					if (nst != NFS4_OK) {
						return nst;
					}
					return NFS4ERR_RESOURCE;
				}
				r->ff_mirrors[m].ds[0].ds_id =
					r->ds[m].ds_id;
				memcpy(r->ff_mirrors[m].ds[0].deviceid,
				       r->ds[m].deviceid,
				       NFS4_DEVICEID4_SIZE);
				r->ff_mirrors[m].ds[0].efficiency = 0;
				r->ff_mirrors[m].ds[0].stateid = r->stateid;
				r->ff_mirrors[m].ds[0].nfs_fh_len =
					r->ds[m].nfs_fh_len;
				memcpy(r->ff_mirrors[m].ds[0].nfs_fh,
				       r->ds[m].nfs_fh,
				       r->ds[m].nfs_fh_len);
				r->ff_mirrors[m].ds[0].ffl_user =
					ffl_user_value;
				r->ff_mirrors[m].ds[0].ffl_group =
					ffl_group_value;
			}
		}
	}

	free(entries);

	/* Fallback I/O signal for tiering (NULL = no-op). */
	io_tracker_bump(cd->io_tracker, cd->current_fh.fileid);

	return NFS4_OK;
}



enum nfs4_status op_getdeviceinfo(const struct compound_data *cd,
					 const struct nfs4_op *op,
					 struct nfs4_result *res)
{
	const struct nfs4_arg_getdeviceinfo *a = &op->arg.getdeviceinfo;
	struct nfs4_res_getdeviceinfo *r = &res->res.getdeviceinfo;
	uint32_t ds_id;
	struct mds_ds_info info;
	enum mds_status st;

	if (a->layout_type != LAYOUT4_NFSV4_1_FILES && a->layout_type != LAYOUT4_FLEX_FILES) {
		return NFS4ERR_NOTSUPP;
}

	ds_id = deviceid_to_ds_id(a->deviceid);
	st = cat_ds_get(cd, ds_id, &info);
	if (st == MDS_ERR_NOTFOUND) {
		return NFS4ERR_INVAL;
}
	if (st != MDS_OK) {
		return NFS4ERR_IO;
}

	/* Return a single-DS device address list. */
	r->layout_type = a->layout_type;
	r->ds_count = 1;
	r->ds[0].ds_id = info.ds_id;
	r->ds[0].port = info.port;
	(void)snprintf(r->ds[0].addr, sizeof(r->ds[0].addr), "%s", info.addr);

	/* Populate endpoint list from per-transport ports. */
	r->ds[0].endpoint_count = 0;
	{
		const char *wire_h = info.host[0] ? info.host : info.addr;

		if (info.tcp_port > 0) {
			uint32_t idx = r->ds[0].endpoint_count++;
			(void)snprintf(r->ds[0].endpoints[idx].netid,
				sizeof(r->ds[0].endpoints[idx].netid), "tcp");
			(void)snprintf(r->ds[0].endpoints[idx].host,
				sizeof(r->ds[0].endpoints[idx].host),
				"%s", wire_h);
			r->ds[0].endpoints[idx].port = info.tcp_port;
		}
		if (info.rdma_port > 0) {
			uint32_t idx = r->ds[0].endpoint_count++;
			(void)snprintf(r->ds[0].endpoints[idx].netid,
				sizeof(r->ds[0].endpoints[idx].netid), "rdma");
			(void)snprintf(r->ds[0].endpoints[idx].host,
				sizeof(r->ds[0].endpoints[idx].host),
				"%s", wire_h);
			r->ds[0].endpoints[idx].port = info.rdma_port;
		}
		if (r->ds[0].endpoint_count == 0 && info.port > 0) {
			r->ds[0].endpoint_count = 1;
			(void)snprintf(r->ds[0].endpoints[0].netid,
				sizeof(r->ds[0].endpoints[0].netid), "tcp");
			(void)snprintf(r->ds[0].endpoints[0].host,
				sizeof(r->ds[0].endpoints[0].host),
				"%s", wire_h);
			r->ds[0].endpoints[0].port = info.port;
		}
	}

	return NFS4_OK;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_layoutreturn(struct compound_data *cd,
					const struct nfs4_op *op,
					struct nfs4_result *res)
{
	const struct nfs4_arg_layoutreturn *a = &op->arg.layoutreturn;
	struct nfs4_res_layoutreturn *r = &res->res.layoutreturn;

	if (a->layout_type != LAYOUT4_NFSV4_1_FILES && a->layout_type != LAYOUT4_FLEX_FILES) {
		return NFS4ERR_NOTSUPP;
}

	if (a->return_type == LAYOUTRETURN4_FILE) {
		enum nfs4_status nst;
		uint32_t cur_seqid = 0;
		struct nfs4_stateid sid_resolved;

		nst = require_current_fh(cd);
		if (nst != NFS4_OK) {
			return nst;
		}

		/* RFC 8881 S16.2.4 -- resolve CURRENT_STATEID4 marker. */
		nst = compound_resolve_stateid(cd, &a->stateid,
					       &sid_resolved);
		if (nst != NFS4_OK) {
			return nst;
		}

		/* RFC 8881 S8.2.2 / S12.5.3: the server MUST validate
		 * the layout stateid's seqid on LAYOUTRETURN.  A seqid
		 * older than the latest issued is NFS4ERR_OLD_STATEID;
		 * a seqid newer than issued is NFS4ERR_BAD_STATEID.
		 *
		 * The probe lives in the in-memory tracker (the only
		 * source-of-truth under transient_state_cache=true).
		 * If the stateid is unknown, accept the LAYOUTRETURN
		 * -- the layout was already cleaned up or never
		 * issued by this daemon, so RFC 8881 S18.44.3's
		 * "SHOULD accept" applies.  Pynfs FFLOOS
		 * (testFlexLayoutOldSeqid). */
		if (layout_seqid_peek(sid_resolved.other, &cur_seqid)) {
			if (sid_resolved.seqid < cur_seqid) {
				return NFS4ERR_OLD_STATEID;
			}
			if (sid_resolved.seqid > cur_seqid) {
				return NFS4ERR_BAD_STATEID;
			}
		}

		/* Delete layout state.  Skip the stripe_map_get
		 * -- layout_return handles index cleanup with
		 * NULL ds_ids.  Saves 1 NDB read RT. */
		if (cd->cat != NULL) {
			(void)mds_coord_layout_return(
				cd->cat, NULL,
				sid_resolved.other,
				cd->clientid,
				cd->current_fh.fileid,
				NULL, 0);
		}
		/* Drop the in-memory seqid record so a subsequent
		 * LAYOUTGET/RETURN on the same `other` is treated as
		 * a brand-new layout, not a renewal. */
		layout_seqid_remove(sid_resolved.other);

		/* Release any stripe lease held by this client for
		 * the returned range (whole-file for FILE returns). */
		if (cd->slt != NULL) {
			/* Patch 0005: LAYOUTRETURN4_FILE -- whole-file return
			 * semantics don't need slice reconstruction. */
			stripe_lease_release_all_for(
				cd->slt,
				cd->current_fh.fileid,
				cd->clientid);
		}
	}
	r->stateid_present = true;
	memset(&r->stateid, 0, sizeof(r->stateid));
	return NFS4_OK;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_layoutcommit(struct compound_data *cd,
					const struct nfs4_op *op,
					struct nfs4_result *res)
{
	const struct nfs4_arg_layoutcommit *a = &op->arg.layoutcommit;
	struct nfs4_res_layoutcommit *r = &res->res.layoutcommit;
	struct nfs4_stateid sid_resolved;
	enum nfs4_status nst;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/* RFC 8881 S16.2.4 -- resolve CURRENT_STATEID4 marker. */
	nst = compound_resolve_stateid(cd, &a->stateid, &sid_resolved);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* Validate the committed layout stateid (RFC 8881 S18.6).
	 *
	 * Two modes:
	 *
	 *   transient_state_cache=off: consult NDB via
	 *     coord_layout_get_by_stateid (which today is an
	 *     unindexed `layout_state` scan by stateid_other --
	 *     ~9 ms p50 in the lab) and accept NOTFOUND as a
	 *     best-effort success.
	 *
	 *   transient_state_cache=on:  every `mds_coord_layout_grant`
	 *     call site is gated off in this mode, so the layout_state
	 *     table is always empty for our own grants -- the NDB
	 *     scan is guaranteed to return NOTFOUND and the daemon
	 *     would proceed regardless.  Skipping the scan removes
	 *     ~9 ms from every LAYOUTCOMMIT (~75% of its total
	 *     latency in the lab cluster).
	 *
	 * Same caveat as the LAYOUTGET recall gate above: this
	 * trade-off assumes a homogeneous fleet (all MDSes share
	 * the same flag).  Mixed-mode deployments would lose the
	 * cross-MDS validation when the local flag is on.
	 */
	if (!cd->skip_transient_ndb) {
		uint64_t lc_clientid = 0, lc_fileid = 0;
		enum mds_status lc_st;

		lc_st = coord_layout_get_by_stateid(
			cd, sid_resolved.other,
			&lc_clientid, &lc_fileid,
			NULL, NULL, NULL, NULL);
		if (lc_st == MDS_OK) {
			if (lc_clientid != cd->clientid ||
			    lc_fileid != cd->current_fh.fileid) {
				return NFS4ERR_BAD_STATEID;
			}
		} else if (lc_st != MDS_ERR_NOTFOUND) {
			return NFS4ERR_IO;
		}
		/* NOTFOUND: layout_grant was best-effort -- proceed. */
	}

	/* Single inode read for all LAYOUTCOMMIT checks. */
	{
		struct mds_inode lc_inode;
		enum mds_status st;

		st = compound_inode_get(cd, cd->current_fh.fileid,
					&lc_inode);
		if (st != MDS_OK) {
			return NFS4ERR_IO;
		}

		/* Phase F of docs/hpc-nto1-plan.md, integration part B --
		 * route HPC-Shared LAYOUTCOMMITs through the in-memory
		 * aggregator instead of writing size/mtime synchronously
		 * to NDB.  The aggregator's periodic timer flushes
		 * max(size) / latest(mtime) per fileid (default 200 ms),
		 * collapsing N concurrent writers into one NDB write per
		 * tick.  Quota accounting still happens upfront so a
		 * runaway writer can't exceed limits during the
		 * aggregation window.  DS_PENDING clearing stays
		 * synchronous because it's a flag, not a size/mtime
		 * field; the aggregator only handles those two. */
		bool use_aggregator = (cd->lcommit_agg != NULL &&
			(lc_inode.flags & MDS_IFLAG_HPC_SHARED) != 0);

		if (use_aggregator) {
			uint64_t new_size = lc_inode.size;
			struct timespec mtime;

			if (a->time_modify_set) {
				mtime = a->time_modify;
			} else {
				/* Server picks now() when client did not
				 * supply a value (RFC 8881 S18.6). */
				clock_gettime(CLOCK_REALTIME, &mtime);
			}

			if (a->new_offset) {
				if (a->last_write_offset == UINT64_MAX) {
					return NFS4ERR_INVAL;
				}
				new_size = a->last_write_offset + 1;

				if (new_size > lc_inode.size) {
					enum nfs4_status lc_nst;
					lc_nst = quota_check_bytes(
						cd, lc_inode.uid,
						lc_inode.gid,
						new_size - lc_inode.size);
					if (lc_nst != NFS4_OK) {
						return lc_nst;
					}
					quota_submit_adjust(cd,
						lc_inode.uid,
						lc_inode.gid,
						(int64_t)(new_size -
							lc_inode.size),
						0);
					r->new_size = true;
					r->new_size_value = new_size;
				}
			}

			/* Submit to the aggregator only when the client
			 * actually reported new state.  Skip pure-noop
			 * commits so the bucket stays clean for files
			 * the client hasn't grown or touched.
			 *
			 * QA Phase 4: a submit failure (OOM under capacity
			 * pressure with no flushable victim) MUST surface
			 * to the client -- silently dropping the size /
			 * mtime update would leave a writer's persisted
			 * file shorter than it actually wrote.  Returning
			 * NFS4ERR_DELAY tells the client to retry the
			 * LAYOUTCOMMIT after a short backoff; by then the
			 * timer's periodic flush will have made room. */
			if (a->new_offset || a->time_modify_set) {
				if (layout_commit_aggregator_submit(
						cd->lcommit_agg,
						cd->current_fh.fileid,
						new_size, mtime) != 0) {
					return NFS4ERR_DELAY;
				}
			}

			/* DS_PENDING clear stays synchronous -- a flag
			 * bit can't be aggregated.  This typically only
			 * fires on the very first LAYOUTCOMMIT after
			 * CREATE for an HPC file. */
			if (lc_inode.flags & MDS_IFLAG_DS_PENDING) {
				lc_inode.flags &= ~MDS_IFLAG_DS_PENDING;
				(void)cat_setattr(cd,
					cd->current_fh.fileid,
					&lc_inode, MDS_ATTR_FLAGS);
				compound_inode_invalidate(cd,
					cd->current_fh.fileid);
			}

			if (!r->new_size) {
				r->new_size = false;
				r->new_size_value = 0;
			}
			return NFS4_OK;
		}

		/*
		 * Coalesced metadata write for LAYOUTCOMMIT.
		 *
		 * The old implementation issued up to three separate
		 * cat_setattr calls per LAYOUTCOMMIT:
		 *   1. SIZE update (when client reports growth)
		 *   2. DS_PENDING flag clear (first commit after CREATE)
		 *   3. MTIME update (when client supplied time_modify)
		 * Each cat_setattr is internally a 2-trip NDB RMW, and
		 * the old code also inserted two intermediate cat_getattr
		 * re-reads between them, so the worst-case path issued
		 * roughly 8 NDB round-trips for one LAYOUTCOMMIT.  Under
		 * an untar workload (which sends size + mtime on every
		 * file) we measured ~2 cat_setattrs per commit averaged,
		 * dominating LAYOUTCOMMIT's ~12 ms p50 cost.
		 *
		 * Merge the three updates into a single masked cat_setattr
		 * so the catalogue performs one R-M-W transaction with all
		 * fields applied at once.  Failure semantics are preserved:
		 *   - SIZE was historically mandatory: any write failure
		 *     here, when SIZE participates, still surfaces
		 *     NFS4ERR_IO to the client.
		 *   - DS_PENDING and MTIME were historically best-effort
		 *     (the old code called (void) cat_setattr and ignored
		 *     errors).  When neither SIZE participates, a write
		 *     failure is swallowed exactly as before.
		 *
		 * Bonus correctness fix: the old DS_PENDING clear used
		 * mask=0, which catalogue_rondb_ns_setattr does not honour
		 * for the flags field (it only copies attrs->flags into
		 * the read-back inode when mask & MDS_ATTR_FLAGS is set).
		 * The in-memory bit-clear was therefore being silently
		 * dropped on every flush.  The merged mask below includes
		 * MDS_ATTR_FLAGS, so DS_PENDING is now actually persisted
		 * cleared on the first LAYOUTCOMMIT after CREATE.
		 */
		{
			uint32_t merged_mask = 0;
			bool size_grew = false;
			uint64_t new_size = lc_inode.size;
			uint64_t old_size = lc_inode.size;

			if (a->new_offset) {
				if (a->last_write_offset == UINT64_MAX) {
					return NFS4ERR_INVAL;
				}
				new_size = a->last_write_offset + 1;

				if (new_size > lc_inode.size) {
					enum nfs4_status lc_nst;
					lc_nst = quota_check_bytes(
						cd, lc_inode.uid,
						lc_inode.gid,
						new_size - lc_inode.size);
					if (lc_nst != NFS4_OK) {
						return lc_nst;
					}
					lc_inode.size = new_size;
					merged_mask |= MDS_ATTR_SIZE;
					size_grew = true;
				}
			}

			if (lc_inode.flags & MDS_IFLAG_DS_PENDING) {
				lc_inode.flags &= ~MDS_IFLAG_DS_PENDING;
				merged_mask |= MDS_ATTR_FLAGS;
			}

			if (a->time_modify_set) {
				lc_inode.mtime = a->time_modify;
				merged_mask |= MDS_ATTR_MTIME;
			}

			if (merged_mask != 0) {
				st = cat_setattr(cd,
					cd->current_fh.fileid,
					&lc_inode, merged_mask);
				if (st != MDS_OK) {
					if (size_grew) {
						return NFS4ERR_IO;
					}
					/* DS_PENDING / mtime: best-effort. */
				} else {
					compound_inode_invalidate(cd,
						cd->current_fh.fileid);
					if (size_grew) {
						quota_submit_adjust(cd,
							lc_inode.uid,
							lc_inode.gid,
							(int64_t)(new_size -
								old_size),
							0);
						r->new_size = true;
						r->new_size_value = new_size;
					}
				}
			}
		}
	}

	if (!r->new_size) {
		r->new_size = false;
		r->new_size_value = 0;
	}

	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * NFSv4.2 operation handlers (RFC 7862)
 * ----------------------------------------------------------------------- */

/** IO_ADVISE (RFC 7862 S15.5): acknowledge I/O hints. */


/** LAYOUTERROR (RFC 7862 S15.6): client reports DS I/O error. */
/* cppcheck-suppress constParameterPointer */
enum nfs4_status op_layouterror(struct compound_data *cd,
				       const struct nfs4_op *op,
				       struct nfs4_result *res)
{
	const struct nfs4_arg_layouterror *a = &op->arg.layouterror;
	enum nfs4_status nst;
	(void)res;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/*
	 * RFC 7862 S15.6 -- log client-reported DS error.
	 *
	 * LAYOUTERROR is advisory only: clients may report errors
	 * caused by MDS-side unresponsiveness (e.g. NDB latency)
	 * that have nothing to do with the DS itself.  Letting
	 * client reports drive DS offline transitions caused
	 * cascading false-positives.  The health probe is the
	 * sole authoritative source for DS state changes.
	 */
	{
		uint32_t ds_id = deviceid_to_ds_id(a->deviceid);
		MDS_LOG_INFO(LOG_COMP_NFS,
			"LAYOUTERROR from client for DS %u "
			"(advisory, not driving health state)",
			(unsigned)ds_id);
	}
	return NFS4_OK;
}

/** LAYOUTSTATS (RFC 7862 S15.7): client reports I/O statistics. */
enum nfs4_status op_layoutstats(const struct compound_data *cd,
				       const struct nfs4_op *op,
				       struct nfs4_result *res)
{
	enum nfs4_status nst;
	(void)res;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* Feed I/O bytes into the tiering tracker (NULL = no-op). */
	const struct nfs4_arg_layoutstats *ls = &op->arg.layoutstats;
	io_tracker_record(cd->io_tracker, cd->current_fh.fileid,
			  ls->read_bytes, ls->write_bytes);

	return NFS4_OK;
}

/** ALLOCATE (RFC 7862 S15.1): preallocate space. */

/*
 * Promote an inline file to DS storage if needed.  Returns NFS4_OK if
 * the file is not inline or was successfully promoted.  On error the
 * caller should return the error status to the client.
 */


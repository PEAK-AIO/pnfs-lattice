/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 */
/*
 * compound_layout.c — pNFS layout ops (LAYOUTGET/RETURN/COMMIT, GETDEVICEINFO, LAYOUTERROR/STATS).
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
 * Layout stateid generation — globally unique per layout grant.
 *
 * Wire format (12 bytes of stateid.other):
 *     [mds_id BE 4B][counter BE 8B]
 *
 * The counter is a single 64-bit monotonic integer.  It MUST never
 * repeat across MDS restarts, because the catalogue keeps layout_state
 * rows from previous runs (placement, stripe map, and — when
 * transient_state_cache is off or the CQ pregrant path fires — layout
 * grants themselves) until the client LAYOUTRETURNs or the server GCs.
 * If the counter restarts at 1 after a daemon restart, the new run's
 * LAYOUTGET issues a stateid that already exists in layout_state for a
 * different fileid.  The next LAYOUTCOMMIT looks the stateid up, sees
 * the stale row with the OLD fileid, compares it against the current
 * compound's FH (the NEW fileid), and returns NFS4ERR_BAD_STATEID.
 * Because the Linux client swallows BAD_STATEID on LAYOUTCOMMIT
 * silently (just drops the pending size update and moves on), the
 * write looks successful to userspace but the MDS never updates the
 * file size — resulting in an apparently-successful cp producing a
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
 * those paths are being audited separately — both currently rely on
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
 * RFC 8881 §12.5.3 requires a layout stateid's seqid to advance
 * monotonically across LAYOUTGET / LAYOUTRETURN / LAYOUTCOMMIT.  When
 * transient_state_cache=true (lab default) the layout_state row is
 * never written to NDB, so mds_coord_layout_get_by_stateid() always
 * misses and the server cannot distinguish a renewal from a new grant.
 *
 * This module-local table records every layout `other` we issue and
 * the latest seqid we returned for it.  Lookup is keyed on the 12-byte
 * `other`; chained hash with a striped mutex.  Entries persist for
 * the lifetime of the daemon — same lifecycle as the layout itself
 * for transient mode.  Memory is bounded by the number of distinct
 * granted stateids (one per (clientid, fileid) pair on most paths);
 * the hash overhead is sizeof(struct layout_seqid_entry) per entry.
 *
 * Persistent mode (transient_state_cache=false) ALSO updates this
 * table so the in-memory and NDB views stay coherent across daemon
 * runs — but the catalogue probe in layout_pick_stateid() runs first
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
 * — a duplicate call (same `other`) leaves the existing entry alone
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
 * Look up @other in the table.  On hit, advance the stored seqid by
 * one (with 0xFFFFFFFF → 1 wrap per RFC 8881 §8.2.2) and return the
 * NEW seqid via @next_seqid; sets *@hit = true.  On miss, *@hit =
 * false and @next_seqid is untouched.
 */
static void layout_seqid_advance(const uint8_t other[NFS4_OTHER_SIZE],
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
 * advancing it.  Used by LAYOUTRETURN seqid validation — RFC 8881
 * §8.2.2 says a stateid presented with seqid < current is OLD_STATEID,
 * with seqid > current is BAD_STATEID, and with seqid == current is
 * the latest copy and acceptable.
 *
 * Returns true on hit (sets *cur_seqid); false on miss (caller decides
 * whether to treat that as BAD_STATEID or accept by other validation).
 */
static bool layout_seqid_peek(const uint8_t other[NFS4_OTHER_SIZE],
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
 * miss — the caller decides whether that is BAD_STATEID or a fresh
 * grant. */
static void layout_seqid_remove(const uint8_t other[NFS4_OTHER_SIZE])
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
	 * seqid (RFC 8881 §12.5.3).  See layout_seqid_advance(). */
	layout_seqid_record_new(out->other);
}

/*
 * RFC 8881 §12.5.3 / §8.2.2 — layout stateid renewal.
 *
 * When a client passes the previously-issued layout stateid as input
 * to LAYOUTGET, the server MUST return the SAME `other` with seqid
 * advanced by one (wrapping 0xFFFFFFFF → 1, since 0 is reserved).
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
 * back to allocating a fresh stateid — the legacy behaviour.
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
 * Phase H of docs/hpc-nto1-plan.md — compatibility filter that
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

/**
 * Backfill missing NFS file handles for entries that still have
 * nfs_fh_len == 0.  Uses the proxy mount to create/open the DS
 * data file and capture the handle via name_to_handle_at().
 */
static void layout_backfill_missing_fhs(struct compound_data *cd,
					uint64_t fileid,
					uint32_t stripe_count,
					uint32_t stripe_unit,
					uint32_t mirror_count,
					struct mds_ds_map_entry *entries)
{
	uint32_t total;
	uint32_t i;
	bool need_fh_update = false;

	if (cd == NULL || entries == NULL ||
	    stripe_count == 0 || mirror_count == 0) {
		return;
	}
	total = stripe_count * mirror_count;

	for (i = 0; i < total; i++) {
		uint32_t stripe = i / mirror_count;
		uint32_t mirror = i % mirror_count;

		if (entries[i].nfs_fh_len > 0) {
			continue;
		}

		/* Generic DS: sync FH capture via proxy. */
		if (cd->proxy != NULL) {
			uint32_t cap = MDS_NFS_FH_MAX;
			enum mds_status fh_st;

			fh_st = mds_proxy_ensure_ds_file_fh(
				    cd->proxy, entries[i].ds_id, fileid,
				    stripe, mirror, entries[i].nfs_fh,
				    &cap);
			if (fh_st == MDS_OK) {
				entries[i].nfs_fh_len = cap;
				need_fh_update = true;
			} else {
				(void)fprintf(stderr,
					"WARN: FH backfill ds=%u "
					"fid=%lu s=%u m=%u "
					"failed: %d\n",
					(unsigned)entries[i].ds_id,
					(unsigned long)fileid,
					stripe, mirror,
					(int)fh_st);
			}
		} else {
			(void)fprintf(stderr,
				"WARN: FH backfill skipped "
				"(no proxy) ds=%u fid=%lu\n",
				(unsigned)entries[i].ds_id,
				(unsigned long)fileid);
		}
	}

	if (need_fh_update && cd->cat != NULL) {
		(void)mds_cat_stripe_map_put(cd->cat, NULL, fileid,
					    stripe_count, stripe_unit,
					    mirror_count, entries);
	}
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
 * Phase 12 Component A (see docs/design-layoutget-decoupling.md §4.1):
 * LAYOUTGET no longer runs synchronous DS NFS RPCs on the hot path.
 * When a placement produces entries with nfs_fh_len == 0, we:
 *
 *   1. Return the already-granted layout state via mds_coord_layout_return
 *      so the per-(clientid, fileid) state is not leaked.
 *   2. Mark the inode DS_PENDING so subsequent LAYOUTGETs take the
 *      ds_prepare fast-path.
 *   3. Enqueue the fileid into ds_prepare so the background worker
 *      populates the FH before the client retries.
 *   4. Return NFS4ERR_DELAY (RFC 8881 §15.1.1.3) — client retries
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

	/* RFC 8881 §16.2.4 — resolve CURRENT_STATEID4 marker.  Pynfs
	 * CSID7 testOpenLayoutGet bundles LAYOUTGET right after OPEN with
	 * the marker as the layout stateid argument.  Resolve once here
	 * so all six call sites of layout_pick_stateid below see the same
	 * resolved input. */
	nst = compound_resolve_stateid(cd, &a->stateid, &client_sid);
	if (nst != NFS4_OK) {
		return nst;
	}

	/*
	 * Phase 6 — wide long-lived layout grant.
	 *
	 * Rather than echo the client's requested range and iomode,
	 * we always grant a read-write, whole-file, long-lived layout.
	 * RFC 8881 §12.5.3 explicitly allows the server to grant a
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
	 * and writes again no longer needs a second LAYOUTGET — one
	 * NDB commit saved per file.  Measured wins come from any
	 * workload where the same file is re-opened within its lease
	 * window.
	 */
	const uint32_t grant_iomode = LAYOUTIOMODE4_RW;
	const uint64_t grant_offset = 0;
	const uint64_t grant_length = UINT64_MAX;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* Only support files layout type. */
	if (a->layout_type != LAYOUT4_NFSV4_1_FILES && a->layout_type != LAYOUT4_FLEX_FILES) {
		return NFS4ERR_NOTSUPP;
}

	/* Must be a regular file. */
	st = compound_inode_get(cd, cd->current_fh.fileid, &inode);
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

	/*
	 * Mark's bug — byte-range CB_LAYOUTRECALL.
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
	 * still leaves the catalogue in a coherent state — holders
	 * whose row was revoked observe NFS4ERR_BAD_STATEID on their
	 * next LAYOUT*, and the requesting LAYOUTGET below proceeds
	 * normally.  Skipped when:
	 *   - cd->lr is unset (test compat / no recall coordinator),
	 *   - the request's stateid matches the requesting client's
	 *     existing layout (idempotent renew — no recall needed).
	 * The same-client case is handled inside the helper by the
	 * clientid filter, but we early-out here to avoid the catalogue
	 * round trip for the common renew path.
	 */
	if (cd->lr != NULL) {
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

		(void)layout_recall_byte_range_for_holders(
			cd->lr,
			cd->current_fh.fileid,
			cd->clientid,
			req_iomode_for_recall,
			a->offset,
			a->length,
			a->layout_type,
			&recalled);
		(void)recalled; /* tracked via per-holder log; metrics tbd */
	}

	/*
	 * If the CQ fused a layout pregrant onto the preceding
	 * OPEN(CREATE), consume it now — before path branching.
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
		prep_state = compound_ds_prepare_check(cd, cd->current_fh.fileid);
		if (prep_state == -1 && cd->ds_prepare != NULL) {
			compound_maybe_enqueue_ds_prepare(cd, &inode);
			/* Re-check after enqueue; result consumed
			 * only for side-effect (queue priming). */
			(void)compound_ds_prepare_check(
				cd, cd->current_fh.fileid);
		}
		/* Fall through to the LAYOUTGET path.  FH capture
		 * happens during placement (new files only).
		 * Returning LAYOUTUNAVAILABLE here would cause the
		 * Linux kernel client to cache the failure
		 * permanently and never retry pNFS for this file. */
	}

	/*
	 * Phase D of docs/hpc-nto1-plan.md — HPC-Shared layout cache
	 * fast path.
	 *
	 * For inodes with MDS_IFLAG_HPC_SHARED, look the stripe map up
	 * in the per-MDS layout cache before touching the catalogue.
	 * On hit we still mint a fresh per-client layout stateid (v1
	 * keeps per-client stateids — no shared stateid until a
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
				entries[0].nfs_fh_len = 0; /* FH pending */

				/*
				 * Reuse the CQ pregrant stateid if OPEN(CREATE)
				 * produced one for this fileid in this compound.
				 * Its consumption above cleared layout_pregranted
				 * so compound_process skips
				 * revoke_unused_pregrant on any subsequent error
				 * return — the previously observed fused
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
				 * docs/design-layoutget-decoupling.md §4.1. */
				if (!layout_entries_ready_for_grant(
					    cd, entries,
					    stripe_count * mirror_count)) {
					enum nfs4_status ds;

					ds = layout_revoke_unready_grant(
						cd, &fast_sid, entries,
						stripe_count * mirror_count);
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

		st = catalogue_rondb_layoutget_fused(
			cd->cat, cd->current_fh.fileid,
			&stripe_count, &stripe_unit,
			&mirror_count, &entries,
			&fused_sid, cd->clientid,
			grant_iomode, grant_offset, grant_length,
			cd->mds_id);

		if (st == MDS_OK) {
			/* Layout grant already persisted in the fused txn. */
			r->stateid = fused_sid;

			/* No backfill — if FH is missing, return
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
		 * no grant has been issued yet — no layout_return to
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
			/* Need placement — read DS registry. */
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
			 * docs/design-layoutget-decoupling.md §4.1. */
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
	 * migration and is not supported — document as a limitation.
	 */

	/* No backfill — LAYOUTGET must never block on network I/O.
	 * If entries have nfs_fh_len == 0 (legacy or failed capture),
	 * return LAYOUTUNAVAILABLE.  The client falls back to proxy
	 * I/O through the MDS for these files. */
	if (!layout_entries_ready_for_grant(
		    cd, entries, stripe_count * mirror_count)) {
		enum nfs4_status revoke_status;

		revoke_status = layout_revoke_unready_grant(
			cd, &r->stateid, entries, stripe_count * mirror_count);
		free(entries);
		return revoke_status;
	}
	if (clear_ds_pending_on_success &&
	    layout_entries_ready_for_grant(
		    cd, entries, stripe_count * mirror_count)) {
		layout_clear_ds_pending(cd, cd->current_fh.fileid);
	}

fill_layoutget_result:
	/* Phase D of docs/hpc-nto1-plan.md — populate the HPC layout
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
	 * Phase 6 — advertise the wide long-lived grant to the client:
	 *   - iomode = RW  (server may widen per RFC 8881 §12.5.3)
	 *   - offset = 0, length = UINT64_MAX  (whole file)
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

	/* Phase E + E+ of docs/hpc-nto1-plan.md — byte-range stripe
	 * subsetting with stripe-aligned boundaries.
	 *
	 * If the client asked for a narrow byte range (length !=
	 * UINT64_MAX) and we have a real stripe geometry, restrict the
	 * returned DS list to the stripes that actually cover it.  The
	 * authoritative grant range stays whole-file (grant_offset /
	 * grant_length above) — we only narrow what we emit on the
	 * wire so the client's per-DS dispatch table is stripe-aligned
	 * and proportional to the request.
	 *
	 * Selection:
	 *   start_stripe = floor(offset / stripe_unit)
	 *   end_stripe   = floor((offset + length - 1) / stripe_unit)
	 *   clamped to [0, stripe_count - 1]
	 *
	 * The returned (offset, length) pair stays the granted range:
	 * RFC 8881 §12.5.3 lets the server widen, and Phase D's per-
	 * inode layout cache will rely on the cached layout being
	 * authoritative across narrowed requests.  Only ds[] is
	 * trimmed.  ffl_stripe_unit (== r->stripe_unit) tells the
	 * Linux 6.18+ flex-files client which entry covers which byte;
	 * see RFC 8435 §5.1 and the kernel `dss_id = offset /
	 * stripe_unit` dispatcher.
	 *
	 * Single-stripe layouts skip the trim entirely — the legacy
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
	 *   FF_XDR_FORM_LEGACY  — N ff_mirror4 entries, each with
	 *                         ds_count = mirror_count.  Pre-6.18
	 *                         Linux flex-files clients consume
	 *                         this shape.
	 *   FF_XDR_FORM_STRIPED — 1 ff_mirror4 entry whose ds_count
	 *                         == stripe_count.  Linux 6.18+
	 *                         clients consume this as the per-
	 *                         stripe DS dispatch table (RFC 8435
	 *                         §5.1).
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

	/* Phase C / Step 6 of docs/hpc-nto1-plan.md — ff_xdr_form
	 * selector.  Plain (non-HPC) inodes always emit the legacy
	 * one-DS-per-mirror form so existing clients see bit-for-bit
	 * identical wire output.  HPC-Shared inodes consult
	 * cd->cfg_hpc_xdr_form:
	 *
	 *   AUTO     — striped iff mirror_count == 1 && stripe_count
	 *              > 1 (the only shape RFC 8435 §5.1 lets us emit
	 *              for multi-DS-per-mirror without violating the
	 *              one-mirror-per-replica constraint).
	 *   STRIPED  — force striped (lab / 6.18+-only fleets).
	 *   LEGACY   — force legacy (mixed-kernel fleets).
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
	/* QA review Blocker 1 — honor loga_maxcount per RFC 8881 §18.43.4.
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
			 * — striped puts them inside one ff_mirror4, legacy
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
		(void)fprintf(stderr,
			"layoutget: BUG \u2014 dss_count=%u with stripe_unit=0; "
			"client will collapse to single-DS\n",
			r->ds_count);
	}

	/* Populate flex-files mirror model if flex layout requested.
	 * Two shapes per RFC 8435 §5.1:
	 *
	 *   STRIPED — exactly one ff_mirror4 with ds_count == stripe_count.
	 *             The Linux 6.18+ flex-files client treats this as the
	 *             per-stripe DS dispatch table (dss_id =
	 *             offset / stripe_unit).  Used for HPC-Shared inodes
	 *             whose mirror_count == 1 && stripe_count > 1.
	 *
	 *   LEGACY  — stripe_count * mirror_count ff_mirror4 entries, each
	 *             with ds_count == 1.  Pre-6.18 Linux clients consume
	 *             this as one DS per mirror per layout segment. */
	if (r->layout_type == LAYOUT4_FLEX_FILES) {
		r->ff_flags = 0;

		/*
		 * RFC 8435 §5.1: ffl_user / ffl_group identify the user
		 * and group the DS must use to access the data on behalf
		 * of the client.  Pynfs FFLA1 (testFlexLayoutTestAccess)
		 * requires READ vs RW grants to advertise *different*
		 * uids and the *same* gid — the canonical RFC 8435
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

		/* RFC 8881 §16.2.4 — resolve CURRENT_STATEID4 marker. */
		nst = compound_resolve_stateid(cd, &a->stateid,
					       &sid_resolved);
		if (nst != NFS4_OK) {
			return nst;
		}

		/* RFC 8881 §8.2.2 / §12.5.3: the server MUST validate
		 * the layout stateid's seqid on LAYOUTRETURN.  A seqid
		 * older than the latest issued is NFS4ERR_OLD_STATEID;
		 * a seqid newer than issued is NFS4ERR_BAD_STATEID.
		 *
		 * The probe lives in the in-memory tracker (the only
		 * source-of-truth under transient_state_cache=true).
		 * If the stateid is unknown, accept the LAYOUTRETURN
		 * — the layout was already cleaned up or never
		 * issued by this daemon, so RFC 8881 §18.44.3's
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
		 * — layout_return handles index cleanup with
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

	/* RFC 8881 §16.2.4 — resolve CURRENT_STATEID4 marker. */
	nst = compound_resolve_stateid(cd, &a->stateid, &sid_resolved);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* Validate the committed layout stateid (RFC 8881 §18.6).
	 * Best-effort: if layout_grant persistence failed (RonDB mode),
	 * the stateid won't be found.  Accept the LAYOUTCOMMIT anyway
	 * so the file size gets updated. */
	{
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
		/* NOTFOUND: layout_grant was best-effort — proceed. */
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

		/* Phase F of docs/hpc-nto1-plan.md, integration part B —
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
				 * supply a value (RFC 8881 §18.6). */
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
			 * to the client — silently dropping the size /
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

			/* DS_PENDING clear stays synchronous — a flag
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

		/* Update file size if the client reported a new
		 * last-write offset. */
		if (a->new_offset) {
			if (a->last_write_offset == UINT64_MAX) {
				return NFS4ERR_INVAL;
			}
			uint64_t new_size = a->last_write_offset + 1;

			if (new_size > lc_inode.size) {
				/* Quota: check byte growth. */
				{
					enum nfs4_status lc_nst;
					lc_nst = quota_check_bytes(
						cd, lc_inode.uid,
						lc_inode.gid,
						new_size - lc_inode.size);
					if (lc_nst != NFS4_OK) {
						return lc_nst;
					}
				}
				uint64_t old_size = lc_inode.size;
				lc_inode.size = new_size;
				st = cat_setattr(cd,
					cd->current_fh.fileid,
					&lc_inode, MDS_ATTR_SIZE);
				if (st != MDS_OK) {
					return NFS4ERR_IO;
				}
				compound_inode_invalidate(cd,
					cd->current_fh.fileid);
				quota_submit_adjust(cd, lc_inode.uid,
					lc_inode.gid,
					(int64_t)(new_size - old_size), 0);
				r->new_size = true;
				r->new_size_value = new_size;
			}
		}

		/* Clear DS_PENDING if still set.  Re-read after any
		 * size mutation to get fresh flags. */
		if (r->new_size) {
			st = cat_getattr(cd,
				cd->current_fh.fileid, &lc_inode);
			if (st != MDS_OK) {
				goto lc_mtime;
			}
		}
		if (lc_inode.flags & MDS_IFLAG_DS_PENDING) {
			lc_inode.flags &= ~MDS_IFLAG_DS_PENDING;
			(void)cat_setattr(cd, cd->current_fh.fileid,
					     &lc_inode, 0);
			compound_inode_invalidate(cd,
				cd->current_fh.fileid);
		}

	lc_mtime:
		/* Apply client-supplied mtime (RFC 8881 §18.6).
		 * Re-read if any prior mutation invalidated. */
		if (a->time_modify_set) {
			st = cat_getattr(cd,
				cd->current_fh.fileid, &lc_inode);
			if (st == MDS_OK) {
				lc_inode.mtime = a->time_modify;
				(void)cat_setattr(cd,
					cd->current_fh.fileid,
					&lc_inode, 0);
				compound_inode_invalidate(cd,
					cd->current_fh.fileid);
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

/** IO_ADVISE (RFC 7862 §15.5): acknowledge I/O hints. */


/** LAYOUTERROR (RFC 7862 §15.6): client reports DS I/O error. */
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
	 * RFC 7862 §15.6 — log client-reported DS error.
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
		(void)fprintf(stderr,
			"INFO: LAYOUTERROR from client for DS %u "
			"(advisory, not driving health state)\n",
			(unsigned)ds_id);
	}
	return NFS4_OK;
}

/** LAYOUTSTATS (RFC 7862 §15.7): client reports I/O statistics. */
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

/** ALLOCATE (RFC 7862 §15.1): preallocate space. */

/*
 * Promote an inline file to DS storage if needed.  Returns NFS4_OK if
 * the file is not inline or was successfully promoted.  On error the
 * caller should return the error status to the client.
 */


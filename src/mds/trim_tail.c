/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * trim_tail.c -- Trim module state: striped per-inode locks and the
 * quiescing-flag table backing the background DS-tail trim.
 *
 * The locks (L(F) in the design doc) are one pthread_mutex per
 * stripe; the stripe index is the fileid folded through splitmix64,
 * the same idiom as the other striped MDS tables.  Two fileids on
 * the same stripe share a mutex and therefore serialize against
 * each other (see trim_inode_lock() in trim_tail.h for why that is
 * acceptable).
 *
 * The quiescing flags are a striped, dynamically growing list: an
 * entry is allocated in trim_quiesce_begin() and freed in
 * trim_quiesce_end(), so steady state holds only in-flight trims.
 * The flag must never fail to be set -- a missing flag leaves the
 * LAYOUTGET gate open and reopens the P1-1 window -- so a malloc
 * failure falls back to a fixed per-stripe emergency slot, and only
 * if even that is full does the flag go unrecorded (logged).
 *
 * The flag list is guarded by its own stripe mutexes, NOT by the
 * inode-lock table: the inode lock is recursion-unsafe by contract
 * and callers are allowed to consult or clear the flag while holding
 * L(F) (the trim worker clears its flag on every exit path, and
 * LAYOUTGET checks the flag without taking L(F)).
 */

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "catalogue_internal.h"  /* struct mds_catalogue (self_mds_id) */
#include "layout_recall.h"
#include "mds_catalogue.h"
#include "mds_log.h"
#include "mds_metrics.h"
#include "proxy_io.h"
#include "subtree_map.h"
#include "trim_tail.h"

#define TRIM_STRIPES           16
#define TRIM_EMERGENCY_SLOTS   8
#define TRIM_DEFAULT_LEASE_SEC 90

/* One quiescing flag is one list entry: presence means quiescing. */
struct trim_quiesce_entry {
	uint64_t fileid;
	struct trim_quiesce_entry *next;
};

/* Per-inode lock table (L(F)): one mutex per stripe. */
static pthread_mutex_t g_lock_stripes[TRIM_STRIPES];
static bool g_locks_inited;

/* Quiescing-flag table: one striped, dynamically growing list, plus
 * fixed emergency slots for the malloc-failure fallback. */
static pthread_mutex_t g_flag_stripes[TRIM_STRIPES];
static struct trim_quiesce_entry *g_flag_heads[TRIM_STRIPES];
static uint64_t g_flag_emergency_fileid[TRIM_STRIPES][TRIM_EMERGENCY_SLOTS];
static bool g_flag_emergency_used[TRIM_STRIPES][TRIM_EMERGENCY_SLOTS];
static bool g_flags_inited;

static uint32_t g_trim_max_wait_sec;  /* resolved (0 config = 3 x lease) */
static uint32_t g_lease_sec;          /* resolved (0 config = library def) */

/* Execution-core bindings (bound at daemon startup before the ds_gc
 * drainer starts; NULL-safe for the unit tests). */
static const struct subtree_map *g_smap;   /* NULL = fail-open ownership */
static struct layout_recall     *g_lr;     /* NULL = no layouts to quiesce */

/* First-seen bookkeeping (spec S3.5): one entry per fileid with a
 * monotonic first-seen timestamp and a one-shot lease-age WARN flag.
 * Cold path: only drainer workers touch it, guarded by a single
 * mutex.  The entry count IS the trim_pending gauge. */
struct trim_first_seen {
	uint64_t                fileid;
	uint64_t                first_ns;
	bool                    warned;
	struct trim_first_seen *next;
};

static pthread_mutex_t          g_seen_mutex = PTHREAD_MUTEX_INITIALIZER;
static struct trim_first_seen  *g_seen_head;

/* Forward declarations (defined with the execution core below; the
 * lifecycle handlers need seen_clear_all). */
static void seen_clear_all(void);

/** splitmix64 -- same hash as session.c / inode_cache.c. */
static uint64_t splitmix64(uint64_t x)
{
	x ^= x >> 30;
	x *= 0xbf58476d1ce4e5b9ULL;
	x ^= x >> 27;
	x *= 0x94d049bb133111ebULL;
	x ^= x >> 31;
	return x;
}

/** Select the stripe from the fileid (MDS striped-table fold idiom). */
static uint32_t trim_stripe_idx(uint64_t fileid)
{
	return (uint32_t)(splitmix64(fileid) % TRIM_STRIPES);
}

/* -----------------------------------------------------------------------
 * Flag-list helpers -- caller holds g_flag_stripes[si]
 * ----------------------------------------------------------------------- */

static struct trim_quiesce_entry *flag_find(uint32_t si, uint64_t fileid)
{
	struct trim_quiesce_entry *e;

	for (e = g_flag_heads[si]; e != NULL; e = e->next) {
		if (e->fileid == fileid) {
			return e;
		}
	}
	return NULL;
}

static bool flag_in_emergency(uint32_t si, uint64_t fileid)
{
	uint32_t i;

	for (i = 0; i < TRIM_EMERGENCY_SLOTS; i++) {
		if (g_flag_emergency_used[si][i] &&
		    g_flag_emergency_fileid[si][i] == fileid) {
			return true;
		}
	}
	return false;
}

/** Free every dynamic entry and clear the emergency slots. */
static void flags_clear_all(void)
{
	uint32_t si;

	for (si = 0; si < TRIM_STRIPES; si++) {
		struct trim_quiesce_entry *e;

		pthread_mutex_lock(&g_flag_stripes[si]);
		for (e = g_flag_heads[si]; e != NULL; ) {
			struct trim_quiesce_entry *victim = e;

			e = e->next;
			free(victim);
		}
		g_flag_heads[si] = NULL;
		memset(g_flag_emergency_used[si], 0,
		       sizeof(g_flag_emergency_used[si]));
		pthread_mutex_unlock(&g_flag_stripes[si]);
	}
}

/* -----------------------------------------------------------------------
 * Module lifecycle
 * ----------------------------------------------------------------------- */

int trim_module_init(uint32_t lease_time_sec, uint32_t trim_max_wait_sec)
{
	uint32_t lease;
	uint64_t wait;
	uint32_t i;

	if (!g_locks_inited) {
		for (i = 0; i < TRIM_STRIPES; i++) {
			(void)pthread_mutex_init(&g_lock_stripes[i], NULL);
		}
		g_locks_inited = true;
	}
	if (!g_flags_inited) {
		for (i = 0; i < TRIM_STRIPES; i++) {
			(void)pthread_mutex_init(&g_flag_stripes[i], NULL);
		}
		g_flags_inited = true;
	}
	/* Re-init is in place: drop any state a previous incarnation
	 * left behind (tests and simulated restarts re-init the
	 * module without a matching fini in between). */
	flags_clear_all();
	seen_clear_all();

	/* lease_time_sec = 0 means "unset": fall back to the session
	 * layer's default lease so the wait scales with the real
	 * lease window the client sees. */
	lease = (lease_time_sec > 0) ? lease_time_sec
				     : TRIM_DEFAULT_LEASE_SEC;
	wait = (trim_max_wait_sec > 0)
	     ? (uint64_t)trim_max_wait_sec
	     : 3ULL * (uint64_t)lease;
	if (wait > UINT32_MAX) {
		wait = UINT32_MAX;
	}
	g_trim_max_wait_sec = (uint32_t)wait;
	g_lease_sec = lease;
	return 0;
}

void trim_module_fini(void)
{
	uint32_t i;

	if (g_flags_inited) {
		flags_clear_all();
		for (i = 0; i < TRIM_STRIPES; i++) {
			pthread_mutex_destroy(&g_flag_stripes[i]);
		}
		g_flags_inited = false;
	}
	if (g_locks_inited) {
		for (i = 0; i < TRIM_STRIPES; i++) {
			pthread_mutex_destroy(&g_lock_stripes[i]);
		}
		g_locks_inited = false;
	}
	seen_clear_all();
	g_trim_max_wait_sec = 0;
	g_lease_sec = 0;
}

/* -----------------------------------------------------------------------
 * Per-inode lock (L(F))
 * ----------------------------------------------------------------------- */

void trim_inode_lock(uint64_t fileid)
{
	uint32_t si = trim_stripe_idx(fileid);

	pthread_mutex_lock(&g_lock_stripes[si]);
}

void trim_inode_unlock(uint64_t fileid)
{
	uint32_t si = trim_stripe_idx(fileid);

	pthread_mutex_unlock(&g_lock_stripes[si]);
}

/* -----------------------------------------------------------------------
 * Quiescing flags
 * ----------------------------------------------------------------------- */

void trim_quiesce_begin(uint64_t fileid)
{
	uint32_t si = trim_stripe_idx(fileid);
	uint32_t i;
	struct trim_quiesce_entry *e;

	pthread_mutex_lock(&g_flag_stripes[si]);
	if (flag_find(si, fileid) == NULL &&
	    !flag_in_emergency(si, fileid)) {
		e = calloc(1, sizeof(*e));
		if (e != NULL) {
			e->fileid = fileid;
			e->next = g_flag_heads[si];
			g_flag_heads[si] = e;
		} else {
			/* Allocation failed: fall back to a fixed
			 * emergency slot so the flag is still set --
			 * a missing flag leaves the LAYOUTGET gate
			 * open and reopens the P1-1 window. */
			for (i = 0; i < TRIM_EMERGENCY_SLOTS; i++) {
				if (!g_flag_emergency_used[si][i]) {
					g_flag_emergency_used[si][i] = true;
					g_flag_emergency_fileid[si][i] = fileid;
					break;
				}
			}
			if (i == TRIM_EMERGENCY_SLOTS) {
				/* Emergency slots exhausted: the flag
				 * cannot be recorded; log it loudly.
				 * Practically unreachable (steady state
				 * holds only in-flight trims). */
				MDS_LOG_ERROR(LOG_COMP_MDS,
					"trim: quiesce flag for fileid %llu "
					"could not be recorded "
					"(emergency slots exhausted)",
					(unsigned long long)fileid);
			}
		}
	}
	pthread_mutex_unlock(&g_flag_stripes[si]);
}

void trim_quiesce_end(uint64_t fileid)
{
	uint32_t si = trim_stripe_idx(fileid);
	uint32_t i;
	bool found = false;

	pthread_mutex_lock(&g_flag_stripes[si]);
	for (i = 0; !found && i < TRIM_EMERGENCY_SLOTS; i++) {
		if (g_flag_emergency_used[si][i] &&
		    g_flag_emergency_fileid[si][i] == fileid) {
			g_flag_emergency_used[si][i] = false;
			found = true;
		}
	}
	if (!found) {
		struct trim_quiesce_entry **link;

		for (link = &g_flag_heads[si];
		     !found && *link != NULL; link = &(*link)->next) {
			if ((*link)->fileid == fileid) {
				struct trim_quiesce_entry *victim = *link;

				*link = victim->next;
				free(victim);
				found = true;
			}
		}
	}
	pthread_mutex_unlock(&g_flag_stripes[si]);
}

bool trim_quiescing(uint64_t fileid)
{
	uint32_t si = trim_stripe_idx(fileid);
	struct trim_quiesce_entry *e;
	bool set = false;

	pthread_mutex_lock(&g_flag_stripes[si]);
	for (e = g_flag_heads[si]; e != NULL && !set; e = e->next) {
		if (e->fileid == fileid) {
			set = true;
		}
	}
	if (!set) {
		set = flag_in_emergency(si, fileid);
	}
	pthread_mutex_unlock(&g_flag_stripes[si]);
	return set;
}

/* -----------------------------------------------------------------------
 * Resolved configuration
 * ----------------------------------------------------------------------- */

uint32_t trim_max_wait_sec(void)
{
	/* Written only by trim_module_init(), which runs before any
	 * other thread starts using the module. */
	return g_trim_max_wait_sec;
}

uint32_t trim_lease_sec(void)
{
	return g_lease_sec;
}

/* -----------------------------------------------------------------------
 * Execution-core bindings
 * ----------------------------------------------------------------------- */

void trim_set_subtree_map(const struct subtree_map *map)
{
	g_smap = map;
}

void trim_set_layout_recall(struct layout_recall *lr)
{
	g_lr = lr;
}

/* -----------------------------------------------------------------------
 * First-seen bookkeeping (backoff bound + trim_pending gauge)
 * ----------------------------------------------------------------------- */

static uint64_t trim_now_ns(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
		return 0;
	}
	return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void seen_clear_all(void)
{
	struct trim_first_seen *e;

	pthread_mutex_lock(&g_seen_mutex);
	for (e = g_seen_head; e != NULL; ) {
		struct trim_first_seen *victim = e;

		e = e->next;
		free(victim);
	}
	g_seen_head = NULL;
	atomic_store_explicit(&g_branch_metrics.trim_pending, 0,
			      memory_order_relaxed);
	pthread_mutex_unlock(&g_seen_mutex);
}

/**
 * Upsert the fileid and return its age in ns since first sight.
 * One-shot WARN when the age crosses one lease period.
 */
static uint64_t seen_touch(uint64_t fileid)
{
	uint64_t                now = trim_now_ns();
	struct trim_first_seen *e;
	uint64_t                age = 0;
	uint32_t                count = 0;
	bool                    found = false;

	pthread_mutex_lock(&g_seen_mutex);
	for (e = g_seen_head; e != NULL; e = e->next) {
		count++;
		if (e->fileid == fileid) {
			found = true;
			age = (now >= e->first_ns)
			     ? now - e->first_ns : 0;
			if (!e->warned && g_lease_sec > 0 &&
			    age >= (uint64_t)g_lease_sec * 1000000000ULL) {
				e->warned = true;
				MDS_LOG_WARN(LOG_COMP_MDS,
					"trim: fileid %llu has been "
					"pending a DS-tail trim for "
					"over one lease period (%us)",
					(unsigned long long)fileid,
					(unsigned)g_lease_sec);
			}
		}
	}
	if (!found) {
		e = calloc(1, sizeof(*e));
		if (e != NULL) {
			e->fileid = fileid;
			e->first_ns = now;
			e->warned = false;
			e->next = g_seen_head;
			g_seen_head = e;
			count++;
		}
	}
	atomic_store_explicit(&g_branch_metrics.trim_pending,
			      (uint64_t)count, memory_order_relaxed);
	pthread_mutex_unlock(&g_seen_mutex);
	return age;
}

/** Drop the entry (row drained); refresh the gauge. */
static void seen_drop(uint64_t fileid)
{
	struct trim_first_seen **link;
	uint32_t                count = 0;

	pthread_mutex_lock(&g_seen_mutex);
	for (link = &g_seen_head; *link != NULL; link = &(*link)->next) {
		if ((*link)->fileid == fileid) {
			struct trim_first_seen *victim = *link;

			*link = victim->next;
			free(victim);
			break;
		}
		count++;
	}
	for (link = &g_seen_head; *link != NULL; link = &(*link)->next) {
		count++;
	}
	atomic_store_explicit(&g_branch_metrics.trim_pending,
			      (uint64_t)count, memory_order_relaxed);
	pthread_mutex_unlock(&g_seen_mutex);
}

/* -----------------------------------------------------------------------
 * Ownership guard (V5): partition map + fileid->path ancestry walk
 * ----------------------------------------------------------------------- */

enum trim_ownership {
	TRIM_OWN,
	TRIM_NOT_OWN,
	TRIM_INDETERMINATE,
};

#define TRIM_MAX_PATH_DEPTH 64

/**
 * Rebuild the absolute path of @a fileid by walking the
 * parent_fileid chain to the root with AUTHORITATIVE reads (the
 * partition map is path-keyed and must be judged against the
 * authority, never a hot-read image).  Every failure mode -- broken
 * chain, cycle, unresolvable name, depth overflow -- returns
 * MDS_ERR_NOTFOUND, which the caller treats as indeterminate: never
 * guess (a non-owner truncate would destroy bytes the owner's
 * clients are writing).
 */
static enum mds_status trim_file_path(struct mds_catalogue *cat,
				      uint64_t fileid,
				      char *path_out,
				      size_t cap)
{
	uint64_t seen[TRIM_MAX_PATH_DEPTH];
	char    *names[TRIM_MAX_PATH_DEPTH];
	char     name_store[TRIM_MAX_PATH_DEPTH][MDS_MAX_NAME + 1];
	uint64_t cur = fileid;
	int      depth;
	int      i;
	char    *p;
	size_t   total;

	if (cap < 2) {
		return MDS_ERR_INVAL;
	}
	path_out[0] = '\0';

	for (depth = 0; depth < TRIM_MAX_PATH_DEPTH; depth++) {
		struct mds_inode in;
		char     *name = name_store[depth];
		enum mds_status st;

		/* Cycle guard: a chain that revisits an inode cannot
		 * be trusted. */
		for (i = 0; i < depth; i++) {
			if (seen[i] == cur) {
				return MDS_ERR_NOTFOUND;
			}
		}
		seen[depth] = cur;

		st = mds_cat_ns_getattr(cat, cur, &in);
		if (st != MDS_OK) {
			return MDS_ERR_NOTFOUND;
		}
		if (in.parent_fileid == 0) {
			if (cur != MDS_FILEID_ROOT) {
				/* Orphan not under the root tree. */
				return MDS_ERR_NOTFOUND;
			}
			break;
		}
		st = mds_cat_ns_dirent_name_for_child(cat, in.parent_fileid,
						      cur, name,
						      MDS_MAX_NAME + 1);
		if (st != MDS_OK) {
			return MDS_ERR_NOTFOUND;
		}
		names[depth] = name;
		cur = in.parent_fileid;
	}
	if (depth == TRIM_MAX_PATH_DEPTH) {
		return MDS_ERR_NOTFOUND;
	}

	/* Assemble "/a/b/c" from the root's side. */
	total = 1;
	for (i = 0; i < depth; i++) {
		total += 1 + strlen(names[i]);
	}
	if (total + 1 > cap) {
		return MDS_ERR_NOTFOUND;
	}
	p = path_out;
	*p++ = '/';
	for (i = depth - 1; i >= 0; i--) {
		size_t nlen = strlen(names[i]);

		*p++ = '/';
		memcpy(p, names[i], nlen);
		p += nlen;
	}
	*p = '\0';
	return MDS_OK;
}

static enum trim_ownership trim_check_ownership(struct mds_catalogue *cat,
						uint64_t fileid)
{
	char               path[MDS_MAX_PATH + 1];
	struct subtree_entry se;

	if (g_smap == NULL) {
		/* No partition map bound (single-MDS deployment,
		 * unit tests): this MDS owns everything. */
		return TRIM_OWN;
	}
	if (trim_file_path(cat, fileid, path, sizeof(path)) != MDS_OK) {
		MDS_LOG_WARN(LOG_COMP_MDS,
			"trim: ownership indeterminate for fileid %llu "
			"(path walk failed); keeping row queued",
			(unsigned long long)fileid);
		return TRIM_INDETERMINATE;
	}
	if (subtree_map_lookup(g_smap, path, &se) != MDS_OK) {
		MDS_LOG_WARN(LOG_COMP_MDS,
			"trim: no partition-map entry covers %s "
			"(fileid %llu); keeping row queued",
			path, (unsigned long long)fileid);
		return TRIM_INDETERMINATE;
	}
	if (se.owner_mds_id != cat->self_mds_id) {
		/* Quiet by design: the row (owner_mds_id = 0) is
		 * visible to every MDS and the owner's drainer will
		 * drain it. */
		return TRIM_NOT_OWN;
	}
	return TRIM_OWN;
}

/* -----------------------------------------------------------------------
 * Trim execution core (spec S3.2/S3.4/S3.5)
 * ----------------------------------------------------------------------- */

enum mds_status trim_process_entry(struct mds_catalogue *cat,
				   const struct mds_proxy_ctx *proxy,
				   const struct mds_gc_entry *e)
{
	uint64_t                  fid;
	struct mds_inode          inode;
	uint32_t                  sc = 0, su = 0, mc = 0;
	struct mds_ds_map_entry  *entries = NULL;
	enum mds_status           st;
	bool                      expired = false;
	uint64_t                  t0, t1;

	if (cat == NULL || proxy == NULL || e == NULL) {
		return MDS_ERR_INVAL;
	}
	fid = e->fileid;

	/* 1. Vanish check: authoritative read (V9 -- never the
	 *    hot-read image).  A gone file has nothing left to
	 *    clear: dequeue as success. */
	st = mds_cat_ns_getattr(cat, fid, &inode);
	if (st == MDS_ERR_NOTFOUND) {
		return (mds_cat_gc_dequeue(cat, NULL, e->gc_seq) == MDS_OK)
		       ? MDS_OK : MDS_ERR_DELAY;
	}
	if (st != MDS_OK) {
		return MDS_ERR_DELAY;
	}

	/* 2. Ownership guard (V5): only the partition owner may
	 *    truncate; a non-owner leaves the row for the successor. */
	if (trim_check_ownership(cat, fid) != TRIM_OWN) {
		return MDS_ERR_DELAY;
	}

	/* 3. Stripe map (v9 inline fallback included): no map means
	 *    no DS bytes: dequeue as success. */
	st = proxy_stripe_map_get(cat, fid, &sc, &su, &mc, &entries);
	if (st == MDS_ERR_NOTFOUND) {
		free(entries);
		return (mds_cat_gc_dequeue(cat, NULL, e->gc_seq) == MDS_OK)
		       ? MDS_OK : MDS_ERR_DELAY;
	}
	if (st != MDS_OK) {
		free(entries);
		return MDS_ERR_DELAY;
	}

	/* 4. First-seen bookkeeping + backoff bound (S3.5): a row
	 *    pending past trim_max_wait while quiescing releases the
	 *    LAYOUTGET gate for a cycle, so a failing DS cannot wedge
	 *    a file in permanent DELAY. */
	{
		uint64_t age = seen_touch(fid);

		if (age > (uint64_t)trim_max_wait_sec() * 1000000000ULL &&
		    trim_quiescing(fid)) {
			trim_quiesce_end(fid);
			free(entries);
			return MDS_ERR_DELAY;
		}
	}

	/* 5. LAYOUTGET gate closed from here.  Every exit below
	 *    clears the flag -- except the deliberate
	 *    truncate/mount-failure keep (see step 7). */
	trim_quiesce_begin(fid);

	/* 6. Quiesce the file's client layouts, lease-bounded. */
	if (g_lr != NULL) {
		int lrc;

		t0 = trim_now_ns();
		lrc = layout_recall_quiesce_for_file(g_lr, fid,
						     trim_lease_sec() * 1000U,
						     &expired);
		t1 = trim_now_ns();
		atomic_fetch_add_explicit(
			&g_branch_metrics.trim_recall_wait_ns_sum,
			(t1 >= t0) ? t1 - t0 : 0, memory_order_relaxed);
		atomic_fetch_add_explicit(
			&g_branch_metrics.trim_recall_wait_count, 1,
			memory_order_relaxed);
		if (lrc != 0) {
			/* Enumeration failure: no revoke performed --
			 * leave the row for the next cycle. */
			trim_quiesce_end(fid);
			free(entries);
			return MDS_ERR_DELAY;
		}
	}

	/* 7. Critical section under L(F): the size and the map may
	 *    have moved since the unlocked checks (a write bracket
	 *    runs under the same lock), so re-read both here -- Rule
	 *    R: truncate to the CURRENT committed size. */
	trim_inode_lock(fid);
	st = mds_cat_ns_getattr(cat, fid, &inode);
	if (st == MDS_ERR_NOTFOUND) {
		/* Vanished under us: nothing to clear. */
		trim_inode_unlock(fid);
		trim_quiesce_end(fid);
		free(entries);
		seen_drop(fid);
		return (mds_cat_gc_dequeue(cat, NULL, e->gc_seq) == MDS_OK)
		       ? MDS_OK : MDS_ERR_DELAY;
	}
	if (st != MDS_OK) {
		trim_inode_unlock(fid);
		trim_quiesce_end(fid);
		free(entries);
		return MDS_ERR_DELAY;
	}
	{
		struct mds_ds_map_entry *locked_entries = NULL;
		uint32_t                 total;
		uint32_t                 i;

		/* Re-fetch: the map may have changed under us. */
		st = proxy_stripe_map_get(cat, fid, &sc, &su, &mc,
					  &locked_entries);
		if (st == MDS_ERR_NOTFOUND) {
			/* Map removed under us: nothing to clear. */
			trim_inode_unlock(fid);
			trim_quiesce_end(fid);
			free(entries);
			seen_drop(fid);
			return (mds_cat_gc_dequeue(cat, NULL, e->gc_seq)
				== MDS_OK) ? MDS_OK : MDS_ERR_DELAY;
		}
		if (st != MDS_OK) {
			trim_inode_unlock(fid);
			trim_quiesce_end(fid);
			free(entries);
			return MDS_ERR_DELAY;
		}
		total = sc * mc;
		for (i = 0; st == MDS_OK && i < total; i++) {
			/* Slot geometry (proxy_io.h): stripe = i % sc,
			 * mirror = i / sc. */
			st = mds_proxy_truncate_ds_file_to(proxy,
				locked_entries[i].ds_id, fid,
				(uint32_t)(i % sc),
				(uint32_t)(i / sc),
				inode.size);
		}
		free(locked_entries);
	}

	if (st == MDS_OK) {
		/* Full success: unblock, drop the bookkeeping,
		 * dequeue. */
		trim_inode_unlock(fid);
		trim_quiesce_end(fid);
		free(entries);
		seen_drop(fid);
		atomic_fetch_add_explicit(&g_branch_metrics.trim_completed,
					  1, memory_order_relaxed);
		return mds_cat_gc_dequeue(cat, NULL, e->gc_seq);
	}
	/* Truncate/mount failure: the DS tail is still stale, so
	 * DELIBERATELY keep the quiescing flag set (the LAYOUTGET
	 * gate stays closed); the row stays queued for retry and the
	 * backoff bound above releases the gate periodically. */
	trim_inode_unlock(fid);
	atomic_fetch_add_explicit(&g_branch_metrics.trim_failed, 1,
				  memory_order_relaxed);
	MDS_LOG_WARN(LOG_COMP_MDS,
		"trim: fileid %llu drain failed (st=%d); keeping the "
		"row queued and the quiescing flag set",
		(unsigned long long)fid, (int)st);
	free(entries);
	return MDS_ERR_DELAY;
}

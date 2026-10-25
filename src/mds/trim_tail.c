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

#include "mds_log.h"
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
	g_trim_max_wait_sec = 0;
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

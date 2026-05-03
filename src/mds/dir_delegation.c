/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * dir_delegation.c — NFSv4.1 directory delegation manager.
 *
 * Phase 8b scope:
 *   - In-memory hash table keyed on dir_fileid, with per-(dir,client)
 *     entries (a single directory can be delegated to multiple clients
 *     when no writer notification is active).
 *   - Striped mutexes (16 stripes, hash on dir_fileid) for low
 *     contention under concurrent COMPOUND processing.
 *   - GDD grants go through dir_deleg_grant.
 *   - Every namespace mutation that changes a directory's dirent set
 *     or a child's attrs must first call dir_deleg_recall_dir, which
 *     issues CB_RECALL best-effort to every other holder and revokes
 *     the grant locally (RFC 8881 §10.9.3).
 *   - No RonDB persistence in 8b — delegations are transient.  A
 *     client that reconnects after an MDS restart simply does not see
 *     the grant and re-issues GDD.  This matches the existing
 *     file-delegation transient_state_cache posture and keeps the 8b
 *     scope contained.
 */
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>

#include <sys/types.h>
#include <unistd.h>

#include "dir_delegation.h"
#include "compound.h"     /* NOTIFY4_* constants */
#include "nfs4_cb.h"
#include "session.h"
#include "rpc_server.h"   /* rpc_conn_get_fd */

/* -----------------------------------------------------------------------
 * Constants
 * ----------------------------------------------------------------------- */

#define DDT_STRIPE_COUNT   16
#define DDT_HASH_SIZE      4096
#define DDT_RECALL_DEFAULT_MS_BUILTIN 5000  /* compile-time fallback */

/*
 * Runtime-configurable default recall / notify timeout.
 * Overridden via dir_deleg_set_default_timeout() from main.c.
 */
static _Atomic uint32_t ddt_recall_default_ms =
	DDT_RECALL_DEFAULT_MS_BUILTIN;

void dir_deleg_set_default_timeout(uint32_t timeout_ms)
{
	if (timeout_ms < 50) {
		timeout_ms = 50;
	}
	atomic_store_explicit(&ddt_recall_default_ms, timeout_ms,
			      memory_order_relaxed);
}

static uint32_t ddt_default_timeout(void)
{
	return atomic_load_explicit(&ddt_recall_default_ms,
				    memory_order_relaxed);
}

/*
 * Match delegation.c: a recall in flight within this window is
 * suppressed so concurrent mutators do not send duplicate CB_RECALLs
 * to the same client for the same grant.  The window scales with
 * the configured default so operators who extend the timeout also
 * extend the dedupe horizon.
 */
static uint64_t ddt_recall_pending_ns(void)
{
	return (uint64_t)ddt_default_timeout() * 1000000ULL;
}

/* -----------------------------------------------------------------------
 * Internal structures
 * ----------------------------------------------------------------------- */

struct ddt_entry {
	struct nfs4_stateid   stateid;
	uint8_t               cookieverf[NFS4_VERIFIER_SIZE];
	uint64_t              clientid;
	uint64_t              dir_fileid;
	uint32_t              notification_mask;
	struct nfs4_session  *session;
	struct ddt_entry     *hash_next;
	bool                  recall_pending;
	uint64_t              recall_sent_ns;
};

struct dir_deleg_table {
	struct ddt_entry     **buckets;   /* [DDT_HASH_SIZE] */
	pthread_mutex_t        stripe_locks[DDT_STRIPE_COUNT];
	uint32_t               mds_id;
	_Atomic uint64_t       sid_counter;
	struct session_table  *st;        /* Borrowed; NULL if not attached. */

	/* Counters, exposed via dir_deleg_counters_snapshot.  All bumped
	 * with memory_order_relaxed; ordering across counters is not
	 * significant. */
	_Atomic uint64_t       cnt_granted;
	_Atomic uint64_t       cnt_recalled;
	_Atomic uint64_t       cnt_revoked;
	_Atomic uint64_t       cnt_returned;
	_Atomic uint64_t       cnt_conflict_unavail;
	_Atomic uint64_t       cnt_notify_sent;
	_Atomic uint64_t       cnt_notify_failed;
};

/* -----------------------------------------------------------------------
 * Hash helpers (same splitmix64 as delegation.c)
 * ----------------------------------------------------------------------- */

static uint32_t ddt_hash(uint64_t fileid)
{
	uint64_t h = fileid;

	h ^= h >> 30;
	h *= 0xbf58476d1ce4e5b9ULL;
	h ^= h >> 27;
	h *= 0x94d049bb133111ebULL;
	h ^= h >> 31;
	return (uint32_t)(h % DDT_HASH_SIZE);
}

static uint32_t stripe_for(uint64_t fileid)
{
	return ddt_hash(fileid) % DDT_STRIPE_COUNT;
}

static void lock_stripe(struct dir_deleg_table *ddt, uint64_t fileid)
{
	pthread_mutex_lock(&ddt->stripe_locks[stripe_for(fileid)]);
}

static void unlock_stripe(struct dir_deleg_table *ddt, uint64_t fileid)
{
	pthread_mutex_unlock(&ddt->stripe_locks[stripe_for(fileid)]);
}

/* -----------------------------------------------------------------------
 * Stateid generation
 *
 * Layout mirrors file-delegation stateids: 4-byte big-endian mds_id
 * followed by 8-byte big-endian counter.  Guarantees uniqueness
 * across the MDS and across delegations issued by the same MDS.
 * ----------------------------------------------------------------------- */

static void make_dir_deleg_stateid(struct dir_deleg_table *ddt,
				   struct nfs4_stateid *out)
{
	uint32_t mds_be = htobe32(ddt->mds_id);
	uint64_t seq_be = htobe64(atomic_fetch_add(&ddt->sid_counter, 1));

	memset(out, 0, sizeof(*out));
	out->seqid = 1;
	memcpy(out->other, &mds_be, 4);
	memcpy(out->other + 4, &seq_be, 8);
}

static void make_cookieverf(uint8_t out[NFS4_VERIFIER_SIZE])
{
	struct timespec now;

	/*
	 * cookieverf4 is opaque to the client; its only use is to invalidate
	 * a client's cached cookie cache when the directory mutates.  The
	 * current realtime clock with nanosecond precision gives us a
	 * monotonic-enough 8-byte opaque that changes on every grant.
	 */
	clock_gettime(CLOCK_REALTIME, &now);
	uint64_t verf = (uint64_t)now.tv_sec << 32 |
			(uint64_t)(now.tv_nsec & 0xFFFFFFFFu);
	uint64_t verf_be = htobe64(verf);

	memcpy(out, &verf_be, NFS4_VERIFIER_SIZE);
}

/* -----------------------------------------------------------------------
 * Entry lookup (caller holds stripe)
 * ----------------------------------------------------------------------- */

static struct ddt_entry *
find_by_dir_client_locked(const struct dir_deleg_table *ddt,
			  uint64_t dir_fileid, uint64_t clientid)
{
	struct ddt_entry *e;
	uint32_t bucket = ddt_hash(dir_fileid);

	for (e = ddt->buckets[bucket]; e != NULL; e = e->hash_next) {
		if (e->dir_fileid == dir_fileid && e->clientid == clientid) {
			return e;
		}
	}
	return NULL;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int dir_deleg_table_init(uint32_t mds_id, struct dir_deleg_table **out)
{
	struct dir_deleg_table *ddt;

	if (out == NULL) {
		return -1;
	}

	ddt = calloc(1, sizeof(*ddt));
	if (ddt == NULL) {
		return -1;
	}

	/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
	ddt->buckets = calloc(DDT_HASH_SIZE, sizeof(struct ddt_entry *));
	if (ddt->buckets == NULL) {
		free(ddt);
		return -1;
	}

	ddt->mds_id = mds_id;
	atomic_store(&ddt->sid_counter, 1);

	for (int i = 0; i < DDT_STRIPE_COUNT; i++) {
		pthread_mutex_init(&ddt->stripe_locks[i], NULL);
	}

	*out = ddt;
	return 0;
}

void dir_deleg_table_destroy(struct dir_deleg_table *ddt)
{
	uint32_t i;

	if (ddt == NULL) {
		return;
	}

	for (i = 0; i < DDT_HASH_SIZE; i++) {
		struct ddt_entry *e = ddt->buckets[i];

		while (e != NULL) {
			struct ddt_entry *next = e->hash_next;

			free(e);
			e = next;
		}
	}

	for (int s = 0; s < DDT_STRIPE_COUNT; s++) {
		pthread_mutex_destroy(&ddt->stripe_locks[s]);
	}

	/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
	free(ddt->buckets);
	free(ddt);
}

int dir_deleg_grant(struct dir_deleg_table *ddt,
		    uint64_t clientid, uint64_t dir_fileid,
		    uint32_t notification_mask,
		    struct nfs4_session *session,
		    struct nfs4_stateid *out_stateid,
		    uint8_t out_cookieverf[NFS4_VERIFIER_SIZE])
{
	struct ddt_entry *e;
	uint32_t bucket;

	if (ddt == NULL || out_stateid == NULL || out_cookieverf == NULL) {
		return -1;
	}

	e = calloc(1, sizeof(*e));
	if (e == NULL) {
		return -1;
	}

	make_dir_deleg_stateid(ddt, &e->stateid);
	make_cookieverf(e->cookieverf);
	e->clientid          = clientid;
	e->dir_fileid        = dir_fileid;
	e->notification_mask = notification_mask;
	e->session           = session;

	lock_stripe(ddt, dir_fileid);

	/*
	 * Same-client re-grant: replace the existing entry rather than
	 * keep duplicates.  The client sees a fresh cookieverf and
	 * stateid which is the correct response to a GDD replay.
	 */
	{
		struct ddt_entry *prev = find_by_dir_client_locked(
			ddt, dir_fileid, clientid);
		if (prev != NULL) {
			struct ddt_entry **pp = &ddt->buckets[ddt_hash(dir_fileid)];

			while (*pp != NULL && *pp != prev) {
				pp = &(*pp)->hash_next;
			}
			if (*pp == prev) {
				*pp = prev->hash_next;
			}
			free(prev);
			atomic_fetch_add_explicit(&ddt->cnt_revoked, 1,
						  memory_order_relaxed);
		}
	}

	bucket = ddt_hash(dir_fileid);
	e->hash_next = ddt->buckets[bucket];
	ddt->buckets[bucket] = e;

	unlock_stripe(ddt, dir_fileid);

	*out_stateid = e->stateid;
	memcpy(out_cookieverf, e->cookieverf, NFS4_VERIFIER_SIZE);

	atomic_fetch_add_explicit(&ddt->cnt_granted, 1,
				  memory_order_relaxed);
	return 0;
}

int dir_deleg_return(struct dir_deleg_table *ddt,
		     const struct nfs4_stateid *stateid,
		     uint64_t clientid)
{
	uint32_t i;

	if (ddt == NULL || stateid == NULL) {
		return -1;
	}

	/*
	 * The stateid is self-identifying but does not encode the
	 * dir_fileid, so we scan every bucket.  DELEGRETURN is a
	 * per-client, per-file op and is rare compared to mutation
	 * traffic; the scan cost is acceptable (cf. delegation.c
	 * deleg_return).
	 */
	for (i = 0; i < DDT_HASH_SIZE; i++) {
		uint32_t stripe = i % DDT_STRIPE_COUNT;

		pthread_mutex_lock(&ddt->stripe_locks[stripe]);

		struct ddt_entry **pp = &ddt->buckets[i];

		while (*pp != NULL) {
			struct ddt_entry *e = *pp;

			if (memcmp(e->stateid.other, stateid->other,
				   NFS4_OTHER_SIZE) == 0 &&
			    e->clientid == clientid) {
				*pp = e->hash_next;
				free(e);
				atomic_fetch_add_explicit(&ddt->cnt_returned,
							  1,
							  memory_order_relaxed);
				pthread_mutex_unlock(
					&ddt->stripe_locks[stripe]);
				return 0;
			}
			pp = &e->hash_next;
		}

		pthread_mutex_unlock(&ddt->stripe_locks[stripe]);
	}

	return -1;
}

bool dir_deleg_is_writer_present(struct dir_deleg_table *ddt,
				 uint64_t dir_fileid, uint64_t clientid)
{
	bool writer = false;
	uint32_t bucket;

	if (ddt == NULL) {
		return false;
	}

	lock_stripe(ddt, dir_fileid);
	bucket = ddt_hash(dir_fileid);
	for (struct ddt_entry *e = ddt->buckets[bucket]; e != NULL;
	     e = e->hash_next) {
		if (e->dir_fileid == dir_fileid && e->clientid != clientid) {
			writer = true;
			break;
		}
	}
	unlock_stripe(ddt, dir_fileid);
	return writer;
}

int dir_deleg_recall_dir(struct dir_deleg_table *ddt,
			 uint64_t dir_fileid, uint64_t requesting_clientid,
			 uint32_t timeout_ms)
{
	uint32_t bucket;
	struct ddt_entry **pp;
	int recalled = 0;

	if (ddt == NULL) {
		return -1;
	}
	if (timeout_ms == 0) {
		timeout_ms = ddt_default_timeout();
	}

	lock_stripe(ddt, dir_fileid);

	bucket = ddt_hash(dir_fileid);
	pp = &ddt->buckets[bucket];

	while (*pp != NULL) {
		struct ddt_entry *e = *pp;

		if (e->dir_fileid != dir_fileid ||
		    e->clientid == requesting_clientid) {
			pp = &e->hash_next;
			continue;
		}

		/* Best-effort CB_RECALL; suppress duplicates within the
		 * configured dedupe window (mirrors delegation.c). */
		if (e->session != NULL) {
			struct timespec nowts;
			uint64_t now_ns;
			bool suppress = false;

			clock_gettime(CLOCK_MONOTONIC, &nowts);
			now_ns = (uint64_t)nowts.tv_sec * 1000000000ULL +
				 (uint64_t)nowts.tv_nsec;

			if (e->recall_pending &&
			    (now_ns - e->recall_sent_ns) <
				    ddt_recall_pending_ns()) {
				suppress = true;
			}

			if (!suppress) {
				struct nfs4_cb_recall_args ra;
				int cbrc;

				memset(&ra, 0, sizeof(ra));
				ra.stateid  = e->stateid;
				ra.truncate = false;
				ra.fileid   = e->dir_fileid;
				e->recall_pending = true;
				e->recall_sent_ns = now_ns;
				cbrc = nfs4_cb_recall(e->session, &ra,
						      timeout_ms);
				if (cbrc != 0) {
					(void)fprintf(stderr,
						"dir_deleg: CB_RECALL "
						"dir=%llu client=%llu "
						"rc=%d \\u2014 revoking\n",
						(unsigned long long)e->dir_fileid,
						(unsigned long long)e->clientid,
						cbrc);
				}
			}
		}

		/* Revoke locally regardless of CB result: caller must
		 * be able to proceed with its mutation without the
		 * delegation still being observable. */
		*pp = e->hash_next;
		free(e);
		recalled++;
		atomic_fetch_add_explicit(&ddt->cnt_recalled, 1,
					  memory_order_relaxed);
		atomic_fetch_add_explicit(&ddt->cnt_revoked, 1,
					  memory_order_relaxed);
	}

	unlock_stripe(ddt, dir_fileid);
	return recalled;
}

void dir_deleg_revoke_client(struct dir_deleg_table *ddt, uint64_t clientid)
{
	uint32_t i;

	if (ddt == NULL) {
		return;
	}

	for (i = 0; i < DDT_HASH_SIZE; i++) {
		uint32_t stripe = i % DDT_STRIPE_COUNT;

		pthread_mutex_lock(&ddt->stripe_locks[stripe]);

		struct ddt_entry **pp = &ddt->buckets[i];

		while (*pp != NULL) {
			struct ddt_entry *e = *pp;

			if (e->clientid == clientid) {
				*pp = e->hash_next;
				free(e);
				atomic_fetch_add_explicit(&ddt->cnt_revoked,
							  1,
							  memory_order_relaxed);
			} else {
				pp = &e->hash_next;
			}
		}

		pthread_mutex_unlock(&ddt->stripe_locks[stripe]);
	}
}

bool dir_deleg_stateid_exists(const struct dir_deleg_table *ddt,
			      const uint8_t other[12])
{
	uint32_t i;

	if (ddt == NULL || other == NULL) {
		return false;
	}

	for (i = 0; i < DDT_HASH_SIZE; i++) {
		uint32_t stripe = i % DDT_STRIPE_COUNT;

		pthread_mutex_lock(
			&((struct dir_deleg_table *)ddt)->stripe_locks[stripe]);

		for (const struct ddt_entry *e = ddt->buckets[i]; e != NULL;
		     e = e->hash_next) {
			if (memcmp(e->stateid.other, other,
				   NFS4_OTHER_SIZE) == 0) {
				pthread_mutex_unlock(
					&((struct dir_deleg_table *)ddt)->stripe_locks[stripe]);
				return true;
			}
		}

		pthread_mutex_unlock(
			&((struct dir_deleg_table *)ddt)->stripe_locks[stripe]);
	}

	return false;
}

void dir_deleg_table_set_session_table(struct dir_deleg_table *ddt,
				       struct session_table *st)
{
	if (ddt != NULL) {
		ddt->st = st;
	}
}

void dir_deleg_counters_snapshot(const struct dir_deleg_table *ddt,
				 struct dir_deleg_counters *out)
{
	if (ddt == NULL || out == NULL) {
		if (out != NULL) {
			memset(out, 0, sizeof(*out));
		}
		return;
	}
	out->granted =
		atomic_load_explicit(&ddt->cnt_granted, memory_order_relaxed);
	out->recalled =
		atomic_load_explicit(&ddt->cnt_recalled, memory_order_relaxed);
	out->revoked =
		atomic_load_explicit(&ddt->cnt_revoked, memory_order_relaxed);
	out->returned =
		atomic_load_explicit(&ddt->cnt_returned, memory_order_relaxed);
	out->conflict_unavail = atomic_load_explicit(
		&ddt->cnt_conflict_unavail, memory_order_relaxed);
	out->notify_sent = atomic_load_explicit(
		&ddt->cnt_notify_sent, memory_order_relaxed);
	out->notify_failed = atomic_load_explicit(
		&ddt->cnt_notify_failed, memory_order_relaxed);
}

/* -----------------------------------------------------------------------
 * Notify-or-recall dispatcher
 *
 * Walks the bucket for dir_fileid, collects matching (other-client)
 * entries, then outside the stripe lock resolves each client's
 * callback metadata via session_for_each_with_cb and dispatches
 * CB_NOTIFY (if the mask covers the event) or CB_RECALL (otherwise).
 *
 * Locking rules are identical to layout_recall's pattern: never
 * perform I/O under the session-table lock; snapshot under the lock,
 * act outside.
 * ----------------------------------------------------------------------- */

struct ddt_emit_target {
	uint64_t             clientid;
	struct nfs4_stateid  stateid;
	uint32_t             granted_mask;
};

#define DDT_EMIT_MAX 32

struct ddt_emit_list {
	struct ddt_emit_target targets[DDT_EMIT_MAX];
	uint32_t               count;
};

struct ddt_cb_target {
	int      fd;
	uint8_t  session_id[SESSION_ID_SIZE];
	uint32_t cb_prog;
	uint32_t slot_seq_id;
	uint32_t num_cb_slots;
	uint32_t minorversion;
	struct nfs4_cb_sec cb_sec; /* RFC 8881 §2.10.8.3 */
};

struct ddt_cb_snap_ctx {
	uint64_t             want_clientid;
	struct ddt_cb_target target;
	bool                 found;
};

static int ddt_cb_snap_cb(const struct session_cb_snap *snap, void *ctx)
{
	struct ddt_cb_snap_ctx *c = ctx;
	int fd;
	int dup_fd;

	if (snap->clientid != c->want_clientid) {
		return 0;
	}
	fd = rpc_conn_get_fd(snap->cb_conn);
	if (fd < 0) {
		return 0;
	}
	dup_fd = dup(fd);
	if (dup_fd < 0) {
		return 0;
	}
	c->target.fd = dup_fd;
	memcpy(c->target.session_id, snap->session_id, SESSION_ID_SIZE);
	c->target.cb_prog = snap->cb_prog;
	c->target.slot_seq_id = snap->slot_seq_id;
	c->target.num_cb_slots = 1; /* session_cb_snap always exposes slot 0 */
	c->target.minorversion = snap->minorversion;
	c->target.cb_sec = snap->cb_sec;
	c->found = true;
	return 1; /* stop iteration */
}

/*
 * Resolve a single client's callback target.  Returns true and fills
 * @out if a dup'd fd was captured; otherwise returns false and the
 * caller must skip the CB and just revoke.  The caller owns out->fd
 * and must close it.
 */
static bool ddt_resolve_cb_target(struct session_table *st,
				  uint64_t clientid,
				  struct ddt_cb_target *out)
{
	struct ddt_cb_snap_ctx ctx;

	memset(&ctx, 0, sizeof(ctx));
	ctx.want_clientid = clientid;
	if (st == NULL) {
		return false;
	}
	(void)session_for_each_with_cb(st, ddt_cb_snap_cb, &ctx);
	if (!ctx.found) {
		return false;
	}
	*out = ctx.target;
	return true;
}

static bool event_is_structural(uint32_t event)
{
	return event == NOTIFY4_REMOVE_ENTRY ||
	       event == NOTIFY4_ADD_ENTRY ||
	       event == NOTIFY4_RENAME_ENTRY;
}

/*
 * Collect entries matching (dir_fileid, !requesting_clientid) under
 * the stripe lock and snapshot (clientid, stateid, granted_mask).
 * Entries that will be revoked (event not covered by mask or CB
 * failure) are unlinked later by the caller outside the lock.
 */
static void ddt_collect_entries(struct dir_deleg_table *ddt,
				uint64_t dir_fileid,
				uint64_t requesting_clientid,
				struct ddt_emit_list *out)
{
	uint32_t bucket = ddt_hash(dir_fileid);

	lock_stripe(ddt, dir_fileid);
	for (struct ddt_entry *e = ddt->buckets[bucket];
	     e != NULL && out->count < DDT_EMIT_MAX;
	     e = e->hash_next) {
		if (e->dir_fileid != dir_fileid ||
		    e->clientid == requesting_clientid) {
			continue;
		}
		out->targets[out->count].clientid = e->clientid;
		out->targets[out->count].stateid = e->stateid;
		out->targets[out->count].granted_mask = e->notification_mask;
		out->count++;
	}
	unlock_stripe(ddt, dir_fileid);
}

static void fill_notify_args(struct nfs4_cb_notify_args *args,
			     const struct ddt_emit_target *t,
			     uint64_t dir_fileid, uint32_t event,
			     const char *old_name, const char *new_name)
{
	memset(args, 0, sizeof(*args));
	args->stateid = t->stateid;
	args->dir_fileid = dir_fileid;
	args->notify_type = event;
	if (old_name != NULL) {
		size_t len = strlen(old_name);
		if (len > sizeof(args->old_name)) {
			len = sizeof(args->old_name);
		}
		memcpy(args->old_name, old_name, len);
		args->old_name_len = (uint32_t)len;
	}
	if (new_name != NULL) {
		size_t len = strlen(new_name);
		if (len > sizeof(args->new_name)) {
			len = sizeof(args->new_name);
		}
		memcpy(args->new_name, new_name, len);
		args->new_name_len = (uint32_t)len;
	}
}

int dir_deleg_notify_dir(struct dir_deleg_table *ddt,
			 uint64_t dir_fileid,
			 uint64_t requesting_clientid,
			 uint32_t event,
			 const char *old_name,
			 const char *new_name,
			 uint32_t timeout_ms)
{
	struct ddt_emit_list list;
	int serviced = 0;

	if (ddt == NULL) {
		return -1;
	}
	if (!event_is_structural(event)) {
		return -1;
	}
	if (timeout_ms == 0) {
		timeout_ms = ddt_default_timeout();
	}

	memset(&list, 0, sizeof(list));
	ddt_collect_entries(ddt, dir_fileid, requesting_clientid, &list);

	for (uint32_t i = 0; i < list.count; i++) {
		const struct ddt_emit_target *t = &list.targets[i];
		bool mask_covers = (t->granted_mask &
				    ((uint32_t)1u << event)) != 0;
		bool notified = false;

		if (mask_covers) {
			struct ddt_cb_target cbt;

			if (ddt_resolve_cb_target(ddt->st, t->clientid,
						  &cbt)) {
				struct nfs4_cb_notify_args args;
				int rc;

				fill_notify_args(&args, t, dir_fileid,
						 event, old_name, new_name);
				rc = nfs4_cb_notify_fd(cbt.fd,
						       cbt.session_id,
						       cbt.cb_prog,
						       cbt.slot_seq_id,
						       cbt.num_cb_slots,
						       cbt.minorversion,
						       &cbt.cb_sec,
						       &args,
						       timeout_ms);
				close(cbt.fd);
				if (rc == 0) {
					atomic_fetch_add_explicit(
						&ddt->cnt_notify_sent,
						1, memory_order_relaxed);
					notified = true;
					serviced++;
				} else {
					atomic_fetch_add_explicit(
						&ddt->cnt_notify_failed,
						1, memory_order_relaxed);
				}
			}
		}

		if (!notified) {
			/*
			 * Event not covered by the mask, or CB_NOTIFY
			 * delivery failed.  Fall back to recall: the
			 * existing single-entry path on dir_fileid
			 * scoped to this client's clientid.
			 */
			(void)dir_deleg_recall_dir(ddt, dir_fileid,
						   requesting_clientid,
						   timeout_ms);
			serviced++;
			/*
			 * dir_deleg_recall_dir revokes every non-req
			 * grant on the dir in one go, so the outer loop
			 * may visit entries that have already vanished;
			 * that is a harmless no-op (ddt_collect_entries
			 * captured a stateid snapshot, the recall walks
			 * by fileid so it will find the entry only if it
			 * is still present).
			 */
			break;
		}
	}

	return serviced;
}

/*
 * Internal helper invoked by the GDD handler when the conflict gate
 * forces an UNAVAIL reply.  Exposed here (not in the header) so only
 * compound.c's handler can bump the metric without exposing the
 * private struct.
 */
void dir_deleg_count_conflict_unavail(struct dir_deleg_table *ddt);
void dir_deleg_count_conflict_unavail(struct dir_deleg_table *ddt)
{
	if (ddt == NULL) {
		return;
	}
	atomic_fetch_add_explicit(&ddt->cnt_conflict_unavail, 1,
				  memory_order_relaxed);
}

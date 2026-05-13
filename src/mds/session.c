/*
 * Copyright (c) 2026 PeakAIO. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-PeakAIO-Proprietary
 *
 * session.c — NFSv4.1 session and clientid management.
 *
 * Implements EXCHANGE_ID, CREATE_SESSION, DESTROY_SESSION, and
 * SEQUENCE operations per RFC 8881 §§18.35–18.37, 18.46.
 *
 * Data structures:
 *   - Client hash table: chained, indexed by clientid.
 *   - Session hash table: chained, indexed by session_id.
 *
 * Thread safety: all operations hold st->lock for the duration.
 * This is sufficient for Phase 1 (single-threaded dispatch).
 *
 * Client_recovery persistence is deferred to Phase 6
 * (grace period).  Currently all state is in-memory only.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>
#include <endian.h>
#include <time.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "session.h"
#include "lock_state.h"
#include "grace.h"
#include "commit_queue.h"
#include "mds_shard.h"
#include "mds_op_metrics.h"
#include <stdbool.h>

/* -----------------------------------------------------------------------
 * Hash table sizing
 * ----------------------------------------------------------------------- */

#define CLIENT_HASH_BUCKETS   256
#define SESSION_HASH_BUCKETS  256
#define OWNER_HASH_BUCKETS    256

/* -----------------------------------------------------------------------
 * Session table (opaque type from session.h)
 * ----------------------------------------------------------------------- */

struct session_table {
	struct nfs4_client  **client_hash;
	struct nfs4_client  **owner_hash;  /* Secondary: by co_ownerid */
	struct nfs4_session **session_hash;
	uint64_t             next_clientid;
	uint64_t             next_session_seq;
	uint32_t             mds_id;
	uint32_t             lease_time_sec;
	pthread_mutex_t      locks[16]; /* striped by clientid */
	struct commit_queue  *cq;    /* Optional: routes recovery writes through repl pipeline */
	struct mds_catalogue *cat;   /* Optional: catalogue vtable fallback for RonDB */
	/* Lease expiry reaper (R2.2). */
	pthread_t             reaper_tid;
	_Atomic int           reaper_running;
	struct open_state_table *ot; /* For cleanup on expiry */
	struct lock_table *lt; /* For cleanup on expiry */
};

/* -----------------------------------------------------------------------
 * Hash functions
 *
 * session_id_shard: select a lock shard from a session_id.
 * Used for cache_reply, get_cached_reply, and sequence_check
 * to reduce contention on locks[0].
 */
static uint32_t session_id_shard(
	const uint8_t session_id[SESSION_ID_SIZE])
{
	/* FNV-1a hash of the first 8 bytes of session_id. */
	uint32_t h = 2166136261u;
	for (uint32_t i = 0; i < 8 && i < SESSION_ID_SIZE; i++) {
		h ^= session_id[i];
		h *= 16777619u;
	}
	return h % 16;
}

/*
 * client_hash:  index by clientid (low bits after mixing)
 * session_hash: index by first 8 bytes of session_id
 * ----------------------------------------------------------------------- */

static uint32_t hash_clientid(uint64_t clientid)
{
	uint64_t h = clientid;

	h ^= h >> 33;
	h *= 0xff51afd7ed558ccdULL;
	h ^= h >> 33;
	return (uint32_t)(h % CLIENT_HASH_BUCKETS);
}

static uint32_t hash_session_id(const uint8_t sid[SESSION_ID_SIZE])
{
	uint64_t v;

	memcpy(&v, sid, sizeof(v));
	v ^= v >> 33;
	v *= 0xff51afd7ed558ccdULL;
	v ^= v >> 33;
	return (uint32_t)(v % SESSION_HASH_BUCKETS);
}

/** FNV-1a hash of co_ownerid for O(1) owner lookup. */
static uint32_t hash_ownerid(const uint8_t *data, uint32_t len)
{
	uint32_t h = 2166136261u;
	for (uint32_t i = 0; i < len; i++) {
		h ^= data[i];
		h *= 16777619u;
	}
	return h % OWNER_HASH_BUCKETS;
}

/* -----------------------------------------------------------------------
 * Internal: find client by clientid
 * ----------------------------------------------------------------------- */

static struct nfs4_client *find_client_by_id(const struct session_table *st,
					     uint64_t clientid)
{
	uint32_t idx = hash_clientid(clientid);
	struct nfs4_client *c;

	for (c = st->client_hash[idx]; c != NULL; c = c->hash_next) {
		if (c->clientid == clientid) {
			return c;
}
	}
	return NULL;
}

/* -----------------------------------------------------------------------
 * Internal: find client by co_ownerid — O(1) via owner_hash
 * ----------------------------------------------------------------------- */

static struct nfs4_client *find_client_by_owner(const struct session_table *st,
						const uint8_t *co_ownerid,
						uint32_t co_ownerid_len)
{
	uint32_t idx = hash_ownerid(co_ownerid, co_ownerid_len);
	struct nfs4_client *c;

	for (c = st->owner_hash[idx]; c != NULL;
	     c = c->owner_hash_next) {
		if (c->co_ownerid_len == co_ownerid_len &&
		    memcmp(c->co_ownerid, co_ownerid,
			   co_ownerid_len) == 0) {
			return c;
		}
	}
	return NULL;
}

/* -----------------------------------------------------------------------
 * Internal: find session by session_id
 * ----------------------------------------------------------------------- */

static struct nfs4_session *find_session(const struct session_table *st,
					 const uint8_t sid[SESSION_ID_SIZE])
{
	uint32_t idx = hash_session_id(sid);
	struct nfs4_session *s;

	for (s = st->session_hash[idx]; s != NULL; s = s->hash_next) {
		if (memcmp(s->session_id, sid, SESSION_ID_SIZE) == 0) {
			return s;
}
	}
	return NULL;
}

/* -----------------------------------------------------------------------
 * Internal: free a single session (does not unlink from hash/client)
 * ----------------------------------------------------------------------- */

static void free_session(struct nfs4_session *s)
{
	if (s == NULL) {
		return;
}
	free(s->cb_slots);
	free(s->slots);
	free(s);
}

/* -----------------------------------------------------------------------
 * Internal: remove client from hash table
 * ----------------------------------------------------------------------- */

static void unhash_client(struct session_table *st, struct nfs4_client *c)
{
	/* Remove from clientid hash. */
	{
		uint32_t idx = hash_clientid(c->clientid);
		struct nfs4_client **pp;

		for (pp = &st->client_hash[idx]; *pp != NULL;
		     pp = &(*pp)->hash_next) {
			if (*pp == c) {
				*pp = c->hash_next;
				break;
			}
		}
	}
	/* Remove from owner hash. */
	{
		uint32_t idx = hash_ownerid(c->co_ownerid,
					   c->co_ownerid_len);
		struct nfs4_client **pp;

		for (pp = &st->owner_hash[idx]; *pp != NULL;
		     pp = &(*pp)->owner_hash_next) {
			if (*pp == c) {
				*pp = c->owner_hash_next;
				break;
			}
		}
	}
}

/* -----------------------------------------------------------------------
 * Internal: remove session from hash table
 * ----------------------------------------------------------------------- */

static void unhash_session(struct session_table *st, struct nfs4_session *s)
{
	uint32_t idx = hash_session_id(s->session_id);
	struct nfs4_session **pp;

	for (pp = &st->session_hash[idx]; *pp != NULL;
	     pp = &(*pp)->hash_next) {
		if (*pp == s) {
			*pp = s->hash_next;
			return;
		}
	}
}

/* -----------------------------------------------------------------------
 * Internal: destroy all sessions for a client
 * ----------------------------------------------------------------------- */

static void destroy_client_sessions(struct session_table *st,
				    struct nfs4_client *c)
{
	struct nfs4_session *s;
	struct nfs4_session *next;

	for (s = c->sessions; s != NULL; s = next) {
		next = s->client_next;
		unhash_session(st, s);
		free_session(s);
	}
	c->sessions = NULL;
}

/* -----------------------------------------------------------------------
 * Internal: free a client and all its sessions
 * ----------------------------------------------------------------------- */

static void free_client(struct session_table *st, struct nfs4_client *c)
{
	if (c == NULL) {
		return;
}
	/* Delete recovery record on client destruction.
	 * Priority: CQ (replicated) > catalogue vtable. */
	if (st->cq != NULL && c->confirmed) {
		struct commit_op cop;
		memset(&cop, 0, sizeof(cop));
		cop.type = COMMIT_OP_RECOVERY_DEL;
		cop.args.recovery_del.clientid = c->clientid;
		(void)commit_queue_submit(st->cq, &cop);
	} else if (st->cat != NULL && c->confirmed) {
		(void)mds_coord_recovery_del(st->cat, NULL, c->clientid);
	}
	destroy_client_sessions(st, c);
	free(c);
}

/* -----------------------------------------------------------------------
 * Internal: generate a session ID
 *
 * Layout: [mds_id 4B BE][session_counter 8B BE][0-pad 4B]
 * ----------------------------------------------------------------------- */

static void make_session_id(struct session_table *st,
			    uint8_t out[SESSION_ID_SIZE])
{
	uint32_t mds_be = htobe32(st->mds_id);
	uint64_t seq_be = htobe64(st->next_session_seq++);

	memset(out, 0, SESSION_ID_SIZE);
	memcpy(out, &mds_be, 4);
	memcpy(out + 4, &seq_be, 8);
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int session_table_init(uint32_t mds_id, uint32_t lease_time_sec,
		       struct session_table **out)
{
	struct session_table *st;

	if (out == NULL) {
		return -1;
}

	st = calloc(1, sizeof(*st));
	if (st == NULL) {
		return -1;
}

	/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
	st->client_hash = calloc(CLIENT_HASH_BUCKETS,
				 sizeof(struct nfs4_client *));
	/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
	st->owner_hash = calloc(OWNER_HASH_BUCKETS,
				 sizeof(struct nfs4_client *));
	if (st->client_hash == NULL || st->owner_hash == NULL) {
		/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
		free(st->client_hash);
		/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
		free(st->owner_hash);
		free(st);
		return -1;
	}

	/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
	st->session_hash = calloc(SESSION_HASH_BUCKETS,
				  sizeof(struct nfs4_session *));
	if (st->session_hash == NULL) {
		/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
		free(st->client_hash);
		/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
		free(st->owner_hash);
		free(st);
		return -1;
	}

	st->mds_id = mds_id;
	st->next_clientid = ((uint64_t)mds_id << 48) | 1;
	st->next_session_seq = 1;
	st->lease_time_sec = (lease_time_sec > 0)
		? lease_time_sec : SESSION_DEFAULT_LEASE_SEC;
	for (uint32_t li = 0; li < 16; li++) {
		pthread_mutex_init(&st->locks[li], NULL);
	}

	*out = st;
	return 0;
}

void session_table_set_cq(struct session_table *st, struct commit_queue *cq)
{
	if (st != NULL) {
		st->cq = cq;
	}
}

void session_table_set_cat(struct session_table *st,
			   struct mds_catalogue *cat)
{
	if (st != NULL) {
		st->cat = cat;
	}
}

void session_table_set_shard(struct session_table *st,
			     const struct mds_shard *shard)
{
	if (st == NULL) {
		return;
	}
	if (shard != NULL) {
		st->cq = shard->cq;
	} else {
		st->cq = NULL;
	}
}

void session_table_destroy(struct session_table *st)
{
	uint32_t i;

	if (st == NULL) {
		return;
}

	for (i = 0; i < CLIENT_HASH_BUCKETS; i++) {
		struct nfs4_client *c = st->client_hash[i];
		struct nfs4_client *next;

		while (c != NULL) {
			next = c->hash_next;
			free_client(st, c);
			c = next;
		}
	}

	for (uint32_t li = 0; li < 16; li++) {
		pthread_mutex_destroy(&st->locks[li]);
	}
	/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
	free(st->session_hash);
	/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
	free(st->client_hash);
	/* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
	free(st->owner_hash);
	free(st);
}

/* ----------------------------------------------------------------------- */

/* -----------------------------------------------------------------------
 * Grace-period recovery scan callback (RonDB path).
 *
 * Called by mds_coord_recovery_list for each persisted recovery record.
 * For each clientid, fetches the full record via mds_coord_recovery_get
 * and compares co_ownerid + verifier.
 * ----------------------------------------------------------------------- */

struct grace_recovery_ctx {
	struct mds_catalogue *cat;
	const uint8_t *co_ownerid;
	uint32_t co_ownerid_len;
	const uint8_t *verifier;
	uint64_t recovered_clientid;
	bool found;
};

static int grace_recovery_scan_cb(uint64_t clientid,
				  uint32_t owner_mds_id,
				  uint64_t owner_boot_epoch,
				  void *arg)
{
	struct grace_recovery_ctx *ctx = arg;
	uint8_t co_buf[1024];
	uint32_t co_len = 0;
	uint8_t ver[8];

	(void)owner_mds_id;
	(void)owner_boot_epoch;

	if (mds_coord_recovery_get(ctx->cat, clientid,
				   co_buf, &co_len, ver) != MDS_OK) {
		return 0; /* skip — record vanished or error */
	}

	if (co_len == ctx->co_ownerid_len &&
	    memcmp(co_buf, ctx->co_ownerid, co_len) == 0 &&
	    memcmp(ver, ctx->verifier, NFS4_VERIFIER_SIZE) == 0) {
		ctx->recovered_clientid = clientid;
		ctx->found = true;
		return 1; /* stop scan */
	}
	return 0;
}

/*
 * Principal-match helper for RFC 8881 §18.35.4 cases 8 / 9 and case 2.
 *
 * When auth_flavor == 0 either side, principal matching is suppressed
 * (legacy / unit-test path).  Otherwise the (flavor, uid, gid) triple
 * must match exactly.  AUTH_SYS is the only protected flavor we
 * currently capture; GSS principals are deferred to a future change.
 */
static bool client_principal_matches(const struct nfs4_client *c,
				     uint32_t auth_flavor,
				     uint32_t cred_uid,
				     uint32_t cred_gid)
{
	if (c->auth_flavor == 0 || auth_flavor == 0) {
		return true;
	}
	return c->auth_flavor == auth_flavor &&
	       c->cred_uid    == cred_uid    &&
	       c->cred_gid    == cred_gid;
}

/*
 * Allocate a fresh client record under the session-table lock.
 * Caller already holds st->locks[0].  Returns SESSION_EID_OK on
 * success and writes the new clientid/seqid/flags via the out_*
 * pointers; SESSION_EID_RESOURCE on allocation failure.
 */
static int session_alloc_new_client(struct session_table *st,
				    const uint8_t *co_ownerid,
				    uint32_t co_ownerid_len,
				    const uint8_t verifier[NFS4_VERIFIER_SIZE],
				    uint32_t auth_flavor,
				    uint32_t cred_uid,
				    uint32_t cred_gid,
				    uint64_t *out_clientid,
				    uint32_t *out_seqid,
				    uint32_t *out_flags)
{
	struct nfs4_client *c;
	uint64_t recovered_clientid = 0;
	bool use_recovered_id = false;

	/* Post-failover recovery: if we are in grace and have a
	 * persisted recovery record that matches this co_ownerid,
	 * re-create the client with its original clientid so that
	 * subsequent CLAIM_PREVIOUS opens resolve correctly. */
	if (st->cat != NULL && grace_is_active()) {
		struct grace_recovery_ctx grc;
		grc.cat = st->cat;
		grc.co_ownerid = co_ownerid;
		grc.co_ownerid_len = co_ownerid_len;
		grc.verifier = verifier;
		grc.found = false;
		(void)mds_coord_recovery_list(st->cat, st->mds_id,
			grace_recovery_scan_cb, &grc);
		if (grc.found) {
			recovered_clientid = grc.recovered_clientid;
			use_recovered_id = true;
		}
	}

	c = calloc(1, sizeof(*c));
	if (c == NULL) {
		return SESSION_EID_RESOURCE;
	}

	if (use_recovered_id) {
		c->clientid = recovered_clientid;
		if (st->next_clientid <= recovered_clientid) {
			st->next_clientid = recovered_clientid + 1;
		}
	} else {
		c->clientid = st->next_clientid++;
	}
	memcpy(c->co_ownerid, co_ownerid, co_ownerid_len);
	c->co_ownerid_len = co_ownerid_len;
	memcpy(c->verifier, verifier, NFS4_VERIFIER_SIZE);
	c->confirmed = false;
	c->create_seq = 1;
	c->last_renewed = time(NULL);
	c->auth_flavor = auth_flavor;
	c->cred_uid = cred_uid;
	c->cred_gid = cred_gid;
	c->sessions = NULL;
	c->hash_next = NULL;
	c->owner_hash_next = NULL;

	{
		uint32_t idx = hash_clientid(c->clientid);
		c->hash_next = st->client_hash[idx];
		st->client_hash[idx] = c;
	}
	{
		uint32_t idx = hash_ownerid(c->co_ownerid, c->co_ownerid_len);
		c->owner_hash_next = st->owner_hash[idx];
		st->owner_hash[idx] = c;
	}

	*out_clientid = c->clientid;
	if (out_seqid != NULL) {
		*out_seqid = c->create_seq;
	}
	if (out_flags != NULL) {
		*out_flags = EXCHGID4_FLAG_USE_PNFS_MDS |
			     EXCHGID4_FLAG_SUPP_MOVED_REFER;
	}
	return SESSION_EID_OK;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int session_exchange_id(struct session_table *st,
			const uint8_t *co_ownerid,
			uint32_t co_ownerid_len,
			const uint8_t verifier[NFS4_VERIFIER_SIZE],
			uint32_t eia_flags,
			uint64_t *out_clientid,
			uint32_t *out_seqid,
			uint32_t *out_flags,
			uint32_t auth_flavor,
			uint32_t cred_uid,
			uint32_t cred_gid)
{
	struct nfs4_client *c;
	bool update;
	bool verf_match;
	bool princ_match;
	int rc;

	if (st == NULL || co_ownerid == NULL || out_clientid == NULL) {
		return SESSION_EID_RESOURCE;
	}
	if (co_ownerid_len == 0 || co_ownerid_len > CO_OWNERID_MAX) {
		return SESSION_EID_RESOURCE;
	}

	update = (eia_flags & EXCHGID4_FLAG_UPD_CONFIRMED_REC_A) != 0;

	pthread_mutex_lock(&st->locks[0]);

	c = find_client_by_owner(st, co_ownerid, co_ownerid_len);

	/*
	 * RFC 8881 §18.35.4 — UPDATE branch.
	 *
	 * The UPDATE flag asserts "there is already a confirmed record
	 * for this co_ownerid; refresh it".  If no record exists, or the
	 * record exists but is unconfirmed, the request fails with
	 * NFS4ERR_NOENT.  Pynfs EID6 / EID6a-d.
	 *
	 * For a confirmed record we then validate verifier and principal:
	 *   verifier mismatch                 → NFS4ERR_NOT_SAME (case 8).
	 *   verifier match + princ mismatch   → NFS4ERR_PERM     (case 9).
	 *   both match                        → case 6, return existing.
	 */
	if (update) {
		if (c == NULL || !c->confirmed) {
			rc = SESSION_EID_NOENT;
			goto out;
		}
		verf_match  = memcmp(c->verifier, verifier,
				     NFS4_VERIFIER_SIZE) == 0;
		princ_match = client_principal_matches(c, auth_flavor,
						       cred_uid, cred_gid);
		if (!verf_match) {
			rc = SESSION_EID_NOT_SAME;
			goto out;
		}
		if (!princ_match) {
			rc = SESSION_EID_PERM;
			goto out;
		}
		/* Case 6 — verifier + principal match, return existing. */
		c->last_renewed = time(NULL);
		*out_clientid = c->clientid;
		if (out_seqid != NULL) {
			*out_seqid = c->create_seq;
		}
		if (out_flags != NULL) {
			*out_flags = EXCHGID4_FLAG_USE_PNFS_MDS |
				     EXCHGID4_FLAG_SUPP_MOVED_REFER;
		}
		rc = SESSION_EID_OK;
		goto out;
	}

	/*
	 * RFC 8881 §18.35.4 — non-UPDATE branch.
	 *
	 * Case 1 (no record)               → fresh allocation.
	 * Case 4 (unconfirmed record)      → record is replaced unconditionally;
	 *                                    a fresh clientid is minted.
	 * Case 2 (confirmed + verf + princ)→ renewal, return existing clientid.
	 * Cases 3/5/7 (confirmed, mismatch)→ record is replaced; a fresh
	 *                                    clientid is minted.  We do not
	 *                                    yet preserve the old record
	 *                                    until CREATE_SESSION confirms
	 *                                    the replacement (case 5 wire
	 *                                    behaviour); pynfs EID5f covers
	 *                                    that path and is left for a
	 *                                    follow-up commit.
	 */
	if (c == NULL) {
		rc = session_alloc_new_client(st, co_ownerid, co_ownerid_len,
					      verifier, auth_flavor,
					      cred_uid, cred_gid,
					      out_clientid, out_seqid,
					      out_flags);
		goto out;
	}

	verf_match  = memcmp(c->verifier, verifier,
			     NFS4_VERIFIER_SIZE) == 0;
	princ_match = client_principal_matches(c, auth_flavor,
					       cred_uid, cred_gid);

	if (c->confirmed && verf_match && princ_match) {
		/* Case 2 — renewal, return existing clientid. */
		c->last_renewed = time(NULL);
		*out_clientid = c->clientid;
		if (out_seqid != NULL) {
			*out_seqid = c->create_seq;
		}
		if (out_flags != NULL) {
			*out_flags = EXCHGID4_FLAG_USE_PNFS_MDS |
				     EXCHGID4_FLAG_SUPP_MOVED_REFER;
		}
		rc = SESSION_EID_OK;
		goto out;
	}

	/*
	 * Cases 3 / 4 / 5 / 7: replace the existing record.
	 *
	 * RFC 8881 §18.35.4 case 5 (confirmed + verf mismatch +
	 * principal match): the old confirmed record and its
	 * sessions MUST survive until the replacement is confirmed
	 * by CREATE_SESSION.  Pynfs EID5f drives this: sess1 must
	 * still work after EXCHANGE_ID mints c2, and only become
	 * BADSESSION after c2's CREATE_SESSION confirms.
	 *
	 * For case 5 we keep c alive (in client_hash and
	 * session_hash) and only remove it from owner_hash so the
	 * new client takes over the co_ownerid slot.  The old
	 * clientid is recorded on the new client as
	 * superseded_clientid and destroyed inside
	 * session_create_session when the replacement confirms.
	 *
	 * Cases 3/7 (principal mismatch or unconfirmed) and case 4
	 * (unconfirmed) destroy the old record immediately — there
	 * are no live sessions to protect.
	 */
	{
		bool defer_old = (c->confirmed && princ_match);
		uint64_t old_clientid = c->clientid;

		if (defer_old) {
			/* Case 5: remove from owner_hash only. */
			uint32_t oidx = hash_ownerid(c->co_ownerid,
						    c->co_ownerid_len);
			struct nfs4_client **pp;
			for (pp = &st->owner_hash[oidx]; *pp != NULL;
			     pp = &(*pp)->owner_hash_next) {
				if (*pp == c) {
					*pp = c->owner_hash_next;
					break;
				}
			}
		} else {
			unhash_client(st, c);
			free_client(st, c);
			old_clientid = 0;
		}
		c = NULL;

		rc = session_alloc_new_client(st, co_ownerid,
					      co_ownerid_len,
					      verifier, auth_flavor,
					      cred_uid, cred_gid,
					      out_clientid, out_seqid,
					      out_flags);
		/* Record superseded clientid for deferred destroy
		 * in session_create_session. */
		if (rc == SESSION_EID_OK && old_clientid != 0) {
			struct nfs4_client *nc =
				find_client_by_id(st, *out_clientid);
			if (nc != NULL) {
				nc->superseded_clientid = old_clientid;
			}
		}
	}

out:
	pthread_mutex_unlock(&st->locks[0]);
	return rc;
}

/* ----------------------------------------------------------------------- */

int session_create_session(struct session_table *st,
			   uint64_t clientid,
			   uint32_t seqid,
			   uint32_t fore_slots,
			   uint32_t back_slots,
			   uint32_t cb_prog,
			   uint32_t cb_sec_flavor,
			   uint32_t fore_max_request_size,
			   uint32_t fore_max_operations,
			   uint32_t a_fore_max_response_size,
			   uint32_t a_fore_max_response_size_cached,
			   uint32_t minorversion,
			   uint32_t auth_flavor,
			   uint32_t cred_uid,
			   uint32_t cred_gid,
			   uint8_t out_session_id[SESSION_ID_SIZE],
			   uint32_t *out_fore_slots,
			   uint32_t *out_back_slots,
			   uint32_t *out_fore_max_request_size,
			   uint32_t *out_fore_max_operations)
{
	struct nfs4_client *c;
	struct nfs4_session *s = NULL;
	uint32_t actual_fore;
	uint32_t actual_back;
	uint32_t actual_max_req;
	uint32_t actual_max_ops;
	/* Server hard caps that match what encode_res_create_session
	 * advertises in the wire reply (1 MiB / NFS4_MAX_OPS).  These
	 * are the absolute upper bounds; the negotiated value is
	 * MIN(client_request, server_pref).  We DO NOT enforce a lower
	 * floor: pynfs SEQ6 sets ca_maxrequestsize=512 and SEQ7 sets
	 * ca_maxoperations to a small N, and both tests rely on the
	 * server accepting and enforcing those small values verbatim. */
	const uint32_t SERVER_MAX_REQUEST_SIZE = 1048576U;
	/* Mirror NFS4_MAX_OPS from xdr_codec.h.  Inlined as a literal
	 * to keep session.c free of XDR-layer header dependencies; if
	 * NFS4_MAX_OPS ever changes, both sites must be updated. */
	const uint32_t SERVER_MAX_OPERATIONS   = 64U;
	int rc = 0;

	if (st == NULL || out_session_id == NULL) {
		return -1;
}

	pthread_mutex_lock(&st->locks[0]);

	c = find_client_by_id(st, clientid);
	if (c == NULL) {
		rc = -1;  /* NFS4ERR_STALE_CLIENTID */
		goto out;
	}

	/*
	 * RFC 8881 §18.35.4: "Any unconfirmed record that is
	 * not confirmed within a lease period SHOULD be removed."
	 * Check on-demand so CREATE_SESSION on an expired
	 * unconfirmed clientid returns NFS4ERR_STALE_CLIENTID
	 * regardless of the background reaper's scan interval.
	 * Pynfs EID9 (testLeasePeriod) drives this path.
	 */
	if (!c->confirmed) {
		time_t now = time(NULL);
		if (now > c->last_renewed &&
		    (uint32_t)(now - c->last_renewed) >= st->lease_time_sec) {
			unhash_client(st, c);
			free_client(st, c);
			rc = -1;  /* NFS4ERR_STALE_CLIENTID */
			goto out;
		}
	}

	/*
	 * RFC 8881 §18.36.4: principal collision check.  When the
	 * client record is unconfirmed and the CREATE_SESSION caller's
	 * principal differs from the EXCHANGE_ID caller's, another
	 * host is trying to confirm a clientid it didn't create.
	 * Return NFS4ERR_CLID_INUSE.  Pynfs CSESS9.
	 */
	if (!c->confirmed &&
	    !client_principal_matches(c, auth_flavor, cred_uid, cred_gid)) {
		rc = -4;  /* NFS4ERR_CLID_INUSE */
		goto out;
	}

	/*
	 * Sequence ID check: RFC 8881 §18.36.4.
	 *
	 * create_seq is the NEXT expected seqid.  After a successful
	 * CREATE_SESSION it is incremented, so replay detection fires
	 * when seqid == create_seq - 1.  RFC 8881 §18.36.4:
	 * "If csa_sequenceid is equal to the sequence id in the
	 * client ID's slot, then this is a replay, and the server
	 * returns the cached result."
	 */
	if (seqid == c->create_seq) {
		/* New request — proceed normally. */
	} else if (c->last_cs_valid &&
		   seqid == c->create_seq - 1) {
		/* Replay — return cached result per RFC 8881 §18.36.4.
		 * Pynfs CSESS5 / CSESS5a / CSESS5b. */
		memcpy(out_session_id, c->last_cs_session_id,
		       SESSION_ID_SIZE);
		if (out_fore_slots != NULL) {
			*out_fore_slots = c->last_cs_fore_slots;
		}
		if (out_back_slots != NULL) {
			*out_back_slots = c->last_cs_back_slots;
		}
		if (out_fore_max_request_size != NULL) {
			*out_fore_max_request_size =
				c->last_cs_fore_max_request_size;
		}
		if (out_fore_max_operations != NULL) {
			*out_fore_max_operations =
				c->last_cs_fore_max_operations;
		}
		rc = 0;  /* NFS4_OK — replay returns the original result */
		goto out;
	} else {
		rc = -2;  /* NFS4ERR_SEQ_MISORDERED */
		goto out;
	}

	/* Negotiate slot counts. */
	actual_fore = (fore_slots > SESSION_MAX_SLOTS)
		? SESSION_MAX_SLOTS : fore_slots;
	if (actual_fore == 0) {
		actual_fore = 1;
}

	actual_back = (back_slots > SESSION_MAX_SLOTS)
		? SESSION_MAX_SLOTS : back_slots;

	/* Negotiate forechannel attrs: MIN(client_request, server_pref).
	 * A zero-valued client request is treated as "use server max".
	 * Any positive client value within (0, SERVER_MAX_*] is honored
	 * verbatim so SEQ6 (small ca_maxrequestsize) and SEQ7 (small
	 * ca_maxoperations) can drive the rpc-server enforcement path. */
	actual_max_req = fore_max_request_size;
	if (actual_max_req == 0U || actual_max_req > SERVER_MAX_REQUEST_SIZE) {
		actual_max_req = SERVER_MAX_REQUEST_SIZE;
	}
	actual_max_ops = fore_max_operations;
	if (actual_max_ops == 0U || actual_max_ops > SERVER_MAX_OPERATIONS) {
		actual_max_ops = SERVER_MAX_OPERATIONS;
	}

	/* Allocate session. */
	s = calloc(1, sizeof(*s));
	if (s == NULL) {
		rc = -3;
		goto out;
	}

	s->slots = calloc(actual_fore, sizeof(struct nfs4_slot));
	if (s->slots == NULL) {
		free(s);
		rc = -3;
		goto out;
	}

	/* RFC 8881 §2.10.6.1.2: slots start at seq_id 0.
	 * Linux kernel 6.8 sends seq_id=1 as the first SEQUENCE
	 * (slot->seq_nr starts at 0, kernel sends seq_nr+1).
	 * With seq_id=0, the check (1 == 0+1) accepts it. */
	s->num_slots = actual_fore;
	s->clientid = clientid;
	s->minorversion = minorversion;
	s->max_request_size = actual_max_req;
	{
		/* Negotiated ca_maxresponsesize / ca_maxresponsesizecached.
		 * Use the raw arg values decoded from the wire.  Zero or
		 * overlarge values default to 1 MiB / 64 KiB. */
		uint32_t mr = a_fore_max_response_size;
		uint32_t mrc = a_fore_max_response_size_cached;
		if (mr == 0U || mr > 1048576U) { mr = 1048576U; }
		if (mrc == 0U || mrc > 1048576U) { mrc = 65536U; }
		s->max_response_size = mr;
		s->max_response_size_cached = mrc;
	}
	s->max_operations = actual_max_ops;

	/* Backchannel slots + callback metadata. */
	s->cb_prog = cb_prog;
	s->cb_sec_flavor = cb_sec_flavor;
	s->cb_conn = NULL;  /* Bound later by session_bind_conn(). */
	if (actual_back > 0) {
		s->cb_slots = calloc(actual_back, sizeof(struct nfs4_slot));
		if (s->cb_slots == NULL) {
			free(s->slots);
			free(s);
			rc = -3;
			goto out;
		}
		s->num_cb_slots = actual_back;
	} else {
		s->cb_slots = NULL;
		s->num_cb_slots = 0;
	}

	make_session_id(st, s->session_id);

	/* Insert into session hash. */
	{
		uint32_t idx = hash_session_id(s->session_id);

		s->hash_next = st->session_hash[idx];
		st->session_hash[idx] = s;
	}

	/* Link to client's session list (head insertion). */
	s->client_next = c->sessions;
	c->sessions = s;

	/* Confirm the client on first CREATE_SESSION. */
	c->confirmed = true;
	c->create_seq++;
	c->last_renewed = time(NULL);

	/* RFC 8881 §18.35.4 case 5: if this client supersedes an
	 * older confirmed record, destroy the old one now that the
	 * replacement is confirmed.  Pynfs EID5f. */
	if (c->superseded_clientid != 0) {
		struct nfs4_client *old =
			find_client_by_id(st, c->superseded_clientid);
		if (old != NULL) {
			unhash_client(st, old);
			free_client(st, old);
		}
		c->superseded_clientid = 0;
	}

	/* Cache the result for CREATE_SESSION replay detection
	 * per RFC 8881 §18.36.4. */
	c->last_cs_valid = true;
	memcpy(c->last_cs_session_id, s->session_id, SESSION_ID_SIZE);
	c->last_cs_fore_slots = actual_fore;
	c->last_cs_back_slots = actual_back;
	c->last_cs_csr_flags = 0;  /* Populated by op_create_session caller */
	c->last_cs_fore_max_request_size = actual_max_req;
	c->last_cs_fore_max_operations = actual_max_ops;

	/* Persist recovery record for failover.
	 * Priority: CQ (replicated) > catalogue vtable. */
	if (st->cq != NULL) {
		struct commit_op cop;
		struct commit_op_recovery_put *a;

		memset(&cop, 0, sizeof(cop));
		cop.type = COMMIT_OP_RECOVERY_PUT;
		a = &cop.args.recovery_put;
		a->clientid = c->clientid;
		a->co_ownerid_len = c->co_ownerid_len;
		memcpy(a->co_ownerid, c->co_ownerid, c->co_ownerid_len);
		memcpy(a->verifier, c->verifier, 8);
		(void)commit_queue_submit(st->cq, &cop);
	} else if (st->cat != NULL) {
		(void)mds_coord_recovery_put(
			st->cat, NULL, c->clientid,
			c->co_ownerid, c->co_ownerid_len,
			c->verifier);
	}

	/* Output. */
	memcpy(out_session_id, s->session_id, SESSION_ID_SIZE);
	if (out_fore_slots != NULL) {
		*out_fore_slots = actual_fore;
}
	if (out_back_slots != NULL) {
		*out_back_slots = actual_back;
}
	if (out_fore_max_request_size != NULL) {
		*out_fore_max_request_size = actual_max_req;
	}
	if (out_fore_max_operations != NULL) {
		*out_fore_max_operations = actual_max_ops;
	}

out:
	pthread_mutex_unlock(&st->locks[0]);
	return rc;
}

int session_get_limits(struct session_table *st,
		       const uint8_t session_id[SESSION_ID_SIZE],
		       uint32_t *out_max_req,
		       uint32_t *out_max_ops)
{
	struct nfs4_session *s;
	int rc = -1;

	if (st == NULL || session_id == NULL) {
		return -1;
	}

	pthread_mutex_lock(&st->locks[0]);
	s = find_session(st, session_id);
	if (s != NULL) {
		if (out_max_req != NULL) {
			*out_max_req = s->max_request_size;
		}
		if (out_max_ops != NULL) {
			*out_max_ops = s->max_operations;
		}
		rc = 0;
	}
	pthread_mutex_unlock(&st->locks[0]);
	return rc;
}

int session_get_response_limits(struct session_table *st,
				const uint8_t session_id[SESSION_ID_SIZE],
				uint32_t *out_max_resp,
				uint32_t *out_max_resp_cached)
{
	struct nfs4_session *s;
	int rc = -1;

	if (st == NULL || session_id == NULL) {
		return -1;
	}

	pthread_mutex_lock(&st->locks[0]);
	s = find_session(st, session_id);
	if (s != NULL) {
		if (out_max_resp != NULL) {
			*out_max_resp = s->max_response_size;
		}
		if (out_max_resp_cached != NULL) {
			*out_max_resp_cached = s->max_response_size_cached;
		}
		rc = 0;
	}
	pthread_mutex_unlock(&st->locks[0]);
	return rc;
}

/* ----------------------------------------------------------------------- */

int session_destroy_session(struct session_table *st,
			    const uint8_t session_id[SESSION_ID_SIZE])
{
	struct nfs4_session *s;
	struct nfs4_client *c;
	int rc = 0;

	if (st == NULL || session_id == NULL) {
		return -1;
}

	pthread_mutex_lock(&st->locks[0]);

	s = find_session(st, session_id);
	if (s == NULL) {
		rc = -1;  /* NFS4ERR_BADSESSION */
		goto out;
	}

	/* Unlink from client's session list via client_next chain. */
	c = find_client_by_id(st, s->clientid);
	if (c != NULL) {
		struct nfs4_session **pp;

		for (pp = &c->sessions; *pp != NULL;
		     pp = &(*pp)->client_next) {
			if (*pp == s) {
				*pp = s->client_next;
				break;
			}
		}
	}

	/* Remove from session hash and free. */
	unhash_session(st, s);
	free_session(s);

out:
	pthread_mutex_unlock(&st->locks[0]);
	return rc;
}

/* ----------------------------------------------------------------------- */

/*
 * RFC 8881 §18.50 DESTROY_CLIENTID — destroy a clientid record.
 *
 * Returns 0 on success, -1 on STALE_CLIENTID (clientid not found),
 * -2 on CLIENTID_BUSY (the client still has confirmed sessions, the
 * client must DESTROY_SESSION on each session first per §18.50.3).
 *
 * Pynfs DESCID3/4/5/6/7/8 drive every leg of this contract:
 *   DESCID3/4   bad clientid → -1 → NFS4ERR_STALE_CLIENTID.
 *   DESCID5/6   client owns at least one session → -2 → CLIENTID_BUSY.
 *   DESCID8     destroy then destroy again → first 0, second -1.
 *
 * The first call removes the client record from both the clientid hash
 * and the owner hash, so a subsequent find_client_by_id() returns NULL.
 */
int session_destroy_client(struct session_table *st, uint64_t clientid)
{
	struct nfs4_client *c;
	int rc = 0;

	if (st == NULL) {
		return -1;
	}

	pthread_mutex_lock(&st->locks[0]);
	c = find_client_by_id(st, clientid);
	if (c == NULL) {
		rc = -1;
		goto out;
	}
	/* RFC 8881 §18.50.3: NFS4ERR_CLIENTID_BUSY when the client
	 * still holds confirmed sessions; the caller must tear those
	 * down with DESTROY_SESSION first.  Unconfirmed clients (no
	 * CREATE_SESSION yet) are eligible for destruction here. */
	if (c->confirmed && c->sessions != NULL) {
		rc = -2;
		goto out;
	}
	unhash_client(st, c);
	free_client(st, c);

out:
	pthread_mutex_unlock(&st->locks[0]);
	return rc;
}

/* -----------------------------------------------------------------------
 * RFC 8881 §18.51 RECLAIM_COMPLETE — per-client one-shot.
 *
 * Atomic test-and-set on the client's reclaim_complete_done flag
 * under the session-table lock.  Independent of the grace recovery
 * set so that brand-new (post-grace) clients can call exactly once
 * and get NFS4_OK; their second call returns NFS4ERR_COMPLETE_ALREADY.
 * Pynfs CALLBACK1 testCbNotifyLockExpiredClient drives the
 * post-grace path.
 *
 * Returns: 0 on success (first call); 1 if already done;
 *          -1 if clientid not found.
 * ----------------------------------------------------------------------- */
int session_client_reclaim_complete(struct session_table *st,
				    uint64_t clientid)
{
	struct nfs4_client *c;
	int rc;

	if (st == NULL) {
		return -1;
	}

	pthread_mutex_lock(&st->locks[0]);
	c = find_client_by_id(st, clientid);
	if (c == NULL) {
		rc = -1;
		goto out;
	}
	if (c->reclaim_complete_done) {
		rc = 1;
		goto out;
	}
	c->reclaim_complete_done = true;
	rc = 0;

out:
	pthread_mutex_unlock(&st->locks[0]);
	return rc;
}

/* ----------------------------------------------------------------------- */

int session_slot_cache_reply(struct session_table *st,
			     const uint8_t session_id[SESSION_ID_SIZE],
			     uint32_t slot_id,
			     const uint8_t *reply, uint32_t reply_len)
{
	struct nfs4_session *s;
	struct nfs4_slot *slot;
	uint32_t shard;

	if (st == NULL || reply == NULL || reply_len == 0) {
		return -1;
	}

	shard = session_id_shard(session_id);
	pthread_mutex_lock(&st->locks[shard]);
	s = find_session(st, session_id);
	if (s == NULL || slot_id >= s->num_slots) {
		pthread_mutex_unlock(&st->locks[shard]);
		return -1;
	}
	slot = &s->slots[slot_id];
	free(slot->cached_reply);
	slot->cached_reply = malloc(reply_len);
	if (slot->cached_reply == NULL) {
		slot->cached_reply_len = 0;
		pthread_mutex_unlock(&st->locks[shard]);
		return -1;
	}
	memcpy(slot->cached_reply, reply, reply_len);
	slot->cached_reply_len = reply_len;
	pthread_mutex_unlock(&st->locks[shard]);
	return 0;
}

int session_slot_get_cached_reply(struct session_table *st,
				  const uint8_t session_id[SESSION_ID_SIZE],
				  uint32_t slot_id,
				  const uint8_t **out_reply,
				  uint32_t *out_len)
{
	struct nfs4_session *s;
	const struct nfs4_slot *slot;
	uint32_t shard;

	if (st == NULL || out_reply == NULL || out_len == NULL) {
		return -1;
	}

	shard = session_id_shard(session_id);
	pthread_mutex_lock(&st->locks[shard]);
	s = find_session(st, session_id);
	if (s == NULL || slot_id >= s->num_slots) {
		pthread_mutex_unlock(&st->locks[shard]);
		return -1;
	}
	slot = &s->slots[slot_id];
	*out_reply = slot->cached_reply;
	*out_len = slot->cached_reply_len;
	pthread_mutex_unlock(&st->locks[shard]);
	return (*out_reply != NULL) ? 0 : -1;
}

int session_sequence_check(struct session_table *st,
			   const uint8_t session_id[SESSION_ID_SIZE],
			   uint32_t slot_id,
			   uint32_t seq_id,
			   uint32_t highest_slot_id,
			   uint32_t *out_highest_slot,
			   uint32_t *out_target_slot,
			   uint32_t *out_status_flags,
			   uint64_t *out_clientid)
{
	struct nfs4_session *s;
	struct nfs4_client *c;
	struct nfs4_slot *slot;
	int rc;
	uint32_t shard;

	(void)highest_slot_id;

	if (st == NULL || session_id == NULL) {
		return -1;
	}

	MDS_PHASE_SCOPE(MDS_PHASE_STATE);

	shard = session_id_shard(session_id);
	pthread_mutex_lock(&st->locks[shard]);

	s = find_session(st, session_id);
	if (s == NULL) {
		rc = -1;  /* NFS4ERR_BADSESSION */
		goto out;
	}

	if (slot_id >= s->num_slots) {
		rc = -2;  /* NFS4ERR_BADSLOT */
		goto out;
	}

	slot = &s->slots[slot_id];

	/* First request on a fresh slot: accept any seq_id.
	 * calloc'd slots have seq_id=0 and in_use=false (0).
	 * After first use, normal seq_id tracking applies. */
	if (!slot->in_use) {
		slot->seq_id = seq_id;
		slot->in_use = 1;
		rc = 0;
		goto accepted;
	}

	if (seq_id == slot->seq_id) {
		/*
		 * Replay: same seq_id as last completed request.
		 * DRC: if a cached reply exists, return rc=2 so the
		 * caller sends the cached response directly.
		 * If no cache, return rc=1 (SEQ_FALSE_RETRY).
		 */
		if (out_clientid != NULL) {
			*out_clientid = s->clientid;
}
		rc = (slot->cached_reply != NULL) ? 2 : 1;
		goto out;
	} else if (seq_id == slot->seq_id + 1) {
		/* New request: advance the slot. */
		free(slot->cached_reply);
		slot->cached_reply = NULL;
		slot->cached_reply_len = 0;
		slot->seq_id = seq_id;
		rc = 0;
	} else {
		/* Misordered. */
		rc = -3;  /* NFS4ERR_SEQ_MISORDERED */
		goto out;
	}

accepted:
	/* Renew lease. */
	c = find_client_by_id(st, s->clientid);
	if (c != NULL) {
		c->last_renewed = time(NULL);
}

	/* Output hints. */
	if (out_highest_slot != NULL) {
		*out_highest_slot = s->num_slots - 1;
}
	if (out_target_slot != NULL) {
		*out_target_slot = s->num_slots - 1;
}
	if (out_status_flags != NULL) {
		*out_status_flags = 0;
}
	if (out_clientid != NULL) {
		*out_clientid = s->clientid;
}

out:
	pthread_mutex_unlock(&st->locks[shard]);
	return rc;
}

/* -----------------------------------------------------------------------
 * Backchannel connection binding
 * ----------------------------------------------------------------------- */

int session_bind_conn(struct session_table *st,
		      const uint8_t session_id[SESSION_ID_SIZE],
		      struct rpc_conn *conn)
{
	struct nfs4_session *s;

	if (st == NULL || session_id == NULL || conn == NULL) {
		return -1;
}

	pthread_mutex_lock(&st->locks[0]);
	s = find_session(st, session_id);
	if (s == NULL) {
		pthread_mutex_unlock(&st->locks[0]);
		return -1;
	}
	s->cb_conn = conn;
	pthread_mutex_unlock(&st->locks[0]);
	return 0;
}

/*
 * RFC 8881 §2.10.8.3 / §18.36 — update the captured callback security
 * parameters on a session.  Called by op_create_session immediately
 * after session_create_session, and by op_backchannel_ctl when the
 * client supplies new bca_sec_parms.  A NULL @sec clears the parms
 * to zero (AUTH_NONE void cred body).
 *
 * Also keeps the legacy cb_sec_flavor scalar in sync so any code
 * that still reads it sees the new flavor.
 */
int session_set_cb_sec(struct session_table *st,
                       const uint8_t session_id[SESSION_ID_SIZE],
                       const struct nfs4_cb_sec *sec)
{
    struct nfs4_session *s;

    if (st == NULL || session_id == NULL) {
        return -1;
    }
    pthread_mutex_lock(&st->locks[0]);
    s = find_session(st, session_id);
    if (s == NULL) {
        pthread_mutex_unlock(&st->locks[0]);
        return -1;
    }
    if (sec != NULL) {
        s->cb_sec = *sec;
    } else {
        memset(&s->cb_sec, 0, sizeof(s->cb_sec));
    }
    s->cb_sec_flavor = s->cb_sec.flavor;
    pthread_mutex_unlock(&st->locks[0]);
    return 0;
}

/*
 * RFC 8881 §18.33 BACKCHANNEL_CTL — update the callback program
 * number on a session.  No-op when the new value matches.
 */
int session_set_cb_prog(struct session_table *st,
                        const uint8_t session_id[SESSION_ID_SIZE],
                        uint32_t cb_prog)
{
    struct nfs4_session *s;

    if (st == NULL || session_id == NULL) {
        return -1;
    }
    pthread_mutex_lock(&st->locks[0]);
    s = find_session(st, session_id);
    if (s == NULL) {
        pthread_mutex_unlock(&st->locks[0]);
        return -1;
    }
    s->cb_prog = cb_prog;
    pthread_mutex_unlock(&st->locks[0]);
    return 0;
}

void session_unbind_conn(struct session_table *st, const struct rpc_conn *conn)
{
	uint32_t i;

	if (st == NULL || conn == NULL) {
		return;
}

	pthread_mutex_lock(&st->locks[0]);
	for (i = 0; i < SESSION_HASH_BUCKETS; i++) {
		struct nfs4_session *s;

		for (s = st->session_hash[i]; s != NULL; s = s->hash_next) {
			if (s->cb_conn == conn) {
				s->cb_conn = NULL;
}
		}
	}
	pthread_mutex_unlock(&st->locks[0]);
}



/* -----------------------------------------------------------------------
 * Callback target enumeration (layout recall support)
 * ----------------------------------------------------------------------- */

/*
 * Build a session_cb_snap from a live session under the session-table
 * lock and invoke @cb.  When @cb returns 1 ("snap consumed"), commit
 * the slot-0 seqid advance back to the session so subsequent CBs on
 * this session use a fresh, monotonic sa_sequenceid per RFC 8881
 * §18.46.4.  Internal helper shared by both iterators below.
 */
static int session_invoke_cb_locked(struct nfs4_session *s,
                                    session_cb_snap_fn cb, void *ctx)
{
    struct session_cb_snap snap;
    uint32_t prepared_seq;
    int rc;

    snap.clientid = s->clientid;
    memcpy(snap.session_id, s->session_id, SESSION_ID_SIZE);
    snap.cb_prog = s->cb_prog;
    snap.cb_sec_flavor = s->cb_sec_flavor;
    snap.cb_sec = s->cb_sec;
    snap.cb_conn = s->cb_conn;
    /*
     * RFC 8881 §2.10.5.1 / §18.46.4: CB_SEQUENCE sa_sequenceid MUST
     * start at 1 and increment by 1 per CB on the slot.  The fd-based
     * callers (delegation/layout conflict-recall) use this snap value
     * verbatim as sa_sequenceid, so we hand them the NEXT id
     * (current + 1), not the current one.  We commit the increment
     * only when @cb returns 1 ("snap consumed").  rc != 1 leaves the
     * slot's seq_id untouched so an uninterested visit doesn't burn
     * sequenceids.
     */
    prepared_seq = (s->cb_slots != NULL && s->num_cb_slots > 0)
                       ? s->cb_slots[0].seq_id + 1 : 0;
    snap.slot_seq_id = prepared_seq;
    snap.minorversion = s->minorversion;

    rc = cb(&snap, ctx);
    if (rc == 1 && s->cb_slots != NULL && s->num_cb_slots > 0) {
        s->cb_slots[0].seq_id = prepared_seq;
    }
    return rc;
}

int session_for_each_with_cb(struct session_table *st,
                             session_cb_snap_fn cb, void *ctx)
{
    uint32_t i;
    int rc = 0;

    if (st == NULL || cb == NULL) {
        return 0;
    }

    pthread_mutex_lock(&st->locks[0]);
    for (i = 0; i < SESSION_HASH_BUCKETS && rc == 0; i++) {
        struct nfs4_session *s;

        for (s = st->session_hash[i]; s != NULL && rc == 0;
             s = s->hash_next) {
            if (s->cb_conn == NULL) {
                continue;
            }

            rc = session_invoke_cb_locked(s, cb, ctx);
            if (rc == 1) {
                /* Snap consumed; iterator stops. */
                break;
            }
            if (rc != 0) {
                break;
            }
        }
    }
    pthread_mutex_unlock(&st->locks[0]);
    return rc;
}

int session_for_each_with_cb_for_clientid(struct session_table *st,
                                          uint64_t clientid,
                                          session_cb_snap_fn cb,
                                          void *ctx)
{
    struct nfs4_client *c;
    struct nfs4_session *s;
    int rc = 0;

    if (st == NULL || cb == NULL) {
        return 0;
    }

    pthread_mutex_lock(&st->locks[0]);
    c = find_client_by_id(st, clientid);
    if (c == NULL) {
        pthread_mutex_unlock(&st->locks[0]);
        return 0;
    }
    /*
     * c->sessions is head-inserted at session_create_session() time
     * (see session_create_session at the head-insertion site), so the
     * first node is the most-recently-created session for this client.
     * That is exactly the session the kernel client believes is
     * current; older sessions whose cb_conn happens to still be
     * non-NULL after a remount are skipped, eliminating the
     * NFS4ERR_BADSESSION window described in include/session.h on
     * this iterator's docstring.
     */
    for (s = c->sessions; s != NULL; s = s->client_next) {
        if (s->cb_conn == NULL) {
            continue;
        }
        rc = session_invoke_cb_locked(s, cb, ctx);
        /* Stop at the first session we visit — we want exactly one
         * CB delivery per clientid per call. */
        break;
    }
    pthread_mutex_unlock(&st->locks[0]);
    return rc;
}

/* -----------------------------------------------------------------------
 * R2.2: Lease expiry reaper
 *
 * Background thread that periodically scans all clients.  Any client
 * whose last_renewed + lease_time_sec < now is considered expired.
 * Expired clients have their sessions and open/layout state cleaned up.
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void *lease_reaper_thread(void *arg)
{
    struct session_table *st = arg;
    uint32_t lease = st->lease_time_sec > 0 ? st->lease_time_sec : 90;

    while (atomic_load(&st->reaper_running)) {
        {
            /* Sleep for half the lease period so expired records
             * are caught within a reasonable window.  The on-demand
             * check in session_create_session handles the
             * correctness-critical path; this loop is for garbage
             * collection of records nobody asks about. */
            uint32_t interval = lease / 2;
            if (interval < 10) { interval = 10; }
            struct timespec ts = {
                .tv_sec = interval,
                .tv_nsec = 0,
            };
            (void)nanosleep(&ts, NULL);
        }
        if (!atomic_load(&st->reaper_running)) {
            break;
        }

        time_t now = time(NULL);

        /* All session-table mutations use locks[0]; the reaper
         * must use the same lock to avoid racing with
         * session_exchange_id / session_create_session. */
        pthread_mutex_lock(&st->locks[0]);
        for (uint32_t b = 0; b < CLIENT_HASH_BUCKETS; b++) {
            struct nfs4_client **pp = &st->client_hash[b];
            while (*pp != NULL) {
                struct nfs4_client *c = *pp;
                bool expired = false;

                if (now <= c->last_renewed) {
                    pp = &c->hash_next;
                    continue;
                }
                uint32_t age = (uint32_t)(now - c->last_renewed);

                if (!c->confirmed && age >= lease) {
                    /* RFC 8881 §18.35.4: unconfirmed records
                     * not confirmed within a lease period
                     * SHOULD be removed. */
                    expired = true;
                } else if (c->confirmed && age > lease * 2) {
                    /* Confirmed clients get 2× lease grace
                     * (they actively renew via SEQUENCE). */
                    expired = true;
                }

                if (expired) {
                    /* Advance the bucket-chain pointer past c
                     * before unhash removes c from the chain. */
                    *pp = c->hash_next;
                    /* unhash_client unlinks from owner_hash;
                     * the client_hash unlink above is redundant
                     * with unhash_client's client_hash walk, but
                     * harmless (walk finds nothing). */
                    unhash_client(st, c);
                    if (c->confirmed) {
                        if (st->ot != NULL) {
                            open_state_close_all_for_client(
                                st->ot, c->clientid);
                        }
                        if (st->lt != NULL) {
                            lock_release_all_for_client(
                                st->lt, c->clientid);
                        }
                        if (st->cat != NULL) {
                            (void)mds_coord_layout_del_all_for_client(
                                st->cat, c->clientid);
                        }
                    }
                    free_client(st, c);
                } else {
                    pp = &c->hash_next;
                }
            }
        }
        pthread_mutex_unlock(&st->locks[0]);
    }
    return NULL;
}

int session_table_start_reaper(struct session_table *st)
{
    if (st == NULL) { return -1; }
    atomic_store(&st->reaper_running, 1);
    return pthread_create(&st->reaper_tid, NULL, lease_reaper_thread, st);
}

void session_table_stop_reaper(struct session_table *st)
{
    if (st == NULL) { return; }
    if (!atomic_load(&st->reaper_running)) { return; }
    atomic_store(&st->reaper_running, 0);
    pthread_join(st->reaper_tid, NULL);
}

bool session_client_has_reclaimed(struct session_table *st,
                                  uint64_t clientid)
{
    struct nfs4_client *c;
    bool done = true; /* default: allow if client not found */

    if (st == NULL) {
        return true;
    }
    pthread_mutex_lock(&st->locks[0]);
    c = find_client_by_id(st, clientid);
    if (c != NULL) {
        done = c->reclaim_complete_done;
    }
    pthread_mutex_unlock(&st->locks[0]);
    return done;
}

bool session_client_lease_expired(struct session_table *st,
                                  uint64_t clientid)
{
    struct nfs4_client *c;
    bool expired = false;

    if (st == NULL) {
        return false;
    }
    pthread_mutex_lock(&st->locks[0]);
    c = find_client_by_id(st, clientid);
    if (c == NULL) {
        /*
         * Clientid not in the session table — the record was
         * replaced by a subsequent EXCHANGE_ID (case 5) or
         * destroyed.  Any open/lock state referencing this
         * clientid is orphaned and must be treated as expired
         * so courtesy-client revocation can clean it up.
         */
        expired = true;
    } else {
        time_t now = time(NULL);
        if (now > c->last_renewed &&
            (uint32_t)(now - c->last_renewed) >= st->lease_time_sec) {
            expired = true;
        }
    }
    pthread_mutex_unlock(&st->locks[0]);
    return expired;
}

void session_table_set_ot(struct session_table *st,
                          struct open_state_table *ot)
{
    if (st != NULL) { st->ot = ot; }
}

void session_table_set_lt(struct session_table *st,
                          struct lock_table *lt)
{
    if (st != NULL) { st->lt = lt; }
}


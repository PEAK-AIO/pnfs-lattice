/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
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

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int session_exchange_id(struct session_table *st,
			const uint8_t *co_ownerid,
			uint32_t co_ownerid_len,
			const uint8_t verifier[NFS4_VERIFIER_SIZE],
			uint32_t eia_flags,
			uint64_t *out_clientid,
			uint32_t *out_seqid,
			uint32_t *out_flags)
{
	struct nfs4_client *c;
	int rc = 0;

	(void)eia_flags;

	if (st == NULL || co_ownerid == NULL || out_clientid == NULL) {
		return -1;
}
	if (co_ownerid_len == 0 || co_ownerid_len > CO_OWNERID_MAX) {
		return -1;
}

	pthread_mutex_lock(&st->locks[0]);

	c = find_client_by_owner(st, co_ownerid, co_ownerid_len);

	if (c != NULL && memcmp(c->verifier, verifier,
				NFS4_VERIFIER_SIZE) == 0) {
		/*
		 * UPDATE case: same co_ownerid, same verifier.
		 * Return existing clientid.
		 */
		c->last_renewed = time(NULL);
		*out_clientid = c->clientid;
		if (out_seqid != NULL) {
			*out_seqid = c->create_seq;
}
		if (out_flags != NULL) {
			*out_flags = EXCHGID4_FLAG_USE_PNFS_MDS |
				     EXCHGID4_FLAG_SUPP_MOVED_REFER;
}
		goto out;
	}

	if (c != NULL) {
		/*
		 * New incarnation: same co_ownerid, different verifier.
		 * Per RFC 8881 §18.35.4 case 3: if the client has no
		 * state, remove the old record and create a new one.
		 * (Confirmed clients with state would need more complex
		 * handling — deferred to SAL/open state layer.)
		 */
		unhash_client(st, c);
		free_client(st, c);
		c = NULL;
	}

	/*
	 * Post-failover recovery: if we are in grace and have a
	 * persisted recovery record that matches this co_ownerid,
	 * re-create the client with its original clientid so that
	 * subsequent CLAIM_PREVIOUS opens resolve correctly.
	 */
	uint64_t recovered_clientid = 0;
	bool use_recovered_id = false;

	if (st->cat != NULL && grace_is_active()) {
		/* Scan recovery records via coordination vtable,
		 * then look up each to match co_ownerid+verifier. */
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

	/* New client: allocate clientid (or reuse recovered one). */
	c = calloc(1, sizeof(*c));
	if (c == NULL) {
		rc = -1;
		goto out;
	}

	if (use_recovered_id) {
		c->clientid = recovered_clientid;
		/* Ensure next_clientid stays above recovered value. */
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
	c->sessions = NULL;
	c->hash_next = NULL;
	c->owner_hash_next = NULL;

	/* Insert into client hash. */
	{
		uint32_t idx = hash_clientid(c->clientid);

		c->hash_next = st->client_hash[idx];
		st->client_hash[idx] = c;
	}
	/* Insert into owner hash. */
	{
		uint32_t idx = hash_ownerid(c->co_ownerid,
					   c->co_ownerid_len);

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
			   uint8_t out_session_id[SESSION_ID_SIZE],
			   uint32_t *out_fore_slots,
			   uint32_t *out_back_slots)
{
	struct nfs4_client *c;
	struct nfs4_session *s = NULL;
	uint32_t actual_fore;
	uint32_t actual_back;
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
	 * Sequence ID check: RFC 8881 §18.36.4.
	 * create_seq tracks the expected value.
	 */
	if (seqid != c->create_seq) {
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

out:
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

            struct session_cb_snap snap;

            snap.clientid = s->clientid;
            memcpy(snap.session_id, s->session_id, SESSION_ID_SIZE);
            snap.cb_prog = s->cb_prog;
            snap.cb_sec_flavor = s->cb_sec_flavor;
            snap.cb_conn = s->cb_conn;
            snap.slot_seq_id = (s->cb_slots != NULL && s->num_cb_slots > 0)
                                ? s->cb_slots[0].seq_id : 0;

            rc = cb(&snap, ctx);
            if (rc != 0) {
                break;
            }
        }
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
    uint32_t shard;

    while (atomic_load(&st->reaper_running)) {
        {
            struct timespec ts = {
                .tv_sec = st->lease_time_sec > 0 ? st->lease_time_sec : 90,
                .tv_nsec = 0,
            };
            (void)nanosleep(&ts, NULL);
        }
        if (!atomic_load(&st->reaper_running)) {
            break;
        }

        time_t now = time(NULL);

        for (shard = 0; shard < 16; shard++) {
            pthread_mutex_lock(&st->locks[shard]);
            for (uint32_t b = 0; b < CLIENT_HASH_BUCKETS; b++) {
                struct nfs4_client **pp = &st->client_hash[b];
                while (*pp != NULL) {
                    struct nfs4_client *c = *pp;
                    uint32_t cs = (uint32_t)(c->clientid % 16);
                    if (cs != shard) { pp = &c->hash_next; continue; }

                    if (c->confirmed &&
                        (uint32_t)(now - c->last_renewed) > st->lease_time_sec * 2) {
                        /* Expired — unlink and clean dependent state. */
                        *pp = c->hash_next;
                        if (st->ot != NULL) {
                            open_state_close_all_for_client(
                                st->ot, c->clientid);
                        }
                        if (st->lt != NULL) {
                            lock_release_all_for_client(
                                st->lt, c->clientid);
                        }
                        /* Layout state cleanup on lease expiry. */
                        if (st->cat != NULL) {
                            (void)mds_coord_layout_del_all_for_client(
                                st->cat, c->clientid);
                        }
                        destroy_client_sessions(st, c);
                        free(c);
                    } else {
                        pp = &c->hash_next;
                    }
                }
            }
            pthread_mutex_unlock(&st->locks[shard]);
        }
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


/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * session.h -- NFSv4.1 session and clientid management.
 *
 * Implements EXCHANGE_ID, CREATE_SESSION, DESTROY_SESSION, and
 * SEQUENCE (slot table exactly-once semantics) per RFC 8881
 * SS18.35--18.37, 18.46.
 *
 * Each MDS node has its own clientid space:
 *   clientid = (mds_id << 48) | monotonic_counter
 *
 * Session IDs are 16-byte values:
 *   [mds_id 4B][session_counter 8B][0-pad 4B]
 *
 * See docs/architecture.md S4.3 for design overview.
 */

#ifndef SESSION_H
#define SESSION_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#include "nfs4_cb_sec.h"  /* struct nfs4_cb_sec -- callback security parms */

struct mds_catalogue;
struct commit_queue;  /* Forward declaration for session_table_set_cq() */
struct rpc_conn;      /* Forward declaration for backchannel binding */

/* -----------------------------------------------------------------------
 * Limits
 * ----------------------------------------------------------------------- */

#define SESSION_ID_SIZE      16
#define SESSION_MAX_SLOTS    64   /* Max forechannel slots per session */
#define CO_OWNERID_MAX       1024 /* RFC 8881 S18.35: co_ownerid<NFS4_OPAQUE_LIMIT> */
#define NFS4_VERIFIER_SIZE   8

/* Default lease time in seconds (overridable via config). */
#define SESSION_DEFAULT_LEASE_SEC 90

/* -----------------------------------------------------------------------
 * EXCHANGE_ID flags (RFC 8881 S18.35)
 * ----------------------------------------------------------------------- */

#define EXCHGID4_FLAG_SUPP_MOVED_REFER     0x00000001
#define EXCHGID4_FLAG_SUPP_MOVED_MIGR      0x00000002
#define EXCHGID4_FLAG_BIND_PRINC_STATEID   0x00000100
#define EXCHGID4_FLAG_USE_NON_PNFS         0x00010000
#define EXCHGID4_FLAG_USE_PNFS_MDS         0x00020000
#define EXCHGID4_FLAG_USE_PNFS_DS          0x00040000
#define EXCHGID4_FLAG_MASK_PNFS            0x00070000
#define EXCHGID4_FLAG_UPD_CONFIRMED_REC_A  0x40000000
/* CONFIRMED_R is a server-only response bit; clients that set it in
 * eia_flags MUST be rejected with NFS4ERR_INVAL (RFC 8881 S18.35.3,
 * pynfs EID7 testSupported1a). */
#define EXCHGID4_FLAG_CONFIRMED_R          0x80000000

/* All bits a client is allowed to set in eia_flags.  Anything else
 * (including CONFIRMED_R, which is server-only) yields NFS4ERR_INVAL
 * per RFC 8881 S18.35.3 -- pynfs EID4 testBadFlags / EID7 testSupported1a. */
#define EXCHGID4_VALID_CLIENT_MASK         (\
	EXCHGID4_FLAG_SUPP_MOVED_REFER | \
	EXCHGID4_FLAG_SUPP_MOVED_MIGR | \
	EXCHGID4_FLAG_BIND_PRINC_STATEID | \
	EXCHGID4_FLAG_USE_NON_PNFS | \
	EXCHGID4_FLAG_USE_PNFS_MDS | \
	EXCHGID4_FLAG_USE_PNFS_DS | \
	EXCHGID4_FLAG_UPD_CONFIRMED_REC_A)

/* Return-code constants for session_exchange_id beyond plain success.
 * Negative so callers can use the existing rc==0 check unchanged. */
#define SESSION_EID_OK         0
#define SESSION_EID_RESOURCE  -1   /* allocation failure */
#define SESSION_EID_NOENT     -2   /* NFS4ERR_NOENT (UPDATE w/o confirmed record) */
#define SESSION_EID_NOT_SAME  -3   /* NFS4ERR_NOT_SAME (UPDATE + verifier mismatch) */
#define SESSION_EID_PERM      -4   /* NFS4ERR_PERM (UPDATE + principal mismatch) */

/* -----------------------------------------------------------------------
 * CREATE_SESSION flags (RFC 8881 S18.36)
 * ----------------------------------------------------------------------- */

#define CREATE_SESSION4_FLAG_PERSIST        0x00000001
#define CREATE_SESSION4_FLAG_CONN_BACK_CHAN  0x00000002
#define CREATE_SESSION4_FLAG_CONN_RDMA      0x00000004

/* -----------------------------------------------------------------------
 * Slot table (per-session, forechannel)
 *
 * Each slot tracks the last completed sequence ID for exactly-once
 * semantics (DRC).  Cached reply data is deferred to a later phase.
 * ----------------------------------------------------------------------- */

struct nfs4_slot {
	uint32_t seq_id;     /* Last completed sequence ID (starts at 0) */
	bool     in_use;     /* Currently executing a request on this slot */
	uint8_t  *cached_reply;     /* DRC: serialized last COMPOUND reply */
	uint32_t  cached_reply_len; /* Length of cached_reply */
};

/* -----------------------------------------------------------------------
 * Session
 * ----------------------------------------------------------------------- */

struct nfs4_session {
	uint8_t              session_id[SESSION_ID_SIZE];
	uint64_t             clientid;
	uint32_t             num_slots;       /* Forechannel slot count */
	struct nfs4_slot    *slots;           /* [num_slots] */
	struct nfs4_session *hash_next;       /* Session hash chain */
	struct nfs4_session *client_next;     /* Per-client session list */
	/* RFC 8881 S2.10.5 / S20.1 -- NFSv4 minor version negotiated by
	 * the EXCHANGE_ID + CREATE_SESSION pair.  Stored on the session
	 * so the CB_COMPOUND emitter (nfs4_cb.c) can echo it in the
	 * `minorversion` field of every callback compound -- RFC 8881
	 * S20.1 requires the callback compound's minorversion to match
	 * the session's negotiated value (a v4.2 client running pynfs
	 * rejects v4.1 callbacks with NFS4ERR_MINOR_VERS_MISMATCH).
	 *
	 * Populated from cd->minorversion at op_create_session time.
	 * 0 indicates v4.0 (no SESSION concept; field unused on the wire). */
	uint32_t             minorversion;
	/* RFC 8881 S18.36.4 negotiated forechannel attrs.  Stored at
	 * session-create time as MIN(client_request, server_pref) and
	 * enforced on every COMPOUND that carries a SEQUENCE on this
	 * session.  Pynfs SEQ6 / SEQ7 rely on these caps. */
	uint32_t             max_request_size;
	uint32_t             max_operations;
	/* Backchannel state (RFC 8881 S2.10.3.1) */
	uint32_t             cb_prog;         /* Callback program number */
	uint32_t             cb_sec_flavor;   /* Callback security flavor (legacy alias for cb_sec.flavor) */
	/*
	 * RFC 8881 S2.10.8.3 / S18.36 -- callback security parameters
	 * captured from CREATE_SESSION's csa_sec_parms<> array.  The
	 * CB encoder uses this to populate the RPC credential body
	 * (flavor + AUTH_SYS authsys_parms or AUTH_NONE void) of
	 * every CB_COMPOUND call.  Updated by BACKCHANNEL_CTL when
	 * implemented.  Default (zero-initialised) yields AUTH_NONE.
	 */
	struct nfs4_cb_sec   cb_sec;
	struct rpc_conn     *cb_conn;         /* Borrowed ptr to client connection */
	struct nfs4_slot    *cb_slots;        /* Backchannel slot table */
	uint32_t             num_cb_slots;    /* Backchannel slot count */
	uint32_t             cb_next_slot;    /* Round-robin index */
};

/* -----------------------------------------------------------------------
 * Client record (one per EXCHANGE_ID co_ownerid)
 * ----------------------------------------------------------------------- */

struct nfs4_client {
	uint64_t             clientid;
	uint8_t              co_ownerid[CO_OWNERID_MAX];
	uint32_t             co_ownerid_len;
	uint8_t              verifier[NFS4_VERIFIER_SIZE];
	bool                 confirmed;       /* Confirmed by CREATE_SESSION */
	time_t               last_renewed;    /* Lease renewal timestamp */
	uint32_t             create_seq;      /* Sequencing for CREATE_SESSION */
	/* RFC 8881 S18.35.4 -- principal identity captured at
	 * EXCHANGE_ID time so subsequent EID + UPDATE flag checks can
	 * detect principal mismatch (cases 8/9 -> NFS4ERR_PERM /
	 * NFS4ERR_NOT_SAME).  auth_flavor==0 means "no principal
	 * captured" (test/legacy path); cred matching is then
	 * suppressed and only the verifier is consulted. */
	uint32_t             auth_flavor;
	uint32_t             cred_uid;
	uint32_t             cred_gid;
	struct nfs4_session *sessions;        /* Linked list of sessions */
	struct nfs4_client  *hash_next;       /* Client hash chain (by clientid) */
	struct nfs4_client  *owner_hash_next; /* Owner hash chain (by co_ownerid) */
};

/* -----------------------------------------------------------------------
 * Session table (top-level container)
 * ----------------------------------------------------------------------- */

struct session_table;  /* Opaque -- defined in session.c */

/* -----------------------------------------------------------------------
 * API -- Lifecycle
 * ----------------------------------------------------------------------- */

/**
 * Initialise the session/clientid manager.
 *
 * @param mds_id         This MDS node's numeric ID (0-based).
 * @param lease_time_sec Lease duration in seconds (0 = default 90s).
 * @param out            Receives the session table handle.
 * @return 0 on success, -1 on allocation failure.
 *
 * Ownership: caller must call session_table_destroy() to free.
 * Thread safety: the returned handle is safe to share; all operations
 * are serialised internally via pthread_mutex.
 */
int session_table_init(uint32_t mds_id, uint32_t lease_time_sec,
		       struct session_table **out);

/**
 * Destroy the session table and free all client/session state.
 *
 * @param st  Handle from session_table_init().  NULL is tolerated.
 */
void session_table_destroy(struct session_table *st);

/**
 * Attach a commit queue for replicated recovery writes.
 *
 * When set, recovery record put/del operations are routed through
 * the commit queue (which builds replication deltas).  When NULL,
 * recovery writes go directly to the catalogue.
 *
 * @param st  Session table.
 * @param cq  Commit queue (NULL disables replicated recovery writes).
 */
void session_table_set_cq(struct session_table *st, struct commit_queue *cq);

/**
 * Attach a catalogue handle for backend-neutral recovery persistence.
 *
 * When both CQ and catalogue are NULL (e.g. RonDB backend),
 * the session table falls through to the catalogue coordination vtable
 * (mds_coord_recovery_put/del, mds_coord_layout_del_all_for_client)
 * for recovery and layout state operations.
 *
 * @param st   Session table.
 * @param cat  Catalogue handle (NULL disables catalogue fallback).
 */
void session_table_set_cat(struct session_table *st,
			   struct mds_catalogue *cat);

struct mds_shard;

/**
 * Bind the session table to a default shard.
 *
 * Convenience: sets both db and cq from the shard in one call.
 * Equivalent to session_table_set_db(st, shard->db) +
 * session_table_set_cq(st, shard->cq).
 *
 * @param st     Session table.
 * @param shard  Default shard (NULL tolerated -- clears both).
 */
void session_table_set_shard(struct session_table *st,
			     const struct mds_shard *shard);

/* -----------------------------------------------------------------------
 * API -- EXCHANGE_ID (RFC 8881 S18.35)
 * ----------------------------------------------------------------------- */

/**
 * Process an EXCHANGE_ID request (RFC 8881 S18.35.4).
 *
 * Implements the seven non-UPDATE / four UPDATE cases of S18.35.4 against
 * the server's per-co_ownerid client record:
 *
 *   - No record + UPDATE flag                                 -> NOENT.
 *   - No record + no UPDATE flag                              -> case 1, fresh clientid.
 *   - Record exists + UPDATE flag + unconfirmed               -> NOENT.
 *   - Record exists + UPDATE flag + confirmed:
 *       verifier match + principal match                       -> case 6, return existing.
 *       verifier mismatch                                       -> NOT_SAME.
 *       verifier match + principal mismatch                     -> PERM.
 *   - Record exists + no UPDATE + unconfirmed                 -> case 4, replace (new clientid).
 *   - Record exists + no UPDATE + confirmed:
 *       verifier match + principal match                       -> case 2, return existing (renew).
 *       otherwise                                              -> case 3/5/7, replace (new clientid).
 *
 * The new client is "unconfirmed" until CREATE_SESSION succeeds.
 * Principal matching uses (auth_flavor, cred_uid, cred_gid).  When
 * auth_flavor == 0 the principal check is suppressed (only the verifier
 * is consulted); this preserves legacy unit-test invocations.
 *
 * @param st              Session table.
 * @param co_ownerid      Client owner ID (opaque).
 * @param co_ownerid_len  Length of co_ownerid.
 * @param verifier        8-byte boot verifier.
 * @param eia_flags       EXCHGID4_FLAG_* from client (UPDATE bit honoured).
 * @param out_clientid    Receives the assigned clientid.
 * @param out_seqid       Receives the sequence ID for CREATE_SESSION.
 * @param out_flags       Receives the server's EXCHGID4_FLAG_* response.
 * @param auth_flavor     RPC auth flavor of the calling principal (0 = no cred / legacy).
 * @param cred_uid        AUTH_SYS uid (ignored when auth_flavor == 0).
 * @param cred_gid        AUTH_SYS gid (ignored when auth_flavor == 0).
 * @return SESSION_EID_OK on success.
 *         SESSION_EID_NOENT, SESSION_EID_NOT_SAME, SESSION_EID_PERM
 *         encode the corresponding RFC errors; SESSION_EID_RESOURCE
 *         indicates an allocation failure.
 */
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
			uint32_t cred_gid);

/* -----------------------------------------------------------------------
 * API -- CREATE_SESSION (RFC 8881 S18.36)
 * ----------------------------------------------------------------------- */

/**
 * Process a CREATE_SESSION request.
 *
 * Creates a new session for a confirmed or unconfirmed client.  The
 * session has a forechannel slot table of the negotiated size.
 *
 * @param st               Session table.
 * @param clientid         From EXCHANGE_ID result.
 * @param seqid            Must match client's create_seq.
 * @param fore_slots       Requested forechannel slots (capped at SESSION_MAX_SLOTS).
 * @param back_slots       Requested backchannel slots (capped at SESSION_MAX_SLOTS).
 * @param cb_prog          Callback program number from CREATE_SESSION.
 * @param cb_sec_flavor    Callback security flavor (AUTH_NONE / AUTH_SYS).
 * @param out_session_id   Receives the 16-byte session ID.
 * @param out_fore_slots   Receives the negotiated forechannel slot count.
 * @param out_back_slots   Receives the negotiated backchannel slot count.
 * @return 0 on success.
 *         -1 = NFS4ERR_STALE_CLIENTID (unknown clientid).
 *         -2 = NFS4ERR_SEQ_MISORDERED (bad seqid).
 *         -3 = allocation failure.
 */
int session_create_session(struct session_table *st,
			   uint64_t clientid,
			   uint32_t seqid,
			   uint32_t fore_slots,
			   uint32_t back_slots,
			   uint32_t cb_prog,
			   uint32_t cb_sec_flavor,
			   uint32_t fore_max_request_size,
			   uint32_t fore_max_operations,
			   uint32_t minorversion,
			   uint8_t out_session_id[SESSION_ID_SIZE],
			   uint32_t *out_fore_slots,
			   uint32_t *out_back_slots,
			   uint32_t *out_fore_max_request_size,
			   uint32_t *out_fore_max_operations);

/**
 * Look up the negotiated per-session forechannel limits.  Used by the
 * RPC layer to enforce RFC 8881 S15.1.10.5 (NFS4ERR_REQ_TOO_BIG) and
 * S15.1.10.4 (NFS4ERR_TOO_MANY_OPS) on every COMPOUND request that
 * carries a SEQUENCE on this session.
 *
 * @param st            Session table.
 * @param session_id    16-byte session ID extracted from SEQUENCE arg.
 * @param out_max_req   Receives the negotiated ca_maxrequestsize
 *                      (output undefined when the function returns -1).
 * @param out_max_ops   Receives the negotiated ca_maxoperations.
 * @return 0 on hit (limits filled), -1 on session not found.
 */
int session_get_limits(struct session_table *st,
		       const uint8_t session_id[SESSION_ID_SIZE],
		       uint32_t *out_max_req,
		       uint32_t *out_max_ops);

/* -----------------------------------------------------------------------
 * API -- DESTROY_SESSION (RFC 8881 S18.37)
 * ----------------------------------------------------------------------- */

/**
 * Destroy a session.
 *
 * Removes the session from the table and frees its slot table.
 *
 * @param st          Session table.
 * @param session_id  16-byte session ID.
 * @return 0 on success, -1 if session not found (NFS4ERR_BADSESSION).
 */
int session_destroy_session(struct session_table *st,
			    const uint8_t session_id[SESSION_ID_SIZE]);

/* -----------------------------------------------------------------------
 * API -- SEQUENCE (RFC 8881 S18.46)
 * ----------------------------------------------------------------------- */

/**
 * Validate a SEQUENCE operation.
 *
 * Checks the session ID, slot ID, and sequence ID for exactly-once
 * semantics.  On success the slot's seq_id is advanced.
 *
 * @param st               Session table.
 * @param session_id       16-byte session ID.
 * @param slot_id          Slot within the forechannel.
 * @param seq_id           Sequence number for this slot.
 * @param highest_slot_id  Client's highest_slot hint (informational).
 * @param out_highest_slot Receives server's highest_slot_id.
 * @param out_target_slot  Receives server's target_highest_slot_id.
 * @param out_status_flags Receives SEQ4_STATUS_* flags.
 * @param out_clientid     Receives the clientid owning this session (optional).
 * @return 0 on success (new request).
 *         1 = replay (seq_id matches last completed; caller should
 *             return cached result if available, else NFS4_OK).
 *         -1 = NFS4ERR_BADSESSION.
 *         -2 = NFS4ERR_BADSLOT.
 *         -3 = NFS4ERR_SEQ_MISORDERED.
 */
/** Cache the serialized COMPOUND reply for slot-based DRC. */
int session_slot_cache_reply(struct session_table *st,
			     const uint8_t session_id[SESSION_ID_SIZE],
			     uint32_t slot_id,
			     const uint8_t *reply, uint32_t reply_len);

/** Retrieve the cached reply for a replayed slot. */
int session_slot_get_cached_reply(struct session_table *st,
				  const uint8_t session_id[SESSION_ID_SIZE],
				  uint32_t slot_id,
				  const uint8_t **out_reply,
				  uint32_t *out_len);

int session_sequence_check(struct session_table *st,
			   const uint8_t session_id[SESSION_ID_SIZE],
			   uint32_t slot_id,
			   uint32_t seq_id,
			   uint32_t highest_slot_id,
			   uint32_t *out_highest_slot,
			   uint32_t *out_target_slot,
			   uint32_t *out_status_flags,
			   uint64_t *out_clientid);

/* -----------------------------------------------------------------------
 * API -- Backchannel connection binding
 * ----------------------------------------------------------------------- */

/**
 * Bind an RPC connection as a session's backchannel transport.
 *
 * The session stores a borrowed pointer to the connection.  The caller
 * must call session_unbind_conn() when the connection closes.
 *
 * @param st   Session table.
 * @param session_id  16-byte session ID.
 * @param conn        Live RPC connection to bind.
 * @return 0 on success, -1 if session not found.
 */
int session_bind_conn(struct session_table *st,
		      const uint8_t session_id[SESSION_ID_SIZE],
		      struct rpc_conn *conn);

/**
 * Unbind an RPC connection from all sessions that reference it.
 *
 * Called when a client connection closes.  Any session whose cb_conn
 * matches @conn will have cb_conn set to NULL.
 *
 * @param st   Session table.
 * @param conn Connection being torn down.
 *
 * Thread safety: acquires the session table lock.
 */
void session_unbind_conn(struct session_table *st, const struct rpc_conn *conn);


/* -----------------------------------------------------------------------
 * API -- Callback target enumeration (layout recall support)
 *
 * The recall coordinator needs to send CB_LAYOUTRECALL to clients that
 * hold affected layouts.  Because cb_conn is a borrowed pointer whose
 * lifetime is tied to the session table lock, callers must NOT do
 * network I/O while holding the lock.
 *
 * The safe pattern:
 *   1. Under the session table lock, collect lightweight snapshots of
 *      the callback-capable sessions (fd, program, session_id, etc.).
 *   2. Release the lock.
 *   3. Perform I/O using the snapshotted fd values.
 *
 * session_for_each_with_cb() implements step 1: it iterates all sessions
 * that have a non-NULL cb_conn and invokes @cb for each, passing a
 * snapshot of the data needed for the callback call.  The callback
 * MUST NOT do network I/O -- it should only copy data into a collector.
 * ----------------------------------------------------------------------- */

/**
 * Snapshot of a session's callback channel metadata.
 *
 * Collected under the session table lock.  The cb_conn pointer is
 * borrowed and valid ONLY for the duration of the callback invocation.
 */
struct session_cb_snap {
    uint64_t clientid;
    uint8_t  session_id[SESSION_ID_SIZE];
    uint32_t cb_prog;
    uint32_t cb_sec_flavor;
    /*
     * RFC 8881 S2.10.8.3 -- captured callback security parameters.
     * Snapped by value so the CB I/O thread doesn't need to take
     * the session-table lock when building the RPC credential.
     */
    struct nfs4_cb_sec cb_sec;
    const struct rpc_conn *cb_conn; /**< Borrowed ptr -- valid only during callback */
    uint32_t slot_seq_id;    /**< Current seq_id for backchannel slot 0 */
    /*
     * RFC 8881 S20.1 -- negotiated NFSv4 minor version.  Carried in
     * the snapshot so the CB encoder can populate the CB_COMPOUND
     * `minorversion` field with the value the client expects on
     * its callback receiver (a mismatch yields
     * NFS4ERR_MINOR_VERS_MISMATCH at the client's CB compound
     * dispatch).
     */
    uint32_t minorversion;
};

/**
 * Callback type for session_for_each_with_cb().
 *
 * Invoked under the session table lock for each session that has a
 * bound backchannel (cb_conn != NULL).  The callback MUST NOT perform
 * any blocking or network I/O.
 *
 * @param snap  Snapshot of the session's callback channel state.
 * @param ctx   Opaque user context.
 * @return 0 to continue iteration, non-zero to stop early.
 */
typedef int (*session_cb_snap_fn)(const struct session_cb_snap *snap,
                                  void *ctx);

/**
 * Enumerate all sessions with a bound backchannel.
 *
 * Acquires the session table lock, iterates all sessions, and for each
 * session where cb_conn != NULL, builds a session_cb_snap (including
 * a dup'd fd) and invokes @cb.
 *
 * If @cb returns non-zero, iteration stops and that value is returned.
 * On success (all sessions visited), returns 0.
 *
 * @param st   Session table (NULL tolerated -- returns 0 immediately).
 * @param cb   Callback invoked per session (must not do network I/O).
 * @param ctx  Opaque user context passed to @cb.
 * @return 0 on success, or the non-zero value returned by @cb.
 */
int session_for_each_with_cb(struct session_table *st,
                             session_cb_snap_fn cb, void *ctx);

/**
 * Enumerate sessions belonging to a single clientid, most recent first.
 *
 * Walks the per-client `c->sessions` list (which is head-inserted by
 * session_create_session, so the head is the most recently created
 * session) and invokes @cb on the FIRST session that has cb_conn !=
 * NULL.  Iteration stops at the first invocation regardless of @cb's
 * return value.
 *
 * Why this exists, separately from session_for_each_with_cb:
 *
 *   The global iterator visits every session-with-backchannel for
 *   every callback emission.  When a holder client just remounted,
 *   its old session may still be in the hash with a stale cb_conn
 *   (the epoll dispatch has not yet processed EPOLLHUP on the dead
 *   socket fd).  The global iterator can pick that stale session
 *   first; the kernel's new mount has no record of the stale
 *   session_id and rejects the CB_SEQUENCE with NFS4ERR_BADSESSION.
 *
 *   Walking c->sessions head-first selects the newest session
 *   (which the kernel client created on its current mount) and
 *   skips lingering stale entries, eliminating the BADSESSION
 *   window.
 *
 * On @cb return == 1 (snap consumed), the slot-0 sequenceid is
 * advanced by 1 under the session-table lock so subsequent CBs on
 * the same session use a fresh, monotonic sa_sequenceid per RFC
 * 8881 S18.46.4.
 *
 * @param st         Session table (NULL tolerated -- returns 0).
 * @param clientid   Target client.
 * @param cb         Callback (must not do network I/O).
 * @param ctx        Opaque user context.
 * @return 0 if no session was visited (no client, no live cb_conn),
 *         otherwise the value @cb returned.
 */
int session_for_each_with_cb_for_clientid(struct session_table *st,
                                          uint64_t clientid,
                                          session_cb_snap_fn cb,
                                          void *ctx);

/**
 * Update the stored callback security parameters for a session.
 *
 * Used by op_create_session and op_backchannel_ctl (RFC 8881 S18.33).
 * The new parms replace the previous value verbatim under the session-
 * table lock.  Subsequent CB_COMPOUND calls use the new parms via
 * snap.cb_sec.
 *
 * @param st          Session table.
 * @param session_id  Target session.
 * @param sec         New parms (NULL clears to zero/AUTH_NONE).
 * @return 0 on success, -1 if session not found.
 */
int session_set_cb_sec(struct session_table *st,
                       const uint8_t session_id[SESSION_ID_SIZE],
                       const struct nfs4_cb_sec *sec);

/**
 * Update the callback program number for a session.
 *
 * Used by op_backchannel_ctl when the client supplies a new cb_prog
 * (RFC 8881 S18.33.1 bca_cb_program).
 */
int session_set_cb_prog(struct session_table *st,
                        const uint8_t session_id[SESSION_ID_SIZE],
                        uint32_t cb_prog);

/**
 * RFC 8881 S18.50 DESTROY_CLIENTID -- destroy a clientid record.
 *
 * Looks up the clientid in the session table.  If absent, returns -1
 * so the caller can map to NFS4ERR_STALE_CLIENTID (pynfs DESCID3/4/8).
 * If present but the client still has confirmed sessions, returns -2
 * so the caller can map to NFS4ERR_CLIENTID_BUSY (pynfs DESCID5/6) --
 * the client must DESTROY_SESSION on each session first.
 *
 * Otherwise the client record (and any unconfirmed sessions, none in
 * a well-behaved client) is unhashed and freed; subsequent lookups of
 * the same clientid will return -1.  Confirmed-but-empty (sessions
 * destroyed) clients are also eligible for destruction here.
 *
 * @param st        Session table (NULL tolerated; returns -1).
 * @param clientid  Client identifier to destroy.
 * @return  0 on success, -1 if not found (STALE_CLIENTID),
 *         -2 if has confirmed sessions (CLIENTID_BUSY).
 */
int session_destroy_client(struct session_table *st, uint64_t clientid);

#endif /* SESSION_H */
/* Lease expiry reaper (R2.2). */
int session_table_start_reaper(struct session_table *st);
void session_table_stop_reaper(struct session_table *st);

struct open_state_table;
struct lock_table;
void session_table_set_ot(struct session_table *st,
                          struct open_state_table *ot);
void session_table_set_lt(struct session_table *st,
                          struct lock_table *lt);

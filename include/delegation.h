/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * delegation.h -- NFSv4.1 file delegation manager.
 *
 * Tracks READ and WRITE delegations granted to clients via OPEN.
 * When a conflicting operation arrives from another client, the
 * delegation is recalled via CB_RECALL on the backchannel.
 *
 * Thread-safe via 16 striped mutexes (hash on fileid).
 * See RFC 8881 S10.4.
 */

#ifndef DELEGATION_H
#define DELEGATION_H

#include <stdint.h>
#include <stdbool.h>
#include "open_state.h"  /* struct nfs4_stateid */

/* Delegation types (RFC 8881 S18.16.4 -- open_delegation_type4,
 * extended by RFC 7862 S15.5 -- NFSv4.2 attribute delegations).
 *
 *   0 OPEN_DELEGATE_NONE              -- v4.0 / fallback (void body)
 *   1 OPEN_DELEGATE_READ              -- read deleg
 *   2 OPEN_DELEGATE_WRITE             -- write deleg
 *   3 OPEN_DELEGATE_NONE_EXT          -- v4.1+ "declined" with reason
 *   4 OPEN_DELEGATE_READ_ATTRS_DELEG  -- v4.2 attr-only deleg (NOT us)
 *   5 OPEN_DELEGATE_WRITE_ATTRS_DELEG -- v4.2 attr-only deleg (NOT us)
 *
 * The encoder selects between OPEN_DELEGATE_NONE and
 * OPEN_DELEGATE_NONE_EXT based on the session minorversion: v4.0
 * sessions get bare NONE (void body); v4.1+ sessions whose request
 * carried any WANT_* hint, or whose grant was declined for a known
 * reason, get NONE_EXT with the appropriate ond_why.
 *
 * IMPORTANT: do NOT confuse NONE_EXT (3) with READ_ATTRS_DELEG (4).
 * An earlier revision of this header used 4 for NONE_EXT; that
 * value collides with the v4.2 attribute-delegation discriminator,
 * caused Wireshark to dissect our "declined" replies as
 * READ_ATTRS_DELEG, and made pynfs parse the open_none_delegation4
 * body as the (longer) attr-deleg body -- producing EOFError on
 * every OPEN reply.  RFC numbers are the source of truth. */
#define OPEN_DELEGATE_NONE                 0
#define OPEN_DELEGATE_READ                 1
#define OPEN_DELEGATE_WRITE                2
#define OPEN_DELEGATE_NONE_EXT             3   /* RFC 8881 S18.16.4 */
#define OPEN_DELEGATE_READ_ATTRS_DELEG     4   /* RFC 7862 S15.5  (unused) */
#define OPEN_DELEGATE_WRITE_ATTRS_DELEG    5   /* RFC 7862 S15.5  (unused) */

/*
 * why_no_delegation4 codes (RFC 8881 S18.16.4).  Only valid when
 * the open_delegation4 type is OPEN_DELEGATE_NONE_EXT.  WND4_NOT_WANTED
 * is the most common reason -- the client set OPEN4_SHARE_ACCESS_WANT_NO_DELEG
 * (or WANT_CANCEL).  WND4_CONTENTION and WND4_RESOURCE carry an
 * additional bool tail (ond_server_will_push_deleg /
 * ond_server_will_signal_avail).  All other variants have a void tail.
 */
#define WND4_NOT_WANTED         0
#define WND4_CONTENTION         1
#define WND4_RESOURCE           2
#define WND4_NOT_SUPP_FTYPE     3
#define WND4_WRITE_DELEG_NOT_SUPP_FTYPE 4
#define WND4_NOT_SUPP_UPGRADE   5
#define WND4_NOT_SUPP_DOWNGRADE 6
#define WND4_CANCELLED          7
#define WND4_IS_DIR             8

struct nfs4_session;  /* Forward -- for CB_RECALL delivery. */
struct mds_catalogue;
struct session_table; /* Forward -- for backchannel snapshot lookup. */

/** Opaque delegation table handle. */
struct deleg_table;

/**
 * Create the delegation table.
 *
 * @param mds_id  MDS node ID (for stateid generation).
 * @param out     Receives the handle.
 * @return 0 on success, -1 on error.
 */
int deleg_table_init(uint32_t mds_id, struct deleg_table **out);

/**
 * Destroy the delegation table and free all state.
 * NULL-safe.
 */
void deleg_table_destroy(struct deleg_table *dt);

/** Attach catalogue for RonDB-backed shared delegation state. */
void deleg_table_set_cat(struct deleg_table *dt,
                         struct mds_catalogue *cat,
                         uint64_t boot_epoch);

/**
 * Attach the session table.
 *
 * Required for deleg_recall_file() to send CB_RECALL on a holder's
 * backchannel.  Without it, the recall path falls back to revoke-
 * without-CB (the legacy behaviour pre-Phase "properly fix delegation
 * conflict-recall").
 *
 * The session table is borrowed; the caller retains ownership and
 * must outlive the deleg_table.  NULL-safe: clears any prior binding.
 */
void deleg_table_set_session_table(struct deleg_table *dt,
                                   struct session_table *st);

/**
 * Toggle transient (in-memory-only) delegation state.
 *
 * When @p skip is true, deleg_grant records the delegation in the
 * in-memory hash table but does NOT persist it to RonDB.  This mirrors
 * the `skip_transient_ndb` path used elsewhere in the MDS; the
 * delegation remains valid for the life of the daemon.  A client that
 * reconnects after a daemon restart simply does not see the grant and
 * may re-open to obtain a new one.
 *
 * Safe to call after init.  NULL-safe.
 */
void deleg_table_set_skip_transient(struct deleg_table *dt, bool skip);

/**
 * Grant a delegation for (clientid, fileid).
 *
 * Generates a unique stateid and records the grant.
 * Caller must have already verified no conflicting delegations
 * exist (via deleg_check_conflict).
 *
 * @param dt         Table handle.
 * @param clientid   Owning client.
 * @param fileid     Delegated file.
 * @param deleg_type OPEN_DELEGATE_READ or OPEN_DELEGATE_WRITE.
 * @param session    Client session (for future CB_RECALL; may be NULL).
 * @param out_sid    Receives the delegation stateid.
 * @return 0 on success, -1 on error.
 */
int deleg_grant(struct deleg_table *dt,
                uint64_t clientid, uint64_t fileid,
                uint32_t deleg_type,
                struct nfs4_session *session,
                struct nfs4_stateid *out_sid);

/**
 * Return (voluntarily release) a delegation by stateid.
 *
 * Called by op_delegreturn().  Validates the stateid exists.
 *
 * @return 0 on success, -1 if not found (NFS4ERR_BAD_STATEID).
 */
int deleg_return(struct deleg_table *dt,
                 const struct nfs4_stateid *stateid,
                 uint64_t clientid);

/**
 * Check for conflicting delegations on a file.
 *
 * A conflict exists if:
 * - Another client holds ANY delegation on the file (READ or WRITE).
 * - The same client holds a WRITE delegation and the operation is
 *   a conflicting OPEN from the same client (no conflict per RFC).
 *
 * @param dt          Table handle.
 * @param fileid      File to check.
 * @param clientid    Client performing the conflicting operation.
 * @param has_conflict  Out: true if a conflicting delegation exists.
 * @return 0 on success, -1 on error.
 */
int deleg_check_conflict(struct deleg_table *dt,
                         uint64_t fileid, uint64_t clientid,
                         bool *has_conflict);

/**
 * Recall all delegations on a file held by OTHER clients.
 *
 * Sends CB_RECALL to each holding client's session, then waits
 * up to timeout_ms for voluntary DELEGRETURN.  On timeout,
 * the delegation is revoked (deleted from table).
 *
 * @param dt          Table handle.
 * @param fileid      File to recall.
 * @param clientid    Requesting client (skip recall for same client).
 * @param timeout_ms  Max wait for DELEGRETURN (0 = default 5000).
 * @return Number of delegations recalled/revoked, or -1 on error.
 */
int deleg_recall_file(struct deleg_table *dt,
                      uint64_t fileid, uint64_t clientid,
                      uint32_t timeout_ms);

/**
 * Revoke all delegations held by a client (lease expiry cleanup).
 *
 * @param dt        Table handle.
 * @param clientid  Client whose delegations to revoke.
 */
void deleg_revoke_client(struct deleg_table *dt, uint64_t clientid);

/**
 * Revoke every delegation recorded against a fileid.
 *
 * Intended for the final-unlink path (op_remove) and any other
 * code path that destroys the underlying object: once the file
 * is gone, every grant against it is meaningless and must be
 * dropped to free the per-grant struct deleg_entry -- otherwise
 * the table grows without bound under open/unlink workloads
 * (heaptrack confirmed ~80 B per OPEN that takes the grant
 * branch is retained forever in deleg_grant).
 *
 * Does NOT issue CB_RECALL: the file no longer exists; the
 * caller has already committed the namespace mutation and any
 * client read/write against the stale stateid will fail at the
 * DS with a benign error.  This mirrors how deleg_revoke_client
 * cleans up on lease expiry.
 *
 * Idempotent and NULL-safe.  Locks only the stripe owning the
 * fileid's bucket -- safe to call from inside any compound op.
 *
 * @param dt      Table handle (NULL is a no-op).
 * @param fileid  File whose grants to drop.
 */
void deleg_revoke_file(struct deleg_table *dt, uint64_t fileid);

/**
 * Check if a delegation stateid exists (for TEST_STATEID).
 */
bool deleg_stateid_exists(const struct deleg_table *dt,
                          const uint8_t other[12]);

/**
 * RFC 8881 §10.2.1: check if a stateid belongs to a revoked delegation.
 * Returns true if the stateid was revoked (caller should return
 * NFS4ERR_DELEG_REVOKED).
 */
bool deleg_is_revoked(const struct deleg_table *dt,
                      const uint8_t other[12]);

/**
 * RFC 8881 §10.2.1: check if the client has any revoked delegation.
 * Used by SEQUENCE to set SEQ4_STATUS_RECALLABLE_STATE_REVOKED.
 */
bool deleg_client_has_revoked(const struct deleg_table *dt,
                              uint64_t clientid);

/**
 * RFC 8881 §18.38: free a revoked delegation stateid.
 * Returns 0 if removed, -1 if not found.
 */
int deleg_free_revoked(struct deleg_table *dt,
                       const uint8_t other[12]);

/**
 * RFC 8881 §20.1: issue CB_GETATTR to a WRITE delegation holder.
 *
 * If @p fileid has an active WRITE delegation from a client other
 * than @p requesting_clientid, snapshot the holder's backchannel
 * metadata, send CB_GETATTR, receive the reply, and return the
 * holder's reported size + change.
 *
 * @param dt                 Delegation table (NULL → -1).
 * @param fileid             Target file.
 * @param requesting_clientid Client doing GETATTR (exclude from CB).
 * @param out_size           Receives holder's size.
 * @param out_change         Receives holder's change.
 * @return 0 on success, -1 if no write deleg or CB failed.
 */
int deleg_cb_getattr_for_file(struct deleg_table *dt,
                              uint64_t fileid,
                              uint64_t requesting_clientid,
                              uint64_t *out_size,
                              uint64_t *out_change);

#endif /* DELEGATION_H */

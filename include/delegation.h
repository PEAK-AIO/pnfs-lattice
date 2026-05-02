/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * delegation.h — NFSv4.1 file delegation manager.
 *
 * Tracks READ and WRITE delegations granted to clients via OPEN.
 * When a conflicting operation arrives from another client, the
 * delegation is recalled via CB_RECALL on the backchannel.
 *
 * Thread-safe via 16 striped mutexes (hash on fileid).
 * See RFC 8881 §10.4.
 */

#ifndef DELEGATION_H
#define DELEGATION_H

#include <stdint.h>
#include <stdbool.h>
#include "open_state.h"  /* struct nfs4_stateid */

/* Delegation types (RFC 8881 §18.16.4 — open_delegation_type4). */
#define OPEN_DELEGATE_NONE   0
#define OPEN_DELEGATE_READ   1
#define OPEN_DELEGATE_WRITE  2

struct nfs4_session;  /* Forward — for CB_RECALL delivery. */
struct mds_catalogue;

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
 * dropped to free the per-grant struct deleg_entry — otherwise
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
 * fileid's bucket — safe to call from inside any compound op.
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

#endif /* DELEGATION_H */

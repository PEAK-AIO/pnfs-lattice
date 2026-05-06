/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * dir_delegation.h -- NFSv4.1 directory delegation manager (RFC 8881 S10.9).
 *
 * Tracks directory delegations granted to clients via GET_DIR_DELEGATION.
 * When a conflicting namespace mutation arrives, the delegation is
 * recalled via CB_RECALL on the backchannel (Phase 8b) or notified via
 * CB_NOTIFY (Phase 8c onward).
 *
 * The state table is keyed on (dir_fileid, clientid) and protected by
 * striped mutexes -- same pattern as file-delegation tracking in
 * delegation.c, but with one entry per (dir, client) rather than per
 * (fileid, single-writer).
 *
 * Thread-safe.  NULL-safe on destroy / revoke / return.
 */
#ifndef DIR_DELEGATION_H
#define DIR_DELEGATION_H

#include <stdint.h>
#include <stdbool.h>
#include "pnfs_mds.h"     /* NFS4_VERIFIER_SIZE */
#include "open_state.h"   /* struct nfs4_stateid */

struct nfs4_session;

/** Opaque directory delegation table handle. */
struct dir_deleg_table;

/**
 * Initialise the directory delegation table.
 *
 * @param mds_id  MDS node ID (used as the high 32 bits of generated
 *                dir-deleg stateids so they are unique across the
 *                cluster).
 * @param out     Receives the handle.
 * @return 0 on success, -1 on allocation failure.
 */
int dir_deleg_table_init(uint32_t mds_id, struct dir_deleg_table **out);

/** Destroy and free the table.  NULL-safe. */
void dir_deleg_table_destroy(struct dir_deleg_table *ddt);

/**
 * Grant a directory delegation for (clientid, dir_fileid).
 *
 * Caller has already verified no conflicting delegation exists on this
 * directory (via dir_deleg_is_writer_present / existing-client check
 * at the GDD call site).  Generates a unique stateid and a cookieverf
 * derived from the current realtime clock.
 *
 * @param ddt               Table handle.
 * @param clientid          Owning client.
 * @param dir_fileid        Directory fileid.
 * @param notification_mask Notification types the client requested
 *                          (intersected with supported set by caller).
 * @param session           Client session for future CB delivery (may
 *                          be NULL; recall becomes silent revoke).
 * @param out_stateid       Receives the granted stateid.
 * @param out_cookieverf    Receives the 8-byte cookieverf.
 * @return 0 on success, -1 on allocation failure or duplicate grant.
 */
int dir_deleg_grant(struct dir_deleg_table *ddt,
		    uint64_t clientid, uint64_t dir_fileid,
		    uint32_t notification_mask,
		    struct nfs4_session *session,
		    struct nfs4_stateid *out_stateid,
		    uint8_t out_cookieverf[NFS4_VERIFIER_SIZE]);

/**
 * Voluntary release by a client (DELEGRETURN on a dir-deleg stateid).
 *
 * @return 0 on success, -1 if no matching delegation found.
 */
int dir_deleg_return(struct dir_deleg_table *ddt,
		     const struct nfs4_stateid *stateid,
		     uint64_t clientid);

/**
 * Check whether another client already holds a writer-type delegation
 * on @p dir_fileid.  Phase 8b uses this as the conflict gate at GDD
 * time: if present, the handler returns UNAVAIL rather than granting
 * a second delegation that would be immediately recalled.
 *
 * @param ddt         Table handle.
 * @param dir_fileid  Directory to check.
 * @param clientid    Caller's clientid (same-client grants never
 *                    conflict per RFC 8881 S10.9.2).
 * @return true if a conflicting grant exists, false otherwise.
 */
bool dir_deleg_is_writer_present(struct dir_deleg_table *ddt,
				 uint64_t dir_fileid, uint64_t clientid);

/**
 * Recall every directory delegation held on @p dir_fileid by clients
 * other than @p requesting_clientid.  Sends CB_RECALL best-effort on
 * each holder's session and revokes the grant from the table (error
 * paths revoke unconditionally so the caller can proceed).
 *
 * @return Number of grants revoked, or -1 on error.
 */
int dir_deleg_recall_dir(struct dir_deleg_table *ddt,
			 uint64_t dir_fileid, uint64_t requesting_clientid,
			 uint32_t timeout_ms);

/** Revoke every delegation held by @p clientid (lease expiry cleanup). */
void dir_deleg_revoke_client(struct dir_deleg_table *ddt, uint64_t clientid);

/** TEST_STATEID support -- check whether a dir-deleg stateid exists. */
bool dir_deleg_stateid_exists(const struct dir_deleg_table *ddt,
			      const uint8_t other[12]);

struct session_table;

/**
 * Attach the session table so the emitter can resolve callback
 * metadata at notify / recall time.  Safe to call at any point; the
 * pointer is borrowed (caller owns the table's lifetime).
 */
void dir_deleg_table_set_session_table(struct dir_deleg_table *ddt,
				       struct session_table *st);

/**
 * Override the default recall / notify timeout used when callers
 * pass timeout_ms=0 to dir_deleg_recall_dir() or
 * dir_deleg_notify_dir().  Wired from the INI
 * `dir_deleg_recall_timeout_ms` key at daemon startup.  Values
 * below 50ms are clamped to 50ms so a misconfiguration cannot
 * turn every CB into an immediate failure.
 */
void dir_deleg_set_default_timeout(uint32_t timeout_ms);

/**
 * Per-event notification dispatcher for namespace mutations.
 *
 * For every dir_fileid delegation held by a client other than
 * @p requesting_clientid:
 *   - If the delegation's granted_mask covers @p event (one of
 *     NOTIFY4_REMOVE_ENTRY / _ADD_ENTRY / _RENAME_ENTRY), emit
 *     CB_NOTIFY and keep the delegation.
 *   - Else (the client cannot handle this event), send CB_RECALL
 *     and revoke the delegation locally.
 *
 * If CB_NOTIFY delivery fails (ENOTCONN / EIO / timeout), fall back
 * to recall + revoke for that entry so the caller can proceed.
 *
 * @param event     One of NOTIFY4_REMOVE_ENTRY / _ADD_ENTRY / _RENAME_ENTRY.
 * @param old_name  First name (NULL for ADD_ENTRY; required for the others).
 * @param new_name  Second name (required for RENAME_ENTRY; ignored otherwise;
 *                  the ADD_ENTRY case uses old_name as the added child).
 * @return Number of delegations serviced (notified + revoked), or -1
 *         on programming error.  Not currently surfaced to callers.
 */
int dir_deleg_notify_dir(struct dir_deleg_table *ddt,
			 uint64_t dir_fileid,
			 uint64_t requesting_clientid,
			 uint32_t event,
			 const char *old_name,
			 const char *new_name,
			 uint32_t timeout_ms);

/** Prometheus counters exported for tests and metrics exposition. */
struct dir_deleg_counters {
	uint64_t granted;
	uint64_t recalled;
	uint64_t revoked;
	uint64_t returned;
	uint64_t conflict_unavail;
	uint64_t notify_sent;
	uint64_t notify_failed;
};

void dir_deleg_counters_snapshot(const struct dir_deleg_table *ddt,
				 struct dir_deleg_counters *out);

#endif /* DIR_DELEGATION_H */

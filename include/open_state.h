/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * open_state.h -- NFSv4.1 open state and stateid management.
 *
 * Implements OPEN/CLOSE stateid tracking and share reservation
 * conflict detection per RFC 8881 SS8.2, 9.1, 18.16, 18.2.
 *
 * Each OPEN allocates a stateid.  Share reservations are enforced:
 * a new OPEN that conflicts with an existing share_deny is rejected
 * with NFS4ERR_SHARE_DENIED.
 *
 * See docs/architecture.md S4.3 for design overview.
 */

#ifndef OPEN_STATE_H
#define OPEN_STATE_H

#include <stdint.h>
#include <stdbool.h>

/* -----------------------------------------------------------------------
 * Stateid (RFC 8881 S8.2)
 *
 *   0          3  4          15
 *   +----------+--+----------+
 *   |  seqid   |    other    |
 *   +----------+--+----------+
 *
 * seqid: incremented on each state-changing operation.
 * other: opaque 12-byte identifier unique per MDS.
 * ----------------------------------------------------------------------- */

#define NFS4_STATEID_SIZE   16
#define NFS4_OTHER_SIZE     12

/** Maximum open-owner opaque bytes (RFC 8881 S18.16.2). */
#define NFS4_OPEN_OWNER_MAX 128

struct nfs4_stateid {
    uint32_t seqid;
    uint8_t  other[NFS4_OTHER_SIZE];
};

/* Special stateids (RFC 8881 S8.2.3). */
#define NFS4_STATEID_ANON_SEQID  0
#define NFS4_STATEID_ANON_OTHER  "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"

/* -----------------------------------------------------------------------
 * Share access and deny modes (RFC 8881 S18.16.3)
 * ----------------------------------------------------------------------- */

#define OPEN4_SHARE_ACCESS_READ    0x0001U
#define OPEN4_SHARE_ACCESS_WRITE   0x0002U
#define OPEN4_SHARE_ACCESS_BOTH    0x0003U

/* RFC 8881 S18.16.3 share_access3 -- mask to extract the basic
 * READ/WRITE bits from the full share_access field on the wire.  The
 * remaining bits encode delegation hints (WANT_*) and when-to-deliver
 * hints (SIGNAL/PUSH); they MUST be masked off before comparison
 * against the OPEN_BOTH constant or stored as the canonical share
 * mode in open_state. */
#define OPEN4_SHARE_ACCESS_BASIC_MASK   0x0003U

/*
 * RFC 8881 S18.16.3 OPEN4_SHARE_ACCESS_WANT_* hints -- client preference
 * for the kind of delegation it wants (or doesn't want) on this OPEN.
 * Encoded in bits 8-11 of the share_access field.  Server-side semantics:
 *
 *   WANT_NO_PREFERENCE       Server picks; default if the client did
 *                            not set any WANT bit.
 *   WANT_READ_DELEG          Server SHOULD grant a READ delegation if
 *                            possible.
 *   WANT_WRITE_DELEG         Server SHOULD grant a WRITE delegation if
 *                            possible.
 *   WANT_ANY_DELEG           Either is fine.
 *   WANT_NO_DELEG            Server MUST NOT grant a delegation.  Used
 *                            by clients that would rather not deal with
 *                            the recall path (and by pynfs DELEG4
 *                            "testNoDeleg" to verify the gate).
 *   WANT_CANCEL              Cancel a previous WANT_*_DELEG hint.
 *
 * The when-bits (SIGNAL_DELEG_WHEN_RESRC_AVAIL, PUSH_DELEG_WHEN_UNCONTENDED)
 * are out of scope for this server today; we ignore them but preserve
 * the bit values so a future revision can surface them via OPEN4_RESULT.
 */
/*
 * RFC 7863 S2 (NFSv4.2 XDR) -- the canonical mask spans bits 8-15:
 *   const OPEN4_SHARE_ACCESS_WANT_DELEG_MASK = 0xFF00;
 * In current use only the low nibble is populated (NO_PREFERENCE,
 * READ_DELEG, WRITE_DELEG, ANY_DELEG, NO_DELEG, CANCEL = 0x0..0x500),
 * but the spec reserves the full byte for future extensions.  We use
 * 0xFF00 verbatim so a future hint value at e.g. 0x0600 is still
 * captured correctly by the client-hint extraction in op_open.
 */
#define OPEN4_SHARE_ACCESS_WANT_DELEG_MASK         0xFF00U
#define OPEN4_SHARE_ACCESS_WANT_NO_PREFERENCE      0x0000U
#define OPEN4_SHARE_ACCESS_WANT_READ_DELEG         0x0100U
#define OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG        0x0200U
#define OPEN4_SHARE_ACCESS_WANT_ANY_DELEG          0x0300U
#define OPEN4_SHARE_ACCESS_WANT_NO_DELEG           0x0400U
#define OPEN4_SHARE_ACCESS_WANT_CANCEL             0x0500U
#define OPEN4_SHARE_ACCESS_WANT_SIGNAL_DELEG_WHEN_RESRC_AVAIL  0x10000U
#define OPEN4_SHARE_ACCESS_WANT_PUSH_DELEG_WHEN_UNCONTENDED    0x20000U

#define OPEN4_SHARE_DENY_NONE      0x0000U
#define OPEN4_SHARE_DENY_READ      0x0001U
#define OPEN4_SHARE_DENY_WRITE     0x0002U
#define OPEN4_SHARE_DENY_BOTH      0x0003U

/* -----------------------------------------------------------------------
 * OPEN4 response flags (RFC 5661 S18.16.4 / RFC 8881 S18.16.4)
 *
 * Carried in the rflags field of the OPEN4resok body.  Hint to the
 * client about server capabilities for the just-opened file.  All
 * bits are advisory; clients use them to enable/disable code paths
 * that would otherwise cost an RPC to discover.
 *
 * Linux specifically gates fcntl(F_SETLK) on LOCKTYPE_POSIX: when
 * the bit is clear, fs/nfs/nfs4proc.c short-circuits every byte-range
 * lock request to ENOLCK without sending a LOCK RPC.  This MDS
 * implements RFC 8881 S18.10 LOCK/LOCKT/LOCKU end-to-end (see
 * compound.c::OP_LOCK and src/mds/lock_state.c), so the bit is set
 * unconditionally in encode_res_open.
 * ----------------------------------------------------------------------- */

#define OPEN4_RESULT_CONFIRM             0x00000002U
#define OPEN4_RESULT_LOCKTYPE_POSIX      0x00000004U
#define OPEN4_RESULT_PRESERVE_UNLINKED   0x00000008U
#define OPEN4_RESULT_MAY_NOTIFY_LOCK     0x00000020U

/* -----------------------------------------------------------------------
 * OPEN claim types (RFC 8881 S18.16.2)
 * ----------------------------------------------------------------------- */

enum nfs4_claim_type {
    CLAIM_NULL        = 0,   /* Open by name in current_fh directory */
    CLAIM_PREVIOUS    = 1,   /* Reclaim open during grace period */
    CLAIM_FH          = 4,   /* Open by current_fh (no name resolution) */
};

/* -----------------------------------------------------------------------
 * OPEN create modes (RFC 8881 S18.16.3)
 * ----------------------------------------------------------------------- */

enum nfs4_createmode {
    CREATEMODE_UNCHECKED4  = 0,  /* Create if absent, open if present */
    CREATEMODE_GUARDED4    = 1,  /* Create only if absent */
    CREATEMODE_EXCLUSIVE4  = 2,  /* Exclusive create with verifier */
    CREATEMODE_EXCLUSIVE4_1 = 3, /* Exclusive create with verifier + attrs */
};

/* -----------------------------------------------------------------------
 * Open state record
 * ----------------------------------------------------------------------- */

struct nfs4_open_state {
    struct nfs4_stateid    stateid;
    uint64_t               clientid;
    uint64_t               fileid;
    uint32_t               share_access;
    uint32_t               share_deny;
    uint8_t                open_owner[NFS4_OPEN_OWNER_MAX];
    uint32_t               open_owner_len;
    struct nfs4_open_state *hash_next;  /* stateid hash chain */
    struct nfs4_open_state *file_next;  /* per-fileid chain (share checks) */
};

/* -----------------------------------------------------------------------
 * Open state table (top-level container)
 * ----------------------------------------------------------------------- */

struct open_state_table;  /* Opaque -- defined in open_state.c */
struct mds_catalogue;     /* Forward for open_state_table_set_cat() */

/* -----------------------------------------------------------------------
 * API -- Lifecycle
 * ----------------------------------------------------------------------- */

/**
 * Initialise the open state manager.
 *
 * @param mds_id  This MDS node's numeric ID (for stateid generation).
 * @param out     Receives the open state table handle.
 * @return 0 on success, -1 on allocation failure.
 *
 * Thread safety: the returned handle is safe to share; all operations
 * are serialised internally via pthread_mutex.
 */
int open_state_table_init(uint32_t mds_id, struct open_state_table **out);

/**
 * Destroy the open state table and free all state.
 *
 * @param ot  Handle from open_state_table_init().  NULL is tolerated.
 */
void open_state_table_destroy(struct open_state_table *ot);


/**
 * Attach a catalogue handle for RonDB-backed shared open state.
 *
 * When set, open_state_open/close dispatch through the coordination
 * vtable.  The local hash table becomes a write-through cache.
 * When NULL (default), operates in local-only mode.
 *
 * @param ot          Open state table.
 * @param cat         Catalogue handle (NULL to disable shared mode).
 * @param boot_epoch  Boot epoch for fencing.
 */
void open_state_table_set_cat(struct open_state_table *ot,
                              struct mds_catalogue *cat,
                              uint64_t boot_epoch);

/**
 * Skip NDB persistence for open/close state.
 *
 * When set, open_state_open/close do not write to RonDB -- the
 * in-memory hash tables are authoritative.  Safe for single-MDS
 * deployments where crash recovery rebuilds from client reclaim.
 *
 * @param ot   Open state table.
 * @param skip true to skip NDB writes.
 */
void open_state_table_set_skip_ndb(struct open_state_table *ot, bool skip);

/* -----------------------------------------------------------------------
 * API -- OPEN (RFC 8881 S18.16)
 * ----------------------------------------------------------------------- */

/**
 * Process an OPEN request -- allocate a stateid and enforce share
 * reservations.
 *
 * The caller has already resolved the target fileid (via CLAIM_NULL
 * name lookup + optional create, or CLAIM_FH).  This function only
 * manages the stateid and share semantics.
 *
 * @param ot              Open state table.
 * @param clientid        Client performing the open.
 * @param open_owner      Opaque open-owner identity (may be NULL).
 * @param open_owner_len  Length of @open_owner in bytes (0 if NULL).
 * @param fileid          Target file.
 * @param share_access    OPEN4_SHARE_ACCESS_* flags.
 * @param share_deny      OPEN4_SHARE_DENY_* flags.
 * @param out_stateid     Receives the new stateid.
 * @return 0 on success.
 *         -1 = NFS4ERR_SHARE_DENIED (share conflict).
 *         -2 = allocation failure (NFS4ERR_RESOURCE).
 *         -3 = invalid parameters.
 */
int open_state_open(struct open_state_table *ot,
                    uint64_t clientid,
                    const uint8_t *open_owner,
                    uint32_t open_owner_len,
                    uint64_t fileid,
                    uint32_t share_access,
                    uint32_t share_deny,
                    struct nfs4_stateid *out_stateid);

/* -----------------------------------------------------------------------
 * API -- CLOSE (RFC 8881 S18.2)
 * ----------------------------------------------------------------------- */

/**
 * Process a CLOSE request -- remove the open state and invalidate
 * the stateid.
 *
 * On success, @out_stateid is set to the stateid with seqid incremented
 * (the "closing" stateid returned to the client; subsequent use is
 * NFS4ERR_BAD_STATEID).
 *
 * @param ot           Open state table.
 * @param clientid     Client performing the close (ownership check).
 * @param stateid      Input stateid from the client.
 * @param out_stateid  Receives the closing stateid.
 * @return 0 on success.
 *         -1 = NFS4ERR_BAD_STATEID (not found, seqid mismatch, or
 *              wrong owner).
 */
int open_state_close(struct open_state_table *ot,
                     uint64_t clientid,
                     const struct nfs4_stateid *stateid,
                     struct nfs4_stateid *out_stateid);

/* -----------------------------------------------------------------------
 * API -- Lookup (for stateid validation in future READ/WRITE/LOCK ops)
 * ----------------------------------------------------------------------- */

/**
 * Look up an open state by stateid and copy it to caller-owned buffer.
 *
 * @param ot       Open state table.
 * @param stateid  Stateid to look up.
 * @param out      Receives a snapshot of the open state record.
 * @return 0 on success, -1 if not found.
 *
 * The record is copied under the lock; the caller may safely use
 * @out after this returns without lifetime concerns.
 */
int open_state_find(struct open_state_table *ot,
                    const struct nfs4_stateid *stateid,
                    struct nfs4_open_state *out);


/**
 * Check whether any client has an open (write) on the given file.
 *
 * @param ot      Open state table.
 * @param fileid  File to check.
 * @return 1 if at least one open exists for fileid, 0 otherwise.
 *         Returns 0 if ot is NULL (feature disabled).
 */
int open_state_file_has_writers(struct open_state_table *ot, uint64_t fileid);

/** Release all open state for a given client (lease expiry). */
int open_state_downgrade(struct open_state_table *ot,
                         uint64_t clientid,
                         const struct nfs4_stateid *stateid,
                         uint32_t new_share_access,
                         uint32_t new_share_deny,
                         struct nfs4_stateid *out_stateid);

void open_state_close_all_for_client(struct open_state_table *ot,
                                     uint64_t clientid);

#endif /* OPEN_STATE_H */

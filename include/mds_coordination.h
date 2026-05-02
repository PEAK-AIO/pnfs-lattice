/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_coordination.h — Recovery-critical coordination state interface.
 *
 * Stage 2 split:
 *   - mds_catalogue.h covers catalogue data (namespace, stripe maps,
 *     DS registry, quota, GC queue, etc.).
 *   - This header covers recovery-critical coordination state
 *     (shared 2PC journal, layout_state, ds_layout_idx, client_recovery).
 *   - Ephemeral/optional state such as open_state, sessions, and DRC
 *     remains outside both interfaces.
 */

#ifndef MDS_COORDINATION_H
#define MDS_COORDINATION_H

#include <stdbool.h>
#include <stdint.h>

#include "pnfs_mds.h"

struct mds_catalogue;
struct mds_cat_txn;
struct nfs4_stateid;

/* -----------------------------------------------------------------------
 * Callback types for scan/iteration operations
 * ----------------------------------------------------------------------- */

/**
 * Maximum coordination-journal payload size.
 *
 * The current largest payload is the rename 2PC inode snapshot plus
 * optional single-file stripe-map payload.
 */
#define MDS_COORD_JOURNAL_PAYLOAD_MAX \
    (sizeof(struct mds_inode) + 12 + sizeof(struct mds_ds_map_entry) * 16)

/**
 * Shared 2PC journal row used by cross-MDS rename and hard-link flows.
 *
 * The field names follow the historical rename-journal layout. Callers
 * that are not rename-specific may repurpose them as needed.
 */
struct mds_coord_journal_record {
    uint64_t txn_id;
    uint8_t  state;
    uint8_t  role;
    uint32_t remote_mds_id;
    uint64_t src_parent_fileid;
    uint64_t dst_parent_fileid;
    uint64_t src_child_fileid;
    char     src_name[MDS_MAX_NAME + 1];
    char     dst_name[MDS_MAX_NAME + 1];
    uint32_t payload_len;
    uint8_t  payload[MDS_COORD_JOURNAL_PAYLOAD_MAX];
    uint64_t created_at_ns;
};

/** Callback for mds_coord_journal_scan(). */
typedef int (*mds_coord_journal_scan_cb)(
    const struct mds_coord_journal_record *record, void *ctx);

/** Callback for mds_coord_ds_layout_idx_scan(). */
typedef int (*mds_coord_ds_layout_cb)(uint64_t clientid,
				      uint64_t fileid, void *ctx);

/** Callback for mds_coord_layout_iter_file(). */
typedef int (*mds_coord_layout_file_iter_cb)(
	uint64_t clientid, const struct nfs4_stateid *stateid,
	uint32_t iomode, void *ctx);

/* -----------------------------------------------------------------------
 * Shared coordination journal
 * ----------------------------------------------------------------------- */

enum mds_status mds_coord_journal_put(
    struct mds_catalogue *cat,
    struct mds_cat_txn *txn,
    const struct mds_coord_journal_record *record);

enum mds_status mds_coord_journal_get(
    struct mds_catalogue *cat,
    struct mds_cat_txn *txn,
    uint64_t txn_id,
    uint8_t role,
    struct mds_coord_journal_record *record);

enum mds_status mds_coord_journal_del(
    struct mds_catalogue *cat,
    struct mds_cat_txn *txn,
    uint64_t txn_id,
    uint8_t role);

enum mds_status mds_coord_journal_scan(
    struct mds_catalogue *cat,
    mds_coord_journal_scan_cb cb,
    void *ctx);

/* -----------------------------------------------------------------------
 * Layout state
 * ----------------------------------------------------------------------- */

enum mds_status mds_coord_layout_grant(struct mds_catalogue *cat,
				       struct mds_cat_txn *txn,
				       uint64_t clientid,
				       uint64_t fileid,
				       uint32_t iomode,
				       uint64_t offset,
				       uint64_t length,
				       const struct nfs4_stateid *stateid,
				       const uint32_t *ds_ids,
				       uint32_t ds_count);

enum mds_status mds_coord_layout_return(struct mds_catalogue *cat,
					struct mds_cat_txn *txn,
					const uint8_t stateid_other[12],
					uint64_t clientid,
					uint64_t fileid,
					const uint32_t *ds_ids,
					uint32_t ds_count);

enum mds_status mds_coord_layout_get_by_stateid(
	struct mds_catalogue *cat,
	const uint8_t stateid_other[12],
	uint64_t *clientid, uint64_t *fileid,
	uint32_t *iomode, uint64_t *offset,
	uint64_t *length, uint32_t *seqid);

enum mds_status mds_coord_layout_scan_for_file(
	struct mds_catalogue *cat,
	uint64_t fileid, bool *has_layout);

enum mds_status mds_coord_layout_del_all_for_client(
	struct mds_catalogue *cat,
	uint64_t clientid);

/**
 * Scan ds_layout_idx for all (clientid, fileid) pairs on a DS.
 * Used by layout recall to enumerate layouts on a failed DS.
 */
enum mds_status mds_coord_ds_layout_idx_scan(
	struct mds_catalogue *cat, uint32_t ds_id,
	mds_coord_ds_layout_cb cb, void *ctx);

/**
 * Iterate all layout holders for a file.
 * Used by layout recall to enumerate holders before revocation.
 */
enum mds_status mds_coord_layout_iter_file(
	struct mds_catalogue *cat, uint64_t fileid,
	mds_coord_layout_file_iter_cb cb, void *ctx);

/* -----------------------------------------------------------------------
 * Client recovery
 * ----------------------------------------------------------------------- */

enum mds_status mds_coord_recovery_put(struct mds_catalogue *cat,
				       struct mds_cat_txn *txn,
				       uint64_t clientid,
				       const uint8_t *co_ownerid,
				       uint32_t co_ownerid_len,
				       const uint8_t verifier[8]);

enum mds_status mds_coord_recovery_del(struct mds_catalogue *cat,
				       struct mds_cat_txn *txn,
				       uint64_t clientid);

enum mds_status mds_coord_recovery_get(struct mds_catalogue *cat,
				       uint64_t clientid,
				       uint8_t *co_ownerid,
				       uint32_t *co_ownerid_len,
				       uint8_t verifier[8]);

/* -----------------------------------------------------------------------
 * Phase 9D — Recovery ownership bulk operations
 * ----------------------------------------------------------------------- */

/** Callback for recovery_list.  Return 0 to continue, non-zero to stop. */
typedef int (*mds_recovery_list_cb)(uint64_t clientid,
				    uint32_t owner_mds_id,
				    uint64_t owner_boot_epoch,
				    void *ctx);

/** List all recovery records owned by a specific MDS. */
enum mds_status mds_coord_recovery_list(
	struct mds_catalogue *cat, uint32_t owner_mds_id,
	mds_recovery_list_cb cb, void *ctx);

/** Bulk-transfer recovery ownership from dead MDS to self.
 *  Uses compare-and-swap: only transfers rows matching old identity. */
enum mds_status mds_coord_recovery_transfer(
	struct mds_catalogue *cat,
	uint32_t old_mds_id, uint64_t old_boot_epoch,
	uint32_t new_mds_id, uint64_t new_boot_epoch);

/* -----------------------------------------------------------------------
 * Phase 9D — Layout grant-owner queries
 * ----------------------------------------------------------------------- */

/** Callback for layout grant-owner scan. */
typedef int (*mds_layout_grant_owner_cb)(
	const uint8_t stateid_other[12],
	uint64_t clientid, uint64_t fileid,
	uint32_t grant_owner_mds_id,
	void *ctx);

/** List all layouts granted by a specific MDS. */
enum mds_status mds_coord_layout_scan_by_grant_owner(
	struct mds_catalogue *cat, uint32_t grant_owner_mds_id,
	mds_layout_grant_owner_cb cb, void *ctx);

/** Bulk-transfer layout grant ownership from dead MDS to self. */
enum mds_status mds_coord_layout_transfer_grant_owner(
	struct mds_catalogue *cat,
	uint32_t old_mds_id, uint32_t new_mds_id);

/* -----------------------------------------------------------------------
 * Shared protocol state — open/share, locks, delegations, sessions, DRC
 *
 * These operations coordinate NFS protocol state that must be shared
 * across MDS nodes.  Each row carries (owner_mds_id, owner_boot_epoch)
 * for fencing and failover reaping.
 * ----------------------------------------------------------------------- */

/* --- Row structures --------------------------------------------------- */

/** Open/share state row (one per OPEN stateid). */
struct mds_coord_open_row {
    uint8_t   stateid_other[12]; /**< PK: stateid.other. */
    uint32_t  seqid;
    uint64_t  clientid;
    uint64_t  fileid;
    uint32_t  share_access;      /**< OPEN4_SHARE_ACCESS_*. */
    uint32_t  share_deny;        /**< OPEN4_SHARE_DENY_*. */
    uint8_t   open_owner[128];   /**< Opaque open-owner identity. */
    uint32_t  open_owner_len;
    uint32_t  owner_mds_id;      /**< MDS that granted the open. */
    uint64_t  owner_boot_epoch;  /**< Boot epoch for fencing. */
};

/** Callback for open-state scan.  Return 0 to continue, non-zero to stop. */
typedef int (*mds_coord_open_scan_cb)(
    const struct mds_coord_open_row *row, void *ctx);

/** Byte-range lock row (one per granted lock). */
struct mds_coord_lock_row {
    uint64_t  fileid;            /**< PK part 1. */
    uint64_t  lock_id;           /**< PK part 2 (unique within file). */
    uint64_t  offset;
    uint64_t  length;
    uint32_t  lock_type;         /**< READ_LT or WRITE_LT. */
    uint64_t  clientid;
    uint8_t   owner[128];        /**< Lock owner opaque. */
    uint32_t  owner_len;
    uint8_t   stateid_other[12]; /**< Lock stateid. */
    uint32_t  seqid;
    uint8_t   open_stateid_other[12]; /**< Associated open stateid. */
    uint32_t  owner_mds_id;
    uint64_t  owner_boot_epoch;
};

/** Callback for lock-state scan.  Return 0 to continue, non-zero to stop. */
typedef int (*mds_coord_lock_scan_cb)(
    const struct mds_coord_lock_row *row, void *ctx);

/** Delegation row (one per granted delegation). */
struct mds_coord_deleg_row {
    uint8_t   stateid_other[12]; /**< PK: stateid.other. */
    uint32_t  seqid;
    uint64_t  clientid;
    uint64_t  fileid;
    uint32_t  deleg_type;        /**< OPEN_DELEGATE_READ/WRITE. */
    uint32_t  owner_mds_id;      /**< MDS that granted the delegation. */
    uint64_t  owner_boot_epoch;
    uint64_t  grant_time_ns;     /**< Clock at grant time. */
    uint8_t   recall_pending;    /**< 1 if recall in progress. */
};

/** Callback for delegation scan.  Return 0 to continue, non-zero to stop. */
typedef int (*mds_coord_deleg_scan_cb)(
    const struct mds_coord_deleg_row *row, void *ctx);

/** Client identity row. */
struct mds_coord_client_row {
    uint64_t  clientid;          /**< PK. */
    uint8_t   co_ownerid[1024];  /**< Opaque owner identity. */
    uint32_t  co_ownerid_len;
    uint8_t   verifier[8];       /**< Client verifier. */
    bool      confirmed;         /**< Confirmed by CREATE_SESSION. */
    uint32_t  owner_mds_id;
    uint64_t  owner_boot_epoch;
    uint64_t  lease_renewed_ns;  /**< Last lease renewal timestamp. */
};

/** Session row. */
struct mds_coord_session_row {
    uint8_t   session_id[16];    /**< PK. */
    uint64_t  clientid;
    uint32_t  num_slots;
    uint32_t  cb_prog;           /**< Callback program number. */
    uint32_t  cb_sec_flavor;     /**< Callback security flavor. */
    uint32_t  owner_mds_id;
    uint64_t  owner_boot_epoch;
    uint64_t  created_ns;
};

/** Callback for session scan.  Return 0 to continue, non-zero to stop. */
typedef int (*mds_coord_session_scan_cb)(
    const struct mds_coord_session_row *row, void *ctx);

/** DRC slot row (one per forechannel slot). */
struct mds_coord_drc_slot_row {
    uint8_t   session_id[16];    /**< PK part 1. */
    uint32_t  slot_id;           /**< PK part 2. */
    uint32_t  seq_id;            /**< Last completed sequence ID. */
    uint8_t  *cached_reply;      /**< Serialised reply (heap, caller frees). */
    uint32_t  reply_len;
    uint64_t  last_used_ns;
};

/* --- Open/share state ------------------------------------------------- */

enum mds_status mds_coord_open_put(struct mds_catalogue *cat,
    const struct mds_coord_open_row *row);

enum mds_status mds_coord_open_get(struct mds_catalogue *cat,
    const uint8_t stateid_other[12],
    struct mds_coord_open_row *row);

enum mds_status mds_coord_open_del(struct mds_catalogue *cat,
    const uint8_t stateid_other[12]);

/** Scan all open-state rows for a given file (share conflict check). */
enum mds_status mds_coord_open_scan_file(struct mds_catalogue *cat,
    uint64_t fileid, mds_coord_open_scan_cb cb, void *ctx);

/** Scan all open-state rows for a given client (lease cleanup). */
enum mds_status mds_coord_open_scan_client(struct mds_catalogue *cat,
    uint64_t clientid, mds_coord_open_scan_cb cb, void *ctx);

/* --- Byte-range locks ------------------------------------------------- */

enum mds_status mds_coord_lock_put(struct mds_catalogue *cat,
    const struct mds_coord_lock_row *row);

enum mds_status mds_coord_lock_del(struct mds_catalogue *cat,
    uint64_t fileid, uint64_t lock_id);

/** Test for conflicting locks on a file (LOCKT). */
enum mds_status mds_coord_lock_test(struct mds_catalogue *cat,
    uint64_t fileid, uint32_t lock_type,
    uint64_t offset, uint64_t length,
    uint64_t clientid, const uint8_t *owner, uint32_t owner_len,
    struct mds_coord_lock_row *conflict);

/** Scan all lock rows for a file. */
enum mds_status mds_coord_lock_scan_file(struct mds_catalogue *cat,
    uint64_t fileid, mds_coord_lock_scan_cb cb, void *ctx);

/** Scan all lock rows for a client+owner (for LOCKU lookup). */
enum mds_status mds_coord_lock_scan_owner(struct mds_catalogue *cat,
    uint64_t clientid, const uint8_t *owner, uint32_t owner_len,
    mds_coord_lock_scan_cb cb, void *ctx);

/** Delete all locks for a client (lease expiry / failover reap). */
enum mds_status mds_coord_lock_reap_client(struct mds_catalogue *cat,
    uint64_t clientid);

/* --- Delegations ------------------------------------------------------ */

enum mds_status mds_coord_deleg_put(struct mds_catalogue *cat,
    const struct mds_coord_deleg_row *row);

enum mds_status mds_coord_deleg_get(struct mds_catalogue *cat,
    const uint8_t stateid_other[12],
    struct mds_coord_deleg_row *row);

enum mds_status mds_coord_deleg_del(struct mds_catalogue *cat,
    const uint8_t stateid_other[12]);

/** Scan all delegation rows for a file (conflict check). */
enum mds_status mds_coord_deleg_scan_file(struct mds_catalogue *cat,
    uint64_t fileid, mds_coord_deleg_scan_cb cb, void *ctx);

/** Scan all delegation rows for a client (cleanup). */
enum mds_status mds_coord_deleg_scan_client(struct mds_catalogue *cat,
    uint64_t clientid, mds_coord_deleg_scan_cb cb, void *ctx);

/* --- Client identity -------------------------------------------------- */

enum mds_status mds_coord_client_put(struct mds_catalogue *cat,
    const struct mds_coord_client_row *row);

enum mds_status mds_coord_client_get(struct mds_catalogue *cat,
    uint64_t clientid, struct mds_coord_client_row *row);

enum mds_status mds_coord_client_del(struct mds_catalogue *cat,
    uint64_t clientid);

/* --- Sessions --------------------------------------------------------- */

enum mds_status mds_coord_session_put(struct mds_catalogue *cat,
    const struct mds_coord_session_row *row);

enum mds_status mds_coord_session_get(struct mds_catalogue *cat,
    const uint8_t session_id[16],
    struct mds_coord_session_row *row);

enum mds_status mds_coord_session_del(struct mds_catalogue *cat,
    const uint8_t session_id[16]);

/** Scan all sessions for a client. */
enum mds_status mds_coord_session_scan_client(struct mds_catalogue *cat,
    uint64_t clientid, mds_coord_session_scan_cb cb, void *ctx);

/* --- DRC slots -------------------------------------------------------- */

enum mds_status mds_coord_slot_put(struct mds_catalogue *cat,
    const uint8_t session_id[16], uint32_t slot_id,
    uint32_t seq_id, const void *cached_reply, uint32_t reply_len);

enum mds_status mds_coord_slot_get(struct mds_catalogue *cat,
    const uint8_t session_id[16], uint32_t slot_id,
    struct mds_coord_drc_slot_row *row);

#endif /* MDS_COORDINATION_H */

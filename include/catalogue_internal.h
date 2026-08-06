/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_internal.h -- Internal catalogue handle layout.
 *
 * This header is intentionally private to the catalogue subsystem.
 * Callers must continue to use the opaque struct mds_catalogue from
 * mds_catalogue.h.
 */

#ifndef CATALOGUE_INTERNAL_H
#define CATALOGUE_INTERNAL_H

#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "catalog_stats.h"

/* Forward declarations for vtable parameter types. */
struct ds_prealloc_ctx;
struct mds_ds_info;
struct mds_quota_rule;
struct mds_quota_usage;
struct mds_gc_entry;
struct nfs4_stateid;
struct catalog_delta_sink;
struct catalog_image;

/* -----------------------------------------------------------------------
 * Transaction handle (opaque to callers via mds_catalogue.h)
 * ----------------------------------------------------------------------- */

struct mds_cat_txn {
    struct mds_catalogue       *cat;
    enum mds_catalogue_backend  txn_backend;
    enum mds_cat_txn_flags      flags;
    void                       *txn_private;
};

/* -----------------------------------------------------------------------
 * Lifecycle ops (close + probe)
 * ----------------------------------------------------------------------- */

struct mds_catalogue_ops {
    void (*close)(struct mds_catalogue *cat);
    enum mds_status (*probe)(struct mds_catalogue *cat);
};

/* -----------------------------------------------------------------------
 * Authority ops vtable -- catalogue data plane
 *
 * One function pointer per mds_cat_* operation declared in
 * mds_catalogue.h.  Each backend (RonDB) populates its own
 * static instance.  Dispatch is through cat->auth_ops.
 * ----------------------------------------------------------------------- */

struct mds_authority_ops {
    /* Namespace */
    enum mds_status (*ns_create)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t parent,
        const char *name, enum mds_file_type type,
        uint32_t mode, uint64_t uid, uint64_t gid,
        struct ds_prealloc_ctx *prealloc, struct mds_inode *out);
    enum mds_status (*ns_create_wide)(struct mds_catalogue *cat,
        uint64_t parent, const char *name,
        const struct mds_inode *child,
        uint32_t stripe_count, uint32_t stripe_unit,
        uint32_t mirror_count,
        const struct mds_ds_map_entry *entries);
    enum mds_status (*ns_remove)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t parent,
        const char *name);
    /** Optional: remove when child inode was already resolved. */
    enum mds_status (*ns_remove_known)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t parent,
        const char *name, const struct mds_inode *child,
        uint32_t stripe_count);
    /* parent_touch flush target: one interpreted update on the parent
     * inode row (change += delta, mtime/ctime = stamp).  Missing parent
     * row is MDS_OK.  Optional slot: NULL => NOSUPPORT and the feature
     * auto-disables at startup. */
    enum mds_status (*ns_parent_touch)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid,
        uint64_t change_delta, struct timespec stamp);
    /* Async-REMOVE delete manifest (schema v18, mds.conf
     * `remove_async`).  Optional slots: a backend without the
     * mds_remove_pending table leaves them NULL and the dispatcher
     * returns MDS_ERR_NOSUPPORT, which keeps op_remove on the legacy
     * synchronous path. */
    enum mds_status (*remove_pending_enqueue)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t dir_fileid, const char *name,
        uint64_t child_fileid, uint64_t child_generation,
        uint64_t *seq_out);
    /* PHASE-R TEST: unlink-at-ack variant — manifest insert plus a guarded
     * dirent delete and inode DELETE_PENDING flag in ONE committed txn.
     * MDS_ERR_STALE on guard mismatch (caller falls back to sync).  Optional. */
    enum mds_status (*remove_pending_enqueue_unlink)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t dir_fileid, const char *name,
        uint64_t child_fileid, uint64_t child_generation,
        uint64_t *seq_out);
    enum mds_status (*remove_pending_peek_batch)(struct mds_catalogue *cat,
        uint64_t now_ns,
        struct mds_remove_pending_entry *entries,
        uint32_t cap, uint32_t *n_out);
    enum mds_status (*remove_pending_claim)(struct mds_catalogue *cat,
        uint64_t remove_seq, uint32_t mds_id, uint64_t boot_epoch,
        uint64_t now_ns, uint64_t claim_ttl_ns);
    enum mds_status (*remove_pending_complete)(struct mds_catalogue *cat,
        uint64_t remove_seq);
    enum mds_status (*remove_pending_bump_retry)(struct mds_catalogue *cat,
        uint64_t remove_seq);
    enum mds_status (*remove_pending_count)(struct mds_catalogue *cat,
        uint32_t *count);
    enum mds_status (*remove_pending_scan_all)(struct mds_catalogue *cat,
        mds_cat_remove_pending_scan_cb cb, void *ctx);


    enum mds_status (*ns_rename)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t src_parent,
        const char *src_name, uint64_t dst_parent,
        const char *dst_name);
    enum mds_status (*ns_link)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t parent,
        const char *name, uint64_t target);
    enum mds_status (*ns_lookup)(struct mds_catalogue *cat,
        uint64_t parent, const char *name,
        struct mds_inode *child);
    enum mds_status (*ns_getattr)(struct mds_catalogue *cat,
        uint64_t fileid, struct mds_inode *inode);
    enum mds_status (*ns_setattr)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid,
        const struct mds_inode *attrs, uint32_t mask);
    enum mds_status (*ns_readdir)(struct mds_catalogue *cat,
        uint64_t parent, const char *start_after,
        uint32_t max_entries,
        struct mds_cat_txn *txn, mds_readdir_cb cb, void *ctx);

    /** Optional: resolve child fileid to dirent name within parent. */
    enum mds_status (*dirent_name_for_child)(struct mds_catalogue *cat,
        uint64_t parent, uint64_t child_fileid,
        char *name_out, size_t name_out_len);

    /**
     * Optional fused readdir + per-entry attr read.
     *
     * Backends that can satisfy the dirent scan and every child
     * inode read in a single backend transaction SHOULD populate
     * this slot; backends that cannot leave it NULL and the
     * dispatch wrapper (mds_cat_ns_readdir_plus) falls back to
     * ns_readdir + ns_getattr per entry.  Either way the caller
     * sees the same callback signature.
     */
    enum mds_status (*ns_readdir_plus)(struct mds_catalogue *cat,
        uint64_t parent, const char *start_after,
        uint32_t max_entries,
        struct mds_cat_txn *txn, mds_readdir_plus_cb cb, void *ctx);

    /**
     * Optional fused readdir_plus resumed by a stable child-fileid
     * cursor instead of a name prefix.  Returns entries whose
     * child_fileid is strictly greater than @start_after_fileid, in
     * ascending child_fileid order, up to @max_entries.  Backends that
     * implement this over an ordered (parent, child_fileid) index make
     * cookie resume O(log N + page) instead of O(N) per page.
     *
     * Leave NULL when the backend cannot resume by fileid; the dispatch
     * wrapper (mds_cat_ns_readdir_plus_from_cookie) then falls back to
     * the name-order ns_readdir_plus resume via a cookie->name lookup.
     */
    enum mds_status (*ns_readdir_plus_from)(struct mds_catalogue *cat,
        uint64_t parent, uint64_t start_after_fileid,
        uint32_t max_entries,
        struct mds_cat_txn *txn, mds_readdir_plus_cb cb, void *ctx);

    enum mds_status (*ns_nlink_adjust)(struct mds_catalogue *cat,
        uint64_t fileid, int32_t delta);
    enum mds_status (*alloc_fileid)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t *fileid);

    /** Full inode write (create or overwrite).
     *  Unlike ns_setattr, this is NOT a masked read-modify-write --
     *  the entire inode record is replaced atomically. */
    enum mds_status (*inode_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, const struct mds_inode *inode);

    /** Standalone inode delete (by fileid, no dirent removal). */
    enum mds_status (*inode_del)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid);

    /** Low-level dirent write (insert or overwrite). */
    enum mds_status (*dirent_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t parent,
        const char *name, uint64_t child_fileid, uint8_t child_type);

    /* Insert-only dirent write: MDS_ERR_EXISTS on name collision
     * instead of dirent_put's silent overwrite. */
    enum mds_status (*dirent_insert)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t parent,
        const char *name, uint64_t child_fileid, uint8_t child_type);

    /** Low-level dirent delete. */
    enum mds_status (*dirent_del)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t parent, const char *name);

    /* Inline data */
    enum mds_status (*inline_get)(struct mds_catalogue *cat,
        uint64_t fileid, void *buf, uint32_t buflen,
        uint32_t *outlen);
    enum mds_status (*inline_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid,
        const void *buf, uint32_t len);
    enum mds_status (*inline_del)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid);

    /* Xattrs */
    enum mds_status (*xattr_get)(struct mds_catalogue *cat,
        uint64_t fileid, const char *name,
        void **val, uint32_t *vallen);
    enum mds_status (*xattr_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid,
        const char *name, const void *val, uint32_t vallen);
    enum mds_status (*xattr_del)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid,
        const char *name);
    enum mds_status (*xattr_list)(struct mds_catalogue *cat,
        uint64_t fileid, mds_xattr_list_cb cb, void *ctx);
    enum mds_status (*xattr_exists)(struct mds_catalogue *cat,
        uint64_t fileid, const char *name);

    /* Stripe maps */
    enum mds_status (*stripe_map_get)(struct mds_catalogue *cat,
        uint64_t fileid, uint32_t *sc, uint32_t *su,
        uint32_t *mc, struct mds_ds_map_entry **entries);
    enum mds_status (*stripe_map_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid,
        uint32_t sc, uint32_t su, uint32_t mc,
        const struct mds_ds_map_entry *entries);
    enum mds_status (*stripe_map_del)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid);
    enum mds_status (*stripe_map_scan)(struct mds_catalogue *cat,
        mds_cat_stripe_map_scan_cb cb, void *ctx);

    /* DS registry */
    enum mds_status (*ds_get)(struct mds_catalogue *cat,
        uint32_t ds_id, struct mds_ds_info *info);
    enum mds_status (*ds_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn,
        const struct mds_ds_info *info);
    enum mds_status (*ds_del)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint32_t ds_id);
    enum mds_status (*ds_list)(struct mds_catalogue *cat,
        struct mds_ds_info **list, uint32_t *count);

    /* DS provisioning */
    enum mds_status (*ds_provision_get)(struct mds_catalogue *cat,
        uint32_t ds_id, uint8_t *secret, uint32_t secret_len,
        uint64_t *epoch);
    enum mds_status (*ds_provision_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint32_t ds_id,
        const uint8_t *secret, uint32_t secret_len,
        uint64_t epoch);
    enum mds_status (*ds_provision_del)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint32_t ds_id);

    /* Quota */
    enum mds_status (*quota_rule_get)(struct mds_catalogue *cat,
        uint8_t scope_type, uint64_t scope_id,
        struct mds_quota_rule *rule);
    enum mds_status (*quota_rule_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint8_t scope_type,
        uint64_t scope_id, const struct mds_quota_rule *rule);
    enum mds_status (*quota_usage_get)(struct mds_catalogue *cat,
        uint8_t usage_type, uint64_t scope_id,
        struct mds_quota_usage *usage);
    enum mds_status (*quota_usage_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint8_t usage_type,
        uint64_t scope_id, const struct mds_quota_usage *usage);

    /* GC queue */
    enum mds_status (*gc_enqueue)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid,
        uint32_t ds_id, const uint8_t *nfs_fh, uint32_t fh_len);
    enum mds_status (*gc_peek)(struct mds_catalogue *cat,
        struct mds_gc_entry *entry);
    enum mds_status (*gc_dequeue)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t gc_seq);
    enum mds_status (*gc_count)(struct mds_catalogue *cat,
        uint32_t *count);

    /**
     * Optional batched peek.
     *
     * Backends that can return the lowest-cap entries in a single
     * backend round-trip SHOULD populate this slot.  Backends that
     * leave it NULL fall back to a single gc_peek call inside the
     * dispatch wrapper (mds_cat_gc_peek_batch), which preserves
     * correctness at the cost of throughput.
     *
     * On success, *n_out receives the number of entries written
     * (0..cap).  *n_out == 0 with MDS_OK means the queue was
     * empty; the caller is expected to back off.
     */
    enum mds_status (*gc_peek_batch)(struct mds_catalogue *cat,
        struct mds_gc_entry *entries, uint32_t cap,
        uint32_t *n_out);

    /* DS prealloc pool (optional; NULL -> engine runs in-memory only). */
    enum mds_status (*prealloc_pool_insert)(struct mds_catalogue *cat,
        uint64_t fileid, uint32_t ds_id, const uint8_t *nfs_fh,
        uint32_t fh_len, uint32_t owner_mds_id, uint32_t stripe_unit);
    enum mds_status (*prealloc_pool_delete)(struct mds_catalogue *cat,
        uint64_t fileid);
    enum mds_status (*prealloc_pool_scan)(struct mds_catalogue *cat,
        uint32_t owner_mds_id, struct mds_prealloc_pool_row **rows_out,
        uint32_t *n_out);

    /* Shard routing */
    enum mds_status (*shard_fileid_get)(struct mds_catalogue *cat,
        uint64_t fileid, uint32_t *shard_id);
    enum mds_status (*shard_fileid_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid, uint32_t shard_id);
    enum mds_status (*shard_fileid_del)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid);

    /* Cross-shard ext_dirents */
    enum mds_status (*ext_dirent_get)(struct mds_catalogue *cat,
        uint64_t parent, const char *name,
        uint32_t *owner_mds_id, uint64_t *target_fileid,
        uint8_t *target_type, uint64_t *anchor_id);
    enum mds_status (*ext_dirent_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t parent, const char *name,
        uint32_t owner_mds_id, uint64_t target_fileid,
        uint8_t target_type, uint64_t anchor_id);
    enum mds_status (*ext_dirent_del)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t parent, const char *name);

    /* Cross-shard link anchors */
    enum mds_status (*link_anchor_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t anchor_id,
        uint32_t remote_mds_id, uint64_t parent_fileid,
        const char *name);
    enum mds_status (*link_anchor_del)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t anchor_id);
};

/* -----------------------------------------------------------------------
 * Coordination ops vtable -- recovery-critical state
 *
 * One function pointer per mds_coord_* operation declared in
 * mds_coordination.h.
 * ----------------------------------------------------------------------- */

struct mds_coordination_ops {
    /* Shared 2PC journal */
    enum mds_status (*journal_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn,
        const struct mds_coord_journal_record *record);
    enum mds_status (*journal_get)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t txn_id, uint8_t role,
        struct mds_coord_journal_record *record);
    enum mds_status (*journal_del)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t txn_id, uint8_t role);
    enum mds_status (*journal_scan)(struct mds_catalogue *cat,
        mds_coord_journal_scan_cb cb, void *ctx);
    /* Layout state */
    enum mds_status (*layout_grant)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t clientid,
        uint64_t fileid, uint32_t iomode,
        uint64_t offset, uint64_t length,
        const struct nfs4_stateid *stateid,
        const uint32_t *ds_ids, uint32_t ds_count);
    enum mds_status (*layout_return)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn,
        const uint8_t stateid_other[12],
        uint64_t clientid, uint64_t fileid,
        const uint32_t *ds_ids, uint32_t ds_count);
    enum mds_status (*layout_get_by_stateid)(
        struct mds_catalogue *cat,
        const uint8_t stateid_other[12],
        uint64_t *clientid, uint64_t *fileid,
        uint32_t *iomode, uint64_t *offset,
        uint64_t *length, uint32_t *seqid);
    enum mds_status (*layout_scan_for_file)(
        struct mds_catalogue *cat,
        uint64_t fileid, bool *has_layout);
    enum mds_status (*layout_del_all_for_client)(
        struct mds_catalogue *cat, uint64_t clientid);
    enum mds_status (*ds_layout_idx_scan)(
        struct mds_catalogue *cat, uint32_t ds_id,
        mds_coord_ds_layout_cb cb, void *ctx);
    enum mds_status (*layout_iter_file)(
        struct mds_catalogue *cat, uint64_t fileid,
        mds_coord_layout_file_iter_cb cb, void *ctx);

    /* Client recovery */
    enum mds_status (*recovery_put)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t clientid,
        const uint8_t *co_ownerid, uint32_t co_ownerid_len,
        const uint8_t verifier[8]);
    enum mds_status (*recovery_del)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t clientid);
    enum mds_status (*recovery_get)(struct mds_catalogue *cat,
        uint64_t clientid, uint8_t *co_ownerid,
        uint32_t *co_ownerid_len, uint8_t verifier[8]);
    enum mds_status (*recovery_list)(struct mds_catalogue *cat,
        uint32_t owner_mds_id,
        mds_recovery_list_cb cb, void *ctx);

    /* ---- Shared protocol state (shared-attr) ---- */

    /* Open/share state */
    enum mds_status (*open_put)(struct mds_catalogue *cat,
        const struct mds_coord_open_row *row);
    enum mds_status (*open_get)(struct mds_catalogue *cat,
        const uint8_t stateid_other[12],
        struct mds_coord_open_row *row);
    enum mds_status (*open_del)(struct mds_catalogue *cat,
        const uint8_t stateid_other[12]);
    enum mds_status (*open_scan_file)(struct mds_catalogue *cat,
        uint64_t fileid, mds_coord_open_scan_cb cb, void *ctx);
    enum mds_status (*open_scan_client)(struct mds_catalogue *cat,
        uint64_t clientid, mds_coord_open_scan_cb cb, void *ctx);

    /* Byte-range locks */
    enum mds_status (*lock_put)(struct mds_catalogue *cat,
        const struct mds_coord_lock_row *row);
    enum mds_status (*lock_del)(struct mds_catalogue *cat,
        uint64_t fileid, uint64_t lock_id);
    enum mds_status (*lock_test)(struct mds_catalogue *cat,
        uint64_t fileid, uint32_t lock_type,
        uint64_t offset, uint64_t length,
        uint64_t clientid, const uint8_t *owner, uint32_t owner_len,
        struct mds_coord_lock_row *conflict);
    enum mds_status (*lock_scan_file)(struct mds_catalogue *cat,
        uint64_t fileid, mds_coord_lock_scan_cb cb, void *ctx);
    enum mds_status (*lock_scan_owner)(struct mds_catalogue *cat,
        uint64_t clientid, const uint8_t *owner, uint32_t owner_len,
        mds_coord_lock_scan_cb cb, void *ctx);
    enum mds_status (*lock_reap_client)(struct mds_catalogue *cat,
        uint64_t clientid);

    /* Delegations */
    enum mds_status (*deleg_put)(struct mds_catalogue *cat,
        const struct mds_coord_deleg_row *row);
    enum mds_status (*deleg_get)(struct mds_catalogue *cat,
        const uint8_t stateid_other[12],
        struct mds_coord_deleg_row *row);
    enum mds_status (*deleg_del)(struct mds_catalogue *cat,
        const uint8_t stateid_other[12]);
    enum mds_status (*deleg_scan_file)(struct mds_catalogue *cat,
        uint64_t fileid, mds_coord_deleg_scan_cb cb, void *ctx);
    enum mds_status (*deleg_scan_client)(struct mds_catalogue *cat,
        uint64_t clientid, mds_coord_deleg_scan_cb cb, void *ctx);

    /* Client identity */
    enum mds_status (*client_put)(struct mds_catalogue *cat,
        const struct mds_coord_client_row *row);
    enum mds_status (*client_get)(struct mds_catalogue *cat,
        uint64_t clientid, struct mds_coord_client_row *row);
    enum mds_status (*client_del)(struct mds_catalogue *cat,
        uint64_t clientid);

    /* Sessions */
    enum mds_status (*session_put)(struct mds_catalogue *cat,
        const struct mds_coord_session_row *row);
    enum mds_status (*session_get)(struct mds_catalogue *cat,
        const uint8_t session_id[16],
        struct mds_coord_session_row *row);
    enum mds_status (*session_del)(struct mds_catalogue *cat,
        const uint8_t session_id[16]);
    enum mds_status (*session_scan_client)(struct mds_catalogue *cat,
        uint64_t clientid, mds_coord_session_scan_cb cb, void *ctx);

    /* DRC slots */
    enum mds_status (*slot_put)(struct mds_catalogue *cat,
        const uint8_t session_id[16], uint32_t slot_id,
        uint32_t seq_id, const void *cached_reply, uint32_t reply_len);
    enum mds_status (*slot_get)(struct mds_catalogue *cat,
        const uint8_t session_id[16], uint32_t slot_id,
        struct mds_coord_drc_slot_row *row);
};

/* -----------------------------------------------------------------------
 * Catalogue handle
 * ----------------------------------------------------------------------- */

struct mds_catalogue {
    enum mds_catalogue_backend        backend;
    const struct mds_authority_ops    *auth_ops;
    const struct mds_coordination_ops *coord_ops;
    const struct mds_catalogue_ops    *ops;       /**< Lifecycle. */
    void                              *backend_private;
    struct catalog_stats               stats;
    struct catalog_delta_sink         *delta_sink;
    struct catalog_image              *image;
    struct commit_queue               *cq;
};

#endif /* CATALOGUE_INTERNAL_H */

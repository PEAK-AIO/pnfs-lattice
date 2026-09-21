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
#include "mds_cluster.h"
#include "catalog_stats.h"

/* Forward declarations for vtable parameter types. */
struct ds_prealloc_ctx;
struct mds_ds_info;
struct mds_ds_map_entry;
struct mds_quota_rule;
struct mds_quota_usage;
struct mds_gc_entry;
struct nfs4_stateid;
struct catalog_delta_sink;
struct catalog_image;

/* -----------------------------------------------------------------------
 * Slot contract -- binding on every backend that populates a vtable
 * below (authority, coordination, lifecycle, cluster).  The dispatcher
 * and every caller are written against these seven rules; a backend
 * that breaks one is wrong even if the tests happen to pass.
 *
 * C1  Re-entrancy.  A caller's callback may call back into the SAME
 *     catalogue handle from the same thread (layout_recall's byte-range
 *     collector does, and so does the dispatcher's readdir_plus
 *     fallback).  A backend therefore never holds a non-reentrant lock
 *     across a callback and must tolerate a nested, independent
 *     transaction on the same handle.  The required shape for a backend
 *     with a mutex or a bounded transaction window is
 *     materialise-then-deliver: fill a bounded page under the lock or
 *     transaction, release it, then invoke the callbacks.
 *
 * C2  Callbacks never run inside a retry body.  A backend that retries
 *     transactions delivers entries only after the attempt has
 *     completed, so a retried attempt never redelivers entries or
 *     delivers after partial progress.  Enumeration may page with one
 *     transaction per page and an explicit cursor; the caller gets
 *     page-consistent results.
 *
 * C3  Coherent-object predicates keep ONE consistency boundary.  The
 *     stripe header plus its entries (stripe_map_get, layoutget_fused),
 *     layout-grant validation (layout_grant_union read-modify-write),
 *     directory emptiness for RMDIR and rename-over-directory, and the
 *     ns_remove_known_gc re-validation are read and decided inside the
 *     single transaction that mutates -- never across pages or across
 *     two calls.
 *
 * C4  Status pass-through.  A dispatcher never flattens a slot's
 *     status: what the slot returns is what the caller sees (e.g. a
 *     node_heartbeat MDS_ERR_NOTFOUND reaches the heartbeat thread
 *     unchanged).  Dispatchers add only MDS_ERR_INVAL for invalid
 *     arguments and MDS_ERR_NOSUPPORT for an absent optional slot.
 *
 * C5  Capability truthfulness.  A populated slot implements the
 *     operation; "present but returns NOSUPPORT" is forbidden, and a
 *     dispatcher fallback for an absent slot must never silently weaken
 *     a correctness property (a fallback may cost more round trips,
 *     never less safety).  cat->caps states only properties the store
 *     actually has.
 *
 * C6  Transaction-token semantics.  struct mds_cat_txn is a grouping
 *     context: mds_cat_txn_begin allocates it, mds_cat_txn_commit and
 *     mds_cat_txn_abort only free it, and a non-NULL txn never joins
 *     operations on any backend.  Multi-record atomicity exists ONLY
 *     inside the fused slots (ns_create_wide, ns_create_with_layout,
 *     ns_remove_known_gc, ns_rename / ns_rename_flags, layoutget_fused);
 *     a backend must not implement anything stronger for the token.
 *
 * C7  Ownership at close.  ops->close(cat) releases backend-owned
 *     resources only (backend_private, connections, threads); the
 *     dispatcher owns struct mds_catalogue and frees it after close
 *     returns (mds_catalogue_close).  A backend never frees cat.
 * ----------------------------------------------------------------------- */

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
 * Lifecycle ops (close + probe + optional bootstrap / native handle)
 * ----------------------------------------------------------------------- */

struct mds_catalogue_ops {
    void (*close)(struct mds_catalogue *cat);
    enum mds_status (*probe)(struct mds_catalogue *cat);
    /** Optional: idempotent schema bootstrap (create missing tables,
     *  seed schema version / fileid counter / root inode).  Leave NULL
     *  when the backend has nothing to bootstrap; the dispatcher then
     *  returns MDS_ERR_NOSUPPORT and the daemon skips the
     *  bootstrap-and-probe retry loop at startup. */
    enum mds_status (*bootstrap)(struct mds_catalogue *cat);
    /** Optional: the backend's native client handle (e.g. the RonDB
     *  shim handle) for backend-specific tools.  Leave NULL when there
     *  is nothing meaningful to expose; the dispatcher returns NULL. */
    void *(*backend_handle)(const struct mds_catalogue *cat);
    /**
     * Optional changefeed / catalog-image feed lifecycle.  A backend
     * that can replay other MDS nodes' mutations into a local
     * catalog_image (RonDB: the mds_delta_broadcast poller) starts a
     * background feed here and stops (joins) it in image_feed_stop.
     * @image is caller-owned and must outlive the feed; @self_mds_id
     * lets the feed skip this node's own stream; @poll_interval_ms is
     * the polling cadence (0 = backend default).  Populate BOTH slots
     * or neither: mds_catalogue_image_feed_supported() requires both,
     * and the dispatchers return MDS_ERR_NOSUPPORT for a NULL slot.
     * image_feed_stop must be idempotent and safe when no feed runs;
     * the backend's close must also stop a feed still running.
     */
    enum mds_status (*image_feed_start)(struct mds_catalogue *cat,
        struct catalog_image *image, uint32_t self_mds_id,
        uint32_t poll_interval_ms);
    void (*image_feed_stop)(struct mds_catalogue *cat);
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
        const struct mds_ds_map_entry *entries,
        bool *safe_to_discard);
    /**
     * Optional fused CREATE + layout pre-grant: ns_create semantics
     * (inode + dirent + parent touch + 1x1 stripe map from the
     * prealloc pop) PLUS, when layout_clientid != 0, the layout_state
     * row and its indexes, all committed in ONE backend transaction so
     * a following LAYOUTGET in the same compound needs no backend
     * round trip.  *layout_ok reports whether the grant was persisted.
     * layout_entry_out / layout_pop_stripe_unit_out (both optional)
     * receive the DS entry and stripe unit of the single prealloc pop
     * the transaction used, so the caller's per-compound stripe cache
     * reflects exactly the persisted placement (0 unit = no pop).
     * Leave NULL when the backend cannot fuse; the dispatch wrapper
     * returns MDS_ERR_NOSUPPORT and callers fall back to ns_create.
     */
    enum mds_status (*ns_create_with_layout)(struct mds_catalogue *cat,
        uint64_t parent, const char *name,
        enum mds_file_type type,
        uint32_t mode, uint64_t uid, uint64_t gid,
        struct ds_prealloc_ctx *prealloc, struct mds_inode *out,
        uint64_t layout_clientid, uint32_t layout_iomode,
        uint64_t layout_offset, uint64_t layout_length,
        const struct nfs4_stateid *layout_stateid,
        uint32_t layout_mds_id,
        bool *layout_ok,
        struct mds_ds_map_entry *layout_entry_out,
        uint32_t *layout_pop_stripe_unit_out);
    enum mds_status (*ns_remove)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t parent,
        const char *name);
    /** Optional: remove when child inode was already resolved. */
    enum mds_status (*ns_remove_known)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t parent,
        const char *name, const struct mds_inode *child,
        uint32_t stripe_count);
    /** Optional fused final-unlink: ns_remove_known semantics PLUS the
     * caller's unique-DS GC-queue rows committed in the SAME backend
     * transaction, so a committed final unlink can never leave its DS
     * objects unqueued and the per-remove gc_enqueue round-trip
     * disappears.  *gc_folded reports whether the rows were actually
     * folded (false when the remove degenerated to a non-final
     * unlink).  MDS_ERR_STALE means the dirent no longer resolves to
     * @child (concurrent replace) and the caller must fall back to
     * the legacy split path.  Leave NULL when the backend cannot fuse;
     * the dispatch wrapper then returns MDS_ERR_NOSUPPORT. */
    enum mds_status (*ns_remove_known_gc)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t parent,
        const char *name, const struct mds_inode *child,
        uint32_t stripe_count,
        const struct mds_ds_map_entry *gc_entries,
        uint32_t gc_entry_count,
        uint32_t gc_sweep_hint,
        bool *gc_folded);
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
    /** Optional flags-aware rename (MDS_CAT_RNF_*).  Leave NULL when
     * the backend has no flags support: the dispatch wrapper then
     * falls back to ns_rename for ns_flags==0 and returns
     * MDS_ERR_NOSUPPORT otherwise. */
    enum mds_status (*ns_rename_flags)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t src_parent,
        const char *src_name, uint64_t dst_parent,
        const char *dst_name, uint32_t ns_flags);
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
     * Optional fused readdir_plus resumed by a READDIR cookie.
     *
     * Every delivered dirent carries a backend-assigned cookie (struct
     * mds_cat_dirent.cookie): unique per entry within the directory,
     * stable for the life of the dirent, never 0, 1 or 2.  This slot
     * returns the entries whose cookie is strictly greater than
     * @start_after_cookie, in the backend's own iteration order (the
     * order in which cookies increase), up to @max_entries; 0 means
     * the first page.  A cookie whose entry was removed is safe by
     * construction because the resume is a strict range.  Backends
     * that implement this over an ordered (parent, cookie) index make
     * cookie resume O(log N + page) instead of O(N) per page.
     *
     * RonDB currently assigns cookie = child fileid and resumes over
     * ix_dirents_parent_child in ascending child_fileid order.  This
     * is a documented deviation: two hard links to one inode in one
     * directory share a cookie, so a page boundary between them drops
     * one name.  It stands until RonDB gains a per-dirent sequence
     * column and ordered index (a separately approved schema change).
     *
     * Leave NULL when the backend cannot resume by cookie; the dispatch
     * wrapper (mds_cat_ns_readdir_plus_from_cookie) then falls back to
     * the name-order ns_readdir_plus resume via a cookie->name lookup
     * (dirent_name_for_child), which only works while cookies are
     * child fileids.
     */
    enum mds_status (*ns_readdir_plus_from)(struct mds_catalogue *cat,
        uint64_t parent, uint64_t start_after_cookie,
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

    /* GC queue.  sweep_hint carries the MDS_GC_SWEEP_* stripe/mirror
     * coverage for the ds_gc drainer (0 = legacy dense sweep). */
    enum mds_status (*gc_enqueue)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t fileid,
        uint32_t ds_id, const uint8_t *nfs_fh, uint32_t fh_len,
        uint32_t sweep_hint);
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

    /** Optional: backend client-side behaviour counters (cumulative
     * round trips, transactions, bytes -- see
     * struct mds_cat_backend_client_stats).  Leave NULL when the
     * backend has no client instrumentation; the dispatch wrapper
     * then returns MDS_ERR_NOSUPPORT. */
    enum mds_status (*backend_client_stats)(struct mds_catalogue *cat,
        struct mds_cat_backend_client_stats *out);
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
    /** Optional renewal variant: persist the saturating UNION of the
     * existing row's byte range and the new window (monotonic seqid)
     * so the row stays a superset of every range granted under the
     * stateid (recall-coverage invariant).  Every in-tree backend
     * populates it; a NULL slot makes the dispatch wrapper return
     * MDS_ERR_NOSUPPORT (C5).  There is no overwrite fallback: a plain
     * layout_grant on renewal would narrow the persisted range and
     * silently lose recall coverage. */
    enum mds_status (*layout_grant_union)(struct mds_catalogue *cat,
        struct mds_cat_txn *txn, uint64_t clientid,
        uint64_t fileid, uint32_t iomode,
        uint64_t offset, uint64_t length,
        const struct nfs4_stateid *stateid,
        const uint32_t *ds_ids, uint32_t ds_count);
    /**
     * Optional fused LAYOUTGET: read the file's stripe map header +
     * entries and persist the layout_state row (+ indexes) for the
     * grant in ONE backend transaction.  Same output contract as
     * stripe_map_get for (*stripe_count, *stripe_unit, *mirror_count,
     * *entries -- caller frees) and the same MDS_ERR_NOTFOUND when the
     * file has no stripe map (nothing is granted in that case).
     * MDS_ERR_DELAY reports a transient failure after the backend's
     * own bounded retry.  Leave NULL when the backend cannot fuse; the
     * dispatch wrapper returns MDS_ERR_NOSUPPORT and callers fall back
     * to stripe_map_get + layout_grant.
     */
    enum mds_status (*layoutget_fused)(struct mds_catalogue *cat,
        uint64_t fileid,
        uint32_t *stripe_count, uint32_t *stripe_unit,
        uint32_t *mirror_count, struct mds_ds_map_entry **entries,
        const struct nfs4_stateid *stateid,
        uint64_t clientid, uint32_t iomode,
        uint64_t offset, uint64_t length,
        uint32_t mds_id);
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
 * Cluster ops vtable -- multi-MDS services (node registry, heartbeat,
 * stale-peer scan, partition map)
 *
 * One function pointer per mds_cluster_* operation declared in
 * mds_cluster.h; the semantics, target contract and current RonDB
 * deviations are documented there.  Every slot is optional: NULL makes
 * the dispatcher return MDS_ERR_NOSUPPORT.  A backend with no cluster
 * services leaves cat->cluster_ops NULL altogether.
 * ----------------------------------------------------------------------- */

struct mds_cluster_ops {
    /* Node registry */
    enum mds_status (*node_register)(struct mds_catalogue *cat,
        uint32_t mds_id, uint64_t boot_epoch, const char *hostname,
        uint16_t nfs_port, uint16_t grpc_port);
    enum mds_status (*node_heartbeat)(struct mds_catalogue *cat,
        uint32_t mds_id, uint64_t boot_epoch);
    /** boot_epoch is part of the signature from the start so the
     *  conditional (epoch-matching) delete needs no signature change;
     *  a slot that does not yet honour it must say so where it is
     *  registered. */
    enum mds_status (*node_deregister)(struct mds_catalogue *cat,
        uint32_t mds_id, uint64_t boot_epoch);
    enum mds_status (*node_list)(struct mds_catalogue *cat,
        mds_cluster_node_cb cb, void *ctx);
    enum mds_status (*node_scan_stale)(struct mds_catalogue *cat,
        uint64_t threshold_ns, mds_cluster_stale_cb cb, void *ctx);

    /* Partition map */
    enum mds_status (*partition_list)(struct mds_catalogue *cat,
        mds_cluster_partition_cb cb, void *ctx);
    /** insert_only: true = MDS_ERR_EXISTS when the row exists (the
     *  root claim), false = upsert.  Same signature-stability note as
     *  node_deregister. */
    enum mds_status (*partition_put)(struct mds_catalogue *cat,
        uint32_t partition_id, uint32_t owner_mds_id, uint8_t state,
        const char *subtree_path, bool insert_only);
    /** Compare-and-swap the owner of one row: owner_mds_id becomes
     *  @new_owner and state @new_state only while the row's owner is
     *  @expected_owner.  MDS_ERR_NOTFOUND when the row is absent,
     *  MDS_ERR_STALE when the owner differs; nothing is written in
     *  either case.  The failover takeover's persistence step. */
    enum mds_status (*partition_cas)(struct mds_catalogue *cat,
        uint32_t partition_id, uint32_t expected_owner,
        uint32_t new_owner, uint8_t new_state);
};

/* -----------------------------------------------------------------------
 * Catalogue handle
 * ----------------------------------------------------------------------- */

/**
 * Backend capability bits (struct mds_catalogue.caps), set once by the
 * backend constructor.  Properties of the store that callers need to
 * know but that are not expressed by the presence of a vtable slot.
 * C5 applies: a bit is set only when the store really has the property.
 */

/** The catalogue is ONE authority shared by every MDS node: an inode
 *  is visible from all MDSes, so a cross-subtree rename moves only
 *  the dirent and never copies or deletes the inode. */
#define MDS_CAT_CAP_SHARED_AUTHORITY   (1U << 0)

/** The store is reachable from more than one process (a database
 *  cluster, not a per-process memory image), so registry rows,
 *  heartbeats and the partition map written by one MDS daemon are
 *  observable by another.  Required, together with the cluster slots,
 *  for mds_cluster_supported(); an in-process store never sets it even
 *  when it populates cluster_ops for tests. */
#define MDS_CAT_CAP_MULTI_PROCESS      (1U << 1)

struct mds_catalogue {
    enum mds_catalogue_backend        backend;
    uint32_t                          caps;      /**< MDS_CAT_CAP_*. */
    const struct mds_authority_ops    *auth_ops;
    const struct mds_coordination_ops *coord_ops;
    const struct mds_catalogue_ops    *ops;       /**< Lifecycle. */
    /** Optional cluster services; NULL when the backend has none. */
    const struct mds_cluster_ops      *cluster_ops;
    void                              *backend_private;
    struct catalog_stats               stats;
    struct catalog_delta_sink         *delta_sink;
    struct catalog_image              *image;
    struct commit_queue               *cq;
};

#endif /* CATALOGUE_INTERNAL_H */

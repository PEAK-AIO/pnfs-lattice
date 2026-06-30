/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_catalogue.h -- Backend-neutral metadata catalogue interface.
 *
 * Decouples the NFS protocol layer from the storage engine.
 * The unit of work is a logical metadata transaction.
 *
 * State classes:
 *
 *   Catalogue data -- inode, dirent, stripe-map, xattr, inline,
 *       DS registry, quota, GC queue, fileid allocation.
 *       Hot metadata path.  Authority vtable dispatches to
 *       the RonDB backend.
 *
 *   Recovery-critical coordination -- shared 2PC journal,
 *       layout_state, ds_layout_idx, client_recovery. Declared in
 *       mds_coordination.h so callers opt into the non-catalogue
 *       state class explicitly.
 *
 *   Ephemeral / optional -- open_state, session, DRC.
 *       Not part of the catalogue interface.
 *
 * See docs/architecture.md for full design context.
 */

#ifndef MDS_CATALOGUE_H
#define MDS_CATALOGUE_H

#include <stdbool.h>
#include <stdint.h>

#include "pnfs_mds.h"

/* Forward declarations -- callers see opaque pointers. */
struct mds_catalogue;
struct mds_cat_txn;

struct ds_prealloc_ctx;
struct mds_ds_info;
struct mds_quota_rule;
struct mds_quota_usage;
struct mds_gc_entry;

/* -----------------------------------------------------------------------
 * Backend-neutral types
 * ----------------------------------------------------------------------- */

/** Directory entry. */
struct mds_cat_dirent {
	uint64_t fileid;
	uint8_t  type;
	char     name[MDS_MAX_NAME + 1];
};

/**
 * Readdir callback.  Return 0 to continue, non-zero to stop.
 */
typedef int (*mds_readdir_cb)(const struct mds_cat_dirent *entry,
			      void *arg);

/**
 * READDIR_PLUS callback.  Delivers the dirent alongside the fused
 * child-inode read for that entry.
 *
 * @param dirent       Directory entry (always non-NULL).
 * @param inode        Child inode; valid only when inode_valid is true.
 *                     Contents are owned by the callee and must not be
 *                     stored by reference across the callback.
 * @param inode_valid  True when the inode read succeeded for this entry;
 *                     false when the inode no longer exists (the race
 *                     where a dirent points to a concurrently-removed
 *                     inode).  Callers must handle both paths.
 * @param arg          Caller-provided context pointer.
 * @return 0 to continue, non-zero to stop iteration.
 */
typedef int (*mds_readdir_plus_cb)(const struct mds_cat_dirent *dirent,
				   const struct mds_inode *inode,
				   bool inode_valid,
				   void *arg);

/**
 * Xattr-list callback.  Return 0 to continue, non-zero to stop.
 */
typedef int (*mds_xattr_list_cb)(const char *name, size_t name_len,
				 void *arg);

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */

/**
 * Open a metadata catalogue.
 *
 * The backend is selected by cfg.
 *
 * @param cfg   MDS configuration.
 * @param out   Receives the catalogue handle.
 * @return MDS_OK on success.
 */
enum mds_status mds_catalogue_open(const struct mds_config *cfg,
				   struct mds_catalogue **out);

/**
 * Close the catalogue and free all resources.
 *
 * Safe to call with NULL.
 */
void mds_catalogue_close(struct mds_catalogue *cat);

/** Return the backend-private handle (e.g. RonDB shim handle).
 *  Returns NULL if cat is NULL or no backend is attached. */
void *mds_catalogue_backend_handle(const struct mds_catalogue *cat);

/* -----------------------------------------------------------------------
 * Transaction control
 *
 * Every write operation is self-contained by default -- the backend
 * handles atomicity internally.  Explicit transactions are provided
 * for callers that need multi-operation atomicity (e.g., cross-
 * directory rename that also updates parent change counters).
 *
 * Read operations do not require a transaction handle.  Backends
 * may internally share a read snapshot within a request scope.
 * ----------------------------------------------------------------------- */

enum mds_cat_txn_flags {
	MDS_CAT_TXN_WRITE    = 0,
	MDS_CAT_TXN_RDONLY   = 1,
};

/**
 * Begin a logical transaction.
 *
 * RonDB write calls are self-contained; the transaction handle is a
 * lightweight grouping context that lets callers use the catalogue
 * API uniformly.
 */
enum mds_status mds_cat_txn_begin(struct mds_catalogue *cat,
				  enum mds_cat_txn_flags flags,
				  struct mds_cat_txn **out);

/** Commit a logical transaction. */
enum mds_status mds_cat_txn_commit(struct mds_cat_txn *txn);

/** Abort a logical transaction. */
void mds_cat_txn_abort(struct mds_cat_txn *txn);

/* -----------------------------------------------------------------------
 * Catalogue data -- Namespace
 *
 * Each operation is self-contained (atomic commit) when txn is NULL.
 * When txn is non-NULL, the operation joins the caller's transaction.
 * ----------------------------------------------------------------------- */

/** Create a file/directory: inode + dirent + parent touch. */
enum mds_status mds_cat_ns_create(struct mds_catalogue *cat,
				  struct mds_cat_txn *txn,
				  uint64_t parent_fileid,
				  const char *name,
				  enum mds_file_type type,
				  uint32_t mode,
				  uint64_t uid, uint64_t gid,
				  struct ds_prealloc_ctx *prealloc,
				  struct mds_inode *out);

/** Remove a dirent + inode (if nlink drops to 0) + parent touch. */
enum mds_status mds_cat_ns_remove(struct mds_catalogue *cat,
				  struct mds_cat_txn *txn,
				  uint64_t parent_fileid,
				  const char *name);

/** Remove when the child inode was already looked up (skips re-read). */
enum mds_status mds_cat_ns_remove_known(struct mds_catalogue *cat,
					struct mds_cat_txn *txn,
					uint64_t parent_fileid,
					const char *name,
					const struct mds_inode *child,
					uint32_t stripe_count);

/** Atomic rename: src dirent -> dst dirent + parent touches. */
enum mds_status mds_cat_ns_rename(struct mds_catalogue *cat,
				  struct mds_cat_txn *txn,
				  uint64_t src_parent,
				  const char *src_name,
				  uint64_t dst_parent,
				  const char *dst_name);

/** Hard link: create dirent + bump nlink on target. */
enum mds_status mds_cat_ns_link(struct mds_catalogue *cat,
				struct mds_cat_txn *txn,
				uint64_t parent_fileid,
				const char *name,
				uint64_t target_fileid);

/** Lookup: dirent resolve + child inode read. */
enum mds_status mds_cat_ns_lookup(struct mds_catalogue *cat,
				  uint64_t parent_fileid,
				  const char *name,
				  struct mds_inode *child);

/** Read inode by fileid. */
enum mds_status mds_cat_ns_getattr(struct mds_catalogue *cat,
				   uint64_t fileid,
				   struct mds_inode *inode);

/** Update inode fields selected by mask. */
enum mds_status mds_cat_ns_setattr(struct mds_catalogue *cat,
				   struct mds_cat_txn *txn,
				   uint64_t fileid,
				   const struct mds_inode *attrs,
				   uint32_t mask);

/** Iterate directory entries (start_after = NULL for first page).
 *  When max_entries > 0, return at most that many entries (0 = unlimited). */
enum mds_status mds_cat_ns_readdir(struct mds_catalogue *cat,
				   uint64_t parent_fileid,
				   const char *start_after,
				   uint32_t max_entries,
				   struct mds_cat_txn *txn,
				   mds_readdir_cb cb, void *ctx);

/**
 * Look up a directory entry name by child fileid within a parent.
 * Used to translate READDIR cookies (fileids) into start_after names.
 */
enum mds_status mds_cat_ns_dirent_name_for_child(
	struct mds_catalogue *cat,
	uint64_t parent_fileid,
	uint64_t child_fileid,
	char *name_out,
	size_t name_out_len);

/**
 * Iterate directory entries delivering dirent + child inode together
 * in a single backend call (the RonDB backend fuses the DIRENTS scan
 * and the per-entry INODES reads into one NDB transaction).
 *
 * Backends that do not implement a native fused path fall back to
 * mds_cat_ns_readdir followed by a per-entry mds_cat_ns_getattr; the
 * callback signature is identical regardless of which path was taken.
 *
 * @param cat           Catalogue handle.
 * @param parent_fileid Directory fileid.
 * @param start_after   NULL for first page; otherwise skip entries
 *                      whose names are <= start_after (bytewise).
 * @param max_entries   Maximum entries to return (0 = unlimited).
 * @param txn           Reserved for future use; pass NULL.
 * @param cb            Called once per entry.
 * @param ctx           Passed to cb.
 * @return MDS_OK on success or a backend error code.
 */
enum mds_status mds_cat_ns_readdir_plus(struct mds_catalogue *cat,
					uint64_t parent_fileid,
					const char *start_after,
					uint32_t max_entries,
					struct mds_cat_txn *txn,
					mds_readdir_plus_cb cb,
					void *ctx);

/**
 * Fused readdir_plus resumed by a READDIR cookie that encodes the last
 * child fileid seen (0 for the first page).  Entries are returned with
 * child_fileid strictly greater than @cookie, in ascending fileid
 * order, up to @max_entries.
 *
 * Fast path: backends exposing ns_readdir_plus_from satisfy this with
 * an indexed range scan (O(log N + page)).  Fallback: the cookie is
 * translated back to a name via dirent_name_for_child and the
 * name-order ns_readdir_plus resume is used, preserving behaviour on
 * backends without the fileid cursor.  A stale cookie (its entry was
 * removed) yields an empty, drained page on the fallback path; the
 * fast path is inherently safe because the resume is a strict
 * fileid > cookie range.
 */
enum mds_status mds_cat_ns_readdir_plus_from_cookie(
					struct mds_catalogue *cat,
					uint64_t parent_fileid,
					uint64_t cookie,
					uint32_t max_entries,
					struct mds_cat_txn *txn,
					mds_readdir_plus_cb cb,
					void *ctx);

/**
 * Resolve an absolute namespace path to its fileid.
 *
 * Walks the namespace from root using backend-neutral lookup operations.
 *
 * @param cat         Catalogue handle.
 * @param path        Absolute path beginning with '/'.
 * @param out_fileid  Receives the resolved fileid.
 * @return MDS_OK on success, MDS_ERR_NOTFOUND if a component is missing,
 *         or MDS_ERR_INVAL for invalid input.
 */
enum mds_status mds_cat_resolve_path(struct mds_catalogue *cat,
				     const char *path,
				     uint64_t *out_fileid);

/** Adjust nlink without full setattr. */
enum mds_status mds_cat_ns_nlink_adjust(struct mds_catalogue *cat,
					uint64_t fileid, int32_t delta);

/** Allocate a globally unique fileid. */
enum mds_status mds_cat_alloc_fileid(struct mds_catalogue *cat,
				     struct mds_cat_txn *txn,
				     uint64_t *fileid);

/* -----------------------------------------------------------------------
 * Catalogue data -- Low-level inode/dirent ops
 *
 * These supplement the higher-level ns_* operations for callers
 * that need direct inode or dirent access without the composite
 * create/remove/rename semantics.
 * ----------------------------------------------------------------------- */

/** Read a dirent: resolve (parent, name) -> (fileid, type). */
enum mds_status mds_cat_dirent_get(struct mds_catalogue *cat,
				   uint64_t parent_fileid,
				   const char *name,
				   uint64_t *child_fileid,
				   uint8_t *child_type);

/** Write a dirent directly (used by 2PC rename, migration). */
enum mds_status mds_cat_dirent_put(struct mds_catalogue *cat,
				   struct mds_cat_txn *txn,
				   uint64_t parent_fileid,
				   const char *name,
				   uint64_t child_fileid,
				   uint8_t child_type);

/** Delete a single dirent (used by 2PC rename, migration). */
enum mds_status mds_cat_dirent_del(struct mds_catalogue *cat,
				   struct mds_cat_txn *txn,
				   uint64_t parent_fileid,
				   const char *name);

/** Check if a directory is empty. */
enum mds_status mds_cat_dir_is_empty(struct mds_catalogue *cat,
				     uint64_t parent_fileid,
				     bool *empty);

/** Delete an inode by fileid (used by remove when nlink == 0). */
enum mds_status mds_cat_inode_del(struct mds_catalogue *cat,
				  struct mds_cat_txn *txn,
				  uint64_t fileid);

/** Write full inode (non-masked put). */
enum mds_status mds_cat_inode_put(struct mds_catalogue *cat,
				  struct mds_cat_txn *txn,
				  const struct mds_inode *inode);

/** Flush backend to durable storage (no-op for RonDB). */
enum mds_status mds_cat_sync(struct mds_catalogue *cat);

/* -----------------------------------------------------------------------
 * Catalogue data -- Inline data (small file acceleration)
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_inline_get(struct mds_catalogue *cat,
				   uint64_t fileid,
				   void *buf, uint32_t buflen,
				   uint32_t *outlen);

enum mds_status mds_cat_inline_put(struct mds_catalogue *cat,
				   struct mds_cat_txn *txn,
				   uint64_t fileid,
				   const void *buf, uint32_t len);

enum mds_status mds_cat_inline_del(struct mds_catalogue *cat,
				   struct mds_cat_txn *txn,
				   uint64_t fileid);

/* -----------------------------------------------------------------------
 * Catalogue data -- Extended attributes
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_xattr_get(struct mds_catalogue *cat,
				  uint64_t fileid, const char *name,
				  void **val, uint32_t *vallen);

enum mds_status mds_cat_xattr_put(struct mds_catalogue *cat,
				  struct mds_cat_txn *txn,
				  uint64_t fileid, const char *name,
				  const void *val, uint32_t vallen);

enum mds_status mds_cat_xattr_del(struct mds_catalogue *cat,
				  struct mds_cat_txn *txn,
				  uint64_t fileid, const char *name);

enum mds_status mds_cat_xattr_list(struct mds_catalogue *cat,
				   uint64_t fileid,
				   mds_xattr_list_cb cb, void *ctx);

enum mds_status mds_cat_xattr_exists(struct mds_catalogue *cat,
				     uint64_t fileid,
				     const char *name);

/* -----------------------------------------------------------------------
 * Catalogue data -- Stripe maps
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_stripe_map_get(struct mds_catalogue *cat,
				       uint64_t fileid,
				       uint32_t *stripe_count,
				       uint32_t *stripe_unit,
				       uint32_t *mirror_count,
				       struct mds_ds_map_entry **entries);

enum mds_status mds_cat_stripe_map_put(struct mds_catalogue *cat,
				       struct mds_cat_txn *txn,
				       uint64_t fileid,
				       uint32_t stripe_count,
				       uint32_t stripe_unit,
				       uint32_t mirror_count,
				       const struct mds_ds_map_entry *entries);

enum mds_status mds_cat_stripe_map_del(struct mds_catalogue *cat,
				       struct mds_cat_txn *txn,
				       uint64_t fileid);

/**
 * Scan all stripe map entries.
 *
 * Calls @a cb for each stripe map record.  If cb returns non-zero,
 * the scan stops early.  Used by rebalance/resilver to find files
 * on a specific DS.
 *
 * @param cat  Catalogue handle.
 * @param cb   Callback per entry (same signature as stripe_map_scan_cb).
 * @param ctx  Opaque context for cb.
 * @return MDS_OK on success.
 */
typedef int (*mds_cat_stripe_map_scan_cb)(uint64_t fileid,
					 uint32_t stripe_count,
					 uint32_t stripe_unit,
					 uint32_t mirror_count,
					 const struct mds_ds_map_entry *entries,
					 void *ctx);

enum mds_status mds_cat_stripe_map_scan(struct mds_catalogue *cat,
					mds_cat_stripe_map_scan_cb cb,
					void *ctx);

/* -----------------------------------------------------------------------
 * Catalogue data -- DS registry
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_ds_get(struct mds_catalogue *cat,
			       uint32_t ds_id,
			       struct mds_ds_info *info);

enum mds_status mds_cat_ds_put(struct mds_catalogue *cat,
			       struct mds_cat_txn *txn,
			       const struct mds_ds_info *info);

enum mds_status mds_cat_ds_del(struct mds_catalogue *cat,
			       struct mds_cat_txn *txn,
			       uint32_t ds_id);

enum mds_status mds_cat_ds_list(struct mds_catalogue *cat,
				struct mds_ds_info **list,
				uint32_t *count);

enum mds_status mds_cat_ds_provision_get(struct mds_catalogue *cat,
					 uint32_t ds_id,
					 uint8_t *secret,
					 uint32_t secret_len,
					 uint64_t *epoch);

enum mds_status mds_cat_ds_provision_put(struct mds_catalogue *cat,
					 struct mds_cat_txn *txn,
					 uint32_t ds_id,
					 const uint8_t *secret,
					 uint32_t secret_len,
					 uint64_t epoch);

enum mds_status mds_cat_ds_provision_del(struct mds_catalogue *cat,
					 struct mds_cat_txn *txn,
					 uint32_t ds_id);

/* -----------------------------------------------------------------------
 * Catalogue data -- Quota
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_quota_rule_get(struct mds_catalogue *cat,
				       uint8_t scope_type,
				       uint64_t scope_id,
				       struct mds_quota_rule *rule);

enum mds_status mds_cat_quota_rule_put(struct mds_catalogue *cat,
				       struct mds_cat_txn *txn,
				       uint8_t scope_type,
				       uint64_t scope_id,
				       const struct mds_quota_rule *rule);

enum mds_status mds_cat_quota_usage_get(struct mds_catalogue *cat,
					uint8_t usage_type,
					uint64_t scope_id,
					struct mds_quota_usage *usage);

enum mds_status mds_cat_quota_usage_put(struct mds_catalogue *cat,
					struct mds_cat_txn *txn,
					uint8_t usage_type,
					uint64_t scope_id,
					const struct mds_quota_usage *usage);

/* -----------------------------------------------------------------------
 * Catalogue data -- GC queue
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_gc_enqueue(struct mds_catalogue *cat,
				   struct mds_cat_txn *txn,
				   uint64_t fileid,
				   uint32_t ds_id,
				   const uint8_t *nfs_fh,
				   uint32_t fh_len);

enum mds_status mds_cat_gc_peek(struct mds_catalogue *cat,
				struct mds_gc_entry *entry);

/**
 * Batched peek: fetch up to @a cap of the oldest queued entries in a
 * single backend call.
 *
 * Backends that implement this op natively (e.g. RonDB shim's single
 * full-table scan that keeps the smallest-cap seqs) drop the per-row
 * scan amortisation cost from O(N) per entry to O(N) per batch.
 * Backends that do not implement it fall back to a single mds_cat_gc_peek
 * call inside the dispatcher, so callers may use this entry point
 * unconditionally.
 *
 * @param cat      Catalogue handle.
 * @param entries  Caller-allocated array, must have room for @a cap.
 * @param cap      Maximum entries to return.  Must be >= 1.
 * @param[out] n_out  Receives the number of entries written (0..cap).
 *                Always set to 0 on error.
 * @return MDS_OK on success (including the empty-queue case where
 *         *n_out == 0), MDS_ERR_INVAL on bad arguments, or a
 *         backend-specific error.
 */
enum mds_status mds_cat_gc_peek_batch(struct mds_catalogue *cat,
				      struct mds_gc_entry *entries,
				      uint32_t cap,
				      uint32_t *n_out);

enum mds_status mds_cat_gc_dequeue(struct mds_catalogue *cat,
				   struct mds_cat_txn *txn,
				   uint64_t gc_seq);

enum mds_status mds_cat_gc_count(struct mds_catalogue *cat,
				 uint32_t *count);


/* -----------------------------------------------------------------------
 * Catalogue data -- DS prealloc pool (ENABLE_DS_PREALLOC)
 *
 * Persisted ring of precreated DS stub files (fileid PK, ds_id, FH,
 * owner_mds_id) so the pool survives a daemon restart.  Optional:
 * backends that leave the vtable slots NULL return MDS_ERR_NOSUPPORT and
 * the prealloc engine runs in-memory only.  struct
 * mds_prealloc_pool_row is defined in pnfs_mds.h.
 * ----------------------------------------------------------------------- */

/** Persist one precreated slot.  fileid is the PK. */
enum mds_status mds_cat_prealloc_pool_insert(struct mds_catalogue *cat,
		uint64_t fileid, uint32_t ds_id, const uint8_t *nfs_fh,
		uint32_t fh_len, uint32_t owner_mds_id, uint32_t stripe_unit);

/** Remove a slot row once it has been consumed (or reclaimed). */
enum mds_status mds_cat_prealloc_pool_delete(struct mds_catalogue *cat,
		uint64_t fileid);

/**
 * Scan the pool rows owned by @a owner_mds_id (0 = all).  Allocates
 * *rows_out (caller frees with free()); *n_out gets the count.
 */
enum mds_status mds_cat_prealloc_pool_scan(struct mds_catalogue *cat,
		uint32_t owner_mds_id,
		struct mds_prealloc_pool_row **rows_out, uint32_t *n_out);


/* -----------------------------------------------------------------------
 * Catalogue data -- Shard routing
 *
 * Maps fileid -> shard_id for cross-shard FH resolution (PUTFH).
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_shard_fileid_get(struct mds_catalogue *cat,
					 uint64_t fileid,
					 uint32_t *shard_id);

enum mds_status mds_cat_shard_fileid_put(struct mds_catalogue *cat,
					 struct mds_cat_txn *txn,
					 uint64_t fileid,
					 uint32_t shard_id);

enum mds_status mds_cat_shard_fileid_del(struct mds_catalogue *cat,
					 struct mds_cat_txn *txn,
					 uint64_t fileid);

/* -----------------------------------------------------------------------
 * Catalogue data -- Cross-shard extended dirents
 *
 * ext_dirents point from a local parent directory to a child inode
 * on a remote shard.  Used by LOOKUP to cross shard boundaries.
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_ext_dirent_get(struct mds_catalogue *cat,
				       uint64_t parent,
				       const char *name,
				       uint32_t *owner_mds_id,
				       uint64_t *target_fileid,
				       uint8_t *target_type,
				       uint64_t *anchor_id);

enum mds_status mds_cat_ext_dirent_put(struct mds_catalogue *cat,
				       struct mds_cat_txn *txn,
				       uint64_t parent,
				       const char *name,
				       uint32_t owner_mds_id,
				       uint64_t target_fileid,
				       uint8_t target_type,
				       uint64_t anchor_id);

enum mds_status mds_cat_ext_dirent_del(struct mds_catalogue *cat,
				       struct mds_cat_txn *txn,
				       uint64_t parent,
				       const char *name);

/* -----------------------------------------------------------------------
 * Catalogue data -- Cross-shard link anchors
 *
 * Anchors record that a remote MDS holds a hard link whose target
 * inode lives on this shard.  Used for nlink tracking on rename/unlink.
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_link_anchor_put(struct mds_catalogue *cat,
					struct mds_cat_txn *txn,
					uint64_t anchor_id,
					uint32_t remote_mds_id,
					uint64_t parent_fileid,
					const char *name);

enum mds_status mds_cat_link_anchor_del(struct mds_catalogue *cat,
					struct mds_cat_txn *txn,
					uint64_t anchor_id);

/* -----------------------------------------------------------------------
 * Catalogue introspection
 * ----------------------------------------------------------------------- */

struct commit_queue;
struct catalog_stats;

/**
 * Return a pointer to the catalogue's stats counters.
 * Never returns NULL for a valid catalogue handle.
 */
struct catalog_stats *mds_catalogue_stats(struct mds_catalogue *cat);

/** Return the backend type of the catalogue. */
enum mds_catalogue_backend mds_catalogue_backend_type(
	const struct mds_catalogue *cat);

/**
 * Run a backend-specific health probe.
 *
 * RonDB: canary row write/read in pre-created table.
 *
 * @param cat  Catalogue handle.
 * @return MDS_OK on success.
 */
enum mds_status mds_catalogue_probe(struct mds_catalogue *cat);

struct mig_inode_chunk;

/** Callback type for subtree iteration. */
typedef int (*mds_cat_subtree_iter_cb)(const struct mig_inode_chunk *chunk,
                                       void *arg);

/**
 * DFS-traverse a subtree rooted at @a root_fileid.
 *
 * For each inode: reads inode, dirents, stripe map, xattrs, and
 * inline data via the catalogue API, then invokes @a cb.
 *
 * @param cat          Catalogue handle.
 * @param root_fileid  Root of the subtree.
 * @param cb           Per-inode callback.
 * @param arg          Opaque argument for @a cb.
 * @return MDS_OK on success.
 */
enum mds_status mds_cat_subtree_iter(struct mds_catalogue *cat,
                                     uint64_t root_fileid,
                                     mds_cat_subtree_iter_cb cb,
                                     void *arg);

/* Backend-specific constructors (called by factory in catalogue_factory.c). */
#ifdef HAVE_RONDB
enum mds_status catalogue_rondb_open(const struct mds_config *cfg,
				     struct mds_catalogue **out);
#endif

/**
 * Wire a commit queue into the catalogue (non-owning).
 *
 * When set, write operations route through the CQ for batched
 * atomic commits + replication.  The catalogue does NOT own the
 * CQ -- the caller is responsible for destroying the CQ before
 * closing the catalogue.
 *
 * @param cat  Catalogue handle.
 * @param cq   Commit queue (NULL to disable CQ routing).
 */
void mds_catalogue_set_cq(struct mds_catalogue *cat,
			  struct commit_queue *cq);

/**
 * Return the commit queue wired into the catalogue (may be NULL).
 *
 * Transition helper for callers not yet migrated to catalogue
 * write operations.
 */
struct commit_queue *mds_catalogue_get_cq(const struct mds_catalogue *cat);

#endif /* MDS_CATALOGUE_H */

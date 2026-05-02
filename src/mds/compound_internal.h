/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * compound_internal.h — Private dispatch header for compound op modules.
 *
 * NOT a public API header.  Only included by compound_*.c and
 * compound.c for cross-file function calls.
 */

#ifndef COMPOUND_INTERNAL_H
#define COMPOUND_INTERNAL_H

#include "compound.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "mds_shard.h"

/* Forward declarations for subsystem types used in compound_data. */
struct mds_proxy_ctx;
struct subtree_map;
struct health_monitor;
struct cluster_membership;
struct copy_offload_table;
struct mds_quota_ctx;
struct commit_queue;
struct ds_health_monitor;
struct io_tracker;
struct rename_2pc_transport;

/* -----------------------------------------------------------------------
 * Cross-cutting helpers (defined in compound.c)
 * ----------------------------------------------------------------------- */

/**
 * Apply a shard to the request context: sets cd->db, cd->cq, and
 * cd->current_shard.  If @a shard is NULL, the context is unchanged.
 * Used by FH/path transition handlers (PUTROOTFH, PUTFH, LOOKUP, etc.).
 */
static inline void apply_shard(struct compound_data *cd,
			       const struct mds_shard *shard)
{
	if (shard == NULL) {
		return;
	}
	cd->current_shard = shard;
	cd->cq = shard->cq;
	if (shard->cat != NULL) {
		cd->cat = shard->cat;
	}
}

/**
 * Resolve the shard for the given path from the shard map and apply it.
 * Falls back to the default shard if path is empty or lookup fails.
 * No-op if shard_map is NULL (test/legacy mode).
 */
static inline void resolve_and_apply_shard(struct compound_data *cd,
					   const char *path)
{
	const struct mds_shard *shard;

	if (cd->shard_map == NULL) {
		return;
	}
	if (path != NULL && path[0] != '\0') {
		shard = mds_shard_map_lookup(cd->shard_map, path);
	} else {
		shard = mds_shard_map_get_default(cd->shard_map);
	}
	apply_shard(cd, shard);
}

void quota_submit_adjust(struct compound_data *cd,
			 uint64_t uid, uint64_t gid,
			 int64_t delta_bytes, int32_t delta_inodes);

/**
 * Generate a unique layout stateid.
 *
 * Single counter shared across all callers (compound_layout.c,
 * compound_data_io.c).  Thread-safe via _Atomic counter.
 */
void make_layout_stateid(uint32_t mds_id, struct nfs4_stateid *out);

/**
 * Read an inode using the request-local snapshot if available.
 *
 * Checks current_inode/saved_inode snapshots first.  On miss,
 * falls back to mds_ns_getattr() and seeds the current_inode
 * snapshot if the fileid matches current_fh.
 */
enum mds_status compound_inode_get(struct compound_data *cd,
				   uint64_t fileid,
				   struct mds_inode *out);

/**
 * Invalidate the snapshot for @a fileid.
 *
 * Must be called immediately after any mutation to the inode,
 * BEFORE any post-mutation re-read in the same handler.
 */
void compound_inode_invalidate(struct compound_data *cd, uint64_t fileid);

/**
 * Compound-local LOOKUP: parent check via snapshot, then local
 * dirent/inode reads from the active namespace backend.
 *
 * Does NOT perform explicit ext_dirent reroute, junction handling,
 * or child snapshot seeding.  Those remain in op_lookup().
 *
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_NOTDIR, or MDS_ERR_IO.
 */
enum mds_status compound_lookup_local_child(
	struct compound_data *cd,
	uint64_t parent_fileid,
	const char *name,
	struct mds_inode *child);

/**
 * Compatibility hook for mutation handlers that historically reset
 * the request-scoped read snapshot.
 *
 * Phase 5A retired that snapshot, so this is now a no-op.  Callers
 * should continue to invalidate request-local inode snapshots after
 * mutations via compound_inode_invalidate().
 */
void compound_ro_txn_reset(struct compound_data *cd);

/** Invalidate a dirent cache entry after a namespace mutation. */
void compound_dirent_invalidate(struct compound_data *cd,
				uint64_t parent_fileid, const char *name);

enum nfs4_status op_delegreturn(struct compound_data *cd,
			       const struct nfs4_op *op,
			       struct nfs4_result *res);

enum nfs4_status op_get_dir_delegation(struct compound_data *cd,
				       const struct nfs4_op *op,
				       struct nfs4_result *res);

/**
 * Recall every directory delegation on @p dir_fileid held by a
 * client other than the requesting client.  Called from namespace
 * mutation handlers before the mutation is committed so the
 * delegation holder learns the dirent set or attrs have changed.
 *
 * Null-safe: if dir delegations are disabled (cd->ddt == NULL) this
 * is a zero-cost no-op, preserving the Phase 8a path bit-for-bit.
 */
void compound_recall_dir_delegations(struct compound_data *cd,
				     uint64_t dir_fileid);

/**
 * Notify-or-recall dispatcher for namespace mutations.
 *
 * For each dir delegation on @p dir_fileid held by a different
 * client, either emit CB_NOTIFY (if the event is covered by the
 * client's granted notification mask) or CB_RECALL (otherwise).
 *
 * @param event     One of NOTIFY4_REMOVE_ENTRY / _ADD_ENTRY / _RENAME_ENTRY.
 * @param old_name  See dir_deleg_notify_dir().
 * @param new_name  See dir_deleg_notify_dir().
 */
void compound_notify_or_recall_dir(struct compound_data *cd,
				   uint64_t dir_fileid,
				   uint32_t event,
				   const char *old_name,
				   const char *new_name);

void compound_maybe_enqueue_ds_prepare(struct compound_data *cd,
				       const struct mds_inode *inode);
int compound_ds_prepare_check(const struct compound_data *cd,
			      uint64_t fileid);
enum mds_status compound_create_tracked_object(
	struct compound_data *cd,
	uint64_t parent_fileid,
	const char *name,
	enum mds_file_type type,
	uint32_t mode,
	uint64_t uid,
	uint64_t gid,
	struct ds_prealloc_ctx *prealloc,
	struct mds_inode *out);
enum mds_status compound_remove_tracked_object(
	struct compound_data *cd,
	uint64_t parent_fileid,
	const char *name);

enum nfs4_status quota_check_inode(struct compound_data *cd,
				   uint64_t uid, uint64_t gid);

enum nfs4_status quota_check_bytes(struct compound_data *cd,
				   uint64_t uid, uint64_t gid,
				   uint64_t additional_bytes);

enum nfs4_status require_current_fh(const struct compound_data *cd);

bool is_xattr_fh(uint64_t fh);

enum nfs4_status require_saved_fh(const struct compound_data *cd);

enum nfs4_status check_fh_frozen(const struct compound_data *cd,
				 uint64_t fh, const char *path);

enum nfs4_status check_subtree_frozen(const struct compound_data *cd);

enum nfs4_status check_repl_health(const struct compound_data *cd);

enum nfs4_status mds_status_to_nfs4(enum mds_status st);

uint64_t xattr_base_fileid(uint64_t fh);

/* -----------------------------------------------------------------------
 * Session ops (compound_session.c)
 * ----------------------------------------------------------------------- */

enum nfs4_status op_sequence(struct compound_data *cd,
			     const struct nfs4_op *op,
			     struct nfs4_result *res);
enum nfs4_status op_exchange_id(struct compound_data *cd,
				const struct nfs4_op *op,
				struct nfs4_result *res);
enum nfs4_status op_create_session(struct compound_data *cd,
				   const struct nfs4_op *op,
				   struct nfs4_result *res);
enum nfs4_status op_destroy_session(struct compound_data *cd,
				    const struct nfs4_op *op,
				    struct nfs4_result *res);
enum nfs4_status op_reclaim_complete(const struct compound_data *cd,
				     const struct nfs4_op *op,
				     struct nfs4_result *res);

/* -----------------------------------------------------------------------
 * Namespace ops (compound_namespace.c)
 * ----------------------------------------------------------------------- */

enum nfs4_status op_access(struct compound_data *cd,
			   const struct nfs4_op *op,
			   struct nfs4_result *res);
enum nfs4_status op_putrootfh(struct compound_data *cd,
			      const struct nfs4_op *op,
			      struct nfs4_result *res);
enum nfs4_status op_putfh(struct compound_data *cd,
			  const struct nfs4_op *op,
			  struct nfs4_result *res);
enum nfs4_status op_getfh(const struct compound_data *cd,
			  const struct nfs4_op *op,
			  struct nfs4_result *res);
enum nfs4_status op_savefh(struct compound_data *cd,
			   const struct nfs4_op *op,
			   struct nfs4_result *res);
enum nfs4_status op_restorefh(struct compound_data *cd,
			      const struct nfs4_op *op,
			      struct nfs4_result *res);
enum nfs4_status op_lookup(struct compound_data *cd,
			   const struct nfs4_op *op,
			   struct nfs4_result *res);
enum nfs4_status op_getattr(struct compound_data *cd,
			    const struct nfs4_op *op,
			    struct nfs4_result *res);
enum nfs4_status op_setattr(struct compound_data *cd,
			    const struct nfs4_op *op,
			    struct nfs4_result *res);
enum nfs4_status op_create(struct compound_data *cd,
			   const struct nfs4_op *op,
			   struct nfs4_result *res);
enum nfs4_status op_remove(struct compound_data *cd,
			   const struct nfs4_op *op,
			   struct nfs4_result *res);
enum nfs4_status op_rename(struct compound_data *cd,
			   const struct nfs4_op *op,
			   struct nfs4_result *res);
enum nfs4_status op_link(struct compound_data *cd,
			 const struct nfs4_op *op,
			 struct nfs4_result *res);
enum nfs4_status op_readdir(struct compound_data *cd,
			    const struct nfs4_op *op,
			    struct nfs4_result *res);

/* -----------------------------------------------------------------------
 * Data I/O ops (compound_data_io.c)
 * ----------------------------------------------------------------------- */

enum nfs4_status op_open(struct compound_data *cd,
			 const struct nfs4_op *op,
			 struct nfs4_result *res);
enum nfs4_status op_close(struct compound_data *cd,
			  const struct nfs4_op *op,
			  struct nfs4_result *res);
enum nfs4_status op_open_downgrade(struct compound_data *cd,
				   const struct nfs4_op *op,
				   struct nfs4_result *res);
enum nfs4_status op_openattr(struct compound_data *cd,
			     const struct nfs4_op *op,
			     struct nfs4_result *res);
enum nfs4_status op_read(struct compound_data *cd,
			 const struct nfs4_op *op,
			 struct nfs4_result *res);
enum nfs4_status op_commit(const struct compound_data *cd,
				  const struct nfs4_op *op,
				  struct nfs4_result *res);
enum nfs4_status op_lookupp(struct compound_data *cd,
				   const struct nfs4_op *op,
				   struct nfs4_result *res);
enum nfs4_status op_readlink(struct compound_data *cd,
				    const struct nfs4_op *op,
				    struct nfs4_result *res);
enum nfs4_status op_write(struct compound_data *cd,
			  const struct nfs4_op *op,
			  struct nfs4_result *res);
enum nfs4_status op_io_advise(const struct compound_data *cd,
			      const struct nfs4_op *op,
			      struct nfs4_result *res);

/** Validate a stateid for I/O (shared with io_advise). */
enum nfs4_status validate_io_stateid(const struct compound_data *cd,
				     const struct nfs4_stateid *stateid,
				     uint32_t required_access);

/** Promote an inline file to DS storage. */
enum nfs4_status promote_inline_to_ds(struct compound_data *cd,
				      struct mds_inode *inode);

/* -----------------------------------------------------------------------
 * pNFS layout ops (compound_layout.c)
 * ----------------------------------------------------------------------- */

enum nfs4_status op_layoutget(struct compound_data *cd,
			      const struct nfs4_op *op,
			      struct nfs4_result *res);
enum nfs4_status op_getdeviceinfo(const struct compound_data *cd,
				  const struct nfs4_op *op,
				  struct nfs4_result *res);
enum nfs4_status op_layoutreturn(struct compound_data *cd,
				 const struct nfs4_op *op,
				 struct nfs4_result *res);
enum nfs4_status op_layoutcommit(struct compound_data *cd,
				 const struct nfs4_op *op,
				 struct nfs4_result *res);
enum nfs4_status op_layouterror(struct compound_data *cd,
				const struct nfs4_op *op,
				struct nfs4_result *res);
enum nfs4_status op_layoutstats(const struct compound_data *cd,
				const struct nfs4_op *op,
				struct nfs4_result *res);

/* -----------------------------------------------------------------------
 * NFSv4.2 ops (compound_nfsv42.c)
 * ----------------------------------------------------------------------- */

enum nfs4_status op_allocate(struct compound_data *cd,
			     const struct nfs4_op *op,
			     struct nfs4_result *res);
enum nfs4_status op_deallocate(struct compound_data *cd,
			       const struct nfs4_op *op,
			       struct nfs4_result *res);
enum nfs4_status op_seek(struct compound_data *cd,
			 const struct nfs4_op *op,
			 struct nfs4_result *res);
enum nfs4_status op_read_plus(struct compound_data *cd,
			      const struct nfs4_op *op,
			      struct nfs4_result *res);
enum nfs4_status op_copy(struct compound_data *cd,
			 const struct nfs4_op *op,
			 struct nfs4_result *res);
enum nfs4_status op_copy_notify(const struct compound_data *cd,
				const struct nfs4_op *op,
				struct nfs4_result *res);
enum nfs4_status op_offload_cancel(struct compound_data *cd,
				   const struct nfs4_op *op,
				   struct nfs4_result *res);
enum nfs4_status op_offload_status(struct compound_data *cd,
				   const struct nfs4_op *op,
				   struct nfs4_result *res);
enum nfs4_status op_write_same(struct compound_data *cd,
			       const struct nfs4_op *op,
			       struct nfs4_result *res);
enum nfs4_status op_clone(struct compound_data *cd,
			  const struct nfs4_op *op,
			  struct nfs4_result *res);

/* RFC 8276 extended attribute operations */
enum nfs4_status op_getxattr(struct compound_data *cd,
			     const struct nfs4_op *op,
			     struct nfs4_result *res);
enum nfs4_status op_setxattr(struct compound_data *cd,
			     const struct nfs4_op *op,
			     struct nfs4_result *res);
enum nfs4_status op_listxattrs(struct compound_data *cd,
			       const struct nfs4_op *op,
			       struct nfs4_result *res);
enum nfs4_status op_removexattr(struct compound_data *cd,
				const struct nfs4_op *op,
				struct nfs4_result *res);

/* -----------------------------------------------------------------------
 * Catalogue dispatch helpers
 *
 * Shard-local helpers route through the catalogue.
 * Root-global helpers route through the root catalogue even if
 * cd has already been switched to a child shard.
 *
 * This split is critical for multi-shard correctness:
 * - namespace and inline/xattr/stripe-map state follow cd->db
 * - DS registry follows the root-global catalogue db
 * - layout/recovery coordination state follows cd->root_db
 * ----------------------------------------------------------------------- */

/**
 * True when the catalogue handle covers the current request db.
 *
 * Returns false when:
 *   - cd->cat is NULL (test / legacy path)
 *   - cd->db has been swapped to a child shard by apply_shard()
 *
 */
/**
 * True when the catalogue handle is available.
 * Always true in RonDB-only mode when cat is set.
 */
static inline bool cat_on_root_db(const struct compound_data *cd)
{
	return (cd != NULL && cd->cat != NULL);
}

/**
 * True when the catalogue handle covers the root-global state.
 * Always true in RonDB-only mode.
 */
static inline bool cat_on_root_global_db(const struct compound_data *cd)
{
	return (cd != NULL && cd->cat != NULL);
}

/**
 * Root-global catalogue handle.
 * Returns the catalogue directly.
 */
static inline struct mds_catalogue *
cat_root_global_cat(const struct compound_data *cd)
{
	if (cd == NULL) {
		return NULL;
	}
	return cd->cat;
}

/* -- Namespace --------------------------------------------------------- */

static inline enum mds_status
cat_getattr(struct compound_data *cd, uint64_t fid,
	    struct mds_inode *out)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ns_getattr(cd->cat, fid, out);
}

static inline enum mds_status
cat_lookup(struct compound_data *cd, uint64_t parent,
	   const char *name, struct mds_inode *child)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ns_lookup(cd->cat, parent, name, child);
}

static inline enum mds_status
cat_create(struct compound_data *cd, uint64_t parent,
	   const char *name, enum mds_file_type type,
	   uint32_t mode, uint64_t uid, uint64_t gid,
	   struct ds_prealloc_ctx *pa, struct mds_inode *out)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ns_create(cd->cat, NULL, parent, name,
				type, mode, uid, gid, pa, out);
}

static inline enum mds_status
cat_remove(struct compound_data *cd, uint64_t parent,
	   const char *name)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ns_remove(cd->cat, NULL, parent, name);
}

static inline enum mds_status
cat_rename(struct compound_data *cd,
	   uint64_t sp, const char *sn,
	   uint64_t dp, const char *dn)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ns_rename(cd->cat, NULL, sp, sn, dp, dn);
}

static inline enum mds_status
cat_link(struct compound_data *cd, uint64_t parent,
	 const char *name, uint64_t target)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ns_link(cd->cat, NULL, parent, name,
			       target);
}

static inline enum mds_status
cat_setattr(struct compound_data *cd, uint64_t fid,
	    const struct mds_inode *attrs, uint32_t mask)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ns_setattr(cd->cat, NULL, fid, attrs,
				  mask);
}

static inline enum mds_status
cat_nlink_adjust(struct compound_data *cd, uint64_t fid,
		 int32_t delta)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ns_nlink_adjust(cd->cat, fid, delta);
}

/**
 * Readdir dispatch: catalogue-only.
 */
static inline enum mds_status
cat_readdir(struct compound_data *cd, uint64_t parent,
	    const char *start_after, mds_readdir_cb cat_cb, void *ctx)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ns_readdir(cd->cat, parent, start_after,
				  NULL, cat_cb, ctx);
}

/**
 * Readdir_plus dispatch: catalogue-only.  Delivers dirent + child
 * inode attrs in a single backend call (one NDB txn on the RonDB
 * backend; transparent fallback to ns_readdir + per-entry ns_getattr
 * for backends that do not implement the fused op natively).
 */
static inline enum mds_status
cat_readdir_plus(struct compound_data *cd, uint64_t parent,
		 const char *start_after,
		 mds_readdir_plus_cb cat_cb, void *ctx)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ns_readdir_plus(cd->cat, parent, start_after,
				       NULL, cat_cb, ctx);
}

/* -- Inline data ------------------------------------------------------- */

static inline enum mds_status
cat_inline_get(struct compound_data *cd, uint64_t fid,
	       void *buf, uint32_t buflen, uint32_t *outlen)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_inline_get(cd->cat, fid, buf,
				  buflen, outlen);
}

/* -- Extended attributes ----------------------------------------------- */

static inline enum mds_status
cat_xattr_get(struct compound_data *cd, uint64_t fid,
	      const char *name, void **val, uint32_t *vallen)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_xattr_get(cd->cat, fid, name, val,
				  vallen);
}

static inline enum mds_status
cat_xattr_list(struct compound_data *cd, uint64_t fid,
	       mds_xattr_list_cb cb, void *ctx)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_xattr_list(cd->cat, fid, cb, ctx);
}

static inline enum mds_status
cat_xattr_exists(struct compound_data *cd, uint64_t fid,
		 const char *name)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_xattr_exists(cd->cat, fid, name);
}

static inline enum mds_status
cat_xattr_put(struct compound_data *cd, uint64_t fid,
	      const char *name, const void *val, uint32_t vallen)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_xattr_put(cd->cat, NULL, fid, name,
				 val, vallen);
}

static inline enum mds_status
cat_xattr_del(struct compound_data *cd, uint64_t fid,
	      const char *name)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_xattr_del(cd->cat, NULL, fid, name);
}

/* -- Stripe maps ------------------------------------------------------- */

static inline enum mds_status
cat_stripe_map_get(struct compound_data *cd, uint64_t fid,
		   uint32_t *sc, uint32_t *su, uint32_t *mc,
		   struct mds_ds_map_entry **entries)
{
	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_stripe_map_get(cd->cat, fid, sc, su,
				      mc, entries);
}

/* -- DS registry ------------------------------------------------------- */

static inline enum mds_status
cat_ds_get(const struct compound_data *cd, uint32_t ds_id,
	   struct mds_ds_info *info)
{
	if (cd == NULL || cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ds_get(cd->cat, ds_id, info);
}

static inline enum mds_status
cat_ds_list(const struct compound_data *cd, struct mds_ds_info **list,
	    uint32_t *count)
{
	if (cd == NULL || cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ds_list(cd->cat, list, count);
}

static inline enum mds_status
cat_ds_provision_get(const struct compound_data *cd, uint32_t ds_id,
		     uint8_t *secret, uint32_t secret_len,
		     uint64_t *epoch)
{
	if (cd == NULL || cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_cat_ds_provision_get(cd->cat, ds_id, secret,
					secret_len, epoch);
}

/* -- Coordination state (read-only lookups) ---------------------------- */

static inline enum mds_status
coord_layout_get_by_stateid(const struct compound_data *cd,
			    const uint8_t other[12],
			    uint64_t *cid, uint64_t *fid,
			    uint32_t *iomode, uint64_t *off,
			    uint64_t *len, uint32_t *seqid)
{
	if (cd == NULL || cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_coord_layout_get_by_stateid(cd->cat, other,
					       cid, fid, iomode,
					       off, len, seqid);
}

static inline enum mds_status
coord_layout_scan_for_file(const struct compound_data *cd, uint64_t fid,
			   bool *has)
{
	if (cd == NULL || cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	return mds_coord_layout_scan_for_file(cd->cat, fid, has);
}

#endif /* COMPOUND_INTERNAL_H */

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * commit_queue.c — Catalogue-dispatch commit pipeline.
 *
 * Request threads build immutable operation descriptors and submit
 * them to commit_queue_submit().  Each op is dispatched directly
 * through the catalogue vtable on the caller's thread — no writer
 * thread, no queue, no serialization.
 *
 * The commit pipeline (writer thread, ring buffer,
 * batch coalescing, txn_delta_builder) was removed in the Level-B
 * cleanup.  RonDB has native concurrent writes with row-level
 * locking.  All backend
 * catalogues route through the same vtable, so
 * correctness is preserved for both backends.
 *
 * Core invariant: no NFS reply is sent before the catalogue write
 * that makes the reply true.
 */

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <stdatomic.h>

#include "commit_queue.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "catalogue_internal.h"
#include "ds_cache.h"
#include "quota.h"

/* ----------------------------------------------------------------------- */

struct commit_queue {
	struct mds_catalogue *cat;     /**< Authority handle (owns if wrapped). */
	struct repl_conn     *repl;
	int                   repl_mode;
	uint32_t              shard_id;
	_Atomic(struct ds_cache *) ds_cache;
	bool                  owns_cat; /**< True when cat was auto-wrapped. */
};

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

struct repl_conn *commit_queue_get_repl(const struct commit_queue *cq)
{
	if (cq == NULL) {
		return NULL;
	}
	return cq->repl;
}

int commit_queue_get_repl_mode(const struct commit_queue *cq)
{
	if (cq == NULL) {
		return 0;
	}
	return cq->repl_mode;
}

void commit_queue_set_ds_cache(struct commit_queue *cq,
			       struct ds_cache *ds_cache)
{
	if (cq == NULL) {
		return;
	}
	atomic_store(&cq->ds_cache, ds_cache);
}

int commit_queue_create(struct mds_catalogue *cat,
			struct repl_conn *repl, int repl_mode,
			uint32_t batch_size, uint32_t batch_max_bytes,
			uint32_t flush_ms, uint32_t queue_depth,
			uint32_t shard_id,
			struct commit_queue **out)
{
	struct commit_queue *cq;
	bool owns_cat = false;

	/* Batch parameters are accepted for API compat but ignored —
	 * each op is dispatched immediately via the catalogue vtable. */
	(void)batch_size;
	(void)batch_max_bytes;
	(void)flush_ms;
	(void)queue_depth;

	if (out == NULL) {
		return -EINVAL;
	}

	if (cat == NULL) {
		return -EINVAL;
	}

	cq = calloc(1, sizeof(*cq));
	if (cq == NULL) {
		if (owns_cat) {
			mds_catalogue_close(cat);
		}
		return -ENOMEM;
	}

	cq->cat       = cat;
	cq->repl      = repl;
	cq->repl_mode = repl_mode;
	cq->shard_id  = shard_id;
	cq->owns_cat  = owns_cat;
	atomic_store(&cq->ds_cache, NULL);

	*out = cq;
	return 0;
}

/**
 * Synchronous catalogue-dispatch submit.
 *
 * Each op is dispatched directly through the catalogue vtable on
 * the caller's thread — no writer thread, no queue, no serialization.
 *
 * Borrowed-pointer lifetime contract
 * ---------------------------------
 * commit_op fields documented as `Borrowed; valid during submit.`
 * (today: commit_op_create.layout_ds_ids,
 * commit_op_layout_put.ds_ids, commit_op_layout_del.ds_ids in
 * include/commit_queue.h) need only outlive the call to
 * commit_queue_submit().  Because dispatch is synchronous and the
 * vtable backends that store the array deep-copy before returning
 * (see tests/catalogue_memdb.c::mem_layout_grant; the RonDB shim
 * re-encodes via the row writer), callers may borrow stack-locals
 * or transient buffers — see e.g. layout_recall.c::revoke_layout
 * and compound_data_io.c::op_open's pre_create_ds_id path.  If
 * commit_queue_submit is ever made asynchronous the contract has
 * to flip to copy-on-submit; track every callsite that takes the
 * address of a stack variable when making that change.
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status commit_queue_submit(struct commit_queue *cq,
				    struct commit_op *op)
{
	struct mds_catalogue *cat;
	enum mds_status st;

	if (cq == NULL || op == NULL) {
		return MDS_ERR_INVAL;
	}

	cat = cq->cat;
	if (cat == NULL) {
		return MDS_ERR_INVAL;
	}

	switch (op->type) {
	case COMMIT_OP_CREATE: {
		struct commit_op_create *a = &op->args.create;
		st = mds_cat_ns_create(cat, NULL,
				       a->parent_fileid, a->name,
				       a->type, a->mode, a->uid, a->gid,
				       a->prealloc, a->out);
		if (st != MDS_OK) { break; }

		/* Layout pre-grant: persist layout state after CREATE.
		 *
		 * When skip_transient_ndb is set (lab / single-MDS default),
		 * the non-fused LAYOUTGET path skips mds_coord_layout_grant
		 * via the guard at src/mds/compound_layout.c:731.  Mirror that
		 * here so the fused path does not regress vs unfused: the
		 * caller's in-memory pregrant stateid is sufficient.
		 * Signal success so the caller still propagates the pregrant.
		 */
		if (a->layout_pregrant && a->out != NULL) {
			if (a->skip_transient_ndb) {
				a->layout_pregrant_ok = true;
			} else {
				enum mds_status lg_st;

				lg_st = mds_coord_layout_grant(
					cat, NULL,
					a->layout_clientid, a->out->fileid,
					a->layout_iomode, a->layout_offset,
					a->layout_length, &a->layout_stateid,
					a->layout_ds_ids,
					a->layout_ds_count);
				a->layout_pregrant_ok = (lg_st == MDS_OK);
			}
		}
		break;
	}
	case COMMIT_OP_REMOVE:
		st = mds_cat_ns_remove(cat, NULL,
				       op->args.remove_op.parent_fileid,
				       op->args.remove_op.name);
		break;

	case COMMIT_OP_RENAME:
		st = mds_cat_ns_rename(cat, NULL,
				       op->args.rename_op.src_parent,
				       op->args.rename_op.src_name,
				       op->args.rename_op.dst_parent,
				       op->args.rename_op.dst_name);
		break;

	case COMMIT_OP_LINK:
		st = mds_cat_ns_link(cat, NULL,
				     op->args.link_op.parent_fileid,
				     op->args.link_op.name,
				     op->args.link_op.target_fileid);
		break;

	case COMMIT_OP_SETATTR:
		st = mds_cat_ns_setattr(cat, NULL,
					op->args.setattr.fileid,
					&op->args.setattr.attrs,
					op->args.setattr.mask);
		break;

	case COMMIT_OP_INLINE_WRITE: {
		struct commit_op_inline_write *iw = &op->args.inline_write;
		st = mds_cat_inline_put(cat, NULL, iw->fileid,
					iw->data + iw->offset, iw->len);
		if (st != MDS_OK) { break; }
		/* Update inode metadata (size, mtime). */
		{
			struct mds_inode inode;
			st = mds_cat_ns_getattr(cat, iw->fileid, &inode);
			if (st == MDS_OK) {
				uint64_t new_end = (uint64_t)iw->offset + iw->len;
				struct timespec now;
				clock_gettime(CLOCK_REALTIME, &now);
				inode.mtime = now;
				inode.ctime = now;
				inode.change++;
				if (new_end > inode.size) {
					inode.size = new_end;
					inode.space_used = new_end;
				}
				st = mds_cat_ns_setattr(cat, NULL,
							iw->fileid, &inode, 0);
			}
		}
		break;
	}
	case COMMIT_OP_INLINE_DEL:
		st = mds_cat_inline_del(cat, NULL,
					op->args.inline_del.fileid);
		break;

	case COMMIT_OP_INODE_PUT:
		st = mds_cat_inode_put(cat, NULL,
				       &op->args.inode_put.inode);
		break;

	case COMMIT_OP_RECOVERY_PUT: {
		struct commit_op_recovery_put *rp = &op->args.recovery_put;
		st = mds_coord_recovery_put(cat, NULL,
					    rp->clientid,
					    rp->co_ownerid,
					    rp->co_ownerid_len,
					    rp->verifier);
		break;
	}
	case COMMIT_OP_RECOVERY_DEL:
		st = mds_coord_recovery_del(cat, NULL,
					    op->args.recovery_del.clientid);
		break;

	case COMMIT_OP_DS_STATE: {
		struct mds_ds_info info;
		st = mds_cat_ds_get(cat, op->args.ds_state.ds_id, &info);
		if (st == MDS_OK) {
			info.state = op->args.ds_state.new_state;
			st = mds_cat_ds_put(cat, NULL, &info);
		}
		break;
	}
	case COMMIT_OP_LAYOUT_STATE_PUT: {
		struct commit_op_layout_put *lp = &op->args.layout_put;
		/* Create stripe map first if requested. */
		if (lp->create_stripe_map && lp->stripe_entries != NULL) {
			st = mds_cat_stripe_map_put(cat, NULL,
						    lp->fileid,
						    lp->stripe_count,
						    lp->stripe_unit,
						    lp->mirror_count,
						    lp->stripe_entries);
			if (st != MDS_OK) { break; }
		}
		st = mds_coord_layout_grant(cat, NULL,
					    lp->clientid, lp->fileid,
					    lp->iomode, lp->offset,
					    lp->length, &lp->stateid,
					    lp->ds_ids, lp->ds_count);
		break;
	}
	case COMMIT_OP_LAYOUT_STATE_DEL: {
		struct commit_op_layout_del *ld = &op->args.layout_del;
		st = mds_coord_layout_return(cat, NULL,
					     ld->stateid_other,
					     ld->clientid, ld->fileid,
					     ld->ds_ids, ld->ds_count);
		break;
	}
	case COMMIT_OP_STRIPE_MAP_PUT: {
		const struct commit_op_stripe_map_put *sp = &op->args.stripe_map_put;
		st = mds_cat_stripe_map_put(cat, NULL,
					    sp->fileid, sp->stripe_count,
					    sp->stripe_unit, sp->mirror_count,
					    sp->entries);
		break;
	}
	case COMMIT_OP_REBALANCE_MOVE: {
		const struct commit_op_rebalance_move *rm = &op->args.rebalance_move;
		st = mds_cat_stripe_map_put(cat, NULL,
					    rm->fileid, rm->stripe_count,
					    rm->stripe_unit, rm->mirror_count,
					    rm->entries);
		if (st == MDS_OK) {
			st = mds_cat_gc_enqueue(cat, NULL,
						rm->gc_fileid, rm->gc_ds_id,
						rm->gc_nfs_fh, rm->gc_fh_len);
		}
		break;
	}
	case COMMIT_OP_QUOTA_ADJUST: {
		struct commit_op_quota_adjust *qa = &op->args.quota_adjust;
		const uint8_t types[2]  = { (uint8_t)'u', (uint8_t)'g' };
		const uint64_t ids[2]   = { qa->uid, qa->gid };
		st = MDS_OK;
		for (int qi = 0; qi < 2 && st == MDS_OK; qi++) {
			struct mds_quota_usage usage;
			enum mds_status gs = mds_cat_quota_usage_get(
				cat, types[qi], ids[qi], &usage);
			if (gs == MDS_ERR_NOTFOUND) {
				memset(&usage, 0, sizeof(usage));
			} else if (gs != MDS_OK) {
				st = gs;
				break;
			}
			if (qa->delta_bytes >= 0) {
				usage.used_bytes += (uint64_t)qa->delta_bytes;
			} else {
				uint64_t sub = (uint64_t)(-qa->delta_bytes);
				usage.used_bytes = (usage.used_bytes > sub)
						 ? usage.used_bytes - sub : 0;
			}
			if (qa->delta_inodes >= 0) {
				usage.used_inodes += (uint64_t)qa->delta_inodes;
			} else {
				uint64_t sub = (uint64_t)(-qa->delta_inodes);
				usage.used_inodes = (usage.used_inodes > sub)
						  ? usage.used_inodes - sub : 0;
			}
			st = mds_cat_quota_usage_put(cat, NULL,
						    types[qi], ids[qi],
						    &usage);
		}
		break;
	}
	case COMMIT_OP_DS_REGISTER: {
		const struct commit_op_ds_register *dr = &op->args.ds_register;
		struct mds_ds_info existing;

		/* Reject duplicate IDs. */
		if (mds_cat_ds_get(cat, dr->info.ds_id, &existing) == MDS_OK) {
			st = MDS_ERR_EXISTS;
			break;
		}
		/* Validate. */
		if (dr->info.addr[0] == '\0' || dr->info.port == 0) {
			st = MDS_ERR_INVAL;
			break;
		}
		if (dr->info.used_bytes > dr->info.total_bytes) {
			st = MDS_ERR_INVAL;
			break;
		}
		/* Force OFFLINE on registration. */
		{
			struct mds_ds_info reg = dr->info;
			reg.state = DS_OFFLINE;
			st = mds_cat_ds_put(cat, NULL, &reg);
		}
		break;
	}
	case COMMIT_OP_DS_REMOVE: {
		uint32_t rm_ds_id = op->args.ds_remove.ds_id;
		struct mds_ds_info rm_info;

		/* Must exist. */
		st = mds_cat_ds_get(cat, rm_ds_id, &rm_info);
		if (st != MDS_OK) { break; }
		/* Must be OFFLINE. */
		if (rm_info.state != DS_OFFLINE) {
			st = MDS_ERR_PERM;
			break;
		}
		st = mds_cat_ds_del(cat, NULL, rm_ds_id);
		break;
	}

	default:
		st = MDS_ERR_INVAL;
		break;
	}

	op->result = st;
	return st;
}

void commit_queue_destroy(struct commit_queue *cq)
{
	if (cq == NULL) {
		return;
	}

	if (cq->owns_cat && cq->cat != NULL) {
		/* Shallow free only — the underlying catalogue handle is
		 * owned by the caller who passed it to
		 * commit_queue_create().  mds_catalogue_close() would
		 * invoke the backend close which closes the db,
		 * causing a double-free. */
		free(cq->cat);
	}
	free(cq);
}

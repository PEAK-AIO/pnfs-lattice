/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 */
/*
 * compound_nfsv42.c -- NFSv4.2 space/copy ops (ALLOCATE..CLONE).
 */

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "compound_internal.h"
#include "open_state.h"
#include "proxy_io.h"
#include "copy_offload.h"
#include "quota.h"
#include "commit_queue.h"
#include "placement.h"
#include "io_tracker.h"
#include "health.h"
#include "hpc_shared.h"

/* RFC 8276 S8.4: the "user." namespace prefix is NOT transmitted on
 * the wire -- the Linux client strips it before XDR, so SETXATTR /
 * GETXATTR / REMOVEXATTR arrive with a->name == "pnfs.hpc_shared".
 * Accept both spellings so local tools that pass the full name and
 * on-the-wire clients hit the same trigger. */
static bool hpc_xattr_name_match(const char *name)
{
	if (name == NULL) {
		return false;
	}
	if (strcmp(name, HPC_SHARED_XATTR_NAME) == 0) {
		return true;
	}
	return strcmp(name, HPC_SHARED_XATTR_NAME +
			    strlen("user.")) == 0;
}


/** ALLOCATE (RFC 7862 S15.1): preallocate space. */

/*
 * Promote an inline file to DS storage if needed.  Returns NFS4_OK if
 * the file is not inline or was successfully promoted.  On error the
 * caller should return the error status to the client.
 */
static enum nfs4_status
promote_if_inline(struct compound_data *cd)
{
	struct mds_inode pi_inode;
	enum mds_status pi_st;

	pi_st = cat_getattr(cd, cd->current_fh.fileid, &pi_inode);
	if (pi_st != MDS_OK) {
		return NFS4_OK; /* Best-effort; let the real op handle errors. */
}

	if (!(pi_inode.flags & MDS_IFLAG_INLINE)) {
		return NFS4_OK;
}

	return promote_inline_to_ds(cd, &pi_inode);
}

enum nfs4_status op_allocate(struct compound_data *cd,
				    const struct nfs4_op *op,
				    struct nfs4_result *res)
{
	const struct nfs4_arg_allocate *a = &op->arg.allocate;
	enum nfs4_status nst;
	enum mds_status st;
	struct mds_inode inode;
	(void)res;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	if (cd->proxy == NULL) {
		return NFS4ERR_NOTSUPP;
}

	/* Verify the file is regular. */
	st = cat_getattr(cd, cd->current_fh.fileid, &inode);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
}
	if (inode.type != MDS_FTYPE_REG) {
		return NFS4ERR_INVAL;
}

	/* Validate stateid. */
	nst = validate_io_stateid(cd, &a->stateid,
				  OPEN4_SHARE_ACCESS_WRITE);
	if (nst != NFS4_OK) {
		return nst;
}

	/* CERT INT30-C: check for uint64 overflow on offset+length. */
	if (a->length > 0 && a->offset > UINT64_MAX - a->length) {
		return NFS4ERR_INVAL;
}

	/* Inline files: promote to DS before fallocate. */
	nst = promote_if_inline(cd);
	if (nst != NFS4_OK) {
		return nst;
}


	/* Quota: check byte quota for allocation growth. */
	if (a->offset + a->length > inode.size) {
		nst = quota_check_bytes(cd, inode.uid, inode.gid,
					a->offset + a->length - inode.size);
		if (nst != NFS4_OK) {
			return nst;
		}
	}
    /* Proxy fallocate to DS. */
	st = mds_proxy_allocate(cd->proxy, cd->cat,
				cd->current_fh.fileid, a->offset, a->length);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
}

	/* Extend file size if allocation goes beyond current size. */
	{
		uint64_t new_end = a->offset + a->length;
		uint64_t old_size = inode.size;

		if (new_end > old_size) {
			inode.size = new_end;
			inode.space_used = inode.size;
			st = cat_setattr(cd, cd->current_fh.fileid,
					    &inode, MDS_ATTR_SIZE);
			if (st != MDS_OK) {
				return mds_status_to_nfs4(st);
			}
			compound_inode_invalidate(cd,
				cd->current_fh.fileid);
			quota_submit_adjust(cd, inode.uid, inode.gid,
					    (int64_t)(new_end - old_size), 0);
		}
	}
	return NFS4_OK;
}

/** DEALLOCATE (RFC 7862 S15.4): punch hole. */
enum nfs4_status op_deallocate(struct compound_data *cd,
				      const struct nfs4_op *op,
				      struct nfs4_result *res)
{
	const struct nfs4_arg_deallocate *a = &op->arg.deallocate;
	enum nfs4_status nst;
	enum mds_status st;
	struct mds_inode inode;
	(void)res;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/* Inline files don't support sparse ops -- promote first. */
	nst = promote_if_inline(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	if (cd->proxy == NULL) {
		return NFS4ERR_NOTSUPP;
}

	st = cat_getattr(cd, cd->current_fh.fileid, &inode);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
}
	if (inode.type != MDS_FTYPE_REG) {
		return NFS4ERR_INVAL;
}

	/* Validate stateid. */
	nst = validate_io_stateid(cd, &a->stateid,
				  OPEN4_SHARE_ACCESS_WRITE);
	if (nst != NFS4_OK) {
		return nst;
}

	st = mds_proxy_deallocate(cd->proxy, cd->cat,
				  cd->current_fh.fileid, a->offset, a->length);
	return mds_status_to_nfs4(st);
}

/** SEEK (RFC 7862 S15.11): find next data or hole. */
enum nfs4_status op_seek(struct compound_data *cd,
				const struct nfs4_op *op,
				struct nfs4_result *res)
{
	const struct nfs4_arg_seek *a = &op->arg.seek;
	struct nfs4_res_seek *r = &res->res.seek;
	enum nfs4_status nst;
	enum mds_status st;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/* Inline files are dense -- promote before seeking holes. */
	nst = promote_if_inline(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	if (cd->proxy == NULL) {
		return NFS4ERR_NOTSUPP;
}

	/* Validate stateid. */
	nst = validate_io_stateid(cd, &a->stateid,
				  OPEN4_SHARE_ACCESS_READ);
	if (nst != NFS4_OK) {
		return nst;
}

	st = mds_proxy_seek(cd->proxy, cd->cat, cd->current_fh.fileid,
			    a->offset, a->what,
			    &r->offset, &r->eof);
	return mds_status_to_nfs4(st);
}

/** READ_PLUS (RFC 7862 S15.10): segmented read. */
enum nfs4_status op_read_plus(struct compound_data *cd,
				     const struct nfs4_op *op,
				     struct nfs4_result *res)
{
	const struct nfs4_arg_read_plus *a = &op->arg.read_plus;
	struct nfs4_res_read_plus *r = &res->res.read_plus;
	enum nfs4_status nst;
	enum mds_status st;
	uint64_t hole_off = 0;
	bool hole_eof = false;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/* Inline files are dense -- promote before hole-aware read. */
	nst = promote_if_inline(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	if (cd->proxy == NULL) {
		return NFS4ERR_NOTSUPP;
}

	/* Validate stateid. */
	nst = validate_io_stateid(cd, &a->stateid,
				  OPEN4_SHARE_ACCESS_READ);
	if (nst != NFS4_OK) {
		return nst;
}

	/*
	 * Check if the region starts in a hole by seeking for data.
	 * If data starts beyond our requested offset, we have a hole
	 * segment first.
	 */
	st = mds_proxy_seek(cd->proxy, cd->cat, cd->current_fh.fileid,
			    a->offset, NFS4_CONTENT_DATA,
			    &hole_off, &hole_eof);
	if (st != MDS_OK && st != MDS_ERR_IO) {
		/* No stripe map -- file may be empty; return single hole. */
		r->eof = true;
		r->seg_count = 1;
		r->segs[0].content_type = NFS4_CONTENT_HOLE;
		r->segs[0].offset = a->offset;
		r->segs[0].u.hole.length = a->count;
		return NFS4_OK;
	}

	if (hole_eof) {
		/* Entire range is beyond EOF. */
		r->eof = true;
		r->seg_count = 1;
		r->segs[0].content_type = NFS4_CONTENT_HOLE;
		r->segs[0].offset = a->offset;
		r->segs[0].u.hole.length = a->count;
		return NFS4_OK;
	}

	if (hole_off > a->offset) {
		/* Hole segment before data. */
		uint64_t hole_len = hole_off - a->offset;

		if (hole_len > a->count) {
			hole_len = a->count;
}
		r->segs[0].content_type = NFS4_CONTENT_HOLE;
		r->segs[0].offset = a->offset;
		r->segs[0].u.hole.length = hole_len;
		r->seg_count = 1;
		r->eof = false;
		return NFS4_OK;
	}

	/* Data segment: read actual bytes via proxy. */
	{
		uint32_t count = a->count;
		uint32_t nr = 0;
		bool eof_flag = false;

		if (count > MDS_XATTR_VAL_MAX) {
			count = MDS_XATTR_VAL_MAX;
}

		st = mds_proxy_read(cd->proxy, cd->cat, cd->current_fh.fileid,
				    a->offset, count,
				    r->segs[0].u.data.data, &nr, &eof_flag);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
}

		r->segs[0].content_type = NFS4_CONTENT_DATA;
		r->segs[0].offset = a->offset;
		r->segs[0].u.data.data_len = nr;
		r->seg_count = 1;
		r->eof = eof_flag;
	}

	return NFS4_OK;
}

/**
 * Promote both current_fh and saved_fh from inline to DS if needed.
 * Restores current_fh after promoting saved_fh.
 */
static enum nfs4_status promote_inline_pair(struct compound_data *cd)
{
	enum nfs4_status nst;

	/* Promote current_fh (destination). */
	nst = promote_if_inline(cd);
	if (nst != NFS4_OK) {
		return nst;
	}
	/* Promote saved_fh (source). */
	{
		uint64_t save = cd->current_fh.fileid;
		cd->current_fh.fileid = cd->saved_fh.fileid;
		nst = promote_if_inline(cd);
		cd->current_fh.fileid = save;
	}
	return nst;
}

/**
 * Update destination inode size after a successful synchronous data
 * copy and submit the corresponding quota adjustment.
 *
 * @return NFS4_OK, or an NFS4 error if the setattr fails.
 */
static enum nfs4_status copy_update_dst_size(struct compound_data *cd,
					     uint64_t dst_fh,
					     uint64_t new_end,
					     uint64_t old_size,
					     uint64_t uid, uint64_t gid)
{
	struct mds_inode post;
	enum mds_status st;

	st = cat_getattr(cd, dst_fh, &post);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
	}
	if (new_end <= post.size) {
		return NFS4_OK; /* Another writer already extended. */
	}
	post.size = new_end;
	post.space_used = new_end;
	st = cat_setattr(cd, dst_fh, &post, MDS_ATTR_SIZE);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
	}
	compound_inode_invalidate(cd, dst_fh);
	quota_submit_adjust(cd, uid, gid,
			    (int64_t)(new_end - old_size), 0);
	return NFS4_OK;
}

/** Context passed from the COPY quota pre-check to the dispatch paths. */
struct copy_pre_ctx {
	uint64_t dst_old_size;
	uint64_t dst_uid;
	uint64_t dst_gid;
	bool     valid;
};

/**
 * Validate src (READ on saved FH) and dst (WRITE on current FH) stateids.
 * No-op when the open-owner table is absent.
 */
static enum nfs4_status copy_validate_stateids(struct compound_data *cd,
					       const struct nfs4_stateid *src,
					       const struct nfs4_stateid *dst,
					       uint64_t saved_fh)
{
	enum nfs4_status nst;

	if (cd->ot == NULL) {
		return NFS4_OK;
	}

	uint64_t save_fh = cd->current_fh.fileid;

	cd->current_fh.fileid = saved_fh;
	nst = validate_io_stateid(cd, src, OPEN4_SHARE_ACCESS_READ);
	cd->current_fh.fileid = save_fh;
	if (nst != NFS4_OK) {
		return nst;
	}
	return validate_io_stateid(cd, dst, OPEN4_SHARE_ACCESS_WRITE);
}

/**
 * Quota pre-check for COPY: read dst inode and cap the effective byte
 * count at the actual source bytes available so that COPY does not
 * over-reject when the source is shorter than @a count.
 */
static enum nfs4_status copy_quota_precheck(struct compound_data *cd,
					    uint64_t saved_fh,
					    uint64_t src_offset,
					    uint64_t dst_offset,
					    uint64_t count,
					    struct copy_pre_ctx *pre)
{
	struct mds_inode cp_pre;

	memset(pre, 0, sizeof(*pre));
	if (cd->quota == NULL) {
		return NFS4_OK;
	}
	if (cat_getattr(cd, cd->current_fh.fileid, &cp_pre) != MDS_OK) {
		return NFS4_OK;
	}
	pre->valid        = true;
	pre->dst_old_size = cp_pre.size;
	pre->dst_uid      = cp_pre.uid;
	pre->dst_gid      = cp_pre.gid;

	uint64_t eff_count = count;
	struct mds_inode src_pre;

	if (cat_getattr(cd, saved_fh, &src_pre) == MDS_OK &&
	    src_offset < src_pre.size) {
		uint64_t src_avail = src_pre.size - src_offset;
		if (src_avail < eff_count) {
			eff_count = src_avail;
		}
	}
	if (dst_offset + eff_count > pre->dst_old_size) {
		return quota_check_bytes(cd, pre->dst_uid, pre->dst_gid,
			dst_offset + eff_count - pre->dst_old_size);
	}
	return NFS4_OK;
}

/** COPY (RFC 7862 S15.2): server-side copy. */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_copy(struct compound_data *cd,
				const struct nfs4_op *op,
				struct nfs4_result *res)
{
	/* cppcheck-suppress unreadVariable
	 * The 'a' / 'r' bindings exist so the dead-but-kept body below
	 * still compiles -- see the TEMP comment under op_copy. */
	/* NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) */
	const struct nfs4_arg_copy *a = &op->arg.copy;
	/* cppcheck-suppress unreadVariable */
	/* NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) */
	struct nfs4_res_copy *r = &res->res.copy;
	enum nfs4_status nst;

	/*
	 * TEMP: server-side COPY is disabled.  mds_proxy_copy_data
	 * has a known silent-data-loss bug on small writes -- it
	 * returns the requested byte count without actually writing
	 * the bytes through to the destination DS, so smoke's
	 * cp+cat(small) test sees "size=19, content=''".  Returning
	 * NFS4ERR_NOTSUPP causes the Linux NFSv4.2 client to fall
	 * back to a regular read+write copy, which goes through the
	 * tested data path.  Re-enable when copy_offload + sync copy
	 * paths are fixed and covered by an integration test.
	 *
	 * The body below is intentionally left in place so the static
	 * helper functions (copy_quota_precheck, copy_validate_stateids,
	 * copy_update_dst_size) keep at least one compileable caller --
	 * stripping them would trip -Wunused-function under -Werror.
	 */
	return NFS4ERR_NOTSUPP;

	/* COPY uses saved FH as source, current FH as destination. */
	nst = require_saved_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
	}
	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* Promote both inline endpoints (dst=current, src=saved). */
	nst = promote_inline_pair(cd);
	if (nst != NFS4_OK) {
		return nst;
	}

	if (cd->proxy == NULL) {
		return NFS4ERR_NOTSUPP;
	}

	nst = copy_validate_stateids(cd, &a->src_stateid,
				     &a->dst_stateid, cd->saved_fh.fileid);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* CERT INT30-C: overflow check before arithmetic on offsets. */
	if (a->count > 0 && a->dst_offset > UINT64_MAX - a->count) {
		return NFS4ERR_INVAL;
	}

	struct copy_pre_ctx pre;

	nst = copy_quota_precheck(cd, cd->saved_fh.fileid, a->src_offset,
				  a->dst_offset, a->count, &pre);
	if (nst != NFS4_OK) {
		return nst;
	}

	if (!a->synchronous && cd->cot != NULL) {
		/* Asynchronous copy via offload tracker. */
		enum mds_status st;

		st = copy_offload_start(cd->cot, cd->proxy, cd->cat,
					cd->saved_fh.fileid, a->src_offset,
					cd->current_fh.fileid, a->dst_offset,
					a->count, &r->copy_stateid,
					cd->cq, pre.dst_uid, pre.dst_gid,
					pre.dst_old_size);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}

		r->write_count  = 0;
		r->committed    = UNSTABLE4;
		r->consecutive  = true;
		r->synchronous  = false;
		return NFS4_OK;
	}

	/* Synchronous copy via proxy. */
	{
		uint64_t copied = 0;
		enum mds_status st;

		st = mds_proxy_copy_data(cd->proxy, cd->cat,
					 cd->saved_fh.fileid, a->src_offset,
					 cd->current_fh.fileid, a->dst_offset,
					 a->count, &copied);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}

		r->write_count = copied;
		r->committed   = FILE_SYNC4;
		memset(r->verifier, 0, sizeof(r->verifier));
		memset(&r->copy_stateid, 0, sizeof(r->copy_stateid));
		r->consecutive = true;
		r->synchronous = true;

		/* Invalidate snapshot and update destination size.
		 * copy_update_dst_size re-reads the inode so it
		 * handles concurrent writers.  Quota accounting uses
		 * the pre-check snapshot when available; size update
		 * is unconditional. */
		if (copied > 0) {
			compound_inode_invalidate(cd,
				cd->current_fh.fileid);
			uint64_t cp_end = a->dst_offset + copied;
			uint64_t old_sz = pre.valid ? pre.dst_old_size : 0;
			uint64_t q_uid  = pre.valid ? pre.dst_uid : 0;
			uint64_t q_gid  = pre.valid ? pre.dst_gid : 0;

			/* Read actual pre-size if quota context unavailable. */
			if (!pre.valid) {
				struct mds_inode dst_cur;
				if (cat_getattr(cd,
					cd->current_fh.fileid,
					&dst_cur) == MDS_OK) {
					old_sz = dst_cur.size;
					q_uid = dst_cur.uid;
					q_gid = dst_cur.gid;
				}
			}
			if (cp_end > old_sz) {
				nst = copy_update_dst_size(
					cd, cd->current_fh.fileid, cp_end,
					old_sz, q_uid, q_gid);
				if (nst != NFS4_OK) {
					return nst;
				}
			}
		}
	}

	return NFS4_OK;
}

/** COPY_NOTIFY (RFC 7862 S15.3): prepare source for inter-server copy. */
enum nfs4_status op_copy_notify(const struct compound_data *cd,
				       const struct nfs4_op *op,
				       struct nfs4_result *res)
{
	struct nfs4_res_copy_notify *r = &res->res.copy_notify;
	enum nfs4_status nst;
	(void)op;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/*
	 * In the single-MDS model, inter-server copy is handled
	 * internally.  Return a stateid and lease_time so the
	 * client can proceed with COPY.
	 */
	memset(&r->stateid, 0, sizeof(r->stateid));
	r->stateid.seqid = 1;
	r->lease_time = 3600; /* 1 hour */

	return NFS4_OK;
}

/** OFFLOAD_CANCEL (RFC 7862 S15.8): cancel async copy. */
enum nfs4_status op_offload_cancel(struct compound_data *cd,
					  const struct nfs4_op *op,
					  struct nfs4_result *res)
{
	const struct nfs4_arg_offload_cancel *a = &op->arg.offload_cancel;
	enum nfs4_status nst;
	enum mds_status st;
	(void)res;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	if (cd->cot == NULL) {
		return NFS4ERR_NOTSUPP;
}

	st = copy_offload_cancel(cd->cot, &a->stateid);
	if (st == MDS_ERR_NOTFOUND) {
		return NFS4ERR_BAD_STATEID;
}
	return mds_status_to_nfs4(st);
}

/** OFFLOAD_STATUS (RFC 7862 S15.9): query async copy progress. */
enum nfs4_status op_offload_status(struct compound_data *cd,
					  const struct nfs4_op *op,
					  struct nfs4_result *res)
{
	const struct nfs4_arg_offload_status *a = &op->arg.offload_status;
	struct nfs4_res_offload_status *r = &res->res.offload_status;
	enum nfs4_status nst;
	enum mds_status st, job_err;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	if (cd->cot == NULL) {
		return NFS4ERR_NOTSUPP;
}

	st = copy_offload_status(cd->cot, &a->stateid,
				 &r->count, &r->complete, &job_err);
	if (st == MDS_ERR_NOTFOUND) {
		return NFS4ERR_BAD_STATEID;
}
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
}

	r->complete_status = r->complete ? mds_status_to_nfs4(job_err)
					  : NFS4_OK;
	return NFS4_OK;
}

/**
 * Quota pre-check for WRITE_SAME: reject if the write extends the
 * file beyond the user's byte quota limit.
 */
static enum nfs4_status ws_quota_precheck(struct compound_data *cd,
					  uint64_t offset,
					  uint64_t length)
{
	struct mds_inode ws_inode;

	if (cd->quota == NULL) {
		return NFS4_OK;
	}
	if (cat_getattr(cd, cd->current_fh.fileid, &ws_inode) != MDS_OK) {
		return NFS4_OK;
	}
	uint64_t ws_end = offset + length;

	if (ws_end > ws_inode.size) {
		return quota_check_bytes(cd, ws_inode.uid, ws_inode.gid,
					ws_end - ws_inode.size);
	}
	return NFS4_OK;
}

/** WRITE_SAME (RFC 7862 S15.12): write repeating pattern. */
enum nfs4_status op_write_same(struct compound_data *cd,
				      const struct nfs4_op *op,
				      struct nfs4_result *res)
{
	const struct nfs4_arg_write_same *a = &op->arg.write_same;
	enum nfs4_status nst;
	enum mds_status st;
	(void)res;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/* Inline files: promote to DS first. */
	nst = promote_if_inline(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	if (cd->proxy == NULL) {
		return NFS4ERR_NOTSUPP;
}

	if (a->data_len == 0) {
		return NFS4ERR_INVAL;
}

	/* Validate stateid. */
	nst = validate_io_stateid(cd, &a->stateid,
				  OPEN4_SHARE_ACCESS_WRITE);
	if (nst != NFS4_OK) {
		return nst;
}

	/* CERT INT30-C: overflow check. */
	if (a->length > 0 && a->offset > UINT64_MAX - a->length) {
		return NFS4ERR_INVAL;
	}

	/* Quota: check byte growth from write_same. */
	nst = ws_quota_precheck(cd, a->offset, a->length);
	if (nst != NFS4_OK) {
		return nst;
	}
	st = mds_proxy_write_same(cd->proxy, cd->cat, cd->current_fh.fileid,
				  a->offset, a->length,
				  a->data, a->data_len);
	if (st == MDS_OK) {
		compound_inode_invalidate(cd, cd->current_fh.fileid);
		struct mds_inode ws_post;
		if (cat_getattr(cd, cd->current_fh.fileid,
				   &ws_post) == MDS_OK) {
			uint64_t ws_end = a->offset + a->length;
			if (ws_end > ws_post.size) {
				nst = copy_update_dst_size(
					cd, cd->current_fh.fileid, ws_end,
					ws_post.size,
					ws_post.uid, ws_post.gid);
				if (nst != NFS4_OK) {
					return nst;
				}
			}
		}
	}
	return mds_status_to_nfs4(st);
}

/** CLONE (RFC 7862 S15.13): clone file data range. */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_clone(struct compound_data *cd,
				 const struct nfs4_op *op,
				 struct nfs4_result *res)
{
	/* cppcheck-suppress unreadVariable
	 * The 'a' binding exists so the dead-but-kept body below still
	 * compiles -- see the TEMP comment under op_clone. */
	/* NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) */
	const struct nfs4_arg_clone *a = &op->arg.clone;
	enum nfs4_status nst;
	enum mds_status st;
	struct mds_inode src_inode, dst_inode;
	(void)res;

	/*
	 * TEMP: server-side CLONE is disabled -- same root cause as
	 * op_copy (mds_proxy_copy_data silent-data-loss).  Returning
	 * NFS4ERR_NOTSUPP forces the Linux client to fall back to
	 * read+write copy.  Re-enable alongside op_copy.
	 */
	return NFS4ERR_NOTSUPP;

	/* CLONE uses saved FH as source, current FH as destination. */
	nst = require_saved_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	if (cd->proxy == NULL) {
		return NFS4ERR_NOTSUPP;
	}

	/* Both must be regular files. */
	st = cat_getattr(cd, cd->saved_fh.fileid, &src_inode);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
}
	st = cat_getattr(cd, cd->current_fh.fileid, &dst_inode);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
}
	if (src_inode.type != MDS_FTYPE_REG ||
	    dst_inode.type != MDS_FTYPE_REG) {
		return NFS4ERR_INVAL;
}

	/* Validate src stateid (READ on saved FH) and dst (WRITE on current). */
	if (cd->ot != NULL) {
		uint64_t save_fh = cd->current_fh.fileid;

		cd->current_fh.fileid = cd->saved_fh.fileid;
		nst = validate_io_stateid(cd, &a->src_stateid,
					  OPEN4_SHARE_ACCESS_READ);
		cd->current_fh.fileid = save_fh;
		if (nst != NFS4_OK) {
			return nst;
}
		nst = validate_io_stateid(cd, &a->dst_stateid,
					  OPEN4_SHARE_ACCESS_WRITE);
		if (nst != NFS4_OK) {
			return nst;
}
	}

	/* Promote both inline endpoints (dst=current, src=saved). */
	nst = promote_inline_pair(cd);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* Determine byte count. */
	{
		uint64_t count = a->count;

		if (count == 0) {
			if (a->src_offset >= src_inode.size) {
				return NFS4_OK; /* Nothing to clone. */
}
			count = src_inode.size - a->src_offset;
		}

		/* CERT INT30-C: overflow check. */
		if (count > 0 && a->dst_offset > UINT64_MAX - count) {
			return NFS4ERR_INVAL;
}

		/* Quota: check BEFORE mutation so we reject over-quota
		 * before mds_proxy_clone_range touches data. */
		uint64_t cl_old = dst_inode.size;
		if (a->dst_offset + count > cl_old) {
			enum nfs4_status cl_nst = quota_check_bytes(
				cd, dst_inode.uid, dst_inode.gid,
				(a->dst_offset + count) - cl_old);
			if (cl_nst != NFS4_OK) {
				return cl_nst;
			}
		}

		st = mds_proxy_clone_range(cd->proxy, cd->cat,
					   cd->saved_fh.fileid, a->src_offset,
					   cd->current_fh.fileid, a->dst_offset,
					   count);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}

		/* Update destination size if clone extended the file. */
		if (a->dst_offset + count > cl_old) {
			dst_inode.size = a->dst_offset + count;
			dst_inode.space_used = dst_inode.size;
			st = cat_setattr(cd, cd->current_fh.fileid,
					    &dst_inode, MDS_ATTR_SIZE);
			if (st != MDS_OK) {
				return mds_status_to_nfs4(st);
			}
			compound_inode_invalidate(cd,
				cd->current_fh.fileid);
			quota_submit_adjust(cd, dst_inode.uid, dst_inode.gid,
					    (int64_t)(dst_inode.size - cl_old), 0);
		}
	}

	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * RFC 8276 -- Extended Attribute Operations
 *
 * These four operations implement RFC 8276 extended attributes using
 * the catalogue vtable (RonDB backend).
 * ----------------------------------------------------------------------- */

/** GETXATTR (RFC 8276 S4.2.2): retrieve a single xattr value. */
enum nfs4_status op_getxattr(struct compound_data *cd,
			     const struct nfs4_op *op,
			     struct nfs4_result *res)
{
	const struct nfs4_arg_getxattr *a = &op->arg.getxattr;
	struct nfs4_res_getxattr *r = &res->res.getxattr;
	enum nfs4_status nst;
	enum mds_status st;
	void *val = NULL;
	uint32_t vallen = 0;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
	}

	if (cd->cat == NULL) {
		return NFS4ERR_NOTSUPP;
	}

	/* Phase B HPC-Shared trigger: synthesize the value rather than
	 * hitting the xattr table.  See include/hpc_shared.h. */
	if (hpc_xattr_name_match(a->name)) {
		uint8_t buf[2];
		uint32_t out_len = 0;
		st = hpc_shared_xattr_synthesize_value(
			cd, cd->current_fh.fileid, buf, sizeof(buf),
			&out_len);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}
		r->value_len = out_len;
		if (out_len > 0) {
			memcpy(r->value, buf, out_len);
		}
		return NFS4_OK;
	}

	st = cat_xattr_get(cd, cd->current_fh.fileid, a->name,
			   &val, &vallen);
	if (st == MDS_ERR_NOTFOUND) {
		return NFS4ERR_NOXATTR;
	}
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
	}

	if (vallen > MDS_XATTR_VAL_MAX) {
		free(val);
		return NFS4ERR_XATTR2BIG;
	}

	r->value_len = vallen;
	if (vallen > 0 && val != NULL) {
		memcpy(r->value, val, vallen);
	}
	free(val);
	return NFS4_OK;
}

/** SETXATTR (RFC 8276 S4.2.3): set an xattr with CREATE/REPLACE semantics. */
enum nfs4_status op_setxattr(struct compound_data *cd,
			     const struct nfs4_op *op,
			     struct nfs4_result *res)
{
	const struct nfs4_arg_setxattr *a = &op->arg.setxattr;
	struct nfs4_res_setxattr *r = &res->res.setxattr;
	enum nfs4_status nst;
	enum mds_status st;
	struct mds_inode inode;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
	}

	if (cd->cat == NULL) {
		return NFS4ERR_NOTSUPP;
	}

	if (a->value_len > MDS_XATTR_VAL_MAX) {
		return NFS4ERR_XATTR2BIG;
	}

	/* Read inode for change_info before mutation. */
	st = cat_getattr(cd, cd->current_fh.fileid, &inode);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
	}
	r->change_before = inode.change;

	/* Phase B HPC-Shared trigger: when the client sets the
	 * trusted.pnfs.hpc_shared control xattr, flip the inode flag
	 * instead of storing the bytes as a real xattr.  Honour the
	 * SETXATTR4_REPLACE pre-condition (the synthetic xattr always
	 * exists from the protocol's POV: it's a knob, not data). */
	if (hpc_xattr_name_match(a->name)) {
		enum nfs4_status hpc_st;

		if (a->option == SETXATTR4_CREATE) {
			/* The control xattr always logically exists. */
			return NFS4ERR_EXIST;
		}
		hpc_st = hpc_shared_xattr_apply(cd, a->value,
						a->value_len, false);
		if (hpc_st != NFS4_OK) {
			return hpc_st;
		}
		/* Refresh change counter for the response. */
		compound_inode_invalidate(cd, cd->current_fh.fileid);
		if (cat_getattr(cd, cd->current_fh.fileid,
				&inode) == MDS_OK) {
			r->change_after = inode.change;
		} else {
			r->change_after = r->change_before + 1;
		}
		return NFS4_OK;
	}

	/* Enforce CREATE / REPLACE semantics. */
	if (a->option == SETXATTR4_CREATE) {
		st = cat_xattr_exists(cd, cd->current_fh.fileid, a->name);
		if (st == MDS_OK) {
			return NFS4ERR_EXIST;
		}
	} else if (a->option == SETXATTR4_REPLACE) {
		st = cat_xattr_exists(cd, cd->current_fh.fileid, a->name);
		if (st != MDS_OK) {
			return NFS4ERR_NOXATTR;
		}
	}
	/* SETXATTR4_EITHER: no pre-check needed. */

	st = cat_xattr_put(cd, cd->current_fh.fileid, a->name,
			   a->value, a->value_len);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
	}

	/* Bump inode change counter via touch. */
	compound_inode_invalidate(cd, cd->current_fh.fileid);
	st = cat_getattr(cd, cd->current_fh.fileid, &inode);
	if (st == MDS_OK) {
		r->change_after = inode.change;
	} else {
		r->change_after = r->change_before + 1;
	}

	return NFS4_OK;
}

/** Context for LISTXATTRS callback. */
struct listxattrs_ctx {
	struct nfs4_res_listxattrs *res;
	uint64_t skip;      /**< Names to skip (cookie). */
	uint64_t index;     /**< Current iteration index. */
	uint32_t maxcount;  /**< Max response bytes remaining. */
};

static int listxattrs_cb(const char *name, size_t name_len, void *arg)
{
	struct listxattrs_ctx *ctx = arg;
	struct nfs4_res_listxattrs *r = ctx->res;

	/* Skip entries before the cookie. */
	if (ctx->index < ctx->skip) {
		ctx->index++;
		return 0;
	}

	/* Stop if we've hit the array limit. */
	if (r->name_count >= NFS4_LISTXATTRS_MAX) {
		return 1; /* stop iteration */
	}

	/* Check maxcount: 4 bytes length + name bytes + XDR pad. */
	{
		uint32_t wire_len = 4 + (uint32_t)name_len;
		uint32_t pad = (4 - (name_len & 3)) & 3;

		wire_len += pad;
		if (wire_len > ctx->maxcount) {
			return 1; /* stop */
		}
		ctx->maxcount -= wire_len;
	}

	{
		uint32_t copy_len = (name_len <= MDS_XATTR_NAME_MAX)
				    ? (uint32_t)name_len
				    : MDS_XATTR_NAME_MAX;

		memcpy(r->names[r->name_count], name, copy_len);
		r->names[r->name_count][copy_len] = '\0';
	}
	r->name_count++;
	ctx->index++;
	return 0;
}

/** LISTXATTRS (RFC 8276 S4.2.4): list xattr names with pagination. */
enum nfs4_status op_listxattrs(struct compound_data *cd,
			       const struct nfs4_op *op,
			       struct nfs4_result *res)
{
	const struct nfs4_arg_listxattrs *a = &op->arg.listxattrs;
	struct nfs4_res_listxattrs *r = &res->res.listxattrs;
	enum nfs4_status nst;
	enum mds_status st;
	struct listxattrs_ctx ctx;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
	}

	if (cd->cat == NULL) {
		return NFS4ERR_NOTSUPP;
	}

	memset(r, 0, sizeof(*r));
	ctx.res      = r;
	ctx.skip     = a->cookie;
	ctx.index    = 0;
	ctx.maxcount = a->maxcount;

	st = cat_xattr_list(cd, cd->current_fh.fileid,
			    listxattrs_cb, &ctx);
	if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
		return mds_status_to_nfs4(st);
	}

	/*
	 * If the callback stopped early (array full or maxcount hit),
	 * more names remain.  Otherwise we reached EOF.
	 */
	if (r->name_count >= NFS4_LISTXATTRS_MAX ||
	    (ctx.maxcount == 0 && r->name_count > 0)) {
		r->eof = false;
	} else {
		r->eof = true;
	}
	r->cookie = ctx.skip + r->name_count;

	return NFS4_OK;
}

/** REMOVEXATTR (RFC 8276 S4.2.5): delete an xattr by name. */
enum nfs4_status op_removexattr(struct compound_data *cd,
				const struct nfs4_op *op,
				struct nfs4_result *res)
{
	const struct nfs4_arg_removexattr *a = &op->arg.removexattr;
	struct nfs4_res_removexattr *r = &res->res.removexattr;
	enum nfs4_status nst;
	enum mds_status st;
	struct mds_inode inode;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
	}

	if (cd->cat == NULL) {
		return NFS4ERR_NOTSUPP;
	}

	/* Read inode for change_info before mutation. */
	st = cat_getattr(cd, cd->current_fh.fileid, &inode);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
	}
	r->change_before = inode.change;

	/* Phase B HPC-Shared trigger: REMOVEXATTR clears the inode flag
	 * without touching the xattr table. */
	if (hpc_xattr_name_match(a->name)) {
		enum nfs4_status hpc_st =
			hpc_shared_xattr_apply(cd, NULL, 0, true);
		if (hpc_st != NFS4_OK) {
			return hpc_st;
		}
		compound_inode_invalidate(cd, cd->current_fh.fileid);
		if (cat_getattr(cd, cd->current_fh.fileid,
				&inode) == MDS_OK) {
			r->change_after = inode.change;
		} else {
			r->change_after = r->change_before + 1;
		}
		return NFS4_OK;
	}

	st = cat_xattr_del(cd, cd->current_fh.fileid, a->name);
	if (st == MDS_ERR_NOTFOUND) {
		return NFS4ERR_NOXATTR;
	}
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
	}

	/* Bump inode change counter. */
	compound_inode_invalidate(cd, cd->current_fh.fileid);
	st = cat_getattr(cd, cd->current_fh.fileid, &inode);
	if (st == MDS_OK) {
		r->change_after = inode.change;
	} else {
		r->change_after = r->change_before + 1;
	}

	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * RECLAIM_COMPLETE (RFC 8881 S18.51)
 *
 * Signals that the client has finished reclaiming all previously held
 * state.  The grace module tracks per-client completion; once all
 * tracked clients have sent RECLAIM_COMPLETE, grace period ends.
 * ----------------------------------------------------------------------- */


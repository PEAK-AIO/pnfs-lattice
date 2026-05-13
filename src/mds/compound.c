/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * compound.c -- NFSv4.1/4.2 COMPOUND request dispatcher.
 *
 * Dispatches an array of NFSv4.1/4.2 operations sequentially,
 * maintaining the current/saved file handle state.  Stops on
 * the first error (RFC 8881 S2.6.3.1.1.4).
 *
 * Handlers call into the namespace layer (namespace.h) for all
 * metadata operations.
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "mds_catalogue.h"
#include "lock_state.h"
#include "compound_internal.h"
#include "commit_queue.h"
#include "layout_ds_ids.h"
#include "grace.h"
#include "health.h"
#include "quota.h"
#include "subtree_map.h"
#include "ds_prepare.h"
#include "mds_shard.h"
#include "inode_cache.h"
#include "dirent_cache.h"
#include "delegation.h"
#include "dir_delegation.h"
#include "xdr_codec.h"
#include "mds_metrics.h"
#include "mds_op_metrics.h"

/* Internal counter helper, defined in dir_delegation.c. */
void dir_deleg_count_conflict_unavail(struct dir_deleg_table *ddt);

/* -----------------------------------------------------------------------
 * RFC 8881 S16.2.4 -- current-stateid helpers.
 *
 * The wire form of the special CURRENT_STATEID4 marker is
 * seqid==1 with other==all-zeros (S16.2.3.1.2).  Operations that
 * consume a stateid call compound_resolve_stateid() before validating
 * it; producing operations call compound_set_current_stateid() in
 * compound_process()'s post-dispatch state machine.  See
 * compound_internal.h for the full contract.
 * ----------------------------------------------------------------------- */

bool compound_is_current_stateid(const struct nfs4_stateid *sid)
{
	static const uint8_t zero_other[NFS4_OTHER_SIZE] = {0};

	if (sid == NULL) {
		return false;
	}
	return sid->seqid == 1u &&
	       memcmp(sid->other, zero_other, NFS4_OTHER_SIZE) == 0;
}

enum nfs4_status compound_resolve_stateid(
	const struct compound_data *cd,
	const struct nfs4_stateid *in,
	struct nfs4_stateid *out)
{
	if (cd == NULL || in == NULL || out == NULL) {
		return NFS4ERR_BAD_STATEID;
	}
	if (compound_is_current_stateid(in)) {
		if (!cd->current_stateid_set) {
			return NFS4ERR_BAD_STATEID;
		}
		*out = cd->current_stateid;
		return NFS4_OK;
	}
	*out = *in;
	return NFS4_OK;
}

void compound_set_current_stateid(struct compound_data *cd,
				  const struct nfs4_stateid *sid)
{
	if (cd == NULL || sid == NULL) {
		return;
	}
	cd->current_stateid = *sid;
	cd->current_stateid_set = true;
}

void compound_invalidate_current_stateid(struct compound_data *cd)
{
	if (cd == NULL) {
		return;
	}
	memset(&cd->current_stateid, 0, sizeof(cd->current_stateid));
	cd->current_stateid_set = false;
}

void compound_save_current_stateid(struct compound_data *cd)
{
	if (cd == NULL) {
		return;
	}
	cd->saved_stateid = cd->current_stateid;
	cd->saved_stateid_set = cd->current_stateid_set;
}

void compound_restore_current_stateid(struct compound_data *cd)
{
	if (cd == NULL) {
		return;
	}
	cd->current_stateid = cd->saved_stateid;
	cd->current_stateid_set = cd->saved_stateid_set;
}

/*
 * Post-dispatch state-machine for current_stateid.  Called from
 * compound_process() after each op that returned NFS4_OK.
 *
 * Producers update current_stateid from the result slot.  Invalidators
 * (FH-changing ops) clear it.  SAVEFH/RESTOREFH save/restore it
 * alongside the current FH.  Ops that neither produce nor invalidate
 * are no-ops here.
 */
static void compound_update_current_stateid_post(
	struct compound_data *cd,
	const struct nfs4_op *op,
	const struct nfs4_result *res)
{
	if (cd == NULL || op == NULL || res == NULL) {
		return;
	}
	switch (op->opnum) {
	case OP_OPEN:
		compound_set_current_stateid(cd, &res->res.open.stateid);
		break;
	case OP_OPEN_DOWNGRADE:
	case OP_CLOSE:
		/* op_close and op_open_downgrade share the close result
		 * union slot for their output stateid. */
		compound_set_current_stateid(cd, &res->res.close.stateid);
		break;
	case OP_LOCK:
		compound_set_current_stateid(cd, &res->res.lock.stateid);
		break;
	case OP_LOCKU:
		compound_set_current_stateid(cd, &res->res.locku.stateid);
		break;
	case OP_LAYOUTGET:
		compound_set_current_stateid(cd, &res->res.layoutget.stateid);
		break;
	case OP_LAYOUTRETURN:
		if (res->res.layoutreturn.stateid_present) {
			compound_set_current_stateid(
				cd, &res->res.layoutreturn.stateid);
		}
		break;
	case OP_PUTFH:
	case OP_PUTROOTFH:
	case OP_LOOKUP:
	case OP_LOOKUPP:
		compound_invalidate_current_stateid(cd);
		break;
	case OP_SAVEFH:
		compound_save_current_stateid(cd);
		break;
	case OP_RESTOREFH:
		compound_restore_current_stateid(cd);
		break;
	default:
		break;
	}
}

/* -----------------------------------------------------------------------
 * RFC 8881 S1.7 / S14.4 -- UTF-8 (utf8str_cs) well-formedness validator.
 *
 * RFC 8881 S1.7 mandates utf8str_cs is the "Net-Unicode" form defined by
 * RFC 5198 S3, which builds on RFC 3629 UTF-8 with two extra constraints
 * relevant here:
 *   - the byte sequence must be a well-formed UTF-8 encoding (RFC 3629);
 *   - the encoded codepoints must NOT include Unicode "noncharacters".
 *
 * This implementation rejects:
 *   - isolated continuation bytes (0x80-0xBF as a lead).
 *   - overlong encodings (0xC0/0xC1 leads, E0 80-9F, F0 80-8F).
 *   - UTF-16 surrogate halves (ED A0-BF).
 *   - codepoints above U+10FFFF (F4 90-BF, plus all F5-FF leads).
 *   - truncated multi-byte sequences.
 *   - noncharacter codepoints: U+FDD0..U+FDEF and U+xxFFFE / U+xxFFFF
 *     (for plane xx in 0..0x10) per The Unicode Standard S2.4 and
 *     RFC 5198 S3, which pynfs RNM8/9 and COMP3 exercise via
 *     get_invalid_utf8strings() (e.g. \xEF\xBF\xBE = U+FFFE).
 *
 * Embedded NUL is treated as valid UTF-8 here; callers that forbid it
 * (component4 names, compound tag) check that separately.
 */
static inline bool utf8_is_noncharacter(uint32_t cp)
{
	/* Per-plane noncharacters: low 16 bits == FFFE or FFFF. */
	uint32_t low = cp & 0xFFFFu;

	if (low == 0xFFFEu || low == 0xFFFFu) {
		return true;
	}
	/* BMP-only noncharacter range. */
	if (cp >= 0xFDD0u && cp <= 0xFDEFu) {
		return true;
	}
	return false;
}

bool compound_is_valid_utf8(const char *buf, size_t len)
{
	const uint8_t *p;
	size_t i;

	if (buf == NULL) {
		return (len == 0);
	}
	p = (const uint8_t *)buf;
	for (i = 0; i < len; ) {
		uint8_t c = p[i];
		uint8_t c1, c2, c3;
		uint32_t cp;

		if (c < 0x80u) {
			i++;
			continue;
		}
		if (c < 0xC2u) {
			return false;
		}
		if (c < 0xE0u) {
			if (i + 1 >= len) {
				return false;
			}
			c1 = p[i + 1];
			if (c1 < 0x80u || c1 > 0xBFu) {
				return false;
			}
			/* 2-byte form encodes U+0080..U+07FF: no noncharacters
			 * live in this range, no further check needed. */
			i += 2;
			continue;
		}
		if (c < 0xF0u) {
			if (i + 2 >= len) {
				return false;
			}
			c1 = p[i + 1];
			c2 = p[i + 2];
			if (c == 0xE0u) {
				if (c1 < 0xA0u || c1 > 0xBFu) {
					return false;
				}
			} else if (c == 0xEDu) {
				if (c1 < 0x80u || c1 > 0x9Fu) {
					return false;
				}
			} else {
				if (c1 < 0x80u || c1 > 0xBFu) {
					return false;
				}
			}
			if (c2 < 0x80u || c2 > 0xBFu) {
				return false;
			}
			cp = ((uint32_t)(c & 0x0Fu) << 12) |
			     ((uint32_t)(c1 & 0x3Fu) << 6) |
			     (uint32_t)(c2 & 0x3Fu);
			if (utf8_is_noncharacter(cp)) {
				return false;
			}
			i += 3;
			continue;
		}
		if (c >= 0xF5u) {
			return false;
		}
		if (i + 3 >= len) {
			return false;
		}
		c1 = p[i + 1];
		c2 = p[i + 2];
		c3 = p[i + 3];
		if (c == 0xF0u) {
			if (c1 < 0x90u || c1 > 0xBFu) {
				return false;
			}
		} else if (c == 0xF4u) {
			if (c1 < 0x80u || c1 > 0x8Fu) {
				return false;
			}
		} else {
			if (c1 < 0x80u || c1 > 0xBFu) {
				return false;
			}
		}
		if (c2 < 0x80u || c2 > 0xBFu) {
			return false;
		}
		if (c3 < 0x80u || c3 > 0xBFu) {
			return false;
		}
		cp = ((uint32_t)(c & 0x07u) << 18) |
		     ((uint32_t)(c1 & 0x3Fu) << 12) |
		     ((uint32_t)(c2 & 0x3Fu) << 6) |
		     (uint32_t)(c3 & 0x3Fu);
		if (utf8_is_noncharacter(cp)) {
			return false;
		}
		i += 4;
	}
	return true;
}

enum nfs4_status compound_validate_name(const char *name)
{
	size_t i;
	size_t len;

	if (name == NULL || name[0] == '\0') {
		return NFS4ERR_INVAL;
	}
	if (name[0] == '.' &&
	    (name[1] == '\0' ||
	     (name[1] == '.' && name[2] == '\0'))) {
		return NFS4ERR_BADNAME;
	}
	len = strlen(name);
	for (i = 0; i < len; i++) {
		if (name[i] == '/' || name[i] == '\0') {
			return NFS4ERR_INVAL;
		}
	}
	if (!compound_is_valid_utf8(name, len)) {
		return NFS4ERR_INVAL;
	}
	return NFS4_OK;
}

/*
 * op_get_dir_delegation -- RFC 8881 S18.39 dispatch handler.
 *
 * Phase 8b: when cd->ddt is NULL (feature flag off) we preserve the
 * Phase 8a UNAVAIL behaviour.  When ddt is present and no other
 * client already holds a delegation on this directory, grant a real
 * delegation: allocate a stateid, a cookieverf, and echo the
 * supported notification subset (which in 8b is empty -- the client
 * falls back to CB_RECALL semantics on any mutation).
 *
 * The client's session is not wired into the grant yet because
 * `struct compound_data` does not carry the session pointer at this
 * layer; Phase 8c adds the session pointer and the CB_NOTIFY path
 * that needs it.  CB_RECALL still works because deleg_recall_dir
 * accepts a NULL session (silent revoke).
 */
void compound_recall_dir_delegations(struct compound_data *cd,
				     uint64_t dir_fileid)
{
	if (cd == NULL || cd->ddt == NULL) {
		return;
	}
	/*
	 * Best-effort CB_RECALL + local revoke.  We do not block on
	 * the mutation if the recall times out or fails; the state
	 * table already revoked locally so the holding client will
	 * see BAD_STATEID on its next use and re-issue GDD.
	 */
	(void)dir_deleg_recall_dir(cd->ddt, dir_fileid, cd->clientid, 0);
}

void compound_notify_or_recall_dir(struct compound_data *cd,
				   uint64_t dir_fileid,
				   uint32_t event,
				   const char *old_name,
				   const char *new_name)
{
	if (cd == NULL || cd->ddt == NULL) {
		return;
	}
	(void)dir_deleg_notify_dir(cd->ddt, dir_fileid, cd->clientid,
				   event, old_name, new_name, 0);
}

enum nfs4_status op_get_dir_delegation(struct compound_data *cd,
				       const struct nfs4_op *op,
				       struct nfs4_result *res)
{
	const struct nfs4_arg_get_dir_delegation *a =
		&op->arg.get_dir_delegation;
	struct nfs4_res_get_dir_delegation *r = &res->res.get_dir_delegation;
	enum nfs4_status nst;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
	}

	memset(r, 0, sizeof(*r));

	/*
	 * Feature flag off -- signal "cannot grant" via the inner
	 * gddrnf_status = GDD4_UNAVAIL, keeping the outer op status
	 * NFS4_OK so the rest of the client's compound (typically a
	 * trailing GETATTR) still executes.  Returning
	 * NFS4ERR_DIRDELEG_UNAVAIL at the outer status halts the
	 * compound and produces EIO on the client -- the regression
	 * this path is fixing.
	 */
	if (cd->ddt == NULL) {
		r->gddrnf_status = GDD4_UNAVAIL;
		r->will_signal_deleg_avail = false;
		return NFS4_OK;
	}

	/*
	 * Conflict gate: another client already holds a delegation on
	 * this dir.  Same outer-OK, inner-UNAVAIL shape.
	 */
	if (dir_deleg_is_writer_present(cd->ddt,
					cd->current_fh.fileid,
					cd->clientid)) {
		dir_deleg_count_conflict_unavail(cd->ddt);
		r->gddrnf_status = GDD4_UNAVAIL;
		r->will_signal_deleg_avail = false;
		return NFS4_OK;
	}

	/*
	 * Phase 8c supported notification mask: three structural
	 * events (REMOVE/ADD/RENAME).  CHANGE_CHILD_ATTRS and
	 * CHANGE_DIR_ATTRS carry fattr4 payloads and land in Phase 8d.
	 * We intersect with the client's request -- never advertise
	 * bits the client did not ask for (RFC 8881 S18.39.2).
	 */
	const uint32_t supported = ((uint32_t)1u << NOTIFY4_REMOVE_ENTRY) |
				   ((uint32_t)1u << NOTIFY4_ADD_ENTRY) |
				   ((uint32_t)1u << NOTIFY4_RENAME_ENTRY);
	uint32_t requested = (a->notification_types_words > 0) ?
		a->notification_types[0] : 0;
	uint32_t granted_mask = requested & supported;

	if (dir_deleg_grant(cd->ddt, cd->clientid,
			    cd->current_fh.fileid, granted_mask,
			    NULL /* session wired in Phase 8d */,
			    &r->stateid, r->cookieverf) != 0) {
		return NFS4ERR_RESOURCE;
	}

	r->gddrnf_status = GDD4_OK;

	/*
	 * Echo the granted notification bitmap.  Child/dir attribute
	 * bitmaps stay empty in 8c -- Phase 8d will negotiate those when
	 * CHANGE_*_ATTRS events are supported.
	 */
	if (granted_mask != 0) {
		r->notification[0] = granted_mask;
		r->notification_words = 1;
	} else {
		r->notification_words = 0;
	}
	r->child_attributes_words = 0;
	r->dir_attributes_words = 0;
	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * Cross-cutting helpers (used by per-module op handlers)
 * ----------------------------------------------------------------------- */

/* -----------------------------------------------------------------------
 * Namespace read helpers
 * ----------------------------------------------------------------------- */

/**
 * Inode read via catalogue (RonDB backend).
 *
 * Checks the global inode cache first; on miss dispatches through
 * the catalogue vtable.  Populates the cache on read-through.
 *
 * Phase 3 of the QA plan: any inode carrying MDS_IFLAG_HPC_CREATE_PENDING
 * is treated as not-yet-visible to NFS clients.  The flag is set during
 * the wide-CREATE atomic window in hpc_shared_create_wide_layout and
 * cleared after the stripe map row commits.  We filter the flag here
 * (the central NFS-facing inode read) and refuse to populate the inode
 * cache with a PENDING entry, so a stale cache hit cannot resurrect
 * the file once the create completes or is rolled back.  Catalogue
 * primitives invoked directly (mds_cat_ns_getattr, mds_cat_ns_setattr's
 * internal read) bypass this helper and remain free to see the orphan
 * for cleanup.
 */
static enum mds_status compound_cat_inode_get(
	struct compound_data *cd, uint64_t fileid,
	struct mds_inode *out)
{
	enum mds_status st;

	/* Check global inode cache first. */
	if (cd->icache != NULL &&
	    inode_cache_get(cd->icache, fileid, out) == 0) {
		if (out->flags & MDS_IFLAG_HPC_CREATE_PENDING) {
			/* Defensive: a PENDING entry must never have entered
			 * the cache via this helper, but evict it on the
			 * off chance it did (e.g. a future caller bypassed
			 * the filter) and fall through to the backend. */
			inode_cache_invalidate(cd->icache, fileid);
		} else {
			return MDS_OK;
		}
	}

	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}
	st = mds_cat_ns_getattr(cd->cat, fileid, out);
	if (st != MDS_OK) {
		return st;
	}

	/* Phase 3 visibility filter: HPC-Shared wide CREATE atomic window. */
	if (out->flags & MDS_IFLAG_HPC_CREATE_PENDING) {
		return MDS_ERR_NOTFOUND;
	}

	/* Populate cache on miss. */
	if (cd->icache != NULL) {
		inode_cache_put(cd->icache, out);
	}
	return MDS_OK;
}

/**
 * Dirent read via catalogue (RonDB backend).
 * Checks the dirent cache first; populates on miss.
 * Caches negative (NOTFOUND) results with TTL.
 */
static enum mds_status compound_cat_dirent_get(
	struct compound_data *cd,
	uint64_t parent_fileid, const char *name,
	uint64_t *child_fileid, uint8_t *type)
{
	struct mds_inode child;
	enum mds_status st;

	/* Check dirent cache first. */
	if (cd->dcache != NULL) {
		int dc_rc = dirent_cache_get(cd->dcache, parent_fileid,
					     name, child_fileid, type);
		if (dc_rc == 0) {
			return MDS_OK; /* positive hit */
		}
		if (dc_rc == 1) {
			return MDS_ERR_NOTFOUND; /* negative hit */
		}
		/* dc_rc == -1: miss -- fall through to backend */
	}

	if (cd->cat == NULL) {
		return MDS_ERR_INVAL;
	}

	st = mds_cat_ns_lookup(cd->cat, parent_fileid, name, &child);
	if (st == MDS_OK) {
		*child_fileid = child.fileid;
		*type = (uint8_t)child.type;
		if (cd->dcache != NULL) {
			dirent_cache_put(cd->dcache, parent_fileid,
					 name, child.fileid,
					 (uint8_t)child.type);
		}
		return MDS_OK;
	}
	if (st == MDS_ERR_NOTFOUND && cd->dcache != NULL) {
		dirent_cache_put_negative(cd->dcache,
					  parent_fileid, name);
	}
	return st;
}

/* -----------------------------------------------------------------------
 * Request-local inode snapshot helpers
 * ----------------------------------------------------------------------- */

enum mds_status compound_inode_get(struct compound_data *cd,
				   uint64_t fileid,
				   struct mds_inode *out)
{
	enum mds_status st;

	if (cd == NULL || out == NULL) {
		return MDS_ERR_INVAL;
	}

	/* Check current_inode snapshot. */
	if (cd->current_inode_valid &&
	    cd->current_fh_set &&
	    cd->current_fh.fileid == fileid &&
	    cd->current_inode.fileid == fileid) {
		*out = cd->current_inode;
		return MDS_OK;
	}

	/* Check saved_inode snapshot. */
	if (cd->saved_inode_valid &&
	    cd->saved_fh_set &&
	    cd->saved_fh.fileid == fileid &&
	    cd->saved_inode.fileid == fileid) {
		*out = cd->saved_inode;
		return MDS_OK;
	}

	/* Miss -- read from the active namespace backend. */
	st = compound_cat_inode_get(cd, fileid, out);
	if (st != MDS_OK) {
		return st;
	}

	/* Seed current_inode if the fileid matches current_fh. */
	if (cd->current_fh_set && cd->current_fh.fileid == fileid) {
		cd->current_inode = *out;
		cd->current_inode_valid = true;
	}

	return MDS_OK;
}

void compound_inode_invalidate(struct compound_data *cd, uint64_t fileid)
{
	if (cd == NULL) {
		return;
	}
	if (cd->current_inode_valid &&
	    cd->current_fh_set &&
	    cd->current_fh.fileid == fileid) {
		cd->current_inode_valid = false;
	}
	if (cd->saved_inode_valid &&
	    cd->saved_fh_set &&
	    cd->saved_fh.fileid == fileid) {
		cd->saved_inode_valid = false;
	}
	/* Evict from global cache so next read gets fresh data. */
	if (cd->icache != NULL) {
		inode_cache_invalidate(cd->icache, fileid);
	}
	compound_ro_txn_reset(cd);
}

/**
 * Invalidate a specific dirent entry in the global dirent cache.
 * Called by mutation handlers (CREATE, REMOVE, RENAME, LINK) after
 * the namespace operation succeeds.
 */
void compound_dirent_invalidate(struct compound_data *cd,
				uint64_t parent_fileid, const char *name)
{
	if (cd == NULL || cd->dcache == NULL || name == NULL) {
		return;
	}
	dirent_cache_invalidate(cd->dcache, parent_fileid, name);
}

void compound_ro_txn_reset(struct compound_data *cd)
{
	/* No-op: retained as a call target for mutation handlers. */
	(void)cd;
}

enum mds_status compound_lookup_local_child(
	struct compound_data *cd,
	uint64_t parent_fileid,
	const char *name,
	struct mds_inode *child)
{
	struct mds_inode parent;
	enum mds_status st;

	if (cd == NULL || (cd->cat == NULL) ||
	    name == NULL || child == NULL) {
		return MDS_ERR_INVAL;
	}

	/* Parent check via snapshot (free on hit). */
	st = compound_inode_get(cd, parent_fileid, &parent);
	if (st != MDS_OK) {
		return st;
	}
	if (parent.type != MDS_FTYPE_DIR) {
		return MDS_ERR_NOTDIR;
	}

	/* Fast path: check dirent cache -> inode cache.
	 * Both caches are populated by prior lookups; repeated
	 * stat/read on the same files hits memory, not NDB. */
	{
		uint64_t cached_fid = 0;
		uint8_t cached_type = 0;
		bool dirent_hit = false;

		if (cd->dcache != NULL) {
			int dc_rc = dirent_cache_get(cd->dcache,
						     parent_fileid, name,
						     &cached_fid,
						     &cached_type);
			if (dc_rc == 1) {
				return MDS_ERR_NOTFOUND; /* negative hit */
			}
			dirent_hit = (dc_rc == 0);
		}

		/* If dirent cache hit, try inode cache. */
		if (dirent_hit && cd->icache != NULL &&
		    inode_cache_get(cd->icache, cached_fid, child) == 0) {
			/* Phase 3 visibility filter: a PENDING entry must
			 * never have entered the icache via the slow path
			 * below, but evict defensively and fall through to
			 * the backend so the filter runs there too. */
			if (child->flags & MDS_IFLAG_HPC_CREATE_PENDING) {
				inode_cache_invalidate(cd->icache,
						       cached_fid);
			} else {
				return MDS_OK; /* both caches hit -- zero NDB */
			}
		}
	}

	/* Cache miss -- fused dirent + inode read via cat_lookup.
	 * Single NDB transaction instead of two separate reads. */
	st = cat_lookup(cd, parent_fileid, name, child);
	if (st == MDS_ERR_NOTFOUND && cd->dcache != NULL) {
		dirent_cache_put_negative(cd->dcache, parent_fileid, name);
	}
	if (st != MDS_OK) {
		return st;
	}

	/* Phase 3 visibility filter: hide HPC-Shared wide CREATEs whose
	 * stripe map has not yet been persisted.  Do not populate the
	 * dirent or inode caches with the PENDING inode -- either the
	 * create is still in flight (the next read will see the flag
	 * cleared and cache normally) or the MDS crashed mid-create and
	 * the orphan must remain invisible until reaped. */
	if (child->flags & MDS_IFLAG_HPC_CREATE_PENDING) {
		return MDS_ERR_NOTFOUND;
	}

	/* Populate both caches on miss. */
	if (cd->dcache != NULL) {
		dirent_cache_put(cd->dcache, parent_fileid, name,
				 child->fileid, (uint8_t)child->type);
	}
	if (cd->icache != NULL) {
		inode_cache_put(cd->icache, child);
	}
	return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Quota enforcement helpers
 * ----------------------------------------------------------------------- */

/**
 * Submit a COMMIT_OP_QUOTA_ADJUST via CQ.
 * For non-CQ (test) path, this is a no-op -- quota accounting is
 * handled only by the writer thread.
 */
void quota_submit_adjust(struct compound_data *cd,
				uint64_t uid, uint64_t gid,
				int64_t delta_bytes, int32_t delta_inodes)
{
	if (cd->quota == NULL || cd->cq == NULL) {
		return;
	}
	struct commit_op cop;
	memset(&cop, 0, sizeof(cop));
	cop.type = COMMIT_OP_QUOTA_ADJUST;
	cop.args.quota_adjust.uid = uid;
	cop.args.quota_adjust.gid = gid;
	cop.args.quota_adjust.delta_bytes = delta_bytes;
	cop.args.quota_adjust.delta_inodes = delta_inodes;
	/* Return value intentionally unchecked: the mutation op
	 * that precedes this was already committed via CQ.  A
	 * QUOTA_ADJUST failure means usage drift, which self-
	 * corrects on the next admin quota recalc.  Blocking the
	 * client here cannot undo the already-committed mutation. */
	(void)commit_queue_submit(cd->cq, &cop);
}

void compound_maybe_enqueue_ds_prepare(struct compound_data *cd,
				       const struct mds_inode *inode)
{
	struct mds_ds_map_entry *entries = NULL;
	uint32_t stripe_count = 0;
	uint32_t stripe_unit = 0;
	uint32_t mirror_count = 0;
	uint32_t total;
	uint32_t i;
	enum mds_status st;

	if (cd == NULL || inode == NULL || cd->ds_prepare == NULL ||
	    cd->cat == NULL || !(inode->flags & MDS_IFLAG_DS_PENDING)) {
		return;
	}

	st = cat_stripe_map_get(cd, inode->fileid,
				     &stripe_count, &stripe_unit,
				     &mirror_count, &entries);
	(void)stripe_unit;
	if (st != MDS_OK || entries == NULL || stripe_count == 0 ||
	    mirror_count == 0) {
		free(entries);
		return;
	}

	total = stripe_count * mirror_count;
	for (i = 0; i < total; i++) {
		uint32_t stripe = i / mirror_count;
		uint32_t mirror = i % mirror_count;

		if (entries[i].nfs_fh_len > 0) {
			continue;
		}

		(void)ds_prepare_enqueue(cd->ds_prepare, cd->cat,
					 inode->fileid, inode->generation,
					 entries[i].ds_id, stripe, mirror);
	}

	free(entries);
}

int compound_ds_prepare_check(const struct compound_data *cd,
			      uint64_t fileid)
{
	if (cd == NULL || cd->ds_prepare == NULL) {
		return -1;
	}

	return ds_prepare_check(cd->ds_prepare, fileid);
}

enum mds_status compound_create_tracked_object(
	struct compound_data *cd,
	uint64_t parent_fileid,
	const char *name,
	enum mds_file_type type,
	uint32_t mode,
	uint64_t uid,
	uint64_t gid,
	struct ds_prealloc_ctx *prealloc,
	struct mds_inode *out)
{
	return cat_create(cd, parent_fileid, name, type,
			  mode, uid, gid, prealloc, out);
}

enum mds_status compound_remove_tracked_object(
	struct compound_data *cd,
	uint64_t parent_fileid,
	const char *name)
{
	return cat_remove(cd, parent_fileid, name);
}

/**
 * Check inode quota before inode-creating operations.
 * Returns NFS4_OK or NFS4ERR_NOSPC.
 */
enum nfs4_status quota_check_inode(struct compound_data *cd,
					  uint64_t uid, uint64_t gid)
{
	if (cd->quota == NULL) {
		return NFS4_OK;
	}
	if (mds_quota_check_inode(cd->quota, uid, gid) != MDS_OK) {
		return NFS4ERR_NOSPC;
	}
	return NFS4_OK;
}

/**
 * Check byte quota before byte-increasing operations.
 * Returns NFS4_OK or NFS4ERR_NOSPC.
 */
enum nfs4_status quota_check_bytes(struct compound_data *cd,
					  uint64_t uid, uint64_t gid,
					  uint64_t additional_bytes)
{
	if (cd->quota == NULL) {
		return NFS4_OK;
	}
	if (mds_quota_check_bytes(cd->quota, uid, gid,
				  additional_bytes) != MDS_OK) {
		return NFS4ERR_NOSPC;
	}
	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * Error mapping
 * ----------------------------------------------------------------------- */

enum nfs4_status mds_status_to_nfs4(enum mds_status st)
{
	switch (st) {
	case MDS_OK:              return NFS4_OK;
	case MDS_ERR_NOMEM:       return NFS4ERR_RESOURCE;
	case MDS_ERR_IO:          return NFS4ERR_IO;
	case MDS_ERR_NOTFOUND:    return NFS4ERR_NOENT;
	case MDS_ERR_EXISTS:      return NFS4ERR_EXIST;
	case MDS_ERR_INVAL:       return NFS4ERR_INVAL;
	case MDS_ERR_PERM:        return NFS4ERR_ACCES;
	case MDS_ERR_STALE:       return NFS4ERR_STALE;
	case MDS_ERR_GRACE:       return NFS4ERR_GRACE;
	case MDS_ERR_MOVED:       return NFS4ERR_MOVED;
	case MDS_ERR_DELAY:       return NFS4ERR_DELAY;
	case MDS_ERR_NOTEMPTY:    return NFS4ERR_NOTEMPTY;
	case MDS_ERR_ISDIR:       return NFS4ERR_ISDIR;
	case MDS_ERR_NOTDIR:      return NFS4ERR_NOTDIR;
	case MDS_ERR_NOSPC:       return NFS4ERR_NOSPC;
	case MDS_ERR_XDEV:        return NFS4ERR_XDEV;
	case MDS_ERR_LAYOUTUNAVAIL: return NFS4ERR_LAYOUTUNAVAILABLE;
	/* NOLINTNEXTLINE(bugprone-branch-clone) */
	case MDS_ERR_REPL:        return NFS4ERR_SERVERFAULT;
	case MDS_ERR_NOSTANDBY:   return NFS4ERR_SERVERFAULT;
	default:                  return NFS4ERR_SERVERFAULT;
	}
}

/* -----------------------------------------------------------------------
 * File handle validation helpers
 * ----------------------------------------------------------------------- */

enum nfs4_status require_current_fh(const struct compound_data *cd)
{
	if (!cd->current_fh_set) {
		return NFS4ERR_NOFILEHANDLE;
}
	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * Xattr filehandle helpers
 * ----------------------------------------------------------------------- */

bool is_xattr_fh(uint64_t fh)
{
	return (fh & XATTR_FH_FLAG) != 0;
}

uint64_t xattr_base_fileid(uint64_t fh)
{
	return fh & ~XATTR_FH_FLAG;
}

enum nfs4_status require_saved_fh(const struct compound_data *cd)
{
	if (!cd->saved_fh_set) {
		return NFS4ERR_NOFILEHANDLE;
}
	return NFS4_OK;
}

/**
 * Return NFS4ERR_DELAY if a given FH/path is within a MIGRATING or FROZEN
 * subtree.  When the path is known, uses the subtree map for lookup.
 * When the path is empty (raw PUTFH), falls back to the frozen fileid
 * set populated by migration_initiate().
 */
enum nfs4_status check_fh_frozen(const struct compound_data *cd,
					uint64_t fh, const char *path)
{
	struct subtree_entry entry;
	enum mds_status st;

	if (cd->smap == NULL) {
		return NFS4_OK;
}

	/* Path unknown (raw PUTFH): check frozen fileid set for a
	 * precise per-fileid answer instead of blocking globally. */
	if (path[0] == '\0') {
		return subtree_map_is_fileid_frozen(cd->smap, fh)
		       ? NFS4ERR_DELAY : NFS4_OK;
}

	st = subtree_map_lookup(cd->smap, path, &entry);
	if (st != MDS_OK) {
		return NFS4_OK; /* Conservative: if lookup fails, allow. */
}

	if (entry.state == SUBTREE_MIGRATING || entry.state == SUBTREE_FROZEN) {
		return NFS4ERR_DELAY;
}

	return NFS4_OK;
}

/**
 * Return NFS4ERR_DELAY if the current FH's subtree is MIGRATING or FROZEN.
 * Called before mutation ops (CREATE, REMOVE, RENAME, SETATTR, LINK).
 */
enum nfs4_status check_subtree_frozen(const struct compound_data *cd)
{
	return check_fh_frozen(cd, cd->current_fh.fileid, cd->current_path);
}

/**
 * Return NFS4ERR_DELAY if replication is unhealthy and write refusal
 * is configured.  Called before mutation ops alongside check_subtree_frozen.
 */
enum nfs4_status check_repl_health(const struct compound_data *cd)
{
	if (cd->hm != NULL && health_writes_blocked(cd->hm)) {
		return NFS4ERR_DELAY;
}
	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * Operation handlers
 *
 * Each returns an NFS4 status code.  On NFS4_OK, the result union
 * fields are populated as needed.
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static enum nfs4_status dispatch_op(struct compound_data *cd,
				    const struct nfs4_op *op,
				    struct nfs4_result *res)
{
	/*
	 * RFC 8881 S18.46.3 + S15.1.1.4: SEQUENCE must be the very
	 * first op of every session-aware COMPOUND.  Receiving it at
	 * any later position is NFS4ERR_SEQUENCE_POS.  Without this
	 * check the second SEQUENCE silently re-passes session state
	 * checks and leaves cd->sequence_done unchanged -- pynfs SEQ2
	 * (testNotFirst).
	 */
	if (cd->minorversion >= 1 && op->opnum == OP_SEQUENCE &&
	    cd->op_index > 0) {
		return NFS4ERR_SEQUENCE_POS;
	}

	/*
	 * RFC 8881 S2.10.6.2: all operations except session management
	 * require a preceding SEQUENCE in the same COMPOUND.
	 * When cd->st is NULL (test-compat mode), enforcement is skipped.
	 */
	/* NFSv4.0 (minorversion 0) does not use SEQUENCE.
	 * Only enforce for v4.1+ (RFC 8881 S2.10.6.2). */
	if (cd->minorversion >= 1 && cd->st != NULL &&
	    !cd->sequence_done) {
		switch (op->opnum) {
		case OP_SEQUENCE:
		case OP_EXCHANGE_ID:
		case OP_CREATE_SESSION:
		case OP_DESTROY_SESSION:
		case OP_DESTROY_CLIENTID:
		case OP_TEST_STATEID:
		case OP_FREE_STATEID:
		case OP_BIND_CONN_TO_SESSION:
		case OP_DELEGRETURN:
			break;
		default:
			/* RFC 8881 S2.10.6.4: unknown opcodes MUST
			 * return NFS4ERR_OP_ILLEGAL regardless of
			 * session state.  Let them fall through to
			 * dispatch_op's default case.  Valid NFSv4.2
			 * opcodes range 3..75; anything outside that
			 * range (plus OP_ILLEGAL=10044) is unknown.
			 * Pynfs COMP5 testUndefined. */
			if (op->opnum >= OP_ACCESS &&
			    op->opnum <= OP_REMOVEXATTR) {
				return NFS4ERR_OP_NOT_IN_SESSION;
			}
			break;
		}
	}

	/*
	 * RFC 8881 S2.10.6.4 / S18.36.3 / S18.37.3 / S18.50.3 --
	 * compound-position rules for session-management ops.
	 *
	 * CREATE_SESSION (S18.36.3): MUST be the SOLE op in its compound.
	 * The session does not exist when it is invoked, so it cannot
	 * follow SEQUENCE.  Pynfs CSESS23 (testNotOnlyOp) verifies this
	 * with [CREATE_SESSION, PUTROOTFH] and expects NFS4ERR_NOT_ONLY_OP.
	 *
	 * DESTROY_SESSION (S18.37.3): if the compound starts with
	 * SEQUENCE, DESTROY_SESSION MUST be the FINAL op (it tears down
	 * the session that SEQUENCE used, so subsequent ops have no
	 * session).  Otherwise it MUST be the SOLE op.  Pynfs DSESS9004
	 * (testDestoryNotFinalOps) and DSESS9005 (testDestoryNotSoleOps)
	 * each expect NFS4ERR_NOT_ONLY_OP.
	 *
	 * DESTROY_CLIENTID (S18.50.3): when the compound does not start
	 * with SEQUENCE, DESTROY_CLIENTID MUST be the SOLE op.  When the
	 * compound starts with SEQUENCE, DESTROY_CLIENTID may follow it
	 * (pynfs DESCID4/5 drive that path).  Pynfs DESCID7
	 * (testDestroyCIDNotOnly) verifies the no-SEQUENCE rule with
	 * [DESTROY_CLIENTID(0), RECLAIM_COMPLETE] expecting NOT_ONLY_OP.
	 *
	 * For CREATE_SESSION specifically, pynfs CSESS29 (testDRCMemLeak)
	 * sends [SEQUENCE, CREATE_SESSION] with bad channel attrs and
	 * expects NFS4ERR_TOOSMALL rather than NFS4ERR_NOT_ONLY_OP -- the
	 * test author's view (and Linux NFSD's behaviour) is that
	 * argument validation precedes placement validation.  We therefore
	 * delegate CREATE_SESSION's NOT_ONLY_OP check to op_create_session,
	 * which runs the arg validators first.
	 */
	{
		bool starts_with_seq = (cd->op_count > 0 && cd->ops != NULL &&
					cd->ops[0].opnum == OP_SEQUENCE);

		switch (op->opnum) {
		case OP_DESTROY_SESSION:
			if (starts_with_seq) {
				if (cd->op_index != cd->op_count - 1) {
					return NFS4ERR_NOT_ONLY_OP;
				}
			} else if (cd->op_count != 1) {
				return NFS4ERR_NOT_ONLY_OP;
			}
			break;
		case OP_DESTROY_CLIENTID:
			if (!starts_with_seq && cd->op_count != 1) {
				return NFS4ERR_NOT_ONLY_OP;
			}
			break;
		default:
			break;
		}
	}

	/*
	 * RFC 8881 S8.4.2.1: During the grace period, reject all
	 * operations except session management, PUTROOTFH, PUTFH,
	 * GETFH, GETATTR, SAVEFH, RESTOREFH, and reclaim-path
	 * OPENs (CLAIM_PREVIOUS for reclaim of previously held opens).
	 */
	if (grace_is_active()) {
		switch (op->opnum) {
		case OP_SEQUENCE:
		case OP_EXCHANGE_ID:
		case OP_CREATE_SESSION:
		case OP_DESTROY_SESSION:
		case OP_PUTROOTFH:
		case OP_PUTFH:
		case OP_GETFH:
		case OP_GETATTR:
		case OP_SAVEFH:
		case OP_RESTOREFH:
		case OP_OPEN:  /* Allow through -- claim type checked in op_open */
		case OP_DESTROY_CLIENTID:
		case OP_TEST_STATEID:
		case OP_FREE_STATEID:
		case OP_BIND_CONN_TO_SESSION:
		case OP_BACKCHANNEL_CTL:  /* RFC 8881 S18.33 -- session admin, safe in grace */
		case OP_LOCK:
		case OP_LOCKT:
		case OP_LOCKU:
		case OP_OPEN_DOWNGRADE:
		case OP_ACCESS:
		case OP_LOOKUP:
		case OP_LOOKUPP:
		case OP_READLINK:
		case OP_COMMIT:
		case OP_SECINFO:
		case OP_DELEGRETURN:
		case OP_RECLAIM_COMPLETE:
		case OP_GETXATTR:
		case OP_LISTXATTRS:
			break;
		default:
			return NFS4ERR_GRACE;
		}
	}

	/* Owner-routing: when cross-subtree hard links are fully
	 * wired, remote-owned FHs will return NFS4ERR_MOVED here
	 * so the client reconnects to the inode-owning MDS.
	 *
	 * Currently disabled because owner_mds_id is not yet
	 * propagated through PUTFH/GETFH/LOOKUP (Phase 1 pending).
	 * The guard is structurally ready for when v1 FH
	 * round-trip is complete. */
#if 0  /* Disabled: cross-subtree hard links not yet semantically complete. */
	if (cd->current_fh_set && cd->current_fh.owner_mds_id != 0 &&
	    cd->current_fh.owner_mds_id != cd->mds_id) {
		switch (op->opnum) {
		case OP_GETATTR: case OP_SETATTR:
		case OP_OPEN: case OP_CLOSE:
		case OP_READ: case OP_WRITE: case OP_COMMIT:
		case OP_LOCK: case OP_LOCKT: case OP_LOCKU:
		case OP_LAYOUTGET: case OP_LAYOUTRETURN:
		case OP_LAYOUTCOMMIT:
			return NFS4ERR_MOVED;
		default:
			break;
		}
	}
#endif

	/* Increment NFS operation counters. */
	atomic_fetch_add_explicit(&g_branch_metrics.nfs_ops_total, 1,
				  memory_order_relaxed);
	switch (op->opnum) {
	case OP_CREATE: atomic_fetch_add_explicit(
		&g_branch_metrics.nfs_op_create, 1, memory_order_relaxed); break;
	case OP_REMOVE: atomic_fetch_add_explicit(
		&g_branch_metrics.nfs_op_remove, 1, memory_order_relaxed); break;
	case OP_LOOKUP: atomic_fetch_add_explicit(
		&g_branch_metrics.nfs_op_lookup, 1, memory_order_relaxed); break;
	case OP_GETATTR: atomic_fetch_add_explicit(
		&g_branch_metrics.nfs_op_getattr, 1, memory_order_relaxed); break;
	case OP_SETATTR: atomic_fetch_add_explicit(
		&g_branch_metrics.nfs_op_setattr, 1, memory_order_relaxed); break;
	case OP_READDIR: atomic_fetch_add_explicit(
		&g_branch_metrics.nfs_op_readdir, 1, memory_order_relaxed); break;
	case OP_OPEN: atomic_fetch_add_explicit(
		&g_branch_metrics.nfs_op_open, 1, memory_order_relaxed); break;
	case OP_CLOSE: atomic_fetch_add_explicit(
		&g_branch_metrics.nfs_op_close, 1, memory_order_relaxed); break;
	case OP_READ: case OP_READ_PLUS: atomic_fetch_add_explicit(
		&g_branch_metrics.nfs_op_read, 1, memory_order_relaxed); break;
	case OP_WRITE: atomic_fetch_add_explicit(
		&g_branch_metrics.nfs_op_write, 1, memory_order_relaxed); break;
	case OP_LAYOUTGET: atomic_fetch_add_explicit(
		&g_branch_metrics.nfs_op_layoutget, 1, memory_order_relaxed); break;
	case OP_RENAME: atomic_fetch_add_explicit(
		&g_branch_metrics.nfs_op_rename, 1, memory_order_relaxed); break;
	default: break;
	}

	switch (op->opnum) {
	case OP_SEQUENCE:        return op_sequence(cd, op, res);
	case OP_EXCHANGE_ID:     return op_exchange_id(cd, op, res);
	case OP_CREATE_SESSION:  return op_create_session(cd, op, res);
	case OP_DESTROY_SESSION: return op_destroy_session(cd, op, res);
	case OP_ACCESS:          return op_access(cd, op, res);
	case OP_PUTROOTFH:       return op_putrootfh(cd, op, res);
	case OP_PUTFH:           return op_putfh(cd, op, res);
	case OP_GETFH:           return op_getfh(cd, op, res);
	case OP_SAVEFH:          return op_savefh(cd, op, res);
	case OP_RESTOREFH:       return op_restorefh(cd, op, res);
	case OP_LOOKUP:          return op_lookup(cd, op, res);
	case OP_GETATTR:         return op_getattr(cd, op, res);
	case OP_SETATTR:         return op_setattr(cd, op, res);
	case OP_CREATE:          return op_create(cd, op, res);
	case OP_REMOVE:          return op_remove(cd, op, res);
	case OP_RENAME:          return op_rename(cd, op, res);
	case OP_LINK:            return op_link(cd, op, res);
	case OP_READDIR:         return op_readdir(cd, op, res);
	case OP_OPEN:            return op_open(cd, op, res);
	case OP_CLOSE:           return op_close(cd, op, res);
	case OP_DELEGRETURN:     return op_delegreturn(cd, op, res);
	case OP_LOCK: {
		const struct nfs4_arg_lock *a = &op->arg.lock;
		struct lock_conflict conf;
		struct nfs4_stateid open_sid_resolved;
		struct nfs4_stateid lock_sid_resolved;
		enum nfs4_status rsid_st;
		int rc;
		if (cd->lt == NULL) { return NFS4ERR_NOTSUPP; }
		if (!cd->current_fh_set) { return NFS4ERR_NOFILEHANDLE; }
		/* Grace-period enforcement (RFC 8881 S18.10.4):
		 * - During grace: only reclaim locks allowed.
		 * - Outside grace: reclaim locks rejected. */
		if (grace_is_active() && !a->reclaim) {
			return NFS4ERR_GRACE;
		}
		if (!grace_is_active() && a->reclaim) {
			return NFS4ERR_NO_GRACE;
		}
		/* RFC 8881 S16.2.4 -- resolve CURRENT_STATEID4 magic in
		 * either locker variant.  The new-lock-owner branch uses
		 * open_stateid; the existing-lock-owner branch uses
		 * lock_stateid.  Both can carry the marker. */
		rsid_st = compound_resolve_stateid(cd, &a->open_stateid,
						   &open_sid_resolved);
		if (rsid_st != NFS4_OK) { return rsid_st; }
		rsid_st = compound_resolve_stateid(cd, &a->lock_stateid,
						   &lock_sid_resolved);
		if (rsid_st != NFS4_OK) { return rsid_st; }
		memset(&conf, 0, sizeof(conf));
		res->res.lock.stateid = lock_sid_resolved;
		rc = lock_acquire(cd->lt, cd->current_fh.fileid,
			a->lock_type, a->offset, a->length,
			cd->clientid,
			a->lock_owner, a->lock_owner_len,
			&open_sid_resolved,
			&res->res.lock.stateid, &conf);
		/*
		 * RFC 8881 §8.4.3 courtesy-client support: if the lock
		 * conflict is with an expired/orphaned client, revoke
		 * its locks and retry once.  Pynfs COUR2.
		 */
		if (rc == NFS4ERR_DENIED && cd->st != NULL) {
			if (lock_revoke_expired_for_file(
				cd->lt, cd->st,
				cd->current_fh.fileid) > 0) {
				memset(&conf, 0, sizeof(conf));
				rc = lock_acquire(cd->lt,
					cd->current_fh.fileid,
					a->lock_type, a->offset, a->length,
					cd->clientid,
					a->lock_owner, a->lock_owner_len,
					&open_sid_resolved,
					&res->res.lock.stateid, &conf);
			}
		}
		if (rc == NFS4ERR_DENIED) {
			res->res.lock.denied.offset = conf.offset;
			res->res.lock.denied.length = conf.length;
			res->res.lock.denied.lock_type = conf.lock_type;
			res->res.lock.denied.clientid = conf.clientid;
			res->res.lock.denied.owner_len = conf.owner_len;
			memcpy(res->res.lock.denied.owner, conf.owner, conf.owner_len);
			return NFS4ERR_DENIED;
		}
		return (rc == 0) ? NFS4_OK : NFS4ERR_SERVERFAULT;
	}
	case OP_LOCKT: {
		const struct nfs4_arg_lockt *a = &op->arg.lockt;
		struct lock_conflict conf;
		int rc;
		if (cd->lt == NULL) { return NFS4ERR_NOTSUPP; }
		if (!cd->current_fh_set) { return NFS4ERR_NOFILEHANDLE; }
		memset(&conf, 0, sizeof(conf));
		rc = lock_test(cd->lt, cd->current_fh.fileid,
			a->lock_type, a->offset, a->length,
			a->clientid, a->owner, a->owner_len, &conf);
		if (rc == NFS4ERR_DENIED) {
			res->res.lock.denied.offset = conf.offset;
			res->res.lock.denied.length = conf.length;
			res->res.lock.denied.lock_type = conf.lock_type;
			res->res.lock.denied.clientid = conf.clientid;
			res->res.lock.denied.owner_len = conf.owner_len;
			memcpy(res->res.lock.denied.owner, conf.owner, conf.owner_len);
			return NFS4ERR_DENIED;
		}
		return (rc == 0) ? NFS4_OK : NFS4ERR_SERVERFAULT;
	}
	case OP_LOCKU: {
		const struct nfs4_arg_locku *a = &op->arg.locku;
		struct nfs4_stateid lock_sid_resolved;
		enum nfs4_status rsid_st;
		int rc;
		if (cd->lt == NULL) { return NFS4ERR_NOTSUPP; }
		rsid_st = compound_resolve_stateid(cd, &a->lock_stateid,
						   &lock_sid_resolved);
		if (rsid_st != NFS4_OK) { return rsid_st; }
		res->res.locku.stateid = lock_sid_resolved;
		rc = lock_release(cd->lt, &res->res.locku.stateid,
			a->lock_type, a->offset, a->length);
		if (rc == NFS4ERR_BAD_STATEID) { return NFS4ERR_BAD_STATEID; }
		return (rc == 0) ? NFS4_OK : NFS4ERR_SERVERFAULT;
	}
	case OP_OPEN_DOWNGRADE:  return op_open_downgrade(cd, op, res);
	case OP_OPENATTR:        return op_openattr(cd, op, res);
	case OP_READ:            return op_read(cd, op, res);
	case OP_WRITE:           return op_write(cd, op, res);
	case OP_COMMIT:          return op_commit(cd, op, res);
	case OP_LOOKUPP:         return op_lookupp(cd, op, res);
	case OP_READLINK:        return op_readlink(cd, op, res);
	/* pNFS layout operations */
	case OP_LAYOUTGET:       return op_layoutget(cd, op, res);
	case OP_GETDEVICEINFO:   return op_getdeviceinfo(cd, op, res);
	case OP_LAYOUTRETURN:    return op_layoutreturn(cd, op, res);
	case OP_LAYOUTCOMMIT:    return op_layoutcommit(cd, op, res);
	/* NFSv4.2 operations (RFC 7862) */
	case OP_ALLOCATE:        return op_allocate(cd, op, res);
	case OP_COPY:            return op_copy(cd, op, res);
	case OP_COPY_NOTIFY:     return op_copy_notify(cd, op, res);
	case OP_DEALLOCATE:      return op_deallocate(cd, op, res);
	case OP_IO_ADVISE:       return op_io_advise(cd, op, res);
	case OP_LAYOUTERROR:     return op_layouterror(cd, op, res);
	case OP_LAYOUTSTATS:     return op_layoutstats(cd, op, res);
	case OP_OFFLOAD_CANCEL:  return op_offload_cancel(cd, op, res);
	case OP_OFFLOAD_STATUS:  return op_offload_status(cd, op, res);
	case OP_READ_PLUS:       return op_read_plus(cd, op, res);
	case OP_SEEK:            return op_seek(cd, op, res);
	case OP_WRITE_SAME:      return op_write_same(cd, op, res);
	case OP_CLONE:           return op_clone(cd, op, res);
	/* RFC 8276 extended attribute operations */
	case OP_GETXATTR:        return op_getxattr(cd, op, res);
	case OP_SETXATTR:        return op_setxattr(cd, op, res);
	case OP_LISTXATTRS:      return op_listxattrs(cd, op, res);
	case OP_REMOVEXATTR:     return op_removexattr(cd, op, res);
	case OP_RECLAIM_COMPLETE: return op_reclaim_complete(cd, op, res);
	case OP_GET_DIR_DELEGATION: return op_get_dir_delegation(cd, op, res);
	/*
	 * RFC 8881 §18.31 VERIFY / §18.19 NVERIFY.
	 *
	 * Compare the client-supplied fattr4 against the current file's
	 * attributes.  VERIFY: match → NFS4_OK, mismatch → NOT_SAME.
	 * NVERIFY: match → SAME, mismatch → NFS4_OK.
	 *
	 * Implementation: encode the current inode's attrs using the
	 * same bitmap into a scratch buffer via xdrmem, then memcmp
	 * the raw bytes.  Pynfs VF1r testMandFile.
	 */
	case OP_VERIFY:
	case OP_NVERIFY: {
		const struct nfs4_arg_verify *vf = &op->arg.verify;
		struct mds_inode vf_inode;
		enum nfs4_status vf_nst;
		enum mds_status vf_st;
		uint32_t vf_bm[NFS4_BITMAP_WORDS];
		uint32_t vf_bm_words = 0;

		vf_nst = require_current_fh(cd);
		if (vf_nst != NFS4_OK) { return vf_nst; }

		/* Read the file's current attributes. */
		vf_st = compound_inode_get(cd, cd->current_fh.fileid,
					  &vf_inode);
		if (vf_st != MDS_OK) {
			return mds_status_to_nfs4(vf_st);
		}

		/* Parse the bitmap from the client's raw fattr4 so we
		 * can encode the server's attrs with the same bitmap. */
		{
			XDR bm_xdr;
			xdrmem_create(&bm_xdr,
				(char *)(uintptr_t)vf->fattr_raw,
				vf->fattr_raw_len, XDR_DECODE);
			memset(vf_bm, 0, sizeof(vf_bm));
			if (!xdr_nfs4_bitmap_decode(&bm_xdr, vf_bm,
						   NFS4_BITMAP_WORDS,
						   &vf_bm_words)) {
				xdr_destroy(&bm_xdr);
				return NFS4ERR_BADXDR;
			}
			xdr_destroy(&bm_xdr);
		}
		(void)vf_bm_words; /* bitmap word count consumed by decoder */

		/* Encode the server's current attrs using the same
		 * bitmap into a scratch buffer. */
		{
			uint8_t srv_buf[NFS4_VERIFY_FATTR_MAX];
			XDR srv_xdr;
			uint32_t srv_len;

			xdrmem_create(&srv_xdr, (char *)srv_buf,
				      sizeof(srv_buf), XDR_ENCODE);
			if (!xdr_nfs4_fattr_encode(&srv_xdr,
						  &vf_inode, vf_bm)) {
				xdr_destroy(&srv_xdr);
				return NFS4ERR_SERVERFAULT;
			}
			srv_len = xdr_getpos(&srv_xdr);
			xdr_destroy(&srv_xdr);

			/* Compare raw fattr4 bytes. */
			bool match = (srv_len == vf->fattr_raw_len &&
				      memcmp(srv_buf, vf->fattr_raw,
					     srv_len) == 0);
			if (op->opnum == OP_VERIFY) {
				return match ? NFS4_OK
					     : NFS4ERR_NOT_SAME;
			}
			/* NVERIFY */
			return match ? NFS4ERR_SAME : NFS4_OK;
		}
	}

	/*
	 * DESTROY_CLIENTID (RFC 8881 S18.50): destroy the named clientid.
	 *
	 *   - clientid not found      -> NFS4ERR_STALE_CLIENTID (DESCID3/4/8)
	 *   - clientid has confirmed  -> NFS4ERR_CLIENTID_BUSY (DESCID5/6)
	 *     sessions
	 *   - otherwise                -> destroy + NFS4_OK
	 *
	 * When session_table is absent (test compat) the legacy match-or-
	 * accept behaviour is preserved.  When the SEQUENCE-bound clientid
	 * (cd->clientid) matches the destroy target, RFC 8881 S18.50.3
	 * mandates NFS4ERR_CLIENTID_BUSY -- the session itself is in use
	 * by the very compound issuing the destroy (DESCID5).
	 */
	case OP_DESTROY_CLIENTID: {
		uint64_t target = op->arg.destroy_clientid;
		int dc_rc;

		if (cd->st == NULL) {
			if (cd->clientid != 0 && target != cd->clientid) {
				return NFS4ERR_STALE_CLIENTID;
			}
			return NFS4_OK;
		}
		if (cd->clientid != 0 && target == cd->clientid) {
			/* Pynfs DESCID5: SEQUENCE on session of client X,
			 * then DESTROY_CLIENTID(X).  RFC S18.50.3 -- BUSY. */
			return NFS4ERR_CLIENTID_BUSY;
		}
		dc_rc = session_destroy_client(cd->st, target);
		if (dc_rc == -1) {
			return NFS4ERR_STALE_CLIENTID;
		}
		if (dc_rc == -2) {
			return NFS4ERR_CLIENTID_BUSY;
		}
		return NFS4_OK;
	}
	case OP_TEST_STATEID: {
		const struct nfs4_arg_test_stateid *ts = &op->arg.test_stateid;
		uint32_t k;
		res->res.test_stateid.count = ts->count;
		for (k = 0; k < ts->count && k < 64; k++) {
			/* Check open state, lock state, layout state. */
			bool found = false;
			if (cd->ot != NULL) {
				struct nfs4_open_state os;
				if (open_state_find(cd->ot, &ts->stateids[k], &os) == 0) {
					found = true;
				}
			}
			if (!found && cd->lt != NULL) {
				if (lock_state_exists(cd->lt,
						      ts->stateids[k].other)) {
					found = true;
				}
			}
			if (!found && cat_on_root_global_db(cd)) {
				uint64_t lc, lf;
				if (coord_layout_get_by_stateid(
					cd, ts->stateids[k].other,
					&lc, &lf, NULL, NULL, NULL, NULL) == MDS_OK) {
					found = true;
				}
			}
			if (!found && cd->ddt != NULL) {
				if (dir_deleg_stateid_exists(cd->ddt,
							     ts->stateids[k].other)) {
					found = true;
				}
			}
			if (found) {
				res->res.test_stateid.status_codes[k] = NFS4_OK;
			} else if (cd->dt != NULL &&
				   deleg_is_revoked(cd->dt,
						    ts->stateids[k].other)) {
				/* RFC 8881 §10.2.1: revoked deleg. */
				res->res.test_stateid.status_codes[k] =
					NFS4ERR_DELEG_REVOKED;
			} else {
				res->res.test_stateid.status_codes[k] =
					NFS4ERR_BAD_STATEID;
			}
		}
		return NFS4_OK;
	}
	case OP_FREE_STATEID: {
		/* RFC 8881 S18.38: free lock/layout stateid.
		 * Open stateids must be freed via CLOSE, not FREE_STATEID;
		 * an attempt to FREE_STATEID a still-open stateid returns
		 * NFS4ERR_LOCKS_HELD (pynfs CSID9 testOpenFreestateidClose). */
		struct nfs4_stateid fs_resolved;
		enum nfs4_status rsid_st;
		rsid_st = compound_resolve_stateid(
			cd, &op->arg.free_stateid, &fs_resolved);
		if (rsid_st != NFS4_OK) { return rsid_st; }
		/* Reject FREE_STATEID against an active open stateid. */
		if (cd->ot != NULL) {
			struct nfs4_open_state os;
			if (open_state_find(cd->ot, &fs_resolved, &os) == 0) {
				return NFS4ERR_LOCKS_HELD;
			}
		}
		if (cd->lt != NULL) {
			struct nfs4_stateid fs_sid = fs_resolved;
			if (lock_release(cd->lt, &fs_sid, 0, 0, 0) == 0) {
				return NFS4_OK;
			}
		}
		/* RFC 8881 §10.2.1: free a revoked delegation stateid. */
		if (cd->dt != NULL) {
			if (deleg_free_revoked(cd->dt,
					       fs_resolved.other) == 0) {
				return NFS4_OK;
			}
		}
		/* Try layout state via catalogue coordination API. */
		if (cd->cat != NULL) {
			if (mds_coord_layout_return(
				cd->cat, NULL, fs_resolved.other,
				cd->clientid, 0,
				NULL, 0) == MDS_OK) {
				return NFS4_OK;
			}
		}
		return NFS4ERR_BAD_STATEID;
	}

	/*
	 * BIND_CONN_TO_SESSION (RFC 8881 S18.34) -- attach the
	 * incoming TCP connection to the named session as a
	 * backchannel.  The Linux kernel client sends this op
	 * after CREATE_SESSION when it wants this connection to
	 * carry callbacks; without an actual binding here the
	 * kernel records the bind as having failed and rejects
	 * every subsequent CB_SEQUENCE with NFS4ERR_BADSESSION.
	 *
	 * The decoder stores the session_id in
	 * op->arg.destroy_session.session_id (it reuses that union
	 * slot to avoid adding a new struct on the hot path; see
	 * decode_one_op).  We treat any direction request as
	 * "bind backchannel on this conn" -- the only direction
	 * the kernel ever asks us to act on -- and return BOTH in
	 * the response so the kernel sees its request honoured.
	 */
	case OP_BIND_CONN_TO_SESSION:
		if (cd->st != NULL && cd->conn != NULL) {
			(void)session_bind_conn(cd->st,
				op->arg.destroy_session.session_id,
				cd->conn);
		}
		memcpy(res->res.sequence.session_id,
		       op->arg.destroy_session.session_id,
		       SESSION_ID_SIZE);
		return NFS4_OK;

	/*
	 * BACKCHANNEL_CTL (RFC 8881 S18.33) -- update CB program
	 * number and/or CB security parms on the SEQUENCE-bound
	 * session.  Status-only result.  Pynfs DELEG7.
	 */
	case OP_BACKCHANNEL_CTL:
		return op_backchannel_ctl(cd, op, res);

	/*
	 * SECINFO / SECINFO_NO_NAME: return supported security flavors.
	 * RFC 8881 S18.29 / S18.45.
	 *
	 * RFC 5661 S2.6.3.1.1.8 / RFC 8881 S2.6.3.1.1.8: SECINFO and
	 * SECINFO_NO_NAME consume the current filehandle -- any op that
	 * follows in the same compound (e.g. GETFH) must observe
	 * NFS4ERR_NOFILEHANDLE.  Pynfs SEC2, SECNN2 cover this.
	 *
	 * RFC 8881 S18.45.3: SECINFO_NO_NAME(SECINFO_STYLE4_PARENT)
	 * applied to the root filehandle returns NFS4ERR_NOENT
	 * because the root has no parent.  Pynfs SECNN3 covers this.
	 * SECNN4 (SECINFO_STYLE4_PARENT on a non-root FH) succeeds.
	 */
	case OP_SECINFO:
	case OP_SECINFO_NO_NAME: {
		enum nfs4_status nst = require_current_fh(cd);
		if (nst != NFS4_OK) {
			return nst;
		}
		if (op->opnum == OP_SECINFO_NO_NAME &&
		    op->arg.secinfo_no_name.style == SECINFO_STYLE4_PARENT &&
		    cd->current_fh.fileid == MDS_FILEID_ROOT) {
			return NFS4ERR_NOENT;
		}
		res->res.secinfo.count = 1;
		res->res.secinfo.flavors[0] = 1; /* AUTH_SYS */
		/* RFC 5661 S2.6.3.1.1.8: drop the current FH so trailing
		 * compound ops see NFS4ERR_NOFILEHANDLE. */
		cd->current_fh_set = false;
		cd->current_inode_valid = false;
		cd->current_path[0] = '\0';
		return NFS4_OK;
	}

	default:
		return NFS4ERR_OP_ILLEGAL;
	}
}

/* -----------------------------------------------------------------------
 * Phase C / Step 1 of docs/hpc-nto1-plan.md -- wire-buffer
 * heap-ification helpers.  See compound.h for the public contract.
 *
 * The functions below are the ONLY allowed lifecycle entry points
 * for layoutget result heap state.  Producers (compound_layout.c)
 * call _alloc, the encoder reads the heap pointers, and rpc_server.c
 * (or any other caller that drives compound_process) calls
 * nfs4_result_destroy after the response has been encoded.
 *
 * Allocation policy:
 *  - calloc, not malloc, so a partially-populated array is safely
 *    zeroed (encoder treats nfs_fh_len == 0 as "no FH").
 *  - On failure, the result is left destroy-safe (NULL pointers,
 *    zero counts).
 *  - _alloc() frees any previously-attached buffers, so calling it
 *    twice on the same struct does not leak.
 * ----------------------------------------------------------------------- */

int nfs4_ff_mirror_alloc(struct nfs4_ff_mirror *m, uint32_t n)
{
	if (m == NULL) {
		return -1;
	}
	free(m->ds);
	m->ds = NULL;
	m->ds_count = 0;
	if (n == 0) {
		/* Empty mirror is a valid state -- caller may want to
		 * record a zero-DS placeholder.  Treat as success. */
		return 0;
	}
	m->ds = calloc(n, sizeof(*m->ds));
	if (m->ds == NULL) {
		return -1;
	}
	m->ds_count = n;
	return 0;
}

void nfs4_ff_mirror_destroy(struct nfs4_ff_mirror *m)
{
	if (m == NULL) {
		return;
	}
	free(m->ds);
	m->ds = NULL;
	m->ds_count = 0;
}

int nfs4_res_layoutget_alloc(struct nfs4_res_layoutget *r,
			      uint32_t ds_count,
			      uint32_t ff_mirror_count)
{
	if (r == NULL) {
		return -1;
	}

	/* Free any prior allocations first so a re-entry on the same
	 * struct does not leak.  Idempotent. */
	nfs4_res_layoutget_destroy(r);

	if (ds_count > 0) {
		r->ds = calloc(ds_count, sizeof(*r->ds));
		if (r->ds == NULL) {
			nfs4_res_layoutget_destroy(r);
			return -1;
		}
		r->ds_count = ds_count;
	}
	if (ff_mirror_count > 0) {
		r->ff_mirrors = calloc(ff_mirror_count,
				       sizeof(*r->ff_mirrors));
		if (r->ff_mirrors == NULL) {
			nfs4_res_layoutget_destroy(r);
			return -1;
		}
		r->ff_mirror_count = ff_mirror_count;
	}
	return 0;
}

void nfs4_res_layoutget_destroy(struct nfs4_res_layoutget *r)
{
	if (r == NULL) {
		return;
	}
	if (r->ff_mirrors != NULL) {
		for (uint32_t i = 0; i < r->ff_mirror_count; i++) {
			nfs4_ff_mirror_destroy(&r->ff_mirrors[i]);
		}
		free(r->ff_mirrors);
		r->ff_mirrors = NULL;
	}
	r->ff_mirror_count = 0;

	free(r->ds);
	r->ds = NULL;
	r->ds_count = 0;
}

void nfs4_result_destroy(struct nfs4_result *r)
{
	if (r == NULL) {
		return;
	}
	/*
	 * Only the layoutget result currently owns heap state; other
	 * opnums fall through and the call is a no-op.  When future
	 * results add heap buffers, extend this dispatcher case-wise.
	 */
	if (r->opnum == OP_LAYOUTGET) {
		nfs4_res_layoutget_destroy(&r->res.layoutget);
	}
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

void compound_init(struct compound_data *cd)
{
	memset(cd, 0, sizeof(*cd));
}


/* -----------------------------------------------------------------------
 * Subtree-resolved op accounting (Phase 1a, Tier 3)
 *
 * Increments the owning subtree root's op counter after a successful
 * mutating operation.  Uses subtree_map_lookup() (longest-prefix match)
 * to resolve the affected subtree, then calls subtree_map_inc_ops()
 * on the root path -- not the raw file path, which would miss most
 * real subtree traffic.
 *
 * Only mutating metadata ops are counted (CREATE, REMOVE, RENAME,
 * LINK, SETATTR, WRITE).  LAYOUTGET is access-hotness, not mutation
 * pressure, and is deliberately excluded.
 * ----------------------------------------------------------------------- */

static bool is_mutating_op(enum nfs_opnum4 opnum)
{
	switch (opnum) {
	case OP_CREATE:
	case OP_REMOVE:
	case OP_RENAME:
	case OP_LINK:
	case OP_SETATTR:
	case OP_WRITE:
		return true;
	default:
		return false;
	}
}

static void account_subtree_op(struct compound_data *cd,
			       enum nfs_opnum4 opnum)
{
	struct subtree_entry entry;

	if (cd->smap == NULL) {
		return;
	}
	if (!is_mutating_op(opnum)) {
		return;
	}
	/* Resolve the owning subtree root from current_path. */
	if (cd->current_path[0] == '\0') {
		return; /* Path unknown (raw PUTFH) -- cannot attribute. */
	}
	if (subtree_map_lookup(cd->smap, cd->current_path,
			       &entry) != MDS_OK) {
		return; /* Lookup failed -- skip silently. */
	}
	/* Increment the resolved subtree root, not the raw path. */
	(void)subtree_map_inc_ops(cd->smap, entry.path);
}

/**
 * Revoke an unused layout pre-grant.
 *
 * If op_open(CREATE) pre-granted a layout but LAYOUTGET never
 * consumed it (e.g. an intermediate op failed), delete the
 * persisted layout_state + ds_layout_idx so they don't leak.
 *
 * This is treated as mandatory cleanup.  If the CQ submit fails
 * and no replication is configured, we fall back to a direct catalogue
 * write txn.  When replication IS configured, a direct-txn delete
 * would create primary/standby divergence (the pre-grant was
 * committed via the replicated CQ, so the standby has it).  In
 * that case we accept the orphan: the stateid was never returned
 * to any client, so the standby can never issue recall for it,
 * and a future layout-state scavenger or restart cleanup will
 * garbage-collect it.
 */
static void revoke_unused_pregrant(struct compound_data *cd)
{
	struct mds_ds_map_entry *sme = NULL;
	struct layout_ds_id_list ds_list = { 0 };
	uint32_t sc, su, mc;
	enum mds_status st;
	bool have_ds_list = false;

	if (cd == NULL || !cd->layout_pregranted) {
		return;
	}

	/* Read stripe map once for ds_ids. */
	st = cat_stripe_map_get(cd,
				     cd->layout_pregrant_fileid,
				     &sc, &su, &mc, &sme);
	if (st == MDS_OK && sme != NULL) {
		uint64_t total64 = (uint64_t)sc * (uint64_t)mc;

		if (total64 <= UINT32_MAX) {
			st = layout_ds_id_list_from_entries(
				&ds_list, sme, (uint32_t)total64);
			have_ds_list = (st == MDS_OK && ds_list.count > 0);
		}
		free(sme);
		sme = NULL;
	}
	if (!have_ds_list) {
		/* Avoid deleting only layout_state while leaving ds_layout_idx
		 * rows behind.  The unused stateid was never returned to the
		 * client; keeping the grant rows consistent is safer than a
		 * partial cleanup when the full DS list is unavailable. */
		layout_ds_id_list_destroy(&ds_list);
		cd->layout_pregranted = false;
		return;
	}

	/* Primary path: delete via CQ (replicated). */
	st = MDS_ERR_IO;
	if (cd->cq != NULL) {
		struct commit_op cop;
		struct commit_op_layout_del *ld;

		memset(&cop, 0, sizeof(cop));
		cop.type = COMMIT_OP_LAYOUT_STATE_DEL;
		ld = &cop.args.layout_del;
		ld->clientid = cd->clientid;
		ld->fileid = cd->layout_pregrant_fileid;
		memcpy(ld->stateid_other,
		       cd->layout_pregrant_stateid.other,
		       sizeof(ld->stateid_other));
		ld->ds_ids = ds_list.ids;
		ld->ds_count = ds_list.count;
		st = commit_queue_submit(cd->cq, &cop);
	}

	/*
	 * Fallback: delete via catalogue coordination API when CQ
	 * submit failed and no replication is active.
	 */
	if (st != MDS_OK && cd->cat != NULL &&
	    (cd->cq == NULL ||
	     commit_queue_get_repl(cd->cq) == NULL)) {
		(void)mds_coord_layout_return(
			cd->cat, NULL,
			cd->layout_pregrant_stateid.other,
			cd->clientid,
			cd->layout_pregrant_fileid,
			ds_list.ids, ds_list.count);
	}

	layout_ds_id_list_destroy(&ds_list);
	cd->layout_pregranted = false;
}

/* Per-op timing: sample 1-in-64 compounds to avoid log spam. */
static _Atomic uint32_t g_compound_sample_ctr = 0;
#define COMPOUND_SAMPLE_INTERVAL 64

static const char *opnum_name(enum nfs_opnum4 op)
{
	switch (op) {
	case OP_SEQUENCE: return "SEQ";
	case OP_PUTROOTFH: return "PRFH";
	case OP_PUTFH: return "PFH";
	case OP_LOOKUP: return "LKP";
	case OP_OPEN: return "OPEN";
	case OP_GETFH: return "GFH";
	case OP_GETATTR: return "GATTR";
	case OP_SETATTR: return "SATTR";
	case OP_LAYOUTGET: return "LG";
	case OP_LAYOUTRETURN: return "LR";
	case OP_CLOSE: return "CLS";
	case OP_CREATE: return "CRT";
	case OP_REMOVE: return "RM";
	case OP_RENAME: return "REN";
	case OP_ACCESS: return "ACC";
	default: return "?";
	}
}

uint32_t compound_process(struct compound_data *cd,
			   const struct nfs4_op *ops,
			   struct nfs4_result *results,
			   uint32_t count)
{
	uint32_t i;
	struct timespec t_start, t_op_start, t_op_end;
	uint64_t op_us[64];
	bool do_sample = (atomic_fetch_add(&g_compound_sample_ctr, 1)
			  % COMPOUND_SAMPLE_INTERVAL) == 0;

	if (do_sample && count <= 64) {
		clock_gettime(CLOCK_MONOTONIC, &t_start);
	}

	cd->ops = ops;
	cd->op_count = count;
	cd->op_index = 0;

	for (i = 0; i < count; i++) {
		cd->op_index = i;
		/*
		 * Phase C / Step 1: free any heap state attached to a
		 * recycled thread-local results slot before zeroing it.
		 * The previous compound on this thread may have left a
		 * layoutget heap buffer reachable; memset() alone does
		 * not free it.  nfs4_result_destroy() dispatches by the
		 * stored opnum, so it must run BEFORE memset clears it.
		 */
		nfs4_result_destroy(&results[i]);
		memset(&results[i], 0, sizeof(results[i]));
		results[i].opnum = ops[i].opnum;

		if (do_sample && i < 64) {
			clock_gettime(CLOCK_MONOTONIC, &t_op_start);
		}

		/*
		 * RFC 8881 S2.10.6.1.3: before dispatching, check if the
		 * accumulated response would exceed ca_maxresponsesize.
		 * Only enforced after SEQUENCE has populated the cap.
		 * Per-op estimate: 8 bytes (opnum+status) + 128 bytes
		 * conservative body.  Pynfs CSESS26 (ca_maxresponsesize=400
		 * with 4xGETATTR) drives this path.
		 */
		if (cd->max_response_size > 0 && i > 0) {
			uint32_t op_est = 136; /* 8 + 128 body */
			if (cd->response_size_est + op_est > cd->max_response_size) {
				results[i].status = NFS4ERR_REP_TOO_BIG;
				goto op_done;
			}
		}

		{
			/*
			 * Per-op latency observability.  Captures wall-
			 * clock around dispatch_op (does NOT include XDR
			 * encode of the result -- that's RPC layer).
			 * Phase tracker stays armed for the duration so
			 * any catalogue / state / ds_io scope inside the
			 * handler gets credited.
			 *
			 * Gated on mds_op_metrics_enabled() so disabling
			 * observability at runtime collapses the whole
			 * block to a bare dispatch_op call.
			 */
			if (__builtin_expect(mds_op_metrics_enabled(), 1)) {
				enum mds_op_class _opc =
					mds_op_class_from_opnum(ops[i].opnum);
				struct timespec _t_op_a, _t_op_b;

				clock_gettime(CLOCK_MONOTONIC, &_t_op_a);
				mds_phase_begin_op();
				results[i].status = dispatch_op(cd, &ops[i],
								&results[i]);
				mds_phase_end_op(_opc);
				clock_gettime(CLOCK_MONOTONIC, &_t_op_b);
				{
					uint64_t _ns = (uint64_t)
						(_t_op_b.tv_sec -
						 _t_op_a.tv_sec) *
						1000000000ULL +
						(uint64_t)(_t_op_b.tv_nsec -
							   _t_op_a.tv_nsec);
					mds_op_observe_total(_opc, _ns);
				}
			} else {
				results[i].status = dispatch_op(cd, &ops[i],
								&results[i]);
			}
		}

		/*
		 * RFC 8881 S2.10.6.4: if the operation is unknown, the
		 * result MUST use OP_ILLEGAL as the opnum, not the raw
		 * wire value the client sent.  Pynfs COMP5 testUndefined.
		 */
		if (results[i].status == NFS4ERR_OP_ILLEGAL) {
			results[i].opnum = OP_ILLEGAL;
		}

op_done:
		/* Update response size estimate for REP_TOO_BIG. */
		if (cd->max_response_size > 0) {
			cd->response_size_est += (results[i].status == NFS4_OK)
				? 136U : 8U;
		}

		if (do_sample && i < 64) {
			clock_gettime(CLOCK_MONOTONIC, &t_op_end);
			int64_t ds = (int64_t)(t_op_end.tv_sec - t_op_start.tv_sec);
			int64_t dn = (int64_t)(t_op_end.tv_nsec - t_op_start.tv_nsec);
			op_us[i] = (uint64_t)(ds * 1000000LL + dn / 1000LL);
		}

		if (results[i].status == NFS4_OK) {
			account_subtree_op(cd, ops[i].opnum);
			/* RFC 8881 S16.2.4 -- update current_stateid from
			 * producer ops (OPEN, OPEN_DOWNGRADE, CLOSE, LOCK,
			 * LOCKU, LAYOUTGET, LAYOUTRETURN), invalidate on
			 * FH-changing ops (PUTFH, PUTROOTFH, LOOKUP,
			 * LOOKUPP), save/restore on SAVEFH/RESTOREFH. */
			compound_update_current_stateid_post(
				cd, &ops[i], &results[i]);
		}
		if (results[i].status != NFS4_OK) {
			revoke_unused_pregrant(cd);
			compound_ro_txn_reset(cd);
			cd->ops = NULL;
			cd->op_count = 0;
			cd->op_index = 0;
			return i + 1;
		}
	}

	/* Log sampled compound timing. */
	if (do_sample && count <= 64) {
		struct timespec t_end;
		clock_gettime(CLOCK_MONOTONIC, &t_end);
		int64_t total_us = (int64_t)(t_end.tv_sec - t_start.tv_sec) * 1000000LL
				  + (int64_t)(t_end.tv_nsec - t_start.tv_nsec) / 1000LL;
		if (total_us > 2000) { /* Only log compounds > 2ms */
			char buf[512];
			int pos = snprintf(buf, sizeof(buf),
				"PERF: compound %lluus [", (unsigned long long)total_us);
			for (uint32_t j = 0; j < count && j < 64; j++) {
				pos += snprintf(buf + pos, sizeof(buf) - (size_t)pos,
					"%s=%lluus ", opnum_name(ops[j].opnum),
					(unsigned long long)op_us[j]);
				if (pos >= (int)sizeof(buf) - 20) { break; }
			}
			(void)snprintf(buf + pos,
				sizeof(buf) - (size_t)pos, "]\n");
			(void)fprintf(stderr, "%s", buf);
		}
	}

	revoke_unused_pregrant(cd);
	compound_ro_txn_reset(cd);
	cd->ops = NULL;
	cd->op_count = 0;
	cd->op_index = 0;
	return count;
}

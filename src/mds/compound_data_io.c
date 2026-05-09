/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 */
/*
 * compound_data_io.c -- open/close, READ, WRITE, IO_ADVISE, inline promotion.
 */

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdatomic.h>
#include <arpa/inet.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "compound_internal.h"
#include "session.h"
#include "open_state.h"
#include "proxy_io.h"
#include "io_tracker.h"
#include "commit_queue.h"
#include "ds_health.h"
#include "placement.h"
#include "ds_prealloc.h"
#include "quota.h"
#include "health.h"
#include "grace.h"
#include "ds_prepare.h"
#include "catalogue_rondb.h"
#include "delegation.h"
#include "dir_delegation.h"
#include "inode_cache.h"
#include "mds_metrics.h"
#include "hpc_shared.h"
#include "layout_commit_aggregator.h"  /* Phase F of docs/hpc-nto1-plan.md */


/**
 * ts_delta_ns - Elapsed nanoseconds between two monotonic samples.
 * @a: Earlier sample.
 * @b: Later sample.
 *
 * Returns (b - a) as unsigned nanoseconds.  Caller must ensure
 * both samples are from CLOCK_MONOTONIC and that b >= a (otherwise
 * the result wraps, which matches traditional Unix behaviour).
 */
static uint64_t ts_delta_ns(const struct timespec *a,
			    const struct timespec *b)
{
	int64_t sec_ns = (int64_t)(b->tv_sec - a->tv_sec) * 1000000000LL;
	int64_t nsec   = (int64_t)(b->tv_nsec - a->tv_nsec);
	return (uint64_t)(sec_ns + nsec);
}


/* -----------------------------------------------------------------------
 * OPEN / CLOSE handlers
 * ----------------------------------------------------------------------- */

/* -----------------------------------------------------------------------
 * Phase C / Step 5 of docs/hpc-nto1-plan.md -- helpers for the
 * HPC-Shared wide pre-warm fast path inside op_open(CREATE).
 *
 * These helpers are static and only called from op_open below; keeping
 * them at file scope (instead of nested in op_open) keeps the existing
 * function's already-high cognitive complexity under control.
 * ----------------------------------------------------------------------- */

/**
 * Compute the configured stripe cap for an HPC-Shared CREATE.
 *
 * Two independent caps apply before any client hint:
 *   - cd->cfg_hpc_max_stripe_count (operator knob; 0 -> default 128)
 *   - MDS_MAX_STRIPES (compile-time hard ceiling, today 1024)
 */
static uint32_t op_open_hpc_config_stripe_cap(const struct compound_data *cd)
{
	uint32_t cap = cd->cfg_hpc_max_stripe_count;

	if (cap == 0) {
		cap = 128;  /* matches src/common/config.c default */
	}
	if (cap > MDS_MAX_STRIPES) {
		cap = MDS_MAX_STRIPES;
	}
	return cap;
}

/**
 * Compute the default effective stripe count for an HPC-Shared CREATE.
 *
 * Returns 0 when no DSes are eligible (caller must surface NOSPC).
 * Best-effort: a transient catalogue read failure returns 0 so the
 * caller falls back to the legacy CREATE branch instead of erroring
 * out the OPEN.
 */
static uint32_t op_open_hpc_clamp_stripes(const struct compound_data *cd,
					  uint32_t *online_ds_count)
{
	uint32_t cap = op_open_hpc_config_stripe_cap(cd);

	struct mds_ds_info *list = NULL;
	uint32_t count = 0;
	if (online_ds_count != NULL) {
		*online_ds_count = 0;
	}

	if (mds_cat_ds_list(cd->cat, &list, &count) != MDS_OK) {
		return 0;
	}
	uint32_t alive = 0;
	for (uint32_t i = 0; i < count; i++) {
		if (list[i].state == DS_ONLINE &&
		    list[i].mode == DS_MODE_GENERIC) {
			alive++;
		}
	}
	free(list);
	if (alive == 0) {
		return 0;
	}
	if (online_ds_count != NULL) {
		*online_ds_count = alive;
	}
	if (cap > alive) {
		cap = alive;
	}
	return cap;
}

/**
 * Run the wide pre-warm sequence for an HPC-Shared CREATE.
 *
 * On MDS_OK, @p out_inode carries the freshly-created child with
 * MDS_IFLAG_HPC_SHARED set, the dirent is committed, and the wide
 * stripe map is persisted.  On any error code the caller must surface
 * the corresponding NFS4 status; @p out_inode is left undefined.
 *
 * The helper picks geometry from cd->cfg_*; mirror_count is fixed at
 * 1 (mirroring is a Phase D follow-up).  strict_unique_ds is true
 * because we already clamped stripe_count to the online DS count, so
 * a unique-DS plan is feasible.
 */
static enum mds_status op_open_hpc_wide_create(
	struct compound_data *cd,
	const struct nfs4_arg_open *a,
	uint64_t eff_uid, uint64_t eff_gid,
	struct mds_inode *out_inode)
{
	uint32_t online_ds_count = 0;
	uint32_t stripe_count = op_open_hpc_clamp_stripes(cd, &online_ds_count);

	if (stripe_count == 0) {
		return MDS_ERR_NOSPC;
	}

	uint32_t stripe_unit = cd->cfg_stripe_unit;  /* 0 -> helper default */
	uint8_t  pref_xport  = cd->gpudirect_required ? DS_TRANSPORT_RDMA : 0;
	uint32_t pref_caps   = cd->gpudirect_required ? DS_CAP_GPUDIRECT : 0;
	if (a->layout_hint.present) {
		uint32_t hinted_count = stripe_count;
		uint32_t hinted_unit = stripe_unit;

		if (hpc_hint_select_geometry(&a->layout_hint.hpc,
					     online_ds_count,
					     &hinted_count,
					     &hinted_unit)) {
			uint32_t cap = op_open_hpc_config_stripe_cap(cd);

			if (hinted_count > cap) {
				hinted_count = cap;
			}
			if (hinted_count > MDS_MAX_STRIPES) {
				hinted_count = MDS_MAX_STRIPES;
			}
			if (hinted_count > online_ds_count) {
				hinted_count = online_ds_count;
			}
			if (hinted_count == 0) {
				return MDS_ERR_NOSPC;
			}
			stripe_count = hinted_count;
			stripe_unit = hinted_unit;
		}
	}

	return hpc_shared_create_wide_layout(
		cd->cat, cd->prealloc,
		cd->current_fh.fileid,
		a->name,
		a->mode,
		eff_uid, eff_gid,
		stripe_count,
		1,                          /* mirror_count */
		stripe_unit,
		DS_TRANSPORT_TCP,
		pref_xport, pref_caps,
		true,                       /* strict_unique_ds */
		out_inode);
}

/**
 * op_open -- NFSv4.1 OPEN operation (RFC 8881 S18.16).
 *
 * Supports CLAIM_NULL (open/create by name in current_fh directory)
 * and CLAIM_FH (open current_fh directly).
 *
 * CLAIM_NULL:
 *   1. current_fh must be a directory.
 *   2. Look up name.  If not found and create is requested, create it.
 *   3. The target must be a regular file.
 *   4. Check share conflicts and allocate stateid.
 *   5. Set current_fh to the opened file.
 *
 * CLAIM_FH:
 *   1. current_fh must be a regular file.
 *   2. Check share conflicts and allocate stateid.
 *   3. current_fh remains the same.
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_open(struct compound_data *cd,
				const struct nfs4_op *op,
				struct nfs4_result *res)
{
	const struct nfs4_arg_open *a = &op->arg.open;
	struct nfs4_res_open *r = &res->res.open;
	struct mds_inode inode;
	enum nfs4_status nst;
	enum mds_status st;
	uint64_t target_fid;
	int rc;
	bool just_created = false;
	/*
	 * Phase-latency instrumentation (mds_branch_metrics
	 * open_create_*).  Samples are taken on the CLAIM_NULL path;
	 * ns_parent and ns_lookup are held in locals until the
	 * NOTFOUND-plus-create branch confirms this is a create path,
	 * at which point they are folded into the counters together
	 * with the later phases.  time_create gates the remaining
	 * phases (ns_create, ds_prepare, state_open, total) that only
	 * fire on a successful create.
	 */
	struct timespec t_open_start;
	struct timespec t_mark;
	struct timespec t_now;
	bool time_create = false;
	uint64_t ns_parent = 0;
	uint64_t ns_lookup = 0;

	if (cd->ot == NULL) {
		return NFS4ERR_NOTSUPP;
}

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	switch (a->claim) {
	case CLAIM_NULL:
		clock_gettime(CLOCK_MONOTONIC, &t_open_start);
		t_mark = t_open_start;

		/* current_fh must be a directory. */
		st = compound_inode_get(cd, cd->current_fh.fileid, &inode);
		clock_gettime(CLOCK_MONOTONIC, &t_now);
		ns_parent = ts_delta_ns(&t_mark, &t_now);
		t_mark = t_now;
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
}
		if (inode.type != MDS_FTYPE_DIR) {
			return NFS4ERR_NOTDIR;
}

		/* Optimistic create-first for UNCHECKED4.
		 *
		 * CREATEMODE_UNCHECKED4 semantics (RFC 8881 S18.16.3): if
		 * the file does not exist, create it; if it does, open the
		 * existing one.  No verifier, no error on collision.  For
		 * mdtest-style workloads with unique filenames the pre-lookup
		 * is a pure ~220 us NDB miss-read -- the dirent negative cache
		 * never helps because the name is first-seen.
		 *
		 * Skip the pre-lookup and go straight to ns_create; the
		 * dirent PK already enforces uniqueness via insertTuple, so
		 * a collision returns MDS_ERR_EXISTS which we then resolve
		 * with a deferred lookup (see goto open_existing below).
		 *
		 * GUARDED4, EXCLUSIVE4, EXCLUSIVE4_1 and non-create opens
		 * keep the lookup-first flow because their semantics depend
		 * on detecting the pre-existing file up front (verifier
		 * replay, EXIST error return).
		 */
		bool opt_create_first = (a->create &&
			a->createmode == CREATEMODE_UNCHECKED4 &&
			cd->cat != NULL);

		if (opt_create_first) {
			/* Skip NDB pre-lookup; pretend NOTFOUND so we fall
			 * into the create branch.  A collision is handled
			 * when cat_create returns MDS_ERR_EXISTS below. */
			st = MDS_ERR_NOTFOUND;
			ns_lookup = 0;
		} else {
			st = compound_lookup_local_child(cd,
				cd->current_fh.fileid, a->name, &inode);
			clock_gettime(CLOCK_MONOTONIC, &t_now);
			ns_lookup = ts_delta_ns(&t_mark, &t_now);
			t_mark = t_now;
		}
		if (st == MDS_ERR_NOTFOUND) {
			if (!a->create) {
				return NFS4ERR_NOENT;
}
			/*
			 * Confirmed create path.  Commit the deferred
			 * parent_get / lookup samples and arm the rest
			 * of the phase timers.
			 */
			time_create = true;
			atomic_fetch_add(
				&g_branch_metrics.open_create_parent_ns_sum,
				ns_parent);
			atomic_fetch_add(
				&g_branch_metrics.open_create_parent_count, 1);
			atomic_fetch_add(
				&g_branch_metrics.open_create_lookup_ns_sum,
				ns_lookup);
			atomic_fetch_add(
				&g_branch_metrics.open_create_lookup_count, 1);
			/* Freeze guard before creating. */
			nst = check_subtree_frozen(cd);
			if (nst != NFS4_OK) {
				return nst;
}
			nst = check_repl_health(cd);
			if (nst != NFS4_OK) {
				return nst;
}
			/* Effective UID/GID: fall back to AUTH_SYS creds. */
			uint64_t eff_uid = a->uid;
			uint64_t eff_gid = a->gid;
			if (eff_uid == 0 && cd->cred_uid != 0) {
				eff_uid = cd->cred_uid;
			}
			if (eff_gid == 0 && cd->cred_gid != 0) {
				eff_gid = cd->cred_gid;
			}
			/*
			 * Phase C / Step 5 of docs/hpc-nto1-plan.md -- detect
			 * whether the parent is HPC-Shared.  We must re-fetch
			 * here because the opt_create_first short-circuit may
			 * have left `inode` holding the parent OR a stale
			 * lookup result; cat_getattr is cheap (snapshot or
			 * inode-cache hit on the parent FH the caller just
			 * PUTFH'd) and avoids reasoning about which branch
			 * left what in `inode`.
			 */
			bool hpc_wide_path = false;
			if (cd->prealloc != NULL && cd->cat != NULL) {
				struct mds_inode parent_inode;
				if (cat_getattr(cd, cd->current_fh.fileid,
						&parent_inode) == MDS_OK &&
				    (parent_inode.flags &
				     MDS_IFLAG_HPC_SHARED) != 0) {
					hpc_wide_path = true;
				}
			}

			/*
			 * Capture DS ID before create consumes prealloc.
			 * Skipped for the HPC wide path -- that path bypasses
			 * the per-DS prealloc rings entirely (it issues a
			 * synchronous batch placement) and the
			 * stripe_cached LAYOUTGET fast-path is irrelevant
			 * because the wide stripe map is already persisted
			 * by the helper before op_open returns.
			 */
			uint32_t pre_create_ds_id = 0;
			uint32_t pre_create_stripe_unit = 0;
			if (!hpc_wide_path && cd->prealloc != NULL) {
				struct mds_ds_map_entry peek_e;
				if (ds_prealloc_peek(cd->prealloc,
						     &peek_e,
						     &pre_create_stripe_unit) == 0) {
					pre_create_ds_id = peek_e.ds_id;
				}
			}

			/* Create the file. */
			clock_gettime(CLOCK_MONOTONIC, &t_mark);
			if (hpc_wide_path) {
				/*
				 * Phase C / Step 5 wide pre-warm.  The helper
				 * persists inode + dirent + wide stripe map in
				 * one logical sequence and rolls back
				 * (best-effort) on any internal failure, so the
				 * caller treats the result the same as any
				 * other ns_create call.
				 */
				st = op_open_hpc_wide_create(cd, a,
					eff_uid, eff_gid, &inode);
			} else if (cd->cq != NULL) {
				struct commit_op cop;
				memset(&cop, 0, sizeof(cop));
				cop.type = COMMIT_OP_CREATE;
				cop.args.create.parent_fileid = cd->current_fh.fileid;
				(void)snprintf(cop.args.create.name,
					       sizeof(cop.args.create.name),
					       "%s", a->name);
				cop.args.create.type = MDS_FTYPE_REG;
				cop.args.create.mode = a->mode;
				cop.args.create.uid = eff_uid;
				cop.args.create.gid = eff_gid;
				cop.args.create.prealloc = cd->prealloc;
				cop.args.create.out = &inode;

				/*
				 * Fused CREATE+LAYOUTGET: if a LAYOUTGET
				 * follows in this compound (past benign
				 * metadata ops), ask the CQ writer to
				 * persist the layout grant in the same txn.
				 *
				 * Scoped to single-shard CQ path only:
				 * root_db == db (layout_state goes to same
				 * catalogue transaction as the CREATE).
				 */
				if (cd->prealloc != NULL &&
				    cd->ops != NULL) {
					uint32_t scan;

					for (scan = cd->op_index + 1;
					     scan < cd->op_count; scan++) {
						enum nfs_opnum4 nop =
							cd->ops[scan].opnum;
						if (nop == OP_LAYOUTGET) {
							const struct nfs4_arg_layoutget *lg =
								&cd->ops[scan].arg.layoutget;
							cop.args.create.layout_pregrant = true;
							cop.args.create.layout_clientid = cd->clientid;
							cop.args.create.layout_iomode = lg->iomode;
							cop.args.create.layout_offset = lg->offset;
							cop.args.create.layout_length = lg->length;
							cop.args.create.skip_transient_ndb =
								cd->skip_transient_ndb;
							if (pre_create_ds_id != 0) {
								/* Borrow the stack-local pre_create_ds_id
								 * for the synchronous commit_queue_submit
								 * below -- see the lifetime contract
								 * comment on commit_queue_submit(). */
								cop.args.create.layout_ds_ids =
									&pre_create_ds_id;
								cop.args.create.layout_ds_count = 1;
							}
							make_layout_stateid(cd->mds_id,
							&cop.args.create.layout_stateid);
							break;
						}
						if (nop != OP_GETFH &&
						    nop != OP_GETATTR &&
						    nop != OP_ACCESS) {
							break;
						}
					}
				}

				st = commit_queue_submit(cd->cq, &cop);

				/* Propagate successful pre-grant. */
				if (st == MDS_OK &&
				    cop.args.create.layout_pregrant_ok) {
					cd->layout_pregranted = true;
					cd->layout_pregrant_fileid =
						inode.fileid;
					cd->layout_pregrant_stateid =
						cop.args.create.layout_stateid;
				}
			} else {
				/*
				 * Direct-catalogue path (cd->cq == NULL).
				 * Fused shim with per-op diagnostic tracing
				 * enabled -- see docs/benchmark-schema-v6.md.
				 */
				uint64_t lg_clientid = 0;
				uint32_t lg_iomode = 0;
				uint64_t lg_offset = 0, lg_length = 0;
				struct nfs4_stateid lg_sid;
				bool do_fused = false;

				memset(&lg_sid, 0, sizeof(lg_sid));
				if (cd->prealloc != NULL && cd->ops != NULL) {
					uint32_t scan;
					for (scan = cd->op_index + 1;
					     scan < cd->op_count; scan++) {
						enum nfs_opnum4 nop =
							cd->ops[scan].opnum;
						if (nop == OP_LAYOUTGET) {
							const struct nfs4_arg_layoutget *lg =
								&cd->ops[scan].arg.layoutget;
							lg_clientid = cd->clientid;
							lg_iomode = lg->iomode;
							lg_offset = lg->offset;
							lg_length = lg->length;
							make_layout_stateid(cd->mds_id,
									    &lg_sid);
							do_fused = true;
							break;
						}
						if (nop != OP_GETFH &&
						    nop != OP_GETATTR &&
						    nop != OP_ACCESS) {
							break;
						}
					}
				}

				if (do_fused) {
					bool layout_ok = false;
					st = catalogue_rondb_ns_create_with_layout(
						cd->cat, cd->current_fh.fileid,
						a->name, MDS_FTYPE_REG,
						a->mode, eff_uid, eff_gid,
						cd->prealloc, &inode,
						lg_clientid, lg_iomode,
						lg_offset, lg_length,
						&lg_sid, cd->mds_id,
						&layout_ok);
					if (st == MDS_OK && layout_ok) {
						cd->layout_pregranted = true;
						cd->layout_pregrant_fileid =
							inode.fileid;
						cd->layout_pregrant_stateid =
							lg_sid;
					}
				} else {
					st = cat_create(cd,
						cd->current_fh.fileid,
						a->name, MDS_FTYPE_REG,
						a->mode, eff_uid, eff_gid,
						cd->prealloc, &inode);
				}
			}
			clock_gettime(CLOCK_MONOTONIC, &t_now);
			atomic_fetch_add(
				&g_branch_metrics.open_create_ns_create_ns_sum,
				ts_delta_ns(&t_mark, &t_now));
			atomic_fetch_add(
				&g_branch_metrics.open_create_ns_create_count, 1);
			t_mark = t_now;
			if (st == MDS_ERR_EXISTS && opt_create_first) {
				/* File existed (or a race lost to another
				 * creator).  Do the deferred lookup now and
				 * fall through to the open-existing path.
				 * Still cheaper than the unconditional
				 * pre-lookup for the common new-file case. */
				st = compound_lookup_local_child(cd,
					cd->current_fh.fileid,
					a->name, &inode);
				if (st != MDS_OK) {
					return mds_status_to_nfs4(st);
				}
				goto open_existing;
			}
			if (st != MDS_OK) {
				return mds_status_to_nfs4(st);
			}
			/* EXCLUSIVE4/EXCLUSIVE4_1: persist verifier for
			 * replay detection.  If the file was created with
			 * mode 0 (EXCLUSIVE4 does not carry attrs), set
			 * an initial mode of 0600 so the Linux NFS client
			 * attribute cache allows the creator to write and
			 * the subsequent SETATTR to set the real mode. */
			if (a->createmode == CREATEMODE_EXCLUSIVE4 ||
			    a->createmode == CREATEMODE_EXCLUSIVE4_1) {
				inode.create_verf = a->create_verf;
				if (inode.mode == 0) {
					inode.mode = 0600;
				}
				(void)cat_setattr(cd, inode.fileid,
						     &inode,
						     MDS_ATTR_MODE);
			}
			just_created = true;
			/* Phase B HPC-Shared: if the parent directory has
			 * MDS_IFLAG_HPC_SHARED set, propagate the bit to
			 * the new child so any later OPEN/LAYOUTGET on it
			 * picks up the HPC fast paths.  Best-effort --
			 * inheritance failure does not abort the create.
			 * cd->current_fh still points at the parent dir at
			 * this point in op_open(CREATE).
			 *
			 * Phase C / Step 5 note: the wide-create helper
			 * already sets MDS_IFLAG_HPC_SHARED on the new
			 * inode, so this call is a no-op fast path on the
			 * HPC branch (the helper checks the flag and bails
			 * before any catalogue write). */
			hpc_shared_inherit_from_parent(cd,
				cd->current_fh.fileid, &inode);
			clock_gettime(CLOCK_MONOTONIC, &t_mark);
			/*
			 * Skip async DS-prepare for the HPC wide path:
			 * every per-stripe DS file handle was captured
			 * synchronously during op_open_hpc_wide_create, so
			 * there is no DS_PENDING flag to clear and no
			 * background work for the prepare worker to do.
			 */
			if (!hpc_wide_path) {
				compound_maybe_enqueue_ds_prepare(cd, &inode);
			}
			clock_gettime(CLOCK_MONOTONIC, &t_now);
			atomic_fetch_add(
				&g_branch_metrics.open_create_ds_prepare_ns_sum,
				ts_delta_ns(&t_mark, &t_now));
			atomic_fetch_add(
				&g_branch_metrics.open_create_ds_prepare_count, 1);

			/* Stash stripe info for LAYOUTGET fast path.
			 * The DS ID was captured before create consumed
			 * the prealloc entry (see pre_create_ds_id above). */
			if (pre_create_ds_id != 0 &&
			    (inode.flags & MDS_IFLAG_DS_PENDING)) {
				cd->stripe_cached = true;
				cd->stripe_cached_fileid = inode.fileid;
				cd->stripe_cached_ds_id = pre_create_ds_id;
				cd->stripe_cached_stripe_unit =
					pre_create_stripe_unit;
			}

			/* Invalidate dirent cache for the new entry. */
			compound_dirent_invalidate(cd,
				cd->current_fh.fileid, a->name);

			/* DS FH capture is deferred to LAYOUTGET time.
			 * This keeps CREATE fast (~5ms RonDB only).
			 * LAYOUTGET backfill will capture FHs on demand. */

			/* Seed current_inode + global cache so the
			 * NFS client's immediate GETATTR hits the
			 * cache instead of a full NDB round-trip.
			 * NOTE: current_fh still points to parent here;
			 * OPEN epilogue (below) updates it to child.
			 * compound_inode_get checks current_fh.fileid ==
			 * fileid, so this won't match until after the
			 * FH update.  Mark valid=true -- the child inode
			 * is authoritative (just created). */
			cd->current_inode = inode;
			cd->current_inode_valid = true;
			compound_ro_txn_reset(cd);
			if (cd->icache != NULL) {
				inode_cache_put(cd->icache, &inode);
			}
		} else if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		} else {
open_existing:
			/* File exists. */
			if (a->create) {
				if (a->createmode == CREATEMODE_EXCLUSIVE4 ||
				    a->createmode == CREATEMODE_EXCLUSIVE4_1) {
					/*
					 * RFC 8881 S18.16.3: If file exists and
					 * verifier matches, this is a replay --
					 * succeed.  If mismatch, reject.
					 */
					if (inode.create_verf != a->create_verf) {
						return NFS4ERR_EXIST;
					}
					/* Replay: fall through to open the existing file. */
				} else if (a->createmode == CREATEMODE_GUARDED4) {
					return NFS4ERR_EXIST;
				}
				/* UNCHECKED4: fall through (open existing). */
			}
		}

		/* Target must be a regular file. */
		if (inode.type != MDS_FTYPE_REG) {
			return NFS4ERR_ISDIR;
}

		target_fid = inode.fileid;
		break;

	case CLAIM_FH:
		/* current_fh must be a regular file. */
		st = compound_inode_get(cd, cd->current_fh.fileid, &inode);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
}
		if (inode.type != MDS_FTYPE_REG) {
			return NFS4ERR_ISDIR;
}

		target_fid = inode.fileid;
		break;

	case CLAIM_PREVIOUS:
		/* RFC 8881 S9.11: reclaim previously held open during grace. */
		if (!grace_is_active()) {
			return NFS4ERR_NO_GRACE;
}
		if (!grace_client_is_recovering(cd->clientid)) {
			return NFS4ERR_NO_GRACE;
}

		/* CLAIM_PREVIOUS uses current_fh (fileid) directly. */
		st = compound_inode_get(cd, cd->current_fh.fileid, &inode);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
}
		if (inode.type != MDS_FTYPE_REG) {
			return NFS4ERR_ISDIR;
}

		target_fid = inode.fileid;
		break;

	default:
		return NFS4ERR_NOTSUPP;
	}

	/* During grace, non-reclaim opens are rejected. */
	if (grace_is_active() && a->claim != CLAIM_PREVIOUS) {
		return NFS4ERR_GRACE;
}


	/*
	 * POSIX permission check: verify the caller has the requested
	 * access (read / write) to the target file based on AUTH_SYS
	 * credentials and the inode mode bits.
	 *
	 * Skip for just-created files: the creator always has access
	 * (RFC 8881 S18.16.3).  EXCLUSIVE4 creates defer mode to a
	 * subsequent SETATTR, so the file has mode 0 at this point.
	 */
	if (!just_created && cd->cred_uid != 0) {
		uint32_t omode = inode.mode;
		uint32_t operm;

		if (cd->cred_uid == (uint32_t)inode.uid) {
			operm = (omode >> 6) & 7;
		} else if (cd->cred_gid == (uint32_t)inode.gid) {
			operm = (omode >> 3) & 7;
		} else {
			operm = omode & 7;
		}

		if ((a->share_access & OPEN4_SHARE_ACCESS_READ) && !(operm & 4)) {
			return NFS4ERR_ACCES;
		}
		if ((a->share_access & OPEN4_SHARE_ACCESS_WRITE) && !(operm & 2)) {
			return NFS4ERR_ACCES;
		}
	}

	/* Allocate open state + check share conflicts. */
	if (time_create) {
		clock_gettime(CLOCK_MONOTONIC, &t_mark);
	}
	rc = open_state_open(cd->ot, cd->clientid,
			     a->open_owner, a->open_owner_len,
			     target_fid,
			     a->share_access, a->share_deny,
			     &r->stateid);
	/*
	 * RFC 8881 §8.4.3 courtesy-client support: if the OPEN hit a
	 * share conflict, check whether the conflicting open state
	 * belongs to a client whose lease has expired.  If so, revoke
	 * the expired state and retry once.  This lets a new client
	 * proceed without waiting for the background reaper.
	 * Pynfs COUR2/3/5/6.
	 */
	if (rc == -1 && cd->st != NULL) {
		if (open_state_revoke_expired_for_file(
			cd->ot, cd->st, target_fid) > 0) {
			rc = open_state_open(cd->ot, cd->clientid,
					     a->open_owner,
					     a->open_owner_len,
					     target_fid,
					     a->share_access,
					     a->share_deny,
					     &r->stateid);
		}
	}
	if (time_create) {
		clock_gettime(CLOCK_MONOTONIC, &t_now);
		atomic_fetch_add(
			&g_branch_metrics.open_create_state_open_ns_sum,
			ts_delta_ns(&t_mark, &t_now));
		atomic_fetch_add(
			&g_branch_metrics.open_create_state_open_count, 1);
	}
	switch (rc) {
	case 0:  break;
	case -1: return NFS4ERR_SHARE_DENIED;
	case -2: return NFS4ERR_RESOURCE;
	default: return NFS4ERR_SERVERFAULT;
	}

	/*
	 * Note: per RFC 8881 S18.51, the client signals completion of
	 * all reclaims via RECLAIM_COMPLETE -- not per individual OPEN.
	 */

	/*
	 * Delegation conflict-recall + conditional grant
	 * (RFC 8881 S10.4 / S18.16.3).
	 *
	 * Step 1 -- recall any other-client delegations that
	 * conflict with this OPEN.  deleg_recall_file() snapshots
	 * each holder's backchannel under the session-table lock,
	 * sends CB_RECALL on a dup'd fd, then revokes the entry
	 * regardless of CB outcome.  Best-effort: caller proceeds
	 * even if some clients are unreachable.
	 *
	 * Step 2 -- conditionally grant a delegation back to the
	 * requesting client.  Honors RFC 8881 S18.16.3 WANT_*
	 * hints carried in share_access bits 8-11:
	 *   WANT_NO_DELEG    -> r->delegation_type stays NONE
	 *                      (pynfs DELEG4 testNoDeleg gate).
	 *   WANT_READ_DELEG  -> grant READ deleg when share_access
	 *                      includes READ.
	 *   WANT_WRITE_DELEG -> grant WRITE deleg when share_access
	 *                      includes WRITE.
	 *   WANT_ANY_DELEG / WANT_NO_PREFERENCE / unset -> server
	 *                      picks based on share_access.
	 *   WANT_CANCEL      -> treated as NO_DELEG (we don't track
	 *                      pending hints, so cancelling one
	 *                      degenerates to don't-grant).
	 *
	 * Additional preconditions (all must hold):
	 *   (a) cd->dt != NULL  -- deleg table wired,
	 *   (b) cd->st != NULL  -- session table wired so a future
	 *       recall can deliver CB_RECALL,
	 *   (c) deleg_check_conflict reports no residual holder
	 *       (Step 1 may not have cleared everything on a
	 *       no-backchannel holder).
	 *
	 * Without (b) we degrade to OPEN_DELEGATE_NONE: an
	 * unrecallable delegation is a deadlock waiting to happen.
	 * The encoder (xdr_ops_core.c::encode_res_open) emits the
	 * full open_delegation4 body when r->delegation_type !=
	 * OPEN_DELEGATE_NONE.
	 */
	r->delegation_type = OPEN_DELEGATE_NONE;
	/*
	 * Default decline reason for v4.1+ NONE_EXT (RFC 8881 S18.16.4).
	 * Zero == WND4_NOT_WANTED, so this is also what compound_process()
	 * leaves after its memset of the result slot; the explicit
	 * assignments below override it on paths where a more specific
	 * reason applies (CONTENTION on residual conflict, RESOURCE on
	 * a missing recall path).  All NONE_EXT tails (will_push /
	 * will_signal) stay at false because we never promise the client
	 * a future push or avail-signal.
	 */
	r->none_reason = WND4_NOT_WANTED;

	if (cd->dt != NULL) {
		/* Step 1: recall conflicting delegations from OTHER
		 * clients.  Returns recall-count; we ignore it because
		 * the only thing the caller cares about is that no
		 * conflicting other-client grant remains in memory
		 * after this returns. */
		(void)deleg_recall_file(cd->dt, target_fid,
					 cd->clientid, 0);

		/* Step 2: try to grant.  Skip if no session table is
		 * wired (no recall path available for the new grant)
		 * or if a residual conflict somehow survives the
		 * recall pass. */
		if (cd->st != NULL) {
			bool has_conflict = false;
			uint32_t deleg_type = OPEN_DELEGATE_NONE;
			uint32_t want_hint = a->share_access &
				OPEN4_SHARE_ACCESS_WANT_DELEG_MASK;

			/* Hard-stop: client explicitly asked for no
			 * delegation.  Skip the grant entirely.
			 * RFC 8881 S18.16.4: WND4_NOT_WANTED is the
			 * correct decline reason for both WANT_NO_DELEG
			 * and WANT_CANCEL. */
			if (want_hint == OPEN4_SHARE_ACCESS_WANT_NO_DELEG ||
			    want_hint == OPEN4_SHARE_ACCESS_WANT_CANCEL) {
				r->none_reason = WND4_NOT_WANTED;
				goto deleg_grant_done;
			}

			/*
			 * RFC 8881 §10.4: do not grant a read delegation
			 * if another client has the file open for write.
			 * Pynfs DELEG9 testWriteOpenvsReadDeleg.
			 */
			if (cd->ot != NULL &&
			    open_state_has_other_writer(cd->ot,
						       target_fid,
						       cd->clientid)) {
				has_conflict = true;
				r->none_reason = WND4_CONTENTION;
				goto deleg_grant_done;
			}
			if (deleg_check_conflict(cd->dt, target_fid,
						 cd->clientid,
						 &has_conflict) == 0 &&
			    !has_conflict) {
				uint32_t basic = a->share_access &
					OPEN4_SHARE_ACCESS_BASIC_MASK;
				uint32_t want_read = basic &
					OPEN4_SHARE_ACCESS_READ;
				uint32_t want_write = basic &
					OPEN4_SHARE_ACCESS_WRITE;

				switch (want_hint) {
				case OPEN4_SHARE_ACCESS_WANT_READ_DELEG:
					/* Client asked for READ deleg.  Grant
					 * iff share_access includes READ. */
					if (want_read) {
						deleg_type = OPEN_DELEGATE_READ;
					}
					break;
				case OPEN4_SHARE_ACCESS_WANT_WRITE_DELEG:
					/* Client asked for WRITE deleg.  Grant
					 * iff share_access includes WRITE. */
					if (want_write) {
						deleg_type = OPEN_DELEGATE_WRITE;
					}
					break;
				case OPEN4_SHARE_ACCESS_WANT_ANY_DELEG:
				case OPEN4_SHARE_ACCESS_WANT_NO_PREFERENCE:
				default:
					/* Server picks based on share_access. */
					if (want_write && !want_read) {
						deleg_type = OPEN_DELEGATE_WRITE;
					} else if (want_read && !want_write) {
						deleg_type = OPEN_DELEGATE_READ;
					} else if (want_read && want_write) {
						/* Mixed RW OPEN: WRITE deleg gives
						 * exclusive caching authority. */
						deleg_type = OPEN_DELEGATE_WRITE;
					}
					break;
				}

				if (deleg_type != OPEN_DELEGATE_NONE) {
					struct nfs4_stateid sid;

					if (deleg_grant(cd->dt, cd->clientid,
							target_fid, deleg_type,
							NULL /* session looked
							       up at recall time
							       via cd->dt->st */,
							&sid) == 0) {
						r->delegation_type = deleg_type;
						r->deleg_stateid = sid;
					} else {
						/* Grant failed (table full,
						 * persistence error): the
						 * server lacked the resource
						 * to issue the deleg. */
						r->none_reason = WND4_RESOURCE;
					}
				}
				/* deleg_type == NONE here means we
				 * decided not to grant (want hint vs.
				 * share_access mismatch).  Leave
				 * none_reason at the WND4_NOT_WANTED
				 * default. */
			} else {
				/* Either the conflict probe failed or a
				 * residual other-client delegation
				 * survived the recall pass (e.g. the
				 * holder's backchannel was unreachable).
				 * RFC 8881 S18.16.4 -- WND4_CONTENTION. */
				r->none_reason = WND4_CONTENTION;
			}
		} else {
			/* Session table not wired: even if we granted, we
			 * could never deliver a CB_RECALL on conflict.
			 * Treat as a server resource shortage so the
			 * client knows it's a transient condition rather
			 * than a permanent server policy. */
			r->none_reason = WND4_RESOURCE;
		}
	}
	/* cd->dt == NULL: deleg subsystem is disabled.  none_reason
	 * stays at WND4_NOT_WANTED -- "server not granting" is the
	 * least misleading reason for a v4.1+ client. */
deleg_grant_done:
	/*
	 * RFC 8881 S18.16.4 wire-form selection.
	 *
	 * v4.1+ sessions: if we did not grant a delegation, upgrade
	 * the result discriminator from OPEN_DELEGATE_NONE (legacy
	 * void) to OPEN_DELEGATE_NONE_EXT so the encoder emits the
	 * open_none_delegation4 body carrying the populated
	 * none_reason.  This is what pynfs DELEG4 (testNoDeleg) and
	 * the v4.1+ Linux client expect when the open carried any
	 * WANT_* hint or hit a conflict.
	 *
	 * v4.0 sessions stay on bare NONE -- discriminator value 4 is
	 * not part of the v4.0 open_delegation4 union, so emitting
	 * it would corrupt the wire form for those clients.
	 */
	if (r->delegation_type == OPEN_DELEGATE_NONE &&
	    cd->minorversion >= 1) {
		r->delegation_type = OPEN_DELEGATE_NONE_EXT;
	}

	/* Populate result and update current_fh. */
	r->inode = inode;
	cd->current_fh.fileid = target_fid;
	/* FH-encoded subtree ownership (suggested by Gaurav Gangalwar's
	 * code review): every claim path on op_open ends up with `inode`
	 * fully populated for the target file (CLAIM_NULL after
	 * lookup-or-create, CLAIM_FH after compound_inode_get,
	 * CLAIM_PREVIOUS after the reclaim read).  Stamp the local mds_id
	 * and copy the inode's generation so the GETFH that typically
	 * follows OPEN emits the v1 wire form with full routing /
	 * freshness metadata. */
	cd->current_fh.owner_mds_id = cd->mds_id;
	cd->current_fh.generation = inode.generation;
	cd->current_fh_set = true;
	/* Seed snapshot with the opened file's inode. */
	cd->current_inode = inode;
	cd->current_inode_valid = true;

	/* Update path tracking for CLAIM_NULL. */
	if (a->claim == CLAIM_NULL && cd->current_path[0] != '\0') {
		size_t plen = strlen(cd->current_path);
		int n;

		if (plen == 1 && cd->current_path[0] == '/') {
			n = snprintf(cd->current_path,
				     sizeof(cd->current_path),
				     "/%s", a->name);
		} else {
			n = snprintf(cd->current_path + plen,
				     sizeof(cd->current_path) - plen,
				     "/%s", a->name);
		}
		/* Truncation is non-fatal; path tracking is best-effort. */
		if (n < 0) {
			cd->current_path[0] = '\0';
}
		resolve_and_apply_shard(cd, cd->current_path);
	}

	if (time_create) {
		clock_gettime(CLOCK_MONOTONIC, &t_now);
		atomic_fetch_add(
			&g_branch_metrics.open_create_total_ns_sum,
			ts_delta_ns(&t_open_start, &t_now));
		atomic_fetch_add(
			&g_branch_metrics.open_create_total_count, 1);
	}
	return NFS4_OK;
}

enum nfs4_status op_close(struct compound_data *cd,
				 const struct nfs4_op *op,
				 struct nfs4_result *res)
{
	const struct nfs4_arg_close *a = &op->arg.close;
	struct nfs4_res_close *r = &res->res.close;
	struct nfs4_stateid sid_resolved;
	enum nfs4_status nst;
	int rc;

	if (cd->ot == NULL) {
		return NFS4ERR_NOTSUPP;
}

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/* RFC 8881 S16.2.4 -- resolve CURRENT_STATEID4 marker.  Pynfs
	 * CSID1 testOpenAndClose, CSID3 testOpenWriteClose, CSID10
	 * testOpenSaveFHLookupRestoreFHClose all bundle CLOSE with the
	 * marker right after an OPEN.  CSID5 / CSID6 expect BAD_STATEID
	 * because LOOKUP / a missing producer leaves current_stateid
	 * unset -- compound_resolve_stateid returns BAD_STATEID for both. */
	nst = compound_resolve_stateid(cd, &a->stateid, &sid_resolved);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* QA Phase 4 + QA review Blocker 4 -- final-close drain hook.
	 *
	 * Capture the fileid before the close call so we can decide
	 * whether to force-flush the LAYOUTCOMMIT aggregator's bucket
	 * for this file.  open_state_close releases the state record;
	 * we detect "this was the last opener" by querying the table
	 * AFTER close: open_state_file_has_writers returns 0 iff no
	 * other open state on the file remains.  In that case we
	 * force-flush so the persisted size / mtime reflect the last
	 * write before any later observer (a stat, a cross-MDS read)
	 * sees a stale row.
	 *
	 * Best-effort by design.  CLOSE cannot meaningfully be retried
	 * by the client (the file is already closed; the kernel will
	 * not re-issue CLOSE), so we DO NOT propagate flush failure
	 * as NFS4ERR_DELAY here.  Instead we log a WARN so operators
	 * see the drift, and rely on the periodic timer to retry on
	 * its next tick (default 200 ms).  Until that retry succeeds,
	 * a `stat` against the just-closed file may briefly show a
	 * size lagging the last write -- documented in
	 * docs/hpc-shared-files.md "Limits".  STRICT GETATTR (the
	 * other path that surfaces the aggregate) DOES propagate
	 * NFS4ERR_DELAY on flush failure (Blocker 4) because the
	 * client can retry GETATTR; the asymmetry is intentional.
	 * NULL-safe (lcommit_agg unset on non-HPC builds / config). */
	uint64_t close_fileid = cd->current_fh.fileid;

	rc = open_state_close(cd->ot, cd->clientid,
			      &sid_resolved, &r->stateid);
	switch (rc) {
	case 0:  break;
	case -4: return NFS4ERR_OLD_STATEID;
	default: return NFS4ERR_BAD_STATEID;
	}

	if (cd->lcommit_agg != NULL && close_fileid != 0 &&
	    open_state_file_has_writers(cd->ot, close_fileid) == 0) {
		int frc = layout_commit_aggregator_flush_fileid(
			cd->lcommit_agg, close_fileid);
		if (frc < 0) {
			(void)fprintf(stderr,
				"WARN: HPC LAYOUTCOMMIT aggregator flush "
				"failed at final CLOSE of fileid=%llu; "
				"persisted size/mtime will lag until the "
				"next periodic flush\n",
				(unsigned long long)close_fileid);
		}
	}

	return NFS4_OK;
}


/* -----------------------------------------------------------------------
 * DELEGRETURN handler (RFC 8881 S18.8)
 * ----------------------------------------------------------------------- */

enum nfs4_status op_delegreturn(struct compound_data *cd,
			       const struct nfs4_op *op,
			       struct nfs4_result *res)
{
	struct nfs4_stateid sid_resolved;
	enum nfs4_status nst;

	(void)res;

	/* RFC 8881 S16.2.4 -- resolve CURRENT_STATEID4 marker.  The
	 * decoder reuses the close arg union slot for DELEGRETURN's
	 * stateid (no separate arg struct), so the magic value can
	 * land here too. */
	nst = compound_resolve_stateid(cd, &op->arg.close.stateid,
				       &sid_resolved);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* File delegation stateid is self-identifying; current_fh is not
	 * strictly required per RFC 8881 S18.8.  Accept gracefully
	 * even if the stateid is not found (expired or already returned). */
	if (cd->dt != NULL) {
		if (deleg_return(cd->dt, &sid_resolved, cd->clientid) == 0) {
			return NFS4_OK;
		}
	}

	/*
	 * Fallback: the stateid may belong to a directory delegation
	 * granted by Phase 8b's dir_deleg_table.  Returning an
	 * already-revoked or unknown stateid is not an error per RFC
	 * 8881 S10.11.1, so we swallow the "not found" result.
	 */
	if (cd->ddt != NULL) {
		(void)dir_deleg_return(cd->ddt, &sid_resolved, cd->clientid);
	}

	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * OPEN_DOWNGRADE handler (RFC 8881 S18.18)
 *
 * Reduces the share access/deny on an existing open stateid.
 * For a metadata-only pNFS server, this validates the stateid and
 * accepts the downgrade.  If the new access is zero (invalid per RFC),
 * return NFS4ERR_INVAL.
 * ----------------------------------------------------------------------- */

enum nfs4_status op_open_downgrade(struct compound_data *cd,
				   const struct nfs4_op *op,
				   struct nfs4_result *res)
{
	const struct nfs4_arg_open_downgrade *a = &op->arg.open_downgrade;
	struct nfs4_stateid sid_resolved;
	enum nfs4_status nst;

	if (cd->ot == NULL) {
		return NFS4ERR_NOTSUPP;
	}

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* share_access must be non-zero (RFC 8881 S18.18). */
	if (a->share_access == 0) {
		return NFS4ERR_INVAL;
	}

	/* RFC 8881 S16.2.4 -- resolve CURRENT_STATEID4 marker. */
	nst = compound_resolve_stateid(cd, &a->stateid, &sid_resolved);
	if (nst != NFS4_OK) {
		return nst;
	}

	/*
	 * Validate the open stateid.  For a metadata-only MDS, accepting
	 * the downgrade and bumping the seqid is sufficient -- actual
	 * I/O share enforcement is at the data-server layer.
	 */
	{
		int rc;

		rc = open_state_downgrade(cd->ot, cd->clientid,
					 &sid_resolved,
					 a->share_access,
					 a->share_deny,
					 &res->res.close.stateid);
		if (rc == 0) {
			return NFS4_OK;
		}
		if (rc == -5) {
			return NFS4ERR_INVAL;
		}
		switch (rc) {
		case -4: return NFS4ERR_OLD_STATEID;
		default: return NFS4ERR_BAD_STATEID;
		}
	}
}
/* -----------------------------------------------------------------------
 * OPENATTR handler (RFC 8881 S18.17)
 * ----------------------------------------------------------------------- */

enum nfs4_status op_openattr(struct compound_data *cd,
				    const struct nfs4_op *op,
				    struct nfs4_result *res)
{
	struct mds_inode inode;
	enum nfs4_status nst;
	enum mds_status st;

	(void)op;
	(void)res;
	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/* Cannot OPENATTR on an xattr-namespace handle. */
	if (is_xattr_fh(cd->current_fh.fileid)) {
		return NFS4ERR_INVAL;
}

	/* Verify the file exists and is a regular file. */
	st = compound_inode_get(cd, cd->current_fh.fileid, &inode);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
}
	if (inode.type != MDS_FTYPE_REG) {
		return NFS4ERR_INVAL;
}

	/* Set current_fh to the xattr namespace for this file. */
	cd->current_fh.fileid = inode.fileid | XATTR_FH_FLAG;
	/* FH-encoded subtree ownership: xattr namespace handles wrap
	 * a base fileid that lives on this MDS -- stamp owner_mds_id so a
	 * subsequent GETFH on the xattr handle still routes correctly.
	 * Generation stays 0: the synthetic xattr-namespace handle is
	 * not an inode in its own right and has no on-disk generation. */
	cd->current_fh.owner_mds_id = cd->mds_id;
	cd->current_fh.generation = 0;
	cd->xattr_obj_set = false;
	memset(cd->xattr_name, 0, sizeof(cd->xattr_name));
	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * Stateid validation for READ / WRITE (RFC 8881 S8.2)
 *
 * Anonymous stateid (seqid=0, other=all-zeros) is allowed as a bypass.
 * Otherwise the stateid must exist, belong to the current FH, and
 * have the appropriate share access for the operation.
 * ----------------------------------------------------------------------- */

static bool is_anonymous_stateid(const struct nfs4_stateid *sid)
{
	static const uint8_t zero_other[NFS4_OTHER_SIZE] = {0};

	return sid->seqid == 0 &&
	       memcmp(sid->other, zero_other, NFS4_OTHER_SIZE) == 0;
}

/*
 * RFC 8881 S16.2.4 / RFC 5661 S8.2.3 -- the special READ_BYPASS
 * stateid (seqid = 0xFFFFFFFF, other = all-1s) bypasses share-deny
 * checks for read-side I/O.  Linux NFSD's nfs4_preprocess_stateid_op
 * also accepts it for ALLOCATE / DEALLOCATE (write-side ops that
 * do not hold any client state of their own); pynfs ALLOC3
 * (testAllocateStateidOne) verifies that behaviour.  Treat it as a
 * special stateid here so the validator below skips open-state
 * lookup, share-mode, and clientid checks the way it does for the
 * anonymous (all-zero) special stateid.
 */
static bool is_read_bypass_stateid(const struct nfs4_stateid *sid)
{
	static const uint8_t ones_other[NFS4_OTHER_SIZE] = {
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	};

	return sid->seqid == 0xFFFFFFFFu &&
	       memcmp(sid->other, ones_other, NFS4_OTHER_SIZE) == 0;
}

/**
 * Validate stateid for a data I/O operation.
 *
 * @param cd              Compound context.
 * @param stateid         Stateid from the READ/WRITE args.
 * @param required_access OPEN4_SHARE_ACCESS_READ or _WRITE.
 * @return NFS4_OK if valid, an appropriate error otherwise.
 */
enum nfs4_status validate_io_stateid(
	const struct compound_data *cd,
	const struct nfs4_stateid *stateid,
	uint32_t required_access)
{
	struct nfs4_open_state os;
	struct nfs4_stateid sid_resolved;
	enum nfs4_status rsid_st;

	if (stateid == NULL) {
		return NFS4ERR_BAD_STATEID;
	}

	/*
	 * RFC 8881 S16.2.4 -- resolve CURRENT_STATEID4 marker before
	 * any other stateid logic.  This covers op_read, op_write,
	 * op_setattr, op_io_advise, op_allocate, op_deallocate, and
	 * any other caller that funnels through this validator.
	 * Pynfs CSID3 testOpenWriteClose drives the WRITE path here.
	 */
	rsid_st = compound_resolve_stateid(cd, stateid, &sid_resolved);
	if (rsid_st != NFS4_OK) {
		return rsid_st;
	}
	stateid = &sid_resolved;

	/* Test-compat mode: no open-state table -> skip validation. */
	if (cd->ot == NULL) {
		return NFS4_OK;
}

	/*
	 * Special stateids (RFC 8881 S16.2.4 / S8.2.3) bypass the
	 * full validation: anonymous (all zeros) and READ_BYPASS
	 * (all ones).  Both are accepted for the data-path ops that
	 * call this validator (READ, WRITE, SETATTR(size), ALLOCATE,
	 * DEALLOCATE), matching Linux NFSD's behaviour.
	 */
	if (is_anonymous_stateid(stateid) || is_read_bypass_stateid(stateid)) {
		return NFS4_OK;
}

	if (open_state_find(cd->ot, stateid, &os) != 0) {
		/*
		 * RFC 8881 §10.4: a delegation stateid is valid for
		 * READ even after the open is CLOSEd.  Check if
		 * the stateid belongs to a live delegation held by
		 * this client for this file.  Pynfs DELEG26.
		 *
		 * RFC 8881 §10.2.1: if the stateid belonged to a
		 * delegation that has since been revoked, return
		 * NFS4ERR_DELEG_REVOKED.  Pynfs DELEG8.
		 * (We don't currently track revoked stateids, so
		 * a revoked deleg looks like "not found" — return
		 * BAD_STATEID.  A future change can track revoked
		 * deleg stateids and return DELEG_REVOKED.)
		 */
		if (cd->dt != NULL &&
		    required_access == OPEN4_SHARE_ACCESS_READ &&
		    deleg_stateid_exists(cd->dt, stateid->other)) {
			return NFS4_OK;
		}
		/*
		 * RFC 8881 §10.2.1: if the stateid belonged to a
		 * revoked delegation, return NFS4ERR_DELEG_REVOKED.
		 * Pynfs DELEG8 testDelegRevocation.
		 */
		if (cd->dt != NULL &&
		    deleg_is_revoked(cd->dt, stateid->other)) {
			return NFS4ERR_DELEG_REVOKED;
		}
		return NFS4ERR_BAD_STATEID;
}

	/* Seqid must match current state (RFC 8881 S8.2.1). */
	if (stateid->seqid != 0 && stateid->seqid != os.stateid.seqid) {
		return NFS4ERR_BAD_STATEID;
}

	/* The owning client must match the session's client. */
	if (os.clientid != cd->clientid) {
		return NFS4ERR_BAD_STATEID;
}

	/* The stateid must belong to the file we are operating on. */
	if (os.fileid != cd->current_fh.fileid) {
		return NFS4ERR_BAD_STATEID;
}

	/* Check share access mode. */
	if ((os.share_access & required_access) == 0) {
		return NFS4ERR_OPENMODE;
}

	return NFS4_OK;
}


/* -----------------------------------------------------------------------
 * Inline-to-DS promotion
 *
 * Called when a WRITE to an inline file would exceed MDS_INLINE_DATA_MAX.
 * Migrates existing inline data to a data server, creates a stripe_map,
 * and clears the inline flag.  After promotion the caller retries via
 * the normal proxy path.
 * ----------------------------------------------------------------------- */

/**
 * Clear MDS_IFLAG_PROMOTING from a persisted inode on failure.
 *
 * Best-effort: if the cleanup txn itself fails, the flag remains
 * set and the next caller will see NFS4ERR_DELAY until an admin
 * clears it or the MDS restarts and re-reads the inode.
 */



/* -----------------------------------------------------------------------
 * Inline-to-DS promotion
 *
 * Called when a WRITE to an inline file would exceed MDS_INLINE_DATA_MAX.
 * Migrates existing inline data to a data server, creates a stripe_map,
 * and clears the inline flag.  After promotion the caller retries via
 * the normal proxy path.
 * ----------------------------------------------------------------------- */

/**
 * Clear MDS_IFLAG_PROMOTING from a persisted inode on failure.
 *
 * Best-effort: if the cleanup txn itself fails, the flag remains
 * set and the next caller will see NFS4ERR_DELAY until an admin
 * clears it or the MDS restarts and re-reads the inode.
 */
static void clear_promoting_flag(struct compound_data *cd,
				 struct mds_inode *inode)
{
	inode->flags &= ~MDS_IFLAG_PROMOTING;
	if (cd->cat != NULL) {
		(void)mds_cat_inode_put(cd->cat, NULL, inode);
	}
}

enum nfs4_status
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
promote_inline_to_ds(struct compound_data *cd, struct mds_inode *inode)
{
	uint8_t ibuf[MDS_INLINE_DATA_MAX];
	uint32_t ilen = 0;
	struct mds_ds_info *ds_list = NULL;
	struct mds_ds_map_entry *entries = NULL;
	uint32_t ds_count = 0;
	uint32_t stripe_count = 1;
	uint32_t mirror_count = 1;
	uint32_t stripe_unit = 65536;
	enum mds_status st;
	enum nfs4_status nst = NFS4ERR_IO;

	if (cd->proxy == NULL) {
		return NFS4ERR_NOTSUPP;
	}

	if (cd->cat == NULL) {
		return NFS4ERR_NOTSUPP;
	}

	/* Reject concurrent promotion attempts. */
	if (inode->flags & MDS_IFLAG_PROMOTING) {
		return NFS4ERR_DELAY;
	}

	/* Set PROMOTING flag via catalogue so concurrent callers back off. */
	inode->flags |= MDS_IFLAG_PROMOTING;
	st = mds_cat_inode_put(cd->cat, NULL, inode);
	if (st != MDS_OK) {
		inode->flags &= ~MDS_IFLAG_PROMOTING;
		return NFS4ERR_IO;
	}
	/* From here, failure paths must clear PROMOTING via goto. */

	/*
	 * Phase 1 -- Read inline payload (read-only, own txn).
	 *
	 * Read before any DS-side work so the inline data is captured
	 * while catalogue still considers the file inline.
	 */
	st = cat_inline_get(cd, cd->current_fh.fileid,
				 ibuf, sizeof(ibuf), &ilen);
	if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
		goto out_clear;
}

	/*
	 * Phase 2 -- DS placement selection (read-only catalogue).
	 */
	st = cat_ds_list(cd, &ds_list, &ds_count);
	if (st == MDS_OK && ds_count > 0) {
		struct mds_ds_info *compat = NULL;
		uint32_t compat_count = 0;
		st = ds_filter_compatible(ds_list, ds_count,
			DS_MODE_GENERIC, DS_TRANSPORT_TCP,
			&compat, &compat_count);
		free(ds_list);
		ds_list = compat;
		ds_count = compat_count;
	}
	if (st != MDS_OK || ds_count == 0) {
		free(ds_list);
		nst = NFS4ERR_NOSPC;
		goto out_clear;
	}

	/* NOLINTNEXTLINE(bugprone-implicit-widening-of-multiplication-result) */
	entries = calloc(stripe_count * mirror_count, sizeof(*entries));
	if (entries == NULL) {
		free(ds_list);
		nst = NFS4ERR_RESOURCE;
		goto out_clear;
	}

	st = placement_select(ds_list, ds_count,
			      stripe_count, mirror_count,
			      stripe_unit, entries);
	free(ds_list);
	ds_list = NULL;
	if (st != MDS_OK) {
		free(entries);
		nst = NFS4ERR_NOSPC;
		goto out_clear;
	}

	/*
	 * Phase 3 -- Ensure DS files + write data (outside any catalogue txn).
	 *
	 * If MDS crashes after the DS write but before the metadata flip
	 * below, catalogue still records the file as inline with its data
	 * intact.  The DS files become orphans, cleaned by gc-scan.
	 */
	{
		uint32_t s, m;
		for (s = 0; s < stripe_count; s++) {
			for (m = 0; m < mirror_count; m++) {
				uint32_t idx = s * mirror_count + m;
				st = mds_proxy_ensure_ds_file(
					cd->proxy,
					entries[idx].ds_id,
					cd->current_fh.fileid, s, m);
				if (st != MDS_OK) {
					free(entries);
					nst = NFS4ERR_IO;
					goto out_clear;
				}
			}
		}
	}

	if (ilen > 0) {
		/*
		 * Write inline payload to the first mirror of stripe 0.
		 * Use the direct path -- the stripe map does not exist
		 * in catalogue yet (created in Phase 4 below).
		 */
		st = mds_proxy_write_direct(cd->proxy,
					    entries[0].ds_id,
					    cd->current_fh.fileid,
					    0, 0, 0, ibuf, ilen);
		if (st != MDS_OK) {
			free(entries);
			nst = NFS4ERR_IO;
			goto out_clear;
		}
	}

	/*
	 * Phase 4 -- Metadata flip (catalogue transaction).
	 *
	 * Persist stripe_map, delete inline_data, clear flags.
	 * After commit, the file is DS-backed.
	 */
	{
		struct mds_cat_txn *cat_txn = NULL;

		st = mds_cat_txn_begin(cd->cat, MDS_CAT_TXN_WRITE,
				       &cat_txn);
		if (st != MDS_OK) {
			free(entries);
			goto out_clear;
		}

		st = mds_cat_stripe_map_put(cd->cat, cat_txn,
					    cd->current_fh.fileid,
					    stripe_count, stripe_unit,
					    mirror_count, entries);
		free(entries);
		entries = NULL;
		if (st != MDS_OK) {
			mds_cat_txn_abort(cat_txn);
			goto out_clear;
		}

		(void)mds_cat_inline_del(cd->cat, cat_txn,
					cd->current_fh.fileid);

		inode->flags &= ~(MDS_IFLAG_INLINE | MDS_IFLAG_PROMOTING);
		st = mds_cat_inode_put(cd->cat, cat_txn, inode);
		if (st != MDS_OK) {
			mds_cat_txn_abort(cat_txn);
			inode->flags |= (MDS_IFLAG_INLINE | MDS_IFLAG_PROMOTING);
			goto out_clear;
		}

		st = mds_cat_txn_commit(cat_txn);
		if (st != MDS_OK) {
			inode->flags |= (MDS_IFLAG_INLINE | MDS_IFLAG_PROMOTING);
			goto out_clear;
		}
	}

	compound_inode_invalidate(cd, cd->current_fh.fileid);
	return NFS4_OK;

out_clear:
	compound_inode_invalidate(cd, cd->current_fh.fileid);
	clear_promoting_flag(cd, inode);
	return nst;
}

/* -----------------------------------------------------------------------
 * READ / WRITE handlers
 *
 * Two modes:
 *   1. Xattr mode (xattr_obj_set == true): read/write xattr via catalogue.
 *   2. Proxy mode (proxy != NULL): stripe-aware I/O through DS mounts.
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */


/* -----------------------------------------------------------------------
 * READ / WRITE handlers
 *
 * Two modes:
 *   1. Xattr mode (xattr_obj_set == true): read/write xattr via catalogue.
 *   2. Proxy mode (proxy != NULL): stripe-aware I/O through DS mounts.
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_read(struct compound_data *cd,
				const struct nfs4_op *op,
				struct nfs4_result *res)
{
	const struct nfs4_arg_read *a = &op->arg.read;
	struct nfs4_res_read *r = &res->res.read;
	enum nfs4_status nst;
	bool stateid_validated = false;

	/* --- Xattr mode --- */
	if (cd->xattr_obj_set) {
		void *val = NULL;
		uint32_t val_len = 0;
		enum mds_status st;

		/* Xattrs are atomic -- reject partial-offset reads. */
		if (a->offset != 0) {
			return NFS4ERR_INVAL;
}

		st = cat_xattr_get(cd,
					xattr_base_fileid(cd->current_fh.fileid),
					cd->xattr_name, &val, &val_len);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
}

		if (val_len > MDS_XATTR_VAL_MAX) {
			val_len = MDS_XATTR_VAL_MAX;
}

		{
			uint32_t full_len = val_len;

			/* Honour client's count limit. */
			if (val_len > a->count) {
				val_len = a->count;
}

			r->data_len = val_len;
			if (val_len > 0) {
				memcpy(r->data, val, val_len);
}
			/* EOF only when we returned everything. */
			r->eof = (val_len >= full_len);
		}

		free(val);
		return NFS4_OK;
	}

	/* --- Inline data mode --- */
	{
		struct mds_inode rd_inode;
		enum mds_status rd_st;
		enum nfs4_status rd_nst;

		rd_nst = require_current_fh(cd);
		if (rd_nst != NFS4_OK) {
			return rd_nst;
}
		if (is_xattr_fh(cd->current_fh.fileid)) {
			goto proxy_read;
}
		if (cd->proxy != NULL) {
			rd_nst = validate_io_stateid(cd, &a->stateid,
						    OPEN4_SHARE_ACCESS_READ);
			if (rd_nst != NFS4_OK) {
				return rd_nst;
			}
			stateid_validated = true;
		}

		rd_st = compound_inode_get(cd, cd->current_fh.fileid, &rd_inode);
		if (rd_st != MDS_OK) {
			goto proxy_read;
}

		/* DS_PENDING: ensure DS file exists, clear flag, use proxy. */
		if ((rd_inode.flags & MDS_IFLAG_DS_PENDING) &&
		    cd->proxy != NULL) {
			{
				struct mds_ds_map_entry *sm = NULL;
				uint32_t sm_sc = 0, sm_mc = 0;
				enum mds_status dsst;

				dsst = cat_stripe_map_get(cd,
					cd->current_fh.fileid, &sm_sc, NULL,
					&sm_mc, &sm);
				if (dsst == MDS_OK && sm_sc > 0) {
					(void)mds_proxy_ensure_ds_file(
						cd->proxy, sm[0].ds_id,
						cd->current_fh.fileid, 0, 0);
					free(sm);
				} else {
					free(sm);
				}
			}
			goto proxy_read;
		}

		/*
		 * Legacy inline regular files: promote to DS before
		 * serving any read.  pNFS data I/O goes through DSes.
		 */
		if ((rd_inode.flags & MDS_IFLAG_INLINE) &&
		    cd->proxy != NULL) {
			rd_nst = promote_inline_to_ds(cd, &rd_inode);
			if (rd_nst != NFS4_OK) {
				return rd_nst;
			}
			goto proxy_read;
		}
	}

proxy_read:
	/* --- Proxy data mode --- */
	if (cd->proxy == NULL) {
		return NFS4ERR_NOTSUPP;
}

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	if (is_xattr_fh(cd->current_fh.fileid)) {
		return NFS4ERR_INVAL;
}
	if (!stateid_validated) {
		nst = validate_io_stateid(cd, &a->stateid,
					  OPEN4_SHARE_ACCESS_READ);
		if (nst != NFS4_OK) {
			return nst;
		}
	}

	{
		uint32_t count = a->count;
		enum mds_status st;

		if (count > MDS_XATTR_VAL_MAX) {
			count = MDS_XATTR_VAL_MAX;
}

		st = mds_proxy_read(cd->proxy, cd->cat, cd->current_fh.fileid,
				    a->offset, count,
				    r->data, &r->data_len, &r->eof);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
}
	}

	return NFS4_OK;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_write(struct compound_data *cd,
				 const struct nfs4_op *op,
				 struct nfs4_result *res)
{
	const struct nfs4_arg_write *a = &op->arg.write;
	struct nfs4_res_write *r = &res->res.write;
	bool proxy_checks_done = false;

	/* --- Xattr mode --- */
	if (cd->xattr_obj_set) {
		enum mds_status st;

		/* Xattrs are atomic -- reject partial-offset writes. */
		if (a->offset != 0) {
			return NFS4ERR_INVAL;
		}

		if (cd->cat == NULL) {
			return NFS4ERR_INVAL;
		}
		st = mds_cat_xattr_put(
			cd->cat, NULL,
			xattr_base_fileid(cd->current_fh.fileid),
			cd->xattr_name,
			a->data, a->data_len);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}

		r->count = a->data_len;
		r->committed = 2; /* FILE_SYNC4 */
		r->write_verf = cd->write_verf;
		return NFS4_OK;
	}

	/* --- Inline data mode --- */
	{
		struct mds_inode wr_inode;
		enum mds_status wr_st;
		enum nfs4_status wr_nst;

		wr_nst = require_current_fh(cd);
		if (wr_nst != NFS4_OK) {
			return wr_nst;
}
		if (is_xattr_fh(cd->current_fh.fileid)) {
			goto proxy_write;
}
		if (cd->proxy != NULL) {
			wr_nst = check_subtree_frozen(cd);
			if (wr_nst != NFS4_OK) {
				return wr_nst;
			}
			wr_nst = check_repl_health(cd);
			if (wr_nst != NFS4_OK) {
				return wr_nst;
			}
			wr_nst = validate_io_stateid(cd, &a->stateid,
						    OPEN4_SHARE_ACCESS_WRITE);
			if (wr_nst != NFS4_OK) {
				return wr_nst;
			}
			proxy_checks_done = true;
		}

		wr_st = cat_getattr(cd, cd->current_fh.fileid, &wr_inode);
		if (wr_st != MDS_OK) {
			goto proxy_write;
}

		/* DS_PENDING: ensure DS file exists, clear flag, use proxy. */
		if ((wr_inode.flags & MDS_IFLAG_DS_PENDING) &&
		    cd->proxy != NULL) {
			int prep_state = compound_ds_prepare_check(
				cd, cd->current_fh.fileid);
			bool async_pending =
				prep_state == DS_PREP_QUEUED ||
				prep_state == DS_PREP_IN_PROGRESS;
			/* Lazy DS file creation. */
			{
				struct mds_ds_map_entry *sm = NULL;
				uint32_t sm_sc = 0, sm_mc = 0;
				enum mds_status dsst;

				dsst = cat_stripe_map_get(cd,
					cd->current_fh.fileid, &sm_sc, NULL,
					&sm_mc, &sm);
				if (dsst == MDS_OK && sm_sc > 0) {
					(void)mds_proxy_ensure_ds_file(
						cd->proxy, sm[0].ds_id,
						cd->current_fh.fileid, 0, 0);
					free(sm);
				} else {
					/*
					 * No stripe map exists -- the file was
					 * DS_PENDING but never got placed.
					 * pNFS requires DS backing -- fail.
					 */
					free(sm);
					return NFS4ERR_NOSPC;
				}
			}
			/* Clear DS_PENDING flag via catalogue vtable
			 * (works for the RonDB backend). */
			if (!async_pending) {
				wr_inode.flags &= ~MDS_IFLAG_DS_PENDING;
				(void)cat_setattr(cd, wr_inode.fileid,
						  &wr_inode,
						  MDS_ATTR_FLAGS);
				compound_inode_invalidate(cd,
							  wr_inode.fileid);
			}
			goto proxy_write;
		}

		/*
		 * Regular-file inline WRITE is not supported in pNFS
		 * mode.  Legacy inline files must be promoted to DS
		 * before any data I/O can proceed.
		 */
		if ((wr_inode.flags & MDS_IFLAG_INLINE) &&
		    cd->proxy != NULL) {
			wr_nst = promote_inline_to_ds(cd, &wr_inode);
			if (wr_nst != NFS4_OK) {
				return wr_nst;
			}
			goto proxy_write;
		}
	}

proxy_write:
	/* --- Proxy data mode --- */
	if (cd->proxy == NULL) {
		return NFS4ERR_NOTSUPP;
}
	if (!proxy_checks_done) {
		enum nfs4_status wr_pnst;

		wr_pnst = require_current_fh(cd);
		if (wr_pnst != NFS4_OK) {
			return wr_pnst;
}
		if (is_xattr_fh(cd->current_fh.fileid)) {
			return NFS4ERR_INVAL;
}

		wr_pnst = check_subtree_frozen(cd);
		if (wr_pnst != NFS4_OK) {
			return wr_pnst;
}
		wr_pnst = check_repl_health(cd);
		if (wr_pnst != NFS4_OK) {
			return wr_pnst;
}

		/* Validate stateid for WRITE access. */
		wr_pnst = validate_io_stateid(cd, &a->stateid,
					      OPEN4_SHARE_ACCESS_WRITE);
		if (wr_pnst != NFS4_OK) {
			return wr_pnst;
}
	}


	/* Quota: check byte quota for extension beyond EOF only. */
	{
		struct mds_inode pw_inode;
		enum mds_status pw_st;
		pw_st = cat_getattr(cd, cd->current_fh.fileid, &pw_inode);
		if (pw_st == MDS_OK) {
			uint64_t pw_end = a->offset + a->data_len;
			if (pw_end > pw_inode.size) {
				enum nfs4_status pw_nst;
				pw_nst = quota_check_bytes(cd, pw_inode.uid,
							   pw_inode.gid,
							   pw_end - pw_inode.size);
				if (pw_nst != NFS4_OK) {
					return pw_nst;
				}
			}
		}
	}
	{
		uint32_t written = 0;
		enum mds_status st;
		uint64_t pre_write_size = 0;
		{
			struct mds_inode pw_pre;
			if (cat_getattr(cd, cd->current_fh.fileid, &pw_pre) == MDS_OK) {
				pre_write_size = pw_pre.size;
			}
		}

		st = mds_proxy_write(cd->proxy, cd->cat, cd->current_fh.fileid,
				     a->offset, a->data, a->data_len,
				     &written);
		if (st != MDS_OK) {
			(void)fprintf(stderr,
				"DBG WRITE: proxy_write failed fh=%llu st=%d checks=%d\n",
				(unsigned long long)cd->current_fh.fileid,
				(int)st, (int)proxy_checks_done);
			return mds_status_to_nfs4(st);
}

		/* Update inode metadata after proxy write:
		 * always bump mtime/ctime/change, extend size if needed. */
		{
			if (written > 0 &&
			    a->offset > UINT64_MAX - (uint64_t)written) {
				return NFS4ERR_INVAL;
			}
			uint64_t new_end = a->offset + written;
			struct mds_inode inode;

			st = cat_getattr(cd, cd->current_fh.fileid, &inode);
			if (st == MDS_OK && written > 0) {
				struct timespec now;
				clock_gettime(CLOCK_REALTIME, &now);
				inode.mtime = now;
				inode.ctime = now;
				inode.change++;
				if (new_end > inode.size) {
					inode.size = new_end;
					inode.space_used = new_end;
				}
				{
					/* setattr always bumps ctime+change;
					 * we add MTIME_NOW + SIZE if extending. */
					uint32_t mask = MDS_ATTR_MTIME_NOW;
					if (new_end > pre_write_size) {
						mask |= MDS_ATTR_SIZE;
					}
					(void)cat_setattr(cd, inode.fileid,
							     &inode, mask);
				}
			}
		}


		/* Quota: account only extension delta. */
		if (written > 0 && cd->quota != NULL) {
			uint64_t pw_new_end = a->offset + written;
			if (pw_new_end > pre_write_size) {
				struct mds_inode pw_q;
				if (cat_getattr(cd, cd->current_fh.fileid, &pw_q) == MDS_OK) {
					quota_submit_adjust(cd, pw_q.uid, pw_q.gid,
							    (int64_t)(pw_new_end - pre_write_size), 0);
				}
			}
		}
		compound_inode_invalidate(cd,
			cd->current_fh.fileid);
		r->count = written;
		r->committed = 2; /* FILE_SYNC4 */
		r->write_verf = cd->write_verf;
	}


	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * pNFS layout handlers
 * ----------------------------------------------------------------------- */

/**
 * Build a device ID from mds_id=1 and ds_id.
 * Format: [mds_id BE 4][ds_id BE 4][zeros 8] = 16 bytes.
 */


/* -----------------------------------------------------------------------
 * NFSv4.2 operation handlers (RFC 7862)
 * ----------------------------------------------------------------------- */

/**
 * IO_ADVISE (RFC 7862 S15.5): act on client I/O hints.
 *
 * Per RFC 7862 S15.5.3 the response `hints` bitmap tells the client
 * which hints the server actually acknowledged -- a lying echo of the
 * full request defeats the client's ability to adapt.  This handler
 * honours exactly the hint bits the MDS can act on:
 *
 *   WILLNEED / WILLNEED_OPP
 *     Bias the tiering evaluator toward keeping this file on hot-tier
 *     DSes by bumping the io_tracker LAYOUTGET signal once.  A crude
 *     lever, but enough to nudge `io_tracker_snapshot_and_reset`-based
 *     hotness rankings between eval cycles.
 *
 *   DONTNEED / NOREUSE
 *     Free MDS memory eagerly by evicting this fileid from the global
 *     inode LRU.  A subsequent GETATTR/LOOKUP re-reads from NDB.
 *     Harmless if the entry isn't cached.
 *
 *   NORMAL
 *     No-op but still echoed: the client uses it as a confirmation
 *     that the op round-trip completed.
 *
 *   READ / WRITE
 *     Directional companions to the primary hints above.  Echoed only
 *     when a primary hint was also honoured.
 *
 *   SEQUENTIAL / SEQUENTIAL_BACK / RANDOM / INIT_PROXIMITY
 *     Stripe geometry and placement proximity hints.  The MDS cannot
 *     act on these today (stripe map is fixed at create, device-
 *     affinity hints require DS-side cooperation we don't wire) so
 *     they are NOT echoed.  The client sees a zero bit in the response
 *     and will not assume the server has honoured them.
 */
enum nfs4_status op_io_advise(const struct compound_data *cd,
				     const struct nfs4_op *op,
				     struct nfs4_result *res)
{
	const struct nfs4_arg_io_advise *a = &op->arg.io_advise;
	struct nfs4_res_io_advise *r = &res->res.io_advise;
	enum nfs4_status nst;
	uint32_t req = a->hints;
	uint32_t honored = 0;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	/* Validate stateid. */
	nst = validate_io_stateid(cd, &a->stateid,
				  OPEN4_SHARE_ACCESS_READ);
	if (nst != NFS4_OK) {
		return nst;
}

	/* NORMAL is the "no preference" base; echo it if the client sent
	 * it so the round-trip is unambiguously confirmed.  The RFC says
	 * NORMAL has value 0, which is also the "no hint" case -- only
	 * echo when the client explicitly set some bit. */
	if (req == 0) {
		/* Zero-hint IO_ADVISE is a protocol-legal probe.  Acknowledge
		 * with zero; the response hints mask already defaults to 0. */
		r->hints = 0;
		atomic_fetch_add_explicit(&g_branch_metrics.io_advise_total, 1,
					  memory_order_relaxed);
		return NFS4_OK;
	}

	/* WILLNEED / WILLNEED_OPP: bias tiering hot. */
	if (req & (IO_ADVISE4_WILLNEED | IO_ADVISE4_WILLNEED_OPP)) {
		io_tracker_bump(cd->io_tracker, cd->current_fh.fileid);
		honored |= (req & (IO_ADVISE4_WILLNEED |
				   IO_ADVISE4_WILLNEED_OPP));
		atomic_fetch_add_explicit(
			&g_branch_metrics.io_advise_willneed, 1,
			memory_order_relaxed);
	}

	/* DONTNEED / NOREUSE: free the inode_cache slot for this fileid.
	 * inode_cache_invalidate is a no-op if the entry isn't cached;
	 * it does not touch NDB or any authoritative state.  Safe through
	 * a const compound_data because cd->icache is a pointer we only
	 * dereference, not cd itself. */
	if (req & (IO_ADVISE4_DONTNEED | IO_ADVISE4_NOREUSE)) {
		if (cd->icache != NULL) {
			inode_cache_invalidate(cd->icache,
					       cd->current_fh.fileid);
		}
		honored |= (req & (IO_ADVISE4_DONTNEED |
				   IO_ADVISE4_NOREUSE));
		atomic_fetch_add_explicit(
			&g_branch_metrics.io_advise_dontneed, 1,
			memory_order_relaxed);
	}

	/* READ / WRITE are directional companions.  Only echo them when
	 * they accompany a primary hint we actually honoured; echoing a
	 * bare READ / WRITE bit alone would imply the server took some
	 * caching action it did not. */
	if (honored != 0) {
		honored |= (req & (IO_ADVISE4_READ | IO_ADVISE4_WRITE));
	}

	/* SEQUENTIAL / SEQUENTIAL_BACK / RANDOM / INIT_PROXIMITY
	 * intentionally drop out of the honored mask -- we cannot act on
	 * them, and RFC 7862 S15.5.3 requires the response mask to
	 * describe only what the server acknowledged. */

	r->hints = honored;
	atomic_fetch_add_explicit(&g_branch_metrics.io_advise_total, 1,
				  memory_order_relaxed);
	return NFS4_OK;
}

/** LAYOUTERROR (RFC 7862 S15.6): client reports DS I/O error. */


/* -----------------------------------------------------------------------
 * COMMIT handler (RFC 8881 S18.3)
 *
 * For proxy I/O, flushes the DS file via fsync.  For pNFS direct I/O,
 * the DS handles durability -- we return the server write verifier.
 * ----------------------------------------------------------------------- */

enum nfs4_status op_commit(const struct compound_data *cd,
			   const struct nfs4_op *op,
			   struct nfs4_result *res)
{
	(void)op;

	/*
	 * For proxy I/O, the MDS writes are already durable (catalogue
	 * commits are synchronous).  DS-side durability is handled
	 * by the DS NFS server.  Accept COMMIT as a no-op.
	 *
	 * Return the server boot epoch as the write verifier so that
	 * clients can detect server restarts (RFC 8881 S18.3).
	 */
	res->res.commit.write_verf = cd->write_verf;
	return NFS4_OK;
}

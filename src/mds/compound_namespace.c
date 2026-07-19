/*
 * Copyright (c) 2026 PeakAIO. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-PeakAIO-Proprietary
 */
/*
 * compound_namespace.c — NFSv4.1 namespace ops (FH, lookup, attrs, dir).
 */

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "remove_manifest.h"
#include "compound_internal.h"
#include "mds_catalogue.h"
#include "delegation.h"
#include "open_state.h"
#include "hpc_shared.h"

#include "subtree_map.h"
#include "cluster_transport.h"
#include "hardlink_2pc.h"
#include "cluster_membership.h"
#include "referral.h"
#include "rename_2pc.h"
#include "migration.h"
#include "grace.h"
#include "io_tracker.h"
#include "commit_queue.h"
#include "quota.h"
#include "proxy_io.h"
#include "xdr_codec.h"
#include "dirent_cache.h"
#include "inode_cache.h"
#include "ds_cache.h"
#include "layout_cache.h"  /* Phase D of docs/hpc-nto1-plan.md */
#include "layout_commit_aggregator.h"  /* Phase F of docs/hpc-nto1-plan.md */
#include "layout_recall.h"  /* Mark Q5: recall layouts on truncate / unlink */

#define ACCESS4_READ    0x00000001
#define ACCESS4_LOOKUP  0x00000002
#define ACCESS4_MODIFY  0x00000004
#define ACCESS4_EXTEND  0x00000008
#define ACCESS4_DELETE  0x00000010
#define ACCESS4_EXECUTE 0x00000020
/* RFC 8276 §8.5: extended ACCESS bits for xattr operations. */
#define ACCESS4_XAREAD  0x00000040
#define ACCESS4_XALIST  0x00000080
#define ACCESS4_XAWRITE 0x00000100
#define ACCESS4_ALL     0x000001FF

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */

static bool compound_next_op_requests_fs_locations(const struct compound_data *cd)
{
    const struct nfs4_op *next;

    if (cd->ops == NULL || (cd->op_index + 1) >= cd->op_count) {
        return false;
    }

    next = &cd->ops[cd->op_index + 1];
    return next->opnum == OP_GETATTR &&
           nfs4_bitmap_test(next->arg.getattr.requested, FATTR4_FS_LOCATIONS);
}


/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_access(struct compound_data *cd,
				  const struct nfs4_op *op,
				  struct nfs4_result *res)
{
	const struct nfs4_arg_access *a = &op->arg.access;
	struct nfs4_res_access *r = &res->res.access;
	enum nfs4_status nst;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/*
	 * ACCESS reports the caller's rights computed from AUTH_SYS
	 * credentials and the inode mode bits.  Deliberately NOT
	 * gated by posix_dac: the reply is advisory (RFC 8881
	 * §18.1.3) and Linux clients enforce POSIX locally from these
	 * bits, so the mapping must stay accurate even when
	 * server-side enforcement is off.
	 */
	{
		struct mds_inode acc_inode;
		enum mds_status acc_st;
		bool acc_is_dir;
		uint32_t type_mask;

		acc_st = compound_inode_get(cd, cd->current_fh.fileid,
					    &acc_inode);
		if (acc_st != MDS_OK) {
			return mds_status_to_nfs4(acc_st);
		}

		acc_is_dir = (acc_inode.type == MDS_FTYPE_DIR);

		/*
		 * Per-type bit sets (matches Linux knfsd): directories
		 * never report EXECUTE, and non-directories never
		 * report LOOKUP or DELETE (deleting an entry is a
		 * right on the PARENT directory, not on the object).
		 */
		if (acc_is_dir) {
			type_mask = ACCESS4_ALL & ~ACCESS4_EXECUTE;
		} else {
			type_mask = ACCESS4_ALL &
				~(ACCESS4_LOOKUP | ACCESS4_DELETE);
		}

		r->supported = a->access & type_mask;

		/*
		 * POSIX permission mapping against caller credentials.
		 *
		 * RFC 8881 §18.1.3: the server SHOULD return all
		 * access rights that the caller has, not only the
		 * requested subset.  This ensures the client can
		 * cache the full permission set in a single RPC.
		 */
		{
			uint32_t mode = acc_inode.mode;
			uint32_t allowed = 0;
			uint32_t perm;

			if (cd->cred_uid == 0) {
				/*
				 * root: DAC override.  Like Linux
				 * CAP_DAC_OVERRIDE, EXECUTE on a regular
				 * file still requires at least one x bit.
				 */
				allowed = type_mask;
				if (!acc_is_dir && (mode & 0111) == 0) {
					allowed &= ~ACCESS4_EXECUTE;
				}
			} else {
				/* Owner / group / other class. */
				if (cd->cred_uid == (uint32_t)acc_inode.uid) {
					perm = (mode >> 6) & 7;
				} else if (compound_cred_in_group(cd,
						(uint32_t)acc_inode.gid)) {
					perm = (mode >> 3) & 7;
				} else {
					perm = mode & 7;
				}

				/*
				 * RFC 8276 §8.5: XAREAD/XALIST follow the
				 * read bit; XAWRITE follows the write bit.
				 */
				if (perm & 4) {
					allowed |= ACCESS4_READ |
						   ACCESS4_XAREAD |
						   ACCESS4_XALIST;
				}
				if (perm & 2) {
					allowed |= ACCESS4_XAWRITE;
				}

				if (acc_is_dir) {
					/*
					 * Directory search (LOOKUP) is
					 * granted by the x bit ONLY —
					 * never by the read bit.  The
					 * read bit grants listing (READ),
					 * not traversal.  Granting LOOKUP
					 * from r let clients bypass the
					 * search check on no-x dirs
					 * (pjdfstest ftruncate/05 et al.
					 * with posix_dac off).  Mutating
					 * entries needs w+x, matching
					 * knfsd's directory access map.
					 */
					if (perm & 1) {
						allowed |= ACCESS4_LOOKUP;
					}
					if ((perm & 3) == 3) {
						allowed |= ACCESS4_MODIFY |
							   ACCESS4_EXTEND |
							   ACCESS4_DELETE;
					}
				} else {
					if (perm & 2) {
						allowed |= ACCESS4_MODIFY |
							   ACCESS4_EXTEND;
					}
					if (perm & 1) {
						allowed |= ACCESS4_EXECUTE;
					}
				}
			}

			/*
			 * Return all rights the caller actually has
			 * (possibly beyond the requested subset) —
			 * this tells the client everything it can do.
			 */
			r->access = allowed & type_mask;
		}
	}
	return NFS4_OK;
}

enum nfs4_status op_putrootfh(struct compound_data *cd,
				     const struct nfs4_op *op,
				     struct nfs4_result *res)
{
	(void)op;
	(void)res;
	cd->current_fh.fileid = MDS_FILEID_ROOT;
	cd->current_fh.owner_mds_id = cd->mds_id;
	cd->current_fh.generation = 0;
	cd->current_fh_set = true;
	(void)snprintf(cd->current_path, sizeof(cd->current_path), "/");
	resolve_and_apply_shard(cd, cd->current_path);

	/* Read the root inode's actual generation so the FH that GETFH
	 * emits matches the FH that LOOKUPP produces when navigating
	 * back to the root.  Pynfs LKPP1d verifies FH identity by
	 * comparing GETFH after PUTROOTFH against GETFH after LOOKUPP;
	 * a generation mismatch (0 vs the on-disk value) makes the
	 * test fail.  On catalogue miss (test-compat / no backend),
	 * generation stays 0 — harmless. */
	{
		struct mds_inode root_ino;
		if (compound_inode_get(cd, MDS_FILEID_ROOT,
				       &root_ino) == MDS_OK) {
			cd->current_fh.generation = root_ino.generation;
			cd->current_inode = root_ino;
			cd->current_inode_valid = true;
		}
	}
	return NFS4_OK;
}

enum nfs4_status op_putfh(struct compound_data *cd,
				 const struct nfs4_op *op,
				 struct nfs4_result *res)
{
	(void)res;

	/*
	 * No referral check on PUTFH.  Junction dir inodes are local
	 * metadata owned by this MDS.  The NFS4ERR_MOVED referral fires
	 * on LOOKUP into the junction (op_lookup), not on PUTFH of the
	 * junction dir itself.  This allows the RFC 8881 §8.5.1 flow:
	 *   LOOKUP(junction) → MOVED → PUTFH(junction_fh) + GETATTR(fs_locations)
	 */

	/*
	 * RFC 8881 §18.19.4 — PUTFH must return NFS4ERR_BADHANDLE when
	 * the supplied filehandle is not in this server's recognised
	 * format.  The decoder uses fileid==0 as the malformed-FH
	 * sentinel (root is fileid 2, no real object lives at 0), so
	 * reject it here before we install it as the current FH.
	 * Pynfs PUTFH2 (testBadHandle) sends a 3-byte 'abc' FH and
	 * expects NFS4ERR_BADHANDLE rather than NFS4ERR_BADXDR.
	 */
	if (op->arg.putfh.fh.fileid == 0) {
		return NFS4ERR_BADHANDLE;
	}

	cd->current_fh = op->arg.putfh.fh;
	cd->current_fh_set = true;
	/* PUTFH with raw fileid — path unknown, clear it. */
	cd->current_path[0] = '\0';

	/* Route first, then validate in the resolved shard.
	 *
	 * When the shard map contains a single shard (the default /
	 * single-mds profile) every fileid maps to the same shard, so
	 * we skip the mds_cat_shard_fileid_get NDB PK read and apply
	 * the default directly.  This saves ~200 us per PUTFH on
	 * unsharded deployments.  Multi-shard deployments keep the
	 * fileid -> shard lookup so cross-subtree FHs still route. */
	if (cd->shard_map != NULL && cd->cat != NULL &&
	    mds_shard_map_count(cd->shard_map) > 1) {
		uint32_t fid_shard_id = 0;
		if (mds_cat_shard_fileid_get(cd->cat,
					     op->arg.putfh.fh.fileid,
					     &fid_shard_id) == MDS_OK) {
			const struct mds_shard *resolved =
				mds_shard_map_lookup_by_id(cd->shard_map,
							   fid_shard_id);
			if (resolved != NULL) {
				apply_shard(cd, resolved);
			} else {
				resolve_and_apply_shard(cd, NULL);
			}
		} else {
			resolve_and_apply_shard(cd, NULL);
		}
	} else {
		resolve_and_apply_shard(cd, NULL);
	}

	{
		struct mds_inode inode;
		enum mds_status st;

		/* Use compound_inode_get so we hit the cross-compound
		 * inode cache on repeat references to the same fileid.
		 * Previously this was cat_getattr which always issued an
		 * NDB PK read; mdtest-style flows do 3 PUTFHs per file
		 * (OPEN, CLOSE, REMOVE) — 2 of those are now icache hits.
		 * compound_inode_get itself seeds cd->current_inode on
		 * success. */
		st = compound_inode_get(cd, op->arg.putfh.fh.fileid,
					&inode);
		if (st == MDS_ERR_NOTFOUND) {
			/* The filehandle is well-formed (the fileid==0
			 * malformed-FH sentinel is rejected above with
			 * NFS4ERR_BADHANDLE) but no longer designates an
			 * existing object: the fileid was deleted, and because
			 * fileids are monotonic and never reused it will never
			 * come back.  RFC 8881 §15.1.2.4 — that is a STALE
			 * filehandle, not a structurally bad one.  Returning
			 * NFS4ERR_STALE lets the Linux client recover
			 * transparently (it re-runs LOOKUP and resolves the
			 * name to the current fileid) instead of surfacing
			 * EBADHANDLE (errno 521) to userspace.  This is the
			 * active-active IOR -F -C read-back failure mode where a
			 * peer MDS served a delete+recreate of the same name. */
			return NFS4ERR_STALE;
		}
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}
	}

	/* Subtree ownership comes from the wire FH itself: v1 FHs
	 * round-trip the owner_mds_id stamped at LOOKUP/GETFH time, so a
	 * cached FH deep inside a foreign shard still gates correctly
	 * (compound.c MOVED dispatch) with no catalogue I/O here.  An
	 * ancestry walk was tried instead and abandoned: it put an NDB
	 * read on the PUTFH hot path, and a single wedged read left the
	 * worker stuck mid-compound (client OPEN never answered).
	 * Legacy v0 FHs decode owner 0 and are never gated (fail-open,
	 * single-namespace semantics). */

	return NFS4_OK;
}

enum nfs4_status op_getfh(const struct compound_data *cd,
				 const struct nfs4_op *op,
				 struct nfs4_result *res)
{
	enum nfs4_status nst;

	(void)op;
	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	res->res.getfh.fh = cd->current_fh;
	return NFS4_OK;
}

enum nfs4_status op_savefh(struct compound_data *cd,
				  const struct nfs4_op *op,
				  struct nfs4_result *res)
{
	enum nfs4_status nst;

	(void)op;
	(void)res;
	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	cd->saved_fh = cd->current_fh;
	cd->saved_fh_set = true;
	(void)snprintf(cd->saved_path, sizeof(cd->saved_path),
		 "%s", cd->current_path);
	cd->saved_shard = cd->current_shard;
	cd->saved_inode = cd->current_inode;
	cd->saved_inode_valid = cd->current_inode_valid;
	return NFS4_OK;
}

enum nfs4_status op_restorefh(struct compound_data *cd,
				     const struct nfs4_op *op,
				     struct nfs4_result *res)
{
	enum nfs4_status nst;

	(void)op;
	(void)res;
	nst = require_saved_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	cd->current_fh = cd->saved_fh;
	cd->current_fh_set = true;
	(void)snprintf(cd->current_path, sizeof(cd->current_path),
		 "%s", cd->saved_path);
	apply_shard(cd, cd->saved_shard);
	cd->current_inode = cd->saved_inode;
	cd->current_inode_valid = cd->saved_inode_valid;
	return NFS4_OK;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_lookup(struct compound_data *cd,
				  const struct nfs4_op *op,
				  struct nfs4_result *res)
{
	struct mds_inode child;
	enum nfs4_status nst;
	enum mds_status st;

	(void)res;
	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/*
	 * RFC 5661 §16.4.5 / RFC 8881 §18.16.5 + §12.7 — component4
	 * validation.  Empty name → NFS4ERR_INVAL (pynfs SEQ9c).
	 * "." / ".." → NFS4ERR_BADNAME (pynfs RNM10 family for
	 * RENAME, same rule applies to LOOKUP per RFC 8881 §12.7
	 * "the dot and dot-dot conventions are not used in NFSv4").
	 * Invalid UTF-8 / embedded NUL / '/' → NFS4ERR_INVAL
	 * (pynfs RNM8/9 testBadutf8*).  Decoder NUL-terminates
	 * op->arg.lookup.name so strlen() inside the helper is safe.
	 * Names longer than MDS_MAX_NAME were consumed-and-flagged by
	 * the decoder -> NFS4ERR_NAMETOOLONG (checked first: the
	 * flagged buffer is empty and would misreport INVAL).
	 */
	if (op->arg.lookup.name_too_long) {
		return NFS4ERR_NAMETOOLONG;
	}
	nst = compound_validate_name(op->arg.lookup.name);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* Xattr namespace: LOOKUP verifies the xattr exists. */
	if (is_xattr_fh(cd->current_fh.fileid)) {
		uint64_t base = xattr_base_fileid(cd->current_fh.fileid);

		st = cat_xattr_exists(cd, base,
					    op->arg.lookup.name);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
}

		/* Store xattr name for subsequent READ/WRITE. */
		(void)snprintf(cd->xattr_name, sizeof(cd->xattr_name),
			 "%s", op->arg.lookup.name);
		cd->xattr_obj_set = true;
		/* current_fh stays as the xattr-namespace handle. */
		return NFS4_OK;
	}

	/*
	 * POSIX DAC: the caller needs search (execute) permission on
	 * the directory to resolve a component in it.  The parent
	 * inode is almost always a request-local snapshot hit (PUTFH /
	 * a previous LOOKUP seeded it), so this adds no catalogue
	 * round-trip on the hot path.
	 */
	if (compound_dac_active(cd)) {
		struct mds_inode lk_dir;

		if (compound_inode_get(cd, cd->current_fh.fileid,
				       &lk_dir) == MDS_OK) {
			nst = compound_dir_search_check(cd, &lk_dir);
			if (nst != NFS4_OK) {
				return nst;
			}
		}
	}

	st = compound_lookup_local_child(cd, cd->current_fh.fileid,
					 op->arg.lookup.name, &child);
	if (st == MDS_ERR_NOTFOUND && cd->cat != NULL &&
	    cd->shard_map != NULL) {
		/* Check ext_dirents for cross-shard links. */
		uint32_t ext_owner = 0;
		uint64_t ext_fid = 0;
		uint8_t  ext_type = 0;
		uint64_t ext_anchor = 0;
		enum mds_status ext_st;

		ext_st = mds_cat_ext_dirent_get(
			cd->cat, cd->current_fh.fileid,
			op->arg.lookup.name,
			&ext_owner, &ext_fid, &ext_type, &ext_anchor);
		if (ext_st == MDS_OK) {
			/* Found cross-shard link. Resolve target shard. */
			uint32_t target_sid = 0;
			if (mds_cat_shard_fileid_get(cd->cat, ext_fid,
						     &target_sid) == MDS_OK) {
					const struct mds_shard *ts =
						mds_shard_map_lookup_by_id(
							cd->shard_map, target_sid);
					if (ts != NULL) {
						apply_shard(cd, ts);
						cd->current_fh.fileid = ext_fid;
						/* FH-encoded subtree ownership: shards
						 * are intra-MDS, so the FH still belongs
						 * to this MDS even after a cross-shard
						 * hop.  Stamp local mds_id; we do not have
						 * the target inode's generation here
						 * (the ext_dirent path returns no inode),
						 * leave it 0 so the encoder treats it as
						 * unspecified — consistent with v0
						 * legacy behaviour for these synthetic
						 * cross-shard handles. */
						cd->current_fh.owner_mds_id = cd->mds_id;
						cd->current_fh.generation = 0;
						cd->current_fh_set = true;
						/* Path tracking: append name. */
						{
							size_t plen = strlen(cd->current_path);
							if (plen == 1 && cd->current_path[0] == '/') {
								(void)snprintf(cd->current_path,
									sizeof(cd->current_path),
									"/%s", op->arg.lookup.name);
							} else if (plen > 0) {
								(void)snprintf(cd->current_path + plen,
									sizeof(cd->current_path) - plen,
									"/%s", op->arg.lookup.name);
							}
						}
					return NFS4_OK;
				}
			}
		}
	}
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
}

	/* Default junction semantics are LOOKUP -> NFS4ERR_MOVED.  The
	 * Linux referral follow-up is a separate compound that does
	 * PUTFH(parent) + LOOKUP(name) + GETATTR(fs_locations).  Allow
	 * that referral-discovery compound to resolve the junction FH so
	 * GETATTR can return fs_locations, but keep ordinary LOOKUPs on
	 * the junction returning MOVED so Linux creates a referral inode
	 * instead of a same-server submount.
	 *
	 * The sticky bit is only the in-band junction MARKER; the
	 * subtree map is the authority.  A sticky directory with no
	 * EXACT subtree-map entry is an ordinary POSIX restricted-
	 * deletion directory (a 01777 tmp dir, pjdfstest's sticky
	 * fixtures) and MUST NOT return MOVED — the previous marker-
	 * only check made every S_ISVTX directory unreachable.  The
	 * match must be EXACT, not longest-prefix: a prefix match
	 * merely reports which subtree REGION contains the path (every
	 * path matches the root entry), which says nothing about this
	 * directory being a registered split boundary — and a stale or
	 * foreign-owned root row would otherwise turn every sticky
	 * directory into a phantom referral.  Real junctions are
	 * registered with their exact path by the split machinery, so
	 * requiring (a) a subtree map, (b) an exact map entry for the
	 * path, and (c) a DIFFERENT owning MDS preserves genuine
	 * referrals (self-owned junctions must resolve normally or
	 * clients referral-loop). */
	if (cd->smap != NULL &&
	    !compound_next_op_requests_fs_locations(cd)) {
		int is_junction = referral_is_junction(
			cd->cat, child.fileid);

		if (is_junction < 0) {
			return NFS4ERR_IO;
		}
		if (is_junction == 1) {
			char jpath[MDS_MAX_PATH];
			struct subtree_entry se;
			size_t pl = strlen(cd->current_path);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-truncation"
			if (pl == 1 && cd->current_path[0] == '/') {
				(void)snprintf(jpath, sizeof(jpath),
					"/%s", op->arg.lookup.name);
			} else {
				(void)snprintf(jpath, sizeof(jpath),
					"%s/%s", cd->current_path,
					op->arg.lookup.name);
			}
#pragma GCC diagnostic pop
			if (subtree_map_lookup_exact(cd->smap, jpath,
						     &se) == MDS_OK &&
			    se.owner_mds_id != cd->mds_id) {
				return NFS4ERR_MOVED;
			}
		}
	}

	cd->current_fh.fileid = child.fileid;
	/* FH-encoded subtree ownership (suggested by Gaurav Gangalwar's
	 * code review): stamp the SUBTREE owner -- not the serving MDS --
	 * so PUTFH-starting compounds and the cross-MDS routing block
	 * (compound.c MOVED dispatch) can recover ownership directly from
	 * the FH.  For the referral-discovery lookup of a junction this
	 * tags the junction FH with its foreign owner, so nothing beyond
	 * GETATTR(fs_locations) can be done with it.  The child's
	 * generation comes from the catalogue read above. */
	cd->current_fh.owner_mds_id = cd->mds_id;
	cd->current_fh.generation = child.generation;
	/* Seed snapshot with authoritative child inode from normal
	 * dirent path (NOT ext_dirent synthetic inodes). */
	cd->current_inode = child;
	cd->current_inode_valid = true;
	/* Update path tracking — check for truncation. */
	{
		size_t plen = strlen(cd->current_path);
		int n;

		if (plen == 1 && cd->current_path[0] == '/') {
			n = snprintf(cd->current_path,
				     sizeof(cd->current_path),
				     "/%s", op->arg.lookup.name);
		} else if (plen > 0) {
			n = snprintf(cd->current_path + plen,
				     sizeof(cd->current_path) - plen,
				     "/%s", op->arg.lookup.name);
		} else {
			n = 0;
		}
		if (n < 0 || (size_t)n >= sizeof(cd->current_path) - plen) {
			return NFS4ERR_NAMETOOLONG;
}
	}
	/* FH-encoded subtree ownership: longest-prefix owner of the
	 * resolved path (in-memory map lookup, no catalogue I/O).  The
	 * root entry means unsharded namespace -- keep the local stamp. */
	if (cd->cfg_referral_strict && cd->smap != NULL) {
		struct subtree_entry sub_se;

		if (subtree_map_lookup(cd->smap, cd->current_path,
				       &sub_se) == MDS_OK &&
		    !(sub_se.path[0] == '/' && sub_se.path[1] == '\0')) {
			cd->current_fh.owner_mds_id = sub_se.owner_mds_id;
		}
	}
	resolve_and_apply_shard(cd, cd->current_path);
	return NFS4_OK;
}

/*
 * Phase F of docs/hpc-nto1-plan.md, integration part B — coherence
 * handshake with the LAYOUTCOMMIT aggregator for op_getattr.
 *
 * Only HPC-Shared inodes can have an aggregate (the synchronous
 * LAYOUTCOMMIT path never submits), so non-HPC GETATTRs and the
 * no-aggregator deployment skip this entirely and stay bit-for-bit
 * identical to the pre-Phase-F path.
 *
 * STRICT (default): force-flush the bucket so the persistent inode
 * reflects the latest size/mtime, then re-read.  This preserves
 * POSIX stat() semantics at the cost of one synchronous NDB write
 * per stat on a hot HPC file.
 *
 * OPTIMISTIC: peek the bucket and overlay max(size) / latest(mtime)
 * onto the response without flushing.  Cheaper but the persisted
 * inode lags by up to one flush interval; documented in
 * docs/hpc-shared-files.md as opt-in.
 *
 * Returns NFS4_OK on success.  On STRICT-path re-read failure,
 * returns the mapped catalogue error so the caller propagates it.
 */
static enum nfs4_status
op_getattr_apply_lca_coherence(struct compound_data *cd,
			       struct nfs4_res_getattr *gr)
{
	if (cd->lcommit_agg == NULL ||
	    (gr->inode.flags & MDS_IFLAG_HPC_SHARED) == 0) {
		return NFS4_OK;
	}

	if (cd->cfg_hpc_getattr_mode == MDS_HPC_GETATTR_STRICT) {
		int frc = layout_commit_aggregator_flush_fileid(
			cd->lcommit_agg, cd->current_fh.fileid);
		if (frc == 1) {
			/* Bucket miss — nothing aggregated for this
			 * fileid; the persisted inode already reflects
			 * the latest known state.  Return what we read. */
			return NFS4_OK;
		}
		if (frc < 0) {
			/* QA review Blocker 4: STRICT promises POSIX-ish
			 * stat semantics — the persisted inode MUST reflect
			 * any pending aggregate before we reply.  The
			 * pre-fix path swallowed callback failures and
			 * served stale data; that contradicted the
			 * documented mode.  Surface as NFS4ERR_DELAY so the
			 * client retries; the periodic timer or a
			 * subsequent successful submit will retry the
			 * flush.  OPTIMISTIC mode keeps the
			 * peek-and-overlay best-effort semantics for
			 * deployments that opt in. */
			return NFS4ERR_DELAY;
		}
		/* frc == 0: bucket was dirty and just persisted.  Drop
		 * the request-local snapshot (the flush callback already
		 * invalidated the cross-compound icache), then re-read
		 * so the response carries the just-flushed values. */
		compound_inode_invalidate(cd, cd->current_fh.fileid);
		enum mds_status st = compound_inode_get(cd,
			cd->current_fh.fileid, &gr->inode);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}
		return NFS4_OK;
	}

	/* OPTIMISTIC */
	uint64_t agg_size = 0;
	struct timespec agg_mtime = { 0, 0 };
	bool agg_dirty = false;

	if (layout_commit_aggregator_peek(cd->lcommit_agg,
					  cd->current_fh.fileid,
					  &agg_size, &agg_mtime,
					  &agg_dirty) != 0 || !agg_dirty) {
		return NFS4_OK;
	}
	if (agg_size > gr->inode.size) {
		gr->inode.size = agg_size;
	}
	if (agg_mtime.tv_sec > gr->inode.mtime.tv_sec ||
	    (agg_mtime.tv_sec == gr->inode.mtime.tv_sec &&
	     agg_mtime.tv_nsec > gr->inode.mtime.tv_nsec)) {
		gr->inode.mtime = agg_mtime;
	}
	return NFS4_OK;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_getattr(struct compound_data *cd,
				   const struct nfs4_op *op,
				   struct nfs4_result *res)
{
	enum nfs4_status nst;
	enum mds_status st;

	(void)op;
	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	st = compound_inode_get(cd, cd->current_fh.fileid,
				&res->res.getattr.inode);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
}

	nst = op_getattr_apply_lca_coherence(cd, &res->res.getattr);
	if (nst != NFS4_OK) {
		return nst;
	}

	/*
	 * RFC 8881 §20.1 CB_GETATTR: if another client holds a WRITE
	 * delegation on this file, the delegation holder may have
	 * modified size/change locally without writing back.  Issue
	 * CB_GETATTR to the holder and overlay the returned attrs.
	 * Pynfs DELEG24/25.
	 */
	if (cd->dt != NULL &&
	    res->res.getattr.inode.type == MDS_FTYPE_REG) {
		uint64_t cb_size = 0;
		uint64_t cb_change = 0;
		if (deleg_cb_getattr_for_file(cd->dt,
					     cd->current_fh.fileid,
					     cd->clientid,
					     &cb_size,
					     &cb_change) == 0) {
			res->res.getattr.inode.size = cb_size;
			res->res.getattr.inode.change = cb_change;
		}
	}

	/*
	 * Populate FS-level SPACE_AVAIL / SPACE_FREE / SPACE_TOTAL.
	 *
	 * The reported values reflect, in order of precedence:
	 *   1. Quota limits for the inode's owner uid/gid (when a
	 *      quota context is wired and rules are installed).
	 *      The no-rules fast path returns UINT64_MAX from
	 *      mds_quota_space_avail() — a sentinel that means
	 *      "unlimited from quota's perspective".
	 *   2. Physical aggregate of all online DSes (sum of the
	 *      live statvfs probes in ds_cache).  This bounds the
	 *      sentinel so `df` shows the actual cluster capacity
	 *      rather than ~16 EB / ~9.2 EB unlimited markers.
	 *
	 * If neither source has data (no quota, no DS cache, or
	 * the probe has not produced a non-zero total yet) the
	 * values stay at UINT64_MAX and the encoder takes over
	 * (see encode_attr_vals SPACE_* clamping).
	 */
	{
		uint64_t avail = UINT64_MAX;
		uint64_t sfree = UINT64_MAX;
		uint64_t total = UINT64_MAX;
		bool have = false;

		if (cd->quota != NULL) {
			mds_quota_space_avail(cd->quota,
				res->res.getattr.inode.uid,
				res->res.getattr.inode.gid,
				&avail, &sfree, &total);
			have = true;
		}

		if (cd->ds_cache != NULL) {
			uint64_t ds_total = 0;
			uint64_t ds_used = 0;
			uint32_t ds_count = 0;
			/*
			 * Staleness gate: skip per-DS observations
			 * older than this many wall-clock seconds.
			 * Default tuned to 5 × the 60 s probe interval
			 * — large enough to ride out a single missed
			 * probe cycle but small enough that a stopped
			 * MDS's frozen reading falls out of df within
			 * minutes.  Hardcoded for now; a config knob
			 * can be added later as a one-line addition.
			 */
			const uint32_t ds_capacity_stale_sec = 300;

			ds_cache_aggregate_capacity(cd->ds_cache,
				ds_capacity_stale_sec,
				&ds_total, &ds_used, &ds_count);

			if (ds_count > 0 && ds_total > 0) {
				uint64_t ds_avail =
					(ds_used >= ds_total)
					? 0
					: (ds_total - ds_used);

				/* Clamp quota "unlimited" sentinel to the
				 * physical aggregate.  When real quota
				 * limits are tighter than the cluster
				 * capacity, they win; when they are
				 * looser (or absent), the cluster
				 * capacity bounds them. */
				if (total > ds_total) {
					total = ds_total;
				}
				if (avail > ds_avail) {
					avail = ds_avail;
				}
				if (sfree > ds_avail) {
					sfree = ds_avail;
				}
				have = true;
			}
		}

		if (have) {
			res->res.getattr.has_fs_space = true;
			res->res.getattr.space_avail  = avail;
			res->res.getattr.space_free   = sfree;
			res->res.getattr.space_total  = total;
		}
	}

	/* Junction referral: only directories can be junctions.
	 * Skip the NDB lookup entirely for regular files. */
	{
		int is_jct = 0;
		if (res->res.getattr.inode.type == MDS_FTYPE_DIR) {
			is_jct = referral_is_junction(cd->cat,
						     cd->current_fh.fileid);
		}
	if (cd->smap != NULL && is_jct == 1) {
		char jpath[MDS_MAX_PATH];
		struct mds_fs_location loc;
		struct subtree_entry jse;
		enum mds_status rp_st = referral_resolve_path(
			cd->cat, cd->current_fh.fileid,
			jpath, sizeof(jpath));
		/* Only surface fs_locations when the junction is owned
		 * by a DIFFERENT MDS.  For junctions owned by the MDS
		 * currently serving this request, returning
		 * fs_locations pointing back at ourselves makes the
		 * Linux NFS client auto-submount the junction as a
		 * separate vfsmount; every directory ever created
		 * inside it is then pinned by is_local_mountpoint() on
		 * the client, which makes rmdir / rm -rf fail with
		 * EBUSY (the RMDIR RPC is rejected by vfs_rmdir before
		 * it ever reaches the server).  Self-owned junctions
		 * need no client-side redirect — we are already the
		 * authoritative MDS for the subtree.
		 *
		 * Exact-match only (mirrors op_lookup): a longest-
		 * prefix hit on an enclosing subtree (ultimately the
		 * root entry) does not make this sticky directory a
		 * registered split boundary. */
		if (rp_st == MDS_OK &&
		    subtree_map_lookup_exact(cd->smap, jpath,
					     &jse) == MDS_OK &&
		    jse.owner_mds_id != cd->mds_id &&
		    referral_build(cd->smap, jpath, &loc) == MDS_OK) {
			res->res.getattr.has_referral = true;
			res->res.getattr.referral_owner_mds_id =
				jse.owner_mds_id;
			(void)snprintf(res->res.getattr.referral_fs_root,
				sizeof(res->res.getattr.referral_fs_root),
				"%s", jpath);
			(void)snprintf(res->res.getattr.referral_server,
				sizeof(res->res.getattr.referral_server),
				"%s", loc.server);
			(void)snprintf(res->res.getattr.referral_rootpath,
				sizeof(res->res.getattr.referral_rootpath),
				"%s", loc.rootpath);
		}
	}
	}

	{
		uint32_t fsid_owner = cd->mds_id;

		if (res->res.getattr.has_referral) {
			fsid_owner = res->res.getattr.referral_owner_mds_id;
		}
		res->res.getattr.fsid_major = referral_fsid_major(fsid_owner);
		res->res.getattr.fsid_minor = referral_fsid_minor(fsid_owner);
	}

	/* Pass the client's requested bitmap to the encoder. */
	memcpy(res->res.getattr.requested, op->arg.getattr.requested,
	       sizeof(res->res.getattr.requested));
	return NFS4_OK;
}

/**
 * Post-setattr quota accounting.
 *
 * Ordering: chown transfer first (using pre-setattr size so the old
 * owner is debited only what they owned), then size-delta adjustment
 * (charged to the new owner).  This avoids double-counting when both
 * size and owner change in a single SETATTR.
 */
static void setattr_quota_post(struct compound_data *cd,
			       const struct nfs4_op *op,
			       const struct mds_inode *pre)
{
	struct mds_inode post;
	uint32_t m = op->arg.setattr.mask;

	if (cd->quota == NULL) {
		return;
	}
	if (cat_getattr(cd, cd->current_fh.fileid, &post) != MDS_OK) {
		return;
	}

	/* 1. chown/chgrp: transfer pre-inode bytes from old→new owner. */
	if ((m & MDS_ATTR_UID) || (m & MDS_ATTR_GID)) {
		uint64_t old_uid = pre->uid;
		uint64_t old_gid = pre->gid;
		uint64_t new_uid = (m & MDS_ATTR_UID)
				   ? op->arg.setattr.attrs.uid : old_uid;
		uint64_t new_gid = (m & MDS_ATTR_GID)
				   ? op->arg.setattr.attrs.gid : old_gid;
		if (old_uid != new_uid || old_gid != new_gid) {
			(void)mds_quota_update_chown(
				cd->quota,
				old_uid, old_gid, new_uid, new_gid,
				pre->size);
		}
	}

	/* 2. Size delta: charged to the (now current) owner. */
	if (m & MDS_ATTR_SIZE) {
		int64_t sd = (int64_t)post.size - (int64_t)pre->size;
		if (sd != 0) {
			quota_submit_adjust(cd, post.uid, post.gid, sd, 0);
		}
	}
}

static bool setattr_times_equal(const struct timespec *lhs,
				const struct timespec *rhs)
{
	if (lhs == NULL || rhs == NULL) {
		return false;
	}

	return lhs->tv_sec == rhs->tv_sec &&
	       lhs->tv_nsec == rhs->tv_nsec;
}

static bool setattr_is_noop(const struct mds_inode *cur,
			    const struct nfs4_arg_setattr *arg)
{
	uint32_t mask;

	if (cur == NULL || arg == NULL) {
		return false;
	}

	mask = arg->mask;
	if ((mask & (MDS_ATTR_ATIME_NOW | MDS_ATTR_MTIME_NOW)) != 0) {
		return false;
	}
	if ((mask & MDS_ATTR_MODE) && arg->attrs.mode != cur->mode) {
		return false;
	}
	if ((mask & MDS_ATTR_UID) && arg->attrs.uid != cur->uid) {
		return false;
	}
	if ((mask & MDS_ATTR_GID) && arg->attrs.gid != cur->gid) {
		return false;
	}
	if ((mask & MDS_ATTR_SIZE) && arg->attrs.size != cur->size) {
		return false;
	}
	if ((mask & MDS_ATTR_ATIME) &&
	    !setattr_times_equal(&arg->attrs.atime, &cur->atime)) {
		return false;
	}
	if ((mask & MDS_ATTR_MTIME) &&
	    !setattr_times_equal(&arg->attrs.mtime, &cur->mtime)) {
		return false;
	}
	if ((mask & MDS_ATTR_FLAGS) && arg->attrs.flags != cur->flags) {
		return false;
	}

	return true;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_setattr(struct compound_data *cd,
				   const struct nfs4_op *op,
				   struct nfs4_result *res)
{
	enum nfs4_status nst;
	enum mds_status st;
	struct mds_inode sa_pre_inode;
	bool sa_pre_valid;
	struct mds_inode sa_attrs;
	uint32_t sa_mask;

	(void)res;
	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	nst = check_subtree_frozen(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	nst = check_repl_health(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/* Validate open stateid if supplied (RFC 8881 §18.30). */
	if (op->arg.setattr.has_stateid && cd->ot != NULL) {
		nst = validate_io_stateid(cd, &op->arg.setattr.stateid,
					  OPEN4_SHARE_ACCESS_WRITE);
		if (nst != NFS4_OK) {
			return nst;
		}
	}
	memset(&sa_pre_inode, 0, sizeof(sa_pre_inode));
	sa_pre_valid = false;
	if (cd->current_inode_valid &&
	    cd->current_inode.fileid == cd->current_fh.fileid) {
		sa_pre_inode = cd->current_inode;
		sa_pre_valid = true;
	} else if (cd->icache != NULL &&
		   inode_cache_get(cd->icache, cd->current_fh.fileid,
				   &sa_pre_inode) == 0) {
		sa_pre_valid = true;
	}

	/*
	 * POSIX DAC: chmod/chown/truncate/utimes permission matrix.
	 * Needs the pre-mutation inode, so force the read when both
	 * the request snapshot and the icache missed.  Runs BEFORE the
	 * no-op shortcut below: a non-owner chmod to the file's
	 * current mode must still fail with EPERM, not succeed as a
	 * no-op.  The helper may rewrite sa_attrs.mode and add
	 * MDS_ATTR_MODE to sa_mask (SGID drop on non-member chmod,
	 * SUID/SGID kill on chown and truncate); the catalogue write
	 * below therefore uses sa_attrs/sa_mask instead of the raw
	 * wire arguments.
	 */
	sa_attrs = op->arg.setattr.attrs;
	sa_mask = op->arg.setattr.mask;
	if (compound_dac_active(cd)) {
		if (!sa_pre_valid) {
			enum mds_status dac_st = compound_inode_get(
				cd, cd->current_fh.fileid, &sa_pre_inode);
			if (dac_st != MDS_OK) {
				return mds_status_to_nfs4(dac_st);
			}
			sa_pre_valid = true;
		}
		nst = compound_setattr_dac_check(cd, &sa_pre_inode,
						 &op->arg.setattr,
						 &sa_attrs, &sa_mask);
		if (nst != NFS4_OK) {
			return nst;
		}
	}

	/*
	 * No-op shortcut -- but only when the DAC pass did not adjust
	 * the write.  A wire-level no-op chown (same uid/gid) still
	 * kills SUID/SGID, so short-circuiting on the ORIGINAL args
	 * would silently drop the folded MODE update.
	 */
	if (sa_mask == op->arg.setattr.mask &&
	    sa_attrs.mode == op->arg.setattr.attrs.mode &&
	    sa_pre_valid &&
	    setattr_is_noop(&sa_pre_inode, &op->arg.setattr)) {
		return NFS4_OK;
	}

	/* Quota: capture pre-setattr inode for delta accounting. */
	if (cd->quota != NULL) {
		if (!sa_pre_valid) {
			sa_pre_valid = (compound_inode_get(
				cd, cd->current_fh.fileid,
				&sa_pre_inode) == MDS_OK);
		}
	}

	/* Quota: pre-check for size increase. */
	if (sa_pre_valid) {
		uint32_t m = op->arg.setattr.mask;
		if ((m & MDS_ATTR_SIZE) &&
		    op->arg.setattr.attrs.size > sa_pre_inode.size) {
			nst = quota_check_bytes(cd, sa_pre_inode.uid,
					sa_pre_inode.gid,
					op->arg.setattr.attrs.size - sa_pre_inode.size);
			if (nst != NFS4_OK) {
				return nst;
			}
		}
	}
	/*
	 * Phase 8b: a SETATTR on a directory changes its attrs,
	 * which dir delegations must be notified about.  We only
	 * recall if the current FH is a directory — file setattrs
	 * do not affect any directory delegation.
	 */
	if (sa_pre_valid && sa_pre_inode.type == MDS_FTYPE_DIR) {
		compound_recall_dir_delegations(cd, cd->current_fh.fileid);
	}

	st = cat_setattr(cd, cd->current_fh.fileid,
			 &sa_attrs, sa_mask);
	/* Invalidate snapshot BEFORE post-mutation re-read. */
	if (st == MDS_OK) {
		compound_inode_invalidate(cd, cd->current_fh.fileid);
	}
	/* Phase D of docs/hpc-nto1-plan.md — a shrinking SETATTR(size)
	 * may turn previously valid file ranges into truncated holes,
	 * so any cached stripe-map snapshot for this fileid must be
	 * dropped before the next LAYOUTGET serves it back to a
	 * client.  We invalidate on every successful size change
	 * (including grow): the cache only holds HPC-Shared inodes
	 * anyway, so non-HPC SETATTRs amortise to a cheap shard-mutex
	 * hash miss.  NULL-safe.
	 *
	 * Phase F of docs/hpc-nto1-plan.md — mirror the same drop on
	 * the LAYOUTCOMMIT aggregator: an explicit SETATTR(size) wins
	 * over any in-memory aggregate, and leaving a stale bucket
	 * around would let the next periodic flush silently restore
	 * the pre-truncate size.  Drop the bucket BEFORE returning so
	 * the next LAYOUTCOMMIT submit (under part B) starts a fresh
	 * aggregation window.  NULL-safe. */
	if (st == MDS_OK &&
	    (op->arg.setattr.mask & MDS_ATTR_SIZE) != 0) {
		layout_cache_invalidate(cd->lcache,
					cd->current_fh.fileid);
		layout_commit_aggregator_drop(cd->lcommit_agg,
					      cd->current_fh.fileid);
	}
	/*
	 * Mark Q5 P03 — layout-recall on truncate.
	 *
	 * RFC 8881 §12.5.5 / §18.30: a SETATTR that changes the file
	 * size invalidates outstanding layouts on that file (a shrink
	 * removes ranges; a grow can only add zeroed bytes, but
	 * holders' caches still need to learn the new size).  We
	 * therefore emit a best-effort whole-file CB_LAYOUTRECALL to
	 * every other client holding a layout on this fileid AFTER
	 * the catalogue size update succeeds.  Doing it post-write
	 * keeps the holder-revoke and the on-disk truth aligned: a
	 * failed cat_setattr leaves layouts intact for the next
	 * attempt.
	 *
	 * Conditions:
	 *   - cat_setattr succeeded (st == MDS_OK)
	 *   - mask carried MDS_ATTR_SIZE
	 *   - we observed the pre-mutation inode (sa_pre_valid) so
	 *     we can confirm the mutation targeted a regular file and
	 *     the size actually changed (the setattr_is_noop check
	 *     above already short-circuits identical-size SETATTRs)
	 *   - cd->lr is wired (NULL-safe — unit-test path leaves it
	 *     unset)
	 *
	 * The byte-range helper's same-client filter
	 * (req_clientid == holder.clientid) skips the requesting
	 * client's own layout, so a client that truncates its own
	 * file does not recall its own grant.  layout_type=0 falls
	 * back to the coordinator's default (LAYOUT4_FLEX_FILES).
	 */
	/*
	 * Under transient_state_cache=on the layout_state table is
	 * empty from this MDS's perspective (every grant site is
	 * gated on `!cd->skip_transient_ndb`), so the recall scan
	 * is a guaranteed-miss NDB round-trip on the SETATTR-with-
	 * size hot path.  Same trade-off as op_layoutget; see the
	 * detailed rationale in compound_layout.c above the
	 * matching call site.
	 */
	if (st == MDS_OK &&
	    (op->arg.setattr.mask & MDS_ATTR_SIZE) != 0 &&
	    sa_pre_valid &&
	    sa_pre_inode.type == MDS_FTYPE_REG &&
	    op->arg.setattr.attrs.size != sa_pre_inode.size &&
	    cd->lr != NULL &&
	    !cd->skip_transient_ndb) {
		(void)layout_recall_byte_range_for_holders(
			cd->lr,
			cd->current_fh.fileid,
			cd->clientid,
			LAYOUTIOMODE4_RW,    /* recall ANY holder */
			0, UINT64_MAX,        /* whole file */
			0,                    /* layout_type → default */
			NULL);
	}
	if (st == MDS_OK && sa_pre_valid) {
		setattr_quota_post(cd, op, &sa_pre_inode);
	}
	return mds_status_to_nfs4(st);
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_create(struct compound_data *cd,
				  const struct nfs4_op *op,
				  struct nfs4_result *res)
{
	const struct nfs4_arg_create *a = &op->arg.create;
	enum nfs4_status nst;
	enum mds_status st;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	/*
	 * RFC 8881 §18.4.4 / §12.7 — component4 validation.  Empty
	 * name → NFS4ERR_INVAL; "." / ".." → NFS4ERR_BADNAME; bad
	 * UTF-8 → NFS4ERR_INVAL.  Validate BEFORE the freeze /
	 * health checks so a malformed argument cannot mask state
	 * with a transient DELAY.  Same precedence as op_lookup /
	 * op_rename above.
	 */
	if (a->name_too_long || a->link_target_too_long) {
		return NFS4ERR_NAMETOOLONG;
	}
	nst = compound_validate_name(a->name);
	if (nst != NFS4_OK) {
		return nst;
	}
	/*
	 * POSIX: creating block/character device nodes requires
	 * appropriate privileges (mknod(2) -> EPERM for non-root).
	 */
	if ((a->type == MDS_FTYPE_BLKDEV || a->type == MDS_FTYPE_CHRDEV) &&
	    compound_dac_active(cd) && cd->cred_uid != 0) {
		return NFS4ERR_PERM;
	}
	nst = check_subtree_frozen(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	nst = check_repl_health(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/* Xattr namespace: CREATE creates a new xattr with empty value. */
	if (is_xattr_fh(cd->current_fh.fileid)) {
		uint64_t base = xattr_base_fileid(cd->current_fh.fileid);
		nst = check_fh_frozen(cd, base, "");
		if (nst != NFS4_OK) {
			return nst;
}
		if (cd->cat == NULL) {
			return NFS4ERR_INVAL;
		}
		st = mds_cat_xattr_put(cd->cat, NULL,
				       base, a->name, NULL, 0);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}

		(void)snprintf(cd->xattr_name, sizeof(cd->xattr_name),
			 "%s", a->name);
		cd->xattr_obj_set = true;
		return NFS4_OK;
	}

	/*
	 * Effective UID/GID: fall back to AUTH_SYS caller
	 * credentials when the client omits owner attrs.
	 */
	uint64_t eff_uid = a->uid;
	uint64_t eff_gid = a->gid;
	if (eff_uid == 0 && cd->cred_uid != 0) {
		eff_uid = cd->cred_uid;
	}
	if (eff_gid == 0 && cd->cred_gid != 0) {
		eff_gid = cd->cred_gid;
	}

	/* Quota: check inode quota before creating. */
	nst = quota_check_inode(cd, eff_uid, eff_gid);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* Phase 8d: NOTIFY4_ADD_ENTRY if the delegation holder
	 * subscribed; otherwise recall.  Preserves Phase 8b
	 * behaviour when no delegation holder is subscribed to
	 * ADD events (common when clients only want attr events). */
	compound_notify_or_recall_dir(cd, cd->current_fh.fileid,
				      NOTIFY4_ADD_ENTRY,
				      a->name, NULL);

	/* Parent must exist and be a directory before CREATE (RFC 8881
	 * §18.16.4).  Also capture change_before for encode_res_create()
	 * without reading from the union-aliased r->res.change_info
	 * fields, which overlap r->res.create.inode. */
	uint64_t parent_change_before = 0;
	{
		struct mds_inode create_parent;

		st = compound_inode_get(cd, cd->current_fh.fileid,
					&create_parent);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}
		if (create_parent.type != MDS_FTYPE_DIR) {
			return NFS4ERR_NOTDIR;
		}
		/* POSIX DAC: write+search on the parent directory. */
		nst = compound_dir_mutate_check(cd, &create_parent);
		if (nst != NFS4_OK) {
			return nst;
		}
		parent_change_before = create_parent.change;
	}

	st = cat_create(cd, cd->current_fh.fileid, a->name,
			   a->type, a->mode, eff_uid, eff_gid,
			   cd->prealloc,
		       &res->res.create.inode);
	if (st == MDS_ERR_EXISTS) {
		/* Concurrent mkdir/create: flush any negative dirent
		 * cache entry so subsequent LOOKUPs on this client
		 * session see the committed object. */
		compound_dirent_invalidate(cd, cd->current_fh.fileid,
					   a->name);
	}
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
	}
	res->res.create.parent_change_before = parent_change_before;
	/* Best-effort re-read of the parent for the "after" value.
	 * On cache-invalidate / re-read failure we fall back to
	 * before+1 so the encoded pair stays monotonically
	 * increasing (matches our FATTR4_CHANGE_ATTR_TYPE =
	 * MONOTONIC_INCR advertisement). */
	{
		struct mds_inode create_parent_post;
		compound_inode_invalidate(cd, cd->current_fh.fileid);
		if (cat_getattr(cd, cd->current_fh.fileid,
				&create_parent_post) == MDS_OK) {
			res->res.create.parent_change_after =
				create_parent_post.change;
		} else {
			res->res.create.parent_change_after =
				parent_change_before + 1;
		}
	}
	compound_maybe_enqueue_ds_prepare(cd, &res->res.create.inode);

	/* Phase B HPC-Shared: propagate the bit from the parent to the
	 * new child (file or directory).  Best-effort — a transient
	 * catalogue error here is non-fatal for the CREATE itself. */
	hpc_shared_inherit_from_parent(cd, cd->current_fh.fileid,
				       &res->res.create.inode);

	/* Invalidate dirent cache for the new name (flush any negative entry). */
	compound_dirent_invalidate(cd, cd->current_fh.fileid, a->name);

	/*
	 * Symlinks: store the link target in the inline_data table.
	 * When the commit queue is enabled, route this through
	 * COMMIT_OP_INLINE_WRITE so the payload is replicated.
	 */
	if (a->type == MDS_FTYPE_SYMLINK) {
		if (cd->cq != NULL) {
			struct commit_op lt_op;

			memset(&lt_op, 0, sizeof(lt_op));
			lt_op.type = COMMIT_OP_INLINE_WRITE;
			lt_op.args.inline_write.fileid =
				res->res.create.inode.fileid;
			lt_op.args.inline_write.offset = 0;
			lt_op.args.inline_write.len = a->link_target_len;
			lt_op.args.inline_write.data =
				(const uint8_t *)a->link_target;
			st = commit_queue_submit(cd->cq, &lt_op);
			if (st != MDS_OK) {
				return mds_status_to_nfs4(st);
			}
		} else {
			/* Direct catalogue path: write symlink target. */
			st = mds_cat_inline_put(cd->cat, NULL,
					res->res.create.inode.fileid,
					a->link_target,
					a->link_target_len);
			if (st != MDS_OK) {
				return mds_status_to_nfs4(st);
			}
			/* Update inode size to reflect symlink target. */
			{
				struct mds_inode inode;
				struct timespec now;

				st = mds_cat_ns_getattr(cd->cat,
					res->res.create.inode.fileid, &inode);
				if (st == MDS_OK) {
					clock_gettime(CLOCK_REALTIME, &now);
					inode.mtime = now;
					inode.ctime = now;
					inode.change++;
					inode.size = a->link_target_len;
					inode.space_used = a->link_target_len;
					(void)mds_cat_inode_put(cd->cat, NULL,
							&inode);
				}
			}
		}
		res->res.create.inode.size = a->link_target_len;
		res->res.create.inode.space_used = a->link_target_len;
	}

	/* Per RFC 8881: CREATE changes the current FH to the new object. */
	cd->current_fh.fileid = res->res.create.inode.fileid;
	/* FH-encoded subtree ownership: the just-created inode lives on
	 * this MDS — stamp owner_mds_id and copy the freshly-allocated
	 * generation so the FH we emit on a subsequent GETFH carries
	 * full v1 routing/freshness metadata. */
	cd->current_fh.owner_mds_id = cd->mds_id;
	cd->current_fh.generation = res->res.create.inode.generation;
	cd->current_inode = res->res.create.inode;
	cd->current_inode_valid = true;
	compound_ro_txn_reset(cd);

	/* Warm the global inode cache so the NFS client's immediate
	 * GETATTR (which always follows CREATE) hits the cache instead
	 * of doing a full RonDB round-trip for an inode we just built. */
	if (cd->icache != NULL) {
		inode_cache_put(cd->icache, &res->res.create.inode);
	}

	/* Quota: account +1 inode for the new object. */
	quota_submit_adjust(cd, eff_uid, eff_gid, 0, 1);
	return NFS4_OK;
}

/*
 * On the final unlink of a regular file, schedule cleanup of every
 * DS-side data file backing the inode by enqueueing GC entries and
 * dropping the orphaned stripe map.
 *
 * Why this exists: cat_remove() drops the inode and dirent (when
 * nlink reaches 0) but does not touch the stripe map record or any
 * DS-side blob.  Without this helper:
 *   - the per-DS data file at {mount}/data/{fileid}_{stripe}_{mirror}
 *     leaks forever, eventually filling the DS export, and
 *   - the catalogue stripe_map row keyed by fileid becomes
 *     unreferenced metadata.
 *
 * Strategy: enqueue one GC entry per UNIQUE ds_id observed in the
 * stripe map (the worker brute-forces stripe/mirror coordinates,
 * bounded by MDS_MAX_STRIPES * MDS_MAX_MIRRORS — currently 4096
 * cheap unlinks per DS, most ENOENT, with an early-out in ds_gc.c
 * once the stripe count is exhausted).  Catalogue stripe rows are
 * deleted in the ns_remove transaction; ds_gc repeats stripe_map_del
 * idempotently after DS bytes are gone.  All steps are
 * best-effort: a failure here only forfeits this round of cleanup;
 * the GC table will catch the next pass.  We never propagate the
 * failure back to the client — the unlink semantics from the user's
 * point of view are already satisfied.
 *
 * Caller contract: invoke ONLY after cat_remove() returns MDS_OK,
 * and ONLY when the looked-up inode held nlink == 1 and is a
 * regular file (i.e. this remove was the last name for the inode).
 */
static void enqueue_gc_for_final_unlink(struct compound_data *cd,
					uint64_t fileid,
					struct mds_ds_map_entry *entries_prefetch,
					uint32_t stripe_count_prefetch,
					uint32_t mirror_count_prefetch)
{
	struct mds_ds_map_entry *entries = entries_prefetch;
	uint32_t stripe_count = stripe_count_prefetch;
	uint32_t stripe_unit = 0;
	uint32_t mirror_count = mirror_count_prefetch;
	enum mds_status st;
	uint32_t total;
	uint32_t i;
	bool entries_owned = false;
	/* De-dup buffer: at most stripe_count*mirror_count unique IDs.
	 * Heap-allocated (Phase A of docs/hpc-nto1-plan.md): with
	 * MDS_MAX_STRIPES at 1024 the worst-case 4096-uint32 buffer is
	 * 16 KiB — still OK on the stack, but tomorrow's MDS_MAX_MIRRORS
	 * bump or a future structurally larger entry would push it past
	 * the 64 KiB stack-safety threshold.  Keep it on the heap so the
	 * scaling axis is never load-bearing on the stack again. */
	uint32_t *seen = NULL;
	uint32_t seen_count = 0;

	if (cd == NULL || cd->cat == NULL) {
		return;
	}

	if (entries == NULL) {
		st = cat_stripe_map_get(cd, fileid, &stripe_count, &stripe_unit,
					&mirror_count, &entries);
		entries_owned = true;
	} else {
		st = (entries != NULL && stripe_count > 0 && mirror_count > 0)
			? MDS_OK : MDS_ERR_NOTFOUND;
	}
	(void)stripe_unit;
	if (st != MDS_OK || entries == NULL ||
	    stripe_count == 0 || mirror_count == 0) {
		if (entries_owned) {
			free(entries);
		}
		return;
	}

	total = stripe_count * mirror_count;
	if (total > (uint32_t)(MDS_MAX_STRIPES * MDS_MAX_MIRRORS)) {
		/* Defensive: catalogue invariant violated. */
		free(entries);
		return;
	}

	seen = calloc(total, sizeof(*seen));
	if (seen == NULL) {
		/* Out of memory — skip GC scheduling for this unlink.
		 * The stripe_map row stays around; a later cleanup pass
		 * (admin or restart-time scan) will pick it up. */
		free(entries);
		return;
	}

	for (i = 0; i < total; i++) {
		uint32_t ds_id = entries[i].ds_id;
		uint32_t fh_len = entries[i].nfs_fh_len;
		bool already_enqueued = false;
		uint32_t k;

		for (k = 0; k < seen_count; k++) {
			if (seen[k] == ds_id) {
				already_enqueued = true;
				break;
			}
		}
		if (already_enqueued) {
			continue;
		}
		seen[seen_count++] = ds_id;

		/* fh_len may legitimately be 0 (DS file never came
		 * online — the create raced with a layout-pending
		 * client).  We still enqueue: the worker uses ds_id
		 * + fileid + stripe/mirror sweep to drive the
		 * unlink, so the FH is informational only.  Clamp
		 * to MDS_NFS_FH_MAX so a malformed catalogue row
		 * cannot drive an OOB read on the GC enqueue. */
		if (fh_len > MDS_NFS_FH_MAX) {
			fh_len = MDS_NFS_FH_MAX;
		}
		(void)mds_cat_gc_enqueue(cd->cat, NULL, fileid, ds_id,
					 entries[i].nfs_fh, fh_len);
	}

	/* Stripe catalogue rows are deleted inside the ns_remove NDB
	 * transaction on final unlink.  Any orphaned stripe_map rows are
	 * dropped asynchronously by ds_gc after DS file cleanup. */

	/* Phase D of docs/hpc-nto1-plan.md — keep the per-MDS HPC
	 * layout cache coherent with the catalogue.  NULL-safe; the
	 * cache itself only holds entries for HPC-Shared inodes, so
	 * for plain inodes this is a cheap shard-mutex hash miss. */
	layout_cache_invalidate(cd->lcache, fileid);

	/* Phase F of docs/hpc-nto1-plan.md — the file is gone, so any
	 * pending size/mtime aggregate is meaningless.  Drop the
	 * bucket without flushing so the timer thread does not race
	 * the unlink and try to setattr a deleted fileid.  NULL-safe. */
	layout_commit_aggregator_drop(cd->lcommit_agg, fileid);

	free(seen);
	if (entries_owned) {
		free(entries);
	}
}

enum nfs4_status op_remove(struct compound_data *cd,
				  const struct nfs4_op *op,
				  struct nfs4_result *res)
{
	enum nfs4_status nst;
	enum mds_status st;

	(void)res;
	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	/*
	 * RFC 8881 §18.25.4 / §12.7 — component4 validation.  Empty
	 * → NFS4ERR_INVAL; "." / ".." → NFS4ERR_BADNAME; bad UTF-8 →
	 * NFS4ERR_INVAL.  Same shape and precedence as op_lookup,
	 * op_create, op_rename above.
	 */
	if (op->arg.remove.name_too_long) {
		return NFS4ERR_NAMETOOLONG;
	}
	nst = compound_validate_name(op->arg.remove.name);
	if (nst != NFS4_OK) {
		return nst;
	}
	nst = check_subtree_frozen(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	nst = check_repl_health(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/* Xattr namespace: REMOVE deletes an xattr. */
	if (is_xattr_fh(cd->current_fh.fileid)) {
		uint64_t base = xattr_base_fileid(cd->current_fh.fileid);
		nst = check_fh_frozen(cd, base, "");
		if (nst != NFS4_OK) {
			return nst;
}
		if (cd->cat == NULL) {
			return NFS4ERR_INVAL;
		}
		st = mds_cat_xattr_del(cd->cat, NULL,
				       base, op->arg.remove.name);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}

		cd->xattr_obj_set = false;
		return NFS4_OK;
	}

	/* R1.3: capture parent change before mutation for change_info.
	 * The same read feeds the POSIX DAC checks (write+search on the
	 * directory here; the sticky rule after the child lookup). */
	uint64_t rm_change_before = 0;
	struct mds_inode rm_parent;
	bool rm_parent_valid = false;
	if (compound_inode_get(cd, cd->current_fh.fileid,
			       &rm_parent) == MDS_OK) {
		rm_change_before = rm_parent.change;
		rm_parent_valid = true;
	}
	/* POSIX DAC: removing an entry mutates the directory. */
	if (rm_parent_valid) {
		nst = compound_dir_mutate_check(cd, &rm_parent);
		if (nst != NFS4_OK) {
			return nst;
		}
	}

	/* Quota: look up child to account for removal.
	 *
	 * The same lookup also tells us whether this is the final
	 * unlink of a regular file: nlink == 1 + type == REG means
	 * the very next cat_remove() will drop the inode to zero
	 * links and orphan its DS-side data files.  We capture the
	 * answer here — BEFORE the mutation — so we can schedule
	 * GC against a still-coherent stripe map after a successful
	 * remove. */
	struct mds_inode rm_inode;
	st = cat_lookup(cd, cd->current_fh.fileid,
			   op->arg.remove.name, &rm_inode);
	/* POSIX DAC: S_ISVTX restricted deletion (sticky directories). */
	if (st == MDS_OK && rm_parent_valid) {
		nst = compound_sticky_delete_check(cd, &rm_parent, &rm_inode);
		if (nst != NFS4_OK) {
			return nst;
		}
	}
	/*
	 * RFC 8881 S18.25.4 / POSIX rmdir(2): removing a non-empty
	 * directory MUST fail with NFS4ERR_NOTEMPTY.  The catalogue
	 * remove primitive drops the dirent unconditionally, which
	 * silently orphaned every child (nfstest_posix rmdir,
	 * pjdfstest rmdir/06).  Same emptiness probe the RENAME
	 * dir-overwrite path already uses; checked BEFORE any
	 * delegation-notify / layout-recall side effects.
	 */
	if (st == MDS_OK && rm_inode.type == MDS_FTYPE_DIR &&
	    cd->cat != NULL) {
		bool rm_dir_empty = true;

		if (mds_cat_dir_is_empty(cd->cat, rm_inode.fileid,
					 &rm_dir_empty) == MDS_OK &&
		    !rm_dir_empty) {
			return NFS4ERR_NOTEMPTY;
		}
	}
	bool rm_quota = (st == MDS_OK && cd->quota != NULL);
	bool rm_final_data_unlink =
		(st == MDS_OK &&
		 rm_inode.type == MDS_FTYPE_REG &&
		 rm_inode.nlink == 1);
	uint64_t rm_fileid = (st == MDS_OK) ? rm_inode.fileid : 0;
	struct mds_ds_map_entry *rm_sm_entries = NULL;
	uint32_t rm_sm_sc = 0;
	uint32_t rm_sm_su = 0;
	uint32_t rm_sm_mc = 0;

	/*
	 * v9: a single-stripe file (MDS_IFLAG_INLINE_STRIPE) carries its one
	 * DS entry on the inode we already read in cat_lookup.  Build the
	 * stripe-map snapshot from it ONCE here -- independent of the
	 * proxy/fence path below -- so both the DS fence AND the GC enqueue
	 * (which cleans up the DS backing file) use it with no
	 * cat_stripe_map_get.  Without this the GC path would fall back to
	 * cat_stripe_map_get, get NOTFOUND (inline files have no stripe
	 * rows), and skip enqueue -> the DS file would leak.
	 */
	if (rm_final_data_unlink && rm_fileid != 0 &&
	    (rm_inode.flags & MDS_IFLAG_INLINE_STRIPE)) {
		rm_sm_entries = calloc(1, sizeof(*rm_sm_entries));
		if (rm_sm_entries != NULL) {
			uint32_t fhl = rm_inode.inline_fh_len;

			if (fhl > MDS_NFS_FH_MAX) {
				fhl = MDS_NFS_FH_MAX;
			}
			rm_sm_entries[0].ds_id = rm_inode.inline_ds_id;
			rm_sm_entries[0].nfs_fh_len = fhl;
			if (fhl > 0) {
				memcpy(rm_sm_entries[0].nfs_fh,
				       rm_inode.inline_fh, fhl);
			}
			rm_sm_sc = 1;
			rm_sm_mc = 1;
			rm_sm_su = rm_inode.stripe_unit;
		}
	}

	/* Phase 8d: NOTIFY4_REMOVE_ENTRY or recall. */
	compound_notify_or_recall_dir(cd, cd->current_fh.fileid,
				      NOTIFY4_REMOVE_ENTRY,
				      op->arg.remove.name, NULL);
	if (rm_final_data_unlink && rm_fileid != 0) {
		/*
		 * Mark Q5 P04 — layout-recall on final unlink.
		 *
		 * Revoke layouts BEFORE cat_remove() drops the final
		 * namespace entry.  Linux answers CB_LAYOUTRECALL by
		 * sending LAYOUTRETURN; that compound starts with PUTFH
		 * on the recalled fileid.  If we remove the inode first,
		 * the holder sees NFS4ERR_STALE on that PUTFH and enters
		 * migration recovery instead of returning the layout,
		 * leaving the peer's unlink path permanently wedged.
		 *
		 * layout_recall_revoke_all_for_unlink() is intentionally
		 * stronger than the ordinary byte-range conflict helper:
		 * it sends best-effort callbacks while PUTFH can still
		 * resolve the object, then unconditionally drops every
		 * layout-state row for this fileid.  If catalogue recall
		 * enumeration fails, fail closed with DELAY rather than
		 * removing the namespace entry while stale layouts remain.
		 */
		/*
		 * Under transient_state_cache=on the layout_state table is empty
		 * (every grant site is gated on !cd->skip_transient_ndb), so this
		 * unlink recall enumeration is a guaranteed-miss layout_state scan
		 * on the REMOVE hot path. Under mass-delete concurrency those scans
		 * exhaust RonDB scan resources and stall removes (10s+). Skip it,
		 * mirroring the truncate path above. DS files are still fenced below
		 * and the inode is removed, so any transient layout holder sees
		 * STALE/EACCES on its next op.
		 */
		if (cd->lr != NULL && !cd->skip_transient_ndb) {
			int lrc = layout_recall_revoke_all_for_unlink(
				cd->lr, rm_fileid, NULL);
			if (lrc != 0) {
				return NFS4ERR_DELAY;
			}
		}

		/*
		 * Prefetch the stripe map once so cat_remove_known() gets
		 * the stripe count and the post-remove GC enqueue below can
		 * reuse the entries.  No DS-side fence on the REMOVE path:
		 * the namespace remove commits and the ds_gc reaper unlinks
		 * the backing objects, and that unlink is what revokes any
		 * straggler I/O (deferred-unlink model).
		 * Layouts were already recalled above.
		 */
		if (cd->cat != NULL && rm_sm_entries == NULL) {
			(void)mds_cat_stripe_map_get(cd->cat, rm_fileid,
					&rm_sm_sc, &rm_sm_su, &rm_sm_mc,
					&rm_sm_entries);
		}

		/*
		 * Free any in-memory delegation grants that were recorded
		 * for this fileid by a prior op_open.  This is done before
		 * cat_remove() for the same final-unlink invalidation
		 * boundary; it is NULL-safe but keep the guard explicit.
		 */
		if (cd->dt != NULL) {
			deleg_revoke_file(cd->dt, rm_fileid);
		}
	}


	/*
	 * Async-REMOVE fast path (ported; delete-at-ack).  Eligible:
	 * final unlink of a regular file whose parent change_info can be
	 * served from the parent_touch aggregator, and no live writer
	 * holds the file open (delete-at-ack must never destroy stripe
	 * data under an active open; such removes keep the synchronous
	 * path).  The recall /
	 * delegation teardown above already ran.  The submit commits ONE
	 * durable transaction (manifest row + dirent DELETE + inode
	 * DELETE_PENDING flag), so the name is gone on EVERY MDS before
	 * the client is acked; the drainer finalizes the inode, DS
	 * objects and quota off the request thread.  Any failure falls
	 * through to the synchronous path below.
	 */
	if (cd->rmf != NULL && st == MDS_OK && rm_final_data_unlink &&
	    rm_fileid != 0 && cd->ot != NULL &&
	    open_state_file_has_writers(cd->ot, rm_fileid) == 0 &&
	    compound_parent_defer_ok(cd) &&
	    parent_touch_prepare(cd->pt, cd->current_fh.fileid,
				 rm_change_before) == 0) {
		if (remove_manifest_submit(cd->rmf,
					   cd->current_fh.fileid,
					   op->arg.remove.name,
					   rm_fileid,
					   rm_inode.generation,
					   true) == 0) {
			struct timespec rm_async_now;
			uint64_t rm_async_before = 0;
			uint64_t rm_async_after = 0;

			clock_gettime(CLOCK_REALTIME, &rm_async_now);
			if (parent_touch_commit_prepared(cd->pt,
					cd->current_fh.fileid,
					rm_async_now,
					&rm_async_before,
					&rm_async_after) == 0) {
				res->res.change_info.before =
					rm_async_before;
				res->res.change_info.after =
					rm_async_after;
			} else {
				res->res.change_info.before =
					rm_change_before;
				res->res.change_info.after =
					rm_change_before + 1;
			}
			compound_dirent_invalidate(cd,
						   cd->current_fh.fileid,
						   op->arg.remove.name);
			compound_inode_invalidate(cd, cd->current_fh.fileid);
			/* Ack txn mutated the child inode (DELETE_PENDING):
			 * drop any cached copy. */
			compound_inode_invalidate(cd, rm_fileid);
			free(rm_sm_entries);
			return NFS4_OK;
		}
		parent_touch_abort_prepared(cd->pt, cd->current_fh.fileid);
	}

	st = (st == MDS_OK)
		? cat_remove_known(cd, cd->current_fh.fileid,
				   op->arg.remove.name, &rm_inode,
				   rm_sm_sc)
		: cat_remove(cd, cd->current_fh.fileid,
			     op->arg.remove.name);
	if (st == MDS_OK && rm_quota) {
		quota_submit_adjust(cd, rm_inode.uid, rm_inode.gid,
				    -(int64_t)rm_inode.size, -1);
	}
	if (st == MDS_OK) {
		/* Invalidate dirent cache for removed entry. */
		compound_dirent_invalidate(cd, cd->current_fh.fileid,
					   op->arg.remove.name);
		/*
		 * Drop the removed child's inode from the global icache.
		 * Without this, a subsequent PUTFH on the removed fileid
		 * (the client may still hold the FH from an earlier OPEN
		 * or GETFH and use it before getting ESTALE the
		 * "natural" way) reads the stale alive inode out of
		 * the icache instead of touching NDB, and downstream
		 * GETATTR / OPEN observe the file as still present.
		 * This is the IOR "file cannot be deleted" symptom:
		 * the unlink returns 0 but a verifying stat still
		 * succeeds against the stale cache.  rm_fileid is
		 * captured pre-mutation from cat_lookup above, so it
		 * is the correct child id even after cat_remove has
		 * dropped the dirent.
		 */
		if (rm_fileid != 0) {
			compound_inode_invalidate(cd, rm_fileid);
		}
		struct mds_inode parent_post;
		/* Invalidate BEFORE post-mutation re-read. */
		compound_inode_invalidate(cd, cd->current_fh.fileid);
		if (cat_getattr(cd, cd->current_fh.fileid,
				   &parent_post) == MDS_OK) {
			res->res.change_info.after = parent_post.change;
			res->res.change_info.before = rm_change_before;
		}

		/* Final unlink of a regular file: schedule DS-side
		 * data cleanup via the GC queue.  Best-effort --
		 * failures here do not affect the client-visible
		 * remove status; the next pass picks up any rows
		 * we miss. */
		if (rm_final_data_unlink && rm_fileid != 0) {
			enqueue_gc_for_final_unlink(cd, rm_fileid,
						    rm_sm_entries,
						    rm_sm_sc, rm_sm_mc);
			rm_sm_entries = NULL;
		}
	}
	free(rm_sm_entries);
	return mds_status_to_nfs4(st);
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
/* Helper: find dirent name by child fileid in a parent directory. */
struct find_name_ctx {
	uint64_t target;
	char name[256];
	bool found;
};
/** Catalogue-typed adapter for find_name_ctx (used by cat_readdir). */
static int find_name_cat_cb(const struct mds_cat_dirent *entry, void *arg)
{
	struct find_name_ctx *c = arg;

	if (entry->fileid == c->target) {
		(void)snprintf(c->name, sizeof(c->name), "%s",
			       entry->name);
		c->found = true;
		return 1;
	}
	return 0;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_rename(struct compound_data *cd,
				  const struct nfs4_op *op,
				  struct nfs4_result *res)
{
	enum nfs4_status nst;
	enum mds_status st;

	(void)res;

	/* saved_fh = source directory, current_fh = target directory. */
	nst = require_saved_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/*
	 * RFC 8881 §18.26.4 / §12.7 — component4 validation for both
	 * names.  Empty → NFS4ERR_INVAL (pynfs SEQ9d).  "." / ".." →
	 * NFS4ERR_BADNAME (pynfs RNM10 testDotsOldname / RNM11
	 * testDotsNewname).  Invalid UTF-8 → NFS4ERR_INVAL (pynfs
	 * RNM8/9 testBadutf8*).  Both names live in NUL-terminated
	 * fixed buffers so the helper can use strlen() safely.
	 * Validation runs BEFORE the freeze / health checks: a
	 * malformed argument is a client bug that deserves an
	 * immediate error regardless of subtree state, matching the
	 * precedence implied by RFC 8881 §2.6.3.1.
	 */
	if (op->arg.rename.src_name_too_long ||
	    op->arg.rename.dst_name_too_long) {
		return NFS4ERR_NAMETOOLONG;
	}
	nst = compound_validate_name(op->arg.rename.src_name);
	if (nst != NFS4_OK) {
		return nst;
	}
	nst = compound_validate_name(op->arg.rename.dst_name);
	if (nst != NFS4_OK) {
		return nst;
	}

	/* Check BOTH target and source directories for freeze. */
	nst = check_subtree_frozen(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	nst = check_fh_frozen(cd, cd->saved_fh.fileid, cd->saved_path);
	if (nst != NFS4_OK) {
		return nst;
}
	nst = check_repl_health(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/*
	 * POSIX DAC: RENAME mutates both directories, so the caller
	 * needs write+search on each.  Sticky directories additionally
	 * restrict who may move an entry out of the source dir and who
	 * may overwrite an existing entry in the destination dir.
	 * Checked BEFORE the self-rename no-op shortcut below: Linux
	 * evaluates permissions before the same-inode short circuit,
	 * so an unauthorised self-rename fails rather than no-ops.
	 */
	if (compound_dac_active(cd)) {
		struct mds_inode rn_src_dir;
		struct mds_inode rn_dst_dir;
		struct mds_inode rn_victim;
		enum mds_status rn_st;

		rn_st = compound_inode_get(cd, cd->saved_fh.fileid,
					   &rn_src_dir);
		if (rn_st != MDS_OK) {
			return mds_status_to_nfs4(rn_st);
		}
		nst = compound_dir_mutate_check(cd, &rn_src_dir);
		if (nst != NFS4_OK) {
			return nst;
		}
		rn_st = compound_inode_get(cd, cd->current_fh.fileid,
					   &rn_dst_dir);
		if (rn_st != MDS_OK) {
			return mds_status_to_nfs4(rn_st);
		}
		nst = compound_dir_mutate_check(cd, &rn_dst_dir);
		if (nst != NFS4_OK) {
			return nst;
		}
		if (compound_lookup_local_child(cd, cd->saved_fh.fileid,
				op->arg.rename.src_name,
				&rn_victim) == MDS_OK) {
			nst = compound_sticky_delete_check(cd, &rn_src_dir,
							   &rn_victim);
			if (nst != NFS4_OK) {
				return nst;
			}
		}
		if (compound_lookup_local_child(cd, cd->current_fh.fileid,
				op->arg.rename.dst_name,
				&rn_victim) == MDS_OK) {
			nst = compound_sticky_delete_check(cd, &rn_dst_dir,
							   &rn_victim);
			if (nst != NFS4_OK) {
				return nst;
			}
		}
	}

	/* Resolve paths if empty (PUTFH clears them).
	 * Walk parent_fileid to build path for cross-subtree detection. */
	if (cd->smap != NULL && cd->current_path[0] == '\0') {
		struct mds_inode dst_dir;
		if (compound_inode_get(cd, cd->current_fh.fileid,
				       &dst_dir) == MDS_OK &&
		    dst_dir.parent_fileid == MDS_FILEID_ROOT) {
			struct find_name_ctx fnc = {
				.target = cd->current_fh.fileid,
				.found = false,
			};
			if (cat_readdir(cd, MDS_FILEID_ROOT, NULL, 0,
					find_name_cat_cb, &fnc) == MDS_OK
			    && fnc.found) {
				(void)snprintf(cd->current_path,
					sizeof(cd->current_path), "/%s", fnc.name);
			}
		}
	}
	if (cd->smap != NULL && cd->saved_path[0] == '\0') {
		struct mds_inode src_dir;
		if (compound_inode_get(cd, cd->saved_fh.fileid,
				       &src_dir) == MDS_OK &&
		    src_dir.fileid == MDS_FILEID_ROOT) {
			(void)snprintf(cd->saved_path,
				sizeof(cd->saved_path), "/");
		}
	}

	/* Cross-subtree rename detection. */
	if (cd->smap != NULL &&
	    cd->saved_path[0] != '\0' && cd->current_path[0] != '\0') {
		bool src_local = subtree_map_is_local(cd->smap, cd->saved_path);
		bool dst_local = subtree_map_is_local(cd->smap, cd->current_path);

		if (src_local && !dst_local) {
			/* Cross-subtree rename: source is local (this MDS owns
			 * the src subtree), destination is owned by another
			 * MDS.  In the single-RonDB-cluster deployment model
			 * (all MDSes point at the same NDB cluster), this is
			 * structurally identical to a same-subtree cross-dir
			 * rename: both directories' rows live on the same data
			 * nodes, and rondb_shim_rename already batches every
			 * mutation (delete src dirent + write dst dirent +
			 * interpreted parent updates + child inode
			 * parent_fileid change) into one NdbTransaction whose
			 * cross-partition atomicity is guaranteed by NDB's
			 * internal 2PC.
			 *
			 * The old gRPC 2PC (src/cluster/rename_2pc.c) was
			 * necessary in the LMDB era when each MDS held its own
			 * metadata store; it is retained today only for
			 * hypothetical future multi-cluster deployments and for
			 * the test_rename_2pc unit coverage.  The runtime path
			 * now goes through cat_rename — ~6 gRPC RTs collapse to
			 * one NDB commit (~10-15 ms → ~2-3 ms per cross-subtree
			 * rename on the lab).  See
			 * docs/design-post-phase3-candidates.md Candidate 3. */
			if (cd->cat == NULL) {
				return NFS4ERR_XDEV;
			}
			st = cat_rename(cd,
				cd->saved_fh.fileid, op->arg.rename.src_name,
				cd->current_fh.fileid, op->arg.rename.dst_name);
			if (st == MDS_OK) {
				compound_inode_invalidate(cd,
					cd->saved_fh.fileid);
				compound_inode_invalidate(cd,
					cd->current_fh.fileid);
				compound_dirent_invalidate(cd,
					cd->saved_fh.fileid,
					op->arg.rename.src_name);
				compound_dirent_invalidate(cd,
					cd->current_fh.fileid,
					op->arg.rename.dst_name);
			}
			return mds_status_to_nfs4(st);
		}
		if (!src_local && dst_local) {
			/* Remote source, local destination — the remote
			 * MDS should be the coordinator.  Refuse here. */
			return NFS4ERR_XDEV;
		}
		if (!src_local && !dst_local) {
			/*
			 * Both paths fall outside any subtree this MDS
			 * actively owns.  In the single-RonDB-cluster
			 * deployment model (the only mode supported by
			 * cat_rename) this is NOT cross-device: every MDS
			 * sees the same NDB metadata, and the shard map
			 * is purely an ownership hint for routing.  An
			 * unmapped path — e.g. the pynfs default test root
			 * `/pynfs-test/...`, which is not configured as
			 * any shard — is reachable by every node.
			 *
			 * Returning NFS4ERR_XDEV here was over-conservative
			 * and broke RFC 5661 §18.26 same-directory rename
			 * for any client whose mount path was not pre-
			 * declared in the shard map (pynfs SEQ10b
			 * testReplayCache007).  Fall through to the normal
			 * cat_rename path; RonDB's cross-partition 2PC
			 * handles atomicity even if src and dst happened
			 * to live on different shards. */
		}
		/* Both local — cross-shard rename (Phase 4). */
		if (cd->current_shard != NULL &&
		    cd->saved_shard != NULL &&
		    cd->current_shard->shard_id != cd->saved_shard->shard_id) {
			if (cd->cat == NULL) {
				return NFS4ERR_XDEV;
			}
			st = rename_2pc_initiate_local_shard(
				cd->cat,
				cd->saved_shard, cd->current_shard,
				cd->saved_fh.fileid, op->arg.rename.src_name,
				cd->current_fh.fileid, op->arg.rename.dst_name);
			return mds_status_to_nfs4(st);
		}
	}

	/*
	 * Cycle detection for directory renames (RFC 8881 §18.26.4):
	 * reject if the source path is a prefix of the destination
	 * path, which would create a disconnected namespace cycle.
	 */
	if (cd->saved_path[0] != '\0' && cd->current_path[0] != '\0') {
		size_t slen = strlen(cd->saved_path);

		if (strncmp(cd->saved_path, cd->current_path, slen) == 0 &&
		    cd->current_path[slen] == '/') {
			return NFS4ERR_INVAL;
}
	}

	/*
	 * RFC 8881 §18.26.4 — RENAME of a file to itself MUST be a
	 * no-op.  This covers two flavours both required by pynfs:
	 *
	 *   RNM19 (testSelfRenameFile): src_name == dst_name in the
	 *   same directory, naming the same object.
	 *
	 *   RNM20 (testLinkRename): src_name != dst_name (possibly in
	 *   different directories) but both names resolve to the same
	 *   inode — i.e. the destination is a hard link to the source.
	 *
	 * In both cases the source and target change_info4 values MUST
	 * have before == after; mutating the catalogue would bump the
	 * change counter on at least one directory and break the test
	 * (and cause real clients to invalidate caches unnecessarily).
	 */
	{
		bool same_dir = (cd->saved_fh.fileid == cd->current_fh.fileid);
		bool same_name = (strcmp(op->arg.rename.src_name,
					 op->arg.rename.dst_name) == 0);
		bool noop = false;

		if (same_dir && same_name) {
			noop = true;
		} else {
			struct mds_inode src_obj;
			struct mds_inode dst_obj;
			enum mds_status sst, dst_st;

			sst = compound_lookup_local_child(cd,
				cd->saved_fh.fileid,
				op->arg.rename.src_name, &src_obj);
			dst_st = compound_lookup_local_child(cd,
				cd->current_fh.fileid,
				op->arg.rename.dst_name, &dst_obj);
			if (sst == MDS_OK && dst_st == MDS_OK &&
			    src_obj.fileid == dst_obj.fileid) {
				noop = true;
			}
		}
		if (noop) {
			struct mds_inode src_dir;
			struct mds_inode dst_dir;

			if (compound_inode_get(cd, cd->saved_fh.fileid,
					       &src_dir) == MDS_OK) {
				res->res.change_info.src_before =
					src_dir.change;
				res->res.change_info.src_after =
					src_dir.change;
			}
			if (compound_inode_get(cd, cd->current_fh.fileid,
					       &dst_dir) == MDS_OK) {
				res->res.change_info.before =
					dst_dir.change;
				res->res.change_info.after =
					dst_dir.change;
			}
			return NFS4_OK;
		}
	}

	/*
	 * RFC 8881 §18.26.4: when the source is a directory and the
	 * target name already exists as a non-empty directory, the
	 * server MUST fail with NFS4ERR_EXIST or NFS4ERR_NOTEMPTY.
	 * Pynfs RNM16 testDirToFullDir.
	 *
	 * We also capture src_fileid (and dst_overwrite_fileid when
	 * the destination name already resolves to a different inode)
	 * so the post-rename branch below can invalidate the affected
	 * child inodes in the icache.  Without those invalidations a
	 * stale icache hit on the renamed inode keeps the pre-rename
	 * parent_fileid alive, and a stale hit on the overwritten
	 * inode pretends the unlinked-by-rename target still exists.
	 * Both are sources of "file appears to still be there after
	 * rename" symptoms under IOR-like concurrent workloads.
	 */
	uint64_t rnm_src_fileid = 0;
	uint64_t rnm_dst_overwrite_fileid = 0;
	{
		struct mds_inode rnm_src;
		struct mds_inode rnm_dst;
		enum mds_status rnm_ss, rnm_ds;

		rnm_ss = compound_lookup_local_child(cd,
			cd->saved_fh.fileid,
			op->arg.rename.src_name, &rnm_src);
		rnm_ds = compound_lookup_local_child(cd,
			cd->current_fh.fileid,
			op->arg.rename.dst_name, &rnm_dst);
		if (rnm_ss == MDS_OK) {
			rnm_src_fileid = rnm_src.fileid;
		}
		if (rnm_ds == MDS_OK &&
		    (rnm_ss != MDS_OK ||
		     rnm_dst.fileid != rnm_src_fileid)) {
			rnm_dst_overwrite_fileid = rnm_dst.fileid;
		}
		if (rnm_ss == MDS_OK && rnm_ds == MDS_OK &&
		    rnm_src.type == MDS_FTYPE_DIR &&
		    rnm_dst.type == MDS_FTYPE_DIR &&
		    cd->cat != NULL) {
			bool empty = true;
			if (mds_cat_dir_is_empty(cd->cat,
						 rnm_dst.fileid,
						 &empty) == MDS_OK &&
			    !empty) {
				return NFS4ERR_EXIST;
			}
		}
	}

	/* Capture pre-rename change counters for both directories. */
	{
		struct mds_inode src_pre, dst_pre;
		if (compound_inode_get(cd, cd->saved_fh.fileid,
				       &src_pre) == MDS_OK) {
			res->res.change_info.src_before = src_pre.change;
		}
		if (compound_inode_get(cd, cd->current_fh.fileid,
				       &dst_pre) == MDS_OK) {
			res->res.change_info.before = dst_pre.change;
		}
	}

	/*
	 * Phase 8d: emit NOTIFY4_RENAME_ENTRY to holders of the
	 * same-dir rename case; for cross-dir renames we still notify
	 * both sides because the entry moved between them.  We pass
	 * old_name and new_name via the single call for the source
	 * directory and reuse recall semantics for the destination.
	 */
	compound_notify_or_recall_dir(cd, cd->saved_fh.fileid,
				      NOTIFY4_RENAME_ENTRY,
				      op->arg.rename.src_name,
				      op->arg.rename.dst_name);
	if (cd->saved_fh.fileid != cd->current_fh.fileid) {
		compound_notify_or_recall_dir(cd,
					      cd->current_fh.fileid,
					      NOTIFY4_ADD_ENTRY,
					      op->arg.rename.dst_name,
					      NULL);
	}

	st = cat_rename(cd,
			cd->saved_fh.fileid, op->arg.rename.src_name,
			cd->current_fh.fileid, op->arg.rename.dst_name);
	if (st == MDS_OK) {
		/* Invalidate BEFORE post-mutation re-read. */
		compound_inode_invalidate(cd, cd->saved_fh.fileid);
		compound_inode_invalidate(cd, cd->current_fh.fileid);

		/*
		 * The renamed inode's parent_fileid changed in NDB but
		 * the icache entry still records the pre-rename parent.
		 * Drop it so the next read re-fetches.
		 *
		 * If the rename overwrote an existing destination, that
		 * inode either lost a link (nlink-- or removal) or was
		 * unlinked entirely.  Either way its icache entry is
		 * stale and would make the overwritten file appear to
		 * still exist via a PUTFH on the captured fileid.  Same
		 * class of bug as the op_remove icache leak addressed
		 * in the previous commit.
		 */
		if (rnm_src_fileid != 0) {
			compound_inode_invalidate(cd, rnm_src_fileid);
		}
		if (rnm_dst_overwrite_fileid != 0) {
			compound_inode_invalidate(cd,
				rnm_dst_overwrite_fileid);
		}

		/* Read post-rename change counters. */
		struct mds_inode src_post, dst_post;
		if (compound_inode_get(cd, cd->saved_fh.fileid,
				       &src_post) == MDS_OK) {
			res->res.change_info.src_after = src_post.change;
		}
		if (compound_inode_get(cd, cd->current_fh.fileid,
				       &dst_post) == MDS_OK) {
			res->res.change_info.after = dst_post.change;
		}

		/* Invalidate dirent caches for both names. */
		compound_dirent_invalidate(cd, cd->saved_fh.fileid,
					   op->arg.rename.src_name);
		compound_dirent_invalidate(cd, cd->current_fh.fileid,
					   op->arg.rename.dst_name);
	}
	return mds_status_to_nfs4(st);
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum nfs4_status op_link(struct compound_data *cd,
				const struct nfs4_op *op,
				struct nfs4_result *res)
{
	enum nfs4_status nst;
	enum mds_status st;

	(void)res;

	/* saved_fh = target file, current_fh = directory for the link. */
	nst = require_saved_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	/*
	 * RFC 8881 §18.20.4 / §12.7 — component4 validation on the
	 * link's new name.  Empty → NFS4ERR_INVAL; "." / ".." →
	 * NFS4ERR_BADNAME; bad UTF-8 → NFS4ERR_INVAL.  Same shape
	 * and precedence as the other namespace ops above.
	 */
	if (op->arg.link.name_too_long) {
		return NFS4ERR_NAMETOOLONG;
	}
	nst = compound_validate_name(op->arg.link.name);
	if (nst != NFS4_OK) {
		return nst;
	}
	nst = check_subtree_frozen(cd);
	if (nst != NFS4_OK) {
		return nst;
}
	nst = check_repl_health(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	/* POSIX DAC: LINK adds an entry to the link directory. */
	if (compound_dac_active(cd)) {
		struct mds_inode ln_dir;
		enum mds_status ln_st;

		ln_st = compound_inode_get(cd, cd->current_fh.fileid,
					   &ln_dir);
		if (ln_st != MDS_OK) {
			return mds_status_to_nfs4(ln_st);
		}
		nst = compound_dir_mutate_check(cd, &ln_dir);
		if (nst != NFS4_OK) {
			return nst;
		}
	}

	/* Cross-shard hard link (Phase 4, Tier 3).
	 * Target inode (saved_fh) is in shard A, link directory (current_fh)
	 * is in shard B.  Create ext_dirent in B, increment nlink in A. */
	if (cd->current_shard != NULL &&
	    cd->saved_shard != NULL &&
	    cd->current_shard->shard_id != cd->saved_shard->shard_id) {
		if (cd->saved_shard->cat == NULL ||
		    cd->current_shard->cat == NULL) {
			return NFS4ERR_XDEV;
		}
		/* Verify target is not a directory. */
		{
			struct mds_inode tgt;
			st = mds_cat_ns_getattr(cd->saved_shard->cat,
						cd->saved_fh.fileid, &tgt);
			if (st != MDS_OK) {
				return mds_status_to_nfs4(st);
			}
			if (tgt.type == MDS_FTYPE_DIR) {
				return NFS4ERR_ISDIR;
			}
		}
		/* Check dest name doesn't already exist. */
		{
			uint64_t ck_fid = 0;
			uint8_t  ck_type = 0;
			st = mds_cat_dirent_get(cd->current_shard->cat,
						cd->current_fh.fileid,
						op->arg.link.name,
						&ck_fid, &ck_type);
			if (st == MDS_OK) {
				return NFS4ERR_EXIST;
			}
			if (st != MDS_ERR_NOTFOUND) {
				return mds_status_to_nfs4(st);
			}
		}
		/* 1. Increment nlink + create link_anchor in shard A. */
		{
			struct mds_inode tgt_ino;
			st = mds_cat_ns_getattr(cd->saved_shard->cat,
						cd->saved_fh.fileid,
						&tgt_ino);
			if (st != MDS_OK) {
				return mds_status_to_nfs4(st);
			}
			tgt_ino.nlink++;
			clock_gettime(CLOCK_REALTIME, &tgt_ino.ctime);
			tgt_ino.change++;
			(void)mds_cat_inode_put(cd->saved_shard->cat,
						NULL, &tgt_ino);
			{
				uint64_t anchor_id = cd->saved_fh.fileid ^
					cd->current_fh.fileid ^
					(uint64_t)tgt_ino.nlink;
				(void)mds_cat_link_anchor_put(
					cd->saved_shard->cat, NULL,
					anchor_id, cd->mds_id,
					cd->current_fh.fileid,
					op->arg.link.name);
			}
		}
		/* 2. Create ext_dirent in shard B. */
		{
			struct mds_inode tgt_re;
			uint64_t ext_anchor_id = cd->saved_fh.fileid ^
				cd->current_fh.fileid;
			if (mds_cat_ns_getattr(cd->saved_shard->cat,
					       cd->saved_fh.fileid,
					       &tgt_re) == MDS_OK) {
				ext_anchor_id ^= (uint64_t)tgt_re.nlink;
			}
			st = mds_cat_ext_dirent_put(
				cd->current_shard->cat, NULL,
				cd->current_fh.fileid,
				op->arg.link.name,
				cd->mds_id, cd->saved_fh.fileid,
				MDS_FTYPE_REG, ext_anchor_id);
			if (st != MDS_OK) {
				(void)mds_cat_ns_nlink_adjust(
					cd->saved_shard->cat,
					cd->saved_fh.fileid, -1);
				return mds_status_to_nfs4(st);
			}
			/* Touch link dir parent metadata. */
			{
				struct mds_inode lp;
				if (mds_cat_ns_getattr(
					cd->current_shard->cat,
					cd->current_fh.fileid,
					&lp) == MDS_OK) {
					clock_gettime(CLOCK_REALTIME,
						      &lp.mtime);
					lp.ctime = lp.mtime;
					lp.change++;
					(void)mds_cat_inode_put(
						cd->current_shard->cat,
						NULL, &lp);
				}
			}
		}
		return NFS4_OK;
	}

	/* Cross-subtree hard link.
	 *
	 * Infrastructure is in place (Phase 1-5):
	 *   - Versioned filehandles (v0/v1)
	 *   - DBI_EXTERNAL_DIRENTS + DBI_LINK_ANCHORS
	 *   - hardlink_2pc journal + coordinator logic
	 *   - Owner-routing for remote FH inode ops
	 *
	 * Remaining work before enabling:
	 *   - End-to-end v1 FH propagation (PUTFH/GETFH/SAVEFH)
	 *   - External dirents in READDIR + REMOVE
	 *   - Real 2PC transport (prepare/commit/abort wire messages)
	 *   - Server-side CT_MSG_NLINK dispatch
	 *   - Migration enforcement for external dirents
	 *   - Cross-MDS hard link tests
	 *
	 * Cross-subtree LINK returns NFS4ERR_XDEV until the feature
	 * is semantically complete.  See doc comment above for status. */
	if (cd->smap != NULL &&
	    cd->saved_path[0] != '\0' && cd->current_path[0] != '\0') {
		bool target_local = subtree_map_is_local(cd->smap,
							 cd->saved_path);
		bool dir_local = subtree_map_is_local(cd->smap,
						      cd->current_path);
		/*
		 * In the single-RonDB-cluster deployment model every MDS
		 * sees the same catalogue, so an unmapped path (e.g. the
		 * pynfs default test root /pynfs-test/...) is reachable
		 * here even though it falls outside any configured subtree
		 * — mirror the op_rename precedent that allows the
		 * `!src_local && !dst_local` case to fall through to
		 * cat_link.  The historical NFS4ERR_XDEV here was
		 * over-conservative and broke pynfs RNM20 (testLinkRename)
		 * which LINKs within the same dir before exercising rename.
		 */
		if ((!target_local && dir_local) ||
		    (target_local && !dir_local)) {
			return NFS4ERR_XDEV;
		}
	}

	/* R1.3: capture parent change before mutation for change_info. */
	uint64_t link_change_before = 0;
	{
		struct mds_inode link_parent;
		if (compound_inode_get(cd, cd->current_fh.fileid,
				       &link_parent) == MDS_OK) {
			link_change_before = link_parent.change;
		}
	}

	/* Phase 8d: link adds a new entry to the link-parent dir. */
	compound_notify_or_recall_dir(cd, cd->current_fh.fileid,
				      NOTIFY4_ADD_ENTRY,
				      op->arg.link.name, NULL);

	st = cat_link(cd, cd->current_fh.fileid,
		      op->arg.link.name, cd->saved_fh.fileid);
	if (st == MDS_OK) {
		struct mds_inode link_parent_post;
		compound_inode_invalidate(cd, cd->current_fh.fileid);
		compound_inode_invalidate(cd, cd->saved_fh.fileid);
		/*
		 * Invalidate the dirent cache for the new link's name in
		 * the link parent.  Without this, a prior negative entry
		 * ("this name does not exist in this dir") survives the
		 * cat_link and shadows the newly-created link for the
		 * negative-cache TTL window -- same class of bug as the
		 * op_create dirent_invalidate that lives at the end of
		 * the create path.  Mirror the existing op_create /
		 * op_remove / op_rename invalidate calls.
		 */
		compound_dirent_invalidate(cd, cd->current_fh.fileid,
					   op->arg.link.name);
		if (cat_getattr(cd, cd->current_fh.fileid,
				   &link_parent_post) == MDS_OK) {
			res->res.change_info.after = link_parent_post.change;
			res->res.change_info.before = link_change_before;
		}
	}
	return mds_status_to_nfs4(st);
}

/* -----------------------------------------------------------------------
 * READDIR byte-budget pagination constants.
 *
 * The page stops at the first of: the entry ceiling (NFS4_READDIR_MAX),
 * the total reply-byte budget, or the dircount (name-only) budget.  A
 * deliberately conservative per-entry size estimate keeps the encoded
 * page within the fixed XDR reply buffer (NFS4_REPLY_BUF_SIZE) so a
 * large page can never overflow it and produce a short/dropped reply.
 * ----------------------------------------------------------------------- */

/* Per-entry4 XDR framing: value_follows(4) + cookie(8) + name len(4),
 * excluding the variable name bytes and the trailing fattr4. */
#define READDIR_ENTRY_FRAMING   16
/* Upper bound on the encoded fattr4 for one entry when attributes are
 * requested.  The worst case across the supported attribute set is
 * ~650 bytes (incl. numeric owner/owner_group strings); round up
 * generously so the byte accumulator can never under-count. */
#define READDIR_ATTR_EST_FULL  768
/* Minimal attrs (TYPE + FILEID) emitted even when none are requested. */
#define READDIR_ATTR_EST_MIN    64
/* Bytes reserved within the reply buffer for the rest of the COMPOUND
 * (RPC + COMPOUND headers, SEQUENCE/PUTFH results, the READDIR
 * cookieverf and trailer).  Far larger than the ~200 bytes needed. */
#define READDIR_REPLY_HEADROOM 8192

/* True if the client requested any per-entry attributes. */
static bool readdir_attrs_requested(const uint32_t req[NFS4_BITMAP_WORDS])
{
	return (req[0] | req[1] | req[2]) != 0;
}

/* Readdir callback context. */
struct readdir_fill {
	struct compound_data    *cd;
	struct nfs4_res_readdir *rd;
	/* Byte-budget pagination state (see constants above). */
	size_t total_budget;   /* max total reply bytes for entries */
	size_t total_used;     /* running estimate of total bytes */
	size_t dir_budget;     /* client dircount (0 = unlimited) */
	size_t dir_used;       /* running name-only bytes */
	size_t attr_est;       /* per-entry attr-bytes upper bound */
	bool   truncated;      /* page stopped before the dir drained */
};

/**
 * Fused readdir_plus callback: fills the dirent row AND the child
 * inode attrs in one pass.  Eliminates the separate
 * populate_readdir_entry_attrs loop (which used to issue one inode
 * read per entry) on the RonDB backend where the catalogue layer
 * resolves both in a single NDB transaction.
 *
 * Entries for which the inode read race-missed (dangling dirent, i.e.
 * inode_valid==false) are still emitted — the kernel client wants the
 * name for plain readdir — but their entry_attrs_valid flag is left
 * false so the GETATTR path falls back to the caller-issued read.
 */
static int readdir_plus_cat_cb(const struct mds_cat_dirent *entry,
			       const struct mds_inode *inode,
			       bool inode_valid,
			       void *arg)
{
	struct readdir_fill *f = arg;
	struct mds_cat_dirent *dst;

	if (entry == NULL) { return 0; }

	/*
	 * RFC 5661 §3.2: a component4 name is a utf8str_cs and the
	 * specification implicitly requires non-zero length for any
	 * directory entry (zero-length names are explicitly rejected
	 * by LOOKUP / RENAME / CREATE / REMOVE per §16.4.5 /
	 * §18.26.4).  Pre-existing zero-length dirents in the
	 * catalogue — e.g. created by an older codepath that did not
	 * validate the name, or by a backend repair that left a
	 * sentinel row — must therefore be hidden from the wire so
	 * conformant clients (pynfs's clean_dir, Linux's getdents)
	 * don't choke trying to LOOKUP("").  This is a defensive
	 * filter only; CREATE/OPEN at the handler level should be
	 * extended to reject empty names so no new such entries are
	 * ever written.
	 */
	if (entry->name[0] == '\0') { return 0; }

	/*
	 * Cosmetic referral-junction hiding (opt-in, default off).
	 *
	 * When cfg_hide_referral_junctions is set, omit the /shardN
	 * referral junction directories from READDIR at the namespace
	 * ROOT only.  These are the per-MDS partition entries the
	 * kernel crosses via an fs_locations referral; some operators
	 * prefer them hidden from a plain `ls /`.
	 *
	 * Safety:
	 *   - Root only (parent fileid == MDS_FILEID_ROOT).
	 *   - Directories only.
	 *   - EXACT subtree-map match (no longest-prefix fallback), so
	 *     an ordinary file or directory whose name is not a
	 *     registered partition is never hidden.
	 *   - LOOKUP is unaffected, so `cd /mnt/pnfs/shardN` still
	 *     works; this only drops the entry from the listing.
	 */
	if (f->cd != NULL && f->cd->cfg_hide_referral_junctions &&
	    f->cd->smap != NULL &&
	    f->cd->current_fh.fileid == MDS_FILEID_ROOT &&
	    entry->type == MDS_FTYPE_DIR) {
		char jpath[MDS_MAX_PATH];
		int jn = snprintf(jpath, sizeof(jpath), "/%s", entry->name);

		if (jn > 0 && (size_t)jn < sizeof(jpath)) {
			struct subtree_entry se;

			if (subtree_map_lookup_exact(f->cd->smap, jpath,
						     &se) == MDS_OK) {
				return 0; /* hidden referral junction */
			}
		}
	}

	/* Skip inodes still pending an HPC wide-create (not yet visible).
	 * Do this before the page-boundary checks so a pending entry does
	 * not consume budget or wrongly mark the page truncated. */
	if (inode_valid && inode != NULL &&
	    (inode->flags & MDS_IFLAG_HPC_CREATE_PENDING) != 0) {
		return 0;
	}

	/* Page boundary 1: hard entry ceiling. */
	if (f->rd->count >= NFS4_READDIR_MAX) {
		f->truncated = true;
		return -1;
	}

	/* Page boundary 2: byte budget.  Use a conservative per-entry
	 * size estimate (framing + 4-byte-aligned name + attr upper
	 * bound) so the encoded page stays within the fixed reply
	 * buffer.  Always emit at least one entry (count == 0). */
	{
		size_t name_len = strnlen(entry->name, MDS_MAX_NAME);
		size_t name_pad = (name_len + 3u) & ~(size_t)3u;
		size_t entry_est = READDIR_ENTRY_FRAMING + name_pad +
				   f->attr_est;
		size_t dir_est = READDIR_ENTRY_FRAMING + name_pad;

		if (f->rd->count >= 1) {
			if (f->total_used + entry_est > f->total_budget) {
				f->truncated = true;
				return -1;
			}
			if (f->dir_budget > 0 &&
			    f->dir_used + dir_est > f->dir_budget) {
				f->truncated = true;
				return -1;
			}
		}
		f->total_used += entry_est;
		f->dir_used += dir_est;
	}

	dst = &f->rd->entries[f->rd->count];
	dst->fileid = entry->fileid;
	dst->type = entry->type;
	memcpy(dst->name, entry->name, sizeof(dst->name));

	if (inode_valid && inode != NULL) {
		f->rd->entry_attrs[f->rd->count] = *inode;
		/* Catalogue-returned type may be stale; override with
		 * the dirent type, matching the pre-fused behaviour of
		 * populate_readdir_entry_attrs. */
		f->rd->entry_attrs[f->rd->count].type =
			(enum mds_file_type)entry->type;
		f->rd->entry_attrs_valid[f->rd->count] = true;

		/* Prime the global inode cache so follow-up GETATTRs on
		 * these fileids hit memory. */
		if (f->cd != NULL && f->cd->icache != NULL) {
			(void)inode_cache_put(f->cd->icache,
					      &f->rd->entry_attrs[f->rd->count]);
		}
	} else {
		f->rd->entry_attrs_valid[f->rd->count] = false;
	}

	f->rd->count++;
	return 0;
}

/* Xattr READDIR callback — fills readdir entries with xattr names. */
struct xattr_readdir_ctx {
	struct nfs4_res_readdir *rd;
};

static int xattr_readdir_cb(const char *name, size_t name_len, void *arg)
{
	struct xattr_readdir_ctx *ctx = arg;
	struct mds_cat_dirent *e;

	if (ctx->rd->count >= NFS4_READDIR_MAX) {
		return -1;
}

	/* Same RFC 5661 §3.2 component4 rule applies to xattr names
	 * (RFC 8276 §4 — xattr names are component4): zero-length
	 * entries cannot exist on the wire, so skip any such row that
	 * may have leaked into the catalogue. */
	if (name == NULL || name_len == 0) {
		return 0;
	}

	e = &ctx->rd->entries[ctx->rd->count];
	memset(e, 0, sizeof(*e));
	e->type = MDS_FTYPE_REG;
	if (name_len > MDS_MAX_NAME) {
		name_len = MDS_MAX_NAME;
}
	memcpy(e->name, name, name_len);
	e->name[name_len] = '\0';
	ctx->rd->count++;
	return 0;
}

enum nfs4_status op_readdir(struct compound_data *cd,
				   const struct nfs4_op *op,
				   struct nfs4_result *res)
{
	enum nfs4_status nst;
	enum mds_status st;

	nst = require_current_fh(cd);
	if (nst != NFS4_OK) {
		return nst;
}

	memset(&res->res.readdir, 0, sizeof(res->res.readdir));
	memcpy(res->res.readdir.requested, op->arg.readdir.requested,
	       sizeof(res->res.readdir.requested));
	res->res.readdir.fsid_major = referral_fsid_major(cd->mds_id);
	res->res.readdir.fsid_minor = referral_fsid_minor(cd->mds_id);

	/* Xattr namespace: list xattr names as directory entries. */
	if (is_xattr_fh(cd->current_fh.fileid)) {
		uint64_t base = xattr_base_fileid(cd->current_fh.fileid);
		struct xattr_readdir_ctx xctx;

		xctx.rd = &res->res.readdir;
		st = cat_xattr_list(cd, base,
					  xattr_readdir_cb, &xctx);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
}

		res->res.readdir.eof =
			(res->res.readdir.count < NFS4_READDIR_MAX);
		return NFS4_OK;
	}

	{
		struct readdir_fill fill;
		size_t budget;

		memset(&fill, 0, sizeof(fill));
		fill.cd = cd;
		fill.rd = &res->res.readdir;

		/* Byte budget = min(reply-buffer headroom, client maxcount,
		 * session ca_maxresponsesize).  The reply-buffer clamp is the
		 * load-bearing one: the whole COMPOUND is XDR-encoded into a
		 * fixed NFS4_REPLY_BUF_SIZE buffer, so a page that does not fit
		 * would be dropped on the wire as a short reply. */
		budget = NFS4_REPLY_BUF_SIZE - READDIR_REPLY_HEADROOM;
		if (op->arg.readdir.maxcount > 0 &&
		    (size_t)op->arg.readdir.maxcount < budget) {
			budget = op->arg.readdir.maxcount;
		}
		if (cd->max_response_size > 0 &&
		    (size_t)cd->max_response_size < budget) {
			budget = cd->max_response_size;
		}
		fill.total_budget = budget;
		fill.dir_budget = op->arg.readdir.dircount; /* 0 = unlimited */
		fill.attr_est =
			readdir_attrs_requested(res->res.readdir.requested)
				? READDIR_ATTR_EST_FULL
				: READDIR_ATTR_EST_MIN;

		/* O(1)-per-page resume: the READDIR cookie IS the last child
		 * fileid seen (0 = first page).  Entries return in ascending
		 * fileid order; the client re-sorts for display (RFC 8881
		 * §3.2 — cookies are server-opaque).  A deleted cookie is
		 * safe because resume is a strict child_fileid > cookie range.
		 * Pass ceiling + 1 so the fill callback -- not the backend --
		 * is the page boundary, which keeps eof exact. */
		st = cat_readdir_plus_from_cookie(cd, cd->current_fh.fileid,
						  op->arg.readdir.cookie,
						  NFS4_READDIR_MAX + 1u,
						  readdir_plus_cat_cb,
						  &fill);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
}

		res->res.readdir.cookie_base = 0; /* Cookies are fileids now */
		/* eof is true only when the scan actually drained -- not when
		 * we stopped on the entry ceiling or the byte budget. */
		res->res.readdir.eof = !fill.truncated;

		/* R1.1: cookieverf from dir inode change attribute. */
		{
			struct mds_inode dir_ino;
			if (compound_inode_get(cd, cd->current_fh.fileid,
					       &dir_ino) == MDS_OK) {
				res->res.readdir.dir_change = dir_ino.change;
			}
		}
	}
	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * OPEN / CLOSE handlers
 * ----------------------------------------------------------------------- */

/**
 * op_open — NFSv4.1 OPEN operation (RFC 8881 §18.16).
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


/* -----------------------------------------------------------------------
 * LOOKUPP handler (RFC 8881 §18.14)
 *
 * Sets current_fh to the parent directory of the current filehandle.
 * ----------------------------------------------------------------------- */

enum nfs4_status op_lookupp(struct compound_data *cd,
			    const struct nfs4_op *op,
			    struct nfs4_result *res)
{
	struct mds_inode inode;
	enum mds_status st;

	(void)op;
	(void)res;

	if (!cd->current_fh_set) {
		return NFS4ERR_NOFILEHANDLE;
	}

	/* Fetch current inode to read parent_fileid. */
	st = compound_inode_get(cd, cd->current_fh.fileid, &inode);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
	}

	/*
	 * RFC 8881 §18.14.4: current FH must be a directory.
	 * Symlinks get NFS4ERR_SYMLINK; other non-dir types get
	 * NFS4ERR_NOTDIR.  Pynfs LKPP1r/s/f/a/d.
	 */
	if (inode.type != MDS_FTYPE_DIR) {
		if (inode.type == MDS_FTYPE_SYMLINK) {
			return NFS4ERR_SYMLINK;
		}
		return NFS4ERR_NOTDIR;
	}

	/*
	 * RFC 8881 §18.14: if the current filehandle is the root
	 * of the server's namespace, return NFS4ERR_NOENT.
	 */
	if (inode.parent_fileid == 0 ||
	    inode.parent_fileid == inode.fileid) {
		return NFS4ERR_NOENT;
	}

	cd->current_fh.fileid = inode.parent_fileid;
	/* FH-encoded subtree ownership: resolve the parent's subtree by
	 * ancestry (a LOOKUPP from just inside a junction lands ON the
	 * junction root, which is foreign); generation is filled in
	 * below once we have re-read the parent inode. */
	cd->current_fh.owner_mds_id = cd->mds_id;
	cd->current_fh.generation = 0;
	/* Update current_path when known. */
	if (cd->current_path[0] != '\0') {
		char *slash;

		slash = strrchr(cd->current_path, '/');
		if (slash == cd->current_path) {
			cd->current_path[1] = '\0';
		} else if (slash != NULL) {
			*slash = '\0';
		}
	}
	/* current_fh_set remains true. */
	if (cd->shard_map != NULL && cd->cat != NULL) {
		uint32_t parent_sid = 0;
		if (mds_cat_shard_fileid_get(cd->cat,
					     cd->current_fh.fileid,
					     &parent_sid) == MDS_OK) {
			const struct mds_shard *ps =
				mds_shard_map_lookup_by_id(
					cd->shard_map, parent_sid);
			if (ps != NULL) {
				apply_shard(cd, ps);
			} else {
				resolve_and_apply_shard(cd, cd->current_path);
			}
		} else {
			resolve_and_apply_shard(cd, cd->current_path);
		}
	} else {
		resolve_and_apply_shard(cd, cd->current_path);
	}

	/* Verify the parent inode after any shard handoff
	 * and seed the snapshot for subsequent ops. */
	{
		struct mds_inode parent;
		st = compound_inode_get(cd, inode.parent_fileid,
					&parent);
		if (st != MDS_OK) {
			return mds_status_to_nfs4(st);
		}
		cd->current_inode = parent;
		cd->current_inode_valid = true;
		/* Now that we have read the parent, stamp its generation
		 * onto the current FH so subsequent GETFH emits the
		 * correct v1 form. */
		cd->current_fh.generation = parent.generation;
	}

	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * READLINK handler (RFC 8881 §18.24)
 *
 * Returns the target of a symbolic link.  Currently not supported —
 * the MDS does not store symlinks.
 * ----------------------------------------------------------------------- */

enum nfs4_status op_readlink(struct compound_data *cd,
			     const struct nfs4_op *op,
			     struct nfs4_result *res)
{
	struct mds_inode inode;
	struct nfs4_res_readlink *r = &res->res.readlink;
	enum mds_status st;
	uint32_t out_len = 0;

	(void)op;

	if (!cd->current_fh_set) {
		return NFS4ERR_NOFILEHANDLE;
	}

	st = compound_inode_get(cd, cd->current_fh.fileid, &inode);
	if (st != MDS_OK) {
		return mds_status_to_nfs4(st);
	}

	/* RFC 8881 §18.24: must be a symbolic link. */
	if (inode.type != MDS_FTYPE_SYMLINK) {
		return NFS4ERR_INVAL;
	}

	/*
	 * Symlink targets are stored in the inline_data table,
	 * keyed by fileid (same table used for small-file inline
	 * data — the file type distinguishes the purpose).
	 */
	st = cat_inline_get(cd, cd->current_fh.fileid,
				  r->target, sizeof(r->target) - 1,
				  &out_len);
	if (st != MDS_OK) {
		return NFS4ERR_IO;
	}

	r->target[out_len] = '\0';
	r->target_len = out_len;
	return NFS4_OK;
}

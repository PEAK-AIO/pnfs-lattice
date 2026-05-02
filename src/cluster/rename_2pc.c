/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * rename_2pc.c — Cross-subtree rename via two-phase commit.
 *
 * See docs/architecture.md §10 for the full protocol description.
 *
 * The transport layer (gRPC) is abstracted via function pointers in
 * struct rename_2pc_transport, allowing in-process loopback for tests.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <time.h>
#include <endian.h>
#include <errno.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "rename_2pc.h"
#include "cluster_transport.h"

/* -----------------------------------------------------------------------
 * 2PC states and roles
 * ----------------------------------------------------------------------- */

enum rename_2pc_state {
	R2PC_IDLE      = 0,
	R2PC_PREPARED  = 1,
	R2PC_COMMITTED = 2,
	R2PC_ABORTED   = 3,
};

enum rename_2pc_role {
	R2PC_COORDINATOR = 0,
	R2PC_PARTICIPANT = 1,
};

/* -----------------------------------------------------------------------
 * Internal rename_2pc_ctx — used for coordination journal record conversion
 * ----------------------------------------------------------------------- */

struct rename_2pc_ctx {
	uint64_t            txn_id;
	enum rename_2pc_state state;
	enum rename_2pc_role  role;
	uint64_t            src_parent;
	char                src_name[256];
	uint64_t            dst_parent;
	char                dst_name[256];
	uint32_t            remote_mds_id;
	uint64_t            src_child_fileid;
	uint32_t            inode_data_len;
	uint8_t             inode_data[sizeof(struct mds_inode) + 12 +
	                               sizeof(struct mds_ds_map_entry) * 16];
};

/* struct rename_2pc_transport is defined in cluster_transport.h. */

/* -----------------------------------------------------------------------
 * Coordination journal dispatch helpers
 * ----------------------------------------------------------------------- */

static bool rename_2pc_cat_is_rondb(const struct mds_catalogue *cat)
{
	return mds_catalogue_backend_type(cat) == MDS_BACKEND_RONDB;
}

static uint64_t rename_2pc_payload_fileid(const struct rename_2pc_ctx *ctx)
{
	struct mds_inode inode;

	if (ctx == NULL) {
		return 0;
	}
	if (ctx->src_child_fileid != 0) {
		return ctx->src_child_fileid;
	}
	if (ctx->inode_data_len < sizeof(inode)) {
		return 0;
	}

	memcpy(&inode, ctx->inode_data, sizeof(inode));
	return inode.fileid;
}

static uint64_t rename_2pc_now_ns(void)
{
	struct timespec now;

	if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
		return 0;
	}
	return ((uint64_t)now.tv_sec * 1000000000ULL) +
	       (uint64_t)now.tv_nsec;
}

static void rename_2pc_ctx_to_record(
	const struct rename_2pc_ctx *ctx,
	struct mds_coord_journal_record *record)
{
	memset(record, 0, sizeof(*record));
	record->txn_id = ctx->txn_id;
	record->state = (uint8_t)ctx->state;
	record->role = (uint8_t)ctx->role;
	record->remote_mds_id = ctx->remote_mds_id;
	record->src_parent_fileid = ctx->src_parent;
	record->dst_parent_fileid = ctx->dst_parent;
	record->src_child_fileid = rename_2pc_payload_fileid(ctx);
	(void)snprintf(record->src_name, sizeof(record->src_name),
		       "%s", ctx->src_name);
	(void)snprintf(record->dst_name, sizeof(record->dst_name),
		       "%s", ctx->dst_name);
	record->payload_len = ctx->inode_data_len;
	if (ctx->inode_data_len > 0) {
		memcpy(record->payload, ctx->inode_data, ctx->inode_data_len);
	}
	record->created_at_ns = rename_2pc_now_ns();
}

static void rename_2pc_record_to_ctx(
	const struct mds_coord_journal_record *record,
	struct rename_2pc_ctx *ctx)
{
	memset(ctx, 0, sizeof(*ctx));
	ctx->txn_id = record->txn_id;
	ctx->state = (enum rename_2pc_state)record->state;
	ctx->role = (enum rename_2pc_role)record->role;
	ctx->remote_mds_id = record->remote_mds_id;
	ctx->src_parent = record->src_parent_fileid;
	ctx->dst_parent = record->dst_parent_fileid;
	ctx->src_child_fileid = record->src_child_fileid;
	(void)snprintf(ctx->src_name, sizeof(ctx->src_name),
		       "%s", record->src_name);
	(void)snprintf(ctx->dst_name, sizeof(ctx->dst_name),
		       "%s", record->dst_name);
	ctx->inode_data_len = record->payload_len;
	if (record->payload_len > 0) {
		memcpy(ctx->inode_data, record->payload, record->payload_len);
	}
}

static enum mds_status rename_2pc_journal_put(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	const struct rename_2pc_ctx *ctx)
{
	struct mds_coord_journal_record record;

	rename_2pc_ctx_to_record(ctx, &record);
	return mds_coord_journal_put(cat, txn, &record);
}

static enum mds_status rename_2pc_journal_get(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t txn_id, uint8_t role,
	struct rename_2pc_ctx *ctx)
{
	struct mds_coord_journal_record record;
	enum mds_status st;

	st = mds_coord_journal_get(cat, txn, txn_id, role, &record);
	if (st != MDS_OK) {
		return st;
	}
	rename_2pc_record_to_ctx(&record, ctx);
	return MDS_OK;
}

static enum mds_status rename_2pc_journal_del(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t txn_id, uint8_t role)
{
	return mds_coord_journal_del(cat, txn, txn_id, role);
}

static enum mds_status rename_2pc_rondb_cleanup_source(
	struct mds_catalogue *cat, uint64_t src_parent,
	const char *src_name)
{
	struct mds_inode parent;
	enum mds_status st;

	if (cat == NULL || src_name == NULL) {
		return MDS_ERR_INVAL;
	}

	st = mds_cat_dirent_del(cat, NULL, src_parent, src_name);
	if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
		return st;
	}

	st = mds_cat_ns_getattr(cat, src_parent, &parent);
	if (st != MDS_OK) {
		return st;
	}
	clock_gettime(CLOCK_REALTIME, &parent.mtime);
	parent.ctime = parent.mtime;
	parent.change++;
	return mds_cat_inode_put(cat, NULL, &parent);
}

/* -----------------------------------------------------------------------
 * Coordinator side
 * ----------------------------------------------------------------------- */

/**
 * @brief Initiate a cross-subtree rename (coordinator).
 *
 * Steps:
 *   1. Write PREPARED journal entry.
 *   2. Send PrepareRename to remote via transport.
 *   3. On VOTE_COMMIT: remove src dirent, write COMMITTED, send Commit.
 *   4. On VOTE_ABORT: write ABORTED, send Abort.
 *   5. Delete journal entry.
 *
 * @param cat             Catalogue handle.
 * @param transport       Transport callbacks.
 * @param src_parent      Source parent fileid (local).
 * @param src_name        Source entry name.
 * @param dst_parent      Destination parent fileid (remote).
 * @param dst_name        Destination entry name.
 * @param remote_mds_id   Remote MDS node ID.
 * @return MDS_OK on success (rename committed on both sides).
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status rename_2pc_initiate(
	struct mds_catalogue *cat,
	const struct rename_2pc_transport *transport,
	uint64_t src_parent, const char *src_name,
	uint64_t dst_parent, const char *dst_name,
	uint32_t remote_mds_id)
{
	if (cat == NULL || transport == NULL ||
	    src_name == NULL || dst_name == NULL) {
	    return MDS_ERR_INVAL;
	}

	enum mds_status st;
	struct mds_cat_txn *ct = NULL;

	/* Verify source entry exists (standalone read). */
	uint64_t src_child;
	uint8_t  src_type;
	st = mds_cat_dirent_get(cat, src_parent, src_name,
	                        &src_child, &src_type);
	if (st != MDS_OK) {
	    return st;
	}

	/* Directories cannot cross subtree boundaries. */
	if (src_type == (uint8_t)MDS_FTYPE_DIR) {
	    return MDS_ERR_XDEV;
	}

	/* Read source inode (will be sent to participant). */
	struct mds_inode src_inode;
	st = mds_cat_ns_getattr(cat, src_child, &src_inode);
	if (st != MDS_OK) {
	    return st;
	}

	/* Allocate txn_id + write PREPARED journal in one txn. */
	st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &ct);
	if (st != MDS_OK) {
	    return st;
	}

	uint64_t txn_id;
	st = mds_cat_alloc_fileid(cat, ct, &txn_id);
	if (st != MDS_OK) {
	    mds_cat_txn_abort(ct);
	    return st;
	}

	/* Phase 1: Write PREPARED journal entry.
	 * Persist the source inode snapshot so it survives into the
	 * COMMITTED entry — needed by on_commit() when coordinator and
	 * participant share the same DB (loopback), and by recovery
	 * when the commit must be retried on a new connection. */
	struct rename_2pc_ctx ctx;
	memset(&ctx, 0, sizeof(ctx));
	ctx.txn_id = txn_id;
	ctx.state = R2PC_PREPARED;
	ctx.role = R2PC_COORDINATOR;
	ctx.src_parent = src_parent;
	ctx.dst_parent = dst_parent;
	ctx.remote_mds_id = remote_mds_id;
	ctx.src_child_fileid = src_child;
	(void)snprintf(ctx.src_name, sizeof(ctx.src_name), "%s", src_name);
	(void)snprintf(ctx.dst_name, sizeof(ctx.dst_name), "%s", dst_name);
	_Static_assert(sizeof(src_inode) <= sizeof(ctx.inode_data),
	               "mds_inode must fit in ctx.inode_data");
	memcpy(ctx.inode_data, &src_inode, sizeof(src_inode));
	ctx.inode_data_len = (uint32_t)sizeof(src_inode);
	st = rename_2pc_journal_put(cat, ct, &ctx);
	if (st != MDS_OK) {
	    mds_cat_txn_abort(ct);
	    return st;
	}

	if (mds_cat_txn_commit(ct) != MDS_OK) {
	    return MDS_ERR_IO;
	}
	ct = NULL;

	/* Build combined payload: inode + stripe map.
	 * Both MDS nodes share the same DS pool, so stripe entries
	 * (ds_id + NFS file handle) are valid on either MDS. */
	uint8_t payload_buf[sizeof(src_inode) + 12 + sizeof(struct mds_ds_map_entry) * 16];
	size_t payload_len = sizeof(src_inode);
	memcpy(payload_buf, &src_inode, sizeof(src_inode));

	{
	    struct mds_ds_map_entry *sm_entries = NULL;
	    uint32_t sm_sc = 0, sm_su = 0, sm_mc = 0;
	    if (mds_cat_stripe_map_get(cat, src_child,
	                                &sm_sc, &sm_su, &sm_mc,
	                                &sm_entries) == MDS_OK && sm_entries) {
	        uint32_t total = sm_sc * sm_mc;
	        size_t sm_size = 12 + total * sizeof(struct mds_ds_map_entry);
	        if (payload_len + sm_size <= sizeof(payload_buf)) {
	            uint8_t *p = payload_buf + payload_len;
	            memcpy(p, &sm_sc, 4); p += 4;
	            memcpy(p, &sm_su, 4); p += 4;
	            memcpy(p, &sm_mc, 4); p += 4;
	            memcpy(p, sm_entries, total * sizeof(*sm_entries));
	            payload_len += sm_size;
	        }
	        free(sm_entries);
	    }
	}

	/* Send PrepareRename to remote. */
	int vote = transport->prepare(
	    remote_mds_id, txn_id, dst_parent, dst_name,
	    payload_buf, payload_len, transport->user_ctx);

	if (vote == 1) {
	    ctx.state = R2PC_COMMITTED;

	    if (rename_2pc_cat_is_rondb(cat)) {
	        /*
	         * RonDB shared-authority mode: keep the source visible until the
	         * participant durably commits the destination dirent.  The inode
	         * itself stays global/shared, so coordinator cleanup removes only
	         * the source dirent after commit delivery succeeds.
	         */
	        st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &ct);
	        if (st != MDS_OK) {
	            return st;
	        }
	        st = rename_2pc_journal_put(cat, ct, &ctx);
	        if (st != MDS_OK) {
	            mds_cat_txn_abort(ct);
	            return st;
	        }
	        if (mds_cat_txn_commit(ct) != MDS_OK) {
	            return MDS_ERR_IO;
	        }
	        ct = NULL;

	        if (transport->commit(remote_mds_id, txn_id,
	                              transport->user_ctx) != 0) {
	            return MDS_ERR_IO;
	        }

	        st = rename_2pc_rondb_cleanup_source(cat, src_parent, src_name);
	        if (st != MDS_OK) {
	            return st;
	        }

	        st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &ct);
	        if (st != MDS_OK) {
	            return st;
	        }
	        st = rename_2pc_journal_del(cat, ct, txn_id,
	                                    R2PC_COORDINATOR);
	        if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
	            mds_cat_txn_abort(ct);
	            return st;
	        }
	        if (mds_cat_txn_commit(ct) != MDS_OK) {
	            return MDS_ERR_IO;
	        }
	        return MDS_OK;
	    }

	    /* Legacy move semantics: remove local source before commit RPC. */
	    st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &ct);
	    if (st != MDS_OK) {
	        return st;
	    }

	    /* Remove src dirent. */
	    st = mds_cat_dirent_del(cat, ct, src_parent, src_name);
	    if (st != MDS_OK) {
	        mds_cat_txn_abort(ct);
	        return st;
	    }

	    /* Remove src inode (it moves to remote MDS). */
	    st = mds_cat_inode_del(cat, ct, src_child);
	    if (st != MDS_OK) {
	        mds_cat_txn_abort(ct);
	        return st;
	    }

	    /* Update parent mtime/ctime/change. */
	    struct mds_inode parent;
	    st = mds_cat_ns_getattr(cat, src_parent, &parent);
	    if (st == MDS_OK) {
	        clock_gettime(CLOCK_REALTIME, &parent.mtime);
	        parent.ctime = parent.mtime;
	        parent.change++;
	        (void)mds_cat_inode_put(cat, ct, &parent);
	    }

	    st = rename_2pc_journal_put(cat, ct, &ctx);
	    if (st != MDS_OK) {
	        mds_cat_txn_abort(ct);
	        return st;
	    }

	    if (mds_cat_txn_commit(ct) != MDS_OK) {
	        return MDS_ERR_IO;
	    }
	    ct = NULL;

	    if (transport->commit(remote_mds_id, txn_id,
	                          transport->user_ctx) == 0) {
	        st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &ct);
	        if (st == MDS_OK) {
	            st = rename_2pc_journal_del(cat, ct, txn_id,
	                                        R2PC_COORDINATOR);
	            if (st == MDS_OK || st == MDS_ERR_NOTFOUND) {
	                (void)mds_cat_txn_commit(ct);
	            } else {
	                mds_cat_txn_abort(ct);
	            }
	        }
	        return MDS_OK;
	    }

	    /*
	     * Commit delivery FAILED.  The source is already deleted
	     * locally and the journal is in COMMITTED state.  We MUST
	     * keep the coordinator row so recovery can retry commit.
	     */
	    return MDS_ERR_IO;
	/* NOLINTNEXTLINE(readability-else-after-return) */
	} else {
	    /* VOTE_ABORT or transport error: record abort and unwind. */
	    st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &ct);
	    if (st != MDS_OK) {
	        return MDS_ERR_IO;
	    }
	    ctx.state = R2PC_ABORTED;
	    st = rename_2pc_journal_put(cat, ct, &ctx);
	    if (st != MDS_OK) {
	        mds_cat_txn_abort(ct);
	        return st;
	    }
	    if (mds_cat_txn_commit(ct) != MDS_OK) {
	        return MDS_ERR_IO;
	    }
	    ct = NULL;

	    if (vote == 0 && transport->abort_rename != NULL) {
	        (void)transport->abort_rename(remote_mds_id, txn_id,
	                                      transport->user_ctx);
	    }

	    st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &ct);
	    if (st == MDS_OK) {
	        st = rename_2pc_journal_del(cat, ct, txn_id,
	                                    R2PC_COORDINATOR);
	        if (st == MDS_OK || st == MDS_ERR_NOTFOUND) {
	            (void)mds_cat_txn_commit(ct);
	        } else {
	            mds_cat_txn_abort(ct);
	        }
	    }

	    return (vote == 0) ? MDS_ERR_XDEV : MDS_ERR_IO;
	}
}

/* -----------------------------------------------------------------------
 * Participant side
 * ----------------------------------------------------------------------- */

/**
 * @brief Handle PrepareRename on the participant side.
 *
 * Validates that dst_parent exists and dst_name does not conflict.
 * Writes PREPARED journal entry.
 *
 * @return 1 for VOTE_COMMIT, 0 for VOTE_ABORT.
 */
int rename_2pc_on_prepare(struct mds_catalogue *cat,
	                      uint64_t txn_id,
	                      uint64_t dst_parent,
	                      const char *dst_name,
	                      const void *inode_data,
	                      size_t data_len,
	                      uint32_t coordinator_mds_id)
{
	if (cat == NULL || dst_name == NULL || inode_data == NULL) {
	    return 0;
	}
	if (data_len < sizeof(struct mds_inode)) {
	    return 0;
	}

	enum mds_status st;

	/* Validate dst_parent exists and is a directory. */
	struct mds_inode parent;
	st = mds_cat_ns_getattr(cat, dst_parent, &parent);
	if (st != MDS_OK || parent.type != MDS_FTYPE_DIR) {
	    return 0;
	}

	/* Check for name conflict. */
	uint64_t existing;
	uint8_t  existing_type;
	st = mds_cat_dirent_get(cat, dst_parent, dst_name,
	                        &existing, &existing_type);
	if (st == MDS_OK) {
	    /* Name already exists — abort. */
	    return 0;
	}
	if (st != MDS_ERR_NOTFOUND) {
	    return 0;
	}

	/* Open a write txn for the journal entry. */
	struct mds_cat_txn *ct = NULL;
	st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &ct);
	if (st != MDS_OK) {
	    return 0;
	}

	/* Write PREPARED journal entry with inode snapshot for recovery. */
	struct rename_2pc_ctx ctx;
	memset(&ctx, 0, sizeof(ctx));
	ctx.txn_id = txn_id;
	ctx.state = R2PC_PREPARED;
	ctx.role = R2PC_PARTICIPANT;
	ctx.dst_parent = dst_parent;
	ctx.remote_mds_id = coordinator_mds_id;
	(void)snprintf(ctx.dst_name, sizeof(ctx.dst_name), "%s", dst_name);
	/*
	 * Persist the inode payload so on_commit() can create the
	 * destination inode from the journal alone, even after the
	 * original TCP connection is gone.
	 */
	if (data_len <= sizeof(ctx.inode_data)) {
	    memcpy(ctx.inode_data, inode_data, data_len);
	    ctx.inode_data_len = (uint32_t)data_len;
	}
	if (data_len >= sizeof(struct mds_inode)) {
	    const struct mds_inode *src_inode = inode_data;
	    ctx.src_child_fileid = src_inode->fileid;
	}

	st = rename_2pc_journal_put(cat, ct, &ctx);
	if (st != MDS_OK) {
	    mds_cat_txn_abort(ct);
	    return 0;
	}

	if (mds_cat_txn_commit(ct) != MDS_OK) {
	    return 0;
	}

	return 1;  /* VOTE_COMMIT */
}

/**
 * @brief Handle CommitRename on the participant side.
 *
 * Reads the inode snapshot from the PREPARE journal entry, inserts
 * the inode and dirent, then deletes the journal entry.
 *
 * The inode_data parameter is no longer required — the participant
 * persists the inode payload in its PREPARE journal for crash
 * recovery across connection loss.
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status rename_2pc_on_commit(struct mds_catalogue *cat,
                                     uint64_t txn_id)
{
	if (cat == NULL) {
	    return MDS_ERR_INVAL;
	}

	enum mds_status st;
	struct mds_cat_txn *ct = NULL;

	st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &ct);
	if (st != MDS_OK) {
	    return st;
	}

	/* Read participant's journal — contains dst_parent, dst_name, and inode snapshot. */
	struct rename_2pc_ctx ctx;
	st = rename_2pc_journal_get(cat, ct, txn_id, R2PC_PARTICIPANT, &ctx);
	if (st != MDS_OK) {
	    mds_cat_txn_abort(ct);
	    return st;
	}

	if (ctx.inode_data_len < sizeof(struct mds_inode)) {
	    mds_cat_txn_abort(ct);
	    return MDS_ERR_INVAL;
	}
	const struct mds_inode *src_inode =
	    (const struct mds_inode *)ctx.inode_data;

	if (rename_2pc_cat_is_rondb(cat)) {
	    uint64_t existing = 0;
	    uint8_t existing_type = 0;
	    uint64_t fileid = rename_2pc_payload_fileid(&ctx);
	    struct mds_inode moved_inode;

	    if (fileid == 0) {
	        mds_cat_txn_abort(ct);
	        return MDS_ERR_INVAL;
	    }

	    st = mds_cat_dirent_get(cat, ctx.dst_parent, ctx.dst_name,
	                            &existing, &existing_type);
	    if (st == MDS_OK) {
	        if (existing != fileid) {
	            mds_cat_txn_abort(ct);
	            return MDS_ERR_EXISTS;
	        }
	        st = rename_2pc_journal_del(cat, ct, txn_id, R2PC_PARTICIPANT);
	        if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
	            mds_cat_txn_abort(ct);
	            return st;
	        }
	        return mds_cat_txn_commit(ct);
	    }
	    if (st != MDS_ERR_NOTFOUND) {
	        mds_cat_txn_abort(ct);
	        return st;
	    }
	    (void)existing_type;

	    st = mds_cat_ns_getattr(cat, fileid, &moved_inode);
	    if (st != MDS_OK) {
	        mds_cat_txn_abort(ct);
	        return st;
	    }

	    moved_inode.parent_fileid = ctx.dst_parent;
	    clock_gettime(CLOCK_REALTIME, &moved_inode.ctime);
	    moved_inode.change++;
	    moved_inode.ds_map = NULL;

	    st = mds_cat_inode_put(cat, ct, &moved_inode);
	    if (st != MDS_OK) {
	        mds_cat_txn_abort(ct);
	        return st;
	    }

	    st = mds_cat_dirent_put(cat, ct, ctx.dst_parent, ctx.dst_name,
	                            fileid, (uint8_t)moved_inode.type);
	    if (st != MDS_OK) {
	        mds_cat_txn_abort(ct);
	        return st;
	    }

	    struct mds_inode parent;
	    st = mds_cat_ns_getattr(cat, ctx.dst_parent, &parent);
	    if (st == MDS_OK) {
	        clock_gettime(CLOCK_REALTIME, &parent.mtime);
	        parent.ctime = parent.mtime;
	        parent.change++;
	        (void)mds_cat_inode_put(cat, ct, &parent);
	    }

	    st = rename_2pc_journal_del(cat, ct, txn_id, R2PC_PARTICIPANT);
	    if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
	        mds_cat_txn_abort(ct);
	        return st;
	    }

	    return mds_cat_txn_commit(ct);
	}

	/* Allocate a new local fileid for the incoming inode. */
	uint64_t new_fileid;
	st = mds_cat_alloc_fileid(cat, ct, &new_fileid);
	if (st != MDS_OK) {
	    mds_cat_txn_abort(ct);
	    return st;
	}

	/* Build inode with new local fileid. */
	struct mds_inode new_inode = *src_inode;
	new_inode.fileid = new_fileid;
	new_inode.ds_map = NULL;  /* Not persisted in inode record. */

	st = mds_cat_inode_put(cat, ct, &new_inode);
	if (st != MDS_OK) {
	    mds_cat_txn_abort(ct);
	    return st;
	}

	/* Write stripe map if present in the inode_data payload. */
	if (ctx.inode_data_len > sizeof(struct mds_inode) + 12) {
	    const uint8_t *smp = ctx.inode_data + sizeof(struct mds_inode);
	    uint32_t sm_sc, sm_su, sm_mc;
	    memcpy(&sm_sc, smp, 4);
	    memcpy(&sm_su, smp + 4, 4);
	    memcpy(&sm_mc, smp + 8, 4);
	    uint32_t total = sm_sc * sm_mc;
	    size_t expected = sizeof(struct mds_inode) + 12 +
	                      total * sizeof(struct mds_ds_map_entry);
	    if (ctx.inode_data_len >= expected && total > 0) {
	        const struct mds_ds_map_entry *sme =
	            (const struct mds_ds_map_entry *)(smp + 12);
	        (void)mds_cat_stripe_map_put(cat, ct, new_fileid,
	                                     sm_sc, sm_su, sm_mc, sme);
	    }
	}

	/* Insert dirent. */
	st = mds_cat_dirent_put(cat, ct, ctx.dst_parent,
	                        ctx.dst_name, new_fileid,
	                        (uint8_t)new_inode.type);
	if (st != MDS_OK) {
	    mds_cat_txn_abort(ct);
	    return st;
	}

	/* Update destination parent. */
	struct mds_inode parent;
	st = mds_cat_ns_getattr(cat, ctx.dst_parent, &parent);
	if (st == MDS_OK) {
	    clock_gettime(CLOCK_REALTIME, &parent.mtime);
	    parent.ctime = parent.mtime;
	    parent.change++;
	    (void)mds_cat_inode_put(cat, ct, &parent);
	}

	/* Delete journal entry. */
	st = rename_2pc_journal_del(cat, ct, txn_id, R2PC_PARTICIPANT);
	if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
	    mds_cat_txn_abort(ct);
	    return st;
	}

	return mds_cat_txn_commit(ct);
}

/**
 * @brief Handle AbortRename on the participant side.
 *
 * Deletes journal entry.  No local changes to undo.
 */
enum mds_status rename_2pc_on_abort(struct mds_catalogue *cat,
                                    uint64_t txn_id)
{
	if (cat == NULL) {
	    return MDS_ERR_INVAL;
	}

	struct mds_cat_txn *ct = NULL;
	enum mds_status st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &ct);
	if (st != MDS_OK) {
	    return st;
	}
	st = rename_2pc_journal_del(cat, ct, txn_id, R2PC_PARTICIPANT);
	if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
	    mds_cat_txn_abort(ct);
	    return st;
	}

	return mds_cat_txn_commit(ct);
}

struct rename_2pc_recovery_ctx {
	struct mds_catalogue *cat;
	const struct rename_2pc_transport *transport;
	enum mds_status first_error;
	uint32_t resolved;
	uint32_t unresolved;
};

static enum mds_status rename_2pc_journal_del_standalone(
	struct mds_catalogue *cat, uint64_t txn_id, uint8_t role)
{
	struct mds_cat_txn *txn = NULL;
	enum mds_status st;

	st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn);
	if (st != MDS_OK) {
		return st;
	}

	st = rename_2pc_journal_del(cat, txn, txn_id, role);
	if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
		mds_cat_txn_abort(txn);
		return st;
	}

	if (mds_cat_txn_commit(txn) != MDS_OK) {
		return MDS_ERR_IO;
	}
	return MDS_OK;
}

static uint64_t rename_2pc_record_fileid(
	const struct mds_coord_journal_record *record)
{
	struct mds_inode inode;

	if (record == NULL) {
		return 0;
	}
	if (record->src_child_fileid != 0) {
		return record->src_child_fileid;
	}
	if (record->payload_len < sizeof(inode)) {
		return 0;
	}

	memcpy(&inode, record->payload, sizeof(inode));
	return inode.fileid;
}

static enum mds_status rename_2pc_dest_matches_record(
	struct mds_catalogue *cat,
	const struct mds_coord_journal_record *record,
	bool *matches)
{
	uint64_t child = 0;
	uint8_t type = 0;
	uint64_t fileid;
	enum mds_status st;

	if (cat == NULL || record == NULL || matches == NULL) {
		return MDS_ERR_INVAL;
	}

	*matches = false;
	fileid = rename_2pc_record_fileid(record);
	if (fileid == 0) {
		return MDS_ERR_INVAL;
	}

	st = mds_cat_dirent_get(cat, record->dst_parent_fileid,
	                        record->dst_name, &child, &type);
	if (st == MDS_ERR_NOTFOUND) {
		return MDS_OK;
	}
	if (st != MDS_OK) {
		return st;
	}
	(void)type;

	*matches = (child == fileid);
	return MDS_OK;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static int rename_2pc_recover_rondb_scan_cb(
	const struct mds_coord_journal_record *record, void *opaque)
{
	struct rename_2pc_recovery_ctx *ctx = opaque;
	enum mds_status st;
	bool dst_matches = false;

	if (ctx == NULL || record == NULL) {
		return 0;
	}

	/* Ignore non-rename users of the shared journal. */
	if (record->state > (uint8_t)R2PC_ABORTED) {
		return 0;
	}

	switch ((enum rename_2pc_state)record->state) {
	case R2PC_ABORTED:
		st = rename_2pc_journal_del_standalone(
			ctx->cat, record->txn_id, record->role);
		if (st == MDS_OK) {
			ctx->resolved++;
		} else {
			if (ctx->first_error == MDS_OK) {
				ctx->first_error = st;
			}
			ctx->unresolved++;
		}
		return 0;

	case R2PC_PREPARED:
		if (record->role == (uint8_t)R2PC_COORDINATOR) {
			st = rename_2pc_journal_del_standalone(
				ctx->cat, record->txn_id, record->role);
			if (st == MDS_OK) {
				ctx->resolved++;
			} else {
				if (ctx->first_error == MDS_OK) {
					ctx->first_error = st;
				}
				ctx->unresolved++;
			}
			return 0;
		}
		if (record->role != (uint8_t)R2PC_PARTICIPANT) {
			return 0;
		}

		st = rename_2pc_dest_matches_record(ctx->cat, record,
		                                    &dst_matches);
		if (st != MDS_OK) {
			if (ctx->first_error == MDS_OK) {
				ctx->first_error = st;
			}
			ctx->unresolved++;
			return 0;
		}
		if (!dst_matches) {
			ctx->unresolved++;
			return 0;
		}

		st = rename_2pc_journal_del_standalone(
			ctx->cat, record->txn_id, record->role);
		if (st == MDS_OK) {
			ctx->resolved++;
		} else {
			if (ctx->first_error == MDS_OK) {
				ctx->first_error = st;
			}
			ctx->unresolved++;
		}
		return 0;

	case R2PC_COMMITTED:
		if (record->role == (uint8_t)R2PC_PARTICIPANT) {
			st = rename_2pc_journal_del_standalone(
				ctx->cat, record->txn_id, record->role);
			if (st == MDS_OK) {
				ctx->resolved++;
			} else {
				if (ctx->first_error == MDS_OK) {
					ctx->first_error = st;
				}
				ctx->unresolved++;
			}
			return 0;
		}
		if (record->role != (uint8_t)R2PC_COORDINATOR) {
			return 0;
		}

		st = rename_2pc_dest_matches_record(ctx->cat, record,
		                                    &dst_matches);
		if (st != MDS_OK) {
			if (ctx->first_error == MDS_OK) {
				ctx->first_error = st;
			}
			ctx->unresolved++;
			return 0;
		}
		if (!dst_matches && ctx->transport != NULL &&
		    ctx->transport->commit != NULL &&
		    ctx->transport->commit(record->remote_mds_id,
		                          record->txn_id,
		                          ctx->transport->user_ctx) == 0) {
			dst_matches = true;
		}
		if (!dst_matches) {
			ctx->unresolved++;
			return 0;
		}

		st = rename_2pc_rondb_cleanup_source(
			ctx->cat, record->src_parent_fileid, record->src_name);
		if (st != MDS_OK) {
			if (ctx->first_error == MDS_OK) {
				ctx->first_error = st;
			}
			ctx->unresolved++;
			return 0;
		}

		st = rename_2pc_journal_del_standalone(
			ctx->cat, record->txn_id, record->role);
		if (st == MDS_OK) {
			ctx->resolved++;
		} else {
			if (ctx->first_error == MDS_OK) {
				ctx->first_error = st;
			}
			ctx->unresolved++;
		}
		return 0;

	case R2PC_IDLE:
	default:
		return 0;
	}
}

static enum mds_status rename_2pc_recover_rondb(
	struct mds_catalogue *cat,
	const struct rename_2pc_transport *transport)
{
	struct rename_2pc_recovery_ctx ctx;
	enum mds_status st;

	memset(&ctx, 0, sizeof(ctx));
	ctx.cat = cat;
	ctx.transport = transport;
	ctx.first_error = MDS_OK;

	st = mds_coord_journal_scan(cat, rename_2pc_recover_rondb_scan_cb,
	                            &ctx);
	if (st != MDS_OK) {
		return st;
	}
	if (ctx.unresolved > 0) {
		return MDS_ERR_DELAY;
	}
	return ctx.first_error;
}

/* -----------------------------------------------------------------------
 * Local cross-shard rename (catalogue-native)
 *
 * In the catalogue model, all shards share the same backend (RonDB or
 * catalogue).  Moving a file between shards does NOT require physically
 * copying records — the metadata already lives in the shared cluster.
 * The rename is a dirent swap + parent timestamp update + fileid-map
 * routing change.  No 2PC journal is needed because the catalogue
 * vtable provides its own atomicity guarantees.
 * ----------------------------------------------------------------------- */

#include "mds_shard.h"
#include "migration.h"

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status rename_2pc_initiate_local_shard(
    struct mds_catalogue *root_cat,
    const struct mds_shard *src_shard,
    const struct mds_shard *dst_shard,
    uint64_t src_parent, const char *src_name,
    uint64_t dst_parent, const char *dst_name)
{
    if (root_cat == NULL || src_shard == NULL || dst_shard == NULL ||
        src_name == NULL || dst_name == NULL) {
        return MDS_ERR_INVAL;
    }
    if (src_shard->cat == NULL || dst_shard->cat == NULL) {
        return MDS_ERR_INVAL;
    }

    enum mds_status st;
    struct mds_catalogue *scat = src_shard->cat;
    struct mds_catalogue *dcat = dst_shard->cat;

    /* 1. Read source entry. */
    uint64_t src_child = 0;
    uint8_t  src_type = 0;
    st = mds_cat_dirent_get(scat, src_parent, src_name,
                            &src_child, &src_type);
    if (st != MDS_OK) {
        return st;
    }

    /* Directories cannot cross shard boundaries. */
    if (src_type == (uint8_t)MDS_FTYPE_DIR) {
        return MDS_ERR_XDEV;
    }

    /* 2. Check destination for overwrite. */
    uint64_t replaced_fid = 0;
    bool     replaced_had_links = false;
    {
        uint64_t existing_fid = 0;
        uint8_t  existing_type = 0;
        st = mds_cat_dirent_get(dcat, dst_parent, dst_name,
                                &existing_fid, &existing_type);
        if (st == MDS_OK) {
            /* Type compatibility checks. */
            if (src_type == (uint8_t)MDS_FTYPE_DIR &&
                existing_type != (uint8_t)MDS_FTYPE_DIR) {
                return MDS_ERR_NOTDIR;
            }
            if (src_type != (uint8_t)MDS_FTYPE_DIR &&
                existing_type == (uint8_t)MDS_FTYPE_DIR) {
                return MDS_ERR_ISDIR;
            }
            if (existing_type == (uint8_t)MDS_FTYPE_DIR) {
                bool empty = false;
                (void)mds_cat_dir_is_empty(dcat, existing_fid, &empty);
                if (!empty) {
                    return MDS_ERR_NOTEMPTY;
                }
            }
            replaced_fid = existing_fid;

            struct mds_inode rep_ino;
            if (mds_cat_ns_getattr(dcat, existing_fid,
                                   &rep_ino) == MDS_OK) {
                replaced_had_links = (rep_ino.nlink > 1);
            }
        } else if (st != MDS_ERR_NOTFOUND) {
            return st;
        }
    }

    /* 3. Handle destination overwrite cleanup. */
    if (replaced_fid != 0) {
        /* Remove dest dirent (will be re-created below). */
        (void)mds_cat_dirent_del(dcat, NULL, dst_parent, dst_name);

        if (!replaced_had_links) {
            /* Enqueue GC for DS data files. */
            struct mds_ds_map_entry *rep_sm = NULL;
            uint32_t rep_sc = 0, rep_su = 0, rep_mc = 0;
            if (mds_cat_stripe_map_get(dcat, replaced_fid,
                                       &rep_sc, &rep_su, &rep_mc,
                                       &rep_sm) == MDS_OK &&
                rep_sm != NULL) {
                uint32_t total = rep_sc * rep_mc;
                for (uint32_t gi = 0; gi < total; gi++) {
                    (void)mds_cat_gc_enqueue(dcat, NULL,
                        replaced_fid, rep_sm[gi].ds_id,
                        rep_sm[gi].nfs_fh, rep_sm[gi].nfs_fh_len);
                }
                free(rep_sm);
            }
            (void)mds_cat_stripe_map_del(dcat, NULL, replaced_fid);
            (void)mds_cat_inode_del(dcat, NULL, replaced_fid);
            (void)mds_cat_inline_del(dcat, NULL, replaced_fid);
        } else {
            /* Decrement nlink for replaced file with hard links. */
            (void)mds_cat_ns_nlink_adjust(dcat, replaced_fid, -1);
        }
    }

    /* 4. Perform the rename via catalogue API.
     *
     * In shared-backend mode (RonDB), both scat and dcat point to the
     * same NDB cluster.  mds_cat_ns_rename handles the dirent swap +
     * parent timestamp updates atomically.  For catalogue backends
     * pointing to the same DB, the same holds. */
    st = mds_cat_ns_rename(scat, NULL,
                           src_parent, src_name,
                           dst_parent, dst_name);
    if (st != MDS_OK) {
        return st;
    }

    /* 5. Update fileid routing: file now belongs to dst shard. */
    (void)mds_cat_shard_fileid_put(root_cat, NULL,
                                   src_child, dst_shard->shard_id);

    /* Remove fileid map for replaced file if it was fully deleted. */
    if (replaced_fid != 0 && !replaced_had_links) {
        (void)mds_cat_shard_fileid_del(root_cat, NULL, replaced_fid);
    }

    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Crash recovery
 * ----------------------------------------------------------------------- */

/**
 * @brief Recover incomplete 2PC transactions on startup.
 *
 * Scans rename_journal DBI.  For each entry, decides based on
 * state and role:
 *
 *   - ABORTED (any role): safe to delete — no side effects.
 *   - PREPARED + COORDINATOR: coordinator crashed before deciding.
 *     Source dirent is still intact.  Safe to abort — delete.
 *   - COMMITTED + COORDINATOR: source is already deleted locally.
 *     If transport is provided, re-send CommitRename.  On success,
 *     delete the journal entry.  On failure, keep it.
 *   - PREPARED + PARTICIPANT: outcome unknown.  Keep entry.
 *
 * @param cat        Catalogue handle.
 * @param transport  Optional transport for commit retry (may be NULL).
 * @return MDS_OK if all entries were resolved.
 *         MDS_ERR_DELAY if unresolved entries remain.
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status rename_2pc_recover(struct mds_catalogue *cat,
	                               const struct rename_2pc_transport *transport,
	                               const struct mds_shard_map *shard_map)
{
	(void)shard_map; /* Legacy parameter — no longer used. */

	if (cat == NULL) {
	    return MDS_ERR_INVAL;
	}
	return rename_2pc_recover_rondb(cat, transport);
}

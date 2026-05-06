/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * commit_queue.h -- Catalogue-dispatch commit pipeline.
 *
 * Request threads build immutable operation descriptors and submit
 * them via commit_queue_submit().  Each op is dispatched directly
 * through the catalogue vtable on the caller's thread.
 *
 * Core invariant: no NFS reply is sent before the catalogue write
 * that makes the reply true.
 */

#ifndef COMMIT_QUEUE_H
#define COMMIT_QUEUE_H

#include <stdbool.h>
#include <stdint.h>

#include "pnfs_mds.h"
#include "layout_ds_ids.h"
#include "open_state.h"

struct repl_conn;
struct ds_cache;
struct mds_catalogue;

/* -----------------------------------------------------------------------
 * Operation descriptor
 * ----------------------------------------------------------------------- */

#define COMMIT_OP_NAME_MAX  256  /* NFS name + NUL */

/** Maximum durable DS IDs per layout-state op. */
#define COMMIT_OP_LAYOUT_MAX_DS  MDS_LAYOUT_DS_ID_MAX

enum commit_op_type {
	COMMIT_OP_CREATE,
	COMMIT_OP_REMOVE,
	COMMIT_OP_RENAME,
	COMMIT_OP_LINK,
	COMMIT_OP_SETATTR,
	COMMIT_OP_INLINE_WRITE,
	COMMIT_OP_INLINE_DEL,
	COMMIT_OP_INODE_PUT,
	COMMIT_OP_RECOVERY_PUT,
	COMMIT_OP_RECOVERY_DEL,
	COMMIT_OP_DS_STATE,
	COMMIT_OP_LAYOUT_STATE_PUT,
	COMMIT_OP_LAYOUT_STATE_DEL,
	COMMIT_OP_STRIPE_MAP_PUT,
	COMMIT_OP_REBALANCE_MOVE,
	COMMIT_OP_QUOTA_ADJUST,
	COMMIT_OP_DS_REGISTER,
	COMMIT_OP_DS_REMOVE,
};

/** Arguments for COMMIT_OP_CREATE. */
struct commit_op_create {
	uint64_t            parent_fileid;
	char                name[COMMIT_OP_NAME_MAX];
	enum mds_file_type  type;
	uint32_t            mode;
	uint64_t            uid;
	uint64_t            gid;
	struct mds_inode   *out;  /* caller-owned, filled on success */
	struct ds_prealloc_ctx *prealloc; /* NULL = inline mode */

	/*
	 * Layout pre-grant for fused CREATE + LAYOUTGET CQ commit.
	 *
	 * When layout_pregrant is true, the writer thread will
	 * attempt to persist layout_state + ds_layout_idx in the
	 * same catalogue transaction as the CREATE, eliminating a
	 * separate CQ round-trip for the following LAYOUTGET.
	 *
	 * Best-effort: if any step fails, CREATE still succeeds
	 * and layout_pregrant_ok remains false.  The LAYOUTGET
	 * handler falls back to its normal persist path.
	 */
	bool                layout_pregrant;
	uint64_t            layout_clientid;
	uint32_t            layout_iomode;
	uint64_t            layout_offset;
	uint64_t            layout_length;
	struct nfs4_stateid layout_stateid;
	const uint32_t     *layout_ds_ids; /**< Borrowed; valid during submit. */
	uint32_t            layout_ds_count;
	/*
	 * When true, the CQ writer skips the mds_coord_layout_grant
	 * NDB write for the pregrant and still reports
	 * layout_pregrant_ok = true.  Mirrors the guard at
	 * src/mds/compound_layout.c:731 on the non-fused LAYOUTGET
	 * path.  Set from cd->skip_transient_ndb in op_open.
	 * See docs/design-post-phase3-candidates.md for the lab
	 * regression that motivated this gate.
	 */
	bool                skip_transient_ndb;
	bool                layout_pregrant_ok; /**< Output: set by writer thread on success. */
};

/** Arguments for COMMIT_OP_REMOVE. */
struct commit_op_remove {
	uint64_t  parent_fileid;
	char      name[COMMIT_OP_NAME_MAX];
};

/** Arguments for COMMIT_OP_RENAME. */
struct commit_op_rename {
	uint64_t  src_parent;
	char      src_name[COMMIT_OP_NAME_MAX];
	uint64_t  dst_parent;
	char      dst_name[COMMIT_OP_NAME_MAX];
};

/** Arguments for COMMIT_OP_LINK. */
struct commit_op_link {
	uint64_t  parent_fileid;
	char      name[COMMIT_OP_NAME_MAX];
	uint64_t  target_fileid;
};

/** Arguments for COMMIT_OP_SETATTR. */
struct commit_op_setattr {
	uint64_t            fileid;
	struct mds_inode    attrs;
	uint32_t            mask;
};

/** Arguments for COMMIT_OP_INLINE_WRITE. */
struct commit_op_inline_write {
	uint64_t  fileid;
	uint32_t  offset;
	uint32_t  len;
	const uint8_t *data;  /* caller-owned; must remain valid until completion */
};

/** Arguments for COMMIT_OP_INLINE_DEL. */
struct commit_op_inline_del {
	uint64_t  fileid;
};

/** Arguments for COMMIT_OP_INODE_PUT. */
struct commit_op_inode_put {
	struct mds_inode  inode;
};

/** Arguments for COMMIT_OP_RECOVERY_PUT. */
struct commit_op_recovery_put {
	uint64_t  clientid;
	uint8_t   co_ownerid[1024];
	uint32_t  co_ownerid_len;
	uint8_t   verifier[8];
};

/** Arguments for COMMIT_OP_RECOVERY_DEL. */
struct commit_op_recovery_del {
	uint64_t  clientid;
};

/** Arguments for COMMIT_OP_DS_STATE. */
struct commit_op_ds_state {
	uint32_t  ds_id;
	uint32_t  new_state;
};


/** Arguments for COMMIT_OP_DS_REGISTER. */
struct commit_op_ds_register {
	struct mds_ds_info info;
};

/** Arguments for COMMIT_OP_DS_REMOVE. */
struct commit_op_ds_remove {
	uint32_t ds_id;
};
/** Arguments for COMMIT_OP_LAYOUT_STATE_PUT.
 *
 *  When create_stripe_map is true, the writer thread will also
 *  create the stripe map if it does not already exist, ensuring
 *  stripe_map + layout_state + ds_layout_idx are written atomically
 *  in a single catalogue transaction.
 *
 *  stripe_entries is a borrowed pointer valid for the lifetime of
 *  the CQ submit (the submitter blocks until the writer completes).
 */
struct commit_op_layout_put {
	uint64_t             clientid;
	uint64_t             fileid;
	uint32_t             iomode;
	uint64_t             offset;
	uint64_t             length;
	struct nfs4_stateid  stateid;
	const uint32_t      *ds_ids; /**< Borrowed; valid during submit. */
	uint32_t             ds_count;

	/* Stripe-map creation (set create_stripe_map=true to enable). */
	bool                              create_stripe_map;
	uint32_t                          stripe_count;
	uint32_t                          stripe_unit;
	uint32_t                          mirror_count;
	const struct mds_ds_map_entry    *stripe_entries; /**< Borrowed; valid during submit. */
};

/** Arguments for COMMIT_OP_LAYOUT_STATE_DEL. */
struct commit_op_layout_del {
	uint64_t  clientid;
	uint64_t  fileid;
	uint8_t   stateid_other[12]; /**< Exact layout stateid.other key. */
	const uint32_t *ds_ids; /**< Borrowed; valid during submit. */
	uint32_t  ds_count;
};

/** Arguments for COMMIT_OP_STRIPE_MAP_PUT (resilvering). */
struct commit_op_stripe_map_put {
	uint64_t                          fileid;
	uint32_t                          stripe_count;
	uint32_t                          stripe_unit;
	uint32_t                          mirror_count;
	const struct mds_ds_map_entry    *entries; /**< Borrowed; valid during submit. */
};

/** Arguments for COMMIT_OP_REBALANCE_MOVE (stripe-map update + GC enqueue). */
struct commit_op_rebalance_move {
	uint64_t                          fileid;
	uint32_t                          stripe_count;
	uint32_t                          stripe_unit;
	uint32_t                          mirror_count;
	const struct mds_ds_map_entry    *entries; /**< New stripe map (borrowed). */
	uint64_t                          gc_fileid;
	uint32_t                          gc_ds_id;
	uint8_t                           gc_nfs_fh[MDS_NFS_FH_MAX];
	uint32_t                          gc_fh_len;
};
/** Arguments for COMMIT_OP_QUOTA_ADJUST. */
struct commit_op_quota_adjust {
	uint64_t uid;
	uint64_t gid;
	int64_t  delta_bytes;
	int32_t  delta_inodes;
};

/**
 * Operation descriptor.
 *
 * Allocated on the submitting thread's stack (or heap).  The
 * descriptor must remain valid for the duration of the
 * commit_queue_submit() call.
 */
struct commit_op {
	enum commit_op_type  type;

	union {
		struct commit_op_create        create;
		struct commit_op_remove        remove_op;
		struct commit_op_rename        rename_op;
		struct commit_op_link          link_op;
		struct commit_op_setattr       setattr;
		struct commit_op_inline_write  inline_write;
		struct commit_op_inline_del    inline_del;
		struct commit_op_inode_put     inode_put;
		struct commit_op_recovery_put  recovery_put;
		struct commit_op_recovery_del  recovery_del;
		struct commit_op_ds_state     ds_state;
		struct commit_op_ds_register  ds_register;
		struct commit_op_ds_remove    ds_remove;
		struct commit_op_layout_put   layout_put;
		struct commit_op_layout_del   layout_del;
		struct commit_op_stripe_map_put stripe_map_put;
		struct commit_op_rebalance_move rebalance_move;
		struct commit_op_quota_adjust  quota_adjust;
	} args;

	enum mds_status   result;
};

/* -----------------------------------------------------------------------
 * Commit queue
 * ----------------------------------------------------------------------- */

struct commit_queue;

/**
 * Create and start a commit queue with its writer thread.
 *
 * @param cat             Catalogue handle (authoritative; NULL for
 *                        per-shard CQs that have no catalogue yet).
 * @param repl            Replication connection (NULL if no standby).
 * @param repl_mode       Replication mode (enum mds_repl_mode).
 * @param batch_size      Max ops per batch (0 = default 128).
 * @param batch_max_bytes Max payload bytes per batch (0 = default 1 MiB).
 * @param flush_ms        Max ms before forced flush (0 = default 2).
 * @param queue_depth     Max queued ops before backpressure (0 = default 4096).
 * @param shard_id        Shard ID to stamp into replicated deltas (0 = root shard).
 * @param out             Receives the new commit queue.
 * @return 0 on success, -errno on failure.
 */
int commit_queue_create(struct mds_catalogue *cat,
			struct repl_conn *repl, int repl_mode,
			uint32_t batch_size, uint32_t batch_max_bytes,
			uint32_t flush_ms, uint32_t queue_depth,
			uint32_t shard_id,
			struct commit_queue **out);

/**
 * Submit an operation to the commit queue and block until committed.
 *
 * The caller fills op->type and op->args before calling.  On return,
 * op->result contains the outcome.  The caller must not modify the
 * op descriptor between submit and return.
 *
 * @param cq   Commit queue.
 * @param op   Operation descriptor (caller-owned, stack is fine).
 * @return op->result (convenience -- same value stored in the op).
 */
enum mds_status commit_queue_submit(struct commit_queue *cq,
				    struct commit_op *op);

/**
 * Shut down the commit queue.
 *
 * Flushes any pending batch, stops the writer thread, and frees
 * resources.  Blocks until the writer thread exits.
 *
 * @param cq  Commit queue (may be NULL).
 */
void commit_queue_destroy(struct commit_queue *cq);

/** @brief Return the replication connection from the commit queue (may be NULL). */
struct repl_conn *commit_queue_get_repl(const struct commit_queue *cq);

/** @brief Return the replication mode from the commit queue. */
int commit_queue_get_repl_mode(const struct commit_queue *cq);

/**
 * @brief Wire an optional DS cache into the commit queue.
 *
 * When present, the writer thread may use the cache for best-effort
 * CREATE-time patched DS filehandle derivation inside the same
 * catalogue transaction as the CREATE itself.
 */
void commit_queue_set_ds_cache(struct commit_queue *cq,
			       struct ds_cache *ds_cache);

#endif /* COMMIT_QUEUE_H */

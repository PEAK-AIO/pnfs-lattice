/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * subtree_split.h — Subtree split/merge orchestration.
 *
 * Orchestrates the full split/merge workflow:
 *   validate → register → optionally migrate → commit / rollback.
 *
 * The subtree_map stays pure (map-only operations); this module
 * performs the catalogue and migration side effects.
 */

#ifndef SUBTREE_SPLIT_H
#define SUBTREE_SPLIT_H

#include <stdint.h>
#include "pnfs_mds.h"

struct subtree_map;
struct mds_catalogue;
struct migration_transport;
struct mds_shard_map;

/**
 * @brief Execute a subtree split.
 *
 * Full procedure:
 *   1. Validate map-level invariants (subtree_map_validate_split).
 *   2. Check the catalogue: child_fileid exists, is a directory,
 *      and is not a junction.
 *   3. Register child as new subtree in the map.
 *   4. If dest_mds_id != self: migrate via migration_initiate().
 *   5. On migration failure: rollback (remove child from map).
 *
 * @param map              Subtree map handle.
 * @param cat              Catalogue handle.
 * @param parent_path      Parent subtree root (e.g. "/data").
 * @param child_path       Proposed child subtree root (e.g. "/data/hot").
 * @param child_fileid     Fileid of the child directory.
 * @param dest_mds_id      Target MDS for the new subtree (self = local split).
 * @param transport        Migration transport (may be NULL if local split).
 * @param max_split_depth  Max depth (0 = default).
 * @return MDS_OK on success, or appropriate error code.
 */
enum mds_status subtree_split_execute(
    struct subtree_map *map,
    struct mds_catalogue *cat,
    const char *parent_path,
    const char *child_path,
    uint64_t child_fileid,
    uint32_t dest_mds_id,
    const struct migration_transport *transport,
    uint32_t max_split_depth);

/**
 * @brief Execute a subtree merge (reverse of split).
 *
 * Procedure:
 *   1. Validate child_path is a subtree root and parent_path is its parent.
 *   2. If child owned remotely: migrate back to parent's owner first.
 *   3. Remove child from map.
 *
 * @param map          Subtree map handle.
 * @param cat          Catalogue handle.
 * @param parent_path  Parent subtree root.
 * @param child_path   Child subtree to merge back.
 * @param transport    Migration transport (may be NULL if local).
 * @return MDS_OK on success.
 */
enum mds_status subtree_merge_execute(
    struct subtree_map *map,
    const struct mds_catalogue *cat,
    const char *parent_path,
    const char *child_path,
    const struct migration_transport *transport);


/* -----------------------------------------------------------------------
 * Automatic split evaluator
 * ----------------------------------------------------------------------- */

/**
 * Configuration for the automatic split evaluator.
 */
struct split_eval_cfg {
    uint64_t split_threshold;     /**< ops/interval to propose split. Default 10000. */
    uint32_t eval_interval_sec;   /**< Evaluation cadence in seconds. Default 300 (5 min). */
    bool     auto_split_enabled;  /**< Master switch (proposal collection). Default false. */
    bool     auto_execute;        /**< Auto-execute approved proposals. Default false. */
    uint32_t cooldown_sec;        /**< Min seconds between splits on same subtree. Default 600. */
    uint32_t sustained_intervals; /**< Consecutive hot intervals before proposing. Default 2. */
    uint32_t min_children;        /**< Skip subtrees with fewer children. Default 4. */
};

/** Default configuration values. */
#define SPLIT_EVAL_DEFAULT_THRESHOLD   10000
#define SPLIT_EVAL_DEFAULT_INTERVAL    300
#define SPLIT_EVAL_DEFAULT_ENABLED     false
#define SPLIT_EVAL_DEFAULT_AUTO_EXEC   false
#define SPLIT_EVAL_DEFAULT_COOLDOWN    600
#define SPLIT_EVAL_DEFAULT_SUSTAINED   2
#define SPLIT_EVAL_DEFAULT_MIN_CHILDREN 4

/* -----------------------------------------------------------------------
 * Split proposal (durable object)
 * ----------------------------------------------------------------------- */

/** Proposal eligibility / lifecycle state. */
enum split_proposal_state {
    PROPOSAL_PENDING   = 0,  /**< Awaiting operator approval. */
    PROPOSAL_APPROVED  = 1,  /**< Approved, awaiting execution. */
    PROPOSAL_REJECTED  = 2,  /**< Explicitly rejected by operator. */
    PROPOSAL_EXECUTING = 3,  /**< Split in progress. */
    PROPOSAL_DONE      = 4,  /**< Split completed successfully. */
    PROPOSAL_FAILED    = 5,  /**< Split execution failed. */
    PROPOSAL_COOLDOWN  = 6,  /**< Recently split, in cooldown. */
};

#define SPLIT_PROPOSAL_MAX       64
#define SPLIT_REJECT_REASON_MAX  128

struct split_proposal {
    char                       path[MDS_MAX_PATH];
    uint64_t                   sampled_ops;       /**< ops/interval at proposal time. */
    uint64_t                   first_seen_sec;    /**< Wall-clock when first hot. */
    uint32_t                   hot_intervals;     /**< Consecutive hot evaluation intervals. */
    uint32_t                   target_mds_id;     /**< Suggested destination MDS (0 = undecided). */
    enum split_proposal_state  state;
    uint64_t                   cooldown_expiry;   /**< Wall-clock after which re-split allowed. */
    char                       reject_reason[SPLIT_REJECT_REASON_MAX];
};

struct split_evaluator;

/**
 * @brief Start the automatic split evaluator thread.
 *
 * Spawns a periodic evaluation thread that scans local subtrees.
 * If any subtree's op_count exceeds threshold * interval, a split
 * is proposed (logged, not auto-executed in this version).
 *
 * @param map          Subtree map handle.
 * @param cat          Catalogue handle used for path resolution and readdir.
 * @param transport    Migration transport (may be NULL for proposals only).
 * @param cfg          Evaluator configuration.
 * @param out          Receives evaluator handle.
 * @return 0 on success, -1 on failure.
 */
int split_evaluator_start(struct subtree_map *map,
                          struct mds_catalogue *cat,
                          const struct migration_transport *transport,
                          const struct split_eval_cfg *cfg,
                          struct split_evaluator **out);

/**
 * @brief Stop the evaluator and join its thread.
 */
void split_evaluator_stop(struct split_evaluator *eval);

/**
 * @brief Return the number of hot-subtree proposals made since start.
 *
 * For testing/monitoring. Thread-safe (atomic read).
 */
uint64_t split_evaluator_proposal_count(const struct split_evaluator *eval);

/**
 * @brief List all current proposals.
 *
 * Returns a snapshot of the proposal array.  Caller must free(*out).
 *
 * @param eval      Evaluator handle (NULL => empty list).
 * @param[out] out  Receives malloc'd array of proposals.
 * @param[out] count Receives number of proposals.
 * @return 0 on success, -1 on allocation failure.
 */
int split_evaluator_list_proposals(const struct split_evaluator *eval,
                                   struct split_proposal **out,
                                   uint32_t *count);

/**
 * @brief Approve a pending proposal for execution.
 *
 * @param eval  Evaluator handle.
 * @param path  Subtree path of the proposal to approve.
 * @return 0 on success, -1 if not found or not PENDING.
 */
int split_evaluator_approve(struct split_evaluator *eval,
                            const char *path);

/**
 * @brief Reject a pending proposal.
 *
 * @param eval    Evaluator handle.
 * @param path    Subtree path.
 * @param reason  Human-readable rejection reason (may be NULL).
 * @return 0 on success, -1 if not found or not PENDING.
 */
int split_evaluator_reject(struct split_evaluator *eval,
                           const char *path,
                           const char *reason);

/**
 * @brief Toggle auto-execute at runtime.
 *
 * @param eval    Evaluator handle.
 * @param enable  true to enable auto-execution, false to disable.
 */
void split_evaluator_set_auto_execute(struct split_evaluator *eval,
                                      bool enable);

/**
 * @brief Query whether auto-execute is enabled.
 */
bool split_evaluator_get_auto_execute(const struct split_evaluator *eval);

/**
 * @brief Run one evaluation cycle synchronously (for testing).
 *
 * Exposed so unit tests can trigger evaluation without waiting
 * for the timer. Not intended for production use.
 */
void split_evaluator_run_once(struct split_evaluator *eval);

#endif /* SUBTREE_SPLIT_H */

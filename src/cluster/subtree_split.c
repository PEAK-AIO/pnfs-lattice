/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * subtree_split.c -- Subtree split/merge orchestration.
 *
 * Performs validation, catalogue checks, and migration coordination.
 * The subtree_map itself stays pure (map-only operations).
 */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>

#include "pnfs_mds.h"
#include "subtree_split.h"
#include "subtree_map.h"
#include "mds_catalogue.h"
#include "referral.h"
#include "migration.h"
#include "mds_shard.h"

enum mds_status subtree_split_execute(
    struct subtree_map *map,
    struct mds_catalogue *cat,
    const char *parent_path,
    const char *child_path,
    uint64_t child_fileid,
    uint32_t dest_mds_id,
    const struct migration_transport *transport,
    uint32_t max_split_depth)
{
    enum mds_status st;
    struct mds_inode inode;
    uint32_t self_id;

    if (map == NULL || cat == NULL ||
        parent_path == NULL || child_path == NULL) {
        return MDS_ERR_INVAL;
    }

    self_id = subtree_map_self_id(map);

    /* Step 1: Map-level validation. */
    st = subtree_map_validate_split(map, parent_path, child_path,
                                    max_split_depth);
    if (st != MDS_OK) {
        return st;
}

    /* Step 2: Catalogue checks -- child must exist and be a directory. */
    st = mds_cat_ns_getattr(cat, child_fileid, &inode);
    if (st != MDS_OK) {
        return st;
}

    if (inode.type != MDS_FTYPE_DIR) {
        return MDS_ERR_INVAL;
}

    /* Step 3: Must not be a junction (sticky bit = referral). */
    {
        int jrc = referral_is_junction(cat, child_fileid);
        if (jrc < 0) {
            return MDS_ERR_IO;  /* Catalogue read error */
}
        if (jrc == 1) {
            return MDS_ERR_INVAL;
}
    }

    /*
     * TODO: Verify that child_path actually resolves to child_fileid
     * in the namespace.  Currently the caller is trusted to supply a
     * consistent pair; a future enhancement should do a path-lookup
     * cross-check here to prevent split-with-wrong-fileid bugs.
     */

    /* Step 4: Register child in map (local claim first). */
    st = subtree_map_register_split(map, parent_path, child_path,
                                    self_id);
    if (st != MDS_OK) {
        return st;
}

    /* Step 5: If remote destination, migrate. */
    if (dest_mds_id != self_id) {
        if (transport == NULL) {
            /* No transport -- rollback. */
            subtree_map_remove_subtree(map, child_path);
            return MDS_ERR_INVAL;
        }

        st = migration_initiate(cat, map, transport, child_path,
                                child_fileid, dest_mds_id,
                                       NULL);
        if (st != MDS_OK) {
            /* Migration failed -- rollback the split. */
            subtree_map_remove_subtree(map, child_path);
            return st;
        }
    }

    return MDS_OK;
}

enum mds_status subtree_merge_execute(
    struct subtree_map *map,
    const struct mds_catalogue *cat,
    const char *parent_path,
    const char *child_path,
    /* NOLINTNEXTLINE(misc-unused-parameters) */
    const struct migration_transport *transport)
{
    enum mds_status st;
    struct subtree_entry parent_entry;
    struct subtree_entry child_entry;
    size_t plen;

    if (map == NULL || cat == NULL ||
        parent_path == NULL || child_path == NULL) {
        return MDS_ERR_INVAL;
}

    /* Cannot merge root. */
    if (strcmp(child_path, "/") == 0) {
        return MDS_ERR_INVAL;
}

    /* Verify parent and child exist in map. */
    st = subtree_map_lookup(map, parent_path, &parent_entry);
    if (st != MDS_OK) {
        return st;
}

    st = subtree_map_lookup(map, child_path, &child_entry);
    if (st != MDS_OK) {
        return st;
}

    /* Verify child is under parent. */
    plen = strlen(parent_path);
    if (strcmp(parent_path, "/") != 0) {
        if (strncmp(child_path, parent_path, plen) != 0 ||
            child_path[plen] != '/') {
            return MDS_ERR_INVAL;
}
    }

    /*
     * Remote merge (child on different MDS than parent) is NOT
     * implemented.  A proper implementation would need to:
     *   1. Look up the child's fileid from child_path.
     *   2. Migrate data back from the child's current owner.
     *   3. Remove child from map after migration completes.
     * Until then, return MDS_ERR_INVAL for cross-MDS merge.
     */
    if (child_entry.owner_mds_id != parent_entry.owner_mds_id) {
        return MDS_ERR_INVAL;
}

    /* Remove child from map. */
    st = subtree_map_remove_subtree(map, child_path);
    return st;
}

/* -----------------------------------------------------------------------
 * Automatic split evaluator
 * ----------------------------------------------------------------------- */

struct split_evaluator {
    struct subtree_map                *map;
    struct mds_catalogue              *cat;
    struct mds_shard_map              *shard_map; /* Phase 3: for per-subtree resolution. */
    const struct migration_transport  *transport;
    struct split_eval_cfg              cfg;
    pthread_t                          thread;
    _Atomic bool                       running;
    _Atomic uint64_t                   proposal_counter;
    /* Proposal store -- protected by mtx. */
    pthread_mutex_t                    mtx;
    struct split_proposal              store[SPLIT_PROPOSAL_MAX];
    uint32_t                           store_count;
    _Atomic bool                       auto_exec;
};

/**
 * Find an existing proposal by path, or return NULL.
 * Caller must hold eval->mtx.
 */
static struct split_proposal *find_proposal(struct split_evaluator *eval,
                                            const char *path)
{
    for (uint32_t i = 0; i < eval->store_count; i++) {
        if (strcmp(eval->store[i].path, path) == 0) {
            return &eval->store[i];
        }
    }
    return NULL;
}

struct child_count_ctx {
    uint32_t count;
    uint32_t limit;
};

static int count_child_cb(const struct mds_cat_dirent *entry, void *arg)
{
    struct child_count_ctx *ctx = arg;

    (void)entry;
    if (ctx == NULL) {
        return 1;
    }
    ctx->count++;
    return (ctx->count >= ctx->limit) ? 1 : 0;
}

static bool subtree_meets_min_children(struct split_evaluator *eval,
                                       const char *path)
{
    uint64_t dir_fileid = 0;
    struct child_count_ctx ctx;
    enum mds_status st;
    uint32_t required;

    if (eval == NULL || path == NULL) {
        return false;
    }

    required = eval->cfg.min_children;
    if (required == 0) {
        return true;
    }
    if (eval->cat == NULL) {
        return false;
    }

    st = mds_cat_resolve_path(eval->cat, path, &dir_fileid);
    if (st != MDS_OK) {
        return false;
    }

    memset(&ctx, 0, sizeof(ctx));
    ctx.limit = required;
    st = mds_cat_ns_readdir(eval->cat, dir_fileid, NULL, 0, NULL,
                            count_child_cb, &ctx);
    if (st != MDS_OK) {
        return false;
    }
    return ctx.count >= required;
}

/**
 * Single evaluation pass: scan all local subtrees, identify any
 * with op_count > threshold, and upsert into the proposal store.
 *
 * Proposals require sustained_intervals consecutive hot intervals
 * before reaching PENDING state.  Subtrees in cooldown, migrating,
 * or at max depth are skipped.
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
void split_evaluator_run_once(struct split_evaluator *eval)
{
    struct subtree_map *map = eval->map;
    uint32_t self_id = subtree_map_self_id(map);
    uint64_t threshold = eval->cfg.split_threshold;
    uint32_t n = subtree_map_count(map);
    uint64_t now_sec = (uint64_t)time(NULL);


    for (uint32_t i = 0; i < n; i++) {
        struct subtree_entry entry;
        uint64_t ops = 0;
        enum mds_status st;

        st = subtree_map_get_entry(map, i, &entry);
        if (st != MDS_OK) {
            break;
        }
        if (entry.owner_mds_id != self_id) {
            continue;
        }
        /* Skip migrating/frozen subtrees. */
        if (entry.state != SUBTREE_ACTIVE) {
            continue;
        }

        st = subtree_map_get_stats(map, entry.path, &ops);
        if (st != MDS_OK) {
            continue;
        }
        /* Always reset counter after sampling. */
        subtree_map_reset_stats(map, entry.path);

        if (ops > threshold && !subtree_meets_min_children(eval, entry.path)) {
            continue;
        }

        pthread_mutex_lock(&eval->mtx);

        if (ops > threshold) {
            struct split_proposal *p = find_proposal(eval, entry.path);

            if (p != NULL) {
                uint32_t req = eval->cfg.sustained_intervals;

                if (req == 0) {
                    req = SPLIT_EVAL_DEFAULT_SUSTAINED;
                }

                /* Existing proposal -- update. */
                p->sampled_ops = ops;
                if (p->state == PROPOSAL_COOLDOWN &&
                    p->cooldown_expiry > 0) {
                    if (now_sec < p->cooldown_expiry) {
                        pthread_mutex_unlock(&eval->mtx);
                        continue;
                    }
                    /* Cooldown expired -- restart sustained tracking. */
                    p->cooldown_expiry = 0;
                    p->hot_intervals = 0;
                    p->first_seen_sec = now_sec;
                }

                p->hot_intervals++;
                /* Promote tracking entry to PENDING once
                 * sustained_intervals threshold is met. */
                if (p->state == PROPOSAL_COOLDOWN &&
                    p->cooldown_expiry == 0 &&
                    p->hot_intervals >= req) {
                    p->state = PROPOSAL_PENDING;
                    atomic_fetch_add_explicit(
                        &eval->proposal_counter, 1,
                        memory_order_relaxed);
                }
            } else if (eval->store_count < SPLIT_PROPOSAL_MAX) {
                /* New proposal -- start tracking. */
                p = &eval->store[eval->store_count++];
                memset(p, 0, sizeof(*p));
                (void)snprintf(p->path, sizeof(p->path),
                               "%s", entry.path);
                p->sampled_ops = ops;
                p->first_seen_sec = now_sec;
                p->hot_intervals = 1;
                /*
                 * Require sustained_intervals consecutive hot
                 * intervals before promoting to PENDING.
                 * Until then, track but don't propose.
                 */
                {
                    uint32_t req = eval->cfg.sustained_intervals;
                    if (req == 0) { req = SPLIT_EVAL_DEFAULT_SUSTAINED; }
                    p->state = (p->hot_intervals >= req)
                               ? PROPOSAL_PENDING : PROPOSAL_COOLDOWN;
                }
                if (p->state == PROPOSAL_PENDING) {
                    atomic_fetch_add_explicit(
                        &eval->proposal_counter, 1,
                        memory_order_relaxed);
                }
            }
        } else {
            struct split_proposal *p = find_proposal(eval, entry.path);

            if (p != NULL) {
                if (p->state == PROPOSAL_PENDING) {
                    /* Pending proposals keep advisory hotness decay. */
                    if (p->hot_intervals > 0) {
                        p->hot_intervals--;
                    }
                } else if (p->state == PROPOSAL_COOLDOWN &&
                           p->cooldown_expiry == 0) {
                    /* Pre-proposal tracking requires consecutive heat. */
                    p->hot_intervals = 0;
                }
            }
        }
        pthread_mutex_unlock(&eval->mtx);
    }
}

static void *eval_thread_func(void *arg)
{
    struct split_evaluator *eval = (struct split_evaluator *)arg;
    uint32_t interval = eval->cfg.eval_interval_sec;

    while (atomic_load_explicit(&eval->running,
                                memory_order_relaxed)) {
        /* Sleep in 1-second increments so stop() returns
         * within ~1s instead of waiting up to interval. */
        for (uint32_t s = 0; s < interval; s++) {
            if (!atomic_load_explicit(&eval->running,
                                      memory_order_relaxed)) {
                return NULL;
            }
            /* NOLINTNEXTLINE(concurrency-mt-unsafe) */
            sleep(1);
        }
        if (!atomic_load_explicit(&eval->running,
                                  memory_order_relaxed)) {
            break;
        }
        split_evaluator_run_once(eval);
    }
    return NULL;
}

int split_evaluator_start(struct subtree_map *map,
                          struct mds_catalogue *cat,
                          const struct migration_transport *transport,
                          const struct split_eval_cfg *cfg,
                          struct split_evaluator **out)
{
    struct split_evaluator *eval;

    if (map == NULL || cfg == NULL || out == NULL) {
        return -1;
}

    if (!cfg->auto_split_enabled) {
        *out = NULL;
        return 0;  /* Not an error -- just disabled. */
    }

    eval = calloc(1, sizeof(*eval));
    if (eval == NULL) {
        return -1;
}

    eval->map       = map;
    eval->cat       = cat;
    eval->shard_map = NULL; /* Set later via cluster_transport wiring. */
    eval->transport = transport;
    eval->cfg       = *cfg;
    atomic_store_explicit(&eval->running, true, memory_order_relaxed);
    atomic_store_explicit(&eval->proposal_counter, 0, memory_order_relaxed);
    atomic_store_explicit(&eval->auto_exec, cfg->auto_execute,
                          memory_order_relaxed);
    pthread_mutex_init(&eval->mtx, NULL);
    eval->store_count = 0;

    if (pthread_create(&eval->thread, NULL, eval_thread_func, eval) != 0) {
        free(eval);
        return -1;
    }

    *out = eval;
    return 0;
}

void split_evaluator_stop(struct split_evaluator *eval)
{
    if (eval == NULL) {
        return;
}

    atomic_store_explicit(&eval->running, false, memory_order_relaxed);
    pthread_join(eval->thread, NULL);
    pthread_mutex_destroy(&eval->mtx);
    free(eval);
}

uint64_t split_evaluator_proposal_count(const struct split_evaluator *eval)
{
    if (eval == NULL) {
        return 0;
}
    return atomic_load_explicit(
        (_Atomic uint64_t *)&eval->proposal_counter,
        memory_order_relaxed);
}

int split_evaluator_list_proposals(const struct split_evaluator *eval,
                                   struct split_proposal **out,
                                   uint32_t *count)
{
    if (out == NULL || count == NULL) {
        return -1;
    }
    if (eval == NULL || eval->store_count == 0) {
        *out = NULL;
        *count = 0;
        return 0;
    }

    pthread_mutex_lock((pthread_mutex_t *)&eval->mtx);
    uint32_t n = eval->store_count;
    struct split_proposal *arr = calloc(n, sizeof(*arr));
    if (arr == NULL) {
        pthread_mutex_unlock((pthread_mutex_t *)&eval->mtx);
        return -1;
    }
    memcpy(arr, eval->store, n * sizeof(*arr));
    pthread_mutex_unlock((pthread_mutex_t *)&eval->mtx);

    *out = arr;
    *count = n;
    return 0;
}

int split_evaluator_approve(struct split_evaluator *eval,
                            const char *path)
{
    if (eval == NULL || path == NULL) {
        return -1;
    }
    int rc = -1;

    pthread_mutex_lock(&eval->mtx);
    struct split_proposal *p = find_proposal(eval, path);
    if (p != NULL && p->state == PROPOSAL_PENDING) {
        p->state = PROPOSAL_APPROVED;
        rc = 0;
    }
    pthread_mutex_unlock(&eval->mtx);
    return rc;
}

int split_evaluator_reject(struct split_evaluator *eval,
                           const char *path,
                           const char *reason)
{
    if (eval == NULL || path == NULL) {
        return -1;
    }
    int rc = -1;

    pthread_mutex_lock(&eval->mtx);
    struct split_proposal *p = find_proposal(eval, path);
    if (p != NULL && p->state == PROPOSAL_PENDING) {
        p->state = PROPOSAL_REJECTED;
        if (reason != NULL) {
            (void)snprintf(p->reject_reason,
                           sizeof(p->reject_reason), "%s", reason);
        }
        rc = 0;
    }
    pthread_mutex_unlock(&eval->mtx);
    return rc;
}

void split_evaluator_set_auto_execute(struct split_evaluator *eval,
                                      bool enable)
{
    if (eval == NULL) {
        return;
    }
    atomic_store_explicit(&eval->auto_exec, enable,
                          memory_order_relaxed);
}

bool split_evaluator_get_auto_execute(const struct split_evaluator *eval)
{
    if (eval == NULL) {
        return false;
    }
    return atomic_load_explicit(
        (_Atomic bool *)&eval->auto_exec,
        memory_order_relaxed);
}

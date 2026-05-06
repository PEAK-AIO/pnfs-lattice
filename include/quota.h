/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * quota.h -- User/Group quota enforcement (architecture.md S4.5 V1).
 *
 * V1 scope: per-UID and per-GID quotas.  Directory (subtree) quotas
 * are deferred to V2 (hard-link 1:N and rename subtree rescan).
 */

#ifndef QUOTA_H
#define QUOTA_H

#include <stdint.h>

#include "pnfs_mds.h"

/* Forward declarations. */
struct mds_catalogue;

/* -----------------------------------------------------------------------
 * Quota scope types (catalogue key prefix byte)
 * ----------------------------------------------------------------------- */

#define MDS_QUOTA_USER   'U'   /**< User quota rule key prefix. */
#define MDS_QUOTA_GROUP  'G'   /**< Group quota rule key prefix. */
/* V2 will add MDS_QUOTA_DIR 'D'. */

/* Usage key prefix (lowercase of rule prefix). */
#define MDS_QUOTA_USER_USAGE   'u'
#define MDS_QUOTA_GROUP_USAGE  'g'

/* Default grace period: 7 days in seconds. */
#define MDS_QUOTA_DEFAULT_GRACE_SEC  (7U * 24U * 3600U)

/* -----------------------------------------------------------------------
 * Quota rule -- persisted in catalogue (40 bytes)
 * ----------------------------------------------------------------------- */

struct mds_quota_rule {
    uint64_t hard_bytes;
    uint64_t soft_bytes;
    uint64_t hard_inodes;
    uint64_t soft_inodes;
    uint32_t grace_sec;
    uint32_t _pad;
};

/* -----------------------------------------------------------------------
 * Quota usage -- persisted in catalogue (32 bytes)
 * ----------------------------------------------------------------------- */

struct mds_quota_usage {
    uint64_t used_bytes;
    uint64_t used_inodes;
    int64_t  grace_start_bytes;   /**< 0 = not in grace. */
    int64_t  grace_start_inodes;  /**< 0 = not in grace. */
};

/* -----------------------------------------------------------------------
 * Quota context (stateless -- wraps catalogue handle)
 * ----------------------------------------------------------------------- */

struct mds_quota_ctx;

/**
 * Create a quota enforcement context.
 *
 * @param cat  Catalogue handle.
 * @param out  Receives the new context.
 * @return MDS_OK or MDS_ERR_NOMEM.
 */
enum mds_status mds_quota_ctx_create(const struct mds_catalogue *cat,
                                     struct mds_quota_ctx **out);

/** Destroy a quota context. NULL is tolerated. */
void mds_quota_ctx_destroy(struct mds_quota_ctx *ctx);

/* -----------------------------------------------------------------------
 * Read-only checks (use their own catalogue read transactions)
 * ----------------------------------------------------------------------- */

/**
 * Check whether creating one more inode is allowed for uid+gid.
 *
 * @return MDS_OK or MDS_ERR_NOSPC if any hard limit is hit (or soft
 *         limit with expired grace).
 */
enum mds_status mds_quota_check_inode(struct mds_quota_ctx *ctx,
                                      uint64_t uid, uint64_t gid);

/**
 * Check whether adding @delta_bytes is allowed for uid+gid.
 *
 * @return MDS_OK or MDS_ERR_NOSPC.
 */
enum mds_status mds_quota_check_bytes(struct mds_quota_ctx *ctx,
                                      uint64_t uid, uint64_t gid,
                                      uint64_t delta_bytes);

/* -----------------------------------------------------------------------
 * Usage updates (must be called inside a write transaction)
 * ----------------------------------------------------------------------- */

/** Increment inode count for uid and gid by 1. */
enum mds_status mds_quota_update_create(struct mds_quota_ctx *ctx,
                                        uint64_t uid, uint64_t gid);

/** Decrement inode count by 1 and bytes by @file_size for uid+gid. */
enum mds_status mds_quota_update_remove(struct mds_quota_ctx *ctx,
                                        uint64_t uid, uint64_t gid,
                                        uint64_t file_size);

/** Adjust byte usage: add (new_size - old_size) for uid+gid. */
enum mds_status mds_quota_update_bytes(struct mds_quota_ctx *ctx,
                                       uint64_t uid, uint64_t gid,
                                       uint64_t old_size,
                                       uint64_t new_size);

/**
 * Transfer one inode + file_size bytes from old owner to new owner.
 *
 * Called on chown/chgrp.
 */
enum mds_status mds_quota_update_chown(struct mds_quota_ctx *ctx,
                                       uint64_t old_uid, uint64_t old_gid,
                                       uint64_t new_uid, uint64_t new_gid,
                                       uint64_t file_size);

/* -----------------------------------------------------------------------
 * Admin operations (own transactions)
 * ----------------------------------------------------------------------- */

/** Set or update a quota rule for scope_type + scope_id. */
enum mds_status mds_quota_set(struct mds_quota_ctx *ctx,
                              uint8_t scope_type, uint64_t scope_id,
                              const struct mds_quota_rule *rule);

/**
 * Read a quota rule and its usage counters.
 *
 * Either @rule or @usage may be NULL if not needed.
 *
 * @return MDS_OK, MDS_ERR_NOTFOUND, or MDS_ERR_IO.
 */
enum mds_status mds_quota_get(struct mds_quota_ctx *ctx,
                              uint8_t scope_type, uint64_t scope_id,
                              struct mds_quota_rule *rule,
                              struct mds_quota_usage *usage);

/** Remove a quota rule. */
enum mds_status mds_quota_del(struct mds_quota_ctx *ctx,
                              uint8_t scope_type, uint64_t scope_id);

/* -----------------------------------------------------------------------
 * GETATTR space reporting
 * ----------------------------------------------------------------------- */

/**
 * Compute quota-adjusted SPACE_AVAIL / SPACE_FREE / SPACE_TOTAL.
 *
 * Returns the minimum remaining across user and group quotas.
 * If no quota is set for uid or gid, that scope is unlimited.
 *
 * @param avail  Receives available bytes (total - used).
 * @param free   Receives free bytes (same as avail for V1).
 * @param total  Receives total quota bytes.
 * @return MDS_OK (always succeeds; unlimited -> UINT64_MAX).
 */
enum mds_status mds_quota_space_avail(struct mds_quota_ctx *ctx,
                                      uint64_t uid, uint64_t gid,
                                      uint64_t *avail,
                                      uint64_t *free_bytes,
                                      uint64_t *total);

#endif /* QUOTA_H */

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * quota_stub.c — no-op stubs for the user/group quota enforcement
 * module.
 *
 * Linked into pnfs_mds_core when ENABLE_QUOTA=OFF (community build).
 * The real implementation is in quota.c in this same directory.
 *
 * Semantics: quota enforcement is disabled.
 *   - ctx_create() returns MDS_OK with a NULL handle so callers'
 *     null-checks short-circuit if they bother to check; if they
 *     don't, every subsequent quota_* call is a no-op anyway.
 *   - check_inode() / check_bytes() return MDS_OK — every CREATE,
 *     WRITE, and SETATTR is allowed.
 *   - update_create() / update_remove() / update_bytes() /
 *     update_chown() return MDS_OK — usage counters are not
 *     tracked.
 *   - get() returns MDS_ERR_NOTFOUND — no rules exist.
 *   - set() / del() return MDS_OK so admin tooling that probes
 *     quota state on a community build does not error out; the
 *     rule is silently ignored.
 *   - space_avail() returns UINT64_MAX for all scopes — every
 *     GETATTR sees unlimited space available.
 *
 * The community daemon therefore behaves like an unbounded server:
 * no per-user or per-group limits, no usage accounting.  Operators
 * who need quotas use the enterprise build.
 */
#include "quota.h"

#include <stddef.h>
#include <stdint.h>

enum mds_status mds_quota_ctx_create(const struct mds_catalogue *cat,
                                     struct mds_quota_ctx **out)
{
    (void)cat;
    if (out) {
        *out = NULL;
    }
    return MDS_OK;
}

void mds_quota_ctx_destroy(struct mds_quota_ctx *ctx)
{
    (void)ctx;
}

enum mds_status mds_quota_check_inode(struct mds_quota_ctx *ctx,
                                      uint64_t uid, uint64_t gid)
{
    (void)ctx;
    (void)uid;
    (void)gid;
    return MDS_OK;
}

enum mds_status mds_quota_check_bytes(struct mds_quota_ctx *ctx,
                                      uint64_t uid, uint64_t gid,
                                      uint64_t delta_bytes)
{
    (void)ctx;
    (void)uid;
    (void)gid;
    (void)delta_bytes;
    return MDS_OK;
}

enum mds_status mds_quota_update_create(struct mds_quota_ctx *ctx,
                                        uint64_t uid, uint64_t gid)
{
    (void)ctx;
    (void)uid;
    (void)gid;
    return MDS_OK;
}

enum mds_status mds_quota_update_remove(struct mds_quota_ctx *ctx,
                                        uint64_t uid, uint64_t gid,
                                        uint64_t file_size)
{
    (void)ctx;
    (void)uid;
    (void)gid;
    (void)file_size;
    return MDS_OK;
}

enum mds_status mds_quota_update_bytes(struct mds_quota_ctx *ctx,
                                       uint64_t uid, uint64_t gid,
                                       uint64_t old_size,
                                       uint64_t new_size)
{
    (void)ctx;
    (void)uid;
    (void)gid;
    (void)old_size;
    (void)new_size;
    return MDS_OK;
}

enum mds_status mds_quota_update_chown(struct mds_quota_ctx *ctx,
                                       uint64_t old_uid, uint64_t old_gid,
                                       uint64_t new_uid, uint64_t new_gid,
                                       uint64_t file_size)
{
    (void)ctx;
    (void)old_uid;
    (void)old_gid;
    (void)new_uid;
    (void)new_gid;
    (void)file_size;
    return MDS_OK;
}

enum mds_status mds_quota_set(struct mds_quota_ctx *ctx,
                              uint8_t scope_type, uint64_t scope_id,
                              const struct mds_quota_rule *rule)
{
    (void)ctx;
    (void)scope_type;
    (void)scope_id;
    (void)rule;
    return MDS_OK;
}

enum mds_status mds_quota_get(struct mds_quota_ctx *ctx,
                              uint8_t scope_type, uint64_t scope_id,
                              struct mds_quota_rule *rule,
                              struct mds_quota_usage *usage)
{
    (void)ctx;
    (void)scope_type;
    (void)scope_id;
    (void)rule;
    (void)usage;
    return MDS_ERR_NOTFOUND;
}

enum mds_status mds_quota_del(struct mds_quota_ctx *ctx,
                              uint8_t scope_type, uint64_t scope_id)
{
    (void)ctx;
    (void)scope_type;
    (void)scope_id;
    return MDS_OK;
}

enum mds_status mds_quota_space_avail(struct mds_quota_ctx *ctx,
                                      uint64_t uid, uint64_t gid,
                                      uint64_t *avail,
                                      uint64_t *free_bytes,
                                      uint64_t *total)
{
    (void)ctx;
    (void)uid;
    (void)gid;

    if (avail) {
        *avail = UINT64_MAX;
    }
    if (free_bytes) {
        *free_bytes = UINT64_MAX;
    }
    if (total) {
        *total = UINT64_MAX;
    }
    return MDS_OK;
}

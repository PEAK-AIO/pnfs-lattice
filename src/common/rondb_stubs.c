/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * rondb_stubs.c -- RonDB-specific fallback implementations for non-RonDB builds.
 */

#include "catalogue_rondb.h"

/* NOLINTNEXTLINE(readability-non-const-parameter) */
enum mds_status catalogue_rondb_layoutget_fused(
    struct mds_catalogue *cat, uint64_t fileid,
    uint32_t *stripe_count, uint32_t *stripe_unit, /* NOLINT */
    uint32_t *mirror_count, struct mds_ds_map_entry **entries, /* NOLINT */
    const struct nfs4_stateid *stateid,
    uint64_t clientid, uint32_t iomode, uint64_t offset,
    uint64_t length, uint32_t mds_id)
{
    (void)cat;
    (void)fileid;
    (void)stripe_count;
    (void)stripe_unit;
    (void)mirror_count;
    (void)entries;
    (void)stateid;
    (void)clientid;
    (void)iomode;
    (void)offset;
    (void)length;
    (void)mds_id;

    return MDS_ERR_NOSUPPORT;
}

enum mds_status catalogue_rondb_ns_create_with_layout(
    struct mds_catalogue *cat,
    uint64_t parent_fileid, const char *name,
    enum mds_file_type type,
    uint32_t mode, uint64_t uid, uint64_t gid,
    struct ds_prealloc_ctx *prealloc,
    struct mds_inode *out,
    uint64_t layout_clientid, uint32_t layout_iomode,
    uint64_t layout_offset, uint64_t layout_length,
    const struct nfs4_stateid *layout_stateid,
    uint32_t layout_mds_id,
    bool *layout_ok)
{
    (void)cat;
    (void)parent_fileid;
    (void)name;
    (void)type;
    (void)mode;
    (void)uid;
    (void)gid;
    (void)prealloc;
    (void)out;
    (void)layout_clientid;
    (void)layout_iomode;
    (void)layout_offset;
    (void)layout_length;
    (void)layout_stateid;
    (void)layout_mds_id;

    if (layout_ok != NULL) {
        *layout_ok = false;
    }

    return MDS_ERR_NOSUPPORT;
}

/* NOLINTNEXTLINE(readability-non-const-parameter) */
int rondb_shim_fileid_batch_alloc(void *handle, uint32_t batch_size,
                                  uint64_t *out_base, uint32_t *out_count)
{
    (void)handle;
    (void)batch_size;
    (void)out_base;
    (void)out_count;

    return -1;
}

/* NOLINTNEXTLINE(readability-non-const-parameter) */
int rondb_shim_bench_create(void *handle, uint32_t n_ops,
                            uint64_t parent_fileid, uint64_t base_fileid,
                            uint64_t *elapsed_us, uint32_t *errors)
{
    (void)handle;
    (void)n_ops;
    (void)parent_fileid;
    (void)base_fileid;
    (void)elapsed_us;
    (void)errors;

    return -1;
}

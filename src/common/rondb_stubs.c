/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * rondb_stubs.c -- RonDB-specific fallback implementations for non-RonDB builds.
 *
 * Only the raw shim entry points that RonDB-specific tools link against
 * live here.  The fused catalogue operations are vtable slots reached
 * through the backend-neutral dispatcher and need no stub.
 */

#include "catalogue_rondb.h"

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

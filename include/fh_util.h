/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * fh_util.h — Synthetic NFS file-handle helpers.
 */

#ifndef FH_UTIL_H
#define FH_UTIL_H

#include <stdint.h>
#include <stdio.h>
#include <inttypes.h>
#include <string.h>

#include "pnfs_mds.h"

/**
 * Build a synthetic DS file handle string into @p buf.
 *
 * Format: "<fileid>_<stripe_idx>_<mirror_idx>".
 *
 * @param buf        Destination buffer (at least MDS_NFS_FH_MAX bytes).
 * @param buf_size   Size of @p buf.
 * @param fileid     File identifier.
 * @param stripe_idx Stripe index.
 * @param mirror_idx Mirror index.
 * @return Length written (excluding NUL), or 0 on truncation/error.
 */
static inline uint32_t mds_synth_fh(char *buf, size_t buf_size,
                                    uint64_t fileid,
                                    uint32_t stripe_idx,
                                    uint32_t mirror_idx)
{
    int n = snprintf(buf, buf_size, "%" PRIu64 "_%u_%u",
                     fileid, stripe_idx, mirror_idx);
    if (n <= 0 || (size_t)n >= buf_size) {
        return 0;
    }
    return (uint32_t)n;
}

/**
 * Build a synthetic FH and write it into an mds_ds_map_entry.
 *
 * @param entry      Target entry to update.
 * @param fileid     File identifier.
 * @param stripe_idx Stripe index.
 * @param mirror_idx Mirror index.
 */
static inline void mds_synth_fh_into_entry(struct mds_ds_map_entry *entry,
                                           uint64_t fileid,
                                           uint32_t stripe_idx,
                                           uint32_t mirror_idx)
{
    char fh_buf[MDS_NFS_FH_MAX];
    uint32_t fh_len = mds_synth_fh(fh_buf, sizeof(fh_buf),
                                   fileid, stripe_idx, mirror_idx);
    if (fh_len > 0) {
        memcpy(entry->nfs_fh, fh_buf, fh_len);
        entry->nfs_fh_len = fh_len;
    }
}

#endif /* FH_UTIL_H */

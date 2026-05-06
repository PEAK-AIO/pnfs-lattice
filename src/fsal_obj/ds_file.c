/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_file.c -- Data server file creation and file handle capture.
 *
 * When a new file is created, the MDS pre-creates empty objects on
 * the target DS nodes via its private NFS mounts and captures the
 * resulting DS file handles for inclusion in layouts.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "pnfs_mds.h"

__attribute__((unused))
static int ds_file_create(const struct mds_ds_map_entry *ds, uint64_t fileid,
                   /* NOLINTNEXTLINE(readability-non-const-parameter) */
                   uint8_t *ds_fh, uint32_t *ds_fh_len)
{
    /* TODO: implement
     * 1. Build DS-local filename: "<fileid_hex>"
     * 2. NFS CREATE on DS private mount
     * 3. NFS GETFH on the created file
     * 4. Copy file handle to ds_fh, set ds_fh_len
     */
    (void)ds;
    (void)fileid;
    (void)ds_fh;
    (void)ds_fh_len;
    return -1;
}

__attribute__((unused))
static int ds_file_remove(const struct mds_ds_map_entry *ds,
                   const uint8_t *ds_fh, uint32_t ds_fh_len)
{
    /* TODO: implement
     * Called when a file is deleted from MDS -- remove DS object.
     * May be deferred / batched for efficiency.
     */
    (void)ds;
    (void)ds_fh;
    (void)ds_fh_len;
    return -1;
}

__attribute__((unused))
static int ds_mount_all(const struct mds_config *cfg)
{
    /* TODO: implement
     * For each DS in cfg->ds_list:
     *   mount -t nfs4 <ds_host>:<ds_export> <local_mountpoint>
     * Store mount descriptors for later use.
     */
    (void)cfg;
    return -1;
}

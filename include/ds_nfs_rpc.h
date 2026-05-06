/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_nfs_rpc.h -- Minimal NFS3 RPC client for DS file handle capture.
 *
 * The MDS needs server-side NFS file handles to include in pNFS
 * layouts.  The standard approach is to MOUNT the DS export via
 * RPC to obtain the root FH, then LOOKUP each path component to
 * resolve the data file's FH.  This avoids the broken
 * name_to_handle_at() path (EINVAL on NFS client mounts).
 *
 * All functions create a temporary TCP connection, perform the
 * RPC exchange, and close the socket.  Thread-safe; signals are
 * blocked during socket operations to prevent NDB EINTR
 * interference.
 *
 * NOTE: This is the fallback FH capture path.  The primary path
 * uses name_to_handle_at() via the local NFS mount (see proxy_io.c).
 * This RPC path is used when the DS is not mounted on the MDS or
 * when name_to_handle_at() fails.
 */

#ifndef DS_NFS_RPC_H
#define DS_NFS_RPC_H

#include <stdint.h>

/**
 * @brief Obtain the NFS3 server file handle for a DS data file.
 *
 * Connects to the DS NFS server, performs MOUNT3 to get the export
 * root FH, then LOOKUP for each component of @a rel_path (e.g.
 * "data/101_0_0").  If the final component does not exist and
 * @a create is true, issues NFS3 CREATE to make the file.
 *
 * The returned FH is the raw NFSv3 server handle suitable for
 * inclusion in a flex-file layout (ff_data_server4.ffl_fh_list).
 *
 * @param host         DS hostname or IP address.
 * @param port         DS NFS port (typically 2049).
 * @param export_path  DS export path (e.g. "/export/ds").
 * @param rel_path     File path relative to export root (e.g. "data/101_0_0").
 * @param create       If true, create the file when LOOKUP returns NOENT.
 * @param fh_out       Receives the server file handle bytes.
 * @param fh_len       In: capacity of fh_out.  Out: actual FH length.
 * @param timeout_ms   TCP + RPC timeout in milliseconds.
 * @return 0 on success, -1 on error.
 */
int ds_nfs3_lookup_fh(const char *host, uint16_t port,
                      const char *export_path,
                      const char *rel_path,
                      int create,
                      uint8_t *fh_out, uint32_t *fh_len,
                      uint32_t timeout_ms);

#endif /* DS_NFS_RPC_H */

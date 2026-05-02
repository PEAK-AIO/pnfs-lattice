/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * proxy_io.h — Non-pNFS client proxy I/O API.
 *
 * Provides stripe-aware READ and mirror-aware WRITE through the MDS
 * for clients that do not support pNFS (no LAYOUTGET).  The MDS
 * forwards I/O to DS data files via its private NFS mounts (or any
 * local mount path in testing).
 *
 * See docs/architecture.md §14 for design overview.
 */

#ifndef PROXY_IO_H
#define PROXY_IO_H

#include <stdint.h>
#include <stdbool.h>

#include "pnfs_mds.h"

/* Forward declarations. */
struct mds_catalogue;

/* Maximum number of DS mount registrations. */
#define MDS_PROXY_MAX_DS 256

/* Opaque proxy context. */
struct mds_proxy_ctx;

/* -----------------------------------------------------------------------
 * Context lifecycle
 * ----------------------------------------------------------------------- */

/**
 * Create a proxy I/O context.
 *
 * @param out  Receives the new context.  Caller owns it.
 * @return MDS_OK on success, MDS_ERR_NOMEM on allocation failure.
 */
enum mds_status mds_proxy_ctx_create(struct mds_proxy_ctx **out);

/**
 * Destroy a proxy I/O context and free all resources.
 *
 * @param ctx  Context from mds_proxy_ctx_create().  NULL is tolerated.
 */
void mds_proxy_ctx_destroy(struct mds_proxy_ctx *ctx);

/* -----------------------------------------------------------------------
 * DS mount registration
 * ----------------------------------------------------------------------- */

/**
 * Register a DS mount path for proxy I/O.
 *
 * The proxy uses this mapping to locate DS data files on the local
 * filesystem.  Data files are expected at: {mount_path}/data/{fileid}_{stripe}_{mirror}
 *
 * @param ctx        Proxy context.
 * @param ds_id      Data server ID (must match ds_registry / stripe_map).
 * @param mount_path Local path where the DS export is mounted.
 * @return MDS_OK, MDS_ERR_INVAL (ds_id too large), MDS_ERR_NOSPC (table full).
 */
enum mds_status mds_proxy_mount_set(struct mds_proxy_ctx *ctx,
                                    uint32_t ds_id,
                                    const char *mount_path);
/**
 * Store DS network info for NFS3 RPC file handle capture.
 *
 * Called after mds_proxy_mount_set() to associate the DS's
 * host/port/export with its ds_id.  Used by
 * mds_proxy_ensure_ds_file_fh() to obtain server FHs via
 * direct NFS3 MOUNT + LOOKUP RPCs (bypassing proxy mounts).
 */
void mds_proxy_mount_set_ds_info(struct mds_proxy_ctx *ctx,
                                 uint32_t ds_id,
                                 const char *host,
                                 uint16_t nfs_port,
                                 const char *export_path);

/**
 * @brief Clear (unregister) the mount entry for a given DS.
 *
 * After this call, proxy I/O will no longer route to ds_id.
 *
 * @param ctx    Proxy context.
 * @param ds_id  Data server to unmap.
 */
void mds_proxy_mount_clear(struct mds_proxy_ctx *ctx, uint32_t ds_id);


/**
 * Check whether a DS has a registered mount in the proxy context.
 *
 * @param ctx    Proxy context.
 * @param ds_id  Data server ID.
 * @return true if mounted, false otherwise.
 */
bool mds_proxy_is_mounted(const struct mds_proxy_ctx *ctx, uint32_t ds_id);


struct mds_catalogue;

/* -----------------------------------------------------------------------
 * Proxy READ / WRITE
 * ----------------------------------------------------------------------- */

/**
 * Proxy READ: read file data via DS mount.
 *
 * Looks up the stripe_map for @fileid, computes which stripe holds
 * the requested offset, opens the DS data file, and reads via pread().
 *
 * Single-stripe-unit reads only (offset+count must not span multiple
 * stripes in this implementation).
 *
 * @param ctx         Proxy context with DS mounts registered.
 * @param cat         Catalogue handle (for stripe_map lookup).
 * @param fileid      File to read from.
 * @param offset      Byte offset into the logical file.
 * @param count       Maximum bytes to read.
 * @param buf         Output buffer (must hold @count bytes).
 * @param bytes_read  Receives actual bytes read.
 * @param eof         Receives true if read reached end of DS file.
 * @return MDS_OK on success (including partial/eof reads),
 *         MDS_ERR_NOTFOUND (no stripe_map),
 *         MDS_ERR_IO (DS file open/read failure).
 */
enum mds_status mds_proxy_read(const struct mds_proxy_ctx *ctx,
                               struct mds_catalogue *cat,
                               uint64_t fileid,
                               uint64_t offset,
                               uint32_t count,
                               void *buf,
                               uint32_t *bytes_read,
                               bool *eof);

/**
 * Proxy WRITE: write file data to DS mount(s).
 *
 * Looks up the stripe_map for @fileid, computes which stripe holds
 * the requested offset, opens the DS data file for **each mirror**
 * of that stripe, and writes via pwrite().
 *
 * Single-stripe-unit writes only.
 *
 * @param ctx            Proxy context with DS mounts registered.
 * @param cat            Catalogue handle (for stripe_map lookup).
 * @param fileid         File to write to.
 * @param offset         Byte offset into the logical file.
 * @param data           Data to write.
 * @param data_len       Number of bytes to write.
 * @param bytes_written  Receives actual bytes written.
 * @return MDS_OK on success,
 *         MDS_ERR_NOTFOUND (no stripe_map),
 *         MDS_ERR_IO (DS file open/write failure).
 */
enum mds_status mds_proxy_write(const struct mds_proxy_ctx *ctx,
                                struct mds_catalogue *cat,
                                uint64_t fileid,
                                uint64_t offset,
                                const void *data,
                                uint32_t data_len,
                                uint32_t *bytes_written);

/**
 * Ensure a DS data file exists (create if absent).
 *
 * Used by the MDS when creating stripe_map entries so that proxy I/O
 * has a backing file to read/write.  Creates intermediate "data/"
 * directory if needed.
 *
 * @param ctx      Proxy context.
 * @param ds_id    Data server ID.
 * @param fileid   Logical file ID.
 * @param stripe   Stripe index.
 * @param mirror   Mirror index.
 * @return MDS_OK, MDS_ERR_NOTFOUND (ds_id not mounted), MDS_ERR_IO.
 */
enum mds_status mds_proxy_ensure_ds_file(const struct mds_proxy_ctx *ctx,
                                         uint32_t ds_id,
                                         uint64_t fileid,
                                         uint32_t stripe,
                                         uint32_t mirror);

/**
 * Unlink a DS data file via the local NFS3 mount.
 *
 * Used by the GC worker (ds_gc) to reclaim DS-side bytes after a
 * client REMOVE has dropped the catalogue inode.  Treats ENOENT as
 * success (idempotent: a previous attempt may already have unlinked
 * the file, or the DS may have been wiped by an operator).
 *
 * @param ctx          Proxy context.
 * @param ds_id        Data server ID.
 * @param fileid       Logical file ID.
 * @param stripe       Stripe index.
 * @param mirror       Mirror index.
 * @param[out] existed Optional.  When non-NULL, set to true when the
 *                     file was actually present and unlinked, false
 *                     when it was already gone (ENOENT).  Lets the
 *                     GC worker short-circuit its (s, m) sweep once
 *                     it has walked off the end of the layout, so a
 *                     1×1 file does not pay the full 64-unlink probe
 *                     cost.
 * @return MDS_OK on success or ENOENT,
 *         MDS_ERR_NOTFOUND if the DS is not mounted on the MDS,
 *         MDS_ERR_IO on any other unlink failure (transient,
 *         worker should retry).
 */
enum mds_status mds_proxy_unlink_ds_file(const struct mds_proxy_ctx *ctx,
                                         uint32_t ds_id,
                                         uint64_t fileid,
                                         uint32_t stripe,
                                         uint32_t mirror,
                                         bool *existed);

/**
 * Ensure DS data file exists and return its NFS file handle.
 *
 * Primary: creates the file via local NFS mount, then uses
 * name_to_handle_at() syscall to extract the server FH (~0.01ms).
 * Fallback: NFS3 MOUNT + LOOKUP RPC to the DS (~2-5ms) when the
 * syscall path fails (DS not mounted, kernel incompatibility).
 */
enum mds_status mds_proxy_ensure_ds_file_fh(
    const struct mds_proxy_ctx *ctx,
    uint32_t ds_id, uint64_t fileid,
    uint32_t stripe, uint32_t mirror,
    uint8_t *fh_out, uint32_t *fh_len);


/**
 * Write data directly to a DS file (no stripe-map lookup).
 *
 * Used during inline-to-DS promotion where the stripe map does not
 * yet exist in the catalogue.
 *
 * @return MDS_OK, MDS_ERR_NOTFOUND (ds_id not mounted), MDS_ERR_IO.
 */
enum mds_status mds_proxy_write_direct(const struct mds_proxy_ctx *ctx,
                                        uint32_t ds_id,
                                        uint64_t fileid,
                                        uint32_t stripe,
                                        uint32_t mirror,
                                        uint64_t offset,
                                        const void *data,
                                        uint32_t data_len);

/**
 * Read data directly from a DS file (no stripe-map lookup).
 *
 * Symmetric with mds_proxy_write_direct().  Offset is DS-file-local.
 *
 * @param bytes_read  Receives actual bytes read.
 * @param eof         Receives true if read reached end of file.
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_IO.
 */
enum mds_status mds_proxy_read_direct(const struct mds_proxy_ctx *ctx,
                                       uint32_t ds_id,
                                       uint64_t fileid,
                                       uint32_t stripe,
                                       uint32_t mirror,
                                       uint64_t offset,
                                       void *buf,
                                       uint32_t count,
                                       uint32_t *bytes_read,
                                       bool *eof);

/**
 * Seek on a DS file directly (no stripe-map lookup).
 *
 * @param what        0 = SEEK_DATA, 1 = SEEK_HOLE.
 * @param out_offset  Receives the found offset (DS-file-local).
 * @param eof         Receives true if beyond file end.
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_IO.
 */
enum mds_status mds_proxy_seek_direct(const struct mds_proxy_ctx *ctx,
                                       uint32_t ds_id,
                                       uint64_t fileid,
                                       uint32_t stripe,
                                       uint32_t mirror,
                                       uint64_t offset,
                                       uint32_t what,
                                       uint64_t *out_offset,
                                       bool *eof);

/**
 * Server-side copy between two DS files using copy_file_range().
 *
 * If both DS mounts are NFSv4.2, the kernel NFS client may perform
 * the copy entirely on the server side (inter-server COPY or intra-
 * server reflink).  Falls back to kernel-mediated read+write if
 * server-side copy is not supported.
 *
 * @param ctx           Proxy context.
 * @param src_ds_id     Source data server ID.
 * @param dst_ds_id     Destination data server ID.
 * @param fileid        Logical file ID.
 * @param stripe        Stripe index.
 * @param mirror        Mirror index.
 * @param size          Number of bytes to copy.
 * @param bytes_copied  Receives actual bytes copied.
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_IO.
 */
enum mds_status mds_proxy_copy_direct(const struct mds_proxy_ctx *ctx,
                                      uint32_t src_ds_id,
                                      uint32_t dst_ds_id,
                                      uint64_t fileid,
                                      uint32_t stripe,
                                      uint32_t mirror,
                                      uint64_t size,
                                      uint64_t *bytes_copied);

/**
 * Truncate a DS data file to zero length.
 *
 * Used before resilver copy to clear stale data from a prior
 * failed run or recovered DS.
 *
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_IO.
 */
enum mds_status mds_proxy_truncate_ds_file(const struct mds_proxy_ctx *ctx,
                                            uint32_t ds_id,
                                            uint64_t fileid,
                                            uint32_t stripe,
                                            uint32_t mirror);

/* -----------------------------------------------------------------------
 * NFSv4.2 proxy operations
 * ----------------------------------------------------------------------- */

/**
 * Proxy ALLOCATE: preallocate space on the DS data file.
 *
 * Calls posix_fallocate() on the DS file covering the given logical
 * file region.
 *
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_IO, MDS_ERR_NOSPC.
 */
enum mds_status mds_proxy_allocate(const struct mds_proxy_ctx *ctx,
                                   struct mds_catalogue *cat,
                                   uint64_t fileid,
                                   uint64_t offset,
                                   uint64_t length);

/**
 * Proxy DEALLOCATE: punch a hole in the DS data file.
 *
 * Calls fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE) on the
 * DS file.
 *
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_IO.
 */
enum mds_status mds_proxy_deallocate(const struct mds_proxy_ctx *ctx,
                                     struct mds_catalogue *cat,
                                     uint64_t fileid,
                                     uint64_t offset,
                                     uint64_t length);

/**
 * Proxy SEEK: find next DATA or HOLE boundary on the DS file.
 *
 * @param what       0 = SEEK_DATA, 1 = SEEK_HOLE.
 * @param out_offset Receives the found offset.
 * @param eof        Receives true if beyond file end.
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_IO.
 */
enum mds_status mds_proxy_seek(const struct mds_proxy_ctx *ctx,
                               struct mds_catalogue *cat,
                               uint64_t fileid,
                               uint64_t offset,
                               uint32_t what,
                               uint64_t *out_offset,
                               bool *eof);

/**
 * Proxy COPY: copy data between two files via DS mounts.
 *
 * Reads from src_fileid at src_offset, writes to dst_fileid at
 * dst_offset, up to @count bytes.  Operates in chunks.
 *
 * @param bytes_copied  Receives total bytes actually copied.
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_IO.
 */
enum mds_status mds_proxy_copy_data(const struct mds_proxy_ctx *ctx,
                                    struct mds_catalogue *cat,
                                    uint64_t src_fileid,
                                    uint64_t src_offset,
                                    uint64_t dst_fileid,
                                    uint64_t dst_offset,
                                    uint64_t count,
                                    uint64_t *bytes_copied);

/**
 * Proxy WRITE_SAME: write a repeating pattern to the DS file.
 *
 * Writes @pattern of @pattern_len bytes repeatedly to fill the
 * range [offset, offset+length) on the DS data file.
 *
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_IO.
 */
enum mds_status mds_proxy_write_same(const struct mds_proxy_ctx *ctx,
                                     struct mds_catalogue *cat,
                                     uint64_t fileid,
                                     uint64_t offset,
                                     uint64_t length,
                                     const void *pattern,
                                     uint32_t pattern_len);

/**
 * Proxy CLONE: clone file data range between DS files.
 *
 * Attempts ioctl(FICLONERANGE) for reflink; falls back to
 * read+write copy if not supported.
 *
 * @return MDS_OK, MDS_ERR_NOTFOUND, MDS_ERR_IO.
 */
enum mds_status mds_proxy_clone_range(struct mds_proxy_ctx *ctx,
                                      struct mds_catalogue *cat,
                                      uint64_t src_fileid,
                                      uint64_t src_offset,
                                      uint64_t dst_fileid,
                                      uint64_t dst_offset,
                                      uint64_t count);

/**
 * Set DS file ownership to synthetic uid/gid per RFC 8435 §2.2.1.
 *
 * Derives uid/gid from HMAC(secret, fileid||stripe||mirror) and
 * calls chown() on the DS data file.  Required for the loosely
 * coupled model so that the client's ffl_user/ffl_group credentials
 * pass the DS's permission check.
 *
 * @param ctx        Proxy context.
 * @param ds_id      Data server ID.
 * @param fileid     MDS file ID.
 * @param stripe     Stripe index.
 * @param mirror     Mirror index.
 * @param secret     DS secret key (32 bytes).
 * @param secret_len Length of secret.
 * @return MDS_OK on success.
 */
/** RFC 8435 §14: Fence client by chowning DS file to neutral uid. */
enum mds_status mds_proxy_fence_ds_file(const struct mds_proxy_ctx *ctx,
                                         uint32_t ds_id, uint64_t fileid,
                                         uint32_t stripe, uint32_t mirror);

enum mds_status mds_proxy_set_ds_owner(const struct mds_proxy_ctx *ctx,
                                        uint32_t ds_id, uint64_t fileid,
                                        uint32_t stripe, uint32_t mirror,
                                        const uint8_t *secret,
                                        uint32_t secret_len);

#endif /* PROXY_IO_H */

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalog_image.h — Materialized in-memory catalog image.
 *
 * The image is a read-only accelerator populated by replaying the
 * semantic journal (catalog_delta records).  It is NOT the source
 * of truth — authority is.
 *
 * Initial scope: inodes + dirents.  Stripe maps, xattr metadata,
 * DS registry snapshots can follow if profiling shows they matter.
 *
 * Thread safety: protected by an internal rwlock (Phase 9C).
 * Read operations (inode_get, dirent_get, readdir) acquire a
 * shared read lock.  Write operations (apply, inode_put/del,
 * dirent_put/del) acquire an exclusive write lock.
 * External callers may also use catalog_image_read_lock/unlock
 * and catalog_image_write_lock/unlock for multi-operation batches.
 */

#ifndef CATALOG_IMAGE_H
#define CATALOG_IMAGE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "pnfs_mds.h"

struct catalog_image;
struct catalog_delta_record;

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */

/**
 * Create an empty catalog image.
 * @param out  Receives the image handle.
 * @return 0 on success, -1 on allocation failure.
 */
int catalog_image_create(struct catalog_image **out);

/**
 * Destroy the image and free all resources.
 */
void catalog_image_destroy(struct catalog_image *img);

/* -----------------------------------------------------------------------
 * Inode operations
 * ----------------------------------------------------------------------- */

/**
 * Insert or update an inode in the image.
 *
 * @param img    Image handle.
 * @param inode  Inode to store (copied).
 * @return 0 on success, -1 on error.
 */
int catalog_image_inode_put(struct catalog_image *img,
                            const struct mds_inode *inode);

/**
 * Look up an inode by fileid.
 *
 * @param img     Image handle.
 * @param fileid  Inode to look up.
 * @param out     Receives the inode data.
 * @return 0 on success, 1 on not-found, -1 on error.
 */
int catalog_image_inode_get(const struct catalog_image *img,
                            uint64_t fileid,
                            struct mds_inode *out);

/**
 * Remove an inode from the image.
 *
 * @param img     Image handle.
 * @param fileid  Inode to remove.
 * @return 0 on success (or not-found), -1 on error.
 */
int catalog_image_inode_del(struct catalog_image *img,
                            uint64_t fileid);

/* -----------------------------------------------------------------------
 * Dirent operations
 * ----------------------------------------------------------------------- */

/**
 * Insert or update a directory entry.
 *
 * @param img      Image handle.
 * @param parent   Parent directory fileid.
 * @param name     Entry name (copied).
 * @param fileid   Child fileid.
 * @param type     Child file type (MDS_FTYPE_*).
 * @return 0 on success, -1 on error.
 */
int catalog_image_dirent_put(struct catalog_image *img,
                             uint64_t parent, const char *name,
                             uint64_t fileid, uint8_t type);

/**
 * Look up a directory entry by (parent, name).
 *
 * @param img      Image handle.
 * @param parent   Parent directory fileid.
 * @param name     Entry name.
 * @param fileid   Receives child fileid.
 * @param type     Receives child type.
 * @return 0 on success, 1 on not-found, -1 on error.
 */
int catalog_image_dirent_get(const struct catalog_image *img,
                             uint64_t parent, const char *name,
                             uint64_t *fileid, uint8_t *type);

/**
 * Remove a directory entry.
 *
 * @param img     Image handle.
 * @param parent  Parent directory fileid.
 * @param name    Entry name.
 * @return 0 on success (or not-found), -1 on error.
 */
int catalog_image_dirent_del(struct catalog_image *img,
                             uint64_t parent, const char *name);

/* -----------------------------------------------------------------------
 * READDIR
 * ----------------------------------------------------------------------- */

/** Readdir callback.  Return 0 to continue, non-zero to stop. */
typedef int (*catalog_image_readdir_cb)(uint64_t fileid, uint8_t type,
                                        const char *name, void *ctx);

/**
 * Iterate directory entries in sorted name order.
 *
 * If @a start_after is non-NULL, entries with name <= start_after
 * are skipped (for pagination).
 *
 * @param img          Image handle.
 * @param parent       Parent directory fileid.
 * @param start_after  Skip entries <= this name (NULL = from start).
 * @param cb           Per-entry callback.
 * @param ctx          Callback context.
 * @return 0 on success, -1 on error.
 */
int catalog_image_readdir(const struct catalog_image *img,
                          uint64_t parent, const char *start_after,
                          catalog_image_readdir_cb cb, void *ctx);

/* -----------------------------------------------------------------------
 * Replay
 * ----------------------------------------------------------------------- */

/**
 * Apply a single semantic delta record to the image.
 *
 * Dispatches on record type: INODE_UPSERT → inode_put,
 * INODE_DELETE → inode_del, DIRENT_PUT → dirent_put,
 * DIRENT_DELETE → dirent_del.  Other types are ignored
 * (not in the initial image scope).
 *
 * Updates the per-stream applied_high_water.
 *
 * @param img  Image handle.
 * @param rec  Delta record to apply.
 * @return 0 on success, -1 on error.
 */
int catalog_image_apply(struct catalog_image *img,
                        const struct catalog_delta_record *rec);

/**
 * Return the applied high-water seqno for a stream.
 *
 * @param img        Image handle.
 * @param stream_id  Stream to query.
 * @return Highest applied seqno (0 if no records applied).
 */
uint64_t catalog_image_applied_high_water(
    const struct catalog_image *img, uint32_t stream_id);

/**
 * Return true if the image has applied all records up to
 * the given seqno for the given stream.
 */
bool catalog_image_is_complete(const struct catalog_image *img,
                               uint32_t stream_id,
                               uint64_t authority_seqno);

/* -----------------------------------------------------------------------
 * Thread-safety locking (Phase 9C)
 *
 * Internal rwlock protects the inode_map and dirent_map.
 * Public API methods acquire it automatically.  These explicit
 * lock/unlock helpers are for external callers that need to hold
 * the lock across multiple operations (e.g., the poller thread).
 * ----------------------------------------------------------------------- */

/** Acquire shared read lock. */
void catalog_image_read_lock(struct catalog_image *img);

/** Release shared read lock. */
void catalog_image_read_unlock(struct catalog_image *img);

/** Acquire exclusive write lock. */
void catalog_image_write_lock(struct catalog_image *img);

/** Release exclusive write lock. */
void catalog_image_write_unlock(struct catalog_image *img);

#endif /* CATALOG_IMAGE_H */

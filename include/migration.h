/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * migration.h — Subtree migration API (freeze, stream, commit, unfreeze).
 *
 * Moves metadata ownership of a directory subtree from one MDS node to
 * another.  See docs/architecture.md §13 for the full protocol.
 */

#ifndef MIGRATION_H
#define MIGRATION_H

#include <stdint.h>
#include <stddef.h>

#include "pnfs_mds.h"

/* Forward declarations. */
struct mds_catalogue;
struct subtree_map;
struct migration_tracker;

/* -----------------------------------------------------------------------
 * Migration state
 * ----------------------------------------------------------------------- */

enum migration_state {
    MIG_IDLE       = 0,
    MIG_FREEZING   = 1,
    MIG_STREAMING  = 2,
    MIG_COMMITTING = 3,
    MIG_DONE       = 4,
    MIG_FAILED     = 5,
};

/* -----------------------------------------------------------------------
 * Migration transport abstraction
 *
 * Callers set these function pointers to route migration RPCs either
 * via TCP (production) or in-process loopback (tests).
 * ----------------------------------------------------------------------- */

struct migration_transport {
    /**
     * Send BeginMigration to the remote MDS.
     * @param subtree_path   Subtree root path being migrated.
     * @param estimated_inodes  Estimated inode count for progress.
     * @return 0 on ACK, -1 on NACK/error.
     */
    int (*send_begin)(uint32_t dest_mds_id,
                      const char *subtree_path,
                      uint32_t estimated_inodes,
                      void *user_ctx);

    /**
     * Send a serialised inode chunk to the remote MDS.
     * @param data  Serialised migration inode chunk.
     * @param len   Length of @data.
     * @return 0 on success, -1 on error.
     */
    int (*send_inode)(uint32_t dest_mds_id,
                      const void *data, size_t len,
                      void *user_ctx);

    /**
     * Send FinishMigration to the remote MDS.
     * @param total_inodes  Total number of inodes streamed.
     * @return 0 on ACK, -1 on NACK/error.
     */
    int (*send_finish)(uint32_t dest_mds_id,
                       uint32_t total_inodes,
                       void *user_ctx);

    void *user_ctx;
};

/* -----------------------------------------------------------------------
 * Migration inode chunk — passed to the receive callback
 * ----------------------------------------------------------------------- */

/** Single directory entry within a migration chunk. */
struct mig_dirent {
    uint64_t child_fileid;
    uint8_t  type;
    uint16_t name_len;
    char     name[MDS_MAX_NAME + 1];
};

/** Single xattr within a migration chunk. */
struct mig_xattr {
    uint16_t name_len;
    char     name[256];
    uint32_t val_len;
    void    *value;      /**< Points into chunk buffer (not owned). */
};

/** Stripe map data within a migration chunk. */
struct mig_stripe_map {
    uint32_t stripe_count;
    uint32_t stripe_unit;
    uint32_t mirror_count;
    struct mds_ds_map_entry *entries;  /**< Points into chunk buffer. */
};

/** Complete migration chunk for one inode. */
struct mig_inode_chunk {
    uint64_t             fileid;
    struct mds_inode     inode;
    uint32_t             dirent_count;
    struct mig_dirent   *dirents;      /**< malloc'd array. */
    int                  has_stripe_map;
    struct mig_stripe_map stripe_map;
    uint32_t             xattr_count;
    struct mig_xattr    *xattrs;       /**< malloc'd array. */
    int                  has_inline_data;
    void                *inline_data;  /**< malloc'd buffer (or NULL). */
    uint32_t             inline_data_len;
};

/* -----------------------------------------------------------------------
 * Serialisation helpers (used by transport layer and tests)
 * ----------------------------------------------------------------------- */

/**
 * @brief Serialise a migration inode chunk into a malloc'd buffer.
 * @param chunk  Populated chunk to serialise.
 * @param[out] out  Receives the buffer (caller must free).
 * @param[out] out_len  Receives the buffer length.
 * @return 0 on success, -errno on failure.
 */
int mig_chunk_serialise(const struct mig_inode_chunk *chunk,
                        void **out, size_t *out_len);

/**
 * @brief Deserialise a migration inode chunk from a buffer.
 * @param data  Serialised chunk.
 * @param len   Length of @data.
 * @param[out] chunk  Receives the deserialised chunk.  Caller must
 *                    call mig_chunk_free() when done.
 * @return 0 on success, -errno on failure.
 */
int mig_chunk_deserialise(const void *data, size_t len,
                          struct mig_inode_chunk *chunk);

/**
 * @brief Free dynamic fields within a migration chunk.
 */
void mig_chunk_free(struct mig_inode_chunk *chunk);

/* -----------------------------------------------------------------------
 * Source-side API
 * ----------------------------------------------------------------------- */

/**
 * @brief Initiate a subtree migration (source MDS side).
 *
 * Freezes the subtree, streams all inodes to the destination, commits
 * the ownership change in the subtree map, and converts the local
 * subtree root to a junction directory.
 *
 * @param cat            Catalogue handle (authority backend).
 * @param map            Subtree map handle.
 * @param transport      Migration transport callbacks.
 * @param subtree_path   Subtree root path (e.g. "/home").
 * @param subtree_root_fileid  Fileid of the subtree root directory.
 * @param dest_mds_id    Destination MDS node ID.
 * @return MDS_OK on success.
 */
enum mds_status migration_initiate(
    struct mds_catalogue *cat,
    struct subtree_map *map,
    const struct migration_transport *transport,
    const char *subtree_path,
    uint64_t subtree_root_fileid,
    uint32_t dest_mds_id,
    struct migration_tracker *tracker);

/* -----------------------------------------------------------------------
 * Dest-side API
 * ----------------------------------------------------------------------- */

/**
 * @brief Apply a received migration inode chunk.
 *
 * Writes the inode, dirents, stripe map, xattrs, and inline data
 * through the catalogue API.  Uses a single
 * transaction for atomicity.  On RonDB, each write is self-contained.
 *
 * @param cat    Catalogue handle (authority backend).
 * @param chunk  Deserialised inode chunk.
 * @return MDS_OK on success.
 */
enum mds_status migration_apply_chunk(struct mds_catalogue *cat,
                                      const struct mig_inode_chunk *chunk);

/* -----------------------------------------------------------------------
 * Cleanup API
 * ----------------------------------------------------------------------- */

/**
 * @brief Delete migrated data from the source catalogue.
 *
 * Walks the subtree rooted at @root_fileid and deletes all inodes,
 * dirents, stripe maps, and xattrs.  Called by the source MDS after
 * a configurable delay post-migration.
 *
 * @param cat            Source catalogue handle.
 * @param root_fileid    Fileid of the migrated subtree root.
 * @return MDS_OK on success.
 */
enum mds_status migration_cleanup(struct mds_catalogue *cat,
                                  uint64_t root_fileid);


/* -----------------------------------------------------------------------
 * Migration progress tracker (Seq 10)
 *
 * Thread-safe, single-active-migration-at-a-time progress reporter.
 * Attach to cluster_server; queried by admin transport for
 * CT_MSG_MIG_PROGRESS_ADMIN_REQ.
 * ----------------------------------------------------------------------- */

struct migration_tracker;

/**
 * @brief Create a migration progress tracker.
 * @param[out] out  Receives the tracker handle.
 * @return MDS_OK, MDS_ERR_NOMEM.
 */
enum mds_status migration_tracker_create(struct migration_tracker **out);

/**
 * @brief Destroy a migration tracker.
 */
void migration_tracker_destroy(struct migration_tracker *t);

/**
 * @brief Mark the start of a new migration.
 *
 * Returns MDS_ERR_DELAY if another migration is already in progress.
 *
 * @param t      Tracker handle.
 * @param path   Subtree path being migrated.
 * @param total  Estimated total inodes.
 * @return MDS_OK, MDS_ERR_DELAY, MDS_ERR_INVAL.
 */
enum mds_status migration_tracker_begin(struct migration_tracker *t,
                                        const char *path,
                                        uint32_t total);

/**
 * @brief Atomically increment done_inodes.
 */
void migration_tracker_update(struct migration_tracker *t,
                              uint32_t done_delta);

/**
 * @brief Mark migration complete (MIG_DONE or MIG_FAILED).
 */
void migration_tracker_finish(struct migration_tracker *t,
                              enum migration_state final_state);

/**
 * @brief Snapshot current progress (thread-safe read).
 *
 * When idle: state=MIG_IDLE, path="", total=0, done=0.
 *
 * @param t          Tracker (NULL => idle).
 * @param[out] state Current state.
 * @param path_out   Buffer for subtree path.
 * @param path_cap   Capacity of path_out.
 * @param[out] total Total inodes.
 * @param[out] done  Inodes completed.
 */
void migration_tracker_get_progress(const struct migration_tracker *t,
                                    enum migration_state *state,
                                    char *path_out, size_t path_cap,
                                    uint32_t *total, uint32_t *done);

#endif /* MIGRATION_H */

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * backup.h -- Online backup + snapshot restore V1.
 *
 * @deprecated All backup APIs return MDS_ERR_NOSUPPORT in RonDB mode.
 * This module has not been ported to the catalogue API.
 */

#ifndef BACKUP_H
#define BACKUP_H

#include <stdbool.h>
#include <stdint.h>

#include "pnfs_mds.h"

struct mds_catalogue;

/* -----------------------------------------------------------------------
 * Flags
 * ----------------------------------------------------------------------- */

/** Compact mode (slower, smaller output). @deprecated Unused in RonDB mode. */
#define MDS_BACKUP_COMPACT            (1U << 0)

/** Sync backend to stable storage before copying. */
#define MDS_BACKUP_FSYNC_BEFORE_COPY  (1U << 1)

/** Allow mds_backup_restore() to overwrite an existing destination. */
#define MDS_BACKUP_RESTORE_OVERWRITE  (1U << 2)

/* Maximum path length stored in a report. */
#define MDS_BACKUP_PATH_MAX 4096

/* -----------------------------------------------------------------------
 * Report structure
 * ----------------------------------------------------------------------- */

/**
 * Summary produced by create, verify, and info operations.
 *
 * Fields are populated from the backup artifact or its sidecar.
 * On error, @a error_msg contains a human-readable description.
 */
struct mds_backup_report {
    int64_t  created_unix_ts;     /**< Backup creation time (UTC). */
    uint32_t schema_version;      /**< Schema version in the backup. */
    uint64_t backup_size_bytes;   /**< Size of the backup data file. */
    uint64_t inode_count;         /**< Inode record count. */
    uint64_t dirent_count;        /**< Dirent record count. */
    uint64_t stripe_map_count;    /**< Stripe map record count. */
    bool     verified;            /**< True if verification passed. */
    char     error_msg[256];      /**< Empty on success. */
    char     source_path[MDS_BACKUP_PATH_MAX]; /**< Source DB path. */
};

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

/**
 * Create a hot backup of the catalogue.
 *
 * @deprecated Returns MDS_ERR_NOSUPPORT -- not ported to RonDB.
 *
 * @param cat       Catalogue handle.
 * @param dest_path Destination path for the backup data file.
 * @param flags     Combination of MDS_BACKUP_* flags.
 * @param out       Optional report filled on success.  May be NULL.
 * @return MDS_ERR_NOSUPPORT.
 */
enum mds_status mds_backup_create(struct mds_catalogue *cat,
                                  const char *dest_path,
                                  uint32_t flags,
                                  struct mds_backup_report *out);

/**
 * Verify a backup artifact.
 *
 * @deprecated Returns MDS_ERR_NOSUPPORT -- not ported to RonDB.
 *
 * @param backup_path Path to the backup data file.
 * @param out         Report filled with counts and verified flag.
 * @return MDS_ERR_NOSUPPORT.
 */
enum mds_status mds_backup_verify(const char *backup_path,
                                  struct mds_backup_report *out);

/**
 * Restore a backup to a new database path.
 *
 * @deprecated Returns MDS_ERR_NOSUPPORT -- not ported to RonDB.
 *
 * @param backup_path  Path to the backup data file.
 * @param dest_db_path Destination path for the restored database.
 * @param flags        Combination of MDS_BACKUP_* flags.
 * @return MDS_ERR_NOSUPPORT.
 */
enum mds_status mds_backup_restore(const char *backup_path,
                                   const char *dest_db_path,
                                   uint32_t flags);

/**
 * Read backup metadata from the `.meta` sidecar file.
 *
 * @deprecated Returns MDS_ERR_NOSUPPORT -- not ported to RonDB.
 *
 * @param backup_path Path to the backup data file (`.meta` appended).
 * @param out         Report filled from sidecar fields.
 * @return MDS_ERR_NOSUPPORT.
 */
enum mds_status mds_backup_info(const char *backup_path,
                                struct mds_backup_report *out);

#endif /* BACKUP_H */

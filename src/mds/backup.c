/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * backup.c — Online backup stubs (RonDB-native).
 *
 * Backup module stub.  RonDB backup uses NDB Backup API
 * (not yet implemented).  All operations return MDS_ERR_NOSUPPORT.
 */

#include <string.h>

#include "backup.h"
#include "mds_catalogue.h"
#include "pnfs_mds.h"

enum mds_status mds_backup_create(struct mds_catalogue *cat,
                                  const char *dest_path,
                                  uint32_t flags,
                                  struct mds_backup_report *out)
{
    (void)cat; (void)dest_path; (void)flags; (void)out;
    return MDS_ERR_NOSUPPORT;
}

enum mds_status mds_backup_verify(const char *backup_path,
                                  struct mds_backup_report *out)
{
    (void)backup_path;
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    return MDS_ERR_NOSUPPORT;
}

enum mds_status mds_backup_restore(const char *backup_path,
                                   const char *dest_db_path,
                                   uint32_t flags)
{
    (void)backup_path; (void)dest_db_path; (void)flags;
    return MDS_ERR_NOSUPPORT;
}

enum mds_status mds_backup_info(const char *backup_path,
                                struct mds_backup_report *out)
{
    (void)backup_path;
    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    return MDS_ERR_NOSUPPORT;
}

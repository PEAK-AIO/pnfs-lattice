/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_memdb.h -- In-memory catalogue backend (reference
 * implementation; non-durable, single-node, bounded capacity).
 *
 * Built into pnfs_mds_core when ENABLE_MEMDB_BACKEND is ON
 * (HAVE_MEMDB_BACKEND=1).  catalogue_memdb_open_cfg() has the shape the
 * backend factory expects for `catalogue_backend = memdb`; the unit
 * tests use catalogue_memdb_open() through tests/test_helpers.h.
 *
 * Every instance is independent: two handles opened in one process
 * share no state.  An in-process multi-MDS test that wants shared
 * authority opens ONE instance and hands the same handle to each MDS
 * context.
 */

#ifndef CATALOGUE_MEMDB_H
#define CATALOGUE_MEMDB_H

#include "pnfs_mds.h"
#include "mds_catalogue.h"

struct mds_config;

/**
 * Open an in-memory catalogue with a pre-seeded root inode (fileid
 * MDS_FILEID_ROOT).
 *
 * @return A fresh, independent handle to release with
 *         mds_catalogue_close(), or NULL when memory is exhausted.
 */
struct mds_catalogue *catalogue_memdb_open(void);

/**
 * Factory-shaped constructor: open an in-memory catalogue for @p cfg.
 *
 * The instance takes no parameter from the configuration today; the
 * argument is validated and reserved for the backend registry.
 * Whether a multi-process cluster may run on this store is decided by
 * the daemon from mds_cluster_supported(), never here.
 *
 * @param cfg  MDS configuration (non-NULL).
 * @param out  Receives the handle (non-NULL); set to NULL on failure.
 * @return MDS_OK; MDS_ERR_INVAL on a NULL argument; MDS_ERR_NOMEM when
 *         the bounded tables cannot be allocated; MDS_ERR_IO when the
 *         instance mutex cannot be initialised.
 */
enum mds_status catalogue_memdb_open_cfg(const struct mds_config *cfg,
                                         struct mds_catalogue **out);

#endif /* CATALOGUE_MEMDB_H */

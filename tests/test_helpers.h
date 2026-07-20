/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_helpers.h -- Shared test utilities.
 *
 * Provides helper functions for unit and integration tests that
 * bypass production-path constraints (e.g., pNFS DS requirement
 * for regular file creation).
 */

#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include <string.h>
#include <time.h>
#include "pnfs_mds.h"
#include "mds_catalogue.h"

/**
 * Open a test catalogue backed by the in-memory memdb backend.
 *
 * Always succeeds -- no external dependencies required.
 * Each call returns a fresh, independent catalogue with a
 * pre-seeded root inode (fileid 2).
 *
 * @return Catalogue handle (never NULL).
 */
extern struct mds_catalogue *catalogue_memdb_open(void);
/** Make the next atomic final-unlink call return @a status unchanged. */
void catalogue_memdb_fail_next_final_unlink(
	struct mds_catalogue *cat, enum mds_status status);

/** Atomically unlink a named file immediately after the next open-state put. */
void catalogue_memdb_unlink_after_next_open_put(
	struct mds_catalogue *cat, uint64_t parent_fileid, const char *name,
	uint64_t fileid, uint64_t generation);

/** Insert and inspect raw GC tasks without production enqueue validation. */
enum mds_status catalogue_memdb_inject_raw_gc_task(
	struct mds_catalogue *cat, const struct mds_gc_task *task);
enum mds_status catalogue_memdb_get_gc_task(
	struct mds_catalogue *cat, uint8_t task_kind, uint64_t task_id,
	struct mds_gc_task *task);

static inline struct mds_catalogue *open_test_catalogue(void)
{
	return catalogue_memdb_open();
}

/**
 * Create a regular file inode + dirent via the catalogue API.
 *
 * Bypasses the pNFS DS requirement (prealloc != NULL) by writing
 * the inode and dirent directly through the low-level catalogue
 * operations.  The file has no flags (no INLINE, no DS_PENDING) --
 * it exists in the namespace but has no data backing.  Suitable
 * for tests that only need an inode in the namespace for LOOKUP,
 * GETATTR, RENAME, LINK, etc.
 *
 * @param cat      Catalogue handle.
 * @param parent   Parent directory fileid.
 * @param name     Entry name.
 * @param mode     Permission bits.
 * @param out      Receives the created inode.
 * @return MDS_OK on success.
 */
static inline enum mds_status
test_create_file(struct mds_catalogue *cat, uint64_t parent,
		 const char *name, uint32_t mode,
		 struct mds_inode *out)
{
	struct mds_cat_txn *txn = NULL;
	struct mds_inode child;
	uint64_t child_fid = 0;
	struct timespec now;
	enum mds_status st;

	st = mds_cat_alloc_fileid(cat, NULL, &child_fid);
	if (st != MDS_OK) {
		return st;
	}

	st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn);
	if (st != MDS_OK) {
		return st;
	}

	clock_gettime(CLOCK_REALTIME, &now);
	memset(&child, 0, sizeof(child));
	child.fileid = child_fid;
	child.type = MDS_FTYPE_REG;
	child.mode = mode;
	child.nlink = 1;
	child.atime = now;
	child.mtime = now;
	child.ctime = now;
	child.change = 1;
	child.generation = 1;
	child.parent_fileid = parent;

	st = mds_cat_inode_put(cat, txn, &child);
	if (st != MDS_OK) {
		mds_cat_txn_abort(txn);
		return st;
	}

	st = mds_cat_dirent_put(cat, txn, parent, name,
				child_fid, (uint8_t)MDS_FTYPE_REG);
	if (st != MDS_OK) {
		mds_cat_txn_abort(txn);
		return st;
	}

	/* Touch parent mtime/change. */
	{
		struct mds_inode par;

		st = mds_cat_ns_getattr(cat, parent, &par);
		if (st == MDS_OK) {
			par.mtime = now;
			par.ctime = now;
			par.change++;
			(void)mds_cat_inode_put(cat, txn, &par);
		}
	}

	st = mds_cat_txn_commit(txn);
	if (st != MDS_OK) {
		return MDS_ERR_IO;
	}

	*out = child;
	return MDS_OK;
}

#endif /* TEST_HELPERS_H */

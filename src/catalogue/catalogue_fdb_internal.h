/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_fdb_internal.h -- helpers shared by the FoundationDB
 * backend's translation units (catalogue_fdb_ns.c, catalogue_fdb_ext.c).
 *
 * Backend-private: nothing here is part of the library's public
 * headers (include/catalogue_fdb.h declares only what catalogue_fdb.c
 * needs to assemble the vtables).  Every helper is a transaction body
 * helper in the sense of fdb_txn.h: it works on the caller's
 * transaction, returns 0 or an fdb_error_t for the body to hand back
 * to the runner, starts or finishes exactly the read it names, and
 * writes blindly -- the caller's body owns every decision.
 */

#ifndef CATALOGUE_FDB_INTERNAL_H
#define CATALOGUE_FDB_INTERNAL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "pnfs_mds.h"
#include "fdb_codec.h"
#include "fdb_keys.h"
#include "fdb_txn.h"

/* -----------------------------------------------------------------------
 * Inode blob primitives.  A directory's nlink, change, mtime and ctime
 * live in the INODE side keys (fdb_keys.h), so a body that must not
 * conflict with those hot counters reads the type or mode it needs
 * through the blob alone, and the side keys are never rewritten
 * through the blob.
 * ----------------------------------------------------------------------- */

/**
 * Start the point read of the inode blob of @p fileid (FDB_INODE_PART_BLOB
 * only, never the side keys).
 *
 * @param tr      The body's transaction.
 * @param p       The handle's key prefix.
 * @param fileid  Inode to read.
 * @return The future to finish with fdb_blob_read_finish(); NULL only on
 *         an unusable key (the finish then reports FDB_ERR_PLATFORM_ERROR).
 *
 * Ownership: the future is owned by the caller until finished.
 * Thread safety: that of @p tr.
 */
static inline FDBFuture *fdb_blob_read_start(FDBTransaction *tr, const struct fdb_key_prefix *p,
                                             uint64_t fileid)
{
    struct fdb_key k;

    fdb_key_inode(&k, p, fileid, FDB_INODE_PART_BLOB);
    return fdb_txn_get_start(tr, &k, false);
}

/**
 * Finish a blob read started by fdb_blob_read_start() and destroy the
 * future.  For a directory the four counter fields of *out are the
 * dead blob copies, not the side keys' values (fdb_codec.h).
 *
 * @param f      Future from fdb_blob_read_start() (NULL tolerated).
 * @param out    Receives the decoded inode when present.
 * @param found  Receives whether the blob exists.
 * @return 0; an fdb_error_t from the read; FDB_ERR_PLATFORM_ERROR when the
 *         blob does not decode (corrupt row) or @p f is NULL.
 *
 * Ownership: consumes @p f.  Thread safety: that of the transaction.
 */
static inline fdb_error_t fdb_blob_read_finish(FDBFuture *f, struct mds_inode *out, bool *found)
{
    uint8_t buf[FDB_INODE_ENC_MAX];
    size_t len = 0;
    fdb_error_t err;

    err = fdb_txn_get_finish(f, buf, sizeof(buf), &len, found);
    if (err != 0) {
        return err;
    }
    if (*found && !fdb_inode_decode(buf, len, out)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    return 0;
}

/**
 * Blind-set only the blob of @p ino; a directory's side keys are left
 * untouched (catalogue_fdb_inode_write writes the whole logical inode).
 *
 * @param tr   The body's transaction.
 * @param p    The handle's key prefix.
 * @param ino  Inode to encode; ino->fileid is the key.
 * @return 0, or FDB_ERR_PLATFORM_ERROR when @p ino cannot be encoded.
 *
 * Ownership: nothing is retained.  Thread safety: that of @p tr.
 */
static inline int fdb_blob_write(FDBTransaction *tr, const struct fdb_key_prefix *p,
                                 const struct mds_inode *ino)
{
    struct mds_inode blob_copy;
    struct fdb_dir_counters ctr;
    uint8_t enc[FDB_INODE_ENC_MAX];
    size_t len = 0;
    struct fdb_key k;

    fdb_inode_split(ino, &blob_copy, &ctr);
    if (!fdb_inode_encode(&blob_copy, enc, sizeof(enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_key_inode(&k, p, ino->fileid, FDB_INODE_PART_BLOB);
    fdb_txn_set(tr, &k, enc, len);
    return 0;
}

/* -----------------------------------------------------------------------
 * Dirent primitives.  A binding is two rows, DIRENT and DIRENT_SEQ
 * (fdb_keys.h), always cleared together; writing one is the namespace
 * unit's business (it mints the cookie).
 * ----------------------------------------------------------------------- */

/**
 * Start the point read of the DIRENT row (@p parent, @p name).
 *
 * @param tr      The body's transaction.
 * @param p       The handle's key prefix.
 * @param parent  Directory fileid.
 * @param name    Entry name, 1..MDS_MAX_NAME bytes.
 * @return The future to finish with fdb_dirent_read_finish(); NULL only
 *         on an unusable key (empty or over-long name).
 *
 * Ownership: the future is owned by the caller until finished.
 * Thread safety: that of @p tr.
 */
static inline FDBFuture *fdb_dirent_read_start(FDBTransaction *tr,
                                               const struct fdb_key_prefix *p, uint64_t parent,
                                               const char *name)
{
    struct fdb_key k;

    fdb_key_dirent(&k, p, parent, name);
    return fdb_txn_get_start(tr, &k, false);
}

/**
 * Finish a dirent read started by fdb_dirent_read_start() and destroy
 * the future.
 *
 * @param f      Future from fdb_dirent_read_start() (NULL tolerated).
 * @param out    Receives the decoded row when present.
 * @param found  Receives whether the row exists.
 * @return 0; an fdb_error_t from the read; FDB_ERR_PLATFORM_ERROR when the
 *         row does not decode or @p f is NULL.
 *
 * Ownership: consumes @p f.  Thread safety: that of the transaction.
 */
static inline fdb_error_t fdb_dirent_read_finish(FDBFuture *f, struct fdb_dirent_val *out,
                                                 bool *found)
{
    uint8_t buf[FDB_DIRENT_ENC_SIZE];
    size_t len = 0;
    fdb_error_t err;

    err = fdb_txn_get_finish(f, buf, sizeof(buf), &len, found);
    if (err != 0) {
        return err;
    }
    if (*found && !fdb_dirent_decode(buf, len, out)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    return 0;
}

/**
 * Blind-clear the DIRENT row (@p parent, @p name) and its DIRENT_SEQ
 * index row @p seq together.
 *
 * @param tr      The body's transaction.
 * @param p       The handle's key prefix.
 * @param parent  Directory fileid.
 * @param name    Entry name.
 * @param seq     The entry's cookie, as read from its DIRENT row.
 *
 * Ownership: nothing is retained.  Thread safety: that of @p tr.
 */
static inline void fdb_dirent_clear(FDBTransaction *tr, const struct fdb_key_prefix *p,
                                    uint64_t parent, const char *name, uint64_t seq)
{
    struct fdb_key k;

    fdb_key_dirent(&k, p, parent, name);
    fdb_txn_clear(tr, &k);
    fdb_key_dirent_seq(&k, p, parent, seq);
    fdb_txn_clear(tr, &k);
}
/* -----------------------------------------------------------------------
 * GC queue rows.  A row and its GC_BY_OWNER key (fdb_keys.h) are one
 * unit: written together by gc_enqueue and by the fused remove, cleared
 * together by gc_dequeue, never one without the other -- that is what
 * lets an owner-scoped peek trust the index without a repair pass.
 * ----------------------------------------------------------------------- */

/**
 * Blind-set the GC row @p e (key GC + e->gc_seq, value fdb_gc codec) and
 * its index key GC_BY_OWNER + e->owner_mds_id + e->gc_seq (empty value)
 * in @p tr.
 *
 * @param tr  The body's transaction.
 * @param p   The handle's key prefix.
 * @param e   Row to write; gc_seq and owner_mds_id must be final.
 * @return 0, or FDB_ERR_PLATFORM_ERROR when @p e cannot be encoded
 *         (nfs_fh_len above MDS_NFS_FH_MAX).
 *
 * Ownership: nothing is retained.  Thread safety: that of @p tr.
 */
static inline int fdb_gc_row_set(FDBTransaction *tr, const struct fdb_key_prefix *p,
                                 const struct mds_gc_entry *e)
{
    static const uint8_t empty = 0;
    uint8_t enc[FDB_GC_ENC_MAX];
    size_t len = 0;
    struct fdb_key k;

    if (!fdb_gc_encode(e, enc, sizeof(enc), &len)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_key_gc(&k, p, e->gc_seq);
    fdb_txn_set(tr, &k, enc, len);
    fdb_key_gc_by_owner(&k, p, e->owner_mds_id, e->gc_seq);
    fdb_txn_set(tr, &k, &empty, 0);
    return 0;
}

/**
 * Blind-clear the GC row @p gc_seq and its index key under
 * @p owner_mds_id (the owner stored in the row) in @p tr.
 *
 * Ownership: nothing is retained.  Thread safety: that of @p tr.
 */
static inline void fdb_gc_row_clear(FDBTransaction *tr, const struct fdb_key_prefix *p,
                                    uint64_t gc_seq, uint32_t owner_mds_id)
{
    struct fdb_key k;

    fdb_key_gc(&k, p, gc_seq);
    fdb_txn_clear(tr, &k);
    fdb_key_gc_by_owner(&k, p, owner_mds_id, gc_seq);
    fdb_txn_clear(tr, &k);
}

#endif /* CATALOGUE_FDB_INTERNAL_H */

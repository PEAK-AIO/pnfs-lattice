/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_daos_internal.h -- helpers shared by the DAOS backend's
 * translation units (catalogue_daos_ns.c, catalogue_daos_ext.c,
 * catalogue_daos_coord.c, catalogue_daos_cluster.c, catalogue_daos.c).
 *
 * Backend-private: nothing here is part of the library's public headers
 * (include/catalogue_daos.h declares only what catalogue_daos.c needs to
 * assemble the vtables).  Every helper is a transaction body helper in
 * the sense of daos_txn.h: it works on the caller's transaction and
 * object handle, returns 0 or a -DER_* code for the body to hand back to
 * the runner, performs exactly the operation it names, and writes
 * blindly -- the caller's body owns every decision and never reads back
 * what it wrote.  The functions are implemented in catalogue_daos_ns.c.
 *
 * Inode layout recap (daos_keys.h): dkey 'a' holds akey 'b' (the
 * fdb_inode blob with a directory's nlink / change / mtime / ctime dead)
 * and, for a directory, K counter shards 'c'+be32 i (daos_codec.h).  A
 * body that must not conflict with the hot counters reads the blob alone
 * or the blob plus the one shard it will rewrite; GETATTR reads the blob
 * plus all K shards in ONE fetch and composes them.
 */

#ifndef CATALOGUE_DAOS_INTERNAL_H
#define CATALOGUE_DAOS_INTERNAL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

#include "pnfs_mds.h"
#include "daos_codec.h"
#include "daos_keys.h"
#include "daos_txn.h"
#include "fdb_codec.h"

/* -----------------------------------------------------------------------
 * Inode reads
 * ----------------------------------------------------------------------- */

/**
 * One inode read in flight: the blob and either no shard, one shard
 * (@p shard >= 0) or all K shards (@p shard == DAOS_INODE_ALL_SHARDS).
 * The descriptor arrays are one heap block (K is a runtime value);
 * daos_inode_req_fini() releases it.  Stable in memory from start to
 * finish; a wave holds one request per object.
 */
#define DAOS_INODE_ALL_SHARDS (-1)

struct daos_inode_req {
    struct daos_fetch      f;
    struct daos_key        ak_blob;
    uint8_t                blob[FDB_INODE_ENC_MAX];
    uint32_t               nshards;     /**< 0, 1 or K. */
    uint32_t               first_shard; /**< Index of the first shard read. */
    bool                   all;         /**< All K shards: compose on finish. */
    uint8_t               *shard_keys;  /**< nshards compact 'c'+be32 akeys. */
    uint8_t               *shard_buf;   /**< nshards * DAOS_DIR_SHARD_ENC_SIZE. */
    struct daos_dir_shard *shards;      /**< nshards decoded values (finish). */
    void                  *block;       /**< The one allocation. */
};

/**
 * Prepare @p r to read the blob of dkey 'a' plus the shards selected by
 * @p shard: DAOS_INODE_ALL_SHARDS for all K shards of the backend, or
 * 0..K-1 for that one shard.  Blob-only reads use
 * daos_inode_req_init_blob().
 *
 * @return 0; -DER_NOMEM; -DER_INVAL for a shard outside [0, K).
 */
int daos_inode_req_init(struct daos_inode_req *r, const struct daos_backend *b, int32_t shard);

/** Prepare @p r to read the blob alone. */
int daos_inode_req_init_blob(struct daos_inode_req *r);

/** Re-arm a finished request for another read of the same shape (a
 *  page that is reused across transactions); no allocation. */
void daos_inode_req_rearm(struct daos_inode_req *r);

/** Release the request's block.  Safe on an unused or zeroed request. */
void daos_inode_req_fini(struct daos_inode_req *r);

/** Issue the fetch of @p r on @p oh (one RPC).  0 or -DER_*. */
int daos_inode_read_start(struct daos_txn *t, daos_handle_t oh, struct daos_inode_req *r);

/**
 * Wait for @p r and decode.  *found reports whether the blob exists; when
 * it does, *out is the decoded inode and, for a directory read with all
 * K shards, its counters are composed from the shards present
 * (daos_dir_shards_compose).  A one-shard request leaves the blob's dead
 * counters in *out and reports the shard through
 * daos_inode_req_shard().
 *
 * @return 0; -DER_IO for a blob or shard that does not decode; -DER_*.
 */
int daos_inode_read_finish(struct daos_txn *t, struct daos_inode_req *r, struct mds_inode *out,
                           bool *found);

/**
 * The one shard of a finished one-shard request.  *present is false when
 * the shard row does not exist (a directory that never touched it).
 *
 * @return true; false when the request did not read exactly one shard or
 *         the shard does not decode.
 */
bool daos_inode_req_shard(const struct daos_inode_req *r, struct daos_dir_shard *out,
                          bool *present);

/** Blob + all K shards in one fetch, composed: the GETATTR read. */
int daos_inode_read_full(struct daos_txn *t, daos_handle_t oh, struct mds_inode *out,
                         bool *found);

/** Blob alone (dead directory counters), one fetch. */
int daos_inode_read_blob(struct daos_txn *t, daos_handle_t oh, struct mds_inode *out,
                         bool *found);

/* -----------------------------------------------------------------------
 * Inode writes
 * ----------------------------------------------------------------------- */

/** Blind update of the blob akey alone (a directory's shards untouched). */
int daos_inode_write_blob(struct daos_txn *t, daos_handle_t oh, const struct mds_inode *ino);

/**
 * Write @p ino as one logical inode: the blob and, for a directory,
 * shard 0 carrying the inode's nlink / change / mtime / ctime.  With
 * @p fresh false (inode_put over a possibly existing directory) shards
 * 1..K-1 are punched as well so the write is a true replace; a freshly
 * created inode (@p fresh true) has no shards to punch.
 */
int daos_inode_write_full(struct daos_txn *t, daos_handle_t oh, const struct mds_inode *ino,
                          bool fresh);

/* -----------------------------------------------------------------------
 * Directory counter shards
 * ----------------------------------------------------------------------- */

/** The shard this thread's mutations use: hash(thread) % K. */
uint32_t daos_dir_shard_pick(const struct daos_backend *b);

/** Apply a mutation to a shard value: nlink_delta += d (saturating),
 *  change += change_delta, and when @p stamp is not NULL mtime / ctime =
 *  *stamp (composition takes the maximum across shards). */
void daos_dir_shard_apply(struct daos_dir_shard *s, int64_t nlink_delta, uint64_t change_delta,
                          const struct timespec *stamp);

/** Blind update of shard @p idx of the directory @p oh. */
int daos_dir_shard_write(struct daos_txn *t, daos_handle_t oh, uint32_t idx,
                         const struct daos_dir_shard *s);

/**
 * Read-modify-write of ONE shard of the directory @p oh: fetch shard
 * @p idx (absent = zero), apply the deltas (daos_dir_shard_apply), write
 * it back.  Two mutations of a directory conflict only when they pick
 * the same shard; a body that already read the shard in its first wave
 * applies the deltas itself and calls daos_dir_shard_write().
 */
int daos_dir_touch(struct daos_txn *t, daos_handle_t oh, uint32_t idx, int64_t nlink_delta,
                   uint64_t change_delta, const struct timespec *stamp);

/**
 * "Has at least one entry" test of the directory @p oh: a one-key
 * listing of the 'N' akeys inside the transaction (contract C3).
 */
int daos_dir_nonempty(struct daos_txn *t, daos_handle_t oh, bool *nonempty);

/* -----------------------------------------------------------------------
 * Dirents.  A binding is the 'N' akey and the 'S'+seq row, always
 * written and cleared together.
 * ----------------------------------------------------------------------- */

/** One dirent read in flight (wave half). */
struct daos_dirent_req {
    struct daos_fetch_small f;
    struct daos_key         ak;
    uint8_t                 buf[FDB_DIRENT_ENC_SIZE];
};

/** Issue the fetch of the 'N' akey @p name on the directory @p oh. */
int daos_dirent_read_start(struct daos_txn *t, daos_handle_t oh, const char *name,
                           struct daos_dirent_req *r);

/** Wait and decode; *found reports presence.  -DER_IO for a corrupt row. */
int daos_dirent_read_finish(struct daos_txn *t, struct daos_dirent_req *r,
                            struct fdb_dirent_val *out, bool *found);

/** start + finish. */
int daos_dirent_read(struct daos_txn *t, daos_handle_t oh, const char *name,
                     struct fdb_dirent_val *out, bool *found);

/**
 * Write the binding (@p name -> @p child of @p type, cookie @p seq): the
 * 'N' akey (DAOS_COND_AKEY_INSERT when @p insert_only, so a concurrent
 * create of the same name fails with -DER_EXIST at the call or at
 * commit) and the 'S'+seq row.
 */
int daos_dirent_write(struct daos_txn *t, daos_handle_t oh, const char *name, uint64_t child,
                      uint8_t type, uint64_t seq, bool insert_only);

/** Punch the 'N' akey @p name and the 'S'+seq dkey together. */
int daos_dirent_clear(struct daos_txn *t, daos_handle_t oh, const char *name, uint64_t seq);

/** Punch the 'S'+seq dkey alone (a rebinding that rewrites 'N'). */
int daos_dirent_seq_clear(struct daos_txn *t, daos_handle_t oh, uint64_t seq);

/* -----------------------------------------------------------------------
 * Index markers (one-byte values; an empty value would be a punch)
 * ----------------------------------------------------------------------- */

/** Blind update of the marker @p akey under @p dkey. */
int daos_marker_set(struct daos_txn *t, daos_handle_t oh, const struct daos_key *dkey,
                    struct daos_key *akey);

/** Punch the marker @p akey under @p dkey (absent is a no-op). */
int daos_marker_clear(struct daos_txn *t, daos_handle_t oh, const struct daos_key *dkey,
                      struct daos_key *akey);

/* -----------------------------------------------------------------------
 * GC queue rows.  A GC row (spread object, dkey be64 gc_seq) and its
 * GC_BY_OWNER(owner) marker (uint64 dkey gc_seq) are one unit: written
 * together by gc_enqueue and the fused remove, cleared together by
 * gc_dequeue, never one without the other -- that is what lets an
 * owner-scoped peek trust the index without a repair pass.  Both
 * objects are opened through daos_txn_obj().
 * ----------------------------------------------------------------------- */

/** Write the GC row @p e and its owner index marker.  -DER_INVAL when
 *  @p e cannot be encoded (nfs_fh_len above MDS_NFS_FH_MAX). */
int daos_gc_row_set(struct daos_txn *t, const struct mds_gc_entry *e);

/** Punch the GC row @p gc_seq and its marker under @p owner_mds_id. */
int daos_gc_row_clear(struct daos_txn *t, uint64_t gc_seq, uint32_t owner_mds_id);

/* -----------------------------------------------------------------------
 * Stripe map blob and its STRIPE_IDX marker (written and cleared together)
 * ----------------------------------------------------------------------- */

/** Write the stripe map of @p fileid (INODE 's'/'m' on @p inode_oh) and
 *  set its STRIPE_IDX marker.  -DER_INVAL for a geometry or entry out
 *  of bounds; -DER_NOMEM. */
int daos_stripe_map_write(struct daos_txn *t, daos_handle_t inode_oh, uint64_t fileid,
                          uint32_t sc, uint32_t su, uint32_t mc,
                          const struct mds_ds_map_entry *entries);

/** Punch the 's' dkey of @p inode_oh and clear the STRIPE_IDX marker. */
int daos_stripe_map_clear(struct daos_txn *t, daos_handle_t inode_oh, uint64_t fileid);

/* -----------------------------------------------------------------------
 * Purge
 * ----------------------------------------------------------------------- */

/**
 * Everything hanging off a deleted inode: the 'a', 'x', 'i' and 'h' dkeys
 * and, unless @p keep_stripes, the stripe map with its STRIPE_IDX marker.
 * The by-file index dkeys ('L', 'O', 'D') and the lock dkey ('K') are
 * left, as the FoundationDB purge leaves its index tables.  REMOVE drops
 * the stripes (the caller snapshotted the map before the mutation);
 * RENAME keeps them for the caller's GC pass (rename_drop_victim).
 */
int daos_inode_purge(struct daos_txn *t, daos_handle_t oh, uint64_t fileid, bool keep_stripes);

#endif /* CATALOGUE_DAOS_INTERNAL_H */

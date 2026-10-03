/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * daos_codec.h -- Value formats specific to the DAOS catalogue backend.
 *
 * Everything else the backend stores reuses fdb_codec.h (pure C, no
 * client-library dependency): inode blob, dirent and dirent-sequence
 * rows, GC rows, the extended-authority rows and the coordination and
 * cluster rows.  Container attribute stamps are plain little-endian
 * integers written with fdb_le64_put / fdb_le32_put and read back with
 * fdb_le64_decode / fdb_le32_decode (exact length).
 *
 * Same contract as fdb_codec.h: explicit versioned little-endian
 * layouts, every decoder rejects a short or padded value, an unknown
 * version and any field outside its bound; a rejected value is reported
 * as MDS_ERR_IO by the slot that read it.  No function allocates except
 * daos_stripe_map_decode, which hands the caller the entries array.
 *
 * Directory counter shard (version 1), DAOS_DIR_SHARD_ENC_SIZE bytes:
 *   u8 version | i64 nlink_delta | u64 change | i64 mtime_ns | i64 ctime_ns
 *   A directory's nlink, change, mtime and ctime are the composition of
 *   its K shards (INODE dkey 'a', akeys 'c'+be32 i): nlink = clamp(sum of
 *   nlink_delta), change = sum of change, mtime / ctime = max.  A
 *   namespace mutation reads one shard and writes it back, so two
 *   mutations of one directory conflict only when they pick the same
 *   shard.  Shard 0 is written with the blob on every full inode write.
 *
 * Stripe map blob (version 1), INODE dkey 's' akey 'm':
 *   u8 version | u32 stripe_count | u32 stripe_unit | u32 mirror_count |
 *   u32 entry_count | entry_count x (u16 len | fdb_stripe_ent codec[len])
 *   entry_count == stripe_count * mirror_count, entries stripe-major as
 *   in struct mds_inode.ds_map.  Header and entries are ONE value, so
 *   stripe_map_get and the fused LAYOUTGET read the map in one fetch.
 */

#ifndef DAOS_CODEC_H
#define DAOS_CODEC_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "pnfs_mds.h"
#include "fdb_codec.h"

/** Directory counter shard: u8 version + four 64-bit fields. */
#define DAOS_DIR_SHARD_VERSION  1U
#define DAOS_DIR_SHARD_ENC_SIZE 33U

/** Stripe map blob. */
#define DAOS_STRIPE_MAP_VERSION   1U
#define DAOS_STRIPE_MAP_ENC_FIXED 17U
/** Largest encoded entry: u16 length prefix plus the fdb_stripe_ent maximum. */
#define DAOS_STRIPE_MAP_ENT_MAX   (2U + FDB_STRIPE_ENT_ENC_MAX)
/** Largest blob: a full MDS_MAX_STRIPES x MDS_MAX_MIRRORS map. */
#define DAOS_STRIPE_MAP_ENC_MAX \
    (DAOS_STRIPE_MAP_ENC_FIXED + \
     (uint32_t)MDS_MAX_STRIPES * (uint32_t)MDS_MAX_MIRRORS * DAOS_STRIPE_MAP_ENT_MAX)

_Static_assert(FDB_STRIPE_ENT_ENC_MAX <= 65535U, "stripe entry length is stored in u16");

/* -----------------------------------------------------------------------
 * Directory counter shard
 * ----------------------------------------------------------------------- */

/** One counter shard of a directory (INODE 'a' / 'c'+be32 i). */
struct daos_dir_shard {
    int64_t  nlink_delta; /**< Signed contribution to nlink. */
    uint64_t change;      /**< Contribution to the change counter. */
    int64_t  mtime_ns;    /**< Last mtime stamp written through this shard. */
    int64_t  ctime_ns;    /**< Last ctime stamp written through this shard. */
};

/**
 * Encode @p s into exactly DAOS_DIR_SHARD_ENC_SIZE bytes.
 *
 * @param s    Shard to encode.
 * @param buf  At least DAOS_DIR_SHARD_ENC_SIZE bytes.
 * @param cap  Capacity of @p buf.
 * @param len  Receives DAOS_DIR_SHARD_ENC_SIZE.
 * @return true; false on a NULL argument or a buffer that is too small.
 */
bool daos_dir_shard_encode(const struct daos_dir_shard *s, uint8_t *buf, size_t cap,
                           size_t *len);

/**
 * Decode a shard; the value must be exactly DAOS_DIR_SHARD_ENC_SIZE bytes
 * of the current version.
 *
 * @return true; false on a NULL argument, a wrong version or length
 *         (*out untouched).
 */
bool daos_dir_shard_decode(const uint8_t *buf, size_t len, struct daos_dir_shard *out);

/**
 * Compose a directory inode after reading: overwrite the four dead blob
 * fields of @p io (fdb_codec.h, directory split) from the @p n shards
 * that were present.  nlink is the clamped sum of the deltas, change the
 * sum, mtime and ctime the maxima.  No-op for a non-directory.
 *
 * @param io      Decoded inode blob; updated in place.
 * @param shards  The shards read (any order); may be NULL when @p n is 0.
 * @param n       Number of shards in @p shards.
 */
void daos_dir_shards_compose(struct mds_inode *io, const struct daos_dir_shard *shards,
                             uint32_t n);

/* -----------------------------------------------------------------------
 * Stripe map blob
 * ----------------------------------------------------------------------- */

/**
 * Exact encoded size of the stripe map (@p sc x @p mc entries); 0 when
 * the geometry or an entry is out of bounds.
 */
size_t daos_stripe_map_enc_size(uint32_t sc, uint32_t mc, const struct mds_ds_map_entry *entries);

/**
 * Encode a stripe map.
 *
 * @param sc       stripe_count, 1..MDS_MAX_STRIPES.
 * @param su       stripe_unit, non-zero.
 * @param mc       mirror_count, 1..MDS_MAX_MIRRORS.
 * @param entries  sc * mc entries, stripe-major; nfs_fh_len <= MDS_NFS_FH_MAX.
 * @param buf      Destination of at least daos_stripe_map_enc_size() bytes.
 * @param cap      Capacity of @p buf.
 * @param len      Receives the encoded length.
 * @return true; false when a field is out of bounds or the buffer is too
 *         small (bytes already written are meaningless).
 */
bool daos_stripe_map_encode(uint32_t sc, uint32_t su, uint32_t mc,
                            const struct mds_ds_map_entry *entries, uint8_t *buf, size_t cap,
                            size_t *len);

/**
 * Decode a stripe map blob.  Re-checks the encoder's bounds, requires
 * entry_count == sc * mc and the value to be consumed exactly, and
 * allocates *entries (sc * mc elements; the caller frees it).
 *
 * @return true; false on a malformed value or an allocation failure
 *         (*entries NULL, the other outputs untouched).
 */
bool daos_stripe_map_decode(const uint8_t *buf, size_t len, uint32_t *sc, uint32_t *su,
                            uint32_t *mc, struct mds_ds_map_entry **entries);

#endif /* DAOS_CODEC_H */

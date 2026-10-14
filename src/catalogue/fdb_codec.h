/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * fdb_codec.h -- Value formats of the FoundationDB catalogue backend.
 *
 * Every value is an explicit, versioned, little-endian byte layout;
 * struct bytes are never written raw, so the on-disk format does not
 * depend on the compiler's padding or the host's endianness and can be
 * evolved by bumping the version byte.  Every decoder rejects a short
 * buffer, an oversized buffer, an unknown version and any field outside
 * its documented bound; a rejected value is reported as MDS_ERR_IO by
 * the slot that read it, never silently defaulted.
 *
 * Integer helpers: fdb_le64_put/get and fdb_le32_put/get are the
 * little-endian counterparts of the big-endian key helpers in
 * endian_helpers.h; FoundationDB atomic ADD operates on little-endian
 * integers, so every counter value (META allocators, directory side
 * keys, witness sequence) uses them.
 *
 * Inode blob (version 1), FDB_INODE_ENC_FIXED + inline_fh_len bytes:
 *   u8 version | u8 type | u16 reserved(0) | u32 mode | u32 nlink |
 *   u32 flags | u64 fileid | u64 uid | u64 gid | u64 size |
 *   u64 space_used | i64 atime_sec u32 atime_nsec | i64 mtime_sec
 *   u32 mtime_nsec | i64 ctime_sec u32 ctime_nsec | u64 change |
 *   u64 generation | u64 create_verf | u64 parent_fileid |
 *   u32 synth_suid | u32 synth_sgid | u32 stripe_count |
 *   u32 stripe_unit | u32 mirror_count | u32 inline_ds_id |
 *   u32 inline_fh_len | u8 inline_fh[inline_fh_len]
 * ds_map is never serialised (decoded inodes carry ds_map = NULL).
 *
 * Directory split: a directory's nlink, change, mtime and ctime live
 * in the INODE side keys (fdb_keys.h) so CREATE/REMOVE can advance them
 * with blind atomic ADD / set instead of rewriting the blob.  The blob
 * copies of those four fields are DEAD: fdb_inode_split zeroes them
 * before the blob is encoded and fdb_inode_compose overwrites them
 * from the side keys after the blob is decoded.  Files keep every
 * field in the blob.
 */

#ifndef FDB_CODEC_H
#define FDB_CODEC_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

#include "pnfs_mds.h"

/** Current inode blob version. */
#define FDB_INODE_VERSION     1U
/** Bytes before the variable-length inline file handle. */
#define FDB_INODE_ENC_FIXED   152U
/** Largest encoded inode (fixed part + MDS_NFS_FH_MAX). */
#define FDB_INODE_ENC_MAX     (FDB_INODE_ENC_FIXED + MDS_NFS_FH_MAX)

/** DIRENT value: u8 version | u64 child_fileid | u8 type | u64 seq. */
#define FDB_DIRENT_VERSION    1U
#define FDB_DIRENT_ENC_SIZE   18U

/** DIRENT_SEQ value: u8 version | u64 child_fileid | u8 type | name bytes. */
#define FDB_DIRENT_SEQ_VERSION   1U
#define FDB_DIRENT_SEQ_ENC_FIXED 10U
#define FDB_DIRENT_SEQ_ENC_MAX   (FDB_DIRENT_SEQ_ENC_FIXED + MDS_MAX_NAME)

/** STRIPE_HDR value: u8 version | u32 stripe_count | u32 stripe_unit |
 *  u32 mirror_count. */
#define FDB_STRIPE_HDR_VERSION  1U
#define FDB_STRIPE_HDR_ENC_SIZE 13U

/** STRIPE_ENT value: u8 version | u32 ds_id | u32 synth_suid |
 *  u32 synth_sgid | u32 nfs_fh_len | u8 nfs_fh[nfs_fh_len]. */
#define FDB_STRIPE_ENT_VERSION   1U
#define FDB_STRIPE_ENT_ENC_FIXED 17U
#define FDB_STRIPE_ENT_ENC_MAX   (FDB_STRIPE_ENT_ENC_FIXED + MDS_NFS_FH_MAX)

/** GC value: u8 version | u64 fileid | u32 ds_id | u32 owner_mds_id |
 *  u32 sweep_hint | u32 nfs_fh_len | u8 nfs_fh[nfs_fh_len]. */
#define FDB_GC_VERSION   1U
#define FDB_GC_ENC_FIXED 25U
#define FDB_GC_ENC_MAX   (FDB_GC_ENC_FIXED + MDS_NFS_FH_MAX)

_Static_assert(FDB_INODE_ENC_MAX < 100000U && FDB_GC_ENC_MAX < 100000U,
               "FoundationDB values must stay under 100 KB");

/* -----------------------------------------------------------------------
 * Contract shared by every encoder and decoder below.
 *
 * Encoders (fdb_*_encode): write the value's bytes into the caller's
 * @p buf of @p cap bytes and set *len to the encoded length; false when
 * an argument is NULL, a field is outside its documented bound or the
 * buffer is too small (then *len is untouched; bytes already written
 * to @p buf are meaningless).  Decoders (fdb_*_decode): read exactly
 * @p len bytes of @p buf into *out; false on a NULL argument, a wrong
 * version, a length that is not exactly the layout's, or a field
 * outside its bound (then *out is untouched).  No function allocates,
 * keeps a pointer to its arguments (fdb_slot_decode's *reply, which
 * points into @p buf, is the one documented exception) or touches
 * shared state; all are thread-safe.
 * ----------------------------------------------------------------------- */

/* -----------------------------------------------------------------------
 * Little-endian integers
 * ----------------------------------------------------------------------- */

/** Store @p v as 8 little-endian bytes at @p dst (caller-owned, >= 8 bytes). */
void     fdb_le64_put(uint8_t *dst, uint64_t v);
/** Load the 8 little-endian bytes at @p src as a u64. */
uint64_t fdb_le64_get(const uint8_t *src);
/** Store @p v as 4 little-endian bytes at @p dst (caller-owned, >= 4 bytes). */
void     fdb_le32_put(uint8_t *dst, uint32_t v);
/** Load the 4 little-endian bytes at @p src as a u32. */
uint32_t fdb_le32_get(const uint8_t *src);

/**
 * Decode a counter value: exactly 8 bytes.  A shorter or longer value
 * is corrupt (returns false; *out untouched).
 */
bool fdb_le64_decode(const uint8_t *src, size_t len, uint64_t *out);

/** Nanoseconds since the epoch of @p ts as a signed 64-bit value. */
int64_t fdb_ts_to_ns(struct timespec ts);

/** Inverse of fdb_ts_to_ns (tv_nsec normalised to [0, 1e9)). */
struct timespec fdb_ns_to_ts(int64_t ns);

/* -----------------------------------------------------------------------
 * Inode
 * ----------------------------------------------------------------------- */

/**
 * Encode @p ino into @p buf.
 *
 * @param ino  Inode; inline_fh_len above MDS_NFS_FH_MAX is rejected.
 * @param buf  At least FDB_INODE_ENC_MAX bytes.
 * @param cap  Capacity of @p buf.
 * @param len  Receives the encoded length.
 * @return true on success; false when a field is out of bounds or the
 *         buffer is too small (nothing is written then).
 */
bool fdb_inode_encode(const struct mds_inode *ino, uint8_t *buf, size_t cap,
                      size_t *len);

/**
 * Decode an inode blob.  Rejects a wrong version, a buffer shorter than
 * the fixed part, an inline_fh_len above MDS_NFS_FH_MAX, a length that
 * is not exactly fixed + inline_fh_len, a type outside enum
 * mds_file_type and a tv_nsec at or above 1e9.  On success *out is
 * fully written with ds_map = NULL.
 */
bool fdb_inode_decode(const uint8_t *buf, size_t len, struct mds_inode *out);

/** True when @p type is a directory (the split applies). */
bool fdb_inode_is_dir(const struct mds_inode *ino);

/**
 * Directory side values as they are stored (LE u64 each).  nlink and
 * change are plain counters; mtime_ns / ctime_ns are fdb_ts_to_ns of
 * the timestamps, stored as two's complement.
 */
struct fdb_dir_counters {
    uint64_t nlink;
    uint64_t change;
    uint64_t mtime_ns;
    uint64_t ctime_ns;
};

/**
 * Split a directory inode for writing: fill @p counters from @p in and
 * write into @p blob_copy a copy of @p in whose nlink, change, mtime and
 * ctime are zeroed (dead).  For a non-directory @p blob_copy is an
 * unmodified copy and @p counters is zeroed.
 */
void fdb_inode_split(const struct mds_inode *in, struct mds_inode *blob_copy,
                     struct fdb_dir_counters *counters);

/**
 * Compose a directory inode after reading: overwrite the four dead
 * blob fields of @p io from @p counters.  nlink is clamped to
 * [0, UINT32_MAX] (a counter driven below zero reads as 0).  No-op
 * for a non-directory.
 */
void fdb_inode_compose(struct mds_inode *io, const struct fdb_dir_counters *counters);

/* -----------------------------------------------------------------------
 * Dirent and dirent-sequence index
 * ----------------------------------------------------------------------- */

struct fdb_dirent_val {
    uint64_t child_fileid;
    uint64_t seq;       /**< The entry's READDIR cookie / DIRENT_SEQ key. */
    uint8_t  type;      /**< enum mds_file_type of the child. */
};

/** Encode into exactly FDB_DIRENT_ENC_SIZE bytes (cap must allow it). */
bool fdb_dirent_encode(const struct fdb_dirent_val *v, uint8_t *buf, size_t cap,
                       size_t *len);
/** Decode; exactly FDB_DIRENT_ENC_SIZE bytes of the current version. */
bool fdb_dirent_decode(const uint8_t *buf, size_t len, struct fdb_dirent_val *out);

struct fdb_dirent_seq_val {
    uint64_t child_fileid;
    uint8_t  type;
    char     name[MDS_MAX_NAME + 1]; /**< NUL-terminated on decode. */
};

/** Encode; the name must be 1..MDS_MAX_NAME bytes. */
bool fdb_dirent_seq_encode(const struct fdb_dirent_seq_val *v, uint8_t *buf, size_t cap,
                           size_t *len);
/** Decode; rejects an empty or over-long name. */
bool fdb_dirent_seq_decode(const uint8_t *buf, size_t len, struct fdb_dirent_seq_val *out);

/* -----------------------------------------------------------------------
 * Stripe map rows
 * ----------------------------------------------------------------------- */

struct fdb_stripe_hdr_val {
    uint32_t stripe_count;
    uint32_t stripe_unit;
    uint32_t mirror_count;
};

/** Encode; stripe_count in 1..MDS_MAX_STRIPES, mirror_count in
 *  1..MDS_MAX_MIRRORS, stripe_unit non-zero. */
bool fdb_stripe_hdr_encode(const struct fdb_stripe_hdr_val *v, uint8_t *buf, size_t cap,
                           size_t *len);
/** Decode exactly FDB_STRIPE_HDR_ENC_SIZE bytes; the encoder's bounds
 *  are re-checked on the stored fields. */
bool fdb_stripe_hdr_decode(const uint8_t *buf, size_t len, struct fdb_stripe_hdr_val *out);

/** Encode one struct mds_ds_map_entry (nfs_fh_len <= MDS_NFS_FH_MAX). */
bool fdb_stripe_ent_encode(const struct mds_ds_map_entry *e, uint8_t *buf, size_t cap,
                           size_t *len);
/** Decode one stripe entry; the length must be exactly fixed part +
 *  nfs_fh_len, nfs_fh_len at most MDS_NFS_FH_MAX. */
bool fdb_stripe_ent_decode(const uint8_t *buf, size_t len, struct mds_ds_map_entry *out);

/* -----------------------------------------------------------------------
 * GC queue row
 * ----------------------------------------------------------------------- */

/** Encode a GC row; gc_seq is the key, not part of the value. */
bool fdb_gc_encode(const struct mds_gc_entry *e, uint8_t *buf, size_t cap, size_t *len);
/** Decode; the caller sets out->gc_seq from the key. */
bool fdb_gc_decode(const uint8_t *buf, size_t len, struct mds_gc_entry *out);

/* -----------------------------------------------------------------------
 * ext track (catalogue_fdb_ext.c): value formats of the extended
 * authority tables.  Same conventions as above: explicit little-endian
 * layouts, one version byte, every decoder consumes the value exactly
 * and rejects any field outside its bound.  Append-only section.
 *
 * REMOVE_PENDING (version 1), FDB_REMOVE_PENDING_ENC_FIXED + name bytes:
 *   u8 version | u64 dir_fileid | u64 child_fileid | u64 child_generation |
 *   u64 enqueued_ns | u64 claim_boot | u64 claim_expires_ns |
 *   u32 claim_mds_id | u32 retries | name[1..MDS_MAX_NAME]
 *   remove_seq is the key, not part of the value.
 * DS (version 1), FDB_DS_INFO_ENC_FIXED + the three string bodies:
 *   u8 version | u32 ds_id | u32 state | u32 tier | u64 total_bytes |
 *   u64 used_bytes | u16 port | u16 tcp_port | u16 rdma_port | u8 mode |
 *   u8 transport | u32 capabilities | u32 weight | u16 addr_len |
 *   u16 host_len | u16 export_len | addr | host | export_path
 *   Each string is stored without its NUL and bounded by its array
 *   (MDS_DS_ADDR_MAX / MDS_DS_HOST_MAX / MDS_DS_EXPORT_MAX minus one).
 * DS_PROVISION (version 1): u8 version | u64 epoch | u8 secret_len |
 *   secret[0..FDB_DS_SECRET_MAX]
 * QUOTA_RULE (version 1): u8 version | u64 hard_bytes | u64 soft_bytes |
 *   u64 hard_inodes | u64 soft_inodes | u32 grace_sec   (the struct's
 *   padding word is never stored)
 * QUOTA_USAGE (version 1): u8 version | u64 used_bytes | u64 used_inodes |
 *   i64 grace_start_bytes | i64 grace_start_inodes
 * EXT_DIRENT (version 1): u8 version | u32 owner_mds_id |
 *   u64 target_fileid | u8 target_type | u64 anchor_id
 * LINK_ANCHOR (version 1), FDB_LINK_ANCHOR_ENC_FIXED + name bytes:
 *   u8 version | u32 remote_mds_id | u64 parent_fileid |
 *   name[0..MDS_MAX_NAME]   (an empty name is legal, as on memdb)
 * SHARD_FILEID: LE u32 shard_id, exactly 4 bytes (fdb_le32_decode).
 * ----------------------------------------------------------------------- */

#include "quota.h"

#define FDB_REMOVE_PENDING_VERSION   1U
#define FDB_REMOVE_PENDING_ENC_FIXED 57U
#define FDB_REMOVE_PENDING_ENC_MAX   (FDB_REMOVE_PENDING_ENC_FIXED + MDS_MAX_NAME)

#define FDB_DS_INFO_VERSION   1U
#define FDB_DS_INFO_ENC_FIXED 51U
#define FDB_DS_INFO_ENC_MAX   (FDB_DS_INFO_ENC_FIXED + (MDS_DS_ADDR_MAX - 1U) + \
                               (MDS_DS_HOST_MAX - 1U) + (MDS_DS_EXPORT_MAX - 1U))

/** Provisioning secrets are bounded like the RonDB column and memdb. */
#define FDB_DS_SECRET_MAX            64U
#define FDB_DS_PROVISION_VERSION     1U
#define FDB_DS_PROVISION_ENC_FIXED   10U
#define FDB_DS_PROVISION_ENC_MAX     (FDB_DS_PROVISION_ENC_FIXED + FDB_DS_SECRET_MAX)

#define FDB_QUOTA_RULE_VERSION   1U
#define FDB_QUOTA_RULE_ENC_SIZE  37U
#define FDB_QUOTA_USAGE_VERSION  1U
#define FDB_QUOTA_USAGE_ENC_SIZE 33U

#define FDB_EXT_DIRENT_VERSION   1U
#define FDB_EXT_DIRENT_ENC_SIZE  22U

#define FDB_LINK_ANCHOR_VERSION   1U
#define FDB_LINK_ANCHOR_ENC_FIXED 13U
#define FDB_LINK_ANCHOR_ENC_MAX   (FDB_LINK_ANCHOR_ENC_FIXED + MDS_MAX_NAME)

_Static_assert(FDB_DS_INFO_ENC_MAX < 100000U && FDB_REMOVE_PENDING_ENC_MAX < 100000U,
               "FoundationDB values must stay under 100 KB");
_Static_assert(FDB_DS_SECRET_MAX <= 255U, "secret_len is stored in one byte");
_Static_assert(MDS_DS_ADDR_MAX <= 65536U && MDS_DS_HOST_MAX <= 65536U &&
               MDS_DS_EXPORT_MAX <= 65536U, "DS string lengths are stored in u16");

/** Decode a LE u32 value: exactly 4 bytes (SHARD_FILEID). */
bool fdb_le32_decode(const uint8_t *src, size_t len, uint32_t *out);

/** Encode a manifest row; remove_seq is the key.  The name must be
 *  1..MDS_MAX_NAME bytes. */
bool fdb_remove_pending_encode(const struct mds_remove_pending_entry *e, uint8_t *buf,
                               size_t cap, size_t *len);
/** Decode; the caller sets out->remove_seq from the key. */
bool fdb_remove_pending_decode(const uint8_t *buf, size_t len,
                               struct mds_remove_pending_entry *out);

/** Encode a DS registry row (every field of struct mds_ds_info). */
bool fdb_ds_info_encode(const struct mds_ds_info *info, uint8_t *buf, size_t cap, size_t *len);
/** Decode a DS registry row; the three strings come back NUL-terminated
 *  within their arrays. */
bool fdb_ds_info_decode(const uint8_t *buf, size_t len, struct mds_ds_info *out);

struct fdb_ds_provision_val {
    uint64_t epoch;
    uint32_t secret_len;                  /**< 0..FDB_DS_SECRET_MAX. */
    uint8_t  secret[FDB_DS_SECRET_MAX];
};

/** Encode a provisioning row; secret_len above FDB_DS_SECRET_MAX is rejected. */
bool fdb_ds_provision_encode(const struct fdb_ds_provision_val *v, uint8_t *buf, size_t cap,
                             size_t *len);
/** Decode; the length must be exactly fixed part + secret_len. */
bool fdb_ds_provision_decode(const uint8_t *buf, size_t len, struct fdb_ds_provision_val *out);

/** Encode a quota rule (fixed FDB_QUOTA_RULE_ENC_SIZE bytes). */
bool fdb_quota_rule_encode(const struct mds_quota_rule *r, uint8_t *buf, size_t cap,
                           size_t *len);
/** Decode exactly FDB_QUOTA_RULE_ENC_SIZE bytes; the struct's padding
 *  word is left zero. */
bool fdb_quota_rule_decode(const uint8_t *buf, size_t len, struct mds_quota_rule *out);

/** Encode a quota usage row (fixed FDB_QUOTA_USAGE_ENC_SIZE bytes). */
bool fdb_quota_usage_encode(const struct mds_quota_usage *u, uint8_t *buf, size_t cap,
                            size_t *len);
/** Decode exactly FDB_QUOTA_USAGE_ENC_SIZE bytes. */
bool fdb_quota_usage_decode(const uint8_t *buf, size_t len, struct mds_quota_usage *out);

struct fdb_ext_dirent_val {
    uint64_t target_fileid;
    uint64_t anchor_id;
    uint32_t owner_mds_id;
    uint8_t  target_type;
};

/** Encode a cross-shard dirent (fixed FDB_EXT_DIRENT_ENC_SIZE bytes). */
bool fdb_ext_dirent_encode(const struct fdb_ext_dirent_val *v, uint8_t *buf, size_t cap,
                           size_t *len);
/** Decode exactly FDB_EXT_DIRENT_ENC_SIZE bytes. */
bool fdb_ext_dirent_decode(const uint8_t *buf, size_t len, struct fdb_ext_dirent_val *out);

struct fdb_link_anchor_val {
    uint64_t parent_fileid;
    uint32_t remote_mds_id;
    char     name[MDS_MAX_NAME + 1];      /**< NUL-terminated; may be empty. */
};

/** Encode a link anchor; the name may be empty, at most MDS_MAX_NAME bytes. */
bool fdb_link_anchor_encode(const struct fdb_link_anchor_val *v, uint8_t *buf, size_t cap,
                            size_t *len);
/** Decode; the name (fixed part to the end of the value) comes back
 *  NUL-terminated. */
bool fdb_link_anchor_decode(const uint8_t *buf, size_t len, struct fdb_link_anchor_val *out);

/* End of ext track section. */

/* =======================================================================
 * fdb-coord track -- coordination and cluster row formats
 *
 * Value formats of the tables the coordination slots
 * (catalogue_fdb_coord.c) and the cluster slots (catalogue_fdb_cluster.c)
 * write.  Same conventions as above: explicit little-endian layout, one
 * version byte, every field bounded, the value consumed exactly.  Key
 * components (stateid, fileid, clientid, session id, ...) are never
 * repeated in the value; a decoder takes them from the caller so the
 * reconstructed row is complete.
 * ======================================================================= */

#include "mds_coordination.h"

/** Unique DS ids one layout row may carry (== MDS_LAYOUT_DS_ID_MAX). */
#define FDB_LAYOUT_DS_MAX ((uint32_t)MDS_MAX_STRIPES * (uint32_t)MDS_MAX_MIRRORS)

/** LAYOUT_STATE value: u8 version | u64 clientid | u64 fileid | u32 iomode |
 *  u64 offset | u64 length | u32 seqid | u32 grant_owner_mds_id |
 *  u32 ds_count | u32 ds_ids[ds_count]. */
#define FDB_LAYOUT_VERSION   1U
#define FDB_LAYOUT_ENC_FIXED 49U
#define FDB_LAYOUT_ENC_MAX   (FDB_LAYOUT_ENC_FIXED + 4U * FDB_LAYOUT_DS_MAX)

/** OPEN value: u8 version | u32 seqid | u64 clientid | u64 fileid |
 *  u32 share_access | u32 share_deny | u32 open_owner_len |
 *  u8 open_owner[len] | u32 owner_mds_id | u64 owner_boot_epoch. */
#define FDB_OPEN_VERSION     1U
#define FDB_OPEN_ENC_FIXED   45U
#define FDB_OPEN_ENC_MAX     (FDB_OPEN_ENC_FIXED + 128U)

/** LOCK value: u8 version | u64 offset | u64 length | u32 lock_type |
 *  u64 clientid | u32 owner_len | u8 owner[len] | u8 stateid_other[12] |
 *  u32 seqid | u8 open_stateid_other[12] | u32 owner_mds_id |
 *  u64 owner_boot_epoch. */
#define FDB_LOCK_VERSION     1U
#define FDB_LOCK_ENC_FIXED   73U
#define FDB_LOCK_ENC_MAX     (FDB_LOCK_ENC_FIXED + 128U)

/** DELEG value: u8 version | u32 seqid | u64 clientid | u64 fileid |
 *  u32 deleg_type | u32 owner_mds_id | u64 owner_boot_epoch |
 *  u64 grant_time_ns | u8 recall_pending. */
#define FDB_DELEG_VERSION    1U
#define FDB_DELEG_ENC_SIZE   46U

/** CLIENT value: u8 version | u32 co_ownerid_len | u8 co_ownerid[len] |
 *  u8 verifier[8] | u8 confirmed | u32 owner_mds_id |
 *  u64 owner_boot_epoch | u64 lease_renewed_ns. */
#define FDB_CLIENT_VERSION   1U
#define FDB_CLIENT_ENC_FIXED 34U
#define FDB_CLIENT_ENC_MAX   (FDB_CLIENT_ENC_FIXED + 1024U)

/** SESSION value: u8 version | u64 clientid | u32 num_slots | u32 cb_prog |
 *  u32 cb_sec_flavor | u32 owner_mds_id | u64 owner_boot_epoch |
 *  u64 created_ns. */
#define FDB_SESSION_VERSION  1U
#define FDB_SESSION_ENC_SIZE 41U

/** SLOT (DRC) value: u8 version | u32 seq_id | u64 last_used_ns |
 *  u32 reply_len | u8 reply[reply_len].  The reply is bounded like the
 *  RonDB blob read-back. */
#define FDB_SLOT_VERSION     1U
#define FDB_SLOT_ENC_FIXED   17U
#define FDB_SLOT_REPLY_MAX   65536U
#define FDB_SLOT_ENC_MAX     (FDB_SLOT_ENC_FIXED + FDB_SLOT_REPLY_MAX)

/** RECOVERY value: u8 version | u32 owner_mds_id | u64 owner_boot_epoch |
 *  u32 co_ownerid_len | u8 co_ownerid[len] | u8 verifier[8]. */
#define FDB_RECOVERY_VERSION   1U
#define FDB_RECOVERY_ENC_FIXED 25U
#define FDB_RECOVERY_CO_MAX    1024U
#define FDB_RECOVERY_ENC_MAX   (FDB_RECOVERY_ENC_FIXED + FDB_RECOVERY_CO_MAX)

/** JOURNAL value: u8 version | u8 state | u32 remote_mds_id |
 *  u64 src_parent_fileid | u64 dst_parent_fileid | u64 src_child_fileid |
 *  u64 created_at_ns | u16 src_name_len | u16 dst_name_len |
 *  u32 payload_len | src_name | dst_name | payload.  (txn_id and role
 *  are the key.) */
#define FDB_JOURNAL_VERSION   1U
#define FDB_JOURNAL_ENC_FIXED 46U
#define FDB_JOURNAL_ENC_MAX   (FDB_JOURNAL_ENC_FIXED + 2U * MDS_MAX_NAME + \
                               MDS_COORD_JOURNAL_PAYLOAD_MAX)

/** NODE_REGISTRY value: u8 version | u64 boot_epoch | u16 nfs_port |
 *  u16 grpc_port | u8 state | u64 last_heartbeat_ns | u64 witness_epoch |
 *  u8 sw_len | u8 sw_version[sw_len] | u16 host_len | u8 hostname[host_len].
 *  witness_epoch is the WITNESS key epoch (fdb_txn.h) of the process
 *  that registered the row; the open-time witness sweep of that mds_id
 *  clears only rows strictly below it.  Version 1 (keyspaces stamped
 *  with schema 1) had no witness_epoch and is rejected. */
#define FDB_NODE_VERSION      2U
#define FDB_NODE_ENC_FIXED    33U
#define FDB_NODE_SW_MAX       63U
#define FDB_NODE_HOST_MAX     255U
#define FDB_NODE_ENC_MAX      (FDB_NODE_ENC_FIXED + FDB_NODE_SW_MAX + FDB_NODE_HOST_MAX)

/** PARTITION_MAP value: u8 version | u32 owner_mds_id | u8 state |
 *  u16 path_len | u8 subtree_path[path_len]. */
#define FDB_PARTITION_VERSION   1U
#define FDB_PARTITION_ENC_FIXED 8U
#define FDB_PARTITION_PATH_MAX  ((uint32_t)MDS_MAX_PATH - 1U)
#define FDB_PARTITION_ENC_MAX   (FDB_PARTITION_ENC_FIXED + FDB_PARTITION_PATH_MAX)

_Static_assert(FDB_LAYOUT_ENC_MAX < 100000U && FDB_SLOT_ENC_MAX < 100000U &&
               FDB_JOURNAL_ENC_MAX < 100000U && FDB_PARTITION_ENC_MAX < 100000U &&
               FDB_CLIENT_ENC_MAX < 100000U,
               "FoundationDB values must stay under 100 KB");

/* --- Layout state ------------------------------------------------------ */

/** Fixed part of a LAYOUT_STATE row (the DS list travels separately). */
struct fdb_layout_hdr {
    uint64_t clientid;
    uint64_t fileid;
    uint64_t offset;
    uint64_t length;
    uint32_t iomode;
    uint32_t seqid;
    uint32_t grant_owner_mds_id;
    uint32_t ds_count;
};

/**
 * Encode a layout row.  h->ds_count ids are taken from @p ds_ids (which
 * may be NULL only when ds_count is 0); ds_count above FDB_LAYOUT_DS_MAX
 * is rejected.
 */
bool fdb_layout_encode(const struct fdb_layout_hdr *h, const uint32_t *ds_ids, uint8_t *buf,
                       size_t cap, size_t *len);

/**
 * Decode a layout row into @p h and, when @p ds_ids is not NULL, copy
 * the DS list into it (rejected when ds_count exceeds @p ds_cap).  With
 * @p ds_ids NULL the list is validated but not copied.
 */
bool fdb_layout_decode(const uint8_t *buf, size_t len, struct fdb_layout_hdr *h,
                       uint32_t *ds_ids, uint32_t ds_cap);

/** True when the encoded layout row @p buf lists @p ds_id; false for a
 *  malformed value. */
bool fdb_layout_ds_contains(const uint8_t *buf, size_t len, uint32_t ds_id);

/* --- Shared protocol state --------------------------------------------- */

/** Encode the non-key fields of an open row (open_owner_len <= 128). */
bool fdb_open_encode(const struct mds_coord_open_row *r, uint8_t *buf, size_t cap, size_t *len);
/** Decode; @p stateid_other (the key) completes the row. */
bool fdb_open_decode(const uint8_t *buf, size_t len, const uint8_t stateid_other[12],
                     struct mds_coord_open_row *out);

/** Encode the non-key fields of a lock row (owner_len <= 128). */
bool fdb_lock_encode(const struct mds_coord_lock_row *r, uint8_t *buf, size_t cap, size_t *len);
/** Decode; (fileid, lock_id) is the key. */
bool fdb_lock_decode(const uint8_t *buf, size_t len, uint64_t fileid, uint64_t lock_id,
                     struct mds_coord_lock_row *out);

/** Encode the non-key fields of a delegation row (fixed FDB_DELEG_ENC_SIZE). */
bool fdb_deleg_encode(const struct mds_coord_deleg_row *r, uint8_t *buf, size_t cap,
                      size_t *len);
/** Decode; @p stateid_other (the key) completes the row. */
bool fdb_deleg_decode(const uint8_t *buf, size_t len, const uint8_t stateid_other[12],
                      struct mds_coord_deleg_row *out);

/** Encode a client row (co_ownerid_len <= 1024). */
bool fdb_client_encode(const struct mds_coord_client_row *r, uint8_t *buf, size_t cap,
                       size_t *len);
/** Decode; @p clientid (the key) completes the row.  A confirmed byte
 *  other than 0 or 1 is rejected. */
bool fdb_client_decode(const uint8_t *buf, size_t len, uint64_t clientid,
                       struct mds_coord_client_row *out);

/** Encode the non-key fields of a session row (fixed FDB_SESSION_ENC_SIZE). */
bool fdb_session_encode(const struct mds_coord_session_row *r, uint8_t *buf, size_t cap,
                        size_t *len);
/** Decode; @p session_id (the key) completes the row. */
bool fdb_session_decode(const uint8_t *buf, size_t len, const uint8_t session_id[16],
                        struct mds_coord_session_row *out);

/** Fixed part of a DRC slot row; the reply bytes travel separately. */
struct fdb_slot_hdr {
    uint32_t seq_id;
    uint64_t last_used_ns;
    uint32_t reply_len;
};

/** Encode; @p reply may be NULL only when h->reply_len is 0; reply_len
 *  above FDB_SLOT_REPLY_MAX is rejected. */
bool fdb_slot_encode(const struct fdb_slot_hdr *h, const uint8_t *reply, uint8_t *buf,
                     size_t cap, size_t *len);
/** Decode; *reply points INTO @p buf (h->reply_len bytes; NULL when 0). */
bool fdb_slot_decode(const uint8_t *buf, size_t len, struct fdb_slot_hdr *h,
                     const uint8_t **reply);

/* --- Client recovery --------------------------------------------------- */

struct fdb_recovery_val {
    uint32_t owner_mds_id;
    uint64_t owner_boot_epoch;
    uint32_t co_ownerid_len;
    uint8_t  co_ownerid[FDB_RECOVERY_CO_MAX];
    uint8_t  verifier[8];
};

/** Encode a recovery row; co_ownerid_len above FDB_RECOVERY_CO_MAX is
 *  rejected.  clientid is the key, not part of the value. */
bool fdb_recovery_encode(const struct fdb_recovery_val *v, uint8_t *buf, size_t cap,
                         size_t *len);
/** Decode; the length must be exactly fixed part + co_ownerid_len. */
bool fdb_recovery_decode(const uint8_t *buf, size_t len, struct fdb_recovery_val *out);

/* --- 2PC journal ------------------------------------------------------- */

/** Encode the non-key fields (payload_len <= MDS_COORD_JOURNAL_PAYLOAD_MAX,
 *  names NUL-terminated within their arrays). */
bool fdb_journal_encode(const struct mds_coord_journal_record *rec, uint8_t *buf, size_t cap,
                        size_t *len);
/** Decode; (txn_id, role) is the key. */
bool fdb_journal_decode(const uint8_t *buf, size_t len, uint64_t txn_id, uint8_t role,
                        struct mds_coord_journal_record *out);

/* --- Cluster: node registry and partition map -------------------------- */

struct fdb_node_val {
    uint64_t boot_epoch;
    uint64_t last_heartbeat_ns;
    uint64_t witness_epoch;                /**< Registrant's witness epoch; 0 = unknown. */
    uint16_t nfs_port;
    uint16_t grpc_port;
    uint8_t  state;                        /**< 0 active, 1 standby, 2 draining. */
    char     sw_version[FDB_NODE_SW_MAX + 1];
    char     hostname[FDB_NODE_HOST_MAX + 1];
};

/** Encode; hostname 1..FDB_NODE_HOST_MAX bytes, sw_version 0..FDB_NODE_SW_MAX. */
bool fdb_node_encode(const struct fdb_node_val *v, uint8_t *buf, size_t cap, size_t *len);
/** Decode a registry row; both strings come back NUL-terminated.  A row
 *  of the previous layout (version 1) is rejected. */
bool fdb_node_decode(const uint8_t *buf, size_t len, struct fdb_node_val *out);

struct fdb_partition_val {
    uint32_t owner_mds_id;
    uint8_t  state;
    char     subtree_path[MDS_MAX_PATH];  /**< NUL-terminated. */
};

/** Encode; subtree_path 1..FDB_PARTITION_PATH_MAX bytes. */
bool fdb_partition_encode(const struct fdb_partition_val *v, uint8_t *buf, size_t cap,
                          size_t *len);
/** Decode; the path comes back NUL-terminated, never empty. */
bool fdb_partition_decode(const uint8_t *buf, size_t len, struct fdb_partition_val *out);

/* ===================== end of fdb-coord track section ==================== */

#endif /* FDB_CODEC_H */

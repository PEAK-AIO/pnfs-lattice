/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * fdb_keys.h -- FoundationDB catalogue key space.
 *
 * Every key the backend writes is
 *
 *     <prefix> <type byte> <components...>
 *
 * where <prefix> is the deployment's configurable byte string
 * (mds.conf fdb_key_prefix, empty by default), the type byte selects
 * the table (enum fdb_key_type, append-only) and the components are
 * fixed-width big-endian integers followed, where a table is keyed by a
 * name, by the raw name bytes.  Big-endian fixed width makes a byte-wise
 * key order a numeric order, so a range over one parent / fileid / id
 * is one contiguous key range.  A name is always the LAST component;
 * where a layout needs more components after a name the name is
 * NUL-terminated (names never contain NUL, MDS_MAX_NAME bounds them).
 *
 * Layouts (value formats in fdb_codec.h; "empty" = zero-length value):
 *
 *   META            + u8 sub                         -> LE u64
 *     sub FDB_META_SCHEMA_VERSION  schema stamp, checked at open
 *         FDB_META_FILEID          fileid allocator (ADD target)
 *         FDB_META_GC_SEQ          GC queue sequence allocator
 *         FDB_META_REMOVE_SEQ      remove_pending sequence allocator
 *         FDB_META_COOKIE_SEQ      READDIR cookie / DIRENT_SEQ allocator
 *   INODE           + be64 fileid + u8 part
 *     part FDB_INODE_PART_BLOB     encoded struct mds_inode (fdb_codec)
 *          FDB_INODE_PART_NLINK    LE u64, DIRECTORIES ONLY (ADD target)
 *          FDB_INODE_PART_CHANGE   LE u64, directories only (ADD target)
 *          FDB_INODE_PART_MTIME    LE i64 ns since epoch, directories only
 *          FDB_INODE_PART_CTIME    LE i64 ns since epoch, directories only
 *     A directory inode is the blob plus the four side keys and is read
 *     with ONE range read over INODE + fileid; the side keys are
 *     authoritative and the blob's copies of those fields are dead
 *     (fdb_inode_split / fdb_inode_compose).  A file is the blob alone.
 *   DIRENT          + be64 parent + name             -> fdb_dirent codec
 *   DIRENT_SEQ      + be64 parent + be64 seq         -> fdb_dirent_seq codec
 *     seq is the entry's READDIR cookie, minted from the keyspace-wide
 *     COOKIE_SEQ allocator when the entry is created or rebound
 *     (fdb_backend_alloc_id): unique across the keyspace, hence unique
 *     per directory by construction, one per dirent (two hard links in
 *     one directory never share a cookie).  READDIR walks this table,
 *     so its order is global insertion order -- the order cookies were
 *     minted, which is insertion order up to the allocator's
 *     thread-local batch reservation -- never name order; a rename or
 *     an overwriting put re-mints and moves the entry to the end.  The
 *     two rows are always written and cleared together.
 *   STRIPE_HDR      + be64 fileid                    -> fdb_stripe_hdr codec
 *   STRIPE_ENT      + be64 fileid + be32 ordinal     -> fdb_stripe_ent codec
 *   XATTR           + be64 fileid + name             -> raw value bytes
 *   INLINE          + be64 fileid                    -> raw data bytes
 *   GC              + be64 gc_seq                    -> fdb_gc codec
 *   GC_BY_OWNER     + be32 owner_mds_id + be64 gc_seq -> empty
 *     owner_mds_id is the row's owner (0 = legacy).  Written and
 *     cleared with the GC row in the same transaction, never alone.
 *     An MDS with an id drains the rows of its own id and of owner 0
 *     and finds them by paging these two prefixes instead of walking
 *     the GC table, so a dead peer's backlog -- older, hence sorted
 *     first -- cannot starve it (catalogue_fdb_ext.c).  Schema version
 *     2 (catalogue_fdb.h): a version-1 keyspace holds GC rows without
 *     index keys and must be re-initialised.
 *   REMOVE_PENDING  + be64 remove_seq                -> fdb_remove_pending codec
 *   LAYOUT_STATE    + stateid_other[12]              -> fdb_layout codec
 *   LAYOUT_BY_FILE  + be64 fileid + stateid_other[12]        -> empty
 *   LAYOUT_BY_CLIENT+ be64 clientid + stateid_other[12]      -> empty
 *   DS_LAYOUT_IDX   + be32 ds_id + be64 clientid + be64 fileid
 *                   + stateid_other[12]                      -> empty
 *   OPEN            + stateid_other[12]              -> fdb_open codec
 *   OPEN_BY_FILE    + be64 fileid + stateid_other[12]        -> empty
 *   OPEN_BY_CLIENT  + be64 clientid + stateid_other[12]      -> empty
 *   LOCK            + be64 fileid + be64 lock_id     -> fdb_lock codec
 *   LOCK_BY_OWNER   + be64 clientid + be32 owner_len + owner bytes
 *                   + be64 fileid + be64 lock_id             -> empty
 *   DELEG           + stateid_other[12]              -> fdb_deleg codec
 *   DELEG_BY_FILE   + be64 fileid + stateid_other[12]        -> empty
 *   DELEG_BY_CLIENT + be64 clientid + stateid_other[12]      -> empty
 *   CLIENT          + be64 clientid                  -> fdb_client codec
 *   SESSION         + session_id[16]                 -> fdb_session codec
 *   SESSION_BY_CLIENT + be64 clientid + session_id[16]       -> empty
 *   SLOT            + session_id[16] + be32 slot_id  -> fdb_slot codec
 *   RECOVERY        + be64 clientid                  -> fdb_recovery codec
 *   RECOVERY_BY_OWNER + be32 owner_mds_id + be64 clientid    -> empty
 *   JOURNAL         + be64 txn_id + u8 role          -> fdb_journal codec
 *   DS              + be32 ds_id                     -> fdb_ds_info codec
 *   DS_PROVISION    + be32 ds_id                     -> fdb_ds_provision codec
 *   QUOTA_RULE      + u8 scope_type + be64 scope_id  -> fdb_quota_rule codec
 *   QUOTA_USAGE     + u8 usage_type + be64 scope_id  -> fdb_quota_usage codec
 *   SHARD_FILEID    + be64 fileid                    -> LE u32 shard_id
 *   EXT_DIRENT      + be64 parent + name             -> fdb_ext_dirent codec
 *   LINK_ANCHOR     + be64 anchor_id                 -> fdb_link_anchor codec
 *   PREALLOC        + be64 fileid                    -> reserved, never written:
 *   PREALLOC_BY_OWNER + be32 owner_mds_id + be64 fileid   the prealloc_pool_*
 *     slots are NULL on this backend (catalogue_fdb_ext.c), so the DS
 *     prealloc engine runs in memory only, as on memdb.
 *   NODE_REGISTRY   + be32 mds_id                    -> fdb_node codec
 *   PARTITION_MAP   + be32 partition_id              -> fdb_partition codec
 *   WITNESS         + be32 mds_id + be64 epoch + be32 slot   -> LE u64 seq
 *     commit-outcome witness (fdb_txn.h); epoch is the process-wide
 *     incarnation stamp, slot the worker's witness slot.
 *   WITNESS         + be32 mds_id + be64 epoch + be32 slot + u8 0x01
 *     the slot's fence anchor: NEVER written.  Every mutating attempt
 *     adds READ + WRITE conflict ranges on it and a fence adds a WRITE
 *     conflict range; the read-your-writes layer keeps explicit
 *     conflict ranges only over keys the transaction did not mutate,
 *     which is why the anchor is a separate, unwritten key (fdb_txn.h).
 *
 * Bounds: the longest key is prefix (FDB_KEY_PREFIX_MAX) + type + be64 +
 * be32 + owner bytes (128) + be64 + be64, or a NUL-terminated
 * MDS_MAX_NAME name plus 16 bytes; both fit FDB_KEY_MAX, which is far
 * below the FoundationDB 10 KB key limit.
 */

#ifndef FDB_KEYS_H
#define FDB_KEYS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "pnfs_mds.h"
#include "endian_helpers.h"

/** Longest configurable key prefix, in bytes (mds.conf fdb_key_prefix). */
#define FDB_KEY_PREFIX_MAX 31U

/** Longest key any layout above can produce. */
#define FDB_KEY_MAX (FDB_KEY_PREFIX_MAX + 1U + 8U + MDS_MAX_NAME + 1U + 16U + 128U)

_Static_assert(FDB_KEY_MAX < 10000U, "FoundationDB keys must stay under 10 KB");
_Static_assert(FDB_KEY_PREFIX_MAX + 1U <= 32U,
               "struct mds_config.fdb_key_prefix holds FDB_KEY_PREFIX_MAX + NUL");

/** Table selector: the byte after the prefix.  Append only, never renumber. */
enum fdb_key_type {
    FDB_KT_META              = 0x00,
    FDB_KT_INODE             = 0x01,
    FDB_KT_DIRENT            = 0x02,
    FDB_KT_DIRENT_SEQ        = 0x03,
    FDB_KT_STRIPE_HDR        = 0x04,
    FDB_KT_STRIPE_ENT        = 0x05,
    FDB_KT_XATTR             = 0x06,
    FDB_KT_INLINE            = 0x07,
    FDB_KT_GC                = 0x08,
    FDB_KT_REMOVE_PENDING    = 0x09,
    FDB_KT_LAYOUT_STATE      = 0x0A,
    FDB_KT_LAYOUT_BY_FILE    = 0x0B,
    FDB_KT_LAYOUT_BY_CLIENT  = 0x0C,
    FDB_KT_DS_LAYOUT_IDX     = 0x0D,
    FDB_KT_OPEN              = 0x0E,
    FDB_KT_OPEN_BY_FILE      = 0x0F,
    FDB_KT_OPEN_BY_CLIENT    = 0x10,
    FDB_KT_LOCK              = 0x11,
    FDB_KT_LOCK_BY_OWNER     = 0x12,
    FDB_KT_DELEG             = 0x13,
    FDB_KT_DELEG_BY_FILE     = 0x14,
    FDB_KT_DELEG_BY_CLIENT   = 0x15,
    FDB_KT_CLIENT            = 0x16,
    FDB_KT_SESSION           = 0x17,
    FDB_KT_SESSION_BY_CLIENT = 0x18,
    FDB_KT_SLOT              = 0x19,
    FDB_KT_RECOVERY          = 0x1A,
    FDB_KT_RECOVERY_BY_OWNER = 0x1B,
    FDB_KT_JOURNAL           = 0x1C,
    FDB_KT_DS                = 0x1D,
    FDB_KT_DS_PROVISION      = 0x1E,
    FDB_KT_QUOTA_RULE        = 0x1F,
    FDB_KT_QUOTA_USAGE       = 0x20,
    FDB_KT_SHARD_FILEID      = 0x21,
    FDB_KT_EXT_DIRENT        = 0x22,
    FDB_KT_LINK_ANCHOR       = 0x23,
    FDB_KT_PREALLOC          = 0x24,
    FDB_KT_PREALLOC_BY_OWNER = 0x25,
    FDB_KT_NODE_REGISTRY     = 0x26,
    FDB_KT_PARTITION_MAP     = 0x27,
    FDB_KT_WITNESS           = 0x28,
    FDB_KT_GC_BY_OWNER       = 0x29,
};

/** META sub-keys (the u8 after the META type byte). */
enum fdb_meta_key {
    FDB_META_SCHEMA_VERSION = 0x00,
    FDB_META_FILEID         = 0x01,
    FDB_META_GC_SEQ         = 0x02,
    FDB_META_REMOVE_SEQ     = 0x03,
    FDB_META_COOKIE_SEQ     = 0x04,
};

/** INODE parts (the u8 after the fileid). */
enum fdb_inode_part {
    FDB_INODE_PART_BLOB   = 0x00,
    FDB_INODE_PART_NLINK  = 0x01,
    FDB_INODE_PART_CHANGE = 0x02,
    FDB_INODE_PART_MTIME  = 0x03,
    FDB_INODE_PART_CTIME  = 0x04,
};

/** Number of INODE parts a directory has (blob + four counters). */
#define FDB_INODE_PART_COUNT 5U

/** The configured per-deployment prefix, copied into every key. */
struct fdb_key_prefix {
    uint8_t  bytes[FDB_KEY_PREFIX_MAX];
    uint32_t len;
};

/** One encoded key.  Built in place; never heap allocated. */
struct fdb_key {
    uint8_t  buf[FDB_KEY_MAX];
    uint32_t len;
    bool     overflow; /**< A component did not fit; the key is unusable. */
};

/** Half-open key range [begin, end). */
struct fdb_key_range {
    struct fdb_key begin;
    struct fdb_key end;
};

/* -----------------------------------------------------------------------
 * Generic builder
 * ----------------------------------------------------------------------- */

/** Encoded widths of the fixed-width components -- what fdb_key_u8,
 *  fdb_key_be32 and fdb_key_be64 append -- for code that parses a key
 *  back (a range page's keys are validated against base + these). */
#define FDB_KEY_U8_LEN   sizeof(uint8_t)
#define FDB_KEY_BE32_LEN sizeof(uint32_t)
#define FDB_KEY_BE64_LEN sizeof(uint64_t)

/** Start @p k with the prefix and @p type. */
static inline void fdb_key_init(struct fdb_key *k, const struct fdb_key_prefix *p,
                                enum fdb_key_type type)
{
    k->len = 0;
    k->overflow = false;
    if (p != NULL && p->len > 0) {
        memcpy(k->buf, p->bytes, p->len);
        k->len = p->len;
    }
    k->buf[k->len++] = (uint8_t)type;
}

/** Append @p n raw bytes.  Sets k->overflow (and appends nothing) when
 *  they do not fit; the caller checks it once, after the last append. */
static inline void fdb_key_bytes(struct fdb_key *k, const void *src, size_t n)
{
    if (n > FDB_KEY_MAX || k->len > FDB_KEY_MAX - n) {
        k->overflow = true;
        return;
    }
    if (n > 0) {
        memcpy(k->buf + k->len, src, n);
        k->len += (uint32_t)n;
    }
}

static inline void fdb_key_u8(struct fdb_key *k, uint8_t v)
{
    fdb_key_bytes(k, &v, 1);
}

static inline void fdb_key_be32(struct fdb_key *k, uint32_t v)
{
    uint8_t b[FDB_KEY_BE32_LEN];

    fdb_put_u32(b, v);
    fdb_key_bytes(k, b, sizeof(b));
}

static inline void fdb_key_be64(struct fdb_key *k, uint64_t v)
{
    uint8_t b[FDB_KEY_BE64_LEN];

    fdb_put_u64(b, v);
    fdb_key_bytes(k, b, sizeof(b));
}

/**
 * Append a dirent / xattr name.  Empty names and names longer than
 * MDS_MAX_NAME set k->overflow.  @p more_follows appends the NUL
 * terminator a mid-key name needs; a final component is stored raw.
 */
static inline void fdb_key_name(struct fdb_key *k, const char *name, bool more_follows)
{
    size_t n;

    if (name == NULL) {
        k->overflow = true;
        return;
    }
    n = strnlen(name, MDS_MAX_NAME + 1U);
    if (n == 0 || n > MDS_MAX_NAME) {
        k->overflow = true;
        return;
    }
    fdb_key_bytes(k, name, n);
    if (more_follows) {
        fdb_key_u8(k, 0);
    }
}

/** True when @p k was built without overflow and may be sent to FDB. */
static inline bool fdb_key_ok(const struct fdb_key *k)
{
    return !k->overflow;
}

/**
 * Turn @p k into the first key strictly greater than every key that
 * has @p k as a prefix ("strinc"): strip trailing 0xFF bytes and
 * increment the last remaining byte.  Returns false when every byte is
 * 0xFF (no such key exists; the key is left unchanged).
 */
static inline bool fdb_key_strinc(struct fdb_key *k)
{
    uint32_t n = k->len;

    while (n > 0 && k->buf[n - 1] == 0xFFU) {
        n--;
    }
    if (n == 0) {
        return false;
    }
    k->buf[n - 1]++;
    k->len = n;
    return true;
}

/**
 * Range covering every key that starts with @p prefix_key:
 * [prefix_key, strinc(prefix_key)).  Returns false (range unusable)
 * when prefix_key overflowed or has no strinc.
 */
static inline bool fdb_key_range_prefix(struct fdb_key_range *r,
                                        const struct fdb_key *prefix_key)
{
    if (!fdb_key_ok(prefix_key)) {
        return false;
    }
    r->begin = *prefix_key;
    r->end = *prefix_key;
    return fdb_key_strinc(&r->end);
}

/** Range [k, k + 0x00) covering exactly the single key @p k. */
static inline void fdb_key_range_single(struct fdb_key_range *r, const struct fdb_key *k)
{
    r->begin = *k;
    r->end = *k;
    fdb_key_u8(&r->end, 0);
}

/* -----------------------------------------------------------------------
 * Typed builders for the tables the core, namespace and extended slots
 * use.  The coordination and cluster slots (catalogue_fdb_coord.c,
 * catalogue_fdb_cluster.c) build their keys with the generic appenders
 * in the layouts documented above.
 * ----------------------------------------------------------------------- */

static inline void fdb_key_meta(struct fdb_key *k, const struct fdb_key_prefix *p,
                                enum fdb_meta_key sub)
{
    fdb_key_init(k, p, FDB_KT_META);
    fdb_key_u8(k, (uint8_t)sub);
}

/** INODE + be64 fileid: the range base of one logical inode. */
static inline void fdb_key_inode_prefix(struct fdb_key *k, const struct fdb_key_prefix *p,
                                        uint64_t fileid)
{
    fdb_key_init(k, p, FDB_KT_INODE);
    fdb_key_be64(k, fileid);
}

static inline void fdb_key_inode(struct fdb_key *k, const struct fdb_key_prefix *p,
                                 uint64_t fileid, enum fdb_inode_part part)
{
    fdb_key_inode_prefix(k, p, fileid);
    fdb_key_u8(k, (uint8_t)part);
}

/** DIRENT + be64 parent: the range base of one directory's entries. */
static inline void fdb_key_dirent_prefix(struct fdb_key *k, const struct fdb_key_prefix *p,
                                         uint64_t parent)
{
    fdb_key_init(k, p, FDB_KT_DIRENT);
    fdb_key_be64(k, parent);
}

static inline void fdb_key_dirent(struct fdb_key *k, const struct fdb_key_prefix *p,
                                  uint64_t parent, const char *name)
{
    fdb_key_dirent_prefix(k, p, parent);
    fdb_key_name(k, name, false);
}

static inline void fdb_key_dirent_seq_prefix(struct fdb_key *k,
                                             const struct fdb_key_prefix *p,
                                             uint64_t parent)
{
    fdb_key_init(k, p, FDB_KT_DIRENT_SEQ);
    fdb_key_be64(k, parent);
}

static inline void fdb_key_dirent_seq(struct fdb_key *k, const struct fdb_key_prefix *p,
                                      uint64_t parent, uint64_t seq)
{
    fdb_key_dirent_seq_prefix(k, p, parent);
    fdb_key_be64(k, seq);
}

static inline void fdb_key_stripe_hdr(struct fdb_key *k, const struct fdb_key_prefix *p,
                                      uint64_t fileid)
{
    fdb_key_init(k, p, FDB_KT_STRIPE_HDR);
    fdb_key_be64(k, fileid);
}

static inline void fdb_key_stripe_ent_prefix(struct fdb_key *k,
                                             const struct fdb_key_prefix *p,
                                             uint64_t fileid)
{
    fdb_key_init(k, p, FDB_KT_STRIPE_ENT);
    fdb_key_be64(k, fileid);
}

static inline void fdb_key_stripe_ent(struct fdb_key *k, const struct fdb_key_prefix *p,
                                      uint64_t fileid, uint32_t ordinal)
{
    fdb_key_stripe_ent_prefix(k, p, fileid);
    fdb_key_be32(k, ordinal);
}

static inline void fdb_key_xattr_prefix(struct fdb_key *k, const struct fdb_key_prefix *p,
                                        uint64_t fileid)
{
    fdb_key_init(k, p, FDB_KT_XATTR);
    fdb_key_be64(k, fileid);
}

static inline void fdb_key_xattr(struct fdb_key *k, const struct fdb_key_prefix *p,
                                 uint64_t fileid, const char *name)
{
    fdb_key_xattr_prefix(k, p, fileid);
    fdb_key_name(k, name, false);
}

static inline void fdb_key_inline(struct fdb_key *k, const struct fdb_key_prefix *p,
                                  uint64_t fileid)
{
    fdb_key_init(k, p, FDB_KT_INLINE);
    fdb_key_be64(k, fileid);
}

static inline void fdb_key_gc(struct fdb_key *k, const struct fdb_key_prefix *p,
                              uint64_t gc_seq)
{
    fdb_key_init(k, p, FDB_KT_GC);
    fdb_key_be64(k, gc_seq);
}

/** GC_BY_OWNER + be32 owner_mds_id: the range base of one owner's GC
 *  rows, in gc_seq order. */
static inline void fdb_key_gc_by_owner_prefix(struct fdb_key *k, const struct fdb_key_prefix *p,
                                              uint32_t owner_mds_id)
{
    fdb_key_init(k, p, FDB_KT_GC_BY_OWNER);
    fdb_key_be32(k, owner_mds_id);
}

/** The index key of the GC row @p gc_seq owned by @p owner_mds_id. */
static inline void fdb_key_gc_by_owner(struct fdb_key *k, const struct fdb_key_prefix *p,
                                       uint32_t owner_mds_id, uint64_t gc_seq)
{
    fdb_key_gc_by_owner_prefix(k, p, owner_mds_id);
    fdb_key_be64(k, gc_seq);
}

/** WITNESS + be32 mds_id: every witness key of one MDS id. */
static inline void fdb_key_witness_mds_prefix(struct fdb_key *k,
                                              const struct fdb_key_prefix *p,
                                              uint32_t mds_id)
{
    fdb_key_init(k, p, FDB_KT_WITNESS);
    fdb_key_be32(k, mds_id);
}

static inline void fdb_key_witness(struct fdb_key *k, const struct fdb_key_prefix *p,
                                   uint32_t mds_id, uint64_t epoch, uint32_t slot)
{
    fdb_key_witness_mds_prefix(k, p, mds_id);
    fdb_key_be64(k, epoch);
    fdb_key_be32(k, slot);
}

/* -----------------------------------------------------------------------
 * ext track (catalogue_fdb_ext.c): typed builders for the extended
 * authority tables, in the layouts documented at the top of this file.
 * Append-only; nothing above this line is changed by the track.
 * ----------------------------------------------------------------------- */

/** REMOVE_PENDING alone: the range base of the whole delete manifest. */
static inline void fdb_key_remove_pending_prefix(struct fdb_key *k,
                                                 const struct fdb_key_prefix *p)
{
    fdb_key_init(k, p, FDB_KT_REMOVE_PENDING);
}

static inline void fdb_key_remove_pending(struct fdb_key *k, const struct fdb_key_prefix *p,
                                          uint64_t remove_seq)
{
    fdb_key_remove_pending_prefix(k, p);
    fdb_key_be64(k, remove_seq);
}

/** GC alone: the range base of the whole GC queue. */
static inline void fdb_key_gc_prefix(struct fdb_key *k, const struct fdb_key_prefix *p)
{
    fdb_key_init(k, p, FDB_KT_GC);
}

/** DS alone: the range base of the DS registry. */
static inline void fdb_key_ds_prefix(struct fdb_key *k, const struct fdb_key_prefix *p)
{
    fdb_key_init(k, p, FDB_KT_DS);
}

static inline void fdb_key_ds(struct fdb_key *k, const struct fdb_key_prefix *p, uint32_t ds_id)
{
    fdb_key_ds_prefix(k, p);
    fdb_key_be32(k, ds_id);
}

static inline void fdb_key_ds_provision(struct fdb_key *k, const struct fdb_key_prefix *p,
                                        uint32_t ds_id)
{
    fdb_key_init(k, p, FDB_KT_DS_PROVISION);
    fdb_key_be32(k, ds_id);
}

static inline void fdb_key_quota_rule(struct fdb_key *k, const struct fdb_key_prefix *p,
                                      uint8_t scope_type, uint64_t scope_id)
{
    fdb_key_init(k, p, FDB_KT_QUOTA_RULE);
    fdb_key_u8(k, scope_type);
    fdb_key_be64(k, scope_id);
}

static inline void fdb_key_quota_usage(struct fdb_key *k, const struct fdb_key_prefix *p,
                                       uint8_t usage_type, uint64_t scope_id)
{
    fdb_key_init(k, p, FDB_KT_QUOTA_USAGE);
    fdb_key_u8(k, usage_type);
    fdb_key_be64(k, scope_id);
}

static inline void fdb_key_shard_fileid(struct fdb_key *k, const struct fdb_key_prefix *p,
                                        uint64_t fileid)
{
    fdb_key_init(k, p, FDB_KT_SHARD_FILEID);
    fdb_key_be64(k, fileid);
}

static inline void fdb_key_ext_dirent(struct fdb_key *k, const struct fdb_key_prefix *p,
                                      uint64_t parent, const char *name)
{
    fdb_key_init(k, p, FDB_KT_EXT_DIRENT);
    fdb_key_be64(k, parent);
    fdb_key_name(k, name, false);
}

static inline void fdb_key_link_anchor(struct fdb_key *k, const struct fdb_key_prefix *p,
                                       uint64_t anchor_id)
{
    fdb_key_init(k, p, FDB_KT_LINK_ANCHOR);
    fdb_key_be64(k, anchor_id);
}

/* End of ext track section. */

/* -----------------------------------------------------------------------
 * fdb-fault track: the fence anchor of a witness slot.
 * ----------------------------------------------------------------------- */

/** Byte appended to a witness key to name its fence anchor. */
#define FDB_WITNESS_FENCE_TAG 0x01U

/**
 * The fence anchor of the witness slot @p witness (fdb_key_witness):
 * the witness key plus FDB_WITNESS_FENCE_TAG.  It is never written;
 * transactions only place conflict ranges on it (fdb_txn.h).  Sorts
 * inside the slot's mds_id / epoch range, so the open-time clear of
 * dead incarnations covers it, and outside [witness, witness + 0x00),
 * so the probe's read of the witness value never touches it.
 */
static inline void fdb_key_witness_fence(struct fdb_key *k, const struct fdb_key *witness)
{
    *k = *witness;
    fdb_key_u8(k, FDB_WITNESS_FENCE_TAG);
}

#endif /* FDB_KEYS_H */

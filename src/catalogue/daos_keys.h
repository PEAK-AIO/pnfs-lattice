/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * daos_keys.h -- DAOS catalogue object model and key space.
 *
 * One DAOS container is one catalogue.  Every table is a family of
 * objects whose ids are DERIVED, never allocated and never mapped:
 *
 *     oid.lo       = entity id (fileid, clientid, ds_id, mds_id, 0)
 *     oid.hi[0:32] = table tag (enum daos_tag, append-only)
 *     oid.hi[32:64]= DAOS reserved bits: object type, redundancy and
 *                    group count of the object class
 *
 * The reserved bits come from the class encodings stamped on the
 * container at bootstrap (DAOS_CAT_ATTR_OCLASS_ID /
 * DAOS_CAT_ATTR_OCLASS_SPREAD_ID), not from a live class resolution:
 * daos_obj_generate_oid() resolves a GX class to the pool's CURRENT
 * target count, so a live resolution would address different objects
 * after the pool grows.  daos_oid_make() refuses an unstamped backend.
 *
 * Two object shapes:
 *
 *   Entity objects   DAOS_OT_DKEY_LEXICAL, class daos_oclass (default
 *                    RP_3G1): sorted lexical dkeys, HASHED akeys.  All
 *                    state of one entity is one object.
 *   Ordered queues   DAOS_OT_DKEY_UINT64, class daos_oclass: dkeys are
 *                    native uint64_t values (8 bytes), enumerated in
 *                    numeric order; hashed akeys.
 *   Spread objects   DAOS_OT_MULTI_HASHED, class daos_oclass_spread
 *                    (default RP_3GX): point access only, no order.
 *
 * The DAOS btree stores a lexical or uint64 key inline in its anchor
 * (EMBEDDED_KEY_MAX = 100 bytes) and rejects a longer key with
 * -DER_KEY2BIG, so every sorted dkey below is at most DAOS_LEX_KEY_MAX
 * bytes (static-asserted); names live in hashed akeys, which have no
 * such limit.  Lexical order is memcmp order with a shorter key first.
 *
 * Layouts (value formats in fdb_codec.h / daos_codec.h; "marker" = the
 * single byte DAOS_MARKER_BYTE, never empty: an update of size 0 is a
 * punch):
 *
 *   META (lo 0)                 dkey "schema"                 akey 'v' -> LE u64
 *                               dkey "fileid" | "gc_seq" | "remove_seq" |
 *                                    "cookie_seq"             akey 'v' -> LE u64
 *     one dkey per allocator so refills of different counters never
 *     conflict (daos_backend_alloc_id).
 *   INODE (lo fileid)           dkey 'a'   akey 'b'           -> fdb_inode codec
 *                                              (directory counters dead:
 *                                              fdb_inode_split)
 *                                          akey 'c'+be32 i    -> daos_dir_shard codec,
 *                                              i in [0, K), directories only
 *                               dkey 'x'   akey = xattr name  -> u8 version + value bytes
 *                               dkey 'i'   akey 'd'           -> u8 version + inline data
 *     (catalogue_daos_ext.c: the version byte keeps a zero-length xattr
 *     or inline value from being an empty update, which is a punch)
 *                               dkey 's'   akey 'm'           -> daos stripe-map blob
 *                               dkey 'h'   akey 'v'           -> LE u32 shard_id
 *                               dkey 'L' | 'O' | 'D'
 *                                          akey = stateid_other[12] -> marker
 *                               dkey 'K'   akey = be64 lock_id -> fdb_lock codec
 *     directories additionally:
 *                               dkey 'N'   akey = name        -> fdb_dirent codec
 *                               dkey 'S'+be64 seq  akey 'v'   -> fdb_dirent_seq codec
 *                               dkey 'E'   akey = name        -> fdb_ext_dirent codec
 *     'N' is one dkey per directory: a lookup is one fetch, CREATE
 *     inserts the akey with DAOS_COND_AKEY_INSERT, emptiness is a
 *     one-key akey listing, and ns_readdir enumerates the akeys in the
 *     store's hash order.  'S'+be64 dkeys sort in cookie order for
 *     ns_readdir_plus_from.  seq is minted from the META cookie counter
 *     (unique container-wide, never below 16); both rows are always
 *     written and cleared together.
 *   CLIENT (lo clientid)        dkey 'c'   akey 'v'           -> fdb_client codec
 *                               dkey 'L' | 'O' | 'D'
 *                                          akey = stateid_other[12] -> marker
 *                               dkey 'S'   akey = session_id[16] -> marker
 *                               dkey 'R'   akey 'v'           -> fdb_recovery codec
 *                               dkey 'K'   akey = be32 owner_len + owner +
 *                                                 be64 fileid + be64 lock_id -> marker
 *     lock_scan_owner lists the client's 'K' akeys and keeps those
 *     with the owner prefix.
 *   RECOVERY_BY_OWNER (lo mds_id) dkey be64 clientid  akey 'm' -> marker
 *   GC_BY_OWNER (lo mds_id)     uint64 dkey gc_seq    akey 'm' -> marker
 *     written and cleared with the GC row, never alone.
 *   NODE_REGISTRY (lo 0)        dkey be32 mds_id      akey 'v' -> fdb_node codec
 *   PARTITION_MAP (lo 0)        dkey be32 partition   akey 'v' -> fdb_partition codec
 *   DS (lo 0)                   dkey 'd'+be32 ds_id   akey 'v' -> fdb_ds_info codec
 *                               dkey 'p'+be32 ds_id   akey 'v' -> fdb_ds_provision codec
 *   QUOTA (lo 0)                dkey 'r'+u8 type+be64 id  akey 'v' -> fdb_quota_rule codec
 *                               dkey 'u'+u8 type+be64 id  akey 'v' -> fdb_quota_usage codec
 *   REMOVE_PENDING (lo 0)       uint64 dkey remove_seq akey 'v' -> fdb_remove_pending codec
 *   LAYOUT_STATE (lo 0)         dkey stateid_other[12] akey 'v' -> fdb_layout codec
 *   OPEN (lo 0)                 dkey stateid_other[12] akey 'v' -> fdb_open codec
 *   DELEG (lo 0)                dkey stateid_other[12] akey 'v' -> fdb_deleg codec
 *   SESSION (lo 0)              dkey session_id[16]   akey 's' -> fdb_session codec
 *                                                     akey "slot"+be32 slot_id
 *                                                              -> fdb_slot codec
 *   JOURNAL (lo 0)              dkey be64 txn_id + u8 role  akey 'v' -> fdb_journal codec
 *   LINK_ANCHOR (lo 0)          dkey be64 anchor_id   akey 'v' -> fdb_link_anchor codec
 *   GC (lo 0)                   dkey be64 gc_seq      akey 'v' -> fdb_gc codec
 *   STRIPE_IDX (lo 0)           dkey be64 fileid      akey 'm' -> marker
 *     written and cleared with the INODE 's' blob so stripe_map_scan
 *     can enumerate the files that have a map.
 *   DS_LAYOUT_IDX (lo ds_id)    dkey be64 clientid + be64 fileid + stateid_other[12]
 *                                                     akey 'm' -> marker
 *
 * Container attributes (set at bootstrap in ONE daos_cont_set_attr,
 * verified at open before any object is touched):
 *   lattice.schema            LE u64, DAOS_CAT_SCHEMA_VERSION
 *   lattice.oclass            NUL-terminated class name of the entity objects
 *   lattice.oclass_spread     NUL-terminated class name of the spread objects
 *   lattice.oclass_id         LE u32 resolved encoding of lattice.oclass
 *   lattice.oclass_spread_id  LE u32 resolved encoding of lattice.oclass_spread
 *   lattice.dir_shards        LE u32 K, the directory counter shard count
 */

#ifndef DAOS_KEYS_H
#define DAOS_KEYS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <daos.h>

#include "pnfs_mds.h"
#include "endian_helpers.h"

/** Longest key any layout above produces (a 255-byte name plus slack;
 *  the lock-owner index akey is 4 + 128 + 8 + 8 = 148). */
#define DAOS_KEY_MAX (MDS_MAX_NAME + 1U + 16U)

/** Inline key limit of the DAOS btree for lexical / uint64 keys
 *  (EMBEDDED_KEY_MAX): every sorted dkey must stay within it. */
#define DAOS_LEX_KEY_MAX 100U

/** Longest sorted dkey of the layouts above ("remove_seq", "cookie_seq",
 *  QUOTA 'r'+u8+be64). */
#define DAOS_LEX_DKEY_LONGEST 10U

_Static_assert(DAOS_KEY_MAX >= 4U + 128U + 8U + 8U, "lock-owner index akey must fit");
_Static_assert(DAOS_KEY_MAX >= MDS_MAX_NAME, "a name akey must fit");
_Static_assert(DAOS_LEX_DKEY_LONGEST <= DAOS_LEX_KEY_MAX,
               "sorted dkeys must fit the DAOS inline key limit");
_Static_assert(DAOS_LEX_DKEY_LONGEST >= 9U, "the 'S'+be64 cookie dkey is 9 bytes");

/** The one-byte value of every index marker (an empty value would be a punch). */
#define DAOS_MARKER_BYTE 0x01U

/** Container attribute names. */
#define DAOS_CAT_ATTR_SCHEMA           "lattice.schema"
#define DAOS_CAT_ATTR_OCLASS           "lattice.oclass"
#define DAOS_CAT_ATTR_OCLASS_SPREAD    "lattice.oclass_spread"
#define DAOS_CAT_ATTR_OCLASS_ID        "lattice.oclass_id"
#define DAOS_CAT_ATTR_OCLASS_SPREAD_ID "lattice.oclass_spread_id"
#define DAOS_CAT_ATTR_DIR_SHARDS       "lattice.dir_shards"

/** Upper bound of the directory counter shard count (mds.conf 1..256). */
#define DAOS_DIR_SHARDS_MAX 256U

/* -----------------------------------------------------------------------
 * Tables
 * ----------------------------------------------------------------------- */

/** Table selector stored in the user bits of oid.hi.  Append only. */
enum daos_tag {
    DAOS_TAG_META              = 0x00,
    DAOS_TAG_INODE             = 0x01,
    DAOS_TAG_CLIENT            = 0x02,
    DAOS_TAG_RECOVERY_BY_OWNER = 0x03,
    DAOS_TAG_GC_BY_OWNER       = 0x04,
    DAOS_TAG_NODE_REGISTRY     = 0x05,
    DAOS_TAG_PARTITION_MAP     = 0x06,
    DAOS_TAG_DS                = 0x07,
    DAOS_TAG_QUOTA             = 0x08,
    DAOS_TAG_REMOVE_PENDING    = 0x09,
    DAOS_TAG_LAYOUT_STATE      = 0x10,
    DAOS_TAG_OPEN              = 0x11,
    DAOS_TAG_DELEG             = 0x12,
    DAOS_TAG_SESSION           = 0x13,
    DAOS_TAG_JOURNAL           = 0x14,
    DAOS_TAG_LINK_ANCHOR       = 0x15,
    DAOS_TAG_GC                = 0x16,
    DAOS_TAG_STRIPE_IDX        = 0x17,
    DAOS_TAG_DS_LAYOUT_IDX     = 0x18,
};

/** META allocator counters and the schema stamp (one dkey each). */
enum daos_meta_key {
    DAOS_META_SCHEMA     = 0,
    DAOS_META_FILEID     = 1,
    DAOS_META_GC_SEQ     = 2,
    DAOS_META_REMOVE_SEQ = 3,
    DAOS_META_COOKIE_SEQ = 4,
};

/** Number of META keys (allocator pools are indexed by enum daos_meta_key). */
#define DAOS_META_KEY_COUNT 5U

/** True for the spread (hashed, point-access) tables. */
static inline bool daos_tag_is_spread(enum daos_tag tag)
{
    return (unsigned)tag >= (unsigned)DAOS_TAG_LAYOUT_STATE &&
           (unsigned)tag <= (unsigned)DAOS_TAG_DS_LAYOUT_IDX;
}

/** True for a tag this build knows. */
static inline bool daos_tag_is_valid(enum daos_tag tag)
{
    return (unsigned)tag <= (unsigned)DAOS_TAG_REMOVE_PENDING || daos_tag_is_spread(tag);
}

/** DAOS object type of a table (see the shapes above). */
static inline enum daos_otype_t daos_tag_otype(enum daos_tag tag)
{
    if (daos_tag_is_spread(tag)) {
        return DAOS_OT_MULTI_HASHED;
    }
    if (tag == DAOS_TAG_GC_BY_OWNER || tag == DAOS_TAG_REMOVE_PENDING) {
        return DAOS_OT_DKEY_UINT64;
    }
    return DAOS_OT_DKEY_LEXICAL;
}

/**
 * Derive the object id of table @p tag, entity @p id.
 *
 * @param tag         Table.
 * @param id          Entity id (oid.lo); 0 for single-object tables.
 * @param cid_entity  Stamped class encoding of the entity / queue objects
 *                    (DAOS_CAT_ATTR_OCLASS_ID): redundancy in the bits above
 *                    OC_REDUN_SHIFT, group count below.
 * @param cid_spread  Stamped class encoding of the spread objects.
 * @param out         Receives the fully encoded id.
 * @return 0; -DER_INVAL for an unknown tag or an unstamped (0) class --
 *         there is no fallback to a live class resolution.
 */
static inline int daos_oid_make(enum daos_tag tag, uint64_t id, daos_oclass_id_t cid_entity,
                                daos_oclass_id_t cid_spread, daos_obj_id_t *out)
{
    daos_oclass_id_t cid = daos_tag_is_spread(tag) ? cid_spread : cid_entity;
    uint64_t ord = (uint64_t)(cid >> OC_REDUN_SHIFT);
    uint64_t nr_grps = (uint64_t)(cid & (daos_oclass_id_t)MAX_NUM_GROUPS);
    uint64_t hi;

    if (out == NULL || !daos_tag_is_valid(tag) || cid == OC_UNKNOWN ||
        ord > OID_FMT_CLASS_MAX) {
        return -DER_INVAL;
    }
    /* Same layout as the client library's daos_obj_set_oid(). */
    hi = (uint64_t)tag;
    hi |= (uint64_t)daos_tag_otype(tag) << OID_FMT_TYPE_SHIFT;
    hi |= ord << OID_FMT_CLASS_SHIFT;
    hi |= nr_grps << OID_FMT_META_SHIFT;
    out->lo = id;
    out->hi = hi;
    return 0;
}

/* -----------------------------------------------------------------------
 * Generic key builder
 * ----------------------------------------------------------------------- */

/** One encoded dkey or akey.  Built in place; never heap allocated. */
struct daos_key {
    uint8_t  buf[DAOS_KEY_MAX];
    uint32_t len;
    bool     overflow; /**< A component did not fit; the key is unusable. */
};

#define DAOS_KEY_U8_LEN   sizeof(uint8_t)
#define DAOS_KEY_BE32_LEN sizeof(uint32_t)
#define DAOS_KEY_BE64_LEN sizeof(uint64_t)

/** Start an empty key. */
static inline void daos_key_init(struct daos_key *k)
{
    k->len = 0;
    k->overflow = false;
}

/** Append @p n raw bytes.  Sets k->overflow (and appends nothing) when
 *  they do not fit; the caller checks it once, after the last append. */
static inline void daos_key_bytes(struct daos_key *k, const void *src, size_t n)
{
    if (n > DAOS_KEY_MAX || k->len > DAOS_KEY_MAX - n) {
        k->overflow = true;
        return;
    }
    if (n > 0) {
        memcpy(k->buf + k->len, src, n);
        k->len += (uint32_t)n;
    }
}

static inline void daos_key_u8(struct daos_key *k, uint8_t v)
{
    daos_key_bytes(k, &v, 1);
}

static inline void daos_key_be32(struct daos_key *k, uint32_t v)
{
    uint8_t b[DAOS_KEY_BE32_LEN];

    fdb_put_u32(b, v);
    daos_key_bytes(k, b, sizeof(b));
}

static inline void daos_key_be64(struct daos_key *k, uint64_t v)
{
    uint8_t b[DAOS_KEY_BE64_LEN];

    fdb_put_u64(b, v);
    daos_key_bytes(k, b, sizeof(b));
}

/**
 * Append a dirent / xattr name as raw bytes (always the LAST component
 * of an akey).  Empty names and names longer than MDS_MAX_NAME set
 * k->overflow.
 */
static inline void daos_key_name(struct daos_key *k, const char *name)
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
    daos_key_bytes(k, name, n);
}

/** True when @p k was built without overflow and may be sent to DAOS. */
static inline bool daos_key_ok(const struct daos_key *k)
{
    return !k->overflow && k->len > 0;
}

/** Point the DAOS key descriptor @p iov at @p k (no copy; @p k must
 *  outlive the operation). */
static inline void daos_key_iov(struct daos_key *k, d_iov_t *iov)
{
    d_iov_set(iov, k->buf, k->len);
}

/** True when @p k holds exactly the bytes @p b[0..n). */
static inline bool daos_key_equals(const struct daos_key *k, const void *b, size_t n)
{
    return k->len == n && (n == 0 || memcmp(k->buf, b, n) == 0);
}

/* -----------------------------------------------------------------------
 * Common akeys
 * ----------------------------------------------------------------------- */

/** 'v': the single value row of a dkey. */
static inline void daos_akey_value(struct daos_key *k)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)'v');
}

/** 'm': the marker akey of an index dkey. */
static inline void daos_akey_marker(struct daos_key *k)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)'m');
}

/** A name akey (dirent, xattr, ext_dirent): the raw name bytes. */
static inline void daos_akey_name(struct daos_key *k, const char *name)
{
    daos_key_init(k);
    daos_key_name(k, name);
}

/** A stateid akey (by-file / by-client indexes): stateid_other[12]. */
static inline void daos_akey_stateid(struct daos_key *k, const uint8_t other[NFS4_OTHER_SIZE])
{
    daos_key_init(k);
    daos_key_bytes(k, other, NFS4_OTHER_SIZE);
}

/** A session akey (CLIENT 'S'): session_id[16]. */
static inline void daos_akey_session(struct daos_key *k, const uint8_t session_id[16])
{
    daos_key_init(k);
    daos_key_bytes(k, session_id, 16);
}

/** INODE 'K' akey: be64 lock_id. */
static inline void daos_akey_lock(struct daos_key *k, uint64_t lock_id)
{
    daos_key_init(k);
    daos_key_be64(k, lock_id);
}

/** CLIENT 'K' akey prefix: be32 owner_len + owner bytes (owner_len
 *  1..128); the filter of lock_scan_owner. */
static inline void daos_akey_lock_owner_prefix(struct daos_key *k, const uint8_t *owner,
                                               uint32_t owner_len)
{
    daos_key_init(k);
    if (owner == NULL || owner_len == 0 || owner_len > 128U) {
        k->overflow = true;
        return;
    }
    daos_key_be32(k, owner_len);
    daos_key_bytes(k, owner, owner_len);
}

/** CLIENT 'K' akey: prefix + be64 fileid + be64 lock_id. */
static inline void daos_akey_lock_owner(struct daos_key *k, const uint8_t *owner,
                                        uint32_t owner_len, uint64_t fileid, uint64_t lock_id)
{
    daos_akey_lock_owner_prefix(k, owner, owner_len);
    daos_key_be64(k, fileid);
    daos_key_be64(k, lock_id);
}

/* -----------------------------------------------------------------------
 * META
 * ----------------------------------------------------------------------- */

/** The dkey name of a META key (NULL for an unknown key). */
static inline const char *daos_meta_key_name(enum daos_meta_key key)
{
    switch (key) {
    case DAOS_META_SCHEMA:
        return "schema";
    case DAOS_META_FILEID:
        return "fileid";
    case DAOS_META_GC_SEQ:
        return "gc_seq";
    case DAOS_META_REMOVE_SEQ:
        return "remove_seq";
    case DAOS_META_COOKIE_SEQ:
        return "cookie_seq";
    default:
        return NULL;
    }
}

/** META dkey of @p key (overflow for an unknown key). */
static inline void daos_dkey_meta(struct daos_key *k, enum daos_meta_key key)
{
    const char *name = daos_meta_key_name(key);

    daos_key_init(k);
    if (name == NULL) {
        k->overflow = true;
        return;
    }
    daos_key_bytes(k, name, strlen(name));
}

/* -----------------------------------------------------------------------
 * INODE
 * ----------------------------------------------------------------------- */

/** The single-byte INODE dkeys. */
enum daos_inode_dkey {
    DAOS_INODE_DKEY_ATTRS      = 'a',
    DAOS_INODE_DKEY_XATTRS     = 'x',
    DAOS_INODE_DKEY_INLINE     = 'i',
    DAOS_INODE_DKEY_STRIPE     = 's',
    DAOS_INODE_DKEY_SHARD      = 'h',
    DAOS_INODE_DKEY_LAYOUTS    = 'L',
    DAOS_INODE_DKEY_OPENS      = 'O',
    DAOS_INODE_DKEY_DELEGS     = 'D',
    DAOS_INODE_DKEY_LOCKS      = 'K',
    DAOS_INODE_DKEY_DIRENTS    = 'N',
    DAOS_INODE_DKEY_EXT_DIRENTS = 'E',
};

/** Prefix byte of the cookie-ordered dirent rows ('S' + be64 seq). */
#define DAOS_INODE_DKEY_DIRENT_SEQ_PREFIX ((uint8_t)'S')
/** Length of an 'S' + be64 dkey. */
#define DAOS_INODE_DKEY_DIRENT_SEQ_LEN (1U + DAOS_KEY_BE64_LEN)

static inline void daos_dkey_inode(struct daos_key *k, enum daos_inode_dkey which)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)which);
}

/** 'S' + be64 seq: the cookie-ordered row of one dirent. */
static inline void daos_dkey_inode_dirent_seq(struct daos_key *k, uint64_t seq)
{
    daos_key_init(k);
    daos_key_u8(k, DAOS_INODE_DKEY_DIRENT_SEQ_PREFIX);
    daos_key_be64(k, seq);
}

/** True when @p key[0..len) is an 'S' + be64 dkey; *seq receives the cookie. */
static inline bool daos_dkey_inode_dirent_seq_parse(const uint8_t *key, size_t len,
                                                    uint64_t *seq)
{
    if (len != DAOS_INODE_DKEY_DIRENT_SEQ_LEN || key[0] != DAOS_INODE_DKEY_DIRENT_SEQ_PREFIX) {
        return false;
    }
    *seq = fdb_get_u64(key + 1);
    return true;
}

/** 'b': the inode blob akey of dkey 'a'. */
static inline void daos_akey_blob(struct daos_key *k)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)'b');
}

/** 'c' + be32 i: directory counter shard @p i of dkey 'a'. */
static inline void daos_akey_dir_shard(struct daos_key *k, uint32_t i)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)'c');
    daos_key_be32(k, i);
}

/** 'd': the inline data akey of dkey 'i'. */
static inline void daos_akey_inline(struct daos_key *k)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)'d');
}

/** 'm': the stripe-map blob akey of dkey 's'. */
static inline void daos_akey_stripe_map(struct daos_key *k)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)'m');
}

/* -----------------------------------------------------------------------
 * CLIENT
 * ----------------------------------------------------------------------- */

/** The single-byte CLIENT dkeys. */
enum daos_client_dkey {
    DAOS_CLIENT_DKEY_ROW      = 'c',
    DAOS_CLIENT_DKEY_LAYOUTS  = 'L',
    DAOS_CLIENT_DKEY_OPENS    = 'O',
    DAOS_CLIENT_DKEY_DELEGS   = 'D',
    DAOS_CLIENT_DKEY_SESSIONS = 'S',
    DAOS_CLIENT_DKEY_RECOVERY = 'R',
    DAOS_CLIENT_DKEY_LOCKS    = 'K',
};

static inline void daos_dkey_client(struct daos_key *k, enum daos_client_dkey which)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)which);
}

/* -----------------------------------------------------------------------
 * Fixed-width dkeys of the other sorted tables
 * ----------------------------------------------------------------------- */

/** be32 id: NODE_REGISTRY (mds_id), PARTITION_MAP (partition_id). */
static inline void daos_dkey_be32(struct daos_key *k, uint32_t id)
{
    daos_key_init(k);
    daos_key_be32(k, id);
}

/** be64 id: RECOVERY_BY_OWNER (clientid), LINK_ANCHOR (anchor_id),
 *  GC (gc_seq), STRIPE_IDX (fileid). */
static inline void daos_dkey_be64(struct daos_key *k, uint64_t id)
{
    daos_key_init(k);
    daos_key_be64(k, id);
}

/** Native uint64 dkey of a DAOS_OT_DKEY_UINT64 object: GC_BY_OWNER
 *  (gc_seq), REMOVE_PENDING (remove_seq).  Exactly 8 bytes of the host
 *  representation; DAOS orders them numerically. */
static inline void daos_dkey_u64(struct daos_key *k, uint64_t v)
{
    daos_key_init(k);
    daos_key_bytes(k, &v, sizeof(v));
}

/** Read back a native uint64 dkey; false unless exactly 8 bytes. */
static inline bool daos_dkey_u64_parse(const uint8_t *key, size_t len, uint64_t *v)
{
    if (len != sizeof(*v)) {
        return false;
    }
    memcpy(v, key, sizeof(*v));
    return true;
}

/** DS 'd' + be32 ds_id (registry row). */
static inline void daos_dkey_ds_info(struct daos_key *k, uint32_t ds_id)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)'d');
    daos_key_be32(k, ds_id);
}

/** DS 'p' + be32 ds_id (provisioning row). */
static inline void daos_dkey_ds_provision(struct daos_key *k, uint32_t ds_id)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)'p');
    daos_key_be32(k, ds_id);
}

/** QUOTA 'r' + u8 scope_type + be64 scope_id (rule). */
static inline void daos_dkey_quota_rule(struct daos_key *k, uint8_t scope_type,
                                        uint64_t scope_id)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)'r');
    daos_key_u8(k, scope_type);
    daos_key_be64(k, scope_id);
}

/** QUOTA 'u' + u8 usage_type + be64 scope_id (usage). */
static inline void daos_dkey_quota_usage(struct daos_key *k, uint8_t usage_type,
                                         uint64_t scope_id)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)'u');
    daos_key_u8(k, usage_type);
    daos_key_be64(k, scope_id);
}

/* -----------------------------------------------------------------------
 * Spread (hashed) dkeys
 * ----------------------------------------------------------------------- */

/** LAYOUT_STATE / OPEN / DELEG dkey: stateid_other[12]. */
static inline void daos_dkey_stateid(struct daos_key *k, const uint8_t other[NFS4_OTHER_SIZE])
{
    daos_key_init(k);
    daos_key_bytes(k, other, NFS4_OTHER_SIZE);
}

/** SESSION dkey: session_id[16]. */
static inline void daos_dkey_session(struct daos_key *k, const uint8_t session_id[16])
{
    daos_key_init(k);
    daos_key_bytes(k, session_id, 16);
}

/** SESSION akey 's': the session row. */
static inline void daos_akey_session_row(struct daos_key *k)
{
    daos_key_init(k);
    daos_key_u8(k, (uint8_t)'s');
}

/** SESSION akey "slot" + be32 slot_id: one DRC slot. */
static inline void daos_akey_drc_slot(struct daos_key *k, uint32_t slot_id)
{
    daos_key_init(k);
    daos_key_bytes(k, "slot", 4);
    daos_key_be32(k, slot_id);
}

/** JOURNAL dkey: be64 txn_id + u8 role. */
static inline void daos_dkey_journal(struct daos_key *k, uint64_t txn_id, uint8_t role)
{
    daos_key_init(k);
    daos_key_be64(k, txn_id);
    daos_key_u8(k, role);
}

/** DS_LAYOUT_IDX dkey: be64 clientid + be64 fileid + stateid_other[12]. */
static inline void daos_dkey_ds_layout_idx(struct daos_key *k, uint64_t clientid,
                                           uint64_t fileid,
                                           const uint8_t other[NFS4_OTHER_SIZE])
{
    daos_key_init(k);
    daos_key_be64(k, clientid);
    daos_key_be64(k, fileid);
    daos_key_bytes(k, other, NFS4_OTHER_SIZE);
}

#endif /* DAOS_KEYS_H */

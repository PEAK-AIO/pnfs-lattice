/*
 * Copyright (c) 2026 PeakAIO. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-PeakAIO-Proprietary
 *
 * xdr_codec.c — NFSv4.1 XDR encode/decode for COMPOUND requests.
 *
 * Uses libntirpc XDR primitives for the low-level encoding.
 * This file implements NFSv4.1-specific compound and per-operation
 * codecs that translate between XDR wire format and the typed C
 * structs in compound.h.
 */

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <arpa/inet.h>

#include "xdr_codec.h"
#include "mds_gss.h"
#include "xdr_internal.h"
#include "pnfs_mds.h"
#include "hpc_shared.h"

/* -----------------------------------------------------------------------
 * NFSv4.1 type codecs
 * ----------------------------------------------------------------------- */

bool xdr_nfs4_fh_encode(XDR *xdrs, uint64_t fileid)
{
    /* Legacy v0: 8-byte local fileid. */
    uint32_t len = 8;
    uint8_t buf[8];
    uint64_t fid_be = htobe64(fileid);
    memcpy(buf, &fid_be, 8);
    if (!xdr_uint32_t(xdrs, &len)) { return false; }
    return xdr_opaque_encode(xdrs, (char *)buf, 8);
}

bool xdr_nfs4_fh_encode_v1(XDR *xdrs, uint32_t owner_mds_id,
                            uint64_t fileid, uint32_t generation)
{
    uint32_t len = 17;
    uint8_t buf[17];
    buf[0] = 0x01;
    { uint32_t m = htobe32(owner_mds_id); memcpy(buf + 1, &m, 4); }
    { uint64_t f = htobe64(fileid); memcpy(buf + 5, &f, 8); }
    { uint32_t g = htobe32(generation); memcpy(buf + 13, &g, 4); }
    if (!xdr_uint32_t(xdrs, &len)) { return false; }
    return xdr_opaque_encode(xdrs, (char *)buf, 17);
}

bool xdr_nfs4_fh_decode(XDR *xdrs, uint64_t *fileid)
{
    /* Backward-compatible decode: v0 (8 bytes) or v1 (17 bytes).
     * Pynfs PUTFH2 sends a 3-byte FH ('abc') and expects the server
     * to return NFS4ERR_BADHANDLE rather than NFS4ERR_BADXDR — so
     * we now accept any well-formed opaque<NFS4_FHSIZE> payload at
     * decode time and let op_putfh decide the malformed-FH error
     * by checking for the fileid==0 sentinel below. */
    uint32_t len = 0;
    uint8_t buf[NFS4_FHSIZE];
    if (!xdr_uint32_t(xdrs, &len)) { return false; }
    if (len > NFS4_FHSIZE) { return false; }
    if (len > 0 && !xdr_opaque_decode(xdrs, (char *)buf, len)) {
        return false;
    }
    if (len == 8) {
        /* v0 legacy: 8-byte fileid BE.  memcpy avoids an unaligned
         * uint64_t load from the byte buffer (UB / SIGBUS on
         * strict-alignment targets). */
        uint64_t fid_be;
        memcpy(&fid_be, buf, sizeof(fid_be));
        *fileid = be64toh(fid_be);
        return true;
    }
    if (len >= 17 && buf[0] == 0x01) {
        /* v1 cluster-global: skip owner_mds_id, extract fileid.
         * buf + 5 is never 8-byte aligned — copy before swapping. */
        uint64_t fid_be;
        memcpy(&fid_be, buf + 5, sizeof(fid_be));
        *fileid = be64toh(fid_be);
        return true;
    }
    /* Malformed wire FH — sentinel fileid=0 so op_putfh returns
     * NFS4ERR_BADHANDLE.  fileid 0 is reserved (root is
     * MDS_FILEID_ROOT == 2). */
    *fileid = 0;
    return true;
}

bool xdr_nfs4_fh_decode_full(XDR *xdrs, struct nfs4_fh_desc *desc)
{
    uint32_t len = 0;
    uint8_t buf[NFS4_FHSIZE];
    if (!xdr_uint32_t(xdrs, &len)) { return false; }
    if (len > NFS4_FHSIZE) { return false; }
    if (len > 0 && !xdr_opaque_decode(xdrs, (char *)buf, len)) {
        return false;
    }
    memset(desc, 0, sizeof(*desc));
    if (len == 8) {
        /* memcpy avoids an unaligned load (see xdr_nfs4_fh_decode). */
        uint64_t fid_be;
        memcpy(&fid_be, buf, sizeof(fid_be));
        desc->fileid = be64toh(fid_be);
        return true;
    }
    if (len >= 17 && buf[0] == 0x01) {
        /* buf + 1 / buf + 5 / buf + 13 are misaligned for their
         * types — copy into properly typed locals before swapping. */
        uint32_t m_be, g_be;
        uint64_t fid_be;
        memcpy(&m_be, buf + 1, sizeof(m_be));
        memcpy(&fid_be, buf + 5, sizeof(fid_be));
        memcpy(&g_be, buf + 13, sizeof(g_be));
        desc->owner_mds_id = be32toh(m_be);
        desc->fileid = be64toh(fid_be);
        desc->generation = be32toh(g_be);
        return true;
    }
    /* Malformed FH — sentinel fileid=0 so op_putfh returns
     * NFS4ERR_BADHANDLE.  Pynfs PUTFH2 covers the 3-byte case;
     * any other unrecognized format gets the same treatment. */
    desc->fileid = 0;
    return true;
}

bool xdr_nfs4_stateid_encode(XDR *xdrs, const struct nfs4_stateid *sid)
{
    uint32_t seqid = sid->seqid;

    if (!xdr_uint32_t(xdrs, &seqid)) {
        return false;
}
    return xdr_opaque_encode(xdrs, (const char *)sid->other,
                             NFS4_OTHER_SIZE);
}

bool xdr_nfs4_stateid_decode(XDR *xdrs, struct nfs4_stateid *sid)
{
    if (!xdr_uint32_t(xdrs, &sid->seqid)) {
        return false;
}
    return xdr_opaque_decode(xdrs, (char *)sid->other, NFS4_OTHER_SIZE);
}

bool xdr_nfs4_bitmap_encode(XDR *xdrs, const uint32_t *bm, uint32_t words)
{
    uint32_t i;
    uint32_t actual_words = words;

    /* Trim trailing zero words — kernel expects minimal encoding. */
    while (actual_words > 0 && bm[actual_words - 1] == 0) {
        actual_words--;
    }

    if (!xdr_uint32_t(xdrs, &actual_words)) {
        return false;
}
    for (i = 0; i < actual_words; i++) {
        uint32_t w = bm[i];

        if (!xdr_uint32_t(xdrs, &w)) {
            return false;
}
    }
    return true;
}

bool xdr_nfs4_bitmap_decode(XDR *xdrs, uint32_t *bm, uint32_t max_words,
                            uint32_t *actual_words)
{
    uint32_t count = 0;
    uint32_t i;

    memset(bm, 0, max_words * sizeof(uint32_t));
    if (!xdr_uint32_t(xdrs, &count)) {
        return false;
}
    *actual_words = count;
    for (i = 0; i < count; i++) {
        uint32_t w;

        if (!xdr_uint32_t(xdrs, &w)) {
            return false;
}
        if (i < max_words) {
            bm[i] = w;
}
        /* else: discard extra words we don't understand */
    }
    return true;
}

bool xdr_nfs4_time_encode(XDR *xdrs, const struct timespec *ts)
{
    int64_t sec = (int64_t)ts->tv_sec;
    uint32_t nsec = (uint32_t)ts->tv_nsec;

    if (!xdr_int64_t(xdrs, &sec)) {
        return false;
}
    return xdr_uint32_t(xdrs, &nsec);
}

bool xdr_nfs4_time_decode(XDR *xdrs, struct timespec *ts)
{
    int64_t sec = 0;
    uint32_t nsec = 0;

    if (!xdr_int64_t(xdrs, &sec)) {
        return false;
}
    if (!xdr_uint32_t(xdrs, &nsec)) {
        return false;
}
    ts->tv_sec = (time_t)sec;
    ts->tv_nsec = (long)nsec;
    return true;
}

/* -----------------------------------------------------------------------
 * fattr4 encode
 *
 * RFC 8881 §5.1: fattr4 = bitmap4 attrmask + opaque attr_vals<>.
 * The attr_vals is itself a length-prefixed opaque containing the
 * XDR-encoded attribute values in bitmap order.
 *
 * We encode into a two-pass approach:
 *   1. Build the actual bitmap (intersection of requested + supported).
 *   2. Encode bitmap, then length-prefixed attr_vals.
 *
 * To get the length, we encode attrs into a temporary buffer first.
 * ----------------------------------------------------------------------- */

/**
 * Precomputed supported attribute bitmap (P3 optimization).
 *
 * Built once at compile time instead of rebuilding on every
 * GETATTR/READDIR response.  Bits set correspond to the attrs
 * we support and encode_attr_vals() knows how to emit.
 *
 * Word 0 (bits 0-31): SUPPORTED_ATTRS(0) TYPE(1) FH_EXPIRE(2)
 *   CHANGE(3) SIZE(4) LINK_SUPPORT(5) SYMLINK_SUPPORT(6) FSID(8)
 *   LEASE_TIME(10) FILEHANDLE(19) FILEID(20) FS_LOCATIONS(24)
 *   MAXNAME(29) MAXREAD(30) MAXWRITE(31)
 * Word 1 (bits 32-63): MODE(33) NUMLINKS(35) OWNER(36)
 *   OWNER_GROUP(37) SPACE_AVAIL(42) SPACE_FREE(43)
 *   SPACE_TOTAL(44) SPACE_USED(45) TIME_ACCESS(47)
 *   TIME_ACCESS_SET(48) TIME_METADATA(52) TIME_MODIFY(53)
 *   TIME_MODIFY_SET(54) MOUNTED_ON_FILEID(55)
 *   FS_LAYOUT_TYPES(62) LAYOUT_HINT(63)
 * Word 2 (bits 64-95): LAYOUT_BLKSIZE(66)
 *   CHANGE_ATTR_TYPE(79) — RFC 7862 monotonic change-counter hint
 *   XATTR_SUPPORT(82) — RFC 8276
 */
static const uint32_t k_supported_bitmap[NFS4_BITMAP_WORDS] = {
    /* word 0 */ (1u << 0) | (1u << 1) | (1u << 2) | (1u << 3) |
                (1u << 4) | (1u << 5) | (1u << 6) | (1u << 8) |
                (1u << 10) | (1u << 19) | (1u << 20) | (1u << 24) |
                (1u << 29) | (1u << 30) | (1u << 31),
    /* word 1 */ (1u << (33-32)) | (1u << (35-32)) | (1u << (36-32)) |
                (1u << (37-32)) | (1u << (42-32)) | (1u << (43-32)) |
                (1u << (44-32)) | (1u << (45-32)) | (1u << (47-32)) |
                (1u << (48-32)) | (1u << (52-32)) | (1u << (53-32)) |
                (1u << (54-32)) | (1u << (55-32)) | (1u << (62-32)) |
                (1u << (FATTR4_LAYOUT_HINT - 32)),
    /* word 2 */ (1u << (66-64)) | (1u << (79-64)) | (1u << (82-64)) |
                (1u << (FATTR4_OPEN_ARGUMENTS - 64)),
};

static void build_supported_bitmap(uint32_t sup[NFS4_BITMAP_WORDS])
{
    memcpy(sup, k_supported_bitmap, sizeof(k_supported_bitmap));
}

/** Map mds_file_type to NFS4 file type (nfs_ftype4). */
/* Exported for xdr_ops_core.c READDIR inline attrs. */
uint32_t mds_type_to_nfs4(enum mds_file_type t)
{
    switch (t) {
    case MDS_FTYPE_REG:     return 1;  /* NF4REG */
    case MDS_FTYPE_DIR:     return 2;  /* NF4DIR */
    case MDS_FTYPE_BLKDEV:  return 3;  /* NF4BLK */
    case MDS_FTYPE_CHRDEV:  return 4;  /* NF4CHR */
    case MDS_FTYPE_SYMLINK: return 5;  /* NF4LNK */
    case MDS_FTYPE_SOCK:    return 6;  /* NF4SOCK */
    case MDS_FTYPE_FIFO:    return 7;  /* NF4FIFO */
    default:                return 0;
    }
}

/** Encode a uid/gid as an ASCII-numeric owner string (e.g. "1000"). */
static bool encode_owner_string(XDR *xdrs, uint64_t id)
{
    char buf[32];
    char *p = buf;
    int len;

    len = snprintf(buf, sizeof(buf), "%lu", (unsigned long)id);
    if (len < 0 || (size_t)len >= sizeof(buf)) {
        return false;
}
    return xdr_string_encode(xdrs, &p, (u_int)sizeof(buf));
}

/**
 * Encode attribute values into an XDR stream.
 * Attributes are encoded in bitmap order (lowest bit first).
 */

/**
 * Encode a pathname4 (RFC 8881 §2.1): component array, not a string.
 * "/" encodes as {count=0}.  "/foo/bar" encodes as {count=2, "foo", "bar"}.
 */
static bool xdr_encode_pathname4(XDR *xdrs, const char *abspath)
{
	char buf[MDS_MAX_PATH];
	char *components[32];
	uint32_t ncomp = 0;

	if (abspath == NULL || abspath[0] == '\0' ||
	    (abspath[0] == '/' && abspath[1] == '\0')) {
		/* Root: zero components. */
		uint32_t zero = 0;
		return xdr_uint32_t(xdrs, &zero);
	}

	(void)snprintf(buf, sizeof(buf), "%s", abspath);

	/* Split on '/' — skip leading slash. */
	char *p = buf;
	if (*p == '/') { p++; }
	while (*p != '\0' && ncomp < 32) {
		components[ncomp++] = p;
		char *sl = strchr(p, '/');
		if (sl != NULL) {
			*sl = '\0';
			p = sl + 1;
		} else {
			break;
		}
	}

	if (!xdr_uint32_t(xdrs, &ncomp)) {
		return false;
	}
	for (uint32_t i = 0; i < ncomp; i++) {
		if (!xdr_string_encode(xdrs, &components[i], 256)) {
			return false;
		}
	}
	return true;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static bool encode_attr_vals(XDR *xdrs, const struct mds_inode *inode,
                             const uint32_t actual[NFS4_BITMAP_WORDS],
                             const struct xdr_fattr_fs_space *fs_space,
                             uint64_t fsid_major,
                             uint64_t fsid_minor,
                             const char *ref_server,
                             const char *ref_rootpath,
                             const char *ref_fs_root)
{
    /* Word 0 attributes (bits 0-31), in order. */

    if (nfs4_bitmap_test(actual, FATTR4_SUPPORTED_ATTRS)) {
        uint32_t sup[NFS4_BITMAP_WORDS];

        build_supported_bitmap(sup);
        if (!xdr_nfs4_bitmap_encode(xdrs, sup, NFS4_BITMAP_WORDS)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_TYPE)) {
        uint32_t t = mds_type_to_nfs4(inode->type);

        if (!xdr_uint32_t(xdrs, &t)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_FH_EXPIRE_TYPE)) {
        uint32_t expire = 0; /* FH4_PERSISTENT */

        if (!xdr_uint32_t(xdrs, &expire)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_CHANGE)) {
        uint64_t change = inode->change;

        if (!xdr_uint64_t(xdrs, &change)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_SIZE)) {
        uint64_t size = inode->size;

        if (!xdr_uint64_t(xdrs, &size)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_LINK_SUPPORT)) {
        int32_t v = 1; /* true */

        if (!xdr_putbool(xdrs, v)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_SYMLINK_SUPPORT)) {
        int32_t v = 1; /* true */

        if (!xdr_putbool(xdrs, v)) {
            return false;
}
    }
    /* FATTR4_FSID (bit 8).
     * Each shard/partition has a distinct FSID (major = owner MDS id)
     * so clients treat referral crossings as separate filesystems. */
    if (nfs4_bitmap_test(actual, FATTR4_FSID)) {
        uint64_t major = fsid_major;
        uint64_t minor = fsid_minor;

        if (major == 0) {
            major = 1;
        }

        if (!xdr_uint64_t(xdrs, &major)) {
            return false;
}
        if (!xdr_uint64_t(xdrs, &minor)) {
            return false;
}
    }
    /* FATTR4_LEASE_TIME (bit 10). */
    if (nfs4_bitmap_test(actual, FATTR4_LEASE_TIME)) {
        uint32_t lease = SESSION_DEFAULT_LEASE_SEC;

        if (!xdr_uint32_t(xdrs, &lease)) {
            return false;
}
    }
    /* FATTR4_FILEHANDLE (bit 19). */
    if (nfs4_bitmap_test(actual, FATTR4_FILEHANDLE)) {
        if (!xdr_nfs4_fh_encode(xdrs, inode->fileid)) {
            return false;
}
    }
    /* FATTR4_FILEID (bit 20). */
    if (nfs4_bitmap_test(actual, FATTR4_FILEID)) {
        uint64_t fid = inode->fileid;

        if (!xdr_uint64_t(xdrs, &fid)) {
            return false;
}
    }
    /* FATTR4_FS_LOCATIONS (bit 24): fs_locations4 per RFC 8881 §11.12.
     * pathname4 = component array: "/" -> {0}, "/x/y" -> {2,"x","y"}.
     *
     * fs_root: path of this filesystem within the referring server.
     * locations[].server: target server address(es).
     * locations[].rootpath: path on the target server. */
    if (nfs4_bitmap_test(actual, FATTR4_FS_LOCATIONS)) {
        /* fs_root: for referrals, the junction path within this FSID.
         * For non-referrals, root of the current filesystem. */
        if (!xdr_encode_pathname4(xdrs,
                (ref_fs_root && ref_fs_root[0]) ? ref_fs_root : "/")) {
            return false;
        }
        if (ref_server != NULL && ref_server[0] != '\0') {
            /* 1 location entry. */
            uint32_t loc_count = 1;
            if (!xdr_uint32_t(xdrs, &loc_count)) {
                return false;
            }
            /* server<>: array of 1 utf8str_cis (IP or hostname). */
            {
                uint32_t srv_count = 1;
                char *srv = (char *)ref_server;
                if (!xdr_uint32_t(xdrs, &srv_count)) {
                    return false;
                }
                if (!xdr_string_encode(xdrs, &srv, 256)) {
                    return false;
                }
            }
            /* rootpath: pathname4 on target server.
             * "/" means target serves content at its export root. */
            if (!xdr_encode_pathname4(xdrs,
                    (ref_rootpath && ref_rootpath[0])
                    ? ref_rootpath : "/")) {
                return false;
            }
        } else {
            uint32_t loc_count = 0;
            if (!xdr_uint32_t(xdrs, &loc_count)) {
                return false;
            }
        }
    }

    if (nfs4_bitmap_test(actual, FATTR4_MAXNAME)) {
        uint32_t maxname = MDS_MAX_NAME;
        if (!xdr_uint32_t(xdrs, &maxname)) { return false; }
    }
    /* FATTR4_MAXREAD (bit 30). */
    if (nfs4_bitmap_test(actual, FATTR4_MAXREAD)) {
        uint64_t maxrd = 1048576; /* 1 MiB */
        if (!xdr_uint64_t(xdrs, &maxrd)) { return false; }
    }
    /* FATTR4_MAXWRITE (bit 31). */
    if (nfs4_bitmap_test(actual, FATTR4_MAXWRITE)) {
        uint64_t maxwr = 1048576; /* 1 MiB */
        if (!xdr_uint64_t(xdrs, &maxwr)) { return false; }
    }

    /* Word 1 attributes (bits 32-63), in order. */

    if (nfs4_bitmap_test(actual, FATTR4_MODE)) {
        uint32_t mode = inode->mode & 07777;

        if (!xdr_uint32_t(xdrs, &mode)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_NUMLINKS)) {
        uint32_t nlink = inode->nlink;

        if (!xdr_uint32_t(xdrs, &nlink)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_OWNER)) {
        if (!encode_owner_string(xdrs, inode->uid)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_OWNER_GROUP)) {
        if (!encode_owner_string(xdrs, inode->gid)) {
            return false;
}
    }
    /* FATTR4_SPACE_AVAIL (42), SPACE_FREE (43), SPACE_TOTAL (44).
     *
     * Two-stage policy:
     *   1. When the caller didn't supply FS-level space values
     *      (fs_space == NULL — typical when no quota context is
     *      wired into the compound), default to INT64_MAX rather
     *      than literal 0.  Emitting 0 makes statvfs(2) report
     *      Total/Free/Available = 0, which causes many user-space
     *      tools (rsync, tar with --check-links, container
     *      runtimes' cp pre-flight, backup agents, GUI file
     *      managers, Java/Go libs that pre-check getDiskSpace())
     *      to refuse writes or abort transfers even though the
     *      filesystem would accept them.
     *
     *   2. Whatever value we end up with, clamp to INT64_MAX
     *      before serialising.  The Linux kernel's NFSv4 statfs
     *      glue computes
     *        f_blocks = (tbytes + (sb->s_blocksize - 1)) >> blockbits;
     *      (fs/nfs/super.c::nfs_statfs).  With tbytes = UINT64_MAX
     *      the addition wraps to a tiny number and the shift
     *      yields 0, so `df` reports 0 even though the wire value
     *      was "unlimited".  This bites whenever an upstream
     *      "unlimited" producer (e.g. mds_quota_space_avail()'s
     *      no-rules fast path returns UINT64_MAX) feeds us.
     *      Clamping at the encoder boundary normalises the wire
     *      output regardless of which producer wrote fs_space.
     *
     * INT64_MAX (~9.2 EB) leaves a full 2^63 of headroom before
     * u64 overflow — safe for any realistic blockres
     * (sb->s_blocksize <= 1 MiB ⇒ blockres < 2^20 ≪ 2^63) — and
     * still reads as "effectively unlimited" to every consumer
     * we care about.  INT64_MAX is also the convention
     * BSD-derived NFS servers use for unlimited-quota responses,
     * so Linux clients have been seeing it forever. */
    {
        const uint64_t k_unlimited_space = (uint64_t)INT64_MAX;

        if (nfs4_bitmap_test(actual, FATTR4_SPACE_AVAIL)) {
            uint64_t avail = (fs_space != NULL)
                                 ? fs_space->space_avail
                                 : k_unlimited_space;
            if (avail > k_unlimited_space) {
                avail = k_unlimited_space;
            }
            if (!xdr_uint64_t(xdrs, &avail)) {
                return false;
            }
        }
        if (nfs4_bitmap_test(actual, FATTR4_SPACE_FREE)) {
            uint64_t sfree = (fs_space != NULL)
                                 ? fs_space->space_free
                                 : k_unlimited_space;
            if (sfree > k_unlimited_space) {
                sfree = k_unlimited_space;
            }
            if (!xdr_uint64_t(xdrs, &sfree)) {
                return false;
            }
        }
        if (nfs4_bitmap_test(actual, FATTR4_SPACE_TOTAL)) {
            uint64_t total = (fs_space != NULL)
                                 ? fs_space->space_total
                                 : k_unlimited_space;
            if (total > k_unlimited_space) {
                total = k_unlimited_space;
            }
            if (!xdr_uint64_t(xdrs, &total)) {
                return false;
            }
        }
    }
    if (nfs4_bitmap_test(actual, FATTR4_SPACE_USED)) {
        uint64_t used = inode->space_used;

        if (!xdr_uint64_t(xdrs, &used)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_TIME_ACCESS)) {
        if (!xdr_nfs4_time_encode(xdrs, &inode->atime)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_TIME_METADATA)) {
        if (!xdr_nfs4_time_encode(xdrs, &inode->ctime)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_TIME_MODIFY)) {
        if (!xdr_nfs4_time_encode(xdrs, &inode->mtime)) {
            return false;
}
    }
    if (nfs4_bitmap_test(actual, FATTR4_MOUNTED_ON_FILEID)) {
        uint64_t mounted_on_fileid = inode->fileid;

        if (!xdr_uint64_t(xdrs, &mounted_on_fileid)) {
            return false;
}
    }

    /* pNFS: fs_layout_types (RFC 5661 §5.12.1). */
    if (nfs4_bitmap_test(actual, FATTR4_FS_LAYOUT_TYPES)) {
        uint32_t count = 1;
        uint32_t lt = 4; /* LAYOUT4_FLEX_FILES */
        if (!xdr_uint32_t(xdrs, &count)) {
            return false;
        }
        if (!xdr_uint32_t(xdrs, &lt)) {
            return false;
        }
    }

    /* pNFS: layout_blksize (RFC 5661 §5.12.4). */
    if (nfs4_bitmap_test(actual, FATTR4_LAYOUT_BLKSIZE)) {
        uint32_t blksz = 65536;
        if (!xdr_uint32_t(xdrs, &blksz)) {
            return false;
        }
    }

    /* FATTR4_CHANGE_ATTR_TYPE (bit 79) — RFC 7862 §10.2.3.
     *
     * Advertises the semantic of our change counter for client-side
     * caching decisions.  We encode MONOTONIC_INCR: every mutation
     * strictly increases `inode->change` (the NDB `change_ctr`
     * column, updated via interpreted `incValue` on every
     * authoritative shim path).  A client seeing
     * stored_change == current_change therefore knows no mutation
     * has occurred, which makes directory delegations viable and
     * unlocks aggressive attribute caching.
     *
     * Bit 79 sits between LAYOUT_BLKSIZE(66) and XATTR_SUPPORT(82)
     * in the word-2 attr-value stream; attrs are emitted in bitmap
     * order per RFC 8881 §5.1. */
    if (nfs4_bitmap_test(actual, FATTR4_CHANGE_ATTR_TYPE)) {
        uint32_t ty = NFS4_CHANGE_TYPE_IS_MONOTONIC_INCR;
        if (!xdr_uint32_t(xdrs, &ty)) { return false; }
    }

    /* RFC 8276: xattr_support (bit 82). */
    if (nfs4_bitmap_test(actual, FATTR4_XATTR_SUPPORT)) {
        int32_t xs = 1; /* true — server supports xattr operations */
        if (!xdr_putbool(xdrs, xs)) {
            return false;
        }
    }

    /* RFC 9480: open_arguments4 (bit 86).
     *
     * Advertises which OPEN share-access, share-deny, want, claim,
     * and create-mode values the server supports.  Each field is a
     * bitmap4 (counted array of uint32) where bit N means
     * "enum value N is supported".  Pynfs DELEG24/25 gate on this
     * attribute being present in SUPPORTED_ATTRS.
     */
    if (nfs4_bitmap_test(actual, FATTR4_OPEN_ARGUMENTS)) {
        /* oa_share_access: READ(1) WRITE(2) BOTH(3) */
        { uint32_t n = 1, w = (1u<<1)|(1u<<2)|(1u<<3);
          if (!xdr_uint32_t(xdrs, &n)) { return false; }
          if (!xdr_uint32_t(xdrs, &w)) { return false; } }
        /* oa_share_deny: NONE(0) READ(1) WRITE(2) BOTH(3) */
        { uint32_t n = 1, w = (1u<<0)|(1u<<1)|(1u<<2)|(1u<<3);
          if (!xdr_uint32_t(xdrs, &n)) { return false; }
          if (!xdr_uint32_t(xdrs, &w)) { return false; } }
        /* oa_share_access_want: ANY(3) NO(4) CANCEL(5) DELEG_TIMESTAMPS(20) */
        { uint32_t n = 1, w = (1u<<3)|(1u<<4)|(1u<<5)|(1u<<20);
          if (!xdr_uint32_t(xdrs, &n)) { return false; }
          if (!xdr_uint32_t(xdrs, &w)) { return false; } }
        /* oa_open_claim: NULL(0) PREVIOUS(1) FH(4) */
        { uint32_t n = 1, w = (1u<<0)|(1u<<1)|(1u<<4);
          if (!xdr_uint32_t(xdrs, &n)) { return false; }
          if (!xdr_uint32_t(xdrs, &w)) { return false; } }
        /* oa_create_mode: UNCHECKED(0) GUARDED(1) EXCLUSIVE4_1(3) */
        { uint32_t n = 1, w = (1u<<0)|(1u<<1)|(1u<<3);
          if (!xdr_uint32_t(xdrs, &n)) { return false; }
          if (!xdr_uint32_t(xdrs, &w)) { return false; } }
    }
    return true;
}

bool xdr_nfs4_fattr_encode(XDR *xdrs, const struct mds_inode *inode,
                           const uint32_t requested[NFS4_BITMAP_WORDS])
{
    uint32_t actual[NFS4_BITMAP_WORDS];
    uint32_t i;

    /* Intersect requested with precomputed supported bitmap. */
    for (i = 0; i < NFS4_BITMAP_WORDS; i++) {
        actual[i] = requested[i] & k_supported_bitmap[i];
    }
    /* Clear SETATTR-only attrs. */
    actual[1] &= ~((uint32_t)1 << (FATTR4_TIME_ACCESS_SET - 32));
    actual[1] &= ~((uint32_t)1 << (FATTR4_TIME_MODIFY_SET - 32));
    actual[1] &= ~((uint32_t)1 << (FATTR4_LAYOUT_HINT - 32));
    /* Never advertise FS_LOCATIONS on this path.  This encoder has
     * no referral context, so emitting an empty fs_locations4 would
     * still tag the returned attrs with NFS_ATTR_FATTR_V4_LOCATIONS
     * on the Linux client and cause it to mark the inode as
     * S_AUTOMOUNT — every newly-accessed directory would then become
     * a local sub-mount and rmdir would fail with EBUSY.  Real
     * referrals go through xdr_nfs4_fattr_encode_ex. */
    actual[0] &= ~((uint32_t)1 << FATTR4_FS_LOCATIONS);

    /* Encode bitmap. */
    if (!xdr_nfs4_bitmap_encode(xdrs, actual, NFS4_BITMAP_WORDS)) {
        return false;
    }

    /* P2: Backpatch encoding — encode directly into outer stream,
     * then backpatch the length prefix.  Eliminates the temp
     * buffer + memcpy that was the second-biggest XDR overhead. */
    {
        uint32_t len_placeholder = 0;
        uint32_t len_pos = xdr_getpos(xdrs);

        if (!xdr_uint32_t(xdrs, &len_placeholder)) {
            return false;
        }

        uint32_t start_pos = xdr_getpos(xdrs);

        if (!encode_attr_vals(xdrs, inode, actual, NULL, 1, 0,
                              NULL, NULL, NULL)) {
            return false;
        }

        uint32_t end_pos = xdr_getpos(xdrs);
        uint32_t attr_len = end_pos - start_pos;

        /* Backpatch length. */
        xdr_setpos(xdrs, len_pos);
        if (!xdr_uint32_t(xdrs, &attr_len)) {
            return false;
        }
        xdr_setpos(xdrs, end_pos);
    }
    return true;
}

bool xdr_nfs4_fattr_encode_ex(XDR *xdrs, const struct mds_inode *inode,
                               const uint32_t requested[NFS4_BITMAP_WORDS],
                               const struct xdr_fattr_fs_space *fs_space,
                               uint64_t fsid_major,
                               uint64_t fsid_minor,
                               const char *ref_server,
                               const char *ref_rootpath,
                               const char *ref_fs_root)
{
    uint32_t actual[NFS4_BITMAP_WORDS];
    uint32_t i;

    for (i = 0; i < NFS4_BITMAP_WORDS; i++) {
        actual[i] = requested[i] & k_supported_bitmap[i];
    }
    actual[1] &= ~((uint32_t)1 << (FATTR4_TIME_ACCESS_SET - 32));
    actual[1] &= ~((uint32_t)1 << (FATTR4_TIME_MODIFY_SET - 32));
    actual[1] &= ~((uint32_t)1 << (FATTR4_LAYOUT_HINT - 32));
    /* Only advertise FS_LOCATIONS when we actually have a referral
     * to announce.  Emitting fs_locations4 on a non-referral FH
     * makes the Linux client mark the inode as S_AUTOMOUNT, which
     * turns every touched directory into a local sub-mount and
     * breaks rmdir with EBUSY (is_local_mountpoint() trips in
     * vfs_rmdir before the server ever sees the RMDIR RPC). */
    if (ref_server == NULL || ref_server[0] == '\0') {
        actual[0] &= ~((uint32_t)1 << FATTR4_FS_LOCATIONS);
    }

    if (!xdr_nfs4_bitmap_encode(xdrs, actual, NFS4_BITMAP_WORDS)) {
        return false;
    }

    /* P2: Backpatch encoding (same as xdr_nfs4_fattr_encode). */
    {
        uint32_t len_placeholder = 0;
        uint32_t len_pos = xdr_getpos(xdrs);

        if (!xdr_uint32_t(xdrs, &len_placeholder)) {
            return false;
        }

        uint32_t start_pos = xdr_getpos(xdrs);

        if (!encode_attr_vals(xdrs, inode, actual,
                             fs_space, fsid_major, fsid_minor,
                             ref_server,
                             ref_rootpath, ref_fs_root)) {
            return false;
        }

        uint32_t end_pos = xdr_getpos(xdrs);
        uint32_t attr_len = end_pos - start_pos;

        xdr_setpos(xdrs, len_pos);
        if (!xdr_uint32_t(xdrs, &attr_len)) {
            return false;
        }
        xdr_setpos(xdrs, end_pos);
    }
    return true;
}

static bool decode_layout_hint_attr(XDR *xdrs,
                                    struct nfs4_layout_hint *layout_hint)
{
    uint32_t layout_type = 0;
    uint32_t body_len = 0;

    if (!xdr_uint32_t(xdrs, &layout_type)) {
        return false;
    }
    if (!xdr_uint32_t(xdrs, &body_len)) {
        return false;
    }

    if (layout_type == LAYOUT4_FLEX_FILES &&
        body_len == HPC_HINT_BODY_SIZE) {
        char body[HPC_HINT_BODY_SIZE];

        if (!xdr_opaque_decode(xdrs, body, body_len)) {
            return false;
        }
        if (layout_hint != NULL &&
            hpc_hint_decode_xdr_body(body, body_len,
                                     &layout_hint->hpc) == MDS_OK) {
            layout_hint->present = true;
            layout_hint->layout_type = layout_type;
        }
        return true;
    }

    /*
     * Unsupported layout type or body size: consume the body so the
     * outer fattr4 stream stays aligned to the next attribute, but
     * leave layout_hint absent so the caller falls back to defaults.
     *
     * Loop in 4 KiB chunks rather than rejecting bodies larger than
     * one local buffer.  RFC 5662 does not bound layouthint4 body_len
     * and a future layout type may carry a larger advisory body.
     * xdr_opaque_decode consumes XDR alignment padding (rounding cnt
     * up to BYTES_PER_XDR_UNIT) on every call, so we keep each
     * intermediate chunk at a multiple of 4 (no per-chunk padding)
     * and let the final partial chunk consume the trailing pad of
     * the whole opaque.  In the common attr_buf<=4096-byte case the
     * loop runs once and behaves identically to the previous code.
     */
    if (body_len > 0) {
        char skip[4096];
        uint32_t consumed = 0;

        while (consumed < body_len) {
            uint32_t chunk = body_len - consumed;

            if (chunk > sizeof(skip)) {
                chunk = (uint32_t)sizeof(skip);
            }
            if (!xdr_opaque_decode(xdrs, skip, chunk)) {
                return false;
            }
            consumed += chunk;
        }
    }
    return true;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
bool xdr_nfs4_fattr_decode_ex(XDR *xdrs, struct mds_inode *inode,
                              uint32_t *mask,
                              struct nfs4_layout_hint *layout_hint)
{
    uint32_t bm[NFS4_BITMAP_WORDS];
    uint32_t bm_words;
    uint32_t attr_len;
    char attr_buf[4096];
    XDR attr_xdrs;

    *mask = 0;
    if (layout_hint != NULL) {
        memset(layout_hint, 0, sizeof(*layout_hint));
    }
    if (!xdr_nfs4_bitmap_decode(xdrs, bm, NFS4_BITMAP_WORDS, &bm_words)) {
        return false;
}

    /* Read attr_vals opaque. */
    if (!xdr_uint32_t(xdrs, &attr_len)) {
        return false;
}
    if (attr_len > sizeof(attr_buf)) {
        return false;
}
    if (attr_len > 0) {
        if (!xdr_opaque_decode(xdrs, attr_buf, attr_len)) {
            return false;
}
    }

    /* Decode attr values from the opaque buffer. */
    xdrmem_ncreate(&attr_xdrs, attr_buf, attr_len, XDR_DECODE);

    /* Only decode the attrs we care about for SETATTR. */
    if (nfs4_bitmap_test(bm, FATTR4_SIZE)) {
        if (!xdr_uint64_t(&attr_xdrs, &inode->size)) {
            return false;
}
        *mask |= MDS_ATTR_SIZE;
    }
    if (nfs4_bitmap_test(bm, FATTR4_MODE)) {
        if (!xdr_uint32_t(&attr_xdrs, &inode->mode)) {
            return false;
}
        *mask |= MDS_ATTR_MODE;
    }
    /* FATTR4_OWNER (bit 36): utf8str_cs numeric uid. */
    if (nfs4_bitmap_test(bm, FATTR4_OWNER)) {
        uint32_t olen;
        char obuf[64];
        if (!xdr_uint32_t(&attr_xdrs, &olen)) { return false; }
        if (olen > sizeof(obuf) - 1) { return false; }
        if (olen > 0) {
            uint32_t padded = (olen + 3) & ~3u;
            if (!xdr_opaque_decode(&attr_xdrs, obuf, padded)) { return false; }
        }
        obuf[olen] = '\0';
        inode->uid = (uint64_t)strtoul(obuf, NULL, 10);
        *mask |= MDS_ATTR_UID;
    }
    /* FATTR4_OWNER_GROUP (bit 37): utf8str_cs numeric gid. */
    if (nfs4_bitmap_test(bm, FATTR4_OWNER_GROUP)) {
        uint32_t olen;
        char obuf[64];
        if (!xdr_uint32_t(&attr_xdrs, &olen)) { return false; }
        if (olen > sizeof(obuf) - 1) { return false; }
        if (olen > 0) {
            uint32_t padded = (olen + 3) & ~3u;
            if (!xdr_opaque_decode(&attr_xdrs, obuf, padded)) { return false; }
        }
        obuf[olen] = '\0';
        inode->gid = (uint64_t)strtoul(obuf, NULL, 10);
        *mask |= MDS_ATTR_GID;
    }
    if (nfs4_bitmap_test(bm, FATTR4_TIME_ACCESS_SET)) {
        uint32_t set_it;
        if (!xdr_uint32_t(&attr_xdrs, &set_it)) {
            return false;
        }
        if (set_it == 1) { /* SET_TO_CLIENT_TIME4 */
            if (!xdr_nfs4_time_decode(&attr_xdrs, &inode->atime)) {
                return false;
            }
            *mask |= MDS_ATTR_ATIME;
        } else { /* SET_TO_SERVER_TIME4 */
            *mask |= MDS_ATTR_ATIME_NOW;
        }
    }
    if (nfs4_bitmap_test(bm, FATTR4_TIME_MODIFY)) {
        if (!xdr_nfs4_time_decode(&attr_xdrs, &inode->mtime)) {
            return false;
}
        *mask |= MDS_ATTR_MTIME;
    }
    if (nfs4_bitmap_test(bm, FATTR4_TIME_MODIFY_SET)) {
        uint32_t set_it;
        if (!xdr_uint32_t(&attr_xdrs, &set_it)) {
            return false;
        }
        if (set_it == 1) { /* SET_TO_CLIENT_TIME4 */
            if (!xdr_nfs4_time_decode(&attr_xdrs, &inode->mtime)) {
                return false;
            }
            *mask |= MDS_ATTR_MTIME;
        } else { /* SET_TO_SERVER_TIME4 */
            *mask |= MDS_ATTR_MTIME_NOW;
        }
    }
    if (nfs4_bitmap_test(bm, FATTR4_LAYOUT_HINT)) {
        if (!decode_layout_hint_attr(&attr_xdrs, layout_hint)) {
            return false;
        }
    }
    return true;
}


/* -----------------------------------------------------------------------
 * Per-operation result encoders
 * ----------------------------------------------------------------------- */

/** Encode change_info4: atomic(bool) + before(uint64) + after(uint64). */
bool encode_change_info(XDR *xdrs, uint64_t before, uint64_t after)
{
    int32_t atomic = 1; /* true */

    if (!xdr_putbool(xdrs, atomic)) {
        return false;
    }
    if (!xdr_uint64_t(xdrs, &before)) {
        return false;
    }
    return xdr_uint64_t(xdrs, &after);
}

/** All supported attrs bitmap for GETATTR responses. */
void build_all_requested(uint32_t bm[NFS4_BITMAP_WORDS])
{
    memset(bm, 0xFF, sizeof(uint32_t) * NFS4_BITMAP_WORDS);
}

/* -----------------------------------------------------------------------
 * pNFS layout XDR encoders
 * ----------------------------------------------------------------------- */


/* -----------------------------------------------------------------------
 * Top-level compound codec
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static bool decode_one_op(XDR *xdrs, struct nfs4_op *op)
{
    uint32_t opnum;

    if (!xdr_uint32_t(xdrs, &opnum)) {
        return false;
    }
    op->opnum = (enum nfs_opnum4)opnum;
    memset(&op->arg, 0, sizeof(op->arg));

    switch (op->opnum) {
    /* NFSv4.1 operations */
    case OP_SEQUENCE:        return decode_op_sequence(xdrs, op);
    case OP_EXCHANGE_ID:     return decode_op_exchange_id(xdrs, op);
    case OP_CREATE_SESSION:  return decode_op_create_session(xdrs, op);
    case OP_DESTROY_SESSION: return decode_op_destroy_session(xdrs, op);
    case OP_PUTROOTFH:       return true; /* no args */
    case OP_PUTFH:           return decode_op_putfh(xdrs, op);
    /* NOLINTNEXTLINE(bugprone-branch-clone) */
    case OP_GETFH:           return true; /* no args */
    case OP_SAVEFH:          return true; /* no args */
    case OP_RESTOREFH:       return true; /* no args */
    case OP_LOOKUP:          return decode_op_lookup(xdrs, op);
    case OP_GETATTR:         return decode_op_getattr(xdrs, op);
    case OP_SETATTR:         return decode_op_setattr(xdrs, op);
    case OP_CREATE:          return decode_op_create(xdrs, op);
    case OP_REMOVE:          return decode_op_remove(xdrs, op);
    case OP_RENAME:          return decode_op_rename(xdrs, op);
    case OP_LINK:            return decode_op_link(xdrs, op);
    case OP_READDIR:         return decode_op_readdir(xdrs, op);
    case OP_OPEN:            return decode_op_open(xdrs, op);
    case OP_CLOSE:           return decode_op_close(xdrs, op);
    case OP_OPENATTR:        return decode_op_openattr(xdrs, op);
    case OP_READ:            return decode_op_read(xdrs, op);
    case OP_WRITE:           return decode_op_write(xdrs, op);
    case OP_COMMIT: {
        /* COMMIT4args: offset(8) + count(4). */
        uint64_t commit_off;
        uint32_t commit_cnt;
        op->opnum = OP_COMMIT;
        if (!xdr_uint64_t(xdrs, &commit_off)) { return false; }
        if (!xdr_uint32_t(xdrs, &commit_cnt)) { return false; }
        return true;
    }
    case OP_LOOKUPP:         op->opnum = OP_LOOKUPP; return true;
    case OP_READLINK:        op->opnum = OP_READLINK; return true;
    /*
     * RFC 8881 §18.31 VERIFY / §18.19 NVERIFY.
     * Args = fattr4 = bitmap4 + opaque attr_vals<>.
     * Capture the complete fattr4 as raw bytes for the handler
     * to re-encode the current attrs and compare.
     */
    case OP_VERIFY:
    case OP_NVERIFY: {
        struct nfs4_arg_verify *vf = &op->arg.verify;
        uint32_t start_pos = xdr_getpos(xdrs);
        /* Skip bitmap4 (counted array). */
        uint32_t bm_count;
        if (!xdr_uint32_t(xdrs, &bm_count)) { return false; }
        for (uint32_t bi = 0; bi < bm_count; bi++) {
            uint32_t w;
            if (!xdr_uint32_t(xdrs, &w)) { return false; }
        }
        /* Skip attrlist4 opaque<>. */
        uint32_t alen;
        if (!xdr_uint32_t(xdrs, &alen)) { return false; }
        if (alen > NFS4_VERIFY_FATTR_MAX) { return false; }
        {
            char skip_buf[NFS4_VERIFY_FATTR_MAX];
            if (alen > 0 && !xdr_opaque_decode(xdrs, skip_buf, alen)) {
                return false;
            }
        }
        uint32_t end_pos = xdr_getpos(xdrs);
        uint32_t total = end_pos - start_pos;
        if (total > NFS4_VERIFY_FATTR_MAX) { return false; }
        /* Rewind and capture raw bytes. */
        xdr_setpos(xdrs, start_pos);
        if (!xdr_opaque_decode(xdrs, (char *)vf->fattr_raw, total)) {
            return false;
        }
        vf->fattr_raw_len = total;
        return true;
    }
    case OP_ACCESS:
        return xdr_uint32_t(xdrs, &op->arg.access.access);
    case OP_DELEGRETURN:
        /*
         * RFC 8881 §18.6.1 DELEGRETURN4args = stateid4 deleg_stateid.
         * op_delegreturn (compound_data_io.c) reads the stateid from
         * op->arg.close.stateid — the close-arg slot is shared with
         * delegreturn since both ops carry only a stateid.  Pre-fix,
         * the decoder wrote the stateid to a stack-local that went
         * out of scope before dispatch, leaving op->arg.close.stateid
         * zero.  op_delegreturn then called deleg_return() with the
         * special-zero stateid, which never matched a grant; the
         * compound completed with NFS4_OK in the status word but
         * without the encoder's per-op tail (DELEGRETURN was missing
         * from the encoder switch entirely — fixed below) so the
         * client never saw the reply at all and waited 30 s before
         * FIN.  Pynfs DELEG1 hung exactly here.
         */
        return xdr_nfs4_stateid_decode(xdrs, &op->arg.close.stateid);
    case OP_LOCK: {
        struct nfs4_arg_lock *a = &op->arg.lock;
        if (!xdr_uint32_t(xdrs, &a->lock_type)) { return false; }
        { int32_t reclaim_val;
          if (!xdr_getbool(xdrs, &reclaim_val)) { return false; }
          a->reclaim = (reclaim_val != 0); }
        if (!xdr_uint64_t(xdrs, &a->offset)) { return false; }
        if (!xdr_uint64_t(xdrs, &a->length)) { return false; }
        {
            int32_t new_lo;
            if (!xdr_getbool(xdrs, &new_lo)) { return false; }
            a->new_lock_owner = (new_lo != 0);
        }
        if (a->new_lock_owner) {
            uint32_t open_seqid;
            if (!xdr_uint32_t(xdrs, &open_seqid)) { return false; }
            if (!xdr_nfs4_stateid_decode(xdrs, &a->open_stateid)) { return false; }
            if (!xdr_uint32_t(xdrs, (uint32_t *)&a->lock_seqid)) { return false; }
            {
                /* lock_owner4 = {clientid(8), owner<>} */
                uint64_t lo_clientid;
                uint32_t olen;
                if (!xdr_uint64_t(xdrs, &lo_clientid)) { return false; }
                if (!xdr_uint32_t(xdrs, &olen)) { return false; }
                /* Reject overlong owners instead of clamping: two
                 * distinct owners truncated to the same 128 bytes
                 * would alias lock ownership.  Failing the decode
                 * yields NFS4ERR_BADXDR. */
                if (olen > sizeof(a->lock_owner)) { return false; }
                a->lock_owner_len = olen;
                if (olen > 0) {
                    if (!xdr_opaque_decode(xdrs, (char *)a->lock_owner, olen)) { return false; }
                }
            }
        } else {
            if (!xdr_nfs4_stateid_decode(xdrs, &a->lock_stateid)) { return false; }
            if (!xdr_uint32_t(xdrs, (uint32_t *)&a->lock_seqid)) { return false; }
        }
        return true;
    }
    case OP_LOCKT: {
        struct nfs4_arg_lockt *a = &op->arg.lockt;
        if (!xdr_uint32_t(xdrs, &a->lock_type)) { return false; }
        if (!xdr_uint64_t(xdrs, &a->offset)) { return false; }
        if (!xdr_uint64_t(xdrs, &a->length)) { return false; }
        {
            uint32_t olen;
            if (!xdr_uint64_t(xdrs, &a->clientid)) { return false; }
            if (!xdr_uint32_t(xdrs, &olen)) { return false; }
            /* Reject overlong owners — same aliasing concern as
             * OP_LOCK above; clamping must not silently truncate. */
            if (olen > sizeof(a->owner)) { return false; }
            a->owner_len = olen;
            if (olen > 0 &&
                !xdr_opaque_decode(xdrs, (char *)a->owner, olen)) { return false; }
        }
        return true;
    }
    case OP_LOCKU: {
        struct nfs4_arg_locku *a = &op->arg.locku;
        if (!xdr_uint32_t(xdrs, &a->lock_type)) { return false; }
        {
            uint32_t seqid;
            if (!xdr_uint32_t(xdrs, &seqid)) { return false; }
        }
        if (!xdr_nfs4_stateid_decode(xdrs, &a->lock_stateid)) { return false; }
        if (!xdr_uint64_t(xdrs, &a->offset)) { return false; }
        if (!xdr_uint64_t(xdrs, &a->length)) { return false; }
        return true;
    }
    case OP_RECLAIM_COMPLETE: return decode_op_reclaim_complete(xdrs, op);
    case OP_DESTROY_CLIENTID:
        return xdr_uint64_t(xdrs, &op->arg.destroy_clientid);
    case OP_SECINFO: {
        /* RFC 8881 §18.29.1 SECINFO4args: component4 name. */
        uint32_t name_len = 0;
        if (!xdr_uint32_t(xdrs, &name_len)) { return false; }
        if (name_len >= MDS_MAX_NAME) { return false; }
        if (name_len > 0 &&
            !xdr_opaque_decode(xdrs, op->arg.lookup.name, name_len)) {
            return false;
        }
        op->arg.lookup.name[name_len] = '\0';
        return true;
    }
    case OP_SECINFO_NO_NAME:
        /* RFC 8881 §18.45.1 SECINFO_NO_NAME4args: secinfo_style4. */
        return xdr_uint32_t(xdrs, &op->arg.secinfo_no_name.style);
    case OP_TEST_STATEID: {
        /* RFC 8881 §18.48: array of stateids to test. */
        uint32_t count, j;
        if (!xdr_uint32_t(xdrs, &count)) {
            return false;
        }
        /* Reject oversized arrays: the reply must carry one status
         * per requested stateid (RFC 8881 §18.48) and the result's
         * status_codes[] holds only 64 — a larger count would make
         * the encoder read past the array and leak memory to the
         * wire. */
        if (count > 64) {
            return false;
        }
        op->arg.test_stateid.count = count;
        for (j = 0; j < count; j++) {
            if (!xdr_nfs4_stateid_decode(xdrs, &op->arg.test_stateid.stateids[j])) {
                return false;
            }
        }
        return true;
    }
    case OP_FREE_STATEID:
        /*
         * RFC 8881 §18.38.1 FREE_STATEID4args = stateid4 fsa_stateid.
         * Pre-fix the decoder wrote the stateid into a stack-local
         * and dropped it on the floor, leaving op->arg.free_stateid
         * zeroed.  Pynfs CSID9 (testOpenFreestateidClose) caught
         * this: the FREE_STATEID(current_stateid) was decoded as the
         * special anonymous stateid (all-zero) and the dispatcher
         * then no-op'd it instead of returning NFS4ERR_LOCKS_HELD.
         */
        return xdr_nfs4_stateid_decode(xdrs, &op->arg.free_stateid);
    case OP_BIND_CONN_TO_SESSION: {
        /* RFC 8881 §18.34: session_id(16) + dir(4) + use_rdma(4).
         * Reuse destroy_session arg struct for the session_id. */
        uint32_t dir, rdma;
        return xdr_opaque_decode(xdrs,
                   (char *)op->arg.destroy_session.session_id, 16)
            && xdr_uint32_t(xdrs, &dir)
            && xdr_uint32_t(xdrs, &rdma);
    }
    case OP_BACKCHANNEL_CTL: {
        /*
         * RFC 8881 §18.33.1 BACKCHANNEL_CTL4args:
         *   uint32_t            bca_cb_program;
         *   callback_sec_parms4 bca_sec_parms<>;
         *
         * Same callback_sec_parms4 wire form as CREATE_SESSION's
         * csa_sec_parms<> entry: flavor (uint32) + per-flavor body
         * (AUTH_NONE void / AUTH_SYS authsys_parms / RPCSEC_GSS
         * gss_cb_handles4).  We capture the FIRST entry into
         * a->cb_sec, mirroring decode_op_create_session.  Subsequent
         * entries are decoded only to advance the wire cursor.
         *
         * Pynfs DELEG7 (testCBSecParmsChange) verifies that the
         * very next CB_RECALL after this BACKCHANNEL_CTL carries
         * the new uid/gid — so cb_sec_set MUST be true when at
         * least one entry was present.
         */
        struct nfs4_arg_backchannel_ctl *a = &op->arg.backchannel_ctl;
        uint32_t count;

        memset(a, 0, sizeof(*a));
        if (!xdr_uint32_t(xdrs, &a->cb_prog)) {
            return false;
        }
        a->cb_prog_set = true;
        if (!xdr_uint32_t(xdrs, &count)) {
            return false;
        }
        for (uint32_t i = 0; i < count; i++) {
            uint32_t flavor;
            if (!xdr_uint32_t(xdrs, &flavor)) {
                return false;
            }
            bool capture = (i == 0);
            if (capture) {
                a->cb_sec.flavor = (flavor == NFS4_CB_AUTH_SYS)
                    ? NFS4_CB_AUTH_SYS : NFS4_CB_AUTH_NONE;
                a->cb_sec_set = true;
            }
            if (flavor == NFS4_CB_AUTH_SYS) {
                uint32_t stamp, mname_len, uid, gid, ngids;
                if (!xdr_uint32_t(xdrs, &stamp)) { return false; }
                if (!xdr_uint32_t(xdrs, &mname_len)) { return false; }
                if (mname_len > 255) { return false; }
                char mname_buf[256];
                if (mname_len > 0) {
                    uint32_t padded = (mname_len + 3) & ~3u;
                    if (padded > sizeof(mname_buf)) { return false; }
                    if (!xdr_opaque_decode(xdrs, mname_buf, padded)) {
                        return false;
                    }
                }
                if (!xdr_uint32_t(xdrs, &uid)) { return false; }
                if (!xdr_uint32_t(xdrs, &gid)) { return false; }
                if (!xdr_uint32_t(xdrs, &ngids)) { return false; }
                uint32_t aux_buf[NFS4_CB_AUX_GIDS_MAX] = {0};
                uint32_t kept_aux = 0;
                for (uint32_t gi = 0; gi < ngids; gi++) {
                    uint32_t aux;
                    if (!xdr_uint32_t(xdrs, &aux)) { return false; }
                    if (capture && kept_aux < NFS4_CB_AUX_GIDS_MAX) {
                        aux_buf[kept_aux++] = aux;
                    }
                }
                if (capture) {
                    a->cb_sec.sys_stamp = stamp;
                    a->cb_sec.sys_uid = uid;
                    a->cb_sec.sys_gid = gid;
                    a->cb_sec.sys_machname_len = mname_len;
                    if (mname_len > 0) {
                        memcpy(a->cb_sec.sys_machname, mname_buf,
                               mname_len);
                    }
                    a->cb_sec.sys_ngids = kept_aux;
                    if (kept_aux > 0) {
                        memcpy(a->cb_sec.sys_gids, aux_buf,
                               kept_aux * sizeof(uint32_t));
                    }
                }
            }
        }
        return true;
    }
    /* pNFS layout operations */
    case OP_LAYOUTGET:       return decode_op_layoutget(xdrs, op);
    case OP_GETDEVICEINFO:   return decode_op_getdeviceinfo(xdrs, op);
    case OP_LAYOUTRETURN:    return decode_op_layoutreturn(xdrs, op);
    case OP_LAYOUTCOMMIT:    return decode_op_layoutcommit(xdrs, op);
    case OP_LAYOUTERROR:     return decode_op_layouterror(xdrs, op);
    case OP_LAYOUTSTATS:     return decode_op_layoutstats(xdrs, op);
    /* Directory delegation (RFC 8881 §18.39) */
    case OP_GET_DIR_DELEGATION: return decode_op_get_dir_delegation(xdrs, op);
    /* NFSv4.2 operations (RFC 7862) */
    case OP_ALLOCATE:        return decode_op_allocate(xdrs, op);
    case OP_DEALLOCATE:      return decode_op_deallocate(xdrs, op);
    case OP_COPY:            return decode_op_copy(xdrs, op);
    case OP_COPY_NOTIFY:     return decode_op_copy_notify(xdrs, op);
    case OP_IO_ADVISE:       return decode_op_io_advise(xdrs, op);
    case OP_OFFLOAD_CANCEL:  return decode_op_offload_cancel(xdrs, op);
    case OP_OFFLOAD_STATUS:  return decode_op_offload_status(xdrs, op);
    case OP_READ_PLUS:       return decode_op_read_plus(xdrs, op);
    case OP_SEEK:            return decode_op_seek(xdrs, op);
    case OP_WRITE_SAME:      return decode_op_write_same(xdrs, op);
    case OP_CLONE:           return decode_op_clone(xdrs, op);
    /* RFC 8276 extended attribute operations */
    case OP_GETXATTR:        return decode_op_getxattr(xdrs, op);
    case OP_SETXATTR:        return decode_op_setxattr(xdrs, op);
    case OP_LISTXATTRS:      return decode_op_listxattrs(xdrs, op);
    case OP_REMOVEXATTR:     return decode_op_removexattr(xdrs, op);
    default:
        /*
         * Unknown op: preserve the raw opnum so dispatch_op() can
         * return NFS4ERR_OP_ILLEGAL per RFC 8881 §2.10.6.4.
         * We cannot skip unknown args, so signal the caller to
         * stop decoding further ops by returning true with the
         * raw opnum intact (checked in nfs4_decode_compound_args).
         */
        return true;
    }
}

int nfs4_decode_compound_args(XDR *xdrs,
                              char *tag, uint32_t tag_maxlen,
                              uint32_t *minorversion,
                              struct nfs4_op *ops, uint32_t max_ops,
                              uint32_t *op_count)
{
    uint32_t tag_len = 0;
    uint32_t count;
    uint32_t i;

    /* tag: utf8str_cs */
    if (!xdr_uint32_t(xdrs, &tag_len)) {
        return -1;
}
    if (tag_len >= tag_maxlen) {
        return -1;
}
    if (tag_len > 0) {
        if (!xdr_opaque_decode(xdrs, tag, tag_len)) {
            return -1;
}
    }
    tag[tag_len] = '\0';

    /* minorversion */
    if (!xdr_uint32_t(xdrs, minorversion)) {
        return -1;
}

    /* argarray count */
    if (!xdr_uint32_t(xdrs, &count)) {
        return -1;
}
    /*
     * Distinct return code so the caller can translate this to the
     * RFC-mandated NFS4ERR_TOO_MANY_OPS rather than a generic XDR
     * failure (RFC 5661 §15.2 / RFC 8881 §2.10.6.1.2).  Without this,
     * the COMPOUND reply ends up with an empty resarray and Linux /
     * pynfs clients crash on resarray[0].
     */
    if (count > max_ops) {
        return -2;
}

    for (i = 0; i < count; i++) {
        if (!decode_one_op(xdrs, &ops[i])) {
            /*
             * Report how many ops decoded successfully so the
             * caller can still inspect ops[0..i-1] (e.g. to read
             * the SEQUENCE session_id and enforce per-session
             * limits before returning NFS4ERR_BADXDR).  This is
             * load-bearing for pynfs SEQ6 (testRequestTooBig)
             * where the LOOKUP after SEQUENCE intentionally
             * carries an oversize component4 that fails the
             * MDS_MAX_NAME bound check inside decode_op_lookup. */
            *op_count = i;
            return -1;
}
        /*
         * RFC 8881 §2.10.6.4: if decode_one_op returned an opnum
         * that dispatch_op does not recognise, we cannot decode
         * subsequent ops (unknown arg size).  Stop here and let
         * compound_process return NFS4ERR_OP_ILLEGAL for this op.
         */
        switch (ops[i].opnum) {
        case OP_SEQUENCE: case OP_EXCHANGE_ID:
        case OP_CREATE_SESSION: case OP_DESTROY_SESSION:
        case OP_ACCESS: case OP_PUTROOTFH: case OP_PUTFH:
        case OP_GETFH: case OP_SAVEFH: case OP_RESTOREFH:
        case OP_LOOKUP: case OP_GETATTR: case OP_SETATTR:
        case OP_CREATE: case OP_REMOVE: case OP_RENAME:
        case OP_LINK: case OP_READDIR: case OP_OPEN:
        case OP_CLOSE: case OP_OPENATTR: case OP_READ:
        case OP_WRITE: case OP_COMMIT:
        case OP_LOOKUPP: case OP_READLINK:
        case OP_DELEGRETURN: case OP_OPEN_DOWNGRADE:
        case OP_LOCK: case OP_LOCKT: case OP_LOCKU:
        case OP_LAYOUTGET: case OP_GETDEVICEINFO:
        case OP_LAYOUTRETURN: case OP_LAYOUTCOMMIT:
        case OP_ALLOCATE: case OP_COPY: case OP_COPY_NOTIFY:
        case OP_DEALLOCATE: case OP_IO_ADVISE:
        case OP_LAYOUTERROR: case OP_LAYOUTSTATS:
        case OP_OFFLOAD_CANCEL: case OP_OFFLOAD_STATUS:
        case OP_READ_PLUS: case OP_SEEK:
        case OP_WRITE_SAME: case OP_CLONE:
        case OP_GETXATTR: case OP_SETXATTR:
        case OP_LISTXATTRS: case OP_REMOVEXATTR:
        case OP_RECLAIM_COMPLETE:
        case OP_DESTROY_CLIENTID:
        case OP_BIND_CONN_TO_SESSION:
        case OP_BACKCHANNEL_CTL:
        case OP_TEST_STATEID:
        case OP_FREE_STATEID:
        case OP_GET_DIR_DELEGATION:
        case OP_SECINFO:
        case OP_SECINFO_NO_NAME:
        case OP_VERIFY:
        case OP_NVERIFY:
            break;  /* known op — continue decoding */
        default:
            /* Unknown op — stop decoding, include this op in count. */
            *op_count = i + 1;
            return 0;
        }
    }
    *op_count = count;
    return 0;
}

/*
 * Per-op exception to the "error status = empty body" rule.
 *
 * Previously GET_DIR_DELEGATION abused the outer op status
 * NFS4ERR_DIRDELEG_UNAVAIL + a trailing bool body.  That outer
 * status halted the compound (RFC 8881 §2.6.3.1.1) and dropped any
 * bundled trailing GETATTR, producing EIO on Linux clients.  GDD
 * now returns NFS4_OK with the UNAVAIL state carried in the inner
 * gddrnf_status discriminator (see encode_res_get_dir_delegation),
 * so no per-op exception is needed here.
 *
 * LOCK / LOCKT under NFS4ERR_DENIED carry a lock4denied body
 * (RFC 8881 §18.10.4 / §18.11.4): the conflicting range, lock_type,
 * and lock_owner are encoded after the status word so the client
 * can report the conflict to its application.
 */
static bool encode_error_body(XDR *xdrs, const struct nfs4_result *r)
{
    if (r->status == NFS4ERR_DENIED &&
        (r->opnum == OP_LOCK || r->opnum == OP_LOCKT)) {
        const struct nfs4_lock_denied *d = &r->res.lock.denied;
        uint64_t offset    = d->offset;
        uint64_t length    = d->length;
        uint32_t lock_type = d->lock_type;
        uint64_t clientid  = d->clientid;
        uint32_t owner_len = d->owner_len;

        if (!xdr_uint64_t(xdrs, &offset))    { return false; }
        if (!xdr_uint64_t(xdrs, &length))    { return false; }
        if (!xdr_uint32_t(xdrs, &lock_type)) { return false; }
        /* lock_owner4 = clientid + owner<> */
        if (!xdr_uint64_t(xdrs, &clientid))  { return false; }
        if (!xdr_uint32_t(xdrs, &owner_len)) { return false; }
        if (owner_len > 0) {
            if (!xdr_opaque_encode(xdrs, (const char *)d->owner,
                                   owner_len)) {
                return false;
            }
        }
        return true;
    }
    (void)xdrs;
    (void)r;
    return true;
}

static bool encode_one_result(XDR *xdrs, const struct nfs4_result *r)
{
    uint32_t opnum = (uint32_t)r->opnum;
    uint32_t status = (uint32_t)r->status;

    if (!xdr_uint32_t(xdrs, &opnum)) {
        return false;
    }
    if (!xdr_uint32_t(xdrs, &status)) {
        return false;
    }

    /*
     * If the operation failed, only the status word is encoded
     * unless encode_error_body() knows a per-op exception.
     */
    if (r->status != NFS4_OK) {
        return encode_error_body(xdrs, r);
    }

    switch (r->opnum) {
    case OP_SEQUENCE:        return encode_res_sequence(xdrs, r);
    case OP_EXCHANGE_ID:     return encode_res_exchange_id(xdrs, r);
    case OP_CREATE_SESSION:  return encode_res_create_session(xdrs, r);
    /* NOLINTNEXTLINE(bugprone-branch-clone) */
    case OP_DESTROY_SESSION: return true; /* status only */
    case OP_DESTROY_CLIENTID: return true; /* status only */
    case OP_BIND_CONN_TO_SESSION: {
        /* Echo back session_id + dir(BOTH=0) + use_rdma(false=0). */
        const struct nfs4_res_sequence *sq = &r->res.sequence;
        uint32_t dir = 0, rdma = 0;
        return xdr_opaque_encode(xdrs, (const char *)sq->session_id, 16)
            && xdr_uint32_t(xdrs, &dir)
            && xdr_uint32_t(xdrs, &rdma);
    }
    case OP_PUTROOTFH:
    case OP_PUTFH:
        return true;
    case OP_GETFH:           return encode_res_getfh(xdrs, r);
    /* NOLINTNEXTLINE(bugprone-branch-clone) */
    case OP_SAVEFH:
    case OP_RESTOREFH:
        return true;
    case OP_LOOKUP:          return true;
    case OP_GETATTR:         return encode_res_getattr(xdrs, r);
    case OP_SETATTR:
        /* SETATTR4res = bitmap4 attrsset (which attrs were set). */
        {
            const uint32_t empty_bm[NFS4_BITMAP_WORDS] = {0, 0};

            return xdr_nfs4_bitmap_encode(xdrs, empty_bm,
                                          NFS4_BITMAP_WORDS);
        }
    case OP_CREATE:          return encode_res_create(xdrs, r);
    case OP_REMOVE:          return encode_res_remove(xdrs, r);
    case OP_RENAME:          return encode_res_rename(xdrs, r);
    case OP_LINK:            return encode_res_link(xdrs, r);
    case OP_READDIR:         return encode_res_readdir(xdrs, r);
    case OP_OPEN:            return encode_res_open(xdrs, r);
    case OP_CLOSE:           return encode_res_close(xdrs, r);
    /* RFC 8881 §18.10.4 LOCK4res: NFS4_OK → stateid4. */
    case OP_LOCK:
        return xdr_nfs4_stateid_encode(xdrs, &r->res.lock.stateid);
    /* RFC 8881 §18.11.4 LOCKT4res: NFS4_OK → void. */
    case OP_LOCKT:
        return true;
    /* RFC 8881 §18.12.4 LOCKU4res: NFS4_OK → stateid4. */
    case OP_LOCKU:
        return xdr_nfs4_stateid_encode(xdrs, &r->res.locku.stateid);
    case OP_OPENATTR:        return true; /* status-only */
    /*
     * RFC 8881 §18.6.4 DELEGRETURN4res — status-only.  The status
     * word is already emitted by encode_one_result before the
     * switch; nothing follows it on the wire.  Pre-fix this case
     * was missing and fell through to `default: return false;`,
     * which made the caller abort the entire compound encode and
     * silently drop the reply (pynfs DELEG1 hang root cause).
     */
    case OP_DELEGRETURN:     return true; /* RFC 8881 §18.6.4 */
    /*
     * RFC 8881 §18.18.4 OPEN_DOWNGRADE4res — stateid4 on NFS4_OK.
     * op_open_downgrade stores the new stateid in r->res.close.stateid
     * (close-result slot is shared, same as the close stateid path).
     */
    case OP_OPEN_DOWNGRADE:
        return xdr_nfs4_stateid_encode(xdrs, &r->res.close.stateid);
    case OP_READ:            return encode_res_read(xdrs, r);
    case OP_WRITE:           return encode_res_write(xdrs, r);
    case OP_COMMIT: {
        /* COMMIT4resok: writeverf4 (8 bytes). */
        uint64_t verf = r->res.commit.write_verf;
        return xdr_uint64_t(xdrs, &verf);
    }
    case OP_SECINFO:
    case OP_SECINFO_NO_NAME: {
        /* secinfo4 array: count + per-entry flavor. */
        const struct nfs4_res_secinfo *si = &r->res.secinfo;
        uint32_t cnt = si->count;
        uint32_t k;
        if (!xdr_uint32_t(xdrs, &cnt)) {
            return false;
        }
        for (k = 0; k < cnt; k++) {
            uint32_t flav = si->flavors[k];
            if (!xdr_uint32_t(xdrs, &flav)) {
                return false;
            }
        }
        return true;
    }
    case OP_LOOKUPP:         return true; /* status only */
    case OP_VERIFY:          return true; /* status only */
    case OP_NVERIFY:         return true; /* status only */
    case OP_READLINK: {
        /* READLINK4resok: linktext4 (utf8str_cs = opaque<>). */
        uint32_t len = r->res.readlink.target_len;
        const char *p = r->res.readlink.target;
        if (!xdr_uint32_t(xdrs, &len)) { return false; }
        if (len > 0 && !xdr_opaque_encode(xdrs, p, len)) {
            return false;
        }
        return true;
    }
    /* pNFS layout operations */
    case OP_LAYOUTGET:       return encode_res_layoutget(xdrs, r);
    case OP_GETDEVICEINFO:   return encode_res_getdeviceinfo(xdrs, r);
    case OP_LAYOUTRETURN:    return encode_res_layoutreturn(xdrs, r);
    case OP_LAYOUTCOMMIT:    return encode_res_layoutcommit(xdrs, r);
    /* NFSv4.2 operations (RFC 7862) */
    /* NOLINTNEXTLINE(bugprone-branch-clone) */
    case OP_ALLOCATE:        return true; /* status-only */
    case OP_DEALLOCATE:      return true; /* status-only */
    case OP_LAYOUTERROR:     return true; /* status-only */
    case OP_LAYOUTSTATS:     return true; /* status-only */
    case OP_OFFLOAD_CANCEL:  return true; /* status-only */
    case OP_WRITE_SAME:      return true; /* status-only */
    case OP_CLONE:           return true; /* status-only */
    case OP_RECLAIM_COMPLETE: return true; /* status-only */
    case OP_FREE_STATEID: return true; /* status-only per RFC 5661 §18.38 */
    /*
     * RFC 8881 §18.33.2 BACKCHANNEL_CTL4res — status-only.  Like
     * DELEGRETURN, the status word is already emitted before the
     * switch and nothing follows it on the wire.
     */
    case OP_BACKCHANNEL_CTL: return true;
    case OP_TEST_STATEID: {
        /* RFC 8881 §18.48: return per-stateid status. */
        const struct nfs4_res_test_stateid *ts = &r->res.test_stateid;
        uint32_t cnt = ts->count;
        uint32_t k;
        if (!xdr_uint32_t(xdrs, &cnt)) {
            return false;
        }
        for (k = 0; k < cnt; k++) {
            uint32_t st_val = ts->status_codes[k];
            if (!xdr_uint32_t(xdrs, &st_val)) {
                return false;
            }
        }
        return true;
    }
    case OP_ACCESS: {
        const struct nfs4_res_access *a = &r->res.access;
        uint32_t supported = a->supported;
        uint32_t access = a->access;
        return xdr_uint32_t(xdrs, &supported)
            && xdr_uint32_t(xdrs, &access);
    }
    case OP_IO_ADVISE:       return encode_res_io_advise(xdrs, r);
    case OP_GET_DIR_DELEGATION: return encode_res_get_dir_delegation(xdrs, r);
    case OP_SEEK:            return encode_res_seek(xdrs, r);
    case OP_READ_PLUS:       return encode_res_read_plus(xdrs, r);
    case OP_COPY:            return encode_res_copy(xdrs, r);
    case OP_COPY_NOTIFY:     return encode_res_copy_notify(xdrs, r);
    case OP_OFFLOAD_STATUS:  return encode_res_offload_status(xdrs, r);
    /* RFC 8276 extended attribute operations */
    case OP_GETXATTR:        return encode_res_getxattr(xdrs, r);
    case OP_SETXATTR:        return encode_res_setxattr(xdrs, r);
    case OP_LISTXATTRS:      return encode_res_listxattrs(xdrs, r);
    case OP_REMOVEXATTR:     return encode_res_removexattr(xdrs, r);
    default:
        return false;
    }
}

int nfs4_encode_compound_res(XDR *xdrs,
                             enum nfs4_status status,
                             const char *tag,
                             const struct nfs4_result *results,
                             uint32_t result_count)
{
    uint32_t nfs_status = (uint32_t)status;
    uint32_t tag_len;
    uint32_t i;
    char *tag_ptr;

    /* status */
    if (!xdr_uint32_t(xdrs, &nfs_status)) {
        return -1;
}

    /* tag (echoed from request) */
    tag_len = (tag != NULL) ? (uint32_t)strlen(tag) : 0;
    tag_ptr = (char *)tag;
    if (!xdr_uint32_t(xdrs, &tag_len)) {
        return -1;
}
    if (tag_len > 0) {
        if (!xdr_opaque_encode(xdrs, tag_ptr, tag_len)) {
            return -1;
}
    }

    /* resarray count */
    if (!xdr_uint32_t(xdrs, &result_count)) {
        return -1;
}

    for (i = 0; i < result_count; i++) {
        if (!encode_one_result(xdrs, &results[i])) {
            return -1;
}
    }
    return 0;
}
bool xdr_nfs4_fattr_decode(XDR *xdrs, struct mds_inode *inode,
                           uint32_t *mask)
{
    return xdr_nfs4_fattr_decode_ex(xdrs, inode, mask, NULL);
}

/* -----------------------------------------------------------------------
 * RPC header helpers
 * ----------------------------------------------------------------------- */

/** Read an opaque_auth, preserving the flavor. */
/** Skip an ONC-RPC auth body (flavor + opaque<400>). */
static bool read_auth(XDR *xdrs, uint32_t *out_flavor)
{
    uint32_t flavor, len;

    if (!xdr_uint32_t(xdrs, &flavor)) {
        return false;
    }
    if (!xdr_uint32_t(xdrs, &len)) {
        return false;
    }
    if (len > 400) { /* MAX_AUTH_BYTES */
        return false;
    }
    if (len > 0) {
        char skip[400];

        if (!xdr_opaque_decode(xdrs, skip, len)) {
            return false;
        }
    }
    if (out_flavor != NULL) {
        *out_flavor = flavor;
    }
    return true;
}

/**
 * Parse an RPCSEC_GSS credential body (RFC 2203 s5.2.2).
 *
 * The XDR cursor must be positioned right after the credential
 * flavor uint32.  On success the body is consumed and @p out is
 * populated.  On failure the stream position is undefined.
 */
static bool read_gss_cred_body(XDR *xdrs, uint32_t body_len,
                                struct rpc_gss_cred *out)
{
    if (body_len < 20) { /* 5 x uint32 minimum */
        return false;
    }

    if (!xdr_uint32_t(xdrs, &out->version)) {
        return false;
    }
    if (out->version != 1) { /* RPCSEC_GSS version 1 only */
        return false;
    }
    if (!xdr_uint32_t(xdrs, &out->procedure)) {
        return false;
    }
    if (!xdr_uint32_t(xdrs, &out->seq_num)) {
        return false;
    }
    if (!xdr_uint32_t(xdrs, &out->service)) {
        return false;
    }
    if (out->service < 1 || out->service > 3) {
        return false; /* Invalid GSS service */
    }

    /* ctx_handle: opaque<> — length-prefixed. */
    uint32_t hlen;
    if (!xdr_uint32_t(xdrs, &hlen)) {
        return false;
    }
    if (hlen > sizeof(out->ctx_handle)) {
        return false;
    }
    if (hlen > 0) {
        if (!xdr_opaque_decode(xdrs, (char *)out->ctx_handle,
                               hlen)) {
            return false;
        }
    }
    out->ctx_handle_len = hlen;
    return true;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int rpc_decode_call_header(XDR *xdrs, uint32_t *xid,
                           uint32_t *prog, uint32_t *vers, uint32_t *proc,
                           uint32_t *cred_flavor,
                           struct rpc_gss_cred *gss_cred,
                           uint32_t *out_uid, uint32_t *out_gid,
                           uint32_t *out_aux_gids, uint32_t *out_aux_gid_count)
{
    uint32_t msg_type, rpcvers;

    if (!xdr_uint32_t(xdrs, xid)) {
        return -1;
}
    if (!xdr_uint32_t(xdrs, &msg_type)) {
        return -1;
}
    if (msg_type != 0) { /* CALL */
        return -1;
}
    if (!xdr_uint32_t(xdrs, &rpcvers)) {
        return -1;
}
    if (rpcvers != 2) {
        return -1;
}
    if (!xdr_uint32_t(xdrs, prog)) {
        return -1;
}
    if (!xdr_uint32_t(xdrs, vers)) {
        return -1;
}
    if (!xdr_uint32_t(xdrs, proc)) {
        return -1;
}

    /* Read credential (flavor + body). */
    uint32_t cred_flav, cred_len;
    if (!xdr_uint32_t(xdrs, &cred_flav)) {
        return -1;
    }
    if (!xdr_uint32_t(xdrs, &cred_len)) {
        return -1;
    }
    if (cred_len > 400) {
        return -1;
    }
    if (cred_flavor != NULL) {
        *cred_flavor = cred_flav;
    }

    if (cred_flav == 6 && gss_cred != NULL && cred_len >= 20) {
        /* RPCSEC_GSS — parse structured credential body. */
        memset(gss_cred, 0, sizeof(*gss_cred));
        if (!read_gss_cred_body(xdrs, cred_len, gss_cred)) {
            return -1; /* Malformed GSS credential. */
        }
    } else if (cred_flav == 1 && cred_len >= 12) {
        /* AUTH_SYS: stamp(4) + machinename(var) + uid(4) + gid(4) + aux_gids(var). */
        uint32_t stamp, mname_len, uid, gid, ngids;
        if (!xdr_uint32_t(xdrs, &stamp)) { return -1; }
        if (!xdr_uint32_t(xdrs, &mname_len)) { return -1; }
        if (mname_len > 255) { return -1; }
        if (mname_len > 0) {
            char mskip[256];
            uint32_t padded = (mname_len + 3) & ~3u;
            if (!xdr_opaque_decode(xdrs, mskip, padded)) {
                return -1;
            }
        }
        if (!xdr_uint32_t(xdrs, &uid)) { return -1; }
        if (!xdr_uint32_t(xdrs, &gid)) { return -1; }
        if (out_uid != NULL) { *out_uid = uid; }
        if (out_gid != NULL) { *out_gid = gid; }
        /* Read auxiliary GIDs array. */
        if (!xdr_uint32_t(xdrs, &ngids)) { return -1; }
        {
            uint32_t gi, stored = 0;
            for (gi = 0; gi < ngids; gi++) {
                uint32_t aux;
                if (!xdr_uint32_t(xdrs, &aux)) { return -1; }
                if (gi < 16 && out_aux_gids != NULL) {
                    out_aux_gids[gi] = aux;
                    stored = gi + 1;
                }
            }
            if (out_aux_gid_count != NULL) {
                *out_aux_gid_count = stored;
            }
        }
    } else if (cred_len > 0) {
        /* Unknown flavor — skip opaque body. */
        char skip[400];
        if (!xdr_opaque_decode(xdrs, skip, cred_len)) {
            return -1;
        }
    }

    /* Save position after credential for header MIC. */
    if (cred_flav == 6 && gss_cred != NULL) {
        gss_cred->cred_end_pos = xdr_getpos(xdrs);
    }

    /* Read verifier. For GSS, save body for header
     * verification; otherwise skip. */
    if (cred_flav == 6 && gss_cred != NULL) {
        uint32_t vf, vl;
        if (!xdr_uint32_t(xdrs, &vf)) {
            return -1;
        }
        if (!xdr_uint32_t(xdrs, &vl)) {
            return -1;
        }
        if (vl > sizeof(gss_cred->verf_body)) {
            return -1;
        }
        gss_cred->verf_flavor = vf;
        gss_cred->verf_body_len = vl;
        if (vl > 0) {
            if (!xdr_opaque_decode(xdrs,
                    (char *)gss_cred->verf_body,
                    vl)) {
                return -1;
            }
        }
    } else {
        if (!read_auth(xdrs, NULL)) {
            return -1;
        }
    }

    return 0;
}

int rpc_encode_accepted_reply(XDR *xdrs, uint32_t xid)
{
    uint32_t msg_type = 1;   /* REPLY */
    uint32_t reply_stat = 0; /* MSG_ACCEPTED */
    uint32_t verf_flavor = 0; /* AUTH_NONE */
    uint32_t verf_len = 0;
    uint32_t accept_stat = 0; /* SUCCESS */

    if (!xdr_uint32_t(xdrs, &xid)) {
        return -1;
}
    if (!xdr_uint32_t(xdrs, &msg_type)) {
        return -1;
}
    if (!xdr_uint32_t(xdrs, &reply_stat)) {
        return -1;
}
    /* verf: AUTH_NONE */
    if (!xdr_uint32_t(xdrs, &verf_flavor)) {
        return -1;
}
    if (!xdr_uint32_t(xdrs, &verf_len)) {
        return -1;
}
    /* accept_stat */
    return xdr_uint32_t(xdrs, &accept_stat) ? 0 : -1;
}

int rpc_encode_error_reply(XDR *xdrs, uint32_t xid, uint32_t accept_stat)
{
    uint32_t msg_type = 1;   /* REPLY */
    uint32_t reply_stat = 0; /* MSG_ACCEPTED */
    uint32_t verf_flavor = 0;
    uint32_t verf_len = 0;

    if (!xdr_uint32_t(xdrs, &xid)) {
        return -1;
}
    if (!xdr_uint32_t(xdrs, &msg_type)) {
        return -1;
}
    if (!xdr_uint32_t(xdrs, &reply_stat)) {
        return -1;
}
    if (!xdr_uint32_t(xdrs, &verf_flavor)) {
        return -1;
}
    if (!xdr_uint32_t(xdrs, &verf_len)) {
        return -1;
}
    return xdr_uint32_t(xdrs, &accept_stat) ? 0 : -1;
}

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * rondb_schema.c -- RonDB serialisation implementations.
 *
 * Pure C, no NDB dependency.  Reuses FDB endian helpers for
 * big-endian wire format.
 */

#include <string.h>
#include <stdio.h>
#include <stdatomic.h>
#include <execinfo.h>

#include "rondb_schema.h"

/* -----------------------------------------------------------------------
 * Inode serialisation (137 bytes)
 * ----------------------------------------------------------------------- */

int rondb_inode_serialize(const struct mds_inode *inode,
                          uint32_t home_shard_id,
                          uint8_t *buf, size_t cap)
{
    uint8_t *p;

    if (inode == NULL || buf == NULL ||
        cap < RONDB_INODE_FIXED_SIZE) {
        return -1;
    }

    p = buf;
    fdb_put_u64(p, inode->fileid);            p += 8;
    fdb_put_u8(p, (uint8_t)inode->type);      p += 1;
    fdb_put_u32(p, inode->mode);              p += 4;
    fdb_put_u32(p, inode->nlink);             p += 4;
    fdb_put_u64(p, inode->uid);               p += 8;
    fdb_put_u64(p, inode->gid);               p += 8;
    fdb_put_u64(p, inode->size);              p += 8;
    fdb_put_u64(p, inode->space_used);        p += 8;
    fdb_put_i64(p, (int64_t)inode->atime.tv_sec);  p += 8;
    fdb_put_u32(p, (uint32_t)inode->atime.tv_nsec); p += 4;
    fdb_put_i64(p, (int64_t)inode->mtime.tv_sec);  p += 8;
    fdb_put_u32(p, (uint32_t)inode->mtime.tv_nsec); p += 4;
    fdb_put_i64(p, (int64_t)inode->ctime.tv_sec);  p += 8;
    fdb_put_u32(p, (uint32_t)inode->ctime.tv_nsec); p += 4;
    fdb_put_u64(p, inode->change);            p += 8;
    fdb_put_u64(p, inode->generation);        p += 8;
    fdb_put_u32(p, inode->flags);             p += 4;
    fdb_put_u64(p, inode->create_verf);       p += 8;
    fdb_put_u64(p, inode->parent_fileid);     p += 8;
    fdb_put_u32(p, home_shard_id);

    return RONDB_INODE_FIXED_SIZE;
}

int rondb_inode_deserialize(const uint8_t *buf, size_t len,
                            struct mds_inode *inode,
                            uint32_t *home_shard_id)
{
    const uint8_t *p;

    if (buf == NULL || inode == NULL ||
        len < RONDB_INODE_FIXED_SIZE) {
        return -1;
    }

    memset(inode, 0, sizeof(*inode));
    p = buf;

    inode->fileid         = fdb_get_u64(p);                p += 8;
    inode->type           = (enum mds_file_type)fdb_get_u8(p); p += 1;
    inode->mode           = fdb_get_u32(p);                p += 4;
    inode->nlink          = fdb_get_u32(p);                p += 4;
    inode->uid            = fdb_get_u64(p);                p += 8;
    inode->gid            = fdb_get_u64(p);                p += 8;
    inode->size           = fdb_get_u64(p);                p += 8;
    inode->space_used     = fdb_get_u64(p);                p += 8;
    inode->atime.tv_sec   = (time_t)fdb_get_i64(p);       p += 8;
    inode->atime.tv_nsec  = (long)fdb_get_u32(p);         p += 4;
    inode->mtime.tv_sec   = (time_t)fdb_get_i64(p);       p += 8;
    inode->mtime.tv_nsec  = (long)fdb_get_u32(p);         p += 4;
    inode->ctime.tv_sec   = (time_t)fdb_get_i64(p);       p += 8;
    inode->ctime.tv_nsec  = (long)fdb_get_u32(p);         p += 4;
    inode->change         = fdb_get_u64(p);                p += 8;
    inode->generation     = fdb_get_u64(p);                p += 8;
    inode->flags          = fdb_get_u32(p);                p += 4;
    inode->create_verf    = fdb_get_u64(p);                p += 8;
    inode->parent_fileid  = fdb_get_u64(p);                p += 8;
    if (home_shard_id != NULL) {
        *home_shard_id    = fdb_get_u32(p);
    }

    return 0;
}

/* -----------------------------------------------------------------------
 * Stripe entry serialisation (per-row)
 * ----------------------------------------------------------------------- */

int rondb_stripe_entry_serialize(const struct mds_ds_map_entry *entry,
                                 uint8_t *buf, size_t cap)
{
    if (entry == NULL || buf == NULL ||
        cap < RONDB_STRIPE_ENTRY_SIZE) {
        return -1;
    }

    if (entry->ds_id >= 65536U) {
        /* Real ds_ids are small (0..N-DS); a huge value here is the
         * garbage that later wedges ds_gc. Catch it AT THE WRITE with a
         * backtrace to pinpoint the create/layout path. Rate-limited. */
        static _Atomic unsigned g_bad_dsid = 0;
        if (atomic_fetch_add_explicit(&g_bad_dsid, 1U,
                                      memory_order_relaxed) < 16U) {
            void *bt[32];
            int nb = backtrace(bt, 32);
            fprintf(stderr, "WARN: stripe write out-of-range ds_id=%u "
                    "fh_len=%u -- origin backtrace:\n",
                    entry->ds_id, entry->nfs_fh_len);
            backtrace_symbols_fd(bt, nb, 2);
        }
    }
    fdb_put_u32(buf, entry->ds_id);
    fdb_put_u32(buf + 4, entry->nfs_fh_len);
    memset(buf + 8, 0, MDS_NFS_FH_MAX);
    if (entry->nfs_fh_len > 0 && entry->nfs_fh_len <= MDS_NFS_FH_MAX) {
        memcpy(buf + 8, entry->nfs_fh, entry->nfs_fh_len);
    }

    return RONDB_STRIPE_ENTRY_SIZE;
}

int rondb_stripe_entry_deserialize(const uint8_t *buf, size_t len,
                                   struct mds_ds_map_entry *entry)
{
    if (buf == NULL || entry == NULL ||
        len < RONDB_STRIPE_ENTRY_SIZE) {
        return -1;
    }

    memset(entry, 0, sizeof(*entry));
    entry->ds_id = fdb_get_u32(buf);
    entry->nfs_fh_len = fdb_get_u32(buf + 4);
    if (entry->nfs_fh_len > MDS_NFS_FH_MAX) {
        entry->nfs_fh_len = MDS_NFS_FH_MAX;
    }
    memcpy(entry->nfs_fh, buf + 8, entry->nfs_fh_len);

    return 0;
}

/* -----------------------------------------------------------------------
 * Rename journal entry serialisation
 * ----------------------------------------------------------------------- */

int rondb_rj_serialize(const struct rondb_rename_journal_entry *entry,
                       uint8_t *buf, size_t cap)
{
    uint8_t *p;
    size_t src_len, dst_len, total;

    if (entry == NULL || buf == NULL) {
        return -1;
    }

    src_len = strlen(entry->src_name);
    dst_len = strlen(entry->dst_name);
    if (src_len > MDS_MAX_NAME || dst_len > MDS_MAX_NAME) {
        return -1;
    }

    total = RONDB_RJ_FIXED_SIZE + src_len + dst_len;
    if (cap < total) {
        return -1;
    }

    p = buf;
    fdb_put_u64(p, entry->txn_id);                p += 8;
    fdb_put_u8(p, entry->state);                   p += 1;
    fdb_put_u8(p, entry->role);                    p += 1;
    fdb_put_u32(p, entry->coordinator_mds_id);     p += 4;
    fdb_put_u64(p, entry->src_parent_fileid);      p += 8;
    fdb_put_u64(p, entry->dst_parent_fileid);      p += 8;
    fdb_put_u64(p, entry->src_child_fileid);       p += 8;
    p[0] = (uint8_t)((src_len >> 8) & 0xFFU);
    p[1] = (uint8_t)(src_len & 0xFFU);
    p += 2;
    p[0] = (uint8_t)((dst_len >> 8) & 0xFFU);
    p[1] = (uint8_t)(dst_len & 0xFFU);
    p += 2;
    memcpy(p, entry->src_name, src_len);           p += src_len;
    memcpy(p, entry->dst_name, dst_len);           p += dst_len;
    fdb_put_u64(p, entry->created_at_ns);

    return (int)total;
}

int rondb_rj_deserialize(const uint8_t *buf, size_t len,
                         struct rondb_rename_journal_entry *entry)
{
    const uint8_t *p;
    uint16_t src_len, dst_len;
    size_t total;

    if (buf == NULL || entry == NULL ||
        len < RONDB_RJ_FIXED_SIZE) {
        return -1;
    }

    memset(entry, 0, sizeof(*entry));
    p = buf;

    entry->txn_id              = fdb_get_u64(p);  p += 8;
    entry->state               = fdb_get_u8(p);   p += 1;
    entry->role                = fdb_get_u8(p);   p += 1;
    entry->coordinator_mds_id  = fdb_get_u32(p);  p += 4;
    entry->src_parent_fileid   = fdb_get_u64(p);  p += 8;
    entry->dst_parent_fileid   = fdb_get_u64(p);  p += 8;
    entry->src_child_fileid    = fdb_get_u64(p);  p += 8;

    /* Name lengths are stored as big-endian u16 values. */
    src_len = (uint16_t)((p[0] << 8) | p[1]);     p += 2;
    dst_len = (uint16_t)((p[0] << 8) | p[1]);     p += 2;

    if (src_len > MDS_MAX_NAME || dst_len > MDS_MAX_NAME) {
        return -1;
    }

    total = RONDB_RJ_FIXED_SIZE + src_len + dst_len;
    if (len < total) {
        return -1;
    }

    memcpy(entry->src_name, p, src_len);
    entry->src_name[src_len] = '\0';
    p += src_len;

    memcpy(entry->dst_name, p, dst_len);
    entry->dst_name[dst_len] = '\0';
    p += dst_len;

    entry->created_at_ns = fdb_get_u64(p);

    return 0;
}

/* -----------------------------------------------------------------------
 * Lock resource key builders
 * ----------------------------------------------------------------------- */

int rondb_lock_res_parent_name(uint64_t parent_fileid,
                               const char *name,
                               uint8_t *buf, size_t cap,
                               uint64_t *partition_hint)
{
    size_t name_len;

    if (name == NULL || buf == NULL || partition_hint == NULL) {
        return -1;
    }

    name_len = strlen(name);
    if (name_len > MDS_MAX_NAME || cap < 8 + name_len) {
        return -1;
    }

    fdb_put_u64(buf, parent_fileid);
    memcpy(buf + 8, name, name_len);
    *partition_hint = parent_fileid;

    return (int)(8 + name_len);
}

int rondb_lock_res_dir(uint64_t dir_fileid,
                       uint8_t *buf, size_t cap,
                       uint64_t *partition_hint)
{
    if (buf == NULL || partition_hint == NULL || cap < 8) {
        return -1;
    }

    fdb_put_u64(buf, dir_fileid);
    *partition_hint = dir_fileid;

    return 8;
}

int rondb_lock_res_inode(uint64_t fileid,
                         uint8_t *buf, size_t cap,
                         uint64_t *partition_hint)
{
    if (buf == NULL || partition_hint == NULL || cap < 8) {
        return -1;
    }

    fdb_put_u64(buf, fileid);
    *partition_hint = fileid;

    return 8;
}

int rondb_lock_res_topology(uint8_t *buf, size_t cap,
                            uint64_t *partition_hint)
{
    if (buf == NULL || partition_hint == NULL || cap < 8) {
        return -1;
    }

    fdb_put_u64(buf, RONDB_TOPOLOGY_SENTINEL);
    *partition_hint = RONDB_TOPOLOGY_SENTINEL;

    return 8;
}

/* -----------------------------------------------------------------------
 * Lock ordering comparator
 * ----------------------------------------------------------------------- */

int rondb_lock_order_cmp(const struct rondb_lock_entry *a,
                         const struct rondb_lock_entry *b)
{
    int min_len;

    if (a == NULL || b == NULL) {
        return 0;
    }

    /* 1. Compare resource_class. */
    if (a->resource_class != b->resource_class) {
        return (a->resource_class < b->resource_class) ? -1 : 1;
    }

    /* 2. Compare partition_hint. */
    if (a->partition_hint != b->partition_hint) {
        return (a->partition_hint < b->partition_hint) ? -1 : 1;
    }

    /* 3. Compare resource_key bytes. */
    min_len = (a->key_len < b->key_len) ? a->key_len : b->key_len;
    if (min_len > 0) {
        int cmp = memcmp(a->key, b->key, (size_t)min_len);
        if (cmp != 0) {
            return cmp;
        }
    }

    /* Shorter key sorts first. */
    if (a->key_len != b->key_len) {
        return (a->key_len < b->key_len) ? -1 : 1;
    }

    return 0;
}

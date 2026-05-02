/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * migration.c — Subtree migration state machine.
 *
 * Moves metadata ownership of a directory subtree from one MDS to
 * another.  See docs/architecture.md §13 for the full protocol.
 *
 * Protocol phases:
 *   1. Freeze   — set subtree to MIGRATING, drain mutations
 *   2. Stream   — DFS-iterate subtree, send each inode to dest
 *   3. Commit   — dest confirms, subtree_map ownership transferred
 *   4. Unfreeze — dest serves subtree, source creates junction
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <errno.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "migration.h"
#include "subtree_map.h"
#include "referral.h"

/* -----------------------------------------------------------------------
 * Serialised inode size
 * ----------------------------------------------------------------------- */

#define MIG_INODE_SERIAL_SIZE  109

/* -----------------------------------------------------------------------
 * Internal inode serialisation
 * ----------------------------------------------------------------------- */

static void mig_ser_u8(uint8_t *buf, size_t off, uint8_t v)
{
    buf[off] = v;
}

static void mig_ser_u16(uint8_t *buf, size_t off, uint16_t v)
{
    memcpy(buf + off, &v, sizeof(v));
}

static void mig_ser_u32(uint8_t *buf, size_t off, uint32_t v)
{
    memcpy(buf + off, &v, sizeof(v));
}

static void mig_ser_u64(uint8_t *buf, size_t off, uint64_t v)
{
    memcpy(buf + off, &v, sizeof(v));
}

static void mig_ser_i64(uint8_t *buf, size_t off, int64_t v)
{
    memcpy(buf + off, &v, sizeof(v));
}

static uint8_t mig_de_u8(const uint8_t *buf, size_t off)
{
    return buf[off];
}

static uint16_t mig_de_u16(const uint8_t *buf, size_t off)
{
    uint16_t v;
    memcpy(&v, buf + off, sizeof(v));
    return v;
}

static uint32_t mig_de_u32(const uint8_t *buf, size_t off)
{
    uint32_t v;
    memcpy(&v, buf + off, sizeof(v));
    return v;
}

static uint64_t mig_de_u64(const uint8_t *buf, size_t off)
{
    uint64_t v;
    memcpy(&v, buf + off, sizeof(v));
    return v;
}

static int64_t mig_de_i64(const uint8_t *buf, size_t off)
{
    int64_t v;
    memcpy(&v, buf + off, sizeof(v));
    return v;
}

/* Serialise an mds_inode into MIG_INODE_SERIAL_SIZE bytes. */
static void mig_inode_ser(const struct mds_inode *inode, uint8_t *buf)
{
    mig_ser_u8(buf,   0, (uint8_t)inode->type);
    mig_ser_u32(buf,  1, inode->mode);
    mig_ser_u32(buf,  5, inode->nlink);
    mig_ser_u64(buf,  9, inode->uid);
    mig_ser_u64(buf, 17, inode->gid);
    mig_ser_u64(buf, 25, inode->size);
    mig_ser_u64(buf, 33, inode->space_used);
    mig_ser_i64(buf, 41, (int64_t)inode->atime.tv_sec);
    mig_ser_i64(buf, 49, (int64_t)inode->atime.tv_nsec);
    mig_ser_i64(buf, 57, (int64_t)inode->mtime.tv_sec);
    mig_ser_i64(buf, 65, (int64_t)inode->mtime.tv_nsec);
    mig_ser_i64(buf, 73, (int64_t)inode->ctime.tv_sec);
    mig_ser_i64(buf, 81, (int64_t)inode->ctime.tv_nsec);
    mig_ser_u64(buf, 89, inode->change);
    mig_ser_u64(buf, 97, inode->generation);
    mig_ser_u32(buf, 105, inode->flags);
}

/* Deserialise MIG_INODE_SERIAL_SIZE bytes into an mds_inode. */
static void mig_inode_de(const uint8_t *buf, struct mds_inode *inode)
{
    inode->type       = (enum mds_file_type)mig_de_u8(buf, 0);
    inode->mode       = mig_de_u32(buf, 1);
    inode->nlink      = mig_de_u32(buf, 5);
    inode->uid        = mig_de_u64(buf, 9);
    inode->gid        = mig_de_u64(buf, 17);
    inode->size       = mig_de_u64(buf, 25);
    inode->space_used = mig_de_u64(buf, 33);
    inode->atime.tv_sec  = (time_t)mig_de_i64(buf, 41);
    inode->atime.tv_nsec = (long)mig_de_i64(buf, 49);
    inode->mtime.tv_sec  = (time_t)mig_de_i64(buf, 57);
    inode->mtime.tv_nsec = (long)mig_de_i64(buf, 65);
    inode->ctime.tv_sec  = (time_t)mig_de_i64(buf, 73);
    inode->ctime.tv_nsec = (long)mig_de_i64(buf, 81);
    inode->change     = mig_de_u64(buf, 89);
    inode->generation = mig_de_u64(buf, 97);
    inode->flags      = mig_de_u32(buf, 105);
}

/* -----------------------------------------------------------------------
 * Chunk serialisation
 *
 * Wire format:
 *   [fileid 8][inode_serial 109][dirent_count 4]
 *   For each dirent: [child_fileid 8][type 1][name_len 2][name ...]
 *   [has_stripe_map 1]
 *   If has_stripe_map: [stripe_count 4][stripe_unit 4][mirror_count 4]
 *     For each entry: [ds_id 4][fh_len 4][fh ...]
 *   [xattr_count 4]
 *   For each xattr: [name_len 2][name ...][val_len 4][val ...]
 * ----------------------------------------------------------------------- */

int mig_chunk_serialise(const struct mig_inode_chunk *chunk,
                        void **out, size_t *out_len)
{
    if (chunk == NULL || out == NULL || out_len == NULL) {
        return -EINVAL;
}

    /* Pass 1: compute total size. */
    size_t total = 8 + MIG_INODE_SERIAL_SIZE + 4;
    for (uint32_t i = 0; i < chunk->dirent_count; i++) {
        total += 8 + 1 + 2 + chunk->dirents[i].name_len;
}
    total += 1;  /* has_stripe_map */
    if (chunk->has_stripe_map) {
        total += 4 + 4 + 4;
        uint32_t n = chunk->stripe_map.stripe_count *
                     chunk->stripe_map.mirror_count;
        for (uint32_t i = 0; i < n; i++) {
            total += 4 + 4 + chunk->stripe_map.entries[i].nfs_fh_len;
}
    }
    total += 4;  /* xattr_count */
    for (uint32_t i = 0; i < chunk->xattr_count; i++) {
        total += 2 + chunk->xattrs[i].name_len + 4 + chunk->xattrs[i].val_len;
}
    total += 1;  /* has_inline_data */
    if (chunk->has_inline_data) {
        total += 4 + chunk->inline_data_len;
}

    /* Pass 2: serialise. */
    uint8_t *buf = malloc(total);
    if (buf == NULL) {
        return -ENOMEM;
}

    size_t off = 0;
    mig_ser_u64(buf, off, chunk->fileid);          off += 8;
    mig_inode_ser(&chunk->inode, buf + off);        off += MIG_INODE_SERIAL_SIZE;
    mig_ser_u32(buf, off, chunk->dirent_count);     off += 4;

    for (uint32_t i = 0; i < chunk->dirent_count; i++) {
        const struct mig_dirent *d = &chunk->dirents[i];
        mig_ser_u64(buf, off, d->child_fileid);    off += 8;
        mig_ser_u8(buf, off, d->type);             off += 1;
        mig_ser_u16(buf, off, d->name_len);        off += 2;
        memcpy(buf + off, d->name, d->name_len);   off += d->name_len;
    }

    mig_ser_u8(buf, off, (uint8_t)chunk->has_stripe_map); off += 1;
    if (chunk->has_stripe_map) {
        const struct mig_stripe_map *sm = &chunk->stripe_map;
        mig_ser_u32(buf, off, sm->stripe_count);   off += 4;
        mig_ser_u32(buf, off, sm->stripe_unit);     off += 4;
        mig_ser_u32(buf, off, sm->mirror_count);    off += 4;
        uint32_t n = sm->stripe_count * sm->mirror_count;
        for (uint32_t i = 0; i < n; i++) {
            mig_ser_u32(buf, off, sm->entries[i].ds_id);       off += 4;
            mig_ser_u32(buf, off, sm->entries[i].nfs_fh_len);  off += 4;
            if (sm->entries[i].nfs_fh_len > 0) {
                memcpy(buf + off, sm->entries[i].nfs_fh,
                       sm->entries[i].nfs_fh_len);
                off += sm->entries[i].nfs_fh_len;
            }
        }
    }

    mig_ser_u32(buf, off, chunk->xattr_count);      off += 4;
    for (uint32_t i = 0; i < chunk->xattr_count; i++) {
        const struct mig_xattr *x = &chunk->xattrs[i];
        mig_ser_u16(buf, off, x->name_len);        off += 2;
        memcpy(buf + off, x->name, x->name_len);   off += x->name_len;
        mig_ser_u32(buf, off, x->val_len);          off += 4;
        if (x->val_len > 0) {
            memcpy(buf + off, x->value, x->val_len);
            off += x->val_len;
        }
    }

    mig_ser_u8(buf, off, (uint8_t)chunk->has_inline_data); off += 1;
    if (chunk->has_inline_data) {
        mig_ser_u32(buf, off, chunk->inline_data_len); off += 4;
        if (chunk->inline_data_len > 0) {
            memcpy(buf + off, chunk->inline_data, chunk->inline_data_len);
            off += chunk->inline_data_len;
        }
    }
    (void)off;

    *out = buf;
    *out_len = total;
    return 0;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int mig_chunk_deserialise(const void *data, size_t len,
                          struct mig_inode_chunk *chunk)
{
    if (data == NULL || chunk == NULL) {
        return -EINVAL;
}

    const uint8_t *p = data;
    size_t off = 0;

    memset(chunk, 0, sizeof(*chunk));

    /* Minimum: fileid(8) + inode(105) + dirent_count(4) +
     * has_stripe_map(1) + xattr_count(4) = 122 */
    if (len < 8 + MIG_INODE_SERIAL_SIZE + 4 + 1 + 4) {
        return -EINVAL;
}

    chunk->fileid = mig_de_u64(p, off);                off += 8;
    mig_inode_de(p + off, &chunk->inode);               off += MIG_INODE_SERIAL_SIZE;
    chunk->inode.fileid = chunk->fileid;
    chunk->dirent_count = mig_de_u32(p, off);           off += 4;

    /* Dirents. */
    if (chunk->dirent_count > 0) {
        chunk->dirents = calloc(chunk->dirent_count, sizeof(*chunk->dirents));
        if (chunk->dirents == NULL) {
            return -ENOMEM;
}
        for (uint32_t i = 0; i < chunk->dirent_count; i++) {
            if (off + 11 > len) { goto fail;
}
            chunk->dirents[i].child_fileid = mig_de_u64(p, off); off += 8;
            chunk->dirents[i].type = mig_de_u8(p, off);          off += 1;
            chunk->dirents[i].name_len = mig_de_u16(p, off);     off += 2;
            if (chunk->dirents[i].name_len > MDS_MAX_NAME ||
                off + chunk->dirents[i].name_len > len) {
                goto fail;
}
            memcpy(chunk->dirents[i].name, p + off,
                   chunk->dirents[i].name_len);
            chunk->dirents[i].name[chunk->dirents[i].name_len] = '\0';
            off += chunk->dirents[i].name_len;
        }
    }

    /* Stripe map. */
    if (off + 1 > len) { goto fail;
}
    chunk->has_stripe_map = mig_de_u8(p, off); off += 1;
    if (chunk->has_stripe_map) {
        if (off + 12 > len) { goto fail;
}
        chunk->stripe_map.stripe_count = mig_de_u32(p, off); off += 4;
        chunk->stripe_map.stripe_unit  = mig_de_u32(p, off); off += 4;
        chunk->stripe_map.mirror_count = mig_de_u32(p, off); off += 4;
        uint32_t n = chunk->stripe_map.stripe_count *
                     chunk->stripe_map.mirror_count;
        if (n > 0) {
            chunk->stripe_map.entries = calloc(n, sizeof(*chunk->stripe_map.entries));
            if (chunk->stripe_map.entries == NULL) { goto fail;
}
            for (uint32_t i = 0; i < n; i++) {
                if (off + 8 > len) { goto fail;
}
                chunk->stripe_map.entries[i].ds_id = mig_de_u32(p, off);      off += 4;
                chunk->stripe_map.entries[i].nfs_fh_len = mig_de_u32(p, off); off += 4;
                uint32_t fhlen = chunk->stripe_map.entries[i].nfs_fh_len;
                if (fhlen > MDS_NFS_FH_MAX || off + fhlen > len) {
                    goto fail;
}
                if (fhlen > 0) {
                    memcpy(chunk->stripe_map.entries[i].nfs_fh,
                           p + off, fhlen);
                    off += fhlen;
                }
            }
        }
    }

    /* Xattrs. */
    if (off + 4 > len) { goto fail;
}
    chunk->xattr_count = mig_de_u32(p, off); off += 4;
    if (chunk->xattr_count > 0) {
        chunk->xattrs = calloc(chunk->xattr_count, sizeof(*chunk->xattrs));
        if (chunk->xattrs == NULL) { goto fail;
}
        for (uint32_t i = 0; i < chunk->xattr_count; i++) {
            if (off + 2 > len) { goto fail;
}
            chunk->xattrs[i].name_len = mig_de_u16(p, off); off += 2;
            if (chunk->xattrs[i].name_len > 255 ||
                off + chunk->xattrs[i].name_len > len) {
                goto fail;
}
            memcpy(chunk->xattrs[i].name, p + off,
                   chunk->xattrs[i].name_len);
            chunk->xattrs[i].name[chunk->xattrs[i].name_len] = '\0';
            off += chunk->xattrs[i].name_len;
            if (off + 4 > len) { goto fail;
}
            chunk->xattrs[i].val_len = mig_de_u32(p, off); off += 4;
            if (off + chunk->xattrs[i].val_len > len) {
                goto fail;
}
            if (chunk->xattrs[i].val_len > 0) {
                chunk->xattrs[i].value = malloc(chunk->xattrs[i].val_len);
                if (chunk->xattrs[i].value == NULL) { goto fail;
}
                memcpy(chunk->xattrs[i].value, p + off,
                       chunk->xattrs[i].val_len);
                off += chunk->xattrs[i].val_len;
            }
        }
    }

    /* Inline data. */
    chunk->has_inline_data = 0;
    if (off + 1 <= len) {
        chunk->has_inline_data = mig_de_u8(p, off); off += 1;
        if (chunk->has_inline_data) {
            if (off + 4 > len) { goto fail;
}
            chunk->inline_data_len = mig_de_u32(p, off); off += 4;
            if (chunk->inline_data_len > MDS_INLINE_DATA_MAX ||
                off + chunk->inline_data_len > len) {
                goto fail;
}
            if (chunk->inline_data_len > 0) {
                chunk->inline_data = malloc(chunk->inline_data_len);
                if (chunk->inline_data == NULL) { goto fail;
}
                memcpy(chunk->inline_data, p + off,
                       chunk->inline_data_len);
                off += chunk->inline_data_len;
            }
        }
    }
    (void)off;

    return 0;

fail:
    mig_chunk_free(chunk);
    memset(chunk, 0, sizeof(*chunk));
    return -EINVAL;
}

void mig_chunk_free(struct mig_inode_chunk *chunk)
{
    if (chunk == NULL) {
        return;
}
    free(chunk->dirents);
    chunk->dirents = NULL;
    free(chunk->stripe_map.entries);
    chunk->stripe_map.entries = NULL;
    free(chunk->inline_data);
    chunk->inline_data = NULL;
    for (uint32_t i = 0; i < chunk->xattr_count; i++) {
        free(chunk->xattrs[i].value);
}
    free(chunk->xattrs);
    chunk->xattrs = NULL;
}

/* -----------------------------------------------------------------------
 * Source-side: streaming callback
 * ----------------------------------------------------------------------- */

struct stream_ctx {
    const struct migration_transport *transport;
    struct migration_tracker *tracker;
    uint32_t dest_mds_id;
    uint32_t inodes_sent;
    int      error;
};

static int stream_inode_cb(const struct mig_inode_chunk *chunk, void *arg)
{
    struct stream_ctx *sc = arg;
    void *buf = NULL;
    size_t buf_len = 0;
    int rc;

    rc = mig_chunk_serialise(chunk, &buf, &buf_len);
    if (rc != 0) {
        sc->error = rc;
        return -1;
    }

    rc = sc->transport->send_inode(sc->dest_mds_id, buf, buf_len,
                                   sc->transport->user_ctx);
    free(buf);

    if (rc != 0) {
        sc->error = -EIO;
        return -1;
    }

    sc->inodes_sent++;
    if (sc->tracker != NULL) {
        migration_tracker_update(sc->tracker, 1);
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * Source-side: frozen fileid collection callback
 * ----------------------------------------------------------------------- */

struct freeze_collect_ctx {
    uint64_t *fids;
    uint32_t  count;
    uint32_t  cap;
};

static int freeze_collect_cb(const struct mig_inode_chunk *chunk, void *arg)
{
    struct freeze_collect_ctx *fc = arg;

    if (fc->count >= fc->cap) {
        uint32_t new_cap = fc->cap == 0 ? 64 : fc->cap * 2;
        uint64_t *tmp = realloc(fc->fids,
                                (size_t)new_cap * sizeof(*tmp));
        if (tmp == NULL) {
            return -1;
}
        fc->fids = tmp;
        fc->cap = new_cap;
    }
    fc->fids[fc->count++] = chunk->fileid;
    return 0;
}

/* -----------------------------------------------------------------------
 * Source-side: migration_initiate()
 * ----------------------------------------------------------------------- */

enum mds_status migration_initiate(
    struct mds_catalogue *cat,
    struct subtree_map *map,
    const struct migration_transport *transport,
    const char *subtree_path,
    uint64_t subtree_root_fileid,
    uint32_t dest_mds_id,
    struct migration_tracker *tracker)
{
    struct subtree_entry entry;
    struct stream_ctx sc;
    struct freeze_collect_ctx fc;
    enum mds_status st;
    int rc;

    if (cat == NULL || map == NULL || transport == NULL ||
        subtree_path == NULL) {
        return MDS_ERR_INVAL;
    }

    memset(&fc, 0, sizeof(fc));

    /* 1. Verify subtree is ACTIVE and owned by us. */
    st = subtree_map_lookup(map, subtree_path, &entry);
    if (st != MDS_OK) {
        return st;
}
    if (entry.owner_mds_id != subtree_map_self_id(map)) {
        return MDS_ERR_PERM;
}
    if (entry.state != SUBTREE_ACTIVE) {
        return MDS_ERR_INVAL;
}

    /* 2. Freeze: set subtree to MIGRATING. */
    st = subtree_map_set_state(map, subtree_path,
                               SUBTREE_MIGRATING, entry.version);
    if (st != MDS_OK) {
        return st;
}

    /* 2b. DFS pre-pass: collect all fileids and register as frozen
     *     so that raw-PUTFH mutations are precisely blocked. */
    st = mds_cat_subtree_iter(cat, subtree_root_fileid,
                              freeze_collect_cb, &fc);
    if (st != MDS_OK || fc.count == 0) {
        free(fc.fids);
        subtree_map_lookup(map, subtree_path, &entry);
        subtree_map_set_state(map, subtree_path,
                              SUBTREE_ACTIVE, entry.version);
        if (tracker != NULL) {
            migration_tracker_finish(tracker, MIG_FAILED);
        }
        return (st != MDS_OK) ? st : MDS_ERR_IO;
    }
    st = subtree_map_freeze_fileids(map, fc.fids, fc.count);
    if (st != MDS_OK) {
        free(fc.fids);
        subtree_map_lookup(map, subtree_path, &entry);
        subtree_map_set_state(map, subtree_path,
                              SUBTREE_ACTIVE, entry.version);
        return st;
    }

    /* 3. Send BeginMigration to dest. */
    rc = transport->send_begin(dest_mds_id, subtree_path, 0,
                               transport->user_ctx);
    if (rc != 0) {
        subtree_map_unfreeze_fileids(map, fc.fids, fc.count);
        free(fc.fids);
        subtree_map_lookup(map, subtree_path, &entry);
        subtree_map_set_state(map, subtree_path,
                              SUBTREE_ACTIVE, entry.version);
        return MDS_ERR_IO;
    }

    /* 4. Stream all inodes via DFS iteration. */
    memset(&sc, 0, sizeof(sc));
    sc.transport = transport;
    sc.dest_mds_id = dest_mds_id;
    sc.tracker = tracker;

    st = mds_cat_subtree_iter(cat, subtree_root_fileid,
                              stream_inode_cb, &sc);
    if (st != MDS_OK || sc.error != 0) {
        subtree_map_unfreeze_fileids(map, fc.fids, fc.count);
        free(fc.fids);
        subtree_map_lookup(map, subtree_path, &entry);
        subtree_map_set_state(map, subtree_path,
                              SUBTREE_ACTIVE, entry.version);
        return (st != MDS_OK) ? st : MDS_ERR_IO;
    }

    /* 5. Send FinishMigration. */
    rc = transport->send_finish(dest_mds_id, sc.inodes_sent,
                                transport->user_ctx);
    if (rc != 0) {
        subtree_map_unfreeze_fileids(map, fc.fids, fc.count);
        free(fc.fids);
        subtree_map_lookup(map, subtree_path, &entry);
        subtree_map_set_state(map, subtree_path,
                              SUBTREE_ACTIVE, entry.version);
        return MDS_ERR_IO;
    }

    /* 6. Commit: transfer ownership in subtree map. */
    st = subtree_map_lookup(map, subtree_path, &entry);
    if (st != MDS_OK) {
        subtree_map_unfreeze_fileids(map, fc.fids, fc.count);
        free(fc.fids);
        return st;
    }
    st = subtree_map_transfer_owner_if_migrating(map, subtree_path,
                               dest_mds_id, entry.version);
    if (st != MDS_OK) {
        subtree_map_unfreeze_fileids(map, fc.fids, fc.count);
        free(fc.fids);
        return st;
    }

    /* Re-read to get updated version, then set ACTIVE. */
    st = subtree_map_lookup(map, subtree_path, &entry);
    if (st != MDS_OK) {
        subtree_map_unfreeze_fileids(map, fc.fids, fc.count);
        free(fc.fids);
        return st;
    }
    st = subtree_map_set_state(map, subtree_path,
                               SUBTREE_ACTIVE, entry.version);
    if (st != MDS_OK) {
        subtree_map_unfreeze_fileids(map, fc.fids, fc.count);
        free(fc.fids);
        return st;
    }

    /* 7. Convert local subtree root to junction (sticky bit marker). */
    {
        struct mds_inode attrs;
        memset(&attrs, 0, sizeof(attrs));
        attrs.mode = 0755 | 01000;
        (void)mds_cat_ns_setattr(cat, NULL, subtree_root_fileid,
                                &attrs, MDS_ATTR_MODE);
    }

    /* 8. Clear frozen fileid set now that migration is complete. */
    subtree_map_unfreeze_fileids(map, fc.fids, fc.count);
    free(fc.fids);

    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Dest-side: migration_apply_chunk()
 * ----------------------------------------------------------------------- */

enum mds_status migration_apply_chunk(struct mds_catalogue *cat,
                                      const struct mig_inode_chunk *chunk)
{
    struct mds_cat_txn *txn = NULL;
    enum mds_status st;

    if (cat == NULL || chunk == NULL) {
        return MDS_ERR_INVAL;
    }

    /* Begin a catalogue transaction for atomicity.
     * On RonDB, each write is self-contained (txn param ignored). */
    st = mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn);
    if (st != MDS_OK) {
        return st;
    }

    /* 1. Write inode. */
    st = mds_cat_inode_put(cat, txn, &chunk->inode);
    if (st != MDS_OK) {
        goto out_abort;
    }

    /* 2. Write dirents. */
    for (uint32_t i = 0; i < chunk->dirent_count; i++) {
        const struct mig_dirent *d = &chunk->dirents[i];
        st = mds_cat_dirent_put(cat, txn,
                                chunk->fileid, d->name,
                                d->child_fileid, d->type);
        if (st != MDS_OK) {
            goto out_abort;
        }
    }

    /* 3. Write stripe map. */
    if (chunk->has_stripe_map && chunk->stripe_map.stripe_count > 0) {
        st = mds_cat_stripe_map_put(cat, txn,
                                    chunk->fileid,
                                    chunk->stripe_map.stripe_count,
                                    chunk->stripe_map.stripe_unit,
                                    chunk->stripe_map.mirror_count,
                                    chunk->stripe_map.entries);
        if (st != MDS_OK) {
            goto out_abort;
        }
    }

    /* 4. Write xattrs. */
    for (uint32_t i = 0; i < chunk->xattr_count; i++) {
        const struct mig_xattr *x = &chunk->xattrs[i];
        st = mds_cat_xattr_put(cat, txn,
                               chunk->fileid, x->name,
                               x->value, x->val_len);
        if (st != MDS_OK) {
            goto out_abort;
        }
    }

    /* 5. Write inline data. */
    if (chunk->has_inline_data && chunk->inline_data_len > 0) {
        st = mds_cat_inline_put(cat, txn,
                                chunk->fileid,
                                chunk->inline_data,
                                chunk->inline_data_len);
        if (st != MDS_OK) {
            goto out_abort;
        }
    }

    return mds_cat_txn_commit(txn);

out_abort:
    mds_cat_txn_abort(txn);
    return st;
}

/* -----------------------------------------------------------------------
 * Source-side: migration_cleanup()
 *
 * Post-migration cleanup: delete all inodes, dirents, stripe maps, and
 * xattrs under the migrated subtree from the source catalogue.
 *
 * Uses a two-pass approach:
 *   Pass 1: DFS collect all fileids in the subtree.
 *   Pass 2: Delete each fileid's data in a write transaction.
 * ----------------------------------------------------------------------- */

struct cleanup_collect_ctx {
    uint64_t *fileids;
    uint32_t  count;
    uint32_t  capacity;
};

static int cleanup_collect_cb(const struct mig_inode_chunk *chunk, void *arg)
{
    struct cleanup_collect_ctx *ctx = arg;

    if (ctx->count >= ctx->capacity) {
        uint32_t new_cap = ctx->capacity == 0 ? 64 : ctx->capacity * 2;
        uint64_t *tmp = realloc(ctx->fileids,
                                (size_t)new_cap * sizeof(*tmp));
        if (tmp == NULL) {
            return -1;
}
        ctx->fileids = tmp;
        ctx->capacity = new_cap;
    }
    ctx->fileids[ctx->count++] = chunk->fileid;
    return 0;
}

/* Xattr name collector for deletion. */
struct cleanup_xattr_ctx {
    char   (*names)[256];
    uint32_t count;
    uint32_t capacity;
};

static int cleanup_collect_xattr(const char *name, size_t name_len,
                                 void *arg)
{
    struct cleanup_xattr_ctx *ctx = arg;

    if (ctx->count >= ctx->capacity) {
        uint32_t new_cap = ctx->capacity == 0 ? 8 : ctx->capacity * 2;
        void *tmp = realloc(ctx->names, (size_t)new_cap * 256);
        if (tmp == NULL) {
            return -1;
}
        ctx->names = tmp;
        ctx->capacity = new_cap;
    }
    size_t clen = name_len < 255 ? name_len : 255;
    memcpy(ctx->names[ctx->count], name, clen);
    ctx->names[ctx->count][clen] = '\0';
    ctx->count++;
    return 0;
}

/* Dirent name collector for deletion. */
struct cleanup_dirent_ctx {
    char   (*names)[MDS_MAX_NAME + 1];
    uint32_t count;
    uint32_t capacity;
};

/** Catalogue readdir callback variant for cleanup_dirent_ctx. */
static int cleanup_collect_dirent_cat(const struct mds_cat_dirent *entry,
                                      void *arg)
{
    struct cleanup_dirent_ctx *ctx = arg;

    if (ctx->count >= ctx->capacity) {
        uint32_t new_cap = ctx->capacity == 0 ? 16 : ctx->capacity * 2;
        void *tmp = realloc(ctx->names,
                            (size_t)new_cap * (MDS_MAX_NAME + 1));
        if (tmp == NULL) {
            return -1;
        }
        ctx->names = tmp;
        ctx->capacity = new_cap;
    }
    (void)snprintf(ctx->names[ctx->count], MDS_MAX_NAME + 1,
                   "%s", entry->name);
    ctx->count++;
    return 0;
}

static int cleanup_collect_dirent(uint64_t child_fileid, uint8_t type,
                                  const char *name, size_t name_len,
                                  void *arg)
{
    struct cleanup_dirent_ctx *ctx = arg;
    (void)child_fileid;
    (void)type;

    if (ctx->count >= ctx->capacity) {
        uint32_t new_cap = ctx->capacity == 0 ? 16 : ctx->capacity * 2;
        void *tmp = realloc(ctx->names,
                            (size_t)new_cap * (MDS_MAX_NAME + 1));
        if (tmp == NULL) {
            return -1;
}
        ctx->names = tmp;
        ctx->capacity = new_cap;
    }
    size_t clen = name_len < MDS_MAX_NAME ? name_len : MDS_MAX_NAME;
    memcpy(ctx->names[ctx->count], name, clen);
    ctx->names[ctx->count][clen] = '\0';
    ctx->count++;
    return 0;
}

enum mds_status migration_cleanup(struct mds_catalogue *cat,
                                  uint64_t root_fileid)
{
    struct cleanup_collect_ctx cc;
    enum mds_status st;

    if (cat == NULL) {
        return MDS_ERR_INVAL;
    }

    /* Pass 1: collect all fileids via catalogue DFS. */
    memset(&cc, 0, sizeof(cc));
    st = mds_cat_subtree_iter(cat, root_fileid, cleanup_collect_cb, &cc);
    if (st != MDS_OK) {
        free(cc.fileids);
        return st;
    }

    /* Pass 2: delete each fileid's data via catalogue ops.
     * Process in reverse order (leaves first).
     * The subtree root is PRESERVED as a junction (sticky-bit referral
     * marker).  We delete its child dirents but keep the inode itself
     * so the parent's dirent remains valid and triggers NFS4ERR_MOVED.
     */
    for (int32_t idx = (int32_t)cc.count - 1; idx >= 0; idx--) {
        uint64_t fid = cc.fileids[idx];
        bool is_root = (fid == root_fileid);

        /* Delete dirents owned by this inode. */
        {
            struct cleanup_dirent_ctx dc;
            memset(&dc, 0, sizeof(dc));
            /* Collect dirent names via catalogue readdir. */
            (void)mds_cat_ns_readdir(cat, fid, NULL, NULL,
                                     cleanup_collect_dirent_cat, &dc);
            for (uint32_t i = 0; i < dc.count; i++) {
                (void)mds_cat_dirent_del(cat, NULL, fid, dc.names[i]);
            }
            free(dc.names);
        }

        /* Delete xattrs. */
        {
            struct cleanup_xattr_ctx xc;
            memset(&xc, 0, sizeof(xc));
            (void)mds_cat_xattr_list(cat, fid,
                                     cleanup_collect_xattr, &xc);
            for (uint32_t i = 0; i < xc.count; i++) {
                (void)mds_cat_xattr_del(cat, NULL, fid, xc.names[i]);
            }
            free(xc.names);
        }

        /* Delete stripe map. */
        (void)mds_cat_stripe_map_del(cat, NULL, fid);

        /* Delete inline data (if any). */
        (void)mds_cat_inline_del(cat, NULL, fid);

        /* Delete inode — but KEEP the subtree root (junction marker). */
        if (!is_root) {
            (void)mds_cat_inode_del(cat, NULL, fid);
        }
    }

    free(cc.fileids);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Migration progress tracker (Seq 10)
 * ----------------------------------------------------------------------- */

#include <stdatomic.h>

struct migration_tracker {
    _Atomic int       state;           /* enum migration_state */
    char              path[MDS_MAX_PATH];
    _Atomic uint32_t  total_inodes;
    _Atomic uint32_t  done_inodes;
    pthread_mutex_t   begin_lock;      /* serialise begin/finish */
};

enum mds_status migration_tracker_create(struct migration_tracker **out)
{
    if (out == NULL) { return MDS_ERR_INVAL; }

    struct migration_tracker *t = calloc(1, sizeof(*t));
    if (t == NULL) { return MDS_ERR_NOMEM; }

    atomic_store(&t->state, (int)MIG_IDLE);
    t->path[0] = '\0';
    atomic_store(&t->total_inodes, 0);
    atomic_store(&t->done_inodes, 0);
    pthread_mutex_init(&t->begin_lock, NULL);

    *out = t;
    return MDS_OK;
}

void migration_tracker_destroy(struct migration_tracker *t)
{
    if (t == NULL) { return; }
    pthread_mutex_destroy(&t->begin_lock);
    free(t);
}

enum mds_status migration_tracker_begin(struct migration_tracker *t,
                                        const char *path,
                                        uint32_t total)
{
    if (t == NULL || path == NULL) { return MDS_ERR_INVAL; }

    pthread_mutex_lock(&t->begin_lock);
    int cur = atomic_load(&t->state);
    if (cur != (int)MIG_IDLE && cur != (int)MIG_DONE &&
        cur != (int)MIG_FAILED) {
        pthread_mutex_unlock(&t->begin_lock);
        return MDS_ERR_DELAY;
    }

    (void)snprintf(t->path, sizeof(t->path), "%s", path);
    atomic_store(&t->total_inodes, total);
    atomic_store(&t->done_inodes, 0);
    atomic_store(&t->state, (int)MIG_FREEZING);
    pthread_mutex_unlock(&t->begin_lock);
    return MDS_OK;
}

void migration_tracker_update(struct migration_tracker *t,
                              uint32_t done_delta)
{
    if (t == NULL) { return; }
    atomic_fetch_add(&t->done_inodes, done_delta);
}

void migration_tracker_finish(struct migration_tracker *t,
                              enum migration_state final_state)
{
    if (t == NULL) { return; }
    pthread_mutex_lock(&t->begin_lock);
    atomic_store(&t->state, (int)final_state);
    if (final_state == MIG_IDLE) {
        t->path[0] = '\0';
        atomic_store(&t->total_inodes, 0);
        atomic_store(&t->done_inodes, 0);
    }
    pthread_mutex_unlock(&t->begin_lock);
}

void migration_tracker_get_progress(const struct migration_tracker *t,
                                    enum migration_state *state,
                                    char *path_out, size_t path_cap,
                                    uint32_t *total, uint32_t *done)
{
    if (t == NULL) {
        if (state != NULL) { *state = MIG_IDLE; }
        if (path_out != NULL && path_cap > 0) { path_out[0] = '\0'; }
        if (total != NULL) { *total = 0; }
        if (done != NULL) { *done = 0; }
        return;
    }

    if (state != NULL) {
        *state = (enum migration_state)atomic_load(&t->state);
    }
    if (path_out != NULL && path_cap > 0) {
        (void)snprintf(path_out, path_cap, "%s", t->path);
    }
    if (total != NULL) {
        *total = atomic_load(&t->total_inodes);
    }
    if (done != NULL) {
        *done = atomic_load(&t->done_inodes);
    }
}

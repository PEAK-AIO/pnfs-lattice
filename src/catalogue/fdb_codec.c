/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * fdb_codec.c -- Value formats of the FoundationDB catalogue backend.
 *
 * See fdb_codec.h for the layouts.  Encoders write through a bounded
 * cursor that refuses to run past the caller's buffer; decoders read
 * through a cursor that refuses to run past the value and then require
 * the value to be consumed exactly, so a truncated or padded value is
 * rejected rather than partially trusted.
 */

#include <string.h>

#include "fdb_codec.h"

#define NS_PER_SEC 1000000000LL

/* -----------------------------------------------------------------------
 * Little-endian integers
 * ----------------------------------------------------------------------- */

void fdb_le64_put(uint8_t *dst, uint64_t v)
{
    for (unsigned i = 0; i < 8; i++) {
        dst[i] = (uint8_t)(v >> (8U * i));
    }
}

uint64_t fdb_le64_get(const uint8_t *src)
{
    uint64_t v = 0;

    for (unsigned i = 0; i < 8; i++) {
        v |= (uint64_t)src[i] << (8U * i);
    }
    return v;
}

void fdb_le32_put(uint8_t *dst, uint32_t v)
{
    for (unsigned i = 0; i < 4; i++) {
        dst[i] = (uint8_t)(v >> (8U * i));
    }
}

uint32_t fdb_le32_get(const uint8_t *src)
{
    uint32_t v = 0;

    for (unsigned i = 0; i < 4; i++) {
        v |= (uint32_t)src[i] << (8U * i);
    }
    return v;
}

bool fdb_le64_decode(const uint8_t *src, size_t len, uint64_t *out)
{
    if (src == NULL || out == NULL || len != 8) {
        return false;
    }
    *out = fdb_le64_get(src);
    return true;
}

int64_t fdb_ts_to_ns(struct timespec ts)
{
    /* Saturate rather than overflow for absurd tv_sec values (~292
     * years either side of the epoch fit). */
    if (ts.tv_sec > INT64_MAX / NS_PER_SEC - 1) {
        return INT64_MAX;
    }
    if (ts.tv_sec < INT64_MIN / NS_PER_SEC + 1) {
        return INT64_MIN;
    }
    return (int64_t)ts.tv_sec * NS_PER_SEC + (int64_t)ts.tv_nsec;
}

struct timespec fdb_ns_to_ts(int64_t ns)
{
    struct timespec ts;
    int64_t sec = ns / NS_PER_SEC;
    int64_t rem = ns % NS_PER_SEC;

    if (rem < 0) {
        rem += NS_PER_SEC;
        sec -= 1;
    }
    ts.tv_sec = (time_t)sec;
    ts.tv_nsec = (long)rem;
    return ts;
}

/* -----------------------------------------------------------------------
 * Bounded cursors
 * ----------------------------------------------------------------------- */

struct wcur {
    uint8_t *buf;
    size_t   cap;
    size_t   pos;
    bool     fail;
};

struct rcur {
    const uint8_t *buf;
    size_t         len;
    size_t         pos;
    bool           fail;
};

static void w_bytes(struct wcur *w, const void *p, size_t n)
{
    if (w->fail || n > w->cap - w->pos) {
        w->fail = true;
        return;
    }
    if (n > 0) {
        memcpy(w->buf + w->pos, p, n);
    }
    w->pos += n;
}

static void w_u8(struct wcur *w, uint8_t v)
{
    w_bytes(w, &v, 1);
}

static void w_u16(struct wcur *w, uint16_t v)
{
    uint8_t b[2] = { (uint8_t)v, (uint8_t)(v >> 8) };

    w_bytes(w, b, sizeof(b));
}

static void w_u32(struct wcur *w, uint32_t v)
{
    uint8_t b[4];

    fdb_le32_put(b, v);
    w_bytes(w, b, sizeof(b));
}

static void w_u64(struct wcur *w, uint64_t v)
{
    uint8_t b[8];

    fdb_le64_put(b, v);
    w_bytes(w, b, sizeof(b));
}

static void w_ts(struct wcur *w, struct timespec ts)
{
    w_u64(w, (uint64_t)(int64_t)ts.tv_sec);
    w_u32(w, (uint32_t)ts.tv_nsec);
}

static bool r_bytes(struct rcur *r, void *p, size_t n)
{
    if (r->fail || n > r->len - r->pos) {
        r->fail = true;
        return false;
    }
    if (n > 0) {
        memcpy(p, r->buf + r->pos, n);
    }
    r->pos += n;
    return true;
}

static uint8_t r_u8(struct rcur *r)
{
    uint8_t v = 0;

    (void)r_bytes(r, &v, 1);
    return v;
}

static uint16_t r_u16(struct rcur *r)
{
    uint8_t b[2] = { 0, 0 };

    (void)r_bytes(r, b, sizeof(b));
    return (uint16_t)(b[0] | ((uint16_t)b[1] << 8));
}

static uint32_t r_u32(struct rcur *r)
{
    uint8_t b[4] = { 0, 0, 0, 0 };

    (void)r_bytes(r, b, sizeof(b));
    return fdb_le32_get(b);
}

static uint64_t r_u64(struct rcur *r)
{
    uint8_t b[8] = { 0, 0, 0, 0, 0, 0, 0, 0 };

    (void)r_bytes(r, b, sizeof(b));
    return fdb_le64_get(b);
}

/* Reads a timestamp; a tv_nsec outside [0, 1e9) marks the cursor failed. */
static struct timespec r_ts(struct rcur *r)
{
    struct timespec ts;
    uint32_t nsec;

    ts.tv_sec = (time_t)(int64_t)r_u64(r);
    nsec = r_u32(r);
    if (nsec >= (uint32_t)NS_PER_SEC) {
        r->fail = true;
        nsec = 0;
    }
    ts.tv_nsec = (long)nsec;
    return ts;
}

/* A decoder is complete only when it consumed the value exactly. */
static bool r_done(const struct rcur *r)
{
    return !r->fail && r->pos == r->len;
}

static bool type_ok(uint32_t t)
{
    return t >= (uint32_t)MDS_FTYPE_REG && t <= (uint32_t)MDS_FTYPE_SOCK;
}

/* -----------------------------------------------------------------------
 * Inode
 * ----------------------------------------------------------------------- */

bool fdb_inode_encode(const struct mds_inode *ino, uint8_t *buf, size_t cap, size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (ino == NULL || buf == NULL || len == NULL ||
        ino->inline_fh_len > MDS_NFS_FH_MAX || !type_ok((uint32_t)ino->type) ||
        (uint64_t)ino->atime.tv_nsec >= (uint64_t)NS_PER_SEC ||
        (uint64_t)ino->mtime.tv_nsec >= (uint64_t)NS_PER_SEC ||
        (uint64_t)ino->ctime.tv_nsec >= (uint64_t)NS_PER_SEC) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_INODE_VERSION);
    w_u8(&w, (uint8_t)ino->type);
    w_u16(&w, 0);
    w_u32(&w, ino->mode);
    w_u32(&w, ino->nlink);
    w_u32(&w, ino->flags);
    w_u64(&w, ino->fileid);
    w_u64(&w, ino->uid);
    w_u64(&w, ino->gid);
    w_u64(&w, ino->size);
    w_u64(&w, ino->space_used);
    w_ts(&w, ino->atime);
    w_ts(&w, ino->mtime);
    w_ts(&w, ino->ctime);
    w_u64(&w, ino->change);
    w_u64(&w, ino->generation);
    w_u64(&w, ino->create_verf);
    w_u64(&w, ino->parent_fileid);
    w_u32(&w, ino->synth_suid);
    w_u32(&w, ino->synth_sgid);
    w_u32(&w, ino->stripe_count);
    w_u32(&w, ino->stripe_unit);
    w_u32(&w, ino->mirror_count);
    w_u32(&w, ino->inline_ds_id);
    w_u32(&w, ino->inline_fh_len);
    w_bytes(&w, ino->inline_fh, ino->inline_fh_len);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_inode_decode(const uint8_t *buf, size_t len, struct mds_inode *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_inode ino;
    uint8_t type;

    if (buf == NULL || out == NULL || len < FDB_INODE_ENC_FIXED || len > FDB_INODE_ENC_MAX) {
        return false;
    }
    memset(&ino, 0, sizeof(ino));
    if (r_u8(&r) != FDB_INODE_VERSION) {
        return false;
    }
    type = r_u8(&r);
    (void)r_u16(&r);
    ino.mode = r_u32(&r);
    ino.nlink = r_u32(&r);
    ino.flags = r_u32(&r);
    ino.fileid = r_u64(&r);
    ino.uid = r_u64(&r);
    ino.gid = r_u64(&r);
    ino.size = r_u64(&r);
    ino.space_used = r_u64(&r);
    ino.atime = r_ts(&r);
    ino.mtime = r_ts(&r);
    ino.ctime = r_ts(&r);
    ino.change = r_u64(&r);
    ino.generation = r_u64(&r);
    ino.create_verf = r_u64(&r);
    ino.parent_fileid = r_u64(&r);
    ino.synth_suid = r_u32(&r);
    ino.synth_sgid = r_u32(&r);
    ino.stripe_count = r_u32(&r);
    ino.stripe_unit = r_u32(&r);
    ino.mirror_count = r_u32(&r);
    ino.inline_ds_id = r_u32(&r);
    ino.inline_fh_len = r_u32(&r);
    if (r.fail || !type_ok(type) || ino.inline_fh_len > MDS_NFS_FH_MAX) {
        return false;
    }
    if (!r_bytes(&r, ino.inline_fh, ino.inline_fh_len) || !r_done(&r)) {
        return false;
    }
    ino.type = (enum mds_file_type)type;
    ino.ds_map = NULL;
    *out = ino;
    return true;
}

bool fdb_inode_is_dir(const struct mds_inode *ino)
{
    return ino != NULL && ino->type == MDS_FTYPE_DIR;
}

void fdb_inode_split(const struct mds_inode *in, struct mds_inode *blob_copy,
                     struct fdb_dir_counters *counters)
{
    *blob_copy = *in;
    memset(counters, 0, sizeof(*counters));
    if (!fdb_inode_is_dir(in)) {
        return;
    }
    counters->nlink = in->nlink;
    counters->change = in->change;
    counters->mtime_ns = (uint64_t)fdb_ts_to_ns(in->mtime);
    counters->ctime_ns = (uint64_t)fdb_ts_to_ns(in->ctime);
    /* Dead copies: anything that reads them by mistake sees zeros,
     * which is obviously wrong rather than plausibly stale. */
    blob_copy->nlink = 0;
    blob_copy->change = 0;
    blob_copy->mtime.tv_sec = 0;
    blob_copy->mtime.tv_nsec = 0;
    blob_copy->ctime.tv_sec = 0;
    blob_copy->ctime.tv_nsec = 0;
}

void fdb_inode_compose(struct mds_inode *io, const struct fdb_dir_counters *counters)
{
    int64_t nlink;

    if (!fdb_inode_is_dir(io)) {
        return;
    }
    /* The counter is an unsigned LE u64 that ADD may drive below zero
     * (two's complement wrap); read it as signed and clamp. */
    nlink = (int64_t)counters->nlink;
    if (nlink < 0) {
        io->nlink = 0;
    } else if (nlink > (int64_t)UINT32_MAX) {
        io->nlink = UINT32_MAX;
    } else {
        io->nlink = (uint32_t)nlink;
    }
    io->change = counters->change;
    io->mtime = fdb_ns_to_ts((int64_t)counters->mtime_ns);
    io->ctime = fdb_ns_to_ts((int64_t)counters->ctime_ns);
}

/* -----------------------------------------------------------------------
 * Dirent and dirent-sequence index
 * ----------------------------------------------------------------------- */

bool fdb_dirent_encode(const struct fdb_dirent_val *v, uint8_t *buf, size_t cap, size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (v == NULL || buf == NULL || len == NULL || !type_ok(v->type)) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_DIRENT_VERSION);
    w_u64(&w, v->child_fileid);
    w_u8(&w, v->type);
    w_u64(&w, v->seq);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_dirent_decode(const uint8_t *buf, size_t len, struct fdb_dirent_val *out)
{
    struct rcur r = { buf, len, 0, false };
    struct fdb_dirent_val v;

    if (buf == NULL || out == NULL || len != FDB_DIRENT_ENC_SIZE) {
        return false;
    }
    if (r_u8(&r) != FDB_DIRENT_VERSION) {
        return false;
    }
    v.child_fileid = r_u64(&r);
    v.type = r_u8(&r);
    v.seq = r_u64(&r);
    if (!r_done(&r) || !type_ok(v.type)) {
        return false;
    }
    *out = v;
    return true;
}

bool fdb_dirent_seq_encode(const struct fdb_dirent_seq_val *v, uint8_t *buf, size_t cap,
                           size_t *len)
{
    struct wcur w = { buf, cap, 0, false };
    size_t n;

    if (v == NULL || buf == NULL || len == NULL || !type_ok(v->type)) {
        return false;
    }
    n = strnlen(v->name, sizeof(v->name));
    if (n == 0 || n > MDS_MAX_NAME) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_DIRENT_SEQ_VERSION);
    w_u64(&w, v->child_fileid);
    w_u8(&w, v->type);
    w_bytes(&w, v->name, n);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_dirent_seq_decode(const uint8_t *buf, size_t len, struct fdb_dirent_seq_val *out)
{
    struct rcur r = { buf, len, 0, false };
    struct fdb_dirent_seq_val v;
    size_t n;

    if (buf == NULL || out == NULL || len <= FDB_DIRENT_SEQ_ENC_FIXED ||
        len > FDB_DIRENT_SEQ_ENC_MAX) {
        return false;
    }
    if (r_u8(&r) != FDB_DIRENT_SEQ_VERSION) {
        return false;
    }
    v.child_fileid = r_u64(&r);
    v.type = r_u8(&r);
    n = len - FDB_DIRENT_SEQ_ENC_FIXED;
    if (!r_bytes(&r, v.name, n) || !r_done(&r) || !type_ok(v.type) ||
        memchr(v.name, '\0', n) != NULL) {
        return false;
    }
    v.name[n] = '\0';
    *out = v;
    return true;
}

/* -----------------------------------------------------------------------
 * Stripe map rows
 * ----------------------------------------------------------------------- */

static bool stripe_hdr_ok(const struct fdb_stripe_hdr_val *v)
{
    return v->stripe_count >= 1 && v->stripe_count <= MDS_MAX_STRIPES &&
           v->mirror_count >= 1 && v->mirror_count <= MDS_MAX_MIRRORS &&
           v->stripe_unit != 0;
}

bool fdb_stripe_hdr_encode(const struct fdb_stripe_hdr_val *v, uint8_t *buf, size_t cap,
                           size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (v == NULL || buf == NULL || len == NULL || !stripe_hdr_ok(v)) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_STRIPE_HDR_VERSION);
    w_u32(&w, v->stripe_count);
    w_u32(&w, v->stripe_unit);
    w_u32(&w, v->mirror_count);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_stripe_hdr_decode(const uint8_t *buf, size_t len, struct fdb_stripe_hdr_val *out)
{
    struct rcur r = { buf, len, 0, false };
    struct fdb_stripe_hdr_val v;

    if (buf == NULL || out == NULL || len != FDB_STRIPE_HDR_ENC_SIZE) {
        return false;
    }
    if (r_u8(&r) != FDB_STRIPE_HDR_VERSION) {
        return false;
    }
    v.stripe_count = r_u32(&r);
    v.stripe_unit = r_u32(&r);
    v.mirror_count = r_u32(&r);
    if (!r_done(&r) || !stripe_hdr_ok(&v)) {
        return false;
    }
    *out = v;
    return true;
}

bool fdb_stripe_ent_encode(const struct mds_ds_map_entry *e, uint8_t *buf, size_t cap,
                           size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (e == NULL || buf == NULL || len == NULL || e->nfs_fh_len > MDS_NFS_FH_MAX) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_STRIPE_ENT_VERSION);
    w_u32(&w, e->ds_id);
    w_u32(&w, e->synth_suid);
    w_u32(&w, e->synth_sgid);
    w_u32(&w, e->nfs_fh_len);
    w_bytes(&w, e->nfs_fh, e->nfs_fh_len);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_stripe_ent_decode(const uint8_t *buf, size_t len, struct mds_ds_map_entry *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_ds_map_entry e;

    if (buf == NULL || out == NULL || len < FDB_STRIPE_ENT_ENC_FIXED ||
        len > FDB_STRIPE_ENT_ENC_MAX) {
        return false;
    }
    memset(&e, 0, sizeof(e));
    if (r_u8(&r) != FDB_STRIPE_ENT_VERSION) {
        return false;
    }
    e.ds_id = r_u32(&r);
    e.synth_suid = r_u32(&r);
    e.synth_sgid = r_u32(&r);
    e.nfs_fh_len = r_u32(&r);
    if (r.fail || e.nfs_fh_len > MDS_NFS_FH_MAX) {
        return false;
    }
    if (!r_bytes(&r, e.nfs_fh, e.nfs_fh_len) || !r_done(&r)) {
        return false;
    }
    *out = e;
    return true;
}

/* -----------------------------------------------------------------------
 * GC queue row
 * ----------------------------------------------------------------------- */

bool fdb_gc_encode(const struct mds_gc_entry *e, uint8_t *buf, size_t cap, size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (e == NULL || buf == NULL || len == NULL || e->nfs_fh_len > MDS_NFS_FH_MAX) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_GC_VERSION);
    w_u64(&w, e->fileid);
    w_u32(&w, e->ds_id);
    w_u32(&w, e->owner_mds_id);
    w_u32(&w, e->sweep_hint);
    w_u32(&w, e->nfs_fh_len);
    w_bytes(&w, e->nfs_fh, e->nfs_fh_len);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_gc_decode(const uint8_t *buf, size_t len, struct mds_gc_entry *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_gc_entry e;

    if (buf == NULL || out == NULL || len < FDB_GC_ENC_FIXED || len > FDB_GC_ENC_MAX) {
        return false;
    }
    memset(&e, 0, sizeof(e));
    if (r_u8(&r) != FDB_GC_VERSION) {
        return false;
    }
    e.fileid = r_u64(&r);
    e.ds_id = r_u32(&r);
    e.owner_mds_id = r_u32(&r);
    e.sweep_hint = r_u32(&r);
    e.nfs_fh_len = r_u32(&r);
    if (r.fail || e.nfs_fh_len > MDS_NFS_FH_MAX) {
        return false;
    }
    if (!r_bytes(&r, e.nfs_fh, e.nfs_fh_len) || !r_done(&r)) {
        return false;
    }
    *out = e;
    return true;
}

/* -----------------------------------------------------------------------
 * ext track (catalogue_fdb_ext.c): extended authority tables.  Layouts
 * in fdb_codec.h; same cursors, same exact-consumption rule.
 * ----------------------------------------------------------------------- */

bool fdb_le32_decode(const uint8_t *src, size_t len, uint32_t *out)
{
    if (src == NULL || out == NULL || len != 4) {
        return false;
    }
    *out = fdb_le32_get(src);
    return true;
}

/* Length of a NUL-terminated string stored in an array of @p cap bytes;
 * SIZE_MAX when it is not terminated inside the array. */
static size_t bounded_strlen(const char *s, size_t cap)
{
    size_t n = strnlen(s, cap);

    return (n == cap) ? SIZE_MAX : n;
}

/* Read @p n string bytes into @p dst (array of @p cap bytes) and
 * NUL-terminate; marks the cursor failed when n does not fit. */
static void r_str_body(struct rcur *r, char *dst, size_t cap, size_t n)
{
    if (n >= cap) {
        r->fail = true;
        return;
    }
    if (!r_bytes(r, dst, n)) {
        return;
    }
    dst[n] = '\0';
}

bool fdb_remove_pending_encode(const struct mds_remove_pending_entry *e, uint8_t *buf,
                               size_t cap, size_t *len)
{
    struct wcur w = { buf, cap, 0, false };
    size_t n;

    if (e == NULL || buf == NULL || len == NULL) {
        return false;
    }
    n = strnlen(e->name, sizeof(e->name));
    if (n == 0 || n > MDS_MAX_NAME) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_REMOVE_PENDING_VERSION);
    w_u64(&w, e->dir_fileid);
    w_u64(&w, e->child_fileid);
    w_u64(&w, e->child_generation);
    w_u64(&w, e->enqueued_ns);
    w_u64(&w, e->claim_boot);
    w_u64(&w, e->claim_expires_ns);
    w_u32(&w, e->claim_mds_id);
    w_u32(&w, e->retries);
    w_bytes(&w, e->name, n);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_remove_pending_decode(const uint8_t *buf, size_t len,
                               struct mds_remove_pending_entry *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_remove_pending_entry e;
    size_t n;

    if (buf == NULL || out == NULL || len <= FDB_REMOVE_PENDING_ENC_FIXED ||
        len > FDB_REMOVE_PENDING_ENC_MAX) {
        return false;
    }
    memset(&e, 0, sizeof(e));
    if (r_u8(&r) != FDB_REMOVE_PENDING_VERSION) {
        return false;
    }
    e.dir_fileid = r_u64(&r);
    e.child_fileid = r_u64(&r);
    e.child_generation = r_u64(&r);
    e.enqueued_ns = r_u64(&r);
    e.claim_boot = r_u64(&r);
    e.claim_expires_ns = r_u64(&r);
    e.claim_mds_id = r_u32(&r);
    e.retries = r_u32(&r);
    n = len - FDB_REMOVE_PENDING_ENC_FIXED;
    r_str_body(&r, e.name, sizeof(e.name), n);
    if (!r_done(&r) || memchr(e.name, '\0', n) != NULL) {
        return false;
    }
    *out = e;
    return true;
}

bool fdb_ds_info_encode(const struct mds_ds_info *info, uint8_t *buf, size_t cap, size_t *len)
{
    struct wcur w = { buf, cap, 0, false };
    size_t addr_n;
    size_t host_n;
    size_t export_n;

    if (info == NULL || buf == NULL || len == NULL) {
        return false;
    }
    addr_n = bounded_strlen(info->addr, sizeof(info->addr));
    host_n = bounded_strlen(info->host, sizeof(info->host));
    export_n = bounded_strlen(info->export_path, sizeof(info->export_path));
    if (addr_n == SIZE_MAX || host_n == SIZE_MAX || export_n == SIZE_MAX) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_DS_INFO_VERSION);
    w_u32(&w, info->ds_id);
    w_u32(&w, info->state);
    w_u32(&w, info->tier);
    w_u64(&w, info->total_bytes);
    w_u64(&w, info->used_bytes);
    w_u16(&w, info->port);
    w_u16(&w, info->tcp_port);
    w_u16(&w, info->rdma_port);
    w_u8(&w, info->mode);
    w_u8(&w, info->transport);
    w_u32(&w, info->capabilities);
    w_u32(&w, info->weight);
    w_u16(&w, (uint16_t)addr_n);
    w_u16(&w, (uint16_t)host_n);
    w_u16(&w, (uint16_t)export_n);
    w_bytes(&w, info->addr, addr_n);
    w_bytes(&w, info->host, host_n);
    w_bytes(&w, info->export_path, export_n);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_ds_info_decode(const uint8_t *buf, size_t len, struct mds_ds_info *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_ds_info info;
    size_t addr_n;
    size_t host_n;
    size_t export_n;

    if (buf == NULL || out == NULL || len < FDB_DS_INFO_ENC_FIXED || len > FDB_DS_INFO_ENC_MAX) {
        return false;
    }
    memset(&info, 0, sizeof(info));
    if (r_u8(&r) != FDB_DS_INFO_VERSION) {
        return false;
    }
    info.ds_id = r_u32(&r);
    info.state = r_u32(&r);
    info.tier = r_u32(&r);
    info.total_bytes = r_u64(&r);
    info.used_bytes = r_u64(&r);
    info.port = r_u16(&r);
    info.tcp_port = r_u16(&r);
    info.rdma_port = r_u16(&r);
    info.mode = r_u8(&r);
    info.transport = r_u8(&r);
    info.capabilities = r_u32(&r);
    info.weight = r_u32(&r);
    addr_n = r_u16(&r);
    host_n = r_u16(&r);
    export_n = r_u16(&r);
    if (r.fail) {
        return false;
    }
    r_str_body(&r, info.addr, sizeof(info.addr), addr_n);
    r_str_body(&r, info.host, sizeof(info.host), host_n);
    r_str_body(&r, info.export_path, sizeof(info.export_path), export_n);
    if (!r_done(&r)) {
        return false;
    }
    *out = info;
    return true;
}

bool fdb_ds_provision_encode(const struct fdb_ds_provision_val *v, uint8_t *buf, size_t cap,
                             size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (v == NULL || buf == NULL || len == NULL || v->secret_len > FDB_DS_SECRET_MAX) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_DS_PROVISION_VERSION);
    w_u64(&w, v->epoch);
    w_u8(&w, (uint8_t)v->secret_len);
    w_bytes(&w, v->secret, v->secret_len);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_ds_provision_decode(const uint8_t *buf, size_t len, struct fdb_ds_provision_val *out)
{
    struct rcur r = { buf, len, 0, false };
    struct fdb_ds_provision_val v;

    if (buf == NULL || out == NULL || len < FDB_DS_PROVISION_ENC_FIXED ||
        len > FDB_DS_PROVISION_ENC_MAX) {
        return false;
    }
    memset(&v, 0, sizeof(v));
    if (r_u8(&r) != FDB_DS_PROVISION_VERSION) {
        return false;
    }
    v.epoch = r_u64(&r);
    v.secret_len = r_u8(&r);
    if (r.fail || v.secret_len > FDB_DS_SECRET_MAX) {
        return false;
    }
    if (!r_bytes(&r, v.secret, v.secret_len) || !r_done(&r)) {
        return false;
    }
    *out = v;
    return true;
}

bool fdb_quota_rule_encode(const struct mds_quota_rule *q, uint8_t *buf, size_t cap,
                           size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (q == NULL || buf == NULL || len == NULL) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_QUOTA_RULE_VERSION);
    w_u64(&w, q->hard_bytes);
    w_u64(&w, q->soft_bytes);
    w_u64(&w, q->hard_inodes);
    w_u64(&w, q->soft_inodes);
    w_u32(&w, q->grace_sec);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_quota_rule_decode(const uint8_t *buf, size_t len, struct mds_quota_rule *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_quota_rule q;

    if (buf == NULL || out == NULL || len != FDB_QUOTA_RULE_ENC_SIZE) {
        return false;
    }
    memset(&q, 0, sizeof(q));
    if (r_u8(&r) != FDB_QUOTA_RULE_VERSION) {
        return false;
    }
    q.hard_bytes = r_u64(&r);
    q.soft_bytes = r_u64(&r);
    q.hard_inodes = r_u64(&r);
    q.soft_inodes = r_u64(&r);
    q.grace_sec = r_u32(&r);
    if (!r_done(&r)) {
        return false;
    }
    *out = q;
    return true;
}

bool fdb_quota_usage_encode(const struct mds_quota_usage *u, uint8_t *buf, size_t cap,
                            size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (u == NULL || buf == NULL || len == NULL) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_QUOTA_USAGE_VERSION);
    w_u64(&w, u->used_bytes);
    w_u64(&w, u->used_inodes);
    w_u64(&w, (uint64_t)u->grace_start_bytes);
    w_u64(&w, (uint64_t)u->grace_start_inodes);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_quota_usage_decode(const uint8_t *buf, size_t len, struct mds_quota_usage *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_quota_usage u;

    if (buf == NULL || out == NULL || len != FDB_QUOTA_USAGE_ENC_SIZE) {
        return false;
    }
    memset(&u, 0, sizeof(u));
    if (r_u8(&r) != FDB_QUOTA_USAGE_VERSION) {
        return false;
    }
    u.used_bytes = r_u64(&r);
    u.used_inodes = r_u64(&r);
    u.grace_start_bytes = (int64_t)r_u64(&r);
    u.grace_start_inodes = (int64_t)r_u64(&r);
    if (!r_done(&r)) {
        return false;
    }
    *out = u;
    return true;
}

bool fdb_ext_dirent_encode(const struct fdb_ext_dirent_val *v, uint8_t *buf, size_t cap,
                           size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (v == NULL || buf == NULL || len == NULL) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_EXT_DIRENT_VERSION);
    w_u32(&w, v->owner_mds_id);
    w_u64(&w, v->target_fileid);
    w_u8(&w, v->target_type);
    w_u64(&w, v->anchor_id);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_ext_dirent_decode(const uint8_t *buf, size_t len, struct fdb_ext_dirent_val *out)
{
    struct rcur r = { buf, len, 0, false };
    struct fdb_ext_dirent_val v;

    if (buf == NULL || out == NULL || len != FDB_EXT_DIRENT_ENC_SIZE) {
        return false;
    }
    memset(&v, 0, sizeof(v));
    if (r_u8(&r) != FDB_EXT_DIRENT_VERSION) {
        return false;
    }
    v.owner_mds_id = r_u32(&r);
    v.target_fileid = r_u64(&r);
    v.target_type = r_u8(&r);
    v.anchor_id = r_u64(&r);
    if (!r_done(&r)) {
        return false;
    }
    *out = v;
    return true;
}

bool fdb_link_anchor_encode(const struct fdb_link_anchor_val *v, uint8_t *buf, size_t cap,
                            size_t *len)
{
    struct wcur w = { buf, cap, 0, false };
    size_t n;

    if (v == NULL || buf == NULL || len == NULL) {
        return false;
    }
    n = strnlen(v->name, sizeof(v->name));
    if (n > MDS_MAX_NAME) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_LINK_ANCHOR_VERSION);
    w_u32(&w, v->remote_mds_id);
    w_u64(&w, v->parent_fileid);
    w_bytes(&w, v->name, n);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_link_anchor_decode(const uint8_t *buf, size_t len, struct fdb_link_anchor_val *out)
{
    struct rcur r = { buf, len, 0, false };
    struct fdb_link_anchor_val v;
    size_t n;

    if (buf == NULL || out == NULL || len < FDB_LINK_ANCHOR_ENC_FIXED ||
        len > FDB_LINK_ANCHOR_ENC_MAX) {
        return false;
    }
    memset(&v, 0, sizeof(v));
    if (r_u8(&r) != FDB_LINK_ANCHOR_VERSION) {
        return false;
    }
    v.remote_mds_id = r_u32(&r);
    v.parent_fileid = r_u64(&r);
    n = len - FDB_LINK_ANCHOR_ENC_FIXED;
    r_str_body(&r, v.name, sizeof(v.name), n);
    if (!r_done(&r) || (n > 0 && memchr(v.name, '\0', n) != NULL)) {
        return false;
    }
    *out = v;
    return true;
}

/* End of ext track section. */

/* =======================================================================
 * fdb-coord track -- coordination and cluster row formats
 * (layouts in fdb_codec.h)
 * ======================================================================= */

/* --- Layout state ------------------------------------------------------ */

bool fdb_layout_encode(const struct fdb_layout_hdr *h, const uint32_t *ds_ids, uint8_t *buf,
                       size_t cap, size_t *len)
{
    struct wcur w = { buf, cap, 0, false };
    uint32_t i;

    if (h == NULL || buf == NULL || len == NULL || h->ds_count > FDB_LAYOUT_DS_MAX ||
        (h->ds_count > 0 && ds_ids == NULL)) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_LAYOUT_VERSION);
    w_u64(&w, h->clientid);
    w_u64(&w, h->fileid);
    w_u32(&w, h->iomode);
    w_u64(&w, h->offset);
    w_u64(&w, h->length);
    w_u32(&w, h->seqid);
    w_u32(&w, h->grant_owner_mds_id);
    w_u32(&w, h->ds_count);
    for (i = 0; i < h->ds_count && !w.fail; i++) {
        w_u32(&w, ds_ids[i]);
    }
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

/* Header part of a layout row; on success the cursor sits at the DS list. */
static bool layout_hdr_read(struct rcur *r, struct fdb_layout_hdr *h)
{
    if (r_u8(r) != FDB_LAYOUT_VERSION) {
        return false;
    }
    h->clientid = r_u64(r);
    h->fileid = r_u64(r);
    h->iomode = r_u32(r);
    h->offset = r_u64(r);
    h->length = r_u64(r);
    h->seqid = r_u32(r);
    h->grant_owner_mds_id = r_u32(r);
    h->ds_count = r_u32(r);
    if (r->fail || h->ds_count > FDB_LAYOUT_DS_MAX ||
        r->len - r->pos != (size_t)h->ds_count * 4U) {
        return false;
    }
    return true;
}

bool fdb_layout_decode(const uint8_t *buf, size_t len, struct fdb_layout_hdr *h,
                       uint32_t *ds_ids, uint32_t ds_cap)
{
    struct rcur r = { buf, len, 0, false };
    struct fdb_layout_hdr hdr;

    if (buf == NULL || h == NULL || len < FDB_LAYOUT_ENC_FIXED || len > FDB_LAYOUT_ENC_MAX) {
        return false;
    }
    memset(&hdr, 0, sizeof(hdr));
    if (!layout_hdr_read(&r, &hdr)) {
        return false;
    }
    if (ds_ids != NULL) {
        uint32_t i;

        if (hdr.ds_count > ds_cap) {
            return false;
        }
        for (i = 0; i < hdr.ds_count; i++) {
            ds_ids[i] = r_u32(&r);
        }
        if (!r_done(&r)) {
            return false;
        }
    }
    *h = hdr;
    return true;
}

bool fdb_layout_ds_contains(const uint8_t *buf, size_t len, uint32_t ds_id)
{
    struct rcur r = { buf, len, 0, false };
    struct fdb_layout_hdr hdr;
    uint32_t i;

    if (buf == NULL || len < FDB_LAYOUT_ENC_FIXED || len > FDB_LAYOUT_ENC_MAX) {
        return false;
    }
    memset(&hdr, 0, sizeof(hdr));
    if (!layout_hdr_read(&r, &hdr)) {
        return false;
    }
    for (i = 0; i < hdr.ds_count; i++) {
        if (r_u32(&r) == ds_id) {
            return !r.fail;
        }
    }
    return false;
}

/* --- Shared protocol state --------------------------------------------- */

bool fdb_open_encode(const struct mds_coord_open_row *r, uint8_t *buf, size_t cap, size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (r == NULL || buf == NULL || len == NULL || r->open_owner_len > sizeof(r->open_owner)) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_OPEN_VERSION);
    w_u32(&w, r->seqid);
    w_u64(&w, r->clientid);
    w_u64(&w, r->fileid);
    w_u32(&w, r->share_access);
    w_u32(&w, r->share_deny);
    w_u32(&w, r->open_owner_len);
    w_bytes(&w, r->open_owner, r->open_owner_len);
    w_u32(&w, r->owner_mds_id);
    w_u64(&w, r->owner_boot_epoch);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_open_decode(const uint8_t *buf, size_t len, const uint8_t stateid_other[12],
                     struct mds_coord_open_row *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_coord_open_row row;

    if (buf == NULL || stateid_other == NULL || out == NULL || len < FDB_OPEN_ENC_FIXED ||
        len > FDB_OPEN_ENC_MAX) {
        return false;
    }
    memset(&row, 0, sizeof(row));
    if (r_u8(&r) != FDB_OPEN_VERSION) {
        return false;
    }
    row.seqid = r_u32(&r);
    row.clientid = r_u64(&r);
    row.fileid = r_u64(&r);
    row.share_access = r_u32(&r);
    row.share_deny = r_u32(&r);
    row.open_owner_len = r_u32(&r);
    if (r.fail || row.open_owner_len > sizeof(row.open_owner)) {
        return false;
    }
    (void)r_bytes(&r, row.open_owner, row.open_owner_len);
    row.owner_mds_id = r_u32(&r);
    row.owner_boot_epoch = r_u64(&r);
    if (!r_done(&r)) {
        return false;
    }
    memcpy(row.stateid_other, stateid_other, sizeof(row.stateid_other));
    *out = row;
    return true;
}

bool fdb_lock_encode(const struct mds_coord_lock_row *r, uint8_t *buf, size_t cap, size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (r == NULL || buf == NULL || len == NULL || r->owner_len > sizeof(r->owner)) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_LOCK_VERSION);
    w_u64(&w, r->offset);
    w_u64(&w, r->length);
    w_u32(&w, r->lock_type);
    w_u64(&w, r->clientid);
    w_u32(&w, r->owner_len);
    w_bytes(&w, r->owner, r->owner_len);
    w_bytes(&w, r->stateid_other, sizeof(r->stateid_other));
    w_u32(&w, r->seqid);
    w_bytes(&w, r->open_stateid_other, sizeof(r->open_stateid_other));
    w_u32(&w, r->owner_mds_id);
    w_u64(&w, r->owner_boot_epoch);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_lock_decode(const uint8_t *buf, size_t len, uint64_t fileid, uint64_t lock_id,
                     struct mds_coord_lock_row *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_coord_lock_row row;

    if (buf == NULL || out == NULL || len < FDB_LOCK_ENC_FIXED || len > FDB_LOCK_ENC_MAX) {
        return false;
    }
    memset(&row, 0, sizeof(row));
    if (r_u8(&r) != FDB_LOCK_VERSION) {
        return false;
    }
    row.offset = r_u64(&r);
    row.length = r_u64(&r);
    row.lock_type = r_u32(&r);
    row.clientid = r_u64(&r);
    row.owner_len = r_u32(&r);
    if (r.fail || row.owner_len > sizeof(row.owner)) {
        return false;
    }
    (void)r_bytes(&r, row.owner, row.owner_len);
    (void)r_bytes(&r, row.stateid_other, sizeof(row.stateid_other));
    row.seqid = r_u32(&r);
    (void)r_bytes(&r, row.open_stateid_other, sizeof(row.open_stateid_other));
    row.owner_mds_id = r_u32(&r);
    row.owner_boot_epoch = r_u64(&r);
    if (!r_done(&r)) {
        return false;
    }
    row.fileid = fileid;
    row.lock_id = lock_id;
    *out = row;
    return true;
}

bool fdb_deleg_encode(const struct mds_coord_deleg_row *r, uint8_t *buf, size_t cap,
                      size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (r == NULL || buf == NULL || len == NULL) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_DELEG_VERSION);
    w_u32(&w, r->seqid);
    w_u64(&w, r->clientid);
    w_u64(&w, r->fileid);
    w_u32(&w, r->deleg_type);
    w_u32(&w, r->owner_mds_id);
    w_u64(&w, r->owner_boot_epoch);
    w_u64(&w, r->grant_time_ns);
    w_u8(&w, r->recall_pending);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_deleg_decode(const uint8_t *buf, size_t len, const uint8_t stateid_other[12],
                      struct mds_coord_deleg_row *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_coord_deleg_row row;

    if (buf == NULL || stateid_other == NULL || out == NULL || len != FDB_DELEG_ENC_SIZE) {
        return false;
    }
    memset(&row, 0, sizeof(row));
    if (r_u8(&r) != FDB_DELEG_VERSION) {
        return false;
    }
    row.seqid = r_u32(&r);
    row.clientid = r_u64(&r);
    row.fileid = r_u64(&r);
    row.deleg_type = r_u32(&r);
    row.owner_mds_id = r_u32(&r);
    row.owner_boot_epoch = r_u64(&r);
    row.grant_time_ns = r_u64(&r);
    row.recall_pending = r_u8(&r);
    if (!r_done(&r)) {
        return false;
    }
    memcpy(row.stateid_other, stateid_other, sizeof(row.stateid_other));
    *out = row;
    return true;
}

bool fdb_client_encode(const struct mds_coord_client_row *r, uint8_t *buf, size_t cap,
                       size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (r == NULL || buf == NULL || len == NULL || r->co_ownerid_len > sizeof(r->co_ownerid)) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_CLIENT_VERSION);
    w_u32(&w, r->co_ownerid_len);
    w_bytes(&w, r->co_ownerid, r->co_ownerid_len);
    w_bytes(&w, r->verifier, sizeof(r->verifier));
    w_u8(&w, r->confirmed ? 1U : 0U);
    w_u32(&w, r->owner_mds_id);
    w_u64(&w, r->owner_boot_epoch);
    w_u64(&w, r->lease_renewed_ns);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_client_decode(const uint8_t *buf, size_t len, uint64_t clientid,
                       struct mds_coord_client_row *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_coord_client_row row;
    uint8_t confirmed;

    if (buf == NULL || out == NULL || len < FDB_CLIENT_ENC_FIXED || len > FDB_CLIENT_ENC_MAX) {
        return false;
    }
    memset(&row, 0, sizeof(row));
    if (r_u8(&r) != FDB_CLIENT_VERSION) {
        return false;
    }
    row.co_ownerid_len = r_u32(&r);
    if (r.fail || row.co_ownerid_len > sizeof(row.co_ownerid)) {
        return false;
    }
    (void)r_bytes(&r, row.co_ownerid, row.co_ownerid_len);
    (void)r_bytes(&r, row.verifier, sizeof(row.verifier));
    confirmed = r_u8(&r);
    row.owner_mds_id = r_u32(&r);
    row.owner_boot_epoch = r_u64(&r);
    row.lease_renewed_ns = r_u64(&r);
    if (!r_done(&r) || confirmed > 1U) {
        return false;
    }
    row.confirmed = (confirmed == 1U);
    row.clientid = clientid;
    *out = row;
    return true;
}

bool fdb_session_encode(const struct mds_coord_session_row *r, uint8_t *buf, size_t cap,
                        size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (r == NULL || buf == NULL || len == NULL) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_SESSION_VERSION);
    w_u64(&w, r->clientid);
    w_u32(&w, r->num_slots);
    w_u32(&w, r->cb_prog);
    w_u32(&w, r->cb_sec_flavor);
    w_u32(&w, r->owner_mds_id);
    w_u64(&w, r->owner_boot_epoch);
    w_u64(&w, r->created_ns);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_session_decode(const uint8_t *buf, size_t len, const uint8_t session_id[16],
                        struct mds_coord_session_row *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_coord_session_row row;

    if (buf == NULL || session_id == NULL || out == NULL || len != FDB_SESSION_ENC_SIZE) {
        return false;
    }
    memset(&row, 0, sizeof(row));
    if (r_u8(&r) != FDB_SESSION_VERSION) {
        return false;
    }
    row.clientid = r_u64(&r);
    row.num_slots = r_u32(&r);
    row.cb_prog = r_u32(&r);
    row.cb_sec_flavor = r_u32(&r);
    row.owner_mds_id = r_u32(&r);
    row.owner_boot_epoch = r_u64(&r);
    row.created_ns = r_u64(&r);
    if (!r_done(&r)) {
        return false;
    }
    memcpy(row.session_id, session_id, sizeof(row.session_id));
    *out = row;
    return true;
}

bool fdb_slot_encode(const struct fdb_slot_hdr *h, const uint8_t *reply, uint8_t *buf,
                     size_t cap, size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (h == NULL || buf == NULL || len == NULL || h->reply_len > FDB_SLOT_REPLY_MAX ||
        (h->reply_len > 0 && reply == NULL)) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_SLOT_VERSION);
    w_u32(&w, h->seq_id);
    w_u64(&w, h->last_used_ns);
    w_u32(&w, h->reply_len);
    if (h->reply_len > 0) {
        w_bytes(&w, reply, h->reply_len);
    }
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_slot_decode(const uint8_t *buf, size_t len, struct fdb_slot_hdr *h,
                     const uint8_t **reply)
{
    struct rcur r = { buf, len, 0, false };
    struct fdb_slot_hdr hdr;

    if (buf == NULL || h == NULL || reply == NULL || len < FDB_SLOT_ENC_FIXED ||
        len > FDB_SLOT_ENC_MAX) {
        return false;
    }
    memset(&hdr, 0, sizeof(hdr));
    if (r_u8(&r) != FDB_SLOT_VERSION) {
        return false;
    }
    hdr.seq_id = r_u32(&r);
    hdr.last_used_ns = r_u64(&r);
    hdr.reply_len = r_u32(&r);
    if (r.fail || hdr.reply_len > FDB_SLOT_REPLY_MAX || len - r.pos != hdr.reply_len) {
        return false;
    }
    *reply = (hdr.reply_len > 0) ? buf + r.pos : NULL;
    *h = hdr;
    return true;
}

/* --- Client recovery --------------------------------------------------- */

bool fdb_recovery_encode(const struct fdb_recovery_val *v, uint8_t *buf, size_t cap,
                         size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (v == NULL || buf == NULL || len == NULL || v->co_ownerid_len > FDB_RECOVERY_CO_MAX) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_RECOVERY_VERSION);
    w_u32(&w, v->owner_mds_id);
    w_u64(&w, v->owner_boot_epoch);
    w_u32(&w, v->co_ownerid_len);
    w_bytes(&w, v->co_ownerid, v->co_ownerid_len);
    w_bytes(&w, v->verifier, sizeof(v->verifier));
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_recovery_decode(const uint8_t *buf, size_t len, struct fdb_recovery_val *out)
{
    struct rcur r = { buf, len, 0, false };
    struct fdb_recovery_val v;

    if (buf == NULL || out == NULL || len < FDB_RECOVERY_ENC_FIXED ||
        len > FDB_RECOVERY_ENC_MAX) {
        return false;
    }
    memset(&v, 0, sizeof(v));
    if (r_u8(&r) != FDB_RECOVERY_VERSION) {
        return false;
    }
    v.owner_mds_id = r_u32(&r);
    v.owner_boot_epoch = r_u64(&r);
    v.co_ownerid_len = r_u32(&r);
    if (r.fail || v.co_ownerid_len > FDB_RECOVERY_CO_MAX) {
        return false;
    }
    (void)r_bytes(&r, v.co_ownerid, v.co_ownerid_len);
    (void)r_bytes(&r, v.verifier, sizeof(v.verifier));
    if (!r_done(&r)) {
        return false;
    }
    *out = v;
    return true;
}

/* --- 2PC journal ------------------------------------------------------- */

bool fdb_journal_encode(const struct mds_coord_journal_record *rec, uint8_t *buf, size_t cap,
                        size_t *len)
{
    struct wcur w = { buf, cap, 0, false };
    size_t src_n;
    size_t dst_n;

    if (rec == NULL || buf == NULL || len == NULL ||
        rec->payload_len > MDS_COORD_JOURNAL_PAYLOAD_MAX) {
        return false;
    }
    src_n = strnlen(rec->src_name, sizeof(rec->src_name));
    dst_n = strnlen(rec->dst_name, sizeof(rec->dst_name));
    if (src_n > MDS_MAX_NAME || dst_n > MDS_MAX_NAME) {
        return false; /* not NUL-terminated within the array */
    }
    w_u8(&w, (uint8_t)FDB_JOURNAL_VERSION);
    w_u8(&w, rec->state);
    w_u32(&w, rec->remote_mds_id);
    w_u64(&w, rec->src_parent_fileid);
    w_u64(&w, rec->dst_parent_fileid);
    w_u64(&w, rec->src_child_fileid);
    w_u64(&w, rec->created_at_ns);
    w_u16(&w, (uint16_t)src_n);
    w_u16(&w, (uint16_t)dst_n);
    w_u32(&w, rec->payload_len);
    w_bytes(&w, rec->src_name, src_n);
    w_bytes(&w, rec->dst_name, dst_n);
    w_bytes(&w, rec->payload, rec->payload_len);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_journal_decode(const uint8_t *buf, size_t len, uint64_t txn_id, uint8_t role,
                        struct mds_coord_journal_record *out)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_coord_journal_record rec;
    uint16_t src_n;
    uint16_t dst_n;

    if (buf == NULL || out == NULL || len < FDB_JOURNAL_ENC_FIXED || len > FDB_JOURNAL_ENC_MAX) {
        return false;
    }
    memset(&rec, 0, sizeof(rec));
    if (r_u8(&r) != FDB_JOURNAL_VERSION) {
        return false;
    }
    rec.state = r_u8(&r);
    rec.remote_mds_id = r_u32(&r);
    rec.src_parent_fileid = r_u64(&r);
    rec.dst_parent_fileid = r_u64(&r);
    rec.src_child_fileid = r_u64(&r);
    rec.created_at_ns = r_u64(&r);
    src_n = r_u16(&r);
    dst_n = r_u16(&r);
    rec.payload_len = r_u32(&r);
    if (r.fail || src_n > MDS_MAX_NAME || dst_n > MDS_MAX_NAME ||
        rec.payload_len > MDS_COORD_JOURNAL_PAYLOAD_MAX) {
        return false;
    }
    (void)r_bytes(&r, rec.src_name, src_n);
    (void)r_bytes(&r, rec.dst_name, dst_n);
    (void)r_bytes(&r, rec.payload, rec.payload_len);
    if (!r_done(&r) || memchr(rec.src_name, '\0', src_n) != NULL ||
        memchr(rec.dst_name, '\0', dst_n) != NULL) {
        return false;
    }
    rec.src_name[src_n] = '\0';
    rec.dst_name[dst_n] = '\0';
    rec.txn_id = txn_id;
    rec.role = role;
    *out = rec;
    return true;
}

/* --- Cluster: node registry and partition map -------------------------- */

bool fdb_node_encode(const struct fdb_node_val *v, uint8_t *buf, size_t cap, size_t *len)
{
    struct wcur w = { buf, cap, 0, false };
    size_t sw_n;
    size_t host_n;

    if (v == NULL || buf == NULL || len == NULL) {
        return false;
    }
    sw_n = strnlen(v->sw_version, sizeof(v->sw_version));
    host_n = strnlen(v->hostname, sizeof(v->hostname));
    if (sw_n > FDB_NODE_SW_MAX || host_n == 0 || host_n > FDB_NODE_HOST_MAX) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_NODE_VERSION);
    w_u64(&w, v->boot_epoch);
    w_u16(&w, v->nfs_port);
    w_u16(&w, v->grpc_port);
    w_u8(&w, v->state);
    w_u64(&w, v->last_heartbeat_ns);
    w_u64(&w, v->witness_epoch);
    w_u8(&w, (uint8_t)sw_n);
    w_bytes(&w, v->sw_version, sw_n);
    w_u16(&w, (uint16_t)host_n);
    w_bytes(&w, v->hostname, host_n);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_node_decode(const uint8_t *buf, size_t len, struct fdb_node_val *out)
{
    struct rcur r = { buf, len, 0, false };
    struct fdb_node_val v;
    uint8_t sw_n;
    uint16_t host_n;

    if (buf == NULL || out == NULL || len < FDB_NODE_ENC_FIXED || len > FDB_NODE_ENC_MAX) {
        return false;
    }
    memset(&v, 0, sizeof(v));
    if (r_u8(&r) != FDB_NODE_VERSION) {
        return false;
    }
    v.boot_epoch = r_u64(&r);
    v.nfs_port = r_u16(&r);
    v.grpc_port = r_u16(&r);
    v.state = r_u8(&r);
    v.last_heartbeat_ns = r_u64(&r);
    v.witness_epoch = r_u64(&r);
    sw_n = r_u8(&r);
    if (r.fail || sw_n > FDB_NODE_SW_MAX) {
        return false;
    }
    (void)r_bytes(&r, v.sw_version, sw_n);
    host_n = r_u16(&r);
    if (r.fail || host_n == 0 || host_n > FDB_NODE_HOST_MAX) {
        return false;
    }
    (void)r_bytes(&r, v.hostname, host_n);
    if (!r_done(&r) || memchr(v.sw_version, '\0', sw_n) != NULL ||
        memchr(v.hostname, '\0', host_n) != NULL) {
        return false;
    }
    v.sw_version[sw_n] = '\0';
    v.hostname[host_n] = '\0';
    *out = v;
    return true;
}

bool fdb_partition_encode(const struct fdb_partition_val *v, uint8_t *buf, size_t cap,
                          size_t *len)
{
    struct wcur w = { buf, cap, 0, false };
    size_t path_n;

    if (v == NULL || buf == NULL || len == NULL) {
        return false;
    }
    path_n = strnlen(v->subtree_path, sizeof(v->subtree_path));
    if (path_n == 0 || path_n > FDB_PARTITION_PATH_MAX) {
        return false;
    }
    w_u8(&w, (uint8_t)FDB_PARTITION_VERSION);
    w_u32(&w, v->owner_mds_id);
    w_u8(&w, v->state);
    w_u16(&w, (uint16_t)path_n);
    w_bytes(&w, v->subtree_path, path_n);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool fdb_partition_decode(const uint8_t *buf, size_t len, struct fdb_partition_val *out)
{
    struct rcur r = { buf, len, 0, false };
    struct fdb_partition_val v;
    uint16_t path_n;

    if (buf == NULL || out == NULL || len < FDB_PARTITION_ENC_FIXED ||
        len > FDB_PARTITION_ENC_MAX) {
        return false;
    }
    memset(&v, 0, sizeof(v));
    if (r_u8(&r) != FDB_PARTITION_VERSION) {
        return false;
    }
    v.owner_mds_id = r_u32(&r);
    v.state = r_u8(&r);
    path_n = r_u16(&r);
    if (r.fail || path_n == 0 || path_n > FDB_PARTITION_PATH_MAX) {
        return false;
    }
    (void)r_bytes(&r, v.subtree_path, path_n);
    if (!r_done(&r) || memchr(v.subtree_path, '\0', path_n) != NULL) {
        return false;
    }
    v.subtree_path[path_n] = '\0';
    *out = v;
    return true;
}

/* ===================== end of fdb-coord track section ==================== */

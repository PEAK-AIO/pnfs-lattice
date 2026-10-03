/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * daos_codec.c -- Value formats specific to the DAOS catalogue backend.
 *
 * See daos_codec.h for the layouts.  Encoders write through a bounded
 * cursor that refuses to run past the caller's buffer; decoders read
 * through a cursor that refuses to run past the value and then require
 * the value to be consumed exactly, so a truncated or padded value is
 * rejected rather than partially trusted.  No store interaction.
 */

#include <stdlib.h>
#include <string.h>

#include "daos_codec.h"

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

static bool r_done(const struct rcur *r)
{
    return !r->fail && r->pos == r->len;
}

/* -----------------------------------------------------------------------
 * Directory counter shard
 * ----------------------------------------------------------------------- */

bool daos_dir_shard_encode(const struct daos_dir_shard *s, uint8_t *buf, size_t cap,
                           size_t *len)
{
    struct wcur w = { buf, cap, 0, false };

    if (s == NULL || buf == NULL || len == NULL) {
        return false;
    }
    w_u8(&w, (uint8_t)DAOS_DIR_SHARD_VERSION);
    w_u64(&w, (uint64_t)s->nlink_delta);
    w_u64(&w, s->change);
    w_u64(&w, (uint64_t)s->mtime_ns);
    w_u64(&w, (uint64_t)s->ctime_ns);
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool daos_dir_shard_decode(const uint8_t *buf, size_t len, struct daos_dir_shard *out)
{
    struct rcur r = { buf, len, 0, false };
    struct daos_dir_shard s;

    if (buf == NULL || out == NULL || len != DAOS_DIR_SHARD_ENC_SIZE) {
        return false;
    }
    if (r_u8(&r) != DAOS_DIR_SHARD_VERSION) {
        return false;
    }
    s.nlink_delta = (int64_t)r_u64(&r);
    s.change = r_u64(&r);
    s.mtime_ns = (int64_t)r_u64(&r);
    s.ctime_ns = (int64_t)r_u64(&r);
    if (!r_done(&r)) {
        return false;
    }
    *out = s;
    return true;
}

void daos_dir_shards_compose(struct mds_inode *io, const struct daos_dir_shard *shards,
                             uint32_t n)
{
    struct fdb_dir_counters ctr;
    int64_t nlink = 0;
    int64_t mtime_ns = INT64_MIN;
    int64_t ctime_ns = INT64_MIN;
    uint64_t change = 0;
    uint32_t i;

    if (io == NULL || !fdb_inode_is_dir(io)) {
        return;
    }
    for (i = 0; i < n && shards != NULL; i++) {
        const struct daos_dir_shard *s = &shards[i];

        /* Saturating sum: a corrupt shard must not wrap the link count
         * into a huge positive value. */
        if (s->nlink_delta > 0 && nlink > INT64_MAX - s->nlink_delta) {
            nlink = INT64_MAX;
        } else if (s->nlink_delta < 0 && nlink < INT64_MIN - s->nlink_delta) {
            nlink = INT64_MIN;
        } else {
            nlink += s->nlink_delta;
        }
        change += s->change;
        if (s->mtime_ns > mtime_ns) {
            mtime_ns = s->mtime_ns;
        }
        if (s->ctime_ns > ctime_ns) {
            ctime_ns = s->ctime_ns;
        }
    }
    if (n == 0 || shards == NULL) {
        mtime_ns = 0;
        ctime_ns = 0;
    }
    ctr.nlink = (uint64_t)nlink;
    ctr.change = change;
    ctr.mtime_ns = (uint64_t)mtime_ns;
    ctr.ctime_ns = (uint64_t)ctime_ns;
    fdb_inode_compose(io, &ctr);
}

/* -----------------------------------------------------------------------
 * Stripe map blob
 * ----------------------------------------------------------------------- */

static bool stripe_geometry_ok(uint32_t sc, uint32_t mc)
{
    return sc >= 1U && sc <= (uint32_t)MDS_MAX_STRIPES && mc >= 1U &&
           mc <= (uint32_t)MDS_MAX_MIRRORS;
}

size_t daos_stripe_map_enc_size(uint32_t sc, uint32_t mc, const struct mds_ds_map_entry *entries)
{
    size_t total = DAOS_STRIPE_MAP_ENC_FIXED;
    uint32_t n;
    uint32_t i;

    if (entries == NULL || !stripe_geometry_ok(sc, mc)) {
        return 0;
    }
    n = sc * mc;
    for (i = 0; i < n; i++) {
        if (entries[i].nfs_fh_len > MDS_NFS_FH_MAX) {
            return 0;
        }
        total += 2U + FDB_STRIPE_ENT_ENC_FIXED + entries[i].nfs_fh_len;
    }
    return total;
}

bool daos_stripe_map_encode(uint32_t sc, uint32_t su, uint32_t mc,
                            const struct mds_ds_map_entry *entries, uint8_t *buf, size_t cap,
                            size_t *len)
{
    struct wcur w = { buf, cap, 0, false };
    uint32_t n;
    uint32_t i;

    if (entries == NULL || buf == NULL || len == NULL || su == 0 ||
        !stripe_geometry_ok(sc, mc)) {
        return false;
    }
    n = sc * mc;
    w_u8(&w, (uint8_t)DAOS_STRIPE_MAP_VERSION);
    w_u32(&w, sc);
    w_u32(&w, su);
    w_u32(&w, mc);
    w_u32(&w, n);
    for (i = 0; i < n; i++) {
        uint8_t ent[FDB_STRIPE_ENT_ENC_MAX];
        size_t elen = 0;

        if (!fdb_stripe_ent_encode(&entries[i], ent, sizeof(ent), &elen)) {
            return false;
        }
        w_u16(&w, (uint16_t)elen);
        w_bytes(&w, ent, elen);
    }
    if (w.fail) {
        return false;
    }
    *len = w.pos;
    return true;
}

bool daos_stripe_map_decode(const uint8_t *buf, size_t len, uint32_t *sc, uint32_t *su,
                            uint32_t *mc, struct mds_ds_map_entry **entries)
{
    struct rcur r = { buf, len, 0, false };
    struct mds_ds_map_entry *ents;
    uint32_t v_sc;
    uint32_t v_su;
    uint32_t v_mc;
    uint32_t v_n;
    uint32_t i;

    if (buf == NULL || sc == NULL || su == NULL || mc == NULL || entries == NULL ||
        len < DAOS_STRIPE_MAP_ENC_FIXED || len > DAOS_STRIPE_MAP_ENC_MAX) {
        return false;
    }
    *entries = NULL;
    if (r_u8(&r) != DAOS_STRIPE_MAP_VERSION) {
        return false;
    }
    v_sc = r_u32(&r);
    v_su = r_u32(&r);
    v_mc = r_u32(&r);
    v_n = r_u32(&r);
    if (r.fail || v_su == 0 || !stripe_geometry_ok(v_sc, v_mc) || v_n != v_sc * v_mc) {
        return false;
    }
    ents = calloc(v_n, sizeof(*ents));
    if (ents == NULL) {
        return false;
    }
    for (i = 0; i < v_n; i++) {
        uint16_t elen = r_u16(&r);

        if (r.fail || elen < FDB_STRIPE_ENT_ENC_FIXED || elen > FDB_STRIPE_ENT_ENC_MAX ||
            elen > r.len - r.pos ||
            !fdb_stripe_ent_decode(r.buf + r.pos, elen, &ents[i])) {
            free(ents);
            return false;
        }
        r.pos += elen;
    }
    if (!r_done(&r)) {
        free(ents);
        return false;
    }
    *sc = v_sc;
    *su = v_su;
    *mc = v_mc;
    *entries = ents;
    return true;
}

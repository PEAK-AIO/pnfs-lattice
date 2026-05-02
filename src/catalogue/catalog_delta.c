/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalog_delta.c — Semantic delta builder + wire serialization.
 */

#include <stdlib.h>
#include <string.h>

#include "catalog_delta.h"
#include "endian_helpers.h"

/* -----------------------------------------------------------------------
 * Builder internals
 * ----------------------------------------------------------------------- */

#define BUILDER_INIT_CAP  4096

struct catalog_delta_builder {
    uint8_t  *buf;       /**< Serialized output buffer. */
    size_t    len;       /**< Current used length. */
    size_t    cap;       /**< Allocated capacity. */
    uint32_t  count;     /**< Number of records appended. */
};

static int builder_ensure(struct catalog_delta_builder *b, size_t need)
{
    size_t new_cap;
    uint8_t *new_buf;

    if (b->len + need <= b->cap) {
        return 0;
    }
    new_cap = (b->cap == 0) ? BUILDER_INIT_CAP : b->cap;
    while (new_cap < b->len + need) {
        new_cap *= 2;
    }
    new_buf = realloc(b->buf, new_cap);
    if (new_buf == NULL) {
        return -1;
    }
    b->buf = new_buf;
    b->cap = new_cap;
    return 0;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int catalog_delta_builder_create(struct catalog_delta_builder **out)
{
    struct catalog_delta_builder *b;

    if (out == NULL) {
        return -1;
    }
    b = calloc(1, sizeof(*b));
    if (b == NULL) {
        return -1;
    }
    *out = b;
    return 0;
}

int catalog_delta_builder_append(struct catalog_delta_builder *b,
                                 const struct catalog_delta_record *rec)
{
    size_t need;
    uint8_t *p;

    if (b == NULL || rec == NULL) {
        return -1;
    }
    if (rec->payload_len > 0 && rec->payload == NULL) {
        return -1;
    }

    need = CATALOG_DELTA_HDR_SIZE + rec->payload_len;
    if (builder_ensure(b, need) != 0) {
        return -1;
    }

    p = b->buf + b->len;

    /* type (1) */
    p[0] = rec->type;
    p += 1;

    /* stream_id (4 BE) */
    fdb_put_u32(p, rec->stream_id);
    p += 4;

    /* seqno (8 BE) */
    fdb_put_u64(p, rec->seqno);
    p += 8;

    /* timestamp_ns (8 BE) */
    fdb_put_u64(p, rec->timestamp_ns);
    p += 8;

    /* payload_len (4 BE) */
    fdb_put_u32(p, rec->payload_len);
    p += 4;

    /* payload */
    if (rec->payload_len > 0) {
        memcpy(p, rec->payload, rec->payload_len);
    }

    b->len += need;
    b->count++;
    return 0;
}

uint32_t catalog_delta_builder_count(
    const struct catalog_delta_builder *b)
{
    if (b == NULL) {
        return 0;
    }
    return b->count;
}

int catalog_delta_builder_serialize(const struct catalog_delta_builder *b,
                                    const void **buf, size_t *len)
{
    if (b == NULL || buf == NULL || len == NULL) {
        return -1;
    }
    *buf = b->buf;
    *len = b->len;
    return 0;
}

void catalog_delta_builder_reset(struct catalog_delta_builder *b)
{
    if (b == NULL) {
        return;
    }
    b->len = 0;
    b->count = 0;
    /* Keep allocation for reuse. */
}

void catalog_delta_builder_destroy(struct catalog_delta_builder *b)
{
    if (b == NULL) {
        return;
    }
    free(b->buf);
    free(b);
}

/* -----------------------------------------------------------------------
 * Deserialization
 * ----------------------------------------------------------------------- */

int catalog_delta_deserialize(const void *buf, size_t len,
                              catalog_delta_record_cb cb, void *ctx)
{
    const uint8_t *p = buf;
    const uint8_t *end;
    struct catalog_delta_record rec;
    int stop = 0;

    if (buf == NULL || cb == NULL) {
        return -1;
    }

    end = p + len;
    while (p + CATALOG_DELTA_HDR_SIZE <= end) {
        rec.type = p[0];
        p += 1;

        rec.stream_id = fdb_get_u32(p);
        p += 4;

        rec.seqno = fdb_get_u64(p);
        p += 8;

        rec.timestamp_ns = fdb_get_u64(p);
        p += 8;

        rec.payload_len = fdb_get_u32(p);
        p += 4;

        if (p + rec.payload_len > end) {
            return -1;  /* truncated payload */
        }

        rec.payload = (rec.payload_len > 0) ? p : NULL;
        p += rec.payload_len;

        if (cb(&rec, ctx) != 0) {
            stop = 1;
            break;
        }
    }
    if (!stop && p != end) {
        return -1;  /* trailing bytes after last complete record */
    }

    return 0;
}

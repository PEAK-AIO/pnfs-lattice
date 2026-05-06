/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * endian_helpers.h -- Big-endian wire-format serialization helpers.
 *
 * Portable inline put/get for u8, u32, u64, i64 in big-endian byte
 * order.  Used by RonDB inode/stripe serialization.
 */

#ifndef ENDIAN_HELPERS_H
#define ENDIAN_HELPERS_H

#include <stdint.h>

static inline void fdb_put_u8(uint8_t *dst, uint8_t v)
{
    dst[0] = v;
}

static inline uint8_t fdb_get_u8(const uint8_t *src)
{
    return src[0];
}

static inline void fdb_put_u32(uint8_t *dst, uint32_t v)
{
    dst[0] = (uint8_t)(v >> 24);
    dst[1] = (uint8_t)(v >> 16);
    dst[2] = (uint8_t)(v >> 8);
    dst[3] = (uint8_t)(v);
}

static inline uint32_t fdb_get_u32(const uint8_t *src)
{
    return ((uint32_t)src[0] << 24) |
           ((uint32_t)src[1] << 16) |
           ((uint32_t)src[2] << 8) |
           ((uint32_t)src[3]);
}

static inline void fdb_put_u64(uint8_t *dst, uint64_t v)
{
    dst[0] = (uint8_t)(v >> 56);
    dst[1] = (uint8_t)(v >> 48);
    dst[2] = (uint8_t)(v >> 40);
    dst[3] = (uint8_t)(v >> 32);
    dst[4] = (uint8_t)(v >> 24);
    dst[5] = (uint8_t)(v >> 16);
    dst[6] = (uint8_t)(v >> 8);
    dst[7] = (uint8_t)(v);
}

static inline uint64_t fdb_get_u64(const uint8_t *src)
{
    return ((uint64_t)src[0] << 56) |
           ((uint64_t)src[1] << 48) |
           ((uint64_t)src[2] << 40) |
           ((uint64_t)src[3] << 32) |
           ((uint64_t)src[4] << 24) |
           ((uint64_t)src[5] << 16) |
           ((uint64_t)src[6] << 8) |
           ((uint64_t)src[7]);
}

static inline void fdb_put_i64(uint8_t *dst, int64_t v)
{
    fdb_put_u64(dst, (uint64_t)v);
}

static inline int64_t fdb_get_i64(const uint8_t *src)
{
    return (int64_t)fdb_get_u64(src);
}

#endif /* ENDIAN_HELPERS_H */

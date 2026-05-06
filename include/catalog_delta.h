/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalog_delta.h -- Semantic replay record types + delta builder.
 *
 * Each catalog mutation emits a typed delta record that captures the
 * semantic intent (INODE_UPSERT, DIRENT_PUT, etc.) rather than the
 * backend-specific storage diff (NDB row ops).
 *
 * Records are serialized to a compact binary wire format for durable
 * journaling and optional file logging.  The journal is the rebuild
 * source for the materialized catalog image (Phase 6).
 *
 * Identity: (stream_id, seqno).  Single-MDS uses stream_id=0.
 * Forward-compatible with partitioned ownership (Phase 9).
 */

#ifndef CATALOG_DELTA_H
#define CATALOG_DELTA_H

#include <stdint.h>
#include <stddef.h>

/* -----------------------------------------------------------------------
 * Semantic record types
 *
 * One type per durable catalog mutation class.  The set covers every
 * state family listed in mds_catalogue.h + mds_coordination.h.
 * ----------------------------------------------------------------------- */

enum catalog_delta_type {
    /* Namespace */
    CAT_DELTA_INODE_UPSERT            = 1,
    CAT_DELTA_INODE_DELETE            = 2,
    CAT_DELTA_DIRENT_PUT              = 3,
    CAT_DELTA_DIRENT_DELETE           = 4,

    /* Stripe maps */
    CAT_DELTA_STRIPE_MAP_PUT          = 5,
    CAT_DELTA_STRIPE_MAP_DELETE       = 6,

    /* Extended attributes */
    CAT_DELTA_XATTR_PUT               = 7,
    CAT_DELTA_XATTR_DELETE            = 8,

    /* DS registry */
    CAT_DELTA_DS_REGISTRY_PUT         = 9,
    CAT_DELTA_DS_REGISTRY_DELETE      = 10,

    /* DS provisioning */
    CAT_DELTA_DS_PROVISION_PUT        = 11,
    CAT_DELTA_DS_PROVISION_DELETE     = 12,

    /* Quota */
    CAT_DELTA_QUOTA_RULE_PUT          = 13,
    CAT_DELTA_QUOTA_USAGE_PUT         = 14,

    /* GC queue */
    CAT_DELTA_GC_ENQUEUE             = 15,
    CAT_DELTA_GC_DEQUEUE             = 16,

    /* Layout coordination */
    CAT_DELTA_LAYOUT_GRANT           = 17,
    CAT_DELTA_LAYOUT_RETURN          = 18,

    /* Client recovery */
    CAT_DELTA_CLIENT_RECOVERY_PUT    = 19,
    CAT_DELTA_CLIENT_RECOVERY_DELETE = 20,

    /* Rename journal */
    CAT_DELTA_RENAME_JOURNAL         = 21,

    /* Partition/ownership */
    CAT_DELTA_PARTITION_MAP_UPDATE   = 22,
};

/* -----------------------------------------------------------------------
 * Wire format
 *
 * Each serialized record:
 *   type          (1 byte)
 *   stream_id     (4 bytes, big-endian)
 *   seqno         (8 bytes, big-endian)
 *   timestamp_ns  (8 bytes, big-endian)
 *   payload_len   (4 bytes, big-endian)
 *   payload       (payload_len bytes, opaque)
 *
 * Total header: 25 bytes.
 * ----------------------------------------------------------------------- */

#define CATALOG_DELTA_HDR_SIZE  25

/* -----------------------------------------------------------------------
 * Delta record (in-memory representation)
 * ----------------------------------------------------------------------- */

struct catalog_delta_record {
    uint8_t     type;          /**< enum catalog_delta_type */
    uint32_t    stream_id;     /**< 0 = single-MDS default */
    uint64_t    seqno;         /**< Monotonic within stream */
    uint64_t    timestamp_ns;  /**< CLOCK_REALTIME nanoseconds */
    uint32_t    payload_len;   /**< Length of payload data */
    const void *payload;       /**< Opaque payload (borrowed, not owned) */
};

/* -----------------------------------------------------------------------
 * Delta builder
 *
 * Accumulates records during a logical write transaction.
 * Serializes to a contiguous buffer for journal write.
 * ----------------------------------------------------------------------- */

struct catalog_delta_builder;

/**
 * Create a new delta builder.
 * @param out  Receives the builder handle.
 * @return 0 on success, -1 on allocation failure.
 */
int catalog_delta_builder_create(struct catalog_delta_builder **out);

/**
 * Append a record to the builder.
 *
 * The payload is copied into the builder's internal buffer.
 *
 * @param b     Builder handle.
 * @param rec   Record to append (payload is copied).
 * @return 0 on success, -1 on error.
 */
int catalog_delta_builder_append(struct catalog_delta_builder *b,
                                 const struct catalog_delta_record *rec);

/**
 * Return the number of records accumulated so far.
 */
uint32_t catalog_delta_builder_count(
    const struct catalog_delta_builder *b);

/**
 * Serialize all accumulated records to a contiguous buffer.
 *
 * The returned buffer is owned by the builder and valid until
 * the next reset or destroy.
 *
 * @param b       Builder handle.
 * @param buf     Receives pointer to serialized data.
 * @param len     Receives total length in bytes.
 * @return 0 on success, -1 on error.
 */
int catalog_delta_builder_serialize(const struct catalog_delta_builder *b,
                                    const void **buf, size_t *len);

/**
 * Reset the builder for the next transaction (keeps allocation).
 */
void catalog_delta_builder_reset(struct catalog_delta_builder *b);

/**
 * Destroy the builder and free all resources.
 */
void catalog_delta_builder_destroy(struct catalog_delta_builder *b);

/* -----------------------------------------------------------------------
 * Deserialization
 * ----------------------------------------------------------------------- */

/**
 * Callback invoked for each deserialized record.
 * Return 0 to continue, non-zero to stop.
 */
typedef int (*catalog_delta_record_cb)(
    const struct catalog_delta_record *rec, void *ctx);

/**
 * Deserialize a buffer of concatenated records.
 *
 * Calls @a cb for each record found.  The record's payload pointer
 * points into @a buf (not copied).
 *
 * @param buf   Serialized data.
 * @param len   Length of data.
 * @param cb    Per-record callback.
 * @param ctx   Callback context.
 * @return 0 on success, -1 on parse error.
 */
int catalog_delta_deserialize(const void *buf, size_t len,
                              catalog_delta_record_cb cb, void *ctx);

#endif /* CATALOG_DELTA_H */

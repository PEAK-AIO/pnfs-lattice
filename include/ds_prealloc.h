/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_prealloc.h -- DS placement pre-allocation for multi-MDS mode.
 *
 * In multi-MDS deployments, regular files must be DS-backed from
 * creation (no inline catalogue storage) so that any MDS can serve
 * reads/writes via the shared DS mounts.
 *
 * The pre-alloc pool runs placement_select() ahead of time and
 * caches the results.  On file CREATE, a cached placement is
 * popped from the pool (fast path) or computed synchronously
 * (slow path when pool is empty).
 */

#ifndef DS_PREALLOC_H
#define DS_PREALLOC_H

#include <stdint.h>
#include <stdbool.h>
#include "pnfs_mds.h"

struct mds_catalogue;
struct mds_proxy_ctx;
struct mds_ds_map_entry;
struct ds_cache;

/** Opaque pre-allocation context. */
struct ds_prealloc_ctx;

/**
 * @brief Initialise the DS pre-allocation pool with default RR policy.
 *
 * Convenience wrapper around ds_prealloc_init_ex() that pins the
 * placement policy to PLACEMENT_RR.  Existing callers (notably the
 * unit-test fixtures) that do not need policy-aware selection should
 * keep using this entrypoint.
 *
 * @param cat         Catalogue handle (for DS registry reads).
 * @param proxy       Proxy context (for DS file creation).
 * @param pool_size   Number of placements to cache (0 = default 128).
 * @param[out] out    Receives the pool context.
 * @return 0 on success, -1 on error.
 */
int ds_prealloc_init(const struct mds_catalogue *cat,
                     struct mds_proxy_ctx *proxy,
                     uint32_t pool_size,
                     struct ds_prealloc_ctx **out);

/**
 * @brief Initialise the DS pre-allocation pool with a chosen placement
 *        policy.
 *
 * The pool consults @a policy whenever ds_prealloc_pop() needs to
 * choose a target DS, so that fresh-CREATE placement is policy-coherent
 * with placement_select_ex() used elsewhere in the daemon.  Callers
 * that do not need policy steering should use ds_prealloc_init().
 *
 * @param cat         Catalogue handle (for DS registry reads).
 * @param proxy       Proxy context (for DS file creation).
 * @param policy      Placement policy applied on every pop:
 *                    PLACEMENT_RR / PLACEMENT_WEIGHTED_RR /
 *                    PLACEMENT_CAPACITY.
 * @param pool_size   Number of placements to cache (0 = default 128).
 * @param[out] out    Receives the pool context.
 * @return 0 on success, -1 on error.
 */
int ds_prealloc_init_ex(const struct mds_catalogue *cat,
                        struct mds_proxy_ctx *proxy,
                        enum mds_placement_policy policy,
                        uint32_t pool_size,
                        struct ds_prealloc_ctx **out);

/**
 * @brief Pop a pre-computed placement from the pool.
 *
 * Fast path: returns a cached placement (no catalogue I/O).
 * Slow path: if pool is empty, computes placement synchronously.
 *
 * @param ctx          Pool context.
 * @param[out] entry   Receives the placement (ds_id populated).
 * @param[out] stripe_unit  Receives the stripe unit size.
 * @return 0 on success, -1 on error (no DSes available).
 */
int ds_prealloc_pop(struct ds_prealloc_ctx *ctx,
                    struct mds_ds_map_entry *entry,
                    uint32_t *stripe_unit,
                    uint64_t *fileid_out);

/**
 * @brief Select any ONLINE DS from the prealloc context's registry DB.
 *
 * Used as a fallback when the patched-only prealloc pool is empty.
 * This preserves the correct root/global DS registry view even for
 * child-shard creates that use a different namespace DB.
 *
 * @param ctx          Pool context.
 * @param[out] entry   Receives the selected DS mapping entry.
 * @param[out] stripe_unit Receives the stripe unit size.
 * @return MDS_OK on success, MDS_ERR_NOSPC if no ONLINE DS exists,
 *         or another mds_status on catalogue/validation failure.
 */
enum mds_status ds_prealloc_select_any_online(
    const struct ds_prealloc_ctx *ctx,
    struct mds_ds_map_entry *entry,
    uint32_t *stripe_unit);

/**
 * @brief Ensure a DS file exists for the given fileid + placement.
 *
 * Calls mds_proxy_ensure_ds_file() with the appropriate parameters.
 * Idempotent -- safe to call if the file already exists.
 *
 * @param ctx     Pool context (provides proxy handle).
 * @param ds_id   Target data server.
 * @param fileid  MDS file ID.
 * @return 0 on success, -1 on error.
 */
int ds_prealloc_ensure(const struct ds_prealloc_ctx *ctx,
                       uint32_t ds_id, uint64_t fileid);

/**
 * @brief Peek at the next pre-computed placement without consuming it.
 *
 * Non-destructive: the entry remains in the pool for the subsequent pop.
 * Used by the Phase 3 fused CREATE+LAYOUTGET to capture the DS ID
 * before the pop happens inside ns_create.
 *
 * @param ctx          Pool context.
 * @param[out] entry   Receives the placement (ds_id populated).
 * @param[out] stripe_unit  Receives the stripe unit size.
 * @return 0 on success, -1 if pool is empty.
 */
int ds_prealloc_peek(const struct ds_prealloc_ctx *ctx,
                     struct mds_ds_map_entry *entry,
                     uint32_t *stripe_unit);

/**
 * @brief Attach (or detach) the in-memory DS cache used to overlay
 *        live capacity and operator weights onto the pop snapshot.
 *
 * Without an attached cache the snapshot is built only from the
 * catalogue's persisted DS registry, which on third-party DSes is
 * typically zeroed for `total_bytes` / `used_bytes` and unset for
 * `weight`.  That collapses WEIGHTED_RR / CAPACITY policies to plain
 * RR for every prealloc-backed CREATE.  Wiring the cache in lets the
 * statvfs() probe and operator `ds set-weight` overrides reach the
 * pop hot path on the next refill tick.
 *
 * Safe to call at any time after ds_prealloc_init() and before
 * ds_prealloc_destroy().  Pass @a cache == NULL to detach (e.g.
 * during shutdown ordering).  The pointer is borrowed; the caller
 * owns the cache and must keep it alive until the prealloc context
 * is destroyed or detached.
 *
 * @param ctx    Prealloc context (NULL-safe).
 * @param cache  DS cache handle (NULL to detach).
 */
void ds_prealloc_set_ds_cache(struct ds_prealloc_ctx *ctx,
                              const struct ds_cache *cache);

/**
 * @brief Destroy the pool and free resources.
 */
void ds_prealloc_destroy(struct ds_prealloc_ctx *ctx);

/* -----------------------------------------------------------------------
 * Phase C of docs/hpc-nto1-plan.md -- wide pre-warm batch.
 *
 * Allocates ONE fileid and synchronously creates + FH-captures every
 * (stripe, mirror) DS file in parallel.  All-or-nothing semantics:
 * on any placement or FH-capture failure the helper rolls back any
 * DS-side files it created (best-effort GC enqueue) and returns an
 * error so the CREATE caller can surface NFS4ERR_NOSPC to the
 * client.  The caller never sees a half-built batch.
 *
 * The single-pop API (ds_prealloc_pop) is unchanged; this is a
 * strictly additive surface that bypasses the per-DS pre-fill rings
 * (which carry one fileid per slot) and runs placement +
 * FH-capture synchronously per call.  See ds_prealloc.c for the
 * design notes.
 *
 * Lifetime:
 *  - On MDS_OK, @ref out->entries is heap-allocated and CALLER OWNS
 *    it.  Free via ds_prealloc_batch_result_destroy() once the
 *    catalogue persistence step (the wide fused CREATE) is either
 *    committed or rolled back.
 *  - On any error code, @ref out->entries is NULL and any DS-side
 *    state has been GC-enqueued; the caller has nothing left to
 *    clean up except its own inode-row state.
 * ----------------------------------------------------------------------- */

struct ds_prealloc_batch_request {
    uint32_t  stripe_count;            /**< 1..MDS_MAX_STRIPES. */
    uint32_t  mirror_count;            /**< 1..MDS_MAX_MIRRORS, default 1. */
    uint32_t  stripe_unit;             /**< Bytes; 0 = ctx default 65536. */
    uint8_t   required_mode;           /**< DS_MODE_GENERIC. */
    uint8_t   required_transport;      /**< DS_TRANSPORT_TCP, etc. */
    uint8_t   preferred_transport;     /**< 0 = no preference. */
    uint32_t  preferred_caps;          /**< DS_CAP_GPUDIRECT, etc. */
    bool      strict_unique_ds;        /**< true: every stripe on a
                                        *  distinct DS, fail with
                                        *  MDS_ERR_NOSPC if the
                                        *  online pool is too small. */
    /**
     * Optional caller-supplied fileid.  When non-zero, the batch
     * uses this fileid instead of allocating one internally.  Used
     * by the OPEN(CREATE) HPC integration (Phase C / Step 5) where
     * the catalogue's ns_create has already allocated and persisted
     * an inode and the wide pre-warm needs to use that same fileid
     * to capture per-(stripe, mirror) DS file handles.  Zero (the
     * default) preserves the original "allocate-then-capture"
     * behaviour and stays compatible with all existing callers.
     */
    uint64_t  fileid_hint;
};

struct ds_prealloc_batch_result {
    uint64_t                  fileid;
    uint32_t                  stripe_count;
    uint32_t                  mirror_count;
    uint32_t                  stripe_unit;
    /** Heap-allocated, sized stripe_count * mirror_count.  CALLER
     *  OWNS after MDS_OK.  Released by ds_prealloc_batch_result_destroy.
     *  NULL on any error. */
    struct mds_ds_map_entry  *entries;
};

/**
 * @brief Run a wide pre-warm batch.
 *
 * Allocates one fileid, picks N DSes via ds_filter_compatible_preferred()
 * (and the internal plan cache so back-to-back CREATEs into the same
 * HPC profile reuse the placement decision), then issues parallel
 * mds_proxy_ensure_ds_file_fh() calls bounded by an internal worker
 * pool.  Each FH capture is retried up to 3 times with exponential
 * backoff (50/200/800 ms) before the batch aborts.
 *
 * @param ctx Pre-existing prealloc context (provides catalogue +
 *            proxy handles + ONLINE-DS snapshot).
 * @param req Geometry + filter request.
 * @param out Receives the populated result.  On error the function
 *            zeroes the struct and frees any internal state.
 * @return MDS_OK on full success; MDS_ERR_NOSPC if placement or any
 *         FH capture exhausted retries; MDS_ERR_INVAL on bad args;
 *         MDS_ERR_NOMEM on allocation failure.
 */
enum mds_status ds_prealloc_batch(
    struct ds_prealloc_ctx *ctx,
    const struct ds_prealloc_batch_request *req,
    struct ds_prealloc_batch_result *out);

/**
 * @brief Release the heap state owned by a batch result.
 *
 * Idempotent and NULL-safe.  Does not touch the catalogue.  After
 * the call the result is zeroed so the caller can safely re-use the
 * stack value for a subsequent batch.
 */
void ds_prealloc_batch_result_destroy(
    struct ds_prealloc_batch_result *res);

/**
 * @brief Test-only: emit deterministic synthetic DS file handles
 *        when no proxy is attached.
 *
 * Benchmarks and integration tests that run without a live DS
 * mount would otherwise see nfs_fh_len == 0 on every prealloc
 * entry, which routes every LAYOUTGET down the NFS4ERR_DELAY /
 * revoke path and invalidates the fused-vs-unfused measurement.
 * Enabling synthetic FH generation makes each produced entry
 * carry a unique 16-byte handle derived from the fileid, so
 * LAYOUTGET returns NFS4_OK with a valid-shaped layout.
 *
 * The synthetic handle is not a real NFS4 file handle and must
 * not be used for actual DS I/O.  The helper is therefore a
 * no-op on contexts that already have a real proxy attached.
 *
 * Safe to call only before the pool has been primed (immediately
 * after ds_prealloc_init).
 */
void ds_prealloc_test_enable_synthetic_fh(struct ds_prealloc_ctx *ctx,
                                          bool enabled);

#endif /* DS_PREALLOC_H */

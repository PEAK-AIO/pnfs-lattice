/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_prealloc.c -- ring-buffered DS pre-creation pool (ENABLE_DS_PREALLOC=ON).
 *
 * Design: docs/prealloc-gc-design.md S4.
 *
 * Goal: a CREATE burst sees a ~100% fast-path hit by keeping rings of
 * already-created DS stub files (0-byte files with a captured NFS FH and
 * a reserved fileid) ready to hand out.  On pop() the OPEN(CREATE) path
 * pays no DS round-trip: it takes a ready (fileid, ds_id, nfs_fh) slot
 * and ns_create persists it as the inode's stripe-map row.
 *
 * Structure:
 *   - M independent rings (cfg.prealloc_ring_count), each with its own
 *     lock + refill worker thread, so producers and consumers do not
 *     contend across rings.
 *   - Each ring owns a disjoint subset of the ONLINE DSes.  In multi-MDS
 *     mode the DS list is first partitioned by mds_id (DS p -> MDS
 *     p % cluster_size) so two MDSes never pre-create on the same DS
 *     slot space; the per-MDS share is then spread across that MDS's
 *     rings (DS -> ring (p / cluster_size) % ring_count).
 *   - Refill workers start at daemon boot and keep their ring full.
 *   - pop() round-robins rings for an O(1) take; if every ring is empty
 *     it falls back to a synchronous select+create (correctness first),
 *     and signals the refill workers.
 *
 * Persistence (best-effort): every produced slot is also written to the
 * catalogue mds_prealloc_pool table (mds_cat_prealloc_pool_insert) and
 * removed on consume (mds_cat_prealloc_pool_delete).  At init the pool
 * is scanned and any row whose fileid is not yet a live inode is
 * recovered into a ring instead of re-created, so precreated DS files
 * are not orphaned across a restart.  Backends that do not implement the
 * pool ops degrade cleanly to in-memory-only rings.
 */
#include "ds_prealloc.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdatomic.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <stdio.h>

#include "mds_catalogue.h"
#include "ds_cache.h"
#include "placement.h"
#include "proxy_io.h"
#include "mds_metrics.h"
#include "pnfs_mds.h"

#define DS_PREALLOC_DEFAULT_STRIPE_UNIT  ((uint32_t)(64U * 1024U))
#define DS_PREALLOC_DEFAULT_RING_COUNT   4U
#define DS_PREALLOC_MAX_RING_COUNT       64U
#define DS_PREALLOC_MIN_RING_DEPTH       8U
#define DS_PREALLOC_REFILL_BACKOFF_NS    (20U * 1000U * 1000U) /* 20 ms */
#define DS_PREALLOC_PLAN_CACHE           8U

/*
 * Cached wide-batch placement decision.  Back-to-back CREATEs into the
 * same HPC profile reuse the same (ordered) DS spread so a striped file
 * family lands consistently, instead of being re-shuffled by the RR
 * cursor on every call.  Keyed by the request geometry/filter and
 * invalidated when the ONLINE DS count changes.
 */
struct prealloc_plan {
    bool      valid;
    uint32_t  stripe_count;
    uint32_t  mirror_count;
    uint8_t   required_mode;
    uint8_t   required_transport;
    uint8_t   preferred_transport;
    uint32_t  preferred_caps;
    bool      strict_unique_ds;
    uint32_t  ds_count_seen;
    uint32_t  n;
    uint32_t *ds_ids;          /* malloc'd, length n */
};

struct prealloc_slot {
    uint64_t                fileid;
    uint32_t                stripe_unit;
    struct mds_ds_map_entry entry;     /* ds_id + nfs_fh(_len) */
};

struct ds_prealloc_ctx;

struct prealloc_ring {
    struct ds_prealloc_ctx *ctx;       /* back-pointer (borrowed) */
    uint32_t                index;

    pthread_mutex_t         lock;
    pthread_cond_t          not_full;   /* refill waits when full */
    struct prealloc_slot   *slots;
    uint32_t                cap;
    uint32_t                head;
    uint32_t                tail;
    uint32_t                count;

    uint32_t                ds_rr;      /* RR cursor within the subset */
    pthread_t               refill;
    bool                    refill_started;
};

struct ds_prealloc_ctx {
    const struct mds_catalogue *cat;
    struct mds_proxy_ctx       *proxy;
    enum mds_placement_policy   policy;
    const struct ds_cache      *cache;
    uint32_t                    stripe_unit;

    uint32_t                    self_mds_id;   /* 0 = single-MDS */
    uint32_t                    cluster_size;  /* >=1 */

    struct prealloc_ring       *rings;
    uint32_t                    ring_count;
    _Atomic uint32_t            pop_rr;

    pthread_mutex_t             plan_lock;
    struct prealloc_plan        plans[DS_PREALLOC_PLAN_CACHE];
    uint32_t                    plan_rr;

    _Atomic bool                stop;
    bool                        synthetic_fh;
};

/* -----------------------------------------------------------------------
 * Small helpers
 * ----------------------------------------------------------------------- */

static void mfetch_add(_Atomic uint64_t *c, uint64_t v)
{
    atomic_fetch_add_explicit(c, v, memory_order_relaxed);
}

/* Deterministic synthetic FH for test fixtures with no live proxy. */
static void synth_fh(struct mds_ds_map_entry *e, uint64_t fileid)
{
    memset(e->nfs_fh, 0, sizeof(e->nfs_fh));
    e->nfs_fh_len = 16;
    for (uint32_t i = 0; i < 8; i++) {
        e->nfs_fh[i]     = (uint8_t)(fileid >> (8 * i));
        e->nfs_fh[8 + i] = (uint8_t)(e->ds_id >> ((8 * i) & 24));
    }
}

/*
 * Does ONLINE-DS at sorted position @p belong to (this MDS, this ring)?
 * DS -> MDS:  p % cluster_size.   per-MDS share -> ring: (p/cluster_size)
 * % ring_count.  self_mds_id 0 collapses to "this is the only MDS".
 */
static bool ds_belongs_to_ring(const struct ds_prealloc_ctx *ctx,
                               uint32_t ring_index, uint32_t p)
{
    uint32_t csz = (ctx->cluster_size >= 1U) ? ctx->cluster_size : 1U;
    uint32_t myslot = (ctx->self_mds_id >= 1U) ? (ctx->self_mds_id - 1U) : 0U;

    if ((p % csz) != (myslot % csz)) {
        return false;
    }
    return (((p / csz) % ctx->ring_count) == ring_index);
}

/*
 * Snapshot ONLINE DSes assigned to @ring, overlay live weights, and run
 * the placement policy to choose one ds_id.  Returns MDS_OK with
 * entry->ds_id set (FH zeroed), MDS_ERR_NOSPC if this ring owns no
 * ONLINE DS right now.
 */
static enum mds_status ring_select_ds(struct ds_prealloc_ctx *ctx,
                                      struct prealloc_ring *ring,
                                      struct mds_ds_map_entry *entry)
{
    struct mds_ds_info *ds_list = NULL;
    struct mds_ds_info *mine = NULL;
    uint32_t ds_count = 0, mine_count = 0;
    enum mds_status st;

    memset(entry, 0, sizeof(*entry));

    st = mds_cat_ds_list((struct mds_catalogue *)ctx->cat, &ds_list,
                         &ds_count);
    if (st != MDS_OK) {
        return st;
    }
    if (ds_list == NULL || ds_count == 0) {
        free(ds_list);
        return MDS_ERR_NOSPC;
    }

    mine = calloc(ds_count, sizeof(*mine));
    if (mine == NULL) {
        free(ds_list);
        return MDS_ERR_NOMEM;
    }
    for (uint32_t p = 0; p < ds_count; p++) {
        if (ds_belongs_to_ring(ctx, ring->index, p)) {
            mine[mine_count++] = ds_list[p];
        }
    }
    free(ds_list);

    if (mine_count == 0) {
        free(mine);
        return MDS_ERR_NOSPC;
    }
    if (ctx->cache != NULL) {
        ds_cache_overlay_weights(ctx->cache, mine, mine_count);
    }
    st = placement_select_ex(ctx->policy, mine, mine_count, 1, 1,
                             ctx->stripe_unit, entry);
    free(mine);
    if (st != MDS_OK) {
        memset(entry, 0, sizeof(*entry));
    }
    return st;
}

/*
 * Produce ONE ready slot: pick a DS, reserve a fileid, pre-create the
 * 0-byte DS file + capture its FH, and (best-effort) persist the pool
 * row.  Returns 0 on success.
 */
static int produce_slot(struct ds_prealloc_ctx *ctx,
                        struct prealloc_ring *ring,
                        struct prealloc_slot *slot)
{
    struct mds_ds_map_entry entry;
    uint64_t fileid = 0;
    enum mds_status st;

    if (ring_select_ds(ctx, ring, &entry) != MDS_OK) {
        return -1;
    }
    st = mds_cat_alloc_fileid((struct mds_catalogue *)ctx->cat, NULL,
                              &fileid);
    if (st != MDS_OK || fileid == 0) {
        return -1;
    }

    if (ctx->proxy != NULL) {
        uint32_t fh_len = (uint32_t)sizeof(entry.nfs_fh);
        st = mds_proxy_ensure_ds_file_fh(ctx->proxy, entry.ds_id, fileid,
                                         0, 0, entry.nfs_fh, &fh_len);
        if (st == MDS_OK && fh_len > 0 && fh_len <= sizeof(entry.nfs_fh)) {
            entry.nfs_fh_len = fh_len;
        } else if (ctx->synthetic_fh) {
            synth_fh(&entry, fileid);
        } else {
            /* DS create/FH-capture failed -- do not publish an
             * unusable slot; the fileid is simply abandoned. */
            return -1;
        }
    } else if (ctx->synthetic_fh) {
        synth_fh(&entry, fileid);
    }

    memset(slot, 0, sizeof(*slot));
    slot->fileid      = fileid;
    slot->stripe_unit = ctx->stripe_unit;
    slot->entry       = entry;

    /* Best-effort persistence (ignored when the backend lacks pool ops). */
    (void)mds_cat_prealloc_pool_insert(
        (struct mds_catalogue *)ctx->cat, fileid, entry.ds_id,
        entry.nfs_fh, entry.nfs_fh_len, ctx->self_mds_id,
        ctx->stripe_unit);
    return 0;
}

/* Push a ready slot into the ring (single-producer; never overflows
 * because the caller only produces what fits). */
static void ring_push(struct prealloc_ring *ring,
                      const struct prealloc_slot *slot)
{
    pthread_mutex_lock(&ring->lock);
    if (ring->count < ring->cap) {
        ring->slots[ring->tail] = *slot;
        ring->tail = (ring->tail + 1U) % ring->cap;
        ring->count++;
        mfetch_add(&g_branch_metrics.prealloc_refill_entries, 1);
    }
    pthread_mutex_unlock(&ring->lock);
}

static void *refill_main(void *arg)
{
    struct prealloc_ring *ring = arg;
    struct ds_prealloc_ctx *ctx = ring->ctx;

    while (!atomic_load_explicit(&ctx->stop, memory_order_relaxed)) {
        uint32_t want;

        pthread_mutex_lock(&ring->lock);
        while (ring->count >= ring->cap &&
               !atomic_load_explicit(&ctx->stop, memory_order_relaxed)) {
            pthread_cond_wait(&ring->not_full, &ring->lock);
        }
        want = (ring->count < ring->cap) ? (ring->cap - ring->count) : 0U;
        pthread_mutex_unlock(&ring->lock);

        if (atomic_load_explicit(&ctx->stop, memory_order_relaxed)) {
            break;
        }
        if (want == 0U) {
            continue;
        }

        mfetch_add(&g_branch_metrics.prealloc_refill_batches, 1);
        for (uint32_t i = 0;
             i < want &&
             !atomic_load_explicit(&ctx->stop, memory_order_relaxed);
             i++) {
            struct prealloc_slot slot;
            if (produce_slot(ctx, ring, &slot) == 0) {
                ring_push(ring, &slot);
            } else {
                /* No DS available / transient error: back off so we
                 * do not spin, then re-evaluate. */
                struct timespec ts = {
                    .tv_sec = 0,
                    .tv_nsec = DS_PREALLOC_REFILL_BACKOFF_NS,
                };
                nanosleep(&ts, NULL);
                break;
            }
        }
    }
    return NULL;
}

/* -----------------------------------------------------------------------
 * Synchronous fallback (rings empty) -- the community-stub behaviour.
 * ----------------------------------------------------------------------- */

static int sync_pop(struct ds_prealloc_ctx *ctx,
                    struct mds_ds_map_entry *entry,
                    uint32_t *stripe_unit,
                    uint64_t *fileid_out)
{
    struct mds_ds_info *ds_list = NULL;
    uint32_t ds_count = 0;
    uint64_t fileid = 0;
    enum mds_status st;

    if (stripe_unit != NULL) { *stripe_unit = ctx->stripe_unit; }
    if (fileid_out != NULL) { *fileid_out = 0; }
    memset(entry, 0, sizeof(*entry));

    st = mds_cat_ds_list((struct mds_catalogue *)ctx->cat, &ds_list,
                         &ds_count);
    if (st != MDS_OK || ds_list == NULL || ds_count == 0) {
        free(ds_list);
        return -1;
    }
    if (ctx->cache != NULL) {
        ds_cache_overlay_weights(ctx->cache, ds_list, ds_count);
    }
    st = placement_select_ex(ctx->policy, ds_list, ds_count, 1, 1,
                             ctx->stripe_unit, entry);
    free(ds_list);
    if (st != MDS_OK) {
        memset(entry, 0, sizeof(*entry));
        return -1;
    }
    st = mds_cat_alloc_fileid((struct mds_catalogue *)ctx->cat, NULL,
                              &fileid);
    if (st != MDS_OK || fileid == 0) {
        memset(entry, 0, sizeof(*entry));
        return -1;
    }
    if (ctx->proxy != NULL) {
        uint32_t fh_len = (uint32_t)sizeof(entry->nfs_fh);
        st = mds_proxy_ensure_ds_file_fh(ctx->proxy, entry->ds_id, fileid,
                                         0, 0, entry->nfs_fh, &fh_len);
        if (st == MDS_OK && fh_len > 0 && fh_len <= sizeof(entry->nfs_fh)) {
            entry->nfs_fh_len = fh_len;
        } else if (ctx->synthetic_fh) {
            synth_fh(entry, fileid);
        } else {
            entry->nfs_fh_len = 0;
        }
    } else if (ctx->synthetic_fh) {
        synth_fh(entry, fileid);
    }
    if (stripe_unit != NULL) { *stripe_unit = ctx->stripe_unit; }
    if (fileid_out != NULL) { *fileid_out = fileid; }
    return 0;
}

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */

static void ctx_free(struct ds_prealloc_ctx *ctx)
{
    if (ctx == NULL) {
        return;
    }
    for (uint32_t i = 0; i < DS_PREALLOC_PLAN_CACHE; i++) {
        free(ctx->plans[i].ds_ids);
    }
    pthread_mutex_destroy(&ctx->plan_lock);
    if (ctx->rings != NULL) {
        for (uint32_t i = 0; i < ctx->ring_count; i++) {
            free(ctx->rings[i].slots);
            pthread_mutex_destroy(&ctx->rings[i].lock);
            pthread_cond_destroy(&ctx->rings[i].not_full);
        }
        free(ctx->rings);
    }
    free(ctx);
}

/*
 * Reclaim pool rows left over from a previous daemon incarnation.
 *
 * Each row names a precreated 0-byte DS file whose fileid was reserved
 * but (as far as this fresh process knows) never consumed.  Rather than
 * re-use them -- which would risk handing out a fileid a prior consume
 * already turned into a live inode -- we hand the DS files to the GC
 * queue for deletion and clear the pool.  Safe (no double-use) and the
 * refill workers rebuild fresh slots immediately.  Best-effort: silent
 * when the backend has no pool table.
 */
static void recover_pool(struct ds_prealloc_ctx *ctx)
{
    struct mds_prealloc_pool_row *rows = NULL;
    uint32_t n = 0;
    uint32_t restored = 0, dropped = 0, ring_i = 0;
    enum mds_status st;

    st = mds_cat_prealloc_pool_scan((struct mds_catalogue *)ctx->cat,
                                    ctx->self_mds_id, &rows, &n);
    if (st != MDS_OK || rows == NULL || n == 0) {
        free(rows);
        return;
    }
    if (ctx->rings == NULL || ctx->ring_count == 0) {
        free(rows);
        return;
    }
    /*
     * Restore persisted slots back into the rings so the precreated DS
     * files are REUSED across a restart.  The old code GC-enqueued every
     * pool row (up to prealloc_pool_size, e.g. 1M) and deleted it, which
     * dumped the entire pool into the GC on every restart -- a self-
     * inflicted multi-million-entry flood that stalled removes and slowed
     * startup.  Slots stay in the pool table until consumed (delete-on-
     * consume); an unconsumed slot is simply re-recovered next restart,
     * and a full ring silently drops the overflow.
     */
    for (uint32_t i = 0; i < n; i++) {
        struct prealloc_slot slot;

        /* Guard a corrupt/out-of-range ds_id: republishing it would re-
         * inject a bad stripe map and re-wedge the GC.  A bad ds_id means
         * the DS precreate never succeeded, so there is nothing to
         * reclaim -- just drop the pool row. */
        if (rows[i].ds_id >= 65536U) {
            (void)mds_cat_prealloc_pool_delete(
                (struct mds_catalogue *)ctx->cat, rows[i].fileid);
            dropped++;
            continue;
        }

        memset(&slot, 0, sizeof(slot));
        slot.fileid      = rows[i].fileid;
        slot.stripe_unit = (rows[i].stripe_unit != 0U)
                               ? rows[i].stripe_unit : ctx->stripe_unit;
        slot.entry.ds_id      = rows[i].ds_id;
        slot.entry.nfs_fh_len = rows[i].nfs_fh_len;
        if (rows[i].nfs_fh_len > 0 &&
            rows[i].nfs_fh_len <= sizeof(slot.entry.nfs_fh)) {
            memcpy(slot.entry.nfs_fh, rows[i].nfs_fh, rows[i].nfs_fh_len);
        }
        ring_push(&ctx->rings[ring_i % ctx->ring_count], &slot);
        ring_i++;
        restored++;
    }
    fprintf(stderr,
            "INFO: ds_prealloc recover_pool: restored %u slots, dropped "
            "%u bad-ds_id rows (of %u scanned)\n", restored, dropped, n);
    free(rows);
}

int ds_prealloc_init(const struct mds_catalogue *cat,
                     struct mds_proxy_ctx *proxy,
                     uint32_t pool_size,
                     struct ds_prealloc_ctx **out)
{
    return ds_prealloc_init_ex(cat, proxy, PLACEMENT_RR, pool_size, out);
}

int ds_prealloc_init_ex(const struct mds_catalogue *cat,
                        struct mds_proxy_ctx *proxy,
                        enum mds_placement_policy policy,
                        uint32_t pool_size,
                        struct ds_prealloc_ctx **out)
{
    /* Single-MDS defaults (self=0, cluster=1, ring_count=auto).  The
     * multi-MDS daemon path uses ds_prealloc_init_ex2 with real
     * topology so each MDS pre-creates on a disjoint DS group. */
    return ds_prealloc_init_ex2(cat, proxy, policy, pool_size,
                                0U, 1U, 0U, out);
}

int ds_prealloc_init_ex2(const struct mds_catalogue *cat,
                         struct mds_proxy_ctx *proxy,
                         enum mds_placement_policy policy,
                         uint32_t pool_size,
                         uint32_t self_mds_id,
                         uint32_t cluster_size,
                         uint32_t ring_count,
                         struct ds_prealloc_ctx **out)
{
    struct ds_prealloc_ctx *ctx;
    uint32_t per_ring;

    if (out == NULL || cat == NULL) {
        if (out != NULL) {
            *out = NULL;
        }
        return -1;
    }
    *out = NULL;

    ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        return -1;
    }
    ctx->cat = cat;
    ctx->proxy = proxy;
    ctx->policy = policy;
    ctx->cache = NULL;
    ctx->stripe_unit = DS_PREALLOC_DEFAULT_STRIPE_UNIT;
    ctx->self_mds_id = self_mds_id;
    ctx->cluster_size = (cluster_size >= 1U) ? cluster_size : 1U;
    atomic_store_explicit(&ctx->stop, false, memory_order_relaxed);
    pthread_mutex_init(&ctx->plan_lock, NULL);

    if (pool_size == 0U) {
        pool_size = 128U;
    }
    if (ring_count == 0U) {
        ring_count = DS_PREALLOC_DEFAULT_RING_COUNT;
    }
    if (ring_count > DS_PREALLOC_MAX_RING_COUNT) {
        ring_count = DS_PREALLOC_MAX_RING_COUNT;
    }
    per_ring = pool_size / ring_count;
    if (per_ring < DS_PREALLOC_MIN_RING_DEPTH) {
        per_ring = DS_PREALLOC_MIN_RING_DEPTH;
    }

    ctx->ring_count = ring_count;
    ctx->rings = calloc(ring_count, sizeof(*ctx->rings));
    if (ctx->rings == NULL) {
        ctx_free(ctx);
        return -1;
    }
    for (uint32_t i = 0; i < ring_count; i++) {
        struct prealloc_ring *r = &ctx->rings[i];
        r->ctx = ctx;
        r->index = i;
        r->cap = per_ring;
        r->slots = calloc(per_ring, sizeof(*r->slots));
        if (r->slots == NULL ||
            pthread_mutex_init(&r->lock, NULL) != 0 ||
            pthread_cond_init(&r->not_full, NULL) != 0) {
            ctx_free(ctx);
            return -1;
        }
    }

    /* Recover persisted slots before the workers start producing. */
    recover_pool(ctx);

    /* Start one refill worker per ring (begins filling immediately). */
    for (uint32_t i = 0; i < ring_count; i++) {
        if (pthread_create(&ctx->rings[i].refill, NULL, refill_main,
                           &ctx->rings[i]) != 0) {
            atomic_store_explicit(&ctx->stop, true, memory_order_relaxed);
            for (uint32_t j = 0; j < i; j++) {
                pthread_mutex_lock(&ctx->rings[j].lock);
                pthread_cond_broadcast(&ctx->rings[j].not_full);
                pthread_mutex_unlock(&ctx->rings[j].lock);
                pthread_join(ctx->rings[j].refill, NULL);
            }
            ctx_free(ctx);
            return -1;
        }
        ctx->rings[i].refill_started = true;
    }

    *out = ctx;
    return 0;
}

int ds_prealloc_pop(struct ds_prealloc_ctx *ctx,
                    struct mds_ds_map_entry *entry,
                    uint32_t *stripe_unit,
                    uint64_t *fileid_out)
{
    if (entry == NULL) {
        return -1;
    }
    memset(entry, 0, sizeof(*entry));
    if (stripe_unit != NULL) { *stripe_unit = 0; }
    if (fileid_out != NULL) { *fileid_out = 0; }
    if (ctx == NULL) {
        return -1;
    }

    uint32_t start = atomic_fetch_add_explicit(&ctx->pop_rr, 1,
                                               memory_order_relaxed);
    for (uint32_t k = 0; k < ctx->ring_count; k++) {
        struct prealloc_ring *r =
            &ctx->rings[(start + k) % ctx->ring_count];
        struct prealloc_slot slot;
        bool got = false;

        pthread_mutex_lock(&r->lock);
        if (r->count > 0) {
            slot = r->slots[r->head];
            r->head = (r->head + 1U) % r->cap;
            r->count--;
            got = true;
            pthread_cond_signal(&r->not_full);
        }
        pthread_mutex_unlock(&r->lock);

        if (got) {
            *entry = slot.entry;
            if (stripe_unit != NULL) { *stripe_unit = slot.stripe_unit; }
            if (fileid_out != NULL) { *fileid_out = slot.fileid; }
            (void)mds_cat_prealloc_pool_delete(
                (struct mds_catalogue *)ctx->cat, slot.fileid);
            mfetch_add(&g_branch_metrics.prealloc_pops_ok, 1);
            if (slot.entry.nfs_fh_len == 0) {
                mfetch_add(&g_branch_metrics.prealloc_pops_fh_missing, 1);
            }
            return 0;
        }
    }

    /* All rings empty -- synchronous fallback. */
    mfetch_add(&g_branch_metrics.prealloc_pops_empty, 1);
    return sync_pop(ctx, entry, stripe_unit, fileid_out);
}

int ds_prealloc_peek(const struct ds_prealloc_ctx *ctx,
                     struct mds_ds_map_entry *entry,
                     uint32_t *stripe_unit)
{
    if (ctx == NULL || entry == NULL) {
        return -1;
    }
    struct ds_prealloc_ctx *c = (struct ds_prealloc_ctx *)ctx;
    uint32_t start = atomic_load_explicit(&c->pop_rr, memory_order_relaxed);
    for (uint32_t k = 0; k < c->ring_count; k++) {
        struct prealloc_ring *r = &c->rings[(start + k) % c->ring_count];
        int rc = -1;
        pthread_mutex_lock(&r->lock);
        if (r->count > 0) {
            *entry = r->slots[r->head].entry;
            if (stripe_unit != NULL) {
                *stripe_unit = r->slots[r->head].stripe_unit;
            }
            rc = 0;
        }
        pthread_mutex_unlock(&r->lock);
        if (rc == 0) {
            return 0;
        }
    }
    /* Nothing ready -- report the policy's choice without side effects. */
    return (ring_select_ds(c, &c->rings[0], entry) == MDS_OK) ? 0 : -1;
}

enum mds_status ds_prealloc_select_any_online(
    const struct ds_prealloc_ctx *ctx,
    struct mds_ds_map_entry *entry,
    uint32_t *stripe_unit)
{
    struct mds_ds_info *ds_list = NULL;
    uint32_t ds_count = 0;
    enum mds_status st;

    if (ctx == NULL || entry == NULL) {
        return MDS_ERR_INVAL;
    }
    if (stripe_unit != NULL) { *stripe_unit = ctx->stripe_unit; }
    st = mds_cat_ds_list((struct mds_catalogue *)ctx->cat, &ds_list,
                         &ds_count);
    if (st != MDS_OK) {
        return st;
    }
    if (ds_list == NULL || ds_count == 0) {
        free(ds_list);
        return MDS_ERR_NOSPC;
    }
    if (ctx->cache != NULL) {
        ds_cache_overlay_weights(ctx->cache, ds_list, ds_count);
    }
    st = placement_select_ex(ctx->policy, ds_list, ds_count, 1, 1,
                             ctx->stripe_unit, entry);
    free(ds_list);
    return st;
}

int ds_prealloc_ensure(const struct ds_prealloc_ctx *ctx,
                       uint32_t ds_id, uint64_t fileid)
{
    enum mds_status st;

    if (ctx == NULL || ctx->proxy == NULL) {
        return 0;
    }
    st = mds_proxy_ensure_ds_file(ctx->proxy, ds_id, fileid, 0, 0);
    return (st == MDS_OK) ? 0 : -1;
}

void ds_prealloc_set_ds_cache(struct ds_prealloc_ctx *ctx,
                              const struct ds_cache *cache)
{
    if (ctx == NULL) {
        return;
    }
    ctx->cache = cache;
}

void ds_prealloc_destroy(struct ds_prealloc_ctx *ctx)
{
    if (ctx == NULL) {
        return;
    }
    atomic_store_explicit(&ctx->stop, true, memory_order_relaxed);
    for (uint32_t i = 0; i < ctx->ring_count; i++) {
        struct prealloc_ring *r = &ctx->rings[i];
        pthread_mutex_lock(&r->lock);
        pthread_cond_broadcast(&r->not_full);
        pthread_mutex_unlock(&r->lock);
        if (r->refill_started) {
            pthread_join(r->refill, NULL);
        }
    }
    ctx_free(ctx);
}

/* -----------------------------------------------------------------------
 * Wide pre-warm batch (HPC-Shared) -- synchronous all-or-nothing.
 * ----------------------------------------------------------------------- */

enum mds_status ds_prealloc_batch(
    struct ds_prealloc_ctx *ctx,
    const struct ds_prealloc_batch_request *req,
    struct ds_prealloc_batch_result *out)
{
    struct mds_ds_info *ds_list = NULL;
    struct mds_ds_map_entry *entries = NULL;
    uint32_t ds_count = 0, n;
    uint32_t stripe_unit;
    uint64_t fileid = 0;
    enum mds_status st;

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    if (ctx == NULL || req == NULL || out == NULL ||
        req->stripe_count == 0U || req->stripe_count > MDS_MAX_STRIPES) {
        return MDS_ERR_INVAL;
    }
    {
        uint32_t mc = (req->mirror_count == 0U) ? 1U : req->mirror_count;
        if (mc > MDS_MAX_MIRRORS) {
            return MDS_ERR_INVAL;
        }
        n = req->stripe_count * mc;
    }
    stripe_unit = (req->stripe_unit != 0U) ? req->stripe_unit
                                           : ctx->stripe_unit;

    st = mds_cat_ds_list((struct mds_catalogue *)ctx->cat, &ds_list,
                         &ds_count);
    if (st != MDS_OK || ds_list == NULL || ds_count == 0) {
        free(ds_list);
        return MDS_ERR_NOSPC;
    }
    /* strict_unique_ds: every stripe must land on a distinct DS, so the
     * online pool must be at least stripe_count wide. */
    if (req->strict_unique_ds && ds_count < req->stripe_count) {
        free(ds_list);
        return MDS_ERR_NOSPC;
    }
    if (ctx->cache != NULL) {
        ds_cache_overlay_weights(ctx->cache, ds_list, ds_count);
    }
    entries = calloc(n, sizeof(*entries));
    if (entries == NULL) {
        free(ds_list);
        return MDS_ERR_NOMEM;
    }

    /* Reuse the cached spread for an identical profile (same geometry,
     * filter, and ONLINE-DS count); otherwise compute + cache it. */
    {
        uint32_t mc = (req->mirror_count == 0U) ? 1U : req->mirror_count;
        bool hit = false;

        pthread_mutex_lock(&ctx->plan_lock);
        for (uint32_t k = 0; k < DS_PREALLOC_PLAN_CACHE; k++) {
            struct prealloc_plan *p = &ctx->plans[k];
            if (p->valid && p->n == n &&
                p->stripe_count == req->stripe_count &&
                p->mirror_count == mc &&
                p->required_mode == req->required_mode &&
                p->required_transport == req->required_transport &&
                p->preferred_transport == req->preferred_transport &&
                p->preferred_caps == req->preferred_caps &&
                p->strict_unique_ds == req->strict_unique_ds &&
                p->ds_count_seen == ds_count) {
                for (uint32_t i = 0; i < n; i++) {
                    entries[i].ds_id = p->ds_ids[i];
                }
                hit = true;
                break;
            }
        }
        pthread_mutex_unlock(&ctx->plan_lock);

        if (!hit) {
            st = placement_select_ex(ctx->policy, ds_list, ds_count,
                                     req->stripe_count, mc, stripe_unit,
                                     entries);
            if (st != MDS_OK) {
                free(ds_list);
                free(entries);
                return MDS_ERR_NOSPC;
            }
            pthread_mutex_lock(&ctx->plan_lock);
            struct prealloc_plan *slot =
                &ctx->plans[ctx->plan_rr++ % DS_PREALLOC_PLAN_CACHE];
            uint32_t *ids = malloc((size_t)n * sizeof(*ids));
            if (ids != NULL) {
                for (uint32_t i = 0; i < n; i++) {
                    ids[i] = entries[i].ds_id;
                }
                free(slot->ds_ids);
                slot->ds_ids             = ids;
                slot->n                  = n;
                slot->stripe_count       = req->stripe_count;
                slot->mirror_count       = mc;
                slot->required_mode      = req->required_mode;
                slot->required_transport = req->required_transport;
                slot->preferred_transport = req->preferred_transport;
                slot->preferred_caps     = req->preferred_caps;
                slot->strict_unique_ds   = req->strict_unique_ds;
                slot->ds_count_seen      = ds_count;
                slot->valid              = true;
            }
            pthread_mutex_unlock(&ctx->plan_lock);
        }
    }
    free(ds_list);

    fileid = req->fileid_hint;
    if (fileid == 0U) {
        st = mds_cat_alloc_fileid((struct mds_catalogue *)ctx->cat, NULL,
                                  &fileid);
        if (st != MDS_OK || fileid == 0U) {
            free(entries);
            return MDS_ERR_NOSPC;
        }
    }

    for (uint32_t i = 0; i < n; i++) {
        uint32_t stripe = i % req->stripe_count;
        uint32_t mirror = i / req->stripe_count;
        bool captured = false;

        if (ctx->proxy != NULL) {
            uint32_t fh_len = (uint32_t)sizeof(entries[i].nfs_fh);
            st = mds_proxy_ensure_ds_file_fh(ctx->proxy, entries[i].ds_id,
                                             fileid, stripe, mirror,
                                             entries[i].nfs_fh, &fh_len);
            if (st == MDS_OK && fh_len > 0 &&
                fh_len <= sizeof(entries[i].nfs_fh)) {
                entries[i].nfs_fh_len = fh_len;
                captured = true;
            } else if (ctx->synthetic_fh) {
                synth_fh(&entries[i], fileid + i + 1U);
                captured = true;
            }
        } else if (ctx->synthetic_fh) {
            synth_fh(&entries[i], fileid + i + 1U);
            captured = true;
        }

        if (!captured) {
            /* No usable FH for this stripe -- roll back the DS files
             * created so far (best-effort GC) and fail the whole batch
             * so the caller never sees a half-built layout. */
            for (uint32_t j = 0; j < i; j++) {
                (void)mds_cat_gc_enqueue(
                    (struct mds_catalogue *)ctx->cat, NULL, fileid,
                    entries[j].ds_id, entries[j].nfs_fh,
                    entries[j].nfs_fh_len);
            }
            free(entries);
            return MDS_ERR_NOSPC;
        }
    }

    out->fileid = fileid;
    out->stripe_count = req->stripe_count;
    out->mirror_count = (req->mirror_count == 0U) ? 1U : req->mirror_count;
    out->stripe_unit = stripe_unit;
    out->entries = entries;
    return MDS_OK;
}

void ds_prealloc_batch_result_destroy(
    struct ds_prealloc_batch_result *res)
{
    if (res == NULL) {
        return;
    }
    free(res->entries);
    memset(res, 0, sizeof(*res));
}

void ds_prealloc_test_enable_synthetic_fh(struct ds_prealloc_ctx *ctx,
                                          bool enabled)
{
    if (ctx == NULL) {
        return;
    }
    ctx->synthetic_fh = enabled;
}

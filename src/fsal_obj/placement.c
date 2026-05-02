/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * placement.c — Stripe placement policy.
 *
 * Phase 2: round-robin across ONLINE data servers.
 * Phase 3 will add capacity-weighted and tier-aware policies.
 * See architecture.md §3.7 for full design.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <time.h>

#include "placement.h"
#include "mds_metrics.h"
#include "wrr.h"   /* mds_wrr_weighted_pick / mds_wrr_capacity_pick */

/*
 * Phase 1.1: cap stack-allocation of the dispatcher's per-call
 * scratch arrays at 64 DSes.  Any realistic cluster fits comfortably;
 * the heap path only fires on pathological ds_count > 64 and bumps
 * `pnfs_mds_placement_heap_fallback_total` so the condition is
 * visible in /metrics.  Must stay in lock-step with MDS_MAX_DS_NODES
 * in pnfs_mds.h (256) - the 64 cap is deliberately smaller so the
 * stack frame never exceeds ~1 KiB on any thread.
 */
#define MDS_PLACEMENT_STACK_MAX 64

/* -----------------------------------------------------------------------
 * Module state
 * ----------------------------------------------------------------------- */

static _Atomic uint32_t g_rr_counter;

/* -----------------------------------------------------------------------
 * Round-robin placement
 * ----------------------------------------------------------------------- */

enum mds_status placement_select(const struct mds_ds_info *ds_list,
                                 uint32_t ds_count,
                                 uint32_t stripe_count,
                                 uint32_t mirror_count,
                                 uint32_t stripe_unit,
                                 struct mds_ds_map_entry *entries)
{
    uint32_t *online = NULL;
    uint32_t online_count = 0;
    uint32_t need;
    uint32_t start;
    uint32_t i, s, m;

    (void)stripe_unit;

    if (ds_list == NULL || entries == NULL ||
        stripe_count == 0 || mirror_count == 0) {
        return MDS_ERR_INVAL;
}

    need = stripe_count * mirror_count;

    /* Build index of ONLINE data servers. */
    online = malloc(ds_count * sizeof(*online));
    if (online == NULL) {
        return MDS_ERR_NOMEM;
}

    for (i = 0; i < ds_count; i++) {
        if (ds_list[i].state == DS_ONLINE) {
            online[online_count++] = i;
}
    }

    /*
     * We need at least `mirror_count` distinct ONLINE DSs so that no
     * two mirrors of the same stripe share a server.
     *
     * Graceful degradation: if fewer DSs are available than the
     * requested stripe_count * mirror_count, reduce stripe_count
     * to fit (minimum 1).  This way LAYOUTGET succeeds with fewer
     * stripes rather than failing outright when a DS goes offline.
     */
    if (online_count < mirror_count) {
        free(online);
        return MDS_ERR_NOSPC;
    }
    if (online_count < need) {
        stripe_count = online_count / mirror_count;
        if (stripe_count == 0) {
            stripe_count = 1;
        }
        need = stripe_count * mirror_count;
    }

    /*
     * Atomic fetch-add gives each concurrent caller a unique starting
     * offset.  The modulo against online_count distributes the load.
     */
    start = atomic_fetch_add_explicit(&g_rr_counter, need,
                                      memory_order_relaxed);

    /*
     * Assign entries: outer loop = stripes, inner loop = mirrors.
     * For each stripe s, the mirror_count DSs are consecutive slots
     * in the online[] array (modulo online_count), guaranteeing
     * distinct servers per mirror when online_count >= mirror_count.
     */
    for (s = 0; s < stripe_count; s++) {
        for (m = 0; m < mirror_count; m++) {
            uint32_t idx = (start + s * mirror_count + m) % online_count;
            uint32_t ds_idx = online[idx];
            struct mds_ds_map_entry *e = &entries[s * mirror_count + m];

            memset(e, 0, sizeof(*e));
            e->ds_id = ds_list[ds_idx].ds_id;
            /* File handle is assigned later by the DS; leave zeroed. */
        }
    }

    free(online);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Phase 1 dispatcher: WEIGHTED_RR + CAPACITY over single-DS stripes
 *
 * The policy-aware front door.  Phase 1 only ever needs one stripe /
 * one mirror; the code below therefore specialises the WEIGHTED_RR
 * and CAPACITY paths for that shape and falls back to the RR helper
 * for any other combination.  The RR path's graceful-degrade rules
 * (placement_select above) cover all fallback cases.
 *
 * The shared PRNG state is per-process, lazy-initialised, and
 * protected only by `rand_r`'s reentrancy contract.  It is acceptable
 * because the output is a placement hint, not a security token; if
 * two callers compute identical weights in the same nanosecond, the
 * resulting pick is still valid.
 * ----------------------------------------------------------------------- */

/*
 * Fill `idx[]` with indices of ONLINE DSes in `ds_list` and
 * `weights[]` with their per-DS placement weights.
 *
 * Precedence, highest to lowest:
 *   1. Operator-configured `ds->weight` (non-zero).  Used on
 *      3rd-party DSes where capacity is not reported to the MDS;
 *      operators set this via `ds_weight.<id>=W` in the INI.
 *   2. Reported free space `total_bytes - used_bytes`.  Used on
 *      DSes whose agents publish capacity to the MDS.
 *   3. Constant 1 (uniform fallback).  Kept so a DS that has not
 *      yet reported anything stays selectable instead of being
 *      invisible to WRR.
 *
 * Caller provides the buffers (stack or heap) of capacity
 * `ds_count`; see placement_select_ex for the sizing logic.
 */
static uint32_t fill_online_with_free(
    const struct mds_ds_info *ds_list, uint32_t ds_count,
    uint32_t *idx, uint64_t *weights)
{
    uint32_t n = 0;
    uint32_t i;

    for (i = 0; i < ds_count; i++) {
        if (ds_list[i].state != DS_ONLINE) {
            continue;
        }
        idx[n] = i;
        if (ds_list[i].weight != 0) {
            weights[n] = (uint64_t)ds_list[i].weight;
        } else {
            weights[n] =
                (ds_list[i].total_bytes > ds_list[i].used_bytes)
                    ? ds_list[i].total_bytes - ds_list[i].used_bytes
                    : 0;
            if (weights[n] == 0) {
                weights[n] = 1;
            }
        }
        n++;
    }
    return n;
}

/*
 * Weighted-random and capacity-derived selection kernels live in
 * the `wrr` opt-in module (src/modules/wrr/) so the community
 * build can compile them out.  See include/wrr.h.  When the wrr
 * module is built as stubs, both kernels return 0 and the
 * walk-forward logic below degrades WEIGHTED_RR / CAPACITY to
 * plain round-robin over the online DS set.
 */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status placement_select_ex(enum mds_placement_policy policy,
                                    const struct mds_ds_info *ds_list,
                                    uint32_t ds_count,
                                    uint32_t stripe_count,
                                    uint32_t mirror_count,
                                    uint32_t stripe_unit,
                                    struct mds_ds_map_entry *entries)
{
    if (ds_list == NULL || entries == NULL ||
        stripe_count == 0 || mirror_count == 0) {
        return MDS_ERR_INVAL;
    }

    /* PLACEMENT_RR always delegates to the legacy RR helper, which
     * already supports stripe>1 / mirror>1 and graceful degrade. */
    if (policy == PLACEMENT_RR) {
        return placement_select(ds_list, ds_count,
                                stripe_count, mirror_count,
                                stripe_unit, entries);
    }

    /*
     * Single-DS fast path: no allocation, no scan, no picking.
     * Common on small labs and early-stage deployments; shaving
     * the alloc+scan off the hot path was the main contributor to
     * the Phase 1 Gate C regression.  Only safe for stripe=1 +
     * mirror=1 because multi-stripe layouts need distinct DSes.
     */
    if (stripe_count == 1 && mirror_count == 1 &&
        ds_count == 1 && ds_list[0].state == DS_ONLINE) {
        memset(&entries[0], 0, sizeof(entries[0]));
        entries[0].ds_id = ds_list[0].ds_id;
        (void)stripe_unit;
        return MDS_OK;
    }

    /*
     * Scratch buffers: stack for ds_count <= MDS_PLACEMENT_STACK_MAX
     * (typical), heap only on oversize clusters.  Heap use bumps a
     * counter so operators see it in /metrics if it ever fires.
     */
    uint32_t stack_idx[MDS_PLACEMENT_STACK_MAX];
    uint64_t stack_w[MDS_PLACEMENT_STACK_MAX];
    uint32_t *online = stack_idx;
    uint64_t *free_bytes = stack_w;
    uint32_t *heap_idx = NULL;
    uint64_t *heap_w = NULL;
    uint32_t online_count;
    uint32_t pick;

    if (ds_count > MDS_PLACEMENT_STACK_MAX) {
        heap_idx = malloc((size_t)ds_count * sizeof(*heap_idx));
        heap_w   = malloc((size_t)ds_count * sizeof(*heap_w));
        if (heap_idx == NULL || heap_w == NULL) {
            free(heap_idx);
            free(heap_w);
            return MDS_ERR_NOMEM;
        }
        online = heap_idx;
        free_bytes = heap_w;
        atomic_fetch_add_explicit(
            &g_branch_metrics.placement_heap_fallback_total,
            1, memory_order_relaxed);
    }

    online_count = fill_online_with_free(ds_list, ds_count,
                                         online, free_bytes);
    if (online_count == 0) {
        free(heap_idx);
        free(heap_w);
        return MDS_ERR_NOSPC;
    }

    /*
     * Policy bail-out: anything outside the Phase 1/3 supported set
     * falls through to the legacy RR helper, which is always safe.
     */
    if (policy != PLACEMENT_WEIGHTED_RR &&
        policy != PLACEMENT_CAPACITY) {
        free(heap_idx);
        free(heap_w);
        return placement_select(ds_list, ds_count,
                                stripe_count, mirror_count,
                                stripe_unit, entries);
    }

    /*
     * Phase 3 fallback (D5): if the cluster has fewer ONLINE DSes
     * than stripe_count * mirror_count, degrade to
     * min(stripe_count, online_count) and recompute mirror_count
     * so the product fits.  This never errors the CREATE; it just
     * advertises a smaller layout than the profile requested and
     * bumps `pnfs_mds_placement_degraded_total` so the degraded
     * condition is visible in /metrics.
     */
    if (online_count < mirror_count) {
        free(heap_idx);
        free(heap_w);
        return MDS_ERR_NOSPC;
    }
    if (stripe_count * mirror_count > online_count) {
        uint32_t new_stripe = online_count / mirror_count;
        if (new_stripe == 0) {
            new_stripe = 1;
        }
        stripe_count = new_stripe;
        atomic_fetch_add_explicit(
            &g_branch_metrics.placement_degraded_total,
            1, memory_order_relaxed);
    }

    /*
     * Phase 3 single-stripe-per-call loop: pick `stripe_count`
     * distinct DSes (one per stripe) and, for each stripe, fill
     * `mirror_count` distinct DSes (round-robin offset from the
     * stripe's chosen DS).  Uses a small local "taken" bitmap to
     * enforce stripe-level distinctness for WRR/CAPACITY; the
     * per-stripe mirror slots then walk the remaining online
     * entries in order.
     */
    uint32_t total = stripe_count * mirror_count;
    bool taken_stack[MDS_PLACEMENT_STACK_MAX] = {0};
    bool *taken = taken_stack;
    bool *taken_heap = NULL;

    if (online_count > MDS_PLACEMENT_STACK_MAX) {
        taken_heap = calloc(online_count, sizeof(*taken_heap));
        if (taken_heap == NULL) {
            free(heap_idx);
            free(heap_w);
            return MDS_ERR_NOMEM;
        }
        taken = taken_heap;
    }

    memset(entries, 0, (size_t)total * sizeof(*entries));
    uint32_t s;

    for (s = 0; s < stripe_count; s++) {
        /*
         * Pick the head DS for stripe s via the configured policy,
         * ignoring slots already taken by previous stripes.
         */
        uint64_t masked[MDS_PLACEMENT_STACK_MAX];
        const uint64_t *pool = free_bytes;

        if (online_count <= MDS_PLACEMENT_STACK_MAX) {
            for (uint32_t i = 0; i < online_count; i++) {
                masked[i] = taken[i] ? 0 : free_bytes[i];
            }
            pool = masked;
        }

        switch (policy) {
        case PLACEMENT_WEIGHTED_RR:
            pick = mds_wrr_weighted_pick(pool, online_count);
            break;
        case PLACEMENT_CAPACITY:
            pick = mds_wrr_capacity_pick(pool, online_count);
            break;
        default:
            pick = 0;
            break;
        }
        /*
         * If the masked pool is all zeros (pool fell behind
         * stripe demand in a >64 cluster with taken_heap), walk
         * forward from pick to find the next unset slot.
         */
        if (taken[pick]) {
            uint32_t j;
            for (j = 0; j < online_count; j++) {
                uint32_t idx = (pick + j) % online_count;
                if (!taken[idx]) {
                    pick = idx;
                    break;
                }
            }
        }
        taken[pick] = true;

        entries[(size_t)s * mirror_count].ds_id =
            ds_list[online[pick]].ds_id;

        /*
         * Mirror slots: walk the online array from pick+1, skipping
         * any slot already taken (either by another stripe's head
         * or by this stripe's earlier mirrors).  Guarantees the
         * no-two-mirrors-on-same-DS invariant.
         */
        for (uint32_t m = 1; m < mirror_count; m++) {
            uint32_t start = (pick + m) % online_count;
            uint32_t j;
            for (j = 0; j < online_count; j++) {
                uint32_t idx = (start + j) % online_count;
                if (!taken[idx]) {
                    entries[(size_t)s * mirror_count + m].ds_id =
                        ds_list[online[idx]].ds_id;
                    taken[idx] = true;
                    break;
                }
            }
        }
    }
    (void)stripe_unit;

    free(taken_heap);
    free(heap_idx);
    free(heap_w);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Replacement DS selection (resilvering)
 * ----------------------------------------------------------------------- */

static bool ds_id_in_list(uint32_t ds_id,
                          const uint32_t *list, uint32_t count)
{
    for (uint32_t i = 0; i < count; i++) {
        if (list[i] == ds_id) {
            return true;
}
    }
    return false;
}

enum mds_status placement_select_replacement(
    const struct mds_ds_info *ds_list, uint32_t ds_count,
    const uint32_t *exclude_ds_ids, uint32_t exclude_count,
    uint32_t target_ds_id, uint32_t *out_ds_id)
{
    if (ds_list == NULL || out_ds_id == NULL) {
        return MDS_ERR_INVAL;
}

    /* Admin-specified target: validate it. */
    if (target_ds_id != 0) {
        for (uint32_t i = 0; i < ds_count; i++) {
            if (ds_list[i].ds_id == target_ds_id) {
                if (ds_list[i].state != DS_ONLINE) {
                    return MDS_ERR_NOSPC;
}
                if (ds_id_in_list(target_ds_id,
                                  exclude_ds_ids, exclude_count)) {
                    return MDS_ERR_NOSPC;
}
                *out_ds_id = target_ds_id;
                return MDS_OK;
            }
        }
        return MDS_ERR_NOSPC; /* target not found */
    }

    /* Auto-select: first eligible ONLINE DS not in exclude list. */
    for (uint32_t i = 0; i < ds_count; i++) {
        if (ds_list[i].state != DS_ONLINE) {
            continue;
}
        if (ds_id_in_list(ds_list[i].ds_id,
                          exclude_ds_ids, exclude_count)) {
            continue;
}
        *out_ds_id = ds_list[i].ds_id;
        return MDS_OK;
    }

    return MDS_ERR_NOSPC;
}

/* -----------------------------------------------------------------------
 * Tier-aware DS selection (tiering mover)
 * ----------------------------------------------------------------------- */

enum mds_status placement_select_for_tier(
    const struct mds_ds_info *ds_list, uint32_t ds_count,
    const uint32_t *exclude_ds_ids, uint32_t exclude_count,
    uint32_t target_tier, uint32_t *out_ds_id)
{
    if (ds_list == NULL || out_ds_id == NULL) {
        return MDS_ERR_INVAL;
    }

    /* Select the eligible DS with the most free space to spread
     * relocations across DSs in a tier (least-used-bytes). */
    uint32_t best_id = 0;
    uint64_t best_free = 0;
    int found = 0;

    for (uint32_t i = 0; i < ds_count; i++) {
        if (ds_list[i].state != DS_ONLINE) {
            continue;
        }
        if (ds_list[i].tier != target_tier) {
            continue;
        }
        if (ds_id_in_list(ds_list[i].ds_id,
                          exclude_ds_ids, exclude_count)) {
            continue;
        }
        uint64_t avail = (ds_list[i].total_bytes > ds_list[i].used_bytes)
                       ? ds_list[i].total_bytes - ds_list[i].used_bytes
                       : 0;
        if (!found || avail > best_free) {
            best_id = ds_list[i].ds_id;
            best_free = avail;
            found = 1;
        }
    }

    if (!found) {
        return MDS_ERR_NOSPC;
    }
    *out_ds_id = best_id;
    return MDS_OK;
}

bool ds_profile_match(const struct mds_ds_info *ds,
                      uint8_t required_mode,
                      uint8_t required_transport)
{
    if (ds == NULL) {
        return false;
    }
    /* Mode check: 0xFF means any mode is acceptable. */
    if (required_mode != 0xFF && ds->mode != required_mode) {
        return false;
    }
    /* Transport check: at least one matching bit required.
     * Legacy records (transport=0) are treated as TCP-capable. */
    uint8_t effective_transport = ds->transport;
    if (effective_transport == 0) {
        effective_transport = DS_TRANSPORT_TCP;
    }
    if (required_transport != 0 &&
        (effective_transport & required_transport) == 0) {
        return false;
    }
    return true;
}

enum mds_status ds_filter_compatible(
    const struct mds_ds_info *in, uint32_t in_count,
    uint8_t required_mode, uint8_t required_transport,
    struct mds_ds_info **out, uint32_t *out_count)
{
    if (in == NULL || out == NULL || out_count == NULL) {
        return MDS_ERR_INVAL;
    }

    /* Count matches first. */
    uint32_t match = 0;
    for (uint32_t i = 0; i < in_count; i++) {
        if (in[i].state == DS_ONLINE &&
            ds_profile_match(&in[i], required_mode,
                             required_transport)) {
            match++;
        }
    }

    if (match == 0) {
        *out = NULL;
        *out_count = 0;
        return MDS_OK;
    }

    struct mds_ds_info *arr = calloc(match, sizeof(*arr));
    if (arr == NULL) {
        return MDS_ERR_NOMEM;
    }

    uint32_t j = 0;
    for (uint32_t i = 0; i < in_count; i++) {
        if (in[i].state == DS_ONLINE &&
            ds_profile_match(&in[i], required_mode,
                             required_transport)) {
            arr[j++] = in[i];
        }
    }

    *out = arr;
    *out_count = match;
    return MDS_OK;
}

bool ds_capability_match(const struct mds_ds_info *ds, uint32_t required_caps)
{
    if (ds == NULL) { return false; }
    if (required_caps == 0) { return true; }
    return (ds->capabilities & required_caps) == required_caps;
}

enum mds_status ds_filter_with_caps(
    const struct mds_ds_info *in, uint32_t in_count,
    uint8_t required_mode, uint8_t required_transport,
    uint32_t required_caps,
    struct mds_ds_info **out, uint32_t *out_count)
{
    if (in == NULL || out == NULL || out_count == NULL) {
        return MDS_ERR_INVAL;
    }

    uint32_t match = 0;
    for (uint32_t i = 0; i < in_count; i++) {
        if (in[i].state == DS_ONLINE &&
            ds_profile_match(&in[i], required_mode,
                             required_transport) &&
            ds_capability_match(&in[i], required_caps)) {
            match++;
        }
    }

    if (match == 0) {
        *out = NULL;
        *out_count = 0;
        return MDS_OK;
    }

    struct mds_ds_info *arr = calloc(match, sizeof(*arr));
    if (arr == NULL) {
        return MDS_ERR_NOMEM;
    }

    uint32_t j = 0;
    for (uint32_t i = 0; i < in_count; i++) {
        if (in[i].state == DS_ONLINE &&
            ds_profile_match(&in[i], required_mode,
                             required_transport) &&
            ds_capability_match(&in[i], required_caps)) {
            arr[j++] = in[i];
        }
    }

    *out = arr;
    *out_count = match;
    return MDS_OK;
}

/*
 * Phase H of docs/hpc-nto1-plan.md — ds_filter_compatible with a soft
 * preference (preferred transport bits + preferred capability bits).
 *
 * Two-pass scan over the same input array:
 *
 *   pass 1 (counting): tally how many ONLINE DSes match the hard
 *                      required filter, and how many of those also
 *                      match the soft preference.
 *   pass 2 (emit):     fill the output array.  When the preferred
 *                      subset is non-empty we emit ONLY those DSes;
 *                      otherwise we emit the full required set
 *                      (graceful fallback for clusters with no
 *                      RDMA / GPUDirect hardware).
 *
 * Behaviour with preferred_transport == 0 && preferred_caps == 0 is
 * delegated to ds_filter_compatible — same wire shape, no allocation
 * overhead beyond what the legacy callers already pay.  Memory
 * ownership matches the rest of the file: caller owns *out and must
 * free it.
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status ds_filter_compatible_preferred(
    const struct mds_ds_info *in, uint32_t in_count,
    uint8_t  required_mode,
    uint8_t  required_transport,
    uint8_t  preferred_transport,
    uint32_t preferred_caps,
    struct mds_ds_info **out, uint32_t *out_count)
{
    if (in == NULL || out == NULL || out_count == NULL) {
        return MDS_ERR_INVAL;
    }

    /* No preference — fall through to the legacy filter so the
     * one-arg-shape callers are guaranteed bit-identical. */
    if (preferred_transport == 0 && preferred_caps == 0) {
        return ds_filter_compatible(in, in_count,
                                    required_mode,
                                    required_transport,
                                    out, out_count);
    }

    uint32_t required_count  = 0;
    uint32_t preferred_count = 0;
    for (uint32_t i = 0; i < in_count; i++) {
        if (in[i].state != DS_ONLINE) {
            continue;
        }
        if (!ds_profile_match(&in[i], required_mode,
                              required_transport)) {
            continue;
        }
        required_count++;

        /* Preferred-transport check uses the same legacy-record
         * convention as ds_profile_match: a stored transport of 0
         * is treated as TCP-only. */
        uint8_t effective_transport = in[i].transport;
        if (effective_transport == 0) {
            effective_transport = DS_TRANSPORT_TCP;
        }
        bool transport_ok = (preferred_transport == 0) ||
            ((effective_transport & preferred_transport) != 0);
        bool caps_ok = ds_capability_match(&in[i], preferred_caps);
        if (transport_ok && caps_ok) {
            preferred_count++;
        }
    }

    bool emit_preferred_only = (preferred_count > 0);
    uint32_t emit_count = emit_preferred_only
                        ? preferred_count
                        : required_count;
    if (emit_count == 0) {
        *out = NULL;
        *out_count = 0;
        return MDS_OK;
    }

    struct mds_ds_info *arr = calloc(emit_count, sizeof(*arr));
    if (arr == NULL) {
        return MDS_ERR_NOMEM;
    }

    uint32_t j = 0;
    for (uint32_t i = 0; i < in_count; i++) {
        if (in[i].state != DS_ONLINE) {
            continue;
        }
        if (!ds_profile_match(&in[i], required_mode,
                              required_transport)) {
            continue;
        }

        if (emit_preferred_only) {
            uint8_t effective_transport = in[i].transport;
            if (effective_transport == 0) {
                effective_transport = DS_TRANSPORT_TCP;
            }
            bool transport_ok = (preferred_transport == 0) ||
                ((effective_transport & preferred_transport) != 0);
            bool caps_ok = ds_capability_match(&in[i], preferred_caps);
            if (!(transport_ok && caps_ok)) {
                continue;
            }
        }
        arr[j++] = in[i];
    }

    *out = arr;
    *out_count = emit_count;
    return MDS_OK;
}

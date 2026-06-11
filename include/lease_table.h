/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * lease_table.h -- Per-(DS-stripe) layout lease table.
 *
 * Enforces exclusive-writer semantics on overlapping byte ranges
 * within a single stripe of a single file: a LAYOUTGET whose lease
 * slice overlaps an active, unexpired lease held by a *different*
 * client on the same (fileid, stripe_index) returns
 * NFS4ERR_LAYOUTTRYLATER.
 *
 * The key change versus the legacy per-(fileid, offset) table is
 * that the conflict domain now matches the physical I/O scope:
 * writers to disjoint DS stripes of the same shared file no longer
 * collide.  ds_id is recorded on each entry for tracing/observability
 * only; it never participates in conflict resolution because the
 * stripe-to-DS mapping may change across re-layouts.
 *
 * Thread-safe: all operations hold a per-shard mutex.
 */

#ifndef LEASE_TABLE_H
#define LEASE_TABLE_H

#include <stdint.h>
#include <stdbool.h>

struct stripe_lease_table;

/**
 * @brief Initialise a stripe lease table.
 * @param[out] out  Receives the new table (caller owns).
 * @return 0 on success, -1 on allocation failure.
 */
int  stripe_lease_table_init(struct stripe_lease_table **out);

/**
 * @brief Destroy the table and free all resources.
 * @param tbl  Table to destroy (NULL-safe).
 */
void stripe_lease_table_destroy(struct stripe_lease_table *tbl);

/**
 * @brief Check whether a lease conflict exists for one stripe slice.
 *
 * Returns true if an unexpired lease held by a *different* clientid
 * overlaps the given (fileid, stripe_index, ds_offset, ds_length)
 * within-stripe range.  Expired entries are lazily evicted during
 * the scan.
 *
 * @param tbl           Lease table.
 * @param fileid        Target file.
 * @param clientid      Requesting client (same-client leases never conflict).
 * @param stripe_index  Logical stripe number (offset / stripe_unit).
 * @param ds_offset     Slice offset within the stripe.
 * @param ds_length     Slice length within the stripe (>0).
 * @return true if a conflict exists, false otherwise.
 */
bool stripe_lease_check_conflict(struct stripe_lease_table *tbl,
                                 uint64_t fileid,
                                 uint64_t clientid,
                                 uint32_t stripe_index,
                                 uint64_t ds_offset,
                                 uint64_t ds_length);

/**
 * @brief Acquire (or renew) a per-stripe slice lease.
 *
 * Inserts or updates the entry keyed on (fileid, clientid,
 * stripe_index, ds_offset).  If an entry already exists for the same
 * tuple, ds_length / ds_id / expiry are refreshed in place.
 *
 * @param tbl           Lease table.
 * @param fileid        File.
 * @param clientid      Acquiring client.
 * @param ds_id         DS that owns the stripe (tracing only, may be 0).
 * @param stripe_index  Logical stripe number.
 * @param ds_offset     Slice offset within the stripe.
 * @param ds_length     Slice length within the stripe (>0).
 * @param duration_ms   Lease duration in milliseconds (>0).
 * @return 0 on success, -1 on allocation failure or invalid input.
 */
int  stripe_lease_acquire(struct stripe_lease_table *tbl,
                          uint64_t fileid,
                          uint64_t clientid,
                          uint32_t ds_id,
                          uint32_t stripe_index,
                          uint64_t ds_offset,
                          uint64_t ds_length,
                          uint32_t duration_ms);

/**
 * @brief Release a single stripe slice lease (best-effort).
 *
 * Removes the entry matching (fileid, clientid, stripe_index,
 * ds_offset).  No-op if no such entry exists (idempotent).
 *
 * @param tbl           Lease table.
 * @param fileid        File.
 * @param clientid      Releasing client.
 * @param stripe_index  Logical stripe number.
 * @param ds_offset     Slice offset within the stripe.
 */
void stripe_lease_release(struct stripe_lease_table *tbl,
                          uint64_t fileid,
                          uint64_t clientid,
                          uint32_t stripe_index,
                          uint64_t ds_offset);

/**
 * @brief Release every lease held by @p clientid on @p fileid.
 *
 * Used by the LAYOUTRETURN4_FILE path: whole-file return semantics
 * do not require slice reconstruction.  Idempotent.
 *
 * @param tbl       Lease table.
 * @param fileid    File whose leases for this client should be released.
 * @param clientid  Owning client.
 */
void stripe_lease_release_all_for(struct stripe_lease_table *tbl,
                                  uint64_t fileid,
                                  uint64_t clientid);


struct stripe_slice;  /* defined in lease_stripe_map.h */

/**
 * @brief Longest conflict-free file-byte prefix of a slice vector.
 *
 * Walks the slice vector in stripe-index order (which is also the
 * file-byte order within the first stripe-ring lap of the lease
 * range).  For each slice queries stripe_lease_check_conflict() and
 * returns the file-byte count, starting at @p lease_offset, that is
 * conflict-free.
 *
 * Used by op_layoutget to proactively narrow a grant on partial
 * contention -- rather than returning NFS4ERR_LAYOUTTRYLATER when
 * any slice is held, the caller shrinks lease+grant length to this
 * prefix and serves a smaller, immediately-usable layout.
 *
 * Returns:
 *   0                  -- first slice already contended; caller
 *                         should return NFS4ERR_LAYOUTTRYLATER.
 *   == lease_length    -- entire range conflict-free; no change.
 *   0 < x < lease_len  -- caller may narrow to x file bytes.
 *
 * NULL-safe: tbl==NULL, slices==NULL, nslices==0, stripe_unit==0
 * or lease_length==0 all return lease_length (caller proceeds as
 * if no contention).
 */
uint64_t stripe_lease_prefix_conflict_free_length(
    struct stripe_lease_table *tbl,
    const struct stripe_slice *slices,
    uint32_t nslices,
    uint32_t stripe_unit,
    uint64_t lease_offset,
    uint64_t lease_length,
    uint64_t fileid,
    uint64_t clientid);

#endif /* LEASE_TABLE_H */

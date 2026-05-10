/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * lease_table.h -- Stripe lease table for per-(fileid, range) layout leases.
 *
 * Enforces exclusive-writer semantics on overlapping stripe ranges:
 * a LAYOUTGET that overlaps an active, unexpired lease held by a
 * different client returns NFS4ERR_LAYOUTTRYLATER.  LAYOUTRETURN
 * releases the lease early.  Clients renew by issuing LAYOUTRETURN
 * followed by a new LAYOUTGET before expiry.
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
 * @brief Check whether a lease conflict exists.
 *
 * Returns true if an unexpired lease held by a *different* clientid
 * overlaps the given (fileid, offset, length) range.  Expired entries
 * are lazily evicted during the scan.
 *
 * @param tbl        Lease table.
 * @param fileid     Target file.
 * @param clientid   Requesting client (same-client leases do not conflict).
 * @param offset     Start of requested byte range.
 * @param length     Length of requested byte range (UINT64_MAX = whole file).
 * @return true if a conflict exists, false otherwise.
 */
bool stripe_lease_check_conflict(struct stripe_lease_table *tbl,
                                 uint64_t fileid,
                                 uint64_t clientid,
                                 uint64_t offset,
                                 uint64_t length);

/**
 * @brief Acquire (or renew) a stripe lease.
 *
 * Inserts or updates a lease entry keyed on (fileid, offset).  If an
 * entry already exists for the same fileid and offset, it is replaced
 * unconditionally (caller must have checked for conflicts first).
 *
 * @param tbl          Lease table.
 * @param fileid       File.
 * @param clientid     Acquiring client.
 * @param offset       Start of byte range.
 * @param length       Length of byte range.
 * @param duration_ms  Lease duration in milliseconds.
 * @return 0 on success, -1 on allocation failure.
 */
int  stripe_lease_acquire(struct stripe_lease_table *tbl,
                          uint64_t fileid,
                          uint64_t clientid,
                          uint64_t offset,
                          uint64_t length,
                          uint32_t duration_ms);

/**
 * @brief Release a stripe lease (best-effort).
 *
 * Removes the entry matching (fileid, clientid, offset).  No-op if
 * no such entry exists (idempotent).
 *
 * @param tbl       Lease table.
 * @param fileid    File.
 * @param clientid  Releasing client.
 * @param offset    Start of byte range (0 + UINT64_MAX length = whole file).
 */
void stripe_lease_release(struct stripe_lease_table *tbl,
                          uint64_t fileid,
                          uint64_t clientid,
                          uint64_t offset);

#endif /* LEASE_TABLE_H */

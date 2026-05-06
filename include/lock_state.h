/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * lock_state.h -- NFSv4.1 byte-range lock manager API.
 *
 * Implements LOCK, LOCKT, LOCKU per RFC 8881 S18.10-18.12.
 * Thread-safe via striped mutexes.
 */

#ifndef LOCK_STATE_H
#define LOCK_STATE_H

#include <stdint.h>
#include <stdbool.h>
#include "compound.h"   /* struct nfs4_stateid */

/* RFC 8881 lock types */
#define READ_LT          1
#define WRITE_LT         2
#define READW_LT         3   /* blocking read (not implemented -- treated as READ_LT) */
#define WRITEW_LT        4   /* blocking write (treated as WRITE_LT) */

#define LOCK_OWNER_MAX  128

/** Information about a conflicting lock (for DENIED response). */
struct lock_conflict {
    uint64_t offset;
    uint64_t length;
    uint32_t lock_type;
    uint64_t clientid;
    uint8_t  owner[LOCK_OWNER_MAX];
    uint32_t owner_len;
};

struct lock_table;  /* Opaque */
struct mds_catalogue;

/**
 * Initialise the lock table.
 * @param mds_id  MDS node ID (for lock stateid generation).
 * @param out     Receives the lock table handle.
 * @return 0 on success, -1 on allocation failure.
 */
int lock_table_init(uint32_t mds_id, struct lock_table **out);

/** Destroy the lock table and free all state. */
void lock_table_destroy(struct lock_table *lt);

/** Attach catalogue for RonDB-backed shared lock state. */
void lock_table_set_cat(struct lock_table *lt,
                        struct mds_catalogue *cat,
                        uint64_t boot_epoch);

/**
 * Acquire a byte-range lock.
 *
 * @param lt            Lock table.
 * @param fileid        Target file.
 * @param lock_type     READ_LT or WRITE_LT.
 * @param offset        Lock range start.
 * @param length        Lock range length (UINT64_MAX = whole file).
 * @param clientid      Owning client.
 * @param owner         Lock owner opaque.
 * @param owner_len     Lock owner length.
 * @param open_stateid  Associated open stateid (for new lock-owner).
 * @param lock_stateid  In/out: existing lock stateid (seqid checked),
 *                      or zeroed for new lock-owner. On success, set
 *                      to the granted lock stateid.
 * @param conflict      Out: filled if NFS4ERR_DENIED.
 * @return 0 on success, NFS4ERR_DENIED on conflict, NFS4ERR_INVAL
 *         on bad args, negative on internal error.
 */
int lock_acquire(struct lock_table *lt,
                 uint64_t fileid, uint32_t lock_type,
                 uint64_t offset, uint64_t length,
                 uint64_t clientid,
                 const uint8_t *owner, uint32_t owner_len,
                 const struct nfs4_stateid *open_stateid,
                 struct nfs4_stateid *lock_stateid,
                 struct lock_conflict *conflict);

/**
 * Test whether a lock would succeed (LOCKT).
 * @return 0 if lock would succeed, NFS4ERR_DENIED + conflict info.
 */
int lock_test(struct lock_table *lt,
              uint64_t fileid, uint32_t lock_type,
              uint64_t offset, uint64_t length,
              uint64_t clientid,
              const uint8_t *owner, uint32_t owner_len,
              struct lock_conflict *conflict);

/**
 * Release a byte-range lock (LOCKU).
 * @param lt            Lock table.
 * @param lock_stateid  Lock stateid (seqid checked and incremented).
 * @param lock_type     Lock type to release.
 * @param offset        Range start.
 * @param length        Range length.
 * @return 0 on success, NFS4ERR_BAD_STATEID, or negative on error.
 */
int lock_release(struct lock_table *lt,
                 struct nfs4_stateid *lock_stateid,
                 uint32_t lock_type,
                 uint64_t offset, uint64_t length);

/**
 * Release all locks held by a client (for lease expiry cleanup).
 */
void lock_release_all_for_client(struct lock_table *lt, uint64_t clientid);

/** Check if a lock stateid exists (for TEST_STATEID). */
bool lock_state_exists(const struct lock_table *lt,
                       const uint8_t other[NFS4_OTHER_SIZE]);

#endif /* LOCK_STATE_H */

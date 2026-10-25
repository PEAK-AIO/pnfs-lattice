/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * trim_tail.h -- Trim module state: the striped per-inode lock
 * (L(F)), the quiescing-flag table, and the resolved trim_max_wait
 * value.
 *
 * Backing for the durable background DS-tail trim: the shrink path
 * (compound SETATTR size) enqueues a TRIM row into the catalogue GC
 * queue, and the ds_gc drainer drains it by quiescing the file's
 * layouts, taking the per-inode lock L(F), and truncating every
 * stripe/mirror DS file to the current committed size.  This module
 * owns only the state shared by those paths; the trim execution core
 * lives in the same .c and arrives with the drainer wiring.
 */

#ifndef TRIM_TAIL_H
#define TRIM_TAIL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "pnfs_mds.h"

/* Forward declarations (bound before the drainer starts; see the
 * setter docs below). */
struct mds_catalogue;
struct mds_proxy_ctx;
struct subtree_map;
struct layout_recall;

/**
 * @brief Initialise the trim module state.
 *
 * All state is static, so this cannot fail at present; the int
 * return keeps the signature in the family of the other module
 * initialisers.  May be called again after trim_module_fini() (or in
 * place, which resets the state: every quiescing flag is cleared and
 * the resolved wait is recomputed).  Only call this before other
 * threads start using the module -- the daemon calls it once at
 * startup, the tests call it to simulate restarts.
 *
 * @param lease_time_sec    NFS lease duration in seconds
 *                          (0 = library default, 90).
 * @param trim_max_wait_sec Maximum quiescence wait in seconds.
 *                          0 means "3 x lease_time_sec"; the resolved
 *                          value is returned by trim_max_wait_sec().
 * @return 0 (no failure mode exists today; see above).
 */
int trim_module_init(uint32_t lease_time_sec, uint32_t trim_max_wait_sec);

/**
 * @brief Tear down the trim module state.
 *
 * Clears every quiescing flag and destroys the lock tables.  No
 * other thread may be using the module when this runs.
 */
void trim_module_fini(void);

/**
 * @brief Lock the per-inode mutex L(F) for @a fileid.
 *
 * The table has 16 stripes and the index is the fileid folded
 * through splitmix64, so fileids on the same stripe share one mutex
 * and serialize against each other (16 stripes is a deliberate
 * over-approximation: L(F) only needs to make per-inode trim
 * exclusive against proxy writes and LAYOUTCOMMIT commits, not to
 * keep unrelated inodes apart).
 *
 * Recursion-unsafe by contract: the caller must not already hold
 * L(F) (the mutex is not recursive).
 *
 * @param fileid Inode id to lock.
 */
void trim_inode_lock(uint64_t fileid);

/**
 * @brief Release L(F).
 *
 * @param fileid Inode id to unlock; the caller must hold L(F).
 */
void trim_inode_unlock(uint64_t fileid);

/**
 * @brief Mark @a fileid as quiescing (idempotent).
 *
 * While the flag is set, LAYOUTGET must answer NFS4ERR_DELAY for
 * this file.  The flag must never fail to be set -- a missing flag
 * leaves the LAYOUTGET gate open and reopens the P1-1 window -- so
 * the table is a striped, dynamically growing list that falls back
 * to fixed per-stripe emergency slots if the entry allocation
 * fails.
 *
 * @param fileid Inode id to mark.
 */
void trim_quiesce_begin(uint64_t fileid);

/**
 * @brief Clear the quiescing flag for @a fileid.
 *
 * No-op when the flag is not set.
 *
 * @param fileid Inode id to clear.
 */
void trim_quiesce_end(uint64_t fileid);

/**
 * @brief Report whether @a fileid is currently quiescing.
 *
 * @param fileid Inode id to query.
 * @return true while a trim is quiescing this file.
 */
bool trim_quiescing(uint64_t fileid);

/**
 * @brief Resolved maximum quiescence wait in seconds.
 *
 * Stable once trim_module_init() has run; 0 in the config resolves
 * to 3 x lease_time_sec.
 *
 * @return Resolved wait in seconds.
 */
uint32_t trim_max_wait_sec(void);

/**
 * @brief Resolved NFS lease duration in seconds.
 *
 * Stable once trim_module_init() has run; 0 in the config resolves
 * to the library default (90).  Binds the layout-quiescence wait in
 * trim_process_entry() (lease_time_sec * 1000 ms).
 *
 * @return Resolved lease in seconds.
 */
uint32_t trim_lease_sec(void);

/**
 * @brief Bind the partition map used by the trim ownership guard.
 *
 * When bound (non-NULL), trim_process_entry() resolves the file's
 * path (parent_fileid ancestry walk against authoritative reads) and
 * compares the owning MDS in @a map against the catalogue handle's
 * self_mds_id: a non-owner leaves the row queued for the successor
 * owner and never touches the DS files.  NULL (the default, and
 * every single-MDS deployment) fails open: this MDS is its own
 * owner.
 *
 * Call this before any trim_process_entry() may run (the daemon
 * binds it at startup, next to trim_module_init; the unit tests bind
 * it on the main thread).
 *
 * @param map  Partition map, or NULL to fail open.
 */
void trim_set_subtree_map(const struct subtree_map *map);

/**
 * @brief Bind the layout recall coordinator for the quiescence step.
 *
 * When bound (non-NULL), trim_process_entry() recalls/revokes every
 * outstanding client layout for the file (bounded by one lease)
 * before its locked truncate.  NULL (the default in unit tests,
 * where the fixtures hold no layouts) makes the step a no-op.
 *
 * Call this before any trim_process_entry() may run.
 *
 * @param lr  Recall coordinator handle, or NULL.
 */
void trim_set_layout_recall(struct layout_recall *lr);

/**
 * @brief Drain one TRIM GC row: quiesce the file's client layouts,
 *        then under L(F) truncate every stripe/mirror DS file to the
 *        current committed size.
 *
 * Execution order (spec S3.2/S3.4/S3.5): authoritative vanish check
 * (inode gone OR no stripe map = dequeue as success), ownership
 * guard (V5; indeterminate walk = keep queued, never guess),
 * first-seen bookkeeping with the trim_max_wait backoff bound,
 * quiescing flag set (closes the LAYOUTGET gate), lease-bounded
 * layout quiescence, then the critical section: re-read the
 * authoritative size and re-fetch the stripe map under L(F) and
 * truncate every slot to that size (Rule R).
 *
 * Status contract: MDS_OK ONLY when the row was drained (dequeued --
 * the caller must not dequeue again); MDS_ERR_DELAY = leave the row
 * queued (non-owner, indeterminate ownership, transient failure,
 * backoff bound), which the ds_gc drainer's generic non-OK handling
 * turns into retry + successor hand-off for free.
 *
 * The quiescing flag is cleared on every exit path EXCEPT a
 * truncate/mount failure, where it is deliberately kept set: the DS
 * tail is still stale, so the LAYOUTGET gate stays closed until a
 * later cycle succeeds (the backoff bound releases it periodically
 * so the gate cannot wedge a file permanently).  A drain that
 * exits clears the flag only when it is the LAST in-flight drain
 * of the fileid (an in-flight counter in the first-seen table,
 * bumped before the flag is set and cleared under the same mutex),
 * so one drain can never open the LAYOUTGET gate while a
 * concurrent drain's truncate is still running.
 *
 * Thread-safe: multiple drainer workers may call this concurrently
 * for the same fileid (one file can hold several TRIM rows); L(F)
 * serializes the critical section and the row is dequeued by its
 * own gc_seq.
 *
 * @param cat    Catalogue handle (authoritative reads + GC queue).
 * @param proxy  DS-mount proxy context (truncate targets).
 * @param e      The TRIM row to drain.
 * @return MDS_OK drained, MDS_ERR_DELAY keep queued,
 *         MDS_ERR_INVAL on NULL arguments.
 */
enum mds_status trim_process_entry(struct mds_catalogue *cat,
                                   const struct mds_proxy_ctx *proxy,
                                   const struct mds_gc_entry *e);

#endif /* TRIM_TAIL_H */

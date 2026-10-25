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
#include <stdint.h>

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

#endif /* TRIM_TAIL_H */

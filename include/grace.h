/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * grace.h — NFSv4.1 grace period management.
 *
 * Thread-safe: all public functions are protected by an internal mutex.
 */

#ifndef GRACE_H
#define GRACE_H

#include <stdint.h>
#include <stdbool.h>

struct client_recovery_rec;  /* Forward declaration (pnfs_mds.h) */

/**
 * @brief Initialise grace period subsystem.  Must be called once
 *        before any other grace_*() function.
 */
void grace_init(void);

/**
 * @brief Enter the grace period (simple, no client tracking).
 * @param duration_sec  Duration in seconds.
 */
void grace_enter(uint32_t duration_sec);

/**
 * @brief Enter the grace period with a known set of recovering clients.
 *
 * Stores the client list internally.  grace_client_reclaimed() marks
 * individual clients done; when all have reclaimed (or the timeout
 * expires), the grace period ends.
 *
 * @param duration_sec  Maximum grace duration in seconds.
 * @param recs          Array of client recovery records (copied internally).
 * @param count         Number of records.  0 is valid (grace with no
 *                      tracked clients — timeout only).
 */
void grace_enter_with_clients(uint32_t duration_sec,
                              const struct client_recovery_rec *recs,
                              uint32_t count);

/**
 * @brief Check whether the grace period is currently active.
 *
 * If the configured duration has elapsed since grace_enter(),
 * the grace period is automatically ended.
 *
 * @return true if the grace period is active.
 */
bool grace_is_active(void);

/**
 * @brief Record that a client has completed reclaim.
 *
 * If all tracked clients have reclaimed, the grace period is
 * automatically exited (early termination).
 *
 * @param clientid  Client ID that reclaimed.
 * @return 0 on success, -1 if clientid not in recovery set.
 */
int grace_client_reclaimed(uint64_t clientid);

/**
 * @brief Check if a client is in the recovering set.
 *
 * Used by CLAIM_PREVIOUS validation to reject reclaim attempts
 * from clients that were not confirmed before failover.
 *
 * @param clientid  Client ID to check.
 * @return true if clientid is in the recovery set and hasn't
 *         yet completed reclaim.
 */
bool grace_client_is_recovering(uint64_t clientid);

/**
 * @brief Return the number of clients that have not yet reclaimed.
 * @return Count of pending clients, or 0 if not in grace.
 */
uint32_t grace_pending_count(void);

/**
 * @brief Force-exit the grace period.
 */
void grace_exit(void);

#endif /* GRACE_H */

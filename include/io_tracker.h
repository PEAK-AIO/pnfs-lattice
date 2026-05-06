/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * io_tracker.h -- Per-file I/O frequency tracker for tiering decisions.
 *
 * Aggregates LAYOUTSTATS read/write bytes (primary) and LAYOUTGET
 * bump counts (fallback) per fileid.  The tiering evaluator snapshots
 * and resets the counters each eval interval.
 */

#ifndef IO_TRACKER_H
#define IO_TRACKER_H

#include <stdint.h>

struct io_tracker;

/** Snapshot entry returned by io_tracker_snapshot_and_reset(). */
struct io_tracker_entry {
    uint64_t fileid;
    uint64_t io_bytes;          /**< Accumulated read_bytes + write_bytes. */
    uint32_t layoutget_bumps;   /**< LAYOUTGET fallback counter. */
};

/**
 * Allocate and initialise an I/O tracker.
 *
 * @param initial_capacity  Hint for hash table size (rounded up to power-of-2).
 * @param out               Receives the tracker handle.
 * @return 0 on success, -1 on allocation failure.
 */
int io_tracker_init(uint32_t initial_capacity, struct io_tracker **out);

/**
 * Destroy the tracker and free all resources.
 * @param iot  Tracker (NULL tolerated).
 */
void io_tracker_destroy(struct io_tracker *iot);

/**
 * Record I/O bytes from LAYOUTSTATS (primary signal).
 *
 * Thread-safe (read-lock path with atomic counters).
 *
 * @param iot         Tracker (NULL tolerated -- no-op).
 * @param fileid      File identifier.
 * @param read_bytes  Bytes read since last report.
 * @param write_bytes Bytes written since last report.
 */
void io_tracker_record(struct io_tracker *iot, uint64_t fileid,
                       uint64_t read_bytes, uint64_t write_bytes);

/**
 * Record a LAYOUTGET access (fallback signal).
 *
 * Thread-safe.
 *
 * @param iot     Tracker (NULL tolerated -- no-op).
 * @param fileid  File identifier.
 */
void io_tracker_bump(struct io_tracker *iot, uint64_t fileid);

/**
 * Snapshot all entries and reset counters to zero.
 *
 * The caller receives a malloc'd array of entries and must free() it.
 * Thread-safe (write-lock -- brief exclusive hold).
 *
 * @param iot    Tracker.
 * @param out    Receives malloc'd array (NULL if count == 0).
 * @param count  Receives number of entries.
 * @return 0 on success, -1 on failure.
 */
int io_tracker_snapshot_and_reset(struct io_tracker *iot,
                                  struct io_tracker_entry **out,
                                  uint32_t *count);

#endif /* IO_TRACKER_H */

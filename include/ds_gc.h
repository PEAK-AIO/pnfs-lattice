/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_gc.h -- Background drainer for the catalogue GC queue.
 *
 * The catalogue GC queue collects (fileid, ds_id, nfs_fh) tuples
 * scheduled for cleanup whenever a regular file is finally unlinked
 * (compound op_remove) or relocated (mover_util / rebalance).  The
 * worker started here drains that queue by issuing path-based unlinks
 * via the proxy I/O DS mounts, so DS storage is reclaimed without
 * stalling the unlink RPC on the client.
 *
 * The worker only does best-effort work: every transient error is
 * retried on the next tick, and ENOENT on the DS is treated as
 * success because the GC entry was meant to delete a file that is
 * already gone.  Permanent errors block dequeue so the queue cannot
 * silently lose work -- operators recover by clearing the underlying
 * DS condition (e.g., remounting the DS export) and the next tick
 * picks up where it left off.
 *
 * Concurrency model: a single thread, signalled to stop via an
 * atomic flag plus a self-pipe.  No locking on the queue itself --
 * the catalogue's gc_peek/gc_dequeue API is the synchronisation
 * point.  Run interval is configurable so labs can tune for fast
 * cleanup vs. low background overhead in production.
 */

#ifndef DS_GC_H
#define DS_GC_H

#include <stdint.h>

struct mds_catalogue;
struct mds_proxy_ctx;
struct ds_gc;

/**
 * @brief Start the background DS GC drainer (single-worker form).
 *
 * Convenience wrapper around ds_gc_start_ex() that hard-codes
 * workers=1 and batch_size=256.  Preserved as a source-level
 * compatibility shim for any caller that still passes the legacy
 * three-argument signature.
 *
 * Lifetime: the caller owns @a cat and @a proxy.  Both must remain
 * valid until ds_gc_stop() returns.  On any allocation/thread-create
 * failure the function returns -1 and *out stays NULL -- callers must
 * tolerate the daemon running without a GC drainer (data still gets
 * cleaned up on the next start, since GC entries are persisted).
 *
 * @param cat      Catalogue handle (peek + dequeue).
 * @param proxy    Proxy I/O context (DS mount lookup + unlink).
 * @param poll_ms  Polling interval in milliseconds.  Values < 100
 *                 are clamped to 100 to avoid spinning the thread.
 *                 0 disables the drainer (returns 0, *out = NULL).
 * @param[out] out Receives the worker handle on success.
 * @return 0 on success or disabled (out == NULL), -1 on error.
 */
int ds_gc_start(struct mds_catalogue *cat,
                struct mds_proxy_ctx *proxy,
                uint32_t poll_ms,
                struct ds_gc **out);

/**
 * @brief Start the background DS GC drainer with N parallel workers.
 *
 * Spawns one coordinator thread that batch-peeks @a batch_size GC
 * entries via mds_cat_gc_peek_batch() and feeds them into a bounded
 * queue.  @a workers worker threads pull one entry at a time and run
 * the same per-entry unlink + dequeue logic the single-worker form
 * uses.  The coordinator only refills the queue once the prior batch
 * is fully drained, so workers always see disjoint entries and the
 * scan amortisation cost drops from O(N) per entry to O(N) per batch.
 *
 * Lifetime: the caller owns @a cat and @a proxy.  Both must remain
 * valid until ds_gc_stop() returns.  On any allocation/thread-create
 * failure the function returns -1 and *out stays NULL.
 *
 * @param cat         Catalogue handle (peek_batch + dequeue).
 * @param proxy       Proxy I/O context (DS mount lookup + unlink).
 * @param poll_ms     Inter-tick poll interval.  Values < 100 are
 *                    clamped to 100.  0 disables the drainer.
 * @param workers     Number of worker threads.  Clamped to [1, 32].
 *                    1 reproduces the historical serial drain rate
 *                    while still benefiting from batched peek.
 * @param batch_size  Maximum entries fetched per coordinator tick.
 *                    Clamped to [1, 4096].  Defaults to 256 in the
 *                    legacy ds_gc_start() entry point.
 * @param[out] out    Receives the worker handle on success.
 * @return 0 on success or disabled (out == NULL), -1 on error.
 */
int ds_gc_start_ex(struct mds_catalogue *cat,
                   struct mds_proxy_ctx *proxy,
                   uint32_t poll_ms,
                   uint32_t workers,
                   uint32_t batch_size,
                   struct ds_gc **out);

/**
 * @brief Stop the background drainer and free resources.
 *
 * Signals the worker thread to exit, joins it, and frees @a gc.
 * Safe to call with a NULL handle.  In-flight syscalls (currently a
 * single unlink at a time) are allowed to complete; the wakeup
 * pipe interrupts only the inter-tick sleep.
 */
void ds_gc_stop(struct ds_gc *gc);

#endif /* DS_GC_H */

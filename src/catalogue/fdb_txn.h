/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * fdb_txn.h -- FoundationDB backend handle and transaction runner.
 *
 * Every catalogue slot of the FoundationDB backend is ONE call to
 * fdb_run_txn(): the runner opens a transaction, runs the caller's body
 * (reads, decisions, mutations), commits, and turns every FoundationDB
 * error into exactly one of the catalogue's statuses.  A body never
 * calls back into the caller (contract C2, catalogue_internal.h): it
 * fills a bounded, caller-owned page and the slot delivers that page
 * after fdb_run_txn() returned.
 *
 * Error classification (by numeric code; names may change, codes are
 * stable):
 *   1020 not_committed and everything
 *   fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED)
 *   accepts (1007 transaction_too_old, 1009 future_version, ...)
 *                          definitive abort: the body is re-run after
 *                          fdb_transaction_on_error's backoff, within
 *                          the operation deadline; exhausted -> DELAY
 *   1021 commit_unknown_result
 *                          the commit is no longer in flight; the
 *                          witness read decides landed / not landed
 *   1025 transaction_cancelled, 1031 transaction_timed_out,
 *   1039 cluster_version_changed
 *                          the commit MAY still be in flight; a fence
 *                          transaction decides landed / can never land
 *   anything else          MDS_ERR_IO
 * Deadline exhausted while an outcome is unresolved -> MDS_ERR_INDOUBT.
 *
 * Commit-outcome witness.  Each thread that runs a mutating body owns
 * one WITNESS slot (fdb_keys.h: WITNESS + mds_id + epoch + slot) for
 * its lifetime, with two keys: the witness key K_w, blind-set by every
 * mutating attempt to the attempt's fresh attempt_seq, and the fence
 * anchor K_f = K_w + 0x01 (fdb_key_witness_fence), which is never
 * written and on which every mutating attempt adds a READ and a WRITE
 * conflict range.  The anchor totally orders the attempts of a slot: a
 * commit from an earlier attempt cannot land after a later attempt
 * committed, and a later attempt whose read version predates an
 * earlier landing aborts with 1020 and re-runs.  On 1021 a fresh
 * default transaction (no GRV cache, no causal-read-risky) reads K_w:
 * equal to attempt_seq -> committed, else definitively not committed.
 * On 1025/1031/1039 a fence transaction (read K_w, add a WRITE conflict
 * range on K_f, no mutation) is committed first: committed and saw
 * attempt_seq -> landed; committed and did not -> the attempt can
 * never land (its READ conflict range on K_f now intersects the
 * fence's write) and the body may be re-run; fence failed -> retried
 * within the deadline.
 *
 * The fence only orders against an attempt whose read version precedes
 * the fence's commit: the resolver compares the attempt's read_snapshot
 * with the versions of the writes it conflicts with.  A body that did
 * not read has no read version yet and the client would take one inside
 * the commit -- possibly after the fence, in which case the fenced
 * attempt would still land.  The runner therefore waits for the
 * transaction's read version before it issues the commit (free when the
 * body read: the future is already ready; the commit's own GRV
 * otherwise, so no extra round trip either way).
 *
 * Why a separate, unwritten anchor: fdb_transaction_add_conflict_range
 * goes through the client's read-your-writes layer, which records a
 * READ conflict range only over the parts of the range the transaction
 * has NOT mutated itself (RYWImpl::updateConflictMap keeps unmodified
 * ranges only).  A READ range on the mutated witness key therefore
 * never reaches the resolver and a fence on that key intersects
 * nothing -- verified against libfdb_c 7.3 by tests/unit/test_fdb_txn.c
 * and the fault-injection suite.  Ranges on a key nobody mutates always
 * reach the resolver, in any call order; this is the construction the
 * client itself uses for its self-conflict key (\xFF/SC/<uid>).  The
 * same elision applies to a range the transaction CLEARS, so a body
 * must never clear a range that covers its handle's WITNESS table
 * (catalogue_fdb_keyspace_clear excludes it): the anchor would be
 * inside the cleared range, its READ range would be dropped and the
 * fence could not stop a late landing of that clear.
 *
 * A transaction object whose commit MAY still be in flight (1025/1031/
 * 1039) is never reused: the runner destroys it and creates a fresh one
 * for the re-run, and does the same with a resolver transaction whose
 * round ended that way.  fdb_transaction_reset is only equivalent to
 * destroy + create when no commit actor is outstanding on the object;
 * under the client's own fault injection (CLIENT_BUGGIFY) the delayed
 * in-flight commit still owns the object and a later commit on the
 * reused object trips the client's ASSERT(!committing.isValid())
 * (internal_error 4100).  After 1021 the client guarantees nothing is
 * in flight, so a reset is enough there.
 *
 * The epoch component is the process-wide incarnation stamp taken when
 * the client network starts (CLOCK_REALTIME ns), not the cluster
 * registry's boot_epoch: it is known before the first mutation, so it
 * never changes under an attempt in flight (a clear racing with an
 * unresolved attempt would erase a live witness and cause a double
 * apply), and every handle of one process shares it.  node_register
 * stamps it into the incarnation's NODE_REGISTRY row (fdb_node_val
 * .witness_epoch).  At open the backend range-clears WITNESS + mds_id +
 * [0, min(W, epoch)), W being the epoch the registry row of this
 * mds_id carries: the keys of incarnations superseded before the
 * registered one.  The registered incarnation's keys sit at exactly W
 * and survive -- a daemon taking over a still-running mds_id must not
 * blind that process's in-flight outcome probes, which would re-run
 * landed bodies -- until the id's next restart finds the row gone
 * (clean deregister) or carrying a newer W.  A row without an epoch
 * (0) sweeps nothing; no row sweeps [0, epoch) (catalogue_fdb.c,
 * witness_clear_body).  fdb_backend_set_boot_epoch() records the
 * registry epoch for rows that carry an owner epoch; it does not touch
 * witness keys.
 *
 * Injection.  The fdb_c calls the runner itself makes go through
 * struct fdb_txn_calls (b->calls); tests script commit outcomes and
 * clock values through a mock table without a buggify build.  Bodies
 * call fdb_c directly through the helpers below, except the id
 * allocator's refill body, which is part of this unit and goes through
 * the table too so its commit outcome can be scripted.
 */

#ifndef FDB_TXN_H
#define FDB_TXN_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>

#include "pnfs_mds.h"
#include "fdb_keys.h"

/** Witness slots per process (bounds live witness keys per MDS). */
#define FDB_WITNESS_SLOTS 1024U

/** Default operation deadline / per-attempt timeout (mds.conf overrides). */
#define FDB_OP_DEADLINE_MS_DEFAULT  8000U
#define FDB_TXN_TIMEOUT_MS_DEFAULT  4000U

/** FoundationDB error codes the runner classifies by value. */
#define FDB_ERR_TRANSACTION_TOO_OLD       1007
#define FDB_ERR_NOT_COMMITTED             1020
#define FDB_ERR_COMMIT_UNKNOWN_RESULT     1021
#define FDB_ERR_TRANSACTION_CANCELLED     1025
#define FDB_ERR_TRANSACTION_TIMED_OUT     1031
#define FDB_ERR_CLUSTER_VERSION_CHANGED   1039
#define FDB_ERR_VALUE_TOO_LARGE           2103
/** Stand-in code for a local allocation failure inside a body. */
#define FDB_ERR_PLATFORM_ERROR            1500

/* -----------------------------------------------------------------------
 * Injectable fdb_c call table (runner-internal calls only)
 * ----------------------------------------------------------------------- */

struct fdb_txn_calls {
    fdb_error_t (*create_transaction)(FDBDatabase *db, FDBTransaction **out);
    void        (*transaction_destroy)(FDBTransaction *tr);
    void        (*transaction_reset)(FDBTransaction *tr);
    fdb_error_t (*transaction_set_option)(FDBTransaction *tr, FDBTransactionOption opt,
                                          const uint8_t *value, int value_len);
    void        (*transaction_set)(FDBTransaction *tr, const uint8_t *key, int key_len,
                                   const uint8_t *value, int value_len);
    FDBFuture  *(*transaction_get)(FDBTransaction *tr, const uint8_t *key, int key_len,
                                   fdb_bool_t snapshot);
    FDBFuture  *(*transaction_get_read_version)(FDBTransaction *tr);
    fdb_error_t (*transaction_add_conflict_range)(FDBTransaction *tr, const uint8_t *begin,
                                                  int begin_len, const uint8_t *end,
                                                  int end_len, FDBConflictRangeType type);
    FDBFuture  *(*transaction_commit)(FDBTransaction *tr);
    FDBFuture  *(*transaction_on_error)(FDBTransaction *tr, fdb_error_t err);
    fdb_error_t (*future_block_until_ready)(FDBFuture *f);
    fdb_error_t (*future_get_error)(FDBFuture *f);
    fdb_error_t (*future_get_value)(FDBFuture *f, fdb_bool_t *present,
                                    const uint8_t **value, int *value_len);
    void        (*future_destroy)(FDBFuture *f);
    fdb_bool_t  (*error_predicate)(int predicate, fdb_error_t err);
    /** Monotonic clock in nanoseconds (deadline arithmetic). */
    uint64_t    (*now_ns)(void);
};

/** The real fdb_c table (b->calls == NULL selects it). */
const struct fdb_txn_calls *fdb_txn_calls_real(void);

/* -----------------------------------------------------------------------
 * Statistics (relaxed atomics; exposed through the backend_client_stats
 * slot of the fdb authority table, catalogue_fdb.c)
 * ----------------------------------------------------------------------- */

struct fdb_txn_stats {
    _Atomic uint64_t attempts;         /**< Body executions. */
    _Atomic uint64_t commits;          /**< Commits reported committed. */
    _Atomic uint64_t retries;          /**< Definitive aborts re-run. */
    _Atomic uint64_t unknown_results;  /**< 1021 outcomes resolved. */
    _Atomic uint64_t unknown_landed;   /**< ... of which had landed. */
    _Atomic uint64_t fences;           /**< 1025/1031/1039 fence rounds. */
    _Atomic uint64_t fence_landed;     /**< ... of which proved a landing. */
    _Atomic uint64_t fenced_reruns;    /**< Attempts a fence proved not landed, re-run. */
    _Atomic uint64_t indoubt;          /**< MDS_ERR_INDOUBT returned. */
    _Atomic uint64_t delay_exhausted;  /**< MDS_ERR_DELAY returned. */
    _Atomic uint64_t io_errors;        /**< MDS_ERR_IO returned. */
};

/* -----------------------------------------------------------------------
 * Backend handle (struct mds_catalogue.backend_private)
 * ----------------------------------------------------------------------- */

struct fdb_backend {
    FDBDatabase                *db;
    struct fdb_key_prefix       prefix;
    uint32_t                    mds_id;
    /** Registry boot epoch recorded by the cluster slot (0 until then). */
    _Atomic uint64_t            boot_epoch;
    /** Process-wide incarnation stamp used in WITNESS keys. */
    uint64_t                    witness_epoch;
    uint32_t                    op_deadline_ms;
    uint32_t                    txn_timeout_ms;
    /** Unique per open(); thread-local id pools are keyed on it. */
    uint64_t                    instance_seq;
    struct fdb_txn_stats        stats;
    /** NULL = real fdb_c; tests inject a mock. */
    const struct fdb_txn_calls *calls;
};

/* -----------------------------------------------------------------------
 * Runner
 * ----------------------------------------------------------------------- */

enum fdb_txn_kind {
    FDB_TXN_READONLY = 0, /**< Never committed, no witness. */
    FDB_TXN_MUTATING = 1, /**< Committed with the witness protocol. */
};

/** Body return values; a positive value is an fdb_error_t. */
enum fdb_body_rc {
    FDB_BODY_COMMIT = 0,  /**< Commit (mutating) / done (read-only); return *st_out. */
    FDB_BODY_DONE   = -1, /**< Do not commit; return *st_out (e.g. EXISTS, NOTFOUND). */
};

/**
 * Transaction body.  Runs one attempt: reads, decisions, mutations.
 * Must not call back into the caller, must tolerate being re-run from
 * scratch (every attempt starts from a reset transaction) and must set
 * *st_out before returning FDB_BODY_COMMIT or FDB_BODY_DONE.  An
 * fdb_error_t from a helper is returned as is for classification.
 */
typedef int (*fdb_txn_body)(FDBTransaction *tr, void *ctx, enum mds_status *st_out);

/**
 * Run @p body under the retry / witness protocol described above.
 *
 * @param b     Backend handle.
 * @param kind  Read-only bodies are never committed and skip the witness.
 * @param op    Operation name for the INDOUBT / error log lines.
 * @param body  Transaction body.
 * @param ctx   Passed to @p body.
 * @return The body's status on success; MDS_ERR_DELAY when the deadline
 *         expired after definitive aborts only; MDS_ERR_INDOUBT when the
 *         deadline expired with a commit outcome unresolved;
 *         MDS_ERR_IO for a non-retryable error; MDS_ERR_INVAL for a NULL
 *         argument.
 */
enum mds_status fdb_run_txn(struct fdb_backend *b, enum fdb_txn_kind kind, const char *op,
                            fdb_txn_body body, void *ctx);

/* -----------------------------------------------------------------------
 * Body helpers (real fdb_c).  Every helper returns 0 or an fdb_error_t
 * the body passes back to the runner.
 * ----------------------------------------------------------------------- */

/** Block on @p f and return its error (0 when ready and successful). */
fdb_error_t fdb_txn_wait(FDBFuture *f);

/** Start a point read; NULL only on an unusable key (caller returns
 *  FDB_ERR_PLATFORM_ERROR). */
FDBFuture *fdb_txn_get_start(FDBTransaction *tr, const struct fdb_key *k, bool snapshot);

/**
 * Finish a point read started by fdb_txn_get_start: copy the value into
 * @p buf (at most @p cap bytes; FDB_ERR_VALUE_TOO_LARGE when it does
 * not fit) and destroy the future.  *found reports presence; *len the
 * value length.
 */
fdb_error_t fdb_txn_get_finish(FDBFuture *f, uint8_t *buf, size_t cap, size_t *len,
                               bool *found);

/** fdb_txn_get_start + fdb_txn_get_finish. */
fdb_error_t fdb_txn_get(FDBTransaction *tr, const struct fdb_key *k, bool snapshot,
                        uint8_t *buf, size_t cap, size_t *len, bool *found);

/**
 * Blind set of @p k to the @p len bytes at @p v (copied by the client;
 * the caller keeps ownership of both).  @p k must have been built
 * without overflow (fdb_key_ok).  Applied at commit, never fails here.
 */
void fdb_txn_set(FDBTransaction *tr, const struct fdb_key *k, const uint8_t *v, size_t len);

/**
 * Blind clear of the single key @p k (built without overflow).  Applied
 * at commit; clearing an absent key is a no-op.
 */
void fdb_txn_clear(FDBTransaction *tr, const struct fdb_key *k);

/**
 * Blind clear of the half-open range [r->begin, r->end) (both built
 * without overflow).  A body must never clear a range that covers its
 * own handle's WITNESS table (see the anchor rule above): the fence
 * could not stop a late landing of that clear.
 */
void fdb_txn_clear_range(FDBTransaction *tr, const struct fdb_key_range *r);

/** Set @p k to LE u64 @p v. */
void fdb_txn_set_le64(FDBTransaction *tr, const struct fdb_key *k, uint64_t v);

/** Atomic ADD of the signed @p delta (LE u64 two's complement) to @p k. */
void fdb_txn_add_le64(FDBTransaction *tr, const struct fdb_key *k, int64_t delta);

/**
 * Start a range read of at most @p limit key-values of [r->begin,
 * r->end) in FDB_STREAMING_MODE_EXACT.  NULL on an unusable range.
 */
FDBFuture *fdb_txn_get_range_start(FDBTransaction *tr, const struct fdb_key_range *r,
                                   int limit, bool snapshot, bool reverse);

/**
 * Wait for a range read.  On success *kvs / *count are valid until the
 * caller destroys @p f (fdb_future_destroy) and *more tells whether the
 * range holds further keys beyond the limit.  Read the elements with
 * fdb_kv_at(): the client hands the array out at whatever alignment its
 * arena produced, which can be below the 4-byte packing fdb_c.h
 * declares for struct FDBKeyValue, so field access through the pointer
 * is undefined behaviour (UBSan reports it against a live fdbserver).
 */
fdb_error_t fdb_txn_get_range_wait(FDBFuture *f, const FDBKeyValue **kvs, int *count,
                                   bool *more);

/** Copy element @p i of a key-value array into an aligned struct. */
static inline void fdb_kv_at(const FDBKeyValue *kvs, int i, FDBKeyValue *out)
{
    memcpy(out, (const unsigned char *)kvs + (size_t)i * sizeof(*out), sizeof(*out));
}

/** Apply FDB_TR_OPTION_TIMEOUT (milliseconds) to @p tr. */
fdb_error_t fdb_txn_set_timeout(FDBTransaction *tr, uint32_t ms);

/* -----------------------------------------------------------------------
 * Unique id allocators (META counters, thread-local batches)
 * ----------------------------------------------------------------------- */

/**
 * Hand out one id from the META counter @p counter (FDB_META_FILEID,
 * FDB_META_GC_SEQ, FDB_META_REMOVE_SEQ or FDB_META_COOKIE_SEQ).  Ids
 * are minted in thread-local batches of MDS_FILEID_BATCH: a refill is
 * one transaction that reads the counter (conflicting) and sets it to
 * counter + batch, so two refills of the same counter conflict and one
 * retries -- batches never overlap, ids are unique cluster-wide and a
 * refill costs one round trip per MDS_FILEID_BATCH ids.  A pool is
 * bound to the backend instance that filled it (b->instance_seq); a
 * different instance on the same thread discards the remainder.
 *
 * A refill whose commit outcome the runner could not resolve is
 * reported as MDS_ERR_DELAY, never MDS_ERR_INDOUBT: the batch it may
 * have reserved is never handed out, so ids stay unique (a batch is
 * never reused) and a lost batch is only a gap in the counter, while
 * the caller has committed nothing user-visible yet and may retry.
 *
 * @return MDS_OK; MDS_ERR_IO when the counter is absent (keyspace not
 *         bootstrapped); MDS_ERR_NOSPC when the counter would wrap;
 *         MDS_ERR_DELAY when the refill's deadline expired (definitive
 *         aborts or an unresolved commit); MDS_ERR_IO from the refill
 *         transaction; MDS_ERR_INVAL for a NULL argument or a counter
 *         that is not an allocator.
 */
enum mds_status fdb_backend_alloc_id(struct fdb_backend *b, enum fdb_meta_key counter,
                                     uint64_t *id);

/* -----------------------------------------------------------------------
 * Witness bookkeeping (process-wide; used by the lifecycle code)
 * ----------------------------------------------------------------------- */

/** Set once when the client network starts; read by every backend. */
void fdb_txn_set_witness_epoch(uint64_t epoch);

/** The process-wide witness epoch (0 before the network started). */
uint64_t fdb_txn_witness_epoch(void);

/** Build this thread's witness key; false when the slot pool is
 *  exhausted (the caller returns MDS_ERR_DELAY). */
bool fdb_txn_witness_key(const struct fdb_backend *b, struct fdb_key *k, uint64_t *seq_out);

/** Monotonic nanoseconds (CLOCK_MONOTONIC). */
uint64_t fdb_txn_now_ns(void);

/* -----------------------------------------------------------------------
 * Network-wait accounting (process-wide, monitoring only)
 * ----------------------------------------------------------------------- */

/**
 * Cumulative count of network waits since process start.
 *
 * One "wait" is one blocking point on a future that was NOT yet ready
 * when the calling thread blocked on it (fdb_future_is_ready() false
 * immediately before fdb_future_block_until_ready()).  A wave of reads
 * started in parallel and waited on in sequence therefore counts once
 * when the later futures have arrived by the time the first completes
 * -- the count is the number of dependent round-trip waves the caller
 * actually paid, the FoundationDB analogue of the NDB API's exec_waits.
 * The client folds a transaction's GRV into its first read (or, for a
 * blind write, into the commit), so the GRV hop is never a wait of its
 * own; a commit is one wait; an outcome probe or fence is one wait per
 * round.  Counted at every blocking point of the runner and of the
 * body helpers above when the real fdb_c table is in use; a mock table
 * (tests) is not counted.  Process-wide because the process has one
 * client network; relaxed atomic; never on the hot path beyond one
 * fdb_future_is_ready() per wait.
 */
uint64_t fdb_txn_net_waits(void);

#endif /* FDB_TXN_H */

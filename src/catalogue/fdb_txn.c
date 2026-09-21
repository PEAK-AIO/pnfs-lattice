/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * fdb_txn.c -- FoundationDB transaction runner (see fdb_txn.h).
 *
 * Structure:
 *   - the real fdb_c call table and the body helpers;
 *   - the process-wide witness slot pool (one slot per thread for the
 *     thread's life, released by a pthread key destructor);
 *   - the outcome resolvers: witness read (1021) and fence
 *     (1025/1031/1039), each a fresh default transaction bounded by the
 *     operation deadline;
 *   - fdb_run_txn itself;
 *   - the thread-local id batch allocators over the META counters.
 */

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "fdb_txn.h"
#include "fdb_codec.h"
#include "mds_log.h"

/* A new attempt is only started with this much of the deadline left;
 * less than that goes to outcome resolution, so a budget that is nearly
 * gone yields DELAY (nothing in flight) instead of a fresh attempt that
 * can only time out into a fence. */
#define FDB_MIN_ATTEMPT_MS 200U

#define NS_PER_MS 1000000ULL

/* -----------------------------------------------------------------------
 * Real call table
 * ----------------------------------------------------------------------- */

uint64_t fdb_txn_now_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static const struct fdb_txn_calls g_real_calls = {
    .create_transaction            = fdb_database_create_transaction,
    .transaction_destroy           = fdb_transaction_destroy,
    .transaction_reset             = fdb_transaction_reset,
    .transaction_set_option        = fdb_transaction_set_option,
    .transaction_set               = fdb_transaction_set,
    .transaction_get               = fdb_transaction_get,
    .transaction_get_read_version  = fdb_transaction_get_read_version,
    .transaction_add_conflict_range = fdb_transaction_add_conflict_range,
    .transaction_commit            = fdb_transaction_commit,
    .transaction_on_error          = fdb_transaction_on_error,
    .future_block_until_ready      = fdb_future_block_until_ready,
    .future_get_error              = fdb_future_get_error,
    .future_get_value              = fdb_future_get_value,
    .future_destroy                = fdb_future_destroy,
    .error_predicate               = fdb_error_predicate,
    .now_ns                        = fdb_txn_now_ns,
};

const struct fdb_txn_calls *fdb_txn_calls_real(void)
{
    return &g_real_calls;
}

/* -----------------------------------------------------------------------
 * Network-wait accounting (fdb_txn.h)
 * ----------------------------------------------------------------------- */

static _Atomic uint64_t g_net_waits;

uint64_t fdb_txn_net_waits(void)
{
    return atomic_load_explicit(&g_net_waits, memory_order_relaxed);
}

/* Block on @p f through the real client, counting the wait when the
 * future had not arrived yet (a ready future costs no round trip). */
static fdb_error_t real_block(FDBFuture *f)
{
    if (!fdb_future_is_ready(f)) {
        atomic_fetch_add_explicit(&g_net_waits, 1U, memory_order_relaxed);
    }
    return fdb_future_block_until_ready(f);
}

/* -----------------------------------------------------------------------
 * Body helpers (real fdb_c)
 * ----------------------------------------------------------------------- */

fdb_error_t fdb_txn_wait(FDBFuture *f)
{
    fdb_error_t err;

    if (f == NULL) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    err = real_block(f);
    if (err != 0) {
        return err;
    }
    return fdb_future_get_error(f);
}

FDBFuture *fdb_txn_get_start(FDBTransaction *tr, const struct fdb_key *k, bool snapshot)
{
    if (!fdb_key_ok(k)) {
        return NULL;
    }
    return fdb_transaction_get(tr, k->buf, (int)k->len, snapshot ? 1 : 0);
}

fdb_error_t fdb_txn_get_finish(FDBFuture *f, uint8_t *buf, size_t cap, size_t *len, bool *found)
{
    fdb_error_t err;
    fdb_bool_t present = 0;
    const uint8_t *val = NULL;
    int vlen = 0;

    *len = 0;
    *found = false;
    if (f == NULL) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    err = fdb_txn_wait(f);
    if (err == 0) {
        err = fdb_future_get_value(f, &present, &val, &vlen);
    }
    if (err == 0 && present != 0) {
        if (vlen < 0 || (size_t)vlen > cap) {
            err = FDB_ERR_VALUE_TOO_LARGE;
        } else {
            if (vlen > 0) {
                memcpy(buf, val, (size_t)vlen);
            }
            *len = (size_t)vlen;
            *found = true;
        }
    }
    fdb_future_destroy(f);
    return err;
}

fdb_error_t fdb_txn_get(FDBTransaction *tr, const struct fdb_key *k, bool snapshot,
                        uint8_t *buf, size_t cap, size_t *len, bool *found)
{
    return fdb_txn_get_finish(fdb_txn_get_start(tr, k, snapshot), buf, cap, len, found);
}

void fdb_txn_set(FDBTransaction *tr, const struct fdb_key *k, const uint8_t *v, size_t len)
{
    fdb_transaction_set(tr, k->buf, (int)k->len, v, (int)len);
}

void fdb_txn_clear(FDBTransaction *tr, const struct fdb_key *k)
{
    fdb_transaction_clear(tr, k->buf, (int)k->len);
}

void fdb_txn_clear_range(FDBTransaction *tr, const struct fdb_key_range *r)
{
    fdb_transaction_clear_range(tr, r->begin.buf, (int)r->begin.len,
                                r->end.buf, (int)r->end.len);
}

void fdb_txn_set_le64(FDBTransaction *tr, const struct fdb_key *k, uint64_t v)
{
    uint8_t b[8];

    fdb_le64_put(b, v);
    fdb_txn_set(tr, k, b, sizeof(b));
}

void fdb_txn_add_le64(FDBTransaction *tr, const struct fdb_key *k, int64_t delta)
{
    uint8_t b[8];

    fdb_le64_put(b, (uint64_t)delta);
    fdb_transaction_atomic_op(tr, k->buf, (int)k->len, b, (int)sizeof(b),
                              FDB_MUTATION_TYPE_ADD);
}

FDBFuture *fdb_txn_get_range_start(FDBTransaction *tr, const struct fdb_key_range *r,
                                   int limit, bool snapshot, bool reverse)
{
    if (!fdb_key_ok(&r->begin) || !fdb_key_ok(&r->end) || limit <= 0) {
        return NULL;
    }
    return fdb_transaction_get_range(tr,
                                     FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(r->begin.buf,
                                                                       (int)r->begin.len),
                                     FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(r->end.buf,
                                                                       (int)r->end.len),
                                     limit, 0 /* target_bytes: FDB default */,
                                     FDB_STREAMING_MODE_EXACT, 1 /* iteration */,
                                     snapshot ? 1 : 0, reverse ? 1 : 0);
}

fdb_error_t fdb_txn_get_range_wait(FDBFuture *f, const FDBKeyValue **kvs, int *count,
                                   bool *more)
{
    fdb_error_t err;
    fdb_bool_t more_flag = 0;

    *kvs = NULL;
    *count = 0;
    *more = false;
    if (f == NULL) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    err = fdb_txn_wait(f);
    if (err == 0) {
        err = fdb_future_get_keyvalue_array(f, kvs, count, &more_flag);
    }
    if (err == 0) {
        *more = (more_flag != 0);
    }
    return err;
}

static fdb_error_t set_timeout_with(const struct fdb_txn_calls *c, FDBTransaction *tr,
                                    uint32_t ms)
{
    int64_t v = (int64_t)ms;

    return c->transaction_set_option(tr, FDB_TR_OPTION_TIMEOUT, (const uint8_t *)&v,
                                     (int)sizeof(v));
}

fdb_error_t fdb_txn_set_timeout(FDBTransaction *tr, uint32_t ms)
{
    return set_timeout_with(&g_real_calls, tr, ms);
}

/* -----------------------------------------------------------------------
 * Witness slot pool
 * ----------------------------------------------------------------------- */

struct witness_slot {
    bool     in_use;
    uint64_t attempt_seq; /**< Persists across owners: monotonic per slot. */
};

static pthread_mutex_t     g_slot_lock = PTHREAD_MUTEX_INITIALIZER;
static struct witness_slot g_slots[FDB_WITNESS_SLOTS];
static uint32_t            g_slot_hint;
static pthread_once_t      g_slot_key_once = PTHREAD_ONCE_INIT;
static pthread_key_t       g_slot_key;
static int                 g_slot_key_rc = -1;
static _Atomic uint64_t    g_witness_epoch;
static _Thread_local int32_t tl_slot = -1;

/* pthread key destructor: the value is the owning thread's slot. */
static void slot_release(void *v)
{
    struct witness_slot *s = v;

    if (s == NULL) {
        return;
    }
    (void)pthread_mutex_lock(&g_slot_lock);
    s->in_use = false;
    (void)pthread_mutex_unlock(&g_slot_lock);
}

static void slot_key_init(void)
{
    g_slot_key_rc = pthread_key_create(&g_slot_key, slot_release);
}

/* Acquire (once) this thread's slot; -1 when the pool is exhausted or
 * the key could not be created. */
static int32_t slot_acquire(void)
{
    uint32_t i;
    int32_t found = -1;

    if (tl_slot >= 0) {
        return tl_slot;
    }
    (void)pthread_once(&g_slot_key_once, slot_key_init);
    if (g_slot_key_rc != 0) {
        return -1;
    }
    (void)pthread_mutex_lock(&g_slot_lock);
    for (i = 0; i < FDB_WITNESS_SLOTS; i++) {
        uint32_t idx = (g_slot_hint + i) % FDB_WITNESS_SLOTS;

        if (!g_slots[idx].in_use) {
            g_slots[idx].in_use = true;
            g_slot_hint = (idx + 1U) % FDB_WITNESS_SLOTS;
            found = (int32_t)idx;
            break;
        }
    }
    (void)pthread_mutex_unlock(&g_slot_lock);
    if (found < 0) {
        return -1;
    }
    if (pthread_setspecific(g_slot_key, &g_slots[found]) != 0) {
        slot_release(&g_slots[found]);
        return -1;
    }
    tl_slot = found;
    return found;
}

void fdb_txn_set_witness_epoch(uint64_t epoch)
{
    atomic_store(&g_witness_epoch, epoch);
}

uint64_t fdb_txn_witness_epoch(void)
{
    return atomic_load(&g_witness_epoch);
}

bool fdb_txn_witness_key(const struct fdb_backend *b, struct fdb_key *k, uint64_t *seq_out)
{
    int32_t slot = slot_acquire();

    if (slot < 0) {
        return false;
    }
    /* Only the owning thread touches its slot's sequence while it owns
     * it; the pool mutex orders an owner change. */
    *seq_out = ++g_slots[slot].attempt_seq;
    fdb_key_witness(k, &b->prefix, b->mds_id, b->witness_epoch, (uint32_t)slot);
    return fdb_key_ok(k);
}

/* -----------------------------------------------------------------------
 * Deadline arithmetic
 * ----------------------------------------------------------------------- */

struct run_state {
    struct fdb_backend         *b;
    const struct fdb_txn_calls *c;
    const char                 *op;
    uint64_t                    deadline_ns;
};

static void stat_inc(_Atomic uint64_t *ctr)
{
    atomic_fetch_add_explicit(ctr, 1U, memory_order_relaxed);
}

/* Block on @p f through table @p c: counted like a body wait when the
 * real table is in use; a mock table has no futures on the wire. */
static fdb_error_t calls_block(const struct fdb_txn_calls *c, FDBFuture *f)
{
    if (c == &g_real_calls) {
        return real_block(f);
    }
    return c->future_block_until_ready(f);
}

static fdb_error_t rs_block(const struct run_state *rs, FDBFuture *f)
{
    return calls_block(rs->c, f);
}

/* Conflicting point read of the LE u64 at @p k through table @p c.
 * *found is false when the key is absent or its value is not exactly
 * 8 bytes; *v is only meaningful when *found. */
static fdb_error_t calls_get_le64(const struct fdb_txn_calls *c, FDBTransaction *tr,
                                  const struct fdb_key *k, uint64_t *v, bool *found)
{
    FDBFuture *f;
    fdb_error_t err;
    fdb_bool_t present = 0;
    const uint8_t *val = NULL;
    int vlen = 0;

    *found = false;
    if (!fdb_key_ok(k)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    f = c->transaction_get(tr, k->buf, (int)k->len, 0);
    if (f == NULL) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    err = calls_block(c, f);
    if (err == 0) {
        err = c->future_get_error(f);
    }
    if (err == 0) {
        err = c->future_get_value(f, &present, &val, &vlen);
    }
    if (err == 0 && present != 0 && vlen == 8) {
        *v = fdb_le64_get(val);
        *found = true;
    }
    c->future_destroy(f);
    return err;
}

static uint64_t remaining_ms(const struct run_state *rs)
{
    uint64_t now = rs->c->now_ns();

    if (now >= rs->deadline_ns) {
        return 0;
    }
    return (rs->deadline_ns - now) / NS_PER_MS;
}

/* Per-attempt timeout: the configured value, capped by what is left of
 * the operation deadline (never 0, which would disable the timeout). */
static uint32_t attempt_timeout_ms(const struct run_state *rs)
{
    uint64_t left = remaining_ms(rs);
    uint32_t t = rs->b->txn_timeout_ms;

    if (left < t) {
        t = (uint32_t)left;
    }
    return t == 0 ? 1U : t;
}

static bool is_retryable_abort(const struct run_state *rs, fdb_error_t err)
{
    return err == FDB_ERR_NOT_COMMITTED || err == FDB_ERR_TRANSACTION_TOO_OLD ||
           rs->c->error_predicate(FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED, err) != 0;
}

static bool is_maybe_in_flight(fdb_error_t err)
{
    return err == FDB_ERR_TRANSACTION_CANCELLED || err == FDB_ERR_TRANSACTION_TIMED_OUT ||
           err == FDB_ERR_CLUSTER_VERSION_CHANGED;
}

/* Run on_error for @p err on @p tr: 0 when the transaction was reset
 * and may be retried, else the terminal error. */
static fdb_error_t back_off(const struct run_state *rs, FDBTransaction *tr, fdb_error_t err)
{
    FDBFuture *f = rs->c->transaction_on_error(tr, err);
    fdb_error_t rerr;

    if (f == NULL) {
        return err;
    }
    rerr = rs_block(rs, f);
    if (rerr == 0) {
        rerr = rs->c->future_get_error(f);
    }
    rs->c->future_destroy(f);
    return rerr;
}

/* Commit @p tr and wait for the outcome: 0 = committed, else the
 * commit future's error, to be classified by the caller. */
static fdb_error_t commit_wait(const struct run_state *rs, FDBTransaction *tr)
{
    FDBFuture *cf = rs->c->transaction_commit(tr);
    fdb_error_t err;

    if (cf == NULL) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    err = rs_block(rs, cf);
    if (err == 0) {
        err = rs->c->future_get_error(cf);
    }
    rs->c->future_destroy(cf);
    return err;
}

/* -----------------------------------------------------------------------
 * Outcome resolution
 * ----------------------------------------------------------------------- */

enum outcome {
    OUTCOME_LANDED,
    OUTCOME_NOT_LANDED,
    OUTCOME_UNKNOWN,
};

/* Read the witness key in @p tr2 (conflicting read) and compare with
 * @p seq.  *saw is only meaningful when 0 is returned. */
static fdb_error_t witness_probe(const struct run_state *rs, FDBTransaction *tr2,
                                 const struct fdb_key *wk, uint64_t seq, bool *saw)
{
    uint64_t v = 0;
    bool found = false;
    fdb_error_t err = calls_get_le64(rs->c, tr2, wk, &v, &found);

    *saw = (err == 0 && found && v == seq);
    return err;
}

/* Conflict range [K_f, K_f + 0x00) of @p type on the fence anchor of
 * witness key @p wk.  The anchor is never mutated, so the read-your-
 * writes layer forwards the range unchanged (see fdb_txn.h). */
static fdb_error_t anchor_conflict_range(const struct run_state *rs, FDBTransaction *tr,
                                         const struct fdb_key *wk, FDBConflictRangeType type)
{
    struct fdb_key fk;
    struct fdb_key_range r;

    fdb_key_witness_fence(&fk, wk);
    if (!fdb_key_ok(&fk)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_key_range_single(&r, &fk);
    return rs->c->transaction_add_conflict_range(tr, r.begin.buf, (int)r.begin.len,
                                                 r.end.buf, (int)r.end.len, type);
}

/* Fence in @p tr2 (which already holds the probe read of the witness
 * key): a WRITE conflict range on the fence anchor, no mutation.
 * Committing it intersects the in-flight attempt's READ conflict range
 * on the anchor, so the attempt can no longer commit after this point;
 * the probe's read of the witness key makes the fence itself abort
 * (1020, re-probed) when the attempt landed in between. */
static fdb_error_t fence_commit(const struct run_state *rs, FDBTransaction *tr2,
                                const struct fdb_key *wk)
{
    fdb_error_t err;

    err = anchor_conflict_range(rs, tr2, wk, FDB_CONFLICT_RANGE_TYPE_WRITE);
    if (err == 0) {
        err = commit_wait(rs, tr2);
    }
    stat_inc(&rs->b->stats.fences);
    return err;
}

/* Replace *trp with a fresh transaction object.  Used after an error
 * that leaves a commit possibly in flight on the old object: the old
 * object stays owned by that commit (see fdb_txn.h), so the next round
 * must not touch it.  False when the client cannot create one; *trp is
 * NULL then and the caller stops. */
static bool recreate_transaction(const struct run_state *rs, FDBTransaction **trp)
{
    rs->c->transaction_destroy(*trp);
    *trp = NULL;
    if (rs->c->create_transaction(rs->b->db, trp) != 0 || *trp == NULL) {
        *trp = NULL;
        return false;
    }
    return true;
}

/* A probe or fence round failed with @p err.  A conflict backs off
 * through on_error; an unknown fence outcome (1021: nothing in flight)
 * resets the transaction; a timeout, a cancel or a version change may
 * leave the fence commit in flight, so the object is replaced.  The
 * next round re-probes either way, so a fence that did land is seen.
 * True when the round may be repeated; false on a terminal error. */
static bool resolve_round_retryable(const struct run_state *rs, FDBTransaction **tr2p,
                                    fdb_error_t err)
{
    if (is_retryable_abort(rs, err)) {
        return back_off(rs, *tr2p, err) == 0;
    }
    if (err == FDB_ERR_COMMIT_UNKNOWN_RESULT) {
        rs->c->transaction_reset(*tr2p);
        return true;
    }
    if (is_maybe_in_flight(err)) {
        return recreate_transaction(rs, tr2p);
    }
    return false;
}

/*
 * Resolve an attempt whose commit came back 1021 (no longer in flight)
 * or 1025/1031/1039 (maybe still in flight).  The read alone decides a
 * landing; a non-landing needs the fence commit when the attempt may
 * still be in flight.  Every round uses a fresh default transaction
 * (no GRV cache, no causal-read-risky) so its read version is causally
 * after any landed commit.  Bounded by the operation deadline; a
 * terminal error leaves the outcome unknown.
 */
static enum outcome resolve_outcome(const struct run_state *rs, const struct fdb_key *wk,
                                    uint64_t seq, bool need_fence)
{
    FDBTransaction *tr2 = NULL;
    enum outcome out = OUTCOME_UNKNOWN;

    if (rs->c->create_transaction(rs->b->db, &tr2) != 0) {
        return OUTCOME_UNKNOWN;
    }
    while (remaining_ms(rs) > 0) {
        bool saw = false;
        fdb_error_t err = set_timeout_with(rs->c, tr2, attempt_timeout_ms(rs));

        if (err == 0) {
            err = witness_probe(rs, tr2, wk, seq, &saw);
        }
        if (err == 0 && !saw && need_fence) {
            err = fence_commit(rs, tr2, wk);
        }
        if (err == 0) {
            out = saw ? OUTCOME_LANDED : OUTCOME_NOT_LANDED;
            break;
        }
        if (!resolve_round_retryable(rs, &tr2, err)) {
            break;
        }
    }
    if (tr2 != NULL) {
        rs->c->transaction_destroy(tr2);
    }
    return out;
}

/* -----------------------------------------------------------------------
 * Runner
 * ----------------------------------------------------------------------- */

static enum mds_status finish_indoubt(const struct run_state *rs, fdb_error_t err)
{
    stat_inc(&rs->b->stats.indoubt);
    MDS_LOG_ERROR(LOG_COMP_CAT,
                  "fdb %s: commit outcome unresolved within %u ms (last error %d %s): "
                  "MDS_ERR_INDOUBT", rs->op, rs->b->op_deadline_ms, (int)err,
                  fdb_get_error(err));
    return MDS_ERR_INDOUBT;
}

static enum mds_status finish_delay(const struct run_state *rs)
{
    stat_inc(&rs->b->stats.delay_exhausted);
    MDS_LOG_WARN(LOG_COMP_CAT, "fdb %s: deadline (%u ms) exhausted after definitive "
                 "aborts: MDS_ERR_DELAY", rs->op, rs->b->op_deadline_ms);
    return MDS_ERR_DELAY;
}

static enum mds_status finish_io(const struct run_state *rs, fdb_error_t err)
{
    stat_inc(&rs->b->stats.io_errors);
    MDS_LOG_ERROR(LOG_COMP_CAT, "fdb %s: error %d (%s): MDS_ERR_IO", rs->op, (int)err,
                  fdb_get_error(err));
    return MDS_ERR_IO;
}

/* Wait for @p tr's read version so the commit request carries a
 * read_snapshot that precedes any later fence (fdb_txn.h).  Already
 * ready when the body read; the commit's own GRV otherwise. */
static fdb_error_t pin_read_version(const struct run_state *rs, FDBTransaction *tr)
{
    FDBFuture *f = rs->c->transaction_get_read_version(tr);
    fdb_error_t err;

    if (f == NULL) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    err = rs_block(rs, f);
    if (err == 0) {
        err = rs->c->future_get_error(f);
    }
    rs->c->future_destroy(f);
    return err;
}

/* Commit @p tr with the witness protocol.  Returns the status to hand
 * back, or MDS_OK with *rerun set when the body must be run again.
 * *issued reports whether the commit was handed to the client: a
 * failure before that point (conflict-range or read-version error) is
 * a definitive failure of the attempt with nothing in flight. */
static enum mds_status commit_attempt(const struct run_state *rs, FDBTransaction *tr,
                                      enum mds_status body_st, bool *rerun, bool *issued,
                                      fdb_error_t *err_out)
{
    struct fdb_key wk;
    uint64_t seq = 0;
    uint8_t v[8];
    fdb_error_t err;
    enum outcome out;

    *rerun = false;
    *issued = false;
    if (!fdb_txn_witness_key(rs->b, &wk, &seq)) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb %s: witness slot pool exhausted (%u slots)",
                      rs->op, FDB_WITNESS_SLOTS);
        stat_inc(&rs->b->stats.delay_exhausted);
        *err_out = 0;
        return MDS_ERR_DELAY;
    }
    /* The witness value for the probe, and the READ + WRITE conflict
     * ranges on the unwritten anchor that let a fence stop this attempt
     * and order it against the slot's other attempts.  A READ range on
     * the witness key itself would be dropped by the read-your-writes
     * layer because this transaction mutates that key (fdb_txn.h). */
    fdb_le64_put(v, seq);
    rs->c->transaction_set(tr, wk.buf, (int)wk.len, v, (int)sizeof(v));
    err = anchor_conflict_range(rs, tr, &wk, FDB_CONFLICT_RANGE_TYPE_READ);
    if (err == 0) {
        err = anchor_conflict_range(rs, tr, &wk, FDB_CONFLICT_RANGE_TYPE_WRITE);
    }
    if (err != 0) {
        *err_out = err;
        return MDS_ERR_IO;
    }
    /* Nothing is committed yet: a failure here is a definitive failure
     * of the attempt, which the caller classifies like a body error. */
    err = pin_read_version(rs, tr);
    if (err != 0) {
        *err_out = err;
        return MDS_ERR_IO;
    }
    *issued = true;
    err = commit_wait(rs, tr);
    *err_out = err;
    if (err == 0) {
        stat_inc(&rs->b->stats.commits);
        return body_st;
    }
    if (err != FDB_ERR_COMMIT_UNKNOWN_RESULT && !is_maybe_in_flight(err)) {
        return MDS_ERR_IO; /* the caller classifies retryable aborts */
    }
    if (err == FDB_ERR_COMMIT_UNKNOWN_RESULT) {
        stat_inc(&rs->b->stats.unknown_results);
    }
    out = resolve_outcome(rs, &wk, seq, err != FDB_ERR_COMMIT_UNKNOWN_RESULT);
    if (out == OUTCOME_LANDED) {
        stat_inc(err == FDB_ERR_COMMIT_UNKNOWN_RESULT ? &rs->b->stats.unknown_landed :
                                                        &rs->b->stats.fence_landed);
        stat_inc(&rs->b->stats.commits);
        return body_st;
    }
    if (out == OUTCOME_NOT_LANDED) {
        if (err != FDB_ERR_COMMIT_UNKNOWN_RESULT) {
            stat_inc(&rs->b->stats.fenced_reruns);
        }
        *rerun = true;
        return MDS_OK;
    }
    return finish_indoubt(rs, err);
}

enum attempt_action {
    ATTEMPT_RETURN, /**< *result is final. */
    ATTEMPT_RERUN,  /**< Transaction reset or replaced; run the body again. */
    ATTEMPT_FAILED, /**< *err is this attempt's definitive failure. */
};

/* One attempt: arm the timeout, run the body, commit when asked.  *trp
 * is replaced when the attempt's commit may still be in flight after a
 * not-landed resolution (see fdb_txn.h); NULL afterwards means the
 * replacement failed and *err is terminal. */
static enum attempt_action run_attempt(const struct run_state *rs, FDBTransaction **trp,
                                       enum fdb_txn_kind kind, fdb_txn_body body, void *ctx,
                                       enum mds_status *result, fdb_error_t *err)
{
    FDBTransaction *tr = *trp;
    enum mds_status st = MDS_ERR_IO;
    bool rerun = false;
    bool issued = false;
    int rc;

    *err = set_timeout_with(rs->c, tr, attempt_timeout_ms(rs));
    if (*err != 0) {
        return ATTEMPT_FAILED;
    }
    stat_inc(&rs->b->stats.attempts);
    rc = body(tr, ctx, &st);
    if (rc == FDB_BODY_DONE || (rc == FDB_BODY_COMMIT && kind == FDB_TXN_READONLY)) {
        *result = st;
        return ATTEMPT_RETURN;
    }
    if (rc != FDB_BODY_COMMIT) {
        *err = (fdb_error_t)rc;
        return ATTEMPT_FAILED;
    }
    *result = commit_attempt(rs, tr, st, &rerun, &issued, err);
    if (rerun) {
        if (is_maybe_in_flight(*err)) {
            /* Fenced, never landing -- but the client-side commit actor
             * may still own this object: replace it. */
            if (!recreate_transaction(rs, trp)) {
                *err = FDB_ERR_PLATFORM_ERROR;
                return ATTEMPT_FAILED;
            }
        } else {
            rs->c->transaction_reset(tr); /* 1021: nothing in flight */
        }
        return ATTEMPT_RERUN;
    }
    if (*result == MDS_ERR_DELAY) {
        return ATTEMPT_RETURN; /* witness slot pool exhausted */
    }
    if (!issued) {
        return ATTEMPT_FAILED; /* nothing in flight: classified like a body error */
    }
    /* Committed, resolved (INDOUBT), or an outcome the witness protocol
     * has already settled: final.  Anything else is a definitive failure
     * of the commit, classified like a body error. */
    if (*err == 0 || *result == MDS_ERR_INDOUBT || *err == FDB_ERR_COMMIT_UNKNOWN_RESULT ||
        is_maybe_in_flight(*err)) {
        return ATTEMPT_RETURN;
    }
    return ATTEMPT_FAILED;
}

/* @p *err is a definitive failure of an attempt (nothing was handed to
 * the commit path, or the commit itself aborted definitively): retry
 * the retryable ones through on_error's backoff; a timeout / cancel /
 * version change that hit a READ is equally definitive and only needs
 * a reset.  True when the attempt may be re-run; false when *err (the
 * backoff's error when that failed) is terminal. */
static bool attempt_failure_retryable(const struct run_state *rs, FDBTransaction *tr,
                                      fdb_error_t *err)
{
    if (is_retryable_abort(rs, *err)) {
        fdb_error_t rerr = back_off(rs, tr, *err);

        if (rerr != 0) {
            *err = rerr;
            return false;
        }
    } else if (is_maybe_in_flight(*err)) {
        rs->c->transaction_reset(tr);
    } else {
        return false;
    }
    stat_inc(&rs->b->stats.retries);
    return true;
}

enum mds_status fdb_run_txn(struct fdb_backend *b, enum fdb_txn_kind kind, const char *op,
                            fdb_txn_body body, void *ctx)
{
    struct run_state rs;
    FDBTransaction *tr = NULL;
    enum mds_status result = MDS_ERR_IO;
    fdb_error_t err;

    if (b == NULL || body == NULL) {
        return MDS_ERR_INVAL;
    }
    rs.b = b;
    rs.c = (b->calls != NULL) ? b->calls : &g_real_calls;
    rs.op = (op != NULL) ? op : "?";
    rs.deadline_ns = rs.c->now_ns() + (uint64_t)b->op_deadline_ms * NS_PER_MS;

    err = rs.c->create_transaction(b->db, &tr);
    if (err != 0 || tr == NULL) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb %s: create_transaction: %d %s", rs.op, (int)err,
                      fdb_get_error(err));
        stat_inc(&b->stats.io_errors);
        return MDS_ERR_IO;
    }

    for (;;) {
        enum attempt_action act;

        if (remaining_ms(&rs) < FDB_MIN_ATTEMPT_MS) {
            result = finish_delay(&rs);
            break;
        }
        act = run_attempt(&rs, &tr, kind, body, ctx, &result, &err);
        if (act == ATTEMPT_RETURN) {
            break;
        }
        if (act == ATTEMPT_FAILED &&
            (tr == NULL || !attempt_failure_retryable(&rs, tr, &err))) {
            result = finish_io(&rs, err);
            break;
        }
    }
    if (tr != NULL) {
        rs.c->transaction_destroy(tr);
    }
    return result;
}

/* -----------------------------------------------------------------------
 * Id allocators
 * ----------------------------------------------------------------------- */

struct id_pool {
    uint64_t instance_seq;
    uint64_t next;
    uint32_t remaining;
};

#define ID_POOL_COUNT ((unsigned)FDB_META_COOKIE_SEQ + 1U)

static _Thread_local struct id_pool tl_pools[ID_POOL_COUNT];

struct refill_ctx {
    const struct fdb_txn_calls *c;
    struct fdb_key              key;
    uint64_t                    first; /**< First id of the reserved batch. */
};

/* Reserve the next batch: read the counter (conflicting, so two refills
 * of one counter serialise) and advance it.  Unlike other bodies this
 * one goes through the call table, so a test can script the refill's
 * commit outcome (fdb_txn.h, Injection). */
static int refill_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct refill_ctx *rc = arg;
    uint8_t v[8];
    bool found = false;
    uint64_t cur = 0;
    fdb_error_t err;

    err = calls_get_le64(rc->c, tr, &rc->key, &cur, &found);
    if (err != 0) {
        return (int)err;
    }
    if (!found) {
        *st_out = MDS_ERR_IO; /* counter absent or corrupt: not bootstrapped */
        return FDB_BODY_DONE;
    }
    if (cur > UINT64_MAX - MDS_FILEID_BATCH) {
        *st_out = MDS_ERR_NOSPC;
        return FDB_BODY_DONE;
    }
    fdb_le64_put(v, cur + MDS_FILEID_BATCH);
    rc->c->transaction_set(tr, rc->key.buf, (int)rc->key.len, v, (int)sizeof(v));
    rc->first = cur + 1U;
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/*
 * Reserve a fresh batch for @p pool.  A refill whose commit outcome
 * stayed unresolved is reported as MDS_ERR_DELAY, not INDOUBT: the
 * batch it may have reserved is never handed out, ids stay unique
 * because a batch is never reused (the counter only advances and the
 * next refill reads it back), so a lost batch is only a gap in a
 * monotonic counter -- and the caller's operation has committed nothing
 * user-visible yet, so it is safe to retry, which INDOUBT (NFS4ERR_IO
 * at the protocol) would deny it.
 */
static enum mds_status pool_refill(struct fdb_backend *b, enum fdb_meta_key counter,
                                   struct id_pool *pool)
{
    struct refill_ctx rc;
    enum mds_status st;

    memset(&rc, 0, sizeof(rc));
    rc.c = (b->calls != NULL) ? b->calls : &g_real_calls;
    fdb_key_meta(&rc.key, &b->prefix, counter);
    st = fdb_run_txn(b, FDB_TXN_MUTATING, "alloc_id", refill_body, &rc);
    if (st == MDS_ERR_INDOUBT) {
        MDS_LOG_WARN(LOG_COMP_CAT, "fdb alloc_id: refill outcome unresolved, the batch is "
                     "abandoned (a gap in counter %u): MDS_ERR_DELAY", (unsigned)counter);
        return MDS_ERR_DELAY;
    }
    if (st != MDS_OK) {
        return st;
    }
    pool->next = rc.first;
    pool->remaining = MDS_FILEID_BATCH;
    return MDS_OK;
}

enum mds_status fdb_backend_alloc_id(struct fdb_backend *b, enum fdb_meta_key counter,
                                     uint64_t *id)
{
    struct id_pool *pool;
    unsigned idx = (unsigned)counter;

    if (b == NULL || id == NULL || idx >= ID_POOL_COUNT || counter == FDB_META_SCHEMA_VERSION) {
        return MDS_ERR_INVAL;
    }
    pool = &tl_pools[idx];
    if (pool->instance_seq != b->instance_seq) {
        /* A different backend instance filled this pool: its ids belong
         * to another keyspace, discard the remainder. */
        pool->instance_seq = b->instance_seq;
        pool->remaining = 0;
    }
    if (pool->remaining == 0) {
        enum mds_status st = pool_refill(b, counter, pool);

        if (st != MDS_OK) {
            return st;
        }
    }
    *id = pool->next;
    pool->next++;
    pool->remaining--;
    return MDS_OK;
}

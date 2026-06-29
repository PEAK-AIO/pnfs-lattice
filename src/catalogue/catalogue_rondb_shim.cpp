/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: GPL-2.0-or-later
 *
 * catalogue_rondb_shim.cpp -- C++ shim hiding NDB API behind C ABI.
 *
 * This file is the ONLY C++ translation unit in the project.
 * It compiles into librondb_shim.a and exports extern "C" functions
 * consumed by catalogue_rondb.c.
 *
 * Current scope (RonDB 24.10.x baseline for the Stage 4 spike branch):
 *   - connect to the cluster via Ndb_cluster_connection
 *   - create/drop a dedicated probe table via NdbDictionary
 *   - run a simple canary write/read probe against that table
 *
 * It does not implement namespace semantics or daemon request paths.
 */

#ifdef HAVE_RONDB

#include <ndbapi/NdbApi.hpp>

#include <algorithm>
#include <condition_variable>
#include <unordered_map>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <mutex>
#include <new>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

extern "C" {
#include "catalogue_rondb.h"
#include "mds_coordination.h"
#include "rondb_schema.h"
#include "quota.h"         /* struct mds_quota_rule, mds_quota_usage */
}

namespace {

/* Number of parallel NDB cluster connections.  Each connection has
 * its own TCP socket to the NDB cluster, eliminating TCP-level
 * serialization between worker threads.  Threads are assigned to
 * connections round-robin via atomic counter. */
/* Maximum NDB connections per MDS.  Actual count is min of this
 * and the pool_size passed to rondb_shim_connect().  Default 4;
 * tunable via ndb_conn_pool_size in mds.conf. */
static constexpr int NDB_CONN_POOL_MAX = 64;

/** Maximum retry attempts for NDB transient errors (lock contention,
 *  temporary resource exhaustion, node recovery). */
static constexpr int NDB_RETRY_MAX = 3;

/** Backoff delay between retries (microseconds). */
static constexpr int NDB_RETRY_DELAY_US = 500;

/**
 * Check if an NDB error is transient and should be retried.
 *
 * Retryable classifications per NDB API documentation:
 *   - TemporaryResourceError (class 4): lock wait timeout, out of ops
 *   - OverloadError (class 6): cluster overloaded
 *   - TimeoutExpired (class 7): transaction timeout
 *   - NodeRecoveryError (class 8): node failover in progress
 */
static bool rondb_is_temporary(const NdbError &err)
{
    switch (err.classification) {
    case NdbError::TemporaryResourceError:
    case NdbError::OverloadError:
    case NdbError::TimeoutExpired:
    case NdbError::NodeRecoveryError:
        return true;
    default:
        return false;
    }
}

/* -----------------------------------------------------------------------
 * Phase 3: NDB Async Batch Pipeline -- data structures
 *
 * Each NDB connection gets a shared Ndb object driven by a dedicated
 * flush thread.  Worker threads define operations under a mutex, call
 * executeAsynchPrepare(), then release the mutex and block until the
 * flush thread delivers the result via sendPreparedTransactions() +
 * pollNdb().  This batches multiple workers' transactions into fewer
 * TCP segments, dramatically improving throughput.
 * ----------------------------------------------------------------------- */

/** Per-operation result passed through the NDB async callback. */
struct ndb_async_result {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    done;
    int                     error;

    ndb_async_result() : done(false), error(0) {}
};

/** Per-connection async context: shared Ndb + flush thread. */
struct ndb_conn_ctx {
    Ndb                     *ndb;         /* shared Ndb for async ops    */
    std::mutex               ndb_mutex;   /* serializes all Ndb access   */
    std::thread              flush_thread;
    std::atomic<bool>        stop;
    std::atomic<int>         in_flight;   /* prepared, not yet completed */
    std::condition_variable  wake_cv;     /* wakes flush thread on work  */

    ndb_conn_ctx() : ndb(nullptr), stop(false), in_flight(0) {}
};

struct rondb_shim_handle {
    Ndb_cluster_connection *connections[NDB_CONN_POOL_MAX];
    int                     conn_count;  /* actual connections created */
    char                    schema[64];
    std::atomic<uint32_t>   conn_rr{0};  /* round-robin counter */

    /*
     * Phase 4: when true, future async-aware shim entrypoints
     * (rondb_shim_ns_create_async etc.) are eligible to route
     * through the rondb_async_exec batch pipeline.  The flag is
     * plumbed now so operators can opt in via mds.conf
     * (ndb_async_writes=true); the actual dispatch lands in a
     * follow-up once concurrent-workload measurement justifies it.
     */
    std::atomic<bool>       async_writes{false};

    /* Thread-local Ndb pool: each thread gets its own Ndb object
     * lazily created from one of the pooled connections.  The pool
     * vector tracks all allocated Ndb objects for cleanup. */
    std::mutex              pool_mutex;
    std::vector<Ndb *>      pool_all;

    /* Legacy accessor for code that uses ->connection directly. */
    Ndb_cluster_connection *connection;   /* alias for connections[0] */

    /* Phase 3: per-connection async contexts (flush threads). */
    ndb_conn_ctx            conn_ctxs[NDB_CONN_POOL_MAX];

    /* Liveness generation for thread-local Ndb caching.  Each handle
     * instance is stamped with a globally unique generation at
     * creation; rondb_destroy_handle re-stamps it before deleting the
     * pooled Ndb objects, so cached tl_ndb pointers recorded by OTHER
     * threads (which remember the generation they cached under) are
     * invalidated instead of dangling. */
    std::atomic<uint64_t>   generation{0};
};

/* Source of unique handle generations (never reused across handles). */
static std::atomic<uint64_t> g_rondb_handle_generation{1};

/* Thread-local Ndb pointer -- one per calling thread, lazily created. */
static thread_local Ndb *tl_ndb = nullptr;
static thread_local rondb_shim_handle *tl_ndb_owner = nullptr;
static thread_local uint64_t tl_ndb_gen = 0;

/**
 * Return a thread-local Ndb object for the given handle.
 * Creates one on first call from each thread.
 *
 * Each thread is assigned to a connection from the pool via
 * round-robin.  This distributes NDB traffic across multiple
 * TCP sockets, eliminating the single-connection bottleneck.
 */
static Ndb *rondb_get_ndb(rondb_shim_handle *state)
{
    if (state == nullptr || state->conn_count == 0) {
        return nullptr;
    }

    /* Fast path: already have an Ndb for this thread + handle.  The
     * cached pointer is valid only if BOTH the owner pointer AND the
     * recorded generation match: a destroyed handle (or a new handle
     * reusing the same address) carries a different generation, so a
     * stale tl_ndb is re-created instead of dereferenced. */
    if (tl_ndb != nullptr && tl_ndb_owner == state &&
        tl_ndb_gen == state->generation.load(std::memory_order_acquire)) {
        return tl_ndb;
    }

    /* Slow path: pick a connection via round-robin and create Ndb. */
    uint32_t idx = state->conn_rr.fetch_add(1, std::memory_order_relaxed)
                   % (uint32_t)state->conn_count;
    Ndb_cluster_connection *conn = state->connections[idx];

    Ndb *ndb = new (std::nothrow) Ndb(conn, state->schema);
    if (ndb == nullptr) {
        return nullptr;
    }
    if (ndb->init() != 0) {
        std::fprintf(stderr,
            "ERROR: Ndb::init() failed for thread %lu conn[%u]: "
            "code=%d msg=%s\n",
            (unsigned long)std::hash<std::thread::id>{}(
                std::this_thread::get_id()),
            idx,
            ndb->getNdbError().code,
            ndb->getNdbError().message);
        delete ndb;
        return nullptr;
    }

    /* Register for cleanup. */
    {
        std::lock_guard<std::mutex> lock(state->pool_mutex);
        state->pool_all.push_back(ndb);
    }

    tl_ndb = ndb;
    tl_ndb_owner = state;
    tl_ndb_gen = state->generation.load(std::memory_order_acquire);
    return ndb;
};

static std::mutex g_rondb_runtime_mutex;
static bool       g_rondb_runtime_ready = false;
static bool       g_rondb_runtime_registered = false;

static const char *k_probe_table = "pnfs_mds_probe";
static const char *k_probe_id_col = "ATTR1";
static const char *k_probe_value_col = "ATTR2";
static const unsigned int k_probe_row_id = 1U;

static void rondb_runtime_shutdown(void)
{
    bool should_shutdown = false;

    {
        std::lock_guard<std::mutex> lock(g_rondb_runtime_mutex);
        should_shutdown = g_rondb_runtime_ready;
        g_rondb_runtime_ready = false;
    }

    if (should_shutdown) {
        ndb_end(0);
    }
}

static int rondb_runtime_init(void)
{
    std::lock_guard<std::mutex> lock(g_rondb_runtime_mutex);

    if (g_rondb_runtime_ready) {
        return 0;
    }

    if (ndb_init() != 0) {
        std::fprintf(stderr, "ERROR: ndb_init() failed\n");
        return -1;
    }

    g_rondb_runtime_ready = true;
    if (!g_rondb_runtime_registered) {
        if (std::atexit(rondb_runtime_shutdown) == 0) {
            g_rondb_runtime_registered = true;
        }
    }

    return 0;
}

static void rondb_destroy_handle(rondb_shim_handle *state)
{
    if (state == nullptr) {
        return;
    }

    /* Phase 3: stop flush threads and destroy async Ndb objects. */
    for (int ci = 0; ci < state->conn_count; ci++) {
        ndb_conn_ctx *ctx = &state->conn_ctxs[ci];
        if (ctx->ndb != nullptr) {
            ctx->stop.store(true, std::memory_order_release);
            ctx->wake_cv.notify_one();
            if (ctx->flush_thread.joinable()) {
                ctx->flush_thread.join();
            }
            delete ctx->ndb;
            ctx->ndb = nullptr;
        }
    }

    /* Invalidate every thread's cached tl_ndb for this handle BEFORE
     * deleting the pooled Ndb objects: rondb_get_ndb compares each
     * thread's recorded generation against this counter, so threads
     * other than the caller re-create their Ndb instead of using a
     * dangling pointer.  A fresh unique value (not +1) guarantees no
     * later handle can ever match a stale recorded generation. */
    state->generation.store(
        g_rondb_handle_generation.fetch_add(1, std::memory_order_relaxed),
        std::memory_order_release);

    /* Delete all thread-local Ndb objects from the pool. */
    {
        std::lock_guard<std::mutex> lock(state->pool_mutex);
        for (Ndb *ndb : state->pool_all) {
            delete ndb;
        }
        state->pool_all.clear();
    }

    /* Clear thread-local pointers if they reference this handle. */
    if (tl_ndb_owner == state) {
        tl_ndb = nullptr;
        tl_ndb_owner = nullptr;
        tl_ndb_gen = 0;
    }

    /* Destroy all pooled connections. */
    for (int i = 0; i < state->conn_count; i++) {
        delete state->connections[i];
        state->connections[i] = nullptr;
    }
    state->connection = nullptr;
    state->conn_count = 0;
    delete state;
}

static int rondb_report_error(const NdbError &err, const char *context)
{
    std::fprintf(stderr,
        "ERROR: %s failed: code=%d class=%d msg=%s\n",
        context != nullptr ? context : "RonDB operation",
        err.code,
        static_cast<int>(err.classification),
        err.message != nullptr ? err.message : "(null)");
    return -1;
}

static rondb_shim_handle *rondb_checked_handle(void *handle, const char *schema)
{
    rondb_shim_handle *state = static_cast<rondb_shim_handle *>(handle);

    if (state == nullptr || state->connection == nullptr) {
        std::fprintf(stderr, "ERROR: RonDB shim handle is not initialized\n");
        return nullptr;
    }
    if (schema != nullptr && schema[0] != '\0' &&
        std::strcmp(state->schema, schema) != 0) {
        std::fprintf(stderr,
            "ERROR: RonDB shim schema mismatch: handle=%s requested=%s\n",
            state->schema, schema);
        return nullptr;
    }

    return state;
}

static NdbDictionary::Dictionary *rondb_get_dictionary(rondb_shim_handle *state)
{
    Ndb *ndb = rondb_get_ndb(state);
    NdbDictionary::Dictionary *dict;

    if (ndb == nullptr) {
        return nullptr;
    }

    dict = ndb->getDictionary();
    if (dict == nullptr) {
        (void)rondb_report_error(ndb->getNdbError(), "getDictionary()");
    }
    return dict;
}

/* -----------------------------------------------------------------------
 * Phase 3: NDB Async Batch Pipeline -- callback, flush thread, helpers
 * ----------------------------------------------------------------------- */

/** Async callback invoked by pollNdb() when a transaction completes. */
static void ndb_async_callback(int result, NdbTransaction *, void *arg)
{
    ndb_async_result *r = static_cast<ndb_async_result *>(arg);
    {
        std::lock_guard<std::mutex> lock(r->mtx);
        r->error = result;
        r->done = true;
    }
    r->cv.notify_one();
}

/** Flush thread: send prepared transactions + poll completions. */
static void ndb_flush_thread_fn(ndb_conn_ctx *ctx)
{
    while (!ctx->stop.load(std::memory_order_acquire)) {
        std::unique_lock<std::mutex> lock(ctx->ndb_mutex);

        /* Sleep when idle to avoid CPU spinning. */
        if (ctx->in_flight.load(std::memory_order_acquire) == 0) {
            ctx->wake_cv.wait_for(lock, std::chrono::milliseconds(10),
                [ctx] {
                    return ctx->in_flight.load(std::memory_order_acquire) > 0
                        || ctx->stop.load(std::memory_order_acquire);
                });
            if (ctx->stop.load(std::memory_order_acquire)) {
                break;
            }
        }

        /* Batch-send all prepared transactions in one TCP segment. */
        ctx->ndb->sendPreparedTransactions(1);

        /* Poll for completions -- triggers ndb_async_callback which
         * signals waiting worker threads via their condvars.
         * Timeout 1 ms, return as soon as >= 1 completes. */
        ctx->ndb->pollNdb(1, 1);
    }

    /* Drain any in-flight transactions on shutdown. */
    {
        std::unique_lock<std::mutex> lock(ctx->ndb_mutex);
        ctx->ndb->sendPreparedTransactions(1);
        ctx->ndb->pollNdb(3000, 0);
    }
}

/* Thread-local connection index for async operations (sticky). */
static thread_local int tl_async_conn_idx = -1;

/**
 * Acquire the per-connection shared Ndb for async operations.
 *
 * Locks the connection's mutex and returns the shared Ndb pointer.
 * Each thread is assigned to a connection round-robin (sticky).
 * Caller MUST call rondb_async_unlock() when done.
 *
 * @return Ndb pointer (caller holds ndb_mutex), or nullptr on error.
 */
static Ndb *rondb_async_lock(rondb_shim_handle *state, int *conn_idx_out)
{
    if (state == nullptr || state->conn_count == 0) {
        return nullptr;
    }
    if (tl_async_conn_idx < 0 || tl_async_conn_idx >= state->conn_count) {
        tl_async_conn_idx = (int)(
            state->conn_rr.fetch_add(1, std::memory_order_relaxed)
            % (uint32_t)state->conn_count);
    }
    ndb_conn_ctx *ctx = &state->conn_ctxs[tl_async_conn_idx];
    if (ctx->ndb == nullptr) {
        return nullptr;
    }
    ctx->ndb_mutex.lock();
    *conn_idx_out = tl_async_conn_idx;
    return ctx->ndb;
}

/** Release the per-connection shared Ndb. */
static void rondb_async_unlock(rondb_shim_handle *state, int conn_idx)
{
    state->conn_ctxs[conn_idx].ndb_mutex.unlock();
}

/**
 * Execute a prepared NDB transaction asynchronously.
 *
 * Drop-in replacement for tx->execute(type).  Caller must hold the
 * connection's ndb_mutex.  This function:
 *   1. Calls executeAsynchPrepare() to enqueue the transaction.
 *   2. Releases ndb_mutex so the flush thread can batch-send.
 *   3. Blocks until the flush thread delivers the result.
 *   4. Re-acquires ndb_mutex for the caller.
 *
 * @return 0 on success, -1 on failure (same semantics as execute()).
 */
static int rondb_async_exec(rondb_shim_handle *state, int conn_idx,
                            NdbTransaction *tx,
                            NdbTransaction::ExecType type)
{
    ndb_conn_ctx *ctx = &state->conn_ctxs[conn_idx];
    ndb_async_result result;

    tx->executeAsynchPrepare(type, ndb_async_callback, &result);
    ctx->in_flight.fetch_add(1, std::memory_order_release);

    /* Release ndb_mutex so flush thread can send/poll. */
    ctx->ndb_mutex.unlock();

    /* Wake flush thread if sleeping. */
    ctx->wake_cv.notify_one();

    /* Block until callback fires (during flush thread's pollNdb). */
    {
        std::unique_lock<std::mutex> lock(result.mtx);
        result.cv.wait(lock, [&result] { return result.done; });
    }

    ctx->in_flight.fetch_sub(1, std::memory_order_release);

    /* Re-acquire ndb_mutex for caller. */
    ctx->ndb_mutex.lock();

    return result.error;
}

static const NdbDictionary::Table *rondb_get_probe_table(rondb_shim_handle *state)
{
    NdbDictionary::Dictionary *dict = rondb_get_dictionary(state);
    const NdbDictionary::Table *table;

    if (dict == nullptr) {
        return nullptr;
    }

    table = dict->getTable(k_probe_table);
    if (table == nullptr) {
        (void)rondb_report_error(dict->getNdbError(), "getTable(pnfs_mds_probe)");
    }
    return table;
}

static int rondb_insert_probe_value(rondb_shim_handle *state,
                                    const NdbDictionary::Table *table,
                                    unsigned int value)
{
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    int rc;

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "startTransaction(insert)");
    }

    op = tx->getNdbOperation(table);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "getNdbOperation(insert)");
    }

    op->insertTuple();
    op->equal(k_probe_id_col, k_probe_row_id);
    op->setValue(k_probe_value_col, value);

    rc = tx->execute(NdbTransaction::Commit);
    if (rc == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::ConstraintViolation) {
            return 1;
        }
        return rondb_report_error(err, "insert probe row");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

static int rondb_read_meta_u64(rondb_shim_handle *state,
                               const char *key_name,
                               uint64_t *value_out)
{
    const NdbDictionary::Table *tbl;
    NdbDictionary::Dictionary *dict = rondb_get_dictionary(state);
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *val_attr;
    NdbError err;
    char key_buf[64];

    if (dict == nullptr || key_name == nullptr || value_out == nullptr) {
        return -1;
    }
    tbl = dict->getTable(RONDB_TBL_META);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                  "read_meta startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "read_meta getOp");
    }

    op->readTuple(NdbOperation::LM_CommittedRead);
    std::memset(key_buf, ' ', sizeof(key_buf));
    std::strncpy(key_buf, key_name, sizeof(key_buf) - 1);
    op->equal(RONDB_META_COL_KEY, key_buf);
    val_attr = op->getValue(RONDB_META_COL_VAL, nullptr);
    if (val_attr == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "read_meta getValue");
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "read_meta commit");
    }

    *value_out = val_attr->u_64_value();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

static int rondb_update_probe_value(rondb_shim_handle *state,
                                    const NdbDictionary::Table *table,
                                    unsigned int value)
{
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "startTransaction(update)");
    }

    op = tx->getNdbOperation(table);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "getNdbOperation(update)");
    }

    op->updateTuple();
    op->equal(k_probe_id_col, k_probe_row_id);
    op->setValue(k_probe_value_col, value);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "update probe row");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

static int rondb_write_probe_value(rondb_shim_handle *state, unsigned int value)
{
    const NdbDictionary::Table *table = rondb_get_probe_table(state);
    int rc;

    if (table == nullptr) {
        return -1;
    }

    rc = rondb_insert_probe_value(state, table, value);
    if (rc == 0) {
        return 0;
    }
    if (rc == 1) {
        return rondb_update_probe_value(state, table, value);
    }
    return -1;
}

static int rondb_read_probe_value(rondb_shim_handle *state, unsigned int *value_out)
{
    const NdbDictionary::Table *table = rondb_get_probe_table(state);
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *attr;
    NdbError err;
    unsigned int value;

    if (table == nullptr || value_out == nullptr) {
        return -1;
    }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "startTransaction(read)");
    }

    op = tx->getNdbOperation(table);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "getNdbOperation(read)");
    }

    op->readTuple(NdbOperation::LM_Read);
    op->equal(k_probe_id_col, k_probe_row_id);
    attr = op->getValue(k_probe_value_col, nullptr);
    if (attr == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "getValue(ATTR2)");
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "read probe row");
    }

    if (attr->isNULL() != 0) {
        rondb_get_ndb(state)->closeTransaction(tx);
        std::fprintf(stderr, "ERROR: RonDB probe row returned NULL value\n");
        return -1;
    }

    value = attr->u_32_value();
    rondb_get_ndb(state)->closeTransaction(tx);
    *value_out = value;
    return 0;
}

}  // namespace

extern "C" {

int rondb_shim_connect(const char *connect_string,
                       const char *schema,
                       void **handle)
{
    return rondb_shim_connect_pool(connect_string, schema, 2, handle);
}

int rondb_shim_connect_pool(const char *connect_string,
                            const char *schema,
                            int pool_size,
                            void **handle)
{
    rondb_shim_handle *state;
    int rc;

    if (connect_string == nullptr || schema == nullptr || handle == nullptr ||
        connect_string[0] == '\0' || schema[0] == '\0') {
        return -1;
    }

    if (rondb_runtime_init() != 0) {
        return -1;
    }

    if (pool_size <= 0) { pool_size = 2; }
    if (pool_size > NDB_CONN_POOL_MAX) { pool_size = NDB_CONN_POOL_MAX; }
    /* Clamp further to RonDB config.ini [api] node count. */

    state = new (std::nothrow) rondb_shim_handle();
    if (state == nullptr) {
        return -1;
    }
    /* Stamp a unique liveness generation (see rondb_get_ndb). */
    state->generation.store(
        g_rondb_handle_generation.fetch_add(1, std::memory_order_relaxed),
        std::memory_order_release);
    std::memset(state->connections, 0, sizeof(state->connections));
    state->conn_count = pool_size;  /* target; actual may be fewer */
    state->connection = nullptr;
    state->schema[0] = '\0';

    std::strncpy(state->schema, schema, sizeof(state->schema) - 1U);
    state->schema[sizeof(state->schema) - 1U] = '\0';

    /* Create a pool of NDB cluster connections.  Each connection
     * has its own TCP socket to the NDB cluster, enabling true
     * parallel NDB operations from different worker threads.
     * The first connection MUST succeed; additional connections
     * are best-effort (fall back to fewer if cluster rejects). */
    int target_pool = pool_size;
    state->conn_count = 0;  /* reset; will be incremented as connections succeed */
    for (int ci = 0; ci < target_pool; ci++) {
        Ndb_cluster_connection *conn =
            new (std::nothrow) Ndb_cluster_connection(connect_string);
        if (conn == nullptr) {
            if (ci == 0) { rondb_destroy_handle(state); return -1; }
            break;
        }

        /* Proximity-based TC selection (HopsFS pattern):
         * routes transactions to the data node closest to the
         * partition key's data, reducing one network hop. */
        conn->set_optimized_node_selection(3);
        rc = conn->connect(4, 5, 0);
        if (rc != 0) {
            if (ci == 0) {
                std::fprintf(stderr,
                    "ERROR: RonDB connect(%s) failed: rc=%d "
                    "latest_error=%d msg=%s\n",
                    connect_string, rc,
                    conn->get_latest_error(),
                    conn->get_latest_error_msg());
                delete conn;
                rondb_destroy_handle(state);
                return -1;
            }
            /* Additional connection failed -- use what we have. */
            delete conn;
            break;
        }

        rc = conn->wait_until_ready(30, 0);
        if (rc < 0) {
            if (ci == 0) {
                std::fprintf(stderr,
                    "ERROR: RonDB wait_until_ready(%s) failed: "
                    "rc=%d\n", connect_string, rc);
                delete conn;
                rondb_destroy_handle(state);
                return -1;
            }
            delete conn;
            break;
        }

        state->connections[ci] = conn;
        state->conn_count = ci + 1;
    }

    /* Legacy alias for code using ->connection. */
    state->connection = state->connections[0];

    /* Phase 3: create per-connection shared Ndb + flush threads.
     * Each flush thread drives sendPreparedTransactions()+pollNdb()
     * for its connection's shared Ndb, batching operations from
     * multiple worker threads into fewer TCP segments. */
    for (int ci = 0; ci < state->conn_count; ci++) {
        ndb_conn_ctx *ctx = &state->conn_ctxs[ci];
        ctx->ndb = new (std::nothrow) Ndb(state->connections[ci],
                                          state->schema);
        if (ctx->ndb == nullptr) {
            std::fprintf(stderr,
                "WARN: async Ndb alloc failed for conn[%d]\n", ci);
            continue;
        }
        if (ctx->ndb->init() != 0) {
            std::fprintf(stderr,
                "WARN: async Ndb::init() failed for conn[%d]: "
                "code=%d msg=%s\n",
                ci, ctx->ndb->getNdbError().code,
                ctx->ndb->getNdbError().message);
            delete ctx->ndb;
            ctx->ndb = nullptr;
            continue;
        }
        ctx->stop.store(false, std::memory_order_release);
        ctx->in_flight.store(0, std::memory_order_release);
        ctx->flush_thread = std::thread(ndb_flush_thread_fn, ctx);
    }

    std::fprintf(stderr,
        "INFO: RonDB connection pool: %d connection(s) to %s\n",
        state->conn_count, connect_string);

    *handle = state;
    return 0;
}

int rondb_shim_bootstrap(void *handle, const char *schema)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, schema);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *existing;
    NdbDictionary::Table table;
    NdbDictionary::Column key_col;
    NdbDictionary::Column value_col;
    NdbError err;

    if (state == nullptr) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) {
        return -1;
    }

    existing = dict->getTable(k_probe_table);
    if (existing != nullptr) {
        return 0;
    }

    table.setName(k_probe_table);

    key_col.setName(k_probe_id_col);
    key_col.setType(NdbDictionary::Column::Unsigned);
    key_col.setPrimaryKey(true);
    key_col.setPartitionKey(true);
    key_col.setNullable(false);
    table.addColumn(key_col);

    value_col.setName(k_probe_value_col);
    value_col.setType(NdbDictionary::Column::Unsigned);
    value_col.setNullable(false);
    table.addColumn(value_col);

    if (dict->createTable(table) != 0) {
        err = dict->getNdbError();
        if (err.classification == NdbError::SchemaObjectExists) {
            return 0;
        }
        return rondb_report_error(err, "createTable(pnfs_mds_probe)");
    }

    return 0;
}

int rondb_shim_probe(void *handle)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    unsigned int expected;
    unsigned int actual;

    if (state == nullptr) {
        return -1;
    }

    expected = static_cast<unsigned int>(std::time(nullptr));
    if (expected == 0U) {
        expected = 1U;
    }

    if (rondb_write_probe_value(state, expected) != 0) {
        return -1;
    }
    if (rondb_read_probe_value(state, &actual) != 0) {
        return -1;
    }
    if (actual != expected) {
        std::fprintf(stderr,
            "ERROR: RonDB probe value mismatch: expected=%u actual=%u\n",
            expected, actual);
        return -1;
    }

    return 0;
}

int rondb_shim_cleanup(void *handle, const char *schema)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, schema);
    NdbDictionary::Dictionary *dict;
    NdbError err;

    if (state == nullptr) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) {
        return -1;
    }

    if (dict->dropTable(k_probe_table) != 0) {
        err = dict->getNdbError();
        if (err.classification == NdbError::NoDataFound ||
            err.classification == NdbError::SchemaError) {
            return 0;
        }
        return rondb_report_error(err, "dropTable(pnfs_mds_probe)");
    }

    return 0;
}

void rondb_shim_disconnect(void *handle)
{
    rondb_destroy_handle(static_cast<rondb_shim_handle *>(handle));
}

void rondb_shim_set_async_writes(void *handle, int enabled)
{
    rondb_shim_handle *state =
        static_cast<rondb_shim_handle *>(handle);
    if (state == nullptr) {
        return;
    }
    state->async_writes.store(enabled != 0,
                              std::memory_order_release);
}

/* -----------------------------------------------------------------------
 * Stage B -- metadata table DDL + row-level CRUD + atomic helpers.
 *
 * Table names and column names are defined in rondb_schema.h and
 * referenced via the RONDB_TBL_* / RONDB_*_COL_* macros.
 * Inode serialisation uses rondb_inode_serialize/deserialize from
 * rondb_schema.c across the C/C++ boundary.
 * ----------------------------------------------------------------------- */

/* NDB lock modes mapped from read_mode parameter. */
static NdbOperation::LockMode rondb_map_lock_mode(int read_mode)
{
    switch (read_mode) {
    case 1:  return NdbOperation::LM_Read;       /* SHARED */
    case 2:  return NdbOperation::LM_Exclusive;   /* EXCLUSIVE */
    default: return NdbOperation::LM_CommittedRead; /* COMMITTED */
    }
}

static int rondb_drop_table_if_exists(NdbDictionary::Dictionary *dict,
                                      const char *table_name)
{
    if (dict->dropTable(table_name) != 0) {
        NdbError err = dict->getNdbError();
        if (err.classification == NdbError::NoDataFound ||
            err.classification == NdbError::SchemaError) {
            /* Table doesn't exist -- flush stale cache entry anyway. */
            dict->invalidateTable(table_name);
            return 0;
        }
        return rondb_report_error(err, table_name);
    }
    /* Invalidate API-side dictionary cache so subsequent createTable
     * and getTable calls see the table as gone.  Without this, the
     * NDB API may return a stale table pointer after drop+create. */
    dict->invalidateTable(table_name);
    return 0;
}

static int rondb_equal_u64(NdbOperation *op, const char *column, uint64_t value)
{
    return op->equal(column, static_cast<Uint64>(value));
}

static int rondb_set_value_u64(NdbOperation *op, const char *column, uint64_t value)
{
    return op->setValue(column, static_cast<Uint64>(value));
}

static int rondb_now_ns(uint64_t *out_now_ns)
{
    struct timespec now;

    if (out_now_ns == nullptr) {
        return -1;
    }
    if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
        std::perror("clock_gettime");
        return -1;
    }

    *out_now_ns = ((uint64_t)now.tv_sec * 1000000000ULL) +
                  (uint64_t)now.tv_nsec;
    return 0;
}

static bool rondb_lock_mode_valid(uint8_t lock_mode)
{
    return lock_mode == RONDB_LOCK_MODE_SHARED ||
           lock_mode == RONDB_LOCK_MODE_EXCLUSIVE;
}

static bool rondb_lock_row_not_found(const NdbError &err)
{
    return err.code == 626 || err.classification == NdbError::NoDataFound;
}

static uint32_t rondb_u32_max(uint32_t a, uint32_t b)
{
    return (a > b) ? a : b;
}

static uint64_t rondb_u64_max(uint64_t a, uint64_t b)
{
    return (a > b) ? a : b;
}
static int rondb_encode_varbinary_value(const uint8_t *value,
                                        uint32_t value_len,
                                        uint32_t prefix_len,
                                        uint8_t *encoded,
                                        uint32_t encoded_cap,
                                        uint32_t *encoded_len_out)
{
    if (encoded == nullptr || encoded_len_out == nullptr ||
        (prefix_len != 1U && prefix_len != 2U) ||
        encoded_cap < value_len + prefix_len ||
        (value_len > 0U && value == nullptr)) {
        return -1;
    }

    /* Write length prefix (1- or 2-byte LE). */
    encoded[0] = (uint8_t)(value_len & 0xFFU);
    if (prefix_len == 2U) {
        encoded[1] = (uint8_t)((value_len >> 8U) & 0xFFU);
    }
    /* Copy payload (may be zero-length). */
    if (value_len > 0U) {
        std::memcpy(encoded + prefix_len, value, value_len);
    }

    *encoded_len_out = value_len + prefix_len;
    return 0;
}

static int rondb_encode_varbinary_string(const char *value,
                                         uint32_t prefix_len,
                                         uint8_t *encoded,
                                         uint32_t encoded_cap,
                                         uint32_t *encoded_len_out)
{
    if (value == nullptr) {
        return -1;
    }

    return rondb_encode_varbinary_value((const uint8_t *)value,
                                        (uint32_t)std::strlen(value),
                                        prefix_len,
                                        encoded,
                                        encoded_cap,
                                        encoded_len_out);
}

static int rondb_decode_varbinary_string_attr(const NdbRecAttr *attr,
                                              char *out, size_t out_cap)
{
    const char *ref;
    uint32_t data_len;
    uint32_t copy_len;

    if (out == nullptr || out_cap == 0U) {
        return -1;
    }

    out[0] = '\0';
    if (attr == nullptr) {
        return 0;
    }
    ref = attr->aRef();
    if (ref == nullptr) {
        return 0;
    }

    data_len = (uint32_t)(uint8_t)ref[0];
    copy_len = (data_len < (uint32_t)(out_cap - 1U))
             ? data_len : (uint32_t)(out_cap - 1U);
    if (copy_len > 0U) {
        std::memcpy(out, ref + 1, copy_len);
    }
    out[copy_len] = '\0';
    return 0;
}

static int rondb_decode_lvb_value_attr(const NdbRecAttr *attr,
                                       uint8_t *out, uint32_t out_cap,
                                       uint32_t *out_len)
{
    const char *ref;
    uint32_t data_len;

    if (out_len == nullptr) {
        return -1;
    }

    *out_len = 0;
    if (attr == nullptr) {
        return 0;
    }
    ref = attr->aRef();
    if (ref == nullptr) {
        return 0;
    }

    data_len = (uint32_t)(uint8_t)ref[0] |
               ((uint32_t)(uint8_t)ref[1] << 8U);
    if (data_len > out_cap || (data_len > 0U && out == nullptr)) {
        return -1;
    }
    if (data_len > 0U) {
        std::memcpy(out, ref + 2, data_len);
    }
    *out_len = data_len;
    return 0;
}

static int rondb_lock_partition_hint_from_key(uint8_t resource_class,
                                              const uint8_t *resource_key,
                                              uint32_t key_len,
                                              uint64_t *partition_hint_out)
{
    if (resource_key == nullptr || partition_hint_out == nullptr ||
        key_len < sizeof(uint64_t)) {
        return -1;
    }

    switch (resource_class) {
    case RONDB_LOCK_CLASS_TOPOLOGY:
        if (key_len != sizeof(uint64_t) ||
            fdb_get_u64(resource_key) != RONDB_TOPOLOGY_SENTINEL) {
            return -1;
        }
        break;
    case RONDB_LOCK_CLASS_PARENT_NAME:
        break;
    case RONDB_LOCK_CLASS_DIR_MUTATION:
    case RONDB_LOCK_CLASS_INODE_ATTR:
        if (key_len != sizeof(uint64_t)) {
            return -1;
        }
        break;
    default:
        return -1;
    }

    *partition_hint_out = fdb_get_u64(resource_key);
    return 0;
}

static int rondb_add_lock_resource_insert(NdbTransaction *tx,
                                          const NdbDictionary::Table *tbl,
                                          uint64_t partition_hint,
                                          uint8_t resource_class,
                                          const uint8_t *resource_key_value,
                                          uint32_t resource_key_value_len,
                                          uint8_t lock_mode,
                                          uint32_t holder_count,
                                          uint32_t owner_mds_id,
                                          uint64_t owner_epoch,
                                          uint64_t fencing_epoch,
                                          uint64_t granted_at_ns,
                                          uint32_t ttl_ms)
{
    NdbOperation *op = tx->getNdbOperation(tbl);

    if (op == nullptr) {
        return -1;
    }

    op->insertTuple();
    (void)rondb_set_value_u64(op, RONDB_LK_COL_PART_HINT, partition_hint);
    op->setValue(RONDB_LK_COL_RES_CLASS, (Uint32)resource_class);
    op->setValue(RONDB_LK_COL_RES_KEY,
                 (const char *)resource_key_value,
                 resource_key_value_len);
    op->setValue(RONDB_LK_COL_LOCK_MODE, (Uint32)lock_mode);
    op->setValue(RONDB_LK_COL_HOLDER_COUNT, holder_count);
    op->setValue(RONDB_LK_COL_OWNER_MDS, owner_mds_id);
    (void)rondb_set_value_u64(op, RONDB_LK_COL_OWNER_EPOCH, owner_epoch);
    (void)rondb_set_value_u64(op, RONDB_LK_COL_FENCE_EPOCH, fencing_epoch);
    (void)rondb_set_value_u64(op, RONDB_LK_COL_GRANTED_AT, granted_at_ns);
    op->setValue(RONDB_LK_COL_TTL_MS, ttl_ms);
    return 0;
}

static int rondb_add_lock_resource_update(NdbTransaction *tx,
                                          const NdbDictionary::Table *tbl,
                                          uint64_t partition_hint,
                                          uint8_t resource_class,
                                          const uint8_t *resource_key_value,
                                          uint32_t resource_key_value_len,
                                          uint8_t lock_mode,
                                          uint32_t holder_count,
                                          uint32_t owner_mds_id,
                                          uint64_t owner_epoch,
                                          uint64_t fencing_epoch,
                                          uint64_t granted_at_ns,
                                          uint32_t ttl_ms)
{
    NdbOperation *op = tx->getNdbOperation(tbl);

    if (op == nullptr) {
        return -1;
    }

    op->updateTuple();
    (void)rondb_equal_u64(op, RONDB_LK_COL_PART_HINT, partition_hint);
    op->equal(RONDB_LK_COL_RES_CLASS, (Uint32)resource_class);
    op->equal(RONDB_LK_COL_RES_KEY,
              (const char *)resource_key_value,
              resource_key_value_len);
    op->setValue(RONDB_LK_COL_LOCK_MODE, (Uint32)lock_mode);
    op->setValue(RONDB_LK_COL_HOLDER_COUNT, holder_count);
    op->setValue(RONDB_LK_COL_OWNER_MDS, owner_mds_id);
    (void)rondb_set_value_u64(op, RONDB_LK_COL_OWNER_EPOCH, owner_epoch);
    (void)rondb_set_value_u64(op, RONDB_LK_COL_FENCE_EPOCH, fencing_epoch);
    (void)rondb_set_value_u64(op, RONDB_LK_COL_GRANTED_AT, granted_at_ns);
    op->setValue(RONDB_LK_COL_TTL_MS, ttl_ms);
    return 0;
}

static int rondb_add_lock_resource_delete(NdbTransaction *tx,
                                          const NdbDictionary::Table *tbl,
                                          uint64_t partition_hint,
                                          uint8_t resource_class,
                                          const uint8_t *resource_key_value,
                                          uint32_t resource_key_value_len)
{
    NdbOperation *op = tx->getNdbOperation(tbl);

    if (op == nullptr) {
        return -1;
    }

    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_LK_COL_PART_HINT, partition_hint);
    op->equal(RONDB_LK_COL_RES_CLASS, (Uint32)resource_class);
    op->equal(RONDB_LK_COL_RES_KEY,
              (const char *)resource_key_value,
              resource_key_value_len);
    return 0;
}

static int rondb_add_lock_holder_insert(NdbTransaction *tx,
                                        const NdbDictionary::Table *tbl,
                                        uint64_t partition_hint,
                                        uint8_t resource_class,
                                        const uint8_t *resource_key_value,
                                        uint32_t resource_key_value_len,
                                        uint8_t lock_mode,
                                        uint32_t owner_mds_id,
                                        uint64_t owner_epoch,
                                        uint64_t fencing_epoch,
                                        uint64_t granted_at_ns,
                                        uint32_t ttl_ms)
{
    NdbOperation *op = tx->getNdbOperation(tbl);

    if (op == nullptr) {
        return -1;
    }

    op->insertTuple();
    (void)rondb_set_value_u64(op, RONDB_LK_COL_PART_HINT, partition_hint);
    op->setValue(RONDB_LK_COL_RES_CLASS, (Uint32)resource_class);
    op->setValue(RONDB_LK_COL_RES_KEY,
                 (const char *)resource_key_value,
                 resource_key_value_len);
    op->setValue(RONDB_LK_COL_OWNER_MDS, owner_mds_id);
    (void)rondb_set_value_u64(op, RONDB_LK_COL_OWNER_EPOCH, owner_epoch);
    op->setValue(RONDB_LK_COL_LOCK_MODE, (Uint32)lock_mode);
    (void)rondb_set_value_u64(op, RONDB_LK_COL_FENCE_EPOCH, fencing_epoch);
    (void)rondb_set_value_u64(op, RONDB_LK_COL_GRANTED_AT, granted_at_ns);
    op->setValue(RONDB_LK_COL_TTL_MS, ttl_ms);
    return 0;
}

static int rondb_add_lock_holder_update(NdbTransaction *tx,
                                        const NdbDictionary::Table *tbl,
                                        uint64_t partition_hint,
                                        uint8_t resource_class,
                                        const uint8_t *resource_key_value,
                                        uint32_t resource_key_value_len,
                                        uint8_t lock_mode,
                                        uint32_t owner_mds_id,
                                        uint64_t owner_epoch,
                                        uint64_t fencing_epoch,
                                        uint64_t granted_at_ns,
                                        uint32_t ttl_ms)
{
    NdbOperation *op = tx->getNdbOperation(tbl);

    if (op == nullptr) {
        return -1;
    }

    op->updateTuple();
    (void)rondb_equal_u64(op, RONDB_LK_COL_PART_HINT, partition_hint);
    op->equal(RONDB_LK_COL_RES_CLASS, (Uint32)resource_class);
    op->equal(RONDB_LK_COL_RES_KEY,
              (const char *)resource_key_value,
              resource_key_value_len);
    op->equal(RONDB_LK_COL_OWNER_MDS, owner_mds_id);
    (void)rondb_equal_u64(op, RONDB_LK_COL_OWNER_EPOCH, owner_epoch);
    op->setValue(RONDB_LK_COL_LOCK_MODE, (Uint32)lock_mode);
    (void)rondb_set_value_u64(op, RONDB_LK_COL_FENCE_EPOCH, fencing_epoch);
    (void)rondb_set_value_u64(op, RONDB_LK_COL_GRANTED_AT, granted_at_ns);
    op->setValue(RONDB_LK_COL_TTL_MS, ttl_ms);
    return 0;
}

static int rondb_add_lock_holder_delete(NdbTransaction *tx,
                                        const NdbDictionary::Table *tbl,
                                        uint64_t partition_hint,
                                        uint8_t resource_class,
                                        const uint8_t *resource_key_value,
                                        uint32_t resource_key_value_len,
                                        uint32_t owner_mds_id,
                                        uint64_t owner_epoch)
{
    NdbOperation *op = tx->getNdbOperation(tbl);

    if (op == nullptr) {
        return -1;
    }

    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_LK_COL_PART_HINT, partition_hint);
    op->equal(RONDB_LK_COL_RES_CLASS, (Uint32)resource_class);
    op->equal(RONDB_LK_COL_RES_KEY,
              (const char *)resource_key_value,
              resource_key_value_len);
    op->equal(RONDB_LK_COL_OWNER_MDS, owner_mds_id);
    (void)rondb_equal_u64(op, RONDB_LK_COL_OWNER_EPOCH, owner_epoch);
    return 0;
}

/**
 * Emit an interpreted-update on a parent inode row within an existing
 * NDB transaction.  Atomically adjusts nlink and change_ctr at the
 * data node (no read-modify-write race), and sets mtime/ctime.
 *
 * Uses interpretedUpdateTuple + incValue/subValue which execute entirely
 * at the NDB data node -- zero extra round trips, immune to the
 * read-modify-write race that caused BUG-1 (nlink corruption under
 * concurrent multi-MDS mutations in the same directory).
 *
 * @param tx             Active NDB transaction.
 * @param ino_tbl        The mds_inodes table descriptor.
 * @param parent_fileid  PK of the parent inode.
 * @param nlink_delta    +1 (dir create), -1 (dir remove), 0 (file ops).
 * @return 0 on success, -1 on error.
 */
static int rondb_interpreted_parent_update(
    NdbTransaction *tx,
    const NdbDictionary::Table *ino_tbl,
    uint64_t parent_fileid,
    int32_t nlink_delta)
{
    NdbOperation *op = tx->getNdbOperation(ino_tbl);
    struct timespec now;

    if (op == nullptr) {
        return -1;
    }

    op->interpretedUpdateTuple();
    (void)rondb_equal_u64(op, RONDB_INO_COL_FILEID, parent_fileid);

    /* Atomic nlink delta -- executed at the data node. */
    if (nlink_delta > 0) {
        op->incValue(RONDB_INO_COL_NLINK, (Uint32)nlink_delta);
    } else if (nlink_delta < 0) {
        op->subValue(RONDB_INO_COL_NLINK, (Uint32)(-nlink_delta));
    }

    /* Atomic change counter increment. */
    op->incValue(RONDB_INO_COL_CHANGE, (Uint64)1);

    /* Absolute mtime + ctime -- both concurrent writers set ~same wall
     * time; last-writer-wins is acceptable for timestamps. */
    clock_gettime(CLOCK_REALTIME, &now);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_MTIME_SEC,
                              (uint64_t)now.tv_sec);
    op->setValue(RONDB_INO_COL_MTIME_NSEC, (Uint32)now.tv_nsec);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_CTIME_SEC,
                              (uint64_t)now.tv_sec);
    op->setValue(RONDB_INO_COL_CTIME_NSEC, (Uint32)now.tv_nsec);

    return 0;
}

static void rondb_set_inode_values(NdbOperation *op,
                                   const struct mds_inode *ino,
                                   uint32_t shard)
{
    op->setValue(RONDB_INO_COL_TYPE, (Uint32)(uint8_t)ino->type);
    op->setValue(RONDB_INO_COL_MODE, ino->mode);
    op->setValue(RONDB_INO_COL_NLINK, ino->nlink);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_UID, ino->uid);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_GID, ino->gid);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_FILE_SIZE, ino->size);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_SPACE_USED, ino->space_used);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_ATIME_SEC,
                              (uint64_t)ino->atime.tv_sec);
    op->setValue(RONDB_INO_COL_ATIME_NSEC, (Uint32)ino->atime.tv_nsec);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_MTIME_SEC,
                              (uint64_t)ino->mtime.tv_sec);
    op->setValue(RONDB_INO_COL_MTIME_NSEC, (Uint32)ino->mtime.tv_nsec);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_CTIME_SEC,
                              (uint64_t)ino->ctime.tv_sec);
    op->setValue(RONDB_INO_COL_CTIME_NSEC, (Uint32)ino->ctime.tv_nsec);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_CHANGE, ino->change);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_GENERATION, ino->generation);
    op->setValue(RONDB_INO_COL_FLAGS, ino->flags);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_CREATE_VERF, ino->create_verf);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_PARENT, ino->parent_fileid);
    op->setValue(RONDB_INO_COL_HOME_SHARD, shard);
}

static int rondb_compare_name_bytes(const char *lhs, size_t lhs_len,
                                    const char *rhs, size_t rhs_len)
{
    size_t min_len = (lhs_len < rhs_len) ? lhs_len : rhs_len;
    int cmp = std::memcmp(lhs, rhs, min_len);

    if (cmp != 0) {
        return cmp;
    }
    if (lhs_len < rhs_len) {
        return -1;
    }
    if (lhs_len > rhs_len) {
        return 1;
    }
    return 0;
}

struct rondb_readdir_row {
    std::string name;
    uint64_t child_fid;
    uint8_t child_type;
};

static void rondb_readdir_sort_rows(std::vector<rondb_readdir_row> &rows)
{
    std::sort(rows.begin(), rows.end(),
              [](const rondb_readdir_row &lhs,
                 const rondb_readdir_row &rhs) {
                  return rondb_compare_name_bytes(lhs.name.data(),
                                                  lhs.name.size(),
                                                  rhs.name.data(),
                                                  rhs.name.size()) < 0;
              });
}

/** Index of the first row with name > start_after (or rows.size()). */
static size_t rondb_readdir_start_index(
    const std::vector<rondb_readdir_row> &rows,
    const char *start_after)
{
    if (start_after == nullptr || start_after[0] == '\0') {
        return 0;
    }

    size_t sa_len = std::strlen(start_after);
    for (size_t i = 0; i < rows.size(); i++) {
        if (rondb_compare_name_bytes(rows[i].name.data(),
                                   rows[i].name.size(),
                                   start_after, sa_len) > 0) {
            return i;
        }
    }
    return rows.size();
}

/** End index (exclusive) for a bounded page. */
static size_t rondb_readdir_end_index(size_t first, size_t total,
                                      uint32_t max_entries)
{
    if (max_entries == 0) {
        return total;
    }
    size_t end = first + (size_t)max_entries;
    return (end < total) ? end : total;
}

static int rondb_readdir_scan_parent(
    rondb_shim_handle *state,
    uint64_t parent_fileid,
    std::vector<rondb_readdir_row> *rows_out,
    const char *err_tag)
{
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_name, *a_cfid, *a_ctype;
    NdbError err;
    int next_rc;

    if (state == nullptr || rows_out == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DIRENTS);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, parent_fileid);
        tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 err_tag != nullptr ? err_tag : "readdir startTx");
    }

    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "readdir getScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "readdir readTuples");
    }

    {
        NdbScanFilter filter(scan);
        filter.begin(NdbScanFilter::AND);
        filter.eq(tbl->getColumn(RONDB_DIR_COL_PARENT)->getColumnNo(),
                  (Uint64)parent_fileid);
        filter.end();
    }

    a_name  = scan->getValue(RONDB_DIR_COL_NAME, nullptr);
    a_cfid  = scan->getValue(RONDB_DIR_COL_CHILD_FID, nullptr);
    a_ctype = scan->getValue(RONDB_DIR_COL_CHILD_TYPE, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "readdir exec");
    }

    rows_out->clear();
    while ((next_rc = scan->nextResult(true)) == 0) {
        uint64_t child_fid = a_cfid->u_64_value();
        uint8_t child_type = (uint8_t)a_ctype->u_8_value();
        const char *name_ptr = a_name->aRef();
        uint32_t name_len;
        rondb_readdir_row row;

        if (name_ptr == nullptr) { continue; }

        name_len = (uint32_t)(uint8_t)name_ptr[0];
        name_ptr += 1;
        row.child_fid = child_fid;
        row.child_type = child_type;
        row.name.assign(name_ptr, name_len);
        rows_out->push_back(row);
    }
    if (next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "readdir nextResult");
    }

    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

static int rondb_create_table_if_not_exists(
    NdbDictionary::Dictionary *dict,
    NdbDictionary::Table &table)
{
    if (dict->getTable(table.getName()) != nullptr) {
        return 0; /* Already exists */
    }
    if (dict->createTable(table) != 0) {
        NdbError err = dict->getNdbError();
        if (err.classification == NdbError::SchemaObjectExists) {
            return 0;
        }
        return rondb_report_error(err, table.getName());
    }
    return 0;
}

/*
 * RonDB can retain negative dictionary-cache entries after a prior
 * getIndex() miss.  For ordered-index callers, retry once after
 * invalidating both the index and parent table caches so live schema
 * changes become visible to this API client.
 */
static const NdbDictionary::Index *
rondb_resolve_index(NdbDictionary::Dictionary *dict,
                    const char *ix_name,
                    const char *table_name)
{
    if (dict == nullptr || ix_name == nullptr || table_name == nullptr) {
        return nullptr;
    }
    const NdbDictionary::Index *ix = dict->getIndex(ix_name, table_name);
    if (ix != nullptr) {
        return ix;
    }
    dict->invalidateIndex(ix_name, table_name);
    dict->invalidateTable(table_name);
    return dict->getIndex(ix_name, table_name);
}

static int rondb_verify_index_visible(NdbDictionary::Dictionary *dict,
                                      const char *ix_name,
                                      const char *table_name)
{
    /* After a clean createIndex() the index may need a short propagation
     * delay before it is visible to NdbDictionary::getIndex(). Callers
     * that received code 4714 (ndb_index_stat absent) should skip this
     * check and rely on the runtime table-scan fallback instead. */
    for (int attempt = 0; attempt < 60; attempt++) {
        if (rondb_resolve_index(dict, ix_name, table_name) != nullptr) {
            return 0;
        }
        if (attempt > 0) {
            usleep(500000);
        }
        dict->invalidateIndex(ix_name, table_name);
        dict->invalidateTable(table_name);
    }
    std::fprintf(stderr,
        "ERROR: ordered index %s on table %s is not visible to the NDB "
        "API client after createIndex(); refusing to continue with a "
        "missing layout index.\n",
        ix_name, table_name);
    return -1;
}

/* Helper: add a BIGUNSIGNED column. */
static void rondb_add_bigunsigned(NdbDictionary::Table &tbl,
                                   const char *name, bool pk, bool part)
{
    NdbDictionary::Column col;
    col.setName(name);
    col.setType(NdbDictionary::Column::Bigunsigned);
    col.setPrimaryKey(pk);
    col.setPartitionKey(part);
    col.setNullable(false);
    tbl.addColumn(col);
}

static void rondb_add_unsigned(NdbDictionary::Table &tbl, const char *name)
{
    NdbDictionary::Column col;
    col.setName(name);
    col.setType(NdbDictionary::Column::Unsigned);
    col.setNullable(false);
    tbl.addColumn(col);
}

static void rondb_add_tinyunsigned(NdbDictionary::Table &tbl, const char *name)
{
    NdbDictionary::Column col;
    col.setName(name);
    col.setType(NdbDictionary::Column::Tinyunsigned);
    col.setNullable(false);
    tbl.addColumn(col);
}

static void rondb_add_char(NdbDictionary::Table &tbl, const char *name,
                            int length, bool pk)
{
    NdbDictionary::Column col;
    col.setName(name);
    col.setType(NdbDictionary::Column::Char);
    col.setLength(length);
    col.setPrimaryKey(pk);
    col.setPartitionKey(pk);
    col.setNullable(false);
    tbl.addColumn(col);
}

static void rondb_add_varbinary(NdbDictionary::Table &tbl, const char *name,
                                 int length, bool pk, bool part)
{
    NdbDictionary::Column col;
    col.setName(name);
    if (length > 255) {
        col.setType(NdbDictionary::Column::Longvarbinary);
    } else {
        col.setType(NdbDictionary::Column::Varbinary);
    }
    col.setLength(length);
    col.setPrimaryKey(pk);
    col.setPartitionKey(part);
    col.setNullable(false);
    tbl.addColumn(col);
}

static int rondb_define_meta_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_META);
    rondb_add_char(tbl, RONDB_META_COL_KEY, 64, true);
    rondb_add_bigunsigned(tbl, RONDB_META_COL_VAL, false, false);
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_inodes_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_INODES);
    rondb_add_bigunsigned(tbl, RONDB_INO_COL_FILEID, true, true);
    rondb_add_tinyunsigned(tbl, RONDB_INO_COL_TYPE);
    rondb_add_unsigned(tbl, RONDB_INO_COL_MODE);
    rondb_add_unsigned(tbl, RONDB_INO_COL_NLINK);
    rondb_add_bigunsigned(tbl, RONDB_INO_COL_UID, false, false);
    rondb_add_bigunsigned(tbl, RONDB_INO_COL_GID, false, false);
    rondb_add_bigunsigned(tbl, RONDB_INO_COL_FILE_SIZE, false, false);
    rondb_add_bigunsigned(tbl, RONDB_INO_COL_SPACE_USED, false, false);
    rondb_add_bigunsigned(tbl, RONDB_INO_COL_ATIME_SEC, false, false);
    rondb_add_unsigned(tbl, RONDB_INO_COL_ATIME_NSEC);
    rondb_add_bigunsigned(tbl, RONDB_INO_COL_MTIME_SEC, false, false);
    rondb_add_unsigned(tbl, RONDB_INO_COL_MTIME_NSEC);
    rondb_add_bigunsigned(tbl, RONDB_INO_COL_CTIME_SEC, false, false);
    rondb_add_unsigned(tbl, RONDB_INO_COL_CTIME_NSEC);
    rondb_add_bigunsigned(tbl, RONDB_INO_COL_CHANGE, false, false);
    rondb_add_bigunsigned(tbl, RONDB_INO_COL_GENERATION, false, false);
    rondb_add_unsigned(tbl, RONDB_INO_COL_FLAGS);
    rondb_add_bigunsigned(tbl, RONDB_INO_COL_CREATE_VERF, false, false);
    rondb_add_bigunsigned(tbl, RONDB_INO_COL_PARENT, false, false);
    rondb_add_unsigned(tbl, RONDB_INO_COL_HOME_SHARD);
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_dirents_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_DIRENTS);
    rondb_add_bigunsigned(tbl, RONDB_DIR_COL_PARENT, true, true);
    rondb_add_varbinary(tbl, RONDB_DIR_COL_NAME, 255, true, false);
    rondb_add_bigunsigned(tbl, RONDB_DIR_COL_CHILD_FID, false, false);
    rondb_add_tinyunsigned(tbl, RONDB_DIR_COL_CHILD_TYPE);

    int rc = rondb_create_table_if_not_exists(dict, tbl);
    if (rc != 0) { return rc; }

    /* Ordered index on (parent_fileid, child_fileid) for O(log N +
     * page) READDIR cookie resume.  Idempotent: tolerate
     * SchemaObjectExists (rerun) and code 4714 (ndb_index_stat tables
     * absent -- the index is functional, only statistics collection is
     * disabled).  Always attempt creation and treat SchemaObjectExists
     * as success to avoid the API-side dictionary-cache hazard
     * documented for the layout_state indices. */
    dict->invalidateIndex(RONDB_IX_DIRENTS_PARENT_CHILD,
                          RONDB_TBL_DIRENTS);
    NdbDictionary::Index ix(RONDB_IX_DIRENTS_PARENT_CHILD);
    ix.setTable(RONDB_TBL_DIRENTS);
    ix.setType(NdbDictionary::Index::OrderedIndex);
    ix.setLogging(false);
    ix.addColumnName(RONDB_DIR_COL_PARENT);
    ix.addColumnName(RONDB_DIR_COL_CHILD_FID);
    if (dict->createIndex(ix) != 0) {
        NdbError ierr = dict->getNdbError();
        if (ierr.classification != NdbError::SchemaObjectExists &&
            ierr.code != 4714) {
            return rondb_report_error(ierr,
                RONDB_IX_DIRENTS_PARENT_CHILD);
        }
    }
    return 0;
}

static int rondb_define_stripe_maps_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_STRIPE_MAPS);
    rondb_add_bigunsigned(tbl, RONDB_SM_COL_FILEID, true, true);
    rondb_add_unsigned(tbl, RONDB_SM_COL_STRIPE_CNT);
    rondb_add_unsigned(tbl, RONDB_SM_COL_STRIPE_UNIT);
    rondb_add_unsigned(tbl, RONDB_SM_COL_MIRROR_CNT);
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_stripe_entries_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_STRIPE_ENTRIES);
    rondb_add_bigunsigned(tbl, RONDB_SE_COL_FILEID, true, true);
    rondb_add_unsigned(tbl, RONDB_SE_COL_ORDINAL);
    {
        NdbDictionary::Column ord_pk;
        /* ordinal is part of composite PK but NOT partition key. */
        /* Already added as UNSIGNED above; mark PK on the column
         * object before addColumn.  Re-add with PK flag. */
    }
    /* Re-do ordinal as PK member: remove the non-PK one and add PK. */
    /* NDB DDL API requires PK columns added in order.  Since we
     * added fileid(PK) then ordinal(non-PK), we need to rebuild.
     * Simpler: build the table from scratch with correct PK flags. */
    {
        NdbDictionary::Table tbl2;
        tbl2.setName(RONDB_TBL_STRIPE_ENTRIES);

        NdbDictionary::Column c_fid;
        c_fid.setName(RONDB_SE_COL_FILEID);
        c_fid.setType(NdbDictionary::Column::Bigunsigned);
        c_fid.setPrimaryKey(true);
        c_fid.setPartitionKey(true);
        c_fid.setNullable(false);
        tbl2.addColumn(c_fid);

        NdbDictionary::Column c_ord;
        c_ord.setName(RONDB_SE_COL_ORDINAL);
        c_ord.setType(NdbDictionary::Column::Unsigned);
        c_ord.setPrimaryKey(true);
        c_ord.setPartitionKey(false);
        c_ord.setNullable(false);
        tbl2.addColumn(c_ord);

        rondb_add_unsigned(tbl2, RONDB_SE_COL_DS_ID);
        rondb_add_unsigned(tbl2, RONDB_SE_COL_NFS_FH_LEN);
        rondb_add_varbinary(tbl2, RONDB_SE_COL_NFS_FH, 128, false, false);

        return rondb_create_table_if_not_exists(dict, tbl2);
    }
}

static int rondb_define_ns_lock_holders_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_NS_LOCK_HOLDERS);
    rondb_add_bigunsigned(tbl, RONDB_LK_COL_PART_HINT, false, true);
    rondb_add_tinyunsigned(tbl, RONDB_LK_COL_RES_CLASS);
    {
        NdbDictionary::Table tbl2;
        tbl2.setName(RONDB_TBL_NS_LOCK_HOLDERS);

        NdbDictionary::Column c_hint;
        c_hint.setName(RONDB_LK_COL_PART_HINT);
        c_hint.setType(NdbDictionary::Column::Bigunsigned);
        c_hint.setPrimaryKey(true);
        c_hint.setPartitionKey(true);
        c_hint.setNullable(false);
        tbl2.addColumn(c_hint);

        NdbDictionary::Column c_class;
        c_class.setName(RONDB_LK_COL_RES_CLASS);
        c_class.setType(NdbDictionary::Column::Tinyunsigned);
        c_class.setPrimaryKey(true);
        c_class.setNullable(false);
        tbl2.addColumn(c_class);
        rondb_add_varbinary(tbl2, RONDB_LK_COL_RES_KEY,
                            RONDB_LOCK_KEY_MAX, true, false);

        NdbDictionary::Column c_owner_mds;
        c_owner_mds.setName(RONDB_LK_COL_OWNER_MDS);
        c_owner_mds.setType(NdbDictionary::Column::Unsigned);
        c_owner_mds.setPrimaryKey(true);
        c_owner_mds.setNullable(false);
        tbl2.addColumn(c_owner_mds);

        NdbDictionary::Column c_owner_epoch;
        c_owner_epoch.setName(RONDB_LK_COL_OWNER_EPOCH);
        c_owner_epoch.setType(NdbDictionary::Column::Bigunsigned);
        c_owner_epoch.setPrimaryKey(true);
        c_owner_epoch.setNullable(false);
        tbl2.addColumn(c_owner_epoch);

        rondb_add_tinyunsigned(tbl2, RONDB_LK_COL_LOCK_MODE);
        rondb_add_bigunsigned(tbl2, RONDB_LK_COL_FENCE_EPOCH, false, false);
        rondb_add_bigunsigned(tbl2, RONDB_LK_COL_GRANTED_AT, false, false);
        rondb_add_unsigned(tbl2, RONDB_LK_COL_TTL_MS);

        return rondb_create_table_if_not_exists(dict, tbl2);
    }
}

static int rondb_define_rename_journal_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_RENAME_JOURNAL);

    {
        NdbDictionary::Column c_txn;
        c_txn.setName(RONDB_RJ_COL_TXN_ID);
        c_txn.setType(NdbDictionary::Column::Bigunsigned);
        c_txn.setPrimaryKey(true);
        c_txn.setPartitionKey(true);
        c_txn.setNullable(false);
        tbl.addColumn(c_txn);
    }
    {
        NdbDictionary::Column c_role;
        c_role.setName(RONDB_RJ_COL_ROLE);
        c_role.setType(NdbDictionary::Column::Tinyunsigned);
        c_role.setPrimaryKey(true);
        c_role.setPartitionKey(false);
        c_role.setNullable(false);
        tbl.addColumn(c_role);
    }

    rondb_add_tinyunsigned(tbl, RONDB_RJ_COL_STATE);
    rondb_add_unsigned(tbl, RONDB_RJ_COL_COORD_MDS);
    rondb_add_bigunsigned(tbl, RONDB_RJ_COL_SRC_PARENT, false, false);
    rondb_add_bigunsigned(tbl, RONDB_RJ_COL_DST_PARENT, false, false);
    rondb_add_bigunsigned(tbl, RONDB_RJ_COL_SRC_CHILD, false, false);
    rondb_add_varbinary(tbl, RONDB_RJ_COL_SRC_NAME, 255, false, false);
    rondb_add_varbinary(tbl, RONDB_RJ_COL_DST_NAME, 255, false, false);
    rondb_add_varbinary(tbl, RONDB_RJ_COL_INODE_SNAP,
                        RONDB_RJ_PAYLOAD_MAX, false, false);
    rondb_add_bigunsigned(tbl, RONDB_RJ_COL_CREATED_AT, false, false);
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_ns_locks_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_NS_LOCKS);
    rondb_add_bigunsigned(tbl, RONDB_LK_COL_PART_HINT, false, true);
    rondb_add_tinyunsigned(tbl, RONDB_LK_COL_RES_CLASS);
    /* resource_class is part of composite PK. */
    {
        NdbDictionary::Table tbl2;
        tbl2.setName(RONDB_TBL_NS_LOCKS);

        NdbDictionary::Column c_hint;
        c_hint.setName(RONDB_LK_COL_PART_HINT);
        c_hint.setType(NdbDictionary::Column::Bigunsigned);
        c_hint.setPrimaryKey(true);
        c_hint.setPartitionKey(true);
        c_hint.setNullable(false);
        tbl2.addColumn(c_hint);

        NdbDictionary::Column c_class;
        c_class.setName(RONDB_LK_COL_RES_CLASS);
        c_class.setType(NdbDictionary::Column::Tinyunsigned);
        c_class.setPrimaryKey(true);
        c_class.setNullable(false);
        tbl2.addColumn(c_class);
        rondb_add_varbinary(tbl2, RONDB_LK_COL_RES_KEY,
                            RONDB_LOCK_KEY_MAX, true, false);

        rondb_add_tinyunsigned(tbl2, RONDB_LK_COL_LOCK_MODE);
        rondb_add_unsigned(tbl2, RONDB_LK_COL_HOLDER_COUNT);
        rondb_add_unsigned(tbl2, RONDB_LK_COL_OWNER_MDS);
        rondb_add_bigunsigned(tbl2, RONDB_LK_COL_OWNER_EPOCH, false, false);
        rondb_add_bigunsigned(tbl2, RONDB_LK_COL_FENCE_EPOCH, false, false);
        rondb_add_bigunsigned(tbl2, RONDB_LK_COL_GRANTED_AT, false, false);
        rondb_add_unsigned(tbl2, RONDB_LK_COL_TTL_MS);

        return rondb_create_table_if_not_exists(dict, tbl2);
    }
}

static int rondb_define_partition_map_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_PARTITION_MAP);
    {
        NdbDictionary::Column c_pid;
        c_pid.setName(RONDB_PM_COL_PART_ID);
        c_pid.setType(NdbDictionary::Column::Unsigned);
        c_pid.setPrimaryKey(true);
        c_pid.setPartitionKey(true);
        c_pid.setNullable(false);
        tbl.addColumn(c_pid);
    }
    rondb_add_unsigned(tbl, RONDB_PM_COL_OWNER_MDS);
    rondb_add_tinyunsigned(tbl, RONDB_PM_COL_STATE);
    rondb_add_varbinary(tbl, RONDB_PM_COL_SUBTREE, 4096, false, false);
    return rondb_create_table_if_not_exists(dict, tbl);
}

/* Seed a mds_meta row. */
static int rondb_seed_meta_row(rondb_shim_handle *state,
                               const char *key_name, uint64_t value)
{
    const NdbDictionary::Table *tbl;
    NdbDictionary::Dictionary *dict = rondb_get_dictionary(state);
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    char key_buf[64];

    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_META);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "seed_meta startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "seed_meta getOp");
    }
    op->insertTuple();
    std::memset(key_buf, ' ', sizeof(key_buf)); /* CHAR(64) is space-padded */
    std::strncpy(key_buf, key_name, sizeof(key_buf) - 1);
    op->equal(RONDB_META_COL_KEY, key_buf);
    (void)rondb_set_value_u64(op, RONDB_META_COL_VAL, value);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::ConstraintViolation) {
            return 0;
        }
        return rondb_report_error(err, "seed_meta commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

static int rondb_seed_root_inode(rondb_shim_handle *state)
{
    NdbDictionary::Dictionary *dict = rondb_get_dictionary(state);
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    struct mds_inode root;
    struct timespec now;
    uint8_t pk_buf[8];

    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_INODES);
    if (tbl == nullptr) { return -1; }

    if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
        std::perror("clock_gettime");
        return -1;
    }

    std::memset(&root, 0, sizeof(root));
    root.fileid = MDS_FILEID_ROOT;
    root.type = MDS_FTYPE_DIR;
    root.mode = 0755;
    root.nlink = 2;
    root.atime = now;
    root.mtime = now;
    root.ctime = now;
    root.change = 1;
    root.generation = 1;

    fdb_put_u64(pk_buf, MDS_FILEID_ROOT);
    tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "seed_root startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "seed_root getOp");
    }

    op->insertTuple();
    (void)rondb_equal_u64(op, RONDB_INO_COL_FILEID, MDS_FILEID_ROOT);
    rondb_set_inode_values(op, &root, 0);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::ConstraintViolation) {
            return 0;
        }
        return rondb_report_error(err, "seed_root commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 1 DDL: catalogue + coordination parity tables
 * ----------------------------------------------------------------------- */

/* mds_inline_data table intentionally omitted -- inline data bypasses
 * the pNFS DS layout path.  RonDB mode forces inline_enabled=false. */

static int rondb_define_xattrs_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_XATTRS);
    /* Composite PK: (fileid, attr_name). Partition by fileid. */
    {
        NdbDictionary::Table tbl2;
        tbl2.setName(RONDB_TBL_XATTRS);

        NdbDictionary::Column c_fid;
        c_fid.setName(RONDB_XA_COL_FILEID);
        c_fid.setType(NdbDictionary::Column::Bigunsigned);
        c_fid.setPrimaryKey(true);
        c_fid.setPartitionKey(true);
        c_fid.setNullable(false);
        tbl2.addColumn(c_fid);

        NdbDictionary::Column c_name;
        c_name.setName(RONDB_XA_COL_ATTR_NAME);
        c_name.setType(NdbDictionary::Column::Varbinary);
        c_name.setLength(MDS_XATTR_NAME_MAX);
        c_name.setPrimaryKey(true);
        c_name.setPartitionKey(false);
        c_name.setNullable(false);
        tbl2.addColumn(c_name);

        NdbDictionary::Column c_val;
        c_val.setName(RONDB_XA_COL_VALUE);
        c_val.setType(NdbDictionary::Column::Blob);
        c_val.setInlineSize(256);  /* bytes stored in main row */
        c_val.setPartSize(2000);   /* blob parts table segment */
        c_val.setStripeSize(0);
        c_val.setNullable(true);
        tbl2.addColumn(c_val);

        return rondb_create_table_if_not_exists(dict, tbl2);
    }
}

static int rondb_define_ds_registry_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_DS_REGISTRY);
    rondb_add_unsigned(tbl, RONDB_DSR_COL_DS_ID);
    /* Mark ds_id as PK + partition key via rebuild. */
    {
        NdbDictionary::Table tbl2;
        tbl2.setName(RONDB_TBL_DS_REGISTRY);

        NdbDictionary::Column c_id;
        c_id.setName(RONDB_DSR_COL_DS_ID);
        c_id.setType(NdbDictionary::Column::Unsigned);
        c_id.setPrimaryKey(true);
        c_id.setPartitionKey(true);
        c_id.setNullable(false);
        tbl2.addColumn(c_id);

        rondb_add_unsigned(tbl2, RONDB_DSR_COL_STATE);
        rondb_add_unsigned(tbl2, RONDB_DSR_COL_TIER);
        rondb_add_bigunsigned(tbl2, RONDB_DSR_COL_TOTAL_BYTES, false, false);
        rondb_add_bigunsigned(tbl2, RONDB_DSR_COL_USED_BYTES, false, false);
        rondb_add_unsigned(tbl2, RONDB_DSR_COL_PORT);
        rondb_add_varbinary(tbl2, RONDB_DSR_COL_ADDR, MDS_DS_ADDR_MAX,
                            false, false);
        rondb_add_tinyunsigned(tbl2, RONDB_DSR_COL_MODE);
        rondb_add_tinyunsigned(tbl2, RONDB_DSR_COL_TRANSPORT);
        rondb_add_varbinary(tbl2, RONDB_DSR_COL_HOST, MDS_DS_HOST_MAX,
                            false, false);
        rondb_add_varbinary(tbl2, RONDB_DSR_COL_EXPORT_PATH,
                            MDS_DS_EXPORT_MAX, false, false);
        rondb_add_unsigned(tbl2, RONDB_DSR_COL_TCP_PORT);
        rondb_add_unsigned(tbl2, RONDB_DSR_COL_RDMA_PORT);
        rondb_add_unsigned(tbl2, RONDB_DSR_COL_CAPS);

        return rondb_create_table_if_not_exists(dict, tbl2);
    }
}

static int rondb_define_ds_provision_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_DS_PROVISION);
    {
        NdbDictionary::Table tbl2;
        tbl2.setName(RONDB_TBL_DS_PROVISION);

        NdbDictionary::Column c_id;
        c_id.setName(RONDB_DSP_COL_DS_ID);
        c_id.setType(NdbDictionary::Column::Unsigned);
        c_id.setPrimaryKey(true);
        c_id.setPartitionKey(true);
        c_id.setNullable(false);
        tbl2.addColumn(c_id);

        rondb_add_varbinary(tbl2, RONDB_DSP_COL_SECRET, 256, false, false);
        rondb_add_bigunsigned(tbl2, RONDB_DSP_COL_EPOCH, false, false);

        return rondb_create_table_if_not_exists(dict, tbl2);
    }
}

static int rondb_define_quota_rules_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_QUOTA_RULES);
    {
        NdbDictionary::Table tbl2;
        tbl2.setName(RONDB_TBL_QUOTA_RULES);

        NdbDictionary::Column c_type;
        c_type.setName(RONDB_QR_COL_SCOPE_TYPE);
        c_type.setType(NdbDictionary::Column::Tinyunsigned);
        c_type.setPrimaryKey(true);
        c_type.setPartitionKey(false);
        c_type.setNullable(false);
        tbl2.addColumn(c_type);

        NdbDictionary::Column c_id;
        c_id.setName(RONDB_QR_COL_SCOPE_ID);
        c_id.setType(NdbDictionary::Column::Bigunsigned);
        c_id.setPrimaryKey(true);
        c_id.setPartitionKey(true);
        c_id.setNullable(false);
        tbl2.addColumn(c_id);

        rondb_add_bigunsigned(tbl2, RONDB_QR_COL_HARD_BYTES, false, false);
        rondb_add_bigunsigned(tbl2, RONDB_QR_COL_SOFT_BYTES, false, false);
        rondb_add_bigunsigned(tbl2, RONDB_QR_COL_HARD_INODES, false, false);
        rondb_add_bigunsigned(tbl2, RONDB_QR_COL_SOFT_INODES, false, false);
        rondb_add_unsigned(tbl2, RONDB_QR_COL_GRACE_SEC);

        return rondb_create_table_if_not_exists(dict, tbl2);
    }
}

static int rondb_define_quota_usage_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_QUOTA_USAGE);
    {
        NdbDictionary::Table tbl2;
        tbl2.setName(RONDB_TBL_QUOTA_USAGE);

        NdbDictionary::Column c_type;
        c_type.setName(RONDB_QU_COL_USAGE_TYPE);
        c_type.setType(NdbDictionary::Column::Tinyunsigned);
        c_type.setPrimaryKey(true);
        c_type.setPartitionKey(false);
        c_type.setNullable(false);
        tbl2.addColumn(c_type);

        NdbDictionary::Column c_id;
        c_id.setName(RONDB_QU_COL_SCOPE_ID);
        c_id.setType(NdbDictionary::Column::Bigunsigned);
        c_id.setPrimaryKey(true);
        c_id.setPartitionKey(true);
        c_id.setNullable(false);
        tbl2.addColumn(c_id);

        rondb_add_bigunsigned(tbl2, RONDB_QU_COL_USED_BYTES, false, false);
        rondb_add_bigunsigned(tbl2, RONDB_QU_COL_USED_INODES, false, false);
        rondb_add_bigunsigned(tbl2, RONDB_QU_COL_GRACE_BYTES, false, false);
        rondb_add_bigunsigned(tbl2, RONDB_QU_COL_GRACE_INODES, false, false);

        return rondb_create_table_if_not_exists(dict, tbl2);
    }
}

static int rondb_define_gc_queue_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_GC_QUEUE);
    rondb_add_bigunsigned(tbl, RONDB_GC_COL_SEQ, true, true);
    rondb_add_bigunsigned(tbl, RONDB_GC_COL_FILEID, false, false);
    rondb_add_unsigned(tbl, RONDB_GC_COL_DS_ID);
    rondb_add_unsigned(tbl, RONDB_GC_COL_NFS_FH_LEN);
    rondb_add_varbinary(tbl, RONDB_GC_COL_NFS_FH, MDS_NFS_FH_MAX,
                        false, false);
    /* owner_mds_id: which MDS drains this row (0 = legacy/unassigned). */
    rondb_add_unsigned(tbl, RONDB_GC_COL_OWNER_MDS);
    return rondb_create_table_if_not_exists(dict, tbl);
}

/*
 * Schema v6 layout_state:
 *   PK            = (fileid BIGUNSIGNED, stateid_other VARBINARY(12))
 *   partition key = fileid
 *   ix_layout_state_stateid  (ordered, stateid_other)
 *   ix_layout_state_clientid (ordered, clientid)
 * Colocates layout_state rows on the child fileid partition so the
 * fused ns_create_with_layout shim writes everything (dirent on
 * parent partition + inode/stripe/layout rows on child partition)
 * in a single Commit without NoCommit flushing.
 */
static int rondb_define_layout_state_table(NdbDictionary::Dictionary *dict)
{
    /* Base table with composite PK (fileid, stateid_other). */
    {
        NdbDictionary::Table tbl;
        tbl.setName(RONDB_TBL_LAYOUT_STATE);

        NdbDictionary::Column c_fid;
        c_fid.setName(RONDB_LS_COL_FILEID);
        c_fid.setType(NdbDictionary::Column::Bigunsigned);
        c_fid.setPrimaryKey(true);
        c_fid.setPartitionKey(true);
        c_fid.setNullable(false);
        tbl.addColumn(c_fid);

        NdbDictionary::Column c_sid;
        c_sid.setName(RONDB_LS_COL_STATEID);
        c_sid.setType(NdbDictionary::Column::Varbinary);
        c_sid.setLength(12);
        c_sid.setPrimaryKey(true);
        c_sid.setPartitionKey(false);
        c_sid.setNullable(false);
        tbl.addColumn(c_sid);

        rondb_add_bigunsigned(tbl, RONDB_LS_COL_CLIENTID, false, false);
        rondb_add_unsigned(tbl, RONDB_LS_COL_IOMODE);
        rondb_add_bigunsigned(tbl, RONDB_LS_COL_OFFSET, false, false);
        rondb_add_bigunsigned(tbl, RONDB_LS_COL_LENGTH, false, false);
        rondb_add_unsigned(tbl, RONDB_LS_COL_SEQID);
        /* Phase 9D: grant-owner identity for cross-MDS recall. */
        rondb_add_unsigned(tbl, RONDB_LS_COL_GRANT_MDS);
        rondb_add_bigunsigned(tbl, RONDB_LS_COL_GRANT_EPOCH, false, false);

        int rc = rondb_create_table_if_not_exists(dict, tbl);
        if (rc != 0) { return rc; }
    }

    /*
     * Ordered-index creation helper.  Tolerates:
     *   - SchemaObjectExists (idempotent reruns),
     *   - error 4714 "Index stats system tables do not exist"
     *     (the cluster's index statistics scaffolding hasn't been
     *     initialised by ndb_index_stat -- the index itself is
     *     functional, only statistics collection is disabled).
     * Any other error is fatal.
     *
     * Lab experience (2026-04-19): do NOT pre-check with
     * dict->getIndex(...) and skip creation when non-null.  After
     * a drop+recreate of the base table (v5->v6 upgrade path), the
     * API-side dictionary cache may still return a pointer for
     * the old index even though the underlying index was destroyed
     * with the table.  Skipping creation on that stale pointer
     * yields a silently-missing ordered index, which later surfaces
     * as LAYOUTCOMMIT NFS4ERR_IO and 0-byte files.  Always attempt
     * the creation and treat SchemaObjectExists as success; this
     * is idempotent and avoids the cache hazard entirely.
     */
    auto create_ix = [&](const char *ix_name,
                          const char *col_name) -> int {
        dict->invalidateIndex(ix_name, RONDB_TBL_LAYOUT_STATE);
        NdbDictionary::Index ix(ix_name);
        ix.setTable(RONDB_TBL_LAYOUT_STATE);
        ix.setType(NdbDictionary::Index::OrderedIndex);
        ix.setLogging(false);
        ix.addColumnName(col_name);
        if (dict->createIndex(ix) == 0) {
            return 0;
        }
        NdbError err = dict->getNdbError();
        if (err.classification == NdbError::SchemaObjectExists) {
            return 0;
        }
        if (err.code == 4714) {
            std::fprintf(stderr,
                "WARN: index %s created without stats "
                "(ndb_index_stat tables absent, code=4714)\n",
                ix_name);
            return 0;
        }
        return rondb_report_error(err, ix_name);
    };

    if (create_ix(RONDB_IX_LS_STATEID,
                  RONDB_LS_COL_STATEID) != 0) {
        return -1;
    }
    if (create_ix(RONDB_IX_LS_CLIENTID,
                  RONDB_LS_COL_CLIENTID) != 0) {
        return -1;
    }

    return 0;
}

static int rondb_define_layout_by_file_table(NdbDictionary::Dictionary *dict)
{
    {
        NdbDictionary::Table tbl2;
        tbl2.setName(RONDB_TBL_LAYOUT_BY_FILE);

        NdbDictionary::Column c_fid;
        c_fid.setName(RONDB_LBF_COL_FILEID);
        c_fid.setType(NdbDictionary::Column::Bigunsigned);
        c_fid.setPrimaryKey(true);
        c_fid.setPartitionKey(true);
        c_fid.setNullable(false);
        tbl2.addColumn(c_fid);

        NdbDictionary::Column c_sid;
        c_sid.setName(RONDB_LBF_COL_STATEID);
        c_sid.setType(NdbDictionary::Column::Varbinary);
        c_sid.setLength(12);
        c_sid.setPrimaryKey(true);
        c_sid.setPartitionKey(false);
        c_sid.setNullable(false);
        tbl2.addColumn(c_sid);

        int rc = rondb_create_table_if_not_exists(dict, tbl2);
        if (rc != 0) {
            return rc;
        }
    }

    bool ix_no_stats = false;
    auto create_ix = [&](const char *ix_name,
                          const char *col_name) -> int {
        dict->invalidateIndex(ix_name, RONDB_TBL_LAYOUT_BY_FILE);
        NdbDictionary::Index ix(ix_name);
        ix.setTable(RONDB_TBL_LAYOUT_BY_FILE);
        ix.setType(NdbDictionary::Index::OrderedIndex);
        ix.setLogging(false);
        ix.addColumnName(col_name);
        if (dict->createIndex(ix) == 0) {
            return 0;
        }
        NdbError err = dict->getNdbError();
        if (err.classification == NdbError::SchemaObjectExists) {
            return 0;
        }
        if (err.code == 4714) {
            /* The index was created successfully; only the stats-tracking
             * scaffolding (ndb_index_stat system tables) is absent.  On a
             * fresh single-node cluster that was never touched by a MySQL
             * Server instance, getIndex() after a 4714 return can fail
             * because the NDB dictionary cache records the 4714 as a
             * creation failure.  Skip the verify step — the index IS
             * functional and will become visible on the next cache
             * invalidation (e.g. first scan that references it). */
            std::fprintf(stderr,
                "WARN: index %s created without stats "
                "(ndb_index_stat tables absent, code=4714)\n",
                ix_name);
            ix_no_stats = true;
            return 0;
        }
        return rondb_report_error(err, ix_name);
    };

    if (create_ix(RONDB_IX_LBF_FILEID, RONDB_LBF_COL_FILEID) != 0) {
        return -1;
    }

    /* Best-effort: verify the index is visible on the startup dictionary.
     * Skipped when createIndex returned 4714 (ndb_index_stat absent) because
     * the propagation window is unbounded on such clusters; the runtime
     * table-scan fallback in rondb_shim_layout_iter_file handles that case. */
    if (!ix_no_stats &&
        rondb_verify_index_visible(dict, RONDB_IX_LBF_FILEID,
                                   RONDB_TBL_LAYOUT_BY_FILE) != 0) {
        std::fprintf(stderr,
            "WARN: %s not yet visible to startup dict; "
            "runtime will use table-scan fallback\n",
            RONDB_IX_LBF_FILEID);
    }
    return 0;
}

static int rondb_define_ds_layout_idx_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_DS_LAYOUT_IDX);
    {
        NdbDictionary::Table tbl2;
        tbl2.setName(RONDB_TBL_DS_LAYOUT_IDX);

        NdbDictionary::Column c_ds;
        c_ds.setName(RONDB_DLI_COL_DS_ID);
        c_ds.setType(NdbDictionary::Column::Unsigned);
        c_ds.setPrimaryKey(true);
        c_ds.setPartitionKey(false);
        c_ds.setNullable(false);
        tbl2.addColumn(c_ds);

        NdbDictionary::Column c_cid;
        c_cid.setName(RONDB_DLI_COL_CLIENTID);
        c_cid.setType(NdbDictionary::Column::Bigunsigned);
        c_cid.setPrimaryKey(true);
        c_cid.setPartitionKey(false);
        c_cid.setNullable(false);
        tbl2.addColumn(c_cid);

        NdbDictionary::Column c_fid;
        c_fid.setName(RONDB_DLI_COL_FILEID);
        c_fid.setType(NdbDictionary::Column::Bigunsigned);
        c_fid.setPrimaryKey(true);
        c_fid.setPartitionKey(true);
        c_fid.setNullable(false);
        tbl2.addColumn(c_fid);

        return rondb_create_table_if_not_exists(dict, tbl2);
    }
}

static int rondb_define_client_recovery_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_CLIENT_RECOVERY);
    rondb_add_bigunsigned(tbl, RONDB_CR_COL_CLIENTID, true, true);
    rondb_add_varbinary(tbl, RONDB_CR_COL_CO_OWNERID, 1024, false, false);
    {
        NdbDictionary::Column col;
        col.setName(RONDB_CR_COL_VERIFIER);
        col.setType(NdbDictionary::Column::Binary);
        col.setLength(8);
        col.setNullable(false);
        tbl.addColumn(col);
    }
    /* Phase 9D: recovery-owner identity for multi-MDS failover. */
    rondb_add_unsigned(tbl, RONDB_CR_COL_OWNER_MDS);
    rondb_add_bigunsigned(tbl, RONDB_CR_COL_OWNER_EPOCH, false, false);
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_seed_partition_row(rondb_shim_handle *state)
{
    NdbDictionary::Dictionary *dict = rondb_get_dictionary(state);
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    uint8_t subtree_value[4];
    uint32_t subtree_value_len = 0;
    char path_buf[2] = "/";

    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_PARTITION_MAP);
    if (tbl == nullptr) { return -1; }
    if (rondb_encode_varbinary_string(path_buf, 2U,
                                      subtree_value, sizeof(subtree_value),
                                      &subtree_value_len) != 0) {
        return -1;
    }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "seed_pm startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "seed_pm getOp");
    }

    op->insertTuple();
    op->equal(RONDB_PM_COL_PART_ID, (Uint32)0);
    op->setValue(RONDB_PM_COL_OWNER_MDS, (Uint32)0);
    op->setValue(RONDB_PM_COL_STATE, (Uint32)RONDB_PM_STATE_ACTIVE);
    op->setValue(RONDB_PM_COL_SUBTREE,
                 (const char *)subtree_value,
                 subtree_value_len);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::ConstraintViolation) {
            return 0;
        }
        return rondb_report_error(err, "seed_pm commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* Forward declarations for DDL functions defined later in this file. */
static int rondb_define_node_registry_table(NdbDictionary::Dictionary *dict);
static int rondb_define_delta_broadcast_table(NdbDictionary::Dictionary *dict);
/* Shared protocol state DDL (shared-attr) */
static int rondb_define_open_state_table(NdbDictionary::Dictionary *dict);
static int rondb_define_open_by_file_table(NdbDictionary::Dictionary *dict);
static int rondb_define_open_by_client_table(NdbDictionary::Dictionary *dict);
static int rondb_define_byte_locks_table(NdbDictionary::Dictionary *dict);
static int rondb_define_lock_by_owner_table(NdbDictionary::Dictionary *dict);
static int rondb_define_delegations_table(NdbDictionary::Dictionary *dict);
static int rondb_define_deleg_by_file_table(NdbDictionary::Dictionary *dict);
static int rondb_define_deleg_by_client_table(NdbDictionary::Dictionary *dict);
static int rondb_define_sessions_table(NdbDictionary::Dictionary *dict);
static int rondb_define_session_by_client_table(NdbDictionary::Dictionary *dict);
static int rondb_define_clients_table(NdbDictionary::Dictionary *dict);
static int rondb_define_drc_slots_table(NdbDictionary::Dictionary *dict);

int rondb_shim_bootstrap_metadata(void *handle, const char *schema)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, schema);
    NdbDictionary::Dictionary *dict;
    uint64_t schema_version = 0;

    if (state == nullptr) { return -1; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }

    /* Create all 21 tables. Order matters: no FK dependencies. */
    if (rondb_define_meta_table(dict) != 0) { return -1; }
    if (rondb_define_inodes_table(dict) != 0) { return -1; }
    if (rondb_define_dirents_table(dict) != 0) { return -1; }
    if (rondb_define_stripe_maps_table(dict) != 0) { return -1; }
    if (rondb_define_stripe_entries_table(dict) != 0) { return -1; }
    if (rondb_define_rename_journal_table(dict) != 0) { return -1; }
    if (rondb_define_ns_locks_table(dict) != 0) { return -1; }
    if (rondb_define_ns_lock_holders_table(dict) != 0) { return -1; }
    if (rondb_define_partition_map_table(dict) != 0) { return -1; }
    /* Phase 1: catalogue + coordination parity tables.
     * No inline_data table -- pNFS routes all data through DSes. */
    if (rondb_define_xattrs_table(dict) != 0) { return -1; }
    if (rondb_define_ds_registry_table(dict) != 0) { return -1; }
    if (rondb_define_ds_provision_table(dict) != 0) { return -1; }
    if (rondb_define_quota_rules_table(dict) != 0) { return -1; }
    if (rondb_define_quota_usage_table(dict) != 0) { return -1; }
    if (rondb_define_gc_queue_table(dict) != 0) { return -1; }
    /*
     * On fresh install the v6 layout_state DDL creates the correct
     * schema directly.  On upgrade from v5 the old (stateid_other)
     * PK table is dropped in the upgrade block below and this DDL
     * is re-run.
     * The v6 schema has no mds_layout_by_client table.
     */
    if (rondb_define_layout_state_table(dict) != 0) { return -1; }
    if (rondb_define_layout_by_file_table(dict) != 0) { return -1; }
    if (rondb_define_ds_layout_idx_table(dict) != 0) { return -1; }
    if (rondb_define_client_recovery_table(dict) != 0) { return -1; }
    /* Phase 9 tables */
    if (rondb_define_node_registry_table(dict) != 0) { return -1; }
    if (rondb_define_delta_broadcast_table(dict) != 0) { return -1; }
    /* Shared protocol state tables (shared-attr) */
    if (rondb_define_open_state_table(dict) != 0) { return -1; }
    if (rondb_define_open_by_file_table(dict) != 0) { return -1; }
    if (rondb_define_open_by_client_table(dict) != 0) { return -1; }
    if (rondb_define_byte_locks_table(dict) != 0) { return -1; }
    if (rondb_define_lock_by_owner_table(dict) != 0) { return -1; }
    if (rondb_define_delegations_table(dict) != 0) { return -1; }
    if (rondb_define_deleg_by_file_table(dict) != 0) { return -1; }
    if (rondb_define_deleg_by_client_table(dict) != 0) { return -1; }
    if (rondb_define_sessions_table(dict) != 0) { return -1; }
    if (rondb_define_session_by_client_table(dict) != 0) { return -1; }
    if (rondb_define_clients_table(dict) != 0) { return -1; }
    if (rondb_define_drc_slots_table(dict) != 0) { return -1; }

    /* Seed metadata rows. */
    if (rondb_seed_meta_row(state, RONDB_META_KEY_SCHEMA,
                            RONDB_SCHEMA_VERSION) != 0) {
        return -1;
    }
    if (rondb_read_meta_u64(state, RONDB_META_KEY_SCHEMA,
                            &schema_version) != 0) {
        return -1;
    }
    if (schema_version < RONDB_SCHEMA_VERSION) {
        /* Upgrade: bump schema version to current. */
        std::fprintf(stderr,
                     "INFO: RonDB schema version %llu -> %u (upgrade)\n",
                     (unsigned long long)schema_version,
                     (unsigned)RONDB_SCHEMA_VERSION);

        /* v5 -> v6: drop the old layout_state table (PK=stateid_other)
         * and the legacy layout_by_client table.  The DDL below will
         * recreate layout_state with the new composite PK on its
         * first call; layout_by_client is gone for good.  Existing
         * layout rows are lost -- acceptable for transient protocol
         * state that clients rebuild on reconnect. */
        if (schema_version < 6) {
            (void)rondb_drop_table_if_exists(dict, RONDB_TBL_LAYOUT_STATE);
            (void)rondb_drop_table_if_exists(dict, RONDB_TBL_LAYOUT_BY_CLIENT);
            /* Re-create layout_state with the new schema. */
            if (rondb_define_layout_state_table(dict) != 0) {
                return -1;
            }
        }
        /* Force-update the schema version row. */
        {
            NdbDictionary::Dictionary *upd_dict = rondb_get_dictionary(state);
            const NdbDictionary::Table *meta_tbl =
                (upd_dict != nullptr) ? upd_dict->getTable(RONDB_TBL_META) : nullptr;
            if (meta_tbl != nullptr) {
                NdbTransaction *utx = rondb_get_ndb(state)->startTransaction();
                if (utx != nullptr) {
                    NdbOperation *uop = utx->getNdbOperation(meta_tbl);
                    if (uop != nullptr) {
                        char kbuf[64];
                        std::memset(kbuf, ' ', sizeof(kbuf));
                        std::strncpy(kbuf, RONDB_META_KEY_SCHEMA, sizeof(kbuf) - 1);
                        uop->updateTuple();
                        uop->equal(RONDB_META_COL_KEY, kbuf);
                        (void)rondb_set_value_u64(uop, RONDB_META_COL_VAL,
                                                  RONDB_SCHEMA_VERSION);
                        (void)utx->execute(NdbTransaction::Commit);
                    }
                    rondb_get_ndb(state)->closeTransaction(utx);
                }
            }
        }
    } else if (schema_version > RONDB_SCHEMA_VERSION) {
        std::fprintf(stderr,
                     "ERROR: RonDB schema version %llu > expected %u; "
                     "downgrade not supported\n",
                     (unsigned long long)schema_version,
                     (unsigned)RONDB_SCHEMA_VERSION);
        return -1;
    }
    if (rondb_seed_meta_row(state, RONDB_META_KEY_FILEID,
                            MDS_FILEID_ROOT + 1) != 0) {
        return -1;
    }
    /* Seed GC sequence counter (starts at 1). */
    if (rondb_seed_meta_row(state, RONDB_META_KEY_GC_SEQ, 1) != 0) {
        return -1;
    }
    /* Seed delta broadcast seqno counter (starts at 1). */
    if (rondb_seed_meta_row(state, RONDB_META_KEY_DELTA_SEQ, 1) != 0) {
        return -1;
    }

    if (rondb_seed_root_inode(state) != 0) {
        return -1;
    }
    if (rondb_seed_partition_row(state) != 0) {
        return -1;
    }

    return 0;
}

int rondb_shim_cleanup_metadata(void *handle, const char *schema)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, schema);
    NdbDictionary::Dictionary *dict;

    if (state == nullptr) { return -1; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }

    /* Drop in reverse dependency order. */
    /* Shared protocol state tables (shared-attr) */
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_DRC_SLOTS);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_CLIENTS);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_SESSION_BY_CLIENT);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_SESSIONS);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_DELEG_BY_CLIENT);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_DELEG_BY_FILE);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_DELEGATIONS);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_LOCK_BY_OWNER);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_BYTE_LOCKS);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_OPEN_BY_CLIENT);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_OPEN_BY_FILE);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_OPEN_STATE);
    /* Phase 9 tables */
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_DELTA_BROADCAST);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_NODE_REGISTRY);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_CLIENT_RECOVERY);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_DS_LAYOUT_IDX);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_LAYOUT_BY_FILE);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_LAYOUT_BY_CLIENT);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_LAYOUT_STATE);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_GC_QUEUE);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_QUOTA_USAGE);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_QUOTA_RULES);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_DS_PROVISION);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_DS_REGISTRY);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_XATTRS);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_PARTITION_MAP);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_NS_LOCK_HOLDERS);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_NS_LOCKS);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_RENAME_JOURNAL);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_STRIPE_ENTRIES);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_STRIPE_MAPS);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_DIRENTS);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_INODES);
    (void)rondb_drop_table_if_exists(dict, RONDB_TBL_META);

    return 0;
}

/* -----------------------------------------------------------------------
 * Fileid batch allocation
 * ----------------------------------------------------------------------- */

int rondb_shim_fileid_batch_alloc(void *handle, uint32_t batch_size,
                                  uint64_t *out_base, uint32_t *out_count)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *val_attr;
    NdbError err;
    uint64_t old_val;
    char key_buf[64];

    if (state == nullptr || out_base == nullptr || out_count == nullptr ||
        batch_size == 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_META);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "fileid_alloc startTx");
    }

    /* Read current counter with exclusive lock. */
    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "fileid_alloc readOp");
    }
    op->readTuple(NdbOperation::LM_Exclusive);
    std::memset(key_buf, ' ', sizeof(key_buf));
    std::strncpy(key_buf, RONDB_META_KEY_FILEID, sizeof(key_buf) - 1);
    op->equal(RONDB_META_COL_KEY, key_buf);
    val_attr = op->getValue(RONDB_META_COL_VAL, nullptr);
    /* getValue can fail (e.g. op exhaustion); the NdbRecAttr is
     * dereferenced after execute, so a NULL here must abort. */
    if (val_attr == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "fileid_alloc getValue");
    }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "fileid_alloc read exec");
    }

    old_val = val_attr->u_64_value();

    /* Update counter = old + batch_size. */
    {
        NdbOperation *upd = tx->getNdbOperation(tbl);
        if (upd == nullptr) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "fileid_alloc updOp");
        }
        upd->updateTuple();
        upd->equal(RONDB_META_COL_KEY, key_buf);
        (void)rondb_set_value_u64(upd, RONDB_META_COL_VAL, old_val + batch_size);
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "fileid_alloc commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    *out_base = old_val;
    *out_count = batch_size;
    return 0;
}

/* -----------------------------------------------------------------------
 * Inode CRUD
 * ----------------------------------------------------------------------- */

int rondb_shim_inode_get(void *handle, uint64_t fileid,
                         uint8_t *buf, uint32_t buflen, uint32_t *outlen,
                         int read_mode)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr || buf == nullptr || outlen == nullptr ||
        buflen < RONDB_INODE_FIXED_SIZE) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_INODES);
    if (tbl == nullptr) { return -1; }

    /* TC locality: start txn hinted to fileid's partition. */
    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "inode_get startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "inode_get getOp");
    }

    op->readTuple(rondb_map_lock_mode(read_mode));
    (void)rondb_equal_u64(op, RONDB_INO_COL_FILEID, fileid);

    /* Request all columns via getValue into NdbRecAttr. */
    NdbRecAttr *a_type     = op->getValue(RONDB_INO_COL_TYPE, nullptr);
    NdbRecAttr *a_mode     = op->getValue(RONDB_INO_COL_MODE, nullptr);
    NdbRecAttr *a_nlink    = op->getValue(RONDB_INO_COL_NLINK, nullptr);
    NdbRecAttr *a_uid      = op->getValue(RONDB_INO_COL_UID, nullptr);
    NdbRecAttr *a_gid      = op->getValue(RONDB_INO_COL_GID, nullptr);
    NdbRecAttr *a_size     = op->getValue(RONDB_INO_COL_FILE_SIZE, nullptr);
    NdbRecAttr *a_sused    = op->getValue(RONDB_INO_COL_SPACE_USED, nullptr);
    NdbRecAttr *a_atsec    = op->getValue(RONDB_INO_COL_ATIME_SEC, nullptr);
    NdbRecAttr *a_atnsec   = op->getValue(RONDB_INO_COL_ATIME_NSEC, nullptr);
    NdbRecAttr *a_mtsec    = op->getValue(RONDB_INO_COL_MTIME_SEC, nullptr);
    NdbRecAttr *a_mtnsec   = op->getValue(RONDB_INO_COL_MTIME_NSEC, nullptr);
    NdbRecAttr *a_ctsec    = op->getValue(RONDB_INO_COL_CTIME_SEC, nullptr);
    NdbRecAttr *a_ctnsec   = op->getValue(RONDB_INO_COL_CTIME_NSEC, nullptr);
    NdbRecAttr *a_change   = op->getValue(RONDB_INO_COL_CHANGE, nullptr);
    NdbRecAttr *a_gen      = op->getValue(RONDB_INO_COL_GENERATION, nullptr);
    NdbRecAttr *a_flags    = op->getValue(RONDB_INO_COL_FLAGS, nullptr);
    NdbRecAttr *a_verf     = op->getValue(RONDB_INO_COL_CREATE_VERF, nullptr);
    NdbRecAttr *a_parent   = op->getValue(RONDB_INO_COL_PARENT, nullptr);
    NdbRecAttr *a_shard    = op->getValue(RONDB_INO_COL_HOME_SHARD, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "inode_get exec");
    }

    /* NDB may report NOT_FOUND on the operation even when execute
     * succeeds.  Check per-operation error before reading values. */
    if (op->getNdbError().code == 626) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 1; /* NOT_FOUND */
    }

    /* Pack columns into rondb_inode byte format via direct endian writes. */
    {
        uint8_t *p = buf;
        fdb_put_u64(p, fileid);                              p += 8;
        fdb_put_u8(p, (uint8_t)a_type->u_8_value());        p += 1;
        fdb_put_u32(p, a_mode->u_32_value());                p += 4;
        fdb_put_u32(p, a_nlink->u_32_value());               p += 4;
        fdb_put_u64(p, a_uid->u_64_value());                 p += 8;
        fdb_put_u64(p, a_gid->u_64_value());                 p += 8;
        fdb_put_u64(p, a_size->u_64_value());                p += 8;
        fdb_put_u64(p, a_sused->u_64_value());               p += 8;
        fdb_put_u64(p, a_atsec->u_64_value());               p += 8;
        fdb_put_u32(p, a_atnsec->u_32_value());              p += 4;
        fdb_put_u64(p, a_mtsec->u_64_value());               p += 8;
        fdb_put_u32(p, a_mtnsec->u_32_value());              p += 4;
        fdb_put_u64(p, a_ctsec->u_64_value());               p += 8;
        fdb_put_u32(p, a_ctnsec->u_32_value());              p += 4;
        fdb_put_u64(p, a_change->u_64_value());              p += 8;
        fdb_put_u64(p, a_gen->u_64_value());                 p += 8;
        fdb_put_u32(p, a_flags->u_32_value());               p += 4;
        fdb_put_u64(p, a_verf->u_64_value());                p += 8;
        fdb_put_u64(p, a_parent->u_64_value());              p += 8;
        fdb_put_u32(p, a_shard->u_32_value());               p += 4;
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    *outlen = RONDB_INODE_FIXED_SIZE;
    return 0;
}

int rondb_shim_inode_put(void *handle, uint64_t fileid,
                         const uint8_t *buf, uint32_t buflen)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    struct mds_inode ino;
    uint32_t shard;

    if (state == nullptr || buf == nullptr ||
        buflen < RONDB_INODE_FIXED_SIZE) {
        return -1;
    }

    /* Deserialise the byte buffer to extract typed column values. */
    if (rondb_inode_deserialize(buf, buflen, &ino, &shard) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_INODES);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "inode_put startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "inode_put getOp");
    }

    op->writeTuple();
    (void)rondb_equal_u64(op, RONDB_INO_COL_FILEID, fileid);
    rondb_set_inode_values(op, &ino, shard);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "inode_put commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_inode_setattr_atomic(void *handle, uint64_t fileid,
                                    const uint8_t *updated_buf,
                                    uint32_t buflen)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *rd_op, *wr_op;
    NdbError err;
    struct mds_inode ino;
    uint32_t shard;

    if (state == nullptr || updated_buf == nullptr ||
        buflen < RONDB_INODE_FIXED_SIZE) {
        return -1;
    }
    if (rondb_inode_deserialize(updated_buf, buflen, &ino, &shard) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_INODES);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(
            tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(
            rondb_get_ndb(state)->getNdbError(),
            "setattr_atomic startTx");
    }

    /* 1. Exclusive-lock read -- serialises concurrent setattrs on
     *    the same inode at the NDB row-lock level. */
    rd_op = tx->getNdbOperation(tbl);
    if (rd_op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "setattr_atomic rdOp");
    }
    rd_op->readTuple(NdbOperation::LM_Exclusive);
    (void)rondb_equal_u64(rd_op, RONDB_INO_COL_FILEID, fileid);
    /* We don't need to fetch values -- the caller already computed
     * the merged result.  The exclusive read is solely for locking. */
    rd_op->getValue(RONDB_INO_COL_NLINK, nullptr);

    /* 2. Update with pre-merged inode values in the same txn. */
    wr_op = tx->getNdbOperation(tbl);
    if (wr_op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "setattr_atomic wrOp");
    }
    wr_op->updateTuple();
    (void)rondb_equal_u64(wr_op, RONDB_INO_COL_FILEID, fileid);
    rondb_set_inode_values(wr_op, &ino, shard);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "setattr_atomic commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/*
 * Single-transaction read-modify-write setattr.
 *
 * Sequence:
 *   1. startTransaction (TC-hinted to fileid's partition).
 *   2. readTuple(LM_Exclusive) fetching every inode column.
 *   3. execute(NoCommit) -- gets values, holds the row lock.
 *   4. Apply caller's MDS_ATTR_* mask onto the read-back struct.
 *   5. Bump ctime, increment change.
 *   6. updateTuple with the merged values.
 *   7. execute(Commit) -- releases lock.
 *
 * The lock is held continuously from step 2 through step 7, closing
 * the lost-update window the legacy two-call form left open between
 * its committed read and its locked write.  Mask handling mirrors
 * catalogue_rondb_ns_setattr's pre-fold semantics bit-for-bit so the
 * caller-visible contract is unchanged.
 */
int rondb_shim_inode_setattr_rmw(void *handle, uint64_t fileid,
                                 uint32_t mask,
                                 const uint8_t *attrs_buf,
                                 uint32_t attrs_buflen)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *rd_op, *wr_op;
    NdbError err;
    struct mds_inode attrs_in;
    struct mds_inode merged;
    uint32_t attrs_shard = 0;
    uint32_t row_shard = 0;
    struct timespec now;

    if (state == nullptr || attrs_buf == nullptr ||
        attrs_buflen < RONDB_INODE_FIXED_SIZE) {
        return -1;
    }
    if (rondb_inode_deserialize(attrs_buf, attrs_buflen,
                                &attrs_in, &attrs_shard) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_INODES);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(
            tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(
            rondb_get_ndb(state)->getNdbError(),
            "setattr_rmw startTx");
    }

    /* 1. Exclusive-lock read, fetching every inode column. */
    rd_op = tx->getNdbOperation(tbl);
    if (rd_op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "setattr_rmw rdOp");
    }
    rd_op->readTuple(NdbOperation::LM_Exclusive);
    (void)rondb_equal_u64(rd_op, RONDB_INO_COL_FILEID, fileid);

    NdbRecAttr *a_type   = rd_op->getValue(RONDB_INO_COL_TYPE, nullptr);
    NdbRecAttr *a_mode   = rd_op->getValue(RONDB_INO_COL_MODE, nullptr);
    NdbRecAttr *a_nlink  = rd_op->getValue(RONDB_INO_COL_NLINK, nullptr);
    NdbRecAttr *a_uid    = rd_op->getValue(RONDB_INO_COL_UID, nullptr);
    NdbRecAttr *a_gid    = rd_op->getValue(RONDB_INO_COL_GID, nullptr);
    NdbRecAttr *a_size   = rd_op->getValue(RONDB_INO_COL_FILE_SIZE, nullptr);
    NdbRecAttr *a_sused  = rd_op->getValue(RONDB_INO_COL_SPACE_USED, nullptr);
    NdbRecAttr *a_atsec  = rd_op->getValue(RONDB_INO_COL_ATIME_SEC, nullptr);
    NdbRecAttr *a_atnsec = rd_op->getValue(RONDB_INO_COL_ATIME_NSEC, nullptr);
    NdbRecAttr *a_mtsec  = rd_op->getValue(RONDB_INO_COL_MTIME_SEC, nullptr);
    NdbRecAttr *a_mtnsec = rd_op->getValue(RONDB_INO_COL_MTIME_NSEC, nullptr);
    NdbRecAttr *a_ctsec  = rd_op->getValue(RONDB_INO_COL_CTIME_SEC, nullptr);
    NdbRecAttr *a_ctnsec = rd_op->getValue(RONDB_INO_COL_CTIME_NSEC, nullptr);
    NdbRecAttr *a_change = rd_op->getValue(RONDB_INO_COL_CHANGE, nullptr);
    NdbRecAttr *a_gen    = rd_op->getValue(RONDB_INO_COL_GENERATION, nullptr);
    NdbRecAttr *a_flags  = rd_op->getValue(RONDB_INO_COL_FLAGS, nullptr);
    NdbRecAttr *a_verf   = rd_op->getValue(RONDB_INO_COL_CREATE_VERF, nullptr);
    NdbRecAttr *a_parent = rd_op->getValue(RONDB_INO_COL_PARENT, nullptr);
    NdbRecAttr *a_shard  = rd_op->getValue(RONDB_INO_COL_HOME_SHARD, nullptr);

    /* NoCommit so values land and the lock is still held for the
     * follow-up updateTuple. */
    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "setattr_rmw rdExec");
    }
    if (rd_op->getNdbError().code == 626) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 1;
    }

    /* 2. Materialise the read-back inode in a local struct. */
    memset(&merged, 0, sizeof(merged));
    merged.fileid        = fileid;
    merged.type          = (enum mds_file_type)a_type->u_8_value();
    merged.mode          = a_mode->u_32_value();
    merged.nlink         = a_nlink->u_32_value();
    merged.uid           = a_uid->u_64_value();
    merged.gid           = a_gid->u_64_value();
    merged.size          = a_size->u_64_value();
    merged.space_used    = a_sused->u_64_value();
    merged.atime.tv_sec  = (time_t)a_atsec->u_64_value();
    merged.atime.tv_nsec = (long)a_atnsec->u_32_value();
    merged.mtime.tv_sec  = (time_t)a_mtsec->u_64_value();
    merged.mtime.tv_nsec = (long)a_mtnsec->u_32_value();
    merged.ctime.tv_sec  = (time_t)a_ctsec->u_64_value();
    merged.ctime.tv_nsec = (long)a_ctnsec->u_32_value();
    merged.change        = a_change->u_64_value();
    merged.generation    = a_gen->u_64_value();
    merged.flags         = a_flags->u_32_value();
    merged.create_verf   = a_verf->u_64_value();
    merged.parent_fileid = a_parent->u_64_value();
    row_shard            = a_shard->u_32_value();

    /* 3. Apply mask -- mirrors catalogue_rondb_ns_setattr's pre-fold
     *    branch list verbatim so callers see identical semantics. */
    clock_gettime(CLOCK_REALTIME, &now);
    if (mask & 0x01U)  { merged.mode  = attrs_in.mode;  }  /* MODE */
    if (mask & 0x02U)  { merged.uid   = attrs_in.uid;   }  /* UID  */
    if (mask & 0x04U)  { merged.gid   = attrs_in.gid;   }  /* GID  */
    if (mask & 0x08U)  { merged.size  = attrs_in.size;  }  /* SIZE */
    if (mask & 0x10U)  { merged.atime = attrs_in.atime; }  /* ATIME */
    if (mask & 0x20U)  { merged.mtime = attrs_in.mtime; }  /* MTIME */
    if (mask & 0x40U)  { merged.atime = now;            }  /* ATIME_NOW */
    if (mask & 0x80U)  { merged.mtime = now;            }  /* MTIME_NOW */
    if (mask & 0x100U) { merged.flags = attrs_in.flags; }  /* FLAGS */
    merged.ctime = now;
    merged.change++;

    /* 4. Update with merged values inside the same transaction. */
    wr_op = tx->getNdbOperation(tbl);
    if (wr_op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "setattr_rmw wrOp");
    }
    wr_op->updateTuple();
    (void)rondb_equal_u64(wr_op, RONDB_INO_COL_FILEID, fileid);
    rondb_set_inode_values(wr_op, &merged, row_shard);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "setattr_rmw wrExec");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_inode_del(void *handle, uint64_t fileid)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_INODES);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "inode_del startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "inode_del getOp");
    }

    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_INO_COL_FILEID, fileid);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 0; } /* row didn't exist -- ok */
        return rondb_report_error(err, "inode_del commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Dirent CRUD
 * ----------------------------------------------------------------------- */

int rondb_shim_ns_lookup(void *handle, uint64_t parent_fileid,
                         const char *name,
                         uint64_t *child_fileid, uint8_t *child_type,
                         uint8_t *inode_buf, uint32_t inode_buflen,
                         uint32_t *inode_outlen)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *dir_tbl;
    const NdbDictionary::Table *ino_tbl;
    NdbTransaction *tx;
    NdbOperation *dir_op;
    NdbOperation *ino_op;
    NdbRecAttr *a_cfid, *a_ctype;
    NdbError err;
    uint8_t name_value[MDS_MAX_NAME + 2];
    uint32_t name_value_len = 0;

    if (state == nullptr || name == nullptr ||
        child_fileid == nullptr || child_type == nullptr ||
        inode_buf == nullptr || inode_outlen == nullptr ||
        inode_buflen < RONDB_INODE_FIXED_SIZE) {
        return -1;
    }
    if (rondb_encode_varbinary_string(name, 1U,
                                      name_value, sizeof(name_value),
                                      &name_value_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    dir_tbl = dict->getTable(RONDB_TBL_DIRENTS);
    ino_tbl = dict->getTable(RONDB_TBL_INODES);
    if (dir_tbl == nullptr || ino_tbl == nullptr) { return -1; }

    /* Start transaction on dirent partition. */
    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, parent_fileid);
        tx = rondb_get_ndb(state)->startTransaction(
            dir_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(
            rondb_get_ndb(state)->getNdbError(),
            "ns_lookup startTx");
    }

    /* Op 1: read dirent. */
    dir_op = tx->getNdbOperation(dir_tbl);
    if (dir_op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ns_lookup dir_op");
    }
    dir_op->readTuple(NdbOperation::LM_CommittedRead);
    (void)rondb_equal_u64(dir_op, RONDB_DIR_COL_PARENT, parent_fileid);
    dir_op->equal(RONDB_DIR_COL_NAME,
                  (const char *)name_value, name_value_len);
    a_cfid  = dir_op->getValue(RONDB_DIR_COL_CHILD_FID, nullptr);
    a_ctype = dir_op->getValue(RONDB_DIR_COL_CHILD_TYPE, nullptr);

    /* Execute dirent read with NoCommit -- keep txn open for inode read. */
    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        if (rondb_is_temporary(err)) { return -2; }
        return rondb_report_error(err, "ns_lookup dir exec");
    }
    if (dir_op->getNdbError().code == 626) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 1; /* NOTFOUND */
    }

    uint64_t cfid = a_cfid->u_64_value();
    uint8_t ctype = (uint8_t)a_ctype->u_8_value();

    /* Op 2: read inode using the child_fileid from the dirent. */
    ino_op = tx->getNdbOperation(ino_tbl);
    if (ino_op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ns_lookup ino_op");
    }
    ino_op->readTuple(NdbOperation::LM_CommittedRead);
    (void)rondb_equal_u64(ino_op, RONDB_INO_COL_FILEID, cfid);

    NdbRecAttr *a_type   = ino_op->getValue(RONDB_INO_COL_TYPE, nullptr);
    NdbRecAttr *a_mode   = ino_op->getValue(RONDB_INO_COL_MODE, nullptr);
    NdbRecAttr *a_nlink  = ino_op->getValue(RONDB_INO_COL_NLINK, nullptr);
    NdbRecAttr *a_uid    = ino_op->getValue(RONDB_INO_COL_UID, nullptr);
    NdbRecAttr *a_gid    = ino_op->getValue(RONDB_INO_COL_GID, nullptr);
    NdbRecAttr *a_size   = ino_op->getValue(RONDB_INO_COL_FILE_SIZE, nullptr);
    NdbRecAttr *a_sused  = ino_op->getValue(RONDB_INO_COL_SPACE_USED, nullptr);
    NdbRecAttr *a_atsec  = ino_op->getValue(RONDB_INO_COL_ATIME_SEC, nullptr);
    NdbRecAttr *a_atnsec = ino_op->getValue(RONDB_INO_COL_ATIME_NSEC, nullptr);
    NdbRecAttr *a_mtsec  = ino_op->getValue(RONDB_INO_COL_MTIME_SEC, nullptr);
    NdbRecAttr *a_mtnsec = ino_op->getValue(RONDB_INO_COL_MTIME_NSEC, nullptr);
    NdbRecAttr *a_ctsec  = ino_op->getValue(RONDB_INO_COL_CTIME_SEC, nullptr);
    NdbRecAttr *a_ctnsec = ino_op->getValue(RONDB_INO_COL_CTIME_NSEC, nullptr);
    NdbRecAttr *a_change = ino_op->getValue(RONDB_INO_COL_CHANGE, nullptr);
    NdbRecAttr *a_gen    = ino_op->getValue(RONDB_INO_COL_GENERATION, nullptr);
    NdbRecAttr *a_flags  = ino_op->getValue(RONDB_INO_COL_FLAGS, nullptr);
    NdbRecAttr *a_verf   = ino_op->getValue(RONDB_INO_COL_CREATE_VERF, nullptr);
    NdbRecAttr *a_parent = ino_op->getValue(RONDB_INO_COL_PARENT, nullptr);
    NdbRecAttr *a_shard  = ino_op->getValue(RONDB_INO_COL_HOME_SHARD, nullptr);

    /* Commit -- both reads complete in this single round-trip. */
    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        if (rondb_is_temporary(err)) { return -2; }
        return rondb_report_error(err, "ns_lookup ino exec");
    }
    if (ino_op->getNdbError().code == 626) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 1; /* inode not found -- orphan dirent */
    }

    *child_fileid = cfid;
    *child_type   = ctype;

    /* Pack inode columns into byte buffer. */
    {
        uint8_t *p = inode_buf;
        fdb_put_u64(p, cfid);                                p += 8;
        fdb_put_u8(p, (uint8_t)a_type->u_8_value());        p += 1;
        fdb_put_u32(p, a_mode->u_32_value());                p += 4;
        fdb_put_u32(p, a_nlink->u_32_value());               p += 4;
        fdb_put_u64(p, a_uid->u_64_value());                 p += 8;
        fdb_put_u64(p, a_gid->u_64_value());                 p += 8;
        fdb_put_u64(p, a_size->u_64_value());                p += 8;
        fdb_put_u64(p, a_sused->u_64_value());               p += 8;
        fdb_put_u64(p, a_atsec->u_64_value());               p += 8;
        fdb_put_u32(p, a_atnsec->u_32_value());              p += 4;
        fdb_put_u64(p, a_mtsec->u_64_value());               p += 8;
        fdb_put_u32(p, a_mtnsec->u_32_value());              p += 4;
        fdb_put_u64(p, a_ctsec->u_64_value());               p += 8;
        fdb_put_u32(p, a_ctnsec->u_32_value());              p += 4;
        fdb_put_u64(p, a_change->u_64_value());              p += 8;
        fdb_put_u64(p, a_gen->u_64_value());                 p += 8;
        fdb_put_u32(p, a_flags->u_32_value());               p += 4;
        fdb_put_u64(p, a_verf->u_64_value());                p += 8;
        fdb_put_u64(p, a_parent->u_64_value());              p += 8;
        fdb_put_u32(p, a_shard->u_32_value());               p += 4;
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    *inode_outlen = RONDB_INODE_FIXED_SIZE;
    return 0;
}

int rondb_shim_dirent_get(void *handle, uint64_t parent_fileid,
                          const char *name,
                          uint64_t *child_fileid, uint8_t *child_type,
                          int read_mode)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *a_cfid, *a_ctype;
    NdbError err;
    uint8_t name_value[MDS_MAX_NAME + 2];
    uint32_t name_value_len = 0;

    if (state == nullptr || name == nullptr ||
        child_fileid == nullptr || child_type == nullptr) {
        return -1;
    }
    if (rondb_encode_varbinary_string(name, 1U,
                                      name_value, sizeof(name_value),
                                      &name_value_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DIRENTS);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, parent_fileid);
        tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "dirent_get startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "dirent_get getOp");
    }

    op->readTuple(rondb_map_lock_mode(read_mode));
    (void)rondb_equal_u64(op, RONDB_DIR_COL_PARENT, parent_fileid);
    op->equal(RONDB_DIR_COL_NAME, (const char *)name_value, name_value_len);

    a_cfid  = op->getValue(RONDB_DIR_COL_CHILD_FID, nullptr);
    a_ctype = op->getValue(RONDB_DIR_COL_CHILD_TYPE, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "dirent_get exec");
    }

    /* NDB may report NOT_FOUND on the operation even when execute
     * succeeds.  Check per-operation error before reading values. */
    if (op->getNdbError().code == 626) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 1; /* NOT_FOUND */
    }

    *child_fileid = a_cfid->u_64_value();
    *child_type   = (uint8_t)a_ctype->u_8_value();

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_dirent_put(void *handle, uint64_t parent_fileid,
                          const char *name,
                          uint64_t child_fileid, uint8_t child_type)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    uint8_t name_value[MDS_MAX_NAME + 2];
    uint32_t name_value_len = 0;

    if (state == nullptr || name == nullptr) { return -1; }
    if (rondb_encode_varbinary_string(name, 1U,
                                      name_value, sizeof(name_value),
                                      &name_value_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DIRENTS);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, parent_fileid);
        tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "dirent_put startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "dirent_put getOp");
    }

    op->writeTuple();
    (void)rondb_equal_u64(op, RONDB_DIR_COL_PARENT, parent_fileid);
    op->equal(RONDB_DIR_COL_NAME, (const char *)name_value, name_value_len);
    (void)rondb_set_value_u64(op, RONDB_DIR_COL_CHILD_FID, child_fileid);
    op->setValue(RONDB_DIR_COL_CHILD_TYPE, (Uint32)child_type);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "dirent_put commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_dirent_del(void *handle, uint64_t parent_fileid,
                          const char *name)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    uint8_t name_value[MDS_MAX_NAME + 2];
    uint32_t name_value_len = 0;

    if (state == nullptr || name == nullptr) { return -1; }
    if (rondb_encode_varbinary_string(name, 1U,
                                      name_value, sizeof(name_value),
                                      &name_value_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DIRENTS);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, parent_fileid);
        tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "dirent_del startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "dirent_del getOp");
    }

    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_DIR_COL_PARENT, parent_fileid);
    op->equal(RONDB_DIR_COL_NAME, (const char *)name_value, name_value_len);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 0; }
        return rondb_report_error(err, "dirent_del commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Readdir -- single-partition scan with explicit in-memory ordering.
 *
 * NdbScanOperation does not guarantee entry_name ordering, so the shim
 * copies all rows for the parent directory and sorts them bytewise before
 * invoking the callback.  This keeps start_after pagination stable.
 * ----------------------------------------------------------------------- */

int rondb_shim_ns_readdir(void *handle,
                          uint64_t parent_fileid,
                          const char *start_after,
                          uint32_t max_entries,
                          rondb_readdir_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    std::vector<rondb_readdir_row> rows;
    size_t first;
    size_t end;
    int rc;

    if (state == nullptr || cb == nullptr) { return -1; }

    rc = rondb_readdir_scan_parent(state, parent_fileid, &rows,
                                   "readdir startTx");
    if (rc != 0) {
        return rc;
    }

    if (rows.size() > 1) {
        rondb_readdir_sort_rows(rows);
    }

    first = rondb_readdir_start_index(rows, start_after);
    end = rondb_readdir_end_index(first, rows.size(), max_entries);

    for (size_t i = first; i < end; i++) {
        const rondb_readdir_row &row = rows[i];
        if (cb(row.child_fid, row.child_type,
               row.name.data(), (uint32_t)row.name.size(), ctx) != 0) {
            break;
        }
    }
    return 0;
}

int rondb_shim_dirent_name_for_child(void *handle,
                                     uint64_t parent_fileid,
                                     uint64_t child_fileid,
                                     char *name_out,
                                     size_t name_out_cap)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    std::vector<rondb_readdir_row> rows;
    int rc;

    if (state == nullptr || name_out == nullptr || name_out_cap == 0) {
        return -1;
    }

    rc = rondb_readdir_scan_parent(state, parent_fileid, &rows,
                                   "dirent_name_for_child startTx");
    if (rc != 0) {
        return rc;
    }

    for (const rondb_readdir_row &row : rows) {
        if (row.child_fid != child_fileid) {
            continue;
        }
        if (row.name.size() + 1 > name_out_cap) {
            return -1;
        }
        std::memcpy(name_out, row.name.data(), row.name.size());
        name_out[row.name.size()] = '\0';
        return 0;
    }
    return 1;
}

/* -----------------------------------------------------------------------
 * READDIR_PLUS -- fused dirent scan + batched inode reads in ONE
 * NdbTransaction.  The scan runs under execute(NoCommit); every row's
 * inode primary-key read is then queued on the same transaction and
 * resolved by a single execute(Commit, AO_IgnoreError).  AO_IgnoreError
 * lets per-operation 626 (NOT_FOUND) be reported per entry instead of
 * aborting the whole batch -- that covers the dirent/inode race where
 * an inode is deleted between the scan and the commit.
 *
 * Net effect: the classic N+1 round-trip cold-cache READDIR_PLUS
 * collapses to exactly two API->TC round-trips (NoCommit + Commit)
 * inside a single NdbTransaction.  TC partition locality is anchored
 * to the DIRENTS parent_fileid partition via the startTransaction
 * hint; inode reads span the INODES partitions of each child_fileid,
 * which NDB's TC fans out in parallel.
 * ----------------------------------------------------------------------- */

/**
 * Per-row set of NdbRecAttr pointers for the fused inode read.  One
 * instance per dirent row keeps the getValue() pointers alive until
 * after execute(Commit) so the values can be packed into the caller's
 * fixed-layout inode buffer.
 */
struct rondb_readdir_plus_ino_set {
    NdbOperation *op;
    NdbRecAttr *a_type, *a_mode, *a_nlink;
    NdbRecAttr *a_uid, *a_gid;
    NdbRecAttr *a_size, *a_sused;
    NdbRecAttr *a_atsec, *a_atnsec;
    NdbRecAttr *a_mtsec, *a_mtnsec;
    NdbRecAttr *a_ctsec, *a_ctnsec;
    NdbRecAttr *a_change, *a_gen;
    NdbRecAttr *a_flags, *a_verf;
    NdbRecAttr *a_parent, *a_shard;
};

/*
 * Shared READDIR_PLUS delivery tail.  Given an open transaction `tx`
 * (whose dirent scan has already executed under NoCommit) and the page
 * window rows[first, end), queue one fused inode read per dirent on the
 * SAME tx, commit with AO_IgnoreError (so a per-row 626 marks just that
 * entry's attrs unavailable instead of aborting the batch), materialise
 * each 137-byte rondb_inode, and deliver via cb.  Closes `tx` before
 * returning.  Used by both the name-order and the fileid-cursor
 * readdir_plus variants.
 */
static int rondb_readdir_plus_deliver(
    rondb_shim_handle *state,
    NdbTransaction *tx,
    const NdbDictionary::Table *ino_tbl,
    const std::vector<rondb_readdir_row> &rows,
    size_t first, size_t end,
    rondb_readdir_plus_cb cb, void *ctx)
{
    NdbError err;

    /* Empty page -- close the txn cleanly without the inode batch. */
    if (first >= end) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 0;
    }

    /* Phase 2: queue one readTuple per dirent on INODES.  All ops are
     * queued on the SAME tx so execute(Commit) below drives a single
     * API->TC round-trip that fans out the reads across partitions. */
    std::vector<rondb_readdir_plus_ino_set> ino_ops(end - first);
    for (size_t i = first; i < end; i++) {
        rondb_readdir_plus_ino_set &s = ino_ops[i - first];

        s.op = tx->getNdbOperation(ino_tbl);
        if (s.op == nullptr) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "readdir_plus getInodeOp");
        }
        s.op->readTuple(NdbOperation::LM_CommittedRead);
        (void)rondb_equal_u64(s.op, RONDB_INO_COL_FILEID,
                              rows[i].child_fid);

        /* Column order mirrors rondb_shim_inode_get to keep a single
         * source of truth for the 137-byte rondb_inode layout. */
        s.a_type   = s.op->getValue(RONDB_INO_COL_TYPE, nullptr);
        s.a_mode   = s.op->getValue(RONDB_INO_COL_MODE, nullptr);
        s.a_nlink  = s.op->getValue(RONDB_INO_COL_NLINK, nullptr);
        s.a_uid    = s.op->getValue(RONDB_INO_COL_UID, nullptr);
        s.a_gid    = s.op->getValue(RONDB_INO_COL_GID, nullptr);
        s.a_size   = s.op->getValue(RONDB_INO_COL_FILE_SIZE, nullptr);
        s.a_sused  = s.op->getValue(RONDB_INO_COL_SPACE_USED, nullptr);
        s.a_atsec  = s.op->getValue(RONDB_INO_COL_ATIME_SEC, nullptr);
        s.a_atnsec = s.op->getValue(RONDB_INO_COL_ATIME_NSEC, nullptr);
        s.a_mtsec  = s.op->getValue(RONDB_INO_COL_MTIME_SEC, nullptr);
        s.a_mtnsec = s.op->getValue(RONDB_INO_COL_MTIME_NSEC, nullptr);
        s.a_ctsec  = s.op->getValue(RONDB_INO_COL_CTIME_SEC, nullptr);
        s.a_ctnsec = s.op->getValue(RONDB_INO_COL_CTIME_NSEC, nullptr);
        s.a_change = s.op->getValue(RONDB_INO_COL_CHANGE, nullptr);
        s.a_gen    = s.op->getValue(RONDB_INO_COL_GENERATION, nullptr);
        s.a_flags  = s.op->getValue(RONDB_INO_COL_FLAGS, nullptr);
        s.a_verf   = s.op->getValue(RONDB_INO_COL_CREATE_VERF, nullptr);
        s.a_parent = s.op->getValue(RONDB_INO_COL_PARENT, nullptr);
        s.a_shard  = s.op->getValue(RONDB_INO_COL_HOME_SHARD, nullptr);
    }

    /* AO_IgnoreError: a per-op 626 (row missing) no longer aborts the
     * whole batch.  The txn-level error is only consulted as a secondary
     * signal; per-op errors are re-checked below for the valid flag. */
    if (tx->execute(NdbTransaction::Commit,
                    NdbOperation::AO_IgnoreError) == -1) {
        err = tx->getNdbError();
        /* 626 at the txn level is expected when some ops missed --
         * tolerate it and drive per-op inspection. */
        if (err.code != 626) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "readdir_plus batch exec");
        }
    }

    /* Phase 3: materialise each row into the 137-byte rondb_inode
     * buffer and deliver it via the caller's callback.  Stopping early
     * (cb returns non-zero) is honoured. */
    for (size_t i = first; i < end; i++) {
        rondb_readdir_plus_ino_set &s = ino_ops[i - first];
        uint8_t inode_buf[RONDB_INODE_FIXED_SIZE];
        int inode_valid = 1;

        if (s.op->getNdbError().code == 626) {
            inode_valid = 0;
        }

        if (inode_valid) {
            uint8_t *p = inode_buf;
            fdb_put_u64(p, rows[i].child_fid);                 p += 8;
            fdb_put_u8(p,  (uint8_t)s.a_type->u_8_value());    p += 1;
            fdb_put_u32(p, s.a_mode->u_32_value());            p += 4;
            fdb_put_u32(p, s.a_nlink->u_32_value());           p += 4;
            fdb_put_u64(p, s.a_uid->u_64_value());             p += 8;
            fdb_put_u64(p, s.a_gid->u_64_value());             p += 8;
            fdb_put_u64(p, s.a_size->u_64_value());            p += 8;
            fdb_put_u64(p, s.a_sused->u_64_value());           p += 8;
            fdb_put_u64(p, s.a_atsec->u_64_value());           p += 8;
            fdb_put_u32(p, s.a_atnsec->u_32_value());          p += 4;
            fdb_put_u64(p, s.a_mtsec->u_64_value());           p += 8;
            fdb_put_u32(p, s.a_mtnsec->u_32_value());          p += 4;
            fdb_put_u64(p, s.a_ctsec->u_64_value());           p += 8;
            fdb_put_u32(p, s.a_ctnsec->u_32_value());          p += 4;
            fdb_put_u64(p, s.a_change->u_64_value());          p += 8;
            fdb_put_u64(p, s.a_gen->u_64_value());             p += 8;
            fdb_put_u32(p, s.a_flags->u_32_value());           p += 4;
            fdb_put_u64(p, s.a_verf->u_64_value());            p += 8;
            fdb_put_u64(p, s.a_parent->u_64_value());          p += 8;
            fdb_put_u32(p, s.a_shard->u_32_value());           p += 4;
        }

        if (cb(rows[i].child_fid, rows[i].child_type,
               rows[i].name.data(), (uint32_t)rows[i].name.size(),
               inode_valid ? inode_buf : nullptr,
               inode_valid ? (uint32_t)RONDB_INODE_FIXED_SIZE : 0U,
               inode_valid, ctx) != 0) {
            break;
        }
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_ns_readdir_plus(void *handle,
                               uint64_t parent_fileid,
                               const char *start_after,
                               uint32_t max_entries,
                               rondb_readdir_plus_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *dir_tbl;
    const NdbDictionary::Table *ino_tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_name, *a_cfid, *a_ctype;
    NdbError err;
    std::vector<rondb_readdir_row> rows;
    int next_rc;

    if (state == nullptr || cb == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    dir_tbl = dict->getTable(RONDB_TBL_DIRENTS);
    ino_tbl = dict->getTable(RONDB_TBL_INODES);
    if (dir_tbl == nullptr || ino_tbl == nullptr) { return -1; }

    /* Start the txn on the DIRENTS partition for the parent fileid
     * so the scan is TC-local.  Subsequent INODES reads may land on
     * other partitions; NDB's TC fans those out in parallel inside
     * the same commit. */
    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, parent_fileid);
        tx = rondb_get_ndb(state)->startTransaction(
            dir_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(
            rondb_get_ndb(state)->getNdbError(),
            "readdir_plus startTx");
    }

    scan = tx->getNdbScanOperation(dir_tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "readdir_plus getScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "readdir_plus readTuples");
    }

    {
        NdbScanFilter filter(scan);
        filter.begin(NdbScanFilter::AND);
        filter.eq(dir_tbl->getColumn(RONDB_DIR_COL_PARENT)->getColumnNo(),
                  (Uint64)parent_fileid);
        filter.end();
    }

    a_name  = scan->getValue(RONDB_DIR_COL_NAME, nullptr);
    a_cfid  = scan->getValue(RONDB_DIR_COL_CHILD_FID, nullptr);
    a_ctype = scan->getValue(RONDB_DIR_COL_CHILD_TYPE, nullptr);

    /* Phase 1: NoCommit scan -- drives one API->TC round-trip. */
    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "readdir_plus scan exec");
    }

    while ((next_rc = scan->nextResult(true)) == 0) {
        uint64_t child_fid = a_cfid->u_64_value();
        uint8_t child_type = (uint8_t)a_ctype->u_8_value();
        const char *name_ptr = a_name->aRef();
        uint32_t name_len;
        rondb_readdir_row row;

        if (name_ptr == nullptr) { continue; }

        /* VARBINARY: 1-byte length prefix for <=255-byte names. */
        name_len = (uint32_t)(uint8_t)name_ptr[0];
        name_ptr += 1;
        row.child_fid = child_fid;
        row.child_type = child_type;
        row.name.assign(name_ptr, name_len);
        rows.push_back(row);
    }
    if (next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "readdir_plus scan nextResult");
    }
    scan->close();

    if (rows.size() > 1) {
        rondb_readdir_sort_rows(rows);
    }

    {
        size_t first = rondb_readdir_start_index(rows, start_after);
        size_t end = rondb_readdir_end_index(first, rows.size(),
                                             max_entries);
        return rondb_readdir_plus_deliver(state, tx, ino_tbl, rows,
                                          first, end, cb, ctx);
    }
}

/* -----------------------------------------------------------------------
 * READDIR_PLUS resumed by a child-fileid cursor.
 *
 * Range-scans the ordered index ix_dirents_parent_child for
 * parent_fileid == P AND child_fileid > start_after_fileid, ascending,
 * capped at max_entries -- O(log N + page) instead of the full scan +
 * sort of the name-order variant.  parent_fileid is the partition key
 * and the leading index column, so the scan is pruned to a single
 * fragment and SF_OrderBy returns rows globally ordered by child
 * fileid.  The fused inode batch then runs on a fresh transaction via
 * the shared delivery helper.
 *
 * Falls back to a full parent scan sorted by child_fileid when the
 * index is absent, which keeps the fileid-cursor semantics (stable
 * ordering, deleted-cookie safety) intact at the legacy O(N log N)
 * per-page cost.
 * ----------------------------------------------------------------------- */
int rondb_shim_ns_readdir_plus_from(void *handle,
                                    uint64_t parent_fileid,
                                    uint64_t start_after_fileid,
                                    uint32_t max_entries,
                                    rondb_readdir_plus_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *dir_tbl;
    const NdbDictionary::Table *ino_tbl;
    const NdbDictionary::Index *ix;
    NdbTransaction *tx;
    NdbError err;
    std::vector<rondb_readdir_row> rows;
    size_t first = 0;
    size_t end;

    if (state == nullptr || cb == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    dir_tbl = dict->getTable(RONDB_TBL_DIRENTS);
    ino_tbl = dict->getTable(RONDB_TBL_INODES);
    if (dir_tbl == nullptr || ino_tbl == nullptr) { return -1; }

    ix = rondb_resolve_index(dict, RONDB_IX_DIRENTS_PARENT_CHILD,
                             RONDB_TBL_DIRENTS);

    if (ix != nullptr) {
        /* Fast path: partition-pruned ordered-index range scan.  Runs
         * on its own transaction which is fully drained and closed
         * before the inode batch, so the page is bounded to at most
         * max_entries rows regardless of directory size. */
        NdbTransaction *stx;
        NdbIndexScanOperation *iscan;
        NdbRecAttr *a_name, *a_cfid, *a_ctype;
        int next_rc;

        {
            uint8_t pk_buf[8];
            fdb_put_u64(pk_buf, parent_fileid);
            stx = rondb_get_ndb(state)->startTransaction(
                dir_tbl, (const char *)pk_buf, 8);
        }
        if (stx == nullptr) {
            return rondb_report_error(
                rondb_get_ndb(state)->getNdbError(),
                "readdir_plus_from startTx");
        }
        iscan = stx->getNdbIndexScanOperation(ix);
        if (iscan == nullptr) {
            err = stx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(stx);
            return rondb_report_error(err,
                "readdir_plus_from getIndexScanOp");
        }
        if (iscan->readTuples(NdbOperation::LM_CommittedRead,
                              NdbScanOperation::SF_OrderBy) != 0) {
            err = iscan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(stx);
            return rondb_report_error(err,
                "readdir_plus_from readTuples");
        }
        {
            Uint64 p = (Uint64)parent_fileid;
            Uint64 c = (Uint64)start_after_fileid;
            /* parent_fileid == P (equality on the leading index column
             * / partition key); child_fileid > cookie.  BoundLT means
             * the bound value is strictly LESS than the returned column
             * values, i.e. child_fileid > c. */
            if (iscan->setBound(RONDB_DIR_COL_PARENT,
                                NdbIndexScanOperation::BoundEQ,
                                &p) != 0 ||
                iscan->setBound(RONDB_DIR_COL_CHILD_FID,
                                NdbIndexScanOperation::BoundLT,
                                &c) != 0) {
                err = iscan->getNdbError();
                rondb_get_ndb(state)->closeTransaction(stx);
                return rondb_report_error(err,
                    "readdir_plus_from setBound");
            }
        }
        a_name  = iscan->getValue(RONDB_DIR_COL_NAME, nullptr);
        a_cfid  = iscan->getValue(RONDB_DIR_COL_CHILD_FID, nullptr);
        a_ctype = iscan->getValue(RONDB_DIR_COL_CHILD_TYPE, nullptr);
        if (stx->execute(NdbTransaction::NoCommit) == -1) {
            err = stx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(stx);
            return rondb_report_error(err,
                "readdir_plus_from scan exec");
        }
        while ((next_rc = iscan->nextResult(true)) == 0) {
            uint64_t child_fid = a_cfid->u_64_value();
            const char *name_ptr = a_name->aRef();
            uint32_t name_len;
            rondb_readdir_row row;

            /* Defence in depth: re-apply the strict cookie bound in
             * software so a misapplied NDB bound can never surface a
             * row at or below the cookie. */
            if (child_fid <= start_after_fileid) { continue; }
            if (name_ptr == nullptr) { continue; }
            name_len = (uint32_t)(uint8_t)name_ptr[0];
            row.child_fid = child_fid;
            row.child_type = (uint8_t)a_ctype->u_8_value();
            row.name.assign(name_ptr + 1, name_len);
            rows.push_back(row);
            if (max_entries > 0 &&
                rows.size() >= (size_t)max_entries) {
                break;
            }
        }
        if (next_rc < 0) {
            err = iscan->getNdbError();
            iscan->close();
            rondb_get_ndb(state)->closeTransaction(stx);
            return rondb_report_error(err,
                "readdir_plus_from nextResult");
        }
        iscan->close();
        rondb_get_ndb(state)->closeTransaction(stx);
        /* SF_OrderBy already returned rows in child_fileid order. */
        end = rows.size();
    } else {
        /* Fallback: ordered index unavailable.  Full parent scan, then
         * sort by child_fileid and resume strictly past the cookie. */
        int rc = rondb_readdir_scan_parent(state, parent_fileid, &rows,
                                           "readdir_plus_from fallback");
        if (rc != 0) { return rc; }

        std::sort(rows.begin(), rows.end(),
                  [](const rondb_readdir_row &a,
                     const rondb_readdir_row &b) {
                      return a.child_fid < b.child_fid;
                  });
        while (first < rows.size() &&
               rows[first].child_fid <= start_after_fileid) {
            first++;
        }
        end = rondb_readdir_end_index(first, rows.size(), max_entries);
    }

    /* Fused inode batch + delivery on a fresh transaction. */
    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                  "readdir_plus_from deliver startTx");
    }
    return rondb_readdir_plus_deliver(state, tx, ino_tbl, rows,
                                      first, end, cb, ctx);
}

/* -----------------------------------------------------------------------
 * Atomic LINK (T2 -- single NDB transaction, multi-row)
 *
 * Insert dirent + update parent inode + update target inode (nlink++).
 * ----------------------------------------------------------------------- */

int rondb_shim_ns_link(void *handle,
                       uint64_t parent_fileid, const char *name,
                       uint64_t target_fileid, uint8_t target_type,
                       const uint8_t *target_inode_buf, uint32_t ti_len)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *ino_tbl, *dir_tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    struct mds_inode target_ino;
    uint32_t target_shard;
    uint8_t name_value[MDS_MAX_NAME + 2];
    uint32_t name_value_len = 0;

    if (state == nullptr || name == nullptr ||
        target_inode_buf == nullptr) {
        return -1;
    }
    if (rondb_encode_varbinary_string(name, 1U,
                                      name_value, sizeof(name_value),
                                      &name_value_len) != 0) {
        return -1;
    }

    if (rondb_inode_deserialize(target_inode_buf, ti_len,
                                &target_ino, &target_shard) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    ino_tbl = dict->getTable(RONDB_TBL_INODES);
    dir_tbl = dict->getTable(RONDB_TBL_DIRENTS);
    if (ino_tbl == nullptr || dir_tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, parent_fileid);
        tx = rondb_get_ndb(state)->startTransaction(dir_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "ns_link startTx");
    }

    /* 1. Insert dirent (PK conflict = EXISTS). */
    op = tx->getNdbOperation(dir_tbl);
    if (op == nullptr) { goto ns_link_err; }
    op->insertTuple();
    (void)rondb_equal_u64(op, RONDB_DIR_COL_PARENT, parent_fileid);
    op->equal(RONDB_DIR_COL_NAME, (const char *)name_value, name_value_len);
    (void)rondb_set_value_u64(op, RONDB_DIR_COL_CHILD_FID, target_fileid);
    op->setValue(RONDB_DIR_COL_CHILD_TYPE, (Uint32)target_type);

    /* 2. Interpreted parent inode update (atomic change ctr + timestamps;
     *    hard links don't affect parent nlink, so delta=0). */
    if (rondb_interpreted_parent_update(tx, ino_tbl, parent_fileid,
                                        0 /* nlink_delta */) != 0) {
        goto ns_link_err;
    }

    /* 3. Update target inode (nlink bumped by caller). */
    op = tx->getNdbOperation(ino_tbl);
    if (op == nullptr) { goto ns_link_err; }
    op->updateTuple();
    (void)rondb_equal_u64(op, RONDB_INO_COL_FILEID, target_ino.fileid);
    rondb_set_inode_values(op, &target_ino, target_shard);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::ConstraintViolation) {
            return 1; /* EXISTS */
        }
        return rondb_report_error(err, "ns_link commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;

ns_link_err:
    err = tx->getNdbError();
    rondb_get_ndb(state)->closeTransaction(tx);
    return rondb_report_error(err, "ns_link op");
}

/* -----------------------------------------------------------------------
 * Atomic nlink_adjust (single-row, single-transaction)
 * ----------------------------------------------------------------------- */

int rondb_shim_ns_nlink_adjust(void *handle, uint64_t fileid, int32_t delta)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbOperation *upd;
    NdbRecAttr *a_type;
    NdbRecAttr *a_mode;
    NdbRecAttr *a_nlink;
    NdbRecAttr *a_uid;
    NdbRecAttr *a_gid;
    NdbRecAttr *a_size;
    NdbRecAttr *a_sused;
    NdbRecAttr *a_atsec;
    NdbRecAttr *a_atnsec;
    NdbRecAttr *a_mtsec;
    NdbRecAttr *a_mtnsec;
    NdbRecAttr *a_ctsec;
    NdbRecAttr *a_ctnsec;
    NdbRecAttr *a_change;
    NdbRecAttr *a_gen;
    NdbRecAttr *a_flags;
    NdbRecAttr *a_verf;
    NdbRecAttr *a_parent;
    NdbRecAttr *a_shard;
    NdbError err;
    struct mds_inode ino;
    uint32_t shard;
    uint8_t buf[RONDB_INODE_FIXED_SIZE];

    if (state == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_INODES);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                  "ns_nlink_adjust startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ns_nlink_adjust getReadOp");
    }

    op->readTuple(NdbOperation::LM_Exclusive);
    (void)rondb_equal_u64(op, RONDB_INO_COL_FILEID, fileid);
    a_type = op->getValue(RONDB_INO_COL_TYPE, nullptr);
    a_mode = op->getValue(RONDB_INO_COL_MODE, nullptr);
    a_nlink = op->getValue(RONDB_INO_COL_NLINK, nullptr);
    a_uid = op->getValue(RONDB_INO_COL_UID, nullptr);
    a_gid = op->getValue(RONDB_INO_COL_GID, nullptr);
    a_size = op->getValue(RONDB_INO_COL_FILE_SIZE, nullptr);
    a_sused = op->getValue(RONDB_INO_COL_SPACE_USED, nullptr);
    a_atsec = op->getValue(RONDB_INO_COL_ATIME_SEC, nullptr);
    a_atnsec = op->getValue(RONDB_INO_COL_ATIME_NSEC, nullptr);
    a_mtsec = op->getValue(RONDB_INO_COL_MTIME_SEC, nullptr);
    a_mtnsec = op->getValue(RONDB_INO_COL_MTIME_NSEC, nullptr);
    a_ctsec = op->getValue(RONDB_INO_COL_CTIME_SEC, nullptr);
    a_ctnsec = op->getValue(RONDB_INO_COL_CTIME_NSEC, nullptr);
    a_change = op->getValue(RONDB_INO_COL_CHANGE, nullptr);
    a_gen = op->getValue(RONDB_INO_COL_GENERATION, nullptr);
    a_flags = op->getValue(RONDB_INO_COL_FLAGS, nullptr);
    a_verf = op->getValue(RONDB_INO_COL_CREATE_VERF, nullptr);
    a_parent = op->getValue(RONDB_INO_COL_PARENT, nullptr);
    a_shard = op->getValue(RONDB_INO_COL_HOME_SHARD, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "ns_nlink_adjust read");
    }

    {
        uint8_t *p = buf;

        fdb_put_u64(p, fileid);                      p += 8;
        fdb_put_u8(p, (uint8_t)a_type->u_8_value()); p += 1;
        fdb_put_u32(p, a_mode->u_32_value());        p += 4;
        fdb_put_u32(p, a_nlink->u_32_value());       p += 4;
        fdb_put_u64(p, a_uid->u_64_value());         p += 8;
        fdb_put_u64(p, a_gid->u_64_value());         p += 8;
        fdb_put_u64(p, a_size->u_64_value());        p += 8;
        fdb_put_u64(p, a_sused->u_64_value());       p += 8;
        fdb_put_u64(p, a_atsec->u_64_value());       p += 8;
        fdb_put_u32(p, a_atnsec->u_32_value());      p += 4;
        fdb_put_u64(p, a_mtsec->u_64_value());       p += 8;
        fdb_put_u32(p, a_mtnsec->u_32_value());      p += 4;
        fdb_put_u64(p, a_ctsec->u_64_value());       p += 8;
        fdb_put_u32(p, a_ctnsec->u_32_value());      p += 4;
        fdb_put_u64(p, a_change->u_64_value());      p += 8;
        fdb_put_u64(p, a_gen->u_64_value());         p += 8;
        fdb_put_u32(p, a_flags->u_32_value());       p += 4;
        fdb_put_u64(p, a_verf->u_64_value());        p += 8;
        fdb_put_u64(p, a_parent->u_64_value());      p += 8;
        fdb_put_u32(p, a_shard->u_32_value());
    }

    if (rondb_inode_deserialize(buf, sizeof(buf), &ino, &shard) != 0) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return -1;
    }

    if (delta > 0) {
        uint32_t inc = (uint32_t)delta;

        if (inc > UINT32_MAX - ino.nlink) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }
        ino.nlink += inc;
    } else if (delta < 0) {
        uint32_t dec = (uint32_t)(-(int64_t)delta);

        if (dec <= ino.nlink) {
            ino.nlink -= dec;
        } else {
            ino.nlink = 0;
        }
    } else {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 0;
    }

    /* Strict monotonicity: every mutation of this inode advertised
     * via FATTR4_CHANGE must bump `change`.  Without this, nlink
     * adjustments (hardlink create / remove) would leave `change`
     * untouched even though POSIX stat would show a different
     * nlink -- breaking the contract we make to clients via
     * FATTR4_CHANGE_ATTR_TYPE = MONOTONIC_INCR.
     *
     * POSIX requires `ctime` to update on link/unlink; do so now
     * so the client sees a coherent (change, ctime) pair. */
    {
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        ino.change += 1;
        ino.ctime = now;
    }

    upd = tx->getNdbOperation(tbl);
    if (upd == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ns_nlink_adjust getUpdateOp");
    }

    upd->updateTuple();
    (void)rondb_equal_u64(upd, RONDB_INO_COL_FILEID, fileid);
    rondb_set_inode_values(upd, &ino, shard);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ns_nlink_adjust commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Stripe map CRUD (header + entries child table)
 *
 * mds_stripe_maps: header row (fileid PK, stripe_count, stripe_unit,
 *                  mirror_count).  Partition key = fileid.
 * mds_stripe_entries: child rows (fileid PK + ordinal PK, ds_id,
 *                     nfs_fh_len, nfs_fh).  Partition key = fileid.
 *
 * Both tables share the same partition key (fileid) so all stripe
 * operations for one file hit a single data node partition.
 *
 * stripe_get: PK read on header + batched PK reads on entries by
 *             (fileid, ordinal=0..stripe_count-1), all in one NDB txn.
 * stripe_put: write header + insert N entries in one NDB transaction.
 * stripe_del: delete header + PK-delete stripe_entries by ordinal.
 * ----------------------------------------------------------------------- */

int rondb_shim_stripe_get(void *handle, uint64_t fileid,
                          uint32_t *stripe_count, uint32_t *stripe_unit,
                          uint32_t *mirror_count,
                          uint8_t *entries_buf, uint32_t entries_buflen,
                          uint32_t *entries_outlen)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *hdr_tbl, *ent_tbl;
    NdbTransaction *tx;
    NdbOperation *hdr_op;
    NdbRecAttr *a_sc, *a_su, *a_mc;
    NdbError err;
    uint32_t sc_val, n_entries;
    uint32_t required_buf;

    if (state == nullptr || stripe_count == nullptr ||
        stripe_unit == nullptr || mirror_count == nullptr ||
        entries_buf == nullptr || entries_outlen == nullptr) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    hdr_tbl = dict->getTable(RONDB_TBL_STRIPE_MAPS);
    ent_tbl = dict->getTable(RONDB_TBL_STRIPE_ENTRIES);
    if (hdr_tbl == nullptr || ent_tbl == nullptr) { return -1; }

    /* TC locality hint: fileid partition. */
    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(hdr_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "stripe_get startTx");
    }

    /* 1. PK read on stripe_maps header. */
    hdr_op = tx->getNdbOperation(hdr_tbl);
    if (hdr_op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "stripe_get hdr readOp");
    }
    hdr_op->readTuple(NdbOperation::LM_CommittedRead);
    (void)rondb_equal_u64(hdr_op, RONDB_SM_COL_FILEID, fileid);
    a_sc = hdr_op->getValue(RONDB_SM_COL_STRIPE_CNT, nullptr);
    a_su = hdr_op->getValue(RONDB_SM_COL_STRIPE_UNIT, nullptr);
    a_mc = hdr_op->getValue(RONDB_SM_COL_MIRROR_CNT, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "stripe_get hdr exec");
    }

    /* Check per-op error: NDB execute succeeds even if the row
     * doesn't exist; we must check the operation's NdbError. */
    {
        NdbError op_err = hdr_op->getNdbError();
        if (op_err.code == 626 ||
            op_err.classification == NdbError::NoDataFound) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return 1; /* NOTFOUND */
        }
    }

    sc_val = a_sc->u_32_value();
    *stripe_count = sc_val;
    *stripe_unit  = a_su->u_32_value();
    *mirror_count = a_mc->u_32_value();

    if (sc_val == 0) {
        /* No entries to read. */
        *entries_outlen = 0;
        rondb_get_ndb(state)->closeTransaction(tx);
        return 0;
    }

    required_buf = sc_val * RONDB_STRIPE_ENTRY_SIZE;
    if (entries_buflen < required_buf) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return -1; /* buffer too small */
    }

    /* 2. Batched PK reads on mds_stripe_entries by (fileid, ordinal).
     *
     * mds_stripe_entries has composite PK (fileid, ordinal) with fileid
     * as the partition key (see rondb_define_stripe_entries_table).
     * Issue sc_val PK reads in the same NDB transaction; they are all
     * local to the fileid partition (same TC we already picked), so a
     * single tx->execute(Commit) round-trip fetches every entry.
     *
     * This replaces a full NdbScanOperation on mds_stripe_entries,
     * which cost ~25 ms per call on even small clusters because the
     * scan walked every row on every data node before applying the
     * server-side fileid filter.  Batched PK reads finish in ~1 ms and
     * match the pattern used by rondb_shim_stripe_get_and_layout_grant
     * on the fused-layout path. */
    {
        struct pk_read_attrs {
            NdbRecAttr *a_dsid;
            NdbRecAttr *a_fhlen;
            NdbRecAttr *a_fh;
            NdbOperation *rd_op;
        };
        std::vector<pk_read_attrs> rd_attrs(sc_val);

        for (uint32_t i = 0; i < sc_val; i++) {
            NdbOperation *rd = tx->getNdbOperation(ent_tbl);
            if (rd == nullptr) {
                err = tx->getNdbError();
                rondb_get_ndb(state)->closeTransaction(tx);
                return rondb_report_error(err, "stripe_get entry readOp");
            }
            rd->readTuple(NdbOperation::LM_CommittedRead);
            (void)rondb_equal_u64(rd, RONDB_SE_COL_FILEID, fileid);
            rd->equal(RONDB_SE_COL_ORDINAL, (Uint32)i);
            rd_attrs[i].a_dsid  = rd->getValue(RONDB_SE_COL_DS_ID, nullptr);
            rd_attrs[i].a_fhlen = rd->getValue(RONDB_SE_COL_NFS_FH_LEN, nullptr);
            rd_attrs[i].a_fh    = rd->getValue(RONDB_SE_COL_NFS_FH, nullptr);
            rd_attrs[i].rd_op   = rd;
        }

        /* Single commit round-trip for all sc_val PK reads. */
        if (tx->execute(NdbTransaction::Commit) == -1) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "stripe_get entries exec");
        }

        /* Entries are written in ordinal order by stripe_put, so results
         * come back in the same order we issued them.  Missing entries
         * (per-op 626) are skipped -- matches the fused-path behaviour. */
        n_entries = 0;
        for (uint32_t i = 0; i < sc_val; i++) {
            NdbError op_err = rd_attrs[i].rd_op->getNdbError();
            if (op_err.code == 626 ||
                op_err.classification == NdbError::NoDataFound) {
                continue;
            }

            struct mds_ds_map_entry me;
            uint32_t fh_len_val = rd_attrs[i].a_fhlen->u_32_value();
            if (fh_len_val > MDS_NFS_FH_MAX) {
                fh_len_val = MDS_NFS_FH_MAX;
            }

            me.ds_id = rd_attrs[i].a_dsid->u_32_value();
            me.nfs_fh_len = fh_len_val;
            std::memset(me.nfs_fh, 0, MDS_NFS_FH_MAX);

            /* VARBINARY(128): 1-byte length prefix + data. */
            const char *fh_ptr = rd_attrs[i].a_fh->aRef();
            if (fh_ptr != nullptr && fh_len_val > 0) {
                uint32_t vb_len = (uint32_t)(uint8_t)fh_ptr[0];
                if (vb_len > MDS_NFS_FH_MAX) {
                    vb_len = MDS_NFS_FH_MAX;
                }
                std::memcpy(me.nfs_fh, fh_ptr + 1, vb_len);
            }

            uint8_t *dst = entries_buf + (n_entries * RONDB_STRIPE_ENTRY_SIZE);
            if (rondb_stripe_entry_serialize(&me, dst,
                                            RONDB_STRIPE_ENTRY_SIZE) < 0) {
                rondb_get_ndb(state)->closeTransaction(tx);
                return -1;
            }
            n_entries++;
        }

        *entries_outlen = n_entries * RONDB_STRIPE_ENTRY_SIZE;
        rondb_get_ndb(state)->closeTransaction(tx);
    }

    return 0;
}

int rondb_shim_stripe_put(void *handle, uint64_t fileid,
                          uint32_t stripe_count, uint32_t stripe_unit,
                          uint32_t mirror_count,
                          const uint8_t *entries_buf, uint32_t entries_len)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *hdr_tbl, *ent_tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr || (stripe_count > 0 && entries_buf == nullptr)) {
        return -1;
    }
    if (stripe_count > 0 &&
        entries_len < stripe_count * RONDB_STRIPE_ENTRY_SIZE) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    hdr_tbl = dict->getTable(RONDB_TBL_STRIPE_MAPS);
    ent_tbl = dict->getTable(RONDB_TBL_STRIPE_ENTRIES);
    if (hdr_tbl == nullptr || ent_tbl == nullptr) { return -1; }

    /* TC locality: fileid partition. */
    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(hdr_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "stripe_put startTx");
    }

    /* 1. Write header (writeTuple = upsert). */
    op = tx->getNdbOperation(hdr_tbl);
    if (op == nullptr) { goto stripe_put_err; }
    op->writeTuple();
    (void)rondb_equal_u64(op, RONDB_SM_COL_FILEID, fileid);
    op->setValue(RONDB_SM_COL_STRIPE_CNT, stripe_count);
    op->setValue(RONDB_SM_COL_STRIPE_UNIT, stripe_unit);
    op->setValue(RONDB_SM_COL_MIRROR_CNT, mirror_count);

    /* 2. Insert entry rows. */
    for (uint32_t i = 0; i < stripe_count; i++) {
        const uint8_t *src = entries_buf + (i * RONDB_STRIPE_ENTRY_SIZE);
        struct mds_ds_map_entry me;
        uint8_t fh_encoded[MDS_NFS_FH_MAX + 2];
        uint32_t fh_encoded_len = 0;

        if (rondb_stripe_entry_deserialize(src, RONDB_STRIPE_ENTRY_SIZE,
                                           &me) != 0) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }

        /* Encode nfs_fh as VARBINARY(128): 1-byte length prefix. */
        if (rondb_encode_varbinary_value(me.nfs_fh, me.nfs_fh_len, 1U,
                                         fh_encoded, sizeof(fh_encoded),
                                         &fh_encoded_len) != 0) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }

        op = tx->getNdbOperation(ent_tbl);
        if (op == nullptr) { goto stripe_put_err; }
        op->writeTuple();
        (void)rondb_equal_u64(op, RONDB_SE_COL_FILEID, fileid);
        op->equal(RONDB_SE_COL_ORDINAL, i);
        op->setValue(RONDB_SE_COL_DS_ID, me.ds_id);
        op->setValue(RONDB_SE_COL_NFS_FH_LEN, me.nfs_fh_len);
        op->setValue(RONDB_SE_COL_NFS_FH,
                     (const char *)fh_encoded,
                     fh_encoded_len);
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (rondb_is_temporary(err)) {
            return -2; /* Retryable -- caller should retry */
        }
        return rondb_report_error(err, "stripe_put commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;

stripe_put_err:
    err = tx->getNdbError();
    rondb_get_ndb(state)->closeTransaction(tx);
    if (rondb_is_temporary(err)) {
        return -2;
    }
    return rondb_report_error(err, "stripe_put op");
}

/*
 * Read mds_stripe_maps.stripe_count for fileid inside an open txn.
 * Returns 0 when the header row is absent (not an error).
 */
static int rondb_txn_read_stripe_entry_count(NdbTransaction *tx,
                                             const NdbDictionary::Table *hdr_tbl,
                                             uint64_t fileid,
                                             uint32_t *stripe_count_out,
                                             NdbError *err_out)
{
    NdbOperation *hdr_op;
    NdbRecAttr *a_sc;
    NdbError err;

    if (stripe_count_out == nullptr) {
        return -1;
    }
    *stripe_count_out = 0;

    hdr_op = tx->getNdbOperation(hdr_tbl);
    if (hdr_op == nullptr) {
        if (err_out != nullptr) {
            *err_out = tx->getNdbError();
        }
        return -1;
    }
    hdr_op->readTuple(NdbOperation::LM_CommittedRead);
    (void)rondb_equal_u64(hdr_op, RONDB_SM_COL_FILEID, fileid);
    a_sc = hdr_op->getValue(RONDB_SM_COL_STRIPE_CNT, nullptr);
    if (a_sc == nullptr) {
        return -1;
    }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        if (err_out != nullptr) {
            *err_out = err;
        }
        if (rondb_is_temporary(err)) {
            return -2;
        }
        return -1;
    }

    err = hdr_op->getNdbError();
    if (err.code == 626 ||
        err.classification == NdbError::NoDataFound) {
        return 0;
    }
    if (err.code != 0) {
        if (err_out != nullptr) {
            *err_out = err;
        }
        if (rondb_is_temporary(err)) {
            return -2;
        }
        return -1;
    }

    *stripe_count_out = a_sc->u_32_value();
    return 0;
}

/*
 * Queue PK deletes for mds_stripe_maps + mds_stripe_entries rows.
 * Caller commits the transaction.
 */
static int rondb_txn_append_stripe_pk_deletes(NdbTransaction *tx,
                                              const NdbDictionary::Table *hdr_tbl,
                                              const NdbDictionary::Table *ent_tbl,
                                              uint64_t fileid,
                                              uint32_t stripe_count)
{
    NdbOperation *op;
    uint32_t capped = stripe_count;

    if (stripe_count == 0) {
        return 0;
    }
    if (capped > (uint32_t)MDS_MAX_STRIPES) {
        capped = (uint32_t)MDS_MAX_STRIPES;
    }

    op = tx->getNdbOperation(hdr_tbl);
    if (op == nullptr) {
        return -1;
    }
    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_SM_COL_FILEID, fileid);

    for (uint32_t i = 0; i < capped; i++) {
        op = tx->getNdbOperation(ent_tbl);
        if (op == nullptr) {
            return -1;
        }
        op->deleteTuple();
        (void)rondb_equal_u64(op, RONDB_SE_COL_FILEID, fileid);
        op->equal(RONDB_SE_COL_ORDINAL, (Uint32)i);
    }
    return 0;
}

/*
 * stripe_del -- delete stripe_maps header + stripe_entries rows for fileid.
 *
 * Uses batched PK deletes on (fileid, ordinal) instead of an exclusive
 * NdbScanOperation, which deadlocked under concurrent mdtest mass-delete.
 * Transient NDB errors return -2 for the C-wrapper retry loop.
 */
int rondb_shim_stripe_del(void *handle, uint64_t fileid,
                          uint32_t max_entries)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *hdr_tbl, *ent_tbl;
    NdbTransaction *tx;
    NdbError err;
    uint32_t stripe_count = max_entries;
    int rc;

    if (state == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    hdr_tbl = dict->getTable(RONDB_TBL_STRIPE_MAPS);
    ent_tbl = dict->getTable(RONDB_TBL_STRIPE_ENTRIES);
    if (hdr_tbl == nullptr || ent_tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(hdr_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "stripe_del startTx");
    }

    if (stripe_count == 0) {
        rc = rondb_txn_read_stripe_entry_count(tx, hdr_tbl, fileid,
                                               &stripe_count, &err);
        if (rc == -2) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return -2;
        }
        if (rc != 0) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "stripe_del hdr read");
        }
        if (stripe_count == 0) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return 0;
        }
    }

    if (rondb_txn_append_stripe_pk_deletes(tx, hdr_tbl, ent_tbl,
                                           fileid, stripe_count) != 0) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "stripe_del delOp");
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626 ||
            err.classification == NdbError::NoDataFound) {
            return 0;
        }
        if (rondb_is_temporary(err)) {
            return -2;
        }
        return rondb_report_error(err, "stripe_del commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 2: Fused stripe_get + layout_grant (single NDB transaction)
 *
 * Reads stripe_maps header + entries, then writes layout_state +
 * layout_by_client + layout_by_file + ds_layout_idx -- all in one NDB
 * transaction.  Saves 1 NDB round-trip vs separate calls.
 *
 * Returns: 0 = success, 1 = stripe NOTFOUND, -1 = error.
 * ----------------------------------------------------------------------- */

int rondb_shim_stripe_get_and_layout_grant(
    void *handle, uint64_t fileid,
    uint32_t *stripe_count, uint32_t *stripe_unit, uint32_t *mirror_count,
    uint8_t *entries_buf, uint32_t entries_buflen, uint32_t *entries_outlen,
    const uint8_t stateid_other[12],
    uint64_t clientid, uint32_t iomode, uint64_t offset, uint64_t length,
    uint32_t seqid, uint32_t mds_id)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *hdr_tbl, *ent_tbl;
    const NdbDictionary::Table *ls_tbl, *lbc_tbl, *lbf_tbl, *dli_tbl;
    NdbTransaction *tx;
    NdbOperation *hdr_op, *op;
    NdbRecAttr *a_sc, *a_su, *a_mc;
    NdbScanOperation *scan;
    NdbRecAttr *a_ord, *a_dsid, *a_fhlen, *a_fh;
    NdbError err;
    int next_rc;
    uint32_t sc_val, n_entries;
    uint8_t sid_enc[14];
    uint32_t sid_enc_len = 0;

    if (state == nullptr || stripe_count == nullptr ||
        stripe_unit == nullptr || mirror_count == nullptr ||
        entries_buf == nullptr || entries_outlen == nullptr ||
        stateid_other == nullptr) {
        return -1;
    }
    if (rondb_encode_varbinary_value(stateid_other, 12, 1U,
                                     sid_enc, sizeof(sid_enc),
                                     &sid_enc_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    hdr_tbl = dict->getTable(RONDB_TBL_STRIPE_MAPS);
    ent_tbl = dict->getTable(RONDB_TBL_STRIPE_ENTRIES);
    ls_tbl  = dict->getTable(RONDB_TBL_LAYOUT_STATE);
    lbf_tbl = dict->getTable(RONDB_TBL_LAYOUT_BY_FILE);
    lbc_tbl = nullptr; /* v6: mds_layout_by_client is gone. */
    (void)lbc_tbl;
    /* ds_layout_idx not used in fused path (written separately). */
    if (hdr_tbl == nullptr || ent_tbl == nullptr ||
        ls_tbl == nullptr || lbf_tbl == nullptr) {
        std::fprintf(stderr,
            "ERROR: fused_lg missing table(s):"
            " stripe_maps=%d stripe_entries=%d"
            " layout_state=%d layout_by_file=%d\n",
            hdr_tbl != nullptr, ent_tbl != nullptr,
            ls_tbl != nullptr, lbf_tbl != nullptr);
        return -1;
    }

    /* TC locality hint on fileid partition. */
    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(
            hdr_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        err = rondb_get_ndb(state)->getNdbError();
        if (rondb_is_temporary(err)) { return -2; }
        return rondb_report_error(err, "fused_lg startTx");
    }

    /* Step 1: PK read stripe header (NoCommit). */
    hdr_op = tx->getNdbOperation(hdr_tbl);
    if (hdr_op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (rondb_is_temporary(err)) { return -2; }
        return rondb_report_error(err, "fused_lg hdr readOp");
    }
    hdr_op->readTuple(NdbOperation::LM_CommittedRead);
    (void)rondb_equal_u64(hdr_op, RONDB_SM_COL_FILEID, fileid);
    a_sc = hdr_op->getValue(RONDB_SM_COL_STRIPE_CNT, nullptr);
    a_su = hdr_op->getValue(RONDB_SM_COL_STRIPE_UNIT, nullptr);
    a_mc = hdr_op->getValue(RONDB_SM_COL_MIRROR_CNT, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (rondb_is_temporary(err)) { return -2; }
        return rondb_report_error(err, "fused_lg hdr exec");
    }

    /* Check per-op error for NOTFOUND. */
    {
        NdbError op_err = hdr_op->getNdbError();
        if (op_err.code == 626 ||
            op_err.classification == NdbError::NoDataFound) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return 1; /* NOTFOUND -- caller does placement */
        }
    }

    sc_val = a_sc->u_32_value();
    *stripe_count = sc_val;
    *stripe_unit  = a_su->u_32_value();
    *mirror_count = a_mc->u_32_value();

    if (sc_val == 0) {
        *entries_outlen = 0;
        rondb_get_ndb(state)->closeTransaction(tx);
        return 0;
    }

    {
        uint32_t required_buf = sc_val * RONDB_STRIPE_ENTRY_SIZE;
        if (entries_buflen < required_buf) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }
    }

    /* Step 2: Batch PK reads for stripe entries by (fileid, ordinal).
     * We know ordinals are 0..stripe_count-1 from the header.
     * PK reads (unlike scans) can be mixed with subsequent writes
     * in the same NDB transaction commit. */
    {
        struct pk_read_attrs {
            NdbRecAttr *a_dsid;
            NdbRecAttr *a_fhlen;
            NdbRecAttr *a_fh;
            NdbOperation *rd_op;
        };
        std::vector<pk_read_attrs> rd_attrs(sc_val);

        for (uint32_t i = 0; i < sc_val; i++) {
            NdbOperation *rd = tx->getNdbOperation(ent_tbl);
            if (rd == nullptr) { goto fused_lg_err; }
            rd->readTuple(NdbOperation::LM_CommittedRead);
            (void)rondb_equal_u64(rd, RONDB_SE_COL_FILEID, fileid);
            rd->equal(RONDB_SE_COL_ORDINAL, (Uint32)i);
            rd_attrs[i].a_dsid  = rd->getValue(RONDB_SE_COL_DS_ID, nullptr);
            rd_attrs[i].a_fhlen = rd->getValue(RONDB_SE_COL_NFS_FH_LEN, nullptr);
            rd_attrs[i].a_fh    = rd->getValue(RONDB_SE_COL_NFS_FH, nullptr);
            rd_attrs[i].rd_op   = rd;
        }

        /* Step 3: Append layout_state + indexes to the SAME batch,
         * then commit everything in one network round-trip. */

        /* 3a. layout_state row. v6 PK = (fileid, stateid_other). */
        op = tx->getNdbOperation(ls_tbl);
        if (op == nullptr) { goto fused_lg_err; }
        op->writeTuple();
        (void)rondb_equal_u64(op, RONDB_LS_COL_FILEID, fileid);
        op->equal(RONDB_LS_COL_STATEID,
                  (const char *)sid_enc, sid_enc_len);
        (void)rondb_set_value_u64(op, RONDB_LS_COL_CLIENTID, clientid);
        op->setValue(RONDB_LS_COL_IOMODE, iomode);
        (void)rondb_set_value_u64(op, RONDB_LS_COL_OFFSET, offset);
        (void)rondb_set_value_u64(op, RONDB_LS_COL_LENGTH, length);
        op->setValue(RONDB_LS_COL_SEQID, seqid);
        if (ls_tbl->getColumn(RONDB_LS_COL_GRANT_MDS) != nullptr) {
            op->setValue(RONDB_LS_COL_GRANT_MDS, (Uint32)mds_id);
            (void)rondb_set_value_u64(op, RONDB_LS_COL_GRANT_EPOCH, (Uint64)0);
        }

        /* 3b. layout_by_file row -- required by
         *     mds_coord_layout_iter_file for byte-range
         *     CB_LAYOUTRECALL conflict detection (Mark's bug:
         *     bugs from mark/mds_byte_range_layoutrecall.md).
         *
         *     The previous version skipped this index write in the
         *     fused path on the assumption that a fallback
         *     layout_state_put would persist it.  In reality the
         *     fused path returns success directly (compound_layout.c
         *     calls catalogue_rondb_layoutget_fused, observes
         *     MDS_OK, and goes straight to fill_layoutget_result),
         *     so layout_by_file was never written for any grant
         *     that hit the fused fast path.  That left the
         *     byte-range recall scanner unable to see existing
         *     holders and silently suppressed every conflict
         *     CB_LAYOUTRECALL.
         *
         *     Adding the write here keeps the fused fast path
         *     atomic (one Commit) and matches the row set produced
         *     by the non-fused rondb_shim_layout_state_put. */
        op = tx->getNdbOperation(lbf_tbl);
        if (op == nullptr) { goto fused_lg_err; }
        op->writeTuple();
        (void)rondb_equal_u64(op, RONDB_LBF_COL_FILEID, fileid);
        op->equal(RONDB_LBF_COL_STATEID,
                  (const char *)sid_enc, sid_enc_len);

        /* 3c. ds_layout_idx is keyed on per-stripe ds_id which is
         *     not known until the entry PK reads above commit.
         *     The DS-failure recall path already tolerates a
         *     missing ds_layout_idx row by falling through to a
         *     full file scan, so leaving it written by the
         *     non-fused path (or a follow-up post-commit insert)
         *     is the conservative choice for now.  Tracked in
         *     docs/pending.md. */

        /* Commit: header PK read + entry PK reads + layout writes
         * all in ONE network round-trip (2 NDB executes total:
         * 1 NoCommit for header, 1 Commit for entries+writes). */
        if (tx->execute(NdbTransaction::Commit) == -1) {
            err = tx->getNdbError();
            bool is_transient = rondb_is_temporary(err);
            /* Diagnostic: check per-op errors when tx error is 0.
             * Skipped for transient commit errors (lock contention
             * under multi-MDS stress used to spam ~4k lines per run
             * in the 3-MDS lab); the caller retries on -2. */
            if (err.code == 0 && !is_transient) {
                for (uint32_t di = 0; di < sc_val; di++) {
                    NdbError oerr = rd_attrs[di].rd_op->getNdbError();
                    if (oerr.code != 0) {
                        std::fprintf(stderr,
                            "DIAG: fused_lg entry[%u] op err: "
                            "code=%d msg=%s\n",
                            di, oerr.code,
                            oerr.message ? oerr.message : "?");
                    }
                }
            }
            rondb_get_ndb(state)->closeTransaction(tx);
            if (is_transient) { return -2; }
            return rondb_report_error(err, "fused_lg commit");
        }

        /* Extract entry data from committed PK reads. */
        n_entries = 0;
        for (uint32_t i = 0; i < sc_val; i++) {
            NdbError op_err = rd_attrs[i].rd_op->getNdbError();
            if (op_err.code == 626) { continue; } /* entry missing */

            uint32_t ds_id_val  = rd_attrs[i].a_dsid->u_32_value();
            uint32_t fh_len_val = rd_attrs[i].a_fhlen->u_32_value();
            if (fh_len_val > MDS_NFS_FH_MAX) { fh_len_val = MDS_NFS_FH_MAX; }

            struct mds_ds_map_entry me;
            me.ds_id = ds_id_val;
            me.nfs_fh_len = fh_len_val;
            std::memset(me.nfs_fh, 0, MDS_NFS_FH_MAX);
            const char *fh_ptr = rd_attrs[i].a_fh->aRef();
            if (fh_ptr != nullptr && fh_len_val > 0) {
                uint32_t vb_len = (uint32_t)(uint8_t)fh_ptr[0];
                if (vb_len > MDS_NFS_FH_MAX) { vb_len = MDS_NFS_FH_MAX; }
                std::memcpy(me.nfs_fh, fh_ptr + 1, vb_len);
            }

            uint8_t *dst = entries_buf + (n_entries * RONDB_STRIPE_ENTRY_SIZE);
            if (rondb_stripe_entry_serialize(&me, dst,
                                            RONDB_STRIPE_ENTRY_SIZE) < 0) {
                rondb_get_ndb(state)->closeTransaction(tx);
                return -1;
            }

            /* Also write ds_layout_idx for this DS (already committed
             * via the PK write batch above -- we pre-add them before
             * knowing which entries exist, using the header count). */
            n_entries++;
        }
        *entries_outlen = n_entries * RONDB_STRIPE_ENTRY_SIZE;

        rondb_get_ndb(state)->closeTransaction(tx);
        return 0;
    }

fused_lg_err:
    err = tx->getNdbError();
    rondb_get_ndb(state)->closeTransaction(tx);
    if (rondb_is_temporary(err)) { return -2; }
    return rondb_report_error(err, "fused_lg op");
}

/* -----------------------------------------------------------------------
 * Extended attribute CRUD (composite PK: fileid + attr_name)
 *
 * mds_xattrs: fileid(PK, partition key) + attr_name(VARBINARY 255, PK)
 *             + value(LONGVARBINARY 64K).
 * attr_name uses 1-byte VARBINARY prefix; value uses 2-byte LONGVARBINARY.
 * ----------------------------------------------------------------------- */

int rondb_shim_xattr_get(void *handle, uint64_t fileid,
                         const char *name,
                         uint8_t *val_buf, uint32_t val_buflen,
                         uint32_t *val_outlen)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbBlob *blob_handle;
    NdbError err;
    uint8_t name_enc[MDS_XATTR_NAME_MAX + 2];
    uint32_t name_enc_len = 0;

    if (state == nullptr || name == nullptr || val_outlen == nullptr) {
        return -1;
    }
    if (rondb_encode_varbinary_string(name, 1U,
                                      name_enc, sizeof(name_enc),
                                      &name_enc_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_XATTRS);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "xattr_get startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "xattr_get readOp");
    }
    op->readTuple(NdbOperation::LM_CommittedRead);
    (void)rondb_equal_u64(op, RONDB_XA_COL_FILEID, fileid);
    op->equal(RONDB_XA_COL_ATTR_NAME,
              (const char *)name_enc, name_enc_len);

    /* Blob column: get handle before execute. */
    blob_handle = op->getBlobHandle(RONDB_XA_COL_VALUE);
    if (blob_handle == nullptr) {
        err = op->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "xattr_get getBlobHandle");
    }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "xattr_get exec");
    }

    {
        NdbError op_err = op->getNdbError();
        if (op_err.code == 626 ||
            op_err.classification == NdbError::NoDataFound) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return 1;
        }
    }

    /* Read blob length, then data. */
    {
        Uint64 blob_len = 0;
        int null_flag = -1;

        if (blob_handle->getNull(null_flag) != 0 || null_flag == 1) {
            *val_outlen = 0;
            rondb_get_ndb(state)->closeTransaction(tx);
            return 0;
        }
        if (blob_handle->getLength(blob_len) != 0) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }
        *val_outlen = (uint32_t)blob_len;
        if (val_buf != nullptr && blob_len > 0) {
            Uint32 to_read = (blob_len > val_buflen) ? val_buflen
                                                     : (Uint32)blob_len;
            if (blob_handle->readData(val_buf, to_read) != 0) {
                rondb_get_ndb(state)->closeTransaction(tx);
                return -1;
            }
            /* Report only the bytes actually copied: callers (e.g.
             * READLINK via inline_get) NUL-terminate / memcpy using
             * *val_outlen, so reporting the full blob length for a
             * short buffer would write past the caller's buffer.  No
             * caller implements a grow-and-retry loop on this length.
             * (val_buf == NULL keeps full-length size-query semantics.) */
            *val_outlen = to_read;
        }
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_xattr_put(void *handle, uint64_t fileid,
                         const char *name,
                         const uint8_t *val, uint32_t val_len)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbBlob *blob_handle;
    NdbError err;
    uint8_t name_enc[MDS_XATTR_NAME_MAX + 2];
    uint32_t name_enc_len = 0;

    if (state == nullptr || name == nullptr ||
        (val_len > 0 && val == nullptr) ||
        val_len > MDS_XATTR_VAL_MAX) {
        return -1;
    }
    if (rondb_encode_varbinary_string(name, 1U,
                                      name_enc, sizeof(name_enc),
                                      &name_enc_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_XATTRS);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "xattr_put startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "xattr_put writeOp");
    }
    op->writeTuple();
    (void)rondb_equal_u64(op, RONDB_XA_COL_FILEID, fileid);
    op->equal(RONDB_XA_COL_ATTR_NAME,
              (const char *)name_enc, name_enc_len);

    /* Blob column: write via blob handle. */
    blob_handle = op->getBlobHandle(RONDB_XA_COL_VALUE);
    if (blob_handle == nullptr) {
        err = op->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "xattr_put getBlobHandle");
    }
    if (val_len > 0) {
        if (blob_handle->setValue(val, val_len) != 0) {
            err = blob_handle->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "xattr_put setValue");
        }
    } else {
        if (blob_handle->setValue(nullptr, 0) != 0) {
            err = blob_handle->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "xattr_put setValueNull");
        }
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "xattr_put commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_xattr_del(void *handle, uint64_t fileid,
                         const char *name)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    uint8_t name_enc[MDS_XATTR_NAME_MAX + 2];
    uint32_t name_enc_len = 0;

    if (state == nullptr || name == nullptr) { return -1; }
    if (rondb_encode_varbinary_string(name, 1U,
                                      name_enc, sizeof(name_enc),
                                      &name_enc_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_XATTRS);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "xattr_del startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "xattr_del delOp");
    }
    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_XA_COL_FILEID, fileid);
    op->equal(RONDB_XA_COL_ATTR_NAME,
              (const char *)name_enc, name_enc_len);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626 ||
            err.classification == NdbError::NoDataFound) {
            return 1; /* NOTFOUND — RFC 8276 §4.2.5 requires NOXATTR */
        }
        return rondb_report_error(err, "xattr_del commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_xattr_list(void *handle, uint64_t fileid,
                          rondb_xattr_list_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_name;
    NdbError err;
    int next_rc;

    if (state == nullptr || cb == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_XATTRS);
    if (tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "xattr_list startTx");
    }

    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "xattr_list getScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "xattr_list readTuples");
    }

    /* Filter: fileid == target. */
    {
        NdbScanFilter filter(scan);
        filter.begin(NdbScanFilter::AND);
        filter.eq(tbl->getColumn(RONDB_XA_COL_FILEID)->getColumnNo(),
                  (Uint64)fileid);
        filter.end();
    }

    a_name = scan->getValue(RONDB_XA_COL_ATTR_NAME, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "xattr_list exec");
    }

    while ((next_rc = scan->nextResult(true)) == 0) {
        const char *name_ptr = a_name->aRef();
        uint32_t name_len;

        if (name_ptr == nullptr) { continue; }

        /* VARBINARY(255): 1-byte length prefix. */
        name_len = (uint32_t)(uint8_t)name_ptr[0];
        name_ptr += 1;

        if (cb(name_ptr, name_len, ctx) != 0) {
            break;
        }
    }
    if (next_rc != 0 && next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "xattr_list nextResult");
    }

    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * DS registry CRUD (typed columns, PK = ds_id)
 *
 * mds_ds_registry: ds_id(PK, partition key, UNSIGNED) + 13 typed columns
 * matching struct mds_ds_info.  No blob serialisation -- direct column
 * access.  String fields (addr, host, export_path) are LONGVARBINARY(256)
 * with 2-byte LE length prefix.
 * ----------------------------------------------------------------------- */

/** Decode a LONGVARBINARY NdbRecAttr into a null-terminated C string. */
static void rondb_decode_lvb_string(const NdbRecAttr *attr,
                                     char *out, size_t out_cap)
{
    const char *ref;
    uint32_t data_len;
    uint32_t copy;

    if (out_cap == 0) { return; }
    out[0] = '\0';
    if (attr == nullptr) { return; }
    ref = attr->aRef();
    if (ref == nullptr) { return; }

    /* LONGVARBINARY: 2-byte LE length prefix. */
    data_len = (uint32_t)(uint8_t)ref[0] |
               ((uint32_t)(uint8_t)ref[1] << 8);
    copy = (data_len < (uint32_t)(out_cap - 1)) ? data_len
                                                  : (uint32_t)(out_cap - 1);
    std::memcpy(out, ref + 2, copy);
    out[copy] = '\0';
}

int rondb_shim_ds_registry_get(void *handle, uint32_t ds_id,
                               struct mds_ds_info *info)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *a_state, *a_tier, *a_total, *a_used, *a_port;
    NdbRecAttr *a_addr, *a_mode, *a_transport;
    NdbRecAttr *a_host, *a_export, *a_tcp_port, *a_rdma_port, *a_caps;
    NdbError err;

    if (state == nullptr || info == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DS_REGISTRY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "ds_reg_get startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ds_reg_get readOp");
    }
    op->readTuple(NdbOperation::LM_CommittedRead);
    op->equal(RONDB_DSR_COL_DS_ID, (Uint32)ds_id);

    a_state = op->getValue(RONDB_DSR_COL_STATE, nullptr);
    a_tier = op->getValue(RONDB_DSR_COL_TIER, nullptr);
    a_total = op->getValue(RONDB_DSR_COL_TOTAL_BYTES, nullptr);
    a_used = op->getValue(RONDB_DSR_COL_USED_BYTES, nullptr);
    a_port = op->getValue(RONDB_DSR_COL_PORT, nullptr);
    a_addr = op->getValue(RONDB_DSR_COL_ADDR, nullptr);
    a_mode = op->getValue(RONDB_DSR_COL_MODE, nullptr);
    a_transport = op->getValue(RONDB_DSR_COL_TRANSPORT, nullptr);
    a_host = op->getValue(RONDB_DSR_COL_HOST, nullptr);
    a_export = op->getValue(RONDB_DSR_COL_EXPORT_PATH, nullptr);
    a_tcp_port = op->getValue(RONDB_DSR_COL_TCP_PORT, nullptr);
    a_rdma_port = op->getValue(RONDB_DSR_COL_RDMA_PORT, nullptr);
    a_caps = op->getValue(RONDB_DSR_COL_CAPS, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626 ||
            err.classification == NdbError::NoDataFound) {
            return 1; /* NOTFOUND */
        }
        return rondb_report_error(err, "ds_reg_get exec");
    }

    {
        NdbError op_err = op->getNdbError();
        if (op_err.code == 626 ||
            op_err.classification == NdbError::NoDataFound) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return 1; /* NOTFOUND */
        }
    }

    std::memset(info, 0, sizeof(*info));
    info->ds_id = ds_id;
    info->state = a_state->u_32_value();
    info->tier = a_tier->u_32_value();
    info->total_bytes = a_total->u_64_value();
    info->used_bytes = a_used->u_64_value();
    info->port = (uint16_t)a_port->u_32_value();
    rondb_decode_lvb_string(a_addr, info->addr, sizeof(info->addr));
    info->mode = (uint8_t)a_mode->u_8_value();
    info->transport = (uint8_t)a_transport->u_8_value();
    rondb_decode_lvb_string(a_host, info->host, sizeof(info->host));
    rondb_decode_lvb_string(a_export, info->export_path,
                            sizeof(info->export_path));
    info->tcp_port = (uint16_t)a_tcp_port->u_32_value();
    info->rdma_port = (uint16_t)a_rdma_port->u_32_value();
    info->capabilities = a_caps->u_32_value();

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_ds_registry_put(void *handle,
                               const struct mds_ds_info *info)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    uint8_t addr_enc[MDS_DS_ADDR_MAX + 4];
    uint32_t addr_enc_len = 0;
    uint8_t host_enc[MDS_DS_HOST_MAX + 4];
    uint32_t host_enc_len = 0;
    uint8_t export_enc[MDS_DS_EXPORT_MAX + 4];
    uint32_t export_enc_len = 0;

    if (state == nullptr || info == nullptr) { return -1; }

    /* Pre-encode string fields as LONGVARBINARY (2-byte LE prefix). */
    if (rondb_encode_varbinary_string(info->addr, 2U,
                                      addr_enc, sizeof(addr_enc),
                                      &addr_enc_len) != 0) {
        return -1;
    }
    if (rondb_encode_varbinary_string(info->host, 2U,
                                      host_enc, sizeof(host_enc),
                                      &host_enc_len) != 0) {
        return -1;
    }
    if (rondb_encode_varbinary_string(info->export_path, 2U,
                                      export_enc, sizeof(export_enc),
                                      &export_enc_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DS_REGISTRY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "ds_reg_put startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ds_reg_put writeOp");
    }
    op->writeTuple();
    op->equal(RONDB_DSR_COL_DS_ID, (Uint32)info->ds_id);
    op->setValue(RONDB_DSR_COL_STATE, (Uint32)info->state);
    op->setValue(RONDB_DSR_COL_TIER, (Uint32)info->tier);
    (void)rondb_set_value_u64(op, RONDB_DSR_COL_TOTAL_BYTES,
                              info->total_bytes);
    (void)rondb_set_value_u64(op, RONDB_DSR_COL_USED_BYTES,
                              info->used_bytes);
    op->setValue(RONDB_DSR_COL_PORT, (Uint32)info->port);
    op->setValue(RONDB_DSR_COL_ADDR,
                 (const char *)addr_enc, addr_enc_len);
    op->setValue(RONDB_DSR_COL_MODE, (Uint32)info->mode);
    op->setValue(RONDB_DSR_COL_TRANSPORT, (Uint32)info->transport);
    op->setValue(RONDB_DSR_COL_HOST,
                 (const char *)host_enc, host_enc_len);
    op->setValue(RONDB_DSR_COL_EXPORT_PATH,
                 (const char *)export_enc, export_enc_len);
    op->setValue(RONDB_DSR_COL_TCP_PORT, (Uint32)info->tcp_port);
    op->setValue(RONDB_DSR_COL_RDMA_PORT, (Uint32)info->rdma_port);
    op->setValue(RONDB_DSR_COL_CAPS, (Uint32)info->capabilities);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ds_reg_put commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_ds_registry_del(void *handle, uint32_t ds_id)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DS_REGISTRY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "ds_reg_del startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ds_reg_del delOp");
    }
    op->deleteTuple();
    op->equal(RONDB_DSR_COL_DS_ID, (Uint32)ds_id);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626 ||
            err.classification == NdbError::NoDataFound) {
            return 1; /* NOTFOUND */
        }
        return rondb_report_error(err, "ds_reg_del commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_ds_registry_list(void *handle,
                                struct mds_ds_info **list_out,
                                uint32_t *count_out)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_id, *a_state, *a_tier, *a_total, *a_used;
    NdbRecAttr *a_port, *a_addr, *a_mode, *a_transport;
    NdbRecAttr *a_host, *a_export, *a_tcp_port, *a_rdma_port, *a_caps;
    NdbError err;
    int next_rc;
    std::vector<struct mds_ds_info> entries;

    if (state == nullptr || list_out == nullptr ||
        count_out == nullptr) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DS_REGISTRY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "ds_reg_list startTx");
    }

    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ds_reg_list getScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ds_reg_list readTuples");
    }

    /* No filter -- full table scan for all DS entries. */
    a_id = scan->getValue(RONDB_DSR_COL_DS_ID, nullptr);
    a_state = scan->getValue(RONDB_DSR_COL_STATE, nullptr);
    a_tier = scan->getValue(RONDB_DSR_COL_TIER, nullptr);
    a_total = scan->getValue(RONDB_DSR_COL_TOTAL_BYTES, nullptr);
    a_used = scan->getValue(RONDB_DSR_COL_USED_BYTES, nullptr);
    a_port = scan->getValue(RONDB_DSR_COL_PORT, nullptr);
    a_addr = scan->getValue(RONDB_DSR_COL_ADDR, nullptr);
    a_mode = scan->getValue(RONDB_DSR_COL_MODE, nullptr);
    a_transport = scan->getValue(RONDB_DSR_COL_TRANSPORT, nullptr);
    a_host = scan->getValue(RONDB_DSR_COL_HOST, nullptr);
    a_export = scan->getValue(RONDB_DSR_COL_EXPORT_PATH, nullptr);
    a_tcp_port = scan->getValue(RONDB_DSR_COL_TCP_PORT, nullptr);
    a_rdma_port = scan->getValue(RONDB_DSR_COL_RDMA_PORT, nullptr);
    a_caps = scan->getValue(RONDB_DSR_COL_CAPS, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ds_reg_list exec");
    }

    while ((next_rc = scan->nextResult(true)) == 0) {
        struct mds_ds_info entry;
        std::memset(&entry, 0, sizeof(entry));

        entry.ds_id = a_id->u_32_value();
        entry.state = a_state->u_32_value();
        entry.tier = a_tier->u_32_value();
        entry.total_bytes = a_total->u_64_value();
        entry.used_bytes = a_used->u_64_value();
        entry.port = (uint16_t)a_port->u_32_value();
        rondb_decode_lvb_string(a_addr, entry.addr,
                                sizeof(entry.addr));
        entry.mode = (uint8_t)a_mode->u_8_value();
        entry.transport = (uint8_t)a_transport->u_8_value();
        rondb_decode_lvb_string(a_host, entry.host,
                                sizeof(entry.host));
        rondb_decode_lvb_string(a_export, entry.export_path,
                                sizeof(entry.export_path));
        entry.tcp_port = (uint16_t)a_tcp_port->u_32_value();
        entry.rdma_port = (uint16_t)a_rdma_port->u_32_value();
        entry.capabilities = a_caps->u_32_value();

        entries.push_back(entry);
    }
    if (next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ds_reg_list nextResult");
    }

    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);

    /* Copy to malloc'd array for C caller. */
    if (entries.empty()) {
        *list_out = nullptr;
        *count_out = 0;
    } else {
        size_t alloc_size = entries.size() * sizeof(struct mds_ds_info);
        struct mds_ds_info *arr =
            (struct mds_ds_info *)std::malloc(alloc_size);
        if (arr == nullptr) {
            return -1;
        }
        std::memcpy(arr, entries.data(), alloc_size);
        *list_out = arr;
        *count_out = (uint32_t)entries.size();
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * Micro-benchmark: raw NDB inode+dirent insert throughput.
 *
 * Does the same multi-row NDB transaction as ns_create (insert inode +
 * insert dirent) in a tight loop, bypassing the entire NFS/RPC stack.
 * Measures pure NDB round-trip cost per create.
 * ----------------------------------------------------------------------- */

int rondb_shim_bench_create(void *handle, uint32_t n_ops,
                            uint64_t parent_fileid, uint64_t base_fileid,
                            uint64_t *elapsed_us, uint32_t *errors)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *ino_tbl, *dir_tbl;
    uint32_t err_count = 0;

    if (state == nullptr || elapsed_us == nullptr || errors == nullptr) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    ino_tbl = dict->getTable(RONDB_TBL_INODES);
    dir_tbl = dict->getTable(RONDB_TBL_DIRENTS);
    if (ino_tbl == nullptr || dir_tbl == nullptr) { return -1; }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (uint32_t i = 0; i < n_ops; i++) {
        uint64_t fid = base_fileid + i;
        char name_buf[32];
        (void)std::snprintf(name_buf, sizeof(name_buf),
                            "b%lu", (unsigned long)fid);
        uint8_t name_enc[34];
        uint32_t name_enc_len = 0;
        if (rondb_encode_varbinary_string(name_buf, 1U,
                                          name_enc, sizeof(name_enc),
                                          &name_enc_len) != 0) {
            err_count++; continue;
        }

        Ndb *ndb = rondb_get_ndb(state);
        if (ndb == nullptr) { err_count++; continue; }

        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, parent_fileid);
        NdbTransaction *tx = ndb->startTransaction(
            dir_tbl, (const char *)pk_buf, 8);
        if (tx == nullptr) { err_count++; continue; }

        NdbOperation *op = tx->getNdbOperation(dir_tbl);
        if (op == nullptr) { ndb->closeTransaction(tx); err_count++; continue; }
        op->insertTuple();
        (void)rondb_equal_u64(op, RONDB_DIR_COL_PARENT, parent_fileid);
        op->equal(RONDB_DIR_COL_NAME, (const char *)name_enc, name_enc_len);
        (void)rondb_set_value_u64(op, RONDB_DIR_COL_CHILD_FID, fid);
        op->setValue(RONDB_DIR_COL_CHILD_TYPE, (Uint32)1);

        op = tx->getNdbOperation(ino_tbl);
        if (op == nullptr) { ndb->closeTransaction(tx); err_count++; continue; }
        op->insertTuple();
        (void)rondb_equal_u64(op, RONDB_INO_COL_FILEID, fid);
        op->setValue(RONDB_INO_COL_TYPE, (Uint32)1);
        op->setValue(RONDB_INO_COL_MODE, (Uint32)0100644);
        op->setValue(RONDB_INO_COL_NLINK, (Uint32)1);
        (void)rondb_set_value_u64(op, RONDB_INO_COL_UID, (Uint64)0);
        (void)rondb_set_value_u64(op, RONDB_INO_COL_GID, (Uint64)0);
        (void)rondb_set_value_u64(op, RONDB_INO_COL_FILE_SIZE, (Uint64)0);
        (void)rondb_set_value_u64(op, RONDB_INO_COL_SPACE_USED, (Uint64)0);
        (void)rondb_set_value_u64(op, RONDB_INO_COL_ATIME_SEC, (Uint64)0);
        op->setValue(RONDB_INO_COL_ATIME_NSEC, (Uint32)0);
        (void)rondb_set_value_u64(op, RONDB_INO_COL_MTIME_SEC, (Uint64)0);
        op->setValue(RONDB_INO_COL_MTIME_NSEC, (Uint32)0);
        (void)rondb_set_value_u64(op, RONDB_INO_COL_CTIME_SEC, (Uint64)0);
        op->setValue(RONDB_INO_COL_CTIME_NSEC, (Uint32)0);
        (void)rondb_set_value_u64(op, RONDB_INO_COL_CHANGE, (Uint64)1);
        (void)rondb_set_value_u64(op, RONDB_INO_COL_GENERATION, (Uint64)1);
        op->setValue(RONDB_INO_COL_FLAGS, (Uint32)0);
        (void)rondb_set_value_u64(op, RONDB_INO_COL_CREATE_VERF, (Uint64)0);
        (void)rondb_set_value_u64(op, RONDB_INO_COL_PARENT, parent_fileid);
        op->setValue(RONDB_INO_COL_HOME_SHARD, (Uint32)0);

        if (tx->execute(NdbTransaction::Commit) == -1) { err_count++; }
        ndb->closeTransaction(tx);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    int64_t dsec  = (int64_t)(t1.tv_sec - t0.tv_sec);
    int64_t dnsec = (int64_t)(t1.tv_nsec - t0.tv_nsec);
    uint64_t us = (uint64_t)(dsec * 1000000LL + dnsec / 1000LL);
    *elapsed_us = us;
    *errors = err_count;
    return 0;
}

/* -----------------------------------------------------------------------
 * Atomic CREATE (T2 -- single NDB transaction, multi-row)
 *
 * Inserts: child inode + dirent + updated parent inode +
 *          optional stripe header+entries.
 * Uses insertTuple for the new child inode + dirent and updateTuple for the
 * existing parent inode so missing/existing row semantics are enforced.
 * TC locality hint: parent_fileid (majority of operations).
 * ----------------------------------------------------------------------- */

int rondb_shim_ns_create(void *handle,
                         uint64_t parent_fileid, const char *name,
                         const uint8_t *child_inode_buf, uint32_t child_ino_len,
                         int32_t parent_nlink_delta,
                         const uint8_t *stripe_buf, uint32_t stripe_len,
                         uint32_t stripe_count)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *ino_tbl, *dir_tbl;
    NdbTransaction *tx;
    NdbOperation *op_child, *op_dirent;
    NdbError err;
    struct mds_inode child_ino;
    uint32_t child_shard;
    uint8_t name_value[MDS_MAX_NAME + 2];
    uint32_t name_value_len = 0;

    if (state == nullptr || name == nullptr ||
        child_inode_buf == nullptr) {
        return -1;
    }
    if (rondb_encode_varbinary_string(name, 1U,
                                      name_value, sizeof(name_value),
                                      &name_value_len) != 0) {
        return -1;
    }

    if (rondb_inode_deserialize(child_inode_buf, child_ino_len,
                                &child_ino, &child_shard) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    ino_tbl = dict->getTable(RONDB_TBL_INODES);
    dir_tbl = dict->getTable(RONDB_TBL_DIRENTS);
    if (ino_tbl == nullptr || dir_tbl == nullptr) { return -1; }

    /* Hint on parent partition (dirent + parent inode live there). */
    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, parent_fileid);
        tx = rondb_get_ndb(state)->startTransaction(dir_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "ns_create startTx");
    }

    /* 1. Insert dirent (insertTuple fails on PK conflict -> EXISTS). */
    op_dirent = tx->getNdbOperation(dir_tbl);
    if (op_dirent == nullptr) { goto ns_create_err; }
    op_dirent->insertTuple();
    (void)rondb_equal_u64(op_dirent, RONDB_DIR_COL_PARENT, parent_fileid);
    op_dirent->equal(RONDB_DIR_COL_NAME,
                     (const char *)name_value,
                     name_value_len);
    (void)rondb_set_value_u64(op_dirent, RONDB_DIR_COL_CHILD_FID,
                              child_ino.fileid);
    op_dirent->setValue(RONDB_DIR_COL_CHILD_TYPE, (Uint32)(uint8_t)child_ino.type);

    /* 2. Insert child inode. */
    op_child = tx->getNdbOperation(ino_tbl);
    if (op_child == nullptr) { goto ns_create_err; }
    op_child->insertTuple();
    (void)rondb_equal_u64(op_child, RONDB_INO_COL_FILEID, child_ino.fileid);
    rondb_set_inode_values(op_child, &child_ino, child_shard);

    /* 3. Interpreted parent inode update (atomic nlink + change). */
    if (rondb_interpreted_parent_update(tx, ino_tbl, parent_fileid,
                                        parent_nlink_delta) != 0) {
        goto ns_create_err;
    }

    /* 4. Stripe data -- create stripe_map header + entries in the
     *    same transaction as the inode+dirent so the file is
     *    immediately ready for LAYOUTGET without a second RT. */
    if (stripe_buf != nullptr && stripe_count > 0 && stripe_len >= 8) {
        const NdbDictionary::Table *sm_tbl =
            dict->getTable(RONDB_TBL_STRIPE_MAPS);
        const NdbDictionary::Table *se_tbl =
            dict->getTable(RONDB_TBL_STRIPE_ENTRIES);
        if (sm_tbl != nullptr && se_tbl != nullptr) {
            /* Stripe header: fileid, stripe_count, stripe_unit, mirror_count.
             * For prealloc V1: stripe_count=1, mirror_count=1. */
            NdbOperation *op_sm = tx->getNdbOperation(sm_tbl);
            if (op_sm != nullptr) {
                op_sm->insertTuple();
                (void)rondb_equal_u64(op_sm, RONDB_SM_COL_FILEID,
                                     child_ino.fileid);
                op_sm->setValue(RONDB_SM_COL_STRIPE_CNT, (Uint32)stripe_count);
                op_sm->setValue(RONDB_SM_COL_STRIPE_UNIT, (Uint32)65536);
                op_sm->setValue(RONDB_SM_COL_MIRROR_CNT, (Uint32)1);
            }
            /* Stripe entries: one per stripe.  The wire layout (see
             * the serializer in catalogue_rondb.c) is variable-length
             * per entry -- [ds_id u32][fh_len u32][fh bytes] -- so
             * walk an offset cursor and bound every read against
             * stripe_len.  The previous fixed 8-byte stride both
             * over-read the buffer and mis-indexed entries whenever
             * stripe_count > 1. */
            uint32_t s_off = 0;
            for (uint32_t si = 0; si < stripe_count; si++) {
                if (s_off + 8 > stripe_len) {
                    rondb_get_ndb(state)->closeTransaction(tx);
                    std::fprintf(stderr,
                        "ERROR: ns_create stripe buffer overrun: "
                        "entry %u header at %u exceeds len %u\n",
                        si, s_off, stripe_len);
                    return -1;
                }
                uint32_t ds_id = fdb_get_u32(stripe_buf + s_off);
                uint32_t fh_len = fdb_get_u32(stripe_buf + s_off + 4);
                if (fh_len > stripe_len - s_off - 8) {
                    rondb_get_ndb(state)->closeTransaction(tx);
                    std::fprintf(stderr,
                        "ERROR: ns_create stripe buffer overrun: "
                        "entry %u fh_len %u exceeds len %u\n",
                        si, fh_len, stripe_len);
                    return -1;
                }
                NdbOperation *op_se = tx->getNdbOperation(se_tbl);
                if (op_se != nullptr) {
                    op_se->insertTuple();
                    (void)rondb_equal_u64(op_se, RONDB_SE_COL_FILEID,
                                         child_ino.fileid);
                    op_se->equal(RONDB_SE_COL_ORDINAL, (Uint32)si);
                    op_se->setValue(RONDB_SE_COL_DS_ID, (Uint32)ds_id);
                    op_se->setValue(RONDB_SE_COL_NFS_FH_LEN, (Uint32)fh_len);
                    if (fh_len > 0) {
                        uint8_t fh_vb[130];
                        uint32_t fh_vb_len = 0;
                        if (rondb_encode_varbinary_value(
                                stripe_buf + s_off + 8,
                                fh_len, 1U, fh_vb,
                                sizeof(fh_vb), &fh_vb_len) == 0) {
                            op_se->setValue(RONDB_SE_COL_NFS_FH,
                                           (const char *)fh_vb,
                                           fh_vb_len);
                        }
                    } else {
                        /* Zero-length FH placeholder: 1-byte prefix. */
                        uint8_t empty_vb[1] = {0};
                        op_se->setValue(RONDB_SE_COL_NFS_FH,
                                       (const char *)empty_vb, 1);
                    }
                }
                s_off += 8 + fh_len;
            }
        }
    }

    {
        int commit_rc = tx->execute(NdbTransaction::Commit);
        if (commit_rc == -1) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            if (err.classification == NdbError::ConstraintViolation) {
                return 1; /* EXISTS -- dirent PK conflict */
            }
            if (rondb_is_temporary(err)) {
                return -2; /* Retryable -- caller should retry */
            }
            return rondb_report_error(err, "ns_create commit");
        }
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;

ns_create_err:
    err = tx->getNdbError();
    rondb_get_ndb(state)->closeTransaction(tx);
    if (rondb_is_temporary(err)) {
        return -2;
    }
    return rondb_report_error(err, "ns_create op");
}

/* -----------------------------------------------------------------------
 * Phase 3: Fused ns_create + layout pre-grant (single NDB transaction)
 *
 * Same as ns_create but piggybacks layout_state + index writes when
 * layout_clientid != 0.  This eliminates the LAYOUTGET NDB round-trips
 * entirely for CREATE+LAYOUTGET compounds.
 * ----------------------------------------------------------------------- */

int rondb_shim_ns_create_with_layout(
    void *handle,
    uint64_t parent_fileid, const char *name,
    const uint8_t *child_inode_buf, uint32_t child_ino_len,
    int32_t parent_nlink_delta,
    const uint8_t *stripe_buf, uint32_t stripe_len, uint32_t stripe_count,
    uint64_t layout_clientid, uint32_t layout_iomode,
    uint64_t layout_offset, uint64_t layout_length,
    const uint8_t layout_stateid_other[12], uint32_t layout_seqid,
    const uint32_t *layout_ds_ids, uint32_t layout_ds_count,
    uint32_t layout_mds_id)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *ino_tbl, *dir_tbl;
    NdbTransaction *tx;
    NdbOperation *op_child, *op_dirent;
    NdbError err;
    struct mds_inode child_ino;
    uint32_t child_shard;
    uint8_t name_value[MDS_MAX_NAME + 2];
    uint32_t name_value_len = 0;

    /*
     * Per-op tracking for commit-failure diagnostics.  Only emits
     * output on Commit failure, so this is zero-cost on the hot
     * success path except for the small vector allocation.  The
     * per-op labels let operators pinpoint which write NDB
     * rejected -- useful e.g. during schema evolution when a PK
     * / partition-key mismatch would otherwise surface as a
     * silent txn abort.
     */
    struct op_tag {
        NdbOperation *op;
        const char   *label;
    };
    std::vector<op_tag> ops;
    ops.reserve(16);
    auto track = [&](NdbOperation *op, const char *label) {
        if (op != nullptr) {
            ops.push_back({op, label});
        }
    };
    auto dump_op_errors = [&](const char *phase) {
        for (const op_tag &t : ops) {
            NdbError oe = t.op->getNdbError();
            if (oe.code != 0) {
                std::fprintf(stderr,
                    "ERROR fused-shim %s: op=%s code=%d class=%d msg=%s\n",
                    phase, t.label, (int)oe.code,
                    (int)oe.classification,
                    oe.message ? oe.message : "?");
            }
        }
    };

    if (state == nullptr || name == nullptr ||
        child_inode_buf == nullptr) {
        return -1;
    }
    if (rondb_encode_varbinary_string(name, 1U,
                                      name_value, sizeof(name_value),
                                      &name_value_len) != 0) {
        return -1;
    }
    if (rondb_inode_deserialize(child_inode_buf, child_ino_len,
                                &child_ino, &child_shard) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    ino_tbl = dict->getTable(RONDB_TBL_INODES);
    dir_tbl = dict->getTable(RONDB_TBL_DIRENTS);
    if (ino_tbl == nullptr || dir_tbl == nullptr) { return -1; }

    /* Hint on parent partition. */
    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, parent_fileid);
        tx = rondb_get_ndb(state)->startTransaction(
            dir_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "ns_create_wl startTx");
    }

    /* 1. Insert dirent. */
    op_dirent = tx->getNdbOperation(dir_tbl);
    if (op_dirent == nullptr) { goto ns_create_wl_err; }
    op_dirent->insertTuple();
    (void)rondb_equal_u64(op_dirent, RONDB_DIR_COL_PARENT, parent_fileid);
    op_dirent->equal(RONDB_DIR_COL_NAME,
                     (const char *)name_value, name_value_len);
    (void)rondb_set_value_u64(op_dirent, RONDB_DIR_COL_CHILD_FID,
                              child_ino.fileid);
    op_dirent->setValue(RONDB_DIR_COL_CHILD_TYPE,
                        (Uint32)(uint8_t)child_ino.type);
    track(op_dirent, "dirent");

    /* 2. Insert child inode. */
    op_child = tx->getNdbOperation(ino_tbl);
    if (op_child == nullptr) { goto ns_create_wl_err; }
    op_child->insertTuple();
    (void)rondb_equal_u64(op_child, RONDB_INO_COL_FILEID,
                          child_ino.fileid);
    rondb_set_inode_values(op_child, &child_ino, child_shard);
    track(op_child, "child_inode");

    /* 3. Atomic parent update. */
    if (rondb_interpreted_parent_update(tx, ino_tbl, parent_fileid,
                                        parent_nlink_delta) != 0) {
        goto ns_create_wl_err;
    }

    /* 4. Stripe data (same as ns_create). */
    if (stripe_buf != nullptr && stripe_count > 0 && stripe_len >= 8) {
        const NdbDictionary::Table *sm_tbl =
            dict->getTable(RONDB_TBL_STRIPE_MAPS);
        const NdbDictionary::Table *se_tbl =
            dict->getTable(RONDB_TBL_STRIPE_ENTRIES);
        if (sm_tbl != nullptr && se_tbl != nullptr) {
            NdbOperation *op_sm = tx->getNdbOperation(sm_tbl);
            if (op_sm != nullptr) {
                op_sm->insertTuple();
                (void)rondb_equal_u64(op_sm, RONDB_SM_COL_FILEID,
                                     child_ino.fileid);
                op_sm->setValue(RONDB_SM_COL_STRIPE_CNT,
                                (Uint32)stripe_count);
                op_sm->setValue(RONDB_SM_COL_STRIPE_UNIT, (Uint32)65536);
                op_sm->setValue(RONDB_SM_COL_MIRROR_CNT, (Uint32)1);
                track(op_sm, "stripe_map");
            }
            /* Variable-length entry walk with bounds checks -- see the
             * matching loop in rondb_shim_ns_create above. */
            uint32_t s_off = 0;
            for (uint32_t si = 0; si < stripe_count; si++) {
                if (s_off + 8 > stripe_len) {
                    rondb_get_ndb(state)->closeTransaction(tx);
                    std::fprintf(stderr,
                        "ERROR: ns_create_wl stripe buffer overrun: "
                        "entry %u header at %u exceeds len %u\n",
                        si, s_off, stripe_len);
                    return -1;
                }
                uint32_t ds_id = fdb_get_u32(stripe_buf + s_off);
                uint32_t fh_len = fdb_get_u32(stripe_buf + s_off + 4);
                if (fh_len > stripe_len - s_off - 8) {
                    rondb_get_ndb(state)->closeTransaction(tx);
                    std::fprintf(stderr,
                        "ERROR: ns_create_wl stripe buffer overrun: "
                        "entry %u fh_len %u exceeds len %u\n",
                        si, fh_len, stripe_len);
                    return -1;
                }
                NdbOperation *op_se = tx->getNdbOperation(se_tbl);
                if (op_se != nullptr) {
                    op_se->insertTuple();
                    (void)rondb_equal_u64(op_se, RONDB_SE_COL_FILEID,
                                         child_ino.fileid);
                    op_se->equal(RONDB_SE_COL_ORDINAL, (Uint32)si);
                    op_se->setValue(RONDB_SE_COL_DS_ID, (Uint32)ds_id);
                    op_se->setValue(RONDB_SE_COL_NFS_FH_LEN, (Uint32)fh_len);
                    if (fh_len > 0) {
                        uint8_t fh_vb[130];
                        uint32_t fh_vb_len = 0;
                        if (rondb_encode_varbinary_value(
                                stripe_buf + s_off + 8,
                                fh_len, 1U, fh_vb,
                                sizeof(fh_vb), &fh_vb_len) == 0) {
                            op_se->setValue(RONDB_SE_COL_NFS_FH,
                                           (const char *)fh_vb, fh_vb_len);
                        }
                    } else {
                        /* Zero-length FH placeholder: 1-byte prefix. */
                        uint8_t empty_vb[1] = {0};
                        op_se->setValue(RONDB_SE_COL_NFS_FH,
                                       (const char *)empty_vb, 1);
                    }
                    track(op_se, "stripe_entry");
                }
                s_off += 8 + fh_len;
            }
        }
    }

    /*
     * Schema v6 placed layout_state / layout_by_file / ds_layout_idx
     * on the child fileid partition, matching the child inode +
     * stripe tables.  With the ds_layout_idx PK fix (all three PK
     * columns via equal()), the full queue of writes commits in a
     * single Commit -- no NoCommit flush needed.  The non-fused
     * ns_create commits the same two-partition shape (parent +
     * child) with one Commit; the fused path now matches that.
     */

    /*
     * 5. Layout pre-grant: piggyback layout_state + indexes into
     *    the same NDB transaction when layout_clientid != 0.
     *    LAYOUTGET will consume this pregrant with zero NDB RTs.
     */
    if (layout_clientid != 0 && layout_stateid_other != nullptr) {
        const NdbDictionary::Table *ls_tbl =
            dict->getTable(RONDB_TBL_LAYOUT_STATE);
        const NdbDictionary::Table *lbf_tbl =
            dict->getTable(RONDB_TBL_LAYOUT_BY_FILE);
        const NdbDictionary::Table *dli_tbl =
            dict->getTable(RONDB_TBL_DS_LAYOUT_IDX);

        if (ls_tbl != nullptr && lbf_tbl != nullptr &&
            dli_tbl != nullptr) {
            uint8_t sid_enc[14];
            uint32_t sid_enc_len = 0;

            if (rondb_encode_varbinary_value(
                    layout_stateid_other, 12, 1U,
                    sid_enc, sizeof(sid_enc), &sid_enc_len) == 0) {
                NdbOperation *lop;

                /* layout_state: PK = (fileid, stateid_other). */
                lop = tx->getNdbOperation(ls_tbl);
                if (lop != nullptr) {
                    lop->writeTuple();
                    (void)rondb_equal_u64(lop, RONDB_LS_COL_FILEID,
                                          child_ino.fileid);
                    lop->equal(RONDB_LS_COL_STATEID,
                               (const char *)sid_enc, sid_enc_len);
                    (void)rondb_set_value_u64(lop, RONDB_LS_COL_CLIENTID,
                                              layout_clientid);
                    lop->setValue(RONDB_LS_COL_IOMODE, layout_iomode);
                    (void)rondb_set_value_u64(lop, RONDB_LS_COL_OFFSET,
                                              layout_offset);
                    (void)rondb_set_value_u64(lop, RONDB_LS_COL_LENGTH,
                                              layout_length);
                    lop->setValue(RONDB_LS_COL_SEQID, layout_seqid);
                    lop->setValue(RONDB_LS_COL_GRANT_MDS,
                                  (Uint32)layout_mds_id);
                    (void)rondb_set_value_u64(lop, RONDB_LS_COL_GRANT_EPOCH,
                                              (Uint64)0);
                    track(lop, "layout_state");
                }

                /* layout_by_file: PK = (fileid, stateid_other). */
                lop = tx->getNdbOperation(lbf_tbl);
                if (lop != nullptr) {
                    lop->writeTuple();
                    (void)rondb_equal_u64(lop, RONDB_LBF_COL_FILEID,
                                          child_ino.fileid);
                    lop->equal(RONDB_LBF_COL_STATEID,
                               (const char *)sid_enc, sid_enc_len);
                    track(lop, "layout_by_file");
                }

                /*
                 * ds_layout_idx PK = (ds_id, clientid, fileid) --
                 * ALL three are PK columns and require equal().
                 * The previous version used setValue() for
                 * clientid / fileid, which NDB rejects with
                 * error 4234 "Illegal to call setValue in this
                 * state" for PK columns on insert/write tuples.
                 */
                for (uint32_t di = 0;
                     di < layout_ds_count && layout_ds_ids != nullptr;
                     di++) {
                    lop = tx->getNdbOperation(dli_tbl);
                    if (lop != nullptr) {
                        lop->writeTuple();
                        lop->equal(RONDB_DLI_COL_DS_ID,
                                   (Uint32)layout_ds_ids[di]);
                        (void)rondb_equal_u64(lop,
                            RONDB_DLI_COL_CLIENTID, layout_clientid);
                        (void)rondb_equal_u64(lop,
                            RONDB_DLI_COL_FILEID, child_ino.fileid);
                        track(lop, "ds_layout_idx");
                    }
                }
            }
        }
    }

    /* Single Commit covering dirent + inode + parent + stripe + layout. */
    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        dump_op_errors("Commit");
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::ConstraintViolation) {
            return 1; /* EXISTS */
        }
        if (rondb_is_temporary(err)) {
            return -2;
        }
        return rondb_report_error(err, "ns_create_wl commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;

ns_create_wl_err:
    err = tx->getNdbError();
    rondb_get_ndb(state)->closeTransaction(tx);
    return rondb_report_error(err, "ns_create_wl op");
}

/* -----------------------------------------------------------------------
 * Atomic REMOVE (T2 -- single NDB transaction, multi-row)
 *
 * Deletes dirent + updates/deletes child inode + updates parent inode.
 * On final unlink (delete_child), stripe_maps + stripe_entries rows for
 * the child fileid are removed in the same NDB transaction.
 * ----------------------------------------------------------------------- */

static int rondb_shim_ns_remove_once(void *handle,
                         uint64_t parent_fileid, const char *name,
                         uint64_t child_fileid,
                         const uint8_t *child_inode_buf, uint32_t child_ino_len,
                         int delete_child,
                         int32_t parent_nlink_delta,
                         uint32_t stripe_count)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *ino_tbl, *dir_tbl;
    const NdbDictionary::Table *sm_hdr_tbl, *sm_ent_tbl;
    NdbTransaction *tx;
    NdbOperation *op_dirent, *op_child;
    NdbError err;
    struct mds_inode child_ino;
    uint32_t child_shard;
    uint32_t stripe_entry_count = stripe_count;
    uint8_t name_value[MDS_MAX_NAME + 2];
    uint32_t name_value_len = 0;
    int stripe_rc;

    if (state == nullptr || name == nullptr ||
        child_inode_buf == nullptr) {
        return -1;
    }
    if (rondb_encode_varbinary_string(name, 1U,
                                      name_value, sizeof(name_value),
                                      &name_value_len) != 0) {
        return -1;
    }

    if (rondb_inode_deserialize(child_inode_buf, child_ino_len,
                                &child_ino, &child_shard) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    ino_tbl = dict->getTable(RONDB_TBL_INODES);
    dir_tbl = dict->getTable(RONDB_TBL_DIRENTS);
    if (ino_tbl == nullptr || dir_tbl == nullptr) { return -1; }
    sm_hdr_tbl = dict->getTable(RONDB_TBL_STRIPE_MAPS);
    sm_ent_tbl = dict->getTable(RONDB_TBL_STRIPE_ENTRIES);

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, parent_fileid);
        tx = rondb_get_ndb(state)->startTransaction(dir_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        err = rondb_get_ndb(state)->getNdbError();
        if (err.code == 266 || err.code == 274) { return err.code; }
        return rondb_report_error(err, "ns_remove startTx");
    }

    if (delete_child && sm_hdr_tbl != nullptr && sm_ent_tbl != nullptr &&
        stripe_entry_count == 0) {
        stripe_rc = rondb_txn_read_stripe_entry_count(
            tx, sm_hdr_tbl, child_fileid, &stripe_entry_count, &err);
        if (stripe_rc == -2) {
            goto ns_remove_err;
        }
        if (stripe_rc != 0) {
            goto ns_remove_err;
        }
    }

    /* 1. Delete dirent. */
    op_dirent = tx->getNdbOperation(dir_tbl);
    if (op_dirent == nullptr) { goto ns_remove_err; }
    op_dirent->deleteTuple();
    (void)rondb_equal_u64(op_dirent, RONDB_DIR_COL_PARENT, parent_fileid);
    op_dirent->equal(RONDB_DIR_COL_NAME,
                     (const char *)name_value,
                     name_value_len);

    /* 2. Child inode: delete or interpreted-update.
     *
     * For delete_child (the nlink==1 common case) we emit a
     * deleteTuple as before.
     *
     * For the hardlink case (nlink > 1) we emit an
     * interpretedUpdateTuple that subtracts 1 from nlink, increments
     * the change counter, and stamps ctime -- all atomically at the
     * data node.  Symmetric to the interpreted parent update used on
     * ns_create.  Wins vs. the previous full-row updateTuple:
     *   * No read-modify-write race with a concurrent setattr/link on
     *     the same inode (WAW lost-update gone).
     *   * Wire payload for the hardlink-remove case shrinks from the
     *     full 137-byte inode record to three atomic opcodes.
     *   * mtime is intentionally NOT touched -- POSIX says unlink()
     *     only updates ctime; the full updateTuple path was rewriting
     *     mtime from the caller's snapshot, which is harmless but
     *     wrong-shaped.
     *
     * The child_ino / child_shard deserialised from child_inode_buf
     * stay available for other future refinements; they are unused
     * on this path today. */
    op_child = tx->getNdbOperation(ino_tbl);
    if (op_child == nullptr) { goto ns_remove_err; }
    if (delete_child) {
        op_child->deleteTuple();
        (void)rondb_equal_u64(op_child, RONDB_INO_COL_FILEID, child_fileid);
    } else {
        struct timespec now;

        op_child->interpretedUpdateTuple();
        (void)rondb_equal_u64(op_child, RONDB_INO_COL_FILEID, child_fileid);
        op_child->subValue(RONDB_INO_COL_NLINK, (Uint32)1);
        op_child->incValue(RONDB_INO_COL_CHANGE, (Uint64)1);
        clock_gettime(CLOCK_REALTIME, &now);
        (void)rondb_set_value_u64(op_child, RONDB_INO_COL_CTIME_SEC,
                                  (uint64_t)now.tv_sec);
        op_child->setValue(RONDB_INO_COL_CTIME_NSEC,
                           (Uint32)now.tv_nsec);
        (void)child_ino;
        (void)child_shard;
    }

    /* 3. Interpreted parent inode update (atomic nlink + change). */
    if (rondb_interpreted_parent_update(tx, ino_tbl, parent_fileid,
                                        parent_nlink_delta) != 0) {
        goto ns_remove_err;
    }

    if (delete_child && stripe_entry_count > 0 &&
        sm_hdr_tbl != nullptr && sm_ent_tbl != nullptr) {
        if (rondb_txn_append_stripe_pk_deletes(tx, sm_hdr_tbl, sm_ent_tbl,
                                               child_fileid,
                                               stripe_entry_count) != 0) {
            goto ns_remove_err;
        }
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 266 || err.code == 274) { return err.code; }
        return rondb_report_error(err, "ns_remove commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;

ns_remove_err:
    err = tx->getNdbError();
    rondb_get_ndb(state)->closeTransaction(tx);
    if (err.code == 266 || err.code == 274) { return err.code; }
    return rondb_report_error(err, "ns_remove op");
}

int rondb_shim_ns_remove(void *handle,
                         uint64_t parent_fileid, const char *name,
                         uint64_t child_fileid,
                         const uint8_t *child_inode_buf, uint32_t child_ino_len,
                         int delete_child,
                         int32_t parent_nlink_delta,
                         uint32_t stripe_count)
{
    for (int attempt = 0; attempt < NDB_RETRY_MAX; attempt++) {
        int rc = rondb_shim_ns_remove_once(
                handle, parent_fileid, name,
                child_fileid, child_inode_buf, child_ino_len,
                delete_child, parent_nlink_delta, stripe_count);
        if (rc == 0) { return 0; }
        if (rc != 266 && rc != 274) { return rc; }
        struct timespec _ts;
        _ts.tv_sec  = 0;
        _ts.tv_nsec = (long)(NDB_RETRY_DELAY_US * (attempt + 1)) * 1000L;
        nanosleep(&_ts, nullptr);
    }
    return -2;  /* exhausted retries — signal MDS_ERR_BUSY to caller */
}

int rondb_shim_ns_remove_full(void *handle,
                              uint64_t parent_fileid, const char *name,
                              uint8_t *out_child_type,
                              uint32_t *out_old_nlink)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *ino_tbl, *dir_tbl;
    NdbTransaction *tx;
    NdbOperation *dir_rd_op;
    NdbRecAttr *a_cfid, *a_ctype;
    NdbRecAttr *a_nlink, *a_itype;
    NdbError err;
    uint8_t name_value[MDS_MAX_NAME + 2];
    uint32_t name_value_len = 0;

    if (state == nullptr || name == nullptr ||
        out_child_type == nullptr || out_old_nlink == nullptr) {
        return -1;
    }
    if (rondb_encode_varbinary_string(name, 1U,
                                      name_value, sizeof(name_value),
                                      &name_value_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    dir_tbl = dict->getTable(RONDB_TBL_DIRENTS);
    ino_tbl = dict->getTable(RONDB_TBL_INODES);
    if (dir_tbl == nullptr || ino_tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, parent_fileid);
        tx = rondb_get_ndb(state)->startTransaction(
            dir_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(
            rondb_get_ndb(state)->getNdbError(),
            "ns_remove_full startTx");
    }

    /* Phase 1: read dirent (NoCommit). */
    dir_rd_op = tx->getNdbOperation(dir_tbl);
    if (dir_rd_op == nullptr) { goto remove_full_err; }
    dir_rd_op->readTuple(NdbOperation::LM_Exclusive);
    (void)rondb_equal_u64(dir_rd_op, RONDB_DIR_COL_PARENT, parent_fileid);
    dir_rd_op->equal(RONDB_DIR_COL_NAME,
                     (const char *)name_value, name_value_len);
    a_cfid  = dir_rd_op->getValue(RONDB_DIR_COL_CHILD_FID, nullptr);
    a_ctype = dir_rd_op->getValue(RONDB_DIR_COL_CHILD_TYPE, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        if (rondb_is_temporary(err)) { return -2; }
        return rondb_report_error(err, "ns_remove_full dirent exec");
    }
    if (dir_rd_op->getNdbError().code == 626) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 1; /* NOTFOUND */
    }

    {
        uint64_t child_fid = a_cfid->u_64_value();
        uint8_t  child_type = (uint8_t)a_ctype->u_8_value();

        /* Phase 2: read child inode nlink (NoCommit). */
        NdbOperation *ino_rd = tx->getNdbOperation(ino_tbl);
        if (ino_rd == nullptr) { goto remove_full_err; }
        ino_rd->readTuple(NdbOperation::LM_Exclusive);
        (void)rondb_equal_u64(ino_rd, RONDB_INO_COL_FILEID, child_fid);
        a_nlink = ino_rd->getValue(RONDB_INO_COL_NLINK, nullptr);
        a_itype = ino_rd->getValue(RONDB_INO_COL_TYPE, nullptr);

        if (tx->execute(NdbTransaction::NoCommit) == -1) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            if (err.code == 626) { return 1; }
            if (rondb_is_temporary(err)) { return -2; }
            return rondb_report_error(err, "ns_remove_full inode exec");
        }
        if (ino_rd->getNdbError().code == 626) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return 1;
        }

        uint32_t old_nlink = a_nlink->u_32_value();
        uint32_t new_nlink = (old_nlink > 0) ? old_nlink - 1 : 0;
        bool delete_child = (new_nlink == 0);
        int32_t parent_nlink_delta =
            ((uint8_t)a_itype->u_8_value() == (uint8_t)MDS_FTYPE_DIR)
            ? -1 : 0;

        *out_child_type = child_type;
        *out_old_nlink  = old_nlink;

        const NdbDictionary::Table *sm_hdr_tbl =
            dict->getTable(RONDB_TBL_STRIPE_MAPS);
        const NdbDictionary::Table *sm_ent_tbl =
            dict->getTable(RONDB_TBL_STRIPE_ENTRIES);
        uint32_t stripe_entry_count = 0;

        if (delete_child && sm_hdr_tbl != nullptr && sm_ent_tbl != nullptr) {
            int stripe_rc = rondb_txn_read_stripe_entry_count(
                tx, sm_hdr_tbl, child_fid, &stripe_entry_count, &err);
            if (stripe_rc == -2) {
                goto remove_full_err;
            }
            if (stripe_rc != 0) {
                goto remove_full_err;
            }
        }

        /* Phase 3: mutations (Commit).
         * All in the same transaction as the reads. */

        /* Delete dirent. */
        NdbOperation *del_dir = tx->getNdbOperation(dir_tbl);
        if (del_dir == nullptr) { goto remove_full_err; }
        del_dir->deleteTuple();
        (void)rondb_equal_u64(del_dir, RONDB_DIR_COL_PARENT, parent_fileid);
        del_dir->equal(RONDB_DIR_COL_NAME,
                       (const char *)name_value, name_value_len);

        /* Child inode: delete or interpreted nlink decrement. */
        NdbOperation *child_op = tx->getNdbOperation(ino_tbl);
        if (child_op == nullptr) { goto remove_full_err; }
        if (delete_child) {
            child_op->deleteTuple();
            (void)rondb_equal_u64(child_op, RONDB_INO_COL_FILEID, child_fid);
        } else {
            /* Interpreted update: decrement nlink + update timestamps
             * atomically on the data node. */
            child_op->interpretedUpdateTuple();
            (void)rondb_equal_u64(child_op, RONDB_INO_COL_FILEID, child_fid);
            child_op->subValue(RONDB_INO_COL_NLINK, (Uint32)1);
            child_op->incValue(RONDB_INO_COL_CHANGE, (Uint64)1);
            {
                struct timespec now;
                clock_gettime(CLOCK_REALTIME, &now);
                (void)rondb_set_value_u64(child_op, RONDB_INO_COL_CTIME_SEC,
                                          (uint64_t)now.tv_sec);
                child_op->setValue(RONDB_INO_COL_CTIME_NSEC,
                                   (Uint32)now.tv_nsec);
            }
        }

        /* Interpreted parent update (nlink + change + mtime). */
        if (rondb_interpreted_parent_update(tx, ino_tbl, parent_fileid,
                                            parent_nlink_delta) != 0) {
            goto remove_full_err;
        }

        if (delete_child && stripe_entry_count > 0 &&
            sm_hdr_tbl != nullptr && sm_ent_tbl != nullptr) {
            if (rondb_txn_append_stripe_pk_deletes(tx, sm_hdr_tbl, sm_ent_tbl,
                                                   child_fid,
                                                   stripe_entry_count) != 0) {
                goto remove_full_err;
            }
        }

        if (tx->execute(NdbTransaction::Commit) == -1) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            if (rondb_is_temporary(err)) { return -2; }
            return rondb_report_error(err, "ns_remove_full commit");
        }
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;

remove_full_err:
    err = tx->getNdbError();
    rondb_get_ndb(state)->closeTransaction(tx);
    if (rondb_is_temporary(err)) { return -2; }
    return rondb_report_error(err, "ns_remove_full op");
}

/* -----------------------------------------------------------------------
 * Atomic RENAME (T2 -- single NDB transaction, multi-row)
 *
 * Option A (single-cluster model): any MDS executes the rename
 * directly as one NDB transaction.  No MDS-level 2PC.
 *
 * Operations (all batched before one execute(Commit)):
 *   1. Delete src dirent
 *   2. Write dst dirent (writeTuple = upsert for overwrite case)
 *   3. Update src parent inode
 *   4. Update dst parent inode (if cross-dir)
 *   5. Update src child inode (parent_fileid change for cross-dir)
 *   6. If overwrite: update/delete dst child inode
 *
 * TC locality hint: src_parent (majority of ops touch that partition).
 * ----------------------------------------------------------------------- */

int rondb_shim_rename(void *handle,
                     uint64_t src_parent, const char *src_name,
                     uint64_t dst_parent, const char *dst_name,
                     int32_t src_parent_nlink_delta,
                     int32_t dst_parent_nlink_delta,
                     const uint8_t *src_child_buf, uint32_t sc_len,
                     uint64_t src_child_fid, uint8_t src_child_type,
                     int dst_exists,
                     const uint8_t *dst_child_buf, uint32_t dc_len,
                     uint64_t dst_child_fid, int delete_dst_child)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *ino_tbl, *dir_tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    struct mds_inode sc_ino, dc_ino;
    uint32_t sc_shard, dc_shard;
    bool cross_dir;
    uint8_t src_name_value[MDS_MAX_NAME + 2];
    uint8_t dst_name_value[MDS_MAX_NAME + 2];
    uint32_t src_name_value_len = 0;
    uint32_t dst_name_value_len = 0;

    if (state == nullptr || src_name == nullptr || dst_name == nullptr ||
        src_child_buf == nullptr) {
        return -1;
    }
    if (rondb_encode_varbinary_string(src_name, 1U,
                                      src_name_value, sizeof(src_name_value),
                                      &src_name_value_len) != 0 ||
        rondb_encode_varbinary_string(dst_name, 1U,
                                      dst_name_value, sizeof(dst_name_value),
                                      &dst_name_value_len) != 0) {
        return -1;
    }

    cross_dir = (src_parent != dst_parent);

    /* Deserialise inode buffers (only child inodes; parent updates
     * are now handled by interpreted atomic operations). */
    if (rondb_inode_deserialize(src_child_buf, sc_len,
                                &sc_ino, &sc_shard) != 0) {
        return -1;
    }
    if (dst_exists && !delete_dst_child && dst_child_buf != nullptr) {
        if (rondb_inode_deserialize(dst_child_buf, dc_len,
                                    &dc_ino, &dc_shard) != 0) {
            return -1;
        }
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    ino_tbl = dict->getTable(RONDB_TBL_INODES);
    dir_tbl = dict->getTable(RONDB_TBL_DIRENTS);
    if (ino_tbl == nullptr || dir_tbl == nullptr) { return -1; }

    /* TC hint on src_parent partition. */
    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, src_parent);
        tx = rondb_get_ndb(state)->startTransaction(dir_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "rename startTx");
    }

    /* 1. Delete src dirent. */
    op = tx->getNdbOperation(dir_tbl);
    if (op == nullptr) { goto rename_err; }
    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_DIR_COL_PARENT, src_parent);
    op->equal(RONDB_DIR_COL_NAME,
              (const char *)src_name_value,
              src_name_value_len);

    /* 2. Write dst dirent (writeTuple handles both insert and overwrite). */
    op = tx->getNdbOperation(dir_tbl);
    if (op == nullptr) { goto rename_err; }
    op->writeTuple();
    (void)rondb_equal_u64(op, RONDB_DIR_COL_PARENT, dst_parent);
    op->equal(RONDB_DIR_COL_NAME,
              (const char *)dst_name_value,
              dst_name_value_len);
    (void)rondb_set_value_u64(op, RONDB_DIR_COL_CHILD_FID, src_child_fid);
    op->setValue(RONDB_DIR_COL_CHILD_TYPE, (Uint32)src_child_type);

    /* 3. Interpreted src parent inode update (atomic nlink + change). */
    if (rondb_interpreted_parent_update(tx, ino_tbl, src_parent,
                                        src_parent_nlink_delta) != 0) {
        goto rename_err;
    }

    /* 4. Interpreted dst parent inode update (if cross-directory). */
    if (cross_dir) {
        if (rondb_interpreted_parent_update(tx, ino_tbl, dst_parent,
                                            dst_parent_nlink_delta) != 0) {
            goto rename_err;
        }
    }

    /* 5. Update src child inode (parent_fileid change for cross-dir). */
    op = tx->getNdbOperation(ino_tbl);
    if (op == nullptr) { goto rename_err; }
    op->updateTuple();
    (void)rondb_equal_u64(op, RONDB_INO_COL_FILEID, sc_ino.fileid);
    rondb_set_inode_values(op, &sc_ino, sc_shard);

    /* 6. Overwrite: update or delete dst child inode. */
    if (dst_exists) {
        op = tx->getNdbOperation(ino_tbl);
        if (op == nullptr) { goto rename_err; }
        if (delete_dst_child) {
            op->deleteTuple();
            (void)rondb_equal_u64(op, RONDB_INO_COL_FILEID, dst_child_fid);
        } else {
            op->updateTuple();
            (void)rondb_equal_u64(op, RONDB_INO_COL_FILEID, dc_ino.fileid);
            rondb_set_inode_values(op, &dc_ino, dc_shard);
        }
    }

    /* Commit all operations atomically. */
    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "rename commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;

rename_err:
    err = tx->getNdbError();
    rondb_get_ndb(state)->closeTransaction(tx);
    return rondb_report_error(err, "rename op");
}

/* -----------------------------------------------------------------------
 * Stage E -- namespace intent lock operations.
 *
 * Acquire = create/update a resource row plus a per-holder row.
 * Release = delete a holder row and shrink/remove the resource row.
 * Test    = readTuple of the aggregate resource row.
 * ----------------------------------------------------------------------- */

static int rondb_shim_lock_acquire_once(void *handle,
                                        uint64_t partition_hint,
                                        uint8_t resource_class,
                                        const uint8_t *resource_key,
                                        uint32_t key_len,
                                        uint8_t lock_mode,
                                        uint32_t owner_mds_id,
                                        uint64_t owner_epoch,
                                        uint64_t fencing_epoch,
                                        uint32_t ttl_ms)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *res_tbl;
    const NdbDictionary::Table *holder_tbl;
    NdbTransaction *tx;
    NdbOperation *res_read;
    NdbOperation *holder_read;
    NdbRecAttr *res_part_hint_attr;
    NdbRecAttr *res_mode_attr;
    NdbRecAttr *res_holder_count_attr;
    NdbRecAttr *res_owner_mds_attr;
    NdbRecAttr *res_owner_epoch_attr;
    NdbRecAttr *res_fence_attr;
    NdbRecAttr *res_ttl_attr;
    NdbRecAttr *holder_mode_attr;
    NdbError err;
    NdbError res_read_err;
    NdbError holder_read_err;
    bool res_exists;
    bool holder_exists;
    uint64_t now_ns;
    uint64_t expected_part_hint = 0;
    uint64_t stored_part_hint = 0;
    uint8_t resource_key_value[RONDB_LOCK_KEY_MAX + 2];
    uint32_t resource_key_value_len = 0;
    uint64_t res_owner_epoch = 0;
    uint64_t res_fence = 0;
    uint32_t res_holder_count = 0;
    uint32_t res_owner_mds = 0;
    uint32_t res_ttl = 0;
    uint8_t res_mode = 0;
    uint8_t holder_mode = 0;

    if (state == nullptr || resource_key == nullptr || key_len == 0 ||
        key_len > RONDB_LOCK_KEY_MAX || !rondb_lock_mode_valid(lock_mode)) {
        return -1;
    }
    if (rondb_lock_partition_hint_from_key(resource_class, resource_key,
                                           key_len, &expected_part_hint) != 0 ||
        expected_part_hint != partition_hint) {
        std::fprintf(stderr,
                     "ERROR: lock_acquire received invalid partition_hint for "
                     "resource_class=%u\n",
                     (unsigned)resource_class);
        return -1;
    }
    if (rondb_encode_varbinary_value(resource_key, key_len, 2U,
                                     resource_key_value,
                                     sizeof(resource_key_value),
                                     &resource_key_value_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    res_tbl = dict->getTable(RONDB_TBL_NS_LOCKS);
    holder_tbl = dict->getTable(RONDB_TBL_NS_LOCK_HOLDERS);
    if (res_tbl == nullptr || holder_tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                  "lock_acquire startTx");
    }

    res_read = tx->getNdbOperation(res_tbl);
    if (res_read == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_acquire getResRead");
    }
    res_read->readTuple(NdbOperation::LM_Exclusive);
    (void)rondb_equal_u64(res_read, RONDB_LK_COL_PART_HINT, partition_hint);
    res_read->equal(RONDB_LK_COL_RES_CLASS, (Uint32)resource_class);
    res_read->equal(RONDB_LK_COL_RES_KEY,
                    (const char *)resource_key_value,
                    resource_key_value_len);
    res_part_hint_attr = res_read->getValue(RONDB_LK_COL_PART_HINT, nullptr);
    res_mode_attr = res_read->getValue(RONDB_LK_COL_LOCK_MODE, nullptr);
    res_holder_count_attr = res_read->getValue(RONDB_LK_COL_HOLDER_COUNT, nullptr);
    res_owner_mds_attr = res_read->getValue(RONDB_LK_COL_OWNER_MDS, nullptr);
    res_owner_epoch_attr = res_read->getValue(RONDB_LK_COL_OWNER_EPOCH, nullptr);
    res_fence_attr = res_read->getValue(RONDB_LK_COL_FENCE_EPOCH, nullptr);
    res_ttl_attr = res_read->getValue(RONDB_LK_COL_TTL_MS, nullptr);

    holder_read = tx->getNdbOperation(holder_tbl);
    if (holder_read == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_acquire getHolderRead");
    }
    holder_read->readTuple(NdbOperation::LM_Exclusive);
    (void)rondb_equal_u64(holder_read, RONDB_LK_COL_PART_HINT, partition_hint);
    holder_read->equal(RONDB_LK_COL_RES_CLASS, (Uint32)resource_class);
    holder_read->equal(RONDB_LK_COL_RES_KEY,
                       (const char *)resource_key_value,
                       resource_key_value_len);
    holder_read->equal(RONDB_LK_COL_OWNER_MDS, owner_mds_id);
    (void)rondb_equal_u64(holder_read, RONDB_LK_COL_OWNER_EPOCH, owner_epoch);
    holder_mode_attr = holder_read->getValue(RONDB_LK_COL_LOCK_MODE, nullptr);

    if (res_part_hint_attr == nullptr || res_mode_attr == nullptr ||
        res_holder_count_attr == nullptr || res_owner_mds_attr == nullptr ||
        res_owner_epoch_attr == nullptr || res_fence_attr == nullptr ||
        res_ttl_attr == nullptr || holder_mode_attr == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_acquire getValue");
    }

    if (tx->execute(NdbTransaction::NoCommit, NdbOperation::AO_IgnoreError) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_acquire readPhase");
    }

    res_read_err = res_read->getNdbError();
    holder_read_err = holder_read->getNdbError();
    res_exists = !rondb_lock_row_not_found(res_read_err);
    holder_exists = !rondb_lock_row_not_found(holder_read_err);

    if (res_read_err.code != 0 && !rondb_lock_row_not_found(res_read_err)) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(res_read_err, "lock_acquire resourceRead");
    }
    if (holder_read_err.code != 0 && !rondb_lock_row_not_found(holder_read_err)) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(holder_read_err, "lock_acquire holderRead");
    }

    if (res_exists) {
        stored_part_hint = res_part_hint_attr->u_64_value();
        res_mode = (uint8_t)res_mode_attr->u_8_value();
        res_holder_count = res_holder_count_attr->u_32_value();
        res_owner_mds = res_owner_mds_attr->u_32_value();
        res_owner_epoch = res_owner_epoch_attr->u_64_value();
        res_fence = res_fence_attr->u_64_value();
        res_ttl = res_ttl_attr->u_32_value();

        if (stored_part_hint != partition_hint) {
            std::fprintf(stderr,
                         "ERROR: lock_acquire partition_hint mismatch for "
                         "resource_class=%u\n",
                         (unsigned)resource_class);
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }
        if (!rondb_lock_mode_valid(res_mode) || res_holder_count == 0U) {
            std::fprintf(stderr,
                         "ERROR: lock_acquire observed invalid resource row "
                         "state for resource_class=%u\n",
                         (unsigned)resource_class);
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }
        if (res_mode == RONDB_LOCK_MODE_EXCLUSIVE) {
            if (res_holder_count != 1U) {
                std::fprintf(stderr,
                             "ERROR: lock_acquire observed invalid exclusive "
                             "resource row for resource_class=%u\n",
                             (unsigned)resource_class);
                rondb_get_ndb(state)->closeTransaction(tx);
                return -1;
            }
        } else if (res_owner_mds != 0U || res_owner_epoch != 0ULL) {
            std::fprintf(stderr,
                         "ERROR: shared resource row carried exclusive owner "
                         "identity for resource_class=%u\n",
                         (unsigned)resource_class);
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }
    }

    if (holder_exists) {
        holder_mode = (uint8_t)holder_mode_attr->u_8_value();
        if (!res_exists) {
            std::fprintf(stderr,
                         "ERROR: lock_acquire saw holder row without resource row\n");
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }
        if (holder_mode != lock_mode || res_mode != lock_mode) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return 1;
        }
        if (res_mode == RONDB_LOCK_MODE_EXCLUSIVE &&
            (res_owner_mds != owner_mds_id || res_owner_epoch != owner_epoch)) {
            std::fprintf(stderr,
                         "ERROR: exclusive resource owner does not match holder row\n");
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }
    }

    if (rondb_now_ns(&now_ns) != 0) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return -1;
    }

    if (!res_exists) {
        uint32_t resource_owner_mds =
            (lock_mode == RONDB_LOCK_MODE_EXCLUSIVE) ? owner_mds_id : 0U;
        uint64_t resource_owner_epoch =
            (lock_mode == RONDB_LOCK_MODE_EXCLUSIVE) ? owner_epoch : 0ULL;

        if (rondb_add_lock_resource_insert(tx, res_tbl, partition_hint,
                                           resource_class, resource_key_value,
                                           resource_key_value_len,
                                           lock_mode, 1U, resource_owner_mds,
                                           resource_owner_epoch, fencing_epoch,
                                           now_ns, ttl_ms) != 0 ||
            rondb_add_lock_holder_insert(tx, holder_tbl, partition_hint,
                                         resource_class, resource_key_value,
                                         resource_key_value_len,
                                         lock_mode, owner_mds_id, owner_epoch,
                                         fencing_epoch, now_ns, ttl_ms) != 0) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "lock_acquire insertPhase");
        }
    } else if (holder_exists) {
        uint64_t resource_fence = fencing_epoch;
        uint32_t resource_ttl = ttl_ms;
        uint32_t resource_owner_mds = owner_mds_id;
        uint64_t resource_owner_epoch = owner_epoch;

        if (lock_mode == RONDB_LOCK_MODE_SHARED) {
            resource_fence = rondb_u64_max(res_fence, fencing_epoch);
            resource_ttl = rondb_u32_max(res_ttl, ttl_ms);
            resource_owner_mds = 0U;
            resource_owner_epoch = 0ULL;
        }

        if (rondb_add_lock_holder_update(tx, holder_tbl, partition_hint,
                                         resource_class, resource_key_value,
                                         resource_key_value_len,
                                         lock_mode, owner_mds_id, owner_epoch,
                                         fencing_epoch, now_ns, ttl_ms) != 0 ||
            rondb_add_lock_resource_update(tx, res_tbl, partition_hint,
                                           resource_class, resource_key_value,
                                           resource_key_value_len,
                                           lock_mode, res_holder_count,
                                           resource_owner_mds,
                                           resource_owner_epoch,
                                           resource_fence, now_ns,
                                           resource_ttl) != 0) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "lock_acquire renewPhase");
        }
    } else if (lock_mode == RONDB_LOCK_MODE_SHARED &&
               res_mode == RONDB_LOCK_MODE_SHARED) {
        if (rondb_add_lock_resource_update(tx, res_tbl, partition_hint,
                                           resource_class, resource_key_value,
                                           resource_key_value_len,
                                           RONDB_LOCK_MODE_SHARED,
                                           res_holder_count + 1U, 0U, 0ULL,
                                           rondb_u64_max(res_fence, fencing_epoch),
                                           now_ns,
                                           rondb_u32_max(res_ttl, ttl_ms)) != 0 ||
            rondb_add_lock_holder_insert(tx, holder_tbl, partition_hint,
                                         resource_class, resource_key_value,
                                         resource_key_value_len,
                                         RONDB_LOCK_MODE_SHARED,
                                         owner_mds_id, owner_epoch,
                                         fencing_epoch, now_ns, ttl_ms) != 0) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "lock_acquire sharedJoinPhase");
        }
    } else {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 1;
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::ConstraintViolation) {
            return 2;
        }
        return rondb_report_error(err, "lock_acquire commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_lock_acquire(void *handle,
                            uint64_t partition_hint,
                            uint8_t resource_class,
                            const uint8_t *resource_key, uint32_t key_len,
                            uint8_t lock_mode,
                            uint32_t owner_mds_id, uint64_t owner_epoch,
                            uint64_t fencing_epoch,
                            uint32_t ttl_ms)
{
    int attempt;

    for (attempt = 0; attempt < 4; attempt++) {
        int rc = rondb_shim_lock_acquire_once(handle, partition_hint,
                                              resource_class, resource_key,
                                              key_len, lock_mode, owner_mds_id,
                                              owner_epoch, fencing_epoch,
                                              ttl_ms);
        if (rc != 2) {
            return rc;
        }
    }

    return 1;
}

int rondb_shim_lock_release(void *handle,
                            uint8_t resource_class,
                            const uint8_t *resource_key, uint32_t key_len,
                            uint32_t owner_mds_id, uint64_t owner_epoch)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *res_tbl;
    const NdbDictionary::Table *holder_tbl;
    NdbTransaction *tx;
    NdbOperation *res_read;
    NdbOperation *holder_read;
    NdbRecAttr *res_part_hint_attr;
    NdbRecAttr *res_mode_attr;
    NdbRecAttr *res_holder_count_attr;
    NdbRecAttr *res_owner_mds_attr;
    NdbRecAttr *res_owner_epoch_attr;
    NdbRecAttr *res_fence_attr;
    NdbRecAttr *res_granted_attr;
    NdbRecAttr *res_ttl_attr;
    NdbError err;
    NdbError res_read_err;
    NdbError holder_read_err;
    bool res_exists;
    bool holder_exists;
    uint64_t partition_hint = 0;
    uint8_t resource_key_value[RONDB_LOCK_KEY_MAX + 2];
    uint32_t resource_key_value_len = 0;
    uint64_t res_owner_epoch = 0;
    uint64_t res_fence = 0;
    uint64_t res_granted_at = 0;
    uint32_t res_holder_count = 0;
    uint32_t res_owner_mds = 0;
    uint32_t res_ttl = 0;
    uint8_t res_mode = 0;

    if (state == nullptr || resource_key == nullptr || key_len == 0 ||
        key_len > RONDB_LOCK_KEY_MAX) {
        return -1;
    }
    if (rondb_lock_partition_hint_from_key(resource_class, resource_key,
                                           key_len, &partition_hint) != 0) {
        std::fprintf(stderr,
                     "ERROR: lock_release received invalid resource key for "
                     "resource_class=%u\n",
                     (unsigned)resource_class);
        return -1;
    }
    if (rondb_encode_varbinary_value(resource_key, key_len, 2U,
                                     resource_key_value,
                                     sizeof(resource_key_value),
                                     &resource_key_value_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    res_tbl = dict->getTable(RONDB_TBL_NS_LOCKS);
    holder_tbl = dict->getTable(RONDB_TBL_NS_LOCK_HOLDERS);
    if (res_tbl == nullptr || holder_tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                  "lock_release startTx");
    }

    res_read = tx->getNdbOperation(res_tbl);
    if (res_read == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_release getResRead");
    }
    res_read->readTuple(NdbOperation::LM_Exclusive);
    (void)rondb_equal_u64(res_read, RONDB_LK_COL_PART_HINT, partition_hint);
    res_read->equal(RONDB_LK_COL_RES_CLASS, (Uint32)resource_class);
    res_read->equal(RONDB_LK_COL_RES_KEY,
                    (const char *)resource_key_value,
                    resource_key_value_len);
    res_part_hint_attr = res_read->getValue(RONDB_LK_COL_PART_HINT, nullptr);
    res_mode_attr = res_read->getValue(RONDB_LK_COL_LOCK_MODE, nullptr);
    res_holder_count_attr = res_read->getValue(RONDB_LK_COL_HOLDER_COUNT, nullptr);
    res_owner_mds_attr = res_read->getValue(RONDB_LK_COL_OWNER_MDS, nullptr);
    res_owner_epoch_attr = res_read->getValue(RONDB_LK_COL_OWNER_EPOCH, nullptr);
    res_fence_attr = res_read->getValue(RONDB_LK_COL_FENCE_EPOCH, nullptr);
    res_granted_attr = res_read->getValue(RONDB_LK_COL_GRANTED_AT, nullptr);
    res_ttl_attr = res_read->getValue(RONDB_LK_COL_TTL_MS, nullptr);

    holder_read = tx->getNdbOperation(holder_tbl);
    if (holder_read == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_release getHolderRead");
    }
    holder_read->readTuple(NdbOperation::LM_Exclusive);
    (void)rondb_equal_u64(holder_read, RONDB_LK_COL_PART_HINT, partition_hint);
    holder_read->equal(RONDB_LK_COL_RES_CLASS, (Uint32)resource_class);
    holder_read->equal(RONDB_LK_COL_RES_KEY,
                       (const char *)resource_key_value,
                       resource_key_value_len);
    holder_read->equal(RONDB_LK_COL_OWNER_MDS, owner_mds_id);
    (void)rondb_equal_u64(holder_read, RONDB_LK_COL_OWNER_EPOCH, owner_epoch);

    if (res_part_hint_attr == nullptr || res_mode_attr == nullptr ||
        res_holder_count_attr == nullptr || res_owner_mds_attr == nullptr ||
        res_owner_epoch_attr == nullptr || res_fence_attr == nullptr ||
        res_granted_attr == nullptr || res_ttl_attr == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_release getValue");
    }

    if (tx->execute(NdbTransaction::NoCommit, NdbOperation::AO_IgnoreError) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_release readPhase");
    }

    res_read_err = res_read->getNdbError();
    holder_read_err = holder_read->getNdbError();
    res_exists = !rondb_lock_row_not_found(res_read_err);
    holder_exists = !rondb_lock_row_not_found(holder_read_err);

    if (res_read_err.code != 0 && !rondb_lock_row_not_found(res_read_err)) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(res_read_err, "lock_release resourceRead");
    }
    if (holder_read_err.code != 0 &&
        !rondb_lock_row_not_found(holder_read_err)) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(holder_read_err, "lock_release holderRead");
    }

    if (!holder_exists) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 0;
    }
    if (!res_exists) {
        rondb_get_ndb(state)->closeTransaction(tx);
        std::fprintf(stderr,
                     "ERROR: lock_release saw holder row without resource row\n");
        return -1;
    }

    partition_hint = res_part_hint_attr->u_64_value();
    res_mode = (uint8_t)res_mode_attr->u_8_value();
    res_holder_count = res_holder_count_attr->u_32_value();
    res_owner_mds = res_owner_mds_attr->u_32_value();
    res_owner_epoch = res_owner_epoch_attr->u_64_value();
    res_fence = res_fence_attr->u_64_value();
    res_granted_at = res_granted_attr->u_64_value();
    res_ttl = res_ttl_attr->u_32_value();

    if (!rondb_lock_mode_valid(res_mode) || res_holder_count == 0U) {
        rondb_get_ndb(state)->closeTransaction(tx);
        std::fprintf(stderr,
                     "ERROR: lock_release observed invalid resource row state\n");
        return -1;
    }

    if (rondb_add_lock_holder_delete(tx, holder_tbl, partition_hint,
                                     resource_class, resource_key_value,
                                     resource_key_value_len,
                                     owner_mds_id, owner_epoch) != 0) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_release deleteHolder");
    }

    if (res_mode == RONDB_LOCK_MODE_EXCLUSIVE) {
        if (res_holder_count != 1U || res_owner_mds != owner_mds_id ||
            res_owner_epoch != owner_epoch) {
            rondb_get_ndb(state)->closeTransaction(tx);
            std::fprintf(stderr,
                         "ERROR: lock_release exclusive owner mismatch\n");
            return -1;
        }
        if (rondb_add_lock_resource_delete(tx, res_tbl, partition_hint,
                                           resource_class, resource_key_value,
                                           resource_key_value_len) != 0) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "lock_release deleteResource");
        }
    } else if (res_holder_count == 1U) {
        if (rondb_add_lock_resource_delete(tx, res_tbl, partition_hint,
                                           resource_class, resource_key_value,
                                           resource_key_value_len) != 0) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err,
                                      "lock_release deleteSharedResource");
        }
    } else {
        if (rondb_add_lock_resource_update(tx, res_tbl, partition_hint,
                                           resource_class, resource_key_value,
                                           resource_key_value_len,
                                           RONDB_LOCK_MODE_SHARED,
                                           res_holder_count - 1U, 0U, 0ULL,
                                           res_fence, res_granted_at,
                                           res_ttl) != 0) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "lock_release shrinkShared");
        }
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_release commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_lock_test(void *handle,
                         uint8_t resource_class,
                         const uint8_t *resource_key, uint32_t key_len,
                         uint8_t *lock_mode_out,
                         uint32_t *holder_count_out,
                         uint64_t *fencing_epoch_out,
                         uint32_t *owner_mds_out,
                         uint64_t *owner_epoch_out)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *a_mode, *a_count, *a_fence, *a_mds, *a_epoch;
    NdbError err;
    uint64_t partition_hint = 0;
    uint8_t resource_key_value[RONDB_LOCK_KEY_MAX + 2];
    uint32_t resource_key_value_len = 0;
    uint8_t mode;

    if (state == nullptr || resource_key == nullptr || key_len == 0 ||
        key_len > RONDB_LOCK_KEY_MAX) {
        return -1;
    }
    if (rondb_lock_partition_hint_from_key(resource_class, resource_key,
                                           key_len, &partition_hint) != 0) {
        std::fprintf(stderr,
                     "ERROR: lock_test received invalid resource key for "
                     "resource_class=%u\n",
                     (unsigned)resource_class);
        return -1;
    }
    if (rondb_encode_varbinary_value(resource_key, key_len, 2U,
                                     resource_key_value,
                                     sizeof(resource_key_value),
                                     &resource_key_value_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_NS_LOCKS);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                  "lock_test startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_test getOp");
    }

    op->readTuple(NdbOperation::LM_CommittedRead);
    (void)rondb_equal_u64(op, RONDB_LK_COL_PART_HINT, partition_hint);
    op->equal(RONDB_LK_COL_RES_CLASS, (Uint32)resource_class);
    op->equal(RONDB_LK_COL_RES_KEY,
              (const char *)resource_key_value,
              resource_key_value_len);

    a_mode = op->getValue(RONDB_LK_COL_LOCK_MODE, nullptr);
    a_count = op->getValue(RONDB_LK_COL_HOLDER_COUNT, nullptr);
    a_fence = op->getValue(RONDB_LK_COL_FENCE_EPOCH, nullptr);
    a_mds = op->getValue(RONDB_LK_COL_OWNER_MDS, nullptr);
    a_epoch = op->getValue(RONDB_LK_COL_OWNER_EPOCH, nullptr);

    if (a_mode == nullptr || a_count == nullptr || a_fence == nullptr ||
        a_mds == nullptr || a_epoch == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_test getValue");
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "lock_test commit");
    }

    mode = (uint8_t)a_mode->u_8_value();
    if (!rondb_lock_mode_valid(mode) || a_count->u_32_value() == 0U) {
        rondb_get_ndb(state)->closeTransaction(tx);
        std::fprintf(stderr, "ERROR: lock_test observed invalid resource row\n");
        return -1;
    }

    if (lock_mode_out != nullptr) {
        *lock_mode_out = mode;
    }
    if (holder_count_out != nullptr) {
        *holder_count_out = a_count->u_32_value();
    }
    if (fencing_epoch_out != nullptr) {
        *fencing_epoch_out = a_fence->u_64_value();
    }
    if (owner_mds_out != nullptr) {
        *owner_mds_out = (mode == RONDB_LOCK_MODE_EXCLUSIVE) ?
                         a_mds->u_32_value() : 0U;
    }
    if (owner_epoch_out != nullptr) {
        *owner_epoch_out = (mode == RONDB_LOCK_MODE_EXCLUSIVE) ?
                           a_epoch->u_64_value() : 0ULL;
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 8A -- DS provisioning CRUD (mds_ds_provision: PK=ds_id)
 * ----------------------------------------------------------------------- */

int rondb_shim_ds_provision_get(void *handle, uint32_t ds_id,
                                uint8_t *secret_buf, uint32_t secret_buflen,
                                uint32_t *secret_outlen, uint64_t *epoch)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *a_secret, *a_epoch;
    NdbError err;

    if (state == nullptr || secret_outlen == nullptr || epoch == nullptr) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DS_PROVISION);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "ds_prov_get startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ds_prov_get readOp");
    }
    op->readTuple(NdbOperation::LM_CommittedRead);
    op->equal(RONDB_DSP_COL_DS_ID, (Uint32)ds_id);
    a_secret = op->getValue(RONDB_DSP_COL_SECRET, nullptr);
    a_epoch  = op->getValue(RONDB_DSP_COL_EPOCH, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "ds_prov_get exec");
    }

    {
        NdbError op_err = op->getNdbError();
        if (op_err.code == 626 ||
            op_err.classification == NdbError::NoDataFound) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return 1;
        }
    }

    *epoch = a_epoch->u_64_value();

    /* VARBINARY(256): 2-byte LE length prefix. */
    {
        const char *ref = a_secret->aRef();
        uint32_t data_len = 0;

        if (ref != nullptr) {
            data_len = (uint32_t)(uint8_t)ref[0] |
                       ((uint32_t)(uint8_t)ref[1] << 8);
            if (secret_buf != nullptr && data_len > 0) {
                uint32_t copy = (data_len > secret_buflen) ? secret_buflen
                                                          : data_len;
                std::memcpy(secret_buf, ref + 2, copy);
            }
        }
        *secret_outlen = data_len;
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_ds_provision_put(void *handle, uint32_t ds_id,
                                const uint8_t *secret, uint32_t secret_len,
                                uint64_t epoch)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    uint8_t secret_enc[256 + 4];
    uint32_t secret_enc_len = 0;

    if (state == nullptr || (secret_len > 0 && secret == nullptr) ||
        secret_len > 256) {
        return -1;
    }
    if (rondb_encode_varbinary_value(secret, secret_len, 2U,
                                     secret_enc, sizeof(secret_enc),
                                     &secret_enc_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DS_PROVISION);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "ds_prov_put startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ds_prov_put writeOp");
    }
    op->writeTuple();
    op->equal(RONDB_DSP_COL_DS_ID, (Uint32)ds_id);
    op->setValue(RONDB_DSP_COL_SECRET,
                 (const char *)secret_enc, secret_enc_len);
    (void)rondb_set_value_u64(op, RONDB_DSP_COL_EPOCH, epoch);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ds_prov_put commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_ds_provision_del(void *handle, uint32_t ds_id)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DS_PROVISION);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "ds_prov_del startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "ds_prov_del delOp");
    }
    op->deleteTuple();
    op->equal(RONDB_DSP_COL_DS_ID, (Uint32)ds_id);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 0; }
        return rondb_report_error(err, "ds_prov_del commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 8A -- Quota rules CRUD (mds_quota_rules: PK=(scope_type, scope_id))
 * ----------------------------------------------------------------------- */

int rondb_shim_quota_rule_get(void *handle, uint8_t scope_type,
                              uint64_t scope_id,
                              struct mds_quota_rule *rule)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *a_hb, *a_sb, *a_hi, *a_si, *a_grace;
    NdbError err;

    if (state == nullptr || rule == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_QUOTA_RULES);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "qrule_get startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "qrule_get readOp");
    }
    op->readTuple(NdbOperation::LM_CommittedRead);
    op->equal(RONDB_QR_COL_SCOPE_TYPE, (Uint32)scope_type);
    (void)rondb_equal_u64(op, RONDB_QR_COL_SCOPE_ID, scope_id);
    a_hb    = op->getValue(RONDB_QR_COL_HARD_BYTES, nullptr);
    a_sb    = op->getValue(RONDB_QR_COL_SOFT_BYTES, nullptr);
    a_hi    = op->getValue(RONDB_QR_COL_HARD_INODES, nullptr);
    a_si    = op->getValue(RONDB_QR_COL_SOFT_INODES, nullptr);
    a_grace = op->getValue(RONDB_QR_COL_GRACE_SEC, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "qrule_get exec");
    }

    {
        NdbError op_err = op->getNdbError();
        if (op_err.code == 626 ||
            op_err.classification == NdbError::NoDataFound) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return 1;
        }
    }

    std::memset(rule, 0, sizeof(*rule));
    rule->hard_bytes  = a_hb->u_64_value();
    rule->soft_bytes  = a_sb->u_64_value();
    rule->hard_inodes = a_hi->u_64_value();
    rule->soft_inodes = a_si->u_64_value();
    rule->grace_sec   = a_grace->u_32_value();

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_quota_rule_put(void *handle, uint8_t scope_type,
                              uint64_t scope_id,
                              const struct mds_quota_rule *rule)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr || rule == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_QUOTA_RULES);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "qrule_put startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "qrule_put writeOp");
    }
    op->writeTuple();
    op->equal(RONDB_QR_COL_SCOPE_TYPE, (Uint32)scope_type);
    (void)rondb_equal_u64(op, RONDB_QR_COL_SCOPE_ID, scope_id);
    (void)rondb_set_value_u64(op, RONDB_QR_COL_HARD_BYTES, rule->hard_bytes);
    (void)rondb_set_value_u64(op, RONDB_QR_COL_SOFT_BYTES, rule->soft_bytes);
    (void)rondb_set_value_u64(op, RONDB_QR_COL_HARD_INODES, rule->hard_inodes);
    (void)rondb_set_value_u64(op, RONDB_QR_COL_SOFT_INODES, rule->soft_inodes);
    op->setValue(RONDB_QR_COL_GRACE_SEC, rule->grace_sec);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "qrule_put commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 8A -- Quota usage CRUD (mds_quota_usage: PK=(usage_type, scope_id))
 * ----------------------------------------------------------------------- */

int rondb_shim_quota_usage_get(void *handle, uint8_t usage_type,
                               uint64_t scope_id,
                               struct mds_quota_usage *usage)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *a_ub, *a_ui, *a_gb, *a_gi;
    NdbError err;

    if (state == nullptr || usage == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_QUOTA_USAGE);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "qusage_get startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "qusage_get readOp");
    }
    op->readTuple(NdbOperation::LM_CommittedRead);
    op->equal(RONDB_QU_COL_USAGE_TYPE, (Uint32)usage_type);
    (void)rondb_equal_u64(op, RONDB_QU_COL_SCOPE_ID, scope_id);
    a_ub = op->getValue(RONDB_QU_COL_USED_BYTES, nullptr);
    a_ui = op->getValue(RONDB_QU_COL_USED_INODES, nullptr);
    a_gb = op->getValue(RONDB_QU_COL_GRACE_BYTES, nullptr);
    a_gi = op->getValue(RONDB_QU_COL_GRACE_INODES, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "qusage_get exec");
    }

    {
        NdbError op_err = op->getNdbError();
        if (op_err.code == 626 ||
            op_err.classification == NdbError::NoDataFound) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return 1;
        }
    }

    std::memset(usage, 0, sizeof(*usage));
    usage->used_bytes        = a_ub->u_64_value();
    usage->used_inodes       = a_ui->u_64_value();
    /* grace_start fields stored as BIGUNSIGNED, reinterpret as int64_t. */
    usage->grace_start_bytes  = (int64_t)a_gb->u_64_value();
    usage->grace_start_inodes = (int64_t)a_gi->u_64_value();

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_quota_usage_put(void *handle, uint8_t usage_type,
                               uint64_t scope_id,
                               const struct mds_quota_usage *usage)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr || usage == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_QUOTA_USAGE);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "qusage_put startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "qusage_put writeOp");
    }
    op->writeTuple();
    op->equal(RONDB_QU_COL_USAGE_TYPE, (Uint32)usage_type);
    (void)rondb_equal_u64(op, RONDB_QU_COL_SCOPE_ID, scope_id);
    (void)rondb_set_value_u64(op, RONDB_QU_COL_USED_BYTES,
                              usage->used_bytes);
    (void)rondb_set_value_u64(op, RONDB_QU_COL_USED_INODES,
                              usage->used_inodes);
    (void)rondb_set_value_u64(op, RONDB_QU_COL_GRACE_BYTES,
                              (uint64_t)usage->grace_start_bytes);
    (void)rondb_set_value_u64(op, RONDB_QU_COL_GRACE_INODES,
                              (uint64_t)usage->grace_start_inodes);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "qusage_put commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 8A -- GC queue (mds_gc_queue: PK=gc_seq, FIFO via mds_meta counter)
 * ----------------------------------------------------------------------- */

int rondb_shim_gc_seq_alloc(void *handle, uint64_t *seq_out)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *val_attr;
    NdbError err;
    uint64_t old_val;
    char key_buf[64];

    if (state == nullptr || seq_out == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_META);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "gc_seq_alloc startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_seq_alloc readOp");
    }
    op->readTuple(NdbOperation::LM_Exclusive);
    std::memset(key_buf, ' ', sizeof(key_buf));
    std::strncpy(key_buf, RONDB_META_KEY_GC_SEQ, sizeof(key_buf) - 1);
    op->equal(RONDB_META_COL_KEY, key_buf);
    val_attr = op->getValue(RONDB_META_COL_VAL, nullptr);
    /* See fileid_batch_alloc: NULL NdbRecAttr must abort before use. */
    if (val_attr == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_seq_alloc getValue");
    }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_seq_alloc read exec");
    }

    old_val = val_attr->u_64_value();

    {
        NdbOperation *upd = tx->getNdbOperation(tbl);
        if (upd == nullptr) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "gc_seq_alloc updOp");
        }
        upd->updateTuple();
        upd->equal(RONDB_META_COL_KEY, key_buf);
        (void)rondb_set_value_u64(upd, RONDB_META_COL_VAL, old_val + 1);
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_seq_alloc commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    *seq_out = old_val;
    return 0;
}

int rondb_shim_gc_enqueue(void *handle, uint64_t gc_seq,
                          uint64_t fileid, uint32_t ds_id,
                          const uint8_t *nfs_fh, uint32_t fh_len,
                          uint32_t owner_mds_id)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    uint8_t fh_enc[MDS_NFS_FH_MAX + 2];
    uint32_t fh_enc_len = 0;

    if (state == nullptr || (fh_len > 0 && nfs_fh == nullptr) ||
        fh_len > MDS_NFS_FH_MAX) {
        return -1;
    }
    if (fh_len > 0) {
        if (rondb_encode_varbinary_value(nfs_fh, fh_len, 1U,
                                         fh_enc, sizeof(fh_enc),
                                         &fh_enc_len) != 0) {
            return -1;
        }
    } else {
        fh_enc[0] = 0;
        fh_enc_len = 1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_GC_QUEUE);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "gc_enqueue startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_enqueue insertOp");
    }
    op->insertTuple();
    (void)rondb_set_value_u64(op, RONDB_GC_COL_SEQ, gc_seq);
    (void)rondb_set_value_u64(op, RONDB_GC_COL_FILEID, fileid);
    op->setValue(RONDB_GC_COL_DS_ID, (Uint32)ds_id);
    op->setValue(RONDB_GC_COL_NFS_FH_LEN, (Uint32)fh_len);
    op->setValue(RONDB_GC_COL_NFS_FH,
                 (const char *)fh_enc, fh_enc_len);
    op->setValue(RONDB_GC_COL_OWNER_MDS, (Uint32)owner_mds_id);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_enqueue commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_gc_peek(void *handle, struct mds_gc_entry *entry)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_seq, *a_fid, *a_dsid, *a_fhlen, *a_fh;
    NdbError err;
    int next_rc;
    bool found = false;
    uint64_t min_seq = UINT64_MAX;

    if (state == nullptr || entry == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_GC_QUEUE);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "gc_peek startTx");
    }

    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_peek getScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_peek readTuples");
    }

    a_seq   = scan->getValue(RONDB_GC_COL_SEQ, nullptr);
    a_fid   = scan->getValue(RONDB_GC_COL_FILEID, nullptr);
    a_dsid  = scan->getValue(RONDB_GC_COL_DS_ID, nullptr);
    a_fhlen = scan->getValue(RONDB_GC_COL_NFS_FH_LEN, nullptr);
    a_fh    = scan->getValue(RONDB_GC_COL_NFS_FH, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_peek exec");
    }

    /* Scan all rows, keep the one with the smallest gc_seq. */
    std::memset(entry, 0, sizeof(*entry));
    while ((next_rc = scan->nextResult(true)) == 0) {
        uint64_t seq = a_seq->u_64_value();
        if (seq < min_seq) {
            min_seq = seq;
            entry->gc_seq  = seq;
            entry->fileid  = a_fid->u_64_value();
            entry->ds_id   = a_dsid->u_32_value();
            entry->nfs_fh_len = a_fhlen->u_32_value();
            if (entry->nfs_fh_len > MDS_NFS_FH_MAX) {
                entry->nfs_fh_len = MDS_NFS_FH_MAX;
            }
            std::memset(entry->nfs_fh, 0, MDS_NFS_FH_MAX);
            const char *fh_ptr = a_fh->aRef();
            if (fh_ptr != nullptr && entry->nfs_fh_len > 0) {
                uint32_t vb_len = (uint32_t)(uint8_t)fh_ptr[0];
                if (vb_len > MDS_NFS_FH_MAX) {
                    vb_len = MDS_NFS_FH_MAX;
                }
                std::memcpy(entry->nfs_fh, fh_ptr + 1, vb_len);
            }
            found = true;
        }
    }
    if (next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_peek nextResult");
    }

    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return found ? 0 : 1;
}

/*
 * Batched peek: walk the GC queue once and return the lowest-`cap`
 * entries by gc_seq.  The single full-table scan replaces N separate
 * scans the drainer would otherwise do, dropping per-entry peek cost
 * from O(N) to O(N / cap) on a backlog of N rows.
 *
 * The candidate set is maintained as a max-heap by gc_seq, capped at
 * `cap` slots.  For each row:
 *   - if the heap has room, push and re-heapify.
 *   - else if the row's seq is strictly less than the current max
 *     (heap.front()), replace the max with this row and re-heapify.
 *   - else skip -- this row cannot displace the worst already kept.
 * After the scan we sort ascending by seq so the caller dequeues in
 * the same FIFO order today's single rondb_shim_gc_peek caller saw.
 *
 * Memory: bounded by `cap` * sizeof(struct mds_gc_entry).  We
 * defensively reserve at most 4096 entries up front; if the caller
 * passes a larger cap, the algorithm is still correct, the vector
 * just realloc-grows to it.
 */
int rondb_shim_gc_peek_batch(void *handle, struct mds_gc_entry *entries,
                             uint32_t cap, uint32_t *n_out,
                             uint32_t self_mds_id)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_seq, *a_fid, *a_dsid, *a_fhlen, *a_fh, *a_owner;
    NdbError err;
    int next_rc;
    std::vector<struct mds_gc_entry> heap;
    auto cmp_max = [](const struct mds_gc_entry &a,
                      const struct mds_gc_entry &b) {
        /* max-heap by gc_seq: heap.front() holds the largest. */
        return a.gc_seq < b.gc_seq;
    };

    if (n_out != nullptr) { *n_out = 0; }
    if (state == nullptr || entries == nullptr || cap == 0 ||
        n_out == nullptr) {
        return -1;
    }
    heap.reserve(cap <= 4096U ? cap : 4096U);

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_GC_QUEUE);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "gc_peek_batch startTx");
    }

    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_peek_batch getScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_peek_batch readTuples");
    }

    a_seq   = scan->getValue(RONDB_GC_COL_SEQ, nullptr);
    a_fid   = scan->getValue(RONDB_GC_COL_FILEID, nullptr);
    a_dsid  = scan->getValue(RONDB_GC_COL_DS_ID, nullptr);
    a_fhlen = scan->getValue(RONDB_GC_COL_NFS_FH_LEN, nullptr);
    a_fh    = scan->getValue(RONDB_GC_COL_NFS_FH, nullptr);
    a_owner = scan->getValue(RONDB_GC_COL_OWNER_MDS, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_peek_batch exec");
    }

    while ((next_rc = scan->nextResult(true)) == 0) {
        uint64_t seq = a_seq->u_64_value();

        /*
         * Per-MDS isolation: only drain rows this MDS owns.  owner==0
         * is a legacy/unassigned row (pre-migration) that any MDS may
         * reclaim.  self_mds_id==0 means "drain everything" (single-MDS
         * deployments / callers that opt out of partitioning).
         */
        uint32_t owner = (a_owner != nullptr) ? a_owner->u_32_value() : 0U;
        if (self_mds_id != 0U && owner != 0U && owner != self_mds_id) {
            continue;
        }

        /*
         * Skip cheaply when this row cannot improve the heap.
         * gc_seq is the unique PK so >= max means "no better".
         */
        if (heap.size() == (size_t)cap &&
            seq >= heap.front().gc_seq) {
            continue;
        }

        /* Candidate -- copy out of NDB's cursor buffer.  aRef()
         * pointer becomes invalid on the next nextResult() call. */
        struct mds_gc_entry e;
        std::memset(&e, 0, sizeof(e));
        e.gc_seq       = seq;
        e.fileid       = a_fid->u_64_value();
        e.ds_id        = a_dsid->u_32_value();
        e.owner_mds_id = owner;
        e.nfs_fh_len   = a_fhlen->u_32_value();
        if (e.nfs_fh_len > MDS_NFS_FH_MAX) {
            e.nfs_fh_len = MDS_NFS_FH_MAX;
        }
        const char *fh_ptr = a_fh->aRef();
        if (fh_ptr != nullptr && e.nfs_fh_len > 0) {
            uint32_t vb_len = (uint32_t)(uint8_t)fh_ptr[0];
            if (vb_len > MDS_NFS_FH_MAX) { vb_len = MDS_NFS_FH_MAX; }
            std::memcpy(e.nfs_fh, fh_ptr + 1, vb_len);
        }

        if (heap.size() < (size_t)cap) {
            heap.push_back(e);
            std::push_heap(heap.begin(), heap.end(), cmp_max);
        } else {
            std::pop_heap(heap.begin(), heap.end(), cmp_max);
            heap.back() = e;
            std::push_heap(heap.begin(), heap.end(), cmp_max);
        }
    }
    if (next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_peek_batch nextResult");
    }

    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);

    /* Sort the kept rows ascending by gc_seq so callers dequeue in
     * the same FIFO order the single-row peek delivered. */
    std::sort(heap.begin(), heap.end(),
              [](const struct mds_gc_entry &a,
                 const struct mds_gc_entry &b) {
                  return a.gc_seq < b.gc_seq;
              });
    for (size_t i = 0; i < heap.size(); i++) {
        entries[i] = heap[i];
    }
    *n_out = (uint32_t)heap.size();
    return 0;
}

int rondb_shim_gc_dequeue(void *handle, uint64_t gc_seq)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_GC_QUEUE);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "gc_dequeue startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_dequeue delOp");
    }
    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_GC_COL_SEQ, gc_seq);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 0; }
        return rondb_report_error(err, "gc_dequeue commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_gc_count(void *handle, uint32_t *count, uint32_t self_mds_id)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_owner = nullptr;
    NdbError err;
    int next_rc;
    uint32_t n = 0;

    if (state == nullptr || count == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_GC_QUEUE);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "gc_count startTx");
    }

    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_count getScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_count readTuples");
    }

    /* PK column drives the row count; owner column drives per-MDS filter. */
    (void)scan->getValue(RONDB_GC_COL_SEQ, nullptr);
    a_owner = scan->getValue(RONDB_GC_COL_OWNER_MDS, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_count exec");
    }

    while ((next_rc = scan->nextResult(true)) == 0) {
        uint32_t owner = (a_owner != nullptr) ? a_owner->u_32_value() : 0U;
        if (self_mds_id != 0U && owner != 0U && owner != self_mds_id) {
            continue;
        }
        n++;
    }
    if (next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "gc_count nextResult");
    }

    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    *count = n;
    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 8A -- Shared 2PC journal CRUD
 * ----------------------------------------------------------------------- */

int rondb_shim_journal_put(void *handle,
                           const struct mds_coord_journal_record *record)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    uint8_t src_name_value[MDS_MAX_NAME + 2];
    uint32_t src_name_value_len = 0;
    uint8_t dst_name_value[MDS_MAX_NAME + 2];
    uint32_t dst_name_value_len = 0;
    uint8_t payload_value[RONDB_RJ_PAYLOAD_MAX + 2];
    uint32_t payload_value_len = 0;

    if (state == nullptr || record == nullptr ||
        record->payload_len > sizeof(record->payload)) {
        return -1;
    }
    if (rondb_encode_varbinary_string(record->src_name, 1U,
                                      src_name_value,
                                      sizeof(src_name_value),
                                      &src_name_value_len) != 0) {
        return -1;
    }
    if (rondb_encode_varbinary_string(record->dst_name, 1U,
                                      dst_name_value,
                                      sizeof(dst_name_value),
                                      &dst_name_value_len) != 0) {
        return -1;
    }
    if (rondb_encode_varbinary_value(record->payload,
                                     record->payload_len,
                                     2U,
                                     payload_value,
                                     sizeof(payload_value),
                                     &payload_value_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_RENAME_JOURNAL);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                  "journal_put startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "journal_put writeOp");
    }

    op->writeTuple();
    (void)rondb_equal_u64(op, RONDB_RJ_COL_TXN_ID, record->txn_id);
    op->equal(RONDB_RJ_COL_ROLE, (Uint32)record->role);
    op->setValue(RONDB_RJ_COL_STATE, (Uint32)record->state);
    op->setValue(RONDB_RJ_COL_COORD_MDS,
                 (Uint32)record->remote_mds_id);
    (void)rondb_set_value_u64(op, RONDB_RJ_COL_SRC_PARENT,
                              record->src_parent_fileid);
    (void)rondb_set_value_u64(op, RONDB_RJ_COL_DST_PARENT,
                              record->dst_parent_fileid);
    (void)rondb_set_value_u64(op, RONDB_RJ_COL_SRC_CHILD,
                              record->src_child_fileid);
    op->setValue(RONDB_RJ_COL_SRC_NAME,
                 (const char *)src_name_value, src_name_value_len);
    op->setValue(RONDB_RJ_COL_DST_NAME,
                 (const char *)dst_name_value, dst_name_value_len);
    op->setValue(RONDB_RJ_COL_INODE_SNAP,
                 (const char *)payload_value, payload_value_len);
    (void)rondb_set_value_u64(op, RONDB_RJ_COL_CREATED_AT,
                              record->created_at_ns);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "journal_put commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_journal_get(void *handle, uint64_t txn_id, uint8_t role,
                           struct mds_coord_journal_record *record)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *a_state, *a_remote, *a_src_parent, *a_dst_parent;
    NdbRecAttr *a_src_child, *a_src_name, *a_dst_name, *a_payload;
    NdbRecAttr *a_created_at;
    NdbError err;

    if (state == nullptr || record == nullptr) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_RENAME_JOURNAL);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                  "journal_get startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "journal_get readOp");
    }

    op->readTuple(NdbOperation::LM_CommittedRead);
    (void)rondb_equal_u64(op, RONDB_RJ_COL_TXN_ID, txn_id);
    op->equal(RONDB_RJ_COL_ROLE, (Uint32)role);

    a_state = op->getValue(RONDB_RJ_COL_STATE, nullptr);
    a_remote = op->getValue(RONDB_RJ_COL_COORD_MDS, nullptr);
    a_src_parent = op->getValue(RONDB_RJ_COL_SRC_PARENT, nullptr);
    a_dst_parent = op->getValue(RONDB_RJ_COL_DST_PARENT, nullptr);
    a_src_child = op->getValue(RONDB_RJ_COL_SRC_CHILD, nullptr);
    a_src_name = op->getValue(RONDB_RJ_COL_SRC_NAME, nullptr);
    a_dst_name = op->getValue(RONDB_RJ_COL_DST_NAME, nullptr);
    a_payload = op->getValue(RONDB_RJ_COL_INODE_SNAP, nullptr);
    a_created_at = op->getValue(RONDB_RJ_COL_CREATED_AT, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626 ||
            err.classification == NdbError::NoDataFound) {
            return 1;
        }
        return rondb_report_error(err, "journal_get exec");
    }

    {
        NdbError op_err = op->getNdbError();
        if (op_err.code == 626 ||
            op_err.classification == NdbError::NoDataFound) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return 1;
        }
    }

    std::memset(record, 0, sizeof(*record));
    record->txn_id = txn_id;
    record->role = role;
    record->state = (uint8_t)a_state->u_32_value();
    record->remote_mds_id = a_remote->u_32_value();
    record->src_parent_fileid = a_src_parent->u_64_value();
    record->dst_parent_fileid = a_dst_parent->u_64_value();
    record->src_child_fileid = a_src_child->u_64_value();
    record->created_at_ns = a_created_at->u_64_value();

    if (rondb_decode_varbinary_string_attr(a_src_name,
                                           record->src_name,
                                           sizeof(record->src_name)) != 0 ||
        rondb_decode_varbinary_string_attr(a_dst_name,
                                           record->dst_name,
                                           sizeof(record->dst_name)) != 0 ||
        rondb_decode_lvb_value_attr(a_payload,
                                    record->payload,
                                    (uint32_t)sizeof(record->payload),
                                    &record->payload_len) != 0) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return -1;
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_journal_del(void *handle, uint64_t txn_id, uint8_t role)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_RENAME_JOURNAL);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                  "journal_del startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "journal_del delOp");
    }

    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_RJ_COL_TXN_ID, txn_id);
    op->equal(RONDB_RJ_COL_ROLE, (Uint32)role);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626 ||
            err.classification == NdbError::NoDataFound) {
            return 1;
        }
        return rondb_report_error(err, "journal_del commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_journal_scan(void *handle,
                            rondb_journal_scan_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_txn, *a_role, *a_state, *a_remote;
    NdbRecAttr *a_src_parent, *a_dst_parent, *a_src_child;
    NdbRecAttr *a_src_name, *a_dst_name, *a_payload, *a_created_at;
    NdbError err;
    int next_rc;

    if (state == nullptr || cb == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_RENAME_JOURNAL);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                  "journal_scan startTx");
    }

    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "journal_scan getScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "journal_scan readTuples");
    }

    a_txn = scan->getValue(RONDB_RJ_COL_TXN_ID, nullptr);
    a_role = scan->getValue(RONDB_RJ_COL_ROLE, nullptr);
    a_state = scan->getValue(RONDB_RJ_COL_STATE, nullptr);
    a_remote = scan->getValue(RONDB_RJ_COL_COORD_MDS, nullptr);
    a_src_parent = scan->getValue(RONDB_RJ_COL_SRC_PARENT, nullptr);
    a_dst_parent = scan->getValue(RONDB_RJ_COL_DST_PARENT, nullptr);
    a_src_child = scan->getValue(RONDB_RJ_COL_SRC_CHILD, nullptr);
    a_src_name = scan->getValue(RONDB_RJ_COL_SRC_NAME, nullptr);
    a_dst_name = scan->getValue(RONDB_RJ_COL_DST_NAME, nullptr);
    a_payload = scan->getValue(RONDB_RJ_COL_INODE_SNAP, nullptr);
    a_created_at = scan->getValue(RONDB_RJ_COL_CREATED_AT, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "journal_scan exec");
    }

    while ((next_rc = scan->nextResult(true)) == 0) {
        struct mds_coord_journal_record record;

        std::memset(&record, 0, sizeof(record));
        record.txn_id = a_txn->u_64_value();
        record.role = (uint8_t)a_role->u_32_value();
        record.state = (uint8_t)a_state->u_32_value();
        record.remote_mds_id = a_remote->u_32_value();
        record.src_parent_fileid = a_src_parent->u_64_value();
        record.dst_parent_fileid = a_dst_parent->u_64_value();
        record.src_child_fileid = a_src_child->u_64_value();
        record.created_at_ns = a_created_at->u_64_value();

        if (rondb_decode_varbinary_string_attr(a_src_name,
                                               record.src_name,
                                               sizeof(record.src_name)) != 0 ||
            rondb_decode_varbinary_string_attr(a_dst_name,
                                               record.dst_name,
                                               sizeof(record.dst_name)) != 0 ||
            rondb_decode_lvb_value_attr(
                a_payload, record.payload,
                (uint32_t)sizeof(record.payload),
                &record.payload_len) != 0) {
            scan->close();
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }

        if (cb(&record, ctx) != 0) {
            break;
        }
    }
    if (next_rc != 0 && next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "journal_scan nextResult");
    }

    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Layout state CRUD (schema v6)
 *   mds_layout_state   : PK=(fileid BIGUNSIGNED, stateid_other VARBINARY 12),
 *                        partition by fileid
 *   mds_layout_by_file : PK=(fileid, stateid_other) -- legacy index kept
 *   mds_ds_layout_idx  : PK=(ds_id, clientid, fileid), partition by fileid
 *
 * mds_layout_by_client was removed in v6; ix_layout_state_clientid
 * covers scan-by-clientid via an ordered index on the base table.
 * ----------------------------------------------------------------------- */

static int rondb_shim_layout_state_put_once(
    void *handle,
    const uint8_t sid_enc[14], uint32_t sid_enc_len,
    uint64_t clientid, uint64_t fileid,
    uint32_t iomode, uint64_t offset, uint64_t length, uint32_t seqid,
    const uint32_t *ds_ids, uint32_t ds_count)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *ls_tbl, *lbf_tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr) { return -1; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    ls_tbl  = dict->getTable(RONDB_TBL_LAYOUT_STATE);
    lbf_tbl = dict->getTable(RONDB_TBL_LAYOUT_BY_FILE);
    if (ls_tbl == nullptr || lbf_tbl == nullptr) { return -1; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(
            ls_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        err = rondb_get_ndb(state)->getNdbError();
        if (err.code == 266 || err.code == 274) { return err.code; }
        return rondb_report_error(err, "layout_put startTx");
    }

    op = tx->getNdbOperation(ls_tbl);
    if (op == nullptr) { goto layout_put_once_err; }
    op->writeTuple();
    (void)rondb_equal_u64(op, RONDB_LS_COL_FILEID, fileid);
    op->equal(RONDB_LS_COL_STATEID, (const char *)sid_enc, sid_enc_len);
    (void)rondb_set_value_u64(op, RONDB_LS_COL_CLIENTID, clientid);
    op->setValue(RONDB_LS_COL_IOMODE, iomode);
    (void)rondb_set_value_u64(op, RONDB_LS_COL_OFFSET, offset);
    (void)rondb_set_value_u64(op, RONDB_LS_COL_LENGTH, length);
    op->setValue(RONDB_LS_COL_SEQID, seqid);
    if (ls_tbl->getColumn(RONDB_LS_COL_GRANT_MDS) != nullptr) {
        op->setValue(RONDB_LS_COL_GRANT_MDS, (Uint32)0);
        (void)rondb_set_value_u64(op, RONDB_LS_COL_GRANT_EPOCH, (Uint64)0);
    }

    op = tx->getNdbOperation(lbf_tbl);
    if (op == nullptr) { goto layout_put_once_err; }
    op->writeTuple();
    (void)rondb_equal_u64(op, RONDB_LBF_COL_FILEID, fileid);
    op->equal(RONDB_LBF_COL_STATEID, (const char *)sid_enc, sid_enc_len);

    (void)ds_ids; (void)ds_count;

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 266 || err.code == 274) { return err.code; }
        return rondb_report_error(err, "layout_put commit");
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;

layout_put_once_err:
    err = tx->getNdbError();
    rondb_get_ndb(state)->closeTransaction(tx);
    if (err.code == 266 || err.code == 274) { return err.code; }
    return rondb_report_error(err, "layout_put op");
}

/* Best-effort post-commit write of ds_layout_idx rows.  Decoupled from
 * the layout_state / layout_by_file transaction to reduce NDB row-lock
 * contention on the critical path (2 ops vs 2+N).  Failure is logged
 * but not propagated -- missing rows are tolerated by layout_state_del
 * (626=NoDataFound is explicitly ignored there) and by DS failover
 * (which can rebuild the index from a scan). */
static void rondb_shim_ds_layout_idx_put_best_effort(
    void *handle,
    uint64_t clientid, uint64_t fileid,
    const uint32_t *ds_ids, uint32_t ds_count)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *dli_tbl;
    NdbTransaction *tx;
    NdbOperation *op;

    if (state == nullptr || ds_ids == nullptr || ds_count == 0) { return; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return; }
    dli_tbl = dict->getTable(RONDB_TBL_DS_LAYOUT_IDX);
    if (dli_tbl == nullptr) { return; }

    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(
            dli_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) { return; }

    for (uint32_t i = 0; i < ds_count; i++) {
        op = tx->getNdbOperation(dli_tbl);
        if (op == nullptr) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return;
        }
        op->writeTuple();
        op->equal(RONDB_DLI_COL_DS_ID, (Uint32)ds_ids[i]);
        (void)rondb_equal_u64(op, RONDB_DLI_COL_CLIENTID, clientid);
        (void)rondb_equal_u64(op, RONDB_DLI_COL_FILEID, fileid);
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        NdbError idx_err = tx->getNdbError();
        if (idx_err.code != 0) {
            (void)rondb_report_error(idx_err, "layout_put ds_idx best_effort");
        }
    }
    rondb_get_ndb(state)->closeTransaction(tx);
}

int rondb_shim_layout_state_put(void *handle,
                                const uint8_t stateid_other[12],
                                uint64_t clientid, uint64_t fileid,
                                uint32_t iomode, uint64_t offset,
                                uint64_t length, uint32_t seqid,
                                const uint32_t *ds_ids, uint32_t ds_count)
{
    uint8_t sid_enc[14];
    uint32_t sid_enc_len = 0;
    int rc;

    if (handle == nullptr || stateid_other == nullptr) { return -1; }
    if (rondb_encode_varbinary_value(stateid_other, 12, 1U,
                                     sid_enc, sizeof(sid_enc),
                                     &sid_enc_len) != 0) {
        return -1;
    }

    for (int attempt = 0; attempt < NDB_RETRY_MAX; attempt++) {
        rc = rondb_shim_layout_state_put_once(
                handle, sid_enc, sid_enc_len,
                clientid, fileid,
                iomode, offset, length, seqid,
                ds_ids, ds_count);
        if (rc == 0) {
            rondb_shim_ds_layout_idx_put_best_effort(
                handle, clientid, fileid, ds_ids, ds_count);
            return 0;
        }
        if (rc != 266 && rc != 274) { return rc; }
        {
            struct timespec _ts;
            _ts.tv_sec  = 0;
            _ts.tv_nsec = (long)(NDB_RETRY_DELAY_US * (attempt + 1)) * 1000L;
            nanosleep(&_ts, nullptr);
        }
    }
    {
        NdbError synth_err = {};
        synth_err.code = rc;
        synth_err.classification = (rc == 274)
            ? NdbError::TimeoutExpired
            : NdbError::TemporaryResourceError;
        synth_err.message = (rc == 266) ? "Lock wait timeout / deadlock"
                                        : "Time-out in NDB";
        (void)rondb_report_error(synth_err, "layout_put retry-exhausted");
    }
    return -1;
}


int rondb_shim_layout_state_del(void *handle,
                                const uint8_t stateid_other[12],
                                uint64_t clientid, uint64_t fileid,
                                const uint32_t *ds_ids, uint32_t ds_count)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *ls_tbl, *lbf_tbl, *dli_tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    uint8_t sid_enc[14];
    uint32_t sid_enc_len = 0;

    if (state == nullptr || stateid_other == nullptr) { return -1; }
    if (rondb_encode_varbinary_value(stateid_other, 12, 1U,
                                     sid_enc, sizeof(sid_enc),
                                     &sid_enc_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    ls_tbl  = dict->getTable(RONDB_TBL_LAYOUT_STATE);
    lbf_tbl = dict->getTable(RONDB_TBL_LAYOUT_BY_FILE);
    dli_tbl = dict->getTable(RONDB_TBL_DS_LAYOUT_IDX);
    if (ls_tbl == nullptr || lbf_tbl == nullptr ||
        dli_tbl == nullptr) {
        return -1;
    }

    /* Hint on fileid partition. */
    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(
            ls_tbl, (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "layout_del startTx");
    }

    /* 1. Delete layout_state row. */
    op = tx->getNdbOperation(ls_tbl);
    if (op == nullptr) { goto layout_del_err; }
    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_LS_COL_FILEID, fileid);
    op->equal(RONDB_LS_COL_STATEID,
              (const char *)sid_enc, sid_enc_len);

    /* 2. Delete layout_by_file index row. */
    op = tx->getNdbOperation(lbf_tbl);
    if (op == nullptr) { goto layout_del_err; }
    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_LBF_COL_FILEID, fileid);
    op->equal(RONDB_LBF_COL_STATEID,
              (const char *)sid_enc, sid_enc_len);

    /* 3. Delete ds_layout_idx rows. */
    (void)clientid;
    for (uint32_t i = 0; i < ds_count; i++) {
        op = tx->getNdbOperation(dli_tbl);
        if (op == nullptr) { goto layout_del_err; }
        op->deleteTuple();
        op->equal(RONDB_DLI_COL_DS_ID, (Uint32)ds_ids[i]);
        (void)rondb_equal_u64(op, RONDB_DLI_COL_CLIENTID, clientid);
        (void)rondb_equal_u64(op, RONDB_DLI_COL_FILEID, fileid);
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        /* Ignore 626 (some rows may not exist). */
        if (err.code != 626 &&
            err.classification != NdbError::NoDataFound) {
            return rondb_report_error(err, "layout_del commit");
        }
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;

layout_del_err:
    err = tx->getNdbError();
    rondb_get_ndb(state)->closeTransaction(tx);
    return rondb_report_error(err, "layout_del op");
}

/*
 * Schema v6: layout_state PK is (fileid, stateid_other).
 *
 * The stateid-only API (OP_TEST_STATEID, OP_LAYOUTCOMMIT) does not
 * carry fileid, so a single-row PK read is not possible.  The schema
 * defines an ordered index `ix_layout_state_stateid` on stateid_other
 * (rondb_schema.h); use a bounded index scan on that index to prune
 * to only the rows matching the requested stateid.  This is O(matches)
 * rather than O(total layout rows).  Falls back to a full table scan
 * with an NdbScanFilter when the index is absent (clusters without
 * ndb_index_stat system tables -- see createIndex code 4714 note).
 */
int rondb_shim_layout_get_by_stateid(void *handle,
                                     const uint8_t stateid_other[12],
                                     uint64_t *clientid, uint64_t *fileid,
                                     uint32_t *iomode, uint64_t *offset,
                                     uint64_t *length, uint32_t *seqid)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan = nullptr;
    NdbIndexScanOperation *iscan;
    NdbRecAttr *a_cid, *a_fid, *a_io, *a_off, *a_len, *a_seq, *a_sid;
    NdbError err;
    uint8_t sid_enc[14];
    uint32_t sid_enc_len = 0;
    int rc = 1; /* default: NOTFOUND */

    if (state == nullptr || stateid_other == nullptr) { return -1; }
    if (rondb_encode_varbinary_value(stateid_other, 12, 1U,
                                     sid_enc, sizeof(sid_enc),
                                     &sid_enc_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_LAYOUT_STATE);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "layout_get_sid startTx");
    }

    /* Fast path: bounded scan on ix_layout_state_stateid.
     * Falls back to a full table scan + filter when the index is absent. */
    iscan = tx->getNdbIndexScanOperation(
        RONDB_IX_LS_STATEID, RONDB_TBL_LAYOUT_STATE);
    if (iscan != nullptr) {
        if (iscan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
            err = iscan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_get_sid idx readTuples");
        }
        if (iscan->setBound(RONDB_LS_COL_STATEID,
                            NdbIndexScanOperation::BoundEQ,
                            sid_enc, sid_enc_len) != 0) {
            err = iscan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_get_sid setBound");
        }
        scan = iscan;
    } else {
        scan = tx->getNdbScanOperation(tbl);
        if (scan == nullptr) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_get_sid getScanOp");
        }
        if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
            err = scan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_get_sid readTuples");
        }
        {
            NdbScanFilter filter(scan);
            filter.begin(NdbScanFilter::AND);
            filter.cmp(NdbScanFilter::COND_EQ,
                       tbl->getColumn(RONDB_LS_COL_STATEID)->getColumnNo(),
                       sid_enc, (Uint32)sid_enc_len);
            filter.end();
        }
    }

    a_cid = scan->getValue(RONDB_LS_COL_CLIENTID, nullptr);
    a_fid = scan->getValue(RONDB_LS_COL_FILEID, nullptr);
    a_io  = scan->getValue(RONDB_LS_COL_IOMODE, nullptr);
    a_off = scan->getValue(RONDB_LS_COL_OFFSET, nullptr);
    a_len = scan->getValue(RONDB_LS_COL_LENGTH, nullptr);
    a_seq = scan->getValue(RONDB_LS_COL_SEQID, nullptr);
    a_sid = scan->getValue(RONDB_LS_COL_STATEID, nullptr);
    if (a_cid == nullptr || a_fid == nullptr || a_io == nullptr ||
        a_off == nullptr || a_len == nullptr || a_seq == nullptr ||
        a_sid == nullptr) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return -1;
    }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "layout_get_sid exec");
    }

    int scan_rc;
    while ((scan_rc = scan->nextResult(true)) == 0) {
        /* Defensive stateid equality recheck. */
        const char *ref = a_sid->aRef();
        if (ref == nullptr) { continue; }
        uint32_t row_len = (uint32_t)(uint8_t)ref[0];
        if (row_len != 12 || std::memcmp(ref + 1, stateid_other, 12) != 0) {
            continue;
        }
        if (clientid != nullptr) { *clientid = a_cid->u_64_value(); }
        if (fileid != nullptr)   { *fileid   = a_fid->u_64_value(); }
        if (iomode != nullptr)   { *iomode   = a_io->u_32_value(); }
        if (offset != nullptr)   { *offset   = a_off->u_64_value(); }
        if (length != nullptr)   { *length   = a_len->u_64_value(); }
        if (seqid != nullptr)    { *seqid    = a_seq->u_32_value(); }
        rc = 0;
        break;
    }
    if (scan_rc != 0 && scan_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "layout_get_sid nextResult");
    }
    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return rc;
}

int rondb_shim_layout_scan_for_file(void *handle, uint64_t fileid,
                                    int *has_layout)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *lbf_tbl;
    const NdbDictionary::Index *ix;
    NdbTransaction *tx;
    NdbIndexScanOperation *scan;
    NdbError err;
    int next_rc;

    if (state == nullptr || has_layout == nullptr) { return -1; }
    *has_layout = 0;

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    lbf_tbl = dict->getTable(RONDB_TBL_LAYOUT_BY_FILE);
    if (lbf_tbl == nullptr) { return -1; }

    /*
     * Partition-pruned index probe on ix_layout_by_file_fileid -- same
     * BoundEQ(fileid) path as rondb_shim_layout_iter_file Step 1, but
     * stop after the first tuple.  Replaces the prior NdbScanOperation
     * + NdbScanFilter pushdown, which visited every fragment and cost
     * ~230 ms p50 on the lab cluster when used as the unlink empty-
     * layout fast path.
     */
    ix = rondb_resolve_index(dict, RONDB_IX_LBF_FILEID,
                             RONDB_TBL_LAYOUT_BY_FILE);
    if (ix == nullptr) {
        return rondb_report_error(dict->getNdbError(),
                                 "layout_scan_file getIndex");
    }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "layout_scan_file startTx");
    }

    scan = tx->getNdbIndexScanOperation(ix);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "layout_scan_file getIndexScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "layout_scan_file readTuples");
    }

    if (scan->setBound(RONDB_LBF_COL_FILEID,
                       NdbIndexScanOperation::BoundEQ,
                       &fileid) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "layout_scan_file setBound");
    }

    (void)scan->getValue(RONDB_LBF_COL_FILEID, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "layout_scan_file exec");
    }

    next_rc = scan->nextResult(true);
    if (next_rc == 0) {
        *has_layout = 1;
    } else if (next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "layout_scan_file nextResult");
    }

    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/*
 * Schema v6: collect every (fileid, stateid_other) pair held by
 * clientid then delete each row.  Uses a bounded index scan on
 * ix_layout_state_clientid when the index is available, falling back
 * to a table scan + NdbScanFilter equality otherwise.
 */
int rondb_shim_layout_del_all_for_client(void *handle, uint64_t clientid)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *ls_tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan = nullptr;
    NdbIndexScanOperation *iscan;
    NdbRecAttr *a_cid, *a_fid, *a_sid;
    NdbError err;
    int next_rc;
    Uint64 bound_cid = (Uint64)clientid;
    struct hit {
        uint64_t fid;
        std::string sid;
    };
    std::vector<hit> hits;

    if (state == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    ls_tbl = dict->getTable(RONDB_TBL_LAYOUT_STATE);
    if (ls_tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "layout_del_client startTx");
    }

    /* Fast path: bounded index scan on ix_layout_state_clientid. */
    iscan = tx->getNdbIndexScanOperation(
        RONDB_IX_LS_CLIENTID, RONDB_TBL_LAYOUT_STATE);
    if (iscan != nullptr) {
        if (iscan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
            err = iscan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_del_client idx readTuples");
        }
        if (iscan->setBound(RONDB_LS_COL_CLIENTID,
                            NdbIndexScanOperation::BoundEQ,
                            &bound_cid) != 0) {
            err = iscan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_del_client setBound");
        }
        scan = iscan;
    } else {
        /* Fallback: full table scan + filter. */
        scan = tx->getNdbScanOperation(ls_tbl);
        if (scan == nullptr) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_del_client getScanOp");
        }
        if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
            err = scan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_del_client readTuples");
        }
        {
            NdbScanFilter filter(scan);
            filter.begin(NdbScanFilter::AND);
            filter.eq(ls_tbl->getColumn(RONDB_LS_COL_CLIENTID)->getColumnNo(),
                      (Uint64)clientid);
            filter.end();
        }
    }

    a_cid = scan->getValue(RONDB_LS_COL_CLIENTID, nullptr);
    a_fid = scan->getValue(RONDB_LS_COL_FILEID, nullptr);
    a_sid = scan->getValue(RONDB_LS_COL_STATEID, nullptr);
    if (a_cid == nullptr || a_fid == nullptr || a_sid == nullptr) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return -1;
    }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "layout_del_client exec");
    }

    while ((next_rc = scan->nextResult(true)) == 0) {
        /* Defensive equality recheck. */
        if (a_cid->u_64_value() != clientid) {
            continue;
        }
        uint64_t row_fid = a_fid->u_64_value();
        const char *ref = a_sid->aRef();
        if (ref == nullptr) {
            continue;
        }
        uint32_t vb_len = (uint32_t)(uint8_t)ref[0];
        if (vb_len != 12) {
            continue;
        }
        hit h;
        h.fid = row_fid;
        h.sid.assign(ref + 1, 12);
        hits.push_back(std::move(h));
    }
    if (next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "layout_del_client nextResult");
    }
    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);

    for (const hit &h : hits) {
        (void)rondb_shim_layout_state_del(
            handle, (const uint8_t *)h.sid.data(),
            clientid, h.fid, nullptr, 0);
    }

    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 8A+ -- ds_layout_idx scan by ds_id
 * ----------------------------------------------------------------------- */

int rondb_shim_ds_layout_idx_scan(void *handle, uint32_t ds_id,
                                  rondb_ds_layout_scan_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_cid, *a_fid;
    NdbError err;
    int next_rc;

    if (state == nullptr || cb == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DS_LAYOUT_IDX);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "dli_scan startTx");
    }

    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "dli_scan getScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "dli_scan readTuples");
    }

    {
        NdbScanFilter filter(scan);
        filter.begin(NdbScanFilter::AND);
        filter.eq(tbl->getColumn(RONDB_DLI_COL_DS_ID)->getColumnNo(),
                  (Uint32)ds_id);
        filter.end();
    }

    a_cid = scan->getValue(RONDB_DLI_COL_CLIENTID, nullptr);
    a_fid = scan->getValue(RONDB_DLI_COL_FILEID, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "dli_scan exec");
    }

    while ((next_rc = scan->nextResult(true)) == 0) {
        int cb_rc = cb(a_cid->u_64_value(), a_fid->u_64_value(), ctx);
        if (cb_rc != 0) {
            break;
        }
    }
    if (next_rc != 0 && next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "dli_scan nextResult");
    }

    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 8A+ -- layout_by_file iteration with layout_state join
 * ----------------------------------------------------------------------- */

int rondb_shim_layout_iter_file(void *handle, uint64_t fileid,
                                rondb_layout_file_iter_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *lbf_tbl;
    const NdbDictionary::Index *ix;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_sid;
    NdbError err;
    int next_rc;
    std::vector<std::string> stateids;

    if (state == nullptr || cb == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    lbf_tbl = dict->getTable(RONDB_TBL_LAYOUT_BY_FILE);
    if (lbf_tbl == nullptr) { return -1; }
    ix = rondb_resolve_index(dict, RONDB_IX_LBF_FILEID,
                             RONDB_TBL_LAYOUT_BY_FILE);
    if (ix == nullptr) {
        /* rondb_resolve_index calls dict->invalidateTable() on a 4243 miss,
         * making the earlier lbf_tbl pointer stale.  Re-fetch before using
         * it in startTransaction() to avoid a SIGSEGV inside libndbclient. */
        lbf_tbl = dict->getTable(RONDB_TBL_LAYOUT_BY_FILE);
        if (lbf_tbl == nullptr) { return -1; }
    }

    /* Partition-pruned startTransaction: fileid is the table partition key,
     * so this hint routes the NDB operation to the correct fragment even
     * when we fall back from the index scan to a full table scan. */
    {
        uint8_t pk_buf[8];
        fdb_put_u64(pk_buf, fileid);
        tx = rondb_get_ndb(state)->startTransaction(lbf_tbl,
                                                    (const char *)pk_buf, 8);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "layout_iter_file startTx");
    }

    if (ix != nullptr) {
        /*
         * Fast path: partition-pruned ordered-index scan of mds_layout_by_file
         * for every stateid_other recorded against `fileid`.  fileid is the
         * partition key and the leading PK column, so an NdbIndexScanOperation
         * with BoundEQ(fileid) reads only the matching rows from a single
         * fragment -- one NDB round-trip regardless of global cardinality.
         */
        NdbIndexScanOperation *ixscan = tx->getNdbIndexScanOperation(ix);
        if (ixscan == nullptr) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_iter_file getIndexScanOp");
        }
        if (ixscan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
            err = ixscan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_iter_file readTuples");
        }
        if (ixscan->setBound(RONDB_LBF_COL_FILEID,
                             NdbIndexScanOperation::BoundEQ,
                             &fileid) != 0) {
            err = ixscan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_iter_file setBound");
        }
        scan = ixscan;
    } else {
        /* Fallback: full table scan with pushed-down filter.
         *
         * ix_layout_by_file_fileid returned code=4243 (Index not found) on
         * this pool connection's per-connection NDB dictionary cache.  This
         * happens when the index was created on a different Ndb instance and
         * the cache on this connection has not yet been refreshed.
         *
         * The startTransaction() partition key hint above still prunes the
         * scan to the one NDB fragment that owns this fileid, so the scan
         * visits only rows for this file -- no global cardinality penalty.
         */
        static int _ix_warn = 0;
        if ((_ix_warn++ & 0x3F) == 0) {
            std::fprintf(stderr,
                "WARN: layout_iter_file: index %s unavailable (4243), "
                "falling back to partition-pruned table scan\n",
                RONDB_IX_LBF_FILEID);
        }
        NdbScanOperation *tscan = tx->getNdbScanOperation(lbf_tbl);
        if (tscan == nullptr) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_iter_file getScanOp");
        }
        if (tscan->readTuples(NdbOperation::LM_CommittedRead,
                              NdbScanOperation::SF_TupScan) != 0) {
            err = tscan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "layout_iter_file readTuples");
        }
        {
            NdbScanFilter filter(tscan);
            filter.begin(NdbScanFilter::AND);
            filter.eq(
                lbf_tbl->getColumn(RONDB_LBF_COL_FILEID)->getColumnNo(),
                (Uint64)fileid);
            filter.end();
        }
        scan = tscan;
    }

    a_sid = scan->getValue(RONDB_LBF_COL_STATEID, nullptr);
    if (a_sid == nullptr) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return -1;
    }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "layout_iter_file exec");
    }

    while ((next_rc = scan->nextResult(true)) == 0) {
        const char *ref = a_sid->aRef();
        if (ref != nullptr) {
            uint32_t vb_len = (uint32_t)(uint8_t)ref[0];
            if (vb_len == 12) {
                stateids.emplace_back(ref + 1, 12);
            }
        }
    }
    if (next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "layout_iter_file nextResult");
    }
    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);

    /* Step 2: for each stateid, read layout_state to get details,
     * then invoke the callback. */
    for (const std::string &sid : stateids) {
        uint64_t cid = 0;
        uint32_t iomode = 0, seqid = 0;
        int rc = rondb_shim_layout_get_by_stateid(
            handle, (const uint8_t *)sid.data(),
            &cid, nullptr, &iomode, nullptr, nullptr, &seqid);
        if (rc == 0) {
            int cb_rc = cb(cid, (const uint8_t *)sid.data(),
                          seqid, iomode, ctx);
            if (cb_rc != 0) {
                break;
            }
        }
        /* rc == 1 (NOTFOUND): stale index entry, skip. */
    }

    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 8A -- Client recovery CRUD (mds_client_recovery: PK=clientid)
 * ----------------------------------------------------------------------- */

int rondb_shim_recovery_put(void *handle, uint64_t clientid,
                            const uint8_t *co_ownerid,
                            uint32_t co_ownerid_len,
                            const uint8_t verifier[8])
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    uint8_t co_enc[1024 + 4];
    uint32_t co_enc_len = 0;

    if (state == nullptr || verifier == nullptr ||
        (co_ownerid_len > 0 && co_ownerid == nullptr) ||
        co_ownerid_len > 1024) {
        return -1;
    }
    if (rondb_encode_varbinary_value(co_ownerid, co_ownerid_len, 2U,
                                     co_enc, sizeof(co_enc),
                                     &co_enc_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_CLIENT_RECOVERY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "recovery_put startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "recovery_put writeOp");
    }
    op->writeTuple();
    (void)rondb_equal_u64(op, RONDB_CR_COL_CLIENTID, clientid);
    op->setValue(RONDB_CR_COL_CO_OWNERID,
                 (const char *)co_enc, co_enc_len);
    op->setValue(RONDB_CR_COL_VERIFIER,
                 (const char *)verifier, 8);
    /* Phase 9D columns: owner identity (default to 0 = not yet assigned). */
    op->setValue(RONDB_CR_COL_OWNER_MDS, (Uint32)0);
    (void)rondb_set_value_u64(op, RONDB_CR_COL_OWNER_EPOCH, (Uint64)0);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "recovery_put commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_recovery_del(void *handle, uint64_t clientid)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_CLIENT_RECOVERY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "recovery_del startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "recovery_del delOp");
    }
    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_CR_COL_CLIENTID, clientid);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 0; }
        return rondb_report_error(err, "recovery_del commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_recovery_get(void *handle, uint64_t clientid,
                            uint8_t *co_ownerid,
                            uint32_t *co_ownerid_len,
                            uint8_t verifier[8])
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *a_co, *a_ver;
    NdbError err;

    if (state == nullptr || co_ownerid_len == nullptr ||
        verifier == nullptr) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_CLIENT_RECOVERY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "recovery_get startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "recovery_get readOp");
    }
    op->readTuple(NdbOperation::LM_CommittedRead);
    (void)rondb_equal_u64(op, RONDB_CR_COL_CLIENTID, clientid);
    a_co  = op->getValue(RONDB_CR_COL_CO_OWNERID, nullptr);
    a_ver = op->getValue(RONDB_CR_COL_VERIFIER, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "recovery_get exec");
    }

    {
        NdbError op_err = op->getNdbError();
        if (op_err.code == 626 ||
            op_err.classification == NdbError::NoDataFound) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return 1;
        }
    }

    /* Decode co_ownerid (LONGVARBINARY: 2-byte LE prefix). */
    {
        const char *ref = a_co->aRef();
        uint32_t data_len = 0;

        if (ref != nullptr) {
            data_len = (uint32_t)(uint8_t)ref[0] |
                       ((uint32_t)(uint8_t)ref[1] << 8);
            if (co_ownerid != nullptr && data_len > 0) {
                uint32_t copy = data_len;
                if (copy > 1024) { copy = 1024; }
                std::memcpy(co_ownerid, ref + 2, copy);
            }
        }
        *co_ownerid_len = data_len;
    }

    /* Decode verifier (BINARY(8): no length prefix). */
    {
        const char *vref = a_ver->aRef();
        if (vref != nullptr) {
            std::memcpy(verifier, vref, 8);
        } else {
            std::memset(verifier, 0, 8);
        }
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 8A+ -- Recovery record scan (full table scan with owner filter)
 * ----------------------------------------------------------------------- */

int rondb_shim_recovery_scan(void *handle, uint32_t filter_mds_id,
                             rondb_recovery_scan_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_cid, *a_mds, *a_epoch;
    NdbError err;
    int next_rc;

    if (state == nullptr || cb == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_CLIENT_RECOVERY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "recovery_scan startTx");
    }

    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "recovery_scan getScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "recovery_scan readTuples");
    }

    /* Filter by owner_mds_id if non-zero. */
    if (filter_mds_id != 0) {
        NdbScanFilter filter(scan);
        filter.begin(NdbScanFilter::AND);
        filter.eq(tbl->getColumn(RONDB_CR_COL_OWNER_MDS)->getColumnNo(),
                  (Uint32)filter_mds_id);
        filter.end();
    }

    a_cid   = scan->getValue(RONDB_CR_COL_CLIENTID, nullptr);
    a_mds   = scan->getValue(RONDB_CR_COL_OWNER_MDS, nullptr);
    a_epoch = scan->getValue(RONDB_CR_COL_OWNER_EPOCH, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "recovery_scan exec");
    }

    while ((next_rc = scan->nextResult(true)) == 0) {
        int cb_rc = cb(a_cid->u_64_value(),
                       a_mds->u_32_value(),
                       a_epoch->u_64_value(), ctx);
        if (cb_rc != 0) {
            break;
        }
    }
    if (next_rc != 0 && next_rc != 1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "recovery_scan nextResult");
    }

    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 9A -- Node registry DDL + CRUD
 * ----------------------------------------------------------------------- */

static int rondb_define_node_registry_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_NODE_REGISTRY);
    rondb_add_unsigned(tbl, RONDB_NR_COL_MDS_ID);
    {
        NdbDictionary::Column &pk = *tbl.getColumn(RONDB_NR_COL_MDS_ID);
        pk.setPrimaryKey(true);
        pk.setPartitionKey(true);
        pk.setNullable(false);
    }
    rondb_add_bigunsigned(tbl, RONDB_NR_COL_BOOT_EPOCH, false, false);
    {
        NdbDictionary::Column col;
        col.setName(RONDB_NR_COL_HOSTNAME);
        col.setType(NdbDictionary::Column::Varchar);
        col.setLength(255);
        col.setNullable(false);
        tbl.addColumn(col);
    }
    rondb_add_unsigned(tbl, RONDB_NR_COL_NFS_PORT);
    rondb_add_unsigned(tbl, RONDB_NR_COL_GRPC_PORT);
    rondb_add_unsigned(tbl, RONDB_NR_COL_STATE);
    rondb_add_bigunsigned(tbl, RONDB_NR_COL_HEARTBEAT_NS, false, false);
    {
        NdbDictionary::Column col;
        col.setName(RONDB_NR_COL_SW_VERSION);
        col.setType(NdbDictionary::Column::Varchar);
        col.setLength(64);
        col.setNullable(true);
        tbl.addColumn(col);
    }
    return rondb_create_table_if_not_exists(dict, tbl);
}

int rondb_shim_mds_register(void *handle, uint32_t mds_id,
                            uint64_t boot_epoch,
                            const char *hostname,
                            uint16_t nfs_port, uint16_t grpc_port)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    struct timespec ts;
    uint64_t now_ns;
    uint8_t hostname_value[258];
    uint32_t hostname_value_len = 0;
    char ver_buf[16];
    uint8_t ver_value[66];
    uint32_t ver_value_len = 0;

    if (state == nullptr || hostname == nullptr) { return -1; }

    clock_gettime(CLOCK_MONOTONIC, &ts);
    now_ns = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;

    if (rondb_encode_varbinary_string(hostname, 1U,
                                      hostname_value, sizeof(hostname_value),
                                      &hostname_value_len) != 0) {
        return -1;
    }
    std::snprintf(ver_buf, sizeof(ver_buf), "%u.%u.%u",
                  (unsigned)PNFS_MDS_VERSION_MAJOR,
                  (unsigned)PNFS_MDS_VERSION_MINOR,
                  (unsigned)PNFS_MDS_VERSION_PATCH);
    if (rondb_encode_varbinary_string(ver_buf, 1U,
                                      ver_value, sizeof(ver_value),
                                      &ver_value_len) != 0) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_NODE_REGISTRY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "mds_register startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "mds_register getOp");
    }

    op->writeTuple();
    op->equal(RONDB_NR_COL_MDS_ID, (Uint32)mds_id);
    (void)rondb_set_value_u64(op, RONDB_NR_COL_BOOT_EPOCH, boot_epoch);
    op->setValue(RONDB_NR_COL_HOSTNAME,
                 (const char *)hostname_value, hostname_value_len);
    op->setValue(RONDB_NR_COL_NFS_PORT, (Uint32)nfs_port);
    op->setValue(RONDB_NR_COL_GRPC_PORT, (Uint32)grpc_port);
    op->setValue(RONDB_NR_COL_STATE, (Uint32)RONDB_NR_STATE_ACTIVE);
    (void)rondb_set_value_u64(op, RONDB_NR_COL_HEARTBEAT_NS, now_ns);
    op->setValue(RONDB_NR_COL_SW_VERSION,
                 (const char *)ver_value, ver_value_len);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "mds_register commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_mds_heartbeat(void *handle, uint32_t mds_id,
                             uint64_t boot_epoch)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    struct timespec ts;
    uint64_t now_ns;

    if (state == nullptr) { return -1; }

    clock_gettime(CLOCK_MONOTONIC, &ts);
    now_ns = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_NODE_REGISTRY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "mds_heartbeat startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "mds_heartbeat getOp");
    }

    op->updateTuple();
    op->equal(RONDB_NR_COL_MDS_ID, (Uint32)mds_id);
    (void)rondb_set_value_u64(op, RONDB_NR_COL_HEARTBEAT_NS, now_ns);
    (void)rondb_set_value_u64(op, RONDB_NR_COL_BOOT_EPOCH, boot_epoch);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; } /* row not found */
        return rondb_report_error(err, "mds_heartbeat commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_mds_deregister(void *handle, uint32_t mds_id)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_NODE_REGISTRY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "mds_deregister startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "mds_deregister getOp");
    }

    op->deleteTuple();
    op->equal(RONDB_NR_COL_MDS_ID, (Uint32)mds_id);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 0; } /* already gone */
        return rondb_report_error(err, "mds_deregister commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_mds_scan_stale(void *handle, uint64_t threshold_ns,
                              rondb_stale_node_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_mds, *a_epoch, *a_hb;
    NdbError err;

    if (state == nullptr || cb == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_NODE_REGISTRY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "scan_stale startTx");
    }

    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "scan_stale getScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "scan_stale readTuples");
    }

    a_mds   = scan->getValue(RONDB_NR_COL_MDS_ID, nullptr);
    a_epoch = scan->getValue(RONDB_NR_COL_BOOT_EPOCH, nullptr);
    a_hb    = scan->getValue(RONDB_NR_COL_HEARTBEAT_NS, nullptr);

    if (a_mds == nullptr || a_epoch == nullptr || a_hb == nullptr) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "scan_stale getValue");
    }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "scan_stale exec");
    }

    for (;;) {
        int rc = scan->nextResult(true);
        if (rc == 1) { break; }   /* no more rows */
        if (rc == -1) {
            err = scan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "scan_stale next");
        }

        uint64_t hb = a_hb->u_64_value();
        if (hb < threshold_ns) {
            int cbrc = cb(a_mds->u_32_value(),
                          a_epoch->u_64_value(),
                          hb, ctx);
            if (cbrc != 0) { break; }
        }
    }

    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 9A -- Range-based counter allocation (CAS pattern)
 *
 * Used for fileid_counter and gc_seq_counter to avoid hot-row
 * contention under multi-MDS concurrent writes.
 * ----------------------------------------------------------------------- */

static int rondb_shim_alloc_counter_range(void *handle,
                                          const char *key_name,
                                          uint32_t batch_size,
                                          uint64_t *range_start)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *val_attr;
    NdbError err;
    uint64_t old_val;
    char key_buf[64];

    if (state == nullptr || range_start == nullptr || batch_size == 0 ||
        key_name == nullptr) {
        return -1;
    }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_META);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "alloc_range startTx");
    }

    /* Read with exclusive lock. */
    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "alloc_range readOp");
    }
    op->readTuple(NdbOperation::LM_Exclusive);
    std::memset(key_buf, ' ', sizeof(key_buf));
    std::strncpy(key_buf, key_name, sizeof(key_buf) - 1);
    op->equal(RONDB_META_COL_KEY, key_buf);
    val_attr = op->getValue(RONDB_META_COL_VAL, nullptr);
    /* See fileid_batch_alloc: NULL NdbRecAttr must abort before use. */
    if (val_attr == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "alloc_range getValue");
    }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "alloc_range read exec");
    }

    old_val = val_attr->u_64_value();

    /* Update counter = old + batch_size. */
    {
        NdbOperation *upd = tx->getNdbOperation(tbl);
        if (upd == nullptr) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "alloc_range updOp");
        }
        upd->updateTuple();
        upd->equal(RONDB_META_COL_KEY, key_buf);
        (void)rondb_set_value_u64(upd, RONDB_META_COL_VAL,
                                  old_val + batch_size);
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "alloc_range commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    *range_start = old_val;
    return 0;
}

int rondb_shim_alloc_fileid_range(void *handle, uint32_t batch_size,
                                  uint64_t *range_start)
{
    return rondb_shim_alloc_counter_range(handle, RONDB_META_KEY_FILEID,
                                          batch_size, range_start);
}

int rondb_shim_alloc_gc_seq_range(void *handle, uint32_t batch_size,
                                  uint64_t *range_start)
{
    return rondb_shim_alloc_counter_range(handle, RONDB_META_KEY_GC_SEQ,
                                          batch_size, range_start);
}

/* -----------------------------------------------------------------------
 * Phase 9A -- Lock reaping for dead nodes
 *
 * Scan mds_ns_lock_holders for all rows matching (owner_mds_id,
 * owner_epoch).  For each holder found, delete the holder row and
 * decrement/delete the corresponding resource row.
 *
 * This is called by the stale-node scanner when a dead MDS is detected.
 * The implementation uses takeover scan for atomic scan-delete.
 * ----------------------------------------------------------------------- */

int rondb_shim_lock_reap_by_owner(void *handle,
                                  uint32_t owner_mds_id,
                                  uint64_t owner_boot_epoch,
                                  uint32_t *reaped_count)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *holder_tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_part, *a_class, *a_key, *a_mds, *a_epoch;
    NdbError err;
    uint32_t count = 0;

    if (state == nullptr) { return -1; }
    if (reaped_count != nullptr) { *reaped_count = 0; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    holder_tbl = dict->getTable(RONDB_TBL_NS_LOCK_HOLDERS);
    if (holder_tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "lock_reap startTx");
    }

    scan = tx->getNdbScanOperation(holder_tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_reap getScanOp");
    }

    /* Exclusive lock for takeover delete. */
    if (scan->readTuples(NdbOperation::LM_Exclusive) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_reap readTuples");
    }

    a_part  = scan->getValue(RONDB_LK_COL_PART_HINT, nullptr);
    a_class = scan->getValue(RONDB_LK_COL_RES_CLASS, nullptr);
    a_key   = scan->getValue(RONDB_LK_COL_RES_KEY, nullptr);
    a_mds   = scan->getValue(RONDB_LK_COL_OWNER_MDS, nullptr);
    a_epoch = scan->getValue(RONDB_LK_COL_OWNER_EPOCH, nullptr);

    if (a_part == nullptr || a_class == nullptr || a_key == nullptr ||
        a_mds == nullptr || a_epoch == nullptr) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_reap getValue");
    }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "lock_reap exec");
    }

    for (;;) {
        int rc = scan->nextResult(true);
        if (rc == 1) { break; }   /* no more rows */
        if (rc == -1) {
            err = scan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "lock_reap next");
        }

        uint32_t row_mds = a_mds->u_32_value();
        uint64_t row_epoch = a_epoch->u_64_value();

        if (row_mds != owner_mds_id || row_epoch != owner_boot_epoch) {
            continue;
        }

        /* Delete this holder via scan takeover. */
        if (scan->deleteCurrentTuple() != 0) {
            err = scan->getNdbError();
            scan->close();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "lock_reap deleteCurrent");
        }
        count++;

        /* Batch-execute the delete.  We'll also need to clean up
         * the resource row, but that requires a separate PK operation
         * which is done via rondb_shim_lock_release after the scan.
         * For now, the holder row delete ensures the lock holder is
         * removed; the resource row will become stale and expire via
         * TTL or be cleaned up by the next acquire attempt. */
    }

    scan->close();

    if (count > 0) {
        if (tx->execute(NdbTransaction::Commit) == -1) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "lock_reap commit");
        }
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    if (reaped_count != nullptr) { *reaped_count = count; }
    return 0;
}

/* -----------------------------------------------------------------------
 * Phase 9C -- Delta broadcast DDL + CRUD
 * ----------------------------------------------------------------------- */

static int rondb_define_delta_broadcast_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_DELTA_BROADCAST);

    /* PK part 1: source_mds_id (UNSIGNED). */
    rondb_add_unsigned(tbl, RONDB_DB_COL_SOURCE_MDS);
    {
        NdbDictionary::Column &pk1 = *tbl.getColumn(RONDB_DB_COL_SOURCE_MDS);
        pk1.setPrimaryKey(true);
        pk1.setPartitionKey(true);
        pk1.setNullable(false);
    }

    /* PK part 2: seqno (BIGUNSIGNED). */
    rondb_add_bigunsigned(tbl, RONDB_DB_COL_SEQNO, true, false);

    /* Non-PK columns. */
    rondb_add_bigunsigned(tbl, RONDB_DB_COL_BOOT_EPOCH, false, false);

    {
        NdbDictionary::Column col;
        col.setName(RONDB_DB_COL_DELTA_TYPE);
        col.setType(NdbDictionary::Column::Tinyunsigned);
        col.setNullable(false);
        tbl.addColumn(col);
    }

    /* payload: VARBINARY (2-byte LE prefix), max 13948.
     * NDB has a ~14000 byte inline row-size limit; 13948 leaves
     * room for PK + other fixed columns.  Delta payloads are
     * typically < 300 bytes (inode serialisation = 137 bytes). */
    {
        NdbDictionary::Column col;
        col.setName(RONDB_DB_COL_PAYLOAD);
        col.setType(NdbDictionary::Column::Longvarbinary);
        col.setLength(13948);
        col.setNullable(true);
        tbl.addColumn(col);
    }

    rondb_add_bigunsigned(tbl, RONDB_DB_COL_TIMESTAMP_NS, false, false);

    return rondb_create_table_if_not_exists(dict, tbl);
}

int rondb_shim_delta_insert(void *handle,
                            uint32_t source_mds_id, uint64_t seqno,
                            uint64_t boot_epoch, uint8_t delta_type,
                            const void *payload, uint32_t payload_len,
                            uint64_t timestamp_ns)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DELTA_BROADCAST);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "delta_insert startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "delta_insert getOp");
    }

    op->insertTuple();
    op->equal(RONDB_DB_COL_SOURCE_MDS, (Uint32)source_mds_id);
    (void)rondb_set_value_u64(op, RONDB_DB_COL_SEQNO, seqno);
    (void)rondb_set_value_u64(op, RONDB_DB_COL_BOOT_EPOCH, boot_epoch);
    op->setValue(RONDB_DB_COL_DELTA_TYPE, (Uint32)delta_type);

    /* Encode payload as LONGVARBINARY (2-byte LE length prefix). */
    if (payload != nullptr && payload_len > 0) {
        uint8_t *pb = (uint8_t *)std::malloc(2 + payload_len);
        if (pb == nullptr) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }
        pb[0] = (uint8_t)(payload_len & 0xFF);
        pb[1] = (uint8_t)((payload_len >> 8) & 0xFF);
        std::memcpy(pb + 2, payload, payload_len);
        op->setValue(RONDB_DB_COL_PAYLOAD, (const char *)pb,
                     2 + payload_len);
        std::free(pb);
    } else {
        op->setValue(RONDB_DB_COL_PAYLOAD, (const char *)nullptr);
    }

    (void)rondb_set_value_u64(op, RONDB_DB_COL_TIMESTAMP_NS, timestamp_ns);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        /* Duplicate key (constraint violation): a row with this
         * (source_mds_id, seqno) already exists -- typically a stale
         * post-crash seqno counter colliding with rows from the
         * previous incarnation.  Return the DISTINCT value 1 so the
         * caller can advance its seqno and retry; returning 0 here
         * would acknowledge a delta that was never written, silently
         * dropping it from the changefeed. */
        if (err.classification == NdbError::ConstraintViolation) {
            return 1;
        }
        return rondb_report_error(err, "delta_insert commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_delta_poll(void *handle,
                          uint32_t source_mds_id, uint64_t min_seqno,
                          uint32_t max_rows,
                          rondb_delta_poll_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbRecAttr *a_seqno = nullptr, *a_epoch = nullptr;
    NdbRecAttr *a_type = nullptr, *a_payload = nullptr, *a_ts = nullptr;
    NdbIndexScanOperation *scan = nullptr; /* unused placeholder */
    NdbError err;
    uint32_t fetched = 0;

    if (state == nullptr || cb == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DELTA_BROADCAST);
    if (tbl == nullptr) { return -1; }

    /* PK = (source_mds_id, seqno), partitioned by source_mds_id.
     * Start the transaction with a partition hint so all scan work
     * stays on the single fragment owning this MDS stream. */
    {
        uint8_t pk_buf[4];
        pk_buf[0] = (uint8_t)(source_mds_id >> 24);
        pk_buf[1] = (uint8_t)(source_mds_id >> 16);
        pk_buf[2] = (uint8_t)(source_mds_id >> 8);
        pk_buf[3] = (uint8_t)(source_mds_id);
        tx = rondb_get_ndb(state)->startTransaction(
            tbl, (const char *)pk_buf, 4);
    }
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "delta_poll startTx");
    }

    /* Fast path: PRIMARY ordered-index scan bounded by
     * source_mds_id == @source_mds_id AND seqno > @min_seqno.
     * Falls back to a full table scan + client-side filter when the
     * PRIMARY ordered index is absent. */
    NdbScanOperation *tscan = nullptr;
    {
        NdbIndexScanOperation *iscan = tx->getNdbIndexScanOperation(
            "PRIMARY", RONDB_TBL_DELTA_BROADCAST);
        if (iscan != nullptr) {
            if (iscan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
                err = iscan->getNdbError();
                rondb_get_ndb(state)->closeTransaction(tx);
                return rondb_report_error(err, "delta_poll idx readTuples");
            }
            /* BoundEQ on source_mds_id prunes to the partition;
             * BoundLT on seqno is actually the lower bound for
             * ascending order:  seqno > min_seqno  ==  LO > min. */
            Uint32 bound_src = (Uint32)source_mds_id;
            if (iscan->setBound(RONDB_DB_COL_SOURCE_MDS,
                                NdbIndexScanOperation::BoundEQ,
                                &bound_src) != 0) {
                err = iscan->getNdbError();
                rondb_get_ndb(state)->closeTransaction(tx);
                return rondb_report_error(err, "delta_poll setBound src");
            }
            Uint64 bound_seq = (Uint64)min_seqno;
            if (iscan->setBound(RONDB_DB_COL_SEQNO,
                                NdbIndexScanOperation::BoundLT,
                                &bound_seq) != 0) {
                err = iscan->getNdbError();
                rondb_get_ndb(state)->closeTransaction(tx);
                return rondb_report_error(err, "delta_poll setBound seq");
            }
            tscan = iscan;
        } else {
            /* Fallback: full table scan. */
            tscan = tx->getNdbScanOperation(tbl);
            if (tscan == nullptr) {
                err = tx->getNdbError();
                rondb_get_ndb(state)->closeTransaction(tx);
                return rondb_report_error(err, "delta_poll getScanOp");
            }
            if (tscan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
                err = tscan->getNdbError();
                rondb_get_ndb(state)->closeTransaction(tx);
                return rondb_report_error(err, "delta_poll readTuples");
            }
        }
    }

    NdbRecAttr *a_src = tscan->getValue(RONDB_DB_COL_SOURCE_MDS, nullptr);
    a_seqno   = tscan->getValue(RONDB_DB_COL_SEQNO, nullptr);
    a_epoch   = tscan->getValue(RONDB_DB_COL_BOOT_EPOCH, nullptr);
    a_type    = tscan->getValue(RONDB_DB_COL_DELTA_TYPE, nullptr);
    a_payload = tscan->getValue(RONDB_DB_COL_PAYLOAD, nullptr);
    a_ts      = tscan->getValue(RONDB_DB_COL_TIMESTAMP_NS, nullptr);

    if (a_src == nullptr || a_seqno == nullptr || a_epoch == nullptr ||
        a_type == nullptr || a_payload == nullptr || a_ts == nullptr) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return -1;
    }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "delta_poll exec");
    }

    for (;;) {
        int rc = tscan->nextResult(true);
        if (rc == 1) { break; }   /* no more rows */
        if (rc == -1) {
            err = tscan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "delta_poll next");
        }

        /* Defensive: recheck filters (for the table-scan fallback). */
        uint32_t row_src = a_src->u_32_value();
        if (row_src != source_mds_id) { continue; }

        uint64_t row_seqno = a_seqno->u_64_value();
        if (row_seqno <= min_seqno) { continue; }

        uint64_t row_epoch = a_epoch->u_64_value();
        uint8_t  row_type  = (uint8_t)a_type->u_32_value();
        uint64_t row_ts    = a_ts->u_64_value();

        /* Decode LONGVARBINARY payload (2-byte LE prefix). */
        const char *pref = a_payload->aRef();
        const void *pdata = nullptr;
        uint32_t plen = 0;
        if (pref != nullptr) {
            plen = (uint32_t)(uint8_t)pref[0] |
                   ((uint32_t)(uint8_t)pref[1] << 8);
            if (plen > 0) {
                pdata = pref + 2;
            }
        }

        if (cb(row_seqno, row_epoch, row_type, pdata, plen,
               row_ts, ctx) != 0) {
            break;
        }

        fetched++;
        if (max_rows > 0 && fetched >= max_rows) {
            break;
        }
    }

    tscan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_delta_trim(void *handle,
                          uint32_t source_mds_id, uint64_t max_seqno)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_seqno;
    NdbError err;

    if (state == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DELTA_BROADCAST);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "delta_trim startTx");
    }

    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "delta_trim getScanOp");
    }

    if (scan->readTuples(NdbOperation::LM_Exclusive) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "delta_trim readTuples");
    }

    NdbRecAttr *a_src = scan->getValue(RONDB_DB_COL_SOURCE_MDS, nullptr);
    a_seqno = scan->getValue(RONDB_DB_COL_SEQNO, nullptr);
    if (a_src == nullptr || a_seqno == nullptr) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return -1;
    }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "delta_trim exec");
    }

    uint32_t deleted = 0;
    bool done = false;
    while (!done) {
        /* Outer loop: fetch a batch of rows from the scan. */
        int rc = scan->nextResult(true); /* wait for batch */
        if (rc == 1) { break; }          /* no more rows */
        if (rc == -1) {
            err = scan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "delta_trim next");
        }

        /* Inner loop: process rows within the current batch. */
        do {
            uint32_t row_src = a_src->u_32_value();
            uint64_t row_seq = a_seqno->u_64_value();
            if (row_src == source_mds_id && row_seq <= max_seqno) {
                if (scan->deleteCurrentTuple() != 0) {
                    err = scan->getNdbError();
                    scan->close();
                    rondb_get_ndb(state)->closeTransaction(tx);
                    return rondb_report_error(err, "delta_trim delete");
                }
                deleted++;
            }
            rc = scan->nextResult(false); /* next row in same batch */
        } while (rc == 0);

        if (rc == -1) {
            err = scan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "delta_trim next2");
        }
        /* rc == 2: batch exhausted, execute deletes + fetch next batch. */
        if (deleted > 0) {
            if (tx->execute(NdbTransaction::NoCommit) == -1) {
                err = tx->getNdbError();
                rondb_get_ndb(state)->closeTransaction(tx);
                return rondb_report_error(err, "delta_trim batch exec");
            }
        }
        if (rc == 1) { done = true; }
    }

    scan->close();
    if (deleted > 0) {
        if (tx->execute(NdbTransaction::Commit) == -1) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "delta_trim commit");
        }
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_delta_seqno_load(void *handle, uint32_t mds_id,
                                uint64_t *seqno)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    char key_buf[64];

    if (state == nullptr || seqno == nullptr) { return -1; }

    /* Per-MDS delta seqno key: "delta_seq_<mds_id>". */
    std::snprintf(key_buf, sizeof(key_buf), "delta_seq_%u",
                  (unsigned)mds_id);

    uint64_t val = 0;
    int rc = rondb_read_meta_u64(state, key_buf, &val);
    if (rc != 0) {
        /* Key not found -- first boot for this MDS, start at 1. */
        *seqno = 1;
        return 0;
    }
    *seqno = val;
    return 0;
}

int rondb_shim_delta_seqno_save(void *handle, uint32_t mds_id,
                                uint64_t seqno)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    char meta_key[64];
    char key_buf[64];

    if (state == nullptr) { return -1; }

    std::snprintf(meta_key, sizeof(meta_key), "delta_seq_%u",
                  (unsigned)mds_id);

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_META);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "delta_seqno_save startTx");
    }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "delta_seqno_save getOp");
    }

    op->writeTuple();
    std::memset(key_buf, ' ', sizeof(key_buf));
    std::memcpy(key_buf, meta_key, std::strlen(meta_key));
    op->equal(RONDB_META_COL_KEY, key_buf);
    (void)rondb_set_value_u64(op, RONDB_META_COL_VAL, seqno);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "delta_seqno_save commit");
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Partition map CRUD (mds_partition_map)
 * ----------------------------------------------------------------------- */

int rondb_shim_partition_map_list(void *handle,
                                 rondb_partition_map_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_pid, *a_owner, *a_state, *a_path;
    NdbError err;

    if (state == nullptr || cb == nullptr) { return -1; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_PARTITION_MAP);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "pm_list startTx");
    }
    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "pm_list getScanOp");
    }
    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "pm_list readTuples");
    }
    a_pid   = scan->getValue(RONDB_PM_COL_PART_ID, nullptr);
    a_owner = scan->getValue(RONDB_PM_COL_OWNER_MDS, nullptr);
    a_state = scan->getValue(RONDB_PM_COL_STATE, nullptr);
    a_path  = scan->getValue(RONDB_PM_COL_SUBTREE, nullptr);
    if (a_pid == nullptr || a_owner == nullptr ||
        a_state == nullptr || a_path == nullptr) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return -1;
    }
    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "pm_list exec");
    }

    int rc;
    while ((rc = scan->nextResult(true)) == 0) {
        do {
            uint32_t pid   = a_pid->u_32_value();
            uint32_t owner = a_owner->u_32_value();
            uint8_t  st8   = (uint8_t)a_state->u_32_value();
            char path_buf[4096];
            const char *ref = a_path->aRef();
            uint32_t plen = 0;
            if (ref != nullptr) {
                plen = (uint32_t)(uint8_t)ref[0] |
                       ((uint32_t)(uint8_t)ref[1] << 8);
                if (plen >= sizeof(path_buf)) { plen = sizeof(path_buf) - 1; }
                std::memcpy(path_buf, ref + 2, plen);
            }
            path_buf[plen] = '\0';
            if (cb(pid, owner, st8, path_buf, ctx) != 0) {
                scan->close();
                rondb_get_ndb(state)->closeTransaction(tx);
                return 0;
            }
            rc = scan->nextResult(false);
        } while (rc == 0);
        if (rc == -1) {
            err = scan->getNdbError();
            scan->close();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "pm_list next");
        }
    }
    if (rc == -1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "pm_list next2");
    }
    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_partition_map_get(void *handle, uint32_t partition_id,
                                uint32_t *owner_mds_id, uint8_t *state_out,
                                char *subtree_path, uint32_t path_cap)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbRecAttr *a_owner, *a_state, *a_path;
    NdbError err;

    if (state == nullptr) { return -1; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_PARTITION_MAP);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "pm_get startTx");
    }
    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "pm_get getOp");
    }
    op->readTuple(NdbOperation::LM_CommittedRead);
    op->equal(RONDB_PM_COL_PART_ID, (Uint32)partition_id);
    a_owner = op->getValue(RONDB_PM_COL_OWNER_MDS, nullptr);
    a_state = op->getValue(RONDB_PM_COL_STATE, nullptr);
    a_path  = op->getValue(RONDB_PM_COL_SUBTREE, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; }
        return rondb_report_error(err, "pm_get exec");
    }
    if (owner_mds_id) { *owner_mds_id = a_owner->u_32_value(); }
    if (state_out)    { *state_out = (uint8_t)a_state->u_32_value(); }
    if (subtree_path && path_cap > 0) {
        const char *ref = a_path->aRef();
        uint32_t plen = 0;
        if (ref != nullptr) {
            plen = (uint32_t)(uint8_t)ref[0] |
                   ((uint32_t)(uint8_t)ref[1] << 8);
            if (plen >= path_cap) { plen = path_cap - 1; }
            std::memcpy(subtree_path, ref + 2, plen);
        }
        subtree_path[plen] = '\0';
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_partition_map_put(void *handle, uint32_t partition_id,
                                uint32_t owner_mds_id, uint8_t pm_state,
                                const char *subtree_path)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    uint8_t path_value[4100];
    uint32_t path_value_len = 0;

    if (state == nullptr || subtree_path == nullptr) { return -1; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_PARTITION_MAP);
    if (tbl == nullptr) { return -1; }

    if (rondb_encode_varbinary_string(subtree_path,
                                      (uint32_t)std::strlen(subtree_path) + 1,
                                      path_value, sizeof(path_value),
                                      &path_value_len) != 0) {
        return -1;
    }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "pm_put startTx");
    }
    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "pm_put getOp");
    }
    op->writeTuple();
    op->equal(RONDB_PM_COL_PART_ID, (Uint32)partition_id);
    op->setValue(RONDB_PM_COL_OWNER_MDS, (Uint32)owner_mds_id);
    op->setValue(RONDB_PM_COL_STATE, (Uint32)pm_state);
    op->setValue(RONDB_PM_COL_SUBTREE,
                 (const char *)path_value, path_value_len);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "pm_put commit");
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_partition_map_cas(void *handle, uint32_t partition_id,
                                uint32_t expected_owner,
                                uint32_t new_owner, uint8_t new_state)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *read_op, *write_op;
    NdbRecAttr *a_owner;
    NdbError err;

    if (state == nullptr) { return -1; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_PARTITION_MAP);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "pm_cas startTx");
    }
    /* Read with exclusive lock. */
    read_op = tx->getNdbOperation(tbl);
    if (read_op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "pm_cas readOp");
    }
    read_op->readTuple(NdbOperation::LM_Exclusive);
    read_op->equal(RONDB_PM_COL_PART_ID, (Uint32)partition_id);
    a_owner = read_op->getValue(RONDB_PM_COL_OWNER_MDS, nullptr);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.code == 626) { return 1; } /* NOTFOUND */
        return rondb_report_error(err, "pm_cas readExec");
    }
    if (a_owner->u_32_value() != expected_owner) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 2; /* CAS mismatch */
    }

    /* Update. */
    write_op = tx->getNdbOperation(tbl);
    if (write_op == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "pm_cas writeOp");
    }
    write_op->updateTuple();
    write_op->equal(RONDB_PM_COL_PART_ID, (Uint32)partition_id);
    write_op->setValue(RONDB_PM_COL_OWNER_MDS, (Uint32)new_owner);
    write_op->setValue(RONDB_PM_COL_STATE, (Uint32)new_state);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "pm_cas commit");
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Node registry scan (mds_node_registry)
 * ----------------------------------------------------------------------- */

int rondb_shim_mds_list(void *handle, rondb_mds_list_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_id, *a_epoch, *a_host, *a_nfs, *a_grpc, *a_hb;
    NdbError err;

    if (state == nullptr || cb == nullptr) { return -1; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_NODE_REGISTRY);
    if (tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                 "mds_list startTx");
    }
    scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "mds_list getScanOp");
    }
    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "mds_list readTuples");
    }
    a_id    = scan->getValue("mds_id", nullptr);
    a_epoch = scan->getValue("boot_epoch", nullptr);
    a_host  = scan->getValue("hostname", nullptr);
    a_nfs   = scan->getValue("nfs_port", nullptr);
    a_grpc  = scan->getValue("grpc_port", nullptr);
    a_hb    = scan->getValue("last_heartbeat_ns", nullptr);
    if (a_id == nullptr || a_epoch == nullptr || a_host == nullptr ||
        a_nfs == nullptr || a_grpc == nullptr || a_hb == nullptr) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return -1;
    }
    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "mds_list exec");
    }

    int rc;
    while ((rc = scan->nextResult(true)) == 0) {
        do {
            uint32_t mid  = a_id->u_32_value();
            uint64_t ep   = a_epoch->u_64_value();
            uint16_t nfsp = (uint16_t)a_nfs->u_32_value();
            uint16_t grpcp = (uint16_t)a_grpc->u_32_value();
            uint64_t hb   = a_hb->u_64_value();
            char host_buf[256];
            /* The hostname column is defined as NdbDictionary::
             * Column::Varchar(length=255) in
             * rondb_define_node_registry_table().  NDB stores
             * Varchar with a 1-byte length prefix when the declared
             * length is <= 255 and a 2-byte prefix otherwise.  The
             * writer (rondb_shim_mds_register()) consistently uses
             * rondb_encode_varbinary_string(..., 1U, ...) to match
             * the 1-byte prefix, but this decoder used to read two
             * prefix bytes: the first content byte was therefore
             * folded into the length computation, and every
             * hostname returned to fs_locations came back with its
             * leading character stripped off ("10.10.10.51" ->
             * "0.10.10.51").  The client tried to mount the
             * malformed address and timed out on the referral,
             * breaking every cross-MDS access. */
            uint32_t hlen = 0;
            const char *ref = a_host->aRef();
            if (ref != nullptr) {
                hlen = (uint32_t)(uint8_t)ref[0];
                if (hlen >= sizeof(host_buf)) { hlen = sizeof(host_buf) - 1; }
                std::memcpy(host_buf, ref + 1, hlen);
            }
            host_buf[hlen] = '\0';
            if (cb(mid, ep, host_buf, nfsp, grpcp, hb, ctx) != 0) {
                scan->close();
                rondb_get_ndb(state)->closeTransaction(tx);
                return 0;
            }
            rc = scan->nextResult(false);
        } while (rc == 0);
        if (rc == -1) {
            err = scan->getNdbError();
            scan->close();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "mds_list next");
        }
    }
    if (rc == -1) {
        err = scan->getNdbError();
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "mds_list next2");
    }
    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Stripe map full table scan (mds_stripe_maps + mds_stripe_entries)
 *
 * Two-scan approach for efficiency:
 *   Phase 1: Full table scan on mds_stripe_maps (headers).
 *   Phase 2: Full table scan on mds_stripe_entries (all entries).
 *   Phase 3: Client-side join by fileid, then call cb per file.
 *
 * This is O(H + E) NDB work (2 scans) instead of O(H * E) from
 * per-file child scans.  Memory is O(H + E) which is bounded by
 * the total number of stripe map rows in the database.
 * ----------------------------------------------------------------------- */

int rondb_shim_stripe_map_scan(void *handle,
                               rondb_stripe_map_scan_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *hdr_tbl, *ent_tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbError err;
    int next_rc;

    if (state == nullptr || cb == nullptr) { return -1; }

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    hdr_tbl = dict->getTable(RONDB_TBL_STRIPE_MAPS);
    ent_tbl = dict->getTable(RONDB_TBL_STRIPE_ENTRIES);
    if (hdr_tbl == nullptr || ent_tbl == nullptr) { return -1; }

    /* ---- Phase 1: scan all stripe map headers ---- */
    struct scan_header {
        uint64_t fileid;
        uint32_t stripe_count;
        uint32_t stripe_unit;
        uint32_t mirror_count;
    };
    std::vector<scan_header> headers;

    {
        NdbRecAttr *a_fid, *a_sc, *a_su, *a_mc;

        tx = rondb_get_ndb(state)->startTransaction();
        if (tx == nullptr) {
            return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                     "smscan hdr startTx");
        }
        scan = tx->getNdbScanOperation(hdr_tbl);
        if (scan == nullptr) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "smscan hdr getScanOp");
        }
        if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
            err = scan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "smscan hdr readTuples");
        }
        a_fid = scan->getValue(RONDB_SM_COL_FILEID, nullptr);
        a_sc  = scan->getValue(RONDB_SM_COL_STRIPE_CNT, nullptr);
        a_su  = scan->getValue(RONDB_SM_COL_STRIPE_UNIT, nullptr);
        a_mc  = scan->getValue(RONDB_SM_COL_MIRROR_CNT, nullptr);
        if (a_fid == nullptr || a_sc == nullptr ||
            a_su == nullptr || a_mc == nullptr) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }
        if (tx->execute(NdbTransaction::NoCommit) == -1) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "smscan hdr exec");
        }
        while ((next_rc = scan->nextResult(true)) == 0) {
            scan_header h;
            h.fileid       = a_fid->u_64_value();
            h.stripe_count = a_sc->u_32_value();
            h.stripe_unit  = a_su->u_32_value();
            h.mirror_count = a_mc->u_32_value();
            headers.push_back(h);
        }
        if (next_rc != 0 && next_rc != 1) {
            err = scan->getNdbError();
            scan->close();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "smscan hdr next");
        }
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
    }

    /* ---- Phase 2: scan all stripe entries ---- */
    struct flat_entry {
        uint64_t fileid;
        uint32_t ordinal;
        uint32_t ds_id;
        uint32_t nfs_fh_len;
        uint8_t  nfs_fh[MDS_NFS_FH_MAX];
    };
    std::vector<flat_entry> all_entries;

    {
        NdbRecAttr *e_fid, *e_ord, *e_dsid, *e_fhlen, *e_fh;

        tx = rondb_get_ndb(state)->startTransaction();
        if (tx == nullptr) {
            return rondb_report_error(rondb_get_ndb(state)->getNdbError(),
                                     "smscan ent startTx");
        }
        scan = tx->getNdbScanOperation(ent_tbl);
        if (scan == nullptr) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "smscan ent getScanOp");
        }
        if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
            err = scan->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "smscan ent readTuples");
        }
        e_fid   = scan->getValue(RONDB_SE_COL_FILEID, nullptr);
        e_ord   = scan->getValue(RONDB_SE_COL_ORDINAL, nullptr);
        e_dsid  = scan->getValue(RONDB_SE_COL_DS_ID, nullptr);
        e_fhlen = scan->getValue(RONDB_SE_COL_NFS_FH_LEN, nullptr);
        e_fh    = scan->getValue(RONDB_SE_COL_NFS_FH, nullptr);
        if (e_fid == nullptr || e_ord == nullptr || e_dsid == nullptr ||
            e_fhlen == nullptr || e_fh == nullptr) {
            rondb_get_ndb(state)->closeTransaction(tx);
            return -1;
        }
        if (tx->execute(NdbTransaction::NoCommit) == -1) {
            err = tx->getNdbError();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "smscan ent exec");
        }
        while ((next_rc = scan->nextResult(true)) == 0) {
            flat_entry fe;
            fe.fileid      = e_fid->u_64_value();
            fe.ordinal     = e_ord->u_32_value();
            fe.ds_id       = e_dsid->u_32_value();
            fe.nfs_fh_len  = e_fhlen->u_32_value();
            if (fe.nfs_fh_len > MDS_NFS_FH_MAX) {
                fe.nfs_fh_len = MDS_NFS_FH_MAX;
            }
            std::memset(fe.nfs_fh, 0, MDS_NFS_FH_MAX);
            const char *fh_ptr = e_fh->aRef();
            if (fh_ptr != nullptr && fe.nfs_fh_len > 0) {
                uint32_t vb_len = (uint32_t)(uint8_t)fh_ptr[0];
                if (vb_len > MDS_NFS_FH_MAX) {
                    vb_len = MDS_NFS_FH_MAX;
                }
                std::memcpy(fe.nfs_fh, fh_ptr + 1, vb_len);
            }
            all_entries.push_back(fe);
        }
        if (next_rc != 0 && next_rc != 1) {
            err = scan->getNdbError();
            scan->close();
            rondb_get_ndb(state)->closeTransaction(tx);
            return rondb_report_error(err, "smscan ent next");
        }
        scan->close();
        rondb_get_ndb(state)->closeTransaction(tx);
    }

    /* ---- Phase 3: client-side join and callback dispatch ---- */

    /* Sort entries by (fileid, ordinal) for grouped lookup. */
    std::sort(all_entries.begin(), all_entries.end(),
              [](const flat_entry &a, const flat_entry &b) {
                  if (a.fileid != b.fileid) return a.fileid < b.fileid;
                  return a.ordinal < b.ordinal;
              });

    /* Build a fileid -> index map for fast entry lookup.
     * Since entries are sorted by fileid, we only need the start
     * offset and count for each fileid. */
    struct entry_span { size_t start; size_t count; };
    std::unordered_map<uint64_t, entry_span> ent_map;
    ent_map.reserve(headers.size());
    {
        size_t i = 0;
        while (i < all_entries.size()) {
            uint64_t fid = all_entries[i].fileid;
            size_t begin = i;
            while (i < all_entries.size() && all_entries[i].fileid == fid) {
                i++;
            }
            ent_map[fid] = {begin, i - begin};
        }
    }

    /* Dispatch callbacks. */
    for (size_t hi = 0; hi < headers.size(); hi++) {
        const scan_header &hdr = headers[hi];
        uint32_t sc = hdr.stripe_count;

        if (sc == 0) {
            if (cb(hdr.fileid, 0, hdr.stripe_unit,
                   hdr.mirror_count, nullptr, ctx) != 0) {
                break;
            }
            continue;
        }

        /* Look up entries for this fileid. */
        auto it = ent_map.find(hdr.fileid);
        if (it == ent_map.end()) {
            /* Header with no entries -- skip. */
            continue;
        }

        const entry_span &span = it->second;
        uint32_t n_ents = (uint32_t)span.count;
        if (n_ents > sc) { n_ents = sc; }

        struct mds_ds_map_entry *entries =
            (struct mds_ds_map_entry *)std::calloc(
                n_ents > 0 ? n_ents : 1, sizeof(*entries));
        if (entries == nullptr) { continue; }

        for (uint32_t e = 0; e < n_ents; e++) {
            const flat_entry &fe = all_entries[span.start + e];
            entries[e].ds_id = fe.ds_id;
            entries[e].nfs_fh_len = fe.nfs_fh_len;
            std::memcpy(entries[e].nfs_fh, fe.nfs_fh, fe.nfs_fh_len);
        }

        int cb_rc = cb(hdr.fileid, sc, hdr.stripe_unit,
                       hdr.mirror_count, entries, ctx);
        std::free(entries);
        if (cb_rc != 0) { break; }
    }

    return 0;
}

/* -----------------------------------------------------------------------
 * Shared protocol state DDL (shared-attr)
 * ----------------------------------------------------------------------- */

/* -----------------------------------------------------------------------
 * Shared protocol state shim CRUD (shared-attr)
 *
 * Open state: fully implemented (put/get/del/scan).
 * Remaining entities: stubs returning -1 (Stage 2 WIP).
 * ----------------------------------------------------------------------- */

int rondb_shim_open_put(void *handle, const struct mds_coord_open_row *row)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl, *idx_f, *idx_c;
    NdbTransaction *tx;
    NdbOperation *op, *op_f, *op_c;
    NdbError err;

    if (state == nullptr || row == nullptr) { return -1; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl   = dict->getTable(RONDB_TBL_OPEN_STATE);
    idx_f = dict->getTable(RONDB_TBL_OPEN_BY_FILE);
    idx_c = dict->getTable(RONDB_TBL_OPEN_BY_CLIENT);
    if (tbl == nullptr || idx_f == nullptr || idx_c == nullptr) { return -1; }

    /* Encode stateid_other as 1-byte-prefixed varbinary. */
    uint8_t sid_vb[13];
    sid_vb[0] = 12;
    std::memcpy(sid_vb + 1, row->stateid_other, 12);

    /* Encode open_owner as 1-byte-prefixed varbinary. */
    uint8_t oo_vb[129];
    uint32_t oo_len = row->open_owner_len;
    if (oo_len > 128) { oo_len = 128; }
    oo_vb[0] = (uint8_t)oo_len;
    std::memcpy(oo_vb + 1, row->open_owner, oo_len);

    tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)sid_vb, 13);
    if (tx == nullptr) {
        return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "open_put startTx");
    }

    /* Main table: writeTuple (insert or update). */
    op = tx->getNdbOperation(tbl);
    if (op == nullptr) { err = tx->getNdbError(); goto fail; }
    op->writeTuple();
    op->equal(RONDB_OS_COL_STATEID, (const char *)sid_vb, 13);
    op->setValue(RONDB_OS_COL_SEQID, (Uint32)row->seqid);
    (void)rondb_set_value_u64(op, RONDB_OS_COL_CLIENTID, row->clientid);
    (void)rondb_set_value_u64(op, RONDB_OS_COL_FILEID, row->fileid);
    op->setValue(RONDB_OS_COL_SHARE_ACCESS, (Uint32)row->share_access);
    op->setValue(RONDB_OS_COL_SHARE_DENY, (Uint32)row->share_deny);
    op->setValue(RONDB_OS_COL_OPEN_OWNER, (const char *)oo_vb, 1 + oo_len);
    op->setValue(RONDB_OS_COL_OWNER_LEN, (Uint32)oo_len);
    op->setValue(RONDB_OS_COL_OWNER_MDS, (Uint32)row->owner_mds_id);
    (void)rondb_set_value_u64(op, RONDB_OS_COL_OWNER_EPOCH, row->owner_boot_epoch);

    /* Secondary index: open_by_file. */
    op_f = tx->getNdbOperation(idx_f);
    if (op_f == nullptr) { err = tx->getNdbError(); goto fail; }
    op_f->writeTuple();
    (void)rondb_equal_u64(op_f, RONDB_OS_COL_FILEID, row->fileid);
    op_f->equal(RONDB_OS_COL_STATEID, (const char *)sid_vb, 13);

    /* Secondary index: open_by_client. */
    op_c = tx->getNdbOperation(idx_c);
    if (op_c == nullptr) { err = tx->getNdbError(); goto fail; }
    op_c->writeTuple();
    (void)rondb_equal_u64(op_c, RONDB_OS_COL_CLIENTID, row->clientid);
    op_c->equal(RONDB_OS_COL_STATEID, (const char *)sid_vb, 13);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        goto fail;
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;

fail:
    rondb_get_ndb(state)->closeTransaction(tx);
    return rondb_report_error(err, "open_put");
}

int rondb_shim_open_get(void *handle, const uint8_t stateid_other[12],
                        struct mds_coord_open_row *row)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;
    NdbRecAttr *a_seq, *a_cid, *a_fid, *a_sa, *a_sd, *a_oo, *a_ol, *a_mds, *a_ep;

    if (state == nullptr || row == nullptr) { return -1; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_OPEN_STATE);
    if (tbl == nullptr) { return -1; }

    uint8_t sid_vb[13];
    sid_vb[0] = 12;
    std::memcpy(sid_vb + 1, stateid_other, 12);

    tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)sid_vb, 13);
    if (tx == nullptr) { return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "open_get startTx"); }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) { err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "open_get getOp"); }
    op->readTuple(NdbOperation::LM_CommittedRead);
    op->equal(RONDB_OS_COL_STATEID, (const char *)sid_vb, 13);
    a_seq = op->getValue(RONDB_OS_COL_SEQID, nullptr);
    a_cid = op->getValue(RONDB_OS_COL_CLIENTID, nullptr);
    a_fid = op->getValue(RONDB_OS_COL_FILEID, nullptr);
    a_sa  = op->getValue(RONDB_OS_COL_SHARE_ACCESS, nullptr);
    a_sd  = op->getValue(RONDB_OS_COL_SHARE_DENY, nullptr);
    a_oo  = op->getValue(RONDB_OS_COL_OPEN_OWNER, nullptr);
    a_ol  = op->getValue(RONDB_OS_COL_OWNER_LEN, nullptr);
    a_mds = op->getValue(RONDB_OS_COL_OWNER_MDS, nullptr);
    a_ep  = op->getValue(RONDB_OS_COL_OWNER_EPOCH, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::NoDataFound) { return 1; }
        return rondb_report_error(err, "open_get exec");
    }
    if (a_seq->isNULL() > 0) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 1; /* NOTFOUND */
    }

    std::memset(row, 0, sizeof(*row));
    std::memcpy(row->stateid_other, stateid_other, 12);
    row->seqid = a_seq->u_32_value();
    row->clientid = a_cid->u_64_value();
    row->fileid = a_fid->u_64_value();
    row->share_access = a_sa->u_32_value();
    row->share_deny = a_sd->u_32_value();
    row->open_owner_len = a_ol->u_32_value();
    if (row->open_owner_len > 128) { row->open_owner_len = 128; }
    { const char *oo_ptr = a_oo->aRef();
      if (oo_ptr != nullptr && row->open_owner_len > 0) {
          uint32_t vb_len = (uint32_t)(uint8_t)oo_ptr[0];
          if (vb_len > 128) { vb_len = 128; }
          std::memcpy(row->open_owner, oo_ptr + 1, vb_len);
      }
    }
    row->owner_mds_id = a_mds->u_32_value();
    row->owner_boot_epoch = a_ep->u_64_value();

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_open_del(void *handle, const uint8_t stateid_other[12])
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    NdbError err;

    if (state == nullptr) { return -1; }

    /* Read the row first to get fileid+clientid for index cleanup. */
    struct mds_coord_open_row row;
    int rc = rondb_shim_open_get(handle, stateid_other, &row);
    if (rc != 0) { return rc; } /* 1=NOTFOUND, -1=error */

    uint8_t sid_vb[13];
    sid_vb[0] = 12;
    std::memcpy(sid_vb + 1, stateid_other, 12);

    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_OPEN_STATE);
    const NdbDictionary::Table *idx_f = dict->getTable(RONDB_TBL_OPEN_BY_FILE);
    const NdbDictionary::Table *idx_c = dict->getTable(RONDB_TBL_OPEN_BY_CLIENT);
    if (tbl == nullptr || idx_f == nullptr || idx_c == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)sid_vb, 13);
    if (tx == nullptr) { return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "open_del startTx"); }

    /* Delete main row. */
    op = tx->getNdbOperation(tbl);
    if (op == nullptr) { err = tx->getNdbError(); goto fail; }
    op->deleteTuple();
    op->equal(RONDB_OS_COL_STATEID, (const char *)sid_vb, 13);

    /* Delete index rows. */
    { NdbOperation *df = tx->getNdbOperation(idx_f);
      if (df != nullptr) {
          df->deleteTuple();
          (void)rondb_equal_u64(df, RONDB_OS_COL_FILEID, row.fileid);
          df->equal(RONDB_OS_COL_STATEID, (const char *)sid_vb, 13);
      }
    }
    { NdbOperation *dc = tx->getNdbOperation(idx_c);
      if (dc != nullptr) {
          dc->deleteTuple();
          (void)rondb_equal_u64(dc, RONDB_OS_COL_CLIENTID, row.clientid);
          dc->equal(RONDB_OS_COL_STATEID, (const char *)sid_vb, 13);
      }
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError();
        goto fail;
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;

fail:
    rondb_get_ndb(state)->closeTransaction(tx);
    return rondb_report_error(err, "open_del");
}

int rondb_shim_open_scan_file(void *handle, uint64_t fileid,
                              rondb_open_scan_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *idx_tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbRecAttr *a_sid;
    NdbError err;

    if (state == nullptr || cb == nullptr) { return -1; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    idx_tbl = dict->getTable(RONDB_TBL_OPEN_BY_FILE);
    if (idx_tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) { return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "open_scan_file startTx"); }

    scan = tx->getNdbScanOperation(idx_tbl);
    if (scan == nullptr) { err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "open_scan_file getScan"); }
    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "open_scan_file readTuples");
    }

    NdbRecAttr *a_fid = scan->getValue(RONDB_OS_COL_FILEID, nullptr);
    a_sid = scan->getValue(RONDB_OS_COL_STATEID, nullptr);
    if (a_fid == nullptr || a_sid == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "open_scan_file exec");
    }

    int next_rc;
    while ((next_rc = scan->nextResult(true)) == 0) {
        if (a_fid->u_64_value() != fileid) { continue; }
        /* Read the full row from the main table. */
        const char *sid_ptr = a_sid->aRef();
        if (sid_ptr == nullptr) { continue; }
        uint8_t sid_raw[12];
        uint32_t vb_len = (uint32_t)(uint8_t)sid_ptr[0];
        if (vb_len != 12) { continue; }
        std::memcpy(sid_raw, sid_ptr + 1, 12);

        struct mds_coord_open_row row;
        if (rondb_shim_open_get(handle, sid_raw, &row) == 0) {
            if (cb(&row, ctx) != 0) { break; }
        }
    }
    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_open_scan_client(void *handle, uint64_t clientid,
                                rondb_open_scan_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *idx_tbl;
    NdbTransaction *tx;
    NdbScanOperation *scan;
    NdbError err;

    if (state == nullptr || cb == nullptr) { return -1; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    idx_tbl = dict->getTable(RONDB_TBL_OPEN_BY_CLIENT);
    if (idx_tbl == nullptr) { return -1; }

    tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) { return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "open_scan_client startTx"); }

    scan = tx->getNdbScanOperation(idx_tbl);
    if (scan == nullptr) { err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "open_scan_client getScan"); }
    if (scan->readTuples(NdbOperation::LM_CommittedRead) != 0) {
        err = scan->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "open_scan_client readTuples");
    }

    NdbRecAttr *a_cid = scan->getValue(RONDB_OS_COL_CLIENTID, nullptr);
    NdbRecAttr *a_sid = scan->getValue(RONDB_OS_COL_STATEID, nullptr);
    if (a_cid == nullptr || a_sid == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "open_scan_client exec");
    }

    int next_rc;
    while ((next_rc = scan->nextResult(true)) == 0) {
        if (a_cid->u_64_value() != clientid) { continue; }
        const char *sid_ptr = a_sid->aRef();
        if (sid_ptr == nullptr) { continue; }
        uint8_t sid_raw[12];
        uint32_t vb_len = (uint32_t)(uint8_t)sid_ptr[0];
        if (vb_len != 12) { continue; }
        std::memcpy(sid_raw, sid_ptr + 1, 12);

        struct mds_coord_open_row row;
        if (rondb_shim_open_get(handle, sid_raw, &row) == 0) {
            if (cb(&row, ctx) != 0) { break; }
        }
    }
    scan->close();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Atomic xattr + inode ctime/change touch (Stage 7)
 *
 * Writes the xattr then updates the inode's ctime and change
 * counter. Both operations use self-contained NDB transactions.
 * A single-transaction implementation is possible but requires
 * cross-table NDB operations within one T2 transaction -- deferred
 * to a future optimization pass.
 * ----------------------------------------------------------------------- */

/*
 * Touch inode ctime + change atomically at the data node.
 *
 * Called by xattr_put_atomic / xattr_del_atomic after the xattr
 * row write succeeds, to bump the owning inode's change counter
 * and ctime so clients see a coherent attribute update.
 *
 * Uses `interpretedUpdateTuple + incValue(change, 1) + setValue
 * (ctime_*)` in a single op, which:
 *   * executes entirely at the NDB data node (zero extra round
 *     trips, same cost as a plain update),
 *   * increments `change` atomically with no read-modify-write
 *     race between concurrent xattr mutators -- the only
 *     interleaving is NDB's own TC-level ordering, which is
 *     strictly serial per row,
 *   * holds the strict-monotonic property we advertise via
 *     FATTR4_CHANGE_ATTR_TYPE = MONOTONIC_INCR.
 *
 * Previous implementation did `readTuple(LM_Exclusive) -> compute
 * new_change = old + 1 -> updateTuple(setValue(CHANGE, new_change))`
 * in a single transaction.  That was serialised by NDB row lock so
 * it WAS monotonic in practice, but cost two round trips per xattr
 * touch.  The interpreted path is one round trip and makes the
 * correctness argument easier (no application-side arithmetic).
 *
 * Best-effort on every failure: xattr write already succeeded; we
 * return 0 so the caller reports success for the visible side
 * effect.  A missed change bump is a cache-coherency degradation,
 * not a data corruption.
 */
static int rondb_inode_touch_atomic(rondb_shim_handle *state,
                                    uint64_t fileid)
{
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx;
    NdbOperation *op;
    struct timespec now;

    if (state == nullptr) { return 0; }
    dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return 0; }
    tbl = dict->getTable(RONDB_TBL_INODES);
    if (tbl == nullptr) { return 0; }

    uint8_t pk[8];
    fdb_put_u64(pk, fileid);
    tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk, 8);
    if (tx == nullptr) { return 0; }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) {
        rondb_get_ndb(state)->closeTransaction(tx);
        return 0;
    }
    op->interpretedUpdateTuple();
    (void)rondb_equal_u64(op, RONDB_INO_COL_FILEID, fileid);
    op->incValue(RONDB_INO_COL_CHANGE, (Uint64)1);
    clock_gettime(CLOCK_REALTIME, &now);
    (void)rondb_set_value_u64(op, RONDB_INO_COL_CTIME_SEC,
                              (uint64_t)now.tv_sec);
    op->setValue(RONDB_INO_COL_CTIME_NSEC, (Uint32)now.tv_nsec);

    (void)tx->execute(NdbTransaction::Commit);
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_xattr_put_atomic(void *handle, uint64_t fileid,
                                const char *name,
                                const void *val, uint32_t vallen)
{
    int rc = rondb_shim_xattr_put(handle, fileid, name,
                                  (const uint8_t *)val, vallen);
    if (rc != 0) { return rc; }
    return rondb_inode_touch_atomic(
        rondb_checked_handle(handle, nullptr), fileid);
}

int rondb_shim_xattr_del_atomic(void *handle, uint64_t fileid,
                                const char *name)
{
    int rc = rondb_shim_xattr_del(handle, fileid, name);
    if (rc != 0) { return rc; }
    return rondb_inode_touch_atomic(
        rondb_checked_handle(handle, nullptr), fileid);
}

/* -----------------------------------------------------------------------
 * Client CRUD (PK = clientid u64)
 * ----------------------------------------------------------------------- */

int rondb_shim_client_put(void *handle, const struct mds_coord_client_row *row)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx; NdbOperation *op; NdbError err;
    if (state == nullptr || row == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_CLIENTS); if (tbl == nullptr) { return -1; }

    uint8_t pk[8]; fdb_put_u64(pk, row->clientid);
    tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk, 8);
    if (tx == nullptr) { return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "client_put startTx"); }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) { err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "client_put getOp"); }
    op->writeTuple();
    (void)rondb_equal_u64(op, RONDB_CL_COL_CLIENTID, row->clientid);

    /* co_ownerid as 2-byte-prefixed longvarbinary. */
    uint32_t co_len = row->co_ownerid_len > 1024 ? 1024 : row->co_ownerid_len;
    { uint8_t *vb = (uint8_t *)std::malloc(2 + co_len);
      if (vb == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
      vb[0] = (uint8_t)(co_len & 0xFF); vb[1] = (uint8_t)((co_len >> 8) & 0xFF);
      std::memcpy(vb + 2, row->co_ownerid, co_len);
      op->setValue(RONDB_CL_COL_CO_OWNERID, (const char *)vb, 2 + co_len);
      std::free(vb);
    }
    { uint8_t vb[9]; vb[0] = 8; std::memcpy(vb + 1, row->verifier, 8);
      op->setValue(RONDB_CL_COL_VERIFIER, (const char *)vb, 9); }
    op->setValue(RONDB_CL_COL_CONFIRMED, (Uint32)(row->confirmed ? 1 : 0));
    op->setValue(RONDB_CL_COL_OWNER_MDS, (Uint32)row->owner_mds_id);
    (void)rondb_set_value_u64(op, RONDB_CL_COL_OWNER_EPOCH, row->owner_boot_epoch);
    (void)rondb_set_value_u64(op, RONDB_CL_COL_LEASE_NS, row->lease_renewed_ns);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "client_put commit");
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_client_get(void *handle, uint64_t clientid,
                          struct mds_coord_client_row *row)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx; NdbOperation *op; NdbError err;
    if (state == nullptr || row == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_CLIENTS); if (tbl == nullptr) { return -1; }

    uint8_t pk[8]; fdb_put_u64(pk, clientid);
    tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk, 8);
    if (tx == nullptr) { return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "client_get startTx"); }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) { err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "client_get getOp"); }
    op->readTuple(NdbOperation::LM_CommittedRead);
    (void)rondb_equal_u64(op, RONDB_CL_COL_CLIENTID, clientid);
    NdbRecAttr *a_co = op->getValue(RONDB_CL_COL_CO_OWNERID, nullptr);
    NdbRecAttr *a_vr = op->getValue(RONDB_CL_COL_VERIFIER, nullptr);
    NdbRecAttr *a_cf = op->getValue(RONDB_CL_COL_CONFIRMED, nullptr);
    NdbRecAttr *a_mds = op->getValue(RONDB_CL_COL_OWNER_MDS, nullptr);
    NdbRecAttr *a_ep = op->getValue(RONDB_CL_COL_OWNER_EPOCH, nullptr);
    NdbRecAttr *a_ln = op->getValue(RONDB_CL_COL_LEASE_NS, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::NoDataFound) { return 1; }
        return rondb_report_error(err, "client_get exec");
    }
    if (a_cf->isNULL() > 0) { rondb_get_ndb(state)->closeTransaction(tx); return 1; }

    std::memset(row, 0, sizeof(*row));
    row->clientid = clientid;
    { const char *p = a_co->aRef();
      if (p != nullptr) {
          uint32_t vlen = (uint32_t)(uint8_t)p[0] | ((uint32_t)(uint8_t)p[1] << 8);
          if (vlen > 1024) vlen = 1024;
          std::memcpy(row->co_ownerid, p + 2, vlen);
          row->co_ownerid_len = vlen;
      }
    }
    { const char *p = a_vr->aRef();
      if (p != nullptr) { uint32_t vl = (uint32_t)(uint8_t)p[0]; if (vl > 8) vl = 8; std::memcpy(row->verifier, p + 1, vl); }
    }
    row->confirmed = (a_cf->u_32_value() != 0);
    row->owner_mds_id = a_mds->u_32_value();
    row->owner_boot_epoch = a_ep->u_64_value();
    row->lease_renewed_ns = a_ln->u_64_value();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_client_del(void *handle, uint64_t clientid)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx; NdbOperation *op; NdbError err;
    if (state == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_CLIENTS); if (tbl == nullptr) { return -1; }

    uint8_t pk[8]; fdb_put_u64(pk, clientid);
    tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk, 8);
    if (tx == nullptr) { return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "client_del startTx"); }
    op = tx->getNdbOperation(tbl);
    if (op == nullptr) { err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "client_del getOp"); }
    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_CL_COL_CLIENTID, clientid);
    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::NoDataFound) { return 1; }
        return rondb_report_error(err, "client_del commit");
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Delegation CRUD (PK = stateid_other 12B varbinary)
 * Same pattern as open-state with different table/column names.
 * ----------------------------------------------------------------------- */

int rondb_shim_deleg_put(void *handle, const struct mds_coord_deleg_row *row)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl, *idx_f, *idx_c;
    NdbTransaction *tx; NdbOperation *op; NdbError err;
    if (state == nullptr || row == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    tbl   = dict->getTable(RONDB_TBL_DELEGATIONS);
    idx_f = dict->getTable(RONDB_TBL_DELEG_BY_FILE);
    idx_c = dict->getTable(RONDB_TBL_DELEG_BY_CLIENT);
    if (tbl == nullptr || idx_f == nullptr || idx_c == nullptr) { return -1; }

    uint8_t sid_vb[13]; sid_vb[0] = 12; std::memcpy(sid_vb + 1, row->stateid_other, 12);
    tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)sid_vb, 13);
    if (tx == nullptr) { return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "deleg_put startTx"); }

    op = tx->getNdbOperation(tbl);
    if (op == nullptr) { err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "deleg_put"); }
    op->writeTuple();
    op->equal(RONDB_DG_COL_STATEID, (const char *)sid_vb, 13);
    op->setValue(RONDB_DG_COL_SEQID, (Uint32)row->seqid);
    (void)rondb_set_value_u64(op, RONDB_DG_COL_CLIENTID, row->clientid);
    (void)rondb_set_value_u64(op, RONDB_DG_COL_FILEID, row->fileid);
    op->setValue(RONDB_DG_COL_DELEG_TYPE, (Uint32)row->deleg_type);
    op->setValue(RONDB_DG_COL_OWNER_MDS, (Uint32)row->owner_mds_id);
    (void)rondb_set_value_u64(op, RONDB_DG_COL_OWNER_EPOCH, row->owner_boot_epoch);
    (void)rondb_set_value_u64(op, RONDB_DG_COL_GRANT_TIME, row->grant_time_ns);
    op->setValue(RONDB_DG_COL_RECALL_PEND, (Uint32)row->recall_pending);

    /* Secondary indexes */
    { NdbOperation *of = tx->getNdbOperation(idx_f);
      if (of) { of->writeTuple(); (void)rondb_equal_u64(of, RONDB_DG_COL_FILEID, row->fileid); of->equal(RONDB_DG_COL_STATEID, (const char *)sid_vb, 13); } }
    { NdbOperation *oc = tx->getNdbOperation(idx_c);
      if (oc) { oc->writeTuple(); (void)rondb_equal_u64(oc, RONDB_DG_COL_CLIENTID, row->clientid); oc->equal(RONDB_DG_COL_STATEID, (const char *)sid_vb, 13); } }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "deleg_put commit");
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_deleg_get(void *handle, const uint8_t stateid_other[12],
                         struct mds_coord_deleg_row *row)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    const NdbDictionary::Table *tbl;
    NdbTransaction *tx; NdbOperation *op; NdbError err;
    if (state == nullptr || row == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    tbl = dict->getTable(RONDB_TBL_DELEGATIONS); if (tbl == nullptr) { return -1; }

    uint8_t sid_vb[13]; sid_vb[0] = 12; std::memcpy(sid_vb + 1, stateid_other, 12);
    tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)sid_vb, 13);
    if (tx == nullptr) { return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "deleg_get startTx"); }
    op = tx->getNdbOperation(tbl);
    if (op == nullptr) { err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "deleg_get"); }
    op->readTuple(NdbOperation::LM_CommittedRead);
    op->equal(RONDB_DG_COL_STATEID, (const char *)sid_vb, 13);
    NdbRecAttr *a_seq = op->getValue(RONDB_DG_COL_SEQID, nullptr);
    NdbRecAttr *a_cid = op->getValue(RONDB_DG_COL_CLIENTID, nullptr);
    NdbRecAttr *a_fid = op->getValue(RONDB_DG_COL_FILEID, nullptr);
    NdbRecAttr *a_dt  = op->getValue(RONDB_DG_COL_DELEG_TYPE, nullptr);
    NdbRecAttr *a_mds = op->getValue(RONDB_DG_COL_OWNER_MDS, nullptr);
    NdbRecAttr *a_ep  = op->getValue(RONDB_DG_COL_OWNER_EPOCH, nullptr);
    NdbRecAttr *a_gt  = op->getValue(RONDB_DG_COL_GRANT_TIME, nullptr);
    NdbRecAttr *a_rp  = op->getValue(RONDB_DG_COL_RECALL_PEND, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::NoDataFound) { return 1; }
        return rondb_report_error(err, "deleg_get exec");
    }
    if (a_seq->isNULL() > 0) { rondb_get_ndb(state)->closeTransaction(tx); return 1; }

    std::memset(row, 0, sizeof(*row));
    std::memcpy(row->stateid_other, stateid_other, 12);
    row->seqid = a_seq->u_32_value();
    row->clientid = a_cid->u_64_value();
    row->fileid = a_fid->u_64_value();
    row->deleg_type = a_dt->u_32_value();
    row->owner_mds_id = a_mds->u_32_value();
    row->owner_boot_epoch = a_ep->u_64_value();
    row->grant_time_ns = a_gt->u_64_value();
    row->recall_pending = (uint8_t)a_rp->u_32_value();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_deleg_del(void *handle, const uint8_t stateid_other[12])
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    if (state == nullptr) { return -1; }
    struct mds_coord_deleg_row row;
    int rc = rondb_shim_deleg_get(handle, stateid_other, &row);
    if (rc != 0) { return rc; }

    NdbDictionary::Dictionary *dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *tbl   = dict->getTable(RONDB_TBL_DELEGATIONS);
    const NdbDictionary::Table *idx_f = dict->getTable(RONDB_TBL_DELEG_BY_FILE);
    const NdbDictionary::Table *idx_c = dict->getTable(RONDB_TBL_DELEG_BY_CLIENT);
    if (tbl == nullptr) { return -1; }

    uint8_t sid_vb[13]; sid_vb[0] = 12; std::memcpy(sid_vb + 1, stateid_other, 12);
    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)sid_vb, 13);
    if (tx == nullptr) { return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "deleg_del startTx"); }
    NdbError err;
    { NdbOperation *op = tx->getNdbOperation(tbl);
      if (op) { op->deleteTuple(); op->equal(RONDB_DG_COL_STATEID, (const char *)sid_vb, 13); } }
    if (idx_f) { NdbOperation *df = tx->getNdbOperation(idx_f);
      if (df) { df->deleteTuple(); (void)rondb_equal_u64(df, RONDB_DG_COL_FILEID, row.fileid); df->equal(RONDB_DG_COL_STATEID, (const char *)sid_vb, 13); } }
    if (idx_c) { NdbOperation *dc = tx->getNdbOperation(idx_c);
      if (dc) { dc->deleteTuple(); (void)rondb_equal_u64(dc, RONDB_DG_COL_CLIENTID, row.clientid); dc->equal(RONDB_DG_COL_STATEID, (const char *)sid_vb, 13); } }
    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); if (err.code == 626) { return 0; } return rondb_report_error(err, "deleg_del commit");
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_deleg_scan_file(void *handle, uint64_t fileid,
                               rondb_deleg_scan_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict; NdbError err;
    if (state == nullptr || cb == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *idx = dict->getTable(RONDB_TBL_DELEG_BY_FILE);
    if (idx == nullptr) { return -1; }
    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) { return -1; }
    NdbScanOperation *scan = tx->getNdbScanOperation(idx);
    if (scan == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    scan->readTuples(NdbOperation::LM_CommittedRead);
    NdbRecAttr *a_fid = scan->getValue(RONDB_DG_COL_FILEID, nullptr);
    NdbRecAttr *a_sid = scan->getValue(RONDB_DG_COL_STATEID, nullptr);
    if (!a_fid || !a_sid) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    if (tx->execute(NdbTransaction::NoCommit) == -1) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    int nr;
    while ((nr = scan->nextResult(true)) == 0) {
        if (a_fid->u_64_value() != fileid) continue;
        const char *sp = a_sid->aRef(); if (!sp) continue;
        uint8_t sr[12]; if ((uint8_t)sp[0] != 12) continue;
        std::memcpy(sr, sp + 1, 12);
        struct mds_coord_deleg_row r;
        if (rondb_shim_deleg_get(handle, sr, &r) == 0) { if (cb(&r, ctx) != 0) break; }
    }
    scan->close(); rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_deleg_scan_client(void *handle, uint64_t clientid,
                                 rondb_deleg_scan_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    if (state == nullptr || cb == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *idx = dict->getTable(RONDB_TBL_DELEG_BY_CLIENT);
    if (idx == nullptr) { return -1; }
    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) { return -1; }
    NdbScanOperation *scan = tx->getNdbScanOperation(idx);
    if (scan == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    scan->readTuples(NdbOperation::LM_CommittedRead);
    NdbRecAttr *a_cid = scan->getValue(RONDB_DG_COL_CLIENTID, nullptr);
    NdbRecAttr *a_sid = scan->getValue(RONDB_DG_COL_STATEID, nullptr);
    if (!a_cid || !a_sid) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    if (tx->execute(NdbTransaction::NoCommit) == -1) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    int nr;
    while ((nr = scan->nextResult(true)) == 0) {
        if (a_cid->u_64_value() != clientid) continue;
        const char *sp = a_sid->aRef(); if (!sp) continue;
        uint8_t sr[12]; if ((uint8_t)sp[0] != 12) continue;
        std::memcpy(sr, sp + 1, 12);
        struct mds_coord_deleg_row r;
        if (rondb_shim_deleg_get(handle, sr, &r) == 0) { if (cb(&r, ctx) != 0) break; }
    }
    scan->close(); rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Session CRUD (PK = session_id 16B varbinary)
 * ----------------------------------------------------------------------- */

int rondb_shim_session_put(void *handle, const struct mds_coord_session_row *row)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    if (state == nullptr || row == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *tbl = dict->getTable(RONDB_TBL_SESSIONS);
    const NdbDictionary::Table *idx_c = dict->getTable(RONDB_TBL_SESSION_BY_CLIENT);
    if (tbl == nullptr || idx_c == nullptr) { return -1; }

    uint8_t sid_vb[17]; sid_vb[0] = 16; std::memcpy(sid_vb + 1, row->session_id, 16);
    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)sid_vb, 17);
    if (tx == nullptr) { return rondb_report_error(rondb_get_ndb(state)->getNdbError(), "session_put startTx"); }
    NdbError err;
    NdbOperation *op = tx->getNdbOperation(tbl);
    if (op == nullptr) { err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "session_put"); }
    op->writeTuple();
    op->equal(RONDB_SS_COL_SESSION_ID, (const char *)sid_vb, 17);
    (void)rondb_set_value_u64(op, RONDB_SS_COL_CLIENTID, row->clientid);
    op->setValue(RONDB_SS_COL_NUM_SLOTS, (Uint32)row->num_slots);
    op->setValue(RONDB_SS_COL_CB_PROG, (Uint32)row->cb_prog);
    op->setValue(RONDB_SS_COL_CB_SEC, (Uint32)row->cb_sec_flavor);
    op->setValue(RONDB_SS_COL_OWNER_MDS, (Uint32)row->owner_mds_id);
    (void)rondb_set_value_u64(op, RONDB_SS_COL_OWNER_EPOCH, row->owner_boot_epoch);
    (void)rondb_set_value_u64(op, RONDB_SS_COL_CREATED_NS, row->created_ns);

    { NdbOperation *oc = tx->getNdbOperation(idx_c);
      if (oc) { oc->writeTuple(); (void)rondb_equal_u64(oc, RONDB_SS_COL_CLIENTID, row->clientid); oc->equal(RONDB_SS_COL_SESSION_ID, (const char *)sid_vb, 17); } }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "session_put commit");
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_session_get(void *handle, const uint8_t session_id[16],
                           struct mds_coord_session_row *row)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    if (state == nullptr || row == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *tbl = dict->getTable(RONDB_TBL_SESSIONS);
    if (tbl == nullptr) { return -1; }

    uint8_t sid_vb[17]; sid_vb[0] = 16; std::memcpy(sid_vb + 1, session_id, 16);
    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)sid_vb, 17);
    if (tx == nullptr) { return -1; }
    NdbError err;
    NdbOperation *op = tx->getNdbOperation(tbl);
    if (op == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    op->readTuple(NdbOperation::LM_CommittedRead);
    op->equal(RONDB_SS_COL_SESSION_ID, (const char *)sid_vb, 17);
    NdbRecAttr *a_cid = op->getValue(RONDB_SS_COL_CLIENTID, nullptr);
    NdbRecAttr *a_ns  = op->getValue(RONDB_SS_COL_NUM_SLOTS, nullptr);
    NdbRecAttr *a_cp  = op->getValue(RONDB_SS_COL_CB_PROG, nullptr);
    NdbRecAttr *a_cs  = op->getValue(RONDB_SS_COL_CB_SEC, nullptr);
    NdbRecAttr *a_mds = op->getValue(RONDB_SS_COL_OWNER_MDS, nullptr);
    NdbRecAttr *a_ep  = op->getValue(RONDB_SS_COL_OWNER_EPOCH, nullptr);
    NdbRecAttr *a_cr  = op->getValue(RONDB_SS_COL_CREATED_NS, nullptr);

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::NoDataFound) { return 1; }
        return -1;
    }
    if (a_ns->isNULL() > 0) { rondb_get_ndb(state)->closeTransaction(tx); return 1; }
    std::memset(row, 0, sizeof(*row));
    std::memcpy(row->session_id, session_id, 16);
    row->clientid = a_cid->u_64_value();
    row->num_slots = a_ns->u_32_value();
    row->cb_prog = a_cp->u_32_value();
    row->cb_sec_flavor = a_cs->u_32_value();
    row->owner_mds_id = a_mds->u_32_value();
    row->owner_boot_epoch = a_ep->u_64_value();
    row->created_ns = a_cr->u_64_value();
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_session_del(void *handle, const uint8_t session_id[16])
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    if (state == nullptr) { return -1; }
    struct mds_coord_session_row row;
    int rc = rondb_shim_session_get(handle, session_id, &row);
    if (rc != 0) { return rc; }

    NdbDictionary::Dictionary *dict = rondb_get_dictionary(state);
    if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *tbl = dict->getTable(RONDB_TBL_SESSIONS);
    const NdbDictionary::Table *idx_c = dict->getTable(RONDB_TBL_SESSION_BY_CLIENT);
    if (tbl == nullptr) { return -1; }

    uint8_t sid_vb[17]; sid_vb[0] = 16; std::memcpy(sid_vb + 1, session_id, 16);
    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)sid_vb, 17);
    if (tx == nullptr) { return -1; }
    NdbError err;
    { NdbOperation *op = tx->getNdbOperation(tbl);
      if (op) { op->deleteTuple(); op->equal(RONDB_SS_COL_SESSION_ID, (const char *)sid_vb, 17); } }
    if (idx_c) { NdbOperation *dc = tx->getNdbOperation(idx_c);
      if (dc) { dc->deleteTuple(); (void)rondb_equal_u64(dc, RONDB_SS_COL_CLIENTID, row.clientid); dc->equal(RONDB_SS_COL_SESSION_ID, (const char *)sid_vb, 17); } }
    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "session_del commit");
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_session_scan_client(void *handle, uint64_t clientid,
                                   rondb_session_scan_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    if (state == nullptr || cb == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *idx = dict->getTable(RONDB_TBL_SESSION_BY_CLIENT);
    if (idx == nullptr) { return -1; }
    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) { return -1; }
    NdbScanOperation *scan = tx->getNdbScanOperation(idx);
    if (scan == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    scan->readTuples(NdbOperation::LM_CommittedRead);
    NdbRecAttr *a_cid = scan->getValue(RONDB_SS_COL_CLIENTID, nullptr);
    NdbRecAttr *a_sid = scan->getValue(RONDB_SS_COL_SESSION_ID, nullptr);
    if (!a_cid || !a_sid) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    if (tx->execute(NdbTransaction::NoCommit) == -1) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    int nr;
    while ((nr = scan->nextResult(true)) == 0) {
        if (a_cid->u_64_value() != clientid) continue;
        const char *sp = a_sid->aRef(); if (!sp) continue;
        uint8_t sr[16]; if ((uint8_t)sp[0] != 16) continue;
        std::memcpy(sr, sp + 1, 16);
        struct mds_coord_session_row r;
        if (rondb_shim_session_get(handle, sr, &r) == 0) { if (cb(&r, ctx) != 0) break; }
    }
    scan->close(); rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Byte-range lock CRUD (PK = fileid + lock_id, both u64)
 * ----------------------------------------------------------------------- */

int rondb_shim_bytelock_put(void *handle, const struct mds_coord_lock_row *row)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    if (state == nullptr || row == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *tbl = dict->getTable(RONDB_TBL_BYTE_LOCKS);
    const NdbDictionary::Table *idx = dict->getTable(RONDB_TBL_LOCK_BY_OWNER);
    if (tbl == nullptr || idx == nullptr) { return -1; }

    uint8_t pk[8]; fdb_put_u64(pk, row->fileid);
    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk, 8);
    if (tx == nullptr) { return -1; }
    NdbError err;
    NdbOperation *op = tx->getNdbOperation(tbl);
    if (op == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    op->writeTuple();
    (void)rondb_equal_u64(op, RONDB_BL_COL_FILEID, row->fileid);
    (void)rondb_equal_u64(op, RONDB_BL_COL_LOCK_ID, row->lock_id);
    (void)rondb_set_value_u64(op, RONDB_BL_COL_OFFSET, row->offset);
    (void)rondb_set_value_u64(op, RONDB_BL_COL_LENGTH, row->length);
    op->setValue(RONDB_BL_COL_LOCK_TYPE, (Uint32)row->lock_type);
    (void)rondb_set_value_u64(op, RONDB_BL_COL_CLIENTID, row->clientid);
    { uint8_t ov[129]; uint32_t ol = row->owner_len > 128 ? 128 : row->owner_len;
      ov[0] = (uint8_t)ol; std::memcpy(ov + 1, row->owner, ol);
      op->setValue(RONDB_BL_COL_OWNER, (const char *)ov, 1 + ol); }
    op->setValue(RONDB_BL_COL_OWNER_LEN, (Uint32)row->owner_len);
    { uint8_t sv[13]; sv[0] = 12; std::memcpy(sv + 1, row->stateid_other, 12);
      op->setValue(RONDB_BL_COL_STATEID, (const char *)sv, 13); }
    op->setValue(RONDB_BL_COL_SEQID, (Uint32)row->seqid);
    { uint8_t osv[13]; osv[0] = 12; std::memcpy(osv + 1, row->open_stateid_other, 12);
      op->setValue(RONDB_BL_COL_OPEN_STATEID, (const char *)osv, 13); }
    op->setValue(RONDB_BL_COL_OWNER_MDS, (Uint32)row->owner_mds_id);
    (void)rondb_set_value_u64(op, RONDB_BL_COL_OWNER_EPOCH, row->owner_boot_epoch);

    { NdbOperation *oi = tx->getNdbOperation(idx);
      if (oi) { oi->writeTuple(); (void)rondb_equal_u64(oi, RONDB_BL_COL_CLIENTID, row->clientid);
                (void)rondb_equal_u64(oi, RONDB_BL_COL_FILEID, row->fileid);
                (void)rondb_equal_u64(oi, RONDB_BL_COL_LOCK_ID, row->lock_id); } }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx); return rondb_report_error(err, "bytelock_put");
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_bytelock_del(void *handle, uint64_t fileid, uint64_t lock_id)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    if (state == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *tbl = dict->getTable(RONDB_TBL_BYTE_LOCKS);
    if (tbl == nullptr) { return -1; }
    uint8_t pk[8]; fdb_put_u64(pk, fileid);
    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)pk, 8);
    if (tx == nullptr) { return -1; }
    NdbOperation *op = tx->getNdbOperation(tbl);
    if (op == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    op->deleteTuple();
    (void)rondb_equal_u64(op, RONDB_BL_COL_FILEID, fileid);
    (void)rondb_equal_u64(op, RONDB_BL_COL_LOCK_ID, lock_id);
    NdbError err;
    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::NoDataFound) { return 1; }
        return rondb_report_error(err, "bytelock_del");
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_bytelock_scan_file(void *handle, uint64_t fileid,
                                  rondb_lock_scan_cb cb, void *ctx)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    if (state == nullptr || cb == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *tbl = dict->getTable(RONDB_TBL_BYTE_LOCKS);
    if (tbl == nullptr) { return -1; }
    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) { return -1; }
    NdbScanOperation *scan = tx->getNdbScanOperation(tbl);
    if (scan == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    scan->readTuples(NdbOperation::LM_CommittedRead);

    NdbRecAttr *a_fid = scan->getValue(RONDB_BL_COL_FILEID, nullptr);
    NdbRecAttr *a_lid = scan->getValue(RONDB_BL_COL_LOCK_ID, nullptr);
    NdbRecAttr *a_off = scan->getValue(RONDB_BL_COL_OFFSET, nullptr);
    NdbRecAttr *a_len = scan->getValue(RONDB_BL_COL_LENGTH, nullptr);
    NdbRecAttr *a_lt  = scan->getValue(RONDB_BL_COL_LOCK_TYPE, nullptr);
    NdbRecAttr *a_cid = scan->getValue(RONDB_BL_COL_CLIENTID, nullptr);
    NdbRecAttr *a_own = scan->getValue(RONDB_BL_COL_OWNER, nullptr);
    NdbRecAttr *a_ol  = scan->getValue(RONDB_BL_COL_OWNER_LEN, nullptr);
    NdbRecAttr *a_sid = scan->getValue(RONDB_BL_COL_STATEID, nullptr);
    NdbRecAttr *a_seq = scan->getValue(RONDB_BL_COL_SEQID, nullptr);
    NdbRecAttr *a_mds = scan->getValue(RONDB_BL_COL_OWNER_MDS, nullptr);
    NdbRecAttr *a_ep  = scan->getValue(RONDB_BL_COL_OWNER_EPOCH, nullptr);
    if (!a_fid || !a_lid || !a_off || !a_len || !a_lt || !a_cid ||
        !a_own || !a_ol || !a_sid || !a_seq || !a_mds || !a_ep) {
        rondb_get_ndb(state)->closeTransaction(tx); return -1;
    }
    if (tx->execute(NdbTransaction::NoCommit) == -1) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    int nr;
    while ((nr = scan->nextResult(true)) == 0) {
        if (a_fid->u_64_value() != fileid) continue;
        struct mds_coord_lock_row r;
        std::memset(&r, 0, sizeof(r));
        r.fileid = a_fid->u_64_value();
        r.lock_id = a_lid->u_64_value();
        r.offset = a_off->u_64_value();
        r.length = a_len->u_64_value();
        r.lock_type = a_lt->u_32_value();
        r.clientid = a_cid->u_64_value();
        r.owner_len = a_ol->u_32_value();
        if (r.owner_len > 128) r.owner_len = 128;
        { const char *p = a_own->aRef();
          if (p) { uint32_t vl = (uint32_t)(uint8_t)p[0]; if (vl > 128) vl = 128; std::memcpy(r.owner, p + 1, vl); } }
        { const char *p = a_sid->aRef();
          if (p && (uint8_t)p[0] == 12) { std::memcpy(r.stateid_other, p + 1, 12); } }
        r.seqid = a_seq->u_32_value();
        r.owner_mds_id = a_mds->u_32_value();
        r.owner_boot_epoch = a_ep->u_64_value();
        if (cb(&r, ctx) != 0) break;
    }
    scan->close(); rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_bytelock_reap_client(void *handle, uint64_t clientid)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    if (state == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *idx = dict->getTable(RONDB_TBL_LOCK_BY_OWNER);
    if (idx == nullptr) { return -1; }

    /* Scan lock_by_owner for this clientid, collect (fileid, lock_id), then delete. */
    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction();
    if (tx == nullptr) { return -1; }
    NdbScanOperation *scan = tx->getNdbScanOperation(idx);
    if (scan == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    scan->readTuples(NdbOperation::LM_CommittedRead);
    NdbRecAttr *a_cid = scan->getValue(RONDB_BL_COL_CLIENTID, nullptr);
    NdbRecAttr *a_fid = scan->getValue(RONDB_BL_COL_FILEID, nullptr);
    NdbRecAttr *a_lid = scan->getValue(RONDB_BL_COL_LOCK_ID, nullptr);
    if (!a_cid || !a_fid || !a_lid) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    if (tx->execute(NdbTransaction::NoCommit) == -1) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }

    struct reap_entry { uint64_t fileid; uint64_t lock_id; };
    std::vector<reap_entry> to_del;
    int nr;
    while ((nr = scan->nextResult(true)) == 0) {
        if (a_cid->u_64_value() != clientid) continue;
        to_del.push_back({a_fid->u_64_value(), a_lid->u_64_value()});
    }
    scan->close(); rondb_get_ndb(state)->closeTransaction(tx);

    for (auto &e : to_del) {
        rondb_shim_bytelock_del(handle, e.fileid, e.lock_id);
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * DRC slot CRUD (PK = session_id + slot_id)
 * cached_reply uses NDB Blob API for large payloads.
 * ----------------------------------------------------------------------- */

int rondb_shim_drc_slot_put(void *handle, const uint8_t session_id[16],
                            uint32_t slot_id, uint32_t seq_id,
                            const void *cached_reply, uint32_t reply_len)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    if (state == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *tbl = dict->getTable(RONDB_TBL_DRC_SLOTS);
    if (tbl == nullptr) { return -1; }

    uint8_t sid_vb[17]; sid_vb[0] = 16;
    if (session_id) { std::memcpy(sid_vb + 1, session_id, 16); }
    else { std::memset(sid_vb + 1, 0, 16); }

    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)sid_vb, 17);
    if (tx == nullptr) { return -1; }
    NdbError err;
    NdbOperation *op = tx->getNdbOperation(tbl);
    if (op == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    op->writeTuple();
    op->equal(RONDB_DR_COL_SESSION_ID, (const char *)sid_vb, 17);
    op->equal(RONDB_DR_COL_SLOT_ID, (Uint32)slot_id);
    op->setValue(RONDB_DR_COL_SEQ_ID, (Uint32)seq_id);
    op->setValue(RONDB_DR_COL_REPLY_LEN, (Uint32)reply_len);

    struct timespec now; clock_gettime(CLOCK_REALTIME, &now);
    uint64_t now_ns = (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
    (void)rondb_set_value_u64(op, RONDB_DR_COL_LAST_USED_NS, now_ns);

    /* Write blob. */
    NdbBlob *blob = op->getBlobHandle(RONDB_DR_COL_REPLY);
    if (blob == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    if (cached_reply && reply_len > 0) {
        if (blob->setValue(cached_reply, reply_len) != 0) {
            rondb_get_ndb(state)->closeTransaction(tx); return -1;
        }
    } else {
        blob->setNull();
    }

    if (tx->execute(NdbTransaction::Commit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx);
        return rondb_report_error(err, "drc_slot_put commit");
    }
    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

int rondb_shim_drc_slot_get(void *handle, const uint8_t session_id[16],
                            uint32_t slot_id,
                            struct mds_coord_drc_slot_row *row)
{
    rondb_shim_handle *state = rondb_checked_handle(handle, nullptr);
    NdbDictionary::Dictionary *dict;
    if (state == nullptr || row == nullptr) { return -1; }
    dict = rondb_get_dictionary(state); if (dict == nullptr) { return -1; }
    const NdbDictionary::Table *tbl = dict->getTable(RONDB_TBL_DRC_SLOTS);
    if (tbl == nullptr) { return -1; }

    uint8_t sid_vb[17]; sid_vb[0] = 16;
    if (session_id) { std::memcpy(sid_vb + 1, session_id, 16); }
    else { std::memset(sid_vb + 1, 0, 16); }

    NdbTransaction *tx = rondb_get_ndb(state)->startTransaction(tbl, (const char *)sid_vb, 17);
    if (tx == nullptr) { return -1; }
    NdbError err;
    NdbOperation *op = tx->getNdbOperation(tbl);
    if (op == nullptr) { rondb_get_ndb(state)->closeTransaction(tx); return -1; }
    op->readTuple(NdbOperation::LM_CommittedRead);
    op->equal(RONDB_DR_COL_SESSION_ID, (const char *)sid_vb, 17);
    op->equal(RONDB_DR_COL_SLOT_ID, (Uint32)slot_id);
    NdbRecAttr *a_seq = op->getValue(RONDB_DR_COL_SEQ_ID, nullptr);
    NdbRecAttr *a_rl  = op->getValue(RONDB_DR_COL_REPLY_LEN, nullptr);
    NdbRecAttr *a_lu  = op->getValue(RONDB_DR_COL_LAST_USED_NS, nullptr);
    NdbBlob *blob = op->getBlobHandle(RONDB_DR_COL_REPLY);

    if (tx->execute(NdbTransaction::NoCommit) == -1) {
        err = tx->getNdbError(); rondb_get_ndb(state)->closeTransaction(tx);
        if (err.classification == NdbError::NoDataFound) { return 1; }
        return -1;
    }
    if (a_seq->isNULL() > 0) { rondb_get_ndb(state)->closeTransaction(tx); return 1; }

    std::memset(row, 0, sizeof(*row));
    if (session_id) { std::memcpy(row->session_id, session_id, 16); }
    row->slot_id = slot_id;
    row->seq_id = a_seq->u_32_value();
    row->reply_len = a_rl->u_32_value();
    row->last_used_ns = a_lu->u_64_value();

    /* Read blob data. */
    row->cached_reply = nullptr;
    if (blob != nullptr && row->reply_len > 0) {
        int is_null = 0;
        blob->getNull(is_null);
        if (!is_null) {
            Uint64 blen = 0;
            blob->getLength(blen);
            if (blen > 0 && blen <= 65536) {
                row->cached_reply = (uint8_t *)std::malloc((size_t)blen);
                if (row->cached_reply) {
                    Uint32 readlen = (Uint32)blen;
                    blob->readData(row->cached_reply, readlen);
                    if (tx->execute(NdbTransaction::NoCommit) == -1) {
                        std::free(row->cached_reply);
                        row->cached_reply = nullptr;
                        row->reply_len = 0;
                    }
                }
            }
        }
    }

    rondb_get_ndb(state)->closeTransaction(tx);
    return 0;
}

static int rondb_define_open_state_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_OPEN_STATE);
    { NdbDictionary::Column c; c.setName(RONDB_OS_COL_STATEID);
      c.setType(NdbDictionary::Column::Varbinary); c.setLength(12);
      c.setPrimaryKey(true); c.setPartitionKey(true); c.setNullable(false);
      tbl.addColumn(c); }
    rondb_add_unsigned(tbl, RONDB_OS_COL_SEQID);
    rondb_add_bigunsigned(tbl, RONDB_OS_COL_CLIENTID, false, false);
    rondb_add_bigunsigned(tbl, RONDB_OS_COL_FILEID, false, false);
    rondb_add_unsigned(tbl, RONDB_OS_COL_SHARE_ACCESS);
    rondb_add_unsigned(tbl, RONDB_OS_COL_SHARE_DENY);
    rondb_add_varbinary(tbl, RONDB_OS_COL_OPEN_OWNER, 128, false, false);
    rondb_add_unsigned(tbl, RONDB_OS_COL_OWNER_LEN);
    rondb_add_unsigned(tbl, RONDB_OS_COL_OWNER_MDS);
    rondb_add_bigunsigned(tbl, RONDB_OS_COL_OWNER_EPOCH, false, false);
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_open_by_file_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_OPEN_BY_FILE);
    { NdbDictionary::Column c; c.setName(RONDB_OS_COL_FILEID);
      c.setType(NdbDictionary::Column::Bigunsigned);
      c.setPrimaryKey(true); c.setPartitionKey(true); c.setNullable(false);
      tbl.addColumn(c); }
    { NdbDictionary::Column c; c.setName(RONDB_OS_COL_STATEID);
      c.setType(NdbDictionary::Column::Varbinary); c.setLength(12);
      c.setPrimaryKey(true); c.setPartitionKey(false); c.setNullable(false);
      tbl.addColumn(c); }
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_open_by_client_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_OPEN_BY_CLIENT);
    { NdbDictionary::Column c; c.setName(RONDB_OS_COL_CLIENTID);
      c.setType(NdbDictionary::Column::Bigunsigned);
      c.setPrimaryKey(true); c.setPartitionKey(true); c.setNullable(false);
      tbl.addColumn(c); }
    { NdbDictionary::Column c; c.setName(RONDB_OS_COL_STATEID);
      c.setType(NdbDictionary::Column::Varbinary); c.setLength(12);
      c.setPrimaryKey(true); c.setPartitionKey(false); c.setNullable(false);
      tbl.addColumn(c); }
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_byte_locks_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_BYTE_LOCKS);
    { NdbDictionary::Column c; c.setName(RONDB_BL_COL_FILEID);
      c.setType(NdbDictionary::Column::Bigunsigned);
      c.setPrimaryKey(true); c.setPartitionKey(true); c.setNullable(false);
      tbl.addColumn(c); }
    { NdbDictionary::Column c; c.setName(RONDB_BL_COL_LOCK_ID);
      c.setType(NdbDictionary::Column::Bigunsigned);
      c.setPrimaryKey(true); c.setPartitionKey(false); c.setNullable(false);
      tbl.addColumn(c); }
    rondb_add_bigunsigned(tbl, RONDB_BL_COL_OFFSET, false, false);
    rondb_add_bigunsigned(tbl, RONDB_BL_COL_LENGTH, false, false);
    rondb_add_unsigned(tbl, RONDB_BL_COL_LOCK_TYPE);
    rondb_add_bigunsigned(tbl, RONDB_BL_COL_CLIENTID, false, false);
    rondb_add_varbinary(tbl, RONDB_BL_COL_OWNER, 128, false, false);
    rondb_add_unsigned(tbl, RONDB_BL_COL_OWNER_LEN);
    rondb_add_varbinary(tbl, RONDB_BL_COL_STATEID, 12, false, false);
    rondb_add_unsigned(tbl, RONDB_BL_COL_SEQID);
    rondb_add_varbinary(tbl, RONDB_BL_COL_OPEN_STATEID, 12, false, false);
    rondb_add_unsigned(tbl, RONDB_BL_COL_OWNER_MDS);
    rondb_add_bigunsigned(tbl, RONDB_BL_COL_OWNER_EPOCH, false, false);
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_lock_by_owner_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_LOCK_BY_OWNER);
    { NdbDictionary::Column c; c.setName(RONDB_BL_COL_CLIENTID);
      c.setType(NdbDictionary::Column::Bigunsigned);
      c.setPrimaryKey(true); c.setPartitionKey(true); c.setNullable(false);
      tbl.addColumn(c); }
    { NdbDictionary::Column c; c.setName(RONDB_BL_COL_FILEID);
      c.setType(NdbDictionary::Column::Bigunsigned);
      c.setPrimaryKey(true); c.setPartitionKey(false); c.setNullable(false);
      tbl.addColumn(c); }
    { NdbDictionary::Column c; c.setName(RONDB_BL_COL_LOCK_ID);
      c.setType(NdbDictionary::Column::Bigunsigned);
      c.setPrimaryKey(true); c.setPartitionKey(false); c.setNullable(false);
      tbl.addColumn(c); }
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_delegations_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_DELEGATIONS);
    { NdbDictionary::Column c; c.setName(RONDB_DG_COL_STATEID);
      c.setType(NdbDictionary::Column::Varbinary); c.setLength(12);
      c.setPrimaryKey(true); c.setPartitionKey(true); c.setNullable(false);
      tbl.addColumn(c); }
    rondb_add_unsigned(tbl, RONDB_DG_COL_SEQID);
    rondb_add_bigunsigned(tbl, RONDB_DG_COL_CLIENTID, false, false);
    rondb_add_bigunsigned(tbl, RONDB_DG_COL_FILEID, false, false);
    rondb_add_unsigned(tbl, RONDB_DG_COL_DELEG_TYPE);
    rondb_add_unsigned(tbl, RONDB_DG_COL_OWNER_MDS);
    rondb_add_bigunsigned(tbl, RONDB_DG_COL_OWNER_EPOCH, false, false);
    rondb_add_bigunsigned(tbl, RONDB_DG_COL_GRANT_TIME, false, false);
    rondb_add_tinyunsigned(tbl, RONDB_DG_COL_RECALL_PEND);
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_deleg_by_file_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_DELEG_BY_FILE);
    { NdbDictionary::Column c; c.setName(RONDB_DG_COL_FILEID);
      c.setType(NdbDictionary::Column::Bigunsigned);
      c.setPrimaryKey(true); c.setPartitionKey(true); c.setNullable(false);
      tbl.addColumn(c); }
    { NdbDictionary::Column c; c.setName(RONDB_DG_COL_STATEID);
      c.setType(NdbDictionary::Column::Varbinary); c.setLength(12);
      c.setPrimaryKey(true); c.setPartitionKey(false); c.setNullable(false);
      tbl.addColumn(c); }
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_deleg_by_client_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_DELEG_BY_CLIENT);
    { NdbDictionary::Column c; c.setName(RONDB_DG_COL_CLIENTID);
      c.setType(NdbDictionary::Column::Bigunsigned);
      c.setPrimaryKey(true); c.setPartitionKey(true); c.setNullable(false);
      tbl.addColumn(c); }
    { NdbDictionary::Column c; c.setName(RONDB_DG_COL_STATEID);
      c.setType(NdbDictionary::Column::Varbinary); c.setLength(12);
      c.setPrimaryKey(true); c.setPartitionKey(false); c.setNullable(false);
      tbl.addColumn(c); }
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_clients_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_CLIENTS);
    { NdbDictionary::Column c; c.setName(RONDB_CL_COL_CLIENTID);
      c.setType(NdbDictionary::Column::Bigunsigned);
      c.setPrimaryKey(true); c.setPartitionKey(true); c.setNullable(false);
      tbl.addColumn(c); }
    rondb_add_varbinary(tbl, RONDB_CL_COL_CO_OWNERID, 1024, false, false);
    rondb_add_varbinary(tbl, RONDB_CL_COL_VERIFIER, 8, false, false);
    rondb_add_tinyunsigned(tbl, RONDB_CL_COL_CONFIRMED);
    rondb_add_unsigned(tbl, RONDB_CL_COL_OWNER_MDS);
    rondb_add_bigunsigned(tbl, RONDB_CL_COL_OWNER_EPOCH, false, false);
    rondb_add_bigunsigned(tbl, RONDB_CL_COL_LEASE_NS, false, false);
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_sessions_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_SESSIONS);
    { NdbDictionary::Column c; c.setName(RONDB_SS_COL_SESSION_ID);
      c.setType(NdbDictionary::Column::Varbinary); c.setLength(16);
      c.setPrimaryKey(true); c.setPartitionKey(true); c.setNullable(false);
      tbl.addColumn(c); }
    rondb_add_bigunsigned(tbl, RONDB_SS_COL_CLIENTID, false, false);
    rondb_add_unsigned(tbl, RONDB_SS_COL_NUM_SLOTS);
    rondb_add_unsigned(tbl, RONDB_SS_COL_CB_PROG);
    rondb_add_unsigned(tbl, RONDB_SS_COL_CB_SEC);
    rondb_add_unsigned(tbl, RONDB_SS_COL_OWNER_MDS);
    rondb_add_bigunsigned(tbl, RONDB_SS_COL_OWNER_EPOCH, false, false);
    rondb_add_bigunsigned(tbl, RONDB_SS_COL_CREATED_NS, false, false);
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_session_by_client_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_SESSION_BY_CLIENT);
    { NdbDictionary::Column c; c.setName(RONDB_SS_COL_CLIENTID);
      c.setType(NdbDictionary::Column::Bigunsigned);
      c.setPrimaryKey(true); c.setPartitionKey(true); c.setNullable(false);
      tbl.addColumn(c); }
    { NdbDictionary::Column c; c.setName(RONDB_SS_COL_SESSION_ID);
      c.setType(NdbDictionary::Column::Varbinary); c.setLength(16);
      c.setPrimaryKey(true); c.setPartitionKey(false); c.setNullable(false);
      tbl.addColumn(c); }
    return rondb_create_table_if_not_exists(dict, tbl);
}

static int rondb_define_drc_slots_table(NdbDictionary::Dictionary *dict)
{
    NdbDictionary::Table tbl;
    tbl.setName(RONDB_TBL_DRC_SLOTS);
    { NdbDictionary::Column c; c.setName(RONDB_DR_COL_SESSION_ID);
      c.setType(NdbDictionary::Column::Varbinary); c.setLength(16);
      c.setPrimaryKey(true); c.setPartitionKey(true); c.setNullable(false);
      tbl.addColumn(c); }
    { NdbDictionary::Column c; c.setName(RONDB_DR_COL_SLOT_ID);
      c.setType(NdbDictionary::Column::Unsigned);
      c.setPrimaryKey(true); c.setPartitionKey(false); c.setNullable(false);
      tbl.addColumn(c); }
    rondb_add_unsigned(tbl, RONDB_DR_COL_SEQ_ID);
    { NdbDictionary::Column c; c.setName(RONDB_DR_COL_REPLY);
      c.setType(NdbDictionary::Column::Blob);
      c.setInlineSize(256); c.setPartSize(2000); c.setStripeSize(0);
      c.setNullable(true);
      tbl.addColumn(c); }
    rondb_add_unsigned(tbl, RONDB_DR_COL_REPLY_LEN);
    rondb_add_bigunsigned(tbl, RONDB_DR_COL_LAST_USED_NS, false, false);
    return rondb_create_table_if_not_exists(dict, tbl);
}

}  /* extern "C" */

#endif /* HAVE_RONDB */

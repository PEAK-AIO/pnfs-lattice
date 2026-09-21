/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_fdb_txn.c -- FoundationDB transaction runner (fdb_txn.[ch]).
 *
 * The runner's fdb_c calls go through an injectable table
 * (struct fdb_txn_calls), so the commit-outcome protocol is driven here
 * by a scripted mock without a buggify build:
 *
 *   commit OK                       one attempt, one witness write
 *   1021, mutation landed           witness read sees the sequence ->
 *                                   success, body not re-run
 *   1021, mutation not landed       witness absent -> body re-run
 *   1031, mutation landed           fence probe sees the sequence ->
 *                                   success, no fence commit
 *   1031, mutation never lands      fence commits (WRITE conflict
 *                                   range, no mutation) -> body re-run
 *   1031 forever                    deadline -> MDS_ERR_INDOUBT
 *   1020 forever                    deadline -> MDS_ERR_DELAY
 *   body errors 1020 / 1007 / 1031  retried; 2103 -> MDS_ERR_IO
 *   read-only body                  never committed, retried on 1007
 *   FDB_BODY_DONE                   status passes through, no commit
 *   alloc_id refill unresolved      MDS_ERR_DELAY, the batch abandoned;
 *                                   the next refill starts above it
 *
 * When the local cluster is reachable the same runner is exercised once
 * for real (a mutating body, a read-back, the id allocator and a fence
 * transaction whose commit is proven to reach the resolver).  The
 * binary exits 77 when the backend is not built or no readable cluster
 * file exists.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pnfs_mds.h"

#ifdef HAVE_FDB

#include "catalogue_fdb.h"
#include "catalogue_internal.h"
#include "fdb_codec.h"
#include "fdb_keys.h"
#include "fdb_txn.h"

static int tests_run;
static int tests_passed;
static int test_failed;

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", __FILE__, __LINE__, #cond); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a); \
    long long _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n", \
                __FILE__, __LINE__, #a, _a, #b, _b); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    fprintf(stdout, "  %-48s", #fn); \
    fflush(stdout); \
    tests_run++; \
    test_failed = 0; \
    fn(); \
    if (test_failed == 0) { \
        tests_passed++; \
        fprintf(stdout, "ok\n"); \
    } else { \
        fprintf(stdout, "FAILED\n"); \
    } \
} while (0)

/* -----------------------------------------------------------------------
 * Mock fdb_c
 *
 * Transactions are slots in a table; futures are heap objects (so a
 * leaked future shows up under ASan and in g_futures_live).  The
 * "database" is two cells: the witness key (present / value) and one
 * META counter (the id allocator's refill reads and sets it; a key is
 * told apart by the type byte after backend_init's one-byte prefix).
 * Commits pop steps from a script; a step says which error the commit
 * reports and whether the transaction's last sets landed.
 * ----------------------------------------------------------------------- */

#define MOCK_TRS 32

struct mock_tr {
    bool     used;
    int      sets;          /* witness transaction_set calls since (re)start */
    uint8_t  last_val[8];
    bool     meta_set;      /* a META counter set since (re)start */
    uint8_t  last_meta[8];
    bool     read_cr;       /* add_conflict_range READ seen */
    bool     write_cr;      /* add_conflict_range WRITE seen */
    uint32_t timeout_ms;    /* last TIMEOUT option */
};

struct mock_future {
    fdb_error_t err;
    bool        present;
    uint8_t     val[8];
};

struct commit_step {
    fdb_error_t err;
    bool        apply;      /* the set lands in the database */
};

static struct mock_tr      g_trs[MOCK_TRS];
static int                 g_trs_created;
static int                 g_futures_live;
static bool                g_db_present;
static uint8_t             g_db_val[8];
static bool                g_meta_present;
static uint8_t             g_meta_val[8];
static struct commit_step  g_script[32];
static int                 g_script_n;
static int                 g_script_i;
static int                 g_commits;           /* commit calls, any tr */
static int                 g_commits_with_set;  /* ... whose tr had a set */
static int                 g_fence_commits;     /* ... with a WRITE range and no set */
static int                 g_probes;            /* witness reads */
static int                 g_on_error_calls;
static uint64_t            g_clock_ns;
static uint64_t            g_clock_step_ns;
static uint64_t            g_witness_vals[16];  /* sets, in order */
static int                 g_witness_val_n;
static struct fdb_key      g_last_probe_key;

static void mock_reset(void)
{
    memset(g_trs, 0, sizeof(g_trs));
    g_trs_created = 0;
    g_futures_live = 0;
    g_db_present = false;
    memset(g_db_val, 0, sizeof(g_db_val));
    g_meta_present = false;
    memset(g_meta_val, 0, sizeof(g_meta_val));
    memset(g_script, 0, sizeof(g_script));
    g_script_n = 0;
    g_script_i = 0;
    g_commits = 0;
    g_commits_with_set = 0;
    g_fence_commits = 0;
    g_probes = 0;
    g_on_error_calls = 0;
    g_clock_ns = 1000000000000ULL;
    g_clock_step_ns = 1000000ULL; /* 1 ms per clock read */
    memset(g_witness_vals, 0, sizeof(g_witness_vals));
    g_witness_val_n = 0;
    memset(&g_last_probe_key, 0, sizeof(g_last_probe_key));
}

static void script_add(fdb_error_t err, bool apply)
{
    if (g_script_n < (int)(sizeof(g_script) / sizeof(g_script[0]))) {
        g_script[g_script_n].err = err;
        g_script[g_script_n].apply = apply;
        g_script_n++;
    }
}

static struct mock_tr *tr_of(FDBTransaction *tr)
{
    return (struct mock_tr *)tr;
}

/* backend_init's prefix is one byte, so the table selector is key[1]. */
static bool key_is_meta(const uint8_t *key, int key_len)
{
    return key_len > 1 && key[1] == (uint8_t)FDB_KT_META;
}

static void mock_meta_seed(uint64_t v)
{
    g_meta_present = true;
    fdb_le64_put(g_meta_val, v);
}

static struct mock_future *future_new(fdb_error_t err)
{
    struct mock_future *f = calloc(1, sizeof(*f));

    if (f != NULL) {
        f->err = err;
        g_futures_live++;
    }
    return f;
}

static fdb_error_t m_create_transaction(FDBDatabase *db, FDBTransaction **out)
{
    int i;

    (void)db;
    for (i = 0; i < MOCK_TRS; i++) {
        if (!g_trs[i].used) {
            memset(&g_trs[i], 0, sizeof(g_trs[i]));
            g_trs[i].used = true;
            g_trs_created++;
            *out = (FDBTransaction *)&g_trs[i];
            return 0;
        }
    }
    return 1500;
}

static void m_transaction_destroy(FDBTransaction *tr)
{
    tr_of(tr)->used = false;
}

static void m_transaction_reset(FDBTransaction *tr)
{
    struct mock_tr *t = tr_of(tr);

    t->sets = 0;
    t->meta_set = false;
    t->read_cr = false;
    t->write_cr = false;
}

static fdb_error_t m_set_option(FDBTransaction *tr, FDBTransactionOption opt,
                                const uint8_t *value, int value_len)
{
    if (opt == FDB_TR_OPTION_TIMEOUT && value_len == 8) {
        int64_t v;

        memcpy(&v, value, 8);
        tr_of(tr)->timeout_ms = (uint32_t)v;
    }
    return 0;
}

static void m_transaction_set(FDBTransaction *tr, const uint8_t *key, int key_len,
                              const uint8_t *value, int value_len)
{
    struct mock_tr *t = tr_of(tr);

    if (key_is_meta(key, key_len)) {
        t->meta_set = true;
        if (value_len == 8) {
            memcpy(t->last_meta, value, 8);
        }
        return;
    }
    t->sets++;
    if (value_len == 8) {
        memcpy(t->last_val, value, 8);
        if (g_witness_val_n < 16) {
            g_witness_vals[g_witness_val_n++] = fdb_le64_get(value);
        }
    }
}

static FDBFuture *m_transaction_get(FDBTransaction *tr, const uint8_t *key, int key_len,
                                    fdb_bool_t snapshot)
{
    struct mock_future *f = future_new(0);

    (void)tr;
    (void)snapshot;
    if (key_is_meta(key, key_len)) {
        if (f != NULL) {
            f->present = g_meta_present;
            memcpy(f->val, g_meta_val, 8);
        }
        return (FDBFuture *)f;
    }
    g_probes++;
    if (key_len > 0 && key_len <= (int)FDB_KEY_MAX) {
        memcpy(g_last_probe_key.buf, key, (size_t)key_len);
        g_last_probe_key.len = (uint32_t)key_len;
    }
    if (f != NULL) {
        f->present = g_db_present;
        memcpy(f->val, g_db_val, 8);
    }
    return (FDBFuture *)f;
}

/* fdb-fault track: the runner pins the read version before a commit. */
static int g_read_versions;

static FDBFuture *m_transaction_get_read_version(FDBTransaction *tr)
{
    (void)tr;
    g_read_versions++;
    return (FDBFuture *)future_new(0);
}

static fdb_error_t m_add_conflict_range(FDBTransaction *tr, const uint8_t *begin, int begin_len,
                                        const uint8_t *end, int end_len,
                                        FDBConflictRangeType type)
{
    struct mock_tr *t = tr_of(tr);

    (void)begin;
    (void)begin_len;
    (void)end;
    (void)end_len;
    if (type == FDB_CONFLICT_RANGE_TYPE_READ) {
        t->read_cr = true;
    } else {
        t->write_cr = true;
    }
    return 0;
}

static FDBFuture *m_transaction_commit(FDBTransaction *tr)
{
    struct mock_tr *t = tr_of(tr);
    struct commit_step step = { 0, true };

    if (g_script_i < g_script_n) {
        step = g_script[g_script_i++];
    }
    g_commits++;
    if (t->sets > 0) {
        g_commits_with_set++;
    } else if (t->write_cr) {
        g_fence_commits++;
    }
    if (step.apply && t->sets > 0) {
        g_db_present = true;
        memcpy(g_db_val, t->last_val, 8);
    }
    if (step.apply && t->meta_set) {
        g_meta_present = true;
        memcpy(g_meta_val, t->last_meta, 8);
    }
    return (FDBFuture *)future_new(step.err);
}

static bool m_is_retryable_not_committed(fdb_error_t err)
{
    return err == 1020 || err == 1007 || err == 1009 || err == 1037 || err == 1038;
}

static FDBFuture *m_transaction_on_error(FDBTransaction *tr, fdb_error_t err)
{
    g_on_error_calls++;
    /* Mirrors the client: conflicts, too-old, future-version and
     * commit_unknown_result are retryable and reset the transaction;
     * a timeout is rethrown. */
    if (m_is_retryable_not_committed(err) || err == 1021) {
        m_transaction_reset(tr);
        return (FDBFuture *)future_new(0);
    }
    return (FDBFuture *)future_new(err);
}

static fdb_error_t m_future_block_until_ready(FDBFuture *f)
{
    (void)f;
    return 0;
}

static fdb_error_t m_future_get_error(FDBFuture *f)
{
    return ((struct mock_future *)f)->err;
}

static fdb_error_t m_future_get_value(FDBFuture *f, fdb_bool_t *present, const uint8_t **value,
                                      int *value_len)
{
    struct mock_future *mf = (struct mock_future *)f;

    if (mf->err != 0) {
        return mf->err;
    }
    *present = mf->present ? 1 : 0;
    *value = mf->val;
    *value_len = 8;
    return 0;
}

static void m_future_destroy(FDBFuture *f)
{
    if (f != NULL) {
        g_futures_live--;
        free(f);
    }
}

static fdb_bool_t m_error_predicate(int predicate, fdb_error_t err)
{
    if (predicate == FDB_ERROR_PREDICATE_RETRYABLE_NOT_COMMITTED) {
        return m_is_retryable_not_committed(err) ? 1 : 0;
    }
    if (predicate == FDB_ERROR_PREDICATE_MAYBE_COMMITTED) {
        return (err == 1021 || err == 1031) ? 1 : 0;
    }
    return (m_is_retryable_not_committed(err) || err == 1021 || err == 1031) ? 1 : 0;
}

static uint64_t m_now_ns(void)
{
    g_clock_ns += g_clock_step_ns;
    return g_clock_ns;
}

static const struct fdb_txn_calls g_mock_calls = {
    .create_transaction             = m_create_transaction,
    .transaction_destroy            = m_transaction_destroy,
    .transaction_reset              = m_transaction_reset,
    .transaction_set_option         = m_set_option,
    .transaction_set                = m_transaction_set,
    .transaction_get                = m_transaction_get,
    .transaction_get_read_version   = m_transaction_get_read_version,
    .transaction_add_conflict_range = m_add_conflict_range,
    .transaction_commit             = m_transaction_commit,
    .transaction_on_error           = m_transaction_on_error,
    .future_block_until_ready       = m_future_block_until_ready,
    .future_get_error               = m_future_get_error,
    .future_get_value               = m_future_get_value,
    .future_destroy                 = m_future_destroy,
    .error_predicate                = m_error_predicate,
    .now_ns                         = m_now_ns,
};

static void backend_init(struct fdb_backend *b)
{
    memset(b, 0, sizeof(*b));
    b->db = (FDBDatabase *)(uintptr_t)0xD0D0;
    memcpy(b->prefix.bytes, "t", 1);
    b->prefix.len = 1;
    b->mds_id = 7;
    b->witness_epoch = 123456789ULL;
    b->op_deadline_ms = 1000;
    b->txn_timeout_ms = 400;
    b->instance_seq = 1;
    b->calls = &g_mock_calls;
}

/* Test bodies: no FDB calls, they just count and decide. */
struct body_ctx {
    int         calls;
    int         fail_first_with;  /* fdb error returned on the first call */
    enum mds_status done_status;  /* != MDS_OK: return FDB_BODY_DONE */
};

static int body_fn(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct body_ctx *c = arg;

    (void)tr;
    c->calls++;
    if (c->calls == 1 && c->fail_first_with != 0) {
        return c->fail_first_with;
    }
    if (c->done_status != MDS_OK) {
        *st_out = c->done_status;
        return FDB_BODY_DONE;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* -----------------------------------------------------------------------
 * Mock-driven tests
 * ----------------------------------------------------------------------- */

static void test_commit_ok(void)
{
    struct fdb_backend b;
    struct body_ctx c = { 0, 0, MDS_OK };
    struct fdb_key wk;
    uint64_t seq_before = 0;
    uint64_t seq = 0;

    mock_reset();
    backend_init(&b);
    /* The slot's next sequence is whatever the previous test left. */
    ASSERT_TRUE(fdb_txn_witness_key(&b, &wk, &seq_before));
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 1);
    ASSERT_EQ(g_commits, 1);
    ASSERT_EQ(g_commits_with_set, 1);
    ASSERT_EQ(g_witness_val_n, 1);
    seq = g_witness_vals[0];
    ASSERT_EQ(seq, seq_before + 1);
    ASSERT_TRUE(g_db_present);
    ASSERT_EQ(fdb_le64_get(g_db_val), seq);
    ASSERT_EQ(g_probes, 0);
    ASSERT_EQ(atomic_load(&b.stats.commits), 1);
    ASSERT_EQ(atomic_load(&b.stats.attempts), 1);
    ASSERT_EQ(g_futures_live, 0);
    /* The attempt transaction carried a READ conflict range and the
     * per-attempt TIMEOUT never exceeds the configured value. */
    ASSERT_TRUE(g_trs[0].read_cr);
    ASSERT_TRUE(g_trs[0].timeout_ms > 0 && g_trs[0].timeout_ms <= 400);
}

static void test_unknown_result_landed(void)
{
    struct fdb_backend b;
    struct body_ctx c = { 0, 0, MDS_OK };

    mock_reset();
    backend_init(&b);
    script_add(1021, true); /* committed, reply lost */
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 1);           /* never re-run: no duplicate */
    ASSERT_EQ(g_commits, 1);
    ASSERT_EQ(g_probes, 1);          /* one witness read */
    ASSERT_EQ(g_fence_commits, 0);   /* 1021 needs no fence */
    ASSERT_EQ(atomic_load(&b.stats.unknown_results), 1);
    ASSERT_EQ(atomic_load(&b.stats.unknown_landed), 1);
    ASSERT_EQ(atomic_load(&b.stats.commits), 1);
    ASSERT_EQ(g_futures_live, 0);
    /* The probe read the witness key, not something else. */
    ASSERT_EQ(g_last_probe_key.buf[b.prefix.len], FDB_KT_WITNESS);
}

static void test_unknown_result_not_landed(void)
{
    struct fdb_backend b;
    struct body_ctx c = { 0, 0, MDS_OK };

    mock_reset();
    backend_init(&b);
    script_add(1021, false); /* the commit never happened */
    script_add(0, true);     /* the re-run commits */
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(g_commits, 2);
    ASSERT_EQ(g_probes, 1);
    ASSERT_EQ(g_fence_commits, 0);
    ASSERT_EQ(g_witness_val_n, 2);
    ASSERT_TRUE(g_witness_vals[1] > g_witness_vals[0]); /* fresh sequence per attempt */
    ASSERT_EQ(fdb_le64_get(g_db_val), g_witness_vals[1]);
    ASSERT_EQ(atomic_load(&b.stats.unknown_results), 1);
    ASSERT_EQ(atomic_load(&b.stats.unknown_landed), 0);
    ASSERT_EQ(atomic_load(&b.stats.commits), 1);
    ASSERT_EQ(g_futures_live, 0);
}

static void test_timed_out_landed(void)
{
    struct fdb_backend b;
    struct body_ctx c = { 0, 0, MDS_OK };

    mock_reset();
    backend_init(&b);
    script_add(1031, true); /* timed out on the client, landed anyway */
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 1);           /* no double apply */
    ASSERT_EQ(g_commits, 1);         /* the probe alone proved the landing */
    ASSERT_EQ(g_probes, 1);
    ASSERT_EQ(g_fence_commits, 0);
    ASSERT_EQ(atomic_load(&b.stats.fence_landed), 1);
    ASSERT_EQ(atomic_load(&b.stats.commits), 1);
    ASSERT_EQ(g_futures_live, 0);
}

static void test_timed_out_fenced(void)
{
    struct fdb_backend b;
    struct body_ctx c = { 0, 0, MDS_OK };
    int i;
    bool fence_seen = false;

    mock_reset();
    backend_init(&b);
    script_add(1031, false); /* maybe in flight, will never land */
    script_add(0, false);    /* the fence commits */
    script_add(0, true);     /* the re-run commits */
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(g_commits, 3);
    ASSERT_EQ(g_fence_commits, 1);   /* WRITE conflict range, no mutation */
    ASSERT_EQ(g_commits_with_set, 2);
    ASSERT_EQ(atomic_load(&b.stats.fences), 1);
    ASSERT_EQ(atomic_load(&b.stats.fence_landed), 0);
    ASSERT_EQ(atomic_load(&b.stats.commits), 1);
    ASSERT_EQ(fdb_le64_get(g_db_val), g_witness_vals[1]);
    /* The fence transaction is a different transaction from the attempt. */
    ASSERT_TRUE(g_trs_created >= 2);
    for (i = 0; i < MOCK_TRS; i++) {
        if (g_trs[i].write_cr) {
            fence_seen = true;
        }
    }
    ASSERT_TRUE(fence_seen);
    ASSERT_EQ(g_futures_live, 0);
}

static void test_deadline_indoubt(void)
{
    struct fdb_backend b;
    struct body_ctx c = { 0, 0, MDS_OK };
    int i;

    mock_reset();
    backend_init(&b);
    b.op_deadline_ms = 500;
    g_clock_step_ns = 60ULL * 1000000ULL; /* 60 ms per clock read */
    /* The attempt times out and every fence round times out too. */
    for (i = 0; i < 32; i++) {
        script_add(1031, false);
    }
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_ERR_INDOUBT);
    ASSERT_EQ(c.calls, 1);           /* never re-run without a resolution */
    ASSERT_TRUE(g_fence_commits >= 1);
    ASSERT_EQ(atomic_load(&b.stats.indoubt), 1);
    ASSERT_EQ(atomic_load(&b.stats.commits), 0);
    ASSERT_TRUE(!g_db_present);
    ASSERT_EQ(g_futures_live, 0);
}

static void test_deadline_delay(void)
{
    struct fdb_backend b;
    struct body_ctx c = { 0, 0, MDS_OK };
    int i;

    mock_reset();
    backend_init(&b);
    b.op_deadline_ms = 500;
    g_clock_step_ns = 60ULL * 1000000ULL;
    for (i = 0; i < 32; i++) {
        script_add(1020, false); /* conflict after conflict */
    }
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_ERR_DELAY);
    ASSERT_TRUE(c.calls >= 2);
    ASSERT_TRUE(g_on_error_calls >= 1);
    ASSERT_EQ(g_probes, 0);          /* definitive aborts never probe */
    ASSERT_EQ(atomic_load(&b.stats.indoubt), 0);
    ASSERT_EQ(atomic_load(&b.stats.delay_exhausted), 1);
    ASSERT_TRUE(!g_db_present);
    ASSERT_EQ(g_futures_live, 0);
}

static void test_retry_classification(void)
{
    struct fdb_backend b;
    struct body_ctx c;

    /* Body read hits a conflict: on_error, re-run, commit. */
    mock_reset();
    backend_init(&b);
    memset(&c, 0, sizeof(c));
    c.fail_first_with = 1020;
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(g_on_error_calls, 1);
    ASSERT_EQ(atomic_load(&b.stats.retries), 1);

    /* transaction_too_old on a read: same. */
    mock_reset();
    backend_init(&b);
    memset(&c, 0, sizeof(c));
    c.fail_first_with = 1007;
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);

    /* A timeout on a READ is definitive: reset and retry, no fence. */
    mock_reset();
    backend_init(&b);
    memset(&c, 0, sizeof(c));
    c.fail_first_with = 1031;
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(g_probes, 0);
    ASSERT_EQ(g_fence_commits, 0);

    /* A non-retryable error is MDS_ERR_IO, nothing committed. */
    mock_reset();
    backend_init(&b);
    memset(&c, 0, sizeof(c));
    c.fail_first_with = 2103;
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_ERR_IO);
    ASSERT_EQ(c.calls, 1);
    ASSERT_EQ(g_commits, 0);
    ASSERT_EQ(atomic_load(&b.stats.io_errors), 1);

    /* A conflict reported by the commit itself: retried. */
    mock_reset();
    backend_init(&b);
    memset(&c, 0, sizeof(c));
    script_add(1020, false);
    script_add(0, true);
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(g_commits, 2);
    ASSERT_EQ(g_futures_live, 0);
}

static void test_readonly_and_done(void)
{
    struct fdb_backend b;
    struct body_ctx c;

    /* Read-only: never committed, no witness, retried on too-old. */
    mock_reset();
    backend_init(&b);
    memset(&c, 0, sizeof(c));
    c.fail_first_with = 1007;
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_READONLY, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(g_commits, 0);
    ASSERT_EQ(g_witness_val_n, 0);

    /* FDB_BODY_DONE: the body's status passes through, nothing commits
     * even though the transaction may carry mutations. */
    mock_reset();
    backend_init(&b);
    memset(&c, 0, sizeof(c));
    c.done_status = MDS_ERR_EXISTS;
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_ERR_EXISTS);
    ASSERT_EQ(g_commits, 0);
    ASSERT_EQ(c.calls, 1);

    /* Argument checks. */
    ASSERT_EQ(fdb_run_txn(NULL, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_ERR_INVAL);
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", NULL, &c), MDS_ERR_INVAL);
    ASSERT_EQ(g_futures_live, 0);
}

static void test_witness_key_shape(void)
{
    struct fdb_backend b;
    struct fdb_key k;
    struct fdb_key k2;
    uint64_t s1 = 0;
    uint64_t s2 = 0;

    backend_init(&b);
    ASSERT_TRUE(fdb_txn_witness_key(&b, &k, &s1));
    ASSERT_TRUE(fdb_txn_witness_key(&b, &k2, &s2));
    ASSERT_EQ(s2, s1 + 1);                       /* monotonic per slot */
    ASSERT_EQ(k.len, k2.len);
    ASSERT_TRUE(memcmp(k.buf, k2.buf, k.len) == 0); /* same slot key on this thread */
    ASSERT_EQ(k.len, 1 + 1 + 4 + 8 + 4);
    ASSERT_EQ(k.buf[1], FDB_KT_WITNESS);
    ASSERT_EQ(fdb_get_u32(k.buf + 2), 7);        /* mds_id */
    ASSERT_EQ(fdb_get_u64(k.buf + 6), 123456789ULL); /* epoch */
}

/* -----------------------------------------------------------------------
 * Phase 6c additions (fault-injection track): a transaction object
 * whose commit may still be in flight is replaced, never reused;
 * 1025 / 1039 are classified like 1031; resolver rounds that keep
 * failing after a landed commit end in INDOUBT with exactly one effect.
 *
 * A thin table over the mock counts the transaction objects the runner
 * creates and destroys and can script the error every probe read
 * reports.
 * ----------------------------------------------------------------------- */

static int         g_w_creates;
static int         g_w_destroys;
static fdb_error_t g_w_probe_err;      /* != 0: probe reads fail with it ... */
static int         g_w_probe_err_left; /* ... this many times (-1: forever) */
static fdb_error_t g_w_rv_err;         /* != 0: read-version pins fail with it ... */
static int         g_w_rv_err_left;    /* ... this many times */

static fdb_error_t w_create_transaction(FDBDatabase *db, FDBTransaction **out)
{
    g_w_creates++;
    return m_create_transaction(db, out);
}

static void w_transaction_destroy(FDBTransaction *tr)
{
    g_w_destroys++;
    m_transaction_destroy(tr);
}

/* Probe reads only: a body's META counter read is never failed here. */
static FDBFuture *w_transaction_get(FDBTransaction *tr, const uint8_t *key, int key_len,
                                    fdb_bool_t snapshot)
{
    struct mock_future *f = (struct mock_future *)m_transaction_get(tr, key, key_len, snapshot);

    if (f != NULL && !key_is_meta(key, key_len) && g_w_probe_err != 0 &&
        g_w_probe_err_left != 0) {
        f->err = g_w_probe_err;
        if (g_w_probe_err_left > 0) {
            g_w_probe_err_left--;
        }
    }
    return (FDBFuture *)f;
}

static FDBFuture *w_transaction_get_read_version(FDBTransaction *tr)
{
    struct mock_future *f = (struct mock_future *)m_transaction_get_read_version(tr);

    if (f != NULL && g_w_rv_err != 0 && g_w_rv_err_left > 0) {
        f->err = g_w_rv_err;
        g_w_rv_err_left--;
    }
    return (FDBFuture *)f;
}

static const struct fdb_txn_calls g_wrap_calls = {
    .create_transaction             = w_create_transaction,
    .transaction_destroy            = w_transaction_destroy,
    .transaction_reset              = m_transaction_reset,
    .transaction_set_option         = m_set_option,
    .transaction_set                = m_transaction_set,
    .transaction_get                = w_transaction_get,
    .transaction_get_read_version   = w_transaction_get_read_version,
    .transaction_add_conflict_range = m_add_conflict_range,
    .transaction_commit             = m_transaction_commit,
    .transaction_on_error           = m_transaction_on_error,
    .future_block_until_ready       = m_future_block_until_ready,
    .future_get_error               = m_future_get_error,
    .future_get_value               = m_future_get_value,
    .future_destroy                 = m_future_destroy,
    .error_predicate                = m_error_predicate,
    .now_ns                         = m_now_ns,
};

static void wrap_reset(struct fdb_backend *b)
{
    mock_reset();
    backend_init(b);
    b->calls = &g_wrap_calls;
    g_w_creates = 0;
    g_w_destroys = 0;
    g_w_probe_err = 0;
    g_w_probe_err_left = 0;
    g_w_rv_err = 0;
    g_w_rv_err_left = 0;
    g_read_versions = 0;
}

/* Every mutating attempt pins its read version exactly once, before the
 * commit; read-only bodies and bodies that decline to commit never do. */
static void test_read_version_pinned(void)
{
    struct fdb_backend b;
    struct body_ctx c;

    wrap_reset(&b);
    memset(&c, 0, sizeof(c));
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(g_read_versions, 1);

    wrap_reset(&b);
    memset(&c, 0, sizeof(c));
    script_add(1031, false); /* fenced, re-run: two attempts, two pins */
    script_add(0, false);
    script_add(0, true);
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(g_read_versions, 2);

    wrap_reset(&b);
    memset(&c, 0, sizeof(c));
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_READONLY, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(g_read_versions, 0);

    wrap_reset(&b);
    memset(&c, 0, sizeof(c));
    c.done_status = MDS_ERR_EXISTS;
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_ERR_EXISTS);
    ASSERT_EQ(g_read_versions, 0);
    ASSERT_EQ(g_futures_live, 0);

    /* A pin that times out happened before anything was issued: a
     * definitive failure, re-run without a probe or a fence. */
    wrap_reset(&b);
    memset(&c, 0, sizeof(c));
    g_w_rv_err = 1031;
    g_w_rv_err_left = 1;
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(g_read_versions, 2);
    ASSERT_EQ(g_probes, 0);
    ASSERT_EQ(g_fence_commits, 0);
    ASSERT_EQ(g_commits, 1);
    ASSERT_EQ(atomic_load(&b.stats.retries), 1);
    ASSERT_EQ(atomic_load(&b.stats.indoubt), 0);
    ASSERT_EQ(g_futures_live, 0);
}

static int mock_live_transactions(void)
{
    int i;
    int live = 0;

    for (i = 0; i < MOCK_TRS; i++) {
        if (g_trs[i].used) {
            live++;
        }
    }
    return live;
}

/* 1031 not landed: the fenced attempt's object is destroyed and a fresh
 * one runs the body again (attempt, resolver, replacement = 3 objects);
 * 1021 not landed keeps the object (nothing can be in flight). */
static void test_inflight_object_replaced(void)
{
    struct fdb_backend b;
    struct body_ctx c = { 0, 0, MDS_OK };

    wrap_reset(&b);
    script_add(1031, false); /* maybe in flight, never lands */
    script_add(0, false);    /* the fence commits */
    script_add(0, true);     /* the re-run commits */
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(g_fence_commits, 1);
    ASSERT_EQ(g_w_creates, 3);
    ASSERT_EQ(g_w_destroys, 3);
    ASSERT_EQ(mock_live_transactions(), 0);
    ASSERT_EQ(fdb_le64_get(g_db_val), g_witness_vals[1]);
    ASSERT_EQ(g_futures_live, 0);

    wrap_reset(&b);
    memset(&c, 0, sizeof(c));
    script_add(1021, false); /* never happened; the client fenced it itself */
    script_add(0, true);
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(g_w_creates, 2);   /* attempt + resolver only: reset, not replaced */
    ASSERT_EQ(g_w_destroys, 2);
    ASSERT_EQ(mock_live_transactions(), 0);
    ASSERT_EQ(g_futures_live, 0);
}

/* 1025 transaction_cancelled and 1039 cluster_version_changed take the
 * fence path exactly like 1031. */
static void test_cancelled_and_version_changed(void)
{
    struct fdb_backend b;
    struct body_ctx c;

    /* 1025, landed: the probe alone proves it, no fence, no re-run. */
    wrap_reset(&b);
    memset(&c, 0, sizeof(c));
    script_add(1025, true);
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 1);
    ASSERT_EQ(g_commits, 1);
    ASSERT_EQ(g_probes, 1);
    ASSERT_EQ(g_fence_commits, 0);
    ASSERT_EQ(atomic_load(&b.stats.fence_landed), 1);
    ASSERT_EQ(atomic_load(&b.stats.commits), 1);

    /* 1039, never lands: fence, replacement object, re-run. */
    wrap_reset(&b);
    memset(&c, 0, sizeof(c));
    script_add(1039, false);
    script_add(0, false);
    script_add(0, true);
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(g_commits, 3);
    ASSERT_EQ(g_fence_commits, 1);
    ASSERT_EQ(atomic_load(&b.stats.fences), 1);
    ASSERT_EQ(atomic_load(&b.stats.fence_landed), 0);
    ASSERT_EQ(g_w_creates, 3);
    ASSERT_EQ(g_w_destroys, 3);
    ASSERT_EQ(mock_live_transactions(), 0);
    ASSERT_EQ(g_futures_live, 0);
}

/* Commit landed with 1021, then every probe read times out until the
 * deadline: INDOUBT, the body never re-run, exactly one effect (the
 * landed one), every resolver object replaced and released. */
static void test_landed_then_probes_fail(void)
{
    struct fdb_backend b;
    struct body_ctx c = { 0, 0, MDS_OK };
    uint64_t seq;

    wrap_reset(&b);
    b.op_deadline_ms = 500;
    g_clock_step_ns = 60ULL * 1000000ULL;
    script_add(1021, true);
    g_w_probe_err = 1031;
    g_w_probe_err_left = -1;
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_ERR_INDOUBT);
    ASSERT_EQ(c.calls, 1);
    ASSERT_EQ(g_commits, 1);
    ASSERT_EQ(g_fence_commits, 0);
    ASSERT_TRUE(g_probes >= 2);
    ASSERT_TRUE(g_db_present);
    seq = g_witness_vals[0];
    ASSERT_EQ(fdb_le64_get(g_db_val), seq); /* the one landed attempt */
    ASSERT_EQ(atomic_load(&b.stats.indoubt), 1);
    ASSERT_EQ(atomic_load(&b.stats.commits), 0);
    ASSERT_TRUE(g_w_creates >= 3);          /* attempt + a resolver per timed-out round */
    ASSERT_EQ(g_w_destroys, g_w_creates);
    ASSERT_EQ(mock_live_transactions(), 0);
    ASSERT_EQ(g_futures_live, 0);

    /* Probe conflicts back off and re-probe; the third read sees it. */
    wrap_reset(&b);
    memset(&c, 0, sizeof(c));
    script_add(1021, true);
    g_w_probe_err = 1020;
    g_w_probe_err_left = 2;
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 1);
    ASSERT_EQ(g_probes, 3);
    ASSERT_EQ(g_on_error_calls, 2);
    ASSERT_EQ(atomic_load(&b.stats.unknown_landed), 1);
    ASSERT_EQ(g_w_creates, 2);
    ASSERT_EQ(g_w_destroys, 2);
    ASSERT_EQ(g_futures_live, 0);
}

/* A fence round whose commit reports 1021 is repeated (reset, re-probe,
 * fence again) and the attempt is only re-run once a fence committed. */
static void test_fence_round_unknown_result(void)
{
    struct fdb_backend b;
    struct body_ctx c = { 0, 0, MDS_OK };

    wrap_reset(&b);
    script_add(1031, false); /* attempt: maybe in flight */
    script_add(1021, false); /* first fence: unknown result */
    script_add(0, false);    /* second fence commits */
    script_add(0, true);     /* re-run commits */
    ASSERT_EQ(fdb_run_txn(&b, FDB_TXN_MUTATING, "t", body_fn, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(g_commits, 4);
    ASSERT_EQ(g_fence_commits, 2);
    ASSERT_EQ(g_probes, 2);
    ASSERT_EQ(atomic_load(&b.stats.fences), 2);
    ASSERT_EQ(atomic_load(&b.stats.commits), 1);
    ASSERT_EQ(g_w_creates, 3);   /* the 1021 fence round reset its object */
    ASSERT_EQ(g_w_destroys, 3);
    ASSERT_EQ(mock_live_transactions(), 0);
    ASSERT_EQ(fdb_le64_get(g_db_val), g_witness_vals[1]);
    ASSERT_EQ(g_futures_live, 0);
}

/* -----------------------------------------------------------------------
 * Id allocator over the mock: a refill whose commit outcome stays
 * unresolved is MDS_ERR_DELAY, never INDOUBT -- the batch it may have
 * reserved is simply never handed out.  Two shapes: the refill landed
 * (1021, then every probe fails until the deadline) -- the counter
 * advanced, that batch is lost and the next refill starts above it;
 * the refill never landed (1031, every fence times out) -- the counter
 * is untouched and the next refill hands out the first id.  The pool
 * is keyed by instance_seq, so values no real handle uses keep these
 * thread-local batches away from the real-cluster tests.
 * ----------------------------------------------------------------------- */

#define ALLOC_MOCK_INSTANCE 0x5EEDULL
#define ALLOC_MOCK_SEED     1000ULL

static void test_alloc_id_unresolved_refill(void)
{
    struct fdb_backend b;
    uint64_t id = 0;
    int commits;
    int i;

    /* Landed, never resolved: the batch is lost, ids stay unique. */
    wrap_reset(&b);
    b.instance_seq = ALLOC_MOCK_INSTANCE;
    b.op_deadline_ms = 500;
    g_clock_step_ns = 60ULL * 1000000ULL;
    mock_meta_seed(ALLOC_MOCK_SEED);
    script_add(1021, true);
    g_w_probe_err = 1031;
    g_w_probe_err_left = -1;
    ASSERT_EQ(fdb_backend_alloc_id(&b, FDB_META_GC_SEQ, &id), MDS_ERR_DELAY);
    ASSERT_EQ(atomic_load(&b.stats.indoubt), 1);      /* the runner's verdict ... */
    ASSERT_EQ(fdb_le64_get(g_meta_val), ALLOC_MOCK_SEED + MDS_FILEID_BATCH); /* ... landed */
    ASSERT_EQ(g_futures_live, 0);
    ASSERT_EQ(mock_live_transactions(), 0);
    /* The next call refills above the lost batch and serves from it. */
    g_w_probe_err = 0;
    g_clock_step_ns = 1000000ULL;
    ASSERT_EQ(fdb_backend_alloc_id(&b, FDB_META_GC_SEQ, &id), MDS_OK);
    ASSERT_EQ(id, ALLOC_MOCK_SEED + MDS_FILEID_BATCH + 1U);
    ASSERT_EQ(fdb_le64_get(g_meta_val), ALLOC_MOCK_SEED + 2U * MDS_FILEID_BATCH);
    commits = g_commits;
    ASSERT_EQ(fdb_backend_alloc_id(&b, FDB_META_GC_SEQ, &id), MDS_OK);
    ASSERT_EQ(id, ALLOC_MOCK_SEED + MDS_FILEID_BATCH + 2U);
    ASSERT_EQ(g_commits, commits);                     /* from the batch, no refill */
    ASSERT_EQ(g_futures_live, 0);

    /* Never landed, never resolved: nothing is lost. */
    mock_reset();
    backend_init(&b);
    b.instance_seq = ALLOC_MOCK_INSTANCE + 1U;
    b.op_deadline_ms = 500;
    g_clock_step_ns = 60ULL * 1000000ULL;
    mock_meta_seed(ALLOC_MOCK_SEED);
    for (i = 0; i < 32; i++) {
        script_add(1031, false);
    }
    ASSERT_EQ(fdb_backend_alloc_id(&b, FDB_META_GC_SEQ, &id), MDS_ERR_DELAY);
    ASSERT_EQ(atomic_load(&b.stats.indoubt), 1);
    ASSERT_TRUE(g_fence_commits >= 1);
    ASSERT_EQ(fdb_le64_get(g_meta_val), ALLOC_MOCK_SEED);
    ASSERT_EQ(g_futures_live, 0);
    g_script_i = g_script_n;                           /* commits succeed from here */
    g_clock_step_ns = 1000000ULL;
    ASSERT_EQ(fdb_backend_alloc_id(&b, FDB_META_GC_SEQ, &id), MDS_OK);
    ASSERT_EQ(id, ALLOC_MOCK_SEED + 1U);
    ASSERT_EQ(fdb_le64_get(g_meta_val), ALLOC_MOCK_SEED + MDS_FILEID_BATCH);
    ASSERT_EQ(g_futures_live, 0);

    /* Definitive aborts to the deadline stay DELAY; an absent counter is
     * IO (not bootstrapped) and commits nothing. */
    mock_reset();
    backend_init(&b);
    b.instance_seq = ALLOC_MOCK_INSTANCE + 2U;
    b.op_deadline_ms = 500;
    g_clock_step_ns = 60ULL * 1000000ULL;
    mock_meta_seed(ALLOC_MOCK_SEED);
    for (i = 0; i < 32; i++) {
        script_add(1020, false);
    }
    ASSERT_EQ(fdb_backend_alloc_id(&b, FDB_META_GC_SEQ, &id), MDS_ERR_DELAY);
    ASSERT_EQ(atomic_load(&b.stats.indoubt), 0);
    ASSERT_EQ(fdb_le64_get(g_meta_val), ALLOC_MOCK_SEED);
    mock_reset();
    backend_init(&b);
    b.instance_seq = ALLOC_MOCK_INSTANCE + 3U;
    ASSERT_EQ(fdb_backend_alloc_id(&b, FDB_META_GC_SEQ, &id), MDS_ERR_IO);
    ASSERT_EQ(g_commits, 0);
    ASSERT_EQ(g_futures_live, 0);
}

/* -----------------------------------------------------------------------
 * Real path (local cluster)
 * ----------------------------------------------------------------------- */

struct real_ctx {
    struct fdb_backend *b;
    struct fdb_key      key;
    uint64_t            value;
    bool                found;
};

static int real_set_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct real_ctx *c = arg;

    fdb_txn_set_le64(tr, &c->key, c->value);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static int real_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct real_ctx *c = arg;
    uint8_t buf[8];
    size_t len = 0;
    fdb_error_t err;

    err = fdb_txn_get(tr, &c->key, false, buf, sizeof(buf), &len, &c->found);
    if (err != 0) {
        return (int)err;
    }
    if (c->found && !fdb_le64_decode(buf, len, &c->value)) {
        *st_out = MDS_ERR_IO;
        return FDB_BODY_DONE;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

static struct mds_catalogue *g_real_cat;

static void test_real_cluster(void)
{
    struct fdb_backend *b = g_real_cat->backend_private;
    struct real_ctx c;
    struct fdb_key wk;
    uint64_t seq = 0;
    uint64_t id1 = 0;
    uint64_t id2 = 0;
    FDBTransaction *tr = NULL;
    FDBFuture *f;
    int64_t committed = -1;

    ASSERT_TRUE(b != NULL);
    memset(&c, 0, sizeof(c));
    c.b = b;
    fdb_key_init(&c.key, &b->prefix, FDB_KT_META);
    fdb_key_u8(&c.key, 0x7F); /* a scratch META sub-key */
    c.value = 0xABCDEF;
    ASSERT_EQ(fdb_run_txn(b, FDB_TXN_MUTATING, "real_set", real_set_body, &c), MDS_OK);
    c.value = 0;
    ASSERT_EQ(fdb_run_txn(b, FDB_TXN_READONLY, "real_get", real_get_body, &c), MDS_OK);
    ASSERT_TRUE(c.found);
    ASSERT_EQ(c.value, 0xABCDEF);

    /* The witness key of this thread carries the last attempt's seq. */
    ASSERT_TRUE(fdb_txn_witness_key(b, &wk, &seq));
    c.key = wk;
    c.found = false;
    ASSERT_EQ(fdb_run_txn(b, FDB_TXN_READONLY, "real_wit", real_get_body, &c), MDS_OK);
    ASSERT_TRUE(c.found);
    ASSERT_EQ(c.value, seq - 1);

    /* Ids come out of a batch, strictly increasing. */
    ASSERT_EQ(fdb_backend_alloc_id(b, FDB_META_FILEID, &id1), MDS_OK);
    ASSERT_EQ(fdb_backend_alloc_id(b, FDB_META_FILEID, &id2), MDS_OK);
    ASSERT_TRUE(id1 > MDS_FILEID_ROOT);
    ASSERT_EQ(id2, id1 + 1);
    ASSERT_EQ(fdb_backend_alloc_id(b, FDB_META_SCHEMA_VERSION, &id1), MDS_ERR_INVAL);

    /* A fence transaction (read + WRITE conflict range, no mutation)
     * must reach the resolver: a real committed version proves the
     * client did not short-circuit it as read-only. */
    ASSERT_EQ(fdb_database_create_transaction(b->db, &tr), 0);
    ASSERT_EQ(fdb_txn_set_timeout(tr, 4000), 0);
    f = fdb_txn_get_start(tr, &wk, false);
    {
        uint8_t buf[8];
        size_t len = 0;
        bool found = false;

        ASSERT_EQ(fdb_txn_get_finish(f, buf, sizeof(buf), &len, &found), 0);
    }
    {
        struct fdb_key_range r;

        fdb_key_range_single(&r, &wk);
        ASSERT_EQ(fdb_transaction_add_conflict_range(tr, r.begin.buf, (int)r.begin.len,
                                                     r.end.buf, (int)r.end.len,
                                                     FDB_CONFLICT_RANGE_TYPE_WRITE), 0);
    }
    f = fdb_transaction_commit(tr);
    ASSERT_EQ(fdb_txn_wait(f), 0);
    fdb_future_destroy(f);
    ASSERT_EQ(fdb_transaction_get_committed_version(tr, &committed), 0);
    ASSERT_TRUE(committed > 0);
    fdb_transaction_destroy(tr);
    ASSERT_TRUE(atomic_load(&b->stats.commits) >= 2);
}

/* -----------------------------------------------------------------------
 * Fence premise against the installed client (fdb-fault track).
 *
 * Bare fdb_c, no runner.  An "attempt" transaction fixes its read
 * version with a read of another key, sets the witness key K and adds
 * its conflict ranges; a "fence" transaction reads K and adds a WRITE
 * conflict range; the fence commits first, then the held-back attempt
 * is committed.  What the resolver then does is the premise:
 *   set K, then READ range on K  -> the attempt COMMITS.  The read-your-
 *                                   writes layer records a READ range
 *                                   only over keys the transaction did
 *                                   not mutate, so the range never
 *                                   reaches the resolver: the reason the
 *                                   runner fences on an unwritten anchor.
 *   READ range on K, then set K  -> reported, not asserted: it fences on
 *                                   7.3 because ranges added before the
 *                                   write are kept, which is undocumented.
 *   anchor K + 0x01, either order -> 1020 not_committed: the fence works.
 *   two attempts on the anchor   -> the one committing second gets 1020.
 *   blind attempt (no read), anchor ranges, read version NOT pinned
 *                                -> the attempt COMMITS: its read version
 *                                   is taken inside the commit, after the
 *                                   fence, so nothing precedes the fence.
 *   blind attempt, read version pinned before the commit (the runner's
 *   pin_read_version)            -> 1020: the fence works.
 * ----------------------------------------------------------------------- */

enum fence_shape {
    SHAPE_SET_THEN_READ = 0,
    SHAPE_READ_THEN_SET,
    SHAPE_ANCHOR,
    SHAPE_ANCHOR_REVERSED,
    SHAPE_BLIND_UNPINNED,
    SHAPE_BLIND_PINNED,
};

static fdb_error_t real_wait_destroy(FDBFuture *f)
{
    fdb_error_t err = fdb_txn_wait(f);

    if (f != NULL) {
        fdb_future_destroy(f);
    }
    return err;
}

static fdb_error_t real_add_range(FDBTransaction *tr, const struct fdb_key *k,
                                  FDBConflictRangeType type)
{
    struct fdb_key_range r;

    fdb_key_range_single(&r, k);
    return fdb_transaction_add_conflict_range(tr, r.begin.buf, (int)r.begin.len, r.end.buf,
                                              (int)r.end.len, type);
}

/* Scratch keys inside the test keyspace (cleared with it). */
static void premise_keys(const struct fdb_backend *b, struct fdb_key *other,
                         struct fdb_key *witness, struct fdb_key *anchor)
{
    fdb_key_init(other, &b->prefix, FDB_KT_META);
    fdb_key_u8(other, 0x70);
    fdb_key_init(witness, &b->prefix, FDB_KT_META);
    fdb_key_u8(witness, 0x71);
    fdb_key_witness_fence(anchor, witness);
}

/* An attempt of @p shape: read version fixed by a read of @p other,
 * witness set, conflict ranges per shape.  Nothing is committed. */
static fdb_error_t premise_attempt(FDBDatabase *db, enum fence_shape shape,
                                   const struct fdb_key *other, const struct fdb_key *witness,
                                   const struct fdb_key *anchor, FDBTransaction **out)
{
    uint8_t v[8] = { 1, 0, 0, 0, 0, 0, 0, 0 };
    fdb_error_t err = fdb_database_create_transaction(db, out);

    if (err != 0) {
        return err;
    }
    err = fdb_txn_set_timeout(*out, 4000);
    if (err == 0 && shape != SHAPE_BLIND_UNPINNED && shape != SHAPE_BLIND_PINNED) {
        err = real_wait_destroy(fdb_txn_get_start(*out, other, false));
    }
    if (err != 0) {
        return err;
    }
    switch (shape) {
    case SHAPE_SET_THEN_READ:
        fdb_txn_set(*out, witness, v, sizeof(v));
        err = real_add_range(*out, witness, FDB_CONFLICT_RANGE_TYPE_READ);
        break;
    case SHAPE_READ_THEN_SET:
        err = real_add_range(*out, witness, FDB_CONFLICT_RANGE_TYPE_READ);
        fdb_txn_set(*out, witness, v, sizeof(v));
        break;
    case SHAPE_ANCHOR_REVERSED:
        fdb_txn_set(*out, witness, v, sizeof(v));
        err = real_add_range(*out, anchor, FDB_CONFLICT_RANGE_TYPE_WRITE);
        if (err == 0) {
            err = real_add_range(*out, anchor, FDB_CONFLICT_RANGE_TYPE_READ);
        }
        break;
    default: /* SHAPE_ANCHOR and the blind shapes */
        fdb_txn_set(*out, witness, v, sizeof(v));
        err = real_add_range(*out, anchor, FDB_CONFLICT_RANGE_TYPE_READ);
        if (err == 0) {
            err = real_add_range(*out, anchor, FDB_CONFLICT_RANGE_TYPE_WRITE);
        }
        break;
    }
    if (err == 0 && shape == SHAPE_BLIND_PINNED) {
        err = real_wait_destroy(fdb_transaction_get_read_version(*out));
    }
    return err;
}

/* The fence: probe read of the witness, WRITE range on @p fence_key. */
static fdb_error_t premise_fence(FDBDatabase *db, const struct fdb_key *witness,
                                 const struct fdb_key *fence_key)
{
    FDBTransaction *fen = NULL;
    fdb_error_t err = fdb_database_create_transaction(db, &fen);

    if (err != 0) {
        return err;
    }
    err = fdb_txn_set_timeout(fen, 4000);
    if (err == 0) {
        err = real_wait_destroy(fdb_txn_get_start(fen, witness, false));
    }
    if (err == 0) {
        err = real_add_range(fen, fence_key, FDB_CONFLICT_RANGE_TYPE_WRITE);
    }
    if (err == 0) {
        err = real_wait_destroy(fdb_transaction_commit(fen));
    }
    fdb_transaction_destroy(fen);
    return err;
}

/* Held-back attempt, fence commits, attempt committed afterwards:
 * returns the attempt's commit error; *fence_err the fence's. */
static fdb_error_t premise_scenario(struct fdb_backend *b, enum fence_shape shape,
                                    fdb_error_t *fence_err)
{
    struct fdb_key other;
    struct fdb_key witness;
    struct fdb_key anchor;
    FDBTransaction *att = NULL;
    fdb_error_t err;

    premise_keys(b, &other, &witness, &anchor);
    err = premise_attempt(b->db, shape, &other, &witness, &anchor, &att);
    if (err == 0) {
        *fence_err = premise_fence(b->db, &witness,
                                   shape >= SHAPE_ANCHOR ? &anchor : &witness);
        err = real_wait_destroy(fdb_transaction_commit(att));
    }
    if (att != NULL) {
        fdb_transaction_destroy(att);
    }
    return err;
}

static void test_real_fence_premise(void)
{
    struct fdb_backend *b = g_real_cat->backend_private;
    struct fdb_key other;
    struct fdb_key witness;
    struct fdb_key anchor;
    FDBTransaction *a1 = NULL;
    FDBTransaction *a2 = NULL;
    fdb_error_t fence_err = -1;
    fdb_error_t err;

    ASSERT_TRUE(b != NULL);
    /* The elision that motivated the anchor: a READ range on the key the
     * transaction set never reaches the resolver. */
    err = premise_scenario(b, SHAPE_SET_THEN_READ, &fence_err);
    ASSERT_EQ(fence_err, 0);
    ASSERT_EQ(err, 0);
    /* Reported only (see the section comment). */
    err = premise_scenario(b, SHAPE_READ_THEN_SET, &fence_err);
    ASSERT_EQ(fence_err, 0);
    fprintf(stdout, "[read range before the write: late commit -> %d %s] ", (int)err,
            fdb_get_error(err));
    /* The anchor fences in either order. */
    err = premise_scenario(b, SHAPE_ANCHOR, &fence_err);
    ASSERT_EQ(fence_err, 0);
    ASSERT_EQ(err, FDB_ERR_NOT_COMMITTED);
    err = premise_scenario(b, SHAPE_ANCHOR_REVERSED, &fence_err);
    ASSERT_EQ(fence_err, 0);
    ASSERT_EQ(err, FDB_ERR_NOT_COMMITTED);
    /* A blind attempt is fenced only when its read version was taken
     * before the fence -- which is what the runner's pin guarantees. */
    err = premise_scenario(b, SHAPE_BLIND_UNPINNED, &fence_err);
    ASSERT_EQ(fence_err, 0);
    ASSERT_EQ(err, 0);
    err = premise_scenario(b, SHAPE_BLIND_PINNED, &fence_err);
    ASSERT_EQ(fence_err, 0);
    ASSERT_EQ(err, FDB_ERR_NOT_COMMITTED);

    /* Total order: two attempts on the anchor, the later one commits
     * first, the earlier one can no longer land. */
    premise_keys(b, &other, &witness, &anchor);
    ASSERT_EQ(premise_attempt(b->db, SHAPE_ANCHOR, &other, &witness, &anchor, &a1), 0);
    ASSERT_EQ(premise_attempt(b->db, SHAPE_ANCHOR, &other, &witness, &anchor, &a2), 0);
    ASSERT_EQ(real_wait_destroy(fdb_transaction_commit(a2)), 0);
    ASSERT_EQ(real_wait_destroy(fdb_transaction_commit(a1)), FDB_ERR_NOT_COMMITTED);
    fdb_transaction_destroy(a1);
    fdb_transaction_destroy(a2);
}

/* -----------------------------------------------------------------------
 * Client-side counters against the real cluster: the fdb authority
 * table populates backend_client_stats from the runner (attempts,
 * commits, retries) and the process-wide network-wait counter.  A
 * read-only body with one point read blocks at least once (the read
 * cannot be ready before the storage server answered) and commits
 * nothing; a mutating body adds exactly one commit and at least one
 * more wait (the commit).  Counted only through the real table: the
 * mock-driven tests above must leave the wait counter untouched.
 * ----------------------------------------------------------------------- */

static void test_real_client_stats(void)
{
    struct fdb_backend *b = g_real_cat->backend_private;
    struct mds_cat_backend_client_stats s0;
    struct mds_cat_backend_client_stats s1;
    struct mds_cat_backend_client_stats s2;
    struct real_ctx c;
    uint64_t waits_before_mock;
    struct fdb_backend mock_b;
    struct body_ctx mock_c = { 0, 0, MDS_OK };

    ASSERT_TRUE(b != NULL);
    ASSERT_EQ(mds_cat_backend_client_stats(NULL, &s0), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_backend_client_stats(g_real_cat, NULL), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_backend_client_stats(g_real_cat, &s0), MDS_OK);
    ASSERT_EQ(s0.client_objects, 1);
    ASSERT_EQ(s0.exec_waits, fdb_txn_net_waits());

    memset(&c, 0, sizeof(c));
    c.b = b;
    fdb_key_init(&c.key, &b->prefix, FDB_KT_META);
    fdb_key_u8(&c.key, 0x7E); /* a scratch META sub-key */
    ASSERT_EQ(fdb_run_txn(b, FDB_TXN_READONLY, "stats_get", real_get_body, &c), MDS_OK);
    ASSERT_EQ(mds_cat_backend_client_stats(g_real_cat, &s1), MDS_OK);
    ASSERT_EQ(s1.txn_started - s0.txn_started, 1);
    ASSERT_EQ(s1.txn_committed, s0.txn_committed);
    ASSERT_EQ(s1.txn_closed - s0.txn_closed, 1);
    ASSERT_TRUE(s1.exec_waits - s0.exec_waits >= 1);

    c.value = 0x5A5A;
    ASSERT_EQ(fdb_run_txn(b, FDB_TXN_MUTATING, "stats_set", real_set_body, &c), MDS_OK);
    ASSERT_EQ(mds_cat_backend_client_stats(g_real_cat, &s2), MDS_OK);
    ASSERT_EQ(s2.txn_started - s1.txn_started, 1);
    ASSERT_EQ(s2.txn_committed - s1.txn_committed, 1);
    ASSERT_EQ(s2.txn_closed, s1.txn_closed);
    ASSERT_TRUE(s2.exec_waits - s1.exec_waits >= 1);
    ASSERT_TRUE(s2.txn_aborted >= s1.txn_aborted);

    /* A mock-driven run has no network: the wait counter stays put. */
    waits_before_mock = fdb_txn_net_waits();
    mock_reset();
    backend_init(&mock_b);
    ASSERT_EQ(fdb_run_txn(&mock_b, FDB_TXN_MUTATING, "t", body_fn, &mock_c), MDS_OK);
    ASSERT_EQ(fdb_txn_net_waits(), waits_before_mock);
}

/* ----------------------------------------------------------------------- */

static int real_open(void)
{
    struct mds_config *cfg;
    const char *cluster = getenv("FDB_CLUSTER_FILE");
    enum mds_status st;

    if (cluster == NULL || cluster[0] == '\0') {
        cluster = "/etc/foundationdb/fdb.cluster";
    }
    if (access(cluster, R_OK) != 0) {
        return -1;
    }
    cfg = calloc(1, sizeof(*cfg));
    if (cfg == NULL) {
        return -1;
    }
    cfg->catalogue_backend = MDS_BACKEND_FDB;
    cfg->self.id = 3;
    (void)snprintf(cfg->fdb_key_prefix, sizeof(cfg->fdb_key_prefix), "tt-%ld",
                   (long)getpid());
    st = mds_catalogue_open(cfg, &g_real_cat);
    free(cfg);
    if (st != MDS_OK) {
        return -1;
    }
    if (catalogue_fdb_keyspace_clear(g_real_cat) != MDS_OK ||
        mds_catalogue_bootstrap(g_real_cat) != MDS_OK) {
        mds_catalogue_close(g_real_cat);
        g_real_cat = NULL;
        return -1;
    }
    return 0;
}

int main(void)
{
    printf("test_fdb_txn:\n");
    if (real_open() != 0) {
        printf("SKIP: FoundationDB cluster not reachable\n");
        mds_catalogue_process_shutdown();
        return 77;
    }

    RUN_TEST(test_commit_ok);
    RUN_TEST(test_unknown_result_landed);
    RUN_TEST(test_unknown_result_not_landed);
    RUN_TEST(test_timed_out_landed);
    RUN_TEST(test_timed_out_fenced);
    RUN_TEST(test_deadline_indoubt);
    RUN_TEST(test_deadline_delay);
    RUN_TEST(test_retry_classification);
    RUN_TEST(test_readonly_and_done);
    RUN_TEST(test_witness_key_shape);
    RUN_TEST(test_inflight_object_replaced);
    RUN_TEST(test_cancelled_and_version_changed);
    RUN_TEST(test_landed_then_probes_fail);
    RUN_TEST(test_fence_round_unknown_result);
    RUN_TEST(test_read_version_pinned);
    RUN_TEST(test_alloc_id_unresolved_refill);
    RUN_TEST(test_real_cluster);
    RUN_TEST(test_real_fence_premise);
    RUN_TEST(test_real_client_stats);

    (void)catalogue_fdb_keyspace_clear(g_real_cat);
    mds_catalogue_close(g_real_cat);
    g_real_cat = NULL;
    mds_catalogue_process_shutdown();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}

#else /* !HAVE_FDB */

int main(void)
{
    printf("test_fdb_txn: SKIP (fdb backend not built)\n");
    return 77;
}

#endif

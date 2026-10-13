/*
 * fdb_bench.c -- FoundationDB micro-benchmarks for the pnfs-lattice
 * metadata-backend feasibility study (experiments A, B, D of the plan).
 *
 * Key layout (mirrors the Lattice catalogue responsibility domains):
 *   P|<parent_hash8 BE><parent_seq BE>      -> u64 parent_counter (nlink-ish)
 *   P|...|D|<name...>                        -> 8B child_fileid   (dirent)
 *   I|<fileid BE>                            -> 137B inode blob
 *   C|counter                                -> u64 global fileid counter
 *
 * Modes:
 *   micro     single-key set+commit latency (P50/P95/P99/P99.9), 1 thread
 *   create    N threads create entries inside ONE directory
 *             (contended parent nlink RMW + contended global counter RMW)
 *   unlink    N threads unlink entries inside ONE directory
 *   counter   N threads bump one shared key via atomic ADD
 *             (queue-head model, no conflicts expected)
 *
 * Targets hypotheses H-01 (atomic RMW / CAS retry bound), H-02 (queue-head
 * hotspot) and H-03 (commit latency on hot path).
 *
 * Build:  gcc -O2 -o fdb_bench fdb_bench.c -lfdb_c -lm
 * Run:    ./fdb_bench <micro|create|unlink|counter> [threads] [duration_s]
 */
#define FDB_API_VERSION FDB_LATEST_API_VERSION
#include <foundationdb/fdb_c.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>
#include <math.h>

static double now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e6 + ts.tv_nsec / 1e3;
}

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y ? 1 : 0;
}

/* ---------------- key builders ---------------- */
static void put_be64(uint8_t *p, uint64_t v)
{
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (56 - i * 8));
}
static uint64_t get_be64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v = (v << 8) | p[i];
    return v;
}

#define PARENT_HASH 0x424C414E444D4551ULL /* "BLANDEMQ" -- bench keyspace */

/* parent key:  'P' hash(8) seq(8)            -> 17 bytes */
/* dirent key:  'P' hash(8) seq(8) 'D' name   -> 18+len */
/* inode key:   'I' fileid(8)                 -> 9 bytes */
/* counter:     "Cc"                          -> 2 bytes */

static void parent_key(uint8_t *out, uint64_t seq)
{
    out[0] = 'P';
    put_be64(out + 1, PARENT_HASH);
    put_be64(out + 9, seq);
}
static void dirent_key(uint8_t *out, uint64_t seq, const char *name, size_t nlen)
{
    parent_key(out, seq);
    out[17] = 'D';
    memcpy(out + 18, name, nlen);
}
static void inode_key(uint8_t *out, uint64_t fileid)
{
    out[0] = 'I';
    put_be64(out + 1, fileid);
}

/* ---------------- transaction helpers (correct 7.4 C API) ---------------- */
static fdb_error_t txn_commit_block(FDBTransaction *tr)
{
    FDBFuture *f = fdb_transaction_commit(tr);
    fdb_future_block_until_ready(f);
    fdb_error_t err = fdb_future_get_error(f);
    fdb_future_destroy(f);
    return err;
}

/* read one key; returns 1 if present (fills buf), 0 if absent, -1 on error.
 * buf must hold >= bufsz bytes; vlen receives actual length. */
static int txn_get(FDBTransaction *tr, const uint8_t *key, uint32_t klen,
                   uint8_t *buf, uint32_t bufsz, uint32_t *vlen)
{
    FDBFuture *f = fdb_transaction_get(tr, key, klen, 0);
    fdb_future_block_until_ready(f);
    fdb_error_t err = fdb_future_get_error(f);
    if (err) { fdb_future_destroy(f); return -1; }
    fdb_bool_t present;
    uint8_t const *val = NULL;
    int vlen_int = 0;
    err = fdb_future_get_value(f, &present, &val, &vlen_int);
    fdb_future_destroy(f);
    if (err) return -1;
    if (!present || val == NULL || vlen_int == 0) return 0;
    if ((uint32_t)vlen_int <= bufsz) memcpy(buf, val, (size_t)vlen_int);
    if (vlen) *vlen = (uint32_t)vlen_int;
    return 1;
}

/* ---------------- worker state & ops ---------------- */
typedef struct {
    FDBDatabase *db;
    int op;              /* 0=create, 1=unlink, 2=counter-bump */
    uint64_t parent_seq;
    char name[24];
    uint64_t ok;
    uint64_t conflicts;
    uint64_t retries;
    uint64_t latency_hist[300000]; /* 1us buckets up to 300ms, then overflow */
    uint64_t overflow;
    volatile int *stop;
} worker_t;

static void hist_add(worker_t *w, double us)
{
    uint64_t b = (uint64_t)us;
    if (b >= 300000) w->overflow++;
    else w->latency_hist[b]++;
}

/* do_create: bump global fileid counter (plain RMW), bump parent nlink
 * (plain RMW), write dirent + 137B inode.  Two RMWs on hot keys = H-01. */
static void do_create(FDBTransaction *tr, worker_t *w)
{
    uint8_t dk[18 + sizeof(w->name)];
    uint8_t pk[17];
    uint8_t ic[9];
    uint8_t fileid_buf[8];
    dirent_key(dk, w->parent_seq, w->name, strlen(w->name));
    parent_key(pk, w->parent_seq);

    /* 1) global fileid counter: read, +1, write (plain RMW) */
    uint8_t curbuf[8];
    if (txn_get(tr, (uint8_t *)"Cc", 2, curbuf, sizeof(curbuf), NULL) > 0)
        put_be64(fileid_buf, get_be64(curbuf) + 1);
    else
        put_be64(fileid_buf, 1);
    fdb_transaction_set(tr, (uint8_t *)"Cc", 2, fileid_buf, 8);

    /* 2) parent nlink RMW */
    uint8_t nl[8];
    if (txn_get(tr, pk, 17, nl, sizeof(nl), NULL) > 0)
        put_be64(nl, get_be64(nl) + 1);
    else
        put_be64(nl, 1);
    fdb_transaction_set(tr, pk, 17, nl, 8);

    /* 3) dirent + inode */
    fdb_transaction_set(tr, dk, 18 + (uint32_t)strlen(w->name), fileid_buf, 8);
    uint64_t fid = get_be64(fileid_buf);
    inode_key(ic, fid);
    uint8_t inb[137];
    memset(inb, 0, sizeof(inb));
    put_be64(inb, fid);
    fdb_transaction_set(tr, ic, 9, inb, 137);
}

/* do_unlink: fetch child fileid, delete dirent + inode, decrement nlink. */
static void do_unlink(FDBTransaction *tr, worker_t *w)
{
    uint8_t dk[18 + sizeof(w->name)];
    uint8_t pk[17];
    uint8_t ino[9];
    uint8_t fidbuf[8];
    dirent_key(dk, w->parent_seq, w->name, strlen(w->name));
    parent_key(pk, w->parent_seq);

    if (txn_get(tr, dk, 18 + (uint32_t)strlen(w->name), fidbuf, sizeof(fidbuf), NULL) <= 0) {
        /* absent or error: treat as conflict, skip */
        w->conflicts++;
        return;
    }
    uint64_t fid = get_be64(fidbuf);
    inode_key(ino, fid);
    fdb_transaction_clear(tr, dk, 18 + (uint32_t)strlen(w->name));
    fdb_transaction_clear(tr, ino, 9);

    uint8_t nl[8];
    if (txn_get(tr, pk, 17, nl, sizeof(nl), NULL) > 0) {
        uint64_t n = get_be64(nl);
        put_be64(nl, n > 0 ? n - 1 : 0);
    } else {
        put_be64(nl, 0);
    }
    fdb_transaction_set(tr, pk, 17, nl, 8);
}

/* do_bump: atomic ADD on the queue-head counter (H-02 target).
 * FDB ADD operand is LITTLE-endian 64-bit. */
static void do_bump(FDBTransaction *tr, worker_t *w)
{
    (void)w;
    uint8_t b[8] = { 1, 0, 0, 0, 0, 0, 0, 0 };
    fdb_transaction_atomic_op(tr, (uint8_t *)"Cc", 2, b, 8,
                              FDB_MUTATION_TYPE_ADD);
}

/* 7.4 conflict-retry: fdb_transaction_on_error(tr, err) returns a future that,
 * once blocked, applies the recommended backoff sleep internally. */
static void conflict_backoff(FDBTransaction *tr, fdb_error_t err)
{
    FDBFuture *f = fdb_transaction_on_error(tr, err);
    fdb_future_block_until_ready(f);
    fdb_future_destroy(f);
}

/* commit already-staged op with conflict retry.
 * On conflict: reset + re-stage + retry.  Returns retry count (0 = first try). */
static int commit_staged(FDBTransaction *tr, worker_t *w, double t0)
{
    int attempt = 0;
    for (;;) {
        fdb_error_t err = txn_commit_block(tr);
        if (err == 0) break;
        conflict_backoff(tr, err);
        fdb_transaction_reset(tr);
        if (++attempt > 500) { w->conflicts++; break; }
        /* re-stage on the reset transaction */
        if (w->op == 0) do_create(tr, w);
        else if (w->op == 1) do_unlink(tr, w);
        else do_bump(tr, w);
    }
    hist_add(w, now_us() - t0);
    w->ok++;
    return attempt;
}

static void *worker_main(void *arg)
{
    worker_t *w = arg;
    FDBTransaction *tr;
    (void)fdb_database_create_transaction(w->db, &tr);
    while (!*w->stop) {
        double t0 = now_us();
        if (w->op == 0) do_create(tr, w);
        else if (w->op == 1) do_unlink(tr, w);
        else do_bump(tr, w);
        int retried = commit_staged(tr, w, t0);
        if (retried) w->retries += (uint64_t)retried;
        (void)fdb_transaction_reset(tr);
        if (w->op == 0 || w->op == 1) {
            snprintf(w->name, sizeof(w->name), "e%llu",
                     (unsigned long long)(w->ok % 128));
        }
    }
    (void)fdb_transaction_destroy(tr);
    return NULL;
}

static void print_stats(const char *label, worker_t *ws, int n, double wall_us)
{
    uint64_t total = 0, conflicts = 0, overflow = 0, retries = 0;
    double sum = 0;
    uint64_t hist[300000] = {0};
    for (int i = 0; i < n; i++) {
        total += ws[i].ok;
        conflicts += ws[i].conflicts;
        retries += ws[i].retries;
        overflow += ws[i].overflow;
        for (uint64_t b = 0; b < 300000; b++) hist[b] += ws[i].latency_hist[b];
    }
    for (uint64_t b = 0; b < 300000; b++) sum += (double)b * (double)hist[b];
    double mean = total ? sum / total : 0;
    uint64_t acc = 0;
    double p50 = 0, p95 = 0, p99 = 0, p999 = 0, maxv = 0;
    for (uint64_t b = 0; b < 300000; b++) {
        if (hist[b]) maxv = b;
        acc += hist[b];
        if (p50 == 0 && acc >= total / 2) p50 = b;
        if (p95 == 0 && acc >= total * 95 / 100) p95 = b;
        if (p99 == 0 && acc >= total * 99 / 100) p99 = b;
        if (p999 == 0 && acc >= total * 999 / 1000) p999 = b;
    }
    double wall_s = wall_us / 1e6;
    printf("%-10s ops=%llu ops/s=%.0f  mean=%.1fus p50=%.0fus p95=%.0fus p99=%.0fus p999=%.0fus max=%.0fus conflicts=%llu retries=%llu overflow=%llu wall=%.2fs\n",
           label, (unsigned long long)total, total / wall_s, mean,
           p50, p95, p99, p999, maxv,
           (unsigned long long)conflicts, (unsigned long long)retries,
           (unsigned long long)overflow, wall_s);
    fflush(stdout);
}

/* ---------------- modes ---------------- */
static int mode_micro(FDBDatabase *db, int threads, int dur_s)
{
    (void)threads;
    FDBTransaction *tr;
    (void)fdb_database_create_transaction(db, &tr);
    uint8_t k[9];
    inode_key(k, 0xAA);
    static uint8_t val[8];
    put_be64(val, 1);
    double *lat = malloc(sizeof(double) * (1 << 20));
    double t_start = now_us();
    uint64_t n = 0, conf = 0;
    while (now_us() - t_start < dur_s * 1e6) {
        double t0 = now_us();
        fdb_transaction_set(tr, k, 9, val, 8);
        fdb_error_t err;
        int attempt = 0;
        for (;;) {
            FDBFuture *f = fdb_transaction_commit(tr);
            fdb_future_block_until_ready(f);
            err = fdb_future_get_error(f);
            fdb_future_destroy(f);
            if (err == 0) break;
            conflict_backoff(tr, err);
            fdb_transaction_reset(tr);
            if (++attempt > 500) { conf++; break; }
            fdb_transaction_set(tr, k, 9, val, 8);
        }
        lat[n & ((1 << 20) - 1)] = now_us() - t0;
        n++;
        (void)fdb_transaction_reset(tr);
    }
    double wall = (now_us() - t_start) / 1e6;
    uint64_t cnt = n < (1 << 20) ? n : (1 << 20);
    uint64_t *s = malloc(sizeof(uint64_t) * cnt);
    for (uint64_t i = 0; i < cnt; i++) s[i] = (uint64_t)lat[i];
    qsort(s, cnt, sizeof(uint64_t), cmp_u64);
    double p50 = s[cnt / 2], p95 = s[cnt * 95 / 100], p99 = s[cnt * 99 / 100];
    double p999 = cnt * 999 >= 1000 ? s[cnt * 999 / 1000] : s[cnt - 1];
    double mx = s[cnt - 1];
    printf("micro      ops=%llu ops/s=%.0f  p50=%.0fus p95=%.0fus p99=%.0fus p999=%.0fus max=%.0fus conflicts=%llu wall=%.2fs\n",
           (unsigned long long)n, n / wall, p50, p95, p99, p999, mx,
           (unsigned long long)conf, wall);
    fflush(stdout);
    free(lat);
    free(s);
    (void)fdb_transaction_destroy(tr);
    return 0;
}

/* blocking FDB event loop, runs in its own thread (7.4 pattern from the
 * official C test client) */
static void *run_network_thread(void *arg)
{
    (void)arg;
    fdb_error_t e = fdb_run_network();
    if (e) fprintf(stderr, "run_network: %s\n", fdb_get_error(e));
    return NULL;
}

static int mode_threads(FDBDatabase *db, int threads, int dur_s, int op)
{
    worker_t *ws = calloc(threads, sizeof(worker_t));
    pthread_t *ts = malloc(sizeof(pthread_t) * threads);
    static int stop;
    stop = 0;
    for (int i = 0; i < threads; i++) {
        ws[i].db = db;
        ws[i].op = op;
        ws[i].parent_seq = 77;
        ws[i].stop = &stop;
        snprintf(ws[i].name, sizeof(ws[i].name), "t%d", i);
        pthread_create(&ts[i], NULL, worker_main, &ws[i]);
    }
    double t0 = now_us();
    sleep(dur_s);
    stop = 1;
    for (int i = 0; i < threads; i++) pthread_join(ts[i], NULL);
    const char *label = op == 0 ? "hotspot-CR" : op == 1 ? "hotspot-UL" : "counter";
    print_stats(label, ws, threads, now_us() - t0);
    free(ws); free(ts);
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "usage: %s <micro|create|unlink|counter> [threads] [dur_s]\n", argv[0]);
        return 2;
    }
    /* 7.4 bootstrap: setup_network (sync) -> run network in a thread (blocking
     * event loop) -> create_database (sync, reads cluster file or $FDB_CLUSTER_FILE) */
    const char *cf = getenv("FDB_CLUSTER_FILE");
    if (!cf || !*cf) cf = "/etc/foundationdb/fdb.cluster";
    fdb_error_t e;
    e = fdb_select_api_version(FDB_API_VERSION);
    if (e) { fprintf(stderr, "select_api_version: %s\n", fdb_get_error(e)); return 1; }
    e = fdb_setup_network();
    if (e) { fprintf(stderr, "setup_network: %s\n", fdb_get_error(e)); return 1; }
    pthread_t net;
    pthread_create(&net, NULL, run_network_thread, NULL);
    FDBDatabase *db;
    e = fdb_create_database(cf, &db);
    if (e) { fprintf(stderr, "create_database: %s\n", fdb_get_error(e)); return 1; }
    int threads = argc > 2 ? atoi(argv[2]) : 1;
    int dur = argc > 3 ? atoi(argv[3]) : 10;
    const char *mode = argv[1];
    int rc = 2;
    if (!strcmp(mode, "micro")) rc = mode_micro(db, threads, dur);
    else if (!strcmp(mode, "create")) rc = mode_threads(db, threads, dur, 0);
    else if (!strcmp(mode, "unlink")) rc = mode_threads(db, threads, dur, 1);
    else if (!strcmp(mode, "counter")) rc = mode_threads(db, threads, dur, 2);
    else fprintf(stderr, "unknown mode\n");
    fdb_database_destroy(db);
    /* fdb_stop_network lets the run_network thread's event loop exit cleanly;
     * pthread_cancel on a thread inside the FDB loop aborts the process. */
    (void)fdb_stop_network();
    (void)pthread_join(net, NULL);
    return rc;
}

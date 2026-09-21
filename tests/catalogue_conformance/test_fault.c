/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_fault.c -- Phase 6c: FoundationDB fault injection and lifecycle.
 *
 * Runs only against the FoundationDB backend (CATALOGUE_TEST_BACKEND=fdb;
 * anything else exits 77).  Every sub-test prints PASS, FAIL or SKIP and
 * the process exits non-zero when any sub-test fails.  Each sub-test
 * uses an isolated key prefix (the harness's per-process prefix, or a
 * private one it clears itself) and a scratch directory.
 *
 * Sub-tests (the premise each one verifies rather than assumes):
 *   scripted_unknown_result  1021 after a landed commit -> the slot
 *                            returns OK, effect once, body not re-run;
 *                            1021 with nothing landed -> body re-run,
 *                            effect once
 *   scripted_timed_out       1031 after a landed commit -> OK once;
 *                            1031 while the commit is in flight and
 *                            lands AFTER the probe's read version ->
 *                            the fence conflicts, the re-probe proves the
 *                            landing, OK once, no re-run; 1031 with the
 *                            commit held back -> the fence commits, and
 *                            committing the held-back attempt afterwards
 *                            fails with 1020 (the fence premise), body
 *                            re-run, effect once
 *   scripted_fence_exhaustion the fence itself fails until the deadline
 *                            -> MDS_ERR_INDOUBT, no replay, nothing
 *                            published, ns_create_wide safe_to_discard
 *                            == false
 *   scripted_probe_exhaustion commit landed, every probe fails until the
 *                            deadline -> INDOUBT, exactly one effect
 *   size_bounds              MDS_MAX_NAME works, MDS_MAX_NAME + 1 is
 *                            refused before any store interaction; a
 *                            wide create with MDS_MAX_STRIPES x
 *                            MDS_MAX_MIRRORS rows commits in one
 *                            transaction and is purged by remove
 *   schema_refusal           a prefix stamped with another schema
 *                            version is refused with MDS_ERR_INVAL and
 *                            its catalogue rows are untouched
 *   witness_restart          a new process incarnation of the same mds_id
 *                            range-clears WITNESS + mds_id + [0, stamp)
 *                            and works; a lower stamp leaves higher keys
 *                            alone (documented clock-step behaviour)
 *   keyspace_clear_witness   catalogue_fdb_keyspace_clear wipes every
 *                            catalogue row but leaves the WITNESS table
 *                            (the live fence anchors) in place
 *   buggify_mixed            2000 mixed mutations (create, remove, rename,
 *                            rename-over, link, setattr, parent_touch)
 *                            under client buggify: every status is
 *                            consistent with the store, never a partial
 *                            or double effect
 *   buggify_concurrent       8 threads creating distinct names for 30 s
 *                            under client buggify: every OK'd name once,
 *                            parent counters consistent
 *   lifecycle                4 threads x 50 open/close cycles over two
 *                            prefixes, then mds_catalogue_process_shutdown:
 *                            the network thread is joined (the task count
 *                            is back to what it was before the first open)
 *
 * Process model.  The fdb_c network options must be set before the
 * process's client network starts, and the harness process has it
 * running from its first open.  Sub-tests that need buggify or a fresh
 * incarnation therefore re-execute this binary with a "--child-<mode>"
 * argument.  The binary is found through argv[0] when that is a path
 * (ctest and the shell hand one over) and through /proc/self/exe
 * otherwise; the former keeps the children real under valgrind, where
 * /proc/self/exe is the memcheck tool, not this test.  The child's
 * environment is assembled BEFORE fork (the parent is multi-threaded;
 * the child only calls execve): CATALOGUE_TEST_KEY_PREFIX is stripped so
 * a child never clears the parent's keyspace, and the buggify children
 * get
 *
 *   FDB_NETWORK_OPTION_CLIENT_BUGGIFY_ENABLE=                  (option 80)
 *   FDB_NETWORK_OPTION_CLIENT_BUGGIFY_SECTION_ACTIVATED_PROBABILITY=100
 *                                                              (option 82)
 *   FDB_NETWORK_OPTION_CLIENT_BUGGIFY_SECTION_FIRED_PROBABILITY=25
 *                                                              (option 83)
 *
 * which libfdb_c 7.3 applies in fdb_setup_network (every network option
 * can be set through FDB_NETWORK_OPTION_<NAME>).  FAULT_BUGGIFY_FIRED
 * overrides the fired probability (1..100).  Under client buggify the
 * 7.3 client injects, at the commit: 1020 / 1007 / 1042 / 1078 / 1021
 * before the request is sent (nothing landed), 1021 after a successful
 * commit reply (landed), and -- when a transaction timeout is set, which
 * the runner always does -- 1031 returned immediately while the real
 * commit is issued after a random 0..5 s delay (the in-flight commit
 * the fence must handle); reads see 1007 / 1009 with low probability.
 * 1025 and 1039 are not injected by the client; their classification is
 * pinned by tests/unit/test_fdb_txn.c.
 *
 * Scripted cases use the runner's injectable call table (fdb_txn.h): a
 * thin table over the real fdb_c that executes or withholds the real
 * commit and reports a scripted error, so the outcome does not depend
 * on cluster timing.
 */

#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "harness.h"

#ifdef HAVE_FDB

#include "catalogue_fdb.h"
#include "catalogue_internal.h"
#include "fdb_codec.h"
#include "fdb_keys.h"
#include "fdb_txn.h"

extern char **environ;

enum result {
    R_PASS = 0,
    R_FAIL = 1,
    R_SKIP = 2,
};

/* Per-sub-test context: the first failure reason is kept; later ones
 * are counted so the log stays readable. */
struct ctx {
    struct mds_catalogue *cat;
    uint64_t dir;
    enum result res;
    unsigned extra_failures;
    char why[256];
};

static void ctx_fail(struct ctx *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

static void ctx_fail(struct ctx *c, const char *fmt, ...)
{
    va_list ap;

    if (c->res == R_FAIL) {
        c->extra_failures++;
        return;
    }
    c->res = R_FAIL;
    va_start(ap, fmt);
    (void)vsnprintf(c->why, sizeof(c->why), fmt, ap);
    va_end(ap);
}

static void ctx_skip(struct ctx *c, const char *why)
{
    if (c->res == R_PASS) {
        c->res = R_SKIP;
        (void)snprintf(c->why, sizeof(c->why), "%s", why);
    }
}

static const char *status_name(enum mds_status st)
{
    switch (st) {
    case MDS_OK:            return "OK";
    case MDS_ERR_NOMEM:     return "NOMEM";
    case MDS_ERR_IO:        return "IO";
    case MDS_ERR_NOTFOUND:  return "NOTFOUND";
    case MDS_ERR_EXISTS:    return "EXISTS";
    case MDS_ERR_INVAL:     return "INVAL";
    case MDS_ERR_STALE:     return "STALE";
    case MDS_ERR_DELAY:     return "DELAY";
    case MDS_ERR_NOTEMPTY:  return "NOTEMPTY";
    case MDS_ERR_ISDIR:     return "ISDIR";
    case MDS_ERR_NOTDIR:    return "NOTDIR";
    case MDS_ERR_NOSPC:     return "NOSPC";
    case MDS_ERR_NOSUPPORT: return "NOSUPPORT";
    case MDS_ERR_INDOUBT:   return "INDOUBT";
    default:                return "other";
    }
}

static void sleep_ms(unsigned ms)
{
    struct timespec ts;

    ts.tv_sec = (time_t)(ms / 1000U);
    ts.tv_nsec = (long)(ms % 1000U) * 1000000L;
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
        /* resume the remainder */
    }
}

static uint64_t monotonic_ms(void)
{
    struct timespec ts;

    (void)clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ULL + (uint64_t)ts.tv_nsec / 1000000ULL;
}

/* xorshift32: deterministic operation mix, reproducible from the seed. */
static uint32_t prng_next(uint32_t *s)
{
    uint32_t x = *s;

    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *s = x;
    return x;
}

/* Bounded, quiescence after which a commit that was in flight when its
 * caller gave up has either landed or can never land: the client's own
 * in-flight simulation delays at most 5 s and a commit older than the
 * cluster's write-transaction life (5 s of versions) is rejected. */
#define QUIESCENCE_MS 6000U

/* -----------------------------------------------------------------------
 * Backend access (test-only: the conformance binaries link the backend)
 * ----------------------------------------------------------------------- */

static struct fdb_backend *be_of(const struct mds_catalogue *cat)
{
    if (cat == NULL || cat->backend != MDS_BACKEND_FDB) {
        return NULL;
    }
    return cat->backend_private;
}

/* Plain copy of the relaxed atomics, for before/after deltas. */
struct stats_snap {
    uint64_t attempts;
    uint64_t commits;
    uint64_t retries;
    uint64_t unknown_results;
    uint64_t unknown_landed;
    uint64_t fences;
    uint64_t fence_landed;
    uint64_t fenced_reruns;
    uint64_t indoubt;
    uint64_t delay_exhausted;
    uint64_t io_errors;
};

static void stats_snapshot(const struct fdb_backend *b, struct stats_snap *s)
{
    s->attempts = atomic_load(&b->stats.attempts);
    s->commits = atomic_load(&b->stats.commits);
    s->retries = atomic_load(&b->stats.retries);
    s->unknown_results = atomic_load(&b->stats.unknown_results);
    s->unknown_landed = atomic_load(&b->stats.unknown_landed);
    s->fences = atomic_load(&b->stats.fences);
    s->fence_landed = atomic_load(&b->stats.fence_landed);
    s->fenced_reruns = atomic_load(&b->stats.fenced_reruns);
    s->indoubt = atomic_load(&b->stats.indoubt);
    s->delay_exhausted = atomic_load(&b->stats.delay_exhausted);
    s->io_errors = atomic_load(&b->stats.io_errors);
}

/* 1021 = commit_unknown_result outcomes (of which landed); 1031-class =
 * timed-out / cancelled / version-changed commits resolved by a fence
 * (rounds include fences re-tried after a conflict), split into landings
 * proven by the probe and attempts proven not landed and re-run. */
static void stats_print(const char *indent, const struct stats_snap *s)
{
    (void)printf("%sruntime: attempts %llu, commits %llu, retries %llu, 1021 %llu "
                 "(landed %llu), 1031-class: fence rounds %llu, landed %llu, fenced+re-run "
                 "%llu; indoubt %llu, delay %llu, io %llu\n", indent,
                 (unsigned long long)s->attempts, (unsigned long long)s->commits,
                 (unsigned long long)s->retries, (unsigned long long)s->unknown_results,
                 (unsigned long long)s->unknown_landed, (unsigned long long)s->fences,
                 (unsigned long long)s->fence_landed, (unsigned long long)s->fenced_reruns,
                 (unsigned long long)s->indoubt, (unsigned long long)s->delay_exhausted,
                 (unsigned long long)s->io_errors);
}

/* Raw key-space inspection through the runner (read-only bodies). */
#define RAW_MAX 64

struct raw_entry {
    uint32_t klen;
    uint32_t vlen;
    uint8_t  key[FDB_KEY_MAX];
    uint8_t  val[16];
};

struct raw_scan {
    struct fdb_backend  *b;
    struct fdb_key_range r;
    int                  limit;
    /* Entries whose key byte at type_off equals skip_type are counted
     * in skipped and left out of count / hash (-1: none). */
    uint32_t             type_off;
    int                  skip_type;
    /* Out. */
    int                  count;
    int                  skipped;
    bool                 more;
    uint64_t             hash;
    struct raw_entry    *ent;    /**< RAW_MAX entries, or NULL. */
};

static uint64_t fnv1a(uint64_t h, const uint8_t *p, size_t n)
{
    size_t i;

    for (i = 0; i < n; i++) {
        h ^= p[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static int raw_scan_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct raw_scan *s = arg;
    const FDBKeyValue *kvs = NULL;
    FDBFuture *f;
    int n = 0;
    int i;
    fdb_error_t err;

    s->count = 0;
    s->skipped = 0;
    s->more = false;
    s->hash = 14695981039346656037ULL;
    f = fdb_txn_get_range_start(tr, &s->r, s->limit, false, false);
    err = fdb_txn_get_range_wait(f, &kvs, &n, &s->more);
    if (err != 0) {
        if (f != NULL) {
            fdb_future_destroy(f);
        }
        return (int)err;
    }
    for (i = 0; i < n; i++) {
        FDBKeyValue kv;

        fdb_kv_at(kvs, i, &kv);
        if (s->skip_type >= 0 && kv.key_length > (int)s->type_off &&
            kv.key[s->type_off] == (uint8_t)s->skip_type) {
            s->skipped++;
            continue;
        }
        s->hash = fnv1a(s->hash, kv.key, (size_t)kv.key_length);
        s->hash = fnv1a(s->hash, kv.value, (size_t)kv.value_length);
        if (s->ent != NULL && s->count < RAW_MAX) {
            struct raw_entry *e = &s->ent[s->count];
            size_t vl = (size_t)kv.value_length;

            memset(e, 0, sizeof(*e));
            e->klen = (uint32_t)kv.key_length;
            e->vlen = (uint32_t)kv.value_length;
            if (e->klen <= FDB_KEY_MAX) {
                memcpy(e->key, kv.key, e->klen);
            }
            memcpy(e->val, kv.value, vl > sizeof(e->val) ? sizeof(e->val) : vl);
        }
        s->count++;
    }
    fdb_future_destroy(f);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Scan every key starting with @p prefix_key. */
static enum mds_status raw_scan_prefix(struct fdb_backend *b, const struct fdb_key *prefix_key,
                                       int limit, struct raw_scan *s)
{
    if (!fdb_key_range_prefix(&s->r, prefix_key)) {
        return MDS_ERR_INVAL;
    }
    s->b = b;
    s->limit = limit;
    return fdb_run_txn(b, FDB_TXN_READONLY, "fault_raw_scan", raw_scan_body, s);
}

struct raw_get {
    struct fdb_key k;
    bool           found;
    uint64_t       v;
    size_t         len;
};

static int raw_get_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct raw_get *g = arg;
    uint8_t buf[FDB_INODE_ENC_MAX];
    fdb_error_t err;

    g->v = 0;
    err = fdb_txn_get(tr, &g->k, false, buf, sizeof(buf), &g->len, &g->found);
    if (err != 0) {
        return (int)err;
    }
    if (g->found && g->len == 8) {
        g->v = fdb_le64_get(buf);
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

struct raw_put {
    struct fdb_key k;
    uint64_t       v;
};

static int raw_put_le64_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct raw_put *p = arg;

    fdb_txn_set_le64(tr, &p->k, p->v);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

struct raw_clear {
    struct fdb_key_range r;
};

static int raw_clear_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct raw_clear *c = arg;

    fdb_txn_clear_range(tr, &c->r);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Prefix object for a byte-string prefix (private key spaces). */
static bool prefix_from_string(struct fdb_key_prefix *p, const char *s)
{
    size_t n = strnlen(s, FDB_KEY_PREFIX_MAX + 1U);

    if (n == 0 || n > FDB_KEY_PREFIX_MAX) {
        return false;
    }
    memset(p, 0, sizeof(*p));
    memcpy(p->bytes, s, n);
    p->len = (uint32_t)n;
    return true;
}

/* The range covering every key of a prefix (key = the bare prefix). */
static void key_of_prefix(struct fdb_key *k, const struct fdb_key_prefix *p)
{
    k->len = p->len;
    k->overflow = false;
    memcpy(k->buf, p->bytes, p->len);
}

/* -----------------------------------------------------------------------
 * Private handles (own prefix, own mds_id, own deadline)
 * ----------------------------------------------------------------------- */

static enum mds_status open_private(const char *prefix, uint32_t mds_id, uint32_t deadline_ms,
                                    struct mds_catalogue **out)
{
    struct mds_config *cfg;
    /* NOLINTNEXTLINE(concurrency-mt-unsafe) */
    const char *cluster = getenv("FDB_CLUSTER_FILE");
    enum mds_status st;

    *out = NULL;
    cfg = calloc(1, sizeof(*cfg));
    if (cfg == NULL) {
        return MDS_ERR_NOMEM;
    }
    cfg->catalogue_backend = MDS_BACKEND_FDB;
    if (cluster != NULL && cluster[0] != '\0') {
        (void)snprintf(cfg->fdb_cluster_file, sizeof(cfg->fdb_cluster_file), "%s", cluster);
    }
    (void)snprintf(cfg->fdb_key_prefix, sizeof(cfg->fdb_key_prefix), "%s", prefix);
    cfg->self.id = mds_id;
    cfg->cluster_size = 1;
    cfg->fdb_op_deadline_ms = deadline_ms;
    st = mds_catalogue_open(cfg, out);
    free(cfg);
    return st;
}

/* Open + bootstrap a private prefix (cleared first when @p wipe). */
static enum mds_status open_private_bootstrapped(const char *prefix, uint32_t mds_id, bool wipe,
                                                 struct mds_catalogue **out)
{
    enum mds_status st = open_private(prefix, mds_id, 0, out);

    if (st != MDS_OK) {
        return st;
    }
    if (wipe) {
        st = catalogue_fdb_keyspace_clear(*out);
    }
    if (st == MDS_OK) {
        st = mds_catalogue_bootstrap(*out);
    }
    if (st != MDS_OK) {
        mds_catalogue_close(*out);
        *out = NULL;
    }
    return st;
}

/* -----------------------------------------------------------------------
 * Child processes: re-execute this binary with "--child-<mode>"
 * ----------------------------------------------------------------------- */

static _Atomic pid_t g_child_pid;

#define CHILD_ENV_MAX 8

/* Build the child environment: the parent's, minus CATALOGUE_TEST_KEY_
 * PREFIX (a child must never share -- and clear -- the parent's prefix)
 * and minus any FDB_NETWORK_OPTION_* already present, plus @p extra
 * (writable buffers: execve takes char *const []). */
static char **child_envp(char *const *extra, int nextra)
{
    size_t n = 0;
    size_t i;
    size_t o = 0;
    char **envp;

    while (environ[n] != NULL) {
        n++;
    }
    envp = (char **)calloc(n + (size_t)nextra + 1U, sizeof(*envp));
    if (envp == NULL) {
        return NULL;
    }
    for (i = 0; i < n; i++) {
        if (strncmp(environ[i], "CATALOGUE_TEST_KEY_PREFIX=", 26) == 0 ||
            strncmp(environ[i], "FDB_NETWORK_OPTION_", 19) == 0) {
            continue;
        }
        envp[o++] = environ[i];
    }
    for (i = 0; i < (size_t)nextra; i++) {
        envp[o++] = extra[i];
    }
    envp[o] = NULL;
    return envp;
}

static char g_child_argv0[] = "conformance_fault";

/* Path this binary was started through (main's argv[0]); the child
 * exec target when it contains a '/'.  A bare name came from a PATH
 * search, which execve does not repeat: /proc/self/exe is used then. */
static const char *g_self_path = "/proc/self/exe";

static void self_path_set(const char *argv0)
{
    if (argv0 != NULL && strchr(argv0, '/') != NULL) {
        g_self_path = argv0;
    }
}

/*
 * Run "<self> --child-<mode> args..." with @p extra environment entries
 * and wait for it.  Returns the child's exit status (0..255), or -1 when
 * it could not be started or died from a signal.  The parent is
 * multi-threaded (client network thread), so the child does nothing
 * but execve between fork and exec.
 */
static int run_child(const char *mode, char *const *args, int nargs, char *const *extra_env,
                     int nextra)
{
    char modearg[64];
    char *argv[16];
    char **envp;
    pid_t pid;
    int status = 0;
    int i;
    int argc = 0;

    if (nargs > 12) {
        return -1;
    }
    (void)snprintf(modearg, sizeof(modearg), "--child-%s", mode);
    envp = child_envp(extra_env, nextra);
    if (envp == NULL) {
        return -1;
    }
    argv[argc++] = g_child_argv0;
    argv[argc++] = modearg;
    for (i = 0; i < nargs; i++) {
        argv[argc++] = args[i];
    }
    argv[argc] = NULL;
    (void)fflush(stdout);
    (void)fflush(stderr);
    pid = fork();
    if (pid < 0) {
        free((void *)envp);
        return -1;
    }
    if (pid == 0) {
        (void)execve(g_self_path, argv, envp);
        _exit(127);
    }
    atomic_store(&g_child_pid, pid);
    free((void *)envp);
    while (waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) {
            atomic_store(&g_child_pid, 0);
            return -1;
        }
    }
    atomic_store(&g_child_pid, 0);
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    return -1;
}

/* Suite watchdog: a hang anywhere becomes a FAIL, and a running child
 * is killed so ctest never waits for it. */
struct watchdog {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    bool done;
    unsigned seconds;
};

static const char *_Atomic g_running_subtest;

static void *watchdog_main(void *arg)
{
    struct watchdog *w = arg;
    struct timespec deadline;
    int rc = 0;

    (void)clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += (time_t)w->seconds;
    pthread_mutex_lock(&w->lock);
    while (!w->done && rc == 0) {
        rc = pthread_cond_timedwait(&w->cond, &w->lock, &deadline);
    }
    if (!w->done) {
        const char *sub = atomic_load(&g_running_subtest);
        pid_t child = atomic_load(&g_child_pid);

        pthread_mutex_unlock(&w->lock);
        if (child > 0) {
            (void)kill(child, SIGKILL);
        }
        (void)printf("  FAIL test_fault: no progress for %u s (stuck in %s)\n", w->seconds,
                     sub != NULL ? sub : "driver");
        (void)fflush(stdout);
        _exit(1);
    }
    pthread_mutex_unlock(&w->lock);
    return NULL;
}

static int watchdog_start(struct watchdog *w, pthread_t *thread, unsigned seconds)
{
    memset(w, 0, sizeof(*w));
    pthread_mutex_init(&w->lock, NULL);
    pthread_cond_init(&w->cond, NULL);
    w->seconds = seconds;
    return pthread_create(thread, NULL, watchdog_main, w);
}

static void watchdog_stop(struct watchdog *w, pthread_t thread)
{
    pthread_mutex_lock(&w->lock);
    w->done = true;
    pthread_cond_signal(&w->cond);
    pthread_mutex_unlock(&w->lock);
    (void)pthread_join(thread, NULL);
    pthread_cond_destroy(&w->cond);
    pthread_mutex_destroy(&w->lock);
}

/* -----------------------------------------------------------------------
 * Namespace helpers shared by the sub-tests
 * ----------------------------------------------------------------------- */

#define NAMES_MAX 256

struct name_page {
    uint32_t count;
    bool     overflow;
    uint64_t fileid[NAMES_MAX];
    uint8_t  type[NAMES_MAX];
    char     name[NAMES_MAX][MDS_MAX_NAME + 1];
};

static int name_page_cb(const struct mds_cat_dirent *entry, void *arg)
{
    struct name_page *p = arg;

    if (p->count >= NAMES_MAX) {
        p->overflow = true;
        return 1;
    }
    p->fileid[p->count] = entry->fileid;
    p->type[p->count] = entry->type;
    (void)snprintf(p->name[p->count], sizeof(p->name[0]), "%s", entry->name);
    p->count++;
    return 0;
}

static enum mds_status read_names(struct mds_catalogue *cat, uint64_t dir, struct name_page *p)
{
    memset(p, 0, sizeof(*p));
    return mds_cat_ns_readdir(cat, dir, NULL, 0, NULL, name_page_cb, p);
}

static unsigned name_count(const struct name_page *p, const char *name)
{
    unsigned i;
    unsigned n = 0;

    for (i = 0; i < p->count; i++) {
        if (strcmp(p->name[i], name) == 0) {
            n++;
        }
    }
    return n;
}

/* getattr with bounded retries: under fault injection a read-only
 * operation may exhaust its deadline (DELAY); that is not a namespace
 * fault, so a few attempts are made before it is reported. */
static enum mds_status getattr_retry(struct mds_catalogue *cat, uint64_t fileid,
                                     struct mds_inode *out)
{
    enum mds_status st = MDS_ERR_IO;
    int i;

    for (i = 0; i < 5; i++) {
        st = mds_cat_ns_getattr(cat, fileid, out);
        if (st == MDS_OK || st == MDS_ERR_NOTFOUND) {
            return st;
        }
        sleep_ms(50);
    }
    return st;
}

static enum mds_status lookup_retry(struct mds_catalogue *cat, uint64_t dir, const char *name,
                                    struct mds_inode *out)
{
    enum mds_status st = MDS_ERR_IO;
    int i;

    for (i = 0; i < 5; i++) {
        st = mds_cat_ns_lookup(cat, dir, name, out);
        if (st == MDS_OK || st == MDS_ERR_NOTFOUND) {
            return st;
        }
        sleep_ms(50);
    }
    return st;
}

/* -----------------------------------------------------------------------
 * Scripted call table over the real fdb_c
 *
 * Only the runner's own calls go through struct fdb_txn_calls (bodies
 * call fdb_c directly), so every transaction_set the table sees is the
 * runner's witness write (marking the attempt transaction), every WRITE
 * conflict range marks a fence transaction and every transaction_get
 * is a witness probe.  The commit of an armed attempt is executed or
 * withheld for real and reported as the scripted error through a fake
 * future; everything else is delegated.
 * ----------------------------------------------------------------------- */

enum fault_mode {
    FM_NONE = 0,
    FM_LANDED_1021,        /**< real commit, then report 1021 */
    FM_LOST_1021,          /**< no commit, report 1021 */
    FM_LANDED_1031,        /**< real commit, then report 1031 */
    FM_INFLIGHT_1031,      /**< report 1031; land the commit after the first
                            *   probe took its read version */
    FM_FENCED_1031,        /**< report 1031; commit the held-back attempt
                            *   when the runner drops it (expect 1020) */
    FM_FENCE_FAILS_1031,   /**< report 1031; every fence commit reports 1031 */
    FM_PROBE_FAILS_1021,   /**< real commit, report 1021; every probe reports
                            *   1031 */
};

#define WTR_MAX   32
#define FAKE_MAX  16

struct wtr {
    FDBTransaction *tr;
    int             sets;
    bool            write_cr;
};

struct fake_future {
    fdb_error_t err;
};

struct fault_script {
    enum fault_mode  mode;
    int              armed;            /**< Attempt commits still to fault. */
    FDBTransaction  *hostage;          /**< Attempt whose commit was withheld. */
    bool             hostage_committed;
    fdb_error_t      hostage_err;      /**< Result of the late real commit. */
    fdb_error_t      real_commit_err;  /**< Result of a real commit the script ran. */
    int              probes;
    int              fence_commits;
    int              attempt_faults;
    struct wtr       trs[WTR_MAX];
    struct fake_future *fakes[FAKE_MAX];
};

static struct fault_script g_fs;

static struct wtr *wtr_find(FDBTransaction *tr)
{
    int i;

    for (i = 0; i < WTR_MAX; i++) {
        if (g_fs.trs[i].tr == tr) {
            return &g_fs.trs[i];
        }
    }
    return NULL;
}

static struct fake_future *fake_find(FDBFuture *f)
{
    int i;

    for (i = 0; i < FAKE_MAX; i++) {
        if (g_fs.fakes[i] != NULL && (FDBFuture *)g_fs.fakes[i] == f) {
            return g_fs.fakes[i];
        }
    }
    return NULL;
}

static FDBFuture *fake_new(fdb_error_t err)
{
    int i;

    for (i = 0; i < FAKE_MAX; i++) {
        if (g_fs.fakes[i] == NULL) {
            g_fs.fakes[i] = calloc(1, sizeof(*g_fs.fakes[i]));
            if (g_fs.fakes[i] == NULL) {
                return NULL;
            }
            g_fs.fakes[i]->err = err;
            return (FDBFuture *)g_fs.fakes[i];
        }
    }
    return NULL;
}

static void fake_free(struct fake_future *ff)
{
    int i;

    for (i = 0; i < FAKE_MAX; i++) {
        if (g_fs.fakes[i] == ff) {
            g_fs.fakes[i] = NULL;
        }
    }
    free(ff);
}

/* Wait for a real commit future and return its error. */
static fdb_error_t real_commit_wait(FDBTransaction *tr)
{
    FDBFuture *cf = fdb_transaction_commit(tr);
    fdb_error_t err = fdb_txn_wait(cf);

    if (cf != NULL) {
        fdb_future_destroy(cf);
    }
    return err;
}

/* The held-back attempt is committed for real the moment the runner
 * lets go of its object: after the fence this must fail with 1020. */
static void hostage_release(FDBTransaction *tr)
{
    if (tr != g_fs.hostage || g_fs.hostage_committed) {
        return;
    }
    if (g_fs.mode == FM_FENCED_1031) {
        g_fs.hostage_err = real_commit_wait(tr);
        g_fs.hostage_committed = true;
    }
}

static fdb_error_t s_create_transaction(FDBDatabase *db, FDBTransaction **out)
{
    fdb_error_t err = fdb_database_create_transaction(db, out);
    struct wtr *w;

    if (err == 0) {
        w = wtr_find(NULL);
        if (w != NULL) {
            w->tr = *out;
            w->sets = 0;
            w->write_cr = false;
        }
    }
    return err;
}

static void s_transaction_destroy(FDBTransaction *tr)
{
    struct wtr *w = wtr_find(tr);

    hostage_release(tr);
    if (w != NULL) {
        memset(w, 0, sizeof(*w));
    }
    fdb_transaction_destroy(tr);
}

static void s_transaction_reset(FDBTransaction *tr)
{
    struct wtr *w = wtr_find(tr);

    hostage_release(tr);
    if (w != NULL) {
        w->sets = 0;
        w->write_cr = false;
    }
    fdb_transaction_reset(tr);
}

static void s_transaction_set(FDBTransaction *tr, const uint8_t *key, int key_len,
                              const uint8_t *value, int value_len)
{
    struct wtr *w = wtr_find(tr);

    if (w != NULL) {
        w->sets++;
    }
    fdb_transaction_set(tr, key, key_len, value, value_len);
}

/* A probe.  FM_INFLIGHT_1031: pin the probe's read version first, then
 * land the held-back commit, so the probe misses and the fence must
 * catch the landing through its conflict. */
static FDBFuture *s_transaction_get(FDBTransaction *tr, const uint8_t *key, int key_len,
                                    fdb_bool_t snapshot)
{
    g_fs.probes++;
    if (g_fs.mode == FM_PROBE_FAILS_1021) {
        return fake_new(FDB_ERR_TRANSACTION_TIMED_OUT);
    }
    if (g_fs.mode == FM_INFLIGHT_1031 && g_fs.hostage != NULL && !g_fs.hostage_committed) {
        FDBFuture *rv = fdb_transaction_get_read_version(tr);
        fdb_error_t err = fdb_txn_wait(rv);

        if (rv != NULL) {
            fdb_future_destroy(rv);
        }
        if (err == 0) {
            g_fs.hostage_err = real_commit_wait(g_fs.hostage);
            g_fs.hostage_committed = true;
        }
    }
    return fdb_transaction_get(tr, key, key_len, snapshot);
}

static fdb_error_t s_add_conflict_range(FDBTransaction *tr, const uint8_t *begin, int begin_len,
                                        const uint8_t *end, int end_len,
                                        FDBConflictRangeType type)
{
    struct wtr *w = wtr_find(tr);

    if (w != NULL && type == FDB_CONFLICT_RANGE_TYPE_WRITE) {
        w->write_cr = true;
    }
    return fdb_transaction_add_conflict_range(tr, begin, begin_len, end, end_len, type);
}

/* An armed attempt commit: execute or withhold the real commit and
 * report the scripted error. */
static FDBFuture *s_attempt_commit(FDBTransaction *tr)
{
    fdb_error_t report;

    g_fs.armed--;
    g_fs.attempt_faults++;
    switch (g_fs.mode) {
    case FM_LANDED_1021:
    case FM_PROBE_FAILS_1021:
        g_fs.real_commit_err = real_commit_wait(tr);
        report = (g_fs.real_commit_err == 0) ? FDB_ERR_COMMIT_UNKNOWN_RESULT :
                                                g_fs.real_commit_err;
        break;
    case FM_LANDED_1031:
        g_fs.real_commit_err = real_commit_wait(tr);
        report = (g_fs.real_commit_err == 0) ? FDB_ERR_TRANSACTION_TIMED_OUT :
                                                g_fs.real_commit_err;
        break;
    case FM_LOST_1021:
        report = FDB_ERR_COMMIT_UNKNOWN_RESULT;
        break;
    case FM_INFLIGHT_1031:
    case FM_FENCED_1031:
    case FM_FENCE_FAILS_1031:
        g_fs.hostage = tr;
        g_fs.hostage_committed = false;
        report = FDB_ERR_TRANSACTION_TIMED_OUT;
        break;
    default:
        return fdb_transaction_commit(tr);
    }
    return fake_new(report);
}

static FDBFuture *s_transaction_commit(FDBTransaction *tr)
{
    struct wtr *w = wtr_find(tr);

    if (w != NULL && w->sets > 0) {
        if (g_fs.armed > 0) {
            return s_attempt_commit(tr);
        }
        return fdb_transaction_commit(tr);
    }
    if (w != NULL && w->write_cr) {
        g_fs.fence_commits++;
        if (g_fs.mode == FM_FENCE_FAILS_1031) {
            return fake_new(FDB_ERR_TRANSACTION_TIMED_OUT);
        }
    }
    return fdb_transaction_commit(tr);
}

static fdb_error_t s_future_block_until_ready(FDBFuture *f)
{
    if (fake_find(f) != NULL) {
        return 0;
    }
    return fdb_future_block_until_ready(f);
}

static fdb_error_t s_future_get_error(FDBFuture *f)
{
    struct fake_future *ff = fake_find(f);

    if (ff != NULL) {
        return ff->err;
    }
    return fdb_future_get_error(f);
}

static fdb_error_t s_future_get_value(FDBFuture *f, fdb_bool_t *present, const uint8_t **value,
                                      int *value_len)
{
    struct fake_future *ff = fake_find(f);

    if (ff != NULL) {
        return ff->err;
    }
    return fdb_future_get_value(f, present, value, value_len);
}

static void s_future_destroy(FDBFuture *f)
{
    struct fake_future *ff = fake_find(f);

    if (ff != NULL) {
        fake_free(ff);
        return;
    }
    fdb_future_destroy(f);
}

static const struct fdb_txn_calls g_script_calls = {
    .create_transaction             = s_create_transaction,
    .transaction_destroy            = s_transaction_destroy,
    .transaction_reset              = s_transaction_reset,
    .transaction_set_option         = fdb_transaction_set_option,
    .transaction_set                = s_transaction_set,
    .transaction_get                = s_transaction_get,
    .transaction_get_read_version   = fdb_transaction_get_read_version,
    .transaction_add_conflict_range = s_add_conflict_range,
    .transaction_commit             = s_transaction_commit,
    .transaction_on_error           = fdb_transaction_on_error,
    .future_block_until_ready       = s_future_block_until_ready,
    .future_get_error               = s_future_get_error,
    .future_get_value               = s_future_get_value,
    .future_destroy                 = s_future_destroy,
    .error_predicate                = fdb_error_predicate,
    .now_ns                         = fdb_txn_now_ns,
};

/* Result of one scripted slot call. */
struct scripted {
    enum mds_status   st;
    struct stats_snap d;         /**< Stats deltas. */
    int               probes;
    int               fence_commits;
    int               attempt_faults;
    bool              hostage_committed;
    fdb_error_t       hostage_err;
    int               fakes_left;
    struct mds_inode  out;
    bool              safe_to_discard;
};

static void script_arm(enum fault_mode mode)
{
    memset(&g_fs, 0, sizeof(g_fs));
    g_fs.mode = mode;
    g_fs.armed = 1;
}

static void stats_delta(const struct stats_snap *a, const struct stats_snap *b,
                        struct stats_snap *d)
{
    d->attempts = b->attempts - a->attempts;
    d->commits = b->commits - a->commits;
    d->retries = b->retries - a->retries;
    d->unknown_results = b->unknown_results - a->unknown_results;
    d->unknown_landed = b->unknown_landed - a->unknown_landed;
    d->fences = b->fences - a->fences;
    d->fence_landed = b->fence_landed - a->fence_landed;
    d->fenced_reruns = b->fenced_reruns - a->fenced_reruns;
    d->indoubt = b->indoubt - a->indoubt;
    d->delay_exhausted = b->delay_exhausted - a->delay_exhausted;
    d->io_errors = b->io_errors - a->io_errors;
}

/* Finish a scripted call: unhook the table, collect counters. */
static void script_collect(struct fdb_backend *b, const struct stats_snap *before,
                           struct scripted *r)
{
    struct stats_snap after;
    int i;

    b->calls = NULL;
    stats_snapshot(b, &after);
    stats_delta(before, &after, &r->d);
    r->probes = g_fs.probes;
    r->fence_commits = g_fs.fence_commits;
    r->attempt_faults = g_fs.attempt_faults;
    r->hostage_committed = g_fs.hostage_committed;
    r->hostage_err = g_fs.hostage_err;
    r->fakes_left = 0;
    for (i = 0; i < FAKE_MAX; i++) {
        if (g_fs.fakes[i] != NULL) {
            r->fakes_left++;
            fake_free(g_fs.fakes[i]);
        }
    }
    g_fs.mode = FM_NONE;
    g_fs.armed = 0;
}

/* ns_create of @p name under the script. */
static void scripted_create(struct ctx *c, enum fault_mode mode, const char *name,
                            struct scripted *r)
{
    struct fdb_backend *b = be_of(c->cat);
    struct stats_snap before;

    memset(r, 0, sizeof(*r));
    stats_snapshot(b, &before);
    script_arm(mode);
    b->calls = &g_script_calls;
    r->st = mds_cat_ns_create(c->cat, NULL, c->dir, name, MDS_FTYPE_REG, 0644, 0, 0, NULL,
                              &r->out);
    script_collect(b, &before, r);
}

/* ns_create_wide (2 x 1 stripes) of @p name under the script. */
static void scripted_create_wide(struct ctx *c, enum fault_mode mode, const char *name,
                                 struct scripted *r)
{
    struct fdb_backend *b = be_of(c->cat);
    struct stats_snap before;
    struct mds_inode child;
    struct mds_ds_map_entry entries[2];
    struct timespec now;

    memset(r, 0, sizeof(*r));
    memset(&child, 0, sizeof(child));
    memset(entries, 0, sizeof(entries));
    entries[0].ds_id = 1;
    entries[0].nfs_fh_len = 4;
    entries[1].ds_id = 2;
    entries[1].nfs_fh_len = 4;
    (void)clock_gettime(CLOCK_REALTIME, &now);
    if (mds_cat_alloc_fileid(c->cat, NULL, &child.fileid) != MDS_OK) {
        r->st = MDS_ERR_IO;
        return;
    }
    child.type = MDS_FTYPE_REG;
    child.mode = 0644;
    child.nlink = 1;
    child.atime = now;
    child.mtime = now;
    child.ctime = now;
    child.change = 1;
    child.generation = 1;
    child.parent_fileid = c->dir;
    child.stripe_count = 2;
    child.stripe_unit = 65536;
    child.mirror_count = 1;
    r->out = child;
    stats_snapshot(b, &before);
    script_arm(mode);
    b->calls = &g_script_calls;
    r->st = mds_cat_ns_create_wide(c->cat, c->dir, name, &child, 2, 65536, 1, entries,
                                   &r->safe_to_discard);
    script_collect(b, &before, r);
}

/* The id pools are thread-local batches refilled through the runner; a
 * refill inside a scripted call would consume the scripted fault.  One
 * plain create (fileid + cookie) and its remove leave both pools far
 * from empty. */
static bool scripted_prewarm(struct ctx *c)
{
    struct mds_inode out;

    if (mds_cat_ns_create(c->cat, NULL, c->dir, "prewarm", MDS_FTYPE_REG, 0644, 0, 0, NULL,
                          &out) != MDS_OK ||
        mds_cat_ns_remove(c->cat, NULL, c->dir, "prewarm") != MDS_OK) {
        ctx_fail(c, "prewarm create/remove failed");
        return false;
    }
    return true;
}

/* Exactly-once check: @p name resolves to @p fileid, appears once in
 * the directory and the parent's change advanced by @p change_delta. */
static void check_effect(struct ctx *c, const char *what, const char *name, uint64_t fileid,
                         const struct mds_inode *parent_before, uint64_t change_delta)
{
    struct mds_inode seen;
    struct mds_inode parent;
    struct name_page *p = calloc(1, sizeof(*p));
    enum mds_status st;

    if (p == NULL) {
        ctx_fail(c, "%s: out of memory", what);
        return;
    }
    st = mds_cat_ns_lookup(c->cat, c->dir, name, &seen);
    if (change_delta > 0) {
        if (st != MDS_OK) {
            ctx_fail(c, "%s: lookup of the published name returned %s", what, status_name(st));
        } else if (fileid != 0 && seen.fileid != fileid) {
            ctx_fail(c, "%s: published fileid %llu, expected %llu", what,
                     (unsigned long long)seen.fileid, (unsigned long long)fileid);
        }
    } else if (st != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "%s: name must be absent, lookup returned %s", what, status_name(st));
    }
    if (read_names(c->cat, c->dir, p) != MDS_OK) {
        ctx_fail(c, "%s: readdir failed", what);
    } else if (name_count(p, name) != (change_delta > 0 ? 1U : 0U)) {
        ctx_fail(c, "%s: name appears %u times in the directory", what, name_count(p, name));
    }
    if (mds_cat_ns_getattr(c->cat, c->dir, &parent) != MDS_OK) {
        ctx_fail(c, "%s: parent getattr failed", what);
    } else if (parent.change != parent_before->change + change_delta) {
        ctx_fail(c, "%s: parent change advanced %llu, expected %llu", what,
                 (unsigned long long)(parent.change - parent_before->change),
                 (unsigned long long)change_delta);
    }
    free(p);
}

/* -----------------------------------------------------------------------
 * scripted_unknown_result
 * ----------------------------------------------------------------------- */

static void scripted_unknown_result(struct ctx *c)
{
    struct scripted r;
    struct mds_inode parent;

    if (!scripted_prewarm(c)) {
        return;
    }
    /* Landed, reply lost: the witness read decides, the body never
     * runs twice. */
    if (mds_cat_ns_getattr(c->cat, c->dir, &parent) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    scripted_create(c, FM_LANDED_1021, "landed-1021", &r);
    if (r.st != MDS_OK) {
        ctx_fail(c, "1021 after a landed commit returned %s", status_name(r.st));
    }
    if (r.attempt_faults != 1 || r.d.attempts != 1 || r.probes != 1 || r.fence_commits != 0 ||
        r.d.unknown_results != 1 || r.d.unknown_landed != 1 || r.d.commits != 1 ||
        r.fakes_left != 0) {
        ctx_fail(c, "landed 1021: attempts %llu probes %d fences %d 1021 %llu landed %llu "
                 "commits %llu", (unsigned long long)r.d.attempts, r.probes, r.fence_commits,
                 (unsigned long long)r.d.unknown_results, (unsigned long long)r.d.unknown_landed,
                 (unsigned long long)r.d.commits);
    }
    check_effect(c, "landed 1021", "landed-1021", r.out.fileid, &parent, 1);

    /* Nothing landed: the witness is absent, the body re-runs once and
     * the second attempt publishes exactly once. */
    if (mds_cat_ns_getattr(c->cat, c->dir, &parent) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    scripted_create(c, FM_LOST_1021, "lost-1021", &r);
    if (r.st != MDS_OK) {
        ctx_fail(c, "1021 with nothing landed returned %s", status_name(r.st));
    }
    if (r.attempt_faults != 1 || r.d.attempts != 2 || r.probes != 1 || r.fence_commits != 0 ||
        r.d.unknown_results != 1 || r.d.unknown_landed != 0 || r.d.commits != 1 ||
        r.fakes_left != 0) {
        ctx_fail(c, "lost 1021: attempts %llu probes %d fences %d 1021 %llu landed %llu "
                 "commits %llu", (unsigned long long)r.d.attempts, r.probes, r.fence_commits,
                 (unsigned long long)r.d.unknown_results, (unsigned long long)r.d.unknown_landed,
                 (unsigned long long)r.d.commits);
    }
    check_effect(c, "lost 1021", "lost-1021", r.out.fileid, &parent, 1);
    (void)printf("    1021 resolved: 1 landed (no re-run), 1 not landed (re-run once)\n");
}

/* -----------------------------------------------------------------------
 * scripted_timed_out
 * ----------------------------------------------------------------------- */

static void scripted_timed_out_landed(struct ctx *c)
{
    struct scripted r;
    struct mds_inode parent;

    if (mds_cat_ns_getattr(c->cat, c->dir, &parent) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    scripted_create(c, FM_LANDED_1031, "landed-1031", &r);
    if (r.st != MDS_OK) {
        ctx_fail(c, "1031 after a landed commit returned %s", status_name(r.st));
    }
    if (r.d.attempts != 1 || r.probes != 1 || r.fence_commits != 0 || r.d.fence_landed != 1 ||
        r.d.commits != 1 || r.fakes_left != 0) {
        ctx_fail(c, "landed 1031: attempts %llu probes %d fence commits %d fence_landed %llu",
                 (unsigned long long)r.d.attempts, r.probes, r.fence_commits,
                 (unsigned long long)r.d.fence_landed);
    }
    check_effect(c, "landed 1031", "landed-1031", r.out.fileid, &parent, 1);
}

/* The commit lands after the probe pinned its read version: the probe
 * misses, the fence conflicts with the landing (1020), the runner backs
 * off, re-probes and sees the witness.  No re-run, one effect. */
static void scripted_timed_out_inflight(struct ctx *c)
{
    struct scripted r;
    struct mds_inode parent;

    if (mds_cat_ns_getattr(c->cat, c->dir, &parent) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    scripted_create(c, FM_INFLIGHT_1031, "inflight-1031", &r);
    if (r.st != MDS_OK) {
        ctx_fail(c, "1031 with the commit landing later returned %s", status_name(r.st));
    }
    if (!r.hostage_committed || r.hostage_err != 0) {
        ctx_fail(c, "in-flight 1031: the delayed real commit reported %d", (int)r.hostage_err);
    }
    if (r.d.attempts != 1 || r.probes < 2 || r.fence_commits < 1 || r.d.fence_landed != 1 ||
        r.d.commits != 1 || r.fakes_left != 0) {
        ctx_fail(c, "in-flight 1031: attempts %llu probes %d fence commits %d fence_landed %llu"
                 " commits %llu", (unsigned long long)r.d.attempts, r.probes, r.fence_commits,
                 (unsigned long long)r.d.fence_landed, (unsigned long long)r.d.commits);
    }
    check_effect(c, "in-flight 1031", "inflight-1031", r.out.fileid, &parent, 1);
    (void)printf("    in-flight 1031: probes %d, fence rounds %d (the first conflicted with "
                 "the landing)\n", r.probes, r.fence_commits);
}

/* The commit is held back; the fence commits; committing the held-back
 * attempt afterwards must fail with 1020 (its READ conflict range on
 * the witness key intersects the fence's WRITE range); the body re-runs
 * and publishes once. */
static void scripted_timed_out_fenced(struct ctx *c)
{
    struct scripted r;
    struct mds_inode parent;

    if (mds_cat_ns_getattr(c->cat, c->dir, &parent) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    scripted_create(c, FM_FENCED_1031, "fenced-1031", &r);
    if (r.st != MDS_OK) {
        ctx_fail(c, "1031 with the commit fenced returned %s", status_name(r.st));
    }
    if (!r.hostage_committed) {
        ctx_fail(c, "fenced 1031: the held-back attempt was never committed for real");
    } else if (r.hostage_err != FDB_ERR_NOT_COMMITTED) {
        ctx_fail(c, "fenced 1031: the held-back attempt committed with %d after the fence "
                 "(expected 1020 not_committed)", (int)r.hostage_err);
    }
    if (r.d.attempts != 2 || r.fence_commits != 1 || r.d.fences != 1 || r.d.fence_landed != 0 ||
        r.d.commits != 1 || r.fakes_left != 0) {
        ctx_fail(c, "fenced 1031: attempts %llu fence commits %d fences %llu landed %llu "
                 "commits %llu", (unsigned long long)r.d.attempts, r.fence_commits,
                 (unsigned long long)r.d.fences, (unsigned long long)r.d.fence_landed,
                 (unsigned long long)r.d.commits);
    }
    check_effect(c, "fenced 1031", "fenced-1031", r.out.fileid, &parent, 1);
    (void)printf("    fenced 1031: late commit of the fenced attempt -> %d %s\n",
                 (int)r.hostage_err, fdb_get_error(r.hostage_err));
}

static void scripted_timed_out(struct ctx *c)
{
    if (!scripted_prewarm(c)) {
        return;
    }
    scripted_timed_out_landed(c);
    scripted_timed_out_inflight(c);
    scripted_timed_out_fenced(c);
}

/* -----------------------------------------------------------------------
 * scripted_fence_exhaustion
 * ----------------------------------------------------------------------- */

#define SCRIPTED_DEADLINE_MS 600U
/* The runner stops a round when whole milliseconds run out, so an
 * INDOUBT may return a few ms before the nominal deadline; the upper
 * bound covers one more real probe round trip plus scheduling. */
#define DEADLINE_SLACK_LO_MS 100U
#define DEADLINE_SLACK_HI_MS 4000U

static bool within_deadline(uint64_t elapsed_ms)
{
    return elapsed_ms + DEADLINE_SLACK_LO_MS >= SCRIPTED_DEADLINE_MS &&
           elapsed_ms <= SCRIPTED_DEADLINE_MS + DEADLINE_SLACK_HI_MS;
}

static void scripted_fence_exhaustion(struct ctx *c)
{
    struct fdb_backend *b = be_of(c->cat);
    struct scripted r;
    struct mds_inode parent;
    uint32_t saved_deadline = b->op_deadline_ms;
    uint64_t t0;

    if (!scripted_prewarm(c)) {
        return;
    }
    if (mds_cat_ns_getattr(c->cat, c->dir, &parent) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    b->op_deadline_ms = SCRIPTED_DEADLINE_MS;
    t0 = monotonic_ms();
    scripted_create(c, FM_FENCE_FAILS_1031, "indoubt-fence", &r);
    t0 = monotonic_ms() - t0;
    if (r.st != MDS_ERR_INDOUBT) {
        ctx_fail(c, "fence failing until the deadline returned %s", status_name(r.st));
    }
    if (r.d.attempts != 1 || r.d.indoubt != 1 || r.fence_commits < 1 || r.d.commits != 0 ||
        r.d.fence_landed != 0 || r.fakes_left != 0) {
        ctx_fail(c, "fence exhaustion: attempts %llu indoubt %llu fence commits %d commits %llu",
                 (unsigned long long)r.d.attempts, (unsigned long long)r.d.indoubt,
                 r.fence_commits, (unsigned long long)r.d.commits);
    }
    if (!within_deadline(t0)) {
        ctx_fail(c, "fence exhaustion took %llu ms for a %u ms deadline",
                 (unsigned long long)t0, SCRIPTED_DEADLINE_MS);
    }
    /* No replay, nothing published: the held-back commit was never issued. */
    check_effect(c, "fence exhaustion", "indoubt-fence", 0, &parent, 0);

    /* Same through ns_create_wide: the DS bundle must not be reclaimed. */
    scripted_create_wide(c, FM_FENCE_FAILS_1031, "indoubt-wide", &r);
    if (r.st != MDS_ERR_INDOUBT) {
        ctx_fail(c, "create_wide with the fence failing returned %s", status_name(r.st));
    }
    if (r.safe_to_discard) {
        ctx_fail(c, "create_wide INDOUBT reported safe_to_discard == true");
    }
    if (r.d.attempts != 1 || r.d.indoubt != 1 || r.fakes_left != 0) {
        ctx_fail(c, "create_wide fence exhaustion: attempts %llu indoubt %llu",
                 (unsigned long long)r.d.attempts, (unsigned long long)r.d.indoubt);
    }
    check_effect(c, "create_wide fence exhaustion", "indoubt-wide", 0, &parent, 0);
    b->op_deadline_ms = saved_deadline;
    (void)printf("    INDOUBT after %llu ms (deadline %u ms), %d fence rounds, no replay, "
                 "safe_to_discard=false\n", (unsigned long long)t0, SCRIPTED_DEADLINE_MS,
                 r.fence_commits);
}

/* -----------------------------------------------------------------------
 * scripted_probe_exhaustion
 * ----------------------------------------------------------------------- */

static void scripted_probe_exhaustion(struct ctx *c)
{
    struct fdb_backend *b = be_of(c->cat);
    struct scripted r;
    struct mds_inode parent;
    uint32_t saved_deadline = b->op_deadline_ms;
    uint64_t t0;

    if (!scripted_prewarm(c)) {
        return;
    }
    if (mds_cat_ns_getattr(c->cat, c->dir, &parent) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    b->op_deadline_ms = SCRIPTED_DEADLINE_MS;
    t0 = monotonic_ms();
    scripted_create(c, FM_PROBE_FAILS_1021, "indoubt-probe", &r);
    t0 = monotonic_ms() - t0;
    b->op_deadline_ms = saved_deadline;
    if (r.st != MDS_ERR_INDOUBT) {
        ctx_fail(c, "landed 1021 with every probe failing returned %s", status_name(r.st));
    }
    if (r.d.attempts != 1 || r.d.indoubt != 1 || r.d.unknown_results != 1 ||
        r.d.unknown_landed != 0 || r.d.commits != 0 || r.probes < 2 || r.fence_commits != 0 ||
        r.fakes_left != 0) {
        ctx_fail(c, "probe exhaustion: attempts %llu indoubt %llu probes %d fences %d "
                 "commits %llu", (unsigned long long)r.d.attempts,
                 (unsigned long long)r.d.indoubt, r.probes, r.fence_commits,
                 (unsigned long long)r.d.commits);
    }
    if (!within_deadline(t0)) {
        ctx_fail(c, "probe exhaustion took %llu ms for a %u ms deadline",
                 (unsigned long long)t0, SCRIPTED_DEADLINE_MS);
    }
    /* The one landed commit is the only effect. */
    check_effect(c, "probe exhaustion", "indoubt-probe", r.out.fileid, &parent, 1);
    (void)printf("    INDOUBT after %llu ms, %d failed probes, effect present exactly once\n",
                 (unsigned long long)t0, r.probes);
}

/* -----------------------------------------------------------------------
 * size_bounds
 * ----------------------------------------------------------------------- */

/* Rows of one file's STRIPE_ENT range and presence of its STRIPE_HDR. */
static bool stripe_rows(struct ctx *c, uint64_t fileid, int *rows, bool *hdr)
{
    struct fdb_backend *b = be_of(c->cat);
    struct raw_scan s;
    struct raw_get g;
    struct fdb_key k;

    memset(&s, 0, sizeof(s));
    s.skip_type = -1;
    fdb_key_stripe_ent_prefix(&k, &b->prefix, fileid);
    if (raw_scan_prefix(b, &k, (int)(MDS_MAX_STRIPES * MDS_MAX_MIRRORS) + 8, &s) != MDS_OK) {
        ctx_fail(c, "stripe row scan failed");
        return false;
    }
    memset(&g, 0, sizeof(g));
    fdb_key_stripe_hdr(&g.k, &b->prefix, fileid);
    if (fdb_run_txn(b, FDB_TXN_READONLY, "fault_raw_get", raw_get_body, &g) != MDS_OK) {
        ctx_fail(c, "stripe header read failed");
        return false;
    }
    *rows = s.count;
    *hdr = g.found;
    return true;
}

static void size_bounds_names(struct ctx *c)
{
    struct fdb_backend *b = be_of(c->cat);
    struct mds_inode out;
    struct mds_inode seen;
    struct mds_inode parent;
    struct mds_inode after;
    struct name_page *p = calloc(1, sizeof(*p));
    char name[MDS_MAX_NAME + 2];
    struct stats_snap s0;
    struct stats_snap s1;
    enum mds_status st;

    if (p == NULL) {
        ctx_fail(c, "out of memory");
        return;
    }
    /* MDS_MAX_NAME bytes: created, visible, removable. */
    memset(name, 'n', MDS_MAX_NAME);
    name[MDS_MAX_NAME] = '\0';
    st = mds_cat_ns_create(c->cat, NULL, c->dir, name, MDS_FTYPE_REG, 0644, 0, 0, NULL, &out);
    if (st != MDS_OK) {
        ctx_fail(c, "create of a %u-byte name returned %s", (unsigned)MDS_MAX_NAME,
                 status_name(st));
    } else {
        if (mds_cat_ns_lookup(c->cat, c->dir, name, &seen) != MDS_OK ||
            seen.fileid != out.fileid) {
            ctx_fail(c, "%u-byte name does not resolve", (unsigned)MDS_MAX_NAME);
        }
        if (read_names(c->cat, c->dir, p) != MDS_OK || name_count(p, name) != 1) {
            ctx_fail(c, "%u-byte name not listed exactly once", (unsigned)MDS_MAX_NAME);
        }
        if (mds_cat_ns_remove(c->cat, NULL, c->dir, name) != MDS_OK) {
            ctx_fail(c, "remove of the %u-byte name failed", (unsigned)MDS_MAX_NAME);
        }
    }
    /* MDS_MAX_NAME + 1: refused before the store is touched (no attempt
     * counted, parent untouched) by every name-taking slot. */
    memset(name, 'x', MDS_MAX_NAME + 1);
    name[MDS_MAX_NAME + 1] = '\0';
    if (mds_cat_ns_getattr(c->cat, c->dir, &parent) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        free(p);
        return;
    }
    stats_snapshot(b, &s0);
    st = mds_cat_ns_create(c->cat, NULL, c->dir, name, MDS_FTYPE_REG, 0644, 0, 0, NULL, &out);
    if (st != MDS_ERR_INVAL) {
        ctx_fail(c, "create of a %u-byte name returned %s (want INVAL)",
                 (unsigned)MDS_MAX_NAME + 1U, status_name(st));
    }
    if (mds_cat_ns_lookup(c->cat, c->dir, name, &seen) != MDS_ERR_INVAL ||
        mds_cat_ns_remove(c->cat, NULL, c->dir, name) != MDS_ERR_INVAL ||
        mds_cat_ns_link(c->cat, NULL, c->dir, name, c->dir) != MDS_ERR_INVAL ||
        mds_cat_ns_rename(c->cat, NULL, c->dir, name, c->dir, "short") != MDS_ERR_INVAL ||
        mds_cat_ns_rename(c->cat, NULL, c->dir, "short", c->dir, name) != MDS_ERR_INVAL) {
        ctx_fail(c, "an over-long name was accepted by lookup/remove/link/rename");
    }
    stats_snapshot(b, &s1);
    if (s1.attempts != s0.attempts) {
        ctx_fail(c, "over-long names reached the store (%llu attempts)",
                 (unsigned long long)(s1.attempts - s0.attempts));
    }
    if (mds_cat_ns_getattr(c->cat, c->dir, &after) != MDS_OK || after.change != parent.change) {
        ctx_fail(c, "over-long name changed the parent");
    }
    free(p);
}

/* The widest file the catalogue admits: MDS_MAX_STRIPES x MDS_MAX_MIRRORS
 * rows with full-size handles, one transaction. */
#define WIDE_ROWS ((size_t)MDS_MAX_STRIPES * (size_t)MDS_MAX_MIRRORS)

static void wide_fill(struct mds_ds_map_entry *entries, struct mds_inode *child,
                      uint64_t parent_fileid)
{
    struct timespec now;
    size_t i;

    for (i = 0; i < WIDE_ROWS; i++) {
        entries[i].ds_id = 1U + (uint32_t)(i % 8U);
        entries[i].nfs_fh_len = MDS_NFS_FH_MAX;
        memset(entries[i].nfs_fh, (int)(i & 0xFFU), MDS_NFS_FH_MAX);
    }
    (void)clock_gettime(CLOCK_REALTIME, &now);
    child->type = MDS_FTYPE_REG;
    child->mode = 0644;
    child->nlink = 1;
    child->atime = now;
    child->mtime = now;
    child->ctime = now;
    child->change = 1;
    child->generation = 1;
    child->parent_fileid = parent_fileid;
    child->stripe_count = MDS_MAX_STRIPES;
    child->stripe_unit = 65536;
    child->mirror_count = MDS_MAX_MIRRORS;
}

/* Replay (definitive EXISTS, bundle reclaimable) and remove (every
 * stripe row purged with the inode). */
static void wide_replay_and_remove(struct ctx *c, const struct mds_inode *child,
                                   const struct mds_ds_map_entry *entries)
{
    struct mds_inode seen;
    bool safe = false;
    bool hdr = false;
    int rows = -1;
    enum mds_status st;

    st = mds_cat_ns_create_wide(c->cat, c->dir, "wide", child, MDS_MAX_STRIPES, 65536,
                                MDS_MAX_MIRRORS, entries, &safe);
    if (st != MDS_ERR_EXISTS || !safe) {
        ctx_fail(c, "replayed wide create returned %s safe_to_discard=%d", status_name(st),
                 safe ? 1 : 0);
    }
    if (mds_cat_ns_remove(c->cat, NULL, c->dir, "wide") != MDS_OK) {
        ctx_fail(c, "remove of the wide file failed");
    } else if (stripe_rows(c, child->fileid, &rows, &hdr) && (rows != 0 || hdr)) {
        ctx_fail(c, "remove left %d stripe rows, header %d", rows, hdr ? 1 : 0);
    }
    if (mds_cat_ns_getattr(c->cat, child->fileid, &seen) != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "wide inode survived its remove");
    }
}

static void size_bounds_wide(struct ctx *c)
{
    struct mds_ds_map_entry *entries = calloc(WIDE_ROWS, sizeof(*entries));
    struct mds_inode child;
    struct mds_inode seen;
    struct mds_inode parent;
    struct mds_inode after;
    bool safe = true;
    bool hdr = false;
    int rows = -1;
    enum mds_status st;

    if (entries == NULL) {
        ctx_fail(c, "out of memory");
        return;
    }
    memset(&child, 0, sizeof(child));
    if (mds_cat_alloc_fileid(c->cat, NULL, &child.fileid) != MDS_OK ||
        mds_cat_ns_getattr(c->cat, c->dir, &parent) != MDS_OK) {
        ctx_fail(c, "wide create setup failed");
        free(entries);
        return;
    }
    wide_fill(entries, &child, c->dir);
    st = mds_cat_ns_create_wide(c->cat, c->dir, "wide", &child, MDS_MAX_STRIPES, 65536,
                                MDS_MAX_MIRRORS, entries, &safe);
    if (st != MDS_OK) {
        ctx_fail(c, "wide create (%u rows) returned %s", (unsigned)WIDE_ROWS, status_name(st));
        free(entries);
        return;
    }
    if (safe) {
        ctx_fail(c, "successful wide create reported safe_to_discard");
    }
    if (mds_cat_ns_lookup(c->cat, c->dir, "wide", &seen) != MDS_OK ||
        seen.fileid != child.fileid || seen.stripe_count != MDS_MAX_STRIPES ||
        seen.mirror_count != MDS_MAX_MIRRORS) {
        ctx_fail(c, "wide inode does not read back with %u x %u", (unsigned)MDS_MAX_STRIPES,
                 (unsigned)MDS_MAX_MIRRORS);
    }
    if (stripe_rows(c, child.fileid, &rows, &hdr) && (rows != (int)WIDE_ROWS || !hdr)) {
        ctx_fail(c, "wide create left %d stripe rows (want %u) header %d", rows,
                 (unsigned)WIDE_ROWS, hdr ? 1 : 0);
    }
    if (mds_cat_ns_getattr(c->cat, c->dir, &after) != MDS_OK ||
        after.change != parent.change + 1) {
        ctx_fail(c, "wide create did not advance the parent change exactly once");
    }
    wide_replay_and_remove(c, &child, entries);
    (void)printf("    %u-byte name ok, %u-byte name refused without a store attempt; wide "
                 "create: %u stripe rows in one commit, purged by remove\n",
                 (unsigned)MDS_MAX_NAME, (unsigned)MDS_MAX_NAME + 1U, (unsigned)WIDE_ROWS);
    free(entries);
}

static void size_bounds(struct ctx *c)
{
    size_bounds_names(c);
    if (c->res != R_FAIL) {
        size_bounds_wide(c);
    }
}

/* -----------------------------------------------------------------------
 * schema_refusal
 * ----------------------------------------------------------------------- */

#define FOREIGN_SCHEMA_VERSION 99ULL

static enum mds_status foreign_put(struct fdb_backend *b, const struct fdb_key_prefix *fp,
                                   enum fdb_meta_key sub, uint64_t v)
{
    struct raw_put p;

    memset(&p, 0, sizeof(p));
    fdb_key_meta(&p.k, fp, sub);
    p.v = v;
    return fdb_run_txn(b, FDB_TXN_MUTATING, "fault_raw_put", raw_put_le64_body, &p);
}

static enum mds_status foreign_clear(struct fdb_backend *b, const struct fdb_key_prefix *fp)
{
    struct raw_clear cl;
    struct fdb_key k;

    key_of_prefix(&k, fp);
    if (!fdb_key_range_prefix(&cl.r, &k)) {
        return MDS_ERR_INVAL;
    }
    return fdb_run_txn(b, FDB_TXN_MUTATING, "fault_raw_clear", raw_clear_body, &cl);
}

/* Scan a foreign prefix, WITNESS rows counted apart. */
static enum mds_status foreign_scan(struct fdb_backend *b, const struct fdb_key_prefix *fp,
                                    struct raw_scan *s)
{
    struct fdb_key k;

    memset(s, 0, sizeof(*s));
    s->type_off = fp->len;
    s->skip_type = FDB_KT_WITNESS;
    key_of_prefix(&k, fp);
    return raw_scan_prefix(b, &k, RAW_MAX, s);
}

static void schema_refusal(struct ctx *c)
{
    struct fdb_backend *b = be_of(c->cat);
    struct fdb_key_prefix fp;
    struct mds_catalogue *foreign = NULL;
    struct raw_scan before;
    struct raw_scan after;
    char prefix[32];
    enum mds_status st;

    (void)snprintf(prefix, sizeof(prefix), "fs-%ld", (long)getpid());
    if (!prefix_from_string(&fp, prefix)) {
        ctx_fail(c, "prefix build failed");
        return;
    }
    /* A keyspace stamped by another schema, with one more catalogue row. */
    if (foreign_clear(b, &fp) != MDS_OK ||
        foreign_put(b, &fp, FDB_META_SCHEMA_VERSION, FOREIGN_SCHEMA_VERSION) != MDS_OK ||
        foreign_put(b, &fp, FDB_META_FILEID, 4242) != MDS_OK) {
        ctx_fail(c, "could not stage the foreign keyspace");
        return;
    }
    if (foreign_scan(b, &fp, &before) != MDS_OK || before.count != 2 || before.skipped != 0) {
        ctx_fail(c, "foreign keyspace holds %d rows before the open (want 2)", before.count);
        (void)foreign_clear(b, &fp);
        return;
    }
    st = open_private(prefix, 1, 0, &foreign);
    if (st != MDS_ERR_INVAL || foreign != NULL) {
        ctx_fail(c, "open on schema %llu returned %s (want INVAL)", FOREIGN_SCHEMA_VERSION,
                 status_name(st));
        if (foreign != NULL) {
            mds_catalogue_close(foreign);
        }
    }
    if (foreign_scan(b, &fp, &after) != MDS_OK) {
        ctx_fail(c, "foreign keyspace scan failed after the open");
    } else if (after.count != before.count || after.hash != before.hash) {
        ctx_fail(c, "refused open changed the catalogue rows (%d -> %d rows)", before.count,
                 after.count);
    }
    (void)printf("    schema %llu refused with INVAL; %d catalogue rows unchanged; %d witness "
                 "row(s) left by the open's reachability transaction\n",
                 FOREIGN_SCHEMA_VERSION, after.count, after.skipped);
    if (foreign_clear(b, &fp) != MDS_OK) {
        ctx_fail(c, "foreign keyspace cleanup failed");
    }
}

/* -----------------------------------------------------------------------
 * witness_restart
 * ----------------------------------------------------------------------- */

#define RESTART_MDS_ID 42U

/* Witness rows of @p mds_id under @p fp: count, and the distinct epochs
 * (at most 4 kept). */
struct witness_rows {
    int      count;
    int      n_epochs;
    uint64_t epoch[4];
};

static enum mds_status witness_scan(struct fdb_backend *b, const struct fdb_key_prefix *fp,
                                    uint32_t mds_id, struct witness_rows *w)
{
    struct raw_scan s;
    struct fdb_key k;
    enum mds_status st;
    int i;

    memset(w, 0, sizeof(*w));
    memset(&s, 0, sizeof(s));
    s.skip_type = -1;
    s.ent = calloc(RAW_MAX, sizeof(*s.ent));
    if (s.ent == NULL) {
        return MDS_ERR_NOMEM;
    }
    fdb_key_witness_mds_prefix(&k, fp, mds_id);
    st = raw_scan_prefix(b, &k, RAW_MAX, &s);
    if (st == MDS_OK) {
        w->count = s.count;
        for (i = 0; i < s.count && i < RAW_MAX; i++) {
            const struct raw_entry *e = &s.ent[i];
            uint64_t epoch;
            int j;
            bool known = false;

            if (e->klen != k.len + 12U) {
                continue;
            }
            epoch = fdb_get_u64(e->key + k.len);
            for (j = 0; j < w->n_epochs; j++) {
                known = known || (w->epoch[j] == epoch);
            }
            if (!known && w->n_epochs < 4) {
                w->epoch[w->n_epochs++] = epoch;
            }
        }
    }
    free(s.ent);
    return st;
}

/* Child: a new incarnation of RESTART_MDS_ID on the parent's prefix. */
static int child_restart(const char *prefix, uint64_t parent_epoch)
{
    struct mds_catalogue *cat = NULL;
    struct fdb_backend *b;
    struct fdb_key_prefix fp;
    struct witness_rows w;
    struct mds_inode out;
    struct mds_inode seen;
    uint64_t epoch;
    int rc = 0;

    if (!prefix_from_string(&fp, prefix)) {
        return 1;
    }
    if (open_private(prefix, RESTART_MDS_ID, 0, &cat) != MDS_OK ||
        mds_catalogue_probe(cat) != MDS_OK) {
        (void)printf("    child: open/probe on the parent's prefix failed\n");
        mds_catalogue_close(cat);
        mds_catalogue_process_shutdown();
        return 1;
    }
    b = be_of(cat);
    epoch = fdb_txn_witness_epoch();
    if (epoch <= parent_epoch) {
        (void)printf("    child: stamp %llu not above the parent's %llu\n",
                     (unsigned long long)epoch, (unsigned long long)parent_epoch);
        rc = 1;
    }
    /* The open cleared [0, stamp): the parent's row is gone.  The clear
     * is itself a witnessed transaction, so exactly one row -- this
     * incarnation's -- exists now. */
    if (witness_scan(b, &fp, RESTART_MDS_ID, &w) != MDS_OK || w.count != 1 || w.n_epochs != 1 ||
        w.epoch[0] != epoch) {
        (void)printf("    child: %d witness row(s), %d epoch(s) after the open (want 1 at stamp "
                     "%llu: the clear's own)\n", w.count, w.n_epochs, (unsigned long long)epoch);
        rc = 1;
    }
    /* Mutations work and stamp the new incarnation. */
    if (mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, "c1", MDS_FTYPE_REG, 0644, 0, 0, NULL,
                          &out) != MDS_OK ||
        mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "a1", &seen) != MDS_OK ||
        mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, "c1", &seen) != MDS_OK) {
        (void)printf("    child: namespace operations failed after the restart\n");
        rc = 1;
    }
    if (witness_scan(b, &fp, RESTART_MDS_ID, &w) != MDS_OK || w.count != 1 || w.n_epochs != 1 ||
        w.epoch[0] != epoch) {
        (void)printf("    child: %d witness row(s) after one mutation (want 1 at stamp %llu)\n",
                     w.count, (unsigned long long)epoch);
        rc = 1;
    }
    (void)printf("    child incarnation stamp %llu: parent's row cleared, own row written\n",
                 (unsigned long long)epoch);
    mds_catalogue_close(cat);
    mds_catalogue_process_shutdown();
    return rc;
}

static void witness_restart_after_child(struct ctx *c, const char *prefix,
                                        const struct fdb_key_prefix *fp, uint64_t epoch)
{
    struct mds_catalogue *a2 = NULL;
    struct witness_rows w;
    struct mds_inode out;
    struct mds_inode seen;
    struct fdb_backend *b;

    /* Re-open in THIS process: the stamp is the older one, so the clear
     * covers [0, epoch) and leaves the child's higher row alone -- the
     * documented behaviour for a stepped-back clock. */
    if (open_private(prefix, RESTART_MDS_ID, 0, &a2) != MDS_OK) {
        ctx_fail(c, "re-open after the child failed");
        return;
    }
    b = be_of(a2);
    /* The re-open's clear transaction wrote this incarnation's row back;
     * the child's higher one must have survived the clear of [0, epoch). */
    if (witness_scan(b, fp, RESTART_MDS_ID, &w) != MDS_OK || w.count != 2 || w.n_epochs != 2 ||
        (w.epoch[0] != epoch && w.epoch[1] != epoch) ||
        (w.epoch[0] <= epoch && w.epoch[1] <= epoch)) {
        ctx_fail(c, "after the re-open: %d row(s), %d epoch(s) (want this incarnation's %llu "
                 "and the child's higher one)", w.count, w.n_epochs, (unsigned long long)epoch);
    }
    if (mds_cat_ns_create(a2, NULL, MDS_FILEID_ROOT, "a2", MDS_FTYPE_REG, 0644, 0, 0, NULL,
                          &out) != MDS_OK ||
        mds_cat_ns_lookup(a2, MDS_FILEID_ROOT, "a1", &seen) != MDS_OK ||
        mds_cat_ns_lookup(a2, MDS_FILEID_ROOT, "c1", &seen) != MDS_OK ||
        mds_cat_ns_lookup(a2, MDS_FILEID_ROOT, "a2", &seen) != MDS_OK) {
        ctx_fail(c, "namespace operations failed after the re-open");
    }
    if (witness_scan(b, fp, RESTART_MDS_ID, &w) != MDS_OK || w.count != 2 || w.n_epochs != 2) {
        ctx_fail(c, "after the re-open: %d row(s), %d epoch(s) (want 2 / 2: the child's and "
                 "this incarnation's)", w.count, w.n_epochs);
    }
    (void)printf("    parent stamp %llu re-opened below the child's: both rows present (%d)\n",
                 (unsigned long long)epoch, w.count);
    (void)catalogue_fdb_keyspace_clear(a2);
    mds_catalogue_close(a2);
}

/* Test housekeeping: clear the WITNESS table of a private prefix with a
 * bare fdb_c transaction, outside the runner (a clear covering WITNESS
 * must never run through it -- fdb_txn.h body rule).  Best effort. */
static void sweep_witness_table(struct fdb_backend *b, const char *prefix)
{
    struct fdb_key_prefix fp;
    struct fdb_key_range r;
    FDBTransaction *tr = NULL;
    FDBFuture *f;

    if (b == NULL || !prefix_from_string(&fp, prefix)) {
        return;
    }
    fdb_key_init(&r.begin, &fp, FDB_KT_WITNESS);
    if (!fdb_key_range_prefix(&r, &r.begin)) {
        return;
    }
    if (fdb_database_create_transaction(b->db, &tr) != 0 || tr == NULL) {
        return;
    }
    if (fdb_txn_set_timeout(tr, 4000) == 0) {
        fdb_txn_clear_range(tr, &r);
        f = fdb_transaction_commit(tr);
        (void)fdb_txn_wait(f);
        if (f != NULL) {
            fdb_future_destroy(f);
        }
    }
    fdb_transaction_destroy(tr);
}

static void witness_restart(struct ctx *c)
{
    struct mds_catalogue *a = NULL;
    struct fdb_key_prefix fp;
    struct witness_rows w;
    struct mds_inode out;
    char prefix[32];
    char epoch_arg[32];
    char *args[2];
    uint64_t epoch = fdb_txn_witness_epoch();
    int rc;

    memset(&w, 0, sizeof(w));
    (void)snprintf(prefix, sizeof(prefix), "fr-%ld", (long)getpid());
    if (!prefix_from_string(&fp, prefix)) {
        ctx_fail(c, "prefix build failed");
        return;
    }
    if (open_private_bootstrapped(prefix, RESTART_MDS_ID, true, &a) != MDS_OK) {
        ctx_fail(c, "private open/bootstrap failed");
        return;
    }
    if (mds_cat_ns_create(a, NULL, MDS_FILEID_ROOT, "a1", MDS_FTYPE_REG, 0644, 0, 0, NULL,
                          &out) != MDS_OK) {
        ctx_fail(c, "create before the restart failed");
    }
    if (witness_scan(be_of(a), &fp, RESTART_MDS_ID, &w) != MDS_OK || w.count != 1 ||
        w.n_epochs != 1 || w.epoch[0] != epoch) {
        ctx_fail(c, "before the restart: %d witness row(s), %d epoch(s) (want 1 at %llu)",
                 w.count, w.n_epochs, (unsigned long long)epoch);
    }
    mds_catalogue_close(a);
    if (c->res == R_FAIL) {
        return;
    }
    (void)snprintf(epoch_arg, sizeof(epoch_arg), "%llu", (unsigned long long)epoch);
    args[0] = prefix;
    args[1] = epoch_arg;
    rc = run_child("restart", args, 2, NULL, 0);
    if (rc != 0) {
        ctx_fail(c, "restart child exited %d", rc);
        return;
    }
    witness_restart_after_child(c, prefix, &fp, epoch);
    sweep_witness_table(be_of(c->cat), prefix);
}

/* -----------------------------------------------------------------------
 * keyspace_clear_witness
 * ----------------------------------------------------------------------- */

static void keyspace_clear_witness(struct ctx *c)
{
    struct mds_catalogue *k = NULL;
    struct fdb_key_prefix fp;
    struct witness_rows w;
    struct raw_scan s;
    struct fdb_key key;
    struct mds_inode out;
    char prefix[32];

    /* The census is reported even when the create short-circuits the
     * scan that would fill it. */
    memset(&w, 0, sizeof(w));
    (void)snprintf(prefix, sizeof(prefix), "fk-%ld", (long)getpid());
    if (!prefix_from_string(&fp, prefix)) {
        ctx_fail(c, "prefix build failed");
        return;
    }
    if (open_private_bootstrapped(prefix, RESTART_MDS_ID, true, &k) != MDS_OK) {
        ctx_fail(c, "private open/bootstrap failed");
        return;
    }
    if (mds_cat_ns_create(k, NULL, MDS_FILEID_ROOT, "k1", MDS_FTYPE_REG, 0644, 0, 0, NULL,
                          &out) != MDS_OK ||
        witness_scan(be_of(k), &fp, RESTART_MDS_ID, &w) != MDS_OK || w.count != 1) {
        ctx_fail(c, "setup: create failed or %d witness row(s) (want 1)", w.count);
    }
    if (catalogue_fdb_keyspace_clear(k) != MDS_OK) {
        ctx_fail(c, "keyspace_clear failed");
    }
    /* Every catalogue row is gone (the stamp too: probe fails) ... */
    memset(&s, 0, sizeof(s));
    s.type_off = fp.len;
    s.skip_type = FDB_KT_WITNESS;
    key_of_prefix(&key, &fp);
    if (raw_scan_prefix(be_of(k), &key, RAW_MAX, &s) != MDS_OK) {
        ctx_fail(c, "scan after the clear failed");
    } else if (s.count != 0) {
        ctx_fail(c, "%d catalogue row(s) survived keyspace_clear", s.count);
    }
    if (mds_catalogue_probe(k) == MDS_OK) {
        ctx_fail(c, "schema stamp survived keyspace_clear");
    }
    /* ... and the WITNESS table did not: this incarnation's row is
     * there, re-stamped by the clear's own commit. */
    if (witness_scan(be_of(k), &fp, RESTART_MDS_ID, &w) != MDS_OK || w.count != 1 ||
        w.n_epochs != 1 || w.epoch[0] != fdb_txn_witness_epoch()) {
        ctx_fail(c, "after keyspace_clear: %d witness row(s), %d epoch(s) (want this "
                 "incarnation's one)", w.count, w.n_epochs);
    }
    /* The keyspace is usable again after a bootstrap. */
    if (mds_catalogue_bootstrap(k) != MDS_OK ||
        mds_cat_ns_create(k, NULL, MDS_FILEID_ROOT, "k2", MDS_FTYPE_REG, 0644, 0, 0, NULL,
                          &out) != MDS_OK) {
        ctx_fail(c, "bootstrap / create after keyspace_clear failed");
    }
    (void)printf("    keyspace_clear: %d catalogue rows left, %d witness row(s) kept\n", s.count,
                 w.count);
    (void)catalogue_fdb_keyspace_clear(k);
    mds_catalogue_close(k);
    sweep_witness_table(be_of(c->cat), prefix);
}

/* -----------------------------------------------------------------------
 * buggify_mixed (child): a model of one directory drives valid create /
 * remove / rename / rename-over / link / setattr / parent_touch
 * operations and checks after every one that the store agrees with the
 * returned status.
 * ----------------------------------------------------------------------- */

#define MODEL_SLOTS   48
#define DEAD_RING     64
#define MIXED_OPS_DEFAULT 2000U
#define MIXED_CHECK_EVERY 250U

enum op_kind {
    OP_CREATE = 0,
    OP_REMOVE,
    OP_RENAME,
    OP_RENAME_OVER,
    OP_LINK,
    OP_SETATTR,
    OP_TOUCH,      /**< ns_parent_touch: a blind ADD after a read -- the shape
                    *   whose re-run does not conflict with a late landing
                    *   of the original, so only the fence protects it. */
    OP_KINDS,
};

static const char *const op_names[OP_KINDS] = {
    "create", "remove", "rename", "rename-over", "link", "setattr", "touch",
};

struct mentry {
    bool     used;
    char     name[24];
    uint64_t fileid;
    uint8_t  type;
    uint32_t mode;
};

struct model {
    struct ctx     c;
    struct mentry  e[MODEL_SLOTS];
    uint64_t       dead[DEAD_RING];   /**< Inodes whose last name is gone. */
    unsigned       dead_n;
    unsigned       dead_next;
    uint64_t       expected_change;
    unsigned       expected_dirs;
    uint32_t       seed;
    unsigned       name_seq;
    unsigned       ops;
    unsigned       ok;
    unsigned       delay;
    unsigned       io;
    unsigned       indoubt;
    unsigned       indoubt_applied;
    unsigned       per_kind[OP_KINDS];
};

/* How the store tells whether an operation was applied. */
struct applied_probe {
    const char *name;              /**< Name to look up (NULL: by mode / parent). */
    uint64_t    fileid;            /**< Required fileid of the name (0: any). */
    bool        present_when_applied;
    bool        by_parent_change;  /**< Applied iff the parent's change moved by 1. */
    uint64_t    mode_fileid;       /**< by mode: inode to read ... */
    uint32_t    mode;              /**< ... and the mode that means applied. */
    uint64_t    seen_fileid;       /**< Out: fileid the name resolved to. */
};

/* 1 applied, 0 not applied, -1 the store could not be read. */
static int probe_applied(struct model *m, struct applied_probe *p)
{
    struct mds_inode seen;
    enum mds_status st;

    if (p->by_parent_change) {
        st = getattr_retry(m->c.cat, m->c.dir, &seen);
        if (st != MDS_OK) {
            ctx_fail(&m->c, "parent getattr for a change probe returned %s", status_name(st));
            return -1;
        }
        if (seen.change == m->expected_change) {
            return 0;
        }
        if (seen.change == m->expected_change + 1) {
            return 1;
        }
        ctx_fail(&m->c, "op %u: parent change %llu, expected %llu or %llu (touch applied more "
                 "than once)", m->ops, (unsigned long long)seen.change,
                 (unsigned long long)m->expected_change,
                 (unsigned long long)m->expected_change + 1);
        return -1;
    }
    if (p->name == NULL) {
        st = getattr_retry(m->c.cat, p->mode_fileid, &seen);
        if (st != MDS_OK) {
            ctx_fail(&m->c, "getattr of %llu for a mode probe returned %s",
                     (unsigned long long)p->mode_fileid, status_name(st));
            return -1;
        }
        return (seen.mode == p->mode) ? 1 : 0;
    }
    st = lookup_retry(m->c.cat, m->c.dir, p->name, &seen);
    if (st == MDS_ERR_NOTFOUND) {
        return p->present_when_applied ? 0 : 1;
    }
    if (st != MDS_OK) {
        ctx_fail(&m->c, "lookup of %s for a probe returned %s", p->name, status_name(st));
        return -1;
    }
    p->seen_fileid = seen.fileid;
    if (!p->present_when_applied) {
        return 0;
    }
    return (p->fileid == 0 || seen.fileid == p->fileid) ? 1 : 0;
}

static bool verify_parent(struct model *m)
{
    struct mds_inode parent;
    enum mds_status st = getattr_retry(m->c.cat, m->c.dir, &parent);

    if (st != MDS_OK) {
        ctx_fail(&m->c, "parent getattr returned %s", status_name(st));
        return false;
    }
    if (parent.change != m->expected_change) {
        ctx_fail(&m->c, "op %u: parent change %llu, expected %llu", m->ops,
                 (unsigned long long)parent.change, (unsigned long long)m->expected_change);
        return false;
    }
    if (parent.nlink != 2U + m->expected_dirs) {
        ctx_fail(&m->c, "op %u: parent nlink %u, expected %u", m->ops, parent.nlink,
                 2U + m->expected_dirs);
        return false;
    }
    return true;
}

/*
 * Classify @p st against the store.  OK and INDOUBT-that-landed count as
 * applied; DELAY / IO must have left no trace; any other status is a
 * contradiction for an operation the model knows to be valid.  Returns
 * 1 applied, 0 not applied, -1 inconsistency (already recorded).
 */
static int settle(struct model *m, enum op_kind kind, enum mds_status st,
                  struct applied_probe *p, bool touches_parent, int dir_delta)
{
    int applied;
    int seen;

    m->ops++;
    m->per_kind[kind]++;
    if (st == MDS_OK) {
        m->ok++;
        applied = 1;
    } else if (st == MDS_ERR_INDOUBT) {
        m->indoubt++;
        sleep_ms(QUIESCENCE_MS);
        applied = probe_applied(m, p);
        if (applied < 0) {
            return -1;
        }
        m->indoubt_applied += (unsigned)applied;
    } else if (st == MDS_ERR_DELAY || st == MDS_ERR_IO) {
        if (st == MDS_ERR_DELAY) {
            m->delay++;
        } else {
            m->io++;
        }
        applied = 0;
    } else {
        ctx_fail(&m->c, "op %u: %s returned %s for a valid operation", m->ops, op_names[kind],
                 status_name(st));
        return -1;
    }
    seen = probe_applied(m, p);
    if (seen < 0) {
        return -1;
    }
    if (seen != applied) {
        ctx_fail(&m->c, "op %u: %s returned %s but the store shows the effect %s", m->ops,
                 op_names[kind], status_name(st), seen != 0 ? "present" : "absent");
        return -1;
    }
    if (applied != 0) {
        if (touches_parent) {
            m->expected_change++;
        }
        m->expected_dirs = (unsigned)((int)m->expected_dirs + dir_delta);
    }
    return verify_parent(m) ? applied : -1;
}

static unsigned model_links(const struct model *m, uint64_t fileid)
{
    unsigned i;
    unsigned n = 0;

    for (i = 0; i < MODEL_SLOTS; i++) {
        if (m->e[i].used && m->e[i].fileid == fileid) {
            n++;
        }
    }
    return n;
}

static int model_free_slot(const struct model *m)
{
    int i;

    for (i = 0; i < MODEL_SLOTS; i++) {
        if (!m->e[i].used) {
            return i;
        }
    }
    return -1;
}

/* A used slot chosen from @p r; files only when @p files. */
static int model_pick(const struct model *m, uint32_t r, bool files)
{
    int i;
    int start = (int)(r % MODEL_SLOTS);

    for (i = 0; i < MODEL_SLOTS; i++) {
        int idx = (start + i) % MODEL_SLOTS;

        if (m->e[idx].used && (!files || m->e[idx].type == (uint8_t)MDS_FTYPE_REG)) {
            return idx;
        }
    }
    return -1;
}

static void model_fresh_name(struct model *m, char *buf, size_t cap)
{
    (void)snprintf(buf, cap, "n%05u", m->name_seq++);
}

static void model_add(struct model *m, int slot, const char *name, uint64_t fileid, uint8_t type,
                      uint32_t mode)
{
    m->e[slot].used = true;
    (void)snprintf(m->e[slot].name, sizeof(m->e[slot].name), "%s", name);
    m->e[slot].fileid = fileid;
    m->e[slot].type = type;
    m->e[slot].mode = mode;
}

/* Drop a name; when it was the inode's last one, remember the inode as
 * dead (its row must be gone). */
static void model_drop(struct model *m, int slot)
{
    uint64_t fileid = m->e[slot].fileid;

    m->e[slot].used = false;
    if (model_links(m, fileid) == 0) {
        m->dead[m->dead_next] = fileid;
        m->dead_next = (m->dead_next + 1U) % DEAD_RING;
        if (m->dead_n < DEAD_RING) {
            m->dead_n++;
        }
    }
}

static void op_create(struct model *m, uint32_t r)
{
    struct applied_probe p;
    struct mds_inode out;
    char name[24];
    int slot = model_free_slot(m);
    bool dir = (r % 5U) == 0;
    enum mds_status st;

    if (slot < 0) {
        return;
    }
    model_fresh_name(m, name, sizeof(name));
    memset(&out, 0, sizeof(out));
    st = mds_cat_ns_create(m->c.cat, NULL, m->c.dir, name, dir ? MDS_FTYPE_DIR : MDS_FTYPE_REG,
                           dir ? 0755 : 0644, 0, 0, NULL, &out);
    memset(&p, 0, sizeof(p));
    p.name = name;
    p.fileid = (st == MDS_OK) ? out.fileid : 0;
    p.present_when_applied = true;
    if (settle(m, OP_CREATE, st, &p, true, dir ? 1 : 0) == 1) {
        model_add(m, slot, name, (st == MDS_OK) ? out.fileid : p.seen_fileid,
                  dir ? (uint8_t)MDS_FTYPE_DIR : (uint8_t)MDS_FTYPE_REG, dir ? 0755 : 0644);
    }
}

static void op_remove(struct model *m, uint32_t r)
{
    struct applied_probe p;
    int slot = model_pick(m, r, false);
    bool dir;
    enum mds_status st;

    if (slot < 0) {
        return;
    }
    dir = m->e[slot].type == (uint8_t)MDS_FTYPE_DIR;
    st = mds_cat_ns_remove(m->c.cat, NULL, m->c.dir, m->e[slot].name);
    memset(&p, 0, sizeof(p));
    p.name = m->e[slot].name;
    p.present_when_applied = false;
    if (settle(m, OP_REMOVE, st, &p, true, dir ? -1 : 0) == 1) {
        model_drop(m, slot);
    }
}

static void op_rename(struct model *m, uint32_t r)
{
    struct applied_probe p;
    char name[24];
    int slot = model_pick(m, r, false);
    enum mds_status st;

    if (slot < 0) {
        return;
    }
    model_fresh_name(m, name, sizeof(name));
    st = mds_cat_ns_rename(m->c.cat, NULL, m->c.dir, m->e[slot].name, m->c.dir, name);
    memset(&p, 0, sizeof(p));
    p.name = name;
    p.fileid = m->e[slot].fileid;
    p.present_when_applied = true;
    if (settle(m, OP_RENAME, st, &p, true, 0) == 1) {
        (void)snprintf(m->e[slot].name, sizeof(m->e[slot].name), "%s", name);
    }
}

/* Rename a file over another file: the victim loses a link, the moved
 * inode keeps every link it had (its source name is gone, the
 * destination name now resolves to it). */
static void op_rename_over(struct model *m, uint32_t r)
{
    struct applied_probe p;
    char dname[24];
    int src = model_pick(m, r, true);
    int dst = model_pick(m, r >> 8, true);
    enum mds_status st;

    if (src < 0 || dst < 0 || src == dst || m->e[src].fileid == m->e[dst].fileid) {
        return;
    }
    (void)snprintf(dname, sizeof(dname), "%s", m->e[dst].name);
    st = mds_cat_ns_rename(m->c.cat, NULL, m->c.dir, m->e[src].name, m->c.dir, dname);
    memset(&p, 0, sizeof(p));
    p.name = dname;
    p.fileid = m->e[src].fileid;
    p.present_when_applied = true;
    if (settle(m, OP_RENAME_OVER, st, &p, true, 0) == 1) {
        struct mentry moved = m->e[src];

        model_drop(m, dst);                    /* the victim's link */
        m->e[src].used = false;                /* not a dropped link: the inode moved */
        model_add(m, dst, dname, moved.fileid, moved.type, moved.mode);
    }
}

static void op_link(struct model *m, uint32_t r)
{
    struct applied_probe p;
    char name[24];
    int slot = model_free_slot(m);
    int target = model_pick(m, r, true);
    enum mds_status st;

    if (slot < 0 || target < 0) {
        return;
    }
    model_fresh_name(m, name, sizeof(name));
    st = mds_cat_ns_link(m->c.cat, NULL, m->c.dir, name, m->e[target].fileid);
    memset(&p, 0, sizeof(p));
    p.name = name;
    p.fileid = m->e[target].fileid;
    p.present_when_applied = true;
    if (settle(m, OP_LINK, st, &p, true, 0) == 1) {
        model_add(m, slot, name, m->e[target].fileid, m->e[target].type, m->e[target].mode);
    }
}

static void op_setattr(struct model *m, uint32_t r)
{
    struct applied_probe p;
    struct mds_inode attrs;
    int slot = model_pick(m, r, true);
    uint32_t mode;
    enum mds_status st;
    unsigned i;

    if (slot < 0) {
        return;
    }
    mode = 0600U + ((r >> 8) % 0100U);
    if (mode == m->e[slot].mode) {
        mode ^= 01U;
    }
    memset(&attrs, 0, sizeof(attrs));
    attrs.mode = mode;
    st = mds_cat_ns_setattr(m->c.cat, NULL, m->e[slot].fileid, &attrs, MDS_ATTR_MODE);
    memset(&p, 0, sizeof(p));
    p.mode_fileid = m->e[slot].fileid;
    p.mode = mode;
    if (settle(m, OP_SETATTR, st, &p, false, 0) == 1) {
        for (i = 0; i < MODEL_SLOTS; i++) {
            if (m->e[i].used && m->e[i].fileid == p.mode_fileid) {
                m->e[i].mode = mode;
            }
        }
    }
}

/* ns_parent_touch(dir, +1): the parent's change advances by exactly one
 * per applied call; a late landing of a fenced attempt would advance it
 * twice, which the change probe reports as an inconsistency. */
static void op_touch(struct model *m)
{
    struct applied_probe p;
    struct timespec now;
    enum mds_status st;

    (void)clock_gettime(CLOCK_REALTIME, &now);
    st = mds_cat_ns_parent_touch(m->c.cat, m->c.dir, 1, now);
    memset(&p, 0, sizeof(p));
    p.by_parent_change = true;
    (void)settle(m, OP_TOUCH, st, &p, true, 0);
}

static void op_dispatch(struct model *m)
{
    uint32_t r = prng_next(&m->seed);
    uint32_t pick = r % 100U;

    r = prng_next(&m->seed);
    if (pick < 28U) {
        op_create(m, r);
    } else if (pick < 46U) {
        op_remove(m, r);
    } else if (pick < 60U) {
        op_rename(m, r);
    } else if (pick < 70U) {
        op_rename_over(m, r);
    } else if (pick < 82U) {
        op_link(m, r);
    } else if (pick < 92U) {
        op_setattr(m, r);
    } else {
        op_touch(m);
    }
}

/* Every name of the model is listed exactly once, nothing else is, every
 * listed inode exists with the modelled type / mode / link count, and
 * every dead inode is gone. */
static void model_verify_all(struct model *m)
{
    struct name_page *p = calloc(1, sizeof(*p));
    struct mds_inode seen;
    unsigned i;
    unsigned modelled = 0;

    if (p == NULL) {
        ctx_fail(&m->c, "out of memory");
        return;
    }
    if (read_names(m->c.cat, m->c.dir, p) != MDS_OK || p->overflow) {
        ctx_fail(&m->c, "op %u: readdir failed or overflowed", m->ops);
        free(p);
        return;
    }
    for (i = 0; i < MODEL_SLOTS; i++) {
        const struct mentry *e = &m->e[i];
        enum mds_status st;

        if (!e->used) {
            continue;
        }
        modelled++;
        if (name_count(p, e->name) != 1) {
            ctx_fail(&m->c, "op %u: %s listed %u times", m->ops, e->name, name_count(p, e->name));
        }
        st = getattr_retry(m->c.cat, e->fileid, &seen);
        if (st != MDS_OK) {
            ctx_fail(&m->c, "op %u: dirent %s -> inode %llu: getattr %s", m->ops, e->name,
                     (unsigned long long)e->fileid, status_name(st));
            continue;
        }
        if ((uint8_t)seen.type != e->type || seen.mode != e->mode ||
            (e->type == (uint8_t)MDS_FTYPE_REG && seen.nlink != model_links(m, e->fileid)) ||
            (e->type == (uint8_t)MDS_FTYPE_DIR && seen.nlink != 2)) {
            ctx_fail(&m->c, "op %u: inode %llu (%s): type %d mode %o nlink %u, model type %u "
                     "mode %o links %u", m->ops, (unsigned long long)e->fileid, e->name,
                     (int)seen.type, seen.mode, seen.nlink, e->type, e->mode,
                     model_links(m, e->fileid));
        }
    }
    if (p->count != modelled) {
        ctx_fail(&m->c, "op %u: directory lists %u names, model has %u", m->ops, p->count,
                 modelled);
    }
    for (i = 0; i < m->dead_n; i++) {
        if (getattr_retry(m->c.cat, m->dead[i], &seen) != MDS_ERR_NOTFOUND) {
            ctx_fail(&m->c, "op %u: inode %llu lost its last name but still exists", m->ops,
                     (unsigned long long)m->dead[i]);
        }
    }
    (void)verify_parent(m);
    free(p);
}

static unsigned env_unsigned(const char *name, unsigned dflt, unsigned lo, unsigned hi)
{
    /* NOLINTNEXTLINE(concurrency-mt-unsafe) */
    const char *s = getenv(name);
    char *end = NULL;
    unsigned long v;

    if (s == NULL || s[0] == '\0') {
        return dflt;
    }
    errno = 0;
    v = strtoul(s, &end, 10);
    if (errno != 0 || end == NULL || *end != '\0' || v < lo || v > hi) {
        return dflt;
    }
    return (unsigned)v;
}

/* Scratch directory with a few retries: the child runs under buggify. */
static bool scratch_retry(struct mds_catalogue *cat, uint64_t *dir)
{
    int i;

    for (i = 0; i < 5; i++) {
        if (conformance_scratch_dir(cat, dir) == MDS_OK) {
            return true;
        }
        sleep_ms(200);
    }
    return false;
}

static void model_cleanup(struct model *m)
{
    unsigned i;

    for (i = 0; i < MODEL_SLOTS; i++) {
        if (m->e[i].used) {
            (void)mds_cat_ns_remove(m->c.cat, NULL, m->c.dir, m->e[i].name);
        }
    }
    conformance_scratch_cleanup(m->c.cat, m->c.dir);
}

static int child_buggify_mixed(void)
{
    struct mds_catalogue *cat = conformance_open_checked();
    struct model *m = calloc(1, sizeof(*m));
    struct mds_inode parent;
    struct stats_snap s;
    unsigned n = env_unsigned("FAULT_MIXED_OPS", MIXED_OPS_DEFAULT, 10, 1000000);
    unsigned i;
    int rc;

    if (m == NULL) {
        mds_catalogue_close(cat);
        conformance_shutdown();
        return 1;
    }
    m->c.cat = cat;
    m->seed = 0x9E3779B9U;
    if (!scratch_retry(cat, &m->c.dir) || getattr_retry(cat, m->c.dir, &parent) != MDS_OK) {
        (void)printf("    mixed: scratch directory could not be created\n");
        free(m);
        mds_catalogue_close(cat);
        conformance_shutdown();
        return 1;
    }
    m->expected_change = parent.change;
    /* A dispatch that finds no operand (full or empty model) executes
     * nothing; run until n operations were actually executed. */
    for (i = 0; m->ops < n && i < 4U * n && m->c.res != R_FAIL; i++) {
        unsigned before = m->ops;

        op_dispatch(m);
        if (m->ops != before && m->ops % MIXED_CHECK_EVERY == 0) {
            model_verify_all(m);
        }
    }
    if (m->c.res != R_FAIL) {
        sleep_ms(QUIESCENCE_MS);
        model_verify_all(m);
    }
    stats_snapshot(be_of(cat), &s);
    (void)printf("    mixed: %u ops (create %u remove %u rename %u rename-over %u link %u "
                 "setattr %u touch %u): OK %u, DELAY %u, IO %u, INDOUBT %u (%u landed)\n",
                 m->ops, m->per_kind[OP_CREATE], m->per_kind[OP_REMOVE], m->per_kind[OP_RENAME],
                 m->per_kind[OP_RENAME_OVER], m->per_kind[OP_LINK], m->per_kind[OP_SETATTR],
                 m->per_kind[OP_TOUCH], m->ok, m->delay, m->io, m->indoubt, m->indoubt_applied);
    stats_print("    mixed: ", &s);
    if (s.unknown_results + s.fences == 0) {
        ctx_fail(&m->c, "no commit-outcome fault was injected: buggify inactive");
    }
    if (m->c.res == R_FAIL) {
        (void)printf("    mixed: FAIL %s", m->c.why);
        if (m->c.extra_failures > 0) {
            (void)printf(" (+%u more)", m->c.extra_failures);
        }
        (void)printf("\n");
    }
    rc = (m->c.res == R_FAIL) ? 1 : 0;
    model_cleanup(m);
    free(m);
    mds_catalogue_close(cat);
    conformance_shutdown();
    return rc;
}

/* -----------------------------------------------------------------------
 * buggify_concurrent (child): 8 threads, distinct names, 30 s
 * ----------------------------------------------------------------------- */

#define CONC_THREADS    8
#define CONC_NAMES_MAX  4000U
#define CONC_SEC_DEFAULT 30U

enum conc_class {
    CC_OK = 0,
    CC_DEFINITIVE = 1,
    CC_INDOUBT = 2,
};

#define CONC_STATUS_MAX 32U

struct conc_thread {
    struct mds_catalogue *cat;
    uint64_t dir;
    int      idx;
    uint64_t stop_ms;
    unsigned n;
    uint8_t  cls[CONC_NAMES_MAX];
    bool     present[CONC_NAMES_MAX];
    bool     inode_ok[CONC_NAMES_MAX];
    unsigned ok;
    unsigned definitive;
    unsigned indoubt;
    unsigned unexpected;   /**< Statuses a distinct-name create cannot return. */
    unsigned hist[CONC_STATUS_MAX]; /**< Per status (-st), for the report. */
    unsigned first_unexpected;      /**< Index of the first one, or n. */
};

static void *conc_main(void *arg)
{
    struct conc_thread *t = arg;

    while (t->n < CONC_NAMES_MAX && monotonic_ms() < t->stop_ms) {
        struct mds_inode out;
        char name[32];
        enum mds_status st;

        (void)snprintf(name, sizeof(name), "t%d-%05u", t->idx, t->n);
        st = mds_cat_ns_create(t->cat, NULL, t->dir, name, MDS_FTYPE_REG, 0644, 0, 0, NULL,
                               &out);
        if ((unsigned)(-(int)st) < CONC_STATUS_MAX) {
            t->hist[(unsigned)(-(int)st)]++;
        }
        if (st == MDS_OK) {
            t->cls[t->n] = CC_OK;
            t->ok++;
        } else if (st == MDS_ERR_INDOUBT) {
            t->cls[t->n] = CC_INDOUBT;
            t->indoubt++;
        } else {
            t->cls[t->n] = CC_DEFINITIVE;
            t->definitive++;
            if (st != MDS_ERR_DELAY && st != MDS_ERR_IO) {
                if (t->unexpected == 0) {
                    t->first_unexpected = t->n;
                }
                t->unexpected++;
            }
        }
        t->n++;
    }
    return NULL;
}

/* Diagnostics for a failed run: status histogram and the state of the
 * keyspace the creates ran in. */
static void conc_diagnose(struct mds_catalogue *cat, uint64_t dir, const struct conc_thread *t,
                          int started)
{
    struct mds_inode seen;
    unsigned hist[CONC_STATUS_MAX];
    unsigned s;
    int i;

    memset(hist, 0, sizeof(hist));
    for (i = 0; i < started; i++) {
        for (s = 0; s < CONC_STATUS_MAX; s++) {
            hist[s] += t[i].hist[s];
        }
    }
    (void)printf("    concurrent: statuses:");
    for (s = 0; s < CONC_STATUS_MAX; s++) {
        if (hist[s] != 0) {
            (void)printf(" %s=%u", status_name((enum mds_status)(-(int)s)), hist[s]);
        }
    }
    (void)printf("; first unexpected at index");
    for (i = 0; i < started; i++) {
        (void)printf(" t%d:%u/%u", i, t[i].first_unexpected, t[i].n);
    }
    (void)printf("\n    concurrent: keyspace now: probe %s, root %s, dir %s\n",
                 status_name(mds_catalogue_probe(cat)),
                 status_name(mds_cat_ns_getattr(cat, MDS_FILEID_ROOT, &seen)),
                 status_name(mds_cat_ns_getattr(cat, dir, &seen)));
}

struct conc_scan {
    struct conc_thread *t;   /**< CONC_THREADS entries. */
    unsigned entries;
    unsigned foreign;        /**< Names no thread created. */
    unsigned no_inode;       /**< Dirents whose inode read failed. */
};

/* Parse "t<idx>-<n>"; false for anything else. */
static bool conc_parse(const char *name, int *idx, unsigned *n)
{
    char *end = NULL;
    unsigned long v;

    if (name[0] != 't') {
        return false;
    }
    errno = 0;
    v = strtoul(name + 1, &end, 10);
    if (errno != 0 || end == NULL || *end != '-' || v >= CONC_THREADS) {
        return false;
    }
    *idx = (int)v;
    v = strtoul(end + 1, &end, 10);
    if (errno != 0 || end == NULL || *end != '\0' || v >= CONC_NAMES_MAX) {
        return false;
    }
    *n = (unsigned)v;
    return true;
}

static int conc_scan_cb(const struct mds_cat_dirent *d, const struct mds_inode *ino,
                        bool inode_valid, void *arg)
{
    struct conc_scan *s = arg;
    int idx = 0;
    unsigned n = 0;

    (void)ino;
    s->entries++;
    if (!inode_valid) {
        s->no_inode++;
    }
    if (!conc_parse(d->name, &idx, &n)) {
        s->foreign++;
        return 0;
    }
    s->t[idx].present[n] = true;
    s->t[idx].inode_ok[n] = inode_valid;
    return 0;
}

/* Compare one thread's recorded statuses with what the directory lists. */
static void conc_check_thread(struct ctx *c, struct conc_thread *t, unsigned *applied)
{
    unsigned i;

    for (i = 0; i < t->n; i++) {
        bool present = t->present[i];

        if (t->cls[i] == CC_OK && !present) {
            ctx_fail(c, "t%d-%05u returned OK but is missing", t->idx, i);
        } else if (t->cls[i] == CC_DEFINITIVE && present) {
            ctx_fail(c, "t%d-%05u failed definitively but exists", t->idx, i);
        } else if (t->cls[i] == CC_INDOUBT && present) {
            (*applied)++;
        }
        if (present && !t->inode_ok[i]) {
            ctx_fail(c, "t%d-%05u is a dirent without an inode", t->idx, i);
        }
    }
}

static int child_buggify_concurrent(void)
{
    struct mds_catalogue *cat = conformance_open_checked();
    struct conc_thread *threads = calloc(CONC_THREADS, sizeof(*threads));
    struct conc_scan scan;
    struct ctx c;
    struct mds_inode parent;
    struct mds_inode after;
    struct stats_snap s;
    pthread_t tids[CONC_THREADS];
    unsigned seconds = env_unsigned("FAULT_CONC_SEC", CONC_SEC_DEFAULT, 1, 3600);
    unsigned ok = 0;
    unsigned definitive = 0;
    unsigned indoubt = 0;
    unsigned applied = 0;
    unsigned unexpected = 0;
    uint64_t dir = 0;
    int started = 0;
    int i;

    memset(&c, 0, sizeof(c));
    c.cat = cat;
    if (threads == NULL || !scratch_retry(cat, &dir) ||
        getattr_retry(cat, dir, &parent) != MDS_OK) {
        (void)printf("    concurrent: setup failed\n");
        free(threads);
        mds_catalogue_close(cat);
        conformance_shutdown();
        return 1;
    }
    for (i = 0; i < CONC_THREADS; i++) {
        threads[i].cat = cat;
        threads[i].dir = dir;
        threads[i].idx = i;
        threads[i].stop_ms = monotonic_ms() + (uint64_t)seconds * 1000ULL;
        if (pthread_create(&tids[i], NULL, conc_main, &threads[i]) != 0) {
            break;
        }
        started++;
    }
    for (i = 0; i < started; i++) {
        (void)pthread_join(tids[i], NULL);
        ok += threads[i].ok;
        definitive += threads[i].definitive;
        indoubt += threads[i].indoubt;
        unexpected += threads[i].unexpected;
    }
    if (started != CONC_THREADS) {
        ctx_fail(&c, "only %d of %d threads started", started, CONC_THREADS);
    }
    if (unexpected > 0) {
        ctx_fail(&c, "%u distinct-name creates returned a status other than OK / DELAY / IO / "
                 "INDOUBT", unexpected);
    }
    sleep_ms(QUIESCENCE_MS);
    memset(&scan, 0, sizeof(scan));
    scan.t = threads;
    if (mds_cat_ns_readdir_plus_from_cookie(cat, dir, 0, 0, NULL, conc_scan_cb, &scan) != MDS_OK) {
        ctx_fail(&c, "readdir_plus after quiescence failed");
    }
    for (i = 0; i < started; i++) {
        conc_check_thread(&c, &threads[i], &applied);
    }
    if (scan.foreign != 0 || scan.no_inode != 0) {
        ctx_fail(&c, "%u foreign names, %u dirents without inode", scan.foreign, scan.no_inode);
    }
    if (scan.entries != ok + applied) {
        ctx_fail(&c, "directory lists %u names, %u were OK'd and %u in-doubt ones landed",
                 scan.entries, ok, applied);
    }
    if (getattr_retry(cat, dir, &after) != MDS_OK) {
        ctx_fail(&c, "parent getattr failed");
    } else if (after.nlink != 2 || after.change != parent.change + ok + applied) {
        ctx_fail(&c, "parent nlink %u change +%llu, expected nlink 2 change +%u", after.nlink,
                 (unsigned long long)(after.change - parent.change), ok + applied);
    }
    stats_snapshot(be_of(cat), &s);
    (void)printf("    concurrent: %d threads x %u s: %u creates, OK %u, definitive %u, "
                 "INDOUBT %u (%u landed), %u names listed\n", started, seconds,
                 ok + definitive + indoubt, ok, definitive, indoubt, applied, scan.entries);
    stats_print("    concurrent: ", &s);
    if (s.unknown_results + s.fences == 0) {
        ctx_fail(&c, "no commit-outcome fault was injected: buggify inactive");
    }
    if (c.res == R_FAIL) {
        (void)printf("    concurrent: FAIL %s", c.why);
        if (c.extra_failures > 0) {
            (void)printf(" (+%u more)", c.extra_failures);
        }
        (void)printf("\n");
        conc_diagnose(cat, dir, threads, started);
    }
    /* Cleanup is bounded by the harness helper (one page); the run's
     * prefix is cleared by conformance_shutdown anyway. */
    conformance_scratch_cleanup(cat, dir);
    free(threads);
    mds_catalogue_close(cat);
    conformance_shutdown();
    return (c.res == R_FAIL) ? 1 : 0;
}

/* -----------------------------------------------------------------------
 * lifecycle (child): open/close churn over two prefixes from four
 * threads, then the process-wide shutdown; the network thread must be
 * gone afterwards.
 * ----------------------------------------------------------------------- */

#define LC_THREADS 4
#define LC_CYCLES  50

struct lc_thread {
    const char *prefix[2];
    int idx;
    int failures;
};

static void *lc_main(void *arg)
{
    struct lc_thread *t = arg;
    int cycle;

    for (cycle = 0; cycle < LC_CYCLES; cycle++) {
        struct mds_catalogue *cat = NULL;
        struct mds_inode out;
        struct mds_inode seen;
        char name[32];

        (void)snprintf(name, sizeof(name), "l%d-%d", t->idx, cycle);
        if (open_private(t->prefix[(t->idx + cycle) % 2], 1, 0, &cat) != MDS_OK) {
            t->failures++;
            continue;
        }
        if (mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, name, MDS_FTYPE_REG, 0644, 0, 0, NULL,
                              &out) != MDS_OK ||
            mds_cat_ns_lookup(cat, MDS_FILEID_ROOT, name, &seen) != MDS_OK ||
            seen.fileid != out.fileid ||
            mds_cat_ns_remove(cat, NULL, MDS_FILEID_ROOT, name) != MDS_OK) {
            t->failures++;
        }
        mds_catalogue_close(cat);
    }
    return NULL;
}

static int count_tasks(void)
{
    DIR *d = opendir("/proc/self/task");
    struct dirent *de;
    int n = 0;

    if (d == NULL) {
        return -1;
    }
    /* Only called while no other thread of this process reads a
     * directory (the test threads are joined; the client network
     * thread never does), so the non-reentrant readdir has no
     * concurrent user. */
    /* NOLINTNEXTLINE(concurrency-mt-unsafe) */
    while ((de = readdir(d)) != NULL) {
        if (de->d_name[0] != '.') {
            n++;
        }
    }
    (void)closedir(d);
    return n;
}

/* Two reads this far apart must agree before a task count is trusted:
 * a joined thread's /proc entry can outlive pthread_join by the tail of
 * its kernel exit path.  Bounded by TASKS_SETTLE_ROUNDS. */
#define TASKS_SETTLE_STEP_MS 10U
#define TASKS_SETTLE_ROUNDS  100

static int count_tasks_settled(void)
{
    int prev = count_tasks();
    int round;

    for (round = 0; round < TASKS_SETTLE_ROUNDS; round++) {
        int now;

        sleep_ms(TASKS_SETTLE_STEP_MS);
        now = count_tasks();
        if (now == prev) {
            return now;
        }
        prev = now;
    }
    return prev;
}

static void *noop_thread_main(void *arg)
{
    (void)arg;
    return NULL;
}

/*
 * Task count the lifecycle check compares against.  It is taken before
 * the first catalogue open, so it does not include the client network
 * thread -- but a sanitizer runtime spawns its own long-lived helper
 * thread lazily (TSan: on the first pthread_create of the process), and
 * a baseline read before any thread existed would attribute that
 * helper to the client.  One no-op thread, created and joined here,
 * makes the runtime pay that cost first.
 */
static int lifecycle_task_baseline(void)
{
    pthread_t t;

    if (pthread_create(&t, NULL, noop_thread_main, NULL) == 0) {
        (void)pthread_join(t, NULL);
    }
    return count_tasks_settled();
}

/*
 * The task count is judged against the process's own baseline, not
 * against an absolute 1: "one task left" is only right for a plain
 * build (see lifecycle_task_baseline).  The client network adds exactly
 * one thread while it runs and mds_catalogue_process_shutdown joins it.
 */
static int child_lifecycle(void)
{
    struct lc_thread threads[LC_THREADS];
    pthread_t tids[LC_THREADS];
    struct mds_catalogue *cat = NULL;
    char p1[32];
    char p2[32];
    int i;
    int failures = 0;
    int started = 0;
    int tasks_baseline;
    int tasks_before;
    int tasks_after;

    tasks_baseline = lifecycle_task_baseline();
    (void)snprintf(p1, sizeof(p1), "fl1-%ld", (long)getpid());
    (void)snprintf(p2, sizeof(p2), "fl2-%ld", (long)getpid());
    for (i = 0; i < 2; i++) {
        if (open_private_bootstrapped(i == 0 ? p1 : p2, 1, true, &cat) != MDS_OK) {
            (void)printf("    lifecycle: bootstrap of prefix %d failed\n", i + 1);
            mds_catalogue_process_shutdown();
            return 1;
        }
        mds_catalogue_close(cat);
    }
    tasks_before = count_tasks_settled();
    memset(threads, 0, sizeof(threads));
    for (i = 0; i < LC_THREADS; i++) {
        threads[i].prefix[0] = p1;
        threads[i].prefix[1] = p2;
        threads[i].idx = i;
        if (pthread_create(&tids[i], NULL, lc_main, &threads[i]) != 0) {
            break;
        }
        started++;
    }
    for (i = 0; i < started; i++) {
        (void)pthread_join(tids[i], NULL);
        failures += threads[i].failures;
    }
    if (started != LC_THREADS) {
        failures++;
    }
    for (i = 0; i < 2; i++) {
        if (open_private(i == 0 ? p1 : p2, 1, 0, &cat) == MDS_OK) {
            (void)catalogue_fdb_keyspace_clear(cat);
            mds_catalogue_close(cat);
        } else {
            failures++;
        }
    }
    if (open_private(p1, 1, 0, &cat) == MDS_OK) {
        sweep_witness_table(be_of(cat), p1);
        sweep_witness_table(be_of(cat), p2);
        mds_catalogue_close(cat);
    }
    mds_catalogue_process_shutdown();
    tasks_after = count_tasks_settled();
    (void)printf("    lifecycle: %d threads x %d cycles over 2 prefixes, %d failures; tasks "
                 "before the first open %d, before shutdown %d, after %d\n", started, LC_CYCLES,
                 failures, tasks_baseline, tasks_before, tasks_after);
    if (tasks_before != tasks_baseline + 1) {
        (void)printf("    lifecycle: expected exactly one client network thread while open\n");
        return 1;
    }
    if (tasks_after != tasks_baseline) {
        (void)printf("    lifecycle: the client network thread is still alive\n");
        return 1;
    }
    return failures == 0 ? 0 : 1;
}

/* -----------------------------------------------------------------------
 * Child-spawning sub-tests (parent side)
 * ----------------------------------------------------------------------- */

static char g_env_enable[] = "FDB_NETWORK_OPTION_CLIENT_BUGGIFY_ENABLE=";
static char g_env_activated[] =
    "FDB_NETWORK_OPTION_CLIENT_BUGGIFY_SECTION_ACTIVATED_PROBABILITY=100";
static char g_env_fired[80];

static void child_result(struct ctx *c, const char *what, int rc)
{
    if (rc == 0) {
        return;
    }
    if (rc == CONFORMANCE_SKIP) {
        ctx_skip(c, "child could not open the backend");
        return;
    }
    ctx_fail(c, "%s child exited %d", what, rc);
}

static void run_buggify_child(struct ctx *c, const char *which)
{
    char *env[3];
    unsigned fired = env_unsigned("FAULT_BUGGIFY_FIRED", 25, 1, 100);

    (void)snprintf(g_env_fired, sizeof(g_env_fired),
                   "FDB_NETWORK_OPTION_CLIENT_BUGGIFY_SECTION_FIRED_PROBABILITY=%u", fired);
    env[0] = g_env_enable;
    env[1] = g_env_activated;
    env[2] = g_env_fired;
    (void)printf("    client buggify: activated 100%%, fired %u%%\n", fired);
    child_result(c, which, run_child(which, NULL, 0, env, 3));
}

static void buggify_mixed(struct ctx *c)
{
    run_buggify_child(c, "mixed");
}

static void buggify_concurrent(struct ctx *c)
{
    run_buggify_child(c, "concurrent");
}

static void lifecycle(struct ctx *c)
{
    child_result(c, "lifecycle", run_child("lifecycle", NULL, 0, NULL, 0));
}

/* -----------------------------------------------------------------------
 * Driver
 * ----------------------------------------------------------------------- */

struct subtest {
    const char *name;
    void (*fn)(struct ctx *c);
    bool needs_scratch;
};

static const struct subtest subtests[] = {
    { "scripted_unknown_result",   scripted_unknown_result,   true  },
    { "scripted_timed_out",        scripted_timed_out,        true  },
    { "scripted_fence_exhaustion", scripted_fence_exhaustion, true  },
    { "scripted_probe_exhaustion", scripted_probe_exhaustion, true  },
    { "size_bounds",               size_bounds,               true  },
    { "schema_refusal",            schema_refusal,            false },
    { "witness_restart",           witness_restart,           false },
    { "keyspace_clear_witness",    keyspace_clear_witness,    false },
    { "buggify_mixed",             buggify_mixed,             false },
    { "buggify_concurrent",        buggify_concurrent,        false },
    { "lifecycle",                 lifecycle,                 false },
};

/* Below the 1800 s ctest limit: the binary, not ctest, reports a hang. */
#define SUITE_TIMEOUT_SEC 1500

static int child_main(const char *mode, int argc, char **argv)
{
    if (strcmp(mode, "mixed") == 0) {
        return child_buggify_mixed();
    }
    if (strcmp(mode, "concurrent") == 0) {
        return child_buggify_concurrent();
    }
    if (strcmp(mode, "lifecycle") == 0) {
        return child_lifecycle();
    }
    if (strcmp(mode, "restart") == 0 && argc == 2) {
        char *end = NULL;
        unsigned long long epoch;

        errno = 0;
        epoch = strtoull(argv[1], &end, 10);
        if (errno != 0 || end == NULL || *end != '\0') {
            return 2;
        }
        return child_restart(argv[0], (uint64_t)epoch);
    }
    (void)fprintf(stderr, "test_fault: unknown child mode %s\n", mode);
    return 2;
}

static void report(const struct subtest *t, const struct ctx *c, unsigned *passed,
                   unsigned *failed, unsigned *skipped)
{
    switch (c->res) {
    case R_PASS:
        (*passed)++;
        (void)printf("  PASS %s\n", t->name);
        break;
    case R_SKIP:
        (*skipped)++;
        (void)printf("  SKIP %s: %s\n", t->name, c->why);
        break;
    case R_FAIL:
    default:
        (*failed)++;
        (void)printf("  FAIL %s: %s", t->name, c->why);
        if (c->extra_failures > 0) {
            (void)printf(" (+%u more)", c->extra_failures);
        }
        (void)printf("\n");
        break;
    }
    (void)fflush(stdout);
}

int main(int argc, char **argv)
{
    struct mds_catalogue *cat;
    struct watchdog wd;
    pthread_t wd_thread;
    const char *only = NULL;
    unsigned passed = 0;
    unsigned failed = 0;
    unsigned skipped = 0;
    size_t i;

    self_path_set(argc > 0 ? argv[0] : NULL);
    if (argc > 1 && strncmp(argv[1], "--child-", 8) == 0) {
        return child_main(argv[1] + 8, argc - 2, argv + 2);
    }
    if (argc > 1) {
        only = argv[1];
    }
    if (!conformance_backend_is("fdb")) {
        conformance_skip("fault injection runs against the fdb backend only");
    }
    cat = conformance_open_checked();
    (void)printf("test_fault (backend=%s):\n", conformance_backend_name());
    if (watchdog_start(&wd, &wd_thread, SUITE_TIMEOUT_SEC) != 0) {
        (void)fprintf(stderr, "cannot start the suite watchdog\n");
        mds_catalogue_close(cat);
        conformance_shutdown();
        return 1;
    }
    for (i = 0; i < sizeof(subtests) / sizeof(subtests[0]); i++) {
        struct ctx c;

        if (only != NULL && strcmp(only, subtests[i].name) != 0) {
            continue;
        }
        memset(&c, 0, sizeof(c));
        c.cat = cat;
        c.res = R_PASS;
        atomic_store(&g_running_subtest, subtests[i].name);
        if (subtests[i].needs_scratch && conformance_scratch_dir(cat, &c.dir) != MDS_OK) {
            ctx_fail(&c, "cannot create scratch directory");
        } else {
            subtests[i].fn(&c);
        }
        if (subtests[i].needs_scratch && c.dir != 0) {
            conformance_scratch_cleanup(cat, c.dir);
        }
        atomic_store(&g_running_subtest, NULL);
        report(&subtests[i], &c, &passed, &failed, &skipped);
    }
    watchdog_stop(&wd, wd_thread);
    mds_catalogue_close(cat);
    (void)printf("\ntest_fault: %u passed, %u failed, %u skipped\n", passed, failed, skipped);
    conformance_shutdown();
    return (failed == 0) ? 0 : 1;
}

#else /* !HAVE_FDB */

int main(void)
{
    (void)printf("test_fault: SKIP (fdb backend not built)\n");
    return CONFORMANCE_SKIP;
}

#endif /* HAVE_FDB */

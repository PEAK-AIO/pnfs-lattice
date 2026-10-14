/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * harness.c -- Backend-parameterised catalogue conformance harness.
 *
 * See harness.h for the contract.  The harness deliberately knows
 * nothing about the tests: it opens a catalogue on the requested
 * backend, turns "backend unavailable" into exit code 77 and provides
 * a scratch directory so the same binaries run against a persistent
 * store without colliding.
 */

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "harness.h"
#ifdef HAVE_FDB
#include "catalogue_fdb.h"
#include "catalogue_internal.h"
#include "fdb_txn.h"
#endif
#ifdef HAVE_DAOS
#include <daos.h>

#include "daos_txn.h"
#endif

/* Bounded table of scratch directories created by this process so the
 * cleanup helper can remove them by (root, name) again.  Beyond the
 * bound the cleanup falls back to the (public) child-to-name lookup. */
#define SCRATCH_MAX 256

struct scratch_entry {
    const struct mds_catalogue *cat; /**< Independent instances reuse fileids. */
    uint64_t fid;
    char     name[64];
};

static pthread_mutex_t   g_scratch_lock = PTHREAD_MUTEX_INITIALIZER;
static struct scratch_entry g_scratch[SCRATCH_MAX];
static uint32_t          g_scratch_count;
static uint32_t          g_scratch_seq;

_Static_assert(CONFORMANCE_SKIP == 77,
               "ctest SKIP_RETURN_CODE in tests/catalogue_conformance/"
               "CMakeLists.txt is 77");

const char *conformance_backend_name(void)
{
    const char *name = getenv(CONFORMANCE_BACKEND_ENV);

    if (name == NULL || name[0] == '\0') {
        return "memdb";
    }
    return name;
}

bool conformance_backend_is(const char *name)
{
    if (name == NULL) {
        return false;
    }
    return strcmp(conformance_backend_name(), name) == 0;
}

_Noreturn void conformance_skip(const char *reason)
{
    (void)fprintf(stdout, "SKIP: %s (backend=%s)\n",
                  reason != NULL ? reason : "no reason given",
                  conformance_backend_name());
    (void)fflush(stdout);
    exit(CONFORMANCE_SKIP);
}

/* memdb: the in-process reference backend, opened through the backend
 * registration table (catalogue_factory.c) exactly like the daemon
 * does for `catalogue_backend = memdb`.  The instance takes nothing
 * from the configuration; an all-zero block with the backend set is
 * the documented "defaults" input (catalogue_memdb.h). */
static enum mds_status open_memdb(struct mds_catalogue **out)
{
    struct mds_config *cfg;
    enum mds_status st;

    if (!mds_catalogue_backend_available(MDS_BACKEND_MEMDB)) {
        conformance_skip("memdb backend not compiled in "
                         "(ENABLE_MEMDB_BACKEND=OFF)");
    }
    /* struct mds_config is large (per-DS spec strings); never on the
     * stack of a test thread. */
    cfg = calloc(1, sizeof(*cfg));
    if (cfg == NULL) {
        return MDS_ERR_NOMEM;
    }
    cfg->catalogue_backend = MDS_BACKEND_MEMDB;

    st = mds_catalogue_open(cfg, out);
    free(cfg);
    return st;
}

static enum mds_status open_rondb(uint32_t self_id, struct mds_catalogue **out)
{
#ifdef HAVE_RONDB
    const char *conf = getenv(CONFORMANCE_RONDB_CONF_ENV);
    struct mds_config *cfg;
    enum mds_status st;

    if (conf == NULL || conf[0] == '\0') {
        conformance_skip(CONFORMANCE_RONDB_CONF_ENV " is not set");
    }
    /* struct mds_config is large (per-DS spec strings); never on the
     * stack of a test thread. */
    cfg = calloc(1, sizeof(*cfg));
    if (cfg == NULL) {
        return MDS_ERR_NOMEM;
    }
    cfg->catalogue_backend = MDS_BACKEND_RONDB;
    (void)snprintf(cfg->catalogue_backend_conf,
                   sizeof(cfg->catalogue_backend_conf), "%s", conf);
    cfg->self.id = self_id;
    (void)snprintf(cfg->self.hostname, sizeof(cfg->self.hostname),
                   "localhost");
    cfg->cluster_size = 1;
    cfg->ndb_conn_pool_size = 2;
    cfg->ndb_async_writes = false;
    cfg->catalog_image_mode = MDS_IMAGE_OFF;
    cfg->catalog_replay_mode = MDS_REPLAY_OFF;

    st = mds_catalogue_open(cfg, out);
    free(cfg);
    return st;
#else
    (void)self_id;
    (void)out;
    conformance_skip("RonDB backend not compiled in (ENABLE_RONDB=OFF)");
#endif
}

#ifdef HAVE_FDB
/*
 * FoundationDB: every open of the process shares one key prefix, taken
 * from CATALOGUE_TEST_KEY_PREFIX or generated once per process, so
 * concurrent binaries against one fdbserver never see each other's
 * rows.  The prefix range is cleared and bootstrapped on the first
 * open and cleared again by conformance_shutdown(), which then stops
 * the client network (mds_catalogue_process_shutdown; terminal for the
 * process).  An atexit() hook runs the same teardown for a main that
 * exits without the explicit call.
 *
 * The teardown clears in two steps.  catalogue_fdb_keyspace_clear runs
 * through the transaction runner and therefore must leave the WITNESS
 * table alone (fdb_txn.h, body rule) -- and writes its own witness row
 * -- so on its own it leaves exactly the run's WITNESS rows behind.
 * fdb_sweep_prefix() then wipes the whole prefix, WITNESS included,
 * with a bare fdb_c transaction outside the runner and reads the range
 * back.  That is safe only because every handle of this process is
 * closed by then (no attempt is in flight whose outcome those rows
 * could still resolve) and the prefix is private to this process.
 */
static pthread_mutex_t g_fdb_lock = PTHREAD_MUTEX_INITIALIZER;
static char            g_fdb_prefix[32];
static bool            g_fdb_prepared;
static bool            g_fdb_torn_down;

/** Attempts of the bare sweep's clear and of its read-back, each on a
 *  fresh transaction with a linear backoff between them. */
#define FDB_SWEEP_ROUNDS 8
/** Backoff step between attempts, ms (round n sleeps n * step). */
#define FDB_SWEEP_BACKOFF_MS 50U
/** Per-attempt timeout of the bare sweep and its read-back, ms. */
#define FDB_SWEEP_TIMEOUT_MS 4000U
/** Rows the read-back fetches at most; more than that reports "+". */
#define FDB_SWEEP_READBACK_LIMIT 16

static void sweep_sleep_ms(unsigned ms)
{
    struct timespec ts;

    ts.tv_sec = (time_t)(ms / 1000U);
    ts.tv_nsec = (long)(ms % 1000U) * 1000000L;
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
        /* resume the remainder */
    }
}

/* Errors after which a fresh attempt is safe: everything the client
 * itself calls retryable, plus 1025 / 1031, whose commit MAY still be
 * in flight -- harmless here because a range clear is idempotent (an
 * attempt landing late clears an already empty range).  The client's
 * fault injection reports 1031 for a commit it issues late, so the
 * buggify children hit this. */
static bool sweep_retryable(fdb_error_t err)
{
    return fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, err) != 0 ||
           err == FDB_ERR_TRANSACTION_CANCELLED || err == FDB_ERR_TRANSACTION_TIMED_OUT;
}

/* One attempt on a fresh transaction: commit a clear of @p r, or
 * (@p readback) count the rows of @p r with a snapshot read.  The
 * transaction is never reused: after 1025 / 1031 a commit may still
 * own it (fdb_txn.h). */
static fdb_error_t sweep_attempt(FDBDatabase *db, const struct fdb_key_range *r, bool readback,
                                 int *remaining, bool *more)
{
    FDBTransaction *tr = NULL;
    FDBFuture *f = NULL;
    const FDBKeyValue *kvs = NULL;
    fdb_error_t err = fdb_database_create_transaction(db, &tr);

    if (err != 0 || tr == NULL) {
        return err != 0 ? err : FDB_ERR_PLATFORM_ERROR;
    }
    err = fdb_txn_set_timeout(tr, FDB_SWEEP_TIMEOUT_MS);
    if (err == 0) {
        if (readback) {
            f = fdb_txn_get_range_start(tr, r, FDB_SWEEP_READBACK_LIMIT, true, false);
            err = fdb_txn_get_range_wait(f, &kvs, remaining, more);
        } else {
            fdb_txn_clear_range(tr, r);
            f = fdb_transaction_commit(tr);
            err = fdb_txn_wait(f);
        }
        if (f != NULL) {
            fdb_future_destroy(f);
        }
    }
    fdb_transaction_destroy(tr);
    return err;
}

static fdb_error_t sweep_with_retry(FDBDatabase *db, const struct fdb_key_range *r,
                                    bool readback, int *remaining, bool *more)
{
    fdb_error_t err = 0;
    int round;

    for (round = 0; round < FDB_SWEEP_ROUNDS; round++) {
        err = sweep_attempt(db, r, readback, remaining, more);
        if (err == 0 || !sweep_retryable(err)) {
            break;
        }
        sweep_sleep_ms(FDB_SWEEP_BACKOFF_MS * (unsigned)(round + 1));
    }
    return err;
}

/*
 * Range-clear [prefix, strinc(prefix)) with bare fdb_c transactions
 * and read the range back with a fresh snapshot read (its read version
 * is causally after a committed clear).  The read-back runs even when
 * every clear attempt reported an error: a clear reported as 1031 may
 * have landed, and the row count is what the caller reports.  Refuses
 * an empty prefix (that would erase the whole database).
 *
 * @param b          Backend of a still-open handle (its database).
 * @param remaining  Receives the row count the read-back saw (-1 when
 *                   the read-back itself failed).
 * @param more       Receives whether the read-back hit its limit.
 * @return 0, or the fdb_error_t of the clear (first) or the read-back.
 */
static fdb_error_t fdb_sweep_prefix(const struct fdb_backend *b, int *remaining, bool *more)
{
    struct fdb_key base;
    struct fdb_key_range r;
    fdb_error_t err;
    fdb_error_t rerr;

    *remaining = -1;
    *more = false;
    if (b->prefix.len == 0) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    base.len = b->prefix.len;
    base.overflow = false;
    memcpy(base.buf, b->prefix.bytes, b->prefix.len);
    if (!fdb_key_range_prefix(&r, &base)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    err = sweep_with_retry(b->db, &r, false, remaining, more);
    rerr = sweep_with_retry(b->db, &r, true, remaining, more);
    if (rerr != 0) {
        *remaining = -1;
    }
    return err != 0 ? err : rerr;
}

static void fdb_fill_cfg(struct mds_config *cfg, uint32_t self_id)
{
    const char *cluster = getenv("FDB_CLUSTER_FILE");

    cfg->catalogue_backend = MDS_BACKEND_FDB;
    if (cluster != NULL && cluster[0] != '\0') {
        (void)snprintf(cfg->fdb_cluster_file, sizeof(cfg->fdb_cluster_file), "%s",
                       cluster);
    }
    (void)snprintf(cfg->fdb_key_prefix, sizeof(cfg->fdb_key_prefix), "%s",
                   g_fdb_prefix);
    cfg->self.id = self_id;
    cfg->cluster_size = 1;
}

/* Once per process, after the last test handle is closed: wipe the
 * run's rows with a fresh handle (catalogue rows through the runner,
 * then the whole prefix with the bare sweep), report what the read-back
 * still sees, then stop the client network. */
static void fdb_teardown_once(void)
{
    struct mds_config *cfg;
    struct mds_catalogue *cat = NULL;
    int remaining = -1;
    bool more = false;
    fdb_error_t err;

    pthread_mutex_lock(&g_fdb_lock);
    if (!g_fdb_prepared || g_fdb_torn_down) {
        pthread_mutex_unlock(&g_fdb_lock);
        return;
    }
    g_fdb_torn_down = true;
    pthread_mutex_unlock(&g_fdb_lock);

    cfg = calloc(1, sizeof(*cfg));
    if (cfg != NULL) {
        fdb_fill_cfg(cfg, 1);
        if (mds_catalogue_open(cfg, &cat) == MDS_OK) {
            (void)catalogue_fdb_keyspace_clear(cat);
            err = fdb_sweep_prefix(cat->backend_private, &remaining, &more);
            if (remaining == 0) {
                (void)printf("conformance: fdb prefix '%s' swept, 0 rows remain\n",
                             g_fdb_prefix);
            } else if (remaining < 0) {
                (void)fprintf(stderr, "conformance: fdb prefix '%s' sweep failed: %d %s\n",
                              g_fdb_prefix, (int)err, fdb_get_error(err));
            } else {
                (void)fprintf(stderr, "conformance: fdb prefix '%s' still holds %d%s row(s) "
                              "after the sweep (clear: %d %s)\n", g_fdb_prefix, remaining,
                              more ? "+" : "", (int)err, fdb_get_error(err));
            }
            mds_catalogue_close(cat);
        }
        free(cfg);
    }
    mds_catalogue_process_shutdown();
}

/* Safety net for a main that exits without conformance_shutdown(). */
static void fdb_atexit(void)
{
    fdb_teardown_once();
}

/* Once per process: choose the prefix, clear it, bootstrap the root. */
static enum mds_status fdb_prepare_once(void)
{
    struct mds_config *cfg;
    struct mds_catalogue *cat = NULL;
    const char *env;
    enum mds_status st;

    pthread_mutex_lock(&g_fdb_lock);
    if (g_fdb_prepared) {
        pthread_mutex_unlock(&g_fdb_lock);
        return MDS_OK;
    }
    env = getenv("CATALOGUE_TEST_KEY_PREFIX");
    if (env != NULL && env[0] != '\0') {
        (void)snprintf(g_fdb_prefix, sizeof(g_fdb_prefix), "%s", env);
    } else {
        struct timespec ts;

        (void)clock_gettime(CLOCK_REALTIME, &ts);
        (void)snprintf(g_fdb_prefix, sizeof(g_fdb_prefix), "ct-%ld-%08lx",
                       (long)getpid(), (unsigned long)ts.tv_nsec);
    }
    cfg = calloc(1, sizeof(*cfg));
    if (cfg == NULL) {
        pthread_mutex_unlock(&g_fdb_lock);
        return MDS_ERR_NOMEM;
    }
    fdb_fill_cfg(cfg, 1);
    st = mds_catalogue_open(cfg, &cat);
    free(cfg);
    if (st != MDS_OK) {
        pthread_mutex_unlock(&g_fdb_lock);
        return st;
    }
    st = catalogue_fdb_keyspace_clear(cat);
    if (st == MDS_OK) {
        st = mds_catalogue_bootstrap(cat);
    }
    mds_catalogue_close(cat);
    if (st == MDS_OK) {
        g_fdb_prepared = true;
        (void)atexit(fdb_atexit);
    }
    pthread_mutex_unlock(&g_fdb_lock);
    return st;
}
#endif /* HAVE_FDB */

static enum mds_status open_fdb(uint32_t self_id, struct mds_catalogue **out)
{
#ifdef HAVE_FDB
    struct mds_config *cfg;
    const char *cluster;
    enum mds_status st;

    if (!mds_catalogue_backend_available(MDS_BACKEND_FDB)) {
        conformance_skip("fdb backend not compiled in (ENABLE_FDB=OFF)");
    }
    cluster = getenv("FDB_CLUSTER_FILE");
    if (cluster == NULL || cluster[0] == '\0') {
        cluster = "/etc/foundationdb/fdb.cluster";
    }
    if (access(cluster, R_OK) != 0) {
        conformance_skip("no readable FoundationDB cluster file "
                         "(FDB_CLUSTER_FILE / /etc/foundationdb/fdb.cluster)");
    }
    st = fdb_prepare_once();
    if (st != MDS_OK) {
        (void)fprintf(stdout, "SKIP: FoundationDB cluster not usable (%d)\n", (int)st);
        conformance_skip("FoundationDB cluster unreachable");
    }
    cfg = calloc(1, sizeof(*cfg));
    if (cfg == NULL) {
        return MDS_ERR_NOMEM;
    }
    fdb_fill_cfg(cfg, self_id);
    st = mds_catalogue_open(cfg, out);
    free(cfg);
    return st;
#else
    (void)self_id;
    (void)out;
    conformance_skip("fdb backend not compiled in (ENABLE_FDB=OFF)");
#endif
}

#ifdef HAVE_DAOS
/*
 * DAOS: one container per process.  Every open of the process shares
 * one container on the pool named by DAOS_POOL (system DAOS_SYS,
 * default daos_server): the one named by CATALOGUE_TEST_CONT -- an
 * operator-owned container that is neither created nor destroyed here
 * and keeps its rows across runs like a RonDB cluster, so the unit
 * tests that expect an empty catalogue need a fresh one -- or a private
 * container labelled ct-<pid>-<nsec> that the first open creates with
 * daos_cont_create_with_label and conformance_shutdown() destroys
 * (force, so a handle a test leaked cannot keep it alive).  Concurrent
 * binaries against one pool therefore never see each other's rows and
 * leave nothing behind.  The container is bootstrapped on the first
 * open; the teardown finally runs mds_catalogue_process_shutdown.  An
 * atexit() hook runs the same teardown for a main that exits without
 * the explicit call.
 *
 * The create and the destroy are bare libdaos calls outside any
 * catalogue handle.  daos_init / daos_fini are reference counted, so
 * the harness holds its own count and pool connection from the create
 * to the destroy, independent of the backend's own use of the library;
 * the backend's handles are all closed by the time the destroy runs.
 */
static pthread_mutex_t g_daos_lock = PTHREAD_MUTEX_INITIALIZER;
static char            g_daos_pool[128];
static char            g_daos_cont[128];
static char            g_daos_sys[16];
static daos_handle_t   g_daos_poh;
static bool            g_daos_connected;    /* harness init + pool connection held */
static bool            g_daos_cont_private; /* created here: destroyed at teardown */
static bool            g_daos_prepared;
static bool            g_daos_torn_down;

/* Copy the pool, system and (optional) container names from the
 * environment.  False when DAOS_POOL is unset. */
static bool conf_daos_env(void)
{
    const char *pool = getenv(CONFORMANCE_DAOS_POOL_ENV);
    const char *sys = getenv(CONFORMANCE_DAOS_SYS_ENV);
    const char *cont = getenv(CONFORMANCE_DAOS_CONT_ENV);

    if (pool == NULL || pool[0] == '\0') {
        return false;
    }
    (void)snprintf(g_daos_pool, sizeof(g_daos_pool), "%s", pool);
    (void)snprintf(g_daos_sys, sizeof(g_daos_sys), "%s",
                   (sys != NULL && sys[0] != '\0') ? sys : DAOS_SYS_DEFAULT);
    if (cont != NULL && cont[0] != '\0') {
        (void)snprintf(g_daos_cont, sizeof(g_daos_cont), "%s", cont);
        g_daos_cont_private = false;
    } else {
        struct timespec ts;

        (void)clock_gettime(CLOCK_REALTIME, &ts);
        (void)snprintf(g_daos_cont, sizeof(g_daos_cont), "ct-%ld-%08lx", (long)getpid(),
                       (unsigned long)ts.tv_nsec);
        g_daos_cont_private = true;
    }
    return true;
}

/* The harness's own library count and pool connection (private
 * container only).  0, or the -DER_* code of the step that failed. */
static int conf_daos_connect(void)
{
    int rc = daos_init();

    if (rc != 0) {
        return rc;
    }
    rc = daos_pool_connect(g_daos_pool, g_daos_sys, DAOS_PC_RW, &g_daos_poh, NULL, NULL);
    if (rc != 0) {
        (void)daos_fini();
        return rc;
    }
    g_daos_connected = true;
    return 0;
}

static void conf_daos_disconnect(void)
{
    if (!g_daos_connected) {
        return;
    }
    (void)daos_pool_disconnect(g_daos_poh, NULL);
    (void)daos_fini();
    g_daos_connected = false;
}

/* Destroy the private container (force: a leaked handle must not keep
 * it alive).  0 or -DER_*. */
static int conf_daos_cont_destroy(void)
{
    return daos_cont_destroy(g_daos_poh, g_daos_cont, 1, NULL);
}

static void conf_daos_fill_cfg(struct mds_config *cfg, uint32_t self_id)
{
    const char *sys = getenv(CONFORMANCE_DAOS_SYS_ENV);

    cfg->catalogue_backend = MDS_BACKEND_DAOS;
    (void)snprintf(cfg->daos_pool, sizeof(cfg->daos_pool), "%s", g_daos_pool);
    (void)snprintf(cfg->daos_cont, sizeof(cfg->daos_cont), "%s", g_daos_cont);
    if (sys != NULL && sys[0] != '\0') {
        (void)snprintf(cfg->daos_sys, sizeof(cfg->daos_sys), "%s", sys);
    }
    cfg->self.id = self_id;
    cfg->cluster_size = 1;
}

/* Once per process, after the last test handle is closed: destroy the
 * private container, drop the harness connection, then finalise the
 * backend's library state. */
static void conf_daos_teardown_once(void)
{
    int rc;

    pthread_mutex_lock(&g_daos_lock);
    if (!g_daos_prepared || g_daos_torn_down) {
        pthread_mutex_unlock(&g_daos_lock);
        return;
    }
    g_daos_torn_down = true;
    pthread_mutex_unlock(&g_daos_lock);

    if (g_daos_cont_private && g_daos_connected) {
        rc = conf_daos_cont_destroy();
        if (rc == 0) {
            (void)printf("conformance: daos container '%s' destroyed\n", g_daos_cont);
        } else {
            (void)fprintf(stderr, "conformance: daos container '%s' not destroyed: %d %s\n",
                          g_daos_cont, rc, daos_errstr(rc));
        }
        conf_daos_disconnect();
    }
    mds_catalogue_process_shutdown();
}

/* Safety net for a main that exits without conformance_shutdown(). */
static void conf_daos_atexit(void)
{
    conf_daos_teardown_once();
}

/* Bootstrap the container through a throwaway handle. */
static enum mds_status conf_daos_bootstrap(void)
{
    struct mds_config *cfg = calloc(1, sizeof(*cfg));
    struct mds_catalogue *cat = NULL;
    enum mds_status st;

    if (cfg == NULL) {
        return MDS_ERR_NOMEM;
    }
    conf_daos_fill_cfg(cfg, 1);
    st = mds_catalogue_open(cfg, &cat);
    free(cfg);
    if (st != MDS_OK) {
        return st;
    }
    st = mds_catalogue_bootstrap(cat);
    mds_catalogue_close(cat);
    return st;
}

/* Once per process: read the environment, create the private
 * container (when no CATALOGUE_TEST_CONT), bootstrap it.  A failure
 * before the container is usable is reported as a status the caller
 * turns into a skip; nothing of this process is left on the pool. */
static enum mds_status conf_daos_prepare_once(void)
{
    enum mds_status st = MDS_OK;
    int rc;

    pthread_mutex_lock(&g_daos_lock);
    if (g_daos_prepared) {
        pthread_mutex_unlock(&g_daos_lock);
        return MDS_OK;
    }
    if (!conf_daos_env()) {
        pthread_mutex_unlock(&g_daos_lock);
        return MDS_ERR_INVAL;
    }
    if (g_daos_cont_private) {
        rc = conf_daos_connect();
        if (rc == 0) {
            rc = daos_cont_create_with_label(g_daos_poh, g_daos_cont, NULL, NULL, NULL);
            if (rc != 0) {
                conf_daos_disconnect();
            }
        }
        if (rc != 0) {
            (void)fprintf(stdout, "SKIP: DAOS pool '%s' not usable: %d %s\n", g_daos_pool, rc,
                          daos_errstr(rc));
            pthread_mutex_unlock(&g_daos_lock);
            return MDS_ERR_IO;
        }
    }
    st = conf_daos_bootstrap();
    if (st == MDS_OK) {
        g_daos_prepared = true;
        (void)atexit(conf_daos_atexit);
    } else if (g_daos_cont_private) {
        (void)conf_daos_cont_destroy();
        conf_daos_disconnect();
    }
    pthread_mutex_unlock(&g_daos_lock);
    return st;
}
#endif /* HAVE_DAOS */

static enum mds_status open_daos(uint32_t self_id, struct mds_catalogue **out)
{
#ifdef HAVE_DAOS
    struct mds_config *cfg;
    const char *pool;
    enum mds_status st;

    if (!mds_catalogue_backend_available(MDS_BACKEND_DAOS)) {
        conformance_skip("daos backend not compiled in (ENABLE_DAOS=OFF)");
    }
    pool = getenv(CONFORMANCE_DAOS_POOL_ENV);
    if (pool == NULL || pool[0] == '\0') {
        conformance_skip(CONFORMANCE_DAOS_POOL_ENV " is not set");
    }
    st = conf_daos_prepare_once();
    if (st != MDS_OK) {
        (void)fprintf(stdout, "SKIP: DAOS container not usable (%d)\n", (int)st);
        conformance_skip("DAOS pool unreachable or container not usable");
    }
    cfg = calloc(1, sizeof(*cfg));
    if (cfg == NULL) {
        return MDS_ERR_NOMEM;
    }
    conf_daos_fill_cfg(cfg, self_id);
    st = mds_catalogue_open(cfg, out);
    free(cfg);
    return st;
#else
    (void)self_id;
    (void)out;
    conformance_skip("daos backend not compiled in (ENABLE_DAOS=OFF)");
#endif
}

enum mds_status conformance_open_as(uint32_t self_id, struct mds_catalogue **out)
{
    const char *name;

    if (out == NULL) {
        return MDS_ERR_INVAL;
    }
    *out = NULL;
    name = conformance_backend_name();

    if (strcmp(name, "memdb") == 0) {
        return open_memdb(out);
    }
    if (strcmp(name, "rondb") == 0) {
        return open_rondb(self_id, out);
    }
    if (strcmp(name, "fdb") == 0) {
        return open_fdb(self_id, out);
    }
    if (strcmp(name, "daos") == 0) {
        return open_daos(self_id, out);
    }

    (void)fprintf(stderr,
                  "conformance: unknown " CONFORMANCE_BACKEND_ENV
                  " '%s' (expected memdb|rondb|fdb|daos)\n", name);
    exit(1);
}

enum mds_status conformance_open(struct mds_catalogue **out)
{
    return conformance_open_as(1, out);
}

void conformance_shutdown(void)
{
#ifdef HAVE_FDB
    fdb_teardown_once();
#endif
#ifdef HAVE_DAOS
    conf_daos_teardown_once();
#endif
    /* No hooks are registered on memdb / rondb: a no-op there, and
     * idempotent after the fdb / daos teardown above. */
    mds_catalogue_process_shutdown();
}

struct mds_catalogue *conformance_open_checked(void)
{
    struct mds_catalogue *cat = NULL;
    enum mds_status st = conformance_open(&cat);

    if (st != MDS_OK || cat == NULL) {
        (void)fprintf(stderr, "conformance: open on backend %s failed: %d\n",
                      conformance_backend_name(), (int)st);
        exit(1);
    }
    return cat;
}

enum mds_status conformance_scratch_dir(struct mds_catalogue *cat,
                                        uint64_t *out_fid)
{
    struct mds_inode dir;
    char name[64];
    uint32_t seq;
    enum mds_status st;

    if (cat == NULL || out_fid == NULL) {
        return MDS_ERR_INVAL;
    }

    pthread_mutex_lock(&g_scratch_lock);
    seq = ++g_scratch_seq;
    pthread_mutex_unlock(&g_scratch_lock);

    (void)snprintf(name, sizeof(name), "conf-%ld-%u", (long)getpid(), seq);
    memset(&dir, 0, sizeof(dir));
    st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, name, MDS_FTYPE_DIR,
                           0755, 0, 0, NULL, &dir);
    if (st != MDS_OK) {
        return st;
    }

    pthread_mutex_lock(&g_scratch_lock);
    if (g_scratch_count < SCRATCH_MAX) {
        g_scratch[g_scratch_count].cat = cat;
        g_scratch[g_scratch_count].fid = dir.fileid;
        (void)snprintf(g_scratch[g_scratch_count].name,
                       sizeof(g_scratch[g_scratch_count].name), "%s", name);
        g_scratch_count++;
    }
    pthread_mutex_unlock(&g_scratch_lock);

    *out_fid = dir.fileid;
    return MDS_OK;
}

/* Bounded name collector for the cleanup readdir. */
#define CLEANUP_PAGE 512

struct cleanup_page {
    uint32_t count;
    uint8_t  type[CLEANUP_PAGE];
    uint64_t fid[CLEANUP_PAGE];
    char     name[CLEANUP_PAGE][MDS_MAX_NAME + 1];
};

static int cleanup_collect_cb(const struct mds_cat_dirent *entry, void *arg)
{
    struct cleanup_page *page = arg;

    if (page->count >= CLEANUP_PAGE) {
        return 1;
    }
    page->type[page->count] = entry->type;
    page->fid[page->count] = entry->fileid;
    (void)snprintf(page->name[page->count], sizeof(page->name[0]), "%s",
                   entry->name);
    page->count++;
    return 0;
}

/* Names of the leftovers the cleanup reports at most; the rest is a
 * count. */
#define CLEANUP_REPORT_NAMES 8

/*
 * Remove one entry of @dir_fid.  ns_remove refuses a raw dirent whose
 * inode is already gone (tests insert such aliases with
 * mds_cat_dirent_insert and then unlink the real name) on a backend
 * that resolves the inode inside the remove: RonDB answers NOTFOUND
 * from its lookup, a guarded remove STALE.  The dangling row itself is
 * then dropped with mds_cat_dirent_del, so the scratch directory can
 * be removed afterwards.  memdb and fdb remove such a dirent through
 * ns_remove already; the fallback never runs there.
 */
static enum mds_status cleanup_remove_entry(struct mds_catalogue *cat,
                                            uint64_t dir_fid,
                                            const char *name)
{
    enum mds_status st = mds_cat_ns_remove(cat, NULL, dir_fid, name);

    if (st == MDS_ERR_NOTFOUND || st == MDS_ERR_STALE) {
        st = mds_cat_dirent_del(cat, NULL, dir_fid, name);
    }
    return st;
}

/* Remove every entry of @dir_fid; entries that are directories have
 * their own (one level of) entries removed first.  Teardown is best
 * effort: an entry that stays is reported, never fatal. */
static void cleanup_dir_entries(struct mds_catalogue *cat, uint64_t dir_fid,
                                int depth)
{
    struct cleanup_page *page = calloc(1, sizeof(*page));
    uint32_t i;

    if (page == NULL) {
        return;
    }
    if (mds_cat_ns_readdir(cat, dir_fid, NULL, CLEANUP_PAGE, NULL,
                           cleanup_collect_cb, page) != MDS_OK) {
        free(page);
        return;
    }
    for (i = 0; i < page->count; i++) {
        enum mds_status st;

        if (page->type[i] == (uint8_t)MDS_FTYPE_DIR && depth > 0) {
            cleanup_dir_entries(cat, page->fid[i], depth - 1);
        }
        st = cleanup_remove_entry(cat, dir_fid, page->name[i]);
        if (st != MDS_OK) {
            (void)fprintf(stderr, "conformance: cleanup left '%s' (fileid %llu) "
                          "in directory %llu: status %d\n", page->name[i],
                          (unsigned long long)page->fid[i],
                          (unsigned long long)dir_fid, (int)st);
        }
    }
    free(page);
}

/* Report what a scratch directory that could not be removed still
 * holds: the first CLEANUP_REPORT_NAMES names and the total. */
static void cleanup_report_leftovers(struct mds_catalogue *cat,
                                     uint64_t dir_fid)
{
    struct cleanup_page *page = calloc(1, sizeof(*page));
    uint32_t i;

    if (page == NULL) {
        return;
    }
    if (mds_cat_ns_readdir(cat, dir_fid, NULL, CLEANUP_PAGE, NULL,
                           cleanup_collect_cb, page) != MDS_OK) {
        free(page);
        return;
    }
    (void)fprintf(stderr, "conformance: %u%s entr%s left in scratch directory "
                  "%llu:", page->count, page->count >= CLEANUP_PAGE ? "+" : "",
                  page->count == 1 ? "y" : "ies", (unsigned long long)dir_fid);
    for (i = 0; i < page->count && i < CLEANUP_REPORT_NAMES; i++) {
        (void)fprintf(stderr, " '%s'", page->name[i]);
    }
    (void)fprintf(stderr, "%s\n", page->count > CLEANUP_REPORT_NAMES ? " ..." : "");
    free(page);
}

void conformance_scratch_cleanup(struct mds_catalogue *cat,
                                 uint64_t dir_fid)
{
    char name[64];
    uint32_t i;
    bool found = false;
    enum mds_status st;

    if (cat == NULL) {
        return;
    }
    name[0] = '\0';
    pthread_mutex_lock(&g_scratch_lock);
    for (i = 0; i < g_scratch_count; i++) {
        if (g_scratch[i].cat == cat && g_scratch[i].fid == dir_fid) {
            (void)snprintf(name, sizeof(name), "%s", g_scratch[i].name);
            found = true;
            /* Release the slot: the table bounds LIVE scratch dirs. */
            g_scratch[i] = g_scratch[g_scratch_count - 1];
            g_scratch_count--;
            break;
        }
    }
    pthread_mutex_unlock(&g_scratch_lock);

    if (!found &&
        mds_cat_ns_dirent_name_for_child(cat, MDS_FILEID_ROOT, dir_fid, name,
                                         sizeof(name)) == MDS_OK) {
        found = true;
    }

    cleanup_dir_entries(cat, dir_fid, 1);
    if (!found) {
        struct mds_inode dir;

        /* Silent when the directory itself is already gone (a test
         * that removed it, or a second cleanup of the same fileid). */
        if (mds_cat_ns_getattr(cat, dir_fid, &dir) == MDS_OK) {
            (void)fprintf(stderr, "conformance: scratch directory %llu has no "
                          "name under the root; not removed\n",
                          (unsigned long long)dir_fid);
        }
        return;
    }
    st = cleanup_remove_entry(cat, MDS_FILEID_ROOT, name);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "conformance: scratch directory '%s' (fileid %llu) "
                      "not removed: status %d\n", name,
                      (unsigned long long)dir_fid, (int)st);
        cleanup_report_leftovers(cat, dir_fid);
    }
}

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_fdb.c -- FoundationDB catalogue backend: lifecycle.
 *
 * Owns the process-wide client network (fdb_setup_network /
 * fdb_run_network are once-per-process and fdb_stop_network is
 * terminal), the per-handle database connection, the schema stamp and
 * the root bootstrap, and assembles the vtables from the slot units'
 * registration hooks.  Every store interaction is one fdb_run_txn
 * (fdb_txn.h).
 *
 * Open:      network start (first open) -> fdb_create_database ->
 *            sweep dead incarnations' witness rows (one mutating txn:
 *            snapshot-read the NODE_REGISTRY row of mds_id, clear
 *            WITNESS + mds_id + [0, min(row.witness_epoch, own epoch));
 *            nothing when the row carries no epoch) ->
 *            read the schema stamp (one read-only txn).
 * Probe:     one read-only txn: stamp present and equal to
 *            FDB_CAT_SCHEMA_VERSION.
 * Bootstrap: one mutating txn: read the stamp (conflicting); absent ->
 *            write stamp, counters and the root inode; a concurrent
 *            bootstrap conflicts, retries and finds the stamp.
 * Close:     release the database handle and the backend state; the
 *            dispatcher frees struct mds_catalogue (C7).
 */

#include <assert.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "catalogue_fdb.h"
#include "catalogue_internal.h"
#include "fdb_codec.h"
#include "fdb_keys.h"
#include "fdb_txn.h"
#include "mds_log.h"

#define FDB_DEFAULT_CLUSTER_FILE "/etc/foundationdb/fdb.cluster"

/* Bootstrap value of the READDIR cookie allocator.  fdb_backend_alloc_id
 * hands out counter + 1 onwards, so the first cookie ever minted is 16,
 * clear of the cookie values NFSv4 reserves and a server must never
 * return (0 restarts a listing; 1 and 2 historically stood for "." and
 * ".."). */
#define FDB_COOKIE_SEQ_SEED 15U

/* -----------------------------------------------------------------------
 * Process-wide client network
 * ----------------------------------------------------------------------- */

enum net_state {
    NET_IDLE = 0,    /**< Never started. */
    NET_RUNNING = 1, /**< fdb_run_network thread alive. */
    NET_STOPPED = 2, /**< fdb_stop_network called: terminal. */
};

static pthread_mutex_t g_net_lock = PTHREAD_MUTEX_INITIALIZER;
static enum net_state  g_net_state = NET_IDLE;
static pthread_t       g_net_thread;
static uint32_t        g_open_handles;
static uint64_t        g_next_instance_seq = 1;

static void *net_thread_main(void *arg)
{
    fdb_error_t err = fdb_run_network();

    (void)arg;
    if (err != 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb_run_network: %d %s", (int)err, fdb_get_error(err));
    }
    return NULL;
}

static uint64_t realtime_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* Runs at process exit when the daemon or a test returned without the
 * explicit call: fdb_stop_network before exit is part of the client's
 * contract.  Idempotent, refuses while handles are open. */
static void net_atexit(void)
{
    catalogue_fdb_process_shutdown();
}

/*
 * First open of the process: select the API version, start the client
 * network thread and take the witness incarnation stamp.  Caller holds
 * g_net_lock; g_net_state is NET_IDLE.
 *
 * The stamp is CLOCK_REALTIME at this point.  Ordering across
 * incarnations of one mds_id (the open-time range clear of
 * WITNESS + mds_id below the registered incarnation's stamp, capped by
 * this one; witness_clear_body) relies on the wall clock not being
 * stepped back below the previous incarnation's stamp -- the same
 * caveat as the registry boot_epoch.  If it is stepped back, the older
 * incarnation's witness keys sort above the new stamp: they are never
 * read (every probe addresses slot keys carrying the current stamp)
 * and never cause a false landing, the range clear simply misses them
 * until a later restart with a higher stamp sweeps them.
 */
static enum mds_status net_start_locked(void)
{
    fdb_error_t err = fdb_select_api_version(FDB_API_VERSION);

    if (err != 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb_select_api_version(%d): %d %s", FDB_API_VERSION,
                      (int)err, fdb_get_error(err));
        return MDS_ERR_IO;
    }
    err = fdb_setup_network();
    if (err != 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb_setup_network: %d %s", (int)err, fdb_get_error(err));
        return MDS_ERR_IO;
    }
    if (pthread_create(&g_net_thread, NULL, net_thread_main, NULL) != 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb: cannot start the client network thread");
        return MDS_ERR_IO;
    }
    fdb_txn_set_witness_epoch(realtime_ns());
    g_net_state = NET_RUNNING;
    (void)mds_catalogue_register_process_shutdown(catalogue_fdb_process_shutdown);
    (void)atexit(net_atexit);
    return MDS_OK;
}

/* Start the network (first open) and count a handle.  Caller does not
 * hold g_net_lock. */
static enum mds_status net_acquire(void)
{
    enum mds_status st = MDS_OK;

    (void)pthread_mutex_lock(&g_net_lock);
    if (g_net_state == NET_STOPPED) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb: client network already stopped in this process; "
                      "open refused (fdb_stop_network is terminal)");
        st = MDS_ERR_INVAL;
    } else if (g_net_state == NET_IDLE) {
        st = net_start_locked();
    }
    if (st == MDS_OK) {
        g_open_handles++;
    }
    (void)pthread_mutex_unlock(&g_net_lock);
    return st;
}

static void net_release(void)
{
    (void)pthread_mutex_lock(&g_net_lock);
    if (g_open_handles > 0) {
        g_open_handles--;
    }
    (void)pthread_mutex_unlock(&g_net_lock);
}

void catalogue_fdb_process_shutdown(void)
{
    bool stop = false;

    (void)pthread_mutex_lock(&g_net_lock);
    if (g_net_state == NET_RUNNING) {
        if (g_open_handles > 0) {
            MDS_LOG_ERROR(LOG_COMP_CAT, "fdb: process shutdown requested with %u catalogue "
                          "handle(s) still open; client network left running",
                          (unsigned)g_open_handles);
        } else {
            g_net_state = NET_STOPPED;
            stop = true;
        }
    }
    (void)pthread_mutex_unlock(&g_net_lock);
    if (!stop) {
        return;
    }
    {
        fdb_error_t err = fdb_stop_network();

        if (err != 0) {
            MDS_LOG_ERROR(LOG_COMP_CAT, "fdb_stop_network: %d %s", (int)err,
                          fdb_get_error(err));
        }
    }
    (void)pthread_join(g_net_thread, NULL);
}

/* -----------------------------------------------------------------------
 * Handle helpers
 * ----------------------------------------------------------------------- */

static struct fdb_backend *fdb_of(const struct mds_catalogue *cat)
{
    if (cat == NULL || cat->backend != MDS_BACKEND_FDB) {
        return NULL;
    }
    return cat->backend_private;
}

/* -----------------------------------------------------------------------
 * Schema stamp: probe / check / bootstrap bodies
 * ----------------------------------------------------------------------- */

struct schema_ctx {
    struct fdb_backend *b;
    bool     present;
    uint64_t version;
};

/* Read-only: report presence and value of the stamp. */
static int schema_read_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct schema_ctx *sc = arg;
    struct fdb_key k;
    uint8_t buf[8];
    size_t len = 0;
    bool found = false;
    fdb_error_t err;

    fdb_key_meta(&k, &sc->b->prefix, FDB_META_SCHEMA_VERSION);
    err = fdb_txn_get(tr, &k, false, buf, sizeof(buf), &len, &found);
    if (err != 0) {
        return (int)err;
    }
    sc->present = found;
    sc->version = 0;
    if (found && !fdb_le64_decode(buf, len, &sc->version)) {
        *st_out = MDS_ERR_IO; /* corrupt stamp */
        return FDB_BODY_DONE;
    }
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* Bootstrap: seed the stamp, the counters and the root inode when the
 * stamp is absent; validate it otherwise.  One transaction; the stamp
 * read is conflicting so two concurrent bootstraps never both seed. */
static int bootstrap_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct schema_ctx *sc = arg;
    struct fdb_key k;
    struct mds_inode root;
    struct timespec now;
    int rc;

    rc = schema_read_body(tr, arg, st_out);
    if (rc != FDB_BODY_COMMIT) {
        return rc;
    }
    if (sc->present) {
        *st_out = (sc->version == FDB_CAT_SCHEMA_VERSION) ? MDS_OK : MDS_ERR_INVAL;
        return FDB_BODY_DONE;
    }
    if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
        *st_out = MDS_ERR_IO;
        return FDB_BODY_DONE;
    }
    memset(&root, 0, sizeof(root));
    root.fileid = MDS_FILEID_ROOT;
    root.type = MDS_FTYPE_DIR;
    root.mode = 0755;
    root.nlink = 2;
    root.atime = now;
    root.mtime = now;
    root.ctime = now;
    root.change = 1;
    root.generation = 1;
    rc = catalogue_fdb_inode_write(tr, &sc->b->prefix, &root);
    if (rc != 0) {
        return rc;
    }
    /* Allocators hand out counter + 1 .. : fileids start after the
     * root, READDIR cookies at FDB_COOKIE_SEQ_SEED + 1, GC and remove
     * sequences at 1. */
    fdb_key_meta(&k, &sc->b->prefix, FDB_META_FILEID);
    fdb_txn_set_le64(tr, &k, MDS_FILEID_ROOT);
    fdb_key_meta(&k, &sc->b->prefix, FDB_META_COOKIE_SEQ);
    fdb_txn_set_le64(tr, &k, FDB_COOKIE_SEQ_SEED);
    fdb_key_meta(&k, &sc->b->prefix, FDB_META_GC_SEQ);
    fdb_txn_set_le64(tr, &k, 0);
    fdb_key_meta(&k, &sc->b->prefix, FDB_META_REMOVE_SEQ);
    fdb_txn_set_le64(tr, &k, 0);
    fdb_key_meta(&k, &sc->b->prefix, FDB_META_SCHEMA_VERSION);
    fdb_txn_set_le64(tr, &k, FDB_CAT_SCHEMA_VERSION);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/*
 * Upper bound of the open-time witness sweep: the witness epoch W the
 * NODE_REGISTRY row of this mds_id carries, capped by this process's
 * own epoch.  *bound is 0 when nothing may be swept.
 *
 * The read is a SNAPSHOT read (no conflict range): the epoch in the row
 * is immutable for an incarnation (node_register writes it, heartbeats
 * rewrite the decoded row), so a heartbeat landing concurrently must
 * not abort the open.  The value is decoded strictly; a row this build
 * cannot decode fails the open rather than being guessed at.
 */
static fdb_error_t witness_sweep_bound(FDBTransaction *tr, const struct fdb_backend *b,
                                       uint64_t *bound)
{
    struct fdb_node_val row;
    struct fdb_key k;
    uint8_t buf[FDB_NODE_ENC_MAX];
    size_t len = 0;
    bool found = false;
    fdb_error_t err;

    *bound = b->witness_epoch;
    fdb_key_init(&k, &b->prefix, FDB_KT_NODE_REGISTRY);
    fdb_key_be32(&k, b->mds_id);
    err = fdb_txn_get(tr, &k, true, buf, sizeof(buf), &len, &found);
    if (err != 0) {
        return err;
    }
    if (!found) {
        return 0;
    }
    if (!fdb_node_decode(buf, len, &row)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    if (row.witness_epoch < *bound) {
        *bound = row.witness_epoch;
    }
    return 0;
}

/*
 * Sweep the witness rows of dead incarnations of this MDS id: clear
 * WITNESS + mds_id + [0, bound) with bound = min(W, own epoch), W being
 * the witness epoch the registry row of mds_id carries.
 *
 * Why the bound is exact.  The registered incarnation's rows sit at
 * exactly W, outside the half-open range, and survive until the id's
 * next restart, when the row is either gone (clean deregister) or
 * carries a newer W.  Every row below W belongs to an incarnation that
 * was superseded before the registered one registered (node_register
 * replaces a row only with a higher boot_epoch), i.e. one that has
 * exited or is fencing itself on its STALE heartbeat (main.c); the one
 * exposure left is a superseded process still inside that heartbeat
 * interval when a THIRD incarnation of the id opens.  A row without an
 * epoch (0: written by a process whose stamp is unknown) sweeps
 * nothing.  No row sweeps [0, own epoch): nothing under this id is
 * registered, and a process that has opened but not yet registered
 * runs only the idempotent bootstrap before it registers, so an
 * unresolved outcome it re-runs applies nothing twice.
 *
 * An unconditional clear of [0, own epoch) is wrong: a daemon taking
 * over a still-running mds_id erased the live process's rows during
 * the window between this open and that process's self-fence, its
 * in-flight commit_unknown_result probes then read "absent", re-ran
 * landed bodies and applied ADD counters twice.
 */
static int witness_clear_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct fdb_backend *b = arg;
    struct fdb_key_range r;
    uint64_t bound = 0;
    fdb_error_t err;

    err = witness_sweep_bound(tr, b, &bound);
    if (err != 0) {
        return (int)err;
    }
    *st_out = MDS_OK;
    if (bound == 0) {
        return FDB_BODY_DONE;
    }
    fdb_key_witness_mds_prefix(&r.begin, &b->prefix, b->mds_id);
    fdb_key_be64(&r.begin, 0);
    fdb_key_witness_mds_prefix(&r.end, &b->prefix, b->mds_id);
    fdb_key_be64(&r.end, bound);
    if (!fdb_key_ok(&r.begin) || !fdb_key_ok(&r.end)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    fdb_txn_clear_range(tr, &r);
    return FDB_BODY_COMMIT;
}

/* Clear every catalogue row under the prefix EXCEPT the WITNESS table:
 * [prefix, prefix + WITNESS) and [prefix + WITNESS + 1, strinc(prefix)).
 * The live incarnation's witness and fence-anchor rows must survive any
 * clear issued through the runner: the read-your-writes layer drops a
 * READ conflict range that lies inside a range the same transaction
 * clears, so a clear covering the anchor could not be fenced and a
 * late landing of it would wipe whatever was written after it -- and
 * clearing other threads' live witness rows blinds their in-flight
 * resolution (fdb_txn.h, body rule).  Dead incarnations' witness rows
 * are swept by a later open of their mds_id (witness_clear_body). */
static int keyspace_clear_body(FDBTransaction *tr, void *arg, enum mds_status *st_out)
{
    struct fdb_backend *b = arg;
    struct fdb_key_range whole;
    struct fdb_key_range r;

    whole.begin.len = b->prefix.len;
    whole.begin.overflow = false;
    memcpy(whole.begin.buf, b->prefix.bytes, b->prefix.len);
    if (!fdb_key_range_prefix(&whole, &whole.begin)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    /* Below the WITNESS table. */
    r.begin = whole.begin;
    fdb_key_init(&r.end, &b->prefix, FDB_KT_WITNESS);
    fdb_txn_clear_range(tr, &r);
    /* Above it: from the first key past the WITNESS table to the end of
     * the prefix. */
    fdb_key_init(&r.begin, &b->prefix, FDB_KT_WITNESS);
    if (!fdb_key_strinc(&r.begin)) {
        return FDB_ERR_PLATFORM_ERROR;
    }
    r.end = whole.end;
    fdb_txn_clear_range(tr, &r);
    *st_out = MDS_OK;
    return FDB_BODY_COMMIT;
}

/* -----------------------------------------------------------------------
 * Lifecycle slots
 * ----------------------------------------------------------------------- */

static void fdb_close(struct mds_catalogue *cat)
{
    struct fdb_backend *b = fdb_of(cat);

    if (b == NULL) {
        return;
    }
    cat->backend_private = NULL;
    if (b->db != NULL) {
        fdb_database_destroy(b->db);
    }
    free(b);
    net_release();
}

static enum mds_status fdb_probe(struct mds_catalogue *cat)
{
    struct fdb_backend *b = fdb_of(cat);
    struct schema_ctx sc;
    enum mds_status st;

    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&sc, 0, sizeof(sc));
    sc.b = b;
    st = fdb_run_txn(b, FDB_TXN_READONLY, "probe", schema_read_body, &sc);
    if (st != MDS_OK) {
        return st;
    }
    if (!sc.present) {
        return MDS_ERR_IO; /* not bootstrapped */
    }
    return (sc.version == FDB_CAT_SCHEMA_VERSION) ? MDS_OK : MDS_ERR_INVAL;
}

static enum mds_status fdb_bootstrap(struct mds_catalogue *cat)
{
    struct fdb_backend *b = fdb_of(cat);
    struct schema_ctx sc;
    enum mds_status st;

    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&sc, 0, sizeof(sc));
    sc.b = b;
    st = fdb_run_txn(b, FDB_TXN_MUTATING, "bootstrap", bootstrap_body, &sc);
    if (st == MDS_ERR_INVAL) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb: keyspace schema %llu, this build supports %u",
                      (unsigned long long)sc.version, FDB_CAT_SCHEMA_VERSION);
    }
    return st;
}

static const struct mds_catalogue_ops fdb_lifecycle_ops = {
    .close            = fdb_close,
    .probe            = fdb_probe,
    .bootstrap        = fdb_bootstrap,
    .backend_handle   = NULL,
    .image_feed_start = NULL,
    .image_feed_stop  = NULL,
};

/* -----------------------------------------------------------------------
 * Client-side counters (mds_cat_backend_client_stats)
 *
 * The runner's counters (fdb_txn.h) in the backend-neutral shape the
 * RonDB shim fills from the NDB API, so a workload phase can be costed
 * the same way on both backends (sample before and after, divide the
 * delta by the operation count):
 *   exec_waits     network waits -- blocking points on a future that had
 *                  not arrived (fdb_txn_net_waits: a parallel read wave
 *                  counts once, the GRV is folded into the first read,
 *                  a commit is one wait); process-wide, like the client
 *                  network thread it measures
 *   txn_started    body executions (attempts), read-only ones included
 *   txn_committed  commits reported committed (witness-resolved landings
 *                  included)
 *   txn_aborted    definitive aborts that were re-run (retries)
 *   txn_closed     attempts that did not commit (read-only bodies,
 *                  FDB_BODY_DONE outcomes, aborts)
 *   client_objects 1 (one database handle)
 * The remaining fields have no FoundationDB counterpart and stay 0.
 * ----------------------------------------------------------------------- */

static uint64_t stat_read(const _Atomic uint64_t *ctr)
{
    return atomic_load_explicit(ctr, memory_order_relaxed);
}

static enum mds_status fdb_backend_client_stats(struct mds_catalogue *cat,
                                                struct mds_cat_backend_client_stats *out)
{
    struct fdb_backend *b = fdb_of(cat);
    uint64_t attempts;
    uint64_t commits;

    if (b == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }
    attempts = stat_read(&b->stats.attempts);
    commits = stat_read(&b->stats.commits);
    memset(out, 0, sizeof(*out));
    out->exec_waits = fdb_txn_net_waits();
    out->txn_started = attempts;
    out->txn_committed = commits;
    out->txn_aborted = stat_read(&b->stats.retries);
    out->txn_closed = (attempts > commits) ? attempts - commits : 0;
    out->client_objects = 1;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Vtables.  The authority table is assembled once per process from the
 * slot units' registration hooks (under g_net_lock, before any handle
 * is published) and then only ever read.  The coordination and cluster
 * tables are constant objects exported by catalogue_fdb_coord.c and
 * catalogue_fdb_cluster.c (fdb_coordination_ops, fdb_cluster_ops) and
 * are installed by catalogue_fdb_open().
 * ----------------------------------------------------------------------- */

static struct mds_authority_ops g_auth_ops;
static bool                     g_tables_ready;

static void tables_assemble(void)
{
    (void)pthread_mutex_lock(&g_net_lock);
    if (!g_tables_ready) {
        memset(&g_auth_ops, 0, sizeof(g_auth_ops));
        catalogue_fdb_ns_register(&g_auth_ops);
        catalogue_fdb_ext_register(&g_auth_ops);
        g_auth_ops.backend_client_stats = fdb_backend_client_stats;
        g_tables_ready = true;
    }
    (void)pthread_mutex_unlock(&g_net_lock);
}

/* -----------------------------------------------------------------------
 * Constructor
 * ----------------------------------------------------------------------- */

static const char *cluster_file_of(const struct mds_config *cfg)
{
    const char *env;

    if (cfg->fdb_cluster_file[0] != '\0') {
        return cfg->fdb_cluster_file;
    }
    /* NOLINTNEXTLINE(concurrency-mt-unsafe) */
    env = getenv("FDB_CLUSTER_FILE");
    if (env != NULL && env[0] != '\0') {
        return env;
    }
    return FDB_DEFAULT_CLUSTER_FILE;
}

static enum mds_status backend_new(const struct mds_config *cfg, struct fdb_backend **out)
{
    struct fdb_backend *b;
    size_t plen = strnlen(cfg->fdb_key_prefix, sizeof(cfg->fdb_key_prefix));

    *out = NULL;
    if (plen > FDB_KEY_PREFIX_MAX) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb_key_prefix longer than %u bytes", FDB_KEY_PREFIX_MAX);
        return MDS_ERR_INVAL;
    }
    b = calloc(1, sizeof(*b));
    if (b == NULL) {
        return MDS_ERR_NOMEM;
    }
    memcpy(b->prefix.bytes, cfg->fdb_key_prefix, plen);
    b->prefix.len = (uint32_t)plen;
    b->mds_id = cfg->self.id;
    b->op_deadline_ms = cfg->fdb_op_deadline_ms != 0 ? cfg->fdb_op_deadline_ms :
                                                       FDB_OP_DEADLINE_MS_DEFAULT;
    b->txn_timeout_ms = cfg->fdb_txn_timeout_ms != 0 ? cfg->fdb_txn_timeout_ms :
                                                       FDB_TXN_TIMEOUT_MS_DEFAULT;
    b->calls = NULL;
    *out = b;
    return MDS_OK;
}

/* Open the database handle, stamp the backend and clear dead
 * incarnations' witness keys (bounded: the registered incarnation's
 * slots and this one's are all that linger between two opens).  The
 * sweep doubles as the reachability check: an unreachable cluster
 * fails the open instead of the first catalogue call. */
static enum mds_status open_connect(struct fdb_backend *b, const char *cluster_file)
{
    enum mds_status st;
    fdb_error_t err = fdb_create_database(cluster_file, &b->db);

    if (err != 0 || b->db == NULL) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb_create_database(%s): %d %s", cluster_file, (int)err,
                      fdb_get_error(err));
        return MDS_ERR_IO;
    }
    (void)pthread_mutex_lock(&g_net_lock);
    b->instance_seq = g_next_instance_seq++;
    (void)pthread_mutex_unlock(&g_net_lock);
    b->witness_epoch = fdb_txn_witness_epoch();

    st = fdb_run_txn(b, FDB_TXN_MUTATING, "witness_clear", witness_clear_body, b);
    if (st != MDS_OK) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb: cluster %s not usable at open (%d)", cluster_file,
                      (int)st);
        return MDS_ERR_IO;
    }
    return MDS_OK;
}

/* Refuse a keyspace written by another schema version; an absent stamp
 * is fine (probe fails until bootstrap runs). */
static enum mds_status open_check_schema(struct fdb_backend *b)
{
    struct schema_ctx sc;
    enum mds_status st;

    memset(&sc, 0, sizeof(sc));
    sc.b = b;
    st = fdb_run_txn(b, FDB_TXN_READONLY, "open_schema", schema_read_body, &sc);
    if (st != MDS_OK) {
        return st;
    }
    if (sc.present && sc.version != FDB_CAT_SCHEMA_VERSION) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "fdb: keyspace schema %llu, this build supports %u; "
                      "open refused", (unsigned long long)sc.version, FDB_CAT_SCHEMA_VERSION);
        return MDS_ERR_INVAL;
    }
    if (!sc.present) {
        MDS_LOG_INFO(LOG_COMP_CAT, "fdb: keyspace not bootstrapped; probe fails until "
                     "bootstrap runs");
    }
    return MDS_OK;
}

enum mds_status catalogue_fdb_open(const struct mds_config *cfg, struct mds_catalogue **out)
{
    struct mds_catalogue *cat = NULL;
    struct fdb_backend *b = NULL;
    enum mds_status st;

    if (cfg == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }
    *out = NULL;

    st = backend_new(cfg, &b);
    if (st != MDS_OK) {
        return st;
    }
    st = net_acquire();
    if (st != MDS_OK) {
        free(b);
        return st;
    }
    tables_assemble();

    st = open_connect(b, cluster_file_of(cfg));
    if (st != MDS_OK) {
        goto fail;
    }
    st = open_check_schema(b);
    if (st != MDS_OK) {
        goto fail;
    }

    cat = calloc(1, sizeof(*cat));
    if (cat == NULL) {
        st = MDS_ERR_NOMEM;
        goto fail;
    }
    cat->backend = MDS_BACKEND_FDB;
    /* One cluster shared by every MDS process: an inode is visible from
     * all of them and registry rows written by one daemon are observed
     * by another. */
    cat->caps = MDS_CAT_CAP_SHARED_AUTHORITY | MDS_CAT_CAP_MULTI_PROCESS;
    cat->ops = &fdb_lifecycle_ops;
    cat->auth_ops = &g_auth_ops;
    cat->coord_ops = &fdb_coordination_ops; /* catalogue_fdb_coord.c */
    cat->cluster_ops = &fdb_cluster_ops;    /* catalogue_fdb_cluster.c */
    cat->backend_private = b;
    *out = cat;
    return MDS_OK;

fail:
    if (b->db != NULL) {
        fdb_database_destroy(b->db);
    }
    free(b);
    net_release();
    return st;
}

enum mds_status fdb_backend_set_boot_epoch(const struct mds_catalogue *cat,
                                           uint64_t boot_epoch)
{
    struct fdb_backend *b = fdb_of(cat);

    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    atomic_store(&b->boot_epoch, boot_epoch);
    return MDS_OK;
}

enum mds_status catalogue_fdb_keyspace_clear(const struct mds_catalogue *cat)
{
    struct fdb_backend *b = fdb_of(cat);

    if (b == NULL || b->prefix.len == 0) {
        return MDS_ERR_INVAL;
    }
    return fdb_run_txn(b, FDB_TXN_MUTATING, "keyspace_clear", keyspace_clear_body, b);
}

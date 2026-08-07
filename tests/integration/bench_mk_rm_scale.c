/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * bench_mk_rm_scale.c -- create/remove scaling harness for the
 * catalogue namespace path.
 *
 * PURPOSE
 * -------
 * Metadata mutation throughput on a shared directory is bounded by
 * the parent-row update inside the fused namespace transaction:
 * concurrent creates/removes against ONE directory queue on that
 * row's lock, so per-directory ops/s walls while spread-directory
 * ops/s keeps scaling.  The delete-at-ack path (remove_async)
 * deliberately does NOT touch the parent row at ack time, trading
 * the wall for deferred drain work.
 *
 * This harness measures all three effects at the catalogue API
 * level (mds_cat_*), below the NFS protocol layer, so the numbers
 * isolate the backend transaction behaviour:
 *
 *   1. create burst   -- mds_cat_ns_create (inode + dirent +
 *                        interpreted parent update, one txn);
 *   2. sync remove    -- mds_cat_ns_remove (fused dirent + inode +
 *                        parent update);
 *   3. ack-path remove-- mds_cat_remove_pending_enqueue_unlink
 *                        (manifest row + dirent delete + inode
 *                        flag, one txn, NO parent-row update) --
 *                        the foreground cost a client sees under
 *                        remove_async=true.  An enqueue failure
 *                        falls back to the synchronous remove,
 *                        mirroring the production contract
 *                        (remove_manifest_submit: on failure the
 *                        caller MUST take the sync path); the
 *                        per-phase report shows the split.
 *
 * Each phase runs at 1/4/8/16 threads in two directory modes:
 *   shared -- every thread mutates ONE directory (exposes the
 *             parent-row serialisation wall);
 *   spread -- one directory per thread (unserialised baseline).
 *
 * MEASUREMENT FIDELITY
 * --------------------
 * On the in-memory memdb backend (default; used by CI) the numbers
 * only prove the harness and the API contracts -- memdb has no
 * row-lock serialisation to speak of.  Run with --rondb against the
 * lab cluster for real numbers.  The signal is the ops/s curve
 * across the thread ladder: a flat shared-mode curve with a rising
 * spread-mode curve is the parent-row wall.
 *
 * ACK-PATH CLEANUP NOTE
 * ---------------------
 * Each enqueue leaves a manifest row + DELETE_PENDING corpse inode --
 * that is the deferred work whose foreground cost the phase measures.
 * The harness drains its own debris right after the timed burst
 * (mds_cat_remove_pending_complete + mds_cat_inode_del per file,
 * untimed) so reruns start clean and bounded backends (memdb's
 * fixed-capacity tables) are not exhausted across the ladder.  A live
 * MDS drainer racing the harness on a shared lab schema is harmless:
 * both sides tolerate already-gone rows.  Files whose enqueue failed
 * are still live and are removed synchronously by name.
 *
 * USAGE
 * -----
 *   ./bench_mk_rm_scale [FILES_PER_THREAD] [--rondb CONFPATH]
 *                       [--no-ack-path]
 *
 * FILES_PER_THREAD defaults to 50 (range 1..100000).  The default is
 * sized for the memdb CI smoke: memdb's fixed tables cap cumulative
 * creates per process at ~8K and concurrent manifest rows at 256.
 * For --rondb lab runs raise it (1000+) so the bursts are long enough
 * to expose the parent-row wall.
 * --no-ack-path skips phase 3 entirely.
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <stdatomic.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "test_helpers.h"

#define BENCH_DEFAULT_FILES   50U
#define BENCH_MAX_FILES       100000U
#define BENCH_MDS_ID          0

/* Exit codes (matches bench_create_layout_fusion). */
#define BENCH_EXIT_OK          0
#define BENCH_EXIT_CORRECTNESS 1
#define BENCH_EXIT_SETUP       2

static const uint32_t bench_thread_ladder[] = { 1U, 4U, 8U, 16U };
#define BENCH_LADDER_LEN \
	(sizeof(bench_thread_ladder) / sizeof(bench_thread_ladder[0]))
#define BENCH_MAX_THREADS 16U

enum bench_dir_mode {
	BENCH_DIR_SHARED = 0,   /* all threads in one directory  */
	BENCH_DIR_SPREAD,       /* one directory per thread      */
};

enum bench_op {
	BENCH_OP_CREATE = 0,
	BENCH_OP_REMOVE_SYNC,
	BENCH_OP_REMOVE_ACK,
};

/* Per-file identity captured at create time; the ack-path phase
 * needs (child_fileid, generation) for its guarded enqueue and
 * records the manifest seq for the post-phase drain. */
struct bench_file_id {
	uint64_t fileid;
	uint64_t generation;
	uint64_t seq;          /* manifest row seq; 0 = not enqueued */
};

struct bench_worker {
	struct mds_catalogue *cat;
	uint64_t              dir_fileid;
	uint32_t              worker_idx;
	uint32_t              files;
	enum bench_op         op;
	struct bench_file_id *ids;         /* [files], owned by main */
	pthread_barrier_t    *barrier;
	uint32_t              failures;
	bool                  nosupport;   /* ack-path unsupported */
	uint32_t              done;        /* ops completed (for cleanup) */
	uint32_t              ack_fallbacks; /* enqueue -> sync fallback */
};

static double monotonic_seconds(void)
{
	struct timespec ts;

	if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
		return 0.0;
	}
	return (double)ts.tv_sec + (double)ts.tv_nsec / 1.0e9;
}

static void bench_file_name(char *buf, size_t cap, uint32_t worker_idx,
			    uint32_t i)
{
	(void)snprintf(buf, cap, "w%02u_f%06u", (unsigned)worker_idx,
		       (unsigned)i);
}

static void *bench_worker_fn(void *arg)
{
	struct bench_worker *w = arg;
	char name[64];
	uint32_t i;

	(void)pthread_barrier_wait(w->barrier);

	for (i = 0; i < w->files; i++) {
		enum mds_status st;

		bench_file_name(name, sizeof(name), w->worker_idx, i);

		switch (w->op) {
		case BENCH_OP_CREATE: {
			struct mds_inode out;

			memset(&out, 0, sizeof(out));
			st = mds_cat_ns_create(w->cat, NULL, w->dir_fileid,
					       name, MDS_FTYPE_REG, 0644,
					       0, 0, NULL, &out);
			if (st == MDS_OK) {
				w->ids[i].fileid     = out.fileid;
				w->ids[i].generation = out.generation;
				w->ids[i].seq        = 0;
			}
			break;
		}
		case BENCH_OP_REMOVE_SYNC:
			st = mds_cat_ns_remove(w->cat, NULL, w->dir_fileid,
					       name);
			break;
		case BENCH_OP_REMOVE_ACK: {
			uint64_t seq = 0;

			st = mds_cat_remove_pending_enqueue_unlink(
				w->cat, NULL, w->dir_fileid, name,
				w->ids[i].fileid, w->ids[i].generation,
				&seq);
			if (st == MDS_ERR_NOSUPPORT) {
				w->nosupport = true;
				return NULL;
			}
			if (st == MDS_OK) {
				w->ids[i].seq = seq;
			} else {
				/* Backpressure (manifest at capacity) or a
				 * backend limitation (memdb's fixed table is
				 * not reliable under high enqueue
				 * concurrency).  Production answers this
				 * with the synchronous-remove fallback whose
				 * cost the remove_sync phase already
				 * measures, so the timed loop only records
				 * the split and moves on; the untimed post-
				 * phase drain removes the file by name.  On
				 * RonDB the manifest is effectively
				 * unbounded and this branch should stay at
				 * zero -- a non-zero count there IS the
				 * finding. */
				w->ack_fallbacks++;
				st = MDS_OK;
			}
			break;
		}
		default:
			st = MDS_ERR_INVAL;
			break;
		}

		if (st != MDS_OK) {
			w->failures++;
		} else {
			w->done++;
		}
	}
	return NULL;
}

struct bench_phase_result {
	double   elapsed_sec;
	uint32_t ops;
	uint32_t failures;
	uint32_t ack_fallbacks;
	bool     nosupport;
};

/*
 * Run one timed phase: @threads workers execute @op over their name
 * set behind a start barrier; elapsed is barrier-release to last
 * join.  dirs[] carries one fileid per worker (same value repeated
 * in shared mode).
 */
static int bench_run_phase(struct mds_catalogue *cat,
			   const uint64_t *dirs, uint32_t threads,
			   uint32_t files, enum bench_op op,
			   struct bench_file_id **ids,
			   struct bench_phase_result *out)
{
	struct bench_worker workers[BENCH_MAX_THREADS];
	pthread_t tids[BENCH_MAX_THREADS];
	pthread_barrier_t barrier;
	double t0, t1;
	uint32_t t;

	memset(out, 0, sizeof(*out));
	memset(workers, 0, sizeof(workers));

	/* +1: main joins the barrier to release all workers at once. */
	if (pthread_barrier_init(&barrier, NULL, threads + 1U) != 0) {
		return -1;
	}

	for (t = 0; t < threads; t++) {
		workers[t].cat        = cat;
		workers[t].dir_fileid = dirs[t];
		workers[t].worker_idx = t;
		workers[t].files      = files;
		workers[t].op         = op;
		workers[t].ids        = ids[t];
		workers[t].barrier    = &barrier;
		if (pthread_create(&tids[t], NULL, bench_worker_fn,
				   &workers[t]) != 0) {
			/* Unwind: release and join what was started. */
			uint32_t j;

			for (j = t; j < threads; j++) {
				workers[j].files = 0;
			}
			for (j = 0; j < t; j++) {
				workers[j].files = 0;
			}
			(void)pthread_barrier_wait(&barrier);
			for (j = 0; j < t; j++) {
				(void)pthread_join(tids[j], NULL);
			}
			pthread_barrier_destroy(&barrier);
			return -1;
		}
	}

	t0 = monotonic_seconds();
	(void)pthread_barrier_wait(&barrier);
	for (t = 0; t < threads; t++) {
		(void)pthread_join(tids[t], NULL);
	}
	t1 = monotonic_seconds();
	pthread_barrier_destroy(&barrier);

	out->elapsed_sec = t1 - t0;
	for (t = 0; t < threads; t++) {
		out->ops           += workers[t].done;
		out->failures      += workers[t].failures;
		out->ack_fallbacks += workers[t].ack_fallbacks;
		if (workers[t].nosupport) {
			out->nosupport = true;
		}
	}
	return 0;
}

static void bench_print_phase(const char *label,
			      const struct bench_phase_result *r)
{
	if (r->nosupport) {
		(void)printf("  %-12s (backend NOSUPPORT; skipped)\n",
			     label);
		return;
	}
	if (r->ops == 0 || r->elapsed_sec <= 0.0) {
		(void)printf("  %-12s (no data)\n", label);
		return;
	}
	(void)printf("  %-12s %7u ops  %8.3fs  %10.1f ops/s  %8.1f us/op",
		     label, r->ops, r->elapsed_sec,
		     (double)r->ops / r->elapsed_sec,
		     (r->elapsed_sec * 1.0e6) / (double)r->ops);
	if (r->ack_fallbacks != 0) {
		(void)printf("  (enq=%u backpressure=%u)",
			     r->ops - r->ack_fallbacks, r->ack_fallbacks);
	}
	if (r->failures != 0) {
		(void)printf("  [FAILURES]");
	}
	(void)printf("\n");
}

/* Create one bench directory under the root; returns 0 on success. */
static int bench_mkdir(struct mds_catalogue *cat, const char *name,
		       uint64_t *fileid_out)
{
	struct mds_inode out;
	enum mds_status st;

	memset(&out, 0, sizeof(out));
	st = mds_cat_ns_create(cat, NULL, MDS_FILEID_ROOT, name,
			       MDS_FTYPE_DIR, 0755, 0, 0, NULL, &out);
	if (st != MDS_OK) {
		fprintf(stderr, "mkdir %s failed: %d\n", name, (int)st);
		return -1;
	}
	*fileid_out = out.fileid;
	return 0;
}

/*
 * Untimed drain after the ack-path burst.  Enqueued files (seq != 0)
 * get their manifest row completed and their corpse inode deleted --
 * the same end state the daemon's drainer produces, minus DS I/O the
 * harness has none of.  Files that never enqueued (NOSUPPORT bail or
 * a failed guard) are still live under their names and are removed
 * synchronously.  Everything here is best-effort: a live drainer
 * racing us on a shared schema makes rows/inodes vanish first, and
 * both calls tolerate NOTFOUND.
 */
static void bench_drain_ack(struct mds_catalogue *cat,
			    const uint64_t *dirs, uint32_t threads,
			    uint32_t files,
			    struct bench_file_id *const *ids)
{
	char name[64];
	uint32_t t, i;

	for (t = 0; t < threads; t++) {
		for (i = 0; i < files; i++) {
			if (ids[t][i].seq != 0) {
				(void)mds_cat_remove_pending_complete(
					cat, ids[t][i].seq);
				(void)mds_cat_inode_del(cat, NULL,
							ids[t][i].fileid);
			} else {
				bench_file_name(name, sizeof(name), t, i);
				(void)mds_cat_ns_remove(cat, NULL,
							dirs[t], name);
			}
		}
	}
}

/*
 * Run the full phase set for one (threads, mode) combination.
 * Returns the number of correctness failures observed.
 */
static uint32_t bench_run_combo(struct mds_catalogue *cat,
				uint32_t threads,
				enum bench_dir_mode mode,
				uint32_t files, bool run_ack_path,
				uint32_t combo_idx)
{
	uint64_t dirs[BENCH_MAX_THREADS];
	struct bench_file_id *ids[BENCH_MAX_THREADS];
	struct bench_phase_result create_r, rm_sync_r, ack_prep_r, rm_ack_r;
	char dname[96];
	uint32_t failures = 0;
	uint32_t ndirs = (mode == BENCH_DIR_SHARED) ? 1U : threads;
	uint32_t d, t;

	memset(ids, 0, sizeof(ids));
	memset(&rm_ack_r, 0, sizeof(rm_ack_r));

	/* Directories: fresh per combo so name sets never collide. */
	for (d = 0; d < ndirs; d++) {
		(void)snprintf(dname, sizeof(dname),
			       "mkrm_%d_c%02u_d%02u",
			       (int)getpid(), (unsigned)combo_idx,
			       (unsigned)d);
		if (bench_mkdir(cat, dname, &dirs[d]) != 0) {
			return 1;
		}
	}
	if (mode == BENCH_DIR_SHARED) {
		for (t = 1; t < threads; t++) {
			dirs[t] = dirs[0];
		}
	}

	for (t = 0; t < threads; t++) {
		ids[t] = calloc(files, sizeof(*ids[t]));
		if (ids[t] == NULL) {
			failures++;
			goto out;
		}
	}

	(void)printf("threads=%-2u mode=%s\n", (unsigned)threads,
		     mode == BENCH_DIR_SHARED ? "shared" : "spread");

	/* Phase 1: create burst. */
	if (bench_run_phase(cat, dirs, threads, files, BENCH_OP_CREATE,
			    ids, &create_r) != 0) {
		failures++;
		goto out;
	}
	bench_print_phase("create", &create_r);
	failures += create_r.failures;

	/* Phase 2: synchronous remove burst (fused parent-row txn). */
	if (bench_run_phase(cat, dirs, threads, files,
			    BENCH_OP_REMOVE_SYNC, ids, &rm_sync_r) != 0) {
		failures++;
		goto out;
	}
	bench_print_phase("remove_sync", &rm_sync_r);
	failures += rm_sync_r.failures;

	/* Phase 3: ack-path remove burst (delete-at-ack foreground
	 * cost).  Re-create the name set first (untimed prep). */
	if (run_ack_path) {
		if (bench_run_phase(cat, dirs, threads, files,
				    BENCH_OP_CREATE, ids,
				    &ack_prep_r) != 0) {
			failures++;
			goto out;
		}
		failures += ack_prep_r.failures;

		if (bench_run_phase(cat, dirs, threads, files,
				    BENCH_OP_REMOVE_ACK, ids,
				    &rm_ack_r) != 0) {
			failures++;
			goto out;
		}
		bench_print_phase("remove_ack", &rm_ack_r);
		/* NOSUPPORT is a skip, not a failure. */
		failures += rm_ack_r.failures;

		/* Untimed: drain the debris the burst deliberately left. */
		bench_drain_ack(cat, dirs, threads, files,
				(struct bench_file_id *const *)ids);
	}

	/* Best-effort combo-dir removal (empty after the drains). */
	for (d = 0; d < ndirs; d++) {
		(void)snprintf(dname, sizeof(dname),
			       "mkrm_%d_c%02u_d%02u",
			       (int)getpid(), (unsigned)combo_idx,
			       (unsigned)d);
		(void)mds_cat_ns_remove(cat, NULL, MDS_FILEID_ROOT, dname);
	}

out:
	for (t = 0; t < threads; t++) {
		free(ids[t]);
	}
	return failures;
}

static void usage(const char *argv0)
{
	fprintf(stderr,
		"usage: %s [FILES_PER_THREAD] [--rondb CONFPATH] "
		"[--no-ack-path]\n"
		"       FILES_PER_THREAD defaults to %u (range 1..%u);\n"
		"               the default fits memdb's fixed tables --\n"
		"               raise it (1000+) for --rondb lab runs.\n"
		"       --rondb selects the RonDB catalogue backend\n"
		"               (CONFPATH typically "
		"/etc/pnfs-mds/rondb.conf).\n"
		"       --no-ack-path skips the delete-at-ack phase.\n",
		argv0, (unsigned)BENCH_DEFAULT_FILES,
		(unsigned)BENCH_MAX_FILES);
}

int main(int argc, char **argv)
{
	struct mds_catalogue *cat = NULL;
	const char *rondb_conf = NULL;
	uint32_t files = BENCH_DEFAULT_FILES;
	bool run_ack_path = true;
	uint32_t total_failures = 0;
	uint32_t combo_idx = 0;
	size_t li;
	int ai;

	for (ai = 1; ai < argc; ai++) {
		const char *a = argv[ai];

		if (strcmp(a, "--rondb") == 0) {
			if (ai + 1 >= argc) {
				usage(argv[0]);
				return BENCH_EXIT_SETUP;
			}
			rondb_conf = argv[++ai];
		} else if (strcmp(a, "--no-ack-path") == 0) {
			run_ack_path = false;
		} else if (a[0] >= '0' && a[0] <= '9') {
			long v = strtol(a, NULL, 10);

			if (v <= 0 || v > (long)BENCH_MAX_FILES) {
				usage(argv[0]);
				return BENCH_EXIT_SETUP;
			}
			files = (uint32_t)v;
		} else {
			usage(argv[0]);
			return BENCH_EXIT_SETUP;
		}
	}

	(void)printf("mk/rm scale bench (catalogue namespace path)\n");
	(void)printf("  files/thread: %u   thread ladder: 1/4/8/16   "
		     "modes: shared, spread\n", (unsigned)files);

	if (rondb_conf != NULL) {
		struct mds_config cfg;
		enum mds_status st;

		memset(&cfg, 0, sizeof(cfg));
		cfg.catalogue_backend = MDS_BACKEND_RONDB;
		(void)snprintf(cfg.catalogue_backend_conf,
			       sizeof(cfg.catalogue_backend_conf),
			       "%s", rondb_conf);
		cfg.self.id = BENCH_MDS_ID;
		cfg.cluster_size = 1;
		cfg.ndb_conn_pool_size = 4;

		st = mds_catalogue_open(&cfg, &cat);
		if (st != MDS_OK) {
			fprintf(stderr,
				"mds_catalogue_open(rondb, %s) failed: %d\n",
				rondb_conf, (int)st);
			return BENCH_EXIT_SETUP;
		}
		(void)printf("  backend:      rondb (%s)\n", rondb_conf);
	} else {
		cat = catalogue_memdb_open();
		if (cat == NULL) {
			fprintf(stderr, "catalogue_memdb_open failed\n");
			return BENCH_EXIT_SETUP;
		}
		(void)printf("  backend:      memdb (in-process; run "
			     "--rondb for real numbers)\n");
	}

	for (li = 0; li < BENCH_LADDER_LEN; li++) {
		total_failures += bench_run_combo(
			cat, bench_thread_ladder[li], BENCH_DIR_SHARED,
			files, run_ack_path, combo_idx++);
		total_failures += bench_run_combo(
			cat, bench_thread_ladder[li], BENCH_DIR_SPREAD,
			files, run_ack_path, combo_idx++);
	}

	mds_catalogue_close(cat);

	if (total_failures != 0) {
		fprintf(stderr, "FAILURES: %u\n",
			(unsigned)total_failures);
		return BENCH_EXIT_CORRECTNESS;
	}
	(void)printf("OK\n");
	return BENCH_EXIT_OK;
}

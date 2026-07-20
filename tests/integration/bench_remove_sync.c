/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * bench_remove_sync.c -- Manual benchmark for synchronous final unlink.
 *
 * The benchmark creates an exclusive temporary parent, pre-seeds regular
 * files with one stripe-map entry and DS backing objects, then times the
 * actual SEQUENCE + PUTFH + REMOVE compound.  It therefore covers compound
 * dispatch, the final-unlink metadata path, and synchronous DS fencing.
 *
 * Without arguments it uses the in-process memdb and a mkdtemp-owned DS
 * directory.  --rondb requires a dedicated zero-GC-row benchmark schema and
 * an empty dedicated local DS mount.  It never recursively removes a directory:
 * every cleanup action addresses a known benchmark file or an empty
 * benchmark-owned directory.
 */

#include <dirent.h>
#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "compound.h"
#include "mds_catalogue.h"
#include "pnfs_mds.h"
#include "proxy_io.h"

#define BENCH_DEFAULT_ITERATIONS 200U
#define BENCH_WARMUP_ITERATIONS 10U
#define BENCH_MEMDB_MAX_ITERATIONS 200U
#define BENCH_RONDB_MAX_ITERATIONS 100000U
#define BENCH_DS_ID 1U
#define BENCH_STRIPE_UNIT 65536U

enum bench_exit_code {
    BENCH_EXIT_OK = 0,
    BENCH_EXIT_USAGE = 1,
    BENCH_EXIT_SETUP = 2,
    BENCH_EXIT_RUN = 3,
    BENCH_EXIT_CLEANUP = 4,
};

struct bench_file {
    uint64_t fileid;
    char name[64];
    bool owns_fileid;
};

struct bench_ctx {
    struct mds_catalogue *cat;
    struct mds_proxy_ctx *proxy;
    char parent_name[96];
    char *ds_mount;
    uint64_t parent_fileid;
    bool owns_ds_mount;
    bool use_rondb;
};

extern struct mds_catalogue *catalogue_memdb_open(void);

/**
 * Return the current monotonic time in seconds.
 *
 * @return Seconds from an arbitrary monotonic epoch.
 */
static double monotonic_seconds(void)
{
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    return (double)now.tv_sec + (double)now.tv_nsec / 1.0e9;
}

/**
 * Compare two latency samples for qsort().
 *
 * @param left Pointer to the first double sample.
 * @param right Pointer to the second double sample.
 * @return Negative, zero, or positive as left is less than, equal to, or
 *         greater than right.
 */
static int compare_double(const void *left, const void *right)
{
    const double left_value = *(const double *)left;
    const double right_value = *(const double *)right;

    if (left_value < right_value) {
        return -1;
    }
    if (left_value > right_value) {
        return 1;
    }
    return 0;
}

/**
 * Report command-line usage.
 *
 * @param program Program name from argv[0].
 */
static void usage(const char *program)
{
    (void)fprintf(stderr,
                  "usage: %s [ITERATIONS] [--rondb RONDB_CONF --ds-mount PATH]\n"
                  "       Default: %u iterations against memdb and a private\n"
                  "       temporary DS directory.  Memdb permits 1..%u;\n"
                  "       RonDB permits 1..%u.\n"
                  "       --rondb uses the actual catalogue backend.  Its schema\n"
                  "       must be dedicated to this benchmark and have zero GC\n"
                  "       rows before the run; --ds-mount must already contain\n"
                  "       an empty data/ directory.\n",
                  program, BENCH_DEFAULT_ITERATIONS,
                  BENCH_MEMDB_MAX_ITERATIONS, BENCH_RONDB_MAX_ITERATIONS);
}

/**
 * Parse a positive iteration count.
 *
 * @param text Input decimal string.
 * @param out Receives the validated count.
 * @return 0 on success, -1 otherwise.
 */
static int parse_iterations(const char *text, uint32_t *out)
{
    char *end = NULL;
    unsigned long parsed;

    if (text == NULL || out == NULL || *text == '\0') {
        return -1;
    }
    errno = 0;
    parsed = strtoul(text, &end, 10);
    if (errno != 0 || end == text || *end != '\0' || parsed == 0 ||
        parsed > UINT32_MAX) {
        return -1;
    }
    *out = (uint32_t)parsed;
    return 0;
}

/**
 * Build a path below a DS mount.
 *
 * @param mount DS mount path.
 * @param suffix Relative component below the mount.
 * @param path Receives the full path.
 * @param path_size Capacity of path.
 * @return 0 on success, -1 on truncation or invalid input.
 */
static int make_mount_path(const char *mount, const char *suffix, char *path,
                           size_t path_size)
{
    int written;

    if (mount == NULL || suffix == NULL || path == NULL || path_size == 0) {
        return -1;
    }
    written = snprintf(path, path_size, "%s/%s", mount, suffix);
    if (written < 0 || (size_t)written >= path_size) {
        return -1;
    }
    return 0;
}

/**
 * Determine whether a directory contains no entries except dot entries.
 *
 * @param path Directory to inspect.
 * @return 0 when empty, -1 otherwise.
 */
static int directory_is_empty(const char *path)
{
    DIR *directory;
    struct dirent *entry;
    int result = 0;

    directory = opendir(path);
    if (directory == NULL) {
        return -1;
    }
    while ((entry = readdir(directory)) != NULL) {
        if (strcmp(entry->d_name, ".") != 0 &&
            strcmp(entry->d_name, "..") != 0) {
            result = -1;
            break;
        }
    }
    if (closedir(directory) != 0) {
        result = -1;
    }
    return result;
}

/**
 * Create the private local DS mount used by the default memdb mode.
 *
 * @param ctx Benchmark context to initialise.
 * @return 0 on success, -1 otherwise.
 */
static int create_private_ds_mount(struct bench_ctx *ctx)
{
    char *mount_template;
    char data_path[MDS_MAX_PATH];

    mount_template = strdup("/tmp/pnfs-lattice-remove-bench-XXXXXX");
    if (mount_template == NULL) {
        return -1;
    }
    if (mkdtemp(mount_template) == NULL) {
        free(mount_template);
        return -1;
    }
    if (make_mount_path(mount_template, "data", data_path,
                        sizeof(data_path)) != 0 ||
        mkdir(data_path, 0700) != 0) {
        (void)rmdir(mount_template);
        free(mount_template);
        return -1;
    }

    ctx->ds_mount = mount_template;
    ctx->owns_ds_mount = true;
    return 0;
}

/**
 * Verify a caller-provided DS mount is ready without creating directories.
 *
 * @param ctx Benchmark context containing ds_mount.
 * @return 0 when data/ exists, is empty, and is a directory; -1 otherwise.
 */
static int verify_external_ds_mount(const struct bench_ctx *ctx)
{
    char data_path[MDS_MAX_PATH];
    struct stat data_stat;

    if (ctx == NULL ||
        make_mount_path(ctx->ds_mount, "data", data_path,
                        sizeof(data_path)) != 0 ||
        stat(data_path, &data_stat) != 0 ||
        !S_ISDIR(data_stat.st_mode) || directory_is_empty(data_path) != 0) {
        return -1;
    }
    return 0;
}

/**
 * Remove only the two empty directories created by create_private_ds_mount().
 *
 * @param ctx Benchmark context containing the owned mount.
 */
static void remove_private_ds_mount(struct bench_ctx *ctx)
{
    char data_path[MDS_MAX_PATH];

    if (ctx == NULL || !ctx->owns_ds_mount || ctx->ds_mount == NULL) {
        return;
    }
    if (make_mount_path(ctx->ds_mount, "data", data_path,
                        sizeof(data_path)) == 0 &&
        rmdir(data_path) != 0) {
        (void)fprintf(stderr, "Cleanup left non-empty directory: %s\n",
                      data_path);
    }
    if (rmdir(ctx->ds_mount) != 0) {
        (void)fprintf(stderr, "Cleanup left directory: %s\n", ctx->ds_mount);
    }
    free(ctx->ds_mount);
    ctx->ds_mount = NULL;
    ctx->owns_ds_mount = false;
}

/**
 * Check whether the catalogue GC queue is empty before an isolated run.
 *
 * @param cat Catalogue to inspect.
 * @return 0 when empty, -1 when rows exist or the count fails.
 */
static int verify_gc_queue_empty(struct mds_catalogue *cat)
{
    enum mds_status status;
    uint32_t count = 0;

    status = mds_cat_gc_count(cat, &count);
    if (status != MDS_OK) {
        (void)fprintf(stderr, "Could not count GC rows: %d\n", status);
        return -1;
    }
    if (count != 0) {
        (void)fprintf(stderr,
                      "Refusing to use a schema with %u existing GC row(s).\n",
                      count);
        return -1;
    }
    return 0;
}

/**
 * Initialise a private catalogue, proxy, and benchmark parent directory.
 *
 * @param ctx Receives the benchmark resources.
 * @param rondb_conf Optional RonDB configuration; NULL selects memdb.
 * @param external_ds_mount Optional existing DS mount for RonDB mode.
 * @return 0 on success, -1 otherwise.
 */
static int bench_setup(struct bench_ctx *ctx, const char *rondb_conf,
                       const char *external_ds_mount)
{
    enum mds_status status;
    struct mds_inode parent_inode;

    memset(ctx, 0, sizeof(*ctx));
    ctx->use_rondb = rondb_conf != NULL;

    if (ctx->use_rondb) {
        struct mds_config config;

        if (external_ds_mount == NULL) {
            return -1;
        }
        memset(&config, 0, sizeof(config));
        config.catalogue_backend = MDS_BACKEND_RONDB;
        config.self.id = 0;
        config.cluster_size = 1;
        config.ndb_conn_pool_size = 2;
        (void)snprintf(config.catalogue_backend_conf,
                       sizeof(config.catalogue_backend_conf), "%s",
                       rondb_conf);
        status = mds_catalogue_open(&config, &ctx->cat);
        if (status != MDS_OK) {
            (void)fprintf(stderr, "mds_catalogue_open failed: %d\n", status);
            return -1;
        }
        ctx->ds_mount = strdup(external_ds_mount);
        if (ctx->ds_mount == NULL || verify_external_ds_mount(ctx) != 0 ||
            verify_gc_queue_empty(ctx->cat) != 0) {
            return -1;
        }
    } else {
        ctx->cat = catalogue_memdb_open();
        if (ctx->cat == NULL || create_private_ds_mount(ctx) != 0) {
            return -1;
        }
    }

    status = mds_proxy_ctx_create(&ctx->proxy);
    if (status != MDS_OK ||
        mds_proxy_mount_set(ctx->proxy, BENCH_DS_ID, ctx->ds_mount) != MDS_OK) {
        return -1;
    }

    (void)snprintf(ctx->parent_name, sizeof(ctx->parent_name),
                   "__bench_remove_%" PRIuMAX "_%ld",
                   (uintmax_t)time(NULL), (long)getpid());
    status = mds_cat_ns_create(ctx->cat, NULL, MDS_FILEID_ROOT,
                               ctx->parent_name, MDS_FTYPE_DIR, 0700,
                               0, 0, NULL, &parent_inode);
    if (status != MDS_OK) {
        (void)fprintf(stderr, "Could not create benchmark parent %s: %d\n",
                      ctx->parent_name, status);
        return -1;
    }
    ctx->parent_fileid = parent_inode.fileid;
    return 0;
}

/**
 * Release benchmark resources after all owned metadata and objects are gone.
 *
 * @param ctx Benchmark context to tear down.
 */
static void bench_teardown(struct bench_ctx *ctx)
{
    if (ctx == NULL) {
        return;
    }
    if (ctx->proxy != NULL) {
        mds_proxy_ctx_destroy(ctx->proxy);
        ctx->proxy = NULL;
    }
    if (ctx->cat != NULL) {
        mds_catalogue_close(ctx->cat);
        ctx->cat = NULL;
    }
    remove_private_ds_mount(ctx);
    if (ctx->ds_mount != NULL) {
        free(ctx->ds_mount);
        ctx->ds_mount = NULL;
    }
}

/**
 * Seed one regular file, stripe map, and DS backing object before timing.
 *
 * @param ctx Benchmark resources.
 * @param name Namespace component for the benchmark file.
 * @param file Receives the resulting file identity.
 * @return 0 on success, -1 otherwise.
 */
static int seed_file(struct bench_ctx *ctx, const char *name,
                     struct bench_file *file)
{
    struct mds_cat_txn *txn = NULL;
    struct mds_ds_map_entry map_entry;
    struct mds_inode inode;
    struct timespec now;
    enum mds_status status;

    memset(file, 0, sizeof(*file));
    (void)snprintf(file->name, sizeof(file->name), "%s", name);
    status = mds_cat_alloc_fileid(ctx->cat, NULL, &file->fileid);
    if (status != MDS_OK) {
        return -1;
    }
    file->owns_fileid = true;
    clock_gettime(CLOCK_REALTIME, &now);
    memset(&inode, 0, sizeof(inode));
    inode.fileid = file->fileid;
    inode.type = MDS_FTYPE_REG;
    inode.mode = 0600;
    inode.nlink = 1;
    inode.atime = now;
    inode.mtime = now;
    inode.ctime = now;
    inode.change = 1;
    inode.generation = 1;
    inode.parent_fileid = ctx->parent_fileid;
    memset(&map_entry, 0, sizeof(map_entry));
    map_entry.ds_id = BENCH_DS_ID;

    status = mds_cat_txn_begin(ctx->cat, MDS_CAT_TXN_WRITE, &txn);
    if (status != MDS_OK) {
        return -1;
    }
    status = mds_cat_inode_put(ctx->cat, txn, &inode);
    if (status == MDS_OK) {
        status = mds_cat_dirent_put(ctx->cat, txn, ctx->parent_fileid, name,
                                    file->fileid, MDS_FTYPE_REG);
    }
    if (status == MDS_OK) {
        status = mds_cat_stripe_map_put(ctx->cat, txn, file->fileid, 1,
                                        BENCH_STRIPE_UNIT, 1, &map_entry);
    }
    if (status != MDS_OK) {
        mds_cat_txn_abort(txn);
        return -1;
    }
    status = mds_cat_txn_commit(txn);
    if (status != MDS_OK) {
        return -1;
    }

    status = mds_proxy_ensure_ds_file(ctx->proxy, BENCH_DS_ID, file->fileid,
                                      0, 0);
    if (status != MDS_OK) {
        return -1;
    }
    return 0;
}

/**
 * Remove an owned DS backing object by its known file identity.
 *
 * @param ctx Benchmark resources.
 * @param file Benchmark file whose DS object should be deleted.
 * @return 0 on success, -1 otherwise.
 */
static int cleanup_ds_object(const struct bench_ctx *ctx,
                             const struct bench_file *file)
{
    bool existed = false;
    enum mds_status status;

    if (!file->owns_fileid) {
        return 0;
    }
    status = mds_proxy_unlink_ds_file(ctx->proxy, BENCH_DS_ID, file->fileid,
                                      0, 0, &existed);
    if (status != MDS_OK) {
        (void)fprintf(stderr, "Could not remove DS object for fileid=%" PRIu64
                      ": %d\n", file->fileid, status);
        return -1;
    }
    return 0;
}

/**
 * Perform the production synchronous compound REMOVE path once.
 *
 * @param ctx Benchmark resources.
 * @param file File to remove.
 * @param latency_seconds Optional output for elapsed time.
 * @return 0 on success, -1 on a protocol or dispatch error.
 */
static int run_remove(const struct bench_ctx *ctx,
                      const struct bench_file *file,
                      double *latency_seconds)
{
    struct compound_data compound;
    struct nfs4_op ops[3];
    struct nfs4_result results[3];
    double started;
    uint32_t result_count;

    compound_init(&compound);
    compound.cat = ctx->cat;
    compound.proxy = ctx->proxy;
    compound.skip_transient_ndb = true;
    memset(ops, 0, sizeof(ops));
    memset(results, 0, sizeof(results));
    ops[0].opnum = OP_SEQUENCE;
    ops[1].opnum = OP_PUTFH;
    ops[1].arg.putfh.fh.fileid = ctx->parent_fileid;
    ops[2].opnum = OP_REMOVE;
    (void)snprintf(ops[2].arg.remove.name, sizeof(ops[2].arg.remove.name),
                   "%s", file->name);

    started = monotonic_seconds();
    result_count = compound_process(&compound, ops, results, 3);
    if (latency_seconds != NULL) {
        *latency_seconds = monotonic_seconds() - started;
    }
    if (result_count != 3 || results[2].status != NFS4_OK) {
        (void)fprintf(stderr,
                      "REMOVE failed for %s: results=%u status=%d\n",
                      file->name, result_count,
                      result_count >= 3 ? (int)results[2].status : -1);
        return -1;
    }
    return 0;
}

/**
 * Delete a known leftover namespace entry after an aborted benchmark run.
 *
 * @param ctx Benchmark resources.
 * @param file File whose metadata may remain.
 */
static void cleanup_file_metadata(const struct bench_ctx *ctx,
                                  const struct bench_file *file)
{
    if (!file->owns_fileid) {
        return;
    }
    (void)mds_cat_ns_remove(ctx->cat, NULL, ctx->parent_fileid, file->name);
    (void)mds_cat_stripe_map_del(ctx->cat, NULL, file->fileid);
}

/**
 * Test whether a GC record belongs to this benchmark run.
 *
 * @param files Array of all benchmark-created files.
 * @param file_count Number of elements in files.
 * @param fileid GC record file ID.
 * @return true exactly when fileid was created by this run.
 */
static bool is_owned_fileid(const struct bench_file *files,
                            uint32_t file_count, uint64_t fileid)
{
    uint32_t file_index;

    for (file_index = 0; file_index < file_count; file_index++) {
        if (files[file_index].owns_fileid &&
            files[file_index].fileid == fileid) {
            return true;
        }
    }
    return false;
}

/**
 * Drain only the GC rows created by this isolated benchmark run.
 *
 * @param ctx Benchmark resources.
 * @param files All files the run created.
 * @param file_count Number of entries in files.
 * @return 0 when every row was verified and removed, -1 otherwise.
 */
static int cleanup_gc_rows(const struct bench_ctx *ctx,
                           const struct bench_file *files,
                           uint32_t file_count)
{
    struct mds_gc_entry entry;
    enum mds_status status;

    for (;;) {
        status = mds_cat_gc_peek(ctx->cat, &entry);
        if (status == MDS_ERR_NOTFOUND) {
            return 0;
        }
        if (status != MDS_OK || !is_owned_fileid(files, file_count,
                                                  entry.fileid)) {
            (void)fprintf(stderr,
                          "Refusing to dequeue non-benchmark GC row "
                          "(fileid=%" PRIu64 ", status=%d).\n",
                          entry.fileid, status);
            return -1;
        }
        status = mds_cat_gc_dequeue(ctx->cat, NULL, entry.gc_seq);
        if (status != MDS_OK) {
            (void)fprintf(stderr, "Could not dequeue GC row %" PRIu64
                          ": %d\n", entry.gc_seq, status);
            return -1;
        }
    }
}

/**
 * Remove the owned parent once its benchmark children are gone.
 *
 * @param ctx Benchmark resources.
 * @return 0 on success, -1 otherwise.
 */
static int cleanup_parent(const struct bench_ctx *ctx)
{
    enum mds_status status;

    status = mds_cat_ns_remove(ctx->cat, NULL, MDS_FILEID_ROOT,
                               ctx->parent_name);
    if (status != MDS_OK) {
        (void)fprintf(stderr, "Could not remove benchmark parent %s: %d\n",
                      ctx->parent_name, status);
        return -1;
    }
    return 0;
}

/**
 * Print timing statistics from a complete serial run.
 *
 * @param latencies Individual latency measurements in seconds.
 * @param count Number of measurements.
 * @param elapsed_seconds Total loop wall-clock time in seconds.
 */
static void print_results(double *latencies, uint32_t count,
                          double elapsed_seconds)
{
    double sum = 0.0;
    uint32_t sample_index;
    uint32_t p50_index;
    uint32_t p95_index;
    uint32_t p99_index;

    for (sample_index = 0; sample_index < count; sample_index++) {
        sum += latencies[sample_index];
    }
    qsort(latencies, count, sizeof(*latencies), compare_double);
    p50_index = (count - 1U) * 50U / 100U;
    p95_index = (count - 1U) * 95U / 100U;
    p99_index = (count - 1U) * 99U / 100U;

    (void)printf("\nSynchronous REMOVE baseline (serial, in-process)\n");
    (void)printf("  Operations:  %u\n", count);
    (void)printf("  Throughput:  %.1f ops/s\n",
                 (double)count / elapsed_seconds);
    (void)printf("  Mean:        %.1f us\n",
                 sum * 1.0e6 / (double)count);
    (void)printf("  P50:         %.1f us\n", latencies[p50_index] * 1.0e6);
    (void)printf("  P95:         %.1f us\n", latencies[p95_index] * 1.0e6);
    (void)printf("  P99:         %.1f us\n", latencies[p99_index] * 1.0e6);
    (void)printf("  Max:         %.1f us\n", latencies[count - 1U] * 1.0e6);
}

/**
 * Execute the benchmark.
 *
 * @param argc Command-line argument count.
 * @param argv Command-line arguments.
 * @return Process exit code.
 */
int main(int argc, char **argv)
{
    struct bench_ctx ctx;
    struct bench_file *files = NULL;
    double *latencies = NULL;
    const char *rondb_conf = NULL;
    const char *ds_mount = NULL;
    uint32_t iterations = BENCH_DEFAULT_ITERATIONS;
    uint32_t created_count = 0;
    uint32_t file_index;
    int argument_index;
    int exit_code = BENCH_EXIT_SETUP;
    double started;
    double elapsed;

    for (argument_index = 1; argument_index < argc; argument_index++) {
        const char *argument = argv[argument_index];

        if (strcmp(argument, "--rondb") == 0) {
            if (++argument_index >= argc || rondb_conf != NULL) {
                usage(argv[0]);
                return BENCH_EXIT_USAGE;
            }
            rondb_conf = argv[argument_index];
        } else if (strcmp(argument, "--ds-mount") == 0) {
            if (++argument_index >= argc || ds_mount != NULL) {
                usage(argv[0]);
                return BENCH_EXIT_USAGE;
            }
            ds_mount = argv[argument_index];
        } else if (parse_iterations(argument, &iterations) != 0) {
            usage(argv[0]);
            return BENCH_EXIT_USAGE;
        }
    }
    if ((rondb_conf == NULL) != (ds_mount == NULL) ||
        (rondb_conf == NULL && iterations > BENCH_MEMDB_MAX_ITERATIONS) ||
        (rondb_conf != NULL && iterations > BENCH_RONDB_MAX_ITERATIONS)) {
        usage(argv[0]);
        return BENCH_EXIT_USAGE;
    }

    if (bench_setup(&ctx, rondb_conf, ds_mount) != 0) {
        (void)fprintf(stderr, "Benchmark setup failed.\n");
        bench_teardown(&ctx);
        return BENCH_EXIT_SETUP;
    }
    (void)printf("Synchronous REMOVE benchmark\n");
    (void)printf("  Backend:     %s\n", ctx.use_rondb ? "RonDB" : "memdb");
    (void)printf("  Parent:      /%s\n", ctx.parent_name);
    (void)printf("  DS mount:    %s\n", ctx.ds_mount);
    (void)printf("  Iterations:  %u (warmup %u)\n", iterations,
                 BENCH_WARMUP_ITERATIONS);
    (void)printf("  Mode:        transient state; synchronous DS fencing enabled\n");

    files = calloc(iterations + BENCH_WARMUP_ITERATIONS, sizeof(*files));
    latencies = calloc(iterations, sizeof(*latencies));
    if (files == NULL || latencies == NULL) {
        (void)fprintf(stderr, "Could not allocate benchmark storage.\n");
        goto cleanup;
    }

    for (file_index = 0; file_index < BENCH_WARMUP_ITERATIONS;
         file_index++) {
        char name[64];

        (void)snprintf(name, sizeof(name), "warm_%06u", file_index);
        if (seed_file(&ctx, name, &files[created_count]) != 0) {
            created_count++;
            (void)fprintf(stderr, "Could not seed warmup %u.\n",
                          file_index);
            goto cleanup;
        }
        created_count++;
        if (run_remove(&ctx, &files[created_count - 1U], NULL) != 0 ||
            cleanup_ds_object(&ctx, &files[created_count - 1U]) != 0) {
            (void)fprintf(stderr, "Warmup failed at iteration %u.\n",
                          file_index);
            goto cleanup;
        }
    }
    for (file_index = 0; file_index < iterations; file_index++) {
        char name[64];

        (void)snprintf(name, sizeof(name), "remove_%06u", file_index);
        if (seed_file(&ctx, name, &files[created_count]) != 0) {
            created_count++;
            (void)fprintf(stderr, "Could not seed iteration %u.\n",
                          file_index);
            goto cleanup;
        }
        created_count++;
    }

    started = monotonic_seconds();
    for (file_index = BENCH_WARMUP_ITERATIONS; file_index < created_count;
         file_index++) {
        uint32_t latency_index = file_index - BENCH_WARMUP_ITERATIONS;
        if (run_remove(&ctx, &files[file_index],
                       &latencies[latency_index]) != 0) {
            (void)fprintf(stderr, "Timed REMOVE failed at iteration %u.\n",
                          latency_index);
            goto cleanup;
        }
    }
    elapsed = monotonic_seconds() - started;
    print_results(latencies, iterations, elapsed);
    for (file_index = BENCH_WARMUP_ITERATIONS; file_index < created_count;
         file_index++) {
        if (cleanup_ds_object(&ctx, &files[file_index]) != 0) {
            exit_code = BENCH_EXIT_CLEANUP;
            goto cleanup;
        }
    }
    exit_code = BENCH_EXIT_OK;

cleanup:
    for (file_index = 0; file_index < created_count; file_index++) {
        cleanup_file_metadata(&ctx, &files[file_index]);
        if (cleanup_ds_object(&ctx, &files[file_index]) != 0 &&
            exit_code == BENCH_EXIT_OK) {
            exit_code = BENCH_EXIT_CLEANUP;
        }
    }
    if (files != NULL &&
        cleanup_gc_rows(&ctx, files, created_count) != 0 &&
        exit_code == BENCH_EXIT_OK) {
        exit_code = BENCH_EXIT_CLEANUP;
    }
    if (ctx.cat != NULL && cleanup_parent(&ctx) != 0 &&
        exit_code == BENCH_EXIT_OK) {
        exit_code = BENCH_EXIT_CLEANUP;
    }
    free(latencies);
    free(files);
    bench_teardown(&ctx);

    if (exit_code == BENCH_EXIT_OK) {
        (void)printf("Cleanup complete.\n");
    }
    return exit_code;
}

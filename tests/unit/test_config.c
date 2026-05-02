/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_config.c — Unit tests for MDS configuration parsing.
 *
 * RonDB-native: default backend is MDS_BACKEND_RONDB.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pnfs_mds.h"

/* ------------------------------------------------------------------- */

static int tests_run;
static int tests_passed;

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n", \
                __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        return; \
    } \
} while (0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", \
                __FILE__, __LINE__, #cond); \
        return; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    tests_run++; \
    fprintf(stdout, "  %-50s", #fn); \
    fflush(stdout); \
    fn(); \
    tests_passed++; \
    fprintf(stdout, "PASS\n"); \
} while (0)

/* ------------------------------------------------------------------- */

static void test_defaults(void)
{
    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));

    /* Default backend is RonDB. */
    ASSERT_EQ(cfg.catalogue_backend, MDS_BACKEND_RONDB);

    /* Default workload profile is DEFAULT. */
    ASSERT_EQ(cfg.workload_profile, MDS_PROFILE_DEFAULT);

    /* Default image/replay modes are OFF. */
    ASSERT_EQ(cfg.catalog_image_mode, MDS_IMAGE_OFF);
    ASSERT_EQ(cfg.catalog_replay_mode, MDS_REPLAY_OFF);
}

static void test_null_args(void)
{
    /* NULL path should fail gracefully. */
    struct mds_config cfg;
    enum mds_status st = mds_config_load(NULL, &cfg);
    ASSERT_EQ(st, MDS_ERR_INVAL);

    /* NULL cfg should fail gracefully. */
    st = mds_config_load("/nonexistent", NULL);
    ASSERT_EQ(st, MDS_ERR_INVAL);
}

static void test_missing_file(void)
{
    struct mds_config cfg;
    enum mds_status st = mds_config_load("/tmp/no_such_config_file.conf", &cfg);
    ASSERT_EQ(st, MDS_ERR_IO);
}

static void test_backend_default_is_rondb(void)
{
    struct mds_config cfg;
    memset(&cfg, 0, sizeof(cfg));
    /* Zero-init backend should be MDS_BACKEND_RONDB (value 0). */
    ASSERT_EQ(cfg.catalogue_backend, MDS_BACKEND_RONDB);
}

static void test_version_constants(void)
{
    ASSERT_TRUE(PNFS_MDS_VERSION_MAJOR >= 0);
    ASSERT_TRUE(PNFS_MDS_WIRE_COMPAT_VERSION >= 1);
}

static void test_workload_profiles_distinct(void)
{
    ASSERT_TRUE(MDS_PROFILE_DEFAULT != MDS_PROFILE_HPC);
    ASSERT_TRUE(MDS_PROFILE_HPC != MDS_PROFILE_AI_TRAINING);
    ASSERT_TRUE(MDS_PROFILE_AI_TRAINING != MDS_PROFILE_GENOMICS);
    ASSERT_TRUE(MDS_PROFILE_GENOMICS != MDS_PROFILE_MEDIA);
}

static void test_image_replay_defaults(void)
{
    ASSERT_EQ(MDS_IMAGE_OFF, 0);
    ASSERT_EQ(MDS_REPLAY_OFF, 0);
    ASSERT_TRUE(MDS_IMAGE_SHADOW != MDS_IMAGE_OFF);
    ASSERT_TRUE(MDS_REPLAY_JOURNAL != MDS_REPLAY_OFF);
}

/*
 * Helper: write an INI fragment to a temp file, parse, check.
 * Every test here goes through mds_config_load() so the parser
 * is exercised end-to-end rather than asserting static defaults.
 */
static int write_tmp_ini(const char *content, char path_out[128])
{
    (void)snprintf(path_out, 128, "/tmp/pnfs-cfg-%d.conf",
                   (int)getpid());
    FILE *f = fopen(path_out, "w");
    if (f == NULL) {
        return -1;
    }
    fputs(content, f);
    fclose(f);
    return 0;
}

static void test_parse_new_cache_keys(void)
{
    char path[128];
    struct mds_config cfg;
    ASSERT_EQ(write_tmp_ini(
        "inline_max_size = 4096\n"
        "dirent_cache_size = 4242\n"
        "negative_cache_ttl_ms = 7777\n",
        path), 0);
    enum mds_status st = mds_config_load(path, &cfg);
    ASSERT_EQ(st, MDS_OK);
    ASSERT_EQ((int)cfg.inline_max_size, 4096);
    ASSERT_EQ((int)cfg.dirent_cache_size, 4242);
    ASSERT_EQ((int)cfg.negative_cache_ttl_ms, 7777);
    unlink(path);
}

static void test_parse_promoted_knobs(void)
{
    char path[128];
    struct mds_config cfg;
    ASSERT_EQ(write_tmp_ini(
        "ds_health_fail_threshold = 12\n"
        "cb_recall_timeout_ms = 2500\n"
        "dir_deleg_recall_timeout_ms = 3500\n"
        "metrics_http_port = 9191\n",
        path), 0);
    ASSERT_EQ(mds_config_load(path, &cfg), MDS_OK);
    ASSERT_EQ((int)cfg.ds_health_fail_threshold, 12);
    ASSERT_EQ((int)cfg.cb_recall_timeout_ms, 2500);
    ASSERT_EQ((int)cfg.dir_deleg_recall_timeout_ms, 3500);
    ASSERT_EQ((int)cfg.metrics_http_port, 9191);
    unlink(path);
}

static void test_promoted_knob_defaults(void)
{
    char path[128];
    struct mds_config cfg;
    /* Empty INI — every promoted knob must land at its
     * documented default. */
    ASSERT_EQ(write_tmp_ini("", path), 0);
    ASSERT_EQ(mds_config_load(path, &cfg), MDS_OK);
    ASSERT_EQ((int)cfg.ds_health_fail_threshold, 6);
    ASSERT_EQ((int)cfg.cb_recall_timeout_ms, 5000);
    ASSERT_EQ((int)cfg.dir_deleg_recall_timeout_ms, 5000);
    ASSERT_EQ((int)cfg.metrics_http_port, 9090);
    ASSERT_EQ((int)cfg.inline_max_size, 65536);
    ASSERT_EQ((int)cfg.dirent_cache_size, 32768);
    ASSERT_EQ((int)cfg.negative_cache_ttl_ms, 5000);
    unlink(path);
}

static void test_out_of_range_rejected(void)
{
    char path[128];
    struct mds_config cfg;
    /* Values outside the documented ranges should NOT be applied
     * (defaults retained).  Keeps operators from foot-gunning the
     * daemon with 50000000 ms timeouts. */
    ASSERT_EQ(write_tmp_ini(
        "ds_health_fail_threshold = 999999\n"
        "cb_recall_timeout_ms = 999999999\n"
        "dir_deleg_recall_timeout_ms = 10\n"
        "metrics_http_port = 200000\n",
        path), 0);
    ASSERT_EQ(mds_config_load(path, &cfg), MDS_OK);
    ASSERT_EQ((int)cfg.ds_health_fail_threshold, 6);
    ASSERT_EQ((int)cfg.cb_recall_timeout_ms, 5000);
    ASSERT_EQ((int)cfg.dir_deleg_recall_timeout_ms, 5000);
    ASSERT_EQ((int)cfg.metrics_http_port, 9090);
    unlink(path);
}

static void test_mountd_compat_defaults(void)
{
    char path[128];
    struct mds_config cfg;
    ASSERT_EQ(write_tmp_ini("", path), 0);
    ASSERT_EQ(mds_config_load(path, &cfg), MDS_OK);
    /* Enabled by default — a fresh install gets `showmount -e`
     * working out-of-the-box.  Operators who want it off set
     * mountd_compat_enabled = false in mds.conf. */
    ASSERT_EQ((int)cfg.mountd_compat_enabled, 1);
    ASSERT_EQ((int)cfg.mountd_compat_port, 20048);
    ASSERT_EQ((int)cfg.mountd_compat_register_rpcbind, 1);
    ASSERT_TRUE(strcmp(cfg.mountd_compat_bind_addr, "0.0.0.0") == 0);
    /* Empty INI — no exports parsed; runtime falls back to "/". */
    ASSERT_EQ((int)cfg.mountd_compat_export_count, 0);
    unlink(path);
}

static void test_mountd_compat_can_be_disabled(void)
{
    char path[128];
    struct mds_config cfg;
    /* Verify the override path — operators can opt out. */
    ASSERT_EQ(write_tmp_ini(
        "mountd_compat_enabled = false\n",
        path), 0);
    ASSERT_EQ(mds_config_load(path, &cfg), MDS_OK);
    ASSERT_EQ((int)cfg.mountd_compat_enabled, 0);
    unlink(path);
}

static void test_mountd_compat_parse(void)
{
    char path[128];
    struct mds_config cfg;
    ASSERT_EQ(write_tmp_ini(
        "mountd_compat_enabled = true\n"
        "mountd_compat_port = 30048\n"
        "mountd_compat_register_rpcbind = false\n"
        "mountd_compat_bind_addr = 127.0.0.1\n"
        "mountd_compat_exports = /pnfs, /scratch, /home\n",
        path), 0);
    ASSERT_EQ(mds_config_load(path, &cfg), MDS_OK);
    ASSERT_EQ((int)cfg.mountd_compat_enabled, 1);
    ASSERT_EQ((int)cfg.mountd_compat_port, 30048);
    ASSERT_EQ((int)cfg.mountd_compat_register_rpcbind, 0);
    ASSERT_TRUE(strcmp(cfg.mountd_compat_bind_addr, "127.0.0.1") == 0);
    ASSERT_EQ((int)cfg.mountd_compat_export_count, 3);
    ASSERT_TRUE(strcmp(cfg.mountd_compat_exports[0], "/pnfs") == 0);
    ASSERT_TRUE(strcmp(cfg.mountd_compat_exports[1], "/scratch") == 0);
    ASSERT_TRUE(strcmp(cfg.mountd_compat_exports[2], "/home") == 0);
    unlink(path);
}

static void test_mountd_compat_export_overflow(void)
{
    char path[128];
    struct mds_config cfg;
    /* 17 entries — the limit is 16, so the last one is dropped. */
    ASSERT_EQ(write_tmp_ini(
        "mountd_compat_exports = /a,/b,/c,/d,/e,/f,/g,/h,"
        "/i,/j,/k,/l,/m,/n,/o,/p,/q\n",
        path), 0);
    ASSERT_EQ(mds_config_load(path, &cfg), MDS_OK);
    ASSERT_EQ((int)cfg.mountd_compat_export_count, 16);
    /* The 16th (index 15) is /p; /q must NOT appear. */
    ASSERT_TRUE(strcmp(cfg.mountd_compat_exports[15], "/p") == 0);
    unlink(path);
}

static void test_mountd_compat_port_out_of_range(void)
{
    char path[128];
    struct mds_config cfg;
    ASSERT_EQ(write_tmp_ini(
        "mountd_compat_port = 999999\n",
        path), 0);
    ASSERT_EQ(mds_config_load(path, &cfg), MDS_OK);
    /* Default retained when out of range. */
    ASSERT_EQ((int)cfg.mountd_compat_port, 20048);
    unlink(path);
}

int main(void)
{
    fprintf(stdout, "test_config (RonDB-native)\n");

    RUN_TEST(test_defaults);
    RUN_TEST(test_null_args);
    RUN_TEST(test_missing_file);
    RUN_TEST(test_backend_default_is_rondb);
    RUN_TEST(test_version_constants);
    RUN_TEST(test_workload_profiles_distinct);
    RUN_TEST(test_image_replay_defaults);

    /* C1: config completeness coverage. */
    RUN_TEST(test_parse_new_cache_keys);
    RUN_TEST(test_parse_promoted_knobs);
    RUN_TEST(test_promoted_knob_defaults);
    RUN_TEST(test_out_of_range_rejected);

    /* mountd_compat keys. */
    RUN_TEST(test_mountd_compat_defaults);
    RUN_TEST(test_mountd_compat_can_be_disabled);
    RUN_TEST(test_mountd_compat_parse);
    RUN_TEST(test_mountd_compat_export_overflow);
    RUN_TEST(test_mountd_compat_port_out_of_range);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}

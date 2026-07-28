/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_companion.c -- Unit tests for the companion process supervisor.
 *
 * Two halves:
 *   1. Config parsing -- companion.<name>.* keys go through the real
 *      mds_config_load() so the allowlist, range checks, and the
 *      startup-refusing validations are exercised end to end.
 *   2. Supervision -- the supervisor is driven against short-lived
 *      helper programs (/bin/sleep, /bin/true, /bin/false) and a
 *      generated shell script, covering spawn, clean exit,
 *      restart-on-failure with a capped burst, deliberate stop,
 *      unknown-name control, and the NDB slot budget in both
 *      advisory and enforce modes.
 *
 * Timing: the supervisor polls on a 200 ms tick, so every assertion
 * about a state transition waits on a deadline rather than sleeping a
 * fixed amount.  Budgets are generous so the tests stay reliable on a
 * loaded CI worker.
 */

#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/types.h>

#include "pnfs_mds.h"
#include "companion.h"

/* -----------------------------------------------------------------------
 * Minimal test framework (mirrors tests/unit/test_ds_gc.c).
 * ----------------------------------------------------------------------- */

static int tests_run    = 0;
static int tests_passed = 0;

#define RUN_TEST(fn) do {                                              \
    tests_run++;                                                        \
    fprintf(stdout, "  %-46s ", #fn);                                   \
    fflush(stdout);                                                     \
    fn();                                                               \
    tests_passed++;                                                     \
    fprintf(stdout, "PASS\n");                                          \
} while (0)

#define ASSERT_EQ(a, b) do {                                           \
    long long _av = (long long)(a);                                     \
    long long _bv = (long long)(b);                                     \
    if (_av != _bv) {                                                   \
        fprintf(stderr, "FAIL at %s:%d: %s (=%lld) != %s (=%lld)\n",    \
                __FILE__, __LINE__, #a, _av, #b, _bv);                  \
        exit(1);                                                        \
    }                                                                   \
} while (0)

#define ASSERT_TRUE(cond) do {                                         \
    if (!(cond)) {                                                      \
        fprintf(stderr, "FAIL at %s:%d: !(%s)\n",                       \
                __FILE__, __LINE__, #cond);                             \
        exit(1);                                                        \
    }                                                                   \
} while (0)

/* -----------------------------------------------------------------------
 * Fixtures
 * ----------------------------------------------------------------------- */

/**
 * Pick an executable that exists on this host.
 *
 * /bin is a symlink to /usr/bin on modern distributions, but check
 * both so the tests do not depend on that being true.
 */
static const char *find_exec(const char *name)
{
    static char path[256];
    const char *dirs[] = { "/bin", "/usr/bin" };

    for (size_t i = 0; i < sizeof(dirs) / sizeof(dirs[0]); i++) {
        (void)snprintf(path, sizeof(path), "%s/%s", dirs[i], name);
        if (access(path, X_OK) == 0) {
            return path;
        }
    }
    fprintf(stderr, "FAIL: cannot find executable '%s'\n", name);
    exit(1);
}

static void write_tmp_file(const char *path, const char *content,
                           mode_t mode)
{
    FILE *f = fopen(path, "w");

    ASSERT_TRUE(f != NULL);
    ASSERT_TRUE(fputs(content, f) >= 0);
    ASSERT_EQ(fclose(f), 0);
    ASSERT_EQ(chmod(path, mode), 0);
}

/** Load a config from an INI fragment written to a per-pid temp file. */
static enum mds_status load_ini(const char *content,
                                struct mds_config *cfg)
{
    char path[128];
    enum mds_status st;

    (void)snprintf(path, sizeof(path), "/tmp/pnfs-companion-%d.conf",
                   (int)getpid());
    write_tmp_file(path, content, 0600);
    st = mds_config_load(path, cfg);
    (void)unlink(path);
    return st;
}

/** Seed a config with one usable declaration and no catalogue needs. */
static void base_decl(struct mds_config *cfg, const char *name,
                      const char *exec_path)
{
    memset(cfg, 0, sizeof(*cfg));
    cfg->companion_enabled = true;
    cfg->companion_count = 1;
    cfg->ndb_conn_pool_size = 2;
    cfg->companion_ndb_admission = COMPANION_NDB_ADVISORY;

    struct mds_companion_decl *d = &cfg->companions[0];

    (void)snprintf(d->name, sizeof(d->name), "%s", name);
    (void)snprintf(d->exec_path, sizeof(d->exec_path), "%s", exec_path);
    d->argc                   = 0;
    d->restart                = COMPANION_RESTART_NEVER;
    d->restart_backoff_ms     = 100;
    d->restart_backoff_max_ms = 200;
    d->max_restarts           = 0;
    d->restart_reset_sec      = 60;
    d->stop_timeout_ms        = 1000;
    d->enabled                = true;
    d->autostart              = true;
}

/** Monotonic milliseconds for test deadlines. */
static uint64_t test_now_ms(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000U + (uint64_t)(ts.tv_nsec / 1000000L);
}

/**
 * Wait until @a name reaches @a want, or the timeout expires.
 *
 * @return The last observed state so callers can assert on it.
 */
static uint8_t wait_for_state(struct companion_supervisor *sup,
                              const char *name, uint8_t want,
                              uint32_t timeout_ms)
{
    uint64_t deadline = test_now_ms() + timeout_ms;
    struct companion_status s;

    memset(&s, 0, sizeof(s));
    for (;;) {
        if (companion_status_one(sup, name, &s) == MDS_OK &&
            s.state == want) {
            return s.state;
        }
        if (test_now_ms() >= deadline) {
            return s.state;
        }
        usleep(20 * 1000);
    }
}

/** Wait until @a name records at least @a want restarts. */
static uint32_t wait_for_restarts(struct companion_supervisor *sup,
                                  const char *name, uint32_t want,
                                  uint32_t timeout_ms)
{
    uint64_t deadline = test_now_ms() + timeout_ms;
    struct companion_status s;

    memset(&s, 0, sizeof(s));
    for (;;) {
        if (companion_status_one(sup, name, &s) == MDS_OK &&
            s.restart_count >= want) {
            return s.restart_count;
        }
        if (test_now_ms() >= deadline) {
            return s.restart_count;
        }
        usleep(20 * 1000);
    }
}

/* -----------------------------------------------------------------------
 * Config parsing
 * ----------------------------------------------------------------------- */

static void test_cfg_minimal_declaration(void)
{
    struct mds_config cfg;

    ASSERT_EQ(load_ini(
        "companion.ingest.exec = /usr/bin/true\n", &cfg), MDS_OK);
    ASSERT_EQ((int)cfg.companion_count, 1);
    ASSERT_TRUE(strcmp(cfg.companions[0].name, "ingest") == 0);
    ASSERT_TRUE(strcmp(cfg.companions[0].exec_path,
                       "/usr/bin/true") == 0);

    /* Documented defaults for an otherwise bare declaration. */
    ASSERT_EQ((int)cfg.companions[0].restart,
              (int)COMPANION_RESTART_ON_FAILURE);
    ASSERT_EQ((int)cfg.companions[0].restart_backoff_ms, 1000);
    ASSERT_EQ((int)cfg.companions[0].restart_backoff_max_ms, 60000);
    ASSERT_EQ((int)cfg.companions[0].max_restarts, 5);
    ASSERT_EQ((int)cfg.companions[0].restart_reset_sec, 60);
    ASSERT_EQ((int)cfg.companions[0].stop_timeout_ms, 10000);
    ASSERT_TRUE(cfg.companions[0].enabled);
    ASSERT_TRUE(cfg.companions[0].autostart);
    ASSERT_EQ((int)cfg.companions[0].argc, 0);

    /* Master switch on, budget undeclared, admission advisory. */
    ASSERT_TRUE(cfg.companion_enabled);
    ASSERT_EQ((int)cfg.ndb_api_slots_total, 0);
    ASSERT_EQ((int)cfg.companion_ndb_admission,
              (int)COMPANION_NDB_ADVISORY);
}

static void test_cfg_full_declaration(void)
{
    struct mds_config cfg;

    ASSERT_EQ(load_ini(
        "ndb_api_slots_total = 48\n"
        "ndb_api_slots_reserved = 8\n"
        "companion_ndb_admission = enforce\n"
        "companion.etl.exec = /opt/site/bin/etl\n"
        "companion.etl.arg[0] = --config\n"
        "companion.etl.arg[1] = /etc/site/etl.conf\n"
        "companion.etl.workdir = /var/lib/site\n"
        "companion.etl.log_file = /var/log/site-etl.log\n"
        "companion.etl.restart = always\n"
        "companion.etl.restart_backoff_ms = 500\n"
        "companion.etl.restart_backoff_max_ms = 30000\n"
        "companion.etl.max_restarts = 9\n"
        "companion.etl.restart_reset_sec = 120\n"
        "companion.etl.stop_timeout_ms = 5000\n"
        "companion.etl.start_delay_ms = 250\n"
        "companion.etl.rlimit_as_mb = 4096\n"
        "companion.etl.rlimit_nofile = 1024\n"
        "companion.etl.rlimit_cpu_sec = 600\n"
        "companion.etl.ndb_conns = 2\n"
        "companion.etl.autostart = false\n", &cfg), MDS_OK);

    const struct mds_companion_decl *d = &cfg.companions[0];

    ASSERT_EQ((int)cfg.companion_count, 1);
    ASSERT_EQ((int)cfg.ndb_api_slots_total, 48);
    ASSERT_EQ((int)cfg.ndb_api_slots_reserved, 8);
    ASSERT_EQ((int)cfg.companion_ndb_admission,
              (int)COMPANION_NDB_ENFORCE);
    ASSERT_EQ((int)d->argc, 2);
    ASSERT_TRUE(strcmp(d->argv[0], "--config") == 0);
    ASSERT_TRUE(strcmp(d->argv[1], "/etc/site/etl.conf") == 0);
    ASSERT_TRUE(strcmp(d->workdir, "/var/lib/site") == 0);
    ASSERT_TRUE(strcmp(d->log_file, "/var/log/site-etl.log") == 0);
    ASSERT_EQ((int)d->restart, (int)COMPANION_RESTART_ALWAYS);
    ASSERT_EQ((int)d->restart_backoff_ms, 500);
    ASSERT_EQ((int)d->restart_backoff_max_ms, 30000);
    ASSERT_EQ((int)d->max_restarts, 9);
    ASSERT_EQ((int)d->restart_reset_sec, 120);
    ASSERT_EQ((int)d->stop_timeout_ms, 5000);
    ASSERT_EQ((int)d->start_delay_ms, 250);
    ASSERT_EQ((int)d->rlimit_as_mb, 4096);
    ASSERT_EQ((int)d->rlimit_nofile, 1024);
    ASSERT_EQ((int)d->rlimit_cpu_sec, 600);
    ASSERT_EQ((int)d->ndb_conns, 2);
    ASSERT_TRUE(!d->autostart);
}

static void test_cfg_multiple_and_ordering(void)
{
    struct mds_config cfg;

    /* Interleaved keys must land on the right declaration. */
    ASSERT_EQ(load_ini(
        "companion.alpha.exec = /usr/bin/true\n"
        "companion.beta.exec = /usr/bin/false\n"
        "companion.alpha.ndb_conns = 3\n"
        "companion.beta.ndb_conns = 1\n", &cfg), MDS_OK);
    ASSERT_EQ((int)cfg.companion_count, 2);
    ASSERT_TRUE(strcmp(cfg.companions[0].name, "alpha") == 0);
    ASSERT_TRUE(strcmp(cfg.companions[1].name, "beta") == 0);
    ASSERT_EQ((int)cfg.companions[0].ndb_conns, 3);
    ASSERT_EQ((int)cfg.companions[1].ndb_conns, 1);
}

static void test_cfg_relative_exec_rejected(void)
{
    struct mds_config cfg;

    /* PATH is never searched, so a bare name cannot be resolved. */
    ASSERT_EQ(load_ini(
        "companion.bad.exec = true\n", &cfg), MDS_ERR_INVAL);
}

static void test_cfg_missing_exec_rejected(void)
{
    struct mds_config cfg;

    ASSERT_EQ(load_ini(
        "companion.bad.ndb_conns = 1\n", &cfg), MDS_ERR_INVAL);
}

static void test_cfg_relative_paths_rejected(void)
{
    struct mds_config cfg;

    ASSERT_EQ(load_ini(
        "companion.bad.exec = /usr/bin/true\n"
        "companion.bad.workdir = var/tmp\n", &cfg), MDS_ERR_INVAL);
    ASSERT_EQ(load_ini(
        "companion.bad.exec = /usr/bin/true\n"
        "companion.bad.log_file = relative.log\n", &cfg), MDS_ERR_INVAL);
}

static void test_cfg_scope_requires_memory_max(void)
{
    struct mds_config cfg;

    ASSERT_EQ(load_ini(
        "companion.bad.exec = /usr/bin/true\n"
        "companion.bad.systemd_scope = true\n", &cfg), MDS_ERR_INVAL);

    /* With a memory ceiling it becomes valid. */
    ASSERT_EQ(load_ini(
        "companion.good.exec = /usr/bin/true\n"
        "companion.good.systemd_scope = true\n"
        "companion.good.memory_max_mb = 512\n", &cfg), MDS_OK);
}

static void test_cfg_arg_hole_rejected(void)
{
    struct mds_config cfg;

    /* arg[0] missing would shift the vector; refuse rather than guess. */
    ASSERT_EQ(load_ini(
        "companion.bad.exec = /usr/bin/true\n"
        "companion.bad.arg[1] = --only-second\n", &cfg), MDS_ERR_INVAL);
}

static void test_cfg_disabled_declaration_kept(void)
{
    struct mds_config cfg;

    /*
     * A disabled declaration is retained (so it still reports as
     * DISABLED) and is exempt from completeness validation.
     */
    ASSERT_EQ(load_ini(
        "companion.off.enabled = false\n", &cfg), MDS_OK);
    ASSERT_EQ((int)cfg.companion_count, 1);
    ASSERT_TRUE(!cfg.companions[0].enabled);
}

static void test_cfg_out_of_range_keeps_default(void)
{
    struct mds_config cfg;

    /* Out-of-range numerics warn and keep the default; not fatal. */
    ASSERT_EQ(load_ini(
        "companion.x.exec = /usr/bin/true\n"
        "companion.x.max_restarts = 999999\n"
        "companion.x.stop_timeout_ms = 1\n"
        "companion.x.ndb_conns = 4096\n", &cfg), MDS_OK);
    ASSERT_EQ((int)cfg.companions[0].max_restarts, 5);
    ASSERT_EQ((int)cfg.companions[0].stop_timeout_ms, 10000);
    ASSERT_EQ((int)cfg.companions[0].ndb_conns, 0);
}

static void test_cfg_invalid_name_ignored(void)
{
    struct mds_config cfg;

    /* '/' is not in the permitted name alphabet. */
    ASSERT_EQ(load_ini(
        "companion.bad/name.exec = /usr/bin/true\n", &cfg), MDS_OK);
    ASSERT_EQ((int)cfg.companion_count, 0);
}

static void test_cfg_backoff_cap_clamped(void)
{
    struct mds_config cfg;

    /* A cap below the initial backoff is raised, not rejected. */
    ASSERT_EQ(load_ini(
        "companion.x.exec = /usr/bin/true\n"
        "companion.x.restart_backoff_ms = 5000\n"
        "companion.x.restart_backoff_max_ms = 1000\n", &cfg), MDS_OK);
    ASSERT_EQ((int)cfg.companions[0].restart_backoff_max_ms, 5000);
}

/* -----------------------------------------------------------------------
 * Supervision
 * ----------------------------------------------------------------------- */

static void test_sup_inert_when_nothing_declared(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = (void *)0xDEADBEEF;

    memset(&cfg, 0, sizeof(cfg));
    cfg.companion_enabled = true;
    cfg.companion_count = 0;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_TRUE(sup == NULL);

    /* NULL handle must be tolerated everywhere. */
    ASSERT_EQ(companion_ctl_start(NULL, "x"), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(companion_ctl_stop(NULL, "x"), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(companion_ctl_restart(NULL, "x"), MDS_ERR_NOSUPPORT);
    ASSERT_EQ(companion_status_all(NULL, NULL, 0), 0);
    companion_stop(NULL);
}

static void test_sup_master_switch_off(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = (void *)0xDEADBEEF;

    base_decl(&cfg, "held", find_exec("sleep"));
    cfg.companion_enabled = false;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_TRUE(sup == NULL);
}

static void test_sup_disabled_declaration_reports_disabled(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    struct companion_status s;

    base_decl(&cfg, "off", find_exec("sleep"));
    cfg.companions[0].enabled = false;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_TRUE(sup != NULL);
    ASSERT_EQ(companion_status_one(sup, "off", &s), MDS_OK);
    ASSERT_EQ((int)s.state, (int)COMPANION_STATE_DISABLED);

    /* A disabled companion cannot be started by an admin request. */
    ASSERT_EQ(companion_ctl_start(sup, "off"), MDS_ERR_PERM);

    companion_stop(sup);
}

static void test_sup_spawn_and_stop(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    struct companion_status s;

    /* Long-lived child so the running state is observable. */
    base_decl(&cfg, "sleeper", find_exec("sleep"));
    (void)snprintf(cfg.companions[0].argv[0],
                   MDS_COMPANION_ARG_MAX, "60");
    cfg.companions[0].argc = 1;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_TRUE(sup != NULL);

    ASSERT_EQ(wait_for_state(sup, "sleeper",
                             COMPANION_STATE_RUNNING, 5000),
              COMPANION_STATE_RUNNING);
    ASSERT_EQ(companion_status_one(sup, "sleeper", &s), MDS_OK);
    ASSERT_TRUE(s.pid > 0);

    /* Starting a running companion is a conflict, not a no-op. */
    ASSERT_EQ(companion_ctl_start(sup, "sleeper"), MDS_ERR_EXISTS);

    /* Deliberate stop settles in STOPPED, never FAILED. */
    ASSERT_EQ(companion_ctl_stop(sup, "sleeper"), MDS_OK);
    ASSERT_EQ(wait_for_state(sup, "sleeper",
                             COMPANION_STATE_STOPPED, 5000),
              COMPANION_STATE_STOPPED);
    ASSERT_EQ(companion_status_one(sup, "sleeper", &s), MDS_OK);
    ASSERT_EQ(s.pid, 0);

    /* And it can be started again afterwards. */
    ASSERT_EQ(companion_ctl_start(sup, "sleeper"), MDS_OK);
    ASSERT_EQ(wait_for_state(sup, "sleeper",
                             COMPANION_STATE_RUNNING, 5000),
              COMPANION_STATE_RUNNING);

    companion_stop(sup);
}

static void test_sup_clean_exit_not_restarted(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    struct companion_status s;

    /* restart=on-failure: exit 0 means "work done". */
    base_decl(&cfg, "oneshot", find_exec("true"));
    cfg.companions[0].restart = COMPANION_RESTART_ON_FAILURE;
    cfg.companions[0].max_restarts = 5;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_EQ(wait_for_state(sup, "oneshot",
                             COMPANION_STATE_STOPPED, 5000),
              COMPANION_STATE_STOPPED);
    ASSERT_EQ(companion_status_one(sup, "oneshot", &s), MDS_OK);
    ASSERT_EQ(s.last_exit_code, 0);
    ASSERT_EQ((int)s.restart_count, 0);

    companion_stop(sup);
}

static void test_sup_failure_restarts_then_gives_up(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    struct companion_status s;

    /* Always-failing child, small burst budget, tiny backoff. */
    base_decl(&cfg, "flapper", find_exec("false"));
    cfg.companions[0].restart              = COMPANION_RESTART_ON_FAILURE;
    cfg.companions[0].max_restarts         = 2;
    cfg.companions[0].restart_backoff_ms   = 100;
    cfg.companions[0].restart_backoff_max_ms = 100;
    /* Long reset window so the burst counter is not cleared. */
    cfg.companions[0].restart_reset_sec    = 3600;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);

    ASSERT_EQ(wait_for_restarts(sup, "flapper", 2, 15000), 2);
    ASSERT_EQ(wait_for_state(sup, "flapper",
                             COMPANION_STATE_FAILED, 15000),
              COMPANION_STATE_FAILED);

    ASSERT_EQ(companion_status_one(sup, "flapper", &s), MDS_OK);
    ASSERT_TRUE(s.last_exit_code != 0);
    ASSERT_EQ((int)s.restart_count, 2);

    /* An explicit start clears the burst so operators can retry. */
    ASSERT_EQ(companion_ctl_start(sup, "flapper"), MDS_OK);
    ASSERT_EQ(companion_status_one(sup, "flapper", &s), MDS_OK);
    ASSERT_TRUE(s.restart_count <= 1);

    companion_stop(sup);
}

static void test_sup_never_policy_leaves_failed(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    struct companion_status s;

    base_decl(&cfg, "nofix", find_exec("false"));
    cfg.companions[0].restart = COMPANION_RESTART_NEVER;
    cfg.companions[0].max_restarts = 5;   /* Policy still wins. */

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_EQ(wait_for_state(sup, "nofix",
                             COMPANION_STATE_FAILED, 5000),
              COMPANION_STATE_FAILED);
    ASSERT_EQ(companion_status_one(sup, "nofix", &s), MDS_OK);
    ASSERT_EQ((int)s.restart_count, 0);

    companion_stop(sup);
}

static void test_sup_exec_failure_reported(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    struct companion_status s;

    /* Absolute but nonexistent: the child exits 127 after execv. */
    base_decl(&cfg, "ghost", "/nonexistent/companion-binary");
    cfg.companions[0].restart = COMPANION_RESTART_NEVER;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_EQ(wait_for_state(sup, "ghost",
                             COMPANION_STATE_FAILED, 5000),
              COMPANION_STATE_FAILED);
    ASSERT_EQ(companion_status_one(sup, "ghost", &s), MDS_OK);
    ASSERT_EQ(s.last_exit_code, 127);

    companion_stop(sup);
}

static void test_sup_unknown_name(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    struct companion_status s;

    base_decl(&cfg, "known", find_exec("sleep"));
    (void)snprintf(cfg.companions[0].argv[0],
                   MDS_COMPANION_ARG_MAX, "60");
    cfg.companions[0].argc = 1;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);

    ASSERT_EQ(companion_ctl_start(sup, "absent"), MDS_ERR_NOTFOUND);
    ASSERT_EQ(companion_ctl_stop(sup, "absent"), MDS_ERR_NOTFOUND);
    ASSERT_EQ(companion_ctl_restart(sup, "absent"), MDS_ERR_NOTFOUND);
    ASSERT_EQ(companion_status_one(sup, "absent", &s), MDS_ERR_NOTFOUND);

    companion_stop(sup);
}

static void test_sup_stop_kills_stubborn_child(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    char script[128];

    /*
     * A child that ignores SIGTERM must still be torn down: the
     * supervisor escalates to SIGKILL after stop_timeout_ms.
     */
    (void)snprintf(script, sizeof(script),
                   "/tmp/pnfs-companion-stubborn-%d.sh", (int)getpid());
    write_tmp_file(script,
        "#!/bin/sh\n"
        "trap '' TERM\n"
        "while true; do sleep 0.2; done\n", 0700);

    base_decl(&cfg, "stubborn", script);
    cfg.companions[0].stop_timeout_ms = 300;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_EQ(wait_for_state(sup, "stubborn",
                             COMPANION_STATE_RUNNING, 5000),
              COMPANION_STATE_RUNNING);

    ASSERT_EQ(companion_ctl_stop(sup, "stubborn"), MDS_OK);
    ASSERT_EQ(wait_for_state(sup, "stubborn",
                             COMPANION_STATE_STOPPED, 10000),
              COMPANION_STATE_STOPPED);

    /* companion_stop must also return promptly with nothing running. */
    companion_stop(sup);
    (void)unlink(script);
}

static void test_sup_shutdown_terminates_children(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    struct companion_status s;
    pid_t child;

    base_decl(&cfg, "longrun", find_exec("sleep"));
    (void)snprintf(cfg.companions[0].argv[0],
                   MDS_COMPANION_ARG_MAX, "300");
    cfg.companions[0].argc = 1;
    cfg.companions[0].stop_timeout_ms = 500;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_EQ(wait_for_state(sup, "longrun",
                             COMPANION_STATE_RUNNING, 5000),
              COMPANION_STATE_RUNNING);
    ASSERT_EQ(companion_status_one(sup, "longrun", &s), MDS_OK);
    child = (pid_t)s.pid;
    ASSERT_TRUE(child > 0);

    companion_stop(sup);

    /*
     * The child was reaped by companion_stop, so its pid is no longer
     * ours; kill(pid, 0) must fail with ESRCH once it is gone.  Allow
     * a brief settle window for the kernel to release it.
     */
    {
        uint64_t deadline = test_now_ms() + 5000;
        bool gone = false;

        while (test_now_ms() < deadline) {
            if (kill(child, 0) != 0) {
                gone = true;
                break;
            }
            usleep(20 * 1000);
        }
        ASSERT_TRUE(gone);
    }
}

/* -----------------------------------------------------------------------
 * NDB API slot budget
 * ----------------------------------------------------------------------- */

static void test_budget_arithmetic(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    struct companion_budget b;

    base_decl(&cfg, "hungry", find_exec("sleep"));
    (void)snprintf(cfg.companions[0].argv[0],
                   MDS_COMPANION_ARG_MAX, "60");
    cfg.companions[0].argc = 1;
    cfg.companions[0].ndb_conns = 3;
    cfg.ndb_conn_pool_size    = 4;   /* slots_mds */
    cfg.ndb_api_slots_total   = 20;
    cfg.ndb_api_slots_reserved = 2;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_EQ(wait_for_state(sup, "hungry",
                             COMPANION_STATE_RUNNING, 5000),
              COMPANION_STATE_RUNNING);

    ASSERT_EQ(companion_budget(sup, &b), MDS_OK);
    ASSERT_EQ((int)b.slots_total, 20);
    ASSERT_EQ((int)b.slots_mds, 4);
    ASSERT_EQ((int)b.slots_reserved, 2);
    ASSERT_EQ((int)b.slots_declared, 3);
    ASSERT_EQ((int)b.slots_running, 3);
    ASSERT_EQ((int)b.companion_count, 1);
    ASSERT_EQ((int)b.running_count, 1);
    /* 20 - 4 - 2 - 3 = 11 */
    ASSERT_EQ((int)b.slots_free, 11);

    /* Stopping it releases the running slots but not the declaration. */
    ASSERT_EQ(companion_ctl_stop(sup, "hungry"), MDS_OK);
    ASSERT_EQ(wait_for_state(sup, "hungry",
                             COMPANION_STATE_STOPPED, 5000),
              COMPANION_STATE_STOPPED);
    ASSERT_EQ(companion_budget(sup, &b), MDS_OK);
    ASSERT_EQ((int)b.slots_running, 0);
    ASSERT_EQ((int)b.slots_declared, 3);
    ASSERT_EQ((int)b.slots_free, 14);

    companion_stop(sup);
}

static void test_budget_undeclared_total(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    struct companion_budget b;

    base_decl(&cfg, "quiet", find_exec("sleep"));
    (void)snprintf(cfg.companions[0].argv[0],
                   MDS_COMPANION_ARG_MAX, "60");
    cfg.companions[0].argc = 1;
    cfg.companions[0].ndb_conns = 2;
    cfg.ndb_api_slots_total = 0;   /* Undeclared. */

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_EQ(companion_budget(sup, &b), MDS_OK);
    ASSERT_EQ((int)b.slots_total, 0);
    /* No total means no meaningful free figure. */
    ASSERT_EQ((int)b.slots_free, 0);

    companion_stop(sup);
}

static void test_budget_oversubscription_is_negative(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    struct companion_budget b;

    /* Deliberately over-committed: free must go negative, not wrap. */
    base_decl(&cfg, "greedy", find_exec("sleep"));
    (void)snprintf(cfg.companions[0].argv[0],
                   MDS_COMPANION_ARG_MAX, "60");
    cfg.companions[0].argc = 1;
    cfg.companions[0].ndb_conns = 8;
    cfg.ndb_conn_pool_size     = 4;
    cfg.ndb_api_slots_total    = 6;
    cfg.ndb_api_slots_reserved = 1;
    cfg.companion_ndb_admission = COMPANION_NDB_ADVISORY;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    /* Advisory mode warns but still starts the program. */
    ASSERT_EQ(wait_for_state(sup, "greedy",
                             COMPANION_STATE_RUNNING, 5000),
              COMPANION_STATE_RUNNING);

    ASSERT_EQ(companion_budget(sup, &b), MDS_OK);
    /* 6 - 4 - 1 - 8 = -7 */
    ASSERT_EQ((int)b.slots_free, -7);
    ASSERT_TRUE(b.slots_free < 0);

    companion_stop(sup);
}

static void test_budget_enforce_refuses_start(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;
    struct companion_status s;

    base_decl(&cfg, "blocked", find_exec("sleep"));
    (void)snprintf(cfg.companions[0].argv[0],
                   MDS_COMPANION_ARG_MAX, "60");
    cfg.companions[0].argc = 1;
    cfg.companions[0].ndb_conns = 8;
    cfg.companions[0].autostart = false;  /* Start it explicitly. */
    cfg.ndb_conn_pool_size     = 4;
    cfg.ndb_api_slots_total    = 6;
    cfg.ndb_api_slots_reserved = 1;
    cfg.companion_ndb_admission = COMPANION_NDB_ENFORCE;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_TRUE(sup != NULL);

    /* Enforce mode refuses rather than oversubscribing the cluster. */
    ASSERT_EQ(companion_ctl_start(sup, "blocked"), MDS_ERR_NOSPC);
    ASSERT_EQ(companion_status_one(sup, "blocked", &s), MDS_OK);
    ASSERT_EQ(s.pid, 0);

    companion_stop(sup);
}

static void test_budget_enforce_allows_within_budget(void)
{
    struct mds_config cfg;
    struct companion_supervisor *sup = NULL;

    base_decl(&cfg, "fits", find_exec("sleep"));
    (void)snprintf(cfg.companions[0].argv[0],
                   MDS_COMPANION_ARG_MAX, "60");
    cfg.companions[0].argc = 1;
    cfg.companions[0].ndb_conns = 2;
    cfg.companions[0].autostart = false;
    cfg.ndb_conn_pool_size     = 4;
    cfg.ndb_api_slots_total    = 48;
    cfg.ndb_api_slots_reserved = 8;
    cfg.companion_ndb_admission = COMPANION_NDB_ENFORCE;

    ASSERT_EQ(companion_start(&cfg, &sup), 0);
    ASSERT_EQ(companion_ctl_start(sup, "fits"), MDS_OK);
    ASSERT_EQ(wait_for_state(sup, "fits",
                             COMPANION_STATE_RUNNING, 5000),
              COMPANION_STATE_RUNNING);

    companion_stop(sup);
}

/* -----------------------------------------------------------------------
 * Entry point
 * ----------------------------------------------------------------------- */

int main(void)
{
    fprintf(stdout, "companion supervisor tests\n");

    /* Config parsing. */
    RUN_TEST(test_cfg_minimal_declaration);
    RUN_TEST(test_cfg_full_declaration);
    RUN_TEST(test_cfg_multiple_and_ordering);
    RUN_TEST(test_cfg_relative_exec_rejected);
    RUN_TEST(test_cfg_missing_exec_rejected);
    RUN_TEST(test_cfg_relative_paths_rejected);
    RUN_TEST(test_cfg_scope_requires_memory_max);
    RUN_TEST(test_cfg_arg_hole_rejected);
    RUN_TEST(test_cfg_disabled_declaration_kept);
    RUN_TEST(test_cfg_out_of_range_keeps_default);
    RUN_TEST(test_cfg_invalid_name_ignored);
    RUN_TEST(test_cfg_backoff_cap_clamped);

    /* Supervision. */
    RUN_TEST(test_sup_inert_when_nothing_declared);
    RUN_TEST(test_sup_master_switch_off);
    RUN_TEST(test_sup_disabled_declaration_reports_disabled);
    RUN_TEST(test_sup_spawn_and_stop);
    RUN_TEST(test_sup_clean_exit_not_restarted);
    RUN_TEST(test_sup_failure_restarts_then_gives_up);
    RUN_TEST(test_sup_never_policy_leaves_failed);
    RUN_TEST(test_sup_exec_failure_reported);
    RUN_TEST(test_sup_unknown_name);
    RUN_TEST(test_sup_stop_kills_stubborn_child);
    RUN_TEST(test_sup_shutdown_terminates_children);

    /* Budget. */
    RUN_TEST(test_budget_arithmetic);
    RUN_TEST(test_budget_undeclared_total);
    RUN_TEST(test_budget_oversubscription_is_negative);
    RUN_TEST(test_budget_enforce_refuses_start);
    RUN_TEST(test_budget_enforce_allows_within_budget);

    fprintf(stdout, "%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}

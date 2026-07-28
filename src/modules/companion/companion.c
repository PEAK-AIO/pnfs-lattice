/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * companion.c -- Supervisor for operator-declared helper processes.
 *
 * See include/companion.h for the API contract and trust model.
 *
 * Why fork()+execv() rather than posix_spawn(): the child must call
 * setrlimit() before exec, and posix_spawn offers no hook to run code
 * in the child.  The cost of that choice is that this file must obey
 * the post-fork rules of a multithreaded process -- between fork() and
 * execv() the child may call only async-signal-safe functions, because
 * any other thread's malloc arena or stdio lock may have been held at
 * the moment of the fork.  Every string, argv slot, and numeric bound
 * the child needs is therefore prepared by the parent before forking,
 * and the child body below is nothing but syscalls.  Do not add
 * logging, allocation, or snprintf to child_exec().
 */

#include "companion.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "mds_log.h"

/* -----------------------------------------------------------------------
 * Tunables that are not operator-visible
 * ----------------------------------------------------------------------- */

/** Supervisor wake-up cadence when nothing has a nearer deadline. */
#define COMPANION_TICK_MS         200

/**
 * argv slots needed in the worst case: the systemd-run wrapper prefix
 * (8) + argv[0] (1) + declared args + NULL terminator.
 */
#define COMPANION_SPAWN_ARGV_MAX  (8 + 1 + MDS_COMPANION_ARGV_MAX + 1)

/** Upper bound for the child's descriptor-closing loop. */
#define COMPANION_FD_CLOSE_CAP    65536

/** Child exit codes reserved for pre-exec setup failures. */
#define COMPANION_EXIT_SETUP      126
#define COMPANION_EXIT_EXEC       127

/** Candidate systemd-run locations, probed once at start. */
static const char *const g_systemd_run_paths[] = {
    "/usr/bin/systemd-run",
    "/bin/systemd-run",
};

/* -----------------------------------------------------------------------
 * Internal state
 * ----------------------------------------------------------------------- */

struct companion_entry {
    /** Immutable copy of the operator's declaration. */
    struct mds_companion_decl decl;

    /* Everything below is guarded by companion_supervisor::lock. */
    enum companion_state state;
    pid_t    pid;                 /**< 0 when not running. */
    uint32_t restart_count;
    int      last_exit_code;      /**< -1 when it never exited normally. */
    int      last_term_signal;    /**< 0 when never signalled. */
    time_t   started_at;          /**< Wall clock, for reporting. */
    time_t   last_exit_at;
    uint64_t started_mono_ms;     /**< Monotonic, for uptime decisions. */
    uint64_t next_spawn_mono_ms;  /**< Deadline for STARTING / BACKOFF. */
    uint64_t stop_deadline_mono_ms; /**< SIGKILL escalation deadline. */
    uint32_t backoff_ms;          /**< Next backoff to apply. */
    bool     stop_requested;      /**< Deliberate stop: skip restart. */
    bool     restart_pending;     /**< Respawn once the stop completes. */
};

struct companion_supervisor {
    struct companion_entry entries[MDS_MAX_COMPANIONS];
    uint32_t count;

    /* Budget inputs, copied at start and never mutated. */
    uint32_t slots_total;
    uint32_t slots_reserved;
    uint32_t slots_mds;
    enum companion_ndb_admission admission;

    pthread_mutex_t lock;
    pthread_t       thread;
    bool            thread_started;
    atomic_bool     running;

    /** Self-pipe: [0] read end polled by the thread, [1] written to wake it. */
    int             wake_pipe[2];

    /** Precomputed bound for the child's fd-closing loop. */
    int             fd_close_max;

    /** Resolved systemd-run path, or empty when unavailable. */
    char            systemd_run_path[MDS_COMPANION_PATH_MAX];
};

/* -----------------------------------------------------------------------
 * Small helpers
 * ----------------------------------------------------------------------- */

/** Monotonic milliseconds; 0 only if the clock is unavailable. */
static uint64_t mono_ms(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000U +
           (uint64_t)(ts.tv_nsec / 1000000L);
}

/** Bounded strerror wrapper; always use the returned pointer. */
static const char *comp_strerror(int err, char *buf, size_t cap)
{
    return strerror_r(err, buf, cap);
}

/** Nudge the supervisor thread out of poll(). */
static void companion_wake(const struct companion_supervisor *sup)
{
    const uint8_t byte = 1;
    ssize_t w;

    if (sup->wake_pipe[1] < 0) {
        return;
    }
    /*
     * The write end is non-blocking.  EAGAIN means the pipe already
     * holds an unread wake byte, which has exactly the effect we want,
     * so the result is deliberately discarded.
     */
    w = write(sup->wake_pipe[1], &byte, 1);
    (void)w;
}

/** Locate an entry by declared name.  Caller holds the lock. */
static struct companion_entry *
entry_by_name(struct companion_supervisor *sup, const char *name)
{
    if (name == NULL) {
        return NULL;
    }
    for (uint32_t i = 0; i < sup->count; i++) {
        if (strcmp(sup->entries[i].decl.name, name) == 0) {
            return &sup->entries[i];
        }
    }
    return NULL;
}

/** Resident set size of @a pid in KiB, or 0 when unavailable. */
static uint64_t read_rss_kb(pid_t pid)
{
    char path[64];
    FILE *f;
    unsigned long long total_pages = 0;
    unsigned long long rss_pages = 0;
    long page_kb;

    if (pid <= 0) {
        return 0;
    }
    if (snprintf(path, sizeof(path), "/proc/%d/statm", (int)pid) < 0) {
        return 0;
    }
    f = fopen(path, "re");
    if (f == NULL) {
        return 0;
    }
    if (fscanf(f, "%llu %llu", &total_pages, &rss_pages) != 2) {
        rss_pages = 0;
    }
    (void)fclose(f);

    page_kb = sysconf(_SC_PAGESIZE) / 1024;
    if (page_kb <= 0) {
        return 0;
    }
    return rss_pages * (uint64_t)page_kb;
}

/* -----------------------------------------------------------------------
 * Budget accounting
 * ----------------------------------------------------------------------- */

/** Fill @a out from current state.  Caller holds the lock. */
static void budget_compute_locked(const struct companion_supervisor *sup,
                                  struct companion_budget *out)
{
    memset(out, 0, sizeof(*out));
    out->slots_total    = sup->slots_total;
    out->slots_mds      = sup->slots_mds;
    out->slots_reserved = sup->slots_reserved;
    out->admission      = (uint8_t)sup->admission;

    for (uint32_t i = 0; i < sup->count; i++) {
        const struct companion_entry *e = &sup->entries[i];

        if (!e->decl.enabled) {
            continue;
        }
        out->companion_count++;
        out->slots_declared += e->decl.ndb_conns;
        if (e->state == COMPANION_STATE_RUNNING) {
            out->running_count++;
            out->slots_running += e->decl.ndb_conns;
        }
    }

    if (sup->slots_total > 0) {
        /*
         * Compute wide and signed: oversubscription must surface as a
         * negative number rather than wrapping around zero.
         */
        int64_t free_slots = (int64_t)sup->slots_total -
                             (int64_t)sup->slots_mds -
                             (int64_t)sup->slots_reserved -
                             (int64_t)out->slots_running;

        if (free_slots > INT32_MAX) {
            free_slots = INT32_MAX;
        } else if (free_slots < INT32_MIN) {
            free_slots = INT32_MIN;
        }
        out->slots_free = (int32_t)free_slots;
    }
}

/**
 * Apply admission control before a spawn.  Caller holds the lock.
 *
 * @return MDS_OK to proceed, MDS_ERR_NOSPC to refuse (enforce mode).
 */
static enum mds_status
admission_check_locked(const struct companion_supervisor *sup,
                       const struct companion_entry *e)
{
    struct companion_budget b;
    int64_t projected;

    if (sup->slots_total == 0 || e->decl.ndb_conns == 0) {
        return MDS_OK;  /* Nothing declared to account against. */
    }

    budget_compute_locked(sup, &b);
    projected = (int64_t)b.slots_mds + (int64_t)b.slots_reserved +
                (int64_t)b.slots_running + (int64_t)e->decl.ndb_conns;

    if (projected <= (int64_t)sup->slots_total) {
        return MDS_OK;
    }

    if (sup->admission == COMPANION_NDB_ENFORCE) {
        MDS_LOG_ERROR(LOG_COMP_MDS,
            "companion '%s': refusing to start -- would need %lld of %u "
            "declared NDB API slots (mds=%u reserved=%u running=%u); "
            "companion_ndb_admission=enforce",
            e->decl.name, (long long)projected,
            (unsigned)sup->slots_total, (unsigned)b.slots_mds,
            (unsigned)b.slots_reserved, (unsigned)b.slots_running);
        return MDS_ERR_NOSPC;
    }

    MDS_LOG_WARN(LOG_COMP_MDS,
        "companion '%s': starting over the declared NDB API slot budget "
        "(%lld of %u; mds=%u reserved=%u running=%u) -- "
        "companion_ndb_admission=advisory",
        e->decl.name, (long long)projected, (unsigned)sup->slots_total,
        (unsigned)b.slots_mds, (unsigned)b.slots_reserved,
        (unsigned)b.slots_running);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Spawn
 * ----------------------------------------------------------------------- */

/**
 * The post-fork child body.  Never returns.
 *
 * ONLY async-signal-safe calls are permitted here: this runs in a
 * forked child of a multithreaded process, so anything that takes a
 * libc lock (malloc, stdio, snprintf, syslog) may deadlock.  All
 * arguments are prepared by the caller before fork().
 */
static void child_exec(const struct companion_entry *e,
                       const char *exec_target,
                       const char *const *argv,
                       int fd_close_max)
{
    sigset_t empty;
    int fd;

    /*
     * Own process group.  Lets the supervisor signal the whole child
     * tree with kill(-pid, ...) and detaches the child from terminal
     * job-control signals aimed at the daemon.
     */
    (void)setpgid(0, 0);

    /*
     * The daemon blocks SIGINT and SIGTERM in every thread and relies
     * on sigwait() in main().  A child inheriting that mask would
     * ignore ordinary termination requests, so clear it.
     */
    sigemptyset(&empty);
    (void)sigprocmask(SIG_SETMASK, &empty, NULL);

    /* Restore default dispositions for anything the daemon changed. */
    for (int sig = 1; sig < NSIG; sig++) {
        if (sig == SIGKILL || sig == SIGSTOP) {
            continue;  /* Cannot be reset; skip rather than fail. */
        }
        (void)signal(sig, SIG_DFL);
    }

    /* stdin from /dev/null: companions are never interactive. */
    fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
        if (fd != STDIN_FILENO) {
            (void)dup2(fd, STDIN_FILENO);
            (void)close(fd);
        }
    } else {
        (void)close(STDIN_FILENO);
    }

    /*
     * stdout+stderr to the declared log file, or inherited when the
     * operator did not declare one (which lands in the journal).
     */
    if (e->decl.log_file[0] != '\0') {
        fd = open(e->decl.log_file,
                  O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0640);
        if (fd < 0) {
            _exit(COMPANION_EXIT_SETUP);
        }
        if (dup2(fd, STDOUT_FILENO) < 0 || dup2(fd, STDERR_FILENO) < 0) {
            _exit(COMPANION_EXIT_SETUP);
        }
        if (fd != STDOUT_FILENO && fd != STDERR_FILENO) {
            (void)close(fd);
        }
    }

    /*
     * Close every inherited descriptor above stderr so the child
     * cannot hold the catalogue connections, listening sockets, TLS
     * state, or the daemon log open.  A bounded loop keeps this
     * async-signal-safe; scanning /proc/self/fd would need opendir.
     */
    for (int i = 3; i < fd_close_max; i++) {
        (void)close(i);
    }

    if (chdir(e->decl.workdir[0] != '\0' ? e->decl.workdir : "/") != 0) {
        _exit(COMPANION_EXIT_SETUP);
    }

    if (e->decl.rlimit_as_mb > 0) {
        struct rlimit rl;

        rl.rlim_cur = (rlim_t)e->decl.rlimit_as_mb * 1024U * 1024U;
        rl.rlim_max = rl.rlim_cur;
        (void)setrlimit(RLIMIT_AS, &rl);
    }
    if (e->decl.rlimit_nofile > 0) {
        struct rlimit rl;

        rl.rlim_cur = (rlim_t)e->decl.rlimit_nofile;
        rl.rlim_max = rl.rlim_cur;
        (void)setrlimit(RLIMIT_NOFILE, &rl);
    }
    if (e->decl.rlimit_cpu_sec > 0) {
        struct rlimit rl;

        rl.rlim_cur = (rlim_t)e->decl.rlimit_cpu_sec;
        rl.rlim_max = rl.rlim_cur;
        (void)setrlimit(RLIMIT_CPU, &rl);
    }

    /*
     * execv, never execvp: the path is absolute and PATH must not
     * influence which program runs.  The cast drops const only because
     * the POSIX prototype predates const-correctness; execv does not
     * modify the vector.
     */
    (void)execv(exec_target, (char *const *)argv);
    _exit(COMPANION_EXIT_EXEC);
}

/**
 * Fork and exec one companion.  Caller holds the lock.
 *
 * @return MDS_OK on success, MDS_ERR_NOSPC when admission refused,
 *         MDS_ERR_IO when fork() failed.
 */
static enum mds_status spawn_locked(struct companion_supervisor *sup,
                                    struct companion_entry *e)
{
    const char *argv[COMPANION_SPAWN_ARGV_MAX];
    const char *exec_target;
    /* Initialised so the trailing log statement is well-defined even
     * when no systemd scope is in play. */
    char mem_arg[64] = "";
    char unit_arg[128] = "";
    char ebuf[96];
    uint32_t n = 0;
    bool use_scope;
    pid_t pid;
    enum mds_status st;

    st = admission_check_locked(sup, e);
    if (st != MDS_OK) {
        e->state = COMPANION_STATE_FAILED;
        return st;
    }

    use_scope = e->decl.systemd_scope && sup->systemd_run_path[0] != '\0';
    if (e->decl.systemd_scope && !use_scope) {
        MDS_LOG_WARN(LOG_COMP_MDS,
            "companion '%s': systemd_scope requested but systemd-run was "
            "not found; starting without a memory scope",
            e->decl.name);
    }

    /*
     * Build the whole argv here, in the parent.  The child may not
     * call snprintf, so every string it dereferences must already
     * exist -- these stack buffers stay valid in the child through
     * copy-on-write until execv replaces the image.
     */
    if (use_scope) {
        if (snprintf(mem_arg, sizeof(mem_arg), "MemoryMax=%uM",
                     (unsigned)e->decl.memory_max_mb) < 0 ||
            snprintf(unit_arg, sizeof(unit_arg),
                     "pnfs-companion-%s.scope", e->decl.name) < 0) {
            MDS_LOG_ERROR(LOG_COMP_MDS,
                "companion '%s': failed to format systemd scope args",
                e->decl.name);
            e->state = COMPANION_STATE_FAILED;
            return MDS_ERR_IO;
        }
        argv[n++] = sup->systemd_run_path;
        argv[n++] = "--scope";
        argv[n++] = "--collect";
        argv[n++] = "-p";
        argv[n++] = mem_arg;
        argv[n++] = "-u";
        argv[n++] = unit_arg;
        argv[n++] = "--";
        exec_target = sup->systemd_run_path;
    } else {
        exec_target = e->decl.exec_path;
    }

    argv[n++] = e->decl.exec_path;   /* argv[0] of the companion. */
    for (uint32_t i = 0; i < e->decl.argc &&
                         i < MDS_COMPANION_ARGV_MAX; i++) {
        argv[n++] = e->decl.argv[i];
    }
    argv[n] = NULL;

    pid = fork();
    if (pid < 0) {
        MDS_LOG_ERROR(LOG_COMP_MDS,
            "companion '%s': fork failed: %s", e->decl.name,
            comp_strerror(errno, ebuf, sizeof(ebuf)));
        e->state = COMPANION_STATE_FAILED;
        return MDS_ERR_IO;
    }
    if (pid == 0) {
        child_exec(e, exec_target, argv, sup->fd_close_max);
        _exit(COMPANION_EXIT_EXEC);  /* Unreachable. */
    }

    /*
     * Parent.  setpgid is also done here to close the race where the
     * supervisor signals the group before the child reaches its own
     * setpgid call; one of the two always wins and EACCES/ESRCH from
     * the loser is harmless.
     */
    (void)setpgid(pid, pid);

    e->pid             = pid;
    e->state           = COMPANION_STATE_RUNNING;
    e->started_at      = time(NULL);
    e->started_mono_ms = mono_ms();
    e->stop_requested  = false;

    MDS_LOG_INFO(LOG_COMP_MDS,
        "companion '%s': started pid=%d exec=%s%s%s",
        e->decl.name, (int)pid, e->decl.exec_path,
        use_scope ? " scope=" : "", use_scope ? unit_arg : "");
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Exit handling
 * ----------------------------------------------------------------------- */

/** Decide whether @a e should be restarted after a given exit. */
static bool should_restart(const struct companion_entry *e, bool failed)
{
    switch (e->decl.restart) {
    case COMPANION_RESTART_ALWAYS:
        return true;
    case COMPANION_RESTART_ON_FAILURE:
        return failed;
    case COMPANION_RESTART_NEVER:
    default:
        return false;
    }
}

/**
 * Record an exit and apply the restart policy.  Caller holds the lock.
 */
static void handle_exit_locked(struct companion_entry *e, int status)
{
    uint64_t now = mono_ms();
    uint64_t uptime_ms = (now > e->started_mono_ms)
                       ? (now - e->started_mono_ms) : 0;
    bool failed;

    if (WIFEXITED(status)) {
        e->last_exit_code   = WEXITSTATUS(status);
        e->last_term_signal = 0;
        failed = (e->last_exit_code != 0);
    } else if (WIFSIGNALED(status)) {
        e->last_exit_code   = -1;
        e->last_term_signal = WTERMSIG(status);
        failed = true;
    } else {
        e->last_exit_code   = -1;
        e->last_term_signal = 0;
        failed = true;
    }

    e->last_exit_at = time(NULL);
    e->pid = 0;

    if (e->last_exit_code == COMPANION_EXIT_EXEC) {
        MDS_LOG_ERROR(LOG_COMP_MDS,
            "companion '%s': exec of '%s' failed in the child",
            e->decl.name, e->decl.exec_path);
    } else if (e->last_exit_code == COMPANION_EXIT_SETUP) {
        MDS_LOG_ERROR(LOG_COMP_MDS,
            "companion '%s': child setup failed (workdir '%s' or "
            "log_file '%s' not usable)", e->decl.name,
            e->decl.workdir, e->decl.log_file);
    }

    /* A deliberate stop never triggers the restart policy. */
    if (e->stop_requested) {
        e->stop_requested = false;
        if (e->restart_pending) {
            e->restart_pending    = false;
            e->restart_count      = 0;
            e->backoff_ms         = e->decl.restart_backoff_ms;
            e->state              = COMPANION_STATE_STARTING;
            e->next_spawn_mono_ms = now;
        } else {
            e->state = COMPANION_STATE_STOPPED;
            MDS_LOG_INFO(LOG_COMP_MDS,
                "companion '%s': stopped", e->decl.name);
        }
        return;
    }

    /* A run that stayed up long enough clears the failure burst. */
    if (uptime_ms >= (uint64_t)e->decl.restart_reset_sec * 1000U) {
        e->restart_count = 0;
        e->backoff_ms    = e->decl.restart_backoff_ms;
    }

    if (!should_restart(e, failed)) {
        e->state = failed ? COMPANION_STATE_FAILED
                          : COMPANION_STATE_STOPPED;
        MDS_LOG_INFO(LOG_COMP_MDS,
            "companion '%s': exited (code=%d signal=%d); "
            "restart policy leaves it %s",
            e->decl.name, e->last_exit_code, e->last_term_signal,
            failed ? "failed" : "stopped");
        return;
    }

    if (e->restart_count >= e->decl.max_restarts) {
        e->state = failed ? COMPANION_STATE_FAILED
                          : COMPANION_STATE_STOPPED;
        MDS_LOG_ERROR(LOG_COMP_MDS,
            "companion '%s': exited (code=%d signal=%d) and exhausted "
            "its restart budget (%u); leaving it %s -- "
            "`mds-admin companion start %s` to retry",
            e->decl.name, e->last_exit_code, e->last_term_signal,
            (unsigned)e->decl.max_restarts,
            failed ? "failed" : "stopped", e->decl.name);
        return;
    }

    if (e->backoff_ms == 0) {
        e->backoff_ms = e->decl.restart_backoff_ms;
    }
    e->restart_count++;
    e->state              = COMPANION_STATE_BACKOFF;
    e->next_spawn_mono_ms = now + e->backoff_ms;

    MDS_LOG_WARN(LOG_COMP_MDS,
        "companion '%s': exited (code=%d signal=%d); restart %u/%u in "
        "%u ms", e->decl.name, e->last_exit_code, e->last_term_signal,
        (unsigned)e->restart_count, (unsigned)e->decl.max_restarts,
        (unsigned)e->backoff_ms);

    /* Exponential backoff, capped. */
    if (e->backoff_ms < e->decl.restart_backoff_max_ms) {
        uint64_t next = (uint64_t)e->backoff_ms * 2U;

        if (next > e->decl.restart_backoff_max_ms) {
            next = e->decl.restart_backoff_max_ms;
        }
        e->backoff_ms = (uint32_t)next;
    }
}

/**
 * Reap @a e if its child has exited.  Caller holds the lock.
 *
 * Only ever waits on a pid this module created, so it cannot consume
 * an exit status belonging to some other part of the daemon.
 */
static void reap_locked(struct companion_entry *e)
{
    int status = 0;
    pid_t r;

    if (e->pid <= 0) {
        return;
    }
    r = waitpid(e->pid, &status, WNOHANG);
    if (r == 0) {
        return;  /* Still running. */
    }
    if (r < 0) {
        if (errno == EINTR) {
            return;
        }
        /* ECHILD: already gone.  Synthesise an unknown-cause exit. */
        status = 0;
    }
    handle_exit_locked(e, status);
}

/**
 * Signal a running child's whole process group.
 *
 * Caller must have exclusive access to @a e: either holding the lock,
 * or running after the supervisor thread has been joined.
 */
static void signal_child_group(const struct companion_entry *e, int sig)
{
    if (e->pid <= 0) {
        return;
    }
    /*
     * Negative pid targets the process group created at spawn, so
     * helpers that fork their own workers are torn down as a unit.
     * Fall back to the bare pid if the group is already gone.
     */
    if (kill(-e->pid, sig) != 0 && errno == ESRCH) {
        (void)kill(e->pid, sig);
    }
}

/* -----------------------------------------------------------------------
 * Supervisor thread
 * ----------------------------------------------------------------------- */

/**
 * One scheduling pass.  Caller holds the lock.
 *
 * @return Milliseconds until the next deadline, capped at the tick.
 */
static int supervise_pass_locked(struct companion_supervisor *sup)
{
    uint64_t now = mono_ms();
    uint64_t nearest = now + COMPANION_TICK_MS;

    for (uint32_t i = 0; i < sup->count; i++) {
        struct companion_entry *e = &sup->entries[i];

        if (e->state == COMPANION_STATE_RUNNING) {
            reap_locked(e);
        }

        /* Escalate a stop that outlived its grace period. */
        if (e->state == COMPANION_STATE_RUNNING && e->stop_requested) {
            if (now >= e->stop_deadline_mono_ms) {
                MDS_LOG_WARN(LOG_COMP_MDS,
                    "companion '%s': pid %d ignored SIGTERM for %u ms; "
                    "sending SIGKILL", e->decl.name, (int)e->pid,
                    (unsigned)e->decl.stop_timeout_ms);
                signal_child_group(e, SIGKILL);
                /* Reaped on a later pass. */
            } else if (e->stop_deadline_mono_ms < nearest) {
                nearest = e->stop_deadline_mono_ms;
            }
            continue;
        }

        if (e->state == COMPANION_STATE_STARTING ||
            e->state == COMPANION_STATE_BACKOFF) {
            if (now >= e->next_spawn_mono_ms) {
                (void)spawn_locked(sup, e);
            } else if (e->next_spawn_mono_ms < nearest) {
                nearest = e->next_spawn_mono_ms;
            }
        }
    }

    if (nearest <= now) {
        return 0;
    }
    if (nearest - now > COMPANION_TICK_MS) {
        return COMPANION_TICK_MS;
    }
    return (int)(nearest - now);
}

static void *supervisor_thread(void *arg)
{
    struct companion_supervisor *sup = arg;

    while (atomic_load(&sup->running)) {
        struct pollfd pfd;
        int timeout_ms;

        pthread_mutex_lock(&sup->lock);
        timeout_ms = supervise_pass_locked(sup);
        pthread_mutex_unlock(&sup->lock);

        pfd.fd      = sup->wake_pipe[0];
        pfd.events  = POLLIN;
        pfd.revents = 0;

        if (poll(&pfd, 1, timeout_ms) > 0 && (pfd.revents & POLLIN) != 0) {
            uint8_t drain[64];

            /* Drain the self-pipe; one wake covers any number of pokes. */
            while (read(sup->wake_pipe[0], drain, sizeof(drain)) > 0) {
                /* keep draining */
            }
        }
    }
    return NULL;
}

/* -----------------------------------------------------------------------
 * Start / stop
 * ----------------------------------------------------------------------- */

/** Probe for systemd-run once so spawns do not stat() repeatedly. */
static void resolve_systemd_run(struct companion_supervisor *sup)
{
    const size_t n = sizeof(g_systemd_run_paths) /
                     sizeof(g_systemd_run_paths[0]);

    sup->systemd_run_path[0] = '\0';
    for (size_t i = 0; i < n; i++) {
        if (access(g_systemd_run_paths[i], X_OK) == 0) {
            (void)snprintf(sup->systemd_run_path,
                           sizeof(sup->systemd_run_path), "%s",
                           g_systemd_run_paths[i]);
            return;
        }
    }
}

/** Compute the bound for the child's descriptor-closing loop. */
static int resolve_fd_close_max(void)
{
    struct rlimit rl;
    long v = 0;

    if (getrlimit(RLIMIT_NOFILE, &rl) == 0 &&
        rl.rlim_cur != RLIM_INFINITY) {
        v = (long)rl.rlim_cur;
    }
    if (v <= 0) {
        v = sysconf(_SC_OPEN_MAX);
    }
    if (v <= 3) {
        v = 1024;
    }
    if (v > COMPANION_FD_CLOSE_CAP) {
        v = COMPANION_FD_CLOSE_CAP;
    }
    return (int)v;
}

int companion_start(const struct mds_config *cfg,
                    struct companion_supervisor **out)
{
    struct companion_supervisor *sup;
    char ebuf[96];

    if (out == NULL) {
        return -1;
    }
    *out = NULL;

    /* Inert cases: nothing to supervise, so allocate nothing. */
    if (cfg == NULL || !cfg->companion_enabled ||
        cfg->companion_count == 0) {
        if (cfg != NULL && !cfg->companion_enabled &&
            cfg->companion_count > 0) {
            MDS_LOG_INFO(LOG_COMP_MDS,
                "companion: %u declaration(s) present but "
                "companion_enabled=false; starting none",
                (unsigned)cfg->companion_count);
        }
        return 0;
    }

    sup = calloc(1, sizeof(*sup));
    if (sup == NULL) {
        MDS_LOG_ERROR(LOG_COMP_MDS,
            "companion: out of memory allocating supervisor");
        return -1;
    }

    if (pthread_mutex_init(&sup->lock, NULL) != 0) {
        free(sup);
        return -1;
    }

    sup->wake_pipe[0] = -1;
    sup->wake_pipe[1] = -1;
    if (pipe(sup->wake_pipe) != 0) {
        MDS_LOG_ERROR(LOG_COMP_MDS,
            "companion: pipe failed: %s",
            comp_strerror(errno, ebuf, sizeof(ebuf)));
        pthread_mutex_destroy(&sup->lock);
        free(sup);
        return -1;
    }
    /*
     * Both ends close-on-exec (children must not inherit them) and the
     * write end non-blocking so companion_wake() can never stall a
     * caller on a full pipe.
     */
    (void)fcntl(sup->wake_pipe[0], F_SETFD, FD_CLOEXEC);
    (void)fcntl(sup->wake_pipe[1], F_SETFD, FD_CLOEXEC);
    (void)fcntl(sup->wake_pipe[0], F_SETFL, O_NONBLOCK);
    (void)fcntl(sup->wake_pipe[1], F_SETFL, O_NONBLOCK);

    sup->slots_total    = cfg->ndb_api_slots_total;
    sup->slots_reserved = cfg->ndb_api_slots_reserved;
    sup->slots_mds      = cfg->ndb_conn_pool_size;
    sup->admission      = cfg->companion_ndb_admission;
    sup->fd_close_max   = resolve_fd_close_max();
    resolve_systemd_run(sup);

    sup->count = cfg->companion_count;
    if (sup->count > MDS_MAX_COMPANIONS) {
        sup->count = MDS_MAX_COMPANIONS;
    }
    for (uint32_t i = 0; i < sup->count; i++) {
        struct companion_entry *e = &sup->entries[i];
        uint64_t now = mono_ms();

        e->decl             = cfg->companions[i];
        e->pid              = 0;
        e->last_exit_code   = -1;
        e->last_term_signal = 0;
        e->backoff_ms       = e->decl.restart_backoff_ms;

        if (!e->decl.enabled) {
            e->state = COMPANION_STATE_DISABLED;
            continue;
        }
        if (!e->decl.autostart) {
            e->state = COMPANION_STATE_STOPPED;
            continue;
        }
        e->state              = COMPANION_STATE_STARTING;
        e->next_spawn_mono_ms = now + e->decl.start_delay_ms;
    }

    atomic_store(&sup->running, true);
    if (pthread_create(&sup->thread, NULL, supervisor_thread, sup) != 0) {
        MDS_LOG_ERROR(LOG_COMP_MDS,
            "companion: pthread_create failed: %s",
            comp_strerror(errno, ebuf, sizeof(ebuf)));
        atomic_store(&sup->running, false);
        (void)close(sup->wake_pipe[0]);
        (void)close(sup->wake_pipe[1]);
        pthread_mutex_destroy(&sup->lock);
        free(sup);
        return -1;
    }
    sup->thread_started = true;

    MDS_LOG_INFO(LOG_COMP_MDS,
        "companion supervisor active (%u declaration(s), "
        "api_slots_total=%u reserved=%u mds_pool=%u admission=%s%s)",
        (unsigned)sup->count, (unsigned)sup->slots_total,
        (unsigned)sup->slots_reserved, (unsigned)sup->slots_mds,
        sup->admission == COMPANION_NDB_ENFORCE ? "enforce" : "advisory",
        sup->systemd_run_path[0] != '\0' ? "" : ", no systemd-run");

    *out = sup;
    return 0;
}

/**
 * Terminate every remaining child.  Runs after the supervisor thread
 * has been joined, so no locking is required and blocking waits are
 * safe.
 */
static void terminate_all(struct companion_supervisor *sup)
{
    uint64_t deadline = mono_ms();
    bool any = false;

    for (uint32_t i = 0; i < sup->count; i++) {
        struct companion_entry *e = &sup->entries[i];

        if (e->pid <= 0) {
            continue;
        }
        any = true;
        signal_child_group(e, SIGTERM);
        if (mono_ms() + e->decl.stop_timeout_ms > deadline) {
            deadline = mono_ms() + e->decl.stop_timeout_ms;
        }
        MDS_LOG_INFO(LOG_COMP_MDS,
            "companion '%s': sending SIGTERM to pid %d for shutdown",
            e->decl.name, (int)e->pid);
    }
    if (!any) {
        return;
    }

    /* Poll for graceful exits until the longest grace period lapses. */
    while (mono_ms() < deadline) {
        bool remaining = false;

        for (uint32_t i = 0; i < sup->count; i++) {
            struct companion_entry *e = &sup->entries[i];
            int status = 0;
            pid_t r;

            if (e->pid <= 0) {
                continue;
            }
            r = waitpid(e->pid, &status, WNOHANG);
            if (r > 0) {
                e->pid = 0;
                e->state = COMPANION_STATE_STOPPED;
            } else {
                remaining = true;
            }
        }
        if (!remaining) {
            return;
        }
        {
            struct timespec ts = { .tv_sec = 0, .tv_nsec = 20L * 1000000L };

            (void)nanosleep(&ts, NULL);
        }
    }

    /* Anything still alive gets SIGKILL and a blocking reap. */
    for (uint32_t i = 0; i < sup->count; i++) {
        struct companion_entry *e = &sup->entries[i];
        int status = 0;

        if (e->pid <= 0) {
            continue;
        }
        MDS_LOG_WARN(LOG_COMP_MDS,
            "companion '%s': pid %d did not exit; sending SIGKILL",
            e->decl.name, (int)e->pid);
        signal_child_group(e, SIGKILL);
        while (waitpid(e->pid, &status, 0) < 0 && errno == EINTR) {
            /* retry */
        }
        e->pid = 0;
        e->state = COMPANION_STATE_STOPPED;
    }
}

void companion_stop(struct companion_supervisor *sup)
{
    if (sup == NULL) {
        return;
    }

    /*
     * Stop the thread first so nothing races us into respawning a
     * child while we are terminating the set.  After the join, this
     * function is the only thread touching sup.
     */
    atomic_store(&sup->running, false);
    companion_wake(sup);
    if (sup->thread_started) {
        (void)pthread_join(sup->thread, NULL);
        sup->thread_started = false;
    }

    terminate_all(sup);

    if (sup->wake_pipe[0] >= 0) {
        (void)close(sup->wake_pipe[0]);
    }
    if (sup->wake_pipe[1] >= 0) {
        (void)close(sup->wake_pipe[1]);
    }
    pthread_mutex_destroy(&sup->lock);
    free(sup);
}

/* -----------------------------------------------------------------------
 * Admin control
 * ----------------------------------------------------------------------- */

enum mds_status companion_ctl_start(struct companion_supervisor *sup,
                                    const char *name)
{
    struct companion_entry *e;
    enum mds_status st;

    if (sup == NULL) {
        return MDS_ERR_NOSUPPORT;
    }

    pthread_mutex_lock(&sup->lock);
    e = entry_by_name(sup, name);
    if (e == NULL) {
        pthread_mutex_unlock(&sup->lock);
        return MDS_ERR_NOTFOUND;
    }
    if (!e->decl.enabled) {
        pthread_mutex_unlock(&sup->lock);
        return MDS_ERR_PERM;
    }
    if (e->state == COMPANION_STATE_RUNNING) {
        pthread_mutex_unlock(&sup->lock);
        return MDS_ERR_EXISTS;
    }

    /*
     * An explicit admin start is immediate and clears the failure
     * burst: start_delay_ms exists to stagger daemon startup, not to
     * delay an operator request.
     */
    e->restart_count   = 0;
    e->backoff_ms      = e->decl.restart_backoff_ms;
    e->restart_pending = false;
    e->stop_requested  = false;
    st = spawn_locked(sup, e);
    pthread_mutex_unlock(&sup->lock);

    companion_wake(sup);
    return st;
}

enum mds_status companion_ctl_stop(struct companion_supervisor *sup,
                                   const char *name)
{
    struct companion_entry *e;

    if (sup == NULL) {
        return MDS_ERR_NOSUPPORT;
    }

    pthread_mutex_lock(&sup->lock);
    e = entry_by_name(sup, name);
    if (e == NULL) {
        pthread_mutex_unlock(&sup->lock);
        return MDS_ERR_NOTFOUND;
    }

    /* Cancel a pending (re)start regardless of the current state. */
    e->restart_pending = false;
    if (e->state == COMPANION_STATE_STARTING ||
        e->state == COMPANION_STATE_BACKOFF) {
        e->state = COMPANION_STATE_STOPPED;
    }

    if (e->state == COMPANION_STATE_RUNNING) {
        e->stop_requested = true;
        e->stop_deadline_mono_ms = mono_ms() + e->decl.stop_timeout_ms;
        signal_child_group(e, SIGTERM);
        MDS_LOG_INFO(LOG_COMP_MDS,
            "companion '%s': stop requested (SIGTERM to pid %d)",
            e->decl.name, (int)e->pid);
    }
    pthread_mutex_unlock(&sup->lock);

    companion_wake(sup);
    return MDS_OK;
}

enum mds_status companion_ctl_restart(struct companion_supervisor *sup,
                                      const char *name)
{
    struct companion_entry *e;
    enum mds_status st = MDS_OK;

    if (sup == NULL) {
        return MDS_ERR_NOSUPPORT;
    }

    pthread_mutex_lock(&sup->lock);
    e = entry_by_name(sup, name);
    if (e == NULL) {
        pthread_mutex_unlock(&sup->lock);
        return MDS_ERR_NOTFOUND;
    }
    if (!e->decl.enabled) {
        pthread_mutex_unlock(&sup->lock);
        return MDS_ERR_PERM;
    }

    e->restart_count = 0;
    e->backoff_ms    = e->decl.restart_backoff_ms;

    if (e->state == COMPANION_STATE_RUNNING) {
        /* Ask it to stop; the reaper respawns it once it is gone. */
        e->stop_requested        = true;
        e->restart_pending       = true;
        e->stop_deadline_mono_ms = mono_ms() + e->decl.stop_timeout_ms;
        signal_child_group(e, SIGTERM);
        MDS_LOG_INFO(LOG_COMP_MDS,
            "companion '%s': restart requested (SIGTERM to pid %d)",
            e->decl.name, (int)e->pid);
    } else {
        e->restart_pending = false;
        e->stop_requested  = false;
        st = spawn_locked(sup, e);
    }
    pthread_mutex_unlock(&sup->lock);

    companion_wake(sup);
    return st;
}

/* -----------------------------------------------------------------------
 * Status / budget
 * ----------------------------------------------------------------------- */

/** Copy one entry into its wire/report form.  Caller holds the lock. */
static void fill_status_locked(const struct companion_entry *e,
                               struct companion_status *out)
{
    memset(out, 0, sizeof(*out));
    (void)snprintf(out->name, sizeof(out->name), "%s", e->decl.name);
    out->state            = (uint8_t)e->state;
    out->pid              = (int32_t)e->pid;
    out->restart_count    = e->restart_count;
    out->last_exit_code   = (int32_t)e->last_exit_code;
    out->last_term_signal = (int32_t)e->last_term_signal;
    out->started_at_unix  = (uint64_t)e->started_at;
    out->last_exit_unix   = (uint64_t)e->last_exit_at;
    out->ndb_conns        = e->decl.ndb_conns;
    /*
     * Read RSS under the lock: reaping also needs the lock, so the pid
     * cannot be recycled underneath us while /proc is consulted.
     */
    out->rss_kb = (e->state == COMPANION_STATE_RUNNING)
                ? read_rss_kb(e->pid) : 0;
}

uint32_t companion_status_all(struct companion_supervisor *sup,
                              struct companion_status *out,
                              uint32_t max)
{
    uint32_t n = 0;

    if (sup == NULL || out == NULL || max == 0) {
        return 0;
    }

    pthread_mutex_lock(&sup->lock);
    for (uint32_t i = 0; i < sup->count && n < max; i++) {
        fill_status_locked(&sup->entries[i], &out[n]);
        n++;
    }
    pthread_mutex_unlock(&sup->lock);
    return n;
}

enum mds_status companion_status_one(struct companion_supervisor *sup,
                                     const char *name,
                                     struct companion_status *out)
{
    struct companion_entry *e;

    if (out == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (sup == NULL) {
        return MDS_ERR_NOSUPPORT;
    }

    pthread_mutex_lock(&sup->lock);
    e = entry_by_name(sup, name);
    if (e != NULL) {
        fill_status_locked(e, out);
    }
    pthread_mutex_unlock(&sup->lock);

    return (e != NULL) ? MDS_OK : MDS_ERR_NOTFOUND;
}

enum mds_status companion_budget(struct companion_supervisor *sup,
                                 struct companion_budget *out)
{
    if (out == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(out, 0, sizeof(*out));
    if (sup == NULL) {
        return MDS_ERR_NOSUPPORT;
    }

    pthread_mutex_lock(&sup->lock);
    budget_compute_locked(sup, out);
    pthread_mutex_unlock(&sup->lock);
    return MDS_OK;
}

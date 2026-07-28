/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * companion.h -- Supervisor for operator-declared helper processes.
 *
 * A "companion" is an external program whose lifetime is bound to the
 * MDS daemon: the daemon spawns it, watches it, restarts it according
 * to policy, and terminates it during shutdown.  Typical uses are
 * ingest pipelines, exporters, and site-specific maintenance helpers
 * that should not outlive the daemon they cooperate with.
 *
 * Trust model
 * -----------
 * Declarations come exclusively from `companion.<name>.*` keys in
 * mds.conf (see struct mds_companion_decl in pnfs_mds.h).  The admin
 * control path addresses companions by declared name only, so no
 * remote request can introduce a new executable, alter argv, or widen
 * a resource limit.  Adding or changing a companion requires editing
 * mds.conf and restarting the daemon; there is no reload path.
 *
 * Programs are executed with execv() against an absolute path.  No
 * shell is involved, PATH is never consulted, and arguments are taken
 * verbatim from individual config keys, so quoting and globbing
 * semantics do not exist here.
 *
 * Process model
 * -------------
 * One supervisor thread owns every child.  Each child is placed in its
 * own process group so signals reach the whole child tree without ever
 * touching the daemon.  Children are reaped with targeted
 * waitpid(pid, ...) calls; the module installs no SIGCHLD handler and
 * never issues waitpid(-1), so it cannot steal an unrelated child's
 * exit status.
 *
 * Concurrency
 * -----------
 * Every function below is safe to call concurrently from admin RPC
 * threads while the supervisor thread runs.  Mutable per-companion
 * state is protected by an internal mutex; declarations are copied at
 * start and never mutated afterwards.
 *
 * Lifetime
 * --------
 * companion_start() copies everything it needs out of @a cfg, so the
 * config object may go out of scope afterwards.  Every function
 * tolerates a NULL supervisor handle, which is the state left behind
 * when the feature is disabled or the module is compiled out.
 */

#ifndef COMPANION_H
#define COMPANION_H

#include <stdint.h>

#include "pnfs_mds.h"

/** Opaque supervisor handle. */
struct companion_supervisor;

/** Observable lifecycle state of a single companion. */
enum companion_state {
    /** Declared and startable, but not currently running. */
    COMPANION_STATE_STOPPED  = 0,
    /** Spawn requested; waiting out start_delay_ms. */
    COMPANION_STATE_STARTING = 1,
    /** Child is alive. */
    COMPANION_STATE_RUNNING  = 2,
    /** Child failed; waiting out the restart backoff. */
    COMPANION_STATE_BACKOFF  = 3,
    /** Restart budget exhausted; no further automatic restarts. */
    COMPANION_STATE_FAILED   = 4,
    /** `enabled = false`, or the master switch is off. */
    COMPANION_STATE_DISABLED = 5,
};

/** Point-in-time snapshot of one companion. */
struct companion_status {
    char     name[MDS_COMPANION_NAME_MAX];
    uint8_t  state;              /**< An enum companion_state value. */
    int32_t  pid;                /**< 0 when not running. */
    uint32_t restart_count;      /**< Restarts in the current burst. */
    /** Exit status of the last run, or -1 if it never exited normally. */
    int32_t  last_exit_code;
    /** Signal that killed the last run, or 0 if none. */
    int32_t  last_term_signal;
    uint64_t started_at_unix;    /**< 0 when never started. */
    uint64_t last_exit_unix;     /**< 0 when never exited. */
    uint64_t rss_kb;             /**< 0 when unavailable. */
    uint32_t ndb_conns;          /**< Declared NDB connections. */
};

/**
 * Advisory RonDB `[api]` slot accounting.
 *
 * Every field is a declared or configured quantity, not a measurement.
 * The daemon cannot see how many NDB connections a child actually
 * opens, and it has no visibility into peer nodes -- use
 * `ndb_api_slots_reserved` to reserve headroom for peers and transient
 * tools.  RonDB refusing a connection remains the authoritative
 * signal that the cluster is out of slots.
 */
struct companion_budget {
    uint32_t slots_total;        /**< Configured total; 0 = undeclared. */
    uint32_t slots_mds;          /**< This daemon's ndb_conn_pool_size. */
    uint32_t slots_reserved;     /**< Operator-reserved headroom. */
    uint32_t slots_declared;     /**< Sum over enabled companions. */
    uint32_t slots_running;      /**< Sum over running companions. */
    /**
     * slots_total - slots_mds - slots_reserved - slots_running.
     * Signed on purpose: oversubscription must be visible rather than
     * clamped.  Meaningless (reported 0) when slots_total is 0.
     */
    int32_t  slots_free;
    uint32_t companion_count;    /**< Enabled declarations. */
    uint32_t running_count;      /**< Currently running. */
    uint8_t  admission;          /**< enum companion_ndb_admission. */
};

/**
 * @brief Create the supervisor and autostart eligible companions.
 *
 * Copies all declarations and budget inputs from @a cfg, then starts
 * the supervisor thread.  Companions with `autostart = true` are
 * spawned; the rest stay COMPANION_STATE_STOPPED until an admin
 * request starts them.
 *
 * When the feature is inert -- @a cfg is NULL, companion_enabled is
 * false, or no companions are declared -- this succeeds with
 * *@a out == NULL and starts no thread.  Callers must therefore treat
 * a NULL handle as normal, not as an error.
 *
 * @param cfg       Live config; only read, never retained.
 * @param[out] out  Receives the handle, or NULL when inert.
 * @return 0 on success (including the inert case), -1 on failure.
 */
int companion_start(const struct mds_config *cfg,
                    struct companion_supervisor **out);

/**
 * @brief Terminate every child, stop the thread, and free the handle.
 *
 * Each running child's process group receives SIGTERM, gets up to its
 * configured stop_timeout_ms to exit, and is then SIGKILLed.  All
 * children are reaped before the supervisor thread is joined, so no
 * zombies are left behind.
 *
 * Must not run concurrently with the admin control path: unregister
 * the handle from the transport (or stop the transport) first.
 *
 * @param sup  Supervisor handle; NULL is tolerated.
 */
void companion_stop(struct companion_supervisor *sup);

/**
 * @brief Start a declared companion that is not currently running.
 *
 * Clears any accumulated restart backoff, so this also serves as the
 * recovery action for a COMPANION_STATE_FAILED companion.
 *
 * @param sup   Supervisor handle.
 * @param name  Declared companion name.
 * @return MDS_OK on success;
 *         MDS_ERR_NOTFOUND if @a name is not declared;
 *         MDS_ERR_EXISTS if it is already running;
 *         MDS_ERR_PERM if the declaration is disabled;
 *         MDS_ERR_NOSPC if enforcing admission and the slot budget is
 *         exhausted;
 *         MDS_ERR_NOSUPPORT if @a sup is NULL;
 *         MDS_ERR_IO if the spawn itself failed.
 */
enum mds_status companion_ctl_start(struct companion_supervisor *sup,
                                    const char *name);

/**
 * @brief Stop a running companion and suppress automatic restart.
 *
 * The stop is deliberate, so the restart policy is not applied when
 * the child exits; the companion settles in COMPANION_STATE_STOPPED.
 *
 * @param sup   Supervisor handle.
 * @param name  Declared companion name.
 * @return MDS_OK on success (including when already stopped);
 *         MDS_ERR_NOTFOUND if @a name is not declared;
 *         MDS_ERR_NOSUPPORT if @a sup is NULL.
 */
enum mds_status companion_ctl_stop(struct companion_supervisor *sup,
                                   const char *name);

/**
 * @brief Stop then start a companion, resetting its restart counter.
 *
 * @param sup   Supervisor handle.
 * @param name  Declared companion name.
 * @return As companion_ctl_start(), except that an already-running
 *         companion is stopped first rather than returning
 *         MDS_ERR_EXISTS.
 */
enum mds_status companion_ctl_restart(struct companion_supervisor *sup,
                                      const char *name);

/**
 * @brief Snapshot every declared companion.
 *
 * @param sup       Supervisor handle; NULL yields 0 entries.
 * @param[out] out  Caller-provided array.
 * @param max       Capacity of @a out in entries.
 * @return Number of entries written (<= max, <= MDS_MAX_COMPANIONS).
 */
uint32_t companion_status_all(struct companion_supervisor *sup,
                              struct companion_status *out,
                              uint32_t max);

/**
 * @brief Snapshot a single companion by declared name.
 *
 * @param sup       Supervisor handle.
 * @param name      Declared companion name.
 * @param[out] out  Receives the snapshot; zeroed on failure.
 * @return MDS_OK, MDS_ERR_NOTFOUND, or MDS_ERR_NOSUPPORT if NULL.
 */
enum mds_status companion_status_one(struct companion_supervisor *sup,
                                     const char *name,
                                     struct companion_status *out);

/**
 * @brief Compute the advisory NDB API slot budget.
 *
 * @param sup       Supervisor handle; NULL yields a zeroed budget.
 * @param[out] out  Receives the budget.
 * @return MDS_OK, or MDS_ERR_NOSUPPORT if @a sup is NULL.
 */
enum mds_status companion_budget(struct companion_supervisor *sup,
                                 struct companion_budget *out);

#endif /* COMPANION_H */

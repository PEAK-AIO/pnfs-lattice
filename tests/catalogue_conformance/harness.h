/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * harness.h -- Backend-parameterised catalogue conformance harness.
 *
 * Every conformance test (and the refactored catalogue unit tests)
 * opens its catalogue through conformance_open(), which selects the
 * backend from the CATALOGUE_TEST_BACKEND environment variable:
 *
 *   memdb   (default)  the in-process reference backend
 *   rondb              RonDB via mds_catalogue_open(); the RonDB
 *                      config file comes from RONDB_CONF
 *   fdb                FoundationDB via mds_catalogue_open() when the
 *                      binary was built with ENABLE_FDB; the cluster
 *                      file comes from FDB_CLUSTER_FILE (default
 *                      /etc/foundationdb/fdb.cluster) and every open of
 *                      the process shares one isolated key prefix
 *                      (CATALOGUE_TEST_KEY_PREFIX, or one generated per
 *                      process) that conformance_shutdown() wipes
 *   daos               DAOS via mds_catalogue_open() when the binary
 *                      was built with ENABLE_DAOS; the pool comes from
 *                      DAOS_POOL (mandatory), the system name from
 *                      DAOS_SYS (default daos_server), and every open of
 *                      the process shares one container: the one named
 *                      by CATALOGUE_TEST_CONT (operator-owned, kept), or
 *                      a private container the first open creates and
 *                      conformance_shutdown() destroys
 *
 * A backend that is unavailable in the running binary or environment
 * is a SKIP, never a pass or a fail: the harness prints the reason and
 * exits with CONFORMANCE_SKIP (77), which every registered test maps to
 * ctest's SKIP_RETURN_CODE.  An unknown backend name is a usage error
 * and exits non-zero.
 */

#ifndef CATALOGUE_CONFORMANCE_HARNESS_H
#define CATALOGUE_CONFORMANCE_HARNESS_H

#include <stdbool.h>
#include <stdint.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"

/** Process exit code for "backend unavailable"; ctest SKIP_RETURN_CODE. */
#define CONFORMANCE_SKIP 77

/** Environment variable selecting the backend under test. */
#define CONFORMANCE_BACKEND_ENV "CATALOGUE_TEST_BACKEND"

/** Environment variable naming the RonDB config file (rondb only). */
#define CONFORMANCE_RONDB_CONF_ENV "RONDB_CONF"

/** Environment variables of the daos backend: the pool label or UUID
 *  (mandatory), the DAOS system name (default daos_server) and an
 *  operator-owned container to use instead of a private one. */
#define CONFORMANCE_DAOS_POOL_ENV "DAOS_POOL"
#define CONFORMANCE_DAOS_SYS_ENV  "DAOS_SYS"
#define CONFORMANCE_DAOS_CONT_ENV "CATALOGUE_TEST_CONT"

/**
 * Open a catalogue on the selected backend.
 *
 * @param out  Receives the handle on MDS_OK.
 * @return MDS_OK, or the backend's open status unchanged.  Does not
 *         return when the backend is unavailable (exits CONFORMANCE_SKIP)
 *         or the backend name is unknown (exits 1).
 */
enum mds_status conformance_open(struct mds_catalogue **out);

/**
 * conformance_open() as MDS @p self_id: a further handle of the SAME
 * store this process prepared -- the fdb key prefix, the daos
 * container, the configured RonDB cluster -- for tests that exercise
 * owner-scoped rows (GC owner filter, recovery ownership, registry
 * epochs) from several MDS identities.  conformance_open() is this
 * call with self_id 1.  On memdb every open is an independent
 * instance and the id is ignored.
 *
 * @param self_id  cfg->self.id of the new handle.
 * @param out      Receives the handle on MDS_OK.
 * @return As conformance_open().
 */
enum mds_status conformance_open_as(uint32_t self_id, struct mds_catalogue **out);

/**
 * conformance_open() for tests whose fixture cannot proceed without a
 * handle: exits 1 with a diagnostic when the open fails (a genuine
 * failure, never a skip).
 *
 * @return A non-NULL catalogue handle.
 */
struct mds_catalogue *conformance_open_checked(void);

/** Name of the selected backend ("memdb", "rondb", "fdb" or "daos"). */
const char *conformance_backend_name(void);

/** True when the selected backend name equals @p name. */
bool conformance_backend_is(const char *name);

/** Print "SKIP: <reason>" and exit with CONFORMANCE_SKIP; never returns. */
_Noreturn void conformance_skip(const char *reason);

/**
 * Process-wide teardown; every test main calls it once, after its last
 * handle is closed and immediately before returning.  On fdb it wipes
 * the run's rows and stops and joins the client network
 * (mds_catalogue_process_shutdown), which is terminal for the process:
 * no conformance_open() may follow.  On daos it destroys the private
 * container the process created (never one named by
 * CATALOGUE_TEST_CONT) and finalises the client library.  On memdb /
 * rondb it is a no-op.  Idempotent; an atexit() safety net covers a
 * main that exits early.
 */
void conformance_shutdown(void);

/**
 * Create a fresh, uniquely named scratch directory under the root so
 * tests never collide with each other or with a persistent store
 * (RonDB keeps its namespace across runs).
 *
 * @param cat      Catalogue handle.
 * @param out_fid  Receives the new directory's fileid.
 * @return MDS_OK or the create status.
 */
enum mds_status conformance_scratch_dir(struct mds_catalogue *cat,
                                        uint64_t *out_fid);

/**
 * Remove every entry of @p dir_fid (one level, files and empty
 * directories) and then @p dir_fid itself from the root.  A raw dirent
 * whose inode is already gone is dropped with mds_cat_dirent_del when
 * ns_remove refuses it (NOTFOUND / STALE).  Best effort: anything that
 * could not be removed is reported on stderr, never a failure; used by
 * tests that share a persistent store.
 *
 * @param cat      Catalogue handle.
 * @param dir_fid  Directory created by conformance_scratch_dir().
 */
void conformance_scratch_cleanup(struct mds_catalogue *cat,
                                 uint64_t dir_fid);

#endif /* CATALOGUE_CONFORMANCE_HARNESS_H */

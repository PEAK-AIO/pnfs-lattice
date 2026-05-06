/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mountd_compat.h -- Compatibility responder for `showmount -e`.
 *
 * The MDS is an NFSv4.1 / pNFS server: it does NOT speak the NFSv3
 * MOUNT protocol and does NOT serve NFSv3 data.  However, many
 * operators reach for `showmount -e <host>` as a sanity check that
 * the server is exporting anything at all.  When asked of an MDS
 * today, that command fails with "RPC: Program not registered",
 * which causes spurious "is the MDS broken?" tickets.
 *
 * This module starts a tiny ONC-RPC listener that answers MOUNT3
 * (program 100005, version 3) procedures NULL, EXPORT and DUMP with
 * a synthetic, MDS-defined response, and rejects every other
 * procedure with PROC_UNAVAIL.  Specifically, MNT (proc 1) -- the
 * only procedure that would yield a usable v3 file handle -- is
 * always rejected at the RPC layer, so a client cannot accidentally
 * mount the MDS over NFSv3.
 *
 * It does NOT proxy any request to a Data Server.  The export list
 * is whatever the operator configured in mds.conf, defaulting to a
 * single entry "/".  No DS hostnames, IPs, or paths are ever
 * advertised.
 *
 * See docs/mountd-compat.md for the full design and threat model.
 */
#ifndef MOUNTD_COMPAT_H
#define MOUNTD_COMPAT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Forward declaration to avoid pulling pnfs_mds.h transitively. */
struct mds_config;

/* Opaque server handle. */
struct mountd_compat_ctx;

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */

/**
 * Start the showmount-compatibility responder.
 *
 * Binds UDP and TCP on @a cfg->mountd_compat_bind_addr at port
 * @a cfg->mountd_compat_port (0 = OS-assigned ephemeral) and spawns
 * one dedicated worker thread to drive the epoll loop.  If
 * @a cfg->mountd_compat_register_rpcbind is true, sends a best-effort
 * PMAPPROC_SET to 127.0.0.1:111 so that `showmount -e <host>` can
 * discover the bound port via the local portmapper.  Failure to
 * register is logged but does not fail startup -- clients with a
 * fixed-port out-of-band discovery still work.
 *
 * The export list and other configuration values are snapshotted at
 * the time of this call; subsequent mutations to @a cfg do not affect
 * the running responder.
 *
 * @param cfg  Live configuration.  Must not be NULL.
 * @param out  Receives the opaque handle on success; set to NULL on
 *             failure.  Must not be NULL.
 * @return 0 on success, -1 on bind/listen/thread failure.
 */
int mountd_compat_start(const struct mds_config *cfg,
                        struct mountd_compat_ctx **out);

/**
 * Quiesce and free the responder.
 *
 * Signals the worker thread to exit, joins it, closes all sockets,
 * and (if registered) sends a best-effort PMAPPROC_UNSET to the
 * local rpcbind so subsequent `showmount` calls cleanly fail.  A
 * NULL @a ctx is tolerated (no-op).
 */
void mountd_compat_stop(struct mountd_compat_ctx *ctx);

/* -----------------------------------------------------------------------
 * Test-only interface -- see tests/unit/test_mountd_compat.c.
 *
 * The dispatcher is exposed as a pure function so unit tests can
 * exercise every error and success path with synthetic RPC packets,
 * without opening sockets or spawning threads.  Real callers
 * (main.c) MUST go through mountd_compat_start / _stop.
 * ----------------------------------------------------------------------- */

/** Maximum number of synthetic exports the responder will advertise. */
#define MOUNTD_COMPAT_MAX_EXPORTS 16

/** Maximum length (including NUL) of a single synthetic export path. */
#define MOUNTD_COMPAT_PATH_MAX    256

/**
 * Pre-validated, frozen export list snapshot.  Each entry is a
 * NUL-terminated UTF-8 path string.  Entries beyond @a count are
 * undefined; do not read them.
 */
struct mountd_compat_exports {
    uint32_t count;
    char     paths[MOUNTD_COMPAT_MAX_EXPORTS][MOUNTD_COMPAT_PATH_MAX];
};

/**
 * Process a single ONC-RPC call message and produce a single reply.
 *
 * The input is a raw ONC-RPC call (no TCP record marker).  On
 * success, an ONC-RPC reply suitable for transmission (without
 * record marker) is written to @a out and its length is stored in
 * @a *out_len.  On a -1 return the call must be silently dropped:
 * the input was so malformed that no XID could be extracted and no
 * RPC reply is appropriate (RFC 5531 S10).
 *
 * The function is pure with respect to its inputs and never
 * allocates, blocks, or reads I/O.  All state is in @a exports
 * (read-only) and the local stack.
 *
 * @param exports  Export list to advertise on EXPORT replies.
 * @param in       Raw RPC call bytes.  Must not be NULL.
 * @param in_len   Length of @a in.  0 < in_len <= 8192.
 * @param out      Output buffer for the reply.  Must not be NULL.
 * @param out_cap  Capacity of @a out.  Must be >= 32.
 * @param out_len  Receives the produced reply length on success.
 *                 Must not be NULL.
 * @return 0 on success, -1 if the call should be dropped.
 */
int mountd_compat_handle_packet(const struct mountd_compat_exports *exports,
                                const uint8_t *in, size_t in_len,
                                uint8_t *out, size_t out_cap,
                                size_t *out_len);

#endif /* MOUNTD_COMPAT_H */

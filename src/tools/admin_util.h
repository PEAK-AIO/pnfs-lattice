/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * admin_util.h — Shared helpers for the mds-admin CLI tool.
 *
 * NOT a public API header.  Only included by mds_admin.c and
 * test_mds_admin.c.
 */

#ifndef ADMIN_UTIL_H
#define ADMIN_UTIL_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

/* Default admin endpoint. */
#define DEFAULT_MDS_HOST  "127.0.0.1"
#define DEFAULT_MDS_PORT  9800

/* Default destination endpoint. */
#define DEFAULT_DEST_HOST "127.0.0.1"
#define DEFAULT_DEST_PORT 9820

/**
 * Parse --mds-host / --mds-port and --json from argv.
 *
 * Scans argv[0..argc-1] for recognised flags, consuming matched pairs.
 * Unrecognised args are left untouched.
 *
 * @param argc   Argument count.
 * @param argv   Argument array (read-only).
 * @param host   Receives host string pointer (into argv or default).
 * @param port   Receives port number.
 * @param json   If non-NULL, set to true when --json is present.
 */
/**
 * @return 0 on success, -1 if --mds-port value is invalid/missing.
 */
int parse_admin_endpoint(int argc, const char *const *argv,
                         const char **host, uint16_t *port,
                         bool *json);

/**
 * Parse --host / --port (destination endpoint) from argv.
 *
 * @param argc      Argument count.
 * @param argv      Argument array (read-only).
 * @param host      Receives destination host string.
 * @param port      Receives destination port.
 * @param have_host Set to true if --host was found.
 * @param have_port Set to true if --port was found.
 * @return 0 on success, -1 if a port value is invalid.
 */
int parse_dest_endpoint(int argc, const char *const *argv,
                        const char **host, uint16_t *port,
                        bool *have_host, bool *have_port);

/**
 * Escape a string for JSON output per RFC 8259.
 *
 * Escapes \, ", and control characters (U+0000..U+001F).
 * The output is NOT wrapped in quotes — the caller adds those.
 *
 * @param in   Input NUL-terminated string.
 * @param out  Output buffer.
 * @param cap  Capacity of output buffer (including NUL).
 * @return Number of characters written (excluding NUL), or -1 on
 *         truncation.
 */
int json_escape_string(const char *in, char *out, size_t cap);

/**
 * Parse a DS state name string to its numeric value.
 *
 * @param name  State name (online/offline/draining/rebalancing).
 * @param out   Receives the numeric state.
 * @return 0 on success, -1 if the name is unrecognised.
 */
int parse_ds_state(const char *name, uint32_t *out);

/**
 * Parse a DS mode name to its numeric value.
 * @return 0 on success, -1 if unrecognised.
 */
int parse_ds_mode(const char *name, uint8_t *out);

/**
 * Parse a DS transport string to a bitmask.
 * Accepts: "tcp", "rdma", "tcp,rdma".
 * @return 0 on success, -1 if unrecognised.
 */
int parse_ds_transport(const char *name, uint8_t *out);

/**
 * Parse a DS data filename like "42_0_0" into components.
 * @return 0 on success, -1 if format is invalid.
 */
int parse_ds_filename(const char *name, uint64_t *fileid,
                      uint32_t *stripe, uint32_t *mirror);

/**
 * Resolve an MDS namespace path (e.g. "/data/subdir") to a fileid.
 * Walks the directory tree from root.
 */
#include "pnfs_mds.h"

/** Safe file copy using sendfile(). @return 0 on success. */
int copy_file_safe(const char *src, const char *dst);
enum mds_status resolve_mds_path(void *unused,
                                 const char *path, const uint64_t *fileid);

#endif /* ADMIN_UTIL_H */

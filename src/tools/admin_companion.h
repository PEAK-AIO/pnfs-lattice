/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * admin_companion.h -- `mds-admin companion` subcommand dispatch.
 *
 * Companion processes are declared in mds.conf and supervised by the
 * daemon (see include/companion.h).  These commands only inspect and
 * control declarations that already exist: the wire protocol carries a
 * name, never an executable path or argument vector, so nothing here
 * can make the daemon run a program the operator did not pre-approve.
 *
 * NOT a public API header.  Only included by mds_admin.c.
 */

#ifndef ADMIN_COMPANION_H
#define ADMIN_COMPANION_H

/**
 * Dispatch entry point for `mds-admin companion <subcommand>`.
 *
 * @param argc  Full argc from main().
 * @param argv  Full argv from main().
 * @return 0 on success, positive on command failure, -1 when the
 *         subcommand is unrecognised (caller prints usage).
 */
int dispatch_companion(int argc, const char *const *argv);

#endif /* ADMIN_COMPANION_H */

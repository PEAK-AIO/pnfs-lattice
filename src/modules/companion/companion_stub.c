/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * companion_stub.c -- no-op stubs for the companion supervisor.
 *
 * Linked into pnfs_mds_core when ENABLE_COMPANION=OFF.  The real
 * implementation is in companion.c.
 *
 * Semantics: the daemon never spawns a helper process, exactly as if
 * the operator had declared none.  companion_start() reports the
 * documented inert result (0 with *out = NULL) so main.c needs no
 * conditional logic, and every control or query entry point reports
 * MDS_ERR_NOSUPPORT so the admin CLI can tell "feature absent" apart
 * from "no companions configured".
 *
 * Any `companion.<name>.*` keys in mds.conf are still parsed and
 * validated by src/common/config.c; they simply have no effect here.
 */

#include "companion.h"

#include <stddef.h>
#include <string.h>

int companion_start(const struct mds_config *cfg,
                    struct companion_supervisor **out)
{
    (void)cfg;

    if (out != NULL) {
        *out = NULL;
    }
    return 0;
}

void companion_stop(struct companion_supervisor *sup)
{
    (void)sup;
}

enum mds_status companion_ctl_start(struct companion_supervisor *sup,
                                    const char *name)
{
    (void)sup;
    (void)name;
    return MDS_ERR_NOSUPPORT;
}

enum mds_status companion_ctl_stop(struct companion_supervisor *sup,
                                   const char *name)
{
    (void)sup;
    (void)name;
    return MDS_ERR_NOSUPPORT;
}

enum mds_status companion_ctl_restart(struct companion_supervisor *sup,
                                      const char *name)
{
    (void)sup;
    (void)name;
    return MDS_ERR_NOSUPPORT;
}

uint32_t companion_status_all(struct companion_supervisor *sup,
                              struct companion_status *out,
                              uint32_t max)
{
    (void)sup;
    (void)out;
    (void)max;
    return 0;
}

enum mds_status companion_status_one(struct companion_supervisor *sup,
                                     const char *name,
                                     struct companion_status *out)
{
    (void)sup;
    (void)name;

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    return MDS_ERR_NOSUPPORT;
}

enum mds_status companion_budget(struct companion_supervisor *sup,
                                 struct companion_budget *out)
{
    (void)sup;

    if (out != NULL) {
        memset(out, 0, sizeof(*out));
    }
    return MDS_ERR_NOSUPPORT;
}

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_factory.c — Backend selection factory for mds_catalogue_open.
 *
 * Dispatches to the appropriate backend constructor based on
 * cfg->catalogue_backend.  Only RonDB is supported.
 */

#include <stdio.h>

#include "mds_catalogue.h"

enum mds_status mds_catalogue_open(const struct mds_config *cfg,
				   struct mds_catalogue **out)
{
	if (cfg == NULL || out == NULL) {
		return MDS_ERR_INVAL;
	}

#ifdef HAVE_RONDB
	if (cfg->catalogue_backend == MDS_BACKEND_RONDB) {
		return catalogue_rondb_open(cfg, out);
	}
#endif

	(void)fprintf(stderr,
		"ERROR: unsupported catalogue_backend %d\n",
		(int)cfg->catalogue_backend);
	return MDS_ERR_INVAL;
}

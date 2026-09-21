/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_factory.c -- Backend registration table for mds_catalogue_open.
 *
 * One static table lists every backend the tree KNOWS about (its enum
 * value) and, when the backend is compiled into this binary, its
 * constructor.  Names are NOT duplicated here: the mds.conf name table
 * lives in pnfs_common (catalogue_backend_names.c) so the config
 * parser can resolve names without depending on this library; this
 * file answers the availability question the parser cannot.  Adding a
 * backend is one row here, one row in the name table and its
 * ENABLE_<BACKEND> build option; no other file needs a per-backend
 * #ifdef.
 *
 * "Known" and "available" are deliberately distinct so an operator who
 * names a backend this binary lacks is told "not compiled in" rather
 * than "no such backend".
 */

#include <stdio.h>
#include <string.h>

#include "mds_catalogue.h"
#include "catalogue_backend_names.h"
#include "mds_log.h"

#ifdef HAVE_MEMDB_BACKEND
#include "catalogue_memdb.h"
#endif
#ifdef HAVE_FDB
#include "catalogue_fdb.h"
#endif

/** One row per known backend.  open == NULL means "known, not compiled
 *  in". */
struct catalogue_backend_entry {
	enum mds_catalogue_backend  id;
	enum mds_status           (*open)(const struct mds_config *cfg,
					  struct mds_catalogue **out);
};

static const struct catalogue_backend_entry catalogue_backends[] = {
	{
		.id   = MDS_BACKEND_RONDB,
#ifdef HAVE_RONDB
		.open = catalogue_rondb_open,
#else
		.open = NULL,
#endif
	},
	{
		.id   = MDS_BACKEND_MEMDB,
#ifdef HAVE_MEMDB_BACKEND
		.open = catalogue_memdb_open_cfg,
#else
		.open = NULL,
#endif
	},
	{
		.id   = MDS_BACKEND_FDB,
#ifdef HAVE_FDB
		.open = catalogue_fdb_open,
#else
		.open = NULL,
#endif
	},
};

#define CATALOGUE_BACKEND_COUNT \
	(sizeof(catalogue_backends) / sizeof(catalogue_backends[0]))

static const struct catalogue_backend_entry *
catalogue_backend_find(enum mds_catalogue_backend id)
{
	size_t i;

	for (i = 0; i < CATALOGUE_BACKEND_COUNT; i++) {
		if (catalogue_backends[i].id == id) {
			return &catalogue_backends[i];
		}
	}
	return NULL;
}

bool mds_catalogue_backend_available(enum mds_catalogue_backend backend)
{
	const struct catalogue_backend_entry *e = catalogue_backend_find(backend);

	return e != NULL && e->open != NULL;
}

size_t mds_catalogue_backend_available_names(char *buf, size_t cap)
{
	enum mds_catalogue_backend ids[CATALOGUE_BACKEND_COUNT];
	size_t n = 0;
	size_t i;

	for (i = 0; i < CATALOGUE_BACKEND_COUNT; i++) {
		/* Only ids[0..n) are ever read; the sentinel fill keeps a
		 * build with no available backend (n == 0) free of an
		 * uninitialised-array diagnostic. */
		ids[i] = MDS_BACKEND_NONE;
		if (catalogue_backends[i].open != NULL) {
			ids[n++] = catalogue_backends[i].id;
		}
	}
	return mds_catalogue_backend_join_names(ids, n, buf, cap);
}

/* Explain why mds_catalogue_open() refuses @id: @e is its table row
 * when the backend is known but not compiled in, NULL when the id is
 * not in the table at all (MDS_BACKEND_NONE, or garbage). */
static void catalogue_open_log_refusal(enum mds_catalogue_backend id,
				       const struct catalogue_backend_entry *e)
{
	char avail[128];

	(void)mds_catalogue_backend_available_names(avail, sizeof(avail));
	if (e != NULL) {
		const char *name = mds_catalogue_backend_name(e->id);

		MDS_LOG_ERROR(LOG_COMP_CAT,
			"catalogue_backend %s not compiled in; available: %s",
			name != NULL ? name : "?", avail);
	} else if (id == MDS_BACKEND_NONE) {
		MDS_LOG_ERROR(LOG_COMP_CAT,
			"catalogue_backend not set and no default backend is "
			"compiled in; available: %s", avail);
	} else {
		MDS_LOG_ERROR(LOG_COMP_CAT,
			"unknown catalogue_backend %d; available: %s",
			(int)id, avail);
	}
}

enum mds_status mds_catalogue_open(const struct mds_config *cfg,
				   struct mds_catalogue **out)
{
	const struct catalogue_backend_entry *e;

	if (cfg == NULL || out == NULL) {
		return MDS_ERR_INVAL;
	}

	e = catalogue_backend_find(cfg->catalogue_backend);
	if (e == NULL || e->open == NULL) {
		catalogue_open_log_refusal(cfg->catalogue_backend, e);
		return MDS_ERR_INVAL;
	}
	return e->open(cfg, out);
}

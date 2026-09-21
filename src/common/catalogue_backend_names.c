/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_backend_names.c -- mds.conf names of the known catalogue
 * backends.
 *
 * The single source of truth for the name of every backend the tree
 * knows about.  It lives in pnfs_common so the config parser resolves
 * names without a dependency on the catalogue core; the core's
 * registration table (src/catalogue/catalogue_factory.c) holds only
 * the constructors and asks this table for names when it reports.
 */

#include <stdio.h>
#include <string.h>

#include "catalogue_backend_names.h"

struct catalogue_backend_name {
	const char                 *name;
	enum mds_catalogue_backend  id;
};

static const struct catalogue_backend_name catalogue_backend_names[] = {
	{ "rondb", MDS_BACKEND_RONDB },
	{ "memdb", MDS_BACKEND_MEMDB },
	{ "fdb",   MDS_BACKEND_FDB },
};

#define CATALOGUE_BACKEND_NAME_COUNT \
	(sizeof(catalogue_backend_names) / sizeof(catalogue_backend_names[0]))

/* MDS_BACKEND_NONE is the "no catalogue" sentinel and must never carry
 * a name: a config that never named a backend must not resolve to
 * anything constructible. */
_Static_assert(MDS_BACKEND_NONE != MDS_BACKEND_RONDB &&
	       MDS_BACKEND_NONE != MDS_BACKEND_MEMDB &&
	       MDS_BACKEND_NONE != MDS_BACKEND_FDB,
	       "MDS_BACKEND_NONE collides with a named backend");

enum mds_status mds_catalogue_backend_from_name(
	const char *name, enum mds_catalogue_backend *out)
{
	size_t i;

	if (name == NULL || out == NULL) {
		return MDS_ERR_INVAL;
	}
	for (i = 0; i < CATALOGUE_BACKEND_NAME_COUNT; i++) {
		if (strcmp(catalogue_backend_names[i].name, name) == 0) {
			*out = catalogue_backend_names[i].id;
			return MDS_OK;
		}
	}
	return MDS_ERR_INVAL;
}

const char *mds_catalogue_backend_name(enum mds_catalogue_backend backend)
{
	size_t i;

	for (i = 0; i < CATALOGUE_BACKEND_NAME_COUNT; i++) {
		if (catalogue_backend_names[i].id == backend) {
			return catalogue_backend_names[i].name;
		}
	}
	return NULL;
}

size_t mds_catalogue_backend_join_names(const enum mds_catalogue_backend *ids,
					size_t n, char *buf, size_t cap)
{
	size_t i;
	size_t used = 0;

	if (buf == NULL || cap == 0) {
		return 0;
	}
	if (n == 0 || ids == NULL) {
		(void)snprintf(buf, cap, "(none)");
		return 0;
	}
	buf[0] = '\0';
	for (i = 0; i < n && used < cap; i++) {
		const char *name = mds_catalogue_backend_name(ids[i]);
		int w;

		w = snprintf(buf + used, cap - used, "%s%s",
			     i > 0 ? ", " : "", name != NULL ? name : "?");
		if (w < 0) {
			break;
		}
		/* snprintf reports what WOULD have been written; saturate
		 * at cap so the offset never leaves the buffer on
		 * truncation (the loop condition then stops the join). */
		used += (size_t)w;
		if (used > cap) {
			used = cap;
		}
	}
	return n;
}

size_t mds_catalogue_backend_known_names(char *buf, size_t cap)
{
	enum mds_catalogue_backend ids[CATALOGUE_BACKEND_NAME_COUNT];
	size_t i;

	for (i = 0; i < CATALOGUE_BACKEND_NAME_COUNT; i++) {
		ids[i] = catalogue_backend_names[i].id;
	}
	return mds_catalogue_backend_join_names(ids, CATALOGUE_BACKEND_NAME_COUNT,
						buf, cap);
}

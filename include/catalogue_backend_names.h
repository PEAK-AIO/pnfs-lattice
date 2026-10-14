/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_backend_names.h -- mds.conf names of the known catalogue
 * backends.
 *
 * Pure name <-> enum mapping, implemented in pnfs_common
 * (src/common/catalogue_backend_names.c) so the config parser can
 * resolve a name without depending on the catalogue core.  "Known" is
 * a property of the source tree; whether a backend is COMPILED IN is a
 * property of the core's build and is answered separately by
 * mds_catalogue_backend_available() (mds_catalogue.h).
 */

#ifndef CATALOGUE_BACKEND_NAMES_H
#define CATALOGUE_BACKEND_NAMES_H

#include <stddef.h>

#include "pnfs_mds.h"

/**
 * Resolve an mds.conf backend name ("rondb", "memdb", "fdb").
 *
 * @param name  NUL-terminated name from the config file (exact match).
 * @param out   Receives the enum value when the name is known.
 * @return MDS_OK when known (compiled in or not); MDS_ERR_INVAL for a
 *         NULL argument or an unknown name (*out untouched).
 */
enum mds_status mds_catalogue_backend_from_name(
	const char *name, enum mds_catalogue_backend *out);

/**
 * The mds.conf name of @p backend, or NULL for MDS_BACKEND_NONE and
 * any value that is not a known backend.  The string is static.
 */
const char *mds_catalogue_backend_name(enum mds_catalogue_backend backend);

/**
 * Join the names of @p ids into @p buf as "a, b, c"; "(none)" when
 * @p n is 0.  An id without a name is written as "?".  The output is
 * truncated to fit; @p cap must be > 0.
 *
 * @return @p n (the number of ids, not the string length); 0 also when
 *         buf is NULL or cap is 0.
 */
size_t mds_catalogue_backend_join_names(const enum mds_catalogue_backend *ids,
					size_t n, char *buf, size_t cap);

/**
 * Write the comma-separated names of every KNOWN backend into @p buf
 * (see mds_catalogue_backend_join_names for the format).
 *
 * @return The number of known backends; 0 when buf is NULL or cap is 0.
 */
size_t mds_catalogue_backend_known_names(char *buf, size_t cap);

#endif /* CATALOGUE_BACKEND_NAMES_H */

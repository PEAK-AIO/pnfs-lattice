/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * slot_matrix.c -- Per-backend slot-coverage matrix (Phase 6a).
 *
 * Opens the catalogue through the conformance harness and compares the
 * presence of every slot of the authority, coordination, cluster and
 * lifecycle vtables, the capability bits and the two public capability
 * predicates against the expected table in slot_matrix_expected.h.
 *
 * The slots are read straight from struct mds_catalogue
 * (catalogue_internal.h): the matrix is about what the backend
 * registers, not about what the dispatcher does with it.  A compile-time
 * check pins the row count of each table to the number of function
 * pointers in the corresponding struct so a new slot cannot be added
 * without a matrix decision.
 */

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "harness.h"
#include "slot_matrix_expected.h"
#include "mds_coordination.h"
#include "mds_cluster.h"

/* -----------------------------------------------------------------------
 * Accessors generated from the expected table
 * ----------------------------------------------------------------------- */

#define AUTH_ACCESSOR(name, m, r, f)                                  \
    static bool auth_has_##name(const struct mds_catalogue *c)        \
    { return c->auth_ops != NULL && c->auth_ops->name != NULL; }
#define COORD_ACCESSOR(name, m, r, f)                                 \
    static bool coord_has_##name(const struct mds_catalogue *c)       \
    { return c->coord_ops != NULL && c->coord_ops->name != NULL; }
#define CLUSTER_ACCESSOR(name, m, r, f)                               \
    static bool cluster_has_##name(const struct mds_catalogue *c)     \
    { return c->cluster_ops != NULL && c->cluster_ops->name != NULL; }
#define LIFECYCLE_ACCESSOR(name, m, r, f)                             \
    static bool lifecycle_has_##name(const struct mds_catalogue *c)   \
    { return c->ops != NULL && c->ops->name != NULL; }

CONFORMANCE_AUTH_SLOTS(AUTH_ACCESSOR)
CONFORMANCE_COORD_SLOTS(COORD_ACCESSOR)
CONFORMANCE_CLUSTER_SLOTS(CLUSTER_ACCESSOR)
CONFORMANCE_LIFECYCLE_SLOTS(LIFECYCLE_ACCESSOR)

/* Which expected column applies to the backend under test. */
enum matrix_column {
    COL_MEMDB,
    COL_RONDB,
    COL_FDB,
};

struct slot_row {
    const char *table;
    const char *slot;
    bool (*present)(const struct mds_catalogue *cat);
    bool expect_memdb;
    bool expect_rondb;
    bool expect_fdb;
};

#define AUTH_ROW(name, m, r, f)      { "auth",      #name, auth_has_##name, m, r, f },
#define COORD_ROW(name, m, r, f)     { "coord",     #name, coord_has_##name, m, r, f },
#define CLUSTER_ROW(name, m, r, f)   { "cluster",   #name, cluster_has_##name, m, r, f },
#define LIFECYCLE_ROW(name, m, r, f) { "lifecycle", #name, lifecycle_has_##name, m, r, f },

static const struct slot_row auth_rows[] = {
    CONFORMANCE_AUTH_SLOTS(AUTH_ROW)
};
static const struct slot_row coord_rows[] = {
    CONFORMANCE_COORD_SLOTS(COORD_ROW)
};
static const struct slot_row cluster_rows[] = {
    CONFORMANCE_CLUSTER_SLOTS(CLUSTER_ROW)
};
static const struct slot_row lifecycle_rows[] = {
    CONFORMANCE_LIFECYCLE_SLOTS(LIFECYCLE_ROW)
};

#define ROWS(a) (sizeof(a) / sizeof((a)[0]))

/* Every vtable struct is made of function pointers only, so the slot
 * count is sizeof(struct) / sizeof(pointer).  A new slot without a
 * matrix row fails to compile here. */
typedef void (*slot_fn_t)(void);
_Static_assert(ROWS(auth_rows) ==
               sizeof(struct mds_authority_ops) / sizeof(slot_fn_t),
               "slot matrix: authority table row count mismatch");
_Static_assert(ROWS(coord_rows) ==
               sizeof(struct mds_coordination_ops) / sizeof(slot_fn_t),
               "slot matrix: coordination table row count mismatch");
_Static_assert(ROWS(cluster_rows) ==
               sizeof(struct mds_cluster_ops) / sizeof(slot_fn_t),
               "slot matrix: cluster table row count mismatch");
_Static_assert(ROWS(lifecycle_rows) ==
               sizeof(struct mds_catalogue_ops) / sizeof(slot_fn_t),
               "slot matrix: lifecycle table row count mismatch");

/* -----------------------------------------------------------------------
 * Matrix evaluation
 * ----------------------------------------------------------------------- */

static unsigned g_rows;
static unsigned g_failed;

static const char *presence(bool p)
{
    return p ? "present" : "absent";
}

static bool expected_of(const struct slot_row *row, enum matrix_column col)
{
    if (col == COL_MEMDB) {
        return row->expect_memdb;
    }
    if (col == COL_RONDB) {
        return row->expect_rondb;
    }
    return row->expect_fdb;
}

static void check_rows(const struct mds_catalogue *cat, enum matrix_column col,
                       const struct slot_row *rows, size_t n)
{
    size_t i;

    for (i = 0; i < n; i++) {
        bool expect = expected_of(&rows[i], col);
        bool actual = rows[i].present(cat);

        g_rows++;
        if (expect == actual) {
            (void)printf("  PASS %s.%s %s\n", rows[i].table, rows[i].slot,
                         presence(actual));
        } else {
            g_failed++;
            (void)printf("  FAIL %s.%s expected=%s actual=%s\n",
                         rows[i].table, rows[i].slot, presence(expect),
                         presence(actual));
        }
    }
}

static void check_value(const char *what, unsigned long expect,
                        unsigned long actual)
{
    g_rows++;
    if (expect == actual) {
        (void)printf("  PASS %s = %lu\n", what, actual);
    } else {
        g_failed++;
        (void)printf("  FAIL %s expected=%lu actual=%lu\n", what, expect,
                     actual);
    }
}

int main(void)
{
    struct mds_catalogue *cat;
    enum matrix_column col;
    enum mds_catalogue_backend expect_type;
    uint32_t expect_caps;
    bool expect_cluster;
    bool expect_shared;

    if (conformance_backend_is("memdb")) {
        col = COL_MEMDB;
        expect_type = MDS_BACKEND_MEMDB;
        expect_caps = CONFORMANCE_CAPS_MEMDB;
        expect_cluster = CONFORMANCE_CLUSTER_SUPPORTED_MEMDB;
        expect_shared = CONFORMANCE_SHARED_STATE_SUPPORTED_MEMDB;
    } else if (conformance_backend_is("rondb")) {
        col = COL_RONDB;
        expect_type = MDS_BACKEND_RONDB;
        expect_caps = CONFORMANCE_CAPS_RONDB;
        expect_cluster = CONFORMANCE_CLUSTER_SUPPORTED_RONDB;
        expect_shared = CONFORMANCE_SHARED_STATE_SUPPORTED_RONDB;
    } else if (conformance_backend_is("fdb")) {
        col = COL_FDB;
        expect_type = MDS_BACKEND_FDB;
        expect_caps = CONFORMANCE_CAPS_FDB;
        expect_cluster = CONFORMANCE_CLUSTER_SUPPORTED_FDB;
        expect_shared = CONFORMANCE_SHARED_STATE_SUPPORTED_FDB;
    } else {
        conformance_skip("no expected slot table for this backend");
    }

    cat = conformance_open_checked();
    (void)printf("slot_matrix (backend=%s):\n", conformance_backend_name());

    check_value("backend_type", (unsigned long)expect_type,
                (unsigned long)mds_catalogue_backend_type(cat));
    check_rows(cat, col, auth_rows, ROWS(auth_rows));
    check_rows(cat, col, coord_rows, ROWS(coord_rows));
    check_rows(cat, col, cluster_rows, ROWS(cluster_rows));
    check_rows(cat, col, lifecycle_rows, ROWS(lifecycle_rows));
    check_value("caps", (unsigned long)expect_caps,
                (unsigned long)cat->caps);
    check_value("mds_cluster_supported", expect_cluster ? 1UL : 0UL,
                mds_cluster_supported(cat) ? 1UL : 0UL);
    check_value("mds_coord_shared_state_supported", expect_shared ? 1UL : 0UL,
                mds_coord_shared_state_supported(cat) ? 1UL : 0UL);

    mds_catalogue_close(cat);

    (void)printf("\nslot_matrix: %u/%u rows match\n", g_rows - g_failed,
                 g_rows);
    conformance_shutdown();
    return (g_failed == 0) ? 0 : 1;
}

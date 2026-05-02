/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_migration.c — Unit tests for subtree migration.
 *
 * RonDB-native: tests migration chunk serialization (backend-neutral)
 * and migration state enum values.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "migration.h"

static int tests_run, tests_passed;

#define ASSERT_EQ(a, b) do { \
    if ((a) != (b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%d) != %s (%d)\n", \
                __FILE__, __LINE__, #a, (int)(a), #b, (int)(b)); \
        return; \
    } \
} while (0)
#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", __FILE__, __LINE__, #cond); \
        return; \
    } \
} while (0)
#define RUN_TEST(fn) do { \
    tests_run++; fprintf(stdout, "  %-50s", #fn); fflush(stdout); \
    fn(); tests_passed++; fprintf(stdout, "PASS\n"); \
} while (0)

/** Test chunk serialization round-trip (no DB needed). */
static void test_chunk_serialize_roundtrip(void)
{
    struct mig_inode_chunk chunk;
    memset(&chunk, 0, sizeof(chunk));
    chunk.fileid = 42;
    chunk.inode.fileid = 42;
    chunk.inode.type = MDS_FTYPE_REG;
    chunk.inode.mode = 0644;
    chunk.inode.nlink = 1;

    void *buf = NULL;
    size_t len = 0;
    ASSERT_EQ(mig_chunk_serialise(&chunk, &buf, &len), 0);
    ASSERT_TRUE(buf != NULL);
    ASSERT_TRUE(len > 0);

    struct mig_inode_chunk got;
    memset(&got, 0, sizeof(got));
    ASSERT_EQ(mig_chunk_deserialise(buf, len, &got), 0);
    ASSERT_EQ(got.fileid, (uint64_t)42);
    ASSERT_EQ((int)got.inode.type, (int)MDS_FTYPE_REG);

    free(buf);
}

/** Test migration state enum values are distinct. */
static void test_migration_states(void)
{
    ASSERT_TRUE(MIG_IDLE != MIG_FREEZING);
    ASSERT_TRUE(MIG_FREEZING != MIG_STREAMING);
    ASSERT_TRUE(MIG_STREAMING != MIG_COMMITTING);
    ASSERT_TRUE(MIG_DONE != MIG_FAILED);
}

int main(void)
{
    fprintf(stdout, "test_migration (RonDB-native)\n");

    RUN_TEST(test_chunk_serialize_roundtrip);
    RUN_TEST(test_migration_states);

    fprintf(stdout, "\n  %d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}

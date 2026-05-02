/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_delegation.c — file-delegation table unit tests.
 *
 * Focused on deleg_revoke_file(): the leak-plug for the
 * open/unlink workload that retains struct deleg_entry once
 * the underlying file has been unlinked.  Also exercises
 * the existing per-client revoke for parity.  CB_RECALL is
 * not exercised here (no session attached); we verify only
 * the in-memory bookkeeping.
 *
 * The table is configured with skip_transient_ndb=true so no
 * RonDB calls are issued; the in-memory hash table is the
 * single source of truth for these tests.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include "pnfs_mds.h"
#include "delegation.h"

static int tests_run;
static int tests_passed;

#define ASSERT_EQ(a, b) do { \
    if ((long long)(a) != (long long)(b)) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n", \
                __FILE__, __LINE__, #a, (long long)(a), \
                #b, (long long)(b)); \
        return; \
    } \
} while (0)

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", \
                __FILE__, __LINE__, #cond); \
        return; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    tests_run++; \
    fprintf(stdout, "  %-50s", #fn); \
    fflush(stdout); \
    fn(); \
    tests_passed++; \
    fprintf(stdout, "PASS\n"); \
} while (0)

/** NULL handle is a safe no-op. */
static void test_revoke_file_null_safe(void)
{
    deleg_revoke_file(NULL, 12345);
}

/** Empty table: revoking any fileid leaves the table consistent. */
static void test_revoke_file_empty(void)
{
    struct deleg_table *dt = NULL;
    uint8_t probe[12] = {0};

    ASSERT_EQ(deleg_table_init(7, &dt), 0);
    ASSERT_TRUE(dt != NULL);
    deleg_table_set_skip_transient(dt, true);

    deleg_revoke_file(dt, 1);
    deleg_revoke_file(dt, 0);

    ASSERT_TRUE(!deleg_stateid_exists(dt, probe));

    deleg_table_destroy(dt);
}

/**
 * Drop only the targeted fileid; grants for other files survive.
 *
 * Layout:
 *   sid_a1 — clientA / file1 / READ
 *   sid_b1 — clientB / file1 / READ   (same file, different client)
 *   sid_a2 — clientA / file2 / WRITE
 *   sid_a3 — clientA / file3 / READ
 *
 * After deleg_revoke_file(file1): a1 and b1 must be gone, a2/a3 must
 * survive.  After a second deleg_revoke_file(file2): only a3 remains.
 */
static void test_revoke_file_targets_correct_grants(void)
{
    struct deleg_table *dt = NULL;
    struct nfs4_stateid sid_a1 = {0};
    struct nfs4_stateid sid_b1 = {0};
    struct nfs4_stateid sid_a2 = {0};
    struct nfs4_stateid sid_a3 = {0};
    const uint64_t client_a = 100;
    const uint64_t client_b = 200;
    const uint64_t file1 = 0xAAAAAAAAULL;
    const uint64_t file2 = 0xBBBBBBBBULL;
    const uint64_t file3 = 0xCCCCCCCCULL;

    ASSERT_EQ(deleg_table_init(1, &dt), 0);
    deleg_table_set_skip_transient(dt, true);

    ASSERT_EQ(deleg_grant(dt, client_a, file1,
                          OPEN_DELEGATE_READ,  NULL, &sid_a1), 0);
    ASSERT_EQ(deleg_grant(dt, client_b, file1,
                          OPEN_DELEGATE_READ,  NULL, &sid_b1), 0);
    ASSERT_EQ(deleg_grant(dt, client_a, file2,
                          OPEN_DELEGATE_WRITE, NULL, &sid_a2), 0);
    ASSERT_EQ(deleg_grant(dt, client_a, file3,
                          OPEN_DELEGATE_READ,  NULL, &sid_a3), 0);

    /* Sanity: all four grants are visible. */
    ASSERT_TRUE(deleg_stateid_exists(dt, sid_a1.other));
    ASSERT_TRUE(deleg_stateid_exists(dt, sid_b1.other));
    ASSERT_TRUE(deleg_stateid_exists(dt, sid_a2.other));
    ASSERT_TRUE(deleg_stateid_exists(dt, sid_a3.other));

    /* Revoke every grant against file1. */
    deleg_revoke_file(dt, file1);

    ASSERT_TRUE(!deleg_stateid_exists(dt, sid_a1.other));
    ASSERT_TRUE(!deleg_stateid_exists(dt, sid_b1.other));
    ASSERT_TRUE(deleg_stateid_exists(dt, sid_a2.other));
    ASSERT_TRUE(deleg_stateid_exists(dt, sid_a3.other));

    /* Idempotency: revoking the same fileid again is a no-op. */
    deleg_revoke_file(dt, file1);
    ASSERT_TRUE(!deleg_stateid_exists(dt, sid_a1.other));
    ASSERT_TRUE(deleg_stateid_exists(dt, sid_a2.other));

    /* Revoking a fileid that has no grants does not touch others. */
    deleg_revoke_file(dt, 0xDEADBEEFULL);
    ASSERT_TRUE(deleg_stateid_exists(dt, sid_a2.other));
    ASSERT_TRUE(deleg_stateid_exists(dt, sid_a3.other));

    /* Revoke file2 — only file3's grant should remain. */
    deleg_revoke_file(dt, file2);
    ASSERT_TRUE(!deleg_stateid_exists(dt, sid_a2.other));
    ASSERT_TRUE(deleg_stateid_exists(dt, sid_a3.other));

    /* Revoke file3 — table is empty. */
    deleg_revoke_file(dt, file3);
    ASSERT_TRUE(!deleg_stateid_exists(dt, sid_a3.other));

    deleg_table_destroy(dt);
}

/**
 * After revoke, deleg_check_conflict must report no conflict for
 * the freed fileid even when a different client tries to take a
 * delegation on it next.  This proves the entry is gone, not just
 * detached from one lookup path.
 */
static void test_revoke_file_clears_conflict(void)
{
    struct deleg_table *dt = NULL;
    struct nfs4_stateid sid = {0};
    const uint64_t client_a = 11;
    const uint64_t client_b = 22;
    const uint64_t fid = 4242;
    bool conflict = true;

    ASSERT_EQ(deleg_table_init(1, &dt), 0);
    deleg_table_set_skip_transient(dt, true);

    ASSERT_EQ(deleg_grant(dt, client_a, fid,
                          OPEN_DELEGATE_WRITE, NULL, &sid), 0);

    /* Before revoke: client B sees a conflict on this fileid. */
    ASSERT_EQ(deleg_check_conflict(dt, fid, client_b, &conflict), 0);
    ASSERT_TRUE(conflict);

    deleg_revoke_file(dt, fid);

    /* After revoke: no conflict, no grant. */
    conflict = true;
    ASSERT_EQ(deleg_check_conflict(dt, fid, client_b, &conflict), 0);
    ASSERT_TRUE(!conflict);
    ASSERT_TRUE(!deleg_stateid_exists(dt, sid.other));

    deleg_table_destroy(dt);
}

/**
 * Stress: load many grants across many fileids, revoke a single
 * fileid, and verify exactly the expected entries are gone.  This
 * also exercises hash collisions on the 4096-bucket table when N
 * is large enough.
 */
static void test_revoke_file_many_grants(void)
{
    struct deleg_table *dt = NULL;
    enum { N = 256 };
    struct nfs4_stateid sids[N];
    const uint64_t target_fid = 999999;
    struct nfs4_stateid target_sid = {0};
    int i;

    ASSERT_EQ(deleg_table_init(1, &dt), 0);
    deleg_table_set_skip_transient(dt, true);

    /* N distinct fileids, one grant each. */
    for (i = 0; i < N; i++) {
        memset(&sids[i], 0, sizeof(sids[i]));
        ASSERT_EQ(deleg_grant(dt, /*clientid*/ (uint64_t)(i + 1),
                              /*fileid*/   (uint64_t)(i + 1),
                              OPEN_DELEGATE_READ,
                              NULL, &sids[i]), 0);
    }
    /* Plus one grant on the target fileid, owned by yet another
     * client.  This is the only entry that should be removed. */
    ASSERT_EQ(deleg_grant(dt, /*clientid*/ 0xFFFFFFFFULL,
                          target_fid, OPEN_DELEGATE_WRITE,
                          NULL, &target_sid), 0);
    ASSERT_TRUE(deleg_stateid_exists(dt, target_sid.other));

    deleg_revoke_file(dt, target_fid);

    ASSERT_TRUE(!deleg_stateid_exists(dt, target_sid.other));
    /* All other grants survived. */
    for (i = 0; i < N; i++) {
        ASSERT_TRUE(deleg_stateid_exists(dt, sids[i].other));
    }

    deleg_table_destroy(dt);
}

int main(void)
{
    fprintf(stdout, "test_delegation:\n");

    RUN_TEST(test_revoke_file_null_safe);
    RUN_TEST(test_revoke_file_empty);
    RUN_TEST(test_revoke_file_targets_correct_grants);
    RUN_TEST(test_revoke_file_clears_conflict);
    RUN_TEST(test_revoke_file_many_grants);

    fprintf(stdout, "  %d/%d tests passed\n",
            tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}

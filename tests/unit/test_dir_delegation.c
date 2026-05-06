/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_dir_delegation.c -- Phase 8b unit tests for dir_deleg_table.
 *
 * Exercises grant / conflict check / recall / return / revoke /
 * stateid exists.  CB_RECALL delivery is not exercised (session
 * pointer is NULL, so dir_deleg_recall_dir does not call out to the
 * backchannel); we verify the table-side bookkeeping is correct.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pnfs_mds.h"
#include "compound.h"       /* NOTIFY4_* */
#include "dir_delegation.h"

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

/** Initial state: no grants, counters zero, no writer, no stateid. */
static void test_ddt_empty(void)
{
    struct dir_deleg_table *ddt = NULL;
    struct dir_deleg_counters c;
    uint8_t probe[12] = {0};

    ASSERT_EQ(dir_deleg_table_init(1, &ddt), 0);
    ASSERT_TRUE(ddt != NULL);

    ASSERT_TRUE(!dir_deleg_is_writer_present(ddt, 100, 42));
    ASSERT_TRUE(!dir_deleg_stateid_exists(ddt, probe));

    dir_deleg_counters_snapshot(ddt, &c);
    ASSERT_EQ(c.granted, 0);
    ASSERT_EQ(c.recalled, 0);
    ASSERT_EQ(c.revoked, 0);
    ASSERT_EQ(c.returned, 0);

    dir_deleg_table_destroy(ddt);
}

/** Grant -> stateid_exists -> recall by *other* client -> grant gone. */
static void test_ddt_grant_and_recall(void)
{
    struct dir_deleg_table *ddt = NULL;
    struct nfs4_stateid sid = {0};
    uint8_t cookieverf[NFS4_VERIFIER_SIZE] = {0};
    const uint64_t dir_fid = 12345;
    const uint64_t client_a = 11;
    const uint64_t client_b = 22;
    struct dir_deleg_counters c;

    ASSERT_EQ(dir_deleg_table_init(1, &ddt), 0);

    ASSERT_EQ(dir_deleg_grant(ddt, client_a, dir_fid, 0, NULL,
                              &sid, cookieverf), 0);
    ASSERT_TRUE(dir_deleg_stateid_exists(ddt, sid.other));

    /* Same client never conflicts with its own grant. */
    ASSERT_TRUE(!dir_deleg_is_writer_present(ddt, dir_fid, client_a));
    /* A different client sees the grant as a writer. */
    ASSERT_TRUE(dir_deleg_is_writer_present(ddt, dir_fid, client_b));

    /* Client B does a conflicting mutation: recall_dir on behalf of
     * client B must revoke client A's grant and not touch any
     * grants owned by client B (there are none). */
    int n = dir_deleg_recall_dir(ddt, dir_fid, client_b, 50);
    ASSERT_EQ(n, 1);

    ASSERT_TRUE(!dir_deleg_stateid_exists(ddt, sid.other));
    ASSERT_TRUE(!dir_deleg_is_writer_present(ddt, dir_fid, client_b));

    dir_deleg_counters_snapshot(ddt, &c);
    ASSERT_EQ(c.granted, 1);
    ASSERT_EQ(c.recalled, 1);
    ASSERT_TRUE(c.revoked >= 1);

    dir_deleg_table_destroy(ddt);
}

/** DELEGRETURN: client voluntarily releases its own grant. */
static void test_ddt_return(void)
{
    struct dir_deleg_table *ddt = NULL;
    struct nfs4_stateid sid = {0};
    uint8_t cookieverf[NFS4_VERIFIER_SIZE] = {0};
    const uint64_t dir_fid = 7777;
    const uint64_t cid = 33;
    struct dir_deleg_counters c;

    ASSERT_EQ(dir_deleg_table_init(1, &ddt), 0);
    ASSERT_EQ(dir_deleg_grant(ddt, cid, dir_fid, 0, NULL,
                              &sid, cookieverf), 0);

    ASSERT_EQ(dir_deleg_return(ddt, &sid, cid), 0);
    ASSERT_TRUE(!dir_deleg_stateid_exists(ddt, sid.other));

    /* Double-return is a no-op per RFC 8881 S10.11.1 \u2014 the state
     * is gone, so return() returns -1 which the caller swallows. */
    ASSERT_EQ(dir_deleg_return(ddt, &sid, cid), -1);

    dir_deleg_counters_snapshot(ddt, &c);
    ASSERT_EQ(c.returned, 1);

    dir_deleg_table_destroy(ddt);
}

/** revoke_client removes every grant held by that client. */
static void test_ddt_revoke_client(void)
{
    struct dir_deleg_table *ddt = NULL;
    struct nfs4_stateid sid_a1 = {0}, sid_a2 = {0}, sid_b = {0};
    uint8_t cv[NFS4_VERIFIER_SIZE] = {0};
    const uint64_t cid_a = 100;
    const uint64_t cid_b = 200;

    ASSERT_EQ(dir_deleg_table_init(1, &ddt), 0);
    ASSERT_EQ(dir_deleg_grant(ddt, cid_a, 1, 0, NULL, &sid_a1, cv), 0);
    ASSERT_EQ(dir_deleg_grant(ddt, cid_a, 2, 0, NULL, &sid_a2, cv), 0);
    ASSERT_EQ(dir_deleg_grant(ddt, cid_b, 3, 0, NULL, &sid_b,  cv), 0);

    dir_deleg_revoke_client(ddt, cid_a);

    ASSERT_TRUE(!dir_deleg_stateid_exists(ddt, sid_a1.other));
    ASSERT_TRUE(!dir_deleg_stateid_exists(ddt, sid_a2.other));
    /* Client B's grant survives. */
    ASSERT_TRUE(dir_deleg_stateid_exists(ddt, sid_b.other));

    dir_deleg_table_destroy(ddt);
}

/**
 * notify_dir: event not covered by granted mask falls back to
 * recall.  No session attached, so CB_RECALL is silent; the grant
 * is still revoked and the recalled counter bumped.
 */
static void test_ddt_notify_falls_back_to_recall(void)
{
    struct dir_deleg_table *ddt = NULL;
    struct nfs4_stateid sid = {0};
    uint8_t cv[NFS4_VERIFIER_SIZE] = {0};
    const uint64_t dir_fid = 500;
    const uint64_t client_a = 10;
    const uint64_t client_b = 20;
    struct dir_deleg_counters c;

    ASSERT_EQ(dir_deleg_table_init(1, &ddt), 0);
    /* client A subscribes to REMOVE only -- an ADD event will not
     * match the mask and must trigger the recall fallback. */
    ASSERT_EQ(dir_deleg_grant(ddt, client_a, dir_fid,
                              1u << NOTIFY4_REMOVE_ENTRY,
                              NULL, &sid, cv), 0);

    int n = dir_deleg_notify_dir(ddt, dir_fid, client_b,
                                 NOTIFY4_ADD_ENTRY, "new", NULL, 50);
    ASSERT_EQ(n, 1);

    ASSERT_TRUE(!dir_deleg_stateid_exists(ddt, sid.other));

    dir_deleg_counters_snapshot(ddt, &c);
    /* Falling back to recall bumps recalled and revoked, not notify_sent. */
    ASSERT_EQ(c.notify_sent, 0);
    ASSERT_TRUE(c.recalled >= 1);

    dir_deleg_table_destroy(ddt);
}

/**
 * notify_dir: same-client event is a no-op (caller is the mutator).
 */
static void test_ddt_notify_same_client_noop(void)
{
    struct dir_deleg_table *ddt = NULL;
    struct nfs4_stateid sid = {0};
    uint8_t cv[NFS4_VERIFIER_SIZE] = {0};
    const uint64_t dir_fid = 600;
    const uint64_t client_a = 1;

    ASSERT_EQ(dir_deleg_table_init(1, &ddt), 0);
    ASSERT_EQ(dir_deleg_grant(ddt, client_a, dir_fid,
                              1u << NOTIFY4_ADD_ENTRY,
                              NULL, &sid, cv), 0);

    int n = dir_deleg_notify_dir(ddt, dir_fid, client_a,
                                 NOTIFY4_ADD_ENTRY, "x", NULL, 50);
    ASSERT_EQ(n, 0);
    ASSERT_TRUE(dir_deleg_stateid_exists(ddt, sid.other));

    dir_deleg_table_destroy(ddt);
}

/**
 * notify_dir: unsupported event type (CHANGE_DIR_ATTRS in 8d) is
 * rejected with -1 -- Phase 8e will add attr payload support.
 */
static void test_ddt_notify_unsupported_event_rejected(void)
{
    struct dir_deleg_table *ddt = NULL;
    ASSERT_EQ(dir_deleg_table_init(1, &ddt), 0);
    ASSERT_EQ(dir_deleg_notify_dir(ddt, 1, 2,
                                   NOTIFY4_CHANGE_DIR_ATTRS,
                                   NULL, NULL, 0), -1);
    dir_deleg_table_destroy(ddt);
}

/** Same-client re-grant replaces the prior entry (fresh stateid). */
static void test_ddt_regrant_replaces(void)
{
    struct dir_deleg_table *ddt = NULL;
    struct nfs4_stateid sid1 = {0}, sid2 = {0};
    uint8_t cv1[NFS4_VERIFIER_SIZE] = {0};
    uint8_t cv2[NFS4_VERIFIER_SIZE] = {0};
    const uint64_t dir_fid = 42;
    const uint64_t cid = 1;

    ASSERT_EQ(dir_deleg_table_init(1, &ddt), 0);
    ASSERT_EQ(dir_deleg_grant(ddt, cid, dir_fid, 0, NULL, &sid1, cv1), 0);
    ASSERT_EQ(dir_deleg_grant(ddt, cid, dir_fid, 0, NULL, &sid2, cv2), 0);

    /* New stateid must differ from the first. */
    ASSERT_TRUE(memcmp(sid1.other, sid2.other, 12) != 0);
    /* Only the fresh stateid is present. */
    ASSERT_TRUE(!dir_deleg_stateid_exists(ddt, sid1.other));
    ASSERT_TRUE(dir_deleg_stateid_exists(ddt, sid2.other));

    dir_deleg_table_destroy(ddt);
}

int main(void)
{
    fprintf(stdout, "test_dir_delegation:\n");
    RUN_TEST(test_ddt_empty);
    RUN_TEST(test_ddt_grant_and_recall);
    RUN_TEST(test_ddt_return);
    RUN_TEST(test_ddt_revoke_client);
    RUN_TEST(test_ddt_regrant_replaces);
    RUN_TEST(test_ddt_notify_falls_back_to_recall);
    RUN_TEST(test_ddt_notify_same_client_noop);
    RUN_TEST(test_ddt_notify_unsupported_event_rejected);
    fprintf(stdout, "\n  %d/%d tests passed.\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_token_semantics.c -- Contract C6: the transaction token is a
 * grouping context (catalogue_internal.h, mds_catalogue.h).
 *
 *   1. A create issued under a token survives mds_cat_txn_abort():
 *      the inode and the dirent are still there.  A backend that
 *      rolled the create back would be implementing the retired
 *      comment, not the contract.
 *   2. mds_cat_txn_commit() only frees the token: nothing becomes
 *      visible that was not already visible.
 *   3. A non-NULL token never changes the result of an operation:
 *      the same sequence with txn == NULL and with a token yields the
 *      same statuses and the same attributes.
 *   4. Argument checks of the token API itself.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "harness.h"
#include "check.h"

static void test_abort_keeps_rows(struct mds_catalogue *cat, uint64_t dir)
{
    struct mds_cat_txn *txn = NULL;
    struct mds_inode child, seen;
    uint64_t fid = 0;
    uint8_t type = 0;

    REQUIRE_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    REQUIRE(txn != NULL);
    memset(&child, 0, sizeof(child));
    REQUIRE_EQ(mds_cat_ns_create(cat, txn, dir, "aborted", MDS_FTYPE_DIR,
                                 0755, 0, 0, NULL, &child), MDS_OK);
    mds_cat_txn_abort(txn);

    /* Inode and dirent are both still there. */
    CHECK_EQ(mds_cat_ns_getattr(cat, child.fileid, &seen), MDS_OK);
    CHECK_EQ(seen.fileid, child.fileid);
    CHECK_EQ(mds_cat_dirent_get(cat, dir, "aborted", &fid, &type), MDS_OK);
    CHECK_EQ(fid, child.fileid);
    CHECK_EQ(type, (uint8_t)MDS_FTYPE_DIR);
    CHECK_EQ(mds_cat_ns_lookup(cat, dir, "aborted", &seen), MDS_OK);
    CHECK_EQ(seen.fileid, child.fileid);

    /* Two writes under one token then abort: both committed (the
     * documented example in mds_catalogue.h). */
    REQUIRE_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    REQUIRE_EQ(mds_cat_ns_create(cat, txn, dir, "pair-a", MDS_FTYPE_DIR,
                                 0755, 0, 0, NULL, &child), MDS_OK);
    REQUIRE_EQ(mds_cat_ns_create(cat, txn, dir, "pair-b", MDS_FTYPE_DIR,
                                 0755, 0, 0, NULL, &child), MDS_OK);
    mds_cat_txn_abort(txn);
    CHECK_EQ(mds_cat_ns_lookup(cat, dir, "pair-a", &seen), MDS_OK);
    CHECK_EQ(mds_cat_ns_lookup(cat, dir, "pair-b", &seen), MDS_OK);
}

static void test_commit_only_frees(struct mds_catalogue *cat, uint64_t dir)
{
    struct mds_cat_txn *txn = NULL;
    struct mds_inode child, seen;

    REQUIRE_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    memset(&child, 0, sizeof(child));
    REQUIRE_EQ(mds_cat_ns_create(cat, txn, dir, "committed", MDS_FTYPE_DIR,
                                 0755, 0, 0, NULL, &child), MDS_OK);
    /* Visible before commit: the backend committed it on its own. */
    CHECK_EQ(mds_cat_ns_lookup(cat, dir, "committed", &seen), MDS_OK);
    CHECK_EQ(seen.fileid, child.fileid);
    CHECK_EQ(mds_cat_txn_commit(txn), MDS_OK);
    /* And unchanged after. */
    CHECK_EQ(mds_cat_ns_lookup(cat, dir, "committed", &seen), MDS_OK);
    CHECK_EQ(seen.fileid, child.fileid);

    /* A read-only token does not stop a write either. */
    REQUIRE_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_RDONLY, &txn), MDS_OK);
    REQUIRE_EQ(mds_cat_ns_create(cat, txn, dir, "rdonly-token",
                                 MDS_FTYPE_DIR, 0755, 0, 0, NULL, &child),
               MDS_OK);
    CHECK_EQ(mds_cat_txn_commit(txn), MDS_OK);
    CHECK_EQ(mds_cat_ns_lookup(cat, dir, "rdonly-token", &seen), MDS_OK);
}

/* Run one create/setattr/link/rename/remove sequence in @dir with the
 * given token and record everything observable about it. */
struct sequence_result {
    enum mds_status create_st;
    enum mds_status dup_create_st;
    enum mds_status setattr_st;
    enum mds_status link_st;
    enum mds_status rename_st;
    enum mds_status remove_st;
    enum mds_status lookup_after_st;
    uint32_t mode_after_setattr;
    uint64_t size_after_setattr;
    uint32_t nlink_after_link;
    uint64_t parent_change_delta;
    uint32_t parent_nlink_delta;
};

static void run_sequence(struct mds_catalogue *cat, uint64_t dir,
                         struct mds_cat_txn *txn, const char *tag,
                         struct sequence_result *r)
{
    struct mds_inode child, attrs, seen, parent_before, parent_after;
    char name[64], link_name[64], renamed[64];

    memset(r, 0, sizeof(*r));
    (void)snprintf(name, sizeof(name), "seq-%s", tag);
    (void)snprintf(link_name, sizeof(link_name), "seq-%s-link", tag);
    (void)snprintf(renamed, sizeof(renamed), "seq-%s-renamed", tag);

    if (mds_cat_ns_getattr(cat, dir, &parent_before) != MDS_OK) {
        memset(&parent_before, 0, sizeof(parent_before));
    }

    memset(&child, 0, sizeof(child));
    r->create_st = mds_cat_ns_create(cat, txn, dir, name, MDS_FTYPE_REG,
                                     0644, 10, 20, NULL, &child);
    r->dup_create_st = mds_cat_ns_create(cat, txn, dir, name, MDS_FTYPE_REG,
                                         0644, 10, 20, NULL, &seen);

    memset(&attrs, 0, sizeof(attrs));
    attrs.mode = 0600;
    attrs.size = 4096;
    r->setattr_st = mds_cat_ns_setattr(cat, txn, child.fileid, &attrs,
                                       MDS_ATTR_MODE | MDS_ATTR_SIZE);
    if (mds_cat_ns_getattr(cat, child.fileid, &seen) == MDS_OK) {
        r->mode_after_setattr = seen.mode;
        r->size_after_setattr = seen.size;
    }

    r->link_st = mds_cat_ns_link(cat, txn, dir, link_name, child.fileid);
    if (mds_cat_ns_getattr(cat, child.fileid, &seen) == MDS_OK) {
        r->nlink_after_link = seen.nlink;
    }

    r->rename_st = mds_cat_ns_rename(cat, txn, dir, name, dir, renamed);
    r->remove_st = mds_cat_ns_remove(cat, txn, dir, renamed);
    r->lookup_after_st = mds_cat_ns_lookup(cat, dir, renamed, &seen);
    (void)mds_cat_ns_remove(cat, txn, dir, link_name);

    if (mds_cat_ns_getattr(cat, dir, &parent_after) == MDS_OK) {
        r->parent_change_delta = parent_after.change - parent_before.change;
        r->parent_nlink_delta = parent_after.nlink - parent_before.nlink;
    }
}

static void test_token_does_not_change_results(struct mds_catalogue *cat,
                                               uint64_t dir)
{
    struct sequence_result plain, tokened;
    struct mds_cat_txn *txn = NULL;

    run_sequence(cat, dir, NULL, "plain", &plain);
    REQUIRE_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    run_sequence(cat, dir, txn, "token", &tokened);
    CHECK_EQ(mds_cat_txn_commit(txn), MDS_OK);

    /* The sequence itself must have worked (otherwise equality proves
     * nothing). */
    CHECK_EQ(plain.create_st, MDS_OK);
    CHECK_EQ(plain.dup_create_st, MDS_ERR_EXISTS);
    CHECK_EQ(plain.setattr_st, MDS_OK);
    CHECK_EQ(plain.link_st, MDS_OK);
    CHECK_EQ(plain.rename_st, MDS_OK);
    CHECK_EQ(plain.remove_st, MDS_OK);
    CHECK_EQ(plain.lookup_after_st, MDS_ERR_NOTFOUND);
    CHECK_EQ(plain.mode_after_setattr, 0600);
    CHECK_EQ(plain.size_after_setattr, 4096);
    CHECK_EQ(plain.nlink_after_link, 2);

    CHECK_EQ(tokened.create_st, plain.create_st);
    CHECK_EQ(tokened.dup_create_st, plain.dup_create_st);
    CHECK_EQ(tokened.setattr_st, plain.setattr_st);
    CHECK_EQ(tokened.link_st, plain.link_st);
    CHECK_EQ(tokened.rename_st, plain.rename_st);
    CHECK_EQ(tokened.remove_st, plain.remove_st);
    CHECK_EQ(tokened.lookup_after_st, plain.lookup_after_st);
    CHECK_EQ(tokened.mode_after_setattr, plain.mode_after_setattr);
    CHECK_EQ(tokened.size_after_setattr, plain.size_after_setattr);
    CHECK_EQ(tokened.nlink_after_link, plain.nlink_after_link);
    CHECK_EQ(tokened.parent_change_delta, plain.parent_change_delta);
    CHECK_EQ(tokened.parent_nlink_delta, plain.parent_nlink_delta);
}

static void test_token_api_arguments(struct mds_catalogue *cat)
{
    struct mds_cat_txn *txn = NULL;

    CHECK_EQ(mds_cat_txn_begin(NULL, MDS_CAT_TXN_WRITE, &txn),
             MDS_ERR_INVAL);
    CHECK_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, NULL),
             MDS_ERR_INVAL);
    CHECK_EQ(mds_cat_txn_commit(NULL), MDS_ERR_INVAL);
    mds_cat_txn_abort(NULL); /* must not crash */

    /* begin/abort and begin/commit pairs leak nothing (checked by the
     * sanitizer configure; see the conformance CMakeLists). */
    REQUIRE_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_WRITE, &txn), MDS_OK);
    mds_cat_txn_abort(txn);
    REQUIRE_EQ(mds_cat_txn_begin(cat, MDS_CAT_TXN_RDONLY, &txn), MDS_OK);
    CHECK_EQ(mds_cat_txn_commit(txn), MDS_OK);
}

int main(void)
{
    struct mds_catalogue *cat = conformance_open_checked();
    uint64_t dir = 0;
    int rc;

    (void)printf("test_token_semantics (backend=%s):\n",
                 conformance_backend_name());

    if (conformance_scratch_dir(cat, &dir) != MDS_OK) {
        (void)fprintf(stderr, "  FAIL: cannot create scratch directory\n");
        mds_catalogue_close(cat);
        return 1;
    }

    test_abort_keeps_rows(cat, dir);
    test_commit_only_frees(cat, dir);
    test_token_does_not_change_results(cat, dir);
    test_token_api_arguments(cat);

    conformance_scratch_cleanup(cat, dir);
    mds_catalogue_close(cat);
    rc = check_summary("test_token_semantics");
    conformance_shutdown();
    return rc;
}

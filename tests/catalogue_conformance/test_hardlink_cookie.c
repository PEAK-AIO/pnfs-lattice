/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_hardlink_cookie.c -- Phase 0b: hard-link-safe READDIR cookies.
 *
 * Two hard links to one inode in one directory, listed through
 * mds_cat_ns_readdir_plus_from_cookie() with a page size of one and
 * the resume cookie taken from the last delivered entry: both names
 * must come back exactly once, every cookie must be >= 3 and the
 * cookies must be strictly increasing across pages.  A backend that
 * derives the cookie from the child fileid hands out the same cookie
 * for both links, so the second page (cookie > first) drops one name.
 *
 * RonDB resumes over ix_dirents_parent_child and has no per-dirent
 * ordering column until its schema change lands, so there the walk
 * delivers one page with one of the two names and then drains.  That
 * shape is the documented deviation and is asserted as such (XFAIL,
 * exit 0); a RonDB run that delivers both names fails with a message
 * to drop the XFAIL, so the schema change cannot land unnoticed.
 * Exit 77 is left to the harness: a backend that is not built or not
 * reachable.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "harness.h"
#include "check.h"

#define MAX_PAGES 8

struct page {
    uint32_t count;
    uint64_t fileid;
    uint64_t cookie;
    char     name[MDS_MAX_NAME + 1];
};

static int page_cb(const struct mds_cat_dirent *entry,
                   const struct mds_inode *inode, bool inode_valid,
                   void *arg)
{
    struct page *p = arg;

    (void)inode;
    (void)inode_valid;
    if (p->count == 0) {
        p->fileid = entry->fileid;
        p->cookie = entry->cookie;
        (void)snprintf(p->name, sizeof(p->name), "%s", entry->name);
    }
    p->count++;
    return 0;
}

/* What the page-size-one walk over the two links delivered. */
struct walk {
    unsigned seen_a;
    unsigned seen_b;
    unsigned seen_other;
    unsigned pages;
};

/*
 * Create "a", link "b" to it and walk the directory one entry per
 * page.  The checks made here hold on every backend (page size, no
 * reserved cookie, strictly increasing cookies, the right fileid);
 * how many names the walk delivers is the backend-specific verdict
 * the caller draws from @p w.
 */
static void walk_two_links_page_size_one(struct mds_catalogue *cat,
                                         uint64_t dir, struct walk *w)
{
    struct mds_inode file, seen;
    uint64_t cookie = 0;
    uint64_t prev_cookie = 0;

    memset(w, 0, sizeof(*w));
    memset(&file, 0, sizeof(file));
    REQUIRE_EQ(mds_cat_ns_create(cat, NULL, dir, "a", MDS_FTYPE_REG, 0644,
                                 0, 0, NULL, &file), MDS_OK);
    REQUIRE_EQ(mds_cat_ns_link(cat, NULL, dir, "b", file.fileid), MDS_OK);
    REQUIRE_EQ(mds_cat_ns_getattr(cat, file.fileid, &seen), MDS_OK);
    CHECK_EQ(seen.nlink, 2);

    for (;;) {
        struct page p;

        memset(&p, 0, sizeof(p));
        REQUIRE_EQ(mds_cat_ns_readdir_plus_from_cookie(cat, dir, cookie, 1,
                                                       NULL, page_cb, &p),
                   MDS_OK);
        if (p.count == 0) {
            break; /* drained */
        }
        w->pages++;
        CHECK_EQ(p.count, 1);            /* page size honoured */
        CHECK(p.cookie >= 3);            /* never a reserved cookie */
        CHECK(p.cookie > prev_cookie);   /* strictly increasing */
        CHECK_EQ(p.fileid, file.fileid);
        if (strcmp(p.name, "a") == 0) {
            w->seen_a++;
        } else if (strcmp(p.name, "b") == 0) {
            w->seen_b++;
        } else {
            w->seen_other++;
        }
        prev_cookie = p.cookie;
        cookie = p.cookie;
        if (w->pages >= MAX_PAGES) {
            break; /* bounded: a broken cursor must not loop forever */
        }
    }
}

/* The contract: both names, each exactly once, on two pages. */
static void expect_both_links(const struct walk *w)
{
    CHECK_EQ(w->seen_a, 1);
    CHECK_EQ(w->seen_b, 1);
    CHECK_EQ(w->seen_other, 0);
    CHECK_EQ(w->pages, 2);
}

/*
 * XFAIL on RonDB: cookie = child fileid and the resume runs over
 * ix_dirents_parent_child, so the second page (cookie > first) finds
 * nothing and exactly one of the two names is delivered (documented
 * deviation, catalogue_internal.h ns_readdir_plus_from).  A walk that
 * delivers both names means the per-dirent sequence column / ordered
 * index schema change has landed: fail loudly so this XFAIL is dropped
 * and the contract above applies to RonDB too.
 */
static void expect_rondb_shared_cookie(const struct walk *w)
{
    if (w->pages == 2 && w->seen_a == 1 && w->seen_b == 1) {
        (void)fprintf(stderr, "  RonDB hard-link cookies now unique: drop "
                      "the XFAIL (expect_rondb_shared_cookie) and let the "
                      "contract check run on rondb\n");
    } else if (w->pages == 1 && w->seen_a + w->seen_b == 1) {
        (void)printf("  XFAIL: one page, one of two hard-link names "
                     "(RonDB shared cookie, documented deviation)\n");
    }
    CHECK_EQ(w->pages, 1);
    CHECK_EQ(w->seen_a + w->seen_b, 1);
    CHECK_EQ(w->seen_other, 0);
}

int main(void)
{
    struct mds_catalogue *cat;
    struct walk w;
    uint64_t dir = 0;
    int rc;

    cat = conformance_open_checked();
    (void)printf("test_hardlink_cookie (backend=%s):\n",
                 conformance_backend_name());

    if (conformance_scratch_dir(cat, &dir) != MDS_OK) {
        (void)fprintf(stderr, "  FAIL: cannot create scratch directory\n");
        mds_catalogue_close(cat);
        return 1;
    }

    walk_two_links_page_size_one(cat, dir, &w);
    if (conformance_backend_is("rondb")) {
        expect_rondb_shared_cookie(&w);
    } else {
        expect_both_links(&w);
    }

    conformance_scratch_cleanup(cat, dir);
    mds_catalogue_close(cat);
    rc = check_summary("test_hardlink_cookie");
    conformance_shutdown();
    return rc;
}

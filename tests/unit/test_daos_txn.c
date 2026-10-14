/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_daos_txn.c -- DAOS transaction runner (daos_txn.[ch]) against a
 * live pool, and the DAOS behaviours the backend's design assumes.
 *
 * The runner has no injectable call table (the DAOS client resolves
 * its own resends and the runner never probes a commit outcome), so
 * everything here runs for real: the conformance harness creates a
 * private container for the process (tests/catalogue_conformance/
 * harness.c), the tests drive daos_run_txn with scripted bodies on a
 * scratch INODE object no allocator can reach, and the binary exits 77
 * when the backend is not built, DAOS_POOL is unset or the pool is
 * unreachable.
 *
 * Pinned FIRST, each as its own test, because every slot of the
 * backend relies on them (daos_keys.h, daos_txn.h):
 *   dkeys_sorted          lexical dkeys of an entity-class object
 *                         enumerate in memcmp order, every key once,
 *                         across pages
 *   list_resume           daos_list_dkeys / daos_list_akeys resume
 *                         strictly after a key -- an existing one, a
 *                         non-existent one between two keys, one before
 *                         the first and one past the last -- and the
 *                         pages together are complete; akeys come back
 *                         in a stable (hash) order with the same strict
 *                         resume
 *   cond_akey_insert      DAOS_COND_AKEY_INSERT (per-akey) of a name that
 *                         exists fails with -DER_EXIST, through the
 *                         runner MDS_ERR_EXISTS, whether the client
 *                         reports it at the call or at the commit, and
 *                         the first value survives; two different names
 *                         under one dkey from two transactions both land;
 *                         the dirent primitive built on it behaves the
 *                         same
 *   fetch_absent_rec2big  an absent akey fetches as size 0 / not found;
 *                         a buffer smaller than the value is -DER_REC2BIG
 *                         (the size the client reported is printed, not
 *                         asserted) and a retry with a large enough
 *                         buffer returns the value
 * Then the runner:
 *   restart               -DER_TX_RESTART from the body re-runs it on the
 *                         same handle; one commit, one effect
 *   transient retry       a transient operation error retries on a
 *                         fresh transaction and commits
 *   deadline              a body that keeps failing transiently ends in
 *                         MDS_ERR_DELAY (never INDOUBT) when the deadline
 *                         is exhausted
 *   classification        -DER_IO / -DER_NONEXIST / -DER_EXIST /
 *                         -DER_NOSPACE / -DER_NO_PERM and a stray positive
 *                         code map to IO / NOTFOUND / EXISTS / NOSPC /
 *                         PERM / IO
 *   kinds                 read-only bodies never commit, a NONE body runs
 *                         without a handle and its single update lands
 *                         at once, a mutating DAOS_BODY_DONE body commits
 *                         nothing even though it buffered a write
 *   alloc_id              batches of two handles on one thread never
 *                         overlap, a switch discards the remainder, the
 *                         schema key is refused
 *   client_stats          backend_client_stats reports the runner's
 *                         counters and moves with a read and a write
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pnfs_mds.h"

#ifdef HAVE_DAOS

#include <daos.h>

#include "catalogue_daos.h"
#include "catalogue_daos_internal.h"
#include "catalogue_internal.h"
#include "daos_codec.h"
#include "daos_keys.h"
#include "daos_txn.h"
#include "fdb_codec.h"
#include "harness.h"
#include "mds_catalogue.h"

static int tests_run;
static int tests_passed;
static int test_failed;

#define ASSERT_TRUE(cond) do { \
    if (!(cond)) { \
        fprintf(stderr, "  FAIL %s:%d: !(%s)\n", __FILE__, __LINE__, #cond); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define ASSERT_EQ(a, b) do { \
    long long _a = (long long)(a); \
    long long _b = (long long)(b); \
    if (_a != _b) { \
        fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n", \
                __FILE__, __LINE__, #a, _a, #b, _b); \
        test_failed = 1; \
        return; \
    } \
} while (0)

#define RUN_TEST(fn) do { \
    fprintf(stdout, "  %-48s", #fn); \
    fflush(stdout); \
    tests_run++; \
    test_failed = 0; \
    fn(); \
    if (test_failed == 0) { \
        tests_passed++; \
        fprintf(stdout, "ok\n"); \
    } else { \
        fprintf(stdout, "FAILED\n"); \
    } \
} while (0)

/* The handle under test (self.id 3) and a second handle of the same
 * container (self.id 4) for the allocator test. */
#define TEST_MDS_ID   3U
#define TEST_MDS_ID_B 4U

static struct mds_catalogue *g_cat;
static struct mds_catalogue *g_cat2;

/* Scratch INODE object: a fileid the allocator never reaches (it hands
 * out MDS_FILEID_ROOT + 1 onwards in batches of MDS_FILEID_BATCH), so
 * the pins may write any dkeys they like without meeting a real inode. */
#define PIN_FILEID 0x7E570000DA050001ULL

/* Lexical scratch dkeys "k00".."k39"; 7 is coprime to 40, so writing
 * key (i * 7) % 40 at step i is a permutation (the store, not the
 * writer, produces the sorted order).  Scratch akeys "n00".."n29" of
 * the 'N' dkey; 11 is coprime to 30. */
#define PIN_DKEYS     40U
#define PIN_DKEY_STEP 7U
#define PIN_AKEYS     30U
#define PIN_AKEY_STEP 11U
#define PIN_KEYS_MAX  64U
#define PIN_KEY_CAP   16U
#define LIST_PAGE     8U

static struct daos_backend *be(void)
{
    return g_cat->backend_private;
}

/* -----------------------------------------------------------------------
 * Key helpers
 * ----------------------------------------------------------------------- */

static int pin_obj(struct daos_txn *t, daos_handle_t *oh)
{
    return daos_txn_obj(t, DAOS_TAG_INODE, PIN_FILEID, oh);
}

static void key_str(struct daos_key *k, const char *s)
{
    daos_key_init(k);
    daos_key_bytes(k, s, strlen(s));
}

static void key_lex(struct daos_key *k, unsigned i)
{
    char name[PIN_KEY_CAP];

    (void)snprintf(name, sizeof(name), "k%02u", i);
    key_str(k, name);
}

static void key_akey(struct daos_key *k, unsigned i)
{
    char name[PIN_KEY_CAP];

    (void)snprintf(name, sizeof(name), "n%02u", i);
    key_str(k, name);
}

/* -----------------------------------------------------------------------
 * Pin bodies
 * ----------------------------------------------------------------------- */

/* Write the PIN_DKEYS lexical dkeys (akey 'v' = marker) or the
 * PIN_AKEYS 'N' akeys (marker values), each set in permuted order. */
struct write_ctx {
    bool akeys;
};

static int pin_write_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct write_ctx *c = arg;
    daos_handle_t oh;
    unsigned n = c->akeys ? PIN_AKEYS : PIN_DKEYS;
    unsigned step = c->akeys ? PIN_AKEY_STEP : PIN_DKEY_STEP;
    unsigned i;
    int rc;

    rc = pin_obj(t, &oh);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < n; i++) {
        struct daos_key dk;
        struct daos_key ak;
        uint8_t v = DAOS_MARKER_BYTE;

        if (c->akeys) {
            daos_dkey_inode(&dk, DAOS_INODE_DKEY_DIRENTS);
            key_akey(&ak, (i * step) % n);
        } else {
            key_lex(&dk, (i * step) % n);
            daos_akey_value(&ak);
        }
        rc = daos_update_single(t, oh, &dk, &ak, &v, sizeof(v), 0, 0);
        if (rc != 0) {
            return rc;
        }
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* List the scratch dkeys (or the 'N' akeys) in pages of LIST_PAGE,
 * resuming strictly after the last key of each page, from @p resume
 * (NULL: the first key).  Keys are collected as strings. */
struct list_ctx {
    const char *resume;
    bool        akeys;
    bool        one_page;   /* Stop after the first page. */
    unsigned    pages;
    unsigned    n;
    bool        eof;
    char        keys[PIN_KEYS_MAX][PIN_KEY_CAP];
};

static struct list_ctx g_list;

static int pin_list_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct list_ctx *c = arg;
    daos_key_desc_t kds[LIST_PAGE];
    uint8_t buf[LIST_PAGE * DAOS_KEY_MAX];
    struct daos_key resume;
    struct daos_key dk;
    struct daos_key *rs = NULL;
    daos_handle_t oh;
    int rc;

    c->n = 0;
    c->pages = 0;
    c->eof = false;
    rc = pin_obj(t, &oh);
    if (rc != 0) {
        return rc;
    }
    if (c->resume != NULL) {
        key_str(&resume, c->resume);
        rs = &resume;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_DIRENTS);
    for (;;) {
        struct daos_list l;
        uint32_t i;

        daos_list_init(&l, kds, LIST_PAGE, buf, sizeof(buf));
        rc = c->akeys ? daos_list_akeys(t, oh, &dk, rs, &l) : daos_list_dkeys(t, oh, rs, &l);
        if (rc != 0) {
            return rc;
        }
        c->pages++;
        for (i = 0; i < l.nr; i++) {
            const uint8_t *key = NULL;
            size_t len = 0;

            if (!daos_list_key(&l, i, &key, &len) || len >= PIN_KEY_CAP ||
                c->n >= PIN_KEYS_MAX) {
                *st_out = MDS_ERR_IO;
                return DAOS_BODY_DONE;
            }
            memcpy(c->keys[c->n], key, len);
            c->keys[c->n][len] = '\0';
            c->n++;
        }
        c->eof = l.eof;
        if (l.eof || l.nr == 0 || c->one_page) {
            break;
        }
        key_str(&resume, c->keys[c->n - 1]);
        rs = &resume;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* Run a listing from @p resume; false when the runner failed. */
static bool pin_list(const char *resume, bool akeys, bool one_page)
{
    memset(&g_list, 0, sizeof(g_list));
    g_list.resume = resume;
    g_list.akeys = akeys;
    g_list.one_page = one_page;
    return daos_run_txn(be(), DAOS_TXN_READONLY, "pin_list", pin_list_body, &g_list) == MDS_OK;
}

/* Insert @p name under 'N' with @p value (8 bytes) and the per-akey
 * condition @p cond. */
struct cond_ctx {
    const char *name;
    uint64_t    cond;
    uint64_t    value;
};

static int pin_cond_insert_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct cond_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    uint8_t v[8];
    int rc;

    rc = pin_obj(t, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_inode(&dk, DAOS_INODE_DKEY_DIRENTS);
    daos_akey_name(&ak, c->name);
    fdb_le64_put(v, c->value);
    rc = daos_update_single(t, oh, &dk, &ak, v, sizeof(v), 0, c->cond);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* The composed primitive CREATE uses: 'N' akey (insert-only) + 'S'+seq. */
struct dirent_ctx {
    const char *name;
    uint64_t    seq;
};

static int pin_dirent_write_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct dirent_ctx *c = arg;
    daos_handle_t oh;
    int rc;

    rc = pin_obj(t, &oh);
    if (rc != 0) {
        return rc;
    }
    rc = daos_dirent_write(t, oh, c->name, 4242, (uint8_t)MDS_FTYPE_REG, c->seq, true);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* One fetch of (dkey, akey) into a buffer of @p cap bytes; the raw
 * result is handed back for the test to judge (0 and -DER_REC2BIG end
 * the body with DAOS_BODY_DONE / MDS_OK, anything else is returned to
 * the runner for classification). */
struct fetch_ctx {
    const char *dkey;      /* NULL: the 'N' dkey. */
    const char *akey;      /* NULL: the 'v' akey. */
    size_t      cap;
    int         rc;
    size_t      len;
    bool        found;
    uint8_t     buf[128];
};

static int pin_fetch_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct fetch_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    int rc;

    rc = pin_obj(t, &oh);
    if (rc != 0) {
        return rc;
    }
    if (c->dkey != NULL) {
        key_str(&dk, c->dkey);
    } else {
        daos_dkey_inode(&dk, DAOS_INODE_DKEY_DIRENTS);
    }
    if (c->akey != NULL) {
        daos_akey_name(&ak, c->akey);
    } else {
        daos_akey_value(&ak);
    }
    memset(c->buf, 0, sizeof(c->buf));
    c->rc = daos_fetch_single(t, oh, &dk, &ak, c->buf, c->cap, &c->len, &c->found);
    if (c->rc != 0 && c->rc != -DER_REC2BIG) {
        return c->rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_DONE;
}

static bool pin_fetch(struct fetch_ctx *c)
{
    return daos_run_txn(be(), DAOS_TXN_READONLY, "pin_fetch", pin_fetch_body, c) == MDS_OK;
}

/* Write a 64-byte pattern under dkey "big", akey 'v'. */
static int pin_big_write_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    uint8_t v[64];
    unsigned i;
    int rc;

    (void)arg;
    rc = pin_obj(t, &oh);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < sizeof(v); i++) {
        v[i] = (uint8_t)(0xB0 + i);
    }
    key_str(&dk, "big");
    daos_akey_value(&ak);
    rc = daos_update_single(t, oh, &dk, &ak, v, sizeof(v), 0, 0);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* -----------------------------------------------------------------------
 * DAOS behaviour pins
 * ----------------------------------------------------------------------- */

static void test_dkeys_sorted(void)
{
    struct write_ctx w = { false };
    unsigned i;

    ASSERT_EQ(daos_run_txn(be(), DAOS_TXN_MUTATING, "pin_dkeys", pin_write_body, &w), MDS_OK);
    ASSERT_TRUE(pin_list(NULL, false, false));
    ASSERT_EQ(g_list.n, PIN_DKEYS);
    ASSERT_TRUE(g_list.eof);
    ASSERT_TRUE(g_list.pages >= PIN_DKEYS / LIST_PAGE);
    for (i = 0; i < g_list.n; i++) {
        char want[PIN_KEY_CAP];

        (void)snprintf(want, sizeof(want), "k%02u", i);
        ASSERT_EQ(strcmp(g_list.keys[i], want), 0);
        if (i > 0) {
            ASSERT_TRUE(strcmp(g_list.keys[i - 1], g_list.keys[i]) < 0);
        }
    }
}

/* Position of @p key in the hash-order listing @p all of @p n keys, or
 * n when absent. */
static unsigned key_position(char (*all)[PIN_KEY_CAP], unsigned n, const char *key)
{
    unsigned i;

    for (i = 0; i < n; i++) {
        if (strcmp(all[i], key) == 0) {
            return i;
        }
    }
    return n;
}

static void test_list_resume(void)
{
    struct write_ctx w = { true };
    char all[PIN_KEYS_MAX][PIN_KEY_CAP];
    uint8_t seen[PIN_AKEYS];
    unsigned n_all;
    unsigned pos;
    unsigned i;

    /* Dkeys, lexical order: resume after an existing key ... */
    ASSERT_TRUE(pin_list("k10", false, false));
    ASSERT_EQ(g_list.n, PIN_DKEYS - 11U);
    ASSERT_EQ(strcmp(g_list.keys[0], "k11"), 0);
    ASSERT_EQ(strcmp(g_list.keys[g_list.n - 1], "k39"), 0);
    /* ... after a key that does not exist but sorts between two ... */
    ASSERT_TRUE(pin_list("k105", false, false));
    ASSERT_EQ(g_list.n, PIN_DKEYS - 11U);
    ASSERT_EQ(strcmp(g_list.keys[0], "k11"), 0);
    /* ... before the first key, after the last key, past every key. */
    ASSERT_TRUE(pin_list("a", false, false));
    ASSERT_EQ(g_list.n, PIN_DKEYS);
    ASSERT_EQ(strcmp(g_list.keys[0], "k00"), 0);
    ASSERT_TRUE(pin_list("k39", false, false));
    ASSERT_EQ(g_list.n, 0);
    ASSERT_TRUE(g_list.eof);
    ASSERT_TRUE(pin_list("zzz", false, false));
    ASSERT_EQ(g_list.n, 0);
    ASSERT_TRUE(g_list.eof);
    /* A single page from the middle starts right after the key and is
     * consecutive.  Whether daos_obj_key2anchor positions at or after
     * the key is deliberately left unknown by the runner (it drops an
     * equal first key), so the page holds LIST_PAGE or LIST_PAGE - 1
     * keys; which one is printed for the lab, not asserted. */
    ASSERT_TRUE(pin_list("k20", false, true));
    ASSERT_TRUE(g_list.n == LIST_PAGE || g_list.n == LIST_PAGE - 1U);
    ASSERT_EQ(strcmp(g_list.keys[0], "k21"), 0);
    for (i = 1; i < g_list.n; i++) {
        char want[PIN_KEY_CAP];

        (void)snprintf(want, sizeof(want), "k%02u", 21U + i);
        ASSERT_EQ(strcmp(g_list.keys[i], want), 0);
    }
    (void)printf("[key2anchor resumes %s the key] ",
                 g_list.n == LIST_PAGE ? "after" : "at");

    /* Akeys: hashed, so the order is the store's, but it is a stable
     * total order with the same strict resume -- what ns_readdir and
     * xattr_list page on.  Every name once ... */
    ASSERT_EQ(daos_run_txn(be(), DAOS_TXN_MUTATING, "pin_akeys", pin_write_body, &w), MDS_OK);
    ASSERT_TRUE(pin_list(NULL, true, false));
    ASSERT_EQ(g_list.n, PIN_AKEYS);
    ASSERT_TRUE(g_list.pages >= PIN_AKEYS / LIST_PAGE);
    memset(seen, 0, sizeof(seen));
    for (i = 0; i < g_list.n; i++) {
        char *end = NULL;
        unsigned long idx;

        ASSERT_EQ(g_list.keys[i][0], 'n');
        idx = strtoul(g_list.keys[i] + 1, &end, 10);
        ASSERT_TRUE(end != NULL && *end == '\0' && idx < PIN_AKEYS);
        seen[idx]++;
    }
    for (i = 0; i < PIN_AKEYS; i++) {
        ASSERT_EQ(seen[i], 1);
    }
    memcpy(all, g_list.keys, sizeof(all));
    n_all = g_list.n;
    /* ... and resuming after the key at position 10 yields exactly the
     * suffix of that order, across pages. */
    ASSERT_TRUE(pin_list(all[10], true, false));
    ASSERT_EQ(g_list.n, n_all - 11U);
    for (i = 0; i < g_list.n; i++) {
        ASSERT_EQ(strcmp(g_list.keys[i], all[11U + i]), 0);
    }
    /* After the last key: nothing.  After a name that does not exist:
     * a suffix of the same order (its hash position decides where),
     * never a repeat and never a gap. */
    ASSERT_TRUE(pin_list(all[n_all - 1], true, false));
    ASSERT_EQ(g_list.n, 0);
    ASSERT_TRUE(pin_list("nope", true, false));
    if (g_list.n > 0) {
        pos = key_position(all, n_all, g_list.keys[0]);
        ASSERT_TRUE(pos < n_all);
        ASSERT_EQ(g_list.n, n_all - pos);
        for (i = 0; i < g_list.n; i++) {
            ASSERT_EQ(strcmp(g_list.keys[i], all[pos + i]), 0);
        }
    }
}

static void test_cond_akey_insert(void)
{
    struct cond_ctx c = { "alpha", DAOS_COND_AKEY_INSERT, 0x1111 };
    struct dirent_ctx d = { "alpha2", 0 };
    struct fetch_ctx f;
    uint64_t commits;
    uint64_t io_errors;

    ASSERT_EQ(daos_run_txn(be(), DAOS_TXN_MUTATING, "cond_a", pin_cond_insert_body, &c), MDS_OK);
    /* The same name again: -DER_EXIST at the call or at the commit,
     * MDS_ERR_EXISTS either way, no commit, not an I/O error. */
    commits = atomic_load(&be()->stats.commits);
    io_errors = atomic_load(&be()->stats.io_errors);
    c.value = 0x2222;
    ASSERT_EQ(daos_run_txn(be(), DAOS_TXN_MUTATING, "cond_a2", pin_cond_insert_body, &c),
              MDS_ERR_EXISTS);
    ASSERT_EQ(atomic_load(&be()->stats.commits), commits);
    ASSERT_EQ(atomic_load(&be()->stats.io_errors), io_errors);
    /* A different name under the same dkey: the condition is per akey. */
    c.name = "beta";
    c.value = 0x3333;
    ASSERT_EQ(daos_run_txn(be(), DAOS_TXN_MUTATING, "cond_b", pin_cond_insert_body, &c), MDS_OK);
    /* Both present; alpha still carries the first value. */
    memset(&f, 0, sizeof(f));
    f.akey = "alpha";
    f.cap = sizeof(f.buf);
    ASSERT_TRUE(pin_fetch(&f));
    ASSERT_EQ(f.rc, 0);
    ASSERT_TRUE(f.found);
    ASSERT_EQ(f.len, 8);
    ASSERT_EQ(fdb_le64_get(f.buf), 0x1111);
    f.akey = "beta";
    ASSERT_TRUE(pin_fetch(&f));
    ASSERT_TRUE(f.found);
    ASSERT_EQ(fdb_le64_get(f.buf), 0x3333);
    /* An unconditional upsert of an existing name is allowed. */
    c.name = "alpha";
    c.cond = 0;
    c.value = 0x4444;
    ASSERT_EQ(daos_run_txn(be(), DAOS_TXN_MUTATING, "cond_up", pin_cond_insert_body, &c), MDS_OK);
    f.akey = "alpha";
    ASSERT_TRUE(pin_fetch(&f));
    ASSERT_EQ(fdb_le64_get(f.buf), 0x4444);

    /* The dirent primitive (insert-only 'N' akey + 'S'+seq row) built on
     * the same condition: once OK, twice EXISTS.  The cookie is minted
     * outside the body, as every slot does. */
    ASSERT_EQ(daos_backend_alloc_id(be(), DAOS_META_COOKIE_SEQ, &d.seq), MDS_OK);
    ASSERT_TRUE(d.seq >= 16U);
    ASSERT_EQ(daos_run_txn(be(), DAOS_TXN_MUTATING, "dirent_a", pin_dirent_write_body, &d),
              MDS_OK);
    ASSERT_EQ(daos_backend_alloc_id(be(), DAOS_META_COOKIE_SEQ, &d.seq), MDS_OK);
    ASSERT_EQ(daos_run_txn(be(), DAOS_TXN_MUTATING, "dirent_a2", pin_dirent_write_body, &d),
              MDS_ERR_EXISTS);
}

static void test_fetch_absent_rec2big(void)
{
    struct fetch_ctx f;
    unsigned i;

    ASSERT_EQ(daos_run_txn(be(), DAOS_TXN_MUTATING, "big", pin_big_write_body, NULL), MDS_OK);

    /* Too small a buffer: -DER_REC2BIG, not found, nothing copied that
     * the caller could mistake for the value.  Whether the client
     * reports the stored size with it is printed for the lab. */
    memset(&f, 0, sizeof(f));
    f.dkey = "big";
    f.cap = 8;
    ASSERT_TRUE(pin_fetch(&f));
    ASSERT_EQ(f.rc, -DER_REC2BIG);
    ASSERT_TRUE(!f.found);
    (void)printf("[REC2BIG reports size %zu] ", f.len);

    /* Absent akey under an existing dkey, and an absent dkey: size 0. */
    memset(&f, 0, sizeof(f));
    f.dkey = "big";
    f.akey = "absent";
    f.cap = sizeof(f.buf);
    ASSERT_TRUE(pin_fetch(&f));
    ASSERT_EQ(f.rc, 0);
    ASSERT_TRUE(!f.found);
    ASSERT_EQ(f.len, 0);
    memset(&f, 0, sizeof(f));
    f.dkey = "nodkey";
    f.cap = sizeof(f.buf);
    ASSERT_TRUE(pin_fetch(&f));
    ASSERT_EQ(f.rc, 0);
    ASSERT_TRUE(!f.found);
    ASSERT_EQ(f.len, 0);

    /* The retry with a large enough buffer returns the value intact. */
    memset(&f, 0, sizeof(f));
    f.dkey = "big";
    f.cap = sizeof(f.buf);
    ASSERT_TRUE(pin_fetch(&f));
    ASSERT_EQ(f.rc, 0);
    ASSERT_TRUE(f.found);
    ASSERT_EQ(f.len, 64);
    for (i = 0; i < 64; i++) {
        ASSERT_EQ(f.buf[i], (uint8_t)(0xB0 + i));
    }
}

/* -----------------------------------------------------------------------
 * Runner: scripted bodies over the scratch object's "runner" row
 * ----------------------------------------------------------------------- */

struct body_ctx {
    int             calls;
    int             fail_first_with;  /* -DER_* returned on the first call. */
    int             fail_always_with; /* -DER_* returned on every call. */
    int             raw_rc;           /* != 0: returned as is (a body bug). */
    enum mds_status done_status;      /* != MDS_OK: DAOS_BODY_DONE with it. */
    bool            write;            /* Blind update of the runner row first. */
    uint64_t        value;
    bool            saw_handle;       /* t->th valid on the last call. */
};

static int runner_row_write(struct daos_txn *t, uint64_t value)
{
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    uint8_t v[8];
    int rc;

    rc = pin_obj(t, &oh);
    if (rc != 0) {
        return rc;
    }
    key_str(&dk, "runner");
    daos_akey_value(&ak);
    fdb_le64_put(v, value);
    return daos_update_single(t, oh, &dk, &ak, v, sizeof(v), 0, 0);
}

static int body_fn(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct body_ctx *c = arg;

    c->calls++;
    c->saw_handle = daos_handle_is_valid(t->th);
    if (c->write) {
        int rc = runner_row_write(t, c->value);

        if (rc != 0) {
            return rc;
        }
    }
    if (c->fail_always_with != 0) {
        return c->fail_always_with;
    }
    if (c->calls == 1 && c->fail_first_with != 0) {
        return c->fail_first_with;
    }
    if (c->raw_rc != 0) {
        return c->raw_rc;
    }
    if (c->done_status != MDS_OK) {
        *st_out = c->done_status;
        return DAOS_BODY_DONE;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* The runner row as the store has it; false when unreadable or absent. */
static bool runner_row_read(uint64_t *value)
{
    struct fetch_ctx f;

    memset(&f, 0, sizeof(f));
    f.dkey = "runner";
    f.cap = sizeof(f.buf);
    if (!pin_fetch(&f) || f.rc != 0 || !f.found || f.len != 8) {
        return false;
    }
    *value = fdb_le64_get(f.buf);
    return true;
}

struct stat_snap {
    uint64_t attempts;
    uint64_t commits;
    uint64_t restarts;
    uint64_t transient;
    uint64_t indoubt;
    uint64_t delay;
    uint64_t io;
};

static struct stat_snap snap(void)
{
    struct daos_backend *b = be();
    struct stat_snap s;

    s.attempts = atomic_load(&b->stats.attempts);
    s.commits = atomic_load(&b->stats.commits);
    s.restarts = atomic_load(&b->stats.restarts);
    s.transient = atomic_load(&b->stats.transient_retries);
    s.indoubt = atomic_load(&b->stats.indoubt);
    s.delay = atomic_load(&b->stats.delay_exhausted);
    s.io = atomic_load(&b->stats.io_errors);
    return s;
}

static enum mds_status run(enum daos_txn_kind kind, struct body_ctx *c)
{
    return daos_run_txn(be(), kind, "t", body_fn, c);
}

static void test_restart_reruns_body(void)
{
    struct body_ctx c;
    struct stat_snap s0 = snap();
    struct stat_snap s1;
    uint64_t v = 0;

    memset(&c, 0, sizeof(c));
    c.fail_first_with = -DER_TX_RESTART;
    c.write = true;
    c.value = 0x1001;
    ASSERT_EQ(run(DAOS_TXN_MUTATING, &c), MDS_OK);
    s1 = snap();
    ASSERT_EQ(c.calls, 2);
    ASSERT_TRUE(c.saw_handle);
    ASSERT_EQ(s1.attempts - s0.attempts, 2);
    ASSERT_EQ(s1.restarts - s0.restarts, 1);
    ASSERT_EQ(s1.commits - s0.commits, 1);
    ASSERT_EQ(s1.transient - s0.transient, 0);
    ASSERT_EQ(s1.indoubt - s0.indoubt, 0);
    /* The restarted attempt's write is the one that landed. */
    ASSERT_TRUE(runner_row_read(&v));
    ASSERT_EQ(v, 0x1001);
}

static void test_transient_retry(void)
{
    struct body_ctx c;
    struct stat_snap s0 = snap();
    struct stat_snap s1;
    uint64_t v = 0;

    /* A transient operation error: abort, back off, fresh transaction,
     * commit.  Nothing of the failed attempt survives. */
    memset(&c, 0, sizeof(c));
    c.fail_first_with = -DER_TIMEDOUT;
    c.write = true;
    c.value = 0x1002;
    ASSERT_EQ(run(DAOS_TXN_MUTATING, &c), MDS_OK);
    s1 = snap();
    ASSERT_EQ(c.calls, 2);
    ASSERT_EQ(s1.transient - s0.transient, 1);
    ASSERT_EQ(s1.restarts - s0.restarts, 0);
    ASSERT_EQ(s1.commits - s0.commits, 1);
    ASSERT_TRUE(runner_row_read(&v));
    ASSERT_EQ(v, 0x1002);

    /* -DER_TX_BUSY and -DER_INPROGRESS are the same class. */
    memset(&c, 0, sizeof(c));
    c.fail_first_with = -DER_TX_BUSY;
    ASSERT_EQ(run(DAOS_TXN_READONLY, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
    memset(&c, 0, sizeof(c));
    c.fail_first_with = -DER_INPROGRESS;
    ASSERT_EQ(run(DAOS_TXN_MUTATING, &c), MDS_OK);
    ASSERT_EQ(c.calls, 2);
}

static void test_deadline_delay(void)
{
    struct daos_backend *b = be();
    struct body_ctx c;
    struct stat_snap s0 = snap();
    struct stat_snap s1;
    uint32_t saved = b->op_deadline_ms;

    /* 400 ms: room for the first attempt (a new attempt needs 200 ms of
     * budget) and a few backoff rounds, then MDS_ERR_DELAY -- never
     * INDOUBT, because nothing was ever committed. */
    b->op_deadline_ms = 400;
    memset(&c, 0, sizeof(c));
    c.fail_always_with = -DER_TIMEDOUT;
    ASSERT_EQ(run(DAOS_TXN_MUTATING, &c), MDS_ERR_DELAY);
    b->op_deadline_ms = saved;
    s1 = snap();
    ASSERT_TRUE(c.calls >= 2);
    ASSERT_TRUE(s1.transient - s0.transient >= 2);
    ASSERT_EQ(s1.delay - s0.delay, 1);
    ASSERT_EQ(s1.indoubt - s0.indoubt, 0);
    ASSERT_EQ(s1.commits - s0.commits, 0);
}

static void test_error_classification(void)
{
    struct body_ctx c;
    struct stat_snap s0 = snap();
    struct stat_snap s1;

    /* Definitive errors: one attempt, no commit, mapped. */
    memset(&c, 0, sizeof(c));
    c.fail_first_with = -DER_IO;
    ASSERT_EQ(run(DAOS_TXN_MUTATING, &c), MDS_ERR_IO);
    ASSERT_EQ(c.calls, 1);
    s1 = snap();
    ASSERT_EQ(s1.io - s0.io, 1);
    ASSERT_EQ(s1.commits - s0.commits, 0);

    memset(&c, 0, sizeof(c));
    c.fail_first_with = -DER_NONEXIST;
    ASSERT_EQ(run(DAOS_TXN_MUTATING, &c), MDS_ERR_NOTFOUND);
    ASSERT_EQ(c.calls, 1);
    memset(&c, 0, sizeof(c));
    c.fail_first_with = -DER_EXIST;
    ASSERT_EQ(run(DAOS_TXN_MUTATING, &c), MDS_ERR_EXISTS);
    memset(&c, 0, sizeof(c));
    c.fail_first_with = -DER_NOSPACE;
    ASSERT_EQ(run(DAOS_TXN_MUTATING, &c), MDS_ERR_NOSPC);
    memset(&c, 0, sizeof(c));
    c.fail_first_with = -DER_NO_PERM;
    ASSERT_EQ(run(DAOS_TXN_MUTATING, &c), MDS_ERR_PERM);
    /* NOTFOUND / EXISTS / NOSPC / PERM are not I/O errors. */
    ASSERT_EQ(snap().io - s0.io, 1);

    /* A positive value that is neither body code is a body bug: IO. */
    memset(&c, 0, sizeof(c));
    c.raw_rc = 7;
    ASSERT_EQ(run(DAOS_TXN_MUTATING, &c), MDS_ERR_IO);
    ASSERT_EQ(c.calls, 1);
    ASSERT_EQ(snap().io - s0.io, 2);
    ASSERT_EQ(snap().commits - s0.commits, 0);
}

static void test_kinds_readonly_none_done(void)
{
    struct body_ctx c;
    struct stat_snap s0 = snap();
    struct stat_snap s1;
    uint64_t v = 0;
    uint64_t before = 0;

    /* Read-only: a handle, no commit even when the body asks for one. */
    memset(&c, 0, sizeof(c));
    ASSERT_EQ(run(DAOS_TXN_READONLY, &c), MDS_OK);
    s1 = snap();
    ASSERT_EQ(c.calls, 1);
    ASSERT_TRUE(c.saw_handle);
    ASSERT_EQ(s1.attempts - s0.attempts, 1);
    ASSERT_EQ(s1.commits - s0.commits, 0);

    /* NONE: no handle; the body's single update is applied at once. */
    memset(&c, 0, sizeof(c));
    c.write = true;
    c.value = 0x2001;
    ASSERT_EQ(run(DAOS_TXN_NONE, &c), MDS_OK);
    ASSERT_EQ(c.calls, 1);
    ASSERT_TRUE(!c.saw_handle);
    ASSERT_EQ(snap().commits - s0.commits, 0);
    ASSERT_TRUE(runner_row_read(&v));
    ASSERT_EQ(v, 0x2001);

    /* DONE: the status passes through and the buffered write is
     * discarded with the transaction. */
    ASSERT_TRUE(runner_row_read(&before));
    memset(&c, 0, sizeof(c));
    c.write = true;
    c.value = 0x2002;
    c.done_status = MDS_ERR_EXISTS;
    ASSERT_EQ(run(DAOS_TXN_MUTATING, &c), MDS_ERR_EXISTS);
    ASSERT_EQ(c.calls, 1);
    ASSERT_EQ(snap().commits - s0.commits, 0);
    ASSERT_TRUE(runner_row_read(&v));
    ASSERT_EQ(v, before);

    /* Argument checks. */
    ASSERT_EQ(daos_run_txn(NULL, DAOS_TXN_MUTATING, "t", body_fn, &c), MDS_ERR_INVAL);
    ASSERT_EQ(daos_run_txn(be(), DAOS_TXN_MUTATING, "t", NULL, &c), MDS_ERR_INVAL);
}

/* Two handles on one thread share the thread-local pool slot of a
 * counter: a switch discards the remainder and refills, so the ids of
 * both handles come from disjoint batches that never overlap.  The
 * GC_SEQ counter is untouched before this test, so the batch boundaries
 * are exact. */
static void test_alloc_id_two_handles(void)
{
    struct daos_backend *b = be();
    struct daos_backend *b2 = g_cat2->backend_private;
    uint64_t id1 = 0;
    uint64_t id2 = 0;
    uint64_t id3 = 0;
    uint64_t id4 = 0;
    uint64_t fid = 0;

    ASSERT_TRUE(b2 != NULL && b2 != b);
    ASSERT_EQ(daos_backend_alloc_id(b, DAOS_META_GC_SEQ, &id1), MDS_OK);
    ASSERT_EQ(daos_backend_alloc_id(b, DAOS_META_GC_SEQ, &id2), MDS_OK);
    ASSERT_EQ(id2, id1 + 1);
    ASSERT_EQ(daos_backend_alloc_id(b2, DAOS_META_GC_SEQ, &id3), MDS_OK);
    ASSERT_EQ(id3, id1 + MDS_FILEID_BATCH);
    ASSERT_EQ(daos_backend_alloc_id(b, DAOS_META_GC_SEQ, &id4), MDS_OK);
    ASSERT_EQ(id4, id1 + 2U * MDS_FILEID_BATCH);
    /* Fileids start above the root; the schema key is no allocator. */
    ASSERT_EQ(daos_backend_alloc_id(b, DAOS_META_FILEID, &fid), MDS_OK);
    ASSERT_TRUE(fid > MDS_FILEID_ROOT);
    ASSERT_EQ(daos_backend_alloc_id(b, DAOS_META_SCHEMA, &fid), MDS_ERR_INVAL);
    ASSERT_EQ(daos_backend_alloc_id(NULL, DAOS_META_GC_SEQ, &fid), MDS_ERR_INVAL);
    ASSERT_EQ(daos_backend_alloc_id(b, DAOS_META_GC_SEQ, NULL), MDS_ERR_INVAL);
}

/* The daos authority table fills backend_client_stats from the runner:
 * attempts, commits, restarts and the network waits.  A read-only body
 * with one fetch blocks at least once (the reply cannot be in before
 * the server answered) and commits nothing; a mutating body adds one
 * commit and at least one more wait. */
static void test_client_stats(void)
{
    struct daos_backend *b = be();
    struct mds_cat_backend_client_stats s0;
    struct mds_cat_backend_client_stats s1;
    struct mds_cat_backend_client_stats s2;
    struct body_ctx c;
    uint64_t v = 0;

    ASSERT_EQ(mds_cat_backend_client_stats(NULL, &s0), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_backend_client_stats(g_cat, NULL), MDS_ERR_INVAL);
    ASSERT_EQ(mds_cat_backend_client_stats(g_cat, &s0), MDS_OK);
    ASSERT_EQ(s0.client_objects, 1);
    ASSERT_EQ(s0.txn_started, atomic_load(&b->stats.attempts));
    ASSERT_EQ(s0.txn_committed, atomic_load(&b->stats.commits));
    ASSERT_EQ(s0.txn_aborted, atomic_load(&b->stats.restarts));
    ASSERT_EQ(s0.exec_waits, atomic_load(&b->stats.waits));

    ASSERT_TRUE(runner_row_read(&v));
    ASSERT_EQ(mds_cat_backend_client_stats(g_cat, &s1), MDS_OK);
    ASSERT_EQ(s1.txn_started - s0.txn_started, 1);
    ASSERT_EQ(s1.txn_committed, s0.txn_committed);
    ASSERT_EQ(s1.txn_closed - s0.txn_closed, 1);
    ASSERT_TRUE(s1.exec_waits - s0.exec_waits >= 1);

    memset(&c, 0, sizeof(c));
    c.write = true;
    c.value = 0x3001;
    ASSERT_EQ(run(DAOS_TXN_MUTATING, &c), MDS_OK);
    ASSERT_EQ(mds_cat_backend_client_stats(g_cat, &s2), MDS_OK);
    ASSERT_EQ(s2.txn_started - s1.txn_started, 1);
    ASSERT_EQ(s2.txn_committed - s1.txn_committed, 1);
    ASSERT_EQ(s2.txn_closed, s1.txn_closed);
    ASSERT_TRUE(s2.exec_waits - s1.exec_waits >= 1);
    ASSERT_TRUE(s2.txn_aborted >= s1.txn_aborted);
}

/* ----------------------------------------------------------------------- */

int main(void)
{
    printf("test_daos_txn:\n");
    /* This binary tests the daos backend whatever the suite-wide
     * selection says; the harness skips (77) when it is unavailable. */
    if (setenv(CONFORMANCE_BACKEND_ENV, "daos", 1) != 0) {
        printf("FAIL: cannot select the daos backend\n");
        return 1;
    }
    if (conformance_open_as(TEST_MDS_ID, &g_cat) != MDS_OK || g_cat == NULL) {
        printf("FAIL: daos handle cannot be opened\n");
        conformance_shutdown();
        return 1;
    }
    if (conformance_open_as(TEST_MDS_ID_B, &g_cat2) != MDS_OK || g_cat2 == NULL) {
        printf("FAIL: second daos handle cannot be opened\n");
        mds_catalogue_close(g_cat);
        conformance_shutdown();
        return 1;
    }

    RUN_TEST(test_dkeys_sorted);
    RUN_TEST(test_list_resume);
    RUN_TEST(test_cond_akey_insert);
    RUN_TEST(test_fetch_absent_rec2big);
    RUN_TEST(test_restart_reruns_body);
    RUN_TEST(test_transient_retry);
    RUN_TEST(test_deadline_delay);
    RUN_TEST(test_error_classification);
    RUN_TEST(test_kinds_readonly_none_done);
    RUN_TEST(test_alloc_id_two_handles);
    RUN_TEST(test_client_stats);

    mds_catalogue_close(g_cat2);
    mds_catalogue_close(g_cat);
    g_cat2 = NULL;
    g_cat = NULL;
    conformance_shutdown();

    printf("\n%d/%d tests passed\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}

#else /* !HAVE_DAOS */

int main(void)
{
    printf("test_daos_txn: SKIP (daos backend not built)\n");
    return 77;
}

#endif

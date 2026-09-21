/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * test_contract.c -- Phase 6b backend contract tests.
 *
 * Every sub-test uses only the public dispatcher API (mds_cat_*,
 * mds_coord_*, mds_cluster_*) so the same binary runs against every
 * backend the harness can open.  Each sub-test prints PASS, FAIL or
 * SKIP; the process exits non-zero when any sub-test FAILs.  SKIP is
 * reserved for an operation the backend does not offer (the dispatcher
 * answers MDS_ERR_NOSUPPORT) or a limit that is not reachable.
 *
 * Sub-tests (the invariant each one attacks):
 *   concurrent_creators    one winner per name; parent counters advance
 *                          once per committed create
 *   concurrent_setattr     no lost or torn update; change is monotonic
 *   create_vs_rmdir        never an orphan dirent: RMDIR of a directory
 *                          that just gained a child fails, or the create
 *                          fails, never both succeed (C3: emptiness is
 *                          decided inside the mutating operation; the
 *                          dir_is_empty-then-remove pattern is exactly
 *                          the race this forbids)
 *   link_vs_final_unlink   never a dirent to a deleted inode
 *   rename_coherence       one readdir page / one lookup sees exactly one
 *                          of the two names of a same-directory rename
 *   rename_notdir_parent   a source or destination "directory" that is a
 *                          regular file answers NOTDIR (never NOENT for
 *                          the name it cannot hold), nothing changes; a
 *                          parent that does not exist is NOTFOUND
 *   rename_over_keeps_stripes
 *                          a rename over the last link of a wide file
 *                          deletes the victim's inode but leaves its
 *                          stripe map for the caller, who reads it after
 *                          the commit to GC the DS objects and drops it
 *   remove_gc_fold_stale   ns_remove_known_gc re-validates in its own
 *                          transaction: stale child (name rebound, or
 *                          the snapshot's link shape contradicted by a
 *                          LINK / REMOVE since) -> STALE, no change
 *   layout_union_recall    union grants keep one row covering all ranges
 *   replay_idempotency     a replayed create/link/remove is EXISTS /
 *                          EXISTS / NOTFOUND and counters advance once
 *   callback_reentrancy    a scan callback may call back into the same
 *                          handle (C1); a deadlock is turned into FAIL by
 *                          a 10 s watchdog
 *   readdir_paging         300 entries, page size 7, every name once,
 *                          cookies strictly increasing
 *   heartbeat_passthrough  NOTFOUND / STALE reach the caller unchanged
 *                          (C4) and deregister honours the epoch
 *   capacity_exhaustion    MDS_ERR_NOSPC leaves state unchanged
 */

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "harness.h"
#include "mds_coordination.h"
#include "mds_cluster.h"
#include "open_state.h"     /* struct nfs4_stateid */

enum result {
    R_PASS = 0,
    R_FAIL = 1,
    R_SKIP = 2,
};

/* Per-sub-test context: the first failure reason is kept; later ones
 * are counted so the log stays readable. */
struct ctx {
    struct mds_catalogue *cat;
    uint64_t dir;
    enum result res;
    unsigned extra_failures;
    char why[256];
};

static void ctx_fail(struct ctx *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

static void ctx_fail(struct ctx *c, const char *fmt, ...)
{
    va_list ap;

    if (c->res == R_FAIL) {
        c->extra_failures++;
        return;
    }
    c->res = R_FAIL;
    va_start(ap, fmt);
    (void)vsnprintf(c->why, sizeof(c->why), fmt, ap);
    va_end(ap);
}

static void ctx_skip(struct ctx *c, const char *why)
{
    if (c->res == R_PASS) {
        c->res = R_SKIP;
        (void)snprintf(c->why, sizeof(c->why), "%s", why);
    }
}

/* -----------------------------------------------------------------------
 * Small shared helpers
 * ----------------------------------------------------------------------- */

#define THREADS 8

/* Start gate: every started thread parks here and the driver releases
 * them all at once, so the operations race as tightly as a barrier
 * would make them, without a fixed participant count that a failed
 * pthread_create could never satisfy. */
struct start_gate {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int arrived;
    bool go;
};

struct barrier_op {
    struct ctx *c;
    struct start_gate *gate;
    int index;
    enum mds_status st;
    struct mds_inode out;
    /* Operation selector and arguments. */
    int what;
    uint64_t parent;
    const char *name;
    enum mds_file_type type;
    uint64_t target;
    uint32_t mode;
    uint64_t size;
    int iterations;
};

enum { OP_CREATE = 1, OP_REMOVE, OP_LINK, OP_SETATTR };

static void gate_wait(struct start_gate *g)
{
    pthread_mutex_lock(&g->lock);
    g->arrived++;
    pthread_cond_broadcast(&g->cond);
    while (!g->go) {
        pthread_cond_wait(&g->cond, &g->lock);
    }
    pthread_mutex_unlock(&g->lock);
}

/* Wait until @expected threads have arrived (bounded), then release. */
static void gate_open(struct start_gate *g, int expected)
{
    struct timespec deadline;

    (void)clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 5;
    pthread_mutex_lock(&g->lock);
    while (g->arrived < expected) {
        if (pthread_cond_timedwait(&g->cond, &g->lock, &deadline) != 0) {
            break;
        }
    }
    g->go = true;
    pthread_cond_broadcast(&g->cond);
    pthread_mutex_unlock(&g->lock);
}

static void *barrier_op_main(void *arg)
{
    struct barrier_op *op = arg;
    int i;

    gate_wait(op->gate);
    switch (op->what) {
    case OP_CREATE:
        memset(&op->out, 0, sizeof(op->out));
        op->st = mds_cat_ns_create(op->c->cat, NULL, op->parent, op->name,
                                   op->type, 0755, 0, 0, NULL, &op->out);
        break;
    case OP_REMOVE:
        op->st = mds_cat_ns_remove(op->c->cat, NULL, op->parent, op->name);
        break;
    case OP_LINK:
        op->st = mds_cat_ns_link(op->c->cat, NULL, op->parent, op->name,
                                 op->target);
        break;
    case OP_SETATTR: {
        struct mds_inode attrs;

        op->st = MDS_OK;
        for (i = 0; i < op->iterations; i++) {
            enum mds_status st;

            memset(&attrs, 0, sizeof(attrs));
            attrs.mode = op->mode;
            attrs.size = op->size;
            st = mds_cat_ns_setattr(op->c->cat, NULL, op->target, &attrs,
                                    MDS_ATTR_MODE | MDS_ATTR_SIZE);
            if (st != MDS_OK) {
                op->st = st;
            }
        }
        break;
    }
    default:
        op->st = MDS_ERR_INVAL;
        break;
    }
    return NULL;
}

/* Run @n gate-synchronised operations on @n threads and join them.  An
 * operation whose thread could not be started reports MDS_ERR_IO. */
static void run_ops(struct barrier_op *ops, int n)
{
    pthread_t threads[THREADS];
    struct start_gate gate;
    int i;
    int started = 0;

    if (n > THREADS) {
        n = THREADS;
    }
    memset(&gate, 0, sizeof(gate));
    pthread_mutex_init(&gate.lock, NULL);
    pthread_cond_init(&gate.cond, NULL);
    for (i = 0; i < n; i++) {
        ops[i].gate = &gate;
        ops[i].index = i;
        ops[i].st = MDS_ERR_IO;
        if (pthread_create(&threads[i], NULL, barrier_op_main, &ops[i]) != 0) {
            break;
        }
        started++;
    }
    gate_open(&gate, started);
    for (i = 0; i < started; i++) {
        (void)pthread_join(threads[i], NULL);
    }
    pthread_cond_destroy(&gate.cond);
    pthread_mutex_destroy(&gate.lock);
}

/* Bounded readdir page collector (fileid + cookie + name).  No sub-test
 * keeps more than a handful of entries in one directory at a time; the
 * 300-entry paging test reads pages of 7. */
#define PAGE_MAX 64

struct page {
    uint32_t count;
    bool     overflow;
    uint64_t fileid[PAGE_MAX];
    uint64_t cookie[PAGE_MAX];
    uint8_t  type[PAGE_MAX];
    char     name[PAGE_MAX][MDS_MAX_NAME + 1];
};

static int page_cb(const struct mds_cat_dirent *entry, void *arg)
{
    struct page *p = arg;

    if (p->count >= PAGE_MAX) {
        p->overflow = true;
        return 1;
    }
    p->fileid[p->count] = entry->fileid;
    p->cookie[p->count] = entry->cookie;
    p->type[p->count] = entry->type;
    (void)snprintf(p->name[p->count], sizeof(p->name[0]), "%s", entry->name);
    p->count++;
    return 0;
}

static int page_plus_cb(const struct mds_cat_dirent *entry,
                        const struct mds_inode *inode, bool inode_valid,
                        void *arg)
{
    (void)inode;
    (void)inode_valid;
    return page_cb(entry, arg);
}

static enum mds_status read_page(struct mds_catalogue *cat, uint64_t dir,
                                 struct page *p)
{
    memset(p, 0, sizeof(*p));
    return mds_cat_ns_readdir(cat, dir, NULL, 0, NULL, page_cb, p);
}

static const char *status_name(enum mds_status st)
{
    switch (st) {
    case MDS_OK:            return "OK";
    case MDS_ERR_NOMEM:     return "NOMEM";
    case MDS_ERR_IO:        return "IO";
    case MDS_ERR_NOTFOUND:  return "NOTFOUND";
    case MDS_ERR_EXISTS:    return "EXISTS";
    case MDS_ERR_INVAL:     return "INVAL";
    case MDS_ERR_STALE:     return "STALE";
    case MDS_ERR_DELAY:     return "DELAY";
    case MDS_ERR_NOTEMPTY:  return "NOTEMPTY";
    case MDS_ERR_NOSPC:     return "NOSPC";
    case MDS_ERR_NOSUPPORT: return "NOSUPPORT";
    default:                return "other";
    }
}

/* -----------------------------------------------------------------------
 * concurrent_creators
 * ----------------------------------------------------------------------- */

static void concurrent_creators(struct ctx *c)
{
    struct barrier_op ops[THREADS];
    struct mds_inode before, after, seen;
    char names[THREADS][32];
    int ok = 0, exists = 0;
    int i;

    /* Phase A: one distinct name per thread -> every create succeeds
     * and the parent advances once per create. */
    if (mds_cat_ns_getattr(c->cat, c->dir, &before) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    memset(ops, 0, sizeof(ops));
    for (i = 0; i < THREADS; i++) {
        (void)snprintf(names[i], sizeof(names[i]), "cc-%d", i);
        ops[i].c = c;
        ops[i].what = OP_CREATE;
        ops[i].parent = c->dir;
        ops[i].name = names[i];
        ops[i].type = MDS_FTYPE_DIR;
    }
    run_ops(ops, THREADS);
    for (i = 0; i < THREADS; i++) {
        if (ops[i].st != MDS_OK) {
            ctx_fail(c, "distinct-name create %d returned %s", i,
                     status_name(ops[i].st));
        } else if (mds_cat_ns_lookup(c->cat, c->dir, names[i], &seen) !=
                       MDS_OK || seen.fileid != ops[i].out.fileid) {
            ctx_fail(c, "distinct-name create %d not visible", i);
        }
    }
    if (mds_cat_ns_getattr(c->cat, c->dir, &after) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    if (after.nlink != before.nlink + THREADS) {
        ctx_fail(c, "parent nlink %u -> %u for %d dir creates",
                 before.nlink, after.nlink, THREADS);
    }
    if (after.change != before.change + THREADS) {
        ctx_fail(c, "parent change advanced %llu for %d creates",
                 (unsigned long long)(after.change - before.change), THREADS);
    }

    /* Phase B: every thread creates the SAME name -> exactly one OK,
     * the rest EXISTS, parent counters advance exactly once. */
    before = after;
    memset(ops, 0, sizeof(ops));
    for (i = 0; i < THREADS; i++) {
        ops[i].c = c;
        ops[i].what = OP_CREATE;
        ops[i].parent = c->dir;
        ops[i].name = "same";
        ops[i].type = MDS_FTYPE_DIR;
    }
    run_ops(ops, THREADS);
    for (i = 0; i < THREADS; i++) {
        if (ops[i].st == MDS_OK) {
            ok++;
        } else if (ops[i].st == MDS_ERR_EXISTS) {
            exists++;
        } else {
            ctx_fail(c, "same-name create %d returned %s", i,
                     status_name(ops[i].st));
        }
    }
    if (ok != 1 || exists != THREADS - 1) {
        ctx_fail(c, "same-name race: %d OK, %d EXISTS (want 1/%d)", ok,
                 exists, THREADS - 1);
    }
    if (mds_cat_ns_getattr(c->cat, c->dir, &after) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    if (after.nlink != before.nlink + 1) {
        ctx_fail(c, "same-name race: parent nlink %u -> %u", before.nlink,
                 after.nlink);
    }
    if (after.change != before.change + 1) {
        ctx_fail(c, "same-name race: parent change advanced %llu",
                 (unsigned long long)(after.change - before.change));
    }
    for (i = 0; i < THREADS; i++) {
        if (ops[i].st == MDS_OK &&
            (mds_cat_ns_lookup(c->cat, c->dir, "same", &seen) != MDS_OK ||
             seen.fileid != ops[i].out.fileid)) {
            ctx_fail(c, "same-name winner is not the visible inode");
        }
    }
}

/* -----------------------------------------------------------------------
 * concurrent_setattr
 * ----------------------------------------------------------------------- */

#define SETATTR_ITER 32

static void concurrent_setattr(struct ctx *c)
{
    struct barrier_op ops[THREADS];
    struct mds_inode file, before, after;
    int i;

    memset(&file, 0, sizeof(file));
    if (mds_cat_ns_create(c->cat, NULL, c->dir, "s", MDS_FTYPE_REG, 0644,
                          0, 0, NULL, &file) != MDS_OK) {
        ctx_fail(c, "create failed");
        return;
    }
    if (mds_cat_ns_getattr(c->cat, file.fileid, &before) != MDS_OK) {
        ctx_fail(c, "getattr failed");
        return;
    }
    memset(ops, 0, sizeof(ops));
    for (i = 0; i < THREADS; i++) {
        ops[i].c = c;
        ops[i].what = OP_SETATTR;
        ops[i].target = file.fileid;
        ops[i].mode = 0600U + (uint32_t)i;
        ops[i].size = 1000U + (uint64_t)i;
        ops[i].iterations = SETATTR_ITER;
    }
    run_ops(ops, THREADS);
    for (i = 0; i < THREADS; i++) {
        if (ops[i].st != MDS_OK) {
            ctx_fail(c, "setattr thread %d returned %s", i,
                     status_name(ops[i].st));
        }
    }
    if (mds_cat_ns_getattr(c->cat, file.fileid, &after) != MDS_OK) {
        ctx_fail(c, "getattr failed");
        return;
    }
    /* Not torn: mode and size come from the same writer. */
    if (after.mode < 0600U || after.mode >= 0600U + THREADS ||
        after.size < 1000U || after.size >= 1000U + THREADS ||
        after.mode - 0600U != after.size - 1000U) {
        ctx_fail(c, "torn setattr: mode %o size %llu", after.mode,
                 (unsigned long long)after.size);
    }
    /* Not lost: every committed setattr advanced the change counter. */
    if (after.change != before.change + (uint64_t)THREADS * SETATTR_ITER) {
        ctx_fail(c, "change advanced %llu for %d setattrs",
                 (unsigned long long)(after.change - before.change),
                 THREADS * SETATTR_ITER);
    }
}

/* -----------------------------------------------------------------------
 * create_vs_rmdir
 * ----------------------------------------------------------------------- */

#define RACE_ROUNDS 200

static void create_vs_rmdir(struct ctx *c)
{
    int round;

    for (round = 0; round < RACE_ROUNDS; round++) {
        struct barrier_op ops[2];
        struct mds_inode d, seen;
        struct page p;
        char dname[32];
        bool dir_gone;

        (void)snprintf(dname, sizeof(dname), "d-%d", round);
        memset(&d, 0, sizeof(d));
        if (mds_cat_ns_create(c->cat, NULL, c->dir, dname, MDS_FTYPE_DIR,
                              0755, 0, 0, NULL, &d) != MDS_OK) {
            ctx_fail(c, "round %d: mkdir failed", round);
            return;
        }
        memset(ops, 0, sizeof(ops));
        ops[0].c = c;
        ops[0].what = OP_CREATE;
        ops[0].parent = d.fileid;
        ops[0].name = "child";
        ops[0].type = MDS_FTYPE_REG;
        ops[1].c = c;
        ops[1].what = OP_REMOVE;
        ops[1].parent = c->dir;
        ops[1].name = dname;
        run_ops(ops, 2);

        dir_gone = (mds_cat_ns_getattr(c->cat, d.fileid, &seen) ==
                    MDS_ERR_NOTFOUND);

        if (ops[0].st == MDS_OK && ops[1].st == MDS_OK) {
            ctx_fail(c, "round %d: create AND rmdir both succeeded "
                     "(orphan dirent)", round);
        }
        if (dir_gone) {
            if (ops[0].st == MDS_OK) {
                ctx_fail(c, "round %d: create OK but directory gone",
                         round);
            }
            if (mds_cat_ns_lookup(c->cat, d.fileid, "child", &seen) !=
                MDS_ERR_NOTFOUND) {
                ctx_fail(c, "round %d: dirent under removed directory",
                         round);
            }
            if (read_page(c->cat, d.fileid, &p) == MDS_OK && p.count != 0) {
                ctx_fail(c, "round %d: %u entries under removed directory",
                         round, p.count);
            }
        } else {
            if (ops[0].st != MDS_OK) {
                ctx_fail(c, "round %d: directory kept but create %s",
                         round, status_name(ops[0].st));
            }
            if (ops[1].st == MDS_OK) {
                ctx_fail(c, "round %d: rmdir OK but directory still there",
                         round);
            }
            /* The create won: the directory is non-empty; clean it. */
            (void)mds_cat_ns_remove(c->cat, NULL, d.fileid, "child");
            (void)mds_cat_ns_remove(c->cat, NULL, c->dir, dname);
        }
        if (c->res == R_FAIL) {
            return;
        }
    }
}

/* -----------------------------------------------------------------------
 * link_vs_final_unlink
 * ----------------------------------------------------------------------- */

static void link_vs_final_unlink(struct ctx *c)
{
    int round;

    for (round = 0; round < RACE_ROUNDS; round++) {
        struct barrier_op ops[2];
        struct mds_inode f, seen;
        struct page p;
        char fname[32], lname[32];
        uint32_t i;

        (void)snprintf(fname, sizeof(fname), "f-%d", round);
        (void)snprintf(lname, sizeof(lname), "l-%d", round);
        memset(&f, 0, sizeof(f));
        if (mds_cat_ns_create(c->cat, NULL, c->dir, fname, MDS_FTYPE_REG,
                              0644, 0, 0, NULL, &f) != MDS_OK) {
            ctx_fail(c, "round %d: create failed", round);
            return;
        }
        memset(ops, 0, sizeof(ops));
        ops[0].c = c;
        ops[0].what = OP_LINK;
        ops[0].parent = c->dir;
        ops[0].name = lname;
        ops[0].target = f.fileid;
        ops[1].c = c;
        ops[1].what = OP_REMOVE;
        ops[1].parent = c->dir;
        ops[1].name = fname;
        run_ops(ops, 2);

        if (ops[1].st != MDS_OK) {
            ctx_fail(c, "round %d: unlink returned %s", round,
                     status_name(ops[1].st));
        }
        if (ops[0].st == MDS_OK) {
            /* The link kept the inode alive with exactly one name. */
            if (mds_cat_ns_getattr(c->cat, f.fileid, &seen) != MDS_OK) {
                ctx_fail(c, "round %d: link OK but inode gone", round);
            } else if (seen.nlink != 1) {
                ctx_fail(c, "round %d: nlink %u after link+unlink", round,
                         seen.nlink);
            }
        } else if (ops[0].st == MDS_ERR_NOTFOUND) {
            if (mds_cat_ns_getattr(c->cat, f.fileid, &seen) != MDS_ERR_NOTFOUND) {
                ctx_fail(c, "round %d: link NOTFOUND but inode alive", round);
            }
        } else {
            ctx_fail(c, "round %d: link returned %s", round,
                     status_name(ops[0].st));
        }
        /* Never a dirent pointing at a deleted inode. */
        if (read_page(c->cat, c->dir, &p) != MDS_OK) {
            ctx_fail(c, "round %d: readdir failed", round);
        } else {
            for (i = 0; i < p.count; i++) {
                if (mds_cat_ns_getattr(c->cat, p.fileid[i], &seen) !=
                    MDS_OK) {
                    ctx_fail(c, "round %d: dirent %s -> fileid %llu has no "
                             "inode", round, p.name[i],
                             (unsigned long long)p.fileid[i]);
                }
            }
        }
        if (ops[0].st == MDS_OK) {
            (void)mds_cat_ns_remove(c->cat, NULL, c->dir, lname);
        }
        if (c->res == R_FAIL) {
            return;
        }
    }
}

/* -----------------------------------------------------------------------
 * rename_coherence
 * ----------------------------------------------------------------------- */

#define RENAME_ITER 400

struct rename_ctx {
    struct ctx *c;
    uint64_t fid;
    /* Written by the worker at exit, polled by the observer: a volatile
     * flag is not a synchronisation primitive (TSan reports it). */
    _Atomic int running;
    enum mds_status st;
};

static void *renamer_main(void *arg)
{
    struct rename_ctx *r = arg;
    int i;

    r->st = MDS_OK;
    for (i = 0; i < RENAME_ITER && r->st == MDS_OK; i++) {
        r->st = mds_cat_ns_rename(r->c->cat, NULL, r->c->dir, "a",
                                  r->c->dir, "b");
        if (r->st == MDS_OK) {
            r->st = mds_cat_ns_rename(r->c->cat, NULL, r->c->dir, "b",
                                      r->c->dir, "a");
        }
    }
    r->running = 0;
    return NULL;
}

static void *creator_main(void *arg)
{
    struct rename_ctx *r = arg;
    struct mds_inode out;
    int i;

    r->st = MDS_OK;
    for (i = 0; i < RENAME_ITER && r->st == MDS_OK; i++) {
        memset(&out, 0, sizeof(out));
        r->st = mds_cat_ns_create(r->c->cat, NULL, r->c->dir, "c",
                                  MDS_FTYPE_REG, 0644, 0, 0, NULL, &out);
        if (r->st == MDS_OK) {
            r->st = mds_cat_ns_remove(r->c->cat, NULL, r->c->dir, "c");
        }
    }
    r->running = 0;
    return NULL;
}

static void rename_coherence(struct ctx *c)
{
    struct rename_ctx renamer, creator;
    struct mds_inode file, seen;
    pthread_t rt, ct;
    unsigned observations = 0;

    memset(&file, 0, sizeof(file));
    if (mds_cat_ns_create(c->cat, NULL, c->dir, "a", MDS_FTYPE_REG, 0644,
                          0, 0, NULL, &file) != MDS_OK) {
        ctx_fail(c, "create failed");
        return;
    }
    memset(&renamer, 0, sizeof(renamer));
    memset(&creator, 0, sizeof(creator));
    renamer.c = c;
    renamer.fid = file.fileid;
    renamer.running = 1;
    creator.c = c;
    creator.running = 1;
    if (pthread_create(&rt, NULL, renamer_main, &renamer) != 0) {
        ctx_fail(c, "pthread_create failed");
        return;
    }
    if (pthread_create(&ct, NULL, creator_main, &creator) != 0) {
        (void)pthread_join(rt, NULL);
        ctx_fail(c, "pthread_create failed");
        return;
    }

    while (renamer.running || creator.running) {
        struct page p;
        unsigned hits = 0, c_hits = 0;
        uint32_t i;
        enum mds_status st;

        /* One readdir page: exactly one name for the renamed inode. */
        if (read_page(c->cat, c->dir, &p) != MDS_OK) {
            ctx_fail(c, "readdir failed during rename");
            break;
        }
        for (i = 0; i < p.count; i++) {
            if (p.fileid[i] == file.fileid) {
                hits++;
                if (strcmp(p.name[i], "a") != 0 &&
                    strcmp(p.name[i], "b") != 0) {
                    ctx_fail(c, "renamed inode visible as '%s'", p.name[i]);
                }
            } else if (strcmp(p.name[i], "c") == 0) {
                c_hits++;
            } else {
                ctx_fail(c, "unexpected entry '%s'", p.name[i]);
            }
        }
        if (hits != 1) {
            ctx_fail(c, "one readdir page saw %u names for the renamed "
                     "inode (want exactly 1)", hits);
        }
        if (c_hits > 1) {
            ctx_fail(c, "'c' appeared %u times in one page", c_hits);
        }
        /* One lookup: whichever name resolves, it is the same inode. */
        st = mds_cat_ns_lookup(c->cat, c->dir, "a", &seen);
        if (st == MDS_OK && seen.fileid != file.fileid) {
            ctx_fail(c, "lookup 'a' resolved to a different inode");
        } else if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
            ctx_fail(c, "lookup 'a' returned %s", status_name(st));
        }
        st = mds_cat_ns_lookup(c->cat, c->dir, "b", &seen);
        if (st == MDS_OK && seen.fileid != file.fileid) {
            ctx_fail(c, "lookup 'b' resolved to a different inode");
        } else if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
            ctx_fail(c, "lookup 'b' returned %s", status_name(st));
        }
        observations++;
        if (c->res == R_FAIL) {
            break;
        }
    }
    (void)pthread_join(rt, NULL);
    (void)pthread_join(ct, NULL);
    if (renamer.st != MDS_OK) {
        ctx_fail(c, "rename returned %s", status_name(renamer.st));
    }
    if (creator.st != MDS_OK) {
        ctx_fail(c, "create/remove 'c' returned %s",
                 status_name(creator.st));
    }
    if (observations == 0) {
        ctx_fail(c, "observer made no observation");
    }
    /* Final state: exactly one of a/b. */
    {
        enum mds_status sa = mds_cat_ns_lookup(c->cat, c->dir, "a", &seen);
        enum mds_status sb = mds_cat_ns_lookup(c->cat, c->dir, "b", &seen);

        if (!((sa == MDS_OK) != (sb == MDS_OK))) {
            ctx_fail(c, "final state: a=%s b=%s", status_name(sa),
                     status_name(sb));
        }
    }
}

/* -----------------------------------------------------------------------
 * rename_notdir_parent
 * ----------------------------------------------------------------------- */

/* RFC 8881 18.26.4: RENAME with a saved or current filehandle that is
 * not a directory is NFS4ERR_NOTDIR.  A regular file has no dirents, so
 * a backend that resolves the source name before it checks the parent's
 * type answers NOENT instead (pynfs RNM2* / RNM3*); the type decision
 * has to come first.  An absent parent stays NOTFOUND. */
static void rename_notdir_parent(struct ctx *c)
{
    struct mds_inode nd, mv, seen, parent_before, parent_after;
    enum mds_status st;

    memset(&nd, 0, sizeof(nd));
    memset(&mv, 0, sizeof(mv));
    if (mds_cat_ns_create(c->cat, NULL, c->dir, "nd", MDS_FTYPE_REG, 0644,
                          0, 0, NULL, &nd) != MDS_OK ||
        mds_cat_ns_create(c->cat, NULL, c->dir, "mv", MDS_FTYPE_REG, 0644,
                          0, 0, NULL, &mv) != MDS_OK ||
        mds_cat_ns_getattr(c->cat, c->dir, &parent_before) != MDS_OK) {
        ctx_fail(c, "setup (two files) failed");
        return;
    }
    /* Source parent is a file (the name it cannot hold is asked for). */
    st = mds_cat_ns_rename(c->cat, NULL, nd.fileid, "x", c->dir, "y");
    if (st != MDS_ERR_NOTDIR) {
        ctx_fail(c, "source parent a file: expected NOTDIR, got %s",
                 status_name(st));
    }
    /* Destination parent is a file (the source name does exist). */
    st = mds_cat_ns_rename(c->cat, NULL, c->dir, "mv", nd.fileid, "y");
    if (st != MDS_ERR_NOTDIR) {
        ctx_fail(c, "destination parent a file: expected NOTDIR, got %s",
                 status_name(st));
    }
    /* Both parents files. */
    st = mds_cat_ns_rename(c->cat, NULL, nd.fileid, "x", mv.fileid, "y");
    if (st != MDS_ERR_NOTDIR) {
        ctx_fail(c, "both parents files: expected NOTDIR, got %s",
                 status_name(st));
    }
    /* A parent that does not exist at all is NOTFOUND, not NOTDIR. */
    st = mds_cat_ns_rename(c->cat, NULL, (uint64_t)1 << 62, "x", c->dir, "y");
    if (st != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "absent source parent: expected NOTFOUND, got %s",
                 status_name(st));
    }
    st = mds_cat_ns_rename(c->cat, NULL, c->dir, "mv", (uint64_t)1 << 62, "y");
    if (st != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "absent destination parent: expected NOTFOUND, got %s",
                 status_name(st));
    }
    /* Nothing moved, nothing appeared, the directory's counters stand. */
    if (mds_cat_ns_lookup(c->cat, c->dir, "mv", &seen) != MDS_OK ||
        seen.fileid != mv.fileid) {
        ctx_fail(c, "'mv' no longer resolves to its inode");
    }
    if (mds_cat_ns_lookup(c->cat, c->dir, "y", &seen) != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "'y' appeared in the directory");
    }
    if (mds_cat_ns_getattr(c->cat, nd.fileid, &seen) != MDS_OK ||
        seen.type != MDS_FTYPE_REG || seen.nlink != 1) {
        ctx_fail(c, "the file used as a parent changed");
    }
    if (mds_cat_ns_getattr(c->cat, c->dir, &parent_after) != MDS_OK ||
        parent_after.change != parent_before.change ||
        parent_after.nlink != parent_before.nlink) {
        ctx_fail(c, "directory counters changed on refused renames");
    }
}

/* -----------------------------------------------------------------------
 * rename_over_keeps_stripes
 * ----------------------------------------------------------------------- */

/* Stripes and DS ids of the wide victim; small so the map cannot be
 * mistaken for an inline 1x1 binding. */
#define RNM_OVER_STRIPES 2U
#define RNM_OVER_UNIT    65536U

/* op_rename (compound_namespace.c) snapshots only the inline 1x1
 * binding of a doomed destination before the rename; for a wide file
 * it calls stripe_map_get AFTER the commit to queue the DS objects for
 * GC and then stripe_map_del.  A backend whose rename purged the map
 * would make every DS object of an overwritten wide file uncollectable
 * -- silently: that get answers NOTFOUND and the GC helper returns.  So
 * the rename must delete exactly the victim's inode and nothing of its
 * stripe map. */
static void rename_over_keeps_stripes(struct ctx *c)
{
    struct mds_inode v, s, seen;
    struct mds_ds_map_entry in[RNM_OVER_STRIPES];
    struct mds_ds_map_entry *out = NULL;
    uint32_t sc = 0, su = 0, mc = 0;
    uint32_t i;
    enum mds_status st;

    memset(&v, 0, sizeof(v));
    memset(&s, 0, sizeof(s));
    if (mds_cat_ns_create(c->cat, NULL, c->dir, "v", MDS_FTYPE_REG, 0644,
                          0, 0, NULL, &v) != MDS_OK ||
        mds_cat_ns_create(c->cat, NULL, c->dir, "s", MDS_FTYPE_REG, 0644,
                          0, 0, NULL, &s) != MDS_OK) {
        ctx_fail(c, "setup (two files) failed");
        return;
    }
    memset(in, 0, sizeof(in));
    for (i = 0; i < RNM_OVER_STRIPES; i++) {
        in[i].ds_id = 10U + i;
        in[i].nfs_fh_len = 4;
        in[i].nfs_fh[0] = (uint8_t)(0xC0U + i);
        in[i].nfs_fh[3] = (uint8_t)i;
    }
    st = mds_cat_stripe_map_put(c->cat, NULL, v.fileid, RNM_OVER_STRIPES,
                                RNM_OVER_UNIT, 1, in);
    if (st == MDS_ERR_NOSUPPORT) {
        ctx_skip(c, "stripe_map_put not provided by this backend");
        return;
    }
    if (st != MDS_OK) {
        ctx_fail(c, "stripe_map_put returned %s", status_name(st));
        return;
    }
    /* Flags 0: no keep-orphan, the victim's last link goes. */
    st = mds_cat_ns_rename(c->cat, NULL, c->dir, "s", c->dir, "v");
    if (st != MDS_OK) {
        ctx_fail(c, "rename over the victim returned %s", status_name(st));
        return;
    }
    if (mds_cat_ns_lookup(c->cat, c->dir, "v", &seen) != MDS_OK ||
        seen.fileid != s.fileid) {
        ctx_fail(c, "'v' does not resolve to the renamed inode");
    }
    if (mds_cat_ns_lookup(c->cat, c->dir, "s", &seen) != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "'s' still resolves after the rename");
    }
    /* Exactly the inode row went ... */
    st = mds_cat_ns_getattr(c->cat, v.fileid, &seen);
    if (st != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "victim inode after rename-over: %s (want NOTFOUND)",
                 status_name(st));
    }
    /* ... and the stripe map is still there for the post-rename GC. */
    st = mds_cat_stripe_map_get(c->cat, v.fileid, &sc, &su, &mc, &out);
    if (st != MDS_OK) {
        ctx_fail(c, "victim stripe map after rename-over: %s (want OK)",
                 status_name(st));
        return;
    }
    if (sc != RNM_OVER_STRIPES || su != RNM_OVER_UNIT || mc != 1 ||
        out == NULL) {
        ctx_fail(c, "victim stripe map geometry %u x %u unit %u after "
                 "rename-over", sc, mc, su);
    } else {
        for (i = 0; i < RNM_OVER_STRIPES; i++) {
            if (out[i].ds_id != in[i].ds_id ||
                out[i].nfs_fh_len != in[i].nfs_fh_len ||
                memcmp(out[i].nfs_fh, in[i].nfs_fh, in[i].nfs_fh_len) != 0) {
                ctx_fail(c, "victim stripe entry %u changed", i);
            }
        }
    }
    free(out);
    out = NULL;
    /* The caller's drop works and is the last trace of the map. */
    st = mds_cat_stripe_map_del(c->cat, NULL, v.fileid);
    if (st != MDS_OK) {
        ctx_fail(c, "stripe_map_del of the victim returned %s",
                 status_name(st));
    }
    st = mds_cat_stripe_map_get(c->cat, v.fileid, &sc, &su, &mc, &out);
    free(out);
    if (st != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "victim stripe map after del: %s (want NOTFOUND)",
                 status_name(st));
    }
}

/* -----------------------------------------------------------------------
 * remove_gc_fold_stale
 * ----------------------------------------------------------------------- */

static void remove_gc_fold_stale(struct ctx *c)
{
    struct mds_inode c1, c2, seen, parent_before, parent_after;
    struct mds_ds_map_entry gc_entry;
    uint32_t gc_before = 0, gc_after = 0;
    bool folded = true;
    enum mds_status st;

    memset(&c1, 0, sizeof(c1));
    memset(&c2, 0, sizeof(c2));
    if (mds_cat_ns_create(c->cat, NULL, c->dir, "g", MDS_FTYPE_REG, 0644,
                          0, 0, NULL, &c1) != MDS_OK ||
        mds_cat_ns_remove(c->cat, NULL, c->dir, "g") != MDS_OK ||
        mds_cat_ns_create(c->cat, NULL, c->dir, "g", MDS_FTYPE_REG, 0644,
                          0, 0, NULL, &c2) != MDS_OK) {
        ctx_fail(c, "setup (create/remove/create) failed");
        return;
    }
    if (c1.fileid == c2.fileid) {
        ctx_fail(c, "replacement reused the fileid; snapshot not stale");
        return;
    }
    memset(&gc_entry, 0, sizeof(gc_entry));
    gc_entry.ds_id = 1;
    gc_entry.nfs_fh_len = 4;
    gc_entry.nfs_fh[0] = 0xAB;

    if (mds_cat_ns_getattr(c->cat, c->dir, &parent_before) != MDS_OK ||
        mds_cat_gc_count(c->cat, &gc_before) != MDS_OK) {
        ctx_fail(c, "snapshot failed");
        return;
    }

    /* Stale snapshot (c1): must be refused with STALE and change nothing. */
    st = mds_cat_ns_remove_known_gc(c->cat, NULL, c->dir, "g", &c1, 1,
                                    &gc_entry, 1, MDS_GC_SWEEP_GEOM(1, 1),
                                    &folded);
    if (st == MDS_ERR_NOSUPPORT) {
        ctx_skip(c, "ns_remove_known_gc not provided by this backend");
        return;
    }
    if (st != MDS_ERR_STALE) {
        ctx_fail(c, "stale child: expected STALE, got %s", status_name(st));
    }
    if (folded) {
        ctx_fail(c, "stale child: gc_folded reported true");
    }
    if (mds_cat_ns_lookup(c->cat, c->dir, "g", &seen) != MDS_OK ||
        seen.fileid != c2.fileid) {
        ctx_fail(c, "stale child: dirent changed");
    }
    if (mds_cat_ns_getattr(c->cat, c->dir, &parent_after) != MDS_OK ||
        parent_after.change != parent_before.change ||
        parent_after.nlink != parent_before.nlink) {
        ctx_fail(c, "stale child: parent counters changed");
    }
    if (mds_cat_gc_count(c->cat, &gc_after) != MDS_OK ||
        gc_after != gc_before) {
        ctx_fail(c, "stale child: GC queue changed (%u -> %u)", gc_before,
                 gc_after);
    }

    /* Current snapshot (c2): final unlink folds exactly the GC row. */
    folded = false;
    st = mds_cat_ns_remove_known_gc(c->cat, NULL, c->dir, "g", &c2, 1,
                                    &gc_entry, 1, MDS_GC_SWEEP_GEOM(1, 1),
                                    &folded);
    if (st != MDS_OK) {
        ctx_fail(c, "current child: expected OK, got %s", status_name(st));
        return;
    }
    if (!folded) {
        ctx_fail(c, "current child: final unlink did not fold GC rows");
    }
    if (mds_cat_ns_lookup(c->cat, c->dir, "g", &seen) != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "current child: name still resolves");
    }
    if (mds_cat_ns_getattr(c->cat, c2.fileid, &seen) != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "current child: inode still there after final unlink");
    }
    if (mds_cat_gc_count(c->cat, &gc_after) != MDS_OK ||
        gc_after != gc_before + 1) {
        ctx_fail(c, "current child: GC queue %u -> %u (want +1)", gc_before,
                 gc_after);
    }
    gc_before = gc_after;

    /* Link shape moved under a snapshot that still names the right
     * inode.  The caller derives "final unlink" -- and with it the GC
     * rows it hands in -- from the snapshot's nlink, so a LINK that
     * landed since (snapshot final, live nlink 2) must be refused: an
     * OK here would have the caller collect a live file's DS objects.
     * The reverse (snapshot non-final, the other name removed since,
     * live nlink 1) must be refused as well: the caller would leak the
     * last link's objects.  Nothing changes on either refusal; a fresh
     * snapshot then folds exactly one GC row. */
    memset(&c1, 0, sizeof(c1));
    if (mds_cat_ns_create(c->cat, NULL, c->dir, "lk", MDS_FTYPE_REG, 0644,
                          0, 0, NULL, &c1) != MDS_OK ||
        mds_cat_ns_link(c->cat, NULL, c->dir, "lk2", c1.fileid) != MDS_OK) {
        ctx_fail(c, "link-shape setup (create + link) failed");
        return;
    }
    folded = true;
    st = mds_cat_ns_remove_known_gc(c->cat, NULL, c->dir, "lk", &c1, 1,
                                    &gc_entry, 1, MDS_GC_SWEEP_GEOM(1, 1),
                                    &folded);
    if (st != MDS_ERR_STALE) {
        ctx_fail(c, "final snapshot, live second link: expected STALE, got %s",
                 status_name(st));
    }
    if (folded) {
        ctx_fail(c, "final snapshot, live second link: gc_folded reported true");
    }
    if (mds_cat_ns_getattr(c->cat, c1.fileid, &seen) != MDS_OK ||
        seen.nlink != 2) {
        ctx_fail(c, "final snapshot, live second link: inode changed "
                 "(nlink %u, want 2)", (unsigned)seen.nlink);
    }
    if (mds_cat_ns_lookup(c->cat, c->dir, "lk", &seen) != MDS_OK ||
        seen.fileid != c1.fileid) {
        ctx_fail(c, "final snapshot, live second link: dirent changed");
    }
    if (mds_cat_gc_count(c->cat, &gc_after) != MDS_OK ||
        gc_after != gc_before) {
        ctx_fail(c, "final snapshot, live second link: GC queue changed "
                 "(%u -> %u)", gc_before, gc_after);
    }
    /* Reverse: a non-final snapshot (nlink 2) after the other name went. */
    if (mds_cat_ns_getattr(c->cat, c1.fileid, &c2) != MDS_OK ||
        mds_cat_ns_remove(c->cat, NULL, c->dir, "lk2") != MDS_OK) {
        ctx_fail(c, "link-shape setup (snapshot + remove lk2) failed");
        return;
    }
    folded = true;
    st = mds_cat_ns_remove_known_gc(c->cat, NULL, c->dir, "lk", &c2, 1,
                                    &gc_entry, 1, MDS_GC_SWEEP_GEOM(1, 1),
                                    &folded);
    if (st != MDS_ERR_STALE) {
        ctx_fail(c, "non-final snapshot, last link: expected STALE, got %s",
                 status_name(st));
    }
    if (folded) {
        ctx_fail(c, "non-final snapshot, last link: gc_folded reported true");
    }
    if (mds_cat_ns_getattr(c->cat, c1.fileid, &seen) != MDS_OK ||
        seen.nlink != 1) {
        ctx_fail(c, "non-final snapshot, last link: inode changed "
                 "(nlink %u, want 1)", (unsigned)seen.nlink);
    }
    if (mds_cat_gc_count(c->cat, &gc_after) != MDS_OK ||
        gc_after != gc_before) {
        ctx_fail(c, "non-final snapshot, last link: GC queue changed "
                 "(%u -> %u)", gc_before, gc_after);
    }
    /* A fresh snapshot is the final link: exactly one GC row folds. */
    if (mds_cat_ns_getattr(c->cat, c1.fileid, &c2) != MDS_OK) {
        ctx_fail(c, "fresh snapshot failed");
        return;
    }
    folded = false;
    st = mds_cat_ns_remove_known_gc(c->cat, NULL, c->dir, "lk", &c2, 1,
                                    &gc_entry, 1, MDS_GC_SWEEP_GEOM(1, 1),
                                    &folded);
    if (st != MDS_OK || !folded) {
        ctx_fail(c, "fresh snapshot: expected OK + folded, got %s folded=%d",
                 status_name(st), (int)folded);
    }
    if (mds_cat_ns_getattr(c->cat, c1.fileid, &seen) != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "fresh snapshot: inode still there after final unlink");
    }
    if (mds_cat_gc_count(c->cat, &gc_after) != MDS_OK ||
        gc_after != gc_before + 1) {
        ctx_fail(c, "fresh snapshot: GC queue %u -> %u (want +1)", gc_before,
                 gc_after);
    }
}

/* -----------------------------------------------------------------------
 * layout_union_recall
 * ----------------------------------------------------------------------- */

struct holder_count {
    unsigned holders;
    struct nfs4_stateid sid;
};

static int holder_cb(uint64_t clientid, const struct nfs4_stateid *stateid,
                     uint32_t iomode, void *arg)
{
    struct holder_count *h = arg;

    (void)clientid;
    (void)iomode;
    h->holders++;
    if (stateid != NULL) {
        h->sid = *stateid;
    }
    return 0;
}

/* Layout rows are keyed by fileid, and a persistent store (RonDB) keeps
 * rows across test binaries: a fixed fileid such as 500 collides with
 * whatever an earlier run or another suite granted on it.  Take the
 * fileid of a file created in this run's scratch directory instead;
 * fileids are never reused, so its layout table is empty. */
static uint64_t fresh_layout_fileid(struct ctx *c, const char *name)
{
    struct mds_inode out;

    memset(&out, 0, sizeof(out));
    if (mds_cat_ns_create(c->cat, NULL, c->dir, name, MDS_FTYPE_REG, 0644,
                          0, 0, NULL, &out) != MDS_OK) {
        ctx_fail(c, "create %s failed", name);
        return 0;
    }
    return out.fileid;
}

static void layout_union_recall(struct ctx *c)
{
    struct nfs4_stateid sid;
    struct holder_count h;
    uint32_t ds_ids[1] = { 1 };
    uint64_t off = 0, len = 0, got_fid = 0;
    const uint64_t fileid = fresh_layout_fileid(c, "union");
    const uint64_t clientid = 7;
    enum mds_status st;

    if (fileid == 0) {
        return;
    }
    memset(&sid, 0, sizeof(sid));
    sid.seqid = 1;
    memset(sid.other, 0x51, sizeof(sid.other));

    st = mds_coord_layout_grant_union(c->cat, NULL, clientid, fileid, 2,
                                      0, 4096, &sid, ds_ids, 1);
    if (st == MDS_ERR_NOSUPPORT) {
        ctx_skip(c, "layout_grant_union not provided by this backend");
        return;
    }
    if (st != MDS_OK) {
        ctx_fail(c, "first union grant returned %s", status_name(st));
        return;
    }
    sid.seqid = 2;
    st = mds_coord_layout_grant_union(c->cat, NULL, clientid, fileid, 2,
                                      8192, 4096, &sid, ds_ids, 1);
    if (st != MDS_OK) {
        ctx_fail(c, "second union grant returned %s", status_name(st));
        return;
    }
    /* The row is a superset of both windows: [0, 12288). */
    st = mds_coord_layout_get_by_stateid(c->cat, sid.other, NULL, &got_fid,
                                         NULL, &off, &len, NULL);
    if (st != MDS_OK) {
        ctx_fail(c, "layout_get_by_stateid returned %s", status_name(st));
        return;
    }
    if (got_fid != fileid || off != 0 || off + len < 12288) {
        ctx_fail(c, "union row covers [%llu, +%llu), want [0, 12288)",
                 (unsigned long long)off, (unsigned long long)len);
    }
    /* Exactly one holder row for the file, so a recall finds it once. */
    memset(&h, 0, sizeof(h));
    if (mds_coord_layout_iter_file(c->cat, fileid, holder_cb, &h) != MDS_OK) {
        ctx_fail(c, "layout_iter_file failed");
    } else if (h.holders != 1) {
        ctx_fail(c, "%u holder rows after two union grants (want 1)",
                 h.holders);
    } else if (memcmp(h.sid.other, sid.other, sizeof(sid.other)) != 0) {
        ctx_fail(c, "holder stateid differs from the granted one");
    }
    /* Return covers everything at once. */
    if (mds_coord_layout_return(c->cat, NULL, sid.other, clientid, fileid,
                                ds_ids, 1) != MDS_OK) {
        ctx_fail(c, "layout_return failed");
    }
    if (mds_coord_layout_get_by_stateid(c->cat, sid.other, NULL, NULL, NULL,
                                        NULL, NULL, NULL) !=
        MDS_ERR_NOTFOUND) {
        ctx_fail(c, "row still there after return");
    }
    (void)mds_coord_layout_del_all_for_client(c->cat, clientid);
}

/* -----------------------------------------------------------------------
 * replay_idempotency
 * ----------------------------------------------------------------------- */

static void replay_idempotency(struct ctx *c)
{
    struct mds_inode before, mid, after, r, r2, seen;

    if (mds_cat_ns_getattr(c->cat, c->dir, &before) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    memset(&r, 0, sizeof(r));
    if (mds_cat_ns_create(c->cat, NULL, c->dir, "r", MDS_FTYPE_DIR, 0755,
                          0, 0, NULL, &r) != MDS_OK) {
        ctx_fail(c, "create failed");
        return;
    }
    /* Replayed create: EXISTS, and the winner is untouched. */
    if (mds_cat_ns_create(c->cat, NULL, c->dir, "r", MDS_FTYPE_DIR, 0755,
                          0, 0, NULL, &seen) != MDS_ERR_EXISTS) {
        ctx_fail(c, "replayed create did not return EXISTS");
    }
    if (mds_cat_ns_lookup(c->cat, c->dir, "r", &seen) != MDS_OK ||
        seen.fileid != r.fileid) {
        ctx_fail(c, "replayed create changed the visible inode");
    }
    if (mds_cat_ns_getattr(c->cat, c->dir, &mid) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    if (mid.nlink != before.nlink + 1 || mid.change != before.change + 1) {
        ctx_fail(c, "counters after create+replay: nlink +%d change +%llu",
                 (int)mid.nlink - (int)before.nlink,
                 (unsigned long long)(mid.change - before.change));
    }
    /* Replayed link on a file: EXISTS and nlink bumped once. */
    memset(&r2, 0, sizeof(r2));
    if (mds_cat_ns_create(c->cat, NULL, c->dir, "r2", MDS_FTYPE_REG, 0644,
                          0, 0, NULL, &r2) != MDS_OK) {
        ctx_fail(c, "create r2 failed");
        return;
    }
    if (mds_cat_ns_link(c->cat, NULL, c->dir, "r2l", r2.fileid) != MDS_OK) {
        ctx_fail(c, "link failed");
    }
    if (mds_cat_ns_link(c->cat, NULL, c->dir, "r2l", r2.fileid) !=
        MDS_ERR_EXISTS) {
        ctx_fail(c, "replayed link did not return EXISTS");
    }
    if (mds_cat_ns_getattr(c->cat, r2.fileid, &seen) != MDS_OK ||
        seen.nlink != 2) {
        ctx_fail(c, "nlink after link+replay is %u (want 2)",
                 seen.nlink);
    }
    (void)mds_cat_ns_remove(c->cat, NULL, c->dir, "r2l");
    (void)mds_cat_ns_remove(c->cat, NULL, c->dir, "r2");
    if (mds_cat_ns_getattr(c->cat, c->dir, &mid) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    /* Replayed remove: NOTFOUND, counters advance once. */
    if (mds_cat_ns_remove(c->cat, NULL, c->dir, "r") != MDS_OK) {
        ctx_fail(c, "remove failed");
    }
    if (mds_cat_ns_remove(c->cat, NULL, c->dir, "r") != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "replayed remove did not return NOTFOUND");
    }
    if (mds_cat_ns_getattr(c->cat, c->dir, &after) != MDS_OK) {
        ctx_fail(c, "parent getattr failed");
        return;
    }
    if (after.nlink != mid.nlink - 1 || after.change != mid.change + 1) {
        ctx_fail(c, "counters after remove+replay: nlink %d change +%llu",
                 (int)after.nlink - (int)mid.nlink,
                 (unsigned long long)(after.change - mid.change));
    }
}

/* -----------------------------------------------------------------------
 * callback_reentrancy
 * ----------------------------------------------------------------------- */

#define REENTRANCY_TIMEOUT_SEC 10

struct watchdog {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    bool done;
    const char *what;
    unsigned seconds;
};

/* Name of the sub-test the driver is running (NULL between two), so a
 * suite-watchdog report says where the binary was stuck. */
static const char *_Atomic g_running_subtest;

static void *watchdog_main(void *arg)
{
    struct watchdog *w = arg;
    struct timespec deadline;
    int rc = 0;

    (void)clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += (time_t)w->seconds;
    pthread_mutex_lock(&w->lock);
    while (!w->done && rc == 0) {
        rc = pthread_cond_timedwait(&w->cond, &w->lock, &deadline);
    }
    if (!w->done) {
        const char *sub = atomic_load(&g_running_subtest);
        bool name_sub = sub != NULL && strcmp(sub, w->what) != 0;

        pthread_mutex_unlock(&w->lock);
        (void)printf("  FAIL %s: no progress for %u s (deadlock%s%s)\n", w->what,
                     w->seconds, name_sub ? " in " : "", name_sub ? sub : "");
        (void)fflush(stdout);
        _exit(1);
    }
    pthread_mutex_unlock(&w->lock);
    return NULL;
}

static int watchdog_start(struct watchdog *w, pthread_t *thread,
                          const char *what, unsigned seconds)
{
    memset(w, 0, sizeof(*w));
    pthread_mutex_init(&w->lock, NULL);
    pthread_cond_init(&w->cond, NULL);
    w->what = what;
    w->seconds = seconds;
    return pthread_create(thread, NULL, watchdog_main, w);
}

static void watchdog_stop(struct watchdog *w, pthread_t thread)
{
    pthread_mutex_lock(&w->lock);
    w->done = true;
    pthread_cond_signal(&w->cond);
    pthread_mutex_unlock(&w->lock);
    (void)pthread_join(thread, NULL);
    pthread_cond_destroy(&w->cond);
    pthread_mutex_destroy(&w->lock);
}

struct reentrant_iter {
    struct mds_catalogue *cat;
    uint64_t fileid;
    unsigned calls;
    unsigned nested_ok;
};

static int reentrant_layout_cb(uint64_t clientid,
                               const struct nfs4_stateid *stateid,
                               uint32_t iomode, void *arg)
{
    struct reentrant_iter *it = arg;
    uint64_t got_fid = 0;
    struct mds_inode seen;

    (void)clientid;
    (void)iomode;
    it->calls++;
    /* The pattern layout_recall.c's byte-range collector uses: call the
     * same handle from inside the iteration callback. */
    if (stateid != NULL &&
        mds_coord_layout_get_by_stateid(it->cat, stateid->other, NULL,
                                        &got_fid, NULL, NULL, NULL,
                                        NULL) == MDS_OK &&
        got_fid == it->fileid &&
        mds_cat_ns_getattr(it->cat, MDS_FILEID_ROOT, &seen) == MDS_OK) {
        it->nested_ok++;
    }
    return 0;
}

struct reentrant_readdir {
    struct mds_catalogue *cat;
    uint64_t dir;
    unsigned calls;
    unsigned nested_ok;
};

static int reentrant_readdir_cb(const struct mds_cat_dirent *entry, void *arg)
{
    struct reentrant_readdir *rd = arg;
    struct mds_inode seen;

    rd->calls++;
    if (mds_cat_ns_lookup(rd->cat, rd->dir, entry->name, &seen) == MDS_OK &&
        seen.fileid == entry->fileid) {
        rd->nested_ok++;
    }
    return 0;
}

static void callback_reentrancy(struct ctx *c)
{
    struct watchdog wd;
    pthread_t wd_thread;
    struct nfs4_stateid sid;
    struct reentrant_iter it;
    struct reentrant_readdir rd;
    struct mds_inode out;
    uint32_t ds_ids[1] = { 1 };
    const uint64_t fileid = fresh_layout_fileid(c, "re-target");
    unsigned i;

    if (fileid == 0) {
        return;
    }
    if (watchdog_start(&wd, &wd_thread, "callback_reentrancy",
                       REENTRANCY_TIMEOUT_SEC) != 0) {
        ctx_fail(c, "watchdog thread failed to start");
        return;
    }

    for (i = 0; i < 3; i++) {
        memset(&sid, 0, sizeof(sid));
        sid.seqid = 1;
        memset(sid.other, 0x60 + (int)i, sizeof(sid.other));
        if (mds_coord_layout_grant(c->cat, NULL, 10 + i, fileid, 1, 0,
                                   UINT64_MAX, &sid, ds_ids, 1) != MDS_OK) {
            ctx_fail(c, "layout_grant %u failed", i);
        }
    }
    memset(&it, 0, sizeof(it));
    it.cat = c->cat;
    it.fileid = fileid;
    if (mds_coord_layout_iter_file(c->cat, fileid, reentrant_layout_cb,
                                   &it) != MDS_OK) {
        ctx_fail(c, "layout_iter_file failed");
    }
    if (it.calls != 3 || it.nested_ok != 3) {
        ctx_fail(c, "layout iteration: %u callbacks, %u nested lookups OK "
                 "(want 3/3)", it.calls, it.nested_ok);
    }
    /* The readdir part below counts exactly the five entries it creates. */
    (void)mds_cat_ns_remove(c->cat, NULL, c->dir, "re-target");

    for (i = 0; i < 5; i++) {
        char name[32];

        (void)snprintf(name, sizeof(name), "re-%u", i);
        memset(&out, 0, sizeof(out));
        if (mds_cat_ns_create(c->cat, NULL, c->dir, name, MDS_FTYPE_REG,
                              0644, 0, 0, NULL, &out) != MDS_OK) {
            ctx_fail(c, "create %s failed", name);
        }
    }
    memset(&rd, 0, sizeof(rd));
    rd.cat = c->cat;
    rd.dir = c->dir;
    if (mds_cat_ns_readdir(c->cat, c->dir, NULL, 0, NULL,
                           reentrant_readdir_cb, &rd) != MDS_OK) {
        ctx_fail(c, "readdir failed");
    }
    if (rd.calls != 5 || rd.nested_ok != 5) {
        ctx_fail(c, "readdir: %u callbacks, %u nested lookups OK (want 5/5)",
                 rd.calls, rd.nested_ok);
    }

    for (i = 0; i < 3; i++) {
        (void)mds_coord_layout_del_all_for_client(c->cat, 10 + i);
    }
    watchdog_stop(&wd, wd_thread);
}

/* -----------------------------------------------------------------------
 * readdir_paging
 * ----------------------------------------------------------------------- */

#define PAGING_ENTRIES 300
#define PAGING_PAGE    7

static void readdir_paging(struct ctx *c)
{
    unsigned char seen[PAGING_ENTRIES];
    struct mds_inode out;
    uint64_t cookie = 0;
    uint64_t prev_cookie = 0;
    unsigned pages = 0, total = 0;
    unsigned i;

    memset(seen, 0, sizeof(seen));
    for (i = 0; i < PAGING_ENTRIES; i++) {
        char name[32];

        (void)snprintf(name, sizeof(name), "p-%03u", i);
        memset(&out, 0, sizeof(out));
        if (mds_cat_ns_create(c->cat, NULL, c->dir, name, MDS_FTYPE_REG,
                              0644, 0, 0, NULL, &out) != MDS_OK) {
            ctx_fail(c, "create %s failed", name);
            return;
        }
    }
    for (;;) {
        struct page p;
        uint32_t k;

        memset(&p, 0, sizeof(p));
        if (mds_cat_ns_readdir_plus_from_cookie(c->cat, c->dir, cookie,
                                                PAGING_PAGE, NULL,
                                                page_plus_cb, &p) != MDS_OK) {
            ctx_fail(c, "readdir_plus_from_cookie failed");
            return;
        }
        if (p.count == 0) {
            break;
        }
        pages++;
        if (p.count > PAGING_PAGE) {
            ctx_fail(c, "page %u has %u entries (max %d)", pages, p.count,
                     PAGING_PAGE);
        }
        for (k = 0; k < p.count; k++) {
            unsigned idx = 0;

            if (p.cookie[k] < 3 || p.cookie[k] <= prev_cookie) {
                ctx_fail(c, "cookie %llu after %llu (page %u)",
                         (unsigned long long)p.cookie[k],
                         (unsigned long long)prev_cookie, pages);
            }
            prev_cookie = p.cookie[k];
            if (sscanf(p.name[k], "p-%3u", &idx) != 1 ||
                idx >= PAGING_ENTRIES || strlen(p.name[k]) != 5) {
                ctx_fail(c, "unexpected entry '%s'", p.name[k]);
            } else if (seen[idx]++ != 0) {
                ctx_fail(c, "entry %s delivered twice", p.name[k]);
            }
            total++;
        }
        cookie = prev_cookie;
        if (pages > PAGING_ENTRIES) {
            ctx_fail(c, "paging did not terminate");
            return;
        }
    }
    if (total != PAGING_ENTRIES) {
        ctx_fail(c, "%u entries delivered (want %d)", total, PAGING_ENTRIES);
    }
    for (i = 0; i < PAGING_ENTRIES; i++) {
        if (seen[i] != 1) {
            ctx_fail(c, "entry p-%03u delivered %u times", i, seen[i]);
            break;
        }
    }
    if (pages != (PAGING_ENTRIES + PAGING_PAGE - 1) / PAGING_PAGE) {
        ctx_fail(c, "%u pages (want %d)", pages,
                 (PAGING_ENTRIES + PAGING_PAGE - 1) / PAGING_PAGE);
    }
}

/* -----------------------------------------------------------------------
 * heartbeat_passthrough
 * ----------------------------------------------------------------------- */

struct node_seen {
    uint32_t mds_id;
    bool found;
    uint64_t boot_epoch;
};

static int node_list_cb(uint32_t mds_id, uint64_t boot_epoch,
                        const char *hostname, uint16_t nfs_port,
                        uint16_t grpc_port, uint64_t last_heartbeat_ns,
                        void *ctx)
{
    struct node_seen *n = ctx;

    (void)hostname;
    (void)nfs_port;
    (void)grpc_port;
    (void)last_heartbeat_ns;
    if (mds_id == n->mds_id) {
        n->found = true;
        n->boot_epoch = boot_epoch;
    }
    return 0;
}

static void heartbeat_passthrough(struct ctx *c)
{
    /* An id no daemon uses: MDS_MAX_NODES is the upper bound of live
     * ids, so the conformance node sits just below it. */
    const uint32_t id = MDS_MAX_NODES - 1;
    const uint64_t epoch = 500;
    struct node_seen n;
    enum mds_status st;

    st = mds_cluster_node_heartbeat(c->cat, id, epoch);
    if (st == MDS_ERR_NOSUPPORT) {
        ctx_skip(c, "no cluster slots on this backend");
        return;
    }
    if (st != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "heartbeat of unregistered node returned %s (want "
                 "NOTFOUND)", status_name(st));
    }
    st = mds_cluster_node_register(c->cat, id, epoch, "conformance", 12049,
                                   15051);
    if (st != MDS_OK) {
        ctx_fail(c, "register returned %s", status_name(st));
        return;
    }
    st = mds_cluster_node_heartbeat(c->cat, id, epoch);
    if (st != MDS_OK) {
        ctx_fail(c, "heartbeat of live node returned %s", status_name(st));
    }
    st = mds_cluster_node_heartbeat(c->cat, id, epoch - 1);
    if (st != MDS_ERR_STALE) {
        ctx_fail(c, "old-epoch heartbeat returned %s (want STALE)",
                 status_name(st));
    }
    memset(&n, 0, sizeof(n));
    n.mds_id = id;
    if (mds_cluster_node_list(c->cat, node_list_cb, &n) != MDS_OK ||
        !n.found || n.boot_epoch != epoch) {
        ctx_fail(c, "old-epoch heartbeat changed the row (found=%d epoch=%llu)",
                 (int)n.found, (unsigned long long)n.boot_epoch);
    }
    /* Deregister honours the epoch: mismatch leaves the row. */
    st = mds_cluster_node_deregister(c->cat, id, epoch - 1);
    if (st != MDS_ERR_STALE) {
        ctx_fail(c, "old-epoch deregister returned %s (want STALE)",
                 status_name(st));
    }
    memset(&n, 0, sizeof(n));
    n.mds_id = id;
    if (mds_cluster_node_list(c->cat, node_list_cb, &n) != MDS_OK ||
        !n.found) {
        ctx_fail(c, "old-epoch deregister removed the row");
    }
    st = mds_cluster_node_deregister(c->cat, id, epoch);
    if (st != MDS_OK) {
        ctx_fail(c, "deregister returned %s", status_name(st));
    }
    st = mds_cluster_node_heartbeat(c->cat, id, epoch);
    if (st != MDS_ERR_NOTFOUND) {
        ctx_fail(c, "heartbeat after deregister returned %s (want NOTFOUND)",
                 status_name(st));
    }
    /* A retried deregister is harmless. */
    st = mds_cluster_node_deregister(c->cat, id, epoch);
    if (st != MDS_OK) {
        ctx_fail(c, "retried deregister returned %s", status_name(st));
    }
}

/* -----------------------------------------------------------------------
 * capacity_exhaustion
 * ----------------------------------------------------------------------- */

#define CAPACITY_BUDGET 100000U

/* A store without a capacity bound (FoundationDB) would run the whole
 * create budget -- two transactions per iteration -- only to SKIP, and
 * the suite watchdog would fire first.  The wall-clock budget reaches
 * that SKIP in bounded time; memdb hits NOSPC well inside it. */
#define CAPACITY_TIME_BUDGET_SEC 120U

static uint64_t monotonic_sec(void)
{
    struct timespec ts;

    (void)clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec;
}

static void capacity_exhaustion(struct ctx *c)
{
    struct mds_catalogue *cat = NULL;
    struct mds_inode out, last_parent, parent, seen;
    uint64_t dir = 0;
    uint64_t start_sec;
    unsigned created = 0;
    unsigned i;
    enum mds_status st = MDS_OK;
    char name[32];

    /* Own handle: filling a store is not something the other sub-tests
     * should share. */
    if (conformance_open(&cat) != MDS_OK || cat == NULL) {
        ctx_fail(c, "open failed");
        return;
    }
    if (conformance_scratch_dir(cat, &dir) != MDS_OK ||
        mds_cat_ns_getattr(cat, dir, &last_parent) != MDS_OK) {
        ctx_fail(c, "scratch setup failed");
        mds_catalogue_close(cat);
        return;
    }
    start_sec = monotonic_sec();
    for (i = 0; i < CAPACITY_BUDGET; i++) {
        (void)snprintf(name, sizeof(name), "cap-%u", i);
        memset(&out, 0, sizeof(out));
        st = mds_cat_ns_create(cat, NULL, dir, name, MDS_FTYPE_REG, 0644,
                               0, 0, NULL, &out);
        if (st != MDS_OK) {
            break;
        }
        created++;
        if (mds_cat_ns_getattr(cat, dir, &last_parent) != MDS_OK) {
            ctx_fail(c, "parent getattr failed after create %u", i);
            break;
        }
        if (monotonic_sec() - start_sec >= CAPACITY_TIME_BUDGET_SEC) {
            break;
        }
    }
    if (c->res != R_FAIL) {
        if (st == MDS_OK) {
            char why[128];

            (void)snprintf(why, sizeof(why), "no MDS_ERR_NOSPC within the "
                           "create/time budget (%u creates in %u s)", created,
                           (unsigned)(monotonic_sec() - start_sec));
            ctx_skip(c, why);
        } else if (st != MDS_ERR_NOSPC) {
            ctx_fail(c, "create %u returned %s (want NOSPC)", created,
                     status_name(st));
        } else {
            /* The failed name is absent and the parent is exactly as the
             * last successful create left it. */
            if (mds_cat_ns_lookup(cat, dir, name, &seen) != MDS_ERR_NOTFOUND) {
                ctx_fail(c, "failed create left '%s' behind", name);
            }
            if (mds_cat_ns_getattr(cat, dir, &parent) != MDS_OK ||
                parent.change != last_parent.change ||
                parent.nlink != last_parent.nlink) {
                ctx_fail(c, "failed create changed the parent counters");
            }
            /* And the store still works after the refusal. */
            if (created > 0) {
                (void)snprintf(name, sizeof(name), "cap-%u", created - 1);
                if (mds_cat_ns_lookup(cat, dir, name, &seen) != MDS_OK) {
                    ctx_fail(c, "last successful create vanished");
                }
            }
        }
    }
    for (i = 0; i < created; i++) {
        (void)snprintf(name, sizeof(name), "cap-%u", i);
        (void)mds_cat_ns_remove(cat, NULL, dir, name);
    }
    conformance_scratch_cleanup(cat, dir);
    mds_catalogue_close(cat);
    (void)printf("    (%u creates before the limit)\n", created);
}

/* -----------------------------------------------------------------------
 * Driver
 * ----------------------------------------------------------------------- */

struct subtest {
    const char *name;
    void (*fn)(struct ctx *c);
    bool needs_scratch;
};

static const struct subtest subtests[] = {
    { "concurrent_creators",   concurrent_creators,   true  },
    { "concurrent_setattr",    concurrent_setattr,    true  },
    { "create_vs_rmdir",       create_vs_rmdir,       true  },
    { "link_vs_final_unlink",  link_vs_final_unlink,  true  },
    { "rename_coherence",      rename_coherence,      true  },
    { "rename_notdir_parent",  rename_notdir_parent,  true  },
    { "rename_over_keeps_stripes", rename_over_keeps_stripes, true },
    { "remove_gc_fold_stale",  remove_gc_fold_stale,  true  },
    { "layout_union_recall",   layout_union_recall,   true  },
    { "replay_idempotency",    replay_idempotency,    true  },
    { "callback_reentrancy",   callback_reentrancy,   true  },
    { "readdir_paging",        readdir_paging,        true  },
    { "heartbeat_passthrough", heartbeat_passthrough, false },
    { "capacity_exhaustion",   capacity_exhaustion,   false },
};

/* The whole binary is bounded too: a backend that hangs in one of the
 * concurrent sub-tests must not leave ctest waiting for its timeout. */
#define SUITE_TIMEOUT_SEC 600

int main(int argc, char **argv)
{
    struct mds_catalogue *cat = conformance_open_checked();
    struct watchdog suite_wd;
    pthread_t suite_wd_thread;
    const char *only = (argc > 1) ? argv[1] : NULL;
    unsigned failed = 0, passed = 0, skipped = 0;
    size_t i;

    (void)printf("test_contract (backend=%s):\n", conformance_backend_name());
    if (watchdog_start(&suite_wd, &suite_wd_thread, "test_contract",
                       SUITE_TIMEOUT_SEC) != 0) {
        (void)fprintf(stderr, "cannot start the suite watchdog\n");
        mds_catalogue_close(cat);
        return 1;
    }

    for (i = 0; i < sizeof(subtests) / sizeof(subtests[0]); i++) {
        struct ctx c;

        if (only != NULL && strcmp(only, subtests[i].name) != 0) {
            continue;
        }
        memset(&c, 0, sizeof(c));
        c.cat = cat;
        c.res = R_PASS;
        atomic_store(&g_running_subtest, subtests[i].name);
        if (subtests[i].needs_scratch &&
            conformance_scratch_dir(cat, &c.dir) != MDS_OK) {
            ctx_fail(&c, "cannot create scratch directory");
        } else {
            subtests[i].fn(&c);
        }
        if (subtests[i].needs_scratch && c.dir != 0) {
            conformance_scratch_cleanup(cat, c.dir);
        }
        atomic_store(&g_running_subtest, NULL);
        switch (c.res) {
        case R_PASS:
            passed++;
            (void)printf("  PASS %s\n", subtests[i].name);
            break;
        case R_SKIP:
            skipped++;
            (void)printf("  SKIP %s: %s\n", subtests[i].name, c.why);
            break;
        case R_FAIL:
        default:
            failed++;
            (void)printf("  FAIL %s: %s", subtests[i].name, c.why);
            if (c.extra_failures > 0) {
                (void)printf(" (+%u more)", c.extra_failures);
            }
            (void)printf("\n");
            break;
        }
        (void)fflush(stdout);
    }

    watchdog_stop(&suite_wd, suite_wd_thread);
    mds_catalogue_close(cat);
    (void)printf("\ntest_contract: %u passed, %u failed, %u skipped\n", passed,
                 failed, skipped);
    conformance_shutdown();
    return (failed == 0) ? 0 : 1;
}

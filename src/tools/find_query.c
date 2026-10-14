/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * find_query.c -- Shared read-only metadata search core.
 *
 * One catalogue scan answers a find-style query, so the cost is
 * O(rows scanned) rather than the O(tree depth x fan-out) NFS round
 * trips a client-side namespace walk would need.
 *
 * Attribute predicates are pushed to the RonDB data nodes so
 * non-matching rows never cross the wire.  Name matching is always
 * finished locally with fnmatch(3): the backend's LIKE pre-filter is
 * only ever a superset, so a backend that implements a different LIKE
 * dialect can cost extra bandwidth but can never change the answer.
 */

#include <errno.h>
#include <fnmatch.h>
#include <stdlib.h>
#include <string.h>

#include "find_query.h"

#include "mds_catalogue.h"
#ifdef HAVE_RONDB
#include "catalogue_rondb.h"
#endif

/* -----------------------------------------------------------------------
 * Pure helpers
 * ----------------------------------------------------------------------- */

bool find_parse_u64(const char *s, uint64_t *out)
{
    char *end = NULL;
    unsigned long long v;

    if (s == NULL || out == NULL || s[0] == '\0') {
        return false;
    }
    /* strtoull happily accepts a leading '-' and wraps; reject it and
     * leading whitespace explicitly so "-1" cannot become UINT64_MAX. */
    if (s[0] == '-' || s[0] == '+' ||
        s[0] == ' ' || s[0] == '\t') {
        return false;
    }

    errno = 0;
    v = strtoull(s, &end, 10);
    if (errno != 0 || end == s || end == NULL || *end != '\0') {
        return false;
    }
    *out = (uint64_t)v;
    return true;
}

bool find_parse_size(const char *s, uint64_t *out)
{
    char     digits[32];
    uint64_t mult = 1U;
    uint64_t v = 0U;
    size_t   len;

    if (s == NULL || out == NULL || s[0] == '\0') {
        return false;
    }
    len = strlen(s);

    switch (s[len - 1U]) {
    case 'k': case 'K': mult = 1024ULL; break;
    case 'm': case 'M': mult = 1024ULL * 1024ULL; break;
    case 'g': case 'G': mult = 1024ULL * 1024ULL * 1024ULL; break;
    case 't': case 'T': mult = 1024ULL * 1024ULL * 1024ULL * 1024ULL; break;
    case 'p': case 'P': mult = 1024ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL;
                        break;
    default: break;
    }

    if (mult == 1U) {
        return find_parse_u64(s, out);
    }

    /* Strip the suffix, then reuse the strict integer parser. */
    len--;
    if (len == 0U || len >= sizeof(digits)) {
        return false;
    }
    memcpy(digits, s, len);
    digits[len] = '\0';
    if (!find_parse_u64(digits, &v)) {
        return false;
    }
    /* Reject rather than wrap: a silently truncated bound would return
     * a confidently wrong result set. */
    if (v != 0U && v > UINT64_MAX / mult) {
        return false;
    }
    *out = v * mult;
    return true;
}

int find_parse_type_char(const char *s)
{
    if (s == NULL || s[0] == '\0' || s[1] != '\0') {
        return -1;
    }
    switch (s[0]) {
    case 'f': return MDS_FTYPE_REG;
    case 'd': return MDS_FTYPE_DIR;
    case 'l': return MDS_FTYPE_SYMLINK;
    case 'b': return MDS_FTYPE_BLKDEV;
    case 'c': return MDS_FTYPE_CHRDEV;
    case 'p': return MDS_FTYPE_FIFO;
    case 's': return MDS_FTYPE_SOCK;
    default:  return -1;
    }
}

char find_type_char(uint8_t type)
{
    switch (type) {
    case MDS_FTYPE_REG:     return '-';
    case MDS_FTYPE_DIR:     return 'd';
    case MDS_FTYPE_SYMLINK: return 'l';
    case MDS_FTYPE_BLKDEV:  return 'b';
    case MDS_FTYPE_CHRDEV:  return 'c';
    case MDS_FTYPE_FIFO:    return 'p';
    case MDS_FTYPE_SOCK:    return 's';
    default:                return '?';
    }
}

void find_format_mode(uint32_t mode, char *out)
{
    static const char rwx[] = "rwx";
    unsigned int i;

    if (out == NULL) {
        return;
    }
    for (i = 0U; i < 9U; i++) {
        /* Bit 8 is owner-read, descending to bit 0 = other-execute. */
        out[i] = (char)(((mode & (1U << (8U - i))) != 0U) ? rwx[i % 3U] : '-');
    }
    /* setuid / setgid / sticky replace the matching execute slot. */
    if ((mode & 04000U) != 0U) { out[2] = ((mode & 0100U) != 0U) ? 's' : 'S'; }
    if ((mode & 02000U) != 0U) { out[5] = ((mode & 0010U) != 0U) ? 's' : 'S'; }
    if ((mode & 01000U) != 0U) { out[8] = ((mode & 0001U) != 0U) ? 't' : 'T'; }
    out[9] = '\0';
}

int find_glob_to_like(const char *glob, char *out, size_t cap)
{
    size_t n = 0U;
    size_t i;
    bool   all_wild = true;

    if (out == NULL || cap == 0U) {
        return -1;
    }
    out[0] = '\0';
    if (glob == NULL || glob[0] == '\0') {
        return 0;
    }
    /* Two glob constructs have no safe LIKE equivalent:
     *
     *   '\' is an escape prefix in LIKE but a literal-quote in a glob.
     *   '[' opens a character class, which LIKE has no notion of, so the
     *       bracket would be matched literally and the pattern would be
     *       NARROWER than the glob -- silently dropping real matches.
     *
     * Neither is worth the subtlety, so decline to push any filter and
     * let fnmatch do all the work. */
    if (strchr(glob, '\\') != NULL || strchr(glob, '[') != NULL) {
        return -1;
    }

    for (i = 0U; glob[i] != '\0'; i++) {
        char c = glob[i];

        if (c == '*') {
            /* Collapse runs of '%': "%%%%" and "%" match the same set,
             * and a shorter program is cheaper to evaluate per row. */
            if (n > 0U && out[n - 1U] == '%') {
                continue;
            }
            c = '%';
        } else if (c == '?') {
            /* '_' constrains the length, so unlike '%' it is selective
             * and worth pushing on its own. */
            c = '_';
            all_wild = false;
        } else {
            all_wild = false;
        }
        if (n + 1U >= cap) {
            return -1;      /* would truncate: pushing it could narrow */
        }
        out[n++] = c;
    }
    out[n] = '\0';

    /* Entry names are Varbinary(255); a longer pattern cannot be pushed. */
    if (n > 255U) {
        out[0] = '\0';
        return -1;
    }
    if (all_wild) {
        /* Matches every row: pushing it is pure overhead. */
        out[0] = '\0';
        return 0;
    }
    return (int)n;
}

bool find_name_matches(const char *glob, const char *name)
{
    if (glob == NULL) {
        return true;
    }
    if (name == NULL) {
        return false;
    }
    return fnmatch(glob, name, 0) == 0;
}

/* -----------------------------------------------------------------------
 * Search execution
 * ----------------------------------------------------------------------- */

/** Copy a bounded, possibly non-terminated name into a result. */
static void find_set_name(struct find_result *r,
                          const char *name, size_t name_len)
{
    if (name == NULL || name_len == 0U) {
        r->name[0] = '\0';
        return;
    }
    if (name_len > sizeof(r->name) - 1U) {
        name_len = sizeof(r->name) - 1U;
    }
    memcpy(r->name, name, name_len);
    r->name[name_len] = '\0';
}

/** Populate a result from a fully-read inode. */
static void find_fill_from_inode(struct find_result *r,
                                 const struct mds_inode *ino)
{
    r->fileid        = ino->fileid;
    r->parent_fileid = ino->parent_fileid;
    r->uid           = ino->uid;
    r->gid           = ino->gid;
    r->size          = ino->size;
    r->mtime_sec     = (int64_t)ino->mtime.tv_sec;
    r->ctime_sec     = (int64_t)ino->ctime.tv_sec;
    r->mode          = ino->mode;
    r->nlink         = ino->nlink;
    r->type          = (uint8_t)ino->type;
}

/**
 * Read one inode by fileid.
 *
 * mds_cat_ns_getattr hands back a heap-allocated ds_map that the caller
 * owns; the search never looks at the layout, so it is released here
 * rather than leaked once per match.
 *
 * @return MDS_OK when @p out was filled.
 */
static enum mds_status find_read_inode(struct mds_catalogue *cat,
                                       uint64_t fileid,
                                       struct mds_inode *out)
{
    enum mds_status st;

    memset(out, 0, sizeof(*out));
    st = mds_cat_ns_getattr(cat, fileid, out);
    if (st == MDS_OK) {
        free(out->ds_map);
        out->ds_map = NULL;
    }
    return st;
}

/** Apply the predicates that are not evaluated by the backend. */
static bool find_attrs_match(const struct find_filter *f,
                             const struct find_result *r)
{
    if (f->type != 0U && r->type != f->type)             { return false; }
    if (f->has_size_min && r->size < f->size_min)        { return false; }
    if (f->has_size_max && r->size > f->size_max)        { return false; }
    if (f->has_uid && r->uid != f->uid)                  { return false; }
    if (f->has_gid && r->gid != f->gid)                  { return false; }
    if (f->has_mtime_min && r->mtime_sec < f->mtime_min) { return false; }
    if (f->has_mtime_max && r->mtime_sec > f->mtime_max) { return false; }
    if (f->has_ctime_min && r->ctime_sec < f->ctime_min) { return false; }
    if (f->has_ctime_max && r->ctime_sec > f->ctime_max) { return false; }
    if (f->has_inum &&
        (r->fileid < f->inum_min || r->fileid > f->inum_max)) {
        return false;
    }
    return true;
}

#ifdef HAVE_RONDB

/** Effective result cap for a filter. */
static uint32_t find_effective_limit(const struct find_filter *f)
{
    if (f->limit == 0U) {
        return FIND_LIMIT_DEFAULT;
    }
    return (f->limit > FIND_LIMIT_MAX) ? FIND_LIMIT_MAX : f->limit;
}

/** True when any attribute (non-name) predicate is active. */
static bool find_has_attr_predicate(const struct find_filter *f)
{
    return f->type != 0U || f->has_size_min || f->has_size_max ||
           f->has_uid || f->has_gid ||
           f->has_mtime_min || f->has_mtime_max ||
           f->has_ctime_min || f->has_ctime_max ||
           f->has_inum;
}

/*
 * Both row callbacks issue keyed reads while the scan transaction is
 * still open.  This is the documented NDB scan-and-act pattern: an Ndb
 * object supports several concurrent NdbTransactions, and these reads
 * are LM_CommittedRead so they take no locks and cannot deadlock
 * against the scan.
 *
 * The one hard rule is that anything borrowed from the scan's result
 * buffer (notably the dirent name, which is neither NUL-terminated nor
 * stable across nextResult) must be copied out BEFORE issuing the
 * nested read.  Both callbacks below do that first.
 */

/** Shared state for both scan strategies. */
struct find_scan_ctx {
    struct mds_catalogue    *cat;
    const struct find_filter *filter;
    find_result_cb           cb;
    void                    *ctx;
    uint32_t                 limit;
    uint32_t                 emitted;
};

/**
 * Resolve an entry name for a scanned inode.
 *
 * @return true when @p r->name was populated.
 */
static bool find_resolve_name(struct find_scan_ctx *sc,
                              struct find_result *r)
{
    char name[MDS_MAX_NAME + 1];

    r->name[0] = '\0';
    if (r->parent_fileid == 0U) {
        return false;       /* root, or a pre-migration inode */
    }
    if (mds_cat_ns_dirent_name_for_child(sc->cat, r->parent_fileid,
                                         r->fileid, name,
                                         sizeof(name)) != MDS_OK) {
        return false;
    }
    name[sizeof(name) - 1U] = '\0';
    find_set_name(r, name, strlen(name));
    return true;
}

/** Deliver a match; returns non-zero when the scan should stop. */
static int find_emit(struct find_scan_ctx *sc, const struct find_result *r)
{
    if (sc->cb(r, sc->ctx) != 0) {
        return 1;
    }
    sc->emitted++;
    return (sc->emitted >= sc->limit) ? 1 : 0;
}

/** Per-row callback for the inode-scan strategy. */
static int find_inode_row_cb(const struct rondb_inode_scan_row *row,
                             void *arg)
{
    struct find_scan_ctx *sc = arg;
    struct find_result r;

    memset(&r, 0, sizeof(r));
    r.fileid        = row->fileid;
    r.parent_fileid = row->parent_fileid;
    r.uid           = row->uid;
    r.gid           = row->gid;
    r.size          = row->size;
    r.mtime_sec     = row->mtime_sec;
    r.ctime_sec     = row->ctime_sec;
    r.mode          = row->mode;
    r.nlink         = row->nlink;
    r.type          = row->type;

    /* Re-check every predicate locally.  The backend filter is an
     * optimisation, not the source of truth, and inum is never pushed. */
    if (!find_attrs_match(sc->filter, &r)) {
        return 0;
    }

    if (sc->filter->name_glob != NULL) {
        /* The name is not in the inode row, so it costs a keyed read.
         * Without a resolvable name the predicate cannot be evaluated,
         * and an unverified row must not be reported as a match. */
        if (!find_resolve_name(sc, &r) ||
            !find_name_matches(sc->filter->name_glob, r.name)) {
            return 0;
        }
    } else {
        (void)find_resolve_name(sc, &r);   /* best-effort, for display */
    }

    return find_emit(sc, &r);
}

/** Per-row callback for the dirent-scan (name-only) strategy. */
static int find_dirent_row_cb(uint64_t parent_fileid, uint64_t child_fileid,
                              uint8_t child_type, const char *name,
                              uint32_t name_len, void *arg)
{
    struct find_scan_ctx *sc = arg;
    struct find_result r;
    struct mds_inode ino;

    memset(&r, 0, sizeof(r));
    find_set_name(&r, name, name_len);
    if (!find_name_matches(sc->filter->name_glob, r.name)) {
        return 0;
    }

    /* The dirent carries no attributes.  One keyed read per surviving
     * match fills them, bounded by the result limit. */
    if (find_read_inode(sc->cat, child_fileid, &ino) != MDS_OK) {
        /* Removed between the scan and the read: it is no longer part of
         * the namespace, so it is not a match. */
        return 0;
    }
    find_fill_from_inode(&r, &ino);
    /* Trust the dirent for parentage: the inode's parent_fileid is 0 for
     * pre-migration rows, and a hardlinked inode names only one parent. */
    r.parent_fileid = parent_fileid;
    if (r.type == 0U) {
        r.type = child_type;
    }

    return find_emit(sc, &r);
}

#endif /* HAVE_RONDB */

/** Serve an exact single-fileid query with one keyed read. */
static enum mds_status find_by_fileid(struct mds_catalogue *cat,
                                      const struct find_filter *filter,
                                      find_result_cb cb, void *ctx)
{
    struct find_result r;
    struct mds_inode ino;
    enum mds_status st;

    st = find_read_inode(cat, filter->inum_min, &ino);
    if (st == MDS_ERR_NOTFOUND) {
        return MDS_OK;      /* no match is not an error */
    }
    if (st != MDS_OK) {
        return st;
    }

    memset(&r, 0, sizeof(r));
    find_fill_from_inode(&r, &ino);
    if (!find_attrs_match(filter, &r)) {
        return MDS_OK;
    }

    if (r.parent_fileid != 0U) {
        char name[MDS_MAX_NAME + 1];

        if (mds_cat_ns_dirent_name_for_child(cat, r.parent_fileid, r.fileid,
                                             name, sizeof(name)) == MDS_OK) {
            name[sizeof(name) - 1U] = '\0';
            find_set_name(&r, name, strlen(name));
        }
    }
    if (!find_name_matches(filter->name_glob, r.name)) {
        return MDS_OK;
    }

    (void)cb(&r, ctx);
    return MDS_OK;
}

enum mds_status find_query_run(struct mds_catalogue *cat,
                               const struct find_filter *filter,
                               find_result_cb cb, void *ctx)
{
    if (cat == NULL || filter == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    if (filter->has_inum && filter->inum_min > filter->inum_max) {
        return MDS_ERR_INVAL;
    }

    /* An exact fileid is a primary key: never scan for it. */
    if (filter->has_inum && filter->inum_min == filter->inum_max) {
        return find_by_fileid(cat, filter, cb, ctx);
    }

#ifdef HAVE_RONDB
    {
        struct find_scan_ctx sc;
        enum mds_status st;

        memset(&sc, 0, sizeof(sc));
        sc.cat = cat;
        sc.filter = filter;
        sc.cb = cb;
        sc.ctx = ctx;
        sc.limit = find_effective_limit(filter);

        if (filter->name_glob != NULL && !find_has_attr_predicate(filter)) {
            /* Name-only: the dirent table already holds the name, so
             * this avoids resolving one per candidate row. */
            char like[FIND_LIKE_MAX];
            int  n = find_glob_to_like(filter->name_glob,
                                       like, sizeof(like));

            st = catalogue_rondb_dirent_scan_name(
                cat, (n > 0) ? like : NULL, find_dirent_row_cb, &sc);
        } else {
            struct rondb_inode_scan_filter sf;

            memset(&sf, 0, sizeof(sf));
            sf.type = filter->type;
            sf.has_size_min  = filter->has_size_min;
            sf.size_min      = filter->size_min;
            sf.has_size_max  = filter->has_size_max;
            sf.size_max      = filter->size_max;
            sf.has_uid       = filter->has_uid;
            sf.uid           = filter->uid;
            sf.has_gid       = filter->has_gid;
            sf.gid           = filter->gid;
            sf.has_mtime_min = filter->has_mtime_min;
            sf.mtime_min     = filter->mtime_min;
            sf.has_mtime_max = filter->has_mtime_max;
            sf.mtime_max     = filter->mtime_max;
            sf.has_ctime_min = filter->has_ctime_min;
            sf.ctime_min     = filter->ctime_min;
            sf.has_ctime_max = filter->has_ctime_max;
            sf.ctime_max     = filter->ctime_max;

            st = catalogue_rondb_inode_scan(cat, &sf,
                                            find_inode_row_cb, &sc);
        }
        return st;
    }
#else
    return MDS_ERR_NOSUPPORT;
#endif
}

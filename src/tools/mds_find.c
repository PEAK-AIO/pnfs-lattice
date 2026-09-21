/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * mds_find.c -- Search the catalogue for entries matching attribute
 * predicates (type, size, times, uid, gid, name glob, fileid).
 *
 * Runs the query directly against the metadata backend through the
 * shared find_query core, so a search costs one table scan instead of
 * the millions of NFS round trips a client-side namespace walk needs.
 *
 * Read-only: this tool never mutates the catalogue.
 */

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "find_query.h"
#include "admin_util.h"

#define MDS_FIND_DEFAULT_CONF "/etc/pnfs-mds/mds.conf"

/** Seconds in a day, used by the relative -mtime / -ctime predicates. */
#define MDS_FIND_SECS_PER_DAY 86400LL

static void usage(const char *prog, int rc)
{
    (void)fprintf(rc == 0 ? stdout : stderr,
        "mds-find - search the pnfs-mds catalogue directly from the backend.\n"
        "\n"
        "Usage:\n"
        "  %s [options] [-type T] [-size +/-N] [-name GLOB] ...\n"
        "\n"
        "Predicates (all optional, AND-combined):\n"
        "  -type f|d|l|b|c|p|s  file type\n"
        "  -size +N|-N          size larger (+) or smaller (-) than N;\n"
        "                       N accepts a K/M/G/T/P suffix (binary)\n"
        "  -mtime +N|-N         modified more than (+) / within (-) N days\n"
        "  -ctime +N|-N         changed more than (+) / within (-) N days\n"
        "  -uid N               owner uid\n"
        "  -gid N               owner gid\n"
        "  -name GLOB           entry-name glob (* and ? wildcards)\n"
        "  -inum N[-M]          exact fileid, or an inclusive fileid range\n"
        "\n"
        "Options:\n"
        "  --config PATH        mds.conf path (default %s)\n"
        "  --limit N            max results, 1..%u (default %u)\n"
        "  --json               JSON-lines output\n"
        "  -h, --help           this help\n"
        "\n"
        "Output: one line per match\n"
        "  (type+mode nlink uid gid size mtime fileid name)\n"
        "\n"
        "Note: results are reported in backend scan order, which is not\n"
        "stable between runs.  Entries whose name cannot be resolved are\n"
        "shown as '?'.\n",
        prog, MDS_FIND_DEFAULT_CONF,
        (unsigned)FIND_LIMIT_MAX, (unsigned)FIND_LIMIT_DEFAULT);
    /* mds-find is single-threaded: exit() races no other thread. */
    exit(rc); /* NOLINT(concurrency-mt-unsafe) */
}

static void die(const char *msg)
{
    (void)fprintf(stderr, "mds-find: error: %s\n", msg);
    exit(1); /* NOLINT(concurrency-mt-unsafe) */
}

struct emit_ctx {
    bool     json;
    uint32_t count;
};

static void format_short_time(int64_t epoch_sec, char *out, size_t cap)
{
    time_t    t = (time_t)epoch_sec;
    struct tm tmv;

    if (cap == 0U) {
        return;
    }
    if (epoch_sec == 0 || localtime_r(&t, &tmv) == NULL ||
        strftime(out, cap, "%Y-%m-%d %H:%M", &tmv) == 0U) {
        (void)snprintf(out, cap, "-");
    }
}

static int emit_result(const struct find_result *r, void *arg)
{
    struct emit_ctx *ec = arg;
    char mode[FIND_MODE_STR_LEN];
    const char *name = (r->name[0] != '\0') ? r->name : "?";

    find_format_mode(r->mode, mode);

    if (ec->json) {
        char esc[4 * (MDS_MAX_NAME + 1)];

        if (json_escape_string(name, esc, sizeof(esc)) < 0) {
            esc[0] = '\0';
        }
        (void)printf("{\"fileid\":%" PRIu64 ",\"type\":\"%c\","
                     "\"mode\":\"%s\",\"nlink\":%u,"
                     "\"uid\":%" PRIu64 ",\"gid\":%" PRIu64 ","
                     "\"size\":%" PRIu64 ","
                     "\"mtime_sec\":%" PRId64 ",\"ctime_sec\":%" PRId64 ","
                     "\"parent_fileid\":%" PRIu64 ",\"name\":\"%s\"}\n",
                     r->fileid, find_type_char(r->type), mode, r->nlink,
                     r->uid, r->gid, r->size, r->mtime_sec, r->ctime_sec,
                     r->parent_fileid, esc);
    } else {
        char tbuf[32];

        format_short_time(r->mtime_sec, tbuf, sizeof(tbuf));
        (void)printf("%c%s %3u %5" PRIu64 " %5" PRIu64 " %12" PRIu64
                     " %s %10" PRIu64 " %s\n",
                     find_type_char(r->type), mode, r->nlink,
                     r->uid, r->gid, r->size, tbuf, r->fileid, name);
    }
    ec->count++;
    return 0;
}

/** Parse a relative +N / -N day bound into absolute epoch bounds. */
static void parse_day_bound(const char *arg, const char *flag,
                            int64_t *min_out, bool *has_min,
                            int64_t *max_out, bool *has_max)
{
    uint64_t days = 0U;
    int64_t  cutoff;
    char     msg[64];

    if (arg == NULL || (arg[0] != '+' && arg[0] != '-')) {
        (void)snprintf(msg, sizeof(msg), "%s requires +N or -N", flag);
        die(msg);
    }
    if (!find_parse_u64(arg + 1, &days)) {
        (void)snprintf(msg, sizeof(msg), "%s: N must be numeric", flag);
        die(msg);
    }
    /* Guard the multiply before it can overflow the signed cutoff. */
    if (days > (uint64_t)(INT64_MAX / MDS_FIND_SECS_PER_DAY)) {
        (void)snprintf(msg, sizeof(msg), "%s: N is too large", flag);
        die(msg);
    }
    cutoff = (int64_t)time(NULL) - (int64_t)days * MDS_FIND_SECS_PER_DAY;

    if (arg[0] == '+') {
        *max_out = cutoff;      /* older than N days */
        *has_max = true;
    } else {
        *min_out = cutoff;      /* within the last N days */
        *has_min = true;
    }
}

/** Parse -inum N or -inum N-M. */
static void parse_inum(const char *arg, struct find_filter *f)
{
    const char *dash;

    if (arg == NULL) {
        die("-inum requires a value");
    }
    dash = strchr(arg, '-');
    if (dash != NULL && dash != arg) {
        char lo[32];
        size_t ll = (size_t)(dash - arg);

        if (ll >= sizeof(lo)) {
            die("-inum range: value too long");
        }
        memcpy(lo, arg, ll);
        lo[ll] = '\0';
        if (!find_parse_u64(lo, &f->inum_min) ||
            !find_parse_u64(dash + 1, &f->inum_max)) {
            die("-inum range must be N-M (numeric)");
        }
        if (f->inum_min > f->inum_max) {
            die("-inum range: low bound exceeds high bound");
        }
    } else {
        if (!find_parse_u64(arg, &f->inum_min)) {
            die("-inum must be numeric");
        }
        f->inum_max = f->inum_min;
    }
    f->has_inum = true;
}

/* Flat argument dispatch; each branch is trivial.
 * NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void parse_args(int argc, char **argv, struct find_filter *f,
                       struct emit_ctx *ec, const char **conf)
{
    int i;

    for (i = 1; i < argc; i++) {
        const char *a = argv[i];
        bool needs_value = (strcmp(a, "-type") == 0 ||
                            strcmp(a, "-size") == 0 ||
                            strcmp(a, "-mtime") == 0 ||
                            strcmp(a, "-ctime") == 0 ||
                            strcmp(a, "-uid") == 0 ||
                            strcmp(a, "-gid") == 0 ||
                            strcmp(a, "-name") == 0 ||
                            strcmp(a, "-inum") == 0 ||
                            strcmp(a, "--limit") == 0 ||
                            strcmp(a, "--config") == 0);

        if (needs_value && i + 1 >= argc) {
            (void)fprintf(stderr, "mds-find: %s requires a value\n", a);
            usage(argv[0], 1);
        }

        if (strcmp(a, "-h") == 0 || strcmp(a, "--help") == 0) {
            usage(argv[0], 0);
        } else if (strcmp(a, "--json") == 0) {
            ec->json = true;
        } else if (strcmp(a, "--config") == 0) {
            *conf = argv[++i];
        } else if (strcmp(a, "-type") == 0) {
            int t = find_parse_type_char(argv[++i]);

            if (t < 0) {
                die("-type must be one of f/d/l/b/c/p/s");
            }
            f->type = (uint8_t)t;
        } else if (strcmp(a, "-size") == 0) {
            const char *sv = argv[++i];

            if (sv[0] == '+') {
                if (!find_parse_size(sv + 1, &f->size_min)) {
                    die("-size +N: bad size");
                }
                f->has_size_min = true;
            } else if (sv[0] == '-') {
                if (!find_parse_size(sv + 1, &f->size_max)) {
                    die("-size -N: bad size");
                }
                f->has_size_max = true;
            } else {
                die("-size requires +N or -N");
            }
        } else if (strcmp(a, "-mtime") == 0) {
            parse_day_bound(argv[++i], "-mtime",
                            &f->mtime_min, &f->has_mtime_min,
                            &f->mtime_max, &f->has_mtime_max);
        } else if (strcmp(a, "-ctime") == 0) {
            parse_day_bound(argv[++i], "-ctime",
                            &f->ctime_min, &f->has_ctime_min,
                            &f->ctime_max, &f->has_ctime_max);
        } else if (strcmp(a, "-uid") == 0) {
            if (!find_parse_u64(argv[++i], &f->uid)) {
                die("-uid must be numeric");
            }
            f->has_uid = true;
        } else if (strcmp(a, "-gid") == 0) {
            if (!find_parse_u64(argv[++i], &f->gid)) {
                die("-gid must be numeric");
            }
            f->has_gid = true;
        } else if (strcmp(a, "-name") == 0) {
            f->name_glob = argv[++i];
        } else if (strcmp(a, "-inum") == 0) {
            parse_inum(argv[++i], f);
        } else if (strcmp(a, "--limit") == 0) {
            uint64_t v = 0U;

            if (!find_parse_u64(argv[++i], &v) ||
                v == 0U || v > (uint64_t)FIND_LIMIT_MAX) {
                die("--limit must be 1..10000000");
            }
            f->limit = (uint32_t)v;
        } else {
            (void)fprintf(stderr, "mds-find: unknown option: %s\n", a);
            usage(argv[0], 1);
        }
    }
}

int main(int argc, char **argv)
{
    struct find_filter filter;
    struct emit_ctx ec;
    struct mds_config cfg;
    struct mds_catalogue *cat = NULL;
    const char *conf = MDS_FIND_DEFAULT_CONF;
    enum mds_status st;

    memset(&filter, 0, sizeof(filter));
    memset(&ec, 0, sizeof(ec));

    parse_args(argc, argv, &filter, &ec, &conf);

    st = mds_config_load(conf, &cfg);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "mds-find: cannot load config %s: %s\n",
                      conf, mds_status_str(st));
        return 1;
    }

    st = mds_catalogue_open(&cfg, &cat);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "mds-find: catalogue open failed: %s\n",
                      mds_status_str(st));
        return 1;
    }

    st = find_query_run(cat, &filter, emit_result, &ec);
    if (st != MDS_OK) {
        (void)fprintf(stderr, "mds-find: search failed: %s\n",
                      mds_status_str(st));
        mds_catalogue_close(cat);
        return 1;
    }

    if (!ec.json) {
        (void)fprintf(stderr, "mds-find: %u match%s\n",
                      ec.count, (ec.count == 1U) ? "" : "es");
    }

    mds_catalogue_close(cat);
    return 0;
}

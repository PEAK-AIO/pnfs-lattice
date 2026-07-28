/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * admin_companion.c -- `mds-admin companion` subcommands.
 *
 *   companion list    [--json]
 *   companion status  <name> [--json]
 *   companion start   <name>
 *   companion stop    <name>
 *   companion restart <name>
 *   companion budget  [--json] [--total-api-slots N]
 *
 * Every request travels over the existing cluster-transport admin
 * endpoint (--mds-host / --mds-port).  Control requests name a
 * companion that mds.conf already declares; adding a new companion is
 * an mds.conf edit plus a daemon restart, by design.
 */

#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "pnfs_mds.h"
#include "cluster_transport.h"
#include "companion.h"

#include "admin_companion.h"
#include "admin_util.h"

/* -----------------------------------------------------------------------
 * Formatting helpers
 * ----------------------------------------------------------------------- */

static const char *state_name(uint8_t state)
{
    switch (state) {
    case COMPANION_STATE_STOPPED:  return "stopped";
    case COMPANION_STATE_STARTING: return "starting";
    case COMPANION_STATE_RUNNING:  return "running";
    case COMPANION_STATE_BACKOFF:  return "backoff";
    case COMPANION_STATE_FAILED:   return "failed";
    case COMPANION_STATE_DISABLED: return "disabled";
    default:                       return "unknown";
    }
}

static const char *admission_name(uint8_t admission)
{
    return (admission == COMPANION_NDB_ENFORCE) ? "enforce" : "advisory";
}

/**
 * Render a unix timestamp as UTC ISO 8601, or "-" when zero.
 *
 * @param unix_sec  Seconds since the epoch; 0 means "never".
 * @param buf       Output buffer.
 * @param cap       Capacity of @a buf (>= 21 recommended).
 */
static void fmt_time(uint64_t unix_sec, char *buf, size_t cap)
{
    time_t t;
    struct tm tm_utc;

    if (cap == 0) {
        return;
    }
    if (unix_sec == 0) {
        (void)snprintf(buf, cap, "-");
        return;
    }
    t = (time_t)unix_sec;
    if (gmtime_r(&t, &tm_utc) == NULL ||
        strftime(buf, cap, "%Y-%m-%dT%H:%M:%SZ", &tm_utc) == 0) {
        (void)snprintf(buf, cap, "-");
    }
}

/** Describe the last exit compactly: "0", "sig 15", or "-". */
static void fmt_exit(const struct companion_status *s, char *buf, size_t cap)
{
    if (cap == 0) {
        return;
    }
    if (s->last_term_signal > 0) {
        (void)snprintf(buf, cap, "sig %d", (int)s->last_term_signal);
    } else if (s->last_exit_unix != 0) {
        (void)snprintf(buf, cap, "%d", (int)s->last_exit_code);
    } else {
        (void)snprintf(buf, cap, "-");
    }
}

/** Locate a bare (non-flag) positional argument at index @a want. */
static const char *positional(int argc, const char *const *argv, int want)
{
    int seen = 0;

    for (int i = 0; i < argc; i++) {
        if (argv[i][0] == '-') {
            /*
             * Skip the flag and, for the value-taking flags this
             * command family accepts, its argument too.
             */
            if ((strcmp(argv[i], "--mds-host") == 0 ||
                 strcmp(argv[i], "--mds-port") == 0 ||
                 strcmp(argv[i], "--total-api-slots") == 0) &&
                i + 1 < argc) {
                i++;
            }
            continue;
        }
        if (seen == want) {
            return argv[i];
        }
        seen++;
    }
    return NULL;
}

/* -----------------------------------------------------------------------
 * companion list
 * ----------------------------------------------------------------------- */

static int cmd_companion_list(int argc, const char *const *argv)
{
    const char *host = NULL;
    uint16_t port = 0;
    bool json = false;
    struct companion_status *list = NULL;
    uint32_t count = 0;
    enum mds_status st;

    if (parse_admin_endpoint(argc, argv, &host, &port, &json) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }

    st = cluster_transport_request_companion_list(host, port,
                                                 &list, &count);
    if (st != MDS_OK) {
        (void)fprintf(stderr,
            "Error: companion list failed (%s)\n", mds_status_str(st));
        return 1;
    }

    if (json) {
        (void)printf("[\n");
        for (uint32_t i = 0; i < count; i++) {
            char esc[MDS_COMPANION_NAME_MAX * 6];
            char started[32];
            char exited[32];

            if (json_escape_string(list[i].name, esc, sizeof(esc)) < 0) {
                (void)fprintf(stderr, "Error: name too long for JSON\n");
                free(list);
                return 1;
            }
            fmt_time(list[i].started_at_unix, started, sizeof(started));
            fmt_time(list[i].last_exit_unix, exited, sizeof(exited));

            (void)printf(
                "  {\n"
                "    \"name\": \"%s\",\n"
                "    \"state\": \"%s\",\n"
                "    \"pid\": %d,\n"
                "    \"restarts\": %u,\n"
                "    \"last_exit_code\": %d,\n"
                "    \"last_term_signal\": %d,\n"
                "    \"started_at\": \"%s\",\n"
                "    \"last_exit_at\": \"%s\",\n"
                "    \"rss_kb\": %" PRIu64 ",\n"
                "    \"ndb_conns\": %u\n"
                "  }%s\n",
                esc, state_name(list[i].state), (int)list[i].pid,
                (unsigned)list[i].restart_count,
                (int)list[i].last_exit_code,
                (int)list[i].last_term_signal,
                started, exited, list[i].rss_kb,
                (unsigned)list[i].ndb_conns,
                (i + 1 < count) ? "," : "");
        }
        (void)printf("]\n");
    } else if (count == 0) {
        (void)printf("No companions declared.\n");
    } else {
        (void)printf("%-24s  %-9s  %-7s  %-8s  %-8s  %-9s  %s\n",
                     "NAME", "STATE", "PID", "RESTARTS", "LAST_EXIT",
                     "RSS_MB", "NDB_CONNS");
        for (uint32_t i = 0; i < count; i++) {
            char exited[32];

            fmt_exit(&list[i], exited, sizeof(exited));
            (void)printf("%-24s  %-9s  %-7d  %-8u  %-8s  %-9" PRIu64
                         "  %u\n",
                         list[i].name, state_name(list[i].state),
                         (int)list[i].pid,
                         (unsigned)list[i].restart_count, exited,
                         list[i].rss_kb / 1024U,
                         (unsigned)list[i].ndb_conns);
        }
    }

    free(list);
    return 0;
}

/* -----------------------------------------------------------------------
 * companion status <name>
 * ----------------------------------------------------------------------- */

static int cmd_companion_status(int argc, const char *const *argv)
{
    const char *host = NULL;
    uint16_t port = 0;
    bool json = false;
    const char *name;
    struct companion_status *list = NULL;
    uint32_t count = 0;
    const struct companion_status *found = NULL;
    enum mds_status st;
    char started[32];
    char exited[32];
    int rc = 0;

    if (parse_admin_endpoint(argc, argv, &host, &port, &json) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }
    name = positional(argc, argv, 0);
    if (name == NULL) {
        (void)fprintf(stderr,
            "Usage: mds-admin companion status <name> [--json]\n");
        return 1;
    }

    /*
     * The daemon has no single-companion query: the list is bounded by
     * MDS_MAX_COMPANIONS, so one round trip plus a local match is
     * cheaper than adding another message type.
     */
    st = cluster_transport_request_companion_list(host, port,
                                                 &list, &count);
    if (st != MDS_OK) {
        (void)fprintf(stderr,
            "Error: companion status failed (%s)\n", mds_status_str(st));
        return 1;
    }
    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(list[i].name, name) == 0) {
            found = &list[i];
            break;
        }
    }
    if (found == NULL) {
        (void)fprintf(stderr,
            "Error: no companion named '%s' is declared on that node\n",
            name);
        free(list);
        return 1;
    }

    fmt_time(found->started_at_unix, started, sizeof(started));
    fmt_time(found->last_exit_unix, exited, sizeof(exited));

    if (json) {
        char esc[MDS_COMPANION_NAME_MAX * 6];

        if (json_escape_string(found->name, esc, sizeof(esc)) < 0) {
            (void)fprintf(stderr, "Error: name too long for JSON\n");
            free(list);
            return 1;
        }
        (void)printf(
            "{\n"
            "  \"name\": \"%s\",\n"
            "  \"state\": \"%s\",\n"
            "  \"pid\": %d,\n"
            "  \"restarts\": %u,\n"
            "  \"last_exit_code\": %d,\n"
            "  \"last_term_signal\": %d,\n"
            "  \"started_at\": \"%s\",\n"
            "  \"last_exit_at\": \"%s\",\n"
            "  \"rss_kb\": %" PRIu64 ",\n"
            "  \"ndb_conns\": %u\n"
            "}\n",
            esc, state_name(found->state), (int)found->pid,
            (unsigned)found->restart_count,
            (int)found->last_exit_code, (int)found->last_term_signal,
            started, exited, found->rss_kb,
            (unsigned)found->ndb_conns);
    } else {
        (void)printf("Name:            %s\n", found->name);
        (void)printf("State:           %s\n", state_name(found->state));
        if (found->pid > 0) {
            (void)printf("PID:             %d\n", (int)found->pid);
        } else {
            (void)printf("PID:             -\n");
        }
        (void)printf("Restarts:        %u\n",
                     (unsigned)found->restart_count);
        fmt_exit(found, exited, sizeof(exited));
        (void)printf("Last exit:       %s\n", exited);
        fmt_time(found->last_exit_unix, exited, sizeof(exited));
        (void)printf("Last exit at:    %s\n", exited);
        (void)printf("Started at:      %s\n", started);
        (void)printf("RSS:             %" PRIu64 " KiB\n", found->rss_kb);
        (void)printf("NDB conns:       %u (declared)\n",
                     (unsigned)found->ndb_conns);
    }

    free(list);
    return rc;
}

/* -----------------------------------------------------------------------
 * companion start|stop|restart <name>
 * ----------------------------------------------------------------------- */

static int cmd_companion_ctl(int argc, const char *const *argv,
                             uint8_t action, const char *verb)
{
    const char *host = NULL;
    uint16_t port = 0;
    bool json = false;
    const char *name;
    enum mds_status st;

    if (parse_admin_endpoint(argc, argv, &host, &port, &json) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }
    name = positional(argc, argv, 0);
    if (name == NULL) {
        (void)fprintf(stderr,
            "Usage: mds-admin companion %s <name>\n", verb);
        return 1;
    }

    st = cluster_transport_request_companion_ctl(host, port, action, name);
    if (st == MDS_OK) {
        (void)printf("companion '%s': %s requested\n", name, verb);
        return 0;
    }

    /* Translate the daemon's status into operator-actionable text. */
    switch (st) {
    case MDS_ERR_NOTFOUND:
        (void)fprintf(stderr,
            "Error: no companion named '%s' is declared on that node. "
            "Companions come from `companion.<name>.*` keys in mds.conf; "
            "adding one needs a daemon restart.\n", name);
        break;
    case MDS_ERR_EXISTS:
        (void)fprintf(stderr,
            "Error: companion '%s' is already running "
            "(use `companion restart %s`)\n", name, name);
        break;
    case MDS_ERR_PERM:
        (void)fprintf(stderr,
            "Error: companion '%s' is declared with enabled=false\n",
            name);
        break;
    case MDS_ERR_NOSPC:
        (void)fprintf(stderr,
            "Error: starting '%s' would exceed the declared NDB API slot "
            "budget and companion_ndb_admission=enforce. Check "
            "`mds-admin companion budget`.\n", name);
        break;
    case MDS_ERR_NOSUPPORT:
        (void)fprintf(stderr,
            "Error: companion supervision is not active on that node "
            "(built with ENABLE_COMPANION=OFF, companion_enabled=false, "
            "or no companions declared)\n");
        break;
    default:
        (void)fprintf(stderr,
            "Error: companion %s failed (%s)\n", verb, mds_status_str(st));
        break;
    }
    return 1;
}

/* -----------------------------------------------------------------------
 * companion budget
 * ----------------------------------------------------------------------- */

/** Parse the display-only --total-api-slots override. */
static int parse_total_override(int argc, const char *const *argv,
                                uint32_t *out, bool *have)
{
    *have = false;
    for (int i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--total-api-slots") == 0) {
            char *end = NULL;
            unsigned long v;

            if (i + 1 >= argc) {
                return -1;
            }
            v = strtoul(argv[i + 1], &end, 10);
            if (end == argv[i + 1] || (end != NULL && *end != '\0') ||
                v > 255UL) {
                return -1;
            }
            *out = (uint32_t)v;
            *have = true;
            return 0;
        }
    }
    return 0;
}

static int cmd_companion_budget(int argc, const char *const *argv)
{
    const char *host = NULL;
    uint16_t port = 0;
    bool json = false;
    struct companion_budget b;
    uint32_t total_override = 0;
    bool have_override = false;
    enum mds_status st;

    if (parse_admin_endpoint(argc, argv, &host, &port, &json) != 0) {
        (void)fprintf(stderr, "Error: invalid --mds-port\n");
        return 1;
    }
    if (parse_total_override(argc, argv, &total_override,
                             &have_override) != 0) {
        (void)fprintf(stderr,
            "Error: --total-api-slots needs a value in 0..255\n");
        return 1;
    }

    st = cluster_transport_request_companion_budget(host, port, &b);
    if (st != MDS_OK) {
        (void)fprintf(stderr,
            "Error: companion budget failed (%s)\n", mds_status_str(st));
        return 1;
    }

    /*
     * --total-api-slots is a local what-if: recompute free slots
     * against the operator's number without touching the daemon,
     * since the configured total only changes on restart.
     */
    if (have_override) {
        int64_t free_slots = (int64_t)total_override -
                             (int64_t)b.slots_mds -
                             (int64_t)b.slots_reserved -
                             (int64_t)b.slots_running;

        b.slots_total = total_override;
        b.slots_free = (int32_t)free_slots;
    }

    if (json) {
        (void)printf(
            "{\n"
            "  \"api_slots_total\": %u,\n"
            "  \"api_slots_mds\": %u,\n"
            "  \"api_slots_reserved\": %u,\n"
            "  \"api_slots_companions_declared\": %u,\n"
            "  \"api_slots_companions_running\": %u,\n"
            "  \"api_slots_free\": %d,\n"
            "  \"companion_count\": %u,\n"
            "  \"running_count\": %u,\n"
            "  \"admission\": \"%s\",\n"
            "  \"total_declared\": %s\n"
            "}\n",
            (unsigned)b.slots_total, (unsigned)b.slots_mds,
            (unsigned)b.slots_reserved, (unsigned)b.slots_declared,
            (unsigned)b.slots_running, (int)b.slots_free,
            (unsigned)b.companion_count, (unsigned)b.running_count,
            admission_name(b.admission),
            (b.slots_total > 0) ? "true" : "false");
        return 0;
    }

    (void)printf("NDB API slots (declared accounting, this node only):\n");
    if (b.slots_total > 0) {
        (void)printf("  total:                 %u%s\n",
                     (unsigned)b.slots_total,
                     have_override ? " (--total-api-slots override)" : "");
    } else {
        (void)printf("  total:                 undeclared "
                     "(set ndb_api_slots_total in mds.conf)\n");
    }
    (void)printf("  mds pool:              %u\n", (unsigned)b.slots_mds);
    (void)printf("  reserved:              %u\n",
                 (unsigned)b.slots_reserved);
    (void)printf("  companions declared:   %u (%u companion(s))\n",
                 (unsigned)b.slots_declared,
                 (unsigned)b.companion_count);
    (void)printf("  companions running:    %u (%u running)\n",
                 (unsigned)b.slots_running,
                 (unsigned)b.running_count);
    if (b.slots_total > 0) {
        (void)printf("  free:                  %d%s\n",
                     (int)b.slots_free,
                     (b.slots_free < 0) ? "  *** OVERSUBSCRIBED ***" : "");
    } else {
        (void)printf("  free:                  unknown\n");
    }
    (void)printf("  admission:             %s\n",
                 admission_name(b.admission));
    (void)printf("\nNote: peer MDS daemons and their companions are not "
                 "visible here.\n"
                 "Use ndb_api_slots_reserved to reserve headroom for "
                 "them and for\ntransient tools. Declared connection "
                 "counts are operator-supplied\nintent, not a "
                 "measurement.\n");
    return 0;
}

/* -----------------------------------------------------------------------
 * Dispatch
 * ----------------------------------------------------------------------- */

int dispatch_companion(int argc, const char *const *argv)
{
    if (argc < 3) {
        return -1;
    }

    /* argv[0]=mds-admin argv[1]=companion argv[2]=subcommand */
    if (strcmp(argv[2], "list") == 0) {
        return cmd_companion_list(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "status") == 0) {
        return cmd_companion_status(argc - 3, argv + 3);
    }
    if (strcmp(argv[2], "start") == 0) {
        return cmd_companion_ctl(argc - 3, argv + 3,
                                 CT_COMPANION_ACTION_START, "start");
    }
    if (strcmp(argv[2], "stop") == 0) {
        return cmd_companion_ctl(argc - 3, argv + 3,
                                 CT_COMPANION_ACTION_STOP, "stop");
    }
    if (strcmp(argv[2], "restart") == 0) {
        return cmd_companion_ctl(argc - 3, argv + 3,
                                 CT_COMPANION_ACTION_RESTART, "restart");
    }
    if (strcmp(argv[2], "budget") == 0) {
        return cmd_companion_budget(argc - 3, argv + 3);
    }
    return -1;
}

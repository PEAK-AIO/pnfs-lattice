/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * ds_health.c — Data server health monitoring.
 *
 * Implements periodic NFS NULL probing and failure detection.
 */

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <fcntl.h>
#include <signal.h>

#include "ds_health.h"
#include "mds_catalogue.h"
#include "commit_queue.h"
#include "mds_shard.h"

/* -----------------------------------------------------------------------
 * Constants
 * ----------------------------------------------------------------------- */

#define DS_HEALTH_DEFAULT_INTERVAL  5000
#define DS_HEALTH_DEFAULT_THRESHOLD 6
#define DS_HEALTH_MAX_DS            256

/* Anti-flapping: recovery requires consecutive successful probes. */
#define DS_HEALTH_RECOVERY_MIN      3

/* Anti-flapping: cooldown period after marking OFFLINE (milliseconds).
 * During cooldown, the DS is not probed for recovery.  Each successive
 * flap doubles the cooldown up to DS_HEALTH_COOLDOWN_CAP_MS. */
#define DS_HEALTH_COOLDOWN_BASE_MS  30000ULL   /* 30 seconds */
#define DS_HEALTH_COOLDOWN_CAP_MS   480000ULL  /* 8 minutes  */
#define DS_HEALTH_FLAP_BACKOFF_MAX  4          /* 2^4 = 16x multiplier */

/* If a DS stays ONLINE for this long, reset its flap counter. */
#define DS_HEALTH_FLAP_RESET_NS     (60ULL * 1000000000ULL) /* 60 seconds */

/* RPC constants for NFS NULL probe. */
#define NFS_PROGRAM   100003
#define NFS_VERSION   3   /* DSes export NFSv3; v4 NULL is rejected */
#define NFS_NULL_PROC 0
#define RPC_CALL      0
#define RPC_MSG_VER   2
#define AUTH_NONE     0

/* -----------------------------------------------------------------------
 * Per-DS failure counter (simple array, indexed by ds_id)
 * ----------------------------------------------------------------------- */

struct ds_fail_state {
    uint32_t ds_id;
    uint32_t consecutive_failures;
    bool     active;          /* slot in use */
    bool     marked_offline;  /* internal offline flag (catalogue-independent) */

    /* Anti-flapping state. */
    uint32_t recovery_successes;  /* consecutive OK probes while OFFLINE */
    uint64_t cooldown_until_ns;   /* monotonic ns: skip probes until then */
    uint32_t flap_count;          /* OFFLINE→ONLINE→OFFLINE transitions */
    uint64_t last_online_ns;      /* when DS last transitioned to ONLINE */
};

/* -----------------------------------------------------------------------
 * Monitor structure
 * ----------------------------------------------------------------------- */

struct ds_health_monitor {
    struct mds_catalogue  *cat;
    struct commit_queue   *cq;
    uint32_t               interval_ms;
    uint32_t               fail_threshold;
    ds_fail_cb             fail_cb;
    void                  *fail_ctx;

    pthread_t              thread;
    _Atomic int            running;
    int                    stop_pipe[2]; /* write to wake polling thread */

    struct ds_fail_state   states[DS_HEALTH_MAX_DS];
    pthread_mutex_t        state_lock;
};

/* -----------------------------------------------------------------------
 * Monotonic clock helper
 * ----------------------------------------------------------------------- */

static uint64_t get_monotonic_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* -----------------------------------------------------------------------
 * Address parser
 * ----------------------------------------------------------------------- */

int ds_addr_parse_host(const char *addr, char *host, size_t host_len)
{
    const char *colon;
    size_t len;

    if (addr == NULL || host == NULL || host_len == 0) {
        return -1;
}

    colon = strchr(addr, ':');
    if (colon == NULL || colon == addr) {
        return -1;
}

    len = (size_t)(colon - addr);
    if (len >= host_len) {
        return -1;
}

    memcpy(host, addr, len);
    host[len] = '\0';
    return 0;
}

/* -----------------------------------------------------------------------
 * NFS NULL probe
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int ds_probe_null(const char *host, uint16_t port, uint32_t timeout_ms)
{
    struct addrinfo hints, *res = NULL;
    int fd = -1;
    int rc = -1;
    char port_str[8];

    if (host == NULL || port == 0) {
        return -1;
}

    (void)snprintf(port_str, sizeof(port_str), "%u", port);

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host, port_str, &hints, &res) != 0) {
        return -1;
}

    fd = socket(res->ai_family, SOCK_STREAM, 0);
    if (fd < 0) {
        goto out;
}

    /* Non-blocking connect with poll-based timeout. */
    {
        int flags = fcntl(fd, F_GETFL, 0);

        if (flags < 0) {
            goto out;
}
        if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
            goto out;
}
    }

    /* Disable Nagle. */
    {
        int val = 1;

        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
    }

    {
        int crc = connect(fd, res->ai_addr, res->ai_addrlen);

        if (crc != 0 && errno != EINPROGRESS) {
            goto out;
}
        if (crc != 0) {
            /* Wait for connect to complete or timeout. */
            struct pollfd cpfd = { .fd = fd, .events = POLLOUT };
            int pr = poll(&cpfd, 1, (int)timeout_ms);

            if (pr <= 0) {
                goto out;
}
            /* Check for connect error. */
            {
                int so_err = 0;
                socklen_t so_len = sizeof(so_err);

                if (getsockopt(fd, SOL_SOCKET, SO_ERROR,
                               &so_err, &so_len) != 0 || so_err != 0) {
                    goto out;
}
            }
        }
    }

    /* Restore blocking mode + set recv timeout for RPC exchange. */
    {
        int flags = fcntl(fd, F_GETFL, 0);

        if (flags >= 0) {
            (void)fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
}
        struct timeval tv;
        tv.tv_sec = timeout_ms / 1000;
        /* NOLINTNEXTLINE(bugprone-implicit-widening-of-multiplication-result) */
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    }

    /* Build RPC NULL call: record-marked message.
     * RPC header: xid(4) + msgtype(4) + rpcvers(4) + prog(4) +
     *             vers(4) + proc(4) + cred_flavor(4) + cred_len(4) +
     *             verf_flavor(4) + verf_len(4) = 40 bytes. */
    {
        uint32_t msg[10];
        static _Atomic uint32_t probe_xid = 100;
        uint32_t xid = atomic_fetch_add(&probe_xid, 1);

        msg[0] = htonl(xid);
        msg[1] = htonl(RPC_CALL);
        msg[2] = htonl(RPC_MSG_VER);
        msg[3] = htonl(NFS_PROGRAM);
        msg[4] = htonl(NFS_VERSION);
        msg[5] = htonl(NFS_NULL_PROC);
        msg[6] = htonl(AUTH_NONE);  /* cred flavor */
        msg[7] = htonl(0);          /* cred body len */
        msg[8] = htonl(AUTH_NONE);  /* verf flavor */
        msg[9] = htonl(0);          /* verf body len */

        /* Record-marked header (last fragment, 40 bytes). */
        uint32_t frag_hdr = htonl(40u | 0x80000000u);
        struct iovec iov[2];

        iov[0].iov_base = &frag_hdr;
        iov[0].iov_len = 4;
        iov[1].iov_base = msg;
        iov[1].iov_len = 40;

        struct msghdr msghdr;
        memset(&msghdr, 0, sizeof(msghdr));
        msghdr.msg_iov = iov;
        msghdr.msg_iovlen = 2;

        ssize_t sent = sendmsg(fd, &msghdr, MSG_NOSIGNAL);
        if (sent != 44) {
            goto out;
}
    }

    /* Read reply (any valid reply means DS is alive). */
    {
        uint8_t buf[128];
        struct pollfd pfd = { .fd = fd, .events = POLLIN };

        if (poll(&pfd, 1, (int)timeout_ms) <= 0) {
            goto out;
}

        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n >= 4) { /* at least a fragment header */
            rc = 0;
}
    }

out:
    if (fd >= 0) {
        close(fd);
}
    if (res != NULL) {
        freeaddrinfo(res);
}
    return rc;
}

/* -----------------------------------------------------------------------
 * Internal: per-DS state management
 * ----------------------------------------------------------------------- */

static struct ds_fail_state *find_or_create_state(
    struct ds_health_monitor *hm, uint32_t ds_id)
{
    uint32_t i;

    /* Find existing. */
    for (i = 0; i < DS_HEALTH_MAX_DS; i++) {
        if (hm->states[i].active && hm->states[i].ds_id == ds_id) {
            return &hm->states[i];
}
    }
    /* Find free slot. */
    for (i = 0; i < DS_HEALTH_MAX_DS; i++) {
        if (!hm->states[i].active) {
            hm->states[i].active = true;
            hm->states[i].ds_id = ds_id;
            hm->states[i].consecutive_failures = 0;
            return &hm->states[i];
        }
    }
    return NULL; /* full */
}

/**
 * Transition a DS to offline via CQ, or direct catalogue vtable.
 * When CQ is available, the state change is replicated.
 * Otherwise, dispatch through the catalogue vtable.
 */
static void mark_ds_offline(struct ds_health_monitor *hm, uint32_t ds_id)
{
    if (hm->cq != NULL) {
        struct commit_op cop;

        memset(&cop, 0, sizeof(cop));
        cop.type = COMMIT_OP_DS_STATE;
        cop.args.ds_state.ds_id = ds_id;
        cop.args.ds_state.new_state = DS_OFFLINE;
        (void)commit_queue_submit(hm->cq, &cop);
    } else if (hm->cat != NULL) {
        /* Direct path via catalogue vtable. */
        struct mds_ds_info info;

        if (mds_cat_ds_get(hm->cat, ds_id, &info) == MDS_OK) {
            info.state = DS_OFFLINE;
            (void)mds_cat_ds_put(hm->cat, NULL, &info);
        }
    }
}

/** Handle a failure event for a DS (thread-safe). */
static void handle_ds_failure(struct ds_health_monitor *hm, uint32_t ds_id,
                              uint32_t increment)
{
    struct ds_fail_state *fs;

    pthread_mutex_lock(&hm->state_lock);
    fs = find_or_create_state(hm, ds_id);
    if (fs == NULL) {
        pthread_mutex_unlock(&hm->state_lock);
        return;
    }

    /* If already offline, suppress further failure accumulation.
     * Client LAYOUTERROR reports can flood in while the DS is down;
     * additional offline transitions are pointless and just spam the
     * log.  Recovery probes will bring it back when it stabilises. */
    if (fs->marked_offline) {
        fs->recovery_successes = 0;
        pthread_mutex_unlock(&hm->state_lock);
        return;
    }

    /* Any failure while recovering resets the recovery counter. */
    fs->recovery_successes = 0;

    fs->consecutive_failures += increment;
    if (fs->consecutive_failures >= hm->fail_threshold) {
        uint32_t exp;
        uint64_t cooldown_ms;
        uint64_t now_ns;

        fs->consecutive_failures = 0; /* reset after trigger */

        /* Compute cooldown with exponential backoff per flap. */
        exp = (fs->flap_count < DS_HEALTH_FLAP_BACKOFF_MAX)
              ? fs->flap_count : DS_HEALTH_FLAP_BACKOFF_MAX;
        cooldown_ms = DS_HEALTH_COOLDOWN_BASE_MS << exp;
        if (cooldown_ms > DS_HEALTH_COOLDOWN_CAP_MS) {
            cooldown_ms = DS_HEALTH_COOLDOWN_CAP_MS;
        }
        now_ns = get_monotonic_ns();
        fs->cooldown_until_ns = now_ns + cooldown_ms * 1000000ULL;
        fs->flap_count++;
        fs->marked_offline = true;

        pthread_mutex_unlock(&hm->state_lock);

        /* Mark offline (may block on CQ). */
        (void)fprintf(stderr,
            "WARN: DS %u failed health check "
            "(%u consecutive failures) — marking OFFLINE "
            "(cooldown %llus, flap #%u)\n",
            (unsigned)ds_id,
            (unsigned)hm->fail_threshold,
            (unsigned long long)(cooldown_ms / 1000),
            (unsigned)fs->flap_count);
        mark_ds_offline(hm, ds_id);

        /* Invoke failure callback. */
        if (hm->fail_cb != NULL) {
            hm->fail_cb(ds_id, hm->fail_ctx);
        }
        return;
    }
    pthread_mutex_unlock(&hm->state_lock);
}

/** Reset failure counter on successful probe. */
static void handle_ds_success(struct ds_health_monitor *hm, uint32_t ds_id)
{
    uint32_t i;

    pthread_mutex_lock(&hm->state_lock);
    for (i = 0; i < DS_HEALTH_MAX_DS; i++) {
        if (hm->states[i].active && hm->states[i].ds_id == ds_id) {
            hm->states[i].consecutive_failures = 0;
            break;
        }
    }
    pthread_mutex_unlock(&hm->state_lock);
}

/* -----------------------------------------------------------------------
 * Polling thread
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static void *health_poll_thread(void *arg)
{
    struct ds_health_monitor *hm = arg;

    /* Block all signals in this thread.  The NDB API sends signals
     * (e.g. SIGUSR1) to wake internal threads, and those signals
     * interrupt poll()/recv()/connect() in our probe code, causing
     * false probe failures.  This thread does not need any signals;
     * shutdown is via the stop_pipe. */
    {
        sigset_t mask;
        sigfillset(&mask);
        pthread_sigmask(SIG_BLOCK, &mask, NULL);
    }

    while (atomic_load(&hm->running)) {
        /* Sleep for interval, but wake on stop_pipe. */
        struct pollfd pfd = { .fd = hm->stop_pipe[0], .events = POLLIN };
        int pr = poll(&pfd, 1, (int)hm->interval_ms);

        if (pr > 0) {
            break; /* stop signal */
        }
        if (!atomic_load(&hm->running)) {
            break;
        }

        /* List all DS_ONLINE servers and probe each. */
        struct mds_ds_info *ds_list = NULL;
        uint32_t ds_count = 0;

        if (mds_cat_ds_list(hm->cat, &ds_list, &ds_count) != MDS_OK) {
            continue;
}

        uint64_t now_ns = get_monotonic_ns();
        uint32_t recovery_threshold = hm->fail_threshold / 2;

        if (recovery_threshold < DS_HEALTH_RECOVERY_MIN) {
            recovery_threshold = DS_HEALTH_RECOVERY_MIN;
        }

        for (uint32_t i = 0; i < ds_count; i++) {
            char host[MDS_DS_ADDR_MAX];
            struct ds_fail_state *fs;
            bool is_offline;

            if (ds_addr_parse_host(ds_list[i].addr, host,
                                   sizeof(host)) != 0) {
                continue;
            }

            /* Use internal marked_offline flag as source of truth.
             * The catalogue state may not reflect mark_ds_offline()
             * if the catalogue put failed silently.  The internal
             * flag is always authoritative. */
            pthread_mutex_lock(&hm->state_lock);
            fs = find_or_create_state(hm, ds_list[i].ds_id);
            is_offline = (fs != NULL && fs->marked_offline);

            /* Anti-flapping: skip DSes still in cooldown. */
            if (is_offline && fs != NULL &&
                fs->cooldown_until_ns > now_ns) {
                pthread_mutex_unlock(&hm->state_lock);
                continue;
            }
            pthread_mutex_unlock(&hm->state_lock);

            uint32_t probe_timeout = hm->interval_ms / 2;
            if (probe_timeout == 0) {
                probe_timeout = 1000;
            }

            int rc = ds_probe_null(host, ds_list[i].port, probe_timeout);

            if (rc == 0) {
                handle_ds_success(hm, ds_list[i].ds_id);

                /* Recovery: require consecutive successes before
                 * transitioning back to ONLINE. */
                if (is_offline) {
                    bool do_recover = false;

                    pthread_mutex_lock(&hm->state_lock);
                    fs = find_or_create_state(hm, ds_list[i].ds_id);
                    if (fs != NULL) {
                        fs->recovery_successes++;
                        if (fs->recovery_successes >= recovery_threshold) {
                            fs->recovery_successes = 0;
                            fs->last_online_ns = now_ns;
                            fs->marked_offline = false;
                            do_recover = true;
                        }
                    }
                    pthread_mutex_unlock(&hm->state_lock);

                    if (do_recover) {
                        (void)fprintf(stderr,
                            "INFO: DS %u (%s:%u) recovered "
                            "(%u consecutive probes OK) \xe2\x80\x94 "
                            "marking ONLINE\n",
                            (unsigned)ds_list[i].ds_id, host,
                            (unsigned)ds_list[i].port,
                            (unsigned)recovery_threshold);
                        if (hm->cat != NULL) {
                            struct mds_ds_info info = ds_list[i];
                            info.state = DS_ONLINE;
                            (void)mds_cat_ds_put(hm->cat, NULL, &info);
                        }
                    }
                }

                /* Reset flap counter if DS has been ONLINE long enough. */
                if (!is_offline) {
                    pthread_mutex_lock(&hm->state_lock);
                    fs = find_or_create_state(hm, ds_list[i].ds_id);
                    if (fs != NULL && fs->flap_count > 0 &&
                        fs->last_online_ns > 0 &&
                        (now_ns - fs->last_online_ns) >
                            DS_HEALTH_FLAP_RESET_NS) {
                        fs->flap_count = 0;
                    }
                    pthread_mutex_unlock(&hm->state_lock);
                }
            } else {
                if (!is_offline) {
                    handle_ds_failure(hm, ds_list[i].ds_id, 1);
                } else {
                    /* Probe during recovery window failed:
                     * reset recovery counter. */
                    handle_ds_failure(hm, ds_list[i].ds_id, 0);
                }
            }
        }
        free(ds_list);
    }
    return NULL;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int ds_health_init(const struct mds_catalogue *cat, struct commit_queue *cq,
                   uint32_t interval_ms, uint32_t fail_threshold,
                   ds_fail_cb cb, void *cb_ctx,
                   struct ds_health_monitor **out)
{
    struct ds_health_monitor *hm;

    if (cat == NULL || out == NULL || interval_ms == 0) {
        return -1;
    }

    hm = calloc(1, sizeof(*hm));
    if (hm == NULL) {
        return -1;
    }

    /* Store catalogue handle for vtable dispatch.
     * Cast away const: the vtable read functions (ds_list, ds_get)
     * take non-const catalogue because the dispatch layer is
     * uniform, but reads are semantically const. */
    hm->cat = (struct mds_catalogue *)cat;
    hm->cq = cq;
    hm->interval_ms = interval_ms;
    hm->fail_threshold = (fail_threshold > 0) ? fail_threshold
                                               : DS_HEALTH_DEFAULT_THRESHOLD;
    hm->fail_cb = cb;
    hm->fail_ctx = cb_ctx;
    atomic_store(&hm->running, 0);
    pthread_mutex_init(&hm->state_lock, NULL);

    if (pipe(hm->stop_pipe) != 0) {
        free(hm);
        return -1;
    }

    *out = hm;
    return 0;
}

int ds_health_start(struct ds_health_monitor *hm)
{
    if (hm == NULL) {
        return -1;
    }

    /* Diagnostic: log DS count visible to health monitor. */
    {
        struct mds_ds_info *ds_list = NULL;
        uint32_t ds_count = 0;
        if (mds_cat_ds_list(hm->cat, &ds_list, &ds_count) == MDS_OK) {
            uint32_t online = 0;
            for (uint32_t i = 0; i < ds_count; i++) {
                if (ds_list[i].state == DS_ONLINE) { online++; }
            }
            (void)fprintf(stderr,
                "INFO: ds_health started: %u DS registered, "
                "%u ONLINE, interval=%ums, threshold=%u\n",
                (unsigned)ds_count, (unsigned)online,
                (unsigned)hm->interval_ms,
                (unsigned)hm->fail_threshold);
            free(ds_list);
        } else {
            (void)fprintf(stderr,
                "WARN: ds_health started but ds_list failed\n");
        }
    }

    atomic_store(&hm->running, 1);
    if (pthread_create(&hm->thread, NULL, health_poll_thread, hm) != 0) {
        atomic_store(&hm->running, 0);
        return -1;
    }
    return 0;
}

void ds_health_stop(struct ds_health_monitor *hm)
{
    if (hm == NULL) {
        return;
}
    if (!atomic_load(&hm->running)) {
        return;
}
    atomic_store(&hm->running, 0);
    /* Wake the polling thread. */
    {
        char c = 1;
        ssize_t rc;

        rc = write(hm->stop_pipe[1], &c, 1);
        if (rc < 0) {
            /* Best-effort wake-up; shutdown continues via running flag. */
        }
    }
    pthread_join(hm->thread, NULL);
}

void ds_health_destroy(struct ds_health_monitor *hm)
{
    if (hm == NULL) {
        return;
}
    ds_health_stop(hm);
    close(hm->stop_pipe[0]);
    close(hm->stop_pipe[1]);
    pthread_mutex_destroy(&hm->state_lock);
    free(hm);
}

void ds_health_report_error(struct ds_health_monitor *hm, uint32_t ds_id)
{
    if (hm == NULL) {
        return;
}
    handle_ds_failure(hm, ds_id, 1);
}

void ds_health_set_shard(struct ds_health_monitor *hm,
                         const struct mds_shard *shard)
{
    if (hm == NULL) {
        return;
    }
    if (shard != NULL) {
        hm->cq = shard->cq;
    } else {
        hm->cq = NULL;
    }
    /* cat pointer is set at init time and not overridden by shard;
     * the shard's db/cq are only used by legacy CQ dispatch path. */
}

void ds_health_test_force_fail(struct ds_health_monitor *hm, uint32_t ds_id)
{
    struct ds_fail_state *fs;

    if (hm == NULL) {
        return;
    }

    /* Clear marked_offline so the force-fail path is not
     * suppressed by the "already offline" guard. */
    pthread_mutex_lock(&hm->state_lock);
    fs = find_or_create_state(hm, ds_id);
    if (fs != NULL) {
        fs->marked_offline = false;
    }
    pthread_mutex_unlock(&hm->state_lock);

    handle_ds_failure(hm, ds_id, hm->fail_threshold);
}

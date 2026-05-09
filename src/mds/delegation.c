/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * delegation.c -- NFSv4.1 file delegation manager.
 *
 * In-memory hash table of active delegations, keyed by fileid.
 * Each file can have multiple READ delegations (one per client)
 * or at most one WRITE delegation.  Striped mutexes (16 stripes)
 * for low contention under concurrent COMPOUND processing.
 *
 * CB_RECALL is sent best-effort via nfs4_cb_recall().  If the
 * backchannel is unavailable or times out, the delegation is
 * revoked immediately.
 *
 * See RFC 8881 S10.4.
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>

#include "delegation.h"
#include "nfs4_cb.h"
#include "session.h"
#include "rpc_server.h" /* rpc_conn_get_fd */
#include "mds_catalogue.h"
#include "mds_coordination.h"

/*
 * Maximum number of delegations on a single fileid we recall in one
 * deleg_recall_file() call.  Bounds stack usage of the snapshot array
 * (~56 B per slot * 64 = 3.5 KiB).  Production workloads see at most
 * one or two READ delegations per fileid; the cap is defensive.
 */
#define DELEG_RECALL_MAX_PER_FILE 64

/*
 * Per-recall snapshot copied out of the bucket while the stripe lock
 * is held.  After the lock is dropped we use only this snapshot to
 * issue CB_RECALL -- there is no chance of dereferencing a stale
 * session pointer because we never read e->session outside the lock.
 */
struct deleg_recall_target {
    struct nfs4_stateid stateid;
    uint64_t            clientid;
    uint64_t            fileid;
};

/*
 * Context passed to session_for_each_with_cb() to find the
 * backchannel snapshot for one specific clientid.  The first match
 * wins (most clients have a single session); we copy out the cb
 * metadata + dup the cb_conn fd while still under the session-table
 * lock, then release the lock and perform the I/O.
 */
struct deleg_cb_lookup_ctx {
    uint64_t want_clientid;
    bool     found;
    uint8_t  session_id[SESSION_ID_SIZE];
    uint32_t cb_prog;
    uint32_t slot_seq_id;
    uint32_t num_cb_slots;
    uint32_t minorversion;  /* RFC 8881 S20.1 -- echo session minor in CB */
    struct nfs4_cb_sec cb_sec; /* RFC 8881 S2.10.8.3 -- captured CB sec parms */
    int      fd;            /* dup'd; caller closes */
};

static int deleg_cb_lookup_cb(const struct session_cb_snap *snap, void *ctx)
{
    struct deleg_cb_lookup_ctx *c = ctx;
    int fd;
    int dup_fd;

    if (snap == NULL || c == NULL) {
        return 0;
    }
    if (c->found) {
        return 1; /* stop -- already snapshotted */
    }
    if (snap->clientid != c->want_clientid) {
        return 0;
    }
    fd = rpc_conn_get_fd(snap->cb_conn);
    if (fd < 0) {
        return 0; /* this session has no usable cb fd; keep looking */
    }
    dup_fd = dup(fd);
    if (dup_fd < 0) {
        return 0; /* resource pressure; keep looking */
    }
    c->found = true;
    memcpy(c->session_id, snap->session_id, SESSION_ID_SIZE);
    c->cb_prog      = snap->cb_prog;
    c->slot_seq_id  = snap->slot_seq_id;
    c->num_cb_slots = 1; /* matches layout_recall.c usage */
    c->minorversion = snap->minorversion;
    c->cb_sec       = snap->cb_sec;
    c->fd           = dup_fd;
    return 1;
}

/* -----------------------------------------------------------------------
 * Constants
 * ----------------------------------------------------------------------- */

#define DELEG_STRIPE_COUNT   16
#define DELEG_HASH_SIZE      4096
#define DELEG_RECALL_DEFAULT_MS 5000

/*
 * Window during which a recall for a given delegation is
 * considered "in flight" and duplicate CB_RECALL sends are
 * suppressed.  Conservatively long enough to cover the
 * DELEG_RECALL_DEFAULT_MS (5 s) in-flight deadline of the first
 * recall attempt.  After this window, if the client has not
 * DELEGRETURNed, the next caller will resend the CB_RECALL.
 */
#define DELEG_RECALL_PENDING_NS  ((uint64_t)DELEG_RECALL_DEFAULT_MS * 1000000ULL)

/* -----------------------------------------------------------------------
 * Internal structures
 * ----------------------------------------------------------------------- */

struct deleg_entry {
    struct nfs4_stateid   stateid;
    uint64_t              clientid;
    uint64_t              fileid;
    uint32_t              deleg_type;  /* OPEN_DELEGATE_READ/WRITE */
    struct nfs4_session  *session;     /* For CB_RECALL (may be NULL) */
    struct deleg_entry   *hash_next;   /* Per-bucket chain */
    /*
     * CB_RECALL dedupe:
     *   recall_pending    set to true when we send CB_RECALL
     *   recall_sent_ns    CLOCK_MONOTONIC timestamp of the send
     *
     * A second concurrent mutator that would also recall this
     * delegation checks these and skips the send if a prior
     * recall is still in flight within DELEG_RECALL_PENDING_NS.
     * Prevents CB_RECALL storms when many fore-ops hit the same
     * delegated file at once.  The foundational piece for safe
     * directory delegations, where a single dir mutation can
     * trigger recalls to many clients simultaneously.
     */
    bool                  recall_pending;
    uint64_t              recall_sent_ns;
};

/*
 * RFC 8881 §10.2.1: revoked delegation stateids.  When a delegation
 * is forcibly revoked (conflict-recall), the stateid is moved here
 * so subsequent ops return NFS4ERR_DELEG_REVOKED (not BAD_STATEID).
 * Entries are freed by FREE_STATEID or by client-destroy cleanup.
 * Protected by revoked_lock (separate from stripe locks).
 */
struct deleg_revoked_entry {
    uint8_t  other[NFS4_OTHER_SIZE];
    uint64_t clientid;
    struct deleg_revoked_entry *next;
};

struct deleg_table {
    struct deleg_entry  **buckets;     /* [DELEG_HASH_SIZE] */
    pthread_mutex_t       stripe_locks[DELEG_STRIPE_COUNT];
    uint32_t              mds_id;
    _Atomic uint64_t      sid_counter; /* Stateid sequence */
    struct mds_catalogue *cat;         /* RonDB (shared-attr). */
    struct session_table *st;
    uint64_t              boot_epoch;
    bool                  skip_transient_ndb;
    /* Revoked-stateid tracking (RFC 8881 §10.2.1). */
    struct deleg_revoked_entry *revoked_head;
    pthread_mutex_t             revoked_lock;
};

/* -----------------------------------------------------------------------
 * Hash helpers
 * ----------------------------------------------------------------------- */

static uint32_t deleg_hash(uint64_t fileid)
{
    uint64_t h = fileid;

    h ^= h >> 30;
    h *= 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 27;
    h *= 0x94d049bb133111ebULL;
    h ^= h >> 31;
    return (uint32_t)(h % DELEG_HASH_SIZE);
}

static uint32_t stripe_for(uint64_t fileid)
{
    return deleg_hash(fileid) % DELEG_STRIPE_COUNT;
}

static void lock_stripe(struct deleg_table *dt, uint64_t fileid)
{
    pthread_mutex_lock(&dt->stripe_locks[stripe_for(fileid)]);
}

static void unlock_stripe(struct deleg_table *dt, uint64_t fileid)
{
    pthread_mutex_unlock(&dt->stripe_locks[stripe_for(fileid)]);
}

/* -----------------------------------------------------------------------
 * Stateid generation (same pattern as open_state.c)
 * ----------------------------------------------------------------------- */

static void make_deleg_stateid(struct deleg_table *dt,
                               struct nfs4_stateid *out)
{
    uint32_t mds_be = htobe32(dt->mds_id);
    uint64_t seq_be = htobe64(
        atomic_fetch_add(&dt->sid_counter, 1));

    memset(out, 0, sizeof(*out));
    out->seqid = 1;
    memcpy(out->other, &mds_be, 4);
    memcpy(out->other + 4, &seq_be, 8);
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int deleg_table_init(uint32_t mds_id, struct deleg_table **out)
{
    struct deleg_table *dt;

    if (out == NULL) {
        return -1;
    }

    dt = calloc(1, sizeof(*dt));
    if (dt == NULL) {
        return -1;
    }

    /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
    dt->buckets = calloc(DELEG_HASH_SIZE,
                         sizeof(struct deleg_entry *));
    if (dt->buckets == NULL) {
        free(dt);
        return -1;
    }

    dt->mds_id = mds_id;
    atomic_store(&dt->sid_counter, 1);

    for (int i = 0; i < DELEG_STRIPE_COUNT; i++) {
        pthread_mutex_init(&dt->stripe_locks[i], NULL);
    }
    pthread_mutex_init(&dt->revoked_lock, NULL);

    *out = dt;
    return 0;
}

void deleg_table_destroy(struct deleg_table *dt)
{
    uint32_t i;

    if (dt == NULL) {
        return;
    }

    for (i = 0; i < DELEG_HASH_SIZE; i++) {
        struct deleg_entry *e = dt->buckets[i];
        while (e != NULL) {
            struct deleg_entry *next = e->hash_next;
            free(e);
            e = next;
        }
    }

    for (int s = 0; s < DELEG_STRIPE_COUNT; s++) {
        pthread_mutex_destroy(&dt->stripe_locks[s]);
    }

    /* Free revoked-stateid list. */
    {
        struct deleg_revoked_entry *re = dt->revoked_head;
        while (re != NULL) {
            struct deleg_revoked_entry *rn = re->next;
            free(re);
            re = rn;
        }
    }
    pthread_mutex_destroy(&dt->revoked_lock);

    /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
    free(dt->buckets);
    free(dt);
}

void deleg_table_set_cat(struct deleg_table *dt,
                         struct mds_catalogue *cat,
                         uint64_t boot_epoch)
{
    if (dt != NULL) {
        dt->cat = cat;
        dt->boot_epoch = boot_epoch;
    }
}

void deleg_table_set_session_table(struct deleg_table *dt,
                                   struct session_table *st)
{
    if (dt != NULL) {
        dt->st = st;
    }
}

void deleg_table_set_skip_transient(struct deleg_table *dt, bool skip)
{
    if (dt != NULL) {
        dt->skip_transient_ndb = skip;
    }
}

int deleg_grant(struct deleg_table *dt,
                uint64_t clientid, uint64_t fileid,
                uint32_t deleg_type,
                struct nfs4_session *session,
                struct nfs4_stateid *out_sid)
{
    struct deleg_entry *e;
    uint32_t bucket;

    if (dt == NULL || out_sid == NULL) {
        return -1;
    }
    if (deleg_type != OPEN_DELEGATE_READ &&
        deleg_type != OPEN_DELEGATE_WRITE) {
        return -1;
    }

    e = calloc(1, sizeof(*e));
    if (e == NULL) {
        return -1;
    }

    make_deleg_stateid(dt, &e->stateid);
    e->clientid   = clientid;
    e->fileid     = fileid;
    e->deleg_type = deleg_type;
    e->session    = session;

    lock_stripe(dt, fileid);

    bucket = deleg_hash(fileid);
    e->hash_next = dt->buckets[bucket];
    dt->buckets[bucket] = e;

    unlock_stripe(dt, fileid);

    *out_sid = e->stateid;

    /* Persist to RonDB (shared-attr write-through).
     *
     * skip_transient_ndb keeps the grant in-memory only.  This
     * removes a ~0.5-1 ms NDB write from the OPEN hot path when the
     * deployment accepts that delegations do not survive an MDS
     * restart.  A reconnecting client simply re-opens and, if
     * eligible, receives a fresh grant. */
    if (dt->cat != NULL && !dt->skip_transient_ndb) {
        struct mds_coord_deleg_row row;
        memset(&row, 0, sizeof(row));
        memcpy(row.stateid_other, e->stateid.other, 12);
        row.seqid = e->stateid.seqid;
        row.clientid = clientid;
        row.fileid = fileid;
        row.deleg_type = deleg_type;
        row.owner_mds_id = dt->mds_id;
        row.owner_boot_epoch = dt->boot_epoch;
        {
            struct timespec now;
            clock_gettime(CLOCK_REALTIME, &now);
            row.grant_time_ns = (uint64_t)now.tv_sec * 1000000000ULL
                              + (uint64_t)now.tv_nsec;
        }
        (void)mds_coord_deleg_put(dt->cat, &row);
    }

    return 0;
}

int deleg_return(struct deleg_table *dt,
                 const struct nfs4_stateid *stateid,
                 uint64_t clientid)
{
    uint32_t i;

    if (dt == NULL || stateid == NULL) {
        return -1;
    }

    /* Scan all buckets -- stateid doesn't encode fileid.
     * Acceptable cost: DELEGRETURN is infrequent. */
    for (i = 0; i < DELEG_HASH_SIZE; i++) {
        uint32_t stripe = i % DELEG_STRIPE_COUNT;

        pthread_mutex_lock(&dt->stripe_locks[stripe]);

        struct deleg_entry **pp = &dt->buckets[i];
        while (*pp != NULL) {
            struct deleg_entry *e = *pp;
            if (memcmp(e->stateid.other, stateid->other,
                       NFS4_OTHER_SIZE) == 0 &&
                e->clientid == clientid) {
                *pp = e->hash_next;
                /* Delete from RonDB (shared-attr). */
                if (dt->cat != NULL) {
                    (void)mds_coord_deleg_del(dt->cat,
                        e->stateid.other);
                }
                free(e);
                pthread_mutex_unlock(&dt->stripe_locks[stripe]);
                return 0;
            }
            pp = &e->hash_next;
        }

        pthread_mutex_unlock(&dt->stripe_locks[stripe]);
    }

    return -1; /* Not found */
}

int deleg_check_conflict(struct deleg_table *dt,
                         uint64_t fileid, uint64_t clientid,
                         bool *has_conflict)
{
    uint32_t bucket;
    struct deleg_entry *e;

    if (dt == NULL || has_conflict == NULL) {
        return -1;
    }

    *has_conflict = false;

    lock_stripe(dt, fileid);

    bucket = deleg_hash(fileid);
    for (e = dt->buckets[bucket]; e != NULL; e = e->hash_next) {
        if (e->fileid != fileid) {
            continue;
        }
        /* Same client: no conflict per RFC 8881 S10.4.1. */
        if (e->clientid == clientid) {
            continue;
        }
        /* Another client holds a delegation -> conflict. */
        *has_conflict = true;
        break;
    }

    unlock_stripe(dt, fileid);
    return 0;
}

int deleg_recall_file(struct deleg_table *dt,
                      uint64_t fileid, uint64_t clientid,
                      uint32_t timeout_ms)
{
    struct deleg_recall_target targets[DELEG_RECALL_MAX_PER_FILE];
    uint32_t target_count = 0;
    uint32_t bucket;
    struct deleg_entry *e;
    struct deleg_entry **pp;
    int recalled;

    if (dt == NULL) {
        return -1;
    }
    if (timeout_ms == 0) {
        timeout_ms = DELEG_RECALL_DEFAULT_MS;
    }

    /*
     * Phase 1 -- under the stripe lock: snapshot every conflicting
     * grant out of the bucket and unlink it.  We MUST NOT call into
     * the session table or send any CB while holding the stripe lock
     * (the session table has its own lock; nesting them creates a
     * lock-order trap with concurrent EXCHANGE_ID / DESTROY_SESSION
     * paths that already grab the session lock first).  By copying
     * the in-memory record into a stack array we can drop the stripe
     * lock immediately and do all CB I/O against detached snapshots
     * with no chance of dereferencing a stale session pointer.
     */
    lock_stripe(dt, fileid);

    bucket = deleg_hash(fileid);
    pp = &dt->buckets[bucket];

    while (*pp != NULL) {
        e = *pp;
        if (e->fileid != fileid || e->clientid == clientid) {
            pp = &e->hash_next;
            continue;
        }

        if (target_count < DELEG_RECALL_MAX_PER_FILE) {
            targets[target_count].stateid  = e->stateid;
            targets[target_count].clientid = e->clientid;
            targets[target_count].fileid   = e->fileid;
            target_count++;
        } else {
            /*
             * Cap exceeded: leave the surplus entry in place and
             * stop scanning.  The next deleg_recall_file caller (or
             * the periodic lease reaper) will pick it up.  This
             * cannot happen with the current per-file delegation
             * model (max one WRITE or N READ where N <= clients),
             * but the bound is here to keep stack usage finite if
             * the model ever loosens.
             */
            (void)fprintf(stderr,
                "deleg: recall cap %u reached on fileid=%llu; "
                "deferring surplus entries\n",
                (unsigned)DELEG_RECALL_MAX_PER_FILE,
                (unsigned long long)fileid);
            break;
        }

        /* Drop authoritative RonDB row before unlinking so a
         * cross-MDS observer sees consistent state. */
        if (dt->cat != NULL && !dt->skip_transient_ndb) {
            (void)mds_coord_deleg_del(dt->cat, e->stateid.other);
        }
        *pp = e->hash_next;

        /*
         * RFC 8881 §10.2.1: move the stateid to the revoked set
         * so the client sees DELEG_REVOKED (not BAD_STATEID)
         * on any subsequent use.  Pynfs DELEG8.
         */
        {
            struct deleg_revoked_entry *re =
                calloc(1, sizeof(*re));
            if (re != NULL) {
                memcpy(re->other, e->stateid.other,
                       NFS4_OTHER_SIZE);
                re->clientid = e->clientid;
                pthread_mutex_lock(&dt->revoked_lock);
                re->next = dt->revoked_head;
                dt->revoked_head = re;
                pthread_mutex_unlock(&dt->revoked_lock);
            }
        }
        free(e);
    }

    unlock_stripe(dt, fileid);

    /*
     * Phase 2 -- outside the stripe lock: for each detached snapshot,
     * find the holder's backchannel via the session table, dup() the
     * cb_conn fd under the session-table lock, then send CB_RECALL on
     * the dup'd fd.  Per RFC 8881 S10.4, the recall is best-effort:
     * the authoritative contract with the caller is "this delegation
     * is gone", which is already true after Phase 1.  Any send error
     * (ENOTCONN / ETIMEDOUT / EIO / NFS4 status) is logged and
     * swallowed.  No retry: the caller proceeds with the conflicting
     * mutation.
     */
    recalled = (int)target_count;
    if (dt->st == NULL) {
        /*
         * No session table wired -- caller intentionally configured
         * "revoke without CB" mode (the legacy path used by tests
         * and by deployments that have no backchannel).  All grants
         * are already gone from memory + RonDB; nothing more to do.
         */
        return recalled;
    }

    for (uint32_t i = 0; i < target_count; i++) {
        struct deleg_cb_lookup_ctx lc;
        struct nfs4_cb_recall_args ra;
        int cbrc;

        memset(&lc, 0, sizeof(lc));
        lc.want_clientid = targets[i].clientid;
        lc.fd = -1;

        (void)session_for_each_with_cb(dt->st, deleg_cb_lookup_cb, &lc);
        if (!lc.found) {
            /* Holder has no bound backchannel -- silent revoke is
             * the only correct outcome.  The client will discover
             * its delegation is gone on its next OPEN/READ/WRITE
             * via NFS4ERR_BAD_STATEID. */
            continue;
        }

        memset(&ra, 0, sizeof(ra));
        ra.stateid  = targets[i].stateid;
        ra.truncate = false;
        ra.fileid   = targets[i].fileid;

        cbrc = nfs4_cb_recall_fd(lc.fd, lc.session_id, lc.cb_prog,
                                 lc.slot_seq_id, lc.num_cb_slots,
                                 lc.minorversion, &lc.cb_sec,
                                 &ra, timeout_ms);
        if (cbrc != 0) {
            (void)fprintf(stderr,
                "deleg: CB_RECALL fileid=%llu client=%llu "
                "rc=%d \u2014 already revoked\n",
                (unsigned long long)targets[i].fileid,
                (unsigned long long)targets[i].clientid, cbrc);
        }
        (void)close(lc.fd);
    }

    return recalled;
}

void deleg_revoke_client(struct deleg_table *dt, uint64_t clientid)
{
    uint32_t i;

    if (dt == NULL) {
        return;
    }

    for (i = 0; i < DELEG_HASH_SIZE; i++) {
        uint32_t stripe = i % DELEG_STRIPE_COUNT;

        pthread_mutex_lock(&dt->stripe_locks[stripe]);

        struct deleg_entry **pp = &dt->buckets[i];
        while (*pp != NULL) {
            struct deleg_entry *e = *pp;
            if (e->clientid == clientid) {
                *pp = e->hash_next;
                free(e);
            } else {
                pp = &e->hash_next;
            }
        }

        pthread_mutex_unlock(&dt->stripe_locks[stripe]);
    }
}

void deleg_revoke_file(struct deleg_table *dt, uint64_t fileid)
{
    uint32_t bucket;
    struct deleg_entry **pp;

    if (dt == NULL) {
        return;
    }

    /*
     * Only the bucket owning this fileid can hold matching
     * entries (deleg_hash maps fileid -> bucket deterministically),
     * so we lock just that one stripe.  Walk the chain and unlink
     * every entry whose fileid matches; entries for other files
     * sharing the same bucket are skipped without touching the
     * other stripes.
     */
    lock_stripe(dt, fileid);

    bucket = deleg_hash(fileid);
    pp = &dt->buckets[bucket];

    while (*pp != NULL) {
        struct deleg_entry *e = *pp;

        if (e->fileid == fileid) {
            *pp = e->hash_next;
            /* Drop authoritative row when persistence is on.
             * Mirrors deleg_recall_file's gating; deleg_revoke_client
             * predates that gate but the leak we are plugging is
             * specifically the in-memory grant retained when the
             * underlying file is removed. */
            if (dt->cat != NULL && !dt->skip_transient_ndb) {
                (void)mds_coord_deleg_del(dt->cat,
                                          e->stateid.other);
            }
            free(e);
        } else {
            pp = &e->hash_next;
        }
    }

    unlock_stripe(dt, fileid);
}

bool deleg_is_revoked(const struct deleg_table *dt,
                      const uint8_t other[12])
{
    if (dt == NULL || other == NULL) { return false; }
    struct deleg_table *mdt = (struct deleg_table *)dt;
    pthread_mutex_lock(&mdt->revoked_lock);
    const struct deleg_revoked_entry *re = mdt->revoked_head;
    while (re != NULL) {
        if (memcmp(re->other, other, NFS4_OTHER_SIZE) == 0) {
            pthread_mutex_unlock(&mdt->revoked_lock);
            return true;
        }
        re = re->next;
    }
    pthread_mutex_unlock(&mdt->revoked_lock);
    return false;
}

bool deleg_client_has_revoked(const struct deleg_table *dt,
                              uint64_t clientid)
{
    if (dt == NULL) { return false; }
    struct deleg_table *mdt = (struct deleg_table *)dt;
    pthread_mutex_lock(&mdt->revoked_lock);
    const struct deleg_revoked_entry *re = mdt->revoked_head;
    while (re != NULL) {
        if (re->clientid == clientid) {
            pthread_mutex_unlock(&mdt->revoked_lock);
            return true;
        }
        re = re->next;
    }
    pthread_mutex_unlock(&mdt->revoked_lock);
    return false;
}

int deleg_free_revoked(struct deleg_table *dt,
                       const uint8_t other[12])
{
    if (dt == NULL || other == NULL) { return -1; }
    pthread_mutex_lock(&dt->revoked_lock);
    struct deleg_revoked_entry **pp = &dt->revoked_head;
    while (*pp != NULL) {
        struct deleg_revoked_entry *re = *pp;
        if (memcmp(re->other, other, NFS4_OTHER_SIZE) == 0) {
            *pp = re->next;
            free(re);
            pthread_mutex_unlock(&dt->revoked_lock);
            return 0;
        }
        pp = &re->next;
    }
    pthread_mutex_unlock(&dt->revoked_lock);
    return -1;
}

bool deleg_stateid_exists(const struct deleg_table *dt,
                          const uint8_t other[12])
{
    uint32_t i;

    if (dt == NULL || other == NULL) {
        return false;
    }

    for (i = 0; i < DELEG_HASH_SIZE; i++) {
        uint32_t stripe = i % DELEG_STRIPE_COUNT;

        pthread_mutex_lock(
            &((struct deleg_table *)dt)->stripe_locks[stripe]);

        const struct deleg_entry *e = dt->buckets[i];
        while (e != NULL) {
            if (memcmp(e->stateid.other, other,
                       NFS4_OTHER_SIZE) == 0) {
                pthread_mutex_unlock(
                    &((struct deleg_table *)dt)->stripe_locks[stripe]);
                return true;
            }
            e = e->hash_next;
        }

        pthread_mutex_unlock(
            &((struct deleg_table *)dt)->stripe_locks[stripe]);
    }

    return false;
}

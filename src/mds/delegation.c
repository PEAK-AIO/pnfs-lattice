/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * delegation.c — NFSv4.1 file delegation manager.
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
 * See RFC 8881 §10.4.
 */

#include <stdlib.h>
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
#include "mds_catalogue.h"
#include "mds_coordination.h"

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

struct deleg_table {
    struct deleg_entry  **buckets;     /* [DELEG_HASH_SIZE] */
    pthread_mutex_t       stripe_locks[DELEG_STRIPE_COUNT];
    uint32_t              mds_id;
    _Atomic uint64_t      sid_counter; /* Stateid sequence */
    struct mds_catalogue *cat;         /* RonDB (shared-attr). */
    uint64_t              boot_epoch;
    /* When true, deleg_grant keeps grants in memory only.
     * Mirrors `skip_transient_ndb` used by layout / open-state paths. */
    bool                  skip_transient_ndb;
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

    /* Scan all buckets — stateid doesn't encode fileid.
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
        /* Same client: no conflict per RFC 8881 §10.4.1. */
        if (e->clientid == clientid) {
            continue;
        }
        /* Another client holds a delegation → conflict. */
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
    uint32_t bucket;
    struct deleg_entry *e;
    struct deleg_entry **pp;
    int recalled = 0;

    if (dt == NULL) {
        return -1;
    }
    if (timeout_ms == 0) {
        timeout_ms = DELEG_RECALL_DEFAULT_MS;
    }

    lock_stripe(dt, fileid);

    bucket = deleg_hash(fileid);
    pp = &dt->buckets[bucket];

    while (*pp != NULL) {
        e = *pp;
        if (e->fileid != fileid || e->clientid == clientid) {
            pp = &e->hash_next;
            continue;
        }

        /* Best-effort CB_RECALL on the backchannel.  Protocol-level:
         * client should DELEGRETURN within lease_time_sec; we send
         * the recall now, record the wire result, and still revoke
         * from our in-memory table.  If the client does DELEGRETURN,
         * op_delegreturn() separately lands and is a no-op on an
         * already-revoked entry (BAD_STATEID is expected and fine).
         *
         * Intentional: we don't block on the client's response — the
         * cb_recall helper already handles its own timeout, and any
         * error (ENOTCONN / ETIMEDOUT / EIO / NFS4 status) means the
         * client is unreachable and immediate revocation is the
         * correct answer.
         *
         * Dedupe: if a recall is already in flight within the
         * DELEG_RECALL_PENDING_NS window (set by a previous
         * concurrent caller), skip sending another CB_RECALL to
         * the same client for the same stateid.  Revoke locally
         * either way — the authoritative contract with the caller
         * is "this delegation is gone after this returns". */
        if (e->session != NULL) {
            struct timespec nowts;
            uint64_t now_ns;
            bool suppress_send = false;

            clock_gettime(CLOCK_MONOTONIC, &nowts);
            now_ns = (uint64_t)nowts.tv_sec * 1000000000ULL +
                     (uint64_t)nowts.tv_nsec;

            if (e->recall_pending &&
                (now_ns - e->recall_sent_ns) < DELEG_RECALL_PENDING_NS) {
                suppress_send = true;
            }

            if (!suppress_send) {
                struct nfs4_cb_recall_args ra;
                int cbrc;

                memset(&ra, 0, sizeof(ra));
                ra.stateid = e->stateid;
                ra.truncate = false;
                ra.fileid   = e->fileid;
                e->recall_pending = true;
                e->recall_sent_ns = now_ns;
                cbrc = nfs4_cb_recall(e->session, &ra, timeout_ms);
                if (cbrc != 0) {
                    (void)fprintf(stderr,
                        "deleg: CB_RECALL fileid=%llu client=%llu "
                        "rc=%d \u2014 revoking\n",
                        (unsigned long long)e->fileid,
                        (unsigned long long)e->clientid, cbrc);
                }
            }
        }

        /* Revoke: remove from in-memory table.  If catalogue-backed
         * delegation persistence is active (cat != NULL &&
         * !skip_transient_ndb) we also drop the authoritative row
         * so other MDSes stop observing the grant. */
        if (dt->cat != NULL && !dt->skip_transient_ndb) {
            (void)mds_coord_deleg_del(dt->cat, e->stateid.other);
        }
        *pp = e->hash_next;
        free(e);
        recalled++;
    }

    unlock_stripe(dt, fileid);
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

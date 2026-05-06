/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * open_state.c -- NFSv4.1 open state and stateid management.
 *
 * Data structures:
 *   - Stateid hash table: chained, indexed by the 12-byte "other" field.
 *   - Per-file chain: each open_state for the same fileid is linked via
 *     file_next.  Used for share reservation conflict detection.
 *
 * Stateid "other" layout (12 bytes):
 *   [mds_id BE 4B][counter BE 8B]
 *
 * Thread safety:
 *   - Per-file chains are protected by striped fileid mutexes.
 *   - Stateid hash lookups are protected by striped RW locks.
 *
 * Open state may optionally be persisted to the catalogue for recovery.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <endian.h>
#include <time.h>

#include "pnfs_mds.h"
#include "open_state.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"

/* -----------------------------------------------------------------------
 * Hash table sizing
 * ----------------------------------------------------------------------- */

#define STATEID_HASH_BUCKETS  256
#define FILE_HASH_BUCKETS     256
#define OPEN_STATE_LOCK_STRIPES 16

/* -----------------------------------------------------------------------
 * Per-file head node -- tracks all opens on a given fileid
 * ----------------------------------------------------------------------- */

struct file_opens {
    uint64_t               fileid;
    struct nfs4_open_state *head;       /* Linked via open_state->file_next */
    struct file_opens      *hash_next;  /* File hash chain */
};

/* -----------------------------------------------------------------------
 * Open state table (opaque type from open_state.h)
 * ----------------------------------------------------------------------- */

struct open_state_table {
    struct nfs4_open_state **stateid_hash;  /* [STATEID_HASH_BUCKETS] */
    struct file_opens      **file_hash;     /* [FILE_HASH_BUCKETS] */
    atomic_uint_fast64_t    next_other_seq;
    uint32_t                mds_id;
    struct mds_catalogue   *cat;  /**< RonDB catalogue (shared-attr). */
    uint64_t                boot_epoch; /**< For fencing (shared-attr). */
    bool                    skip_ndb_persist; /**< Skip NDB writes for perf. */
    pthread_mutex_t         locks[OPEN_STATE_LOCK_STRIPES];
    pthread_rwlock_t        stateid_locks[OPEN_STATE_LOCK_STRIPES];
};


static uint32_t hash_fileid(uint64_t fileid);

/* Stripe lock index from fileid.
 * MUST use the same hash as hash_fileid() to ensure all operations
 * on the same hash bucket are serialized by the same lock stripe.
 * Using raw fileid % 16 allowed two fileids in the same bucket to
 * hold different locks, corrupting the hash chain under concurrency. */
static inline uint32_t lock_stripe(uint64_t fileid)
{
    return hash_fileid(fileid) % OPEN_STATE_LOCK_STRIPES;
}

/* -----------------------------------------------------------------------
 * Hash functions
 * ----------------------------------------------------------------------- */

static uint32_t hash_other(const uint8_t other[NFS4_OTHER_SIZE])
{
    uint64_t v;

    memcpy(&v, other + 4, sizeof(v));  /* counter portion */
    v ^= v >> 33;
    v *= 0xff51afd7ed558ccdULL;
    v ^= v >> 33;
    return (uint32_t)(v % STATEID_HASH_BUCKETS);
}

static uint32_t hash_fileid(uint64_t fileid)
{
    uint64_t h = fileid;

    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    return (uint32_t)(h % FILE_HASH_BUCKETS);
}

static inline uint32_t stateid_lock_stripe(
    const uint8_t other[NFS4_OTHER_SIZE])
{
    return hash_other(other) % OPEN_STATE_LOCK_STRIPES;
}

/* -----------------------------------------------------------------------
 * Internal: generate a unique stateid "other"
 *
 * Layout: [mds_id BE 4B][counter BE 8B]
 * ----------------------------------------------------------------------- */

static void make_stateid_other(struct open_state_table *ot,
                               uint8_t out[NFS4_OTHER_SIZE])
{
    uint32_t mds_be = htobe32(ot->mds_id);
    uint64_t seq =
        atomic_fetch_add_explicit(&ot->next_other_seq, 1,
                                  memory_order_relaxed);
    uint64_t seq_be = htobe64(seq);

    memcpy(out, &mds_be, 4);
    memcpy(out + 4, &seq_be, 8);
}

/* -----------------------------------------------------------------------
 * Internal: find open_state by stateid "other"
 * ----------------------------------------------------------------------- */

static struct nfs4_open_state *find_by_other(const struct open_state_table *ot,
                                             const uint8_t other[NFS4_OTHER_SIZE])
{
    uint32_t idx = hash_other(other);
    struct nfs4_open_state *os;

    for (os = ot->stateid_hash[idx]; os != NULL; os = os->hash_next) {
        if (memcmp(os->stateid.other, other, NFS4_OTHER_SIZE) == 0) {
            return os;
}
    }
    return NULL;
}

/* -----------------------------------------------------------------------
 * Internal: find or create the file_opens head for a fileid
 * ----------------------------------------------------------------------- */

static struct file_opens *find_file_opens(const struct open_state_table *ot,
                                          uint64_t fileid)
{
    uint32_t idx = hash_fileid(fileid);
    struct file_opens *fo;

    for (fo = ot->file_hash[idx]; fo != NULL; fo = fo->hash_next) {
        if (fo->fileid == fileid) {
            return fo;
}
    }
    return NULL;
}

static struct file_opens *get_or_create_file_opens(
    struct open_state_table *ot, uint64_t fileid)
{
    struct file_opens *fo;
    uint32_t idx;

    fo = find_file_opens(ot, fileid);
    if (fo != NULL) {
        return fo;
}

    fo = calloc(1, sizeof(*fo));
    if (fo == NULL) {
        return NULL;
}

    fo->fileid = fileid;
    fo->head = NULL;

    idx = hash_fileid(fileid);
    fo->hash_next = ot->file_hash[idx];
    ot->file_hash[idx] = fo;
    return fo;
}

/* -----------------------------------------------------------------------
 * Internal: remove a file_opens head if empty
 * ----------------------------------------------------------------------- */

static void maybe_free_file_opens(struct open_state_table *ot,
                                  uint64_t fileid)
{
    uint32_t idx = hash_fileid(fileid);
    struct file_opens **pp;

    for (pp = &ot->file_hash[idx]; *pp != NULL; pp = &(*pp)->hash_next) {
        if ((*pp)->fileid == fileid && (*pp)->head == NULL) {
            struct file_opens *fo = *pp;

            *pp = fo->hash_next;
            free(fo);
            return;
        }
    }
}

/* -----------------------------------------------------------------------
 * Internal: share reservation conflict check
 *
 * RFC 8881 S9.1.1: A new OPEN conflicts if:
 *   (new share_access) & (existing share_deny) != 0, OR
 *   (existing share_access) & (new share_deny) != 0.
 * ----------------------------------------------------------------------- */

static bool share_conflict(const struct file_opens *fo,
                           uint32_t new_access,
                           uint32_t new_deny)
{
    const struct nfs4_open_state *os;

    if (fo == NULL) {
        return false;
}

    for (os = fo->head; os != NULL; os = os->file_next) {
        if ((new_access & os->share_deny) != 0) {
            return true;
}
        if ((os->share_access & new_deny) != 0) {
            return true;
}
    }
    return false;
}

/* Same as share_conflict() but skips a single entry on the chain.
 *
 * Used by the same-owner re-OPEN path: the existing stateid IS already
 * advertising its current (access,deny) on the chain, so checking the
 * upgraded merged reservation against it would be a self-conflict
 * (e.g. existing access=WRITE/deny=READ + new access=READ/deny=NONE
 * merges to access=READ|WRITE/deny=READ, and the chain's existing
 * deny=READ would alias the merged access=READ).  RFC 5661 S9.1.1
 * defines share-conflict over distinct opens; same-owner upgrades are
 * scoped per RFC 8881 S8.2.2 / S9.1.4. */
static bool share_conflict_excluding(const struct file_opens *fo,
                                     const struct nfs4_open_state *skip,
                                     uint32_t new_access,
                                     uint32_t new_deny)
{
    const struct nfs4_open_state *os;

    if (fo == NULL) {
        return false;
    }

    for (os = fo->head; os != NULL; os = os->file_next) {
        if (os == skip) {
            continue;
        }
        if ((new_access & os->share_deny) != 0) {
            return true;
        }
        if ((os->share_access & new_deny) != 0) {
            return true;
        }
    }
    return false;
}

/* -----------------------------------------------------------------------
 * Internal: unlink open_state from stateid hash
 * ----------------------------------------------------------------------- */

static void unhash_stateid(struct open_state_table *ot,
                           struct nfs4_open_state *os)
{
    uint32_t idx = hash_other(os->stateid.other);
    struct nfs4_open_state **pp;

    for (pp = &ot->stateid_hash[idx]; *pp != NULL;
         pp = &(*pp)->hash_next) {
        if (*pp == os) {
            *pp = os->hash_next;
            return;
        }
    }
}

/* -----------------------------------------------------------------------
 * Internal: unlink open_state from file chain
 * ----------------------------------------------------------------------- */

static void unlink_from_file(struct open_state_table *ot,
                             struct nfs4_open_state *os)
{
    struct file_opens *fo = find_file_opens(ot, os->fileid);
    struct nfs4_open_state **pp;

    if (fo == NULL) {
        return;
}

    for (pp = &fo->head; *pp != NULL; pp = &(*pp)->file_next) {
        if (*pp == os) {
            *pp = os->file_next;
            break;
        }
    }

    maybe_free_file_opens(ot, os->fileid);
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int open_state_table_init(uint32_t mds_id, struct open_state_table **out)
{
    struct open_state_table *ot;

    if (out == NULL) {
        return -1;
}

    ot = calloc(1, sizeof(*ot));
    if (ot == NULL) {
        return -1;
}

    /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
    ot->stateid_hash = calloc(STATEID_HASH_BUCKETS,
                              sizeof(struct nfs4_open_state *));
    if (ot->stateid_hash == NULL) {
        free(ot);
        return -1;
    }

    /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
    ot->file_hash = calloc(FILE_HASH_BUCKETS,
                           sizeof(struct file_opens *));
    if (ot->file_hash == NULL) {
        /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
        free(ot->stateid_hash);
        free(ot);
        return -1;
    }

    ot->mds_id = mds_id;
    /* Seed the stateid counter from wall-clock nanoseconds so it is
     * always strictly larger than any counter value persisted by a
     * previous daemon boot.  Without this the counter restarts at 1
     * each time the process starts; any row left in the open_state
     * table from the previous run (e.g. when shared-attr write-
     * through is on or under test_mds_admin fixtures) collides with
     * the newly-issued stateid and downstream checks (LAYOUTCOMMIT's
     * clientid/fileid verification, OP_COPY's src/dst stateid
     * lookup, and so on) hit BAD_STATEID for a file that appears to
     * have been opened normally.  Same root cause as the layout-
     * stateid counter fix in compound_layout.c:make_layout_stateid. */
    {
        struct timespec seed_ts;
        uint64_t seed;
        if (clock_gettime(CLOCK_REALTIME, &seed_ts) != 0) {
            (void)clock_gettime(CLOCK_MONOTONIC, &seed_ts);
        }
        seed = (uint64_t)seed_ts.tv_sec * 1000000000ULL +
               (uint64_t)seed_ts.tv_nsec;
        if (seed == 0) { seed = 1; }
        atomic_init(&ot->next_other_seq, seed);
    }
    for (uint32_t li = 0; li < OPEN_STATE_LOCK_STRIPES; li++) {
        pthread_mutex_init(&ot->locks[li], NULL);
        pthread_rwlock_init(&ot->stateid_locks[li], NULL);
    }

    *out = ot;
    return 0;
}

void open_state_table_destroy(struct open_state_table *ot)
{
    uint32_t i;

    if (ot == NULL) {
        return;
}

    /* Free all open states via the stateid hash. */
    for (i = 0; i < STATEID_HASH_BUCKETS; i++) {
        struct nfs4_open_state *os = ot->stateid_hash[i];
        struct nfs4_open_state *next;

        while (os != NULL) {
            next = os->hash_next;
            free(os);
            os = next;
        }
    }

    /* Free all file_opens heads. */
    for (i = 0; i < FILE_HASH_BUCKETS; i++) {
        struct file_opens *fo = ot->file_hash[i];
        struct file_opens *next;

        while (fo != NULL) {
            next = fo->hash_next;
            free(fo);
            fo = next;
        }
    }

    for (uint32_t li = 0; li < OPEN_STATE_LOCK_STRIPES; li++) {
        pthread_mutex_destroy(&ot->locks[li]);
        pthread_rwlock_destroy(&ot->stateid_locks[li]);
    }
    /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
    free(ot->file_hash);
    /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
    free(ot->stateid_hash);
    free(ot);
}

void open_state_table_set_cat(struct open_state_table *ot,
                              struct mds_catalogue *cat,
                              uint64_t boot_epoch)
{
    if (ot != NULL) {
        ot->cat = cat;
        ot->boot_epoch = boot_epoch;
    }
}

void open_state_table_set_skip_ndb(struct open_state_table *ot, bool skip)
{
    if (ot != NULL) {
        ot->skip_ndb_persist = skip;
    }
}

/* -----------------------------------------------------------------------
 * Share conflict check against RonDB open-state rows.
 * ----------------------------------------------------------------------- */

struct rondb_share_check_ctx {
    uint32_t share_access;
    uint32_t share_deny;
    bool conflict;
};

static int rondb_share_check_cb(const struct mds_coord_open_row *row,
                                 void *arg)
{
    struct rondb_share_check_ctx *ctx = arg;

    /* RFC 8881 S9.1: deny modes vs access modes. */
    if ((row->share_deny & OPEN4_SHARE_DENY_READ) &&
        (ctx->share_access & OPEN4_SHARE_ACCESS_READ)) {
        ctx->conflict = true;
        return 1; /* stop scan */
    }
    if ((row->share_deny & OPEN4_SHARE_DENY_WRITE) &&
        (ctx->share_access & OPEN4_SHARE_ACCESS_WRITE)) {
        ctx->conflict = true;
        return 1;
    }
    if ((ctx->share_deny & OPEN4_SHARE_DENY_READ) &&
        (row->share_access & OPEN4_SHARE_ACCESS_READ)) {
        ctx->conflict = true;
        return 1;
    }
    if ((ctx->share_deny & OPEN4_SHARE_DENY_WRITE) &&
        (row->share_access & OPEN4_SHARE_ACCESS_WRITE)) {
        ctx->conflict = true;
        return 1;
    }
    return 0; /* continue scan */
}

/* ----------------------------------------------------------------------- */

int open_state_open(struct open_state_table *ot,
                    uint64_t clientid,
                    const uint8_t *open_owner,
                    uint32_t open_owner_len,
                    uint64_t fileid,
                    uint32_t share_access,
                    uint32_t share_deny,
                    struct nfs4_stateid *out_stateid)
{
    struct file_opens *fo;
    struct nfs4_open_state *os = NULL;
    struct nfs4_open_state *existing = NULL;
    uint32_t file_lock_idx;
    uint32_t stateid_lock_idx;
    uint32_t idx;
    int rc = 0;

    if (ot == NULL || out_stateid == NULL) {
        return -3;
}
    if ((share_access & OPEN4_SHARE_ACCESS_BOTH) == 0) {
        return -3;
}
    if (open_owner_len > NFS4_OPEN_OWNER_MAX) {
        return -3;
}

    file_lock_idx = lock_stripe(fileid);
    pthread_mutex_lock(&ot->locks[file_lock_idx]);

    /* RFC 8881 S8.2.2 + S9.1.4 + S18.16.4: a subsequent OPEN by the
     * same {clientid, open_owner} for the same file MUST return the
     * existing open stateid with seqid bumped and share_access /
     * share_deny upgraded to the union of all OPENs by that owner.
     * Allocating a fresh stateid each time (the previous behaviour)
     * leaks server state and breaks pynfs OPEN2 (testOpenAgain),
     * which expects seqid to advance from N to N+1. */
    fo = find_file_opens(ot, fileid);
    if (fo != NULL) {
        for (existing = fo->head; existing != NULL;
             existing = existing->file_next) {
            if (existing->clientid != clientid) {
                continue;
            }
            if (existing->open_owner_len != open_owner_len) {
                continue;
            }
            if (open_owner_len == 0 ||
                (open_owner != NULL &&
                 memcmp(existing->open_owner, open_owner,
                        open_owner_len) == 0)) {
                break; /* match */
            }
        }
    }

    if (existing != NULL) {
        uint32_t merged_access =
            existing->share_access | share_access;
        uint32_t merged_deny =
            existing->share_deny | share_deny;

        /* Re-validate share reservations against every OTHER open on
         * the file using the upgraded (merged) modes.  Skipping
         * "existing" itself avoids a self-conflict where its own
         * deny bits would alias the merged access bits. */
        if (share_conflict_excluding(fo, existing,
                                     merged_access, merged_deny)) {
            rc = -1; /* NFS4ERR_SHARE_DENIED */
            goto out_unlock;
        }

        stateid_lock_idx =
            stateid_lock_stripe(existing->stateid.other);
        pthread_rwlock_wrlock(
            &ot->stateid_locks[stateid_lock_idx]);

        /* RFC 8881 S8.2.2: bump seqid by one; the value 0 is
         * reserved, so 0xFFFFFFFF wraps to 1 (not 0). */
        uint32_t next_seqid = existing->stateid.seqid + 1U;
        if (next_seqid == 0U) {
            next_seqid = 1U;
        }
        existing->stateid.seqid = next_seqid;
        existing->share_access = merged_access;
        existing->share_deny = merged_deny;

        *out_stateid = existing->stateid;

        pthread_rwlock_unlock(
            &ot->stateid_locks[stateid_lock_idx]);

        /* Persist updated row (same primary key as the original
         * insert, so this is an in-place update). */
        if (ot->cat != NULL && !ot->skip_ndb_persist) {
            struct mds_coord_open_row row;
            memset(&row, 0, sizeof(row));
            memcpy(row.stateid_other, existing->stateid.other,
                   NFS4_OTHER_SIZE);
            row.seqid = existing->stateid.seqid;
            row.clientid = clientid;
            row.fileid = fileid;
            row.share_access = merged_access;
            row.share_deny = merged_deny;
            if (open_owner != NULL && open_owner_len > 0) {
                memcpy(row.open_owner, open_owner,
                       open_owner_len);
            }
            row.open_owner_len = open_owner_len;
            row.owner_mds_id = ot->mds_id;
            row.owner_boot_epoch = ot->boot_epoch;
            (void)mds_coord_open_put(ot->cat, &row);
        }

        pthread_mutex_unlock(&ot->locks[file_lock_idx]);
        return 0;
    }

    /* No prior open by this {clientid, open_owner}: allocate fresh.
     *
     * Allocation under the file-stripe lock is acceptable because
     * calloc on a small struct does not block on I/O and the
     * per-fileid stripe is only contended by other ops on the
     * same fileid. */
    os = calloc(1, sizeof(*os));
    if (os == NULL) {
        rc = -2; /* NFS4ERR_RESOURCE */
        goto out_unlock;
    }

    os->stateid.seqid = 1;
    make_stateid_other(ot, os->stateid.other);
    os->clientid = clientid;
    os->fileid = fileid;
    os->share_access = share_access;
    os->share_deny = share_deny;
    if (open_owner != NULL && open_owner_len > 0) {
        memcpy(os->open_owner, open_owner, open_owner_len);
        os->open_owner_len = open_owner_len;
    }
    stateid_lock_idx = stateid_lock_stripe(os->stateid.other);

    /* Check share conflicts against all existing opens for this file. */
    if (share_conflict(fo, share_access, share_deny)) {
        rc = -1;  /* NFS4ERR_SHARE_DENIED */
        goto out_unlock_free;
    }

    /* Insert into per-file chain. */
    fo = get_or_create_file_opens(ot, fileid);
    if (fo == NULL) {
        rc = -2;
        goto out_unlock_free;
    }
    os->file_next = fo->head;
    fo->head = os;

    /* Insert into stateid hash. */
    pthread_rwlock_wrlock(&ot->stateid_locks[stateid_lock_idx]);
    idx = hash_other(os->stateid.other);
    os->hash_next = ot->stateid_hash[idx];
    ot->stateid_hash[idx] = os;
    pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);

    /* Output. */
    *out_stateid = os->stateid;

    /* Persist to RonDB if catalogue is set (shared-attr)
     * and transient caching is off. */
    if (ot->cat != NULL && !ot->skip_ndb_persist) {
        struct mds_coord_open_row row;
        memset(&row, 0, sizeof(row));
        memcpy(row.stateid_other, os->stateid.other, NFS4_OTHER_SIZE);
        row.seqid = os->stateid.seqid;
        row.clientid = clientid;
        row.fileid = fileid;
        row.share_access = share_access;
        row.share_deny = share_deny;
        if (open_owner != NULL && open_owner_len > 0) {
            memcpy(row.open_owner, open_owner, open_owner_len);
        }
        row.open_owner_len = open_owner_len;
        row.owner_mds_id = ot->mds_id;
        row.owner_boot_epoch = ot->boot_epoch;
        (void)mds_coord_open_put(ot->cat, &row);
    }

    pthread_mutex_unlock(&ot->locks[file_lock_idx]);
    return rc;

out_unlock_free:
    pthread_mutex_unlock(&ot->locks[file_lock_idx]);
    free(os);
    return rc;

out_unlock:
    pthread_mutex_unlock(&ot->locks[file_lock_idx]);
    return rc;
}

/* ----------------------------------------------------------------------- */

int open_state_close(struct open_state_table *ot,
                     uint64_t clientid,
                     const struct nfs4_stateid *stateid,
                     struct nfs4_stateid *out_stateid)
{
    struct nfs4_open_state *os;
    uint32_t file_lock_idx;
    uint32_t stateid_lock_idx;
    uint64_t fileid;
    int rc = 0;

    if (ot == NULL || stateid == NULL || out_stateid == NULL) {
        return -1;
}

    stateid_lock_idx = stateid_lock_stripe(stateid->other);
    pthread_rwlock_rdlock(&ot->stateid_locks[stateid_lock_idx]);
    os = find_by_other(ot, stateid->other);
    if (os == NULL) {
        pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);
        return -1;  /* NFS4ERR_BAD_STATEID */
    }
    fileid = os->fileid;
    pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);

    file_lock_idx = lock_stripe(fileid);
    pthread_mutex_lock(&ot->locks[file_lock_idx]);
    pthread_rwlock_wrlock(&ot->stateid_locks[stateid_lock_idx]);

    os = find_by_other(ot, stateid->other);
    if (os == NULL || os->fileid != fileid) {
        rc = -1;  /* NFS4ERR_BAD_STATEID */
        goto out;
    }
    if (os->clientid != clientid) {
        rc = -1;  /* NFS4ERR_BAD_STATEID -- not owner */
        goto out;
    }

    /*
     * Validate seqid (RFC 5661 S8.2.1, RFC 8881 S8.2.2):
     * - seqid == 0   -> "current" / "don't care"; server uses its
     *                  own stored seqid and skips the comparison.
     *                  Standard pynfs convention; some Linux
     *                  client paths (LOCKU, OPEN_DOWNGRADE) also
     *                  emit zero-seqid stateids per RFC.
     * - seqid <  current -> NFS4ERR_OLD_STATEID (rc = -4)
     * - seqid >  current -> NFS4ERR_BAD_STATEID (rc = -1)
     */
    if (stateid->seqid != 0 &&
        stateid->seqid != os->stateid.seqid) {
        if (stateid->seqid < os->stateid.seqid) {
            rc = -4;  /* NFS4ERR_OLD_STATEID */
        } else {
            rc = -1;  /* NFS4ERR_BAD_STATEID */
}
        goto out;
    }

    /* Build closing stateid: same other, seqid + 1. */
    *out_stateid = os->stateid;
    out_stateid->seqid = os->stateid.seqid + 1;

    /* Remove from both hash tables. */
    unhash_stateid(ot, os);
    unlink_from_file(ot, os);

    /* Delete from RonDB if catalogue is set (shared-attr)
     * and transient caching is off. */
    if (ot->cat != NULL && !ot->skip_ndb_persist) {
        (void)mds_coord_open_del(ot->cat, stateid->other);
    }

    free(os);

out:
    pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);
    pthread_mutex_unlock(&ot->locks[file_lock_idx]);
    return rc;
}

/* ----------------------------------------------------------------------- */

int open_state_find(struct open_state_table *ot,
                    const struct nfs4_stateid *stateid,
                    struct nfs4_open_state *out)
{
    const struct nfs4_open_state *os;
    uint32_t stateid_lock_idx;
    int rc = -1;

    if (ot == NULL || stateid == NULL || out == NULL) {
        return -1;
}

    stateid_lock_idx = stateid_lock_stripe(stateid->other);
    pthread_rwlock_rdlock(&ot->stateid_locks[stateid_lock_idx]);
    os = find_by_other(ot, stateid->other);
    if (os != NULL) {
        *out = *os;
        /* Clear internal chain pointers in the copy. */
        out->hash_next = NULL;
        out->file_next = NULL;
        rc = 0;
    }
    pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);
    return rc;
}

int open_state_file_has_writers(struct open_state_table *ot, uint64_t fileid)
{
    const struct file_opens *fo;
    int has_writers;
    uint32_t file_lock_idx;
    if (ot == NULL) {
        return 0;
    }
    file_lock_idx = lock_stripe(fileid);
    pthread_mutex_lock(&ot->locks[file_lock_idx]);
    fo = find_file_opens(ot, fileid);
    has_writers = (fo != NULL && fo->head != NULL) ? 1 : 0;
    pthread_mutex_unlock(&ot->locks[file_lock_idx]);
    return has_writers;
}


void open_state_close_all_for_client(struct open_state_table *ot,
                                     uint64_t clientid)
{
    uint32_t b, s;
    if (ot == NULL) { return; }

    /* Lock all stripes to prevent races during bulk cleanup. */
    for (s = 0; s < OPEN_STATE_LOCK_STRIPES; s++) {
        pthread_mutex_lock(&ot->locks[s]);
    }
    for (s = 0; s < OPEN_STATE_LOCK_STRIPES; s++) {
        pthread_rwlock_wrlock(&ot->stateid_locks[s]);
    }

    for (b = 0; b < STATEID_HASH_BUCKETS; b++) {
        struct nfs4_open_state **pp = &ot->stateid_hash[b];
        while (*pp != NULL) {
            struct nfs4_open_state *os = *pp;
            if (os->clientid == clientid) {
                *pp = os->hash_next;
                unlink_from_file(ot, os);
                free(os);
            } else {
                pp = &os->hash_next;
            }
        }
    }

    for (s = 0; s < OPEN_STATE_LOCK_STRIPES; s++) {
        pthread_rwlock_unlock(&ot->stateid_locks[s]);
    }
    for (s = 0; s < OPEN_STATE_LOCK_STRIPES; s++) {
        pthread_mutex_unlock(&ot->locks[s]);
    }
}

int open_state_downgrade(struct open_state_table *ot,
                         uint64_t clientid,
                         const struct nfs4_stateid *stateid,
                         uint32_t new_share_access,
                         uint32_t new_share_deny,
                         struct nfs4_stateid *out_stateid)
{
    struct nfs4_open_state *os;
    uint32_t file_lock_idx;
    uint32_t stateid_lock_idx;
    uint64_t fileid;

    if (ot == NULL || stateid == NULL || out_stateid == NULL) {
        return -1;
    }
    stateid_lock_idx = stateid_lock_stripe(stateid->other);
    pthread_rwlock_rdlock(&ot->stateid_locks[stateid_lock_idx]);

    os = find_by_other(ot, stateid->other);
    if (os == NULL) {
        pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);
        return -2; /* BAD_STATEID */
    }
    fileid = os->fileid;
    pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);

    file_lock_idx = lock_stripe(fileid);
    pthread_mutex_lock(&ot->locks[file_lock_idx]);
    pthread_rwlock_wrlock(&ot->stateid_locks[stateid_lock_idx]);

    os = find_by_other(ot, stateid->other);
    if (os == NULL || os->fileid != fileid) {
        pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);
        pthread_mutex_unlock(&ot->locks[file_lock_idx]);
        return -2;
    }
    if (os->clientid != clientid) {
        pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);
        pthread_mutex_unlock(&ot->locks[file_lock_idx]);
        return -2;
    }
    /*
     * Zero-seqid: per RFC 5661 S8.2.2 the server MUST treat a
     * zero seqid as "current" -- use its own stored seqid and
     * skip the strict equality check.  See open_state_close()
     * for the matching CLOSE-path comment.
     */
    if (stateid->seqid != 0 &&
        os->stateid.seqid != stateid->seqid) {
        int seq_rc = (stateid->seqid < os->stateid.seqid) ? -4 : -3;

        pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);
        pthread_mutex_unlock(&ot->locks[file_lock_idx]);
        return seq_rc;
    }
    if ((new_share_access & ~os->share_access) != 0) {
        pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);
        pthread_mutex_unlock(&ot->locks[file_lock_idx]);
        return -5; /* INVAL */
    }

    os->share_access = new_share_access;
    os->share_deny = new_share_deny;
    os->stateid.seqid++;

    *out_stateid = os->stateid;

    pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);
    pthread_mutex_unlock(&ot->locks[file_lock_idx]);
    return 0;
}

static int reload_cb(const uint8_t *other,
                     uint64_t clientid, uint64_t fileid,
                     uint32_t share_access, uint32_t share_deny,
                     const uint8_t *owner, uint32_t owner_len,
                     void *arg)
{
    struct open_state_table *ot = arg;
    struct nfs4_open_state *os = calloc(1, sizeof(*os));
    uint32_t idx;

    if (os == NULL) { return 0; }
    memcpy(os->stateid.other, other, NFS4_OTHER_SIZE);
    os->stateid.seqid = 1;
    os->clientid = clientid;
    os->fileid = fileid;
    os->share_access = share_access;
    os->share_deny = share_deny;
    os->open_owner_len = (owner_len <= NFS4_OPEN_OWNER_MAX) ? owner_len : NFS4_OPEN_OWNER_MAX;
    if (owner_len > 0) {
        memcpy(os->open_owner, owner, os->open_owner_len);
    }

    idx = hash_other(os->stateid.other);
    os->hash_next = ot->stateid_hash[idx];
    ot->stateid_hash[idx] = os;

    /* Rebuild the per-file index (file_hash + file_next) so that
     * share-deny conflict detection and has_writers work after reload. */
    {
        struct file_opens *fo = get_or_create_file_opens(ot, fileid);
        if (fo != NULL) {
            os->file_next = fo->head;
            fo->head = os;
        }
    }

    return 0;
}

int open_state_table_reload(struct open_state_table *ot, void *unused)
{
    (void)ot; (void)unused;
    return 0; /* Memory-only: nothing to reload. */
}

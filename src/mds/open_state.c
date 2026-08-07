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
#include "session.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "mds_op_metrics.h"

/* -----------------------------------------------------------------------
 * Hash table sizing (Wave 4 T4.2)
 *
 * Bucket and stripe counts are per-table fields configured at init
 * time (open_state_table_init_ex); the defaults live in open_state.h
 * (OPEN_STATE_DEFAULT_*).  OPEN_STATE_POOL_CHUNK is the growth unit
 * of the per-stripe open-state allocation pool: entries are handed
 * out and recycled under the already-held file-stripe mutex, so the
 * hot path performs no malloc/free per OPEN/CLOSE.
 * ----------------------------------------------------------------------- */

#define OPEN_STATE_POOL_CHUNK 64U

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

/** Growth unit of the per-stripe allocation pool. */
struct os_pool_chunk {
    struct os_pool_chunk  *next;
    struct nfs4_open_state entries[OPEN_STATE_POOL_CHUNK];
};

/** Per-stripe open-state pool.  Guarded by the stripe's file mutex
 *  (ot->locks[i]); free entries are linked through ->hash_next. */
struct os_stripe_pool {
    struct nfs4_open_state *free_head;
    struct os_pool_chunk   *chunks;
};

struct open_state_table {
    struct nfs4_open_state **stateid_hash;  /* [stateid_buckets] */
    struct file_opens      **file_hash;     /* [file_buckets] */
    uint32_t                stateid_buckets;
    uint32_t                file_buckets;
    uint32_t                lock_stripes;
    atomic_uint_fast64_t    next_other_seq;
    uint32_t                mds_id;
    struct mds_catalogue   *cat;  /**< RonDB catalogue (shared-attr). */
    uint64_t                boot_epoch; /**< For fencing (shared-attr). */
    bool                    skip_ndb_persist; /**< Skip NDB writes for perf. */
    pthread_mutex_t        *locks;          /* [lock_stripes] */
    pthread_rwlock_t       *stateid_locks;  /* [lock_stripes] */
    struct os_stripe_pool  *pools;          /* [lock_stripes] */
};


static uint32_t hash_fileid(const struct open_state_table *ot,
                            uint64_t fileid);

/* Stripe lock index from fileid.
 * MUST derive from the same bucket index as hash_fileid() to ensure
 * all operations on the same hash bucket are serialized by the same
 * lock stripe.  Using an independent hash allowed two fileids in the
 * same bucket to hold different locks, corrupting the hash chain
 * under concurrency. */
static inline uint32_t lock_stripe(const struct open_state_table *ot,
                                   uint64_t fileid)
{
    return hash_fileid(ot, fileid) % ot->lock_stripes;
}

/* -----------------------------------------------------------------------
 * Hash functions
 * ----------------------------------------------------------------------- */

/** fmix64 finaliser shared by both hash tables. */
static uint64_t os_mix64(uint64_t v)
{
    v ^= v >> 33;
    v *= 0xff51afd7ed558ccdULL;
    v ^= v >> 33;
    return v;
}

static uint32_t hash_other(const struct open_state_table *ot,
                           const uint8_t other[NFS4_OTHER_SIZE])
{
    uint64_t v;

    memcpy(&v, other + 4, sizeof(v));  /* counter portion */
    return (uint32_t)(os_mix64(v) % ot->stateid_buckets);
}

static uint32_t hash_fileid(const struct open_state_table *ot,
                            uint64_t fileid)
{
    return (uint32_t)(os_mix64(fileid) % ot->file_buckets);
}

static inline uint32_t stateid_lock_stripe(
    const struct open_state_table *ot,
    const uint8_t other[NFS4_OTHER_SIZE])
{
    return hash_other(ot, other) % ot->lock_stripes;
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
    uint32_t idx = hash_other(ot, other);
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
    uint32_t idx = hash_fileid(ot, fileid);
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

    idx = hash_fileid(ot, fileid);
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
    uint32_t idx = hash_fileid(ot, fileid);
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
    uint32_t idx = hash_other(ot, os->stateid.other);
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
 * Per-stripe open-state allocation pool (Wave 4 T4.2)
 *
 * Callers hold ot->locks[stripe] (or every stripe lock on the bulk
 * cleanup paths), so the free list needs no locking of its own.
 * Chunk allocation failure degrades to a plain calloc entry
 * (pooled == false), which os_free() releases with free().
 * ----------------------------------------------------------------------- */

/** Allocate a zeroed open-state record from the stripe pool. */
static struct nfs4_open_state *os_alloc(struct open_state_table *ot,
                                        uint32_t stripe)
{
    struct os_stripe_pool *pool = &ot->pools[stripe];
    struct nfs4_open_state *os;

    if (pool->free_head == NULL) {
        struct os_pool_chunk *chunk = calloc(1, sizeof(*chunk));
        uint32_t i;

        if (chunk == NULL) {
            /* Degrade to a plain heap entry (pooled stays false). */
            return calloc(1, sizeof(struct nfs4_open_state));
        }
        chunk->next = pool->chunks;
        pool->chunks = chunk;
        for (i = 0; i < OPEN_STATE_POOL_CHUNK; i++) {
            chunk->entries[i].hash_next = pool->free_head;
            pool->free_head = &chunk->entries[i];
        }
    }

    os = pool->free_head;
    pool->free_head = os->hash_next;
    memset(os, 0, sizeof(*os));
    os->pooled = true;
    return os;
}

/** Return a record to its stripe pool (or the heap).  The caller has
 *  already unhashed and unlinked it. */
static void os_free(struct open_state_table *ot, uint32_t stripe,
                    struct nfs4_open_state *os)
{
    if (!os->pooled) {
        free(os);
        return;
    }
    os->hash_next = ot->pools[stripe].free_head;
    ot->pools[stripe].free_head = os;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int open_state_table_init_ex(uint32_t mds_id,
                             uint32_t file_buckets,
                             uint32_t stateid_buckets,
                             uint32_t lock_stripes,
                             struct open_state_table **out)
{
    struct open_state_table *ot;

    if (out == NULL) {
        return -1;
}

    if (file_buckets == 0) {
        file_buckets = OPEN_STATE_DEFAULT_FILE_BUCKETS;
    }
    if (stateid_buckets == 0) {
        stateid_buckets = OPEN_STATE_DEFAULT_STATEID_BUCKETS;
    }
    if (lock_stripes == 0) {
        lock_stripes = OPEN_STATE_DEFAULT_LOCK_STRIPES;
    }
    /* lock_stripe() maps bucket -> stripe, so more stripes than
     * buckets would leave stripes uncovered; clamp to the smaller
     * bucket count. */
    if (lock_stripes > file_buckets) {
        lock_stripes = file_buckets;
    }
    if (lock_stripes > stateid_buckets) {
        lock_stripes = stateid_buckets;
    }

    ot = calloc(1, sizeof(*ot));
    if (ot == NULL) {
        return -1;
}

    ot->file_buckets = file_buckets;
    ot->stateid_buckets = stateid_buckets;
    ot->lock_stripes = lock_stripes;

    /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
    ot->stateid_hash = calloc(stateid_buckets,
                              sizeof(struct nfs4_open_state *));
    /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
    ot->file_hash = calloc(file_buckets,
                           sizeof(struct file_opens *));
    ot->locks = calloc(lock_stripes, sizeof(pthread_mutex_t));
    ot->stateid_locks = calloc(lock_stripes, sizeof(pthread_rwlock_t));
    ot->pools = calloc(lock_stripes, sizeof(struct os_stripe_pool));
    if (ot->stateid_hash == NULL || ot->file_hash == NULL ||
        ot->locks == NULL || ot->stateid_locks == NULL ||
        ot->pools == NULL) {
        /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
        free(ot->stateid_hash);
        /* NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion) */
        free(ot->file_hash);
        free(ot->locks);
        free(ot->stateid_locks);
        free(ot->pools);
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
    for (uint32_t li = 0; li < ot->lock_stripes; li++) {
        pthread_mutex_init(&ot->locks[li], NULL);
        pthread_rwlock_init(&ot->stateid_locks[li], NULL);
    }

    *out = ot;
    return 0;
}

int open_state_table_init(uint32_t mds_id, struct open_state_table **out)
{
    return open_state_table_init_ex(mds_id, 0, 0, 0, out);
}

void open_state_table_destroy(struct open_state_table *ot)
{
    uint32_t i;

    if (ot == NULL) {
        return;
}

    /* Free all non-pooled open states via the stateid hash; pooled
     * entries are chunk memory released wholesale below. */
    for (i = 0; i < ot->stateid_buckets; i++) {
        struct nfs4_open_state *os = ot->stateid_hash[i];
        struct nfs4_open_state *next;

        while (os != NULL) {
            next = os->hash_next;
            if (!os->pooled) {
                free(os);
            }
            os = next;
        }
    }

    /* Free all file_opens heads. */
    for (i = 0; i < ot->file_buckets; i++) {
        struct file_opens *fo = ot->file_hash[i];
        struct file_opens *next;

        while (fo != NULL) {
            next = fo->hash_next;
            free(fo);
            fo = next;
        }
    }

    for (uint32_t li = 0; li < ot->lock_stripes; li++) {
        pthread_mutex_destroy(&ot->locks[li]);
        pthread_rwlock_destroy(&ot->stateid_locks[li]);
    }

    /* Release the per-stripe pool chunks. */
    for (i = 0; i < ot->lock_stripes; i++) {
        struct os_pool_chunk *chunk = ot->pools[i].chunks;

        while (chunk != NULL) {
            struct os_pool_chunk *next = chunk->next;

            free(chunk);
            chunk = next;
        }
    }

    free(ot->pools);
    free(ot->locks);
    free(ot->stateid_locks);
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

    MDS_PHASE_SCOPE(MDS_PHASE_STATE);

    file_lock_idx = lock_stripe(ot, fileid);
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
        uint32_t prev_seqid;
        uint32_t prev_access;
        uint32_t prev_deny;

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
            stateid_lock_stripe(ot, existing->stateid.other);
        pthread_rwlock_wrlock(
            &ot->stateid_locks[stateid_lock_idx]);

        /* Snapshot the pre-upgrade values so a failed persist can
         * restore them (T4.1 persist-failure policy below). */
        prev_seqid = existing->stateid.seqid;
        prev_access = existing->share_access;
        prev_deny = existing->share_deny;

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
         * insert, so this is an in-place update).
         *
         * T4.1 persist-failure policy: fail the OPEN.  Under the
         * any-MDS routing contract the durable row is what makes
         * this open's share reservation visible to peer MDSes;
         * publishing in-memory state whose persist failed would
         * let a peer grant a conflicting open.  The RonDB layer
         * already retries transient NDB errors internally, so a
         * failure surfacing here is treated as real: restore the
         * pre-upgrade state and return -5 (mapped to
         * NFS4ERR_DELAY -- the client retries once NDB heals). */
        if (ot->cat != NULL && !ot->skip_ndb_persist) {
            struct mds_coord_open_row row;
            enum mds_status pst;

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
            pst = mds_coord_open_put(ot->cat, &row);
            /* NOSUPPORT = the backend has no shared open-state
             * table at all; nothing durable was expected, so it
             * is not a persist failure (same effective contract
             * as cat == NULL). */
            if (pst != MDS_OK && pst != MDS_ERR_NOSUPPORT) {
                pthread_rwlock_wrlock(
                    &ot->stateid_locks[stateid_lock_idx]);
                existing->stateid.seqid = prev_seqid;
                existing->share_access = prev_access;
                existing->share_deny = prev_deny;
                pthread_rwlock_unlock(
                    &ot->stateid_locks[stateid_lock_idx]);
                rc = -5; /* NFS4ERR_DELAY */
                goto out_unlock;
            }
        }

        pthread_mutex_unlock(&ot->locks[file_lock_idx]);
        return 0;
    }

    /* No prior open by this {clientid, open_owner}: allocate fresh
     * from the stripe's pool (T4.2) -- the common case is a free-
     * list pop under the already-held stripe mutex, no malloc. */
    os = os_alloc(ot, file_lock_idx);
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
    stateid_lock_idx = stateid_lock_stripe(ot, os->stateid.other);

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
    idx = hash_other(ot, os->stateid.other);
    os->hash_next = ot->stateid_hash[idx];
    ot->stateid_hash[idx] = os;
    pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);

    /* Output. */
    *out_stateid = os->stateid;

    /* Persist to RonDB if catalogue is set (shared-attr)
     * and transient caching is off.
     *
     * T4.1 persist-failure policy: fail the OPEN.  A stateid whose
     * durable row was never written must not be handed to the
     * client -- peer MDSes would miss its share reservation and a
     * restart would forget it entirely.  Unwind the in-memory
     * insert (stateid hash + file chain) and return -5 (mapped to
     * NFS4ERR_DELAY so the client retries once NDB heals). */
    if (ot->cat != NULL && !ot->skip_ndb_persist) {
        struct mds_coord_open_row row;
        enum mds_status pst;

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
        pst = mds_coord_open_put(ot->cat, &row);
        /* NOSUPPORT = no shared open-state table on this backend;
         * treated as "nothing to persist", same as cat == NULL. */
        if (pst != MDS_OK && pst != MDS_ERR_NOSUPPORT) {
            pthread_rwlock_wrlock(
                &ot->stateid_locks[stateid_lock_idx]);
            unhash_stateid(ot, os);
            pthread_rwlock_unlock(
                &ot->stateid_locks[stateid_lock_idx]);
            unlink_from_file(ot, os);
            rc = -5; /* NFS4ERR_DELAY */
            goto out_unlock_free;
        }
    }

    pthread_mutex_unlock(&ot->locks[file_lock_idx]);
    return rc;

out_unlock_free:
    /* Recycle under the still-held stripe lock (pool contract). */
    os_free(ot, file_lock_idx, os);
    pthread_mutex_unlock(&ot->locks[file_lock_idx]);
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

    MDS_PHASE_SCOPE(MDS_PHASE_STATE);

    stateid_lock_idx = stateid_lock_stripe(ot, stateid->other);
    pthread_rwlock_rdlock(&ot->stateid_locks[stateid_lock_idx]);
    os = find_by_other(ot, stateid->other);
    if (os == NULL) {
        pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);
        return -1;  /* NFS4ERR_BAD_STATEID */
    }
    fileid = os->fileid;
    pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);

    file_lock_idx = lock_stripe(ot, fileid);
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

    os_free(ot, file_lock_idx, os);

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

    MDS_PHASE_SCOPE(MDS_PHASE_STATE);

    stateid_lock_idx = stateid_lock_stripe(ot, stateid->other);
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
    file_lock_idx = lock_stripe(ot, fileid);
    pthread_mutex_lock(&ot->locks[file_lock_idx]);
    fo = find_file_opens(ot, fileid);
    has_writers = (fo != NULL && fo->head != NULL) ? 1 : 0;
    pthread_mutex_unlock(&ot->locks[file_lock_idx]);
    return has_writers;
}

bool open_state_has_other_writer(struct open_state_table *ot,
                                 uint64_t fileid,
                                 uint64_t clientid)
{
    const struct file_opens *fo;
    const struct nfs4_open_state *os;
    bool found = false;
    uint32_t file_lock_idx;

    if (ot == NULL) {
        return false;
    }
    file_lock_idx = lock_stripe(ot, fileid);
    pthread_mutex_lock(&ot->locks[file_lock_idx]);
    fo = find_file_opens(ot, fileid);
    if (fo != NULL) {
        for (os = fo->head; os != NULL; os = os->file_next) {
            if (os->clientid != clientid &&
                (os->share_access & OPEN4_SHARE_ACCESS_WRITE) != 0) {
                found = true;
                break;
            }
        }
    }
    pthread_mutex_unlock(&ot->locks[file_lock_idx]);
    return found;
}


void open_state_close_all_for_client(struct open_state_table *ot,
                                     uint64_t clientid)
{
    uint32_t b, s;
    if (ot == NULL) { return; }

    /* Lock all stripes to prevent races during bulk cleanup. */
    for (s = 0; s < ot->lock_stripes; s++) {
        pthread_mutex_lock(&ot->locks[s]);
    }
    for (s = 0; s < ot->lock_stripes; s++) {
        pthread_rwlock_wrlock(&ot->stateid_locks[s]);
    }

    for (b = 0; b < ot->stateid_buckets; b++) {
        struct nfs4_open_state **pp = &ot->stateid_hash[b];
        while (*pp != NULL) {
            struct nfs4_open_state *os = *pp;
            if (os->clientid == clientid) {
                *pp = os->hash_next;
                unlink_from_file(ot, os);
                /* Every stripe lock is held, so recycling into
                 * the record's own stripe pool is safe. */
                os_free(ot, lock_stripe(ot, os->fileid), os);
            } else {
                pp = &os->hash_next;
            }
        }
    }

    for (s = 0; s < ot->lock_stripes; s++) {
        pthread_rwlock_unlock(&ot->stateid_locks[s]);
    }
    for (s = 0; s < ot->lock_stripes; s++) {
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

    MDS_PHASE_SCOPE(MDS_PHASE_STATE);

    stateid_lock_idx = stateid_lock_stripe(ot, stateid->other);
    pthread_rwlock_rdlock(&ot->stateid_locks[stateid_lock_idx]);

    os = find_by_other(ot, stateid->other);
    if (os == NULL) {
        pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);
        return -2; /* BAD_STATEID */
    }
    fileid = os->fileid;
    pthread_rwlock_unlock(&ot->stateid_locks[stateid_lock_idx]);

    file_lock_idx = lock_stripe(ot, fileid);
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

    idx = hash_other(ot, os->stateid.other);
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

/*
 * RFC 8881 §8.4.3 courtesy-client support: revoke open state on a
 * single file for all clients whose lease has expired.  Called from
 * op_open (compound_data_io.c) when a share conflict is detected.
 *
 * Three-phase design to avoid an ABBA deadlock with the lease reaper:
 *   Reaper:   st->locks[0] → ot->locks (via close_all_for_client)
 *   Us:       ot->locks → st->locks[0] (via session_client_lease_expired)
 *
 * Phase 1 — collect unique clientids from the per-file chain under
 *           the open-state file-stripe lock (no session lock).
 * Phase 2 — check each collected clientid against the session table
 *           (no open-state locks held).
 * Phase 3 — re-acquire open-state locks and remove entries whose
 *           clientid was marked expired in phase 2.
 */

/* Bounded scratch buffer for phase-1 client collection.  64 is far
 * more than any realistic per-file open count. */
#define REVOKE_MAX_CLIENTS 64

int open_state_revoke_expired_for_file(struct open_state_table *ot,
                                       struct session_table *st,
                                       uint64_t fileid)
{
    uint64_t cids[REVOKE_MAX_CLIENTS];
    bool     exp[REVOKE_MAX_CLIENTS];
    uint32_t n_cids = 0;
    uint32_t file_lock_idx;
    uint32_t s, i;
    int revoked = 0;
    bool any_expired = false;

    if (ot == NULL || st == NULL) {
        return 0;
    }

    file_lock_idx = lock_stripe(ot, fileid);

    /* ---- Phase 1: collect unique clientids (open-state lock only) ---- */
    pthread_mutex_lock(&ot->locks[file_lock_idx]);
    {
        const struct file_opens *fo = find_file_opens(ot, fileid);
        if (fo != NULL) {
            const struct nfs4_open_state *os;
            for (os = fo->head; os != NULL; os = os->file_next) {
                bool seen = false;
                for (i = 0; i < n_cids; i++) {
                    if (cids[i] == os->clientid) {
                        seen = true;
                        break;
                    }
                }
                if (!seen && n_cids < REVOKE_MAX_CLIENTS) {
                    cids[n_cids++] = os->clientid;
                }
            }
        }
    }
    pthread_mutex_unlock(&ot->locks[file_lock_idx]);

    if (n_cids == 0) {
        return 0;
    }

    /* ---- Phase 2: check lease expiry (session lock only) ---- */
    for (i = 0; i < n_cids; i++) {
        exp[i] = session_client_lease_expired(st, cids[i]);
        if (exp[i]) {
            any_expired = true;
        }
    }
    if (!any_expired) {
        return 0;
    }

    /* ---- Phase 3: remove expired entries (open-state locks only) ---- */
    pthread_mutex_lock(&ot->locks[file_lock_idx]);
    for (s = 0; s < ot->lock_stripes; s++) {
        pthread_rwlock_wrlock(&ot->stateid_locks[s]);
    }

    {
        struct file_opens *fo = find_file_opens(ot, fileid);
        if (fo != NULL) {
            struct nfs4_open_state **pp = &fo->head;
            while (*pp != NULL) {
                struct nfs4_open_state *os = *pp;
                bool is_expired = false;
                for (i = 0; i < n_cids; i++) {
                    if (cids[i] == os->clientid && exp[i]) {
                        is_expired = true;
                        break;
                    }
                }
                if (is_expired) {
                    *pp = os->file_next;
                    unhash_stateid(ot, os);
                    /* All states on this chain share the file's
                     * stripe, whose mutex is held. */
                    os_free(ot, file_lock_idx, os);
                    revoked++;
                } else {
                    pp = &os->file_next;
                }
            }
            if (fo->head == NULL) {
                maybe_free_file_opens(ot, fileid);
            }
        }
    }

    for (s = 0; s < ot->lock_stripes; s++) {
        pthread_rwlock_unlock(&ot->stateid_locks[s]);
    }
    pthread_mutex_unlock(&ot->locks[file_lock_idx]);
    return revoked;
}

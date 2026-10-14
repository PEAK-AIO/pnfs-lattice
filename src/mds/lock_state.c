/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * lock_state.c -- NFSv4.1 byte-range lock manager.
 *
 * Implements LOCK, LOCKT, LOCKU per RFC 8881 S18.10-18.12 with the
 * per-lock-owner stateid lifecycle the protocol (and the Linux
 * client) require:
 *
 *   - ONE lock stateid per (fileid, clientid, lock-owner).  The
 *     stateid's `other` field is fixed when the owner state is
 *     created; its seqid increments on every successful LOCK and
 *     LOCKU touching that owner (RFC 8881 S8.2.2).  A LOCK for an
 *     existing owner therefore returns the SAME `other` with a
 *     bumped seqid -- returning a fresh stateid per LOCK (the old
 *     behaviour) makes the Linux client discard the reply and
 *     retransmit forever (~600 LOCK/s storm).
 *   - POSIX range semantics: a new lock by the same owner REPLACES
 *     the overlapping portions of that owner's existing ranges
 *     (upgrade/downgrade with splitting); LOCKU removes only the
 *     given range, splitting holders as needed.  The owner state
 *     survives its last range so TEST_STATEID keeps reporting it
 *     and FREE_STATEID can release it (NFS4ERR_LOCKS_HELD while
 *     ranges remain).
 *   - Conflict checks compare DIFFERENT owners only (a lock owner
 *     never conflicts with itself), including two owners on the
 *     same clientid (two processes on one client).
 *
 * Persistence: each in-memory range owns one row in the shared
 * byte_locks table, keyed by (fileid, lock_id) where lock_id is
 * allocated from an atomic counter -- unique per row, so concurrent
 * lock traffic never rewrites the same primary key (the previous
 * design reused one key for every lock, convoying the NDB row lock
 * until a worker wedged inside execute()).  Rows are updated on
 * trim, deleted on release, and reaped per client on lease expiry.
 *
 * Thread safety: 16 stripe locks partition all mutable state.  Each
 * stripe owns its own file_hash and sid_hash tables.  The stripe
 * index is encoded in byte 4 of the lock stateid so stateid-keyed
 * entry points can lock the right stripe before any lookup.
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <pthread.h>
#include <endian.h>
#include <time.h>

#include "pnfs_mds.h"
#include "lock_state.h"
#include "session.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "mds_op_metrics.h"

/* ----------------------------------------------------------------------- */

/** Per-stripe hash table sizing.  16 stripes x 32 = 512 effective buckets. */
#define FILE_HASH_PER_STRIPE  32
#define SID_HASH_PER_STRIPE   32
#define LOCK_STRIPES          16

/**
 * One held byte range.  Owned by exactly one lock_owner_state; the
 * lock_id names this range's row in the shared byte_locks table
 * (unique across the table's lifetime -- see lock_table.next_seq).
 */
struct lock_range {
    uint64_t           offset;
    uint64_t           length;    /* Never 0; UINT64_MAX = to EOF. */
    uint32_t           lock_type; /* READ_LT or WRITE_LT */
    uint64_t           lock_id;   /* Persistence row id. */
    struct lock_range *next;
};

/**
 * Per-(fileid, clientid, lock-owner) state.  Owns THE lock stateid
 * for that owner on that file and the list of currently held
 * ranges.  Lives from the first LOCK (new_lock_owner) until
 * FREE_STATEID / client cleanup -- NOT until the last unlock, so a
 * rangeless owner state still answers TEST_STATEID with NFS4_OK.
 */
struct lock_owner_state {
    uint64_t                 fileid;
    uint64_t                 clientid;
    uint8_t                  owner[LOCK_OWNER_MAX];
    uint32_t                 owner_len;
    struct nfs4_stateid      stateid;   /* seqid = current value */
    struct lock_range       *ranges;
    struct lock_owner_state *file_next; /* Per-file chain */
    struct lock_owner_state *sid_next;  /* Stateid hash chain */
};

struct file_locks {
    uint64_t                 fileid;
    struct lock_owner_state *head;
    struct file_locks       *hash_next;
};

/**
 * Per-stripe partition.  All fields are accessed only while the
 * stripe's mutex is held, eliminating cross-stripe data races.
 */
struct lock_stripe {
    struct file_locks       *file_hash[FILE_HASH_PER_STRIPE];
    struct lock_owner_state *sid_hash[SID_HASH_PER_STRIPE];
    pthread_mutex_t          lock;
};

struct lock_table {
    struct lock_stripe  stripes[LOCK_STRIPES];
    _Atomic uint64_t    next_seq;  /* Stateid + lock_id source. */
    uint32_t            mds_id;
    struct mds_catalogue *cat;     /**< RonDB catalogue (shared-attr). */
    uint64_t            boot_epoch;
};

/* ----------------------------------------------------------------------- */

static uint32_t fhash(uint64_t fileid)
{
    uint64_t h = fileid;
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdULL;
    h ^= h >> 33;
    return (uint32_t)(h % FILE_HASH_PER_STRIPE);
}

static uint32_t shash(const uint8_t other[NFS4_OTHER_SIZE])
{
    uint64_t v;
    memcpy(&v, other + 4, sizeof(v));
    v ^= v >> 33;
    v *= 0xff51afd7ed558ccdULL;
    v ^= v >> 33;
    return (uint32_t)(v % SID_HASH_PER_STRIPE);
}

static uint32_t stripe(uint64_t fileid)
{
    return (uint32_t)(fileid % LOCK_STRIPES);
}

/**
 * Extract the stripe index encoded in a lock stateid.
 * Byte other[4] holds the stripe index written by make_lock_stateid.
 */
static uint32_t stateid_stripe(const uint8_t other[NFS4_OTHER_SIZE])
{
    return (uint32_t)other[4] % LOCK_STRIPES;
}

/**
 * Mint the stateid for a NEW lock-owner state.  Called exactly once
 * per owner state; every subsequent LOCK/LOCKU on that owner only
 * bumps stateid.seqid.
 *
 * Layout of stateid.other[12]:
 *   [0..3] : mds_id  (big-endian)
 *   [4]    : stripe index  (0--15)
 *   [5..11]: sequence counter (big-endian, high 7 bytes)
 *
 * seqid starts at 0 here; the grant path increments it so the first
 * reply carries seqid 1 (RFC 8881 S8.2.2).
 */
static void make_lock_stateid(struct lock_table *lt,
                              uint64_t fileid,
                              struct nfs4_stateid *sid)
{
    uint32_t mds_be = htobe32(lt->mds_id);
    uint64_t seq_be = htobe64(atomic_fetch_add(&lt->next_seq, 1));
    sid->seqid = 0;
    memcpy(sid->other, &mds_be, 4);
    sid->other[4] = (uint8_t)stripe(fileid);
    memcpy(sid->other + 5, ((const uint8_t *)&seq_be) + 1, 7);
}

static bool owner_match(const struct lock_owner_state *os,
                        uint64_t clientid,
                        const uint8_t *owner, uint32_t owner_len)
{
    return os->clientid == clientid &&
           os->owner_len == owner_len &&
           memcmp(os->owner, owner, owner_len) == 0;
}

/** Exclusive end of a range; UINT64_MAX length means to-EOF. */
static uint64_t range_end(uint64_t offset, uint64_t length)
{
    if (length == UINT64_MAX || offset + length < offset) {
        return UINT64_MAX;
    }
    return offset + length;
}

/** Check if two byte ranges overlap. */
static bool ranges_overlap(uint64_t o1, uint64_t l1,
                           uint64_t o2, uint64_t l2)
{
    return o1 < range_end(o2, l2) && o2 < range_end(o1, l1);
}

/** Find or create the file_locks bucket.  Caller must hold stripe lock. */
static struct file_locks *find_or_create_file(struct lock_stripe *st,
                                              uint64_t fileid)
{
    uint32_t idx = fhash(fileid);
    struct file_locks *fl;

    for (fl = st->file_hash[idx]; fl != NULL; fl = fl->hash_next) {
        if (fl->fileid == fileid) {
            return fl;
        }
    }
    fl = calloc(1, sizeof(*fl));
    if (fl == NULL) { return NULL; }
    fl->fileid = fileid;
    fl->hash_next = st->file_hash[idx];
    st->file_hash[idx] = fl;
    return fl;
}

/** Find the file_locks bucket without creating it. */
static struct file_locks *find_file(const struct lock_stripe *st,
                                    uint64_t fileid)
{
    uint32_t idx = fhash(fileid);
    struct file_locks *fl;

    for (fl = st->file_hash[idx]; fl != NULL; fl = fl->hash_next) {
        if (fl->fileid == fileid) {
            return fl;
        }
    }
    return NULL;
}

/** Find an owner state by stateid.  Caller must hold the stripe lock. */
static struct lock_owner_state *find_by_sid(
    const struct lock_stripe *st,
    const uint8_t other[NFS4_OTHER_SIZE])
{
    uint32_t idx = shash(other);
    struct lock_owner_state *os;

    for (os = st->sid_hash[idx]; os != NULL; os = os->sid_next) {
        if (memcmp(os->stateid.other, other, NFS4_OTHER_SIZE) == 0) {
            return os;
        }
    }
    return NULL;
}

/** Find an owner state by identity within a file.  Stripe lock held. */
static struct lock_owner_state *find_owner(const struct file_locks *fl,
                                           uint64_t clientid,
                                           const uint8_t *owner,
                                           uint32_t owner_len)
{
    struct lock_owner_state *os;

    for (os = fl->head; os != NULL; os = os->file_next) {
        if (owner_match(os, clientid, owner, owner_len)) {
            return os;
        }
    }
    return NULL;
}

/**
 * Scan for a conflicting range held by a DIFFERENT owner.
 *
 * @param self  Owner state to skip (the requester), or NULL when the
 *              requester has no state yet (new owner / LOCKT) -- in
 *              that case (clientid, owner) identifies the requester.
 * @return The conflicting owner state (with @p rout set to the
 *         conflicting range) or NULL when the range is free.
 */
static const struct lock_owner_state *scan_conflict(
    const struct file_locks *fl,
    const struct lock_owner_state *self,
    uint64_t clientid, const uint8_t *owner, uint32_t owner_len,
    uint32_t eff_type, uint64_t offset, uint64_t length,
    const struct lock_range **rout)
{
    const struct lock_owner_state *os;
    const struct lock_range *r;

    for (os = fl->head; os != NULL; os = os->file_next) {
        if (os == self ||
            owner_match(os, clientid, owner, owner_len)) {
            continue; /* Same owner: replace/upgrade, no conflict. */
        }
        for (r = os->ranges; r != NULL; r = r->next) {
            if (!ranges_overlap(r->offset, r->length,
                                offset, length)) {
                continue;
            }
            /* Read vs Read: no conflict. */
            if (r->lock_type == READ_LT && eff_type == READ_LT) {
                continue;
            }
            if (rout != NULL) { *rout = r; }
            return os;
        }
    }
    return NULL;
}

/* -----------------------------------------------------------------------
 * Persistence helpers (shared byte_locks table)
 *
 * One row per in-memory range, keyed by (fileid, lock_id) with a
 * unique lock_id per row.  All writes are best-effort: the in-memory
 * tables are authoritative for conflict decisions on this MDS; the
 * rows exist for failover reaping and cross-MDS visibility.
 * ----------------------------------------------------------------------- */

static void persist_range_put(struct lock_table *lt,
                              const struct lock_owner_state *os,
                              const struct lock_range *r,
                              const struct nfs4_stateid *open_stateid)
{
    struct mds_coord_lock_row row;

    if (lt->cat == NULL) { return; }

    memset(&row, 0, sizeof(row));
    row.fileid = os->fileid;
    row.lock_id = r->lock_id;
    row.offset = r->offset;
    row.length = r->length;
    row.lock_type = r->lock_type;
    row.clientid = os->clientid;
    memcpy(row.owner, os->owner, os->owner_len);
    row.owner_len = os->owner_len;
    memcpy(row.stateid_other, os->stateid.other, 12);
    row.seqid = os->stateid.seqid;
    if (open_stateid != NULL) {
        memcpy(row.open_stateid_other, open_stateid->other, 12);
    }
    row.owner_mds_id = lt->mds_id;
    row.owner_boot_epoch = lt->boot_epoch;
    (void)mds_coord_lock_put(lt->cat, &row);
}

static void persist_range_del(struct lock_table *lt,
                              uint64_t fileid, uint64_t lock_id)
{
    if (lt->cat == NULL) { return; }
    (void)mds_coord_lock_del(lt->cat, fileid, lock_id);
}

/* -----------------------------------------------------------------------
 * Range arithmetic
 * ----------------------------------------------------------------------- */

/**
 * Carve [offset, end) out of @p os's range list (POSIX overwrite /
 * unlock semantics for a single owner):
 *
 *   - ranges fully inside the carve region are removed,
 *   - ranges sticking out on one side are trimmed,
 *   - a range sticking out on BOTH sides is split in two, consuming
 *     the caller-provided @p spare node (at most one split can occur
 *     per carve, because ranges of one owner never overlap).
 *
 * Persistence mirrors every step: removed ranges delete their row,
 * trimmed ranges upsert theirs, the split remainder inserts a new
 * row under a fresh lock_id.
 *
 * @return true when @p spare was consumed.
 */
static bool carve_range(struct lock_table *lt,
                        struct lock_owner_state *os,
                        uint64_t offset, uint64_t end,
                        struct lock_range *spare)
{
    struct lock_range **pp = &os->ranges;
    bool spare_used = false;

    while (*pp != NULL) {
        struct lock_range *r = *pp;
        uint64_t rend = range_end(r->offset, r->length);

        if (rend <= offset || r->offset >= end) {
            pp = &r->next; /* No overlap. */
            continue;
        }
        if (r->offset < offset && rend > end) {
            /* Strictly covers the carve region: split. */
            spare->offset = end;
            spare->length = (rend == UINT64_MAX)
                            ? UINT64_MAX : (rend - end);
            spare->lock_type = r->lock_type;
            spare->lock_id = atomic_fetch_add(&lt->next_seq, 1);
            spare->next = r->next;

            r->length = offset - r->offset;
            r->next = spare;

            persist_range_put(lt, os, r, NULL);
            persist_range_put(lt, os, spare, NULL);
            spare_used = true;
            break; /* Nothing else can overlap. */
        }
        if (r->offset >= offset && rend <= end) {
            /* Fully covered: remove. */
            *pp = r->next;
            persist_range_del(lt, os->fileid, r->lock_id);
            free(r);
            continue;
        }
        if (r->offset < offset) {
            /* Overlaps on the left: keep the head. */
            r->length = offset - r->offset;
            persist_range_put(lt, os, r, NULL);
            pp = &r->next;
            continue;
        }
        /* Overlaps on the right: keep the tail. */
        r->length = (rend == UINT64_MAX)
                    ? UINT64_MAX : (rend - end);
        r->offset = end;
        persist_range_put(lt, os, r, NULL);
        pp = &r->next;
    }
    return spare_used;
}

/* ----------------------------------------------------------------------- */

int lock_table_init(uint32_t mds_id, struct lock_table **out)
{
    struct lock_table *lt;

    if (out == NULL) { return -1; }
    *out = NULL;

    lt = calloc(1, sizeof(*lt));
    if (lt == NULL) { return -1; }

    lt->mds_id = mds_id;
    /* See compound_layout.c:seed_layout_sid_counter for the
     * rationale: starting at 1 collides with any persisted lock
     * stateids from a previous daemon boot.  Seed from wall-clock
     * nanoseconds so the next counter value is always strictly
     * larger than any value a previous boot could have reached. */
    {
        struct timespec seed_ts;
        uint64_t seed;
        if (clock_gettime(CLOCK_REALTIME, &seed_ts) != 0) {
            (void)clock_gettime(CLOCK_MONOTONIC, &seed_ts);
        }
        seed = (uint64_t)seed_ts.tv_sec * 1000000000ULL +
               (uint64_t)seed_ts.tv_nsec;
        if (seed == 0) { seed = 1; }
        lt->next_seq = seed;
    }
    for (int i = 0; i < LOCK_STRIPES; i++) {
        pthread_mutex_init(&lt->stripes[i].lock, NULL);
    }

    *out = lt;
    return 0;
}

/** Free one owner state and all its ranges (no persistence). */
static void owner_state_free(struct lock_owner_state *os)
{
    struct lock_range *r = os->ranges;

    while (r != NULL) {
        struct lock_range *rn = r->next;
        free(r);
        r = rn;
    }
    free(os);
}

void lock_table_destroy(struct lock_table *lt)
{
    if (lt == NULL) { return; }
    for (uint32_t s = 0; s < LOCK_STRIPES; s++) {
        struct lock_stripe *st = &lt->stripes[s];
        for (uint32_t b = 0; b < FILE_HASH_PER_STRIPE; b++) {
            struct file_locks *fl = st->file_hash[b];
            while (fl != NULL) {
                struct file_locks *fn = fl->hash_next;
                struct lock_owner_state *os = fl->head;
                while (os != NULL) {
                    struct lock_owner_state *on = os->file_next;
                    owner_state_free(os);
                    os = on;
                }
                free(fl);
                fl = fn;
            }
        }
        pthread_mutex_destroy(&st->lock);
    }
    free(lt);
}

void lock_table_set_cat(struct lock_table *lt,
                        struct mds_catalogue *cat,
                        uint64_t boot_epoch)
{
    if (lt != NULL) {
        lt->cat = cat;
        lt->boot_epoch = boot_epoch;
    }
}

/**
 * Validate a presented stateid seqid against the current one.
 * seqid 0 means "current" (RFC 8881 S8.2.2 special value).
 */
static int seqid_check(const struct lock_owner_state *os,
                       uint32_t presented_seqid)
{
    if (presented_seqid == 0 ||
        presented_seqid == os->stateid.seqid) {
        return 0;
    }
    if (presented_seqid < os->stateid.seqid) {
        return NFS4ERR_OLD_STATEID;
    }
    return NFS4ERR_BAD_STATEID;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int lock_acquire(struct lock_table *lt,
                 uint64_t fileid, uint32_t lock_type,
                 uint64_t offset, uint64_t length,
                 uint64_t clientid,
                 const uint8_t *owner, uint32_t owner_len,
                 const struct nfs4_stateid *open_stateid,
                 bool new_lock_owner,
                 struct nfs4_stateid *lock_stateid,
                 struct lock_conflict *conflict)
{
    uint32_t s, eff_type;
    struct file_locks *fl;
    struct lock_owner_state *os;
    struct lock_stripe *st;
    struct lock_range *nr, *spare;
    const struct lock_owner_state *cos;
    const struct lock_range *cr = NULL;

    if (lt == NULL || lock_stateid == NULL) {
        return NFS4ERR_INVAL;
    }

    /* Normalize blocking types. */
    eff_type = (lock_type == READW_LT) ? READ_LT :
               (lock_type == WRITEW_LT) ? WRITE_LT : lock_type;

    /* RFC 8881: length=0 means "lock to end of file" = UINT64_MAX. */
    if (length == 0) { length = UINT64_MAX; }
    if (eff_type != READ_LT && eff_type != WRITE_LT) {
        return NFS4ERR_INVAL;
    }
    if (owner_len > LOCK_OWNER_MAX) { owner_len = LOCK_OWNER_MAX; }

    MDS_PHASE_SCOPE(MDS_PHASE_STATE);

    /* Allocate outside the surgery: the new range plus the split
     * spare carve_range may need.  Freed below when unused. */
    nr = calloc(1, sizeof(*nr));
    spare = calloc(1, sizeof(*spare));
    if (nr == NULL || spare == NULL) {
        free(nr);
        free(spare);
        return -1;
    }

    s = stripe(fileid);
    st = &lt->stripes[s];
    pthread_mutex_lock(&st->lock);

    fl = find_or_create_file(st, fileid);
    if (fl == NULL) {
        pthread_mutex_unlock(&st->lock);
        free(nr);
        free(spare);
        return -1;
    }

    /*
     * Resolve the requesting owner state.
     *
     * new_lock_owner: identified by (clientid, owner) from the wire;
     * reuse an existing state (retransmit / owner reuse after the
     * client discarded its handle) or create one below.
     *
     * existing owner: the wire carries ONLY the lock stateid (the
     * owner bytes are not on the wire for this arm), so resolve by
     * stateid and validate it names this file.
     */
    if (new_lock_owner) {
        os = find_owner(fl, clientid, owner, owner_len);
    } else {
        os = find_by_sid(st, lock_stateid->other);
        if (os == NULL || os->fileid != fileid) {
            pthread_mutex_unlock(&st->lock);
            free(nr);
            free(spare);
            return NFS4ERR_BAD_STATEID;
        }
        {
            int sq = seqid_check(os, lock_stateid->seqid);
            if (sq != 0) {
                pthread_mutex_unlock(&st->lock);
                free(nr);
                free(spare);
                return sq;
            }
        }
    }

    /* Conflict check against every OTHER owner. */
    cos = scan_conflict(fl, os,
                        (os != NULL) ? os->clientid : clientid,
                        (os != NULL) ? os->owner : owner,
                        (os != NULL) ? os->owner_len : owner_len,
                        eff_type, offset, length, &cr);
    if (cos != NULL) {
        if (conflict != NULL) {
            conflict->offset = cr->offset;
            conflict->length = cr->length;
            conflict->lock_type = cr->lock_type;
            conflict->clientid = cos->clientid;
            conflict->owner_len = cos->owner_len;
            memcpy(conflict->owner, cos->owner, cos->owner_len);
        }
        pthread_mutex_unlock(&st->lock);
        free(nr);
        free(spare);
        return NFS4ERR_DENIED;
    }

    if (os == NULL) {
        /* First LOCK by this owner on this file. */
        os = calloc(1, sizeof(*os));
        if (os == NULL) {
            pthread_mutex_unlock(&st->lock);
            free(nr);
            free(spare);
            return -1;
        }
        os->fileid = fileid;
        os->clientid = clientid;
        os->owner_len = owner_len;
        if (owner_len > 0) {
            memcpy(os->owner, owner, owner_len);
        }
        make_lock_stateid(lt, fileid, &os->stateid);

        os->file_next = fl->head;
        fl->head = os;
        {
            uint32_t si = shash(os->stateid.other);
            os->sid_next = st->sid_hash[si];
            st->sid_hash[si] = os;
        }
    }

    /* Same-owner overlap is replaced (upgrade/downgrade + split). */
    if (carve_range(lt, os, offset, range_end(offset, length), spare)) {
        spare = NULL; /* Consumed by a split. */
    }

    nr->offset = offset;
    nr->length = length;
    nr->lock_type = eff_type;
    nr->lock_id = atomic_fetch_add(&lt->next_seq, 1);
    nr->next = os->ranges;
    os->ranges = nr;

    /* RFC 8881 S8.2.2: bump the stateid on every successful LOCK. */
    os->stateid.seqid++;
    *lock_stateid = os->stateid;

    persist_range_put(lt, os, nr, open_stateid);

    pthread_mutex_unlock(&st->lock);
    free(spare); /* NULL when consumed. */
    return 0;
}

int lock_test(struct lock_table *lt,
              uint64_t fileid, uint32_t lock_type,
              uint64_t offset, uint64_t length,
              uint64_t clientid,
              const uint8_t *owner, uint32_t owner_len,
              struct lock_conflict *conflict)
{
    uint32_t s, eff_type;
    const struct file_locks *fl;
    struct lock_stripe *st;
    const struct lock_owner_state *cos;
    const struct lock_range *cr = NULL;

    if (lt == NULL) { return NFS4ERR_INVAL; }

    eff_type = (lock_type == READW_LT) ? READ_LT :
               (lock_type == WRITEW_LT) ? WRITE_LT : lock_type;

    /* RFC 8881: length=0 means "lock to end of file". */
    if (length == 0) { length = UINT64_MAX; }
    if (owner_len > LOCK_OWNER_MAX) { owner_len = LOCK_OWNER_MAX; }

    MDS_PHASE_SCOPE(MDS_PHASE_STATE);

    s = stripe(fileid);
    st = &lt->stripes[s];
    pthread_mutex_lock(&st->lock);

    fl = find_file(st, fileid);
    if (fl == NULL) {
        pthread_mutex_unlock(&st->lock);
        return 0; /* No locks on file */
    }

    cos = scan_conflict(fl, NULL, clientid, owner, owner_len,
                        eff_type, offset, length, &cr);
    if (cos != NULL) {
        if (conflict != NULL) {
            conflict->offset = cr->offset;
            conflict->length = cr->length;
            conflict->lock_type = cr->lock_type;
            conflict->clientid = cos->clientid;
            conflict->owner_len = cos->owner_len;
            memcpy(conflict->owner, cos->owner, cos->owner_len);
        }
        pthread_mutex_unlock(&st->lock);
        return NFS4ERR_DENIED;
    }

    pthread_mutex_unlock(&st->lock);
    return 0;
}

/**
 * Release a byte range (LOCKU).
 *
 * Removes ONLY the given range from the owner state named by the
 * stateid, splitting held ranges when the unlock punches a hole.
 * The owner state itself survives (rangeless states answer
 * TEST_STATEID and are reclaimed by FREE_STATEID), and the stateid
 * seqid increments so the client can chain further LOCK/LOCKU ops.
 *
 * The stripe is extracted from the stateid itself (byte other[4]),
 * so we lock the correct stripe BEFORE searching.
 */
int lock_release(struct lock_table *lt,
                 struct nfs4_stateid *lock_stateid,
                 uint32_t lock_type,
                 uint64_t offset, uint64_t length)
{
    struct lock_owner_state *os;
    uint32_t s;
    struct lock_stripe *st;
    struct lock_range *spare;

    (void)lock_type; /* RFC 8881 S18.12: type is ignored on unlock. */

    if (lt == NULL || lock_stateid == NULL) {
        return NFS4ERR_INVAL;
    }
    if (length == 0) { length = UINT64_MAX; }

    MDS_PHASE_SCOPE(MDS_PHASE_STATE);

    spare = calloc(1, sizeof(*spare));
    if (spare == NULL) { return -1; }

    /* Derive stripe from the stateid -- no unprotected dereference. */
    s = stateid_stripe(lock_stateid->other);
    st = &lt->stripes[s];
    pthread_mutex_lock(&st->lock);

    os = find_by_sid(st, lock_stateid->other);
    if (os == NULL) {
        pthread_mutex_unlock(&st->lock);
        free(spare);
        return NFS4ERR_BAD_STATEID;
    }
    {
        int sq = seqid_check(os, lock_stateid->seqid);
        if (sq != 0) {
            pthread_mutex_unlock(&st->lock);
            free(spare);
            return sq;
        }
    }

    if (carve_range(lt, os, offset, range_end(offset, length), spare)) {
        spare = NULL;
    }

    /* RFC 8881 S8.2.2: bump the stateid on every successful LOCKU. */
    os->stateid.seqid++;
    lock_stateid->seqid = os->stateid.seqid;
    memcpy(lock_stateid->other, os->stateid.other, NFS4_OTHER_SIZE);

    pthread_mutex_unlock(&st->lock);
    free(spare);
    return 0;
}

/** Remove an owner state from the per-stripe sid hash.  Lock held. */
static void sid_hash_unlink(struct lock_stripe *st,
                            const struct lock_owner_state *os)
{
    uint32_t si = shash(os->stateid.other);
    struct lock_owner_state **sp = &st->sid_hash[si];

    while (*sp != NULL) {
        if (*sp == os) {
            *sp = os->sid_next;
            break;
        }
        sp = &(*sp)->sid_next;
    }
}

/** Unlink an owner state from both per-stripe tables.  Lock held. */
static void owner_state_unlink(struct lock_stripe *st,
                               struct lock_owner_state *os)
{
    uint32_t fi = fhash(os->fileid);
    struct file_locks *fl;

    for (fl = st->file_hash[fi]; fl != NULL; fl = fl->hash_next) {
        if (fl->fileid != os->fileid) { continue; }
        struct lock_owner_state **pp = &fl->head;
        while (*pp != NULL) {
            if (*pp == os) {
                *pp = os->file_next;
                break;
            }
            pp = &(*pp)->file_next;
        }
        break;
    }
    sid_hash_unlink(st, os);
}

int lock_free_stateid(struct lock_table *lt,
                      const uint8_t other[NFS4_OTHER_SIZE])
{
    struct lock_owner_state *os;
    uint32_t s;
    struct lock_stripe *st;

    if (lt == NULL || other == NULL) {
        return NFS4ERR_BAD_STATEID;
    }

    MDS_PHASE_SCOPE(MDS_PHASE_STATE);

    s = stateid_stripe(other);
    st = &lt->stripes[s];
    pthread_mutex_lock(&st->lock);

    os = find_by_sid(st, other);
    if (os == NULL) {
        pthread_mutex_unlock(&st->lock);
        return NFS4ERR_BAD_STATEID;
    }
    if (os->ranges != NULL) {
        /* RFC 8881 S18.38.3: locks still held -> refuse. */
        pthread_mutex_unlock(&st->lock);
        return NFS4ERR_LOCKS_HELD;
    }

    owner_state_unlink(st, os);
    owner_state_free(os);
    pthread_mutex_unlock(&st->lock);
    return 0;
}

/**
 * Release all locks held by a client.
 *
 * Processes one stripe at a time (never holds more than one stripe
 * lock simultaneously), reducing worst-case lock-hold time from
 * O(all_stripes) to O(one_stripe).  Persistence: one reap-by-client
 * pass on the shared table (best-effort).
 */
void lock_release_all_for_client(struct lock_table *lt, uint64_t clientid)
{
    if (lt == NULL) { return; }

    for (uint32_t s = 0; s < LOCK_STRIPES; s++) {
        struct lock_stripe *st = &lt->stripes[s];
        pthread_mutex_lock(&st->lock);

        for (uint32_t b = 0; b < FILE_HASH_PER_STRIPE; b++) {
            struct file_locks *fl;
            for (fl = st->file_hash[b]; fl != NULL;
                 fl = fl->hash_next) {
                struct lock_owner_state **pp = &fl->head;
                while (*pp != NULL) {
                    struct lock_owner_state *os = *pp;
                    if (os->clientid != clientid) {
                        pp = &(*pp)->file_next;
                        continue;
                    }
                    *pp = os->file_next;
                    /* Remove from per-stripe sid hash. */
                    sid_hash_unlink(st, os);
                    owner_state_free(os);
                }
            }
        }

        pthread_mutex_unlock(&st->lock);
    }

    if (lt->cat != NULL) {
        (void)mds_coord_lock_reap_client(lt->cat, clientid);
    }
}

bool lock_state_exists(const struct lock_table *lt,
                       const uint8_t other[NFS4_OTHER_SIZE])
{
    uint32_t s;
    struct lock_stripe *st;
    bool found;

    if (lt == NULL || other == NULL) { return false; }

    s = stateid_stripe(other);
    st = &((struct lock_table *)lt)->stripes[s];
    pthread_mutex_lock(&st->lock);
    found = (find_by_sid(st, other) != NULL);
    pthread_mutex_unlock(&st->lock);
    return found;
}

/*
 * RFC 8881 §8.4.3 courtesy-client support for byte-range locks.
 * Same three-phase pattern as open_state_revoke_expired_for_file:
 *   Phase 1 — collect unique clientids under stripe lock.
 *   Phase 2 — check lease expiry via session table (no stripe lock).
 *   Phase 3 — re-acquire stripe lock and remove expired states.
 * Pynfs COUR2.
 */
#define LOCK_REVOKE_MAX_CLIENTS 64

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int lock_revoke_expired_for_file(struct lock_table *lt,
                                 struct session_table *sst,
                                 uint64_t fileid)
{
    uint64_t cids[LOCK_REVOKE_MAX_CLIENTS];
    bool     exp[LOCK_REVOKE_MAX_CLIENTS];
    uint32_t n_cids = 0;
    uint32_t s_idx, i;
    int revoked = 0;
    bool any_expired = false;
    struct lock_stripe *st;

    if (lt == NULL || sst == NULL) {
        return 0;
    }

    s_idx = stripe(fileid);
    st = &lt->stripes[s_idx];

    /* Phase 1: collect unique clientids. */
    pthread_mutex_lock(&st->lock);
    {
        struct file_locks *fl = find_file(st, fileid);
        const struct lock_owner_state *os;

        for (os = (fl != NULL) ? fl->head : NULL; os != NULL;
             os = os->file_next) {
            bool seen = false;
            for (i = 0; i < n_cids; i++) {
                if (cids[i] == os->clientid) {
                    seen = true;
                    break;
                }
            }
            if (!seen && n_cids < LOCK_REVOKE_MAX_CLIENTS) {
                cids[n_cids++] = os->clientid;
            }
        }
    }
    pthread_mutex_unlock(&st->lock);

    if (n_cids == 0) { return 0; }

    /* Phase 2: check lease expiry (session lock only). */
    for (i = 0; i < n_cids; i++) {
        exp[i] = session_client_lease_expired(sst, cids[i]);
        if (exp[i]) { any_expired = true; }
    }
    if (!any_expired) { return 0; }

    /* Phase 3: remove expired owner states. */
    pthread_mutex_lock(&st->lock);
    {
        struct file_locks *fl = find_file(st, fileid);
        struct lock_owner_state **pp = (fl != NULL) ? &fl->head : NULL;

        while (pp != NULL && *pp != NULL) {
            struct lock_owner_state *os = *pp;
            bool is_expired = false;
            for (i = 0; i < n_cids; i++) {
                if (cids[i] == os->clientid && exp[i]) {
                    is_expired = true;
                    break;
                }
            }
            if (is_expired) {
                *pp = os->file_next;
                sid_hash_unlink(st, os);
                owner_state_free(os);
                revoked++;
            } else {
                pp = &os->file_next;
            }
        }
    }
    pthread_mutex_unlock(&st->lock);

    /* Best-effort persistence reap for the expired clients. */
    if (revoked > 0 && lt->cat != NULL) {
        for (i = 0; i < n_cids; i++) {
            if (exp[i]) {
                (void)mds_coord_lock_reap_client(lt->cat, cids[i]);
            }
        }
    }
    return revoked;
}

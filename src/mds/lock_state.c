/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * lock_state.c — NFSv4.1 byte-range lock manager.
 *
 * Maintains per-file linked lists of lock_entry, hashed by fileid.
 * Conflict detection follows RFC 8881 §18.10.4: read locks conflict
 * with write locks from different owners; write locks conflict with
 * everything from different owners.
 *
 * Thread safety: 16 stripe locks partition all mutable state.
 * Each stripe owns its own file_hash and sid_hash tables, so
 * concurrent operations on different stripes never share linked
 * lists.  The stripe index is encoded in byte 4 of the lock
 * stateid so that lock_release/lock_state_exists can determine
 * the correct stripe without an unprotected hash lookup.
 */

#include <stdlib.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
#include <endian.h>
#include <time.h>

#include "pnfs_mds.h"
#include "lock_state.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"

/* ----------------------------------------------------------------------- */

/** Per-stripe hash table sizing.  16 stripes × 32 = 512 effective buckets. */
#define FILE_HASH_PER_STRIPE  32
#define SID_HASH_PER_STRIPE   32
#define LOCK_STRIPES          16

struct lock_entry {
    uint64_t            fileid;
    uint64_t            offset;
    uint64_t            length;
    uint32_t            lock_type; /* READ_LT or WRITE_LT */
    uint64_t            clientid;
    uint8_t             owner[LOCK_OWNER_MAX];
    uint32_t            owner_len;
    struct nfs4_stateid stateid;   /* Lock stateid */
    struct lock_entry  *file_next; /* Per-file chain */
    struct lock_entry  *sid_next;  /* Stateid hash chain */
};

struct file_locks {
    uint64_t            fileid;
    struct lock_entry  *head;
    struct file_locks  *hash_next;
};

/**
 * Per-stripe partition.  All fields are accessed only while the
 * stripe's mutex is held, eliminating cross-stripe data races.
 */
struct lock_stripe {
    struct file_locks  *file_hash[FILE_HASH_PER_STRIPE];
    struct lock_entry  *sid_hash[SID_HASH_PER_STRIPE];
    pthread_mutex_t     lock;
};

struct lock_table {
    struct lock_stripe  stripes[LOCK_STRIPES];
    _Atomic uint64_t    next_seq;  /* Atomic: accessed from any stripe. */
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
 * Generate a unique lock stateid that embeds the stripe index.
 *
 * Layout of stateid.other[12]:
 *   [0..3] : mds_id  (big-endian)
 *   [4]    : stripe index  (0–15)
 *   [5..11]: sequence counter (big-endian, high 7 bytes)
 *
 * Caller must hold the stripe lock (serialises next_seq access).
 */
static void make_lock_stateid(struct lock_table *lt,
                              uint64_t fileid,
                              struct nfs4_stateid *sid)
{
    uint32_t mds_be = htobe32(lt->mds_id);
    uint64_t seq_be = htobe64(atomic_fetch_add(&lt->next_seq, 1));
    sid->seqid = 1;
    memcpy(sid->other, &mds_be, 4);
    sid->other[4] = (uint8_t)stripe(fileid);
    memcpy(sid->other + 5, ((const uint8_t *)&seq_be) + 1, 7);
}

static bool same_owner(const struct lock_entry *e,
                       uint64_t clientid,
                       const uint8_t *owner, uint32_t owner_len)
{
    return e->clientid == clientid &&
           e->owner_len == owner_len &&
           memcmp(e->owner, owner, owner_len) == 0;
}

/** Check if two byte ranges overlap. */
static bool ranges_overlap(uint64_t o1, uint64_t l1,
                           uint64_t o2, uint64_t l2)
{
    uint64_t end1 = (l1 == UINT64_MAX) ? UINT64_MAX : o1 + l1;
    uint64_t end2 = (l2 == UINT64_MAX) ? UINT64_MAX : o2 + l2;
    return o1 < end2 && o2 < end1;
}

/** Check if lock_entry e conflicts with a proposed lock. */
static bool conflicts(const struct lock_entry *e,
                      uint32_t lock_type,
                      uint64_t offset, uint64_t length,
                      uint64_t clientid,
                      const uint8_t *owner, uint32_t owner_len)
{
    if (!ranges_overlap(e->offset, e->length, offset, length)) {
        return false;
    }
    if (same_owner(e, clientid, owner, owner_len)) {
        return false; /* Same owner: upgrade, no conflict */
    }
    /* Read vs Read: no conflict. */
    if (e->lock_type == READ_LT && lock_type == READ_LT) {
        return false;
    }
    return true;
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

/** Find lock entry by stateid.  Caller must hold the stripe lock. */
static struct lock_entry *find_by_sid(const struct lock_stripe *st,
                                      const uint8_t other[NFS4_OTHER_SIZE])
{
    uint32_t idx = shash(other);
    struct lock_entry *e;

    for (e = st->sid_hash[idx]; e != NULL; e = e->sid_next) {
        if (memcmp(e->stateid.other, other, NFS4_OTHER_SIZE) == 0) {
            return e;
        }
    }
    return NULL;
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

void lock_table_destroy(struct lock_table *lt)
{
    if (lt == NULL) { return; }
    for (uint32_t s = 0; s < LOCK_STRIPES; s++) {
        struct lock_stripe *st = &lt->stripes[s];
        for (uint32_t b = 0; b < FILE_HASH_PER_STRIPE; b++) {
            struct file_locks *fl = st->file_hash[b];
            while (fl != NULL) {
                struct file_locks *fn = fl->hash_next;
                struct lock_entry *e = fl->head;
                while (e != NULL) {
                    struct lock_entry *en = e->file_next;
                    free(e);
                    e = en;
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

int lock_acquire(struct lock_table *lt,
                 uint64_t fileid, uint32_t lock_type,
                 uint64_t offset, uint64_t length,
                 uint64_t clientid,
                 const uint8_t *owner, uint32_t owner_len,
                 const struct nfs4_stateid *open_stateid,
                 struct nfs4_stateid *lock_stateid,
                 struct lock_conflict *conflict)
{
    uint32_t s, eff_type;
    struct file_locks *fl;
    struct lock_entry *e, *new_entry;
    struct lock_stripe *st;

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

    (void)open_stateid; /* Validated at compound level */

    s = stripe(fileid);
    st = &lt->stripes[s];
    pthread_mutex_lock(&st->lock);

    fl = find_or_create_file(st, fileid);
    if (fl == NULL) {
        pthread_mutex_unlock(&st->lock);
        return -1;
    }

    /* Check for conflicts. */
    for (e = fl->head; e != NULL; e = e->file_next) {
        if (conflicts(e, eff_type, offset, length,
                      clientid, owner, owner_len)) {
            if (conflict != NULL) {
                conflict->offset = e->offset;
                conflict->length = e->length;
                conflict->lock_type = e->lock_type;
                conflict->clientid = e->clientid;
                conflict->owner_len = e->owner_len;
                memcpy(conflict->owner, e->owner, e->owner_len);
            }
            pthread_mutex_unlock(&st->lock);
            return NFS4ERR_DENIED;
        }
    }

    /* No conflict — create lock entry. */
    new_entry = calloc(1, sizeof(*new_entry));
    if (new_entry == NULL) {
        pthread_mutex_unlock(&st->lock);
        return -1;
    }

    new_entry->fileid = fileid;
    new_entry->offset = offset;
    new_entry->length = length;
    new_entry->lock_type = eff_type;
    new_entry->clientid = clientid;
    new_entry->owner_len = (owner_len <= LOCK_OWNER_MAX)
                           ? owner_len : LOCK_OWNER_MAX;
    if (owner_len > 0) {
        memcpy(new_entry->owner, owner, new_entry->owner_len);
    }

    make_lock_stateid(lt, fileid, &new_entry->stateid);

    /* Insert into file chain. */
    new_entry->file_next = fl->head;
    fl->head = new_entry;

    /* Insert into per-stripe stateid hash. */
    {
        uint32_t si = shash(new_entry->stateid.other);
        new_entry->sid_next = st->sid_hash[si];
        st->sid_hash[si] = new_entry;
    }

    *lock_stateid = new_entry->stateid;

    /* Persist to RonDB (shared-attr write-through). */
    if (lt->cat != NULL) {
        struct mds_coord_lock_row row;
        memset(&row, 0, sizeof(row));
        row.fileid = fileid;
        row.lock_id = atomic_load(&lt->next_seq); /* unique per lock */
        row.offset = offset;
        row.length = length;
        row.lock_type = eff_type;
        row.clientid = clientid;
        if (owner_len > 0) {
            memcpy(row.owner, owner, new_entry->owner_len);
        }
        row.owner_len = new_entry->owner_len;
        memcpy(row.stateid_other, new_entry->stateid.other, 12);
        row.seqid = new_entry->stateid.seqid;
        if (open_stateid != NULL) {
            memcpy(row.open_stateid_other, open_stateid->other, 12);
        }
        row.owner_mds_id = lt->mds_id;
        row.owner_boot_epoch = lt->boot_epoch;
        (void)mds_coord_lock_put(lt->cat, &row);
    }

    pthread_mutex_unlock(&st->lock);
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
    struct file_locks *fl;
    struct lock_entry *e;
    struct lock_stripe *st;

    if (lt == NULL) { return NFS4ERR_INVAL; }

    eff_type = (lock_type == READW_LT) ? READ_LT :
               (lock_type == WRITEW_LT) ? WRITE_LT : lock_type;

    /* RFC 8881: length=0 means "lock to end of file". */
    if (length == 0) { length = UINT64_MAX; }

    s = stripe(fileid);
    st = &lt->stripes[s];
    pthread_mutex_lock(&st->lock);

    fl = find_or_create_file(st, fileid);
    if (fl == NULL) {
        pthread_mutex_unlock(&st->lock);
        return 0; /* No locks on file */
    }

    for (e = fl->head; e != NULL; e = e->file_next) {
        if (conflicts(e, eff_type, offset, length,
                      clientid, owner, owner_len)) {
            if (conflict != NULL) {
                conflict->offset = e->offset;
                conflict->length = e->length;
                conflict->lock_type = e->lock_type;
                conflict->clientid = e->clientid;
                conflict->owner_len = e->owner_len;
                memcpy(conflict->owner, e->owner, e->owner_len);
            }
            pthread_mutex_unlock(&st->lock);
            return NFS4ERR_DENIED;
        }
    }

    pthread_mutex_unlock(&st->lock);
    return 0;
}

/**
 * Release a byte-range lock.
 *
 * The stripe is extracted from the stateid itself (byte other[4]),
 * so we lock the correct stripe BEFORE searching — eliminating the
 * previous use-after-free race where find_by_sid ran unlocked.
 */
int lock_release(struct lock_table *lt,
                 struct nfs4_stateid *lock_stateid,
                 uint32_t lock_type,
                 uint64_t offset, uint64_t length)
{
    struct lock_entry *e;
    uint32_t s;
    struct lock_stripe *st;

    (void)lock_type;
    (void)offset;
    (void)length;

    if (lt == NULL || lock_stateid == NULL) {
        return NFS4ERR_INVAL;
    }

    /* Derive stripe from the stateid — no unprotected dereference. */
    s = stateid_stripe(lock_stateid->other);
    st = &lt->stripes[s];
    pthread_mutex_lock(&st->lock);

    e = find_by_sid(st, lock_stateid->other);
    if (e == NULL) {
        pthread_mutex_unlock(&st->lock);
        return NFS4ERR_BAD_STATEID;
    }

    /* Remove from file chain. */
    {
        uint32_t fi = fhash(e->fileid);
        struct file_locks *fl;
        for (fl = st->file_hash[fi]; fl != NULL; fl = fl->hash_next) {
            if (fl->fileid == e->fileid) {
                struct lock_entry **pp = &fl->head;
                while (*pp != NULL) {
                    if (*pp == e) {
                        *pp = e->file_next;
                        break;
                    }
                    pp = &(*pp)->file_next;
                }
                break;
            }
        }
    }

    /* Remove from per-stripe stateid hash. */
    {
        uint32_t si = shash(e->stateid.other);
        struct lock_entry **pp = &st->sid_hash[si];
        while (*pp != NULL) {
            if (*pp == e) {
                *pp = e->sid_next;
                break;
            }
            pp = &(*pp)->sid_next;
        }
    }

    /* Return updated stateid with incremented seqid. */
    lock_stateid->seqid = e->stateid.seqid + 1;
    memcpy(lock_stateid->other, e->stateid.other, NFS4_OTHER_SIZE);

    free(e);
    pthread_mutex_unlock(&st->lock);
    return 0;
}

/**
 * Release all locks held by a client.
 *
 * Processes one stripe at a time (never holds more than one stripe
 * lock simultaneously), reducing worst-case lock-hold time from
 * O(all_stripes) to O(one_stripe).
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
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
                struct lock_entry **pp = &fl->head;
                while (*pp != NULL) {
                    struct lock_entry *e = *pp;
                    if (e->clientid == clientid) {
                        *pp = e->file_next;
                        /* Remove from per-stripe sid hash. */
                        uint32_t si = shash(e->stateid.other);
                        struct lock_entry **sp =
                            &st->sid_hash[si];
                        while (*sp != NULL) {
                            if (*sp == e) {
                                *sp = e->sid_next;
                                break;
                            }
                            sp = &(*sp)->sid_next;
                        }
                        free(e);
                    } else {
                        pp = &(*pp)->file_next;
                    }
                }
            }
        }

        pthread_mutex_unlock(&st->lock);
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

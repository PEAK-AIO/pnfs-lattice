/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * daos_txn.h -- DAOS backend handle and transaction runner.
 *
 * Every catalogue slot of the DAOS backend is ONE call to
 * daos_run_txn(): the runner opens a transaction of the requested kind,
 * runs the caller's body (reads, decisions, blind writes), commits when
 * asked, and turns every DAOS error into exactly one of the catalogue's
 * statuses.  A body never calls back into the caller (contract C2,
 * catalogue_internal.h): it fills a bounded, caller-owned page and the
 * slot delivers that page after daos_run_txn() returned.
 *
 * Transaction kinds:
 *   DAOS_TXN_NONE      no transaction handle (DAOS_TX_NONE): the body
 *                      issues independent single operations, one RPC
 *                      each (GETATTR, probe).  No conflict detection.
 *   DAOS_TXN_READONLY  daos_tx_open(DAOS_TF_RDONLY): every read of the
 *                      body sees one epoch; never committed.
 *   DAOS_TXN_MUTATING  daos_tx_open -> body -> daos_tx_commit.
 *
 * DAOS transaction semantics the runner and every body rely on
 * (daos_api.h, release 2.6):
 *   - serializable MVCC; a conflict surfaces as -DER_TX_RESTART from
 *     daos_tx_commit (or from an operation); the runner calls
 *     daos_tx_restart() and re-runs the body on the same handle;
 *   - NO read-your-writes: a body reads first, decides, then issues
 *     blind updates and punches; it never reads back what it wrote;
 *   - updates and punches are buffered client-side and shipped in one
 *     compound RPC at commit, so a mutating slot costs its read waves
 *     plus one commit round trip;
 *   - conditional flags (DAOS_COND_DKEY_INSERT, DAOS_COND_AKEY_INSERT via
 *     DAOS_COND_PER_AKEY, DAOS_COND_PUNCH, ...) fail with -DER_EXIST /
 *     -DER_NONEXIST either from the operation call or from the commit;
 *     both are mapped to MDS_ERR_EXISTS / MDS_ERR_NOTFOUND;
 *   - one transaction is used by one thread, and every operation of a
 *     transaction uses ONE event mode: either all NULL events or all
 *     events from the one event queue the transaction borrowed.
 *
 * Event queues.  The backend owns a bounded pool of event queues
 * (created lazily, destroyed at close).  A transaction borrows one for
 * its whole lifetime (every attempt, every read, write and the commit),
 * which lets a body issue a parallel fetch wave (daos_fetch_start x N,
 * daos_fetch_finish x N) and pay one round-trip latency for N objects.
 * When no queue is free the transaction runs with NULL events and a
 * wave degrades to sequential fetches; the result is identical.
 * DAOS_TXN_NONE bodies always run with NULL events.
 *
 * Error classification (daos_errno.h; APIs return -DER_*):
 *   -DER_TX_RESTART              restart the same transaction, re-run the body
 *   -DER_INPROGRESS, -DER_TX_BUSY, -DER_AGAIN, -DER_OVERLOAD_RETRY,
 *   -DER_TIMEDOUT, -DER_UNREACH, -DER_OOG, -DER_HG, -DER_EXCLUDED,
 *   -DER_GRPVER, -DER_STALE, -DER_NOTLEADER, -DER_TX_UNCERTAIN
 *       from an operation:  abort, close, back off, fresh transaction,
 *                           within the operation deadline (exhausted ->
 *                           MDS_ERR_DELAY)
 *       from daos_tx_commit: MDS_ERR_INDOUBT -- the commit may have
 *                           landed; never re-run, never report success
 *   -DER_NONEXIST -> MDS_ERR_NOTFOUND     -DER_EXIST    -> MDS_ERR_EXISTS
 *   -DER_NOSPACE  -> MDS_ERR_NOSPC        -DER_NO_PERM  -> MDS_ERR_PERM
 *   -DER_NOMEM    -> MDS_ERR_NOMEM
 *   -DER_REC2BIG, -DER_KEY2BIG and anything else -> MDS_ERR_IO
 * Deadline exhausted with nothing in flight -> MDS_ERR_DELAY.
 *
 * There is no commit-outcome witness in this version: the DAOS client
 * resolves its own resends internally and the indeterminate case is
 * expected to be rare; the runner counts it (stats.indoubt) so the lab
 * can decide whether a witness protocol is worth adding.
 */

#ifndef DAOS_TXN_H
#define DAOS_TXN_H

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <daos.h>

#include "pnfs_mds.h"
#include "daos_keys.h"

/** Default operation deadline (mds.conf daos_op_deadline_ms overrides). */
#define DAOS_OP_DEADLINE_MS_DEFAULT 8000U
/** Defaults of the mds.conf keys the constructor fills in when empty / 0. */
#define DAOS_SYS_DEFAULT            "daos_server"
#define DAOS_OCLASS_DEFAULT         "RP_3G1"
#define DAOS_OCLASS_SPREAD_DEFAULT  "RP_3GX"
#define DAOS_DIR_SHARDS_DEFAULT     16U

/** Event-queue pool: max(worker_threads, DAOS_EQ_POOL_MIN) + DAOS_EQ_POOL_EXTRA
 *  queues, capped at DAOS_EQ_POOL_CAP (each queue is a network context). */
#define DAOS_EQ_POOL_MIN   8U
#define DAOS_EQ_POOL_EXTRA 8U
#define DAOS_EQ_POOL_CAP   256U

/** Object handles one attempt may hold through daos_txn_obj(). */
#define DAOS_TXN_OBJ_MAX 8U

/** Keys one daos_punch_akeys / daos_punch_dkeys call may name. */
#define DAOS_PUNCH_MAX 8U

/* -----------------------------------------------------------------------
 * Statistics (relaxed atomics; exposed through the backend_client_stats
 * slot of the daos authority table, catalogue_daos.c)
 * ----------------------------------------------------------------------- */

struct daos_txn_stats {
    _Atomic uint64_t attempts;          /**< Body executions. */
    _Atomic uint64_t commits;           /**< Commits reported committed. */
    _Atomic uint64_t restarts;          /**< -DER_TX_RESTART re-runs. */
    _Atomic uint64_t transient_retries; /**< Fresh transactions after a transient error. */
    _Atomic uint64_t indoubt;           /**< MDS_ERR_INDOUBT returned. */
    _Atomic uint64_t delay_exhausted;   /**< MDS_ERR_DELAY returned. */
    _Atomic uint64_t io_errors;         /**< MDS_ERR_IO (and NOMEM) returned. */
    _Atomic uint64_t waits;             /**< Blocking network waits. */
};

/* -----------------------------------------------------------------------
 * Event-queue pool
 * ----------------------------------------------------------------------- */

struct daos_eq_slot {
    daos_handle_t eqh;
    bool          created;
    bool          busy;
};

struct daos_eq_pool {
    pthread_mutex_t      lock;
    struct daos_eq_slot *slots;   /**< cap entries; created lazily. */
    uint32_t             cap;
    uint32_t             created;
};

/* -----------------------------------------------------------------------
 * Backend handle (struct mds_catalogue.backend_private)
 * ----------------------------------------------------------------------- */

struct daos_backend {
    daos_handle_t     poh;
    daos_handle_t     coh;
    char              pool[128];
    char              cont[128];
    char              sys[16];
    char              oclass_name[32];
    char              oclass_spread_name[32];
    /** Stamped class encodings (DAOS_CAT_ATTR_OCLASS_ID /
     *  DAOS_CAT_ATTR_OCLASS_SPREAD_ID); OC_UNKNOWN until the container
     *  attributes were verified, which makes daos_oid_make() refuse. */
    daos_oclass_id_t  cid_entity;
    daos_oclass_id_t  cid_spread;
    /** Directory counter shard count K (stamped; the configured value
     *  until bootstrap stamps it). */
    uint32_t          dir_shards;
    uint32_t          mds_id;
    uint32_t          op_deadline_ms;
    /** Unique per open(); thread-local id pools are keyed on it. */
    uint64_t          instance_seq;
    /** Registry boot epoch recorded by the cluster slot (0 until then). */
    _Atomic uint64_t  boot_epoch;
    struct daos_txn_stats stats;
    struct daos_eq_pool   eqs;
};

/* -----------------------------------------------------------------------
 * Runner
 * ----------------------------------------------------------------------- */

enum daos_txn_kind {
    DAOS_TXN_NONE     = 0, /**< No transaction handle; single operations. */
    DAOS_TXN_READONLY = 1, /**< DAOS_TF_RDONLY; never committed. */
    DAOS_TXN_MUTATING = 2, /**< Committed; restarted on conflict. */
};

/** Body return values; a negative value is a -DER_* code. */
enum daos_body_rc {
    DAOS_BODY_COMMIT = 0, /**< Commit (mutating) / done (others); return *st_out. */
    DAOS_BODY_DONE   = 1, /**< Do not commit; return *st_out (e.g. EXISTS, NOTFOUND). */
};

/** One object handle an attempt opened through daos_txn_obj(). */
struct daos_txn_obj {
    daos_obj_id_t oid;
    daos_handle_t oh;
    bool          open;
};

/**
 * The transaction a body runs under.  Owned by the runner; a body only
 * reads b / th / eqh / use_events through the helpers below.
 */
struct daos_txn {
    struct daos_backend *b;
    enum daos_txn_kind   kind;
    daos_handle_t        th;         /**< DAOS_TX_NONE for DAOS_TXN_NONE. */
    daos_handle_t        eqh;        /**< Borrowed queue; valid when use_events. */
    bool                 use_events;
    struct daos_txn_obj  objs[DAOS_TXN_OBJ_MAX];
    uint32_t             nobjs;
};

/**
 * Transaction body.  Runs one attempt: reads, decisions, blind writes.
 * Must not call back into the caller, must tolerate being re-run from
 * scratch (a restarted attempt sees none of its earlier writes) and must
 * set *st_out before returning DAOS_BODY_COMMIT or DAOS_BODY_DONE.  A
 * -DER_* code from a helper is returned as is for classification.
 */
typedef int (*daos_txn_body)(struct daos_txn *t, void *ctx, enum mds_status *st_out);

/**
 * Run @p body under the retry protocol described above.
 *
 * @param b     Backend handle.
 * @param kind  Transaction kind.
 * @param op    Operation name for the INDOUBT / error log lines.
 * @param body  Transaction body.
 * @param ctx   Passed to @p body.
 * @return The body's status on success; MDS_ERR_DELAY when the deadline
 *         expired with nothing in flight; MDS_ERR_INDOUBT when a commit
 *         ended with a transport-class error; MDS_ERR_EXISTS /
 *         MDS_ERR_NOTFOUND for a failed conditional operation;
 *         MDS_ERR_NOSPC, MDS_ERR_PERM, MDS_ERR_NOMEM, MDS_ERR_IO as
 *         classified; MDS_ERR_INVAL for a NULL argument.
 */
enum mds_status daos_run_txn(struct daos_backend *b, enum daos_txn_kind kind, const char *op,
                             daos_txn_body body, void *ctx);

/** Map a -DER_* code to the catalogue status the runner would report
 *  for an operation error (never INDOUBT; a transient code maps to
 *  MDS_ERR_DELAY).  For lifecycle calls made outside the runner. */
enum mds_status daos_err_status(int rc);

/** True for the transient (retry) class listed above. */
bool daos_err_is_transient(int rc);

/** d_errstr(rc), never NULL. */
const char *daos_errstr(int rc);

/** Monotonic nanoseconds (CLOCK_MONOTONIC). */
uint64_t daos_txn_now_ns(void);

/* -----------------------------------------------------------------------
 * Event-queue pool (lifecycle code)
 * ----------------------------------------------------------------------- */

/**
 * Size the pool for @p worker_threads (nothing is created yet).
 *
 * @return MDS_OK; MDS_ERR_NOMEM.
 */
enum mds_status daos_eq_pool_init(struct daos_eq_pool *p, uint32_t worker_threads);

/** Destroy every created queue and free the slot array.  Idempotent. */
void daos_eq_pool_destroy(struct daos_eq_pool *p);

/* -----------------------------------------------------------------------
 * One DAOS operation under the transaction's event mode
 *
 * Every helper below is begin -> DAOS call -> finish.  A body that
 * builds a parallel wave uses the start / finish halves of the fetch
 * helper; everything else is synchronous.
 * ----------------------------------------------------------------------- */

struct daos_op {
    daos_event_t ev;
    bool         launched;
};

/**
 * Begin an operation: *evp receives the event to pass to the DAOS call
 * (NULL when the transaction runs with NULL events).
 *
 * @return 0; a -DER_* code from the event initialisation, in which case
 *         the operation must not be issued (nothing to finish).
 */
int daos_op_begin(struct daos_txn *t, struct daos_op *op, daos_event_t **evp);

/**
 * Finish an operation whose DAOS call returned @p rc_call: waits for the
 * event (event mode), counts the wait, releases the event.  A non-zero
 * @p rc_call means the client refused the call before launching it and
 * is returned as is.
 *
 * @return 0 or the operation's -DER_* result.
 */
int daos_op_finish(struct daos_txn *t, struct daos_op *op, int rc_call);

/* -----------------------------------------------------------------------
 * Object handles
 * ----------------------------------------------------------------------- */

/**
 * Open (or reuse) the object of table @p tag, entity @p id for this
 * attempt.  Handles are deduplicated per attempt and closed by the
 * runner after the body returns, so a body never closes them.  At most
 * DAOS_TXN_OBJ_MAX distinct objects per attempt; a wave over more
 * objects uses daos_obj_open_tag() / daos_obj_close_h() itself.
 *
 * @return 0; -DER_INVAL (unknown tag, unstamped backend); -DER_NOMEM
 *         when the attempt's table is full; a -DER_* from daos_obj_open.
 */
int daos_txn_obj(struct daos_txn *t, enum daos_tag tag, uint64_t id, daos_handle_t *oh);

/** Untracked open (DAOS_OO_RW); the caller closes with daos_obj_close_h(). */
int daos_obj_open_tag(const struct daos_backend *b, enum daos_tag tag, uint64_t id,
                      daos_handle_t *oh);

/** Close @p *oh when valid and invalidate it.  Safe on an invalid handle. */
void daos_obj_close_h(daos_handle_t *oh);

/* -----------------------------------------------------------------------
 * Fetch: one dkey, N single-value akeys, one RPC
 * ----------------------------------------------------------------------- */

/**
 * One fetch request.  The dkey is copied in; akeys are referenced (the
 * caller's struct daos_key must outlive the finish); value buffers and
 * the three descriptor arrays (cap entries each) are the caller's.
 * Stable in memory from start to finish.
 */
struct daos_fetch {
    struct daos_op   op;
    struct daos_key  dkey;
    d_iov_t          dkey_iov;
    daos_iod_t      *iods;
    d_sg_list_t     *sgls;
    d_iov_t         *iovs;
    uint32_t         nr;
    uint32_t         cap;
    bool             started;
    int              rc_call; /**< Return of daos_obj_fetch (event mode). */
};

/** Capacity of the embedded-storage request below. */
#define DAOS_FETCH_SMALL_CAP 4U

/** A fetch request with embedded storage for up to DAOS_FETCH_SMALL_CAP akeys. */
struct daos_fetch_small {
    struct daos_fetch f;
    daos_iod_t        iods[DAOS_FETCH_SMALL_CAP];
    d_sg_list_t       sgls[DAOS_FETCH_SMALL_CAP];
    d_iov_t           iovs[DAOS_FETCH_SMALL_CAP];
};

/** Start a request for @p dkey over the caller's arrays of @p cap entries. */
void daos_fetch_init(struct daos_fetch *f, const struct daos_key *dkey, daos_iod_t *iods,
                     d_sg_list_t *sgls, d_iov_t *iovs, uint32_t cap);

/** daos_fetch_init over the embedded storage of @p s. */
static inline void daos_fetch_small_init(struct daos_fetch_small *s, const struct daos_key *dkey)
{
    daos_fetch_init(&s->f, dkey, s->iods, s->sgls, s->iovs, DAOS_FETCH_SMALL_CAP);
}

/**
 * Add the single-value akey @p akey, to be read into @p buf (at most
 * @p cap bytes; iod_size DAOS_REC_ANY).
 *
 * @return false when the request is full or the key is unusable.
 */
bool daos_fetch_add(struct daos_fetch *f, struct daos_key *akey, void *buf, size_t cap);

/**
 * daos_fetch_add with the akey given as @p akey_len raw bytes at @p akey
 * (referenced, not copied: a listed key inside a page buffer, or a
 * compact key array).
 */
bool daos_fetch_add_raw(struct daos_fetch *f, void *akey, size_t akey_len, void *buf, size_t cap);

/**
 * Issue the fetch on @p oh under the transaction.  In event mode the
 * call returns at once and daos_fetch_finish() waits; with NULL events
 * the RPC completes here and finish only reports the result.
 *
 * @return 0; -DER_INVAL for an empty request or an unusable dkey.  Other
 *         failures are reported by daos_fetch_finish().
 */
int daos_fetch_start(struct daos_txn *t, daos_handle_t oh, struct daos_fetch *f);

/**
 * Wait for a started fetch.  On success daos_fetch_size(f, i) is the
 * stored size of akey i (0 = absent) and the value bytes are in the
 * caller's buffer.  -DER_REC2BIG when a value exceeds its buffer (the
 * sizes then report the stored sizes the client returned; a caller may
 * retry with a larger buffer).
 *
 * @return 0 or a -DER_* code for the body to return.
 */
int daos_fetch_finish(struct daos_txn *t, struct daos_fetch *f);

/** daos_fetch_start + daos_fetch_finish. */
int daos_fetch_run(struct daos_txn *t, daos_handle_t oh, struct daos_fetch *f);

/** Stored size of akey @p i after a finished fetch (0 = absent). */
static inline size_t daos_fetch_size(const struct daos_fetch *f, uint32_t i)
{
    return (size_t)f->iods[i].iod_size;
}

/**
 * Fetch one single-value akey into @p buf (at most @p cap bytes).
 *
 * @param len    Receives the stored size (0 when absent; on -DER_REC2BIG
 *               the stored size when the client reported it).
 * @param found  Receives size != 0.
 * @return 0; -DER_REC2BIG; -DER_INVAL for an unusable key; -DER_* from DAOS.
 */
int daos_fetch_single(struct daos_txn *t, daos_handle_t oh, const struct daos_key *dkey,
                      struct daos_key *akey, void *buf, size_t cap, size_t *len, bool *found);

/* -----------------------------------------------------------------------
 * Update: one dkey, N single-value akeys, one buffered operation
 * ----------------------------------------------------------------------- */

struct daos_update {
    struct daos_op   op;
    struct daos_key  dkey;
    d_iov_t          dkey_iov;
    daos_iod_t      *iods;
    d_sg_list_t     *sgls;
    d_iov_t         *iovs;
    uint32_t         nr;
    uint32_t         cap;
    uint64_t         flags; /**< DAOS_COND_* dkey-level flags. */
};

/** Capacity of the embedded-storage request below. */
#define DAOS_UPDATE_SMALL_CAP 4U

/** An update request with embedded storage for up to DAOS_UPDATE_SMALL_CAP akeys. */
struct daos_update_small {
    struct daos_update u;
    daos_iod_t         iods[DAOS_UPDATE_SMALL_CAP];
    d_sg_list_t        sgls[DAOS_UPDATE_SMALL_CAP];
    d_iov_t            iovs[DAOS_UPDATE_SMALL_CAP];
};

/**
 * Start an update of @p dkey over the caller's arrays of @p cap entries.
 * @p flags are the dkey-level conditional flags (DAOS_COND_DKEY_INSERT /
 * _UPDATE, 0 for a plain upsert); DAOS_COND_PER_AKEY is added
 * automatically when an akey carries its own condition.
 */
void daos_update_init(struct daos_update *u, const struct daos_key *dkey, daos_iod_t *iods,
                      d_sg_list_t *sgls, d_iov_t *iovs, uint32_t cap, uint64_t flags);

/** daos_update_init over the embedded storage of @p s. */
static inline void daos_update_small_init(struct daos_update_small *s,
                                          const struct daos_key *dkey, uint64_t flags)
{
    daos_update_init(&s->u, dkey, s->iods, s->sgls, s->iovs, DAOS_UPDATE_SMALL_CAP, flags);
}

/**
 * Add a single-value akey of @p len bytes (len > 0: a zero-length update
 * is a punch and is refused) with the per-akey condition @p akey_cond
 * (0, DAOS_COND_AKEY_INSERT or DAOS_COND_AKEY_UPDATE).  The value is
 * copied by the client when the update is issued.
 *
 * @return false when the request is full, @p len is 0 or the key is unusable.
 */
bool daos_update_add(struct daos_update *u, struct daos_key *akey, const void *val, size_t len,
                     uint64_t akey_cond);

/**
 * Issue the update (synchronous: begin, call, finish).  Under a
 * transaction it is buffered until commit and a condition failure may
 * surface from the commit instead.
 *
 * @return 0; -DER_INVAL for an empty request; -DER_EXIST / -DER_NONEXIST
 *         for a condition evaluated at the call; other -DER_* codes.
 */
int daos_update_run(struct daos_txn *t, daos_handle_t oh, struct daos_update *u);

/** One-akey update: daos_update_init + add + run. */
int daos_update_single(struct daos_txn *t, daos_handle_t oh, const struct daos_key *dkey,
                       struct daos_key *akey, const void *val, size_t len, uint64_t flags,
                       uint64_t akey_cond);

/* -----------------------------------------------------------------------
 * Punch
 * ----------------------------------------------------------------------- */

/**
 * Punch @p nr akeys (<= DAOS_PUNCH_MAX) of @p dkey.  @p flags: 0 or
 * DAOS_COND_PUNCH (then -DER_NONEXIST when a key is absent).
 *
 * @return 0; -DER_INVAL for nr == 0, nr > DAOS_PUNCH_MAX or an unusable key.
 */
int daos_punch_akeys(struct daos_txn *t, daos_handle_t oh, const struct daos_key *dkey,
                     struct daos_key *akeys, uint32_t nr, uint64_t flags);

/** Punch @p nr whole dkeys (<= DAOS_PUNCH_MAX) with all their akeys. */
int daos_punch_dkeys(struct daos_txn *t, daos_handle_t oh, struct daos_key *dkeys,
                     uint32_t nr, uint64_t flags);

/* -----------------------------------------------------------------------
 * Enumeration (one page per call; keys only -- values need a fetch)
 * ----------------------------------------------------------------------- */

/**
 * One page of keys.  kds / buf are the caller's: @p cap descriptors and
 * a byte buffer of @p buf_cap (cap * DAOS_KEY_MAX is always enough).
 */
struct daos_list {
    daos_key_desc_t *kds;
    uint8_t         *buf;
    size_t           buf_cap;
    uint32_t         cap;
    uint32_t         nr;   /**< Out: keys in this page. */
    bool             eof;  /**< Out: nothing follows this page. */
};

void daos_list_init(struct daos_list *l, daos_key_desc_t *kds, uint32_t cap, uint8_t *buf,
                    size_t buf_cap);

/**
 * List up to l->cap dkeys of @p oh, starting strictly after @p resume
 * (NULL: from the first key).  The resume position comes from
 * daos_obj_key2anchor(); whether that resumes at or after the key is
 * treated as unknown, so a returned key equal to @p resume is dropped
 * and the listing continues until the page holds a key or hits the end.
 * Sorted (lexical / numeric) only on a sorted-dkey object; hash order
 * otherwise -- stable in both cases.
 *
 * @return 0; -DER_KEY2BIG when a key does not fit the buffer; -DER_*.
 */
int daos_list_dkeys(struct daos_txn *t, daos_handle_t oh, struct daos_key *resume,
                    struct daos_list *l);

/** Same for the akeys of @p dkey (hash order on the backend's objects). */
int daos_list_akeys(struct daos_txn *t, daos_handle_t oh, struct daos_key *dkey,
                    struct daos_key *resume, struct daos_list *l);

/** Key @p i of a listed page (pointer into l->buf); false when i >= nr. */
bool daos_list_key(const struct daos_list *l, uint32_t i, const uint8_t **key, size_t *len);

/* -----------------------------------------------------------------------
 * Unique id allocators (META counters, thread-local batches)
 * ----------------------------------------------------------------------- */

/**
 * Hand out one id from the META counter @p counter (DAOS_META_FILEID,
 * DAOS_META_GC_SEQ, DAOS_META_REMOVE_SEQ or DAOS_META_COOKIE_SEQ).  Ids
 * are minted in thread-local batches of MDS_FILEID_BATCH: a refill is
 * one mutating transaction that reads the counter and writes counter +
 * batch, so two refills of one counter conflict and one restarts --
 * batches never overlap, ids are unique container-wide and a refill
 * costs one commit per MDS_FILEID_BATCH ids.  A pool is bound to the
 * backend instance that filled it (b->instance_seq); a different
 * instance on the same thread discards the remainder.
 *
 * A refill whose commit outcome is in doubt is reported as MDS_ERR_DELAY,
 * never MDS_ERR_INDOUBT: the batch it may have reserved is never handed
 * out, so ids stay unique and a lost batch is only a gap, while the
 * caller has committed nothing user-visible yet and may retry.
 *
 * @return MDS_OK; MDS_ERR_IO when the counter is absent (container not
 *         bootstrapped); MDS_ERR_NOSPC when the counter would wrap;
 *         MDS_ERR_DELAY; MDS_ERR_IO from the refill; MDS_ERR_INVAL for a
 *         NULL argument or a key that is not an allocator.
 */
enum mds_status daos_backend_alloc_id(struct daos_backend *b, enum daos_meta_key counter,
                                      uint64_t *id);

#endif /* DAOS_TXN_H */

/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * daos_txn.c -- DAOS transaction runner (see daos_txn.h).
 *
 * Structure:
 *   - error classification and the monotonic clock;
 *   - the event-queue pool a transaction borrows one queue from;
 *   - daos_op_begin / daos_op_finish, the one place that waits on the
 *     network (every helper and the runner itself go through it, so the
 *     event mode of a transaction is uniform by construction);
 *   - object handles, fetch, update, punch and enumeration helpers;
 *   - daos_run_txn: open -> body -> commit, restart on -DER_TX_RESTART,
 *     fresh transaction after a transient operation error, INDOUBT for a
 *     transient commit error, bounded by the operation deadline;
 *   - the thread-local id batch allocators over the META counters.
 */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "daos_txn.h"
#include "fdb_codec.h"
#include "mds_log.h"

/* A new attempt is only started with this much of the deadline left. */
#define DAOS_MIN_ATTEMPT_MS 200U
/* Transient-error backoff: 1 ms doubling per round up to this cap. */
#define DAOS_BACKOFF_MAX_MS 100U
#define DAOS_BACKOFF_MAX_SHIFT 7U

#define NS_PER_MS 1000000ULL

/* -----------------------------------------------------------------------
 * Clock, error strings, classification
 * ----------------------------------------------------------------------- */

uint64_t daos_txn_now_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

const char *daos_errstr(int rc)
{
    const char *s = d_errstr(rc);

    return s != NULL ? s : "?";
}

bool daos_err_is_transient(int rc)
{
    switch (rc) {
    case -DER_INPROGRESS:
    case -DER_TX_BUSY:
    case -DER_AGAIN:
    case -DER_OVERLOAD_RETRY:
    case -DER_TIMEDOUT:
    case -DER_UNREACH:
    case -DER_OOG:
    case -DER_HG:
    case -DER_EXCLUDED:
    case -DER_GRPVER:
    case -DER_STALE:
    case -DER_NOTLEADER:
    case -DER_TX_UNCERTAIN:
        return true;
    default:
        return false;
    }
}

enum mds_status daos_err_status(int rc)
{
    if (rc == 0) {
        return MDS_OK;
    }
    if (daos_err_is_transient(rc)) {
        return MDS_ERR_DELAY;
    }
    switch (rc) {
    case -DER_NONEXIST:
        return MDS_ERR_NOTFOUND;
    case -DER_EXIST:
        return MDS_ERR_EXISTS;
    case -DER_NOSPACE:
        return MDS_ERR_NOSPC;
    case -DER_NO_PERM:
        return MDS_ERR_PERM;
    case -DER_NOMEM:
        return MDS_ERR_NOMEM;
    default:
        return MDS_ERR_IO;
    }
}

static void stat_inc(_Atomic uint64_t *ctr)
{
    atomic_fetch_add_explicit(ctr, 1U, memory_order_relaxed);
}

/* -----------------------------------------------------------------------
 * Event-queue pool
 * ----------------------------------------------------------------------- */

enum mds_status daos_eq_pool_init(struct daos_eq_pool *p, uint32_t worker_threads)
{
    uint32_t n = worker_threads > DAOS_EQ_POOL_MIN ? worker_threads : DAOS_EQ_POOL_MIN;

    if (p == NULL) {
        return MDS_ERR_INVAL;
    }
    if (n > DAOS_EQ_POOL_CAP - DAOS_EQ_POOL_EXTRA) {
        n = DAOS_EQ_POOL_CAP - DAOS_EQ_POOL_EXTRA;
    }
    n += DAOS_EQ_POOL_EXTRA;
    memset(p, 0, sizeof(*p));
    if (pthread_mutex_init(&p->lock, NULL) != 0) {
        return MDS_ERR_IO;
    }
    p->slots = calloc(n, sizeof(*p->slots));
    if (p->slots == NULL) {
        (void)pthread_mutex_destroy(&p->lock);
        return MDS_ERR_NOMEM;
    }
    p->cap = n;
    return MDS_OK;
}

void daos_eq_pool_destroy(struct daos_eq_pool *p)
{
    uint32_t i;

    if (p == NULL || p->slots == NULL) {
        return;
    }
    (void)pthread_mutex_lock(&p->lock);
    for (i = 0; i < p->cap; i++) {
        struct daos_eq_slot *s = &p->slots[i];
        int rc;

        if (!s->created) {
            continue;
        }
        rc = daos_eq_destroy(s->eqh, 0);
        if (rc == -DER_BUSY) {
            MDS_LOG_WARN(LOG_COMP_CAT, "daos: event queue %u busy at close; forcing", i);
            rc = daos_eq_destroy(s->eqh, DAOS_EQ_DESTROY_FORCE);
        }
        if (rc != 0) {
            MDS_LOG_ERROR(LOG_COMP_CAT, "daos_eq_destroy: %d %s", rc, daos_errstr(rc));
        }
        s->created = false;
    }
    (void)pthread_mutex_unlock(&p->lock);
    (void)pthread_mutex_destroy(&p->lock);
    free(p->slots);
    memset(p, 0, sizeof(*p));
}

/* Borrow a free queue, creating one lazily while slots remain.  False
 * (NULL-event mode) when every queue is busy or creation failed. */
static bool eq_borrow(struct daos_eq_pool *p, daos_handle_t *eqh)
{
    bool got = false;
    uint32_t i;

    if (p->slots == NULL) {
        return false;
    }
    (void)pthread_mutex_lock(&p->lock);
    for (i = 0; i < p->created; i++) {
        if (!p->slots[i].busy) {
            p->slots[i].busy = true;
            *eqh = p->slots[i].eqh;
            got = true;
            break;
        }
    }
    if (!got && p->created < p->cap) {
        struct daos_eq_slot *s = &p->slots[p->created];
        int rc = daos_eq_create(&s->eqh);

        if (rc == 0) {
            s->created = true;
            s->busy = true;
            p->created++;
            *eqh = s->eqh;
            got = true;
        } else {
            MDS_LOG_WARN(LOG_COMP_CAT, "daos_eq_create: %d %s; transaction runs without "
                         "events", rc, daos_errstr(rc));
        }
    }
    (void)pthread_mutex_unlock(&p->lock);
    return got;
}

static void eq_return(struct daos_eq_pool *p, daos_handle_t eqh)
{
    uint32_t i;

    (void)pthread_mutex_lock(&p->lock);
    for (i = 0; i < p->created; i++) {
        if (p->slots[i].eqh.cookie == eqh.cookie) {
            p->slots[i].busy = false;
            break;
        }
    }
    (void)pthread_mutex_unlock(&p->lock);
}

/* -----------------------------------------------------------------------
 * One operation under the transaction's event mode
 * ----------------------------------------------------------------------- */

int daos_op_begin(struct daos_txn *t, struct daos_op *op, daos_event_t **evp)
{
    int rc;

    op->launched = false;
    *evp = NULL;
    if (!t->use_events) {
        return 0;
    }
    rc = daos_event_init(&op->ev, t->eqh, NULL);
    if (rc != 0) {
        return rc;
    }
    op->launched = true;
    *evp = &op->ev;
    return 0;
}

/* Wait for a launched event.  A completed event is popped from its
 * queue by daos_event_test; the blocking wait is counted when @p count. */
static int op_wait(struct daos_txn *t, struct daos_op *op, bool count)
{
    bool done = false;
    int rc;

    rc = daos_event_test(&op->ev, DAOS_EQ_NOWAIT, &done);
    if (rc == 0 && !done && count) {
        stat_inc(&t->b->stats.waits);
    }
    while (rc == 0 && !done) {
        rc = daos_event_test(&op->ev, DAOS_EQ_WAIT, &done);
    }
    if (rc == 0) {
        rc = op->ev.ev_error;
    }
    return rc;
}

/* daos_op_finish with the wait accounting switchable: the local
 * transaction-handle operations (open, restart, abort, close) are no
 * network round trip and are not counted. */
static int op_finish_ex(struct daos_txn *t, struct daos_op *op, int rc_call, bool count)
{
    int rc;

    if (!op->launched) {
        /* NULL-event mode: the blocking call itself was the wait. */
        if (count) {
            stat_inc(&t->b->stats.waits);
        }
        return rc_call;
    }
    if (rc_call != 0) {
        /* Refused before launch: no completion will ever arrive. */
        rc = rc_call;
    } else {
        rc = op_wait(t, op, count);
    }
    (void)daos_event_fini(&op->ev);
    op->launched = false;
    return rc;
}

int daos_op_finish(struct daos_txn *t, struct daos_op *op, int rc_call)
{
    return op_finish_ex(t, op, rc_call, true);
}

/* -----------------------------------------------------------------------
 * Object handles
 * ----------------------------------------------------------------------- */

int daos_obj_open_tag(const struct daos_backend *b, enum daos_tag tag, uint64_t id,
                      daos_handle_t *oh)
{
    daos_obj_id_t oid;
    int rc;

    if (b == NULL || oh == NULL) {
        return -DER_INVAL;
    }
    *oh = DAOS_HDL_INVAL;
    rc = daos_oid_make(tag, id, b->cid_entity, b->cid_spread, &oid);
    if (rc != 0) {
        return rc;
    }
    return daos_obj_open(b->coh, oid, DAOS_OO_RW, oh, NULL);
}

void daos_obj_close_h(daos_handle_t *oh)
{
    if (oh != NULL && daos_handle_is_valid(*oh)) {
        (void)daos_obj_close(*oh, NULL);
        *oh = DAOS_HDL_INVAL;
    }
}

int daos_txn_obj(struct daos_txn *t, enum daos_tag tag, uint64_t id, daos_handle_t *oh)
{
    daos_obj_id_t oid;
    struct daos_txn_obj *slot;
    uint32_t i;
    int rc;

    if (t == NULL || oh == NULL) {
        return -DER_INVAL;
    }
    rc = daos_oid_make(tag, id, t->b->cid_entity, t->b->cid_spread, &oid);
    if (rc != 0) {
        return rc;
    }
    for (i = 0; i < t->nobjs; i++) {
        if (t->objs[i].open && t->objs[i].oid.lo == oid.lo && t->objs[i].oid.hi == oid.hi) {
            *oh = t->objs[i].oh;
            return 0;
        }
    }
    if (t->nobjs >= DAOS_TXN_OBJ_MAX) {
        return -DER_NOMEM;
    }
    slot = &t->objs[t->nobjs];
    slot->oid = oid;
    slot->oh = DAOS_HDL_INVAL;
    rc = daos_obj_open(t->b->coh, oid, DAOS_OO_RW, &slot->oh, NULL);
    if (rc != 0) {
        return rc;
    }
    slot->open = true;
    t->nobjs++;
    *oh = slot->oh;
    return 0;
}

/* Close every handle the attempt opened through daos_txn_obj(). */
static void txn_objs_close(struct daos_txn *t)
{
    uint32_t i;

    for (i = 0; i < t->nobjs; i++) {
        if (t->objs[i].open) {
            daos_obj_close_h(&t->objs[i].oh);
            t->objs[i].open = false;
        }
    }
    t->nobjs = 0;
}

/* -----------------------------------------------------------------------
 * Fetch
 * ----------------------------------------------------------------------- */

void daos_fetch_init(struct daos_fetch *f, const struct daos_key *dkey, daos_iod_t *iods,
                     d_sg_list_t *sgls, d_iov_t *iovs, uint32_t cap)
{
    memset(&f->op, 0, sizeof(f->op));
    f->dkey = *dkey;
    daos_key_iov(&f->dkey, &f->dkey_iov);
    f->iods = iods;
    f->sgls = sgls;
    f->iovs = iovs;
    f->nr = 0;
    f->cap = cap;
    f->started = false;
    f->rc_call = 0;
}

bool daos_fetch_add_raw(struct daos_fetch *f, void *akey, size_t akey_len, void *buf, size_t cap)
{
    daos_iod_t *iod;
    uint32_t i = f->nr;

    if (i >= f->cap || akey == NULL || akey_len == 0 || akey_len > DAOS_KEY_MAX ||
        (cap > 0 && buf == NULL)) {
        return false;
    }
    iod = &f->iods[i];
    memset(iod, 0, sizeof(*iod));
    d_iov_set(&iod->iod_name, akey, akey_len);
    iod->iod_type = DAOS_IOD_SINGLE;
    iod->iod_size = DAOS_REC_ANY;
    iod->iod_nr = 1;
    iod->iod_recxs = NULL;
    d_iov_set(&f->iovs[i], buf, cap);
    f->sgls[i].sg_nr = 1;
    f->sgls[i].sg_nr_out = 0;
    f->sgls[i].sg_iovs = &f->iovs[i];
    f->nr++;
    return true;
}

bool daos_fetch_add(struct daos_fetch *f, struct daos_key *akey, void *buf, size_t cap)
{
    if (akey == NULL || !daos_key_ok(akey)) {
        return false;
    }
    return daos_fetch_add_raw(f, akey->buf, akey->len, buf, cap);
}

int daos_fetch_start(struct daos_txn *t, daos_handle_t oh, struct daos_fetch *f)
{
    daos_event_t *ev = NULL;
    int rc;

    if (f->nr == 0 || !daos_key_ok(&f->dkey)) {
        return -DER_INVAL;
    }
    rc = daos_op_begin(t, &f->op, &ev);
    if (rc != 0) {
        return rc;
    }
    f->started = true;
    f->rc_call = daos_obj_fetch(oh, t->th, 0, &f->dkey_iov, f->nr, f->iods, f->sgls, NULL, ev);
    return 0;
}

int daos_fetch_finish(struct daos_txn *t, struct daos_fetch *f)
{
    if (!f->started) {
        return -DER_INVAL;
    }
    f->started = false;
    return daos_op_finish(t, &f->op, f->rc_call);
}

int daos_fetch_run(struct daos_txn *t, daos_handle_t oh, struct daos_fetch *f)
{
    int rc = daos_fetch_start(t, oh, f);

    if (rc != 0) {
        return rc;
    }
    return daos_fetch_finish(t, f);
}

int daos_fetch_single(struct daos_txn *t, daos_handle_t oh, const struct daos_key *dkey,
                      struct daos_key *akey, void *buf, size_t cap, size_t *len, bool *found)
{
    struct daos_fetch_small s;
    int rc;

    *len = 0;
    *found = false;
    daos_fetch_small_init(&s, dkey);
    if (!daos_fetch_add(&s.f, akey, buf, cap)) {
        return -DER_INVAL;
    }
    rc = daos_fetch_run(t, oh, &s.f);
    if (rc == 0 || rc == -DER_REC2BIG) {
        *len = daos_fetch_size(&s.f, 0);
        *found = (rc == 0 && *len != 0);
    }
    return rc;
}

/* -----------------------------------------------------------------------
 * Update
 * ----------------------------------------------------------------------- */

void daos_update_init(struct daos_update *u, const struct daos_key *dkey, daos_iod_t *iods,
                      d_sg_list_t *sgls, d_iov_t *iovs, uint32_t cap, uint64_t flags)
{
    memset(&u->op, 0, sizeof(u->op));
    u->dkey = *dkey;
    daos_key_iov(&u->dkey, &u->dkey_iov);
    u->iods = iods;
    u->sgls = sgls;
    u->iovs = iovs;
    u->nr = 0;
    u->cap = cap;
    u->flags = flags;
}

bool daos_update_add(struct daos_update *u, struct daos_key *akey, const void *val, size_t len,
                     uint64_t akey_cond)
{
    daos_iod_t *iod;
    uint32_t i = u->nr;

    if (i >= u->cap || akey == NULL || !daos_key_ok(akey) || val == NULL || len == 0) {
        return false;
    }
    iod = &u->iods[i];
    memset(iod, 0, sizeof(*iod));
    daos_key_iov(akey, &iod->iod_name);
    iod->iod_type = DAOS_IOD_SINGLE;
    iod->iod_size = len;
    iod->iod_nr = 1;
    iod->iod_recxs = NULL;
    iod->iod_flags = akey_cond;
    if (akey_cond != 0) {
        u->flags |= DAOS_COND_PER_AKEY;
    }
    /* The client copies the value when the update is issued; the API is
     * not const-correct, hence the cast. */
    d_iov_set(&u->iovs[i], (void *)(uintptr_t)val, len);
    u->sgls[i].sg_nr = 1;
    u->sgls[i].sg_nr_out = 0;
    u->sgls[i].sg_iovs = &u->iovs[i];
    u->nr++;
    return true;
}

int daos_update_run(struct daos_txn *t, daos_handle_t oh, struct daos_update *u)
{
    daos_event_t *ev = NULL;
    int rc;

    if (u->nr == 0 || !daos_key_ok(&u->dkey)) {
        return -DER_INVAL;
    }
    rc = daos_op_begin(t, &u->op, &ev);
    if (rc != 0) {
        return rc;
    }
    rc = daos_obj_update(oh, t->th, u->flags, &u->dkey_iov, u->nr, u->iods, u->sgls, ev);
    return daos_op_finish(t, &u->op, rc);
}

int daos_update_single(struct daos_txn *t, daos_handle_t oh, const struct daos_key *dkey,
                       struct daos_key *akey, const void *val, size_t len, uint64_t flags,
                       uint64_t akey_cond)
{
    struct daos_update_small s;

    daos_update_small_init(&s, dkey, flags);
    if (!daos_update_add(&s.u, akey, val, len, akey_cond)) {
        return -DER_INVAL;
    }
    return daos_update_run(t, oh, &s.u);
}

/* -----------------------------------------------------------------------
 * Punch
 * ----------------------------------------------------------------------- */

static bool keys_to_iovs(struct daos_key *keys, uint32_t nr, d_iov_t *iovs)
{
    uint32_t i;

    if (keys == NULL || nr == 0 || nr > DAOS_PUNCH_MAX) {
        return false;
    }
    for (i = 0; i < nr; i++) {
        if (!daos_key_ok(&keys[i])) {
            return false;
        }
        daos_key_iov(&keys[i], &iovs[i]);
    }
    return true;
}

int daos_punch_akeys(struct daos_txn *t, daos_handle_t oh, const struct daos_key *dkey,
                     struct daos_key *akeys, uint32_t nr, uint64_t flags)
{
    struct daos_key dk;
    d_iov_t dk_iov;
    d_iov_t aks[DAOS_PUNCH_MAX];
    struct daos_op op;
    daos_event_t *ev = NULL;
    int rc;

    if (dkey == NULL || !daos_key_ok(dkey) || !keys_to_iovs(akeys, nr, aks)) {
        return -DER_INVAL;
    }
    dk = *dkey;
    daos_key_iov(&dk, &dk_iov);
    rc = daos_op_begin(t, &op, &ev);
    if (rc != 0) {
        return rc;
    }
    rc = daos_obj_punch_akeys(oh, t->th, flags, &dk_iov, nr, aks, ev);
    return daos_op_finish(t, &op, rc);
}

int daos_punch_dkeys(struct daos_txn *t, daos_handle_t oh, struct daos_key *dkeys,
                     uint32_t nr, uint64_t flags)
{
    d_iov_t dks[DAOS_PUNCH_MAX];
    struct daos_op op;
    daos_event_t *ev = NULL;
    int rc;

    if (!keys_to_iovs(dkeys, nr, dks)) {
        return -DER_INVAL;
    }
    rc = daos_op_begin(t, &op, &ev);
    if (rc != 0) {
        return rc;
    }
    rc = daos_obj_punch_dkeys(oh, t->th, flags, nr, dks, ev);
    return daos_op_finish(t, &op, rc);
}

/* -----------------------------------------------------------------------
 * Enumeration
 * ----------------------------------------------------------------------- */

void daos_list_init(struct daos_list *l, daos_key_desc_t *kds, uint32_t cap, uint8_t *buf,
                    size_t buf_cap)
{
    l->kds = kds;
    l->buf = buf;
    l->buf_cap = buf_cap;
    l->cap = cap;
    l->nr = 0;
    l->eof = false;
}

bool daos_list_key(const struct daos_list *l, uint32_t i, const uint8_t **key, size_t *len)
{
    size_t off = 0;
    uint32_t j;

    if (i >= l->nr) {
        return false;
    }
    for (j = 0; j < i; j++) {
        off += (size_t)l->kds[j].kd_key_len;
    }
    *key = l->buf + off;
    *len = (size_t)l->kds[i].kd_key_len;
    return true;
}

/* Position the anchor strictly at @p resume's place in the key order
 * (whether at or after it is unknown; the caller drops an equal key). */
static int list_resume(struct daos_txn *t, daos_handle_t oh, d_iov_t *dkey, d_iov_t *resume,
                       bool akeys, daos_anchor_t *anchor)
{
    struct daos_op op;
    daos_event_t *ev = NULL;
    int rc;

    rc = daos_op_begin(t, &op, &ev);
    if (rc != 0) {
        return rc;
    }
    if (akeys) {
        rc = daos_obj_key2anchor(oh, t->th, dkey, resume, anchor, ev);
    } else {
        rc = daos_obj_key2anchor(oh, t->th, resume, NULL, anchor, ev);
    }
    return daos_op_finish(t, &op, rc);
}

/* Drop the first listed key when it is the resume key itself. */
static void list_drop_resume(struct daos_list *l, const struct daos_key *resume)
{
    size_t first;
    size_t total = 0;
    uint32_t i;

    if (l->nr == 0) {
        return;
    }
    first = (size_t)l->kds[0].kd_key_len;
    if (!daos_key_equals(resume, l->buf, first)) {
        return;
    }
    for (i = 0; i < l->nr; i++) {
        total += (size_t)l->kds[i].kd_key_len;
    }
    memmove(l->buf, l->buf + first, total - first);
    memmove(&l->kds[0], &l->kds[1], (size_t)(l->nr - 1) * sizeof(l->kds[0]));
    l->nr--;
}

static int list_keys(struct daos_txn *t, daos_handle_t oh, struct daos_key *dkey,
                     struct daos_key *resume, struct daos_list *l)
{
    daos_anchor_t anchor;
    d_iov_t dk_iov;
    d_iov_t rs_iov;
    int rc;

    l->nr = 0;
    l->eof = false;
    if (l->cap == 0 || l->kds == NULL || l->buf == NULL || l->buf_cap == 0 ||
        (dkey != NULL && !daos_key_ok(dkey)) || (resume != NULL && !daos_key_ok(resume))) {
        return -DER_INVAL;
    }
    if (dkey != NULL) {
        daos_key_iov(dkey, &dk_iov);
    }
    (void)daos_anchor_init(&anchor, 0);
    if (resume != NULL) {
        daos_key_iov(resume, &rs_iov);
        rc = list_resume(t, oh, dkey != NULL ? &dk_iov : NULL, &rs_iov, dkey != NULL, &anchor);
        if (rc != 0) {
            return rc;
        }
    }
    for (;;) {
        struct daos_op op;
        daos_event_t *ev = NULL;
        d_sg_list_t sgl;
        d_iov_t iov;
        uint32_t nr = l->cap;

        d_iov_set(&iov, l->buf, l->buf_cap);
        sgl.sg_nr = 1;
        sgl.sg_nr_out = 0;
        sgl.sg_iovs = &iov;
        rc = daos_op_begin(t, &op, &ev);
        if (rc != 0) {
            return rc;
        }
        if (dkey != NULL) {
            rc = daos_obj_list_akey(oh, t->th, &dk_iov, &nr, l->kds, &sgl, &anchor, ev);
        } else {
            rc = daos_obj_list_dkey(oh, t->th, &nr, l->kds, &sgl, &anchor, ev);
        }
        rc = daos_op_finish(t, &op, rc);
        if (rc != 0) {
            return rc;
        }
        l->nr = nr;
        l->eof = daos_anchor_is_eof(&anchor);
        if (resume != NULL) {
            list_drop_resume(l, resume);
        }
        /* An empty page that is not the end (only the resume key, or
         * keys punched under the anchor) continues from the anchor. */
        if (l->nr > 0 || l->eof) {
            return 0;
        }
    }
}

int daos_list_dkeys(struct daos_txn *t, daos_handle_t oh, struct daos_key *resume,
                    struct daos_list *l)
{
    return list_keys(t, oh, NULL, resume, l);
}

int daos_list_akeys(struct daos_txn *t, daos_handle_t oh, struct daos_key *dkey,
                    struct daos_key *resume, struct daos_list *l)
{
    if (dkey == NULL) {
        return -DER_INVAL;
    }
    return list_keys(t, oh, dkey, resume, l);
}

/* -----------------------------------------------------------------------
 * Transaction handle operations (through the event mode)
 * ----------------------------------------------------------------------- */

static int tx_open(struct daos_txn *t, uint64_t flags)
{
    struct daos_op op;
    daos_event_t *ev = NULL;
    int rc;

    rc = daos_op_begin(t, &op, &ev);
    if (rc != 0) {
        return rc;
    }
    rc = daos_tx_open(t->b->coh, &t->th, flags, ev);
    rc = op_finish_ex(t, &op, rc, false);
    if (rc != 0) {
        t->th = DAOS_TX_NONE;
    }
    return rc;
}

static int tx_commit(struct daos_txn *t)
{
    struct daos_op op;
    daos_event_t *ev = NULL;
    int rc;

    rc = daos_op_begin(t, &op, &ev);
    if (rc != 0) {
        return rc;
    }
    rc = daos_tx_commit(t->th, ev);
    return daos_op_finish(t, &op, rc);
}

static int tx_restart(struct daos_txn *t)
{
    struct daos_op op;
    daos_event_t *ev = NULL;
    int rc;

    rc = daos_op_begin(t, &op, &ev);
    if (rc != 0) {
        return rc;
    }
    rc = daos_tx_restart(t->th, ev);
    return op_finish_ex(t, &op, rc, false);
}

/* Discard (when @p abort) and close the transaction handle.  Errors
 * from abort / close of a handle that is already finished are
 * uninteresting: the handle is dropped either way. */
static void tx_abort_close(struct daos_txn *t, bool abort)
{
    struct daos_op op;
    daos_event_t *ev = NULL;
    int rc;

    if (!daos_handle_is_valid(t->th)) {
        return;
    }
    if (abort) {
        rc = daos_op_begin(t, &op, &ev);
        if (rc == 0) {
            rc = daos_tx_abort(t->th, ev);
            (void)op_finish_ex(t, &op, rc, false);
        }
    }
    rc = daos_op_begin(t, &op, &ev);
    if (rc == 0) {
        rc = daos_tx_close(t->th, ev);
        (void)op_finish_ex(t, &op, rc, false);
    }
    t->th = DAOS_TX_NONE;
}

/* -----------------------------------------------------------------------
 * Runner
 * ----------------------------------------------------------------------- */

struct run_state {
    struct daos_backend *b;
    const char          *op;
    uint64_t             deadline_ns;
    uint32_t             backoff_round;
};

enum attempt_action {
    ATTEMPT_RETURN,  /**< *result is final. */
    ATTEMPT_RERUN,   /**< Same handle, restarted: run the body again. */
    ATTEMPT_BACKOFF, /**< Handle closed: back off, fresh transaction. */
};

static uint64_t remaining_ms(const struct run_state *rs)
{
    uint64_t now = daos_txn_now_ns();

    if (now >= rs->deadline_ns) {
        return 0;
    }
    return (rs->deadline_ns - now) / NS_PER_MS;
}

static void backoff(struct run_state *rs)
{
    uint32_t shift = rs->backoff_round < DAOS_BACKOFF_MAX_SHIFT ? rs->backoff_round :
                                                                  DAOS_BACKOFF_MAX_SHIFT;
    uint64_t ms = 1ULL << shift;
    uint64_t left = remaining_ms(rs);
    struct timespec ts;

    if (ms > DAOS_BACKOFF_MAX_MS) {
        ms = DAOS_BACKOFF_MAX_MS;
    }
    if (ms > left) {
        ms = left;
    }
    rs->backoff_round++;
    if (ms == 0) {
        return;
    }
    ts.tv_sec = (time_t)(ms / 1000U);
    ts.tv_nsec = (long)((ms % 1000U) * NS_PER_MS);
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {
        continue;
    }
}

static enum mds_status finish_indoubt(const struct run_state *rs, int rc)
{
    stat_inc(&rs->b->stats.indoubt);
    MDS_LOG_ERROR(LOG_COMP_CAT, "daos %s: commit ended with %d (%s): outcome unknown, "
                  "MDS_ERR_INDOUBT", rs->op, rc, daos_errstr(rc));
    return MDS_ERR_INDOUBT;
}

static enum mds_status finish_delay(const struct run_state *rs)
{
    stat_inc(&rs->b->stats.delay_exhausted);
    MDS_LOG_WARN(LOG_COMP_CAT, "daos %s: deadline (%u ms) exhausted: MDS_ERR_DELAY", rs->op,
                 rs->b->op_deadline_ms);
    return MDS_ERR_DELAY;
}

/* Final classification of a definitive operation or commit error. */
static enum mds_status finish_error(const struct run_state *rs, int rc)
{
    enum mds_status st = daos_err_status(rc);

    if (st == MDS_ERR_IO || st == MDS_ERR_NOMEM) {
        stat_inc(&rs->b->stats.io_errors);
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos %s: error %d (%s): %s", rs->op, rc, daos_errstr(rc),
                      st == MDS_ERR_IO ? "MDS_ERR_IO" : "MDS_ERR_NOMEM");
    }
    return st;
}

/* Restart the handle after -DER_TX_RESTART; false when that failed
 * (*rc then holds the restart error). */
static bool restart_handle(struct run_state *rs, struct daos_txn *t, int *rc)
{
    int rrc;

    stat_inc(&rs->b->stats.restarts);
    rrc = tx_restart(t);
    if (rrc == 0) {
        return true;
    }
    *rc = rrc;
    return false;
}

/* The body returned the operation error @p rc. */
static enum attempt_action op_error(struct run_state *rs, struct daos_txn *t, int rc,
                                    enum mds_status *result)
{
    bool mutating = (t->kind == DAOS_TXN_MUTATING);

    if (rc == -DER_TX_RESTART && t->kind != DAOS_TXN_NONE) {
        if (restart_handle(rs, t, &rc)) {
            return ATTEMPT_RERUN;
        }
    }
    if (daos_err_is_transient(rc) || (rc == -DER_TX_RESTART && t->kind == DAOS_TXN_NONE)) {
        stat_inc(&rs->b->stats.transient_retries);
        tx_abort_close(t, mutating);
        return ATTEMPT_BACKOFF;
    }
    tx_abort_close(t, mutating);
    *result = finish_error(rs, rc);
    return ATTEMPT_RETURN;
}

/* Commit a mutating attempt whose body asked for it. */
static enum attempt_action commit_attempt(struct run_state *rs, struct daos_txn *t,
                                          enum mds_status body_st, enum mds_status *result)
{
    int rc = tx_commit(t);

    if (rc == 0) {
        stat_inc(&rs->b->stats.commits);
        tx_abort_close(t, false);
        *result = body_st;
        return ATTEMPT_RETURN;
    }
    if (rc == -DER_TX_RESTART) {
        if (restart_handle(rs, t, &rc)) {
            return ATTEMPT_RERUN;
        }
        tx_abort_close(t, true);
        *result = finish_error(rs, rc);
        return ATTEMPT_RETURN;
    }
    if (daos_err_is_transient(rc)) {
        /* The compound RPC may have been applied: never re-run, never
         * claim success.  The handle is dropped without an abort. */
        *result = finish_indoubt(rs, rc);
        tx_abort_close(t, false);
        return ATTEMPT_RETURN;
    }
    /* -DER_EXIST / -DER_NONEXIST from a conditional operation evaluated
     * at commit, or a definitive failure: nothing was applied. */
    tx_abort_close(t, true);
    *result = finish_error(rs, rc);
    return ATTEMPT_RETURN;
}

static enum attempt_action attempt_finish(struct run_state *rs, struct daos_txn *t, int rc,
                                          enum mds_status st, enum mds_status *result)
{
    if (rc == DAOS_BODY_DONE) {
        tx_abort_close(t, t->kind == DAOS_TXN_MUTATING);
        *result = st;
        return ATTEMPT_RETURN;
    }
    if (rc == DAOS_BODY_COMMIT) {
        if (t->kind != DAOS_TXN_MUTATING) {
            tx_abort_close(t, false);
            *result = st;
            return ATTEMPT_RETURN;
        }
        return commit_attempt(rs, t, st, result);
    }
    if (rc > 0) {
        /* Not a body code and not a -DER_* value: a body bug. */
        tx_abort_close(t, t->kind == DAOS_TXN_MUTATING);
        *result = finish_error(rs, -DER_INVAL);
        return ATTEMPT_RETURN;
    }
    return op_error(rs, t, rc, result);
}

enum mds_status daos_run_txn(struct daos_backend *b, enum daos_txn_kind kind, const char *op,
                             daos_txn_body body, void *ctx)
{
    struct daos_txn t;
    struct run_state rs;
    enum mds_status result = MDS_ERR_IO;

    if (b == NULL || body == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(&t, 0, sizeof(t));
    t.b = b;
    t.kind = kind;
    t.th = DAOS_TX_NONE;
    t.eqh = DAOS_HDL_INVAL;
    rs.b = b;
    rs.op = (op != NULL) ? op : "?";
    rs.deadline_ns = daos_txn_now_ns() + (uint64_t)b->op_deadline_ms * NS_PER_MS;
    rs.backoff_round = 0;
    if (kind != DAOS_TXN_NONE) {
        t.use_events = eq_borrow(&b->eqs, &t.eqh);
    }

    for (;;) {
        enum mds_status st = MDS_ERR_IO;
        enum attempt_action act;
        int rc;

        if (remaining_ms(&rs) < DAOS_MIN_ATTEMPT_MS) {
            result = finish_delay(&rs);
            break;
        }
        if (kind != DAOS_TXN_NONE && !daos_handle_is_valid(t.th)) {
            rc = tx_open(&t, kind == DAOS_TXN_READONLY ? DAOS_TF_RDONLY : 0);
            if (rc != 0) {
                if (daos_err_is_transient(rc)) {
                    stat_inc(&b->stats.transient_retries);
                    backoff(&rs);
                    continue;
                }
                result = finish_error(&rs, rc);
                break;
            }
        }
        stat_inc(&b->stats.attempts);
        rc = body(&t, ctx, &st);
        txn_objs_close(&t);
        act = attempt_finish(&rs, &t, rc, st, &result);
        if (act == ATTEMPT_RETURN) {
            break;
        }
        if (act == ATTEMPT_BACKOFF) {
            backoff(&rs);
        }
    }
    txn_objs_close(&t);
    tx_abort_close(&t, kind == DAOS_TXN_MUTATING);
    if (t.use_events) {
        eq_return(&b->eqs, t.eqh);
    }
    return result;
}

/* -----------------------------------------------------------------------
 * Id allocators
 * ----------------------------------------------------------------------- */

struct id_pool {
    uint64_t instance_seq;
    uint64_t next;
    uint32_t remaining;
};

static _Thread_local struct id_pool tl_pools[DAOS_META_KEY_COUNT];

struct refill_ctx {
    enum daos_meta_key counter;
    uint64_t           first; /**< First id of the reserved batch. */
};

/* Reserve the next batch: read the counter and advance it; two refills
 * of one counter conflict at commit and one restarts. */
static int refill_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct refill_ctx *c = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    uint8_t buf[8];
    uint8_t next[8];
    size_t len = 0;
    bool found = false;
    uint64_t cur = 0;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_META, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_meta(&dk, c->counter);
    daos_akey_value(&ak);
    rc = daos_fetch_single(t, oh, &dk, &ak, buf, sizeof(buf), &len, &found);
    if (rc != 0) {
        return rc;
    }
    if (!found || !fdb_le64_decode(buf, len, &cur)) {
        *st_out = MDS_ERR_IO; /* counter absent or corrupt: not bootstrapped */
        return DAOS_BODY_DONE;
    }
    if (cur > UINT64_MAX - MDS_FILEID_BATCH) {
        *st_out = MDS_ERR_NOSPC;
        return DAOS_BODY_DONE;
    }
    fdb_le64_put(next, cur + MDS_FILEID_BATCH);
    rc = daos_update_single(t, oh, &dk, &ak, next, sizeof(next), 0, 0);
    if (rc != 0) {
        return rc;
    }
    c->first = cur + 1U;
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/*
 * Reserve a fresh batch for @p pool.  An INDOUBT refill is reported as
 * MDS_ERR_DELAY: the batch it may have reserved is never handed out,
 * ids stay unique because the counter only advances, and the caller has
 * committed nothing user-visible yet, so it may retry.
 */
static enum mds_status pool_refill(struct daos_backend *b, enum daos_meta_key counter,
                                   struct id_pool *pool)
{
    struct refill_ctx c;
    enum mds_status st;

    memset(&c, 0, sizeof(c));
    c.counter = counter;
    st = daos_run_txn(b, DAOS_TXN_MUTATING, "alloc_id", refill_body, &c);
    if (st == MDS_ERR_INDOUBT) {
        MDS_LOG_WARN(LOG_COMP_CAT, "daos alloc_id: refill outcome unknown, the batch is "
                     "abandoned (a gap in counter %u): MDS_ERR_DELAY", (unsigned)counter);
        return MDS_ERR_DELAY;
    }
    if (st != MDS_OK) {
        return st;
    }
    pool->next = c.first;
    pool->remaining = MDS_FILEID_BATCH;
    return MDS_OK;
}

enum mds_status daos_backend_alloc_id(struct daos_backend *b, enum daos_meta_key counter,
                                      uint64_t *id)
{
    struct id_pool *pool;
    unsigned idx = (unsigned)counter;

    if (b == NULL || id == NULL || idx >= DAOS_META_KEY_COUNT || counter == DAOS_META_SCHEMA) {
        return MDS_ERR_INVAL;
    }
    pool = &tl_pools[idx];
    if (pool->instance_seq != b->instance_seq) {
        /* A different backend instance filled this pool: its ids belong
         * to another container, discard the remainder. */
        pool->instance_seq = b->instance_seq;
        pool->remaining = 0;
    }
    if (pool->remaining == 0) {
        enum mds_status st = pool_refill(b, counter, pool);

        if (st != MDS_OK) {
            return st;
        }
    }
    *id = pool->next;
    pool->next++;
    pool->remaining--;
    return MDS_OK;
}

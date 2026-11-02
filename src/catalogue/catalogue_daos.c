/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_daos.c -- DAOS catalogue backend: lifecycle.
 *
 * Owns the process-wide library state (daos_init / daos_fini are
 * reference counted; the first open initialises, the process shutdown
 * hook finalises when no handle is open), the per-handle pool and
 * container handles, the container attribute stamps and the root
 * bootstrap, and assembles the vtables from the slot units'
 * registration hooks.  Every object interaction is one daos_run_txn
 * (daos_txn.h); the container attributes are read and written through
 * daos_cont_get_attr / daos_cont_set_attr, outside any object.
 *
 * Open:      library init (first open) -> daos_pool_connect ->
 *            daos_cont_open -> read the six stamps: present -> every
 *            value must match the configuration (the class encodings
 *            become the handle's object-id classes); absent -> the
 *            handle opens unstamped and probe fails until bootstrap.
 * Probe:     stamps verified AND one DAOS_TXN_NONE fetch of META
 *            "schema" equal to DAOS_CAT_SCHEMA_VERSION.
 * Bootstrap: resolve the configured classes (the one place a live class
 *            resolution is allowed), then ONE mutating transaction:
 *            insert META "schema" with DAOS_COND_AKEY_INSERT (the atomic
 *            claim), write the four counters and the root inode.  The
 *            claim's winner stamps the attributes and re-reads them; a
 *            loser (-DER_EXIST from the call or the commit) adopts the
 *            winner's stamps when they match its configuration, waits
 *            (MDS_ERR_DELAY) while the winner is still stamping, and
 *            refuses (MDS_ERR_INVAL) a different configuration.
 * Close:     container close, pool disconnect, event-queue pool destroy,
 *            free; the dispatcher frees struct mds_catalogue (C7).
 *
 * The entity class must be a single-group class (G1 / S1): sorted dkey
 * enumeration -- cookie order, GC and manifest queues -- holds only
 * within one redundancy group, and the META object every daemon claims
 * at bootstrap must have the same id before any stamp exists, which a
 * GX class (group count derived from the pool size) cannot guarantee.
 */

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <daos.h>

#include "catalogue_daos.h"
#include "catalogue_daos_internal.h"
#include "catalogue_internal.h"
#include "daos_codec.h"
#include "daos_keys.h"
#include "daos_txn.h"
#include "fdb_codec.h"
#include "mds_log.h"

/* Bootstrap value of the READDIR cookie allocator.  daos_backend_alloc_id
 * hands out counter + 1 onwards, so the first cookie ever minted is 16,
 * clear of the cookie values NFSv4 reserves (0 restarts a listing; 1 and
 * 2 historically stood for "." and ".."). */
#define DAOS_COOKIE_SEQ_SEED 15U

/* Number of container attributes stamped at bootstrap. */
#define DAOS_ATTR_COUNT 6

/* -----------------------------------------------------------------------
 * Process-wide library state
 * ----------------------------------------------------------------------- */

static pthread_mutex_t g_lib_lock = PTHREAD_MUTEX_INITIALIZER;
static bool            g_lib_ready;
static bool            g_hooks_registered;
static uint32_t        g_open_handles;
static uint64_t        g_next_instance_seq = 1;

static void lib_atexit(void)
{
    catalogue_daos_process_shutdown();
}

/* Initialise the library when needed and count a handle. */
static enum mds_status lib_acquire(void)
{
    enum mds_status st = MDS_OK;

    (void)pthread_mutex_lock(&g_lib_lock);
    if (!g_lib_ready) {
        int rc = daos_init();

        if (rc != 0) {
            MDS_LOG_ERROR(LOG_COMP_CAT, "daos_init: %d %s", rc, daos_errstr(rc));
            st = MDS_ERR_IO;
        } else {
            g_lib_ready = true;
        }
    }
    if (st == MDS_OK) {
        g_open_handles++;
        if (!g_hooks_registered) {
            (void)mds_catalogue_register_process_shutdown(catalogue_daos_process_shutdown);
            (void)atexit(lib_atexit);
            g_hooks_registered = true;
        }
    }
    (void)pthread_mutex_unlock(&g_lib_lock);
    return st;
}

static void lib_release(void)
{
    (void)pthread_mutex_lock(&g_lib_lock);
    if (g_open_handles > 0) {
        g_open_handles--;
    }
    (void)pthread_mutex_unlock(&g_lib_lock);
}

void catalogue_daos_process_shutdown(void)
{
    bool fini = false;

    (void)pthread_mutex_lock(&g_lib_lock);
    if (g_lib_ready) {
        if (g_open_handles > 0) {
            MDS_LOG_ERROR(LOG_COMP_CAT, "daos: process shutdown requested with %u catalogue "
                          "handle(s) still open; library left initialised",
                          (unsigned)g_open_handles);
        } else {
            g_lib_ready = false;
            fini = true;
        }
    }
    (void)pthread_mutex_unlock(&g_lib_lock);
    if (fini) {
        int rc = daos_fini();

        if (rc != 0) {
            MDS_LOG_ERROR(LOG_COMP_CAT, "daos_fini: %d %s", rc, daos_errstr(rc));
        }
    }
}

/* -----------------------------------------------------------------------
 * Handle helpers
 * ----------------------------------------------------------------------- */

static struct daos_backend *daos_of(const struct mds_catalogue *cat)
{
    if (cat == NULL || cat->backend != MDS_BACKEND_DAOS) {
        return NULL;
    }
    return cat->backend_private;
}

static bool backend_stamped(const struct daos_backend *b)
{
    return b->cid_entity != OC_UNKNOWN && b->cid_spread != OC_UNKNOWN;
}

/* -----------------------------------------------------------------------
 * Container attribute stamps
 * ----------------------------------------------------------------------- */

/** The stamps, decoded. */
struct daos_stamps {
    uint64_t         schema;
    char             oclass[32];
    char             oclass_spread[32];
    daos_oclass_id_t cid_entity;
    daos_oclass_id_t cid_spread;
    uint32_t         dir_shards;
};

static const char *const g_attr_names[DAOS_ATTR_COUNT] = {
    DAOS_CAT_ATTR_SCHEMA,
    DAOS_CAT_ATTR_OCLASS,
    DAOS_CAT_ATTR_OCLASS_SPREAD,
    DAOS_CAT_ATTR_OCLASS_ID,
    DAOS_CAT_ATTR_OCLASS_SPREAD_ID,
    DAOS_CAT_ATTR_DIR_SHARDS,
};

/* Copy a NUL-terminated attribute value of @p len bytes into @p dst. */
static bool attr_string(const uint8_t *buf, size_t len, char *dst, size_t cap)
{
    if (len == 0 || len > cap || buf[len - 1] != '\0' ||
        strnlen((const char *)buf, len) != len - 1) {
        return false;
    }
    memcpy(dst, buf, len);
    return true;
}

/*
 * Read the stamps.  *present is false when the container carries none
 * (daos_cont_get_attr reports -DER_NONEXIST only when every requested
 * attribute is absent; a partial set is corrupt and reported as INVAL).
 */
static enum mds_status stamps_read(const struct daos_backend *b, struct daos_stamps *s,
                                   bool *present)
{
    uint8_t schema_buf[8];
    uint8_t ent_name[32];
    uint8_t spr_name[32];
    uint8_t ent_id[4];
    uint8_t spr_id[4];
    uint8_t shards[4];
    void *const bufs[DAOS_ATTR_COUNT] = { schema_buf, ent_name, spr_name, ent_id, spr_id,
                                          shards };
    size_t sizes[DAOS_ATTR_COUNT] = { sizeof(schema_buf), sizeof(ent_name), sizeof(spr_name),
                                      sizeof(ent_id), sizeof(spr_id), sizeof(shards) };
    uint32_t id_ent = 0;
    uint32_t id_spr = 0;
    int rc;
    int i;

    *present = false;
    memset(s, 0, sizeof(*s));
    rc = daos_cont_get_attr(b->coh, DAOS_ATTR_COUNT, g_attr_names, bufs, sizes, NULL);
    if (rc == -DER_NONEXIST) {
        return MDS_OK;
    }
    if (rc != 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos_cont_get_attr: %d %s", rc, daos_errstr(rc));
        return MDS_ERR_IO;
    }
    for (i = 0; i < DAOS_ATTR_COUNT; i++) {
        if (sizes[i] == 0) {
            MDS_LOG_ERROR(LOG_COMP_CAT, "daos: container attribute %s missing while others "
                          "are present; stamps corrupt", g_attr_names[i]);
            return MDS_ERR_INVAL;
        }
    }
    if (!fdb_le64_decode(schema_buf, sizes[0], &s->schema) ||
        !attr_string(ent_name, sizes[1], s->oclass, sizeof(s->oclass)) ||
        !attr_string(spr_name, sizes[2], s->oclass_spread, sizeof(s->oclass_spread)) ||
        !fdb_le32_decode(ent_id, sizes[3], &id_ent) ||
        !fdb_le32_decode(spr_id, sizes[4], &id_spr) ||
        !fdb_le32_decode(shards, sizes[5], &s->dir_shards)) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos: container attributes do not decode; stamps corrupt");
        return MDS_ERR_INVAL;
    }
    s->cid_entity = (daos_oclass_id_t)id_ent;
    s->cid_spread = (daos_oclass_id_t)id_spr;
    *present = true;
    return MDS_OK;
}

/* Refuse a container stamped for another configuration; name the
 * mismatch.  On success the stamped class encodings become the handle's. */
static enum mds_status stamps_adopt(struct daos_backend *b, const struct daos_stamps *s)
{
    if (s->schema != DAOS_CAT_SCHEMA_VERSION) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos: container schema %llu, this build supports %u",
                      (unsigned long long)s->schema, DAOS_CAT_SCHEMA_VERSION);
        return MDS_ERR_INVAL;
    }
    if (strcmp(s->oclass, b->oclass_name) != 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos: container stamped daos_oclass=%s, configured %s",
                      s->oclass, b->oclass_name);
        return MDS_ERR_INVAL;
    }
    if (strcmp(s->oclass_spread, b->oclass_spread_name) != 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos: container stamped daos_oclass_spread=%s, "
                      "configured %s", s->oclass_spread, b->oclass_spread_name);
        return MDS_ERR_INVAL;
    }
    if (s->dir_shards != b->dir_shards) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos: container stamped daos_dir_counter_shards=%u, "
                      "configured %u", s->dir_shards, b->dir_shards);
        return MDS_ERR_INVAL;
    }
    if (s->cid_entity == OC_UNKNOWN || s->cid_spread == OC_UNKNOWN) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos: container class encodings are unset; stamps corrupt");
        return MDS_ERR_INVAL;
    }
    b->cid_entity = s->cid_entity;
    b->cid_spread = s->cid_spread;
    return MDS_OK;
}

/* Write the six stamps in one call (one metadata transaction). */
static enum mds_status stamps_write(const struct daos_backend *b)
{
    uint8_t schema_buf[8];
    uint8_t ent_id[4];
    uint8_t spr_id[4];
    uint8_t shards[4];
    const void *const vals[DAOS_ATTR_COUNT] = { schema_buf, b->oclass_name,
                                                b->oclass_spread_name, ent_id, spr_id, shards };
    const size_t sizes[DAOS_ATTR_COUNT] = { sizeof(schema_buf), strlen(b->oclass_name) + 1U,
                                            strlen(b->oclass_spread_name) + 1U,
                                            sizeof(ent_id), sizeof(spr_id), sizeof(shards) };
    int rc;

    fdb_le64_put(schema_buf, DAOS_CAT_SCHEMA_VERSION);
    fdb_le32_put(ent_id, (uint32_t)b->cid_entity);
    fdb_le32_put(spr_id, (uint32_t)b->cid_spread);
    fdb_le32_put(shards, b->dir_shards);
    rc = daos_cont_set_attr(b->coh, DAOS_ATTR_COUNT, g_attr_names, vals, sizes, NULL);
    if (rc != 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos_cont_set_attr: %d %s", rc, daos_errstr(rc));
        return MDS_ERR_IO;
    }
    return MDS_OK;
}

/* Read the stamps and adopt them.  *present false: none stamped. */
static enum mds_status stamps_verify(struct daos_backend *b, bool *present)
{
    struct daos_stamps s;
    enum mds_status st = stamps_read(b, &s, present);

    if (st != MDS_OK || !*present) {
        return st;
    }
    return stamps_adopt(b, &s);
}

/* -----------------------------------------------------------------------
 * Schema probe body (DAOS_TXN_NONE: one fetch)
 * ----------------------------------------------------------------------- */

struct schema_ctx {
    bool     present;
    uint64_t version;
};

static int schema_read_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    struct schema_ctx *sc = arg;
    struct daos_key dk;
    struct daos_key ak;
    daos_handle_t oh;
    uint8_t buf[8];
    size_t len = 0;
    bool found = false;
    int rc;

    rc = daos_txn_obj(t, DAOS_TAG_META, 0, &oh);
    if (rc != 0) {
        return rc;
    }
    daos_dkey_meta(&dk, DAOS_META_SCHEMA);
    daos_akey_value(&ak);
    rc = daos_fetch_single(t, oh, &dk, &ak, buf, sizeof(buf), &len, &found);
    if (rc != 0) {
        return rc;
    }
    sc->present = found;
    sc->version = 0;
    if (found && !fdb_le64_decode(buf, len, &sc->version)) {
        *st_out = MDS_ERR_IO; /* corrupt stamp */
        return DAOS_BODY_DONE;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* -----------------------------------------------------------------------
 * Bootstrap body (one mutating transaction)
 * ----------------------------------------------------------------------- */

/* Write META @p key = @p value (LE u64); @p cond is 0 or DAOS_COND_AKEY_INSERT. */
static int meta_counter_write(struct daos_txn *t, daos_handle_t oh, enum daos_meta_key key,
                              uint64_t value, uint64_t cond)
{
    struct daos_key dk;
    struct daos_key ak;
    uint8_t v[8];

    daos_dkey_meta(&dk, key);
    daos_akey_value(&ak);
    fdb_le64_put(v, value);
    return daos_update_single(t, oh, &dk, &ak, v, sizeof(v), 0, cond);
}

static int bootstrap_body(struct daos_txn *t, void *arg, enum mds_status *st_out)
{
    daos_handle_t meta;
    daos_handle_t root_oh;
    struct mds_inode root;
    struct timespec now;
    int rc;

    (void)arg;
    rc = daos_txn_obj(t, DAOS_TAG_META, 0, &meta);
    if (rc != 0) {
        return rc;
    }
    rc = daos_txn_obj(t, DAOS_TAG_INODE, MDS_FILEID_ROOT, &root_oh);
    if (rc != 0) {
        return rc;
    }
    if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
        *st_out = MDS_ERR_IO;
        return DAOS_BODY_DONE;
    }
    /* The atomic claim: a second bootstrap fails here or at commit with
     * -DER_EXIST and nothing else of this transaction is applied. */
    rc = meta_counter_write(t, meta, DAOS_META_SCHEMA, DAOS_CAT_SCHEMA_VERSION,
                            DAOS_COND_AKEY_INSERT);
    if (rc != 0) {
        return rc;
    }
    /* Allocators hand out counter + 1 ..: fileids start after the root,
     * READDIR cookies at DAOS_COOKIE_SEQ_SEED + 1, GC and remove
     * sequences at 1. */
    rc = meta_counter_write(t, meta, DAOS_META_FILEID, MDS_FILEID_ROOT, 0);
    if (rc == 0) {
        rc = meta_counter_write(t, meta, DAOS_META_COOKIE_SEQ, DAOS_COOKIE_SEQ_SEED, 0);
    }
    if (rc == 0) {
        rc = meta_counter_write(t, meta, DAOS_META_GC_SEQ, 0, 0);
    }
    if (rc == 0) {
        rc = meta_counter_write(t, meta, DAOS_META_REMOVE_SEQ, 0, 0);
    }
    if (rc != 0) {
        return rc;
    }
    memset(&root, 0, sizeof(root));
    root.fileid = MDS_FILEID_ROOT;
    root.type = MDS_FTYPE_DIR;
    root.mode = 0755;
    root.nlink = 2;
    root.atime = now;
    root.mtime = now;
    root.ctime = now;
    root.change = 1;
    root.generation = 1;
    rc = daos_inode_write_full(t, root_oh, &root, true);
    if (rc != 0) {
        return rc;
    }
    *st_out = MDS_OK;
    return DAOS_BODY_COMMIT;
}

/* -----------------------------------------------------------------------
 * Object classes
 * ----------------------------------------------------------------------- */

/* Resolve the configured class @p cid for objects of @p type to the
 * encoding daos_obj_generate_oid() would stamp today.  Bootstrap only. */
static enum mds_status oclass_resolve(const struct daos_backend *b, enum daos_otype_t type,
                                      daos_oclass_id_t cid, const char *what,
                                      daos_oclass_id_t *resolved)
{
    daos_obj_id_t oid = { 0, 0 };
    int rc = daos_obj_generate_oid(b->coh, &oid, type, cid, 0, 0);

    if (rc != 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos: object class %s cannot be placed in this pool: "
                      "%d %s (fewer fault domains than the class needs?)", what, rc,
                      daos_errstr(rc));
        return MDS_ERR_INVAL;
    }
    *resolved = daos_obj_id2class(oid);
    if (*resolved == OC_UNKNOWN) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos: object class %s resolved to no encoding", what);
        return MDS_ERR_INVAL;
    }
    return MDS_OK;
}

/* Parse a class name; the entity class must be a single-group explicit
 * class (see the file comment). */
static enum mds_status oclass_parse(const char *name, bool entity, daos_oclass_id_t *cid)
{
    int id = daos_oclass_name2id(name);

    if (id <= 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos: unknown object class '%s'", name);
        return MDS_ERR_INVAL;
    }
    if (entity && ((uint32_t)id < ((uint32_t)OR_RP_1 << OC_REDUN_SHIFT) ||
                   ((uint32_t)id & (uint32_t)MAX_NUM_GROUPS) != 1U)) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos_oclass '%s' is not a single-group class; use a "
                      "G1 / S1 class (sorted enumeration and the bootstrap claim need one "
                      "redundancy group)", name);
        return MDS_ERR_INVAL;
    }
    *cid = (daos_oclass_id_t)id;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Lifecycle slots
 * ----------------------------------------------------------------------- */

static void backend_disconnect(struct daos_backend *b)
{
    int rc;

    daos_eq_pool_destroy(&b->eqs);
    if (daos_handle_is_valid(b->coh)) {
        rc = daos_cont_close(b->coh, NULL);
        if (rc != 0) {
            MDS_LOG_ERROR(LOG_COMP_CAT, "daos_cont_close: %d %s", rc, daos_errstr(rc));
        }
        b->coh = DAOS_HDL_INVAL;
    }
    if (daos_handle_is_valid(b->poh)) {
        rc = daos_pool_disconnect(b->poh, NULL);
        if (rc != 0) {
            MDS_LOG_ERROR(LOG_COMP_CAT, "daos_pool_disconnect: %d %s", rc, daos_errstr(rc));
        }
        b->poh = DAOS_HDL_INVAL;
    }
}

static void daos_close(struct mds_catalogue *cat)
{
    struct daos_backend *b = daos_of(cat);

    if (b == NULL) {
        return;
    }
    cat->backend_private = NULL;
    backend_disconnect(b);
    free(b);
    lib_release();
}

static enum mds_status daos_probe(struct mds_catalogue *cat)
{
    struct daos_backend *b = daos_of(cat);
    struct schema_ctx sc;
    enum mds_status st;

    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    if (!backend_stamped(b)) {
        bool present = false;

        /* Another daemon may have bootstrapped since open. */
        st = stamps_verify(b, &present);
        if (st != MDS_OK) {
            return st;
        }
        if (!present) {
            return MDS_ERR_IO; /* not bootstrapped */
        }
    }
    memset(&sc, 0, sizeof(sc));
    st = daos_run_txn(b, DAOS_TXN_NONE, "probe", schema_read_body, &sc);
    if (st != MDS_OK) {
        return st;
    }
    if (!sc.present) {
        return MDS_ERR_IO; /* stamped but the META row is missing */
    }
    return (sc.version == DAOS_CAT_SCHEMA_VERSION) ? MDS_OK : MDS_ERR_INVAL;
}

/* The claim was lost (or the container was bootstrapped before): adopt
 * the winner's stamps when they match, wait while it is still stamping. */
static enum mds_status bootstrap_adopt(struct daos_backend *b)
{
    bool present = false;
    enum mds_status st = stamps_verify(b, &present);

    if (st == MDS_OK && !present) {
        MDS_LOG_INFO(LOG_COMP_CAT, "daos: another daemon is bootstrapping this container; "
                     "retry");
        st = MDS_ERR_DELAY;
    }
    if (st != MDS_OK) {
        b->cid_entity = OC_UNKNOWN;
        b->cid_spread = OC_UNKNOWN;
    }
    return st;
}

static enum mds_status daos_bootstrap(struct mds_catalogue *cat)
{
    struct daos_backend *b = daos_of(cat);
    daos_oclass_id_t cfg_ent;
    daos_oclass_id_t cfg_spr;
    bool live = false;
    bool present = false;
    enum mds_status st;

    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    if (!backend_stamped(b)) {
        /* The one place a live class resolution is allowed: the claim
         * below decides whose resolution the container keeps. */
        st = oclass_parse(b->oclass_name, true, &cfg_ent);
        if (st == MDS_OK) {
            st = oclass_parse(b->oclass_spread_name, false, &cfg_spr);
        }
        if (st == MDS_OK) {
            st = oclass_resolve(b, DAOS_OT_DKEY_LEXICAL, cfg_ent, "daos_oclass", &b->cid_entity);
        }
        if (st == MDS_OK) {
            st = oclass_resolve(b, DAOS_OT_MULTI_HASHED, cfg_spr, "daos_oclass_spread",
                                &b->cid_spread);
        }
        if (st != MDS_OK) {
            b->cid_entity = OC_UNKNOWN;
            b->cid_spread = OC_UNKNOWN;
            return st;
        }
        live = true;
    }
    st = daos_run_txn(b, DAOS_TXN_MUTATING, "bootstrap", bootstrap_body, NULL);
    if (st == MDS_ERR_EXISTS) {
        return bootstrap_adopt(b);
    }
    if (st != MDS_OK) {
        if (live) {
            b->cid_entity = OC_UNKNOWN;
            b->cid_spread = OC_UNKNOWN;
        }
        return st;
    }
    /* The claim is ours: stamp and confirm. */
    st = stamps_write(b);
    if (st == MDS_OK) {
        st = stamps_verify(b, &present);
        if (st == MDS_OK && !present) {
            MDS_LOG_ERROR(LOG_COMP_CAT, "daos: container attributes absent right after "
                          "stamping");
            st = MDS_ERR_IO;
        }
    }
    if (st != MDS_OK) {
        b->cid_entity = OC_UNKNOWN;
        b->cid_spread = OC_UNKNOWN;
    }
    return st;
}

static const struct mds_catalogue_ops daos_lifecycle_ops = {
    .close            = daos_close,
    .probe            = daos_probe,
    .bootstrap        = daos_bootstrap,
    .backend_handle   = NULL,
    .image_feed_start = NULL,
    .image_feed_stop  = NULL,
};

/* -----------------------------------------------------------------------
 * Client-side counters (mds_cat_backend_client_stats)
 *
 * The runner's counters (daos_txn.h) in the backend-neutral shape the
 * RonDB shim fills from the NDB API:
 *   exec_waits     blocking network waits (a parallel fetch wave counts
 *                  once; local handle operations are not counted)
 *   txn_started    body executions (attempts), read-only ones included
 *   txn_committed  commits reported committed
 *   txn_aborted    -DER_TX_RESTART re-runs
 *   txn_closed     attempts that did not commit
 *   client_objects 1 (one container handle)
 * The remaining fields have no DAOS counterpart and stay 0.
 * ----------------------------------------------------------------------- */

static uint64_t stat_read(const _Atomic uint64_t *ctr)
{
    return atomic_load_explicit(ctr, memory_order_relaxed);
}

static enum mds_status daos_backend_client_stats(struct mds_catalogue *cat,
                                                 struct mds_cat_backend_client_stats *out)
{
    struct daos_backend *b = daos_of(cat);
    uint64_t attempts;
    uint64_t commits;

    if (b == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }
    attempts = stat_read(&b->stats.attempts);
    commits = stat_read(&b->stats.commits);
    memset(out, 0, sizeof(*out));
    out->exec_waits = stat_read(&b->stats.waits);
    out->txn_started = attempts;
    out->txn_committed = commits;
    out->txn_aborted = stat_read(&b->stats.restarts);
    out->txn_closed = (attempts > commits) ? attempts - commits : 0;
    out->client_objects = 1;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Vtables.  The authority table is assembled once per process from the
 * slot units' registration hooks (under g_lib_lock, before any handle
 * is published) and then only ever read.  The coordination and cluster
 * tables are constant objects exported by catalogue_daos_coord.c and
 * catalogue_daos_cluster.c.
 * ----------------------------------------------------------------------- */

static struct mds_authority_ops g_auth_ops;
static bool                     g_tables_ready;

static void tables_assemble(void)
{
    (void)pthread_mutex_lock(&g_lib_lock);
    if (!g_tables_ready) {
        memset(&g_auth_ops, 0, sizeof(g_auth_ops));
        catalogue_daos_ns_register(&g_auth_ops);
        catalogue_daos_ext_register(&g_auth_ops);
        g_auth_ops.backend_client_stats = daos_backend_client_stats;
        g_tables_ready = true;
    }
    (void)pthread_mutex_unlock(&g_lib_lock);
}

/* -----------------------------------------------------------------------
 * Constructor
 * ----------------------------------------------------------------------- */

/* Copy @p src (or @p def when empty) into @p dst; false when it does not fit. */
static bool cfg_string(char *dst, size_t cap, const char *src, size_t src_cap, const char *def)
{
    size_t n = strnlen(src, src_cap);

    if (n == 0) {
        src = def;
        n = strlen(def);
    }
    if (n >= cap) {
        return false;
    }
    memcpy(dst, src, n);
    dst[n] = '\0';
    return true;
}

static enum mds_status backend_new(const struct mds_config *cfg, struct daos_backend **out)
{
    struct daos_backend *b;
    enum mds_status st;

    *out = NULL;
    if (cfg->daos_pool[0] == '\0' || cfg->daos_cont[0] == '\0') {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos_pool and daos_cont must both be set");
        return MDS_ERR_INVAL;
    }
    b = calloc(1, sizeof(*b));
    if (b == NULL) {
        return MDS_ERR_NOMEM;
    }
    if (!cfg_string(b->pool, sizeof(b->pool), cfg->daos_pool, sizeof(cfg->daos_pool), "") ||
        !cfg_string(b->cont, sizeof(b->cont), cfg->daos_cont, sizeof(cfg->daos_cont), "") ||
        !cfg_string(b->sys, sizeof(b->sys), cfg->daos_sys, sizeof(cfg->daos_sys),
                    DAOS_SYS_DEFAULT) ||
        !cfg_string(b->oclass_name, sizeof(b->oclass_name), cfg->daos_oclass,
                    sizeof(cfg->daos_oclass), DAOS_OCLASS_DEFAULT) ||
        !cfg_string(b->oclass_spread_name, sizeof(b->oclass_spread_name),
                    cfg->daos_oclass_spread, sizeof(cfg->daos_oclass_spread),
                    DAOS_OCLASS_SPREAD_DEFAULT)) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos: a daos_* configuration string is too long");
        free(b);
        return MDS_ERR_INVAL;
    }
    b->dir_shards = cfg->daos_dir_counter_shards != 0 ? cfg->daos_dir_counter_shards :
                                                         DAOS_DIR_SHARDS_DEFAULT;
    if (b->dir_shards > DAOS_DIR_SHARDS_MAX) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos_dir_counter_shards %u above %u", b->dir_shards,
                      DAOS_DIR_SHARDS_MAX);
        free(b);
        return MDS_ERR_INVAL;
    }
    b->mds_id = cfg->self.id;
    b->op_deadline_ms = cfg->daos_op_deadline_ms != 0 ? cfg->daos_op_deadline_ms :
                                                        DAOS_OP_DEADLINE_MS_DEFAULT;
    b->poh = DAOS_HDL_INVAL;
    b->coh = DAOS_HDL_INVAL;
    st = daos_eq_pool_init(&b->eqs, cfg->worker_threads);
    if (st != MDS_OK) {
        free(b);
        return st;
    }
    *out = b;
    return MDS_OK;
}

/* Validate the class names (the library is initialised), connect and
 * open; an unreachable pool fails the open instead of the first slot. */
static enum mds_status open_connect(struct daos_backend *b)
{
    daos_oclass_id_t cid;
    enum mds_status st;
    int rc;

    st = oclass_parse(b->oclass_name, true, &cid);
    if (st == MDS_OK) {
        st = oclass_parse(b->oclass_spread_name, false, &cid);
    }
    if (st != MDS_OK) {
        return st;
    }
    rc = daos_pool_connect(b->pool, b->sys, DAOS_PC_RW, &b->poh, NULL, NULL);
    if (rc != 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos_pool_connect(%s, %s): %d %s", b->pool, b->sys, rc,
                      daos_errstr(rc));
        b->poh = DAOS_HDL_INVAL;
        return MDS_ERR_IO;
    }
    rc = daos_cont_open(b->poh, b->cont, DAOS_COO_RW, &b->coh, NULL, NULL);
    if (rc != 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT, "daos_cont_open(%s): %d %s", b->cont, rc, daos_errstr(rc));
        b->coh = DAOS_HDL_INVAL;
        return MDS_ERR_IO;
    }
    (void)pthread_mutex_lock(&g_lib_lock);
    b->instance_seq = g_next_instance_seq++;
    (void)pthread_mutex_unlock(&g_lib_lock);
    return MDS_OK;
}

enum mds_status catalogue_daos_open(const struct mds_config *cfg, struct mds_catalogue **out)
{
    struct mds_catalogue *cat = NULL;
    struct daos_backend *b = NULL;
    bool present = false;
    enum mds_status st;

    if (cfg == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }
    *out = NULL;

    st = backend_new(cfg, &b);
    if (st != MDS_OK) {
        return st;
    }
    st = lib_acquire();
    if (st != MDS_OK) {
        daos_eq_pool_destroy(&b->eqs);
        free(b);
        return st;
    }
    tables_assemble();

    st = open_connect(b);
    if (st != MDS_OK) {
        goto fail;
    }
    st = stamps_verify(b, &present);
    if (st != MDS_OK) {
        goto fail;
    }
    if (!present) {
        MDS_LOG_INFO(LOG_COMP_CAT, "daos: container %s not bootstrapped; probe fails until "
                     "bootstrap runs", b->cont);
    }

    cat = calloc(1, sizeof(*cat));
    if (cat == NULL) {
        st = MDS_ERR_NOMEM;
        goto fail;
    }
    cat->backend = MDS_BACKEND_DAOS;
    /* One container shared by every MDS process: an inode is visible
     * from all of them and registry rows written by one daemon are
     * observed by another. */
    cat->caps = MDS_CAT_CAP_SHARED_AUTHORITY | MDS_CAT_CAP_MULTI_PROCESS;
    cat->self_mds_id = cfg->self.id;
    cat->ops = &daos_lifecycle_ops;
    cat->auth_ops = &g_auth_ops;
    cat->coord_ops = &daos_coordination_ops; /* catalogue_daos_coord.c */
    cat->cluster_ops = &daos_cluster_ops;    /* catalogue_daos_cluster.c */
    cat->backend_private = b;
    *out = cat;
    return MDS_OK;

fail:
    backend_disconnect(b);
    free(b);
    lib_release();
    return st;
}

enum mds_status daos_backend_set_boot_epoch(const struct mds_catalogue *cat,
                                            uint64_t boot_epoch)
{
    struct daos_backend *b = daos_of(cat);

    if (b == NULL) {
        return MDS_ERR_INVAL;
    }
    atomic_store(&b->boot_epoch, boot_epoch);
    return MDS_OK;
}

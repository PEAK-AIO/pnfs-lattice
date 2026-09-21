/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_memdb.c -- In-memory catalogue backend (reference
 * implementation, non-durable, single-node).
 *
 * One process, one instance, no durability: every table is a heap
 * array owned by the instance and is gone when the handle is closed.
 * The backend is the reference implementation of the slot contract in
 * catalogue_internal.h (C1-C7): every slot the RonDB backend
 * populates in its authority, coordination and cluster tables has an
 * honest bounded-table implementation here, so the write-through
 * paths the daemon runs on RonDB run unchanged on
 * `catalogue_backend = memdb` and the unit tests exercise real state.
 *
 * Concurrency.  Exactly one mutex per instance guards every slot body.
 * Nothing is ever called back while the mutex is held: an enumerating
 * slot materialises a bounded page under the lock, releases it, then
 * delivers the page (C1), so a callback may re-enter the same handle
 * from the same thread.  A non-zero callback return stops delivery.
 * Each page is a consistent snapshot; a row created or removed between
 * two pages is seen once or not at all (C2).
 *
 * Capacity.  Every table has a compile-time bound (MEMDB_MAX_*) and is
 * allocated once at open.  A slot that would exceed a bound returns
 * MDS_ERR_NOSPC before it mutates anything, so a full table never
 * leaves a dangling dirent or a bumped parent counter behind.  Lookups
 * are linear scans of the bounded tables; there is no hashing and no
 * tunable, by design.
 *
 * Ownership.  The backend never retains caller heap pointers: inodes
 * are stored with ds_map = NULL; xattr values, inline data, stripe
 * entries, layout DS lists and cached DRC replies are private copies
 * freed on delete, overwrite and close.  ops->close releases only the
 * backend state; the dispatcher frees struct mds_catalogue (C7).
 *
 * Capabilities.  MDS_CAT_CAP_SHARED_AUTHORITY only.  The store is one
 * in-process image, so MDS_CAT_CAP_MULTI_PROCESS is never set and
 * mds_cluster_supported() stays false even though cluster_ops is
 * populated for in-process multi-MDS tests.
 *
 * Usage:
 *   struct mds_catalogue *cat = catalogue_memdb_open();
 *   ... mds_cat_* / mds_coord_* / mds_cluster_* as on any backend ...
 *   mds_catalogue_close(cat);
 */

#include <assert.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "pnfs_mds.h"
#include "layout_ds_ids.h"
#include "layout_range.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "mds_cluster.h"
#include "open_state.h"
#include "quota.h"
#include "catalogue_internal.h"
#include "catalogue_memdb.h"

/* -----------------------------------------------------------------------
 * Compile-time table bounds
 *
 * Each bound is the number of rows the heap table allocated at open
 * holds.  Exceeding one is MDS_ERR_NOSPC, reported before anything is
 * mutated.  The values are sized for unit tests and the single-node
 * daemon smoke, not for production data; a store that needs more is a
 * different backend, not a bigger constant.
 * ----------------------------------------------------------------------- */

#define MEMDB_MAX_INODES          4096U
#define MEMDB_MAX_DIRENTS         4096U
#define MEMDB_MAX_INLINE           256U
#define MEMDB_MAX_XATTRS          1024U
#define MEMDB_MAX_STRIPE_MAPS      256U
#define MEMDB_MAX_DS                64U
#define MEMDB_MAX_PROVISION         64U
#define MEMDB_MAX_QUOTA_RULES       64U
#define MEMDB_MAX_QUOTA_USAGE      256U
#define MEMDB_MAX_GC               256U
#define MEMDB_MAX_REMOVE_PENDING   256U
#define MEMDB_MAX_SHARD_FIDS       256U
#define MEMDB_MAX_EXT_DIRENTS       64U
#define MEMDB_MAX_LINK_ANCHORS      64U
#define MEMDB_MAX_JOURNAL           64U
#define MEMDB_MAX_LAYOUTS          256U
#define MEMDB_MAX_RECOVERY          64U
#define MEMDB_MAX_OPENS           1024U
#define MEMDB_MAX_LOCKS           1024U
#define MEMDB_MAX_DELEGS           256U
#define MEMDB_MAX_CLIENTS          256U
#define MEMDB_MAX_SESSIONS         256U
#define MEMDB_MAX_DRC_SLOTS       4096U
#define MEMDB_MAX_NODES           ((uint32_t)MDS_MAX_NODES)
#define MEMDB_MAX_PARTITIONS       256U

/* Rows materialised per callback batch by the enumerating slots. */
#define MEMDB_SCAN_PAGE             64U

/* First READDIR cookie a dirent may carry.  0, 1 and 2 are reserved
 * (struct mds_cat_dirent in mds_catalogue.h) and the dispatcher refuses
 * anything below 3; the per-instance cookie sequence starts here. */
#define MEMDB_COOKIE_FIRST           3U
_Static_assert(MEMDB_COOKIE_FIRST > 2U, "READDIR cookies 0, 1 and 2 are reserved");

/* Provisioning secrets are bounded like the RonDB column. */
#define MEMDB_SECRET_MAX            64U

/* Node-registry hostname column (RonDB: VARCHAR(255)). */
#define MEMDB_HOSTNAME_MAX         255U

/* LAYOUTIOMODE4_RW.  compound.h is not included here (it drags in the
 * RPC headers); the union slot keeps a stateid ever granted RW at RW,
 * exactly like the RonDB renewal union. */
#define MEMDB_LAYOUTIOMODE_RW        2U

/* RFC 8881 nfs_lock_type4.  lock_state.h is not included here (it
 * drags in compound.h); the blocking variants normalise to their
 * plain type for conflict purposes. */
#define MEMDB_READ_LT                1U
#define MEMDB_WRITE_LT               2U
#define MEMDB_READW_LT               3U
#define MEMDB_WRITEW_LT              4U

/* Every table index fits an int so "-1 = not found" is unambiguous. */
_Static_assert(MEMDB_MAX_DRC_SLOTS < 0x7FFFFFFFU, "table bounds must fit int");
_Static_assert(MEMDB_MAX_INODES < 0x7FFFFFFFU, "table bounds must fit int");

/* -----------------------------------------------------------------------
 * Row types.  Every row starts with a `used` flag; a cleared flag is a
 * free slot.  Heap members are private copies owned by the row.
 * ----------------------------------------------------------------------- */

struct memdb_inode {
    bool             used;
    struct mds_inode ino;          /* ds_map is always NULL in the store */
};

struct memdb_dirent {
    bool     used;
    uint64_t parent;
    uint64_t child_fileid;
    uint64_t cookie;               /* per-instance sequence, >= MEMDB_COOKIE_FIRST */
    uint8_t  child_type;
    char     name[MDS_MAX_NAME + 1];
};

struct memdb_inline {
    bool     used;
    uint64_t fileid;
    uint8_t *data;                 /* heap, len bytes; NULL when len == 0 */
    uint32_t len;
};

struct memdb_xattr {
    bool     used;
    uint64_t fileid;
    uint8_t *val;                  /* heap, vallen bytes; NULL when vallen == 0 */
    uint32_t vallen;
    char     name[MDS_XATTR_NAME_MAX + 1];
};

struct memdb_stripe {
    bool     used;
    uint64_t fileid;
    uint32_t stripe_count;
    uint32_t stripe_unit;
    uint32_t mirror_count;
    struct mds_ds_map_entry *entries;  /* heap, stripe_count * mirror_count */
};

struct memdb_ds {
    bool               used;
    struct mds_ds_info info;
};

struct memdb_provision {
    bool     used;
    uint32_t ds_id;
    uint32_t secret_len;
    uint64_t epoch;
    uint8_t  secret[MEMDB_SECRET_MAX];
};

struct memdb_quota_rule {
    bool                  used;
    uint8_t               scope_type;
    uint64_t              scope_id;
    struct mds_quota_rule rule;
};

struct memdb_quota_usage {
    bool                   used;
    uint8_t                usage_type;
    uint64_t               scope_id;
    struct mds_quota_usage usage;
};

struct memdb_gc {
    bool                used;
    struct mds_gc_entry entry;
};

struct memdb_remove_pending {
    bool                            used;
    struct mds_remove_pending_entry entry;
};

struct memdb_shard_fid {
    bool     used;
    uint64_t fileid;
    uint32_t shard_id;
};

struct memdb_ext_dirent {
    bool     used;
    uint64_t parent;
    uint32_t owner_mds_id;
    uint64_t target_fileid;
    uint8_t  target_type;
    uint64_t anchor_id;
    char     name[MDS_MAX_NAME + 1];
};

struct memdb_link_anchor {
    bool     used;
    uint64_t anchor_id;
    uint32_t remote_mds_id;
    uint64_t parent_fileid;
    char     name[MDS_MAX_NAME + 1];
};

struct memdb_journal {
    bool                            used;
    struct mds_coord_journal_record rec;
};

/* Layout-state row, keyed (fileid, stateid.other) like the RonDB
 * mds_layout_state table. */
struct memdb_layout {
    bool                used;
    uint64_t            clientid;
    uint64_t            fileid;
    uint32_t            iomode;
    uint64_t            offset;
    uint64_t            length;
    struct nfs4_stateid stateid;
    uint32_t           *ds_ids;    /* heap, ds_count entries; NULL when 0 */
    uint32_t            ds_count;
};

/* Client recovery row.  owner_mds_id is the identity of the instance
 * that wrote the row (the MDS that served the client, as on RonDB);
 * a single-node in-memory store has no boot epoch, so the epoch is 0. */
struct memdb_recovery {
    bool     used;
    uint64_t clientid;
    uint32_t owner_mds_id;
    uint64_t owner_boot_epoch;
    uint32_t co_ownerid_len;
    uint8_t  co_ownerid[1024];
    uint8_t  verifier[8];
};

struct memdb_open {
    bool                      used;
    struct mds_coord_open_row row;
};

struct memdb_lock {
    bool                      used;
    struct mds_coord_lock_row row;
};

struct memdb_deleg {
    bool                       used;
    struct mds_coord_deleg_row row;
};

struct memdb_client {
    bool                        used;
    struct mds_coord_client_row row;
};

struct memdb_session {
    bool                         used;
    struct mds_coord_session_row row;
};

struct memdb_drc_slot {
    bool     used;
    uint8_t  session_id[16];
    uint32_t slot_id;
    uint32_t seq_id;
    uint8_t *reply;                /* heap, reply_len bytes; NULL when 0 */
    uint32_t reply_len;
    uint64_t last_used_ns;
};

struct memdb_node {
    bool     used;
    uint32_t mds_id;
    uint64_t boot_epoch;
    uint16_t nfs_port;
    uint16_t grpc_port;
    uint64_t last_heartbeat_ns;    /* writer's CLOCK_REALTIME */
    char     hostname[MEMDB_HOSTNAME_MAX + 1];
};

struct memdb_partition {
    bool     used;
    uint32_t partition_id;
    uint32_t owner_mds_id;
    uint8_t  state;
    char     subtree_path[MDS_MAX_PATH];
};

/* -----------------------------------------------------------------------
 * Instance
 * ----------------------------------------------------------------------- */

struct memdb {
    pthread_mutex_t lock;          /* guards every field below */
    /* Identity of the MDS this instance serves (cfg->self.id at open;
     * 0 for the bare test constructor).  Stamped on rows that record
     * their owning MDS (client recovery).  Immutable after open. */
    uint32_t        self_mds_id;

    /* Namespace */
    struct memdb_inode      *inodes;
    struct memdb_dirent     *dirents;
    uint64_t                 next_fileid;   /* monotonic per instance */
    uint64_t                 next_cookie;   /* monotonic per instance */

    /* Catalogue data */
    struct memdb_inline     *inlines;
    struct memdb_xattr      *xattrs;
    struct memdb_stripe     *stripes;
    struct memdb_ds         *ds;
    struct memdb_provision  *provisions;
    struct memdb_quota_rule *quota_rules;
    struct memdb_quota_usage *quota_usage;
    struct memdb_gc         *gc;
    uint64_t                 next_gc_seq;
    struct memdb_remove_pending *remove_pending;
    uint64_t                 next_remove_seq;
    struct memdb_shard_fid  *shard_fids;
    struct memdb_ext_dirent *ext_dirents;
    struct memdb_link_anchor *link_anchors;

    /* Coordination */
    struct memdb_journal    *journals;
    struct memdb_layout     *layouts;
    struct memdb_recovery   *recoveries;
    struct memdb_open       *opens;
    struct memdb_lock       *locks;
    struct memdb_deleg      *delegs;
    struct memdb_client     *clients;
    struct memdb_session    *sessions;
    struct memdb_drc_slot   *drc_slots;

    /* Cluster */
    struct memdb_node       *nodes;
    struct memdb_partition  *partitions;
};

/* -----------------------------------------------------------------------
 * Small helpers
 * ----------------------------------------------------------------------- */

static struct memdb *memdb_of(const struct mds_catalogue *cat)
{
    return cat->backend_private;
}

/* The mutex is initialised at open and destroyed at close; the only
 * failures pthread_mutex_lock can report on it are programming errors. */
static void memdb_lock(struct memdb *m)
{
    int rc = pthread_mutex_lock(&m->lock);

    assert(rc == 0);
    (void)rc;
}

static void memdb_unlock(struct memdb *m)
{
    int rc = pthread_mutex_unlock(&m->lock);

    assert(rc == 0);
    (void)rc;
}

static void memdb_now(struct timespec *ts)
{
    if (clock_gettime(CLOCK_REALTIME, ts) != 0) {
        ts->tv_sec = 0;
        ts->tv_nsec = 0;
    }
}

static uint64_t memdb_now_ns(void)
{
    struct timespec ts;

    memdb_now(&ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* A dirent name: non-empty and within MDS_MAX_NAME. */
static bool memdb_name_ok(const char *name)
{
    return name != NULL && name[0] != '\0' && strlen(name) <= MDS_MAX_NAME;
}

static void memdb_copy_name(char *dst, size_t dst_len, const char *src)
{
    (void)snprintf(dst, dst_len, "%s", src);
}

/* Directory bookkeeping shared by every namespace mutation. */
static void memdb_parent_touch(struct mds_inode *parent, const struct timespec *now)
{
    parent->mtime = *now;
    parent->ctime = *now;
    parent->change++;
}

/* --- inode table --- */

static int memdb_inode_find(const struct memdb *m, uint64_t fileid)
{
    for (uint32_t i = 0; i < MEMDB_MAX_INODES; i++) {
        if (m->inodes[i].used && m->inodes[i].ino.fileid == fileid) {
            return (int)i;
        }
    }
    return -1;
}

static int memdb_inode_free_slot(const struct memdb *m)
{
    for (uint32_t i = 0; i < MEMDB_MAX_INODES; i++) {
        if (!m->inodes[i].used) {
            return (int)i;
        }
    }
    return -1;
}

/* Store an inode by value.  ds_map is a caller-owned heap pointer the
 * store must never retain or hand back (callers free it after getattr). */
static void memdb_inode_store(struct memdb_inode *row, const struct mds_inode *ino)
{
    row->used = true;
    row->ino = *ino;
    row->ino.ds_map = NULL;
}

/* --- dirent table --- */

static int memdb_dirent_find(const struct memdb *m, uint64_t parent, const char *name)
{
    for (uint32_t i = 0; i < MEMDB_MAX_DIRENTS; i++) {
        if (m->dirents[i].used && m->dirents[i].parent == parent &&
            strcmp(m->dirents[i].name, name) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static int memdb_dirent_free_slot(const struct memdb *m)
{
    for (uint32_t i = 0; i < MEMDB_MAX_DIRENTS; i++) {
        if (!m->dirents[i].used) {
            return (int)i;
        }
    }
    return -1;
}

/* True when @dir_fileid has at least one entry (RMDIR / rename-over
 * directory emptiness, decided under the same lock hold as the
 * mutation -- C3). */
static bool memdb_dir_has_entries(const struct memdb *m, uint64_t dir_fileid)
{
    for (uint32_t i = 0; i < MEMDB_MAX_DIRENTS; i++) {
        if (m->dirents[i].used && m->dirents[i].parent == dir_fileid) {
            return true;
        }
    }
    return false;
}

/* Write a dirent into @slot with a fresh cookie.  A cookie identifies
 * one (name -> child) binding: any rebinding of a name is a new dirent
 * and gets a new cookie, so a cookie is stable for the life of the
 * binding and never reused within the instance. */
static void memdb_dirent_set(struct memdb *m, int slot, uint64_t parent, const char *name,
                             uint64_t child_fileid, uint8_t child_type)
{
    struct memdb_dirent *d = &m->dirents[slot];

    d->used = true;
    d->parent = parent;
    d->child_fileid = child_fileid;
    d->child_type = child_type;
    d->cookie = m->next_cookie++;
    memdb_copy_name(d->name, sizeof(d->name), name);
}

/* --- stripe / inline / xattr helpers used by the namespace slots --- */

static int memdb_stripe_find(const struct memdb *m, uint64_t fileid)
{
    for (uint32_t i = 0; i < MEMDB_MAX_STRIPE_MAPS; i++) {
        if (m->stripes[i].used && m->stripes[i].fileid == fileid) {
            return (int)i;
        }
    }
    return -1;
}

static int memdb_stripe_free_slot(const struct memdb *m)
{
    for (uint32_t i = 0; i < MEMDB_MAX_STRIPE_MAPS; i++) {
        if (!m->stripes[i].used) {
            return (int)i;
        }
    }
    return -1;
}

static void memdb_stripe_clear(struct memdb_stripe *s)
{
    free(s->entries);
    s->entries = NULL;
    s->used = false;
}

static uint32_t memdb_gc_free_count(const struct memdb *m)
{
    uint32_t n = 0;

    for (uint32_t i = 0; i < MEMDB_MAX_GC; i++) {
        if (!m->gc[i].used) {
            n++;
        }
    }
    return n;
}

static int memdb_gc_free_slot(const struct memdb *m)
{
    for (uint32_t i = 0; i < MEMDB_MAX_GC; i++) {
        if (!m->gc[i].used) {
            return (int)i;
        }
    }
    return -1;
}

/* Insert one GC row into a slot the caller has already reserved by
 * checking memdb_gc_free_count(). */
static void memdb_gc_insert(struct memdb *m, uint64_t fileid, uint32_t ds_id,
                            const uint8_t *nfs_fh, uint32_t fh_len, uint32_t sweep_hint)
{
    int slot = memdb_gc_free_slot(m);
    struct mds_gc_entry *e;

    assert(slot >= 0);
    if (slot < 0) {
        return;
    }
    e = &m->gc[slot].entry;
    memset(e, 0, sizeof(*e));
    e->gc_seq = m->next_gc_seq++;
    e->fileid = fileid;
    e->ds_id = ds_id;
    e->sweep_hint = sweep_hint;
    if (fh_len > MDS_NFS_FH_MAX) {
        fh_len = MDS_NFS_FH_MAX;
    }
    e->nfs_fh_len = fh_len;
    if (fh_len > 0 && nfs_fh != NULL) {
        memcpy(e->nfs_fh, nfs_fh, fh_len);
    }
    m->gc[slot].used = true;
}

/* Drop everything hanging off a deleted inode except the stripe map,
 * which the namespace callers read after the remove (op_remove and
 * op_rename fetch it for the GC enqueue) and delete themselves, exactly
 * as they do on RonDB.  The final-unlink slots that own the stripe map
 * (ns_remove, ns_remove_known_gc) drop it explicitly. */
static void memdb_inode_purge_side_tables(struct memdb *m, uint64_t fileid)
{
    for (uint32_t i = 0; i < MEMDB_MAX_INLINE; i++) {
        if (m->inlines[i].used && m->inlines[i].fileid == fileid) {
            free(m->inlines[i].data);
            m->inlines[i].data = NULL;
            m->inlines[i].used = false;
        }
    }
    for (uint32_t i = 0; i < MEMDB_MAX_XATTRS; i++) {
        if (m->xattrs[i].used && m->xattrs[i].fileid == fileid) {
            free(m->xattrs[i].val);
            m->xattrs[i].val = NULL;
            m->xattrs[i].used = false;
        }
    }
}

/* -----------------------------------------------------------------------
 * Authority ops -- fileid allocation, raw inode / dirent rows
 * ----------------------------------------------------------------------- */

static enum mds_status mem_alloc_fileid(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t *fileid)
{
    struct memdb *m = memdb_of(cat);

    (void)txn;
    if (fileid == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    *fileid = m->next_fileid++;
    memdb_unlock(m);
    return MDS_OK;
}

/* Full inode write: insert or overwrite the whole record. */
static enum mds_status mem_inode_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, const struct mds_inode *inode)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    if (inode == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_inode_find(m, inode->fileid);
    if (idx < 0) {
        idx = memdb_inode_free_slot(m);
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    memdb_inode_store(&m->inodes[idx], inode);
    memdb_unlock(m);
    return MDS_OK;
}

/* Standalone inode-row delete; side tables stay (callers drop them). */
static enum mds_status mem_inode_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    memdb_lock(m);
    idx = memdb_inode_find(m, fileid);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->inodes[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_ns_getattr(struct mds_catalogue *cat,
    uint64_t fileid, struct mds_inode *inode)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (inode == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_inode_find(m, fileid);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    *inode = m->inodes[idx].ino;
    memdb_unlock(m);
    return MDS_OK;
}

/* Masked read-modify-write under the instance lock; the mask branches
 * mirror the RonDB setattr (rondb_shim_inode_setattr_rmw) bit for bit,
 * including the grow-only MDS_ATTR_SIZE_EXTEND and the unconditional
 * ctime / change bump. */
static enum mds_status mem_ns_setattr(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid,
    const struct mds_inode *attrs, uint32_t mask)
{
    struct memdb *m = memdb_of(cat);
    struct mds_inode *i;
    struct timespec now;
    int idx;

    (void)txn;
    if (attrs == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_now(&now);
    memdb_lock(m);
    idx = memdb_inode_find(m, fileid);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    i = &m->inodes[idx].ino;
    if (mask & MDS_ATTR_MODE) {
        i->mode = attrs->mode;
    }
    if (mask & MDS_ATTR_UID) {
        i->uid = attrs->uid;
    }
    if (mask & MDS_ATTR_GID) {
        i->gid = attrs->gid;
    }
    if (mask & MDS_ATTR_SIZE) {
        i->size = attrs->size;
    }
    if ((mask & MDS_ATTR_SIZE_EXTEND) && attrs->size > i->size) {
        i->size = attrs->size;
    }
    if (mask & MDS_ATTR_ATIME) {
        i->atime = attrs->atime;
    }
    if (mask & MDS_ATTR_MTIME) {
        i->mtime = attrs->mtime;
    }
    if (mask & MDS_ATTR_ATIME_NOW) {
        i->atime = now;
    }
    if (mask & MDS_ATTR_MTIME_NOW) {
        i->mtime = now;
    }
    if (mask & MDS_ATTR_FLAGS) {
        i->flags = attrs->flags;
    }
    i->ctime = now;
    i->change++;
    memdb_unlock(m);
    return MDS_OK;
}

/* Raw dirent write (insert or overwrite); no parent validation, like
 * the RonDB row write it mirrors.  An overwrite is a rebinding and gets
 * a fresh cookie. */
static enum mds_status mem_dirent_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent,
    const char *name, uint64_t child_fileid, uint8_t child_type)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    if (!memdb_name_ok(name)) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_dirent_find(m, parent, name);
    if (idx < 0) {
        idx = memdb_dirent_free_slot(m);
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    memdb_dirent_set(m, idx, parent, name, child_fileid, child_type);
    memdb_unlock(m);
    return MDS_OK;
}

/* Insert-only dirent write: MDS_ERR_EXISTS on a name collision. */
static enum mds_status mem_dirent_insert(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent,
    const char *name, uint64_t child_fileid, uint8_t child_type)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    if (!memdb_name_ok(name)) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    if (memdb_dirent_find(m, parent, name) >= 0) {
        memdb_unlock(m);
        return MDS_ERR_EXISTS;
    }
    idx = memdb_dirent_free_slot(m);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOSPC;
    }
    memdb_dirent_set(m, idx, parent, name, child_fileid, child_type);
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_dirent_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent, const char *name)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    if (name == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_dirent_find(m, parent, name);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->dirents[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_dirent_name_for_child(
    struct mds_catalogue *cat, uint64_t parent, uint64_t child_fileid,
    char *name_out, size_t name_out_len)
{
    struct memdb *m = memdb_of(cat);

    if (name_out == NULL || name_out_len == 0) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_DIRENTS; i++) {
        if (!m->dirents[i].used || m->dirents[i].parent != parent ||
            m->dirents[i].child_fileid != child_fileid) {
            continue;
        }
        memdb_copy_name(name_out, name_out_len, m->dirents[i].name);
        memdb_unlock(m);
        return MDS_OK;
    }
    memdb_unlock(m);
    return MDS_ERR_NOTFOUND;
}

/* -----------------------------------------------------------------------
 * Authority ops -- namespace mutations
 *
 * Every mutation is one lock hold: validate, reserve every row it will
 * insert, then mutate.  Nothing is written when any check fails.
 * ----------------------------------------------------------------------- */

/* CREATE: inode + dirent + parent bookkeeping.  @prealloc is accepted
 * for the slot signature and ignored: the reference backend does not
 * bind DS objects at create time (regular files start inline). */
static enum mds_status mem_ns_create(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent,
    const char *name, enum mds_file_type type,
    uint32_t mode, uint64_t uid, uint64_t gid,
    struct ds_prealloc_ctx *prealloc, struct mds_inode *out)
{
    struct memdb *m = memdb_of(cat);
    struct mds_inode child;
    struct timespec now;
    int pidx;
    int iidx;
    int didx;

    (void)txn;
    (void)prealloc;
    if (!memdb_name_ok(name) || out == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_now(&now);

    memdb_lock(m);
    pidx = memdb_inode_find(m, parent);
    if (pidx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    if (m->inodes[pidx].ino.type != MDS_FTYPE_DIR) {
        memdb_unlock(m);
        return MDS_ERR_NOTDIR;
    }
    if (memdb_dirent_find(m, parent, name) >= 0) {
        memdb_unlock(m);
        return MDS_ERR_EXISTS;
    }
    iidx = memdb_inode_free_slot(m);
    didx = memdb_dirent_free_slot(m);
    if (iidx < 0 || didx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOSPC;
    }

    memset(&child, 0, sizeof(child));
    child.fileid = m->next_fileid++;
    child.type = type;
    child.mode = mode;
    child.uid = uid;
    child.gid = gid;
    child.nlink = (type == MDS_FTYPE_DIR) ? 2U : 1U;
    child.atime = now;
    child.mtime = now;
    child.ctime = now;
    child.change = 1;
    child.generation = 1;
    child.parent_fileid = parent;
    if (type == MDS_FTYPE_REG) {
        child.flags = MDS_IFLAG_INLINE;
    }

    memdb_inode_store(&m->inodes[iidx], &child);
    memdb_dirent_set(m, didx, parent, name, child.fileid, (uint8_t)type);
    if (type == MDS_FTYPE_DIR) {
        m->inodes[pidx].ino.nlink++;
    }
    memdb_parent_touch(&m->inodes[pidx].ino, &now);
    memdb_unlock(m);

    *out = child;
    return MDS_OK;
}

/* Atomic wide create: inode + insert-only dirent + exact stripe map +
 * parent touch.  memdb is synchronous, so every non-success result
 * proves nothing was published and the DS bundle may be reclaimed. */
static enum mds_status mem_ns_create_wide(struct mds_catalogue *cat,
    uint64_t parent_fileid, const char *name,
    const struct mds_inode *child,
    uint32_t stripe_count, uint32_t stripe_unit, uint32_t mirror_count,
    const struct mds_ds_map_entry *entries, bool *safe_to_discard)
{
    struct memdb *m;
    struct mds_ds_map_entry *copied;
    struct timespec now;
    uint64_t entry_count;
    enum mds_status st = MDS_OK;
    int pidx;
    int iidx;
    int didx;
    int sidx;

    if (safe_to_discard != NULL) {
        *safe_to_discard = false;
    }
    if (cat == NULL || !memdb_name_ok(name) || child == NULL || entries == NULL ||
        child->fileid == 0 || child->parent_fileid != parent_fileid ||
        child->type != MDS_FTYPE_REG || stripe_count == 0 ||
        stripe_count > MDS_MAX_STRIPES || stripe_unit == 0 ||
        mirror_count == 0 || mirror_count > MDS_MAX_MIRRORS ||
        (child->flags & MDS_IFLAG_HPC_CREATE_PENDING) != 0) {
        if (safe_to_discard != NULL) {
            *safe_to_discard = true;
        }
        return MDS_ERR_INVAL;
    }
    entry_count = (uint64_t)stripe_count * mirror_count;
    copied = calloc((size_t)entry_count, sizeof(*copied));
    if (copied == NULL) {
        if (safe_to_discard != NULL) {
            *safe_to_discard = true;
        }
        return MDS_ERR_NOMEM;
    }
    memcpy(copied, entries, (size_t)entry_count * sizeof(*copied));
    memdb_now(&now);

    m = memdb_of(cat);
    memdb_lock(m);
    pidx = memdb_inode_find(m, parent_fileid);
    if (pidx < 0) {
        st = MDS_ERR_NOTFOUND;
    } else if (m->inodes[pidx].ino.type != MDS_FTYPE_DIR) {
        st = MDS_ERR_NOTDIR;
    } else if (memdb_dirent_find(m, parent_fileid, name) >= 0 ||
               memdb_inode_find(m, child->fileid) >= 0) {
        st = MDS_ERR_EXISTS;
    }
    if (st != MDS_OK) {
        memdb_unlock(m);
        free(copied);
        if (safe_to_discard != NULL) {
            *safe_to_discard = true;
        }
        return st;
    }
    iidx = memdb_inode_free_slot(m);
    didx = memdb_dirent_free_slot(m);
    sidx = memdb_stripe_free_slot(m);
    if (iidx < 0 || didx < 0 || sidx < 0) {
        memdb_unlock(m);
        free(copied);
        if (safe_to_discard != NULL) {
            *safe_to_discard = true;
        }
        return MDS_ERR_NOSPC;
    }

    memdb_inode_store(&m->inodes[iidx], child);
    memdb_dirent_set(m, didx, parent_fileid, name, child->fileid, (uint8_t)child->type);
    m->stripes[sidx].used = true;
    m->stripes[sidx].fileid = child->fileid;
    m->stripes[sidx].stripe_count = stripe_count;
    m->stripes[sidx].stripe_unit = stripe_unit;
    m->stripes[sidx].mirror_count = mirror_count;
    m->stripes[sidx].entries = copied;
    memdb_parent_touch(&m->inodes[pidx].ino, &now);
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_ns_lookup(struct mds_catalogue *cat,
    uint64_t parent, const char *name, struct mds_inode *child)
{
    struct memdb *m = memdb_of(cat);
    int didx;
    int iidx;

    if (name == NULL || child == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    didx = memdb_dirent_find(m, parent, name);
    if (didx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    iidx = memdb_inode_find(m, m->dirents[didx].child_fileid);
    if (iidx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    *child = m->inodes[iidx].ino;
    memdb_unlock(m);
    return MDS_OK;
}

/* LINK: the target must be a live non-directory, the name must be
 * free; then dirent + nlink + parent touch in one hold. */
static enum mds_status mem_ns_link(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent,
    const char *name, uint64_t target)
{
    struct memdb *m = memdb_of(cat);
    struct timespec now;
    int pidx;
    int tidx;
    int didx;

    (void)txn;
    if (!memdb_name_ok(name)) {
        return MDS_ERR_INVAL;
    }
    memdb_now(&now);

    memdb_lock(m);
    pidx = memdb_inode_find(m, parent);
    tidx = memdb_inode_find(m, target);
    if (pidx < 0 || tidx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    if (m->inodes[pidx].ino.type != MDS_FTYPE_DIR) {
        memdb_unlock(m);
        return MDS_ERR_NOTDIR;
    }
    if (m->inodes[tidx].ino.type == MDS_FTYPE_DIR) {
        memdb_unlock(m);
        return MDS_ERR_ISDIR;
    }
    if (memdb_dirent_find(m, parent, name) >= 0) {
        memdb_unlock(m);
        return MDS_ERR_EXISTS;
    }
    didx = memdb_dirent_free_slot(m);
    if (didx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOSPC;
    }

    memdb_dirent_set(m, didx, parent, name, target, (uint8_t)m->inodes[tidx].ino.type);
    m->inodes[tidx].ino.nlink++;
    m->inodes[tidx].ino.ctime = now;
    m->inodes[tidx].ino.change++;
    memdb_parent_touch(&m->inodes[pidx].ino, &now);
    memdb_unlock(m);
    return MDS_OK;
}

/*
 * RENAME (caller holds the lock) runs as validate -> mutate.  The
 * validation half resolves every operand into a plan; the mutation
 * half rebinds the source dirent row in place to the destination name
 * (no allocation, so no capacity failure), an overwritten destination
 * loses a link (or, for a directory, is deleted after the emptiness
 * check -- C3), directory moves carry the ".." link between parents,
 * and both parents are touched.  Renaming a name onto another name of
 * the same inode is a POSIX no-op.  MDS_CAT_RNF_KEEP_DST_ORPHAN keeps
 * the final link of an overwritten regular file as an nlink-0
 * UNLINK_ORPHAN row.
 */

/* Operands resolved by memdb_rename_check_locked.  didx is -1 when
 * there is no destination entry; spidx/scidx/dcidx are -1 when the
 * referenced inode row is missing (a dangling entry the namespace
 * tolerates). */
struct memdb_rename_plan {
    int      sidx;
    int      didx;
    int      spidx;
    int      dpidx;
    int      scidx;
    int      dcidx;
    uint64_t src_fid;
    bool     src_is_dir;
    bool     dst_is_dir;
    bool     noop;         /* both names already resolve to src_fid */
};

/* Type of a dirent's child: the live inode's when present, otherwise
 * the type recorded on the dirent itself. */
static bool memdb_child_is_dir(const struct memdb *m, int didx, int cidx)
{
    if (cidx >= 0) {
        return m->inodes[cidx].ino.type == MDS_FTYPE_DIR;
    }
    return m->dirents[didx].child_type == (uint8_t)MDS_FTYPE_DIR;
}

/* Validation half: on MDS_OK the plan is complete and, unless
 * plan->noop is set, the mutation half may run. */
static enum mds_status memdb_rename_check_locked(const struct memdb *m,
    uint64_t src_parent, const char *src_name,
    uint64_t dst_parent, const char *dst_name,
    struct memdb_rename_plan *p)
{
    uint64_t dst_fid;

    memset(p, 0, sizeof(*p));
    p->didx = -1;
    p->dcidx = -1;

    /* A parent that exists but is not a directory is NOTDIR (RFC 8881
     * 18.26.4), decided before the source name is looked up: a
     * non-directory has no dirents, so the name is always missing under
     * it and the answer would otherwise degrade to NOTFOUND.  Same
     * order as the RonDB shim and the fdb backend. */
    p->spidx = memdb_inode_find(m, src_parent);
    p->dpidx = memdb_inode_find(m, dst_parent);
    if ((p->spidx >= 0 && m->inodes[p->spidx].ino.type != MDS_FTYPE_DIR) ||
        (p->dpidx >= 0 && m->inodes[p->dpidx].ino.type != MDS_FTYPE_DIR)) {
        return MDS_ERR_NOTDIR;
    }
    p->sidx = memdb_dirent_find(m, src_parent, src_name);
    if (p->sidx < 0) {
        return MDS_ERR_NOTFOUND;
    }
    if (p->dpidx < 0) {
        return MDS_ERR_NOTFOUND;
    }
    p->src_fid = m->dirents[p->sidx].child_fileid;
    p->scidx = memdb_inode_find(m, p->src_fid);
    p->src_is_dir = memdb_child_is_dir(m, p->sidx, p->scidx);

    p->didx = memdb_dirent_find(m, dst_parent, dst_name);
    if (p->didx < 0) {
        return MDS_OK;
    }
    dst_fid = m->dirents[p->didx].child_fileid;
    if (dst_fid == p->src_fid) {
        p->noop = true;
        return MDS_OK;
    }
    p->dcidx = memdb_inode_find(m, dst_fid);
    p->dst_is_dir = memdb_child_is_dir(m, p->didx, p->dcidx);
    if (p->dst_is_dir) {
        if (!p->src_is_dir) {
            return MDS_ERR_ISDIR;
        }
        if (memdb_dir_has_entries(m, dst_fid)) {
            return MDS_ERR_NOTEMPTY;
        }
    } else if (p->src_is_dir) {
        return MDS_ERR_NOTDIR;
    }
    return MDS_OK;
}

/* Mutation half for an overwritten destination inode (checks passed).
 * A directory victim is deleted and the destination parent loses its
 * ".." link; a non-directory victim loses one link and, on its final
 * link, is deleted or kept as an UNLINK_ORPHAN row.  The destination
 * dirent itself is the caller's to drop. */
static void memdb_rename_drop_victim_locked(struct memdb *m,
    const struct memdb_rename_plan *p, uint32_t ns_flags,
    const struct timespec *now)
{
    struct mds_inode *victim;

    if (p->dcidx < 0) {
        return;
    }
    victim = &m->inodes[p->dcidx].ino;
    if (p->dst_is_dir) {
        m->inodes[p->dcidx].used = false;
        memdb_inode_purge_side_tables(m, victim->fileid);
        if (m->inodes[p->dpidx].ino.nlink > 0) {
            m->inodes[p->dpidx].ino.nlink--;
        }
        return;
    }
    if (victim->nlink > 0) {
        victim->nlink--;
    }
    if (victim->nlink > 0) {
        victim->ctime = *now;
        victim->change++;
        return;
    }
    if (victim->type == MDS_FTYPE_REG &&
        (ns_flags & MDS_CAT_RNF_KEEP_DST_ORPHAN) != 0U) {
        victim->flags |= MDS_IFLAG_UNLINK_ORPHAN;
        victim->ctime = *now;
        victim->change++;
        return;
    }
    m->inodes[p->dcidx].used = false;
    memdb_inode_purge_side_tables(m, victim->fileid);
}

static enum mds_status memdb_rename_locked(struct memdb *m,
    uint64_t src_parent, const char *src_name,
    uint64_t dst_parent, const char *dst_name, uint32_t ns_flags)
{
    struct memdb_rename_plan p;
    struct timespec now;
    enum mds_status st;

    st = memdb_rename_check_locked(m, src_parent, src_name, dst_parent, dst_name, &p);
    if (st != MDS_OK || p.noop) {
        return st;
    }

    /* Every check passed: mutate. */
    memdb_now(&now);
    if (p.didx >= 0) {
        memdb_rename_drop_victim_locked(m, &p, ns_flags, &now);
        m->dirents[p.didx].used = false;
    }

    /* Rebind the source row in place: a new (name -> child) binding,
     * hence a new cookie. */
    memdb_dirent_set(m, p.sidx, dst_parent, dst_name, p.src_fid,
                     m->dirents[p.sidx].child_type);

    if (src_parent != dst_parent) {
        if (p.src_is_dir) {
            if (p.spidx >= 0 && m->inodes[p.spidx].ino.nlink > 0) {
                m->inodes[p.spidx].ino.nlink--;
            }
            m->inodes[p.dpidx].ino.nlink++;
        }
        if (p.scidx >= 0) {
            m->inodes[p.scidx].ino.parent_fileid = dst_parent;
        }
        if (p.spidx >= 0) {
            memdb_parent_touch(&m->inodes[p.spidx].ino, &now);
        }
    }
    memdb_parent_touch(&m->inodes[p.dpidx].ino, &now);
    return MDS_OK;
}

static enum mds_status mem_ns_rename_flags(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t src_parent,
    const char *src_name, uint64_t dst_parent, const char *dst_name,
    uint32_t ns_flags)
{
    struct memdb *m = memdb_of(cat);
    enum mds_status st;

    (void)txn;
    if (!memdb_name_ok(src_name) || !memdb_name_ok(dst_name)) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    st = memdb_rename_locked(m, src_parent, src_name, dst_parent, dst_name, ns_flags);
    memdb_unlock(m);
    return st;
}

static enum mds_status mem_ns_rename(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t src_parent,
    const char *src_name, uint64_t dst_parent, const char *dst_name)
{
    return mem_ns_rename_flags(cat, txn, src_parent, src_name, dst_parent, dst_name, 0U);
}

/*
 * Unlink core shared by ns_remove and ns_remove_known_gc (caller holds
 * the lock).  Validates the dirent (and, when @guard is given, that it
 * still resolves to guard->fileid and that guard->nlink still predicts
 * the final-link answer -- MDS_ERR_STALE otherwise, exactly as the
 * RonDB shim's data-node guards decide), decides directory emptiness
 * and the final-link question, reserves the GC rows the fused caller
 * asked for, and only then mutates: dirent gone, the child loses a
 * link or is deleted with its inline data, xattrs and stripe map, the
 * GC rows land, the parent is touched.  A directory has exactly one
 * name, so removing it deletes it and drops the parent's ".." link.
 * *final_out reports whether the child inode was deleted.
 */

/* Mutation half for the child inode at @cidx (checks passed): the
 * final link deletes the inode with its side tables and stripe map and
 * lands the caller's GC rows; an earlier link just drops nlink. */
static void memdb_unlink_child_locked(struct memdb *m, int cidx, uint64_t child_fid,
    bool final, const struct mds_ds_map_entry *gc_entries,
    uint32_t gc_entry_count, uint32_t gc_sweep_hint,
    const struct timespec *now)
{
    int sidx;

    if (!final) {
        m->inodes[cidx].ino.nlink--;
        m->inodes[cidx].ino.ctime = *now;
        m->inodes[cidx].ino.change++;
        return;
    }
    m->inodes[cidx].used = false;
    memdb_inode_purge_side_tables(m, child_fid);
    sidx = memdb_stripe_find(m, child_fid);
    if (sidx >= 0) {
        memdb_stripe_clear(&m->stripes[sidx]);
    }
    for (uint32_t i = 0; i < gc_entry_count; i++) {
        memdb_gc_insert(m, child_fid, gc_entries[i].ds_id, gc_entries[i].nfs_fh,
                        gc_entries[i].nfs_fh_len, gc_sweep_hint);
    }
}

static enum mds_status memdb_unlink_locked(struct memdb *m, uint64_t parent,
    const char *name, const struct mds_inode *guard,
    const struct mds_ds_map_entry *gc_entries, uint32_t gc_entry_count,
    uint32_t gc_sweep_hint, bool *final_out)
{
    struct timespec now;
    uint64_t child_fid;
    int didx;
    int cidx;
    int pidx;
    bool is_dir = false;
    bool final = false;

    *final_out = false;
    didx = memdb_dirent_find(m, parent, name);
    if (didx < 0) {
        return MDS_ERR_NOTFOUND;
    }
    child_fid = m->dirents[didx].child_fileid;
    if (guard != NULL && child_fid != guard->fileid) {
        return MDS_ERR_STALE;
    }
    cidx = memdb_inode_find(m, child_fid);
    pidx = memdb_inode_find(m, parent);
    if (cidx >= 0) {
        const struct mds_inode *c = &m->inodes[cidx].ino;

        is_dir = (c->type == MDS_FTYPE_DIR);
        if (is_dir) {
            if (memdb_dir_has_entries(m, child_fid)) {
                return MDS_ERR_NOTEMPTY;
            }
            final = true;
        } else {
            final = (c->nlink <= 1U);
            /* The caller derived its GC / quota bookkeeping from the
             * snapshot's link count; a LINK or another REMOVE since then
             * makes that plan wrong for this inode.  Refuse instead of
             * silently doing the other shape. */
            if (guard != NULL && (guard->nlink <= 1U) != final) {
                return MDS_ERR_STALE;
            }
        }
    }
    if (final && gc_entry_count > 0 && memdb_gc_free_count(m) < gc_entry_count) {
        return MDS_ERR_NOSPC;
    }

    /* Every check passed: mutate. */
    memdb_now(&now);
    m->dirents[didx].used = false;
    if (cidx >= 0) {
        memdb_unlink_child_locked(m, cidx, child_fid, final, gc_entries, gc_entry_count,
                                  gc_sweep_hint, &now);
    }
    if (pidx >= 0) {
        if (is_dir && m->inodes[pidx].ino.nlink > 0) {
            m->inodes[pidx].ino.nlink--;
        }
        memdb_parent_touch(&m->inodes[pidx].ino, &now);
    }
    *final_out = final;
    return MDS_OK;
}

/* REMOVE: dirent + inode (when the last link goes) + parent touch.
 * GC rows are the caller's business here, as on RonDB. */
static enum mds_status mem_ns_remove(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent, const char *name)
{
    struct memdb *m = memdb_of(cat);
    enum mds_status st;
    bool final = false;

    (void)txn;
    if (name == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    st = memdb_unlink_locked(m, parent, name, NULL, NULL, 0, 0, &final);
    memdb_unlock(m);
    return st;
}

/* Fused final unlink: the caller's GC rows land in the same hold as
 * the namespace mutation, or nothing lands at all. */
static enum mds_status mem_ns_remove_known_gc(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent, const char *name,
    const struct mds_inode *child, uint32_t stripe_count,
    const struct mds_ds_map_entry *gc_entries,
    uint32_t gc_entry_count, uint32_t gc_sweep_hint,
    bool *gc_folded)
{
    struct memdb *m = memdb_of(cat);
    enum mds_status st;
    bool final = false;

    (void)txn;
    (void)stripe_count;
    if (name == NULL || child == NULL || gc_folded == NULL ||
        (gc_entry_count > 0 && gc_entries == NULL)) {
        return MDS_ERR_INVAL;
    }
    *gc_folded = false;
    memdb_lock(m);
    st = memdb_unlink_locked(m, parent, name, child, gc_entries, gc_entry_count,
                             gc_sweep_hint, &final);
    memdb_unlock(m);
    if (st == MDS_OK) {
        *gc_folded = final;
    }
    return st;
}

static enum mds_status mem_ns_parent_touch(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid,
    uint64_t change_delta, struct timespec stamp)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    if (change_delta == 0) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_inode_find(m, fileid);
    if (idx >= 0) {
        m->inodes[idx].ino.change += change_delta;
        m->inodes[idx].ino.mtime = stamp;
        m->inodes[idx].ino.ctime = stamp;
    }
    memdb_unlock(m);
    return MDS_OK; /* a missing parent row is MDS_OK by contract */
}

/* The link count saturates at both ends of uint32_t: it is never
 * negative and never wraps.  The sum is formed in int64_t, which holds
 * every (uint32_t + int32_t) exactly -- negating @delta would not (no
 * int32_t holds -INT32_MIN). */
static enum mds_status mem_ns_nlink_adjust(struct mds_catalogue *cat,
    uint64_t fileid, int32_t delta)
{
    struct memdb *m = memdb_of(cat);
    struct mds_inode *i;
    int64_t sum;
    int idx;

    memdb_lock(m);
    idx = memdb_inode_find(m, fileid);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    i = &m->inodes[idx].ino;
    sum = (int64_t)i->nlink + (int64_t)delta;
    if (sum < 0) {
        i->nlink = 0;
    } else if (sum > (int64_t)UINT32_MAX) {
        i->nlink = UINT32_MAX;
    } else {
        i->nlink = (uint32_t)sum;
    }
    memdb_unlock(m);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Authority ops -- async-REMOVE delete manifest
 * ----------------------------------------------------------------------- */

static int memdb_remove_pending_find(const struct memdb *m, uint64_t remove_seq)
{
    for (uint32_t i = 0; i < MEMDB_MAX_REMOVE_PENDING; i++) {
        if (m->remove_pending[i].used &&
            m->remove_pending[i].entry.remove_seq == remove_seq) {
            return (int)i;
        }
    }
    return -1;
}

static int memdb_remove_pending_free_slot(const struct memdb *m)
{
    for (uint32_t i = 0; i < MEMDB_MAX_REMOVE_PENDING; i++) {
        if (!m->remove_pending[i].used) {
            return (int)i;
        }
    }
    return -1;
}

/* Caller holds the lock and has reserved @slot. */
static uint64_t memdb_remove_pending_insert(struct memdb *m, int slot, uint64_t dir_fileid,
    const char *name, uint64_t child_fileid, uint64_t child_generation)
{
    struct mds_remove_pending_entry *e = &m->remove_pending[slot].entry;

    memset(e, 0, sizeof(*e));
    e->remove_seq = m->next_remove_seq++;
    e->dir_fileid = dir_fileid;
    e->child_fileid = child_fileid;
    e->child_generation = child_generation;
    e->enqueued_ns = memdb_now_ns();
    memdb_copy_name(e->name, sizeof(e->name), name);
    m->remove_pending[slot].used = true;
    return e->remove_seq;
}

static enum mds_status mem_remove_pending_enqueue(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t dir_fileid, const char *name,
    uint64_t child_fileid, uint64_t child_generation, uint64_t *seq_out)
{
    struct memdb *m = memdb_of(cat);
    int slot;

    (void)txn;
    if (!memdb_name_ok(name) || seq_out == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    slot = memdb_remove_pending_free_slot(m);
    if (slot < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOSPC;
    }
    *seq_out = memdb_remove_pending_insert(m, slot, dir_fileid, name, child_fileid,
                                           child_generation);
    memdb_unlock(m);
    return MDS_OK;
}

/* Delete-at-ack: manifest row + guarded dirent delete + DELETE_PENDING
 * flag in one hold.  MDS_ERR_STALE when the dirent no longer resolves
 * to (child_fileid, child_generation).  The parent is deliberately not
 * touched: the ack path folds that delta into its aggregator. */
static enum mds_status mem_remove_pending_enqueue_unlink(
    struct mds_catalogue *cat, struct mds_cat_txn *txn,
    uint64_t dir_fileid, const char *name, uint64_t child_fileid,
    uint64_t child_generation, uint64_t *seq_out)
{
    struct memdb *m = memdb_of(cat);
    int didx;
    int cidx;
    int slot;

    (void)txn;
    if (!memdb_name_ok(name) || seq_out == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    didx = memdb_dirent_find(m, dir_fileid, name);
    if (didx < 0 || m->dirents[didx].child_fileid != child_fileid) {
        memdb_unlock(m);
        return MDS_ERR_STALE;
    }
    cidx = memdb_inode_find(m, child_fileid);
    if (cidx < 0 || m->inodes[cidx].ino.generation != child_generation) {
        memdb_unlock(m);
        return MDS_ERR_STALE;
    }
    slot = memdb_remove_pending_free_slot(m);
    if (slot < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOSPC;
    }
    m->inodes[cidx].ino.flags |= MDS_IFLAG_DELETE_PENDING;
    m->dirents[didx].used = false;
    *seq_out = memdb_remove_pending_insert(m, slot, dir_fileid, name, child_fileid,
                                           child_generation);
    memdb_unlock(m);
    return MDS_OK;
}

static int memdb_remove_pending_cmp(const void *a, const void *b)
{
    const struct mds_remove_pending_entry *ea = a;
    const struct mds_remove_pending_entry *eb = b;

    if (ea->remove_seq < eb->remove_seq) {
        return -1;
    }
    return (ea->remove_seq > eb->remove_seq) ? 1 : 0;
}

static enum mds_status mem_remove_pending_peek_batch(struct mds_catalogue *cat,
    uint64_t now_ns, struct mds_remove_pending_entry *entries,
    uint32_t cap, uint32_t *n_out)
{
    struct memdb *m = memdb_of(cat);
    uint32_t n = 0;

    if (n_out != NULL) {
        *n_out = 0;
    }
    if (entries == NULL || cap == 0 || n_out == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_REMOVE_PENDING && n < cap; i++) {
        const struct mds_remove_pending_entry *e = &m->remove_pending[i].entry;

        if (!m->remove_pending[i].used) {
            continue;
        }
        if (e->claim_mds_id != 0 && e->claim_expires_ns >= now_ns) {
            continue; /* claimed by a live drainer */
        }
        entries[n++] = *e;
    }
    memdb_unlock(m);
    if (n > 1) {
        qsort(entries, n, sizeof(*entries), memdb_remove_pending_cmp);
    }
    *n_out = n;
    return MDS_OK;
}

static enum mds_status mem_remove_pending_claim(struct mds_catalogue *cat,
    uint64_t remove_seq, uint32_t mds_id, uint64_t boot_epoch,
    uint64_t now_ns, uint64_t claim_ttl_ns)
{
    struct memdb *m = memdb_of(cat);
    struct mds_remove_pending_entry *e;
    int idx;

    memdb_lock(m);
    idx = memdb_remove_pending_find(m, remove_seq);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    e = &m->remove_pending[idx].entry;
    if (e->claim_mds_id != 0 && e->claim_expires_ns >= now_ns) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND; /* another drainer holds a live claim */
    }
    e->claim_mds_id = mds_id;
    e->claim_boot = boot_epoch;
    e->claim_expires_ns = (claim_ttl_ns > UINT64_MAX - now_ns)
                          ? UINT64_MAX : now_ns + claim_ttl_ns;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_remove_pending_complete(struct mds_catalogue *cat,
    uint64_t remove_seq)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    memdb_lock(m);
    idx = memdb_remove_pending_find(m, remove_seq);
    if (idx >= 0) {
        m->remove_pending[idx].used = false;
    }
    memdb_unlock(m);
    return MDS_OK; /* idempotent: a completed row may be completed again */
}

static enum mds_status mem_remove_pending_bump_retry(struct mds_catalogue *cat,
    uint64_t remove_seq)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    memdb_lock(m);
    idx = memdb_remove_pending_find(m, remove_seq);
    if (idx >= 0) {
        m->remove_pending[idx].entry.retries++;
    }
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_remove_pending_count(struct mds_catalogue *cat,
    uint32_t *count)
{
    struct memdb *m = memdb_of(cat);
    uint32_t n = 0;

    if (count == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_REMOVE_PENDING; i++) {
        if (m->remove_pending[i].used) {
            n++;
        }
    }
    memdb_unlock(m);
    *count = n;
    return MDS_OK;
}

/* Paged by slot index: a page is copied under the lock and delivered
 * without it (C1). */
static enum mds_status mem_remove_pending_scan_all(struct mds_catalogue *cat,
    mds_cat_remove_pending_scan_cb cb, void *ctx)
{
    struct memdb *m = memdb_of(cat);
    struct mds_remove_pending_entry *page;
    uint32_t cursor = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    page = calloc(MEMDB_SCAN_PAGE, sizeof(*page));
    if (page == NULL) {
        return MDS_ERR_NOMEM;
    }
    while (cursor < MEMDB_MAX_REMOVE_PENDING) {
        uint32_t n = 0;

        memdb_lock(m);
        while (cursor < MEMDB_MAX_REMOVE_PENDING && n < MEMDB_SCAN_PAGE) {
            if (m->remove_pending[cursor].used) {
                page[n++] = m->remove_pending[cursor].entry;
            }
            cursor++;
        }
        memdb_unlock(m);
        for (uint32_t i = 0; i < n; i++) {
            if (cb(&page[i], ctx) != 0) {
                free(page);
                return MDS_OK;
            }
        }
    }
    free(page);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Authority ops -- READDIR
 *
 * Both enumeration orders page the same way: under the lock, keep the
 * MEMDB_SCAN_PAGE smallest keys strictly greater than the cursor
 * (name order for ns_readdir, cookie order for ns_readdir_plus_from),
 * release the lock, deliver, and resume from the last delivered key.
 * The cookie-order page also snapshots each child inode under the same
 * hold, so a readdir_plus page is internally consistent.
 * ----------------------------------------------------------------------- */

struct memdb_rd_row {
    struct mds_cat_dirent d;
    struct mds_inode      ino;
    bool                  ino_valid;
};

static void memdb_rd_row_fill(const struct memdb *m, const struct memdb_dirent *src,
                              struct memdb_rd_row *row, bool with_inode)
{
    memset(row, 0, sizeof(*row));
    row->d.fileid = src->child_fileid;
    row->d.cookie = src->cookie;
    row->d.type = src->child_type;
    memdb_copy_name(row->d.name, sizeof(row->d.name), src->name);
    if (with_inode) {
        int iidx = memdb_inode_find(m, src->child_fileid);

        if (iidx >= 0) {
            row->ino = m->inodes[iidx].ino;
            row->ino_valid = true;
        }
    }
}

/* Insert @src into the sorted page at @pos, dropping the largest row
 * when the page is full.  Caller has determined @pos < page_cap. */
static void memdb_rd_page_insert(const struct memdb *m, struct memdb_rd_row *page,
                                 uint32_t *n, uint32_t page_cap, uint32_t pos,
                                 const struct memdb_dirent *src, bool with_inode)
{
    if (*n == page_cap) {
        (*n)--; /* evict the largest kept row to make room */
    }
    memmove(&page[pos + 1], &page[pos], (size_t)(*n - pos) * sizeof(*page));
    memdb_rd_row_fill(m, src, &page[pos], with_inode);
    (*n)++;
}

/* Name order: entries with name > @after (NULL: all).  Caller holds the lock. */
static uint32_t memdb_rd_page_by_name(const struct memdb *m, uint64_t parent,
                                      const char *after, struct memdb_rd_row *page,
                                      uint32_t page_cap)
{
    uint32_t n = 0;

    for (uint32_t i = 0; i < MEMDB_MAX_DIRENTS; i++) {
        const struct memdb_dirent *d = &m->dirents[i];
        uint32_t pos;

        if (!d->used || d->parent != parent) {
            continue;
        }
        if (after != NULL && strcmp(d->name, after) <= 0) {
            continue;
        }
        pos = n;
        while (pos > 0 && strcmp(page[pos - 1].d.name, d->name) > 0) {
            pos--;
        }
        if (pos >= page_cap) {
            continue; /* larger than every kept name */
        }
        memdb_rd_page_insert(m, page, &n, page_cap, pos, d, false);
    }
    return n;
}

/* Cookie order: entries with cookie > @after_cookie, plus their inodes.
 * Caller holds the lock. */
static uint32_t memdb_rd_page_by_cookie(const struct memdb *m, uint64_t parent,
                                        uint64_t after_cookie, struct memdb_rd_row *page,
                                        uint32_t page_cap)
{
    uint32_t n = 0;

    for (uint32_t i = 0; i < MEMDB_MAX_DIRENTS; i++) {
        const struct memdb_dirent *d = &m->dirents[i];
        uint32_t pos;

        if (!d->used || d->parent != parent || d->cookie <= after_cookie) {
            continue;
        }
        pos = n;
        while (pos > 0 && page[pos - 1].d.cookie > d->cookie) {
            pos--;
        }
        if (pos >= page_cap) {
            continue;
        }
        memdb_rd_page_insert(m, page, &n, page_cap, pos, d, true);
    }
    return n;
}

static enum mds_status mem_ns_readdir(struct mds_catalogue *cat,
    uint64_t parent, const char *start_after, uint32_t max_entries,
    struct mds_cat_txn *txn, mds_readdir_cb cb, void *ctx)
{
    struct memdb *m = memdb_of(cat);
    struct memdb_rd_row *page;
    char cursor[MDS_MAX_NAME + 1];
    const char *after = start_after;
    uint32_t delivered = 0;

    (void)txn;
    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    if (start_after != NULL && strlen(start_after) > MDS_MAX_NAME) {
        return MDS_ERR_INVAL;
    }
    page = calloc(MEMDB_SCAN_PAGE, sizeof(*page));
    if (page == NULL) {
        return MDS_ERR_NOMEM;
    }
    for (;;) {
        uint32_t n;

        memdb_lock(m);
        n = memdb_rd_page_by_name(m, parent, after, page, MEMDB_SCAN_PAGE);
        memdb_unlock(m);
        for (uint32_t i = 0; i < n; i++) {
            if (cb(&page[i].d, ctx) != 0) {
                free(page);
                return MDS_OK;
            }
            delivered++;
            if (max_entries > 0 && delivered >= max_entries) {
                free(page);
                return MDS_OK;
            }
        }
        if (n < MEMDB_SCAN_PAGE) {
            break; /* the last page was short: directory drained */
        }
        memdb_copy_name(cursor, sizeof(cursor), page[n - 1].d.name);
        after = cursor;
    }
    free(page);
    return MDS_OK;
}

static enum mds_status mem_ns_readdir_plus_from(struct mds_catalogue *cat,
    uint64_t parent, uint64_t start_after_cookie, uint32_t max_entries,
    struct mds_cat_txn *txn, mds_readdir_plus_cb cb, void *ctx)
{
    struct memdb *m = memdb_of(cat);
    struct memdb_rd_row *page;
    uint64_t after = start_after_cookie;
    uint32_t delivered = 0;

    (void)txn;
    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    page = calloc(MEMDB_SCAN_PAGE, sizeof(*page));
    if (page == NULL) {
        return MDS_ERR_NOMEM;
    }
    for (;;) {
        uint32_t n;

        memdb_lock(m);
        n = memdb_rd_page_by_cookie(m, parent, after, page, MEMDB_SCAN_PAGE);
        memdb_unlock(m);
        for (uint32_t i = 0; i < n; i++) {
            const struct memdb_rd_row *r = &page[i];

            if (cb(&r->d, r->ino_valid ? &r->ino : NULL, r->ino_valid, ctx) != 0) {
                free(page);
                return MDS_OK;
            }
            delivered++;
            if (max_entries > 0 && delivered >= max_entries) {
                free(page);
                return MDS_OK;
            }
        }
        if (n < MEMDB_SCAN_PAGE) {
            break;
        }
        after = page[n - 1].d.cookie;
    }
    free(page);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Authority ops -- inline data
 * ----------------------------------------------------------------------- */

static int memdb_inline_find(const struct memdb *m, uint64_t fileid)
{
    for (uint32_t i = 0; i < MEMDB_MAX_INLINE; i++) {
        if (m->inlines[i].used && m->inlines[i].fileid == fileid) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status mem_inline_get(struct mds_catalogue *cat,
    uint64_t fileid, void *buf, uint32_t buflen, uint32_t *outlen)
{
    struct memdb *m = memdb_of(cat);
    uint32_t copy;
    int idx;

    if (outlen == NULL || (buflen > 0 && buf == NULL)) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_inline_find(m, fileid);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    copy = m->inlines[idx].len;
    if (copy > buflen) {
        copy = buflen;
    }
    if (copy > 0) {
        memcpy(buf, m->inlines[idx].data, copy);
    }
    *outlen = copy;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_inline_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid, const void *buf, uint32_t len)
{
    struct memdb *m = memdb_of(cat);
    uint8_t *copy = NULL;
    int idx;

    (void)txn;
    if (len > MDS_INLINE_DATA_MAX || (len > 0 && buf == NULL)) {
        return MDS_ERR_INVAL;
    }
    if (len > 0) {
        copy = malloc(len);
        if (copy == NULL) {
            return MDS_ERR_NOMEM;
        }
        memcpy(copy, buf, len);
    }
    memdb_lock(m);
    idx = memdb_inline_find(m, fileid);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_INLINE; i++) {
            if (!m->inlines[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            free(copy);
            return MDS_ERR_NOSPC;
        }
    }
    free(m->inlines[idx].data);
    m->inlines[idx].used = true;
    m->inlines[idx].fileid = fileid;
    m->inlines[idx].data = copy;
    m->inlines[idx].len = len;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_inline_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    memdb_lock(m);
    idx = memdb_inline_find(m, fileid);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    free(m->inlines[idx].data);
    m->inlines[idx].data = NULL;
    m->inlines[idx].len = 0;
    m->inlines[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Authority ops -- extended attributes
 *
 * Every xattr mutation bumps the inode's change / ctime under the
 * same hold, mirroring the RonDB atomic xattr writes.
 * ----------------------------------------------------------------------- */

static int memdb_xattr_find(const struct memdb *m, uint64_t fileid, const char *name)
{
    for (uint32_t i = 0; i < MEMDB_MAX_XATTRS; i++) {
        if (m->xattrs[i].used && m->xattrs[i].fileid == fileid &&
            strcmp(m->xattrs[i].name, name) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static void memdb_inode_touch_locked(struct memdb *m, uint64_t fileid)
{
    int idx = memdb_inode_find(m, fileid);

    if (idx >= 0) {
        memdb_now(&m->inodes[idx].ino.ctime);
        m->inodes[idx].ino.change++;
    }
}

static enum mds_status mem_xattr_get(struct mds_catalogue *cat,
    uint64_t fileid, const char *name, void **val, uint32_t *vallen)
{
    struct memdb *m = memdb_of(cat);
    uint8_t *copy = NULL;
    int idx;

    if (name == NULL || val == NULL || vallen == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_xattr_find(m, fileid, name);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    /* A zero-length value still hands back a (freeable) buffer so the
     * caller's contract (free *val) holds uniformly. */
    copy = malloc(m->xattrs[idx].vallen > 0 ? m->xattrs[idx].vallen : 1U);
    if (copy == NULL) {
        memdb_unlock(m);
        return MDS_ERR_NOMEM;
    }
    if (m->xattrs[idx].vallen > 0) {
        memcpy(copy, m->xattrs[idx].val, m->xattrs[idx].vallen);
    }
    *val = copy;
    *vallen = m->xattrs[idx].vallen;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_xattr_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid,
    const char *name, const void *val, uint32_t vallen)
{
    struct memdb *m = memdb_of(cat);
    uint8_t *copy = NULL;
    int idx;

    (void)txn;
    if (name == NULL || name[0] == '\0' || strlen(name) > MDS_XATTR_NAME_MAX ||
        vallen > MDS_XATTR_VAL_MAX || (vallen > 0 && val == NULL)) {
        return MDS_ERR_INVAL;
    }
    if (vallen > 0) {
        copy = malloc(vallen);
        if (copy == NULL) {
            return MDS_ERR_NOMEM;
        }
        memcpy(copy, val, vallen);
    }
    memdb_lock(m);
    idx = memdb_xattr_find(m, fileid, name);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_XATTRS; i++) {
            if (!m->xattrs[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            free(copy);
            return MDS_ERR_NOSPC;
        }
        m->xattrs[idx].fileid = fileid;
        memdb_copy_name(m->xattrs[idx].name, sizeof(m->xattrs[idx].name), name);
    }
    free(m->xattrs[idx].val);
    m->xattrs[idx].val = copy;
    m->xattrs[idx].vallen = vallen;
    m->xattrs[idx].used = true;
    memdb_inode_touch_locked(m, fileid);
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_xattr_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid, const char *name)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    if (name == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_xattr_find(m, fileid, name);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    free(m->xattrs[idx].val);
    m->xattrs[idx].val = NULL;
    m->xattrs[idx].vallen = 0;
    m->xattrs[idx].used = false;
    memdb_inode_touch_locked(m, fileid);
    memdb_unlock(m);
    return MDS_OK;
}

/* Names are paged by slot index and delivered without the lock. */
static enum mds_status mem_xattr_list(struct mds_catalogue *cat,
    uint64_t fileid, mds_xattr_list_cb cb, void *ctx)
{
    struct memdb *m = memdb_of(cat);
    char (*page)[MDS_XATTR_NAME_MAX + 1];
    uint32_t cursor = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    page = calloc(MEMDB_SCAN_PAGE, sizeof(*page));
    if (page == NULL) {
        return MDS_ERR_NOMEM;
    }
    while (cursor < MEMDB_MAX_XATTRS) {
        uint32_t n = 0;

        memdb_lock(m);
        while (cursor < MEMDB_MAX_XATTRS && n < MEMDB_SCAN_PAGE) {
            if (m->xattrs[cursor].used && m->xattrs[cursor].fileid == fileid) {
                memdb_copy_name(page[n], sizeof(page[n]), m->xattrs[cursor].name);
                n++;
            }
            cursor++;
        }
        memdb_unlock(m);
        for (uint32_t i = 0; i < n; i++) {
            if (cb(page[i], strlen(page[i]), ctx) != 0) {
                free(page);
                return MDS_OK;
            }
        }
    }
    free(page);
    return MDS_OK;
}

static enum mds_status mem_xattr_exists(struct mds_catalogue *cat,
    uint64_t fileid, const char *name)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (name == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_xattr_find(m, fileid, name);
    memdb_unlock(m);
    return (idx >= 0) ? MDS_OK : MDS_ERR_NOTFOUND;
}

/* -----------------------------------------------------------------------
 * Authority ops -- stripe maps
 * ----------------------------------------------------------------------- */

static enum mds_status mem_stripe_map_get(struct mds_catalogue *cat,
    uint64_t fileid, uint32_t *sc, uint32_t *su, uint32_t *mc,
    struct mds_ds_map_entry **entries)
{
    struct memdb *m = memdb_of(cat);
    const struct memdb_stripe *s;
    uint64_t total;
    int idx;

    if (entries != NULL) {
        *entries = NULL;
    }
    memdb_lock(m);
    idx = memdb_stripe_find(m, fileid);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    s = &m->stripes[idx];
    if (sc != NULL) {
        *sc = s->stripe_count;
    }
    if (su != NULL) {
        *su = s->stripe_unit;
    }
    if (mc != NULL) {
        *mc = s->mirror_count;
    }
    total = (uint64_t)s->stripe_count * s->mirror_count;
    if (entries != NULL && total > 0 && s->entries != NULL) {
        *entries = malloc((size_t)total * sizeof(**entries));
        if (*entries == NULL) {
            memdb_unlock(m);
            return MDS_ERR_NOMEM;
        }
        memcpy(*entries, s->entries, (size_t)total * sizeof(**entries));
    }
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_stripe_map_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid,
    uint32_t sc, uint32_t su, uint32_t mc,
    const struct mds_ds_map_entry *entries)
{
    struct memdb *m = memdb_of(cat);
    struct mds_ds_map_entry *copy = NULL;
    uint64_t total;
    int idx;

    (void)txn;
    if (sc > MDS_MAX_STRIPES || mc > MDS_MAX_MIRRORS) {
        return MDS_ERR_INVAL;
    }
    total = (uint64_t)sc * mc;
    if (total > 0 && entries == NULL) {
        return MDS_ERR_INVAL;
    }
    if (total > 0) {
        copy = malloc((size_t)total * sizeof(*copy));
        if (copy == NULL) {
            return MDS_ERR_NOMEM;
        }
        memcpy(copy, entries, (size_t)total * sizeof(*copy));
    }
    memdb_lock(m);
    idx = memdb_stripe_find(m, fileid);
    if (idx < 0) {
        idx = memdb_stripe_free_slot(m);
        if (idx < 0) {
            memdb_unlock(m);
            free(copy);
            return MDS_ERR_NOSPC;
        }
    }
    free(m->stripes[idx].entries);
    m->stripes[idx].used = true;
    m->stripes[idx].fileid = fileid;
    m->stripes[idx].stripe_count = sc;
    m->stripes[idx].stripe_unit = su;
    m->stripes[idx].mirror_count = mc;
    m->stripes[idx].entries = copy;
    memdb_unlock(m);
    return MDS_OK;
}

/* Idempotent: deleting an absent map is MDS_OK (ds_gc repeats it). */
static enum mds_status mem_stripe_map_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    memdb_lock(m);
    idx = memdb_stripe_find(m, fileid);
    if (idx >= 0) {
        memdb_stripe_clear(&m->stripes[idx]);
    }
    memdb_unlock(m);
    return MDS_OK;
}

/* One stripe map per callback: header + a private copy of the entries
 * taken under the lock, delivered without it. */
static enum mds_status mem_stripe_map_scan(struct mds_catalogue *cat,
    mds_cat_stripe_map_scan_cb cb, void *ctx)
{
    struct memdb *m = memdb_of(cat);
    uint32_t cursor = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    while (cursor < MEMDB_MAX_STRIPE_MAPS) {
        struct mds_ds_map_entry *copy = NULL;
        uint64_t fileid = 0;
        uint32_t sc = 0;
        uint32_t su = 0;
        uint32_t mc = 0;
        uint64_t total = 0;
        bool have = false;
        int rc;

        memdb_lock(m);
        while (cursor < MEMDB_MAX_STRIPE_MAPS && !have) {
            const struct memdb_stripe *s = &m->stripes[cursor];

            cursor++;
            if (!s->used) {
                continue;
            }
            fileid = s->fileid;
            sc = s->stripe_count;
            su = s->stripe_unit;
            mc = s->mirror_count;
            total = (uint64_t)sc * mc;
            if (total > 0 && s->entries != NULL) {
                copy = malloc((size_t)total * sizeof(*copy));
                if (copy == NULL) {
                    memdb_unlock(m);
                    return MDS_ERR_NOMEM;
                }
                memcpy(copy, s->entries, (size_t)total * sizeof(*copy));
            }
            have = true;
        }
        memdb_unlock(m);
        if (!have) {
            break;
        }
        rc = cb(fileid, sc, su, mc, copy, ctx);
        free(copy);
        if (rc != 0) {
            return MDS_OK;
        }
    }
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Authority ops -- DS registry and provisioning
 * ----------------------------------------------------------------------- */

static int memdb_ds_find(const struct memdb *m, uint32_t ds_id)
{
    for (uint32_t i = 0; i < MEMDB_MAX_DS; i++) {
        if (m->ds[i].used && m->ds[i].info.ds_id == ds_id) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status mem_ds_get(struct mds_catalogue *cat,
    uint32_t ds_id, struct mds_ds_info *info)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (info == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_ds_find(m, ds_id);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    *info = m->ds[idx].info;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_ds_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, const struct mds_ds_info *info)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    if (info == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_ds_find(m, info->ds_id);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_DS; i++) {
            if (!m->ds[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    m->ds[idx].used = true;
    m->ds[idx].info = *info;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_ds_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint32_t ds_id)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    memdb_lock(m);
    idx = memdb_ds_find(m, ds_id);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->ds[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_ds_list(struct mds_catalogue *cat,
    struct mds_ds_info **list, uint32_t *count)
{
    struct memdb *m = memdb_of(cat);
    struct mds_ds_info *out;
    uint32_t n = 0;

    if (list == NULL || count == NULL) {
        return MDS_ERR_INVAL;
    }
    /* The table is bounded, so one allocation of the bound is exact
     * enough and keeps the copy inside a single hold. */
    out = calloc(MEMDB_MAX_DS, sizeof(*out));
    if (out == NULL) {
        return MDS_ERR_NOMEM;
    }
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_DS; i++) {
        if (m->ds[i].used) {
            out[n++] = m->ds[i].info;
        }
    }
    memdb_unlock(m);
    *list = out;
    *count = n;
    return MDS_OK;
}

static int memdb_provision_find(const struct memdb *m, uint32_t ds_id)
{
    for (uint32_t i = 0; i < MEMDB_MAX_PROVISION; i++) {
        if (m->provisions[i].used && m->provisions[i].ds_id == ds_id) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status mem_ds_provision_get(struct mds_catalogue *cat,
    uint32_t ds_id, uint8_t *secret, uint32_t secret_len, uint64_t *epoch)
{
    struct memdb *m = memdb_of(cat);
    uint32_t copy;
    int idx;

    if ((secret_len > 0 && secret == NULL) || epoch == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_provision_find(m, ds_id);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    copy = m->provisions[idx].secret_len;
    if (copy > secret_len) {
        copy = secret_len;
    }
    if (copy > 0) {
        memcpy(secret, m->provisions[idx].secret, copy);
    }
    *epoch = m->provisions[idx].epoch;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_ds_provision_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint32_t ds_id,
    const uint8_t *secret, uint32_t secret_len, uint64_t epoch)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    if (secret_len > MEMDB_SECRET_MAX || (secret_len > 0 && secret == NULL)) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_provision_find(m, ds_id);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_PROVISION; i++) {
            if (!m->provisions[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    m->provisions[idx].used = true;
    m->provisions[idx].ds_id = ds_id;
    memset(m->provisions[idx].secret, 0, sizeof(m->provisions[idx].secret));
    if (secret_len > 0) {
        memcpy(m->provisions[idx].secret, secret, secret_len);
    }
    m->provisions[idx].secret_len = secret_len;
    m->provisions[idx].epoch = epoch;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_ds_provision_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint32_t ds_id)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    memdb_lock(m);
    idx = memdb_provision_find(m, ds_id);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->provisions[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Authority ops -- quota rules and usage (keyed scope_type + scope_id)
 * ----------------------------------------------------------------------- */

static enum mds_status mem_quota_rule_get(struct mds_catalogue *cat,
    uint8_t scope_type, uint64_t scope_id, struct mds_quota_rule *rule)
{
    struct memdb *m = memdb_of(cat);

    if (rule == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_QUOTA_RULES; i++) {
        if (m->quota_rules[i].used && m->quota_rules[i].scope_type == scope_type &&
            m->quota_rules[i].scope_id == scope_id) {
            *rule = m->quota_rules[i].rule;
            memdb_unlock(m);
            return MDS_OK;
        }
    }
    memdb_unlock(m);
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_quota_rule_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint8_t scope_type, uint64_t scope_id,
    const struct mds_quota_rule *rule)
{
    struct memdb *m = memdb_of(cat);
    int idx = -1;

    (void)txn;
    if (rule == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_QUOTA_RULES; i++) {
        if (m->quota_rules[i].used && m->quota_rules[i].scope_type == scope_type &&
            m->quota_rules[i].scope_id == scope_id) {
            idx = (int)i;
            break;
        }
        if (idx < 0 && !m->quota_rules[i].used) {
            idx = (int)i; /* first free slot, kept unless a match follows */
        }
    }
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOSPC;
    }
    m->quota_rules[idx].used = true;
    m->quota_rules[idx].scope_type = scope_type;
    m->quota_rules[idx].scope_id = scope_id;
    m->quota_rules[idx].rule = *rule;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_quota_usage_get(struct mds_catalogue *cat,
    uint8_t usage_type, uint64_t scope_id, struct mds_quota_usage *usage)
{
    struct memdb *m = memdb_of(cat);

    if (usage == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_QUOTA_USAGE; i++) {
        if (m->quota_usage[i].used && m->quota_usage[i].usage_type == usage_type &&
            m->quota_usage[i].scope_id == scope_id) {
            *usage = m->quota_usage[i].usage;
            memdb_unlock(m);
            return MDS_OK;
        }
    }
    memdb_unlock(m);
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_quota_usage_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint8_t usage_type, uint64_t scope_id,
    const struct mds_quota_usage *usage)
{
    struct memdb *m = memdb_of(cat);
    int idx = -1;

    (void)txn;
    if (usage == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_QUOTA_USAGE; i++) {
        if (m->quota_usage[i].used && m->quota_usage[i].usage_type == usage_type &&
            m->quota_usage[i].scope_id == scope_id) {
            idx = (int)i;
            break;
        }
        if (idx < 0 && !m->quota_usage[i].used) {
            idx = (int)i;
        }
    }
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOSPC;
    }
    m->quota_usage[idx].used = true;
    m->quota_usage[idx].usage_type = usage_type;
    m->quota_usage[idx].scope_id = scope_id;
    m->quota_usage[idx].usage = *usage;
    memdb_unlock(m);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Authority ops -- GC queue (peek order is ascending gc_seq)
 * ----------------------------------------------------------------------- */

static enum mds_status mem_gc_enqueue(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid,
    uint32_t ds_id, const uint8_t *nfs_fh, uint32_t fh_len, uint32_t sweep_hint)
{
    struct memdb *m = memdb_of(cat);

    (void)txn;
    if (fh_len > MDS_NFS_FH_MAX || (fh_len > 0 && nfs_fh == NULL)) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    if (memdb_gc_free_count(m) == 0) {
        memdb_unlock(m);
        return MDS_ERR_NOSPC;
    }
    memdb_gc_insert(m, fileid, ds_id, nfs_fh, fh_len, sweep_hint);
    memdb_unlock(m);
    return MDS_OK;
}

/* Copy the @cap lowest-seq rows into @entries, ascending (caller holds
 * the lock). */
static uint32_t memdb_gc_lowest_locked(const struct memdb *m,
                                       struct mds_gc_entry *entries, uint32_t cap)
{
    uint32_t n = 0;

    for (uint32_t i = 0; i < MEMDB_MAX_GC; i++) {
        const struct mds_gc_entry *e = &m->gc[i].entry;
        uint32_t pos;

        if (!m->gc[i].used) {
            continue;
        }
        pos = n;
        while (pos > 0 && entries[pos - 1].gc_seq > e->gc_seq) {
            pos--;
        }
        if (pos >= cap) {
            continue;
        }
        if (n == cap) {
            n--;
        }
        memmove(&entries[pos + 1], &entries[pos], (size_t)(n - pos) * sizeof(*entries));
        entries[pos] = *e;
        n++;
    }
    return n;
}

static enum mds_status mem_gc_peek(struct mds_catalogue *cat, struct mds_gc_entry *entry)
{
    struct memdb *m = memdb_of(cat);
    uint32_t n;

    if (entry == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    n = memdb_gc_lowest_locked(m, entry, 1);
    memdb_unlock(m);
    return (n == 1) ? MDS_OK : MDS_ERR_NOTFOUND;
}

static enum mds_status mem_gc_peek_batch(struct mds_catalogue *cat,
    struct mds_gc_entry *entries, uint32_t cap, uint32_t *n_out)
{
    struct memdb *m = memdb_of(cat);

    if (n_out != NULL) {
        *n_out = 0;
    }
    if (entries == NULL || cap == 0 || n_out == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    *n_out = memdb_gc_lowest_locked(m, entries, cap);
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_gc_dequeue(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t gc_seq)
{
    struct memdb *m = memdb_of(cat);

    (void)txn;
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_GC; i++) {
        if (m->gc[i].used && m->gc[i].entry.gc_seq == gc_seq) {
            m->gc[i].used = false;
            memdb_unlock(m);
            return MDS_OK;
        }
    }
    memdb_unlock(m);
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_gc_count(struct mds_catalogue *cat, uint32_t *count)
{
    struct memdb *m = memdb_of(cat);

    if (count == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    *count = MEMDB_MAX_GC - memdb_gc_free_count(m);
    memdb_unlock(m);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Authority ops -- shard routing, cross-shard dirents, link anchors
 * ----------------------------------------------------------------------- */

static int memdb_shard_fid_find(const struct memdb *m, uint64_t fileid)
{
    for (uint32_t i = 0; i < MEMDB_MAX_SHARD_FIDS; i++) {
        if (m->shard_fids[i].used && m->shard_fids[i].fileid == fileid) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status mem_shard_fileid_get(struct mds_catalogue *cat,
    uint64_t fileid, uint32_t *shard_id)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (shard_id == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_shard_fid_find(m, fileid);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    *shard_id = m->shard_fids[idx].shard_id;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_shard_fileid_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid, uint32_t shard_id)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    memdb_lock(m);
    idx = memdb_shard_fid_find(m, fileid);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_SHARD_FIDS; i++) {
            if (!m->shard_fids[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    m->shard_fids[idx].used = true;
    m->shard_fids[idx].fileid = fileid;
    m->shard_fids[idx].shard_id = shard_id;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_shard_fileid_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    memdb_lock(m);
    idx = memdb_shard_fid_find(m, fileid);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->shard_fids[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

static int memdb_ext_dirent_find(const struct memdb *m, uint64_t parent, const char *name)
{
    for (uint32_t i = 0; i < MEMDB_MAX_EXT_DIRENTS; i++) {
        if (m->ext_dirents[i].used && m->ext_dirents[i].parent == parent &&
            strcmp(m->ext_dirents[i].name, name) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status mem_ext_dirent_get(struct mds_catalogue *cat,
    uint64_t parent, const char *name,
    uint32_t *owner_mds_id, uint64_t *target_fileid,
    uint8_t *target_type, uint64_t *anchor_id)
{
    struct memdb *m = memdb_of(cat);
    const struct memdb_ext_dirent *e;
    int idx;

    if (name == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_ext_dirent_find(m, parent, name);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    e = &m->ext_dirents[idx];
    if (owner_mds_id != NULL) {
        *owner_mds_id = e->owner_mds_id;
    }
    if (target_fileid != NULL) {
        *target_fileid = e->target_fileid;
    }
    if (target_type != NULL) {
        *target_type = e->target_type;
    }
    if (anchor_id != NULL) {
        *anchor_id = e->anchor_id;
    }
    memdb_unlock(m);
    return MDS_OK;
}

/* Upsert keyed (parent, name). */
static enum mds_status mem_ext_dirent_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent, const char *name,
    uint32_t owner_mds_id, uint64_t target_fileid,
    uint8_t target_type, uint64_t anchor_id)
{
    struct memdb *m = memdb_of(cat);
    struct memdb_ext_dirent *e;
    int idx;

    (void)txn;
    if (!memdb_name_ok(name)) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_ext_dirent_find(m, parent, name);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_EXT_DIRENTS; i++) {
            if (!m->ext_dirents[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    e = &m->ext_dirents[idx];
    e->used = true;
    e->parent = parent;
    memdb_copy_name(e->name, sizeof(e->name), name);
    e->owner_mds_id = owner_mds_id;
    e->target_fileid = target_fileid;
    e->target_type = target_type;
    e->anchor_id = anchor_id;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_ext_dirent_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent, const char *name)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    if (name == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_ext_dirent_find(m, parent, name);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->ext_dirents[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

static int memdb_link_anchor_find(const struct memdb *m, uint64_t anchor_id)
{
    for (uint32_t i = 0; i < MEMDB_MAX_LINK_ANCHORS; i++) {
        if (m->link_anchors[i].used && m->link_anchors[i].anchor_id == anchor_id) {
            return (int)i;
        }
    }
    return -1;
}

/* Upsert keyed anchor_id. */
static enum mds_status mem_link_anchor_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t anchor_id,
    uint32_t remote_mds_id, uint64_t parent_fileid, const char *name)
{
    struct memdb *m = memdb_of(cat);
    struct memdb_link_anchor *a;
    int idx;

    (void)txn;
    if (name == NULL || strlen(name) > MDS_MAX_NAME) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_link_anchor_find(m, anchor_id);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_LINK_ANCHORS; i++) {
            if (!m->link_anchors[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    a = &m->link_anchors[idx];
    a->used = true;
    a->anchor_id = anchor_id;
    a->remote_mds_id = remote_mds_id;
    a->parent_fileid = parent_fileid;
    memdb_copy_name(a->name, sizeof(a->name), name);
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_link_anchor_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t anchor_id)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    memdb_lock(m);
    idx = memdb_link_anchor_find(m, anchor_id);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->link_anchors[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Coordination ops -- shared 2PC journal (keyed txn_id + role)
 * ----------------------------------------------------------------------- */

static int memdb_journal_find(const struct memdb *m, uint64_t txn_id, uint8_t role)
{
    for (uint32_t i = 0; i < MEMDB_MAX_JOURNAL; i++) {
        if (m->journals[i].used && m->journals[i].rec.txn_id == txn_id &&
            m->journals[i].rec.role == role) {
            return (int)i;
        }
    }
    return -1;
}

/* Upsert: a PREPARED record is replaced by the COMMITTED one for the
 * same (txn_id, role), as the RonDB row write does. */
static enum mds_status mem_journal_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, const struct mds_coord_journal_record *record)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    if (record == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_journal_find(m, record->txn_id, record->role);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_JOURNAL; i++) {
            if (!m->journals[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    m->journals[idx].used = true;
    m->journals[idx].rec = *record;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_journal_get(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t txn_id, uint8_t role,
    struct mds_coord_journal_record *record)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    if (record == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_journal_find(m, txn_id, role);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    *record = m->journals[idx].rec;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_journal_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t txn_id, uint8_t role)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    memdb_lock(m);
    idx = memdb_journal_find(m, txn_id, role);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->journals[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

/* Ascending created_at_ns; equal stamps keep insertion (slot) order. */
static int memdb_journal_cmp(const void *a, const void *b)
{
    const struct mds_coord_journal_record *ra = a;
    const struct mds_coord_journal_record *rb = b;

    if (ra->created_at_ns < rb->created_at_ns) {
        return -1;
    }
    return (ra->created_at_ns > rb->created_at_ns) ? 1 : 0;
}

/* The whole (bounded) journal is snapshotted in one hold and delivered
 * oldest first, so a recovery scan sees a transaction's history in the
 * order it was written; the snapshot is delivered without the lock. */
static enum mds_status mem_journal_scan(struct mds_catalogue *cat,
    mds_coord_journal_scan_cb cb, void *ctx)
{
    struct memdb *m = memdb_of(cat);
    struct mds_coord_journal_record *rows;
    uint32_t n = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    rows = calloc(MEMDB_MAX_JOURNAL, sizeof(*rows));
    if (rows == NULL) {
        return MDS_ERR_NOMEM;
    }
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_JOURNAL; i++) {
        if (m->journals[i].used) {
            rows[n++] = m->journals[i].rec;
        }
    }
    memdb_unlock(m);
    /* Insertion sort: stable, so equal stamps stay in slot order. */
    for (uint32_t i = 1; i < n; i++) {
        struct mds_coord_journal_record tmp = rows[i];
        uint32_t j = i;

        while (j > 0 && memdb_journal_cmp(&rows[j - 1], &tmp) > 0) {
            rows[j] = rows[j - 1];
            j--;
        }
        rows[j] = tmp;
    }
    for (uint32_t i = 0; i < n; i++) {
        if (cb(&rows[i], ctx) != 0) {
            break;
        }
    }
    free(rows);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Coordination ops -- layout state (keyed fileid + stateid.other)
 * ----------------------------------------------------------------------- */

static int memdb_layout_find(const struct memdb *m, uint64_t fileid,
                             const uint8_t other[NFS4_OTHER_SIZE])
{
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        if (m->layouts[i].used && m->layouts[i].fileid == fileid &&
            memcmp(m->layouts[i].stateid.other, other, NFS4_OTHER_SIZE) == 0) {
            return (int)i;
        }
    }
    return -1;
}

/* First row carrying @other, whatever its file (the stateid lookups
 * have no fileid to hand). */
static int memdb_layout_find_by_other(const struct memdb *m,
                                      const uint8_t other[NFS4_OTHER_SIZE])
{
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        if (m->layouts[i].used &&
            memcmp(m->layouts[i].stateid.other, other, NFS4_OTHER_SIZE) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static int memdb_layout_free_slot(const struct memdb *m)
{
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        if (!m->layouts[i].used) {
            return (int)i;
        }
    }
    return -1;
}

static void memdb_layout_clear(struct memdb_layout *l)
{
    free(l->ds_ids);
    l->ds_ids = NULL;
    l->ds_count = 0;
    l->used = false;
}

/* Private copy of the caller's DS list; NULL for an empty list. */
static enum mds_status memdb_ds_ids_copy(const uint32_t *ds_ids, uint32_t ds_count,
                                         uint32_t **out)
{
    *out = NULL;
    if (ds_count > MDS_LAYOUT_DS_ID_MAX || (ds_count > 0 && ds_ids == NULL)) {
        return MDS_ERR_INVAL;
    }
    if (ds_count > 0) {
        *out = malloc((size_t)ds_count * sizeof(**out));
        if (*out == NULL) {
            return MDS_ERR_NOMEM;
        }
        memcpy(*out, ds_ids, (size_t)ds_count * sizeof(**out));
    }
    return MDS_OK;
}

/* Overwrite @row with a full grant (the RonDB layout_state_put).
 * Takes ownership of @ids. */
static void memdb_layout_write(struct memdb_layout *row, uint64_t clientid,
                               uint64_t fileid, uint32_t iomode, uint64_t offset,
                               uint64_t length, const struct nfs4_stateid *stateid,
                               uint32_t *ids, uint32_t ds_count)
{
    free(row->ds_ids);
    row->used = true;
    row->clientid = clientid;
    row->fileid = fileid;
    row->iomode = iomode;
    row->offset = offset;
    row->length = length;
    if (stateid != NULL) {
        row->stateid = *stateid;
    } else {
        memset(&row->stateid, 0, sizeof(row->stateid));
    }
    row->ds_ids = ids;
    row->ds_count = ds_count;
}

/* Plain grant: insert, or overwrite the row with the same key. */
static enum mds_status mem_layout_grant(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t clientid, uint64_t fileid, uint32_t iomode,
    uint64_t offset, uint64_t length, const struct nfs4_stateid *stateid,
    const uint32_t *ds_ids, uint32_t ds_count)
{
    struct memdb *m = memdb_of(cat);
    static const uint8_t zero_other[NFS4_OTHER_SIZE];
    const uint8_t *other = (stateid != NULL) ? stateid->other : zero_other;
    uint32_t *ids = NULL;
    enum mds_status st;
    int idx;

    (void)txn;
    st = memdb_ds_ids_copy(ds_ids, ds_count, &ids);
    if (st != MDS_OK) {
        return st;
    }
    memdb_lock(m);
    idx = memdb_layout_find(m, fileid, other);
    if (idx < 0) {
        idx = memdb_layout_free_slot(m);
        if (idx < 0) {
            memdb_unlock(m);
            free(ids);
            return MDS_ERR_NOSPC;
        }
    }
    memdb_layout_write(&m->layouts[idx], clientid, fileid, iomode, offset, length,
                       stateid, ids, ds_count);
    memdb_unlock(m);
    return MDS_OK;
}

/*
 * Renewal union.  Mirrors the RonDB union put exactly: an absent row is
 * a full insert (including its DS list); a present row keeps a
 * SUPERSET of every range granted under the stateid
 * (layout_range_union_saturating), a monotonic seqid, an RW-dominant
 * iomode and the newest clientid, while its DS list is left as the
 * first grant wrote it (the RonDB union touches no index rows).  Read,
 * decide and write happen in one hold (C3).
 */
static enum mds_status mem_layout_grant_union(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t clientid, uint64_t fileid, uint32_t iomode,
    uint64_t offset, uint64_t length, const struct nfs4_stateid *stateid,
    const uint32_t *ds_ids, uint32_t ds_count)
{
    struct memdb *m = memdb_of(cat);
    struct memdb_layout *row;
    uint32_t *ids = NULL;
    uint64_t u_off = 0;
    uint64_t u_len = 0;
    enum mds_status st;
    int idx;

    (void)txn;
    if (stateid == NULL) {
        return MDS_ERR_INVAL;
    }
    st = memdb_ds_ids_copy(ds_ids, ds_count, &ids);
    if (st != MDS_OK) {
        return st;
    }
    memdb_lock(m);
    idx = memdb_layout_find(m, fileid, stateid->other);
    if (idx < 0) {
        idx = memdb_layout_free_slot(m);
        if (idx < 0) {
            memdb_unlock(m);
            free(ids);
            return MDS_ERR_NOSPC;
        }
        memdb_layout_write(&m->layouts[idx], clientid, fileid, iomode, offset, length,
                           stateid, ids, ds_count);
        memdb_unlock(m);
        return MDS_OK;
    }
    row = &m->layouts[idx];
    layout_range_union_saturating(row->offset, row->length, offset, length,
                                  &u_off, &u_len);
    row->offset = u_off;
    row->length = u_len;
    if (stateid->seqid > row->stateid.seqid) {
        row->stateid.seqid = stateid->seqid;
    }
    row->iomode = (row->iomode == MEMDB_LAYOUTIOMODE_RW || iomode == MEMDB_LAYOUTIOMODE_RW)
                  ? MEMDB_LAYOUTIOMODE_RW : iomode;
    row->clientid = clientid;
    memdb_unlock(m);
    free(ids); /* the existing row keeps its DS list */
    return MDS_OK;
}

/* LAYOUTRETURN / revoke: delete the row for @stateid_other, matched on
 * fileid too when the caller knows it (fileid 0 = any file). */
static enum mds_status mem_layout_return(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, const uint8_t stateid_other[12],
    uint64_t clientid, uint64_t fileid, const uint32_t *ds_ids, uint32_t ds_count)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    (void)clientid;
    (void)ds_ids;
    (void)ds_count;
    if (stateid_other == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = (fileid != 0) ? memdb_layout_find(m, fileid, stateid_other)
                        : memdb_layout_find_by_other(m, stateid_other);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    memdb_layout_clear(&m->layouts[idx]);
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_layout_get_by_stateid(struct mds_catalogue *cat,
    const uint8_t stateid_other[12], uint64_t *clientid, uint64_t *fileid,
    uint32_t *iomode, uint64_t *offset, uint64_t *length, uint32_t *seqid)
{
    struct memdb *m = memdb_of(cat);
    const struct memdb_layout *row;
    int idx;

    if (stateid_other == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_layout_find_by_other(m, stateid_other);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    row = &m->layouts[idx];
    if (clientid != NULL) {
        *clientid = row->clientid;
    }
    if (fileid != NULL) {
        *fileid = row->fileid;
    }
    if (iomode != NULL) {
        *iomode = row->iomode;
    }
    if (offset != NULL) {
        *offset = row->offset;
    }
    if (length != NULL) {
        *length = row->length;
    }
    if (seqid != NULL) {
        *seqid = row->stateid.seqid;
    }
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_layout_scan_for_file(struct mds_catalogue *cat,
    uint64_t fileid, bool *has_layout)
{
    struct memdb *m = memdb_of(cat);

    if (has_layout == NULL) {
        return MDS_ERR_INVAL;
    }
    *has_layout = false;
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        if (m->layouts[i].used && m->layouts[i].fileid == fileid) {
            *has_layout = true;
            break;
        }
    }
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_layout_del_all_for_client(struct mds_catalogue *cat,
    uint64_t clientid)
{
    struct memdb *m = memdb_of(cat);

    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        if (m->layouts[i].used && m->layouts[i].clientid == clientid) {
            memdb_layout_clear(&m->layouts[i]);
        }
    }
    memdb_unlock(m);
    return MDS_OK;
}

/* Reverse index view: every (clientid, fileid) pair with a layout row
 * naming @ds_id, delivered once (the RonDB index is keyed per DS /
 * client / file, so two stateids on one file collapse to one row). */
struct memdb_ds_idx_row {
    uint64_t clientid;
    uint64_t fileid;
};

static bool memdb_ds_idx_seen(const struct memdb_ds_idx_row *rows, uint32_t n,
                              uint64_t clientid, uint64_t fileid)
{
    for (uint32_t i = 0; i < n; i++) {
        if (rows[i].clientid == clientid && rows[i].fileid == fileid) {
            return true;
        }
    }
    return false;
}

static enum mds_status mem_ds_layout_idx_scan(struct mds_catalogue *cat,
    uint32_t ds_id, mds_coord_ds_layout_cb cb, void *ctx)
{
    struct memdb *m = memdb_of(cat);
    struct memdb_ds_idx_row *rows;
    uint32_t n = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    /* Bounded by the layout table: one row per layout at most. */
    rows = calloc(MEMDB_MAX_LAYOUTS, sizeof(*rows));
    if (rows == NULL) {
        return MDS_ERR_NOMEM;
    }
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        const struct memdb_layout *l = &m->layouts[i];
        bool on_ds = false;

        if (!l->used) {
            continue;
        }
        for (uint32_t j = 0; j < l->ds_count; j++) {
            if (l->ds_ids[j] == ds_id) {
                on_ds = true;
                break;
            }
        }
        if (on_ds && !memdb_ds_idx_seen(rows, n, l->clientid, l->fileid)) {
            rows[n].clientid = l->clientid;
            rows[n].fileid = l->fileid;
            n++;
        }
    }
    memdb_unlock(m);
    for (uint32_t i = 0; i < n; i++) {
        if (cb(rows[i].clientid, rows[i].fileid, ctx) != 0) {
            break;
        }
    }
    free(rows);
    return MDS_OK;
}

struct memdb_layout_holder {
    uint64_t            clientid;
    struct nfs4_stateid stateid;
    uint32_t            iomode;
};

/* Every holder of @fileid, materialised in one hold and delivered
 * without the lock: the recall collector re-enters the handle
 * (mds_coord_layout_get_by_stateid) from inside this callback. */
static enum mds_status mem_layout_iter_file(struct mds_catalogue *cat,
    uint64_t fileid, mds_coord_layout_file_iter_cb cb, void *ctx)
{
    struct memdb *m = memdb_of(cat);
    struct memdb_layout_holder *rows;
    uint32_t n = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    rows = calloc(MEMDB_MAX_LAYOUTS, sizeof(*rows));
    if (rows == NULL) {
        return MDS_ERR_NOMEM;
    }
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        const struct memdb_layout *l = &m->layouts[i];

        if (l->used && l->fileid == fileid) {
            rows[n].clientid = l->clientid;
            rows[n].stateid = l->stateid;
            rows[n].iomode = l->iomode;
            n++;
        }
    }
    memdb_unlock(m);
    for (uint32_t i = 0; i < n; i++) {
        if (cb(rows[i].clientid, &rows[i].stateid, rows[i].iomode, ctx) != 0) {
            break;
        }
    }
    free(rows);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Coordination ops -- client recovery (keyed clientid)
 * ----------------------------------------------------------------------- */

static int memdb_recovery_find(const struct memdb *m, uint64_t clientid)
{
    for (uint32_t i = 0; i < MEMDB_MAX_RECOVERY; i++) {
        if (m->recoveries[i].used && m->recoveries[i].clientid == clientid) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status mem_recovery_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t clientid,
    const uint8_t *co_ownerid, uint32_t co_ownerid_len, const uint8_t verifier[8])
{
    struct memdb *m = memdb_of(cat);
    struct memdb_recovery *r;
    int idx;

    (void)txn;
    if (verifier == NULL || co_ownerid_len > sizeof(r->co_ownerid) ||
        (co_ownerid_len > 0 && co_ownerid == NULL)) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_recovery_find(m, clientid);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_RECOVERY; i++) {
            if (!m->recoveries[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    r = &m->recoveries[idx];
    r->used = true;
    r->clientid = clientid;
    r->owner_mds_id = m->self_mds_id;
    r->owner_boot_epoch = 0;
    memset(r->co_ownerid, 0, sizeof(r->co_ownerid));
    if (co_ownerid_len > 0) {
        memcpy(r->co_ownerid, co_ownerid, co_ownerid_len);
    }
    r->co_ownerid_len = co_ownerid_len;
    memcpy(r->verifier, verifier, sizeof(r->verifier));
    memdb_unlock(m);
    return MDS_OK;
}

/* Idempotent, like the RonDB delete (a missing row is MDS_OK). */
static enum mds_status mem_recovery_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t clientid)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    (void)txn;
    memdb_lock(m);
    idx = memdb_recovery_find(m, clientid);
    if (idx >= 0) {
        m->recoveries[idx].used = false;
    }
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_recovery_get(struct mds_catalogue *cat,
    uint64_t clientid, uint8_t *co_ownerid, uint32_t *co_ownerid_len,
    uint8_t verifier[8])
{
    struct memdb *m = memdb_of(cat);
    const struct memdb_recovery *r;
    int idx;

    memdb_lock(m);
    idx = memdb_recovery_find(m, clientid);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    r = &m->recoveries[idx];
    if (co_ownerid != NULL && r->co_ownerid_len > 0) {
        memcpy(co_ownerid, r->co_ownerid, r->co_ownerid_len);
    }
    if (co_ownerid_len != NULL) {
        *co_ownerid_len = r->co_ownerid_len;
    }
    if (verifier != NULL) {
        memcpy(verifier, r->verifier, sizeof(r->verifier));
    }
    memdb_unlock(m);
    return MDS_OK;
}

/* One materialised recovery row for the paged listing. */
struct memdb_recovery_page_row {
    uint64_t clientid;
    uint32_t owner_mds_id;
    uint64_t owner_boot_epoch;
};

/* The rows owned by @owner_mds_id plus the unassigned ones (stored
 * owner 0: written by an instance without an identity), like the RonDB
 * scan filter; @owner_mds_id 0 lists every row.  Rows explicitly owned
 * by another MDS are never returned.  The callback receives the stored
 * owner and epoch.  A promoting standby lists its dead partner's id; an
 * MDS matching EXCHANGE_IDs in grace lists its own. */
static enum mds_status mem_recovery_list(struct mds_catalogue *cat,
    uint32_t owner_mds_id, mds_recovery_list_cb cb, void *ctx)
{
    struct memdb *m = memdb_of(cat);
    struct memdb_recovery_page_row *page;
    uint32_t cursor = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    page = calloc(MEMDB_SCAN_PAGE, sizeof(*page));
    if (page == NULL) {
        return MDS_ERR_NOMEM;
    }
    while (cursor < MEMDB_MAX_RECOVERY) {
        uint32_t n = 0;

        memdb_lock(m);
        while (cursor < MEMDB_MAX_RECOVERY && n < MEMDB_SCAN_PAGE) {
            const struct memdb_recovery *r = &m->recoveries[cursor];

            if (r->used &&
                (owner_mds_id == 0 || r->owner_mds_id == 0 ||
                 r->owner_mds_id == owner_mds_id)) {
                page[n].clientid = r->clientid;
                page[n].owner_mds_id = r->owner_mds_id;
                page[n].owner_boot_epoch = r->owner_boot_epoch;
                n++;
            }
            cursor++;
        }
        memdb_unlock(m);
        for (uint32_t i = 0; i < n; i++) {
            if (cb(page[i].clientid, page[i].owner_mds_id,
                   page[i].owner_boot_epoch, ctx) != 0) {
                free(page);
                return MDS_OK;
            }
        }
    }
    free(page);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Coordination ops -- open/share state (keyed stateid.other)
 * ----------------------------------------------------------------------- */

static int memdb_open_find(const struct memdb *m, const uint8_t other[NFS4_OTHER_SIZE])
{
    for (uint32_t i = 0; i < MEMDB_MAX_OPENS; i++) {
        if (m->opens[i].used &&
            memcmp(m->opens[i].row.stateid_other, other, NFS4_OTHER_SIZE) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status mem_open_put(struct mds_catalogue *cat,
    const struct mds_coord_open_row *row)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (row == NULL || row->open_owner_len > sizeof(row->open_owner)) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_open_find(m, row->stateid_other);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_OPENS; i++) {
            if (!m->opens[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    m->opens[idx].used = true;
    m->opens[idx].row = *row;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_open_get(struct mds_catalogue *cat,
    const uint8_t stateid_other[12], struct mds_coord_open_row *row)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (stateid_other == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_open_find(m, stateid_other);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    *row = m->opens[idx].row;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_open_del(struct mds_catalogue *cat,
    const uint8_t stateid_other[12])
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (stateid_other == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_open_find(m, stateid_other);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->opens[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

/* Paged scan of the open table by either key; NULL filter = all rows of
 * the other key. */
static enum mds_status memdb_open_scan(struct memdb *m, const uint64_t *fileid,
                                       const uint64_t *clientid,
                                       mds_coord_open_scan_cb cb, void *ctx)
{
    struct mds_coord_open_row *page;
    uint32_t cursor = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    page = calloc(MEMDB_SCAN_PAGE, sizeof(*page));
    if (page == NULL) {
        return MDS_ERR_NOMEM;
    }
    while (cursor < MEMDB_MAX_OPENS) {
        uint32_t n = 0;

        memdb_lock(m);
        while (cursor < MEMDB_MAX_OPENS && n < MEMDB_SCAN_PAGE) {
            const struct memdb_open *o = &m->opens[cursor];

            cursor++;
            if (!o->used) {
                continue;
            }
            if (fileid != NULL && o->row.fileid != *fileid) {
                continue;
            }
            if (clientid != NULL && o->row.clientid != *clientid) {
                continue;
            }
            page[n++] = o->row;
        }
        memdb_unlock(m);
        for (uint32_t i = 0; i < n; i++) {
            if (cb(&page[i], ctx) != 0) {
                free(page);
                return MDS_OK;
            }
        }
    }
    free(page);
    return MDS_OK;
}

static enum mds_status mem_open_scan_file(struct mds_catalogue *cat,
    uint64_t fileid, mds_coord_open_scan_cb cb, void *ctx)
{
    return memdb_open_scan(memdb_of(cat), &fileid, NULL, cb, ctx);
}

static enum mds_status mem_open_scan_client(struct mds_catalogue *cat,
    uint64_t clientid, mds_coord_open_scan_cb cb, void *ctx)
{
    return memdb_open_scan(memdb_of(cat), NULL, &clientid, cb, ctx);
}

/* -----------------------------------------------------------------------
 * Coordination ops -- byte-range locks (keyed fileid + lock_id)
 * ----------------------------------------------------------------------- */

static int memdb_lock_find(const struct memdb *m, uint64_t fileid, uint64_t lock_id)
{
    for (uint32_t i = 0; i < MEMDB_MAX_LOCKS; i++) {
        if (m->locks[i].used && m->locks[i].row.fileid == fileid &&
            m->locks[i].row.lock_id == lock_id) {
            return (int)i;
        }
    }
    return -1;
}

static bool memdb_lock_owner_eq(const struct mds_coord_lock_row *row, uint64_t clientid,
                                const uint8_t *owner, uint32_t owner_len)
{
    if (row->clientid != clientid || row->owner_len != owner_len) {
        return false;
    }
    return owner_len == 0 || memcmp(row->owner, owner, owner_len) == 0;
}

/* Exclusive end of a lock range; 0 and UINT64_MAX mean "to EOF". */
static uint64_t memdb_lock_end(uint64_t offset, uint64_t length)
{
    if (length == 0 || length == UINT64_MAX || length > UINT64_MAX - offset) {
        return UINT64_MAX;
    }
    return offset + length;
}

static bool memdb_lock_is_read(uint32_t lock_type)
{
    return lock_type == MEMDB_READ_LT || lock_type == MEMDB_READW_LT;
}

static enum mds_status mem_lock_put(struct mds_catalogue *cat,
    const struct mds_coord_lock_row *row)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (row == NULL || row->owner_len > sizeof(row->owner)) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_lock_find(m, row->fileid, row->lock_id);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_LOCKS; i++) {
            if (!m->locks[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    m->locks[idx].used = true;
    m->locks[idx].row = *row;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_lock_del(struct mds_catalogue *cat,
    uint64_t fileid, uint64_t lock_id)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    memdb_lock(m);
    idx = memdb_lock_find(m, fileid, lock_id);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->locks[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

/*
 * LOCKT against the persisted rows: a row on @fileid held by a
 * DIFFERENT (clientid, owner) whose range overlaps the request and is
 * not read-vs-read is a conflict.  MDS_OK means the range is free
 * (*conflict zeroed); MDS_ERR_EXISTS means a conflicting lock exists
 * and *conflict holds its row.  A caller's own rows never conflict
 * (POSIX replace / upgrade).
 */
static enum mds_status mem_lock_test(struct mds_catalogue *cat,
    uint64_t fileid, uint32_t lock_type, uint64_t offset, uint64_t length,
    uint64_t clientid, const uint8_t *owner, uint32_t owner_len,
    struct mds_coord_lock_row *conflict)
{
    struct memdb *m = memdb_of(cat);
    uint64_t end = memdb_lock_end(offset, length);
    bool req_read = memdb_lock_is_read(lock_type);

    if ((owner_len > 0 && owner == NULL) || conflict == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(conflict, 0, sizeof(*conflict));
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_LOCKS; i++) {
        const struct mds_coord_lock_row *r = &m->locks[i].row;
        uint64_t r_end;

        if (!m->locks[i].used || r->fileid != fileid) {
            continue;
        }
        if (memdb_lock_owner_eq(r, clientid, owner, owner_len)) {
            continue;
        }
        r_end = memdb_lock_end(r->offset, r->length);
        if (r->offset >= end || offset >= r_end) {
            continue; /* disjoint */
        }
        if (req_read && memdb_lock_is_read(r->lock_type)) {
            continue; /* read vs read */
        }
        *conflict = *r;
        memdb_unlock(m);
        return MDS_ERR_EXISTS;
    }
    memdb_unlock(m);
    return MDS_OK;
}

/* Paged scan of the lock table: by file, or by (clientid, owner). */
static enum mds_status memdb_lock_scan(struct memdb *m, const uint64_t *fileid,
                                       const uint64_t *clientid, const uint8_t *owner,
                                       uint32_t owner_len,
                                       mds_coord_lock_scan_cb cb, void *ctx)
{
    struct mds_coord_lock_row *page;
    uint32_t cursor = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    page = calloc(MEMDB_SCAN_PAGE, sizeof(*page));
    if (page == NULL) {
        return MDS_ERR_NOMEM;
    }
    while (cursor < MEMDB_MAX_LOCKS) {
        uint32_t n = 0;

        memdb_lock(m);
        while (cursor < MEMDB_MAX_LOCKS && n < MEMDB_SCAN_PAGE) {
            const struct memdb_lock *l = &m->locks[cursor];

            cursor++;
            if (!l->used) {
                continue;
            }
            if (fileid != NULL && l->row.fileid != *fileid) {
                continue;
            }
            if (clientid != NULL &&
                !memdb_lock_owner_eq(&l->row, *clientid, owner, owner_len)) {
                continue;
            }
            page[n++] = l->row;
        }
        memdb_unlock(m);
        for (uint32_t i = 0; i < n; i++) {
            if (cb(&page[i], ctx) != 0) {
                free(page);
                return MDS_OK;
            }
        }
    }
    free(page);
    return MDS_OK;
}

static enum mds_status mem_lock_scan_file(struct mds_catalogue *cat,
    uint64_t fileid, mds_coord_lock_scan_cb cb, void *ctx)
{
    return memdb_lock_scan(memdb_of(cat), &fileid, NULL, NULL, 0, cb, ctx);
}

/* Every row of one lock-owner (LOCKU lookup). */
static enum mds_status mem_lock_scan_owner(struct mds_catalogue *cat,
    uint64_t clientid, const uint8_t *owner, uint32_t owner_len,
    mds_coord_lock_scan_cb cb, void *ctx)
{
    if (owner_len > 0 && owner == NULL) {
        return MDS_ERR_INVAL;
    }
    return memdb_lock_scan(memdb_of(cat), NULL, &clientid, owner, owner_len, cb, ctx);
}

static enum mds_status mem_lock_reap_client(struct mds_catalogue *cat, uint64_t clientid)
{
    struct memdb *m = memdb_of(cat);

    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_LOCKS; i++) {
        if (m->locks[i].used && m->locks[i].row.clientid == clientid) {
            m->locks[i].used = false;
        }
    }
    memdb_unlock(m);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Coordination ops -- delegations (keyed stateid.other)
 * ----------------------------------------------------------------------- */

static int memdb_deleg_find(const struct memdb *m, const uint8_t other[NFS4_OTHER_SIZE])
{
    for (uint32_t i = 0; i < MEMDB_MAX_DELEGS; i++) {
        if (m->delegs[i].used &&
            memcmp(m->delegs[i].row.stateid_other, other, NFS4_OTHER_SIZE) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status mem_deleg_put(struct mds_catalogue *cat,
    const struct mds_coord_deleg_row *row)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (row == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_deleg_find(m, row->stateid_other);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_DELEGS; i++) {
            if (!m->delegs[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    m->delegs[idx].used = true;
    m->delegs[idx].row = *row;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_deleg_get(struct mds_catalogue *cat,
    const uint8_t stateid_other[12], struct mds_coord_deleg_row *row)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (stateid_other == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_deleg_find(m, stateid_other);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    *row = m->delegs[idx].row;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_deleg_del(struct mds_catalogue *cat,
    const uint8_t stateid_other[12])
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (stateid_other == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_deleg_find(m, stateid_other);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->delegs[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status memdb_deleg_scan(struct memdb *m, const uint64_t *fileid,
                                        const uint64_t *clientid,
                                        mds_coord_deleg_scan_cb cb, void *ctx)
{
    struct mds_coord_deleg_row *page;
    uint32_t cursor = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    page = calloc(MEMDB_SCAN_PAGE, sizeof(*page));
    if (page == NULL) {
        return MDS_ERR_NOMEM;
    }
    while (cursor < MEMDB_MAX_DELEGS) {
        uint32_t n = 0;

        memdb_lock(m);
        while (cursor < MEMDB_MAX_DELEGS && n < MEMDB_SCAN_PAGE) {
            const struct memdb_deleg *d = &m->delegs[cursor];

            cursor++;
            if (!d->used) {
                continue;
            }
            if (fileid != NULL && d->row.fileid != *fileid) {
                continue;
            }
            if (clientid != NULL && d->row.clientid != *clientid) {
                continue;
            }
            page[n++] = d->row;
        }
        memdb_unlock(m);
        for (uint32_t i = 0; i < n; i++) {
            if (cb(&page[i], ctx) != 0) {
                free(page);
                return MDS_OK;
            }
        }
    }
    free(page);
    return MDS_OK;
}

static enum mds_status mem_deleg_scan_file(struct mds_catalogue *cat,
    uint64_t fileid, mds_coord_deleg_scan_cb cb, void *ctx)
{
    return memdb_deleg_scan(memdb_of(cat), &fileid, NULL, cb, ctx);
}

static enum mds_status mem_deleg_scan_client(struct mds_catalogue *cat,
    uint64_t clientid, mds_coord_deleg_scan_cb cb, void *ctx)
{
    return memdb_deleg_scan(memdb_of(cat), NULL, &clientid, cb, ctx);
}

/* -----------------------------------------------------------------------
 * Coordination ops -- client identity (keyed clientid)
 * ----------------------------------------------------------------------- */

static int memdb_client_find(const struct memdb *m, uint64_t clientid)
{
    for (uint32_t i = 0; i < MEMDB_MAX_CLIENTS; i++) {
        if (m->clients[i].used && m->clients[i].row.clientid == clientid) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status mem_client_put(struct mds_catalogue *cat,
    const struct mds_coord_client_row *row)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (row == NULL || row->co_ownerid_len > sizeof(row->co_ownerid)) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_client_find(m, row->clientid);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_CLIENTS; i++) {
            if (!m->clients[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    m->clients[idx].used = true;
    m->clients[idx].row = *row;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_client_get(struct mds_catalogue *cat,
    uint64_t clientid, struct mds_coord_client_row *row)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (row == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_client_find(m, clientid);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    *row = m->clients[idx].row;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_client_del(struct mds_catalogue *cat, uint64_t clientid)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    memdb_lock(m);
    idx = memdb_client_find(m, clientid);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->clients[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Coordination ops -- sessions (keyed session_id)
 * ----------------------------------------------------------------------- */

static int memdb_session_find(const struct memdb *m, const uint8_t session_id[16])
{
    for (uint32_t i = 0; i < MEMDB_MAX_SESSIONS; i++) {
        if (m->sessions[i].used &&
            memcmp(m->sessions[i].row.session_id, session_id, 16) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status mem_session_put(struct mds_catalogue *cat,
    const struct mds_coord_session_row *row)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (row == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_session_find(m, row->session_id);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_SESSIONS; i++) {
            if (!m->sessions[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    m->sessions[idx].used = true;
    m->sessions[idx].row = *row;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_session_get(struct mds_catalogue *cat,
    const uint8_t session_id[16], struct mds_coord_session_row *row)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (session_id == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_session_find(m, session_id);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    *row = m->sessions[idx].row;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_session_del(struct mds_catalogue *cat,
    const uint8_t session_id[16])
{
    struct memdb *m = memdb_of(cat);
    int idx;

    if (session_id == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_session_find(m, session_id);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    m->sessions[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

static enum mds_status mem_session_scan_client(struct mds_catalogue *cat,
    uint64_t clientid, mds_coord_session_scan_cb cb, void *ctx)
{
    struct memdb *m = memdb_of(cat);
    struct mds_coord_session_row *page;
    uint32_t cursor = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    page = calloc(MEMDB_SCAN_PAGE, sizeof(*page));
    if (page == NULL) {
        return MDS_ERR_NOMEM;
    }
    while (cursor < MEMDB_MAX_SESSIONS) {
        uint32_t n = 0;

        memdb_lock(m);
        while (cursor < MEMDB_MAX_SESSIONS && n < MEMDB_SCAN_PAGE) {
            const struct memdb_session *s = &m->sessions[cursor];

            cursor++;
            if (s->used && s->row.clientid == clientid) {
                page[n++] = s->row;
            }
        }
        memdb_unlock(m);
        for (uint32_t i = 0; i < n; i++) {
            if (cb(&page[i], ctx) != 0) {
                free(page);
                return MDS_OK;
            }
        }
    }
    free(page);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Coordination ops -- DRC slots (keyed session_id + slot_id)
 * ----------------------------------------------------------------------- */

static int memdb_drc_find(const struct memdb *m, const uint8_t session_id[16],
                          uint32_t slot_id)
{
    for (uint32_t i = 0; i < MEMDB_MAX_DRC_SLOTS; i++) {
        if (m->drc_slots[i].used && m->drc_slots[i].slot_id == slot_id &&
            memcmp(m->drc_slots[i].session_id, session_id, 16) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status mem_slot_put(struct mds_catalogue *cat,
    const uint8_t session_id[16], uint32_t slot_id, uint32_t seq_id,
    const void *cached_reply, uint32_t reply_len)
{
    struct memdb *m = memdb_of(cat);
    struct memdb_drc_slot *s;
    uint8_t *copy = NULL;
    int idx;

    if (session_id == NULL || (reply_len > 0 && cached_reply == NULL)) {
        return MDS_ERR_INVAL;
    }
    if (reply_len > 0) {
        copy = malloc(reply_len);
        if (copy == NULL) {
            return MDS_ERR_NOMEM;
        }
        memcpy(copy, cached_reply, reply_len);
    }
    memdb_lock(m);
    idx = memdb_drc_find(m, session_id, slot_id);
    if (idx < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_DRC_SLOTS; i++) {
            if (!m->drc_slots[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            free(copy);
            return MDS_ERR_NOSPC;
        }
    }
    s = &m->drc_slots[idx];
    free(s->reply);
    s->used = true;
    memcpy(s->session_id, session_id, sizeof(s->session_id));
    s->slot_id = slot_id;
    s->seq_id = seq_id;
    s->reply = copy;
    s->reply_len = reply_len;
    s->last_used_ns = memdb_now_ns();
    memdb_unlock(m);
    return MDS_OK;
}

/* row->cached_reply is a heap copy the caller frees (NULL when empty). */
static enum mds_status mem_slot_get(struct mds_catalogue *cat,
    const uint8_t session_id[16], uint32_t slot_id,
    struct mds_coord_drc_slot_row *row)
{
    struct memdb *m = memdb_of(cat);
    const struct memdb_drc_slot *s;
    uint8_t *copy = NULL;
    int idx;

    if (session_id == NULL || row == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_drc_find(m, session_id, slot_id);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    s = &m->drc_slots[idx];
    if (s->reply_len > 0) {
        copy = malloc(s->reply_len);
        if (copy == NULL) {
            memdb_unlock(m);
            return MDS_ERR_NOMEM;
        }
        memcpy(copy, s->reply, s->reply_len);
    }
    memset(row, 0, sizeof(*row));
    memcpy(row->session_id, s->session_id, sizeof(row->session_id));
    row->slot_id = s->slot_id;
    row->seq_id = s->seq_id;
    row->cached_reply = copy;
    row->reply_len = s->reply_len;
    row->last_used_ns = s->last_used_ns;
    memdb_unlock(m);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Cluster ops -- node registry and partition map
 *
 * The in-memory implementation of the target contract in
 * mds_cluster.h: conditional register, epoch-checked heartbeat and
 * deregister, CLOCK_REALTIME timestamps, insert-only root claim.  It
 * exists so the in-process multi-MDS tests exercise the cluster glue
 * against ONE shared instance; the handle never carries
 * MDS_CAT_CAP_MULTI_PROCESS, so mds_cluster_supported() stays false.
 * ----------------------------------------------------------------------- */

static int memdb_node_find(const struct memdb *m, uint32_t mds_id)
{
    for (uint32_t i = 0; i < MEMDB_MAX_NODES; i++) {
        if (m->nodes[i].used && m->nodes[i].mds_id == mds_id) {
            return (int)i;
        }
    }
    return -1;
}

/* Conditional upsert: insert when absent, replace when the existing
 * row's boot_epoch is lower, MDS_ERR_EXISTS otherwise (a duplicate
 * live mds_id is split-brain and is refused). */
static enum mds_status mem_node_register(struct mds_catalogue *cat,
    uint32_t mds_id, uint64_t boot_epoch, const char *hostname,
    uint16_t nfs_port, uint16_t grpc_port)
{
    struct memdb *m = memdb_of(cat);
    struct memdb_node *n;
    int idx;

    if (hostname == NULL || strlen(hostname) > MEMDB_HOSTNAME_MAX) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_node_find(m, mds_id);
    if (idx >= 0) {
        if (m->nodes[idx].boot_epoch >= boot_epoch) {
            memdb_unlock(m);
            return MDS_ERR_EXISTS;
        }
    } else {
        for (uint32_t i = 0; i < MEMDB_MAX_NODES; i++) {
            if (!m->nodes[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    n = &m->nodes[idx];
    n->used = true;
    n->mds_id = mds_id;
    n->boot_epoch = boot_epoch;
    n->nfs_port = nfs_port;
    n->grpc_port = grpc_port;
    n->last_heartbeat_ns = memdb_now_ns();
    memdb_copy_name(n->hostname, sizeof(n->hostname), hostname);
    memdb_unlock(m);
    return MDS_OK;
}

/* Update only when the row exists AND its epoch matches: NOTFOUND when
 * absent, STALE on a mismatch (the old incarnation never overwrites
 * its replacement). */
static enum mds_status mem_node_heartbeat(struct mds_catalogue *cat,
    uint32_t mds_id, uint64_t boot_epoch)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    memdb_lock(m);
    idx = memdb_node_find(m, mds_id);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    if (m->nodes[idx].boot_epoch != boot_epoch) {
        memdb_unlock(m);
        return MDS_ERR_STALE;
    }
    m->nodes[idx].last_heartbeat_ns = memdb_now_ns();
    memdb_unlock(m);
    return MDS_OK;
}

/* Delete only on an epoch match; an absent row is MDS_OK (a retried
 * shutdown is harmless), a mismatch is STALE and deletes nothing. */
static enum mds_status mem_node_deregister(struct mds_catalogue *cat,
    uint32_t mds_id, uint64_t boot_epoch)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    memdb_lock(m);
    idx = memdb_node_find(m, mds_id);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_OK;
    }
    if (m->nodes[idx].boot_epoch != boot_epoch) {
        memdb_unlock(m);
        return MDS_ERR_STALE;
    }
    m->nodes[idx].used = false;
    memdb_unlock(m);
    return MDS_OK;
}

/* Snapshot of the whole registry (bounded by MEMDB_MAX_NODES) taken in
 * one hold, delivered without the lock.  @stale_before == 0 lists
 * every row; otherwise only rows whose heartbeat is older. */
static enum mds_status memdb_node_snapshot(struct memdb *m, uint64_t stale_before,
                                           struct memdb_node **rows_out, uint32_t *n_out)
{
    struct memdb_node *rows;
    uint32_t n = 0;

    rows = calloc(MEMDB_MAX_NODES, sizeof(*rows));
    if (rows == NULL) {
        return MDS_ERR_NOMEM;
    }
    memdb_lock(m);
    for (uint32_t i = 0; i < MEMDB_MAX_NODES; i++) {
        if (!m->nodes[i].used) {
            continue;
        }
        if (stale_before != 0 && m->nodes[i].last_heartbeat_ns >= stale_before) {
            continue;
        }
        rows[n++] = m->nodes[i];
    }
    memdb_unlock(m);
    *rows_out = rows;
    *n_out = n;
    return MDS_OK;
}

static enum mds_status mem_node_list(struct mds_catalogue *cat,
    mds_cluster_node_cb cb, void *ctx)
{
    struct memdb_node *rows = NULL;
    uint32_t n = 0;
    enum mds_status st;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    st = memdb_node_snapshot(memdb_of(cat), 0, &rows, &n);
    if (st != MDS_OK) {
        return st;
    }
    for (uint32_t i = 0; i < n; i++) {
        if (cb(rows[i].mds_id, rows[i].boot_epoch, rows[i].hostname,
               rows[i].nfs_port, rows[i].grpc_port, rows[i].last_heartbeat_ns, ctx) != 0) {
            break;
        }
    }
    free(rows);
    return MDS_OK;
}

/* Rows whose last_heartbeat_ns < threshold_ns (same CLOCK_REALTIME
 * domain the heartbeat writes). */
static enum mds_status mem_node_scan_stale(struct mds_catalogue *cat,
    uint64_t threshold_ns, mds_cluster_stale_cb cb, void *ctx)
{
    struct memdb_node *rows = NULL;
    uint32_t n = 0;
    enum mds_status st;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    if (threshold_ns == 0) {
        return MDS_OK; /* nothing is older than the epoch */
    }
    st = memdb_node_snapshot(memdb_of(cat), threshold_ns, &rows, &n);
    if (st != MDS_OK) {
        return st;
    }
    for (uint32_t i = 0; i < n; i++) {
        if (cb(rows[i].mds_id, rows[i].boot_epoch, rows[i].last_heartbeat_ns, ctx) != 0) {
            break;
        }
    }
    free(rows);
    return MDS_OK;
}

static int memdb_partition_find(const struct memdb *m, uint32_t partition_id)
{
    for (uint32_t i = 0; i < MEMDB_MAX_PARTITIONS; i++) {
        if (m->partitions[i].used && m->partitions[i].partition_id == partition_id) {
            return (int)i;
        }
    }
    return -1;
}

static enum mds_status mem_partition_list(struct mds_catalogue *cat,
    mds_cluster_partition_cb cb, void *ctx)
{
    struct memdb *m = memdb_of(cat);
    struct memdb_partition *page;
    uint32_t cursor = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }
    page = calloc(MEMDB_SCAN_PAGE, sizeof(*page));
    if (page == NULL) {
        return MDS_ERR_NOMEM;
    }
    while (cursor < MEMDB_MAX_PARTITIONS) {
        uint32_t n = 0;

        memdb_lock(m);
        while (cursor < MEMDB_MAX_PARTITIONS && n < MEMDB_SCAN_PAGE) {
            if (m->partitions[cursor].used) {
                page[n++] = m->partitions[cursor];
            }
            cursor++;
        }
        memdb_unlock(m);
        for (uint32_t i = 0; i < n; i++) {
            if (cb(page[i].partition_id, page[i].owner_mds_id, page[i].state,
                   page[i].subtree_path, ctx) != 0) {
                free(page);
                return MDS_OK;
            }
        }
    }
    free(page);
    return MDS_OK;
}

/* insert_only honoured: EXISTS when the row exists; otherwise upsert. */
static enum mds_status mem_partition_put(struct mds_catalogue *cat,
    uint32_t partition_id, uint32_t owner_mds_id, uint8_t state,
    const char *subtree_path, bool insert_only)
{
    struct memdb *m = memdb_of(cat);
    struct memdb_partition *p;
    int idx;

    if (subtree_path == NULL || strlen(subtree_path) >= MDS_MAX_PATH) {
        return MDS_ERR_INVAL;
    }
    memdb_lock(m);
    idx = memdb_partition_find(m, partition_id);
    if (idx >= 0) {
        if (insert_only) {
            memdb_unlock(m);
            return MDS_ERR_EXISTS;
        }
    } else {
        for (uint32_t i = 0; i < MEMDB_MAX_PARTITIONS; i++) {
            if (!m->partitions[i].used) {
                idx = (int)i;
                break;
            }
        }
        if (idx < 0) {
            memdb_unlock(m);
            return MDS_ERR_NOSPC;
        }
    }
    p = &m->partitions[idx];
    p->used = true;
    p->partition_id = partition_id;
    p->owner_mds_id = owner_mds_id;
    p->state = state;
    memdb_copy_name(p->subtree_path, sizeof(p->subtree_path), subtree_path);
    memdb_unlock(m);
    return MDS_OK;
}

/* Owner CAS: compare and write under the one instance lock, so two
 * takeovers racing for the same row see exactly one winner. */
static enum mds_status mem_partition_cas(struct mds_catalogue *cat,
    uint32_t partition_id, uint32_t expected_owner, uint32_t new_owner,
    uint8_t new_state)
{
    struct memdb *m = memdb_of(cat);
    int idx;

    memdb_lock(m);
    idx = memdb_partition_find(m, partition_id);
    if (idx < 0) {
        memdb_unlock(m);
        return MDS_ERR_NOTFOUND;
    }
    if (m->partitions[idx].owner_mds_id != expected_owner) {
        memdb_unlock(m);
        return MDS_ERR_STALE;
    }
    m->partitions[idx].owner_mds_id = new_owner;
    m->partitions[idx].state = new_state;
    memdb_unlock(m);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */

static void memdb_free_tables(struct memdb *m)
{
    if (m->inlines != NULL) {
        for (uint32_t i = 0; i < MEMDB_MAX_INLINE; i++) {
            free(m->inlines[i].data);
        }
    }
    if (m->xattrs != NULL) {
        for (uint32_t i = 0; i < MEMDB_MAX_XATTRS; i++) {
            free(m->xattrs[i].val);
        }
    }
    if (m->stripes != NULL) {
        for (uint32_t i = 0; i < MEMDB_MAX_STRIPE_MAPS; i++) {
            free(m->stripes[i].entries);
        }
    }
    if (m->layouts != NULL) {
        for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
            free(m->layouts[i].ds_ids);
        }
    }
    if (m->drc_slots != NULL) {
        for (uint32_t i = 0; i < MEMDB_MAX_DRC_SLOTS; i++) {
            free(m->drc_slots[i].reply);
        }
    }
    free(m->inodes);
    free(m->dirents);
    free(m->inlines);
    free(m->xattrs);
    free(m->stripes);
    free(m->ds);
    free(m->provisions);
    free(m->quota_rules);
    free(m->quota_usage);
    free(m->gc);
    free(m->remove_pending);
    free(m->shard_fids);
    free(m->ext_dirents);
    free(m->link_anchors);
    free(m->journals);
    free(m->layouts);
    free(m->recoveries);
    free(m->opens);
    free(m->locks);
    free(m->delegs);
    free(m->clients);
    free(m->sessions);
    free(m->drc_slots);
    free(m->nodes);
    free(m->partitions);
}

/* Every table is allocated here, once, at its compile-time bound. */
static bool memdb_alloc_tables(struct memdb *m)
{
    m->inodes = calloc(MEMDB_MAX_INODES, sizeof(*m->inodes));
    m->dirents = calloc(MEMDB_MAX_DIRENTS, sizeof(*m->dirents));
    m->inlines = calloc(MEMDB_MAX_INLINE, sizeof(*m->inlines));
    m->xattrs = calloc(MEMDB_MAX_XATTRS, sizeof(*m->xattrs));
    m->stripes = calloc(MEMDB_MAX_STRIPE_MAPS, sizeof(*m->stripes));
    m->ds = calloc(MEMDB_MAX_DS, sizeof(*m->ds));
    m->provisions = calloc(MEMDB_MAX_PROVISION, sizeof(*m->provisions));
    m->quota_rules = calloc(MEMDB_MAX_QUOTA_RULES, sizeof(*m->quota_rules));
    m->quota_usage = calloc(MEMDB_MAX_QUOTA_USAGE, sizeof(*m->quota_usage));
    m->gc = calloc(MEMDB_MAX_GC, sizeof(*m->gc));
    m->remove_pending = calloc(MEMDB_MAX_REMOVE_PENDING, sizeof(*m->remove_pending));
    m->shard_fids = calloc(MEMDB_MAX_SHARD_FIDS, sizeof(*m->shard_fids));
    m->ext_dirents = calloc(MEMDB_MAX_EXT_DIRENTS, sizeof(*m->ext_dirents));
    m->link_anchors = calloc(MEMDB_MAX_LINK_ANCHORS, sizeof(*m->link_anchors));
    m->journals = calloc(MEMDB_MAX_JOURNAL, sizeof(*m->journals));
    m->layouts = calloc(MEMDB_MAX_LAYOUTS, sizeof(*m->layouts));
    m->recoveries = calloc(MEMDB_MAX_RECOVERY, sizeof(*m->recoveries));
    m->opens = calloc(MEMDB_MAX_OPENS, sizeof(*m->opens));
    m->locks = calloc(MEMDB_MAX_LOCKS, sizeof(*m->locks));
    m->delegs = calloc(MEMDB_MAX_DELEGS, sizeof(*m->delegs));
    m->clients = calloc(MEMDB_MAX_CLIENTS, sizeof(*m->clients));
    m->sessions = calloc(MEMDB_MAX_SESSIONS, sizeof(*m->sessions));
    m->drc_slots = calloc(MEMDB_MAX_DRC_SLOTS, sizeof(*m->drc_slots));
    m->nodes = calloc(MEMDB_MAX_NODES, sizeof(*m->nodes));
    m->partitions = calloc(MEMDB_MAX_PARTITIONS, sizeof(*m->partitions));

    return m->inodes != NULL && m->dirents != NULL && m->inlines != NULL &&
           m->xattrs != NULL && m->stripes != NULL && m->ds != NULL &&
           m->provisions != NULL && m->quota_rules != NULL && m->quota_usage != NULL &&
           m->gc != NULL && m->remove_pending != NULL && m->shard_fids != NULL &&
           m->ext_dirents != NULL && m->link_anchors != NULL && m->journals != NULL &&
           m->layouts != NULL && m->recoveries != NULL && m->opens != NULL &&
           m->locks != NULL && m->delegs != NULL && m->clients != NULL &&
           m->sessions != NULL && m->drc_slots != NULL && m->nodes != NULL &&
           m->partitions != NULL;
}

/* C7: release backend-owned state only; the dispatcher frees @cat. */
static void mem_close(struct mds_catalogue *cat)
{
    struct memdb *m;

    if (cat == NULL || cat->backend_private == NULL) {
        return;
    }
    m = cat->backend_private;
    cat->backend_private = NULL;
    memdb_free_tables(m);
    (void)pthread_mutex_destroy(&m->lock);
    free(m);
}

static enum mds_status mem_probe(struct mds_catalogue *cat)
{
    return (cat != NULL && cat->backend_private != NULL) ? MDS_OK : MDS_ERR_INVAL;
}

/* -----------------------------------------------------------------------
 * Vtables
 *
 * Optional slots deliberately left NULL (the dispatcher returns
 * MDS_ERR_NOSUPPORT or takes its documented fallback): bootstrap and
 * backend_handle (nothing to bootstrap, no native handle);
 * ns_create_with_layout and layoutget_fused (no fused DS placement);
 * ns_remove_known (the dispatcher falls back to ns_remove);
 * ns_readdir_plus (the dispatcher's ns_readdir + ns_getattr fallback
 * is exercised here; cookie resume goes through ns_readdir_plus_from);
 * prealloc_pool_* and backend_client_stats.
 * ----------------------------------------------------------------------- */

static const struct mds_catalogue_ops memdb_lifecycle_ops = {
    .close = mem_close,
    .probe = mem_probe,
    .bootstrap = NULL,
    .backend_handle = NULL,
};

static const struct mds_authority_ops memdb_auth_ops = {
    .ns_create                     = mem_ns_create,
    .ns_create_wide                = mem_ns_create_wide,
    .ns_create_with_layout         = NULL,
    .ns_remove                     = mem_ns_remove,
    .ns_remove_known               = NULL,
    .ns_remove_known_gc            = mem_ns_remove_known_gc,
    .ns_parent_touch               = mem_ns_parent_touch,
    .remove_pending_enqueue        = mem_remove_pending_enqueue,
    .remove_pending_enqueue_unlink = mem_remove_pending_enqueue_unlink,
    .remove_pending_peek_batch     = mem_remove_pending_peek_batch,
    .remove_pending_claim          = mem_remove_pending_claim,
    .remove_pending_complete       = mem_remove_pending_complete,
    .remove_pending_bump_retry     = mem_remove_pending_bump_retry,
    .remove_pending_count          = mem_remove_pending_count,
    .remove_pending_scan_all       = mem_remove_pending_scan_all,
    .ns_rename                     = mem_ns_rename,
    .ns_rename_flags               = mem_ns_rename_flags,
    .ns_link                       = mem_ns_link,
    .ns_lookup                     = mem_ns_lookup,
    .ns_getattr                    = mem_ns_getattr,
    .ns_setattr                    = mem_ns_setattr,
    .ns_readdir                    = mem_ns_readdir,
    .dirent_name_for_child         = mem_dirent_name_for_child,
    .ns_readdir_plus               = NULL,
    .ns_readdir_plus_from          = mem_ns_readdir_plus_from,
    .ns_nlink_adjust               = mem_ns_nlink_adjust,
    .alloc_fileid                  = mem_alloc_fileid,
    .inode_put                     = mem_inode_put,
    .inode_del                     = mem_inode_del,
    .dirent_put                    = mem_dirent_put,
    .dirent_insert                 = mem_dirent_insert,
    .dirent_del                    = mem_dirent_del,
    .inline_get                    = mem_inline_get,
    .inline_put                    = mem_inline_put,
    .inline_del                    = mem_inline_del,
    .xattr_get                     = mem_xattr_get,
    .xattr_put                     = mem_xattr_put,
    .xattr_del                     = mem_xattr_del,
    .xattr_list                    = mem_xattr_list,
    .xattr_exists                  = mem_xattr_exists,
    .stripe_map_get                = mem_stripe_map_get,
    .stripe_map_put                = mem_stripe_map_put,
    .stripe_map_del                = mem_stripe_map_del,
    .stripe_map_scan               = mem_stripe_map_scan,
    .ds_get                        = mem_ds_get,
    .ds_put                        = mem_ds_put,
    .ds_del                        = mem_ds_del,
    .ds_list                       = mem_ds_list,
    .ds_provision_get              = mem_ds_provision_get,
    .ds_provision_put              = mem_ds_provision_put,
    .ds_provision_del              = mem_ds_provision_del,
    .quota_rule_get                = mem_quota_rule_get,
    .quota_rule_put                = mem_quota_rule_put,
    .quota_usage_get               = mem_quota_usage_get,
    .quota_usage_put               = mem_quota_usage_put,
    .gc_enqueue                    = mem_gc_enqueue,
    .gc_peek                       = mem_gc_peek,
    .gc_dequeue                    = mem_gc_dequeue,
    .gc_count                      = mem_gc_count,
    .gc_peek_batch                 = mem_gc_peek_batch,
    .prealloc_pool_insert          = NULL,
    .prealloc_pool_delete          = NULL,
    .prealloc_pool_scan            = NULL,
    .shard_fileid_get              = mem_shard_fileid_get,
    .shard_fileid_put              = mem_shard_fileid_put,
    .shard_fileid_del              = mem_shard_fileid_del,
    .ext_dirent_get                = mem_ext_dirent_get,
    .ext_dirent_put                = mem_ext_dirent_put,
    .ext_dirent_del                = mem_ext_dirent_del,
    .link_anchor_put               = mem_link_anchor_put,
    .link_anchor_del               = mem_link_anchor_del,
    .backend_client_stats          = NULL,
};

static const struct mds_coordination_ops memdb_coord_ops = {
    .journal_put               = mem_journal_put,
    .journal_get               = mem_journal_get,
    .journal_del               = mem_journal_del,
    .journal_scan              = mem_journal_scan,
    .layout_grant              = mem_layout_grant,
    .layout_grant_union        = mem_layout_grant_union,
    .layoutget_fused           = NULL,
    .layout_return             = mem_layout_return,
    .layout_get_by_stateid     = mem_layout_get_by_stateid,
    .layout_scan_for_file      = mem_layout_scan_for_file,
    .layout_del_all_for_client = mem_layout_del_all_for_client,
    .ds_layout_idx_scan        = mem_ds_layout_idx_scan,
    .layout_iter_file          = mem_layout_iter_file,
    .recovery_put              = mem_recovery_put,
    .recovery_del              = mem_recovery_del,
    .recovery_get              = mem_recovery_get,
    .recovery_list             = mem_recovery_list,
    .open_put                  = mem_open_put,
    .open_get                  = mem_open_get,
    .open_del                  = mem_open_del,
    .open_scan_file            = mem_open_scan_file,
    .open_scan_client          = mem_open_scan_client,
    .lock_put                  = mem_lock_put,
    .lock_del                  = mem_lock_del,
    .lock_test                 = mem_lock_test,
    .lock_scan_file            = mem_lock_scan_file,
    .lock_scan_owner           = mem_lock_scan_owner,
    .lock_reap_client          = mem_lock_reap_client,
    .deleg_put                 = mem_deleg_put,
    .deleg_get                 = mem_deleg_get,
    .deleg_del                 = mem_deleg_del,
    .deleg_scan_file           = mem_deleg_scan_file,
    .deleg_scan_client         = mem_deleg_scan_client,
    .client_put                = mem_client_put,
    .client_get                = mem_client_get,
    .client_del                = mem_client_del,
    .session_put               = mem_session_put,
    .session_get               = mem_session_get,
    .session_del               = mem_session_del,
    .session_scan_client       = mem_session_scan_client,
    .slot_put                  = mem_slot_put,
    .slot_get                  = mem_slot_get,
};

static const struct mds_cluster_ops memdb_cluster_ops = {
    .node_register   = mem_node_register,
    .node_heartbeat  = mem_node_heartbeat,
    .node_deregister = mem_node_deregister,
    .node_list       = mem_node_list,
    .node_scan_stale = mem_node_scan_stale,
    .partition_list  = mem_partition_list,
    .partition_put   = mem_partition_put,
    .partition_cas   = mem_partition_cas,
};

/* -----------------------------------------------------------------------
 * Constructors
 * ----------------------------------------------------------------------- */

/* Build one instance: tables, mutex, root inode (fileid 2 like the
 * RonDB bootstrap) and the per-instance sequences. */
static enum mds_status memdb_instance_new(struct memdb **out)
{
    struct memdb *m;
    struct mds_inode root;
    struct timespec now;

    m = calloc(1, sizeof(*m));
    if (m == NULL) {
        return MDS_ERR_NOMEM;
    }
    if (!memdb_alloc_tables(m)) {
        memdb_free_tables(m);
        free(m);
        return MDS_ERR_NOMEM;
    }
    if (pthread_mutex_init(&m->lock, NULL) != 0) {
        memdb_free_tables(m);
        free(m);
        return MDS_ERR_IO;
    }

    memdb_now(&now);
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
    memdb_inode_store(&m->inodes[0], &root);

    m->next_fileid = MDS_FILEID_ROOT + 1;
    m->next_cookie = MEMDB_COOKIE_FIRST;
    m->next_gc_seq = 1;
    m->next_remove_seq = 1;
    *out = m;
    return MDS_OK;
}

enum mds_status catalogue_memdb_open_cfg(const struct mds_config *cfg,
                                         struct mds_catalogue **out)
{
    struct mds_catalogue *cat;
    struct memdb *m = NULL;
    enum mds_status st;

    if (cfg == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }
    *out = NULL;

    cat = calloc(1, sizeof(*cat));
    if (cat == NULL) {
        return MDS_ERR_NOMEM;
    }
    st = memdb_instance_new(&m);
    if (st != MDS_OK) {
        free(cat);
        return st;
    }

    /* The one configuration input: the identity stamped on rows that
     * record their owning MDS (client recovery).  An in-process store
     * shared by several MDS contexts carries the opener's id. */
    m->self_mds_id = cfg->self.id;

    cat->backend = MDS_BACKEND_MEMDB;
    /* One in-process store shared by every MDS context that opens it:
     * a cross-subtree rename moves the dirent and keeps the inode.
     * Never MDS_CAT_CAP_MULTI_PROCESS: the image is private to this
     * process, so no other daemon can observe its registry rows. */
    cat->caps = MDS_CAT_CAP_SHARED_AUTHORITY;
    cat->ops = &memdb_lifecycle_ops;
    cat->auth_ops = &memdb_auth_ops;
    cat->coord_ops = &memdb_coord_ops;
    cat->cluster_ops = &memdb_cluster_ops;
    cat->backend_private = m;
    *out = cat;
    return MDS_OK;
}

struct mds_catalogue *catalogue_memdb_open(void)
{
    struct mds_config cfg;
    struct mds_catalogue *cat = NULL;

    /* The only configuration input is cfg->self.id (the identity
     * stamped on client recovery rows); the all-zero block gives this
     * test handle identity 0, i.e. it writes unassigned rows. */
    memset(&cfg, 0, sizeof(cfg));
    cfg.catalogue_backend = MDS_BACKEND_MEMDB;
    if (catalogue_memdb_open_cfg(&cfg, &cat) != MDS_OK) {
        return NULL;
    }
    return cat;
}

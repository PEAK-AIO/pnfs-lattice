/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_memdb.c -- In-memory catalogue backend for unit tests.
 *
 * Provides a lightweight implementation of the catalogue + coordination
 * vtables backed by flat arrays and linear scans.  No external
 * dependencies (no RonDB, no backend).  Designed for correctness testing,
 * not performance.
 *
 * Usage:
 *   struct mds_catalogue *cat = catalogue_memdb_open();
 *   // ... use mds_cat_* / mds_coord_* API as normal ...
 *   mds_catalogue_close(cat);
 */

#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>

#include "pnfs_mds.h"
#include "layout_ds_ids.h"
#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "open_state.h"
#include "catalogue_internal.h"
#include "quota.h"
#include "test_helpers.h"

/* -----------------------------------------------------------------------
 * Storage limits (generous for unit tests)
 * ----------------------------------------------------------------------- */

#define MEMDB_MAX_INODES    4096
#define MEMDB_MAX_DIRENTS   4096
#define MEMDB_MAX_INLINE    256
#define MEMDB_MAX_XATTRS    1024
#define MEMDB_MAX_STRIPE    256
#define MEMDB_MAX_DS        64
#define MEMDB_MAX_GC        256
#define MEMDB_MAX_GC_TASKS  256
#define MEMDB_MAX_JOURNAL   64
#define MEMDB_MAX_LAYOUTS   256
#define MEMDB_MAX_OPENS     256
#define MEMDB_MAX_RECOVERY  64
#define MEMDB_MAX_PROVISION 64
#define MEMDB_MAX_QUOTA_USAGE 256

/* -----------------------------------------------------------------------
 * In-memory data structures
 * ----------------------------------------------------------------------- */

struct memdb_dirent {
    uint64_t parent;
    char     name[MDS_MAX_NAME + 1];
    uint64_t child_fileid;
    uint8_t  child_type;
    int      used;
};
struct memdb_open {
    struct mds_coord_open_row row;
    int                       used;
};

struct memdb_inline {
    uint64_t fileid;
    uint8_t  data[MDS_INLINE_DATA_MAX];
    uint32_t len;
    int      used;
};

struct memdb_xattr {
    uint64_t fileid;
    char     name[MDS_XATTR_NAME_MAX + 1];
    uint8_t  val[MDS_XATTR_VAL_MAX];
    uint32_t vallen;
    int      used;
};

struct memdb_stripe {
    uint64_t fileid;
    uint32_t stripe_count;
    uint32_t stripe_unit;
    uint32_t mirror_count;
    /* Heap-allocated to keep the in-process memdb small after Phase
     * A bumped MDS_MAX_STRIPES to 1024 (a fixed-size inline array
     * would be ~140 MB across MEMDB_MAX_STRIPE records).  Allocated
     * lazily in mem_stripe_map_put and freed on stripe_map_del,
     * memdb close, and on overwrite. */
    struct mds_ds_map_entry *entries;
    uint32_t entries_cap;
    int      used;
};

struct memdb_gc {
    struct mds_gc_entry entry;
    int used;
};
struct memdb_gc_task {
    struct mds_gc_task task;
    int                used;
};

struct memdb_layout {
    uint64_t clientid;
    uint64_t fileid;
    uint32_t iomode;
    uint64_t offset;
    uint64_t length;
    struct nfs4_stateid stateid;
    uint32_t *ds_ids;
    uint32_t ds_count;
    int      used;
};

struct memdb_recovery {
    uint64_t clientid;
    uint8_t  co_ownerid[1024];
    uint32_t co_ownerid_len;
    uint8_t  verifier[8];
    int      used;
};

struct memdb_provision {
    uint32_t ds_id;
    uint8_t  secret[64];
    uint32_t secret_len;
    uint64_t epoch;
    int      used;
};
struct memdb_quota_usage {
    uint8_t                usage_type;
    uint64_t               scope_id;
    struct mds_quota_usage usage;
    int                    used;
};

struct memdb {
    /* Namespace */
    struct mds_inode     inodes[MEMDB_MAX_INODES];
    int                  inode_used[MEMDB_MAX_INODES];
    uint32_t             inode_count;
    struct memdb_dirent  dirents[MEMDB_MAX_DIRENTS];
    uint64_t             next_fileid;

    /* Inline data */
    struct memdb_inline  inlines[MEMDB_MAX_INLINE];

    /* Xattrs */
    struct memdb_xattr   xattrs[MEMDB_MAX_XATTRS];

    /* Stripe maps */
    struct memdb_stripe  stripes[MEMDB_MAX_STRIPE];

    /* DS registry */
    struct mds_ds_info   ds_registry[MEMDB_MAX_DS];
    int                  ds_used[MEMDB_MAX_DS];

    /* DS provisioning */
    struct memdb_provision provisions[MEMDB_MAX_PROVISION];
    /* Quota usage */
    struct memdb_quota_usage quota_usage[MEMDB_MAX_QUOTA_USAGE];

    /* GC queue */
    struct memdb_gc      gc_queue[MEMDB_MAX_GC];
    struct memdb_gc_task gc_tasks[MEMDB_MAX_GC_TASKS];
    uint64_t             gc_seq_next;

    /* Journal */
    struct mds_coord_journal_record journals[MEMDB_MAX_JOURNAL];
    int                  journal_used[MEMDB_MAX_JOURNAL];

    /* Layout state */
    struct memdb_layout  layouts[MEMDB_MAX_LAYOUTS];
    /* Durable shared open state */
    struct memdb_open    opens[MEMDB_MAX_OPENS];

    /* Client recovery */
    struct memdb_recovery recoveries[MEMDB_MAX_RECOVERY];
    /* One-shot fault controls used only by unit tests. */
    enum mds_status next_final_unlink_status;
    struct {
        bool armed;
        uint64_t parent_fileid;
        char name[MDS_MAX_NAME + 1];
        uint64_t fileid;
        uint64_t generation;
    } unlink_after_open_put;

    pthread_mutex_t      lock;
};

/* -----------------------------------------------------------------------
 * Inode helpers
 * ----------------------------------------------------------------------- */

static int memdb_find_inode(struct memdb *m, uint64_t fileid)
{
    for (uint32_t i = 0; i < MEMDB_MAX_INODES; i++) {
        if (m->inode_used[i] && m->inodes[i].fileid == fileid) {
            return (int)i;
        }
    }
    return -1;
}

static int memdb_alloc_inode_slot(struct memdb *m)
{
    for (uint32_t i = 0; i < MEMDB_MAX_INODES; i++) {
        if (!m->inode_used[i]) {
            return (int)i;
        }
    }
    return -1;
}
static int memdb_find_quota_usage(
    const struct memdb *memdb,
    uint8_t usage_type,
    uint64_t scope_id)
{
    for (uint32_t index = 0; index < MEMDB_MAX_QUOTA_USAGE; index++) {
        if (memdb->quota_usage[index].used &&
            memdb->quota_usage[index].usage_type == usage_type &&
            memdb->quota_usage[index].scope_id == scope_id) {
            return (int)index;
        }
    }
    return -1;
}

/* -----------------------------------------------------------------------
 * Dirent helpers
 * ----------------------------------------------------------------------- */

static int memdb_find_dirent(struct memdb *m, uint64_t parent, const char *name)
{
    for (uint32_t i = 0; i < MEMDB_MAX_DIRENTS; i++) {
        if (m->dirents[i].used &&
            m->dirents[i].parent == parent &&
            strcmp(m->dirents[i].name, name) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static int memdb_alloc_dirent_slot(struct memdb *m)
{
    for (uint32_t i = 0; i < MEMDB_MAX_DIRENTS; i++) {
        if (!m->dirents[i].used) {
            return (int)i;
        }
    }
    return -1;
}

/* -----------------------------------------------------------------------
 * Authority ops implementation
 * ----------------------------------------------------------------------- */
static enum mds_status mem_ns_setattr_locked(
    struct memdb *memdb,
    uint64_t fileid,
    const struct mds_inode *attrs,
    uint32_t mask,
    struct mds_size_extend_result *size_result);

static enum mds_status mem_alloc_fileid(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t *fileid)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    pthread_mutex_lock(&m->lock);
    *fileid = m->next_fileid++;
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

static enum mds_status mem_ns_setattr(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid,
    const struct mds_inode *attrs, uint32_t mask)
{
    struct memdb *memdb;
    enum mds_status status;

    (void)txn;
    if (attrs == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb = cat->backend_private;
    pthread_mutex_lock(&memdb->lock);
    status = mem_ns_setattr_locked(memdb, fileid, attrs, mask, NULL);
    pthread_mutex_unlock(&memdb->lock);
    return status;
}

static enum mds_status mem_ns_setattr_size_extend(
    struct mds_catalogue *cat,
    struct mds_cat_txn *txn,
    uint64_t fileid,
    const struct mds_inode *attrs,
    uint32_t mask,
    struct mds_size_extend_result *result)
{
    struct memdb *memdb;
    enum mds_status status;

    (void)txn;
    if (attrs == NULL || result == NULL ||
        !(mask & MDS_ATTR_SIZE_EXTEND)) {
        return MDS_ERR_INVAL;
    }
    memdb = cat->backend_private;
    pthread_mutex_lock(&memdb->lock);
    status = mem_ns_setattr_locked(memdb, fileid, attrs, mask, result);
    pthread_mutex_unlock(&memdb->lock);
    return status;
}

static enum mds_status mem_inode_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, const struct mds_inode *inode)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    pthread_mutex_lock(&m->lock);

    int idx = memdb_find_inode(m, inode->fileid);
    if (idx < 0) {
        idx = memdb_alloc_inode_slot(m);
        if (idx < 0) {
            pthread_mutex_unlock(&m->lock);
            return MDS_ERR_NOSPC;
        }
        m->inode_used[idx] = 1;
    }
    m->inodes[idx] = *inode;
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

static enum mds_status mem_inode_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    pthread_mutex_lock(&m->lock);
    int idx = memdb_find_inode(m, fileid);
    if (idx >= 0) {
        m->inode_used[idx] = 0;
    }
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

static enum mds_status mem_ns_getattr(struct mds_catalogue *cat,
    uint64_t fileid, struct mds_inode *inode)
{
    struct memdb *m = cat->backend_private;
    pthread_mutex_lock(&m->lock);
    int idx = memdb_find_inode(m, fileid);
    if (idx < 0) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_NOTFOUND;
    }
    *inode = m->inodes[idx];
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

static enum mds_status mem_ns_setattr_locked(
    struct memdb *memdb,
    uint64_t fileid,
    const struct mds_inode *attrs,
    uint32_t mask,
    struct mds_size_extend_result *size_result)
{
    int inode_index;
    struct mds_inode *inode;

    inode_index = memdb_find_inode(memdb, fileid);
    if (inode_index < 0) {
        return MDS_ERR_NOTFOUND;
    }
    inode = &memdb->inodes[inode_index];

    if (size_result != NULL) {
        size_result->locked_old_size = inode->size;
    }
    if (mask & MDS_ATTR_MODE)  inode->mode = attrs->mode;
    if (mask & MDS_ATTR_UID)   inode->uid = attrs->uid;
    if (mask & MDS_ATTR_GID)   inode->gid = attrs->gid;
    if (mask & MDS_ATTR_SIZE)  inode->size = attrs->size;
    if (mask & MDS_ATTR_SIZE_EXTEND && attrs->size > inode->size) {
        inode->size = attrs->size;
    }
    if (mask & MDS_ATTR_FLAGS) inode->flags = attrs->flags;
    inode->change++;
    clock_gettime(CLOCK_REALTIME, &inode->ctime);
    if (size_result != NULL) {
        size_result->committed_size = inode->size;
    }
    return MDS_OK;
}

static enum mds_status mem_dirent_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent,
    const char *name, uint64_t child_fileid, uint8_t child_type)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    pthread_mutex_lock(&m->lock);

    int idx = memdb_find_dirent(m, parent, name);
    if (idx < 0) {
        idx = memdb_alloc_dirent_slot(m);
        if (idx < 0) {
            pthread_mutex_unlock(&m->lock);
            return MDS_ERR_NOSPC;
        }
    }
    m->dirents[idx].used = 1;
    m->dirents[idx].parent = parent;
    snprintf(m->dirents[idx].name, sizeof(m->dirents[idx].name), "%s", name);
    m->dirents[idx].child_fileid = child_fileid;
    m->dirents[idx].child_type = child_type;
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

static enum mds_status mem_dirent_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent, const char *name)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    pthread_mutex_lock(&m->lock);
    int idx = memdb_find_dirent(m, parent, name);
    if (idx < 0) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_NOTFOUND;
    }
    m->dirents[idx].used = 0;
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

static enum mds_status mem_ns_create(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent,
    const char *name, enum mds_file_type type,
    uint32_t mode, uint64_t uid, uint64_t gid,
    struct ds_prealloc_ctx *prealloc, struct mds_inode *out)
{
    (void)prealloc;
    struct memdb *m = cat->backend_private;

    /* Check parent exists. */
    if (memdb_find_inode(m, parent) < 0) {
        return MDS_ERR_NOTFOUND;
    }
    /* Check name doesn't already exist. */
    if (memdb_find_dirent(m, parent, name) >= 0) {
        return MDS_ERR_EXISTS;
    }

    uint64_t fid = 0;
    enum mds_status st = mem_alloc_fileid(cat, txn, &fid);
    if (st != MDS_OK) return st;

    struct mds_inode child;
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    memset(&child, 0, sizeof(child));
    child.fileid = fid;
    child.type = type;
    child.mode = mode;
    child.uid = uid;
    child.gid = gid;
    child.nlink = (type == MDS_FTYPE_DIR) ? 2 : 1;
    child.atime = now;
    child.mtime = now;
    child.ctime = now;
    child.change = 1;
    child.generation = 1;
    child.parent_fileid = parent;
    if (type == MDS_FTYPE_REG) {
        child.flags = MDS_IFLAG_INLINE;
    }

    st = mem_inode_put(cat, txn, &child);
    if (st != MDS_OK) return st;

    st = mem_dirent_put(cat, txn, parent, name, fid, (uint8_t)type);
    if (st != MDS_OK) return st;

    /* Bump parent nlink for directories. */
    if (type == MDS_FTYPE_DIR) {
        pthread_mutex_lock(&m->lock);
        int pidx = memdb_find_inode(m, parent);
        if (pidx >= 0) m->inodes[pidx].nlink++;
        pthread_mutex_unlock(&m->lock);
    }

    /* Touch parent mtime/change. */
    pthread_mutex_lock(&m->lock);
    int pidx = memdb_find_inode(m, parent);
    if (pidx >= 0) {
        m->inodes[pidx].mtime = now;
        m->inodes[pidx].ctime = now;
        m->inodes[pidx].change++;
    }
    pthread_mutex_unlock(&m->lock);

    *out = child;
    return MDS_OK;
}
static enum mds_status mem_ns_create_wide(
    struct mds_catalogue *cat,
    uint64_t parent_fileid,
    const char *name,
    const struct mds_inode *child,
    uint32_t stripe_count,
    uint32_t stripe_unit,
    uint32_t mirror_count,
    const struct mds_ds_map_entry *entries)
{
    struct memdb *memdb;
    struct mds_ds_map_entry *copied_entries;
    struct timespec now;
    uint64_t entry_count;
    int child_index;
    int dirent_index;
    int parent_index;
    int stripe_index;

    if (cat == NULL || name == NULL || child == NULL || entries == NULL ||
        child->fileid == 0 || child->parent_fileid != parent_fileid ||
        child->type != MDS_FTYPE_REG || stripe_count == 0 ||
        stripe_count > MDS_MAX_STRIPES || stripe_unit == 0 ||
        mirror_count == 0 || mirror_count > MDS_MAX_MIRRORS ||
        (child->flags & MDS_IFLAG_HPC_CREATE_PENDING) != 0) {
        return MDS_ERR_INVAL;
    }

    entry_count = (uint64_t)stripe_count * mirror_count;
    if (entry_count > UINT32_MAX) {
        return MDS_ERR_INVAL;
    }

    copied_entries = calloc((size_t)entry_count, sizeof(*copied_entries));
    if (copied_entries == NULL) {
        return MDS_ERR_NOMEM;
    }
    memcpy(copied_entries, entries,
           (size_t)entry_count * sizeof(*copied_entries));

    if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
        free(copied_entries);
        return MDS_ERR_IO;
    }

    memdb = cat->backend_private;
    pthread_mutex_lock(&memdb->lock);
    parent_index = memdb_find_inode(memdb, parent_fileid);
    if (parent_index < 0) {
        pthread_mutex_unlock(&memdb->lock);
        free(copied_entries);
        return MDS_ERR_NOTFOUND;
    }
    if (memdb->inodes[parent_index].type != MDS_FTYPE_DIR) {
        pthread_mutex_unlock(&memdb->lock);
        free(copied_entries);
        return MDS_ERR_NOTDIR;
    }
    if (memdb_find_dirent(memdb, parent_fileid, name) >= 0) {
        pthread_mutex_unlock(&memdb->lock);
        free(copied_entries);
        return MDS_ERR_EXISTS;
    }
    if (memdb_find_inode(memdb, child->fileid) >= 0) {
        pthread_mutex_unlock(&memdb->lock);
        free(copied_entries);
        return MDS_ERR_EXISTS;
    }

    child_index = memdb_alloc_inode_slot(memdb);
    dirent_index = memdb_alloc_dirent_slot(memdb);
    stripe_index = -1;
    for (uint32_t stripe_slot = 0; stripe_slot < MEMDB_MAX_STRIPE;
         stripe_slot++) {
        if (!memdb->stripes[stripe_slot].used) {
            stripe_index = (int)stripe_slot;
            break;
        }
    }
    if (child_index < 0 || dirent_index < 0 || stripe_index < 0) {
        pthread_mutex_unlock(&memdb->lock);
        free(copied_entries);
        return MDS_ERR_NOSPC;
    }

    memdb->inodes[child_index] = *child;
    memdb->inode_used[child_index] = true;
    memdb->dirents[dirent_index].used = true;
    memdb->dirents[dirent_index].parent = parent_fileid;
    (void)snprintf(memdb->dirents[dirent_index].name,
                   sizeof(memdb->dirents[dirent_index].name), "%s", name);
    memdb->dirents[dirent_index].child_fileid = child->fileid;
    memdb->dirents[dirent_index].child_type = (uint8_t)child->type;
    memdb->stripes[stripe_index].used = true;
    memdb->stripes[stripe_index].fileid = child->fileid;
    memdb->stripes[stripe_index].stripe_count = stripe_count;
    memdb->stripes[stripe_index].stripe_unit = stripe_unit;
    memdb->stripes[stripe_index].mirror_count = mirror_count;
    memdb->stripes[stripe_index].entries = copied_entries;
    memdb->stripes[stripe_index].entries_cap = (uint32_t)entry_count;
    memdb->inodes[parent_index].mtime = now;
    memdb->inodes[parent_index].ctime = now;
    memdb->inodes[parent_index].change++;
    pthread_mutex_unlock(&memdb->lock);
    return MDS_OK;
}

static enum mds_status mem_ns_remove(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent, const char *name)
{
    struct memdb *m = cat->backend_private;
    pthread_mutex_lock(&m->lock);

    int didx = memdb_find_dirent(m, parent, name);
    if (didx < 0) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_NOTFOUND;
    }
    uint64_t child_fid = m->dirents[didx].child_fileid;
    m->dirents[didx].used = 0;

    int cidx = memdb_find_inode(m, child_fid);
    bool nlink_zero = false;
    uint8_t child_type = 0;
    if (cidx >= 0) {
        child_type = (uint8_t)m->inodes[cidx].type;
        m->inodes[cidx].nlink--;
        if (m->inodes[cidx].nlink == 0) {
            nlink_zero = true;
            m->inode_used[cidx] = 0;
        }
    }
    pthread_mutex_unlock(&m->lock);

    /* GC: enqueue stripe map entries for deleted regular files. */
    if (nlink_zero && child_type != MDS_FTYPE_DIR) {
        struct mds_ds_map_entry *sme = NULL;
        uint32_t smc = 0, smmc = 0;
        if (mds_cat_stripe_map_get(cat, child_fid, &smc, NULL, &smmc,
                                   &sme) == MDS_OK && sme != NULL) {
            uint32_t total = smc * (smmc ? smmc : 1);
            for (uint32_t gi = 0; gi < total; gi++) {
                mds_cat_gc_enqueue(cat, txn, child_fid, sme[gi].ds_id,
                                   sme[gi].nfs_fh, sme[gi].nfs_fh_len);
            }
            free(sme);
            mds_cat_stripe_map_del(cat, txn, child_fid);
        }
        /* Drop inline data and xattrs for the removed inode. */
        pthread_mutex_lock(&m->lock);
        for (uint32_t i = 0; i < MEMDB_MAX_INLINE; i++) {
            if (m->inlines[i].used &&
                m->inlines[i].fileid == child_fid) {
                m->inlines[i].used = 0;
            }
        }
        for (uint32_t i = 0; i < MEMDB_MAX_XATTRS; i++) {
            if (m->xattrs[i].used &&
                m->xattrs[i].fileid == child_fid) {
                m->xattrs[i].used = 0;
            }
        }
        pthread_mutex_unlock(&m->lock);
    }
    return MDS_OK;
}

static enum mds_status mem_ns_rename(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t src_parent,
    const char *src_name, uint64_t dst_parent, const char *dst_name)
{
    struct memdb *m = cat->backend_private;
    pthread_mutex_lock(&m->lock);

    int sidx = memdb_find_dirent(m, src_parent, src_name);
    if (sidx < 0) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_NOTFOUND;
    }
    uint64_t fid = m->dirents[sidx].child_fileid;
    uint8_t type = m->dirents[sidx].child_type;

    /* Remove destination if it exists. */
    int didx = memdb_find_dirent(m, dst_parent, dst_name);
    if (didx >= 0) {
        m->dirents[didx].used = 0;
    }

    /* Remove source dirent. */
    m->dirents[sidx].used = 0;
    pthread_mutex_unlock(&m->lock);

    /* Create destination dirent. */
    return mem_dirent_put(cat, txn, dst_parent, dst_name, fid, type);
}

static enum mds_status mem_ns_link(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent,
    const char *name, uint64_t target)
{
    struct memdb *m = cat->backend_private;
    pthread_mutex_lock(&m->lock);

    int tidx = memdb_find_inode(m, target);
    if (tidx < 0) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_NOTFOUND;
    }
    if (m->inodes[tidx].type == MDS_FTYPE_DIR) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_ISDIR;
    }
    m->inodes[tidx].nlink++;
    uint8_t type = (uint8_t)m->inodes[tidx].type;
    pthread_mutex_unlock(&m->lock);

    return mem_dirent_put(cat, txn, parent, name, target, type);
}

static enum mds_status mem_ns_lookup(struct mds_catalogue *cat,
    uint64_t parent, const char *name, struct mds_inode *child)
{
    struct memdb *m = cat->backend_private;
    pthread_mutex_lock(&m->lock);

    int didx = memdb_find_dirent(m, parent, name);
    if (didx < 0) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_NOTFOUND;
    }
    uint64_t fid = m->dirents[didx].child_fileid;
    int iidx = memdb_find_inode(m, fid);
    if (iidx < 0) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_NOTFOUND;
    }
    *child = m->inodes[iidx];
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

static enum mds_status mem_dirent_name_for_child(
    struct mds_catalogue *cat, uint64_t parent, uint64_t child_fileid,
    char *name_out, size_t name_out_len)
{
    struct memdb *m = cat->backend_private;

    if (name_out == NULL || name_out_len == 0) {
        return MDS_ERR_INVAL;
    }

    pthread_mutex_lock(&m->lock);
    for (uint32_t i = 0; i < MEMDB_MAX_DIRENTS; i++) {
        if (!m->dirents[i].used || m->dirents[i].parent != parent ||
            m->dirents[i].child_fileid != child_fileid) {
            continue;
        }
        (void)snprintf(name_out, name_out_len, "%s", m->dirents[i].name);
        pthread_mutex_unlock(&m->lock);
        return MDS_OK;
    }
    pthread_mutex_unlock(&m->lock);
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_ns_readdir(struct mds_catalogue *cat,
    uint64_t parent, const char *start_after,
    uint32_t max_entries,
    struct mds_cat_txn *txn, mds_readdir_cb cb, void *ctx)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    uint32_t emitted = 0;

    pthread_mutex_lock(&m->lock);

    for (uint32_t i = 0; i < MEMDB_MAX_DIRENTS; i++) {
        if (!m->dirents[i].used || m->dirents[i].parent != parent)
            continue;
        if (start_after && strcmp(m->dirents[i].name, start_after) <= 0)
            continue;
        struct mds_cat_dirent d;
        d.fileid = m->dirents[i].child_fileid;
        d.type = m->dirents[i].child_type;
        snprintf(d.name, sizeof(d.name), "%s", m->dirents[i].name);
        pthread_mutex_unlock(&m->lock);
        if (cb(&d, ctx) != 0) return MDS_OK;
        emitted++;
        if (max_entries > 0 && emitted >= max_entries) {
            return MDS_OK;
        }
        pthread_mutex_lock(&m->lock);
    }
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

/* Fused readdir_plus resumed by a child-fileid cursor: returns entries
 * whose child_fileid is strictly greater than start_after_fileid, in
 * ascending fileid order, up to max_entries.  Mirrors the RonDB
 * ordered-index cursor semantics so the cookie-resume path (ordering,
 * deleted-cookie safety, drain/eof) is exercised by the unit tests. */
struct mem_rdp_row {
    uint64_t fid;
    uint8_t  type;
    char     name[MDS_MAX_NAME + 1];
};

static int mem_rdp_row_cmp(const void *a, const void *b)
{
    const struct mem_rdp_row *ra = a;
    const struct mem_rdp_row *rb = b;

    if (ra->fid < rb->fid) { return -1; }
    if (ra->fid > rb->fid) { return 1; }
    return 0;
}

static enum mds_status mem_ns_readdir_plus_from(struct mds_catalogue *cat,
    uint64_t parent, uint64_t start_after_fileid,
    uint32_t max_entries,
    struct mds_cat_txn *txn, mds_readdir_plus_cb cb, void *ctx)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    struct mem_rdp_row *rows;
    uint32_t n = 0;

    if (cb == NULL) {
        return MDS_ERR_INVAL;
    }

    rows = calloc(MEMDB_MAX_DIRENTS, sizeof(*rows));
    if (rows == NULL) {
        return MDS_ERR_NOMEM;
    }

    /* Snapshot the matching children under the lock, then sort and
     * deliver without holding it (mem_ns_getattr re-locks). */
    pthread_mutex_lock(&m->lock);
    for (uint32_t i = 0; i < MEMDB_MAX_DIRENTS; i++) {
        if (!m->dirents[i].used || m->dirents[i].parent != parent) {
            continue;
        }
        if (m->dirents[i].child_fileid <= start_after_fileid) {
            continue;
        }
        rows[n].fid = m->dirents[i].child_fileid;
        rows[n].type = m->dirents[i].child_type;
        snprintf(rows[n].name, sizeof(rows[n].name), "%s",
                 m->dirents[i].name);
        n++;
    }
    pthread_mutex_unlock(&m->lock);

    if (n > 1) {
        qsort(rows, n, sizeof(*rows), mem_rdp_row_cmp);
    }

    for (uint32_t i = 0; i < n; i++) {
        struct mds_cat_dirent d;
        struct mds_inode inode;
        bool valid;

        if (max_entries > 0 && i >= max_entries) {
            break;
        }
        memset(&d, 0, sizeof(d));
        d.fileid = rows[i].fid;
        d.type = rows[i].type;
        snprintf(d.name, sizeof(d.name), "%s", rows[i].name);

        valid = (mem_ns_getattr(cat, rows[i].fid, &inode) == MDS_OK);
        if (cb(&d, valid ? &inode : NULL, valid, ctx) != 0) {
            break;
        }
    }

    free(rows);
    return MDS_OK;
}

static enum mds_status mem_ns_nlink_adjust(struct mds_catalogue *cat,
    uint64_t fileid, int32_t delta)
{
    struct memdb *m = cat->backend_private;
    pthread_mutex_lock(&m->lock);
    int idx = memdb_find_inode(m, fileid);
    if (idx < 0) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_NOTFOUND;
    }
    m->inodes[idx].nlink = (uint32_t)((int32_t)m->inodes[idx].nlink + delta);
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

/* Inline data */
static enum mds_status mem_inline_get(struct mds_catalogue *cat,
    uint64_t fileid, void *buf, uint32_t buflen, uint32_t *outlen)
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_INLINE; i++) {
        if (m->inlines[i].used && m->inlines[i].fileid == fileid) {
            uint32_t copy = m->inlines[i].len;
            if (copy > buflen) copy = buflen;
            memcpy(buf, m->inlines[i].data, copy);
            *outlen = copy;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_inline_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid,
    const void *buf, uint32_t len)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    if (len > MDS_INLINE_DATA_MAX) {
        return MDS_ERR_INVAL;
    }
    if (len > 0 && buf == NULL) {
        return MDS_ERR_INVAL;
    }
    for (uint32_t i = 0; i < MEMDB_MAX_INLINE; i++) {
        if (m->inlines[i].used && m->inlines[i].fileid == fileid) {
            if (len > 0) {
                memcpy(m->inlines[i].data, buf, len);
            }
            m->inlines[i].len = len;
            return MDS_OK;
        }
    }
    for (uint32_t i = 0; i < MEMDB_MAX_INLINE; i++) {
        if (!m->inlines[i].used) {
            m->inlines[i].used = 1;
            m->inlines[i].fileid = fileid;
            if (len > 0) {
                memcpy(m->inlines[i].data, buf, len);
            }
            m->inlines[i].len = len;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOSPC;
}

static enum mds_status mem_inline_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_INLINE; i++) {
        if (m->inlines[i].used && m->inlines[i].fileid == fileid) {
            m->inlines[i].used = 0;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

/* Xattrs */
static enum mds_status mem_xattr_get(struct mds_catalogue *cat,
    uint64_t fileid, const char *name, void **val, uint32_t *vallen)
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_XATTRS; i++) {
        if (m->xattrs[i].used && m->xattrs[i].fileid == fileid &&
            strcmp(m->xattrs[i].name, name) == 0) {
            *val = malloc(m->xattrs[i].vallen);
            if (*val == NULL) return MDS_ERR_NOMEM;
            memcpy(*val, m->xattrs[i].val, m->xattrs[i].vallen);
            *vallen = m->xattrs[i].vallen;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_xattr_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid,
    const char *name, const void *val, uint32_t vallen)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    /* Update existing. */
    for (uint32_t i = 0; i < MEMDB_MAX_XATTRS; i++) {
        if (m->xattrs[i].used && m->xattrs[i].fileid == fileid &&
            strcmp(m->xattrs[i].name, name) == 0) {
            memcpy(m->xattrs[i].val, val, vallen);
            m->xattrs[i].vallen = vallen;
            return MDS_OK;
        }
    }
    /* Insert new. */
    for (uint32_t i = 0; i < MEMDB_MAX_XATTRS; i++) {
        if (!m->xattrs[i].used) {
            m->xattrs[i].used = 1;
            m->xattrs[i].fileid = fileid;
            snprintf(m->xattrs[i].name, sizeof(m->xattrs[i].name), "%s", name);
            memcpy(m->xattrs[i].val, val, vallen);
            m->xattrs[i].vallen = vallen;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOSPC;
}

static enum mds_status mem_xattr_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid, const char *name)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_XATTRS; i++) {
        if (m->xattrs[i].used && m->xattrs[i].fileid == fileid &&
            strcmp(m->xattrs[i].name, name) == 0) {
            m->xattrs[i].used = 0;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_xattr_list(struct mds_catalogue *cat,
    uint64_t fileid, mds_xattr_list_cb cb, void *ctx)
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_XATTRS; i++) {
        if (m->xattrs[i].used && m->xattrs[i].fileid == fileid) {
            if (cb(m->xattrs[i].name, strlen(m->xattrs[i].name), ctx) != 0)
                break;
        }
    }
    return MDS_OK;
}

static enum mds_status mem_xattr_exists(struct mds_catalogue *cat,
    uint64_t fileid, const char *name)
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_XATTRS; i++) {
        if (m->xattrs[i].used && m->xattrs[i].fileid == fileid &&
            strcmp(m->xattrs[i].name, name) == 0)
            return MDS_OK;
    }
    return MDS_ERR_NOTFOUND;
}

/* Stripe maps */
static enum mds_status mem_stripe_map_get(struct mds_catalogue *cat,
    uint64_t fileid, uint32_t *sc, uint32_t *su, uint32_t *mc,
    struct mds_ds_map_entry **entries)
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_STRIPE; i++) {
        if (m->stripes[i].used && m->stripes[i].fileid == fileid) {
            if (sc) *sc = m->stripes[i].stripe_count;
            if (su) *su = m->stripes[i].stripe_unit;
            if (mc) *mc = m->stripes[i].mirror_count;
            uint32_t total = m->stripes[i].stripe_count *
                             m->stripes[i].mirror_count;
            if (entries) {
                if (total == 0 || m->stripes[i].entries == NULL) {
                    *entries = NULL;
                } else {
                    *entries = malloc(total * sizeof(struct mds_ds_map_entry));
                    if (*entries == NULL) return MDS_ERR_NOMEM;
                    memcpy(*entries, m->stripes[i].entries,
                           total * sizeof(struct mds_ds_map_entry));
                }
            }
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_stripe_map_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid,
    uint32_t sc, uint32_t su, uint32_t mc,
    const struct mds_ds_map_entry *entries)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    uint32_t total;
    /* Find existing or allocate. */
    int slot = -1;
    for (uint32_t i = 0; i < MEMDB_MAX_STRIPE; i++) {
        if (m->stripes[i].used && m->stripes[i].fileid == fileid) {
            slot = (int)i; break;
        }
    }
    if (slot < 0) {
        for (uint32_t i = 0; i < MEMDB_MAX_STRIPE; i++) {
            if (!m->stripes[i].used) { slot = (int)i; break; }
        }
    }
    if (slot < 0) return MDS_ERR_NOSPC;

    total = sc * mc;
    /* Grow the heap-backed entries buffer if needed (Phase A heap
     * conversion: the old fixed-size inline array would have been
     * 140 MB at MDS_MAX_STRIPES=1024).  Reuse the existing buffer
     * when capacity already covers the new payload. */
    if (total > m->stripes[slot].entries_cap) {
        struct mds_ds_map_entry *new_buf = NULL;
        if (total > 0) {
            new_buf = realloc(m->stripes[slot].entries,
                              total * sizeof(struct mds_ds_map_entry));
            if (new_buf == NULL) {
                return MDS_ERR_NOMEM;
            }
        } else {
            free(m->stripes[slot].entries);
        }
        m->stripes[slot].entries = new_buf;
        m->stripes[slot].entries_cap = total;
    }

    m->stripes[slot].used = 1;
    m->stripes[slot].fileid = fileid;
    m->stripes[slot].stripe_count = sc;
    m->stripes[slot].stripe_unit = su;
    m->stripes[slot].mirror_count = mc;
    if (total > 0 && entries != NULL && m->stripes[slot].entries != NULL) {
        memcpy(m->stripes[slot].entries, entries,
               total * sizeof(struct mds_ds_map_entry));
    }
    return MDS_OK;
}

static enum mds_status mem_stripe_map_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_STRIPE; i++) {
        if (m->stripes[i].used && m->stripes[i].fileid == fileid) {
            free(m->stripes[i].entries);
            m->stripes[i].entries = NULL;
            m->stripes[i].entries_cap = 0;
            m->stripes[i].used = 0;
            return MDS_OK;
        }
    }
    return MDS_OK;
}

static enum mds_status mem_stripe_map_scan(struct mds_catalogue *cat,
    mds_cat_stripe_map_scan_cb cb, void *ctx)
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_STRIPE; i++) {
        if (m->stripes[i].used) {
            if (cb(m->stripes[i].fileid, m->stripes[i].stripe_count,
                   m->stripes[i].stripe_unit, m->stripes[i].mirror_count,
                   m->stripes[i].entries, ctx) != 0)
                break;
        }
    }
    return MDS_OK;
}

/* DS registry */
static enum mds_status mem_ds_get(struct mds_catalogue *cat,
    uint32_t ds_id, struct mds_ds_info *info)
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_DS; i++) {
        if (m->ds_used[i] && m->ds_registry[i].ds_id == ds_id) {
            *info = m->ds_registry[i];
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_ds_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, const struct mds_ds_info *info)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_DS; i++) {
        if (m->ds_used[i] && m->ds_registry[i].ds_id == info->ds_id) {
            m->ds_registry[i] = *info;
            return MDS_OK;
        }
    }
    for (uint32_t i = 0; i < MEMDB_MAX_DS; i++) {
        if (!m->ds_used[i]) {
            m->ds_used[i] = 1;
            m->ds_registry[i] = *info;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOSPC;
}

static enum mds_status mem_ds_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint32_t ds_id)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_DS; i++) {
        if (m->ds_used[i] && m->ds_registry[i].ds_id == ds_id) {
            m->ds_used[i] = 0;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_ds_list(struct mds_catalogue *cat,
    struct mds_ds_info **list, uint32_t *count)
{
    struct memdb *m = cat->backend_private;
    uint32_t n = 0;
    for (uint32_t i = 0; i < MEMDB_MAX_DS; i++) {
        if (m->ds_used[i]) n++;
    }
    *list = calloc(n ? n : 1, sizeof(struct mds_ds_info));
    if (*list == NULL) return MDS_ERR_NOMEM;
    uint32_t j = 0;
    for (uint32_t i = 0; i < MEMDB_MAX_DS; i++) {
        if (m->ds_used[i]) (*list)[j++] = m->ds_registry[i];
    }
    *count = n;
    return MDS_OK;
}

/* Durable GC tasks */
static uint64_t mem_gc_task_now_ns(void)
{
    struct timespec now;

    if (clock_gettime(CLOCK_REALTIME, &now) != 0) {
        return 0;
    }
    return (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
}

static int mem_gc_task_find(struct memdb *m, uint8_t task_kind,
                            uint64_t task_id)
{
    for (uint32_t i = 0; i < MEMDB_MAX_GC_TASKS; i++) {
        if (m->gc_tasks[i].used &&
            m->gc_tasks[i].task.task_kind == task_kind &&
            m->gc_tasks[i].task.task_id == task_id) {
            return (int)i;
        }
    }
    return -1;
}
static enum mds_status mem_ns_remove_final_file(
    struct mds_catalogue *cat,
    uint64_t parent_fileid,
    const char *name,
    uint64_t expected_fileid,
    uint64_t expected_generation,
    struct mds_final_unlink_result *result)
{
    struct memdb *memdb = cat->backend_private;
    struct timespec now;
    uint64_t now_ns;
    int dirent_index;
    int child_index;
    int parent_index;
    int free_task_index = -1;
    enum mds_status injected_status;

    if (name == NULL || expected_fileid == 0 || expected_generation == 0 ||
        result == NULL || clock_gettime(CLOCK_REALTIME, &now) != 0) {
        return MDS_ERR_INVAL;
    }
    now_ns = (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;

    pthread_mutex_lock(&memdb->lock);
    injected_status = memdb->next_final_unlink_status;
    memdb->next_final_unlink_status = MDS_OK;
    if (injected_status != MDS_OK) {
        pthread_mutex_unlock(&memdb->lock);
        return injected_status;
    }
    dirent_index = memdb_find_dirent(memdb, parent_fileid, name);
    if (dirent_index < 0) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_NOTFOUND;
    }
    if (memdb->dirents[dirent_index].child_fileid != expected_fileid ||
        memdb->dirents[dirent_index].child_type != MDS_FTYPE_REG) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_STALE;
    }
    child_index = memdb_find_inode(memdb, expected_fileid);
    if (child_index < 0) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_NOTFOUND;
    }
    if (memdb->inodes[child_index].type != MDS_FTYPE_REG ||
        memdb->inodes[child_index].generation != expected_generation ||
        memdb->inodes[child_index].nlink != 1 ||
        (memdb->inodes[child_index].flags & MDS_IFLAG_DELETE_PENDING) != 0 ||
        memdb->inodes[child_index].change == UINT64_MAX) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_STALE;
    }
    parent_index = memdb_find_inode(memdb, parent_fileid);
    if (parent_index < 0) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_NOTFOUND;
    }
    if (memdb->inodes[parent_index].change == UINT64_MAX) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_IO;
    }
    if (mem_gc_task_find(memdb, MDS_GC_TASK_FILE_UNLINK,
                         expected_fileid) >= 0) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_STALE;
    }
    for (uint32_t task_slot = 0; task_slot < MEMDB_MAX_GC_TASKS;
         task_slot++) {
        if (!memdb->gc_tasks[task_slot].used) {
            free_task_index = (int)task_slot;
            break;
        }
    }
    if (free_task_index < 0) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_NOSPC;
    }

    result->parent_change_before = memdb->inodes[parent_index].change;
    result->parent_change_after = result->parent_change_before + 1U;
    memdb->dirents[dirent_index].used = 0;
    memdb->inodes[child_index].nlink = 0;
    memdb->inodes[child_index].flags |= MDS_IFLAG_DELETE_PENDING;
    memdb->inodes[child_index].ctime = now;
    memdb->inodes[child_index].change++;
    memdb->inodes[parent_index].mtime = now;
    memdb->inodes[parent_index].ctime = now;
    memdb->inodes[parent_index].change = result->parent_change_after;
    memset(&memdb->gc_tasks[free_task_index].task, 0,
           sizeof(memdb->gc_tasks[free_task_index].task));
    memdb->gc_tasks[free_task_index].task.task_kind =
        MDS_GC_TASK_FILE_UNLINK;
    memdb->gc_tasks[free_task_index].task.state = MDS_GC_TASK_PENDING;
    memdb->gc_tasks[free_task_index].task.task_id = expected_fileid;
    memdb->gc_tasks[free_task_index].task.fileid = expected_fileid;
    memdb->gc_tasks[free_task_index].task.inode_generation =
        expected_generation;
    memdb->gc_tasks[free_task_index].task.created_ns = now_ns;
    memdb->gc_tasks[free_task_index].task.not_before_ns = now_ns;
    memdb->gc_tasks[free_task_index].used = 1;
    pthread_mutex_unlock(&memdb->lock);
    return MDS_OK;
}

static bool mem_gc_task_before(const struct mds_gc_task *left,
                               const struct mds_gc_task *right)
{
    if (left->task_kind != right->task_kind) {
        return left->task_kind < right->task_kind;
    }
    if (left->created_ns != right->created_ns) {
        return left->created_ns < right->created_ns;
    }
    return left->task_id < right->task_id;
}

static bool mem_gc_task_valid(const struct mds_gc_task *task)
{
    if (task->task_kind != MDS_GC_TASK_FILE_UNLINK &&
        task->task_kind != MDS_GC_TASK_LEGACY_DS_UNLINK) {
        return false;
    }
    if (task->task_id == 0 || task->fileid == 0) {
        return false;
    }
    if (task->state != MDS_GC_TASK_PENDING &&
        task->state != MDS_GC_TASK_CLAIMED &&
        task->state != MDS_GC_TASK_QUARANTINED) {
        return false;
    }
    if (task->task_kind == MDS_GC_TASK_FILE_UNLINK) {
        return task->task_id == task->fileid &&
               task->inode_generation != 0 &&
               task->ds_id == 0 && task->nfs_fh_len == 0;
    }
    return task->inode_generation == 0 &&
           task->nfs_fh_len > 0 &&
           task->nfs_fh_len <= MDS_NFS_FH_MAX;
}

static enum mds_status mem_gc_task_enqueue(
    struct mds_catalogue *cat, struct mds_cat_txn *txn,
    const struct mds_gc_task *task)
{
    struct memdb *m = cat->backend_private;
    uint64_t now_ns;

    (void)txn;
    if (task == NULL || task->task_kind == 0 || task->task_id == 0 ||
        task->fileid == 0 || task->nfs_fh_len > MDS_NFS_FH_MAX) {
        return MDS_ERR_INVAL;
    }
    pthread_mutex_lock(&m->lock);
    if (mem_gc_task_find(m, task->task_kind, task->task_id) >= 0) {
        pthread_mutex_unlock(&m->lock);
        return MDS_OK;
    }
    for (uint32_t i = 0; i < MEMDB_MAX_GC_TASKS; i++) {
        if (!m->gc_tasks[i].used) {
            now_ns = mem_gc_task_now_ns();
            m->gc_tasks[i].task = *task;
            m->gc_tasks[i].task.state = MDS_GC_TASK_PENDING;
            m->gc_tasks[i].task.created_ns =
                task->created_ns == 0 ? now_ns : task->created_ns;
            m->gc_tasks[i].task.not_before_ns =
                task->not_before_ns == 0 ? now_ns : task->not_before_ns;
            m->gc_tasks[i].task.lease_owner_mds_id = 0;
            m->gc_tasks[i].task.lease_owner_boot_epoch = 0;
            m->gc_tasks[i].task.lease_expiry_ns = 0;
            m->gc_tasks[i].used = 1;
            pthread_mutex_unlock(&m->lock);
            return MDS_OK;
        }
    }
    pthread_mutex_unlock(&m->lock);
    return MDS_ERR_NOSPC;
}

static enum mds_status mem_gc_task_stats(
    struct mds_catalogue *cat, struct mds_gc_task_stats *stats)
{
    struct memdb *memdb = cat->backend_private;

    if (stats == NULL) {
        return MDS_ERR_INVAL;
    }
    memset(stats, 0, sizeof(*stats));
    pthread_mutex_lock(&memdb->lock);
    for (uint32_t index = 0; index < MEMDB_MAX_GC_TASKS; index++) {
        const struct mds_gc_task *task = &memdb->gc_tasks[index].task;

        if (!memdb->gc_tasks[index].used ||
            task->task_kind != MDS_GC_TASK_FILE_UNLINK) {
            continue;
        }
        if (task->state == MDS_GC_TASK_QUARANTINED) {
            stats->quarantined_file_tasks++;
            continue;
        }
        stats->active_file_tasks++;
        if (task->state == MDS_GC_TASK_CLAIMED) {
            stats->claimed_file_tasks++;
        }
        if (stats->oldest_file_task_created_ns == 0 ||
            task->created_ns < stats->oldest_file_task_created_ns) {
            stats->oldest_file_task_created_ns = task->created_ns;
        }
    }
    pthread_mutex_unlock(&memdb->lock);
    return MDS_OK;
}

static enum mds_status mem_gc_task_claim_batch(
    struct mds_catalogue *cat, struct mds_gc_task *tasks, uint32_t cap,
    uint32_t *n_out, uint32_t owner_mds_id, uint64_t owner_boot_epoch,
    uint32_t lease_ms, uint32_t stale_owner_ms)
{
    struct memdb *m = cat->backend_private;
    uint64_t now_ns;
    uint64_t lease_ns;

    (void)stale_owner_ms;
    if (tasks == NULL || n_out == NULL || cap == 0 || lease_ms == 0) {
        return MDS_ERR_INVAL;
    }
    *n_out = 0;
    now_ns = mem_gc_task_now_ns();
    lease_ns = (uint64_t)lease_ms * 1000000ULL;
    pthread_mutex_lock(&m->lock);
    while (*n_out < cap) {
        int best = -1;
        for (uint32_t i = 0; i < MEMDB_MAX_GC_TASKS; i++) {
            struct mds_gc_task *task = &m->gc_tasks[i].task;
            bool eligible;

            if (!m->gc_tasks[i].used ||
                task->state == MDS_GC_TASK_QUARANTINED) {
                continue;
            }
            if (!mem_gc_task_valid(task)) {
                task->state = MDS_GC_TASK_QUARANTINED;
                task->last_error = MDS_ERR_INVAL;
                task->lease_owner_mds_id = 0;
                task->lease_owner_boot_epoch = 0;
                task->lease_expiry_ns = 0;
                continue;
            }
            eligible = (task->state == MDS_GC_TASK_PENDING &&
                        task->not_before_ns <= now_ns) ||
                       (task->state == MDS_GC_TASK_CLAIMED &&
                        task->lease_expiry_ns <= now_ns);
            if (!eligible) {
                continue;
            }
            if (best < 0 ||
                mem_gc_task_before(task, &m->gc_tasks[best].task)) {
                best = (int)i;
            }
        }
        if (best < 0) {
            break;
        }
        bool takeover =
            m->gc_tasks[best].task.state == MDS_GC_TASK_CLAIMED;
        m->gc_tasks[best].task.state = MDS_GC_TASK_CLAIMED;
        m->gc_tasks[best].task.attempt_count++;
        m->gc_tasks[best].task.lease_owner_mds_id = owner_mds_id;
        m->gc_tasks[best].task.lease_owner_boot_epoch = owner_boot_epoch;
        m->gc_tasks[best].task.lease_expiry_ns = now_ns + lease_ns;
        tasks[*n_out] = m->gc_tasks[best].task;
        tasks[*n_out].claim_flags = takeover
            ? MDS_GC_TASK_CLAIM_F_TAKEOVER : 0;
        (*n_out)++;
    }
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

static enum mds_status mem_gc_task_renew(
    struct mds_catalogue *cat, uint8_t task_kind, uint64_t task_id,
    uint32_t owner_mds_id, uint64_t owner_boot_epoch, uint32_t lease_ms)
{
    struct memdb *m = cat->backend_private;
    int index;

    if (lease_ms == 0) {
        return MDS_ERR_INVAL;
    }
    pthread_mutex_lock(&m->lock);
    index = mem_gc_task_find(m, task_kind, task_id);
    if (index < 0) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_NOTFOUND;
    }
    struct mds_gc_task *task = &m->gc_tasks[index].task;
    if (task->state != MDS_GC_TASK_CLAIMED ||
        task->lease_owner_mds_id != owner_mds_id ||
        task->lease_owner_boot_epoch != owner_boot_epoch) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_STALE;
    }
    task->lease_expiry_ns = mem_gc_task_now_ns() +
                            (uint64_t)lease_ms * 1000000ULL;
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

static enum mds_status mem_gc_task_reschedule(
    struct mds_catalogue *cat, uint8_t task_kind, uint64_t task_id,
    uint32_t owner_mds_id, uint64_t owner_boot_epoch, int32_t last_error,
    uint32_t retry_ms)
{
    struct memdb *m = cat->backend_private;
    int index;

    pthread_mutex_lock(&m->lock);
    index = mem_gc_task_find(m, task_kind, task_id);
    if (index < 0) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_NOTFOUND;
    }
    struct mds_gc_task *task = &m->gc_tasks[index].task;
    if (task->state != MDS_GC_TASK_CLAIMED ||
        task->lease_owner_mds_id != owner_mds_id ||
        task->lease_owner_boot_epoch != owner_boot_epoch) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_STALE;
    }
    task->state = MDS_GC_TASK_PENDING;
    task->last_error = last_error;
    task->not_before_ns = mem_gc_task_now_ns() +
                          (uint64_t)retry_ms * 1000000ULL;
    task->lease_owner_mds_id = 0;
    task->lease_owner_boot_epoch = 0;
    task->lease_expiry_ns = 0;
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

static enum mds_status mem_gc_task_complete(
    struct mds_catalogue *cat, uint8_t task_kind, uint64_t task_id,
    uint32_t owner_mds_id, uint64_t owner_boot_epoch)
{
    struct memdb *m = cat->backend_private;
    int index;

    pthread_mutex_lock(&m->lock);
    index = mem_gc_task_find(m, task_kind, task_id);
    if (index < 0) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_NOTFOUND;
    }
    const struct mds_gc_task *task = &m->gc_tasks[index].task;
    if (task->state != MDS_GC_TASK_CLAIMED ||
        task->lease_owner_mds_id != owner_mds_id ||
        task->lease_owner_boot_epoch != owner_boot_epoch) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_STALE;
    }
    if (task_kind == MDS_GC_TASK_LEGACY_DS_UNLINK) {
        for (uint32_t i = 0; i < MEMDB_MAX_GC; i++) {
            if (m->gc_queue[i].used &&
                m->gc_queue[i].entry.gc_seq == task_id) {
                m->gc_queue[i].used = 0;
                break;
            }
        }
    }
    m->gc_tasks[index].used = 0;
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}

/* GC queue */
static enum mds_status mem_gc_enqueue(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid,
    uint32_t ds_id, const uint8_t *nfs_fh, uint32_t fh_len)
{
    struct memdb *m = cat->backend_private;
    struct mds_gc_task task;
    uint32_t gc_slot = MEMDB_MAX_GC;
    uint32_t task_slot = MEMDB_MAX_GC_TASKS;
    uint64_t now_ns;

    (void)txn;

    if (fh_len > MDS_NFS_FH_MAX || (fh_len > 0 && nfs_fh == NULL)) {
        return MDS_ERR_INVAL;
    }
    pthread_mutex_lock(&m->lock);
    for (uint32_t i = 0; i < MEMDB_MAX_GC; i++) {
        if (!m->gc_queue[i].used) {
            gc_slot = i;
            break;
        }
    }
    for (uint32_t i = 0; i < MEMDB_MAX_GC_TASKS; i++) {
        if (!m->gc_tasks[i].used) {
            task_slot = i;
            break;
        }
    }
    if (gc_slot == MEMDB_MAX_GC || task_slot == MEMDB_MAX_GC_TASKS) {
        pthread_mutex_unlock(&m->lock);
        return MDS_ERR_NOSPC;
    }
    now_ns = mem_gc_task_now_ns();
    memset(&task, 0, sizeof(task));
    task.task_kind = MDS_GC_TASK_LEGACY_DS_UNLINK;
    task.task_id = m->gc_seq_next;
    task.fileid = fileid;
    task.state = MDS_GC_TASK_PENDING;
    task.ds_id = ds_id;
    task.nfs_fh_len = fh_len;
    task.created_ns = now_ns;
    task.not_before_ns = now_ns;
    if (fh_len > 0) {
        memcpy(task.nfs_fh, nfs_fh, fh_len);
    }
    m->gc_queue[gc_slot].entry.gc_seq = task.task_id;
    m->gc_queue[gc_slot].entry.fileid = fileid;
    m->gc_queue[gc_slot].entry.ds_id = ds_id;
    m->gc_queue[gc_slot].entry.nfs_fh_len = fh_len;
    if (fh_len > 0) {
        memcpy(m->gc_queue[gc_slot].entry.nfs_fh, nfs_fh, fh_len);
    }
    m->gc_queue[gc_slot].used = 1;
    m->gc_tasks[task_slot].task = task;
    m->gc_tasks[task_slot].used = 1;
    m->gc_seq_next++;
    pthread_mutex_unlock(&m->lock);
    return MDS_OK;
}
static enum mds_status mem_gc_task_quarantine(
    struct mds_catalogue *cat, uint8_t task_kind, uint64_t task_id,
    uint32_t owner_mds_id, uint64_t owner_boot_epoch, int32_t last_error)
{
    struct memdb *memdb = cat->backend_private;
    struct mds_gc_task *task;
    int task_index;

    pthread_mutex_lock(&memdb->lock);
    task_index = mem_gc_task_find(memdb, task_kind, task_id);
    if (task_index < 0) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_NOTFOUND;
    }
    task = &memdb->gc_tasks[task_index].task;
    if (task->state != MDS_GC_TASK_CLAIMED ||
        task->lease_owner_mds_id != owner_mds_id ||
        task->lease_owner_boot_epoch != owner_boot_epoch) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_STALE;
    }
    task->state = MDS_GC_TASK_QUARANTINED;
    task->last_error = last_error;
    task->lease_owner_mds_id = 0;
    task->lease_owner_boot_epoch = 0;
    task->lease_expiry_ns = 0;
    pthread_mutex_unlock(&memdb->lock);
    return MDS_OK;
}

static void mem_gc_task_release_quota_usage(
    struct memdb *memdb, const struct mds_inode *inode)
{
    const uint8_t usage_types[] = {
        MDS_QUOTA_USER_USAGE,
        MDS_QUOTA_GROUP_USAGE,
    };
    const uint64_t scope_ids[] = {
        inode->uid,
        inode->gid,
    };

    for (uint32_t index = 0; index < 2; index++) {
        int usage_index = memdb_find_quota_usage(
            memdb, usage_types[index], scope_ids[index]);

        if (usage_index < 0) {
            continue;
        }
        if (memdb->quota_usage[usage_index].usage.used_bytes > inode->size) {
            memdb->quota_usage[usage_index].usage.used_bytes -= inode->size;
        } else {
            memdb->quota_usage[usage_index].usage.used_bytes = 0;
        }
        if (memdb->quota_usage[usage_index].usage.used_inodes > 0) {
            memdb->quota_usage[usage_index].usage.used_inodes--;
        }
    }
}

static void mem_gc_task_delete_stripe_metadata(
    struct memdb *memdb, const struct mds_inode *inode)
{
    if (inode->flags & MDS_IFLAG_INLINE_STRIPE) {
        return;
    }
    for (uint32_t index = 0; index < MEMDB_MAX_STRIPE; index++) {
        if (memdb->stripes[index].used &&
            memdb->stripes[index].fileid == inode->fileid) {
            free(memdb->stripes[index].entries);
            memdb->stripes[index].entries = NULL;
            memdb->stripes[index].entries_cap = 0;
            memdb->stripes[index].used = 0;
            return;
        }
    }
}

static enum mds_status mem_gc_task_finalize_file(
    struct mds_catalogue *cat, uint64_t fileid,
    uint64_t expected_generation, uint32_t owner_mds_id,
    uint64_t owner_boot_epoch)
{
    struct memdb *memdb = cat->backend_private;
    const struct mds_gc_task *task;
    const struct mds_inode *inode;
    int task_index;
    int inode_index;

    pthread_mutex_lock(&memdb->lock);
    task_index = mem_gc_task_find(
        memdb, MDS_GC_TASK_FILE_UNLINK, fileid);
    if (task_index < 0) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_NOTFOUND;
    }
    task = &memdb->gc_tasks[task_index].task;
    if (task->state != MDS_GC_TASK_CLAIMED ||
        task->lease_owner_mds_id != owner_mds_id ||
        task->lease_owner_boot_epoch != owner_boot_epoch) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_STALE;
    }
    inode_index = memdb_find_inode(memdb, fileid);
    if (inode_index < 0) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_NOTFOUND;
    }
    inode = &memdb->inodes[inode_index];
    if (inode->type != MDS_FTYPE_REG ||
        inode->generation != expected_generation ||
        inode->generation != task->inode_generation ||
        inode->nlink != 0 ||
        !(inode->flags & MDS_IFLAG_DELETE_PENDING)) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_STALE;
    }

    mem_gc_task_delete_stripe_metadata(memdb, inode);
    mem_gc_task_release_quota_usage(memdb, inode);
    memdb->inode_used[inode_index] = 0;
    memdb->gc_tasks[task_index].used = 0;
    pthread_mutex_unlock(&memdb->lock);
    return MDS_OK;
}

static enum mds_status mem_gc_peek(struct mds_catalogue *cat,
    struct mds_gc_entry *entry)
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_GC; i++) {
        if (m->gc_queue[i].used) {
            *entry = m->gc_queue[i].entry;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_gc_dequeue(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t gc_seq)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_GC; i++) {
        if (m->gc_queue[i].used && m->gc_queue[i].entry.gc_seq == gc_seq) {
            m->gc_queue[i].used = 0;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_gc_count(struct mds_catalogue *cat,
    uint32_t *count)
{
    struct memdb *m = cat->backend_private;
    uint32_t n = 0;
    for (uint32_t i = 0; i < MEMDB_MAX_GC; i++) {
        if (m->gc_queue[i].used) n++;
    }
    *count = n;
    return MDS_OK;
}

/* Stubs for operations not commonly used in unit tests */
static enum mds_status mem_stub_notfound(void) { return MDS_ERR_NOTFOUND; }
static enum mds_status mem_stub_ok(void) { return MDS_OK; }
static enum mds_status mem_ds_provision_get(struct mds_catalogue *cat,
    uint32_t ds_id, uint8_t *secret, uint32_t secret_len, uint64_t *epoch)
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_PROVISION; i++) {
        if (m->provisions[i].used && m->provisions[i].ds_id == ds_id) {
            uint32_t copy = m->provisions[i].secret_len;
            if (copy > secret_len) copy = secret_len;
            memcpy(secret, m->provisions[i].secret, copy);
            *epoch = m->provisions[i].epoch;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_ds_provision_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint32_t ds_id,
    const uint8_t *secret, uint32_t secret_len, uint64_t epoch)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_PROVISION; i++) {
        if (m->provisions[i].used && m->provisions[i].ds_id == ds_id) {
            memcpy(m->provisions[i].secret, secret, secret_len);
            m->provisions[i].secret_len = secret_len;
            m->provisions[i].epoch = epoch;
            return MDS_OK;
        }
    }
    for (uint32_t i = 0; i < MEMDB_MAX_PROVISION; i++) {
        if (!m->provisions[i].used) {
            m->provisions[i].used = 1;
            m->provisions[i].ds_id = ds_id;
            memcpy(m->provisions[i].secret, secret, secret_len);
            m->provisions[i].secret_len = secret_len;
            m->provisions[i].epoch = epoch;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOSPC;
}

static enum mds_status mem_ds_provision_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint32_t ds_id)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_PROVISION; i++) {
        if (m->provisions[i].used && m->provisions[i].ds_id == ds_id) {
            m->provisions[i].used = 0;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

/* Dirent get (low-level) */
static enum mds_status mem_dirent_get(struct mds_catalogue *cat,
    uint64_t parent, const char *name,
    uint64_t *child_fileid, uint8_t *child_type)
{
    struct memdb *m = cat->backend_private;
    int idx = memdb_find_dirent(m, parent, name);
    if (idx < 0) return MDS_ERR_NOTFOUND;
    *child_fileid = m->dirents[idx].child_fileid;
    *child_type = m->dirents[idx].child_type;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Coordination ops implementation
 * ----------------------------------------------------------------------- */

static enum mds_status mem_journal_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn,
    const struct mds_coord_journal_record *record)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_JOURNAL; i++) {
        if (!m->journal_used[i]) {
            m->journals[i] = *record;
            m->journal_used[i] = 1;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOSPC;
}

static enum mds_status mem_journal_get(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t txn_id, uint8_t role,
    struct mds_coord_journal_record *record)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_JOURNAL; i++) {
        if (m->journal_used[i] &&
            m->journals[i].txn_id == txn_id &&
            m->journals[i].role == role) {
            *record = m->journals[i];
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_journal_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t txn_id, uint8_t role)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_JOURNAL; i++) {
        if (m->journal_used[i] &&
            m->journals[i].txn_id == txn_id &&
            m->journals[i].role == role) {
            m->journal_used[i] = 0;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_journal_scan(struct mds_catalogue *cat,
    mds_coord_journal_scan_cb cb, void *ctx)
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_JOURNAL; i++) {
        if (m->journal_used[i]) {
            if (cb(&m->journals[i], ctx) != 0)
                break;
        }
    }
    return MDS_OK;
}

/* Layout state */
static enum mds_status mem_layout_grant(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t clientid,
    uint64_t fileid, uint32_t iomode,
    uint64_t offset, uint64_t length,
    const struct nfs4_stateid *stateid,
    const uint32_t *ds_ids, uint32_t ds_count)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    uint32_t *ids_copy = NULL;

    if (ds_count > MDS_LAYOUT_DS_ID_MAX ||
        (ds_count > 0 && ds_ids == NULL)) {
        return MDS_ERR_INVAL;
    }
    if (ds_count > 0) {
        ids_copy = calloc(ds_count, sizeof(*ids_copy));
        if (ids_copy == NULL) {
            return MDS_ERR_NOMEM;
        }
        memcpy(ids_copy, ds_ids, ds_count * sizeof(*ids_copy));
    }
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        if (!m->layouts[i].used) {
            m->layouts[i].used = 1;
            m->layouts[i].clientid = clientid;
            m->layouts[i].fileid = fileid;
            m->layouts[i].iomode = iomode;
            m->layouts[i].offset = offset;
            m->layouts[i].length = length;
            if (stateid) m->layouts[i].stateid = *stateid;
            else memset(&m->layouts[i].stateid, 0, sizeof(m->layouts[i].stateid));
            free(m->layouts[i].ds_ids);
            m->layouts[i].ds_ids = ids_copy;
            m->layouts[i].ds_count = ds_count;
            return MDS_OK;
        }
    }
    free(ids_copy);
    return MDS_ERR_NOSPC;
}

static enum mds_status mem_layout_return(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, const uint8_t stateid_other[12],
    uint64_t clientid, uint64_t fileid,
    const uint32_t *ds_ids, uint32_t ds_count)
{
    (void)txn; (void)clientid; (void)fileid; (void)ds_ids; (void)ds_count;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        if (m->layouts[i].used &&
            memcmp(m->layouts[i].stateid.other, stateid_other, 12) == 0) {
            free(m->layouts[i].ds_ids);
            m->layouts[i].ds_ids = NULL;
            m->layouts[i].ds_count = 0;
            m->layouts[i].used = 0;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_layout_get_by_stateid(struct mds_catalogue *cat,
    const uint8_t stateid_other[12],
    uint64_t *clientid, uint64_t *fileid,
    uint32_t *iomode, uint64_t *offset,
    uint64_t *length, uint32_t *seqid)
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        if (m->layouts[i].used &&
            memcmp(m->layouts[i].stateid.other, stateid_other, 12) == 0) {
            if (clientid) *clientid = m->layouts[i].clientid;
            if (fileid) *fileid = m->layouts[i].fileid;
            if (iomode) *iomode = m->layouts[i].iomode;
            if (offset) *offset = m->layouts[i].offset;
            if (length) *length = m->layouts[i].length;
            if (seqid) *seqid = m->layouts[i].stateid.seqid;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_layout_scan_for_file(struct mds_catalogue *cat,
    uint64_t fileid, bool *has_layout)
{
    struct memdb *m = cat->backend_private;
    *has_layout = false;
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        if (m->layouts[i].used && m->layouts[i].fileid == fileid) {
            *has_layout = true;
            return MDS_OK;
        }
    }
    return MDS_OK;
}

static enum mds_status mem_ds_layout_idx_scan(struct mds_catalogue *cat,
    uint32_t ds_id, mds_coord_ds_layout_cb cb, void *ctx)
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        if (!m->layouts[i].used) continue;
        if (m->layouts[i].ds_count > 0 && m->layouts[i].ds_ids == NULL) {
            continue;
        }
        for (uint32_t j = 0; j < m->layouts[i].ds_count; j++) {
            if (m->layouts[i].ds_ids[j] == ds_id) {
                if (cb(m->layouts[i].clientid,
                       m->layouts[i].fileid, ctx) != 0)
                    return MDS_OK;
            }
        }
    }
    return MDS_OK;
}

/* Client recovery */
static enum mds_status mem_recovery_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t clientid,
    const uint8_t *co_ownerid, uint32_t co_ownerid_len,
    const uint8_t verifier[8])
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_RECOVERY; i++) {
        if (!m->recoveries[i].used) {
            m->recoveries[i].used = 1;
            m->recoveries[i].clientid = clientid;
            memcpy(m->recoveries[i].co_ownerid, co_ownerid, co_ownerid_len);
            m->recoveries[i].co_ownerid_len = co_ownerid_len;
            memcpy(m->recoveries[i].verifier, verifier, 8);
            return MDS_OK;
        }
    }
    return MDS_ERR_NOSPC;
}

static enum mds_status mem_recovery_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t clientid)
{
    (void)txn;
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_RECOVERY; i++) {
        if (m->recoveries[i].used && m->recoveries[i].clientid == clientid) {
            m->recoveries[i].used = 0;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_recovery_get(struct mds_catalogue *cat,
    uint64_t clientid, uint8_t *co_ownerid,
    uint32_t *co_ownerid_len, uint8_t verifier[8])
{
    struct memdb *m = cat->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_RECOVERY; i++) {
        if (m->recoveries[i].used && m->recoveries[i].clientid == clientid) {
            if (co_ownerid)
                memcpy(co_ownerid, m->recoveries[i].co_ownerid,
                       m->recoveries[i].co_ownerid_len);
            if (co_ownerid_len)
                *co_ownerid_len = m->recoveries[i].co_ownerid_len;
            if (verifier)
                memcpy(verifier, m->recoveries[i].verifier, 8);
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

/* Shard fileid map */
#define MEMDB_MAX_SHARD_FID 256
struct memdb_shard_fid { uint64_t fileid; uint32_t shard_id; int used; };
static struct memdb_shard_fid g_shard_fids[MEMDB_MAX_SHARD_FID];

static enum mds_status mem_shard_fileid_get(struct mds_catalogue *cat,
    uint64_t fileid, uint32_t *shard_id)
{
    (void)cat;
    for (uint32_t i = 0; i < MEMDB_MAX_SHARD_FID; i++) {
        if (g_shard_fids[i].used && g_shard_fids[i].fileid == fileid) {
            *shard_id = g_shard_fids[i].shard_id;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_shard_fileid_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid, uint32_t shard_id)
{
    (void)cat; (void)txn;
    for (uint32_t i = 0; i < MEMDB_MAX_SHARD_FID; i++) {
        if (g_shard_fids[i].used && g_shard_fids[i].fileid == fileid) {
            g_shard_fids[i].shard_id = shard_id;
            return MDS_OK;
        }
    }
    for (uint32_t i = 0; i < MEMDB_MAX_SHARD_FID; i++) {
        if (!g_shard_fids[i].used) {
            g_shard_fids[i].used = 1;
            g_shard_fids[i].fileid = fileid;
            g_shard_fids[i].shard_id = shard_id;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOSPC;
}

static enum mds_status mem_shard_fileid_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t fileid)
{
    (void)cat; (void)txn;
    for (uint32_t i = 0; i < MEMDB_MAX_SHARD_FID; i++) {
        if (g_shard_fids[i].used && g_shard_fids[i].fileid == fileid) {
            g_shard_fids[i].used = 0;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

/* Ext dirents */
#define MEMDB_MAX_EXT_DIRENT 64
struct memdb_ext_dirent {
    uint64_t parent; char name[MDS_MAX_NAME + 1];
    uint32_t owner_mds_id; uint64_t target_fileid;
    uint8_t target_type; uint64_t anchor_id; int used;
};
static struct memdb_ext_dirent g_ext_dirents[MEMDB_MAX_EXT_DIRENT];

static enum mds_status mem_ext_dirent_get(struct mds_catalogue *cat,
    uint64_t parent, const char *name,
    uint32_t *owner_mds_id, uint64_t *target_fileid,
    uint8_t *target_type, uint64_t *anchor_id)
{
    (void)cat;
    for (uint32_t i = 0; i < MEMDB_MAX_EXT_DIRENT; i++) {
        if (g_ext_dirents[i].used && g_ext_dirents[i].parent == parent &&
            strcmp(g_ext_dirents[i].name, name) == 0) {
            if (owner_mds_id) *owner_mds_id = g_ext_dirents[i].owner_mds_id;
            if (target_fileid) *target_fileid = g_ext_dirents[i].target_fileid;
            if (target_type) *target_type = g_ext_dirents[i].target_type;
            if (anchor_id) *anchor_id = g_ext_dirents[i].anchor_id;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

static enum mds_status mem_ext_dirent_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent, const char *name,
    uint32_t owner_mds_id, uint64_t target_fileid,
    uint8_t target_type, uint64_t anchor_id)
{
    (void)cat; (void)txn;
    for (uint32_t i = 0; i < MEMDB_MAX_EXT_DIRENT; i++) {
        if (!g_ext_dirents[i].used) {
            g_ext_dirents[i].used = 1;
            g_ext_dirents[i].parent = parent;
            snprintf(g_ext_dirents[i].name, sizeof(g_ext_dirents[i].name), "%s", name);
            g_ext_dirents[i].owner_mds_id = owner_mds_id;
            g_ext_dirents[i].target_fileid = target_fileid;
            g_ext_dirents[i].target_type = target_type;
            g_ext_dirents[i].anchor_id = anchor_id;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOSPC;
}

static enum mds_status mem_ext_dirent_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t parent, const char *name)
{
    (void)cat; (void)txn;
    for (uint32_t i = 0; i < MEMDB_MAX_EXT_DIRENT; i++) {
        if (g_ext_dirents[i].used && g_ext_dirents[i].parent == parent &&
            strcmp(g_ext_dirents[i].name, name) == 0) {
            g_ext_dirents[i].used = 0;
            return MDS_OK;
        }
    }
    return MDS_ERR_NOTFOUND;
}

/* Link anchors -- stub */
static enum mds_status mem_link_anchor_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t anchor_id,
    uint32_t remote_mds_id, uint64_t parent_fileid, const char *name)
{ (void)cat;(void)txn;(void)anchor_id;(void)remote_mds_id;(void)parent_fileid;(void)name; return MDS_OK; }

static enum mds_status mem_link_anchor_del(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint64_t anchor_id)
{ (void)cat;(void)txn;(void)anchor_id; return MDS_OK; }

/* Quota rules remain stubs; usage supports accounting tests. */
static enum mds_status mem_quota_rule_get(struct mds_catalogue *cat,
    uint8_t scope_type, uint64_t scope_id, struct mds_quota_rule *rule)
{ (void)cat;(void)scope_type;(void)scope_id;(void)rule; return MDS_ERR_NOTFOUND; }
static enum mds_status mem_quota_rule_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint8_t scope_type, uint64_t scope_id,
    const struct mds_quota_rule *rule)
{ (void)cat;(void)txn;(void)scope_type;(void)scope_id;(void)rule; return MDS_OK; }
static enum mds_status mem_quota_usage_get(struct mds_catalogue *cat,
    uint8_t usage_type, uint64_t scope_id, struct mds_quota_usage *usage)
{
    struct memdb *memdb;
    int usage_index;

    if (cat == NULL || usage == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb = cat->backend_private;
    pthread_mutex_lock(&memdb->lock);
    usage_index = memdb_find_quota_usage(memdb, usage_type, scope_id);
    if (usage_index >= 0) {
        *usage = memdb->quota_usage[usage_index].usage;
    }
    pthread_mutex_unlock(&memdb->lock);
    return usage_index >= 0 ? MDS_OK : MDS_ERR_NOTFOUND;
}
static enum mds_status mem_quota_usage_put(struct mds_catalogue *cat,
    struct mds_cat_txn *txn, uint8_t usage_type, uint64_t scope_id,
    const struct mds_quota_usage *usage)
{
    struct memdb *memdb;
    int usage_index;

    (void)txn;
    if (cat == NULL || usage == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb = cat->backend_private;
    pthread_mutex_lock(&memdb->lock);
    usage_index = memdb_find_quota_usage(memdb, usage_type, scope_id);
    if (usage_index < 0) {
        for (uint32_t index = 0; index < MEMDB_MAX_QUOTA_USAGE; index++) {
            if (!memdb->quota_usage[index].used) {
                usage_index = (int)index;
                memdb->quota_usage[index].used = true;
                memdb->quota_usage[index].usage_type = usage_type;
                memdb->quota_usage[index].scope_id = scope_id;
                break;
            }
        }
    }
    if (usage_index >= 0) {
        memdb->quota_usage[usage_index].usage = *usage;
    }
    pthread_mutex_unlock(&memdb->lock);
    return usage_index >= 0 ? MDS_OK : MDS_ERR_NOSPC;
}

/* Coord stubs for operations not used in basic tests.
 * Each stub matches its vtable signature exactly to avoid
 * ISO C function-pointer-to-void* conversion warnings. */
static enum mds_status mem_layout_del_all_for_client(struct mds_catalogue *c, uint64_t cid)
{
    struct memdb *m = c->backend_private;

    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        if (m->layouts[i].used && m->layouts[i].clientid == cid) {
            free(m->layouts[i].ds_ids);
            m->layouts[i].ds_ids = NULL;
            m->layouts[i].ds_count = 0;
            m->layouts[i].used = 0;
        }
    }
    return MDS_OK;
}
/*
 * Iterate every layout-state row whose fileid matches @fid, invoking
 * @cb once per row with a transient (clientid, stateid, iomode) tuple.
 *
 * The coordination-layer callback contract (see
 * mds_coord_layout_file_iter_cb in include/mds_coordination.h):
 *   return 0 to continue, non-zero to stop.  The caller (e.g.
 *   layout_recall byte-range collector) looks up the row's
 *   (offset, length) via mds_coord_layout_get_by_stateid() keyed on
 *   stateid->other, so we only need to forward the in-row stateid as-is.
 *
 * Implementation mirrors mem_ds_layout_idx_scan above: linear scan of
 * the bounded m->layouts[] array under the same single-threaded test
 * harness assumptions (no locking).
 */
static enum mds_status mem_layout_iter_file(struct mds_catalogue *c, uint64_t fid,
    mds_coord_layout_file_iter_cb cb, void *ctx)
{
    struct memdb *m;

    if (c == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    m = c->backend_private;
    if (m == NULL) {
        return MDS_ERR_INVAL;
    }
    for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
        if (!m->layouts[i].used) {
            continue;
        }
        if (m->layouts[i].fileid != fid) {
            continue;
        }
        if (cb(m->layouts[i].clientid,
               &m->layouts[i].stateid,
               m->layouts[i].iomode, ctx) != 0) {
            break;
        }
    }
    return MDS_OK;
}
static enum mds_status mem_recovery_list(struct mds_catalogue *c, uint32_t mid,
    mds_recovery_list_cb cb, void *ctx)
{
    (void)mid;
    struct memdb *m = c->backend_private;
    for (uint32_t i = 0; i < MEMDB_MAX_RECOVERY; i++) {
        if (m->recoveries[i].used) {
            if (cb(m->recoveries[i].clientid, mid, 0, ctx) != 0)
                break;
        }
    }
    return MDS_OK;
}
static int mem_open_find(
    const struct memdb *memdb, const uint8_t stateid_other[12])
{
    for (uint32_t index = 0; index < MEMDB_MAX_OPENS; index++) {
        if (memdb->opens[index].used &&
            memcmp(memdb->opens[index].row.stateid_other, stateid_other,
                   sizeof(memdb->opens[index].row.stateid_other)) == 0) {
            return (int)index;
        }
    }
    return -1;
}

static enum mds_status mem_open_put(
    struct mds_catalogue *cat, const struct mds_coord_open_row *row)
{
    struct memdb *memdb = cat->backend_private;
    struct mds_final_unlink_result unlink_result;
    uint64_t unlink_parent_fileid = 0;
    uint64_t unlink_fileid = 0;
    uint64_t unlink_generation = 0;
    char unlink_name[MDS_MAX_NAME + 1] = {0};
    bool run_unlink = false;
    int open_index;

    if (row == NULL || row->open_owner_len > sizeof(row->open_owner)) {
        return MDS_ERR_INVAL;
    }
    pthread_mutex_lock(&memdb->lock);
    open_index = mem_open_find(memdb, row->stateid_other);
    if (open_index < 0) {
        for (uint32_t index = 0; index < MEMDB_MAX_OPENS; index++) {
            if (!memdb->opens[index].used) {
                open_index = (int)index;
                break;
            }
        }
    }
    if (open_index < 0) {
        pthread_mutex_unlock(&memdb->lock);
        return MDS_ERR_NOSPC;
    }
    memdb->opens[open_index].row = *row;
    memdb->opens[open_index].used = 1;
    if (memdb->unlink_after_open_put.armed) {
        run_unlink = true;
        unlink_parent_fileid =
            memdb->unlink_after_open_put.parent_fileid;
        unlink_fileid = memdb->unlink_after_open_put.fileid;
        unlink_generation = memdb->unlink_after_open_put.generation;
        memcpy(unlink_name, memdb->unlink_after_open_put.name,
               sizeof(unlink_name));
        memdb->unlink_after_open_put.armed = false;
    }
    pthread_mutex_unlock(&memdb->lock);
    if (run_unlink &&
        mem_ns_remove_final_file(
            cat, unlink_parent_fileid, unlink_name, unlink_fileid,
            unlink_generation, &unlink_result) != MDS_OK) {
        return MDS_ERR_IO;
    }
    return MDS_OK;
}

static enum mds_status mem_open_get(
    struct mds_catalogue *cat, const uint8_t stateid_other[12],
    struct mds_coord_open_row *row)
{
    struct memdb *memdb = cat->backend_private;
    int open_index;

    if (row == NULL) {
        return MDS_ERR_INVAL;
    }
    pthread_mutex_lock(&memdb->lock);
    open_index = mem_open_find(memdb, stateid_other);
    if (open_index >= 0) {
        *row = memdb->opens[open_index].row;
    }
    pthread_mutex_unlock(&memdb->lock);
    return open_index >= 0 ? MDS_OK : MDS_ERR_NOTFOUND;
}

static enum mds_status mem_open_del(
    struct mds_catalogue *cat, const uint8_t stateid_other[12])
{
    struct memdb *memdb = cat->backend_private;
    int open_index;

    pthread_mutex_lock(&memdb->lock);
    open_index = mem_open_find(memdb, stateid_other);
    if (open_index >= 0) {
        memdb->opens[open_index].used = 0;
    }
    pthread_mutex_unlock(&memdb->lock);
    return open_index >= 0 ? MDS_OK : MDS_ERR_NOTFOUND;
}

static enum mds_status mem_open_scan(
    struct mds_catalogue *cat, uint64_t id, bool by_file,
    mds_coord_open_scan_cb callback, void *ctx)
{
    struct memdb *memdb = cat->backend_private;

    if (callback == NULL) {
        return MDS_ERR_INVAL;
    }
    pthread_mutex_lock(&memdb->lock);
    for (uint32_t index = 0; index < MEMDB_MAX_OPENS; index++) {
        const struct mds_coord_open_row *row = &memdb->opens[index].row;

        if (!memdb->opens[index].used ||
            (by_file ? row->fileid : row->clientid) != id) {
            continue;
        }
        if (callback(row, ctx) != 0) {
            break;
        }
    }
    pthread_mutex_unlock(&memdb->lock);
    return MDS_OK;
}

static enum mds_status mem_open_scan_file(
    struct mds_catalogue *cat, uint64_t fileid,
    mds_coord_open_scan_cb callback, void *ctx)
{
    return mem_open_scan(cat, fileid, true, callback, ctx);
}

static enum mds_status mem_open_scan_client(
    struct mds_catalogue *cat, uint64_t clientid,
    mds_coord_open_scan_cb callback, void *ctx)
{
    return mem_open_scan(cat, clientid, false, callback, ctx);
}
static enum mds_status mem_lock_put(struct mds_catalogue *c, const struct mds_coord_lock_row *r)
{ (void)c;(void)r; return MDS_OK; }
static enum mds_status mem_lock_del(struct mds_catalogue *c, uint64_t f, uint64_t l)
{ (void)c;(void)f;(void)l; return MDS_OK; }
static enum mds_status mem_lock_test(struct mds_catalogue *c, uint64_t f, uint32_t t,
    uint64_t o, uint64_t l, uint64_t cid, const uint8_t *ow, uint32_t ol, struct mds_coord_lock_row *co)
{ (void)c;(void)f;(void)t;(void)o;(void)l;(void)cid;(void)ow;(void)ol;(void)co; return MDS_ERR_NOTFOUND; }
static enum mds_status mem_lock_scan_file(struct mds_catalogue *c, uint64_t f, mds_coord_lock_scan_cb cb, void *ctx)
{ (void)c;(void)f;(void)cb;(void)ctx; return MDS_OK; }
static enum mds_status mem_lock_scan_owner(struct mds_catalogue *c, uint64_t cid, const uint8_t *ow, uint32_t ol,
    mds_coord_lock_scan_cb cb, void *ctx)
{ (void)c;(void)cid;(void)ow;(void)ol;(void)cb;(void)ctx; return MDS_OK; }
static enum mds_status mem_lock_reap_client(struct mds_catalogue *c, uint64_t cid)
{ (void)c;(void)cid; return MDS_OK; }
static enum mds_status mem_deleg_put(struct mds_catalogue *c, const struct mds_coord_deleg_row *r)
{ (void)c;(void)r; return MDS_OK; }
static enum mds_status mem_deleg_get(struct mds_catalogue *c, const uint8_t o[12], struct mds_coord_deleg_row *r)
{ (void)c;(void)o;(void)r; return MDS_ERR_NOTFOUND; }
static enum mds_status mem_deleg_del(struct mds_catalogue *c, const uint8_t o[12])
{ (void)c;(void)o; return MDS_OK; }
static enum mds_status mem_deleg_scan_file(struct mds_catalogue *c, uint64_t f, mds_coord_deleg_scan_cb cb, void *ctx)
{ (void)c;(void)f;(void)cb;(void)ctx; return MDS_OK; }
static enum mds_status mem_deleg_scan_client(struct mds_catalogue *c, uint64_t cid, mds_coord_deleg_scan_cb cb, void *ctx)
{ (void)c;(void)cid;(void)cb;(void)ctx; return MDS_OK; }
static enum mds_status mem_client_put(struct mds_catalogue *c, const struct mds_coord_client_row *r)
{ (void)c;(void)r; return MDS_OK; }
static enum mds_status mem_client_get(struct mds_catalogue *c, uint64_t cid, struct mds_coord_client_row *r)
{ (void)c;(void)cid;(void)r; return MDS_ERR_NOTFOUND; }
static enum mds_status mem_client_del(struct mds_catalogue *c, uint64_t cid)
{ (void)c;(void)cid; return MDS_OK; }
static enum mds_status mem_session_put(struct mds_catalogue *c, const struct mds_coord_session_row *r)
{ (void)c;(void)r; return MDS_OK; }
static enum mds_status mem_session_get(struct mds_catalogue *c, const uint8_t s[16], struct mds_coord_session_row *r)
{ (void)c;(void)s;(void)r; return MDS_ERR_NOTFOUND; }
static enum mds_status mem_session_del(struct mds_catalogue *c, const uint8_t s[16])
{ (void)c;(void)s; return MDS_OK; }
static enum mds_status mem_session_scan_client(struct mds_catalogue *c, uint64_t cid, mds_coord_session_scan_cb cb, void *ctx)
{ (void)c;(void)cid;(void)cb;(void)ctx; return MDS_OK; }
static enum mds_status mem_slot_put(struct mds_catalogue *c, const uint8_t s[16], uint32_t si,
    uint32_t seq, const void *cr, uint32_t rl)
{ (void)c;(void)s;(void)si;(void)seq;(void)cr;(void)rl; return MDS_OK; }
static enum mds_status mem_slot_get(struct mds_catalogue *c, const uint8_t s[16], uint32_t si,
    struct mds_coord_drc_slot_row *r)
{ (void)c;(void)s;(void)si;(void)r; return MDS_ERR_NOTFOUND; }

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */

static void mem_close(struct mds_catalogue *cat)
{
    if (cat == NULL) return;
    struct memdb *m = cat->backend_private;
    if (m != NULL) {
        /* Phase A heap conversion: free the per-record entries[]
         * buffers before freeing the memdb itself. */
        for (uint32_t i = 0; i < MEMDB_MAX_STRIPE; i++) {
            free(m->stripes[i].entries);
            m->stripes[i].entries = NULL;
            m->stripes[i].entries_cap = 0;
        }
        for (uint32_t i = 0; i < MEMDB_MAX_LAYOUTS; i++) {
            free(m->layouts[i].ds_ids);
            m->layouts[i].ds_ids = NULL;
            m->layouts[i].ds_count = 0;
        }
        pthread_mutex_destroy(&m->lock);
        free(m);
    }
}

static enum mds_status mem_probe(struct mds_catalogue *cat)
{
    (void)cat;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Vtables
 * ----------------------------------------------------------------------- */

static const struct mds_catalogue_ops memdb_lifecycle_ops = {
    .close = mem_close,
    .probe = mem_probe,
};

static const struct mds_authority_ops memdb_auth_ops = {
    .ns_create       = mem_ns_create,
    .ns_create_wide  = mem_ns_create_wide,
    .ns_remove       = mem_ns_remove,
    .ns_remove_final_file = mem_ns_remove_final_file,
    .ns_rename       = mem_ns_rename,
    .ns_link         = mem_ns_link,
    .ns_lookup       = mem_ns_lookup,
    .ns_getattr      = mem_ns_getattr,
    .ns_setattr      = mem_ns_setattr,
    .ns_setattr_size_extend = mem_ns_setattr_size_extend,
    .ns_readdir      = mem_ns_readdir,
    .ns_readdir_plus_from = mem_ns_readdir_plus_from,
    .dirent_name_for_child = mem_dirent_name_for_child,
    .ns_nlink_adjust = mem_ns_nlink_adjust,
    .alloc_fileid    = mem_alloc_fileid,
    .inode_put       = mem_inode_put,
    .inode_del       = mem_inode_del,
    .dirent_put      = mem_dirent_put,
    .dirent_del      = mem_dirent_del,
    .inline_get      = mem_inline_get,
    .inline_put      = mem_inline_put,
    .inline_del      = mem_inline_del,
    .xattr_get       = mem_xattr_get,
    .xattr_put       = mem_xattr_put,
    .xattr_del       = mem_xattr_del,
    .xattr_list      = mem_xattr_list,
    .xattr_exists    = mem_xattr_exists,
    .stripe_map_get  = mem_stripe_map_get,
    .stripe_map_put  = mem_stripe_map_put,
    .stripe_map_del  = mem_stripe_map_del,
    .stripe_map_scan = mem_stripe_map_scan,
    .ds_get          = mem_ds_get,
    .ds_put          = mem_ds_put,
    .ds_del          = mem_ds_del,
    .ds_list         = mem_ds_list,
    .ds_provision_get = mem_ds_provision_get,
    .ds_provision_put = mem_ds_provision_put,
    .ds_provision_del = mem_ds_provision_del,
    .quota_rule_get  = mem_quota_rule_get,
    .quota_rule_put  = mem_quota_rule_put,
    .quota_usage_get = mem_quota_usage_get,
    .quota_usage_put = mem_quota_usage_put,
    .gc_enqueue      = mem_gc_enqueue,
    .gc_peek         = mem_gc_peek,
    .gc_dequeue      = mem_gc_dequeue,
    .gc_count        = mem_gc_count,
    .gc_task_stats   = mem_gc_task_stats,
    .gc_task_enqueue = mem_gc_task_enqueue,
    .gc_task_claim_batch = mem_gc_task_claim_batch,
    .gc_task_renew = mem_gc_task_renew,
    .gc_task_reschedule = mem_gc_task_reschedule,
    .gc_task_complete = mem_gc_task_complete,
    .gc_task_quarantine = mem_gc_task_quarantine,
    .gc_task_finalize_file = mem_gc_task_finalize_file,
    .shard_fileid_get = mem_shard_fileid_get,
    .shard_fileid_put = mem_shard_fileid_put,
    .shard_fileid_del = mem_shard_fileid_del,
    .ext_dirent_get  = mem_ext_dirent_get,
    .ext_dirent_put  = mem_ext_dirent_put,
    .ext_dirent_del  = mem_ext_dirent_del,
    .link_anchor_put = mem_link_anchor_put,
    .link_anchor_del = mem_link_anchor_del,
};

static const struct mds_coordination_ops memdb_coord_ops = {
    .journal_put              = mem_journal_put,
    .journal_get              = mem_journal_get,
    .journal_del              = mem_journal_del,
    .journal_scan             = mem_journal_scan,
    .layout_grant             = mem_layout_grant,
    .layout_return            = mem_layout_return,
    .layout_get_by_stateid    = mem_layout_get_by_stateid,
    .layout_scan_for_file     = mem_layout_scan_for_file,
    .ds_layout_idx_scan       = mem_ds_layout_idx_scan,
    .layout_iter_file         = mem_layout_iter_file,
    .layout_del_all_for_client = mem_layout_del_all_for_client,
    .recovery_put             = mem_recovery_put,
    .recovery_del             = mem_recovery_del,
    .recovery_get             = mem_recovery_get,
    .recovery_list            = mem_recovery_list,
    .open_put = mem_open_put,
    .open_get = mem_open_get,
    .open_del = mem_open_del,
    .open_scan_file = mem_open_scan_file,
    .open_scan_client = mem_open_scan_client,
    .lock_put = mem_lock_put,
    .lock_del = mem_lock_del,
    .lock_test = mem_lock_test,
    .lock_scan_file = mem_lock_scan_file,
    .lock_scan_owner = mem_lock_scan_owner,
    .lock_reap_client = mem_lock_reap_client,
    .deleg_put = mem_deleg_put,
    .deleg_get = mem_deleg_get,
    .deleg_del = mem_deleg_del,
    .deleg_scan_file = mem_deleg_scan_file,
    .deleg_scan_client = mem_deleg_scan_client,
    .client_put = mem_client_put,
    .client_get = mem_client_get,
    .client_del = mem_client_del,
    .session_put = mem_session_put,
    .session_get = mem_session_get,
    .session_del = mem_session_del,
    .session_scan_client = mem_session_scan_client,
    .slot_put = mem_slot_put,
    .slot_get = mem_slot_get,
};

/* -----------------------------------------------------------------------
 * Public API -- used by test_helpers.h
 * ----------------------------------------------------------------------- */

struct mds_catalogue *catalogue_memdb_open(void)
{
    struct mds_catalogue *cat = calloc(1, sizeof(*cat));
    if (cat == NULL) return NULL;

    struct memdb *m = calloc(1, sizeof(*m));
    if (m == NULL) { free(cat); return NULL; }

    pthread_mutex_init(&m->lock, NULL);

    /* Seed root inode (fileid 2, like RonDB bootstrap). */
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    m->inodes[0].fileid = MDS_FILEID_ROOT;
    m->inodes[0].type = MDS_FTYPE_DIR;
    m->inodes[0].mode = 0755;
    m->inodes[0].nlink = 2;
    m->inodes[0].atime = now;
    m->inodes[0].mtime = now;
    m->inodes[0].ctime = now;
    m->inodes[0].change = 1;
    m->inodes[0].generation = 1;
    m->inode_used[0] = 1;
    m->next_fileid = MDS_FILEID_ROOT + 1;
    m->gc_seq_next = 1;

    cat->backend = MDS_BACKEND_RONDB;  /* Pretend to be RonDB. */
    cat->ops = &memdb_lifecycle_ops;
    cat->auth_ops = &memdb_auth_ops;
    cat->coord_ops = &memdb_coord_ops;
    cat->backend_private = m;

    return cat;
}

void catalogue_memdb_fail_next_final_unlink(
    struct mds_catalogue *cat, enum mds_status status)
{
    struct memdb *memdb;

    if (cat == NULL) {
        return;
    }
    memdb = cat->backend_private;
    pthread_mutex_lock(&memdb->lock);
    memdb->next_final_unlink_status = status;
    pthread_mutex_unlock(&memdb->lock);
}

void catalogue_memdb_unlink_after_next_open_put(
    struct mds_catalogue *cat, uint64_t parent_fileid, const char *name,
    uint64_t fileid, uint64_t generation)
{
    struct memdb *memdb;

    if (cat == NULL || name == NULL) {
        return;
    }
    memdb = cat->backend_private;
    pthread_mutex_lock(&memdb->lock);
    memdb->unlink_after_open_put.armed = true;
    memdb->unlink_after_open_put.parent_fileid = parent_fileid;
    snprintf(memdb->unlink_after_open_put.name,
             sizeof(memdb->unlink_after_open_put.name), "%s", name);
    memdb->unlink_after_open_put.fileid = fileid;
    memdb->unlink_after_open_put.generation = generation;
    pthread_mutex_unlock(&memdb->lock);
}

enum mds_status catalogue_memdb_inject_raw_gc_task(
    struct mds_catalogue *cat, const struct mds_gc_task *task)
{
    struct memdb *memdb;

    if (cat == NULL || task == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb = cat->backend_private;
    pthread_mutex_lock(&memdb->lock);
    for (uint32_t index = 0; index < MEMDB_MAX_GC_TASKS; index++) {
        if (!memdb->gc_tasks[index].used) {
            memdb->gc_tasks[index].task = *task;
            memdb->gc_tasks[index].used = 1;
            pthread_mutex_unlock(&memdb->lock);
            return MDS_OK;
        }
    }
    pthread_mutex_unlock(&memdb->lock);
    return MDS_ERR_NOSPC;
}

enum mds_status catalogue_memdb_get_gc_task(
    struct mds_catalogue *cat, uint8_t task_kind, uint64_t task_id,
    struct mds_gc_task *task)
{
    struct memdb *memdb;
    int task_index;

    if (cat == NULL || task == NULL) {
        return MDS_ERR_INVAL;
    }
    memdb = cat->backend_private;
    pthread_mutex_lock(&memdb->lock);
    task_index = mem_gc_task_find(memdb, task_kind, task_id);
    if (task_index >= 0) {
        *task = memdb->gc_tasks[task_index].task;
    }
    pthread_mutex_unlock(&memdb->lock);
    return task_index >= 0 ? MDS_OK : MDS_ERR_NOTFOUND;
}

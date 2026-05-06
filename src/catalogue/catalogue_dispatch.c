/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_dispatch.c -- Vtable dispatch for mds_cat_* / mds_coord_*.
 *
 * Each public catalogue operation dispatches through the authority
 * or coordination vtable populated at catalogue open time.  This
 * file contains no backend-specific logic.
 */

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "mds_catalogue.h"
#include "mds_coordination.h"
#include "catalogue_internal.h"
#include "commit_queue.h"
#include "migration.h"

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */

/* mds_catalogue_open lives in catalogue_factory.c. */

void mds_catalogue_close(struct mds_catalogue *cat)
{
    if (cat == NULL) {
        return;
    }
    if (cat->ops != NULL && cat->ops->close != NULL) {
        cat->ops->close(cat);
    }
    free(cat);
}

/* Default implementation -- overridden by RonDB backend. */
__attribute__((weak))
void *mds_catalogue_backend_handle(const struct mds_catalogue *cat)
{
    (void)cat;
    return NULL;
}

struct catalog_stats *mds_catalogue_stats(struct mds_catalogue *cat)
{
    if (cat == NULL) {
        return NULL;
    }
    return &cat->stats;
}

enum mds_catalogue_backend mds_catalogue_backend_type(
    const struct mds_catalogue *cat)
{
    if (cat == NULL) {
        return MDS_BACKEND_RONDB;
    }
    return cat->backend;
}

enum mds_status mds_catalogue_probe(struct mds_catalogue *cat)
{
    if (cat == NULL || cat->ops == NULL || cat->ops->probe == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->ops->probe(cat);
}

void mds_catalogue_set_cq(struct mds_catalogue *cat,
                          struct commit_queue *cq)
{
    if (cat != NULL) {
        cat->cq = cq;
    }
}

struct commit_queue *mds_catalogue_get_cq(const struct mds_catalogue *cat)
{
    if (cat == NULL) {
        return NULL;
    }
    return cat->cq;
}

/* -----------------------------------------------------------------------
 * Transaction control
 *
 * RonDB operations are self-contained.  The transaction handle is a
 * lightweight grouping context (txn_private == NULL) that lets callers
 * use the catalogue API uniformly.
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_txn_begin(struct mds_catalogue *cat,
                                  enum mds_cat_txn_flags flags,
                                  struct mds_cat_txn **out)
{
    struct mds_cat_txn *ct;

    if (cat == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }

    ct = calloc(1, sizeof(*ct));
    if (ct == NULL) {
        return MDS_ERR_NOMEM;
    }

    ct->cat = cat;
    ct->txn_backend = cat->backend;
    ct->flags = flags;
    ct->txn_private = NULL; /* RonDB: no raw txn handle */
    *out = ct;
    return MDS_OK;
}

enum mds_status mds_cat_txn_commit(struct mds_cat_txn *txn)
{
    if (txn == NULL) {
        return MDS_ERR_INVAL;
    }
    free(txn);
    return MDS_OK;
}

void mds_cat_txn_abort(struct mds_cat_txn *txn)
{
    free(txn);
}

/* -----------------------------------------------------------------------
 * Authority ops dispatch -- Namespace
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_ns_create(struct mds_catalogue *cat,
                                  struct mds_cat_txn *txn,
                                  uint64_t parent_fileid,
                                  const char *name,
                                  enum mds_file_type type,
                                  uint32_t mode,
                                  uint64_t uid, uint64_t gid,
                                  struct ds_prealloc_ctx *prealloc,
                                  struct mds_inode *out)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ns_create(cat, txn, parent_fileid, name,
                                    type, mode, uid, gid, prealloc, out);
}

enum mds_status mds_cat_ns_remove(struct mds_catalogue *cat,
                                  struct mds_cat_txn *txn,
                                  uint64_t parent_fileid,
                                  const char *name)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ns_remove(cat, txn, parent_fileid, name);
}

enum mds_status mds_cat_ns_rename(struct mds_catalogue *cat,
                                  struct mds_cat_txn *txn,
                                  uint64_t src_parent,
                                  const char *src_name,
                                  uint64_t dst_parent,
                                  const char *dst_name)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ns_rename(cat, txn, src_parent, src_name,
                                    dst_parent, dst_name);
}

enum mds_status mds_cat_ns_link(struct mds_catalogue *cat,
                                struct mds_cat_txn *txn,
                                uint64_t parent_fileid,
                                const char *name,
                                uint64_t target_fileid)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ns_link(cat, txn, parent_fileid, name,
                                  target_fileid);
}

enum mds_status mds_cat_ns_lookup(struct mds_catalogue *cat,
                                  uint64_t parent_fileid,
                                  const char *name,
                                  struct mds_inode *child)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ns_lookup(cat, parent_fileid, name, child);
}

enum mds_status mds_cat_ns_getattr(struct mds_catalogue *cat,
                                   uint64_t fileid,
                                   struct mds_inode *inode)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ns_getattr(cat, fileid, inode);
}

enum mds_status mds_cat_ns_setattr(struct mds_catalogue *cat,
                                   struct mds_cat_txn *txn,
                                   uint64_t fileid,
                                   const struct mds_inode *attrs,
                                   uint32_t mask)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ns_setattr(cat, txn, fileid, attrs, mask);
}

enum mds_status mds_cat_ns_readdir(struct mds_catalogue *cat,
                                   uint64_t parent_fileid,
                                   const char *start_after,
                                   struct mds_cat_txn *txn,
                                   mds_readdir_cb cb, void *ctx)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ns_readdir(cat, parent_fileid, start_after,
                                     txn, cb, ctx);
}

/* -----------------------------------------------------------------------
 * Readdir_plus dispatch with null-safe fallback.
 *
 * Backends that do not implement ns_readdir_plus natively still get a
 * working fused-signature entry point: the fallback drives ns_readdir
 * and issues one ns_getattr per dirent, delivering both through the
 * caller's mds_readdir_plus_cb.  The caller therefore never has to
 * branch on backend capability.
 *
 * The fallback reports each entry's inode availability via the
 * `inode_valid` flag: MDS_OK => true; MDS_ERR_NOTFOUND => false
 * (dangling dirent, matching the native-path race semantics); any
 * other catalogue error propagates out via cb_status and stops the
 * scan early with that status.
 * ----------------------------------------------------------------------- */

struct ns_readdir_plus_fallback_ctx {
    struct mds_catalogue *cat;
    mds_readdir_plus_cb   caller_cb;
    void                 *caller_ctx;
    enum mds_status       last_status;
};

static int ns_readdir_plus_fallback_cb(const struct mds_cat_dirent *entry,
                                       void *arg)
{
    struct ns_readdir_plus_fallback_ctx *fctx = arg;
    struct mds_inode inode;
    enum mds_status gst;
    bool valid;
    int cb_rc;

    if (entry == NULL) { return 0; }

    gst = fctx->cat->auth_ops->ns_getattr(fctx->cat, entry->fileid,
                                          &inode);
    if (gst == MDS_OK) {
        valid = true;
    } else if (gst == MDS_ERR_NOTFOUND) {
        memset(&inode, 0, sizeof(inode));
        valid = false;
    } else {
        fctx->last_status = gst;
        return 1; /* stop scan */
    }

    cb_rc = fctx->caller_cb(entry, valid ? &inode : NULL,
                            valid, fctx->caller_ctx);
    return cb_rc;
}

enum mds_status mds_cat_ns_readdir_plus(struct mds_catalogue *cat,
                                        uint64_t parent_fileid,
                                        const char *start_after,
                                        struct mds_cat_txn *txn,
                                        mds_readdir_plus_cb cb,
                                        void *ctx)
{
    struct ns_readdir_plus_fallback_ctx fctx;
    enum mds_status st;

    if (cat == NULL || cat->auth_ops == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }

    /* Fast path: backend implements the fused op. */
    if (cat->auth_ops->ns_readdir_plus != NULL) {
        return cat->auth_ops->ns_readdir_plus(cat, parent_fileid,
                                              start_after, txn,
                                              cb, ctx);
    }

    /* Fallback: ns_readdir + ns_getattr per entry. */
    fctx.cat         = cat;
    fctx.caller_cb   = cb;
    fctx.caller_ctx  = ctx;
    fctx.last_status = MDS_OK;

    st = cat->auth_ops->ns_readdir(cat, parent_fileid, start_after,
                                   txn, ns_readdir_plus_fallback_cb,
                                   &fctx);
    if (st != MDS_OK) {
        return st;
    }
    return fctx.last_status;
}

enum mds_status mds_cat_resolve_path(struct mds_catalogue *cat,
                                     const char *path,
                                     uint64_t *out_fileid)
{
    char buf[MDS_MAX_PATH];
    uint64_t cur_fileid = MDS_FILEID_ROOT;
    char *saveptr = NULL;
    /* cppcheck-suppress constVariablePointer ; strtok_r yields non-const */
    char *tok;
    size_t plen;

    if (cat == NULL || path == NULL || out_fileid == NULL) {
        return MDS_ERR_INVAL;
    }
    if (path[0] != '/') {
        return MDS_ERR_INVAL;
    }
    if (path[1] == '\0') {
        *out_fileid = MDS_FILEID_ROOT;
        return MDS_OK;
    }

    plen = strlen(path);
    if (plen >= sizeof(buf)) {
        return MDS_ERR_INVAL;
    }
    memcpy(buf, path, plen + 1);

    /* cppcheck-suppress constVariablePointer */
    tok = strtok_r(buf + 1, "/", &saveptr);
    while (tok != NULL) {
        struct mds_inode child;
        enum mds_status st;

        st = mds_cat_ns_lookup(cat, cur_fileid, tok, &child);
        if (st != MDS_OK) {
            return st;
        }
        cur_fileid = child.fileid;
        tok = strtok_r(NULL, "/", &saveptr);
    }

    *out_fileid = cur_fileid;
    return MDS_OK;
}

enum mds_status mds_cat_ns_nlink_adjust(struct mds_catalogue *cat,
                                        uint64_t fileid, int32_t delta)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ns_nlink_adjust(cat, fileid, delta);
}

enum mds_status mds_cat_alloc_fileid(struct mds_catalogue *cat,
                                     struct mds_cat_txn *txn,
                                     uint64_t *fileid)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->alloc_fileid(cat, txn, fileid);
}

/* -----------------------------------------------------------------------
 * Authority ops dispatch -- Low-level inode/dirent ops
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_dirent_get(struct mds_catalogue *cat,
                                   uint64_t parent_fileid,
                                   const char *name,
                                   uint64_t *child_fileid,
                                   uint8_t *child_type)
{
    /* Implemented via ns_lookup + discard inode, or direct shim.
     * For RonDB: call rondb_shim_dirent_get directly. */
    struct mds_inode child;
    enum mds_status st;

    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    st = cat->auth_ops->ns_lookup(cat, parent_fileid, name, &child);
    if (st != MDS_OK) { return st; }
    if (child_fileid != NULL) { *child_fileid = child.fileid; }
    if (child_type != NULL) { *child_type = (uint8_t)child.type; }
    return MDS_OK;
}

enum mds_status mds_cat_dirent_put(struct mds_catalogue *cat,
                                   struct mds_cat_txn *txn,
                                   uint64_t parent_fileid,
                                   const char *name,
                                   uint64_t child_fileid,
                                   uint8_t child_type)
{
    if (cat == NULL || cat->auth_ops == NULL ||
        cat->auth_ops->dirent_put == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->dirent_put(cat, txn, parent_fileid, name,
                                     child_fileid, child_type);
}

enum mds_status mds_cat_dirent_del(struct mds_catalogue *cat,
                                   struct mds_cat_txn *txn,
                                   uint64_t parent_fileid,
                                   const char *name)
{
    if (cat == NULL || cat->auth_ops == NULL ||
        cat->auth_ops->dirent_del == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->dirent_del(cat, txn, parent_fileid, name);
}

/** Readdir callback for mds_cat_dir_is_empty: stop on first entry. */
struct dir_empty_ctx {
    bool found;
};

static int dir_empty_cb(const struct mds_cat_dirent *entry, void *arg)
{
    struct dir_empty_ctx *ctx = arg;

    (void)entry;
    ctx->found = true;
    return 1; /* Stop iteration -- at least one entry exists. */
}

enum mds_status mds_cat_dir_is_empty(struct mds_catalogue *cat,
                                     uint64_t parent_fileid,
                                     bool *empty)
{
    struct dir_empty_ctx ctx = { false };
    enum mds_status st;

    if (cat == NULL || cat->auth_ops == NULL || empty == NULL) {
        return MDS_ERR_INVAL;
    }

    st = cat->auth_ops->ns_readdir(cat, parent_fileid, NULL, NULL,
                                   dir_empty_cb, &ctx);
    /* readdir returns MDS_OK even if the dir is empty (0 entries). */
    *empty = !ctx.found;
    (void)st;
    return MDS_OK;
}

/* cppcheck-suppress constParameterPointer */
enum mds_status mds_cat_inode_del(struct mds_catalogue *cat,
                                  struct mds_cat_txn *txn,
                                  uint64_t fileid)
{
    if (cat == NULL || cat->auth_ops == NULL ||
        cat->auth_ops->inode_del == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->inode_del(cat, txn, fileid);
}

enum mds_status mds_cat_inode_put(struct mds_catalogue *cat,
                                  struct mds_cat_txn *txn,
                                  const struct mds_inode *inode)
{
    if (cat == NULL || cat->auth_ops == NULL || inode == NULL) {
        return MDS_ERR_INVAL;
    }
    if (cat->auth_ops->inode_put == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->auth_ops->inode_put(cat, txn, inode);
}

enum mds_status mds_cat_sync(struct mds_catalogue *cat)
{
    (void)cat;
    return MDS_OK; /* RonDB: all writes are immediately durable. */
}

/* -----------------------------------------------------------------------
 * Catalogue-native subtree DFS iterator
 *
 * Catalogue-native DFS subtree iterator.  Builds a
 * mig_inode_chunk for each inode via catalogue ops.
 * ----------------------------------------------------------------------- */

/** Readdir collector: accumulates dirents into a dynamic array. */
struct cat_iter_dirent_ctx {
    struct mig_dirent *entries;
    uint32_t count;
    uint32_t capacity;
};

static int cat_iter_collect_dirent(const struct mds_cat_dirent *entry,
                                   void *arg)
{
    struct cat_iter_dirent_ctx *ctx = arg;

    if (ctx->count >= ctx->capacity) {
        uint32_t new_cap = ctx->capacity == 0 ? 16 : ctx->capacity * 2;
        struct mig_dirent *tmp = realloc(ctx->entries,
                                         (size_t)new_cap * sizeof(*tmp));
        if (tmp == NULL) {
            return -1;
        }
        ctx->entries = tmp;
        ctx->capacity = new_cap;
    }

    struct mig_dirent *d = &ctx->entries[ctx->count];
    d->child_fileid = entry->fileid;
    d->type = entry->type;
    d->name_len = (uint16_t)strlen(entry->name);
    memcpy(d->name, entry->name, d->name_len + 1);
    ctx->count++;
    return 0;
}

/** Xattr name collector for subtree iteration. */
struct cat_iter_xattr_ctx {
    char   (*names)[256];
    uint32_t count;
    uint32_t capacity;
};

static int cat_iter_collect_xattr_name(const char *name, size_t name_len,
                                       void *arg)
{
    struct cat_iter_xattr_ctx *ctx = arg;

    if (ctx->count >= ctx->capacity) {
        uint32_t new_cap = ctx->capacity == 0 ? 8 : ctx->capacity * 2;
        void *tmp = realloc(ctx->names, (size_t)new_cap * 256);
        if (tmp == NULL) {
            return -1;
        }
        ctx->names = tmp;
        ctx->capacity = new_cap;
    }
    size_t clen = name_len < 255 ? name_len : 255;
    memcpy(ctx->names[ctx->count], name, clen);
    ctx->names[ctx->count][clen] = '\0';
    ctx->count++;
    return 0;
}

/**
 * Recursive DFS helper -- catalogue-native.
 */
/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
static enum mds_status cat_subtree_dfs(struct mds_catalogue *cat,
                                       uint64_t fileid,
                                       mds_cat_subtree_iter_cb cb,
                                       void *arg)
{
    struct mig_inode_chunk chunk;
    struct cat_iter_dirent_ctx dc;
    struct cat_iter_xattr_ctx xc;
    enum mds_status st;

    memset(&chunk, 0, sizeof(chunk));
    memset(&dc, 0, sizeof(dc));
    memset(&xc, 0, sizeof(xc));

    /* 1. Read inode. */
    chunk.fileid = fileid;
    st = mds_cat_ns_getattr(cat, fileid, &chunk.inode);
    if (st != MDS_OK) {
        return st;
    }
    chunk.inode.fileid = fileid;

    /* 2. Collect dirents (directories only). */
    if (chunk.inode.type == MDS_FTYPE_DIR) {
        st = mds_cat_ns_readdir(cat, fileid, NULL, NULL,
                                cat_iter_collect_dirent, &dc);
        if (st != MDS_OK) {
            goto out_free;
        }
    }
    chunk.dirent_count = dc.count;
    chunk.dirents = dc.entries;

    /* 3. Read stripe map (regular files only). */
    chunk.has_stripe_map = 0;
    if (chunk.inode.type == MDS_FTYPE_REG) {
        uint32_t sc = 0, su = 0, mc = 0;
        struct mds_ds_map_entry *sm_entries = NULL;

        st = mds_cat_stripe_map_get(cat, fileid,
                                    &sc, &su, &mc, &sm_entries);
        if (st == MDS_OK && sc > 0) {
            chunk.has_stripe_map = 1;
            chunk.stripe_map.stripe_count = sc;
            chunk.stripe_map.stripe_unit = su;
            chunk.stripe_map.mirror_count = mc;
            chunk.stripe_map.entries = sm_entries;
        } else if (st != MDS_OK && st != MDS_ERR_NOTFOUND) {
            goto out_free;
        }
    }

    /* 3.5. Read inline data (inline regular files only). */
    chunk.has_inline_data = 0;
    if (chunk.inode.type == MDS_FTYPE_REG &&
        (chunk.inode.flags & MDS_IFLAG_INLINE)) {
        uint8_t inline_tmp[MDS_INLINE_DATA_MAX];
        uint32_t inline_len = 0;

        st = mds_cat_inline_get(cat, fileid,
                                inline_tmp, sizeof(inline_tmp),
                                &inline_len);
        if (st == MDS_OK) {
            chunk.inline_data = malloc(inline_len);
            if (chunk.inline_data == NULL) {
                st = MDS_ERR_NOMEM;
                goto out_free;
            }
            memcpy(chunk.inline_data, inline_tmp, inline_len);
            chunk.inline_data_len = inline_len;
            chunk.has_inline_data = 1;
        } else if (st != MDS_ERR_NOTFOUND) {
            goto out_free;
        }
    }

    /* 4. Collect xattr names, then read each value. */
    st = mds_cat_xattr_list(cat, fileid,
                            cat_iter_collect_xattr_name, &xc);
    if (st != MDS_OK) {
        goto out_free;
    }

    if (xc.count > 0) {
        chunk.xattrs = calloc(xc.count, sizeof(*chunk.xattrs));
        if (chunk.xattrs == NULL) {
            st = MDS_ERR_NOMEM;
            goto out_free;
        }
        chunk.xattr_count = xc.count;

        for (uint32_t i = 0; i < xc.count; i++) {
            size_t nlen = strlen(xc.names[i]);
            chunk.xattrs[i].name_len = (uint16_t)nlen;
            memcpy(chunk.xattrs[i].name, xc.names[i], nlen + 1);

            st = mds_cat_xattr_get(cat, fileid, xc.names[i],
                                   &chunk.xattrs[i].value,
                                   &chunk.xattrs[i].val_len);
            if (st != MDS_OK) {
                goto out_free;
            }
        }
    }

    /* 5. Invoke callback. */
    if (cb(&chunk, arg) != 0) {
        st = MDS_OK; /* Early stop is not an error. */
        goto out_free;
    }

    /* 6. Recurse into all children. */
    for (uint32_t i = 0; i < dc.count; i++) {
        st = cat_subtree_dfs(cat, dc.entries[i].child_fileid, cb, arg);
        if (st != MDS_OK) {
            goto out_free;
        }
    }

    st = MDS_OK;

out_free:
    for (uint32_t i = 0; i < chunk.xattr_count; i++) {
        /* NOLINTNEXTLINE(clang-analyzer-core.NullDereference) */
        free(chunk.xattrs[i].value);
    }
    free(chunk.xattrs);
    free(chunk.inline_data);
    free(chunk.stripe_map.entries);
    free(dc.entries);
    free(xc.names);
    return st;
}

enum mds_status mds_cat_subtree_iter(struct mds_catalogue *cat,
                                     uint64_t root_fileid,
                                     mds_cat_subtree_iter_cb cb,
                                     void *arg)
{
    if (cat == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat_subtree_dfs(cat, root_fileid, cb, arg);
}

/* -----------------------------------------------------------------------
 * Authority ops dispatch -- Inline data
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_inline_get(struct mds_catalogue *cat,
                                   uint64_t fileid,
                                   void *buf, uint32_t buflen,
                                   uint32_t *outlen)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->inline_get(cat, fileid, buf, buflen, outlen);
}

enum mds_status mds_cat_inline_put(struct mds_catalogue *cat,
                                   struct mds_cat_txn *txn,
                                   uint64_t fileid,
                                   const void *buf, uint32_t len)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->inline_put(cat, txn, fileid, buf, len);
}

enum mds_status mds_cat_inline_del(struct mds_catalogue *cat,
                                   struct mds_cat_txn *txn,
                                   uint64_t fileid)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->inline_del(cat, txn, fileid);
}

/* -----------------------------------------------------------------------
 * Authority ops dispatch -- Extended attributes
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_xattr_get(struct mds_catalogue *cat,
                                  uint64_t fileid, const char *name,
                                  void **val, uint32_t *vallen)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->xattr_get(cat, fileid, name, val, vallen);
}

enum mds_status mds_cat_xattr_put(struct mds_catalogue *cat,
                                  struct mds_cat_txn *txn,
                                  uint64_t fileid, const char *name,
                                  const void *val, uint32_t vallen)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->xattr_put(cat, txn, fileid, name,
                                    val, vallen);
}

enum mds_status mds_cat_xattr_del(struct mds_catalogue *cat,
                                  struct mds_cat_txn *txn,
                                  uint64_t fileid, const char *name)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->xattr_del(cat, txn, fileid, name);
}

enum mds_status mds_cat_xattr_list(struct mds_catalogue *cat,
                                   uint64_t fileid,
                                   mds_xattr_list_cb cb, void *ctx)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->xattr_list(cat, fileid, cb, ctx);
}

enum mds_status mds_cat_xattr_exists(struct mds_catalogue *cat,
                                     uint64_t fileid,
                                     const char *name)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->xattr_exists(cat, fileid, name);
}

/* -----------------------------------------------------------------------
 * Authority ops dispatch -- Stripe maps
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_stripe_map_get(struct mds_catalogue *cat,
                                       uint64_t fileid,
                                       uint32_t *stripe_count,
                                       uint32_t *stripe_unit,
                                       uint32_t *mirror_count,
                                       struct mds_ds_map_entry **entries)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->stripe_map_get(cat, fileid, stripe_count,
                                         stripe_unit, mirror_count,
                                         entries);
}

enum mds_status mds_cat_stripe_map_put(struct mds_catalogue *cat,
                                       struct mds_cat_txn *txn,
                                       uint64_t fileid,
                                       uint32_t stripe_count,
                                       uint32_t stripe_unit,
                                       uint32_t mirror_count,
                                       const struct mds_ds_map_entry *entries)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->stripe_map_put(cat, txn, fileid,
                                         stripe_count, stripe_unit,
                                         mirror_count, entries);
}

enum mds_status mds_cat_stripe_map_del(struct mds_catalogue *cat,
                                       struct mds_cat_txn *txn,
                                       uint64_t fileid)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->stripe_map_del(cat, txn, fileid);
}

enum mds_status mds_cat_stripe_map_scan(struct mds_catalogue *cat,
                                        mds_cat_stripe_map_scan_cb cb,
                                        void *ctx)
{
    if (cat == NULL || cat->auth_ops == NULL ||
        cat->auth_ops->stripe_map_scan == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->auth_ops->stripe_map_scan(cat, cb, ctx);
}

/* -----------------------------------------------------------------------
 * Authority ops dispatch -- DS registry
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_ds_get(struct mds_catalogue *cat,
                               uint32_t ds_id,
                               struct mds_ds_info *info)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ds_get(cat, ds_id, info);
}

enum mds_status mds_cat_ds_put(struct mds_catalogue *cat,
                               struct mds_cat_txn *txn,
                               const struct mds_ds_info *info)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ds_put(cat, txn, info);
}

enum mds_status mds_cat_ds_del(struct mds_catalogue *cat,
                               struct mds_cat_txn *txn,
                               uint32_t ds_id)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ds_del(cat, txn, ds_id);
}

enum mds_status mds_cat_ds_list(struct mds_catalogue *cat,
                                struct mds_ds_info **list,
                                uint32_t *count)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ds_list(cat, list, count);
}

enum mds_status mds_cat_ds_provision_get(struct mds_catalogue *cat,
                                         uint32_t ds_id,
                                         uint8_t *secret,
                                         uint32_t secret_len,
                                         uint64_t *epoch)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ds_provision_get(cat, ds_id, secret,
                                           secret_len, epoch);
}

enum mds_status mds_cat_ds_provision_put(struct mds_catalogue *cat,
                                         struct mds_cat_txn *txn,
                                         uint32_t ds_id,
                                         const uint8_t *secret,
                                         uint32_t secret_len,
                                         uint64_t epoch)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ds_provision_put(cat, txn, ds_id, secret,
                                           secret_len, epoch);
}

enum mds_status mds_cat_ds_provision_del(struct mds_catalogue *cat,
                                         struct mds_cat_txn *txn,
                                         uint32_t ds_id)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ds_provision_del(cat, txn, ds_id);
}

/* -----------------------------------------------------------------------
 * Authority ops dispatch -- Quota
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_quota_rule_get(struct mds_catalogue *cat,
                                       uint8_t scope_type,
                                       uint64_t scope_id,
                                       struct mds_quota_rule *rule)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->quota_rule_get(cat, scope_type, scope_id,
                                         rule);
}

enum mds_status mds_cat_quota_rule_put(struct mds_catalogue *cat,
                                       struct mds_cat_txn *txn,
                                       uint8_t scope_type,
                                       uint64_t scope_id,
                                       const struct mds_quota_rule *rule)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->quota_rule_put(cat, txn, scope_type,
                                         scope_id, rule);
}

enum mds_status mds_cat_quota_usage_get(struct mds_catalogue *cat,
                                        uint8_t usage_type,
                                        uint64_t scope_id,
                                        struct mds_quota_usage *usage)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->quota_usage_get(cat, usage_type, scope_id,
                                          usage);
}

enum mds_status mds_cat_quota_usage_put(struct mds_catalogue *cat,
                                        struct mds_cat_txn *txn,
                                        uint8_t usage_type,
                                        uint64_t scope_id,
                                        const struct mds_quota_usage *usage)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->quota_usage_put(cat, txn, usage_type,
                                          scope_id, usage);
}

/* -----------------------------------------------------------------------
 * Authority ops dispatch -- GC queue
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_gc_enqueue(struct mds_catalogue *cat,
                                   struct mds_cat_txn *txn,
                                   uint64_t fileid,
                                   uint32_t ds_id,
                                   const uint8_t *nfs_fh,
                                   uint32_t fh_len)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->gc_enqueue(cat, txn, fileid, ds_id,
                                     nfs_fh, fh_len);
}

enum mds_status mds_cat_gc_peek(struct mds_catalogue *cat,
                                struct mds_gc_entry *entry)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->gc_peek(cat, entry);
}

enum mds_status mds_cat_gc_peek_batch(struct mds_catalogue *cat,
                                      struct mds_gc_entry *entries,
                                      uint32_t cap,
                                      uint32_t *n_out)
{
    enum mds_status st;

    if (n_out == NULL) {
        return MDS_ERR_INVAL;
    }
    *n_out = 0;
    if (cat == NULL || cat->auth_ops == NULL ||
        entries == NULL || cap == 0) {
        return MDS_ERR_INVAL;
    }

    /* Native batch path. */
    if (cat->auth_ops->gc_peek_batch != NULL) {
        return cat->auth_ops->gc_peek_batch(cat, entries, cap, n_out);
    }

    /* Portable fallback: single-row peek.  Preserves correctness on
     * backends that have not implemented the batch slot (e.g. the
     * memdb test fixture) at the cost of degraded scan amortisation.
     * NOTFOUND on an empty queue is mapped to MDS_OK + *n_out=0 so
     * callers can use a uniform contract regardless of backend. */
    if (cat->auth_ops->gc_peek == NULL) {
        return MDS_ERR_INVAL;
    }
    st = cat->auth_ops->gc_peek(cat, &entries[0]);
    if (st == MDS_OK) {
        *n_out = 1;
        return MDS_OK;
    }
    if (st == MDS_ERR_NOTFOUND) {
        return MDS_OK;
    }
    return st;
}

enum mds_status mds_cat_gc_dequeue(struct mds_catalogue *cat,
                                   struct mds_cat_txn *txn,
                                   uint64_t gc_seq)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->gc_dequeue(cat, txn, gc_seq);
}

enum mds_status mds_cat_gc_count(struct mds_catalogue *cat,
                                 uint32_t *count)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->gc_count(cat, count);
}

/* -----------------------------------------------------------------------
 * Authority ops dispatch -- Shard routing
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_shard_fileid_get(struct mds_catalogue *cat,
                                         uint64_t fileid,
                                         uint32_t *shard_id)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->shard_fileid_get(cat, fileid, shard_id);
}

enum mds_status mds_cat_shard_fileid_put(struct mds_catalogue *cat,
                                         struct mds_cat_txn *txn,
                                         uint64_t fileid,
                                         uint32_t shard_id)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->shard_fileid_put(cat, txn, fileid, shard_id);
}

enum mds_status mds_cat_shard_fileid_del(struct mds_catalogue *cat,
                                         struct mds_cat_txn *txn,
                                         uint64_t fileid)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->shard_fileid_del(cat, txn, fileid);
}

/* -----------------------------------------------------------------------
 * Authority ops dispatch -- Cross-shard ext_dirents
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_ext_dirent_get(struct mds_catalogue *cat,
                                       uint64_t parent,
                                       const char *name,
                                       uint32_t *owner_mds_id,
                                       uint64_t *target_fileid,
                                       uint8_t *target_type,
                                       uint64_t *anchor_id)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ext_dirent_get(cat, parent, name,
                                         owner_mds_id, target_fileid,
                                         target_type, anchor_id);
}

enum mds_status mds_cat_ext_dirent_put(struct mds_catalogue *cat,
                                       struct mds_cat_txn *txn,
                                       uint64_t parent,
                                       const char *name,
                                       uint32_t owner_mds_id,
                                       uint64_t target_fileid,
                                       uint8_t target_type,
                                       uint64_t anchor_id)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ext_dirent_put(cat, txn, parent, name,
                                         owner_mds_id, target_fileid,
                                         target_type, anchor_id);
}

enum mds_status mds_cat_ext_dirent_del(struct mds_catalogue *cat,
                                       struct mds_cat_txn *txn,
                                       uint64_t parent,
                                       const char *name)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->ext_dirent_del(cat, txn, parent, name);
}

/* -----------------------------------------------------------------------
 * Authority ops dispatch -- Cross-shard link anchors
 * ----------------------------------------------------------------------- */

enum mds_status mds_cat_link_anchor_put(struct mds_catalogue *cat,
                                        struct mds_cat_txn *txn,
                                        uint64_t anchor_id,
                                        uint32_t remote_mds_id,
                                        uint64_t parent_fileid,
                                        const char *name)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->link_anchor_put(cat, txn, anchor_id,
                                          remote_mds_id,
                                          parent_fileid, name);
}

enum mds_status mds_cat_link_anchor_del(struct mds_catalogue *cat,
                                        struct mds_cat_txn *txn,
                                        uint64_t anchor_id)
{
    if (cat == NULL || cat->auth_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->auth_ops->link_anchor_del(cat, txn, anchor_id);
}

/* -----------------------------------------------------------------------
 * Coordination ops dispatch -- Shared 2PC journal
 * ----------------------------------------------------------------------- */

enum mds_status mds_coord_journal_put(
    struct mds_catalogue *cat,
    struct mds_cat_txn *txn,
    const struct mds_coord_journal_record *record)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->journal_put == NULL || record == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->journal_put(cat, txn, record);
}

enum mds_status mds_coord_journal_get(
    struct mds_catalogue *cat,
    struct mds_cat_txn *txn,
    uint64_t txn_id,
    uint8_t role,
    struct mds_coord_journal_record *record)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->journal_get == NULL || record == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->journal_get(cat, txn, txn_id, role, record);
}

enum mds_status mds_coord_journal_del(
    struct mds_catalogue *cat,
    struct mds_cat_txn *txn,
    uint64_t txn_id,
    uint8_t role)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->journal_del == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->journal_del(cat, txn, txn_id, role);
}

enum mds_status mds_coord_journal_scan(
    struct mds_catalogue *cat,
    mds_coord_journal_scan_cb cb,
    void *ctx)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->journal_scan == NULL || cb == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->journal_scan(cat, cb, ctx);
}

/* -----------------------------------------------------------------------
 * Coordination ops dispatch -- Layout state
 * ----------------------------------------------------------------------- */

enum mds_status mds_coord_layout_grant(struct mds_catalogue *cat,
                                       struct mds_cat_txn *txn,
                                       uint64_t clientid,
                                       uint64_t fileid,
                                       uint32_t iomode,
                                       uint64_t offset,
                                       uint64_t length,
                                       const struct nfs4_stateid *stateid,
                                       const uint32_t *ds_ids,
                                       uint32_t ds_count)
{
    if (cat == NULL || cat->coord_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    if (ds_count > COMMIT_OP_LAYOUT_MAX_DS ||
        (ds_count > 0 && ds_ids == NULL)) {
        return MDS_ERR_INVAL;
    }
    return cat->coord_ops->layout_grant(cat, txn, clientid, fileid,
                                        iomode, offset, length,
                                        stateid, ds_ids, ds_count);
}

enum mds_status mds_coord_layout_return(struct mds_catalogue *cat,
                                        struct mds_cat_txn *txn,
                                        const uint8_t stateid_other[12],
                                        uint64_t clientid,
                                        uint64_t fileid,
                                        const uint32_t *ds_ids,
                                        uint32_t ds_count)
{
    if (cat == NULL || cat->coord_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    if (ds_count > COMMIT_OP_LAYOUT_MAX_DS ||
        (ds_count > 0 && ds_ids == NULL)) {
        return MDS_ERR_INVAL;
    }
    return cat->coord_ops->layout_return(cat, txn, stateid_other,
                                         clientid, fileid,
                                         ds_ids, ds_count);
}

enum mds_status mds_coord_layout_get_by_stateid(
    struct mds_catalogue *cat,
    const uint8_t stateid_other[12],
    uint64_t *clientid, uint64_t *fileid,
    uint32_t *iomode, uint64_t *offset,
    uint64_t *length, uint32_t *seqid)
{
    if (cat == NULL || cat->coord_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->coord_ops->layout_get_by_stateid(cat, stateid_other,
                                                  clientid, fileid,
                                                  iomode, offset,
                                                  length, seqid);
}

enum mds_status mds_coord_layout_scan_for_file(
    struct mds_catalogue *cat,
    uint64_t fileid, bool *has_layout)
{
    if (cat == NULL || cat->coord_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->coord_ops->layout_scan_for_file(cat, fileid,
                                                 has_layout);
}

enum mds_status mds_coord_layout_del_all_for_client(
    struct mds_catalogue *cat,
    uint64_t clientid)
{
    if (cat == NULL || cat->coord_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->coord_ops->layout_del_all_for_client(cat, clientid);
}

enum mds_status mds_coord_ds_layout_idx_scan(
    struct mds_catalogue *cat, uint32_t ds_id,
    mds_coord_ds_layout_cb cb, void *ctx)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->ds_layout_idx_scan == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->ds_layout_idx_scan(cat, ds_id, cb, ctx);
}

enum mds_status mds_coord_layout_iter_file(
    struct mds_catalogue *cat, uint64_t fileid,
    mds_coord_layout_file_iter_cb cb, void *ctx)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->layout_iter_file == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->layout_iter_file(cat, fileid, cb, ctx);
}

/* -----------------------------------------------------------------------
 * Coordination ops dispatch -- Client recovery
 * ----------------------------------------------------------------------- */

enum mds_status mds_coord_recovery_put(struct mds_catalogue *cat,
                                       struct mds_cat_txn *txn,
                                       uint64_t clientid,
                                       const uint8_t *co_ownerid,
                                       uint32_t co_ownerid_len,
                                       const uint8_t verifier[8])
{
    if (cat == NULL || cat->coord_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->coord_ops->recovery_put(cat, txn, clientid,
                                        co_ownerid, co_ownerid_len,
                                        verifier);
}

enum mds_status mds_coord_recovery_del(struct mds_catalogue *cat,
                                       struct mds_cat_txn *txn,
                                       uint64_t clientid)
{
    if (cat == NULL || cat->coord_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->coord_ops->recovery_del(cat, txn, clientid);
}

enum mds_status mds_coord_recovery_get(struct mds_catalogue *cat,
                                       uint64_t clientid,
                                       uint8_t *co_ownerid,
                                       uint32_t *co_ownerid_len,
                                       uint8_t verifier[8])
{
    if (cat == NULL || cat->coord_ops == NULL) {
        return MDS_ERR_INVAL;
    }
    return cat->coord_ops->recovery_get(cat, clientid, co_ownerid,
                                        co_ownerid_len, verifier);
}

/* -----------------------------------------------------------------------
 * Coordination ops dispatch -- Phase 9D bulk operations
 *
 * These functions are not (yet) dispatched through the vtable;
 * the RonDB implementations are called directly from the cluster
 * layer.  The dispatch wrappers here are provided for subsystems
 * (e.g. session.c grace recovery) that use the public API.
 * ----------------------------------------------------------------------- */

enum mds_status mds_coord_recovery_list(
    struct mds_catalogue *cat, uint32_t owner_mds_id,
    mds_recovery_list_cb cb, void *ctx)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->recovery_list == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->recovery_list(cat, owner_mds_id, cb, ctx);
}

enum mds_status mds_coord_recovery_transfer(
    struct mds_catalogue *cat,
    uint32_t old_mds_id, uint64_t old_boot_epoch,
    uint32_t new_mds_id, uint64_t new_boot_epoch)
{
    (void)cat; (void)old_mds_id; (void)old_boot_epoch;
    (void)new_mds_id; (void)new_boot_epoch;
    return MDS_ERR_NOSUPPORT;
}

enum mds_status mds_coord_layout_scan_by_grant_owner(
    struct mds_catalogue *cat, uint32_t grant_owner_mds_id,
    mds_layout_grant_owner_cb cb, void *ctx)
{
    (void)cat; (void)grant_owner_mds_id; (void)cb; (void)ctx;
    return MDS_ERR_NOSUPPORT;
}

enum mds_status mds_coord_layout_transfer_grant_owner(
    struct mds_catalogue *cat,
    uint32_t old_mds_id, uint32_t new_mds_id)
{
    (void)cat; (void)old_mds_id; (void)new_mds_id;
    return MDS_ERR_NOSUPPORT;
}

/* -----------------------------------------------------------------------
 * Coordination ops dispatch -- Shared protocol state (shared-attr)
 * ----------------------------------------------------------------------- */

/* --- Open/share state ------------------------------------------------- */

enum mds_status mds_coord_open_put(struct mds_catalogue *cat,
    const struct mds_coord_open_row *row)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->open_put == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->open_put(cat, row);
}

enum mds_status mds_coord_open_get(struct mds_catalogue *cat,
    const uint8_t stateid_other[12],
    struct mds_coord_open_row *row)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->open_get == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->open_get(cat, stateid_other, row);
}

enum mds_status mds_coord_open_del(struct mds_catalogue *cat,
    const uint8_t stateid_other[12])
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->open_del == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->open_del(cat, stateid_other);
}

enum mds_status mds_coord_open_scan_file(struct mds_catalogue *cat,
    uint64_t fileid, mds_coord_open_scan_cb cb, void *ctx)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->open_scan_file == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->open_scan_file(cat, fileid, cb, ctx);
}

enum mds_status mds_coord_open_scan_client(struct mds_catalogue *cat,
    uint64_t clientid, mds_coord_open_scan_cb cb, void *ctx)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->open_scan_client == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->open_scan_client(cat, clientid, cb, ctx);
}

/* --- Byte-range locks ------------------------------------------------- */

enum mds_status mds_coord_lock_put(struct mds_catalogue *cat,
    const struct mds_coord_lock_row *row)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->lock_put == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->lock_put(cat, row);
}

enum mds_status mds_coord_lock_del(struct mds_catalogue *cat,
    uint64_t fileid, uint64_t lock_id)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->lock_del == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->lock_del(cat, fileid, lock_id);
}

enum mds_status mds_coord_lock_test(struct mds_catalogue *cat,
    uint64_t fileid, uint32_t lock_type,
    uint64_t offset, uint64_t length,
    uint64_t clientid, const uint8_t *owner, uint32_t owner_len,
    struct mds_coord_lock_row *conflict)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->lock_test == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->lock_test(cat, fileid, lock_type,
        offset, length, clientid, owner, owner_len, conflict);
}

enum mds_status mds_coord_lock_scan_file(struct mds_catalogue *cat,
    uint64_t fileid, mds_coord_lock_scan_cb cb, void *ctx)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->lock_scan_file == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->lock_scan_file(cat, fileid, cb, ctx);
}

enum mds_status mds_coord_lock_scan_owner(struct mds_catalogue *cat,
    uint64_t clientid, const uint8_t *owner, uint32_t owner_len,
    mds_coord_lock_scan_cb cb, void *ctx)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->lock_scan_owner == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->lock_scan_owner(cat, clientid,
        owner, owner_len, cb, ctx);
}

enum mds_status mds_coord_lock_reap_client(struct mds_catalogue *cat,
    uint64_t clientid)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->lock_reap_client == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->lock_reap_client(cat, clientid);
}

/* --- Delegations ------------------------------------------------------ */

enum mds_status mds_coord_deleg_put(struct mds_catalogue *cat,
    const struct mds_coord_deleg_row *row)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->deleg_put == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->deleg_put(cat, row);
}

enum mds_status mds_coord_deleg_get(struct mds_catalogue *cat,
    const uint8_t stateid_other[12],
    struct mds_coord_deleg_row *row)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->deleg_get == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->deleg_get(cat, stateid_other, row);
}

enum mds_status mds_coord_deleg_del(struct mds_catalogue *cat,
    const uint8_t stateid_other[12])
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->deleg_del == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->deleg_del(cat, stateid_other);
}

enum mds_status mds_coord_deleg_scan_file(struct mds_catalogue *cat,
    uint64_t fileid, mds_coord_deleg_scan_cb cb, void *ctx)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->deleg_scan_file == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->deleg_scan_file(cat, fileid, cb, ctx);
}

enum mds_status mds_coord_deleg_scan_client(struct mds_catalogue *cat,
    uint64_t clientid, mds_coord_deleg_scan_cb cb, void *ctx)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->deleg_scan_client == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->deleg_scan_client(cat, clientid, cb, ctx);
}

/* --- Client identity -------------------------------------------------- */

enum mds_status mds_coord_client_put(struct mds_catalogue *cat,
    const struct mds_coord_client_row *row)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->client_put == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->client_put(cat, row);
}

enum mds_status mds_coord_client_get(struct mds_catalogue *cat,
    uint64_t clientid, struct mds_coord_client_row *row)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->client_get == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->client_get(cat, clientid, row);
}

enum mds_status mds_coord_client_del(struct mds_catalogue *cat,
    uint64_t clientid)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->client_del == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->client_del(cat, clientid);
}

/* --- Sessions --------------------------------------------------------- */

enum mds_status mds_coord_session_put(struct mds_catalogue *cat,
    const struct mds_coord_session_row *row)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->session_put == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->session_put(cat, row);
}

enum mds_status mds_coord_session_get(struct mds_catalogue *cat,
    const uint8_t session_id[16],
    struct mds_coord_session_row *row)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->session_get == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->session_get(cat, session_id, row);
}

enum mds_status mds_coord_session_del(struct mds_catalogue *cat,
    const uint8_t session_id[16])
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->session_del == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->session_del(cat, session_id);
}

enum mds_status mds_coord_session_scan_client(struct mds_catalogue *cat,
    uint64_t clientid, mds_coord_session_scan_cb cb, void *ctx)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->session_scan_client == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->session_scan_client(cat, clientid, cb, ctx);
}

/* --- DRC slots -------------------------------------------------------- */

enum mds_status mds_coord_slot_put(struct mds_catalogue *cat,
    const uint8_t session_id[16], uint32_t slot_id,
    uint32_t seq_id, const void *cached_reply, uint32_t reply_len)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->slot_put == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->slot_put(cat, session_id, slot_id,
        seq_id, cached_reply, reply_len);
}

enum mds_status mds_coord_slot_get(struct mds_catalogue *cat,
    const uint8_t session_id[16], uint32_t slot_id,
    struct mds_coord_drc_slot_row *row)
{
    if (cat == NULL || cat->coord_ops == NULL ||
        cat->coord_ops->slot_get == NULL) {
        return MDS_ERR_NOSUPPORT;
    }
    return cat->coord_ops->slot_get(cat, session_id, slot_id, row);
}

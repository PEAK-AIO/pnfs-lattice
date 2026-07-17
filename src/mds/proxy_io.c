/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * proxy_io.c -- Non-pNFS client proxy read/write.
 *
 * Clients that do not support pNFS (LAYOUTGET fails or is not attempted)
 * fall back to READ/WRITE through the MDS.  The MDS proxies I/O to the
 * DS via NFS over its private mounts.
 *
 * Data file naming convention (architecture.md S3.4):
 *   {mount_path}/data/{fileid}_{stripe}_{mirror}
 *
 * Stripe addressing for single-stripe-unit operations:
 *   stripe_idx  = (offset / stripe_unit) % stripe_count
 *   stripe_pos  = offset / stripe_unit / stripe_count
 *   local_off   = stripe_pos * stripe_unit + (offset % stripe_unit)
 *
 * Mirror writes: the same data is written to every mirror of the
 * target stripe.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE  /* for SEEK_DATA, SEEK_HOLE, FALLOC_FL_PUNCH_HOLE */
#endif

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <linux/fs.h>    /* FICLONERANGE */
#include <inttypes.h>

#include <pthread.h>

#include "proxy_io.h"
#include "mds_catalogue.h"
#include "ds_nfs_rpc.h"
#include "mds_op_metrics.h"
#include "mds_log.h"
#include "synth_uid.h"

/* -----------------------------------------------------------------------
 * Internal types
 * ----------------------------------------------------------------------- */

/** Per-DS mount registration. */
struct ds_mount {
    uint32_t ds_id;
    char     path[MDS_MAX_PATH];
    bool     registered;
    /* DS network info for NFS3 RPC FH capture. */
    char     host[MDS_DS_HOST_MAX];
    uint16_t nfs_port;
    char     export_path[MDS_DS_EXPORT_MAX];
};

/* -----------------------------------------------------------------------
 * FD cache -- avoids open()/close() per I/O chunk
 *
 * Hash table keyed by (fileid, ds_id, stripe, mirror, flags).
 * Doubly-linked LRU list for eviction.  Thread-safe via mutex.
 *
 * Lifetime model (pin/unpin, no extra syscalls on the hot path):
 *   - fd_cache_get()        -- returns fd, pins entry (refcount++).
 *   - fd_cache_put()        -- inserts new entry already pinned (refcount=1).
 *   - fd_cache_release()    -- unpins (refcount--).  Closes+frees if the
 *                             entry was marked dead and no pins remain.
 *   - fd_cache_invalidate_* -- marks entry dead and unlinks it from the
 *                             hash/LRU; actual close is deferred to the
 *                             final release so in-flight I/O never sees
 *                             the fd closed underneath it.
 *
 * Callers MUST pair each successful get/put with exactly one release.
 * ----------------------------------------------------------------------- */

#define FD_CACHE_MAX      256
#define FD_CACHE_HASH_SZ  512  /* power-of-2 for mask */

struct fd_entry {
    int      fd;           /**< Open file descriptor (-1 = empty). */
    uint64_t fileid;
    uint32_t ds_id;
    uint32_t stripe;
    uint32_t mirror;
    int      flags;        /**< O_RDONLY or O_WRONLY|O_CREAT. */
    uint32_t refcount;     /**< Pin count; protected by fd_cache::lock. */
    bool     dead;         /**< Retired; unlinked from LRU, stays in hash
                              *  until refcount hits 0 so release can
                              *  locate the entry by fd. */
    struct fd_entry *hash_next;
    struct fd_entry *lru_prev;
    struct fd_entry *lru_next;
};

struct fd_cache {
    struct fd_entry  *hash[FD_CACHE_HASH_SZ];
    struct fd_entry  *lru_head;  /**< MRU end.  Live entries only. */
    struct fd_entry  *lru_tail;  /**< LRU end (eviction victim). */
    uint32_t          count;     /**< Live entry count (excludes dead). */
    pthread_mutex_t   lock;
};

static uint32_t fd_hash(uint64_t fileid, uint32_t ds_id,
                        uint32_t stripe, uint32_t mirror, int flags)
{
    uint64_t h = fileid;
    h ^= (uint64_t)ds_id << 16;
    h ^= (uint64_t)stripe << 8;
    h ^= (uint64_t)mirror;
    h ^= (uint64_t)(unsigned)flags << 32;
    h ^= h >> 17;
    h *= 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 31;
    return (uint32_t)(h & (FD_CACHE_HASH_SZ - 1));
}

static void fd_cache_init(struct fd_cache *c)
{
    memset(c, 0, sizeof(*c));
    pthread_mutex_init(&c->lock, NULL);
}

static void fd_cache_destroy(struct fd_cache *c)
{
    /* No callers should be active at destroy time (server shut down),
     * so refcounts are moot.  Walk the hash to catch both live entries
     * (also in LRU) and any dead-but-unreleased ones still lingering. */
    for (uint32_t b = 0; b < FD_CACHE_HASH_SZ; b++) {
        struct fd_entry *e = c->hash[b];
        while (e != NULL) {
            struct fd_entry *next = e->hash_next;
            if (e->fd >= 0) { close(e->fd); }
            free(e);
            e = next;
        }
        c->hash[b] = NULL;
    }
    c->lru_head = c->lru_tail = NULL;
    c->count = 0;
    pthread_mutex_destroy(&c->lock);
}

static void fdc_lru_unlink(struct fd_cache *c, struct fd_entry *e)
{
    if (e->lru_next) {
        e->lru_next->lru_prev = e->lru_prev;
    } else {
        c->lru_tail = e->lru_prev;
    }
    if (e->lru_prev) {
        e->lru_prev->lru_next = e->lru_next;
    } else {
        c->lru_head = e->lru_next;
    }
    e->lru_prev = e->lru_next = NULL;
}

static void fdc_lru_push_front(struct fd_cache *c, struct fd_entry *e)
{
    e->lru_prev = NULL;
    e->lru_next = c->lru_head;
    if (c->lru_head) {
        c->lru_head->lru_prev = e;
    }
    c->lru_head = e;
    if (c->lru_tail == NULL) {
        c->lru_tail = e;
    }
}

static void fdc_hash_insert(struct fd_cache *c, struct fd_entry *e)
{
    uint32_t b = fd_hash(e->fileid, e->ds_id, e->stripe, e->mirror, e->flags);
    e->hash_next = c->hash[b];
    c->hash[b] = e;
}

static void fdc_hash_remove(struct fd_cache *c, struct fd_entry *e)
{
    uint32_t b = fd_hash(e->fileid, e->ds_id, e->stripe, e->mirror, e->flags);
    struct fd_entry **pp;
    for (pp = &c->hash[b]; *pp; pp = &(*pp)->hash_next) {
        if (*pp == e) { *pp = e->hash_next; return; }
    }
}

/** Retire a live entry: unlink from LRU, mark dead.  Leaves it in the
 *  hash so an outstanding release can find it by fd.  If no pins remain,
 *  remove from hash, close, and free immediately. */
static void fdc_retire_locked(struct fd_cache *c, struct fd_entry *e)
{
    if (e->dead) { return; }
    fdc_lru_unlink(c, e);
    e->dead = true;
    c->count--;
    if (e->refcount == 0) {
        fdc_hash_remove(c, e);
        if (e->fd >= 0) { close(e->fd); }
        free(e);
    }
}

/** Look up a cached fd and pin it.  Returns fd >= 0 on hit (caller MUST
 *  call fd_cache_release) or -1 on miss.  Dead entries are skipped so
 *  lookups only return usable fds. */
static int fd_cache_get(struct fd_cache *c, uint64_t fileid,
                        uint32_t ds_id, uint32_t stripe, uint32_t mirror,
                        int flags)
{
    uint32_t b = fd_hash(fileid, ds_id, stripe, mirror, flags);
    int result = -1;

    pthread_mutex_lock(&c->lock);
    for (struct fd_entry *e = c->hash[b]; e; e = e->hash_next) {
        if (!e->dead &&
            e->fileid == fileid && e->ds_id == ds_id &&
            e->stripe == stripe && e->mirror == mirror &&
            e->flags == flags) {
            /* Promote to MRU. */
            if (c->lru_head != e) {
                fdc_lru_unlink(c, e);
                fdc_lru_push_front(c, e);
            }
            e->refcount++;
            result = e->fd;
            break;
        }
    }
    pthread_mutex_unlock(&c->lock);
    return result;
}

/** Insert an fd into the cache, pinning it for the caller.
 *  Evicts a non-pinned LRU victim if at capacity; if every entry is
 *  pinned, the cache is temporarily allowed to exceed FD_CACHE_MAX
 *  and will settle once pins drain.
 *
 *  @return 0 on success -- caller owns a pin and MUST call
 *          fd_cache_release() once I/O completes.
 *          -1 on failure -- @a fd has been closed, caller MUST NOT use it.
 */
static int fd_cache_put(struct fd_cache *c, uint64_t fileid,
                        uint32_t ds_id, uint32_t stripe, uint32_t mirror,
                        int flags, int fd)
{
    struct fd_entry *e = calloc(1, sizeof(*e));
    if (e == NULL) {
        close(fd);
        return -1;
    }
    e->fd = fd;
    e->fileid = fileid;
    e->ds_id = ds_id;
    e->stripe = stripe;
    e->mirror = mirror;
    e->flags = flags;
    e->refcount = 1;   /* Caller holds the initial pin. */
    e->dead = false;

    pthread_mutex_lock(&c->lock);

    /* Evict LRU tail if at capacity, walking back past pinned entries.
     * Pinned entries cannot be closed without risking UAF in another
     * thread that already has their fd. */
    if (c->count >= FD_CACHE_MAX) {
        struct fd_entry *victim = c->lru_tail;
        while (victim != NULL && victim->refcount > 0) {
            victim = victim->lru_prev;
        }
        if (victim != NULL) {
            fdc_retire_locked(c, victim);
        }
        /* If every live entry is pinned we over-commit briefly; the
         * excess drains via normal eviction once pins clear. */
    }

    fdc_hash_insert(c, e);
    fdc_lru_push_front(c, e);
    c->count++;

    pthread_mutex_unlock(&c->lock);
    return 0;
}

/** Release a pin taken by fd_cache_get() or fd_cache_put().
 *  If the entry was marked dead and this was the last pin, the fd is
 *  closed and the entry is freed here -- deferring the close until no
 *  thread is using the fd. */
static void fd_cache_release(struct fd_cache *c, uint64_t fileid,
                             uint32_t ds_id, uint32_t stripe,
                             uint32_t mirror, int flags, int fd)
{
    uint32_t b = fd_hash(fileid, ds_id, stripe, mirror, flags);
    struct fd_entry **pp;

    pthread_mutex_lock(&c->lock);
    /* Match on fd (unique while the entry exists) so we always touch the
     * exact entry the caller pinned, even when the key has since been
     * reinserted as a new live entry. */
    for (pp = &c->hash[b]; *pp != NULL; pp = &(*pp)->hash_next) {
        struct fd_entry *e = *pp;
        if (e->fd != fd) { continue; }
        if (e->refcount > 0) { e->refcount--; }
        if (e->dead && e->refcount == 0) {
            *pp = e->hash_next;
            if (e->fd >= 0) { close(e->fd); }
            free(e);
        }
        break;
    }
    pthread_mutex_unlock(&c->lock);
}

/** Invalidate every entry belonging to a given ds_id.
 *  Pinned entries are marked dead; their fds are closed when the last
 *  outstanding release lands. */
static void fd_cache_invalidate_ds(struct fd_cache *c, uint32_t ds_id)
{
    pthread_mutex_lock(&c->lock);
    struct fd_entry *e = c->lru_head;
    while (e != NULL) {
        struct fd_entry *next = e->lru_next;
        if (e->ds_id == ds_id) {
            fdc_retire_locked(c, e);
        }
        e = next;
    }
    pthread_mutex_unlock(&c->lock);
}

/** Invalidate a single entry after an I/O error.
 *  Takes the specific fd the caller is holding so a concurrently
 *  reinserted (live) entry with the same key is not disturbed. */
static void fd_cache_invalidate_one(struct fd_cache *c, uint64_t fileid,
                                    uint32_t ds_id, uint32_t stripe,
                                    uint32_t mirror, int flags, int fd)
{
    uint32_t b = fd_hash(fileid, ds_id, stripe, mirror, flags);

    pthread_mutex_lock(&c->lock);
    for (struct fd_entry *e = c->hash[b]; e; e = e->hash_next) {
        if (!e->dead && e->fd == fd &&
            e->fileid == fileid && e->ds_id == ds_id &&
            e->stripe == stripe && e->mirror == mirror &&
            e->flags == flags) {
            fdc_retire_locked(c, e);
            break;
        }
    }
    pthread_mutex_unlock(&c->lock);
}

/* -----------------------------------------------------------------------
 * Proxy I/O context
 * ----------------------------------------------------------------------- */

/** Proxy I/O context.
 *  mounts[] is direct-indexed by ds_id for O(1) lookup. */
struct mds_proxy_ctx {
    struct ds_mount mounts[MDS_PROXY_MAX_DS];
    struct fd_cache fdc;
    /* FH-capture validation: false (default, from calloc) = opaque
     * server FHs per RFC 8435 §2.1; true = legacy knfsd-only
     * validation (require the 0x01 version byte). */
    bool fh_knfsd_strict;
};

/* -----------------------------------------------------------------------
 * Context lifecycle
 * ----------------------------------------------------------------------- */

enum mds_status mds_proxy_ctx_create(struct mds_proxy_ctx **out)
{
    struct mds_proxy_ctx *ctx = NULL;

    if (out == NULL) {
        return MDS_ERR_INVAL;
}

    ctx = calloc(1, sizeof(*ctx));
    if (ctx == NULL) {
        return MDS_ERR_NOMEM;
    }

    fd_cache_init(&ctx->fdc);
    *out = ctx;
    return MDS_OK;
}

void mds_proxy_ctx_destroy(struct mds_proxy_ctx *ctx)
{
    if (ctx == NULL) { return; }
    fd_cache_destroy(&ctx->fdc);
    free(ctx);
}

void mds_proxy_set_fh_knfsd_strict(struct mds_proxy_ctx *ctx,
                                   bool knfsd_strict)
{
    if (ctx == NULL) {
        return;
    }
    ctx->fh_knfsd_strict = knfsd_strict;
}

/* -----------------------------------------------------------------------
 * DS mount registration
 * ----------------------------------------------------------------------- */

enum mds_status mds_proxy_mount_set(struct mds_proxy_ctx *ctx,
                                    uint32_t ds_id,
                                    const char *mount_path)
{
    if (ctx == NULL || mount_path == NULL) {
        return MDS_ERR_INVAL;
    }
    if (ds_id >= MDS_PROXY_MAX_DS) {
        return MDS_ERR_NOSPC;
    }

    ctx->mounts[ds_id].ds_id = ds_id;
    (void)snprintf(ctx->mounts[ds_id].path,
             sizeof(ctx->mounts[ds_id].path),
             "%s", mount_path);
    ctx->mounts[ds_id].registered = true;
    return MDS_OK;
}

void mds_proxy_mount_set_ds_info(struct mds_proxy_ctx *ctx,
                                 uint32_t ds_id,
                                 const char *host,
                                 uint16_t nfs_port,
                                 const char *export_path)
{
    if (ctx == NULL || ds_id >= MDS_PROXY_MAX_DS) {
        return;
    }
    if (host != NULL) {
        (void)snprintf(ctx->mounts[ds_id].host,
                 sizeof(ctx->mounts[ds_id].host), "%s", host);
    }
    ctx->mounts[ds_id].nfs_port = nfs_port;
    if (export_path != NULL) {
        (void)snprintf(ctx->mounts[ds_id].export_path,
                 sizeof(ctx->mounts[ds_id].export_path),
                 "%s", export_path);
    }
}

void mds_proxy_mount_clear(struct mds_proxy_ctx *ctx, uint32_t ds_id)
{
    if (ctx == NULL || ds_id >= MDS_PROXY_MAX_DS) {
        return;
    }
    fd_cache_invalidate_ds(&ctx->fdc, ds_id);
    ctx->mounts[ds_id].registered = false;
    ctx->mounts[ds_id].path[0] = '\0';
}

/* -----------------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------------- */

/**
 * Find mount path for a given ds_id.
 * Returns NULL if the DS is not registered.
 */
static const char *find_mount(const struct mds_proxy_ctx *ctx, uint32_t ds_id)
{
    if (ds_id >= MDS_PROXY_MAX_DS || !ctx->mounts[ds_id].registered) {
        return NULL;
    }
    return ctx->mounts[ds_id].path;
}

bool mds_proxy_is_mounted(const struct mds_proxy_ctx *ctx, uint32_t ds_id)
{
    if (ctx == NULL) {
        return false;
}
    return find_mount(ctx, ds_id) != NULL;
}


/**
 * Build the DS data file path into @buf.
 *
 * Format: {mount}/data/{fileid}_{stripe}_{mirror}
 *
 * @return 0 on success, -1 if the path would be truncated.
 */
static int build_ds_path(char *buf, size_t buf_len,
                         const char *mount_path,
                         uint64_t fileid,
                         uint32_t stripe,
                         uint32_t mirror)
{
    int n;

    n = snprintf(buf, buf_len,
                 "%s/data/%" PRIu64 "_%u_%u",
                 mount_path, fileid, stripe, mirror);
    if (n < 0 || (size_t)n >= buf_len) {
        return -1;
}
    return 0;
}

/**
 * Compute stripe addressing for a single I/O operation.
 *
 * @param offset        Logical file offset.
 * @param stripe_unit   Stripe unit in bytes.
 * @param stripe_count  Number of stripes.
 * @param stripe_idx    Receives which stripe index (0..stripe_count-1).
 * @param local_offset  Receives the DS-file-local byte offset.
 */
static void compute_stripe_addr(uint64_t offset,
                                uint32_t stripe_unit,
                                uint32_t stripe_count,
                                uint32_t *stripe_idx,
                                uint64_t *local_offset)
{
    uint64_t stripe_num;  /* Which logical stripe unit we're in. */
    uint64_t stripe_pos;  /* Position within the DS file's stripe space. */

    if (stripe_unit == 0 || stripe_count == 0) {
        *stripe_idx = 0;
        *local_offset = offset;
        return;
    }

    stripe_num = offset / stripe_unit;
    *stripe_idx = (uint32_t)(stripe_num % stripe_count);
    stripe_pos = stripe_num / stripe_count;
    *local_offset = stripe_pos * stripe_unit + (offset % stripe_unit);
}

/* -----------------------------------------------------------------------
 * Proxy READ
 * ----------------------------------------------------------------------- */

/*
 * Stripe-map lookup with v9 inline single-stripe fallback.
 *
 * Files created through the inline fast path (MDS_IFLAG_INLINE_STRIPE)
 * store their one DS entry on the inode and have NO stripe-map row, so
 * a bare mds_cat_stripe_map_get returns NOTFOUND.  The proxy I/O paths
 * are the fallback data path when a client cannot (or may not) do
 * DS-direct I/O -- e.g. LAYOUTUNAVAILABLE after a DS health event --
 * and returning NOTFOUND there surfaced to applications as ENOENT on
 * write(2) against a perfectly healthy file.  Synthesize the
 * single-entry map from the inode instead, mirroring the inline fast
 * path in op_layoutget.
 *
 * Caller frees *entries with free() in all cases, same as the raw
 * catalogue call.
 */
static enum mds_status proxy_stripe_map_get(struct mds_catalogue *cat,
                                            uint64_t fileid,
                                            uint32_t *stripe_count,
                                            uint32_t *stripe_unit,
                                            uint32_t *mirror_count,
                                            struct mds_ds_map_entry **entries)
{
    enum mds_status st;
    struct mds_inode inode;
    struct mds_ds_map_entry *e;
    uint32_t fhl;

    st = mds_cat_stripe_map_get(cat, fileid, stripe_count,
                                stripe_unit, mirror_count, entries);
    if (st != MDS_ERR_NOTFOUND) {
        return st;
    }

    st = mds_cat_ns_getattr(cat, fileid, &inode);
    if (st != MDS_OK) {
        return MDS_ERR_NOTFOUND;
    }
    if ((inode.flags & MDS_IFLAG_INLINE_STRIPE) == 0 ||
        inode.inline_fh_len == 0) {
        return MDS_ERR_NOTFOUND;
    }

    e = calloc(1, sizeof(*e));
    if (e == NULL) {
        return MDS_ERR_NOMEM;
    }
    fhl = inode.inline_fh_len;
    if (fhl > MDS_NFS_FH_MAX) {
        fhl = MDS_NFS_FH_MAX;
    }
    e[0].ds_id = inode.inline_ds_id;
    e[0].nfs_fh_len = fhl;
    memcpy(e[0].nfs_fh, inode.inline_fh, fhl);
    *stripe_count = 1;
    *mirror_count = 1;
    *stripe_unit = (inode.stripe_unit > 0) ? inode.stripe_unit : 65536;
    *entries = e;
    return MDS_OK;
}

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status mds_proxy_read(const struct mds_proxy_ctx *ctx,
                               struct mds_catalogue *cat,
                               uint64_t fileid,
                               uint64_t offset,
                               uint32_t count,
                               void *buf,
                               uint32_t *bytes_read,
                               bool *eof)
{
    struct mds_ds_map_entry *entries = NULL;
    uint32_t stripe_count, stripe_unit, mirror_count;
    enum mds_status st;
    uint32_t total_read = 0;

    if (ctx == NULL || cat == NULL || buf == NULL ||
        bytes_read == NULL || eof == NULL) {
        return MDS_ERR_INVAL;
    }

    *bytes_read = 0;
    *eof = false;

    /* Look up stripe map via catalogue vtable. */
    st = proxy_stripe_map_get(cat, fileid,
                                &stripe_count, &stripe_unit,
                                &mirror_count, &entries);
    if (st != MDS_OK) {
        return st;
    }

    MDS_PHASE_SCOPE(MDS_PHASE_DS_IO);

    /*
     * Spanning loop: iterate across stripe boundaries, reading from
     * the correct DS for each stripe unit in sequence.  Each iteration
     * reads at most one stripe_unit's worth of data.
     */
    while (total_read < count) {
        uint32_t stripe_idx;
        uint64_t local_offset;
        uint32_t chunk;
        uint32_t entry_idx;
        uint32_t m;
        bool read_ok = false;

        compute_stripe_addr(offset + total_read, stripe_unit, stripe_count,
                            &stripe_idx, &local_offset);

        /* Clamp to stripe boundary. */
        chunk = count - total_read;
        if (stripe_unit > 0) {
            uint64_t remaining = stripe_unit -
                ((offset + total_read) % stripe_unit);
            if (chunk > (uint32_t)remaining) {
                chunk = (uint32_t)remaining;
            }
        }

        /* Mirror failover: try each mirror until one succeeds. */
        for (m = 0; m < mirror_count; m++) {
            const char *mount;
            char path[MDS_MAX_PATH];
            int fd;
            ssize_t nr;

            entry_idx = stripe_idx * mirror_count + m;
            if (entry_idx >= stripe_count * mirror_count) {
                continue;
            }

            mount = find_mount(ctx, entries[entry_idx].ds_id);
            if (mount == NULL) {
                continue;
            }

            if (build_ds_path(path, sizeof(path), mount,
                              fileid, stripe_idx, m) != 0) {
                continue;
            }

            /* Try FD cache first; fall back to open().  The fd returned
             * (cache hit OR cache miss insert) is pinned until we call
             * fd_cache_release() below -- no other thread can close it
             * underneath our pread(). */
            fd = fd_cache_get(&((struct mds_proxy_ctx *)ctx)->fdc,
                              fileid, entries[entry_idx].ds_id,
                              stripe_idx, m, O_RDONLY);
            if (fd < 0) {
                fd = open(path, O_RDONLY);
                if (fd < 0) { continue; }
                if (fd_cache_put(&((struct mds_proxy_ctx *)ctx)->fdc,
                                 fileid, entries[entry_idx].ds_id,
                                 stripe_idx, m, O_RDONLY, fd) != 0) {
                    /* Insert failed -- fd is already closed by put. */
                    continue;
                }
            }

            nr = pread(fd, buf, chunk, (off_t)local_offset);

            if (nr < 0) {
                /* Stale fd -- mark dead (still pinned by us) and drop
                 * our pin.  The deferred close fires inside release
                 * because we were the last pin holder. */
                fd_cache_invalidate_one(
                    &((struct mds_proxy_ctx *)ctx)->fdc,
                    fileid, entries[entry_idx].ds_id,
                    stripe_idx, m, O_RDONLY, fd);
                fd_cache_release(&((struct mds_proxy_ctx *)ctx)->fdc,
                                 fileid, entries[entry_idx].ds_id,
                                 stripe_idx, m, O_RDONLY, fd);
                continue;
            }

            fd_cache_release(&((struct mds_proxy_ctx *)ctx)->fdc,
                             fileid, entries[entry_idx].ds_id,
                             stripe_idx, m, O_RDONLY, fd);

            /* Successful read from this mirror. */
            total_read += (uint32_t)nr;
            buf = (uint8_t *)buf + nr;
            read_ok = true;

            /* Short read means EOF on this DS file. */
            if ((uint32_t)nr < chunk) {
                *eof = true;
                goto done;
            }
            break;  /* Success -- move to next stripe. */
        }

        if (!read_ok) {
            /* All mirrors failed for this stripe. */
            if (total_read > 0) {
                goto done;  /* Return what we have so far. */
            }
            free(entries);
            return MDS_ERR_IO;
        }
    }

done:
    free(entries);
    *bytes_read = total_read;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Proxy WRITE
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
enum mds_status mds_proxy_write(const struct mds_proxy_ctx *ctx,
                                struct mds_catalogue *cat,
                                uint64_t fileid,
                                uint64_t offset,
                                const void *data,
                                uint32_t data_len,
                                uint32_t *bytes_written)
{
    struct mds_ds_map_entry *entries = NULL;
    uint32_t stripe_count, stripe_unit, mirror_count;
    enum mds_status st;
    uint32_t total_written = 0;

    if (ctx == NULL || cat == NULL || data == NULL || bytes_written == NULL) {
        return MDS_ERR_INVAL;
    }

    *bytes_written = 0;

    /* Look up stripe map via catalogue vtable. */
    st = proxy_stripe_map_get(cat, fileid,
                                &stripe_count, &stripe_unit,
                                &mirror_count, &entries);
    if (st != MDS_OK) {
        return st;
    }

    MDS_PHASE_SCOPE(MDS_PHASE_DS_IO);

    /*
     * Spanning loop: iterate across stripe boundaries, writing to
     * every mirror of each stripe unit in sequence.
     */
    while (total_written < data_len) {
        uint32_t stripe_idx;
        uint64_t local_offset;
        uint32_t chunk;
        uint32_t m;

        compute_stripe_addr(offset + total_written, stripe_unit,
                            stripe_count, &stripe_idx, &local_offset);

        /* Clamp to stripe boundary. */
        chunk = data_len - total_written;
        if (stripe_unit > 0) {
            uint64_t remaining = stripe_unit -
                ((offset + total_written) % stripe_unit);
            if (chunk > (uint32_t)remaining) {
                chunk = (uint32_t)remaining;
            }
        }

        /* Write to every mirror of this stripe. */
        for (m = 0; m < mirror_count; m++) {
            uint32_t entry_idx = stripe_idx * mirror_count + m;
            const char *mount;
            char path[MDS_MAX_PATH];
            int fd;
            ssize_t nw;

            if (entry_idx >= stripe_count * mirror_count) {
                free(entries);
                return MDS_ERR_IO;
            }

            mount = find_mount(ctx, entries[entry_idx].ds_id);
            if (mount == NULL) {
                free(entries);
                return MDS_ERR_NOTFOUND;
            }

            if (build_ds_path(path, sizeof(path), mount,
                              fileid, stripe_idx, m) != 0) {
                free(entries);
                return MDS_ERR_IO;
            }

            /* Try FD cache first; fall back to open().  Returned fd is
             * pinned until fd_cache_release() below. */
            fd = fd_cache_get(&((struct mds_proxy_ctx *)ctx)->fdc,
                              fileid, entries[entry_idx].ds_id,
                              stripe_idx, m, O_WRONLY | O_CREAT);
            if (fd < 0) {
                fd = open(path, O_WRONLY | O_CREAT, 0644);
                if (fd < 0) {
                    free(entries);
                    return MDS_ERR_IO;
                }
                if (fd_cache_put(&((struct mds_proxy_ctx *)ctx)->fdc,
                                 fileid, entries[entry_idx].ds_id,
                                 stripe_idx, m, O_WRONLY | O_CREAT,
                                 fd) != 0) {
                    /* Insert failed -- fd already closed by put. */
                    free(entries);
                    return MDS_ERR_IO;
                }
            }

            nw = pwrite(fd, (const uint8_t *)data + total_written,
                        chunk, (off_t)local_offset);

            if (nw < 0 || (uint32_t)nw != chunk) {
                fd_cache_invalidate_one(
                    &((struct mds_proxy_ctx *)ctx)->fdc,
                    fileid, entries[entry_idx].ds_id,
                    stripe_idx, m, O_WRONLY | O_CREAT, fd);
                fd_cache_release(&((struct mds_proxy_ctx *)ctx)->fdc,
                                 fileid, entries[entry_idx].ds_id,
                                 stripe_idx, m, O_WRONLY | O_CREAT, fd);
                free(entries);
                return MDS_ERR_IO;
            }

            fd_cache_release(&((struct mds_proxy_ctx *)ctx)->fdc,
                             fileid, entries[entry_idx].ds_id,
                             stripe_idx, m, O_WRONLY | O_CREAT, fd);
        }

        total_written += chunk;
    }

    free(entries);
    *bytes_written = total_written;
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * DS file creation helper
 * ----------------------------------------------------------------------- */

enum mds_status mds_proxy_ensure_ds_file(const struct mds_proxy_ctx *ctx,
                                         uint32_t ds_id,
                                         uint64_t fileid,
                                         uint32_t stripe,
                                         uint32_t mirror)
{
    const char *mount;
    char dir_path[MDS_MAX_PATH];
    char file_path[MDS_MAX_PATH];
    int fd;

    if (ctx == NULL) {
        return MDS_ERR_INVAL;
}

    MDS_PHASE_SCOPE(MDS_PHASE_DS_IO);

    mount = find_mount(ctx, ds_id);
    if (mount == NULL) {
        return MDS_ERR_NOTFOUND;
}

    /* Ensure data/ subdirectory exists. */
    (void)snprintf(dir_path, sizeof(dir_path), "%s/data", mount);
    (void)mkdir(dir_path, 0755);  /* Ignore EEXIST. */

    /* Create the data file if absent. */
    if (build_ds_path(file_path, sizeof(file_path), mount,
                      fileid, stripe, mirror) != 0) {
        return MDS_ERR_IO;
}

    fd = open(file_path, O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
        return MDS_ERR_IO;
    }

    /*
     * RFC 8435 S2.2.1: In the loosely coupled model, the MDS
     * MUST set the owner of the data file to the synthetic
     * uid/gid so that the client's AUTH_SYS credentials
     * (from ffl_user/ffl_group in the layout) pass the DS's
     * permission check.
     *
     * Mode 0666: the DS backing file must be writable by the
     * client's real AUTH_SYS uid, which for an ordinary (non-root)
     * workload is NOT root -- so the old 0600 + "rely on
     * no_root_squash" model silently blocked every non-root client
     * with EACCES.  Aligning ownership via a post-create path chown
     * is racy (the fresh file is not yet visible through the MDS's
     * NFS mount of the DS, so the chown hits ENOENT), which left the
     * file 0600 and unwritable.  In this trusted pNFS cluster the DS
     * is a backend store gated by the MDS layout grant, so a
     * permissive backing-file mode is the correct, race-free model.
     */
    (void)fchmod(fd, 0666);
    close(fd);

    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * name_to_handle_at() FH extraction (primary, ~0.01ms syscall)
 *
 * On NFS v3 mounts, the VFS file_handle contains the DS's opaque
 * server FH at a fixed offset.  This avoids the ~2-5ms NFS3 RPC.
 *
 * VFS handle layout (kernel 6.8+, NFSv3, handle_type=13):
 *   f_handle[0..11]  : VFS NFS wrapper (fs identity, opaque)
 *   f_handle[12..13] : uint16_t LE -- server FH size (e.g. 36)
 *   f_handle[14..]   : raw server FH bytes (opaque)
 * ----------------------------------------------------------------------- */

/** NFS VFS handle types for an opaque server FH.
 *
 * The NFS client's exportfs encoding has produced two type values
 * across kernel releases for the same wrapper layout: 0x0d (13,
 * observed on 6.8-era kernels) and 0x0c (12, observed on newer
 * kernels; also the historical parent-less encoding).  Both carry
 * the identical f_handle payload this extractor parses -- wrapper
 * bytes 0..11, uint16 LE server-FH size at offset 12, raw server
 * FH bytes from offset 14 -- so accept either and let the
 * structural checks below (size bounds) do the real validation. */
#define NFS_FH_HANDLE_TYPE          13
#define NFS_FH_HANDLE_TYPE_NOPARENT 12

/** Byte offset of the nfs_fh.size field within f_handle. */
#define NFS_VFS_FH_SIZE_OFFSET 12

/** Byte offset of the server FH data within f_handle. */
#define NFS_VFS_FH_DATA_OFFSET 14

int mds_proxy_extract_server_fh(const uint8_t *f_handle,
                                uint32_t handle_bytes,
                                int handle_type,
                                bool knfsd_strict,
                                uint8_t *fh_out, uint32_t fh_cap,
                                uint32_t *fh_len)
{
    uint16_t server_fh_size;

    if (f_handle == NULL || fh_out == NULL || fh_len == NULL) {
        return -1;
    }

    /* Validate handle type -- must be an NFS opaque FH (either
     * encoding; see the NFS_FH_HANDLE_TYPE* comment above). */
    if (handle_type != NFS_FH_HANDLE_TYPE &&
        handle_type != NFS_FH_HANDLE_TYPE_NOPARENT) {
        return -1;
    }

    /* Extract server FH size (uint16 LE at offset 12 in f_handle). */
    if (handle_bytes < NFS_VFS_FH_DATA_OFFSET + 1) {
        return -1;
    }
    server_fh_size = (uint16_t)f_handle[NFS_VFS_FH_SIZE_OFFSET]
                   | ((uint16_t)f_handle[NFS_VFS_FH_SIZE_OFFSET + 1] << 8);

    if (server_fh_size == 0 || server_fh_size > fh_cap ||
        (uint32_t)(NFS_VFS_FH_DATA_OFFSET + server_fh_size) >
            handle_bytes) {
        return -1;
    }

    /*
     * The server FH is OPAQUE (RFC 8435 §2.1): flex-files layouts
     * hand these bytes to the client verbatim, so the MDS must not
     * interpret them.  knfsd (0x01 version byte first), NetApp
     * ONTAP (48/56/60-byte v3 FHs leading with the n3_utility
     * version/flags byte), and any other server all pass the
     * default path.  The optional strict mode pins the legacy
     * Linux-knfsd version byte for all-knfsd deployments
     * (ds_fh_format = knfsd).
     */
    if (knfsd_strict &&
        f_handle[NFS_VFS_FH_DATA_OFFSET] != 0x01) {
        return -1;
    }

    memcpy(fh_out, &f_handle[NFS_VFS_FH_DATA_OFFSET], server_fh_size);
    *fh_len = (uint32_t)server_fh_size;
    return 0;
}

/**
 * Extract the DS server FH via name_to_handle_at().
 *
 * @param path          Local filesystem path to the DS data file.
 * @param knfsd_strict  Require the legacy knfsd 0x01 first FH byte.
 * @param fh_out        Receives raw server FH bytes.
 * @param fh_cap        Capacity of fh_out.
 * @param fh_len        Receives actual FH length.
 * @return 0 on success, -1 on failure (caller should fall back to RPC).
 */
static int proxy_name_to_handle(const char *path, bool knfsd_strict,
                                uint8_t *fh_out, uint32_t fh_cap,
                                uint32_t *fh_len)
{
    /* file_handle: handle_bytes(4) + handle_type(4) + f_handle[]. */
    uint8_t buf[sizeof(struct file_handle) + 128];
    struct file_handle *fh = (struct file_handle *)buf;
    int mount_id = 0;

    fh->handle_bytes = 128;

    if (name_to_handle_at(AT_FDCWD, path, fh, &mount_id, 0) != 0) {
        return -1;
    }

    return mds_proxy_extract_server_fh(fh->f_handle,
                                       (uint32_t)fh->handle_bytes,
                                       fh->handle_type, knfsd_strict,
                                       fh_out, fh_cap, fh_len);
}

/**
 * Ensure a DS data file exists and return its NFS file handle.
 *
 * Primary path: create the file via the local NFS mount, then
 * extract the server FH with name_to_handle_at() (~0.01ms).
 *
 * Fallback: if name_to_handle_at() fails (DS not mounted, wrong
 * kernel, handle format mismatch), fall back to NFS3 MOUNT +
 * LOOKUP RPC (~2-5ms).
 *
 * @param ctx       Proxy context.
 * @param ds_id     Data server ID.
 * @param fileid    MDS file ID.
 * @param stripe    Stripe index.
 * @param mirror    Mirror index.
 * @param fh_out    Receives NFS file handle bytes.
 * @param fh_len    In: capacity of fh_out.  Out: actual handle length.
 * @return MDS_OK on success.
 */
enum mds_status mds_proxy_ensure_ds_file_fh(
    const struct mds_proxy_ctx *ctx,
    uint32_t ds_id, uint64_t fileid,
    uint32_t stripe, uint32_t mirror,
    uint8_t *fh_out, uint32_t *fh_len)
{
    const char *mount;
    char file_path[MDS_MAX_PATH];

    if (ctx == NULL || fh_out == NULL || fh_len == NULL) {
        return MDS_ERR_INVAL;
    }
    if (ds_id >= MDS_PROXY_MAX_DS || !ctx->mounts[ds_id].registered) {
        return MDS_ERR_NOTFOUND;
    }

    MDS_PHASE_SCOPE(MDS_PHASE_DS_IO);

    mount = find_mount(ctx, ds_id);

    /*
     * Primary path: name_to_handle_at() on the local NFS mount.
     * Requires the DS to be NFS-mounted on the MDS.
     */
    if (mount != NULL) {
        char dir_path[MDS_MAX_PATH];
        int fd;
        struct timespec t0, t1, t2;

        clock_gettime(CLOCK_MONOTONIC, &t0);

        /* Ensure data/ subdirectory exists. */
        (void)snprintf(dir_path, sizeof(dir_path), "%s/data", mount);
        (void)mkdir(dir_path, 0755);

        /* Create the file if absent. */
        if (build_ds_path(file_path, sizeof(file_path), mount,
                          fileid, stripe, mirror) != 0) {
            goto fallback_rpc;
        }
        fd = open(file_path, O_WRONLY | O_CREAT, 0644);
        if (fd < 0) {
            goto fallback_rpc;
        }
        /* 0666: DS backing file must be writable by the client's
         * real (non-root) AUTH_SYS uid; see the mode rationale in
         * mds_proxy_ensure_ds_file above. */
        (void)fchmod(fd, 0666);
        close(fd);

        clock_gettime(CLOCK_MONOTONIC, &t1);

        /* Extract server FH via syscall. */
        errno = 0;
        if (proxy_name_to_handle(file_path, ctx->fh_knfsd_strict,
                                 fh_out, *fh_len, fh_len) == 0) {
            clock_gettime(CLOCK_MONOTONIC, &t2);
            {
                int64_t open_us = (t1.tv_sec - t0.tv_sec) * 1000000LL
                    + (t1.tv_nsec - t0.tv_nsec) / 1000LL;
                int64_t nth_us = (t2.tv_sec - t1.tv_sec) * 1000000LL
                    + (t2.tv_nsec - t1.tv_nsec) / 1000LL;
                MDS_LOG_DEBUG(LOG_COMP_MDS,
                    "FH_TIMING syscall: open=%" PRId64 "us nth=%" PRId64
                    "us total=%" PRId64 "us",
                    open_us, nth_us, open_us + nth_us);
            }
            return MDS_OK;
        }
        /* name_to_handle_at failed -- fall through to RPC. */
        MDS_LOG_WARN(LOG_COMP_MDS,
            "ensure_ds_file_fh: name_to_handle_at path failed "
            "(path=%s errno=%d) -- falling back to NFS3 RPC",
            file_path, errno);
    }

fallback_rpc:
    /* Fallback: NFS3 MOUNT + LOOKUP RPC to the DS. */
    {
        char rel_path[256];
        const char *host = ctx->mounts[ds_id].host;
        uint16_t nfs_port = ctx->mounts[ds_id].nfs_port;
        const char *export_path = ctx->mounts[ds_id].export_path;
        struct timespec tr0, tr1;

        if (host[0] == '\0' || export_path[0] == '\0' || nfs_port == 0) {
            MDS_LOG_WARN(LOG_COMP_MDS,
                "ensure_ds_file_fh: NFS3 RPC fallback unavailable "
                "(ds=%u host='%s' export='%s' port=%u)",
                (unsigned)ds_id, host, export_path,
                (unsigned)nfs_port);
            return MDS_ERR_INVAL;
        }

        (void)snprintf(rel_path, sizeof(rel_path),
                       "data/%" PRIu64 "_%u_%u", fileid, stripe, mirror);

        clock_gettime(CLOCK_MONOTONIC, &tr0);
        if (ds_nfs3_lookup_fh(host, nfs_port, export_path,
                              rel_path, 1, fh_out, fh_len, 3000) != 0) {
            MDS_LOG_WARN(LOG_COMP_MDS,
                "ensure_ds_file_fh: NFS3 RPC FH lookup failed "
                "(ds=%u host=%s export=%s rel=%s)",
                (unsigned)ds_id, host, export_path, rel_path);
            return MDS_ERR_IO;
        }
        clock_gettime(CLOCK_MONOTONIC, &tr1);
        {
            int64_t rpc_us = (tr1.tv_sec - tr0.tv_sec) * 1000000LL
                + (tr1.tv_nsec - tr0.tv_nsec) / 1000LL;
            MDS_LOG_DEBUG(LOG_COMP_MDS,
                "FH_TIMING rpc_fallback: %" PRId64 "us", rpc_us);
        }
        return MDS_OK;
    }
}

enum mds_status mds_proxy_write_direct(const struct mds_proxy_ctx *ctx,
                                        uint32_t ds_id,
                                        uint64_t fileid,
                                        uint32_t stripe,
                                        uint32_t mirror,
                                        uint64_t offset,
                                        const void *data,
                                        uint32_t data_len)
{
    const char *mount;
    char file_path[MDS_MAX_PATH];
    int fd;
    ssize_t nw;

    if (ctx == NULL || (data == NULL && data_len > 0)) {
        return MDS_ERR_INVAL;
}

    mount = find_mount(ctx, ds_id);
    if (mount == NULL) {
        return MDS_ERR_NOTFOUND;
}

    if (build_ds_path(file_path, sizeof(file_path), mount,
                      fileid, stripe, mirror) != 0) {
        return MDS_ERR_IO;
}

    fd = open(file_path, O_WRONLY);
    if (fd < 0) {
        return MDS_ERR_IO;
}

    nw = pwrite(fd, data, data_len, (off_t)offset);
    close(fd);

    return (nw == (ssize_t)data_len) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * NFSv4.2 proxy helpers -- shared stripe resolver
 *
 * Opens the DS data file for a given fileid at stripe 0, mirror 0
 * and returns the fd.  Caller must close() when done.
 * ----------------------------------------------------------------------- */

/**
 * Open the DS data file for a given fileid+offset.
 *
 * If @a pre_entries is non-NULL, uses those instead of fetching from
 * the catalogue (avoids redundant RonDB round-trip).
 */
static int open_ds_file_ex(const struct mds_proxy_ctx *ctx,
                           struct mds_catalogue *cat,
                           uint64_t fileid, uint64_t offset,
                           int open_flags,
                           const struct mds_ds_map_entry *pre_entries,
                           uint32_t pre_sc, uint32_t pre_su, uint32_t pre_mc,
                           uint32_t *out_stripe_idx,
                           uint64_t *out_local_offset)
{
    struct mds_ds_map_entry *fetched = NULL;
    const struct mds_ds_map_entry *entries;
    uint32_t stripe_count, stripe_unit, mirror_count;
    uint32_t stripe_idx, entry_idx;
    uint64_t local_offset;
    const char *mount;
    char path[MDS_MAX_PATH];
    int fd;

    if (pre_entries != NULL) {
        entries = pre_entries;
        stripe_count = pre_sc;
        stripe_unit = pre_su;
        mirror_count = pre_mc;
    } else {
        enum mds_status st = proxy_stripe_map_get(
            cat, fileid, &stripe_count, &stripe_unit,
            &mirror_count, &fetched);
        if (st != MDS_OK) { return -1; }
        entries = fetched;
    }

    compute_stripe_addr(offset, stripe_unit, stripe_count,
                        &stripe_idx, &local_offset);

    entry_idx = stripe_idx * mirror_count;
    if (entry_idx >= stripe_count * mirror_count) {
        free(fetched);
        return -1;
    }

    mount = find_mount(ctx, entries[entry_idx].ds_id);
    if (mount == NULL) {
        free(fetched);
        return -1;
    }

    if (build_ds_path(path, sizeof(path), mount,
                      fileid, stripe_idx, 0) != 0) {
        free(fetched);
        return -1;
    }

    free(fetched);

    fd = open(path, open_flags, 0644);
    if (fd < 0) { return -1; }

    if (out_stripe_idx != NULL) {
        *out_stripe_idx = stripe_idx;
    }
    if (out_local_offset != NULL) {
        *out_local_offset = local_offset;
    }
    return fd;
}

/** Legacy wrapper -- fetches stripe map from catalogue. */
static int open_ds_file(const struct mds_proxy_ctx *ctx,
                        struct mds_catalogue *cat,
                        uint64_t fileid, uint64_t offset,
                        int open_flags,
                        uint32_t *out_stripe_idx,
                        uint64_t *out_local_offset)
{
    return open_ds_file_ex(ctx, cat, fileid, offset, open_flags,
                           NULL, 0, 0, 0,
                           out_stripe_idx, out_local_offset);
}

/* -----------------------------------------------------------------------
 * Proxy ALLOCATE
 * ----------------------------------------------------------------------- */

enum mds_status mds_proxy_allocate(const struct mds_proxy_ctx *ctx,
                                   struct mds_catalogue *cat,
                                   uint64_t fileid,
                                   uint64_t offset,
                                   uint64_t length)
{
    struct mds_ds_map_entry *entries = NULL;
    uint32_t stripe_count, stripe_unit, mirror_count;
    uint32_t stripe_idx, m;
    uint64_t local_offset;
    enum mds_status st;

    if (ctx == NULL || cat == NULL) {
        return MDS_ERR_INVAL;
}

    st = proxy_stripe_map_get(cat, fileid,
                                 &stripe_count, &stripe_unit,
                                 &mirror_count, &entries);
    if (st != MDS_OK) {
        return st;
}

    compute_stripe_addr(offset, stripe_unit, stripe_count,
                        &stripe_idx, &local_offset);

    /* Apply fallocate to every mirror of the target stripe. */
    for (m = 0; m < mirror_count; m++) {
        uint32_t entry_idx = stripe_idx * mirror_count + m;
        const char *mount;
        char path[MDS_MAX_PATH];
        int fd, rc;

        if (entry_idx >= stripe_count * mirror_count) {
            free(entries);
            return MDS_ERR_IO;
        }

        mount = find_mount(ctx, entries[entry_idx].ds_id);
        if (mount == NULL) {
            free(entries);
            return MDS_ERR_NOTFOUND;
        }

        if (build_ds_path(path, sizeof(path), mount,
                          fileid, stripe_idx, m) != 0) {
            free(entries);
            return MDS_ERR_IO;
        }

        fd = open(path, O_WRONLY | O_CREAT, 0644);
        if (fd < 0) {
            free(entries);
            return MDS_ERR_IO;
        }

        rc = posix_fallocate(fd, (off_t)local_offset, (off_t)length);
        close(fd);

        if (rc != 0) {
            free(entries);
            return (rc == ENOSPC) ? MDS_ERR_NOSPC : MDS_ERR_IO;
        }
    }

    free(entries);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Proxy DEALLOCATE
 * ----------------------------------------------------------------------- */

enum mds_status mds_proxy_deallocate(const struct mds_proxy_ctx *ctx,
                                     struct mds_catalogue *cat,
                                     uint64_t fileid,
                                     uint64_t offset,
                                     uint64_t length)
{
    struct mds_ds_map_entry *entries = NULL;
    uint32_t stripe_count, stripe_unit, mirror_count;
    uint32_t stripe_idx, m;
    uint64_t local_offset;
    enum mds_status st;

    if (ctx == NULL || cat == NULL) {
        return MDS_ERR_INVAL;
}

    st = proxy_stripe_map_get(cat, fileid,
                                 &stripe_count, &stripe_unit,
                                 &mirror_count, &entries);
    if (st != MDS_OK) {
        return st;
}

    compute_stripe_addr(offset, stripe_unit, stripe_count,
                        &stripe_idx, &local_offset);

    /* Punch hole on every mirror of the target stripe. */
    for (m = 0; m < mirror_count; m++) {
        uint32_t entry_idx = stripe_idx * mirror_count + m;
        const char *mount;
        char path[MDS_MAX_PATH];
        int fd, rc;

        if (entry_idx >= stripe_count * mirror_count) {
            free(entries);
            return MDS_ERR_IO;
        }

        mount = find_mount(ctx, entries[entry_idx].ds_id);
        if (mount == NULL) {
            free(entries);
            return MDS_ERR_NOTFOUND;
        }

        if (build_ds_path(path, sizeof(path), mount,
                          fileid, stripe_idx, m) != 0) {
            free(entries);
            return MDS_ERR_IO;
        }

        fd = open(path, O_WRONLY, 0644);
        if (fd < 0) {
            free(entries);
            return MDS_ERR_IO;
        }

        rc = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                       (off_t)local_offset, (off_t)length);
        close(fd);

        if (rc != 0) {
            free(entries);
            return MDS_ERR_IO;
        }
    }

    free(entries);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Proxy SEEK
 * ----------------------------------------------------------------------- */

enum mds_status mds_proxy_seek(const struct mds_proxy_ctx *ctx,
                               struct mds_catalogue *cat,
                               uint64_t fileid,
                               uint64_t offset,
                               uint32_t what,
                               uint64_t *out_offset,
                               bool *eof)
{
    uint64_t local_offset;
    int fd;
    off_t result;
    int whence;

    if (ctx == NULL || cat == NULL || out_offset == NULL || eof == NULL) {
        return MDS_ERR_INVAL;
}

    *eof = false;

    fd = open_ds_file(ctx, cat, fileid, offset, O_RDONLY, NULL, &local_offset);
    if (fd < 0) {
        return MDS_ERR_IO;
}

    whence = (what == 0) ? SEEK_DATA : SEEK_HOLE;
    result = lseek(fd, (off_t)local_offset, whence);

    if (result < 0) {
        if (errno == ENXIO) {
            /* Beyond end of data / no more holes -- report EOF. */
            *eof = true;
            *out_offset = 0;
            close(fd);
            return MDS_OK;
        }
        close(fd);
        return MDS_ERR_IO;
    }

    close(fd);

    /*
     * Translate DS-local offset back to logical offset.
     * For single-stripe files (the common fast path), local == logical.
     * For multi-stripe, we'd need the inverse of compute_stripe_addr.
     * For now, we use the delta from the requested offset.
     */
    *out_offset = offset + ((uint64_t)result - local_offset);
    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Proxy COPY (synchronous, chunked)
 * ----------------------------------------------------------------------- */

enum mds_status mds_proxy_copy_data(const struct mds_proxy_ctx *ctx,
                                    struct mds_catalogue *cat,
                                    uint64_t src_fileid,
                                    uint64_t src_offset,
                                    uint64_t dst_fileid,
                                    uint64_t dst_offset,
                                    uint64_t count,
                                    uint64_t *bytes_copied)
{
    uint8_t *buf = NULL;
    uint64_t total = 0;
    enum mds_status st = MDS_OK;
    static const uint32_t chunk = 1024 * 1024; /* 1 MiB */

    if (ctx == NULL || cat == NULL || bytes_copied == NULL) {
        return MDS_ERR_INVAL;
}

    *bytes_copied = 0;

    buf = malloc(chunk);
    if (buf == NULL) {
        return MDS_ERR_NOMEM;
}

    while (total < count) {
        uint32_t want = chunk;
        uint32_t nr = 0;
        uint32_t nw = 0;
        bool eof_flag = false;

        if (count - total < want) {
            want = (uint32_t)(count - total);
}

        st = mds_proxy_read(ctx, cat, src_fileid,
                            src_offset + total, want,
                            buf, &nr, &eof_flag);
        if (st != MDS_OK) {
            break;
}
        if (nr == 0) {
            break;
}

        st = mds_proxy_write(ctx, cat, dst_fileid,
                             dst_offset + total,
                             buf, nr, &nw);
        if (st != MDS_OK) {
            break;
}

        total += nw;
        if (eof_flag) {
            break;
}
    }

    free(buf);
    *bytes_copied = total;
    return st;
}

/* -----------------------------------------------------------------------
 * Proxy WRITE_SAME
 * ----------------------------------------------------------------------- */

enum mds_status mds_proxy_write_same(const struct mds_proxy_ctx *ctx,
                                     struct mds_catalogue *cat,
                                     uint64_t fileid,
                                     uint64_t offset,
                                     uint64_t length,
                                     const void *pattern,
                                     uint32_t pattern_len)
{
    uint64_t written = 0;
    /* cppcheck-suppress variableScope */
    /* cppcheck-suppress variableScope */
    enum mds_status st;

    if (ctx == NULL || cat == NULL || pattern == NULL || pattern_len == 0) {
        return MDS_ERR_INVAL;
}

    while (written < length) {
        uint32_t want = pattern_len;
        uint32_t nw = 0;

        if (length - written < want) {
            want = (uint32_t)(length - written);
}

        st = mds_proxy_write(ctx, cat, fileid,
                             offset + written,
                             pattern, want, &nw);
        if (st != MDS_OK) {
            return st;
}
        written += nw;
    }

    return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Proxy CLONE (FICLONERANGE with copy fallback)
 * ----------------------------------------------------------------------- */

/* cppcheck-suppress constParameterPointer */
enum mds_status mds_proxy_clone_range(struct mds_proxy_ctx *ctx,
/* cppcheck-suppress constParameterPointer */
                                      struct mds_catalogue *cat,
                                      uint64_t src_fileid,
                                      uint64_t src_offset,
                                      uint64_t dst_fileid,
                                      uint64_t dst_offset,
                                      uint64_t count)
{
    uint64_t src_local, dst_local;
    int src_fd = -1;
    int dst_fd = -1;
    int rc;

    if (ctx == NULL || cat == NULL) {
        return MDS_ERR_INVAL;
}

    src_fd = open_ds_file(ctx, cat, src_fileid, src_offset,
                          O_RDONLY, NULL, &src_local);
    if (src_fd < 0) {
        return MDS_ERR_IO;
}

    dst_fd = open_ds_file(ctx, cat, dst_fileid, dst_offset,
                          O_WRONLY, NULL, &dst_local);
    if (dst_fd < 0) {
        close(src_fd);
        return MDS_ERR_IO;
    }

    /* Try reflink first. */
#ifdef FICLONERANGE
    {
        struct file_clone_range fcr;

        memset(&fcr, 0, sizeof(fcr));
        fcr.src_fd = src_fd;
        fcr.src_offset = src_local;
        fcr.src_length = count;
        fcr.dest_offset = dst_local;
        rc = ioctl(dst_fd, FICLONERANGE, &fcr);
    }
#else
    rc = -1;
    errno = EOPNOTSUPP;
#endif

    close(src_fd);
    close(dst_fd);

    /* cppcheck-suppress knownConditionTrueFalse */
    if (rc == 0) {
    /* cppcheck-suppress knownConditionTrueFalse */
        return MDS_OK;
}

    /* Only fall back to data copy for unsupported-reflink errors.
     * Real I/O errors (ENOSPC, EIO, etc.) must propagate. */
    if (errno != EOPNOTSUPP && errno != ENOTSUP && errno != EXDEV) {
        return (errno == ENOSPC) ? MDS_ERR_NOSPC : MDS_ERR_IO;
}

    {
        uint64_t copied = 0;

        return mds_proxy_copy_data(ctx, cat,
                                   src_fileid, src_offset,
                                   dst_fileid, dst_offset,
                                   count, &copied);
    }
}

/* -----------------------------------------------------------------------
 * Direct DS file operations for resilver (no stripe-map lookup)
 * ----------------------------------------------------------------------- */

enum mds_status mds_proxy_read_direct(const struct mds_proxy_ctx *ctx,
                                       uint32_t ds_id,
                                       uint64_t fileid,
                                       uint32_t stripe,
                                       uint32_t mirror,
                                       uint64_t offset,
                                       void *buf,
                                       uint32_t count,
                                       uint32_t *bytes_read,
                                       bool *eof)
{
    const char *mount;
    char file_path[MDS_MAX_PATH];
    int fd;
    ssize_t nr;

    if (ctx == NULL || buf == NULL || bytes_read == NULL || eof == NULL) {
        return MDS_ERR_INVAL;
}

    *bytes_read = 0;
    *eof = false;

    mount = find_mount(ctx, ds_id);
    if (mount == NULL) {
        return MDS_ERR_NOTFOUND;
}

    if (build_ds_path(file_path, sizeof(file_path), mount,
                      fileid, stripe, mirror) != 0) {
        return MDS_ERR_IO;
}

    fd = open(file_path, O_RDONLY);
    if (fd < 0) {
        return (errno == ENOENT) ? MDS_ERR_NOTFOUND : MDS_ERR_IO;
}

    nr = pread(fd, buf, count, (off_t)offset);
    if (nr < 0) {
        close(fd);
        return MDS_ERR_IO;
    }

    *bytes_read = (uint32_t)nr;
    if ((uint32_t)nr < count) {
        *eof = true;
}

    close(fd);
    return MDS_OK;
}

enum mds_status mds_proxy_seek_direct(const struct mds_proxy_ctx *ctx,
                                       uint32_t ds_id,
                                       uint64_t fileid,
                                       uint32_t stripe,
                                       uint32_t mirror,
                                       uint64_t offset,
                                       uint32_t what,
                                       uint64_t *out_offset,
                                       bool *eof)
{
    const char *mount;
    char file_path[MDS_MAX_PATH];
    int fd, whence;
    off_t result;

    if (ctx == NULL || out_offset == NULL || eof == NULL) {
        return MDS_ERR_INVAL;
}

    *eof = false;

    mount = find_mount(ctx, ds_id);
    if (mount == NULL) {
        return MDS_ERR_NOTFOUND;
}

    if (build_ds_path(file_path, sizeof(file_path), mount,
                      fileid, stripe, mirror) != 0) {
        return MDS_ERR_IO;
}

    fd = open(file_path, O_RDONLY);
    if (fd < 0) {
        return (errno == ENOENT) ? MDS_ERR_NOTFOUND : MDS_ERR_IO;
}

    whence = (what == 0) ? SEEK_DATA : SEEK_HOLE;
    result = lseek(fd, (off_t)offset, whence);
    if (result < 0) {
        if (errno == ENXIO) {
            /* past end -- no more data/holes */
            *eof = true;
            close(fd);
            return MDS_OK;
        }
        close(fd);
        return MDS_ERR_IO;
    }

    *out_offset = (uint64_t)result;
    close(fd);
    return MDS_OK;
}

enum mds_status mds_proxy_copy_direct(const struct mds_proxy_ctx *ctx,
                                      uint32_t src_ds_id,
                                      uint32_t dst_ds_id,
                                      uint64_t fileid,
                                      uint32_t stripe,
                                      uint32_t mirror,
                                      uint64_t size,
                                      uint64_t *bytes_copied)
{
    const char *src_mount;
    const char *dst_mount;
    char src_path[MDS_MAX_PATH];
    char dst_path[MDS_MAX_PATH];
    int src_fd = -1;
    int dst_fd = -1;
    uint64_t total = 0;

    if (ctx == NULL || bytes_copied == NULL) {
        return MDS_ERR_INVAL;
    }
    *bytes_copied = 0;

    src_mount = find_mount(ctx, src_ds_id);
    dst_mount = find_mount(ctx, dst_ds_id);
    if (src_mount == NULL || dst_mount == NULL) {
        return MDS_ERR_NOTFOUND;
    }
    if (build_ds_path(src_path, sizeof(src_path), src_mount,
                      fileid, stripe, mirror) != 0 ||
        build_ds_path(dst_path, sizeof(dst_path), dst_mount,
                      fileid, stripe, mirror) != 0) {
        return MDS_ERR_IO;
    }

    src_fd = open(src_path, O_RDONLY);
    if (src_fd < 0) {
        return (errno == ENOENT) ? MDS_ERR_NOTFOUND : MDS_ERR_IO;
    }
    dst_fd = open(dst_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dst_fd < 0) {
        close(src_fd);
        return MDS_ERR_IO;
    }

    /* copy_file_range: if both files are on NFSv4.2 mounts, the
     * kernel NFS client may delegate the copy to the server(s),
     * avoiding data transfer through the MDS entirely.  On older
     * kernels or non-NFS filesystems, the kernel performs a
     * splice-based copy (still zero-copy in user-space). */
    while (total < size) {
        loff_t off_in  = (loff_t)total;
        loff_t off_out = (loff_t)total;
        size_t chunk = size - total;
        /* NOLINTNEXTLINE(bugprone-implicit-widening-of-multiplication-result) */
        if (chunk > (size_t)(1024 * 1024)) {
            chunk = (size_t)1024 * 1024; /* 1 MiB per call */
        }

        ssize_t rc = copy_file_range(src_fd, &off_in,
                                     dst_fd, &off_out,
                                     chunk, 0);
        if (rc < 0) {
            close(src_fd);
            close(dst_fd);
            return MDS_ERR_IO;
        }
        if (rc == 0) {
            break; /* EOF */
        }
        total += (uint64_t)rc;
    }

    close(src_fd);
    close(dst_fd);
    *bytes_copied = total;
    return MDS_OK;
}

enum mds_status mds_proxy_truncate_ds_file(const struct mds_proxy_ctx *ctx,
                                            uint32_t ds_id,
                                            uint64_t fileid,
                                            uint32_t stripe,
                                            uint32_t mirror)
{
    const char *mount;
    char file_path[MDS_MAX_PATH];
    int rc;

    if (ctx == NULL) {
        return MDS_ERR_INVAL;
}

    MDS_PHASE_SCOPE(MDS_PHASE_DS_IO);

    mount = find_mount(ctx, ds_id);
    if (mount == NULL) {
        return MDS_ERR_NOTFOUND;
}

    if (build_ds_path(file_path, sizeof(file_path), mount,
                      fileid, stripe, mirror) != 0) {
        return MDS_ERR_IO;
}

    rc = truncate(file_path, 0);
    if (rc != 0) {
        return (errno == ENOENT) ? MDS_ERR_NOTFOUND : MDS_ERR_IO;
}

    return MDS_OK;
}

enum mds_status mds_proxy_unlink_ds_file(const struct mds_proxy_ctx *ctx,
                                         uint32_t ds_id,
                                         uint64_t fileid,
                                         uint32_t stripe,
                                         uint32_t mirror,
                                         bool *existed)
{
    const char *mount;
    char file_path[MDS_MAX_PATH];

    if (existed != NULL) {
        *existed = false;
    }

    if (ctx == NULL) {
        return MDS_ERR_INVAL;
    }

    MDS_PHASE_SCOPE(MDS_PHASE_DS_IO);

    mount = find_mount(ctx, ds_id);
    if (mount == NULL) {
        /* DS isn't NFS-mounted on this MDS host -- the GC worker
         * cannot reach the file.  Surface a distinct status so
         * the caller can decide whether to retry later (DS may
         * come online) or treat this entry as unreachable. */
        return MDS_ERR_NOTFOUND;
    }

    if (build_ds_path(file_path, sizeof(file_path), mount,
                      fileid, stripe, mirror) != 0) {
        return MDS_ERR_IO;
    }

    if (unlink(file_path) != 0) {
        /* ENOENT is benign -- the GC entry refers to a file that
         * was already removed, possibly by a previous worker run
         * or by an operator wiping /export/ds/data.  Treat as
         * success so the caller dequeues, but signal
         * existed=false so the caller can short-circuit further
         * (s, m) probes. */
        if (errno == ENOENT) {
            return MDS_OK;
        }
        return MDS_ERR_IO;
    }

    if (existed != NULL) {
        *existed = true;
    }
    return MDS_OK;
}

/* RFC 8435 S14: Fencing uid/gid -- revokes client access to DS file. */
#define MDS_FENCE_UID  1
#define MDS_FENCE_GID  1

enum mds_status mds_proxy_fence_ds_file(const struct mds_proxy_ctx *ctx,
                                         uint32_t ds_id, uint64_t fileid,
                                         uint32_t stripe, uint32_t mirror)
{
    const char *mount;
    char file_path[MDS_MAX_PATH];

    if (ctx == NULL) {
        return MDS_ERR_INVAL;
    }

    MDS_PHASE_SCOPE(MDS_PHASE_DS_IO);

    mount = find_mount(ctx, ds_id);
    if (mount == NULL) {
        return MDS_ERR_NOTFOUND;
    }

    if (build_ds_path(file_path, sizeof(file_path), mount,
                      fileid, stripe, mirror) != 0) {
        return MDS_ERR_IO;
    }

    /* Revoke access by changing owner to fencing uid.
     * The revoked client's synthetic credentials no longer match. */
    if (chown(file_path, (uid_t)MDS_FENCE_UID,
              (gid_t)MDS_FENCE_GID) != 0) {
        return MDS_ERR_IO;
    }

    return MDS_OK;
}

enum mds_status mds_proxy_set_ds_owner_explicit(
    const struct mds_proxy_ctx *ctx,
    uint32_t ds_id, uint64_t fileid,
    uint32_t stripe, uint32_t mirror,
    uid_t uid, gid_t gid)
{
    const char *mount;
    char file_path[MDS_MAX_PATH];

    if (ctx == NULL) {
        return MDS_ERR_INVAL;
    }

    MDS_PHASE_SCOPE(MDS_PHASE_DS_IO);

    mount = find_mount(ctx, ds_id);
    if (mount == NULL) {
        return MDS_ERR_NOTFOUND;
    }

    if (build_ds_path(file_path, sizeof(file_path), mount,
                      fileid, stripe, mirror) != 0) {
        return MDS_ERR_IO;
    }

    /*
     * Loosely coupled, generic-DS chown: align the DS backing file's
     * owner with the (uid, gid) the next LAYOUTGET will advertise in
     * ffl_user / ffl_group, so the DS's AUTH_SYS permission check
     * accepts the client's READ/WRITE.  Caller is responsible for
     * passing the same values it will put on the wire; see
     * compound_layout.c::op_layoutget().
     */
    if (chown(file_path, uid, gid) != 0) {
        return MDS_ERR_IO;
    }

    return MDS_OK;
}

/*
 * RFC 8435 §2.2.1: HMAC-derived synthetic-owner chown.
 *
 * Derives uid from HMAC-SHA256(secret, fileid||stripe||mirror) and
 * chowns the DS data file.  The uid is clamped to [0x40000000,
 * 0x7FFFFFFF] so it cannot collide with real local users.
 * gid is set to 0 (synthetic isolation is uid-only; gid namespace
 * is less sensitive per the RFC).
 */
enum mds_status mds_proxy_set_ds_owner(
    const struct mds_proxy_ctx *ctx,
    uint32_t ds_id, uint64_t fileid,
    uint32_t stripe, uint32_t mirror,
    const uint8_t *secret, uint32_t secret_len)
{
    const char *mount;
    char file_path[MDS_MAX_PATH];
    uint32_t synth_uid;

    if (ctx == NULL || secret == NULL || secret_len == 0) {
        return MDS_ERR_INVAL;
    }

    MDS_PHASE_SCOPE(MDS_PHASE_DS_IO);

    mount = find_mount(ctx, ds_id);
    if (mount == NULL) {
        return MDS_ERR_NOTFOUND;
    }

    if (build_ds_path(file_path, sizeof(file_path), mount,
                      fileid, stripe, mirror) != 0) {
        return MDS_ERR_IO;
    }

    synth_uid = synth_uid_from_secret(secret, secret_len,
                                      fileid, stripe, mirror);

    if (chown(file_path, (uid_t)synth_uid, (gid_t)0) != 0) {
        return MDS_ERR_IO;
    }

    return MDS_OK;
}

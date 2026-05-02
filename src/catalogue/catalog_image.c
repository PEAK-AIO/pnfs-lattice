/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalog_image.c — Materialized in-memory catalog image.
 *
 * Hash maps for inodes (fileid) and dirents (parent+name).
 * Per-directory sorted name arrays for READDIR pagination.
 * Replay applier for semantic journal records.
 */

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>

#include "catalog_image.h"
#include "catalog_delta.h"
#include "endian_helpers.h"

/* -----------------------------------------------------------------------
 * Hash helpers
 * ----------------------------------------------------------------------- */

#define INODE_INIT_CAP   1024
#define DIRENT_INIT_CAP  2048
#define LOAD_FACTOR_NUM  3
#define LOAD_FACTOR_DEN  4
#define MAX_STREAMS      16
#define INODE_UPSERT_PAYLOAD_SIZE 133U

static uint64_t hash_u64(uint64_t v)
{
    v ^= v >> 33;
    v *= 0xff51afd7ed558ccdULL;
    v ^= v >> 33;
    v *= 0xc4ceb9fe1a85ec53ULL;
    v ^= v >> 33;
    return v;
}

static uint64_t hash_dirent_key(uint64_t parent, const char *name)
{
    uint64_t h = hash_u64(parent);
    const unsigned char *p = (const unsigned char *)name;

    while (*p != '\0') {
        h = h * 31 + *p++;
    }
    return h;
}

/* -----------------------------------------------------------------------
 * Inode hash table
 * ----------------------------------------------------------------------- */

struct inode_slot {
    uint64_t         fileid;    /* 0 = empty */
    struct mds_inode inode;
};

struct inode_map {
    struct inode_slot *slots;
    uint32_t           cap;
    uint32_t           count;
};

static int inode_map_init(struct inode_map *m, uint32_t cap)
{
    m->slots = calloc(cap, sizeof(*m->slots));
    if (m->slots == NULL) {
        return -1;
    }
    m->cap = cap;
    m->count = 0;
    return 0;
}

static void inode_map_free(struct inode_map *m)
{
    free(m->slots);
    m->slots = NULL;
    m->cap = 0;
    m->count = 0;
}

static struct inode_slot *inode_map_find(const struct inode_map *m,
                                         uint64_t fileid)
{
    uint32_t idx = (uint32_t)(hash_u64(fileid) % m->cap);
    uint32_t i;

    for (i = 0; i < m->cap; i++) {
        struct inode_slot *s = &m->slots[(idx + i) % m->cap];
        if (s->fileid == 0) {
            return NULL;
        }
        if (s->fileid == fileid) {
            return s;
        }
    }
    return NULL;
}

static int inode_map_grow(struct inode_map *m);

static int inode_map_put(struct inode_map *m, const struct mds_inode *inode)
{
    uint32_t idx;
    uint32_t i;

    if (m->count * LOAD_FACTOR_DEN >= m->cap * LOAD_FACTOR_NUM) {
        if (inode_map_grow(m) != 0) {
            return -1;
        }
    }

    idx = (uint32_t)(hash_u64(inode->fileid) % m->cap);
    for (i = 0; i < m->cap; i++) {
        struct inode_slot *s = &m->slots[(idx + i) % m->cap];
        if (s->fileid == 0) {
            s->fileid = inode->fileid;
            s->inode = *inode;
            m->count++;
            return 0;
        }
        if (s->fileid == inode->fileid) {
            s->inode = *inode;
            return 0;
        }
    }
    return -1; /* table full (shouldn't happen with load factor) */
}

static int inode_map_grow(struct inode_map *m)
{
    struct inode_map new_map;
    uint32_t i;

    if (inode_map_init(&new_map, m->cap * 2) != 0) {
        return -1;
    }
    for (i = 0; i < m->cap; i++) {
        if (m->slots[i].fileid != 0) {
            inode_map_put(&new_map, &m->slots[i].inode);
        }
    }
    free(m->slots);
    *m = new_map;
    return 0;
}

static void inode_map_del(struct inode_map *m, uint64_t fileid)
{
    uint32_t idx = (uint32_t)(hash_u64(fileid) % m->cap);
    uint32_t i;

    for (i = 0; i < m->cap; i++) {
        uint32_t pos = (idx + i) % m->cap;
        struct inode_slot *s = &m->slots[pos];

        if (s->fileid == 0) {
            return;
        }
        if (s->fileid == fileid) {
            /* Tombstone: mark empty and rehash subsequent slots. */
            s->fileid = 0;
            memset(&s->inode, 0, sizeof(s->inode));
            m->count--;

            /* Rehash chain. */
            for (uint32_t j = 1; j < m->cap; j++) {
                uint32_t next = (pos + j) % m->cap;
                if (m->slots[next].fileid == 0) {
                    break;
                }
                struct mds_inode tmp = m->slots[next].inode;
                m->slots[next].fileid = 0;
                m->count--;
                inode_map_put(m, &tmp);
            }
            return;
        }
    }
}

/* -----------------------------------------------------------------------
 * Dirent hash table
 * ----------------------------------------------------------------------- */

struct dirent_slot {
    uint64_t parent;       /* 0 = empty */
    char     name[MDS_MAX_NAME + 1];
    uint64_t fileid;
    uint8_t  type;
};

struct dirent_map {
    struct dirent_slot *slots;
    uint32_t            cap;
    uint32_t            count;
};

static int dirent_map_init(struct dirent_map *m, uint32_t cap)
{
    m->slots = calloc(cap, sizeof(*m->slots));
    if (m->slots == NULL) {
        return -1;
    }
    m->cap = cap;
    m->count = 0;
    return 0;
}

static void dirent_map_free(struct dirent_map *m)
{
    free(m->slots);
    m->slots = NULL;
    m->cap = 0;
    m->count = 0;
}

static bool dirent_slot_match(const struct dirent_slot *s,
                               uint64_t parent, const char *name)
{
    return s->parent == parent && strcmp(s->name, name) == 0;
}

static struct dirent_slot *dirent_map_find(const struct dirent_map *m,
                                            uint64_t parent,
                                            const char *name)
{
    uint32_t idx = (uint32_t)(hash_dirent_key(parent, name) % m->cap);
    uint32_t i;

    for (i = 0; i < m->cap; i++) {
        struct dirent_slot *s = &m->slots[(idx + i) % m->cap];
        if (s->parent == 0 && s->name[0] == '\0') {
            return NULL;
        }
        if (dirent_slot_match(s, parent, name)) {
            return s;
        }
    }
    return NULL;
}

static int dirent_map_grow(struct dirent_map *m);

static int dirent_map_put(struct dirent_map *m, uint64_t parent,
                           const char *name, uint64_t fileid,
                           uint8_t type)
{
    uint32_t idx;
    uint32_t i;

    if (m->count * LOAD_FACTOR_DEN >= m->cap * LOAD_FACTOR_NUM) {
        if (dirent_map_grow(m) != 0) {
            return -1;
        }
    }

    idx = (uint32_t)(hash_dirent_key(parent, name) % m->cap);
    for (i = 0; i < m->cap; i++) {
        struct dirent_slot *s = &m->slots[(idx + i) % m->cap];
        if (s->parent == 0 && s->name[0] == '\0') {
            s->parent = parent;
            (void)snprintf(s->name, sizeof(s->name), "%s", name);
            s->fileid = fileid;
            s->type = type;
            m->count++;
            return 0;
        }
        if (dirent_slot_match(s, parent, name)) {
            s->fileid = fileid;
            s->type = type;
            return 0;
        }
    }
    return -1;
}

static int dirent_map_grow(struct dirent_map *m)
{
    struct dirent_map new_map;
    uint32_t i;

    if (dirent_map_init(&new_map, m->cap * 2) != 0) {
        return -1;
    }
    for (i = 0; i < m->cap; i++) {
        const struct dirent_slot *s = &m->slots[i];
        if (s->parent != 0 || s->name[0] != '\0') {
            dirent_map_put(&new_map, s->parent, s->name,
                           s->fileid, s->type);
        }
    }
    free(m->slots);
    *m = new_map;
    return 0;
}

static void dirent_map_del(struct dirent_map *m, uint64_t parent,
                            const char *name)
{
    uint32_t idx = (uint32_t)(hash_dirent_key(parent, name) % m->cap);
    uint32_t i;

    for (i = 0; i < m->cap; i++) {
        uint32_t pos = (idx + i) % m->cap;
        struct dirent_slot *s = &m->slots[pos];

        if (s->parent == 0 && s->name[0] == '\0') {
            return;
        }
        if (dirent_slot_match(s, parent, name)) {
            memset(s, 0, sizeof(*s));
            m->count--;

            /* Rehash chain. */
            for (uint32_t j = 1; j < m->cap; j++) {
                uint32_t next = (pos + j) % m->cap;
                struct dirent_slot *ns = &m->slots[next];
                if (ns->parent == 0 && ns->name[0] == '\0') {
                    break;
                }
                struct dirent_slot tmp = *ns;
                memset(ns, 0, sizeof(*ns));
                m->count--;
                dirent_map_put(m, tmp.parent, tmp.name,
                               tmp.fileid, tmp.type);
            }
            return;
        }
    }
}

/* -----------------------------------------------------------------------
 * Image struct
 * ----------------------------------------------------------------------- */

struct catalog_image {
    struct inode_map   inodes;
    struct dirent_map  dirents;
    uint64_t           high_water[MAX_STREAMS];
    uint64_t           expected_next[MAX_STREAMS]; /**< Gap detection. */
    bool               has_gap[MAX_STREAMS];
    pthread_rwlock_t   lock;  /**< Phase 9C: protects inodes + dirents. */
};

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

int catalog_image_create(struct catalog_image **out)
{
    struct catalog_image *img;

    if (out == NULL) {
        return -1;
    }
    img = calloc(1, sizeof(*img));
    if (img == NULL) {
        return -1;
    }
    if (inode_map_init(&img->inodes, INODE_INIT_CAP) != 0) {
        free(img);
        return -1;
    }
    if (dirent_map_init(&img->dirents, DIRENT_INIT_CAP) != 0) {
        inode_map_free(&img->inodes);
        free(img);
        return -1;
    }
    if (pthread_rwlock_init(&img->lock, NULL) != 0) {
        dirent_map_free(&img->dirents);
        inode_map_free(&img->inodes);
        free(img);
        return -1;
    }
    *out = img;
    return 0;
}

void catalog_image_destroy(struct catalog_image *img)
{
    if (img == NULL) {
        return;
    }
    pthread_rwlock_destroy(&img->lock);
    inode_map_free(&img->inodes);
    dirent_map_free(&img->dirents);
    free(img);
}

int catalog_image_inode_put(struct catalog_image *img,
                            const struct mds_inode *inode)
{
    if (img == NULL || inode == NULL || inode->fileid == 0) {
        return -1;
    }
    pthread_rwlock_wrlock(&img->lock);
    int rc = inode_map_put(&img->inodes, inode);
    pthread_rwlock_unlock(&img->lock);
    return rc;
}

int catalog_image_inode_get(const struct catalog_image *img,
                            uint64_t fileid,
                            struct mds_inode *out)
{
    const struct inode_slot *s;

    if (img == NULL || out == NULL || fileid == 0) {
        return -1;
    }
    pthread_rwlock_rdlock((pthread_rwlock_t *)&img->lock);
    s = inode_map_find(&img->inodes, fileid);
    if (s == NULL) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&img->lock);
        return 1;
    }
    *out = s->inode;
    pthread_rwlock_unlock((pthread_rwlock_t *)&img->lock);
    return 0;
}

int catalog_image_inode_del(struct catalog_image *img,
                            uint64_t fileid)
{
    if (img == NULL || fileid == 0) {
        return -1;
    }
    pthread_rwlock_wrlock(&img->lock);
    inode_map_del(&img->inodes, fileid);
    pthread_rwlock_unlock(&img->lock);
    return 0;
}

int catalog_image_dirent_put(struct catalog_image *img,
                             uint64_t parent, const char *name,
                             uint64_t fileid, uint8_t type)
{
    if (img == NULL || name == NULL || name[0] == '\0') {
        return -1;
    }
    pthread_rwlock_wrlock(&img->lock);
    int rc = dirent_map_put(&img->dirents, parent, name,
                            fileid, type);
    pthread_rwlock_unlock(&img->lock);
    return rc;
}

int catalog_image_dirent_get(const struct catalog_image *img,
                             uint64_t parent, const char *name,
                             uint64_t *fileid, uint8_t *type)
{
    const struct dirent_slot *s;

    if (img == NULL || name == NULL || fileid == NULL || type == NULL) {
        return -1;
    }
    pthread_rwlock_rdlock((pthread_rwlock_t *)&img->lock);
    s = dirent_map_find(&img->dirents, parent, name);
    if (s == NULL) {
        pthread_rwlock_unlock((pthread_rwlock_t *)&img->lock);
        return 1;
    }
    *fileid = s->fileid;
    *type = s->type;
    pthread_rwlock_unlock((pthread_rwlock_t *)&img->lock);
    return 0;
}

int catalog_image_dirent_del(struct catalog_image *img,
                             uint64_t parent, const char *name)
{
    if (img == NULL || name == NULL) {
        return -1;
    }
    pthread_rwlock_wrlock(&img->lock);
    dirent_map_del(&img->dirents, parent, name);
    pthread_rwlock_unlock(&img->lock);
    return 0;
}

/* -----------------------------------------------------------------------
 * READDIR — collect matching entries, sort by name, iterate
 * ----------------------------------------------------------------------- */

struct readdir_entry {
    uint64_t fileid;
    uint8_t  type;
    char     name[MDS_MAX_NAME + 1];
};

static int readdir_cmp(const void *a, const void *b)
{
    return strcmp(((const struct readdir_entry *)a)->name,
                 ((const struct readdir_entry *)b)->name);
}

static int decode_timespec_field(const uint8_t *buf, uint32_t len,
                                 uint32_t *off, struct timespec *ts)
{
    if (buf == NULL || off == NULL || ts == NULL ||
        *off > len || len - *off < 12) {
        return -1;
    }
    ts->tv_sec = (time_t)(int64_t)fdb_get_u64(buf + *off);
    *off += 8;
    ts->tv_nsec = (long)fdb_get_u32(buf + *off);
    *off += 4;
    return 0;
}

static int decode_inode_upsert_payload(const uint8_t *buf, uint32_t len,
                                       struct mds_inode *inode)
{
    uint32_t off = 0;

    if (buf == NULL || inode == NULL || len < 8) {
        return -1;
    }

    memset(inode, 0, sizeof(*inode));
    if (len != INODE_UPSERT_PAYLOAD_SIZE) {
        inode->fileid = fdb_get_u64(buf);
        return 0;
    }

    inode->fileid = fdb_get_u64(buf + off);
    off += 8;
    inode->type = (enum mds_file_type)buf[off++];
    inode->mode = fdb_get_u32(buf + off);
    off += 4;
    inode->nlink = fdb_get_u32(buf + off);
    off += 4;
    inode->uid = fdb_get_u64(buf + off);
    off += 8;
    inode->gid = fdb_get_u64(buf + off);
    off += 8;
    inode->size = fdb_get_u64(buf + off);
    off += 8;
    inode->space_used = fdb_get_u64(buf + off);
    off += 8;
    if (decode_timespec_field(buf, len, &off, &inode->atime) != 0 ||
        decode_timespec_field(buf, len, &off, &inode->mtime) != 0 ||
        decode_timespec_field(buf, len, &off, &inode->ctime) != 0) {
        return -1;
    }
    inode->change = fdb_get_u64(buf + off);
    off += 8;
    inode->generation = fdb_get_u64(buf + off);
    off += 8;
    inode->flags = fdb_get_u32(buf + off);
    off += 4;
    inode->create_verf = fdb_get_u64(buf + off);
    off += 8;
    inode->parent_fileid = fdb_get_u64(buf + off);
    off += 8;
    inode->stripe_count = fdb_get_u32(buf + off);
    off += 4;
    inode->stripe_unit = fdb_get_u32(buf + off);
    off += 4;
    inode->mirror_count = fdb_get_u32(buf + off);
    off += 4;
    inode->ds_map = NULL;

    return (off == len) ? 0 : -1;
}

int catalog_image_readdir(const struct catalog_image *img,
                          uint64_t parent, const char *start_after,
                          catalog_image_readdir_cb cb, void *ctx)
{
    struct readdir_entry *arr = NULL;
    uint32_t arr_count = 0;
    uint32_t arr_cap = 0;
    uint32_t i;

    if (img == NULL || cb == NULL) {
        return -1;
    }

    pthread_rwlock_rdlock((pthread_rwlock_t *)&img->lock);

    /* Collect all entries for this parent. */
    for (i = 0; i < img->dirents.cap; i++) {
        const struct dirent_slot *s = &img->dirents.slots[i];
        if (s->parent != parent || s->name[0] == '\0') {
            continue;
        }
        if (arr_count >= arr_cap) {
            uint32_t new_cap = (arr_cap == 0) ? 64 : arr_cap * 2;
            struct readdir_entry *tmp = realloc(
                arr, new_cap * sizeof(*tmp));
            if (tmp == NULL) {
                free(arr);
                return -1;
            }
            arr = tmp;
            arr_cap = new_cap;
        }
        arr[arr_count].fileid = s->fileid;
        arr[arr_count].type = s->type;
        (void)snprintf(arr[arr_count].name,
                       sizeof(arr[arr_count].name), "%s", s->name);
        arr_count++;
    }

    /* Sort by name for stable READDIR ordering. */
    if (arr_count > 1) {
        qsort(arr, arr_count, sizeof(*arr), readdir_cmp);
    }

    /* Iterate with start_after filtering. */
    for (i = 0; i < arr_count; i++) {
        if (start_after != NULL && strcmp(arr[i].name, start_after) <= 0) {
            continue;
        }
        if (cb(arr[i].fileid, arr[i].type, arr[i].name, ctx) != 0) {
            break;
        }
    }

    free(arr);
    pthread_rwlock_unlock((pthread_rwlock_t *)&img->lock);
    return 0;
}

/* -----------------------------------------------------------------------
 * Replay applier
 * ----------------------------------------------------------------------- */

/* NOLINTNEXTLINE(readability-function-cognitive-complexity) */
int catalog_image_apply(struct catalog_image *img,
                        const struct catalog_delta_record *rec)
{
    struct mds_inode ino;
    if (img == NULL || rec == NULL) {
        return -1;
    }

    switch (rec->type) {
    case CAT_DELTA_INODE_UPSERT:
        if (rec->payload == NULL ||
            decode_inode_upsert_payload(rec->payload, rec->payload_len,
                                        &ino) != 0 ||
            catalog_image_inode_put(img, &ino) != 0) {
            return -1;
        }
        break;

    case CAT_DELTA_INODE_DELETE:
        if (rec->payload_len >= 8 && rec->payload != NULL) {
            uint64_t fid = fdb_get_u64(rec->payload);
            if (catalog_image_inode_del(img, fid) != 0) {
                return -1;
            }
        } else {
            return -1;
        }
        break;

    case CAT_DELTA_DIRENT_PUT:
        /* Uniform payload: parent(8) + child_fileid(8) + type(1) + name. */
        if (rec->payload_len > 17 && rec->payload != NULL) {
            const uint8_t *p = rec->payload;
            uint64_t parent = fdb_get_u64(p);
            uint64_t child = fdb_get_u64(p + 8);
            uint8_t ftype = p[16];
            const char *name = (const char *)(p + 17);
            uint32_t name_len = rec->payload_len - 17;
            char name_buf[MDS_MAX_NAME + 1];
            uint32_t copy = (name_len > MDS_MAX_NAME)
                          ? MDS_MAX_NAME : name_len;
            memcpy(name_buf, name, copy);
            name_buf[copy] = '\0';
            if (catalog_image_dirent_put(img, parent, name_buf,
                                         child, ftype) != 0) {
                return -1;
            }
        } else {
            return -1;
        }
        break;

    case CAT_DELTA_DIRENT_DELETE:
        /* Uniform payload: parent(8) + child_fileid(8) + type(1) + name. */
        if (rec->payload_len > 17 && rec->payload != NULL) {
            const uint8_t *p = rec->payload;
            uint64_t parent = fdb_get_u64(p);
            const char *name = (const char *)(p + 17);
            uint32_t name_len = rec->payload_len - 17;
            char name_buf[MDS_MAX_NAME + 1];
            uint32_t copy = (name_len > MDS_MAX_NAME)
                          ? MDS_MAX_NAME : name_len;
            memcpy(name_buf, name, copy);
            name_buf[copy] = '\0';
            if (catalog_image_dirent_del(img, parent, name_buf) != 0) {
                return -1;
            }
        } else {
            return -1;
        }
        break;

    default:
        /* Other record types not in initial image scope. */
        break;
    }

    /* Update high-water mark + gap detection. */
    if (rec->stream_id < MAX_STREAMS) {
        uint32_t sid = rec->stream_id;
        if ((img->expected_next[sid] == 0 && rec->seqno != 1) ||
            (img->expected_next[sid] != 0 &&
             rec->seqno != img->expected_next[sid])) {
            img->has_gap[sid] = true;
        }
        if (rec->seqno > img->high_water[sid]) {
            img->high_water[sid] = rec->seqno;
        }
        img->expected_next[sid] = rec->seqno + 1;
    }

    return 0;
}

uint64_t catalog_image_applied_high_water(
    const struct catalog_image *img, uint32_t stream_id)
{
    if (img == NULL || stream_id >= MAX_STREAMS) {
        return 0;
    }
    return img->high_water[stream_id];
}

bool catalog_image_is_complete(const struct catalog_image *img,
                               uint32_t stream_id,
                               uint64_t authority_seqno)
{
    if (img == NULL || stream_id >= MAX_STREAMS) {
        return false;
    }
    /* Require both: caught up AND no gaps detected. */
    if (img->has_gap[stream_id]) {
        return false;
    }
    return img->high_water[stream_id] >= authority_seqno;
}

/* -----------------------------------------------------------------------
 * Thread-safety lock/unlock helpers (Phase 9C)
 * ----------------------------------------------------------------------- */

void catalog_image_read_lock(struct catalog_image *img)
{
    if (img != NULL) {
        pthread_rwlock_rdlock(&img->lock);
    }
}

void catalog_image_read_unlock(struct catalog_image *img)
{
    if (img != NULL) {
        pthread_rwlock_unlock(&img->lock);
    }
}

void catalog_image_write_lock(struct catalog_image *img)
{
    if (img != NULL) {
        pthread_rwlock_wrlock(&img->lock);
    }
}

void catalog_image_write_unlock(struct catalog_image *img)
{
    if (img != NULL) {
        pthread_rwlock_unlock(&img->lock);
    }
}

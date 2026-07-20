/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * copy_offload.c -- NFSv4.2 asynchronous COPY offload tracker.
 *
 * Each async COPY spawns a worker thread that reads from the source
 * file and writes to the destination file via proxy I/O in 1 MiB
 * chunks.  The worker periodically checks the job's cancelled flag
 * and stops early if set.
 */

#include <stdlib.h>
#include <string.h>

#include "copy_offload.h"
#include "proxy_io.h"
#include "commit_queue.h"
#include "mds_catalogue.h"

/* -----------------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------------- */

static struct copy_job *find_job(struct copy_offload_table *cot,
                                 const struct nfs4_stateid *stateid)
{
    uint32_t i;

    for (i = 0; i < COPY_OFFLOAD_MAX_JOBS; i++) {
        struct copy_job *j = &cot->jobs[i];

        if (!j->active) {
            continue;
}
        if (memcmp(&j->stateid, stateid, sizeof(*stateid)) == 0) {
            return j;
}
    }
    return NULL;
}

/**
 * Post-copy metadata update: extend destination inode size and submit
 * quota adjustment.  Called by copy_worker() BEFORE marking the job
 * complete so that OFFLOAD_STATUS never reports success with stale
 * metadata.
 *
 * @return MDS_OK on success, or an error if the inode update fails.
 */
static enum mds_status copy_worker_finish(struct copy_job *j,
                                          uint64_t total)
{
    uint64_t new_end = j->dst_offset + total;
    struct mds_inode dst_ino;
    enum mds_status st;

    if (new_end <= j->dst_old_size) {
        return MDS_OK;
    }

    st = mds_cat_ns_getattr(j->cat, j->dst_fileid, &dst_ino);
    if (st != MDS_OK) {
        return st;
    }
    if (new_end <= dst_ino.size) {
        return MDS_OK; /* Another writer already extended. */
    }

    dst_ino.size = new_end;
    dst_ino.space_used = new_end;
    st = mds_cat_ns_setattr(j->cat, NULL, j->dst_fileid,
                            &dst_ino, MDS_ATTR_SIZE);
    if (st != MDS_OK) {
        return st;
    }

    /* Submit quota adjustment via CQ if available. */
    if (j->cq != NULL) {
        struct commit_op cop;
        memset(&cop, 0, sizeof(cop));
        cop.type = COMMIT_OP_QUOTA_ADJUST;
        cop.args.quota_adjust.uid = j->dst_uid;
        cop.args.quota_adjust.gid = j->dst_gid;
        cop.args.quota_adjust.delta_bytes =
            (int64_t)(new_end - j->dst_old_size);
        cop.args.quota_adjust.delta_inodes = 0;
        (void)commit_queue_submit(j->cq, &cop);
    }

    return MDS_OK;
}

/* Worker thread entry point. */
static void *copy_worker(void *arg)
{
    struct copy_job *j = (struct copy_job *)arg;
    static const uint32_t chunk = 1024 * 1024; /* 1 MiB */
    uint8_t *buf = NULL;
    struct mds_inode src_inode;
    uint64_t total = 0;
    uint64_t copy_count;
    enum mds_status st = MDS_OK;
    st = mds_cat_ns_getattr(j->cat, j->src_fileid, &src_inode);
    if (st != MDS_OK) {
        goto done;
    }
    if (j->src_offset >= src_inode.size) {
        goto done;
    }
    copy_count = j->count;
    if (copy_count > src_inode.size - j->src_offset) {
        copy_count = src_inode.size - j->src_offset;
    }

    buf = malloc(chunk);
    if (buf == NULL) {
        st = MDS_ERR_NOMEM;
        goto done;
    }

    while (total < copy_count) {
        uint32_t want = chunk;
        uint32_t nr = 0;
        uint32_t nw = 0;
        bool eof_flag = false;
        bool cancelled = false;

        pthread_mutex_lock(&j->mtx);
        cancelled = (j->state == COPY_JOB_CANCELLED);
        pthread_mutex_unlock(&j->mtx);
        if (cancelled) {
            break;
        }

        if (copy_count - total < want) {
            want = (uint32_t)(copy_count - total);
        }

        st = mds_proxy_read(j->proxy, j->cat, j->src_fileid,
                            src_inode.size,
                            j->src_offset + total, want,
                            buf, &nr, &eof_flag);
        if (st != MDS_OK || nr == 0) {
            break;
        }

        st = mds_proxy_write(j->proxy, j->cat, j->dst_fileid,
                             j->dst_offset + total,
                             buf, nr, &nw);
        if (st != MDS_OK) {
            break;
        }
        if (nw != nr) {
            st = MDS_ERR_IO;
            break;
        }

        total += nw;

        pthread_mutex_lock(&j->mtx);
        j->bytes_done = total;
        pthread_mutex_unlock(&j->mtx);

        if (eof_flag) {
            break;
        }
    }

done:
    free(buf);

    /* Finalise metadata BEFORE marking the job complete so that
     * OFFLOAD_STATUS never reports success with stale inode state. */
    if (st == MDS_OK && total > 0) {
        st = copy_worker_finish(j, total);
    }

    pthread_mutex_lock(&j->mtx);
    if (j->state == COPY_JOB_RUNNING) {
        if (st != MDS_OK) {
            j->state = COPY_JOB_ERROR;
            j->error = st;
        } else {
            j->state = COPY_JOB_COMPLETE;
            j->error = MDS_OK;
        }
    }
    j->bytes_done = total;
    pthread_mutex_unlock(&j->mtx);

    return NULL;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

enum mds_status copy_offload_create(struct copy_offload_table **out)
{
    struct copy_offload_table *cot = NULL;

    if (out == NULL) {
        return MDS_ERR_INVAL;
}

    cot = calloc(1, sizeof(*cot));
    if (cot == NULL) {
        return MDS_ERR_NOMEM;
}

    pthread_mutex_init(&cot->lock, NULL);
    cot->next_seq = 1;
    *out = cot;
    return MDS_OK;
}

void copy_offload_destroy(struct copy_offload_table *cot)
{
    uint32_t i;

    if (cot == NULL) {
        return;
}

    /* Cancel all running jobs and wait for threads. */
    pthread_mutex_lock(&cot->lock);
    for (i = 0; i < COPY_OFFLOAD_MAX_JOBS; i++) {
        struct copy_job *j = &cot->jobs[i];

        if (!j->active) {
            continue;
}
        pthread_mutex_lock(&j->mtx);
        if (j->state == COPY_JOB_RUNNING) {
            j->state = COPY_JOB_CANCELLED;
}
        pthread_mutex_unlock(&j->mtx);
    }
    pthread_mutex_unlock(&cot->lock);

    /* Join threads (they will see the cancelled flag). */
    for (i = 0; i < COPY_OFFLOAD_MAX_JOBS; i++) {
        struct copy_job *j = &cot->jobs[i];

        if (!j->active) {
            continue;
}
        pthread_join(j->thread, NULL);
        pthread_mutex_destroy(&j->mtx);
        j->active = false;
    }

    pthread_mutex_destroy(&cot->lock);
    free(cot);
}

enum mds_status copy_offload_start(struct copy_offload_table *cot,
                                   struct mds_proxy_ctx *proxy,
                                   struct mds_catalogue *cat,
                                   uint64_t src_fileid,
                                   uint64_t src_offset,
                                   uint64_t dst_fileid,
                                   uint64_t dst_offset,
                                   uint64_t count,
                                   struct nfs4_stateid *stateid,
                                   struct commit_queue *cq,
                                   uint64_t dst_uid,
                                   uint64_t dst_gid,
                                   uint64_t dst_old_size)
{
    struct copy_job *j = NULL;
    uint32_t i;
    int rc;

    if (cot == NULL || proxy == NULL || cat == NULL || stateid == NULL) {
        return MDS_ERR_INVAL;
}

    pthread_mutex_lock(&cot->lock);

    /* Reap completed/cancelled/error jobs so slots can be reused
     * even if the client never calls OFFLOAD_STATUS. */
    for (i = 0; i < COPY_OFFLOAD_MAX_JOBS; i++) {
        struct copy_job *rj = &cot->jobs[i];

        if (!rj->active) {
            continue;
}
        pthread_mutex_lock(&rj->mtx);
        if (rj->state != COPY_JOB_RUNNING) {
            pthread_mutex_unlock(&rj->mtx);
            pthread_join(rj->thread, NULL);
            pthread_mutex_destroy(&rj->mtx);
            rj->active = false;
        } else {
            pthread_mutex_unlock(&rj->mtx);
        }
    }

    /* Find a free slot. */
    for (i = 0; i < COPY_OFFLOAD_MAX_JOBS; i++) {
        if (!cot->jobs[i].active) {
            j = &cot->jobs[i];
            break;
        }
    }

    if (j == NULL) {
        pthread_mutex_unlock(&cot->lock);
        return MDS_ERR_NOSPC;
    }

    /* Initialise job. */
    memset(j, 0, sizeof(*j));
    j->active = true;
    j->state = COPY_JOB_RUNNING;
    j->src_fileid = src_fileid;
    j->src_offset = src_offset;
    j->dst_fileid = dst_fileid;
    j->dst_offset = dst_offset;
    j->count = count;
    j->proxy = proxy;
    j->cat = cat;
    j->cq = cq;
    j->dst_uid = dst_uid;
    j->dst_gid = dst_gid;
    j->dst_old_size = dst_old_size;

    /* Generate a unique copy_stateid. */
    j->stateid.seqid = cot->next_seq++;
    j->stateid.other[0] = (uint8_t)(i & 0xFF);
    j->stateid.other[1] = (uint8_t)((i >> 8) & 0xFF);
    /* Remaining bytes are zero (from memset). */

    pthread_mutex_init(&j->mtx, NULL);

    /* Spawn worker thread. */
    rc = pthread_create(&j->thread, NULL, copy_worker, j);
    if (rc != 0) {
        pthread_mutex_destroy(&j->mtx);
        j->active = false;
        pthread_mutex_unlock(&cot->lock);
        return MDS_ERR_IO;
    }

    *stateid = j->stateid;
    pthread_mutex_unlock(&cot->lock);
    return MDS_OK;
}

enum mds_status copy_offload_status(struct copy_offload_table *cot,
                                    const struct nfs4_stateid *stateid,
                                    uint64_t *count,
                                    bool *complete,
                                    enum mds_status *error)
{
    struct copy_job *j;

    if (cot == NULL || stateid == NULL) {
        return MDS_ERR_INVAL;
}

    pthread_mutex_lock(&cot->lock);
    j = find_job(cot, stateid);
    if (j == NULL) {
        pthread_mutex_unlock(&cot->lock);
        return MDS_ERR_NOTFOUND;
    }

    enum copy_job_state saved_state;

    pthread_mutex_lock(&j->mtx);
    saved_state = j->state;
    if (count != NULL) {
        *count = j->bytes_done;
}
    if (complete != NULL) {
        *complete = (saved_state != COPY_JOB_RUNNING);
}
    if (error != NULL) {
        *error = j->error;
}
    pthread_mutex_unlock(&j->mtx);

    /* If the job is done, reap the thread and free the slot. */
    if (saved_state != COPY_JOB_RUNNING) {
        pthread_join(j->thread, NULL);
        pthread_mutex_destroy(&j->mtx);
        j->active = false;
    }

    pthread_mutex_unlock(&cot->lock);
    return MDS_OK;
}

enum mds_status copy_offload_cancel(struct copy_offload_table *cot,
                                    const struct nfs4_stateid *stateid)
{
    struct copy_job *j;

    if (cot == NULL || stateid == NULL) {
        return MDS_ERR_INVAL;
}

    pthread_mutex_lock(&cot->lock);
    j = find_job(cot, stateid);
    if (j == NULL) {
        pthread_mutex_unlock(&cot->lock);
        return MDS_ERR_NOTFOUND;
    }

    pthread_mutex_lock(&j->mtx);
    if (j->state == COPY_JOB_RUNNING) {
        j->state = COPY_JOB_CANCELLED;
}
    pthread_mutex_unlock(&j->mtx);

    pthread_mutex_unlock(&cot->lock);
    return MDS_OK;
}

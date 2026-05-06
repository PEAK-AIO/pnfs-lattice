/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * copy_offload.h -- NFSv4.2 asynchronous COPY offload tracker.
 *
 * Manages background copy jobs spawned by OP_COPY when the client
 * requests asynchronous operation (synchronous=false).  Each job gets
 * a unique copy_stateid that the client uses with OFFLOAD_STATUS and
 * OFFLOAD_CANCEL.
 */

#ifndef COPY_OFFLOAD_H
#define COPY_OFFLOAD_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

#include "pnfs_mds.h"
#include "open_state.h"  /* struct nfs4_stateid */

/* Forward declarations. */
struct mds_catalogue;
struct mds_proxy_ctx;
struct commit_queue;

/* -----------------------------------------------------------------------
 * Copy job states
 * ----------------------------------------------------------------------- */

enum copy_job_state {
    COPY_JOB_RUNNING   = 0,
    COPY_JOB_COMPLETE  = 1,
    COPY_JOB_CANCELLED = 2,
    COPY_JOB_ERROR     = 3,
};

/* -----------------------------------------------------------------------
 * Copy job
 * ----------------------------------------------------------------------- */

#define COPY_OFFLOAD_MAX_JOBS 256

struct copy_job {
    struct nfs4_stateid stateid;
    uint64_t            src_fileid;
    uint64_t            src_offset;
    uint64_t            dst_fileid;
    uint64_t            dst_offset;
    uint64_t            count;       /**< Requested byte count. */
    uint64_t            bytes_done;  /**< Bytes copied so far. */
    enum copy_job_state state;
    enum mds_status     error;       /**< Set when state == ERROR. */
    bool                active;      /**< Slot in use. */
    pthread_t           thread;
    pthread_mutex_t     mtx;
    /* Back-pointers for the worker thread. */
    struct mds_proxy_ctx *proxy;
    struct mds_catalogue *cat;
    /* Quota accounting: populated when quota is active. */
    struct commit_queue  *cq;       /**< CQ for async quota adjust. */
    uint64_t              dst_uid;
    uint64_t              dst_gid;
    uint64_t              dst_old_size;
};

/* -----------------------------------------------------------------------
 * Copy offload table
 * ----------------------------------------------------------------------- */

struct copy_offload_table {
    struct copy_job  jobs[COPY_OFFLOAD_MAX_JOBS];
    pthread_mutex_t  lock;
    uint32_t         next_seq;  /**< Monotonic counter for stateid. */
};

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

/**
 * Create a copy offload table.
 *
 * @param out  Receives the new table.  Caller owns it.
 * @return MDS_OK on success.
 */
enum mds_status copy_offload_create(struct copy_offload_table **out);

/**
 * Destroy a copy offload table and cancel all running jobs.
 *
 * @param cot  Table from copy_offload_create().  NULL is tolerated.
 */
void copy_offload_destroy(struct copy_offload_table *cot);

/**
 * Start an asynchronous copy job.
 *
 * Spawns a detached worker thread that copies data via proxy I/O.
 * Returns immediately with a copy_stateid the client can poll.
 *
 * @param cot         Copy offload table.
 * @param proxy       Proxy I/O context (for read/write).
 * @param cat         Catalogue handle (for stripe map and inode updates).
 * @param src_fileid  Source file.
 * @param src_offset  Source offset.
 * @param dst_fileid  Destination file.
 * @param dst_offset  Destination offset.
 * @param count       Bytes to copy.
 * @param stateid     Receives the copy_stateid for this job.
 * @param cq          Commit queue for async quota accounting (may be NULL).
 * @param dst_uid     Destination file owner UID (for quota).
 * @param dst_gid     Destination file owner GID (for quota).
 * @param dst_old_size Destination file size before copy (for quota delta).
 * @return MDS_OK, MDS_ERR_NOSPC (too many concurrent jobs).
 */
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
                                   uint64_t dst_old_size);

/**
 * Query the status of an async copy job.
 *
 * @param cot       Copy offload table.
 * @param stateid   Copy stateid from copy_offload_start().
 * @param count     Receives bytes copied so far.
 * @param complete  Receives true if the job has finished.
 * @param error     Receives the completion status (MDS_OK or error).
 * @return MDS_OK if job found, MDS_ERR_NOTFOUND otherwise.
 */
enum mds_status copy_offload_status(struct copy_offload_table *cot,
                                    const struct nfs4_stateid *stateid,
                                    uint64_t *count,
                                    bool *complete,
                                    enum mds_status *error);

/**
 * Cancel an async copy job.
 *
 * Sets the cancelled flag; the worker checks periodically and stops.
 * The job slot is freed once the worker exits.
 *
 * @param cot      Copy offload table.
 * @param stateid  Copy stateid to cancel.
 * @return MDS_OK if cancelled, MDS_ERR_NOTFOUND if not found.
 */
enum mds_status copy_offload_cancel(struct copy_offload_table *cot,
                                    const struct nfs4_stateid *stateid);

#endif /* COPY_OFFLOAD_H */

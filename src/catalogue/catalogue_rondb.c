/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_rondb.c -- RonDB catalogue backend C wrapper.
 *
 * Implements the mds_cat_* API for the RonDB backend by calling
 * through to catalogue_rondb_shim.cpp via the extern "C" ABI.
 * All NDB API usage is contained in the shim; this file is pure C.
 *
 * Inode data crosses the C/C++ boundary as a fixed 137-byte buffer
 * (rondb_inode_serialize/deserialize from rondb_schema.h).
 *
 * See docs/architecture.md S4.4.3 and the RonDB catalogue plan.
 */

#ifdef HAVE_RONDB

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>

#include "mds_catalogue.h"
#include "mds_log.h"
#include "catalogue_internal.h"
#include "catalogue_rondb.h"
#include "catalog_image.h"
#include "catalog_delta.h"
#include "endian_helpers.h"
#include "rondb_schema.h"
#include "ds_prealloc.h"
#include "quota.h"
#include "open_state.h"

#include <time.h>

struct mds_rondb_state {
	struct mds_rondb_config cfg;
	void                   *handle;
	/* Phase 9B: multi-MDS locking state. */
	bool                    multi_mds;
	uint32_t                lock_ttl_ms;
	uint32_t                mds_id;
	uint64_t                boot_epoch;
	/* Phase 9C: changefeed delta emission. */
	bool                    changefeed_enabled;
	uint64_t                delta_seqno;
	uint64_t                delta_persist_counter; /**< Persist seqno every 64 mutations. */
	pthread_mutex_t         delta_mu; /**< Guards delta_seqno + delta_persist_counter. */
	/* Phase 9C: background image poller. */
	struct catalog_image   *poller_image;
	pthread_t               poller_thread;
	_Atomic bool            poller_running;
	uint32_t                poller_interval_ms;
	uint32_t                poller_self_mds_id;
};

/** Persist the changefeed seqno counter every N mutations. */
#define DELTA_PERSIST_INTERVAL 64

/** Retries for rondb_shim rc==-2 (lock contention / transient NDB). */
#define RONDB_TRANSIENT_RETRIES 8

static void rondb_transient_backoff(int attempt)
{
	/* 500us .. 16ms exponential; mdtest parallel bursts need headroom. */
	uint32_t us = 500U << (uint32_t)(attempt > 5 ? 5 : attempt);
	usleep(us);
}

static void catalogue_rondb_close_backend(struct mds_catalogue *cat);

/* Override the weak default from catalogue_dispatch.c. */
void *mds_catalogue_backend_handle(const struct mds_catalogue *cat)
{
    if (cat == NULL || cat->backend_private == NULL) {
        return NULL;
    }
    return ((struct mds_rondb_state *)cat->backend_private)->handle;
}
static enum mds_status catalogue_rondb_probe_backend(struct mds_catalogue *cat);

/* Forward declarations for vtables defined at end of file. */
static const struct mds_authority_ops rondb_authority_ops;
static const struct mds_authority_ops rondb_locked_authority_ops;
static const struct mds_coordination_ops rondb_coordination_ops;

static const struct mds_catalogue_ops catalogue_rondb_ops = {
    .close = catalogue_rondb_close_backend,
    .probe = catalogue_rondb_probe_backend,
};


enum mds_status mds_rondb_config_load(const char *path,
                                      struct mds_rondb_config *out)
{
    FILE *fp;
    char line[1024];

    if (path == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }

    memset(out, 0, sizeof(*out));
    (void)snprintf(out->schema_name, sizeof(out->schema_name), "pnfs_mds");

    fp = fopen(path, "r");
    if (fp == NULL) {
        return MDS_ERR_IO;
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        char *s = line;
        char *eq;
        char *val;

        while (*s == ' ' || *s == '\t') {
            s++;
        }
        if (*s == '#' || *s == '\0' || *s == '\n') {
            continue;
        }

        eq = strchr(s, '=');
        if (eq == NULL) {
            continue;
        }
        *eq = '\0';

        {
            char *end = eq - 1;
            while (end > s && (*end == ' ' || *end == '\t')) {
                *end-- = '\0';
            }
        }

        val = eq + 1;
        while (*val == ' ' || *val == '\t') {
            val++;
        }
        {
            size_t vlen = strlen(val);
            if (vlen > 0 && val[vlen - 1] == '\n') {
                val[vlen - 1] = '\0';
            }
        }

        if (strcmp(s, "connect_string") == 0) {
            (void)snprintf(out->connect_string,
                           sizeof(out->connect_string), "%s", val);
        } else if (strcmp(s, "schema_name") == 0) {
            (void)snprintf(out->schema_name,
                           sizeof(out->schema_name), "%s", val);
        }
    }

    fclose(fp);

    if (out->connect_string[0] == '\0') {
        MDS_LOG_ERROR(LOG_COMP_CAT,
            "RonDB config missing connect_string");
        return MDS_ERR_INVAL;
    }
    if (out->schema_name[0] == '\0') {
        MDS_LOG_ERROR(LOG_COMP_CAT,
            "RonDB config missing schema_name");
        return MDS_ERR_INVAL;
    }

    return MDS_OK;
}

enum mds_status catalogue_rondb_open(const struct mds_config *cfg,
                                     struct mds_catalogue **out)
{
    struct mds_catalogue *cat;
    struct mds_rondb_state *state;
    enum mds_status st;
    int rc;

    if (cfg == NULL || out == NULL) {
        return MDS_ERR_INVAL;
    }
    if (cfg->catalog_replay_mode == MDS_REPLAY_JOURNAL) {
        MDS_LOG_ERROR(LOG_COMP_CAT,
            "catalogue_backend=rondb with "
            "catalog_replay_mode=journal is not supported until a "
            "RonDB-native replay journal exists");
        return MDS_ERR_INVAL;
    }
    if (cfg->catalogue_backend_conf[0] == '\0') {
        MDS_LOG_ERROR(LOG_COMP_CAT,
            "catalogue_backend=rondb requires "
            "catalogue_backend_conf");
        return MDS_ERR_INVAL;
    }

    state = calloc(1, sizeof(*state));
    cat = calloc(1, sizeof(*cat));
    if (state == NULL || cat == NULL) {
        free(state);
        free(cat);
        return MDS_ERR_NOMEM;
    }

    st = mds_rondb_config_load(cfg->catalogue_backend_conf, &state->cfg);
    if (st != MDS_OK) {
        free(state);
        free(cat);
        return st;
    }

    {
        int pool = (int)cfg->ndb_conn_pool_size;
        if (pool <= 0) { pool = 2; }
        rc = rondb_shim_connect_pool(state->cfg.connect_string,
                                     state->cfg.schema_name,
                                     pool,
                                     &state->handle);
    }
    if (rc != 0 || state->handle == NULL) {
        MDS_LOG_ERROR(LOG_COMP_CAT,
            "rondb_shim_connect() failed for schema %s",
            state->cfg.schema_name);
        free(state);
        free(cat);
        return MDS_ERR_IO;
    }

    /*
     * Phase 4 scaffold: propagate the ndb_async_writes feature
     * flag to the shim.  Off by default; flipping it on alone
     * does nothing until the async-aware ns_create variant
     * lands.  Logged so operators can confirm the config is
     * being parsed correctly.
     */
    rondb_shim_set_async_writes(state->handle, cfg->ndb_async_writes);
    MDS_LOG_INFO(LOG_COMP_CAT,
        "RonDB ndb_async_writes=%s (scaffold; async-aware "
        "write path pending empirical validation)",
        cfg->ndb_async_writes ? "true" : "false");

	cat->backend = MDS_BACKEND_RONDB;
	cat->ops = &catalogue_rondb_ops;
	cat->coord_ops = &rondb_coordination_ops;
	cat->backend_private = state;

	/* Phase 9B: multi-MDS config. */
	state->multi_mds = (cfg->cluster_size > 1);
	state->lock_ttl_ms = 30000; /* 30s default */
	state->mds_id = cfg->self.id;
	state->boot_epoch = 0; /* Set by main.c after register. */

	/* Phase 9C: changefeed enabled when multi-MDS + image mode != OFF. */
	state->changefeed_enabled = (state->multi_mds &&
		cfg->catalog_image_mode != MDS_IMAGE_OFF);
	state->delta_seqno = 1;
	state->delta_persist_counter = 0;
	if (pthread_mutex_init(&state->delta_mu, NULL) != 0) {
		rondb_shim_disconnect(state->handle);
		free(state);
		free(cat);
		return MDS_ERR_IO;
	}
	if (state->changefeed_enabled && state->handle != NULL) {
		uint64_t loaded = 0;
		if (rondb_shim_delta_seqno_load(state->handle,
						state->mds_id,
						&loaded) == 0 && loaded > 0) {
			/* The counter is only persisted every
			 * DELTA_PERSIST_INTERVAL mutations (and on clean
			 * shutdown), so after a crash up to that many
			 * seqnos beyond the persisted value may already
			 * exist in mds_delta_broadcast.  Skip the whole
			 * window so restarted emission does not collide
			 * with already-used keys. */
			state->delta_seqno = loaded + DELTA_PERSIST_INTERVAL;
		}
	}

	cat->auth_ops = state->multi_mds
	              ? &rondb_locked_authority_ops
	              : &rondb_authority_ops;


	*out = cat;
	return MDS_OK;
}

static void catalogue_rondb_close_backend(struct mds_catalogue *cat)
{
	struct mds_rondb_state *state;

	if (cat == NULL) {
		return;
	}

	/* Stop the poller before tearing down the handle: the poller
	 * thread dereferences state->handle on every iteration, so
	 * disconnecting/freeing first would be a use-after-free.
	 * poller_stop joins the thread; close_backend is never called
	 * from the poller itself, so this cannot self-deadlock. */
	catalogue_rondb_poller_stop(cat);

	state = cat->backend_private;
	if (state != NULL) {
		if (state->handle != NULL) {
			rondb_shim_disconnect(state->handle);
			state->handle = NULL;
		}
		pthread_mutex_destroy(&state->delta_mu);
		free(state);
		cat->backend_private = NULL;
	}
}

static enum mds_status catalogue_rondb_probe_backend(struct mds_catalogue *cat)
{
    struct mds_rondb_state *state;

    if (cat == NULL || cat->backend != MDS_BACKEND_RONDB ||
        cat->backend_private == NULL) {
        return MDS_ERR_INVAL;
    }

    state = cat->backend_private;
    return rondb_shim_probe(state->handle) == 0 ? MDS_OK : MDS_ERR_IO;
}

enum mds_status mds_rondb_bootstrap(struct mds_catalogue *cat)
{
	struct mds_rondb_state *state;

	if (cat == NULL || cat->backend != MDS_BACKEND_RONDB ||
	    cat->backend_private == NULL) {
		return MDS_ERR_INVAL;
	}

	state = cat->backend_private;

	/* Bootstrap metadata tables (all 8) + seed rows. */
	if (rondb_shim_bootstrap_metadata(state->handle,
					  state->cfg.schema_name) != 0) {
		return MDS_ERR_IO;
	}

	/* Also bootstrap legacy probe table for backward compat. */
	if (rondb_shim_bootstrap(state->handle,
				 state->cfg.schema_name) != 0) {
		return MDS_ERR_IO;
	}

	return MDS_OK;
}

enum mds_status mds_rondb_cleanup(struct mds_catalogue *cat)
{
	struct mds_rondb_state *state;

	if (cat == NULL || cat->backend != MDS_BACKEND_RONDB ||
	    cat->backend_private == NULL) {
		return MDS_ERR_INVAL;
	}

	state = cat->backend_private;

	/* Drop metadata tables first, then legacy probe table. */
	(void)rondb_shim_cleanup_metadata(state->handle,
					  state->cfg.schema_name);
	(void)rondb_shim_cleanup(state->handle, state->cfg.schema_name);

	return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Stage C -- Catalogue operations
 *
 * Each function validates inputs, calls the shim, and translates
 * return codes to mds_status.  Inode data is exchanged as 137-byte
 * buffers via rondb_inode_serialize/deserialize.
 * ----------------------------------------------------------------------- */

static void *rondb_handle(const struct mds_catalogue *cat)
{
	struct mds_rondb_state *state;

	if (cat == NULL || cat->backend_private == NULL) {
		return NULL;
	}
	state = cat->backend_private;
	return state->handle;
}

/*
 * Per-thread fileid pool.  Each NFS worker thread maintains its own
 * batch of pre-allocated fileids, eliminating all contention on the
 * create hot path.  Refills hit RonDB only when the local batch is
 * exhausted (~once per 1024 creates per thread).
 */
static _Thread_local uint64_t tl_fileid_base      = 0;
static _Thread_local uint32_t tl_fileid_remaining  = 0;

enum mds_status catalogue_rondb_alloc_fileid(struct mds_catalogue *cat,
					     uint64_t *fileid)
{
	struct mds_rondb_state *state;

	if (cat == NULL || fileid == NULL ||
	    cat->backend_private == NULL) {
		return MDS_ERR_INVAL;
	}
	state = cat->backend_private;

	/* Thread-local batch exhausted -- refill from RonDB. */
	if (tl_fileid_remaining == 0) {
		uint64_t base = 0;
		uint32_t count = 0;
		int rc;

		rc = rondb_shim_fileid_batch_alloc(
			state->handle, MDS_FILEID_BATCH,
			&base, &count);
		if (rc != 0 || count == 0) {
			return MDS_ERR_IO;
		}
		tl_fileid_base = base;
		tl_fileid_remaining = count;
	}

	*fileid = tl_fileid_base;
	tl_fileid_base++;
	tl_fileid_remaining--;
	return MDS_OK;
}

enum mds_status catalogue_rondb_ns_getattr(struct mds_catalogue *cat,
					   uint64_t fileid,
					   struct mds_inode *inode)
{
	void *h = rondb_handle(cat);
	uint8_t buf[RONDB_INODE_MAX_SIZE];
	uint32_t outlen = 0;
	uint32_t shard = 0;
	int rc;

	if (h == NULL || inode == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_inode_get(h, fileid, buf, sizeof(buf),
				  &outlen, 0 /* COMMITTED */);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	if (rc != 0) {
		return MDS_ERR_IO;
	}
	if (rondb_inode_deserialize(buf, outlen, inode, &shard) != 0) {
		return MDS_ERR_IO;
	}

	return MDS_OK;
}

enum mds_status catalogue_rondb_ns_lookup(struct mds_catalogue *cat,
					  uint64_t parent_fileid,
					  const char *name,
					  struct mds_inode *child)
{
	void *h = rondb_handle(cat);
	uint64_t child_fid = 0;
	uint8_t child_type = 0;
	uint8_t buf[RONDB_INODE_MAX_SIZE];
	uint32_t outlen = 0;
	uint32_t shard = 0;
	int rc;

	if (h == NULL || name == NULL || child == NULL) {
		return MDS_ERR_INVAL;
	}

	/* Fused dirent + inode read in one NDB transaction.
	 * NoCommit after dirent read, Commit with inode read.
	 * Saves one NDB round-trip vs two separate transactions. */
	rc = rondb_shim_ns_lookup(h, parent_fileid, name,
				  &child_fid, &child_type,
				  buf, sizeof(buf), &outlen);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	if (rc == -2) {
		return MDS_ERR_DELAY;
	}
	if (rc != 0) {
		return MDS_ERR_IO;
	}
	if (rondb_inode_deserialize(buf, outlen, child, &shard) != 0) {
		return MDS_ERR_IO;
	}

	return MDS_OK;
}

enum mds_status catalogue_rondb_ns_create(
	struct mds_catalogue *cat,
	uint64_t parent_fileid,
	const char *name,
	enum mds_file_type type,
	uint32_t mode, uint64_t uid, uint64_t gid,
	struct ds_prealloc_ctx *prealloc,
	struct mds_inode *out)
{
	void *h = rondb_handle(cat);
	struct mds_inode parent, child;
	struct timespec now;
	uint8_t parent_buf[RONDB_INODE_MAX_SIZE];
	uint8_t child_buf[RONDB_INODE_MAX_SIZE];
	uint32_t outlen = 0;
	uint64_t child_fid = 0;
	int32_t parent_nlink_delta;
	int rc;
	enum mds_status st;

	if (h == NULL || name == NULL || out == NULL) {
		return MDS_ERR_INVAL;
	}

	/* Pre-validation of parent type and dirent non-existence is
	 * SKIPPED for performance.  The compound OPEN handler already
	 * validated the parent via compound_inode_get() (cached), and
	 * the T2 rondb_shim_ns_create() returns EXISTS (rc==1) if the
	 * dirent already exists.  This eliminates 2 NDB round trips
	 * from the create hot path.
	 *
	 * Safety: the NDB insertTuple for the dirent is the
	 * authoritative uniqueness check (ConstraintViolation).
	 * Parent type is validated by the NFS protocol layer before
	 * we reach this function. */
	(void)parent_buf;
	(void)parent;

	/* Pre-alloc stripe map + fileid for regular files. */
	uint8_t stripe_buf[256];
	uint32_t stripe_buf_len = 0;
	uint32_t stripe_count_for_create = 0;

	if (type == MDS_FTYPE_REG && prealloc != NULL) {
		struct mds_ds_map_entry ds_entry;
		uint32_t stripe_unit = 0;
		uint64_t prealloc_fid = 0;
		int pop_rc = ds_prealloc_pop(prealloc, &ds_entry,
					    &stripe_unit, &prealloc_fid);
		if (pop_rc == 0) {
			if (prealloc_fid != 0) {
				child_fid = prealloc_fid;
			}
			fdb_put_u32(stripe_buf, ds_entry.ds_id);
			fdb_put_u32(stripe_buf + 4, ds_entry.nfs_fh_len);
			if (ds_entry.nfs_fh_len > 0) {
				memcpy(stripe_buf + 8, ds_entry.nfs_fh,
				       ds_entry.nfs_fh_len);
			}
			stripe_buf_len = 8 + ds_entry.nfs_fh_len;
			stripe_count_for_create = 1;
		}
	}

	/* Allocate fileid (skip if pre-allocated from pool). */
	if (child_fid == 0) {
		st = catalogue_rondb_alloc_fileid(cat, &child_fid);
		if (st != MDS_OK) {
			return st;
		}
	}

	/* Build child inode. */
	clock_gettime(CLOCK_REALTIME, &now);
	memset(&child, 0, sizeof(child));
	child.fileid = child_fid;
	child.type = type;
	child.mode = mode;
	child.uid = uid;
	child.gid = gid;
	child.atime = now;
	child.mtime = now;
	child.ctime = now;
	child.change = 1;
	child.generation = 1;
	child.parent_fileid = parent_fileid;
	child.nlink = (type == MDS_FTYPE_DIR) ? 2 : 1;
	if (stripe_count_for_create > 0 && stripe_buf_len <= 8) {
		child.flags |= MDS_IFLAG_DS_PENDING;
	}

	parent_nlink_delta = (type == MDS_FTYPE_DIR) ? 1 : 0;

	if (rondb_inode_serialize(&child, 0, child_buf,
				  sizeof(child_buf)) < 0) {
		return MDS_ERR_IO;
	}

	/* Atomic create via shim (T2 transaction).
	 * Parent nlink/change/mtime updated atomically at NDB data node
	 * via interpretedUpdateTuple -- immune to read-modify-write race.
	 *
	 * Retry on transient NDB errors (rc == -2): lock contention,
	 * temporary resource exhaustion, node recovery. */
	for (int attempt = 0; attempt < RONDB_TRANSIENT_RETRIES; attempt++) {
		rc = rondb_shim_ns_create(h, parent_fileid, name,
					  child_buf, RONDB_INODE_FIXED_SIZE,
					  parent_nlink_delta,
					  stripe_count_for_create > 0
					      ? stripe_buf : NULL,
					  stripe_buf_len,
					  stripe_count_for_create);
		if (rc != -2) {
			break; /* Success, EXISTS, or permanent error. */
		}
		rondb_transient_backoff(attempt);
	}
	if (rc == 1) {
		return MDS_ERR_EXISTS;
	}
	if (rc != 0) {
		return MDS_ERR_IO;
	}

	*out = child;
	catalog_stat_inc(&cat->stats.authority_writes);
	return MDS_OK;
}

enum mds_status catalogue_rondb_ns_remove_known(struct mds_catalogue *cat,
						uint64_t parent_fileid,
						const char *name,
						const struct mds_inode *child);

enum mds_status catalogue_rondb_ns_remove(struct mds_catalogue *cat,
					  uint64_t parent_fileid,
					  const char *name)
{
	void *h = rondb_handle(cat);
	struct mds_inode child_ino;
	uint64_t child_fid = 0;
	uint8_t child_type = 0;
	uint8_t child_buf[RONDB_INODE_MAX_SIZE];
	uint32_t outlen = 0;
	int rc;

	if (h == NULL || name == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_ns_lookup(h, parent_fileid, name,
				  &child_fid, &child_type,
				  child_buf, sizeof(child_buf),
				  &outlen);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	if (rc == -2) {
		return MDS_ERR_DELAY;
	}
	if (rc != 0) {
		return MDS_ERR_IO;
	}
	if (rondb_inode_deserialize(child_buf, outlen,
				   &child_ino, NULL) != 0) {
		return MDS_ERR_IO;
	}
	return catalogue_rondb_ns_remove_known(cat, parent_fileid, name,
					       &child_ino);
}

enum mds_status catalogue_rondb_ns_remove_known(struct mds_catalogue *cat,
						uint64_t parent_fileid,
						const char *name,
						const struct mds_inode *child)
{
	void *h = rondb_handle(cat);
	struct mds_inode child_ino;
	uint8_t child_buf[RONDB_INODE_MAX_SIZE];
	int delete_child;
	int32_t parent_nlink_delta;
	struct timespec now;
	int rc;

	if (h == NULL || name == NULL || child == NULL) {
		return MDS_ERR_INVAL;
	}

	child_ino = *child;

	/* Prepare updated child inode (nlink decrement). */
	clock_gettime(CLOCK_REALTIME, &now);
	if (child_ino.nlink > 0) {
		child_ino.nlink--;
	}
	child_ino.ctime = now;
	child_ino.change++;
	delete_child = (child_ino.nlink == 0) ? 1 : 0;

	parent_nlink_delta =
		(child_ino.type == MDS_FTYPE_DIR) ? -1 : 0;

	if (rondb_inode_serialize(&child_ino, 0,
				  child_buf, sizeof(child_buf)) < 0) {
		return MDS_ERR_IO;
	}

	for (int attempt = 0; attempt < RONDB_TRANSIENT_RETRIES; attempt++) {
		rc = rondb_shim_ns_remove(h, parent_fileid, name,
					  child_ino.fileid,
					  child_buf, RONDB_INODE_FIXED_SIZE,
					  delete_child,
					  parent_nlink_delta,
					  0 /* stripe_count */);
		if (rc != -2) { break; }
		rondb_transient_backoff(attempt);
	}
	if (rc == 1) {
		/* TOCTOU: concurrent remove beat us; row already gone. */
		return MDS_ERR_NOTFOUND;
	}
	if (rc != 0) {
		return MDS_ERR_IO;
	}

	catalog_stat_inc(&cat->stats.authority_writes);
	return MDS_OK;
}

enum mds_status catalogue_rondb_ns_setattr(struct mds_catalogue *cat,
					   uint64_t fileid,
					   const struct mds_inode *attrs,
					   uint32_t mask)
{
	void *h = rondb_handle(cat);
	uint8_t buf[RONDB_INODE_MAX_SIZE];
	int rc;

	if (h == NULL || attrs == NULL) {
		return MDS_ERR_INVAL;
	}

	/*
	 * Fold the read, merge, and write into a single NDB transaction
	 * via rondb_shim_inode_setattr_rmw.  The shim holds the
	 * exclusive row lock from the read through the update, so the
	 * lost-update window the legacy two-call sequence had between
	 * its committed read and its later locked write is closed; the
	 * caller's masked attributes always merge with the most recent
	 * committed state.  This also saves the startTransaction /
	 * closeTransaction pair the old unlocked read paid for.
	 *
	 * The shim consumes the caller's `attrs` only through the
	 * `mask` bits, exactly as the pre-fold C code did, so semantics
	 * for callers are unchanged.  We serialise `attrs` into a
	 * scratch buffer because the shim's interface is the same byte
	 * format the rest of the codebase uses for inode I/O.
	 */
	if (rondb_inode_serialize(attrs, 0, buf, sizeof(buf)) < 0) {
		return MDS_ERR_IO;
	}
	rc = rondb_shim_inode_setattr_rmw(h, fileid, mask, buf,
					  RONDB_INODE_FIXED_SIZE);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	if (rc != 0) {
		return MDS_ERR_IO;
	}

	catalog_stat_inc(&cat->stats.authority_writes);
	return MDS_OK;
}

/**
 * Full inode write -- direct serialize + atomic write.
 *
 * Unlike ns_setattr (read-modify-write with mask), this replaces
 * the entire inode record atomically.  Used by mds_cat_inode_put()
 * for unconditional inode persistence (insert-or-update).
 *
 * Implementation note: routes through rondb_shim_inode_put (writeTuple)
 * rather than rondb_shim_inode_setattr_atomic (exclusive-read +
 * updateTuple).  setattr_atomic returns NOT_FOUND on a fresh inode and
 * therefore cannot be used here -- wide-stripe CREATE persists a brand
 * new inode through this path before any LOOKUP is possible.
 */
enum mds_status catalogue_rondb_dirent_put(struct mds_catalogue *cat,
					   struct mds_cat_txn *txn,
					   uint64_t parent,
					   const char *name,
					   uint64_t child_fileid,
					   uint8_t child_type)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn; /* RonDB writes are self-contained. */
	if (h == NULL || name == NULL) {
		return MDS_ERR_INVAL;
	}
	rc = rondb_shim_dirent_put(h, parent, name,
				   child_fileid, child_type);
	if (rc != 0) {
		return MDS_ERR_IO;
	}
	catalog_stat_inc(&cat->stats.authority_writes);
	return MDS_OK;
}

enum mds_status catalogue_rondb_dirent_del(struct mds_catalogue *cat,
					   struct mds_cat_txn *txn,
					   uint64_t parent,
					   const char *name)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn; /* RonDB writes are self-contained. */
	if (h == NULL || name == NULL) {
		return MDS_ERR_INVAL;
	}
	rc = rondb_shim_dirent_del(h, parent, name);
	if (rc != 0) {
		return MDS_ERR_IO;
	}
	catalog_stat_inc(&cat->stats.authority_writes);
	return MDS_OK;
}

/**
 * Delete an inode record by fileid (RonDB backend).
 *
 * @param cat    Catalogue handle.
 * @param txn    Unused (RonDB writes are self-contained).
 * @param fileid Inode to delete.
 * @return MDS_OK, MDS_ERR_NOTFOUND, or MDS_ERR_IO.
 */
static enum mds_status catalogue_rondb_inode_del(
	struct mds_catalogue *cat,
	struct mds_cat_txn *txn,
	uint64_t fileid)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn; /* RonDB writes are self-contained. */
	if (h == NULL) {
		return MDS_ERR_INVAL;
	}
	rc = rondb_shim_inode_del(h, fileid);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	if (rc != 0) {
		return MDS_ERR_IO;
	}
	catalog_stat_inc(&cat->stats.authority_writes);
	return MDS_OK;
}

enum mds_status catalogue_rondb_inode_put(struct mds_catalogue *cat,
					  struct mds_cat_txn *txn,
					  const struct mds_inode *inode)
{
	void *h = rondb_handle(cat);
	uint8_t buf[RONDB_INODE_MAX_SIZE];
	int rc;

	(void)txn; /* RonDB writes are self-contained. */
	if (h == NULL || inode == NULL) {
		return MDS_ERR_INVAL;
	}

	if (rondb_inode_serialize(inode, 0, buf, sizeof(buf)) < 0) {
		return MDS_ERR_IO;
	}

	/* writeTuple semantics: insert-or-update.  Required for HPC
	 * wide-stripe CREATE which persists a freshly minted inode that
	 * has no prior row in mds_inodes.  Update-only paths (e.g.
	 * rondb_shim_inode_setattr_atomic) cannot be used here -- they
	 * return NOT_FOUND on a brand-new fileid and surface as ENOENT
	 * to the client. */
	rc = rondb_shim_inode_put(h, inode->fileid, buf,
				  RONDB_INODE_FIXED_SIZE);
	if (rc != 0) {
		return MDS_ERR_IO;
	}

	catalog_stat_inc(&cat->stats.authority_writes);
	return MDS_OK;
}

/**
 * Rename implementation.  On success, *src_fid_out / *src_type_out
 * (when non-NULL) receive the renamed child's fileid and type as
 * resolved from the source dirent.  The changefeed wrapper needs them
 * to emit a DIRENT_PUT delta that maps the destination name to the
 * real child instead of fileid 0.
 */
static enum mds_status rondb_ns_rename_resolved(
	struct mds_catalogue *cat,
	uint64_t src_parent,
	const char *src_name,
	uint64_t dst_parent,
	const char *dst_name,
	uint64_t *src_fid_out,
	uint8_t *src_type_out)
{
	void *h = rondb_handle(cat);
	struct mds_inode sp, dp, sc, dc;
	uint32_t sp_shard = 0, dp_shard = 0, sc_shard = 0, dc_shard = 0;
	uint64_t src_fid = 0, dst_fid = 0;
	uint8_t src_type = 0, dst_type = 0;
	uint8_t sp_buf[RONDB_INODE_MAX_SIZE], dp_buf[RONDB_INODE_MAX_SIZE];
	uint8_t sc_buf[RONDB_INODE_MAX_SIZE], dc_buf[RONDB_INODE_MAX_SIZE];
	uint32_t outlen = 0;
	struct timespec now;
	int dst_exists = 0;
	int delete_dst = 0;
	int cross_dir;
	int rc;

	if (h == NULL || src_name == NULL || dst_name == NULL) {
		return MDS_ERR_INVAL;
	}

	cross_dir = (src_parent != dst_parent) ? 1 : 0;

	/* Read src parent (validation only: must exist and be a dir). */
	rc = rondb_shim_inode_get(h, src_parent, sp_buf,
				  sizeof(sp_buf), &outlen, 0);
	if (rc != 0) {
		return (rc == 1) ? MDS_ERR_NOTFOUND : MDS_ERR_IO;
	}
	if (rondb_inode_deserialize(sp_buf, outlen, &sp, NULL) != 0) {
		return MDS_ERR_IO;
	}
	if (sp.type != MDS_FTYPE_DIR) {
		return MDS_ERR_NOTDIR;
	}

	/* Read dst parent (validation only: must exist and be a dir). */
	if (cross_dir) {
		rc = rondb_shim_inode_get(h, dst_parent, dp_buf,
					  sizeof(dp_buf), &outlen, 0);
		if (rc != 0) {
			return (rc == 1) ? MDS_ERR_NOTFOUND : MDS_ERR_IO;
		}
		if (rondb_inode_deserialize(dp_buf, outlen,
					   &dp, NULL) != 0) {
			return MDS_ERR_IO;
		}
		if (dp.type != MDS_FTYPE_DIR) {
			return MDS_ERR_NOTDIR;
		}
	}

	/* Read src dirent. */
	rc = rondb_shim_dirent_get(h, src_parent, src_name,
				   &src_fid, &src_type, 0);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	if (rc != 0) {
		return MDS_ERR_IO;
	}

	/* Read src child inode. */
	rc = rondb_shim_inode_get(h, src_fid, sc_buf,
				  sizeof(sc_buf), &outlen, 0);
	if (rc != 0) {
		return (rc == 1) ? MDS_ERR_NOTFOUND : MDS_ERR_IO;
	}
	if (rondb_inode_deserialize(sc_buf, outlen, &sc, &sc_shard) != 0) {
		return MDS_ERR_IO;
	}

	/* Check dst dirent (overwrite case). */
	rc = rondb_shim_dirent_get(h, dst_parent, dst_name,
				   &dst_fid, &dst_type, 0);
	if (rc == 0) {
		/* Destination exists -- check type compatibility. */
		dst_exists = 1;

		if (dst_fid == src_fid) {
			/* Same file -- no-op. */
			if (src_fid_out != NULL) { *src_fid_out = src_fid; }
			if (src_type_out != NULL) { *src_type_out = src_type; }
			return MDS_OK;
		}

		/* Type checks: dir->file = NOTDIR, file->dir = ISDIR. */
		if (sc.type == MDS_FTYPE_DIR && dst_type != MDS_FTYPE_DIR) {
			return MDS_ERR_NOTDIR;
		}
		if (sc.type != MDS_FTYPE_DIR && dst_type == MDS_FTYPE_DIR) {
			return MDS_ERR_ISDIR;
		}

		/* Read dst child for nlink update. */
		rc = rondb_shim_inode_get(h, dst_fid, dc_buf,
					  sizeof(dc_buf), &outlen, 0);
		if (rc != 0) {
			return (rc == 1) ? MDS_ERR_NOTFOUND : MDS_ERR_IO;
		}
		if (rondb_inode_deserialize(dc_buf, outlen,
					   &dc, &dc_shard) != 0) {
			return MDS_ERR_IO;
		}

		/* TODO: empty-dir check for dir-replacing-dir. */

		if (dc.nlink > 0) {
			dc.nlink--;
		}
		delete_dst = (dc.nlink == 0) ? 1 : 0;
	} else if (rc != 1) {
		return MDS_ERR_IO;
	}

	/* Compute nlink deltas for parents (handled atomically by shim).
	 * Src parent: -1 if dir moves out in cross-dir rename.
	 * Dst parent: +1 if dir moves in, -1 if overwriting a dir. */
	int32_t sp_nlink_delta = 0;
	int32_t dp_nlink_delta = 0;

	if (cross_dir && sc.type == MDS_FTYPE_DIR) {
		sp_nlink_delta = -1;
		dp_nlink_delta += 1;
	}
	if (cross_dir && dst_exists &&
	    dc.type == MDS_FTYPE_DIR) {
		dp_nlink_delta -= 1;
	}

	/* Prepare updated child inodes. */
	clock_gettime(CLOCK_REALTIME, &now);

	/* Src child: update parent_fileid if cross-dir. */
	if (cross_dir) {
		sc.parent_fileid = dst_parent;
	}
	sc.ctime = now;
	sc.change++;

	/* Dst child: timestamps if not being deleted. */
	if (dst_exists && !delete_dst) {
		dc.ctime = now;
		dc.change++;
	}

	/* Serialize child inodes only (parents updated atomically by shim). */
	if (rondb_inode_serialize(&sc, sc_shard,
				  sc_buf, sizeof(sc_buf)) < 0) {
		return MDS_ERR_IO;
	}
	if (dst_exists && !delete_dst) {
		if (rondb_inode_serialize(&dc, dc_shard,
					  dc_buf, sizeof(dc_buf)) < 0) {
			return MDS_ERR_IO;
		}
	}

	/* Atomic rename via shim (T2 transaction).
	 * Parent nlink/change/mtime updated atomically at NDB data node. */
	rc = rondb_shim_rename(h, src_parent, src_name,
			       dst_parent, dst_name,
			       sp_nlink_delta, dp_nlink_delta,
			       sc_buf, RONDB_INODE_FIXED_SIZE,
			       src_fid, src_type,
			       dst_exists,
			       dst_exists ? dc_buf : NULL,
			       dst_exists ? RONDB_INODE_FIXED_SIZE : 0,
			       dst_fid, delete_dst);
	if (rc != 0) {
		return MDS_ERR_IO;
	}

	if (src_fid_out != NULL) { *src_fid_out = src_fid; }
	if (src_type_out != NULL) { *src_type_out = src_type; }
	return MDS_OK;
}

enum mds_status catalogue_rondb_ns_rename(
	struct mds_catalogue *cat,
	uint64_t src_parent,
	const char *src_name,
	uint64_t dst_parent,
	const char *dst_name)
{
	return rondb_ns_rename_resolved(cat, src_parent, src_name,
					dst_parent, dst_name, NULL, NULL);
}

/* -----------------------------------------------------------------------
 * Readdir -- shim scan adapter
 * ----------------------------------------------------------------------- */

struct rondb_readdir_adapt {
	mds_readdir_cb  cat_cb;
	void           *cat_ctx;
};

static int rondb_readdir_shim_cb(uint64_t child_fid, uint8_t child_type,
				 const char *name, uint32_t name_len,
				 void *arg)
{
	struct rondb_readdir_adapt *a = arg;
	struct mds_cat_dirent ent;

	memset(&ent, 0, sizeof(ent));
	ent.fileid = child_fid;
	ent.type = child_type;
	if (name_len > MDS_MAX_NAME) {
		name_len = MDS_MAX_NAME;
	}
	memcpy(ent.name, name, name_len);
	ent.name[name_len] = '\0';

	return a->cat_cb(&ent, a->cat_ctx);
}

enum mds_status catalogue_rondb_ns_readdir(
	struct mds_catalogue *cat,
	uint64_t parent_fileid,
	const char *start_after,
	uint32_t max_entries,
	mds_readdir_cb cb, void *ctx)
{
	void *h = rondb_handle(cat);
	struct rondb_readdir_adapt adapt;
	int rc;

	if (h == NULL || cb == NULL) {
		return MDS_ERR_INVAL;
	}

	adapt.cat_cb  = cb;
	adapt.cat_ctx = ctx;

	rc = rondb_shim_ns_readdir(h, parent_fileid, start_after,
				   max_entries,
				   rondb_readdir_shim_cb, &adapt);
	if (rc != 0) {
		return MDS_ERR_IO;
	}

	return MDS_OK;
}

enum mds_status catalogue_rondb_dirent_name_for_child(
	struct mds_catalogue *cat,
	uint64_t parent_fileid,
	uint64_t child_fileid,
	char *name_out,
	size_t name_out_len)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || name_out == NULL || name_out_len == 0) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_dirent_name_for_child(h, parent_fileid,
					      child_fileid,
					      name_out, name_out_len);
	if (rc == 0) {
		return MDS_OK;
	}
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Readdir_plus -- fused shim adapter (single NDB transaction).
 * Delivers dirent + per-entry inode attrs through mds_readdir_plus_cb
 * without issuing a separate ns_getattr per entry.
 * ----------------------------------------------------------------------- */

struct rondb_readdir_plus_adapt {
	mds_readdir_plus_cb  cat_cb;
	void                *cat_ctx;
	enum mds_status      cb_status;
};

static int rondb_readdir_plus_shim_cb(uint64_t child_fid,
				      uint8_t child_type,
				      const char *name,
				      uint32_t name_len,
				      const uint8_t *inode_buf,
				      uint32_t inode_len,
				      int inode_valid,
				      void *arg)
{
	struct rondb_readdir_plus_adapt *a = arg;
	struct mds_cat_dirent ent;
	struct mds_inode inode;
	bool valid = inode_valid != 0;

	memset(&ent, 0, sizeof(ent));
	ent.fileid = child_fid;
	ent.type = child_type;
	if (name_len > MDS_MAX_NAME) {
		name_len = MDS_MAX_NAME;
	}
	memcpy(ent.name, name, name_len);
	ent.name[name_len] = '\0';

	memset(&inode, 0, sizeof(inode));
	if (valid) {
		if (inode_buf == NULL ||
		    inode_len < RONDB_INODE_FIXED_SIZE ||
		    rondb_inode_deserialize(inode_buf, inode_len,
					    &inode, NULL) != 0) {
			/* Treat a malformed inode buffer as invalid rather
			 * than stopping the whole scan. */
			valid = false;
		}
	}

	return a->cat_cb(&ent, valid ? &inode : NULL, valid, a->cat_ctx);
}

enum mds_status catalogue_rondb_ns_readdir_plus(
	struct mds_catalogue *cat,
	uint64_t parent_fileid,
	const char *start_after,
	uint32_t max_entries,
	mds_readdir_plus_cb cb, void *ctx)
{
	void *h = rondb_handle(cat);
	struct rondb_readdir_plus_adapt adapt;
	int rc;

	if (h == NULL || cb == NULL) {
		return MDS_ERR_INVAL;
	}

	adapt.cat_cb    = cb;
	adapt.cat_ctx   = ctx;
	adapt.cb_status = MDS_OK;

	rc = rondb_shim_ns_readdir_plus(h, parent_fileid, start_after,
					max_entries,
					rondb_readdir_plus_shim_cb,
					&adapt);
	if (rc != 0) {
		return MDS_ERR_IO;
	}

	return MDS_OK;
}

/* Fused readdir_plus resumed by a child-fileid cursor (ascending
 * fileid order, O(log N + page) on RonDB).  Reuses the same shim
 * adapter as the name-order variant. */
static enum mds_status catalogue_rondb_ns_readdir_plus_from(
	struct mds_catalogue *cat,
	uint64_t parent_fileid,
	uint64_t start_after_fileid,
	uint32_t max_entries,
	mds_readdir_plus_cb cb, void *ctx)
{
	void *h = rondb_handle(cat);
	struct rondb_readdir_plus_adapt adapt;
	int rc;

	if (h == NULL || cb == NULL) {
		return MDS_ERR_INVAL;
	}

	adapt.cat_cb    = cb;
	adapt.cat_ctx   = ctx;
	adapt.cb_status = MDS_OK;

	rc = rondb_shim_ns_readdir_plus_from(h, parent_fileid,
					     start_after_fileid,
					     max_entries,
					     rondb_readdir_plus_shim_cb,
					     &adapt);
	if (rc != 0) {
		return MDS_ERR_IO;
	}

	return MDS_OK;
}

/* -----------------------------------------------------------------------
 * Link -- create dirent + bump target nlink atomically
 * ----------------------------------------------------------------------- */

enum mds_status catalogue_rondb_ns_link(
	struct mds_catalogue *cat,
	uint64_t parent_fileid,
	const char *name,
	uint64_t target_fileid)
{
	void *h = rondb_handle(cat);
	struct mds_inode parent, target;
	uint32_t parent_shard = 0, target_shard = 0;
	uint8_t parent_buf[RONDB_INODE_MAX_SIZE];
	uint8_t target_buf[RONDB_INODE_MAX_SIZE];
	uint32_t outlen = 0;
	struct timespec now;
	int rc;

	if (h == NULL || name == NULL) {
		return MDS_ERR_INVAL;
	}

	/* Read parent inode (validation only: must exist and be a dir). */
	rc = rondb_shim_inode_get(h, parent_fileid, parent_buf,
				  sizeof(parent_buf), &outlen, 0);
	if (rc != 0) {
		return (rc == 1) ? MDS_ERR_NOTFOUND : MDS_ERR_IO;
	}
	if (rondb_inode_deserialize(parent_buf, outlen,
				   &parent, NULL) != 0) {
		return MDS_ERR_IO;
	}
	if (parent.type != MDS_FTYPE_DIR) {
		return MDS_ERR_NOTDIR;
	}

	/* Read target inode. */
	rc = rondb_shim_inode_get(h, target_fileid, target_buf,
				  sizeof(target_buf), &outlen, 0);
	if (rc != 0) {
		return (rc == 1) ? MDS_ERR_NOTFOUND : MDS_ERR_IO;
	}
	if (rondb_inode_deserialize(target_buf, outlen,
				   &target, &target_shard) != 0) {
		return MDS_ERR_IO;
	}
	if (target.type == MDS_FTYPE_DIR) {
		return MDS_ERR_ISDIR;
	}

	/* Prepare target inode update (nlink bump). */
	clock_gettime(CLOCK_REALTIME, &now);
	target.nlink++;
	target.ctime = now;
	target.change++;

	/* Serialize target inode only (parent updated atomically by shim). */
	if (rondb_inode_serialize(&target, target_shard,
				  target_buf, sizeof(target_buf)) < 0) {
		return MDS_ERR_IO;
	}

	rc = rondb_shim_ns_link(h, parent_fileid, name,
				target_fileid, (uint8_t)target.type,
				target_buf, RONDB_INODE_FIXED_SIZE);
	if (rc == 1) {
		return MDS_ERR_EXISTS;
	}
	if (rc != 0) {
		return MDS_ERR_IO;
	}

	return MDS_OK;
}

/* -----------------------------------------------------------------------
 * nlink_adjust -- standalone atomic shim helper
 * ----------------------------------------------------------------------- */

enum mds_status catalogue_rondb_ns_nlink_adjust(
	struct mds_catalogue *cat,
	uint64_t fileid,
	int32_t delta)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_ns_nlink_adjust(h, fileid, delta);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_stripe_map_scan(
	struct mds_catalogue *cat,
	mds_cat_stripe_map_scan_cb cb, void *ctx)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || cb == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_stripe_map_scan(h,
		(rondb_stripe_map_scan_cb)cb, ctx);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/*
 * stripe_map serialization layout (RonDB shim contract).
 *
 * The buffer is a flat array of `stripe_count` RONDB_STRIPE_ENTRY_SIZE
 * records.  mirror_count is stored as a separate scalar in the
 * stripe_map row but the per-mirror DS entries are NOT serialized
 * here -- the buffer length is sized strictly by stripe_count.  This
 * matches today's HPC-Shared usage (mirror_count == 1, enforced by
 * hpc_shared_create_wide_layout's NOSUPPORT guard at
 * src/mds/hpc_shared.c:368) and the pre-HPC legacy 1x1 placement.
 *
 * QA review Blocker 6: future support for mirror_count > 1 wide
 * layouts will need to widen this serialization to
 * stripe_count * mirror_count entries (and corresponding shim
 * changes), or migrate to a row-per-(stripe, mirror) layout.  The
 * mirror_count > 1 NOSUPPORT guard at the helper layer is the only
 * thing keeping this in sync today; do NOT remove the guard until
 * this serialization is widened and tested.
 */
enum mds_status catalogue_rondb_stripe_map_get(
	struct mds_catalogue *cat, uint64_t fileid,
	uint32_t *stripe_count, uint32_t *stripe_unit,
	uint32_t *mirror_count,
	struct mds_ds_map_entry **entries)
{
	void *h = rondb_handle(cat);
	uint8_t *buf = NULL;
	uint32_t buflen;
	uint32_t outlen = 0;
	uint32_t sc = 0, su = 0, mc = 0;
	int rc;

	if (h == NULL || stripe_count == NULL || stripe_unit == NULL ||
	    mirror_count == NULL || entries == NULL) {
		return MDS_ERR_INVAL;
	}

	/* Allocate buffer for worst case (MDS_MAX_STRIPES entries).
	 * stripe_count entries, NOT stripe_count * mirror_count -- see
	 * the file-level note above (QA review Blocker 6). */
	buflen = MDS_MAX_STRIPES * RONDB_STRIPE_ENTRY_SIZE;
	buf = malloc(buflen);
	if (buf == NULL) {
		return MDS_ERR_NOMEM;
	}

	rc = rondb_shim_stripe_get(h, fileid, &sc, &su, &mc,
				   buf, buflen, &outlen);
	if (rc == 1) {
		free(buf);
		return MDS_ERR_NOTFOUND;
	}
	if (rc != 0) {
		free(buf);
		return MDS_ERR_IO;
	}

	*stripe_count = sc;
	*stripe_unit  = su;
	*mirror_count = mc;

	if (sc == 0) {
		*entries = NULL;
		free(buf);
		return MDS_OK;
	}

	/* Deserialize entries from flat buffer into mds_ds_map_entry array. */
	{
		struct mds_ds_map_entry *ents;
		uint32_t i;

		ents = calloc(sc, sizeof(*ents));
		if (ents == NULL) {
			free(buf);
			return MDS_ERR_NOMEM;
		}

		for (i = 0; i < sc; i++) {
			if (rondb_stripe_entry_deserialize(
				buf + (i * RONDB_STRIPE_ENTRY_SIZE),
				RONDB_STRIPE_ENTRY_SIZE, &ents[i]) != 0) {
				free(ents);
				free(buf);
				return MDS_ERR_IO;
			}
		}

		*entries = ents;
	}

	free(buf);
	return MDS_OK;
}

enum mds_status catalogue_rondb_stripe_map_put(
	struct mds_catalogue *cat, uint64_t fileid,
	uint32_t stripe_count, uint32_t stripe_unit,
	uint32_t mirror_count,
	const struct mds_ds_map_entry *entries)
{
	void *h = rondb_handle(cat);
	uint8_t *buf = NULL;
	uint32_t buflen;
	int rc;

	if (h == NULL) {
		return MDS_ERR_INVAL;
	}
	if (stripe_count > 0 && entries == NULL) {
		return MDS_ERR_INVAL;
	}
	if (stripe_count > MDS_MAX_STRIPES) {
		return MDS_ERR_INVAL;
	}

	/* Serialize entries into flat buffer for the shim. */
	buflen = stripe_count * RONDB_STRIPE_ENTRY_SIZE;
	if (buflen > 0) {
		uint32_t i;

		buf = malloc(buflen);
		if (buf == NULL) {
			return MDS_ERR_NOMEM;
		}

		for (i = 0; i < stripe_count; i++) {
			if (rondb_stripe_entry_serialize(
				&entries[i],
				buf + (i * RONDB_STRIPE_ENTRY_SIZE),
				RONDB_STRIPE_ENTRY_SIZE) < 0) {
				free(buf);
				return MDS_ERR_IO;
			}
		}
	}

	/* Retry on transient NDB errors (lock contention / deadlock).
	 * Same pattern as catalogue_rondb_ns_create. */
	for (int attempt = 0; attempt < 3; attempt++) {
		rc = rondb_shim_stripe_put(h, fileid, stripe_count,
					   stripe_unit, mirror_count,
					   buf, buflen);
		if (rc != -2) {
			break;
		}
		usleep(500 * (uint32_t)(attempt + 1));
	}
	free(buf);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_stripe_map_del(
	struct mds_catalogue *cat, uint64_t fileid)
{
	void *h = rondb_handle(cat);
	int rc = -1;

	if (h == NULL) {
		return MDS_ERR_INVAL;
	}

	/*
	 * Retry on transient NDB errors (lock contention).  The shim uses
	 * batched PK deletes instead of an exclusive scan.  stripe_map_del
	 * is idempotent; ds_gc may call it after ns_remove already removed
	 * the rows in the same transaction as the namespace delete.
	 */
	for (int attempt = 0; attempt < 3; attempt++) {
		rc = rondb_shim_stripe_del(h, fileid, MDS_MAX_STRIPES);
		if (rc == 0) {
			break;
		}
		usleep(500 * (uint32_t)(attempt + 1));
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Extended attributes (composite PK: fileid + attr_name)
 * ----------------------------------------------------------------------- */

enum mds_status catalogue_rondb_xattr_get(
	struct mds_catalogue *cat, uint64_t fileid, const char *name,
	void **val, uint32_t *vallen)
{
	void *h = rondb_handle(cat);
	uint8_t *buf = NULL;
	uint32_t outlen = 0;
	int rc;

	if (h == NULL || name == NULL || val == NULL || vallen == NULL) {
		return MDS_ERR_INVAL;
	}

	buf = malloc(MDS_XATTR_VAL_MAX);
	if (buf == NULL) {
		return MDS_ERR_NOMEM;
	}

	rc = rondb_shim_xattr_get(h, fileid, name,
				  buf, MDS_XATTR_VAL_MAX, &outlen);
	if (rc == 1) {
		free(buf);
		return MDS_ERR_NOTFOUND;
	}
	if (rc != 0) {
		free(buf);
		return MDS_ERR_IO;
	}

	/* Shrink allocation to actual size. */
	if (outlen < MDS_XATTR_VAL_MAX) {
		uint8_t *trimmed = realloc(buf, outlen > 0 ? outlen : 1);
		if (trimmed != NULL) {
			buf = trimmed;
		}
	}
	*val = buf;
	*vallen = outlen;
	return MDS_OK;
}

enum mds_status catalogue_rondb_xattr_put(
	struct mds_catalogue *cat, uint64_t fileid, const char *name,
	const void *val, uint32_t vallen)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || name == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_xattr_put(h, fileid, name, val, vallen);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_xattr_del(
	struct mds_catalogue *cat, uint64_t fileid, const char *name)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || name == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_xattr_del(h, fileid, name);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

struct rondb_xattr_list_adapter {
	mds_xattr_list_cb cb;
	void             *ctx;
};

static int rondb_xattr_list_trampoline(const char *name, uint32_t name_len,
				       void *arg)
{
	struct rondb_xattr_list_adapter *a = arg;
	return a->cb(name, (size_t)name_len, a->ctx);
}

enum mds_status catalogue_rondb_xattr_list(
	struct mds_catalogue *cat, uint64_t fileid,
	mds_xattr_list_cb cb, void *ctx)
{
	void *h = rondb_handle(cat);
	struct rondb_xattr_list_adapter adapter;
	int rc;

	if (h == NULL || cb == NULL) {
		return MDS_ERR_INVAL;
	}

	adapter.cb  = cb;
	adapter.ctx = ctx;
	rc = rondb_shim_xattr_list(h, fileid,
				   rondb_xattr_list_trampoline, &adapter);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_xattr_exists(
	struct mds_catalogue *cat, uint64_t fileid, const char *name)
{
	void *h = rondb_handle(cat);
	uint8_t tmp[1];
	uint32_t outlen = 0;
	int rc;

	if (h == NULL || name == NULL) {
		return MDS_ERR_INVAL;
	}

	/* exists = try to read; NOTFOUND means doesn't exist. */
	rc = rondb_shim_xattr_get(h, fileid, name, tmp, 0, &outlen);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * DS registry (typed columns, no blob serialisation)
 * ----------------------------------------------------------------------- */

enum mds_status catalogue_rondb_ds_get(
	struct mds_catalogue *cat, uint32_t ds_id,
	struct mds_ds_info *info)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || info == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_ds_registry_get(h, ds_id, info);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_ds_put(
	struct mds_catalogue *cat,
	const struct mds_ds_info *info)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || info == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_ds_registry_put(h, info);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_ds_del(
	struct mds_catalogue *cat, uint32_t ds_id)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_ds_registry_del(h, ds_id);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_ds_list(
	struct mds_catalogue *cat,
	struct mds_ds_info **list, uint32_t *count)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || list == NULL || count == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_ds_registry_list(h, list, count);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Phase 8A C wrappers -- DS provisioning
 * ----------------------------------------------------------------------- */

static enum mds_status catalogue_rondb_ds_provision_get(
	struct mds_catalogue *cat, uint32_t ds_id,
	uint8_t *secret, uint32_t secret_len, uint64_t *epoch)
{
	void *h = rondb_handle(cat);
	uint32_t outlen = 0;
	int rc;

	if (h == NULL || epoch == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_ds_provision_get(h, ds_id, secret, secret_len,
					 &outlen, epoch);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_ds_provision_put(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint32_t ds_id, const uint8_t *secret,
	uint32_t secret_len, uint64_t epoch)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_ds_provision_put(h, ds_id, secret, secret_len, epoch);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_ds_provision_del(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint32_t ds_id)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_ds_provision_del(h, ds_id);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Phase 8A C wrappers -- Quota
 * ----------------------------------------------------------------------- */

static enum mds_status catalogue_rondb_quota_rule_get(
	struct mds_catalogue *cat, uint8_t scope_type,
	uint64_t scope_id, struct mds_quota_rule *rule)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || rule == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_quota_rule_get(h, scope_type, scope_id, rule);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_quota_rule_put(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint8_t scope_type, uint64_t scope_id,
	const struct mds_quota_rule *rule)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL || rule == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_quota_rule_put(h, scope_type, scope_id, rule);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_quota_usage_get(
	struct mds_catalogue *cat, uint8_t usage_type,
	uint64_t scope_id, struct mds_quota_usage *usage)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || usage == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_quota_usage_get(h, usage_type, scope_id, usage);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_quota_usage_put(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint8_t usage_type, uint64_t scope_id,
	const struct mds_quota_usage *usage)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL || usage == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_quota_usage_put(h, usage_type, scope_id, usage);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Phase 8A C wrappers -- GC queue
 * ----------------------------------------------------------------------- */

static enum mds_status catalogue_rondb_gc_enqueue(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t fileid, uint32_t ds_id,
	const uint8_t *nfs_fh, uint32_t fh_len)
{
	void *h = rondb_handle(cat);
	uint64_t seq = 0;
	int rc;

	(void)txn;
	if (h == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_gc_seq_alloc(h, &seq);
	if (rc != 0) {
		return MDS_ERR_IO;
	}

	rc = rondb_shim_gc_enqueue(h, seq, fileid, ds_id, nfs_fh, fh_len);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_gc_peek(
	struct mds_catalogue *cat, struct mds_gc_entry *entry)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || entry == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_gc_peek(h, entry);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_gc_dequeue(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t gc_seq)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_gc_dequeue(h, gc_seq);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_gc_count(
	struct mds_catalogue *cat, uint32_t *count)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || count == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_gc_count(h, count);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_gc_peek_batch(
	struct mds_catalogue *cat, struct mds_gc_entry *entries,
	uint32_t cap, uint32_t *n_out)
{
	void *h = rondb_handle(cat);
	int rc;

	if (n_out != NULL) {
		*n_out = 0;
	}
	if (h == NULL || entries == NULL || cap == 0 || n_out == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_gc_peek_batch(h, entries, cap, n_out);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Phase 8A C wrappers -- Inline data via xattr table
 *
 * Symlink targets and other small metadata blobs are stored in the
 * mds_xattrs table under a reserved key that cannot collide with
 * user-visible xattrs (NFS user xattrs are prefixed "user.").
 * This avoids adding a separate inline_data table to the RonDB schema.
 * ----------------------------------------------------------------------- */

/** Reserved xattr key for inline data (symlink targets). */
#define RONDB_INLINE_XATTR_KEY  "__inline_data"

static enum mds_status catalogue_rondb_inline_get(
	struct mds_catalogue *cat, uint64_t fileid,
	void *buf, uint32_t buflen, uint32_t *outlen)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || buf == NULL || outlen == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_xattr_get(h, fileid, RONDB_INLINE_XATTR_KEY,
				  buf, buflen, outlen);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_inline_put(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t fileid, const void *buf, uint32_t len)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_xattr_put(h, fileid, RONDB_INLINE_XATTR_KEY,
				  buf, len);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_inline_del(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t fileid)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_xattr_del(h, fileid, RONDB_INLINE_XATTR_KEY);
	/* Ignore NOTFOUND -- deleting non-existent inline data is fine. */
	if (rc == 1) {
		return MDS_OK;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Phase 8A C wrappers -- Shared 2PC journal (coordination ops)
 * ----------------------------------------------------------------------- */

static enum mds_status catalogue_rondb_journal_put(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	const struct mds_coord_journal_record *record)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL || record == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_journal_put(h, record);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_journal_get(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t txn_id, uint8_t role,
	struct mds_coord_journal_record *record)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL || record == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_journal_get(h, txn_id, role, record);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_journal_del(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t txn_id, uint8_t role)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_journal_del(h, txn_id, role);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_journal_scan(
	struct mds_catalogue *cat,
	mds_coord_journal_scan_cb cb, void *ctx)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || cb == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_journal_scan(h, cb, ctx);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Phase 8A C wrappers -- Layout state (coordination ops)
 * ----------------------------------------------------------------------- */

static enum mds_status catalogue_rondb_layout_grant(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t clientid, uint64_t fileid, uint32_t iomode,
	uint64_t offset, uint64_t length,
	const struct nfs4_stateid *stateid,
	const uint32_t *ds_ids, uint32_t ds_count)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL || stateid == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_layout_state_put(h, stateid->other,
					 clientid, fileid,
					 iomode, offset, length,
					 stateid->seqid,
					 ds_ids, ds_count);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/**
 * Phase 2: Fused stripe_get + layout_grant in one NDB txn.
 * Saves 1 NDB round-trip compared to separate calls.
 */
enum mds_status catalogue_rondb_layoutget_fused(
	struct mds_catalogue *cat, uint64_t fileid,
	uint32_t *stripe_count, uint32_t *stripe_unit,
	uint32_t *mirror_count, struct mds_ds_map_entry **entries,
	const struct nfs4_stateid *stateid,
	uint64_t clientid, uint32_t iomode, uint64_t offset,
	uint64_t length, uint32_t mds_id)
{
	void *h = rondb_handle(cat);
	/* Phase 2 of the QA plan -- heap-back the stripe entry buffer.
	 * The legacy stack array `uint8_t buf[MDS_MAX_LAYOUT_DS *
	 * RONDB_STRIPE_ENTRY_SIZE]` only provisioned 16 entries; for a
	 * 1024-stripe HPC layout the shim could not return the full map.
	 * Mirror the pattern in catalogue_rondb_stripe_map_get (line
	 * ~1298): allocate MDS_MAX_STRIPES * MDS_MAX_MIRRORS *
	 * RONDB_STRIPE_ENTRY_SIZE bytes (~1 MiB worst case) on the
	 * heap.  Allocation failure surfaces as MDS_ERR_NOMEM. */
	uint8_t *buf = NULL;
	size_t buf_cap = (size_t)MDS_MAX_STRIPES *
			 (size_t)MDS_MAX_MIRRORS *
			 RONDB_STRIPE_ENTRY_SIZE;
	uint32_t outlen = 0;
	uint32_t sc, su, mc, i, total;
	int rc;

	if (h == NULL || stateid == NULL || stripe_count == NULL ||
	    stripe_unit == NULL || mirror_count == NULL ||
	    entries == NULL) {
		return MDS_ERR_INVAL;
	}

	buf = malloc(buf_cap);
	if (buf == NULL) {
		return MDS_ERR_NOMEM;
	}

	/* Retry up to 3 times on transient NDB errors (lock contention,
	 * temporary resource exhaustion from concurrent multi-MDS ops). */
	for (int attempt = 0; attempt < 3; attempt++) {
		rc = rondb_shim_stripe_get_and_layout_grant(
			h, fileid, &sc, &su, &mc, buf, (uint32_t)buf_cap, &outlen,
			stateid->other, clientid, iomode, offset, length,
			stateid->seqid, mds_id);
		if (rc != -2) {
			break;
		}
		usleep(500 * (uint32_t)(attempt + 1));
	}
	if (rc == 1) {
		free(buf);
		return MDS_ERR_NOTFOUND;
	}
	if (rc == -2) {
		free(buf);
		return MDS_ERR_DELAY; /* caller falls back to non-fused path */
	}
	if (rc != 0) {
		free(buf);
		return MDS_ERR_IO;
	}

	*stripe_count = sc;
	*stripe_unit  = su;
	*mirror_count = mc;

	total = sc * mc;
	if (total == 0) {
		*entries = NULL;
		free(buf);
		return MDS_OK;
	}

	*entries = calloc(total, sizeof(struct mds_ds_map_entry));
	if (*entries == NULL) {
		free(buf);
		return MDS_ERR_NOMEM;
	}

	for (i = 0; i < total && i * RONDB_STRIPE_ENTRY_SIZE < outlen; i++) {
		if (rondb_stripe_entry_deserialize(
			    buf + i * RONDB_STRIPE_ENTRY_SIZE,
			    RONDB_STRIPE_ENTRY_SIZE,
			    &(*entries)[i]) != 0) {
			free(*entries);
			*entries = NULL;
			free(buf);
			return MDS_ERR_IO;
		}
	}

	free(buf);
	return MDS_OK;
}

/**
 * Phase 3: ns_create with layout pre-grant (single NDB transaction).
 *
 * Uses rondb_shim_ns_create_with_layout which embeds CREATE +
 * stripe + layout_state + layout indexes all in one NDB transaction
 * (NoCommit flush + Commit = 2 NDB internal RTs, but 1 user-visible
 * wall-clock NDB transaction).  LAYOUTGET becomes 0 NDB RTs.
 */
enum mds_status catalogue_rondb_ns_create_with_layout(
	struct mds_catalogue *cat,
	uint64_t parent_fileid,
	const char *name,
	enum mds_file_type type,
	uint32_t mode, uint64_t uid, uint64_t gid,
	struct ds_prealloc_ctx *prealloc,
	struct mds_inode *out,
	uint64_t layout_clientid, uint32_t layout_iomode,
	uint64_t layout_offset, uint64_t layout_length,
	const struct nfs4_stateid *layout_stateid,
	uint32_t layout_mds_id,
	bool *layout_ok,
	struct mds_ds_map_entry *layout_entry_out)
{
	void *h = rondb_handle(cat);
	struct mds_inode child;
	struct timespec now;
	uint8_t child_buf[RONDB_INODE_MAX_SIZE];
	int32_t parent_nlink_delta;
	int rc;
	enum mds_status st;

	if (layout_ok != NULL) {
		*layout_ok = false;
	}
	if (layout_entry_out != NULL) {
		memset(layout_entry_out, 0, sizeof(*layout_entry_out));
	}
	if (h == NULL || name == NULL || out == NULL) {
		return MDS_ERR_INVAL;
	}

	/* Peek prealloc for DS ID + stripe data before pop. */
	uint32_t layout_ds_id = 0;
	uint32_t layout_ds_count = 0;
	uint8_t stripe_buf[256];
	uint32_t stripe_buf_len = 0;
	uint32_t stripe_count_for_create = 0;

	uint64_t child_fid = 0;
	if (type == MDS_FTYPE_REG && prealloc != NULL) {
		struct mds_ds_map_entry ds_entry;
		uint32_t stripe_unit = 0;
		uint64_t prealloc_fid = 0;
		if (ds_prealloc_peek(prealloc, &ds_entry, &stripe_unit) == 0) {
			layout_ds_id = ds_entry.ds_id;
			layout_ds_count = 1;
		}
		/* Pop and encode stripe data + pre-allocated fileid+FH. */
		if (ds_prealloc_pop(prealloc, &ds_entry, &stripe_unit,
				    &prealloc_fid) == 0) {
			if (prealloc_fid != 0) {
				child_fid = prealloc_fid;
			}
			fdb_put_u32(stripe_buf, ds_entry.ds_id);
			fdb_put_u32(stripe_buf + 4, ds_entry.nfs_fh_len);
			if (ds_entry.nfs_fh_len > 0) {
				memcpy(stripe_buf + 8, ds_entry.nfs_fh,
				       ds_entry.nfs_fh_len);
			}
			stripe_buf_len = 8 + ds_entry.nfs_fh_len;
			stripe_count_for_create = 1;

			/*
			 * Surface the popped entry to the caller so a
			 * follow-up LAYOUTGET in the same compound can
			 * skip stripe_map_get's NDB read.  Only meaningful
			 * when an FH was captured by ds_prealloc_pop --
			 * pre-Phase-12 / proxy-less paths leave nfs_fh_len
			 * at zero and the caller falls back to the legacy
			 * DS_PENDING flow.
			 */
			if (layout_entry_out != NULL) {
				*layout_entry_out = ds_entry;
			}
		}
	}

	/* Allocate fileid (skip if pre-allocated from pool). */
	if (child_fid == 0) {
		st = catalogue_rondb_alloc_fileid(cat, &child_fid);
		if (st != MDS_OK) {
			return st;
		}
	}

	/* Build child inode. */
	clock_gettime(CLOCK_REALTIME, &now);
	memset(&child, 0, sizeof(child));
	child.fileid = child_fid;
	child.type = type;
	child.mode = mode;
	child.uid = uid;
	child.gid = gid;
	child.atime = now;
	child.mtime = now;
	child.ctime = now;
	child.change = 1;
	child.generation = 1;
	child.parent_fileid = parent_fileid;
	child.nlink = (type == MDS_FTYPE_DIR) ? 2 : 1;
	/* Only set DS_PENDING if stripe has no pre-captured FH. */
	if (stripe_count_for_create > 0 && stripe_buf_len <= 8) {
		child.flags |= MDS_IFLAG_DS_PENDING;
	}
	parent_nlink_delta = (type == MDS_FTYPE_DIR) ? 1 : 0;

	if (rondb_inode_serialize(&child, 0, child_buf,
				  sizeof(child_buf)) < 0) {
		return MDS_ERR_IO;
	}

	/* Single fused NDB transaction: create + stripe + layout. */
	for (int attempt = 0; attempt < RONDB_TRANSIENT_RETRIES; attempt++) {
		rc = rondb_shim_ns_create_with_layout(
			h, parent_fileid, name,
			child_buf, RONDB_INODE_FIXED_SIZE,
			parent_nlink_delta,
			stripe_count_for_create > 0 ? stripe_buf : NULL,
			stripe_buf_len, stripe_count_for_create,
			layout_clientid, layout_iomode,
			layout_offset, layout_length,
			layout_stateid ? layout_stateid->other : NULL,
			layout_stateid ? layout_stateid->seqid : 0,
			layout_ds_count > 0 ? &layout_ds_id : NULL,
			layout_ds_count, layout_mds_id);
		if (rc != -2) {
			break;
		}
		rondb_transient_backoff(attempt);
	}
	if (rc == 1) {
		return MDS_ERR_EXISTS;
	}
	if (rc != 0) {
		return MDS_ERR_IO;
	}

	*out = child;
	if (layout_ok != NULL && layout_clientid != 0) {
		*layout_ok = true;
	}
	catalog_stat_inc(&cat->stats.authority_writes);
	return MDS_OK;
}

static enum mds_status catalogue_rondb_layout_return(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	const uint8_t stateid_other[12],
	uint64_t clientid, uint64_t fileid,
	const uint32_t *ds_ids, uint32_t ds_count)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL || stateid_other == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_layout_state_del(h, stateid_other,
					 clientid, fileid,
					 ds_ids, ds_count);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_layout_get_by_stateid(
	struct mds_catalogue *cat,
	const uint8_t stateid_other[12],
	uint64_t *clientid, uint64_t *fileid,
	uint32_t *iomode, uint64_t *offset,
	uint64_t *length, uint32_t *seqid)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || stateid_other == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_layout_get_by_stateid(h, stateid_other,
					      clientid, fileid,
					      iomode, offset,
					      length, seqid);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_layout_scan_for_file(
	struct mds_catalogue *cat, uint64_t fileid, bool *has_layout)
{
	void *h = rondb_handle(cat);
	int found = 0;
	int rc;

	if (h == NULL || has_layout == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_layout_scan_for_file(h, fileid, &found);
	if (rc != 0) {
		return MDS_ERR_IO;
	}
	*has_layout = (found != 0);
	return MDS_OK;
}

static enum mds_status catalogue_rondb_layout_del_all_for_client(
	struct mds_catalogue *cat, uint64_t clientid)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_layout_del_all_for_client(h, clientid);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Phase 8A+ C wrappers -- ds_layout_idx_scan, layout_iter_file
 * ----------------------------------------------------------------------- */

static enum mds_status catalogue_rondb_ds_layout_idx_scan(
	struct mds_catalogue *cat, uint32_t ds_id,
	mds_coord_ds_layout_cb cb, void *ctx)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || cb == NULL) {
		return MDS_ERR_INVAL;
	}
	rc = rondb_shim_ds_layout_idx_scan(h, ds_id, cb, ctx);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/**
 * Adapter: the RonDB shim callback delivers raw stateid_other + seqid;
 * construct a transient nfs4_stateid for the coordination-layer callback.
 */
struct rondb_layout_iter_adapt {
	mds_coord_layout_file_iter_cb cb;
	void *ctx;
};

static int rondb_layout_iter_adapt_cb(uint64_t clientid,
				      const uint8_t *stateid_other,
				      uint32_t seqid, uint32_t iomode,
				      void *arg)
{
	struct rondb_layout_iter_adapt *a = arg;
	struct nfs4_stateid sid;

	memset(&sid, 0, sizeof(sid));
	memcpy(sid.other, stateid_other, 12);
	sid.seqid = seqid;
	return a->cb(clientid, &sid, iomode, a->ctx);
}

static enum mds_status catalogue_rondb_layout_iter_file(
	struct mds_catalogue *cat, uint64_t fileid,
	mds_coord_layout_file_iter_cb cb, void *ctx)
{
	void *h = rondb_handle(cat);
	struct rondb_layout_iter_adapt adapt;
	int rc;

	if (h == NULL || cb == NULL) {
		return MDS_ERR_INVAL;
	}
	adapt.cb = cb;
	adapt.ctx = ctx;
	rc = rondb_shim_layout_iter_file(h, fileid,
					 rondb_layout_iter_adapt_cb,
					 &adapt);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Phase 8A C wrappers -- Client recovery (coordination ops)
 * ----------------------------------------------------------------------- */

static enum mds_status catalogue_rondb_recovery_put(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t clientid, const uint8_t *co_ownerid,
	uint32_t co_ownerid_len, const uint8_t verifier[8])
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL || verifier == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_recovery_put(h, clientid, co_ownerid,
				     co_ownerid_len, verifier);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_recovery_del(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t clientid)
{
	void *h = rondb_handle(cat);
	int rc;

	(void)txn;
	if (h == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_recovery_del(h, clientid);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_recovery_get(
	struct mds_catalogue *cat, uint64_t clientid,
	uint8_t *co_ownerid, uint32_t *co_ownerid_len,
	uint8_t verifier[8])
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || co_ownerid_len == NULL || verifier == NULL) {
		return MDS_ERR_INVAL;
	}

	rc = rondb_shim_recovery_get(h, clientid, co_ownerid,
				     co_ownerid_len, verifier);
	if (rc == 1) {
		return MDS_ERR_NOTFOUND;
	}
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Phase 8B -- Vtable wrappers matching authority_ops signature conventions
 *
 * The authority_ops vtable passes struct mds_cat_txn *txn as an extra
 * parameter.  For the RonDB backend each operation is self-contained
 * (atomicity via NDB transaction), so txn is unused.
 * ----------------------------------------------------------------------- */

static enum mds_status rondb_auth_ns_create(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t parent, const char *name, enum mds_file_type type,
	uint32_t mode, uint64_t uid, uint64_t gid,
	struct ds_prealloc_ctx *prealloc, struct mds_inode *out)
{
	(void)txn;
	return catalogue_rondb_ns_create(cat, parent, name, type,
					mode, uid, gid, prealloc, out);
}

static enum mds_status rondb_auth_ns_remove(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t parent, const char *name)
{
	(void)txn;
	return catalogue_rondb_ns_remove(cat, parent, name);
}

static enum mds_status rondb_auth_ns_remove_known(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t parent, const char *name,
	const struct mds_inode *child)
{
	(void)txn;
	return catalogue_rondb_ns_remove_known(cat, parent, name, child);
}

static enum mds_status rondb_auth_ns_rename(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t src_parent, const char *src_name,
	uint64_t dst_parent, const char *dst_name)
{
	(void)txn;
	return catalogue_rondb_ns_rename(cat, src_parent, src_name,
					dst_parent, dst_name);
}

static enum mds_status rondb_auth_ns_link(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t parent, const char *name, uint64_t target)
{
	(void)txn;
	return catalogue_rondb_ns_link(cat, parent, name, target);
}

static enum mds_status rondb_auth_ns_setattr(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t fileid, const struct mds_inode *attrs, uint32_t mask)
{
	(void)txn;
	return catalogue_rondb_ns_setattr(cat, fileid, attrs, mask);
}

static enum mds_status rondb_auth_ns_readdir(
	struct mds_catalogue *cat, uint64_t parent,
	const char *start_after, uint32_t max_entries,
	struct mds_cat_txn *txn,
	mds_readdir_cb cb, void *ctx)
{
	(void)txn;
	return catalogue_rondb_ns_readdir(cat, parent, start_after,
					 max_entries, cb, ctx);
}

static enum mds_status rondb_auth_dirent_name_for_child(
	struct mds_catalogue *cat, uint64_t parent,
	uint64_t child_fileid, char *name_out, size_t name_out_len)
{
	return catalogue_rondb_dirent_name_for_child(cat, parent,
						     child_fileid,
						     name_out, name_out_len);
}

static enum mds_status rondb_auth_ns_readdir_plus(
	struct mds_catalogue *cat, uint64_t parent,
	const char *start_after, uint32_t max_entries,
	struct mds_cat_txn *txn,
	mds_readdir_plus_cb cb, void *ctx)
{
	(void)txn;
	return catalogue_rondb_ns_readdir_plus(cat, parent, start_after,
					       max_entries, cb, ctx);
}

static enum mds_status rondb_auth_ns_readdir_plus_from(
	struct mds_catalogue *cat, uint64_t parent,
	uint64_t start_after_fileid, uint32_t max_entries,
	struct mds_cat_txn *txn,
	mds_readdir_plus_cb cb, void *ctx)
{
	(void)txn;
	return catalogue_rondb_ns_readdir_plus_from(cat, parent,
						    start_after_fileid,
						    max_entries, cb, ctx);
}

static enum mds_status rondb_auth_alloc_fileid(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t *fileid)
{
	(void)txn;
	return catalogue_rondb_alloc_fileid(cat, fileid);
}

static enum mds_status rondb_auth_stripe_map_put(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t fileid, uint32_t sc, uint32_t su,
	uint32_t mc, const struct mds_ds_map_entry *entries)
{
	(void)txn;
	return catalogue_rondb_stripe_map_put(cat, fileid, sc, su,
					     mc, entries);
}

static enum mds_status rondb_auth_stripe_map_del(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t fileid)
{
	(void)txn;
	return catalogue_rondb_stripe_map_del(cat, fileid);
}

static enum mds_status rondb_auth_xattr_put(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t fileid, const char *name,
	const void *val, uint32_t vallen)
{
	void *h = rondb_handle(cat);
	(void)txn;
	if (h == NULL || name == NULL) {
		return MDS_ERR_INVAL;
	}
	/* Stage 7: atomic xattr + inode ctime/change touch. */
	int rc = rondb_shim_xattr_put_atomic(h, fileid, name, val, vallen);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status rondb_auth_xattr_del(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint64_t fileid, const char *name)
{
	void *h = rondb_handle(cat);
	(void)txn;
	if (h == NULL || name == NULL) {
		return MDS_ERR_INVAL;
	}
	/* Stage 7: atomic xattr + inode ctime/change touch. */
	int rc = rondb_shim_xattr_del_atomic(h, fileid, name);
	if (rc == 1) { return MDS_ERR_NOTFOUND; }
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status rondb_auth_ds_put(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	const struct mds_ds_info *info)
{
	(void)txn;
	return catalogue_rondb_ds_put(cat, info);
}

static enum mds_status rondb_auth_ds_del(
	struct mds_catalogue *cat, struct mds_cat_txn *txn,
	uint32_t ds_id)
{
	(void)txn;
	return catalogue_rondb_ds_del(cat, ds_id);
}

/* -----------------------------------------------------------------------
 * Phase 9B -- Lock-aware namespace mutation wrappers
 *
 * Each wrapper: build lock entries -> sort -> acquire in order ->
 * call underlying mutation -> release in reverse order.
 * On contention (acquire returns 1), return MDS_ERR_DELAY.
 * ----------------------------------------------------------------------- */

static struct mds_rondb_state *rondb_state(const struct mds_catalogue *cat)
{
    if (cat == NULL || cat->backend_private == NULL) {
        return NULL;
    }
    return (struct mds_rondb_state *)cat->backend_private;
}

/** Acquire a single lock.  Returns MDS_OK, MDS_ERR_DELAY, or MDS_ERR_IO. */
static enum mds_status rondb_lock_one(
    void *h, const struct mds_rondb_state *st,
    const struct rondb_lock_entry *le, uint8_t mode)
{
    int rc = rondb_shim_lock_acquire(
        h, le->partition_hint, le->resource_class,
        le->key, (uint32_t)le->key_len, mode,
        st->mds_id, st->boot_epoch, st->boot_epoch,
        st->lock_ttl_ms);
    if (rc == 1) { return MDS_ERR_DELAY; }
    return rc == 0 ? MDS_OK : MDS_ERR_IO;
}

/** Release a single lock (best-effort; failure logged by shim). */
static void rondb_unlock_one(
    void *h, const struct mds_rondb_state *st,
    const struct rondb_lock_entry *le)
{
    (void)rondb_shim_lock_release(
        h, le->resource_class,
        le->key, (uint32_t)le->key_len,
        st->mds_id, st->boot_epoch);
}

/** Acquire N locks in order; on failure, release already-held locks. */
static enum mds_status rondb_lock_set(
    void *h, const struct mds_rondb_state *st,
    struct rondb_lock_entry *locks, int count, uint8_t mode)
{
    int i;

    /* Sort into canonical acquisition order. */
    for (int a = 0; a < count - 1; a++) {
        for (int b = a + 1; b < count; b++) {
            if (rondb_lock_order_cmp(&locks[a], &locks[b]) > 0) {
                struct rondb_lock_entry tmp = locks[a];
                locks[a] = locks[b];
                locks[b] = tmp;
            }
        }
    }

    for (i = 0; i < count; i++) {
        enum mds_status st2 = rondb_lock_one(h, st, &locks[i], mode);
        if (st2 != MDS_OK) {
            /* Release locks acquired so far, in reverse. */
            for (int j = i - 1; j >= 0; j--) {
                rondb_unlock_one(h, st, &locks[j]);
            }
            return st2;
        }
    }
    return MDS_OK;
}

/** Release N locks in reverse order. */
static void rondb_unlock_set(
    void *h, const struct mds_rondb_state *st,
    const struct rondb_lock_entry *locks, int count)
{
    for (int i = count - 1; i >= 0; i--) {
        rondb_unlock_one(h, st, &locks[i]);
    }
}

/* -----------------------------------------------------------------------
 * Phase 9C -- Delta emission helper
 *
 * Called by each locked mutation wrapper after a successful mutation.
 * Builds a delta record and inserts it into mds_delta_broadcast.
 * Returns 0 on success, -1 on failure.  When changefeed is enabled,
 * a failed delta insert means the mutation must be reported as failed
 * to the caller -- otherwise authority and image diverge permanently.
 * ----------------------------------------------------------------------- */

static int rondb_emit_delta(
    struct mds_rondb_state *st, void *h,
    uint8_t delta_type, const void *payload, uint32_t payload_len)
{
    struct timespec ts;
    uint64_t now_ns;
    uint64_t persist_seqno = 0;
    int rc = -1;

    if (!st->changefeed_enabled) { return 0; }

    clock_gettime(CLOCK_REALTIME, &ts);
    now_ns = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;

    /* delta_seqno / delta_persist_counter are shared by every mutating
     * thread; serialise allocation + insert so two threads can never
     * claim the same seqno. */
    pthread_mutex_lock(&st->delta_mu);
    for (int attempt = 0; attempt < 128; attempt++) {
        st->delta_seqno++;
        rc = rondb_shim_delta_insert(
            h, st->mds_id, st->delta_seqno, st->boot_epoch,
            delta_type, payload, payload_len, now_ns);
        if (rc != 1) {
            break;
        }
        /* rc == 1: duplicate key.  A stale post-crash seqno collided
         * with a row from the previous incarnation; keep bumping so
         * the delta is emitted instead of silently dropped. */
    }
    if (rc != 0) {
        MDS_LOG_ERROR(LOG_COMP_CAT,
            "delta_insert failed for seqno=%llu type=%u rc=%d -- "
            "mutation will be reported as failed",
            (unsigned long long)st->delta_seqno, (unsigned)delta_type,
            rc);
        if (rc < 0) {
            /* Insert failed outright; the seqno was not consumed in
             * the table, so it is safe to reuse.  Duplicate-key
             * seqnos (rc == 1) stay consumed. */
            st->delta_seqno--;
        }
        pthread_mutex_unlock(&st->delta_mu);
        return -1;
    }

    /* Persist seqno counter periodically.  The save itself runs
     * outside the mutex: the startup load adds a
     * DELTA_PERSIST_INTERVAL safety margin, so persisting a slightly
     * stale value is harmless. */
    st->delta_persist_counter++;
    if (st->delta_persist_counter >= DELTA_PERSIST_INTERVAL) {
        persist_seqno = st->delta_seqno;
        st->delta_persist_counter = 0;
    }
    pthread_mutex_unlock(&st->delta_mu);

    if (persist_seqno != 0) {
        (void)rondb_shim_delta_seqno_save(h, st->mds_id, persist_seqno);
    }
    return 0;
}

/* --- Locked RENAME: TOPOLOGY + PARENT_NAME(src) + PARENT_NAME(dst) --- */

/*
 * Cross-MDS RENAME serialises on a single TOPOLOGY lock.  Prior
 * revisions also acquired PARENT_NAME(src) + PARENT_NAME(dst), but
 * those finer-grained locks were redundant: no other mutation path
 * (ns_create / ns_remove / ns_link / ns_setattr) acquires
 * PARENT_NAME, so the only writers they guarded against were other
 * renames, and the TOPOLOGY lock already serialises every rename
 * globally.  Dropping them eliminates 4 NDB round-trips per rename
 * (2 acquires + 2 releases) and keeps the correctness proof intact.
 *
 * NDB row locks on the dirent, child inode, and parent inode rows
 * acquired inside catalogue_rondb_ns_rename still protect against
 * concurrent non-rename mutations of the same rows.
 */
static enum mds_status rondb_locked_ns_rename(
    struct mds_catalogue *cat, struct mds_cat_txn *txn,
    uint64_t src_parent, const char *src_name,
    uint64_t dst_parent, const char *dst_name)
{
    struct mds_rondb_state *st = rondb_state(cat);
    void *h = rondb_handle(cat);
    struct rondb_lock_entry lock;
    enum mds_status rc;
    uint64_t child_fid = 0;
    uint8_t child_type = 0;

    if (st == NULL || h == NULL ||
        src_name == NULL || dst_name == NULL) {
        return MDS_ERR_INVAL;
    }

    /* TOPOLOGY (global singleton): serialises all cross-MDS
     * renames while the multi-read prologue of
     * catalogue_rondb_ns_rename runs.  See comment above. */
    lock.resource_class = RONDB_LOCK_CLASS_TOPOLOGY;
    lock.key_len = rondb_lock_res_topology(
        lock.key, sizeof(lock.key), &lock.partition_hint);
    if (lock.key_len <= 0) { return MDS_ERR_INVAL; }

    rc = rondb_lock_set(h, st, &lock, 1, RONDB_LOCK_MODE_EXCLUSIVE);
    if (rc != MDS_OK) { return rc; }

    rc = rondb_ns_rename_resolved(cat, src_parent, src_name,
                                  dst_parent, dst_name,
                                  &child_fid, &child_type);
    if (rc == MDS_OK && st->changefeed_enabled) {
        /* Emit DIRENT_DELETE(src) + DIRENT_PUT(dst) carrying the
         * child fileid/type resolved by the rename prologue.
         * Emitting fileid 0 here would make peer images map the
         * renamed name to fileid 0. */
        size_t snlen = strlen(src_name);
        uint8_t sbuf[17 + MDS_MAX_NAME];
        fdb_put_u64(sbuf, src_parent);
        fdb_put_u64(sbuf + 8, child_fid);
        sbuf[16] = child_type;
        memcpy(sbuf + 17, src_name, snlen);
        if (rondb_emit_delta(st, h, CAT_DELTA_DIRENT_DELETE,
                             sbuf, (uint32_t)(17 + snlen)) != 0) {
            rc = MDS_ERR_IO;
        }

        if (rc == MDS_OK) {
            size_t dnlen = strlen(dst_name);
            uint8_t dbuf[17 + MDS_MAX_NAME];
            fdb_put_u64(dbuf, dst_parent);
            fdb_put_u64(dbuf + 8, child_fid);
            dbuf[16] = child_type;
            memcpy(dbuf + 17, dst_name, dnlen);
            if (rondb_emit_delta(st, h, CAT_DELTA_DIRENT_PUT,
                                 dbuf, (uint32_t)(17 + dnlen)) != 0) {
                rc = MDS_ERR_IO;
            }
        }
    }
    rondb_unlock_set(h, st, &lock, 1);
    return rc;
}

/* -----------------------------------------------------------------------
 * Phase 9C -- Background delta broadcast poller thread
 *
 * Polls mds_delta_broadcast for foreign MDS streams and applies
 * INODE_UPSERT/DELETE + DIRENT_PUT/DELETE to the local catalog_image.
 * ----------------------------------------------------------------------- */

#define POLLER_MAX_FOREIGN 16
#define POLLER_BATCH       100
#define POLLER_TRIM_SEC    60

struct poller_apply_ctx {
    struct catalog_image *img;
    uint32_t              stream_id;
    uint64_t              max_applied;
};

static int poller_delta_cb(uint64_t seqno, uint64_t boot_epoch,
                           uint8_t delta_type,
                           const void *payload, uint32_t payload_len,
                           uint64_t timestamp_ns, void *ctx)
{
    struct poller_apply_ctx *pa = ctx;
    struct catalog_delta_record rec;

    (void)boot_epoch;
    (void)timestamp_ns;

    memset(&rec, 0, sizeof(rec));
    rec.type = delta_type;
    rec.stream_id = pa->stream_id;
    rec.seqno = seqno;
    rec.timestamp_ns = timestamp_ns;
    rec.payload_len = payload_len;
    rec.payload = payload;

    (void)catalog_image_apply(pa->img, &rec);
    if (seqno > pa->max_applied) {
        pa->max_applied = seqno;
    }
    return 0;
}

struct foreign_node {
    uint32_t mds_id;
    uint64_t boot_epoch;
    uint64_t last_applied;
};

static int collect_foreign_cb(uint32_t mds_id, uint64_t boot_epoch,
                              uint64_t last_heartbeat_ns, void *ctx)
{
    (void)last_heartbeat_ns;
    struct {
        struct foreign_node *nodes;
        uint32_t count;
        uint32_t self_id;
    } *state = ctx;

    if (mds_id == state->self_id) { return 0; }
    if (state->count >= POLLER_MAX_FOREIGN) { return 0; }

    state->nodes[state->count].mds_id = mds_id;
    state->nodes[state->count].boot_epoch = boot_epoch;
    state->nodes[state->count].last_applied = 0;
    state->count++;
    return 0;
}

static void *rondb_poller_thread(void *arg)
{
    struct mds_rondb_state *st = arg;
    struct foreign_node foreign[POLLER_MAX_FOREIGN];
    uint32_t foreign_count = 0;
    time_t last_discover = 0;
    time_t last_trim = 0;

    while (atomic_load(&st->poller_running)) {
        time_t now = time(NULL);

        /* Discover foreign MDS IDs every 30s. */
        if (now - last_discover >= 30 || foreign_count == 0) {
            struct {
                struct foreign_node *nodes;
                uint32_t count;
                uint32_t self_id;
            } dc = { .nodes = foreign, .count = 0,
                     .self_id = st->poller_self_mds_id };

            /* Preserve existing last_applied values. */
            struct foreign_node old[POLLER_MAX_FOREIGN];
            uint32_t old_count = foreign_count;
            memcpy(old, foreign, old_count * sizeof(old[0]));

            (void)rondb_shim_mds_scan_stale(st->handle, UINT64_MAX,
                                            collect_foreign_cb, &dc);
            foreign_count = dc.count;

            /* Restore last_applied for known MDS IDs. */
            for (uint32_t i = 0; i < foreign_count; i++) {
                for (uint32_t j = 0; j < old_count; j++) {
                    if (foreign[i].mds_id == old[j].mds_id) {
                        foreign[i].last_applied = old[j].last_applied;
                        break;
                    }
                }
            }
            last_discover = now;
        }

        /* Poll each foreign stream. */
        for (uint32_t i = 0; i < foreign_count; i++) {
            struct poller_apply_ctx pa = {
                .img = st->poller_image,
                .stream_id = foreign[i].mds_id,
                .max_applied = foreign[i].last_applied,
            };
            (void)rondb_shim_delta_poll(
                st->handle, foreign[i].mds_id,
                foreign[i].last_applied, POLLER_BATCH,
                poller_delta_cb, &pa);
            if (pa.max_applied > foreign[i].last_applied) {
                foreign[i].last_applied = pa.max_applied;
            }
        }

        /* Periodic age-based trim (every 60s). */
        if (now - last_trim >= POLLER_TRIM_SEC && foreign_count > 0) {
            uint64_t min_applied = UINT64_MAX;
            for (uint32_t i = 0; i < foreign_count; i++) {
                if (foreign[i].last_applied < min_applied) {
                    min_applied = foreign[i].last_applied;
                }
            }
            if (min_applied > 0 && min_applied != UINT64_MAX) {
                for (uint32_t i = 0; i < foreign_count; i++) {
                    (void)rondb_shim_delta_trim(
                        st->handle, foreign[i].mds_id,
                        min_applied);
                }
            }
            last_trim = now;
        }

        /* Sleep poll interval. */
        usleep(st->poller_interval_ms * 1000U);
    }

    /* Persist delta seqno on shutdown.  Read the counter under its
     * mutex; mutating threads may still be emitting deltas. */
    if (st->changefeed_enabled) {
        uint64_t final_seqno;

        pthread_mutex_lock(&st->delta_mu);
        final_seqno = st->delta_seqno;
        pthread_mutex_unlock(&st->delta_mu);
        (void)rondb_shim_delta_seqno_save(st->handle, st->mds_id,
                                          final_seqno);
    }

    return NULL;
}

int catalogue_rondb_poller_start(struct mds_catalogue *cat,
                                 struct catalog_image *img,
                                 uint32_t self_mds_id,
                                 uint32_t poll_interval_ms)
{
    struct mds_rondb_state *st;

    if (cat == NULL || img == NULL) { return -1; }
    st = rondb_state(cat);
    if (st == NULL) { return -1; }

    st->poller_image = img;
    st->poller_self_mds_id = self_mds_id;
    st->poller_interval_ms = (poll_interval_ms > 0) ? poll_interval_ms : 50;
    atomic_store(&st->poller_running, true);

    if (pthread_create(&st->poller_thread, NULL,
                       rondb_poller_thread, st) != 0) {
        atomic_store(&st->poller_running, false);
        return -1;
    }

    return 0;
}

void catalogue_rondb_poller_stop(struct mds_catalogue *cat)
{
    struct mds_rondb_state *st;

    if (cat == NULL) { return; }
    st = rondb_state(cat);
    if (st == NULL || !atomic_load(&st->poller_running)) { return; }

    atomic_store(&st->poller_running, false);
    pthread_join(st->poller_thread, NULL);
    st->poller_image = NULL;
}

/* -----------------------------------------------------------------------
 * Phase 8B -- RonDB authority vtable (single-MDS, no locking)
 * ----------------------------------------------------------------------- */

static const struct mds_authority_ops rondb_authority_ops = {
	.ns_create         = rondb_auth_ns_create,
	.ns_remove         = rondb_auth_ns_remove,
	.ns_remove_known   = rondb_auth_ns_remove_known,
	.ns_rename         = rondb_auth_ns_rename,
	.ns_link           = rondb_auth_ns_link,
	.ns_lookup         = catalogue_rondb_ns_lookup,
	.ns_getattr        = catalogue_rondb_ns_getattr,
	.ns_setattr        = rondb_auth_ns_setattr,
	.ns_readdir        = rondb_auth_ns_readdir,
	.ns_readdir_plus   = rondb_auth_ns_readdir_plus,
	.ns_readdir_plus_from = rondb_auth_ns_readdir_plus_from,
	.dirent_name_for_child = rondb_auth_dirent_name_for_child,
	.ns_nlink_adjust   = catalogue_rondb_ns_nlink_adjust,
	.alloc_fileid      = rondb_auth_alloc_fileid,
	.inode_put         = catalogue_rondb_inode_put,
	.inode_del         = catalogue_rondb_inode_del,
	.dirent_put        = catalogue_rondb_dirent_put,
	.dirent_del        = catalogue_rondb_dirent_del,
	.inline_get        = catalogue_rondb_inline_get,
	.inline_put        = catalogue_rondb_inline_put,
	.inline_del        = catalogue_rondb_inline_del,
	.xattr_get         = catalogue_rondb_xattr_get,
	.xattr_put         = rondb_auth_xattr_put,
	.xattr_del         = rondb_auth_xattr_del,
	.xattr_list        = catalogue_rondb_xattr_list,
	.xattr_exists      = catalogue_rondb_xattr_exists,
	.stripe_map_get    = catalogue_rondb_stripe_map_get,
	.stripe_map_put    = rondb_auth_stripe_map_put,
	.stripe_map_del    = rondb_auth_stripe_map_del,
	.stripe_map_scan   = catalogue_rondb_stripe_map_scan,
	.ds_get            = catalogue_rondb_ds_get,
	.ds_put            = rondb_auth_ds_put,
	.ds_del            = rondb_auth_ds_del,
	.ds_list           = catalogue_rondb_ds_list,
	.ds_provision_get  = catalogue_rondb_ds_provision_get,
	.ds_provision_put  = catalogue_rondb_ds_provision_put,
	.ds_provision_del  = catalogue_rondb_ds_provision_del,
	.quota_rule_get    = catalogue_rondb_quota_rule_get,
	.quota_rule_put    = catalogue_rondb_quota_rule_put,
	.quota_usage_get   = catalogue_rondb_quota_usage_get,
	.quota_usage_put   = catalogue_rondb_quota_usage_put,
	.gc_enqueue        = catalogue_rondb_gc_enqueue,
	.gc_peek           = catalogue_rondb_gc_peek,
	.gc_dequeue        = catalogue_rondb_gc_dequeue,
	.gc_count          = catalogue_rondb_gc_count,
	.gc_peek_batch     = catalogue_rondb_gc_peek_batch,
};

/* -----------------------------------------------------------------------
 * Phase 8B -- RonDB coordination vtable
 * ----------------------------------------------------------------------- */

/* -----------------------------------------------------------------------
 * Phase 12 -- Multi-MDS authority vtable (Phase 3 of review plan)
 *
 * The earlier Phase-9B design routed every CREATE / REMOVE / LINK /
 * SETATTR in multi-MDS mode through an application-level lock
 * manager (mds_ns_locks + mds_ns_lock_holders, two NDB tables whose
 * rows are acquired and released around each mutation).  That layer
 * was a correctness belt-and-braces above what NDB already provides:
 *   - ns_create: the shim's dirent insertTuple is the authoritative
 *     uniqueness gate (ConstraintViolation on duplicate); parent
 *     nlink/mtime/change are updated via NDB interpretedUpdateTuple,
 *     which is atomic across all API nodes.
 *   - ns_remove: analogous atomic interpreted update on the parent,
 *     plus single-txn delete of dirent + inode.
 *   - ns_setattr: rondb_shim_inode_setattr_atomic takes
 *     LM_Exclusive on the inode row and commits read+write inside
 *     one NDB transaction.
 *   - ns_link: dirent insertTuple + interpreted nlink bump, same
 *     pattern as ns_create.
 * NDB row locks already serialise concurrent writers from multiple
 * MDS daemons.  The app-level wrapper simply adds 3--5 extra NDB
 * round-trips per mutation and, under 16-way contention, livelocks
 * on its sort-before-acquire retry loop (see docs/benchmark-phase1.md
 * for the empirical evidence -- 1,911 creates/sec baseline missed by
 * ~96%, 16-task mdtest deadlocks).
 *
 * Cross-directory RENAME is the one operation that genuinely needs
 * application-level serialisation: the catalogue_rondb_ns_rename
 * prologue issues multiple separate NDB reads (src parent, dst
 * parent, src dirent, src child, optional dst dirent + child) before
 * the atomic commit, and a concurrent MDS could mutate between those
 * reads without the TOPOLOGY lock.  Keep ns_rename on the locked
 * wrapper; unlock everything else.
 * ----------------------------------------------------------------------- */

static const struct mds_authority_ops rondb_locked_authority_ops = {
	.ns_create         = rondb_auth_ns_create,
	.ns_remove         = rondb_auth_ns_remove,
	.ns_remove_known   = rondb_auth_ns_remove_known,
	.ns_rename         = rondb_locked_ns_rename,
	.ns_link           = rondb_auth_ns_link,
	.ns_lookup         = catalogue_rondb_ns_lookup,
	.ns_getattr        = catalogue_rondb_ns_getattr,
	.ns_setattr        = rondb_auth_ns_setattr,
	.ns_readdir        = rondb_auth_ns_readdir,
	.ns_readdir_plus   = rondb_auth_ns_readdir_plus,
	.ns_readdir_plus_from = rondb_auth_ns_readdir_plus_from,
	.dirent_name_for_child = rondb_auth_dirent_name_for_child,
	.ns_nlink_adjust   = catalogue_rondb_ns_nlink_adjust,
	.alloc_fileid      = rondb_auth_alloc_fileid,
	.inode_put         = catalogue_rondb_inode_put,
	.inode_del         = catalogue_rondb_inode_del,
	.dirent_put        = catalogue_rondb_dirent_put,
	.dirent_del        = catalogue_rondb_dirent_del,
	.inline_get        = catalogue_rondb_inline_get,
	.inline_put        = catalogue_rondb_inline_put,
	.inline_del        = catalogue_rondb_inline_del,
	.xattr_get         = catalogue_rondb_xattr_get,
	.xattr_put         = rondb_auth_xattr_put,
	.xattr_del         = rondb_auth_xattr_del,
	.xattr_list        = catalogue_rondb_xattr_list,
	.xattr_exists      = catalogue_rondb_xattr_exists,
	.stripe_map_get    = catalogue_rondb_stripe_map_get,
	.stripe_map_put    = rondb_auth_stripe_map_put,
	.stripe_map_del    = rondb_auth_stripe_map_del,
	.stripe_map_scan   = catalogue_rondb_stripe_map_scan,
	.ds_get            = catalogue_rondb_ds_get,
	.ds_put            = rondb_auth_ds_put,
	.ds_del            = rondb_auth_ds_del,
	.ds_list           = catalogue_rondb_ds_list,
	.ds_provision_get  = catalogue_rondb_ds_provision_get,
	.ds_provision_put  = catalogue_rondb_ds_provision_put,
	.ds_provision_del  = catalogue_rondb_ds_provision_del,
	.quota_rule_get    = catalogue_rondb_quota_rule_get,
	.quota_rule_put    = catalogue_rondb_quota_rule_put,
	.quota_usage_get   = catalogue_rondb_quota_usage_get,
	.quota_usage_put   = catalogue_rondb_quota_usage_put,
	.gc_enqueue        = catalogue_rondb_gc_enqueue,
	.gc_peek           = catalogue_rondb_gc_peek,
	.gc_dequeue        = catalogue_rondb_gc_dequeue,
	.gc_count          = catalogue_rondb_gc_count,
	.gc_peek_batch     = catalogue_rondb_gc_peek_batch,
};

static enum mds_status catalogue_rondb_recovery_list(
	struct mds_catalogue *cat, uint32_t owner_mds_id,
	mds_recovery_list_cb cb, void *ctx)
{
	void *h = rondb_handle(cat);
	int rc;

	if (h == NULL || cb == NULL) {
		return MDS_ERR_INVAL;
	}
	rc = rondb_shim_recovery_scan(h, owner_mds_id, cb, ctx);
	return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Shared protocol state C wrappers (shared-attr)
 * ----------------------------------------------------------------------- */

static enum mds_status catalogue_rondb_open_put(
    struct mds_catalogue *cat, const struct mds_coord_open_row *row)
{
    void *h = rondb_handle(cat);
    if (h == NULL || row == NULL) { return MDS_ERR_INVAL; }
    int rc = rondb_shim_open_put(h, row);
    return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_open_get(
    struct mds_catalogue *cat, const uint8_t stateid_other[12],
    struct mds_coord_open_row *row)
{
    void *h = rondb_handle(cat);
    if (h == NULL || row == NULL) { return MDS_ERR_INVAL; }
    int rc = rondb_shim_open_get(h, stateid_other, row);
    if (rc == 1) { return MDS_ERR_NOTFOUND; }
    return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_open_del(
    struct mds_catalogue *cat, const uint8_t stateid_other[12])
{
    void *h = rondb_handle(cat);
    if (h == NULL) { return MDS_ERR_INVAL; }
    int rc = rondb_shim_open_del(h, stateid_other);
    if (rc == 1) { return MDS_ERR_NOTFOUND; }
    return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_open_scan_file(
    struct mds_catalogue *cat, uint64_t fileid,
    mds_coord_open_scan_cb cb, void *ctx)
{
    void *h = rondb_handle(cat);
    if (h == NULL || cb == NULL) { return MDS_ERR_INVAL; }
    int rc = rondb_shim_open_scan_file(h, fileid,
        (rondb_open_scan_cb)cb, ctx);
    return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

static enum mds_status catalogue_rondb_open_scan_client(
    struct mds_catalogue *cat, uint64_t clientid,
    mds_coord_open_scan_cb cb, void *ctx)
{
    void *h = rondb_handle(cat);
    if (h == NULL || cb == NULL) { return MDS_ERR_INVAL; }
    int rc = rondb_shim_open_scan_client(h, clientid,
        (rondb_open_scan_cb)cb, ctx);
    return (rc == 0) ? MDS_OK : MDS_ERR_IO;
}

/* Remaining entity C wrappers -- same pattern as open_* above. */

#define RONDB_WRAP_PUT(name, type, shim_fn) \
static enum mds_status catalogue_rondb_##name( \
    struct mds_catalogue *cat, const struct type *row) \
{ void *h = rondb_handle(cat); \
  if (h == NULL || row == NULL) { return MDS_ERR_INVAL; } \
  int rc = shim_fn(h, row); \
  return (rc == 0) ? MDS_OK : MDS_ERR_IO; }

#define RONDB_WRAP_GET_SID(name, type, shim_fn) \
static enum mds_status catalogue_rondb_##name( \
    struct mds_catalogue *cat, const uint8_t sid[12], struct type *row) \
{ void *h = rondb_handle(cat); \
  if (h == NULL || row == NULL) { return MDS_ERR_INVAL; } \
  int rc = shim_fn(h, sid, row); \
  if (rc == 1) { return MDS_ERR_NOTFOUND; } \
  return (rc == 0) ? MDS_OK : MDS_ERR_IO; }

#define RONDB_WRAP_DEL_SID(name, shim_fn) \
static enum mds_status catalogue_rondb_##name( \
    struct mds_catalogue *cat, const uint8_t sid[12]) \
{ void *h = rondb_handle(cat); \
  if (h == NULL) { return MDS_ERR_INVAL; } \
  int rc = shim_fn(h, sid); \
  if (rc == 1) { return MDS_ERR_NOTFOUND; } \
  return (rc == 0) ? MDS_OK : MDS_ERR_IO; }

RONDB_WRAP_PUT(deleg_put, mds_coord_deleg_row, rondb_shim_deleg_put)
RONDB_WRAP_GET_SID(deleg_get, mds_coord_deleg_row, rondb_shim_deleg_get)
RONDB_WRAP_DEL_SID(deleg_del, rondb_shim_deleg_del)

static enum mds_status catalogue_rondb_deleg_scan_file(
    struct mds_catalogue *cat, uint64_t fileid,
    mds_coord_deleg_scan_cb cb, void *ctx)
{ void *h = rondb_handle(cat);
  if (h == NULL || cb == NULL) { return MDS_ERR_INVAL; }
  return rondb_shim_deleg_scan_file(h, fileid, (rondb_deleg_scan_cb)cb, ctx) == 0
         ? MDS_OK : MDS_ERR_IO; }

static enum mds_status catalogue_rondb_deleg_scan_client(
    struct mds_catalogue *cat, uint64_t clientid,
    mds_coord_deleg_scan_cb cb, void *ctx)
{ void *h = rondb_handle(cat);
  if (h == NULL || cb == NULL) { return MDS_ERR_INVAL; }
  return rondb_shim_deleg_scan_client(h, clientid, (rondb_deleg_scan_cb)cb, ctx) == 0
         ? MDS_OK : MDS_ERR_IO; }

RONDB_WRAP_PUT(client_put, mds_coord_client_row, rondb_shim_client_put)

static enum mds_status catalogue_rondb_client_get(
    struct mds_catalogue *cat, uint64_t clientid,
    struct mds_coord_client_row *row)
{ void *h = rondb_handle(cat);
  if (h == NULL || row == NULL) { return MDS_ERR_INVAL; }
  int rc = rondb_shim_client_get(h, clientid, row);
  if (rc == 1) { return MDS_ERR_NOTFOUND; }
  return (rc == 0) ? MDS_OK : MDS_ERR_IO; }

static enum mds_status catalogue_rondb_client_del(
    struct mds_catalogue *cat, uint64_t clientid)
{ void *h = rondb_handle(cat);
  if (h == NULL) { return MDS_ERR_INVAL; }
  int rc = rondb_shim_client_del(h, clientid);
  if (rc == 1) { return MDS_ERR_NOTFOUND; }
  return (rc == 0) ? MDS_OK : MDS_ERR_IO; }

RONDB_WRAP_PUT(session_put, mds_coord_session_row, rondb_shim_session_put)

static enum mds_status catalogue_rondb_session_get(
    struct mds_catalogue *cat, const uint8_t session_id[16],
    struct mds_coord_session_row *row)
{ void *h = rondb_handle(cat);
  if (h == NULL || row == NULL) { return MDS_ERR_INVAL; }
  int rc = rondb_shim_session_get(h, session_id, row);
  if (rc == 1) { return MDS_ERR_NOTFOUND; }
  return (rc == 0) ? MDS_OK : MDS_ERR_IO; }

static enum mds_status catalogue_rondb_session_del(
    struct mds_catalogue *cat, const uint8_t session_id[16])
{ void *h = rondb_handle(cat);
  if (h == NULL) { return MDS_ERR_INVAL; }
  int rc = rondb_shim_session_del(h, session_id);
  if (rc == 1) { return MDS_ERR_NOTFOUND; }
  return (rc == 0) ? MDS_OK : MDS_ERR_IO; }

static enum mds_status catalogue_rondb_session_scan_client(
    struct mds_catalogue *cat, uint64_t clientid,
    mds_coord_session_scan_cb cb, void *ctx)
{ void *h = rondb_handle(cat);
  if (h == NULL || cb == NULL) { return MDS_ERR_INVAL; }
  return rondb_shim_session_scan_client(h, clientid,
      (rondb_session_scan_cb)cb, ctx) == 0 ? MDS_OK : MDS_ERR_IO; }

RONDB_WRAP_PUT(lock_put, mds_coord_lock_row, rondb_shim_bytelock_put)

static enum mds_status catalogue_rondb_lock_del(
    struct mds_catalogue *cat, uint64_t fileid, uint64_t lock_id)
{ void *h = rondb_handle(cat);
  if (h == NULL) { return MDS_ERR_INVAL; }
  int rc = rondb_shim_bytelock_del(h, fileid, lock_id);
  if (rc == 1) { return MDS_ERR_NOTFOUND; }
  return (rc == 0) ? MDS_OK : MDS_ERR_IO; }

static enum mds_status catalogue_rondb_lock_scan_file(
    struct mds_catalogue *cat, uint64_t fileid,
    mds_coord_lock_scan_cb cb, void *ctx)
{ void *h = rondb_handle(cat);
  if (h == NULL || cb == NULL) { return MDS_ERR_INVAL; }
  return rondb_shim_bytelock_scan_file(h, fileid,
      (rondb_lock_scan_cb)cb, ctx) == 0 ? MDS_OK : MDS_ERR_IO; }

static enum mds_status catalogue_rondb_lock_reap_client(
    struct mds_catalogue *cat, uint64_t clientid)
{ void *h = rondb_handle(cat);
  if (h == NULL) { return MDS_ERR_INVAL; }
  return rondb_shim_bytelock_reap_client(h, clientid) == 0
         ? MDS_OK : MDS_ERR_IO; }

static enum mds_status catalogue_rondb_slot_put(
    struct mds_catalogue *cat, const uint8_t session_id[16],
    uint32_t slot_id, uint32_t seq_id,
    const void *cached_reply, uint32_t reply_len)
{ void *h = rondb_handle(cat);
  if (h == NULL) { return MDS_ERR_INVAL; }
  return rondb_shim_drc_slot_put(h, session_id, slot_id,
      seq_id, cached_reply, reply_len) == 0 ? MDS_OK : MDS_ERR_IO; }

static enum mds_status catalogue_rondb_slot_get(
    struct mds_catalogue *cat, const uint8_t session_id[16],
    uint32_t slot_id, struct mds_coord_drc_slot_row *row)
{ void *h = rondb_handle(cat);
  if (h == NULL || row == NULL) { return MDS_ERR_INVAL; }
  int rc = rondb_shim_drc_slot_get(h, session_id, slot_id, row);
  if (rc == 1) { return MDS_ERR_NOTFOUND; }
  return (rc == 0) ? MDS_OK : MDS_ERR_IO; }

#undef RONDB_WRAP_PUT
#undef RONDB_WRAP_GET_SID
#undef RONDB_WRAP_DEL_SID

static const struct mds_coordination_ops rondb_coordination_ops = {
	.journal_put             = catalogue_rondb_journal_put,
	.journal_get             = catalogue_rondb_journal_get,
	.journal_del             = catalogue_rondb_journal_del,
	.journal_scan            = catalogue_rondb_journal_scan,
	.layout_grant            = catalogue_rondb_layout_grant,
	.layout_return           = catalogue_rondb_layout_return,
	.layout_get_by_stateid   = catalogue_rondb_layout_get_by_stateid,
	.layout_scan_for_file    = catalogue_rondb_layout_scan_for_file,
	.layout_del_all_for_client = catalogue_rondb_layout_del_all_for_client,
	.ds_layout_idx_scan      = catalogue_rondb_ds_layout_idx_scan,
	.layout_iter_file        = catalogue_rondb_layout_iter_file,
	.recovery_put            = catalogue_rondb_recovery_put,
	.recovery_del            = catalogue_rondb_recovery_del,
	.recovery_get            = catalogue_rondb_recovery_get,
	.recovery_list           = catalogue_rondb_recovery_list,
	/* Shared protocol state -- all entities wired. */
	.open_put          = catalogue_rondb_open_put,
	.open_get          = catalogue_rondb_open_get,
	.open_del          = catalogue_rondb_open_del,
	.open_scan_file    = catalogue_rondb_open_scan_file,
	.open_scan_client  = catalogue_rondb_open_scan_client,
	.lock_put          = catalogue_rondb_lock_put,
	.lock_del          = catalogue_rondb_lock_del,
	.lock_test         = NULL, /* Stage 4: conflict check logic */
	.lock_scan_file    = catalogue_rondb_lock_scan_file,
	.lock_scan_owner   = NULL, /* Stage 4: owner scan */
	.lock_reap_client  = catalogue_rondb_lock_reap_client,
	.deleg_put         = catalogue_rondb_deleg_put,
	.deleg_get         = catalogue_rondb_deleg_get,
	.deleg_del         = catalogue_rondb_deleg_del,
	.deleg_scan_file   = catalogue_rondb_deleg_scan_file,
	.deleg_scan_client = catalogue_rondb_deleg_scan_client,
	.client_put        = catalogue_rondb_client_put,
	.client_get        = catalogue_rondb_client_get,
	.client_del        = catalogue_rondb_client_del,
	.session_put       = catalogue_rondb_session_put,
	.session_get       = catalogue_rondb_session_get,
	.session_del       = catalogue_rondb_session_del,
	.session_scan_client = catalogue_rondb_session_scan_client,
	.slot_put          = catalogue_rondb_slot_put,
	.slot_get          = catalogue_rondb_slot_get,
};

/* -----------------------------------------------------------------------
 * Phase 9A -- Node registry C wrappers
 * ----------------------------------------------------------------------- */

enum mds_status catalogue_rondb_mds_register(struct mds_catalogue *cat,
                                             uint32_t mds_id,
                                             uint64_t boot_epoch,
                                             const char *hostname,
                                             uint16_t nfs_port,
                                             uint16_t grpc_port)
{
    void *h = rondb_handle(cat);

    if (h == NULL || hostname == NULL) {
        return MDS_ERR_INVAL;
    }
    return rondb_shim_mds_register(h, mds_id, boot_epoch,
                                   hostname, nfs_port, grpc_port) == 0
           ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_mds_heartbeat(struct mds_catalogue *cat,
                                              uint32_t mds_id,
                                              uint64_t boot_epoch)
{
    void *h = rondb_handle(cat);

    if (h == NULL) {
        return MDS_ERR_INVAL;
    }
    int rc = rondb_shim_mds_heartbeat(h, mds_id, boot_epoch);
    if (rc == 1) { return MDS_ERR_NOTFOUND; }
    return rc == 0 ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_mds_deregister(struct mds_catalogue *cat,
                                               uint32_t mds_id)
{
    void *h = rondb_handle(cat);

    if (h == NULL) {
        return MDS_ERR_INVAL;
    }
    return rondb_shim_mds_deregister(h, mds_id) == 0
           ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_mds_scan_stale(
    struct mds_catalogue *cat,
    uint64_t threshold_ns,
    rondb_stale_node_cb cb, void *ctx)
{
    void *h = rondb_handle(cat);

    if (h == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    return rondb_shim_mds_scan_stale(h, threshold_ns, cb, ctx) == 0
           ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_alloc_fileid_range(
    struct mds_catalogue *cat,
    uint32_t batch_size,
    uint64_t *range_start)
{
    void *h = rondb_handle(cat);

    if (h == NULL || range_start == NULL || batch_size == 0) {
        return MDS_ERR_INVAL;
    }
    return rondb_shim_alloc_fileid_range(h, batch_size, range_start) == 0
           ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_alloc_gc_seq_range(
    struct mds_catalogue *cat,
    uint32_t batch_size,
    uint64_t *range_start)
{
    void *h = rondb_handle(cat);

    if (h == NULL || range_start == NULL || batch_size == 0) {
        return MDS_ERR_INVAL;
    }
    return rondb_shim_alloc_gc_seq_range(h, batch_size, range_start) == 0
           ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_lock_reap_by_owner(
    struct mds_catalogue *cat,
    uint32_t owner_mds_id,
    uint64_t owner_boot_epoch,
    uint32_t *reaped_count)
{
    void *h = rondb_handle(cat);

    if (h == NULL) {
        return MDS_ERR_INVAL;
    }
    return rondb_shim_lock_reap_by_owner(h, owner_mds_id,
                                          owner_boot_epoch,
                                          reaped_count) == 0
           ? MDS_OK : MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Partition map C wrappers
 * ----------------------------------------------------------------------- */

enum mds_status catalogue_rondb_partition_map_list(
    struct mds_catalogue *cat, rondb_partition_map_cb cb, void *ctx)
{
    void *h = rondb_handle(cat);

    if (h == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    return rondb_shim_partition_map_list(h, cb, ctx) == 0
           ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_partition_map_put(
    struct mds_catalogue *cat, uint32_t partition_id,
    uint32_t owner_mds_id, uint8_t state, const char *subtree_path)
{
    void *h = rondb_handle(cat);

    if (h == NULL || subtree_path == NULL) {
        return MDS_ERR_INVAL;
    }
    return rondb_shim_partition_map_put(h, partition_id,
                                       owner_mds_id, state,
                                       subtree_path) == 0
           ? MDS_OK : MDS_ERR_IO;
}

enum mds_status catalogue_rondb_partition_map_cas(
    struct mds_catalogue *cat, uint32_t partition_id,
    uint32_t expected_owner, uint32_t new_owner, uint8_t new_state)
{
    void *h = rondb_handle(cat);
    int rc;

    if (h == NULL) {
        return MDS_ERR_INVAL;
    }
    rc = rondb_shim_partition_map_cas(h, partition_id,
                                     expected_owner, new_owner,
                                     new_state);
    if (rc == 0) { return MDS_OK; }
    if (rc == 1) { return MDS_ERR_NOTFOUND; }
    if (rc == 2) { return MDS_ERR_STALE; }
    return MDS_ERR_IO;
}

/* -----------------------------------------------------------------------
 * Node registry list C wrapper
 * ----------------------------------------------------------------------- */

enum mds_status catalogue_rondb_mds_list(
    struct mds_catalogue *cat, rondb_mds_list_cb cb, void *ctx)
{
    void *h = rondb_handle(cat);

    if (h == NULL || cb == NULL) {
        return MDS_ERR_INVAL;
    }
    return rondb_shim_mds_list(h, cb, ctx) == 0
           ? MDS_OK : MDS_ERR_IO;
}

#endif /* HAVE_RONDB */

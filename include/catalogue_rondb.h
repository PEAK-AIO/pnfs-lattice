/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * catalogue_rondb.h -- RonDB backend configuration + C ABI shim interface.
 *
 * The RonDB native API is C++.  All C++ code is contained in
 * catalogue_rondb_shim.cpp which exports extern "C" functions.
 * This header is pure C and safe to include from any MDS module.
 */

#ifndef CATALOGUE_RONDB_H
#define CATALOGUE_RONDB_H

#include "pnfs_mds.h"

struct mds_catalogue;
struct mds_coord_journal_record;

/** RonDB backend configuration. */
struct mds_rondb_config {
    char     connect_string[512];  /**< NDB mgm endpoints ("mgm1:1186"). */
    char     schema_name[64];      /**< Database/schema name (default "pnfs_mds"). */
};

/**
 * Parse RonDB backend config from file.
 *
 * @param path  Path to RonDB config file.
 * @param out   Receives parsed config.
 * @return MDS_OK on success.
 */
enum mds_status mds_rondb_config_load(const char *path,
                                      struct mds_rondb_config *out);

/* -----------------------------------------------------------------------
 * C ABI shim -- implemented in catalogue_rondb_shim.cpp
 *
 * These functions hide the C++ NDB API behind a narrow C boundary.
 * The shim is compiled only when ENABLE_RONDB is active.
 * ----------------------------------------------------------------------- */

/** Connect to the RonDB management node(s). */
int rondb_shim_connect(const char *connect_string,
                       const char *schema,
                       void **handle);

/** Connect with explicit NDB connection pool size.
 *  Each connection gets its own TCP socket for parallel NDB ops.
 *  pool_size <= 0 defaults to 2. Max 8. */
int rondb_shim_connect_pool(const char *connect_string,
                            const char *schema,
                            int pool_size,
                            void **handle);

/** One-time: create the dedicated RonDB probe table. */
int rondb_shim_bootstrap(void *handle, const char *schema);

/** Health probe: read/write canary row in pre-created table. No DDL. */
int rondb_shim_probe(void *handle);

/** Explicit cleanup: drop the dedicated RonDB probe table. */
int rondb_shim_cleanup(void *handle, const char *schema);

/** Disconnect and free resources. */
void rondb_shim_disconnect(void *handle);

/**
 * Phase 4 feature flag plumbing.  Toggle whether ns_create and
 * ns_remove should route through the rondb_async_exec batch
 * pipeline (executeAsynchPrepare + sendPreparedTransactions driven
 * by the per-connection flush thread).
 *
 * The flag only arms the code path; the async variant of
 * rondb_shim_ns_create is a separate landing gated on empirical
 * concurrent-mutator throughput numbers (see
 * docs/phase4-ndb-async-writes.md).  Until that wire-up lands the
 * flag is a no-op: callers still take the sync execute() path.
 *
 * Safe to call multiple times; atomic, lock-free.
 */
void rondb_shim_set_async_writes(void *handle, int enabled);

/** Benchmark: tight-loop NDB inode+dirent inserts (no NFS overhead).
 *  Creates n_ops dummy rows, returns elapsed_us and error count.
 *  Caller must clean up the test rows afterward. */
int rondb_shim_bench_create(void *handle, uint32_t n_ops,
                            uint64_t parent_fileid, uint64_t base_fileid,
                            uint64_t *elapsed_us, uint32_t *errors);

/* -----------------------------------------------------------------------
 * Stage B shim -- metadata table lifecycle + row-level CRUD
 *
 * Each function is one NDB transaction unless documented otherwise.
 * read_mode: 0=COMMITTED, 1=SHARED, 2=EXCLUSIVE (NDB LockMode).
 * ----------------------------------------------------------------------- */

/** DDL: create all 9 metadata tables and seed only missing bootstrap rows
 *  (schema_version, fileid counter, root inode, partition_map(0)). */
int rondb_shim_bootstrap_metadata(void *handle, const char *schema);

/** DDL: drop all 9 metadata tables. */
int rondb_shim_cleanup_metadata(void *handle, const char *schema);

/** Atomic fileid batch allocation: read counter, increment by batch_size,
 *  write back, return old value as out_base.  Single NDB transaction. */
int rondb_shim_fileid_batch_alloc(void *handle, uint32_t batch_size,
                                  uint64_t *out_base, uint32_t *out_count);

/** Read inode row into caller-supplied byte buffer (rondb_inode format).
 *  read_mode selects NDB lock mode. */
int rondb_shim_inode_get(void *handle, uint64_t fileid,
                         uint8_t *buf, uint32_t buflen, uint32_t *outlen,
                         int read_mode);

/** Write/update inode row from caller-supplied byte buffer. */
int rondb_shim_inode_put(void *handle, uint64_t fileid,
                         const uint8_t *buf, uint32_t buflen);

/** Atomic setattr: exclusive-lock read + update in one NDB transaction.
 *  Prevents concurrent setattr lost-update race (BUG-2).
 *  updated_buf is the fully-merged inode (mask already applied by caller).
 *  Returns 0 on success, 1 on NOTFOUND, -1 on error. */
int rondb_shim_inode_setattr_atomic(void *handle, uint64_t fileid,
                                    const uint8_t *updated_buf,
                                    uint32_t buflen);

/** Delete inode row. */
int rondb_shim_inode_del(void *handle, uint64_t fileid);

/** Fused LOOKUP: dirent read + inode read in one NDB transaction.
 *  Eliminates one NDB round-trip vs separate dirent_get + inode_get.
 *  Returns 0 on success, 1 on NOTFOUND, -1 on error, -2 transient. */
int rondb_shim_ns_lookup(void *handle, uint64_t parent_fileid,
                         const char *name,
                         uint64_t *child_fileid, uint8_t *child_type,
                         uint8_t *inode_buf, uint32_t inode_buflen,
                         uint32_t *inode_outlen);

/** Read dirent by (parent, name).  read_mode selects NDB lock mode. */
int rondb_shim_dirent_get(void *handle, uint64_t parent_fileid,
                          const char *name,
                          uint64_t *child_fileid, uint8_t *child_type,
                          int read_mode);

/** Write/insert dirent. */
int rondb_shim_dirent_put(void *handle, uint64_t parent_fileid,
                          const char *name,
                          uint64_t child_fileid, uint8_t child_type);

/** Delete dirent. */
int rondb_shim_dirent_del(void *handle, uint64_t parent_fileid,
                          const char *name);

/** Stage B stub: current implementation returns -1 until stripe CRUD lands.
 *  Intended shape: read stripe map header + entries for a fileid. */
int rondb_shim_stripe_get(void *handle, uint64_t fileid,
                          uint32_t *stripe_count, uint32_t *stripe_unit,
                          uint32_t *mirror_count,
                          uint8_t *entries_buf, uint32_t entries_buflen,
                          uint32_t *entries_outlen);
/** Stage B stub: current implementation returns -1 until stripe CRUD lands.
 *  Intended shape: write stripe map header + entries in one NDB txn. */
int rondb_shim_stripe_put(void *handle, uint64_t fileid,
                          uint32_t stripe_count, uint32_t stripe_unit,
                          uint32_t mirror_count,
                          const uint8_t *entries_buf, uint32_t entries_len);
/** Stage B stub: current implementation returns -1 until stripe CRUD lands.
 *  Intended shape: delete stripe map header + all entries for fileid. */
int rondb_shim_stripe_del(void *handle, uint64_t fileid,
                          uint32_t max_entries);

/** Callback for stripe_map_scan.  Receives one fully-assembled stripe map
 *  per call.  Return 0 to continue, non-zero to stop. */
typedef int (*rondb_stripe_map_scan_cb)(uint64_t fileid,
                                        uint32_t stripe_count,
                                        uint32_t stripe_unit,
                                        uint32_t mirror_count,
                                        const struct mds_ds_map_entry *entries,
                                        void *ctx);

/** Full table scan on mds_stripe_maps + mds_stripe_entries.
 *  Scans all stripe map headers, fetches entries for each, and
 *  calls cb with the assembled result.  Used by rebalance/resilver
 *  to discover files on a specific DS.
 *  Returns 0 on success, -1 on error. */
int rondb_shim_stripe_map_scan(void *handle,
                               rondb_stripe_map_scan_cb cb, void *ctx);

/** Atomic CREATE: insert child inode + dirent + atomic parent update +
 *  optional stripe data.  Single NDB transaction (T2 class).
 *  Parent nlink/change/mtime are updated atomically via
 *  interpretedUpdateTuple (no read-modify-write race).
 *  Returns 0 on success, 1 on EXISTS (dirent conflict), -1 on error. */
int rondb_shim_ns_create(void *handle,
                         uint64_t parent_fileid, const char *name,
                         const uint8_t *child_inode_buf, uint32_t child_ino_len,
                         int32_t parent_nlink_delta,
                         const uint8_t *stripe_buf, uint32_t stripe_len,
                         uint32_t stripe_count);

/** Atomic RENAME: single NDB transaction (T2/T3 class).
 *  Deletes src dirent, writes dst dirent, atomically updates both
 *  parent inodes via interpretedUpdateTuple, optionally handles
 *  overwrite (dst child nlink decrement or delete).
 *  Returns 0 on success, 1 on src NOTFOUND, -1 on error. */
int rondb_shim_rename(void *handle,
                     uint64_t src_parent, const char *src_name,
                     uint64_t dst_parent, const char *dst_name,
                     int32_t src_parent_nlink_delta,
                     int32_t dst_parent_nlink_delta,
                     const uint8_t *src_child_buf, uint32_t sc_len,
                     uint64_t src_child_fid, uint8_t src_child_type,
                     int dst_exists,
                     const uint8_t *dst_child_buf, uint32_t dc_len,
                     uint64_t dst_child_fid, int delete_dst_child);

/** Atomic REMOVE: delete dirent + update/delete child inode + atomic
 *  parent update + delete stripe data if nlink=0.  Single NDB txn.
 *  child_inode_buf contains the updated child inode (nlink decremented).
 *  Parent nlink/change/mtime updated atomically via interpretedUpdateTuple.
 *  Returns 0 on success, -1 on error. */
int rondb_shim_ns_remove(void *handle,
                         uint64_t parent_fileid, const char *name,
                         uint64_t child_fileid,
                         const uint8_t *child_inode_buf, uint32_t child_ino_len,
                         int delete_child,
                         int32_t parent_nlink_delta,
                         uint32_t stripe_count);

/** Fused REMOVE: dirent read + inode read + atomic delete in ONE NDB
 *  transaction (2 execute phases: NoCommit reads + Commit mutations).
 *  Replaces the separate ns_lookup + ns_remove call pair.
 *  Returns child_type and nlink via out params for caller's use.
 *  Returns 0 on success, 1 on NOTFOUND, -1 on error, -2 transient. */
int rondb_shim_ns_remove_full(void *handle,
                              uint64_t parent_fileid, const char *name,
                              uint8_t *out_child_type,
                              uint32_t *out_old_nlink);

/** Readdir callback for shim -- called per dirent found.
 *  Return 0 to continue, non-zero to stop. */
typedef int (*rondb_readdir_cb)(uint64_t child_fileid,
                                uint8_t child_type,
                                const char *name,
                                uint32_t name_len,
                                void *ctx);

/** Scan dirents for a parent directory.  Ordered by entry_name.
 *  If start_after is non-NULL, entries with names <= start_after
 *  are skipped.  Calls cb for each entry found.
 *  Returns 0 on success, -1 on error. */
int rondb_shim_ns_readdir(void *handle,
                          uint64_t parent_fileid,
                          const char *start_after,
                          rondb_readdir_cb cb, void *ctx);

/** READDIR_PLUS shim callback -- called once per dirent with both the
 *  dirent columns and the fused child-inode read.
 *
 *  The `inode_buf` / `inode_len` pair uses the same 137-byte
 *  rondb_inode serialisation as rondb_shim_inode_get; when the inode
 *  read returned NDB NOT_FOUND (626) -- the race where a dirent points
 *  at an inode that was concurrently deleted -- `inode_valid` is 0,
 *  `inode_buf` is NULL, `inode_len` is 0 and the caller should treat
 *  the entry's attributes as unavailable rather than failing the
 *  whole RPC.
 *
 *  Return 0 to continue, non-zero to stop. */
typedef int (*rondb_readdir_plus_cb)(uint64_t child_fileid,
                                     uint8_t child_type,
                                     const char *name,
                                     uint32_t name_len,
                                     const uint8_t *inode_buf,
                                     uint32_t inode_len,
                                     int inode_valid,
                                     void *ctx);

/** Fused READDIR_PLUS: dirent scan + batched inode reads in ONE NDB
 *  transaction.  Equivalent in result to rondb_shim_ns_readdir followed
 *  by one rondb_shim_inode_get per entry, but collapses the N+1
 *  round-trips into 2 (one NoCommit scan + one Commit batch).
 *
 *  Entry order matches rondb_shim_ns_readdir (bytewise by name).
 *  `start_after` semantics match rondb_shim_ns_readdir.
 *
 *  Returns 0 on success, -1 on error. */
int rondb_shim_ns_readdir_plus(void *handle,
                               uint64_t parent_fileid,
                               const char *start_after,
                               rondb_readdir_plus_cb cb, void *ctx);

/** Atomic LINK: create dirent + bump target nlink + atomic parent
 *  update.  Single NDB txn.  Parent nlink/change/mtime updated
 *  atomically via interpretedUpdateTuple (delta=0 for hard links).
 *  Returns 0 on success, 1 on EXISTS (dirent conflict), -1 on error. */
int rondb_shim_ns_link(void *handle,
                       uint64_t parent_fileid, const char *name,
                       uint64_t target_fileid, uint8_t target_type,
                       const uint8_t *target_inode_buf, uint32_t ti_len);

/** Atomic nlink adjustment on a single inode row.
 *  Returns 0 on success, 1 on NOTFOUND, -1 on error. */
int rondb_shim_ns_nlink_adjust(void *handle,
                               uint64_t fileid,
                               int32_t delta);

/* -----------------------------------------------------------------------
 * Phase 1 shim -- inline data, xattr, DS registry, quota, GC, layout,
 *                client recovery.
 * ----------------------------------------------------------------------- */

/** Read xattr value by (fileid, name). Returns 0/1(NOTFOUND)/-1. */
int rondb_shim_xattr_get(void *handle, uint64_t fileid,
                         const char *name,
                         uint8_t *val_buf, uint32_t val_buflen,
                         uint32_t *val_outlen);

/** Write/overwrite xattr. Returns 0/-1. */
int rondb_shim_xattr_put(void *handle, uint64_t fileid,
                         const char *name,
                         const uint8_t *val, uint32_t val_len);

/** Delete xattr. Returns 0/-1. */
int rondb_shim_xattr_del(void *handle, uint64_t fileid,
                         const char *name);

/** Xattr list callback for shim. Return 0 to continue, non-zero to stop. */
typedef int (*rondb_xattr_list_cb)(const char *name, uint32_t name_len,
                                   void *ctx);

/** List all xattrs for a fileid via scan. Returns 0/-1. */
int rondb_shim_xattr_list(void *handle, uint64_t fileid,
                          rondb_xattr_list_cb cb, void *ctx);

/* Inline data intentionally omitted for RonDB
 * data through DS layout paths.  RonDB mode forces inline_enabled=false.
 * Inline data is stored via the catalogue xattr interface.
 *  */

/* -----------------------------------------------------------------------
 * DS registry CRUD (typed columns, no blob serialisation)
 * ----------------------------------------------------------------------- */

struct mds_ds_info;

/** Read DS registry entry by ds_id. Returns 0/1(NOTFOUND)/-1. */
int rondb_shim_ds_registry_get(void *handle, uint32_t ds_id,
                               struct mds_ds_info *info);

/** Write/overwrite DS registry entry. Returns 0/-1. */
int rondb_shim_ds_registry_put(void *handle,
                               const struct mds_ds_info *info);

/** Delete DS registry entry. Returns 0/-1. */
int rondb_shim_ds_registry_del(void *handle, uint32_t ds_id);

/** List all DS registry entries. Caller must free(*list_out). Returns 0/-1. */
int rondb_shim_ds_registry_list(void *handle,
                                struct mds_ds_info **list_out,
                                uint32_t *count_out);

/* -----------------------------------------------------------------------
 * Phase 8A shim -- DS provisioning, quota, GC queue, layout state,
 *                 client recovery.
 * ----------------------------------------------------------------------- */

struct mds_quota_rule;
struct mds_quota_usage;
struct mds_gc_entry;
struct nfs4_stateid;

/* DS provisioning (mds_ds_provision: PK=ds_id). */
int rondb_shim_ds_provision_get(void *handle, uint32_t ds_id,
                                uint8_t *secret_buf, uint32_t secret_buflen,
                                uint32_t *secret_outlen, uint64_t *epoch);
int rondb_shim_ds_provision_put(void *handle, uint32_t ds_id,
                                const uint8_t *secret, uint32_t secret_len,
                                uint64_t epoch);
int rondb_shim_ds_provision_del(void *handle, uint32_t ds_id);

/* Quota rules (mds_quota_rules: PK=(scope_type, scope_id)). */
int rondb_shim_quota_rule_get(void *handle, uint8_t scope_type,
                              uint64_t scope_id,
                              struct mds_quota_rule *rule);
int rondb_shim_quota_rule_put(void *handle, uint8_t scope_type,
                              uint64_t scope_id,
                              const struct mds_quota_rule *rule);

/* Quota usage (mds_quota_usage: PK=(usage_type, scope_id)). */
int rondb_shim_quota_usage_get(void *handle, uint8_t usage_type,
                               uint64_t scope_id,
                               struct mds_quota_usage *usage);
int rondb_shim_quota_usage_put(void *handle, uint8_t usage_type,
                               uint64_t scope_id,
                               const struct mds_quota_usage *usage);

/* GC queue (mds_gc_queue: PK=gc_seq). */
int rondb_shim_gc_seq_alloc(void *handle, uint64_t *seq_out);
int rondb_shim_gc_enqueue(void *handle, uint64_t gc_seq,
                          uint64_t fileid, uint32_t ds_id,
                          const uint8_t *nfs_fh, uint32_t fh_len);
int rondb_shim_gc_peek(void *handle, struct mds_gc_entry *entry);

/** Batched peek: scan mds_gc_queue once, return the lowest-`cap`
 *  entries by gc_seq into `entries[]` (ascending order).  Caller
 *  must allocate space for `cap` entries.  *n_out gets the number
 *  written (0..cap).  Returns 0 on success (including empty queue,
 *  *n_out == 0) or -1 on error.  Does not commit / does not modify
 *  the queue. */
int rondb_shim_gc_peek_batch(void *handle, struct mds_gc_entry *entries,
                             uint32_t cap, uint32_t *n_out);
int rondb_shim_gc_dequeue(void *handle, uint64_t gc_seq);
int rondb_shim_gc_count(void *handle, uint32_t *count);
/* Shared 2PC journal (mds_rename_journal: PK=(txn_id, role)). */
typedef int (*rondb_journal_scan_cb)(
    const struct mds_coord_journal_record *record, void *ctx);
int rondb_shim_journal_put(void *handle,
                           const struct mds_coord_journal_record *record);
int rondb_shim_journal_get(void *handle, uint64_t txn_id, uint8_t role,
                           struct mds_coord_journal_record *record);
int rondb_shim_journal_del(void *handle, uint64_t txn_id, uint8_t role);
int rondb_shim_journal_scan(void *handle,
                            rondb_journal_scan_cb cb, void *ctx);

/* Layout state (mds_layout_state + 3 index tables). */
int rondb_shim_layout_state_put(void *handle,
                                const uint8_t stateid_other[12],
                                uint64_t clientid, uint64_t fileid,
                                uint32_t iomode, uint64_t offset,
                                uint64_t length, uint32_t seqid,
                                const uint32_t *ds_ids, uint32_t ds_count);

/** Phase 2: Fused stripe_get + layout_grant in one NDB transaction.
 *  Reads stripe map header+entries, then writes layout_state + indexes,
 *  all in a single NDB transaction (saves 1 NDB RT vs separate calls).
 *  Returns 0 on success, 1 on stripe NOTFOUND, -1 on error. */
int rondb_shim_stripe_get_and_layout_grant(
    void *handle, uint64_t fileid,
    uint32_t *stripe_count, uint32_t *stripe_unit, uint32_t *mirror_count,
    uint8_t *entries_buf, uint32_t entries_buflen, uint32_t *entries_outlen,
    const uint8_t stateid_other[12],
    uint64_t clientid, uint32_t iomode, uint64_t offset, uint64_t length,
    uint32_t seqid, uint32_t mds_id);

/** Phase 3: Fused ns_create with optional layout pre-grant.
 *  Same as rondb_shim_ns_create but also writes layout_state + indexes
 *  when layout_clientid != 0.  Eliminates LAYOUTGET NDB round-trips
 *  for CREATE+LAYOUTGET compounds.
 *  Returns 0 on success, 1 on EXISTS, -1 on error. */
int rondb_shim_ns_create_with_layout(
    void *handle,
    uint64_t parent_fileid, const char *name,
    const uint8_t *child_inode_buf, uint32_t child_ino_len,
    int32_t parent_nlink_delta,
    const uint8_t *stripe_buf, uint32_t stripe_len, uint32_t stripe_count,
    uint64_t layout_clientid, uint32_t layout_iomode,
    uint64_t layout_offset, uint64_t layout_length,
    const uint8_t layout_stateid_other[12], uint32_t layout_seqid,
    const uint32_t *layout_ds_ids, uint32_t layout_ds_count,
    uint32_t layout_mds_id);
int rondb_shim_layout_state_del(void *handle,
                                const uint8_t stateid_other[12],
                                uint64_t clientid, uint64_t fileid,
                                const uint32_t *ds_ids, uint32_t ds_count);
int rondb_shim_layout_get_by_stateid(void *handle,
                                     const uint8_t stateid_other[12],
                                     uint64_t *clientid, uint64_t *fileid,
                                     uint32_t *iomode, uint64_t *offset,
                                     uint64_t *length, uint32_t *seqid);
int rondb_shim_layout_scan_for_file(void *handle, uint64_t fileid,
                                    int *has_layout);
int rondb_shim_layout_del_all_for_client(void *handle, uint64_t clientid);

/** Scan ds_layout_idx by ds_id, invoking cb for each (clientid, fileid). */
typedef int (*rondb_ds_layout_scan_cb)(uint64_t clientid,
                                       uint64_t fileid, void *ctx);
int rondb_shim_ds_layout_idx_scan(void *handle, uint32_t ds_id,
                                  rondb_ds_layout_scan_cb cb, void *ctx);

/** Iterate layout_by_file + layout_state for a file. */
typedef int (*rondb_layout_file_iter_cb)(uint64_t clientid,
                                         const uint8_t *stateid_other,
                                         uint32_t seqid, uint32_t iomode,
                                         void *ctx);
int rondb_shim_layout_iter_file(void *handle, uint64_t fileid,
                                rondb_layout_file_iter_cb cb, void *ctx);

/* Client recovery (mds_client_recovery: PK=clientid). */
int rondb_shim_recovery_put(void *handle, uint64_t clientid,
                            const uint8_t *co_ownerid,
                            uint32_t co_ownerid_len,
                            const uint8_t verifier[8]);
int rondb_shim_recovery_del(void *handle, uint64_t clientid);
int rondb_shim_recovery_get(void *handle, uint64_t clientid,
                            uint8_t *co_ownerid,
                            uint32_t *co_ownerid_len,
                            uint8_t verifier[8]);

/** Scan all client recovery records.  Callback receives
 *  (clientid, owner_mds_id, owner_boot_epoch). */
typedef int (*rondb_recovery_scan_cb)(uint64_t clientid,
                                      uint32_t owner_mds_id,
                                      uint64_t owner_boot_epoch,
                                      void *ctx);
int rondb_shim_recovery_scan(void *handle, uint32_t filter_mds_id,
                             rondb_recovery_scan_cb cb, void *ctx);

/* -----------------------------------------------------------------------
 * Stage E shim
 *
 * mds_ns_locks stores one aggregate row per resource so acquires serialize
 * cleanly, while mds_ns_lock_holders stores one row per holder so release can
 * verify ownership safely.
 *
 * Acquire = insert/update resource row + holder row in one NDB transaction.
 * Release = delete holder row + decrement/delete resource row.
 * Test    = read aggregate resource row.
 *
 * Returns: 0 = success, 1 = conflict (lock held), -1 = error.
 * ----------------------------------------------------------------------- */

/** Acquire a namespace lock. Returns 1 if already held by another owner. */
int rondb_shim_lock_acquire(void *handle,
                            uint64_t partition_hint,
                            uint8_t resource_class,
                            const uint8_t *resource_key, uint32_t key_len,
                            uint8_t lock_mode,
                            uint32_t owner_mds_id, uint64_t owner_epoch,
                            uint64_t fencing_epoch,
                            uint32_t ttl_ms);

/** Release a namespace lock held by the specified owner identity. */
int rondb_shim_lock_release(void *handle,
                            uint8_t resource_class,
                            const uint8_t *resource_key, uint32_t key_len,
                            uint32_t owner_mds_id, uint64_t owner_epoch);

/** Test if a namespace lock is held. Returns 0 if held, 1 if not, -1 on error.
 *  Out params filled only when return == 0.
 *  owner_* is meaningful only when lock_mode_out == RONDB_LOCK_MODE_EXCLUSIVE;
 *  shared locks report holder_count and aggregate fencing state but no single
 *  owner identity. */
int rondb_shim_lock_test(void *handle,
                         uint8_t resource_class,
                         const uint8_t *resource_key, uint32_t key_len,
                         uint8_t *lock_mode_out,
                         uint32_t *holder_count_out,
                         uint64_t *fencing_epoch_out,
                         uint32_t *owner_mds_out,
                         uint64_t *owner_epoch_out);

/* -----------------------------------------------------------------------
 * Phase 9A -- Node registry, range allocation, lock reaping
 * ----------------------------------------------------------------------- */

/** Register this MDS in mds_node_registry (writeTuple = insert or update). */
int rondb_shim_mds_register(void *handle, uint32_t mds_id,
                            uint64_t boot_epoch,
                            const char *hostname,
                            uint16_t nfs_port, uint16_t grpc_port);

/** Update last_heartbeat_ns for this (mds_id, boot_epoch). */
int rondb_shim_mds_heartbeat(void *handle, uint32_t mds_id,
                             uint64_t boot_epoch);

/** Remove this MDS from the registry. */
int rondb_shim_mds_deregister(void *handle, uint32_t mds_id);

/** Callback for scan_stale.  Return 0 to continue, non-zero to stop. */
typedef int (*rondb_stale_node_cb)(uint32_t mds_id, uint64_t boot_epoch,
                                   uint64_t last_heartbeat_ns, void *ctx);

/** Scan mds_node_registry for nodes with last_heartbeat_ns < threshold_ns. */
int rondb_shim_mds_scan_stale(void *handle, uint64_t threshold_ns,
                              rondb_stale_node_cb cb, void *ctx);

/** Atomically claim a fileid range [*range_start, *range_start + batch_size)
 *  via CAS on fileid_counter in mds_meta. */
int rondb_shim_alloc_fileid_range(void *handle, uint32_t batch_size,
                                  uint64_t *range_start);

/** Atomically claim a GC-seq range via CAS on gc_seq_counter. */
int rondb_shim_alloc_gc_seq_range(void *handle, uint32_t batch_size,
                                  uint64_t *range_start);

/** Scan-delete all lock holders for a dead (mds_id, boot_epoch) pair.
 *  Also removes/shrinks the corresponding resource rows.
 *  Returns 0 on success; *reaped_count receives the number of holders
 *  deleted. */
int rondb_shim_lock_reap_by_owner(void *handle,
                                  uint32_t owner_mds_id,
                                  uint64_t owner_boot_epoch,
                                  uint32_t *reaped_count);

/* -----------------------------------------------------------------------
 * Phase 9C -- Delta broadcast shim
 * ----------------------------------------------------------------------- */

/** Callback for delta_poll.  Return 0 to continue, non-zero to stop. */
typedef int (*rondb_delta_poll_cb)(uint64_t seqno, uint64_t boot_epoch,
                                   uint8_t delta_type,
                                   const void *payload, uint32_t payload_len,
                                   uint64_t timestamp_ns, void *ctx);

/** Insert a delta broadcast record.  Returns 0/-1. */
int rondb_shim_delta_insert(void *handle,
                            uint32_t source_mds_id, uint64_t seqno,
                            uint64_t boot_epoch, uint8_t delta_type,
                            const void *payload, uint32_t payload_len,
                            uint64_t timestamp_ns);

/** Poll delta broadcasts for a given source MDS, seqno > min_seqno.
 *  Calls cb for each row found (up to max_rows, 0=unlimited). */
int rondb_shim_delta_poll(void *handle,
                          uint32_t source_mds_id, uint64_t min_seqno,
                          uint32_t max_rows,
                          rondb_delta_poll_cb cb, void *ctx);

/** Trim (delete) delta broadcast records WHERE source_mds_id=X AND seqno <= max_seqno. */
int rondb_shim_delta_trim(void *handle,
                          uint32_t source_mds_id, uint64_t max_seqno);

/** Load per-MDS delta seqno counter from mds_meta. */
int rondb_shim_delta_seqno_load(void *handle, uint32_t mds_id,
                                uint64_t *seqno);

/** Persist per-MDS delta seqno counter to mds_meta. */
int rondb_shim_delta_seqno_save(void *handle, uint32_t mds_id,
                                uint64_t seqno);

/* -----------------------------------------------------------------------
 * Phase 9C -- Background image poller
 * ----------------------------------------------------------------------- */

struct catalog_image;

/**
 * Start the background delta broadcast poller thread.
 *
 * Polls mds_delta_broadcast for foreign MDS streams, deserialises
 * payloads, and applies them to the local catalog_image.
 *
 * @param cat              Catalogue handle (RonDB backend).
 * @param img              Catalog image to apply deltas to.
 * @param self_mds_id      This MDS's ID (skip own deltas).
 * @param poll_interval_ms Polling cadence (default 50).
 * @return 0 on success, -1 on error.
 */
int catalogue_rondb_poller_start(struct mds_catalogue *cat,
                                 struct catalog_image *img,
                                 uint32_t self_mds_id,
                                 uint32_t poll_interval_ms);

/**
 * Stop the background poller and persist the delta seqno counter.
 * Blocks until the poller thread exits.
 */
void catalogue_rondb_poller_stop(struct mds_catalogue *cat);

/* -----------------------------------------------------------------------
 * Phase 9A -- Node registry C wrappers
 * ----------------------------------------------------------------------- */

/** Register this MDS in the node registry. */
enum mds_status catalogue_rondb_mds_register(struct mds_catalogue *cat,
                                             uint32_t mds_id,
                                             uint64_t boot_epoch,
                                             const char *hostname,
                                             uint16_t nfs_port,
                                             uint16_t grpc_port);

/** Update heartbeat timestamp. */
enum mds_status catalogue_rondb_mds_heartbeat(struct mds_catalogue *cat,
                                              uint32_t mds_id,
                                              uint64_t boot_epoch);

/** Deregister from node registry. */
enum mds_status catalogue_rondb_mds_deregister(struct mds_catalogue *cat,
                                               uint32_t mds_id);

/**
 * Scan mds_node_registry for nodes whose last_heartbeat_ns is
 * older than @a threshold_ns.  Callback receives
 * (mds_id, boot_epoch, last_heartbeat_ns).
 */
enum mds_status catalogue_rondb_mds_scan_stale(
    struct mds_catalogue *cat,
    uint64_t threshold_ns,
    rondb_stale_node_cb cb, void *ctx);

/* -----------------------------------------------------------------------
 * Partition map shim (subtree ownership in mds_partition_map)
 * ----------------------------------------------------------------------- */

/** Callback for partition_map_list. Return 0 to continue, non-zero to stop. */
typedef int (*rondb_partition_map_cb)(uint32_t partition_id,
                                     uint32_t owner_mds_id,
                                     uint8_t state,
                                     const char *subtree_path,
                                     void *ctx);

/** Scan all partition_map rows. */
int rondb_shim_partition_map_list(void *handle,
                                 rondb_partition_map_cb cb, void *ctx);

/** Read a single partition_map entry by partition_id. */
int rondb_shim_partition_map_get(void *handle, uint32_t partition_id,
                                uint32_t *owner_mds_id, uint8_t *state,
                                char *subtree_path, uint32_t path_cap);

/** Insert or update a partition_map entry. */
int rondb_shim_partition_map_put(void *handle, uint32_t partition_id,
                                uint32_t owner_mds_id, uint8_t state,
                                const char *subtree_path);

/** CAS: update owner_mds_id only if current owner matches expected. */
int rondb_shim_partition_map_cas(void *handle, uint32_t partition_id,
                                uint32_t expected_owner,
                                uint32_t new_owner, uint8_t new_state);

/** C wrappers for partition_map. */
enum mds_status catalogue_rondb_partition_map_list(
    struct mds_catalogue *cat, rondb_partition_map_cb cb, void *ctx);
enum mds_status catalogue_rondb_partition_map_put(
    struct mds_catalogue *cat, uint32_t partition_id,
    uint32_t owner_mds_id, uint8_t state, const char *subtree_path);
enum mds_status catalogue_rondb_partition_map_cas(
    struct mds_catalogue *cat, uint32_t partition_id,
    uint32_t expected_owner, uint32_t new_owner, uint8_t new_state);

/* -----------------------------------------------------------------------
 * Node registry list shim
 * ----------------------------------------------------------------------- */

/** Callback for mds_list. Return 0 to continue, non-zero to stop. */
typedef int (*rondb_mds_list_cb)(uint32_t mds_id, uint64_t boot_epoch,
                                const char *hostname,
                                uint16_t nfs_port, uint16_t grpc_port,
                                uint64_t last_heartbeat_ns, void *ctx);

/** Scan all node_registry rows. */
int rondb_shim_mds_list(void *handle, rondb_mds_list_cb cb, void *ctx);

/** C wrapper. */
enum mds_status catalogue_rondb_mds_list(
    struct mds_catalogue *cat, rondb_mds_list_cb cb, void *ctx);

/* -----------------------------------------------------------------------
 * Phase 2/3 fused operation C wrappers
 *
 * These combine multiple NDB operations into fewer round-trips.
 * ----------------------------------------------------------------------- */

struct mds_ds_map_entry;
struct nfs4_stateid;
struct ds_prealloc_ctx;

/** Phase 2: Fused stripe_get + layout_grant in one NDB transaction.
 *  Reads stripe map header+entries, then writes layout_state + indexes.
 *  Returns MDS_OK, MDS_ERR_NOTFOUND (no stripe map), or MDS_ERR_IO. */
enum mds_status catalogue_rondb_layoutget_fused(
    struct mds_catalogue *cat, uint64_t fileid,
    uint32_t *stripe_count, uint32_t *stripe_unit,
    uint32_t *mirror_count, struct mds_ds_map_entry **entries,
    const struct nfs4_stateid *stateid,
    uint64_t clientid, uint32_t iomode, uint64_t offset,
    uint64_t length, uint32_t mds_id);

/** Phase 3: ns_create with optional layout pre-grant.
 *  When layout_clientid != 0, piggybacks layout_state + indexes into the
 *  same NDB transaction.  Sets *layout_ok to true if grant succeeded. */
enum mds_status catalogue_rondb_ns_create_with_layout(
    struct mds_catalogue *cat,
    uint64_t parent_fileid, const char *name,
    enum mds_file_type type,
    uint32_t mode, uint64_t uid, uint64_t gid,
    struct ds_prealloc_ctx *prealloc,
    struct mds_inode *out,
    uint64_t layout_clientid, uint32_t layout_iomode,
    uint64_t layout_offset, uint64_t layout_length,
    const struct nfs4_stateid *layout_stateid,
    uint32_t layout_mds_id,
    bool *layout_ok);

/**
 * Run RonDB bootstrap via the catalogue handle.
 *
 * @param cat  Catalogue handle (must be RonDB backend).
 * @return MDS_OK on success.
 */
enum mds_status mds_rondb_bootstrap(struct mds_catalogue *cat);
enum mds_status mds_rondb_cleanup(struct mds_catalogue *cat);

/* -----------------------------------------------------------------------
 * Shared protocol state shim CRUD (shared-attr)
 *
 * Each function operates on a single NDB transaction.
 * Returns: 0 = success, 1 = NOTFOUND, -1 = error.
 * ----------------------------------------------------------------------- */

struct mds_coord_open_row;
struct mds_coord_lock_row;
struct mds_coord_deleg_row;
struct mds_coord_client_row;
struct mds_coord_session_row;
struct mds_coord_drc_slot_row;

/** Atomic xattr put + inode ctime/change touch (Stage 7). */
int rondb_shim_xattr_put_atomic(void *handle, uint64_t fileid,
                                const char *name,
                                const void *val, uint32_t vallen);

/** Atomic xattr del + inode ctime/change touch (Stage 7). */
int rondb_shim_xattr_del_atomic(void *handle, uint64_t fileid,
                                const char *name);

/* Open state */
int rondb_shim_open_put(void *handle, const struct mds_coord_open_row *row);
int rondb_shim_open_get(void *handle, const uint8_t stateid_other[12],
                        struct mds_coord_open_row *row);
int rondb_shim_open_del(void *handle, const uint8_t stateid_other[12]);
typedef int (*rondb_open_scan_cb)(const struct mds_coord_open_row *row,
                                  void *ctx);
int rondb_shim_open_scan_file(void *handle, uint64_t fileid,
                              rondb_open_scan_cb cb, void *ctx);
int rondb_shim_open_scan_client(void *handle, uint64_t clientid,
                                rondb_open_scan_cb cb, void *ctx);

/* Byte-range locks */
int rondb_shim_bytelock_put(void *handle, const struct mds_coord_lock_row *row);
int rondb_shim_bytelock_del(void *handle, uint64_t fileid, uint64_t lock_id);
typedef int (*rondb_lock_scan_cb)(const struct mds_coord_lock_row *row,
                                  void *ctx);
int rondb_shim_bytelock_scan_file(void *handle, uint64_t fileid,
                                  rondb_lock_scan_cb cb, void *ctx);
int rondb_shim_bytelock_reap_client(void *handle, uint64_t clientid);

/* Delegations */
int rondb_shim_deleg_put(void *handle, const struct mds_coord_deleg_row *row);
int rondb_shim_deleg_get(void *handle, const uint8_t stateid_other[12],
                         struct mds_coord_deleg_row *row);
int rondb_shim_deleg_del(void *handle, const uint8_t stateid_other[12]);
typedef int (*rondb_deleg_scan_cb)(const struct mds_coord_deleg_row *row,
                                   void *ctx);
int rondb_shim_deleg_scan_file(void *handle, uint64_t fileid,
                               rondb_deleg_scan_cb cb, void *ctx);
int rondb_shim_deleg_scan_client(void *handle, uint64_t clientid,
                                 rondb_deleg_scan_cb cb, void *ctx);

/* Clients */
int rondb_shim_client_put(void *handle, const struct mds_coord_client_row *row);
int rondb_shim_client_get(void *handle, uint64_t clientid,
                          struct mds_coord_client_row *row);
int rondb_shim_client_del(void *handle, uint64_t clientid);

/* Sessions */
int rondb_shim_session_put(void *handle, const struct mds_coord_session_row *row);
int rondb_shim_session_get(void *handle, const uint8_t session_id[16],
                           struct mds_coord_session_row *row);
int rondb_shim_session_del(void *handle, const uint8_t session_id[16]);
typedef int (*rondb_session_scan_cb)(const struct mds_coord_session_row *row,
                                     void *ctx);
int rondb_shim_session_scan_client(void *handle, uint64_t clientid,
                                   rondb_session_scan_cb cb, void *ctx);

/* DRC slots */
int rondb_shim_drc_slot_put(void *handle, const uint8_t session_id[16],
                            uint32_t slot_id, uint32_t seq_id,
                            const void *cached_reply, uint32_t reply_len);
int rondb_shim_drc_slot_get(void *handle, const uint8_t session_id[16],
                            uint32_t slot_id,
                            struct mds_coord_drc_slot_row *row);

#endif /* CATALOGUE_RONDB_H */

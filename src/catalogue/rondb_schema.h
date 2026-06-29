/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * rondb_schema.h -- RonDB table layout, column names, and serialization.
 *
 * Pure C, no NDB dependency.  All table/column name constants are
 * shared between the C++ shim (DDL) and the C wrapper (encoding).
 *
 * Inode serialization uses big-endian helpers from endian_helpers.h.
 * The byte layout adds home_shard_id (4 bytes) after parent_fileid,
 * giving a fixed size of 137 bytes per inode.
 */

#ifndef RONDB_SCHEMA_H
#define RONDB_SCHEMA_H

#include <stdint.h>
#include <stddef.h>
#include "pnfs_mds.h"
#include "mds_coordination.h"
#include "endian_helpers.h"  /* Big-endian helpers: fdb_put_u32/u64/i64, fdb_get_* */

/* -----------------------------------------------------------------------
 * Schema version
 * ----------------------------------------------------------------------- */

/*
 * v5 -> v6 (2026):
 *   - mds_layout_state PK changed from (stateid_other) to
 *     (fileid, stateid_other), partitioned by fileid.
 *   - Added ordered indices ix_layout_state_stateid and
 *     ix_layout_state_clientid to cover the former roles of
 *     mds_layout_by_client and lookup-by-stateid.
 *   - mds_layout_by_client table DROPPED (redundant with new index).
 *   This change colocates every write in the fused
 *   rondb_shim_ns_create_with_layout onto at most two partitions
 *   (parent + child), eliminating the NoCommit flush that caused
 *   the disabled 36 vs 73 ops/sec regression.
 *
 *   Upgrade from v5: mds_rondb_bootstrap drops the old layout_state
 *   + layout_by_client tables and lets the DDL recreate layout_state
 *   with the new schema.  Existing rows are lost; this is acceptable
 *   because layout_state is transient protocol state rebuilt by
 *   clients on reconnect (it's in the same class as open_state,
 *   which is also drop-on-restart in transient_state_cache mode).
 */
#define RONDB_SCHEMA_VERSION  6

/* -----------------------------------------------------------------------
 * Table names
 * ----------------------------------------------------------------------- */

#define RONDB_TBL_META            "mds_meta"
#define RONDB_TBL_INODES          "mds_inodes"
#define RONDB_TBL_DIRENTS         "mds_dirents"
#define RONDB_TBL_STRIPE_MAPS     "mds_stripe_maps"
#define RONDB_TBL_STRIPE_ENTRIES  "mds_stripe_entries"
#define RONDB_TBL_RENAME_JOURNAL  "mds_rename_journal"
#define RONDB_TBL_NS_LOCKS        "mds_ns_locks"
#define RONDB_TBL_NS_LOCK_HOLDERS "mds_ns_lock_holders"
#define RONDB_TBL_PARTITION_MAP   "mds_partition_map"
/* Phase 1 tables (catalogue + coordination parity) */
/* mds_inline_data intentionally omitted -- inline data defeats the
 * pNFS architecture.  RonDB mode forces inline_enabled=false so all
 * file data goes through DS layout paths. */
#define RONDB_TBL_XATTRS          "mds_xattrs"
#define RONDB_TBL_DS_REGISTRY     "mds_ds_registry"
#define RONDB_TBL_DS_PROVISION    "mds_ds_provision"
#define RONDB_TBL_QUOTA_RULES     "mds_quota_rules"
#define RONDB_TBL_QUOTA_USAGE     "mds_quota_usage"
#define RONDB_TBL_GC_QUEUE        "mds_gc_queue"
#define RONDB_TBL_LAYOUT_STATE    "mds_layout_state"
#define RONDB_TBL_LAYOUT_BY_CLIENT "mds_layout_by_client"
#define RONDB_TBL_LAYOUT_BY_FILE  "mds_layout_by_file"
#define RONDB_TBL_DS_LAYOUT_IDX   "mds_ds_layout_idx"
#define RONDB_TBL_CLIENT_RECOVERY "mds_client_recovery"
/* Phase 9 tables */
#define RONDB_TBL_NODE_REGISTRY   "mds_node_registry"
#define RONDB_TBL_DELTA_BROADCAST "mds_delta_broadcast"
/* Shared protocol state tables (shared-attr) */
#define RONDB_TBL_OPEN_STATE      "mds_open_state"
#define RONDB_TBL_OPEN_BY_FILE    "mds_open_by_file"
#define RONDB_TBL_OPEN_BY_CLIENT  "mds_open_by_client"
#define RONDB_TBL_BYTE_LOCKS      "mds_byte_range_locks"
#define RONDB_TBL_LOCK_BY_OWNER   "mds_lock_by_owner"
#define RONDB_TBL_DELEGATIONS     "mds_delegations"
#define RONDB_TBL_DELEG_BY_FILE   "mds_deleg_by_file"
#define RONDB_TBL_DELEG_BY_CLIENT "mds_deleg_by_client"
#define RONDB_TBL_SESSIONS        "mds_sessions"
#define RONDB_TBL_SESSION_BY_CLIENT "mds_session_by_client"
#define RONDB_TBL_CLIENTS         "mds_clients"
#define RONDB_TBL_DRC_SLOTS       "mds_drc_slots"
#define RONDB_TABLE_COUNT  35

/* -----------------------------------------------------------------------
 * Column names -- mds_meta
 * ----------------------------------------------------------------------- */

#define RONDB_META_COL_KEY       "key_name"
#define RONDB_META_COL_VAL       "val_u64"

#define RONDB_META_KEY_SCHEMA    "schema_version"
#define RONDB_META_KEY_FILEID    "fileid_counter"
#define RONDB_META_KEY_GC_SEQ    "gc_seq_counter"
#define RONDB_META_KEY_DELTA_SEQ "delta_seqno_counter"

/* -----------------------------------------------------------------------
 * Column names -- mds_inodes
 * ----------------------------------------------------------------------- */

#define RONDB_INO_COL_FILEID       "fileid"
#define RONDB_INO_COL_TYPE         "inode_type"
#define RONDB_INO_COL_MODE         "mode"
#define RONDB_INO_COL_NLINK        "nlink"
#define RONDB_INO_COL_UID          "uid"
#define RONDB_INO_COL_GID          "gid"
#define RONDB_INO_COL_FILE_SIZE    "file_size"
#define RONDB_INO_COL_SPACE_USED   "space_used"
#define RONDB_INO_COL_ATIME_SEC    "atime_sec"
#define RONDB_INO_COL_ATIME_NSEC   "atime_nsec"
#define RONDB_INO_COL_MTIME_SEC    "mtime_sec"
#define RONDB_INO_COL_MTIME_NSEC   "mtime_nsec"
#define RONDB_INO_COL_CTIME_SEC    "ctime_sec"
#define RONDB_INO_COL_CTIME_NSEC   "ctime_nsec"
#define RONDB_INO_COL_CHANGE       "change_ctr"
#define RONDB_INO_COL_GENERATION   "generation"
#define RONDB_INO_COL_FLAGS        "flags"
#define RONDB_INO_COL_CREATE_VERF  "create_verf"
#define RONDB_INO_COL_PARENT       "parent_fileid"
#define RONDB_INO_COL_HOME_SHARD   "home_shard_id"

/* -----------------------------------------------------------------------
 * Column names -- mds_dirents
 * ----------------------------------------------------------------------- */

#define RONDB_DIR_COL_PARENT      "parent_fileid"
#define RONDB_DIR_COL_NAME        "entry_name"
#define RONDB_DIR_COL_CHILD_FID   "child_fileid"
#define RONDB_DIR_COL_CHILD_TYPE  "child_type"

/* Ordered index on (parent_fileid, child_fileid).  Enables O(log N +
 * page) READDIR cookie resume by child fileid (range scan child_fileid
 * > cookie, ascending).  Additive: created idempotently at bootstrap,
 * no row-format change and no schema-version bump required. */
#define RONDB_IX_DIRENTS_PARENT_CHILD  "ix_dirents_parent_child"

/* -----------------------------------------------------------------------
 * Column names -- mds_stripe_maps (header)
 * ----------------------------------------------------------------------- */

#define RONDB_SM_COL_FILEID       "fileid"
#define RONDB_SM_COL_STRIPE_CNT   "stripe_count"
#define RONDB_SM_COL_STRIPE_UNIT  "stripe_unit"
#define RONDB_SM_COL_MIRROR_CNT   "mirror_count"

/* -----------------------------------------------------------------------
 * Column names -- mds_stripe_entries (child)
 * ----------------------------------------------------------------------- */

#define RONDB_SE_COL_FILEID       "fileid"
#define RONDB_SE_COL_ORDINAL      "ordinal"
#define RONDB_SE_COL_DS_ID        "ds_id"
#define RONDB_SE_COL_NFS_FH_LEN   "nfs_fh_len"
#define RONDB_SE_COL_NFS_FH       "nfs_fh"

/* -----------------------------------------------------------------------
 * Column names -- mds_rename_journal
 * ----------------------------------------------------------------------- */

#define RONDB_RJ_COL_TXN_ID       "txn_id"
#define RONDB_RJ_COL_STATE        "state"
#define RONDB_RJ_COL_ROLE         "role"
#define RONDB_RJ_COL_COORD_MDS    "coordinator_mds_id"
#define RONDB_RJ_COL_SRC_PARENT   "src_parent_fileid"
#define RONDB_RJ_COL_DST_PARENT   "dst_parent_fileid"
#define RONDB_RJ_COL_SRC_CHILD    "src_child_fileid"
#define RONDB_RJ_COL_SRC_NAME     "src_name"
#define RONDB_RJ_COL_DST_NAME     "dst_name"
#define RONDB_RJ_COL_INODE_SNAP   "inode_snapshot"
#define RONDB_RJ_COL_CREATED_AT   "created_at_ns"
#define RONDB_RJ_PAYLOAD_MAX      MDS_COORD_JOURNAL_PAYLOAD_MAX

/* Rename journal states */
#define RONDB_RJ_STATE_PREPARED   0
#define RONDB_RJ_STATE_COMMITTED  1
#define RONDB_RJ_STATE_ABORTED    2

/* Rename journal roles */
#define RONDB_RJ_ROLE_COORDINATOR 0
#define RONDB_RJ_ROLE_PARTICIPANT 1

/* -----------------------------------------------------------------------
 * Column names -- mds_ns_locks
 * ----------------------------------------------------------------------- */

#define RONDB_LK_COL_PART_HINT    "partition_hint"
#define RONDB_LK_COL_RES_CLASS    "resource_class"
#define RONDB_LK_COL_RES_KEY      "resource_key"
#define RONDB_LK_COL_LOCK_MODE    "lock_mode"
#define RONDB_LK_COL_HOLDER_COUNT "holder_count"
#define RONDB_LK_COL_OWNER_MDS    "owner_mds_id"
#define RONDB_LK_COL_OWNER_EPOCH  "owner_epoch"
#define RONDB_LK_COL_FENCE_EPOCH  "fencing_epoch"
#define RONDB_LK_COL_GRANTED_AT   "granted_at_ns"
#define RONDB_LK_COL_TTL_MS       "ttl_ms"

/* Lock resource classes (ascending order = acquisition order).
 * Topology lock (class 0) is acquired FIRST to prevent
 * directory rename races.  All multi-resource operations
 * acquire locks sorted by (class, partition_hint, key_bytes)
 * in ascending order.
 *
 * Classes 0--3: namespace structure mutations (Phase 9B).
 * Classes 4--7: shared protocol state (shared-attr). */
#define RONDB_LOCK_CLASS_TOPOLOGY        0
#define RONDB_LOCK_CLASS_PARENT_NAME     1
#define RONDB_LOCK_CLASS_DIR_MUTATION    2
#define RONDB_LOCK_CLASS_INODE_ATTR      3
#define RONDB_LOCK_CLASS_FILE_STATE      4  /**< OPEN/CLOSE/setattr/deleg per fileid. */
#define RONDB_LOCK_CLASS_LOCK_TABLE      5  /**< LOCK/LOCKT/LOCKU per fileid. */
#define RONDB_LOCK_CLASS_CLIENT_SESSION  6  /**< EXCHANGE_ID/session per clientid. */
#define RONDB_LOCK_CLASS_SLOT_DRC        7  /**< SEQUENCE per (session_id, slot_id). */

/* Lock modes */
#define RONDB_LOCK_MODE_SHARED    1
#define RONDB_LOCK_MODE_EXCLUSIVE 2

/* Topology lock sentinel */
#define RONDB_TOPOLOGY_SENTINEL   UINT64_C(0xFFFFFFFFFFFFFFFF)

/* -----------------------------------------------------------------------
 * Column names -- mds_partition_map
 * ----------------------------------------------------------------------- */

#define RONDB_PM_COL_PART_ID      "partition_id"
#define RONDB_PM_COL_OWNER_MDS    "owner_mds_id"
#define RONDB_PM_COL_STATE        "state"
#define RONDB_PM_COL_SUBTREE      "subtree_path"

/* Partition states */
#define RONDB_PM_STATE_ACTIVE     0
#define RONDB_PM_STATE_MIGRATING  1
#define RONDB_PM_STATE_FROZEN     2

/* -----------------------------------------------------------------------
 * Column names -- mds_xattrs
 *
 * Max name  = MDS_XATTR_NAME_MAX (255, pnfs_mds.h).
 * Max value = MDS_XATTR_VAL_MAX  (65536, pnfs_mds.h).
 * ----------------------------------------------------------------------- */

#define RONDB_XA_COL_FILEID       "fileid"
#define RONDB_XA_COL_ATTR_NAME    "attr_name"
#define RONDB_XA_COL_VALUE        "value"

/* -----------------------------------------------------------------------
 * Column names -- mds_ds_registry (typed, matches struct mds_ds_info)
 * ----------------------------------------------------------------------- */

#define RONDB_DSR_COL_DS_ID       "ds_id"
#define RONDB_DSR_COL_STATE       "state"
#define RONDB_DSR_COL_TIER        "tier"
#define RONDB_DSR_COL_TOTAL_BYTES "total_bytes"
#define RONDB_DSR_COL_USED_BYTES  "used_bytes"
#define RONDB_DSR_COL_PORT        "port"
#define RONDB_DSR_COL_ADDR        "addr"
#define RONDB_DSR_COL_MODE        "mode"
#define RONDB_DSR_COL_TRANSPORT   "transport"
#define RONDB_DSR_COL_HOST        "host"
#define RONDB_DSR_COL_EXPORT_PATH "export_path"
#define RONDB_DSR_COL_TCP_PORT    "tcp_port"
#define RONDB_DSR_COL_RDMA_PORT   "rdma_port"
#define RONDB_DSR_COL_CAPS        "capabilities"

/* -----------------------------------------------------------------------
 * Column names -- mds_ds_provision (typed)
 * ----------------------------------------------------------------------- */

#define RONDB_DSP_COL_DS_ID       "ds_id"
#define RONDB_DSP_COL_SECRET      "secret"
#define RONDB_DSP_COL_EPOCH       "epoch"

/* -----------------------------------------------------------------------
 * Column names -- mds_quota_rules (typed, matches struct mds_quota_rule)
 * ----------------------------------------------------------------------- */

#define RONDB_QR_COL_SCOPE_TYPE   "scope_type"
#define RONDB_QR_COL_SCOPE_ID     "scope_id"
#define RONDB_QR_COL_HARD_BYTES   "hard_bytes"
#define RONDB_QR_COL_SOFT_BYTES   "soft_bytes"
#define RONDB_QR_COL_HARD_INODES  "hard_inodes"
#define RONDB_QR_COL_SOFT_INODES  "soft_inodes"
#define RONDB_QR_COL_GRACE_SEC    "grace_sec"

/* -----------------------------------------------------------------------
 * Column names -- mds_quota_usage (typed, matches struct mds_quota_usage)
 * ----------------------------------------------------------------------- */

#define RONDB_QU_COL_USAGE_TYPE   "usage_type"
#define RONDB_QU_COL_SCOPE_ID     "scope_id"
#define RONDB_QU_COL_USED_BYTES   "used_bytes"
#define RONDB_QU_COL_USED_INODES  "used_inodes"
#define RONDB_QU_COL_GRACE_BYTES  "grace_start_bytes"
#define RONDB_QU_COL_GRACE_INODES "grace_start_inodes"

/* -----------------------------------------------------------------------
 * Column names -- mds_gc_queue
 *
 * FIFO queue keyed by monotonic gc_seq from mds_meta gc_seq_counter.
 * ----------------------------------------------------------------------- */

#define RONDB_GC_COL_SEQ          "gc_seq"
#define RONDB_GC_COL_FILEID       "fileid"
#define RONDB_GC_COL_DS_ID        "ds_id"
#define RONDB_GC_COL_NFS_FH_LEN   "nfs_fh_len"
#define RONDB_GC_COL_NFS_FH       "nfs_fh"
/*
 * owner_mds_id: the MDS that enqueued this GC entry.  In multi-MDS
 * deployments each MDS drains only the rows it owns, so GC workers on
 * different MDSes never scan/contend the same entries.  Legacy rows
 * written before this column existed read back as 0; the drainer treats
 * owner_mds_id == 0 as "unassigned" and any MDS may reclaim them.
 */
#define RONDB_GC_COL_OWNER_MDS    "owner_mds_id"

/* -----------------------------------------------------------------------
 * Column names -- mds_prealloc_pool
 *
 * Persisted ring of precreated DS stub files for ENABLE_DS_PREALLOC.
 * PK = fileid.  owner_mds_id scopes the rows to the MDS that created
 * them so each MDS only recovers/reclaims its own slots on restart.
 * ----------------------------------------------------------------------- */

#define RONDB_TBL_PREALLOC_POOL   "mds_prealloc_pool"
#define RONDB_PP_COL_FILEID       "fileid"
#define RONDB_PP_COL_DS_ID        "ds_id"
#define RONDB_PP_COL_OWNER_MDS    "owner_mds_id"
#define RONDB_PP_COL_STRIPE_UNIT  "stripe_unit"
#define RONDB_PP_COL_NFS_FH_LEN   "nfs_fh_len"
#define RONDB_PP_COL_NFS_FH       "nfs_fh"

/* -----------------------------------------------------------------------
 * Column names -- mds_layout_state
 * PK      = (fileid, stateid_other)
 * partition key = fileid
 * indices = ix_layout_state_stateid (stateid_other),
 *           ix_layout_state_clientid (clientid)
 * ----------------------------------------------------------------------- */

#define RONDB_LS_COL_STATEID      "stateid_other"
#define RONDB_LS_COL_CLIENTID     "clientid"
#define RONDB_LS_COL_FILEID       "fileid"
#define RONDB_LS_COL_IOMODE       "iomode"
#define RONDB_LS_COL_OFFSET       "offset"
#define RONDB_LS_COL_LENGTH       "length"
#define RONDB_LS_COL_SEQID        "seqid"

#define RONDB_IX_LS_STATEID       "ix_layout_state_stateid"
#define RONDB_IX_LS_CLIENTID      "ix_layout_state_clientid"

/* -----------------------------------------------------------------------
 * Column names -- mds_layout_by_client (index: clientid -> stateid)
 * ----------------------------------------------------------------------- */

#define RONDB_LBC_COL_CLIENTID    "clientid"
#define RONDB_LBC_COL_STATEID     "stateid_other"

/* -----------------------------------------------------------------------
 * Column names -- mds_layout_by_file (index: fileid -> stateid)
 * ----------------------------------------------------------------------- */

#define RONDB_LBF_COL_FILEID      "fileid"
#define RONDB_LBF_COL_STATEID     "stateid_other"
#define RONDB_IX_LBF_FILEID       "ix_layout_by_file_fileid"

/* -----------------------------------------------------------------------
 * Column names -- mds_ds_layout_idx (index: ds_id -> client+file)
 * ----------------------------------------------------------------------- */

#define RONDB_DLI_COL_DS_ID       "ds_id"
#define RONDB_DLI_COL_CLIENTID    "clientid"
#define RONDB_DLI_COL_FILEID      "fileid"

/* -----------------------------------------------------------------------
 * Column names -- mds_client_recovery
 * ----------------------------------------------------------------------- */

#define RONDB_CR_COL_CLIENTID     "clientid"
#define RONDB_CR_COL_CO_OWNERID   "co_ownerid"
#define RONDB_CR_COL_VERIFIER     "verifier"
#define RONDB_CR_COL_OWNER_MDS    "owner_mds_id"
#define RONDB_CR_COL_OWNER_EPOCH  "owner_boot_epoch"

/* -----------------------------------------------------------------------
 * Column names -- mds_layout_state (Phase 9 additions)
 * ----------------------------------------------------------------------- */

#define RONDB_LS_COL_GRANT_MDS    "grant_owner_mds_id"
#define RONDB_LS_COL_GRANT_EPOCH  "grant_owner_boot_epoch"

/* -----------------------------------------------------------------------
 * Column names -- mds_node_registry (Phase 9)
 *
 * PK = mds_id (uint32).
 * boot_epoch distinguishes daemon reincarnations with the same mds_id.
 * ----------------------------------------------------------------------- */

#define RONDB_NR_COL_MDS_ID       "mds_id"
#define RONDB_NR_COL_BOOT_EPOCH   "boot_epoch"
#define RONDB_NR_COL_HOSTNAME     "hostname"
#define RONDB_NR_COL_NFS_PORT     "nfs_port"
#define RONDB_NR_COL_GRPC_PORT    "grpc_port"
#define RONDB_NR_COL_STATE        "state"
#define RONDB_NR_COL_HEARTBEAT_NS "last_heartbeat_ns"
#define RONDB_NR_COL_SW_VERSION   "software_version"

/* Node registry states */
#define RONDB_NR_STATE_ACTIVE     0
#define RONDB_NR_STATE_STANDBY    1
#define RONDB_NR_STATE_DRAINING   2

/* -----------------------------------------------------------------------
 * Column names -- mds_delta_broadcast (Phase 9)
 *
 * PK = (source_mds_id, seqno).  boot_epoch is non-PK.
 * seqno is globally monotonic per MDS (never resets on restart).
 * ----------------------------------------------------------------------- */

#define RONDB_DB_COL_SOURCE_MDS   "source_mds_id"
#define RONDB_DB_COL_SEQNO        "seqno"
#define RONDB_DB_COL_BOOT_EPOCH   "boot_epoch"
#define RONDB_DB_COL_DELTA_TYPE   "delta_type"
#define RONDB_DB_COL_PAYLOAD      "payload"
#define RONDB_DB_COL_TIMESTAMP_NS "timestamp_ns"

/* -----------------------------------------------------------------------
 * Inode serialisation
 *
 * Fixed layout (big-endian), extends FDB format with home_shard_id:
 *   fileid(8) type(1) mode(4) nlink(4) uid(8) gid(8)
 *   size(8) space_used(8)
 *   atime_sec(8) atime_nsec(4)
 *   mtime_sec(8) mtime_nsec(4)
 *   ctime_sec(8) ctime_nsec(4)
 *   change(8) generation(8)
 *   flags(4) create_verf(8) parent_fileid(8)
 *   home_shard_id(4)
 * Total: 137 bytes
 * ----------------------------------------------------------------------- */

#define RONDB_INODE_FIXED_SIZE  137
#define RONDB_INODE_MAX_SIZE    256

/** Serialize an inode to buf. Returns bytes written, or -1 on error. */
int rondb_inode_serialize(const struct mds_inode *inode,
                          uint32_t home_shard_id,
                          uint8_t *buf, size_t cap);

/** Deserialize buf into inode + home_shard_id. Returns 0 on success. */
int rondb_inode_deserialize(const uint8_t *buf, size_t len,
                            struct mds_inode *inode,
                            uint32_t *home_shard_id);

/* -----------------------------------------------------------------------
 * Stripe entry serialisation (per-row, for mds_stripe_entries child table)
 *
 * Layout: ds_id(4) + nfs_fh_len(4) + nfs_fh(MDS_NFS_FH_MAX)
 * ----------------------------------------------------------------------- */

#define RONDB_STRIPE_ENTRY_SIZE  (4 + 4 + MDS_NFS_FH_MAX)

int rondb_stripe_entry_serialize(const struct mds_ds_map_entry *entry,
                                 uint8_t *buf, size_t cap);

int rondb_stripe_entry_deserialize(const uint8_t *buf, size_t len,
                                   struct mds_ds_map_entry *entry);

/* -----------------------------------------------------------------------
 * Rename journal entry serialisation
 *
 * Layout: txn_id(8) state(1) role(1) coordinator_mds_id(4)
 *         src_parent(8) dst_parent(8) src_child(8)
 *         src_name_len(2) dst_name_len(2)
 *         src_name(var) dst_name(var) created_at(8)
 * ----------------------------------------------------------------------- */

#define RONDB_RJ_FIXED_SIZE  50  /* before variable-length names */
#define RONDB_RJ_MAX_SIZE    (RONDB_RJ_FIXED_SIZE + 255 + 255)

struct rondb_rename_journal_entry {
    uint64_t txn_id;
    uint8_t  state;
    uint8_t  role;
    uint32_t coordinator_mds_id;
    uint64_t src_parent_fileid;
    uint64_t dst_parent_fileid;
    uint64_t src_child_fileid;
    char     src_name[MDS_MAX_NAME + 1];
    char     dst_name[MDS_MAX_NAME + 1];
    uint64_t created_at_ns;
};

int rondb_rj_serialize(const struct rondb_rename_journal_entry *entry,
                       uint8_t *buf, size_t cap);

int rondb_rj_deserialize(const uint8_t *buf, size_t len,
                         struct rondb_rename_journal_entry *entry);

/* -----------------------------------------------------------------------
 * Column names -- mds_open_state (shared-attr)
 *
 * PK = stateid_other (12B Varbinary).
 * Secondary indexes: mds_open_by_file (fileid), mds_open_by_client (clientid).
 * ----------------------------------------------------------------------- */

#define RONDB_OS_COL_STATEID       "stateid_other"
#define RONDB_OS_COL_SEQID         "seqid"
#define RONDB_OS_COL_CLIENTID      "clientid"
#define RONDB_OS_COL_FILEID        "fileid"
#define RONDB_OS_COL_SHARE_ACCESS  "share_access"
#define RONDB_OS_COL_SHARE_DENY    "share_deny"
#define RONDB_OS_COL_OPEN_OWNER    "open_owner"
#define RONDB_OS_COL_OWNER_LEN     "open_owner_len"
#define RONDB_OS_COL_OWNER_MDS     "owner_mds_id"
#define RONDB_OS_COL_OWNER_EPOCH   "owner_boot_epoch"

/* -----------------------------------------------------------------------
 * Column names -- mds_byte_range_locks (shared-attr)
 *
 * PK = (fileid, lock_id).  lock_id is a monotonic per-file counter.
 * Secondary index: mds_lock_by_owner (clientid, owner_hash).
 * ----------------------------------------------------------------------- */

#define RONDB_BL_COL_FILEID        "fileid"
#define RONDB_BL_COL_LOCK_ID       "lock_id"
#define RONDB_BL_COL_OFFSET        "offset"
#define RONDB_BL_COL_LENGTH        "length"
#define RONDB_BL_COL_LOCK_TYPE     "lock_type"
#define RONDB_BL_COL_CLIENTID      "clientid"
#define RONDB_BL_COL_OWNER         "owner"
#define RONDB_BL_COL_OWNER_LEN     "owner_len"
#define RONDB_BL_COL_STATEID       "stateid_other"
#define RONDB_BL_COL_SEQID         "seqid"
#define RONDB_BL_COL_OPEN_STATEID  "open_stateid_other"
#define RONDB_BL_COL_OWNER_MDS     "owner_mds_id"
#define RONDB_BL_COL_OWNER_EPOCH   "owner_boot_epoch"

/* -----------------------------------------------------------------------
 * Column names -- mds_delegations (shared-attr)
 *
 * PK = stateid_other (12B).
 * Secondary indexes: mds_deleg_by_file (fileid), mds_deleg_by_client (clientid).
 * ----------------------------------------------------------------------- */

#define RONDB_DG_COL_STATEID       "stateid_other"
#define RONDB_DG_COL_SEQID         "seqid"
#define RONDB_DG_COL_CLIENTID      "clientid"
#define RONDB_DG_COL_FILEID        "fileid"
#define RONDB_DG_COL_DELEG_TYPE    "deleg_type"
#define RONDB_DG_COL_OWNER_MDS     "owner_mds_id"
#define RONDB_DG_COL_OWNER_EPOCH   "owner_boot_epoch"
#define RONDB_DG_COL_GRANT_TIME    "grant_time_ns"
#define RONDB_DG_COL_RECALL_PEND   "recall_pending"

/* -----------------------------------------------------------------------
 * Column names -- mds_clients (shared-attr)
 *
 * PK = clientid (u64).
 * ----------------------------------------------------------------------- */

#define RONDB_CL_COL_CLIENTID      "clientid"
#define RONDB_CL_COL_CO_OWNERID   "co_ownerid"
#define RONDB_CL_COL_VERIFIER     "verifier"
#define RONDB_CL_COL_CONFIRMED    "confirmed"
#define RONDB_CL_COL_OWNER_MDS    "owner_mds_id"
#define RONDB_CL_COL_OWNER_EPOCH  "owner_boot_epoch"
#define RONDB_CL_COL_LEASE_NS     "lease_renewed_ns"

/* -----------------------------------------------------------------------
 * Column names -- mds_sessions (shared-attr)
 *
 * PK = session_id (16B Varbinary).
 * Secondary index: mds_session_by_client (clientid).
 * ----------------------------------------------------------------------- */

#define RONDB_SS_COL_SESSION_ID    "session_id"
#define RONDB_SS_COL_CLIENTID      "clientid"
#define RONDB_SS_COL_NUM_SLOTS     "num_slots"
#define RONDB_SS_COL_CB_PROG       "cb_prog"
#define RONDB_SS_COL_CB_SEC        "cb_sec_flavor"
#define RONDB_SS_COL_OWNER_MDS     "owner_mds_id"
#define RONDB_SS_COL_OWNER_EPOCH   "owner_boot_epoch"
#define RONDB_SS_COL_CREATED_NS    "created_ns"

/* -----------------------------------------------------------------------
 * Column names -- mds_drc_slots (shared-attr)
 *
 * PK = (session_id, slot_id).
 * cached_reply is a Blob column (NDB Blob API).
 * ----------------------------------------------------------------------- */

#define RONDB_DR_COL_SESSION_ID    "session_id"
#define RONDB_DR_COL_SLOT_ID       "slot_id"
#define RONDB_DR_COL_SEQ_ID        "seq_id"
#define RONDB_DR_COL_REPLY         "cached_reply"
#define RONDB_DR_COL_REPLY_LEN     "reply_len"
#define RONDB_DR_COL_LAST_USED_NS  "last_used_ns"

/* -----------------------------------------------------------------------
 * Lock resource key builders
 *
 * Each builder writes a class-specific key into buf and returns the
 * key length, or -1 on error.  The key is used as the resource_key
 * column in mds_ns_locks.
 * ----------------------------------------------------------------------- */

#define RONDB_LOCK_KEY_MAX  272  /* Class 1: fileid(8) + name(up to 255) + margin */

/**
 * Class 1: Parent/Name intent lock.
 * Key = parent_fileid(8) || name_bytes.
 * partition_hint = parent_fileid.
 */
int rondb_lock_res_parent_name(uint64_t parent_fileid,
                               const char *name,
                               uint8_t *buf, size_t cap,
                               uint64_t *partition_hint);

/**
 * Class 2: Directory mutation lock.
 * Key = dir_fileid(8).
 * partition_hint = dir_fileid.
 */
int rondb_lock_res_dir(uint64_t dir_fileid,
                       uint8_t *buf, size_t cap,
                       uint64_t *partition_hint);

/**
 * Class 3: Inode attribute/layout lock.
 * Key = fileid(8).
 * partition_hint = fileid.
 */
int rondb_lock_res_inode(uint64_t fileid,
                         uint8_t *buf, size_t cap,
                         uint64_t *partition_hint);

/**
 * Class 0: Global topology lock.
 * Key = sentinel(8) = 0xFFFFFFFFFFFFFFFF.
 * partition_hint = sentinel.
 */
int rondb_lock_res_topology(uint8_t *buf, size_t cap,
                            uint64_t *partition_hint);

/* -----------------------------------------------------------------------
 * Lock ordering comparator
 *
 * Returns < 0, 0, or > 0 for canonical total-order comparison.
 * Sort key: (resource_class, partition_hint, resource_key_bytes).
 * All multi-resource ops must acquire locks in ascending order.
 * ----------------------------------------------------------------------- */

struct rondb_lock_entry {
	uint8_t  resource_class;
	uint64_t partition_hint;
	uint8_t  key[RONDB_LOCK_KEY_MAX];
	int      key_len;
};

/**
 * Compare two lock entries for canonical total-order acquisition.
 * Returns negative if a < b, 0 if equal, positive if a > b.
 */
int rondb_lock_order_cmp(const struct rondb_lock_entry *a,
                         const struct rondb_lock_entry *b);

#endif /* RONDB_SCHEMA_H */

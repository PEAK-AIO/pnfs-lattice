/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * pnfs_mds.h -- Top-level header for the pNFS MDS project.
 */

#ifndef PNFS_MDS_H
#define PNFS_MDS_H

/* NFS authentication mode. */
enum nfs_auth_mode {
    NFS_AUTH_MODE_SYS   = 0,
    NFS_AUTH_MODE_KRB5  = 1,
    NFS_AUTH_MODE_KRB5I = 2,
    NFS_AUTH_MODE_KRB5P = 3,
};

/* -----------------------------------------------------------------------
 * Toolchain gate -- GCC >= 11.1 is mandatory.
 *
 * GCC 11.x ships with Rocky/RHEL 9, GCC 14.x with Rocky/RHEL 10.
 * The gate was lowered to 11.1 to cover both Rocky 9 and 10.
 * GCC 12, 14, and 15 are fully supported.
 * ----------------------------------------------------------------------- */
#if defined(__GNUC__) && !defined(__clang__)
# define PNFS_GCC_VERSION (__GNUC__ * 10000 + __GNUC_MINOR__ * 100)
# if PNFS_GCC_VERSION < 110100
_Static_assert(0, "pnfs-mds requires GCC >= 11.1 -- see docs/architecture.md section 20");
# endif
#elif !defined(__clang__)
# error "pnfs-mds requires GCC >= 11.1"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <time.h>
#include <stdio.h>
/* Component-based leveled logging interface: enum log_level,
 * enum log_component, the mds_log* prototypes, and the MDS_LOG_*
 * macros.  Implementation lives in src/common/log.c. */
#include "mds_log.h"

/* -----------------------------------------------------------------------
 * Version
 * ----------------------------------------------------------------------- */
#define PNFS_MDS_VERSION_MAJOR 0
#define PNFS_MDS_VERSION_MINOR 1
#define PNFS_MDS_VERSION_PATCH 0

#define TO_STR(x) #x
#define VERSION(maj, min, patch) TO_STR(maj) "." TO_STR(min) "." TO_STR(patch)

/* A build-time -DPNFS_MDS_VERSION="..." (set by the deb/rpm packaging from
 * PNFS_MDS_VERSION_OVERRIDE) takes precedence over this default; the #ifndef
 * guard avoids a macro redefinition that -Werror would reject. */
#ifndef PNFS_MDS_VERSION
#define PNFS_MDS_VERSION VERSION(PNFS_MDS_VERSION_MAJOR, \
                                 PNFS_MDS_VERSION_MINOR, \
                                 PNFS_MDS_VERSION_PATCH)
#endif

/* Short git commit the build was cut from.  A build-time
 * -DPNFS_MDS_GIT_COMMIT="..." (set by CMake from `git rev-parse`, or by the
 * deb/rpm packaging via PNFS_MDS_GIT_COMMIT_OVERRIDE) wins over this default. */
#ifndef PNFS_MDS_GIT_COMMIT
#define PNFS_MDS_GIT_COMMIT "unknown"
#endif

/**
 * Wire compatibility version -- bumped only on breaking wire/replication/RPC
 * changes.  Used as the promotion/demotion gate during rolling upgrades.
 */
#define PNFS_MDS_WIRE_COMPAT_VERSION  1

/* -----------------------------------------------------------------------
 * Limits
 * ----------------------------------------------------------------------- */
#define MDS_MAX_NODES       128  /* Supports up to 128 MDS nodes */
#define MDS_MAX_DS_NODES    256
#define MDS_MAX_MIRRORS     4
/* Wide-stripe ceiling for HPC N-to-1 workloads (Phase A of
 * docs/hpc-nto1-plan.md).  The catalogue and stripe-map paths must
 * support up to MDS_MAX_STRIPES stripes per file; practical defaults
 * stay much lower (cfg.hpc_max_stripe_count, default 128).  LAYOUTGET
 * and durable layout-state DS arrays are heap-backed and bounded by
 * MDS_MAX_STRIPES * MDS_MAX_MIRRORS. */
#define MDS_MAX_STRIPES     1024
#define MDS_MAX_PATH        4096
#define MDS_MAX_NAME        255
#define MDS_NFS_FH_MAX      128

/* Extended attribute limits */
#define MDS_XATTR_NAME_MAX       255
#define MDS_XATTR_VAL_MAX        65536   /* 64 KiB */
#define MDS_XATTR_PER_FILE_MAX   1024

/* Inline data limit (small file acceleration) */
#define MDS_INLINE_DATA_MAX      65536   /* 64 KiB max inline file size */

/* DS info field limits */
#define MDS_DS_ADDR_MAX     256
#define MDS_DS_HOST_MAX     256
#define MDS_DS_EXPORT_MAX   256

/* Schema version (bump on incompatible DB changes).
 * v2 -> v3 (Phase A of docs/hpc-nto1-plan.md): MDS_MAX_STRIPES bumped
 * from 16 to 1024 and MDS_IFLAG_HPC_SHARED added.  No on-disk row
 * format change -- stripe maps already serialise stripe_count as a
 * uint32 -- but bump the version anyway so backups produced by older
 * MDSes can be flagged. */
#define MDS_SCHEMA_VERSION  3

/* Pre-allocate fileids in batches */
#define MDS_OPEN_OWNER_MAX  1024
#define NFS4_VERIFIER_SIZE  8
#define NFS4_OTHER_SIZE     12

/* DS states */
#define DS_ONLINE      0
#define DS_OFFLINE     1
#define DS_DRAINING    2
#define DS_REBALANCING 3

/* DS operation mode */
#define DS_MODE_GENERIC     1

/* DS transport bitmask */
#define DS_TRANSPORT_TCP    0x01
#define DS_TRANSPORT_RDMA   0x02

/* DS capabilities */
#define DS_CAP_GPUDIRECT    0x01

/* Attribute mask bits for setattr */
#define MDS_ATTR_MODE       (1U << 0)
#define MDS_ATTR_UID        (1U << 1)
#define MDS_ATTR_GID        (1U << 2)
#define MDS_ATTR_SIZE       (1U << 3)
#define MDS_ATTR_ATIME      (1U << 4)
#define MDS_ATTR_MTIME      (1U << 5)
#define MDS_ATTR_ATIME_NOW  (1U << 6)
#define MDS_ATTR_MTIME_NOW  (1U << 7)
#define MDS_ATTR_FLAGS      (1U << 8)

/* Root inode is 2 to match POSIX convention
 * bad-block inode on ext2/3/4; NFS clients expect root != 0/1). */
#define MDS_FILEID_ROOT     2

/* Pre-allocate fileids in batches to reduce catalogue write contention.
 * Each MDS node claims 1024 ids at a time from the global counter. */
#define MDS_FILEID_BATCH    1024

/* -----------------------------------------------------------------------
 * Return codes
 * ----------------------------------------------------------------------- */

enum mds_status {
    MDS_OK             =   0,
    MDS_ERR_NOMEM      =  -1,
    MDS_ERR_IO         =  -2,
    MDS_ERR_NOTFOUND   =  -3,
    MDS_ERR_EXISTS     =  -4,
    MDS_ERR_INVAL      =  -5,
    MDS_ERR_PERM       =  -6,
    MDS_ERR_STALE      =  -7,
    MDS_ERR_GRACE      =  -8,
    MDS_ERR_REPL       =  -9,
    MDS_ERR_MOVED      = -10,
    MDS_ERR_DELAY      = -11,
    MDS_ERR_NOSTANDBY  = -12,
    MDS_ERR_XDEV       = -13,
    MDS_ERR_NOTEMPTY   = -14,
    MDS_ERR_ISDIR      = -15,
    MDS_ERR_NOTDIR     = -16,
    MDS_ERR_NOSPC      = -17,
    MDS_ERR_LAYOUTUNAVAIL = -18,
    MDS_ERR_NOSUPPORT  = -19,
};

/* -----------------------------------------------------------------------
 * Cluster mode
 * ----------------------------------------------------------------------- */

enum cluster_mode {
    CLUSTER_MODE_LOCAL = 0,  /**< In-memory only; single-node or test. */
    CLUSTER_MODE_ETCD  = 1,  /**< Authoritative etcd backend (legacy). */
    CLUSTER_MODE_RONDB = 2,  /**< RonDB partition_map + node_registry. */
};

/* -----------------------------------------------------------------------
 * Inode flags (persisted in catalogue inode record)
 * ----------------------------------------------------------------------- */

#define MDS_IFLAG_INLINE  (1U << 0)  /* File data stored inline in catalogue */
#define MDS_IFLAG_PROMOTING (1U << 1)  /* Promotion to DS in progress (serialisation) */
#define MDS_IFLAG_DS_PENDING (1U << 2) /* Stripe map assigned, DS file not yet created */
/* Phase A of docs/hpc-nto1-plan.md -- HPC-Shared mode (N-to-1 wide
 * stripe path).  Set by Phase B's three triggers (layouthint4, the
 * trusted.pnfs.hpc_shared xattr, or `mds-admin hpc set`).  Consumed
 * by Phase C (CREATE pre-warm), Phase D (shared layout cache), and
 * Phase F (aggregated LAYOUTCOMMIT).  Persisted in the inode flags
 * column so all MDSes observe the mode. */
#define MDS_IFLAG_HPC_SHARED (1U << 3)
/* Phase 3 of the QA plan -- HPC-Shared wide CREATE in flight.
 *
 * Set by hpc_shared_create_wide_layout() on the freshly-created
 * inode BEFORE the wide stripe map is persisted.  Cleared after
 * mds_cat_stripe_map_put() returns MDS_OK.  An MDS crash between
 * the inode commit and the stripe-map commit leaves the file with
 * this bit set; the inode + dirent are visible in the catalogue
 * but should not be reachable via NFSv4 because the wide stripe map
 * is missing.
 *
 * The compound read path (compound_inode_get, compound_lookup_local_child)
 * filters inodes carrying this flag so clients see NFS4ERR_NOENT
 * instead of an inconsistent file.
 *
 * Cleanup status (QA review Blocker 5).  The runtime filter is the
 * ONLY mechanism shipped today -- there is NO lazy-reap-on-access and
 * NO scan-based reaper.  A persistent orphan inode that survives an
 * MDS crash stays in the catalogue indefinitely; it is harmless
 * (invisible to every client and to readdir) but consumes a fileid
 * row.  Adding a startup / periodic scanner that walks inodes by flag
 * and removes any whose stripe_map row is absent is tracked as a
 * focused follow-up (see docs/hpc-shared-files.md "Deferred").  Do
 * NOT rely on the GC drainer's stripe_map_scan to reap these: the
 * GC drainer reclaims orphan stripe_map rows whose inode is gone,
 * NOT orphan inodes whose stripe_map is gone -- the inverse direction.
 *
 * Plain (non-HPC) CREATEs do NOT set this flag -- the legacy 1x1
 * fused CREATE primitive in catalogue_rondb_ns_create_with_layout
 * already gives crash atomicity. */
#define MDS_IFLAG_HPC_CREATE_PENDING (1U << 4)

/*
 * Inline single-stripe layout (schema v9).  Set when the file has
 * stripe_count == 1 && mirror_count == 1 and its one DS map entry is
 * stored directly in the inode (inline_ds_id / inline_fh + stripe_unit)
 * instead of the mds_stripe_maps / mds_stripe_entries side tables.  Lets
 * LAYOUTGET, the unlink fence, and GC enqueue serve the layout straight
 * from the inode read -- no separate cat_stripe_map_get round-trip, and
 * no stripe-table writes on create / deletes on remove.  Multi-stripe
 * (>1) and mirrored files keep the side tables and leave this clear.
 */
#define MDS_IFLAG_INLINE_STRIPE (1U << 5)

/* -----------------------------------------------------------------------
 * MDS Node Identity
 * ----------------------------------------------------------------------- */

struct mds_node_id {
    uint32_t id;
    char     hostname[256];
    uint16_t nfs_port;
    uint16_t grpc_port;
};

/* -----------------------------------------------------------------------
 * File types
 * ----------------------------------------------------------------------- */

enum mds_file_type {
    MDS_FTYPE_REG     = 1,
    MDS_FTYPE_DIR     = 2,
    MDS_FTYPE_SYMLINK = 3,
    MDS_FTYPE_BLKDEV  = 4,
    MDS_FTYPE_CHRDEV  = 5,
    MDS_FTYPE_FIFO    = 6,
    MDS_FTYPE_SOCK    = 7,
};

/* -----------------------------------------------------------------------
 * DS data file mapping
 * ----------------------------------------------------------------------- */

struct mds_ds_map_entry {
    uint32_t ds_id;
    uint32_t nfs_fh_len;
    uint8_t  nfs_fh[MDS_NFS_FH_MAX];
    /*
     * Stored synthetic DS-owner (RFC 8435 S2.2), carried prestage ->
     * ring -> create.  Per-file, not per-stripe: the same pair is used
     * for every stripe/mirror of the file and stored once on the inode.
     * NOT serialised into the stripe map (mds_ds_map rows); it lives in
     * the inode's synth_suid/synth_sgid columns.  0 = unset (legacy /
     * ds_synth_owner disabled).
     */
    uint32_t synth_suid;
    uint32_t synth_sgid;
};

/* -----------------------------------------------------------------------
 * DS registry record
 * ----------------------------------------------------------------------- */

struct mds_ds_info {
	uint32_t ds_id;
	uint32_t state;       /**< DS_ONLINE, DS_OFFLINE, etc. */
	uint32_t tier;        /**< 0=hot, 1=warm, 2=cold */
	uint64_t total_bytes;
	uint64_t used_bytes;
	uint16_t port;
	char     addr[MDS_DS_ADDR_MAX];
	uint8_t  mode;        /**< DS_MODE_GENERIC */
	uint8_t  transport;   /**< DS_TRANSPORT_TCP | DS_TRANSPORT_RDMA */
	char     host[MDS_DS_HOST_MAX];
	char     export_path[MDS_DS_EXPORT_MAX];
	uint16_t tcp_port;
	uint16_t rdma_port;
	uint32_t capabilities;
	/*
	 * Placement weight for WRR.  Runtime field populated from
	 * config (ds_weight.<id>) at daemon startup; not persisted in
	 * RonDB so it can be tuned per-MDS without a schema migration.
	 * A value of 0 means "unset" -- the WRR dispatcher then falls
	 * back to the old free-bytes heuristic (which on 3rd-party DSes
	 * collapses to uniform because total_bytes / used_bytes are
	 * never populated).  Operator-assigned weights are the reliable
	 * signal for heterogeneous NetApp / Isilon / Ceph clusters.
	 */
	uint32_t weight;
};

/* -----------------------------------------------------------------------
 * GC queue entry
 * ----------------------------------------------------------------------- */

struct mds_gc_entry {
	uint64_t gc_seq;
	uint64_t fileid;
	uint32_t ds_id;
	uint32_t nfs_fh_len;
	uint8_t  nfs_fh[MDS_NFS_FH_MAX];
	uint32_t owner_mds_id;   /* MDS that enqueued this entry (0 = legacy). */
};

/* One persisted DS-prealloc slot (ENABLE_DS_PREALLOC).  See
 * mds_cat_prealloc_pool_* in mds_catalogue.h. */
struct mds_prealloc_pool_row {
	uint64_t fileid;
	uint32_t ds_id;
	uint32_t owner_mds_id;
	uint32_t stripe_unit;
	uint32_t nfs_fh_len;
	uint8_t  nfs_fh[MDS_NFS_FH_MAX];
	/* v8: synth owner the prestaged DS file was chowned to (0 if legacy). */
	uint32_t synth_suid;
	uint32_t synth_sgid;
};

/* -----------------------------------------------------------------------
 * Client recovery record
 * ----------------------------------------------------------------------- */

struct client_recovery_rec {
	uint64_t clientid;
	uint32_t co_ownerid_len;
	uint8_t  co_ownerid[1024];
	uint8_t  verifier[8];
};

/* -----------------------------------------------------------------------
 * Inode
 * ----------------------------------------------------------------------- */

struct mds_inode {
    uint64_t            fileid;
    enum mds_file_type  type;
    uint32_t            mode;
    uint32_t            nlink;
    uint64_t            uid;
    uint64_t            gid;
    uint64_t            size;
    uint64_t            space_used;
    struct timespec     atime;
    struct timespec     mtime;
    struct timespec     ctime;
    uint64_t            change;
    uint64_t            generation;
    uint32_t            flags;          /* MDS_IFLAG_* bitfield */
    uint64_t            create_verf;    /* EXCLUSIVE4 verifier (cleared on first SETATTR) */
    uint64_t            parent_fileid;  /* Parent directory (0 = unknown/unmigrated) */

    /*
     * Stored synthetic DS owner (RFC 8435 S2.2, ds_synth_owner mode).
     * Random unguessable (suid, sgid) the file's DS backing files were
     * chowned to at prestage/DS-create; LAYOUTGET advertises these in
     * ffl_user/ffl_group instead of the owner uid/gid, and does no chown.
     * 0 = unset (legacy inode / mode disabled -> owner-aligned chown).
     * Persisted in the synth_suid/synth_sgid inode columns; length-
     * tolerant so pre-feature rows read back as 0.
     */
    uint32_t            synth_suid;
    uint32_t            synth_sgid;

    /* Stripe/mirror layout (regular files only) */
    uint32_t            stripe_count;
    uint32_t            stripe_unit;
    uint32_t            mirror_count;
    struct mds_ds_map_entry *ds_map;   /* [stripe_count * mirror_count] */

    /*
     * Inline single-stripe DS map (schema v9, MDS_IFLAG_INLINE_STRIPE).
     * When the flag is set the file's one (ds_id, nfs_fh) lives here in
     * the inode instead of the stripe side tables, and is packed into the
     * inode wire image so a single inode read serves LAYOUTGET / fence /
     * GC with no cat_stripe_map_get.  All zero when the flag is clear.
     */
    uint32_t            inline_ds_id;
    uint32_t            inline_fh_len;
    uint8_t             inline_fh[MDS_NFS_FH_MAX];

    /* TODO: add refcnt + per-inode lock when inode cache
     * is integrated with write-through coherence (Phase 2). */
};

/* -----------------------------------------------------------------------
 * Replication mode
 * ----------------------------------------------------------------------- */

enum mds_repl_mode {
    MDS_REPL_SYNC      = 0,
    MDS_REPL_ASYNC     = 1,
    MDS_REPL_SEMI_SYNC = 2,
};

/* -----------------------------------------------------------------------
 * Workload profiles (tuning presets)
 * ----------------------------------------------------------------------- */

enum mds_workload_profile {
    MDS_PROFILE_DEFAULT      = 0,
    MDS_PROFILE_HPC          = 1,  /**< Traditional HPC: MPI jobs, large sequential files. */
    MDS_PROFILE_AI_TRAINING  = 2,  /**< Checkpoint-biased: burst creates + large writes. */
    MDS_PROFILE_GENOMICS     = 3,  /**< Bioinformatics: many small files, high metadata rate. */
    MDS_PROFILE_MEDIA        = 4,  /**< Video/render: few very large files, low metadata rate. */
};

/* Bitmask: which tuning fields have deliberate values (from profile or
 * explicit config key).  Used by post-parse auto-sizing and main.c to
 * distinguish "not configured" from "intentionally set to X". */
#define MDS_CFG_SET_WORKER_THREADS         (1ULL << 0)
#define MDS_CFG_SET_PREALLOC_POOL_SIZE     (1ULL << 1)
#define MDS_CFG_SET_COMMIT_BATCH_SIZE      (1ULL << 2)
#define MDS_CFG_SET_COMMIT_FLUSH_MS        (1ULL << 3)
#define MDS_CFG_SET_COMMIT_BATCH_MAX_BYTES (1ULL << 4)
#define MDS_CFG_SET_COMMIT_QUEUE_DEPTH     (1ULL << 5)
#define MDS_CFG_SET_STRIPE_UNIT_BYTES      (1ULL << 6)
#define MDS_CFG_SET_INLINE_ENABLED         (1ULL << 7)
#define MDS_CFG_SET_INLINE_MAX_SIZE        (1ULL << 8)
#define MDS_CFG_SET_DS_PREPARE_QUEUE_DEPTH (1ULL << 9)
#define MDS_CFG_SET_LEASE_TIME_SEC         (1ULL << 10)
#define MDS_CFG_SET_PLACEMENT_POLICY       (1ULL << 11)
#define MDS_CFG_SET_DEFAULT_STRIPE_COUNT   (1ULL << 12)
#define MDS_CFG_SET_DEFAULT_MIRROR_COUNT   (1ULL << 13)
/* Phase C of docs/hpc-nto1-plan.md -- wide-stripe HPC knobs.  Set when
 * an INI key (or a profile) explicitly populates the matching field;
 * leaves the post-parse auto-sizer free to fall back to the
 * compile-time default when the bit is clear. */
#define MDS_CFG_SET_HPC_MAX_STRIPE_COUNT   (1ULL << 14)
#define MDS_CFG_SET_HPC_XDR_FORM           (1ULL << 15)
#define MDS_CFG_SET_STRIPE_LEASE_DURATION   (1ULL << 16)

/* -----------------------------------------------------------------------
 * Catalogue backend selection
 * ----------------------------------------------------------------------- */

enum mds_catalogue_backend {
    MDS_BACKEND_RONDB   = 0,  /**< Production: RonDB / NDB Cluster (distributed). */
};

/* -----------------------------------------------------------------------
 * DS placement policy (Phase 1 - metadata placement dispatcher)
 *
 * Selects which DS gets a new file's single stripe.  Multi-DS striping
 * is deferred to a later phase; all values currently operate on
 * stripe_count=1, mirror_count=1.
 *
 * PLACEMENT_RR           : round-robin across ONLINE DSes (today's behaviour).
 * PLACEMENT_WEIGHTED_RR  : reservoir pick weighted by (total - used) free bytes.
 * PLACEMENT_CAPACITY     : pick the ONLINE DS with the most free bytes.
 *
 * When `placement_policy_enabled` is false (the v1 default) the
 * dispatcher is bypassed and the legacy `placement_select()` path
 * runs unchanged - i.e. RR.  Flipping the flag on activates the
 * configured policy on LAYOUTGET for new files only; existing files
 * keep their stripe_map.
 * ----------------------------------------------------------------------- */
enum mds_placement_policy {
    PLACEMENT_RR           = 0,
    PLACEMENT_WEIGHTED_RR  = 1,
    PLACEMENT_CAPACITY     = 2,
};

/* -----------------------------------------------------------------------
 * Capacity-derived auto-weighting (Phase B2).
 *
 * When the capacity probe reports total/used for a DS, the probe can
 * also derive a WRR weight from the fullness ratio so an operator
 * does not have to assign a static ds_weight.<id> per DS.  The mode
 * is controlled by `placement_capacity_weighting` in mds.conf.
 *
 * OFF          : probe only records total/used; WRR falls back to
 *                operator weight > free-bytes > uniform (the old
 *                behaviour, unchanged when the key is absent).
 * PROPORTIONAL : probe derives auto_weight = max(1, floor((1 -
 *                used/total) * 100)) and writes it into the DS
 *                cache.  Overlay precedence in the placement path
 *                becomes operator weight > auto_weight > free-bytes
 *                > uniform, so a fuller DS drifts toward the floor
 *                of 1 while an empty DS tops out at 100.
 *
 * The derived value is bounded to [1, 100] so a full DS stays
 * selectable (no silent capacity lockout -- use ds set-state
 * offline for that) and one empty DS cannot dominate peers by more
 * than a factor of 100.  Operators who want sharper skew can still
 * layer ds_weight.<id> on top; the auto_weight path is only
 * consulted when the operator weight is zero.
 * ----------------------------------------------------------------------- */
enum mds_placement_capacity_weighting {
    CAP_WEIGHT_OFF          = 0,
    CAP_WEIGHT_PROPORTIONAL = 1,
};

/* -----------------------------------------------------------------------
 * Catalog image / replay mode (authority/image split)
 * ----------------------------------------------------------------------- */

/** Catalog image lifecycle mode. */
enum mds_catalog_image_mode {
    MDS_IMAGE_OFF     = 0,  /**< No image; authority serves all reads. */
    MDS_IMAGE_SHADOW  = 1,  /**< Image replays but replies from authority. */
    MDS_IMAGE_COMPARE = 2,  /**< Hot reads compare image vs authority. */
    MDS_IMAGE_PRIMARY = 3,  /**< Hot reads from image, fallback to authority. */
};

/** Catalog replay journal mode. */
enum mds_catalog_replay_mode {
    MDS_REPLAY_OFF     = 0,  /**< No replay journal. */
    MDS_REPLAY_LOG     = 1,  /**< File-only debug log (not durable). */
    MDS_REPLAY_JOURNAL = 2,  /**< Durable journal (co-committed with authority). */
};

/* -----------------------------------------------------------------------
 * HPC-Shared GETATTR consistency mode (Phase F of docs/hpc-nto1-plan.md).
 *
 * STRICT      -- default.  GETATTR forces a flush of any in-memory
 *               LAYOUTCOMMIT aggregation for the fileid before
 *               replying.  POSIX `stat()` semantics preserved.
 * OPTIMISTIC  -- GETATTR returns max(persisted_size, aggregated_size)
 *               from memory without forcing a flush.  Cheaper but
 *               deviates from POSIX; opt-in only.  Documented in
 *               docs/hpc-shared-files.md.
 * ----------------------------------------------------------------------- */
enum mds_hpc_getattr_mode {
    MDS_HPC_GETATTR_STRICT     = 0,
    MDS_HPC_GETATTR_OPTIMISTIC = 1,
};

/* -----------------------------------------------------------------------
 * HPC-Shared layout XDR wire form (Phase C of docs/hpc-nto1-plan.md).
 *
 * AUTO        -- default.  Emit the multi-DS-per-mirror form for
 *               HPC-Shared inodes whose layout has mirror_count == 1
 *               and stripe_count > 1.  All other inodes (plain files,
 *               mirrored layouts) keep the legacy one-DS-per-mirror
 *               form so existing clients see bit-for-bit identical
 *               wire output.
 * LEGACY      -- force one-DS-per-mirror unconditionally.  Useful for
 *               operators with pre-6.18 Linux clients in the fleet
 *               (see docs/hpc-nto1-plan.md S14).
 * STRIPED     -- force multi-DS-per-mirror unconditionally.  Used in
 *               lab and on fleets confirmed to be 6.18+ across the
 *               board.
 *
 * The Phase C v1 selector (compound_layout.c) interprets AUTO; the
 * encoder fork and wire-buffer heap-ification needed to honour
 * STRIPED on a 1024-stripe layout land in a follow-up commit and are
 * tracked under TODO Step 1 / Step 6 of the master Phase C plan.  The
 * config field exists now so operators can pin the form ahead of
 * those commits without an mds.conf migration later.
 * ----------------------------------------------------------------------- */
enum mds_hpc_xdr_form {
    MDS_HPC_XDR_FORM_AUTO    = 0,
    MDS_HPC_XDR_FORM_LEGACY  = 1,
    MDS_HPC_XDR_FORM_STRIPED = 2,
};

/* -----------------------------------------------------------------------
 * Configuration
 * ----------------------------------------------------------------------- */

/* NOLINTBEGIN(clang-analyzer-optin.performance.Padding) */
struct mds_config {
    struct mds_node_id  self;
    uint32_t            cluster_size;
    uint32_t            ds_count;

    /* Catalogue backend */
    enum mds_catalogue_backend catalogue_backend; /**< Default: RonDB. */
    char                catalogue_backend_conf[MDS_MAX_PATH]; /**< Backend-specific config file. */

    /* Replication */
    enum mds_repl_mode  repl_mode;
    char                standby_host[256];
    uint16_t            standby_port;
    uint16_t            repl_listen_port; /**< Standby receiver port (default 9401). */
    uint32_t            repl_semi_sync_n;

    /* Failover topology (Seq 9) */
    int                 self_role;              /**< 0 = NODE_ACTIVE (default), 1 = NODE_STANDBY.
                                                 *   Stored as int to avoid circular include with
                                                 *   cluster_membership.h. */
    uint32_t            self_failover_partner_id; /**< Paired partner MDS ID (0 = none). */

    /* Replication health monitoring */
    uint32_t            repl_health_interval_ms;
    bool                gpudirect_required; /**< Require 0 */

    /* NFS authentication (Item 49). */
    enum nfs_auth_mode  nfs_auth_mode;
    char                krb5_keytab_path[256];
    char                krb5_principal[256];

    /* TLS for inter-MDS transport (Item 49 Stage 3). */
    char                cluster_ca_file[256];
    char                node_cert_file[256];
    char                node_key_file[256];
    bool                require_mtls;
    bool                repl_refuse_writes_on_resync;

    /* Data servers: "host:/export" strings */
    char                ds_specs[MDS_MAX_DS_NODES][512];

    /*
     * Per-DS placement weights for WRR, keyed by ds_id
     * (0..MDS_MAX_DS_NODES-1).  Populated from
     * `ds_weight.<ds_id>=<value>` INI keys.  Zero = unset, which
     * lets the WRR dispatcher fall back to free-bytes heuristics
     * for clusters that do maintain live capacity.
     */
    uint32_t            ds_weight_by_id[MDS_MAX_DS_NODES];

    /*
     * Live DS capacity probe interval (milliseconds).  0 disables
     * the probe; the admin-weight path from ds_weight_by_id still
     * works.  Default 60000 (60s) leaves plenty of headroom for
     * statvfs() round-trips on busy mounts.
     */
    uint32_t            ds_capacity_poll_ms;

    /*
     * DS-health probe: consecutive failure count before a DS is
     * marked OFFLINE.  0 = compile-time default (6).  Promoted
     * from hardcoded DS_HEALTH_DEFAULT_THRESHOLD so operators can
     * harden or loosen failure detection per deployment.
     */
    uint32_t            ds_health_fail_threshold;

    /*
     * Default timeout (ms) applied to CB_RECALL / CB_LAYOUTRECALL
     * / CB_NOTIFY when the caller passes 0.  Promoted from the
     * hardcoded CB_DEFAULT_TIMEOUT in src/mds/nfs4_cb.c so slow
     * backchannel clients can be given more room without a
     * recompile.  Default 5000.
     */
    uint32_t            cb_recall_timeout_ms;

    /*
     * Default timeout (ms) applied to dir_deleg_recall_dir() +
     * dir_deleg_notify_dir() when the caller passes 0.  Promoted
     * from DDT_RECALL_DEFAULT_MS.  Default 5000.
     */
    uint32_t            dir_deleg_recall_timeout_ms;

    /*
     * Prometheus metrics HTTP listener port.  0 disables the
     * endpoint entirely; non-zero binds on 0.0.0.0:<port>.  Default
     * 9090 (industry convention).  Promoted from hardcoded 9090
     * in main.c.
     */
    uint16_t            metrics_http_port;

    /*
     * Master kill-switch for the per-op latency, per-catalogue-op
     * latency, and per-op*phase observability built on top of the
     * mds_op_metrics module.  When false, all `mds_phase_*`,
     * `mds_op_observe_*`, and `mds_cat_op_observe` callers take an
     * early-return path on a single relaxed atomic load (~1-2 ns).
     *
     * The dispatcher metrics in threadpool.c (worker saturation,
     * queue depth, queue-wait histogram) are NOT gated by this
     * flag; they remain cheap enough to leave always-on.
     *
     * Default: true.  Set `metrics_op_enabled = false` in mds.conf
     * to disable at startup without recompiling.  Toggle at runtime
     * via mds_op_metrics_set_enabled().
     */
    bool                metrics_op_enabled;

    /*
     * Compound PERF log threshold in microseconds.  When > 0, roughly
     * 1-in-64 compounds whose total wall time exceeds this value
     * are logged at INFO ("PERF: compound ...").  Default 0 disables
     * the sampler entirely (no timing overhead on the hot path).
     */
    uint32_t            compound_perf_threshold_us;

    /*
     * `showmount -e` compatibility responder (mountd_compat).
     *
     * The MDS is an NFSv4.1 / pNFS server and does NOT speak NFSv3
     * MOUNT.  Some operators reach for `showmount -e <host>` as a
     * sanity check, however; when run against an MDS today, that
     * command fails with "RPC: Program not registered".  When this
     * shim is enabled, the MDS answers ONC-RPC program 100005 v3
     * with a synthetic export list (procedure NULL, EXPORT, DUMP
     * only -- every other procedure, including MNT, returns
     * PROC_UNAVAIL so the MDS cannot be NFSv3-mounted).
     *
     * No DS interaction.  ENABLED by default -- see CHANGELOG and
     * docs/mountd-compat.md for the upgrade-path notes.  Operators
     * who want the shim off (no extra port, no rpcbind entry) set
     * `mountd_compat_enabled = false` in mds.conf.
     *
     * Limits below match include/mountd_compat.h:
     *   MOUNTD_COMPAT_MAX_EXPORTS = 16
     *   MOUNTD_COMPAT_PATH_MAX    = 256
     */
    bool                mountd_compat_enabled;          /* default true  */
    uint16_t            mountd_compat_port;             /* default 20048 */
    bool                mountd_compat_register_rpcbind; /* default true  */
    char                mountd_compat_bind_addr[64];    /* default "0.0.0.0" */
    char                mountd_compat_exports[16][256]; /* synthetic export paths */
    uint32_t            mountd_compat_export_count;     /* 0 = use default "/" */

    /* Cluster transport security */
    char                cluster_bind_addr[64];
    char                cluster_allowed_peers[MDS_MAX_NODES][64];
    uint32_t            cluster_allowed_peer_count;
    uint32_t            cluster_max_conns;

    /*
     * Admin-only allowed hosts.  Separate from cluster_peer[] so
     * monitoring / web-UI hosts can connect to the admin transport
     * without being treated as MDS cluster members.  Checked after
     * the cluster peer ACL: a connection is accepted if the source
     * IP matches ANY entry in either list (or if TLS is enabled and
     * neither list is populated).
     *
     * INI key:  admin_allowed_hosts = 192.168.1.10, 10.0.0.0/24
     * Up to 32 entries; plain IPv4 addresses only (no CIDR yet).
     */
    char                admin_allowed_hosts[32][64];
    uint32_t            admin_allowed_host_count;

    /* Tuning */
    uint32_t            worker_threads;
    /* Bounded request pipelining: max COMPOUNDs processed concurrently
     * per TCP connection by the worker pool (0 = default 8).  Higher
     * values let clients with few connections but many session slots
     * use more of the worker pool. */
    uint32_t            max_inflight_per_conn;
    uint32_t            ds_heartbeat_ms;
    uint32_t            stripe_unit_bytes;
    bool                auto_widen_lease_on_4k;
    uint64_t            layout_grant_max_length_bytes;
    /* Phase 3: default stripe geometry for new files' layouts.
     * Both default to 1 (no striping, no mirroring) unless a
     * profile sets them or an explicit INI key overrides. */
    uint32_t            default_stripe_count;
    uint32_t            default_mirror_count;
    uint32_t            lease_time_sec;
    uint32_t            grace_period_sec;
    uint32_t            prealloc_pool_size;
    uint32_t            prealloc_ring_count;  /* prealloc refill rings/workers
                                               * (0 = engine default). */

    /* Inline data (small file acceleration) */
    bool                inline_enabled;       /* Master switch (default true) */
    uint32_t            inline_max_size;      /* Max bytes for inline storage (default 65536) */

    /* Commit pipeline (single-writer batch commit) */
    uint32_t            commit_batch_size;    /* Max ops per batch (default 128) */
    uint32_t            commit_batch_max_bytes;/* Max payload bytes per batch (default 1 MiB) */
    uint32_t            commit_flush_ms;      /* Max ms before forced flush (default 2) */
    uint32_t            commit_queue_depth;   /* Backpressure limit (default 4096) */

    /* Automatic split evaluator (Tier 3 Phase 1) */
    bool                auto_split_enabled;   /* Proposal collection. Default false. */
    bool                auto_split_execute;   /* Auto-execute approved splits. Default false. */
    uint64_t            auto_split_threshold; /* ops/interval to propose. Default 10000. */
    uint32_t            auto_split_interval;  /* Eval cadence in seconds. Default 300. */
    uint32_t            auto_split_cooldown;  /* Min sec between re-splits. Default 600. */
    uint32_t            auto_split_sustained; /* Consecutive hot intervals. Default 2. */
    uint32_t            auto_split_min_children; /* Min children to be eligible. Default 4. */

    /* DS mount path format (proxy I/O) */
    char                ds_mount_path_fmt[128]; /* printf fmt, e.g. "/mnt/ds%u" */

    /* GETDEVICEINFO transport advertisement for the flex-files device addr.
     * ds_getdev_transport: 0=tcp (default), 1=rdma, 2=both. ds_rdma_port is
     * the RDMA port advertised to clients (default 20049). Values match
     * enum ff_transport_policy. */
    uint8_t             ds_getdev_transport;
    uint16_t            ds_rdma_port;

    /* Sharding (Tier 3 Phase 3) */
    bool                shard_enabled;         /* Master switch. Default false. */

    /*
     * Cosmetic READDIR filter (default false).  When true, the
     * daemon omits referral junction directories (the /shardN
     * partition entries that surface as fs_locations referrals) from
     * READDIR replies at the namespace ROOT only.  LOOKUP still
     * resolves them, so `cd /mnt/pnfs/shardN` keeps working -- this
     * only hides them from a plain `ls /mnt/pnfs`.  Detection is an
     * exact subtree-map match, so ordinary files and directories are
     * never affected.
     */
    bool                hide_referral_junctions;

    /*
     * POSIX DAC enforcement (default true).  When set, AUTH_SYS
     * requests are subject to classic POSIX permission semantics on
     * mutations: owner-only chmod/chown/utimes, directory
     * write+search bits for CREATE/REMOVE/RENAME/LINK/OPEN(CREATE),
     * the S_ISVTX sticky-deletion rule, and SUID/SGID clearing on
     * chown/truncate/write.  Disabling restores the historical
     * permissive behaviour where any principal could mutate any
     * object (useful only for fully-trusted single-user clusters).
     * INI key: posix_dac = true|false.
     */
    bool                posix_dac;

    /* DS async prepare (Phase 6) */
    uint32_t            ds_prepare_queue_depth; /* Per-DS queue (0 = default 4096). */
    uint32_t            ds_prepare_workers;     /* Worker threads (0 = 1 per DS). */

    /*
     * DS GC drainer parallelism.  ds_gc_workers controls the number
     * of worker threads consuming GC entries; ds_gc_batch_size sets
     * the bounded queue depth refilled by the coordinator on each
     * tick.  Defaults (4 workers, 256 batch) match the drainer's
     * lab tuning; setting workers=1 reproduces the legacy serial
     * drainer with the addition of batched peek.  Both are clamped
     * by ds_gc_start_ex (workers in [1,32], batch in [1,4096]).
     */
    uint32_t            ds_gc_workers;
    uint32_t            ds_gc_batch_size;

    /* Inode cache */
    uint32_t            inode_cache_size;  /**< Max cached inodes (0 = disabled). */

    /* Dirent cache (positive + negative entries) */
    uint32_t            dirent_cache_size;     /**< Max cached dirents (0 = default 32768). */
    uint32_t            negative_cache_ttl_ms; /**< Negative entry TTL in ms (0 = default 5000). */
    /* Positive (name->fileid / inode) cache entry TTL in ms.  0 = unset:
     * main.c leaves positive entries unbounded on single-MDS and applies
     * a small bound (~1s) on multi-MDS (cluster_size>1) for cross-MDS
     * cache coherence.  An explicit non-zero value always wins. */
    uint32_t            positive_cache_ttl_ms;

    /* HPC-Shared layout cache (Phase D of docs/hpc-nto1-plan.md).
     * Max number of cached stripe maps; cache is sharded 16 ways so
     * each shard gets ceil(N / 16) entries.  Default 1024 entries
     * (~16 KiB metadata + caller-bounded heap for the entry
     * arrays -- see layout_cache.h memory-footprint note). */
    uint32_t            layout_cache_size;     /**< 0 = default 1024. */

    /* HPC-Shared LAYOUTCOMMIT aggregator (Phase F of
     * docs/hpc-nto1-plan.md).  Bucket capacity (sharded 16 ways) and
     * periodic flush interval.  Both fields are consumed by the
     * Phase F integration patch -- v1 keeps the aggregator unwired
     * so the synchronous LAYOUTCOMMIT path stays bit-for-bit
     * identical for every inode regardless of HPC_SHARED. */
    uint32_t            layout_commit_aggregator_size;     /**< 0 = default 4096 buckets. */
    uint32_t            layout_commit_aggregator_flush_ms; /**< 0 = default 200 ms. */
    enum mds_hpc_getattr_mode hpc_getattr_mode;            /**< Default STRICT. */

    /* Phase C of docs/hpc-nto1-plan.md -- wide-stripe pre-warm.
     *
     * hpc_max_stripe_count caps stripe_count for HPC-Shared CREATEs
     * regardless of how many ONLINE DSes the cluster has.  Default
     * 128 matches the master plan's Phase C target geometry; raise
     * up to MDS_MAX_STRIPES (1024) for >128-DS clusters that have
     * been validated against the wire-buffer heap-ification commit.
     *
     * hpc_xdr_form selects the flex-files layout form on the wire
     * for HPC-Shared inodes; see enum mds_hpc_xdr_form above.
     * Default AUTO. */
    uint32_t            hpc_max_stripe_count;              /**< 0 = default 128. */
    enum mds_hpc_xdr_form hpc_xdr_form;                    /**< Default AUTO. */

    /* Transient protocol state caching.
     * When true, open_state and layout_state NDB persistence is
     * skipped -- in-memory tables are authoritative.  Safe for
     * single-MDS deployments.  Default: false (RonDB write-through). */
    bool                transient_state_cache;

    /* Directory delegations (RFC 8881 S10.9, S18.39).
     * When false (the default), GET_DIR_DELEGATION responds with
     * NFS4ERR_DIRDELEG_UNAVAIL regardless of cluster state -- the
     * same behaviour as Phase 8a.  When true, the MDS maintains a
     * dir_deleg_table and grants delegations for LOOKUPed
     * directories; concurrent mutations recall via CB_RECALL. */
    bool                dir_delegations_enabled;

    /* File delegations (RFC 8881 S10.4).
     *
     * When true (the default), op_open() grants OPEN_DELEGATE_READ /
     * OPEN_DELEGATE_WRITE to clients that did not pass
     * OPEN4_SHARE_ACCESS_WANT_NO_DELEG, and uses CB_RECALL to break
     * conflicts between clients.
     *
     * When false, the MDS never wires a delegation table into the RPC
     * server (rpc_cfg.dt = NULL); op_open() short-circuits the deleg
     * grant path because cd->dt == NULL and reports
     * OPEN_DELEGATE_NONE_EXT with WND4_NOT_WANTED.  Useful for
     * deployments that want to avoid CB_RECALL traffic entirely
     * (e.g. PEAK:AIO Mark's two-client harness with
     * `clientaddr=0.0.0.0`, where Linux v4.1+ does not translate the
     * mount option into the OPEN's WANT_NO_DELEG bit).
     *
     * The CB_LAYOUTRECALL path is unaffected by this flag: layout
     * conflict-recall (Mark's byte-range bug) is gated separately by
     * the layout_recall coordinator. */
    bool                file_delegations_enabled;

    /* RonDB connection pool */
    /* NDB connections per MDS (0 = auto, max 64). */
    uint32_t            ndb_conn_pool_size;

    /*
     * Phase 4 feature flag: when true, ns_create routes through the
     * existing rondb_async_exec pipeline (executeAsynchPrepare +
     * sendPreparedTransactions driven by the per-connection flush
     * thread).  At high concurrency this batches multiple worker
     * threads' commits into fewer TCP segments, reducing network
     * overhead.  Default false: the sync execute() path has the
     * same correctness and lower per-op latency at low concurrency.
     * Flip to true and re-run the concurrent mdtest / bench to
     * decide if the async path is worth adopting for a given
     * deployment profile.
     */
    bool                ndb_async_writes;

    /*
     * Placement policy for new files' stripe layout at LAYOUTGET.
     * See `enum mds_placement_policy` above for semantics.  Only
     * consulted when `placement_policy_enabled` is true.  Default
     * PLACEMENT_RR matches the pre-feature behaviour so an operator
     * can enable the dispatcher without choosing a new policy up
     * front.
     */
    enum mds_placement_policy placement_policy;
    bool                placement_policy_enabled;

    /*
     * Phase B2: derive per-DS WRR weights from the statvfs probe.
     * Default CAP_WEIGHT_OFF preserves the pre-feature behaviour
     * (operator weight > free-bytes > uniform).  CAP_WEIGHT_PROPORTIONAL
     * stamps auto_weight into the DS cache from (1 - used/total).
     * See enum mds_placement_capacity_weighting above.
     */
    enum mds_placement_capacity_weighting placement_capacity_weighting;

    /* Workload profile + explicit-set tracking */
    enum mds_workload_profile workload_profile;
    uint64_t            tuning_set;  /**< MDS_CFG_SET_* bitmask. */

    /* Authority / image split (Phase 1 scaffolding) */
    enum mds_catalog_image_mode  catalog_image_mode;  /**< Default MDS_IMAGE_OFF. */
    bool                catalog_compare_reads;  /**< Enable compare-read validation. */
    enum mds_catalog_replay_mode catalog_replay_mode; /**< Default MDS_REPLAY_OFF. */
    char                catalog_replay_snapshot_path[MDS_MAX_PATH];
    bool                catalog_replay_rebuild_on_start;
    char                catalog_delta_log_path[MDS_MAX_PATH];

    /*
     * Stripe lease duration (milliseconds).  When non-zero, LAYOUTGET
     * grants carry FF_FLAGS_STRIPE_LEASE and the MDS enforces per-
     * (fileid, range_offset) leases so concurrent clients on the same
     * stripe must wait or retry.  0 disables.  Default 30000 (30s).
     */
    uint32_t            stripe_lease_duration_ms;

    /*
     * RFC 8435 §2.2.1: DS synthetic-ID secret.
     *
     * When ds_synth_secret_file is non-empty, the daemon loads a
     * 32-byte binary key at startup and uses HMAC-SHA256 to derive
     * per-(fileid, stripe, mirror) synthetic uid values for
     * ffl_user in LAYOUTGET and for chown on the DS backing file.
     * This implements the loosely-coupled model's per-grant
     * credential isolation.
     *
     * When empty (the default), the daemon falls back to the
     * caller's real uid -- the pre-patch behaviour.
     */
    char                ds_synth_secret_file[256];
    uint8_t             ds_synth_secret[32];
    uint32_t            ds_synth_secret_len; /**< 0 = unconfigured, 32 = active */

    /*
     * RFC 8435 §2.2: stored synthetic-owner DS decoupling.
     *
     * When true, each regular file gets a random, unguessable synthetic
     * (suid, sgid) generated when its DS backing file is first created
     * (prestage or the DS_PENDING fallback).  The DS file is chowned to
     * that pair once, at creation time, off the RPC path; the pair is
     * stored on the inode (synth_suid/synth_sgid columns).  LAYOUTGET
     * then advertises the stored synthetic (suid, sgid) in
     * ffl_user/ffl_group and performs NO chown -- DS access is fully
     * decoupled from the file's owner uid/gid, which the MDS alone
     * enforces for metadata.  This removes the per-LAYOUTGET DS chown
     * (and the async-chown race where a client reaches the DS before the
     * chown lands).  Default false -> legacy owner-aligned chown path.
     */
    bool                ds_synth_owner;

    /*
     * Logging (src/common/log.c).  log_file is the diagnostics output
     * path; an empty string sends output to stderr.  log_level_global
     * is the default verbosity (an enum log_level value) applied to
     * every component at startup; log_level_by_component[i] overrides
     * component i when >= 0, or inherits the global when -1.
     * Defaults: stderr, LOG_INFO, all components inheriting.
     */
    char                log_file[MDS_MAX_PATH];
    int                 log_level_global;
    int                 log_level_by_component[LOG_COMP_COUNT];
};
/* NOLINTEND(clang-analyzer-optin.performance.Padding) */

/**
 * @brief Parse configuration from file.
 * @param path  Path to config file.
 * @param cfg   Output config structure.
 * @return MDS_OK on success.
 */
enum mds_status mds_config_load(const char *path, struct mds_config *cfg);


/* -----------------------------------------------------------------------
 * Logging
 * -----------------------------------------------------------------------
 * The logging interface (enum log_level, enum log_component, the
 * mds_log* prototypes, and the MDS_LOG_* convenience macros) lives in
 * mds_log.h, included near the top of this header.
 * ----------------------------------------------------------------------- */

/* -----------------------------------------------------------------------
 * Error helpers  (src/common/error.c)
 * ----------------------------------------------------------------------- */

/** Map mds_status to a human-readable string. */
const char *mds_status_str(enum mds_status s);

/** Map errno to mds_status. */
enum mds_status mds_errno_to_status(int err);

/* -----------------------------------------------------------------------
 * Heartbeat  (src/common/heartbeat.c)
 * ----------------------------------------------------------------------- */

struct heartbeat_ctx;

int  heartbeat_init(uint32_t self_id, uint64_t epoch,
                    uint32_t interval_ms, uint32_t timeout_ms,
                    struct heartbeat_ctx **out);
int  heartbeat_start(struct heartbeat_ctx *ctx);
int  heartbeat_stop(struct heartbeat_ctx *ctx);
void heartbeat_destroy(struct heartbeat_ctx *ctx);

/* -----------------------------------------------------------------------
 * Thread pool  (src/common/threadpool.c)
 * ----------------------------------------------------------------------- */

struct threadpool;
struct mds_histogram;

typedef void (*tp_work_fn)(void *arg);

int  threadpool_create(uint32_t count, struct threadpool **out);
int  threadpool_submit(struct threadpool *tp, tp_work_fn fn, void *arg);
void threadpool_destroy(struct threadpool *tp);

/**
 * Point-in-time snapshot of dispatcher health.  Lets operators
 * answer the "are we worker-starved?" question directly:
 *
 *   - worker_active == worker_total      => fully saturated
 *   - queue_depth     >  0  for long     => backlog forming
 *   - queue_wait_ns_sum / queue_wait_count >> p99 op latency
 *                                        => dispatcher is the bottleneck
 *   - queue_full_total      > 0          => clients getting RST/ECONNRESET
 *
 * `queue_wait_hist` is a live pointer into the threadpool; the
 * Prometheus renderer drains it with relaxed atomic loads.
 */
struct threadpool_stats {
	uint32_t  worker_total;
	uint32_t  worker_active;
	uint32_t  queue_depth;
	uint32_t  queue_capacity;
	uint64_t  submitted_total;
	uint64_t  completed_total;
	uint64_t  queue_full_total;
	uint64_t  queue_wait_ns_sum;
	uint64_t  queue_wait_count;
	struct mds_histogram *queue_wait_hist;
};

/**
 * Capture a snapshot of the threadpool's live counters.
 *
 * Safe to call from any thread; takes the pool mutex briefly to
 * read queue_depth and queue_capacity consistently.  Other fields
 * are loaded atomically without serialisation.
 *
 * (Named with the `_get_` infix so the symbol does not collide
 *  with the `struct threadpool_stats` type when the header is
 *  included from C++ translation units -- gcc -Werror=shadow
 *  flags a same-named function vs. struct as hiding the
 *  implicit constructor.)
 *
 * @param tp   Pool handle (may be NULL; out is zeroed).
 * @param out  Caller-provided snapshot buffer.
 */
void threadpool_get_stats(struct threadpool *tp,
			  struct threadpool_stats *out);


#endif /* PNFS_MDS_H */

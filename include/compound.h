/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * compound.h -- NFSv4.1/4.2 COMPOUND request dispatch types and API.
 *
 * This is the internal dispatch layer.  Operations use typed C structs
 * rather than XDR-encoded buffers; the XDR codec translates between
 * the wire format and these types.
 *
 * See docs/architecture.md S4.2 for design overview.
 */

#ifndef COMPOUND_H
#define COMPOUND_H

#include <stdint.h>
#include <stdbool.h>
#include "pnfs_mds.h"
#include "mds_catalogue.h"
#include "session.h"
#include "open_state.h"
#include "hpc_hint.h"

struct mds_catalogue;
struct session_table;
struct open_state_table;
struct lock_table;
struct mds_proxy_ctx;
struct health_monitor;
struct io_tracker;
struct dirent_cache;
struct deleg_table;
struct health_monitor;
struct rename_2pc_transport;
struct cluster_membership;
struct copy_offload_table;
struct mds_quota_ctx;
struct ds_prealloc_ctx;
struct commit_queue;
struct ds_health_monitor;
struct io_tracker;
struct mds_shard_map;
struct mds_shard;
struct dir_deleg_table;

/* -----------------------------------------------------------------------
 * NFSv4.1 status codes (RFC 8881 S15)
 * ----------------------------------------------------------------------- */

enum nfs4_status {
	NFS4_OK              = 0,
	NFS4ERR_PERM         = 1,
	NFS4ERR_NOENT        = 2,
	NFS4ERR_IO           = 5,
	NFS4ERR_ACCES        = 13,
	NFS4ERR_EXIST        = 17,
	NFS4ERR_XDEV         = 18,
	NFS4ERR_NOTDIR       = 20,
	NFS4ERR_ISDIR        = 21,
	NFS4ERR_INVAL        = 22,
	NFS4ERR_NOSPC        = 28,
	NFS4ERR_NOTEMPTY     = 66,
	NFS4ERR_STALE        = 70,
	/* RFC 8881 S15.1 numeric values are authoritative.  Earlier
	 * revisions of this enum aliased NFS4ERR_DENIED and
	 * NFS4ERR_STALE_CLIENTID to 10012, which is in fact
	 * NFS4ERR_LOCKED on the wire -- pynfs DESCID3/4/8 caught the
	 * mis-numbering by reporting "got NFS4ERR_LOCKED" when the
	 * server thought it was sending NFS4ERR_STALE_CLIENTID. */
	NFS4ERR_DENIED       = 10010,
	NFS4ERR_EXPIRED      = 10011,
	NFS4ERR_LOCKED       = 10012,
	NFS4ERR_GRACE        = 10013,
	/* RFC 8881 S15.1.16.5 / S18.35.4 -- EXCHANGE_ID UPDATE with a
	 * verifier that does not match the existing record's verifier.
	 * pynfs EID6f testUpdate101. */
	NFS4ERR_NOT_SAME     = 10027,
	NFS4ERR_FHEXPIRED    = 10014,
	NFS4ERR_WRONGSEC     = 10016,
	NFS4ERR_RESOURCE     = 10018,
	NFS4ERR_MOVED        = 10019,
	NFS4ERR_NOFILEHANDLE = 10020,
	NFS4ERR_MINOR_VERS_MISMATCH = 10021,
	NFS4ERR_STALE_CLIENTID    = 10022,
	NFS4ERR_BADHANDLE    = 10001,
	NFS4ERR_NOTSUPP      = 10004,
	NFS4ERR_SERVERFAULT  = 10006,
	NFS4ERR_DELAY        = 10008,
	NFS4ERR_NO_GRACE     = 10033,
	NFS4ERR_TOOSMALL     = 10005,  /* RFC 8881 S15 -- reply > maxcount. */
	NFS4ERR_OP_NOT_IN_SESSION = 10071,
	NFS4ERR_OLD_STATEID       = 10024,
	NFS4ERR_BAD_STATEID       = 10025,
	/* RFC 8881 S15.1 -- FREE_STATEID called against a stateid that
	 * still has locks (or, per Linux NFSD's interpretation, an open
	 * stateid that has not been CLOSEd).  Pynfs CSID9. */
	NFS4ERR_LOCKS_HELD        = 10037,
	NFS4ERR_SHARE_DENIED      = 10015,
	NFS4ERR_OPENMODE          = 10038,
	NFS4ERR_BADSESSION        = 10052,
	NFS4ERR_BADSLOT           = 10053,
	NFS4ERR_SEQ_MISORDERED    = 10063,
	/* RFC 5661 S15.1.10.2 -- the slot's reply was not cached so the
	 * server cannot replay it.  Returned when the client retries
	 * a request that was originally sent with sa_cachethis = FALSE
	 * (RFC 5661 S2.10.6.2 permits the server to either reconstruct
	 * the reply or return this code; we choose the simpler path).
	 * Drives pynfs SEQ10b. */
	NFS4ERR_RETRY_UNCACHED_REP = 10068,
	NFS4ERR_SEQ_FALSE_RETRY   = 10076,
	NFS4ERR_LAYOUTUNAVAILABLE = 10058,
	/* RFC 5661 S15.1.10.10 -- client CB_LAYOUTRECALL response indicating
	 * I/O in flight; server must NOT preemptively revoke. */
	NFS4ERR_RECALLCONFLICT    = 10061,
	/* RFC 5661 S15.1 numeric values are authoritative.  Earlier
	 * revisions of this enum used the wrong codes for NAMETOOLONG
	 * (was 10110) and REQ_TOO_BIG (was 10041 -- which is actually
	 * NFS4ERR_BADNAME on the wire).  pynfs SEQ6 caught the
	 * REQ_TOO_BIG mis-numbering by reporting "got NFS4ERR_BADNAME"
	 * when the server thought it was sending REQ_TOO_BIG. */
	NFS4ERR_NAMETOOLONG       = 63,
	/* RFC 5661 S15.1.1 -- wire-format errors used by the COMPOUND
	 * decode-failure path (rpc_server.c).  These MUST be carried in
	 * a synthesised SEQUENCE result so the resarray is non-empty. */
	NFS4ERR_BADXDR            = 10036,
	NFS4ERR_BADCHAR           = 10040,
	NFS4ERR_BADNAME           = 10041,
	NFS4ERR_REQ_TOO_BIG       = 10065,
	NFS4ERR_TOO_MANY_OPS      = 10070,
	/* RFC 8881 S15.1.16.4 -- DESTROY_CLIENTID called against a clientid
	 * that still has confirmed sessions or has not finished cleanup.
	 * pynfs DESCID5/6 (testDestroyCIDSessionB / testDestroyCIDCSession). */
	NFS4ERR_CLIENTID_BUSY     = 10074,
	/* RFC 8881 S15.1.10.10 -- a session/clientid management op was
	 * combined with another op in violation of the per-op compound
	 * placement rules (S2.10.6.4 / S18.36.3 / S18.37.3 / S18.50.3).
	 * pynfs CSESS23, DSESS9004/9005, DESCID7. */
	NFS4ERR_NOT_ONLY_OP       = 10081,
	/* RFC 8881 S15.1.1.4 / S18.46.3 -- SEQUENCE must be the first op
	 * in every COMPOUND that uses session-state.  pynfs SEQ2. */
	NFS4ERR_SEQUENCE_POS      = 10064,
	/* RFC 8881 S15.1.10.4 -- directory delegation not available. */
	NFS4ERR_DIRDELEG_UNAVAIL  = 10084,
	/* NFSv4.2 status codes (RFC 7862) */
	NFS4ERR_UNION_NOTSUPP     = 10090,
	NFS4ERR_OFFLOAD_NO_REQS   = 10094,
	NFS4ERR_NOXATTR           = 10095,
	NFS4ERR_XATTR2BIG         = 10096,
	NFS4ERR_OP_ILLEGAL        = 10044,
	NFS4ERR_COMPLETE_ALREADY   = 10054,
};

/* -----------------------------------------------------------------------
 * NFSv4.1/4.2 operation numbers (RFC 8881 / RFC 7862)
 * ----------------------------------------------------------------------- */

enum nfs_opnum4 {
	/* NFSv4.1 operations (RFC 8881 S18) */
	OP_ACCESS           = 3,
	OP_CLOSE            = 4,
	OP_COMMIT           = 5,
	OP_CREATE           = 6,
	OP_DELEGRETURN      = 8,
	OP_GETATTR          = 9,
	OP_GETFH            = 10,
	OP_LINK             = 11,
	OP_LOCK             = 12,
	OP_LOCKT            = 13,
	OP_LOCKU            = 14,
	OP_LOOKUP           = 15,
	OP_LOOKUPP          = 16,
	OP_OPEN             = 18,
	OP_OPENATTR         = 19,
	OP_OPEN_DOWNGRADE   = 21,
	OP_PUTFH            = 22,
	OP_PUTROOTFH        = 24,
	OP_READ             = 25,
	OP_READLINK         = 27,
	OP_READDIR          = 26,
	OP_REMOVE           = 28,
	OP_RENAME           = 29,
	OP_RESTOREFH        = 31,
	OP_SAVEFH           = 32,
	OP_SETATTR          = 34,
	OP_WRITE            = 38,
	OP_EXCHANGE_ID      = 42,
	OP_CREATE_SESSION   = 43,
	OP_DESTROY_SESSION  = 44,
	OP_GETDEVICEINFO    = 47,
	/* RFC 8881 S18.39 -- GET_DIR_DELEGATION (wire op 46). */
	OP_GET_DIR_DELEGATION = 46,
	OP_LAYOUTCOMMIT     = 49,
	OP_LAYOUTGET        = 50,
	OP_LAYOUTRETURN     = 51,
	OP_SEQUENCE         = 53,
	OP_BACKCHANNEL_CTL  = 40,  /* RFC 8881 S18.33 */
	OP_BIND_CONN_TO_SESSION = 41,
	OP_FREE_STATEID     = 45,
	OP_TEST_STATEID     = 55,
	OP_DESTROY_CLIENTID = 57,
	OP_RECLAIM_COMPLETE = 58,
	OP_SECINFO          = 33,
	OP_SECINFO_NO_NAME  = 52,

	/* NFSv4.2 operations (RFC 7862) */
	OP_ALLOCATE         = 59,
	OP_COPY             = 60,
	OP_COPY_NOTIFY      = 61,
	OP_DEALLOCATE       = 62,
	OP_IO_ADVISE        = 63,
	OP_LAYOUTERROR      = 64,
	OP_LAYOUTSTATS      = 65,
	OP_OFFLOAD_CANCEL   = 66,
	OP_OFFLOAD_STATUS   = 67,
	OP_READ_PLUS        = 68,
	OP_SEEK             = 69,
	OP_WRITE_SAME       = 70,
	OP_CLONE            = 71,

	/* RFC 8276 extended attribute operations */
	OP_GETXATTR         = 72,
	OP_SETXATTR         = 73,
	OP_LISTXATTRS       = 74,
	OP_REMOVEXATTR      = 75,

	/* RFC 8881 S18.41: illegal operation sentinel */
	OP_ILLEGAL          = 10044,

	/* Callback operations (RFC 8881 S20) */
	OP_CB_GETATTR       = 3,
	OP_CB_RECALL        = 4,
	OP_CB_LAYOUTRECALL  = 5,
	OP_CB_NOTIFY        = 6,
	OP_CB_SEQUENCE      = 11,
};

/* -----------------------------------------------------------------------
 * notify_type4 (RFC 8881 S3.3.15) -- directory notification event
 * numbers.  Bits in the GDD / CB_NOTIFY mask are (1U << notify_type).
 * ----------------------------------------------------------------------- */
#define NOTIFY4_CHANGE_CHILD_ATTRS 0
#define NOTIFY4_CHANGE_DIR_ATTRS   1
#define NOTIFY4_REMOVE_ENTRY       2
#define NOTIFY4_ADD_ENTRY          3
#define NOTIFY4_RENAME_ENTRY       4
#define NOTIFY4_CHANGE_COOKIE_VERIFIER 5

/* -----------------------------------------------------------------------
 * Per-operation argument structures
 * ----------------------------------------------------------------------- */

struct nfs4_arg_access {
	uint32_t access;  /**< ACCESS4 bit mask (RFC 8881 S6.3.1) */
};

/** Filehandle descriptor -- local or cluster-global identity. */
struct nfs4_fh_desc {
	uint64_t fileid;
	uint32_t owner_mds_id;  /**< 0 = local (this MDS) */
	uint32_t generation;
};

struct nfs4_arg_putfh {
	struct nfs4_fh_desc fh;
};

struct nfs4_arg_lookup {
	char name[MDS_MAX_NAME + 1];
};
struct nfs4_layout_hint {
	bool present;
	uint32_t layout_type;
	struct pnfs_hpc_hint hpc;
};

struct nfs4_arg_create {
	char               name[MDS_MAX_NAME + 1];
	enum mds_file_type type;
	uint32_t           mode;
	uint64_t           uid;
	uint64_t           gid;
	char               link_target[1024]; /**< Symlink target (NF4LNK only). */
	uint32_t           link_target_len;   /**< Length of link_target. */
};

struct nfs4_arg_remove {
	char name[MDS_MAX_NAME + 1];
};

struct nfs4_arg_rename {
	char src_name[MDS_MAX_NAME + 1];
	char dst_name[MDS_MAX_NAME + 1];
};

struct nfs4_arg_link {
	char name[MDS_MAX_NAME + 1];
};

struct nfs4_arg_open_downgrade {
	struct nfs4_stateid stateid;
	uint32_t            share_access;
	uint32_t            share_deny;
};

struct nfs4_arg_setattr {
	struct nfs4_stateid stateid; /**< Optional open stateid. */
	bool             has_stateid; /**< True if non-special. */
	uint32_t         mask;
	struct mds_inode attrs;
	struct nfs4_layout_hint layout_hint;
};

struct nfs4_arg_readdir {
	uint64_t cookie;                     /* 0 = first page; opaque resume cookie */
	uint32_t dircount;                  /* Client's preferred dirent bytes */
	uint32_t maxcount;                  /* Client's hard reply-size limit */
	uint32_t requested[3];             /* Requested entry attrs */
};

struct nfs4_arg_open {
	enum nfs4_claim_type  claim;
	char                  name[MDS_MAX_NAME + 1]; /* CLAIM_NULL only */
	uint32_t              share_access;
	uint32_t              share_deny;
	uint8_t               open_owner[NFS4_OPEN_OWNER_MAX];
	uint32_t              open_owner_len;
	bool                  create;               /* Create if absent? */
	enum nfs4_createmode  createmode;
	uint64_t              create_verf;    /* EXCLUSIVE4 verifier */
	/* Create attrs (used when create=true, CLAIM_NULL): */
	uint32_t              mode;
	uint64_t              uid;
	uint64_t              gid;
	struct nfs4_layout_hint layout_hint;
};

struct nfs4_arg_close {
	struct nfs4_stateid stateid;
};

struct nfs4_arg_exchange_id {
	uint8_t  co_ownerid[CO_OWNERID_MAX];
	uint32_t co_ownerid_len;
	uint8_t  verifier[NFS4_VERIFIER_SIZE];
	uint32_t eia_flags;
};

struct nfs4_arg_create_session {
	uint64_t clientid;
	uint32_t seqid;
	uint32_t csa_flags;
	uint32_t fore_slots;
	uint32_t back_slots;
	uint32_t cb_prog;        /**< Callback program number. */
	uint32_t cb_sec_flavor;  /**< Callback security flavor (legacy alias). */
	/*
	 * RFC 8881 S2.10.8.3 / S18.36 -- callback_sec_parms4 entry
	 * captured by the decoder for the FIRST entry of csa_sec_parms<>.
	 * Stored on the new session by op_create_session via
	 * session_set_cb_sec() so the CB encoder can emit the right RPC
	 * credential body (AUTH_NONE void or AUTH_SYS authsys_parms).
	 * pynfs DELEG5/6/7 (testCBSecParms*) verify uid/gid/flavor on
	 * received CB_RECALLs by inspecting this on the wire.
	 */
	struct nfs4_cb_sec cb_sec;
	/* RFC 8881 S18.36.4 client-requested forechannel attrs.  The
	 * server emits MIN(client_request, server_pref) in the reply
	 * and stores the negotiated values on the session for enforcement
	 * at SEQUENCE-receive time.  Pynfs SEQ6 (testRequestTooBig) +
	 * SEQ7 (testTooManyOps). */
	uint32_t fore_max_request_size;
	uint32_t fore_max_operations;
	/* RFC 8881 S18.36.4 channel_attrs4 fields kept for
	 * NFS4ERR_TOOSMALL validation in op_create_session.  Decoded
	 * verbatim from the wire; never used by session bookkeeping.
	 * Pynfs CSESS25 (fore maxresponsesize=0), CSESS28 (fore
	 * maxrequestsize=20), CSESS29 (back maxrequestsize=10) all
	 * expect NFS4ERR_TOOSMALL -- see op_create_session for the
	 * floor we apply (>= 256 bytes for max{request,response}size
	 * and >= 1 for max{operations,requests}). */
	uint32_t fore_max_response_size;
	uint32_t back_max_request_size;
	uint32_t back_max_response_size;
	uint32_t back_max_operations;
	uint32_t back_max_requests;
};

struct nfs4_arg_destroy_session {
	uint8_t session_id[SESSION_ID_SIZE];
};

/**
 * RFC 8881 S18.33.1 -- BACKCHANNEL_CTL arguments.
 *
 *   struct BACKCHANNEL_CTL4args {
 *       uint32_t            bca_cb_program;
 *       callback_sec_parms4 bca_sec_parms<>;
 *   };
 *
 * Used by clients to update either the callback program number or the
 * captured callback security parameters (or both) on an existing
 * session, without having to tear it down and re-create it.  Pynfs
 * DELEG7 (testCBSecParmsChange) drives this path by calling
 * BACKCHANNEL_CTL with a fresh authsys_parms{uid2, gid2} after a
 * CREATE_SESSION that supplied {uid1, gid1}; the test then verifies
 * that the next CB_RECALL carries uid2/gid2.
 *
 * cb_prog_set is true iff the wire request actually carried a non-zero
 * cb_program (we always decode it but want to distinguish "don't
 * change" from "set to zero" for future-proofing; today we always
 * apply it).  cb_sec is populated from the FIRST entry of
 * bca_sec_parms<>; subsequent entries are decoded only to advance the
 * wire cursor.
 */
struct nfs4_arg_backchannel_ctl {
	uint32_t            cb_prog;
	bool                cb_prog_set;
	struct nfs4_cb_sec  cb_sec;
	bool                cb_sec_set;
};

struct nfs4_arg_sequence {
	uint8_t  session_id[SESSION_ID_SIZE];
	uint32_t seq_id;     /**< sa_sequenceid (RFC 8881) */
	uint32_t slot_id;    /**< sa_slotid */
	uint32_t highest_slot_id;
	bool     cache_this;
};

/** RFC 8881 S18.51 -- RECLAIM_COMPLETE arguments. */
struct nfs4_arg_reclaim_complete {
	bool rca_one_fs;  /**< true = per-fs reclaim; false = global */
};

/** RFC 8881 S18.45 -- SECINFO_NO_NAME arguments. */
#define SECINFO_STYLE4_CURRENT_FH 0
#define SECINFO_STYLE4_PARENT     1
struct nfs4_arg_secinfo_no_name {
	uint32_t style;  /**< SECINFO_STYLE4_CURRENT_FH or SECINFO_STYLE4_PARENT */
};

/** RFC 5661 S18.48 -- TEST_STATEID arguments (count preserved for result). */
struct nfs4_arg_test_stateid {
	uint32_t count;
	struct nfs4_stateid stateids[64];
};

/* -----------------------------------------------------------------------
 * NFSv4.2 constants (RFC 7862)
 * ----------------------------------------------------------------------- */

/** SEEK what values (RFC 7862 S15.11.1). */
#define NFS4_CONTENT_DATA  0
#define NFS4_CONTENT_HOLE  1

/** IO_ADVISE4 hint bits (RFC 7862 S15.5.3). */
#define IO_ADVISE4_NORMAL           0x00
#define IO_ADVISE4_SEQUENTIAL       0x01
#define IO_ADVISE4_SEQUENTIAL_BACK  0x02
#define IO_ADVISE4_RANDOM           0x04
#define IO_ADVISE4_WILLNEED         0x08
#define IO_ADVISE4_WILLNEED_OPP     0x10
#define IO_ADVISE4_DONTNEED         0x20
#define IO_ADVISE4_NOREUSE          0x40
#define IO_ADVISE4_READ             0x80
#define IO_ADVISE4_WRITE            0x100
#define IO_ADVISE4_INIT_PROXIMITY   0x200

/** NFS4 write stability levels (shared with COPY result). */
#define UNSTABLE4    0
#define DATA_SYNC4   1
#define FILE_SYNC4   2

/** Maximum bytes for a single proxy copy chunk. */
#define MDS_COPY_CHUNK_SIZE  (1024 * 1024)  /* 1 MiB */

/* -----------------------------------------------------------------------
 * NFSv4.2 per-operation argument structures (RFC 7862)
 * ----------------------------------------------------------------------- */

struct nfs4_arg_allocate {
	struct nfs4_stateid stateid;
	uint64_t            offset;
	uint64_t            length;
};

struct nfs4_arg_deallocate {
	struct nfs4_stateid stateid;
	uint64_t            offset;
	uint64_t            length;
};

struct nfs4_arg_copy {
	struct nfs4_stateid src_stateid;
	struct nfs4_stateid dst_stateid;
	uint64_t            src_offset;
	uint64_t            dst_offset;
	uint64_t            count;
	bool                consecutive;
	bool                synchronous;
};

struct nfs4_arg_copy_notify {
	struct nfs4_stateid stateid;
};

struct nfs4_arg_seek {
	struct nfs4_stateid stateid;
	uint64_t            offset;
	uint32_t            what;   /**< NFS4_CONTENT_DATA=0, NFS4_CONTENT_HOLE=1 */
};

struct nfs4_arg_io_advise {
	struct nfs4_stateid stateid;
	uint64_t            offset;
	uint64_t            count;
	uint32_t            hints;  /**< IO_ADVISE4 bitmask */
};

struct nfs4_arg_offload_cancel {
	struct nfs4_stateid stateid;
};

struct nfs4_arg_offload_status {
	struct nfs4_stateid stateid;
};

struct nfs4_arg_clone {
	struct nfs4_stateid src_stateid;
	struct nfs4_stateid dst_stateid;
	uint64_t            src_offset;
	uint64_t            dst_offset;
	uint64_t            count;
};

struct nfs4_arg_read_plus {
	struct nfs4_stateid stateid;
	uint64_t            offset;
	uint32_t            count;
};

struct nfs4_arg_write_same {
	struct nfs4_stateid stateid;
	uint64_t            offset;
	uint64_t            length;    /**< Total byte range to fill. */
	uint32_t            data_len;  /**< Pattern block size. */
	uint8_t             data[MDS_XATTR_VAL_MAX]; /**< Pattern data. */
};

/** Device ID size (RFC 8881 S3.3.14 -- 16 bytes). */
#ifndef NFS4_DEVICEID4_SIZE
#define NFS4_DEVICEID4_SIZE  16
#endif

struct nfs4_arg_layouterror {
	struct nfs4_stateid stateid;     /**< Layout stateid. */
	uint64_t            offset;
	uint64_t            length;
	uint8_t             deviceid[NFS4_DEVICEID4_SIZE];
	enum nfs4_status    status;
	enum nfs_opnum4     opnum;     /**< Which op encountered the error. */
};

struct nfs4_arg_layoutstats {
	uint64_t            offset;
	uint64_t            length;
	struct nfs4_stateid stateid;
	uint64_t            read_count;
	uint64_t            read_bytes;
	uint64_t            write_count;
	uint64_t            write_bytes;
	uint8_t             deviceid[NFS4_DEVICEID4_SIZE];
};

/* -----------------------------------------------------------------------
 * pNFS layout constants (RFC 8881 S3.3.13, S12)
 * ----------------------------------------------------------------------- */

/* -----------------------------------------------------------------------
 * RFC 8276 extended attribute constants
 * ----------------------------------------------------------------------- */

/** SETXATTR option values (RFC 8276 S4.2.3). */
#define SETXATTR4_EITHER  0  /**< Create or replace unconditionally. */
#define SETXATTR4_CREATE  1  /**< Create only; fail if xattr exists. */
#define SETXATTR4_REPLACE 2  /**< Replace only; fail if xattr absent. */

/** Maximum xattr names in a single LISTXATTRS response. */
#define NFS4_LISTXATTRS_MAX  256

/** Xattr filehandle flag -- high bit marks xattr-namespace handles. */
#define XATTR_FH_FLAG  (1ULL << 63)

/** Layout types (RFC 8881 S3.3.13). */
#define LAYOUT4_NFSV4_1_FILES  1
#define LAYOUT4_FLEX_FILES     4   /* RFC 8435 */

/** Layout iomode (RFC 8881 S3.3.20). */
#define LAYOUTIOMODE4_READ     1
#define LAYOUTIOMODE4_RW       2
#define LAYOUTIOMODE4_ANY      3

/** Layout return type (RFC 8881 S18.44). */
#define LAYOUTRETURN4_FILE   1
#define LAYOUTRETURN4_FSID   2
#define LAYOUTRETURN4_ALL    3

/** Device ID size (RFC 8881 S3.3.14 -- 16 bytes). */
#ifndef NFS4_DEVICEID4_SIZE
#define NFS4_DEVICEID4_SIZE  16
#endif

/** Max DS endpoint entries returned by GETDEVICEINFO. */
#define MDS_MAX_LAYOUT_DS    16

/* -----------------------------------------------------------------------
 * pNFS per-operation argument structures
 * ----------------------------------------------------------------------- */

struct nfs4_arg_openattr {
	bool create_dir; /**< Create xattr directory if absent (ignored). */
};

/* -----------------------------------------------------------------------
 * RFC 8276 per-operation argument structures
 * ----------------------------------------------------------------------- */

/** RFC 8276 S4.2.2 -- GETXATTR arguments. */
struct nfs4_arg_getxattr {
	char name[MDS_XATTR_NAME_MAX + 1];
};

/** RFC 8276 S4.2.3 -- SETXATTR arguments. */
struct nfs4_arg_setxattr {
	uint32_t option;  /**< SETXATTR4_EITHER/CREATE/REPLACE */
	char     name[MDS_XATTR_NAME_MAX + 1];
	uint32_t value_len;
	uint8_t  value[MDS_XATTR_VAL_MAX];
};

/** RFC 8276 S4.2.4 -- LISTXATTRS arguments. */
struct nfs4_arg_listxattrs {
	uint64_t cookie;
	uint32_t maxcount;
};

/** RFC 8276 S4.2.5 -- REMOVEXATTR arguments. */
struct nfs4_arg_removexattr {
	char name[MDS_XATTR_NAME_MAX + 1];
};

struct nfs4_arg_read {
	struct nfs4_stateid stateid;
	uint64_t            offset;
	uint32_t            count;
};

struct nfs4_arg_write {
	struct nfs4_stateid stateid;
	uint64_t            offset;
	uint32_t            data_len;
	uint8_t             data[MDS_XATTR_VAL_MAX];
};

struct nfs4_arg_layoutget {
	bool                signal_available;
	uint32_t            layout_type;
	uint32_t            iomode;
	uint64_t            offset;
	uint64_t            length;
	uint64_t            minlength;
	struct nfs4_stateid stateid;
	uint32_t            maxcount;
};

struct nfs4_arg_getdeviceinfo {
	uint8_t  deviceid[NFS4_DEVICEID4_SIZE];
	uint32_t layout_type;
	uint32_t maxcount;
};

struct nfs4_arg_layoutreturn {
	bool                reclaim;
	uint32_t            layout_type;
	uint32_t            iomode;
	uint32_t            return_type; /**< LAYOUTRETURN4_FILE/FSID/ALL */
	/* Fields below are valid only when return_type == FILE. */
	uint64_t            offset;
	uint64_t            length;
	struct nfs4_stateid stateid;
};

struct nfs4_arg_layoutcommit {
	uint64_t            offset;
	uint64_t            length;
	bool                reclaim;
	struct nfs4_stateid stateid;
	bool                new_offset;
	uint64_t            last_write_offset;
	bool                time_modify_set;
	struct timespec     time_modify;
	uint32_t            layout_type;
};

struct nfs4_arg_getattr {
	uint32_t requested[3]; /* NFS4_BITMAP_WORDS */
};

/**
 * RFC 8881 S18.39.1 -- GET_DIR_DELEGATION arguments.
 *
 * The current filehandle identifies the directory the client wants
 * a delegation for.  All bitmaps use the 3-word NFS4_BITMAP_WORDS
 * representation shared with fattr4.  `child_attr_delay` and
 * `dir_attr_delay` are `attr_notice4` (RFC 5661 S3.3.7, typedef of
 * nfstime4) -- how long the server may coalesce change notifications
 * before delivering them.
 */
struct nfs4_arg_get_dir_delegation {
	bool             signal_deleg_avail;
	uint32_t         notification_types[3];
	uint32_t         notification_types_words;
	struct timespec  child_attr_delay;
	struct timespec  dir_attr_delay;
	uint32_t         child_attributes[3];
	uint32_t         child_attributes_words;
	uint32_t         dir_attributes[3];
	uint32_t         dir_attributes_words;
};


/* -----------------------------------------------------------------------
 * LOCK / LOCKT / LOCKU argument and result structures (RFC 8881)
 * ----------------------------------------------------------------------- */

struct nfs4_arg_lock {
	uint32_t            lock_type;  /* READ_LT, WRITE_LT, etc. */
	bool                reclaim;    /* Lock reclaim during grace */
	uint64_t            offset;
	uint64_t            length;
	/* Locker: new lock-owner or existing lock stateid. */
	bool                new_lock_owner;
	struct nfs4_stateid open_stateid;  /* For new lock-owner */
	uint64_t            lock_seqid;
	uint8_t             lock_owner[128];
	uint32_t            lock_owner_len;
	struct nfs4_stateid lock_stateid;  /* For existing lock-owner */
};

struct nfs4_arg_lockt {
	uint32_t            lock_type;
	uint64_t            offset;
	uint64_t            length;
	uint64_t            clientid;
	uint8_t             owner[128];
	uint32_t            owner_len;
};

struct nfs4_arg_locku {
	uint32_t            lock_type;
	struct nfs4_stateid lock_stateid;
	uint64_t            offset;
	uint64_t            length;
};

struct nfs4_lock_denied {
	uint64_t            offset;
	uint64_t            length;
	uint32_t            lock_type;
	uint64_t            clientid;
	uint8_t             owner[128];
	uint32_t            owner_len;
};

struct nfs4_res_lock {
	struct nfs4_stateid     stateid;
	struct nfs4_lock_denied denied;  /* Filled when NFS4ERR_DENIED */
};

struct nfs4_res_locku {
	struct nfs4_stateid     stateid;
};

/* Tagged union of operation arguments. */
struct nfs4_op {
	enum nfs_opnum4 opnum;
	union {
		/* NFSv4.1 */
		struct nfs4_arg_access          access;
		struct nfs4_arg_lock            lock;
		struct nfs4_arg_lockt           lockt;
		struct nfs4_arg_locku           locku;
		struct nfs4_arg_getattr        getattr;
		struct nfs4_arg_putfh           putfh;
		struct nfs4_arg_lookup          lookup;
		struct nfs4_arg_create          create;
		struct nfs4_arg_remove          remove;
		struct nfs4_arg_rename          rename;
		struct nfs4_arg_link            link;
		struct nfs4_arg_setattr         setattr;
		struct nfs4_arg_readdir         readdir;
		struct nfs4_arg_open            open;
		struct nfs4_arg_close           close;
		struct nfs4_arg_open_downgrade  open_downgrade;
		struct nfs4_arg_openattr        openattr;
		struct nfs4_arg_read            read;
		struct nfs4_arg_write           write;
		struct nfs4_arg_exchange_id     exchange_id;
		struct nfs4_arg_create_session  create_session;
		struct nfs4_arg_destroy_session destroy_session;
		struct nfs4_arg_backchannel_ctl backchannel_ctl;
		uint64_t                        destroy_clientid;
		struct nfs4_stateid             free_stateid;
		struct nfs4_arg_sequence        sequence;
		struct nfs4_arg_reclaim_complete reclaim_complete;
		struct nfs4_arg_test_stateid   test_stateid;
		struct nfs4_arg_get_dir_delegation get_dir_delegation;
		struct nfs4_arg_secinfo_no_name secinfo_no_name;
		/* pNFS layout */
		struct nfs4_arg_layoutget       layoutget;
		struct nfs4_arg_getdeviceinfo   getdeviceinfo;
		struct nfs4_arg_layoutreturn    layoutreturn;
		struct nfs4_arg_layoutcommit    layoutcommit;
		/* NFSv4.2 */
		struct nfs4_arg_allocate        allocate;
		struct nfs4_arg_copy            copy;
		struct nfs4_arg_copy_notify     copy_notify;
		struct nfs4_arg_deallocate      deallocate;
		struct nfs4_arg_io_advise       io_advise;
		struct nfs4_arg_layouterror     layouterror;
		struct nfs4_arg_layoutstats     layoutstats;
		struct nfs4_arg_offload_cancel  offload_cancel;
		struct nfs4_arg_offload_status  offload_status;
		struct nfs4_arg_read_plus       read_plus;
		struct nfs4_arg_seek            seek;
		struct nfs4_arg_write_same      write_same;
		struct nfs4_arg_clone           clone;
		/* RFC 8276 xattr */
		struct nfs4_arg_getxattr        getxattr;
		struct nfs4_arg_setxattr         setxattr;
		struct nfs4_arg_listxattrs       listxattrs;
		struct nfs4_arg_removexattr      removexattr;
	} arg;
};

/* -----------------------------------------------------------------------
 * Per-operation result structures
 * ----------------------------------------------------------------------- */

#define NFS4_READDIR_MAX 128

struct nfs4_res_access {
	uint32_t supported;  /**< Which bits the server recognises */
	uint32_t access;     /**< Which bits are permitted */
};

struct nfs4_res_getfh {
	struct nfs4_fh_desc fh;
};

struct nfs4_res_getattr {
	struct mds_inode inode;
	uint32_t requested[3]; /* client's requested bitmap */
	bool has_fs_space;
	uint64_t space_avail;
	uint64_t space_free;
	uint64_t space_total;
	/* Junction referral (inline storage, no heap). */
	bool has_referral;
	char referral_server[256];
	char referral_rootpath[MDS_MAX_PATH];
	char referral_fs_root[MDS_MAX_PATH];
};

struct nfs4_res_create {
	struct mds_inode inode;
	/* Parent-directory change attribute captured by op_create()
	 * before and after the mutation (RFC 8881 S18.4 change_info4).
	 * These two fields are NOT aliased by the surrounding union
	 * (they live inside the same union member as `inode`), so
	 * unlike struct nfs4_res_change_info they are safe to use from
	 * op_create / encode_res_create in the same compound slot. */
	uint64_t parent_change_before;
	uint64_t parent_change_after;
};

struct nfs4_res_readdir {
	struct mds_cat_dirent entries[NFS4_READDIR_MAX];
	struct mds_inode     entry_attrs[NFS4_READDIR_MAX];
	bool                 entry_attrs_valid[NFS4_READDIR_MAX];
	uint32_t             requested[3];
	uint32_t             count;
	bool                 eof;
	uint64_t             cookie_base; /**< Skip offset for absolute cookies */
	uint64_t             dir_change;  /**< Dir inode change attr for cookieverf */
};

struct nfs4_res_open {
	struct nfs4_stateid stateid;
	struct mds_inode    inode;    /* Opened/created file attributes */
	/*
	 * Delegation grant (RFC 8881 S18.16.4).
	 *
	 * delegation_type is one of:
	 *   OPEN_DELEGATE_NONE      -- v4.0-style "no delegation" (void body).
	 *   OPEN_DELEGATE_READ      -- READ delegation; deleg_stateid valid.
	 *   OPEN_DELEGATE_WRITE     -- WRITE delegation; deleg_stateid valid.
	 *   OPEN_DELEGATE_NONE_EXT  -- v4.1+ "declined" with reason; reads
	 *                             none_reason from the why_no_delegation4
	 *                             bag.  WND4_NOT_WANTED is the typical
	 *                             reason when the client set
	 *                             OPEN4_SHARE_ACCESS_WANT_NO_DELEG.
	 *
	 * none_reason is consulted only when delegation_type is
	 * OPEN_DELEGATE_NONE_EXT.  none_will_push / none_will_signal apply
	 * to WND4_CONTENTION / WND4_RESOURCE respectively per RFC 8881
	 * S18.16.4.  None of these tails are populated by op_open today
	 * (we never promise to push/signal); the fields are zero-init in
	 * the result and consumed by encode_res_open as the union tail.
	 */
	uint32_t            delegation_type;
	struct nfs4_stateid deleg_stateid;   /* Valid when delegation_type is READ or WRITE */
	uint32_t            none_reason;     /* WND4_*, valid when delegation_type == OPEN_DELEGATE_NONE_EXT */
	bool                none_will_push;  /* WND4_CONTENTION tail */
	bool                none_will_signal;/* WND4_RESOURCE tail */
};

struct nfs4_res_close {
	struct nfs4_stateid stateid;  /* Closing stateid (seqid+1) */
};

struct nfs4_res_exchange_id {
	uint64_t clientid;
	uint32_t seqid;
	uint32_t eir_flags;
};

struct nfs4_res_create_session {
	uint8_t  session_id[SESSION_ID_SIZE];
	uint32_t csr_sequence;
	uint32_t csr_flags;
	uint32_t fore_slots;
	uint32_t back_slots;
	/* Negotiated forechannel attrs returned to the client.  These are
	 * MIN(client_request, server_pref); the same values are stored on
	 * the new session and consulted by the SEQUENCE wire-receive path
	 * to enforce ca_maxrequestsize / ca_maxoperations. */
	uint32_t fore_max_request_size;
	uint32_t fore_max_operations;
};

struct nfs4_res_sequence {
	uint8_t  session_id[SESSION_ID_SIZE];
	uint32_t seq_id;     /**< sr_sequenceid (RFC 8881) */
	uint32_t slot_id;    /**< sr_slotid */
	uint32_t highest_slot_id;
	uint32_t target_highest_slot_id;
	uint32_t status_flags;
};


/** RFC 5661 S18.48 -- TEST_STATEID result (per-stateid statuses). */
struct nfs4_res_test_stateid {
	uint32_t count;
	uint32_t status_codes[64]; /**< Per-stateid NFS4 status. */
};

/**
 * RFC 8881 S18.39.2 -- GET_DIR_DELEGATION result.
 *
 * The XDR has an outer union keyed on `nfsstat4 gdrd_status` and an
 * inner union keyed on `gdd4_status gddrnf_status`:
 *
 *   union GET_DIR_DELEGATION4res switch (nfsstat4 gdrd_status) {
 *    case NFS4_OK:
 *       union GET_DIR_DELEGATION4res_non_fatal4
 *             switch (gdd4_status gddrnf_status) {
 *          case GDD4_OK:      GET_DIR_DELEGATION4resok         gddr_resok4;
 *          case GDD4_UNAVAIL: GET_DIR_DELEGATION4res_non_fatal gddr_res_non_fatal4;
 *       };
 *    default: void;
 *   };
 *
 * The outer status stays NFS4_OK both for "granted" and for "not
 * granted right now" -- that is what keeps the kernel's bundled
 * GETATTR from being stripped out of the compound (a non-OK outer
 * status halts the compound per RFC 8881 S2.6.3.1.1, producing EIO
 * when a following op would have supplied the attrs the client
 * needed).  The inner `gddrnf_status` selects between the granted
 * body and the "will_signal_deleg_avail" non-fatal body.
 */
enum gdd4_status {
	GDD4_OK      = 0,
	GDD4_UNAVAIL = 1,
};
struct nfs4_res_get_dir_delegation {
	/* Inner union discriminator. */
	uint32_t            gddrnf_status;
	/* GDD4_UNAVAIL body. */
	bool                will_signal_deleg_avail;
	/* GDD4_OK body. */
	uint8_t             cookieverf[NFS4_VERIFIER_SIZE];
	struct nfs4_stateid stateid;
	uint32_t            notification[3];
	uint32_t            notification_words;
	uint32_t            child_attributes[3];
	uint32_t            child_attributes_words;
	uint32_t            dir_attributes[3];
	uint32_t            dir_attributes_words;
};
/* -----------------------------------------------------------------------
 * NFSv4.2 per-operation result structures (RFC 7862)
 * ----------------------------------------------------------------------- */

struct nfs4_res_seek {
	bool     eof;
	uint64_t offset;
};

struct nfs4_res_copy {
	uint64_t            write_count;
	uint32_t            committed;    /**< UNSTABLE4/DATA_SYNC4/FILE_SYNC4 */
	uint8_t             verifier[NFS4_VERIFIER_SIZE];
	struct nfs4_stateid copy_stateid; /**< For async copy tracking */
	bool                consecutive;
	bool                synchronous;
};

struct nfs4_res_copy_notify {
	uint64_t            lease_time;   /**< Seconds. */
	struct nfs4_stateid stateid;
};

struct nfs4_res_offload_status {
	uint64_t count;
	bool     complete;
	enum nfs4_status complete_status;
};

struct nfs4_res_io_advise {
	uint32_t hints;   /**< Acknowledged IO_ADVISE4 bitmask. */
};

/** READ_PLUS content segment (RFC 7862 S15.10). */
struct nfs4_read_plus_content {
	uint32_t content_type;  /**< NFS4_CONTENT_DATA or NFS4_CONTENT_HOLE */
	uint64_t offset;
	union {
		struct {
			uint32_t data_len;
			uint8_t  data[MDS_XATTR_VAL_MAX];
		} data;
		struct {
			uint64_t length;
		} hole;
	} u;
};

/** Max content segments in a READ_PLUS result. */
#define NFS4_READ_PLUS_MAX_SEGS  8

struct nfs4_res_read_plus {
	bool     eof;
	uint32_t seg_count;
	struct nfs4_read_plus_content segs[NFS4_READ_PLUS_MAX_SEGS];
};

/* -----------------------------------------------------------------------
 * RFC 8276 per-operation result structures
 * ----------------------------------------------------------------------- */

/** RFC 8276 S4.2.2 -- GETXATTR result. */
struct nfs4_res_getxattr {
	uint32_t value_len;
	uint8_t  value[MDS_XATTR_VAL_MAX];
};

/** RFC 8276 S4.2.3 -- SETXATTR result. */
struct nfs4_res_setxattr {
	uint64_t change_before;
	uint64_t change_after;
};

/** RFC 8276 S4.2.4 -- LISTXATTRS result. */
struct nfs4_res_listxattrs {
	uint64_t cookie;
	uint32_t name_count;
	char     names[NFS4_LISTXATTRS_MAX][MDS_XATTR_NAME_MAX + 1];
	bool     eof;
};

/** RFC 8276 S4.2.5 -- REMOVEXATTR result. */
struct nfs4_res_removexattr {
	uint64_t change_before;
	uint64_t change_after;
};

/* -----------------------------------------------------------------------
 * pNFS per-operation result structures
 * ----------------------------------------------------------------------- */

/** Single DS descriptor inside a LAYOUTGET response. */
struct nfs4_layout_ds {
	uint32_t ds_id;
	uint32_t nfs_fh_len;
	uint8_t  nfs_fh[MDS_NFS_FH_MAX];
	uint8_t  deviceid[NFS4_DEVICEID4_SIZE];
};

/** SECINFO result (RFC 8881 S18.29). */
/** change_info4 for REMOVE, RENAME, LINK (RFC 8881). */
struct nfs4_res_change_info {
	uint64_t before;
	uint64_t after;
	uint64_t src_before; /**< RENAME only: source dir before. */
	uint64_t src_after;  /**< RENAME only: source dir after. */
};

struct nfs4_res_secinfo {
	uint32_t count;       /**< Number of flavors. */
	uint32_t flavors[4];  /**< Security flavor values. */
};

/** COMMIT result (RFC 8881 S18.3). */
struct nfs4_res_commit {
	uint64_t write_verf; /**< Server write verifier (boot epoch). */
};

/** READLINK result (RFC 8881 S18.24). */
struct nfs4_res_readlink {
	char     target[1024]; /**< Symlink target string. */
	uint32_t target_len;   /**< Byte length of target. */
};

struct nfs4_res_read {
	bool     eof;
	uint32_t data_len;
	uint8_t  data[MDS_XATTR_VAL_MAX];
};

struct nfs4_res_write {
	uint32_t count;
	uint32_t committed;  /**< FILE_SYNC4 = 2 */
	uint64_t write_verf; /**< Server write verifier (boot epoch). */
};


/* Flex-files layout result model (RFC 8435). */
struct nfs4_ff_ds {
	uint32_t ds_id;
	uint8_t  deviceid[NFS4_DEVICEID4_SIZE];
	uint32_t efficiency;
	struct nfs4_stateid stateid;
	uint32_t nfs_fh_len;
	uint8_t  nfs_fh[MDS_NFS_FH_MAX];
	uint32_t ffl_user;
	uint32_t ffl_group;
};

/*
 * Phase C / Step 1 of docs/hpc-nto1-plan.md -- wire-buffer
 * heap-ification.  The inner DS array of an ff_mirror was previously
 * a fixed-size [MDS_MAX_LAYOUT_DS] inline buffer; now a heap pointer
 * sized by the producer at alloc time.  Lifetime is owned by the
 * surrounding nfs4_res_layoutget and freed by
 * nfs4_res_layoutget_destroy().
 *
 * Producers should call nfs4_ff_mirror_alloc() before populating
 * ds[]; an unallocated mirror has ds == NULL and ds_count == 0,
 * which the encoder treats as an empty entry.
 */
struct nfs4_ff_mirror {
	uint32_t            ds_count;
	struct nfs4_ff_ds  *ds;     /* heap, sized ds_count */
};

/*
 * Phase C / Step 6 of docs/hpc-nto1-plan.md -- flex-files XDR wire
 * form selector.  Two RFC 8435-conformant shapes:
 *
 *   LEGACY  -- N ff_mirror4 entries, each with ds_count = mirror_count.
 *             Pre-6.18 Linux flex-files clients consume this form;
 *             they treat each ff_mirror4 as one stripe and pick one
 *             DS per stripe (effectively single-DS per layout
 *             segment when mirror_count == 1).
 *   STRIPED -- 1 ff_mirror4 entry whose ds_count == stripe_count,
 *             listing all DSes in the layout.  Linux 6.18+ clients
 *             consume this as the per-stripe DS dispatch table
 *             (dss_id = offset / stripe_unit).  Yields true
 *             per-client parallelism across all DSes.
 *
 * The default value (0 == LEGACY) preserves bit-for-bit compatibility
 * with pre-Phase-C clients.  Producers opt into STRIPED by setting
 * @c ff_xdr_form on the result struct AFTER nfs4_res_layoutget_alloc()
 * but BEFORE populating the buffers.  The encoder reads the field at
 * emit time.
 */
enum nfs4_ff_xdr_form {
	NFS4_FF_XDR_FORM_LEGACY  = 0,
	NFS4_FF_XDR_FORM_STRIPED = 1,
};

struct nfs4_res_layoutget {
	bool                return_on_close;
	struct nfs4_stateid stateid;
	uint32_t            layout_type;
	uint32_t            iomode;
	uint64_t            offset;
	uint64_t            length;
	uint32_t            stripe_unit;
	/*
	 * Outer DS array -- used by both files-layout and the legacy
	 * flex-files form.  Heap-allocated by
	 * nfs4_res_layoutget_alloc(); sized to ds_count.
	 */
	uint32_t            ds_count;
	struct nfs4_layout_ds *ds;     /* heap, sized ds_count */

	/*
	 * Flex-files mirror array (populated when layout_type ==
	 * LAYOUT4_FLEX_FILES).  Heap-allocated by
	 * nfs4_res_layoutget_alloc(); sized to ff_mirror_count.  Each
	 * ff_mirrors[i].ds is itself a heap pointer with its own
	 * ds_count, allocated separately by nfs4_ff_mirror_alloc().
	 */
	uint32_t              ff_mirror_count;
	struct nfs4_ff_mirror *ff_mirrors; /* heap, sized ff_mirror_count */
	uint32_t              ff_flags;

	/*
	 * Phase C / Step 6 -- wire-form selector.  Default LEGACY (= 0)
	 * preserves the pre-feature wire shape.  The populator sets
	 * STRIPED only after building the multi-DS-per-mirror form
	 * (single ff_mirror4 with ds_count == stripe_count).  See enum
	 * nfs4_ff_xdr_form above.
	 */
	enum nfs4_ff_xdr_form ff_xdr_form;
};

/* -----------------------------------------------------------------------
 * Wire-buffer lifecycle helpers (Phase C / Step 1).
 *
 * These keep the heap-allocated layout-get arrays from leaking and
 * make the producer / consumer responsibilities explicit.  All
 * helpers are NULL-safe and idempotent.
 * ----------------------------------------------------------------------- */

/**
 * Allocate the inner DS array of a single flex-files mirror.  Frees
 * any previous allocation first.  Sets ds_count to @p n on success;
 * leaves the mirror in a destroy-safe state on failure (ds = NULL,
 * ds_count = 0).
 *
 * @return 0 on success, -1 on allocation failure.
 */
int  nfs4_ff_mirror_alloc(struct nfs4_ff_mirror *m, uint32_t n);

/**
 * Free the inner DS array of a single flex-files mirror and zero
 * the count.  NULL-safe.
 */
void nfs4_ff_mirror_destroy(struct nfs4_ff_mirror *m);

/**
 * Allocate the outer ds[] and ff_mirrors[] arrays sized to
 * @p ds_count and @p ff_mirror_count respectively.  Inner
 * ff_mirror[i].ds buffers are NOT allocated here; the producer
 * calls nfs4_ff_mirror_alloc() per mirror once it knows the inner
 * geometry (legacy form: 1; striped form: stripe_count).
 *
 * Frees any previously-attached buffers first.  On allocation
 * failure the result is left in a destroy-safe state (all
 * pointers NULL, all counts 0).
 *
 * @return 0 on success, -1 on allocation failure.
 */
int  nfs4_res_layoutget_alloc(struct nfs4_res_layoutget *r,
			      uint32_t ds_count,
			      uint32_t ff_mirror_count);

/**
 * Free every heap buffer reachable from @p r and zero the relevant
 * fields (counts and pointers).  Other fields (stateid, offsets,
 * stripe_unit, etc.) are left untouched so callers can re-use the
 * struct after a destroy without re-initialising them.  NULL-safe.
 */
void nfs4_res_layoutget_destroy(struct nfs4_res_layoutget *r);

/* Flex-files transport advertisement policy. */
enum ff_transport_policy {
	FF_TRANSPORT_TCP  = 0,  /**< TCP only (default, Phase 3 compat) */
	FF_TRANSPORT_RDMA = 1,  /**< RDMA only */
	FF_TRANSPORT_BOTH = 2,  /**< Both, RDMA first */
};

struct nfs4_ds_endpoint {
	char     netid[8];
	char     host[256];
	uint16_t port;
};

struct nfs4_res_getdeviceinfo {
	uint32_t layout_type;
	uint32_t ds_count;
	struct {
		uint32_t ds_id;
		char     addr[256];  /**< DEPRECATED: legacy "host:port" */
		uint16_t port;       /**< Legacy single port */
		uint32_t endpoint_count;
		struct nfs4_ds_endpoint endpoints[2];
	} ds[MDS_MAX_LAYOUT_DS];
};

struct nfs4_res_layoutreturn {
	bool                stateid_present;
	struct nfs4_stateid stateid;
};

struct nfs4_res_layoutcommit {
	bool     new_size;
	uint64_t new_size_value;
};

struct ds_prepare_ctx;
struct ds_cache;

/* Tagged union of operation results. */
struct nfs4_result {
	enum nfs_opnum4  opnum;
	enum nfs4_status status;
	union {
		/* NFSv4.1 */
		struct nfs4_res_access         access;
		struct nfs4_res_lock            lock;
		struct nfs4_res_locku           locku;
		struct nfs4_res_getfh          getfh;
		struct nfs4_res_getattr        getattr;
		struct nfs4_res_create         create;
		struct nfs4_res_readdir        readdir;
		struct nfs4_res_open           open;
		struct nfs4_res_close          close;
		struct nfs4_res_change_info     change_info;
		struct nfs4_res_read           read;
		struct nfs4_res_write          write;
		struct nfs4_res_secinfo        secinfo;
		struct nfs4_res_commit         commit;
		struct nfs4_res_readlink       readlink;
		struct nfs4_res_exchange_id    exchange_id;
		struct nfs4_res_create_session create_session;
		struct nfs4_res_sequence       sequence;
		struct nfs4_res_test_stateid  test_stateid;
		struct nfs4_res_get_dir_delegation get_dir_delegation;
		/* pNFS layout */
		struct nfs4_res_layoutget      layoutget;
		struct nfs4_res_getdeviceinfo  getdeviceinfo;
		struct nfs4_res_layoutreturn   layoutreturn;
		struct nfs4_res_layoutcommit   layoutcommit;
		/* NFSv4.2 */
		struct nfs4_res_seek           seek;
		struct nfs4_res_copy           copy;
		struct nfs4_res_copy_notify    copy_notify;
		struct nfs4_res_offload_status offload_status;
		struct nfs4_res_io_advise      io_advise;
		struct nfs4_res_read_plus      read_plus;
		/* RFC 8276 xattr */
		struct nfs4_res_getxattr       getxattr;
		struct nfs4_res_setxattr       setxattr;
		struct nfs4_res_listxattrs     listxattrs;
		struct nfs4_res_removexattr    removexattr;
	} res;
};

/* -----------------------------------------------------------------------
 * Compound processing context
 * ----------------------------------------------------------------------- */

/**
 * Per-COMPOUND processing context.
 *
 * Single-use: call compound_init() before each COMPOUND request.
 * Fields such as sequence_done and current/saved FH are accumulated
 * across ops within one compound_process() call and must not carry
 * over to the next COMPOUND.
 */


/* NOLINTNEXTLINE(clang-analyzer-optin.performance.Padding) */
struct compound_data {
	enum ff_transport_policy ff_policy; /**< Flex-files transport policy */
	bool gpudirect_required; /**< Require 0 for placement */
	uint32_t auth_flavor;     /**< RPC credential flavor (AUTH_SYS=1, etc.) */
	uint32_t cred_uid;        /**< AUTH_SYS caller UID. */
	uint32_t cred_gid;        /**< AUTH_SYS caller GID. */
	uint32_t aux_gids[16];    /**< AUTH_SYS supplementary GIDs. */
	uint32_t aux_gid_count;   /**< Number of supplementary GIDs. */
	uint32_t gss_svc;          /**< GSS service level (0 if not GSS) */
	enum nfs_auth_mode min_auth; /**< Minimum required auth level */
	struct mds_catalogue     *cat;     /* Catalogue handle */
	struct commit_queue      *root_cq;
	struct session_table     *st;  /* NULL = SEQUENCE auto-passes (tests) */
	struct open_state_table  *ot;  /* NULL = OPEN/CLOSE return NOTSUPP */
	struct lock_table        *lt;  /* NULL = LOCK/LOCKT/LOCKU return NOTSUPP */
	struct mds_proxy_ctx     *proxy; /* NULL = proxy READ/WRITE return NOTSUPP */
	struct copy_offload_table *cot;  /* NULL = COPY returns sync-only */
	struct health_monitor    *hm;   /* NULL = repl health check disabled */
	struct rpc_conn          *conn; /* Live client RPC connection (NULL in tests) */
	/* Cluster -- NULL = cluster features disabled (single-node mode). */
	struct subtree_map               *smap;
	const struct rename_2pc_transport *transport;
	const struct cluster_membership  *membership; /* NULL = no peer resolution */
	struct commit_queue      *cq;   /* NULL = direct txns (tests/legacy) */
	struct ds_health_monitor *ds_hm; /* NULL = LAYOUTERROR ignored */
	struct io_tracker        *io_tracker; /* NULL = tiering I/O tracking disabled */
	struct mds_quota_ctx     *quota;    /* NULL = quota enforcement disabled */
	struct ds_prealloc_ctx   *prealloc; /* NULL = inline mode (single-MDS) */
	struct ds_prepare_ctx    *ds_prepare; /* NULL = generic DS async prepare disabled */
	struct ds_cache          *ds_cache;   /* NULL = fall back to catalogue reads */
	struct inode_cache       *icache;     /* NULL = no cross-compound caching */
	struct dirent_cache      *dcache;     /* NULL = no dirent/negative caching */
	/* Phase D of docs/hpc-nto1-plan.md -- per-inode stripe-map cache.
	 * Populated and consumed only for inodes with MDS_IFLAG_HPC_SHARED;
	 * NULL means "no cache" (every LAYOUTGET reads the catalogue).
	 * Cache contents must be invalidated by op_remove (final unlink)
	 * and op_setattr (size shrink) so a stale stripe map cannot leak
	 * into a layout grant for a later (post-shrink / post-recreate)
	 * file at the same fileid. */
	struct layout_cache      *lcache;     /* NULL = no HPC layout cache */
	/* Phase F of docs/hpc-nto1-plan.md -- per-fileid LAYOUTCOMMIT
	 * aggregator.  When non-NULL, op_layoutcommit on HPC-Shared
	 * inodes routes through the aggregator instead of writing
	 * synchronously, op_getattr coalesces with the aggregator
	 * snapshot per cfg.hpc_getattr_mode, and op_remove drops the
	 * bucket on final unlink.  NULL means "no aggregator" and the
	 * legacy synchronous LAYOUTCOMMIT path runs unchanged. */
	struct layout_commit_aggregator *lcommit_agg;
	struct deleg_table       *dt;         /* NULL = no delegations */
	struct dir_deleg_table   *ddt;        /* NULL = no dir delegations */
	/*
	 * Layout recall coordinator (NULL = no recall, legacy path).
	 * Used by op_layoutget for byte-range conflict-recall and by
	 * any future cross-client revoke initiated from a compound op.
	 * Borrowed: lifetime is the daemon's, set once at startup.
	 */
	struct layout_recall     *lr;
	struct mds_shard_map     *shard_map; /* NULL = no shard routing (tests/legacy) */
	const struct mds_shard   *current_shard; /* Active shard (set by FH/path transitions) */
	const struct mds_shard   *saved_shard;   /* SAVEFH snapshot of current_shard */
	struct nfs4_fh_desc       current_fh;
	struct nfs4_fh_desc       saved_fh;
	uint64_t                  clientid;  /* Set by SEQUENCE from session */
	bool                      current_fh_set;
	bool                      saved_fh_set;
	uint32_t                  mds_id;       /* This MDS node numeric ID */
	uint32_t                  cfg_stripe_unit; /* Config stripe_unit_bytes (0 = default 64KiB) */
	/*
	 * Phase 1 placement policy dispatch.  cfg_placement_policy_enabled
	 * gates the new path; when false, LAYOUTGET's new-file branch uses
	 * the legacy RR placement_select() (today's behaviour).  When true,
	 * LAYOUTGET routes through placement_select_ex(cfg_placement_policy).
	 * Both fields are populated from the rpc_server config at compound
	 * init time.
	 */
	enum mds_placement_policy cfg_placement_policy;
	bool                      cfg_placement_policy_enabled;
	/*
	 * Phase 3: default stripe geometry for new-file LAYOUTGET.
	 * Both default to 1 (legacy behaviour); consulted only when
	 * cfg_placement_policy_enabled is true.
	 */
	uint32_t                  cfg_default_stripe_count;
	uint32_t                  cfg_default_mirror_count;
	/*
	 * Phase F of docs/hpc-nto1-plan.md -- GETATTR consistency mode
	 * for HPC-Shared inodes.  STRICT (default) forces a flush of
	 * the LAYOUTCOMMIT aggregator before serving GETATTR;
	 * OPTIMISTIC peeks the bucket and overlays max(size) /
	 * latest(mtime) without flushing.  Consumed only when
	 * cd->lcommit_agg != NULL AND the inode carries
	 * MDS_IFLAG_HPC_SHARED.  See enum mds_hpc_getattr_mode.
	 */
	enum mds_hpc_getattr_mode cfg_hpc_getattr_mode;
	/*
	 * Phase C / Step 5 of docs/hpc-nto1-plan.md -- wide pre-warm
	 * stripe-count cap for HPC-Shared CREATEs.  0 selects the
	 * compile-time default (128, matches src/common/config.c).
	 * Consumed by op_open(CREATE) when the parent inode carries
	 * MDS_IFLAG_HPC_SHARED.  Plain (non-HPC) CREATEs ignore this
	 * field.
	 */
	uint32_t                  cfg_hpc_max_stripe_count;
	/*
	 * Phase C of docs/hpc-nto1-plan.md -- flex-files layout XDR
	 * wire form for HPC-Shared inodes.  Consumed by op_layoutget()
	 * when populating the response: AUTO selects striped iff the
	 * inode is HPC-Shared with mirror_count == 1 && stripe_count > 1;
	 * LEGACY / STRIPED force the form.  Plain inodes always emit
	 * the legacy one-DS-per-mirror form regardless of this setting.
	 */
	enum mds_hpc_xdr_form     cfg_hpc_xdr_form;
	uint64_t                  write_verf;   /* Server boot epoch (writeverf4) */
	uint32_t                  minorversion; /* NFSv4 minor version (0 or 1) */
	bool                      sequence_done; /* Set by successful SEQUENCE */
	bool                      replay_cached; /* DRC: cached reply available */
	/* Path tracking for subtree ownership checks. */
	char                      current_path[MDS_MAX_PATH];
	char                      saved_path[MDS_MAX_PATH];
	/* Xattr state -- set by OPENATTR+LOOKUP for READ/WRITE on xattr objects. */
	char                      xattr_name[MDS_XATTR_NAME_MAX + 1];
	bool                      xattr_obj_set;  /* xattr_name is valid */

	/*
	 * Request-local inode snapshots.
	 *
	 * Mirrors current_fh / saved_fh.  Seeded by ops that read
	 * the inode (LOOKUP, PUTFH, GETATTR, OPEN), consumed by
	 * subsequent ops (ACCESS, GETATTR), and invalidated
	 * immediately on any mutation within the same compound.
	 * Avoids repeated catalogue reads for the same fileid within
	 * one request.  No cross-request state.
	 */
	struct mds_inode          current_inode;
	bool                      current_inode_valid;
	struct mds_inode          saved_inode;
	bool                      saved_inode_valid;

	/*
	 * Recently-created stripe cache.
	 *
	 * When OPEN(CREATE) creates a file with a pre-allocated stripe,
	 * the ds_id is stashed here.  LAYOUTGET checks this before
	 * reading NDB, eliminating 2 NDB round-trips for new files.
	 * Valid only within the same compound processing context.
	 */
	bool                      stripe_cached;
	uint64_t                  stripe_cached_fileid;
	uint32_t                  stripe_cached_ds_id;
	uint32_t                  stripe_cached_stripe_unit;

	/*
	 * Layout pre-grant -- fused CREATE + LAYOUTGET CQ commit.
	 *
	 * When op_open(CREATE) detects a following LAYOUTGET in the
	 * same compound and the writer thread successfully persists
	 * the layout_state in the CREATE CQ transaction, these fields
	 * are set so op_layoutget() can skip its own CQ submit.
	 *
	 * Scoped to the single-shard CQ path only.  Cleaned up by
	 * compound_process() if LAYOUTGET never consumes the grant.
	 */
	bool                      layout_pregranted;
	uint64_t                  layout_pregrant_fileid;
	struct nfs4_stateid       layout_pregrant_stateid;

	/* Skip NDB persistence for transient protocol state
	 * (open_state, layout_state).  Set from config
	 * transient_state_cache flag. */
	bool                      skip_transient_ndb;

	/* RFC 8881 S16.2.4 -- current stateid tracking.
	 *
	 * Updated by ops that PRODUCE a stateid (OPEN, OPEN_DOWNGRADE,
	 * CLOSE, LOCK, LOCKU, LAYOUTGET, LAYOUTRETURN).  Read by ops that
	 * CONSUME a stateid when the wire stateid is the special
	 * CURRENT_STATEID4 marker (seqid==1, other==all-zeros, RFC 8881
	 * S16.2.3.1.2).  Cleared by FH-mutating ops (PUTFH, PUTROOTFH,
	 * LOOKUP, LOOKUPP).  Saved/restored by SAVEFH/RESTOREFH alongside
	 * the current FH.
	 *
	 * `current_stateid_set` is true once a producing op has set the
	 * value within this compound.  When false, resolving the magic
	 * value yields NFS4ERR_BAD_STATEID (matches Linux NFSD; pynfs
	 * CSID5/CSID6 expect NFS4ERR_STALE_STATEID or NFS4ERR_BAD_STATEID).
	 *
	 * `saved_stateid` mirrors `saved_fh` for the SAVEFH/RESTOREFH
	 * pair so a compound that saves an FH+stateid pair, swaps to a
	 * different FH, and restores can resume consuming current_stateid
	 * from the saved producer (pynfs CSID10).
	 */
	struct nfs4_stateid       current_stateid;
	bool                      current_stateid_set;
	struct nfs4_stateid       saved_stateid;
	bool                      saved_stateid_set;

	/* Active COMPOUND op stream (valid only during compound_process). */
	const struct nfs4_op      *ops;
	uint32_t                  op_count;
	uint32_t                  op_index;
};

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

/**
 * Initialise a compound processing context.
 *
 * Must be called once before compound_process().  The resulting context
 * is single-use: callers MUST call compound_init() again before issuing
 * another COMPOUND (sequence_done, current/saved FH carry over otherwise).
 * The wire-path server (rpc_server.c) already does this correctly.
 *
 * @param cd   Compound context to initialise (caller-owned).
 * @param cat  Catalogue handle used for namespace operations.
 */
void compound_init(struct compound_data *cd);

/**
 * RFC 8881 S1.7 / S14.4 -- UTF-8 well-formedness check.
 *
 * Returns true iff @a buf points at @a len bytes that form a valid
 * UTF-8 sequence: no overlong encodings, no surrogates, no codepoints
 * above U+10FFFF, no isolated continuation bytes.  An embedded NUL
 * (0x00) is treated as valid UTF-8 here -- callers that need to
 * forbid it (component4 names, compound tag) check for it separately.
 *
 * Used by compound_validate_name() (compound_internal.h) and the
 * compound-tag validator in rpc_server.c to honour pynfs RNM8/9
 * (testBadutf8*) and COMP3 (testBadTags) which iterate
 * get_invalid_utf8strings() and expect NFS4ERR_INVAL.
 */
bool compound_is_valid_utf8(const char *buf, size_t len);

/**
 * Free heap state owned by a result.  Currently only the layoutget
 * result has heap-allocated wire buffers (Phase C / Step 1); other
 * opnum values are no-ops.  NULL-safe.
 *
 * Callers MUST invoke this on every result after the response has
 * been encoded and sent -- typically immediately before reusing the
 * thread-local result array on the next compound.  compound_process
 * itself does not call this because the heap buffers are still
 * needed by the encoder after the function returns.
 */
void nfs4_result_destroy(struct nfs4_result *r);

/**
 * Process a single NFSv4.1 COMPOUND request.
 *
 * Operations are executed sequentially.  Processing stops on the first
 * error (per RFC 8881 S2.6.3.1.1.4).  The result array must have room
 * for @count entries.
 *
 * NOTE: this function is designed for single-COMPOUND-per-init use.
 * Fields such as sequence_done, current_fh, and saved_fh are NOT
 * reset between calls.  Always call compound_init() before each
 * new COMPOUND request to ensure clean state.
 *
 * @param cd       Compound context (initialised by compound_init).
 * @param ops      Array of operations.
 * @param results  Array of results (one per op).
 * @param count    Number of operations.
 * @return Number of results written (always <= count).
 */
uint32_t compound_process(struct compound_data *cd,
			   const struct nfs4_op *ops,
			   struct nfs4_result *results,
			   uint32_t count);

/**
 * Map internal mds_status to NFSv4 status code.
 */
enum nfs4_status mds_status_to_nfs4(enum mds_status st);

#endif /* COMPOUND_H */

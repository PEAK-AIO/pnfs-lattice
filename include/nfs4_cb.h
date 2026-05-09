/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * nfs4_cb.h -- NFSv4.1 callback channel sender (CB_COMPOUND).
 *
 * Sends server-initiated callback operations to clients on their
 * existing forechannel TCP connection (RFC 8881 S2.10.3.1).
 *
 * See docs/architecture.md S15 for DS failure detection design.
 */

#ifndef NFS4_CB_H
#define NFS4_CB_H

#include <stdint.h>
#include "session.h"    /* struct nfs4_session, struct nfs4_cb_sec */
#include "open_state.h" /* struct nfs4_stateid */

/**
 * layoutrecall_type4 (RFC 8881 S20.3).
 */
enum layoutrecall_type {
    LAYOUTRECALL4_FILE = 1,
    LAYOUTRECALL4_FSID = 2,
    LAYOUTRECALL4_ALL  = 3,
};

/**
 * Arguments for a single CB_LAYOUTRECALL.
 */
struct nfs4_cb_layoutrecall_args {
    uint32_t             layout_type;   /**< LAYOUT4_NFSV4_1_FILES etc. */
    uint32_t             iomode;        /**< LAYOUTIOMODE4_READ / _RW / _ANY */
    enum layoutrecall_type recall_type;
    /* For LAYOUTRECALL4_FILE: */
    uint64_t             fileid;        /**< File handle (our fileid). */
    struct nfs4_stateid  stateid;       /**< Layout stateid to recall. */
    uint64_t             offset;
    uint64_t             length;
};

/**
 * Send CB_LAYOUTRECALL to a client via an active session.
 *
 * Builds a CB_COMPOUND { CB_SEQUENCE, CB_LAYOUTRECALL }, XDR-encodes it,
 * sends it on the session's backchannel connection, reads and decodes
 * the reply.
 *
 * @param session  Session with a bound backchannel (cb_conn != NULL).
 * @param args     Recall arguments.
 * @param timeout_ms  Reply timeout in milliseconds (0 = default 5000).
 * @return 0 on success (client acknowledged the recall).
 *         -ENOTCONN if no backchannel connection.
 *         -ETIMEDOUT if reply timeout.
 *         -EIO on XDR/RPC errors.
 *         Positive NFS4 status on callback error.
 */
int nfs4_cb_layoutrecall(struct nfs4_session *session,
                         const struct nfs4_cb_layoutrecall_args *args,
                         uint32_t timeout_ms);


/**
 * Send CB_LAYOUTRECALL on a raw file descriptor.
 *
 * Same as nfs4_cb_layoutrecall() but takes pre-extracted callback
 * channel parameters instead of a session pointer.  This is used by
 * the layout recall coordinator which works with dup'd fds from
 * session snapshots (to avoid rpc_conn lifetime issues).
 *
 * @param fd           Connected socket fd (caller owns lifetime).
 * @param session_id   16-byte session ID.
 * @param cb_prog      Callback program number.
 * @param slot_seq_id  Backchannel slot 0 sequence ID.
 * @param num_cb_slots Total backchannel slots (for highest_slot).
 * @param args         Recall arguments.
 * @param timeout_ms   Reply timeout (0 = default 5000).
 * @return 0 on success, negative errno on failure.
 */
int nfs4_cb_layoutrecall_fd(int fd,
                            const uint8_t session_id[SESSION_ID_SIZE],
                            uint32_t cb_prog,
                            uint32_t slot_seq_id,
                            uint32_t num_cb_slots,
                            uint32_t minorversion,
                            const struct nfs4_cb_sec *sec,
                            const struct nfs4_cb_layoutrecall_args *args,
                            uint32_t timeout_ms);

/**
 * Arguments for a single CB_RECALL (RFC 8881 S20.2).
 *
 * Used to ask a client to return a delegation.  The wire form is
 * stateid + truncate flag + filehandle.
 */
struct nfs4_cb_recall_args {
	struct nfs4_stateid stateid;   /**< Delegation stateid to recall. */
	bool                truncate;  /**< Server will truncate the file. */
	uint64_t            fileid;    /**< Encoded as 8-byte filehandle. */
};

/**
 * Send CB_RECALL to a client via an active session.
 *
 * Builds a CB_COMPOUND { CB_SEQUENCE, CB_RECALL }, XDR-encodes it,
 * sends it on the session's backchannel connection, waits for the
 * reply.
 *
 * @param session    Session with a bound backchannel (cb_conn != NULL).
 * @param args       Recall arguments.
 * @param timeout_ms Reply timeout in milliseconds (0 = default 5000).
 * @return 0 on success (client acknowledged the recall).
 *         -ENOTCONN if no backchannel connection.
 *         -ETIMEDOUT if reply timeout.
 *         -EIO on XDR/RPC errors.
 *         Positive NFS4 status on callback error.
 */
int nfs4_cb_recall(struct nfs4_session *session,
		   const struct nfs4_cb_recall_args *args,
		   uint32_t timeout_ms);

/**
 * Send CB_RECALL on a raw file descriptor.
 *
 * Same as nfs4_cb_recall() but takes pre-extracted callback channel
 * parameters instead of a session pointer.  Mirrors the
 * nfs4_cb_layoutrecall_fd() helper used by the layout recall
 * coordinator: callers snapshot the session's cb metadata under the
 * session-table lock, dup() the cb_conn fd, then perform the I/O
 * outside the lock with a stable fd ownership boundary.
 *
 * @param fd            Connected socket fd (caller owns lifetime).
 * @param session_id    16-byte session ID.
 * @param cb_prog       Callback program number.
 * @param slot_seq_id   Backchannel slot 0 sequence id.
 * @param num_cb_slots  Total backchannel slots.
 * @param args          Recall arguments.
 * @param timeout_ms    Reply timeout (0 = default 5000).
 * @return 0 on success, negative errno on failure, positive NFS4
 *         status on per-op error in the reply.
 */
int nfs4_cb_recall_fd(int fd,
		      const uint8_t session_id[SESSION_ID_SIZE],
		      uint32_t cb_prog,
		      uint32_t slot_seq_id,
		      uint32_t num_cb_slots,
		      uint32_t minorversion,
		      const struct nfs4_cb_sec *sec,
		      const struct nfs4_cb_recall_args *args,
		      uint32_t timeout_ms);

/**
 * Arguments for a single CB_NOTIFY (RFC 8881 S20.4).
 *
 * v8c supports three structural event types:
 *   - NOTIFY4_REMOVE_ENTRY -- old_name is the removed child.
 *   - NOTIFY4_ADD_ENTRY    -- new_name is the added child.
 *   - NOTIFY4_RENAME_ENTRY -- old_name -> new_name within the dir.
 *
 * Phase 8d extends this with NOTIFY4_CHANGE_CHILD_ATTRS and
 * NOTIFY4_CHANGE_DIR_ATTRS once fattr4 payloads are supported.
 */
struct nfs4_cb_notify_args {
	struct nfs4_stateid stateid;     /**< Dir-deleg stateid. */
	uint64_t            dir_fileid;  /**< Delegated directory. */
	uint32_t            notify_type; /**< One of NOTIFY4_*_ENTRY. */
	char                old_name[256];
	uint32_t            old_name_len;
	char                new_name[256];
	uint32_t            new_name_len;
};

/**
 * Send CB_NOTIFY to a client via an active session.
 *
 * Builds a CB_COMPOUND { CB_SEQUENCE, CB_NOTIFY } message, XDR-encodes
 * it, sends it on the session's backchannel connection, and reads
 * the reply.  The wire format matches RFC 8881 S20.4.
 *
 * @param session    Session with a bound backchannel (cb_conn != NULL).
 * @param args       Notification payload.
 * @param timeout_ms Reply timeout in milliseconds (0 = default 5000).
 * @return 0 on success (client acknowledged).
 *         -EINVAL for bad arguments.
 *         -ENOTCONN if no backchannel.
 *         -ETIMEDOUT on reply timeout.
 *         -EIO on XDR/RPC errors.
 *         Positive NFS4 status on per-op error in the reply.
 */
int nfs4_cb_notify(struct nfs4_session *session,
		   const struct nfs4_cb_notify_args *args,
		   uint32_t timeout_ms);

/**
 * Send CB_NOTIFY on a raw file descriptor.
 *
 * Parallels nfs4_cb_layoutrecall_fd -- used by the dir_notify
 * emitter which snapshots callback-channel metadata under the
 * session-table lock and then performs I/O outside the lock using
 * a dup'd fd (to avoid rpc_conn lifetime issues).
 *
 * @param fd            Connected socket fd (caller owns lifetime).
 * @param session_id    16-byte session ID.
 * @param cb_prog       Callback program number.
 * @param slot_seq_id   Backchannel slot 0 sequence id.
 * @param num_cb_slots  Total backchannel slots.
 * @param args          Notification payload.
 * @param timeout_ms    Reply timeout (0 = default 5000).
 * @return 0 on success, negative errno on failure.
 */
int nfs4_cb_notify_fd(int fd,
		      const uint8_t session_id[SESSION_ID_SIZE],
		      uint32_t cb_prog,
		      uint32_t slot_seq_id,
		      uint32_t num_cb_slots,
		      uint32_t minorversion,
		      const struct nfs4_cb_sec *sec,
		      const struct nfs4_cb_notify_args *args,
		      uint32_t timeout_ms);

/**
 * Result of a CB_GETATTR round-trip (RFC 8881 §20.1).
 */
struct nfs4_cb_getattr_result {
	uint64_t size;    /**< FATTR4_SIZE from the delegation holder. */
	uint64_t change;  /**< FATTR4_CHANGE from the delegation holder. */
	bool     valid;   /**< true iff the callback succeeded. */
};

/**
 * Send CB_GETATTR on a raw fd and read the reply.
 *
 * Builds CB_COMPOUND { CB_SEQUENCE, CB_GETATTR }, sends it, reads the
 * RPC record reply, and parses the returned fattr4 for SIZE + CHANGE.
 *
 * @param fd            Connected backchannel socket (caller owns).
 * @param session_id    16-byte session ID.
 * @param cb_prog       Callback program number.
 * @param slot_seq_id   Backchannel slot 0 sequence id.
 * @param num_cb_slots  Total backchannel slots.
 * @param minorversion  Session minor version.
 * @param sec           Callback security parameters.
 * @param fileid        File whose attrs to query.
 * @param deleg_stateid Delegation stateid for the file.
 * @param timeout_ms    Reply timeout (0 = default).
 * @param out           Receives the result.
 * @return 0 on success, negative errno on failure.
 */
int nfs4_cb_getattr_fd(int fd,
		       const uint8_t session_id[SESSION_ID_SIZE],
		       uint32_t cb_prog,
		       uint32_t slot_seq_id,
		       uint32_t num_cb_slots,
		       uint32_t minorversion,
		       const struct nfs4_cb_sec *sec,
		       uint64_t fileid,
		       uint32_t mds_id,
		       const struct nfs4_stateid *deleg_stateid,
		       uint32_t timeout_ms,
		       struct nfs4_cb_getattr_result *out);

/**
 * Override the default callback timeout used when a caller passes
 * timeout_ms=0.  Wired from the INI `cb_recall_timeout_ms` key at
 * daemon startup; safe to call at any point.  Values <50ms are
 * clamped to 50ms so a misconfiguration does not break the
 * backchannel.
 */
void nfs4_cb_set_default_timeout(uint32_t timeout_ms);

/* -----------------------------------------------------------------------
 * Pending CB reply slot — multiplexed backchannel reply delivery
 *
 * RFC 8881 §2.10.3.1: the backchannel shares the forechannel TCP
 * connection.  The RPC record reader must demux incoming REPLY
 * records (msg_type=1) and route them here so synchronous CB
 * operations (CB_GETATTR) can receive their replies.
 * ----------------------------------------------------------------------- */

/** One-time init.  Call from daemon startup before any CB send. */
void nfs4_cb_pending_reply_init(void);

/**
 * Deliver a callback reply record from the RPC reader.
 *
 * Called by the epoll/conn_read path when it reads an RPC record
 * with msg_type == 1 (REPLY).  If a pending slot is waiting, the
 * record is copied and the waiter is signalled.
 *
 * @param buf  Complete RPC record (without the 4-byte frag header).
 * @param len  Record length.
 */
void nfs4_cb_deliver_reply(const uint8_t *buf, uint32_t len);

#endif /* NFS4_CB_H */

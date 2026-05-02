/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 */
/*
 * compound_session.c — NFSv4.1 session management ops.
 */

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>

#include "pnfs_mds.h"
#include "compound.h"
#include "compound_internal.h"
#include "session.h"
#include "grace.h"


/* -----------------------------------------------------------------------
 * Operation handlers
 *
 * Each returns an NFS4 status code.  On NFS4_OK, the result union
 * fields are populated as needed.
 * ----------------------------------------------------------------------- */

enum nfs4_status op_sequence(struct compound_data *cd,
				    const struct nfs4_op *op,
				    struct nfs4_result *res)
{
	const struct nfs4_arg_sequence *a = &op->arg.sequence;
	struct nfs4_res_sequence *r = &res->res.sequence;
	int rc;

	/* If no session table configured, auto-pass (test compat). */
	if (cd->st == NULL) {
		cd->sequence_done = true;
		return NFS4_OK;
	}

	rc = session_sequence_check(cd->st,
				    a->session_id,
				    a->slot_id,
				    a->seq_id,
				    a->highest_slot_id,
				    &r->highest_slot_id,
				    &r->target_highest_slot_id,
				    &r->status_flags,
				    &cd->clientid);
	switch (rc) {
	case 0:  /* new request */
		memcpy(r->session_id, a->session_id, SESSION_ID_SIZE);
		r->slot_id = a->slot_id;
		r->seq_id = a->seq_id;
		cd->sequence_done = true;
		return NFS4_OK;
	case 1:
		/*
		 * Replay detected, no cached reply.
		 * RFC 8881 §2.10.6.1.3: SEQ_FALSE_RETRY.
		 */
		return NFS4ERR_SEQ_FALSE_RETRY;
	case 2:
		/*
		 * Replay with cached reply available.
		 * Signal rpc_server to send the cached response.
		 */
		cd->replay_cached = true;
		memcpy(r->session_id, a->session_id, SESSION_ID_SIZE);
		r->slot_id = a->slot_id;
		r->seq_id = a->seq_id;
		cd->sequence_done = true;
		return NFS4_OK;
	case -1: return NFS4ERR_BADSESSION;
	case -2: return NFS4ERR_BADSLOT;
	case -3: return NFS4ERR_SEQ_MISORDERED;
	default: return NFS4ERR_SERVERFAULT;
	}
}

enum nfs4_status op_exchange_id(struct compound_data *cd,
				       const struct nfs4_op *op,
				       struct nfs4_result *res)
{
	const struct nfs4_arg_exchange_id *a = &op->arg.exchange_id;
	struct nfs4_res_exchange_id *r = &res->res.exchange_id;
	int rc;

	if (cd->st == NULL) {
		return NFS4ERR_DELAY;
	}

	rc = session_exchange_id(cd->st,
				 a->co_ownerid,
				 a->co_ownerid_len,
				 a->verifier,
				 a->eia_flags,
				 &r->clientid,
				 &r->seqid,
				 &r->eir_flags);
	if (rc != 0) {
		return NFS4ERR_SERVERFAULT;
	}

	return NFS4_OK;
}

enum nfs4_status op_create_session(struct compound_data *cd,
					  const struct nfs4_op *op,
					  struct nfs4_result *res)
{
	const struct nfs4_arg_create_session *a = &op->arg.create_session;
	struct nfs4_res_create_session *r = &res->res.create_session;
	int rc;

	if (cd->st == NULL) {
		return NFS4ERR_SERVERFAULT;
}

	rc = session_create_session(cd->st,
				    a->clientid,
				    a->seqid,
				    a->fore_slots,
				    a->back_slots,
				    a->cb_prog,
				    a->cb_sec_flavor,
				    r->session_id,
				    &r->fore_slots,
				    &r->back_slots);
	switch (rc) {
	case 0:
		r->csr_sequence = a->seqid;
		r->csr_flags = 0;
		/* Bind this client connection as the backchannel transport. */
		if (cd->conn != NULL) {
			(void)session_bind_conn(cd->st, r->session_id,
					       cd->conn);
}
		return NFS4_OK;
	case -1: return NFS4ERR_STALE_CLIENTID;
	case -2: return NFS4ERR_SEQ_MISORDERED;
	default: return NFS4ERR_SERVERFAULT;
	}
}

enum nfs4_status op_destroy_session(struct compound_data *cd,
					   const struct nfs4_op *op,
					   struct nfs4_result *res)
{
	const struct nfs4_arg_destroy_session *a = &op->arg.destroy_session;
	int rc;

	(void)res;

	if (cd->st == NULL) {
		return NFS4ERR_SERVERFAULT;
}

	rc = session_destroy_session(cd->st, a->session_id);
	if (rc != 0) {
		return NFS4ERR_BADSESSION;
}

	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * ACCESS (RFC 8881 §18.1) — mandatory.
 *
 * Phase 1: grant all requested access bits (no POSIX ACL enforcement).
 * FIXME: enforce inode mode bits once uid/gid context is available.
 * ----------------------------------------------------------------------- */

#define ACCESS4_READ    0x00000001
#define ACCESS4_LOOKUP  0x00000002
#define ACCESS4_MODIFY  0x00000004
#define ACCESS4_EXTEND  0x00000008
#define ACCESS4_DELETE  0x00000010
#define ACCESS4_EXECUTE 0x00000020
#define ACCESS4_ALL     0x0000003F


/* -----------------------------------------------------------------------
 * RECLAIM_COMPLETE (RFC 8881 §18.51)
 *
 * Signals that the client has finished reclaiming all previously held
 * state.  The grace module tracks per-client completion; once all
 * tracked clients have sent RECLAIM_COMPLETE, grace period ends.
 * ----------------------------------------------------------------------- */
enum nfs4_status op_reclaim_complete(const struct compound_data *cd,
					    const struct nfs4_op *op,
					    struct nfs4_result *res)
{
	(void)op;
	(void)res;

	if (!cd->sequence_done) {
		return NFS4ERR_OP_NOT_IN_SESSION;
}

	if (!grace_is_active()) {
		return NFS4ERR_COMPLETE_ALREADY;
}

	if (grace_client_reclaimed(cd->clientid) < 0) {
		return NFS4ERR_COMPLETE_ALREADY;
}

	return NFS4_OK;
}

/* -----------------------------------------------------------------------
 * Dispatch table
 * ----------------------------------------------------------------------- */

typedef enum nfs4_status (*op_handler_t)(struct compound_data *,
					const struct nfs4_op *,
					struct nfs4_result *);


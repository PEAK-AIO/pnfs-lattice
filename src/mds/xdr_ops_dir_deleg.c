/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 */
/*
 * xdr_ops_dir_deleg.c — RFC 8881 §18.39 GET_DIR_DELEGATION codec.
 *
 * Phase 8a: wire-format only.  The dispatch handler returns
 * NFS4ERR_DIRDELEG_UNAVAIL unconditionally, so the OK-branch of the
 * encoder is exercised only from Phase 8b onward when the dir-deleg
 * state table starts issuing real grants.
 *
 * The argument decoder must consume *every* byte of the GDD arg list
 * even when we intend to fail; otherwise the compound decoder cannot
 * continue to the next op (RFC 8881 §2.10.6.4).
 */
#include <stdint.h>
#include <stdbool.h>
#include <string.h>

#include "xdr_codec.h"
#include "xdr_internal.h"
#include "compound.h"

/* -----------------------------------------------------------------------
 * Decoder — RFC 8881 §18.39.1
 *
 *   struct GET_DIR_DELEGATION4args {
 *       -- CURRENT_FH: delegated directory --
 *       bool         gdda_signal_deleg_avail;
 *       bitmap4      gdda_notification_types;
 *       attr_notice4 gdda_child_attr_delay;    -- typedef nfstime4
 *       attr_notice4 gdda_dir_attr_delay;      -- typedef nfstime4
 *       bitmap4      gdda_child_attributes;
 *       bitmap4      gdda_dir_attributes;
 *   }
 * ----------------------------------------------------------------------- */
bool decode_op_get_dir_delegation(XDR *xdrs, struct nfs4_op *op)
{
	struct nfs4_arg_get_dir_delegation *a = &op->arg.get_dir_delegation;
	uint32_t signal_val = 0;

	if (!xdr_uint32_t(xdrs, &signal_val)) {
		return false;
	}
	a->signal_deleg_avail = (signal_val != 0);

	if (!xdr_nfs4_bitmap_decode(xdrs, a->notification_types,
				    NFS4_BITMAP_WORDS,
				    &a->notification_types_words)) {
		return false;
	}

	if (!xdr_nfs4_time_decode(xdrs, &a->child_attr_delay)) {
		return false;
	}
	if (!xdr_nfs4_time_decode(xdrs, &a->dir_attr_delay)) {
		return false;
	}

	if (!xdr_nfs4_bitmap_decode(xdrs, a->child_attributes,
				    NFS4_BITMAP_WORDS,
				    &a->child_attributes_words)) {
		return false;
	}
	if (!xdr_nfs4_bitmap_decode(xdrs, a->dir_attributes,
				    NFS4_BITMAP_WORDS,
				    &a->dir_attributes_words)) {
		return false;
	}
	return true;
}

/* -----------------------------------------------------------------------
 * Result encoder — RFC 8881 §18.39.2.
 *
 * Wire layout when the outer op status is NFS4_OK (the only path
 * this encoder handles; other outer statuses get a status-only body
 * via encode_one_result):
 *
 *   uint32_t  gddrnf_status            -- GDD4_OK | GDD4_UNAVAIL
 *   switch (gddrnf_status) {
 *   case GDD4_OK:
 *       verifier4   gddr_cookieverf
 *       stateid4    gddr_stateid
 *       bitmap4     gddr_notification
 *       bitmap4     gddr_child_attributes
 *       bitmap4     gddr_dir_attributes
 *   case GDD4_UNAVAIL:
 *       bool        gddrnf_will_signal_deleg_avail
 *   }
 *
 * The outer op status stays NFS4_OK in both cases so the compound
 * does not halt — halting would strip any trailing GETATTR the
 * kernel bundled after GDD and surface as EIO on the client.
 * ----------------------------------------------------------------------- */
bool encode_res_get_dir_delegation(XDR *xdrs, const struct nfs4_result *r)
{
	const struct nfs4_res_get_dir_delegation *g = &r->res.get_dir_delegation;
	uint32_t inner = g->gddrnf_status;

	if (!xdr_uint32_t(xdrs, &inner)) {
		return false;
	}

	if (g->gddrnf_status == GDD4_UNAVAIL) {
		int32_t will_signal = g->will_signal_deleg_avail ? 1 : 0;
		return xdr_putbool(xdrs, will_signal);
	}

	/* GDD4_OK body. */
	if (!xdr_opaque_encode(xdrs, (const char *)g->cookieverf,
			       NFS4_VERIFIER_SIZE)) {
		return false;
	}
	if (!xdr_nfs4_stateid_encode(xdrs, &g->stateid)) {
		return false;
	}
	if (!xdr_nfs4_bitmap_encode(xdrs, g->notification,
				    g->notification_words)) {
		return false;
	}
	if (!xdr_nfs4_bitmap_encode(xdrs, g->child_attributes,
				    g->child_attributes_words)) {
		return false;
	}
	if (!xdr_nfs4_bitmap_encode(xdrs, g->dir_attributes,
				    g->dir_attributes_words)) {
		return false;
	}
	return true;
}

/* -----------------------------------------------------------------------
 * Retained for ABI compatibility with xdr_codec.c's encode_error_body,
 * which still compiles a reference to this symbol.  With the fix to
 * return NFS4_OK + inner gddrnf_status, encode_error_body's
 * DIRDELEG_UNAVAIL path is no longer reachable, but keeping the
 * symbol avoids a link break until the corresponding branch is
 * removed.  The implementation is a no-op success.
 * ----------------------------------------------------------------------- */
bool xdr_encode_gdd_unavail_body(XDR *xdrs,
				 const struct nfs4_result *r)
{
	(void)xdrs;
	(void)r;
	return true;
}

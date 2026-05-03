/*
 * Copyright (c) 2026 PeakAIO. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-PeakAIO-Proprietary
 *
 * nfs4_cb_sec.h — NFSv4.1 callback security parameters (RFC 8881 §2.10.8.3).
 *
 * Extracted into its own header so both session.h (struct nfs4_session)
 * and nfs4_cb.h (struct session_cb_snap, fd helpers) can embed/reference
 * struct nfs4_cb_sec without creating a circular include chain.
 *
 * The struct holds the callback-channel security parameters captured
 * from CREATE_SESSION's csa_sec_parms<> (RFC 8881 §18.36) and updated
 * by BACKCHANNEL_CTL's bca_sec_parms<> (RFC 8881 §18.33).  The CB
 * encoder uses it to populate the RPC credential body (AUTH_NONE void
 * or AUTH_SYS authsys_parms{stamp, machinename, uid, gid, gids<16>})
 * of every CB_COMPOUND call.
 */

#ifndef NFS4_CB_SEC_H
#define NFS4_CB_SEC_H

#include <stdint.h>

/* RPC auth flavor numbers per RFC 5531.  Replicated here as
 * NFS4_CB_AUTH_* so we don't have to drag in the wider RPC headers. */
#ifndef NFS4_CB_AUTH_NONE
#define NFS4_CB_AUTH_NONE 0
#define NFS4_CB_AUTH_SYS  1
#endif

#define NFS4_CB_MACHNAME_MAX 256
#define NFS4_CB_AUX_GIDS_MAX 16

/*
 * Captured callback security parameters.  A zero-initialised struct
 * encodes as AUTH_NONE (RFC 8881 §2.10.8.3 permits AUTH_NONE for
 * callbacks; it produces a void RPC cred body).
 *
 * For AUTH_SYS the body fields carry the authsys_parms tuple verbatim
 * as decoded from CREATE_SESSION's callback_sec_parms4 entry: stamp,
 * machinename<255>, uid, gid, gids<16>.  RPCSEC_GSS is not yet wired;
 * any other flavor is treated as AUTH_NONE by the encoder.
 */
struct nfs4_cb_sec {
    uint32_t flavor;                              /* NFS4_CB_AUTH_NONE / NFS4_CB_AUTH_SYS */
    uint32_t sys_stamp;                           /* AUTH_SYS body fields */
    uint32_t sys_uid;
    uint32_t sys_gid;
    uint32_t sys_machname_len;
    uint32_t sys_ngids;
    char     sys_machname[NFS4_CB_MACHNAME_MAX];
    uint32_t sys_gids[NFS4_CB_AUX_GIDS_MAX];
};

#endif /* NFS4_CB_SEC_H */

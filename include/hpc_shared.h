/*
 * Copyright (c) 2026 PeakAIO
 * SPDX-License-Identifier: MIT
 *
 * hpc_shared.h -- Phase B helpers for the HPC-Shared file mode.
 *
 * Two entry points:
 *
 *   - hpc_shared_xattr_apply: handles set/clear of MDS_IFLAG_HPC_SHARED
 *     when a client SETXATTR/REMOVEXATTR targets the
 *     "trusted.pnfs.hpc_shared" control xattr.  Caller must be
 *     CAP_SYS_ADMIN (cred_uid == 0).  Does NOT store the bytes as a
 *     real xattr.
 *
 *   - hpc_shared_inherit_from_parent: propagates the flag from a
 *     directory inode to a freshly-created child.  Called from the
 *     OPEN(create) branch in compound_data_io.c and from op_create in
 *     compound_namespace.c so any file or directory created inside an
 *     HPC-Shared parent inherits the mode.
 *
 *   - hpc_shared_xattr_synthesize_value: produces the wire value for
 *     GETXATTR("trusted.pnfs.hpc_shared") so the same control xattr is
 *     a round-trippable knob ("0" or "1"), without an underlying
 *     stored xattr.
 *
 * Master design: docs/hpc-nto1-plan.md S5 Phase B.
 *
 * Operator note on the client-side xattr cache: the Linux NFS
 * xattr support (fs/nfs/nfs42xattr.c) marks freshly-created inodes
 * as `xattr cache complete, none present` and short-circuits
 * subsequent GETXATTR(name) calls locally without consulting the
 * server.  As a result, on an inherited child or a just-cleared
 * file, a `getfattr -n user.pnfs.hpc_shared` may report "No such
 * attribute" even though the server-side MDS_IFLAG_HPC_SHARED bit
 * is set / cleared as expected.  The flag itself is authoritative
 * for the HPC fast paths (Phase C / D / F consume it directly
 * from the inode); the xattr is only a control surface, not a
 * data carrier.  Phase I integration tests verify the bit via
 * catalogue introspection rather than client-side getfattr.
 */

#ifndef HPC_SHARED_H
#define HPC_SHARED_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "compound.h"  /* enum nfs4_status, struct compound_data */
#include "hpc_hint.h"

/** Canonical name of the control xattr that toggles HPC_SHARED.
 *
 * Note on namespace choice: trusted.* would be the Linux convention
 * for an admin-only knob, but the upstream Linux NFS xattr support
 * (RFC 8276 / fs/nfs/nfs42xattr.c) only forwards the `user.*`
 * namespace over the wire -- trusted.* is handled locally by the
 * client and never reaches the MDS.  Using `user.pnfs.hpc_shared`
 * lets operators toggle HPC mode with a normal `setfattr` from any
 * mounted client.  The privilege gate is enforced server-side by
 * hpc_shared_xattr_apply (cred_uid == 0), so an unprivileged user
 * cannot flip the bit and pre-warm wide stripes.  This trades the
 * trusted.* namespace tag for an AUTH_SYS uid check; deployments that
 * need stronger guarantees should already be running krb5/krb5p,
 * which the privilege gate composes with naturally. */
#define HPC_SHARED_XATTR_NAME "user.pnfs.hpc_shared"

/**
 * Apply the set / clear of MDS_IFLAG_HPC_SHARED on the inode pointed
 * to by cd->current_fh, as triggered by a SETXATTR / REMOVEXATTR on
 * @c HPC_SHARED_XATTR_NAME.
 *
 * @param cd        Compound context (current_fh selects the inode).
 * @param value     Pointer to value bytes; ignored when @p remove is
 *                  true.  "1", "true", "yes", "on" (case-insensitive)
 *                  set the flag; anything else clears it.
 * @param value_len Length of @p value in bytes.
 * @param remove    True for REMOVEXATTR (always clears).
 *
 * @return NFS4_OK on success; NFS4ERR_PERM if caller is unprivileged;
 *         NFS4ERR_NOFILEHANDLE if no current FH is set; otherwise the
 *         catalogue error mapped to NFS4 status.
 */
enum nfs4_status hpc_shared_xattr_apply(struct compound_data *cd,
                                        const void *value,
                                        uint32_t value_len,
                                        bool remove);

/**
 * Propagate MDS_IFLAG_HPC_SHARED from @p parent_fileid to @p child if
 * the parent has it set and the child does not.  Best-effort; failures
 * are silently swallowed so a transient catalogue error during CREATE
 * never aborts the create itself.  Updates @p child in place AND
 * persists the new flags column via cat_setattr so other MDSes see
 * the bit.
 *
 * Safe to call with NULL @p child.
 */
void hpc_shared_inherit_from_parent(struct compound_data *cd,
                                    uint64_t parent_fileid,
                                    struct mds_inode *child);

/**
 * Synthesize the wire value for GETXATTR(HPC_SHARED_XATTR_NAME): one
 * byte "1" or "0" reflecting the current MDS_IFLAG_HPC_SHARED state
 * of the inode at @p fileid.  Caller passes a buffer of at least 2
 * bytes; the function writes the value byte and a NUL.
 *
 * @return MDS_OK on success, MDS_ERR_NOTFOUND if the inode is gone,
 *         MDS_ERR_INVAL on bad arguments.
 */
enum mds_status hpc_shared_xattr_synthesize_value(struct compound_data *cd,
                                                  uint64_t fileid,
                                                  void *out, uint32_t out_cap,
                                                  uint32_t *out_len);

/**
 * Recover a legacy HPC wide-create inode carrying
 * MDS_IFLAG_HPC_CREATE_PENDING.
 *
 * A complete captured stripe map makes the inode visible.  An incomplete
 * legacy row is removed and its captured DS objects are enqueued for cleanup.
 *
 * @param cd     Compound context containing the catalogue and caches.
 * @param inode  Legacy pending inode; updated if recovery makes it visible.
 * @return MDS_OK when @p inode is visible, MDS_ERR_NOTFOUND when removed, or
 *         a catalogue error.
 *
 * Ownership: caller retains @p inode.
 * Thread safety: the namespace mutation is serialized by the catalogue.
 */
enum mds_status hpc_shared_recover_pending(
    struct compound_data *cd,
    struct mds_inode *inode);

/**
 * Summary of a legacy pending-wide-create recovery scan.
 */
struct hpc_pending_recovery_stats {
    uint64_t promoted;
    uint64_t reaped;
};

/**
 * Sweep the namespace rooted at MDS_FILEID_ROOT for legacy pending rows.
 *
 * This is intended for daemon startup before requests are served. Normal
 * lookup paths also invoke hpc_shared_recover_pending(), so a row created
 * by an older release after the sweep is still repaired lazily.
 *
 * @param cat    Open catalogue.
 * @param stats  Optional recovery counters.
 * @return MDS_OK or the first catalogue error encountered during the scan.
 */
enum mds_status hpc_shared_recover_pending_scan(
    struct mds_catalogue *cat,
    struct hpc_pending_recovery_stats *stats);

/**
 * Override the incomplete-row reap grace window (seconds).
 *
 * Recovery hides -- but does not reap -- an incomplete legacy pending
 * row younger than the grace window, so a rolling upgrade cannot reap
 * an in-flight create still being written by a pre-atomic peer MDS.
 * Production keeps the built-in default; tests that fabricate fresh
 * legacy rows set 0 for immediate reaping.
 *
 * @param grace_sec  New window in seconds (0 = reap immediately).
 */
void hpc_shared_test_set_pending_reap_grace(uint32_t grace_sec);

/* -----------------------------------------------------------------------
 * Phase C / Steps 4 + 5 of docs/hpc-nto1-plan.md -- wide HPC create.
 *
 * Creates a regular file under @p parent_fileid with a fully wired
 * wide stripe map and the @c MDS_IFLAG_HPC_SHARED flag set on the
 * child inode.  Sequencing:
 *
 *   1. Allocate a new fileid via mds_cat_alloc_fileid().
 *   2. ds_prealloc_batch(fileid_hint = child fileid) captures DS file
 *      handles in parallel for stripe_count * mirror_count slots.
 *   3. mds_cat_ns_create_wide() atomically inserts the child inode, parent
 *      dirent, exact stripe map, and parent change/timestamp update.
 *
 * Failure handling:
 *   * Steps 1--2 failure: no namespace metadata was written.
 *   * Step 3 failure: any captured DS files are GC-enqueued; the failed
 *     catalogue transaction leaves no inode, dirent, or stripe-map row.
 *
 * Strict per master plan S5 "all-or-nothing": a successful return
 * means the file exists with a complete wide stripe map; any error
 * leaves no orphan rows or DS files reachable from the namespace.
 *
 * @param cat            Catalogue handle.
 * @param prealloc       Prealloc context (provides catalogue + proxy).
 * @param parent_fileid  Parent directory.
 * @param name           Child name (must be unique under parent).
 * @param mode           File mode bits.
 * @param uid, gid       Synthetic uid/gid.
 * @param stripe_count   Number of stripes (1..MDS_MAX_STRIPES).
 * @param mirror_count   Mirrors per stripe (1..MDS_MAX_MIRRORS).
 * @param stripe_unit    Stripe unit in bytes (0 = default 65536).
 * @param required_transport, preferred_transport, preferred_caps,
 *                       strict_unique_ds   Forwarded to ds_prealloc_batch.
 * @param[out] out       Receives the populated child inode.
 * @return MDS_OK on full success;
 *         MDS_ERR_NOSPC if placement / FH capture exhausted retries;
 *         MDS_ERR_EXISTS if @p name already exists under @p parent_fileid;
 *         MDS_ERR_IO on persistence failure;
 *         MDS_ERR_INVAL on bad arguments;
 *         MDS_ERR_NOMEM on allocation failure.
 */
enum mds_status hpc_shared_create_wide_layout(
    struct mds_catalogue   *cat,
    struct ds_prealloc_ctx *prealloc,
    uint64_t                parent_fileid,
    const char             *name,
    uint32_t                mode,
    uint64_t                uid,
    uint64_t                gid,
    uint32_t                stripe_count,
    uint32_t                mirror_count,
    uint32_t                stripe_unit,
    uint8_t                 required_transport,
    uint8_t                 preferred_transport,
    uint32_t                preferred_caps,
    bool                    strict_unique_ds,
    struct mds_inode       *out);

/* -----------------------------------------------------------------------
 * Phase G of docs/hpc-nto1-plan.md -- client striping hint consumption.
 *
 * The MDS-side wire body for the HPC striping hint is a fixed 16-byte
 * tuple, big-endian on the wire.  All three fields are unsigned:
 *
 *     struct pnfs_hpc_hint {
 *         uint64_t expected_file_size;     // bytes
 *         uint32_t expected_client_count;  // peer cardinality
 *         uint32_t flags;                  // future extension; ignore
 *                                          // unknown bits silently
 *     };
 *
 * The pure helpers in this section stay independent from NFSv4 /
 * compound dispatch; xdr_codec.c unwraps layouthint4 and the
 * OPEN(create) path applies the decoded value to HPC-Shared wide
 * create geometry.  Decoupling the geometry policy from the wire
 * format lets each piece be tested independently and lets Phase J
 * (rank-affinity) extend the hint body without touching placement
 * code.
 *
 * Geometry selection tiers (master plan S5 Phase G):
 *
 *   tier 1: expected_file_size    >= 1 TiB (1ULL << 40)
 *           OR expected_client_count >= 1024
 *           -> stripe across all ONLINE DSes (capped at MDS_MAX_STRIPES)
 *           -> 1 MiB stripe unit
 *
 *   tier 2: expected_file_size    >= 64 GiB (64ULL << 30)
 *           -> stripe across min(64, online_ds_count) DSes
 *           -> 512 KiB stripe unit
 *
 *   tier 0: otherwise
 *           -> no override; caller's defaults stand
 * ----------------------------------------------------------------------- */


/**
 * Decode an XDR-encoded pnfs_hpc_hint body (HPC_HINT_BODY_SIZE bytes,
 * big-endian).  Validates length only -- unknown @c flags bits stay in
 * @p out->flags so callers can introspect them; the geometry helper
 * ignores unknown bits.
 *
 * @return MDS_OK on success, MDS_ERR_INVAL on bad arguments or if
 *         @p buf_len != HPC_HINT_BODY_SIZE.
 */
enum mds_status hpc_hint_decode_xdr_body(const void *buf, uint32_t buf_len,
                                         struct pnfs_hpc_hint *out);

/**
 * Apply the hint to default placement geometry.
 *
 * If the hint qualifies for tier 1 or tier 2 (see header comment),
 * @p stripe_count and @p stripe_unit are overwritten with the
 * tier-derived values.  Otherwise both are left untouched and the
 * caller's defaults stand.
 *
 * Returns false (no override applied) if any of the following hold:
 *   - hint, stripe_count, or stripe_unit is NULL
 *   - online_ds_count == 0 (no DSes to stripe across)
 *   - the hint sits in tier 0 (small file, few clients)
 *
 * @param hint              Hint values (NULL => no override).
 * @param online_ds_count   Number of ONLINE compatible DSes available.
 * @param stripe_count      In/out: caller's default; may be widened.
 * @param stripe_unit       In/out: caller's default; may be replaced.
 * @return true if the hint produced an override, false otherwise.
 */
bool hpc_hint_select_geometry(const struct pnfs_hpc_hint *hint,
                              uint32_t online_ds_count,
                              uint32_t *stripe_count,
                              uint32_t *stripe_unit);

#endif /* HPC_SHARED_H */

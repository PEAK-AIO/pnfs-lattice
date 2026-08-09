# NFSv4.1/4.2 conformance findings — pynfs + NFStest campaign

Status snapshot after the first conformance fix round (PR #90).
Environment: 3-MDS cluster (Release + RonDB backend), 4 generic
NFSv3 data servers, Linux kernel clients (Ubuntu 24.04, NFSv4.2
mounts, `nconnect=8`).  Suites: pynfs NFSv4.1 (kofemann), NFStest
(`nfstest_posix`, `nfstest_pnfs`).

## Scores
- pynfs `--rundeps all` (198 tests): **198/198 pass** with
  `file_delegations_enabled = true`; 187/198 with delegations
  disabled (the 11 failures are exactly the delegation tests, which
  cannot run without grants).
- pynfs delegation batch (`deleg destroy_session backchannel_ctl`,
  20 tests): 19/20 — every delegation test passes (grant, CB_RECALL,
  callback security parms, revocation, self-conflict, cb-getattr);
  the one failure is DSESS9003 (below).
- pynfs pnfs/layout batch (35 tests): 28 pass, 4 skip, 3 fail — all
  three failures are harness artifacts (two tests require a
  BLOCK-layout server; one is a pynfs Python-3 str/bytes bug).
- `nfstest_posix`: **459/461 pass** (4m15s).  The two failures are
  the timestamp items in the open queue below.

## Fixed in PR #90
- EXCHANGE_ID `state_protect4_a` union arms were never consumed by
  the decoder, desyncing the XDR stream after an SP4_SSV request
  (pynfs EID50 saw BADXDR on subsequent ops).  SP4_SSV is now
  rejected up front with `NFS4ERR_ENCR_ALG_UNSUPP` (SSV remains
  unimplemented, matching Linux knfsd).
- DESTROY_SESSION accepted from connections not bound to the session
  (pynfs DSESS9001).  Sessions now track fore-channel bindings
  (RFC 8881 §2.10.3.1); unbound requesters get
  `NFS4ERR_CONN_NOT_BOUND_TO_SESSION`.
- CLOSE after RENAME-over-an-open-file returned `NFS4ERR_STALE`
  (pynfs RNM21).  Rename-overwrite now implements POSIX
  unlink-of-open semantics via `MDS_CAT_RNF_KEEP_DST_ORPHAN` +
  `MDS_IFLAG_UNLINK_ORPHAN`, finalized at the last CLOSE.  The same
  change plugged a DS-object leak: the overwrite path previously did
  no layout recall, GC enqueue, stripe-row cleanup, or quota release.

## Resolved without code change
- "Blocking-lock hang" (nfstest_posix `F_SETLKW`, pynfs CALLBACK1
  symptom): root cause was a **100% full data server**, not the lock
  manager.  A locked read forces a client cache flush; the flush's
  write to the full DS failed ENOSPC forever and the harness retried
  indefinitely.  Two-process blocked-lock repro passes (poll-based
  grants work); after clearing the DS, the exact failing sequence and
  the full `nfstest_posix` suite pass.  ds_gc reclaimed ~94 GB within
  minutes of the namespace deletes.
- pynfs LKPP4 (`--usespecial`): harness artifact — `--maketree` never
  creates the default `tree/special` object, and explicitly passing
  `--usespecial` trips a Python-3 bytes/str bug in the option
  validator.  Server-side behaviour (including EPERM for unprivileged
  mknod) is correct.

## Open defect queue (priority order)
1. DS-capacity-aware placement — the MDS placed new files on a 100%
   full DS.  Generic DSes report no capacity; the placement heuristic
   degrades to uniform and nothing marks a full DS ineligible.
   Needs FSSTAT probing (alongside the existing FSINFO limit prober),
   a placement eligibility gate, and ENOSPC feedback from writes.
   DEFERRED: weighted placement is an enterprise-tier feature; track
   there.
2. DSESS9003 — a pending CB_RECALL is not re-issued over a
   replacement session (trigger callback, leave it unanswered,
   destroy session, create a new session: no retransmit arrives).
3. mtime-on-write not updated (nfstest_posix) — triage which write
   path fails to bump mtime (LAYOUTCOMMIT vs MDS proxy/inline).
4. atime-on-read not updated (nfstest_posix) — expected for DS-direct
   reads (the MDS never sees them); needs a policy decision
   (relatime-style semantics vs LAYOUTSTATS-driven updates).
5. COPY/CLONE re-enable — `op_copy`/`op_clone` deliberately return
   NFS4ERR_NOTSUPP until the proxy copy data-loss bug is fixed
   (pynfs COPY5 stays expected-fail until then).
6. Unlink-orphan sweeper — reclaim `MDS_IFLAG_UNLINK_ORPHAN` inode
   rows leaked by an MDS crash or cross-MDS open between a
   rename-overwrite and the last CLOSE.
7. CB_NOTIFY_LOCK — latency optimisation only; blocked-lock polling
   is proven correct, so clients currently wait up to one poll
   interval (≤30 s) after a conflicting lock clears.

## Operational notes
- Test-state hygiene matters: leaked layout-state rows from
  interrupted stress runs degrade LAYOUTGET conflict scans (see the
  layout-table purge procedure in the ops runbook), and full DS disks
  masquerade as protocol hangs.  Both mimic server defects; check
  `pnfs_mds_rpc_queue_depth` and DS `df` before suspecting the
  protocol path.
- Delegations passed the full conformance matrix; the current
  default-off posture is a measured performance trade, not a
  correctness concern.

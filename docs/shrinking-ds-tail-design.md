# Durable, ordered DS tail discard for shrinking SETATTR(size)

Status: design finalized — the three builder-check questions are answered by
the partner (decisions D5–D7 below); implemented on
`fix/ds-tail-trim-rework` (head `5621508`) and physically validated on a
3-node cluster, 2026-10-26/27 (see the PR description's Physical
validation section).
Supersedes: the inline-truncate approach of `b28e7f9` / `5b53572`
("mds: clear DS tail on shrinking SETATTR(size)"), retracted from PR #110
after external review found two P1 data-safety holes.
Companion: `88da17f` grow-only SIZE_EXTEND write fix — **merged to community
main as PR #110 (merge commit `1e7d1f1`, 2026-10-25)**.

## 0. Background & why

Original bug: a shrinking SETATTR(size) commits the new size to the catalogue
but leaves the per-stripe DS files at their old length; a later grow exposes
the pre-truncate bytes instead of the zeros POSIX requires.

`b28e7f9` fixed this by walking the stripe map inline, after the catalogue
commit, and truncating every DS file to the SETATTR target. External review
of PR #110 rejected the truncate portion:

- **P1-1 (ordering):** the delayed truncate is unconditional w.r.t. the size
  captured at SETATTR commit, with no fence against concurrent in-flight
  proxy WRITEs. Repro: shrink publishes 4 KiB; a concurrent WRITE lands
  8–12 KiB, extends the catalogue to 12 KiB, returns NFS4_OK; the delayed
  truncate cuts the DS to 4 KiB. Both ops returned OK; reads return zeros
  where acknowledged data must remain. Data loss.
- **P1-2 (non-recoverable failure):** a failed discard only logs a warning,
  leaves no durable pending work; retrying the same size never re-runs the
  cleanup (the shrink predicate is false once the catalogue committed); a
  later grow re-exposes the stale tail. A missing DS mount also silently
  returns success (`find_mount` → `continue` in the helper).

Reviewer requirement: per-file ordering covering **both** metadata and DS
mutations; retries must be recoverable and generation-ordered so a retry can
never erase newer writes.

## 1. Decided semantics (partner decisions — do not reopen without asking)

- **D1.** Shrinking SETATTR returns success even though the DS truncate has
  not happened yet. Tail cleanup is **durable background work retried by the
  MDS itself**.
- **D2.** Cleanup runs **only on the file's owner MDS** (namespace is
  partitioned; a file has exactly one owner at any time). **No cross-MDS
  claim/CAS protocol.** After failover or subtree migration, the successor
  owner's drainer picks the pending work up from the shared catalogue.
- **D3.** In-flight **DS-direct (pNFS layout) writes** are protected by
  *graceful layout quiescence* before any truncate (mechanism in §3.4).
  The layout is the only channel by which a client can write a DS file
  directly (non-pNFS clients never receive DS filehandles — verify V1).
- **D4.** Accepted bound: if a layout holder does not release, the cleanup
  waits up to **one lease period (default 90 s, `lease_time_sec`)**; during
  the whole quiescence window, **new LAYOUTGETs on the file are blocked**.
  Both limits were explicitly accepted by the partner.
- **D5. Phased handling of `transient_state_cache` mode** (builder check,
  Q1). In transient mode layout grants are **not persisted** to
  `layout_state`, so the §3.4 quiescence fence would be vacuous — a trim
  with a vacuous fence in the lab/install default configuration would
  reintroduce the P1-1 class hole. Decision: **phase 1 (this work) enqueues
  nothing when the MDS runs with transient state cache** — current
  behaviour is preserved there (the stale-tail bug remains, documented as a
  known limitation in §17 and the PR description). The proxy-path fence
  (§3.3) works in both modes. **Phase 2 (out of scope):** a per-fileid
  in-memory holder registry (hooks in LAYOUTGET/LAYOUTRETURN/LAYOUTCOMMIT)
  that makes the fence effective in transient mode.
- **D6. Globally-visible TRIM rows** (builder check, Q2). Backend peeks
  (RonDB batch path, FDB/DAOS `GC_BY_OWNER` index) only see rows whose
  `owner_mds_id` is 0 or self, and `gc_enqueue` hard-stamps the enqueuer —
  a successor owner after migration or id-changing failover could never
  peek the row, breaking D2/I2 hand-off. Decision: **TRIM rows are
  enqueued globally visible (`owner_mds_id = 0`)**, requiring an
  owner-overridable enqueue path in all four backends (the column already
  exists — additive parameter, no schema change). Processing stays gated by
  the drainer-level ownership guard (§3.5).
- **D7. DS-reachability deployment invariant** (builder check, Q3). The
  **owner MDS can always reach every DS that backs a file it owns** (proxy
  I/O already requires this: a stripe on an unreachable DS makes the file
  unusable from that MDS). A missing DS mount is therefore a **transient
  failure** — the entry stays queued and retries until the mount returns.

## 2. Vehicle: reuse the existing durable GC queue

The community edition already has durable DS-side pending work:

- `gc_queue` rows in **all four backends** — slots `gc_enqueue` /
  `gc_peek` / `gc_peek_batch` / `gc_dequeue` / `gc_count`
  (`include/catalogue_internal.h` ~436–460; `mds_cat_gc_*` wrappers in
  `include/mds_catalogue.h`).
- The `ds_gc` drainer (`src/modules/ds_gc/ds_gc.c`, `ENABLE_DS_GC` default
  ON): coordinator thread batch-peeks, bounded work queue, N workers;
  today's workers do path-based unlinks on DS mounts then dequeue.
  Metrics via `mds_metrics.h` (`gc_pending` gauge).
- Entry shape `struct mds_gc_entry` (`include/pnfs_mds.h:398`):
  `gc_seq`, `fileid`, `ds_id`, `nfs_fh`, `owner_mds_id` (enqueuer, 0 =
  legacy), `sweep_hint` (flag-bit encoded: `MDS_GC_SWEEP_GEOM(sc,mc)`
  whole-file, `MDS_GC_SWEEP_SLOT(s,m)` single slot with bit 31 set;
  `include/pnfs_mds.h:363–375`).

Decisions for this work (builder's calls where marked):

- **A1.** Add one new sweep mode, `MDS_GC_SWEEP_TRIM`, in the existing
  flag-bit encoding of `sweep_hint`. The entry's `ds_id` / `nfs_fh` are
  unused for TRIM (builder: decide whether to zero or store the primary
  stripe's values — cosmetic).
- **A2.** **No new catalogue entity, no row schema change.** In particular
  the entry stores **no target size** — see rule R below.
- **A3.** The drainer dispatches on `sweep_hint`: GEOM/SLOT keep today's
  unlink path; TRIM goes to a new trim path (§3).
- **A4. (D6) Owner-overridable enqueue.** Add a `uint32_t owner_id`
  parameter to the `gc_enqueue` slot / `mds_cat_gc_enqueue` wrapper with a
  sentinel `MDS_GC_OWNER_GLOBAL = 0` (globally visible). Callers that pass
  the enqueuer's id (or the default) keep today's behaviour for GEOM/SLOT;
  TRIM enqueues with the global sentinel. Note `mds_id` defaults to 1, so
  per-MDS peek isolation is on by default — the override is TRIM-only.

## 3. The design

### 3.1 Enqueue (same transaction as the size commit)

In the shrinking-SETATTR path (`src/mds/compound_namespace.c`, the block at
~1226–1259 that today does the inline truncate):

- **Mode gate (D5):** if the MDS runs with `transient_state_cache` (the
  same gate that skips `layout_state` persistence,
  `cd->skip_transient_ndb`), **enqueue nothing** — the shrink path ends
  after the size commit + notification recall and current (pre-fix)
  behaviour is preserved. No quiescence state is created in this mode in
  phase 1; the known limitation is documented in §17 and the PR
  description.
- Otherwise, enqueue a `MDS_GC_SWEEP_TRIM` entry for the fileid **inside
  the same catalogue transaction** as the size setattr, with
  `owner_mds_id = 0` (globally visible, D6/A4). Same transaction = the
  reviewer's "same ordering domain": the pending DS work and the metadata
  fact are committed atomically, or not at all.
- **Remove** the inline best-effort truncate block entirely. The
  synchronous SETATTR path no longer touches DS mounts at all (this also
  addresses the reviewer's perf note: no stripe-map lookup and no
  sequential DS truncates inside SETATTR).
- Keep the existing best-effort whole-file `CB_LAYOUTRECALL` after the
  size commit (`compound_namespace.c:1257`) as a fast-path notification so
  holders learn the new size early. It carries **no safety role**; safety
  comes from §3.4.
- Builder (V4): confirm/add a *transactional* enqueue per backend.
  Precedent for folding GC rows into a mutation transaction:
  `ns_remove_known_gc` / `gc_folded` (`include/catalogue_internal.h:202`).
  Fallback if a backend cannot fold: enqueue **before** the size commit in
  its own transaction — safe by the argument that if the size commit is
  lost, the trim is a no-op (§3.2 rule R on the old size). Flag any backend
  where even the fallback is unavailable.

### 3.2 Rule R (load-bearing invariant)

> A TRIM entry for fileid F is processed by F's **owner** MDS. In the
> per-inode lock L(F) critical section, the worker reads F's **current
> committed** size S from the **authoritative** catalogue (never a hot-read
> image — §3.6) and truncates **every** stripe/mirror DS file to S.

Why no stored target: the correct target at processing time is by definition
the size committed *by then*. A grow that superseded the shrink raises S, so
the truncate preserves the new data; a second shrink lowers S, so the newer
target wins. `truncate(2)` to a length ≥ current file length is a no-op, so
S > DS length (sparse file) is harmless. The truncate stays **unconditional**
(no "already short enough" stat guard): the MDS's NFSv3 attribute cache can
lag DS-direct writes — the original b28e7f9 rationale, still valid.
`ENOENT` = stripe file never created = skip that stripe — interpreted only
under a healthy mount: the drainer resolves `{mount}/data` first, so a
detached mount (where nothing beneath the mountpoint resolves, data dir
and slot alike) is an IO error that keeps the entry queued, never a
silent skip (refined after physical validation found the detached-mount
case; `5621508`).

**I1 — no acknowledged data is ever erased.** Acknowledged data means a
range whose commit has advanced the catalogue size past its end: proxy
WRITE (DS write + SIZE_EXTEND commit, both under L(F) — see `88da17f`) or a
pNFS LAYOUTCOMMIT applied at the MDS. Both are fenced against the trim's
[S-read … truncate] window by L(F) and the quiescence-completion condition
(§3.4), so S at read time already covers the range; truncate-to-S preserves
it. Data beyond the committed S that gets erased was never committed —
protocol-unacknowledged (same durability class as a client crash mid-
LAYOUTCOMMIT), and the catalogue never claims a size covering it.

**I2 — no stale tail survives without durable pending work.** The entry is
committed atomically with the shrink; every failure (EIO, missing mount,
DS down) leaves the entry queued; the owner's drainer — or the successor
owner's after failover/migration (reachable via D6 global visibility) —
retries until success. **A missing DS mount is a failure, never a silent
success** (D7: transient by the reachability invariant). In transient mode
phase 1 no pending work is created at all (D5); I2 applies to persistent
mode, where the stale-tail limitation is documented rather than fixed.

**I3 — idempotent under duplicate/stale processing.** All MDS drainers peek
the same shared queue in RonDB mode; only the owner processes (ownership
guard, §3.5); a no-op truncate (S ≥ DS length) is harmless even if two
owners ever processed; reprocessing after a grow is a no-op.

### 3.3 Per-inode lock L(F)

- New striped per-inode mutex table (follow the existing 16-stripe
  table patterns in the MDS).
- **Proxy write path** (`op_write` in `src/mds/compound_data_io.c` /
  `src/mds/proxy_io.c`): hold L(F) across the DS WRITE **and** the
  subsequent SIZE_EXTEND commit. Hot-path cost is one uncontended mutex
  pair; contention exists only while a trim is processing that file.
- **Trim worker**: hold L(F) across the critical section = [re-read S …
  truncate loop] only — **not** across the quiescence wait (§3.4); holding
  it across a 90 s wait would stall legitimate proxy writes into client
  timeout territory.
- **LAYOUTCOMMIT** (direct clients): hold L(F) around its size commit, so
  the same mutual exclusion fences it against the critical section.
  (Verify against the LAYOUTCOMMIT aggregator's coalesced commit path,
  `src/mds/layout_commit_aggregator.c` — the lock must bracket the point
  where the coalesced cat_setattr actually applies.)

### 3.4 Graceful layout quiescence (new; the P1-1 fence for direct writes)

**Precondition (D5):** the TRIM path only runs in persistent layout-state
mode; in transient mode no TRIM entry is ever enqueued (§3.1), so this
mechanism is never invoked there in phase 1.

Today's `layout_recall_for_file()` (`src/mds/layout_recall.c`) is
best-effort: it sends `CB_LAYOUTRECALL` then **authoritatively deletes**
the layout rows regardless of client ack (header admits "Client ack
tracking and timeout/retry are still TODO"). That is acceptable for unlink
(the file is gone) but not for trim (an un-acknowledged holder's
uncommitted window could be under our truncate). Add a **graceful variant**
used by the TRIM path:

1. **Set a per-file "quiescing" flag** (in-memory, owner MDS; a small
   striped table or entry in an existing per-file state table — builder's
   call). While set, `op_layoutget` for this fileid returns
   `NFS4ERR_DELAY` (or waits briefly and returns DELAY — builder's call);
   no new holder can appear.
2. **Recall** all active holders (reuse the existing enumeration of
   `layout_state` rows for the fileid).
3. **Wait** until no active layout row remains for the fileid — i.e. every
   holder has `LAYOUTRETURN`ed (the MDS observes returns; per RFC 8028 a
   recalled client LAYOUTCOMMITs its pending window before returning, which
   advances S and is fenced by L(F)/§3.3). Bound the wait by one lease
   period (D4); for a holder that never returns, reuse the existing
   lease-expiry path to authoritatively expire its layout (V6).
4. **Quiescence-complete condition** (all of, checked under L(F) at the
   start of the critical section):
   (a) quiescing flag set,
   (b) no active `layout_state` rows for the fileid,
   (c) no pending LAYOUTCOMMIT work for the fileid — the aggregator bucket
   for the fileid is dropped/flushed (the shrink path already calls
   `layout_commit_aggregator_drop()` — confirm drop semantics, V8; if drop
   *discards* unapplied commits, the client re-sends and gets
   `BAD_STATEID` on the returned layout: that data was uncommitted, i.e.
   outside I1's protection class, and S never claimed it).
5. **Critical section under L(F)**: re-read S, truncate every stripe/mirror
   to S, clear the flag, **dequeue the entry only on full success**.
   Any failure (EIO, missing mount, recall enumeration error) → leave the
   flag set (keeps LAYOUTGET blocked), leave the entry queued; the drainer
   re-peeks it and the whole quiescence+trim runs again from step 1.
6. **Time bound (my call):** the quiescing flag is bounded by
   `trim_max_wait` (new config key, default 3 × `lease_time_sec`), judged
   on the entry's queue age. A worker re-peeking an entry past the bound
   clears the flag, leaves the entry queued, and backs off before the next
   attempt — LAYOUTGET is blocked for at most one wait (≤ 1 lease, D4) plus
   the poll interval, even when the underlying DS fault is long-lived.
   Document the key in `docs/config-keys.md` and `docs/man/mds.conf.5`.

### 3.5 Ownership guard

TRIM rows are globally visible (D6): **every** MDS drainer peeks them.
Before processing, the drainer checks that its MDS is the current
**owner** of the fileid (builder check: the lookup is
`mds_cat_shard_fileid_get`). Non-owner → **leave the entry queued**, do
not dequeue (the successor owner's drainer will pick it up after
failover/migration — now actually reachable, unlike self-stamped rows).
Builder (V2): confirm subtree migration quiesces in-flight writes for
migrating files, since L(F) is per-MDS in memory.

### 3.6 Authoritative size read

If `catalog_image_mode` (hot-read image, changefeed-poll based, lags by
construction) is enabled, the worker's S-read **must bypass the image** and
read the authoritative store (or force a compare/refresh for the inode).
A stale (too-small) image size would truncate past committed data — an I1
violation. Builder: identify the authoritative-read API used by the
mutation paths.

### 3.7 Observability

- Reuse `gc_pending`; add trim-specific counters: `trim_pending`,
  `trim_completed`, `trim_failed`, `trim_recall_wait_ns` (histogram or
  gauge — follow existing `mds_metrics` conventions).
- WARN log when a TRIM entry's queue age exceeds one lease period (the
  stuck-holder case operators need to see).

## 4. Non-goals / anti-goals

Non-goals (out of scope, separate work):

- Changes to the existing unlink GC (GEOM/SLOT) paths.
- A cross-MDS claim/lease protocol for the GC queue.
- Any new catalogue entity, table, or row schema (A2).
- Changes to LAYOUTCOMMIT aggregation design (only locking around its
  commit point, §3.3).
- Fixing the MDS-side NFSv3 attribute cache staleness (we keep
  unconditional truncates instead).
- Client-visible shrink behavior: unchanged (NFS4_OK, same best-effort
  notification recall).

Anti-goals (this design exists to make these impossible):

- Any inline/synchronous DS truncate on the SETATTR path (the retracted
  design).
- Any silent success on DS failure (EIO, missing mount).
- Truncating to a size captured at enqueue time rather than at processing
  time.

## 5. Constraints

- C11, Linux kernel style: hard tabs, K&R with function definitions on the
  next line, ≤ 100 cols, no `//` comments in `.c`, declarations at block
  top, `static` by default, `goto cleanup` on multi-resource error paths,
  every allocation checked, free-then-NULL. PeakAIO copyright header on
  every touched file.
- `src/catalogue/catalogue_rondb_shim.cpp` is the only C++/GPL file —
  no NDB headers elsewhere.
- Unit tests are plain C binaries (no CMocka); the gate is
  `ctest --test-dir build -L unit --output-on-failure`;
  `scripts/check-style.sh` must pass; keep `docs/architecture.md` §17 in
  sync.
- No row schema change (A2). The `gc_enqueue` slot gains the owner
  parameter (A4), so the gc_enqueue conformance fixtures change on all
  four backends — expected and limited; row formats are untouched.
  Local gate runs memdb conformance; FDB/DAOS self-skip (exit 77) locally
  and run in CI.
- Commits: `mds: imperative summary` (< 72 chars), one logical change,
  each commit compiles and passes tests. Author `changfuwu
  <changfu.wu@lenovonetapp.com>`; no Co-Authored-By or other trailers.
- Submission path: this tree's identity (`changfuwu`) has **no write
  access to PEAK-AIO/pnfs-lattice**; work goes through the
  `changfuwu/pnfs-lattice` fork + cross-repo PR (precedent: PR #110).
  Sequence after PR #110 merges.

## 6. Open verifications for the builder (assumptions to prove in code)

- **V1.** The layout is the only DS-direct write channel: a WRITE
  delegation does not expose DS filehandles to non-pNFS clients.
- **V2.** Subtree migration quiesces in-flight writes for the migrating
  file (so the successor owner's trim cannot race the predecessor's
  in-flight proxy writes).
- **V3.** ~~`MDS_GC_SWEEP_TRIM` fits the 32-bit `sweep_hint`~~ — resolved
  by the builder check: `MDS_MAX_STRIPES = 1024` (2^10), so GEOM's sc/mc
  fit in 20 bits and a TRIM sentinel bit exists in `sweep_hint`.
- **V4.** Transactional GC enqueue availability per backend (fold
  precedent: `ns_remove_known_gc`/`gc_folded`); fallback ordering argument
  in §3.1; plus the A4 owner parameter on the same slot.
- **V5.** ~~The ownership-lookup API~~ — resolved by the builder check:
  `mds_cat_shard_fileid_get`.
- **V6.** The existing lease-expiry path is reusable for authoritatively
  expiring a non-responding holder during quiescence; the ds_gc worker
  thread may poll/wait (it already polls — `DS_GC_MIN_POLL_MS`).
- **V7.** Re-stated after the builder check (original premise was wrong):
  peeks are per-MDS isolated by design (RonDB batch path skips rows whose
  `owner_mds_id` is neither 0 nor self; FDB/DAOS index the same two
  prefixes; `mds_id` defaults to 1 so the filter is on by default). With
  A4's global enqueue (`owner_mds_id = 0`), verify that the RonDB shim
  batch path and the FDB/DAOS `GC_BY_OWNER` index both treat 0 as visible
  to every MDS, and that self-stamped GEOM/SLOT rows are unaffected.
- **V8.** `layout_commit_aggregator_drop()` semantics: apply-pending vs
  discard-pending; quiescence-complete condition (c) in §3.4 step 4 depends
  on it.
- **V9.** The authoritative (image-bypassing) inode read API for §3.6.

### 6a. Builder-check resolutions (my calls — check these while reading)

- **Base branch:** implement on top of local `main` (currently
  `5b53572`, the recreated truncate commit sitting on merged PR #110).
  **Commit 1 removes the inline truncate block** from the shrink path,
  replacing the old approach in place — the branch is then correct under
  either future (whether `5b53572` ever lands upstream as-is).
- **Vanished file at trim time:** the stripe-map read reports NOTFOUND for
  a file with no DS backing (or the inode is gone after final unlink) ⇒
  **dequeue as success** — nothing to clear (the original b28e7f9
  NOTFOUND-tolerant semantics, now with a live check).
- **Drainer NOTFOUND-drop exception:** the existing drainer drop-on-
  NOTFOUND behaviour (tuned for unlink entries) needs a TRIM exception in
  the A3 dispatch, per the previous item.
- **LAYOUTCOMMIT lock bracket in this tree:** the aggregator is a no-op
  stub here; the synchronous commit point
  (`compound_layout.c` ~3426–3429) is where L(F) brackets the size commit.
  The enterprise coalesced path is covered by the API-level wording in
  §3.3.
- **`trim_max_wait`** default 3 × `lease_time_sec` (§3.4 step 6).

## 7. Test plan (memdb + local DS fixtures, same standard as the reviewer's)

- **T1 (P1-1 proxy fence, both orderings):** shrink to 4 KiB with the trim
  in flight; interleave `op_write` at 8–12 KiB (a) committing before the
  worker's S-read and (b) after it. Written bytes must survive; reads
  return them, never zeros.
- **T2 (P1-2 recoverable failure):** inject EIO on one stripe's truncate →
  entry stays queued, SETATTR still succeeded; clear the fault → drainer
  retry completes; a later grow returns zeros.
- **T3 (superseding grow):** shrink, then grow past the old tail before the
  worker runs → trim is a no-op, new data intact.
- **T4 (double shrink):** shrink to 4 KiB then 2 KiB → one final DS state
  at 2 KiB; duplicate/stale entries drain idempotently.
- **T5 (direct-write quiescence; persistent mode only, phase 1):** a
  layout holder is recalled, commits a window extending S, returns →
  preserved; a holder committing within the target → trimmed correctly; a
  non-responding holder → lease-bound expiry path, no hang.
- **T6 (missing mount):** a DS mount missing → entry stays queued (no
  silent OK), SETATTR unaffected, warning logged.
- **T7 (global visibility + ownership guard):** a non-owner MDS's drainer
  peeks the (`owner_mds_id = 0`) entry and leaves it queued; the owner's
  drainer drains it; after a simulated ownership transfer (migration /
  id-changing failover), the **successor** owner's drainer drains it.
- **T8 (stale image):** with a lagging hot-read image, the trim reads the
  authoritative size.
- **T9 (transient-mode gate, D5):** with `transient_state_cache` on, a
  shrinking SETATTR enqueues no TRIM entry, performs no quiescence, and
  still returns NFS4_OK; the stale-tail behaviour is unchanged (documented
  limitation).
- Conformance suite stays green on all compiled-in backends (including the
  A4 owner-parameter change to the gc_enqueue fixtures); unit gate green;
  style gate green.

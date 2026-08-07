# Performance wave 7 — mutation-path architecture (design)

This is a design document, not an implementation change.  It answers
the questions a mutation-rate redesign has to answer before any code
is written: why one metadata commit costs what it costs, which of the
four candidate strategies (cheaper commits, overlapped commits,
avoided commits, more MDS nodes) are actually available to this
architecture, what each one trades away, and what has to be measured
before adopting any of them.  Every number in this file is either
cited from an existing lab report or explicitly marked **pending**
with the procedure that will produce it.  No production code, config
default, or test changes accompany this document.

## Problem statement

Creates and removes are one to two orders of magnitude slower than
reads and stats on the same deployment, and none of the MDS-local
optimisation waves moved them, because the mutation path is not
MDS-local-bound.

Measured on the prior lab profile (3 MDS, 2 RonDB data nodes,
16-rank mdtest, `-F -z 0 -n 200 -i 1`):

- 1-DIR create: 2,883/s, `ns_create` 0.78 ms = 99.44% of the op
- 16-DIR create: 2,048/s, `ns_create` 1.68 ms
- End-to-end cold-cache: create 408/s, remove 313.6/s, tree create
  76.4/s — against stat 77,326/s and read 18,553/s
- Client→MDS RTT is tens of µs and explicitly not the cap

A `perf` profile of the serving MDS puts essentially all mutation
time under the NDB transport send path
(`NdbTransaction::executeNoBlobs` → `TCP_Transporter::doSend` →
`writev`): the MDS is waiting on remote commits, not computing.

The round-trip *count* is already minimal: the create path is a
single fused NDB transaction (`rondb_shim_ns_create_with_layout`,
`src/catalogue/catalogue_rondb_shim.cpp`) writing the dirent, the
child inode, an interpreted parent update (atomic nlink/mtime/change
bump), the stripe rows (or the v9 inline-stripe inode fields), and
the layout pre-grant, all hinted onto the parent's partition and
committed once.  The problem is that this one commit costs ~0.78 ms
and is serialised per operation per worker thread.

That reframes the design space into four strategies:

- **A** — make the commit cheaper (configuration and topology);
- **B** — overlap commits so latency stops bounding throughput;
- **C** — avoid commits (client caching / deferred durability);
- **D** — add MDS nodes (scale out).

They are not mutually exclusive.  The recommendation at the end
names a combination, with each component gated on a measurement.

## Question 0 — why does one commit cost 0.78 ms?

Published NDB commit latencies on dedicated clusters are in the low
hundreds of microseconds, so 0.78 ms is high and must be decomposed
before any redesign.  What is already known:

- **API→TC network hop.**  The colocation benchmark measured the
  API→TC round-trip dropping from ~1 ms to ~0.05 ms when a data node
  runs on the MDS host, worth +25% creates on that (older, pre-fusion)
  build.
- **Replica acknowledgment.**  `NoOfReplicas=1` was worth a further
  +75% creates on the same build — a durability trade, not a free
  win (see Axis A).
- **Row count.**  The fused transaction writes 4–8+ rows across the
  parent partition (dirent, parent inode) and the child partition
  (child inode, stripe map/entries or inline fields, layout rows).
  Whether row count or round-trip dominates is unmeasured.
- **TC batching.**  16 writers against ONE parent partition
  outperformed 16 writers against 16 partitions (2,883/s vs
  2,048/s): NDB's transaction coordinator batches same-partition
  commits into fewer redo writes.  Commit cost is therefore not a
  constant; it improves under exactly the contention that a
  row-lock analysis would predict should hurt.

**Pending — decomposition procedure.**  On the lab, holding the
workload fixed (16-rank mdtest per the harness section):

1. Record baseline per-commit latency from the existing `ns_create`
   phase timer.
2. Sweep one variable at a time and re-measure: data node on/off the
   MDS host (A1); `NoOfReplicas` 2 → 1 (A2); redo/durability
   settings (`ndb_default_operation_redo_problem_action`, LCP/redo
   parameters) at fixed topology; transaction row count by
   benchmarking `ns_create` with and without the layout pre-grant
   arm and with the v9 inline-stripe path on/off.
3. Attribute the residue.  ndbinfo exposes per-phase counters
   (`ndbinfo.transactions`, `threadstat`, `transporters`) that
   separate TC queueing from LDM execution from transport; record
   them before/after each run.

Decision gate: if configuration alone (A1+A2 on a shippable
topology) recovers the commit to ≤0.2 ms, Axis B arithmetic changes
by 4× and parts of this design become unnecessary.  Run Q0 first.

## Axis A — make the commit cheaper

**A1 — colocation.**  Run RonDB data nodes on the MDS hosts so the
TC is local.  Existing evidence: +25% creates, API→TC ~1 ms →
~0.05 ms.  Costs to state in the result record: MDS and DB failure
domains couple (an MDS host loss now also removes a data node);
capacity planning changes; RonDB's DataMemory competes with the MDS
for RAM.  **Pending:** re-measure on the current fused-create build
— the +25% figure predates the single-commit create.

**A2 — replica and durability settings.**  `NoOfReplicas=1` was
+75% creates on the older build.  The durability statement, written
down: with one replica, a data-node loss makes its node group —
i.e. the whole cluster — unavailable until recovery from redo log +
local checkpoint completes, and any transactions in the redo window
not yet flushed are lost.  Metadata for files whose data lives on
DSes would be reconstructible only by external audit.  This is not
a shippable default; it is a measured option for deployments that
accept metadata-recovery RTO in exchange for latency.  **Pending:**
quantify redo-log durability settings separately from replica count
so the recommendation can price each independently.

**A3 — transport.**  NDB supports shared-memory transporters for
colocated API↔data-node pairs (a colocation follow-on, free of
extra durability cost) and the lab's fabric may support kernel
bypass.  **Pending:** measure SHM transporter vs TCP loopback under
A1 before considering anything more exotic.

**A4 — transaction shape.**  If Q0 attributes material cost to row
count, two shapes are testable without schema work: create WITHOUT
the layout pre-grant arm (LAYOUTGET then pays its own commit — a
regression for the fused CREATE+LAYOUTGET compound, so only worth it
if the pre-grant rows dominate) and the v9 inline-stripe path
(already merged; stripe rows fold into the inode row).  A third —
moving `layout_state` onto the namespace partition key so the fused
commit never crosses partitions — is a one-shot schema rework known
from the earlier fusion attempt (the cross-partition
NoCommit+Commit variant regressed 36 vs 73 ops/s on the older
build) and should be priced only if Q0 shows cross-partition commit
overhead.

## Axis B — overlap commits

This axis raises *throughput* only.  A single client thread issuing
dependent operations still observes the full per-commit latency;
nothing here changes that.  The design target asks for both rates
and latency; B serves rates.

**Correction to the wave brief: the pipeline is not a dormant
scaffold.**  As of the Phase 4 work, `ndb_async_writes = true`
routes both `ns_create` and the fused create+layout through the
async batch pipeline (`rondb_shim_ns_create` and
`rondb_shim_ns_create_with_layout` both take the
`rondb_async_exec()` branch; the gate is propagated at catalogue
open, `src/catalogue/catalogue_rondb.c`).  Per-connection flush
threads exist only when the flag is set, and each worker still
blocks until its own transaction's completion callback fires
(`rondb_async_exec`, `src/catalogue/catalogue_rondb_shim.cpp`): the
pipeline batches many workers' `sendPreparedTransactions()` +
`pollNdb()` into shared cycles, and changes nothing else.

**B3 — error and ordering semantics: answered at the desk.**  The
as-built pipeline is NFSv4-invisible by construction:

- *Per-request blocking is preserved.*  The worker that prepared the
  transaction sleeps on its own condvar until its callback delivers
  the result; the COMPOUND reply is not sent before its commit is
  durable.  Session reply-cache and COMPOUND ordering semantics are
  untouched because nothing about request processing became
  asynchronous — only the NDB network waits of *different* requests
  overlap.
- *Error mapping is identical.*  The commit result flows through the
  same NdbError classification (EXISTS on constraint violation,
  retryable on temporary, error report otherwise) in both modes.
- *Change-attribute monotonicity holds.*  The parent's change bump
  is an interpreted update inside the same transaction; NDB's row
  lock serialises concurrent parents cluster-wide, so observers can
  never see change values regress regardless of completion order
  between unrelated transactions.

**B2 — the concurrency ceiling, first finding: `Ndb::init()`.**
Both Ndb-object creation sites call `init()` with the default
argument, and the NDB API default is `maxNoOfTransactions = 4`
(`Ndb.hpp`).  The async pipeline's shared per-connection Ndb can
therefore hold at most 4 concurrent transactions, and with the
default `ndb_conn_pool_size = 2` the whole MDS caps at **8
in-flight commits** — the 5th concurrent `startTransaction()` on a
connection fails and the create errors.  Any B1 measurement run
without raising this limit measures the cap, not the pipeline.
The remaining candidate ceilings, in the order they will bind:

1. `Ndb::init(N)` per shared Ndb (currently 4);
2. worker threads — each in-flight commit pins one blocked worker,
   so depth ≤ `worker_threads` regardless of NDB limits;
3. client-side concurrency — `nconnect` × session slot table bounds
   how many COMPOUNDs are even outstanding;
4. the flush thread's `pollNdb(1, 1)` loop and the RonDB `[api]`
   slot budget (`num_MDS × pool`) — the same budget constraint
   recorded against the pool-sizing question in wave 6.

**B1 — prototype procedure (pending).**  On the lab, with
`Ndb::init(1024)` at both sites (a two-line lab patch, not shipped):
sweep `ndb_async_writes` off/on × `ndb_conn_pool_size` {2, 4, 8} ×
worker threads {16, 32, 64} × mdtest ranks {16, 32, 64}, recording
creates/s, `ns_create` p50/p99, and worker occupancy.  The
arithmetic to confirm or refute: at 0.78 ms per commit and N
overlapped commits, creates/s approaches N / 0.78 ms — N=8 is
~10k/s, N=64 is ~80k/s, but only if the TC continues to batch and
the redo log absorbs the aggregate write rate; expect the knee well
before the arithmetic bound.  Record where it lands and which
ceiling (1–4 above) binds first.

## Axis C — avoid the commit

**C1 — why delegations are disabled: answered, and it is not a
correctness defect.**  The tuning scripts disable file delegations
because of a *measured performance regression on create-heavy
workloads*: with delegations on, single-client mdtest saw ~83% of
grants returned via DELEGRETURN at ~4 ms per return, because the
kernel client evicted inodes from its dcache faster than it could
reuse the delegations — every grant became pure overhead plus an
NDB `deleg_del` on the close path.  The scripts' own comment says
read-heavy / repeated-open workloads should flip it back on.  In
lattice, `file_delegations_enabled = false` (the default) means the
delegation table is never wired and OPEN never grants
(`src/mds/main.c`).  Two consequences for this design: there is no
hidden correctness prerequisite blocking Axis C, and file
delegations are the wrong tool for mutation anyway — they cache
per-file open/attr state, not namespace mutation authority.

**C2 — directory delegations: what the protocol actually permits.**
The Lustre model — clients batch namespace mutations locally under
a distributed lock and write them back — has **no NFSv4.1
analogue**.  GET_DIR_DELEGATION (RFC 8881 §18.39) delegates
*read* authority over a directory: a client holding one may cache
directory contents and attributes and trust CB_NOTIFY (or a recall)
to invalidate them.  Every CREATE, REMOVE, LINK and RENAME still
travels to the server and commits there.  There is no write-back
namespace mutation in the protocol, so Axis C cannot remove the
commit from the create path; the realistic gains are ancillary:

- eliminating the client's revalidation traffic around mutation
  bursts (LOOKUP/GETATTR re-reads of the parent it just wrote to —
  visible in mdtest stat-phase behaviour and in `ls` after create);
- letting *other* clients keep their cached view of a hot directory
  without re-reading it after every peer mutation.

The machinery exists behind `dir_delegations_enabled = false`: the
table, grant/recall/notify paths and session wiring
(`src/mds/dir_delegation.c`, wired in `src/mds/main.c`), and the
GDD result encoding fixed during the test-suite work.  The costs to
measure before enabling: CB_NOTIFY fan-out is per-mutation work
proportional to the number of delegation holders on the parent
(under a multi-client create storm into one directory this is new
per-create overhead, the exact opposite of the intent), and recall
storms when writers collide with holders.  **Pending:** measure a
multi-client mdtest with `dir_delegations_enabled` on vs off, plus
a `ls -l`-after-create probe, before drawing conclusions.  The C1
history is a warning that delegation machinery can be net-negative
on precisely the workloads this wave targets.

**C3 — deferred-durability paths already in-tree (both default
off).**

- `parent_touch_deferred` — batches parent-directory
  mtime/change/nlink updates in an in-memory aggregator flushed
  every `parent_touch_flush_ms`, removing the interpreted parent
  update from the create/remove transaction.  Backend-gated
  (requires `ns_parent_touch`); falls back to synchronous when
  unsupported (`src/mds/main.c`).  The prior profile is a caution:
  parent-row serialisation was *not* the wall at 16 ranks (TC
  batching made the shared parent the FASTER case), so this knob's
  value is workload-shaped — many-clients-one-directory patterns,
  not spread mdtest.
- `remove_async` — the delete-at-ack manifest
  (`src/mds/remove_manifest.c`): op_remove's fast path durably
  commits the dirent delete plus one `mds_remove_pending` row,
  inserts an in-memory tombstone, and acks; drainer workers execute
  the guarded `ns_remove` and final-unlink cleanup (stripe/layout
  rows, GC, DS fencing) off the request thread, with crash re-claim
  via the ownership lease.  Requires the parent_touch aggregator
  (REMOVE change_info is served from it).  Note what this does and
  does not defer: the *name removal* is still one durable NDB
  commit at ack time — DB-authoritative on every MDS — so REMOVE
  latency improves by shedding cleanup, not by skipping the commit.

**Pending:** measure each knob's create/remove delta on the lab
(mdtest 16-rank, plus an 8-rank shared-parent variant where
parent_touch should show), recording the crash-consistency budget
below alongside.

**C4 — crash-consistency budget.**  What an MDS crash loses under
each option, stated per option:

| Option | Lost on MDS crash | Change-attr / COMMIT semantics |
|---|---|---|
| Async pipeline (B) | Nothing — callers block until commit | Unchanged (per-request durability identical to sync) |
| `remove_async` | Cleanup progress only; re-claimed via lease after restart. Names already durably removed at ack | Unchanged for visibility; change_info served from aggregator (see below) |
| `parent_touch_deferred` | Up to `parent_touch_flush_ms` of parent mtime/change/nlink updates | Parent change attr can REGRESS across a crash: clients that read an aggregated (unflushed) change value can see a lower value after restart. Violates the advertised `NFS4_CHANGE_TYPE_IS_MONOTONIC_INCR` unless the flushed value is made crash-monotonic (e.g. epoch-salted high bits or flush-before-reply for readers) — this is the one open correctness question on Axis C and must be resolved in design before the knob is recommended anywhere |
| `NoOfReplicas=1` (A2) | Cluster unavailable until node recovery; redo-window transactions lost on media loss | Unchanged while up; recovery gap is an availability/durability trade, not a semantics change |

File data is never at stake in any option — data lives on DSes and
NFSv4 COMMIT semantics bind clients to DS write durability, not MDS
metadata timing.

## Axis D — scale out

**Serialisation inventory — what still crosses MDS nodes.**  The
Phase-9B application-level lock manager is already gone for
everything except cross-directory RENAME: the current authority
vtable documents that NDB row locks serialise concurrent writers
from multiple MDS daemons (dirent insertTuple is the uniqueness
gate; interpreted parent updates are atomic across API nodes), that
the app-lock wrapper added 3–5 extra round-trips per mutation and
livelocked at 16-way, and that only `ns_rename`'s multi-read
prologue still needs the TOPOLOGY lock
(`src/catalogue/catalogue_rondb.c`, `rondb_locked_authority_ops`).
What remains shared across MDS nodes, in expected order of
significance:

1. the RonDB cluster itself — TC threads, LDM threads, redo
   bandwidth are the aggregate commit budget all MDSes draw from;
2. per-directory parent rows — the interpreted update serialises
   same-directory mutations cluster-wide at the row lock (though
   the Q0 evidence shows TC batching currently makes the shared-row
   case *cheaper* at 16 threads, not slower);
3. cross-directory RENAME's topology lock;
4. the fileid counter — batched range allocation via CAS on
   `mds_meta.fileid_counter` (`rondb_shim_alloc_fileid_range`),
   amortised to one CAS per batch, not per create;
5. control-plane only: partition/subtree-map updates, changefeed
   seqno persistence.

**D1 — per-MDS vs aggregate scaling: existing evidence.**  The
Phase 3 lab run scaled creates 4.6× per-MDS *and* 4.6× aggregate
across three MDSes once a shared bottleneck (sync FH capture) was
removed — before that fix, all three MDSes pinned at an identical
32/s, the signature of a shared upstream serialiser.  Aggregate
scaling works when nothing is shared; the inventory above is the
list of remaining shared things.  **Pending:** repeat at higher MDS
counts with per-MDS AND aggregate rates recorded, watching items 1
and 2 for the knee.

**D3 — position.**  Aggregate parity is this architecture's native
answer, the same way multi-MDT deployments are the native answer
elsewhere: subtree ownership, referrals and shard junctions already
exist and Wave-scale work has kept them healthy.  If per-MDS
creates plateau near the commit-latency bound but aggregate scales
to N nodes against a sufficiently provisioned RonDB cluster, the
design goal can be met at a different cost point.  The
recommendation treats D as the deployment lever it is, not as an
engineering work item.

## Constraints, applied

- **COMPOUND ordering / session reply cache** — unaffected by A, B
  (per-request blocking preserved), D; C3's `remove_async` keeps
  the ack transaction durable before the reply is cacheable;
  C2 adds callback traffic but no request-path reordering.
- **Monotonic change attribute** — the one live risk is
  `parent_touch_deferred` across a crash (C4 table); every other
  option preserves it structurally.
- **Multi-MDS coherence** — B is per-MDS batching of already
  row-locked commits; C3's tombstones are backed by durable rows
  every MDS can see; C2's delegation state is per-MDS granted but
  recall/notify must be driven by the changefeed for peer-MDS
  mutations — enabling dir delegations in a multi-MDS deployment
  requires that wiring to be verified (add to the C2 measurement's
  checklist).
- **Crash consistency** — per-option budget in C4; A2's replica
  trade is stated in Axis A.

## Preliminary recommendation (gated)

Ordered by expected return, each gated on its measurement:

1. **Q0 first.**  Decompose the 0.78 ms.  Everything else is
   mis-sized until this exists.
2. **B — validate the existing pipeline** after lifting the
   `Ndb::init()` cap in a lab patch.  It is the only lever that
   attacks the measured wall (per-worker serialised commit waits)
   with zero semantic cost.  If the depth sweep confirms even a
   fraction of the arithmetic, the production follow-ups are small
   and local: size `init(N)` and the pool from measurements, keep
   the flag default-off until burn-in.  B does nothing for
   single-op latency — say so wherever the result is quoted.
3. **A1 (+A3 SHM) as deployment guidance** — colocation is
   configuration, not code, and its +25% predates the fused create;
   re-measure and write the failure-domain caveats into the ops
   docs.  **A2 stays an explicitly priced option**, never a
   default: publish the measured latency delta next to the
   durability statement and let deployments choose.
4. **C3 for the remove path** — `remove_async` (with its
   `parent_touch_deferred` prerequisite) attacks remove/tree-remove
   rates without touching name-visibility durability.  Gate on the
   parent change-attr monotonicity question being answered in
   design first (C4), and on measured deltas.
5. **C2 as an ancillary-traffic experiment**, not a mutation-rate
   lever: the protocol does not allow what the Lustre model does,
   and the C1 history shows delegation machinery can be
   net-negative on create-heavy workloads.  Measure before any
   default change; multi-MDS notify wiring must be verified first.
6. **D as the deployment answer for aggregate targets** — document
   per-MDS vs aggregate framing in every result, and treat RonDB
   cluster provisioning (item 1 of the inventory) as the aggregate
   budget it is.

**Revised target, expressed as multiples (no matched competitive
baseline exists yet — the prerequisite stands).**  Current
reference points: 2,883 creates/s (16-rank, 1-DIR, warm lab) and
408 creates/s / 313.6 removes/s (cold, end-to-end).  The
hypotheses to confirm: B at effective depth 16–64 targets 3–10× the
per-MDS create rate; A1+A3 targets a further 1.2–2× by cutting the
per-commit floor; D multiplies whatever per-MDS number survives by
node count until the RonDB cluster binds.  If the measured
combination cannot reach parity with entrenched scale-out metadata
servers without the A2 durability trade, this document's answer is
that aggregate scaling (D) plus the measured per-MDS number is the
honest offer — and that is an acceptable outcome of the wave.

## Lab measurement plan (harness requirements)

Every result set records, per the Wave 0 harness: both revision
hashes; the generated `mds.conf` verbatim; the effective-config
checklist (`worker_threads`, `ndb_conn_pool_size`,
`ndb_async_writes`, `transient_state_cache`, `inode_cache_size`,
`parent_touch_deferred`, `remove_async`, `dir_delegations_enabled`,
`file_delegations_enabled`, listener count, build type, assertion
policy); MDS count, client count, `nconnect`, DS count, RonDB
topology and replica count, `Ndb::init()` value where patched; and
the exact mdtest invocation.  Metrics come from the Prometheus
endpoint via `scripts/mds-metrics-diff` around each burst plus
`perf record -g` on the serving MDS; ndbinfo snapshots accompany
every Axis A/B run.

Runs, in order: Q0 sweep (baseline, A1, A2, redo settings, A4
shapes); B1/B2 depth sweep (init × pool × workers × ranks); C3
knob deltas (16-rank spread + 8-rank shared-parent); C2 on/off
(multi-client, plus `ls -l`-after-create probe); D repeat of the
3-MDS aggregate run at higher node counts.

## Implementation task list (draft; gated)

Every item is gated on the measurement named above.

1. Lab-only patch: `Ndb::init(N)` parameterisation for the B sweep
   (never shipped as-is; production sizing comes from the sweep).
2. If B validates: production sizing of `init(N)` +
   `ndb_conn_pool_size`, flag remains opt-in until burn-in
   (gate: B1/B2 results).
3. If Q0 attributes ≥30% to API→TC: colocation deployment guide +
   SHM transporter enablement notes (gate: Q0 + A1/A3 numbers).
4. Design note resolving parent change-attr monotonicity across
   crash for `parent_touch_deferred` (gate: none — pure design,
   prerequisite for any C3 recommendation).
5. If C3 validates: enable-path documentation for
   `remove_async`+`parent_touch_deferred` with the C4 budget table
   copied into the config docs (gate: C3 deltas + item 4).
6. If C2 measures positive anywhere: changefeed-driven notify
   verification for multi-MDS, then a workload-scoped enablement
   note (gate: C2 measurements).
7. A2 write-up as a priced option in the ops docs (gate: Q0/A2
   numbers).

## Non-goals

- Writing or merging production code in this wave.
- Enabling `ndb_async_writes`, colocation, either delegation type,
  or any deferred-durability knob by default.
- Treating any single benchmark number as "the" mutation rate:
  every quoted result carries its workload shape (rank count,
  directory layout, cache state) or it is not quotable.

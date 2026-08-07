# Performance wave 6 — profile-gated items: dispositions

Wave 6 items are gated on measurements: each either gets
profile-justified work, or is closed with its disposition and the
deciding evidence (or the instrumentation that will produce it)
recorded.  This file is that record.  Lab measurements referenced as
"pending" are read from the Prometheus endpoint with
`scripts/mds-metrics-diff` around a workload burst, or taken with
`perf` on the serving MDS.

## T6.1 — dispatch buffer swap and epoll arming flag

**Disposition: deferred pending a lab CPU profile.  No code.**

`dispatch_record()` copies each assembled record into an owned work
item, and every worker completion issues one `epoll_ctl(MOD)`
(`src/mds/rpc_server.c`).  Neither cost is invisible: the `MOD` count
equals the completed-request count (`pnfs_mds_rpc_completed_total`)
and the copied bytes equal the request bytes, so new counters would
add no information — the missing evidence is a CPU profile showing
allocation, copy, or `epoll_ctl` time.  There is also no reference
implementation to diverge from.  Proceed only if `perf` on a loaded
MDS attributes measurable time to these sites; the related
buffer-pooling / eventfd / reply-drain ideas are profile-first for
the same reason.

## T6.2 — NDB pool sizing default

**Disposition: deferred to Wave 7 by design.  No code.**

The auto-size (worker count capped at 64,
`src/common/config.c`) stays.  The wave doc itself forbids settling
this before Wave 7's async-pipeline prototypes, because an adopted
batch pipeline changes the right pool size.  Constraint to respect
when it is measured: the RonDB `[api]` slot budget is
`num_MDS × pool`.

## T6.3 — RonDB retry tuning

**Disposition: instrumented; tuning remains data-driven in the lab.**

The wrapper retries transient NDB errors 8× with 500 µs–16 ms
exponential backoff through one choke point
(`rondb_transient_backoff()`, `src/catalogue/catalogue_rondb.c`); the
shim retries 12× internally.  Both layers were invisible.  New
counters expose the wrapper layer, which is where shim-internal
exhaustion surfaces anyway:

- `pnfs_mds_cat_transient_retries` — backoff sleeps taken
- `pnfs_mds_cat_transient_backoff_us` — total time workers slept
  (worker-occupancy cost of retrying)
- `pnfs_mds_cat_transient_retry_exhausted` — loops that surrendered
  with the error still transient (client saw DELAY/IO)

Decision procedure: run the mdtest contention workloads, read the
deltas alongside p99 and worker occupancy, then judge whether
shorter/longer ladders help.  Sleeping workers amplify queueing;
returning DELAY earlier shifts retry work to clients — a trade, not a
free win.

## T6.4 — per-DS indexing for the DS filehandle cache

**Disposition: instrumented; indexing gated on the measured hit
ratio.  Known cliff recorded below.**

The FH-capture cache (`src/mds/ds_nfs_rpc.c`) is a 16-entry linear
scan under one global mutex, caching one "data/" directory FH per
(host, export).  New counters `pnfs_mds_ds_fh_cache_hits` /
`_misses` expose the hit ratio; a structurally low ratio during
capture bursts is the go-signal for per-DS indexing.

Recorded cliff: `DS_FH_CACHE_MAX` is 16 while the DS registry allows
`MDS_MAX_DS_NODES` = 256, and the eviction policy is evict-slot-0.
Above 16 DSes the cache silently thrashes (every miss re-pays
portmapper + MOUNT + path walk, three TCP connections).  Irrelevant
at current deployment sizes; must be revisited before any deployment
crosses 16 DSes even if the hit ratio looks healthy today.

## T6.5 — REMOVE path

**Disposition: instrumented for cost separation; any change to the
delete path stays gated on those numbers.**

The wave doc requires four costs separated before touching the
REMOVE path.  All four are now independently readable:

1. Synchronous metadata mutation —
   `pnfs_mds_cat_op_latency_seconds{cat_op="ns_remove"}`.
2. Client-visible final-unlink layout recall — new
   `{cat_op="unlink_recall"}` histogram (the ~10 ms/unlink cost class
   measured on the reference tree; count = removes that paid it).
3. Persistent post-remove cleanup — `pnfs_mds_remove_async_*`
   counters (`remove_async` itself remains off by default).
4. DS reclamation latency — `pnfs_mds_gc_pending` gauge + ds_gc
   counters.

Raise the item substantially if delete-heavy workloads matter; the
mutation itself is one NDB commit and shares Wave 7's conclusion.

## T6.6 — single-MDS parity audit

**Disposition: audited; existing gating is correct.  No switched code
paths added** (per the wave's own warning that switches double the
test matrix).

The scripted single-MDS profile sets `transient_state_cache = true`,
which the audit confirms already removes the two costs Waves 3–4
attack, and Wave 5's additions are orthogonal or config-disabled:

- Wave 4 T4.1/T4.3 (OPEN persist policy + lock-free persist window):
  `open_state_open()` returns via a fast path before any
  persist-pending publish/unlock/relock when `skip_ndb_persist` is
  set; the fresh path only marks `persist_pending` when a durable
  write follows; CLOSE skips the NDB delete.  Residual single-MDS
  cost: one `bool` test per pending-guard check.
  (`src/mds/open_state.c`, fast-path return, pending-mark guard, and
  CLOSE guard.)
- Wave 3 T3.1 (new-file LAYOUTGET fast path): the holder scan is
  gated on `!cd->skip_transient_ndb`, so the fast path is inert —
  not harmful — under transient mode, and its skip counter
  deliberately counts only when a scan would otherwise have run
  (`src/mds/compound_layout.c`, recall-scan gate).
- Wave 4 T4.2 (table sizing): config defaults only; no runtime
  switch, no per-request cost.
- Inode-cache TTL: the TTL clock read happens only when `ttl_ms != 0`
  (`src/mds/inode_cache.c`, `inode_cache_get`).  A single-MDS
  deployment sets the TTL key to 0 and pays one integer compare per
  hit; no code switch is warranted.
- Wave 5 `ds_io_limits`: orthogonal to transient mode;
  `ds_iolimit_probe_ms = 0` restores the legacy wire constants
  bit-for-bit.

Conclusion: single-MDS deployments already avoid the multi-MDS
costs through existing runtime checks whose price is a boolean test.
No compile-time or additional runtime switches are justified by this
audit; revisit only if a lab profile of the single-MDS configuration
attributes measurable time to one of the guarded sites.

## Exit status

- Closed, no code: T6.1 (pending CPU profile), T6.2 (deferred to
  Wave 7), T6.6 (audited correct).
- Instrumented, decision pending lab numbers: T6.3, T6.4, T6.5.

Nothing in this wave changed request-path behaviour; the additions
are relaxed-atomic counters and one timed scope on an existing call.

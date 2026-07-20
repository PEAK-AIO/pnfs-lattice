# Lattice Architecture
Lattice is a parallel NFS (pNFS, RFC 8881 / RFC 7862) metadata server.  It speaks
NFSv4.1 and NFSv4.2 to clients on the front end and routes bulk file I/O around
itself by handing clients a layout that points at one or more data servers
(DSes).  The metadata is kept in a transactional, shared-nothing key-value
store (RonDB / NDB) so that several Lattice daemons can serve the same namespace
simultaneously without a coordinator.
This document describes the architecture as it stands in the source tree.  It
is intended for contributors and operators who need to understand how the
pieces fit together before touching code.
## 1. Goals and non-goals
### Goals
- **Active-active metadata.**  Any number of Lattice daemons can serve the same
  filesystem at the same time.  There is no leader, no global lock service,
  and no fail-over event between daemons.
- **Out-of-band data.**  Clients never read or write file payload through the
  MDS.  The MDS only mints layouts; the data path is client → DS over NFSv3
  (flex-files) or NFSv4.1 file layout.
- **Correctness over throughput.**  Every namespace mutation is a single
  transactional operation in the catalogue.  Concurrent OPEN, CREATE, RENAME,
  REMOVE, LINK between daemons cannot lose updates or produce torn reads.
- **Operability on commodity hardware.**  No special kernel modules, no
  custom hardware, no kernel patches.  RonDB and the standard Linux NFS
  stack are sufficient.
### Non-goals
- **Embedded local metadata.**  Lattice does not store authoritative metadata
  on local disk.  Stateless on restart; the catalogue is the source of truth.
- **Block storage / direct-attached striping.**  Lattice is a metadata service.
  Stripe placement is calculated by the MDS but the actual blocks live on
  data servers that present a regular file-per-stripe interface.
- **Cross-cluster replication / DR.**  A single Lattice deployment is one
  RonDB cluster.  Cross-cluster replication is delegated to RonDB's own
  binlog tooling and is out of Lattice's scope.
## 2. System view
```mermaid
flowchart LR
    subgraph clients[NFS clients]
        c1[Linux pNFS client]
        c2[Linux pNFS client]
    end
    subgraph mds[Lattice MDS cluster]
        m1[lattice-pnfs 1]
        m2[lattice-pnfs 2]
        m3[lattice-pnfs N]
    end
    subgraph ndb[RonDB cluster]
        ndbA[(NDB data node)]
        ndbB[(NDB data node)]
        ndbC[(NDB data node)]
    end
    subgraph ds[Data servers]
        d1[DS 1 NFSv3/4.1]
        d2[DS 2 NFSv3/4.1]
        d3[DS N NFSv3/4.1]
    end
    clients -- "RPC: NFSv4.1/4.2" --> mds
    mds -- "NDB API\n(transactional KV)" --> ndb
    clients -- "NFS READ/WRITE\n(layout-directed)" --> ds
    mds -- "GC unlink, prepare,\nplacement probes" --> ds
```
A request flows like this:
1. Client mounts the export and walks the namespace via LOOKUP / GETATTR.
2. On OPEN, the MDS returns a stateid; on the immediate LAYOUTGET it returns
   a flex-files or file layout pointing at one or more DSes.
3. Client sends NFS READ / WRITE directly to those DSes.
4. On CLOSE, LAYOUTRETURN releases the layout state.
The MDS never sees data bytes during the steady state.  The MDS does see
COMMITs and SETATTR(size=...) which it persists to the catalogue.
## 3. Process model
A single Lattice binary (`lattice-pnfs`, `src/mds/main.c`) runs as one Linux
process per node.  Inside that process there is no fork: every subsystem is
threads sharing one address space.
| Thread group | Source | Role |
|---|---|---|
| RPC listeners | `src/mds/rpc_server.c` | One per configured listen socket; accepts RPC connections, decodes NFSv4 COMPOUND, hands work to the worker pool. |
| Compound workers | `src/mds/rpc_server.c`, `compound.c` | Pool that runs `compound_process()` end-to-end for one COMPOUND request. |
| Catalogue I/O threads | `src/catalogue/catalogue_rondb_shim.cpp` | Bound to NDB cluster connections; drive transactions on behalf of compound workers. |
| Commit queue | `src/mds/commit_queue.c` | Optional batched-write path that coalesces small NDB writes (inline data, dirent updates) into larger transactions. |
| Layout recall | `src/mds/layout_recall.c` | CB_LAYOUTRECALL deliveries on the back-channel. |
| DS GC | `src/modules/ds_gc/ds_gc.c` | Coordinator + worker pool; drains durable final-unlink tasks and issues fenced DS cleanup. |
| DS pre-allocator | `src/mds/ds_prealloc.c` | Refills a small per-DS lookahead pool of stripe coordinates so OPEN(create)+LAYOUTGET hits no NDB pre-write. |
| DS health | `src/mds/ds_health.c` | Periodic NFS NULL probe + LAYOUTERROR aggregation; feeds placement. |
| Cluster transport | `src/cluster/cluster_transport.c` | gRPC peer messaging for cross-MDS cache invalidation, hard-link 2PC, etc. |
| Sessions / DRC | `src/mds/session.c` | NFSv4.1 session table, slot tables, replay cache. |
| Backchannel | `src/mds/nfs4_cb.c` | CB_COMPOUND encoder + transport for CB_RECALL, CB_LAYOUTRECALL, CB_NOTIFY. |
| Metrics | `src/mds/metrics_http.c` | HTTP `/metrics` endpoint (Prometheus text format). |
The worker pool is sized by `worker_threads` (config); the listener pool by
`listener_threads`.  All other threads are singletons or small fixed pools.
## 4. Source layout
```text path=null start=null
src/
├── mds/         # NFSv4.1/4.2 protocol surface and per-op handlers
├── catalogue/   # Pluggable metadata backend (RonDB, in-memory test stub)
├── cluster/     # Cross-MDS coordination (transport, membership, 2PC)
├── common/      # Shared utilities: config, fh codec, endian helpers
├── fsal_obj/    # FSAL-style object abstractions used by the MDS
├── tools/       # CLI tools (admin, dump, replay, etc.)
└── bpf/         # Optional eBPF tracepoints for observability
include/         # Public-facing headers; one per logical subsystem
proto/           # gRPC service definitions for cluster transport
tests/
├── unit/        # Per-module C unit tests
└── integration/ # End-to-end tests against a memdb catalogue
```
The boundary between `mds/` and the rest is intentional.  `mds/` knows about
NFSv4 ops; everything below is protocol-agnostic and could in principle be
reused by a different front end.
## 5. Compound processing
Every NFSv4.1/4.2 client request is a COMPOUND containing one or more ops.
The processing pipeline is:
```mermaid
flowchart LR
    A[RPC listener\naccept + decode] --> B[Worker pool\nXDR decode + auth]
    B --> C[compound_init\nbuild compound_data]
    C --> D[per-op dispatch\nop_open / op_read / ...]
    D --> E[encode result\nXDR + RPC reply]
    D -. on mutate .-> F[catalogue txn\nsingle NDB commit]
    D -. on layout .-> G[placement\n+ stripe map write]
```
### `compound_data`
A short-lived per-request struct (`include/compound.h`) that holds:
- The current and saved file handles (FH).
- A small inline cache of the inode for current/saved FH (so a sequence
  PUTFH+GETATTR doesn't re-read the catalogue).
- Pointers to the long-lived subsystem handles: catalogue, sessions,
  open-state table, lock table, delegation tables, caches, quota, shard
  map, subtree map, cluster transport.
- Caller credentials (AUTH_SYS uid/gid + supplementary GIDs).
- A per-request notion of "current shard" used by sharded deployments.
The struct is rebuilt fresh per COMPOUND.  Any state that must outlive a
compound lives in one of the long-lived subsystems it points at.
### Per-op dispatch
Op handlers are split across files by topic:
- `compound_namespace.c` — ACCESS, PUTFH/PUTROOTFH/SAVEFH/RESTOREFH/GETFH,
  LOOKUP/LOOKUPP, GETATTR/SETATTR, CREATE/REMOVE/RENAME/LINK, READDIR,
  READLINK.
- `compound_data_io.c` — OPEN/CLOSE, READ/WRITE, IO_ADVISE, COMMIT, the
  delegation grant point, and inline-data promotion.
- `compound_layout.c` — LAYOUTGET, LAYOUTCOMMIT, LAYOUTRETURN,
  GETDEVICEINFO, LAYOUTERROR, LAYOUTSTATS.
- `compound_session.c` — EXCHANGE_ID, CREATE_SESSION, SEQUENCE,
  DESTROY_SESSION, BIND_CONN_TO_SESSION, RECLAIM_COMPLETE.
- `compound_nfsv42.c` — ALLOCATE, DEALLOCATE, COPY/COPY_NOTIFY/CLONE/SEEK,
  xattr ops.
- `compound.c` — top-level dispatcher and helpers (`compound_process`,
  `compound_inode_get`, snapshot invalidation).
Each op returns an `enum nfs4_status`; the encoder (`xdr_codec.c`) turns the
result union into a wire reply.
## 6. Catalogue (metadata backend)
Lattice abstracts its metadata store behind a small C ABI in
`include/mds_catalogue.h`.  Two backends ship in tree:
- **RonDB / NDB** (production) — `src/catalogue/catalogue_rondb_shim.cpp`
  wraps the NDB C++ API behind a narrow C surface.  The shim opens NDB
  cluster connections, manages a per-thread `Ndb` object, and exposes a
  one-call-one-transaction interface to the rest of Lattice.
- **memdb** (tests) — `src/catalogue/catalogue_memdb.c` is an in-memory
  hash-table backend used by the unit tests so the suite has no external
  dependency.
Both backends implement the same vtable (`include/catalogue_internal.h`).
Tables (logical, not literal NDB DDL):
| Table | Purpose |
|---|---|
| `inodes` | Per-inode attributes (mode, owner, size, change, parent_fileid). |
| `dirents` | Parent-fileid + name → child fileid + type. |
| `stripe_maps` | Per-file layout: stripe count, mirror count, ordered (ds_id, FH) list. |
| `inline_data` | Small-file payload + symlink targets. |
| `xattrs` | RFC 8276 user xattrs. |
| `gc_tasks` | Durable lease-fenced cleanup work, keyed by task kind and ID. |
| `gc_queue` | Legacy migration source retained until its rows become `gc_tasks`. |
| `delegations` (optional) | Persisted file delegations for cross-MDS visibility. |
| `layouts` (optional) | Persisted layout state for cross-MDS visibility. |
| `coord_*` | Cross-MDS coordination state (subtree ownership, fencing). |
### Atomicity contract
Every mutating MDS op compiles down to **one NDB transaction**.  Examples:
- `mkdir`, `rmdir`, `link`, `rename` use NDB `interpretedUpdateTuple`
  (`incValue` / `subValue`) to update the parent's `nlink`, `mtime`, `ctime`,
  and `change` counter atomically with the dirent insert / delete.  No
  read-modify-write race even when several MDS daemons mutate the same
  parent directory concurrently.
- `setattr` takes an exclusive row lock, reads the current inode, merges
  the requested attribute mask, and writes back in a single transaction.
- `rename` (including the same-cluster cross-subtree case) is one
  transaction in `rondb_shim_rename`: delete src dirent, write dst dirent,
  interpreted parent updates, and child `parent_fileid` change all commit
  atomically.
### Cross-MDS coordination
For a small set of operations that span more than one logical row group
across shards or MDSes, Lattice layers a higher-level protocol on top of the
catalogue:
- `src/cluster/rename_2pc.c` — cross-shard rename when the deployment is
  sharded (the single-RonDB-cluster case collapses into one NDB txn; this
  module exists for multi-cluster topologies).
- `src/cluster/hardlink_2pc.c` — cross-subtree hard link (target inode and
  link directory in different shards).  Disabled by default until the
  surrounding plumbing is complete.
## 7. pNFS layout path
Lattice serves two pNFS layout types:
- **Flex-files** (default) — DS endpoints are NFSv3, one file per stripe.
- **NFSv4.1 file layout** — DS endpoints are NFSv4.1.
Both share the same in-MDS pipeline:
1. **OPEN(create)**.  `op_open` calls `cat_create()`, which allocates a
   fileid, writes the dirent and inode, and (when the pre-allocator is on)
   pulls a stripe coordinate from a per-DS lookahead pool so the layout is
   ready before LAYOUTGET arrives.
2. **LAYOUTGET**.  `op_layoutget` either reuses the pre-grant from OPEN or
   computes placement via `placement_select_ex()` (`src/mds/placement.c`)
   against the active `mds_shard_map` / `ds_health_monitor` view.  The
   resulting stripe map is persisted to the `stripe_maps` table; the layout
   stateid is minted by `layout_state.c` and returned to the client.
3. **Client I/O**.  Client opens the DSes named in the layout and reads or
   writes directly.  The MDS sees nothing.
4. **LAYOUTCOMMIT**.  Client tells the MDS the new file size and mtime; the
   MDS updates the inode in one transaction.
5. **LAYOUTRETURN** / **CB_LAYOUTRECALL**.  Either side can drop the layout.
   Recalls are sent on the NFSv4.1 back-channel via `nfs4_cb_layoutrecall`.
The DS pre-allocator (`ds_prealloc.c`) and ds_cache (`ds_cache.c`) exist to
cut the LAYOUTGET fast path to a single in-memory lookup; on cache miss the
fallback is a normal catalogue read.
### DS file-handle verification
Before LAYOUTGET emits stripe-map entries that did not come from the layout
cache, `compound_layout.c` verifies each DS file handle through the configured
proxy.  A changed handle is first persisted to the stripe map, then replaces
any cached map.  If verification or the durable rewrite fails, Lattice
invalidates the local layout-cache entry, revokes the provisional layout,
marks the file `DS_PENDING`, and returns `NFS4ERR_DELAY`; it never emits a
stale nonzero DS file handle from the verify path.  Layout-cache hits are
served without re-verification: entries only enter the cache after a verified
serve, and re-ensuring every (stripe, mirror) handle per hit would cost
`stripe_count * mirror_count` serial DS round trips per LAYOUTGET.  A handle
that goes stale behind a cached entry is recovered through the client's
`LAYOUTERROR`: `op_layouterror` invalidates the cache entry, so the next
LAYOUTGET misses and takes the fail-closed verify path.  Deployments without
a proxy retain their existing synthetic-handle behavior.
### Authoritative `LAYOUTCOMMIT` size
For ordinary (non-aggregated) `LAYOUTCOMMIT` size growth, the byte quota is
enforced before the durable write using the request-snapshot delta (a
conservative estimate), exactly as on the aggregated HPC path.  The write
itself, `mds_cat_ns_setattr_size_extend`, performs one exclusive-row
read-modify-write transaction and returns both the size observed under the
row lock and the size durable after commit.  The reply reports that durable
size, while quota accounting uses only the locked-to-committed delta.  Thus a
stale smaller commit following a larger concurrent commit cannot reduce the
reported size or charge the already-accounted growth again, and an over-quota
caller is rejected before any durable change.
### Final-unlink GC
When `op_remove` drops the last link of a regular file
(`compound_namespace.c` → `enqueue_gc_for_final_unlink`), the MDS:
1. Reads the file's stripe map.
2. Enqueues one durable cleanup task per unique DS in the map (the worker
   later sweeps stripe/mirror coordinates within each DS).
3. Drops the stripe-map row.
4. Frees any in-memory delegation grants for the now-gone file
   (`deleg_revoke_file`).
The DS GC subsystem (`ds_gc.c`) drains tasks with a coordinator + worker pool:
the coordinator claims batches via `mds_cat_gc_task_claim_batch`, hands them
to workers that issue NFS UNLINK to the DSes, and completes a task only after
successful cleanup.  Every DS mount or I/O error reschedules the task with a
back-off; cleanup work is never silently dropped.

The durable task implementation uses `mds_gc_tasks`, keyed by
`(task_kind, task_id)`.  A worker claims a task with its MDS ID and boot epoch;
another worker can reclaim it only after lease expiry or when the recorded
owner is absent, restarted, or stale in the shared node registry.  Completion,
renewal, and retry release require the same fenced owner identity.  Thus an
unavailable DS mount or I/O failure moves the task back to `PENDING` with a
retry time instead of discarding cleanup work.  During the schema-v10 upgrade,
each legacy queue row is copied into an idempotent legacy task and removed only
as part of the durable task migration transaction.  Malformed legacy rows are
converted atomically into quarantined tasks instead of preventing startup.
Claim scans likewise quarantine malformed task rows after the scan and continue
claiming valid work from the same batch.
### Atomic final-file unlink handoff
`mds_cat_ns_remove_final_file` is the dedicated delete-at-ack catalogue
primitive for a final regular-file unlink.  In one backend transaction, it
validates the named fileid and generation, removes the dirent, retains the
inode with `nlink=0` and `MDS_IFLAG_DELETE_PENDING`, advances the parent
change counter, and inserts the `(FILE_UNLINK, fileid)` durable task.  The
retained stripe map supplies the exact DS cleanup layout; no legacy GC queue
row or sequence number is created.  The transaction reports the authoritative
parent change values after commit.

`remove_async` is default-off. When enabled, it additionally requires a live
GC worker, durable shared open-state persistence, and cross-MDS cache
invalidation. The remove request does no DS RPC after a successful atomic
handoff. The inode remains delete-pending until the worker observes no durable
opens, fences and unlinks its exact DS objects, finalizes metadata, and releases
the deferred quota exactly once. New opens that encounter delete-pending state
fail `NFS4ERR_STALE`.

Open-unlinked retention currently lasts only for the running server instance.
`CLAIM_PREVIOUS` recovery is not implemented, so an open-unlinked lifetime
cannot be reconstructed safely after restart.  OPEN therefore intentionally
withholds `OPEN4_RESULT_PRESERVE_UNLINKED`, even when `remove_async` is active.
The encoder retains support for the flag, but it must not be advertised until
restart-safe reclaim completes the wire-level contract.

The GC coordinator periodically snapshots active durable file tasks. Request
threads make a cache-only hysteretic decision: reaching
`remove_async_high_watermark` switches final unlinks to the established
synchronous path, and async handoff resumes only at or below
`remove_async_low_watermark`. Each accepted async handoff increments the
cached backlog immediately. This bounds queue growth without a request-thread
catalogue scan or an acknowledged-but-untracked delete.
### Synchronous REMOVE baseline
`tests/integration/bench_remove_sync.c` is a manually invoked serial baseline
for the synchronous final-unlink implementation.  It pre-seeds a dedicated
parent directory with one-stripe regular files and local DS backing objects,
then measures the full in-process `SEQUENCE + PUTFH + REMOVE` compound.  The
benchmark sets `skip_transient_ndb=true`, so it confirms the active
transient-state fallback still fences every mapped DS object before replying.
Its explicit cleanup removes only benchmark-created object IDs and drains
only verified benchmark GC rows.  The default memdb result is a regression
signal; RonDB measurements must use an isolated schema with an empty GC
queue.  Neither mode includes front-channel RPC or client-network latency.

Directory change accounting remains exact by default and by implementation:
each namespace mutation atomically advances the durable parent inode change
counter and timestamps.  The former `PNFS_RELAX_DIR_CHANGE` environment
bypass is not supported because it can expose stale directory change
attributes.  A scalable parent-delta mode requires an independently reviewed
durable bucket schema, derived GETATTR and `change_info4` values, cache and
crash behavior, and concurrent multi-MDS RonDB measurements.  The serial
memdb baseline above cannot establish that prerequisite.
## 8. Concurrency and consistency
### State partitioning
- **Catalogue rows** — authoritative.  Concurrent writers are serialised by
  NDB's per-row locking and the single-transaction-per-mutation contract.
- **Per-MDS in-memory state** — hot caches (inode, dirent, ds), open-state
  table, lock table, delegation tables, session table.  Cross-MDS
  visibility for the parts that need it (delegations, sessions, layout
  state) is achieved by also persisting to RonDB; the in-memory copy is a
  fast path keyed by the canonical row.
- **Cross-MDS invalidation** — best-effort, over the cluster transport.
  Caches are tolerant of staleness because every fast-path read validates
  against the catalogue's `change` counter or row generation.
### Locking primitives
- **Striped mutexes.**  16-stripe `pthread_mutex_t` arrays are the default
  pattern (`open_state`, `delegation`, `lock_state`).  Hash on the natural
  key (clientid, fileid) selects the stripe.
- **NDB exclusive row locks.**  Used for `setattr` and a handful of other
  read-modify-write paths that must serialise with concurrent MDSes.
- **Read-mostly hot configs** — `_Atomic` pointers + RCU-style swap on
  reload for the shard map and DS table.  No explicit reader lock.
### Compound-internal snapshot caching
Within a single COMPOUND, an inode is read at most once per fileid.
`compound_inode_get` first checks `cd->current_inode` / `cd->saved_inode`,
then the global `inode_cache`, then the catalogue.  Mutations within the
same compound invalidate via `compound_inode_invalidate`.  This is what
keeps `PUTFH+GETATTR` to a single NDB read.
### Lease and recovery
NFSv4.1 leases are tracked per-client in `session.c`.  Lease expiry runs a
janitor that:
- Calls `deleg_revoke_client` to drop in-memory delegation grants.
- Schedules `open_state_revoke_client` to clean up locks and opens.
- Removes session/connection entries.
Grace mode (`grace.c`) prevents non-reclaim opens during the configured
grace window after daemon start.
## 9. Networking and security
### Front-channel
- ONC-RPC over TCP, NFSv4.1 minor version 1 and 2.
- Auth: AUTH_SYS, AUTH_NULL.  RPCSEC_GSS (krb5/krb5i/krb5p) is gated on the
  `mds_gss` build option.
- TLS: `mds_tls.c` provides per-listener TLS; configurable per export.
### Back-channel
NFSv4.1 sessions carry an explicit back-channel.  Lattice uses it for:
- `CB_RECALL` of file delegations.
- `CB_LAYOUTRECALL` of pNFS layouts.
- `CB_NOTIFY` for directory delegations (RFC 8881 §10.6).
The back-channel transport is implemented in `nfs4_cb.c` with the same XDR
codec used for the fore-channel.
### Inter-MDS
gRPC over TCP for the cluster transport (`src/cluster/cluster_transport.c`,
`proto/`).  Used for:
- Subtree map / membership change notifications.
- Best-effort cross-MDS cache invalidation.
- The 2PC paths (`rename_2pc`, `hardlink_2pc`) that may live alongside
  multi-cluster deployments.
## 10. Observability
- **Metrics.** Prometheus text format on a configurable HTTP endpoint
  (`metrics_http.c`). Counters cover NFS op rates, catalogue txn rates,
  placement decisions, layout error counts, and branch-level latency
  histograms. Async final-file REMOVE exports active/claimed/oldest GC-task
  gauges; claim, retry, open-blocked, unavailable-DS, takeover, permanent
  failure, and completion totals; deferred quota gauges; and the synchronous
  fallback and backpressure-active state.
- **Structured logs.**  Journald via systemd by default; one line per
  significant event (recall, revoke, deadlock-class NDB error, GC error).
- **Optional eBPF tracepoints.**  `src/bpf/` ships a minimal set of USDT
  probes for compound-op latency and catalogue txn timing.
- **Soak harness.**  `scripts/pnfs-lab` drives a multi-host build + deploy +
  smoke + soak workflow against a lab fleet for regression testing.
## 11. Build and test
- **Build.**  CMake-based; `cmake -S . -B build && cmake --build build`.
  The RonDB backend is gated by `-DHAVE_RONDB=ON` and links against
  `libndbclient`.
- **Unit tests.**  `tests/unit/` against the memdb backend; one binary per
  module.  No external services required.
- **Integration tests.**  `tests/integration/` exercises multi-component
  flows still against memdb.
- **QA gates.**  `scripts/qa-check.sh --quick` runs style, gcc + clang
  builds, cppcheck, clang-tidy, and unit tests.  The full mode adds
  Valgrind and the integration suite.
- **Smoke.**  `scripts/pnfs-smoke.sh` runs a 17-step real-NFS smoke against
  a deployed lab.
- **Soak.**  `tests/soak/pnfs-soak.sh` produces an iter / pass / fail log
  consumed by `pnfs-soak-report.sh` for trend analysis (RSS slope, error
  rate, GC backlog).
## 12. Configuration model
Lattice reads a single INI file (default `/etc/lattice-pnfs/mds.conf`).  Keys are
namespaced by subsystem; the canonical reference is `man lattice-pnfs.conf`
(`docs/man/`).  Important groups:
- `[server]` — listen addresses, worker counts, lease time.
- `[catalogue]` — backend selection, NDB connection string.
- `[cluster]` — MDS id, peer list, subtree map source.
- `[ds]` — DS endpoints, health probe cadence, pre-allocator pool sizes.
- `[gc]` — `ds_gc_workers`, `ds_gc_batch_size`, retry policy.
- `[delegations]` — file and directory delegation toggles, transient mode.
- `[security]` — TLS / GSS settings.
All keys have safe defaults; a minimal config can be a half-dozen lines.
## 13. Limits and known constraints
- One RonDB cluster per Lattice deployment.  Multi-cluster federation is
  out of scope for the core; the 2PC modules exist as building blocks but
  are not exposed as a supported configuration.
- Maximum stripe count and mirror count are fixed at compile time by
  `MDS_MAX_STRIPES` and `MDS_MAX_MIRRORS`.  The defaults are sized for the
  flex-files target.
- Directory delegations are off by default.  Enabling them requires the
  shared-attr backend to be on so recall propagation works across MDSes.
- The hard-link 2PC path is feature-gated and not exercised in production
  deployments.  Cross-subtree LINK returns `NFS4ERR_XDEV` until the
  surrounding work is complete.
## 14. Where to start reading
- **A new COMPOUND op handler.**  `src/mds/compound.c` for dispatch, then
  the topic file (`compound_namespace.c`, `compound_data_io.c`, etc.) and
  one of the existing handlers as a template.  Add the op to the XDR codec
  in `src/common/xdr_codec.c` and the result union in `include/compound.h`.
- **A new catalogue table.**  Add a vtable entry in
  `include/catalogue_internal.h`, implement it for memdb (test path) and
  RonDB shim, expose a typed wrapper in `include/mds_catalogue.h`, and add
  a unit test against memdb.
- **A new background subsystem.**  Mirror `ds_gc.c`: a coordinator + worker
  pool, a stop-pipe + condvar shutdown sequence, INI keys with documented
  defaults in the man page, and a unit test driving the memdb backend.
- **A new pNFS layout type.**  Extend `layout_types.h`, teach
  `compound_layout.c` to mint and return the new layout, and provide a
  device-info encoder in `xdr_codec.c`.
## 15. HPC-Shared file mode (N-to-1 wide stripe)
Lattice ships an opt-in per-inode mode for the N-to-1 HPC workload
pattern (many compute clients writing into the same file at
distinct byte ranges).  When the `MDS_IFLAG_HPC_SHARED` bit is set,
LAYOUTGET emits a wide stripe geometry, prefers RDMA / GPUDirect DSes
for placement, and (in deferred phases) bypasses the per-client
layout grant and aggregates LAYOUTCOMMITs.
Files without the bit set continue to use the legacy paths
bit-for-bit unchanged.  Operator-facing surface — triggers, tunables,
deferred phases, caveats — is in `hpc-shared-files.md`.  The full
phase plan and design rationale is in `hpc-nto1-plan.md`.
### Atomic wide create and legacy recovery
HPC wide CREATE allocates a fileid, preallocates and captures every DS file
handle, then commits the child inode, insert-only parent dirent, complete
stripe map, and parent change/timestamp update in one RonDB transaction.
No new wide create sets `MDS_IFLAG_HPC_CREATE_PENDING`; a failed catalogue
commit leaves no reachable namespace metadata and queues the captured DS
handles for cleanup.

`MDS_IFLAG_HPC_CREATE_PENDING` remains a compatibility marker for rows left
by earlier releases that committed these pieces separately.  Recovery runs
lazily on every NFS inode and directory lookup that encounters a marked row:
a valid complete map is promoted by clearing the marker; incomplete metadata
is removed and any captured DS handles are queued for cleanup.  Legacy rows
are therefore never exposed as partially initialized files.  An optional
startup sweep (`hpc_pending_recovery_scan = true`, default off) additionally
walks the whole namespace to reclaim marked rows nobody looks up; because it
is a full catalogue walk, enable it once after upgrading from an affected
release rather than permanently.

Rolling upgrades are protected by a reap grace window: an incomplete marked
row younger than the window (300 s) is hidden, not reaped, because it may be
an in-flight create on a peer MDS still running the pre-atomic release.
Complete maps are promoted immediately regardless of age; a crashed create
becomes reapable once it ages past the window.
### Sparse proxy reads and logical EOF
When client-direct layouts are unavailable, including with
`serve_layouts=false`, READ, READ_PLUS, and copy fallback use the inode's
authoritative logical size rather than any individual DS file length.  A
request is clamped to that size; an in-range short successful DS read is a
sparse hole, so the MDS zero-fills the rest of the stripe unit and continues
through later stripe units.  Global EOF is reported only at logical EOF.

READ_PLUS walks the same logical stripe sequence when locating data or holes.
This prevents an empty sparse DS object from hiding later-stripe data, while
read-open and read-I/O failures still use configured mirror failover.
### Effective placement geometry
Placement callers allocate for their requested stripe and mirror geometry, but
persist and operate only on placement's effective stripe count.  The shared
geometry validator bounds the effective stripe-map entry count and prevents
degraded placement from creating zero-initialized phantom entries; DS ID zero
is a valid placement and is never used as an unfilled-entry sentinel.

Wide preallocation first filters ONLINE DSes by required mode and transport,
then selects a deterministic preferred transport/capability subset when one is
available.  It applies strict placement checks to that eligible set, returns
the effective geometry, and interprets entries in stripe-major order:
`entry_index / mirror_count` is the stripe and
`entry_index % mirror_count` is the mirror.  Inline promotion uses the same
effective geometry before creating DS files and persisting its stripe map.
It splits existing inline payloads at logical stripe-unit boundaries, writes
each segment at its sparse logical offset, and copies that segment to every
mirror of the selected stripe before the metadata flip makes the DS layout
visible.
### Create placement and final-unlink fencing
The fused CREATE plus LAYOUTGET path consumes one preallocation entry and
derives the stripe map and layout grant DS from that same entry.  The
commit-queue variant reads the just-persisted single-stripe map before it
records a layout pregrant; if the map is unavailable, the following
LAYOUTGET uses its normal path rather than granting speculative placement.

Stripe-map cleanup is header-bounded: deletion obtains the stored geometry
inside the RonDB transaction, so single-stripe files do not issue deletes for
every possible stripe index.  Final `REMOVE` continues to fence each known DS
file synchronously before the namespace mutation replies.  This remains true
when transient protocol state is enabled: the mode skips only persisted
layout-recall enumeration, never the DS fence needed for a non-cooperating
client.

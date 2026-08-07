# Changelog

All notable changes to this project are documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- **Build hygiene knobs** — `ENABLE_RELEASE_ASSERTS` (default ON keeps
  the historical `-UNDEBUG`; OFF makes the assertion cost measurable),
  `CMAKE_BUILD_TYPE` defaults to Release when unset (a bare cmake
  invocation can no longer produce a silent -O0 tree), and opt-in
  `ENABLE_LTO` / `ENABLE_NATIVE_ARCH` codegen knobs (both OFF).
- **mk/rm scale benchmark** — `bench_mk_rm_scale` (tests/integration)
  measures create, synchronous-remove, and delete-at-ack (ack-path)
  throughput at 1/4/8/16 threads in shared-directory vs
  directory-per-thread modes, directly exposing the parent-row
  serialisation wall at the catalogue API level.  Runs against memdb
  (CI smoke) or a live RonDB cluster (`--rondb CONF`).
- **`scripts/mds-metrics-diff`** — read-only helper that snapshots the
  Prometheus `/metrics` endpoint before/after a workload burst and
  prints the largest counter and histogram `_sum`/`_count` deltas;
  the fastest way to attribute burst milliseconds to a code region.
- **`rpc_listener_threads` config key** — makes the TCP RPC listener
  (SO_REUSEPORT epoll loop) count operator-tunable (range 0..32).
  `0` (the default) keeps the historical auto rule
  `min(worker_threads, 4)`; explicit values are clamped to online
  CPUs and to the compile-time maximum (32).  At `nconnect=8/16` four
  listeners can be the binding constraint before worker count.
  Rendered by `mds-admin config show`.
- **Async NDB write pipeline (Phase 4)** — setting
  `ndb_async_writes = true` now routes single-commit creates
  (`ns_create` and the fused create+layout) through the
  per-connection async batch pipeline (`executeAsynchPrepare` +
  `sendPreparedTransactions` + `pollNdb` driven by a flush thread),
  so concurrent worker threads share NDB send/poll cycles instead of
  serializing one `execute(Commit)` round trip each.  Per-request
  semantics are unchanged: each caller still blocks until its own
  transaction commits, and error mapping (EXISTS / retryable /
  permanent) is identical to the synchronous path.

### Changed
- **Wave-2 heap scratch**: the compound op/result unions no longer
  inline worst-case payloads.  READ, READ_PLUS, GETXATTR, LISTXATTRS
  and the READDIR page arrays (results), plus WRITE, WRITE_SAME and
  SETXATTR bytes (op arguments), live in per-slot scratch blocks
  OUTSIDE the unions, allocated grow-once at the protocol maximum and
  reused for the worker's lifetime.  `sizeof(struct nfs4_result)`
  drops 524,496 -> 12,928 B and `sizeof(struct nfs4_op)` 65,808 ->
  4,416 B; static per-worker slot scratch falls ~37.8 MB -> ~1.11 MB.
- The per-op full-union memset in `compound_process` (524 KB per op,
  ~2.6 MB cleared per five-op compound) is replaced by
  `nfs4_result_reset()`, which zeroes only the incoming op's union arm
  and nothing at all for status-only ops.  `nfs4_result_destroy()`
  still runs first, and the fresh-thread zero-init guarantee is
  preserved (both thread-local slot arrays are calloc'd).
- `compound_init()` clears ~1.7 KB instead of ~9.8 KB per request: the
  two 4 KB path buffers moved to the struct tail (layout pinned by
  `_Static_assert`s) and are emptied by a single NUL byte each.
- The stripe-entry serialisation buffers in `stripe_map_get` and the
  fused LAYOUTGET (~136 KiB / ~544 KiB worst case) now come from a
  grow-once thread-local scratch instead of a per-call `malloc`/`free`.
  Both sizes exceeded glibc's 128 KiB mmap threshold, so every call
  previously paid an mmap + page-fault + munmap cycle on the LAYOUTGET
  hot path.  Buffers are released at thread exit via a pthread key
  destructor.
- CREATE placement is now pop-once on the live path: the pre-create
  `ds_prealloc_peek` was removed and every per-compound stripe-cache
  fill derives from the entry the fused create actually popped
  (including its stripe unit, surfaced via a new out-parameter).  The
  peek remains only in the commit-queue pregrant branch, which is
  test-only under the RonDB daemon (`cq` is pinned NULL) and documented
  as such.  Saves one prealloc ring-mutex acquisition per CREATE and
  removes the last two-source placement pattern.
- Logging level checks moved from inside `mds_log()` into the
  `MDS_LOG_*` macros: a suppressed DEBUG/TRACE call site now costs one
  relaxed atomic load and a predicted branch instead of a varargs
  function call.  Runtime level changes via `mds_log_set_level()`
  remain thread-safe (atomic stores paired with the macro loads).
- The RPC threadpool's queue-wait sampling (two `clock_gettime` calls
  per work item plus a histogram observation) is now gated on the
  existing `metrics_op_enabled` switch; with op metrics disabled the
  dispatch path performs no clock reads.  The plain dispatcher
  counters (submitted/completed/queue-full totals, active workers,
  queue depth) stay always-on.
- Per-connection NDB flush threads are now created lazily, only when
  `ndb_async_writes = true` is set at startup.  With the flag off
  (the default) no flush threads exist, removing the idle
  send/poll cycle that previously ran every 10 ms per connection.
  Armed flush threads also skip NDB API calls entirely while no
  transaction is in flight.

### Fixed
- The fused CREATE+layout path now persists the popped prealloc
  entry's configured stripe unit in the durable stripe-map header.  It
  previously hardcoded 65536 (despite a comment claiming otherwise), a
  latent geometry mismatch whenever `stripe_unit_bytes` was configured
  differently (e.g. 1 MiB lab profiles).
- README and configuration docs no longer overstate the cache
  subsystem: the inode cache is a global LRU under a single mutex
  (not striped) and both the inode and dirent caches default to
  disabled (`inode_cache_size = 0`, `dirent_cache_size = 0`).  The
  stale 16384/32768 defaults in `docs/config-keys.md` and
  `mds.conf(5)` were corrected to match the code.
- `test_config` now actually fails when an assertion fails: the
  runner previously counted every test as passed because assertion
  macros only printed and returned.  This had been masking a stale
  assertion that still expected the old 32768 dirent-cache default;
  the assertion was updated to the shipped default (0, disabled).

## [v0.1.1-community] — 2026-05-02

### Added
- **`showmount -e` compatibility responder** — a tiny ONC-RPC listener
  on UDP and TCP (default port `20048`) that answers program `100005`
  (mountd) v3 procedures `NULL`, `EXPORT`, and `DUMP` with a
  synthetic, MDS-defined export list. Every other procedure —
  including `MNT` — is rejected with `PROC_UNAVAIL` at the RPC layer,
  so the MDS still cannot be NFSv3-mounted through this shim. No DS
  interaction; the export strings are operator-controlled. See
  `docs/mountd-compat.md` for the full design and threat model.
- New `mds.conf` keys (all optional):
  `mountd_compat_enabled`, `mountd_compat_port`,
  `mountd_compat_bind_addr`, `mountd_compat_register_rpcbind`,
  `mountd_compat_exports`.

### Changed (upgrade behaviour)
- The shim is **enabled by default**. On first restart after upgrade,
  the daemon will additionally:
  - Bind UDP and TCP on `0.0.0.0:20048` (IANA mountd port).
  - Register `100005/3` with the local rpcbind via `PMAPPROC_SET`.
  - Log `INFO: mountd_compat: listening on ...` at startup.

  Operators monitoring listening ports or rpcbind registrations will
  see one new entry per MDS. NFSv4.1 / pNFS service on port 2049 is
  unaffected. To suppress the new listener entirely on a given host,
  set `mountd_compat_enabled = false` in `mds.conf`.

### Notes
Failure modes for the new responder are soft and non-service-affecting:
- Port 20048 already taken (e.g. host co-located with nfs-utils
  `rpc.mountd`): logs `WARN: mountd_compat: TCP bind ... failed`;
  daemon continues serving NFSv4.
- rpcbind not running: logs `WARN: rpcbind registration failed`;
  daemon continues serving. `showmount -e` cannot find the port via
  portmap until rpcbind is started, but direct-port clients still
  work.

## [v0.1.0-community] — 2026-04-29

Initial community release of pnfs-mds.

### Highlights
- NFSv4.1 / pNFS metadata server with flex-files layouts.
- Multi-MDS topology with referrals and partition map.
- RonDB-backed catalogue (distributed, multi-node).
- Inline-data acceleration for small files.
- Inode + dirent caches with negative-entry TTL.
- DS health monitoring, capacity probe, and round-robin placement.
- Module-extracted architecture: `resilver`, `rebalance`, `tiering`,
  `observability`, `replication`, `layout_cache`,
  `layout_commit_aggregator`, `ds_gc`, `ds_prealloc`, `wrr`, `quota`
  are all built as optional modules with public stubs in this
  edition; enable a real implementation by toggling the
  corresponding `ENABLE_<NAME>` CMake flag and supplying the source.
- Configuration via `/etc/pnfs-mds/mds.conf` (INI-style); see
  `docs/config-keys.md` and `mds.conf(5)`.

### Licensing
- MIT for the bulk of the source (see `LICENSE-MIT`).
- GPL-2.0 for `src/catalogue/catalogue_rondb_shim.cpp` (see
  `LICENSE-GPL-2.0`) — required because that file links against
  RonDB / NDB API headers, which are GPL-2.0.
- See `LICENSING.md` for the per-file rationale.

# Changelog

All notable changes to this project are documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
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

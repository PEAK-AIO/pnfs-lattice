# Changelog

All notable changes to this project are documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- **Companion processes** — a new `companion` feature module
  (`src/modules/companion/`, gated by `ENABLE_COMPANION`, default ON)
  that lets the daemon spawn, supervise, restart, and terminate
  operator-declared helper programs alongside itself. Each child runs
  in its own process group, with an optional `setrlimit` fence
  (`RLIMIT_AS` / `RLIMIT_NOFILE` / `RLIMIT_CPU`) and an optional
  transient systemd scope carrying `MemoryMax`. Restart policy is
  `never | on-failure | always` with exponential backoff, a capped
  failure burst, and a counter that resets after stable uptime. See
  `docs/companion-processes.md`.
- Companions are declared **only** in `mds.conf` via
  `companion.<name>.*` keys, which act as the allowlist: `exec`,
  `arg[N]`, `workdir`, `log_file`, `enabled`, `autostart`, `restart`,
  `restart_backoff_ms`, `restart_backoff_max_ms`, `max_restarts`,
  `restart_reset_sec`, `stop_timeout_ms`, `start_delay_ms`,
  `rlimit_as_mb`, `rlimit_nofile`, `rlimit_cpu_sec`, `systemd_scope`,
  `memory_max_mb`, `ndb_conns`. Plus the node-wide
  `companion_enabled`, `ndb_api_slots_total`,
  `ndb_api_slots_reserved`, and `companion_ndb_admission`.
- **Advisory RonDB `[api]` slot budget** — companions declare the NDB
  connections they will open, and `mds-admin companion budget` reports
  declared total, this daemon's pool, reserved headroom, and declared
  versus running companion slots. The free figure is signed so
  oversubscription is visible instead of clamped.
  `companion_ndb_admission = enforce` refuses a spawn that would
  exceed the declared total; the default `advisory` warns and proceeds.
- New `mds-admin companion` subcommands: `list`, `status`, `start`,
  `stop`, `restart`, and `budget` (with a display-only
  `--total-api-slots` what-if override), carried over the existing
  cluster-transport admin endpoint as message types 89–94.

### Security
- The admin control path transmits an action plus a **declared
  companion name** only — never an executable path, argument vector,
  or resource limit. An operator with admin-port access can start and
  stop pre-approved programs and nothing else, so the feature adds no
  remote code-execution surface. Programs are launched with `execv()`
  against an absolute path: no shell, no `PATH` lookup, and one config
  key per argument, so quoting and globbing semantics do not exist.
- Spawned children have their signal mask cleared (the daemon blocks
  `SIGINT`/`SIGTERM` in every thread, which a child would otherwise
  inherit) and every inherited descriptor above stderr closed, so a
  companion cannot hold catalogue connections, listening sockets, TLS
  state, or the daemon log open.

### Notes
- **Inert by default.** With no `companion.<name>.*` keys declared, no
  supervisor thread is created and no process is spawned, so an
  upgrade changes nothing until an operator opts in.
- Adding or changing a companion requires editing `mds.conf` and
  restarting the daemon; there is no reload path and no way to launch
  an ad-hoc command.
- A structurally invalid declaration (relative `exec`, `workdir`, or
  `log_file`; missing `exec`; a gap in `arg[N]`; `systemd_scope`
  without `memory_max_mb`) now aborts daemon startup with an error
  naming the key, rather than failing later at spawn time.
- Slot budgeting is advisory accounting, not a guarantee: the daemon
  cannot observe a child's real NDB connection count, and the view is
  per node — peer MDS daemons and their companions are invisible, so
  `ndb_api_slots_reserved` exists to reserve headroom for them.

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

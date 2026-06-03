# pnfs-mds configuration keys
Reference for every INI key parsed by `mds_config_load()` in
`src/common/config.c`.  Keys are grouped by subsystem.  Default
column shows the value applied when the key is absent.  Range
column shows operator-visible validation; out-of-range values
are logged as `WARN:` and the default is kept.
## Identity and cluster membership
- `mds_id` — unique node id (1..UINT32_MAX).  Default: 1.
- `hostname` — this node's network name.  Default: `localhost`.
- `nfs_port` — port for the NFSv4.1 listener (1..65535).  Default: 2049.
- `grpc_port` — port for the cluster-transport listener (1..65535).  Default: 50051.
- `cluster_size` — expected number of MDS nodes.  Informational.
- `cluster_bind_addr` — inter-MDS bind address.  Default: `127.0.0.1`.
- `cluster_max_conns` — max peer conns (1..256).  Default: 16.
- `cluster_peer[N]` — ACL entry at index N.
- `cluster_ca_file` / `node_cert_file` / `node_key_file` — cluster TLS material.
- `require_mtls` — bool; require peer mTLS.  Default: false.
## Catalogue
- `catalogue_backend` — `rondb` (only option).  Default: rondb.
- `catalogue_backend_conf` — backend-specific config path.
- `catalog_image_mode` — `off|shadow|compare|primary`.  Default: off.
- `catalog_compare_reads` — enable image-vs-authority compare reads.  Default: false.
- `catalog_replay_mode` — `off|log|journal`.  Default: off.
- `catalog_replay_snapshot_path`
- `catalog_replay_rebuild_on_start`
- `catalog_delta_log_path`
- `ndb_conn_pool_size` — NDB connections per MDS (1..32).  Default: auto.
- `ndb_async_writes` — bool; use async NDB batch path.  Default: false.
- `transient_state_cache` — bool; skip NDB write-through for open/layout state.  Default: true.
## Replication / failover
- `repl_mode` — `sync|async|semi_sync`.
- `standby_host` / `standby_port` — standby target.
- `repl_listen_port` — incoming replica port.  Default: 9401.
- `repl_semi_sync_n` — min acks for SEMI_SYNC.
- `repl_health_interval_ms`
- `repl_refuse_writes_on_resync` — bool.
- `self_role` — 0=ACTIVE, 1=STANDBY.
- `self_failover_partner_id` — paired partner mds_id.
## Workload / tuning
- `workload_profile` — `default|hpc|ai_training|genomics|media`.
- `worker_threads` — COMPOUND dispatch thread count.  Default: 16.
- `stripe_unit_bytes` — default stripe unit.  Default: 65536.
- `default_stripe_count` / `default_mirror_count` — geometry for new files.  Default: 1 / 1.
- `lease_time_sec` / `grace_period_sec` — NFSv4 lease + grace.
- `gpudirect_required` — bool.
- `inline_enabled` — inline-data acceleration.  Default: true.
- `inline_max_size` — max bytes stored inline (1..65536).  Default: 65536.
## Commit pipeline
- `CommitBatchSize`, `CommitBatchMaxBytes`, `CommitFlushMs`, `CommitQueueDepth` — single-writer batch commit knobs.
## Caches
- `inode_cache_size` (0..1000000).  Default: 16384.
- `dirent_cache_size` (0..1000000).  Default: 32768.
- `negative_cache_ttl_ms` (0..3600000).  Default: 5000.
## Data servers
- `ds_count` — number of configured DSes.
- `ds[N]` — `host:/export` spec for DS index N.
- `ds_mount_path_fmt` — printf format with exactly one `%u` for mount paths.  Default: `/mnt/ds%u`.
- `ds_heartbeat_ms` — DS probe interval.  Default: 5000.
- `ds_health_fail_threshold` — consecutive failures before OFFLINE (1..1024).  Default: 6.
- `ds_weight.<id>` — per-DS WRR weight (any uint32).  Default: 0 (unset ⇒ free-bytes fallback).
- `ds_capacity_poll_ms` — statvfs() sweep interval (0..86400000).  Default: 60000.  0 disables.
- `ds_prepare_queue_depth` (0..65536), `ds_prepare_workers` (0..64).
## Placement
- `placement_policy` — `rr|wrr|weighted_rr|capacity`.  Default: rr.
- `placement_policy_enabled` — master switch.  Default: false.
- `placement_capacity_weighting` — `off|proportional`.  Default: off.  When `proportional`, the statvfs probe derives `auto_weight = max(1, floor((1 - used/total) * 100))` in [1, 100] and writes it into the DS cache.  Overlay precedence: `ds_weight.<id>` > `auto_weight` > free-bytes > uniform, so an operator override always wins.  Visible as the `AUTO` column in `mds-admin ds capacity show`.
## Authentication
- `nfs_auth_mode` — `sys|krb5|krb5i|krb5p`.
- `krb5_keytab` / `krb5_principal` — GSS credentials.
## Directory delegations
- `dir_delegations_enabled` — master switch.  Default: false.
- `dir_deleg_recall_timeout_ms` — default CB_RECALL / CB_NOTIFY timeout (50..300000).  Default: 5000.  Scales the in-flight dedupe window.
## Callback channel
- `cb_recall_timeout_ms` — default for CB_RECALL / CB_LAYOUTRECALL / CB_NOTIFY when callers pass 0 (50..300000).  Default: 5000.
## Observability
- `metrics_http_port` — Prometheus scrape port (0..65535).  Default: 9090.  0 disables the endpoint.
## `showmount -e` compatibility (mountd_compat)
A tiny ONC-RPC responder that answers `showmount -e <mds>` with a
synthetic, MDS-defined export list.  **Enabled by default** (since
v0.1.0+mountd-compat).  Never proxies to any DS and never implements
NFSv3 MOUNT — the MNT procedure is rejected at the RPC layer with
`PROC_UNAVAIL`, so it is impossible to NFSv3-mount the MDS through
this shim.  See `docs/mountd-compat.md` for the full design and the
upgrade-path notes (new listening port + rpcbind entry on existing
hosts).
- `mountd_compat_enabled` — master switch.  Default: **true**.  Set to `false` to suppress the listener entirely (no port bound, no rpcbind entry).
- `mountd_compat_port` — UDP+TCP port (0..65535).  Default: 20048 (IANA mountd).  `0` lets the OS pick an ephemeral port.
- `mountd_compat_bind_addr` — bind address.  Default: `0.0.0.0`.
- `mountd_compat_register_rpcbind` — register `100005/3 → port` with the local rpcbind on startup so `showmount -e` can discover the port via portmap on 111.  Default: true.  Requires rpcbind running on the host.
- `mountd_compat_exports` — comma-separated list of synthetic export paths.  Up to 16 entries, each ≤ 255 bytes.  Default: `/`.  Example: `mountd_compat_exports = /pnfs, /scratch`.
## Auto-split
- `auto_split_enabled` / `auto_split_execute` — bool gates.
- `auto_split_threshold` — ops/interval to propose.  Default: 10000.
- `auto_split_interval` — eval cadence in seconds.  Default: 300.
- `auto_split_cooldown` — min seconds between re-splits.  Default: 600.
- `auto_split_sustained` — consecutive hot intervals.  Default: 2.
- `auto_split_min_children` — min children eligible.  Default: 4.
## Sharding
- `shard_enabled` — bool master switch.  Default: false.
- `hide_referral_junctions` — bool.  Default: false.  Cosmetic only.  When true, the `/shardN` referral junction directories are omitted from READDIR replies at the namespace **root only**.  `LOOKUP` still resolves them (so `cd /mnt/pnfs/shardN` works); this just hides them from `ls /mnt/pnfs`.  Hiding is an exact subtree-map match, so ordinary files and directories are never affected.  Caveat: tools that enumerate the root (`find`, `rsync`, `rm -rf /mnt/pnfs`, backup) will not descend into the hidden shards.
## Logging
The daemon routes diagnostics through a leveled, component-aware logger (`src/common/log.c`).  Output defaults to stderr at `info`, which reproduces the historical behaviour (every pre-existing diagnostic is emitted at `info` or above).
- `log_file` — path for diagnostics output.  Empty/unset → stderr.  A path is opened in **append** mode; if it cannot be opened the logger falls back to stderr.  Each record carries a UTC timestamp, component, and level.
- `log_level` — global verbosity applied to every component.  One of `fatal`, `error`, `warn`, `info` (default), `debug`, `trace` (case-insensitive).  A component emits a record only when its level is at or above the record's severity (e.g. `warn` passes fatal/error/warn and drops info/debug/trace).
- `log_level.<component>` — per-component override.  `<component>` is one of `mds`, `fsal`, `cluster`, `repl`, `cat`, `bpf`, `nfs` (case-insensitive).  Components without an override inherit `log_level`.  Example: `log_level.cat = debug`.
Unknown level or component tokens are warned about and ignored (the default is kept).
## What is not (yet) in config
These knobs exist as hardcoded constants and can be promoted on request:
- `DS_HEALTH_DEFAULT_INTERVAL` — alias for `ds_heartbeat_ms` today.
- `DS_HEALTH_COOLDOWN_BASE_MS` / `DS_HEALTH_COOLDOWN_CAP_MS` / `DS_HEALTH_FLAP_BACKOFF_MAX` — flap suppression.
- `DS_HEALTH_RECOVERY_MIN` — consecutive OK probes to mark ONLINE.
- `DELEG_STRIPE_COUNT` / `DDT_STRIPE_COUNT` — striped-lock width.
- Inode / dirent-cache shard width.
- Callback XDR buffer size (`CB_MAX_MSG_SIZE`, 4096 bytes).
If any of these becomes operationally relevant, add the field to
`struct mds_config`, the parser to `config.c`, and a row here.

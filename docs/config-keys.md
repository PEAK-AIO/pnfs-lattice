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
- `admin_allowed_hosts` — comma-separated IPv4 addresses (up to 32, no CIDR) allowed to connect to the admin transport in addition to `cluster_peer[N]`, so monitoring / UI hosts need not be cluster members.  Default: empty.
## Catalogue
- `catalogue_backend` — `rondb|memdb|fdb`.  Default: rondb when compiled in, otherwise mandatory; a known backend that is not compiled in is refused at startup.
- `catalogue_backend_conf` — backend-specific config path (rondb).
- `fdb_cluster_file` — FoundationDB cluster file (fdb).  Default: `FDB_CLUSTER_FILE`, then `/etc/foundationdb/fdb.cluster`.
- `fdb_key_prefix` — key prefix of this catalogue inside the cluster, at most 31 bytes (fdb).  Default: empty.
- `fdb_op_deadline_ms` — per-operation budget across attempts, 1..600000 (fdb).  Default: 8000.
- `fdb_txn_timeout_ms` — per-attempt transaction timeout, 1..4900 (fdb).  Default: 4000.
- `catalog_image_mode` — `off|shadow|compare|primary`.  Default: off.
- `catalog_compare_reads` — enable image-vs-authority compare reads.  Default: false.
- `catalog_replay_mode` — `off|log|journal`.  Default: off.
- `catalog_replay_snapshot_path`
- `catalog_replay_rebuild_on_start`
- `catalog_delta_log_path`
- `ndb_conn_pool_size` — NDB connections per MDS (1..64).  Default: auto.
- `ndb_async_writes` — bool; use async NDB batch path.  Default: false.
- `transient_state_cache` — bool; skip NDB write-through for open/layout state (in-memory only, single-MDS lab use).  Default: false.
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
- `rpc_listener_threads` — TCP RPC listener (SO_REUSEPORT) count (0..32).  Default: 0 = auto, the historical rule `min(worker_threads, 4)`.  Explicit values are clamped to online CPUs and to the compile-time maximum (32).  Raise when a high-`nconnect` bandwidth sweep shows listener saturation.
- `max_inflight_per_conn` — max COMPOUNDs processed concurrently per TCP connection by the worker pool (1..1024).  Default: 0 = 8.  Raise for clients with few connections but many session slots.
- `session_fore_slots` — fore-channel slot cap offered in CREATE_SESSION (`ca_maxrequests`, RFC 8881 §18.36.4) (1..512).  Default: 0 = 64.  One slot admits one in-flight COMPOUND per client.
- `stripe_unit_bytes` — default stripe unit in bytes (1..1073741824).  Default: 65536.  `stripe_unit` is accepted as an alias.
- `default_stripe_count` / `default_mirror_count` — geometry for new files.  Default: 1 / 1.
- `lease_time_sec` / `grace_period_sec` — NFSv4 lease + grace.
- `gpudirect_required` — bool.
- `serve_layouts` — bool.  Default: true.  Master switch for client-direct pNFS layouts; when false every LAYOUTGET returns `LAYOUTUNAVAILABLE` and clients fall back to READ/WRITE through the MDS proxy.
- `layout_grant_max_length_bytes` — cap on the byte range of one LAYOUTGET grant (uint64; values below 65536 are raised to 65536).  Default: 68719476736 (64 GiB).  Bounds the wire response and the persisted row so recalls can be byte-ranged; clients re-issue LAYOUTGET past the window.
- `layoutget_newfile_fastpath` — bool.  Default: **false**.  Skip the LAYOUTGET byte-range conflict-recall holder scan when the target file was created earlier in the **same compound** (fused OPEN(CREATE)+LAYOUTGET).  A fileid that did not exist before the request cannot have layout holders, so the scan is a guaranteed-miss catalogue round-trip on the create hot path.  Pre-existing files always keep the full scan + recall behaviour regardless of this switch.  Skipped scans are counted in `pnfs_mds_layoutget_newfile_scan_skipped`.
- `stripe_lease_duration_ms` — stripe lease duration (0..300000).  Default: 30000.  When non-zero, grants carry `FF_FLAGS_STRIPE_LEASE` and the MDS enforces per-(fileid, range) leases so concurrent clients on the same stripe wait or retry; 0 disables.
- `auto_widen_lease_on_4k` — bool.  Default: true.  When a LAYOUTGET carries `loga_minlength = 4096` with an unbounded `loga_length` (the Linux page-cache writeback pattern), widen the stripe lease to the whole granted range instead of one page; see `compound_layout.c` (lease-scope block in `op_layoutget`).
- `prealloc_pool_size` — DS pre-allocation placements cached per pool (uint32).  Default: 128.
- `prealloc_ring_count` — pre-allocation refill rings / workers (0..64).  Default: 0 = engine default.
- `inline_enabled` — inline-data acceleration.  Default: false (the `hpc`, `ai_training`, `genomics` and `media` profiles set it true; the RonDB backend refuses `true`).
- `inline_max_size` — max bytes stored inline (1..65536).  Default: 65536.
## HPC-Shared (wide-stripe) files
Operator surface for the per-inode N-to-1 mode described in `hpc-shared-files.md`.
- `hpc_max_stripe_count` — cap on `stripe_count` for HPC-Shared creates regardless of ONLINE DS count (1..1024, the compile-time `MDS_MAX_STRIPES`).  Default: 128.  An out-of-range value is a fatal configuration error, not a warning.
- `hpc_serve_layouts` — bool.  Default: false.  Serve pNFS layouts for HPC-Shared inodes; off answers their LAYOUTGET with `LAYOUTUNAVAILABLE` (MDS proxy I/O).  Turn on only when every client runs Linux 6.18+ (multi-DS-per-mirror flex-files); older clients treat the stripes as mirrors and corrupt data.  Independent of `serve_layouts`, which must also be on.
- `hpc_pending_recovery_scan` — bool.  Default: false.  Run the start-up scan that repairs PENDING wide-create inodes left by pre-atomic releases (namespace rows persisted separately from the stripe map).  The scan walks the whole namespace from the root before requests are accepted, so enable it once after upgrading from an affected release; lazy lookup-time recovery is always active regardless.
- `hpc_xdr_form` — `auto|legacy|striped`.  Default: auto (multi-DS-per-mirror form only for HPC-Shared inodes with `mirror_count == 1` and `stripe_count > 1`; `legacy` forces one-DS-per-mirror, `striped` forces multi-DS-per-mirror).  Any other token is a fatal configuration error.
## Commit pipeline
- `CommitBatchSize`, `CommitBatchMaxBytes`, `CommitFlushMs`, `CommitQueueDepth` — single-writer batch commit knobs.
## Caches
- `inode_cache_size` (0..1000000).  Default: 0 (disabled; set e.g. 16384 to enable the previous lab default).
- `dirent_cache_size` (0..1000000).  Default: 0 (disabled; set e.g. 32768 to enable the previous lab default).
- `negative_cache_ttl_ms` (0..3600000).  Default: 5000.
- `positive_cache_ttl_ms` — TTL of positive inode / dirent cache entries (0..3600000).  Default: 0 = unset: unbounded on a single MDS, 1000 ms when `cluster_size > 1` (cross-MDS coherence bound); an explicit non-zero value always wins.
## Deferred mutation paths
- `parent_touch_deferred` — bool.  Default: false.  Aggregate parent-directory attribute updates (change / mtime / ctime) in memory and persist them periodically instead of inside every namespace transaction.  Requires a backend that implements `ns_parent_touch`; otherwise a warning is logged and the synchronous path stays.
- `parent_touch_flush_ms` — flush interval of the aggregator (1..60000).  Default: 50.  Out-of-range values are ignored.
- `parent_touch_max_dirs` — directories tracked by the aggregator (16..1048576).  Default: 4096.  Out-of-range values are ignored.
- `remove_async` — bool.  Default: false.  Acknowledge eligible REMOVEs after an in-memory tombstone plus one durable pending-delete row; a drainer performs the real remove off the request thread (delete-at-ack).  Requires `parent_touch_deferred = true`; otherwise a warning is logged and REMOVE stays synchronous.  See `include/remove_manifest.h`.
- `remove_async_batch` — drainer claim batch (1..4096).  Default: 128.
- `remove_async_workers` — drainer worker threads (1..32).  Default: 4.
- `remove_async_poll_ms` — drainer poll interval (10..60000).  Default: 200.
- `remove_async_claim_ttl_ms` — ownership lease on claimed pending rows (1000..600000).  Default: 30000.
Out-of-range `remove_async_*` values are ignored without a warning.
## Protocol state tables
Sizing knobs for the in-memory NFSv4.1 state tables (Wave 4).  All default to 0 = built-in default; the effective open-state sizing is logged at startup.
- `open_state_file_buckets` (256..16777216).  Default: 1048576.  Per-file open-chain hash buckets.
- `open_state_stateid_buckets` (256..16777216).  Default: 1048576.  Stateid hash buckets.
- `open_state_lock_stripes` (16..4096).  Default: 1024.  Mutex/rwlock stripes over the open-state tables; clamped to the bucket counts.  The pre-Wave-4 value (16) serialised 1/16 of the fileid space behind each OPEN's synchronous state persist.
- `session_client_buckets` / `session_session_buckets` / `session_owner_buckets` (256..1048576).  Default: 65536 each.  Session-table hash buckets (clientid / session-id / co_ownerid).  The session stripe-lock count is intentionally fixed at 16 — every unhash path takes all stripes, so the protocol cost scales with stripe count while client cardinality stays low.
## Data servers
- `ds_count` — number of configured DSes.
- `ds[N]` — `host:/export` spec for DS index N.
- `ds_mount_path_fmt` — printf format with exactly one `%u` for mount paths.  Default: `/mnt/ds%u`.
- `ds_fh_format` — `opaque|knfsd`.  Default: **opaque**.  Validation of DS server file handles captured on the `name_to_handle_at()` fast path.  RFC 8435 treats DS filehandles as opaque (flex-files layouts hand them to clients verbatim), so `opaque` — structural checks only — is required for NetApp ONTAP and other non-Linux data servers.  `knfsd` restores the legacy extra check that the first FH byte is Linux knfsd's `0x01` version byte.
- `ds_transport` — `tcp|rdma|both`.  Default: tcp.  Transport advertised in the flex-files device address returned by GETDEVICEINFO; any other token selects tcp.
- `ds_rdma_port` — RDMA port advertised when `ds_transport` is `rdma` or `both` (1..65535).  Default: 20049.
- `ds_heartbeat_ms` — DS probe interval.  Default: 5000.
- `ds_health_fail_threshold` — consecutive failures before OFFLINE (1..1024).  Default: 6.
- `ds_gc_workers` — DS GC drainer worker threads (1..32).  Default: 4.
- `ds_gc_batch_size` — GC queue entries fetched per coordinator tick (1..4096).  Default: 256.
- `ds_synth_owner` — bool.  Default: false.  Store a random synthetic (suid, sgid) per regular file when its DS backing file is created and advertise it in `ffl_user`/`ffl_group`, so LAYOUTGET performs no DS chown (RFC 8435 §2.2); off keeps the legacy owner-aligned chown path.
- `ds_synth_secret_file` — path to a 32-byte key.  Default: empty.  When set, per-(fileid, stripe, mirror) synthetic uids for `ffl_user` and the DS chown are derived with HMAC-SHA256 from this key (RFC 8435 §2.2.1); empty uses the caller's real uid.
- `ds_weight.<id>` — per-DS WRR weight (any uint32).  Default: 0 (unset ⇒ free-bytes fallback).
- `ds_capacity_poll_ms` — statvfs() sweep interval (0..86400000).  Default: 60000.  0 disables.
- `ds_iolimit_probe_ms` — per-DS I/O limit (NFSv3 FSINFO) probe interval (0..86400000).  Default: 60000.  0 disables probing and restores the legacy hardcoded 1 MiB wire constants.  The prober asks each ONLINE generic DS for its real `rtmax`/`wtmax` so GETDEVICEINFO advertises per-DS `ffdv_rsize`/`ffdv_wsize` the DS actually accepts (probed values are capped at 1 MiB and rounded down to 4 KiB; an unprobed DS advertises a safe 64 KiB fallback; a failed probe keeps last-known-good; decoded limits below 4 KiB mark the DS ineligible for new layout placement).  Any effective change bumps the DS's device-ID generation so clients re-fetch device info; a DECREASE additionally recalls the DS's outstanding layouts — after the new values are published, never before.  Observability: `pnfs_mds_ds_iolimit_probe_failures`, `pnfs_mds_ds_iolimit_capability_recalls` (counters), `pnfs_mds_ds_iolimit_min_read`/`_write` (gauges).
- `ds_prepare_queue_depth` (0..65536), `ds_prepare_workers` (0..64).
## Placement
- `placement_policy` — `rr|wrr|weighted_rr|capacity`.  Default: rr.
- `placement_policy_enabled` — master switch.  Default: false.
- `placement_capacity_weighting` — `off|proportional`.  Default: off.  When `proportional`, the statvfs probe derives `auto_weight = max(1, floor((1 - used/total) * 100))` in [1, 100] and writes it into the DS cache.  Overlay precedence: `ds_weight.<id>` > `auto_weight` > free-bytes > uniform, so an operator override always wins.  Visible as the `AUTO` column in `mds-admin ds capacity show`.
## Authentication
- `nfs_auth_mode` — `sys|krb5|krb5i|krb5p`.
- `krb5_keytab` / `krb5_principal` — GSS credentials.
- `posix_dac` — bool.  Default: **false**.  When set to `true`, enforce POSIX permission semantics for AUTH_SYS requests: owner-only chmod/chown/utimes, directory write+search bits for CREATE/REMOVE/RENAME/LINK/OPEN(CREATE), search bits on LOOKUP, the S_ISVTX sticky-deletion rule, root-only device-node creation, and SUID/SGID clearing on chown/truncate/write.  `uid 0` bypasses the permission gates (no root squash).  The default (off) is the historical permissive behaviour where any principal may mutate any object; enable it on multi-user or untrusted clusters.  Non-AUTH_SYS flavors (AUTH_NONE, RPCSEC_GSS) are not subject to these checks because no usable uid/gid mapping exists at this layer.
- `referral_strict` — bool.  Default: **true**.  Enforce the referral topology: an operation on a filehandle whose subtree is owned by another MDS returns `NFS4ERR_MOVED`, forcing the client to re-walk the path and follow the junction referral to the owning MDS.  Ownership is resolved server-side by walking the FH's parent chain to a registered `/shardN` partition root, so cached filehandles presented after a referral submount expires are routed correctly.  Only registered partition subtrees are affected — the unsharded namespace is served by any MDS (single-namespace semantics).  Filehandle/session plumbing ops and `GETATTR` requesting `fs_locations` are exempt (RFC 8881 §8.5.1 referral discovery).  Rejections are counted in the `pnfs_mds_nfs_moved_total` metric.  Set to `false` to restore the historical serve-anywhere behaviour.
## Delegations
- `file_delegations_enabled` — bool.  Default: false.  When true the RPC server is wired with a delegation table and OPEN may grant read/write file delegations (RFC 8881 §10.4); when false OPEN answers `OPEN_DELEGATE_NONE_EXT` / `WND4_NOT_WANTED` and no CB_RECALL traffic is generated.  CB_LAYOUTRECALL is independent of this key.
- `dir_delegations_enabled` — master switch for directory delegations.  Default: false.
- `dir_deleg_recall_timeout_ms` — default CB_RECALL / CB_NOTIFY timeout (50..300000).  Default: 5000.  Scales the in-flight dedupe window.
## Callback channel
- `cb_recall_timeout_ms` — default for CB_RECALL / CB_LAYOUTRECALL / CB_NOTIFY when callers pass 0 (50..300000).  Default: 5000.
## Observability
- `metrics_http_port` — Prometheus scrape port (0..65535).  Default: 9090.  0 disables the endpoint.
- `metrics_op_enabled` — bool.  Default: true.  Master switch for the per-op, per-catalogue-op and per-op×phase latency histograms and the thread pool's queue-wait sampling; false takes them off the hot path (plain counters and gauges stay on).
- `compound_perf_threshold_us` — uint32.  Default: 0 (sampler off).  When > 0, about one in 64 compounds whose wall time exceeds this many microseconds is logged at INFO as `PERF: compound ...`.
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

# Lattice: Elastic Metadata Services for pNFS

**Copyright (c) 2026 PEAK:AIO. Licensed under MIT, except for `src/catalogue/catalogue_rondb_shim.cpp`, which is licensed under GPL-2.0-only. See `LICENSING.md` for details.**

Lattice is an open source, scale out metadata and coordination architecture for NFSv4.2, pNFS Flex Files and future distributed storage services.

Initiated by PEAK:AIO and shaped through technical collaboration with Los Alamos National Laboratory and CMU, Lattice provides a user space metadata server architecture designed for high performance computing, AI/ML storage and large scale namespace workloads.

The project addresses limitations in traditional pNFS metadata designs by separating metadata authority, protocol coordination and data service placement. Multiple MDS daemons can share a RonDB metadata authority, allowing metadata services to scale beyond a single fixed server model while still preserving a clear source of truth.

Primary focus: an elastic, ephemeral, user space pNFS metadata server for high performance computing and AI/ML storage.


## Project Status

Lattice is an early open source release intended for research, development, testing and collaboration.

It is not yet recommended for production use without independent validation, security review and operational testing in your own environment.

At any given time, there may be improvements, fixes and new features under active development in separate branches. These are not part of the released code until they have been reviewed, approved and passed QA.

The software is provided as is, without warranty. Use is at your own risk. See the licence files for the full terms.




## Architecture

```
                  ┌──────────────────────────────────────────┐
                  │        MDS Cluster (active-active)        │
                  │                                          │
                  │  ┌────────┐  ┌────────┐  ┌────────┐     │
                  │  │ MDS-0  │  │ MDS-1  │  │ MDS-N  │     │
                  │  └───┬────┘  └───┬────┘  └───┬────┘     │
                  │      └───────────┼───────────┘           │
                  └──────────────────┼───────────────────────┘
                                     │ NDB C++ API
                       ┌─────────────┴─────────────┐
                       │     RonDB Cluster          │
                       │  (shared metadata store)   │
                       │                            │
                       │  inodes · dirents · stripe │
                       │  ds_registry · node_reg    │
                       │  config_kv · fileid_ctr    │
                       └─────────────┬─────────────┘
                                     │
                  ┌──────────────────┼──────────────────┐
           ┌──────▼──────┐   ┌──────▼──────┐   ┌───────▼──────┐
           │  NFS DS-0   │   │  NFS DS-1   │   │  NFS DS-N    │
           │flex TCP/RDMA│   │flex TCP/RDMA│   │flex TCP/RDMA  │
           └─────────────┘   └─────────────┘   └──────────────┘
```

Every MDS daemon connects to the same RonDB cluster.  All metadata
mutations (create, remove, rename, link, setattr) execute as single NDB
transactions with row-level locking.  There is no per-node local metadata
store in RonDB mode — the cluster is the single source of truth.

Clients reach the correct MDS via NFSv4 `fs_locations` referrals
(NFS4ERR_MOVED).  Data servers are standard unpatched knfsd instances,
registered with TCP and/or RDMA endpoints.

(Additional sharding options in QA and to be added)

### Data Flow

```
1. Client → MDS    OPEN("/data/file")
                   MDS resolves inode in RonDB, returns stateid.

2. Client → MDS    LAYOUTGET(stateid, READ, 0..EOF)
                   MDS returns flex file layout: DS addresses + per-stripe
                   NFS file handles.

3. Client → DS     NFS READ/WRITE directly to data servers (parallel stripes).

4. Client → MDS    LAYOUTRETURN on close or recall.
```

## Metadata Backend

### RonDB (primary)

RonDB (MySQL NDB Cluster) provides the distributed, in-memory metadata
authority.  The NDB C++ API is wrapped behind a narrow C ABI shim
(`catalogue_rondb_shim.cpp`) so the rest of the daemon remains pure C.

Key properties:

- **Atomic parent-inode mutations** — `mkdir`, `rmdir`, `link`, `rename`
  update the parent's nlink, mtime, ctime, and change counter via NDB
  `interpretedUpdateTuple` (`incValue`/`subValue`).  No read-modify-write
  race even with multiple concurrent MDS daemons.
- **Serialized setattr** — `setattr` acquires an exclusive row lock, reads,
  merges the attribute mask, and writes back in one NDB transaction.
- **Node registration and liveness** — Each MDS registers itself in a
  `node_registry` table at startup and emits heartbeats every 5 seconds.
  Peer daemons detect liveness through the same table.
- **CAS bootstrap** — On first start the daemon creates the schema tables.
  A probe-and-retry loop prevents DDL collisions when multiple daemons
  start simultaneously.
- **Changefeed poller** — Optional: when `catalog_image_mode` is enabled,
  a background thread polls RonDB for recent mutations and feeds them into
  a local catalog image for hot-read acceleration.

Schema tables: `inodes`, `dirents`, `stripe_map`, `layout_state`,
`ds_registry`, `config_kv`, `node_registry`, `fileid_counter`,
`partition_map`.


### Pluggable Catalogue Layer

Backend selection is determined at startup by `catalogue_backend` in the
daemon config.  The catalogue vtable (`catalogue_dispatch.c`) routes all
namespace operations to the active backend.  Adding a new backend requires
implementing the `mds_cat_authority_ops` function table and a factory entry
in `catalogue_factory.c`.

## Multi-MDS Concurrency

Three classes of concurrency bugs were identified during multi-MDS smoke
testing and fixed (Phase 10A):

1. **Parent nlink race** — NDB interpreted updates replace the
   read-modify-write pattern on the parent inode.
2. **Setattr lost-update** — Exclusive row-level lock serializes
   concurrent attribute modifications on the same inode.
3. **Bootstrap DDL collision** — CAS probe-and-retry prevents multiple
   daemons from running schema DDL simultaneously.

Validated on a live RonDB 24.10.19 cluster with 2 MDS daemons:
20 concurrent mkdirs → exact nlink; 20 concurrent rmdirs → exact nlink
reset; zero stale-handle NFS errors; zero crashes.

## NFSv4.1/4.2 Protocol Coverage

### Session and Namespace

EXCHANGE_ID · CREATE_SESSION · DESTROY_SESSION · SEQUENCE ·
PUTROOTFH · PUTFH · LOOKUP · GETATTR · SETATTR · READDIR ·
CREATE · REMOVE · RENAME (cycle detection) · LINK · GETFH ·
SAVEFH · RESTOREFH · ACCESS · OPEN (CLAIM_NULL, CLAIM_FH,
CLAIM_PREVIOUS) · CLOSE · DELEGRETURN · Delegations (READ/WRITE)

### pNFS Layouts

LAYOUTGET · LAYOUTRETURN · LAYOUTCOMMIT · GETDEVICEINFO ·
CB_LAYOUTRECALL · flex file layouts (RFC 8435)

### NFSv4.2 (RFC 7862 — all 13 operations)

ALLOCATE · DEALLOCATE · COPY (sync + async offload) · COPY_NOTIFY ·
OFFLOAD_STATUS · OFFLOAD_CANCEL · SEEK · READ_PLUS · WRITE_SAME ·
CLONE (reflink) · IO_ADVISE · LAYOUTERROR · LAYOUTSTATS

### Proxy I/O

READ · WRITE · COMMIT forwarding for non-pNFS clients.

## Components

| Directory        | Description                                               |
|------------------|-----------------------------------------------------------|
| `src/mds/`       | MDS daemon — compound dispatch, sessions, open/lock state, proxy I/O, copy offload |
| `src/catalogue/` | Metadata backend — RonDB authority + dispatch layer        |
| `src/fsal_obj/`  | FSAL plugin — layouts, DS file ops, placement              |
| `src/cluster/`   | Inter-MDS — TCP transport, 2PC rename, migration, replication |
| `src/common/`    | Shared — config, logging, threadpool, heartbeat, TLS       |
| `src/tools/`     | `mds-admin` CLI + `mds-backend-probe` diagnostic           |
| `src/bpf/`       | Optional eBPF monitoring hooks (build-time flag)           |
| `include/`       | Public C headers                                           |
| `docs/`          | Architecture reference, lab setup, deployment guide        |
| `tests/`         | Unit and integration tests (CMocka)                        |

## Installation

pnfs-mds ships with a single lab/deployment orchestrator, `scripts/pnfs-lab`,
for repeatable SSH-driven bring-up of the RonDB-backed lab and for
snapshot-friendly test iteration.

### Lab / Cluster Deployment (`scripts/pnfs-lab`)

The orchestrator is command-driven and reads an inventory shell file:

```sh
./scripts/pnfs-lab validate                   # default; just SSH + tool checks
./scripts/pnfs-lab full-single                # bring up a single-MDS lab
./scripts/pnfs-lab full-multi                 # bring up the multi-MDS lab
./scripts/pnfs-lab menu                       # interactive menu
./scripts/pnfs-lab stop                       # stop services + unmount
./scripts/pnfs-lab --confirm-reset reset      # clear generated lab state
```

Individual stages (`validate`, `deps`, `ds`, `rondb`, `build`, `single-mds`,
`multi-mds`, `client`, `smoke`, `test-suite`) can be run on their own so you
can re-run just what you need between snapshot reverts.

Safe defaults:

- runs `validate` only when no command is given
- supports `--dry-run` for every command
- requires `--confirm-reset` before any destructive reset
- prints timestamped per-stage progress banners and a heartbeat during long ops

#### Inventory

The default inventory is `scripts/pnfs-lab.env`, which describes two profiles:

- `single-mds`: one MDS + colocated RonDB data node for fast rebuild-and-retest loops
- `multi-mds`: three MDS nodes with two RonDB data nodes (NoOfReplicas=2)

Edit `scripts/pnfs-lab.env` to change RonDB version, DS IPs, client host, or
profile layout. Keep SSH passwords out of git — pass them at runtime:

```sh
export PNFS_LAB_SSH_PASSWORD='...'   # only if SSH key auth is unavailable
./scripts/pnfs-lab full-multi
```

The target user must have passwordless sudo on every lab host.

### Example: 3-Node Style RonDB Lab Topology

```
Node A (10.0.0.50) — MDS + RonDB data node
Node B (10.0.0.51) — MDS + RonDB data node
Node C (10.0.0.52) — DS
Node D (10.0.0.53) — DS
Node E (10.0.0.55) — RonDB management node
Node F (10.0.0.54) — Client
```

Point `scripts/pnfs-lab.env` at these hosts (multi-mds profile) and run:

```sh
export PNFS_LAB_SSH_PASSWORD='...'
./scripts/pnfs-lab full-multi
```

### Service Management

```sh
sudo systemctl start pnfs-mds    # Start
sudo systemctl stop pnfs-mds     # Stop
sudo systemctl restart pnfs-mds  # Restart
sudo systemctl status pnfs-mds   # Status
journalctl -u pnfs-mds -f        # Live logs
```

## Building from Source

### Prerequisites

**Core:**
- Linux (tested on Ubuntu 22.04+)
- CMake 3.20+
- GCC ≥ 13.1 (mandatory — older versions miss critical diagnostics)
- RonDB 24.10+ / NDB API headers and libraries
- OpenSSL (HMAC credential derivation)
- libntirpc (NFS RPC transport)
- pthreads

**Optional:**
- CMocka (tests)
- libbpf (`ENABLE_EBPF=ON`)
- gRPC + protobuf (optional admin API stub)

### Compile

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Release -DENABLE_RONDB=ON
cmake --build build -j$(nproc)
```

### Build Options

```sh
cmake -B build \
    -DENABLE_RONDB=ON \
    -DENABLE_EBPF=ON \
    -DENABLE_ADMIN_CLI=ON \
    -DENABLE_TESTS=ON
```

### Install

```sh
cmake --install build --prefix /usr/local
```

## Configuration

Flat INI-style config file (default `/etc/pnfs-mds/mds.conf`).
Section headers are ignored; all keys are global.

### Minimal RonDB Example

```ini
# Identity
mds_id              = 0
hostname            = mds0.internal
nfs_port            = 2049

# Backend
catalogue_backend      = rondb
catalogue_backend_conf = /etc/pnfs-mds/rondb.conf

# Data servers
ds_count = 2
ds_0     = ds0.internal:/srv/ds0
ds_1     = ds1.internal:/srv/ds1

# Layout
stripe_unit_bytes    = 1048576
default_mirror_count = 2
```

The RonDB-specific config file (`rondb.conf`):

```ini
connect_string = ndb-mgmd0.internal:1186
schema_name    = pnfs_mds
```

### Key Configuration Reference

| Key | Default | Description |
|-----|---------|-------------|
| `catalogue_backend` | `rondb` | Catalogue backend (only `rondb` is supported today) |
| `catalogue_backend_conf` | — | Path to backend-specific config |
| `worker_threads` | auto | RPC worker count |
| `lease_time_sec` | 90 | NFSv4 lease time |
| `grace_period_sec` | 90 | Grace period duration |
| `workload_profile` | `default` | Tuning preset: `default`, `hpc`, `ai_training`, `genomics`, `media` |
| `inline_enabled` | `false` | Small-file acceleration (must be false in RonDB mode) |
| `catalog_image_mode` | `off` | Hot-read image: `off`, `shadow`, `compare`, `primary` |

See `docs/architecture.md` for the full configuration reference.


### Running Tests

```sh
cmake -B build -DCMAKE_BUILD_TYPE=Debug -DENABLE_RONDB=ON -DENABLE_TESTS=ON
cmake --build build -j$(nproc)
ctest --test-dir build --output-on-failure
```

### QA Gates

```sh
cmake --build build --target qa-quick   # compile + cppcheck + clang-tidy + unit tests
cmake --build build --target qa         # full pipeline (adds valgrind + integration)
```

### Valgrind

```sh
valgrind --leak-check=full --error-exitcode=1 ./build/tests/test_open_state
```

## Data Servers

Data servers are **standard unpatched Linux knfsd** instances.  The MDS
creates data files on DS NFS exports, captures file handles, and issues
flex-file layouts pointing clients directly to the DSes for I/O.

- **Flex file layouts** — per-stripe NFS file handles for parallel I/O
- **TCP and RDMA endpoints** — dual-transport advertisement in GETDEVICEINFO
- **Placement** — round-robin, stripe
- **GC** — asynchronous data file cleanup after metadata deletion

## Cluster Features

- **Active-active** — partitioned namespace with `fs_locations` referrals
- **Cross-subtree rename** — two-phase commit over inter-MDS transport
- **Subtree migration** — live ownership transfer via admin RPC
- **Failover** — RonDB-native partner-loss detection (watchdog on
  `mds_node_registry.last_heartbeat_ns`) with automatic promotion and
  NFSv4.1 grace period; controlled demote/failback
- **Replication** — Handled natively by the NDB cluster; there is no
  application-level delta-shipping layer in the daemon anymore

## Security

- **NFS authentication** — AUTH_SYS, Kerberos (krb5/krb5i/krb5p)
- **Cluster TLS** — mTLS for inter-MDS transport (optional)
- **DS access control** — restrict DS exports by IP/subnet; standard
  NFS export security applies

## Performance

### Architecture

The production binary contains **zero LMDB implementation code**.  All
mds_lmdb_* symbols are satisfied by weak stubs returning MDS_ERR_NOSUPPORT.
All metadata operations dispatch through the RonDB catalogue vtable
(`mds_cat_*` / `mds_coord_*`) with native concurrent NDB transactions.

Key optimisations:

- **Direct catalogue dispatch** — removed the LMDB commit_queue writer
  thread; writes go directly to RonDB vtable from worker threads
- **Per-thread fileid pools** — `_Thread_local` batch alloc, zero contention
- **Fused LOOKUP** — dirent + inode read in 1 NDB transaction
- **Fused REMOVE reads** — 3 NDB transactions → 2
- **NDB connection pool** — auto-sized to worker_threads (tunable to 32)
- **NDB transaction retry** — 3 attempts with backoff for transient errors
- **Inode cache warmup** — CREATE populates cache for immediate GETATTR
- **Striped inode cache** — LRU (32K entries), pre-warmed with root inode
- **Striped dirent cache** — LRU (32K entries) with negative TTL (5s)
- **Striped lock table** — per-stripe hash + stateid-encoded stripe index
- **Zero-copy RPC dispatch** — worker reads directly from recv_buf
- **Completion pipe** — O(completed) epoll wakeup, not O(max_conns)
- **Threadpool slab** — pre-allocated work items, no malloc per RPC
- **Per-thread XDR buffers** — eliminates ~1MB alloc/free per RPC
- **Fused NDB LAYOUTGET** — batch PK reads + layout write in one txn
- **NFSv4.1 delegations** — READ/WRITE grants on OPEN
- **Concurrency hardened** — verified 16 tasks, 336K ops, zero crashes


## Remaining Hardening

- Session/open/lock state persistence to RonDB
- DS health monitoring lab verification
- Kerberos / mTLS lab testing


## Documentation

- `docs/architecture.md` — Single source of truth (23 sections)
- `docs/QUICKSTART.md` — Quick start: build from source (single node or lab cluster)
- `docs/MANUAL_INSTALL.md` — Step-by-step manual lab install
- `docs/INSTALL_ROCKY.md` — Rocky Linux 9 / 10 community edition install
- `CONTRIBUTING.md` — Code style, compiler discipline, patch process

## License

MIT for the bulk of the source (`LICENSE-MIT`). One file —
`src/catalogue/catalogue_rondb_shim.cpp` — is GPL-2.0
(`LICENSE-GPL-2.0`) because it links against the RonDB / NDB API
headers, which are GPL-2.0. See `LICENSING.md` for the per-file
rationale and `LICENSE` for the umbrella file.

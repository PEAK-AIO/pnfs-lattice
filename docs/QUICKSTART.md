# Quick Start — Build from Source

**Time:** ~5 minutes (single node) | ~15 minutes (lab cluster)

## Prerequisites

- Linux (Ubuntu 22.04+ or equivalent)
- GCC ≥ 13.1 (mandatory — build will fail on older compilers)
- CMake 3.20+
- RonDB 26.02+ (NDB API headers and `libndbclient.so`)
- libntirpc (`apt install libntirpc-dev`)
- OpenSSL (`apt install libssl-dev`)
- pkg-config (`apt install pkg-config`)

## 1. Single-Node Development Build

```bash
# Install build dependencies (Ubuntu/Debian)
sudo apt install -y build-essential cmake pkg-config \
    libssl-dev libntirpc-dev libkrb5-dev

# Clone and build (no RonDB cluster required for unit tests)
git clone https://github.com/PEAK-AIO/pnfs-lattice.git
cd pnfs-lattice
./scripts/cmake-fresh.sh build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)

# Run tests (in-memory catalogue stub, no RonDB needed)
ctest --test-dir build --output-on-failure
```

If CMake reports a `CMakeCache.txt` path mismatch, the tarball probably
included a `build/` tree from another host. Re-pack with
`./scripts/package-source.sh` (excludes `build/`), or on the build host:

```bash
rm -rf build
./scripts/cmake-fresh.sh build -DCMAKE_BUILD_TYPE=Release -DENABLE_RONDB=ON -DRonDB_ROOT=/opt/rondb
```

MDS build-machine install (after extracting `lattice.tar.gz`):

```bash
export CC=gcc-13 CXX=g++-13
./scripts/build-install-mds.sh
```

## 2. Build with RonDB Backend

Requires a running RonDB cluster with NDB API headers installed
(typically at `/opt/rondb/include/storage/ndb`).

```bash
# Build with RonDB support
./scripts/cmake-fresh.sh build-rondb \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_RONDB=ON \
    -DRONDB_INCLUDE_DIR=/opt/rondb/include/storage/ndb \
    -DRONDB_LIBRARY=/opt/rondb/lib/libndbclient.so

cmake --build build-rondb -j$(nproc)
```

## 3. Install and Run

```bash
# Install binary
sudo install -m 0755 build-rondb/src/mds/pnfs-mds /usr/local/bin/

# Create config directory
sudo mkdir -p /etc/pnfs-mds /var/lib/pnfs-mds

# Minimal RonDB config
sudo tee /etc/pnfs-mds/mds.conf << EOF
mds_id = 1
hostname = $(hostname -I | awk '{print $1}')
nfs_port = 2049
worker_threads = $(nproc)
catalogue_backend = rondb
catalogue_backend_conf = /etc/pnfs-mds/rondb.conf
inline_enabled = false
grace_period_sec = 0
inode_cache_size = 32768
ds_count = 1
ds[0] = DS_IP:/export/ds
EOF

sudo tee /etc/pnfs-mds/rondb.conf << EOF
connect_string = RONDB_MGMD_IP:1186
schema_name = pnfs_mds
EOF

# Start (foreground for first run — auto-bootstraps RonDB schema)
sudo LD_LIBRARY_PATH=/opt/rondb/lib:/opt/rondb/lib64 \
    /usr/local/bin/pnfs-mds /etc/pnfs-mds/mds.conf
```

On first start, the MDS auto-creates all 35 RonDB tables and seeds
the root inode.

## 4. Mount from Client

```bash
# On an NFS client machine:
sudo mount -t nfs4 -o vers=4.2,sec=sys,nconnect=8 MDS_IP:/ /mnt/pnfs
ls /mnt/pnfs/
```

## 5. Verify with mdtest

```bash
# Single-client smoke test
mdtest -d /mnt/pnfs/test -u -z 1 -b 10 -I 10

# Parallel stress test (8 tasks)
mpirun --allow-run-as-root -np 8 \
    mdtest -d /mnt/pnfs/bench -u -z 1 -b 30 -I 30 -r -P -C
```

Expected: ~130 ops/s (single), ~1,900 ops/s (8 tasks) for file creation.

## 6. Lab / Cluster Deployment (`scripts/pnfs-lab`)

For multi-node bring-up over SSH, use the canonical lab orchestrator:

```bash
export PNFS_LAB_SSH_PASSWORD='...'   # only if SSH key auth is unavailable
./scripts/pnfs-lab validate
./scripts/pnfs-lab full-single       # single-MDS profile
./scripts/pnfs-lab full-multi        # multi-MDS + 2 RonDB data nodes
```

Topology, RonDB version, DS hosts, and per-profile tuning live in
`scripts/pnfs-lab.env`. The target user must have passwordless sudo on every
lab host.

## Configuration Reference

| Key | Default | Description |
|-----|---------|-------------|
| `catalogue_backend` | `rondb` | Backend: `rondb` (only supported backend) |
| `catalogue_backend_conf` | — | Path to `rondb.conf` |
| `worker_threads` | `nproc` | RPC worker thread count |
| `inode_cache_size` | 32768 | LRU inode cache entries |
| `grace_period_sec` | 90 | Grace period (0 for testing) |
| `nconnect` | — | Set on NFS client mount: `-o nconnect=8` or `=16` |

See `docs/architecture.md §17` for the full ~70 config keys.

## Troubleshooting

**"No free node id" on RonDB connect:**
Increase `[api]` slots in RonDB `config.ini`. Each MDS uses
`ndb_conn_pool_size` API slots (default: `min(worker_threads, 8)`).

**Mount hangs:**
Check MDS is listening: `ss -tlnp | grep 2049`. Check logs:
`tail /tmp/pnfs-mds.log`.

**EEXIST on parallel creates:**
This was a fileid race bug fixed in early development. Ensure you
are building from the latest `main` branch.

**Segfault under 16+ tasks:**
This was an open_state hash bug fixed in early development. Ensure you
are building from the latest `main` branch.

# pnfs-lattice — Rocky Linux 9 / 10 Installation Guide

Single-MDS community edition install on Rocky Linux 9 or 10 (RHEL /
Alma / Fedora equivalents should be near-identical). Every command in
this guide was verified on both:

- **Rocky 9.7** — kernel 5.14, GCC 11.5, cmake 3.26
- **Rocky 10.1** — kernel 6.12, GCC 14.3, cmake 3.30

## Lab topology

| Role       | Hostname | IP              | Purpose                        |
|------------|----------|-----------------|--------------------------------|
| MDS + RonDB| mds1     | 192.168.100.171 | Metadata server + RonDB (all-in-one) |
| DS 1       | ds1      | 192.168.100.172 | NFS data server 1              |
| DS 2       | ds2      | 192.168.100.173 | NFS data server 2              |
| Client 1   | client1  | 192.168.100.174 | pNFS client                    |
| Client 2   | client2  | 192.168.100.175 | pNFS client                    |

Adjust IPs, hostnames, and the subnet mask (`192.168.100.0/24`) to
match your network. The guide assumes a `peak` user with passwordless
`sudo` on every host. SSH key access from your workstation (or from
`mds1`) to all hosts is strongly recommended.

## Prerequisites

- 5 machines (physical or virtual) running Rocky Linux 9.x or 10.x
  minimal install.
- All hosts can reach each other on the LAN.
- Each host has at least 4 GB RAM. The MDS host should have 8 GB+
  because it runs RonDB in addition to pnfs-mds.

---

## Step 1 — Set hostnames (all hosts)

Run once on each host:

```bash
# On 192.168.100.171:
sudo hostnamectl set-hostname mds1

# On 192.168.100.172:
sudo hostnamectl set-hostname ds1

# On 192.168.100.173:
sudo hostnamectl set-hostname ds2

# On 192.168.100.174:
sudo hostnamectl set-hostname client1

# On 192.168.100.175:
sudo hostnamectl set-hostname client2
```

## Step 2 — Install build dependencies on mds1

SSH to **mds1** (`192.168.100.171`):

```bash
ssh peak@192.168.100.171
```

Enable the CRB (Code Ready Builder) and EPEL repositories — some
development packages live there:

```bash
sudo dnf install -y epel-release
sudo dnf config-manager --set-enabled crb
```

Install the compiler toolchain and library headers:

```bash
sudo dnf install -y \
    gcc gcc-c++ cmake make pkgconf git wget tar rsync \
    openssl-devel krb5-devel userspace-rcu-devel libnsl2-devel \
    libaio-devel
```

> **Note:** `libaio-devel` is needed at RonDB runtime, not for
> building pnfs-lattice. Including it here avoids a second `dnf` pass
> later.

Verify the toolchain:

```bash
gcc --version     # Rocky 9: GCC 11.x, Rocky 10: GCC 14.x
cmake --version   # Rocky 9: 3.26+, Rocky 10: 3.30+
```

## Step 3 — Build libntirpc from source

Neither Rocky 9 nor 10 ship a `libntirpc-dev` package. The pnfs-lattice
repository includes a helper script that builds and installs it:

```bash
cd ~
git clone https://github.com/PEAK-AIO/pnfs-lattice.git
cd pnfs-lattice
sudo bash deps/build-ntirpc.sh
```

Verify the library is installed:

```bash
pkg-config --modversion libntirpc    # should print 5.8
ls /usr/local/lib64/libntirpc.so     # should exist
```

## Step 4 — Download and install RonDB

pnfs-lattice uses RonDB as its metadata catalogue backend. Download the
RonDB 26.02.4 release:

```bash
sudo mkdir -p /opt/pnfs-mds-lab
cd /opt/pnfs-mds-lab
sudo wget -O rondb-26.02.4.tar.gz \
    https://repo.hops.works/master/rondb-26.02.4-linux-glibc2.28-x86_64.tar.gz
sudo tar xzf rondb-26.02.4.tar.gz
sudo ln -sfn /opt/pnfs-mds-lab/rondb-26.02.4-linux-glibc2.28-x86_64 /opt/rondb
```

> **Caution:** if the glob `rondb-26.02.4*` matches both the `.tar.gz`
> and the extracted directory, use the explicit directory name in the
> `ln` command above.

Register the shared libraries:

```bash
sudo tee /etc/ld.so.conf.d/pnfs-mds-rondb.conf >/dev/null <<'EOF'
/opt/rondb/lib
/opt/rondb/lib/mysql
EOF
sudo ldconfig
```

Verify:

```bash
/opt/rondb/bin/ndb_mgm --version
# Expected: RonDB Management Server RonDB-26.02.4
```

## Step 5 — Build pnfs-lattice with RonDB support

```bash
cd ~/pnfs-lattice
cmake -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_RONDB=ON \
    -DRonDB_ROOT=/opt/rondb \
    -DENABLE_EBPF=OFF \
    -DENABLE_TESTS=OFF

cmake --build build -j$(nproc)
```

Check the binaries:

```bash
ls -la build/src/mds/pnfs-mds build/src/tools/mds-admin
# Both should exist and be ~500 KB–1 MB each
```

## Step 6 — Configure and start the RonDB management node on mds1

Create directories:

```bash
sudo mkdir -p /etc/rondb /var/lib/rondb/mgm /var/lib/rondb/data \
              /usr/local/lib/pnfs-lab
```

Write the cluster config. This single-node setup runs the management
server, one data node, and all API slots on the same host:

```bash
sudo tee /etc/rondb/config.ini >/dev/null <<'EOF'
[ndb_mgmd]
NodeId=65
HostName=192.168.100.171
PortNumber=1186
DataDir=/var/lib/rondb/mgm

[ndbd default]
NoOfReplicas=1
DataDir=/var/lib/rondb/data
FileSystemPath=/var/lib/rondb/data
DataMemory=1G
SharedGlobalMemory=128M
RedoBuffer=32M
TransactionMemory=64M
IndexMemory=32M

[ndbd]
NodeId=1
HostName=192.168.100.171

# API slots for pnfs-mds (2 connections for pool)
[api]
NodeId=101
HostName=192.168.100.171
[api]
NodeId=102
HostName=192.168.100.171

# 8 free-floating API slots (ndb_mgm, mds-admin, etc.)
[api]
NodeId=103
[api]
NodeId=104
[api]
NodeId=105
[api]
NodeId=106
[api]
NodeId=107
[api]
NodeId=108
[api]
NodeId=109
[api]
NodeId=110
EOF
```

Write a launcher script that passes `--initial` only on first start:

```bash
sudo tee /usr/local/lib/pnfs-lab/rondb-mgmd-start.sh >/dev/null <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
args=()
if [ ! -e /var/lib/rondb/mgm/ndb_65_config.bin.1 ]; then
    args+=(--initial)
fi
exec /opt/rondb/bin/ndb_mgmd \
    -f /etc/rondb/config.ini \
    --configdir=/var/lib/rondb/mgm \
    "${args[@]}" \
    --nodaemon
EOF
sudo chmod 0755 /usr/local/lib/pnfs-lab/rondb-mgmd-start.sh
```

Install the systemd unit and start:

```bash
sudo tee /etc/systemd/system/rondb-mgmd.service >/dev/null <<'EOF'
[Unit]
Description=RonDB management server for pnfs lab
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
Environment=LD_LIBRARY_PATH=/opt/rondb/lib:/opt/rondb/lib/mysql
ExecStart=/usr/local/lib/pnfs-lab/rondb-mgmd-start.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now rondb-mgmd.service
sudo systemctl status rondb-mgmd.service --no-pager
```

You should see `Active: active (running)`.

## Step 7 — Configure and start the RonDB data node on mds1

```bash
sudo tee /usr/local/lib/pnfs-lab/rondb-ndbmtd-start.sh >/dev/null <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
NODE_ID=1
args=()
if [ ! -d /var/lib/rondb/data ] || \
   [ -z "$(find /var/lib/rondb/data -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]; then
    args+=(--initial)
fi
exec /opt/rondb/bin/ndbmtd \
    --ndb-connectstring=192.168.100.171:1186 \
    --ndb-nodeid="${NODE_ID}" \
    "${args[@]}" \
    --nodaemon
EOF
sudo chmod 0755 /usr/local/lib/pnfs-lab/rondb-ndbmtd-start.sh

sudo tee /etc/systemd/system/rondb-ndbmtd.service >/dev/null <<'EOF'
[Unit]
Description=RonDB data node for pnfs lab
After=network-online.target rondb-mgmd.service
Wants=network-online.target

[Service]
Type=simple
Environment=LD_LIBRARY_PATH=/opt/rondb/lib:/opt/rondb/lib/mysql
ExecStart=/usr/local/lib/pnfs-lab/rondb-ndbmtd-start.sh
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now rondb-ndbmtd.service
```

## Step 8 — Verify the RonDB cluster is healthy

Wait 10–15 seconds for the data node to initialise, then:

```bash
/opt/rondb/bin/ndb_mgm --ndb-connectstring=192.168.100.171:1186 -e show
```

Expected output:

```
[ndbd(NDB)]     1 node(s)
id=1    @192.168.100.171  (RonDB-26.02.4, Nodegroup: 0, *)

[ndb_mgmd(MGM)] 1 node(s)
id=65   @192.168.100.171  (RonDB-26.02.4)

[mysqld(API)]   10 node(s)
id=101 (not connected, accepting connect from 192.168.100.171)
...
```

If the data node shows `not connected`, check:

```bash
sudo journalctl -u rondb-ndbmtd -n 200 --no-pager
```

## Step 9 — Configure the NFS data servers (ds1 and ds2)

Run the **same** block on both **ds1** (`192.168.100.172`) and **ds2**
(`192.168.100.173`):

```bash
ssh peak@192.168.100.172    # repeat for 192.168.100.173

sudo dnf install -y nfs-utils
sudo install -d -m 0777 /export/ds /export/ds/data
sudo install -d -m 0755 /etc/exports.d

sudo tee /etc/exports.d/pnfs-ds.exports >/dev/null <<'EOF'
/export/ds 192.168.100.0/24(rw,sync,no_subtree_check,no_root_squash,insecure)
EOF

sudo systemctl enable --now nfs-server
sudo exportfs -ra
sudo exportfs -v
```

Open the firewall for NFS traffic:

```bash
sudo firewall-cmd --permanent --add-service=nfs
sudo firewall-cmd --permanent --add-service=mountd
sudo firewall-cmd --permanent --add-service=rpc-bind
sudo firewall-cmd --reload
```

Verify from **mds1**:

```bash
showmount -e 192.168.100.172
showmount -e 192.168.100.173
```

You should see `/export/ds 192.168.100.0/24` for each DS.

## Step 10 — Install pnfs-mds on mds1

### 10a. Install NFS client tools and create directories

```bash
ssh peak@192.168.100.171

sudo dnf install -y nfs-utils attr
sudo install -d -m 0755 /usr/local/bin /etc/pnfs-mds /var/lib/pnfs-mds
```

### 10b. Install the binaries

```bash
sudo install -m 0755 ~/pnfs-lattice/build/src/mds/pnfs-mds  /usr/local/bin/pnfs-mds
sudo install -m 0755 ~/pnfs-lattice/build/src/tools/mds-admin /usr/local/bin/mds-admin
```

### 10c. Write the RonDB backend config

```bash
sudo tee /etc/pnfs-mds/rondb.conf >/dev/null <<'EOF'
connect_string = 192.168.100.171:1186
schema_name = pnfs_mds
EOF
```

### 10d. Write the MDS config

```bash
sudo tee /etc/pnfs-mds/mds.conf >/dev/null <<'EOF'
mds_id = 1
hostname = 192.168.100.171
nfs_port = 2049
grpc_port = 50051
catalogue_backend = rondb
catalogue_backend_conf = /etc/pnfs-mds/rondb.conf
worker_threads = 8
inline_enabled = false
lease_time_sec = 90
grace_period_sec = 0
ds_heartbeat_ms = 5000
cluster_size = 1
cluster_bind_addr = 192.168.100.171
prealloc_pool_size = 4096
inode_cache_size = 32768
transient_state_cache = true
ds_count = 2
ds[0] = 192.168.100.172:/export/ds
ds[1] = 192.168.100.173:/export/ds
ds_mount_path_fmt = /mnt/ds%u
EOF
```

### 10e. Mount DS exports on the MDS

The MDS needs NFSv3 back-mounts to each DS for file handle
preparation. Clients still write directly to the DSes via pNFS.

```bash
sudo install -d -m 0755 /mnt/ds0 /mnt/ds1
sudo mount -t nfs -o vers=3,nconnect=8 192.168.100.172:/export/ds /mnt/ds0
sudo mount -t nfs -o vers=3,nconnect=8 192.168.100.173:/export/ds /mnt/ds1
```

Make them survive reboot:

```bash
echo '192.168.100.172:/export/ds /mnt/ds0 nfs vers=3,nconnect=8,_netdev 0 0' | sudo tee -a /etc/fstab
echo '192.168.100.173:/export/ds /mnt/ds1 nfs vers=3,nconnect=8,_netdev 0 0' | sudo tee -a /etc/fstab
```

### 10f. Open the firewall on mds1

```bash
sudo firewall-cmd --permanent --add-service=nfs
sudo firewall-cmd --permanent --add-service=mountd
sudo firewall-cmd --permanent --add-service=rpc-bind
sudo firewall-cmd --reload
```

### 10g. Install the systemd unit and start pnfs-mds

```bash
sudo tee /etc/systemd/system/pnfs-mds.service >/dev/null <<'EOF'
[Unit]
Description=pnfs-mds daemon
After=network-online.target rondb-mgmd.service rondb-ndbmtd.service remote-fs.target
Wants=network-online.target

[Service]
Type=simple
Environment=LD_LIBRARY_PATH=/opt/rondb/lib:/opt/rondb/lib/mysql
ExecStart=/usr/local/bin/pnfs-mds /etc/pnfs-mds/mds.conf
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now pnfs-mds.service
sudo systemctl status pnfs-mds.service --no-pager
```

First-boot logs should include:

```
INFO: RonDB connection pool: N connection(s) to 192.168.100.171:1186
INFO: RonDB node 1 registered
INFO: seeded DS 0: 192.168.100.172:/export/ds
INFO: seeded DS 1: 192.168.100.173:/export/ds
INFO: ds_health started: 2 DS registered, 2 ONLINE
```

Verify port 2049 is listening:

```bash
ss -tln | grep ':2049'
```

## Step 11 — Mount the namespace on the clients

Run on **client1** (`192.168.100.174`) and **client2**
(`192.168.100.175`):

```bash
ssh peak@192.168.100.174    # repeat for 192.168.100.175

sudo dnf install -y nfs-utils attr
sudo install -d -m 0755 /mnt/pnfs
sudo mount -t nfs4 -o vers=4.2,nconnect=8 192.168.100.171:/ /mnt/pnfs
mount | grep /mnt/pnfs
```

You should see the mount type `nfs4` with `vers=4.2`.

To mount at boot:

```bash
echo '192.168.100.171:/ /mnt/pnfs nfs4 vers=4.2,nconnect=8,_netdev 0 0' | sudo tee -a /etc/fstab
```

## Step 12 — Smoke test

From **client1**:

```bash
cd /mnt/pnfs

# Create a directory and a file
sudo mkdir -p smoke
echo hello | sudo tee smoke/f1 >/dev/null
sudo cat smoke/f1

# Hardlink
sudo ln smoke/f1 smoke/f1.hl
ls -li smoke/f1 smoke/f1.hl    # same inode number, nlink=2

# Symlink
sudo ln -s f1 smoke/f1.sym
sudo readlink smoke/f1.sym

# Extended attributes
sudo setfattr -n user.foo -v bar smoke/f1
sudo getfattr -n user.foo --only-values smoke/f1; echo

# Large file (10 MB)
sudo dd if=/dev/urandom of=smoke/big.bin bs=1M count=10 status=none
sudo stat -c '%s bytes' smoke/big.bin

# Clean up
cd /mnt/pnfs
sudo rm -rf smoke
```

Verify the data path lands on a DS (not stored on the MDS):

```bash
# Write a test file from the client:
sudo dd if=/dev/urandom of=/mnt/pnfs/ds_check.bin bs=1M count=2 status=none

# From ds1 or ds2, the file should appear:
ssh peak@192.168.100.172 'ls -la /export/ds/data/'
ssh peak@192.168.100.173 'ls -la /export/ds/data/'

# Clean up
sudo rm /mnt/pnfs/ds_check.bin
```

## Step 13 — Everyday service control

Restart the MDS:

```bash
sudo systemctl restart pnfs-mds
sudo systemctl status pnfs-mds
sudo journalctl -u pnfs-mds -f
```

Stop everything (client → MDS → RonDB data → RonDB mgm):

```bash
# On each client:
sudo umount /mnt/pnfs || true

# On mds1:
sudo systemctl stop pnfs-mds
sudo umount /mnt/ds0 /mnt/ds1 || true
sudo systemctl stop rondb-ndbmtd
sudo systemctl stop rondb-mgmd
```

Start again in reverse order: `rondb-mgmd` first, then
`rondb-ndbmtd`, wait until `ndb_mgm -e show` reports the data node
connected, then `pnfs-mds`, then remount the clients.

## Step 14 — Troubleshooting

- **`pnfs-mds` won't start, log says `failed to connect to RonDB`** —
  Run `ndb_mgm -e show` on mds1
  (`--ndb-connectstring=192.168.100.171:1186`). If the data node
  shows `not connected`, check
  `sudo journalctl -u rondb-ndbmtd -n 200 --no-pager`.

- **`ndb_mgm` says `Unable to connect`** — mgmd is not running or
  port 1186 is blocked. Check
  `sudo systemctl status rondb-mgmd` and
  `sudo firewall-cmd --list-all`.

- **MDS log: `could not mount DS`** — Run `mount | grep /mnt/ds` on
  mds1 and `showmount -e 192.168.100.172` from mds1. The export path
  must be `/export/ds` and the DS must allow `192.168.100.0/24`.

- **Client mount hangs** — On the client, run
  `ss -tn state established '( dport = :2049 )'`; it should show a
  connection to 192.168.100.171. If not, check `ss -tln | grep :2049`
  on mds1. Also verify `firewall-cmd --list-services` on mds1
  includes `nfs`.

- **`Address already in use` on port 2049** — Rocky's stock
  `nfs-server` is also bound to 2049. Do **not** enable
  `nfs-server` on the MDS host; it was only installed for the
  `showmount` and NFS client tools.

- **libntirpc not found during `cmake`** — Run
  `sudo bash deps/build-ntirpc.sh` from the pnfs-lattice directory.
  CMake will print a `FATAL_ERROR` message pointing to this script
  if the library is missing.

- **Starting over with a clean RonDB** — Stop all services, then:
  ```bash
  sudo rm -rf /var/lib/rondb/mgm/*
  sudo rm -rf /var/lib/rondb/data/*
  ```
  The launcher scripts re-pass `--initial` automatically when the
  data dirs are empty.

## Appendix — Full dependency list

### mds1 (build host + MDS + RonDB)

```
gcc gcc-c++ cmake make pkgconf git wget tar rsync
openssl-devel krb5-devel userspace-rcu-devel libnsl2-devel
libaio-devel nfs-utils attr
epel-release (for EPEL repo)
```

Plus from source: **libntirpc v5.8** (`deps/build-ntirpc.sh`).

### ds1, ds2

```
nfs-utils
```

### client1, client2

```
nfs-utils attr
```

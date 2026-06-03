# pnfs-mds Lab — Manual Install

Step-by-step manual install of the RonDB-backed pnfs-mds lab. No
orchestrator script is used; every command is run by hand on the host
named in the section header. For the scripted equivalent, see
`docs/DEPLOY.md`.

## Topology and accounts

| Host | Mgmt IP | Internal IP | Role |
|------|---------|-------------|------|
| mds1 | 192.168.100.50 | 10.10.10.50 | `pnfs-mds` + RonDB data node 1 |
| mds2 | 192.168.100.51 | 10.10.10.51 | `pnfs-mds` + RonDB data node 2 |
| mds3 | 192.168.100.55 | 10.10.10.55 | `pnfs-mds` + RonDB management node |
| ds1  | 192.168.100.52 | 10.10.10.52 | NFS data server (`/export/ds`) |
| ds2  | 192.168.100.53 | 10.10.10.53 | NFS data server (`/export/ds`) |
| client1 | 192.168.100.54 | 10.10.10.54 | NFSv4.2 client (`/mnt/pnfs`) |

Login everywhere: user `peak`, password `peak`, passwordless `sudo`.

All operational traffic (NFS, RonDB, MDS↔MDS) uses `10.10.10.0/24`.
Make sure that subnet is routable between every lab host before you
start.

## Step 1 — Pick a build host and install build tools

You need one machine to compile `pnfs-mds`. It can be your laptop or
`mds1` itself. It must have Internet access. In this guide we use
**mds1** as the build host.

SSH in:

```bash
ssh peak@192.168.100.50
```

Install build + deploy tools:

```bash
sudo apt update
sudo apt install -y \
    build-essential cmake pkg-config git wget tar \
    libssl-dev libntirpc-dev libkrb5-dev liblmdb-dev \
    gcc-13 g++-13
```

Confirm GCC is ≥ 13.1:

```bash
gcc-13 --version
```

## Step 2 — Put the source on the build host

Copy the `pnfs-mds` source tree to the build host at
`/home/peak/pnfs-mds` (or anywhere; we'll use this path throughout):

```bash
# From wherever the source lives, e.g. your workstation:
rsync -a /home/warp/code/pnfs-mds/ peak@192.168.100.50:/home/peak/pnfs-mds/
```

## Step 3 — Download and unpack the RonDB 24.10.19 SDK on the build host

On **mds1** (the build host):

```bash
sudo mkdir -p /opt/pnfs-mds-lab
cd /opt/pnfs-mds-lab
sudo wget -O rondb-24.10.19.tar.gz \
    https://repo.hops.works/master/rondb-24.10.19-linux-glibc2.28-x86_64.tar.gz
sudo tar xzf rondb-24.10.19.tar.gz
sudo ln -sfn /opt/pnfs-mds-lab/rondb-24.10.19-linux-glibc2.28-x86_64 /opt/rondb
```

> **Note:** Do **not** use a glob like
> `/opt/pnfs-mds-lab/rondb-24.10.19*` in the `ln` command. It will
> match both the extracted directory and the `.tar.gz` archive, which
> causes `ln` to treat `/opt/rondb` as a non-existent target directory
> and fail with `No such file or directory`. If you prefer to avoid
> the explicit suffix, use a glob that excludes the tarball:
>
> ```bash
> rondb_dir=$(ls -d /opt/pnfs-mds-lab/rondb-24.10.19-*/ | head -n1)
> sudo ln -sfn "${rondb_dir%/}" /opt/rondb
> ```

Verify:

```bash
ls -l /opt/rondb
/opt/rondb/bin/ndb_mgm --version
```

This RonDB tarball gives you both the **SDK** (NDB headers +
`libndbclient.so` needed to compile `pnfs-mds`) and the **runtime**
(`ndb_mgmd`, `ndbmtd`, `ndb_mgm`) used later.

## Step 4 — Build pnfs-mds

Still on **mds1**:

```bash
cd /home/peak/pnfs-mds
export CC=gcc-13 CXX=g++-13
cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_RONDB=ON \
    -DRonDB_ROOT=/opt/rondb
cmake --build build -j"$(nproc)"
```

Confirm the binaries exist:

```bash
ls -l build/src/mds/pnfs-mds build/src/tools/mds-admin
```

## Step 5 — Install RonDB runtime binaries on every MDS host

Repeat this block on **each** of `mds1` (.50), `mds2` (.51), and
`mds3` (.55). On the build host (mds1) the tarball is already there,
so you can skip the `wget`.

```bash
ssh peak@192.168.100.51    # then mds3 the same way
sudo apt update
sudo apt install -y wget tar libncurses6
# Try the modern package first; fall back if not found
sudo apt install -y libaio1t64 || sudo apt install -y libaio1

sudo mkdir -p /opt/pnfs-mds-lab
cd /opt/pnfs-mds-lab
sudo wget -O rondb-24.10.19.tar.gz \
    https://repo.hops.works/master/rondb-24.10.19-linux-glibc2.28-x86_64.tar.gz
sudo tar xzf rondb-24.10.19.tar.gz
sudo ln -sfn /opt/pnfs-mds-lab/rondb-24.10.19-linux-glibc2.28-x86_64 /opt/rondb

# Make the RonDB libs discoverable by ldconfig
sudo tee /etc/ld.so.conf.d/pnfs-mds-rondb.conf >/dev/null <<'EOF'
/opt/rondb/lib
/opt/rondb/lib/mysql
EOF
sudo ldconfig

/opt/rondb/bin/ndb_mgm --version
```

> **Note:** Same caveat as Step 3 — always pass the explicit
> `rondb-24.10.19-linux-glibc2.28-x86_64` directory to `ln`, or use
> the `rondb-24.10.19-*/` trailing-slash glob so the tarball is
> excluded.

Result after step 5: each of mds1/mds2/mds3 has `/opt/rondb` and its
shared libraries are registered with ldconfig.

## Step 6 — Configure and start the RonDB management node on mds3

SSH in:

```bash
ssh peak@192.168.100.55
```

Create directories:

```bash
sudo mkdir -p /etc/rondb /var/lib/rondb/mgm /usr/local/lib/pnfs-lab
```

Write the cluster config file. It must be identical across the
cluster — `mgmd` is the source of truth.

```bash
sudo tee /etc/rondb/config.ini >/dev/null <<'EOF'
[ndb_mgmd]
NodeId=65
HostName=10.10.10.55
PortNumber=1186
DataDir=/var/lib/rondb/mgm

[ndbd default]
NoOfReplicas=2
DataDir=/var/lib/rondb/data
FileSystemPath=/var/lib/rondb/data
DataMemory=2G
SharedGlobalMemory=256M
RedoBuffer=64M
TransactionMemory=128M
IndexMemory=64M

[ndbd]
NodeId=1
HostName=10.10.10.50

[ndbd]
NodeId=2
HostName=10.10.10.51

# API slots pinned to each MDS (two per MDS for connection pooling)
[api]
NodeId=101
HostName=10.10.10.50
[api]
NodeId=102
HostName=10.10.10.50
[api]
NodeId=103
HostName=10.10.10.51
[api]
NodeId=104
HostName=10.10.10.51
[api]
NodeId=105
HostName=10.10.10.55
[api]
NodeId=106
HostName=10.10.10.55

# 16 free-floating API slots (ndb_mgm, mds-admin, etc.)
[api]
NodeId=107
[api]
NodeId=108
[api]
NodeId=109
[api]
NodeId=110
[api]
NodeId=111
[api]
NodeId=112
[api]
NodeId=113
[api]
NodeId=114
[api]
NodeId=115
[api]
NodeId=116
[api]
NodeId=117
[api]
NodeId=118
[api]
NodeId=119
[api]
NodeId=120
[api]
NodeId=121
[api]
NodeId=122
EOF
```

Write a small launcher script that adds `--initial` on first start
only:

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

Install a systemd unit:

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

## Step 7 — Configure and start RonDB data nodes on mds1 and mds2

Run on **mds1** first, then **mds2**. The only difference between the
two is `NODE_ID`.

On **mds1** (.50):

```bash
ssh peak@192.168.100.50
sudo mkdir -p /var/lib/rondb/data /usr/local/lib/pnfs-lab

sudo tee /usr/local/lib/pnfs-lab/rondb-ndbmtd-start.sh >/dev/null <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
NODE_ID=1            # <-- set to 2 on mds2
args=()
if [ ! -d /var/lib/rondb/data ] || \
   [ -z "$(find /var/lib/rondb/data -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]; then
    args+=(--initial)
fi
exec /opt/rondb/bin/ndbmtd \
    --ndb-connectstring=10.10.10.55:1186 \
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

On **mds2** (.51): repeat the exact same block, but change the line
`NODE_ID=1` to `NODE_ID=2` before you paste the launcher. Everything
else is identical.

## Step 8 — Verify the RonDB cluster is healthy

From **mds3**:

```bash
/opt/rondb/bin/ndb_mgm --ndb-connectstring=10.10.10.55:1186 -e show
```

Expected output contains, in order:

- `[ndb_mgmd(MGM)] 1 node(s) ... @10.10.10.55 ... Nodegroup: 65`
- `[ndbd(NDB)] 2 node(s)` with both `id=1 @10.10.10.50` and
  `id=2 @10.10.10.51` showing `(mysql-x.x.x, Nodegroup: 0, *)`

If either data node shows `not connected`, check
`sudo journalctl -u rondb-ndbmtd -n 200 --no-pager` on the affected
node.

## Step 9 — Configure the NFS data servers ds1 and ds2

Run the **same** block on both **ds1** (.52) and **ds2** (.53):

```bash
ssh peak@192.168.100.52    # and 192.168.100.53
sudo apt update
sudo apt install -y nfs-kernel-server nfs-common

sudo install -d -m 0777 /export/ds /export/ds/data
sudo install -d -m 0755 /etc/exports.d

sudo tee /etc/exports.d/pnfs-ds.exports >/dev/null <<'EOF'
/export/ds 10.10.10.0/24(rw,sync,no_subtree_check,no_root_squash,insecure)
EOF

sudo systemctl enable --now nfs-kernel-server
sudo exportfs -ra
sudo exportfs -v

# Raise knfsd thread pool from the default 8 to 32
if [ -w /proc/fs/nfsd/threads ]; then
    echo 32 | sudo tee /proc/fs/nfsd/threads >/dev/null
fi
```

Quick sanity check from any MDS host:

```bash
showmount -e 10.10.10.52
showmount -e 10.10.10.53
```

You should see `/export/ds 10.10.10.0/24`.

## Step 10 — Install pnfs-mds on each MDS node

### 10a. Copy the freshly built binaries from the build host

On the build host (mds1):

```bash
cd /home/peak/pnfs-mds
for ip in 192.168.100.50 192.168.100.51 192.168.100.55; do
    scp build/src/mds/pnfs-mds        peak@"$ip":/tmp/pnfs-mds
    scp build/src/tools/mds-admin     peak@"$ip":/tmp/mds-admin
done
```

### 10b. Install binaries + NFS client package — repeat on each MDS

Run on **mds1** (.50), **mds2** (.51), and **mds3** (.55):

```bash
sudo apt update
sudo apt install -y libntirpc-dev libgssapi-krb5-2 libssl3 nfs-common attr

sudo install -d -m 0755 /usr/local/bin /etc/pnfs-mds /var/lib/pnfs-mds
sudo install -m 0755 /tmp/pnfs-mds    /usr/local/bin/pnfs-mds
sudo install -m 0755 /tmp/mds-admin   /usr/local/bin/mds-admin
rm -f /tmp/pnfs-mds /tmp/mds-admin
```

### 10c. Write the RonDB backend config — same on every MDS

```bash
sudo tee /etc/pnfs-mds/rondb.conf >/dev/null <<'EOF'
connect_string = 10.10.10.55:1186
schema_name = pnfs_mds
EOF
```

### 10d. Write the per-MDS config

This is the only file that differs between MDS hosts. Change
`mds_id`, `hostname`, and `cluster_bind_addr` per host.

On **mds1** (.50):

```bash
sudo tee /etc/pnfs-mds/mds.conf >/dev/null <<'EOF'
mds_id = 1
hostname = 10.10.10.50
nfs_port = 2049
grpc_port = 50051
catalogue_backend = rondb
catalogue_backend_conf = /etc/pnfs-mds/rondb.conf
worker_threads = 8
inline_enabled = false
lease_time_sec = 90
grace_period_sec = 0
ds_heartbeat_ms = 5000
cluster_size = 3
cluster_bind_addr = 10.10.10.50
prealloc_pool_size = 4096
inode_cache_size = 32768
transient_state_cache = true
cluster_peer[0] = 10.10.10.50
cluster_peer[1] = 10.10.10.51
cluster_peer[2] = 10.10.10.55
ds_count = 2
ds[0] = 10.10.10.52:/export/ds
ds[1] = 10.10.10.53:/export/ds
ds_mount_path_fmt = /mnt/ds%u
EOF
```

On **mds2** (.51): identical, but change:

- `mds_id = 2`
- `hostname = 10.10.10.51`
- `cluster_bind_addr = 10.10.10.51`

On **mds3** (.55): identical, but change:

- `mds_id = 3`
- `hostname = 10.10.10.55`
- `cluster_bind_addr = 10.10.10.55`

### 10e. Mount DS exports on each MDS

The MDS needs NFSv3 back-mounts to each DS so it can prepare file
handles. This is **not** the client data path; clients still write
directly to the DSes via pNFS. Run on **every MDS**:

```bash
sudo install -d -m 0755 /mnt/ds0 /mnt/ds1
sudo mount -t nfs -o vers=3,nconnect=8 10.10.10.52:/export/ds /mnt/ds0
sudo mount -t nfs -o vers=3,nconnect=8 10.10.10.53:/export/ds /mnt/ds1
```

To make them survive reboot, add to `/etc/fstab`:

```bash
echo '10.10.10.52:/export/ds /mnt/ds0 nfs vers=3,nconnect=8,_netdev 0 0' | sudo tee -a /etc/fstab
echo '10.10.10.53:/export/ds /mnt/ds1 nfs vers=3,nconnect=8,_netdev 0 0' | sudo tee -a /etc/fstab
```

### 10f. Install the systemd unit and start pnfs-mds

Run on each MDS:

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

First-boot logs on **any one** MDS should include lines similar to:

```
INFO: RonDB connection pool: N connection(s) to 10.10.10.55:1186
INFO: RonDB probe failed, running bootstrap...        # first start only
INFO: RonDB node X registered
INFO: shared protocol state active
```

Check port 2049 is open on every MDS:

```bash
ss -tln | grep ':2049'
```

## Step 11 — Mount the namespace on the client

SSH to **client1** (.54):

```bash
ssh peak@192.168.100.54
sudo apt update
sudo apt install -y nfs-common attr

sudo install -d -m 0755 /mnt/pnfs
sudo mount -t nfs4 -o vers=4.2,nconnect=8 10.10.10.50:/ /mnt/pnfs
mount | grep /mnt/pnfs
ls /mnt/pnfs
```

Because the cluster is multi-MDS, the namespace root is an ordinary
writable directory (mode `0755`, owned by `root`) that also holds one
referral entry per MDS. You should see one entry per MDS:

```
shard1  shard2  shard3
```

Each `shardN` is an NFSv4 `fs_locations` referral to the MDS that owns
it (`shard1` → mds1 @ 10.10.10.50, `shard2` → mds2 @ 10.10.10.51,
`shard3` → mds3 @ 10.10.10.55). The kernel crosses the referral the
first time you `cd` into a shard. Writing directly at the root works
too — the shared RonDB catalogue makes root-level entries visible from
every MDS — but prefer working **inside** a shard so metadata load is
spread across MDSes via the referrals instead of landing entirely on
the MDS you mounted.

To mount at boot, append to `/etc/fstab`:

```bash
echo '10.10.10.50:/ /mnt/pnfs nfs4 vers=4.2,nconnect=8,_netdev 0 0' | sudo tee -a /etc/fstab
```

## Step 12 — Smoke test

From the client. Pick any shard as the workdir — the example uses
`shard1`. Writing directly at `/mnt/pnfs` also works (the root is a
normal writable directory), but using a shard exercises the per-MDS
referral path.

```bash
SHARD=/mnt/pnfs/shard1    # or shard2 / shard3
cd "$SHARD"
sudo mkdir -p smoke && cd smoke
echo hello | sudo tee f1 >/dev/null
sudo cat f1
sudo ln f1 f1.hl
sudo ln -s f1 f1.sym
sudo readlink f1.sym
sudo setfattr -n user.foo -v bar f1
sudo getfattr -n user.foo --only-values f1; echo
sudo dd if=/dev/urandom of=big.bin bs=1M count=10 status=none
sudo stat -c '%s bytes' big.bin
cd "$SHARD"     # leave the smoke dir before removing it
sudo rm -rf "$SHARD/smoke"
```

Confirm the data path actually landed on a DS (the MDS is not storing
payload):

```bash
# From the client, write a test file into a shard:
sudo dd if=/dev/urandom of=/mnt/pnfs/shard1/ds_check.bin bs=1M count=2 status=none

# From ds1 (or ds2): the file should appear on one of them
ssh peak@192.168.100.52 'ls -lat /export/ds/data/ | head'
```

Repeat against `shard2` and `shard3` if you want to exercise the
other two MDS nodes.

## Step 13 — Everyday service control

Restart one MDS:

```bash
sudo systemctl restart pnfs-mds
sudo systemctl status pnfs-mds
sudo journalctl -u pnfs-mds -f
```

Stop everything (client → MDS → RonDB data → RonDB mgm):

```bash
# on client1
sudo umount /mnt/pnfs || true

# on each MDS (.50, .51, .55)
sudo systemctl stop pnfs-mds
sudo umount /mnt/ds0 /mnt/ds1 || true

# on mds1 and mds2
sudo systemctl stop rondb-ndbmtd

# on mds3
sudo systemctl stop rondb-mgmd
```

Start again in the reverse order: `rondb-mgmd` first (mds3), then
`rondb-ndbmtd` on mds1/mds2, wait until `ndb_mgm -e show` reports
both data nodes connected, then `pnfs-mds` on each MDS, then remount
the client.

## Step 14 — Troubleshooting cheatsheet

- **`ln: target '/opt/rondb': No such file or directory`** — you used
  a glob like `rondb-24.10.19*` that matched both the `.tar.gz` and
  the extracted directory. Pass the extracted directory to `ln`
  explicitly, e.g.
  `/opt/pnfs-mds-lab/rondb-24.10.19-linux-glibc2.28-x86_64`. See the
  notes in Steps 3 and 5.
- **`pnfs-mds` won't start, log says `failed to connect to RonDB`** —
  run `ndb_mgm -e show` from any MDS
  (`--ndb-connectstring=10.10.10.55:1186`). If a data node shows
  `not connected`, look at `sudo journalctl -u rondb-ndbmtd` on that
  host.
- **`ndb_mgm` says `Unable to connect with connect string`** — mgmd
  is not running on mds3 or port 1186 is blocked; check
  `sudo systemctl status rondb-mgmd` and firewall rules on the
  `10.10.10.0/24` subnet.
- **MDS log: `could not mount DS`** — verify `mount | grep /mnt/ds`
  on that MDS and `showmount -e 10.10.10.52` from the MDS. The
  export path must be `/export/ds` and the DS must allow
  `10.10.10.0/24`.
- **Client mount hangs** — on the client,
  `ss -tn state established '( dport = :2049 )'` should show a
  connection to 10.10.10.50. If not, check `ss -tln | grep :2049` on
  mds1; `pnfs-mds` may not have started.
- **Writes at `/mnt/pnfs/<file>` return `Permission denied`** — the
  namespace root is owned by `root` with mode `0755`, so only `root`
  may create entries directly at the root. This is ordinary POSIX
  permission, not a read-only filesystem. Use `sudo`, or create your
  files inside `/mnt/pnfs/shard1`, `/mnt/pnfs/shard2`, or
  `/mnt/pnfs/shard3`.
- **`rm: cannot remove '...': Device or resource busy`** — on NFS,
  removing a directory while the current shell is still `cd`'d into
  it fails because the kernel holds a reference to the open
  directory. `cd` to the parent (or any other path) first, then
  retry the `rm -rf`.
- **`cd /mnt/pnfs/shardN` hangs or returns `ESTALE`** — the MDS that
  owns that shard is down. `ls /mnt/pnfs` still works because it is
  served by mds1, but crossing into the shard requires the owning
  MDS to be up. Check `systemctl status pnfs-mds` on the matching
  host (`shard1`→.50, `shard2`→.51, `shard3`→.55).
- **`Address already in use` on port 2049** — Ubuntu's stock
  `nfs-kernel-server` is also bound to 2049. Do **not** install or
  enable `nfs-kernel-server` on the MDS hosts; it was only installed
  on the DS hosts (ds1, ds2).
- **Starting over with a clean RonDB** — stop all `pnfs-mds` and
  RonDB services, then on mds3
  `sudo rm -rf /var/lib/rondb/mgm/*` and on mds1/mds2
  `sudo rm -rf /var/lib/rondb/data/*`, then start again. The
  launcher scripts re-pass `--initial` automatically when the data
  dirs are empty.

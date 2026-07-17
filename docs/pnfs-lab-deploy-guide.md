# pnfs-lattice cluster deployment guide (pnfs-lab)

Deploys a pNFS cluster — MDS (pnfs-mds + RonDB), data servers (knfsd
exports), and clients — from one inventory file, over SSH, building
pnfs-mds from source on the machine you run it on.

## Prerequisites

- **Build host** (usually the first MDS): Ubuntu 22.04/24.04 with the
  source tree checked out. The `deps` phase installs the toolchain
  (`build-essential cmake pkg-config libntirpc-dev libkrb5-dev libssl-dev`)
  and the per-node runtime packages.
- **All nodes**: reachable over SSH as `LAB_USER` with passwordless sudo,
  from the build host.
- **Networks**: you need, per node, the address you SSH with (control
  plane) and the address on the storage network (data plane). If they are
  the same network, use the same IP for both.

## Step 0 — generate the inventory

Do not write the env file by hand; generate it from a short spec:

```bash
./scripts/pnfs-lab-genenv --sample > cluster.spec   # template with comments
vi cluster.spec                                     # fill in your hosts/IPs
./scripts/pnfs-lab-genenv cluster.spec > pnfs-lab.env
```

The spec is three host lists plus a handful of settings:

```ini
[cluster]
user = ubuntu
source_dir = /home/ubuntu/pnfs-lattice
# RonDB runtime: EITHER a local .deb path, OR a public tarball:
# rondb_deb = /home/ubuntu/pnfs-rondb_26.02.4-1_amd64.deb
rondb_tarball_url = https://repo.hops.works/master/rondb-26.02.4-linux-glibc2.28-x86_64.tar.gz
rondb_version = 26.02.4
replicas = 2          # MDS count must be a multiple of this (NDB rule)
data_memory = 64G     # RonDB DataMemory per data node

[mds]                 # <ssh-host>  <data-ip>; first MDS also hosts mgmd
mds0.example.com  10.118.1.34
mds1.example.com  10.118.1.29

[ds]
ds00.example.com  10.118.1.12
ds01.example.com  10.118.1.124

[clients]
c00.example.com  10.118.1.78
c01.example.com  10.118.1.142
```

Single-network clusters may put just one IP per line (used for both SSH
and data). The generator validates duplicates, IPv4 syntax, and the
replica rule, and assigns all node IDs automatically.

## Step 1 — deploy, phase by phase

```bash
INV=$PWD/pnfs-lab.env
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds validate   # SSH + inventory sanity
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds deps       # packages everywhere + build toolchain
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds ds         # DS exports (idempotent)
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds rondb      # RonDB mgmd + data nodes
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds build      # build pnfs-mds from source
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds multi-mds  # deploy + start all MDS
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds client     # mount all clients
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds smoke      # end-to-end checks
```

Use `--profile single-mds` and the `single-mds` command for a
single-MDS cluster (the generated env contains both profiles).

Notes:

- The `deps` phase configures `needrestart` to list-only mode on every
  node: on testbed images a broken daemon (e.g. Emulab's `pubsubd`)
  otherwise makes apt exit non-zero and abort phases, and auto-restarting
  `ssh.service` drops the deploy's own sessions.
- The MDS deploy verifies the RonDB runtime is present on each MDS and
  fails with instructions if not (re-run the `rondb` phase after changing
  the MDS/RonDB host arrays — it installs the runtime on every MDS).
- The MDS deploy ships any locally built runtime libraries (for example
  `libntirpc`) along with the binary and registers them via
  `ld.so.conf.d`, so non-build-host MDS run the same bits.
- The **first** MDS start creates the entire RonDB schema; the deploy
  waits up to 180 s per MDS and prints `systemctl`/`journalctl`
  diagnostics if one does not come up.
- RonDB `TransactionMemory` defaults to 2G via
  `LAB_RONDB_TRANSACTION_MEMORY` (the historic 128M default aborts
  transactions under concurrent metadata load — RonDB error 4350).

## Re-initializing the namespace

Never re-init RonDB by hand while data servers still hold backing files:
a fresh namespace restarts the fileid counter, and stale backing files
with colliding names **silently corrupt data** (reads resolve to old
files). Use the guarded subcommand, which does the whole sequence in the
safe order (unmount clients → stop MDS → wipe RonDB dirs → wipe DS
backing files → restart → wait):

```bash
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds --confirm-reset rondb-reinit
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds client   # remount
```

Clients must be unmounted before the re-init (the subcommand does this):
a live re-init invalidates client NFS sessions and wedges the kernel
client state — only a reboot recovers a wedged client.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `pnfs-mds did not open port 2049 ... (diagnostics above)` | Read the printed journal: `rondb_shim_connect() failed` → RonDB not up (check `ndb_mgm -e show`); `status=127` + `not found` from ldd → missing runtime lib (re-run `multi-mds`, it ships libs). First start can take ~2 min. |
| `ndb_mgm -e show` shows `not connected` | Data node down: `journalctl -u rondb-ndbmtd`. Error 2308 = incompatible old data (use `rondb-reinit`). Error 2805 = missing `/var/lib/rondb/data` directory. |
| `ndb_mgm` shows a stale topology | mgmd serves a cached config: stop `rondb-mgmd`, remove `ndb_<id>_config.bin.1` from the mgm dir, start again. |
| `exportfs: duplicated export entries` | Pre-existing manual entry in `/etc/exports`; the `ds` phase now comments it out automatically (backup at `/etc/exports.pnfs-lab.bak`). |
| Client mount hangs / EIO after re-init | Client kept a stale mount across the re-init — reboot the client, then re-run the `client` phase. |

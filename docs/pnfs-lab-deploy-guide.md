# pnfs-lattice cluster deployment guide (pnfs-lab)

Deploys a pNFS cluster — MDS (pnfs-mds + RonDB), data servers (knfsd
exports), and clients — from one inventory file, over SSH, building
pnfs-mds from source on the machine you run it on.  The MDS can also be
run on a FoundationDB cluster instead of RonDB; see "FoundationDB as the
metadata store" below.

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

## FoundationDB as the metadata store

`catalogue_backend = fdb` runs the same pnfs-mds binary against a
FoundationDB cluster.  In pnfs-lab the store is a dimension orthogonal to
the MDS profile: the inventory's `LAB_CATALOGUE_BACKEND` (`rondb`,
default, or `fdb`) or the per-invocation `--catalogue NAME` decides which
`catalogue_backend` block the deploy renders into `mds.conf`; everything
else in the config (cluster_size, peers, DS wiring) is identical.

Spec: add the FoundationDB hosts and, optionally, the store:

```ini
[cluster]
catalogue = fdb              # or rondb (default); pnfs-lab --catalogue overrides
fdb_version = 7.3.62         # server on the [fdb] hosts, clients on every MDS
fdb_key_prefix = lab         # one keyspace shared by every MDS of the deployment
fdb_redundancy = double      # configure new <redundancy> <engine>
fdb_storage_engine = ssd

[fdb]                        # one fdbserver each, all of them coordinators
fdb0.example.com  10.118.1.201
fdb1.example.com  10.118.1.202
fdb2.example.com  10.118.1.203
```

Phases (the RonDB phases stay as they are; nothing below touches RonDB):

```bash
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds fdb        # FDB server + clients, cluster file, configure new
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds build      # ENABLE_FDB=ON when fdb_c.h is installed (auto)
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds --catalogue fdb multi-mds client smoke
```

The `fdb` phase installs the `foundationdb-clients` and
`foundationdb-server` release packages
(github.com/apple/foundationdb/releases, version pinned by
`LAB_FDB_VERSION`, cached under `/var/cache/pnfs-lab`) on every `[fdb]`
host, writes ONE cluster file naming all of them as coordinators BEFORE
the server package is installed (its postinst would otherwise create a
private single-node database), creates the database with
`configure new <redundancy> <engine>` once, then installs the clients
package and the same cluster file (`LAB_FDB_CLUSTER_FILE`, default
`/etc/foundationdb/fdb.cluster`) on every MDS host.  It is idempotent:
an existing cluster file on the first FDB host is reused, installed
packages of the right version are left alone, an available database is
not configured again.

The build phase enables the FoundationDB backend automatically when the
client headers are present on the build host (`LAB_BUILD_ENABLE_FDB=
auto|on|off`), with RonDB still enabled, so one binary carries both
backends and `mds.conf` alone selects the store.  The deploy checks the
prerequisites of the selected store (RonDB runtime whenever the binary
links libndbclient; libfdb_c and the cluster file for fdb), compares
binary, config and unit with the live copies and leaves an unchanged
daemon running -- a repeated switch is a no-op -- and ends with one
`backend-report` line per MDS: the deployed `catalogue_backend` and the
daemon's live connections to the FoundationDB coordinators and to the
RonDB management server.

Switching stores is a config change plus a restart of the daemons.
Unmount the clients first (a different store is a different namespace),
then:

```bash
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds --catalogue fdb multi-mds client    # MDS -> FoundationDB
./scripts/pnfs-lab --inventory "$INV" --profile multi-mds --catalogue rondb multi-mds client  # ... and back
```

The RonDB services keep running while the MDS serve from FoundationDB
(and vice versa), so the namespace of the inactive store is preserved.
`stop` stops the FoundationDB processes only when the active catalogue
is fdb; `reset` never removes `/var/lib/foundationdb` -- for a fresh
database stop `foundationdb.service` on every FDB host, remove
`/var/lib/foundationdb/data/*`, start it again and run
`fdbcli --exec 'configure new <redundancy> <engine>'`.

Useful checks on an MDS host (`FDB_KEY_PREFIX` is the deployment's
`fdb_key_prefix`):

```bash
fdbcli --exec 'status minimal'                                   # The database is available.
CATALOGUE_TEST_BACKEND=fdb FDB_KEY_PREFIX=lab ./tests/lab_cluster_drive nodes        # node registry
CATALOGUE_TEST_BACKEND=fdb FDB_KEY_PREFIX=lab ./tests/lab_cluster_drive partitions   # partition map
CATALOGUE_TEST_BACKEND=fdb FDB_KEY_PREFIX=lab ./tests/lab_cluster_drive bench 2000   # per-op latency + round-trip waits
fdbcli --exec 'getrange lab\x26 lab\x27'                          # raw registry rows (type byte 0x26)
```

The conformance suite runs against the deployment's cluster with
`CATALOGUE_TEST_BACKEND=fdb FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster`
(it uses private `ct-*` key prefixes, never the daemons' keyspace), and
`DAEMON_SMOKE_BACKEND=fdb tests/integration/daemon_smoke.sh <build-dir>`
starts a throw-away daemon on the same cluster under its own prefix.

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

### State persistence per profile

`transient_state_cache` decides whether open/layout state is written to
RonDB or held in the granting MDS's memory:

- **single-mds**: `true`. No peer can take the state over, so persisting
  it costs a round-trip per operation and buys nothing.
- **multi-mds**: `false`. State must outlive the MDS that granted it or
  a failover peer cannot reconstruct what clients still hold.

Persistent state means mass-delete scans hit NDB; that is what
`LAB_RONDB_TRANSACTION_MEMORY` (2G by default) sizes for, so do not
lower it on the multi-mds profile.  Override per profile with
`LAB_PROFILE_MULTI_TRANSIENT_STATE_CACHE` /
`LAB_PROFILE_SINGLE_TRANSIENT_STATE_CACHE` in the inventory.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `pnfs-mds did not open port 2049 ... (diagnostics above)` | Read the printed journal: `rondb_shim_connect() failed` → RonDB not up (check `ndb_mgm -e show`); `status=127` + `not found` from ldd → missing runtime lib (re-run `multi-mds`, it ships libs). First start can take ~2 min. |
| `layout_get_sid exec failed: code=4350 ... Transaction already aborted` in the MDS log, and/or data files that stat as 0 bytes | The `ndb_index_stat` system tables are missing, so index-backed lookups abort their transaction.  Metadata (and 0-byte mdtest) keep working, which hides it.  The `rondb` phase now creates them; on a cluster deployed before that, run `<rondb>/bin/ndb_index_stat --sys-create-if-not-exist -c <mgmd-ip>:1186` and then restart pnfs-mds (the index cache is per-connection).  Verify with a real write, not a 0-byte mdtest: `dd if=/dev/urandom of=/mnt/pnfs/t bs=1M count=16 conv=fsync` then check `stat -c %s /mnt/pnfs/t` is non-zero. |
| `ndb_mgm -e show` shows `not connected` | Data node down: `journalctl -u rondb-ndbmtd`. Error 2308 = incompatible old data (use `rondb-reinit`). Error 2805 = missing `/var/lib/rondb/data` directory. |
| `ndb_mgm` shows a stale topology | mgmd serves a cached config: stop `rondb-mgmd`, remove `ndb_<id>_config.bin.1` from the mgm dir, start again. |
| `exportfs: duplicated export entries` | Pre-existing manual entry in `/etc/exports`; the `ds` phase now comments it out automatically (backup at `/etc/exports.pnfs-lab.bak`). |
| Client mount hangs / EIO after re-init | Client kept a stale mount across the re-init — reboot the client, then re-run the `client` phase. |

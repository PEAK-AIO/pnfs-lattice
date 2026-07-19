# File Striping (HPC-Shared) User Guide

This guide covers wide file striping: creating files whose data is
distributed across many data servers (DS), and choosing how clients
access them.  It reflects the flex-files (RFC 8435) striped layout
support introduced on this branch.

## Concepts

Striping is decided in two independent places:

1. **Create time** — whether a file's data is laid out across N DS
   backing files.  Controlled by a directory flag plus geometry keys.
   This is baked into the file at creation and never changes.
2. **Access time** — whether clients do striped I/O **directly** to
   the DS set (fast path) or send I/O to the MDS, which performs the
   striped DS I/O on their behalf (**proxy** path).  Controlled by two
   config switches; can be changed at any time without touching data.

A file's on-disk format is identical in both access modes: stripe `k`
of a file lives in its own DS backing file, addressed **sparsely**
(the DS-file offset equals the file's logical offset).

## Enabling striping

### 1. Flag the directory (create-time trigger)

Set the `user.pnfs.hpc_shared` xattr on a directory **as root**:

```bash
# setfattr:
setfattr -n user.pnfs.hpc_shared -v 1 /mnt/pnfs/mydata
# or without setfattr installed:
python3 -c "import os; os.setxattr('/mnt/pnfs/mydata','user.pnfs.hpc_shared',b'1')"
```

Every file **subsequently created** in that directory is wide-striped.
Files created before the flag, or in unflagged directories, are
single-stripe and are not affected.  The flag is root-only to set;
remember to make the directory writable for the intended users
(`chmod`/`chown`) — flagging does not change permissions.

Verify:

```bash
python3 -c "import os; print(os.getxattr('/mnt/pnfs/mydata','user.pnfs.hpc_shared'))"
```

### 2. Geometry (mds.conf, create-time parameters)

```ini
hpc_max_stripe_count = 16     # stripe width for flagged dirs
stripe_unit_bytes    = 1048576  # chunk size (default 65536 -- set this!)
```

- Width is clamped to the number of ONLINE DS.
- **Width must stay ≤ ~32**: the Linux client's layout buffer is a
  hardcoded 4096 bytes (`PNFS_LAYOUT_MAXSIZE`); wider layouts do not
  fit, the client receives `NFS4ERR_TOOSMALL`, never retries, and
  silently falls back to proxy I/O.  16 is a well-tested width.
- Leave `stripe_unit_bytes` at 1 MiB unless you have a reason;
  the 64 KiB default costs ~16x the RPC count.
- Successive files rotate their DS selection across the full ONLINE
  pool, so aggregate load spreads over all DS even at width < pool
  size.

### 3. Access mode (mds.conf, access-time switches)

```ini
serve_layouts     = true    # master switch, default true
hpc_serve_layouts = true    # serve WIDE layouts client-direct, default false
```

| serve_layouts | hpc_serve_layouts | flagged-dir files      | normal files  |
|---------------|-------------------|------------------------|---------------|
| true          | true              | client-direct, striped | client-direct |
| true          | false (default)   | striped via MDS proxy  | client-direct |
| false         | (any)             | all I/O via MDS proxy  | via proxy     |

`hpc_serve_layouts=false` (the default) is the safe mode: files are
still striped on the backend (capacity and backend bandwidth spread),
but clients access them through the MDS.  Enable `true` only when
**every client that can mount runs Linux ≥ 6.18** — older flex-files
clients misinterpret the striped layout form as mirrors and corrupt
data.

## Client requirements for client-direct mode

- Linux kernel **6.18 or newer** (striped flex-files decoder).
- NFSv4.2 mount to the MDS; clients open NFSv3 connections to each DS
  in a file's stripe set on first access (width × per-client
  connections).

## Fallback semantics

Client-direct is the normal path; the MDS proxy is the automatic
exception path.  If a DS is briefly unreachable or a layout is
unavailable, the client transparently redirects that I/O through the
MDS and returns to direct I/O afterwards.  Applications see a
slowdown, not an error.  Both paths use the same sparse addressing,
so mixed direct/proxy access to one file is consistent.

## Operational notes

- The flag lives on the directory: tools that **delete and recreate**
  their data directories (io500 removes its ior dirs at run end) come
  back unflagged — re-flag before every run.
- Only new creates are affected; there is no restripe of existing
  files.
- A wide file's stripe map is fixed at create; `hpc_max_stripe_count`
  changes affect only subsequently created files.

## Status / caveats

- Client-direct striped I/O is **correct under normal operation and
  under clean single-DS outage/recovery** (verified).  Under extreme
  sustained load, transient DS errors can surface as application
  `fsync` failures whose data is not recoverable by retry — until
  that is resolved, treat `hpc_serve_layouts=true` as evaluation
  quality and verify data integrity for critical workloads.
- Wide-create placement currently ignores transport/capability
  preferences (RDMA/GPUDirect placement hints); TCP-uniform pools are
  unaffected.

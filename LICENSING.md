# Licensing
This file documents the licensing of the **pnfs-mds (community edition)** source tree.
## Quick summary
- **Almost everything** is MIT-licensed (see `LICENSE-MIT`).
- **One file** is GPL-2.0-or-later: `src/catalogue/catalogue_rondb_shim.cpp` (see `LICENSE-GPL-2.0`).
- Every source file carries an `SPDX-License-Identifier` header naming its
  licence; that header is the source of truth when in doubt.
## Why two licences?
The community edition implements the pNFS metadata server protocol, the
RonDB-backed catalogue, multi-MDS coordination, the layout protocol, and the
HPC N-to-1 shared-file path.  Almost every file in this tree is MIT-licensed
and may be reused under that permissive grant.
The one exception is `src/catalogue/catalogue_rondb_shim.cpp`, which is the
C++ shim that bridges the C catalogue API to the RonDB / MySQL Cluster NDB
client headers.  Those NDB headers are GPL-2.0, and including them in your
own source file pulls the GPL-2.0-or-later licence onto that file.  The
shim is therefore licensed `GPL-2.0-or-later` to match.
The shim is the only file that links against NDB; every other source file
is MIT-licensed and can be reused independently.
## Combined-binary clause
When the daemon `pnfs-mds` is built **with** the RonDB backend
(`-DENABLE_RONDB=ON`, which is the default), the resulting binary links the
GPL-licensed shim plus the GPL-licensed NDB libraries.  Distribution of that
binary is therefore subject to GPL-2.0-or-later.
When the daemon is built **without** RonDB
(`cmake -DENABLE_RONDB=OFF ..`), the shim is not compiled and the binary is
pure-MIT.  This is useful for development, testing, or for sites that want
to write a different catalogue backend.
## Enterprise modules (not in this tree)
The community edition omits the following feature modules, which live in
the proprietary enterprise build under separate licensing:
- `resilver`, `rebalance`, `tiering` — admin-triggered mover features
- `observability` — the Prometheus `/metrics` HTTP endpoint
- `replication` — cluster health monitor
- `layout_cache` — sharded LRU stripe-map cache
- `layout_commit_aggregator` — per-fileid LAYOUTCOMMIT coalescing
- `ds_gc` — background DS unlink drainer
- `ds_prealloc` — DS placement pre-allocation pool
- `wrr` — weighted-round-robin + capacity-derived placement
- `quota` — user/group quota enforcement
The community tree ships no-op stubs for each of these so the daemon
builds and runs without them.  The community edition therefore provides
the full pNFS protocol surface; the missing modules are performance and
operational features available in the enterprise edition.
## How to comply
If you redistribute the community edition with RonDB enabled, the easiest
path is to honour both the MIT and GPL-2.0-or-later terms — i.e. ship the
licence texts (`LICENSE-MIT`, `LICENSE-GPL-2.0`) plus the source for the
shim file along with any binary.  This is identical to how every other
GPL-shimmed open-source storage project (e.g. Ceph's RBD client) handles
the same situation.
If you redistribute with `-DENABLE_RONDB=OFF`, only MIT applies.
## Questions
If a specific file's licence is unclear, the `SPDX-License-Identifier`
header at the top of that file is authoritative.  If even that is missing
or unclear, please open an issue.

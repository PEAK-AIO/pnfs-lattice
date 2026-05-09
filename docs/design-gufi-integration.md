# Technical Design: GUFI Integration for pnfs-mds

**Status:** Draft for joint LANL / PeakAIO review
**Owner:** pnfs-mds team
**Collaborators:** LANL GUFI team
**Last updated:** 2026-05-03

## 1. Summary

This document proposes integrating LANL's Grand Unified File Index (GUFI) with
the pnfs-mds metadata server to enable sub-second analytical queries
(`find`-class, `du`-class, custom SQL) against billion-file namespaces. The
recommended architecture maintains a live, eventually-consistent GUFI
per-directory SQLite mirror of the RonDB-backed metadata, fed by the existing
catalog-image changefeed. Standard GUFI tools (`gufi_find`, `gufi_query`,
`gufi_stats`) run unmodified against the mirror. A future phase contributes a
backend abstraction upstream to GUFI so its tools can speak directly to a
centralized metadata store, eliminating the mirror entirely.

## 2. Problem statement

NFSv4 has no native query verb. Operations like "list all files modified in
the last 24 hours under `/datasets`" or "report total bytes per user under
`/scratch`" are answered by walking the namespace via `LOOKUP` / `READDIR` /
`GETATTR` over millions of RPCs. On a billion-file tree this takes hours and
saturates the metadata server.

HPC, AI training, scientific computing, and compliance workflows routinely
require such queries. Current options for pnfs-mds users are:

- Walk the namespace via NFS — minutes to hours, perturbs production traffic.
- Run a separate indexing product (Spectrum Discover, Starfish, NetApp XCP) —
  external dependency, additional licensing, separate operational surface.
- Live without the capability — most current users do this and absorb the cost.

The pnfs-mds metadata is centralized in RonDB. The data needed to answer these
queries already exists in a single (sharded) SQL store. There is no
fundamental reason a query should require a million NFS RPCs.

## 3. Background

### 3.1 GUFI

The Grand Unified File Index is a LANL-developed (BSD-3) tool for fast
filesystem queries at extreme scale. Its design:

- One SQLite database per directory, stored alongside the directory contents.
- A parallel tree-walker visits each per-directory SQLite database in parallel
  rather than `stat()`-ing every file.
- Queries are SQL with POSIX permissions enforced in the SQL layer, so
  unprivileged users only see entries they could have reached via a
  `stat()` walk.
- CLI tools: `gufi_find`, `gufi_ls`, `gufi_stats`, `gufi_query`,
  `gufi_treesummary`.

The performance win comes from indexing (SQL predicates skip whole subtrees)
and from avoiding the per-file `stat()` cost of a POSIX walk.

### 3.2 pnfs-mds

The pNFS metadata server stores all namespace metadata in a RonDB cluster
(`mds_inodes`, `mds_dirents`, `mds_stripe_maps`, etc.). The server already
implements an optional **catalog-image changefeed** (`src/catalogue/catalog_image.c`)
that streams metadata mutations out of RonDB for downstream consumers
(local read-acceleration cache, replication, audit). This changefeed is the
key integration primitive for this design.

## 4. Goals and non-goals

### 4.1 Goals

- Sub-second `find`/`du`-class query response on billion-file namespaces.
- Standard GUFI tools (`gufi_find`, `gufi_query`, etc.) operate against
  pnfs-mds metadata with zero modification in the first phase.
- POSIX permission semantics enforced at query time.
- Eventual-consistency model with a documented bounded-staleness SLO
  (target: ≤ 30 s under normal load).
- Zero impact on the NFSv4 hot path under normal operation.
- Joint upstreaming opportunity with the LANL GUFI team for a backend
  abstraction (Phase 2).

### 4.2 Non-goals

- Strong consistency between metadata mutations and query results. Queries
  return a recent-but-not-instant view.
- Replacing the NFSv4 protocol surface. Queries use a side-channel,
  not NFS.
- Indexing data content (file payloads). Only metadata is in scope.
- Real-time event streaming to clients. Queries are pull-based.

## 5. Architectural options considered

Four shapes were evaluated:

### Option A — RonDB → per-directory SQLite mirror (recommended for Phase 1)

A sync daemon consumes the existing catalog-image changefeed and maintains a
per-directory SQLite tree in GUFI's native schema. Standard GUFI tools query
the mirror unmodified.

**Pros:** uses GUFI off the shelf; no GUFI modifications; reuses existing
changefeed; permission semantics inherited from GUFI.
**Cons:** dual-store (must be kept in sync); storage cost for the mirror;
eventual consistency.

### Option B — Native query frontend with GUFI-shaped CLI

Reimplement GUFI's tooling against direct RonDB queries (NDB scans or MySQL
frontend). No mirror, no GUFI code dependency.

**Pros:** no dual store; no consistency window; simpler operational picture.
**Cons:** reimplements permission enforcement, query planning, and CLI
parity; loses joint LANL upstream story; harder to evolve.

### Option C — GUFI backend abstraction (recommended for Phase 2)

Contribute a backend plugin interface to GUFI upstream. Per-directory SQLite
becomes one implementation; a "RonDB backend" lets GUFI tools speak to a
centralized metadata store directly with no mirror.

**Pros:** strategic upstream contribution; benefits other metadata-store
projects (MarFS, future filesystems); publishable joint work; eliminates the
sync-lag concern.
**Cons:** larger engineering effort; requires GUFI maintainer agreement on
the abstraction; longer time-to-first-demo.

### Option D — Hybrid (Option A then Option C)

Ship A first to validate the use case with real customers and workloads. Use
the lessons to guide the design of C as upstream contribution.

**Decision:** Option D. Phase 1 is Option A. Phase 2 is Option C.

## 6. Phase 1 architecture (Option A)

### 6.1 System diagram

```text
                ┌─────────────────────────────────┐
                │           MDS Cluster           │
                │  (active-active, RonDB-backed)  │
                └─────────────────┬───────────────┘
                                  │
                                  │ catalog_image changefeed
                                  │ (existing primitive)
                                  ▼
                ┌─────────────────────────────────┐
                │       mds-gufi-sync daemon      │
                │ - subscribes to changefeed      │
                │ - applies mutations to mirror   │
                │ - persists last-applied offset  │
                └─────────────────┬───────────────┘
                                  │ idempotent
                                  │ SQLite updates
                                  ▼
        ┌─────────────────────────────────────────────────┐
        │            GUFI per-directory mirror            │
        │   /var/lib/pnfs-mds/gufi/<tree>/.../db.sqlite   │
        └─────────────────────────────────────────────────┘
                                  ▲
                                  │ standard GUFI tools
                                  │ (unmodified)
                                  │
              gufi_find · gufi_query · gufi_stats · gufi_du
                                  ▲
                                  │
           mds-find · mds-du · mds-ls (thin convenience wrappers)
```

### 6.2 Components

#### 6.2.1 `mds-gufi-sync` daemon

A new long-running process, sister to the existing admin and metrics
services. Not part of the MDS daemon itself — runs as a sidecar to keep
crash isolation and to avoid loading SQLite into the NFSv4 hot path.

Responsibilities:

- Connect to the catalog-image changefeed via the existing internal API
  (`src/catalogue/catalog_image.c`).
- Maintain a durable cursor (last-applied changefeed offset) under
  `/var/lib/pnfs-mds/gufi/.cursor`.
- For each changefeed event, apply the corresponding GUFI mirror update
  idempotently:
  - inode insert/update/delete → SQLite row in the parent directory's `db`
  - dirent insert/delete → SQLite row in the parent's `db`
  - rename → delete + insert across two parent directories
  - permissions / xattr update → SQLite row update
- Periodically checkpoint the cursor and `fsync` the affected SQLite files.
- Emit Prometheus-style metrics on lag, error count, throughput.

Concurrency model: per-shard worker that owns a hash range of parent fileids
to avoid lock contention on individual SQLite files. Within a shard, ops are
serialized and idempotent.

#### 6.2.2 GUFI mirror layout

The mirror lives under a configurable root, default
`/var/lib/pnfs-mds/gufi/`. Layout follows GUFI's conventions:

```
/var/lib/pnfs-mds/gufi/
  .cursor                       # last-applied changefeed offset
  .schema-version
  /                             # root of the mirrored tree
    db.sqlite
    datasets/
      db.sqlite
      imagenet/
        db.sqlite
      cosmoflow/
        db.sqlite
    scratch/
      db.sqlite
```

Each `db.sqlite` follows the GUFI schema:

```sql
CREATE TABLE entries (
    name TEXT,
    type TEXT,         -- f|d|l|...
    inode INTEGER,
    mode INTEGER,
    nlink INTEGER,
    uid INTEGER,
    gid INTEGER,
    size INTEGER,
    blksize INTEGER,
    blocks INTEGER,
    atime INTEGER,
    mtime INTEGER,
    ctime INTEGER,
    linkname TEXT,
    xattrs TEXT
    -- additional GUFI-standard columns
);
CREATE INDEX entries_size ON entries(size);
CREATE INDEX entries_mtime ON entries(mtime);
CREATE INDEX entries_uid ON entries(uid);
```

Schema fields not present in the pnfs-mds inode (e.g., `linkname` for
non-symlinks, `xattrs` if not yet implemented) are populated as `NULL`.

#### 6.2.3 CLI wrappers

Convenience binaries in `src/tools/`:

- `mds-find` — wraps `gufi_find` with a default mirror path and flag mapping.
- `mds-du` — wraps `gufi_du`.
- `mds-ls` — wraps `gufi_ls`.
- `mds-query` — passes through to `gufi_query` with auth context.

These are thin wrappers, not reimplementations. They primarily resolve the
mirror root from MDS configuration and handle authentication.

### 6.3 Data flow

#### 6.3.1 Steady-state mutation

```
Client → MDS  : NFSv4 CREATE
MDS           : COMMIT to RonDB (mds_inodes + mds_dirents)
MDS → cf      : changefeed event (op=CREATE, parent, child, attrs)
cf → sync     : daemon receives event
sync          : INSERT INTO entries (...) within parent dir's db.sqlite
sync          : checkpoint cursor when batch flushes
```

Lag from RonDB commit to mirror update under normal load: target ≤ 5 s, SLO
≤ 30 s. Lag spike during heavy mutation bursts is acceptable; documented and
reported in metrics.

#### 6.3.2 Steady-state query

```
User    : gufi_find /var/lib/pnfs-mds/gufi/datasets -size +1G -mtime -1
gufi_*  : opens per-dir db.sqlite files in parallel
gufi_*  : applies SQL with permission predicates
gufi_*  : returns results
```

The MDS daemon and RonDB are not involved in the query path. The query reads
exclusively from the mirror.

### 6.4 Permission model

GUFI's existing permission enforcement is reused. The mirror schema includes
`uid`, `gid`, `mode`, and (when applicable) directory-walk permissions. GUFI
tools predicate query results on whether the calling user could have reached
the entry via a normal POSIX walk.

Two practical requirements:

1. The changefeed must carry `uid`, `gid`, `mode` for every event so the
   mirror reflects current permissions.
2. The CLI wrappers must pass the calling user's identity to GUFI (via
   `setuid`-like mechanisms or explicit `--user`). Default-deny if the
   caller's identity cannot be established.

Cross-server access (a query running on a host that is not the MDS) must
authenticate the caller. Recommended: same Kerberos/mTLS surface used for
the existing admin transport.

### 6.5 Consistency model

**Decision: eventual consistency, bounded staleness ≤ 30 seconds.**

Rationale: analytical queries (find, du, audit) are dominated by
*aggregation* over many entries; a 30-second window of staleness is
operationally indistinguishable from "now" for these use cases. Strong
consistency would require taking the changefeed write path into the NFSv4
critical section, which defeats the design.

Contract:

- Writes are strongly consistent within RonDB.
- Mirror eventually reflects them within the SLO.
- Queries always see a self-consistent snapshot of the mirror at some point
  in the recent past, never a torn or partial state.
- A `--consistency=fresh` flag on the wrappers can block until the mirror
  has advanced past a target changefeed offset; default is fast and
  eventually-consistent.

### 6.6 Failure handling

- **Sync daemon crash:** restart, replay from persisted cursor. Idempotent
  application means duplicate events are safe.
- **Mirror corruption:** SQLite is robust, but defense in depth — periodic
  consistency check against RonDB row counts; on detected divergence, mark
  the affected subtree for full rebuild.
- **Changefeed lag spike:** monitored via metrics; daemon emits a warning
  when lag exceeds SLO; queries can return a "stale-by" header when
  appropriate.
- **Initial bootstrap:** one-time full scan of `mds_inodes` + `mds_dirents`
  using NDB ordered-index scans. Snapshot consistency via a single
  read-only NDB transaction at the start of the scan; events that arrive
  during the scan are queued and replayed at the end.
- **MDS active-active:** each MDS in the cluster can run its own sync
  daemon and maintain an independent mirror, or one designated host can
  maintain a single shared mirror. Phase 1 ships with per-host mirrors
  (simpler, no HA concerns); production deployments can choose.

## 7. Performance considerations

### 7.1 Write amplification

Every namespace-mutation operation (CREATE, REMOVE, RENAME, SETATTR) now
also incurs an asynchronous mirror update. The mirror update is
out-of-band of the NFSv4 reply, so the hot path is unaffected. The cost
falls on the sync daemon's CPU and the mirror SSD's write bandwidth.

Estimated cost at the current ~2 K creates/sec:

- Sync throughput: ~2 K SQLite inserts/sec into the appropriate parent dir's
  db. Trivial for SQLite with WAL mode.
- Storage growth: ~200 bytes per entry → ~400 KB/sec of mirror writes at
  steady state, easily handled by any modern SSD.

At 100K creates/sec (forward-looking target), the sync daemon becomes a real
workload but remains within single-host capacity given proper sharding.

### 7.2 Storage cost

A SQLite mirror of a billion-file namespace is roughly:

- 200 bytes/entry × 1B entries = ~200 GB raw
- With SQLite indexes: ~400–600 GB
- Per-directory db files have per-file overhead, so deep narrow trees waste
  more than wide shallow trees.

Typical: 2–5% of underlying namespace storage. Budget accordingly.

### 7.3 Query performance

GUFI's published numbers (sub-second `find` on billion-file trees) apply
once the mirror is populated. The pnfs-mds integration does not change
GUFI's query characteristics.

## 8. Open design questions for LANL

These are the questions that benefit most from direct input from the GUFI
team and should be discussed in a joint design session:

1. **Live changefeed-driven update mode.** GUFI was designed primarily for
   offline indexing (snapshot a tree, query the snapshot). Has the team
   considered live-updated mirrors? Are there schema or tooling
   considerations specific to that mode?
2. **Backend abstraction roadmap.** Is there existing or planned work on a
   plugin interface that would let GUFI tools speak to alternative
   backends? If yes, the Phase 2 plan can build on it directly.
3. **Permission semantics under concurrent updates.** A query running while
   a `chmod` is propagating — what is the expected behavior? GUFI today
   assumes a static snapshot.
4. **xattr representation.** GUFI's schema includes an `xattrs` column.
   pnfs-mds has an `mds_xattrs` table. How should xattrs flow into the
   mirror, and what's the encoding the GUFI tools expect?
5. **Subtree scoping.** Most production deployments will not want a mirror
   of the entire namespace. What does GUFI assume about partial trees?
6. **Joint benchmark plan.** What real workloads (Trinity, Crossroads,
   future LANL systems) can we use as validation targets?
7. **Joint publication interest.** A "live-mirrored GUFI on a transactional
   metadata store" paper is a novel contribution. Worth scoping early so
   engineering and writing happen in parallel.

## 9. Phased roadmap

### Phase 0 — joint design session (~2 weeks)

- Resolve open design questions (Section 8) with the LANL team.
- Finalize schema mapping and changefeed event contract.
- Agree on consistency SLO.
- Identify benchmark workloads.

### Phase 1 — minimum-viable mirror (~6–10 weeks)

- Implement `mds-gufi-sync` daemon (initial bootstrap + incremental
  application).
- Define the changefeed event format extensions needed for full mirror
  fidelity (uid, gid, mode, xattrs).
- Ship CLI wrappers (`mds-find`, `mds-du`, `mds-ls`, `mds-query`).
- Documentation: deployment guide, query examples, consistency semantics.
- Basic metrics: lag, throughput, error rate.
- Tests: unit (idempotent application, crash recovery), integration
  (mutate-then-query SLO verification).

### Phase 2 — production hardening + Option C scoping (~3 months)

- Resource isolation: ensure heavy queries don't impact NFS traffic.
- Active-active mirror policy: per-host vs designated-keeper.
- Joint design with LANL on the GUFI backend abstraction.
- Prototype a RonDB backend for GUFI as a separate component.

### Phase 3 — upstream contribution (~6+ months)

- Land the GUFI backend abstraction upstream.
- Land the RonDB backend.
- Joint paper submission.

## 10. Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
| ---- | ---------- | ------ | ---------- |
| Sync lag exceeds SLO under burst load | Medium | Medium | Monitor; allow per-subtree scoping; size sync daemon for peak load |
| SQLite mirror corruption | Low | High | WAL mode + periodic consistency checks; subtree rebuild on detected divergence |
| Permission bug exposes data | Low | Critical | Reuse GUFI's tested permission enforcement; explicit access tests in CI |
| Changefeed primitive insufficient | Medium | High | Validate the changefeed event surface in Phase 0; extend if needed |
| LANL collaboration shape diverges | Low | Medium | Joint design session up front; clearly defined milestones; written agreement on Phase 2/3 scope |
| Storage cost surprises customers | Medium | Low | Document budget guidance; allow scoped mirrors |

## 11. Alternatives considered (and rejected)

- **Embed GUFI as a library in the MDS daemon.** Rejected: tighter coupling,
  no crash isolation, drags SQLite into the NFSv4 hot path. Sidecar process
  is the standard pattern for this kind of integration.
- **Run GUFI's standard offline indexer periodically.** Rejected: stale
  results between runs; full re-index every cycle is expensive at scale;
  defeats the "live query" use case.
- **Use the RonDB MySQL frontend as the only query path.** Rejected for
  Phase 1: requires reimplementing GUFI's tooling and permission semantics;
  loses the joint LANL story. Reasonable as a debug/admin path in addition
  to the mirror.
- **Skip the mirror entirely; query NDB directly from custom CLI.**
  Considered as Option B; rejected because Phase 2's upstream backend
  abstraction is the cleaner long-term shape and Phase 1's mirror is the
  faster validation path.

## 12. Future work

- **Phase 2 backend abstraction (Option C).** Contribute upstream so GUFI
  tools speak to centralized metadata stores natively. Eliminates the
  sync lag concern entirely.
- **Subtree summarization.** Pre-aggregate counts and sizes per directory
  on changefeed application so `du`-class queries are O(depth) rather than
  O(entries). Already a GUFI feature; ensure the mirror updates summaries
  correctly.
- **Query result streaming via gRPC.** Long queries currently block on a
  single CLI invocation. A streaming API enables building dashboards and
  Web UIs.
- **Integration with policy engines (tiering, GC).** A GUFI-style query is
  the natural way to drive bulk policy actions ("demote everything not
  accessed in 90 days"). Wire the query layer into the existing tiering
  and rebalance machinery.

## 13. Appendix: example queries

### 13.1 Find all files modified in the last 24 hours under `/datasets`

```sh
gufi_find /var/lib/pnfs-mds/gufi/datasets -mtime -1
```

### 13.2 Per-user disk usage under `/scratch`

```sh
gufi_query \
  -E "SELECT uid, SUM(size) AS bytes FROM entries WHERE type='f' GROUP BY uid" \
  /var/lib/pnfs-mds/gufi/scratch
```

### 13.3 Cold-data candidates for tier demotion

```sh
gufi_find /var/lib/pnfs-mds/gufi -atime +90 -size +100M
```

### 13.4 Audit: files owned by a departed user

```sh
gufi_find /var/lib/pnfs-mds/gufi -uid 4711
```

### 13.5 Custom: count files per type globally

```sh
gufi_query \
  -E "SELECT type, COUNT(*) FROM entries GROUP BY type" \
  /var/lib/pnfs-mds/gufi
```

## 14. References

- LANL GUFI: <https://github.com/mar-file-system/GUFI>
- pnfs-mds catalog-image changefeed: `src/catalogue/catalog_image.c`
- pnfs-mds RonDB schema: `src/catalogue/rondb_schema.h`
- pnfs-mds admin tooling: `src/tools/mds_admin.c`

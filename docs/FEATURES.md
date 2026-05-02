# pNFS MDS — Feature Status

**Last verified:** 2026-04-10 | **Build:** `cedfe07` | **Tests:** 53/53 pass
**Lab cluster:** 1×MDS (RonDB) + 2×DS + 1×Client | **Stress test:** 16 tasks, 336,800 ops, zero crashes

This document lists features by implementation status based on actual
code, unit/integration tests, and lab verification.  Status is
**Verified** (tested on live cluster), **Implemented** (code + unit tests),
**Partial**, or **Not Implemented**.

## Metadata Backend

| Feature | Status | Notes |
|:--------|:-------|:------|
| RonDB catalogue (primary) | Verified | Full authority + coordination vtables, 9 NDB tables |
| Pluggable catalogue vtable | Verified | `catalogue_dispatch.c` routes to active backend |
| LMDB catalogue (legacy) | Implemented | Used only in test fixtures; production binary uses stubs |
| Per-thread fileid pools | Verified | `_Thread_local` batch alloc, zero contention (`e90e00c`) |
| Fused LOOKUP (1 NDB txn) | Verified | dirent + inode read in single transaction (`c31562e`) |
| Fused REMOVE reads (2 txns) | Verified | dirent + inode fused, then atomic delete (`28f2c56`) |
| Atomic parent-inode mutations | Verified | NDB `interpretedUpdateTuple` for nlink/mtime/change |
| Serialized setattr | Verified | Exclusive row-lock + update in one NDB transaction |
| Inode cache (32K LRU) | Verified | Striped, pre-warmed with root inode, CREATE warmup (`724ec71`) |
| Dirent cache (32K LRU) | Verified | Negative entry TTL (5s default) |
| NDB connection pool | Verified | Auto-sized to `min(worker_threads, 8)`, max 32 |
| NDB transient retry | Verified | 3 attempts with 500µs backoff |
| Commit queue (direct dispatch) | Verified | Writer thread removed; ops go directly to vtable (`3f825b2`) |

## Metadata Operations

| Feature | Status | Notes |
|:--------|:-------|:------|
| mkdir / rmdir | Verified | Nested directories, atomic parent nlink |
| create (touch/open) | Verified | Regular files, directories, symlinks |
| remove (unlink) | Verified | Files and empty directories |
| rename (same directory) | Verified | Atomic within one MDS |
| rename (cross-directory) | Verified | Both dirs on same MDS |
| hard link (same MDS) | Verified | nlink incremented, shared inode |
| symlink | Verified | Create, readlink, follow |
| readdir | Verified | Pagination, per-entry attrs |
| getattr / stat | Verified | All standard attrs |
| setattr / chmod / chown | Verified | Mode, uid, gid persisted |
| deep directory trees | Verified | Tested 6+ levels |
| rename (cross-subtree 2PC) | Implemented | Unit tested; requires multi-MDS lab for live test |
| hard link (cross-subtree) | Not Implemented | Would require distributed inode references |

## Concurrency & Thread Safety

| Feature | Status | Notes |
|:--------|:-------|:------|
| 8-task parallel create/remove | Verified | 7,440 files, zero errors |
| 16-task parallel create/remove | Verified | 336,800 ops, zero crashes (`cedfe07`) |
| Fileid allocator thread safety | Verified | Per-thread pools, no shared state |
| Open state hash chain safety | Verified | Lock stripe aligned to hash bucket (`cedfe07`) |
| Striped open state locks (16) | Verified | Per-fileid-bucket mutexes + per-stateid rwlocks |
| Striped lock table (16) | Implemented | Unit tested |
| Worker thread pool | Verified | Configurable `worker_threads`, epoll + pool dispatch |

## Data I/O

| Feature | Status | Notes |
|:--------|:-------|:------|
| Small file write+read | Verified | Data integrity verified |
| Large write (1 MB) | Verified | md5 checksum verified |
| Append / overwrite | Verified | Multi-line append, in-place overwrite |
| Truncate | Verified | truncate(2) to 0 |
| Sparse files | Verified | seek past EOF |
| Parallel writes (8+ procs) | Verified | Concurrent file creation |
| Striping across DSes | Not Implemented | Single stripe per file (round-robin DS placement) |
| Multi-stripe per file | Not Implemented | stripe_count=1 only |
| Mirror (replication) | Not Implemented | mirror_count=1 only |

## pNFS Layout

| Feature | Status | Notes |
|:--------|:-------|:------|
| Flex-files layout (RFC 8435) | Verified | Client sees pnfs=LAYOUT_FLEX_FILES |
| LAYOUTGET | Verified | Returns DS address + NFS file handle |
| GETDEVICEINFO | Verified | DS address parsed by kernel |
| LAYOUTCOMMIT | Verified | Size updates propagated |
| LAYOUTRETURN | Verified | Clean lifecycle |
| CB_LAYOUTRECALL | Implemented | DS failure triggers recall via vtable |
| DS round-robin placement | Verified | Files distributed across DSes |
| DS file pre-creation | Verified | Files created on DS during LAYOUTGET |
| Fused CREATE+LAYOUTGET | Verified | Single NDB txn for RonDB path |
| DS protocol: NFSv3 | Verified | Standard flex-files model |
| DS protocol: NFSv4 | Not Working | Kernel returns EINVAL on 6.17+ |
| Dual-transport (TCP+RDMA) | Implemented | Advertised in GETDEVICEINFO; RDMA not lab-tested |

## Session & State

| Feature | Status | Notes |
|:--------|:-------|:------|
| EXCHANGE_ID | Verified | USE_PNFS_MDS flag |
| CREATE_SESSION | Verified | Slot table, DRC |
| DESTROY_SESSION | Verified | |
| SEQUENCE (per-slot DRC) | Verified | Exactly-once semantics |
| OPEN (CLAIM_NULL, CLAIM_FH, CLAIM_PREVIOUS) | Verified | |
| CLOSE | Verified | Stateid lifecycle |
| Share reservations | Verified | RFC 8881 §9.1.1 conflict detection |
| Delegations (READ/WRITE) | Implemented | OPEN grants, DELEGRETURN handler; CB_RECALL delivery deferred |
| Grace period | Verified | Configurable, 0 for testing |
| Session recovery via RonDB | Implemented | `mds_coord_recovery_*` vtable path |

## NFSv4.2 Operations (RFC 7862)

| Feature | Status | Notes |
|:--------|:-------|:------|
| COPY (sync + async) | Verified | Chunked copy via proxy |
| ALLOCATE / DEALLOCATE | Verified | posix_fallocate / punch hole on DS |
| SEEK (data/hole) | Verified | SEEK_DATA / SEEK_HOLE on DS |
| CLONE (reflink) | Verified | FICLONERANGE with copy fallback |
| IO_ADVISE | Verified | Hints echoed back |
| LAYOUTERROR / LAYOUTSTATS | Verified | DS error + I/O stats reporting |
| OFFLOAD_STATUS / OFFLOAD_CANCEL | Verified | Async copy lifecycle |
| READ_PLUS / WRITE_SAME | Verified | DATA+HOLE segments / pattern write |

## Cluster (Multi-MDS)

| Feature | Status | Notes |
|:--------|:-------|:------|
| RonDB shared metadata authority | Verified | 2 MDS daemons, concurrent mkdir/rmdir |
| fs_locations referrals | Verified | NFS4ERR_MOVED flow |
| Cross-subtree rename (2PC) | Implemented | Unit tested; not lab-verified multi-MDS |
| Subtree migration | Implemented | Admin-triggered via mds-admin |
| Node registry + heartbeat | Verified | RonDB `node_registry` table, 5s interval |
| Multi-MDS row-level locking | Implemented | `rondb_locked_authority_ops` vtable |
| Failover / promotion | Implemented | Unit + integration tested; requires LMDB replication (not available in RonDB mode) |
| LMDB replication (delta shipping) | Removed | Writer thread removed; RonDB handles replication natively |

## Data Server Management

| Feature | Status | Notes |
|:--------|:-------|:------|
| DS registry (RonDB) | Verified | CRUD via admin CLI + CQ dispatch |
| DS health monitor | Implemented | Code exists; `ds_heartbeat_ms=0` disables |
| DS rebalance | Implemented | Code exists, not lab-tested |
| DS resilver | Implemented | Code exists, not lab-tested |
| Storage tiering | Implemented | Code exists, not lab-tested |
| DS drain | Implemented | Code exists, not lab-tested |
| GC queue | Verified | Async data file cleanup after metadata deletion |

## Proxy I/O (non-pNFS clients)

| Feature | Status | Notes |
|:--------|:-------|:------|
| READ/WRITE/COMMIT through MDS | Verified | Stripe-aware forwarding |
| Inline data (small files ≤64KB) | Implemented | LMDB-only; disabled for RonDB |

## Security

| Feature | Status | Notes |
|:--------|:-------|:------|
| AUTH_SYS | Verified | Default, tested on lab |
| POSIX permission enforcement | Verified | Mode bits, uid/gid checked |
| Kerberos (krb5/krb5i/krb5p) | Implemented | Config + GSS init code exists; not lab-tested |
| mTLS (cluster transport) | Implemented | Config + TLS code exists; not lab-tested |

## Known Limitations

1. **Single-stripe files only** — each file lives on one DS
2. **No cross-subtree hard links** — operations that span MDS boundaries
3. **NFSv4 DS protocol broken on kernel 6.17+** — use NFSv3 DS
4. **Inline data disabled for RonDB** — small files go through DS
5. **CB_RECALL not delivered** — delegations are revoke-only
6. **Failover requires LMDB replication** — not functional in RonDB-only mode

## Performance (Lab, 2026-04-10)

### Single client (mdtest, nconnect=8)

| Operation | ops/sec |
|-----------|---------|
| File creation | 127 |
| Directory creation | 202 |
| File stat | 317 |
| File removal | 365 |

### 8 parallel tasks (mdtest, nconnect=8)

| Operation | ops/sec |
|-----------|---------|
| File creation | 1,911 |
| Directory creation | 3,024 |
| File removal | 2,443 |

### 16 parallel tasks (mdtest, nconnect=16, stress test)

| Operation | ops/sec |
|-----------|---------|
| File creation | 2,094 |
| Directory creation | 3,507 |
| File removal | 2,541 |
| **Total ops** | **336,800 (zero errors)** |

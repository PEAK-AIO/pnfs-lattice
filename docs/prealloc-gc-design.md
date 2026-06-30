# DS prealloc rings + per-MDS lazy-delete GC — audit & implementation spec

Status: audit complete, decisions locked, implementation pending.
Branch: `scale-fix`. Build verified possible on dev host (`/opt/rondb`, cmake 3.22).

## 1. Audit findings (current state)

### Lazy delete / GC — IMPLEMENTED, 2 gaps
- `op_remove` is already lazy: `compound_namespace.c:~1450` calls `mds_cat_ns_remove`
  (namespace + stripe rows deleted in the NDB txn, parent attrs updated) **and**
  `mds_cat_gc_enqueue(fileid, ds_id, nfs_fh)`. No DS unlink in the RPC.
- `src/modules/ds_gc/ds_gc.c`: real multi-worker drainer (coordinator batch-peeks
  `mds_gc_queue`, bounded queue, N workers `mds_proxy_unlink_ds_file`). Wired in
  `main.c:~995` via `ds_gc_start_ex(cat, proxy, 5000, ds_gc_workers=4, ds_gc_batch_size=256)`.
  Entries persisted → survive restart.
- **Gap G1**: `mds_gc_queue` is a single GLOBAL table keyed by `gc_seq`; `rondb_shim_gc_peek_batch`
  scans ALL rows with no mds_id filter → multi-MDS workers contend the same rows.
- **Gap G2**: no `gc_pending` gauge → pnfs-mds-top can't show GC backlog.

### Prealloc / precreate rings — NOT IMPLEMENTED
- Build links `ds_prealloc_stub.c` (synchronous-on-demand): every `pop()` does
  select + alloc-fileid + DS-FH-capture inline → no rings, no refill, no 100% burst hit.
- The "enterprise" `ds_prealloc.c` referenced everywhere DOES NOT EXIST in the repo;
  `ENABLE_DS_PREALLOC=ON` would fail to link. Default build is the stub.
- Metric scaffolding exists but is dead: `mds_metrics.h` already declares
  `prealloc_pops_ok/empty/fh_missing`, `prealloc_refill_entries/batches`; the stub
  never increments them. Prometheus emit for them is already in `mds_metrics.c:~330`.

## 2. Locked design decisions
- **D1 Prealloc FH store**: persisted RonDB pool table (`mds_prealloc_pool`:
  fileid PK, ds_id, nfs_fh_len, nfs_fh, owner_mds_id, state free/used). Survives
  restart, no orphans. One NDB write per slot refill (accepted).
- **D2 Per-MDS GC**: add `owner_mds_id` column to the single `mds_gc_queue` table;
  enqueue tags it, peek/count filter by it. No new tables.
- **D3 Order**: GC gaps first (smaller, infra exists), then prealloc ring engine.

## 3. Implementation — Phase 1: per-MDS GC + pending metric

Inject the owner id at the RonDB backend layer using the already-present
`mds_rondb_state.mds_id` (`catalogue_rondb.c:47,255`). **No changes to the
dispatch vtable signatures or the namespace call sites** (compound_namespace.c,
commit_queue.c, hpc_shared.c, mover_util.c, rename_2pc.c stay as-is).

Edits:
1. `src/catalogue/rondb_schema.h`: add `#define RONDB_GC_COL_OWNER_MDS "owner_mds_id"`.
2. `include/pnfs_mds.h`: add `uint32_t owner_mds_id;` to `struct mds_gc_entry`.
3. `include/catalogue_rondb.h`: extend shim decls —
   `rondb_shim_gc_enqueue(..., uint32_t owner_mds_id)`,
   `rondb_shim_gc_peek_batch(..., uint32_t self_mds_id)`,
   `rondb_shim_gc_count(..., uint32_t self_mds_id)`.
4. `src/catalogue/catalogue_rondb_shim.cpp`:
   - DDL `~2322`: `rondb_add_unsigned(tbl, RONDB_GC_COL_OWNER_MDS);`
   - `gc_enqueue`: `op->setValue(RONDB_GC_COL_OWNER_MDS, owner_mds_id);`
   - `gc_peek_batch` + `gc_count`: read owner col, skip rows where
     `owner != self_mds_id && owner != 0` (0 = legacy/unassigned, any MDS reclaims).
5. `src/catalogue/catalogue_rondb.c`: add `rondb_self_mds_id(cat)` helper
   (returns `state->mds_id`); pass it from the 3 gc wrappers (`~1924/1980/1994`).
6. `include/mds_metrics.h`: add `_Atomic uint64_t gc_pending;` (gauge).
   `src/common/mds_metrics.c`: emit `pnfs_mds_gc_pending` (gauge) + add setter
   `mds_metrics_set_gc_pending(uint64_t)`.
7. `src/modules/ds_gc/ds_gc.c`: in `ds_gc_coordinator_main`, after each
   `mds_cat_gc_peek_batch`, call `mds_cat_gc_count` and
   `mds_metrics_set_gc_pending(n)` (add mds_metrics dep to the ds_gc CMake target).
8. `scripts/pnfs-mds-top`: parse + display `pnfs_mds_gc_pending`.

Live-cluster migration note: `mds_gc_queue` already exists on the running cluster.
Adding a column needs an online `ALTER TABLE ... ADD owner_mds_id INT UNSIGNED
DEFAULT 0` (or drain + recreate, since the queue is transient). The drainer's
`owner==0 → reclaimable` rule keeps pre-existing rows drainable during rollout.
This is the one step that touches live infra — coordinate before applying.

## 4. Implementation — Phase 2: prealloc ring engine (`ds_prealloc.c`, ENABLE_DS_PREALLOC=ON)

New `src/modules/ds_prealloc/ds_prealloc.c` (replaces stub when option ON):
- **Rings**: `M` rings (config `prealloc_ring_count`, default = min(ds_count, ncpu));
  each ring owns a DS subset (round-robin DS→ring). In multi-MDS, a ring only holds
  DSes in this MDS's assigned DS group.
- **Slots**: each ring = fixed array (`prealloc_pool_size / ring_count` slots) of
  precreated `(fileid, ds_id, nfs_fh)` rows. Backed by `mds_prealloc_pool` (D1):
  refill INSERTs a free row; pop marks used; on consume the create copies the FH
  into the file's stripe-map row.
- **Refill workers**: one thread per ring, started at daemon boot (right after
  catalogue+proxy up). Each loops: count free slots, for each empty slot
  alloc fileid → `mds_proxy_ensure_ds_file_fh` (precreate 0-byte DS file + capture
  FH) → persist pool row → publish into the in-RAM ring. Increment
  `prealloc_refill_entries/batches`.
- **pop()**: O(1) take from the round-robin-selected ring (lock or MPSC). On hit
  increment `prealloc_pops_ok`; on empty fall back to synchronous select+capture
  (current stub logic) and increment `prealloc_pops_empty`. Guarantees correctness;
  100% burst hit comes from deep enough rings + fast refill.
- **Stats** (already-declared counters + new): pops_ok/empty, refill_entries/batches,
  plus add `prealloc_ring_depth` (gauge, summed free slots) and derive hit ratio
  in pnfs-mds-top (`pops_ok / (pops_ok+pops_empty)`).
- **Multi-MDS DS groups**: DS→MDS assignment from the cluster's DS ownership (the
  subtree/DS map); a ring only precreates on DSes this MDS owns, so two MDSes never
  precreate on the same DS slot space.
- Schema: `mds_prealloc_pool` table + DDL in the shim; new shim ops
  pool_insert/pool_claim_free/pool_mark_used/pool_count_free + `catalogue_rondb.c`
  wrappers + dispatch decls.
- `main.c`: pass `prealloc_ring_count` + DS group to `ds_prealloc_init_ex`;
  start refill workers; `ds_prealloc_destroy` stops them.
- Build the cluster with `-DENABLE_DS_PREALLOC=ON`.

## 5. Validation
- GC: unlink a tree, watch `pnfs_mds_gc_pending` rise then drain; in multi-MDS confirm
  each MDS only drains its own rows.
- Prealloc: create burst (mdtest -C) and confirm `prealloc_pops_ok` ≈ 100% of creates,
  `pops_empty` ≈ 0 once rings are warm; check ring depth stays > 0 under sustained load.

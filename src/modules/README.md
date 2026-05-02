# src/modules — opt-in feature modules
This directory holds enterprise-only feature modules that can be
compiled out for the community edition. Each module is gated by a
top-level CMake option:
```
cmake -DENABLE_RESILVER=OFF -DENABLE_QUOTA=OFF -D... ..
```
With every gate `ON` (the default) the build produces the full
enterprise binary. With every gate `OFF` the build produces the
community binary — same protocol surface, no enterprise modules.
## Convention
For a module called `foo`:
```
src/modules/foo/
├── CMakeLists.txt         # picks foo.c (real) or foo_stub.c (community)
├── foo.c                  # real implementation
└── foo_stub.c             # no-op stubs of every function in include/foo.h
```
The public header `include/foo.h` stays unchanged. Call sites in
core code do not move. Only the linked `.c` differs between
enterprise and community.
The CMake glue is intentionally tiny:
```cmake
if(ENABLE_FOO)
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/foo.c)
else()
    target_sources(pnfs_mds_core PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/foo_stub.c)
endif()
```
## Why stubs, not `#ifdef` everywhere
- Call sites stay readable. No `#ifdef ENABLE_FOO` scattered through
  protocol-critical paths.
- Stubs document the "feature disabled" semantics in one place: a
  stub `mds_quota_check()` returning `MDS_OK` says "quota disabled =
  always allow" once, instead of forcing every caller to know.
- Compile-time gating avoids runtime cost and keeps the binary lean.
- A stub-built binary still exposes the API surface so out-of-tree
  callers (tests, mds-admin) link cleanly.
## Adding a new module
1. Create `src/modules/<name>/{CMakeLists.txt,<name>.c,<name>_stub.c}`.
2. Move the real implementation from `src/mds/<name>.c` (or wherever
   it lives) into the new directory.
3. Write `<name>_stub.c` with no-op implementations of every symbol
   declared in `include/<name>.h`. The stubs should match the
   semantic intent: "feature disabled, return success / safe value".
4. Add the option to the top-level `CMakeLists.txt`:
   `option(ENABLE_<NAME> "Enable <name> module" ON)`.
5. Append `add_subdirectory(<name>)` to `src/modules/CMakeLists.txt`.
6. Remove the source from `src/mds/CMakeLists.txt` (or wherever it
   was previously listed).
7. Verify both `cmake -DENABLE_<NAME>=ON ..` and
   `cmake -DENABLE_<NAME>=OFF ..` build clean.
8. Confirm the test for the module either skips or `add_test` is
   gated behind the option in `tests/CMakeLists.txt`.
## Currently extracted modules
| Module | Source | Default | Notes |
|---|---|---|---|
| `resilver` | `src/modules/resilver/` | ON | Background resilvering for degraded mirrored files. |
| `rebalance` | `src/modules/rebalance/` | ON | Admin-triggered mirror relocation between data servers. |
| `tiering` | `src/modules/tiering/` | ON | DS storage tiering: promote/demote files based on I/O scores. |
| `observability` | `src/modules/observability/` | ON | Prometheus /metrics HTTP endpoint. |
| `replication` | `src/modules/replication/` | ON | Cluster health monitor for replicated MDS pairs. |
| `layout_cache` | `src/modules/layout_cache/` | ON | Sharded LRU cache of stripe maps for fast LAYOUTGET. |
| `layout_commit_aggregator` | `src/modules/layout_commit_aggregator/` | ON | Per-fileid LAYOUTCOMMIT coalescing for write-heavy fan-in. |
| `ds_gc` | `src/modules/ds_gc/` | ON | Background drainer that unlinks DS files after MDS-side delete. |
| `ds_prealloc` | `src/modules/ds_prealloc/` | ON | Pre-allocated DS placements to amortise CREATE cost. |
| `wrr` | `src/modules/wrr/` | ON | Weighted-round-robin + capacity-derived placement kernels (community degrades to plain RR). |
| `quota` | `src/modules/quota/` | ON | User/group quota enforcement (community is unbounded; all checks allow). |

# FDB 可行性分析 — 交付物 0：catalogue / coordination 接口与调用点清单

> 计划文件：`foundationdb-feasibility-analysis-plan.html`（启动清单第 4 项：从源码生成 catalogue / coordination 接口与调用点清单）。
> 基线 commit：`6b4dcde`（main，2026-09-30）。文中数字均可由第 7 节命令复核。
> 证据等级：REFERENCE（源码取证，不得用于评分）。

## 1. 后端抽象结构（现状事实）

```
struct mds_catalogue {
    enum mds_catalogue_backend        backend;
    const struct mds_authority_ops    *auth_ops;   /* 数据平面 vtable */
    const struct mds_coordination_ops *coord_ops;  /* 恢复关键状态 vtable */
    const struct mds_catalogue_ops    *ops;        /* 生命周期 close/probe */
    void *backend_private;  ...
};
```

调用链：**业务代码 → `mds_cat_*` / `mds_coord_*` 公开包装函数（include/mds_catalogue.h、mds_coordination.h）→ src/catalogue/catalogue_dispatch.c → vtable → 后端实现**。
当前后端实现只有一个：RonDB（`src/catalogue/catalogue_rondb.c` 4743 行 + `catalogue_rondb_shim.cpp` 15982 行；shim 是全项目唯一 C++ 翻译单元，封装 NDB API）。
**仓库中尚无任何 FoundationDB 代码**（全库检索 `catalogue_fdb` / `FoundationDB` 仅命中计划文件本身）。

RonDB 侧有两个 authority vtable 变体：
- `rondb_authority_ops`（63 槽填充，默认）：create/remove/link/setattr 直接走 NDB 行锁 + interpretedUpdate；
- `rondb_locked_authority_ops`：仅 `ns_rename` / `ns_rename_flags` 换用应用层目录锁包装（跨目录 RENAME 在原子提交前有多次独立 NDB 读，需 TOPOLOGY 锁串行化，见 catalogue_rondb.c:4173 注释；Phase 12 实证：去掉应用层锁后 16 路竞争下 create 吞吐回升约 96%）。

## 2. vtable 槽位总账：authority 71 + coordination 41 + 生命周期 2 = 114

- RonDB 填充：authority **63/71**，coordination **39/41**
- authority 留 NULL（未实现）：`ext_dirent_del`, `ext_dirent_get`, `ext_dirent_put`, `link_anchor_del`, `link_anchor_put`, `shard_fileid_del`, `shard_fileid_get`, `shard_fileid_put`（均为跨分片 / ext-dirent / link-anchor 操作）
- coordination 留 NULL（未实现）：`lock_test`, `lock_scan_owner`（源码注释标记 Stage 4 TODO，catalogue_rondb.c:4509/4511）
  dispatch 对 NULL 槽返回 MDS_ERR_NOSUPPORT，对应功能自动禁用。

### 2.1 authority（数据平面）（71 槽）

| 槽位 | RonDB 实现 | 声明为可选* |
|---|---|---|
| `ns_create` | ✅ |  |
| `ns_create_wide` | ✅ |  |
| `ns_remove` | ✅ |  |
| `ns_remove_known` | ✅ | 可选 |
| `ns_remove_known_gc` | ✅ | 可选 |
| `ns_parent_touch` | ✅ |  |
| `remove_pending_enqueue` | ✅ |  |
| `remove_pending_enqueue_unlink` | ✅ |  |
| `remove_pending_peek_batch` | ✅ |  |
| `remove_pending_claim` | ✅ |  |
| `remove_pending_complete` | ✅ |  |
| `remove_pending_bump_retry` | ✅ |  |
| `remove_pending_count` | ✅ |  |
| `remove_pending_scan_all` | ✅ |  |
| `ns_rename` | ✅ |  |
| `ns_rename_flags` | ✅ | 可选 |
| `ns_link` | ✅ |  |
| `ns_lookup` | ✅ |  |
| `ns_getattr` | ✅ |  |
| `ns_setattr` | ✅ |  |
| `ns_readdir` | ✅ |  |
| `dirent_name_for_child` | ✅ | 可选 |
| `ns_readdir_plus` | ✅ | 可选 |
| `ns_readdir_plus_from` | ✅ | 可选 |
| `ns_nlink_adjust` | ✅ |  |
| `alloc_fileid` | ✅ |  |
| `inode_put` | ✅ |  |
| `inode_del` | ✅ |  |
| `dirent_put` | ✅ |  |
| `dirent_insert` | ✅ |  |
| `dirent_del` | ✅ |  |
| `inline_get` | ✅ |  |
| `inline_put` | ✅ |  |
| `inline_del` | ✅ |  |
| `xattr_get` | ✅ |  |
| `xattr_put` | ✅ |  |
| `xattr_del` | ✅ |  |
| `xattr_list` | ✅ |  |
| `xattr_exists` | ✅ |  |
| `stripe_map_get` | ✅ |  |
| `stripe_map_put` | ✅ |  |
| `stripe_map_del` | ✅ |  |
| `stripe_map_scan` | ✅ |  |
| `ds_get` | ✅ |  |
| `ds_put` | ✅ |  |
| `ds_del` | ✅ |  |
| `ds_list` | ✅ |  |
| `ds_provision_get` | ✅ |  |
| `ds_provision_put` | ✅ |  |
| `ds_provision_del` | ✅ |  |
| `quota_rule_get` | ✅ |  |
| `quota_rule_put` | ✅ |  |
| `quota_usage_get` | ✅ |  |
| `quota_usage_put` | ✅ |  |
| `gc_enqueue` | ✅ |  |
| `gc_peek` | ✅ |  |
| `gc_dequeue` | ✅ |  |
| `gc_count` | ✅ |  |
| `gc_peek_batch` | ✅ | 可选 |
| `prealloc_pool_insert` | ✅ |  |
| `prealloc_pool_delete` | ✅ |  |
| `prealloc_pool_scan` | ✅ |  |
| `shard_fileid_get` | ⬜ NULL |  |
| `shard_fileid_put` | ⬜ NULL |  |
| `shard_fileid_del` | ⬜ NULL |  |
| `ext_dirent_get` | ⬜ NULL |  |
| `ext_dirent_put` | ⬜ NULL |  |
| `ext_dirent_del` | ⬜ NULL |  |
| `link_anchor_put` | ⬜ NULL |  |
| `link_anchor_del` | ⬜ NULL |  |
| `backend_client_stats` | ✅ | 可选 |

\* 可选 = 头注释允许 backend 留 NULL（dispatch 回退或返回 NOSUPPORT）；FDB 后端可按能力逐槽补齐。

### 2.2 coordination（恢复关键状态）（41 槽）

| 槽位 | RonDB 实现 | 声明为可选* |
|---|---|---|
| `journal_put` | ✅ |  |
| `journal_get` | ✅ |  |
| `journal_del` | ✅ |  |
| `journal_scan` | ✅ |  |
| `layout_grant` | ✅ |  |
| `layout_grant_union` | ✅ | 可选 |
| `layout_return` | ✅ |  |
| `layout_get_by_stateid` | ✅ |  |
| `layout_scan_for_file` | ✅ |  |
| `layout_del_all_for_client` | ✅ |  |
| `ds_layout_idx_scan` | ✅ |  |
| `layout_iter_file` | ✅ |  |
| `recovery_put` | ✅ |  |
| `recovery_del` | ✅ |  |
| `recovery_get` | ✅ |  |
| `recovery_list` | ✅ |  |
| `open_put` | ✅ |  |
| `open_get` | ✅ |  |
| `open_del` | ✅ |  |
| `open_scan_file` | ✅ |  |
| `open_scan_client` | ✅ |  |
| `lock_put` | ✅ |  |
| `lock_del` | ✅ |  |
| `lock_test` | ⬜ NULL |  |
| `lock_scan_file` | ✅ |  |
| `lock_scan_owner` | ⬜ NULL |  |
| `lock_reap_client` | ✅ |  |
| `deleg_put` | ✅ |  |
| `deleg_get` | ✅ |  |
| `deleg_del` | ✅ |  |
| `deleg_scan_file` | ✅ |  |
| `deleg_scan_client` | ✅ |  |
| `client_put` | ✅ |  |
| `client_get` | ✅ |  |
| `client_del` | ✅ |  |
| `session_put` | ✅ |  |
| `session_get` | ✅ |  |
| `session_del` | ✅ |  |
| `session_scan_client` | ✅ |  |
| `slot_put` | ✅ |  |
| `slot_get` | ✅ |  |

\* 可选 = 头注释允许 backend 留 NULL（dispatch 回退或返回 NOSUPPORT）；FDB 后端可按能力逐槽补齐。

## 3. 公开包装函数与调用点（83 个 mds_cat_* + 44 个 mds_coord_*）

调用点 = 排除 catalogue 子系统自身（dispatch / rondb / factory / memdb / 两个公共头）后，全库对包装函数的引用计数（含 tests/；测试引用是 FDB 后端必须保持 ABI 兼容的验收依据）。

### 3.1 `mds_cat_*`（按调用点降序）

| 包装函数 | 调用点 | 主要位置 |
|---|---|---|
| `mds_cat_ns_create` | 99 | src/fsal_obj/referral.c, src/mds/commit_queue.c, src/mds/compound_internal.h, src/tools/bench_rondb_create.c |
| `mds_cat_txn_commit` | 92 | src/cluster/hardlink_2pc.c, src/cluster/migration.c, src/cluster/rename_2pc.c, src/mds/compound_data_io.c |
| `mds_cat_txn_begin` | 91 | src/cluster/hardlink_2pc.c, src/cluster/migration.c, src/cluster/rename_2pc.c, src/mds/compound_data_io.c |
| `mds_cat_ns_getattr` | 73 | src/cluster/cluster_transport.c, src/cluster/hardlink_2pc.c, src/cluster/rename_2pc.c, src/cluster/subtree_split.c |
| `mds_cat_txn_abort` | 50 | src/cluster/hardlink_2pc.c, src/cluster/migration.c, src/cluster/rename_2pc.c, src/mds/compound_data_io.c |
| `mds_cat_ds_put` | 41 | src/cluster/cluster_transport.c, src/mds/commit_queue.c, src/mds/ds_capacity.c, src/mds/ds_health.c |
| `mds_cat_inode_put` | 31 | src/cluster/hardlink_2pc.c, src/cluster/migration.c, src/cluster/rename_2pc.c, src/mds/commit_queue.c |
| `mds_cat_stripe_map_get` | 27 | src/cluster/rename_2pc.c, src/mds/compound_internal.h, src/mds/compound_namespace.c, src/mds/ds_prepare.c |
| `mds_cat_stripe_map_put` | 24 | src/cluster/migration.c, src/cluster/rename_2pc.c, src/mds/commit_queue.c, src/mds/compound_data_io.c |
| `mds_cat_ds_list` | 21 | include/placement.h, src/cluster/cluster_transport.c, src/mds/compound_data_io.c, src/mds/compound_internal.h |
| `mds_cat_ns_lookup` | 20 | src/fsal_obj/referral.c, src/mds/compound.c, src/mds/compound_internal.h, src/mds/main.c |
| `mds_cat_dirent_get` | 19 | src/cluster/rename_2pc.c, src/mds/compound_namespace.c, src/mds/main.c, src/mds/remove_manifest.c |
| `mds_cat_alloc_fileid` | 17 | include/hpc_shared.h, src/cluster/hardlink_2pc.c, src/cluster/rename_2pc.c, src/mds/hpc_shared.c |
| `mds_cat_ds_get` | 17 | src/cluster/cluster_transport.c, src/mds/commit_queue.c, src/mds/compound_internal.h, src/mds/ds_capacity.c |
| `mds_cat_resolve_path` | 13 | src/cluster/cluster_drain.c, src/cluster/cluster_transport.c, src/cluster/subtree_split.c, tests/unit/test_drain.c |
| `mds_cat_inline_put` | 12 | src/cluster/migration.c, src/mds/commit_queue.c, src/mds/compound_namespace.c, tests/unit/test_compound.c |
| `mds_cat_ns_remove` | 12 | src/mds/commit_queue.c, src/mds/compound_internal.h, src/mds/main.c, src/tools/bench_rondb_create.c |
| `mds_cat_ns_setattr` | 12 | src/cluster/cluster_transport.c, src/cluster/migration.c, src/fsal_obj/referral.c, src/mds/commit_queue.c |
| `mds_cat_gc_count` | 11 | src/modules/ds_gc/ds_gc.c, tests/unit/test_compound.c, tests/unit/test_ds_gc.c, tests/unit/test_hpc_shared.c |
| `mds_cat_inline_get` | 11 | src/mds/compound_internal.h, tests/unit/test_inline_data.c |
| `mds_cat_gc_enqueue_hint` | 10 | src/cluster/rename_2pc.c, src/mds/commit_queue.c, src/mds/compound_namespace.c, src/mds/hpc_shared.c |
| `mds_cat_stripe_map_del` | 9 | src/cluster/migration.c, src/cluster/rename_2pc.c, src/mds/compound_namespace.c, src/mds/hpc_shared.c |
| `mds_cat_inode_del` | 7 | src/cluster/migration.c, src/cluster/rename_2pc.c, src/mds/compound_namespace.c, src/mds/hpc_shared.c |
| `mds_cat_ns_readdir` | 7 | src/cluster/migration.c, src/cluster/subtree_split.c, src/fsal_obj/referral.c, src/mds/compound_internal.h |
| `mds_cat_inline_del` | 6 | src/cluster/migration.c, src/cluster/rename_2pc.c, src/mds/commit_queue.c, src/mds/compound_data_io.c |
| `mds_cat_dirent_put` | 5 | src/cluster/migration.c, src/cluster/rename_2pc.c, tests/test_helpers.h, tests/unit/test_hpc_shared.c |
| `mds_cat_ds_provision_put` | 5 | src/cluster/cluster_transport.c, tests/integration/bench_create_layout_fusion.c, tests/unit/test_catalogue.c, tests/unit/test_compound.c |
| `mds_cat_ns_create_wide` | 5 | include/hpc_shared.h, src/mds/hpc_shared.c, tests/unit/test_hpc_shared.c |
| `mds_cat_subtree_iter` | 5 | src/cluster/migration.c, src/mds/hpc_shared.c, src/mds/mds_shard.c |
| `mds_cat_backend_client_stats` | 4 | src/modules/observability/metrics_http_lite.c, tests/unit/test_compound.c |
| `mds_cat_dirent_del` | 4 | src/cluster/migration.c, src/cluster/rename_2pc.c |
| `mds_cat_gc_peek` | 4 | tests/unit/test_compound.c, tests/unit/test_rebalance.c |
| `mds_cat_ns_dirent_name_for_child` | 4 | src/mds/hpc_shared.c, src/tools/find_query.c, tests/unit/test_catalogue.c |
| `mds_cat_ns_nlink_adjust` | 4 | src/cluster/cluster_transport.c, src/cluster/rename_2pc.c, src/mds/compound_internal.h, src/mds/compound_namespace.c |
| `mds_cat_ns_readdir_plus` | 4 | src/mds/compound_internal.h, tests/unit/test_catalogue.c |
| `mds_cat_xattr_put` | 4 | src/cluster/migration.c, src/mds/compound_data_io.c, src/mds/compound_internal.h, src/mds/compound_namespace.c |
| `mds_cat_dir_is_empty` | 3 | src/cluster/rename_2pc.c, src/mds/compound_namespace.c |
| `mds_cat_ext_dirent_put` | 3 | src/cluster/hardlink_2pc.c, src/mds/compound_namespace.c, tests/unit/test_compound.c |
| `mds_cat_ns_rename` | 3 | src/cluster/rename_2pc.c, src/mds/commit_queue.c, src/mds/compound_internal.h |
| `mds_cat_ns_rename_flags` | 3 | src/mds/compound_internal.h, tests/unit/test_catalogue.c |
| `mds_cat_prealloc_pool_delete` | 3 | src/modules/ds_prealloc/ds_prealloc.c |
| `mds_cat_remove_pending_complete` | 3 | src/mds/remove_manifest.c, tests/integration/bench_mk_rm_scale.c, tests/unit/test_remove_manifest.c |
| `mds_cat_remove_pending_enqueue_unlink` | 3 | src/mds/remove_manifest.c, tests/integration/bench_mk_rm_scale.c, tests/unit/test_remove_manifest.c |
| `mds_cat_shard_fileid_get` | 3 | src/mds/compound_namespace.c |
| `mds_cat_shard_fileid_put` | 3 | src/cluster/rename_2pc.c, src/mds/mds_shard.c, tests/unit/test_compound.c |
| `mds_cat_stripe_map_scan` | 3 | tests/unit/test_resilver.c |
| `mds_cat_xattr_del` | 3 | src/cluster/migration.c, src/mds/compound_internal.h, src/mds/compound_namespace.c |
| `mds_cat_xattr_exists` | 3 | src/mds/compound_internal.h, tests/unit/test_compound.c |
| `mds_cat_dirent_insert` | 2 | tests/unit/test_catalogue.c |
| `mds_cat_ds_del` | 2 | src/cluster/cluster_transport.c, src/mds/commit_queue.c |
| `mds_cat_ds_provision_get` | 2 | src/mds/compound_internal.h, src/mds/ds_cache.c |
| `mds_cat_gc_peek_batch` | 2 | include/ds_gc.h, src/modules/ds_gc/ds_gc.c |
| `mds_cat_link_anchor_put` | 2 | src/cluster/hardlink_2pc.c, src/mds/compound_namespace.c |
| `mds_cat_ns_link` | 2 | src/mds/commit_queue.c, src/mds/compound_internal.h |
| `mds_cat_ns_remove_known` | 2 | src/mds/compound_internal.h, src/mds/hpc_shared.c |
| `mds_cat_quota_usage_get` | 2 | src/mds/commit_queue.c, tests/unit/test_quota.c |
| `mds_cat_remove_pending_claim` | 2 | src/mds/remove_manifest.c |
| `mds_cat_remove_pending_count` | 2 | src/mds/remove_manifest.c, tests/unit/test_remove_manifest.c |
| `mds_cat_remove_pending_enqueue` | 2 | src/mds/remove_manifest.c, tests/unit/test_remove_manifest.c |
| `mds_cat_remove_pending_scan_all` | 2 | src/mds/remove_manifest.c, tests/unit/test_remove_manifest.c |
| `mds_cat_xattr_list` | 2 | src/cluster/migration.c, src/mds/compound_internal.h |
| `mds_cat_ext_dirent_get` | 1 | src/mds/compound_namespace.c |
| `mds_cat_gc_dequeue` | 1 | src/modules/ds_gc/ds_gc.c |
| `mds_cat_gc_enqueue` | 1 | tests/unit/test_ds_gc.c |
| `mds_cat_ns_parent_touch` | 1 | src/mds/main.c |
| `mds_cat_ns_parent_touch_supported` | 1 | src/mds/main.c |
| `mds_cat_ns_readdir_plus_from_cookie` | 1 | src/mds/compound_internal.h |
| `mds_cat_ns_remove_info_flags` | 1 | src/mds/remove_manifest.c |
| `mds_cat_ns_remove_info_verified_flags` | 1 | src/mds/remove_manifest.c |
| `mds_cat_ns_remove_known_gc` | 1 | src/mds/compound_internal.h |
| `mds_cat_prealloc_pool_insert` | 1 | src/modules/ds_prealloc/ds_prealloc.c |
| `mds_cat_prealloc_pool_scan` | 1 | src/modules/ds_prealloc/ds_prealloc.c |
| `mds_cat_quota_rule_get` | 1 | tests/unit/test_quota.c |
| `mds_cat_quota_usage_put` | 1 | src/mds/commit_queue.c |
| `mds_cat_remove_pending_bump_retry` | 1 | src/mds/remove_manifest.c |
| `mds_cat_remove_pending_peek_batch` | 1 | src/mds/remove_manifest.c |
| `mds_cat_shard_fileid_del` | 1 | src/cluster/rename_2pc.c |
| `mds_cat_xattr_get` | 1 | src/mds/compound_internal.h |
| `mds_cat_ds_provision_del` | 0 |  |
| `mds_cat_ext_dirent_del` | 0 |  |
| `mds_cat_link_anchor_del` | 0 |  |
| `mds_cat_quota_rule_put` | 0 |  |
| `mds_cat_sync` | 0 |  |

### 3.2 `mds_coord_*`

| 包装函数 | 调用点 | 主要位置 |
|---|---|---|
| `mds_coord_layout_grant` | 31 | src/mds/commit_queue.c, src/mds/compound_layout.c, tests/integration/bench_create_layout_fusion.c, tests/unit/test_catalogue.c |
| `mds_coord_layout_scan_for_file` | 20 | src/mds/compound_internal.h, src/mds/mover_util.c, tests/unit/test_catalogue.c, tests/unit/test_compound.c |
| `mds_coord_ds_layout_idx_scan` | 18 | src/mds/layout_recall.c, tests/unit/test_catalogue.c, tests/unit/test_compound.c, tests/unit/test_ds_layout_idx.c |
| `mds_coord_layout_return` | 10 | src/mds/commit_queue.c, src/mds/compound.c, src/mds/compound_layout.c, src/mds/layout_recall.c |
| `mds_coord_layout_get_by_stateid` | 8 | src/mds/compound_internal.h, src/mds/compound_layout.c, src/mds/layout_recall.c, tests/unit/test_catalogue.c |
| `mds_coord_recovery_put` | 6 | src/mds/commit_queue.c, src/mds/session.c, tests/integration/test_failover.c, tests/unit/test_catalogue.c |
| `mds_coord_journal_put` | 5 | src/cluster/hardlink_2pc.c, src/cluster/rename_2pc.c, tests/unit/test_rename_2pc.c |
| `mds_coord_layout_iter_file` | 5 | src/mds/layout_recall.c, tests/unit/test_catalogue.c |
| `mds_coord_open_put` | 5 | src/mds/open_state.c, tests/unit/test_coord_shared_state.c, tests/unit/test_open_state.c |
| `mds_coord_recovery_del` | 5 | src/mds/commit_queue.c, src/mds/session.c, tests/integration/test_failover.c, tests/unit/test_catalogue.c |
| `mds_coord_recovery_get` | 5 | src/mds/session.c, tests/integration/test_failover.c, tests/unit/test_catalogue.c |
| `mds_coord_deleg_del` | 4 | src/mds/delegation.c, tests/unit/test_coord_shared_state.c |
| `mds_coord_journal_get` | 4 | src/cluster/rename_2pc.c, tests/unit/test_rename_2pc.c |
| `mds_coord_deleg_put` | 3 | src/mds/delegation.c, tests/unit/test_coord_shared_state.c |
| `mds_coord_lock_put` | 3 | src/mds/lock_state.c, tests/unit/test_coord_shared_state.c |
| `mds_coord_lock_reap_client` | 3 | src/mds/lock_state.c, tests/unit/test_coord_shared_state.c |
| `mds_coord_open_del` | 3 | src/mds/open_state.c, tests/unit/test_coord_shared_state.c |
| `mds_coord_client_put` | 2 | tests/unit/test_coord_shared_state.c |
| `mds_coord_journal_del` | 2 | src/cluster/hardlink_2pc.c, src/cluster/rename_2pc.c |
| `mds_coord_journal_scan` | 2 | src/cluster/rename_2pc.c, tests/unit/test_rename_2pc.c |
| `mds_coord_lock_del` | 2 | src/mds/lock_state.c, tests/unit/test_coord_shared_state.c |
| `mds_coord_recovery_list` | 2 | src/cluster/failover.c, src/mds/session.c |
| `mds_coord_session_put` | 2 | tests/unit/test_coord_shared_state.c |
| `mds_coord_slot_put` | 2 | tests/unit/test_coord_shared_state.c |
| `mds_coord_client_del` | 1 | tests/unit/test_coord_shared_state.c |
| `mds_coord_client_get` | 1 | tests/unit/test_coord_shared_state.c |
| `mds_coord_deleg_get` | 1 | tests/unit/test_coord_shared_state.c |
| `mds_coord_layout_del_all_for_client` | 1 | src/mds/session.c |
| `mds_coord_layout_grant_union` | 1 | src/mds/compound_layout.c |
| `mds_coord_lock_scan_file` | 1 | tests/unit/test_coord_shared_state.c |
| `mds_coord_lock_test` | 1 | tests/unit/test_coord_shared_state.c |
| `mds_coord_open_get` | 1 | tests/unit/test_coord_shared_state.c |
| `mds_coord_open_scan_client` | 1 | tests/unit/test_coord_shared_state.c |
| `mds_coord_open_scan_file` | 1 | tests/unit/test_coord_shared_state.c |
| `mds_coord_session_del` | 1 | tests/unit/test_coord_shared_state.c |
| `mds_coord_session_get` | 1 | tests/unit/test_coord_shared_state.c |
| `mds_coord_slot_get` | 1 | tests/unit/test_coord_shared_state.c |
| `mds_coord_deleg_scan_client` | 0 |  |
| `mds_coord_deleg_scan_file` | 0 |  |
| `mds_coord_layout_scan_by_grant_owner` | 0 |  |
| `mds_coord_layout_transfer_grant_owner` | 0 |  |
| `mds_coord_lock_scan_owner` | 0 |  |
| `mds_coord_recovery_transfer` | 0 |  |
| `mds_coord_session_scan_client` | 0 |  |

### 3.3 调用分布（按目录，含测试）

- tests/unit: 575
- src/mds: 207
- src/cluster: 169
- tests/integration: 21
- src/modules/ds_prealloc: 20
- tests: 9
- src/fsal_obj: 7
- src/tools: 7
- include: 4
- src/modules/ds_gc: 4
- src/modules/observability: 1

**零调用点**（接口存在但当前无生产/测试调用者，FDB 后端可后置）：

`mds_cat_ds_provision_del`, `mds_cat_ext_dirent_del`, `mds_cat_link_anchor_del`, `mds_cat_quota_rule_put`, `mds_cat_sync`, `mds_coord_deleg_scan_client`, `mds_coord_deleg_scan_file`, `mds_coord_layout_scan_by_grant_owner`, `mds_coord_layout_transfer_grant_owner`, `mds_coord_lock_scan_owner`, `mds_coord_recovery_transfer`, `mds_coord_session_scan_client`

## 4. RonDB 表清单（FDB keyspace 设计的最小输入）

`src/catalogue/rondb_schema.h` 定义 **36 张 NDB 表**：

`mds_meta`, `mds_inodes`, `mds_dirents`, `mds_stripe_maps`, `mds_stripe_entries`, `mds_rename_journal`, `mds_ns_locks`, `mds_ns_lock_holders`, `mds_partition_map`, `mds_xattrs`, `mds_ds_registry`, `mds_ds_provision`, `mds_quota_rules`, `mds_quota_usage`, `mds_gc_queue`, `mds_remove_pending`, `mds_layout_state`, `mds_layout_by_client`, `mds_layout_by_file`, `mds_ds_layout_idx`, `mds_client_recovery`, `mds_node_registry`, `mds_delta_broadcast`, `mds_open_state`, `mds_open_by_file`, `mds_open_by_client`, `mds_byte_range_locks`, `mds_lock_by_owner`, `mds_delegations`, `mds_deleg_by_file`, `mds_deleg_by_client`, `mds_sessions`, `mds_session_by_client`, `mds_clients`, `mds_drc_slots`, `mds_prealloc_pool`

备注：`mds_ns_locks` / `mds_ns_lock_holders` 只服务 locked rename 路径；若 FDB 方案以事务冲突 + 读快照版本替代应用层锁，对应 keyspace 可不建，但 rename 的 TOCTOU 窗口（多次读→一次写）必须用 FDB 事务读快照语义重新论证（维度 3）。

## 5. 序列化与容量事实

- inode 固定编码 137 字节（`rondb_schema.c`），大端 wire 格式；`include/endian_helpers.h` 已有 `fdb_*` 命名前缀的字节序辅助函数（命名占位，非 FDB 依赖）。
- NDB 连接模型：并行 cluster connection 池（shim 默认 4 路）；跨节点原子 RMW 依赖 NDB interpretedUpdate。**FDB 侧必须对标的语义点**：原子计数 / mutate，或读-改-写 + 冲突重试（上限待实测）。
- 事务边界：一个复合元数据操作（如 create = dirent 插入 + inode 创建 + 父目录 nlink/mtime/change 更新 + 可选 GC 行）在 RonDB 侧是单个 NDB 事务；FDB 后端必须保持等价原子性（单事务多键写）。

## 6. 生成的待证假设（HYPOTHESIS，仅产生验证任务）

- **H-01** NDB interpretedUpdate 类原子 RMW（ns_setattr 掩码更新、nlink 调整、remove_pending_claim 的 CAS、lock_test）在 FDB 上需用原子 mutation 或冲突重试等价表达；CAS 类操作的冲突重试上界必须实测（维度 3 闸门）。**〔已验证，见 §7.1：普通 RMW 打热键触发冲突风暴，吞吐在 4 线程即封顶 ~370 ops/s、p99 达 122–175ms、重试 4.6k–10.6k；同一热键改原子 ADD 则 0 冲突 0 重试、16 线程近线性 6.5k ops/s、p99 ~3.5ms。热键 RMW 必须走 atomic_op。〕**
- **H-02** 队列类表（gc_queue / remove_pending / prealloc_pool）的队头 peek/claim 在 FDB 上是单键热点，需批量 claim 或分片前缀设计（维度 4）。**〔部分验证，见 §7.2：单键队头用原子 ADD 可 0 冲突近线性扩展到 16 线程；普通 RMW 队头则同 H-01 冲突封顶。批量/分片前缀仍待设计验证。〕**
- **H-03** `mds_drc_slots` / `mds_sessions` 位于 NFSv4.1 序列号热路径（每请求一次写事务），FDB 提交延迟直接进入 P50；需连接/事务池与提交策略专项基准（维度 4）。**〔已验证基线，见 §7.3：单键写事务提交 p50≈2.3ms、p99≈3.3ms（3 节点 lab 集群），即每次元数据写事务在 FDB 侧引入的提交延迟量级。〕**
- **H-04** 114 槽 vtable 的 fallback 语义（NULL → NOSUPPORT / 回退路径）使 FDB 后端可分阶段补齐：先核心 ns_* + layout + 协议状态，后 prealloc / ext / shard 分片功能。

## 7. 微基准实测（H-01 / H-02 / H-03）

环境：FDB 7.4.8，3 节点（10.131.9.44/.45/.46），triple 冗余 / ssd-2，每节点 4C x86-64-v3 / 8GB / 40GB（PVE 10.131.9.20 上 VMID 601/602/603），10G 内网。客户端与集群同节点（10.131.9.44）本地回环访问 coordinator，故延迟含真实集群 quorum 往返但不含跨机网络放大。基准程序 `tests/empirical/fdb_bench.c`，key 布局镜像 catalogue 责任域（P 父计数 / P..D dirent / I 137B inode / C 全局计数器）。

### 7.1 H-01：热键普通 RMW vs 原子 mutation（create 模式）

create = 每事务对两个热键做普通 read-modify-write（全局 fileid 计数器 + 父目录 nlink），再写 dirent + 137B inode。

| 模式 | 线程 | ops/s | mean | p50 | p95 | p99 | 冲突 | 重试 |
|---|---|---|---|---|---|---|---|---|
| create（普通 RMW×2 热键） | 4 | 374 | 10.7ms | 2.77ms | 56ms | 122ms | 0 | 4629 |
| create（普通 RMW×2 热键） | 8 | 369 | 21.2ms | 3.04ms | 99ms | 175ms | 0 | 10640 |
| counter（原子 ADD 单热键） | 4 | 1866 | 2.14ms | 2.06ms | 2.58ms | 3.15ms | 0 | 0 |
| counter（原子 ADD 单热键） | 8 | 3432 | 2.33ms | 2.27ms | 2.84ms | 3.62ms | 0 | 0 |
| counter（原子 ADD 单热键） | 16 | 6474 | 2.47ms | 2.36ms | 2.97ms | 3.52ms | 0 | 0 |

**结论（H-01 成立）**：
- 普通 RMW 打热键触发 FDB 冲突重试风暴——4→8 线程吞吐纹丝不动（374→369 ops/s），mean 翻倍（10.7→21.2ms），p99 升到 122→175ms，重试从 4.6k 涨到 10.6k。即普通 RMW 的热键并发存在硬吞吐上限，且尾延迟随并发恶化。
- 同一热键改 `fdb_transaction_atomic_op(ADD)`：0 冲突、0 重试，4→16 线程近线性（1866→3432→6474 ops/s），p99 恒定 ~3.5ms。
- 对照：原子 vs 普通 RMW 在同一热键上约 **17x 吞吐差**（6474 vs 374）、**p99 差 ~50x**（3.5ms vs 175ms）。
- **工程含义**：Lattice 里所有 NDB interpretedUpdate/CAS 语义（ns_setattr 掩码、nlink 增减、remove_pending_claim、lock_test、各类队列 claim）在 FDB 后端**必须**映射到 atomic mutation（ADD_VALUE / SET_VALUE_IF / BYTE_MAX 等）或显式 CAS 重试，不能写成普通 read-modify-write，否则热键并发即撞吞吐墙。

### 7.2 H-02：队头单键热点

counter 模式即队列队头模型（所有线程对同一 counter 键做 +1）：原子 ADD 下 0 冲突、近线性扩展到 16 线程（§7.1 末三行）。普通 RMW 队头则退化为 §7.1 的 create 冲突曲线。**部分验证**：原子化解决单键热点；生产队列（gc_queue/remove_pending/prealloc_pool）的批量 claim / 分片前缀设计仍需在 keyspace 评审阶段（启动清单第 18 项）落地后复测。

### 7.3 H-03：提交延迟基线（micro 单键写）

micro = 单线程对单键做 set+commit：

| 模式 | 线程 | ops/s | p50 | p95 | p99 | p999 | max |
|---|---|---|---|---|---|---|---|
| micro（单键 set+commit） | 1 | 424 | 2.27ms | 2.92ms | 3.33ms | 5.50ms | 8.40ms |

**结论（H-03 基线成立）**：FDB 侧单次元数据写事务的提交延迟 p50≈2.3ms、p99≈3.3ms（3 节点 lab 集群、客户端本地）。`mds_drc_slots`/`mds_sessions` 每 NFSv4.1 请求一次写事务，意味着每次序列号/会话写会在 P50 引入 ~2.3ms 的 FDB 提交延迟。是否满足 SLO 取决于（尚空的）SLO 登记；若 SLO 对该热路径 P50 敏感，需事务/连接池与提交策略专项优化。

### 7.4 读数注意

- unlink 模式（8T 12496 ops/s、p50 613us、conflicts=149818）的“conflicts”是基准工件——各线程对同一组循环文件名竞争，键已删时按冲突计数提前返回，**不是** FDB 冲突重试（该模式 retries 仅 250）。unlink 无计数器 RMW，故比 create 快一个量级，可作“只删不改计数”路径的延迟下界。
- micro/create/counter 的 key 形状不同，绝对 ops/s 不直接可比；可比的是**冲突/重试行为**与**提交延迟量级**。
- 本环境为 lab 基准拓扑，非生产；40GB 盘只够小数据集，triple 冗余下 Fault Tolerance=0（3 节点丢 1 节点仍可读写、丢 2 才丢数据）。生产规模外推需按启动清单第 11 项做等价性验收。

### 7.5 FDB 7.4 环境踩坑记录（复现/排障用）

1. **集群文件 2104 "Connection string invalid"**：7.3+ 语法是 `描述:ID@ip:port,ip:port,...`（`@` 分隔 key 与地址、地址间逗号）。老文档/脚本里"空格分隔多地址"写法在 7.4 直接抛 2104。解析逻辑见 `fdbclient/MonitorLeader.actor.cpp` 的 `ClusterConnectionString` 构造（key 必须含一个 `:`，描述只允许 a-z0-9_）。**包没坏**——官方 GitHub deb 与 PVE 镜像 deb md5 一致。
2. **fdbserver 只绑 127.0.0.1**：`foundationdb.conf` 的 `public-address = auto:$ID` 在本环境解析成回环，需显式 `public-address = <本机IP>:4500`。
3. **7.4 的 C API 与 7.x 不兼容**（非 bug，是版本重写）：`fdb_network_init`→`fdb_setup_network`（同步）、`fdb_database_create`/`open_default`→`fdb_create_database(path,&db)`（同步）、`fdb_future_get_error(f)` 变单参（≥23 去掉 description 出参）、重试 `fdb_transaction_get_retry_info`+`fdb_future_get_retry_delay`→`fdb_transaction_on_error(tr,err)`（返回 future，block 等待即自动 backoff）、原子加是 `FDB_MUTATION_TYPE_ADD`（=2，定义在 `fdb_c_options.g.h`）。libfdb_c.so 只导出 111 个 `fdb_*` 符号（不含上述老名）。写 C 客户端务必按 7.4 头文件，别照 7.x 示例。
4. **fdbmonitor 无 systemd unit**（7.4 deb 不带）：自建 `/etc/systemd/system/fdbmonitor.service`，`Type=simple`（7.4 的 fdbmonitor 是前台进程，不是 forking），enable 后重启自启已验证。

## 8. 复核命令

```bash
cd /work/jay/pnfs-lattice
grep -n 'mds_authority_ops rondb_authority_ops = {' src/catalogue/catalogue_rondb.c
grep -oE 'RONDB_TBL_[A-Z_]+ +"mds_[a-z_]+" ' src/catalogue/rondb_schema.h | sort -u
grep -oE '\bmds_(cat|coord)_[a-z_]+\(' include/mds_catalogue.h include/mds_coordination.h | sort -u
# §7 微基准复跑（在 FDB 集群客户端节点，需 gcc）
gcc -O2 -o fdb_bench tests/empirical/fdb_bench.c -lfdb_c -lm
./fdb_bench micro 1 8
./fdb_bench create 4 12
./fdb_bench counter 16 12
```

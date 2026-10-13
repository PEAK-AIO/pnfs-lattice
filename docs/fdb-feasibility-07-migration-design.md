# FDB 可行性分析 — 交付物 7：迁移策略纸面设计（§07 验证树，D6 前置）

> 计划文件：`foundationdb-feasibility-analysis-plan.html`（§07 迁移策略的验证树）
> 基线 commit：`88e679f`。输入：`fdb-feasibility-00`（36 表 + 114 槽）、`-05`（keyspace 映射）、`-04`（对账/幂等）、`-02`（不变量 I-1..I-9）。
> 证据等级：REFERENCE（纸面设计）。每条路线的通过标准留"执行前填写"（§06 规范），Phase 4 在真实目标环境彩排后回填。
> 顺序铁律（§07）：**先验证可逆性，再讨论上线速度**。

---

## 0. 迁移对象与规模锚点

- 数据面 = 36 张 NDB 表 → §05 keyspace 前缀（inode 285B wire 直接可搬，rondb_schema.h:501）。
- 规模未知（生产画像缺失，`-06` §2）：lab 残留量级 = 25 万 0 字节文件 + 50 万 prealloc 槽 + catalogue 2.3G。**所有时间预算公式化（GB/行 → 小时），不用绝对数**。
- 状态面 = NFSv4.1 会话/槽位/open/lock/deleg/layout：这些是**有状态协议数据**，迁移期间客户端可能持有活跃 stateid——这是 pNFS 迁移比纯文件系统迁移多出的核心难点。

## 1. 一致快照的定义（§07 必答问题 1）

RonDB 侧快照 = **NDB LCP（local checkpoint）+ GCI 水位**（实测：LCP 41/42，GCI 263385，skill rondb-ops）。一致性水位 = LCP 完成时的 GCI。
FDB 侧目标 = **单事务原子导入批次**（每批 ≤ FDB 事务上限，分批间用 NDB GCI 增量追平）。
**一致性判定**：导入完成后，对每个键空间前缀跑不变量 I-1..I-9（`-02` §B.1）+ 行数/和值对账（每表 row count + 关键列 checksum）。

## 2. 四条路线评估（§07 四棵树，按可逆性排序）

### 路线 A：停机迁移（推荐起步）
```
停 MDS(NFS 不可用) → NDB LCP 冻结 → 全量导出 36 表
→ 分批导入 FDB(每批单事务原子 + 批间校验) → 不变量校验
→ FDB 后端启动 → 冒烟(0007 17 项) → 恢复 NFS
回滚 = 重新用 RonDB 后端启动(数据未动)
```
- **可逆性**：**完全可逆**——RonDB 数据在切换成功前不被修改，回滚 = 换回后端配置重启。
- **唯一不可逆点**：切换后**第一次** FDB 侧写（任何 NFS 请求）。之前的任何时刻回滚零损失。
- **测量项**（Phase 4 回填）：N 行导出速率、M 批次导入速率、校验耗时、总停机窗口 = f(数据量)。
- **通过标准（执行前填写）**：停机 ≤ ___h（生产 SLO 定）；回滚演练 ≤ ___min；校验零差异。
- **局限**：NFS 客户端断连重连（NFSv4 state 丢失 → 客户端 recovery）；若停机窗口不可接受，升级路线 B。

### 路线 B：全量 + 增量追平（⚠️ 前提已被 M-2 证伪，见 §7）
- 增量源 = NDB **delta_broadcast** 表（`0x82`，source_mds+seqno 全局单调，rondb_schema.h:489-495）——**注：原以为"现成的增量流"，M-2 静态核实证伪：现状只 emit 跨 MDS rename 的 2 种 dirent delta（§7），非全量 changefeed**。
- 流程：全量导入（不停机，NDB 继续服务）→ 记 GCI 水位 W0 → 继续增量导入 delta(seqno > W0) → 追平（lag < ___ms 持续 ___s）→ **短停机切流**（秒级）→ 验证。
- **可逆性**：追平期间 FDB 只读影子，**切流前回滚 = 丢弃 FDB 数据**，零损失。切流后回滚需要反向同步（新写入回 NDB）——**这是路线 B 的不可逆点管理核心**。
- **增量完备性（已由 M-2 静态证伪，§7）**：现状 delta_broadcast 不覆盖 36 表全量变更（只 emit 跨 MDS rename 的 2 种 dirent delta，且单 MDS 不 emit）。路线 B 要成立需先做 changefeed 全覆盖工程。
- **通过标准（执行前填写）**：追平 lag ≤ ___ms；切流窗口 ≤ ___s；影子读差异 = 0。

### 路线 C：双写 + 影子读（评估结论：不推荐）
- 元数据双写 = 每个复合操作写 NDB + FDB 两份，跨系统原子性**无法保证**（NDB 提交 ≠ FDB 提交），差异补偿成本极高。
- 对 pNFS 的致命问题：双写把每请求延迟翻倍且引入**跨后端部分提交**——直接违反 I-1..I-9 的单事务原子性假设。
- **结论**：仅当路线 B 的增量完备性验证失败、且停机窗口不可接受时才考虑；当前证据下不进入 Phase 3/4 验证范围。

### 路线 D：按域/分区渐进（评估结论：延后评估）
- NDB 有 `mds_partition_map`（partition_id → owner_mds，subtree 粒度，rondb_schema.h:274-285）——**按 subtree 分域的后端路由在架构上存在**。
- 但跨边界 rename/link（跨域操作）在双后端共存期间无法原子（跨 NDB+FDB 事务不存在），必须禁止或强制路由——操作语义改变，风险面大。
- **结论**：只在"Hybrid 决策"（部分责任域用 FDB，`-03` §4）成立后才展开；全量替换路线下不需要。

## 3. 回滚与不可逆点（§07 必答问题 6）

| 路线 | 不可逆点 | 回滚方式 | 回滚数据损失 |
|---|---|---|---|
| A | 切流后第一次 FDB 写 | 换回 RonDB 配置重启 MDS | 不可逆点后的 FDB 写入（= 切换后新元数据，需导出回 NDB 或接受丢失——**切换后新写入反向导出必须作为回滚演练的一部分**） |
| B | 追平完成 + 切流后第一次 FDB 写 | 追平前 = 丢弃 FDB；切流后 = 反向同步 FDB 增量回 NDB | 同上 |

**强制要求**：反向同步（FDB→NDB 增量导出）必须实现并演练，否则路线 A/B 的"回滚"只是"放弃切换后的新写入"——对生产不可接受。

## 4. 迁移专用校验器（接入 `-02` §B）

逐表：row count 相等 + 每表关键列聚合校验和（fileid 全集、nlink 和、per-parent dirent 计数和、queue seq 连续无空洞）。跨表：I-1..I-9 全量。输出落 `results/fdb/<run>/migration/`（manifest 引用）。

## 5. Go/No-Go 观察项（§07 必答问题 9，供决策方填人）

- 切换期间：NFS 错误码分布（CLIENT 面）、MDS 队列深度、FDB 冲突率（DB 面）、不变量巡检（正确性面）。
- 阈值（执行前填写）：切换后 30min 内 unexpected NFS error = 0；不变量巡检连续 ___ 轮 PASS；FDB 冲突率 < ___/txn。
- Go/No-Go 权限：决策方指定（`-03` §6 决策点 4 同款）。

## 6. 待验证假设汇总（→ Phase 3/4 任务）

| # | 假设 | 验证 | 状态 |
|---|---|---|---|
| M-1 | NDB LCP+GCI 可作一致性水位导出 | Phase 3 导出工具实现 + 一致性断言 | 待验证 |
| M-2 | delta_broadcast 覆盖 36 表全部变更（路线 B 前提） | 静态核源码 | **已证伪（2026-10-13）**，见 §7 |
| M-3 | 分批导入单事务原子 + 批间 GCI 追平可行 | Phase 4 彩排（lab 2.3G 量级先行） | 待验证 |
| M-4 | 反向同步 FDB→NDB 增量导出可实现 | Phase 4 回滚演练 | 待验证 |
| M-5 | 切换后 NFS 客户端 recovery 行为可接受 | Phase 4 真实客户端切换彩排 | 待验证 |

## 7. M-2 证伪记录（2026-10-13 静态核实，代码证据）

**delta_broadcast 不是全量 changefeed，路线 B（全量+增量追平）的前提不成立（现状代码下）**：

1. **开关**：`changefeed_enabled = multi_mds && image_mode != OFF`（catalogue_rondb.c:291-292）——**单 MDS 部署根本不 emit**（lab 即单 MDS）。
2. **唯一 emit 点**：`rondb_locked_ns_rename_flags`（catalogue_rondb.c:3854-3881），且只 emit **2 种**：`CAT_DELTA_DIRENT_DELETE(src) + CAT_DELTA_DIRENT_PUT(dst)`——即只有**跨 MDS rename** 进 broadcast 表。
3. **apply 面**：`catalog_image_apply` 只处理 4 种（INODE_UPSERT/INODE_DELETE/DIRENT_PUT/DIRENT_DELETE，catalog_image.c），其余 enum 类型（stripe/xattr/ds/quota/gc/layout/协议状态，`catalog_delta.h`）有定义无 emit 路径。
4. **定位**：delta_broadcast 是 **Phase 9C 跨 MDS catalog_image 同步**机制（poller 只 apply 到本地 image，catalogue_rondb.c:3900），不是迁移增量流。

**对路线 B 的修正**：现状代码下路线 B 的"增量追平"无现成数据源，要成立需**新增全量 changefeed emit**（15+ 类型 × 全部 mutation 路径）——这是相当大的前置工程，且本身引入"authority 与 image 永久分叉"风险（rondb_emit_delta 注释：insert 失败必须把 mutation 报失败）。**评估修正**：路线 B 从"低成本备选"降级为"需先做 changefeed 全覆盖工程才可评估"；**推荐迁移路线收敛为 A（停机迁移）起步**，路线 B 仅在停机窗口不可接受 + changefeed 工程已立项时复活。

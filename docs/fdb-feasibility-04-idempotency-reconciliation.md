# FDB 可行性分析 — 交付物 4：未知提交 / 部分失败 / 毒任务的幂等·对账·补偿设计

> 计划文件：`foundationdb-feasibility-analysis-plan.html`
> 对应启动清单第 16 项：为未知提交结果、部分失败和后台毒任务设计幂等、对账与补偿测试。
> 基线 commit：`efdc724`。文中 file:line 可复核。证据等级：REFERENCE（源码取证 + 测试设计）。
> 前置：`fdb-feasibility-02` §A.4（G2 错误语义）、§B.1 I-8（队列 claim 幂等不变量）。

---

## 0. 设计目标（对齐计划硬闸门 G-GATE-4）

三类"提交结果不确定"场景必须做到**无重复副作用、无数据丢失、最终收敛**，且每类有可复现的注入测试：

1. **未知提交结果**（unknown-commit）：commit 请求发出后、结果未知（断网/进程退出/FDB `commit_unknown_result` 1021）。
2. **部分失败**（partial-failure）：一个复合元数据操作内，部分键已写、后续失败（跨记录不变量 I-1..I-4 半截状态）。
3. **后台毒任务**（poison task）：GC / remove_pending / prealloc 队列中的任务反复失败、不可推进。

铁律（不得违反）：禁止无限重试；禁止静默丢数据；禁止错误码漂移；**重复副作用必须可对账判定**（计划 §06 异常矩阵"未知提交结果"行 + §08 硬闸门）。

---

## 1. 现有代码事实（grounded）——三类机制已存在，FDB 后端必须等价复现

### 1.1 跨 MDS 原子操作的 in-doubt 对账：rename_2pc / hardlink_2pc journal

- 跨目录 rename、跨 MDS hardlink 走 **2PC**，journal 状态机 `R2PC_PREPARED(1) → R2PC_COMMITTED(2)`（`src/cluster/rename_2pc.c:34`）。
- Phase 1 写 `PREPARED` journal（含 inode 快照，供恢复重建，rename_2pc.c:549）；Phase 2 VOTE_COMMIT 后删源 dirent + 写 `COMMITTED`（rename_2pc.c:217）。
- **协调者崩溃在"已发 Commit 但 journal 仍是 PREPARED"的窗口**：代码显式处理——"keep the coordinator row so recovery can retry commit"（rename_2pc.c:452）。
- **启动对账** `rename_2pc_recover_*`（rename_2pc.c:791）：扫全库 journal，逐条判定 `resolved` / `unresolved`（rename_2pc.c:897-941）；`PREPARED` 的 in-doubt 事务按规则推进 commit 或 abort。
- **对账语义（FDB 必须保持）**：以 journal 为唯一权威记录"是否已提交"；恢复时**重读当前数据状态**判定该 in-doubt 事务的实际效果，再决定补齐还是回滚——**不盲目重放**（盲目重放 = 重复副作用）。

### 1.2 后台队列的幂等 claim：remove_pending（ownership-lease）

- `mds_cat_remove_pending_claim` 用 **ownership-lease（TTL 租约）+ CAS** 认领行（`src/mds/remove_manifest.c:978`，`claim_ttl_ns` 见 :127）。
- claim 只在**跨 MDS 崩溃**时提供 re-claim 保证；正常路径是本地内存 manifest 驱动（remove_manifest.c:19）。
- **租约过期 → 行被重新排队**（remove_manifest.c:440-456）；PEER 认领本节点行时，原节点的内存 tombstone 被对账清理（remove_manifest.c:919-928，"NOTFOUND drops the note; a successful claim means the row is overdue"）。
- **幂等点**：claim 的 CAS 保证**同一行不会被两个 worker 同时持有**；租约 TTL 保证崩溃 worker 的行**最终可被他人回收**（不永久锁死）——这正是 I-8 不变量的实现基础。

### 1.3 有界重试 / 毒任务

- 队列行带 `retries` 列（`rondb_schema` RONDB_RP_COL_RETRIES，shim:4318），`mds_cat_remove_pending_bump_retry` 递增（remove_manifest.c:443）。
- 事务级两层有界重试见 `fdb-feasibility-02` §A.2（内层 12 / 外层 8）。

---

## 2. FDB 后端的幂等·对账·补偿设计（待实现规范）

### 2.1 未知提交结果（unknown-commit）

**根因约束**：FDB `commit_unknown_result`（1021）与 RonDB 的"提交后断连"等价。两者都**禁止直接重试**（重试 = 可能重复执行 create/remove/rename 的外部副作用）。

**对账规范**（每个复合操作必须可执行）：
1. 提交结果未知时，**先读回该事务写入的确定性标记键**判定实际状态：
   - 用**幂等键 / txn 序号**（对应现有 `txn_id`，rename_2pc.c:267）作为事务身份。
   - 读回 journal 状态（PREPARED/COMMITTED）或数据键本身（如 dirent 是否已插入、nlink 是否已增）。
2. 依据读回结果三分：
   - **已提交** → 走成功路径（补齐后续副作用，不重写）。
   - **确定未提交** → 走重试路径（此时重试无副作用风险）。
   - **仍不确定**（读回也失败/超时）→ **不得**盲目重试，转 in-doubt 记录 + 后台对账（复用 2PC journal 机制），前台返回可重试态（NFS4ERR_DELAY，见 02 §A.4 G2 修复）。
3. **外部副作用幂等化**：DS 文件创建、GC 入队等不可回滚副作用，必须以"幂等键存在性"作为"是否已做"的判据，而非"本次请求是否执行过"。

**为何不能只靠 FDB 客户端**：FDB 的 `commit_unknown_result` 把对账责任抛回应用——客户端不替你做副作用对账。现有 2PC journal 正是这层对账，FDB 后端必须保留等价的 journal + 启动扫描。

### 2.2 部分失败（partial-failure）

**根因约束**：一个 create/rename 在 FDB 侧是**单事务多键写**（inventory §5 原子性要求），事务本身要么全成要么全不成——所以**数据平面的部分失败在 FDB 事务边界内天然不存在**（这是相对"多次独立 NDB 读→一次写"的优势）。

真正的部分失败发生在**事务之外的副作用序列**：
- 例：rename 的 NDB 事务成功，但后续的跨 MDS 2PC / DS 通知失败。
- **规范**：
  1. 数据原子性由**单 FDB 事务**保证（I-1..I-4 不因部分键写而破坏）——这是 FDB 后端相对 RonDB 的简化点。
  2. 跨事务的副作用序列用**现有 2PC journal + in-doubt 对账**（§1.1）兜底，崩溃窗口内的半截状态由启动对账收敛。
  3. 跨目录 rename 的 TOCTOU 窗口（多次读→一次写）必须用 **FDB 事务读快照版本**关闭（inventory §4 备注），不得用应用层锁替代后留窗口。
  4. 每次部分失败注入测试断言：**I-1..I-9 全程成立**（02 §B.1），且 in-doubt 集合最终为空（所有 `unresolved` 收敛为 `resolved`）。

### 2.3 后台毒任务（poison task）

**根因约束**：GC / remove_pending / prealloc 的 claim 行若反复失败（数据损坏、外部 DS 不可达、schema 不兼容），不得无限重试（硬闸门），也不得永久锁死（I-8）。

**规范**：
1. **有界重试**：复用 `retries` 列 + `bump_retry`（§1.3），设 `MAX_POISON_RETRY`（草案值：随 SLO 定，见 §4 决策点）。超限不再自动重试。
2. **隔离（dead-letter）**：超限行移入隔离状态（新键前缀 / 标记列），停止参与正常 claim，**可被单独检视与人工处置**；隔离行不阻塞队列头部（避免队头毒化拖垮整条队列）。
3. **可回收**：租约 TTL 机制（§1.2）保证任何 claim 最终过期可被他人回收——即使隔离前的崩溃也不留永久锁。
4. **不重复外部删除**：毒任务隔离时，若其外部副作用（如 DS 删除）状态未知，必须先对账（§2.1）再决定，防止"隔离后又重试导致重复删除"。
5. **可观测**：隔离行计数、最长滞留时间进 `/metrics`（对齐 02 §A.2 的指标化做法，`cat_transient_retries` 同款）。

---

## 3. 注入测试设计（Phase 3 交付，本文件定 spec）

每类 3 个故障点 × 复现脚本，全部**只读断言 I-1..I-9 + in-doubt 收敛**，落 `results/fdb/<run_id>/invariants/`（02 §B.3）。

### 3.1 未知提交结果
| 注入 | 位置 | 期望 |
|---|---|---|
| commit 后断网 | 提交发出、结果未回 | 前台返回 NFS4ERR_DELAY；后台对账后无重复副作用（fileid 不重复分配、nlink 不双减） |
| commit 后 kill MDS | 同上 | 重启后 journal 扫描把 in-doubt 收敛，数据与"提交成功"或"未提交"之一完全一致 |
| FDB commit_unknown_result(1021) | 注入错误码 | 走对账三分（§2.1），绝不盲目重试 |

### 3.2 部分失败
| 注入 | 位置 | 期望 |
|---|---|---|
| rename 跨 MDS 2PC 中间崩溃 | Phase1 后 Phase2 前 | 启动对账推进到 commit 或 abort；I-1/I-3/I-4 全程成立，无半截 dirent |
| 事务外副作用序列中断 | DS 通知失败 | journal 保留 in-doubt，收敛后无重复 DS 操作 |
| 读版本过旧中止 | 长事务读快照过期 | 明确拒绝/分批（02 §A.4），不写半成品 |

### 3.3 毒任务
| 注入 | 位置 | 期望 |
|---|---|---|
| GC 行反复损坏 | remove_pending claim | 达 `MAX_POISON_RETRY` 进 dead-letter；队列头不被拖死；其余行正常推进 |
| 毒任务崩溃 worker 持有租约 | claim 后 kill | 租约 TTL 过期后行被回收，无永久锁 |
| 毒任务外部删除状态未知 | 隔离前 DS 删除结果未知 | 隔离前对账，无重复外部删除 |

---

## 4. 决策点（待 Phase 3 / 决策方确认）

1. `MAX_POISON_RETRY` 阈值：随 SLO 与队列规模定（草案起步值需在生产工作负载画像后校准，见清单第 13 项待补）。
2. dead-letter 键空间前缀 / 标记列命名（随 Phase 2 keyspace 设计定）。
3. in-doubt 对账的扫描周期：复用现有"启动扫描 + 周期扫描"还是纯启动扫描？（现有 2PC 是启动扫描 rename_2pc.c:791；若运行期也可能产生 in-doubt，需加周期扫描——**这是相对 RonDB 的潜在新增运维项**，纳入 03 §2 运维维度评分）。

---

## 5. 复核命令

```
grep -n 'R2PC_PREPARED\|R2PC_COMMITTED' src/cluster/rename_2pc.c        # 2PC 状态机
sed -n '791,945p' src/cluster/rename_2pc.c                              # 启动对账
grep -n 'remove_pending_claim\|claim_ttl\|bump_retry' src/mds/remove_manifest.c
grep -n 'commit_unknown_result\|1021' <FDB 头文件/源码>                 # Phase 3 实现时
```

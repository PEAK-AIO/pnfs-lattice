# FDB 可行性分析 — 交付物 2：错误/重试规范 + 不变量与结果归档

> 计划文件：`foundationdb-feasibility-analysis-plan.html`
> 对应启动清单第 15 项（数据库错误 → mds_status → NFSv4 错误码映射 + 有界重试规范）与第 14 项（跨后端不变量校验器 + 结果归档目录，spec 级）。
> 基线 commit：`ad7992d`。文中所有 file:line 均可复核（`grep -n`）。
> 证据等级：REFERENCE（源码取证 + 规范设计）。本文件是**规范要求**，不是对 FDB 后端的验证结论。

---

## A. 错误分类与映射（清单第 15 项）

### A.1 现有代码事实（grounded）

**内部状态枚举** `enum mds_status`（`include/pnfs_mds.h:161`，20 个值）：

| 值 | 符号 | 值 | 值 | 符号 |
|---|---|---|---|---|
| 0 | MDS_OK | -2 | MDS_ERR_IO | -7 | MDS_ERR_STALE |
| -1 | MDS_ERR_NOMEM | -3 | MDS_ERR_NOTFOUND | -8 | MDS_ERR_GRACE |
|  |  | -4 | MDS_ERR_EXISTS | -9 | MDS_ERR_REPL |
| -5 | MDS_ERR_INVAL | -10 | MDS_ERR_MOVED | -11 | MDS_ERR_DELAY |
| -6 | MDS_ERR_PERM | -12 | MDS_ERR_NOSTANDBY | -13 | MDS_ERR_XDEV |
|  |  | -14 | MDS_ERR_NOTEMPTY | -15 | MDS_ERR_ISDIR |
| -16 | MDS_ERR_NOTDIR | -17 | MDS_ERR_NOSPC | -18 | MDS_ERR_LAYOUTUNAVAIL |
| -19 | MDS_ERR_NOSUPPORT |  |  |  |

**mDS → NFSv4 映射函数** `mds_status_to_nfs4()`（`src/mds/compound.c:950`）。这是**唯一**的生产映射点，被 `compound.c`、`compound_data_io.c` 等所有 compound 操作共用。当前实现（逐槽）：

| mds_status | 当前 NFSv4 | 可重试?(RFC 语义) |
|---|---|---|
| MDS_OK | NFS4_OK | — |
| MDS_ERR_NOMEM | NFS4ERR_RESOURCE | 否(资源) |
| MDS_ERR_IO | NFS4ERR_IO | 否(终态) |
| MDS_ERR_NOTFOUND | NFS4ERR_NOENT | 否 |
| MDS_ERR_EXISTS | NFS4ERR_EXIST | 否 |
| MDS_ERR_INVAL | NFS4ERR_INVAL | 否 |
| MDS_ERR_PERM | NFS4ERR_ACCES | 否 |
| MDS_ERR_STALE | NFS4ERR_STALE | 否(句柄作废) |
| MDS_ERR_GRACE | NFS4ERR_GRACE | 是(等 grace) |
| MDS_ERR_MOVED | NFS4ERR_MOVED | 否(referral) |
| MDS_ERR_DELAY | NFS4ERR_DELAY | 是(稍后重试) |
| MDS_ERR_NOTEMPTY | NFS4ERR_NOTEMPTY | 否 |
| MDS_ERR_ISDIR | NFS4ERR_ISDIR | 否 |
| MDS_ERR_NOTDIR | NFS4ERR_NOTDIR | 否 |
| MDS_ERR_NOSPC | NFS4ERR_NOSPC | 否 |
| MDS_ERR_XDEV | NFS4ERR_XDEV | 否 |
| MDS_ERR_LAYOUTUNAVAIL | NFS4ERR_LAYOUTUNAVAILABLE | 是(重取 layout) |
| MDS_ERR_REPL | NFS4ERR_SERVERFAULT | 是(恢复) |
| MDS_ERR_NOSTANDBY | NFS4ERR_SERVERFAULT | 是(恢复) |
| **default（含 MDS_ERR_NOSUPPORT）** | **NFS4ERR_SERVERFAULT** | **是(恢复) ← 见 A.3 G1** |

**errno → mds_status** `mds_errno_to_status()`（`src/common/error.c:41`）：非白名单 errno 一律 `MDS_ERR_IO`（兜底）。

### A.2 现有有界重试架构（两层，grounded）

RonDB 后端已有完整的双层有界重试，FDB 后端**必须等价复现**（这是"禁止无限重试"硬闸门的现有参照实现）：

1. **内层（shim，NDB 事务级）** `src/catalogue/catalogue_rondb_shim.cpp:70`
   - `NDB_RETRY_MAX = 12` 次
   - **仅**对 NDB 瞬时错误 `266`(lock timeout) / `274`(contention) 重试；其它错误直接返回
   - 退避：`NDB_RETRY_DELAY_US(2000us) × (attempt+1) + 随机 jitter(0..2000us)`（shim:8517，jitter 防死锁事务同步重试再碰撞，shim:75）
   - 耗尽 → 返回 `-2`（shim:8521）

2. **外层（C wrapper，操作级）** `src/catalogue/catalogue_rondb.c:67`
   - `RONDB_TRANSIENT_RETRIES = 8` 次
   - **仅**当内层返回 `-2` 时重试
   - 退避：`500us << min(attempt,5)` 即 500us→16ms 指数（`rondb_transient_backoff`，catalogue_rondb.c:69）；每次 backoff 计入 `cat_transient_retries` / `cat_transient_backoff_us` 指标（catalogue_rondb.c:81）
   - 耗尽 → `MDS_ERR_IO`（catalogue_rondb.c:670 等）

**最坏嵌套**：单次 ns_create 在热锁竞争下最多 `8(外) × 12(内) = 96` 次 NDB 提交尝试后放弃 → `MDS_ERR_IO`。这是当前的"有界"上限。

### A.3 发现的两个真实缺陷（FDB 后端设计前必须消解）

**G1 — MDS_ERR_NOSUPPORT 误映射为 SERVERFAULT**
- `mds_status_to_nfs4()`（compound.c:950）**没有** `case MDS_ERR_NOSUPPORT`，落入 `default` → `NFS4ERR_SERVERFAULT`。
- 但 vtable 对 NULL 槽（未启用能力，如 `shard_fileid_*`、`lock_test`、`lock_scan_owner`）由 dispatch 返回 `MDS_ERR_NOSUPPORT`（见 `fdb-feasibility-00-interface-inventory.md` §2）。
- 后果：一个**能力未实现**（确定性、可预期的）被暴露为 SERVERFAULT。RFC 7530 对 SERVERFAULT 的客户端行为是触发 recovery / 重连 / 重试，即"服务器坏了"，而真实语义是"这个操作后端不支持"。这正是计划硬闸门点名的**错误码漂移**风险：能力缺口被伪装成服务器故障，掩盖真实问题并诱导客户端做无意义的 recovery。
- **规范**：FDB 后端的 `MDS_ERR_NOSUPPORT` 必须显式映射，不得落到 default。由于 NFSv4.1 无通用 "operation not supported" 状态码，规范要求按操作上下文选择：对**协议操作**（LAYOUTGET 等）映射 `NFS4ERR_NOTSUPP`；对**非协议能力**（管理/工具面）应阻断在 mds 层不外抛 NFS。同时 `mds_status_to_nfs4` 的 `default` 分支应在编译期或启动自检中告警，避免未来新增状态再次静默落到 SERVERFAULT。

**G2 — 重试耗尽 → IO 终态，而非可重试的 DELAY**
- shim 耗尽注释写 "signal MDS_ERR_BUSY to caller"（shim:8521），但**枚举里没有 `MDS_ERR_BUSY`**（`include/pnfs_mds.h:161` 全表核对）。实际返回 `-2` = `MDS_ERR_IO` → `NFS4ERR_IO`。
- `NFS4ERR_IO` 对客户端是**终态**：Linux 客户端将其视为不可恢复的 I/O 错误，可能触发文件句柄作废 / 客户端 recovery，**不会**像 `NFS4ERR_DELAY` 那样透明重试。
- 后果：一个**瞬时**的锁竞争耗尽（本应"稍后重试"）被客户端当作**永久** I/O 故障。在 FDB 后端这会更严重——FDB 的提交延迟 p50≈2.3ms（H-03，比 RonDB 裸事务高 ~17x），事务边界超限 / 冲突 / 版本过旧 / 集群降级的瞬时概率更高，耗尽映射到 IO 终态会放大客户端侧故障面。
- **规范**：引入可重试瞬时态（如 `MDS_ERR_BUSY` / 复用 `MDS_ERR_DELAY`）用于"瞬时错误重试耗尽"，映射到 `NFS4ERR_DELAY`，让客户端透明重试；`MDS_ERR_IO` 只保留给**真正**的介质/后端损坏。shim:8521 的注释与行为必须对齐（改枚举或改注释，二者必居其一）。

### A.4 FDB 错误 → mds_status 映射规范（待实现，供 Phase 3 校验）

FoundationDB 7.4 C API 错误（`fdb_transaction_on_error` 分类）按下表归一。**这是规范，Phase 3 需为每行建注入测试**（对应异常处理专项矩阵，计划 §06）：

| FDB 错误类别 | 代表错误码/含义 | → mds_status | 是否重试(有界) | → NFSv4 |
|---|---|---|---|---|
| 事务冲突/可重试 | 1020 `transaction_conflict`、1021 `commit_unknown_result`、1031/1032 版本过旧、1023 请求过旧 | 瞬时可重试(经 G2 修复的态) | 是，内层≤12 指数+jitter，外层≤8 | NFS4ERR_DELAY |
| 客户端可重试 | 1033 `process_behind`、2002/2003 网络/代理不可达 | 瞬时可重试 | 是，同上但**更快熔断**(见 A.5) | NFS4ERR_DELAY |
| 提交未知结果 | `commit_unknown_result`(1021) | 瞬时可重试 + **幂等对账** | 是，但必须先判"是否已提交"再决定 | NFS4ERR_DELAY（对账后）|
| 事务边界超限 | 事务过大/超时(1005/1008 类) | **不可重试**，需分批 | 否 | 按操作映射(非 IO 终态) |
| 资源耗尽 | 内存/连接池/队列满 | MDS_ERR_NOMEM / BUSY | 是(背压) | NFS4ERR_RESOURCE / DELAY |
| 数据/schema 异常 | 缺键、坏编码、未知版本 | MDS_ERR_IO(损坏) | 否，隔离+告警 | NFS4ERR_IO |
| 集群不可用/降级 | 1024 `server_request_alive`、集群 leader 不可达 | MDS_ERR_IO 或 NOSTANDBY | 否(熔断后快速失败) | NFS4ERR_IO / SERVERFAULT |
| 配置/启动异常 | cluster file 错误、版本不兼容 | 启动前 fail-closed，不对外 | — | 不对外服务 |

**硬性要求**（对齐计划硬闸门，§08）：
1. **禁止无限重试**：任何重试路径必须有界（内层/外层上限 + 总墙钟预算），耗尽走 A.4 的可重试终态，不得忙等。
2. **未知提交结果必须对账**：`commit_unknown_result` 不得直接重试（会重复副作用），必须先读回/幂等键确认提交结果（对应清单项 16 的幂等设计）。create/remove/rename 的外部副作用（DS 操作、GC 入队）必须幂等。
3. **错误码不漂移**：`mds_status_to_nfs4` 的 `default` 分支必须被收敛（A.3 G1），任何未显式枚举的状态不得静默变 SERVERFAULT。
4. **集群降级快速失败**：代理/节点不可达时不得让每个 NFS 请求各自排队重试到超时，需熔断 + 稳定错误映射（避免恢复后请求风暴）。

---

## B. 跨后端不变量 + 结果归档（清单第 14 项，spec 级）

### B.1 不变量清单（grounded，FDB 后端必须逐条保持）

来源：`fdb-feasibility-00-interface-inventory.md` §2/§5 + 源码事务边界注释。这些是**两个后端都必须同时满足**的原子不变量，是跨后端校验器的判定基准。

| # | 不变量 | 涉及的原子操作(单事务多键写) | 校验点 |
|---|---|---|---|
| I-1 | create 原子性 | ns_create = dirent 插入 + inode 创建 + 父 nlink/mtime/change 更新 (+ 可选 stripe/GC 行)，**单事务**（inventory §5；catalogue_rondb.c:646） | create 后父 nlink==Σchildren、inode 存在且 fileid 唯一、无半截 dirent |
| I-2 | remove 原子性 | ns_remove = dirent 删 + 父 nlink/mtime/change 减 + (delete_child: inode 删 + GC 入队)，单事务 | remove 后父 nlink 减 1、child 不可见、GC 行与 nlink 一致 |
| I-3 | rename 跨目录原子性 | ns_rename = 源 dirent 删 + 目标 dirent 插 + 源/目标父 nlink/mtime/change，单事务 | rename 后源目录 nlink-1、目标+1、child 只在一个目录、TOCTOU 窗口被事务快照版本关闭（inventory §4 备注） |
| I-4 | nlink 一致性 | ns_nlink_adjust / parent_touch 必须与 dirent 增删在同一事务 | Σ(dirent 指向 P) == inode[P].nlink，全库抽样 |
| I-5 | fileid 唯一 | alloc_fileid 原子计数（H-01：FDB 侧必须 atomic mutation，否则热键墙） | 全库 fileid 无重复；单调不回收 |
| I-6 | 布局引用完整 | stripe_map_* / layout_* 与 inode 的引用闭环 | 每个 layout 行的 fileid 存在；inode 若有 layout 则 stripe_map 可达 |
| I-7 | 协议状态一致 | session/slot/open/lock/deleg 表间引用 | stateid 可解析、owner 唯一、slot 不重复、无孤儿 open |
| I-8 | 队列 claim 幂等 | gc_queue/remove_pending/prealloc claim 的 CAS（H-02） | claim 不重复、崩溃可回收、无永久锁、无重复外部删除 |
| I-9 | 无孤儿 inode | inode 存在 ⇒ 至少一个 dirent 指向（或显式 GC 挂起） | inode 表 − dirent 引用 = GC 挂起集，差集为空 |

**跨后端语义等价要求**：RonDB 侧的原子性靠 NDB interpretedUpdate + 单事务；FDB 侧靠多键写 + 冲突快照。**不变量本身后端无关**——校验器只校验"不变量是否成立"，不关心实现。任何一侧破坏 I-1..I-9 即为回归。

### B.2 不变量校验器（Phase 3 交付，本文件只定 spec）

- **形态**：独立只读工具 `tests/validate_invariants`，接受 `--backend rondb|fdb --conf <path>`，对给定命名空间/全库执行 I-1..I-9 的只读扫描，输出每不变量的 PASS/FAIL + 违例样本（前 N 个）+ 计数。**只读**，不写 catalog。
- **注入面**（对应异常矩阵）：在 create/remove/rename 的每个外部副作用前后设故障点，kill 校验断言不变量不被半截状态破坏（未知提交结果、部分失败）。
- **为何现在不实现**：仓库中**尚无任何 FDB 后端代码**（inventory §1，全库检索无 `catalogue_fdb`），校验器对 FDB 侧无可测对象。计划自身将"实现校验器"排在 **Phase 3 预验证实现**（§05）。本阶段交付的是**基准 + 判定条件 + 注入点定义**，实现随 Phase 3 的 FDB 高风险操作族一起落地。
- **RonDB 侧可先行**：校验器对 RonDB 后端可在现有 lab（MDS 164）实现并作为"正确性参照"——FDB 侧实现后跑同一套不变量，两侧结果 diff 即为跨后端等价性证据（计划 §05 PHASE 3 "结果只用于完善真实实验"）。

### B.3 结果归档目录规范

现状：`tests/empirical/results/` 已按 `<run_id>/phase_*_{summary.json,log,pcap}` 组织（run_id 如 `0003`…`0007`、`baseline_lattice_main`）。FDB 可行性工作**沿用并扩展**该约定，新增：

- **目录**：`tests/empirical/results/fdb/`（FDB 侧实验）与既有 `baseline_lattice_main/`（RonDB 侧 A/B 对照基线）。每次 A/B 实验一个子目录 `<run_id>_<backend>_<variant>/`。
- **manifest**：每个结果目录放 `manifest.json`，最少字段：
  - `commit_sha`（被测代码）、`backend`(rondb/fdb)、`env`（硬件/版本/拓扑，引用等价性记录）、`slo_anchor`（对比用的 SLO/基线编号）、`evidence_class`(REAL/REFERENCE/HYPOTHESIS)、`raw_paths`(原始日志/pcap/trace 相对路径)、`operator`、`timestamp`。
- **归档纪律**（对齐计划 §06 "原始结果、日志、trace、配置、脚本与 commit SHA 一并归档"）：
  - 只存**原始**产物 + manifest；摘要/结论进 `docs/fdb-feasibility-*` 并回链 manifest 路径。
  - `evidence_class` 字段强制三档（REAL/REFERENCE/HYPOTHESIS），评分（§08）只允许吃 REAL。
- **不变量结果**：`validate_invariants` 的 FAIL 样本落 `results/fdb/<run_id>/invariants/`，manifest 引用。

---

## C. 后续行动（交接）

1. **G1/G2 是 Phase 2/3 前的硬前置**：FDB 后端 `mds_status_to_nfs4` 与重试耗尽路径必须按 A.3/A.4 实现，否则违反 §08 硬闸门（错误码漂移 / 未知提交结果无法对账）。
2. **不变量校验器**随 Phase 3 FDB 高风险操作族实现；RonDB 侧可先行作为参照。
3. **结果归档**：下一次 A/B 实验起即用 B.3 的 manifest 约定，不再用裸目录。
4. 复核命令：
   ```
   grep -n 'MDS_ERR_NOSUPPORT' src/mds/compound.c          # G1: 无 case
   grep -n 'MDS_ERR_BUSY' include/pnfs_mds.h               # G2: 不存在
   sed -n '8517,8522p' src/catalogue/catalogue_rondb_shim.cpp  # 内层耗尽→-2
   grep -n 'RONDB_TRANSIENT_RETRIES' src/catalogue/catalogue_rondb.c
   ```

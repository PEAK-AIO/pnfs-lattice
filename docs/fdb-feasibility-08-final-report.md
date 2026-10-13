# FDB 可行性分析 — 交付物 8：可行性分析报告（Phase 0–2 收官版，§09 结论页）

> 计划文件：`foundationdb-feasibility-analysis-plan.html`（§09 最终报告结论页模板）
> 基线 commit：`c8aa106`。本文件是 **Phase 0–2（冻结目标 + 现状取证 + 纸面映射）的收官报告**。
> **结论效力声明（计划边界，§01/§05/§08 强制）**：Phase 0–2 产出的全部证据为 **REFERENCE/HYPOTHESIS** 级，无任何 **REAL**（真实目标环境可复现实测）。按硬闸门 **G-GATE-6**（任一关键维度缺真实验证 → 结论只能是"证据不足"），本报告**不构成 Go/No-Go 可行性结论**。它是 Phase 3/4 的完整输入 + 决策方所需的全部纸面资产。

---

## 1. 核心决策问题（§01 原文）

在满足 pNFS/NFSv4 元数据正确性、目标性能、故障恢复与运维约束的前提下，是否值得将当前 RonDB / MySQL NDB 后端替换为 FoundationDB？若值得，应采用何种数据模型、接入方式和迁移策略？

## 2. 推荐决策与适用边界（当前证据下）

**当前可给出的决策：Defer（延后至 Phase 3/4 完成）** —— 不是"不可行"，而是**证据不足**（G-GATE-6）。Phase 0–2 已把 Defer 的成本降到最低：

1. 决策所需的全部纸面前置件已齐（§3 证据清单），Phase 3 可直接进入预验证实现。
2. 纸面分析已**排除/降级**两条路线：双写迁移（C，跨后端原子性不成立）、增量追平迁移（B，changefeed 覆盖 M-2 证伪）——**迁移推荐收敛为 A 停机迁移起步**。
3. 纸面分析已**锁定**三个结构性风险（不实现就无法消除，Phase 3 必须优先验证）：
   - **K-4 rename TOCTOU**（正确性红线，G-GATE-1）：建议路线 A 保底（保留 ns_locks），路线 B（去锁赌 FDB 快照）作独立优化项。
   - **热键冲突墙**（H-01 实测 17x 吞吐差）：所有原子 RMW 语义必须映射 FDB atomic_op，keyspace 已按此设计（`-05` §2.2/2.3）。
   - **每请求 2.3ms 提交延迟**（H-03）：SEQUENCE/drc_slots 热路径（wire 实测占 23.8% op）的 SLO 符合性**取决于尚空的 SLO 登记**——这是决策方必须最先补的输入。
4. 适用边界：若最终走 **Hybrid**（仅协调/恢复关键状态迁 FDB，命名空间权威留 RonDB），Phase 3 范围可大幅缩小（coordination 41 槽 vs authority 71 槽），Defer→Hybrid 的触发条件见 §5。

## 3. 最关键的三项证据（现有证据等级内最强）

| # | 证据 | 等级 | 位置 | 决策含义 |
|---|---|---|---|---|
| E-1 | 热键普通 RMW 370 ops/s 冲突墙 vs atomic ADD 6.5k ops/s 0 冲突（17x 吞吐差、p99 差 50x） | HYPOTHESIS（lab 微基准，非生产） | `-00` §7.1 | FDB 后端热路径**必须** atomic_op，否则扩展性结构性失败；keyspace 已按此设计 |
| E-2 | RonDB 裸事务 ~0.12ms vs FDB 单键写提交 p50 2.3ms（同机/同 lab 口径，17x） | HYPOTHESIS 级（lab A/B 口径，`-01` §1 vs `-00` §7.3） | `-01`/`-00` | 每请求写事务（SEQUENCE 23.8% 占比，`-06`）P50 影响量级已知；SLO 登记后立判 |
| E-3 | 现有错误路径两个真实缺陷：G1（NOSUPPORT→SERVERFAULT 漂移）、G2（重试耗尽→IO 终态而非 DELAY） | REFERENCE（代码确认） | `-02` §A.3 | FDB 后端实现前**必须**修复，否则直接触发 G-GATE-4（错误码漂移/未知提交无法对账） |

**关键缺口（阻塞评分，按权重排序）**：
- SLO/验收线未登记（§01 "必须提前量化"）——权重最高的 3 个维度（性能 15% + 正确性 20% + 一致性 18% 的通过线）无法定义。
- 生产工作负载画像缺失（`-06` §3）——所有 lab 数字只能标"预验证"。
- FDB 后端代码为零——114 槽 vtable 无一槽在 FDB 上跑过。

## 4. 残余风险与责任人（`-03` §4 决策点 4 指定裁决人后激活）

| 风险 | 等级 | 责任人 | 消减路径 |
|---|---|---|---|
| K-4 rename TOCTOU（G-GATE-1） | 极高 | 待指定（架构） | Phase 3 路线 A/B 交错 + 故障注入（`-02` §B.1 I-3） |
| K-3 SEQUENCE 热路径 SLO | 高 | 待指定（性能） | 先登记 SLO；Phase 3 提交策略专项（`-05` §2.6） |
| G1/G2 错误语义缺陷 | 高 | 待指定（开发） | FDB 后端 PR 的前置 fix（`-02` §A.3） |
| 迁移不可逆点管理（反向同步） | 高 | 待指定（SRE） | 路线 A 彩排 + 回滚演练（`-07` §3） |
| 生产等价性 | 高 | 待指定（SRE） | §01 等价性验收（`-06` §2 外推说明） |
| 权重未冻结（治理） | 中 | 决策委员会 | 签署 `-03`（`-03` §6） |
| 无 FDB 值守能力（技能差距） | 中 | 待指定（SRE） | Phase 4 runbook 演练（维度 7） |

## 5. 下一阶段与退出条件

**下一阶段 = Phase 3 预验证实现**（`-03` 签署 + SLO 登记后即可启动，无需等待决策方做 Go/No-Go）：
1. 前置 fix：G1/G2（`-02` §A.3）——小改动，可独立合入。
2. 最小垂直切片（维度 10 要求）：FDB 后端实现 **核心 ns_* 6 槽**（ns_create/ns_remove/ns_lookup/ns_getattr/ns_setattr/ns_readdir，覆盖 I-1..I-5 不变量）+ atomic_op 热路径 + keyspace 按 `-05` §1。
3. 不变量校验器 `validate_invariants`（`-02` §B.2，RonDB 侧先行作参照）。
4. 9 个注入测试（`-04` §3）+ K-1..K-9 逐条实测（`-05` §4）。
5. 结果按 `-02` §B.3 manifest 归档，证据等级标注。

**Defer 退出条件（复评触发，任一满足即回决策委员会）**：
- Phase 3 全部 REAL 数据齐 + Phase 4 真实环境 A/B 完成 → 正常进 §08 评分。
- 任一硬闸门在 Phase 3 即被触达（如 K-4 路线 A 也保不住 I-3）→ 提前 No-Go，省 Phase 4 投入。
- **Hybrid 触发**：若 Phase 3 显示 authority 71 槽的 FDB 化成本/风险显著高于 coordination 41 槽（如 K-4 无法在合理预算内关闭），建议转 Hybrid 决策（仅 coordination/恢复状态迁 FDB，利用其高可用 + 无锁恢复语义），命名空间权威留 RonDB。
- 反向触发（维持 Defer 升级 No-Go 的红旗）：FDB 集群降级时错误映射无法稳定（G-GATE-5 灾备项）在生产拓扑等价性验收下无法消减。

## 6. 交付物索引（本分析 Phase 0–2 全量产出）

| 文件 | 内容 | 计划对应 |
|---|---|---|
| `foundationdb-feasibility-analysis-plan.html` | 执行框架 + 进度登记区 + 启动清单（12 项：4 勾 / 2 部分 / 1 待签 / 5 待 Phase3+） | §05–09 |
| `docs/fdb-feasibility-00-interface-inventory.md` | 114 槽 vtable 清单 + 127 包装函数调用点 + 36 表 + H-01/02/03 微基准 | D1 现状基线（接口面） |
| `docs/fdb-feasibility-01-rondb-baseline.md` | RonDB 实验室性能基线（4 组 bench，多轮方差）+ 恢复事件 | D1 现状基线（性能面） |
| `docs/fdb-feasibility-02-error-retry-invariants.md` | 错误映射规范 + 两层重试架构 + G1/G2 缺陷 + I-1..I-9 不变量 + 校验器/归档 spec | D2/D6 前置 |
| `docs/fdb-feasibility-03-scoring-decision-proposal.md` | 权重/硬闸门/证据等级/争议裁决（待签） | §08 |
| `docs/fdb-feasibility-04-idempotency-reconciliation.md` | 未知提交/部分失败/毒任务设计 + 9 注入测试 | D6 前置 |
| `docs/fdb-feasibility-05-keyspace-mapping.md` | 36 表 keyspace 映射 + K-1..K-9 + 事务预算表 | D2 目标设计（草案） |
| `docs/fdb-feasibility-06-workload-profile.md` | wire-level 操作混合 + conformance 基线 + 压测配方 | D1 工作负载画像（lab 半） |
| `docs/fdb-feasibility-07-migration-design.md` | 四路线评估 + M-2 证伪 + 回滚设计 | D6 迁移 |
| `tests/empirical/fdb_bench.c` | FDB 7.4 C API 微基准（micro/create/unlink/counter） | D3 验证资产（部分） |

**D3 真实验证资产 / D4 性能容量报告 / D5 可靠性运维**：Phase 3/4 产物，当前为空（如实登记，不代填）。

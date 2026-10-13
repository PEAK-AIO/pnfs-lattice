# FDB 可行性分析 — 交付物 1：RonDB 现状基线（Phase 1）

> 计划文件：`foundationdb-feasibility-analysis-plan.html`（启动清单第 13 项：提取 RonDB 现状基线及工作负载画像）。
> 基线 commit：`feabecd`（feat/fdb-feasibility-phase0）。证据等级：REFERENCE（实验室实测，非生产拓扑；生产规模外推需等价性验收）。

## 0. 环境

- MDS 604（10.131.9.164，4C/8G/20G，PVE 10.131.9.20）：pnfs-mds（worker_threads=48，catalogue_backend=rondb）+ RonDB 26.02.4（mgmd NodeId 65，ndbd NodeId 1，NoOfReplicas=1，DataMemory=4G / TransactionMemory=2G，与 MDS 同机混部）
- DS 605（10.131.9.165）：NFS 导出 `/export/ds`，客户端挂载 `/pnfs`
- 测试库状态：约 21 万个遗留 bench 文件、499237 个 prealloc 槽（9/30 起多次基准累计）
- 测量日期：2026-10-13（UTC）。所有 bench 在 MDS 主机以 root 运行；bench 二进制为 commit `feabecd` 源码编译（CMake Release，本机 x86-64）。

## 1. 原始 NDB 事务成本（bench-rondb-create，单线程，直连 catalogue 层）

2000 文件/轮，共 4 轮（第 1 轮 + 复测 3 轮）：

| 轮次 | create ops/s | create 均值 | remove ops/s | remove 均值 |
|---|---|---|---|---|
| 1 | 7897.2 | 0.13ms | 2502.1 | 0.40ms |
| 2 | 8806.5 | 0.11ms | 2391.9 | 0.42ms |
| 3 | 8005.5 | 0.12ms | 2212.2 | 0.45ms |
| 4 | 7819.3 | 0.13ms | 2334.6 | 0.43ms |

**读法**：
- 单事务 ns_create（dirent + inode + 父目录 nlink/mtime/change + prealloc 回收）≈ **0.11–0.13ms / ~7800–8800 ops/s**（同机 NDB，1 replica）。
- ns_remove 是 create 的 ~3.4x 慢（含 GC 入队 + 目录项清理），~2200–2500 ops/s。
- 这是 **NDB 裸事务成本**，不含 NFS/XDR/RPC/compound 调度。FDB 侧 H-03 已测同口径单键写提交 p50≈2.3ms——**FDB 提交延迟约是 RonDB 同机裸事务的 17x 量级**，是否可接受取决于（尚待登记的）SLO；这是 Phase 1→Phase 4 之间最关键的对比锚点。

## 2. 融合 create+layout 路径（bench_create_layout_fusion，500 迭代 × 3 轮，--no-cq 直连 catalogue，与 lab 守护进程路径一致）

| 轮次 | fused ops/s | fused us/op | unfused ops/s | unfused us/op | ratio |
|---|---|---|---|---|---|
| 1 | 1151.3 | 868.6 | 1416.9 | 705.8 | 0.813x |
| 2 | 1394.7 | 717.0 | 1643.1 | 608.6 | 0.849x |
| 3 | 1116.5 | 895.7 | 1141.7 | 875.9 | 0.978x |

**读法**：fused 路径（SEQUENCE+PUTFH+OPEN+GETFH+LAYOUTGET 单 compound）在当前配置下**不优于**拆分路径（ratio 0.81–0.98，即 unfused 更快或持平）。layout 全部走 delayed 路径（ready=0 delayed=500，lab 配置 `hpc_serve_layouts=false` + skip_transient_ndb）。FDB 侧设计必须复现该量级：每次 LAYOUTGET 涉及 layout 状态 + stripe map 读，若每个子操作各自一次 FDB 事务，叠加 ~2.3ms 提交延迟会显著恶化。

## 3. 多目录 create/remove 扩展（bench_mk_rm_scale，4/8/16 线程 × shared/spread）

| 线程 | 模式 | create ops/s | remove_sync ops/s | remove_ack ops/s |
|---|---|---|---|---|
| 4 | shared | 27306.1 | 9088.1 | 14609.8 |
| 4 | spread | 32042.6 | 9777.8 | 15645.4 |
| 8 | shared | 30227.4 | 11053.2 | 13124.9 |
| 8 | spread | 37327.0 | 12009.6 | 18364.3 |
| 16 | shared | 23068.3 | 11744.2 | 21569.6 |
| 16 | spread | 37807.7 | 14430.1 | 19948.0 |

**读法**：
- spread（目录分散）create 近线性到 16 线程（~37.8k ops/s），shared（同一父目录热点）16 线程反而回落到 23k——**热点父目录是 RonDB 侧的扩展墙**，与 Phase 0 的 H-01（FDB 热键 RMW 冲突墙）形成对称的对比锚点：FDB 后端若把父目录 nlink/mtime 写成普通 RMW，将复现 H-01 的 370 ops/s 级封顶；必须走 atomic mutation（FDB 侧热键 ADD 已实测 6.5k ops/s 0 冲突）。
- remove_ack 比 remove_sync 快 ~1.5x（异步 GC 路径）。

## 4. NFS 端到端（本轮）

- 客户端挂载 `10.131.9.164:/` → `/pnfs` 成功，`ls /pnfs` 元数据读路径通过（metrics：getattr=10、readdir=1、access=1 无错误）。
- **写回环已验证**：8MB urandom `dd` 写入 + `sync` + 读回，两端 md5 一致（`311caf96...`），数据路径通。
- 残留：8MB 探针文件 `/pnfs/baseline_probe` 清理（rm）被审批门拦，待下次写批准时一并清理。
- **smoke 17 项端到端检查未执行**：脚本在客户端创建/删除测试目录（写操作），审批门超时被拦。待补跑 `scripts/pnfs-smoke.sh`（PNFS_MNT=/pnfs）。
- 已发现残留问题：catalogue DS 注册表中存在遗留的 `bench-ds`（export 空、port 2049）条目，每次 MDS 启动会连续 6 次健康检查失败后标 OFFLINE（cooldown 30s flap）。不影响真实 DS（10.131.9.165:/export/ds 正常服务），但污染日志与 DS 健康计数——**清理该残留条目应列入 Phase 1 后续任务**。

## 5. 恢复事件记录（2026-10-13 01:45–01:58 UTC）

- 现象：MDS 持续 `ERROR 4009 No data node(s) available`；ndbd 进程活着（内存 7.2G 符合配置）但集群不可用，20h 无成功事务。journal 已轮转，根因日志缺失。
- 处置：`systemctl restart rondb-ndbmtd`（用户批准）→ 从 LCP 41 恢复、LCP 42 落盘、01:53:22 "Started" → 但 MDS 旧连接（48 线程连接池）未自愈，`systemctl restart pnfs-mds` 后错误停止，读路径恢复。
- 教训（进 runbook 候选）：**ndbd 恢复后 MDS 不会自动重连，需要重启 MDS**；`No data node(s) available` 刷屏 + ndb_mgm SHOW 无异常时，先查 mgmtd 日志确认 ndbd 启动 phase 是否走完，再重启 MDS。

## 6. 对 Phase 2/3 的输入

1. **延迟预算锚点**：RonDB 同机裸事务 ~0.12ms vs FDB 提交 p50 ~2.3ms（17x）。任何把每 NFS 请求 1 次 NDB 事务的 FDB 实现，P50 将至少增加 ~2.2ms；`mds_drc_slots`/`mds_sessions` 热路径必须专项优化（批量/合并提交、连接池）。
2. **热点策略**：shared 父目录在 RonDB 上已出现 16 线程扩展退化；FDB 侧父目录计数器/nlink 必须 atomic mutation（H-01 结论），否则扩展墙会低一个数量级。
3. **remove 成本**：RonDB remove 是 create 的 3.4x；FDB keyspace 设计时 remove 的 GC 入队 + 目录项删除在同一事务内的读写集要单独估算。
4. **DS 注册表残留**：清理 `bench-ds` 条目（写操作，待批）。

## 7. 复核命令

```bash
# 在 MDS 主机 10.131.9.164（bench 运行期间需先 stop pnfs-mds，[api] 池被占满，见 references/rondb-ops.md）
sudo systemctl stop pnfs-mds
sudo /tmp/bench-rondb-create /etc/pnfs-mds/mds.conf 2000
sudo /tmp/bench_create_layout_fusion 500 --rondb /etc/pnfs-mds/rondb.conf --no-cq
sudo /tmp/bench_mk_rm_scale 1000 --rondb /etc/pnfs-mds/rondb.conf
sudo systemctl start pnfs-mds
# NFS 端到端（客户端 10.131.9.165）
sudo mount -t nfs4 -o timeo=50 10.131.9.164:/ /pnfs && ls /pnfs
```

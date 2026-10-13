# FDB 可行性分析 — 交付物 5：keyspace 与访问路径纸面映射（Phase 2）

> 计划文件：`foundationdb-feasibility-analysis-plan.html`
> 对应 PHASE 2（纸面映射）+ 启动清单第 18 项（先评审 keyspace 与事务设计，再批准候选实现）。
> 基线 commit：`4b731b3`。输入 = `fdb-feasibility-00`（114 槽 vtable + 36 表 + 序列化事实）+ `src/catalogue/rondb_schema.h`（36 表逐列，file:line 可复核）+ `fdb-feasibility-01`（性能锚点）+ `fdb-feasibility-02`（错误/重试/不变量）。
> 证据等级：REFERENCE（纸面设计）。**不是结论、不是实现**。每个设计决定都标注依据 + 待验证假设（HYPOTHESIS），Phase 3 必须逐条实测。

---

## 0. 设计目标与不变量约束（先立约束，再画 keyspace）

FDB 与 NDB 的**根本差异**决定了 keyspace 设计的所有难点：

| 维度 | NDB（现状） | FDB（候选） | keyspace 影响 |
|---|---|---|---|
| 二级索引 | DB 内建（`ix_dirents_parent_child` 等） | **无二级索引**，只有有序 KV + 前缀范围读 | 每个 NDB 索引必须**显式编码成独立键前缀**（双向写） |
| 原子 RMW | interpretedUpdate（行级） | `atomic_op`（ADD/SET_IF/BYTE_MAX…）或冲突重试 | 热计数器/热父目录**必须** atomic_op（H-01，`fdb-feasibility-01`） |
| 行锁 / 并发 | 行锁 + 事务 | **冲突范围**（读集/写集键区间重叠即冲突重试） | 宽读集 = 大冲突范围 = 重试风暴；键设计要**收窄写集** |
| 事务原子性 | 单 NDB 事务跨表 | 单 FDB 事务跨键（同样多键原子） | 数据平面原子性**等价可得**（02 §B.1 I-1..I-4） |
| 事务大小/时长 | 宽松 | 有上限（读写集字节、时长、读版本保留） | 大目录 READDIR/批量 GC 必须分批（02 §A.4） |

**关键洞察**：FDB 后端的复杂度 90% 不在"存数据"，而在**把 NDB 的二级索引 + 应用层锁翻译成"键前缀 + 冲突范围控制"**。下面逐表映射。

---

## 1. 顶层 keyspace 布局（前缀分区）

统一 2 字节主前缀 + 8 字节子键。前缀表（16 进制，便于十六进制调试与范围裁剪）：

```
0x00  meta            计数器 / schema（热，atomic_op）
0x10  inodes          inode 主表
0x20  dirents         dirent 主表（parent 有序）
0x21  dirents_child   dirent 二级索引（parent,child_fileid 有序，READDIR cookie）
0x30  stripe_maps     stripe 头
0x31  stripe_entries  stripe 明细（fileid,ordinal）
0x40  xattrs          (fileid, attr_name)
0x41  gc_queue        gc_seq 单调 FIFO（分片前缀，H-02）
0x42  remove_pending  remove_seq FIFO + TTL claim（分片前缀，H-02）
0x43  prealloc_pool   预分配槽（fileid 主键 + owner 分片）
0x50  layout_state    (fileid, stateid) 主
0x51  layout_by_client / 0x52 by_file / 0x53 ds_layout_idx  三个二级
0x60  open_state      (fileid, stateid) 主 / 0x61 by_file / 0x62 by_client
0x63  byte_range_locks / 0x64 lock_by_owner
0x65  delegations     / 0x66 by_file / 0x67 by_client
0x70  sessions        / 0x71 session_by_client / 0x72 drc_slots（热）
0x73  clients
0x80  node_registry   (mds_id)
0x81  partition_map   (partition_id)
0x82  delta_broadcast (source_mds, seqno)
0x90  rename_journal  (txn_id)  in-doubt 对账（04 §1.1）
0x91  ns_locks        应用层锁（8 类，见 §4 硬问题）
0xA0  ds_registry / 0xA1 ds_provision
0xB0  quota_rules / 0xB1 quota_usage
0xC0  client_recovery
```

**为何不用 1 字节**：8 字节 fileid 已是热路径主体，2 字节主前缀保留 255 个命名空间 + 便于未来按域渐进迁移（计划 §07 "按域/分区渐进"）。

---

## 2. 逐表键/值编码 + 访问路径成本

值编码**复用现有 inode wire 格式**——`rondb_schema.h:501` 注释明写该格式 "extends FDB format with home_shard_id"，即 137/145/285B 定长（v1/v8/v9）**本就是为 KV 后端设计的**。这大幅降低序列化改造成本（HYPOTHESIS：可直接作为 FDB value，Phase 3 需确认 285B 是否超 FDB 单值建议上限——FDB 单值软上限 ~100KB，285B 远低于，**低风险**）。

### 2.1 inode 主表（0x10）
- **key** = `0x10 ‖ fileid(8)`  → **value** = 285B wire（rondb_schema.h:508-514）
- **访问**：`ns_getattr/lookup` 命中 = 1 点读（O(1)）；create 写 = 1 键。
- **无二级索引需求**（fileid 即主键）。`home_shard_id` 已在 wire 内。
- **成本**：读 1 键 / 写 1 键。冲突范围 = 单键。**这是最干净的表。**

### 2.2 dirent 主表（0x20）+ 二级索引（0x21）★ 热点核心
- **主键** = `0x20 ‖ parent_fileid(8) ‖ entry_name(len)`（name 变长，`MDS_XATTR_NAME_MAX` 量级）
  - `ns_lookup` = 1 点读。
  - **create** = 写 `0x20`（新 dirent）+ 写 `0x21`（索引）+ 写 `0x10`（inode）+ **atomic_op 父目录 nlink/change/mtime**（见 2.3）+ 可选 stripe/gc。**单 FDB 事务 4-6 键写**。
  - **remove** = 对称删除 + atomic_op 父目录 -1 + gc_queue 入队（`0x41`）。
- **二级索引 0x21** = `0x21 ‖ parent_fileid(8) ‖ child_fileid(8)` → value 空或 name。**双向写**：create 同时写 0x20 + 0x21，remove 同删两键。
  - **为何必须**：READDIR cookie 续读依赖 `ix_dirents_parent_child` 的 `child_fileid > cookie` 有序范围读（rondb_schema.h:183-187）。FDB 无二级索引，这个有序性**必须**用 0x21 前缀承载，否则 READDIR 退化全量扫描。
- **冲突范围分析（H-01 直接适用）**：同父目录 N 个并发 create，写集都含父 inode 键 → 若父 nlink 用普通 RMW = 冲突风暴（实测 370 ops/s 墙，`fdb-feasibility-01`）。**必须**：父目录 nlink/change/mtime 走 **atomic_op（ADD/SET）**，把父键从"冲突写"降级为"原子计数写"，冲突范围收敛到 dirent 键本身（不同 name → 不同键 → 不冲突）。
  - **待验证（HYPOTHESIS）**：atomic_op 父目录后，同父并发 create 的吞吐上限 = ?（Phase 3 热点目录压测，对标 RonDB shared 23.1k ops/s @16T，`fdb-feasibility-01`）。

### 2.3 热计数器（0x00 meta）★ H-01/H-02 直接适用
- `fileid_counter`（RONDB_META_KEY_FILEID）、`gc_seq_counter`、`delta_seqno_counter` 三个单调计数（rondb_schema.h:133-136）。
- **编码** = `0x00 ‖ key_name` → value = u64 大端。**alloc_fileid = `atomic_op(ADD, 1)` 单键**（H-01 实测：原子 ADD 6.5k ops/s 0 冲突 16T 近线性）。
- **禁止**普通 read-modify-write（370 ops/s 墙）。
- **成本**：1 原子写/次，冲突 0。

### 2.4 队列族（0x41 gc / 0x42 remove_pending / 0x43 prealloc）★ H-02 部分验证
现状 NDB：
- gc_queue：`gc_seq` 单调 FIFO，`owner_mds_id` 分区 drain（各 MDS 只 drain 自己的行，rondb_schema.h:361-366），有序索引 on gc_seq（:376）。
- remove_pending：`(remove_seq, claim_mds_id, claim_boot, claim_expires_ns, retries)` TTL 租约 claim（:294-303，04 §1.2）。

**keyspace 设计（HYPOTHESIS，Phase 3 必测）**：
- **队头单键热点**是 H-02 已证风险：所有 worker 抢同一 `gc_seq` 队头 = 单键写热点。**必须分片前缀**：
  - 主键改 `0x41 ‖ owner_mds_id(4) ‖ gc_seq(8)`（现状 owner_mds 分区天然可作分片维度）→ 各 MDS 只扫自己前缀，**队头冲突隔离到 MDS 内**。
  - worker 并发 drain 同一 MDS 前缀时，用 **atomic_op 推进队头指针**（`0x00 ‖ gc_head_<mds>`）+ 按 seq 范围 claim，避免"peek 队头"单键争抢。
- remove_pending 的 **TTL claim** 映射：claim = 单事务写 `(claim_mds_id, claim_boot, claim_expires_ns)` + **CAS 前置**（`atomic_op SET_VALUE_IF` 旧值=未认领，防双 claim，04 §1.2 I-8）。`retries` 超限 → dead-letter 前缀（04 §2.3）。
- **待验证**：批量 claim（一次事务 claim K 行）vs 逐行 claim 的吞吐/冲突比（Phase 3 队列积压压测）。

### 2.5 stripe map（0x30/0x31）
- 头 `0x30 ‖ fileid` → (stripe_count, stripe_unit, mirror_count)；明细 `0x31 ‖ fileid(8) ‖ ordinal(2)` → (ds_id, nfs_fh)。
- 单 stripe 文件走 **inline**（wire v9 内嵌，MDS_IFLAG_INLINE_STRIPE，rondb_schema.h:170-172）→ **不建 0x30/0x31 键**，LAYOUTGET 直读 inode（rondb_schema.h:222-227 注释的优化）。
- 多 stripe：LAYOUTGET = 1 点头 + N 点读明细（N=stripe_count）。**冲突范围小**（fileid 维度），非热点。

### 2.6 NFSv4 有状态协议表（0x50-0x73）★ H-03 热路径
- **layout_state** 主键 `0x50 ‖ fileid(8) ‖ stateid_other(8)`（NDB PK，rondb_schema.h:406）；二级 `0x51`(by_client)、`0x52`(by_file)、`0x53` ds_layout_idx `(ds_id,clientid,fileid)` → 各需双向写。
- **open_state / byte_range_locks / delegations** 同构：主表 + by_file + by_client 双向索引。
- **sessions / drc_slots（0x72）= NFSv4.1 SEQUENCE 热路径**：每请求一次 slot 写。**H-03 已证**：单键写提交 p50≈2.3ms（`fdb-feasibility-01`/`00` §7.3）。
  - **设计**：slot 键 `0x72 ‖ session_id ‖ slot_id` → value (seqid, ...)。每请求 1 键写，提交延迟进入 P50。
  - **缓解（HYPOTHESIS，Phase 3 专项）**：(a) 连接/事务池复用（避免每请求建事务）；(b) 批合并（同 session 同槽的幂等重放不重复写）；(c) 若 SLO 对 P50 敏感，评估 slot 状态**内存化 + 异步落盘**（grace 恢复用 0x73 clients + journal 兜底）。**是否可接受取决于尚空的 SLO 登记**（`fdb-feasibility-00` §7.3）。

### 2.7 协调 / 恢复表（0x80-0x82, 0xC0, 0x90）
- **node_registry** `0x80 ‖ mds_id` → (boot_epoch, hostname, ports, state, heartbeat)。心跳 = 周期单键写，非热（5s 一次）。
- **delta_broadcast** `0x82 ‖ source_mds(4) ‖ seqno(8)` 全局单调（rondb_schema.h:491-495）→ 追加式，changefeed 候选（计划维度 6 "changefeed 增量传播"）。
- **rename_journal** `0x90 ‖ txn_id` → in-doubt 对账（04 §1.1，启动扫描 0x90 全前缀）。
- **client_recovery** `0xC0 ‖ clientid` → grace 恢复。

---

## 3. 应用层锁（0x91 ns_locks）— 本设计最硬的开放问题 ★★

现状：`mds_ns_locks` 承载 **8 个资源类**的应用层目录锁（rondb_schema.h:257-264）：
```
0 TOPOLOGY / 1 PARENT_NAME / 2 DIR_MUTATION / 3 INODE_ATTR
4 FILE_STATE / 5 LOCK_TABLE / 6 CLIENT_SESSION / 7 SLOT_DRC
```
获取顺序按 (class, partition_hint, key_bytes) 升序，TOPOLOGY 最优先（防目录 rename 竞争，:250-253）。这是**跨目录 RENAME 的 TOCTOU 防护**：原子提交前有多次独立 NDB 读，靠 TOPOLOGY 锁串行化（inventory §1，catalogue_rondb.c:4173；Phase 12 实测去锁后 16 路 create 吞吐回升 ~96%）。

**FDB 侧两条路线（HYPOTHESIS，Phase 2 评审核心）**：

- **路线 A：保留显式锁键（0x91）**
  - 编码 = `0x91 ‖ res_class(1) ‖ resource_key(len)` → (lock_mode, holder_count, owner_mds, owner_epoch, fence_epoch, granted_at, ttl)。
  - 优点：语义与现状 1:1，TOCTOU 防护直接继承，风险低。
  - 缺点：锁本身是**额外事务 + 键写**，热类（TOPOLOGY）成单键热点；fencing_epoch 的脑裂防护需在 FDB 重新论证。

- **路线 B：去掉应用层锁，用 FDB 事务冲突 + 读快照版本替代**
  - 依据：inventory §4 备注——"若 FDB 方案以事务冲突 + 读快照版本替代应用层锁，对应 keyspace 可不建"。FDB 事务读快照 + 冲突重试理论上能关闭 rename 的 TOCTOU（读到的版本在提交时若被并发修改 → 冲突重试）。
  - 优点：少一类键 + 少一次锁事务，rename 路径更短。
  - 缺点：**必须重新论证 TOCTOU 窗口**（多次读→一次写是否被 FDB 快照版本完全关闭）；跨目录 rename 的读集可能跨 TOPOLOGY 边界，冲突范围大。**这是维度 3（事务/一致性）的一级风险**，Phase 3 必须做高并发交错 + 故障注入测试（02 §B.1 I-3）。

**我的倾向（待决策方评审，不是结论）**：先 **路线 A 保底**（正确性可继承），把路线 B 作为 Phase 3 优化项单独验证——因为 Phase 12 已证应用层锁有 ~96% 的吞吐影响，去掉它收益大，但 rename TOCTOU 是正确性红线，**不能在没有故障注入证据前直接赌路线 B**。（对应 `fdb-feasibility-02` G-GATE-1 不变量 + G-GATE-4 错误语义）。

---

## 4. 能力差距与风险假设汇总（→ Phase 3 验证任务）

| # | 风险/差距 | 依据 | 验证任务（Phase 3） | 严重度 |
|---|---|---|---|---|
| K-1 | 热父目录 nlink 必须 atomic_op，否则 370 ops/s 墙 | H-01 实测 | 热点目录并发 create 压测，对标 23.1k@16T | 高 |
| K-2 | 队列队头单键热点需分片前缀 | H-02 部分 | 队列积压 + 分片 claim 压测 | 中 |
| K-3 | drc_slots/sessions 每请求 2.3ms 提交进 P50 | H-03 实测 | SEQUENCE 热路径专项 + SLO 对照 | 高（SLO 敏感）|
| K-4 | rename TOCTOU：应用层锁 vs FDB 快照替代 | inventory §4 | 路线 A/B 交错 + 故障注入 | **最高**（正确性红线）|
| K-5 | 二级索引双向写的写放大 | §0 FDB 无索引 | create 路径读写字节分解 vs NDB | 中 |
| K-6 | 大目录 READDIR / 批量 GC 事务超限 | FDB 事务上限 | 百万级目录分页 + GC 批量边界 | 中 |
| K-7 | inode 285B value 是否触发 FDB 值大小告警 | 远低于 100KB，理论低风险 | 确认 + 长稳 | 低 |
| K-8 | 冲突范围随并发扩大的尾延迟（p99） | H-01 p99 122-175ms | 尾延迟画像 @目标并发 | 高 |
| K-9 | 跨 MDS 2PC / in-doubt 对账在 FDB 的等价性 | 04 §1.1 | 崩溃注入 + journal 扫描收敛 | 高 |

---

## 5. 事务预算表（每个访问模式的读写集 + 冲突范围，供 Phase 3 填验收线）

| 操作 | FDB 事务键写集 | 读集 | 冲突范围 | 原子性要求（不变量） | 验收线（待 Phase 3 填） |
|---|---|---|---|---|---|
| ns_create | dirent(0x20)+idx(0x21)+inode(0x10)+父 atomic+可选 | 父 inode | 父键 atomic 化后≈单键 | I-1 | ops/s ≥ RonDB shared 同并发 |
| ns_remove | 删 dirent+idx+inode+父 atomic-1+gc(0x41) | dirent,inode | 单键 | I-2 | 同上 |
| ns_rename（跨目录） | 源/目标 dirent+idx+双父 atomic+journal | 多键（TOCTOU 读）| **大**（路线 A 靠锁收窄 / 路线 B 靠快照）| I-3/I-4 | 零不变量破坏 + p99 预算 |
| LAYOUTGET | 读（inode inline 或 0x30+0x31）| 1-N 键 | 无写 | I-6 | 提交延迟预算 |
| SEQUENCE（slot）| 1 键(0x72) | 1 键 | 单键 | — | P50 ≤ ?（SLO）|
| GC claim | 队头 atomic + K 行 claim | 前缀范围 | MDS 前缀内 | I-8 | claim 吞吐 + 无重复 |

---

## 6. 决策点（Phase 2 评审需拍板，启动清单第 18 项前置）

1. **K-4 rename 路线 A vs B**：保底 A（继承锁语义）还是激进 B（去锁赌快照）？影响整个 0x91 子空间是否建立 + 维度 3 测试范围。**我的建议：A 保底，B 作为独立优化项 Phase 3 验证**（理由见 §3）。
2. **队列分片维度**：按 owner_mds 分片（现状天然）还是按 seq 哈希分片（更均匀但破坏 FIFO 顺序语义）？倾向 owner_mds（保持现状 drain 语义）。
3. **drc_slots/sessions 是否内存化**：取决于 SLO（尚空）。**建议：先登记 SLO（清单第 13 项待补），再定**——不要在没有 SLO 锚点前做提交策略优化。
4. **keyspace 前缀表（§1）冻结**：评审通过后写回本文件 §1 为"已评审"，Phase 3 实现严格遵循。

---

## 7. 复核命令

```
grep -n "RONDB_TBL_\|COL_" src/catalogue/rondb_schema.h         # 36 表逐列
sed -n '257,271p' src/catalogue/rondb_schema.h                    # 8 类应用层锁
sed -n '405,443p' src/catalogue/rondb_schema.h                    # layout_state PK + 3 二级
sed -n '498,530p' src/catalogue/rondb_schema.h                    # inode wire "FDB format"
sed -n '349,380p' src/catalogue/rondb_schema.h                    # gc_queue FIFO + owner 分区
```

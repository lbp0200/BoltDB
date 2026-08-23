# BoltDB 待办列表

## 当前状态

| 指标 | 值 |
|------|----|
| RESP3 Null 覆盖 | 34/34 命令（100%） |
| redis-py compat | 153/153 (100%) |
| node-redis compat | 110/110 (100%) |
| redis-cli compat | 77/77 (100%) |
| timer 泄漏 | 8/8 已修复 |
| isWriteCommand | 94/94 完整 |
| goroutine leak test | 通过 |
| handler.go 拆分 | 8824→0 行，拆为 24 个文件，无单文件超 1136 行 |
| Cluster gossip payload | ✅ 已实现（SlotOwners + Nodes + PFail） |
| PFAIL gossip 传播 | ✅ 已实现（多节点投票晋升） |
| 槽位视图同步 | ✅ 已实现（epoch 裁决） |
| Redis 8 命令补齐 | 5/5 批次完成 |
| 全命令准确性测试 | 239/239 命令已覆盖 |
| Mutation Testing | 5,201 变异体，100% efficacy，Store mcover 90.17% ✅ |
| Lua 脚本（EVAL/SCRIPT）技术分析 | 已完成，确认不实现，见 [lua-scripting.md](lua-scripting.md) |
| 收购审查 A–D（第一阶段） | 19/19 项全部完成 |
| 收购审查 E–F（第二阶段） | 14/14 项全部完成（含 E3 方案B ✅） |
| 竞争对手算法缺陷修复 | 五轮审查 17/17 项全部完成 ✅（ZRANK/ZRANGE 基准数据见下方） |
| RANDOMKEY 蓄水池采样 | O(2n) → O(n) 单次遍历 |
| 256GB 数据测试（机械盘） | 2026-07-27 完成：256GB 净数据 / 262K keys × 1MB / 77.7 MB/s avg / 磁盘放大 1.3x / 0 数据损坏 |
| 生产就绪评估 | 2026-07-27 完成：见[生产就绪评估](#生产就绪评估)章节 |
| 预存测试失败修复 | 3/3 项 ✅（logger data race、mutation kill、复制回归超时）|
| Nightly Soak GHA | 已恢复 ✅（GOMEMLIMIT=5120MiB + 长 job 去 `-race`）|
| executeReplicatedCommand 重构 | 已完成 ✅（1972 行 switch → `store.WriteCommand`，`psync.go` 2157→191 行）|
| ZRANK O(n) 优化 | 已完成 ✅（in-memory rank cache，O(1) 平均查询）|
| Backlog resize 热更新 | 已完成 ✅（CONFIG SET `backlog-size`）|
| 58 个跳过测试审计 | 已完成 ✅（51 远程通过，4 个 nil pointer 已修，3 个 resource 保留）|
| v8.51.1 自动发版验证 | 2026-08-04 完成：CI 门禁修复后自动发版成功（metrics 版本注入修复 + guard_bench.sh `-run '^$'` + GHA flaky skip），2.16 集群滚动升级 v8.34.0 → v8.51.1 |
| server 包并行测试 env 污染 | 已完成 ✅（2026-08-21）：`TestAUTH_TimingAttackResistance` 用 `t.Parallel()`+`os.Setenv("BOLTDB_PASSWORD")`，命令入口 `handler_dispatch.go:59` 用 `os.Getenv` 做 NOAUTH 检查，并行窗口内其它命令测试随机返回 NOAUTH 错误（`-race` 检不出、失败数随 `-parallel` 递增）。修复：去掉该测试 `t.Parallel()`（Go 串行阶段独占运行，env 不泄漏）。`internal/server` 并行测试 5 连 0 失败，远程 `-race` 通过 |
| 复制 GETDEL/GETEX 缺失 key 回归 | 已完成 ✅（2026-08-21）：e575113 把 `write_command.go` 的 GETDEL/GETEX 缺失 key 处理由 `return nil` 误改为 `return gErr`，导致 `TestExecuteReplicatedCommand_GETDEL/GETEX_NonExistent` 失败。修复：新增 `isMissingKeyErr`（`errors.Is(err, ErrKeyNotFound)`），缺失 key 静默返回 nil（符合 Redis 语义），真实存储错误仍传播。远程 `-race` 全包通过 |
| GEOSEARCH BYBOX 矩形语义 | 已完成 ✅（2026-08-22）：原实现 `radius = width/2`（TODO 遗留）把 BYBOX 退化为圆——height 解析后被忽略、width 又少 2 倍，5000×1000km 盒实际是 2500km 圆（南北方向多收 2500km 内的点）。修复：`geoBoxInTxn` 真实矩形过滤（半宽按中心纬度子午线长度换算、经度用环越距离判定以正确跨 ±180°）；`GeoSearchStore` 增加 `shape/boxHeight` 参数；GEOSEARCHSTORE handler 与 `WriteCommand` 复制分支补 BYBOX 解析。三层 9 个新测试（store/handler/replication，每个盒含判别点），远程 `-race` 三包全绿（fab6735） |
| 4 个无测试行为修复回归补全 | 已完成 ✅（2026-08-22/23）：四个行为修复改了运行时逻辑但没补配套测试——CLIENT SETNAME 128 字符上限（736a4f1）、CONFIG SET 未知参数报错（de9d5f8）、COPY 保留源 TTL（ddfc36e）、XINFO CONSUMERS 字段 `seen`→`idle`（13454f3，原覆盖只检查 NotNil 不验证字段名，客户端依赖 Redis 兼容的 `idle` 字段，回退会静默破坏）。补 5 个 handler 层回归测试（含边界值 + 负例 + 精确响应结构断言），本地三包全绿 + 远程 `-race` server 包通过（2b7a777 + 211d7f9） |
| 5 个 Redis 8 缺失命令补全 | 已完成 ✅（2026-08-23）：用本机 redis-server 8.2.1 `COMMAND LIST` 对比 command_info.go 发现 48 个缺失命令，先补 5 个纯别名/只读变体——SUBSTR（GETRANGE 别名）、GEORADIUSBYMEMBER（**基础命令整个缺失**，非只 _RO 变体，复用 geoRadiusCommon 公共逻辑）、GEORADIUS_RO / GEORADIUSBYMEMBER_RO / SORT_RO（禁止 STORE，报 `ERR syntax error` 与 Redis 8 一致）。command_info 250→255 条，官方 arity/flags 元数据。功能测试 + RESP shape 测试 + 注册表计数更新（6860190） |
| redis-py 8 RESP3 wire 兼容 | 已完成 ✅（2026-08-23）：redis-py 8.0.1 默认 RESP3（`DEFAULT_RESP_VERSION=3`），其 RESP3 解析器严格——数组内 `$-1` 会**阻塞挂起**、`*-1` 解析为 []（WatchError 检测失效）、非 `,` 前缀的 score 返回 bytes 而非 float、stream 回复期待 map（`response.items()`）。redis-py 套件从 49/235 崩到 **235/235 全过**，修 5 类 wire 缺口：HGETALL→Map、HMGET/MGET/ZMSCORE 缺失项→Null、ZSCORE/ZINCRBY/ZMSCORE→Double、XREAD/XREADGROUP→扁平 Map（初版嵌套数组编码成 `%0` 的 bug 已修）、EXEC WATCH 冲突→Null；套件脚本 2 处陈旧假设同步修正（ebbc30b）。node-redis 110/110、redis-cli 77/77 同轮全过 |
| BLMPOP + 阻塞命令 RESP3 nil | 已完成 ✅（2026-08-23）：BLMPOP（Redis 7+ 阻塞多 key list pop）补全——store 层 `BLMPopBlocking` 复用 registerAndRecheck 阻塞机制（阻塞唤醒只 pop 1 个，Redis 语义），handler 返回 `[key, [elems]]` 嵌套格式（与 redis-server 8.2.1 实测一致，redis-py 8 双端对比结果相同）。同时发现阻塞命令空结果 RESP3 下 `*-1` 被 redis-py 8 解析为 []（Redis 返回 `_`）——新增 `nilArrayOrNull` helper，BLPOP/BRPOP/BRPOPLPUSH/BZMPOP/BZPOPMAX/BZPOPMIN/ZMPOP/BLMPOP 全部切换（358e45f） |
| RESP3 wire 差分审计 44 项 | 已完成 ✅（2026-08-23）：对 24+20 项命令做 BoltDB vs redis-server 8.2.1 的 RESP3 返回类型差分，修 3 个剩余缺口后 **44/44 零差异**：CONFIG GET→Map（`configArrayToMap`）、XINFO STREAM→Map 且补 first/last-entry（redis-py 8 无条件读 `last-entry`，`streamFirstLastEntries` 用 XRange/XRevRange 取）、XINFO GROUPS/CONSUMERS 每项→Map、ZRANGE 系 WITHSCORES→`[member, Double]` 嵌套（`zrangeWithScoresReply` 覆盖 7 处调用点，ZREVRANGE 指针切片在调用点转换）。redis-py 7.4.1 同轮 235/235 确认 RESP2 无回归（5b01d9e） |
| GEORADIUS STORE/STOREDIST | 已完成 ✅（2026-08-23）：Redis 8 兼容缺口——GEORADIUS 系只支持读路径，STORE/STOREDIST 报 unknown option。`geoRadiusCommon` 增加 STORE/STOREDIST 解析（写 zset：STORE 存 geohash score、STOREDIST 存 km 距离，与 redis-server 8.2.1 实测一致），返回写入数量，目标 key 走 markDirtyKeys 复制传播；_RO 变体仍拒绝。附 Palermo↔Catania 距离核实：166.41 vs Redis 166.27km（<0.1% 差异来自地球半径常量，非 bug）（3e95825） |
| 16 处集群路由检查顺序 bug | 已完成 ✅（2026-08-23）：系统审计全部 `checkAndHandleMultiKeyRedirect(keys)` 调用点，发现 **16 个命令**在 keys 填充**前**检查（空 slice → 集群模式下 MOVED/ASK 路由完全失效，命令在错误节点本地执行）：ZINTERCARD/ZINTER/ZUNION/ZDIFF/ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE/BZPOPMAX/BZPOPMIN/BZMPOP/BLPOP/BRPOP/LMPOP/SINTER/SUNION/SDIFF/SINTERSTORE/SUNIONSTORE/SDIFFSTORE/TS.MGET（ZMPOP/BLMPOP 同批修复）。全部移到填充循环后（5a5210b） |
| Sharded Pub/Sub（Redis 7+） | 已完成 ✅（2026-08-23）：SSUBSCRIBE/SUNSUBSCRIBE/SPUBLISH 完全缺失。store 层 `PubSubManager` 加独立 `shardChannels` 命名空间（Message 加 Shard 标志；`RemoveSubscriber` 同步清理 shard 订阅防泄漏），handler 层 3 命令接入两条分发路径（handler_dispatch + processPubSubCommand），`buildPubSubPush` 输出 `smessage` push。redis-py 8 双端实测与 redis-server 8.2.1 一致（receivers=1、smessage 送达、普通 PUBLISH 不达 shard 订阅者）。command_info 256→259（468a321） |
| redis-py 套件扩展 245/245 | 已完成 ✅（2026-08-23）：套件补 10 个新命令用例（BLMPOP LEFT/RIGHT/timeout、GEORADIUS STORE/STOREDIST 计数与距离、SPUBLISH 送达数与 shard 隔离），235→245 全过；同批用例对 redis-server 8.2.1 同样通过（780d47c） |
| BLMPOP/GEORADIUS STORE 复制传播 | 已完成 ✅（2026-08-23）：新增写命令的复制审计发现两条**静默主从分歧**路径——①BLMPOP 不在 `getWriteCommandSet`/`ReplicatedCommands`，master 的 pop 完全不达 replica（BLPOP/BRPOP/BZMPOP 均有覆盖）；②GEORADIUS(BYMEMBER) 带 STORE/STOREDIST 时在 master 写 zset 但命令标记 readonly，通用传播不触发。修复：BLMPOP 双注册 + `WriteCommand` 非阻塞等价重放（注意 args 含命令名、偏移比 handler 多 1）；GEORADIUS STORE 分支显式传播规范化命令 `GEORADIUS key lon lat radius unit [COUNT n] STORE|STOREDIST dst`（SPOP→SREM 同模式），`WriteCommand` 增 GEORADIUS 重放 case，纯读 GEORADIUS 仍不传播。新增 `TestExecuteReplicatedCommand_BLMPOP/_GEORADIUSStore`，复制对称性 checklist 通过（d24a471） |
| PUBSUB SHARDCHANNELS + redis-cli 套件 86/86 | 已完成 ✅（2026-08-23）：①PUBSUB 补 SHARDCHANNELS [pattern] 子命令（Redis 7+ sharded pub/sub 配套查询，`GetShardChannels` 只列 shard 频道、与普通频道隔离，测试含 pattern 过滤）；②redis-cli 套件新增第 12 节 GEO+BLMPOP（GEOADD/GEORADIUS STORE/STOREDIST 计数与距离/BLMPOP LEFT COUNT 2、RIGHT、超时 nil），77→86 全过；③启动一致性检查抓出 BLMPOP 漏注册 `allDispatchCommands`（手写 dispatch 清单）导致服务无法启动，已补（d5a94f1） |

---

## ZRANK/ZRANGE 基准数据（2026-08-20，五轮审查遗留项关闭）

> d7a8b2d 决策"ZRANK 跳表暂不实施，用基准测试替代"（100/1K/10K/100K 四档）。本次补齐 100K 档并记录基线。
> 环境：远程 192.168.1.251（8 核 Linux），`-benchmem -count=3`（100K 档 `-count=1`），ZRank 取最坏成员（末位）。
> 注：ZRank 测量为稳态（热缓存）路径——首次查询的缓存重建成本（O(n log n)）被 N≈15M 次迭代摊销，不在表内体现。

| 基准 | 100 | 1K | 10K | 100K |
|------|-----|----|-----|------|
| ZAdd（追加新成员） | ~68 µs | ~68 µs | ~68 µs | ~69 µs |
| ZRange(0,-1) 全量 | ~70 µs | ~561 µs | ~7.3 ms | ~79 ms |
| ZRank（最坏成员，热缓存） | ~139 ns | ~131 ns | ~139 ns | ~141 ns |

**结论**：
- ZRank 的 in-memory rank cache 在各规模下均为 O(1)（100→100K 仅 135ns→139ns），无需引入跳表
- ZRange 全量读取为 O(n) 线性（100K → ~80 ms / 17.8 MB 分配），大集合建议客户端分页
- ZAdd 单成员追加各规模基本持平（66-74 µs），瓶颈在单条写事务而非集合规模

**实现说明**：
- `preloadZSet` 改为按 1000 成员分批 ZAdd（单次 100K 成员 ZAdd 超 badger 单请求事务上限 "Txn is too big to fit into one request"）
- `guard_bench.sh` store pattern 改为 `BenchmarkZ(Add|Range|Rank)_(100|1K|10K)$`（`$` 锚定——`-bench` 是子串匹配，不加锚点 `100` 会误匹配 `100K`）；100K 档不纳入 CI 门禁（preload 耗时高 + 无基线触发 benchstat 缺项），手动跑：`bash scripts/remote-test.sh -bench='BenchmarkZ.*_100K' -benchmem ./internal/store/`

---

## 架构边界（已决策：不做）

| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射 | 架构级改造。当前 bounded duplicate window (µs) 可接受 |
| 完全线性化 FULLRESYNC | 等价于上一条。当前保证：无丢失写、无结构性损坏 |
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL (partial) / FUNCTION / MIGRATE | FUNCTION 随 Lua 排除；MIGRATE 不适用（嵌入式）；ACL 仅实现 CAT/LIST/USERS/WHOAMI/HELP，SETUSER/DELUSER 未实现 |
| **1TB+ 规模化验证** | ❌ **无硬件条件** | 仅有 256GB HDD 测试环境，无法完成 1TB+ 验证 |

---

## 生产就绪评估（2026-07-27）

> 基于全部已有数据 + 256GB 实测的生产可用性评估。

### 分场景结论

| 场景 | 结论 | 说明 |
|------|------|------|
| 单节点 / 数十 GB 级数据 | ✅ **生产可用** | 239/239 命令、三方客户端 100% 兼容、7 层 OOM 防护 |
| 主从复制（1主1从/1主多从） | ✅ **生产可用** | K:HASH:47 已根因修复 + 纵深防御，复制回归全 PASS |
| Cluster 多节点（3+） | ⚠️ **可用，建议观察期** | 幽灵节点、ID 持久化已修复，但缺长时 soak |
| 100GB–256GB 数据（HDD） | ✅ **已验证可行** | 2026-07-27 机械盘测试：77.7 MB/s avg，磁盘放大 1.3x，0 损坏 |
| **1TB+ 大规模数据** | ❌ **无硬件条件** | 仅有 256GB HDD 测试环境，无法完成 1TB+ 验证 |
| **强一致性 / 金融级场景** | ❌ **不建议** | BadgerDB LSM + Redis 最终一致性模型，非强一致 |

### 已满足的生产就绪条件

#### 数据安全（✅ 强）
- **239/239 命令**全覆盖，100% RESP3 Null 覆盖
- redis-py (153/153), node-redis (110/110), redis-cli (77/77) **三方客户端 100% 兼容**
- **RDB CRC64 校验** — 从节点加载前验证快照完整性
- **复制 offset 锁步修复**（K:HASH:47） — 主从 offset 漂移元凶已根因修复 + 纵深防御
- **5 轮竞争对手审查**，17/16 项算法缺陷已修复
- **Mutation testing**: 5,201 变异体，100% 击杀率，Store 90.17%

#### 稳定性（✅ 强）
- **7 层 OOM 防护**：GOMEMLIMIT → OutputBufferLimit → L0 背压 → 并发写信号量 → RESP 协议限制 → MaxClients → SCAN 书签淘汰
- **自动内存检测**：启动时自动探测 RAM，按比例推导 OutputBufferLimit / MaxInputBytes
- **goroutine leak 测试**：CI 集成，`>10` 偏差告警
- **Tier A CI 全绿**（lint + unit + fast integration + bench guard）

#### 运维（✅ 中等）
- Docker Compose 支持（standalone / cluster / master-slave / sentinel）
- TOML 配置文件（CLI flag > 配置 > 自动推导 > 硬编码默认值，优先级链完整）
- Prometheus metrics 端点
- 10 个版本 tag，成熟发版节奏（v8.39.1）

### 已知缺口与风险

#### 🔵 已知不阻塞

| 项 | 说明 |
|----|------|
| Cluster 长时 soak | 2026-08-05 已补跑 ✅（2.16 三节点 2GB 混合读写 5min+，goroutine 恒定 28、l0_score 0、cluster_state: ok 全程稳定）；建议后续按需积累小时级监控数据 |
| 3 个重测试保留跳过 | `TestReplicationStress`/`Chaos`/`ClusterMultiNode` — 即使远程也需长时间，GHA 保持跳过 |
| **node1 重启后节点 ID 持久化失败** | 2026-08-04 v8.34.0→v8.51.1 升级：node1 重启后 `loadPersistedNodeID` 未恢复旧 ID（`27fd849d` → `5f8ced07`），槽位 0-5460 被 6339 认领、数据路由错位，已手工 `DELSLOTS`+`ADDSLOTSRANGE` 修复。根因：旧版本 config 顶层 `node_id` 字段缺失，`loadPersistedNodeID` 读到空后生成新 ID。**已修复 ✅**：`loadPersistedNodeID(db, addr)` 增加 addr 回退——顶层 `node_id` 为空时按地址从节点表匹配自身 ID（见 commit），防止升级/重启后 ID 漂移 |

### 256GB 实测数据（2026-07-27）

| 指标 | 实测值 |
|------|--------|
| 数据集 | **256 GB** 净数据（262K keys × 1MB） |
| 存储介质 | 机械盘 `/dev/sda1`（916G） |
| 写入速率 | **77.7 MB/s 平均**，稳定 56 分钟 |
| 磁盘放大 | **1.3x**（256G → 334G 磁盘占用） |
| 数据完整性 | ✅ **100%**（随机抽样验证全通过） |
| 服务稳定性 | ✅ 0 崩溃，0 panic，0 race |

**关键发现：** 机械盘 + 256GB 规模下 BadgerDB compaction 能收敛，1.3x 放大符合预期。速率从 375 MB/s 降至 77.7 MB/s 的瓶颈在 **机械盘随机写入 + compaction 稳态，非代码**。建议 SSD 复测以获取 SSD 基线。

**SSD 复测观察（2026-08-05，未完成）：** 远程 10.1.2.16 有 nvme SSD（/home 129G 可用），尝试 16GB（16,383 keys × 1MB）填充复测。结果**不可靠，未采纳为基线**：16 个 write i/o timeout、DBSIZE 13228/16383（缺 19%）、磁盘占用 91G（放大 5.7x，与机械盘 1.3x 严重背离）。

**机械盘集群复测（2026-08-05）✅ 已完成：** 根因确认为 `scale-data-filler` 每 batch 1000 keys × 1MB = 1GB 单请求体超时（"Keys written" 计数误导），**已修复**：batch 按 value-size 自适应（单批 ≤16MB）+ 失败逐 key 重试精确计数。修复后重测 64GB（65,534 keys × 1MB）**全部完成，0 errors，39m6s（28 keys/s ≈ 28 MB/s 聚合）**。**P5 集群 bug**（见下）已修复并验证：高负载下 PFAIL 标记偶发但无单点误判晋升、无 usurp，仅 1 次合法多数派晋升（本地检测 + 1 peer 报告，2-4s 内自动恢复归还槽位），epoch 稳定 5490 三节点一致，`cluster_state: ok` 全程。**实测数据**：DBSIZE 2,086,131（覆盖写无丢失）、磁盘 76G→207G（+131G，覆盖写 + 旧值未回收，放大 ~2.05x 高于全新写 1.3x，属预期）、0 panic/0 stall。**对比 256GB 单机基线（77.7 MB/s、1.3x）**：集群聚合 28 MB/s 较低——三节点共享同一机械盘 /usr IO 竞争 + 覆盖写 + gossip 开销，非代码瓶颈。

### 推荐生产部署配置

```bash
# 单节点生产
boltDB -dir=/data/boltdb \
  -addr=:6379 \
  -skip-startup-cleanup \
  -log-level=info \
  -client-output-buffer-limit=32MB

# 主从
boltDB -dir=/data/boltdb -addr=:6379 --replicaof 10.0.0.1:6379

# Cluster（3节点起步，先 ADDSLOTS 再 MEET）
boltDB -dir=/data/boltdb -addr=:6379 -cluster
```

**建议配套**：
- Prometheus metrics（默认 `/metrics` 端点）
- 定时 BGSAVE
- 监控：磁盘使用率 >80% 告警、L0 score >10 告警、goroutine 异常增长
- 生产环境**推荐 NVMe SSD**，机械盘仅适用于非延迟敏感场景

---

## 2.16 集群测试计划（2026-08-04，v8.51.2）

**环境**：`10.1.2.16:6337-6339` 三节点集群，v8.34.0 → v8.51.2 升级完成（含 node ID 持久化修复）。

**目标**：验证升级后集群的生产可用性——数据完整性、故障自愈、备份恢复、性能基线。

| 阶段 | 内容 | 验证点 |
|------|------|--------|
| 0 基线 | 版本/集群状态/槽位/节点 ID | 三节点 v8.51.2，`cluster_state: ok`，5461/5462/5461，ID 稳定 |
| 1 冒烟 | 命令覆盖抽查 + 跨节点路由 + 错误路径 | String/List/Hash/Set/ZSet/Stream 代表命令、MOVED/ASK、WRONGTYPE |
| 2 数据 | `scale-data-filler` 灌入 10 万 key + 完整性校验 + 重启持久化 | DBSIZE 之和 = 总数；抽样 GET 全命中；重启后数据不丢 |
| 3 故障 | 单节点 kill + 恢复 + 全节点滚动重启 | 降级窗口、gossip 自愈、槽位回迁、ID 稳定 |
| 4 备份 | BGSAVE → RDB 产物 → 恢复验证 | RDB 完整、恢复后数据一致 |
| 5 性能 | redis-benchmark GET/SET 吞吐 + P99 | 对比 v8.34.0 scale-tier1 基线（GET 68,587 req/s、P99 < 0.9ms） |
| 6 稳定 | 持续混合读写 + goroutine/L0/内存监控 | 无异常增长（2026-08-05 已执行：2GB 混合读写，goroutine 恒定 28、l0_score 0、0 阻塞） |

**执行状态**：✅ 已完成（2026-08-04 阶段 0-5；2026-08-05 补跑阶段 6 长期稳定性）。六阶段全部执行完毕。

### 测试结果摘要（2026-08-04）

| 阶段 | 结果 |
|------|------|
| 0 基线 | ✅ 三节点 v8.51.2，`cluster_state: ok`，槽位 5461/5462/5461，ID 稳定 |
| 1 冒烟 | ✅ String/List/Hash/Set/ZSet 命令覆盖全部通过；跨节点 MOVED 正确；错误路径（WRONGTYPE/非法参数/未知命令）正确 |
| 2 数据 | ✅ 灌入 99,485 keys（0 错误，95,600 keys/s）；DBSIZE 总和精确吻合；抽样 500/500 命中；node2 重启后数据零丢失、ID 稳定 |
| 3 故障 | ✅ kill node3 → 存活节点服务不中断；恢复后 gossip 自愈、数据完整；全节点滚动重启后 99,500 keys 完好（含 TTL 过期归因） |
| 4 备份 | ✅ BGSAVE 三节点 RDB 产物（REDIS0009 魔数 + CRC64 尾部）；LoadRDBWithStore 恢复验证通过（0 错误） |
| 5 性能 | ✅ GET 67,935 req/s（基线 68,587，-0.95% 持平）；P99 < 0.9ms（持平）；SET 62,972 req/s |

### 测试中发现并修复的问题

| 问题 | 根因 | 修复 | 状态 |
|------|------|------|------|
| **P1 视图不一致** | 旧 config 顶层 `node_id` 缺失（v8.34.0 之前创建），升级后 `loadPersistedNodeID` 读到空 → 生成新 ID → 旧 ID 变幽灵节点、槽位被其他节点认领 | 已修（loadPersistedNodeID addr 回退，commit `ad6f7db`）+ 运维 FORGET 幽灵节点 + MEET 重建视图 | ✅ 已修复 |
| **P2 EXPIRE/TTL 集群路由缺失** | EXPIRE/EXPIREAT/PEXPIRE/PEXPIREAT/TTL/PTTL/PERSIST/EXPIRETIME/PEXPIRETIME 缺 `checkAndHandleRedirect`，非 owner 节点返回 0/-2 而非 MOVED | 9 个命令补上集群路由检查（commit `8185d6c`），已部署 2.16 实测 MOVED 生效 | ✅ 已修复 |
| **P3 FAIL 晋升槽位不归还** | `bus.go` PFAIL 多数票 → FAIL 晋升时将失败节点槽位重新分配给"自己"，节点恢复后槽位不归还 → 视图永久错位 | 已修：FAIL 晋升时记录接管清单（`usurpedSlots`，含持久化），节点恢复（直接 PING/PONG 或 gossip 新鲜 PongRecv）时清除 FAIL 标记并归还槽位、提升 epoch 传播到全集群；归还仅限仍由本节点持有的槽位。**2.16 集群实测（2026-08-05）**：kill node3 → node1/node2 晋升 FAIL 并接管 `[10923-16383]` → 恢复 node3 → FAIL 自动清除、槽位自动归还，三节点 `cluster_state: ok`（5461/5462/5461），数据路由/DBSIZE 无异常。实测发现并修复 2 个边界缺口：① 恢复节点自身视图被接管广播污染后无法自我纠正（slot owner reconciliation 跳过"自己"条目）→ 允许自己条目参与 epoch 仲裁；② 仲裁更新 Slots 数组但未同步 `node.Slots` 字段（CLUSTER NODES 显示与广播失真）→ `AddSlotRange` 同步 | ✅ 已修复（commits `8bc4100` + `70b5465`，远程测试 + 2.16 集群实测通过） |
| **P4 BZPOPMAX/MIN on string 不返回 WRONGTYPE** | `redis_cli_compat.sh` 402/406 行：对 string 类型 key 执行 BZPOPMAX/BZPOPMIN，期望返回 `WRONGTYPE`，实际返回 nil（空响应）——阻塞弹出未做类型检查 | 已修（commit `8fcd879`）：`ZPopMax`/`ZPopMin` 事务开头加 `checkKeyType(KeyTypeSortedSet)`，非 zset 类型返回 `ErrWrongType`，handler 层转 WRONGTYPE | ✅ 已修复（兼容套件 77/77，远程测试 store + server 通过） |
| **P5 FAIL 晋升阈值过低（单报告即晋升）** | 64GB 机械盘集群复测（2026-08-05）时发现：`bus.go` 阈值公式 `totalNodes/2+1` 配 `totalNodes<=2→1` 特例，3 节点集群（totalNodes=2）被错误降为 1——单节点 gossip 报告即可把健康节点晋升 FAIL；且 `PromotePFailToFail` 用 `hasFailFlag()`（FAIL **或** PFAIL）判断，本地已 PFAIL 的节点反而无法晋升 | 已修（待提交）：① 阈值改 `(totalNodes+1)/2`（多数派，含本地 1 票）；② 晋升前置条件 `node.HasFailFlag()`（本地已检测 PFAIL，防单报告误判）；③ `PromotePFailToFail` 只检查 FlagFail（允许从 PFAIL 晋升）。高负载下 gossip 超时不再单点误判 FAIL、不再 usurp 健康节点槽位 | ✅ 已修复（远程测试通过；2.16 集群 RESET HARD 重建视图后 `cluster_state: ok`，5461/5462/5461） |

---

## v8.52.0 发布遗留项（2026-08-06）

> 发布 `v8.52.0`（WAL 截断 `f250ad3` + vlog GC/FLUSHDB 保护 `d82b2b4` + GC Flatten `5288001`）后记录的遗留观察，均为非阻塞项。

| 项 | 现象 | 状态 |
|----|------|------|
| **node3 vlog 35GB 未回收** | `DEBUG GC` 在 node1/node2 完整回收（36G/32G → 1.1M），node3 返回 0 次重写：FLUSHDB 的 tombstone 卡在空 L0 层（3.6KB 表，score 0.00），无法下沉到 L5/L6 旧数据触发 discard 统计；`Flatten` 按 score 跳过空层。属 badger 机制限制，非命令缺陷 | ⏳ 待自然 compaction 后重跑 `DEBUG GC`（有写流量触发 L0 堆积后即可） |
| **`CLUSTER INFO` slots_assigned 统计口径** | 原统计"本节点拥有的槽数"（如 5461），Redis 语义为"全集群已分配槽总数"（16384）；客户端据此判断集群完整性 | ✅ 已修复（v8.52.1 `f6e33cc`）：改为全集群口径 + 槽不全时 `cluster_state:fail`；线上三节点均显示 `slots_assigned:16384` |
| **node2 slots_assigned 恒为 5462（off-by-one）** | 已澄清：非统计 bug，是**分配不均**——初始部署范围 `0-5460 / 5461-10922 / 10923-16383` 中 node2 恰好含 5462 个槽（范围含两端）；新口径（全集群槽数）下不再暴露；若需均分可改 `5461-10921 / 10922-16383`（空库时无迁移成本） | ✅ 已澄清（v8.52.1） |
| **gossip 心跳 1s→5s** | 空闲节点每节点 ~4% CPU（gossip PING/PONG + badger compactor 50ms 轮询），实测瞬时 CPU 0%、load 0.27；调大 `pingPeriod` 可降空闲开销 | ✅ 已支持（2026-08-17 核实）：`-gossip-interval` flag 已存在（`cmd/boltDB/main.go:39`，默认 1s，注释已提示可调 5s 降 CPU）；默认保持 1s 保证 PFAIL 检测时效，空闲集群按需 `-gossip-interval=5s` |
| **`BlockCacheSize` 未显式配置** | 吃 badger 默认 256MB/节点，`IndexCacheSize` 已显式 100MB；可显式调小 | ✅ 已配置（2026-08-17）：`define.go` 显式 `BlockCacheSize = 128MB`（降每节点固定内存，注释说明读密集可调大），OPTIMIZATION.md 已同步 |
| **Replay 流式瘦身** | `Replay` 用 `os.ReadFile` 整文件读入 + 逐条 make/加锁；WAL 截断后文件有界（4KB-1.9M），触发条件已消除 | ✅ 已实现（commit `e13e88c`）：`replayStreaming` 两趟扫描 O(64KB) 内存（pass1 找最终 offset，pass2 只恢复活窗口内条目），`replayDirect` 保留为后备；`TestRDBLengthEncoding` 等复制回归已过 |

| **scale-data-filler 集群模式慢（~10 keys/s）** | `cmd/scale-data-filler` 用 `redis.NewClusterClient`，但 `Pipeline()` 跨槽 key 时 Exec 失败 → 回退逐 key Set → 1MB value 只有 ~10 keys/s（机械盘 28 keys/s 的"基线"同样受影响，非磁盘瓶颈）。修复方向：启动时拉 `CLUSTER SLOTS` 按槽分组，每组用普通 client pipeline；SSD 基线本次已改用 redis-benchmark（~340 MB/s 聚合） | ✅ 已修复（2026-08-17）：启动拉 `CLUSTER SLOTS` 建立槽位→节点映射，batch 按槽归属分组、每组用普通 client（非 ClusterClient）pipeline 发往对应节点，内置与 `internal/cluster.Slot` 一致的 CRC16；新增 `main_test.go` 验证 slot 一致性；远程 `-race` 通过。可用 filler 替代 redis-benchmark 跑 SSD 基线 |

| **SSD 基线复测（下次执行）** | 目标：拿到可信 SSD 写入基线（对比机械盘 28 MB/s）。**正确姿势**（2026-08-06 调查结论）：① 先确认 `DEBUG GC` 已完成（GC 与写入严重互斥，GC 期间 1MB SET 减速 1350×）；② `ps aux \| grep redis-benchmark` 确认无残留进程；③ 单进程 `redis-benchmark --cluster -h 10.1.2.16 -p 6337 -t set -n 65534 -r 20000000 -d 1048576 -c 50`（必须带 `-r`，否则覆盖写同一 key）；④ 记录吞吐 + DBSIZE 分布 + 磁盘占用；⑤ 测完 FLUSHDB 清理。备选：先修 `scale-data-filler`（按 CLUSTER SLOTS 分组 pipeline）再用 filler | ⏳ 下次（预计 ~5 分钟/64GB） |

| **usurp 后 CLUSTER NODES 显示失真（小）** | 2026-08-06 实测：FAIL 晋升 usurp 后 `CLUSTER SLOTS` 正确显示接管，但 `CLUSTER NODES` 的接管方行不显示新槽位（node.Slots 字段未随 usurp 同步，P3 的 AddSlotRange 同步修复覆盖了归还路径未覆盖 usurp 路径）；不影响路由 | ✅ 已核实（2026-08-17）：`usurpFailedNodeSlots` 内已含 `Myself.AddSlotRange(i,i)`，usurp 路径 node.Slots 已同步；已补回归断言至 `TestApplyGossipPayloadFailRecoveryDirect`（usurp 后 `Myself.HasSlot` + CLUSTER NODES 输出含 `1000-2000`） |
| **SSD 1MB value 写入调查结论（2026-08-06）** | ① 单次 1MB SET 无 GC 时 0.011s（写入路径正常）；② **DEBUG GC 重写 vlog 期间单次 SET 14.8s（1350× 减速）**——GC 与写入严重互斥（GC 应在维护窗口跑）；③ 并发 benchmark 1-7 keys/s 的测量被"多 benchmark 残留进程 + 集群重启拓扑未稳定 + node2 usurp 事件"污染，非单一根因；④ vlog 74GB"放大"系 benchmark 缺 -r 覆盖写同一 key 所致（65,534×1MB），非 bug | ✅ 已查清（GC 干扰为真实运维注意事项，已记入 cluster-ops.sh gc 说明） |
| **usurp 重启边界** | node 重启后 usurpedSlots 从持久化恢复，但节点未被标 FAIL → 无归还事件 → 槽位永久占用（2026-08-06 线上实测） | ✅ 已修复（v8.52.3 `a25781d`：PONG 新鲜即归还，bus 层触发 + 2 测试；线上 kill/恢复场景验证通过） |

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
| Redis 8 命令补齐 | 5/5 批次完成（UNLINK/ZINTERCARD/BITFIELD_RO/BZMPOP + Stream + TimeSeries + Admin + DEBUG） |
| 全命令准确性测试 | 239/239 命令已覆盖（`TestCommandCompleteness`，16 个数据类型组） |
| Mutation Testing | 5,201 变异体，100% efficacy，82.3% mcover → Store 90.17%（target 90% ✅） |
| 新增 mutation kill 测试 | ~390 个（2026-07-02），远程全部通过 |

---

## 已完成的里程碑

| Phase | 内容 | 完成日期 |
|-------|------|---------|
| 1 | 加固 Coverage 测试（317 个测试审查 + 弱断言替换） | — |
| 2 | Soak 数据一致性（SET→GET/INCR 写后读验证） | — |
| 3 | 并发数据完整性（INCR 精确计数 + SET 最终一致性） | — |
| 4 | 边界和异常（RDB roundtrip/大载荷/连接中断/参数边界） | — |
| 5 | 测试质量度量（mutation testing 引入） | — |
| 6 | 消灭伪覆盖（fire-and-forget/零断言/弱断言/表驱动负面用例） | — |
| 8 | Mutation Testing 重跑（5,201 变异体，100% efficacy） | 2026-06-29 |
| 9 | Mutation Test NOT COVERED 修复（GEO/RESTORE/SORT/Admin） | — |
| 10 | Store 层核心算法优化（List 双向遍历/ZRank 双向扫描/HRandField 蓄水池/APPEND 合并事务） | — |
| 11 | 生产事故回归测试（振荡/写入风暴/脑裂/磁盘压力/BGSAVE 并发/PubSub 扇出） | — |
| 12 | Redis 8 命令补齐（5 批次，239/239 命令） | — |

---

## 下一步（按收益排序）

### 1. 修复 FLUSHDB Stream 内部 key 清理 ✅

**投入**：S（半天）　**收益**：高——生产环境真实 bug

- [x] `Del()` 缺少 `KeyTypeStream`/`KeyTypeHyperLogLog`/`KeyTypeGeo` 分支 → 已补全
- [x] `NextStartup()` 缺少 Stream/HLL/Geo 孤儿清理 → 已补全 3 个 cleanup 函数
- [x] `checkDataExists()` 缺少 Stream/HLL/Geo → 已补全
- [x] 远程验证通过：Stream teardown 0 orphan data keys

### 2. 主从写命令传播测试 ✅

**投入**：M（1 天）　**收益**：高——复制模式下写命令正确性零覆盖

- [x] 新建 `cmd/integration/replication_completeness_test.go`
- [x] 9 个数据类型组：String/Key/List/Hash/Set/SortedSet/Stream/JSON/HLL/Geo
- [x] 覆盖 ~70 个写命令的 master→slave 传播验证
- [x] 发现并修复 XTRIM panic（`stream.go:1018` 空切片越界）
- [x] 远程验证通过：9/9 组 PASS

### 3. mcover 提升至 90% ✅

**投入**：已完成两轮　**收益**：中——防御性提升

**第一轮已完成：**
- [x] 新建 `internal/server/handler_mutation_kill2_test.go`（35 个测试）— handler 层变异体
- [x] 新建 `internal/store/base_mutation_kill_test.go`（15 个测试）— store 层 Del/NextStartup/FlushDB 变异体
- [x] 共 50 个新测试，覆盖 ~120 个 NOT COVERED 变异体
- [x] 远程测试通过：50/50 PASS，无回归
- [x] 新建 `internal/server/info_handler_core_mutation_kill_test.go`（48 个测试）— info.go + handler_core.go 变异体
- [x] 新建 `internal/store/base_mutation_kill3_test.go`（66 个测试）— Del JSON/TimeSeries、Rename 全类型覆盖
- [x] 新建 `internal/server/zset_mutation_kill_test.go`（~80 个测试）— ZSET 全参数验证分支
- [x] 新建 `internal/server/stream_ts_mutation_kill_test.go`（~60 个测试）— Stream + TimeSeries 全参数验证分支

**第二轮已完成（2026-07-02）：**
- [x] 新建 `internal/store/mutation_kill_mcover90_test.go`（~80 个测试）— sorted_set decodeDataValue/memberInLexRange、list.go LPos、define.go extractRawKey、string.go/Expire/PExpire/RenameNX/Scan/ObjectEncoding 等
- [x] 修复 `internal/store/mutation_kill_phase9_test.go` 编译错误（TSExists 不存在 + uint64 类型不匹配）
- [x] 远程测试通过：`-race -short` 全 PASS

**Mutation Testing 最终结果（2026-07-02）：**
- Store 包：Runnable 3,110 / Not Covered 339 / **mcover 90.17%**（目标 90% ✅）
- Efficacy: 100%（所有 runnable 变异体均已杀死）
- 新增 ~80 个测试（1 个新测试文件 + 1 个修复文件）

**当前状态**：mcover 达标 90%。如需继续提升：
- 针对 store 包剩余 339 NOT COVERED（stream.go 93、sorted_set.go 59、define.go 38、base.go 37、string.go 32）补充测试
- 剩余 NOT COVERED 多为 corruption paths、blocking operations、dead code，测试难度较高

### 4. 长时间 Soak 稳定性 ✅

**投入**：S（命令已就绪）　**收益**：中——验证 24h+ 运行稳定性

基础设施已完备，直接在 2.16 后台运行：

```bash
# 单机 Soak（默认 1h，可调）
SOAK_DURATION=24h go test -race -timeout 25h -run TestSoak -count=1 ./cmd/integration/ &

# 主从 Soak
SOAK_REPL_DURATION=24h go test -race -timeout 25h -run TestSoakReplication -count=1 ./cmd/integration/ &

# 或用 nightly 脚本（含 standalone + replication + evolution gate）
SOAK_DURATION=1h SOAK_REPL_DURATION=1h bash scripts/run_nightly_soak.sh
```

- [x] `TestSoak`：单机随机操作 + 并发客户端 + 压力监控
- [x] `TestSoakReplication`：主从复制 + 生命周期断连重连
- [x] `TestClusterSoak`：集群多节点 + 槽位混沌
- [x] 报告输出到 `/tmp/soak-artifacts/`（Markdown + JSON）
- [x] 2.16 上运行超过 24h（PID 2057040，自 2026-06-30 起，CPU 102%，内存稳定）

---

## 已知问题

第一阶段（11 项）已全部修复 ✅。第二阶段（8 项）已完成（2026-07-02 本次整改）。

---

## 架构边界（已决策：不做）

| 边界 | 原因 |
|------|------|
| commit-seq ↔ repl-offset 映射 | 架构级改造。当前 bounded duplicate window (µs) 可接受 |
| 完全线性化 FULLRESYNC | 等价于上一条。当前保证：无丢失写、无结构性损坏 |
| EVAL / SCRIPT (Lua) | Lua 沙箱逃逸风险 + 维护成本，非定位 |
| Lock Sharding / Lock-Free | 复杂度指数级，有明确瓶颈时再碰 |
| ACL (partial) / FUNCTION / MIGRATE | FUNCTION 随 Lua 排除；MIGRATE 不适用（嵌入式）；ACL 仅实现 CAT/LIST/USERS/WHOAMI/HELP，SETUSER/DELUSER 未实现 |

---

## 整改计划（收购前必须完成）

> 来源：2025-07 — 2026-07 两轮尽职调查。
>
> **第一阶段**（已完成）：2025-07 初始审查识别的 11 项（A1–C4，见下方）。
>
> **第二阶段**（进行中）：2026-07 收购审查新增 8 项（D1–D8），以最苛刻标准审视代码库健康度。
>
> 每项标注投入（S/M/L/XL）和优先级（P0 = 阻塞收购 / P1 = 必须修复 / P2 = 强烈建议）。

### A. 安全防护（P0 — 不修复则不具备生产可用性）

#### A1. TLS 加密传输

**投入**：L（3-5 天）　**优先级**：P0

- [x] 新增 `--tls-cert` / `--tls-key` / `--tls-ca` CLI 参数
- [x] `listener` 层包装 `tls.NewListener()`，支持可选 TLS（不配置则明文，兼容旧客户端）
- [x] 复制连接（`handleSlaveReplicationConnection`）支持 TLS 拨号
- [x] 集群总线（`internal/cluster/bus.go`）支持 TLS 连接
- [x] 哨兵 gossip（`internal/sentinel/network.go`）支持 TLS 连接
- [x] 测试：TLS 握手 + 非 TLS 客户端被拒绝（如配置了 `--tls-require`）
- [x] 文档：部署指南增加 TLS 配置章节

#### A2. 连接数限制与空闲超时

**投入**：M（1-2 天）　**优先级**：P0

- [x] `handler_core.go:handleConnection()` 入口处增加 `maxclients` 检查，超限返回 `-ERR max number of clients reached`
- [x] 用 `atomic.Int64` 跟踪当前连接数，`Shutdown` 时减去
- [x] 新增 `--maxclients` CLI 参数（默认 10000）
- [x] 新增 `--timeout` CLI 参数（空闲超时秒数，默认 0 = 不超时）
- [x] `handleConnection()` 中 `ReadRESP` 前设置 `conn.SetReadDeadline()`
- [x] 每次成功读取后重置 deadline
- [x] 测试：连接数上限拒绝 + 空闲超时断开

#### A3. RESP 协议解析加固

**投入**：S（半天）　**优先级**：P0

- [x] `proto/resp.go:readLine()` 增加行长度限制（默认 512MB → 改为 64MB，可配置）
- [x] 新增 `MaxLineLen` 常量，超限返回 `ERR protocol error`
- [x] `MaxBulkLen` 从 512MB 降为 **256MB**（与 Redis 默认 `client-query-buffer-limit` 对齐）
- [x] 新增 `--proto-max-bulk-len` CLI 参数
- [x] Inline 命令解析增加引号支持（兼容 `SET key "hello world"`）

#### A4. 密码比较安全加固

**投入**：S（1 小时）　**优先级**：P1

- [x] `admin2_commands.go:307` 替换 `==` 为 `crypto/subtle.ConstantTimeCompare()`
- [x] 同时检查 `internal/sentinel/network.go` 中 AUTH 命令构建是否泄露密码长度
  - **结论**：无需修复，`$%d` 是 RESP 协议标准格式，非安全漏洞
- [x] 测试：验证错误密码比较时间与正确密码一致（timing attack test）

---

### B. 集群/哨兵/复制缺陷（P1 — 核心功能不可靠）

#### B1. 集群 slot 迁移实现

**投入**：L（5-8 天）　**优先级**：P1

- [x] 实现 `CLUSTER MIGRATESLOT <slot> <targetNodeID> [COPY]` 命令，从源节点读取 slot 内所有 key 并逐个迁移到目标节点
- [x] 目标节点通过 RESTORE 命令接受迁移中的 key 写入（自动 REPLACE）
- [x] 迁移完成后自动调用 `SETSLOT NODE <node-id>` 更新 slot ownership
- [x] 迁移过程中对 slot 内 key 的请求返回 `ASK` 重定向（已有的 `CheckSlotRedirect`）
- [x] 迁移中断恢复机制（基于 `MIGRATING-TO` 元数据持久化到集群配置）
- [x] 集群总线 `GossipPayload` 从 JSON 改为紧凑二进制编码（使用 `encoding/gob`，向后兼容 JSON）
- [x] 集群 bus 使用 `context.Context` 替代 `context.Background()`，响应服务关闭
- [x] 修复 Gossiper `started` bool 的 data race（`gossip.go:27`），改用 `atomic.Bool`
- [x] 修复 `CLUSTER SETSLOT STABLE` 实际清除迁移状态（之前为空操作）
- [x] 测试：slot 迁移完整生命周期 + MOVED/ASK 重定向（已有 `TestClusterMigrateSlot`、`TestClusterAskRedirect`、`TestClusterMovedRedirect`）

#### B2. 哨兵 ODOWN 多哨兵共识

**投入**：M（2-3 天）　**优先级**：P1

- [x] 重写 `master.go:checkMaster()` 中的 ODOWN 判断逻辑
- [x] ODOWN 基于**不同哨兵实例的 SDOWN 报告数** ≥ quorum（`sdownReporters map[string]bool` per-sentinel 去重）
- [x] gossip channel 满时不再丢弃 SDOWN 消息（阻塞 + 丢弃最旧 + 扩容至 256）
- [x] SDOWN 线协议携带源哨兵 `runID`，接收端调用 `ReportSdown(sourceRunID)` 去重
- [x] 验证：3 哨兵 + 1 主节点场景下，单哨兵重复报告不触发 failover
- [x] 验证：3/3 哨兵确认 SDOWN 后正确触发 ODOWN + failover

#### B3. 复制流静默丢弃修复

**投入**：S（半天）　**优先级**：P0

- [x] `psync.go:1792-1794` default 分支：收到未知写命令时**返回错误**而非静默 `return nil`
- [x] 评估：是命令未注册（应报错）还是读命令（可安全忽略）— 分为两个分支
- [x] 新增 `isWriteCommand` 自动校验：启动时对比 `handler_dispatch.go` 的 switch 和 `isWriteCommand` map，不一致则 panic
- [x] 测试：向从库发送未注册命令，验证返回错误

---

### C. 项目治理（P2 — 信任与合规）

#### C1. LICENSE 修复

**投入**：S（10 分钟）　**优先级**：P1

- [x] 删除 `LICENSE` 第 4 行嵌入的中文 Markdown（`- **为什么？** 好 README = 好星标。...`）
- [x] 恢复为标准 MIT License 纯文本
- [x] 检查 git 历史中 LICENSE 是否曾被修改，确认 MIT 许可证的完整性
- [x] 如果有贡献者，在 README 中明确标注

#### C2. go.mod 版本号修正

**投入**：S（5 分钟）　**优先级**：P1

- [x] 将 `go 1.25.7` 修改为当前实际使用的 Go 稳定版本（`go 1.24.x`）
  - **实际验证**：`go 1.25.7` 在当前环境 `go1.26.4 darwin/amd64` 下有效，无需修改
- [x] 同步更新 CI workflow 中的 Go 版本（go.yml + nightly-soak.yml 均为 1.25.7）
- [x] 确认 `go.sum` 与实际版本一致（`go mod verify`：all modules verified）

#### C3. 仓库卫生清理

**投入**：S（30 分钟）　**优先级**：P2

- [x] 删除仓库中所有编译产物（`*.test`、`boltDB`、`debug`、`evolution`、`sentinel`、`benchmark`）
- [x] `.gitignore` 增加：`*.test`、`/build/`、`node_modules/`、`package-lock.json`
- [x] `git rm --cached package-lock.json`
- [x] 创建 `CONTRIBUTING.md`（即使是单人项目，收购后会有团队加入）
- [x] 创建 `CODE_OF_CONDUCT.md`（Contributor Covenant v2.0）

#### C4. 关键错误处理修复

**投入**：M（2-3 天）　**优先级**：P1

- [x] `internal/replication/psync.go` 中 11 处 `strconv.Parse*` 静默忽略错误 → 改为返回错误
  - 行 1163-1164（LIMIT 参数）、1358-1359（GEO 坐标）、1371（半径）、1386（COUNT）、1483（保留期）、1508（时间戳）、1634-1635（偏移量）
- [x] `internal/server/stream_commands.go:1030` `_ = h.Db.XAckDelRemoveRefs(...)` → 检查并返回错误
- [x] `internal/server/migrate_command.go:124` `_, _ = h.Db.Del(migrateKey)` → 检查并返回错误
- [x] 评估 `handler_core.go` 中 goroutine launch site 的 panic 恢复覆盖 → 发现 5 个 goroutine 零 recover，已全部添加 panic recovery + 日志 + 资源清理

---

### D. 收购审查新增项（P0–P2）

> 来源：2026-07-02 收购尽职调查，以潜在收购方视角全面审查。
> 与第一阶段互补：第一阶段聚焦安全/集群/合规，第二阶段聚焦代码库长期健康度、可维护性、生产稳健性。

#### D1. init() panic 风险（P0 — 单点故障）

**投入**：S（半天）　**优先级**：P0

- [x] 已验证：`ValidateWriteCommandConsistency()` 已在 `main.go:56` 调用，不在 `init()` 中（代码已正确实现）
- [x] 验证通过：启动时返回错误而非 panic

#### D2. BadgerDB 错误字符串匹配（P0 — 版本升级熔断风险）

**投入**：S（半天）　**优先级**：P0

- [x] 已验证：`set.go` 及全库已使用 `errors.Is(err, badger.ErrConflict)` 替代字符串匹配
- [x] 验证通过：retry 逻辑使用 `errors.Is` 而非 `strings.Contains`

#### D3. Store 层巨型文件拆分（P1 — 收购后团队接手成本）

**投入**：M（3-5 天）　**优先级**：P1

当前 5 个单文件超 1900 行（handler.go 已拆分，store 层已拆分 base.go + sorted_set.go）：

| 文件 | 原行数 | 现行数 | 状态 | 拆分后文件 |
|------|--------|--------|------|-----------|
| `internal/store/base.go` | 2,258 | 566 | ✅ 已拆分 | `del.go`、`scan.go`、`rename.go`、`dump_restore.go`、`cleanup.go`、`memory.go` |
| `internal/store/sorted_set.go` | 2,247 | 271 | ✅ 已拆分 | `zset_types.go`、`zadd_zrem.go`、`zrange.go`、`zrank.go`、`zpop.go`、`zinter_store.go`、`zlex.go`、`zcard_score.go`、`zscan_rand.go` |
| `internal/store/list.go` | 2,149 | — (已删除) | ✅ 已拆分 | `list_types.go`、`lpush_rpop.go`、`lpushpop_core.go`、`lrange_lindex.go`、`ltrim.go`、`linsert.go`、`blpop.go` |
| `internal/store/stream.go` | 1,942 | — (已删除) | ✅ 已拆分 | `stream_types.go`、`xadd.go`、`xread.go`、`xrange.go`、`xdel.go`、`xinfo.go`、`xtrim.go`、`xgroup.go`、`xreadgroup.go`、`xack.go`、`xautoclaim.go` |
| `internal/store/string.go` | 1,118 | 1,240 | 可按需拆分 | — |
- [x] base.go 已拆分（2,258→566 行）为 6 个功能文件
- [x] sorted_set.go 已拆分（2,247→271 行）为 9 个功能文件
- [x] list.go 已拆分（2,149→已删除）为 7 个功能文件
- [x] stream.go 已拆分（1,942→已删除）为 11 个功能文件
- [x] 验证：`go build ./internal/store/` 通过 + `go build ./...` 通过

#### D4. connState 锁保护不一致（P1 — 隐藏竞态风险）

**投入**：S（1 天）　**优先级**：P1

- [x] `internal/server/handler_dispatch.go:19,29,34`：已为 `state.authenticated`、`state.inTransaction`、`state.commands` 添加 `state.mu.Lock()`/`Unlock()` 保护
- [x] 采用**方案 A**：在 `executeCommand` 入口处统一加锁
- [x] 也修复了 `handler_dispatch.go` default 分支中的无锁访问
- [x] 验证：`go build ./...` 通过

#### D5. 错误链截断 — `fmt.Sprintf("ERR %v", err)`（P1 — 可调试性）

**投入**：M（1-2 天）　**优先级**：P1

- [x] `internal/server/handler_utils.go:wrapStoreError()`：已添加 `logger.Debug().Err(err).Msg("store error wrapped for client response")` 保留完整错误链
- [x] 新增 `wrapLogError(err) proto.RESP` 辅助函数，自动记录错误链到日志
- [x] 全库 15 个文件、**83 处** `proto.NewError(fmt.Sprintf("ERR %v", err))` ✅→ `wrapLogError(err)`
- [x] 涵盖：zset_commands(9)、hash_commands(3)、timeseries_commands(17)、string_commands(4)、stream_commands(19)、json_commands(12)、geo_commands(5)、list_commands(3)、set_commands(2) 等
- [x] 验证：`go build ./...` 通过
- [x] 剩余少量非常规格式（如内联 logger 记录 + 4 处 migrate_command.go 错误处理）已处理

#### D6. gofmt 一致性强制执行（P1 — 工程纪律红线）

**投入**：S（1 小时）　**优先级**：P1

- [x] 运行 `gofmt -w` 修正 `internal/server/handler_dispatch.go`
- [x] CI 中增加 `gofmt -d` 检查步骤（`go.yml` 中 lint 后执行）
- [x] 验证：`gofmt -l .` 返回空

#### D7. 全局可变状态原子化（P2 — 并发安全）

**投入**：S（半天）　**优先级**：P2

- [x] `internal/proto/resp.go:15-22`：`MaxBulkLen`、`MaxArrayLen`、`MaxLineLen` 从 `var int64` 改为 `atomic.Int64`
- [x] `SetMaxBulkLen` 使用 `.Store()`
- [x] 所有读取位置使用 `.Load()`
- [x] 同步更新 `internal/replication/master.go` 中的 3 处引用
- [x] 验证：`go build ./...` 通过

#### D8. 监控包生产构建副作用（P2 — 二进制纯净度）

**投入**：S（半天）　**优先级**：P2

- [x] 验证：`cmd/boltDB/main.go` 不导入 `internal/monitor`
- [x] 验证：`strings build/boltDB | grep "git log"` 返回空（主二进制不含 git 依赖）
- [x] 在 `internal/monitor/anomaly.go` 添加注释警告：禁止在主二进制中导入
- [x] `getCommitsInWindow()` 已在 git 不可用时优雅降级（返回 nil）

---

### 整改进度跟踪

| 项 | 负责人 | 预估开始 | 状态 | 完成日期 |
|----|--------|---------|------|---------|
| A1. TLS 加密传输 | AI | — | ✅ 已完成所有子项 | 2026-07-02 |
| A2. 连接数限制与空闲超时 | AI | — | ✅ 已完成 | 2025-07 |
| A3. RESP 协议解析加固 | AI | — | ✅ 已完成 | 2025-07 |
| A4. 密码比较安全加固 | AI | — | ✅ 已完成 | 2025-07 |
| B1. 集群 slot 迁移 | AI | — | ✅ 已完成 | 2026-07-02 |
| B2. 哨兵 ODOWN 共识 | AI | — | ✅ 已完成 | 2025-07 |
| B3. 复制流静默丢弃 | AI | — | ✅ 已完成 | 2025-07 |
| C1. LICENSE 修复 | AI | — | ✅ 已完成 | 2025-07 |
| C2. go.mod 版本号 | AI | — | ✅ 验证有效 | 2025-07 |
| C3. 仓库卫生清理 | AI | — | ✅ 已完成 | 2025-07 |
| C4. 关键错误处理 | AI | — | ✅ 已完成 | 2025-07 |
| D1. init() panic 风险 | AI | — | ✅ 已验证：ValidateWriteCommandConsistency 已在 main.go:56 调用，未使用 init() | 2026-07-02 |
| D2. BadgerDB 错误字符串匹配 | AI | — | ✅ 已验证：store 层已全量使用 errors.Is(err, badger.ErrConflict) | 2026-07-02 |
| D3. Store 层巨型文件拆分 | AI | 2026-07-02 | ✅ 已完成：base.go + sorted_set.go + list.go + stream.go 已拆分（28 个新文件） | 2026-07-02 |
| D4. connState 锁保护不一致 | AI | — | ✅ 已完成：handler_dispatch.go 加锁保护 | 2026-07-02 |
| D5. 错误链截断 | AI | — | ✅ 已完成：83 处替换 + wrapLogError 辅助函数 | 2026-07-02 |
| D6. gofmt 一致性强制执行 | AI | — | ✅ 已完成：gofmt -w + CI 检查步骤 | 2026-07-02 |
| D7. 全局可变状态原子化 | AI | — | ✅ 已完成：atomic.Int64 替代 var int64 | 2026-07-02 |
| D8. 监控包生产构建副作用 | AI | — | ✅ 已验证：主二进制不含 git log | 2026-07-02 |

### 优先级排序建议

```
第一阶段（已完成）：
  Week 1:  C1 + C2 + C3（低成本，立即修复信任问题）
           + A3 + A4（安全加固小项）
           + B3（复制静默丢弃，半天可修复）
  Week 2:  A2（连接限制与超时）
           + B2（哨兵 ODOWN 重写）
  Week 3-4: A1（TLS 全链路加密）
  Week 5-8: B1（集群 slot 迁移，最大工程量）

第二阶段（已完成，2026-07-02 更新）：
  Day 1:   D1 + D2（✅ 已验证已修复，非阻塞性问题）
           + D4 + D7（✅ 已完成：connState 加锁 + atomic.Int64）
  Day 2:   D6（✅ 已完成：gofmt + CI 检查）
           + D8（✅ 已验证完成：主二进制不含 git 依赖）
  Week 2:  D3（✅ 已完成：base.go + sorted_set.go + list.go + stream.go 已拆分，28 个新文件）
  Week 3:  D5（✅ 已完成：83 处错误链替换 + wrapLogError 辅助函数）
```

---

## 附录：Verification Commands

```bash
# 全命令准确性测试（单机，~1s）
bash scripts/remote-test.sh -race -timeout 30s -run TestCommandCompleteness ./cmd/integration/...

# 全部单元测试
bash scripts/remote-test.sh -race -short ./internal/...

# RESP shape suite
bash scripts/remote-test.sh -race -timeout 30s -run TestRESPShape ./internal/server/...

# Goroutine leak
bash scripts/remote-test.sh -race -timeout 120s -run TestGoroutineLeak ./cmd/integration/...

# Compatibility
python3 scripts/redis_py_compat.py
node scripts/redis_node_compat.mjs
bash scripts/redis_cli_compat.sh

# 后台全量测试（2.16 服务器）
bash scripts/test-all-commands-remote.sh

# Linter
golangci-lint run --timeout 5m

# Mutation testing
bash scripts/mutation-test.sh --run
```

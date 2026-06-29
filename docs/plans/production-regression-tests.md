# 生产事故回归测试计划

## 现有回归测试覆盖

| 生产事故 | 回归测试 | 状态 |
|---------|---------|------|
| Backlog Exhaustion | `TestRegressionBacklogExhaustion` | ✅ 已有 |
| L0 Collapse | `TestRegressionL0Collapse` | ✅ 已有 |
| Replication Thrash | `TestRegressionReplicationThrash` + `Fullresync` | ✅ 已有 |
| Retry Storm | `TestRegressionRetryStorm` | ✅ 已有 |
| Shutdown Race | `TestRegressionShutdownRace` | ✅ 已有 |
| Snapshot Inconsistency | `TestRegressionSnapshotConsistency` + `ConcurrentWrites` + `FullresyncOffset` | ✅ 已有 |
| Deterministic Replay | `TestRegressionCanonical*` (SPOP/XADD/EXPIRE) | ✅ 已有 |
| Duplicate Window | `TestRegressionDuplicateWindowMeasurement` | ✅ 已有 |
| PSYNC Reconnect | `TestRegressionPsyncReconnectNoLoss` | ✅ 已有 |
| Failover Oscillation | `TestRegressionFailoverOscillation` | ✅ 已写 (v8.32.0) |
| Replication Write Deadline | `TestRegressionWriteDeadlineStorm` | ✅ 已写 (v8.32.0) |
| Split-Brain Convergence | `TestRegressionSplitBrainConvergence` | ✅ 已写 (v8.32.0) |

## 未覆盖的生产事故

### 1. Failover Oscillation（Sentinel 振荡）

**事故描述：** 网络分区恢复后，stale gossip 导致 sentinel 在新旧 master 之间翻转。`selectNewMaster` 不验证 slave 存活，反复选中死节点。

**症状：** Sentinel agreement 剧烈波动，`FailoverStarted` >> `SuccessfulFailovers`。

**回归测试设计：**
- 启动 master + 2 slave，配置 sentinel
- 注入网络分区（kill master），等待 sentinel 选出新 master
- 恢复旧 master，观察 sentinel 是否稳定在新 master
- 验证：agreement 不出现 >10% 波动，failover cooldown 生效，无震荡循环

### 2. Replication Write Deadline Storm

**事故描述：** `SetWriteDeadline` 在 slave 加载 RDB 期间触发超时，bufio.Writer 进入不可恢复状态，导致反复 FULLRESYNC 循环。

**症状：** Reconnect/FULLRESYNC 风暴，每次循环丢失写入。

**回归测试设计：**
- 启动 master + slave，写入数据触发 FULLRESYNC
- 在 slave 加载 RDB 期间写入大量数据
- 验证：FULLRESYNC 后数据完整，reconnect count ≤ 5，无写入风暴

### 3. Split-Brain Convergence（脑裂收敛）

**事故描述：** 网络分区恢复后，stale gossip 导致非单调 agreement 变化，系统在分叉状态之间振荡。

**症状：** agreement 从 100% 骤降再恢复，收敛窗口过长。

**回归测试设计：**
- 启动 3 节点 sentinel 集群
- 注入网络分区，等待 sentinel 进入 split-brain
- 恢复分区，监控 agreement 收敛轨迹
- 验证：agreement 单调递增，收敛时间 < 60s，无振荡

### 4. Concurrent FULLRESYNC + Write Storm

**事故描述：** 高写入并发下触发 FULLRESYNC，RDB 生成期间 backlog 被覆盖，slave 永远无法增量同步。

**回归测试设计：**
- 启动 master + slave
- 30 个 writer 持续写入
- Kill slave 触发 backlog 溢出
- 恢复 slave，验证 FULLRESYNC 后数据一致
- 验证：最终 offset lag < 100，无 goroutine 泄漏

### 5. Graceful Degradation Under Disk Pressure

**事故描述：** 磁盘 I/O 变慢（如 SSD 磨损），BadgerDB compaction 阻塞，L0 持续升高，写入被拒绝。

**回归测试设计：**
- 模拟慢磁盘（大量并发写入占满 IOPS）
- 验证 backpressure 系统正确触发（soft threshold → delay，hard threshold → reject）
- 停止写入后，L0 恢复到正常水平
- 验证：无 panic，L0 < 20，写入恢复

### 6. Client Buffer Overflow

**事故描述：** 慢客户端消费不过来，output buffer 超限，连接被 kill。

**回归测试设计：**
- 启动 server，配置 client-output-buffer-limit
- 客户端发送大量命令但不读响应
- 验证：连接被正确 kill，server 不受影响，其他客户端正常

### 7. RDB + Concurrent Config Change

**事故描述：** BGSAVE 期间修改 maxmemory 或其他配置，导致 RDB 与新配置不一致。

**回归测试设计：**
- 触发 BGSAVE
- 在 RDB 生成期间修改 maxmemory
- 验证：重启后配置正确，数据一致

### 8. PubSub Fan-Out Storm

**事故描述：** 大量订阅者 + 高频 publish，导致内存爆炸或 OOM。

**回归测试设计：**
- 创建 100 个订阅者
- 10 个 publisher 持续 publish 大消息
- 验证：无 OOM，goroutine 泄漏 ≤ 20，server 响应正常

## 优先级排序

| 优先级 | 测试 | 原因 |
|--------|------|------|
| P0 | Failover Oscillation | 生产环境 sentinel 部署，直接影响可用性 |
| P0 | Replication Write Deadline | 已有修复但无回归测试，复发风险高 |
| P0 | Split-Brain Convergence | 生产环境多节点部署，脑裂恢复是关键路径 |
| P1 | Concurrent FULLRESYNC + Write Storm | 复制与写入风暴叠加是高并发场景 |
| P1 | Client Buffer Overflow | 慢客户端是生产常见问题 |
| P2 | Graceful Degradation Under Disk Pressure | 需要特殊环境模拟 |
| P2 | RDB + Concurrent Config Change | 低频操作，风险可控 |
| P2 | PubSub Fan-Out Storm | PubSub 使用场景有限 |

## 执行计划

1. ✅ 运行现有 12 个回归测试，确认基线
2. ✅ 补充 P0 级别的 3 个回归测试（Failover Oscillation / Write Deadline Storm / Split-Brain Convergence）
3. 补充 P1 级别的 2 个回归测试
4. 补充 P2 级别的 3 个回归测试
5. 所有测试纳入 Tier B（post-merge）自动运行

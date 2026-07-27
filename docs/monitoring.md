# BoltDB 监控

## 概述

BoltDB 通过 `internal/metrics/` 包提供内置的 Prometheus 兼容指标端点。指标采集器（`Collector`）通过函数指针注入各模块的实时读数，快照缓存策略避免高频采集时的锁竞争。

## 启用

通过 `-metrics-addr` 命令行参数指定 Prometheus 指标端点的监听地址：

```bash
# 监听所有接口的 6338 端口
go run ./cmd/boltDB/ -metrics-addr :6338

# 或构建后运行
./build/boltDB -addr :6337 -dir /var/lib/boltDB -metrics-addr :6338
```

默认值为空字符串（禁用）。指定端口后，服务启动日志会输出：

```
INFO metric endpoint enabled addr=:6338
```

## HTTP 端点

| 路径 | 格式 | 用途 |
|------|------|------|
| `/metrics` | Prometheus exposition format (v0.0.4) | **标准 Prometheus 抓取端点** |
| `/debug/metrics` | 纯文本可读格式 | 人工调试 / 脚本解析 |
| `/debug/vars` | JSON (indented) | 程序化读取 / soap dashboard |
| `/healthz` | `{"status":"ok"}` | 健康检查 |

## 指标全集

所有指标以 `boltdb_` 为前缀。下方列出名称、类型和说明。

### 构建信息

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `boltdb_build_info{version="8.50.1"}` | gauge | 版本号，固定值 1（标签携带版本信息） |

### L0 / 写背压

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `boltdb_l0_score` | gauge | BadgerDB L0 层 compaction 分数。1-5 正常，5-10 轻度延迟，10-20 明显延迟，>20 拒绝写入 |
| `boltdb_active_retries` | gauge | 当前正在重试的写操作数（`retryUpdate` goroutine 并发数） |
| `boltdb_total_retries` | counter | 累计重试写操作次数 |
| `boltdb_writes_blocked` | gauge | 因全局信号量（默认 50）达到上限而被阻塞的写操作数 |
| `boltdb_writes_l0_rejected` | counter | 因 L0 分数超过硬阈值（默认 20.0）而被拒绝的写操作数 |
| `boltdb_writes_l0_delayed` | counter | 因 L0 分数超过软阈值（默认 8.0）而被延迟的写操作数 |

详见 `internal/store/backpressure.go` 和 `docs/failures/l0-collapse.md`。

### Go 运行时

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `boltdb_goroutines` | gauge | 当前 goroutine 数量 |
| `boltdb_alloc_bytes` | gauge | 堆上已分配字节数 (`MemStats.Alloc`) |
| `boltdb_heap_inuse` | gauge | 正在使用的堆内存字节数 (`MemStats.HeapInuse`) |
| `boltdb_stack_inuse` | gauge | 正在使用的栈内存字节数 (`MemStats.StackInuse`) |
| `boltdb_gc_total` | counter | GC 累计执行次数 (`MemStats.NumGC`) |

### 复制

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `boltdb_role` | gauge | 角色：1 = master，0 = slave |
| `boltdb_master_repl_offset` | gauge | 主节点复制偏移量（所有写入的累计字节数） |
| `boltdb_slave_repl_offset` | gauge | 从节点已应用的复制偏移量 |
| `boltdb_replication_lag` | gauge | 主从复制延迟（`master_offset - slave_offset`，仅 slave 角色有意义） |
| `boltdb_slave_count` | gauge | 当前连接的从节点数量 |
| `boltdb_reconnect_count` | counter | 从节点累计重连次数 |

### 复制 Backlog

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `boltdb_backlog_size` | gauge | backlog 总容量（字节） |
| `boltdb_backlog_available` | gauge | backlog 可用空间（字节） |

### 客户端连接

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `boltdb_clients_active` | gauge | 当前活跃（有未完成命令）的客户端连接数 |
| `boltdb_clients_blocked` | gauge | 当前被阻塞（BLPOP 等）的客户端连接数 |
| `boltdb_clients_monitor` | gauge | MONITOR 模式的客户端连接数 |
| `boltdb_clients_pubsub` | gauge | 已订阅 Pub/Sub 频道的客户端连接数 |
| `boltdb_pubsub_subscribers` | gauge | 总的 Pub/Sub 订阅数（一个客户端可订阅多个频道） |
| `boltdb_total_output_bytes` | counter | 向所有客户端发送的累计输出字节数 |

## Prometheus 抓取配置示例

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'boltdb'
    static_configs:
      - targets: ['localhost:6338']
    # 默认每 15 秒抓取一次
    scrape_interval: 15s
```

## 可用看板

### 1. 内置终端 Dashboard

`scripts/soak_dashboard.sh` 提供了一个终端实时看板，直接从 `/debug/vars` 拉取数据：

```bash
bash scripts/soak_dashboard.sh
```

依赖: `curl`, `bc`, `tput`。支持多节点视图。

### 2. Grafana

指标通过标准的 Prometheus exposition format 暴露，可直接在 Grafana 中配置数据源。建议关注的指标面板：

- **L0 Score 时间序列**：`rate(boltdb_l0_score[1m])`，判断 compaction 压力趋势
- **写背压事件**：`rate(boltdb_writes_l0_rejected[5m])` 和 `rate(boltdb_writes_l0_delayed[5m])`
- **复制延迟**：`boltdb_replication_lag`，slave 健康度核心指标
- **重连风暴检测**：`rate(boltdb_reconnect_count[5m])`，陡升表示网络问题或主节点不稳定
- **goroutine 泄漏**：`boltdb_goroutines` 长期增长趋势
- **客户端水位**：`boltdb_clients_active` + `boltdb_clients_blocked`
- **Backlog 水位**：`boltdb_backlog_available / boltdb_backlog_size` 接近 0 时意味着 slave 追赶不上

## 架构

### 采集器 Collector

`internal/metrics/collector.go` — `Collector` 结构体通过函数指针注入各模块的读取方法：

```
main.go 初始化:
  collector := metrics.NewCollector()
  collector.RetryMetricsFn  = db.GetRetryMetrics       // 来自 internal/store
  collector.MasterReplOffsetFn = replMgr.GetMasterReplOffset  // 来自 internal/replication
  collector.ActiveClientsFn = handler.ActiveClientCount       // 来自 internal/server
  ...
```

调用 `collector.Snapshot()` 时：

1. 检查缓存年龄（< 1 秒直接返回上次快照）
2. 否则执行 `refresh()`：在 RLock 保护下并行读取所有函数指针，采集 runtime memstats
3. 计算派生值（如 `ReplicationLag = masterOff - slaveOff`）
4. 缓存快照并返回

### 定期快照

```go
metrics.StartPeriodicSnapshot(ctx, collector, 60*time.Second, &metricsWg)
```

每 60 秒采集一次完整快照并写入 zerolog 日志（INFO 级别），包含 goroutine 数、L0 score、重试次数、复制延迟、客户端数、GC 次数等 14 个字段。

### 端点渲染

`internal/metrics/http.go` — `ServeMetrics()` 启动独立的 HTTP server，注册端点：

- `/metrics` → `prometheusText(s)` (prometheus.go) — 生成标准 Prometheus 文本格式
- `/debug/metrics` → `s.String()` (snapshot.go) — 纯文本可读格式
- `/debug/vars` → `s.JSON()` — JSON 格式
- `/healthz` → 固定 `{"status":"ok"}`

### 关机顺序

```
replMgr.Stop() → cancel() → metricsWg.Wait() → handler.Shutdown() → db.Close()
```

- `metricsWg.Wait()` 在 `handler.Shutdown()` 之前，确保定期快照协程不再访问 handler 数据后 handler 才关闭
- metrics HTTP server 通过 `ctx.Done()` 信号优雅关闭（2 秒超时）

详见 `docs/state-machine.md`。

## 源码索引

| 文件 | 职责 |
|------|------|
| `internal/metrics/collector.go` | 采集器核心，函数指针注入 + 缓存快照 |
| `internal/metrics/snapshot.go` | `Snapshot` 结构体定义，`String()` / `JSON()` 渲染 |
| `internal/metrics/prometheus.go` | Prometheus exposition format 渲染 |
| `internal/metrics/http.go` | HTTP 服务器 + 端点注册 (`AttachHTTP`, `ServeMetrics`) |
| `internal/metrics/periodic.go` | 定期快照日志写入 (`StartPeriodicSnapshot`) |
| `internal/store/backpressure.go` | 写背压指标 (`GetRetryMetrics`, `GetQueryBudgetTrips`) |
| `internal/replication/replicator.go` | 复制偏移 / 从节点计数 |
| `internal/server/handler.go` | 客户端连接统计 |
| `scripts/soak_dashboard.sh` | 终端实时看板 |
| `cmd/boltDB/main.go:353-382` | 采集器初始化和端点启动 |

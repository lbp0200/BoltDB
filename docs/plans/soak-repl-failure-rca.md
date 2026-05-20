# 主从浸泡测试 6h 失败分析

## 测试参数

- Duration: 6h (SOAK_REPL_DURATION=6h)
- Writers: 4 (SOAK_REPL_WRITERS=4)
- 生命周期混沌: 周期性杀死从机 TCP 连接
- 最终验证: SCAN 全量比对主从数据集

## 失败现象

1. **~180 运行时错误** — 主要是 lifecycle check value mismatch + 少量 SPOP i/o timeout
2. **最终 SCAN i/o timeout** — 测试结束时主服务器对 SCAN 命令无响应，无法做最终数据集比对

## 疑似根因

### 1. Backlog 无上限 (大概率)
- `DefaultBacklogSize = 1MB`, `MaxBacklogSize = 512MB`
- 6 小时持续写入，backlog 达到上限后开始覆盖旧数据
- 从机断开后重连时，需要的 offset 可能已被覆盖 → 全量 RDB 同步
- 全量同步期间内存/CPU 飙升 → 服务器响应变慢

### 2. TCP 连接泄漏 (中概率)
- 生命周期测试反复 kill slave connections，没有检查连接是否被正确关闭
- `ServeTCP` 中 goroutine 可能泄漏，fd 被耗尽

### 3. BadgerDB Value Log 堆积 (中概率)
- 6 小时大量写入可能产生大量 value log 文件
- L0 表压缩跟不上写入速度 → 读放大 → 查询超时

### 4. 服务器无 TCP Keepalive / Timeout (低概率)
- 当前没有设置 `SetKeepAlive` 或 `SetReadDeadline`
- 断开连接的 goroutine 可能永远阻塞在 Read 上

## 调查方案

### Step 1: 确认 backlog 行为
```
# 加一个写入器，1h duration，观察 backlog 使用量
SOAK_REPL_DURATION=1h SOAK_REPL_WRITERS=1 go test -race -timeout 2h ./cmd/integration/ -run TestSoakReplication -v
```
- 在日志中监控 `write backlog` 相关错误
- 检查是否有 `full resync requested` 日志

### Step 2: 简化场景复现
去掉 lifecycle chaos，只跑纯写压：
```
SOAK_REPL_DURATION=2h SOAK_REPL_WRITERS=4 go test -race -timeout 3h ./cmd/integration/ -run TestSoakReplication -v
```
- 如果通过 → 问题在 lifecycle 混沌（连接泄漏）
- 如果失败 → 问题在纯写压（backlog / BadgerDB）

### Step 3: BadgerDB 指标
在 `internal/store/define.go` 或 `internal/replication/` 中添加：
- 定时打印 BadgerDB L0 表数量
- 定时打印 value log 文件大小
- 退出前打印文件描述符计数

### Step 4: Goroutine dump
测试中定期调用 `pprof.Lookup("goroutine").WriteTo(os.Stdout, 2)`：
- 检查是否有持续的 goroutine 增长
- 特别关注 `handleConnection` 和 `ServeTCP` 的 goroutine 数

### Step 5: 修复方向（确认根因后）
| 根因 | 修复 |
|------|------|
| Backlog 过小被覆盖 | 增大 `DefaultBacklogSize` 或改为动态增长 |
| TCP 连接泄漏 | 检查 `removeConnection` 是否在所有路径被调用 |
| 无 Keepalive | `conn.SetKeepAlive(true)` + `SetReadDeadline` |
| BadgerDB 压缩跟不上 | 调优 BadgerDB 压缩参数或增加 L0 表上限 |

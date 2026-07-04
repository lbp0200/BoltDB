# Lua Scripting (EVAL/EVALSHA/SCRIPT/FCALL) — 技术分析与设计方案

## 状态

当前：不实现。这是一个有意的设计决策，不是功能遗漏。

本文档记录前因后果和权衡分析，供未来决定实现时直接作为设计起点使用。

---

## 为什么不做？

### 核心矛盾：长事务 x LSM-Tree

Redis 的 Lua 原子性靠阻塞单线程事件循环实现——自然、零成本。
BoltDB 基于 BadgerDB（MVCC + LSM-tree），不能阻塞事件循环，只能把脚本包进 `db.Update(fn)`。

问题是：

```
Lua 脚本包在 db.Update(fn) 中执行
  -> fn 持有 Badger 事务的 readTs
  -> compaction 无法丢弃该 readTs 之下的旧版本
  -> L0 堆积速度 > compaction 处理速度
  -> L0 score 增长（5 -> 10 -> 15 -> 20+）
  -> preWriteCheck 开始延迟写入（L0 > 8）
  -> 最终拒绝写入（L0 > 20）
```

整个写入吞吐量在脚本执行期间被雪崩式压制，所有其他客户端无辜受害。

### 两个备选方案都有致命缺陷

| 方案 | 优点 | 致命缺陷 |
|------|------|----------|
| A: 整个脚本一个 db.Update(fn) | Redis 语义兼容，原子性天然成立 | 长事务阻塞 LSM compaction，打穿背压系统 |
| B: Lua 生成命令列表 -> 最后批量 replay | 事务极短（~3ms），不影响 compaction | Lua 内部的读-改-写模式无法 replay（如 if GET x then SET y） |

没有方案 C 能同时满足：Redis 语义原子性、LSM 写入稳定性、实现简单度。

---

## 如果未来要做——受限脚本环境

不做完全兼容 Redis Lua，而是受限沙箱，明确告知用户这不是 Redis Lua 的平替。

### 硬限制（第一版）

| 限制 | 值 | 理由 |
|------|----|------|
| 最大执行时间 | 100ms | 超过即 SCRIPT KILL，防止 L0 堆积 |
| 最大 redis.call() 次数 | 10,000 | 防止无限循环拖死事务 |
| 最大返回体 | 8MB | 控制网络缓冲区 |
| 单脚本 db.Update 内最大写入 key 数 | 1,000 | 限制 MVCC 事务大小 |
| 禁止的库 | os, io, math.random（无种子） | 确定性沙箱 |

### 实现路径

1. gopher-lua 作为 Lua VM（Go 生态中最成熟的 Lua 5.1 实现）
2. LState.SetContext(ctx) + context.WithTimeout 实现超时控制
3. 注册 redis.call / redis.pcall / redis.log / redis.sha1hex / redis.status_reply / redis.error_reply
4. redis.call 内部操作走同一个 db.Update(fn) 事务（方案 A）
5. 移除 os、io 库；math.random 强制使用固定种子
6. 用 LState.SetRegistry() 钩子监控内存

### 可选：脚本预热缓存

SCRIPT LOAD "" -> 返回 SHA -> EVALSHA 从内存缓存执行，不走 BadgerDB

与 PropagateCommand 集成：传播 EVAL <script> <args> 到从节点。

---

## 验证标准

如果将来决定实现，以下 soak 测试必须通过才能合入主分支：

Soak 名称：LuaLongTxImpact

流量：
- 每秒 10 个短 Lua 脚本（5-20 次 redis.call，< 5ms）
- 混合 50% 普通 SET/GET
- 每分钟插入 1 个极限脚本（~10000 次 HSET，故意逼近限制）

验收指标：
- P99 写入延迟 <= 基线 + 20%
- L0 score 均值 <= 12
- 背压拒绝率 <= 0.1%
- 脚本执行后 L0 在 60s 内恢复

---

## 参考

- 相关代码入口：internal/server/handler_dispatch.go 的 default 分支（返回 unknown command）
- BadgerDB 事务机制：github.com/dgraph-io/badger/v4
- gopher-lua：github.com/yuin/gopher-lua
- Redis EVAL 文档：https://redis.io/commands/eval/
- 本项目的背压系统：internal/store/backpressure.go、internal/store/set.go:retryUpdate

## 历史讨论

- 2026-07-04：首次技术分析，确认不实现，记录本文档

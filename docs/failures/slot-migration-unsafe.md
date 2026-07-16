# Slot Migration Unsafe — 已知缺陷与修复计划

> ⚠️ `CLUSTER MIGRATESLOT` 当前为**预览特性**，**不应用于生产环境**。
> 完整修复（两阶段提交 + WAL）预计需要 2-3 周开发。
> 参见 [TODO.md#问题-2-cluster-slot-迁移已实现但不可靠](../plans/TODO.md#-cluster-slot-迁移已实现但不可靠)

---

## 六项致命缺陷

| # | 缺陷 | 位置 | 后果 |
|---|------|------|------|
| 1 | **无事务边界** | `cluster.go:551-620` | 每个 key 独立操作，迁移中断后无法保持原子性 |
| 2 | **无重做/回滚日志** | 全程 | 任何步骤崩溃，无法恢复中间状态 |
| 3 | **连接中断 → 半完成迁移** | `cluster.go:569-618` | 部分 key 已迁移/部分未迁移，`AssignSlot` 仍执行 |
| 4 | **源端 DEL 不回滚** | `cluster.go:623-631` | TCP RESTORE 成功后目标可能未提交，但源端已 DEL → 数据丢失 |
| 5 | **全量扫描** | `cluster.go:553-558` | `IterateRawKeys` 全 DB 扫描，千万级 key 时阻塞所有读取 |
| 6 | **无并发保护** | 全程 | 迁移期间客户端读写冲突（MOVED vs ASKING 语义不协调） |

### 附加：Phase-1 RESTORE 与 ASKING 写竞态（2026-07-16 / 续）

**已落地缓解**：

1. Phase 1 `RESTORE` **不带 REPLACE**；目标 key 已存在则跳过覆盖。
2. **IMPORTING write fence**：`ASKING` + 写命令（除 `RESTORE`）在 IMPORTING
   slot 上返回错误，防止客户端与迁移 RESTORE 竞态。
3. **MIGRATING source write fence**：源端 MIGRATING 时对**已存在 key 的写**也拒绝，
   避免 Phase-1 已拷贝后客户端再改源端、Phase-2 删除却未再拷贝导致丢数。
4. **Phase-1 abort 清理**：迁移失败或 crash 恢复（INIT/COPYING）时，源端向目标
   发送 `CLUSTER DELKEYSINSLOT` + `SETSLOT STABLE`，清理部分 RESTORE 孤儿。
5. 迁移开始时源端主动对目标 `SETSLOT IMPORTING`。

**仍开放 / 预览限制**：

- 无完整 slot 级 write fence 覆盖所有边角（如 MULTI 队列内写、非 ASKING 路径）
- 最终 catch-up 扫描与 commit 原子性仍弱于 Redis
- `CLUSTER MIGRATESLOT` 仍建议仅用于实验/预览，生产前再做 soak

**生产建议**：优先静态 ADDSLOTS / 离线迁移，直至 MIGRATESLOT 通过多节点 soak。

---

## 当前实现（`Cluster.MigrateSlot`）

```go
// cluster.go:521-659
for _, key := range keys {
    DUMP key → TCP发送 → 目标RESTORE → 本地DEL
}
AssignSlot()  // 最后才切换槽归属
```

- 没有预写日志（WAL）
- 没有两阶段提交
- 没有回滚机制
- 没有并发写入保护

---

## 预期修复

详见 [TODO.md 问题 2 阶段 2](../plans/TODO.md#阶段-22-3-周完整修复两阶段提交--wal)。

**修复方案**：两阶段提交 + WAL

```
Phase 1 — COPY（逐 key 复制，不删除源端）：
   迁移日志记录每个 key 的复制状态
   全部完成后标记 COPIED

Phase 2 — COMMIT（原子切换 slot + 批量删除）：
   BadgerDB 事务批量删除源端 key
   AssignSlot(slot, targetNodeID)
   标记 DONE

恢复（crash 后）:
   COPIED 但未 DONE → 重新 Phase 2
   COPYING 中 → 跳过已复制的 key 继续
```

---

## 临时替代方案

在生产环境需要调整 slot 时，目前只能：

1. **集群初始化时静态分配**：启动前规划好 slot → node 映射，使用 `CLUSTER ADDSLOTS`
2. **重启调整**：停止集群 → 更新节点配置 → 重启
3. **数据迁移脚本**：在客户端手动 dump/restore 数据（外部脚本，不依赖 BoltDB 内部迁移）

# ZRANK 内存缓存设计文档

## 1. 问题定义

### 1.1 现状

`ZRANK key member` 返回 member 在 sorted set 中的排名（0-based score 升序）。

当前实现（`internal/store/zrank.go`）使用 BadgerDB 迭代器线性扫描 sorted set 的 index 前缀：

```
prefix := zset:key:index:<encoded_score>:<member>:<version>
```

遍历全部 member 直到找到目标，最坏情况 O(n)。mid-point 优化（正向扫描到中值后切反向）仅将常数减半，不改 O(n)。

### 1.2 影响

| 场景 | zset 大小 | 当前延迟 (估计) | Redis 延迟 |
|------|-----------|-----------------|------------|
| 排行榜 top 10 | 1000 | 20-50 µs | ~1 µs |
| 大 zset 排名查询 | 100K | 2-5 ms | ~1 µs |
| 大 zset 频繁 ZRANK | 1M | 20-50 ms | ~2 µs |
| 批量 ZRANK (M 次) | 1M | O(M × N) | O(M × log N) |

### 1.3 使用率假设

**尚无生产数据。** 这是关键决策前提。

- 如果 ZRANK 占总命令的 < 1%，当前 O(n) 可能可接受
- 如果 ZRANK > 10%，缓存是必要的

---

## 2. 候选方案

### 方案 A：内存 B-tree（推荐）

在每个 `BotreonStore` 中维护一个全局 map：`map[string]*btree.BTree`，key=zset name，value=以 (score, member) 排序的 B-tree。

**数据结构选型：**

| 属性 | B-tree (google/btree) | 跳表 (手写) | 排序切片 + 二分 |
|------|----------------------|-------------|----------------|
| 插入 | O(log n) | O(log n) | O(n) |
| 删除 | O(log n) | O(log n) | O(n) |
| ZRANK | O(log n) | O(log n) | O(log n) |
| 内存开销 | ~48 bytes/node | ~64 bytes/node | ~24 bytes/elem + 移位开销 |
| 依赖 | 需新增 | 无 | 无 |
| 并发安全 | 需自行加锁 | 需自行加锁 | 需自行加锁 |

B-tree 是最适合 sorted set 的数据结构（有序、范围查询、Rank 计算均 O(log n)）。`google/btree` 是成熟库。

**核心类型：**

```go
type zsetCacheItem struct {
    Member string
    Score  float64
}

// Less implements btree.Item
func (a zsetCacheItem) Less(b btree.Item) bool {
    other := b.(zsetCacheItem)
    if a.Score != other.Score {
        return a.Score < other.Score
    }
    return a.Member < other.Member
}

type zsetCache struct {
    mu    sync.RWMutex
    trees map[string]*btree.BTreeG[zsetCacheItem]
}
```

**ZRANK 实现：**

```go
func (c *zsetCache) ZRank(key, member string, score float64) (int64, bool) {
    c.mu.RLock()
    defer c.mu.RUnlock()
    tree, ok := c.trees[key]
    if !ok {
        return 0, false
    }
    target := zsetCacheItem{Member: member, Score: score}
    rank := 0
    tree.Ascend(func(item zsetCacheItem) bool {
        if item.Member == member && item.Score == score {
            return false // stop
        }
        rank++
        return true
    })
    return int64(rank), true
}
```

> **注意：** `btree.BTreeG` 是泛型版本，`Ascend` 是 O(n) 遍历。要实现 O(log n) ZRANK 需要利用 B-tree 的 `Get` + 计数。更优方式：使用 `btree.BTree` 的 `Get` 判断存在性，然后利用 B-tree 的顺序统计特性（需要扩展或使用支持 rank 的 btree 变体）。

**更优替代：** 直接使用 `google/btree` 的 `Ascend` + 提前终止 = O(k) 其中 k 是 rank 值（最坏仍 O(n)）。要真正 O(log n) ZRANK，需要 B-tree 节点存储子树大小（顺序统计树）。这需要自定义实现或另一个库。

### 方案 B：顺序统计树（Order Statistic Tree，最优）

在标准 B-tree 每个节点中额外存储 **子树大小**（subtree node count）。这样：

| 操作 | 复杂度 | 实现 |
|------|--------|------|
| Insert | O(log n) | 插入时自底向上更新子树大小 |
| Delete | O(log n) | 删除时自底向上更新子树大小 |
| ZRANK (by member+score) | O(log n) | 从根到叶子遍历，累加左子树大小 |
| ZRANGE by rank | O(log n + k) | 根据 rank 向下搜索 |

**代价：** 自定义实现 ~500 行，不需要外部依赖。每个节点多 4 字节存储 subtree size。

这是最佳方案但需要从零实现。

### 方案 C：只加 ZRANK 专用索引（折中）

不重建全局缓存，只为 ZRANK 操作构建一个从 member→rank 的映射：

```go
type rankCache struct {
    mu       sync.RWMutex
    rankMap  map[string]map[string]int64 // key → member → rank
    dirty    map[string]bool             // keys needing rebuild
}
```

- **读取时懒重建**：ZRANK 时如果 key 标记为 dirty，全量扫描 BadgerDB 重建 rankMap
- **写时标记 dirty**：ZADD/ZREM/ZINCRBY 时标记 key 为 dirty
- **优点**：不需要额外依赖，不需要维护一致性
- **缺点**：写密集场景下标记 dirty 后下一次 ZRANK 仍 O(n)；读多写少场景可用

---

## 3. 写路径影响分析

所有修改 sorted set 的操作必须同步维护缓存：

| 写操作 | 文件 | 变更复杂度 |
|--------|------|-----------|
| ZAdd | `zadd_zrem.go` | 新增/更新 member，需要删除旧 score 条目、插入新条目 |
| ZRem | `zadd_zrem.go` | 删除 member |
| ZIncrBy | `zcard_score.go` | 同 ZAdd（本质是 ZAdd with delta） |
| ZPopMax / ZPopMin | `zpop.go` | 移除最值 member |
| ZRemRangeByRank | `zadd_zrem.go` | 按 rank 范围批量删除 |
| ZRemRangeByScore | `zadd_zrem.go` | 按 score 范围批量删除 |
| ZRemRangeByLex | `zlex.go` | 按 lex 范围批量删除 |
| ZInterStore / ZUnionStore / ZDiffStore | `zinter_store.go` | 创建新 zset，缓存需重建 |
| ZRangeStore | server handler | 创建新 zset，缓存需重建 |
| GEOADD | `geospatial.go` | 底层使用 zset，需要同步 |
| GEORem | `geospatial.go` | 底层使用 zset |
| rename/delete 涉及 zset | `rename.go` / `del.go` | 脏标记或重命名缓存 key |
| FLUSHDB/FLUSHALL | 各 handler | 清空全部缓存 |

**总计：约 15 个写路径需要修改。**

---

## 4. 锁粒度

| 方案 | 粒度 | 争用 | 复杂度 |
|------|------|------|--------|
| 全局 RWMutex | 所有 zset 共用 | 高（并发 ZRANK 不同 key 互相阻塞） | 低 |
| 每 zset RWMutex | 每个 key 独立 | 低 | 中 |
| 无锁 (sync.Map) | key→tree 映射 | 低（读多写少） | 中（但 btree 操作仍需锁） |

**推荐：** `map[string]*btree.BTree` + 每 zset `sync.RWMutex` + 全局 `sync.RWMutex` 仅保护 map 的增删 key。

```go
type zsetCache struct {
    globalMu sync.RWMutex
    trees    map[string]*zsetCacheTree
}

type zsetCacheTree struct {
    mu   sync.RWMutex
    tree *btree.BTreeG[zsetCacheItem]
}
```

---

## 5. 一致性策略

### 5.1 写路径

所有写操作通过 `BotreonStore.retryUpdate()` 进行 BadgerDB 事务提交。缓存同步方式：

**方案 A：写通（Write-Through）—— 推荐**

```
retryUpdate(txn → {
    // 1. BadgerDB 写入
    txn.Set(dataKey, ...)
    txn.Set(indexKey, ...)
    txn.Set(metaKey, ...)
    // 2. 不在此提交
})
// 3. BadgerDB 提交成功后再更新内存缓存
cache.Update(...)
```

优点：DB 始终是权威源，缓存损坏可重建。
缺点：无法原子化 DB + 缓存，crash 后缓存落后于 DB。

**方案 B：双写事务内（不可行）**
BadgerDB 不支持跨存储的事务。无法在同一个事务中原子更新 Badger + 内存。

**方案 C：版本校验**

在缓存中存储 version，ZRANK 时检查 BadgerDB meta version：
```go
func (s *BotreonStore) ZRank(key, member string) (int64, error) {
    dbVersion := s.readMetaVersion(key)
    cacheVersion := cache.GetVersion(key)
    if dbVersion != cacheVersion {
        cache.Rebuild(key) // or stale read
    }
    return cache.GetRank(key, member)
}
```

优点：crash 后自动恢复，不需要启动重建。
缺点：每次 ZRANK 多一次 BadgerDB 读 meta（约 5-20 µs），抵消部分缓存收益。

**推荐方案 A + 启动重建：**

```
启动时：
  遍历 BadgerDB 所有 zset meta key
  对每个非空 zset，全量扫描 index 构建 B-tree

运行时：
  每次写操作成功后同步更新 B-tree (write-through)
  
crash 后：
  重启时重建（与正常启动一致）
```

### 5.2 启动重建成本

| zset 大小 | 重建时间（估计） |
|-----------|----------------|
| 1000 | ~0.5 ms |
| 10K | ~5 ms |
| 100K | ~50 ms |
| 1M | ~500 ms |

重建是单 goroutine 顺序遍历。如果 DB 中有 1000 个平均 1K 的 zset，重建约 0.5 秒。可以在启动时异步完成，不阻塞 accepting connections。

---

## 6. 内存成本

| 条目 | 每 member 开销 (bytes) | 说明 |
|------|----------------------|------|
| btree.Item (struct) | 32 | Member string header(16) + Score float64(8) + padding |
| string 数据 | len(member) | 与 BadgerDB 共享，不额外复制 |
| btree node (degree=32) | ~256 | 每节点存储 31-63 个 item |
| map entry overhead | ~50 | Go map 的 per-entry 开销 |
| **总计** | **~90 + len(member)** | |

**全量缓存成本估算（极简）：**

| zset 配置 | 条目数 | 内存 |
|-----------|--------|------|
| 100 个 zset × 1K member | 100K | ~10 MB |
| 1000 个 zset × 10K member | 10M | ~1 GB |
| 1 个大 zset × 1M member | 1M | ~100 MB |

**结论：** 对于大多数生产场景（总条目 < 10M），内存成本在可接受范围内（< 1 GB）。

---

## 7. 风险与缓解

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|---------|
| 缓存与 DB 不一致（crash） | 中 | 低 | 重启重建；RDB snapshot 后重建 |
| B-tree 并发 bug | 低 | 中 | 代码审查 + race test |
| 内存泄漏（zset 删了但缓存未删） | 中 | 低 | DEL 路径验证；启动重建可修复泄漏 |
| 写路径遗漏（某个写操作未更新缓存） | 中 | 中 | 对称性测试（类似本次 replication 测试） |
| ZRANK 不常用，ROI 低 | 高 | — | **先埋点统计使用率，再决定是否实施** |

---

## 8. 实施计划

### Phase 0（前置，1-2 天）
- [ ] 在内部 metrics 中添加 ZRANK/ZREVRANK/ZRANGE by rank 命令计数
- [ ] 在 soak 测试或 staging 环境采集使用率数据
- [ ] 决策：如果 ZRANK + ZRANGE by rank 总计 < 5%，不实施

### Phase 1（核心实现，1-2 周）
- [ ] 选择方案 A（B-tree 缓存）或方案 B（顺序统计树），或方案 C（懒重建）
- [ ] 实现 `zsetCache` 类型（含 lock、rebuild from DB、update）
- [ ] 修改所有写路径（~15 个），在 DB 写入成功后同步缓存
- [ ] 修改读路径（ZRank、ZRevRank、ZRange 0/-1 等）使用缓存
- [ ] 启动时重建缓存

### Phase 2（验证，3-5 天）
- [ ] 写路径同步正确性测试（随机操作 → 验证 ZRANK 结果与 DB 一致）
- [ ] 并发安全测试（race test）
- [ ] Crash 恢复测试
- [ ] Benchmark：ZRANK 10K/100K/1M 条目延迟对比

---

## 9. 决策建议

**不做（目前）。** 理由：

1. **无使用率数据。** 投入 1-2 周前应先确认这个优化有用户
2. **15 个写路径需要修改。** 每个都是潜在的 bug 来源
3. **启动重建 + 运行时一致性** 是额外复杂度
4. **替代方案**：如果 ZRANK 使用率低，O(n) 扫描在 10K 条目下只需 0.2-2ms——对大多数应用可接受

**替代方案（推荐立即执行）：**
- 在 metrics 中加入 ZRANK/ZREVRANK 调用计数（1 天）
- 运行 soak + 生产负载后决策是否做缓存
- 如果决定不做，关闭 TODO 中的 ZRANK 任务，标记为 "wontfix（待数据）"

**如果决定做，推荐方案 A（google/btree）+ write-through + 启动重建。**

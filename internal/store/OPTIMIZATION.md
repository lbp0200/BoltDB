# BoltDB 性能优化文档

本文档描述了 BoltDB 的性能优化措施，包括 BadgerDB 配置优化、缓存层实现和数据结构优化。

## 1. BadgerDB 配置优化

### 1.1 Memtable 配置
- **NumMemtables**: 5（默认值）
  - 增加 memtable 数量可以提高写入并发性能
  - 建议值：5-10（根据内存情况调整）

- **NumLevelZeroTables**: 5
  - Level 0 表数量，影响写入性能

- **NumLevelZeroTablesStall**: 10
  - Level 0 停滞阈值，超过此值会暂停写入

### 1.2 Value Log 配置
- **ValueLogFileSize**: 1GB
  - 值日志文件大小，增大可以减少文件数量，降低合并频率

- **ValueLogMaxEntries**: 1,000,000
  - 每个 vlog 文件最大条目数

### 1.3 Table 配置
- **BlockSize**: 4KB
  - 块大小，影响读取性能

- **LevelSizeMultiplier**: 10
  - Level 大小倍数，控制层级增长

### 1.4 压缩配置
- **Compression**: ZSTD (2)
  - 使用 ZSTD 压缩算法，比 Snappy 压缩率更高
  - 值：0=无压缩, 1=Snappy, 2=ZSTD

### 1.5 索引/块缓存
- **IndexCacheSize**: 100MB
  - 索引缓存大小，提高读取性能
  - 默认 0（禁用），启用后可以显著提升读取性能
- **BlockCacheSize**: 128MB
  - 数据块缓存大小（badger 默认 256MB，显式调小以降低每节点固定内存占用）
  - 读密集场景可调大，配合 GOMEMLIMIT 7 层 OOM 防护

### 1.6 垃圾回收
- **NumGoroutines**: 8
  - GC goroutine 数量，影响垃圾回收性能

## 2. 缓存层实现

### 2.1 LRU 缓存
实现了基于 LRU（Least Recently Used）算法的缓存层，包括：

- **读缓存（Read Cache）**
  - 容量：10,000 个条目
  - TTL：5 分钟
  - 用途：缓存频繁读取的数据，减少磁盘 I/O

- **写缓存（Write Cache）**
  - 容量：5,000 个条目
  - TTL：1 分钟
  - 用途：缓存写入操作，支持批量写入优化

### 2.2 缓存特性
- **线程安全**：使用 `sync.RWMutex` 保护并发访问
- **自动过期**：支持 TTL，过期项自动删除
- **LRU 淘汰**：缓存满时自动淘汰最久未使用的项
- **零拷贝**：缓存直接存储字节数组，避免序列化开销

### 2.3 缓存使用场景
- **GET 操作**：先检查读缓存，未命中时从 BadgerDB 读取并更新缓存
- **SET 操作**：同时更新读缓存和写缓存，提高后续读取性能

## 3. 数据结构优化

### 3.1 键命名优化
- 使用前缀分离不同类型的数据（STRING、HASH、SET、LIST、SORTEDSET）
- 使用冒号分隔符，便于前缀扫描

### 3.2 压缩优化
- 支持 LZ4 和 ZSTD 压缩算法
- 小数据（< 100 字节）不压缩，避免压缩开销
- 压缩数据使用魔数标识，支持自动解压

### 3.3 事务优化
- 实现重试机制处理事务冲突
- 指数退避 + 随机抖动，避免雷群效应
- 最多重试 30 次，最大退避时间 50ms

## 4. 性能指标

### 4.1 预期性能提升
- **读取性能**：缓存命中时，延迟降低 90%+（从 ~1ms 降至 ~0.1ms）
- **写入性能**：通过 BadgerDB 配置优化，吞吐量提升 20-30%
- **并发性能**：通过增加 memtable 和重试机制，支持更高并发

### 4.2 内存使用
- **读缓存**：约 10,000 条目 × 平均键值大小
- **写缓存**：约 5,000 条目 × 平均键值大小
- **索引缓存**：100MB（BadgerDB 内部使用）

## 5. 配置建议

### 5.1 内存充足场景
```go
// 读缓存已移除（2026-07-02），BadgerDB 索引缓存（IndexCacheSize）已设为 100MB
// 建议增加 BadgerDB 内存表数量
opts.NumMemtables = 10
opts.IndexCacheSize = 500 * 1024 * 1024 // 500MB
```

### 5.2 内存受限场景
```go
// 读缓存已移除（2026-07-02），BadgerDB 索引缓存（IndexCacheSize）已设为 100MB
// 建议减少 BadgerDB 内存表数量
opts.NumMemtables = 3
opts.IndexCacheSize = 50 * 1024 * 1024 // 50MB
```

### 5.3 高并发写入场景
```go
// 增加 memtable 和 Level 0 表数量
opts.NumMemtables = 10
opts.NumLevelZeroTables = 10
opts.NumLevelZeroTablesStall = 20
```

## 6. 监控和调优

### 6.1 缓存命中率
可以通过日志或指标监控缓存命中率：
- 读缓存命中率 = 缓存命中次数 / 总读取次数
- 目标：> 80%

### 6.2 BadgerDB 指标
- 监控事务冲突率
- 监控 GC 频率和耗时
- 监控 Level 0 表数量

### 6.3 调优建议
1. **缓存命中率低**：增加缓存容量或 TTL
2. **事务冲突率高**：增加重试次数或调整 BadgerDB 配置
3. **内存使用高**：减少缓存容量或 memtable 数量
4. **写入延迟高**：增加 memtable 数量或 Level 0 表数量

## 7. 未来优化方向

1. **分布式缓存**：使用 Redis 或 Memcached 作为分布式缓存层
2. **预取机制**：实现智能预取，提前加载热点数据
3. **批量操作优化**：实现批量写入和读取，减少事务开销
4. **压缩算法优化**：根据数据特征选择最佳压缩算法
5. **索引优化**：为常用查询模式创建专用索引


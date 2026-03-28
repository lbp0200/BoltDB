# Test Depth Improvement Design

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan.

**Goal:** 系统性提升测试深度，覆盖边界条件、错误处理、并发安全三类场景，减少 bug 漏过。

**Strategy:** Incremental by module — add boundary, error, and concurrent tests module-by-module, building on the work already started in March 28 session.

---

## Background

现有测试主要覆盖 happy path，存在以下问题：
- 边界条件未充分测试（空值、极限值、临界长度）
- 错误处理路径未充分验证（类型错误、参数错误、状态错误）
- 并发安全场景缺失（多 client 竞争、事务冲突）

### Gap Analysis Summary (2026-03-28)

**Commands with no integration test coverage:**
- String: DECR, DECRBY, GETRANGE, SETBIT, GETBIT, SETRANGE, SETEX, PSETEX
- Key: PERSIST, PEXPIRE, PEXPIREAT, PTTL, TOUCH
- List: LINSERT, LPOS, LMOVE, LPUSHX, RPUSHX
- Hash: HMSET, HINCRBY, HINCRBYFLOAT
- Set: SISMEMBER, SPOP, SRANDMEMBER, SMOVE, SDIFF, SDIFFSTORE, SSCAN
- SortedSet: ZMSCORE, ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE
- Server: FLUSHALL, SAVE
- Cluster: ADDSLOTS, SETSLOT, FORGET, REPLICATE (only basic redirect tested)

**Error scenarios not tested:**
- WRONGTYPE errors (string cmd on hash key, etc.)
- Invalid argument types (INCRBY on non-integer, ZADD with NaN)
- Index out of bounds (LSET, LINDEX, LRANGE)
- Non-existent key operations
- Transaction error paths (EXEC outside MULTI, etc.)

**Reliability:** Excellent — no flaky tests detected
**Speed:** Good — full suite in ~40s

---

## Three Test Categories

### 1. 边界条件测试 (Boundary)

测试数据结构和命令在极限情况下的行为：
- 空值：`GET` 不存在的 key、`LPOP` 空 list、`SMEMBERS` 空 set
- 极限值：极大 value、超长 key、极大集合（超过内存限制）
- 临界长度：最大 key 长度、value 长度、集合元素数量
- 溢出边界：整数递增到最大值、列表索引越界

**命名约定：** `Test<Type>Boundary_<Scenario>`

### 2. 错误处理测试 (Error)

测试命令在错误参数或错误状态下的行为：
- 类型错误：`INCR` hash key、`LPUSH` string key
- 参数错误：缺少必需参数、参数数量过多、参数类型错误
- 状态错误：已删除的 key、已关闭的连接、过期资源

**命名约定：** `Test<Type>Error_<Scenario>`

### 3. 并发安全测试 (Concurrent)

测试多个 client 同时操作时的数据一致性和正确性：
- 读写冲突：client A 读同一 key 时 client B 写入
- 写写冲突：多个 client 同时写同一 key
- 边界竞态：key 过期/删除的瞬间访问
- 事务并发：WATCH 下的并发修改

**命名约定：** `Test<Type>Concurrent_<Scenario>`

---

## Implementation Phases

| Phase | Module | Untested Commands | Priority |
|-------|--------|-------------------|----------|
| 1 | String | DECR, DECRBY, GETRANGE, SETBIT, GETBIT, SETRANGE, SETEX, PSETEX | High |
| 2 | Key | PERSIST, PEXPIRE, PEXPIREAT, PTTL, TOUCH, FLUSHALL, SAVE | High |
| 3 | List | LINSERT, LPOS, LMOVE, LPUSHX, RPUSHX | Medium |
| 4 | Hash | HMSET, HINCRBY, HINCRBYFLOAT | Medium |
| 5 | Set | SISMEMBER, SPOP, SRANDMEMBER, SMOVE, SDIFF, SDIFFSTORE, SSCAN | Medium |
| 6 | SortedSet | ZMSCORE, ZUNIONSTORE, ZINTERSTORE, ZDIFFSTORE | Medium |
| 7 | Cluster | ADDSLOTS, SETSLOT, FORGET, REPLICATE edge cases | Low |
| 8 | Replication | PSYNC edge cases, full sync edge cases | Low |

**Error path coverage per phase:**
- WRONGTYPE errors for each data type
- Invalid argument type errors
- Index out of bounds
- Non-existent key edge cases

---

## Test File Structure

```
cmd/integration/depth_test.go
├── Concurrent tests (already started)
│   ├── TestStringConcurrent_ReadWriteConflict
│   ├── TestStringConcurrent_ConcurrentIncrement
│   ├── TestStringConcurrent_AppendRace
│   ├── TestListConcurrent_PushPopRace
│   ├── TestListConcurrent_MultipleBlockingPops
│   └── TestHashConcurrent_HgetHsetRace
└── [Boundary + Error tests to be added per phase]

internal/server/handler_*_test.go
├── Unit-level boundary tests
└── Unit-level error tests
```

---

## Commit Strategy

每完成一个 phase commit 一次，格式：
```
test: add depth tests for <module> (boundary + error + concurrent)
```

---

## Success Criteria

1. 边界条件测试覆盖所有数据类型的临界场景
2. 错误处理测试覆盖所有命令的类型/参数错误路径
3. 并发测试覆盖高频竞态场景
4. 无新增 flaky test（使用适当同步机制）
5. 测试执行时间不超过现有时间的 2x（≤ 80s）
6. 所有 40+  untested commands get integration test coverage

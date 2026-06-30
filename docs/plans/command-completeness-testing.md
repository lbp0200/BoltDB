# 全命令准确性测试方案

## 目标

在**单机**和**主从**两种模式下，系统性测试 BoltDB 全部 239 个命令的准确性，并支持在 2.16 服务器上后台运行。

## 现状分析

### 已有测试覆盖

| 测试类型 | 文件 | 覆盖范围 | 局限 |
|---------|------|---------|------|
| Go 集成测试 | `cmd/integration/*.go` (35 文件, 200+ 测试) | 大部分命令有测试 | ~34 个命令缺少 Go 集成测试 |
| redis-py 兼容 | `scripts/redis_py_compat.py` | 250+ 检查点，覆盖大部分命令 | 外部客户端视角，非 Go 内部验证 |
| node-redis 兼容 | `scripts/redis_node_compat.mjs` | 110+ 检查点 | 同上 |
| redis-cli 兼容 | `scripts/redis_cli_compat.sh` | 77 检查点 | 同上 |
| 主从传播测试 | `replication_full_test.go` | ~20 个写命令的传播 | 大量写命令未测试传播 |

### 缺口分析

**单机模式缺口**（~34 个命令缺少 Go 集成测试）：
HELLO, GETEX, MSETNX, BITFIELD, BITLEN, TOUCH, SMOVE, SINTER, SUNION, SDIFF, SUNIONSTORE, SMISMEMBER, SSCAN, LINSERT, LPOS, LMOVE, LMPOP, HRANDFIELD, PFINFO, GEORADIUS, GEOSEARCH, GEOSEARCHSTORE, XREVRANGE, XTRIM, ASKING, SAVE, MIGRATE, ZMPOP, TS.MGET, ZDIFFSTORE 等

**主从模式缺口**（写命令未测试传播）：
JSON.*, TS.DEL/INFO, PFADD/PFCOUNT, GEO*, BITOP, BITFIELD, SINTER/SUNION/SDIFF (STORE 变体), HSETNX, HMSET, ZUNIONSTORE, ZINTERSTORE, SINTERSTORE, SDIFFSTORE, SUNIONSTORE, LINSERT, LPOS, LMOVE, XREVRANGE, XTRIM, GEOSEARCH, GEOSEARCHSTORE 等

---

## 方案设计

### Phase 1: 单机全命令准确性测试

**新文件**: `cmd/integration/command_completeness_test.go`

核心思路：**表驱动 + 按数据类型分组**，每个命令一个测试函数，覆盖：
- 基本读写正确性（写入 → 读取 → 验证返回值）
- 边界情况（空值、不存在的 key、错误类型）
- 返回类型正确性（Integer vs String vs Array vs Nil）

测试函数列表：

| 测试函数 | 覆盖命令 | 预计命令数 |
|---------|---------|-----------|
| `TestCommandCompleteness_String` | SET/GET/GETEX/GETDEL/SETNX/MSETNX/INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT/APPEND/STRLEN/SETEX/PSETEX/GETSET/MGET/MSET/GETRANGE/SETRANGE/SETBIT/GETBIT/BITCOUNT/BITOP/BITFIELD/BITFIELD_RO/BITPOS | ~27 |
| `TestCommandCompleteness_Key` | DEL/UNLINK/EXISTS/TYPE/KEYS/RANDOMKEY/RENAME/RENAMENX/COPY/SWAPDB/TOUCH/SORT/DUMP/RESTORE/OBJECT*/SCAN/MOVE/EXPIRE/EXPIREAT/PEXPIRE/PEXPIREAT/TTL/PTTL/EXPIRETIME/PEXPIRETIME/PERSIST/DBSIZE/SELECT/FLUSHDB/FLUSHALL | ~30 |
| `TestCommandCompleteness_List` | LPUSH/RPUSH/LPOP/RPOP/LLEN/LINDEX/LRANGE/LSET/LTRIM/LINSERT/LPOS/LREM/LPUSHX/RPUSHX/LMOVE/BLMOVE/BLPOP/BRPOP/BRPOPLPUSH/LCS | ~19 |
| `TestCommandCompleteness_Hash` | HSET/HGET/HDEL/HLEN/HGETALL/HEXISTS/HKEYS/HVALS/HMSET/HMGET/HSETNX/HINCRBY/HINCRBYFLOAT/HSTRLEN/HRANDFIELD/HSCAN | ~16 |
| `TestCommandCompleteness_Set` | SADD/SREM/SCARD/SISMEMBER/SMEMBERS/SPOP/SRANDMEMBER/SMOVE/SINTER/SUNION/SDIFF/SINTERSTORE/SUNIONSTORE/SDIFFSTORE/SMISMEMBER/SINTERCARD/SSCAN | ~16 |
| `TestCommandCompleteness_SortedSet` | ZADD/ZREM/ZCARD/ZSCORE/ZMSCORE/ZRANGE/ZREVRANGE/ZRANGEBYSCORE/ZREVRANGEBYSCORE/ZRANK/ZREVRANK/ZCOUNT/ZINCRBY/ZREMRANGEBYRANK/ZREMRANGEBYSCORE/ZPOPMAX/ZPOPMIN/BZPOPMAX/BZPOPMIN/ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE/ZDIFF/ZINTER/ZUNION/ZINTERCARD/ZLEXCOUNT/ZRANGEBYLEX/ZREVRANGEBYLEX/ZREMRANGEBYLEX/ZSCAN/ZRANGESTORE/ZRANDMEMBER/ZMPOP/BZMPOP | ~34 |
| `TestCommandCompleteness_HLL` | PFADD/PFCOUNT/PFMERGE/PFINFO | ~4 |
| `TestCommandCompleteness_Geo` | GEOADD/GEOPOS/GEOHASH/GEODIST/GEOSEARCH/GEOSEARCHSTORE | ~6 |
| `TestCommandCompleteness_Stream` | XADD/XLEN/XREAD/XRANGE/XREVRANGE/XDEL/XACK/XSETID/XGROUP*/XREADGROUP/XCLAIM/XAUTOCLAIM/XPENDING/XINFO*/XTRIM | ~24 |
| `TestCommandCompleteness_JSON` | JSON.SET/GET/DEL/TYPE/MGET/ARRAPPEND/ARRLEN/OBJKEYS/NUMINCRBY/NUMMULTBY/CLEAR/DEBUG | ~12 |
| `TestCommandCompleteness_TimeSeries` | TS.CREATE/TS.ADD/TS.GET/TS.RANGE/TS.DEL/TS.INFO/TS.LEN/TS.MGET/TS.REVRANGE/TS.MRANGE/TS.MREVRANGE/TS.QUERYINDEX/TS.MADD/TS.INCRBY/TS.CREATERULE/TS.DELETERULE | ~16 |
| `TestCommandCompleteness_Transaction` | MULTI/EXEC/DISCARD/WATCH/UNWATCH | ~5 |
| `TestCommandCompleteness_PubSub` | PUBLISH/SUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE/PUNSUBSCRIBE/PUBSUB CHANNELS/NUMSUB/NUMPAT | ~7 |
| `TestCommandCompleteness_Connection` | PING/ECHO/AUTH/HELLO/COMMAND/CLIENT*/ACL/MONITOR | ~11 |
| `TestCommandCompleteness_Server` | INFO/SAVE/BGSAVE/LASTSAVE/TIME/CONFIG*/SLOWLOG*/MEMORY*/LATENCY*/DEBUG | ~16 |

### Phase 2: 主从全写命令传播测试

**新文件**: `cmd/integration/replication_completeness_test.go`

核心思路：启动 master + slave，对每个写命令：
1. 在 master 执行写操作
2. 等待 200ms 传播延迟
3. 在 slave 读取验证数据一致性

测试函数列表：

| 测试函数 | 覆盖写命令 |
|---------|-----------|
| `TestReplicationCompleteness_String` | SET/SETNX/SETEX/PSETEX/GETSET/INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT/APPEND/SETBIT/SETREPLACE/MSET/MSETNX |
| `TestReplicationCompleteness_Key` | DEL/UNLINK/RENAME/RENAMENX/COPY/EXPIRE/EXPIREAT/PEXPIRE/PEXPIREAT/PERSIST/RESTORE/TOUCH/MOVE |
| `TestReplicationCompleteness_List` | LPUSH/RPUSH/LPOP/RPOP/LSET/LTRIM/LINSERT/LREM/LPUSHX/RPUSHX/LMOVE |
| `TestReplicationCompleteness_Hash` | HSET/HDEL/HMSET/HSETNX/HINCRBY/HINCRBYFLOAT |
| `TestReplicationCompleteness_Set` | SADD/SREM/SMOVE/SINTERSTORE/SUNIONSTORE/SDIFFSTORE |
| `TestReplicationCompleteness_SortedSet` | ZADD/ZREM/ZINCRBY/ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE/ZREMRANGEBYRANK/ZREMRANGEBYSCORE/ZREMRANGEBYLEX |
| `TestReplicationCompleteness_Stream` | XADD/XDEL/XACK/XSETID/XGROUP CREATE/DESTROY/SETID/DELCONSUMER |
| `TestReplicationCompleteness_JSON` | JSON.SET/JSON.DEL/JSON.CLEAR/JSON.ARRAPPEND/JSON.NUMINCRBY/JSON.NUMMULTBY |
| `TestReplicationCompleteness_TimeSeries` | TS.CREATE/TS.ADD/TS.DEL/TS.INCRBY/TS.MADD/TS.CREATERULE/TS.DELETERULE |
| `TestReplicationCompleteness_HLL` | PFADD/PFMERGE |
| `TestReplicationCompleteness_Geo` | GEOADD |
| `TestReplicationCompleteness_Transaction` | MULTI/EXEC (包含写命令) |
| `TestReplicationCompleteness_Bitmap` | SETBIT/BITOP/BITFIELD |
| `TestReplicationCompleteness_FlushDB` | FLUSHDB/FLUSHALL |

### Phase 3: 远程运行脚本

**新文件**: `scripts/test-all-commands-remote.sh`

功能：
1. rsync 代码到 2.16
2. 后台运行全部测试（单机 + 主从）
3. 输出结果到日志文件
4. 支持 tmux/screen 后台运行

```bash
#!/bin/bash
# 运行全部命令准确性测试（单机 + 主从）
# 用法: bash scripts/test-all-commands-remote.sh
```

执行流程：
```
1. rsync 代码到 2.16
2. 后台启动 tmux session: "boltdb-cmd-test"
3. 依次运行:
   a. 单机全命令测试: go test -race -timeout 300s -run TestCommandCompleteness ./cmd/integration/...
   b. 主从全命令测试: go test -race -timeout 600s -run TestReplicationCompleteness ./cmd/integration/...
   c. 已有集成测试: go test -race -timeout 300s ./cmd/integration/...
   d. redis-py 兼容: python3 scripts/redis_py_compat.py
   e. redis-cli 兼容: bash scripts/redis_cli_compat.sh
4. 结果汇总到 /tmp/boltdb-cmd-test-results.log
```

---

## 文件变更清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `cmd/integration/command_completeness_test.go` | 新建 | 单机全命令准确性测试 (~1500 行) |
| `cmd/integration/replication_completeness_test.go` | 新建 | 主从全写命令传播测试 (~800 行) |
| `scripts/test-all-commands-remote.sh` | 新建 | 远程后台运行脚本 |

## 预估时间

- Phase 1 (单机测试): ~1500 行 Go 代码
- Phase 2 (主从测试): ~800 行 Go 代码
- Phase 3 (远程脚本): ~100 行 Shell 代码
- 远程测试运行时间: 单机 ~3 分钟, 主从 ~5 分钟, 总计 ~10 分钟

## 验证方式

远程运行完成后检查：
```bash
# 查看结果
cat /tmp/boltdb-cmd-test-results.log

# 或 tmux 中查看
tmux attach -t boltdb-cmd-test
```

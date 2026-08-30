package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// CloseTimeout is the maximum time to wait for BadgerDB to close.
// BadgerDB's doWrites goroutine can block during shutdown in rare cases,
// causing the full test suite to hang. A short timeout prevents cascading
// timeouts across all 279 tests (which would otherwise take ~558s).
// Returning nil is safe: temp dirs are cleaned by t.TempDir(), and
// Go 1.21+ waits for orphaned goroutines on process exit.
const CloseTimeout = 2 * time.Second

const (
	KeyTypeString      = "STRING"
	KeyTypeList        = "LIST"
	KeyTypeHash        = "HASH"
	KeyTypeSet         = "SET"
	KeyTypeJSON        = "JSON"
	KeyTypeTimeSeries  = "TIMESERIES"
	KeyTypeStream      = "STREAM"
	KeyTypeHyperLogLog = "hyperloglog"
)

const HyperLogLogPrefix = "hll:"

const (
	batchSize   = 500
	maxAttempts = 30
	maxRetries  = 60
)

var (
	prefixKeyTypeBytes       = []byte("TYPE_")
	prefixKeyJSONBytes       = []byte("JSON:")
	prefixKeyTimeSeriesBytes = []byte("TS:")
	// replMetaKey 是复制元数据的存储键（replId 持久化）
	replMetaKey   = []byte("__REPL_META__:repl_id")
	replOffsetKey = []byte("__REPL_META__:master_offset")
	backlogKey    = []byte("__REPL_META__:backlog")
)

// BlockingResult represents the result of a blocking pop operation
type BlockingResult struct {
	Key   string
	Value string
}

// StreamReadResult represents the result of a stream read operation
type StreamReadResult struct {
	Key     string
	Entries []StreamEntry
}

// zsetRankCache provides O(1) ZRANK/ZREVRANK lookups for a single zset.
// Built lazily on first ZRANK after any write to the zset.
type zsetRankCache struct {
	mu      sync.RWMutex
	dirty   bool
	members map[string]int64 // member → forward rank (0-based)
	scores  map[string]float64
}

// BotreonStore is the main store structure
type BotreonStore struct {
	db              *badger.DB
	compressionType CompressionType

	// Key-level locking for atomic operations
	keyLockMgr *KeyLockManager

	// Blocking queue support
	blockingMu       sync.RWMutex
	blockingPopChans map[string][]chan BlockingResult // key -> channels waiting for data

	// Blocking sorted set pop support
	blockingZPopMu    sync.Mutex
	blockingZPopChans map[string][]chan string // key -> channels waiting for sorted set pop (channel receives key name)

	// Stream blocking support
	streamBlockingMu    sync.RWMutex
	streamBlockingChans map[string][]chan StreamReadResult // key -> channels waiting for stream data

	// 主动背压
	backpressure atomic.Pointer[writeSlot]
	bpConfig     atomic.Pointer[BackpressureConfig]
	l0Cache      *l0Cache

	// 查询预算 — O(n) scan 防退化
	queryBudgetConfig atomic.Pointer[QueryBudgetConfig]
	// queryBudgetTrips counts how many times checkScanBudget returned ErrQueryBudgetExceeded.
	queryBudgetTrips atomic.Int64

	// zsetRankCache 为 ZSet 提供 O(1) ZRANK/ZREVRANK 缓存。
	// 键为 zset 名，值为 member → rank 映射 + dirty 标记。
	// lazy build: 首次 ZRANK 时扫描 BadgerDB 构建，写操作标记 dirty。
	zsetRankMu     sync.RWMutex
	zsetRankCaches map[string]*zsetRankCache

	// 重试指标
	retryMu      sync.Mutex
	retryMetrics struct {
		activeRetries int64
		totalRetries  int64
		writesBlocked int64
		l0Rejected    int64
		l0Delayed     int64
	}

	// 错误注入（仅测试用，nil = 禁用）
	errorInjector *ErrorInjector

	// closeCh 在 Close()/CloseWithTimeout() 时关闭，用于通知阻塞操作
	// （BLPOP/BRPOP/BZPOPMIN/BZPOPMAX/XREAD BLOCK）立即退出，
	// 避免操作结束后 channel 残留。
	closeCh   chan struct{}
	closeOnce sync.Once

	// scanBookmarks 存储 SCAN/HSCAN/SSCAN/ZSCAN 的游标书签。
	// cursor_id → last_key（完整 Badger key bytes）。
	// 替代之前 O(n²) 的位置计数器游标，实现 O(n) 全表遍历。
	// 上限 10000 条目，超出时淘汰最旧条目，防止客户端泄漏书签。
	scanBookmarks   map[uint64][]byte
	scanBookmarkSeq atomic.Uint64
	scanBookmarkMu  sync.Mutex

	// snapshotMu 将 commit → repl-offset 与 FULLRESYNC 的 snapshotOffset→View
	// 绑在同一把锁上（Issue #3）。
	//
	// - FULLRESYNC 持写锁：GetMasterReplOffset() → GenerateRDB View。
	// - 客户端写路径在 processRequest 持读锁，跨越 badger 提交与
	//   PropagateCommand（backlog.Append = offset）。读锁互不阻塞，写者
	//   排队时新的读会被挡住，因此不存在「已提交未传播」的可见窗口。
	snapshotMu sync.RWMutex
}

// SetErrorInjector 设置错误注入器（仅测试用）。传 nil 可禁用。
func (s *BotreonStore) SetErrorInjector(ei *ErrorInjector) {
	s.errorInjector = ei
}

// ErrorInjector 返回当前的错误注入器（可能为 nil）。
func (s *BotreonStore) ErrorInjector() *ErrorInjector {
	return s.errorInjector
}

// checkErrorInjector 检查是否有错误需注入。无注入时返回 nil。
func (s *BotreonStore) checkErrorInjector(method string) error {
	if s.errorInjector == nil {
		return nil
	}
	return s.errorInjector.Check(method)
}

// Check 执行存储一致性检查
// 验证所有键的 TYPE 记录与数据记录是否配对，检测孤立键
func (s *BotreonStore) Check() error {
	type keyInfo struct {
		hasType bool
		hasData bool
	}
	keyMap := make(map[string]*keyInfo)

	// extractRawKey 从存储键中提取原始 Redis key
	// TYPE_ 键格式为 TYPE_<type>:<keyname>，数据键格式为 <type>:<keyname>:<suffix>
	// 两者应提取相同的 keyname
	extractRawKey := func(k string) string {
		switch {
		case strings.HasPrefix(k, string(prefixKeyTypeBytes)):
			// TYPE_<full_key_name> -> 提取 <full_key_name>
			return strings.TrimPrefix(k, string(prefixKeyTypeBytes))
		case strings.HasPrefix(k, KeyTypeString+":"):
			// string:<keyname> -> <keyname>
			return strings.TrimPrefix(k, KeyTypeString+":")
		case strings.HasPrefix(k, KeyTypeList+":"):
			// LIST:<keyname>:<rest>，keyname 可以包含冒号
			// 已知后缀: :length, :start, :end
			// UUID 节点: <uuid> 或 <uuid>:next/:prev
			rest := strings.TrimPrefix(k, KeyTypeList+":")
			// 先尝试匹配已知后缀
			for _, suffix := range []string{":length", ":start", ":end"} {
				if strings.HasSuffix(rest, suffix) {
					return strings.TrimSuffix(rest, suffix)
				}
			}
			// 检查是否以 :next 或 :prev 结尾（节点指针）
			if strings.HasSuffix(rest, ":next") || strings.HasSuffix(rest, ":prev") {
				// 提取 UUID 部分 (strip :next or :prev)
				candidate := strings.TrimSuffix(rest, ":next")
				candidate = strings.TrimSuffix(candidate, ":prev")
				// candidate 现在是 keyname:UUID，提取 keyname
				lastColon := strings.LastIndex(candidate, ":")
				if lastColon > 0 {
					return candidate[:lastColon]
				}
				return candidate
			}
			// 检查最后一段是否是 UUID (36字符: 8-4-4-4-12)
			lastColon := strings.LastIndex(rest, ":")
			if lastColon > 0 {
				lastPart := rest[lastColon+1:]
				if len(lastPart) == 36 && isUUIDFormat(lastPart) {
					// 提取 keyname (UUID 之前的部分)
					return rest[:lastColon]
				}
			}
			// 如果没找到后缀，返回原样
			return rest
		case strings.HasPrefix(k, KeyTypeHash+":"):
			// HASH:<keyname>:<rest>，keyname 可以包含冒号
			rest := strings.TrimPrefix(k, KeyTypeHash+":")
			// 已知后缀: :__count__
			if strings.HasSuffix(rest, ":__count__") {
				return strings.TrimSuffix(rest, ":__count__")
			}
			// 其他后缀格式为 :<field>，keyname 可以包含冒号
			// 找最后一个冒号，它后面是 field 名称
			if idx := strings.LastIndex(rest, ":"); idx >= 0 {
				return rest[:idx]
			}
			return rest
		case strings.HasPrefix(k, KeyTypeSet+":"):
			// SET:<keyname>:<rest>，keyname 可以包含冒号
			rest := strings.TrimPrefix(k, KeyTypeSet+":")
			// 已知后缀: :count, :member:<member>
			if strings.HasSuffix(rest, ":count") {
				return strings.TrimSuffix(rest, ":count")
			}
			if strings.Contains(rest, ":member:") {
				// 格式: SET:keyname:member:membervalue
				idx := strings.Index(rest, ":member:")
				if idx >= 0 {
					return rest[:idx]
				}
			}
			// 其他后缀
			if idx := strings.Index(rest, ":"); idx >= 0 {
				return rest[:idx]
			}
			return rest
		case strings.HasPrefix(k, "zset:"):
			// zset:<keyname>:<rest>，keyname 可以包含冒号
			// 格式: zset:<keyname>:meta, zset:<keyname>:data:<member>, zset:<keyname>:index:<encoded>
			rest := strings.TrimPrefix(k, "zset:")
			// 精确匹配 :meta 后缀
			if strings.HasSuffix(rest, ":meta") {
				return strings.TrimSuffix(rest, ":meta")
			}
			// 匹配 :data: 和 :index: 前缀（这些后面还有成员信息）
			for _, prefix := range []string{":data:", ":index:"} {
				if strings.Contains(rest, prefix) {
					idx := strings.Index(rest, prefix)
					return rest[:idx]
				}
			}
			// 其他后缀
			if idx := strings.Index(rest, ":"); idx >= 0 {
				return rest[:idx]
			}
			return rest
		case strings.HasPrefix(k, string(prefixKeyJSONBytes)):
			// JSON:<keyname> -> <keyname>
			return strings.TrimPrefix(k, string(prefixKeyJSONBytes))
		case strings.HasPrefix(k, string(prefixKeyTimeSeriesBytes)):
			// TS:<keyname>:<suffix>，keyname 可以包含冒号
			// 已知后缀: :meta, :data:<timestamp>
			// TS:rule:<src>:<dst> 是聚合规则元数据，不需要 TYPE_ 键
			rest := strings.TrimPrefix(k, string(prefixKeyTimeSeriesBytes))
			if strings.HasPrefix(rest, "rule:") {
				return "" // 跳过规则键，不参与一致性检查
			}
			// 匹配 :meta 和 :data: 前缀
			for _, suffix := range []string{":meta", ":data:"} {
				if idx := strings.Index(rest, suffix); idx >= 0 {
					return rest[:idx]
				}
			}
			return rest
		case strings.HasPrefix(k, "hll:"):
			// hll:<keyname> -> <keyname>
			return strings.TrimPrefix(k, "hll:")
		case strings.HasPrefix(k, "geo:"):
			// Format: geo:<keyname>:meta, geo:<keyname>:index:member, geo:<keyname>:members:, geo:<keyname>:hash:...
			rest := strings.TrimPrefix(k, "geo:")
			for _, suffix := range []string{":meta", ":index", ":members", ":hash"} {
				if strings.HasPrefix(rest, suffix) {
					return strings.TrimPrefix(rest, suffix)
				}
				if idx := strings.Index(rest, suffix); idx >= 0 {
					return rest[:idx]
				}
			}
			if idx := strings.Index(rest, ":"); idx >= 0 {
				return rest[:idx]
			}
			return rest
		case strings.HasPrefix(k, "stream:"):
			// Format: stream:<keyname>:meta, stream:<keyname>:data:..., stream:<keyname>:groups:...
			rest := strings.TrimPrefix(k, "stream:")
			if idx := strings.Index(rest, ":"); idx >= 0 {
				return rest[:idx]
			}
			return rest
		}
		return ""
	}

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := string(it.Item().Key())

			// 跳过 BadgerDB 内部键
			if strings.HasPrefix(key, "_") || key == "BARRING" {
				continue
			}

			rawKey := extractRawKey(key)
			if rawKey == "" {
				continue
			}

			if _, ok := keyMap[rawKey]; !ok {
				keyMap[rawKey] = &keyInfo{}
			}
			if strings.HasPrefix(key, string(prefixKeyTypeBytes)) {
				keyMap[rawKey].hasType = true
			} else {
				keyMap[rawKey].hasData = true
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to iterate keys: %w", err)
	}

	var orphanType, orphanData []string
	for rawKey, info := range keyMap {
		if info.hasType && !info.hasData {
			orphanType = append(orphanType, rawKey)
		} else if !info.hasType && info.hasData {
			orphanData = append(orphanData, rawKey)
		}
	}

	if len(orphanType) > 0 || len(orphanData) > 0 {
		return fmt.Errorf("consistency check failed: %d orphan type keys (TYPE_ exists but data missing: %v), %d orphan data keys (data exists but TYPE_ missing: %v)",
			len(orphanType), orphanType, len(orphanData), orphanData)
	}

	// 执行 TTL 格式迁移（纳秒 → 秒）
	// 旧版本 Expire()/PExpire() 使用 uint64(time.Now().UnixNano()) 写入 ExpiresAt，
	// 这是与 BadgerDB 预期格式（秒级 Unix 时间戳）不一致的错误格式。
	// 迁移会扫描所有 key，将纳秒格式的 TTL 转换为秒格式。
	if err := s.migrateTTLFormat(); err != nil {
		logger.Logger.Warn().Err(err).Msg("TTL 格式迁移失败（非致命，旧格式 TTL 仍可被识别）")
	}
	return nil
}

// migrateTTLFormat 将 BadgerDB 中纳秒格式的 ExpiresAt 迁移为秒格式。
// 旧版 Expire()/PExpire() 错误地写入了纳秒级时间戳，而 BadgerDB 期望秒级 Unix 时间戳。
// 这个迁移是一次性的：扫描所有 key，将纳秒格式的 TTL 转换为秒格式。
func (s *BotreonStore) migrateTTLFormat() error {
	nowUnix := uint64(time.Now().Unix())
	type fixEntry struct {
		key    []byte
		newExp uint64
	}
	var keysToFix []fixEntry

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			expiresAt := item.ExpiresAt()
			if expiresAt == 0 {
				continue
			}
			// 纳秒格式：值远大于 nowUnix*100（约 1.75e18 vs 1.75e9）
			if expiresAt > nowUnix*100 {
				keysToFix = append(keysToFix, fixEntry{
					key:    item.KeyCopy(nil),
					newExp: expiresAt / 1e9, // 纳秒 → 秒
				})
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to scan for TTL migration: %w", err)
	}

	if len(keysToFix) == 0 {
		return nil // 无需迁移
	}

	logger.Logger.Info().Int("count", len(keysToFix)).Msg("TTL 格式迁移：发现纳秒格式的过期时间，正在迁移为秒格式")

	for _, kf := range keysToFix {
		if err := s.db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get(kf.key)
			if err != nil {
				return err
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			e := badger.NewEntry(kf.key, val)
			e.ExpiresAt = kf.newExp
			return txn.SetEntry(e)
		}); err != nil {
			logger.Logger.Warn().Err(err).Bytes("key", kf.key).Msg("TTL 迁移：单个 key 迁移失败，继续")
		}
	}

	logger.Logger.Info().Int("migrated", len(keysToFix)).Msg("TTL 格式迁移完成")
	return nil
}

// NewBotreonStore 创建新的BotreonStore实例
func NewBotreonStore(path string) (*BotreonStore, error) {
	return NewBotreonStoreWithCompression(path, CompressionSnappy)
}

// NewBotreonStoreWithCompression 创建新的BotreonStore实例，指定压缩算法
func NewBotreonStoreWithCompression(path string, compressionType CompressionType) (*BotreonStore, error) {
	opts := badger.DefaultOptions(path)

	// 性能优化配置
	// 1. 增加 memtable 数量，提高写入并发性能（优化：从5增加到7，减少事务冲突）
	opts.NumMemtables = 7             // 增加到 7，提高并发写入性能
	opts.NumLevelZeroTables = 5       // Level 0 表数量
	opts.NumLevelZeroTablesStall = 10 // Level 0 停滞阈值

	// 2. 优化 Value Log 配置
	opts.ValueLogFileSize = 1024 * 1024 * 1024 // 1GB（默认 1GB，适合大值）
	opts.ValueLogMaxEntries = 1000000          // 每个 vlog 文件最大条目数

	// 3. 优化 Table 配置
	// BadgerDB v4 使用 BlockSize 而不是 MaxTableSize
	opts.BlockSize = 4 * 1024     // 4KB 块大小（默认 4KB）
	opts.LevelSizeMultiplier = 10 // Level 大小倍数（默认 10）

	// 4. 压缩配置（BadgerDB 内置压缩）
	// BadgerDB v4 使用 CompressionType，值为 0=无压缩, 1=Snappy, 2=ZSTD
	opts.Compression = 2 // 使用 ZSTD 压缩（比 Snappy 更好）

	// 5. 索引/块缓存配置
	opts.IndexCacheSize = 100 * 1024 * 1024 // 100MB 索引缓存（默认 0，禁用）
	// 显式配置数据块缓存：badger 默认 256MB/节点，显式调小到 128MB 以降低
	// 每节点固定内存占用（配合 GOMEMLIMIT 7 层 OOM 防护）；读密集场景可调大。
	opts.BlockCacheSize = 128 * 1024 * 1024 // 128MB 数据块缓存

	// 6. 减少同步频率（提高性能，但降低持久性）
	// opts.SyncWrites = false // 默认 false，异步写入提高性能

	// 7. 优化垃圾回收
	opts.NumGoroutines = 8 // GC goroutine 数量（默认 8）

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	bpConfig := DefaultBackpressureConfig()

	p := newWriteSlot(bpConfig.MaxConcurrentWrites)
	s := &BotreonStore{
		db:                  db,
		compressionType:     compressionType,
		keyLockMgr:          NewKeyLockManager(runtime.GOMAXPROCS(0) * 16),
		blockingPopChans:    make(map[string][]chan BlockingResult),
		blockingZPopChans:   make(map[string][]chan string),
		streamBlockingChans: make(map[string][]chan StreamReadResult),
		l0Cache:             &l0Cache{},
		zsetRankCaches:      make(map[string]*zsetRankCache),
		closeCh:             make(chan struct{}),
		scanBookmarks:       make(map[uint64][]byte),
	}
	s.backpressure.Store(p)
	cfg := bpConfig
	s.bpConfig.Store(&cfg)
	qbCfg := DefaultQueryBudgetConfig()
	s.queryBudgetConfig.Store(&qbCfg)
	return s, nil
}

func (s *BotreonStore) Close() error {
	s.closeOnce.Do(func() {
		close(s.closeCh)
	})
	return s.db.Close()
}

// CloseWithTimeout closes the store with a timeout to prevent
// indefinite blocking due to BadgerDB's doWrites drain bug.
func (s *BotreonStore) CloseWithTimeout(timeout time.Duration) error {
	s.closeOnce.Do(func() {
		close(s.closeCh)
	})
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- s.db.Close()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		// Close is taking too long, return timeout error
		return fmt.Errorf("close timed out after %v", timeout)
	}
}

// SetBackpressureConfig 配置主动背压参数
func (s *BotreonStore) SetBackpressureConfig(cfg BackpressureConfig) {
	s.retryMu.Lock()
	defer s.retryMu.Unlock()
	if cfg.MaxConcurrentWrites <= 0 {
		cfg.MaxConcurrentWrites = defaultMaxConcurrentWrites
	}
	s.backpressure.Store(newWriteSlot(cfg.MaxConcurrentWrites))
	s.bpConfig.Store(&cfg)
}

// GetBackpressureConfig 返回当前背压配置
func (s *BotreonStore) GetBackpressureConfig() BackpressureConfig {
	return *s.bpConfig.Load()
}

// GetDB 获取BadgerDB实例（用于复制和备份）
func (s *BotreonStore) GetDB() *badger.DB {
	return s.db
}

// SaveReplID 持久化复制 ID 到 BadgerDB，重启后保持 replId 不变
// 从而避免主节点重启导致所有从节点触发 FULLRESYNC。
func (s *BotreonStore) SaveReplID(id string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(replMetaKey, []byte(id))
	})
}

// SaveReplIDLocked 与 SaveReplID 相同，但在 snapshotMu 读锁保护下执行，
// 用于 FULLRESYNC 临界区内的元数据写入。
func (s *BotreonStore) SaveReplIDLocked(id string) error {
	return s.RunWriteLocked(func(txn *badger.Txn) error {
		return txn.Set(replMetaKey, []byte(id))
	})
}

// LoadReplID 从 BadgerDB 读取持久化的复制 ID。
// 返回空字符串表示没有持久化的 ID（首次启动或旧数据库）。
func (s *BotreonStore) LoadReplID() (string, error) {
	var id string
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(replMetaKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 首次启动，无持久化 replId
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		id = string(val)
		return nil
	})
	return id, err
}

// SaveMasterReplOffset 持久化主节点复制偏移量到 BadgerDB，
// 确保重启后从节点可以通过 PSYNC CONTINUE 而非 FULLRESYNC 重新连接。
func (s *BotreonStore) SaveMasterReplOffset(offset int64) error {
	return s.db.Update(func(txn *badger.Txn) error {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(offset))
		return txn.Set(replOffsetKey, buf)
	})
}

// SaveMasterReplOffsetLocked 与 SaveMasterReplOffset 相同，但在 snapshotMu
// 读锁保护下执行，避免与 FULLRESYNC 快照窗口并发。
func (s *BotreonStore) SaveMasterReplOffsetLocked(offset int64) error {
	return s.RunWriteLocked(func(txn *badger.Txn) error {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(offset))
		return txn.Set(replOffsetKey, buf)
	})
}

// LoadMasterReplOffset 从 BadgerDB 读取持久化的主节点复制偏移量。
func (s *BotreonStore) LoadMasterReplOffset() (int64, error) {
	var offset int64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(replOffsetKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 首次启动，无持久化偏移量
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if len(val) >= 8 {
			offset = int64(binary.BigEndian.Uint64(val[:8]))
		}
		return nil
	})
	return offset, err
}

// SaveBacklog 持久化 replication backlog 的环形缓冲区、偏移量和大小到 BadgerDB，
// 确保干净重启后从节点可以通过 PSYNC CONTINUE 增量同步。
// backlog 最大 512MB；默认 1MB 的持久化开销可接受。
func (s *BotreonStore) SaveBacklog(offset int64, buffer []byte, size int64) error {
	// 编码：offset(int64) + size(int64) + buffer([]byte)
	buf := make([]byte, 16+len(buffer))
	binary.BigEndian.PutUint64(buf[0:8], uint64(offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(size))
	copy(buf[16:], buffer)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(backlogKey, buf)
	})
}

// SaveBacklogLocked 与 SaveBacklog 相同，但在 snapshotMu 读锁保护下执行。
func (s *BotreonStore) SaveBacklogLocked(offset int64, buffer []byte, size int64) error {
	buf := make([]byte, 16+len(buffer))
	binary.BigEndian.PutUint64(buf[0:8], uint64(offset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(size))
	copy(buf[16:], buffer)
	return s.RunWriteLocked(func(txn *badger.Txn) error {
		return txn.Set(backlogKey, buf)
	})
}

// LoadBacklog 从 BadgerDB 读取持久化的 replication backlog。
// 返回 (offset, buffer, size, error)。buffer 为 nil 表示首次启动或无持久化数据。
func (s *BotreonStore) LoadBacklog() (int64, []byte, int64, error) {
	var offset int64
	var buffer []byte
	var size int64
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(backlogKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 首次启动
		}
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if len(val) < 16 {
			return nil // 无效数据
		}
		offset = int64(binary.BigEndian.Uint64(val[0:8]))
		size = int64(binary.BigEndian.Uint64(val[8:16]))
		buffer = make([]byte, len(val)-16)
		copy(buffer, val[16:])
		return nil
	})
	return offset, buffer, size, err
}

// DeleteBacklog 删除持久化的 backlog 数据。
func (s *BotreonStore) DeleteBacklog() error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(backlogKey)
	})
}

// DeleteBacklogLocked 与 DeleteBacklog 相同，但在 snapshotMu 读锁保护下执行。
func (s *BotreonStore) DeleteBacklogLocked() error {
	return s.RunWriteLocked(func(txn *badger.Txn) error {
		return txn.Delete(backlogKey)
	})
}

// IterateRawKeys 遍历所有 TYPE_ 记录，对每个逻辑 key 调用 fn。
// fn 返回 false 时停止遍历。
func (s *BotreonStore) IterateRawKeys(fn func(rawKey string) bool) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefixKeyTypeBytes
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := string(item.Key())
			rawKey := strings.TrimPrefix(k, string(prefixKeyTypeBytes))
			if !fn(rawKey) {
				break
			}
		}
		return nil
	})
}

// FlushDB 删除数据库中的所有键
// NOTE: 使用 ClearAllData 替代 DropAll 以避免在集成测试中遇到 DropAll 的阻塞问题
func (s *BotreonStore) FlushDB() error {
	if err := s.ClearAllData(); err != nil {
		return err
	}
	s.ClearCaches()
	return nil
}

// ClearAllData 安全地清空所有数据，用于测试隔离
// 使用分批迭代删除，每批独立重试处理 "Writes are blocked"
func (s *BotreonStore) ClearAllData() error {
	return s.clearAllDataIterative()
}

func (s *BotreonStore) ClearCaches() {
	s.clearAllZSetRankCaches()
}

// clearAllDataIterative 迭代删除所有键（DropPrefix 失败时的备选方案）
// 使用分批迭代删除，并在开始前等待 doWrites 可用
func (s *BotreonStore) clearAllDataIterative() error {
	// 等待直到 doWrites 可以接受写请求，避免后续批次持续重试
	if err := s.waitForWritesReady(); err != nil {
		return err
	}

	// 步骤1: 收集所有键（只读事务，不会阻塞）
	keys, err := s.collectAllKeys()
	if err != nil {
		return err
	}

	// 如果没有键，直接返回
	if len(keys) == 0 {
		return nil
	}

	// 步骤2: 分批删除，每批独立重试
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]

		if err := s.deleteBatchWithRetry(batch); err != nil {
			return err
		}
	}
	return nil
}

// waitForWritesReady 等待 Badger 的 doWrites 通道恢复，允许写操作
// 通过发送一个空的 Update 事务来探测，遇到 "Writes are blocked" 时重试
func (s *BotreonStore) waitForWritesReady() error {
	for i := 0; i < maxAttempts; i++ {
		err := s.retryUpdate(func(txn *badger.Txn) error {
			return nil // no-op
		}, 30)
		if err == nil {
			return nil // 可以写入了
		}
		if !errors.Is(err, badger.ErrBlockedWrites) {
			return err // 非阻塞错误，直接返回
		}
		// 指数退避
		backoff := time.Duration(1<<uint(i)) * time.Millisecond
		if backoff > 2*time.Second {
			backoff = 2 * time.Second
		}
		time.Sleep(backoff)
	}
	return fmt.Errorf("writes blocked after %d attempts", maxAttempts)
}

// systemKeyPrefixes 是清库（FLUSHDB/ClearAllData）时必须保留的系统 key。
// 删除它们会破坏复制身份与集群拓扑：replId 丢失 → 重启生成新 ID，
// 从节点被迫 FULLRESYNC；cluster:config 丢失 → 重启后节点认领全部槽位，
// 多节点同时重启即脑裂（见 2026-08 线上事故）。
var systemKeyPrefixes = [][]byte{
	[]byte("__REPL_META__:"), // replId、master_offset、backlog
	[]byte("cluster:config"), // 集群节点表/槽位/epoch
}

// isSystemKey 报告 key 是否为清库时应保留的系统 key。
func isSystemKey(key []byte) bool {
	for _, prefix := range systemKeyPrefixes {
		if bytes.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// collectAllKeys 只读扫描所有键
func (s *BotreonStore) collectAllKeys() ([][]byte, error) {
	var keys [][]byte
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			keyCopy := make([]byte, len(item.Key()))
			copy(keyCopy, item.Key())
			if isSystemKey(keyCopy) {
				continue // 保留复制元数据与集群配置
			}
			keys = append(keys, keyCopy)
		}
		return nil
	})
	return keys, err
}

// deleteBatchWithRetry 删除单个批次，遇到 "Writes are blocked" 时重试
func (s *BotreonStore) deleteBatchWithRetry(keys [][]byte) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		lastErr = s.retryUpdate(func(txn *badger.Txn) error {
			for _, key := range keys {
				if err := txn.Delete(key); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
			}
			return nil
		}, 30)
		if lastErr == nil {
			return nil
		}

		// 只对 Writes are blocked 错误进行重试
		if !errors.Is(lastErr, badger.ErrBlockedWrites) {
			return lastErr
		}

		// 指数退避，起始 500ms，最大 5 秒
		backoff := 500 * time.Millisecond * time.Duration(1<<uint(attempt))
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
		// 添加抖动避免重试风暴
		jitter := time.Duration(rand.Float64() * float64(backoff) * 0.2)
		time.Sleep(backoff + jitter)
	}

	return fmt.Errorf("delete batch failed after %d retries: %w", maxRetries, lastErr)
}

// TypeOfKeyGet 用于生成存储类型的键
func TypeOfKeyGet(strKey string) []byte {
	bKey := []byte(strKey)
	bKey = append(prefixKeyTypeBytes, bKey...)
	return bKey
}

// isUUIDFormat 检查字符串是否符合 UUID 格式 (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
func isUUIDFormat(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else {
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
				return false
			}
		}
	}
	return true
}

// RunValueLogGC runs BadgerDB value log garbage collection, rewriting vlog
// files whose discard ratio exceeds the given threshold (0.0-1.0, e.g. 0.5
// rewrites files that are at least 50% garbage). It keeps rewriting files
// until no eligible file remains (badger.ErrNoRewrite) and returns the number
// of vlog files rewritten. A single pass may not reclaim everything; callers
// can invoke it repeatedly.
func (s *BotreonStore) RunValueLogGC(discardRatio float64) (int, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("store not open")
	}
	if discardRatio < 0 || discardRatio > 1 {
		return 0, fmt.Errorf("invalid discard ratio %v (must be 0.0-1.0)", discardRatio)
	}
	rewritten := 0
	for {
		err := s.db.RunValueLogGC(discardRatio)
		if errors.Is(err, badger.ErrNoRewrite) {
			return rewritten, nil
		}
		if err != nil {
			return rewritten, err
		}
		rewritten++
	}
}

// snapshotMu 相关：线性 FULLRESYNC 边界的同步原语。

// ViewWithSnapshotLock 在 snapshotMu 写锁保护下执行 View。
func (s *BotreonStore) ViewWithSnapshotLock(fn func(txn *badger.Txn) error) error {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	return s.db.View(fn)
}

// RunWriteLocked 在 snapshotMu 读锁保护下执行一次 Update 事务。
// 所有写路径应改用此辅助，使并发写入在 FULLRESYNC 快照窗口内被短暂
// 阻塞，实现 snapshotOffset 与 MVCC 的线性绑定。
func (s *BotreonStore) RunWriteLocked(fn func(txn *badger.Txn) error) error {
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()
	return s.db.Update(fn)
}

// SnapshotMuLock / SnapshotMuUnlock 暴露给需要跨多步原子绑定的
// 场景（FULLRESYNC：先捕获偏移再开 View）。调用方负责配对 Unlock。
func (s *BotreonStore) SnapshotMuLock()   { s.snapshotMu.Lock() }
func (s *BotreonStore) SnapshotMuUnlock() { s.snapshotMu.Unlock() }

// SnapshotMuRLock / SnapshotMuRUnlock 给写路径跨越 commit → offset 赋值。
// 必须与 FULLRESYNC 的写锁配对；禁止在已持有读锁时再 RLock（写者排队会死锁）。
func (s *BotreonStore) SnapshotMuRLock()   { s.snapshotMu.RLock() }
func (s *BotreonStore) SnapshotMuRUnlock() { s.snapshotMu.RUnlock() }

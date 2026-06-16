package store

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// CloseTimeout is the maximum time to wait for BadgerDB to close.
// BadgerDB's doWrites goroutine can block during shutdown in rare cases,
// causing the full test suite to hang. A short timeout prevents cascading
// timeouts across all 279 tests (which would otherwise take ~558s).
// Returning nil is safe: temp dirs are cleaned by t.TempDir(), and
// Go 1.21+ waits for orphaned goroutines on process exit.
const CloseTimeout = 2 * time.Second

const (
	//UnderScore       = "_"
	KeyTypeString      = "STRING"
	KeyTypeList        = "LIST"
	KeyTypeHash        = "HASH"
	KeyTypeSet         = "SET"
	KeyTypeJSON        = "JSON"
	KeyTypeTimeSeries  = "TIMESERIES"
	KeyTypeStream      = "STREAM"
	KeyTypeHyperLogLog = "hyperloglog"
	//KeyTypeSortedSet = "SORTEDSET" - defined in sorted_set.go as "zset"
	//
	//sortedSetIndex = "_INDEX_"
	//sortedSetData  = "_DATA_"
)

var (
	prefixKeyTypeBytes = []byte("TYPE_")
	//prefixKeySortedSetBytes = []byte("SORTEDSET_")
	prefixKeyJSONBytes       = []byte("JSON:")
	prefixKeyTimeSeriesBytes = []byte("TS:")
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

// BotreonStore is the main store structure
type BotreonStore struct {
	db              *badger.DB
	compressionType CompressionType
	readCache       *LRUCache

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
	backpressure *writeSlot
	bpConfig     BackpressureConfig
	l0Cache      *l0Cache

	// 重试指标
	retryMu      sync.Mutex
	retryMetrics struct {
		activeRetries int64
		totalRetries  int64
		writesBlocked int64
		l0Rejected    int64
		l0Delayed     int64
	}
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
			rest := strings.TrimPrefix(k, string(prefixKeyTimeSeriesBytes))
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

	// 5. 索引缓存配置
	opts.IndexCacheSize = 100 * 1024 * 1024 // 100MB 索引缓存（默认 0，禁用）

	// 6. 减少同步频率（提高性能，但降低持久性）
	// opts.SyncWrites = false // 默认 false，异步写入提高性能

	// 7. 优化垃圾回收
	opts.NumGoroutines = 8 // GC goroutine 数量（默认 8）

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	readCache := NewLRUCache(10000, 5*time.Minute)

	bpConfig := DefaultBackpressureConfig()

	return &BotreonStore{
		db:                  db,
		compressionType:     compressionType,
		readCache:           readCache,
		keyLockMgr:          NewKeyLockManager(256),
		blockingPopChans:    make(map[string][]chan BlockingResult),
		blockingZPopChans:   make(map[string][]chan string),
		streamBlockingChans: make(map[string][]chan StreamReadResult),
		backpressure:        newWriteSlot(bpConfig.MaxConcurrentWrites),
		bpConfig:            bpConfig,
		l0Cache:             &l0Cache{},
	}, nil
}

func (s *BotreonStore) Close() error {
	return s.db.Close()
}

// CloseWithTimeout closes the store with a timeout to prevent
// indefinite blocking due to BadgerDB's doWrites drain bug.
func (s *BotreonStore) CloseWithTimeout(timeout time.Duration) error {
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
	s.backpressure = newWriteSlot(cfg.MaxConcurrentWrites)
	s.bpConfig = cfg
}

// GetBackpressureConfig 返回当前背压配置
func (s *BotreonStore) GetBackpressureConfig() BackpressureConfig {
	s.retryMu.Lock()
	defer s.retryMu.Unlock()
	return s.bpConfig
}

// GetDB 获取BadgerDB实例（用于复制和备份）
func (s *BotreonStore) GetDB() *badger.DB {
	return s.db
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
	return s.ClearAllData()
}

// ClearAllData 安全地清空所有数据，用于测试隔离
// 使用分批迭代删除，每批独立重试处理 "Writes are blocked"
func (s *BotreonStore) ClearAllData() error {
	return s.clearAllDataIterative()
}

func (s *BotreonStore) ClearCaches() {
	if s.readCache != nil {
		s.readCache.Clear()
	}
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
	const batchSize = 500
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
	const maxAttempts = 30
	for i := 0; i < maxAttempts; i++ {
		err := s.retryUpdate(func(txn *badger.Txn) error {
			return nil // no-op
		}, 30)
		if err == nil {
			return nil // 可以写入了
		}
		if !strings.Contains(err.Error(), "Writes are blocked") {
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
			keys = append(keys, keyCopy)
		}
		return nil
	})
	return keys, err
}

// deleteBatchWithRetry 删除单个批次，遇到 "Writes are blocked" 时重试
func (s *BotreonStore) deleteBatchWithRetry(keys [][]byte) error {
	const maxRetries = 60

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		lastErr = s.retryUpdate(func(txn *badger.Txn) error {
			for _, key := range keys {
				if err := txn.Delete(key); err != nil && err != badger.ErrKeyNotFound {
					return err
				}
			}
			return nil
		}, 30)
		if lastErr == nil {
			return nil
		}

		// 只对 "Writes are blocked" 错误进行重试
		if !strings.Contains(lastErr.Error(), "Writes are blocked") {
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
		pos := i % 37
		if pos == 8 || pos == 13 || pos == 18 || pos == 23 {
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

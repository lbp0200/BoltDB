package store

import (
	"github.com/dgraph-io/badger/v4"
)

// ScanResult 是 SCAN 命令的返回结果
type ScanResult struct {
	Cursor uint64
	Keys   []string
}

// scanBookmarkStore 管理 SCAN 书签查找和释放
const maxScanBookmarks = 10000

func (s *BotreonStore) scanBookmarkLookup(cursor uint64) []byte {
	s.scanBookmarkMu.Lock()
	defer s.scanBookmarkMu.Unlock()
	key, ok := s.scanBookmarks[cursor]
	if !ok {
		return nil
	}
	return key
}

func (s *BotreonStore) scanBookmarkStore(lastKey []byte) uint64 {
	s.scanBookmarkMu.Lock()
	defer s.scanBookmarkMu.Unlock()
	s.scanBookmarkSeq.Add(1)
	id := s.scanBookmarkSeq.Load()
	s.scanBookmarks[id] = lastKey
	// 超过上限时批量淘汰到 75% 容量，防止并发 SCAN 下 map 无限增长
	if len(s.scanBookmarks) > maxScanBookmarks {
		target := maxScanBookmarks * 3 / 4
		for k := range s.scanBookmarks {
			if k == id {
				continue
			}
			delete(s.scanBookmarks, k)
			if len(s.scanBookmarks) <= target {
				break
			}
		}
	}
	return id
}

func (s *BotreonStore) scanBookmarkRelease(cursor uint64) {
	if cursor == 0 {
		return
	}
	s.scanBookmarkMu.Lock()
	defer s.scanBookmarkMu.Unlock()
	delete(s.scanBookmarks, cursor)
}

// Scan 实现 SCAN 命令，增量迭代数据库中的键。
// 游标从位置计数器改为书签（最后返回的 key），
// 复杂度从 O(n²) 降为 O(n)。
func (s *BotreonStore) Scan(cursor uint64, pattern string, count int) (ScanResult, error) {
	var result ScanResult
	result.Keys = []string{}

	if count <= 0 {
		count = 10 // 默认值
	}

	// 从书签恢复 seek key
	seekKey := s.scanBookmarkLookup(cursor)
	s.scanBookmarkRelease(cursor) // 释放旧书签

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := prefixKeyTypeBytes
		if seekKey != nil {
			iter.Seek(seekKey)
			// 跳过 bookmark key 本身，避免重复返回
			if iter.ValidForPrefix(prefix) {
				iter.Next()
			}
		} else {
			iter.Seek(prefix)
		}

		collected := 0
		var lastKey []byte
		for iter.ValidForPrefix(prefix) && collected < count {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			key := string(keyBytes[len(prefixKeyTypeBytes):])

			if pattern == "" || pattern == "*" || matchPattern(key, pattern) {
				result.Keys = append(result.Keys, key)
				collected++
			}

			lastKey = keyBytes
			iter.Next()
		}

		// 还有更多键？存书签供下次使用
		if iter.ValidForPrefix(prefix) {
			result.Cursor = s.scanBookmarkStore(lastKey)
		} else {
			result.Cursor = 0 // 0 表示迭代完成
		}

		return nil
	})
	return result, err
}

// DBSize 返回数据库中键的数量（不展开键列表，避免大库 O(n) 分配）。
func (s *BotreonStore) DBSize() (int64, error) {
	var count int64
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()
		prefix := prefixKeyTypeBytes
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			count++
		}
		return nil
	})
	return count, err
}

// Keys 实现 Keys 命令，返回匹配模式的键列表
func (s *BotreonStore) Keys(pattern string) ([]string, error) {
	var keys []string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := prefixKeyTypeBytes
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			key := string(keyBytes[len(prefixKeyTypeBytes):])
			if matchPattern(key, pattern) {
				keys = append(keys, key)
			}
		}
		return nil
	})
	return keys, err
}

// matchPattern 实现通配符匹配（*和?），确定性 O(len(key)×len(pattern)) 动态规划
// 避免原回溯算法在 *a*b*c*d 等模式下的灾难性指数回溯
func matchPattern(key, pattern string) bool {
	if pattern == "*" {
		return true
	}
	keyRunes := []rune(key)
	patRunes := []rune(pattern)
	n, m := len(keyRunes), len(patRunes)

	if m == 0 {
		return n == 0
	}

	// DP 状态压缩：只保留两行，O(m) 空间
	prev := make([]bool, m+1)
	curr := make([]bool, m+1)
	prev[0] = true
	for j := 1; j <= m; j++ {
		if patRunes[j-1] == '*' {
			prev[j] = prev[j-1]
		}
	}

	for i := 1; i <= n; i++ {
		curr[0] = false
		for j := 1; j <= m; j++ {
			switch patRunes[j-1] {
			case '*':
				curr[j] = curr[j-1] || prev[j]
			case '?':
				curr[j] = prev[j-1]
			default:
				if patRunes[j-1] == keyRunes[i-1] {
					curr[j] = prev[j-1]
				} else {
					curr[j] = false
				}
			}
		}
		prev, curr = curr, prev
	}

	return prev[m]
}

// RandomKey 实现 RANDOMKEY 命令。
// 使用蓄水池采样（reservoir sampling），单次遍历 O(n)，
// 替代之前的双遍历 O(2n)。
func (s *BotreonStore) RandomKey() (string, error) {
	var key string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := prefixKeyTypeBytes

		// 蓄水池采样：单次遍历，每个键以 1/i 概率替换选中键
		count := 0
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			count++
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			if count == 1 {
				key = string(keyBytes[len(prefixKeyTypeBytes):])
			} else {
				j := randomIntn(count)
				if j == 0 {
					key = string(keyBytes[len(prefixKeyTypeBytes):])
				}
			}
		}

		return nil
	})
	return key, err
}

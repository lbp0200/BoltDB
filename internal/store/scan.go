package store

import (
	"time"

	"github.com/dgraph-io/badger/v4"
)

// ScanResult 是 SCAN 命令的返回结果
type ScanResult struct {
	Cursor uint64
	Keys   []string
}

// Scan 实现 SCAN 命令，迭代数据库中的键
func (s *BotreonStore) Scan(cursor uint64, pattern string, count int) (ScanResult, error) {
	var result ScanResult
	result.Cursor = 0
	result.Keys = []string{}

	if count <= 0 {
		count = 10 // 默认值
	}

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := prefixKeyTypeBytes
		currentPos := uint64(0)
		collected := 0

		// 如果cursor不为0，需要跳过前面的键
		if cursor > 0 {
			// 简单实现：从头开始迭代，跳过cursor个键
			for iter.Seek(prefix); iter.ValidForPrefix(prefix) && currentPos < cursor; iter.Next() {
				currentPos++
			}
		} else {
			iter.Seek(prefix)
		}

		// 收集匹配的键
		for iter.ValidForPrefix(prefix) && collected < count {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			key := string(keyBytes[len(prefixKeyTypeBytes):])

			if pattern == "" || pattern == "*" || matchPattern(key, pattern) {
				result.Keys = append(result.Keys, key)
				collected++
			}

			currentPos++
			iter.Next()
		}

		// 检查是否还有更多键
		if iter.ValidForPrefix(prefix) {
			result.Cursor = currentPos
		} else {
			result.Cursor = 0 // 0表示迭代完成
		}

		return nil
	})
	return result, err
}

// Keys 实现 Keys 命令，返回匹配模式的键列表
func (s *BotreonStore) Keys(pattern string) ([]string, error) {
	var keys []string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		// 查找所有TYPE_前缀的键
		prefix := prefixKeyTypeBytes
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)
			// 提取实际键名（去掉TYPE_前缀）
			key := string(keyBytes[len(prefixKeyTypeBytes):])
			if matchPattern(key, pattern) {
				keys = append(keys, key)
			}
		}
		return nil
	})
	return keys, err
}

// matchPattern 实现简单的通配符匹配（*和?）
func matchPattern(key, pattern string) bool {
	// 简单的通配符匹配实现
	if pattern == "*" {
		return true
	}
	// 使用简单的字符串匹配
	keyRunes := []rune(key)
	patternRunes := []rune(pattern)

	keyIdx := 0
	patternIdx := 0
	keyStar := -1
	patternStar := -1

	for keyIdx < len(keyRunes) || patternIdx < len(patternRunes) {
		if patternIdx < len(patternRunes) && patternRunes[patternIdx] == '*' {
			keyStar = keyIdx
			patternStar = patternIdx
			patternIdx++
			continue
		}
		if keyIdx < len(keyRunes) && patternIdx < len(patternRunes) &&
			(patternRunes[patternIdx] == '?' || patternRunes[patternIdx] == keyRunes[keyIdx]) {
			keyIdx++
			patternIdx++
			continue
		}
		if keyStar >= 0 {
			// If keyStar has reached/exceeded key length, no more backtracking possible
			if keyStar >= len(keyRunes) {
				return false
			}
			keyStar++
			keyIdx = keyStar
			patternIdx = patternStar + 1
			continue
		}
		return false
	}

	// 处理pattern末尾的*
	for patternIdx < len(patternRunes) && patternRunes[patternIdx] == '*' {
		patternIdx++
	}

	return patternIdx == len(patternRunes)
}

// RandomKey 实现 RANDOMKEY 命令
func (s *BotreonStore) RandomKey() (string, error) {
	var key string
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := prefixKeyTypeBytes

		// 先计算总数
		count := 0
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			count++
		}

		if count == 0 {
			return nil // 没有键
		}

		// 随机选择一个位置
		randPos := 0
		if count > 1 {
			// 使用简单的伪随机
			randPos = int(time.Now().UnixNano() % int64(count))
		}

		// 迭代到随机位置
		iter.Rewind()
		current := 0
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			if current == randPos {
				item := iter.Item()
				keyBytes := item.KeyCopy(nil)
				key = string(keyBytes[len(prefixKeyTypeBytes):])
				return nil
			}
			current++
		}

		return nil
	})
	return key, err
}

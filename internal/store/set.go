package store

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/helper"
)

// randomFloat64 生成 [0, 1) 范围的随机浮点数
func randomFloat64() float64 {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	// 将字节转换为 [0, 1) 范围的浮点数
	return float64(binary.BigEndian.Uint64(b)) / (1 << 64)
}

func (s *BotreonStore) retryUpdate(fn func(*badger.Txn) error, maxRetries int) error {
	// 主动背压：在进入 retry 循环前检查 L0 状态
	if s.bpConfig.Enabled {
		delay, reject := s.preWriteCheck()
		if reject {
			s.retryMu.Lock()
			s.retryMetrics.l0Rejected++
			s.retryMu.Unlock()
			return fmt.Errorf("write rejected: L0 score %.1f exceeds hard threshold %.0f",
				s.l0ScoreCached(), s.bpConfig.L0HardThreshold)
		}
		if delay > 0 {
			s.retryMu.Lock()
			s.retryMetrics.l0Delayed++
			s.retryMu.Unlock()
			time.Sleep(delay)
		}
	}

	// 限流：防止 retry goroutine 雪崩
	s.backpressure.Acquire()
	defer s.backpressure.Release()

	s.retryMu.Lock()
	s.retryMetrics.activeRetries++
	s.retryMu.Unlock()

	var err error
	for i := 0; i < maxRetries; i++ {
		err = s.db.Update(fn)
		if err == nil {
			s.retryMu.Lock()
			s.retryMetrics.activeRetries--
			s.retryMu.Unlock()
			return nil
		}
		errStr := err.Error()
		if strings.Contains(errStr, "Transaction Conflict") ||
			strings.Contains(errStr, "conflict") ||
			strings.Contains(errStr, "Conflict") {
			baseBackoff := time.Duration(1<<uint(i)) * time.Millisecond
			if baseBackoff > 50*time.Millisecond {
				baseBackoff = 50 * time.Millisecond
			}
			jitter := time.Duration(randomFloat64() * float64(baseBackoff) * 0.5)
			backoff := baseBackoff + jitter
			time.Sleep(backoff)
			continue
		}
		if strings.Contains(errStr, "Writes are blocked") {
			s.retryMu.Lock()
			s.retryMetrics.writesBlocked++
			s.retryMu.Unlock()
			baseBackoff := time.Duration(1<<uint(i)) * time.Millisecond
			if baseBackoff > 2*time.Second {
				baseBackoff = 2 * time.Second
			}
			jitter := time.Duration(randomFloat64() * float64(baseBackoff) * 0.2)
			backoff := baseBackoff + jitter
			time.Sleep(backoff)
			continue
		}
		s.retryMu.Lock()
		s.retryMetrics.activeRetries--
		s.retryMu.Unlock()
		return err
	}
	s.retryMu.Lock()
	s.retryMetrics.activeRetries--
	s.retryMu.Unlock()
	return fmt.Errorf("max retries exhausted (%d): %w", maxRetries, err)
}

// setKey 方法用于生成存储在 Badger 数据库中的键
func (s *BotreonStore) setKey(key string, parts ...string) string {
	base := []string{KeyTypeSet, key}
	base = append(base, parts...)
	return strings.Join(base, ":")
}

// SAdd 实现 Redis SADD 命令
func (s *BotreonStore) SAdd(key string, members ...string) (int, error) {
	added := 0
	err := s.retryUpdate(func(txn *badger.Txn) error {
		badgerTypeKey := TypeOfKeyGet(key)

		// Check if key already exists with a different type
		item, err := txn.Get(badgerTypeKey)
		if err == nil {
			// Key exists, check its type
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeSet {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}

		if err := txn.Set(badgerTypeKey, []byte(KeyTypeSet)); err != nil {
			return err
		}
		countKey := s.setKey(key, "count")
		var count uint64

		// 获取当前计数器值
		item, err = txn.Get([]byte(countKey))
		if err != badger.ErrKeyNotFound {
			if err != nil {
				return err
			}
			countBytes, _ := item.ValueCopy(nil)
			count = helper.BytesToUint64(countBytes)
		}

		added = 0
		for _, member := range members {
			memberKey := s.setKey(key, "member", member)

			// 检查成员是否存在
			if _, err := txn.Get([]byte(memberKey)); err == badger.ErrKeyNotFound {
				// 新成员：写入成员键并增加计数器
				if err := txn.Set([]byte(memberKey), []byte{}); err != nil {
					return err
				}
				count++
				added++
			}
		}

		// 更新计数器
		if added > 0 {
			return txn.Set([]byte(countKey), helper.Uint64ToBytes(count))
		}
		return nil
	}, 30) // 最多重试 30 次（高并发时需要更多重试）
	return added, err
}

// SRem 实现 Redis SREM 命令
func (s *BotreonStore) SRem(key string, members ...string) (int, error) {
	removed := 0
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// Check if key already exists with a different type
		typeKey := TypeOfKeyGet(key)
		typeItem, typeErr := txn.Get(typeKey)
		if typeErr == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeSet {
				return ErrWrongType
			}
		} else if !errors.Is(typeErr, badger.ErrKeyNotFound) {
			return typeErr
		}

		countKey := s.setKey(key, "count")
		var count uint64

		// 获取当前计数器值
		item, err := txn.Get([]byte(countKey))
		if !errors.Is(err, badger.ErrKeyNotFound) {
			if err != nil {
				return err
			}
			countBytes, _ := item.ValueCopy(nil)
			count = helper.BytesToUint64(countBytes)
		}

		removed = 0
		for _, member := range members {
			memberKey := s.setKey(key, "member", member)

			// 检查成员是否存在
			if _, err := txn.Get([]byte(memberKey)); err == nil {
				if err := txn.Delete([]byte(memberKey)); err != nil {
					return err
				}
				count--
				removed++
			}
		}

		// 更新计数器（即使计数器为0也要保留）
		if removed > 0 {
			return txn.Set([]byte(countKey), helper.Uint64ToBytes(count))
		}
		return nil
	}, 30) // 最多重试 30 次（高并发时需要更多重试）
	return removed, err
}

// SCard 实现 Redis SCARD 命令
func (s *BotreonStore) SCard(key string) (uint64, error) {
	var count uint64
	err := s.db.View(func(txn *badger.Txn) error {
		// Check if key already exists with a different type
		typeKey := TypeOfKeyGet(key)
		typeItem, typeErr := txn.Get(typeKey)
		if typeErr == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeSet {
				return ErrWrongType
			}
		} else if !errors.Is(typeErr, badger.ErrKeyNotFound) {
			return typeErr
		}

		countKey := s.setKey(key, "count")
		item, err := txn.Get([]byte(countKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		countBytes, _ := item.ValueCopy(nil)
		count = helper.BytesToUint64(countBytes)
		return nil
	})
	return count, err
}

// SIsMember 实现 Redis SISMEMBER 命令
func (s *BotreonStore) SIsMember(key string, member string) (bool, error) {
	exists := false
	err := s.db.View(func(txn *badger.Txn) error {
		// Check if key already exists with a different type
		typeKey := TypeOfKeyGet(key)
		typeItem, typeErr := txn.Get(typeKey)
		if typeErr == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeSet {
				return ErrWrongType
			}
		} else if !errors.Is(typeErr, badger.ErrKeyNotFound) {
			return typeErr
		}

		memberKey := s.setKey(key, "member", member)
		_, err := txn.Get([]byte(memberKey))
		if err == nil {
			exists = true
			return nil
		}
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return err
	})
	return exists, err
}

// getAllMembers 获取集合中的所有成员
func (s *BotreonStore) getAllMembers(txn *badger.Txn, key string) ([]string, error) {
	var members []string
	prefix := s.setKey(key, "member")
	prefixBytes := []byte(prefix + ":")
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	for iter.Seek(prefixBytes); iter.ValidForPrefix(prefixBytes); iter.Next() {
		k := iter.Item().Key()
		kStr := string(k)
		// 提取成员名：SET:key:member:memberName
		// 移除前缀，获取成员名
		if strings.HasPrefix(kStr, prefix+":") {
			member := kStr[len(prefix)+1:] // 跳过 "SET:key:member:" 前缀
			members = append(members, member)
		}
	}
	return members, nil
}

// SMembers 实现 Redis SMEMBERS 命令
func (s *BotreonStore) SMembers(key string) ([]string, error) {
	var members []string
	err := s.db.View(func(txn *badger.Txn) error {
		// Check if key already exists with a different type
		typeKey := TypeOfKeyGet(key)
		typeItem, typeErr := txn.Get(typeKey)
		if typeErr == nil {
			typeVal, err := typeItem.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(typeVal)
			if keyType != "" && keyType != KeyTypeSet {
				return ErrWrongType
			}
		} else if !errors.Is(typeErr, badger.ErrKeyNotFound) {
			return typeErr
		}

		var err error
		members, err = s.getAllMembers(txn, key)
		return err
	})
	return members, err
}

// SPop 实现 Redis SPOP 命令，随机弹出并删除一个成员
// 优化：使用迭代器随机选择，避免加载所有成员
func (s *BotreonStore) SPop(key string) (string, error) {
	var member string
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// 先获取集合大小
		countKey := s.setKey(key, "count")
		item, err := txn.Get([]byte(countKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 集合为空
		}
		if err != nil {
			return err
		}
		countBytes, _ := item.ValueCopy(nil)
		count := helper.BytesToUint64(countBytes)
		if count == 0 {
			return nil // 集合为空
		}

		// 随机选择一个索引（0 到 count-1）
		// #nosec G115 - count is bounded by practical set size limits
		targetIndex := randomIntn(int(count))

		// 使用迭代器遍历到目标索引位置
		prefix := s.setKey(key, "member")
		prefixBytes := []byte(prefix + ":")
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		currentIndex := 0
		for iter.Seek(prefixBytes); iter.ValidForPrefix(prefixBytes); iter.Next() {
			if currentIndex == targetIndex {
				// 找到目标成员
				k := iter.Item().Key()
				kStr := string(k)
				// 提取成员名：SET:key:member:memberName
				if strings.HasPrefix(kStr, prefix+":") {
					member = kStr[len(prefix)+1:] // 跳过 "SET:key:member:" 前缀

					// 删除成员
					if err := txn.Delete(k); err != nil {
						return err
					}

					// 更新计数器
					count--
					return txn.Set([]byte(countKey), helper.Uint64ToBytes(count))
				}
			}
			currentIndex++
		}

		// 如果迭代器没有找到（理论上不应该发生），回退到旧方法
		// 但这种情况应该很少见
		return nil
	}, 30) // 最多重试 30 次（高并发时需要更多重试）
	return member, err
}

// SPopN 实现 Redis SPOP 命令（带count参数），随机弹出并删除多个成员
func (s *BotreonStore) SPopN(key string, count int) ([]string, error) {
	var members []string
	err := s.retryUpdate(func(txn *badger.Txn) error {
		allMembers, err := s.getAllMembers(txn, key)
		if err != nil {
			return err
		}
		if len(allMembers) == 0 {
			return nil
		}

		// 限制count不超过集合大小
		if count > len(allMembers) {
			count = len(allMembers)
		}
		if count < 0 {
			count = 0
		}

		// 随机选择成员
		randomShuffle(len(allMembers), func(i, j int) {
			allMembers[i], allMembers[j] = allMembers[j], allMembers[i]
		})

		// 删除选中的成员
		for i := 0; i < count; i++ {
			member := allMembers[i]
			memberKey := s.setKey(key, "member", member)
			if err := txn.Delete([]byte(memberKey)); err != nil {
				return err
			}
			members = append(members, member)
		}

		// 更新计数器
		if count > 0 {
			countKey := s.setKey(key, "count")
			item, err := txn.Get([]byte(countKey))
			if err == nil {
				countBytes, _ := item.ValueCopy(nil)
				currentCount := helper.BytesToUint64(countBytes)
				// #nosec G115 - count is bounded by practical set size limits
				if currentCount >= uint64(count) {
					currentCount -= uint64(count)
					return txn.Set([]byte(countKey), helper.Uint64ToBytes(currentCount))
				}
			}
		}
		return nil
	}, 30) // 最多重试 30 次（高并发时需要更多重试）
	return members, err
}

// SRandMember 实现 Redis SRANDMEMBER 命令，随机获取一个成员（不删除）
func (s *BotreonStore) SRandMember(key string) (string, error) {
	var member string
	err := s.db.View(func(txn *badger.Txn) error {
		members, err := s.getAllMembers(txn, key)
		if err != nil {
			return err
		}
		if len(members) == 0 {
			return nil
		}

		// 随机选择一个成员
		index := randomIntn(len(members))
		member = members[index]
		return nil
	})
	return member, err
}

// SRandMemberN 实现 Redis SRANDMEMBER 命令（带count参数），随机获取多个成员（不删除）
func (s *BotreonStore) SRandMemberN(key string, count int) ([]string, error) {
	var members []string
	err := s.db.View(func(txn *badger.Txn) error {
		allMembers, err := s.getAllMembers(txn, key)
		if err != nil {
			return err
		}
		if len(allMembers) == 0 {
			return nil
		}

		// 如果count为负数，允许重复
		if count < 0 {
			count = -count
			for i := 0; i < count; i++ {
				index := randomIntn(len(allMembers))
				members = append(members, allMembers[index])
			}
		} else {
			// 限制count不超过集合大小
			if count > len(allMembers) {
				count = len(allMembers)
			}
			// 随机选择不重复的成员
			randomShuffle(len(allMembers), func(i, j int) {
				allMembers[i], allMembers[j] = allMembers[j], allMembers[i]
			})
			members = allMembers[:count]
		}
		return nil
	})
	return members, err
}

// SMove 实现 Redis SMOVE 命令，将成员从源集合移动到目标集合
func (s *BotreonStore) SMove(source, destination, member string) (bool, error) {
	moved := false
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// 检查成员是否在源集合中
		sourceMemberKey := s.setKey(source, "member", member)
		_, err := txn.Get([]byte(sourceMemberKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil // 成员不存在，返回false
		}
		if err != nil {
			return err
		}

		// 检查成员是否已在目标集合中
		destMemberKey := s.setKey(destination, "member", member)
		_, err = txn.Get([]byte(destMemberKey))
		alreadyInDest := err == nil

		// 从源集合删除
		if err := txn.Delete([]byte(sourceMemberKey)); err != nil {
			return err
		}

		// 更新源集合计数器
		sourceCountKey := s.setKey(source, "count")
		item, err := txn.Get([]byte(sourceCountKey))
		if err == nil {
			countBytes, _ := item.ValueCopy(nil)
			count := helper.BytesToUint64(countBytes)
			if count > 0 {
				count--
				if err := txn.Set([]byte(sourceCountKey), helper.Uint64ToBytes(count)); err != nil {
					return err
				}
			}
		}

		// 如果成员不在目标集合中，添加到目标集合
		if !alreadyInDest {
			if err := txn.Set(TypeOfKeyGet(destination), []byte(KeyTypeSet)); err != nil {
				return err
			}
			if err := txn.Set([]byte(destMemberKey), []byte{}); err != nil {
				return err
			}

			// 更新目标集合计数器
			destCountKey := s.setKey(destination, "count")
			item, err = txn.Get([]byte(destCountKey))
			var destCount uint64
			if err == nil {
				countBytes, _ := item.ValueCopy(nil)
				destCount = helper.BytesToUint64(countBytes)
			}
			destCount++
			if err := txn.Set([]byte(destCountKey), helper.Uint64ToBytes(destCount)); err != nil {
				return err
			}
		}

		moved = true
		return nil
	}, 30)
	return moved, err
}

// SInter 实现 Redis SINTER 命令，计算多个集合的交集
func (s *BotreonStore) SInter(keys ...string) ([]string, error) {
	var result []string
	err := s.db.View(func(txn *badger.Txn) error {
		if len(keys) == 0 {
			return nil
		}

		// 获取第一个集合的所有成员
		firstMembers, err := s.getAllMembers(txn, keys[0])
		if err != nil {
			return err
		}
		if len(firstMembers) == 0 {
			return nil
		}

		// 检查每个成员是否在所有其他集合中
		for _, member := range firstMembers {
			inAll := true
			for i := 1; i < len(keys); i++ {
				memberKey := s.setKey(keys[i], "member", member)
				_, err := txn.Get([]byte(memberKey))
				if errors.Is(err, badger.ErrKeyNotFound) {
					inAll = false
					break
				}
				if err != nil {
					return err
				}
			}
			if inAll {
				result = append(result, member)
			}
		}
		return nil
	})
	return result, err
}

// SUnion 实现 Redis SUNION 命令，计算多个集合的并集
func (s *BotreonStore) SUnion(keys ...string) ([]string, error) {
	var result []string
	seen := make(map[string]bool)
	err := s.db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			members, err := s.getAllMembers(txn, key)
			if err != nil {
				return err
			}
			for _, member := range members {
				if !seen[member] {
					seen[member] = true
					result = append(result, member)
				}
			}
		}
		return nil
	})
	return result, err
}

// SDiff 实现 Redis SDIFF 命令，计算第一个集合与其他集合的差集
func (s *BotreonStore) SDiff(keys ...string) ([]string, error) {
	var result []string
	err := s.db.View(func(txn *badger.Txn) error {
		if len(keys) == 0 {
			return nil
		}

		// 获取第一个集合的所有成员
		firstMembers, err := s.getAllMembers(txn, keys[0])
		if err != nil {
			return err
		}

		// 构建其他集合的成员集合
		otherMembers := make(map[string]bool)
		for i := 1; i < len(keys); i++ {
			members, err := s.getAllMembers(txn, keys[i])
			if err != nil {
				return err
			}
			for _, member := range members {
				otherMembers[member] = true
			}
		}

		// 找出只在第一个集合中的成员
		for _, member := range firstMembers {
			if !otherMembers[member] {
				result = append(result, member)
			}
		}
		return nil
	})
	return result, err
}

// SInterStore 实现 Redis SINTERSTORE 命令，计算交集并存储到目标集合
func (s *BotreonStore) SInterStore(destination string, keys ...string) (int, error) {
	var count int
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// 在事务中计算交集
		var result []string
		if len(keys) > 0 {
			// 获取第一个集合的所有成员
			firstMembers, err := s.getAllMembers(txn, keys[0])
			if err != nil {
				return err
			}

			// 检查每个成员是否在所有其他集合中
			for _, member := range firstMembers {
				inAll := true
				for i := 1; i < len(keys); i++ {
					memberKey := s.setKey(keys[i], "member", member)
					_, err := txn.Get([]byte(memberKey))
					if errors.Is(err, badger.ErrKeyNotFound) {
						inAll = false
						break
					}
					if err != nil {
						return err
					}
				}
				if inAll {
					result = append(result, member)
				}
			}
		}

		// 删除目标集合的现有成员
		existingMembers, err := s.getAllMembers(txn, destination)
		if err == nil {
			for _, member := range existingMembers {
				memberKey := s.setKey(destination, "member", member)
				_ = txn.Delete([]byte(memberKey))
			}
		}

		// 添加交集结果到目标集合
		if len(result) > 0 {
			if err := txn.Set(TypeOfKeyGet(destination), []byte(KeyTypeSet)); err != nil {
				return err
			}
			for _, member := range result {
				memberKey := s.setKey(destination, "member", member)
				if err := txn.Set([]byte(memberKey), []byte{}); err != nil {
					return err
				}
			}
		}

		// 更新计数器
		count = len(result)
		countKey := s.setKey(destination, "count")
		// #nosec G115 - count is bounded by practical set size limits
		return txn.Set([]byte(countKey), helper.Uint64ToBytes(uint64(count)))
	}, 30)
	return count, err
}

// SUnionStore 实现 Redis SUNIONSTORE 命令，计算并集并存储到目标集合
func (s *BotreonStore) SUnionStore(destination string, keys ...string) (int, error) {
	var count int
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// 在事务中计算并集
		var result []string
		seen := make(map[string]bool)
		for _, key := range keys {
			members, err := s.getAllMembers(txn, key)
			if err != nil {
				return err
			}
			for _, member := range members {
				if !seen[member] {
					seen[member] = true
					result = append(result, member)
				}
			}
		}

		// 删除目标集合的现有成员
		existingMembers, err := s.getAllMembers(txn, destination)
		if err == nil {
			for _, member := range existingMembers {
				memberKey := s.setKey(destination, "member", member)
				_ = txn.Delete([]byte(memberKey))
			}
		}

		// 添加并集结果到目标集合
		if len(result) > 0 {
			if err := txn.Set(TypeOfKeyGet(destination), []byte(KeyTypeSet)); err != nil {
				return err
			}
			for _, member := range result {
				memberKey := s.setKey(destination, "member", member)
				if err := txn.Set([]byte(memberKey), []byte{}); err != nil {
					return err
				}
			}
		}

		// 更新计数器
		count = len(result)
		countKey := s.setKey(destination, "count")
		// #nosec G115 - count is bounded by practical set size limits
		return txn.Set([]byte(countKey), helper.Uint64ToBytes(uint64(count)))
	}, 30)
	return count, err
}

// SDiffStore 实现 Redis SDIFFSTORE 命令，计算差集并存储到目标集合
func (s *BotreonStore) SDiffStore(destination string, keys ...string) (int, error) {
	var count int
	err := s.retryUpdate(func(txn *badger.Txn) error {
		// 在事务中计算差集
		var result []string
		if len(keys) > 0 {
			// 获取第一个集合的所有成员
			firstMembers, err := s.getAllMembers(txn, keys[0])
			if err != nil {
				return err
			}

			// 构建其他集合的成员集合
			otherMembers := make(map[string]bool)
			for i := 1; i < len(keys); i++ {
				members, err := s.getAllMembers(txn, keys[i])
				if err != nil {
					return err
				}
				for _, member := range members {
					otherMembers[member] = true
				}
			}

			// 找出只在第一个集合中的成员
			for _, member := range firstMembers {
				if !otherMembers[member] {
					result = append(result, member)
				}
			}
		}

		// 删除目标集合的现有成员
		existingMembers, err := s.getAllMembers(txn, destination)
		if err == nil {
			for _, member := range existingMembers {
				memberKey := s.setKey(destination, "member", member)
				_ = txn.Delete([]byte(memberKey))
			}
		}

		// 添加差集结果到目标集合
		if len(result) > 0 {
			if err := txn.Set(TypeOfKeyGet(destination), []byte(KeyTypeSet)); err != nil {
				return err
			}
			for _, member := range result {
				memberKey := s.setKey(destination, "member", member)
				if err := txn.Set([]byte(memberKey), []byte{}); err != nil {
					return err
				}
			}
		}

		// 更新计数器
		count = len(result)
		countKey := s.setKey(destination, "count")
		// #nosec G115 - count is bounded by practical set size limits
		return txn.Set([]byte(countKey), helper.Uint64ToBytes(uint64(count)))
	}, 30)
	return count, err
}

// SMIsMember 实现 Redis SMISMEMBER 命令，检查多个成员是否在集合中
func (s *BotreonStore) SMIsMember(key string, members ...string) ([]int64, error) {
	results := make([]int64, len(members))
	err := s.db.View(func(txn *badger.Txn) error {
		for i, member := range members {
			memberKey := s.setKey(key, "member", member)
			_, err := txn.Get([]byte(memberKey))
			if errors.Is(err, badger.ErrKeyNotFound) {
				results[i] = 0
			} else if err != nil {
				return err
			} else {
				results[i] = 1
			}
		}
		return nil
	})
	return results, err
}

// SInterCard 实现 Redis SINTERCARD 命令，返回多个集合的交集基数
func (s *BotreonStore) SInterCard(keys ...string) (int64, error) {
	var count int64
	err := s.db.View(func(txn *badger.Txn) error {
		if len(keys) == 0 {
			return nil
		}

		// 获取第一个集合的所有成员
		firstMembers, err := s.getAllMembers(txn, keys[0])
		if err != nil {
			return err
		}
		if len(firstMembers) == 0 {
			return nil
		}

		// 统计在所有集合中都存在的成员
		for _, member := range firstMembers {
			inAll := true
			for i := 1; i < len(keys); i++ {
				memberKey := s.setKey(keys[i], "member", member)
				_, err := txn.Get([]byte(memberKey))
				if errors.Is(err, badger.ErrKeyNotFound) {
					inAll = false
					break
				}
				if err != nil {
					return err
				}
			}
			if inAll {
				count++
			}
		}
		return nil
	})
	return count, err
}

// SScanResult 定义 SSCAN 命令的返回结果
type SScanResult struct {
	Cursor  uint64
	Members []string
}

// SScan 实现 Redis SSCAN 命令，增量迭代集合的成员
func (s *BotreonStore) SScan(key string, cursor uint64, pattern string, count int) (SScanResult, error) {
	var result SScanResult
	result.Cursor = 0
	result.Members = []string{}

	if count <= 0 {
		count = 10 // 默认值
	}

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		prefix := []byte(KeyTypeSet + ":" + key + ":member:")
		opts.Prefix = prefix
		opts.PrefetchValues = false

		iter := txn.NewIterator(opts)
		defer iter.Close()

		currentPos := uint64(0)
		collected := 0

		// 如果cursor不为0，需要跳过前面的成员
		if cursor > 0 {
			for iter.Seek(prefix); iter.ValidForPrefix(prefix) && currentPos < cursor; iter.Next() {
				currentPos++
			}
		} else {
			iter.Seek(prefix)
		}

		// 收集匹配的成员
		for iter.ValidForPrefix(prefix) && collected < count {
			item := iter.Item()
			keyBytes := item.KeyCopy(nil)

			// 解析键格式: set:key:member:memberValue
			member := string(keyBytes[len(prefix):])

			if pattern == "" || pattern == "*" || matchPattern(member, pattern) {
				result.Members = append(result.Members, member)
				collected++
			}

			currentPos++
			iter.Next()
		}

		// 检查是否还有更多成员
		if iter.ValidForPrefix(prefix) {
			result.Cursor = currentPos
		} else {
			result.Cursor = 0 // 0表示迭代完成
		}

		return nil
	})
	return result, err
}

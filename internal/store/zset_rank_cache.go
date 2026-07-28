package store

import (
	"sort"

	"github.com/dgraph-io/badger/v4"
)

// getZSetRankCache 返回指定 zset 的 rank cache（如不存在则创建空缓存，标记 dirty）。
// 新缓存标记 dirty=true，保证首次 ZRANK 时从 BadgerDB 重建。
// 调用方应持读锁（zsetRankMu.RLock）或写锁。
func (s *BotreonStore) getZSetRankCache(zsetName string) *zsetRankCache {
	c, ok := s.zsetRankCaches[zsetName]
	if !ok {
		c = &zsetRankCache{
			dirty:   true,
			members: make(map[string]int64),
			scores:  make(map[string]float64),
		}
		s.zsetRankCaches[zsetName] = c
	}
	return c
}

// markZSetDirty 标记 zset 的 rank cache 为 dirty，下次 ZRANK 时重建。
func (s *BotreonStore) markZSetDirty(zsetName string) {
	s.zsetRankMu.RLock()
	c, ok := s.zsetRankCaches[zsetName]
	s.zsetRankMu.RUnlock()
	if !ok {
		return
	}
	c.mu.Lock()
	c.dirty = true
	c.mu.Unlock()
}

// buildZSetRankCache 从 BadgerDB 扫描构建 zset 的 rank cache。
// 调用方必须已获取 c.mu 写锁。
func (s *BotreonStore) buildZSetRankCache(txn *badger.Txn, c *zsetRankCache, zsetName string) {
	prefix := keyBadgerGet(prefixKeySortedSetBytes, []byte(zsetName+sortedSetIndex))
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false

	type entry struct {
		member string
		score  float64
	}
	var entries []entry

	it := txn.NewIterator(opts)
	defer it.Close()

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		memberScore, memberName, _, ok := parseZSetIndexKey(it.Item().Key(), prefix)
		if !ok {
			continue
		}
		entries = append(entries, entry{member: memberName, score: float64(memberScore)})
	}

	// Sort by (score, member) — same order as ZRANK
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].score != entries[j].score {
			return entries[i].score < entries[j].score
		}
		return entries[i].member < entries[j].member
	})

	c.members = make(map[string]int64, len(entries))
	c.scores = make(map[string]float64, len(entries))
	for rank, e := range entries {
		c.members[e.member] = int64(rank)
		c.scores[e.member] = e.score
	}
	c.dirty = false
}

// clearAllZSetRankCaches 清空所有 zset rank cache（用于 FLUSHDB/FLUSHALL）。
func (s *BotreonStore) clearAllZSetRankCaches() {
	s.zsetRankMu.Lock()
	s.zsetRankCaches = make(map[string]*zsetRankCache)
	s.zsetRankMu.Unlock()
}

// getZSetRank 返回成员在 zset 中的前向排名（0-based）。
// 如果缓存不存在或 dirty，则从 BadgerDB 重建。
// 返回 -1 表示成员不存在。
func (s *BotreonStore) getZSetRank(zsetName, member string, score float64) (int64, error) {
	s.zsetRankMu.RLock()
	c, ok := s.zsetRankCaches[zsetName]
	s.zsetRankMu.RUnlock()

	if !ok {
		s.zsetRankMu.Lock()
		c = s.getZSetRankCache(zsetName)
		s.zsetRankMu.Unlock()
	}

	c.mu.RLock()
	dirty := c.dirty
	rank, hasMember := c.members[member]
	c.mu.RUnlock()

	if !dirty && hasMember {
		return rank, nil
	}
	if !dirty && !hasMember {
		return -1, nil
	}

	// Rebuild from BadgerDB
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.dirty {
		if r, ok := c.members[member]; ok {
			return r, nil
		}
		return -1, nil
	}

	err := s.db.View(func(txn *badger.Txn) error {
		s.buildZSetRankCache(txn, c, zsetName)
		return nil
	})
	if err != nil {
		return -1, err
	}

	if r, ok := c.members[member]; ok {
		return r, nil
	}
	return -1, nil
}

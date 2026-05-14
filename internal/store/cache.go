package store

import (
	"sync"
	"time"
)

// CacheEntry 缓存条目
type CacheEntry struct {
	Value     []byte
	ExpiresAt time.Time
}

// IsExpired 检查是否过期
func (e *CacheEntry) IsExpired() bool {
	return !e.ExpiresAt.IsZero() && time.Now().After(e.ExpiresAt)
}

// LRUCache LRU 缓存实现
type LRUCache struct {
	mu          sync.RWMutex
	cache       map[string]*CacheEntry
	maxSize     int
	ttl         time.Duration
	accessOrder []string // 用于 LRU 排序
}

// NewLRUCache 创建新的 LRU 缓存
func NewLRUCache(maxSize int, ttl time.Duration) *LRUCache {
	return &LRUCache{
		cache:       make(map[string]*CacheEntry, maxSize),
		maxSize:     maxSize,
		ttl:         ttl,
		accessOrder: make([]string, 0, maxSize),
	}
}

func (c *LRUCache) Get(key string) ([]byte, bool) {
	c.mu.RLock()
	entry, exists := c.cache[key]
	if !exists {
		c.mu.RUnlock()
		return nil, false
	}

	if entry.IsExpired() {
		c.mu.RUnlock()
		c.Delete(key)
		return nil, false
	}

	value := entry.Value
	c.mu.RUnlock()

	c.mu.Lock()
	c.updateAccessOrder(key)
	c.mu.Unlock()

	return value, true
}

// Set 设置缓存值
func (c *LRUCache) Set(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果已存在，更新
	if _, exists := c.cache[key]; exists {
		c.cache[key].Value = value
		if c.ttl > 0 {
			c.cache[key].ExpiresAt = time.Now().Add(c.ttl)
		}
		c.updateAccessOrder(key)
		return
	}

	// 如果缓存已满，删除最久未使用的项
	if len(c.cache) >= c.maxSize {
		c.evictLRU()
	}

	// 添加新项
	entry := &CacheEntry{
		Value: value,
	}
	if c.ttl > 0 {
		entry.ExpiresAt = time.Now().Add(c.ttl)
	}
	c.cache[key] = entry
	c.accessOrder = append(c.accessOrder, key)
}

// Delete 删除缓存项
func (c *LRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delete(key)
}

// delete 内部删除方法（不加锁）
func (c *LRUCache) delete(key string) {
	if _, exists := c.cache[key]; exists {
		delete(c.cache, key)
		// 从访问顺序中删除
		for i, k := range c.accessOrder {
			if k == key {
				c.accessOrder = append(c.accessOrder[:i], c.accessOrder[i+1:]...)
				break
			}
		}
	}
}

// updateAccessOrder 更新访问顺序
func (c *LRUCache) updateAccessOrder(key string) {
	// 从当前位置删除
	for i, k := range c.accessOrder {
		if k == key {
			// 安全地删除元素
			if i < len(c.accessOrder)-1 {
				c.accessOrder = append(c.accessOrder[:i], c.accessOrder[i+1:]...)
			} else {
				c.accessOrder = c.accessOrder[:i]
			}
			break
		}
	}
	// 添加到末尾（最近使用）
	c.accessOrder = append(c.accessOrder, key)
}

// evictLRU 删除最久未使用的项
func (c *LRUCache) evictLRU() {
	if len(c.accessOrder) == 0 {
		return
	}
	// 删除第一个（最久未使用）
	oldestKey := c.accessOrder[0]
	c.accessOrder = c.accessOrder[1:]
	delete(c.cache, oldestKey)
}

// Clear 清空缓存
func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*CacheEntry, c.maxSize)
	c.accessOrder = make([]string, 0, c.maxSize)
}

// Size 返回当前缓存大小
func (c *LRUCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

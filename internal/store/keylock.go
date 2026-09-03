package store

import (
	"hash/fnv"
	"sort"
	"sync"
)

type KeyLockManager struct {
	shards   int
	keyLocks []sync.RWMutex
}

func NewKeyLockManager(shards int) *KeyLockManager {
	if shards <= 0 {
		shards = 256
	}
	return &KeyLockManager{
		shards:   shards,
		keyLocks: make([]sync.RWMutex, shards),
	}
}

// getShard 获取 key 对应的分片索引
func (klm *KeyLockManager) getShard(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32() % uint32(klm.shards)
}

// Lock 获取 key 的写锁
func (klm *KeyLockManager) Lock(key string) {
	shard := klm.getShard(key)
	klm.keyLocks[shard].Lock()
}

// Unlock 释放 key 的写锁
func (klm *KeyLockManager) Unlock(key string) {
	shard := klm.getShard(key)
	klm.keyLocks[shard].Unlock()
}

// RLock 获取 key 的读锁
func (klm *KeyLockManager) RLock(key string) {
	shard := klm.getShard(key)
	klm.keyLocks[shard].RLock()
}

// RUnlock 释放 key 的读锁
func (klm *KeyLockManager) RUnlock(key string) {
	shard := klm.getShard(key)
	klm.keyLocks[shard].RUnlock()
}

// LockMulti 按分片序获取多个 key 的写锁并返回释放函数（防死锁：全排序获取——
// 不同线程以不同顺序请求同一组 key 时，按同一分片序排队即不死锁；同分片 key
// 去重——不自死锁）。多 key 写命令（RENAME/S*STORE/Z*STORE/RPOPLPUSH/MSETNX/
// PFMERGE/BITOP 等）必须用它而非逐 key Lock。
func (klm *KeyLockManager) LockMulti(keys []string) func() {
	seen := make(map[uint32]struct{}, len(keys))
	shards := make([]uint32, 0, len(keys))
	for _, key := range keys {
		s := klm.getShard(key)
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		shards = append(shards, s)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	for _, s := range shards {
		klm.keyLocks[s].Lock()
	}
	return func() {
		for i := len(shards) - 1; i >= 0; i-- {
			klm.keyLocks[shards[i]].Unlock()
		}
	}
}

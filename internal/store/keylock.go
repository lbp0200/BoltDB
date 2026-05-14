package store

import (
	"hash/fnv"
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

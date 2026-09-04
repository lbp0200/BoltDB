package store

import (
	"errors"
	"strconv"

	"github.com/dgraph-io/badger/v4"
)

func (s *BotreonStore) LPush(key string, values ...string) (int, error) {
	if err := s.checkErrorInjector("LPush"); err != nil {
		return 0, err
	}
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var finalLength uint64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		item, err := txn.Get(TypeOfKeyGet(key))
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeList {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeList)); err != nil {
			return err
		}
		length, start, end, metaErr := s.listGetMetaTxn(txn, key)
		if metaErr != nil {
			return metaErr
		}
		for _, value := range values {
			nodeID, err := s.createNode(txn, key, []byte(value))
			if err != nil {
				return err
			}
			if length == 0 {
				start = nodeID
				end = nodeID
				if err := s.linkNodes(txn, key, nodeID, nodeID); err != nil {
					return err
				}
			} else {
				if err := s.linkNodes(txn, key, nodeID, start); err != nil {
					return err
				}
				if err := txn.Set([]byte(s.listKey(key, start, "prev")), []byte(nodeID)); err != nil {
					return err
				}
				start = nodeID
			}
			length++
		}
		finalLength = length
		return s.listUpdateMeta(txn, key, length, start, end)
	}, 30, encodePropagateStringArgs([]byte("LPUSH"), append([]string{key}, values...)))

	s.notifyBlockingPop(key, values[0])

	return int(finalLength), err
}

// RPush 实现 Redis RPUSH 命令
func (s *BotreonStore) RPush(key string, values ...string) (int, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var finalLength uint64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		item, err := txn.Get(TypeOfKeyGet(key))
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			keyType := string(val)
			if keyType != "" && keyType != KeyTypeList {
				return ErrWrongType
			}
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		if err := txn.Set(TypeOfKeyGet(key), []byte(KeyTypeList)); err != nil {
			return err
		}
		length, start, end, metaErr := s.listGetMetaTxn(txn, key)
		if metaErr != nil {
			return metaErr
		}
		for _, value := range values {
			nodeID, err := s.createNode(txn, key, []byte(value))
			if err != nil {
				return err
			}
			if length == 0 {
				start = nodeID
				end = nodeID
				if err := s.linkNodes(txn, key, nodeID, nodeID); err != nil {
					return err
				}
			} else {
				if err := s.linkNodes(txn, key, end, nodeID); err != nil {
					return err
				}
				if err := txn.Set([]byte(s.listKey(key, end, "next")), []byte(nodeID)); err != nil {
					return err
				}
				end = nodeID
			}
			length++
		}
		finalLength = length
		return s.listUpdateMeta(txn, key, length, start, end)
	}, 30, encodePropagateStringArgs([]byte("RPUSH"), append([]string{key}, values...)))

	s.notifyBlockingPop(key, values[0])

	return int(finalLength), err
}

// RPop 实现 Redis RPOP 命令
func (s *BotreonStore) RPop(key string) (string, error) {
	var value string
	err := s.checkTypeBeforeOp(key, KeyTypeList)
	if err != nil {
		return "", err
	}
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	err = s.retryUpdate(func(txn *badger.Txn) error {
		length, start, end, err := s.listGetMetaTxn(txn, key)
		if err != nil {
			if length == 0 {
				return nil
			}
			return err
		}
		if length == 0 {
			return nil
		}

		endNodeKey := s.listKey(key, end)
		item, err := txn.Get([]byte(endNodeKey))
		if err != nil {
			return err
		}
		valueBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		value = string(valueBytes)

		var newEnd string
		if length > 1 {
			newEndKey := s.listKey(key, end, "prev")
			item, err = txn.Get([]byte(newEndKey))
			if err != nil {
				return err
			}
			newEndVal, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			newEnd = string(newEndVal)
		} else {
			newEnd = ""
			start = ""
		}

		if length > 1 {
			if err := s.linkNodes(txn, key, newEnd, start); err != nil {
				return err
			}
		}

		if err := txn.Delete([]byte(endNodeKey)); err != nil {
			return err
		}
		if err := txn.Delete([]byte(s.listKey(key, end, "prev"))); err != nil {
			return err
		}
		if err := txn.Delete([]byte(s.listKey(key, end, "next"))); err != nil {
			return err
		}

		return s.listUpdateMeta(txn, key, length-1, start, newEnd)
	}, 30, encodePropagateCommand([]byte("RPOP"), []byte(key)))
	return value, err
}

// LPop 实现 Redis LPOP 命令
func (s *BotreonStore) LPop(key string) (string, error) {
	var value string
	err := s.checkTypeBeforeOp(key, KeyTypeList)
	if err != nil {
		return "", err
	}
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)
	err = s.retryUpdate(func(txn *badger.Txn) error {
		length, start, end, err := s.listGetMetaTxn(txn, key)
		if err != nil {
			if length == 0 {
				return nil
			}
			return err
		}
		if length == 0 {
			return nil
		}

		startNodeKey := s.listKey(key, start)
		item, err := txn.Get([]byte(startNodeKey))
		if err != nil {
			return err
		}
		valueBytes, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		value = string(valueBytes)

		var newStart string
		if length > 1 {
			newStartKey := s.listKey(key, start, "next")
			item, err = txn.Get([]byte(newStartKey))
			if err != nil {
				return err
			}
			newStartVal, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			newStart = string(newStartVal)
		} else {
			newStart = ""
			end = ""
		}

		if length > 1 {
			if err := s.linkNodes(txn, key, end, newStart); err != nil {
				return err
			}
		}

		if err := txn.Delete([]byte(startNodeKey)); err != nil {
			return err
		}
		if err := txn.Delete([]byte(s.listKey(key, start, "prev"))); err != nil {
			return err
		}
		if err := txn.Delete([]byte(s.listKey(key, start, "next"))); err != nil {
			return err
		}

		return s.listUpdateMeta(txn, key, length-1, newStart, end)
	}, 30, encodePropagateCommand([]byte("LPOP"), []byte(key)))
	return value, err
}

// LPUSHX 实现 Redis LPUSHX 命令
func (s *BotreonStore) LPUSHX(key string, values ...string) (int, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var finalLength uint64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		finalLength = 0
		length, pushErr := s.listPushXInTxn(txn, key, values, true)
		if pushErr != nil {
			return pushErr
		}
		finalLength = length
		return nil
	}, 30, encodePropagateStringArgs([]byte("LPUSHX"), append([]string{key}, values...)))
	if err == nil && finalLength > 0 && len(values) > 0 {
		s.notifyBlockingPop(key, values[0])
	}
	return int(finalLength), err
}

// RPUSHX 实现 Redis RPUSHX 命令
func (s *BotreonStore) RPUSHX(key string, values ...string) (int, error) {
	s.keyLockMgr.Lock(key)
	defer s.keyLockMgr.Unlock(key)

	var finalLength uint64
	err := s.retryUpdate(func(txn *badger.Txn) error {
		finalLength = 0
		length, pushErr := s.listPushXInTxn(txn, key, values, false)
		if pushErr != nil {
			return pushErr
		}
		finalLength = length
		return nil
	}, 30, encodePropagateStringArgs([]byte("RPUSHX"), append([]string{key}, values...)))
	if err == nil && finalLength > 0 && len(values) > 0 {
		s.notifyBlockingPop(key, values[len(values)-1])
	}
	return int(finalLength), err
}

// RPopLPush 实现 Redis RPOPLPUSH 命令
func (s *BotreonStore) RPopLPush(source, destination string) (string, error) {
	unlock := s.lockListKeysOrdered(source, destination)
	defer unlock()

	var value string
	err := s.retryUpdate(func(txn *badger.Txn) error {
		value = ""
		popped, err := s.listPopInTxn(txn, source, false)
		if err != nil {
			return err
		}
		if popped == "" {
			return nil
		}
		value = popped
		return s.listPushInTxn(txn, destination, value, true)
	}, 30, encodePropagateCommand([]byte("RPOPLPUSH"), []byte(source), []byte(destination)))
	return value, err
}

// LMPop 实现 Redis LMPOP 命令
func (s *BotreonStore) LMPop(keys []string, modifier string, count int) (string, []string, error) {
	for _, key := range keys {
		err := s.checkTypeBeforeOp(key, KeyTypeList)
		if err != nil {
			return "", nil, err
		}
		s.keyLockMgr.Lock(key)
		var results []string
		err = s.retryUpdate(func(txn *badger.Txn) error {
			length, start, end, err := s.listGetMetaTxn(txn, key)
			if err != nil {
				if length == 0 {
					return nil
				}
				return err
			}
			if length == 0 {
				return nil
			}

			currentStart := start
			currentEnd := end
			currentLength := length
			results = make([]string, 0, count)

			for i := 0; i < count && currentLength > 0; i++ {
				var nodeID, nextNodeID string
				if modifier == "LEFT" {
					nodeID = currentStart
					if currentLength > 1 {
						nextKey := s.listKey(key, nodeID, "next")
						item, err := txn.Get([]byte(nextKey))
						if err != nil {
							return err
						}
						nextVal, err := item.ValueCopy(nil)
						if err != nil {
							return err
						}
						nextNodeID = string(nextVal)
					}
				} else {
					nodeID = currentEnd
					if currentLength > 1 {
						prevKey := s.listKey(key, nodeID, "prev")
						item, err := txn.Get([]byte(prevKey))
						if err != nil {
							return err
						}
						prevVal, err := item.ValueCopy(nil)
						if err != nil {
							return err
						}
						nextNodeID = string(prevVal)
					}
				}

				nodeKey := s.listKey(key, nodeID)
				item, err := txn.Get([]byte(nodeKey))
				if err != nil {
					return err
				}
				valueBytes, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				results = append(results, string(valueBytes))

				if err := s.deleteNode(txn, key, nodeID); err != nil {
					return err
				}

				if modifier == "LEFT" {
					currentStart = nextNodeID
					if currentLength > 1 {
						if err := s.linkNodes(txn, key, end, currentStart); err != nil {
							return err
						}
					} else {
						currentStart = ""
						currentEnd = ""
					}
				} else {
					currentEnd = nextNodeID
					if currentLength > 1 {
						if err := s.linkNodes(txn, key, currentEnd, start); err != nil {
							return err
						}
					} else {
						currentStart = ""
						currentEnd = ""
					}
				}
				currentLength--
			}

			return s.listUpdateMeta(txn, key, currentLength, currentStart, currentEnd)
		}, 30, func() []byte {
			// D4 全重放：LMPOP key... <LEFT|RIGHT> count
			args := make([][]byte, 0, 1+len(keys)+2)
			args = append(args, []byte("LMPOP"))
			for _, k := range keys {
				args = append(args, []byte(k))
			}
			args = append(args, []byte(modifier), []byte(strconv.Itoa(count)))
			return encodePropagateCommand(args...)
		}())
		s.keyLockMgr.Unlock(key)
		if err != nil {
			return "", nil, err
		}
		if len(results) > 0 {
			return key, results, nil
		}
	}
	return "", nil, nil
}

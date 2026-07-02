package store

import (
	"errors"

	"github.com/dgraph-io/badger/v4"
)

// listPopInTxn pops one element from the head (fromLeft=true) or tail of a list.
func (s *BotreonStore) listPopInTxn(txn *badger.Txn, key string, fromLeft bool) (string, error) {
	length, start, end, err := s.listGetMetaTxn(txn, key)
	if err != nil || length == 0 {
		return "", nil
	}

	var nodeID, neighborID string
	if fromLeft {
		nodeID = start
		if length > 1 {
			item, err := txn.Get([]byte(s.listKey(key, nodeID, "next")))
			if err != nil {
				return "", err
			}
			nextVal, err := item.ValueCopy(nil)
			if err != nil {
				return "", err
			}
			neighborID = string(nextVal)
		}
	} else {
		nodeID = end
		if length > 1 {
			item, err := txn.Get([]byte(s.listKey(key, nodeID, "prev")))
			if err != nil {
				return "", err
			}
			prevVal, err := item.ValueCopy(nil)
			if err != nil {
				return "", err
			}
			neighborID = string(prevVal)
		}
	}

	item, err := txn.Get([]byte(s.listKey(key, nodeID)))
	if err != nil {
		return "", err
	}
	valueBytes, err := item.ValueCopy(nil)
	if err != nil {
		return "", err
	}
	value := string(valueBytes)

	if fromLeft {
		if length > 1 {
			if err := s.linkNodes(txn, key, end, neighborID); err != nil {
				return "", err
			}
			start = neighborID
		} else {
			start = ""
			end = ""
		}
	} else {
		if length > 1 {
			if err := s.linkNodes(txn, key, neighborID, start); err != nil {
				return "", err
			}
			end = neighborID
		} else {
			start = ""
			end = ""
		}
	}

	if err := s.deleteNode(txn, key, nodeID); err != nil {
		return "", err
	}
	if err := s.listUpdateMeta(txn, key, length-1, start, end); err != nil {
		return "", err
	}
	return value, nil
}

// listPushInTxn pushes one element to the head (toLeft=true) or tail of a list.
func (s *BotreonStore) listPushInTxn(txn *badger.Txn, key, value string, toLeft bool) error {
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

	length, start, end, err := s.listGetMetaTxn(txn, key)
	if err != nil {
		return err
	}

	newNodeID, err := s.createNode(txn, key, []byte(value))
	if err != nil {
		return err
	}

	if length == 0 {
		if err := s.linkNodes(txn, key, newNodeID, newNodeID); err != nil {
			return err
		}
		return s.listUpdateMeta(txn, key, 1, newNodeID, newNodeID)
	}

	if toLeft {
		if err := s.linkNodes(txn, key, newNodeID, start); err != nil {
			return err
		}
		if err := txn.Set([]byte(s.listKey(key, start, "prev")), []byte(newNodeID)); err != nil {
			return err
		}
		start = newNodeID
	} else {
		if err := s.linkNodes(txn, key, end, newNodeID); err != nil {
			return err
		}
		if err := txn.Set([]byte(s.listKey(key, end, "next")), []byte(newNodeID)); err != nil {
			return err
		}
		end = newNodeID
	}
	return s.listUpdateMeta(txn, key, length+1, start, end)
}

// listPushXInTxn pushes values only when the key already exists as a list.
func (s *BotreonStore) listPushXInTxn(txn *badger.Txn, key string, values []string, toLeft bool) (uint64, error) {
	typeKey := TypeOfKeyGet(key)
	item, err := txn.Get(typeKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return 0, err
	}
	if string(val) != KeyTypeList {
		return 0, ErrWrongType
	}

	length, start, end, err := s.listGetMetaTxn(txn, key)
	if err != nil {
		return 0, err
	}

	for _, value := range values {
		nodeID, err := s.createNode(txn, key, []byte(value))
		if err != nil {
			return 0, err
		}
		if length == 0 {
			start = nodeID
			end = nodeID
			if err := s.linkNodes(txn, key, nodeID, nodeID); err != nil {
				return 0, err
			}
		} else if toLeft {
			if err := s.linkNodes(txn, key, nodeID, start); err != nil {
				return 0, err
			}
			if err := txn.Set([]byte(s.listKey(key, start, "prev")), []byte(nodeID)); err != nil {
				return 0, err
			}
			start = nodeID
		} else {
			if err := s.linkNodes(txn, key, end, nodeID); err != nil {
				return 0, err
			}
			if err := txn.Set([]byte(s.listKey(key, end, "next")), []byte(nodeID)); err != nil {
				return 0, err
			}
			end = nodeID
		}
		length++
	}
	if err := s.listUpdateMeta(txn, key, length, start, end); err != nil {
		return 0, err
	}
	return length, nil
}

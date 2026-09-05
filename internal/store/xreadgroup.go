package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// ErrNOGroup mirrors redis's NOGROUP error: XREADGROUP on a stream key that
// has no consumer group (including a missing key) must fail, never silently
// return an empty read.
type ErrNOGroup struct {
	Key   string
	Group string
}

func (e *ErrNOGroup) Error() string {
	return fmt.Sprintf("NOGROUP No such key '%s' or consumer group '%s'", e.Key, e.Group)
}

// XReadGroup reads from a consumer group
func (s *BotreonStore) XReadGroup(ctx context.Context, group, consumer string, count int64, block int64, keys ...string) ([]map[string][]StreamEntry, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	result := make([]map[string][]StreamEntry, 0)

	// 锁只覆盖非阻塞读段：阻塞等待（下方 xReadGroupBlocking）必须在锁外——
	// 若持 key 锁等待，XADD（需同 key 锁）无法推进 → 等待永不唤醒（死锁）。
	unlock := s.keyLockMgr.LockMulti(keys)
	err := s.retryUpdateLazy(func(txn *badger.Txn) error {
		now := time.Now().UnixNano() / int64(time.Millisecond)

		for _, key := range keys {
			if err := checkKeyType(txn, key, KeyTypeStream); err != nil {
				return err
			}
			groupKey := streamGroupDataKey(key, group)
			item, err := txn.Get(groupKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return &ErrNOGroup{Key: key, Group: group}
			}
			if err != nil {
				return err
			}

			var groupData *StreamGroup
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &groupData)
			}); err != nil {
				return err
			}

			if groupData.Consumers == nil {
				groupData.Consumers = make(map[string]*StreamConsumer)
			}

			if _, exists := groupData.Consumers[consumer]; !exists {
				groupData.Consumers[consumer] = &StreamConsumer{
					Name:     consumer,
					LastSeen: now,
				}
			} else {
				groupData.Consumers[consumer].LastSeen = now
			}

			if groupData.Pending == nil {
				groupData.Pending = make(map[string]*StreamPendingEntry)
			}

			entries := make([]StreamEntry, 0)
			prefix := streamDataPrefix(key)
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			lastTS, lastSeq, _ := parseStreamID(groupData.LastDeliveredID)
			var lastID string

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				id := string(bytes.TrimPrefix(item.Key(), prefix))

				ts, seq, _ := parseStreamID(id)
				if ts < lastTS || (ts == lastTS && seq <= lastSeq) {
					continue
				}

				var fields map[string]string
				if err := item.Value(func(val []byte) error {
					return json.Unmarshal(val, &fields)
				}); err != nil {
					return err
				}

				entries = append(entries, StreamEntry{
					ID:        id,
					Fields:    fields,
					Timestamp: ts,
					Sequence:  seq,
				})

				if _, pending := groupData.Pending[id]; !pending {
					groupData.Pending[id] = &StreamPendingEntry{
						ID:            id,
						Consumer:      consumer,
						DeliveryCount: 1,
						LastDelivery:  now,
					}
				}

				lastID = id

				if count > 0 && int64(len(entries)) >= count {
					break
				}
			}

			if lastID != "" {
				groupData.LastDeliveredID = lastID
			}

			data, err := json.Marshal(groupData)
			if err != nil {
				return err
			}
			if err := txn.Set(groupKey, data); err != nil {
				return err
			}

			if len(entries) > 0 {
				result = append(result, map[string][]StreamEntry{key: entries})
			}
		}
		return nil
	}, 30, func() []byte {
		// D4 全重放：XREADGROUP [COUNT n] GROUP group consumer STREAMS key... >
		// （stream 消费组状态复制——PEL/consumer/LastDeliveredID——WriteCommand
		// XREADGROUP 分支按此格式解析——id 参数被忽略（store 用 LastDeliveredID
		// 游标）——无新消息时不写 log 避免空帧膨胀）
		if len(result) == 0 {
			return nil
		}
		args := make([][]byte, 0, 5+2*len(keys))
		args = append(args, []byte("XREADGROUP"))
		if count > 0 {
			args = append(args, []byte("COUNT"), []byte(strconv.FormatInt(count, 10)))
		}
		args = append(args, []byte("GROUP"), []byte(group), []byte(consumer))
		args = append(args, []byte("STREAMS"))
		for _, k := range keys {
			args = append(args, []byte(k))
		}
		for range keys {
			args = append(args, []byte(">"))
		}
		return encodePropagateCommand(args...)
	})
	unlock()

	if block >= 0 && len(result) == 0 && len(keys) > 0 {
		r, e := s.xReadGroupBlocking(ctx, group, consumer, count, block, keys)
		return r, e
	}
	return result, err
}

// xReadGroupBlocking implements blocking XREADGROUP
func (s *BotreonStore) xReadGroupBlocking(ctx context.Context, group, consumer string, count int64, block int64, keys []string) ([]map[string][]StreamEntry, error) {
	resultCh := make(chan StreamReadResult, 1)

	s.streamBlockingMu.Lock()
	for _, key := range keys {
		s.streamBlockingChans[key] = append(s.streamBlockingChans[key], resultCh)
	}
	s.streamBlockingMu.Unlock()

	result, err := s.XReadGroup(ctx, group, consumer, count, -1, keys...)
	if err != nil {
		s.unregisterStreamBlocking(resultCh, keys)
		return nil, err
	}
	if len(result) > 0 {
		s.unregisterStreamBlocking(resultCh, keys)
		return result, nil
	}

	if block == 0 {
		for {
			select {
			case <-ctx.Done():
				s.unregisterStreamBlocking(resultCh, keys)
				return nil, nil
			case <-s.closeCh:
				s.unregisterStreamBlocking(resultCh, keys)
				return nil, nil
			case <-resultCh:
				result, err = s.XReadGroup(ctx, group, consumer, count, -1, keys...)
				if err != nil {
					s.unregisterStreamBlocking(resultCh, keys)
					return nil, err
				}
				if len(result) > 0 {
					s.unregisterStreamBlocking(resultCh, keys)
					return result, nil
				}
				s.streamBlockingMu.Lock()
				for _, key := range keys {
					s.streamBlockingChans[key] = append(s.streamBlockingChans[key], resultCh)
				}
				s.streamBlockingMu.Unlock()
			}
		}
	}

	timer := time.NewTimer(time.Duration(block) * time.Millisecond)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-ctx.Done():
		s.unregisterStreamBlocking(resultCh, keys)
		return nil, nil
	case <-s.closeCh:
		s.unregisterStreamBlocking(resultCh, keys)
		return nil, nil
	case <-resultCh:
		result, err = s.XReadGroup(ctx, group, consumer, count, -1, keys...)
		if err != nil {
			s.unregisterStreamBlocking(resultCh, keys)
			return nil, err
		}
		if len(result) > 0 {
			s.unregisterStreamBlocking(resultCh, keys)
			return result, nil
		}
		s.unregisterStreamBlocking(resultCh, keys)
		return nil, nil
	case <-timer.C:
		s.unregisterStreamBlocking(resultCh, keys)
		return nil, nil
	}
}

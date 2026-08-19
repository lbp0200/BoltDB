package store

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// XAutoClaimOptions contains options for XAUTOCLAIM
type XAutoClaimOptions struct {
	Count      int64
	JustID     bool
	IdleMS     int64 // 认领后 LastDelivery = now-IdleMS（XAUTOCLAIM IDLE）
	TimeMS     int64 // 认领后 LastDelivery = TimeMS（XAUTOCLAIM TIME）
	RetryCount int64 // 认领后 DeliveryCount = RetryCount（XAUTOCLAIM RETRYCOUNT）
	Force      bool  // FORCE：即使条目不在 PEL 也认领（XAUTOCLAIM FORCE）
}

// XAutoClaimResult contains the result of XAUTOCLAIM
type XAutoClaimResult struct {
	NextID     string
	ClaimedIDs []string
	Messages   []StreamEntry
}

// XAutoClaim automatically claims pending messages
func (s *BotreonStore) XAutoClaim(key, group, consumer string, minIdleTime int64, start string, opts XAutoClaimOptions) (*XAutoClaimResult, error) {
	var result XAutoClaimResult

	err := s.retryUpdate(func(txn *badger.Txn) error {
		groupKey := streamGroupDataKey(key, group)
		item, err := txn.Get(groupKey)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
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

		now := time.Now().UnixNano() / int64(time.Millisecond)
		lastDelivery := now
		if opts.IdleMS > 0 {
			lastDelivery = now - opts.IdleMS
		}
		if opts.TimeMS > 0 {
			lastDelivery = opts.TimeMS
		}

		startTS, startSeq, _ := parseStreamID(start)

		// FORCE：收集流中 start 之后的所有条目 ID（即使不在 PEL 也认领），
		// 与 PEL 中的候选合并；非 FORCE 仅认领 PEL 内的条目。
		candidates := make(map[string]*StreamPendingEntry)
		for id, pending := range groupData.Pending {
			candidates[id] = pending
		}
		if opts.Force {
			prefix := streamDataPrefix(key)
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				keyBytes := item.KeyCopy(nil)
				idStr := string(keyBytes[len(prefix):])
				if _, exists := candidates[idStr]; !exists {
					candidates[idStr] = nil // 标记为需要认领
				}
			}
		}

		for id, pending := range candidates {
			idTS, idSeq, _ := parseStreamID(id)

			if idTS < startTS || (idTS == startTS && idSeq <= startSeq) {
				continue
			}

			if pending != nil {
				idleTime := now - pending.LastDelivery
				if idleTime < minIdleTime {
					continue
				}
			}

			if pending == nil {
				// FORCE 认领不在 PEL 中的条目：创建 Pending 条目。
				// RETRYCOUNT 覆盖初始 DeliveryCount（与已存在条目一致）。
				deliveryCount := int64(1)
				if opts.RetryCount > 0 {
					deliveryCount = opts.RetryCount
				}
				pending = &StreamPendingEntry{
					ID:            id,
					Consumer:      consumer,
					LastDelivery:  lastDelivery,
					DeliveryCount: deliveryCount,
				}
				groupData.Pending[id] = pending
			} else {
				pending.Consumer = consumer
				pending.LastDelivery = lastDelivery
				if opts.RetryCount > 0 {
					pending.DeliveryCount = opts.RetryCount
				} else {
					pending.DeliveryCount++
				}
			}

			result.ClaimedIDs = append(result.ClaimedIDs, id)

			dataKey := streamDataKey(key, id)
			msgItem, err := txn.Get(dataKey)
			if err == nil {
				var fields map[string]string
				if err := msgItem.Value(func(val []byte) error {
					return json.Unmarshal(val, &fields)
				}); err == nil {
					result.Messages = append(result.Messages, StreamEntry{
						ID:     id,
						Fields: fields,
					})
				}
			}

			if opts.Count > 0 && int64(len(result.ClaimedIDs)) >= opts.Count {
				break
			}
		}

		if len(result.ClaimedIDs) > 0 {
			lastID := result.ClaimedIDs[len(result.ClaimedIDs)-1]
			lastTS, lastSeq, _ := parseStreamID(lastID)
			result.NextID = formatStreamID(lastTS, lastSeq)
		} else {
			result.NextID = start
		}

		data, err := json.Marshal(groupData)
		if err != nil {
			return err
		}
		return txn.Set(groupKey, data)
	}, 30)

	return &result, err
}

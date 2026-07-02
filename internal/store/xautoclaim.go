package store

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// XAutoClaimOptions contains options for XAUTOCLAIM
type XAutoClaimOptions struct {
	Count  int64
	JustID bool
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

		startTS, startSeq, _ := parseStreamID(start)

		for id, pending := range groupData.Pending {
			idTS, idSeq, _ := parseStreamID(id)

			if idTS < startTS || (idTS == startTS && idSeq <= startSeq) {
				continue
			}

			idleTime := now - pending.LastDelivery
			if idleTime < minIdleTime {
				continue
			}

			pending.Consumer = consumer
			pending.LastDelivery = now
			pending.DeliveryCount++

			result.ClaimedIDs = append(result.ClaimedIDs, id)

			dataKey := streamDataKey(key, id)
			msgItem, err := txn.Get(dataKey)
			if err == nil && !errors.Is(err, badger.ErrKeyNotFound) {
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

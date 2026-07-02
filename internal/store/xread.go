package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// XRead reads entries from one or more streams
func (s *BotreonStore) XRead(ctx context.Context, count int64, block int64, args ...string) ([]map[string][]StreamEntry, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, errors.New("ERR wrong number of arguments for 'XREAD' command")
	}

	result := make([]map[string][]StreamEntry, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		for i := 0; i < len(args); i += 2 {
			key := args[i]
			if err := checkKeyType(txn, key, KeyTypeStream); err != nil {
				return err
			}
			startID := args[i+1]

			metaKey := streamKey(key)
			var meta *streamMetaData
			item, err := txn.Get(metaKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if err := item.Value(func(val []byte) error {
				meta, err = decodeStreamMeta(val)
				return err
			}); err != nil {
				return err
			}

			entries := make([]StreamEntry, 0)
			prefix := streamDataPrefix(key)
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			startTS, startSeq, _ := parseStreamID(startID)
			if startID == "$" {
				startTS = meta.LastID
				startSeq = meta.LastSeq + 1
			}

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				id := string(bytes.TrimPrefix(item.Key(), prefix))

				ts, seq, _ := parseStreamID(id)
				if compareStreamID(id, startID) <= 0 && startID != "$" {
					continue
				}
				if startID == "$" && (ts < startTS || (ts == startTS && seq <= startSeq)) {
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

				if count > 0 && int64(len(entries)) >= count {
					break
				}
			}

			if len(entries) > 0 {
				result = append(result, map[string][]StreamEntry{key: entries})
			}
		}
		return nil
	})

	if block >= 0 && len(result) == 0 {
		return s.xReadBlocking(ctx, count, block, args)
	}
	return result, err
}

// xReadBlocking implements blocking XREAD
func (s *BotreonStore) xReadBlocking(ctx context.Context, count int64, block int64, args []string) ([]map[string][]StreamEntry, error) {
	resultCh := make(chan StreamReadResult, 1)
	keys := streamKeys(args)

	s.streamBlockingMu.Lock()
	for _, key := range keys {
		s.streamBlockingChans[key] = append(s.streamBlockingChans[key], resultCh)
	}
	s.streamBlockingMu.Unlock()

	result, err := s.xReadImmediate(count, args...)
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
			case streamResult := <-resultCh:
				if len(streamResult.Entries) > 0 {
					return []map[string][]StreamEntry{{streamResult.Key: streamResult.Entries}}, nil
				}
				s.streamBlockingMu.Lock()
				for _, key := range keys {
					s.streamBlockingChans[key] = append(s.streamBlockingChans[key], resultCh)
				}
				s.streamBlockingMu.Unlock()
				result, err := s.xReadImmediate(count, args...)
				if err == nil && len(result) > 0 {
					s.unregisterStreamBlocking(resultCh, keys)
					return result, nil
				}
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
	case streamResult := <-resultCh:
		if len(streamResult.Entries) > 0 {
			s.unregisterStreamBlocking(resultCh, keys)
			return []map[string][]StreamEntry{{streamResult.Key: streamResult.Entries}}, nil
		}
	case <-timer.C:
	}
	s.unregisterStreamBlocking(resultCh, keys)
	return nil, nil
}

// xReadImmediate performs an immediate (non-blocking) XREAD
func (s *BotreonStore) xReadImmediate(count int64, args ...string) ([]map[string][]StreamEntry, error) {
	result := make([]map[string][]StreamEntry, 0)

	err := s.db.View(func(txn *badger.Txn) error {
		for i := 0; i < len(args); i += 2 {
			key := args[i]
			if err := checkKeyType(txn, key, KeyTypeStream); err != nil {
				return err
			}
			startID := args[i+1]

			metaKey := streamKey(key)
			var meta *streamMetaData
			item, err := txn.Get(metaKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if err := item.Value(func(val []byte) error {
				meta, err = decodeStreamMeta(val)
				return err
			}); err != nil {
				return err
			}

			entries := make([]StreamEntry, 0)
			prefix := streamDataPrefix(key)
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()

			startTS, startSeq, _ := parseStreamID(startID)
			if startID == "$" {
				startTS = meta.LastID
				startSeq = meta.LastSeq + 1
			}

			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				id := string(bytes.TrimPrefix(item.Key(), prefix))

				ts, seq, _ := parseStreamID(id)
				if compareStreamID(id, startID) <= 0 && startID != "$" {
					continue
				}
				if startID == "$" && (ts < startTS || (ts == startTS && seq <= startSeq)) {
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

				if count > 0 && int64(len(entries)) >= count {
					break
				}
			}

			if len(entries) > 0 {
				result = append(result, map[string][]StreamEntry{key: entries})
			}
		}
		return nil
	})
	return result, err
}

// notifyStreamRead notifies waiting stream readers
func (s *BotreonStore) notifyStreamRead(key string, entries []StreamEntry) {
	s.streamBlockingMu.Lock()
	defer s.streamBlockingMu.Unlock()

	chans := s.streamBlockingChans[key]
	for _, ch := range chans {
		select {
		case ch <- StreamReadResult{Key: key, Entries: entries}:
		default:
		}
	}
}

// unregisterStreamBlocking removes a specific channel from all stream keys' wait lists
func (s *BotreonStore) unregisterStreamBlocking(ch chan StreamReadResult, keys []string) {
	s.streamBlockingMu.Lock()
	defer s.streamBlockingMu.Unlock()

	for _, key := range keys {
		chans := s.streamBlockingChans[key]
		for j, c := range chans {
			if c == ch {
				s.streamBlockingChans[key] = append(chans[:j], chans[j+1:]...)
				break
			}
		}
		if len(s.streamBlockingChans[key]) == 0 {
			delete(s.streamBlockingChans, key)
		}
	}
}

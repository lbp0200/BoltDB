package store

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// 复制传播日志键（S2——D 定案——kvrocks 式 log-in-commit，a4 §10 附6）：
// 每命令的传播日志条目与数据变更在**同一 commitTS 事务**写入（同 ts = 天然绑定——
// 无分发侧打标、无竞态）。日志键 = replLogPrefix + 8 字节大端 ts——键序即 ts 序
// （读侧前缀扫描即按 ts 排水——backlog/PSYNC/ACK 迁移 ts 记账的数据源）。

var replLogPrefix = []byte("REPLLOG_")

// replLogKey 构造传播日志键：前缀 + ts 大端（badger 键字节序 = ts 升序）。
func replLogKey(ts uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], ts)
	k := make([]byte, 0, len(replLogPrefix)+8)
	k = append(k, replLogPrefix...)
	return append(k, b[:]...)
}

// encodePropagateCommand 将命令参数编码为 RESP 数组字节（与复制层 serializeCommand
// 同格式——复制层日志键消费者可直接解码重放）。格式在消费者增量前保持冻结。
func encodePropagateCommand(args ...[]byte) []byte {
	var b []byte
	b = append(b, '*')
	b = append(b, fmt.Sprintf("%d", len(args))...)
	b = append(b, '\r', '\n')
	for _, a := range args {
		b = append(b, '$')
		b = append(b, fmt.Sprintf("%d", len(a))...)
		b = append(b, '\r', '\n')
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	return b
}

// encodePropagateStringArgs 构造全重放日志值（命令 + 字符串参数摊平——D4 形态——零
// 对齐 feed 值源——MSET/MSETNX/BITOP/BITFIELD 等扁平参数站点的日志值构造）。
func encodePropagateStringArgs(cmd []byte, args []string) []byte {
	all := make([][]byte, 0, 1+len(args))
	all = append(all, cmd)
	for _, a := range args {
		all = append(all, []byte(a))
	}
	return encodePropagateCommand(all...)
}

// ReplLogEntry 是传播日志键读侧的条目（S2——§10 附7 D1b——日志键为增量续传/排水源）。
type ReplLogEntry struct {
	TS    uint64
	Value []byte
}

// ReplLogStartTS 返回当前传播日志键的最小 ts（日志键范围下界——PSYNC-ts 判定
// [logStartTS, currentTS] 的起点）。正向前缀 Seek 第一个日志键（O(log N) 级）。
func (s *BotreonStore) ReplLogStartTS() (uint64, error) {
	var ts uint64
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		it.Seek(replLogPrefix)
		if it.Valid() && bytes.HasPrefix(it.Item().Key(), replLogPrefix) {
			key := it.Item().Key()
			ts = binary.BigEndian.Uint64(key[len(key)-8:])
		}
		return nil
	})
	return ts, err
}

// ReplLogCurrentTS 返回当前传播日志键的最大 ts（主侧 currentTS 水位——GETACK 回复的
// ts 携带与排水判据的基准）。反向 Seek 到前缀最大键（O(log N) 级——非全扫描）。
func (s *BotreonStore) ReplLogCurrentTS() (uint64, error) {
	var ts uint64
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{Reverse: true})
		defer it.Close()
		it.Seek(replLogKey(^uint64(0)))
		if it.Valid() && bytes.HasPrefix(it.Item().Key(), replLogPrefix) {
			key := it.Item().Key()
			ts = binary.BigEndian.Uint64(key[len(key)-8:])
		}
		return nil
	})
	return ts, err
}

// ReplLogEntries 遍历全部传播日志键（REPLLOG_ 前缀扫描——键序即 ts 升序）返回条目。
// 读侧探针（§10 附7 分级-1——影子双写一致性比对）与 S2 增量续传（D1b）的数据源。
func (s *BotreonStore) ReplLogEntries() ([]ReplLogEntry, error) {
	return s.ReplLogEntriesFrom(0)
}

// ReplLogEntriesFrom 从指定 ts（含）起遍历传播日志键（replLogKey(since) seek——
// 键序即 ts 升序——首个 ts >= since 的日志键起）。log-key 增量流（S2 分级 2/3 重排——
// master 侧按从侧请求 ts 增量发送）的读取基础。
func (s *BotreonStore) ReplLogEntriesFrom(since uint64) ([]ReplLogEntry, error) {
	var out []ReplLogEntry
	seekKey := replLogKey(since)
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(seekKey); it.Valid() && bytes.HasPrefix(it.Item().Key(), replLogPrefix); it.Next() {
			v, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			key := it.Item().Key()
			out = append(out, ReplLogEntry{TS: binary.BigEndian.Uint64(key[len(key)-8:]), Value: v})
		}
		return nil
	})
	return out, err
}

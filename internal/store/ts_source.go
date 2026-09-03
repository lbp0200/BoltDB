package store

import (
	"sync"

	"github.com/dgraph-io/badger/v4"
)

// tsSource 为 managed-mode 提交分配串行唯一的 commit timestamp（A4 §10 附4）。
//
// managed 下 badger 不校验 CommitAt 的顺序（§8 坏点 #2）——本源的互斥保证每次
// 分配严格递增；失败提交尝试消耗的 ts 空洞容忍（读-at-ts 语义只依赖 ≤N 前缀
// 完整，非连续）。重启水位 = db.MaxVersion()+1：badger 暴露库内最大已提交版本
// （db.go:434——memtable + LSM 表），Init 后新 ts 不与重启前任何提交碰撞
// （跨生命周期单调）。
// _test.go）——task #4（S1-A2 大爆炸：CommitAt 化）起被生产写路径接线使用。
//
//nolint:unused // tsSource 为预接线前件：当前仅被 ts_source_test.go 引用（linter 跳过
type tsSource struct {
	mu   sync.Mutex
	next uint64
}

// newTSSource 返回从 1 开始的 ts 源（Init 会按水位覆写起始值）。
//
//nolint:unused
func newTSSource() *tsSource {
	return &tsSource{next: 1}
}

// Init 以库内最大已提交版本为水位恢复起始 ts：next = MaxVersion()+1。
//
//nolint:unused
func (t *tsSource) Init(db *badger.DB) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.next = db.MaxVersion() + 1
}

// Next 分配下一个 ts（互斥——串行唯一单调）。
//
//nolint:unused
func (t *tsSource) Next() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	v := t.next
	t.next++
	return v
}

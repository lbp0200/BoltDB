package store

import (
	"math"
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
//
// 除分配外，本源还维护**有序完成水位**（discard-ts 的安全推进基准）：并发提交
// 乱序完成时，discard-ts 只能推进到**连续完成的 ts 前缀**（done）——否则会越过
// 未完成提交的 ts，触发 badger 内部 `ts >= o.lastCleanupTs` 断言（txn.go:176——
// managed 下 cleanup 的 maxReadTs = discardTs，txn.go:207——2026-09-03 全包套件
// 实证：仅并发上下文触发）。
type tsSource struct {
	mu        sync.Mutex
	next      uint64
	pending   map[uint64]struct{} // 已分配未完成的 ts
	done      uint64              // 连续完成的 ts 前缀最高值（discard-ts 可安全推进至此）
	discarded uint64              // 已推进给 badger SetDiscardTs 的最高值（单调）
}

// newTSSource 返回从 1 开始的 ts 源（Init 会按水位覆写起始值）。
func newTSSource() *tsSource {
	return &tsSource{next: 1, pending: make(map[uint64]struct{})}
}

// Init 以库内最大已提交版本为水位恢复起始 ts：next = MaxVersion()+1。
func (t *tsSource) Init(db *badger.DB) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.next = db.MaxVersion() + 1
}

// Begin 分配下一个 ts 并登记为 in-flight（提交尝试开始）。
func (t *tsSource) Begin() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	ts := t.next
	t.next++
	t.pending[ts] = struct{}{}
	return ts
}

// End 释放一个 in-flight ts（提交尝试结束——无论成败）并推进连续完成前缀。
func (t *tsSource) End(ts uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.pending, ts)
	// done 只推进到已分配区间（< t.next）：未分配的 ts 不在 pending 中——
	// 无上界则顺序完成时 done 无限推进（uint64 溢出）。
	for t.done+1 < t.next {
		if _, inFlight := t.pending[t.done+1]; inFlight {
			return // 前缀断裂——有更早 ts 仍未完成
		}
		t.done++
	}
}

// SafeDiscard 返回当前可安全推进 discard-ts 的值：连续完成前缀（所有 ≤ done 的
// 提交均已结束——无 in-flight ts ≤ done——discard-ts ≤ done 永不越过未完成提交）。
func (t *tsSource) SafeDiscard() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.done
}

// AdvanceDiscard 返回本次可推进 discard-ts 的单调值（0 = 不推进）：done 可能因
// in-flight 前缀断裂而低于已推进值（并发乱序完成）——回退推进会触发 badger cleanup
// 断言 `maxReadTs(discardTs) >= o.lastCleanupTs`（txn.go:211——2026-09-03 栈捕获
// 实证：commitTS 的 SetDiscardTs → oracle cleanup → 断言）。故只允许单调前进。
func (t *tsSource) AdvanceDiscard() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done > t.discarded {
		t.discarded = t.done
		return t.done
	}
	return 0
}

// commitTS 以 tsSource 分配的 ts 执行 managed-mode 写提交（A4 §10 附4）——
// 原 `db.Update(fn)` 的 managed 等价：NewTransactionAt(MaxUint64, true)
// （最新快照读——badger 自家 managed 测试的规范模式）+ fn 写体 +
// CommitAt(ts, nil)（nil 回调 = 同步 Commit——错误直返——契合 retryUpdate 的
// 同步重试语义）。fn 错误不消耗 ts 提交（Begin 已登记——End 释放——空洞容忍）。
//
// 提交完成后推进 discard-ts（SetDiscardTs(sd)）：managed 模式无自动版本回收——
// 不推进则每次写/删的新 ts 版本永不回收 → MVCC 版本累积 → 写删量级压实停滞
// （60000 写 + 48000 删规模远程挂起实证——gc_test 同因）。推进取有序完成水位
// （AdvanceDiscard——连续完成前缀 + 单调守卫），且 AdvanceDiscard+SetDiscardTs
// 在 discardMu 下成原子对（见 define.go discardMu 注释——并发低值后落回退实证）。
func (s *BotreonStore) commitTS(fn func(*badger.Txn) error, logValue ...[]byte) error {
	txn := s.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	ts := s.tsSource.Begin()
	defer s.tsSource.End(ts)
	if err := fn(txn); err != nil {
		return err
	}
	// S2 D 定案（a4 §10 附6——kvrocks 式 log-in-commit）：传播日志键与数据变更
	// 同事务写入——同 ts = 天然绑定（无分发侧打标/竞态）。fn 成功后写（失败回复
	// 不入日志——防 slave apply 错误/FULLRESYNC thrash——handler_core 754 注释语义）。
	if len(logValue) > 0 && len(logValue[0]) > 0 {
		if err := txn.Set(replLogKey(ts), logValue[0]); err != nil {
			return err
		}
	}
	if err := txn.CommitAt(ts, nil); err != nil {
		return err
	}
	s.discardMu.Lock()
	if sd := s.tsSource.AdvanceDiscard(); sd > 0 {
		s.db.SetDiscardTs(sd)
	}
	s.discardMu.Unlock()
	return nil
}

// commitTSLazy 与 commitTS 相同，但 logValue 为延迟求值函数（fn 成功后再调用——
// 支持依赖事务内结果的 log 编码（XADD 的 stream id 生成——修复 log 帧写 `*`
// 导致从侧重放 id 漂移——lost 家族扫面扩展发现 2026-09-06）。
func (s *BotreonStore) commitTSLazy(fn func(*badger.Txn) error, logValue func() []byte) error {
	txn := s.db.NewTransactionAt(math.MaxUint64, true)
	defer txn.Discard()
	ts := s.tsSource.Begin()
	defer s.tsSource.End(ts)
	if err := fn(txn); err != nil {
		return err
	}
	if lv := logValue(); len(lv) > 0 {
		if err := txn.Set(replLogKey(ts), lv); err != nil {
			return err
		}
	}
	if err := txn.CommitAt(ts, nil); err != nil {
		return err
	}
	s.discardMu.Lock()
	if sd := s.tsSource.AdvanceDiscard(); sd > 0 {
		s.db.SetDiscardTs(sd)
	}
	s.discardMu.Unlock()
	return nil
}

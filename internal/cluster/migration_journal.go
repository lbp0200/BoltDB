package cluster

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
)

// Migration journal keys (stored in BadgerDB for crash recovery).
// Format: __MIGRATE_JOURNAL__:<slot>
const journalKeyPrefix = "__MIGRATE_JOURNAL__:"

// Migration phases.
const (
	PhaseInit    = "INIT"    // 迁移初始化
	PhaseCopying = "COPYING" // Phase 1：复制 key 到目标节点
	PhaseCopied  = "COPIED"  // Phase 1 完成：所有 key 已复制
	PhaseCommit  = "COMMIT"  // Phase 2：正在提交（批量删除 + 切换 slot）
	PhaseDone    = "DONE"    // 迁移完成
)

// migrationJournal 记录一次 slot 迁移的所有状态。
// 持久化在 BadgerDB 中，crash 后可通过 RecoverSlotMigration 恢复。
// 注意：不再存储 Keys 字段（全量 key 列表），Phase 2 的 key 删除改为重新扫描 slot。
type migrationJournal struct {
	Slot       uint32 `json:"slot"`
	TargetID   string `json:"target_id"`
	TargetAddr string `json:"target_addr"`
	Phase      string `json:"phase"`
	CopyKeys   bool   `json:"copy_keys"` // 是否 COPY 模式（不删除源端）
}

// journalKey 返回槽位对应迁移日志的 BadgerDB key。
func journalKey(slot uint32) []byte {
	return []byte(journalKeyPrefix + strconv.FormatUint(uint64(slot), 10))
}

// saveJournal 持久化迁移日志到 BadgerDB。
func (c *Cluster) saveJournal(j *migrationJournal) error {
	data, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal migration journal: %w", err)
	}
	return c.Store.GetDB().Update(func(txn *badger.Txn) error {
		return txn.Set(journalKey(j.Slot), data)
	})
}

// loadJournal 从 BadgerDB 读取迁移日志。
// 返回 nil, nil 表示不存在日志（首次启动或已清理）。
func (c *Cluster) loadJournal(slot uint32) (*migrationJournal, error) {
	var data []byte
	err := c.Store.GetDB().View(func(txn *badger.Txn) error {
		item, err := txn.Get(journalKey(slot))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("read migration journal for slot %d: %w", slot, err)
	}
	if data == nil {
		return nil, nil
	}
	var j migrationJournal
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("unmarshal migration journal for slot %d: %w", slot, err)
	}
	return &j, nil
}

// deleteJournal 删除迁移日志（迁移完成后清理）。
func (c *Cluster) deleteJournal(slot uint32) error {
	return c.Store.GetDB().Update(func(txn *badger.Txn) error {
		return txn.Delete(journalKey(slot))
	})
}

// migrateConn 封装单个 key 的迁移 TCP 连接。
type migrateConn struct {
	conn   net.Conn
	reader *bufio.Reader
	mu     sync.Mutex
}

func newMigrateConn(addr string) (*migrateConn, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect to target %s: %w", addr, err)
	}
	return &migrateConn{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}, nil
}

func (mc *migrateConn) sendRestore(key string, data []byte) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	// Do NOT use REPLACE. During MIGRATING, ASKING clients may write newer
	// values on the IMPORTING target; REPLACE would clobber them with a stale
	// source snapshot (data loss). If the key already exists on the target,
	// keep the target value and continue Phase 1.
	restoreCmd := &proto.Array{
		Args: [][]byte{
			[]byte("RESTORE"),
			[]byte(key),
			[]byte("0"),
			data,
		},
	}
	if err := proto.WriteRESP(mc.conn, restoreCmd); err != nil {
		return fmt.Errorf("write RESTORE: %w", err)
	}

	resp, err := proto.ReadRESP(mc.reader)
	if err != nil {
		return fmt.Errorf("read RESTORE response: %w", err)
	}

	msg := restoreResponseMsg(resp)
	if msg == "" || msg == "OK" {
		return nil
	}
	// Key already present on target (ASKING write or prior partial copy) —
	// leave target value intact.
	if strings.Contains(msg, "already exists") || strings.Contains(msg, "BUSYKEY") {
		logger.Logger.Debug().
			Str("key", key).
			Str("msg", msg).
			Msg("migrate RESTORE skipped: target key exists (no REPLACE)")
		return nil
	}
	return fmt.Errorf("target error: %s", msg)
}

// restoreResponseMsg extracts a response message from ReadRESP's Array form.
// Success is "OK"; errors look like "ERR ..." (leading '-' already stripped).
func restoreResponseMsg(resp *proto.Array) string {
	if resp == nil || len(resp.Args) == 0 {
		return "empty response"
	}
	return string(resp.Args[0])
}

func (mc *migrateConn) sendStable(slot uint32) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	stableCmd := &proto.Array{
		Args: [][]byte{
			[]byte("CLUSTER"),
			[]byte("SETSLOT"),
			[]byte(strconv.FormatUint(uint64(slot), 10)),
			[]byte("STABLE"),
		},
	}
	_ = proto.WriteRESP(mc.conn, stableCmd)
	// Drain optional reply so the connection stays aligned for further commands.
	_, _ = proto.ReadRESP(mc.reader)
}

// sendSetSlotImporting marks the target slot as IMPORTING from sourceNodeID.
func (mc *migrateConn) sendSetSlotImporting(slot uint32, sourceNodeID string) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	cmd := &proto.Array{
		Args: [][]byte{
			[]byte("CLUSTER"),
			[]byte("SETSLOT"),
			[]byte(strconv.FormatUint(uint64(slot), 10)),
			[]byte("IMPORTING"),
			[]byte(sourceNodeID),
		},
	}
	if err := proto.WriteRESP(mc.conn, cmd); err != nil {
		return fmt.Errorf("write SETSLOT IMPORTING: %w", err)
	}
	resp, err := proto.ReadRESP(mc.reader)
	if err != nil {
		return fmt.Errorf("read SETSLOT IMPORTING: %w", err)
	}
	msg := restoreResponseMsg(resp)
	if msg != "" && msg != "OK" && strings.HasPrefix(msg, "ERR") {
		return fmt.Errorf("SETSLOT IMPORTING: %s", msg)
	}
	return nil
}

// sendDelKeysInSlot asks the target to drop all keys in slot (Phase-1 abort cleanup).
func (mc *migrateConn) sendDelKeysInSlot(slot uint32) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	cmd := &proto.Array{
		Args: [][]byte{
			[]byte("CLUSTER"),
			[]byte("DELKEYSINSLOT"),
			[]byte(strconv.FormatUint(uint64(slot), 10)),
		},
	}
	if err := proto.WriteRESP(mc.conn, cmd); err != nil {
		return fmt.Errorf("write DELKEYSINSLOT: %w", err)
	}
	resp, err := proto.ReadRESP(mc.reader)
	if err != nil {
		return fmt.Errorf("read DELKEYSINSLOT: %w", err)
	}
	msg := restoreResponseMsg(resp)
	if strings.HasPrefix(msg, "ERR") {
		return fmt.Errorf("DELKEYSINSLOT: %s", msg)
	}
	return nil
}

// abortTargetImport best-effort: delete partial RESTORE keys and clear IMPORTING.
func (mc *migrateConn) abortTargetImport(slot uint32) {
	if err := mc.sendDelKeysInSlot(slot); err != nil {
		logger.Logger.Warn().Err(err).Uint32("slot", slot).Msg("abortTargetImport: DELKEYSINSLOT failed")
	}
	mc.sendStable(slot)
}

func (mc *migrateConn) close() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.conn.Close() //nolint:errcheck
}

// collectSlotKeys 收集指定 slot 的所有 key。
func (c *Cluster) collectSlotKeys(slot uint32) ([]string, error) {
	var keys []string
	err := c.Store.IterateRawKeys(func(rawKey string) bool {
		if Slot(rawKey) == slot {
			keys = append(keys, rawKey)
		}
		return true
	})
	return keys, err
}

// DelKeysInSlot deletes every key that hashes to slot. Used for migration
// abort cleanup on the IMPORTING target (partial RESTORE orphans).
func (c *Cluster) DelKeysInSlot(slot uint32) (int, error) {
	keys, err := c.collectSlotKeys(slot)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, key := range keys {
		n, dErr := c.Store.Del(key)
		if dErr != nil {
			return deleted, fmt.Errorf("del key %s in slot %d: %w", key, slot, dErr)
		}
		if n > 0 {
			deleted++
		}
	}
	return deleted, nil
}

// MigrateSlotCrashSafe 是 MigrateSlot 的 crash-safe 版本（两阶段提交）。
// 它使用迁移日志记录每个 key 的迁移进度，在 crash 后可恢复。
//
// 参数：
//   - slot：要迁移的槽位
//   - targetNodeID：目标节点 ID
//   - copyKeys：如果为 true，只复制不删除源端
//
// 两阶段提交流程：
//
//	Phase 1 (COPY)：逐 key DUMP → TCP RESTORE，记录每个 key 的复制状态到日志
//	Phase 2 (COMMIT)：BadgerDB 事务批量删除源端 key + AssignSlot，日志标记 DONE
//
// Crash 恢复（重启时由 RecoverSlotMigrations 调用）：
//   - INIT / COPYING：清除 MIGRATING 状态，回滚（源端未删除，安全）
//   - COPIED 但非 DONE：重新执行 Phase 2（批量删除 + AssignSlot）
//   - DONE：清理迁移日志
func (c *Cluster) MigrateSlotCrashSafe(slot uint32, targetNodeID string, copyKeys bool) error {
	if slot >= SlotCount {
		return fmt.Errorf("slot %d out of range", slot)
	}

	// 验证 slot 在当前节点处于 MIGRATING 状态
	if !c.IsMigratingSlot(slot) {
		return fmt.Errorf("slot %d is not in MIGRATING state", slot)
	}

	// 检查是否已有未完成的迁移日志（防止并发迁移同一个 slot）
	existing, err := c.loadJournal(slot)
	if err != nil {
		return fmt.Errorf("check existing journal for slot %d: %w", slot, err)
	}
	if existing != nil && existing.Phase != PhaseDone {
		return fmt.Errorf("slot %d already has an incomplete migration (phase=%s)", slot, existing.Phase)
	}

	// 找目标节点
	c.mu.RLock()
	targetNode, exists := c.Nodes[targetNodeID]
	c.mu.RUnlock()
	if !exists || targetNode == nil {
		return fmt.Errorf("target node %s not found", targetNodeID)
	}

	logger.Logger.Info().
		Uint32("slot", slot).
		Str("target", targetNodeID).
		Str("target_addr", targetNode.Addr).
		Bool("copy", copyKeys).
		Msg("MigrateSlotCrashSafe: starting two-phase slot migration")

	// 收集所有属于该 slot 的 key
	keys, err := c.collectSlotKeys(slot)
	if err != nil {
		return fmt.Errorf("iterate keys for slot %d: %w", slot, err)
	}

	logger.Logger.Info().
		Uint32("slot", slot).
		Int("key_count", len(keys)).
		Msg("MigrateSlotCrashSafe: collected keys to migrate")

	// 初始化迁移日志（不再存储全量 key 列表，避免 O(N) 内存占用和序列化开销）
	journal := &migrationJournal{
		Slot:       slot,
		TargetID:   targetNodeID,
		TargetAddr: targetNode.Addr,
		Phase:      PhaseInit,
		CopyKeys:   copyKeys,
	}

	// 持久化 INIT 状态
	journal.Phase = PhaseInit
	if err := c.saveJournal(journal); err != nil {
		return fmt.Errorf("save initial migration journal: %w", err)
	}

	// ---- Phase 1: COPY ----
	journal.Phase = PhaseCopying
	if err := c.saveJournal(journal); err != nil {
		return fmt.Errorf("save COPYING journal: %w", err)
	}

	// 连接目标节点
	mc, err := newMigrateConn(targetNode.Addr)
	if err != nil {
		return err
	}
	defer mc.close()

	// Ensure target marks slot IMPORTING so ASKING + write fence apply.
	if err := mc.sendSetSlotImporting(slot, c.Myself.ID); err != nil {
		logger.Logger.Warn().Err(err).Uint32("slot", slot).Msg("MigrateSlotCrashSafe: SETSLOT IMPORTING on target failed (continuing)")
	}

	// 逐 key DUMP → RESTORE（不持久化逐 key 状态；若 crash，recovery 对 Phase 1 做回滚而非恢复）
	for _, key := range keys {
		// DUMP key
		data, err := c.Store.Dump(key)
		if err != nil {
			if strings.Contains(err.Error(), "no such key") {
				// key 在收集中存在但在 DUMP 时已被删除，跳过
				continue
			}
			// Phase-1 failure: clean partial RESTOREs on target
			mc.abortTargetImport(slot)
			return fmt.Errorf("dump key %s: %w", key, err)
		}

		// 发送 RESTORE 到目标
		if err := mc.sendRestore(key, data); err != nil {
			mc.abortTargetImport(slot)
			return fmt.Errorf("restore key %s on target: %w", key, err)
		}
	}

	// Phase 1 完成
	journal.Phase = PhaseCopied
	if err := c.saveJournal(journal); err != nil {
		return fmt.Errorf("save COPIED journal: %w", err)
	}

	// 在 COPY 模式下，不执行 Phase 2（不删除源端，不切换 slot）
	if copyKeys {
		journal.Phase = PhaseDone
		if err := c.saveJournal(journal); err != nil {
			return fmt.Errorf("save DONE journal (copy mode): %w", err)
		}
		c.ClearSlotMigration(slot)

		logger.Logger.Info().
			Uint32("slot", slot).
			Str("target", targetNodeID).
			Int("copied", len(keys)).
			Msg("MigrateSlotCrashSafe: copy completed")
		return nil
	}

	// ---- Phase 2: COMMIT ----
	journal.Phase = PhaseCommit
	if err := c.saveJournal(journal); err != nil {
		return fmt.Errorf("save COMMIT journal: %w", err)
	}

	// 重新扫描 slot 的 key 并删除（而非依赖迁移开始时收集的列表）
	// 这样做的好处是：避免在 journal 中存储全量 key 列表，且 Del 对不存在的 key 幂等返回 0
	slotKeys, err := c.collectSlotKeys(slot)
	if err != nil {
		return fmt.Errorf("re-collect keys for slot %d during commit: %w", slot, err)
	}
	for _, key := range slotKeys {
		if _, err := c.Store.Del(key); err != nil {
			_ = c.saveJournal(journal)
			return fmt.Errorf("delete key %s during slot %d migration: %w", key, slot, err)
		}
	}

	// 原子切换 slot 归属
	c.ClearSlotMigration(slot)
	if err := c.AssignSlot(slot, targetNodeID); err != nil {
		// AssignSlot 失败但 key 已删除——这是危险状态，记录日志
		logger.Logger.Error().
			Err(err).
			Uint32("slot", slot).
			Msg("CRITICAL: keys deleted but AssignSlot failed! Manual intervention required.")
		_ = c.saveJournal(journal)
		return fmt.Errorf("assign slot %d to %s after key deletion: %w", slot, targetNodeID, err)
	}

	// 持久化集群配置（包含 slot 变更）
	if err := c.SaveConfig(); err != nil {
		logger.Logger.Warn().Err(err).Uint32("slot", slot).Msg("failed to save config after slot migration")
	}

	// 通知目标节点清除 IMPORTING 状态
	mc.sendStable(slot)

	// 标记完成
	journal.Phase = PhaseDone
	if err := c.saveJournal(journal); err != nil {
		return fmt.Errorf("save DONE journal: %w", err)
	}

	// 清理迁移日志（非关键，可在下次启动时清理）
	_ = c.deleteJournal(slot)

	logger.Logger.Info().
		Uint32("slot", slot).
		Str("target", targetNodeID).
		Int("migrated", len(keys)).
		Msg("MigrateSlotCrashSafe: migration completed successfully")

	return nil
}

// RecoverSlotMigrations 在集群启动时恢复未完成的 slot 迁移。
// 遍历 BadgerDB 中的迁移日志，根据当前阶段决定恢复策略。
// 应在 Cluster 初始化完成后调用。
func (c *Cluster) RecoverSlotMigrations() error {
	// 扫描所有迁移日志 key
	prefix := []byte(journalKeyPrefix)
	var slots []uint32

	err := c.Store.GetDB().View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			slotStr := key[len(journalKeyPrefix):]
			if slotUint, err := strconv.ParseUint(slotStr, 10, 32); err == nil {
				slots = append(slots, uint32(slotUint))
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("scan migration journals: %w", err)
	}

	for _, slot := range slots {
		journal, err := c.loadJournal(slot)
		if err != nil {
			logger.Logger.Warn().Err(err).Uint32("slot", slot).Msg("RecoverSlotMigrations: failed to load journal")
			continue
		}
		if journal == nil {
			continue
		}

		logger.Logger.Info().
			Uint32("slot", slot).
			Str("phase", journal.Phase).
			Msg("RecoverSlotMigrations: found incomplete migration")

		switch journal.Phase {
		case PhaseInit, PhaseCopying:
			// Phase 1 未完成 → 回滚源端 MIGRATING；并尽力清理目标上的部分 RESTORE
			logger.Logger.Warn().
				Uint32("slot", slot).
				Str("target_addr", journal.TargetAddr).
				Msg("RecoverSlotMigrations: Phase 1 incomplete, rolling back + cleaning target orphans")
			if journal.TargetAddr != "" {
				if tmc, dialErr := newMigrateConn(journal.TargetAddr); dialErr != nil {
					logger.Logger.Warn().Err(dialErr).Uint32("slot", slot).
						Msg("RecoverSlotMigrations: cannot reach target for abort cleanup")
				} else {
					tmc.abortTargetImport(slot)
					tmc.close()
				}
			}
			c.ClearSlotMigration(slot)
			_ = c.deleteJournal(slot)

		case PhaseCopied:
			// Phase 1 完成但 Phase 2 未开始 → 重新执行 Phase 2
			logger.Logger.Warn().
				Uint32("slot", slot).
				Str("target", journal.TargetID).
				Msg("RecoverSlotMigrations: Phase 1 complete, re-executing Phase 2 (COMMIT)")
			if err := c.retryCommitPhase(journal); err != nil {
				logger.Logger.Error().
					Err(err).
					Uint32("slot", slot).
					Msg("RecoverSlotMigrations: Phase 2 retry failed, manual intervention may be needed")
			}

		case PhaseCommit:
			// Phase 2 中断——可能部分 key 已删除 + AssignSlot 未执行
			logger.Logger.Warn().
				Uint32("slot", slot).
				Str("target", journal.TargetID).
				Msg("RecoverSlotMigrations: Phase 2 interrupted, retrying COMMIT")
			if err := c.retryCommitPhase(journal); err != nil {
				logger.Logger.Error().
					Err(err).
					Uint32("slot", slot).
					Msg("RecoverSlotMigrations: Phase 2 retry after interrupt failed")
			}

		case PhaseDone:
			// 完成后清理
			_ = c.deleteJournal(slot)
		}
	}

	return nil
}

// retryCommitPhase 重新执行 Phase 2（批量删除 + AssignSlot）。
// 用于 crash 恢复：Phase 1 已完成，需要提交。
func (c *Cluster) retryCommitPhase(journal *migrationJournal) error {
	slot := journal.Slot

	if !journal.CopyKeys {
		// 重新扫描 slot 的 key 并删除（而非依赖存储在 journal 中的全量 key 列表）
		// Del 对不存在的 key 幂等返回 0，不会报错
		slotKeys, err := c.collectSlotKeys(slot)
		if err != nil {
			return fmt.Errorf("re-collect keys for slot %d during retry commit: %w", slot, err)
		}
		for _, key := range slotKeys {
			if _, err := c.Store.Del(key); err != nil {
				return fmt.Errorf("retry delete key %s for slot %d: %w", key, slot, err)
			}
		}
	}

	// AssignSlot 是幂等的
	c.ClearSlotMigration(slot)
	if err := c.AssignSlot(slot, journal.TargetID); err != nil {
		return fmt.Errorf("retry assign slot %d to %s: %w", slot, journal.TargetID, err)
	}

	if err := c.SaveConfig(); err != nil {
		logger.Logger.Warn().Err(err).Uint32("slot", slot).Msg("retryCommitPhase: save config failed")
	}

	journal.Phase = PhaseDone
	_ = c.saveJournal(journal)
	_ = c.deleteJournal(slot)

	return nil
}

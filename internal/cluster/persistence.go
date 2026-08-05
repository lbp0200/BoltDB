package cluster

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

const (
	configKey = "cluster:config"
)

type persistedClusterState struct {
	Nodes          map[string]*persistedNode `json:"nodes"`
	Epoch          int64                     `json:"epoch"`
	Slots          []persistedSlotOwner      `json:"slots"`
	NodeID         string                    `json:"node_id"`
	Addr           string                    `json:"addr"`
	MigratingSlots []persistedSlotMigration  `json:"migrating_slots,omitempty"`
	ImportingSlots []persistedSlotMigration  `json:"importing_slots,omitempty"`
	// UsurpedSlots: FAIL 晋升时从失败节点接管的槽位清单（节点恢复时归还）。
	UsurpedSlots []persistedSlotOwner `json:"usurped_slots,omitempty"`
}

type persistedSlotMigration struct {
	Slot uint32 `json:"slot"`
	Addr string `json:"addr"`
}

type persistedNode struct {
	ID       string   `json:"id"`
	Addr     string   `json:"addr"`
	Flags    []string `json:"flags"`
	MasterID string   `json:"master_id,omitempty"`
	Epoch    int64    `json:"epoch"`
}

type persistedSlotOwner struct {
	Start  uint32 `json:"start"`
	End    uint32 `json:"end"`
	NodeID string `json:"node_id"`
}

// saveConfigLocked 写入集群配置到数据库。
// 调用者必须保证至少持有 c.mu 的读锁（RLock）。
func (c *Cluster) saveConfigLocked() error {
	state := persistedClusterState{
		NodeID: c.Myself.ID,
		Addr:   c.Myself.Addr,
		Epoch:  c.Epoch,
		Nodes:  make(map[string]*persistedNode),
	}

	for id, node := range c.Nodes {
		flags, masterID, epoch := node.PersistSnapshot()
		state.Nodes[id] = &persistedNode{
			ID:       node.ID,
			Addr:     node.Addr,
			Flags:    flags,
			MasterID: masterID,
			Epoch:    epoch,
		}
	}

	mergeSlotOwners(c.Slots, &state)

	// 持久化 FAIL 晋升时接管的槽位清单（节点恢复时归还）
	for nodeID, ranges := range c.usurpedSlots {
		for _, r := range ranges {
			state.UsurpedSlots = append(state.UsurpedSlots, persistedSlotOwner{
				Start:  r.Start,
				End:    r.End,
				NodeID: nodeID,
			})
		}
	}

	// 持久化当前节点的迁移状态（中断恢复）
	for slot, targetAddr := range c.Myself.GetMigratingSlotsMap() {
		state.MigratingSlots = append(state.MigratingSlots, persistedSlotMigration{
			Slot: slot,
			Addr: targetAddr,
		})
	}
	for slot, sourceAddr := range c.Myself.GetImportingSlotsMap() {
		state.ImportingSlots = append(state.ImportingSlots, persistedSlotMigration{
			Slot: slot,
			Addr: sourceAddr,
		})
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal cluster config: %w", err)
	}

	return c.Store.GetDB().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(configKey), data)
	})
}

// SaveConfig persists cluster state (nodes, slots, epoch) to BadgerDB.
// Called by CLUSTER SAVECONFIG and automatically on slot changes.
// 线程安全：自动获取读锁。
func (c *Cluster) SaveConfig() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.saveConfigLocked()
}

func mergeSlotOwners(slots [SlotCount]*Node, state *persistedClusterState) {
	ownerMap := make(map[string][]uint32)
	for i := uint32(0); i < SlotCount; i++ {
		if slots[i] != nil {
			ownerMap[slots[i].ID] = append(ownerMap[slots[i].ID], i)
		}
	}
	for nodeID, slotList := range ownerMap {
		ranges := mergeConsecutiveSlots(slotList)
		for _, r := range ranges {
			state.Slots = append(state.Slots, persistedSlotOwner{
				Start:  r.Start,
				End:    r.End,
				NodeID: nodeID,
			})
		}
	}
}

// LoadConfig restores cluster state from BadgerDB.
// Returns true if config was found and loaded.
func (c *Cluster) LoadConfig() (bool, error) {
	var data []byte
	err := c.Store.GetDB().View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(configKey))
		if err != nil {
			return err
		}
		data, err = item.ValueCopy(nil)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read cluster config: %w", err)
	}

	var state persistedClusterState
	if err := json.Unmarshal(data, &state); err != nil {
		return false, fmt.Errorf("unmarshal cluster config: %w", err)
	}

	c.loadState(state)
	return true, nil
}

// loadPersistedNodeID 读取持久化的节点 ID。
// 用于重启时复用相同的节点 ID，避免槽位表丢失。
// addr 用于兼容旧版本 config：顶层 node_id 字段可能为空（如 v8.34.0 之前的
// 节点目录），此时回退到从节点表中按地址匹配自己的节点 ID，避免重启后
// 生成新 ID 导致旧 ID 变幽灵节点、槽位被其他节点认领（见 v8.51.1 升级事故）。
func loadPersistedNodeID(db *badger.DB, addr string) string {
	var nodeID string
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(configKey))
		if err != nil {
			return err
		}
		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		var state persistedClusterState
		if err := json.Unmarshal(data, &state); err != nil {
			return err
		}
		nodeID = state.NodeID
		if nodeID == "" && addr != "" {
			// 旧版本 config 顶层 node_id 缺失：按地址匹配节点表中的自身条目
			for id, n := range state.Nodes {
				if n.Addr == addr {
					nodeID = id
					break
				}
			}
		}
		return nil
	})
	if err != nil {
		return ""
	}
	return nodeID
}

func (c *Cluster) loadState(state persistedClusterState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Epoch = state.Epoch

	for id, pn := range state.Nodes {
		if node, exists := c.Nodes[id]; exists {
			node.LoadFromPersisted(pn.Flags, pn.MasterID, pn.Epoch)
		} else {
			n := NewNode(pn.ID, pn.Addr)
			n.LoadFromPersisted(pn.Flags, pn.MasterID, pn.Epoch)
			c.Nodes[id] = n
		}
	}

	clearSlots(c.Slots[:])
	for _, so := range state.Slots {
		node, exists := c.Nodes[so.NodeID]
		if !exists {
			node = c.Myself
		}
		for i := so.Start; i <= so.End; i++ {
			c.Slots[i] = node
		}
		node.AddSlotRange(so.Start, so.End)
	}

	// 恢复迁移状态（中断恢复）
	for _, ms := range state.MigratingSlots {
		c.Myself.migratingSlots[ms.Slot] = ms.Addr
	}
	for _, is := range state.ImportingSlots {
		c.Myself.importingSlots[is.Slot] = is.Addr
	}

	// 恢复 FAIL 晋升时接管的槽位清单（节点恢复时归还）
	for _, u := range state.UsurpedSlots {
		c.usurpedSlots[u.NodeID] = append(c.usurpedSlots[u.NodeID], SlotRange{Start: u.Start, End: u.End})
	}
}

func clearSlots(slots []*Node) {
	for i := range slots {
		slots[i] = nil
	}
}

func copyFlags(src []string) []string {
	dst := make([]string, len(src))
	copy(dst, src)
	return dst
}

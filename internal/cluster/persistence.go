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
	Nodes  map[string]*persistedNode `json:"nodes"`
	Epoch  int64                     `json:"epoch"`
	Slots  []persistedSlotOwner      `json:"slots"`
	NodeID string                    `json:"node_id"`
	Addr   string                    `json:"addr"`
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
		state.Nodes[id] = &persistedNode{
			ID:       node.ID,
			Addr:     node.Addr,
			Flags:    copyFlags(node.Flags),
			MasterID: node.MasterID,
			Epoch:    node.Epoch,
		}
	}

	mergeSlotOwners(c.Slots, &state)

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

func (c *Cluster) loadState(state persistedClusterState) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Epoch = state.Epoch

	for id, pn := range state.Nodes {
		if node, exists := c.Nodes[id]; exists {
			node.Flags = copyFlags(pn.Flags)
			node.MasterID = pn.MasterID
			node.Epoch = pn.Epoch
		} else {
			n := NewNode(pn.ID, pn.Addr)
			n.Flags = copyFlags(pn.Flags)
			n.MasterID = pn.MasterID
			n.Epoch = pn.Epoch
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

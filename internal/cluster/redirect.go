package cluster

import (
	"fmt"
)

// RedirectError 表示需要重定向的错误
type RedirectError struct {
	Type    string // "MOVED" 或 "ASK"
	Slot    uint32
	Address string
}

func (e *RedirectError) Error() string {
	return fmt.Sprintf("%s %d %s", e.Type, e.Slot, e.Address)
}

// NewMovedError 创建MOVED重定向错误
func NewMovedError(slot uint32, address string) *RedirectError {
	return &RedirectError{
		Type:    "MOVED",
		Slot:    slot,
		Address: address,
	}
}

// NewAskError 创建ASK重定向错误
func NewAskError(slot uint32, address string) *RedirectError {
	return &RedirectError{
		Type:    "ASK",
		Slot:    slot,
		Address: address,
	}
}

// CheckSlotRedirect 检查键是否需要重定向
// 返回 MOVED（槽位不属于本节点）或 ASK（槽位正在迁出，键可能已不在本地）
func (c *Cluster) CheckSlotRedirect(key string) *RedirectError {
	slot := Slot(key)
	node := c.GetNodeBySlot(slot)

	if node == nil {
		return nil
	}

	// 如果槽位不属于当前节点，返回 MOVED 重定向
	if node.ID != c.Myself.ID {
		return NewMovedError(slot, node.Addr)
	}

	// 如果槽位属于当前节点但正在迁出（MIGRATING），返回 ASK 重定向
	if c.IsMigratingSlot(slot) {
		targetNodeID := c.Myself.GetMigratingSlotTarget(slot)
		if targetNodeID != "" {
			targetNode := c.GetNodeByID(targetNodeID)
			if targetNode != nil && targetNode.Addr != "" {
				return NewAskError(slot, targetNode.Addr)
			}
			// Fallback: use node ID as address if node not yet known.
			// Redis clients receiving this will get a redirect they can act on,
			// even if the address is technically a node ID.
			return NewAskError(slot, targetNodeID)
		}
	}

	return nil
}

// GetRedirectAddress 获取重定向地址
func (c *Cluster) GetRedirectAddress(slot uint32) (string, error) {
	node := c.GetNodeBySlot(slot)
	if node == nil {
		return "", fmt.Errorf("slot %d not assigned", slot)
	}
	return node.Addr, nil
}

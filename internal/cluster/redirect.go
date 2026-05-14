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
func (c *Cluster) CheckSlotRedirect(key string) *RedirectError {
	slot := Slot(key)
	node := c.GetNodeBySlot(slot)

	if node == nil {
		// 槽位未分配，返回错误
		return nil
	}

	// 如果槽位不属于当前节点，返回重定向
	if node.ID != c.Myself.ID {
		return NewMovedError(slot, node.Addr)
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

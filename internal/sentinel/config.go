package sentinel

import (
	"fmt"
	"strings"
)

// ConfigProvider 配置提供者
type ConfigProvider struct {
	sentinel *Sentinel
}

// NewConfigProvider 创建新的配置提供者
func NewConfigProvider(sentinel *Sentinel) *ConfigProvider {
	return &ConfigProvider{
		sentinel: sentinel,
	}
}

// GetMasterAddrByName 根据名称获取主节点地址
func (cp *ConfigProvider) GetMasterAddrByName(name string) (string, error) {
	master := cp.sentinel.GetMaster(name)
	if master == nil {
		return "", fmt.Errorf("master %s not found", name)
	}

	return master.GetAddr(), nil
}

// GetMasters 获取所有主节点信息
func (cp *ConfigProvider) GetMasters() []map[string]string {
	masters := cp.sentinel.GetAllMasters()
	result := make([]map[string]string, 0, len(masters))

	for name, master := range masters {
		info := map[string]string{
			"name":                    name,
			"ip":                      strings.Split(master.GetAddr(), ":")[0],
			"port":                    strings.Split(master.GetAddr(), ":")[1],
			"runid":                   "",
			"flags":                   master.GetState(),
			"link-pending-commands":   "0",
			"link-refcount":           "1",
			"last-ping-sent":          "0",
			"last-ok-ping-reply":      "0",
			"last-ping-reply":         "0",
			"down-after-milliseconds": fmt.Sprintf("%d", master.downAfter.Milliseconds()),
			"info-refresh":            "0",
			"role-reported":           "master",
			"role-reported-time":      "0",
			"config-epoch":            fmt.Sprintf("%d", cp.sentinel.GetConfigEpoch()),
			"num-slaves":              fmt.Sprintf("%d", len(master.GetSlaves())),
			"num-other-sentinels":     fmt.Sprintf("%d", len(master.sentinels)),
			"quorum":                  fmt.Sprintf("%d", master.quorum),
			"failover-timeout":        "180000",
			"parallel-syncs":          "1",
		}
		result = append(result, info)
	}

	return result
}

// GetSlaves 获取主节点的所有从节点
func (cp *ConfigProvider) GetSlaves(masterName string) []map[string]string {
	master := cp.sentinel.GetMaster(masterName)
	if master == nil {
		return nil
	}

	slaves := master.GetSlaves()
	result := make([]map[string]string, 0, len(slaves))

	for _, slave := range slaves {
		info := map[string]string{
			"name":                    slave.ID,
			"ip":                      strings.Split(slave.Addr, ":")[0],
			"port":                    strings.Split(slave.Addr, ":")[1],
			"runid":                   "",
			"flags":                   strings.Join(slave.Flags, ","),
			"link-pending-commands":   "0",
			"link-refcount":           "1",
			"last-ping-sent":          "0",
			"last-ok-ping-reply":      "0",
			"last-ping-reply":         "0",
			"down-after-milliseconds": "5000",
			"info-refresh":            "0",
			"role-reported":           "slave",
			"role-reported-time":      "0",
			"master-link-down-time":   "0",
			"master-link-status":      "ok",
			"master-host":             strings.Split(master.GetAddr(), ":")[0],
			"master-port":             strings.Split(master.GetAddr(), ":")[1],
			"slave-priority":          "100",
			"slave-repl-offset":       fmt.Sprintf("%d", slave.Offset),
		}
		result = append(result, info)
	}

	return result
}

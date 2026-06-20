package sentinel

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/lbp0200/BoltDB/internal/logger"
)

const configFileName = "sentinel.conf.json"

// ConfigState contains the persistent sentinel configuration.
type ConfigState struct {
	RunID       string         `json:"run_id"`
	ConfigEpoch int64          `json:"config_epoch"`
	Masters     []MasterConfig `json:"masters"`
	OtherPeers  []string       `json:"other_peers,omitempty"`
}

// MasterConfig is a persisted master entry.
type MasterConfig struct {
	Name      string `json:"name"`
	Addr      string `json:"addr"`
	Quorum    int    `json:"quorum"`
	DownAfter string `json:"down_after"`
}

type configManager struct {
	mu       sync.Mutex
	dataDir  string
	sentinel *Sentinel
}

func newConfigManager(s *Sentinel, dataDir string) *configManager {
	return &configManager{
		sentinel: s,
		dataDir:  dataDir,
	}
}

// Save persists the sentinel configuration to disk.
func (cm *configManager) Save() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.sentinel.mu.RLock()
	state := ConfigState{
		RunID:       cm.sentinel.runID,
		ConfigEpoch: cm.sentinel.configEpoch,
		OtherPeers:  append([]string{}, cm.sentinel.otherSentinels...),
	}
	cm.sentinel.mu.RUnlock()

	cm.sentinel.mu.RLock()
	for name, master := range cm.sentinel.masters {
		master.mu.RLock()
		state.Masters = append(state.Masters, MasterConfig{
			Name:      name,
			Addr:      master.addr,
			Quorum:    master.quorum,
			DownAfter: master.downAfter.String(),
		})
		master.mu.RUnlock()
	}
	cm.sentinel.mu.RUnlock()

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal sentinel config: %w", err)
	}

	if err := os.MkdirAll(cm.dataDir, 0755); err != nil {
		return fmt.Errorf("create sentinel config dir: %w", err)
	}

	path := filepath.Join(cm.dataDir, configFileName)
	return os.WriteFile(path, data, 0644)
}

// Load restores the sentinel configuration from disk.
func (cm *configManager) Load() (bool, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	path := filepath.Join(cm.dataDir, configFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("read sentinel config: %w", err)
	}

	var state ConfigState
	if err := json.Unmarshal(data, &state); err != nil {
		return false, fmt.Errorf("unmarshal sentinel config: %w", err)
	}

	cm.sentinel.mu.Lock()
	cm.sentinel.runID = state.RunID
	cm.sentinel.configEpoch = state.ConfigEpoch
	cm.sentinel.otherSentinels = state.OtherPeers
	cm.sentinel.mu.Unlock()

	// Re-register masters from config
	for _, mc := range state.Masters {
		if err := cm.sentinel.AddMaster(mc.Name, mc.Addr, mc.Quorum); err != nil {
			logger.Logger.Warn().Str("master", mc.Name).Err(err).Msg("failed to re-add master from config")
		}
	}

	return true, nil
}

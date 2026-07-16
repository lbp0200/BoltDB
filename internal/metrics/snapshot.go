package metrics

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"time"
)

type Snapshot struct {
	Time time.Time `json:"time"`

	ActiveRetries    int64   `json:"active_retries"`
	TotalRetries     int64   `json:"total_retries"`
	WritesBlocked    int64   `json:"writes_blocked"`
	L0Rejected       int64   `json:"l0_rejected"`
	L0Delayed        int64   `json:"l0_delayed"`
	L0Score          float64 `json:"l0_score"`
	QueryBudgetTrips int64   `json:"query_budget_trips,omitempty"`

	Goroutines  int    `json:"goroutines"`
	AllocBytes  uint64 `json:"alloc_bytes"`
	HeapObjects uint64 `json:"heap_objects"`
	HeapInuse   uint64 `json:"heap_inuse"`
	StackInuse  uint64 `json:"stack_inuse"`
	NumGC       uint32 `json:"num_gc"`
	LastGC      string `json:"last_gc"`

	MasterReplOffset int64  `json:"master_repl_offset"`
	SlaveReplOffset  int64  `json:"slave_repl_offset"`
	ReplicationLag   int64  `json:"replication_lag"`
	ReconnectCount   int64  `json:"reconnect_count"`
	SlaveCount       int    `json:"slave_count"`
	BacklogSize      int64  `json:"backlog_size"`
	BacklogAvailable int64  `json:"backlog_available"`
	Role             string `json:"role"`

	ActiveClients    int   `json:"active_clients"`
	BlockedClients   int   `json:"blocked_clients"`
	MonitorClients   int   `json:"monitor_clients"`
	PubSubClients    int   `json:"pubsub_clients"`
	PubSubSubs       int   `json:"pubsub_subscribers"`
	TotalOutputBytes int64 `json:"total_output_bytes"`
}

func (s Snapshot) JSON() []byte {
	b, _ := json.MarshalIndent(s, "", "  ")
	return b
}

func (s Snapshot) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "=== BoltDB Metrics %s ===\n", s.Time.Format("15:04:05"))
	fmt.Fprintf(&b, "  L0:        score=%.1f retries(active=%d total=%d blocked=%d rejected=%d delayed=%d)\n",
		s.L0Score, s.ActiveRetries, s.TotalRetries, s.WritesBlocked, s.L0Rejected, s.L0Delayed)
	if s.QueryBudgetTrips > 0 {
		fmt.Fprintf(&b, "  Query:     budget_trips=%d\n", s.QueryBudgetTrips)
	}
	fmt.Fprintf(&b, "  Go:        goroutines=%d alloc=%s heap=%s stack=%s gc=%d\n",
		s.Goroutines, bytesStr(s.AllocBytes), bytesStr(s.HeapInuse), bytesStr(s.StackInuse), s.NumGC)
	fmt.Fprintf(&b, "  Repl:      role=%s master_offset=%d slave_offset=%d lag=%d reconnects=%d slaves=%d\n",
		s.Role, s.MasterReplOffset, s.SlaveReplOffset, s.ReplicationLag, s.ReconnectCount, s.SlaveCount)
	fmt.Fprintf(&b, "  Backlog:   size=%s available=%s\n",
		bytesStr(uint64(s.BacklogSize)), bytesStr(uint64(s.BacklogAvailable)))
	fmt.Fprintf(&b, "  Clients:   active=%d blocked=%d monitor=%d pubsub=%d subs=%d output=%s\n",
		s.ActiveClients, s.BlockedClients, s.MonitorClients, s.PubSubClients, s.PubSubSubs, bytesStr(uint64(s.TotalOutputBytes)))
	return b.String()
}

func bytesStr(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func CollectRuntime() (goroutines int, mem runtime.MemStats) {
	runtime.ReadMemStats(&mem)
	return runtime.NumGoroutine(), mem
}

func formatLastGC(nano uint64) string {
	if nano == 0 {
		return "never"
	}
	return time.Unix(0, int64(nano)).Format("15:04:05.000")
}

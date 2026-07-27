package metrics

import (
	"fmt"
	"strings"
)

// prometheusText 将 Snapshot 渲染为 Prometheus 文本格式（exposition format）。
// 符合 https://prometheus.io/docs/instrumenting/exposition_formats/
func prometheusText(s Snapshot) string {
	var b strings.Builder

	b.WriteString("# HELP boltdb_build_info BoltDB build info\n")
	b.WriteString("# TYPE boltdb_build_info gauge\n")
	b.WriteString("boltdb_build_info{version=\"8.50.1\"} 1\n")

	// L0 / Write Backpressure
	b.WriteString("# HELP boltdb_l0_score L0 compaction score\n")
	b.WriteString("# TYPE boltdb_l0_score gauge\n")
	fmt.Fprintf(&b, "boltdb_l0_score %.1f\n", s.L0Score)

	b.WriteString("# HELP boltdb_active_retries Number of active write retries\n")
	b.WriteString("# TYPE boltdb_active_retries gauge\n")
	fmt.Fprintf(&b, "boltdb_active_retries %d\n", s.ActiveRetries)

	b.WriteString("# HELP boltdb_total_retries Cumulative write retries\n")
	b.WriteString("# TYPE boltdb_total_retries counter\n")
	fmt.Fprintf(&b, "boltdb_total_retries %d\n", s.TotalRetries)

	b.WriteString("# HELP boltdb_writes_blocked Writes blocked by semaphore\n")
	b.WriteString("# TYPE boltdb_writes_blocked gauge\n")
	fmt.Fprintf(&b, "boltdb_writes_blocked %d\n", s.WritesBlocked)

	b.WriteString("# HELP boltdb_writes_l0_rejected Writes rejected by L0 threshold\n")
	b.WriteString("# TYPE boltdb_writes_l0_rejected counter\n")
	fmt.Fprintf(&b, "boltdb_writes_l0_rejected %d\n", s.L0Rejected)

	b.WriteString("# HELP boltdb_writes_l0_delayed Writes delayed by L0 backpressure\n")
	b.WriteString("# TYPE boltdb_writes_l0_delayed counter\n")
	fmt.Fprintf(&b, "boltdb_writes_l0_delayed %d\n", s.L0Delayed)

	// Go runtime
	b.WriteString("# HELP boltdb_goroutines Current goroutine count\n")
	b.WriteString("# TYPE boltdb_goroutines gauge\n")
	fmt.Fprintf(&b, "boltdb_goroutines %d\n", s.Goroutines)

	b.WriteString("# HELP boltdb_alloc_bytes Heap alloc bytes\n")
	b.WriteString("# TYPE boltdb_alloc_bytes gauge\n")
	fmt.Fprintf(&b, "boltdb_alloc_bytes %d\n", s.AllocBytes)

	b.WriteString("# HELP boltdb_heap_inuse Heap inuse bytes\n")
	b.WriteString("# TYPE boltdb_heap_inuse gauge\n")
	fmt.Fprintf(&b, "boltdb_heap_inuse %d\n", s.HeapInuse)

	b.WriteString("# HELP boltdb_stack_inuse Stack inuse bytes\n")
	b.WriteString("# TYPE boltdb_stack_inuse gauge\n")
	fmt.Fprintf(&b, "boltdb_stack_inuse %d\n", s.StackInuse)

	b.WriteString("# HELP boltdb_gc_total Total GC cycles\n")
	b.WriteString("# TYPE boltdb_gc_total counter\n")
	fmt.Fprintf(&b, "boltdb_gc_total %d\n", s.NumGC)

	// Replication
	b.WriteString("# HELP boltdb_role Server role (1=master, 0=slave)\n")
	b.WriteString("# TYPE boltdb_role gauge\n")
	roleVal := "0"
	if s.Role == "master" {
		roleVal = "1"
	}
	fmt.Fprintf(&b, "boltdb_role %s\n", roleVal)

	b.WriteString("# HELP boltdb_master_repl_offset Master replication offset\n")
	b.WriteString("# TYPE boltdb_master_repl_offset gauge\n")
	fmt.Fprintf(&b, "boltdb_master_repl_offset %d\n", s.MasterReplOffset)

	b.WriteString("# HELP boltdb_slave_repl_offset Slave replication offset\n")
	b.WriteString("# TYPE boltdb_slave_repl_offset gauge\n")
	fmt.Fprintf(&b, "boltdb_slave_repl_offset %d\n", s.SlaveReplOffset)

	b.WriteString("# HELP boltdb_replication_lag Master-slave replication lag\n")
	b.WriteString("# TYPE boltdb_replication_lag gauge\n")
	fmt.Fprintf(&b, "boltdb_replication_lag %d\n", s.ReplicationLag)

	b.WriteString("# HELP boltdb_slave_count Connected slave count\n")
	b.WriteString("# TYPE boltdb_slave_count gauge\n")
	fmt.Fprintf(&b, "boltdb_slave_count %d\n", s.SlaveCount)

	b.WriteString("# HELP boltdb_reconnect_count Slave reconnect count\n")
	b.WriteString("# TYPE boltdb_reconnect_count counter\n")
	fmt.Fprintf(&b, "boltdb_reconnect_count %d\n", s.ReconnectCount)

	// Backlog
	b.WriteString("# HELP boltdb_backlog_size Replication backlog size bytes\n")
	b.WriteString("# TYPE boltdb_backlog_size gauge\n")
	fmt.Fprintf(&b, "boltdb_backlog_size %d\n", s.BacklogSize)

	b.WriteString("# HELP boltdb_backlog_available Replication backlog available bytes\n")
	b.WriteString("# TYPE boltdb_backlog_available gauge\n")
	fmt.Fprintf(&b, "boltdb_backlog_available %d\n", s.BacklogAvailable)

	// Clients
	b.WriteString("# HELP boltdb_clients_active Active client connections\n")
	b.WriteString("# TYPE boltdb_clients_active gauge\n")
	fmt.Fprintf(&b, "boltdb_clients_active %d\n", s.ActiveClients)

	b.WriteString("# HELP boltdb_clients_blocked Blocked client connections\n")
	b.WriteString("# TYPE boltdb_clients_blocked gauge\n")
	fmt.Fprintf(&b, "boltdb_clients_blocked %d\n", s.BlockedClients)

	b.WriteString("# HELP boltdb_clients_monitor Monitor client connections\n")
	b.WriteString("# TYPE boltdb_clients_monitor gauge\n")
	fmt.Fprintf(&b, "boltdb_clients_monitor %d\n", s.MonitorClients)

	b.WriteString("# HELP boltdb_clients_pubsub PubSub client connections\n")
	b.WriteString("# TYPE boltdb_clients_pubsub gauge\n")
	fmt.Fprintf(&b, "boltdb_clients_pubsub %d\n", s.PubSubClients)

	b.WriteString("# HELP boltdb_pubsub_subscribers PubSub subscribers\n")
	b.WriteString("# TYPE boltdb_pubsub_subscribers gauge\n")
	fmt.Fprintf(&b, "boltdb_pubsub_subscribers %d\n", s.PubSubSubs)

	b.WriteString("# HELP boltdb_total_output_bytes Total output bytes sent to clients\n")
	b.WriteString("# TYPE boltdb_total_output_bytes counter\n")
	fmt.Fprintf(&b, "boltdb_total_output_bytes %d\n", s.TotalOutputBytes)

	return b.String()
}

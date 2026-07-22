package server

import (
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/replication"
)

// Version 版本号，通过 ldflags 注入
var Version = "8.39.1"

// GitCommitID Git commit ID，通过 ldflags 注入
var GitCommitID = ""

// BuildTime 编译时间，通过 ldflags 注入
var BuildTime = ""

// buildInfoResponse 构建INFO响应
// 增强对 redis-sentinel 的兼容性
func (h *Handler) buildInfoResponse(section string) string {
	var builder strings.Builder

	if section == "" || section == "ALL" || section == "SERVER" {
		builder.WriteString("# Server\n")
		builder.WriteString("redis_version:boltdb-" + Version + "\n")
		builder.WriteString("git_commit_id:" + GitCommitID + "\n")
		builder.WriteString("build_time:" + BuildTime + "\n")
		builder.WriteString("os:" + runtime.GOOS + "\n")
		builder.WriteString("arch_bits:64\n")
		builder.WriteString("tcp_port:" + strconv.Itoa(h.Port) + "\n")
		if runtime.GOOS == "linux" {
			builder.WriteString("multiplexing_api:epoll\n")
		} else {
			builder.WriteString("multiplexing_api:kqueue\n")
		}
		builder.WriteString("gcc_version:" + runtime.Version() + "\n")
		builder.WriteString("process_id:" + strconv.Itoa(os.Getpid()) + "\n")
		builder.WriteString("run_id:\n")
		builder.WriteString("tcp_backlog:511\n")
		builder.WriteString("uptime_in_seconds:0\n")
		builder.WriteString("uptime_in_days:0\n")
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "REPLICATION" {
		builder.WriteString("# Replication\n")
		if h.Replication != nil {
			role := h.Replication.GetRole()
			builder.WriteString("role:" + role + "\n")

			switch role {
			case replication.RoleMaster:
				builder.WriteString("connected_slaves:" + strconv.Itoa(h.Replication.GetSlaveCount()) + "\n")
				builder.WriteString("master_replid:" + h.Replication.GetReplicationID() + "\n")
				builder.WriteString("master_repl_offset:" + strconv.FormatInt(h.Replication.GetMasterReplOffset(), 10) + "\n")
				builder.WriteString("second_repl_offset:-1\n")
				builder.WriteString("repl_backlog_active:1\n")
				builder.WriteString("repl_backlog_size:1048576\n")
				builder.WriteString("repl_backlog_first_byte_offset:0\n")
				builder.WriteString("repl_backlog_histlen:0\n")

				slaves := h.Replication.GetSlaves()
				for i, slave := range slaves {
					builder.WriteString("slave" + strconv.Itoa(i) + ":ip=" + slave.Addr + ",port=6379,state=online,offset=" + strconv.FormatInt(slave.GetReplOffset(), 10) + ",lag=0\n")
				}
			case replication.RoleSlave:
				masterAddr := h.Replication.GetMasterAddr()
				if masterAddr != "" {
					builder.WriteString("master_host:" + strings.Split(masterAddr, ":")[0] + "\n")
					if parts := strings.Split(masterAddr, ":"); len(parts) > 1 {
						builder.WriteString("master_port:" + parts[1] + "\n")
					}
				}
				builder.WriteString("master_link_status:up\n")
				builder.WriteString("master_link_down_since_seconds:0\n")
				builder.WriteString("slave_priority:100\n")
				builder.WriteString("slave_read_only:1\n")
				builder.WriteString("replica_announced:1\n")
				builder.WriteString("connected_slaves:0\n")
				builder.WriteString("master_replid:" + h.Replication.GetReplicationID() + "\n")
				builder.WriteString("master_repl_offset:" + strconv.FormatInt(h.Replication.GetMasterReplOffset(), 10) + "\n")
				builder.WriteString("slave_repl_offset:" + strconv.FormatInt(h.Replication.GetSlaveReplOffset(), 10) + "\n")
				builder.WriteString("second_repl_offset:-1\n")
				builder.WriteString("repl_backlog_active:0\n")
				builder.WriteString("repl_backlog_size:0\n")
				builder.WriteString("repl_backlog_first_byte_offset:0\n")
				builder.WriteString("repl_backlog_histlen:0\n")
			case "sentinel":
				builder.WriteString("connected_slaves:0\n")
				builder.WriteString("master_replid:8371b4fb5d6973276c54b0f0ab738c2e6f00fa8d\n")
				builder.WriteString("master_repl_offset:0\n")
				builder.WriteString("second_repl_offset:-1\n")
			}
		}
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "PERSISTENCE" {
		builder.WriteString("# Persistence\n")
		if h.Backup != nil {
			lastSave := h.Backup.LastSave()
			builder.WriteString("rdb_last_save_time:" + strconv.FormatInt(lastSave, 10) + "\n")
			builder.WriteString("rdb_changes_since_last_save:0\n")
		}
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "STATS" {
		builder.WriteString("# Stats\n")
		builder.WriteString("total_commands_processed:0\n")
		builder.WriteString("instantaneous_ops_per_sec:0\n")
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "CLUSTER" {
		builder.WriteString("# Cluster\n")
		if h.Cluster != nil {
			builder.WriteString("cluster_enabled:1\n")
		} else {
			builder.WriteString("cluster_enabled:0\n")
		}
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "CLIENTS" {
		builder.WriteString("# Clients\n")
		builder.WriteString("connected_clients:" + strconv.Itoa(h.ActiveClientCount()) + "\n")
		builder.WriteString("cluster_connections:0\n")
		builder.WriteString("maxclients:" + strconv.Itoa(h.GetMaxClients()) + "\n")
		builder.WriteString("blocked_clients:" + strconv.Itoa(h.BlockedClientCount()) + "\n")
		builder.WriteString("tracking_clients:0\n")
		builder.WriteString("max_blocking_keys:0\n")
		builder.WriteString("io_threads_active:0\n")
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "MEMORY" {
		builder.WriteString("# Memory\n")
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		builder.WriteString("used_memory:" + strconv.FormatUint(m.Alloc, 10) + "\n")
		builder.WriteString("used_memory_human:" + formatBytes(m.Alloc) + "\n")
		builder.WriteString("used_memory_rss:" + strconv.FormatUint(m.Sys, 10) + "\n")
		builder.WriteString("used_memory_rss_human:" + formatBytes(m.Sys) + "\n")
		builder.WriteString("mem_fragmentation_ratio:" + strconv.FormatFloat(float64(m.Sys)/float64(m.Alloc), 'f', 2, 64) + "\n")
		builder.WriteString("maxmemory:0\n")
		builder.WriteString("maxmemory_human:0B\n")
		builder.WriteString("maxmemory_policy:noeviction\n")
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "CPU" {
		builder.WriteString("# CPU\n")
		builder.WriteString("used_cpu_sys:0.00\n")
		builder.WriteString("used_cpu_user:0.00\n")
		builder.WriteString("used_cpu_sys_children:0.00\n")
		builder.WriteString("used_cpu_user_children:0.00\n")
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "KEYSPACE" {
		builder.WriteString("# Keyspace\n")
		// Per-DB stats - simplified (no per-DB tracking in BoltDB)
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "COMMANDSTATS" {
		builder.WriteString("# Commandstats\n")
		// No per-command stats in BoltDB (hardcoded for compatibility)
		builder.WriteString("\n")
	}

	if section == "" || section == "ALL" || section == "LATENCY" {
		builder.WriteString("# Latency\n")
		builder.WriteString(" Latest latency events:\n")
		builder.WriteString("\n")
	}

	return builder.String()
}

// formatBytes formats bytes as human-readable string
func formatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return strconv.FormatFloat(float64(bytes)/float64(GB), 'f', 2, 64) + "G"
	case bytes >= MB:
		return strconv.FormatFloat(float64(bytes)/float64(MB), 'f', 2, 64) + "M"
	case bytes >= KB:
		return strconv.FormatFloat(float64(bytes)/float64(KB), 'f', 2, 64) + "K"
	default:
		return strconv.FormatUint(bytes, 10) + "B"
	}
}

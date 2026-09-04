package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

// Config 是 BoltDB TOML 配置文件的完整结构
// 所有字段都有默认值，未在配置文件中填写的字段走 CLI flag / 自动推导
type Config struct {
	Server      ServerConfig      `toml:"server"`
	Memory      MemoryConfig      `toml:"memory"`
	Replication ReplicationConfig `toml:"replication"`
	TLS         TLSConfig         `toml:"tls"`
}

type ServerConfig struct {
	Addr       string `toml:"addr"`
	Dir        string `toml:"dir"`
	LogLevel   string `toml:"log-level"`
	MaxClients int    `toml:"max-clients"`
	Timeout    int    `toml:"timeout"` // seconds, 0 = no timeout
}

type MemoryConfig struct {
	GOMEMLIMIT              string `toml:"gomemlimit"`
	MaxInputBytes           string `toml:"max-input-bytes"`
	ClientOutputBufferLimit string `toml:"client-output-buffer-limit"`
	ProtoMaxBulkLen         string `toml:"proto-max-bulk-len"`
}

type ReplicationConfig struct {
	BacklogSize string `toml:"backlog-size"`
	ReplicaOf   string `toml:"replicaof"`
	FeedLoop    bool   `toml:"feedloop"`
}

type TLSConfig struct {
	Cert    string `toml:"cert"`
	Key     string `toml:"key"`
	CA      string `toml:"ca"`
	Require bool   `toml:"require"`
}

// DefaultConfig 返回出厂默认配置（4C8G 基准）
func DefaultConfig() Config {
	return Config{
		Server: ServerConfig{
			Addr:       ":6337",
			Dir:        "",
			LogLevel:   "",
			MaxClients: 10000,
			Timeout:    0,
		},
		Memory: MemoryConfig{
			GOMEMLIMIT:              "",
			MaxInputBytes:           "1GB",
			ClientOutputBufferLimit: "32MB",
			ProtoMaxBulkLen:         "64MB",
		},
		Replication: ReplicationConfig{
			BacklogSize: "",
			ReplicaOf:   "",
		},
		TLS: TLSConfig{
			Cert:    "",
			Key:     "",
			CA:      "",
			Require: false,
		},
	}
}

// parseBytes 解析人类可读的大小字符串，如 "32MB", "1GB", "256MB"
// 支持: B, KB, MB, GB, TB（大小写不敏感，无分隔符）
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size string")
	}

	// 尝试纯数字（字节）
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}

	s = strings.ToUpper(s)
	var multiplier int64
	switch {
	case strings.HasSuffix(s, "TB"):
		multiplier = 1 << 40
		s = strings.TrimSuffix(s, "TB")
	case strings.HasSuffix(s, "GB"):
		multiplier = 1 << 30
		s = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "MB"):
		multiplier = 1 << 20
		s = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KB"):
		multiplier = 1 << 10
		s = strings.TrimSuffix(s, "KB")
	case strings.HasSuffix(s, "B"):
		multiplier = 1
		s = strings.TrimSuffix(s, "B")
	default:
		return 0, fmt.Errorf("unrecognized size suffix: %q", s)
	}

	s = strings.TrimSpace(s)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size number %q: %w", s, err)
	}
	return n * multiplier, nil
}

// loadConfigFile 读取并解析 TOML 配置文件
func loadConfigFile(path string) (*Config, error) {
	cfg := DefaultConfig()
	md, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		return nil, fmt.Errorf("config file %s contains unknown keys: %v", path, undecoded)
	}
	return &cfg, nil
}

// applyConfigOverlay 将配置文件中的值应用到 CLI flag（仅当 CLI 未显式设置时覆盖）
// 返回被配置文件设置的 flag 名称集合，供后续自动推导判断使用
func applyConfigOverlay(cfg *Config, seenFlags map[string]bool) map[string]bool {
	setByConfig := make(map[string]bool)

	// === Server ===
	if !seenFlags["addr"] && cfg.Server.Addr != "" {
		*addrFlag = cfg.Server.Addr
		setByConfig["addr"] = true
	}
	if !seenFlags["dir"] && cfg.Server.Dir != "" {
		*dbPathFlag = cfg.Server.Dir
		setByConfig["dir"] = true
	}
	if !seenFlags["log-level"] && cfg.Server.LogLevel != "" {
		*logLevelFlag = cfg.Server.LogLevel
		setByConfig["log-level"] = true
	}
	if !seenFlags["maxclients"] && cfg.Server.MaxClients > 0 {
		*maxClientsFlag = cfg.Server.MaxClients
		setByConfig["maxclients"] = true
	}
	if !seenFlags["timeout"] && cfg.Server.Timeout > 0 {
		*idleTimeoutFlag = cfg.Server.Timeout
		setByConfig["timeout"] = true
	}

	// === Memory ===
	if !seenFlags["client-output-buffer-limit"] && cfg.Memory.ClientOutputBufferLimit != "" {
		if v, err := parseBytes(cfg.Memory.ClientOutputBufferLimit); err == nil {
			*clientOutputBufferLimit = v
			setByConfig["client-output-buffer-limit"] = true
		}
	}
	if !seenFlags["max-input-bytes"] && cfg.Memory.MaxInputBytes != "" {
		if v, err := parseBytes(cfg.Memory.MaxInputBytes); err == nil {
			*maxInputBytesFlag = v
			setByConfig["max-input-bytes"] = true
		}
	}
	if !seenFlags["proto-max-bulk-len"] && cfg.Memory.ProtoMaxBulkLen != "" {
		if v, err := parseBytes(cfg.Memory.ProtoMaxBulkLen); err == nil {
			*protoMaxBulkLenFlag = v
			setByConfig["proto-max-bulk-len"] = true
		}
	}
	// GOMEMLIMIT is handled by environment variable + auto-detect; config file value is advisory
	// If user wants a specific GOMEMLIMIT, they should set the env var.

	// === Replication ===
	if !seenFlags["repl-backlog-size"] && cfg.Replication.BacklogSize != "" {
		*replBacklogSizeFlag = cfg.Replication.BacklogSize
		setByConfig["repl-backlog-size"] = true
	}
	if !seenFlags["replicaof"] && cfg.Replication.ReplicaOf != "" {
		*replicaofFlag = cfg.Replication.ReplicaOf
		setByConfig["replicaof"] = true
	}
	if !seenFlags["feed-loop"] && cfg.Replication.FeedLoop {
		*feedLoopFlag = true
		setByConfig["feed-loop"] = true
	}

	// === TLS ===
	if !seenFlags["tls-cert"] && cfg.TLS.Cert != "" {
		*tlsCertFlag = cfg.TLS.Cert
		setByConfig["tls-cert"] = true
	}
	if !seenFlags["tls-key"] && cfg.TLS.Key != "" {
		*tlsKeyFlag = cfg.TLS.Key
		setByConfig["tls-key"] = true
	}
	if !seenFlags["tls-ca"] && cfg.TLS.CA != "" {
		*tlsCAFlag = cfg.TLS.CA
		setByConfig["tls-ca"] = true
	}
	if !seenFlags["tls-require"] && cfg.TLS.Require {
		*tlsRequireFlag = cfg.TLS.Require
		setByConfig["tls-require"] = true
	}

	return setByConfig
}

// dumpConfigTemplate 将默认配置以 TOML 格式写入标准输出（供 --dump-config 使用）
func dumpConfigTemplate() {
	cfg := DefaultConfig()
	// 使用 fmt.Fprintf + 逐行输出替代多行字符串，保证注释完整性
	out := func(format string, args ...interface{}) {
		fmt.Fprintf(os.Stderr, format, args...)
	}

	out("##############################################\n")
	out("# BoltDB Configuration\n")
	out("# 优化基准: CPU 4 核 / RAM 8GB / SSD\n")
	out("#\n")
	out("# 使用方式:\n")
	out("#   boltDB --dump-config > boltdb.toml\n")
	out("#   boltDB -config=boltdb.toml\n")
	out("#\n")
	out("# 优先级链（高 → 低）:\n")
	out("#   CLI flag > 配置文件 > 自动推导(基于RAM) > 硬编码默认值\n")
	out("#\n")
	out("# 环境变量:\n")
	out("#   BOLTDB_ADDR      覆盖 [server] addr\n")
	out("#   BOLTDB_DIR       覆盖 [server] dir\n")
	out("#   BOLTDB_LOG_LEVEL 覆盖 [server] log-level\n")
	out("#   GOMEMLIMIT       覆盖 [memory] gomemlimit\n")
	out("##############################################\n")
	out("\n")
	out("[server]\n")
	out("  # 监听地址，格式 host:port\n")
	out("  # 环境变量: BOLTDB_ADDR\n")
	out("  # 默认: \":6337\"（区别于 Redis 默认 :6379）\n")
	out("  # addr = \"%s\"\n", cfg.Server.Addr)
	out("\n")
	out("  # 数据目录，BadgerDB 存储路径\n")
	out("  # 环境变量: BOLTDB_DIR\n")
	out("  # 默认: 系统临时目录\n")
	out("  # 生产环境务必指定持久化路径，如 /var/lib/boltdb\n")
	out("  # dir = \"%s\"\n", cfg.Server.Dir)
	out("\n")
	out("  # 日志级别，可选: DEBUG | INFO | WARNING | ERROR\n")
	out("  # 环境变量: BOLTDB_LOG_LEVEL\n")
	out("  # 默认: WARNING（不设置时日志最少）\n")
	out("  # log-level = \"%s\"\n", cfg.Server.LogLevel)
	out("\n")
	out("  # 最大并发连接数，0 表示不限制\n")
	out("  # 超限时新连接被拒绝并返回 ERR max number of clients reached\n")
	out("  # 4C8G 基准: 10000（约占用 800MB 内存用于连接缓冲区）\n")
	out("  # 2GB VPS 建议: 1000\n")
	out("  # max-clients = %d\n", cfg.Server.MaxClients)
	out("\n")
	out("  # 空闲连接超时秒数，0 表示不超时\n")
	out("  # 到达超时后服务器主动断开该连接\n")
	out("  # 生产环境建议设置 300（5 分钟）防止 zombie 连接堆积\n")
	out("  # timeout = %d\n", cfg.Server.Timeout)
	out("\n")
	out("[memory]\n")
	out("  # Go 堆内存软限制，格式如 \"6GB\"、\"4GB\"\n")
	out("  # 环境变量: GOMEMLIMIT（优先级最高，推荐用环境变量设置）\n")
	out("  # 未设置时自动计算: totalRAM - 2GB，不低于 256MB，不超过 90%%\n")
	out("  # 作用: 告诉 Go GC 在堆接近此值时积极回收，防止 OOM\n")
	out("  # 注意: 这是软限制，瞬时峰值可能超过此值\n")
	out("  # gomemlimit = \"%s\"\n", cfg.Memory.GOMEMLIMIT)
	out("\n")
	out("  # 每连接累计输入字节上限，0 表示不限制\n")
	out("  # 防止慢速攻击: 单连接长期发送大量请求耗服务器内存\n")
	out("  # 4C8G 基准: 1GB，超限服务器主动断开连接\n")
	out("  # 64GB 服务器建议: 8GB，2GB VPS 建议: 128MB\n")
	out("  # max-input-bytes = \"%s\"\n", cfg.Memory.MaxInputBytes)
	out("\n")
	out("  # 每连接输出缓冲区硬限制(bytes)，0 表示不限制\n")
	out("  # 保护: 慢客户端不消费响应时积压数据无界增长\n")
	out("  # 超限服务器主动断开该连接\n")
	out("  # 4C8G 基准: 32MB，自动推导: min(32MB, RAM/256)\n")
	out("  # PubSub 订阅者建议单独配置更小值\n")
	out("  # client-output-buffer-limit = \"%s\"\n", cfg.Memory.ClientOutputBufferLimit)
	out("\n")
	out("  # RESP 协议 bulk string 最大长度(bytes)\n")
	out("  # 防止客户端发送超大单条数据耗尽解析内存\n")
	out("  # 4C8G 基准: 64MB（256MB 对 8GB 机器过于激进）\n")
	out("  # proto-max-bulk-len = \"%s\"\n", cfg.Memory.ProtoMaxBulkLen)
	out("\n")
	out("[replication]\n")
	out("  # 复制积压缓冲区大小，格式如 \"100mb\"、\"1gb\"\n")
	out("  # 决定 PARTIAL RESYNC (PSYNC) 能补多大范围的丢失数据\n")
	out("  # 积压越大，从节点短时断连后越可能走 CONTINUE 而非 FULLRESYNC\n")
	out("  # 默认: 1MB，高写入量建议: 100MB 或更大\n")
	out("  # backlog-size = \"%s\"\n", cfg.Replication.BacklogSize)
	out("\n")
	out("  # 主从复制: 指定主节点地址 host:port\n")
	out("  # 设置后本节点启动后自动连接主节点并执行 PSYNC\n")
	out("  # 运行时也可通过 SLAVEOF / REPLICAOF 命令动态切换\n")
	out("  # replicaof = \"%s\"\n", cfg.Replication.ReplicaOf)
	out("\n")
	out("[tls]\n")
	out("  # TLS 证书 PEM 文件路径\n")
	out("  # 启用 TLS 后，所有客户端连接必须使用 TLS 加密\n")
	out("  # 与 tls-key 必须同时设置\n")
	out("  # cert = \"%s\"\n", cfg.TLS.Cert)
	out("\n")
	out("  # TLS 私钥 PEM 文件路径\n")
	out("  # 与 tls-cert 必须同时设置\n")
	out("  # key = \"%s\"\n", cfg.TLS.Key)
	out("\n")
	out("  # CA 证书 PEM 文件路径（可选，用于客户端证书验证）\n")
	out("  # 设置后服务器要求客户端提供有效证书\n")
	out("  # ca = \"%s\"\n", cfg.TLS.CA)
	out("\n")
	out("  # 是否拒绝非 TLS 连接\n")
	out("  # tls-require = true 时，非 TLS 连接直接被拒绝\n")
	out("  # 仅在 tls-cert 和 tls-key 已设置时生效\n")
	out("  # require = %v\n", cfg.TLS.Require)
}

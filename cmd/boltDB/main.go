package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/metrics"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
)

// Command-line flags - defined at package level for testability
var (
	addrFlag                = flag.String("addr", getEnv("BOLTDB_ADDR", ":6337"), "listen addr (or BOLTDB_ADDR env)")
	dbPathFlag              = flag.String("dir", getEnv("BOLTDB_DIR", os.TempDir()), "badger dir (or BOLTDB_DIR env)")
	logLevelFlag            = flag.String("log-level", "", "log level: DEBUG, INFO, WARNING, ERROR (default: WARNING, or BOLTDB_LOG_LEVEL env)")
	clusterEnabledFlag      = flag.Bool("cluster", false, "enable cluster mode")
	replicaofFlag           = flag.String("replicaof", "", "replicaof master host:port")
	skipStartupCleanup      = flag.Bool("skip-startup-cleanup", false, "skip startup cleanup (data integrity check)")
	clientOutputBufferLimit = flag.Int64("client-output-buffer-limit", 32<<20, "per-client output buffer hard limit in bytes (default 32MB, 0 = unlimited)")
	replBacklogSizeFlag     = flag.String("repl-backlog-size", "", "replication backlog size (e.g. 100mb, 1gb, default 1mb)")
	gossipIntervalFlag      = flag.Duration("gossip-interval", 1*time.Second, "cluster gossip PING interval (default 1s, e.g. 5s to reduce idle CPU)")
	metricsAddrFlag         = flag.String("metrics-addr", "", "metrics HTTP listen addr (e.g. :6338, empty = disabled)")
	maxClientsFlag          = flag.Int("maxclients", 10000, "max number of connected clients (0 = unlimited)")
	idleTimeoutFlag         = flag.Int("timeout", 0, "idle client timeout in seconds (0 = no timeout)")
	protoMaxBulkLenFlag     = flag.Int64("proto-max-bulk-len", 64*1024*1024, "max bulk string length in bytes (default 64MB, recommended for 4C8G)")
	maxInputBytesFlag       = flag.Int64("max-input-bytes", 1<<30, "per-client cumulative input byte limit (default 1GB, 0 = unlimited)")
	tlsCertFlag             = flag.String("tls-cert", "", "path to TLS certificate PEM file (empty = no TLS)")
	tlsKeyFlag              = flag.String("tls-key", "", "path to TLS private key PEM file (empty = no TLS)")
	tlsCAFlag               = flag.String("tls-ca", "", "path to CA certificate PEM file for client verification (optional)")
	tlsRequireFlag          = flag.Bool("tls-require", false, "reject non-TLS connections when TLS is enabled")
	configFileFlag          = flag.String("config", "", "path to TOML config file")
	dumpConfigFlag          = flag.Bool("dump-config", false, "print default config template and exit")
	// queryBudgetMaxScan=0 means unlimited (compat default). Large deployments should set e.g. 1_000_000.
	queryBudgetMaxScanFlag = flag.Int64("query-budget-max-scan", 0, "max iterations per scan (GEO/Z* etc); 0 = unlimited")
)

// getEnv returns the value of the environment variable named by key, or
// fallback if the variable is not set or empty.
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// detectTotalMemory 检测系统总内存（字节数）。
// 返回 0 表示无法检测。
func detectTotalMemory() int64 {
	// Linux: /proc/meminfo MemTotal (kB)
	data, err := os.ReadFile("/proc/meminfo")
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				var totalKB int64
				if _, err := fmt.Sscanf(line, "MemTotal: %d kB", &totalKB); err == nil && totalKB > 0 {
					return totalKB * 1024
				}
			}
		}
	}

	// macOS: sysctl hw.memsize (bytes)
	_, err = os.Stat("/usr/sbin/sysctl")
	if err == nil {
		// sysctl is available, try to execute it
		cmd := exec.Command("/usr/sbin/sysctl", "-n", "hw.memsize")
		output, err := cmd.Output()
		if err == nil {
			var totalBytes int64
			if _, err := fmt.Sscanf(strings.TrimSpace(string(output)), "%d", &totalBytes); err == nil && totalBytes > 0 {
				return totalBytes
			}
		}
	}

	return 0
}

// formatBytes 将字节数格式化为人类可读的字符串（如 "1GB", "64MB"）
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; exp++ {
		div *= unit
		n /= unit
	}
	return fmt.Sprintf("%.0f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func main() {
	flag.Parse()

	// 记录哪些 flag 被用户显式设置（用于后续 auto-config 判断：手动设置优先）
	seenFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		seenFlags[f.Name] = true
	})

	// --dump-config: 打印默认配置模板并退出
	if *dumpConfigFlag {
		dumpConfigTemplate()
		os.Exit(0)
	}

	// 如果指定了 -config，加载 TOML 配置文件并覆盖未被 CLI 显式设置的 flag
	cfgSet := make(map[string]bool) // 被配置文件设置的 flag
	if *configFileFlag != "" {
		cfg, err := loadConfigFile(*configFileFlag)
		if err != nil {
			logger.Logger.Fatal().Err(err).Str("config", *configFileFlag).Msg("Failed to load config file")
		}
		cfgSet = applyConfigOverlay(cfg, seenFlags)
		logger.Logger.Info().Str("file", *configFileFlag).Msg("Config file loaded")
	}

	// 合并被设置的 flag（CLI + 配置文件），用于后续自动推导判断
	isSet := func(name string) bool {
		return seenFlags[name] || cfgSet[name]
	}

	// 启动时校验 isWriteCommand map 与 dispatch switch 一致性
	if err := server.ValidateWriteCommandConsistency(); err != nil {
		logger.Logger.Fatal().Err(err).Msg("Write command consistency check failed: isWriteCommand map out of sync with dispatch switch")
	}

	// 设置日志级别
	if *logLevelFlag != "" {
		logger.SetLevelFromString(*logLevelFlag)
	}

	// 设置 RESP 协议限制
	proto.SetMaxBulkLen(*protoMaxBulkLenFlag)

	// 自动设置 GOMEMLIMIT（Go 堆软限制），防止高并发下堆无界增长导致 OOM
	// 如果 GOMEMLIMIT 环境变量已设置，优先尊重用户显式配置
	// 检测策略：
	//   - Linux: /proc/meminfo MemTotal（kB）
	//   - macOS: sysctl hw.memsize（bytes）
	//   - 其他/失败: 不设置（fallback 到 Go 运行时默认行为）
	// 公式：limit = min(total - 2GB, total × 90%)，不低于 256MB
	if os.Getenv("GOMEMLIMIT") == "" {
		if limitBytes := detectTotalMemory(); limitBytes > 0 {
			const (
				reserveBytes = 2 << 30   // 2GB
				minLimit     = 256 << 20 // 256MB
			)
			limit := limitBytes - reserveBytes
			if limit <= 0 {
				limit = limitBytes * 75 / 100
			}
			// 不超过总内存的 90%
			pctLimit := int64(float64(limitBytes) * 0.9)
			if limit > pctLimit {
				limit = pctLimit
			}
			if limit < minLimit {
				limit = minLimit
			}
			debug.SetMemoryLimit(limit)
			logger.Logger.Info().Int64("bytes", limit).Msg("GOMEMLIMIT 自动设置完成")
		} else {
			logger.Logger.Info().Msg("无法检测系统内存，跳过 GOMEMLIMIT 自动设置（可手动设置 GOMEMLIMIT 环境变量）")
		}
	} else {
		logger.Logger.Info().Msg("GOMEMLIMIT 环境变量已设置，跳过自动检测")
	}

	// 自动推导基于 RAM 比例的内存参数（仅当用户未手动或配置文件设置时）
	// 公式：
	//   client-output-buffer-limit = min(32MB, RAM/256)
	//   max-input-bytes = min(1GB, RAM/8)
	// CLI flag > 配置文件 > 自动推导 > 硬编码默认值
	if memBytes := detectTotalMemory(); memBytes > 0 {
		if !isSet("client-output-buffer-limit") {
			autoVal := memBytes / 256
			if autoVal > 32<<20 {
				autoVal = 32 << 20
			}
			if autoVal < 1<<20 { // 不低于 1MB
				autoVal = 1 << 20
			}
			*clientOutputBufferLimit = autoVal
			logger.Logger.Debug().Int64("bytes", autoVal).Msg("client-output-buffer-limit 自动计算完成")
		}

		if !isSet("max-input-bytes") {
			autoVal := memBytes / 8
			if autoVal > 1<<30 {
				autoVal = 1 << 30
			}
			if autoVal < 128<<20 { // 不低于 128MB
				autoVal = 128 << 20
			}
			*maxInputBytesFlag = autoVal
			logger.Logger.Debug().Int64("bytes", autoVal).Msg("max-input-bytes 自动计算完成")
		}
	}

	// 启动 banner：打印检测到的硬件信息和生效配置摘要
	{
		cpuCores := runtime.NumCPU()
		memBytes := detectTotalMemory()

		gomemlimit := os.Getenv("GOMEMLIMIT")
		if gomemlimit == "" {
			if l := debug.SetMemoryLimit(-1); l > 0 && l < 1<<50 {
				gomemlimit = formatBytes(l)
			} else {
				gomemlimit = "unlimited"
			}
		}

		fmt.Fprintf(os.Stderr, "\n=== BoltDB Configuration ===\n")
		fmt.Fprintf(os.Stderr, "Detected: CPU %d cores", cpuCores)
		if memBytes > 0 {
			fmt.Fprintf(os.Stderr, " / RAM %s", formatBytes(memBytes))
		}
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "Active config:\n")
		fmt.Fprintf(os.Stderr, "  GOMEMLIMIT=%s  max-input-bytes=%s\n", gomemlimit, formatBytes(*maxInputBytesFlag))
		fmt.Fprintf(os.Stderr, "  client-output-buffer-limit=%s  max-clients=%d\n", formatBytes(*clientOutputBufferLimit), *maxClientsFlag)
		fmt.Fprintf(os.Stderr, "  proto-max-bulk-len=%s\n", formatBytes(*protoMaxBulkLenFlag))
		fmt.Fprintf(os.Stderr, "============================\n\n")
	}

	db, err := store.NewBotreonStore(*dbPathFlag)
	if err != nil {
		logger.Logger.Fatal().Err(err).Msg("Failed to create store")
	}
	defer func() {
		if err := db.CloseWithTimeout(store.CloseTimeout); err != nil {
			logger.Logger.Error().Err(err).Msg("failed to close database")
		}
	}()

	// 启动时恢复数据状态
	if !*skipStartupCleanup {
		if err := db.NextStartup(); err != nil {
			logger.Logger.Error().Err(err).Msg("Failed to run nextStartup")
		}
	} else {
		logger.Logger.Info().Msg("Skipping startup cleanup")
	}

	// Optional query budget for pathological O(n) scans (default unlimited).
	if *queryBudgetMaxScanFlag > 0 {
		db.SetQueryBudgetConfig(store.QueryBudgetConfig{
			MaxScanIterations: *queryBudgetMaxScanFlag,
		})
		logger.Logger.Info().
			Int64("max_scan_iterations", *queryBudgetMaxScanFlag).
			Msg("Query budget enabled")
	}

	// 初始化复制管理器
	replMgr := replication.NewReplicationManager(db)

	// 如果指定了 -repl-backlog-size 参数，设置积压缓冲区大小
	if *replBacklogSizeFlag != "" {
		size, err := replication.ParseBacklogSize(*replBacklogSizeFlag)
		if err != nil {
			logger.Logger.Fatal().Err(err).Str("size", *replBacklogSizeFlag).Msg("Invalid backlog size")
		}
		replMgr.SetBacklogSize(size)
		logger.Logger.Info().Int64("size", size).Msg("Replication backlog size set")
	}

	// 创建并启用 backlog WAL（文件持久化），提供崩溃恢复能力
	// WAL 以 append-only 格式记录每条写命令，主节点崩溃重启后可从 WAL 重建
	// 内存 backlog，避免所有从节点被迫 FULLRESYNC
	walDir := filepath.Join(*dbPathFlag, replication.WALDirName)
	wal, walErr := replication.NewBacklogWAL(walDir)
	if walErr != nil {
		logger.Logger.Warn().Err(walErr).Str("dir", walDir).Msg("Failed to create backlog WAL, continuing without persistence")
	} else {
		replMgr.SetBacklogWAL(wal)
		logger.Logger.Info().Str("dir", walDir).Msg("Backlog WAL enabled for crash recovery")
	}

	// TLS 配置（提前创建，供 replication 和 listener 使用）
	tlsCfg := &server.TLSConfig{
		CertFile: *tlsCertFlag,
		KeyFile:  *tlsKeyFlag,
		CAFile:   *tlsCAFlag,
		Require:  *tlsRequireFlag,
	}
	if tlsCfg.IsEnabled() {
		logger.Logger.Info().Str("cert", *tlsCertFlag).Msg("TLS enabled")
		if tlsCfg.CAFile != "" {
			logger.Logger.Info().Str("ca", *tlsCAFlag).Msg("Client certificate verification enabled")
		}
	}

	// 设置复制管理器的 TLS 配置
	if tlsCfg.IsEnabled() {
		tlsGoCfg, tlsErr := tlsCfg.BuildTLSConfig()
		if tlsErr != nil {
			logger.Logger.Fatal().Err(tlsErr).Msg("Failed to build TLS config for replication")
		}
		replMgr.SetTLSConfig(tlsGoCfg)
	}

	// 如果指定了 -replicaof 参数，启动从复制
	if *replicaofFlag != "" {
		logger.Logger.Info().Str("master", *replicaofFlag).Msg("Starting slave replication")
		if err := replication.StartSlaveReplication(replMgr, db, *replicaofFlag); err != nil {
			logger.Logger.Fatal().Err(err).Str("master", *replicaofFlag).Msg("Failed to start slave replication")
		}
	}

	// 初始化备份管理器
	backupDir := *dbPathFlag + "/backup"
	backupMgr := backup.NewBackupManager(db, backupDir)

	// 初始化Pub/Sub管理器
	pubsubMgr := store.NewPubSubManager()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	handler := &server.Handler{
		Db:                db,
		Replication:       replMgr,
		Backup:            backupMgr,
		PubSub:            pubsubMgr,
		Ctx:               ctx,
		OutputBufferLimit: *clientOutputBufferLimit,
		MaxClients:        *maxClientsFlag,
		MaxInputBytes:     *maxInputBytesFlag,
		Timeout:           time.Duration(*idleTimeoutFlag) * time.Second,
	}
	handler.SetAuthPassword(os.Getenv("BOLTDB_PASSWORD"))
	// SHUTDOWN 命令 → cancel() → ctx.Done() → ServeTCP 返回 → 走完整关闭序列
	handler.OnShutdown = cancel
	// CLUSTER CALLS 真实统计源：命令数 / 输入字节 / 输出字节
	cluster.SetCallsStatsProvider(func() (int64, int64, int64) {
		return handler.TotalCommandsProcessed(), handler.TotalInputBytes(), handler.TotalOutputBytes()
	})

	// 初始化 metrics 采集
	metrics.BuildVersion = server.Version
	collector := metrics.NewCollector()
	collector.QueryBudgetTripsFn = db.GetQueryBudgetTrips
	collector.RetryMetricsFn = func() (int64, int64, int64, int64, int64, float64) {
		m := db.GetRetryMetrics()
		return m.ActiveRetries, m.TotalRetries, m.WritesBlocked, m.L0Rejected, m.L0Delayed, m.LastL0Score
	}
	collector.MasterReplOffsetFn = replMgr.GetMasterReplOffset
	collector.SlaveReplOffsetFn = replMgr.GetSlaveReplOffset
	collector.ReconnectCountFn = replMgr.GetReconnectCount
	collector.SlaveCountFn = replMgr.GetSlaveCount
	collector.BacklogSizeFn = func() int64 { return replMgr.GetBacklog().GetSize() }
	collector.BacklogAvailFn = func() int64 { return replMgr.GetBacklog().GetAvailableLength() }
	collector.RoleFn = replMgr.GetRole
	collector.ActiveClientsFn = handler.ActiveClientCount
	collector.BlockedClientsFn = handler.BlockedClientCount
	collector.MonitorClientsFn = handler.MonitorClientCount
	collector.PubSubClientsFn = handler.PubSubClientCount
	collector.TotalOutputBytesFn = handler.TotalOutputBytes
	collector.PubSubSubsFn = pubsubMgr.GetTotalSubscriberCount

	if *metricsAddrFlag != "" {
		go func() {
			if err := metrics.ServeMetrics(ctx, *metricsAddrFlag, collector); err != nil {
				logger.Logger.Warn().Err(err).Msg("metrics HTTP server exited")
			}
		}()
		logger.Logger.Info().Str("addr", *metricsAddrFlag).Msg("metrics endpoint enabled")
	}
	var metricsWg sync.WaitGroup
	metrics.StartPeriodicSnapshot(ctx, collector, 60*time.Second, &metricsWg)

	// 初始化集群（如果启用了集群模式）
	if *clusterEnabledFlag {
		c, err := cluster.NewCluster(db, "", *addrFlag, ctx)
		if err != nil {
			logger.Logger.Fatal().Err(err).Msg("Failed to create cluster")
		}
		handler.Cluster = c
		// 替换为服务器生命周期 context，使 gossip 随 shutdown 自动停止
		c.Gossip = cluster.NewGossiper(ctx, c)
		// 可配置的 PING 间隔：空闲集群可用 -gossip-interval 调大以降 CPU
		c.Gossip.SetPingInterval(*gossipIntervalFlag)
		// 启动 gossip 循环
		c.Gossip.Start()
		// 使用服务器生命周期 context 替代 context.Background()
		c.Bus.SetContext(ctx)
		// 传递 TLS 配置给集群总线
		if tlsCfg.IsEnabled() {
			tlsGoCfg, tlsErr := tlsCfg.BuildTLSConfig()
			if tlsErr == nil {
				c.Bus.SetTLSConfig(tlsGoCfg)
			}
		}
		logger.Logger.Info().Msg("Cluster mode enabled")
	}
	ln, err := net.Listen("tcp", *addrFlag)
	if err != nil {
		logger.Logger.Fatal().Err(err).Str("addr", *addrFlag).Msg("Failed to listen")
	}

	// TLS 包装 listener（使用提前创建的 tlsCfg）
	if tlsCfg.IsEnabled() {
		tlsGoCfg, tlsErr := tlsCfg.BuildTLSConfig()
		if tlsErr != nil {
			logger.Logger.Fatal().Err(tlsErr).Msg("Failed to build TLS config")
		}
		ln = server.WrapListener(ln, tlsGoCfg)
	}
	// 获取实际监听端口
	tcpAddr := ln.Addr().(*net.TCPAddr)
	handler.Port = tcpAddr.Port

	// 启动集群总线（必须在数据端口确定之后）
	if handler.Cluster != nil {
		host, _, _ := net.SplitHostPort(*addrFlag)
		if host == "" {
			host = "0.0.0.0"
		}
		if err := handler.Cluster.Bus.Start(host, tcpAddr.Port); err != nil {
			logger.Logger.Fatal().Err(err).Msg("Failed to start cluster bus")
		}
	}

	logger.Warning("BoltDB 服务器启动，监听地址: %s", *addrFlag)
	logger.Warning("当前日志级别: %s", logger.GetLevelString())

	go func() {
		<-ctx.Done()
		logger.Logger.Info().Msg("收到关闭信号，正在关闭服务器...")
		_ = ln.Close()
	}()

	if err := handler.ServeTCP(ln); err != nil {
		logger.Logger.Info().Msg("服务器已停止")
	}

	logger.Logger.Info().Msg("开始执行关闭序列...")
	replMgr.Stop()
	if handler.Cluster != nil {
		handler.Cluster.Gossip.Stop()
		handler.Cluster.Bus.Stop()
	}
	cancel()
	metricsWg.Wait()
	handler.Shutdown()
	backupMgr.Wait()
	logger.Logger.Info().Msg("关闭序列完成")
}

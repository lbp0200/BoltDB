package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/metrics"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"
	"github.com/lbp0200/BoltDB/internal/store"
)

// Command-line flags - defined at package level for testability
var (
	addrFlag                = flag.String("addr", ":6337", "listen addr")
	dbPathFlag              = flag.String("dir", os.TempDir(), "badger dir")
	logLevelFlag            = flag.String("log-level", "", "log level: DEBUG, INFO, WARNING, ERROR (default: WARNING, or from BOLTDB_LOG_LEVEL env)")
	clusterEnabledFlag      = flag.Bool("cluster", false, "enable cluster mode")
	replicaofFlag           = flag.String("replicaof", "", "replicaof master host:port")
	skipStartupCleanup      = flag.Bool("skip-startup-cleanup", false, "skip startup cleanup (data integrity check)")
	clientOutputBufferLimit = flag.Int64("client-output-buffer-limit", 0, "per-client output buffer hard limit in bytes (0 = unlimited)")
	metricsAddrFlag         = flag.String("metrics-addr", "", "metrics HTTP listen addr (e.g. :6338, empty = disabled)")
)

func main() {
	flag.Parse()

	// 设置日志级别
	if *logLevelFlag != "" {
		logger.SetLevelFromString(*logLevelFlag)
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

	// 初始化复制管理器
	replMgr := replication.NewReplicationManager(db)

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
	}

	// 初始化 metrics 采集
	collector := metrics.NewCollector()
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
		c, err := cluster.NewCluster(db, "", *addrFlag)
		if err != nil {
			logger.Logger.Fatal().Err(err).Msg("Failed to create cluster")
		}
		handler.Cluster = c
		// 替换为服务器生命周期 context，使 gossip 随 shutdown 自动停止
		c.Gossip = cluster.NewGossiper(ctx, c)
		// 启动 gossip 循环
		c.Gossip.Start()
		logger.Logger.Info().Msg("Cluster mode enabled")
	}
	ln, err := net.Listen("tcp", *addrFlag)
	if err != nil {
		logger.Logger.Fatal().Err(err).Str("addr", *addrFlag).Msg("Failed to listen")
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

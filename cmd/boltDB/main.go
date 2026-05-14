package main

import (
	"flag"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"

	"github.com/lbp0200/BoltDB/internal/store"
)

// Command-line flags - defined at package level for testability
var (
	addrFlag           = flag.String("addr", ":6337", "listen addr")
	dbPathFlag         = flag.String("dir", os.TempDir(), "badger dir")
	logLevelFlag       = flag.String("log-level", "", "log level: DEBUG, INFO, WARNING, ERROR (default: WARNING, or from BOLTDB_LOG_LEVEL env)")
	clusterEnabledFlag = flag.Bool("cluster", false, "enable cluster mode")
	replicaofFlag      = flag.String("replicaof", "", "replicaof master host:port")
	skipStartupCleanup = flag.Bool("skip-startup-cleanup", false, "skip startup cleanup (data integrity check)")
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

	handler := &server.Handler{
		Db:          db,
		Replication: replMgr,
		Backup:      backupMgr,
		PubSub:      pubsubMgr,
	}

	// 初始化集群（如果启用了集群模式）
	if *clusterEnabledFlag {
		c, err := cluster.NewCluster(db, "", *addrFlag)
		if err != nil {
			logger.Logger.Fatal().Err(err).Msg("Failed to create cluster")
		}
		handler.Cluster = c
		logger.Logger.Info().Msg("Cluster mode enabled")
	}
	ln, err := net.Listen("tcp", *addrFlag)
	if err != nil {
		logger.Logger.Fatal().Err(err).Str("addr", *addrFlag).Msg("Failed to listen")
	}
	// 获取实际监听端口
	tcpAddr := ln.Addr().(*net.TCPAddr)
	handler.Port = tcpAddr.Port

	logger.Warning("BoltDB 服务器启动，监听地址: %s", *addrFlag)
	logger.Warning("当前日志级别: %s", logger.GetLevelString())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Logger.Info().Str("signal", sig.String()).Msg("收到关闭信号，正在关闭服务器...")
		_ = ln.Close()
	}()

	if err := handler.ServeTCP(ln); err != nil {
		logger.Logger.Info().Msg("服务器已停止")
	}

	replMgr.Stop()
}

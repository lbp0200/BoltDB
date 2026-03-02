package main

import (
	"flag"
	"net"
	"os"

	"github.com/lbp0200/BoltDB/internal/backup"
	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/replication"
	"github.com/lbp0200/BoltDB/internal/server"

	"github.com/lbp0200/BoltDB/internal/store"
)

func main() {
	addr := flag.String("addr", ":6337", "listen addr")
	dbPath := flag.String("dir", os.TempDir(), "badger dir")
	logLevel := flag.String("log-level", "", "log level: DEBUG, INFO, WARNING, ERROR (default: WARNING, or from BOLTDB_LOG_LEVEL env)")
	clusterEnabled := flag.Bool("cluster", false, "enable cluster mode")
	replicaof := flag.String("replicaof", "", "replicaof master host:port")
	flag.Parse()

	// 设置日志级别
	if *logLevel != "" {
		logger.SetLevelFromString(*logLevel)
	}

	db, err := store.NewBotreonStore(*dbPath)
	if err != nil {
		logger.Logger.Fatal().Err(err).Msg("Failed to create store")
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.Logger.Error().Err(err).Msg("failed to close database")
		}
	}()

	// 启动时恢复数据状态
	if err := db.NextStartup(); err != nil {
		logger.Logger.Error().Err(err).Msg("Failed to run nextStartup")
	}

	// 初始化复制管理器
	replMgr := replication.NewReplicationManager(db)

	// 如果指定了 -replicaof 参数，启动从复制
	if *replicaof != "" {
		logger.Logger.Info().Str("master", *replicaof).Msg("Starting slave replication")
		if err := replication.StartSlaveReplication(replMgr, db, *replicaof); err != nil {
			logger.Logger.Fatal().Err(err).Str("master", *replicaof).Msg("Failed to start slave replication")
		}
	}

	// 初始化备份管理器
	backupDir := *dbPath + "/backup"
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
	if *clusterEnabled {
		c, err := cluster.NewCluster(db, "", *addr)
		if err != nil {
			logger.Logger.Fatal().Err(err).Msg("Failed to create cluster")
		}
		handler.Cluster = c
		logger.Logger.Info().Msg("Cluster mode enabled")
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Logger.Fatal().Err(err).Str("addr", *addr).Msg("Failed to listen")
	}
	// 获取实际监听端口
	tcpAddr := ln.Addr().(*net.TCPAddr)
	handler.Port = tcpAddr.Port

	// 启动信息使用 WARN 级别，确保默认配置下也能显示
	logger.Warning("BoltDB 服务器启动，监听地址: %s", *addr)
	logger.Warning("当前日志级别: %s", logger.GetLevelString())
	if err := handler.ServeTCP(ln); err != nil {
		logger.Logger.Fatal().Err(err).Msg("Server failed")
	}
}

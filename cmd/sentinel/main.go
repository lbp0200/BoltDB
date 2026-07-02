package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/sentinel"
	"github.com/lbp0200/BoltDB/internal/server"
)

var (
	addrFlag       = flag.String("addr", ":26379", "sentinel listen addr")
	configFlag     = flag.String("config", "", "sentinel config file (redis sentinel.conf format)")
	gossipPortFlag = flag.Int("gossip-port", 0, "sentinel gossip port (0 = random)")
	passwordFlag   = flag.String("password", "", "AUTH password for connecting to monitored masters/slaves (or BOLTDB_PASSWORD env)")
	logLevelFlag   = flag.String("log-level", "", "log level: DEBUG, INFO, WARNING, ERROR (default: WARNING, or BOLTDB_LOG_LEVEL env)")
	tlsCertFlag    = flag.String("tls-cert", "", "path to TLS certificate PEM file (empty = no TLS)")
	tlsKeyFlag     = flag.String("tls-key", "", "path to TLS private key PEM file (empty = no TLS)")
	tlsCAFlag      = flag.String("tls-ca", "", "path to CA certificate PEM file for server verification (optional)")
)

func main() {
	flag.Parse()

	if *passwordFlag != "" {
		sentinel.SetSentinelPassword(*passwordFlag)
	}

	// Initialize TLS for outbound Sentinel connections (to monitored BoltDB servers)
	if *tlsCertFlag != "" && *tlsKeyFlag != "" {
		tlsCfg := &server.TLSConfig{
			CertFile: *tlsCertFlag,
			KeyFile:  *tlsKeyFlag,
			CAFile:   *tlsCAFlag,
		}
		tlsGoCfg, err := tlsCfg.BuildTLSConfig()
		if err != nil {
			logger.Logger.Fatal().Err(err).Msg("Failed to build TLS config for sentinel")
		}
		// For client-side (outbound) connections, use the server cert as client cert
		// and CA cert for server verification
		tlsGoCfg.ServerName = "bolt-server" // default, override via tls-ca
		sentinel.SetSentinelTLSConfig(tlsGoCfg)
		logger.Logger.Info().Msg("Sentinel TLS enabled for outbound connections")
	}

	if *logLevelFlag != "" {
		logger.SetLevelFromString(*logLevelFlag)
	}

	s := sentinel.NewSentinel(2, 30*time.Second)

	if *configFlag != "" {
		if err := parseConfigFile(s, *configFlag); err != nil {
			logger.Logger.Fatal().Err(err).Str("file", *configFlag).Msg("failed to parse config")
		}
	}

	// 启动 gossip 协议
	gossipCfg := sentinel.DefaultGossipConfig()
	gossipCfg.Port = *gossipPortFlag
	gp := sentinel.NewGossipProtocol(s, gossipCfg)
	s.Gossip = gp
	if err := gp.Start(); err != nil {
		logger.Logger.Fatal().Err(err).Msg("failed to start gossip protocol")
	}

	// 从配置中的 known-sentinel 条目添加对等体
	for _, peerAddr := range s.GetOtherSentinels() {
		if err := gp.AddPeer(peerAddr, s.GetRunID()); err != nil {
			logger.Logger.Warn().Str("peer", peerAddr).Err(err).Msg("failed to add gossip peer")
		}
	}

	s.Start()

	ln, err := net.Listen("tcp", *addrFlag)
	if err != nil {
		logger.Logger.Fatal().Err(err).Str("addr", *addrFlag).Msg("failed to listen")
	}
	logger.Logger.Info().Str("addr", *addrFlag).Int("gossip-port", gp.GetPort()).Msg("sentinel started")

	handler := sentinel.NewSentinelHandler(s)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Logger.Info().Msg("shutting down sentinel")
		_ = ln.Close()
		handler.Stop()
		gp.Stop()
		s.Stop()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			break
		}
		go handler.HandleConnection(conn)
	}
}

func parseConfigFile(s *sentinel.Sentinel, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if err := parseConfigLine(s, line); err != nil {
			logger.Logger.Warn().Str("line", line).Err(err).Msg("skip config line")
		}
	}
	return scanner.Err()
}

func parseConfigLine(s *sentinel.Sentinel, line string) error {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return nil
	}

	switch strings.ToLower(fields[0]) {
	case "sentinel":
		return parseSentinelDirective(s, fields[1:])
	}
	return nil
}

func parseSentinelDirective(s *sentinel.Sentinel, args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("invalid sentinel directive: %v", args)
	}

	sub := strings.ToLower(args[0])
	masterName := args[1]

	switch sub {
	case "monitor":
		if len(args) < 4 {
			return fmt.Errorf("sentinel monitor requires <name> <ip> <port> <quorum>")
		}
		ip := args[2]
		port := args[3]
		quorum := 2
		if len(args) >= 5 {
			if _, err := fmt.Sscanf(args[4], "%d", &quorum); err != nil {
				return fmt.Errorf("invalid quorum: %s", args[4])
			}
		}
		addr := fmt.Sprintf("%s:%s", ip, port)
		return s.AddMaster(masterName, addr, quorum)

	case "down-after-milliseconds":
		return nil

	case "known-sentinel":
		if len(args) < 3 {
			return nil
		}
		s.AddSentinel(args[2])
		return nil

	case "auth-pass":
		if len(args) < 3 {
			return fmt.Errorf("sentinel auth-pass requires <master-name> <password>")
		}
		masterName := args[1]
		password := args[2]
		// Find the master instance and set its password
		masters := s.GetAllMasters()
		for _, m := range masters {
			if m.GetName() == masterName {
				m.SetAuthPass(password)
				logger.Logger.Info().
					Str("master_name", masterName).
					Str("password_length", fmt.Sprintf("%d", len(password))).
					Msg("set AUTH password for master")
				return nil
			}
		}
		return fmt.Errorf("master %s not found for auth-pass", masterName)
	}
	return nil
}

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
)

var (
	addrFlag     = flag.String("addr", ":26379", "sentinel listen addr")
	configFlag   = flag.String("config", "", "sentinel config file (redis sentinel.conf format)")
	logLevelFlag = flag.String("log-level", "", "log level: DEBUG, INFO, WARNING, ERROR")
)

func main() {
	flag.Parse()

	if *logLevelFlag != "" {
		logger.SetLevelFromString(*logLevelFlag)
	}

	s := sentinel.NewSentinel(2, 30*time.Second)

	if *configFlag != "" {
		if err := parseConfigFile(s, *configFlag); err != nil {
			logger.Logger.Fatal().Err(err).Str("file", *configFlag).Msg("failed to parse config")
		}
	}

	s.Start()

	ln, err := net.Listen("tcp", *addrFlag)
	if err != nil {
		logger.Logger.Fatal().Err(err).Str("addr", *addrFlag).Msg("failed to listen")
	}
	logger.Logger.Info().Str("addr", *addrFlag).Msg("sentinel started")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Logger.Info().Msg("shutting down sentinel")
		_ = ln.Close()
		s.Stop()
	}()

	handler := sentinel.NewSentinelHandler(s)
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
	}
	return nil
}

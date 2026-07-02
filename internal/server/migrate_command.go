package server

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
)

// handleMIGRATE 实现 MIGRATE 命令
func (h *Handler) handleMIGRATE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	// MIGRATE host port key|"" db timeout [COPY] [REPLACE] [KEYS key ...]
	if len(args) < 4 {
		return proto.NewError("ERR wrong number of arguments for 'MIGRATE' command")
	}
	host := string(args[0])
	port := string(args[1])
	timeoutStr := string(args[4])
	timeoutMS, err := strconv.Atoi(timeoutStr)
	if err != nil || timeoutMS < 0 {
		return proto.NewError("ERR timeout is not an integer or out of range")
	}
	timeout := time.Duration(timeoutMS) * time.Millisecond
	if timeout < time.Second {
		timeout = time.Second
	}

	// Parse options
	copyKey := false
	replace := false
	var keysToMigrate []string

	// key is at args[2], can be "" meaning "use KEYS option"
	keyArg := string(args[2])
	if keyArg != "" {
		keysToMigrate = append(keysToMigrate, keyArg)
	}

	for i := 5; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "COPY":
			copyKey = true
		case "REPLACE":
			replace = true
		case "KEYS":
			for j := i + 1; j < len(args); j++ {
				keysToMigrate = append(keysToMigrate, string(args[j]))
			}
			i = len(args)
		}
	}

	if len(keysToMigrate) == 0 {
		return proto.NewError("ERR no keys to migrate")
	}

	targetAddr := net.JoinHostPort(host, port)

	// Connect once and send all RESTORE commands over the same connection
	conn, err := net.DialTimeout("tcp", targetAddr, timeout)
	if err != nil {
		return proto.NewError(fmt.Sprintf("ERR MIGRATE: connecting to %s: %v", targetAddr, err))
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	reader := bufio.NewReader(conn)
	var migratedKeys []string

	for _, migrateKey := range keysToMigrate {
		data, err := h.Db.Dump(migrateKey)
		if err != nil {
			if strings.Contains(err.Error(), "no such key") {
				continue
			}
			_ = conn.Close()
			return proto.NewError(fmt.Sprintf("ERR MIGRATE: dump key %s: %v", migrateKey, err))
		}

		// Build RESTORE command using raw RESP format
		var restoreData []byte
		if replace {
			restoreData = []byte(fmt.Sprintf("*5\r\n$7\r\nRESTORE\r\n$%d\r\n%s\r\n$1\r\n0\r\n$%d\r\n",
				len(migrateKey), migrateKey, len(data)))
		} else {
			restoreData = []byte(fmt.Sprintf("*4\r\n$7\r\nRESTORE\r\n$%d\r\n%s\r\n$1\r\n0\r\n$%d\r\n",
				len(migrateKey), migrateKey, len(data)))
		}
		restoreData = append(restoreData, data...)
		restoreData = append(restoreData, "\r\n"...)
		if replace {
			restoreData = append(restoreData, "$7\r\nREPLACE\r\n"...)
		}

		if _, err := conn.Write(restoreData); err != nil {
			_ = conn.Close()
			return proto.NewError(fmt.Sprintf("ERR MIGRATE: write to %s: %v", targetAddr, err))
		}

		resp, err := proto.ReadRESP(reader)
		if err != nil {
			_ = conn.Close()
			return proto.NewError(fmt.Sprintf("ERR MIGRATE: target response for key %s: %v", migrateKey, err))
		}

		targetErr := targetRespError(resp)
		if targetErr != "" {
			_ = conn.Close()
			return proto.NewError(fmt.Sprintf("ERR MIGRATE: target error for key %s: %s", migrateKey, targetErr))
		}

		migratedKeys = append(migratedKeys, migrateKey)
	}

	// Only delete local keys after all RESTOREs succeeded
	if !copyKey {
		for _, migrateKey := range migratedKeys {
			h.markDirtyKeys(state, migrateKey)
			if _, delErr := h.Db.Del(migrateKey); delErr != nil {
				logger.Logger.Warn().Err(delErr).Str("key", migrateKey).
					Msg("MIGRATE: failed to delete local key after successful migration")
			}
		}
	}
	return proto.OK
}

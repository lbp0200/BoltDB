package server

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

func getResponseType(resp proto.RESP) string {
	switch resp.(type) {
	case *proto.SimpleString:
		return "SimpleString"
	case *proto.BulkString:
		return "BulkString"
	case proto.Error:
		return "Error"
	case proto.Integer:
		return "Integer"
	case *proto.Array:
		return "Array"
	case *proto.Map:
		return "Map"
	case *proto.Set:
		return "Set"
	case *proto.Push:
		return "Push"
	case *proto.Null:
		return "Null"
	case *proto.Double:
		return "Double"
	case *proto.Boolean:
		return "Boolean"
	case *proto.BigNumber:
		return "BigNumber"
	case *proto.VerbatimString:
		return "VerbatimString"
	default:
		return "Unknown"
	}
}

// PubSubQuitSignal signals that the pubsub loop should close after sending OK
type PubSubQuitSignal struct{}

func (PubSubQuitSignal) String() string { return "+OK\r\n" }

// MultiResponse carries multiple RESP responses for a single command (e.g. SUBSCRIBE ch1 ch2)
type MultiResponse struct {
	Responses []proto.RESP
}

func (m *MultiResponse) String() string {
	if len(m.Responses) == 0 {
		return ""
	}
	return m.Responses[0].String()
}

// clientListRESP 返回 CLIENT LIST 的 RESP 响应
func (h *Handler) clientListRESP() proto.RESP {
	h.connsMu.RLock()
	defer h.connsMu.RUnlock()

	if len(h.conns) == 0 {
		return proto.NewBulkString([]byte("id=1 addr=127.0.0.1:0 fd=0 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 cmd=client events=r oFlags= keys=0"))
	}

	var lines []string
	now := time.Now()
	for state, meta := range h.conns {
		id := meta.id
		addr := meta.remoteAddr
		name := ""
		age := int(now.Sub(meta.created).Seconds())
		idle := 0

		state.mu.Lock()
		flags := "N"
		sub := 0
		psub := 0
		multi := -1
		if state.subscriber != nil {
			flags = "P"
			sub = len(state.subscriber.Channels)
			psub = len(state.subscriber.Patterns)
		} else if state.inTransaction {
			flags = "t"
		} else if state.blocking.Load() {
			flags = "b"
		}
		if state.inTransaction {
			multi = len(state.commands)
		}
		state.mu.Unlock()

		oFlags := ""
		if h.OutputBufferLimit > 0 && meta.outputBytes > h.OutputBufferLimit/2 {
			oFlags = ">"
		}
		line := fmt.Sprintf("id=%d addr=%s fd=0 name=%s age=%d idle=%d flags=%s db=0 sub=%d psub=%d multi=%d cmd=client events=r obl=0 oll=0 omem=%d oFlags=%s keys=0",
			id, addr, name, age, idle, flags, sub, psub, multi, meta.outputBytes, oFlags)
		lines = append(lines, line)
	}
	return proto.NewBulkString([]byte(strings.Join(lines, "\n")))
}

func wrapStoreError(err error) proto.RESP {
	if errors.Is(err, store.ErrWrongType) {
		return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return proto.NewError(fmt.Sprintf("ERR %v", err))
}

// targetRespError extracts a human-readable error string from a RESP response,
// or returns "" if the response indicates success (SimpleString "+OK").
func targetRespError(resp *proto.Array) string {
	if resp == nil || len(resp.Args) == 0 {
		return ""
	}
	msg := resp.Args[0]
	if msg == nil {
		return ""
	}
	firstWord := string(msg)
	if idx := strings.IndexByte(firstWord, ' '); idx > 0 {
		firstWord = firstWord[:idx]
	}
	if firstWord == "OK" {
		return ""
	}
	return string(msg)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// parseScoreExclusive checks if a score string represents an exclusive bound
func parseScoreExclusive(s string) (float64, bool, error) {
	exclusive := false
	if len(s) > 0 && s[0] == '(' {
		exclusive = true
		s = s[1:]
	} else if len(s) > 0 && s[0] == '[' {
		exclusive = false
		s = s[1:]
	}

	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// 检查特殊值
		switch s {
		case "-inf":
			return float64(math.Inf(-1)), exclusive, nil
		case "+inf", "inf":
			return float64(math.Inf(1)), exclusive, nil
		}
		return 0, false, err
	}
	return val, exclusive, nil
}

// markDirtyKeys 标记键为脏（被修改）
// 通过共享 watchMonitors 通知所有正在监视该键的连接
func (h *Handler) markDirtyKeys(state *connState, keys ...string) {
	h.watchMu.Lock()
	for _, key := range keys {
		if watchers, exists := h.watchMonitors[key]; exists {
			for watcher := range watchers {
				watcher.mu.Lock()
				if watcher.dirtyKeys == nil {
					watcher.dirtyKeys = make(map[string]struct{})
				}
				watcher.dirtyKeys[key] = struct{}{}
				watcher.mu.Unlock()
			}
		}
	}
	h.watchMu.Unlock()
}

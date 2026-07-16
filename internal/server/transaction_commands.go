package server

import (
	"github.com/lbp0200/BoltDB/internal/proto"
)

// handleMULTI 实现 MULTI 命令
func (h *Handler) handleMULTI(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if state.inTransaction {
		return proto.NewError("ERR MULTI calls can not be nested")
	}
	state.inTransaction = true
	state.commands = make([]TransactionCommand, 0)
	if state.transaction == nil {
		state.transaction = &TransactionState{
			Commands:  make([]TransactionCommand, 0),
			WatchKeys: make(map[string]struct{}),
			DirtyKeys: make(map[string]struct{}),
		}
	}
	state.transaction.InTransaction = true
	state.transaction.Commands = make([]TransactionCommand, 0)
	return proto.NewSimpleString("OK")
}

// handleEXEC 实现 EXEC 命令
func (h *Handler) handleEXEC(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if !state.inTransaction {
		return proto.NewError("ERR EXEC without MULTI")
	}
	if len(state.watchedKeys) > 0 {
		h.watchMu.Lock()
		for watchKey := range state.watchedKeys {
			if _, dirty := state.dirtyKeys[watchKey]; dirty {
				h.watchMu.Unlock()
				state.inTransaction = false
				state.commands = nil
				return proto.NilArray{}
			}
		}
		h.watchMu.Unlock()
	}
	results := make([]proto.RESP, len(state.commands))
	for i, tc := range state.commands {
		results[i] = h.executeQueuedCommand(tc.Command, tc.Args, state.respVersion)
	}

	// 传播事务中的写命令到从节点。
	// SPOP 必须规范化为 SREM（与非事务 handleSPOP 一致），否则 slave 独立随机 pop。
	// 错误回复与未实现复制的写命令不进入 backlog。
	if h.Replication != nil && h.Replication.IsMaster() {
		for i, tc := range state.commands {
			if !isWriteCommand(tc.Command) || isErrorResponse(results[i]) {
				continue
			}
			propCmd := tc.Command
			propArgs := tc.Args
			if tc.Command == "SPOP" {
				sremArgs := spopResultToSREM(tc.Args, results[i])
				if sremArgs == nil {
					continue // empty pop — nothing to replicate
				}
				propCmd = "SREM"
				propArgs = sremArgs
			} else if !shouldPropagateCommand(tc.Command) {
				continue
			}
			fullArgs := make([][]byte, 1, len(propArgs)+1)
			fullArgs[0] = []byte(propCmd)
			fullArgs = append(fullArgs, propArgs...)
			h.Replication.PropagateCommand(fullArgs)
		}
	}

	state.inTransaction = false
	state.commands = nil
	return &proto.NestedArray{Elems: results}
}

// spopResultToSREM builds SREM args [key, member...] from an SPOP result.
// Returns nil when the pop was empty (nothing to replicate).
func spopResultToSREM(spopArgs [][]byte, result proto.RESP) [][]byte {
	if len(spopArgs) < 1 {
		return nil
	}
	key := spopArgs[0]
	switch r := result.(type) {
	case *proto.BulkString:
		if r == nil || len(*r) == 0 {
			return nil
		}
		return [][]byte{key, []byte(*r)}
	case *proto.Array:
		if r == nil || len(r.Args) == 0 {
			return nil
		}
		out := make([][]byte, 1, 1+len(r.Args))
		out[0] = key
		out = append(out, r.Args...)
		return out
	case *proto.Null:
		return nil
	default:
		return nil
	}
}

// handleDISCARD 实现 DISCARD 命令
func (h *Handler) handleDISCARD(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if !state.inTransaction {
		return proto.NewError("ERR DISCARD without MULTI")
	}
	state.inTransaction = false
	state.commands = nil
	state.transaction = nil
	return proto.NewSimpleString("OK")
}

// handleWATCH 实现 WATCH 命令
func (h *Handler) handleWATCH(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'WATCH' command")
	}
	if state.inTransaction && len(state.commands) > 0 {
		return proto.NewError("ERR WATCH inside MULTI is not allowed")
	}
	if state.watchedKeys == nil {
		state.watchedKeys = make(map[string]struct{})
		state.dirtyKeys = make(map[string]struct{})
	}
	h.watchMu.Lock()
	if h.watchMonitors == nil {
		h.watchMonitors = make(map[string]map[*connState]struct{})
	}
	for _, arg := range args {
		key := string(arg)
		state.watchedKeys[key] = struct{}{}
		if h.watchMonitors[key] == nil {
			h.watchMonitors[key] = make(map[*connState]struct{})
		}
		h.watchMonitors[key][state] = struct{}{}
	}
	h.watchMu.Unlock()
	return proto.NewSimpleString("OK")
}

// handleUNWATCH 实现 UNWATCH 命令
func (h *Handler) handleUNWATCH(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if state.watchedKeys != nil {
		h.watchMu.Lock()
		for key := range state.watchedKeys {
			if set, exists := h.watchMonitors[key]; exists {
				delete(set, state)
				if len(set) == 0 {
					delete(h.watchMonitors, key)
				}
			}
		}
		h.watchMu.Unlock()
		state.watchedKeys = make(map[string]struct{})
		state.dirtyKeys = make(map[string]struct{})
	}
	return proto.NewSimpleString("OK")

	// ==================== Geo commands ====================
}

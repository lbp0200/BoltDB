package server

import (
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
)

// queuedCommandKeys 提取事务队列中命令涉及的 key 列表（用于 WATCH 脏标记）。
// 复用 commandRegistry 的 firstKey/lastKey/step 元数据，保证与集群路由一致。
func queuedCommandKeys(cmd string, args [][]byte) []string {
	info, ok := commandMap[cmd]
	if !ok {
		if len(args) > 0 {
			return []string{string(args[0])}
		}
		return nil
	}
	if info.firstKey == 0 {
		return nil
	}
	// commandInfo 的 firstKey/lastKey 以 1 为基数，指代 args 中的位置（不含命令名）。
	// 如 DEL 的 firstKey=1 lastKey=-1 表示 args[0:len(args)] 全是 key。
	n := len(args)
	if n == 0 {
		return nil
	}
	first := info.firstKey - 1
	if first < 0 {
		first = 0
	}
	if first >= n {
		return nil
	}
	last := info.lastKey
	if last > 0 {
		last = last - 1
		if last >= n {
			last = n - 1
		}
	} else if last < 0 {
		last = n + last // -1 => n-1, -2 => n-2（排除末尾非 key 参数如 timeout）
		if last < first {
			return nil
		}
	} else {
		// lastKey==0 的容器命令在 init 已规范为具体 key 位置，无需处理
		return nil
	}
	step := info.step
	if step <= 0 {
		step = 1
	}
	var out []string
	for i := first; i <= last; i += step {
		out = append(out, string(args[i]))
	}
	// 特殊命令的额外 destKey（如 COPY src dst、RENAME src dst、SORT src STORE dst 等
	// 在 commandRegistry 中 lastKey 可能只覆盖 src；这里按命令语义补齐）。
	switch cmd {
	case "COPY", "RENAME", "RENAMENX":
		if len(args) >= 2 {
			dst := string(args[1])
			found := false
			for _, k := range out {
				if k == dst {
					found = true
					break
				}
			}
			if !found {
				out = append(out, dst)
			}
		}
	case "SORT":
		// SORT src [BY ...] [GET ...]* [LIMIT ...] STORE dst
		for i := 0; i < len(args); i++ {
			if strings.EqualFold(string(args[i]), "STORE") && i+1 < len(args) {
				dst := string(args[i+1])
				found := false
				for _, k := range out {
					if k == dst {
						found = true
						break
					}
				}
				if !found {
					out = append(out, dst)
				}
				break
			}
		}
	}
	return out
}

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
				// RESP3: nil array must use the Null type ('_'); redis-py 8's
				// RESP3 parser turns '*-1' into an empty list, which makes
				// pipeline.execute report "Wrong number of response items"
				// instead of raising WatchError.
				if state.respVersion == 3 {
					return &proto.Null{}
				}
				return proto.NilArray{}
			}
		}
		h.watchMu.Unlock()
	}
	results := make([]proto.RESP, len(state.commands))
	for i, tc := range state.commands {
		results[i] = h.executeQueuedCommand(tc.Command, tc.Args, state.respVersion)
	}

	// 必须像非事务路径一样标记 WATCH 脏键 + 触发统计/备份计数：
	// 否则其它连接 WATCH 的乐观锁不会因本事务的写而失效（watch 正确性漏洞），
	// 且 rdbChanges/instantaneous_ops 会漏计事务内的写。
	for i, tc := range state.commands {
		isErr := isErrorResponse(results[i])
		// WATCH 脏标记：仅对成功执行的写操作生效（错误命令不污染 WATCH）。
		if !isErr {
			shouldDirty := isWriteCommand(tc.Command) && shouldPropagateCommand(tc.Command)
			if tc.Command == "SORT" {
				hasStore := false
				for _, a := range tc.Args {
					if strings.EqualFold(string(a), "STORE") {
						hasStore = true
						break
					}
				}
				if !hasStore {
					shouldDirty = false
				}
			}
			if tc.Command == "SET" {
				hasNX, hasXX := false, false
				for _, a := range tc.Args {
					switch strings.ToUpper(string(a)) {
					case "NX":
						hasNX = true
					case "XX":
						hasXX = true
					}
				}
				if hasNX || hasXX {
					if _, isNull := results[i].(*proto.Null); isNull {
						shouldDirty = false
					} else if bs, ok := results[i].(*proto.BulkString); ok && bs == nil {
						shouldDirty = false
					}
				}
			}
			if tc.Command == "SETNX" || tc.Command == "MSETNX" || tc.Command == "HSETNX" {
				if !isPositiveIntegerResp(results[i]) {
					shouldDirty = false
				}
			}
			if tc.Command == "SPOP" {
				if spopResultToSREM(tc.Args, results[i]) == nil {
					shouldDirty = false
				}
			}
			if shouldDirty {
				keys := queuedCommandKeys(tc.Command, tc.Args)
				if len(keys) > 0 {
					h.markDirtyKeys(state, keys...)
				}
			}
		}
		// 统计：事务内每条命令（无论成功/失败）均计入 OPS 与 COMMANDSTATS，
		// 与非事务路径 executeCommand 的 defer 语义一致；bumpRdbChanges
		// 仅对写命令计入（与主路径相同，错误写也会留下脏位，等待下次 BGSAVE）。
		h.recordOps()
		if isWriteCommand(tc.Command) {
			h.bumpRdbChanges()
		}
		h.incrementCmdCounter(tc.Command)
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
			// SORT 仅带 STORE 时写；不带 STORE 的 SORT 是只读，不进 backlog（R7）
			if tc.Command == "SORT" {
				hasStore := false
				for _, a := range tc.Args {
					if strings.EqualFold(string(a), "STORE") {
						hasStore = true
						break
					}
				}
				if !hasStore {
					continue
				}
			}
			// SET 条件写（NX/XX）空转返回 Null 时未写入
			if tc.Command == "SET" {
				hasNX := false
				hasXX := false
				for _, a := range tc.Args {
					switch strings.ToUpper(string(a)) {
					case "NX":
						hasNX = true
					case "XX":
						hasXX = true
					}
				}
				if hasNX || hasXX {
					if _, isNull := results[i].(*proto.Null); isNull {
						continue
					}
					if bs, ok := results[i].(*proto.BulkString); ok && bs == nil {
						continue
					}
				}
			}
			if tc.Command == "SETNX" || tc.Command == "MSETNX" || tc.Command == "HSETNX" {
				if !isPositiveIntegerResp(results[i]) {
					continue
				}
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

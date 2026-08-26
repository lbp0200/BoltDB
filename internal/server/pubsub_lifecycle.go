package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

func (h *Handler) runPubSubLoop(ctx context.Context, conn net.Conn, reader *bufio.Reader, writer *bufio.Writer, state *connState, remoteAddr string) {
	subscriber := state.subscriber
	if subscriber == nil {
		return
	}
	if ctx == nil {
		ctx = h.Ctx
	}

	logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("进入 PubSub 模式")

	cmdCh := make(chan *proto.Array, 16)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	defer close(done)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Logger.Error().
					Str("remote_addr", remoteAddr).
					Interface("panic", r).
					Str("stack", string(debug.Stack())).
					Msg("recovered panic in pubsub reader")
				select {
				case errCh <- fmt.Errorf("panic recovered: %v", r):
				case <-done:
				}
			}
		}()
		for {
			req, err := proto.ReadRESP(reader)
			if err != nil {
				select {
				case errCh <- err:
				case <-done:
				}
				return
			}
			select {
			case cmdCh <- req:
			case <-done:
				return
			}
		}
	}()

	flushTicker := time.NewTicker(100 * time.Millisecond)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("pubsub loop cancelled by context")
			return

		case msg, ok := <-subscriber.MessageCh:
			if !ok {
				return
			}
			resp := buildPubSubPush(msg, state.respVersion)
			if err := proto.WriteRESP(writer, resp); err != nil {
				return
			}

		case req := <-cmdCh:
			// PubSub 模式下的命令同样计入可观测性（与主 dispatch 路径一致）
			if len(req.Args) > 0 {
				cmd := strings.ToUpper(string(req.Args[0]))
				h.recordOps()
				h.incrementCmdCounter(cmd)
			}
			resp := h.processPubSubCommand(state, req, remoteAddr)
			switch r := resp.(type) {
			case *PubSubQuitSignal:
				_ = proto.WriteRESP(writer, proto.NewSimpleString("OK"))
				return
			case *MultiResponse:
				for _, subResp := range r.Responses {
					if err := proto.WriteRESP(writer, subResp); err != nil {
						return
					}
				}
			default:
				if err := proto.WriteRESP(writer, resp); err != nil {
					return
				}
			}

		case <-flushTicker.C:
			if n := writer.Buffered(); n > 0 {
				if err := writer.Flush(); err != nil {
					return
				}
				h.connsMu.Lock()
				m, tracked := h.conns[state]
				if tracked {
					m.outputBytes += int64(n)
				}
				h.connsMu.Unlock()
				// CLIENT NOEVICT ON：输出缓冲区超限不主动断开该连接
				if tracked && h.OutputBufferLimit > 0 && m.outputBytes > h.OutputBufferLimit && !state.noEvict.Load() {
					logger.Logger.Warn().
						Str("remote_addr", remoteAddr).
						Int64("output_bytes", m.outputBytes).
						Int64("limit", h.OutputBufferLimit).
						Msg("客户端输出缓冲区超限，断开连接")
					return
				}
			}

		case err := <-errCh:
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Err(err).Msg("pubsub read error")
			return
		}
	}
}

// buildPubSubPush constructs a RESP push message from a store.Message.
// For RESP3 (respVersion == 3), uses Push type; for RESP2, uses Array.
func buildPubSubPush(msg *store.Message, respVersion int) proto.RESP {
	var elems []proto.RESP
	if msg.Shard {
		elems = []proto.RESP{
			proto.NewBulkString([]byte("smessage")),
			proto.NewBulkString([]byte(msg.Channel)),
			proto.NewBulkString(msg.Data),
		}
	} else if msg.Pattern != "" {
		elems = []proto.RESP{
			proto.NewBulkString([]byte("pmessage")),
			proto.NewBulkString([]byte(msg.Pattern)),
			proto.NewBulkString([]byte(msg.Channel)),
			proto.NewBulkString(msg.Data),
		}
	} else {
		elems = []proto.RESP{
			proto.NewBulkString([]byte("message")),
			proto.NewBulkString([]byte(msg.Channel)),
			proto.NewBulkString(msg.Data),
		}
	}
	if respVersion == 3 {
		return &proto.Push{Elems: elems}
	}
	return &proto.NestedArray{Elems: elems}
}

// makePushOrArray wraps elements in Push (RESP3) or NestedArray (RESP2)
func makePushOrArray(elems []proto.RESP, respVersion int) proto.RESP {
	if respVersion == 3 {
		return &proto.Push{Elems: elems}
	}
	return &proto.NestedArray{Elems: elems}
}

// processPubSubCommand handles commands received while in PubSub mode.
// Only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed.
func (h *Handler) processPubSubCommand(state *connState, req *proto.Array, remoteAddr string) proto.RESP {
	args := req.Args
	if len(args) == 0 {
		return proto.NewError("ERR no command")
	}
	cmd := strings.ToUpper(string(args[0]))

	switch cmd {
	case "SUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SUBSCRIBE' command")
		}
		channels := make([]string, len(args)-1)
		for i, arg := range args[1:] {
			channels[i] = string(arg)
		}
		subscribed := h.PubSub.Subscribe(state.subscriber, channels...)
		resp := &MultiResponse{
			Responses: make([]proto.RESP, len(subscribed)),
		}
		for i, ch := range subscribed {
			resp.Responses[i] = makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("subscribe")),
				proto.NewBulkString([]byte(ch)),
				proto.NewInteger(int64(i + 1)),
			}, state.respVersion)
		}
		return resp

	case "PSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'PSUBSCRIBE' command")
		}
		patterns := make([]string, len(args)-1)
		for i, arg := range args[1:] {
			patterns[i] = string(arg)
		}
		subscribed := h.PubSub.PSubscribe(state.subscriber, patterns...)
		resp := &MultiResponse{
			Responses: make([]proto.RESP, len(subscribed)),
		}
		for i, p := range subscribed {
			resp.Responses[i] = makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("psubscribe")),
				proto.NewBulkString([]byte(p)),
				proto.NewInteger(int64(i + 1)),
			}, state.respVersion)
		}
		return resp

	case "UNSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		var unsubscribed []string
		if len(args) > 1 {
			channels := make([]string, len(args)-1)
			for i, arg := range args[1:] {
				channels[i] = string(arg)
			}
			unsubscribed = h.PubSub.Unsubscribe(state.subscriber, channels...)
		} else {
			unsubscribed = h.PubSub.Unsubscribe(state.subscriber)
		}
		if len(unsubscribed) == 0 {
			return makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("unsubscribe")),
				proto.NewBulkString([]byte("")),
				proto.NewInteger(0),
			}, state.respVersion)
		}
		resp := &MultiResponse{
			Responses: make([]proto.RESP, len(unsubscribed)),
		}
		for i, ch := range unsubscribed {
			resp.Responses[i] = makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("unsubscribe")),
				proto.NewBulkString([]byte(ch)),
				proto.NewInteger(0),
			}, state.respVersion)
		}
		return resp

	case "PUNSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		var unsubscribed []string
		if len(args) > 1 {
			patterns := make([]string, len(args)-1)
			for i, arg := range args[1:] {
				patterns[i] = string(arg)
			}
			unsubscribed = h.PubSub.PUnsubscribe(state.subscriber, patterns...)
		} else {
			unsubscribed = h.PubSub.PUnsubscribe(state.subscriber)
		}
		if len(unsubscribed) == 0 {
			return makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("punsubscribe")),
				proto.NewBulkString([]byte("")),
				proto.NewInteger(0),
			}, state.respVersion)
		}
		resp := &MultiResponse{
			Responses: make([]proto.RESP, len(unsubscribed)),
		}
		for i, p := range unsubscribed {
			resp.Responses[i] = makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("punsubscribe")),
				proto.NewBulkString([]byte(p)),
				proto.NewInteger(0),
			}, state.respVersion)
		}
		return resp

	case "PING":
		return proto.NewSimpleString("PONG")

	case "SSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'SSUBSCRIBE' command")
		}
		channels := make([]string, len(args)-1)
		for i, arg := range args[1:] {
			channels[i] = string(arg)
		}
		subscribed := h.PubSub.SSubscribe(state.subscriber, channels...)
		resp := &MultiResponse{
			Responses: make([]proto.RESP, len(subscribed)),
		}
		for i, ch := range subscribed {
			resp.Responses[i] = makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("ssubscribe")),
				proto.NewBulkString([]byte(ch)),
				proto.NewInteger(int64(i + 1)),
			}, state.respVersion)
		}
		return resp

	case "SUNSUBSCRIBE":
		if h.PubSub == nil {
			return proto.NewError("ERR pubsub not enabled")
		}
		var unsubscribed []string
		if len(args) > 1 {
			channels := make([]string, len(args)-1)
			for i, arg := range args[1:] {
				channels[i] = string(arg)
			}
			unsubscribed = h.PubSub.SUnsubscribe(state.subscriber, channels...)
		} else {
			unsubscribed = h.PubSub.SUnsubscribe(state.subscriber)
		}
		if len(unsubscribed) == 0 {
			return makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("sunsubscribe")),
				proto.NewBulkString([]byte("")),
				proto.NewInteger(0),
			}, state.respVersion)
		}
		resp := &MultiResponse{
			Responses: make([]proto.RESP, len(unsubscribed)),
		}
		for i, ch := range unsubscribed {
			resp.Responses[i] = makePushOrArray([]proto.RESP{
				proto.NewBulkString([]byte("sunsubscribe")),
				proto.NewBulkString([]byte(ch)),
				proto.NewInteger(0),
			}, state.respVersion)
		}
		return resp

	case "QUIT":
		return &PubSubQuitSignal{}

	default:
		return proto.NewError("ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context")
	}
}

func (h *Handler) registerMonitorClient(state *connState) {
	h.monitorMu.Lock()
	defer h.monitorMu.Unlock()
	if h.monitorClients == nil {
		h.monitorClients = make(map[*connState]chan []byte)
	}
	ch := make(chan []byte, 1024)
	state.monitorCh = ch
	h.monitorClients[state] = ch
}

func (h *Handler) unregisterMonitorClient(state *connState) {
	h.monitorMu.Lock()
	defer h.monitorMu.Unlock()
	if ch, ok := h.monitorClients[state]; ok {
		close(ch)
		delete(h.monitorClients, state)
	}
}

func (h *Handler) broadcastToMonitors(cmd string, args [][]byte, remoteAddr string) {
	msg := formatMonitorMessage(cmd, args, remoteAddr)
	h.monitorMu.Lock()
	defer h.monitorMu.Unlock()
	for _, ch := range h.monitorClients {
		select {
		case ch <- msg:
		default:
		}
	}
}

func formatMonitorMessage(cmd string, args [][]byte, remoteAddr string) []byte {
	now := time.Now()
	sec := now.Unix()
	usec := now.Nanosecond() / 1000
	timestamp := fmt.Sprintf("%d.%06d", sec, usec)

	var b strings.Builder
	b.WriteString("+")
	b.WriteString(timestamp)
	b.WriteString(" [0 ")
	b.WriteString(remoteAddr)
	b.WriteString("]")
	b.WriteString(" \"")
	b.WriteString(cmd)
	b.WriteString("\"")
	for _, arg := range args {
		b.WriteString(" \"")
		escaped := strings.ReplaceAll(string(arg), "\\", "\\\\")
		escaped = strings.ReplaceAll(escaped, "\"", "\\\"")
		b.WriteString(escaped)
		b.WriteString("\"")
	}
	b.WriteString("\r\n")
	return []byte(b.String())
}

func (h *Handler) runMonitorLoop(conn net.Conn, writer *bufio.Writer, state *connState, remoteAddr string) {
	ch := state.monitorCh
	if ch == nil {
		return
	}

	logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("进入 MONITOR 模式")

	// 先发送 OK 响应
	if err := writer.Flush(); err != nil {
		return
	}

	reader := bufio.NewReader(conn)
	cmdCh := make(chan *proto.Array, 16)
	errCh := make(chan error, 1)
	done := make(chan struct{})
	defer close(done)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Logger.Error().
					Str("remote_addr", remoteAddr).
					Interface("panic", r).
					Str("stack", string(debug.Stack())).
					Msg("recovered panic in monitor reader")
				select {
				case errCh <- fmt.Errorf("panic recovered: %v", r):
				case <-done:
				}
			}
		}()
		for {
			req, err := proto.ReadRESP(reader)
			if err != nil {
				select {
				case errCh <- err:
				case <-done:
				}
				return
			}
			select {
			case cmdCh <- req:
			case <-done:
				return
			}
		}
	}()

	flushTicker := time.NewTicker(100 * time.Millisecond)
	defer flushTicker.Stop()

	for {
		select {
		case <-state.ctx.Done():
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Msg("monitor loop cancelled by context")
			return

		case msg, ok := <-ch:
			if !ok {
				return
			}
			if _, err := writer.Write(msg); err != nil {
				return
			}

		case req := <-cmdCh:
			if len(req.Args) > 0 {
				cmd := strings.ToUpper(string(req.Args[0]))
				h.recordOps()
				h.incrementCmdCounter(cmd)
			}
			resp := h.processMonitorCommand(req, remoteAddr)
			if _, isQuit := resp.(*PubSubQuitSignal); isQuit {
				_ = proto.WriteRESP(writer, proto.NewSimpleString("OK"))
				_ = writer.Flush()
				return
			}
			if err := proto.WriteRESP(writer, resp); err != nil {
				return
			}

		case <-flushTicker.C:
			if n := writer.Buffered(); n > 0 {
				if err := writer.Flush(); err != nil {
					return
				}
				h.connsMu.Lock()
				m, tracked := h.conns[state]
				if tracked {
					m.outputBytes += int64(n)
				}
				h.connsMu.Unlock()
				// CLIENT NOEVICT ON：输出缓冲区超限不主动断开该连接
				if tracked && h.OutputBufferLimit > 0 && m.outputBytes > h.OutputBufferLimit && !state.noEvict.Load() {
					logger.Logger.Warn().
						Str("remote_addr", remoteAddr).
						Int64("output_bytes", m.outputBytes).
						Int64("limit", h.OutputBufferLimit).
						Msg("客户端输出缓冲区超限，断开连接")
					return
				}
			}

		case err := <-errCh:
			logger.Logger.Debug().Str("remote_addr", remoteAddr).Err(err).Msg("monitor read error")
			return
		}
	}
}

func (h *Handler) processMonitorCommand(req *proto.Array, remoteAddr string) proto.RESP {
	args := req.Args
	if len(args) == 0 {
		return proto.NewError("ERR no command")
	}
	cmd := strings.ToUpper(string(args[0]))
	switch cmd {
	case "QUIT":
		return &PubSubQuitSignal{}
	case "PING":
		return proto.NewSimpleString("PONG")
	default:
		return proto.NewError("ERR only PING / QUIT allowed in this context")
	}
}

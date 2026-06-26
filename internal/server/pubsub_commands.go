package server

import (
	"fmt"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"strconv"
	"strings"
	"time"
)

// handlePUBLISH 实现 PUBLISH 命令
func (h *Handler) handlePUBLISH(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if h.PubSub == nil {
		return proto.NewError("ERR pubsub not enabled")
	}
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'PUBLISH' command")
	}
	channel := string(args[0])
	message := args[1]
	count := h.PubSub.Publish(channel, message)
	// #nosec G115 - count is bounded by practical data size limits
	return proto.NewInteger(int64(count))
}

// handleSUBSCRIBE 实现 SUBSCRIBE 命令
func (h *Handler) handleSUBSCRIBE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if h.PubSub == nil {
		return proto.NewError("ERR pubsub not enabled")
	}
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'SUBSCRIBE' command")
	}
	channels := make([]string, len(args))
	for i, arg := range args {
		channels[i] = string(arg)
	}
	state.mu.Lock()
	if state.subscriber == nil {
		state.subscriber = store.NewSubscriber(fmt.Sprintf("%s:%d", remoteAddr, time.Now().UnixNano()))
	}
	state.mu.Unlock()
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
}

// handlePSUBSCRIBE 实现 PSUBSCRIBE 命令
func (h *Handler) handlePSUBSCRIBE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if h.PubSub == nil {
		return proto.NewError("ERR pubsub not enabled")
	}
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'PSUBSCRIBE' command")
	}
	patterns := make([]string, len(args))
	for i, arg := range args {
		patterns[i] = string(arg)
	}
	state.mu.Lock()
	if state.subscriber == nil {
		state.subscriber = store.NewSubscriber(fmt.Sprintf("%s:%d", remoteAddr, time.Now().UnixNano()))
	}
	state.mu.Unlock()
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
}

// handleUNSUBSCRIBE 实现 UNSUBSCRIBE 命令
func (h *Handler) handleUNSUBSCRIBE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if h.PubSub == nil {
		return proto.NewError("ERR pubsub not enabled")
	}
	if state.subscriber == nil {
		// Not in pubsub mode, return empty confirmation
		channel := ""
		if len(args) >= 1 {
			channel = string(args[0])
		}
		return makePushOrArray([]proto.RESP{
			proto.NewBulkString([]byte("unsubscribe")),
			proto.NewBulkString([]byte(channel)),
			proto.NewInteger(0),
		}, state.respVersion)
	}
	var unsubscribed []string
	if len(args) >= 1 {
		channels := make([]string, len(args))
		for i, arg := range args {
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
}

// handlePUNSUBSCRIBE 实现 PUNSUBSCRIBE 命令
func (h *Handler) handlePUNSUBSCRIBE(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if h.PubSub == nil {
		return proto.NewError("ERR pubsub not enabled")
	}
	if state.subscriber == nil {
		// Not in pubsub mode, return empty confirmation
		pattern := ""
		if len(args) >= 1 {
			pattern = string(args[0])
		}
		return makePushOrArray([]proto.RESP{
			proto.NewBulkString([]byte("punsubscribe")),
			proto.NewBulkString([]byte(pattern)),
			proto.NewInteger(0),
		}, state.respVersion)
	}
	var unsubscribed []string
	if len(args) >= 1 {
		patterns := make([]string, len(args))
		for i, arg := range args {
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
}

// handlePUBSUB 实现 PUBSUB 命令
func (h *Handler) handlePUBSUB(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if h.PubSub == nil {
		return proto.NewError("ERR pubsub not enabled")
	}
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'PUBSUB' command")
	}
	subcommand := strings.ToUpper(string(args[0]))
	switch subcommand {
	case "CHANNELS":
		pattern := "*"
		if len(args) >= 2 {
			pattern = string(args[1])
		}
		channels := h.PubSub.GetChannels(pattern)
		results := make([][]byte, len(channels))
		for i, ch := range channels {
			results[i] = []byte(ch)
		}
		return &proto.Array{Args: results}
	case "NUMSUB":
		if len(args) < 2 {
			return proto.NewError("ERR wrong number of arguments for 'PUBSUB NUMSUB' command")
		}
		results := make([][]byte, 0)
		for i := 1; i < len(args); i++ {
			channel := string(args[i])
			count := h.PubSub.GetSubscriberCount(channel)
			results = append(results, []byte(channel), []byte(strconv.FormatInt(int64(count), 10)))
		}
		return &proto.Array{Args: results}
	case "NUMPAT":
		count := h.PubSub.GetPatternCount()
		return proto.NewInteger(int64(count))
	case "HELP":
		return &proto.Array{Args: [][]byte{
			[]byte("PUBSUB CHANNELS [pattern]  -- Return the list of active channels matching a pattern."),
			[]byte("PUBSUB NUMSUB [channel ...] -- Return the number of subscribers for the specified channels."),
			[]byte("PUBSUB NUMPAT              -- Return the number of subscriptions to patterns."),
			[]byte("PUBSUB HELP                -- Show helpful text about this subcommand."),
		}}
	default:
		return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", subcommand))
	}

	// Transaction commands - 事务命令
}

package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
)

// handleZINTER 实现 ZINTER 命令（Redis 7.0+）
func (h *Handler) handleZINTER(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'ZINTER' command")
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	if numKeys < 1 {
		return proto.NewError("ERR syntax error")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}
	weights := []float64{}
	aggregate := "SUM"
	withScores := false
	i := 1 + numKeys
	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "WEIGHTS":
			if i+numKeys >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			weights = make([]float64, numKeys)
			for w := 0; w < numKeys; w++ {
				f, err := strconv.ParseFloat(string(args[i+1+w]), 64)
				if err != nil {
					return proto.NewError("ERR value is not a float")
				}
				weights[w] = f
			}
			i += numKeys + 1
		case "AGGREGATE":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			aggregate = strings.ToUpper(string(args[i+1]))
			if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
				return proto.NewError("ERR syntax error")
			}
			i += 2
		case "WITHSCORES":
			withScores = true
			i++
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", args[i]))
		}
	}
	members, err := h.Db.ZInter(keys, weights, aggregate)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	if len(members) == 0 {
		return &proto.Array{Args: [][]byte{}}
	}
	if withScores {
		result := make([][]byte, 0, len(members)*2)
		for _, m := range members {
			result = append(result, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
		}
		return &proto.Array{Args: result}
	}
	result := make([][]byte, len(members))
	for i, m := range members {
		result[i] = []byte(m.Member)
	}
	return &proto.Array{Args: result}
}

// handleZUNION 实现 ZUNION 命令（Redis 7.0+）
func (h *Handler) handleZUNION(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 2 {
		return proto.NewError("ERR wrong number of arguments for 'ZUNION' command")
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return proto.NewError("ERR value is not an integer")
	}
	if numKeys < 1 {
		return proto.NewError("ERR syntax error")
	}
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = string(args[1+i])
	}
	weights := []float64{}
	aggregate := "SUM"
	withScores := false
	i := 1 + numKeys
	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "WEIGHTS":
			if i+numKeys >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			weights = make([]float64, numKeys)
			for w := 0; w < numKeys; w++ {
				f, err := strconv.ParseFloat(string(args[i+1+w]), 64)
				if err != nil {
					return proto.NewError("ERR value is not a float")
				}
				weights[w] = f
			}
			i += numKeys + 1
		case "AGGREGATE":
			if i+1 >= len(args) {
				return proto.NewError("ERR syntax error")
			}
			aggregate = strings.ToUpper(string(args[i+1]))
			if aggregate != "SUM" && aggregate != "MIN" && aggregate != "MAX" {
				return proto.NewError("ERR syntax error")
			}
			i += 2
		case "WITHSCORES":
			withScores = true
			i++
		default:
			return proto.NewError(fmt.Sprintf("ERR syntax error, unknown option '%s'", args[i]))
		}
	}
	members, err := h.Db.ZUnion(keys, weights, aggregate)
	if err != nil {
		if errors.Is(err, store.ErrWrongType) {
			return proto.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		return proto.NewError(fmt.Sprintf("ERR %v", err))
	}
	if len(members) == 0 {
		return &proto.Array{Args: [][]byte{}}
	}
	if withScores {
		result := make([][]byte, 0, len(members)*2)
		for _, m := range members {
			result = append(result, []byte(m.Member), []byte(strconv.FormatFloat(m.Score, 'f', -1, 64)))
		}
		return &proto.Array{Args: result}
	}
	result := make([][]byte, len(members))
	for i, m := range members {
		result[i] = []byte(m.Member)
	}
	return &proto.Array{Args: result}
}

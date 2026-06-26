package server

import (
	"fmt"
	"strings"

	"github.com/lbp0200/BoltDB/internal/proto"
)

// handleACL 实现 ACL 命令的基本子命令
func handleACL(args [][]byte) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'ACL' command")
	}

	subcommand := strings.ToUpper(string(args[0]))
	switch subcommand {
	case "WHOAMI":
		return proto.NewBulkString([]byte("default"))
	case "LIST":
		return &proto.Array{Args: [][]byte{
			[]byte("user default on nopass ~* +@all"),
		}}
	case "USERS":
		return &proto.Array{Args: [][]byte{
			[]byte("default"),
		}}
	case "CAT":
		return &proto.Array{Args: [][]byte{
			[]byte("@admin"),
			[]byte("@dangerous"),
			[]byte("@fast"),
			[]byte("@keyspace"),
			[]byte("@list"),
			[]byte("@read"),
			[]byte("@set"),
			[]byte("@slow"),
			[]byte("@sortedset"),
			[]byte("@stream"),
			[]byte("@string"),
			[]byte("@write"),
		}}
	case "HELP":
		return &proto.Array{Args: [][]byte{
			[]byte("ACL <subcommand> [...]"),
			[]byte("ACL CAT [category]"),
			[]byte("ACL HELP"),
			[]byte("ACL LIST"),
			[]byte("ACL USERS"),
			[]byte("ACL WHOAMI"),
		}}
	default:
		return proto.NewError(fmt.Sprintf("ERR unknown subcommand '%s'", subcommand))
	}
}

// handleExpireTime 实现 EXPIRETIME 命令
func (h *Handler) handleExpireTime(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'EXPIRETIME' command")
	}
	key := string(args[0])
	expireTime, err := h.Db.ExpireTime(key)
	if err != nil {
		return proto.NewInteger(-2)
	}
	return proto.NewInteger(expireTime)
}

// handlePExpireTime 实现 PEXPIRETIME 命令
func (h *Handler) handlePExpireTime(state *connState, args [][]byte, remoteAddr string) proto.RESP {
	if len(args) < 1 {
		return proto.NewError("ERR wrong number of arguments for 'PEXPIRETIME' command")
	}
	key := string(args[0])
	pexpireTime, err := h.Db.PExpireTime(key)
	if err != nil {
		return proto.NewInteger(-2)
	}
	return proto.NewInteger(pexpireTime)
}

package server

import (
	"context"
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/cluster"
	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/lbp0200/BoltDB/internal/store"
	"github.com/zeebo/assert"
)

func TestCommandRegistryCount(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 255, len(commandRegistry))
	assert.Equal(t, 255, len(commandMap))
}

func TestCommandMapKeysMatchRegistry(t *testing.T) {
	t.Parallel()
	for _, c := range commandRegistry {
		m, ok := commandMap[c.name]
		if !ok {
			t.Fatalf("command %s missing from commandMap", c.name)
		}
		assert.Equal(t, c.arity, m.arity)
	}
}

func TestCommandRegistrySorted(t *testing.T) {
	t.Parallel()
	for i := 1; i < len(commandRegistry); i++ {
		if commandRegistry[i-1].name > commandRegistry[i].name {
			t.Fatalf("commandRegistry not sorted at index %d: %s > %s",
				i, commandRegistry[i-1].name, commandRegistry[i].name)
		}
	}
}

func TestHandleCommandNoArgs(t *testing.T) {
	t.Parallel()
	resp := handleCommand(nil)
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 255, len(na.Elems))

	for _, elem := range na.Elems {
		cmdInfo, ok := elem.(*proto.NestedArray)
		assert.True(t, ok)
		assert.Equal(t, 6, len(cmdInfo.Elems))

		name, ok := cmdInfo.Elems[0].(*proto.BulkString)
		assert.True(t, ok)
		if len(string(*name)) == 0 {
			t.Fatal("empty command name")
		}
	}
}

func TestHandleCommandCount(t *testing.T) {
	t.Parallel()
	resp := handleCommand([][]byte{[]byte("COUNT")})
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(255), int64(*integer))
}

func TestHandleCommandInfoExisting(t *testing.T) {
	t.Parallel()
	resp := handleCommand([][]byte{[]byte("INFO"), []byte("GET"), []byte("SET")})
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(na.Elems))

	getInfo := na.Elems[0].(*proto.NestedArray)
	getName := getInfo.Elems[0].(*proto.BulkString)
	assert.Equal(t, "GET", string(*getName))

	setInfo := na.Elems[1].(*proto.NestedArray)
	setName := setInfo.Elems[0].(*proto.BulkString)
	assert.Equal(t, "SET", string(*setName))
}

func TestHandleCommandInfoUnknown(t *testing.T) {
	t.Parallel()
	resp := handleCommand([][]byte{[]byte("INFO"), []byte("UNKNOWNCMD")})
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(na.Elems))
	_, ok = na.Elems[0].(proto.NilArray)
	assert.True(t, ok)
}

func TestHandleCommandInfoMixed(t *testing.T) {
	t.Parallel()
	resp := handleCommand([][]byte{[]byte("INFO"), []byte("GET"), []byte("NONEXISTENT")})
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 2, len(na.Elems))

	_, ok = na.Elems[0].(*proto.NestedArray)
	assert.True(t, ok)
	_, ok = na.Elems[1].(proto.NilArray)
	assert.True(t, ok)
}

func TestHandleCommandInfoNoArgsReturnsNilArray(t *testing.T) {
	t.Parallel()
	resp := handleCommand([][]byte{[]byte("INFO")})
	_, ok := resp.(proto.NilArray)
	assert.True(t, ok)
}

func TestHandleCommandUnknownSubcommand(t *testing.T) {
	t.Parallel()
	// GETKEYS 已是合法子命令：无参时返回参数错误而非 unknown subcommand
	resp := handleCommand([][]byte{[]byte("GETKEYS")})
	err, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*err), "GETKEYS"))
}

func TestBuildCommandInfoStructure(t *testing.T) {
	t.Parallel()
	c := cmdInfo{
		name:     "TESTCMD",
		arity:    3,
		flags:    []commandFlag{flagWrite, flagDenyOOM},
		firstKey: 1,
		lastKey:  1,
		step:     1,
	}
	resp := buildCommandInfo(c)
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 6, len(na.Elems))

	name := na.Elems[0].(*proto.BulkString)
	assert.Equal(t, "TESTCMD", string(*name))

	arity := na.Elems[1].(*proto.Integer)
	assert.Equal(t, int64(3), int64(*arity))

	flags := na.Elems[2].(*proto.Array)
	assert.Equal(t, 2, len(flags.Args))

	firstKey := na.Elems[3].(*proto.Integer)
	assert.Equal(t, int64(1), int64(*firstKey))

	lastKey := na.Elems[4].(*proto.Integer)
	assert.Equal(t, int64(1), int64(*lastKey))

	step := na.Elems[5].(*proto.Integer)
	assert.Equal(t, int64(1), int64(*step))
}

func TestBuildCommandInfoNoKeyPositions(t *testing.T) {
	t.Parallel()
	c := cmdInfo{
		name:  "PING",
		arity: 1,
		flags: []commandFlag{flagReadonly},
	}
	resp := buildCommandInfo(c)
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)

	flags := na.Elems[2].(*proto.Array)
	assert.Equal(t, 1, len(flags.Args))
	assert.Equal(t, "readonly", string(flags.Args[0]))
}

func TestBuildCommandInfoNoFlags(t *testing.T) {
	t.Parallel()
	c := cmdInfo{
		name:  "TESTCMD",
		arity: 1,
	}
	resp := buildCommandInfo(c)
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)

	flags := na.Elems[2].(*proto.Array)
	assert.Equal(t, 0, len(flags.Args))
}

func TestHandleCommandCaseInsensitive(t *testing.T) {
	t.Parallel()
	resp := handleCommand([][]byte{[]byte("count")})
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(255), int64(*integer))

	resp = handleCommand([][]byte{[]byte("Count")})
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(255), int64(*integer))
}

func TestHandleCommandInfoCaseInsensitive(t *testing.T) {
	t.Parallel()
	resp := handleCommand([][]byte{[]byte("info"), []byte("get")})
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(na.Elems))

	cmdInfo := na.Elems[0].(*proto.NestedArray)
	cmdName := cmdInfo.Elems[0].(*proto.BulkString)
	assert.Equal(t, "GET", string(*cmdName))
}

func TestHandleCommandInfoReadonlyFlagExists(t *testing.T) {
	t.Parallel()
	get, ok := commandMap["GET"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(get.flags))
	assert.Equal(t, flagReadonly, get.flags[0])
}

func TestHandleCommandInfoWriteFlagExists(t *testing.T) {
	t.Parallel()
	set, ok := commandMap["SET"]
	assert.True(t, ok)
	hasWrite := false
	hasDenyOOM := false
	for _, f := range set.flags {
		if f == flagWrite {
			hasWrite = true
		}
		if f == flagDenyOOM {
			hasDenyOOM = true
		}
	}
	assert.True(t, hasWrite)
	assert.True(t, hasDenyOOM)
}

func TestHandleCommandKeyPositions(t *testing.T) {
	t.Parallel()
	mset, ok := commandMap["MSET"]
	assert.True(t, ok)
	assert.Equal(t, 1, mset.firstKey)
	assert.Equal(t, -1, mset.lastKey)
	assert.Equal(t, 2, mset.step)

	bitop, ok := commandMap["BITOP"]
	assert.True(t, ok)
	assert.Equal(t, 2, bitop.firstKey)
	assert.Equal(t, -1, bitop.lastKey)
	assert.Equal(t, 1, bitop.step)
}

func TestHandleCommandAllInfosHaveSixElements(t *testing.T) {
	t.Parallel()
	resp := handleCommand(nil)
	na := resp.(*proto.NestedArray)
	for i, elem := range na.Elems {
		cmdInfo := elem.(*proto.NestedArray)
		if len(cmdInfo.Elems) != 6 {
			t.Fatalf("command at index %d has %d elements", i, len(cmdInfo.Elems))
		}
	}
}

func TestHandleCommandAllNamesNonEmpty(t *testing.T) {
	t.Parallel()
	resp := handleCommand(nil)
	na := resp.(*proto.NestedArray)
	for _, elem := range na.Elems {
		cmdInfo := elem.(*proto.NestedArray)
		name := cmdInfo.Elems[0].(*proto.BulkString)
		if len(string(*name)) == 0 {
			t.Fatal("empty command name")
		}
	}
}

func TestHandleCommandAllFlagsValid(t *testing.T) {
	t.Parallel()
	validFlags := map[commandFlag]bool{
		flagWrite:    true,
		flagReadonly: true,
		flagAdmin:    true,
		flagPubSub:   true,
		flagNoscript: true,
		flagDenyOOM:  true,
	}
	for _, c := range commandRegistry {
		for _, f := range c.flags {
			if !validFlags[f] {
				t.Fatalf("command %s has unknown flag %s", c.name, f)
			}
		}
	}
}

func TestHandleCommandFlagConsistency(t *testing.T) {
	t.Parallel()
	writeCommands := make(map[string]bool)
	for _, c := range commandRegistry {
		for _, f := range c.flags {
			if f == flagWrite {
				writeCommands[c.name] = true
				break
			}
		}
	}
	assert.True(t, writeCommands["SET"])
	assert.True(t, writeCommands["DEL"])
	if writeCommands["GET"] {
		t.Fatal("GET should not have write flag")
	}
	if writeCommands["PING"] {
		t.Fatal("PING should not have write flag")
	}
}

func TestCommandInfoNegativeArity(t *testing.T) {
	t.Parallel()
	ping, ok := commandMap["PING"]
	assert.True(t, ok)
	assert.Equal(t, -1, ping.arity)

	cluster, ok := commandMap["CLUSTER"]
	assert.True(t, ok)
	assert.Equal(t, -2, cluster.arity)
}

func TestClusterSubcommandArity(t *testing.T) {
	t.Parallel()
	dbPath := t.TempDir()
	db, err := store.NewBotreonStore(dbPath)
	assert.NoError(t, err)
	defer db.Close()

	c, err := cluster.NewCluster(db, "", "127.0.0.1:6379", context.Background())
	assert.NoError(t, err)

	cc := cluster.NewClusterCommands(c)

	tests := []struct {
		subcommand string
		args       []string
		wantOK     bool
	}{
		{"MEET", []string{}, false},
		{"MEET", []string{"127.0.0.1", "6379"}, true},
		{"MEET", []string{"127.0.0.1", "6379", "nodeid"}, true},
		{"NODES", []string{}, true},
		{"NODES", []string{"extra"}, true},
		{"INFO", []string{}, true},
		{"MYID", []string{}, true},
		{"KEYSLOT", []string{}, false},
		{"KEYSLOT", []string{"mykey"}, true},
		{"SLOTS", []string{}, true},
		{"ADDSLOTS", []string{}, false},
		{"ADDSLOTS", []string{"0"}, true},
		{"FORGET", []string{}, false},
		{"FORGET", []string{"nodeid"}, true},
		{"SAVECONFIG", []string{}, true},
		{"FLUSHSLOTS", []string{}, true},
		{"COUNTKEYSINSLOT", []string{}, false},
		{"COUNTKEYSINSLOT", []string{"0"}, true},
		{"EPOCH", []string{}, true},
		{"SLAVES", []string{}, false},
		{"SLAVES", []string{"nodeid"}, true},
		{"RESET", []string{}, true},
		{"RESET", []string{"HARD"}, true},
		{"CALLS", []string{}, true},
		{"SETSLOT", []string{}, false},
		{"SETSLOT", []string{"0", "NODE", "nodeid"}, true},
		{"DELSLOTS", []string{}, false},
		{"DELSLOTS", []string{"0"}, true},
	}

	for _, tt := range tests {
		args := make([]string, 0, 1+len(tt.args))
		args = append(args, tt.subcommand)
		args = append(args, tt.args...)

		_, err := cc.HandleCommand(args)
		if tt.wantOK {
			if err != nil && strings.Contains(err.Error(), "unknown subcommand") {
				t.Errorf("CLUSTER %s: unexpected unknown subcommand error: %v", args, err)
			}
		} else {
			if err == nil {
				t.Errorf("CLUSTER %s: expected arity error, got nil", args)
			}
		}
	}
}

func TestCommandInfoFlagCheck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		flag commandFlag
		want bool
	}{
		{"BZPOPMAX", flagNoscript, true},
		{"BZPOPMAX", flagWrite, true},
		{"BZPOPMIN", flagNoscript, true},
		{"BZPOPMIN", flagWrite, true},
		{"ZMPOP", flagWrite, true},
		{"ZMPOP", flagDenyOOM, true},
		{"GETDEL", flagWrite, true},
		{"GETEX", flagWrite, true},
	}
	for _, tt := range tests {
		c, ok := commandMap[tt.name]
		if !ok {
			t.Fatalf("command %s missing from registry", tt.name)
		}
		found := false
		for _, f := range c.flags {
			if f == tt.flag {
				found = true
				break
			}
		}
		if found != tt.want {
			t.Fatalf("command %s flag %s: got %v, want %v", tt.name, tt.flag, found, tt.want)
		}
	}
}

func TestCommandInfoAdminCommands(t *testing.T) {
	t.Parallel()
	adminCmds := []string{"BGSAVE", "CLIENT", "CONFIG", "FLUSHALL", "FLUSHDB",
		"LATENCY", "MODULE", "MONITOR", "REPLCONF", "REPLICAOF", "SAVE",
		"SHUTDOWN", "SLAVEOF", "SLOWLOG"}
	for _, name := range adminCmds {
		c, ok := commandMap[name]
		if !ok {
			t.Fatalf("admin command %s missing from registry", name)
		}
		hasAdmin := false
		for _, f := range c.flags {
			if f == flagAdmin {
				hasAdmin = true
				break
			}
		}
		if !hasAdmin {
			t.Fatalf("admin command %s missing admin flag", name)
		}
	}
}

func TestCommandInfoPubSubCommands(t *testing.T) {
	t.Parallel()
	pubsubCmds := []string{"PSUBSCRIBE", "PUBLISH", "PUBSUB", "PUNSUBSCRIBE", "SUBSCRIBE", "UNSUBSCRIBE"}
	for _, name := range pubsubCmds {
		c, ok := commandMap[name]
		if !ok {
			t.Fatalf("pubsub command %s missing from registry", name)
		}
		hasPubSub := false
		for _, f := range c.flags {
			if f == flagPubSub {
				hasPubSub = true
				break
			}
		}
		if !hasPubSub {
			t.Fatalf("pubsub command %s missing pubsub flag", name)
		}
	}
}

func TestCommandInfoArityBounds(t *testing.T) {
	t.Parallel()
	for _, c := range commandRegistry {
		if c.arity == 0 {
			t.Fatalf("command %s has arity 0 which is invalid", c.name)
		}
	}
}

func TestCommandInfoKeyPositionsConsistency(t *testing.T) {
	t.Parallel()
	for _, c := range commandRegistry {
		if c.firstKey == 0 && c.lastKey == 0 && c.step == 0 {
			continue
		}
		if c.firstKey <= 0 && c.firstKey != -1 {
			t.Fatalf("command %s has invalid firstKey: %d", c.name, c.firstKey)
		}
		if c.lastKey <= 0 && c.lastKey != -1 {
			t.Fatalf("command %s has invalid lastKey: %d", c.name, c.lastKey)
		}
		if c.step <= 0 {
			t.Fatalf("command %s has invalid step: %d", c.name, c.step)
		}
	}
}

func TestHandleCommandDocsExisting(t *testing.T) {
	t.Parallel()
	resp := handleCommand([][]byte{[]byte("DOCS"), []byte("GET"), []byte("SET")})
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	// 每命令 2 个元素：[name, doc]
	assert.Equal(t, 4, len(na.Elems))

	getName, ok := na.Elems[0].(*proto.BulkString)
	assert.True(t, ok)
	assert.Equal(t, "GET", string(*getName))

	// doc 为 [field, value, ...] 数组，包含 arity 字段
	doc, ok := na.Elems[1].(*proto.NestedArray)
	assert.True(t, ok)
	assert.True(t, len(doc.Elems) >= 2)
	// 校验 field 名称出现在 doc 中
	foundArity := false
	for i := 0; i+1 < len(doc.Elems); i += 2 {
		field, ok := doc.Elems[i].(*proto.BulkString)
		if ok && string(*field) == "arity" {
			foundArity = true
		}
	}
	assert.True(t, foundArity)
}

func TestHandleCommandDocsUnknownSkipped(t *testing.T) {
	t.Parallel()
	resp := handleCommand([][]byte{[]byte("DOCS"), []byte("UNKNOWNCMD")})
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	// 未知命令被跳过，返回空数组
	assert.Equal(t, 0, len(na.Elems))
}

func TestHandleCommandDocsAll(t *testing.T) {
	t.Parallel()
	resp := handleCommand([][]byte{[]byte("DOCS")})
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	// 所有命令 × 2（name + doc）
	assert.Equal(t, 2*len(commandRegistry), len(na.Elems))
}

func TestHandleCommandList(t *testing.T) {
	t.Parallel()
	resp := handleCommand([][]byte{[]byte("LIST")})
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	// 所有命令名
	assert.Equal(t, len(commandRegistry), len(arr.Args))
}

func TestHandleCommandHelp(t *testing.T) {
	t.Parallel()
	resp := handleCommand([][]byte{[]byte("HELP")})
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	// 帮助文本含子命令说明
	joined := ""
	for _, a := range arr.Args {
		joined += string(a) + "\n"
	}
	assert.True(t, strings.Contains(joined, "GETKEYS"))
	assert.True(t, strings.Contains(joined, "LIST"))
}

func TestHandleCommandGetKeys(t *testing.T) {
	t.Parallel()
	// DEL k1 k2 k3 → 3 个 key
	resp := handleCommand([][]byte{[]byte("GETKEYS"), []byte("DEL"), []byte("k1"), []byte("k2"), []byte("k3")})
	arr, ok := resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 3, len(arr.Args))
	assert.Equal(t, "k1", string(arr.Args[0]))
	assert.Equal(t, "k3", string(arr.Args[2]))

	// SET k v → 1 个 key
	resp = handleCommand([][]byte{[]byte("GETKEYS"), []byte("SET"), []byte("mykey"), []byte("myval")})
	arr, ok = resp.(*proto.Array)
	assert.True(t, ok)
	assert.Equal(t, 1, len(arr.Args))
	assert.Equal(t, "mykey", string(arr.Args[0]))

	// PING 无 key → 报错
	resp = handleCommand([][]byte{[]byte("GETKEYS"), []byte("PING")})
	errResp, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "no key arguments"))

	// 未知命令 → 报错
	resp = handleCommand([][]byte{[]byte("GETKEYS"), []byte("NOSUCHCMD"), []byte("x")})
	errResp, ok = resp.(*proto.Error)
	assert.True(t, ok)
	assert.True(t, strings.Contains(string(*errResp), "Invalid command"))
}

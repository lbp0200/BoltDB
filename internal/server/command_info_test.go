package server

import (
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
	"github.com/zeebo/assert"
)

func TestCommandRegistryCount(t *testing.T) {
	assert.Equal(t, 225, len(commandRegistry))
	assert.Equal(t, 225, len(commandMap))
}

func TestCommandMapKeysMatchRegistry(t *testing.T) {
	for _, c := range commandRegistry {
		m, ok := commandMap[c.name]
		if !ok {
			t.Fatalf("command %s missing from commandMap", c.name)
		}
		assert.Equal(t, c.arity, m.arity)
	}
}

func TestCommandRegistrySorted(t *testing.T) {
	for i := 1; i < len(commandRegistry); i++ {
		if commandRegistry[i-1].name > commandRegistry[i].name {
			t.Fatalf("commandRegistry not sorted at index %d: %s > %s",
				i, commandRegistry[i-1].name, commandRegistry[i].name)
		}
	}
}

func TestHandleCommandNoArgs(t *testing.T) {
	resp := handleCommand(nil)
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 225, len(na.Elems))

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
	resp := handleCommand([][]byte{[]byte("COUNT")})
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(225), int64(*integer))
}

func TestHandleCommandInfoExisting(t *testing.T) {
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
	resp := handleCommand([][]byte{[]byte("INFO"), []byte("UNKNOWNCMD")})
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(na.Elems))
	_, ok = na.Elems[0].(proto.NilArray)
	assert.True(t, ok)
}

func TestHandleCommandInfoMixed(t *testing.T) {
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
	resp := handleCommand([][]byte{[]byte("INFO")})
	_, ok := resp.(proto.NilArray)
	assert.True(t, ok)
}

func TestHandleCommandUnknownSubcommand(t *testing.T) {
	resp := handleCommand([][]byte{[]byte("GETKEYS")})
	err, ok := resp.(*proto.Error)
	assert.True(t, ok)
	assert.Equal(t, "ERR unknown subcommand 'GETKEYS'", string(*err))
}

func TestBuildCommandInfoStructure(t *testing.T) {
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
	resp := handleCommand([][]byte{[]byte("count")})
	integer, ok := resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(225), int64(*integer))

	resp = handleCommand([][]byte{[]byte("Count")})
	integer, ok = resp.(*proto.Integer)
	assert.True(t, ok)
	assert.Equal(t, int64(225), int64(*integer))
}

func TestHandleCommandInfoCaseInsensitive(t *testing.T) {
	resp := handleCommand([][]byte{[]byte("info"), []byte("get")})
	na, ok := resp.(*proto.NestedArray)
	assert.True(t, ok)
	assert.Equal(t, 1, len(na.Elems))

	cmdInfo := na.Elems[0].(*proto.NestedArray)
	cmdName := cmdInfo.Elems[0].(*proto.BulkString)
	assert.Equal(t, "GET", string(*cmdName))
}

func TestHandleCommandInfoReadonlyFlagExists(t *testing.T) {
	get, ok := commandMap["GET"]
	assert.True(t, ok)
	assert.Equal(t, 1, len(get.flags))
	assert.Equal(t, flagReadonly, get.flags[0])
}

func TestHandleCommandInfoWriteFlagExists(t *testing.T) {
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
	ping, ok := commandMap["PING"]
	assert.True(t, ok)
	assert.Equal(t, -1, ping.arity)

	cluster, ok := commandMap["CLUSTER"]
	assert.True(t, ok)
	assert.Equal(t, -2, cluster.arity)
}

func TestCommandInfoFlagCheck(t *testing.T) {
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
	for _, c := range commandRegistry {
		if c.arity == 0 {
			t.Fatalf("command %s has arity 0 which is invalid", c.name)
		}
	}
}

func TestCommandInfoKeyPositionsConsistency(t *testing.T) {
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

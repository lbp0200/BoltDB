package replication

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/lbp0200/BoltDB/internal/proto"
)

// The replica advances its own replication offset by the length of what it
// re-serialises: readCommandLoop does
//
//	req, _ := proto.ReadRESP(reader)
//	cmdBytes := serializeCommand(req.Args)
//	sr.lastOffset.Add(int64(len(cmdBytes)))
//
// while the master counted the bytes it wrote into the backlog. So the two
// offsets stay locked together only if parse → re-serialise is byte-identical.
// Any difference is a permanent drift between master_repl_offset and the
// replica's lastOffset (observed as mo=1341949 / so=1341905, 44 bytes, with
// every INCR key still matching exactly).
func TestReplicatedCommandRoundTripsByteIdentically(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cmd  [][]byte
	}{
		{"set", replCmd("SET", "k", "v")},
		{"lpush", replCmd("LPUSH", "dw:list:1", "lv:1:1234")},
		{"incr", replCmd("INCR", "dw:incr:0")},
		{"empty-value", replCmd("SET", "k", "")},
		{"empty-key", replCmd("DEL", "")},
		{"binary", replCmd("SET", "k", "\x00\x01\r\n\xff")},
		{"crlf-in-value", replCmd("SET", "k", "a\r\nb")},
		{"spaces", replCmd("SET", "k", "  spaced  ")},
		{"utf8", replCmd("SET", "键", "值\u00a0")},
		{"many-args", replCmd("MSET", "k1", "v1", "k2", "v2", "k3", "v3")},
		{"expire-normalised", replCmd("PEXPIREAT", "k", "1725000000000")},
		{"marker-shape", replCmd("SET", "dw:converge:1725000000000000000", "done")},
		{"lowercase-cmd", replCmd("lpush", "k", "v")},
		{"long-value", replCmd("SET", "k", strings.Repeat("x", 8192))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			onWire := serializeCommand(tc.cmd)

			resp, err := proto.ReadRESP(bufio.NewReader(bytes.NewReader(onWire)))
			if err != nil {
				t.Fatalf("replica could not parse its own stream: %v", err)
			}

			back := serializeCommand(resp.Args)
			if !bytes.Equal(onWire, back) {
				t.Errorf("round-trip is not byte-identical: master wrote %d bytes, replica re-serialises %d bytes "+
					"(delta %+d) — its lastOffset drifts by that much on every such command\n master: %q\n replica: %q",
					len(onWire), len(back), len(back)-len(onWire), preview(onWire), preview(back))
			}
		})
	}
}

func preview(b []byte) string {
	const max = 64
	if len(b) > max {
		return string(b[:max]) + "…"
	}
	return string(b)
}

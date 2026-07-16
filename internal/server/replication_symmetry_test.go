package server

import (
	"fmt"
	"sort"
	"testing"

	"github.com/lbp0200/BoltDB/internal/replication"
)

// TestReplicationSymmetry_WriteCommandsCovered verifies every isWriteCommand has a
// corresponding handler in executeReplicatedCommand (or is explicitly excluded).
// This prevents drift between the handler dispatch and replication dispatch.
func TestReplicationSymmetry_WriteCommandsCovered(t *testing.T) {
	writeCmds := getWriteCommandSet()

	var missing, excluded []string
	for cmd := range writeCmds {
		if replication.ValidateReplicationMapping(cmd) {
			continue
		}
		if reason, ok := replication.IsReplicationExcluded(cmd); ok {
			excluded = append(excluded, fmt.Sprintf("%s (%s)", cmd, reason))
			continue
		}
		missing = append(missing, cmd)
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		t.Errorf("写命令在 executeReplicatedCommand 中缺失 (%d 个):\n%s",
			len(missing), formatCmdList(missing))
	}

	if t.Failed() {
		t.Logf("已知排除的写命令 (%d 个):", len(excluded))
		for _, e := range excluded {
			t.Logf("  [排除] %s", e)
		}
	}
}

// TestReplicationSymmetry_NoOrphanCommands verifies every ReplicatedCommands entry
// corresponds to an actual isWriteCommand. Orphans indicate either dead code or
// a command that was removed from the handler but not from replication.
func TestReplicationSymmetry_NoOrphanCommands(t *testing.T) {
	writeCmds := getWriteCommandSet()

	var orphan []string
	for cmd := range replication.ReplicatedCommands {
		if !writeCmds[cmd] {
			orphan = append(orphan, cmd)
		}
	}

	if len(orphan) > 0 {
		sort.Strings(orphan)
		t.Errorf("ReplicatedCommands 中的命令不在 isWriteCommand 中 (可能是孤立/过期):\n%s",
			formatCmdList(orphan))
	}
}

func formatCmdList(cmds []string) string {
	s := ""
	for i, c := range cmds {
		if i > 0 {
			s += ", "
		}
		s += c
		if (i+1)%5 == 0 {
			s += "\n"
		}
	}
	return s
}

// TestReplicationWriteCommandChecklist is the executable checklist for adding
// a new write command. Failures mean the command will thrash FULLRESYNC or
// silently diverge under replication.
//
// Human steps (must all pass before merge):
//  1. isWriteCommand / getWriteCommandSet includes the command
//  2. ReplicatedCommands has an executeReplicatedCommand case, OR
//     ReplicatedCommandsExcluded documents why it must not propagate
//  3. shouldPropagateCommand: not double-prop if handler rewrites (SPOP→SREM)
//  4. Integration: attach slave first → write → assert slave state
//  5. Error path: WRONGTYPE / failed write must not enter backlog as success
func TestReplicationWriteCommandChecklist(t *testing.T) {
	// Step 1–2: symmetry (same as Covered + NoOrphan)
	writeCmds := getWriteCommandSet()
	for cmd := range writeCmds {
		mapped := replication.ValidateReplicationMapping(cmd)
		_, excluded := replication.IsReplicationExcluded(cmd)
		if !mapped && !excluded {
			t.Errorf("checklist step1/2 FAIL: write cmd %q not in ReplicatedCommands and not excluded", cmd)
		}
	}
	for cmd := range replication.ReplicatedCommands {
		if !writeCmds[cmd] {
			t.Errorf("checklist step1/2 FAIL: ReplicatedCommands orphan %q (not isWriteCommand)", cmd)
		}
	}

	// Step 3: known canonical rewrites must not generic-propagate
	if shouldPropagateCommand("SPOP") {
		t.Error("checklist step3 FAIL: SPOP must not generic-propagate (handler sends SREM)")
	}
	if !shouldPropagateCommand("SREM") {
		t.Error("checklist step3 FAIL: SREM must propagate")
	}
	if shouldPropagateCommand("MIGRATE") || shouldPropagateCommand("PUBLISH") {
		t.Error("checklist step3 FAIL: excluded cmds must not propagate")
	}

	// Step 4–5 are integration/regression tests (not re-run here).
	// Guard: high-risk non-idempotent / rewrite cmds stay registered.
	mustWrite := []string{"INCR", "LPUSH", "SPOP", "ZPOPMIN", "XCLAIM", "XREADGROUP", "XAUTOCLAIM", "SORT"}
	mustReplicated := []string{"INCR", "LPUSH", "ZPOPMIN", "XCLAIM", "XREADGROUP", "XAUTOCLAIM", "SREM"}
	for _, cmd := range mustWrite {
		if !writeCmds[cmd] {
			t.Errorf("checklist step4 guard: expected isWriteCommand(%q)", cmd)
		}
	}
	for _, cmd := range mustReplicated {
		if !replication.ValidateReplicationMapping(cmd) {
			t.Errorf("checklist step4 guard: expected ReplicatedCommands[%q]", cmd)
		}
	}
}

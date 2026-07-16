#!/usr/bin/env bash
# Targeted mutation checks for high-value server/replication predicates.
# Each mutation is a single, known-dangerous flip; the matching unit tests must FAIL
# (mutation killed). Restores source files after every case.
#
# Usage:
#   bash scripts/targeted-mutation-check.sh
#   bash scripts/targeted-mutation-check.sh remote   # run tests via remote-test.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

REMOTE="${1:-local}"
run_tests() {
  local pattern="$1"
  local pkgs="$2"
  if [[ "$REMOTE" == "remote" ]]; then
    # shellcheck disable=SC2086
    bash scripts/remote-test.sh -count=1 -timeout 120s -run "$pattern" $pkgs
  else
    # Local without -race (Mac M-series). Prefer remote in CI.
    # shellcheck disable=SC2086
    go test -count=1 -timeout 120s -run "$pattern" $pkgs
  fi
}

backup_and_mutate() {
  local file="$1"
  local old="$2"
  local new="$3"
  cp "$file" "${file}.mutbak"
  # portable in-place: write to temp then mv
  if grep -qF "$old" "$file"; then
    # Use python for reliable single-occurrence replace
    python3 - "$file" "$old" "$new" <<'PY'
import sys
path, old, new = sys.argv[1], sys.argv[2], sys.argv[3]
text = open(path).read()
if old not in text:
    sys.exit(2)
open(path, "w").write(text.replace(old, new, 1))
PY
  else
    echo "SKIP: pattern not found in $file: $old"
    rm -f "${file}.mutbak"
    return 2
  fi
}

restore() {
  local file="$1"
  if [[ -f "${file}.mutbak" ]]; then
    mv "${file}.mutbak" "$file"
  fi
}

killed=0
lived=0
skipped=0

run_case() {
  local name="$1"
  local file="$2"
  local old="$3"
  local new="$4"
  local pattern="$5"
  local pkgs="$6"

  echo ""
  echo "=== MUTATION: $name ==="
  if ! backup_and_mutate "$file" "$old" "$new"; then
    skipped=$((skipped + 1))
    return 0
  fi
  set +e
  out=$(run_tests "$pattern" "$pkgs" 2>&1)
  rc=$?
  set -e
  restore "$file"
  if [[ $rc -eq 0 ]]; then
    echo "LIVED (tests still green — oracle gap): $name"
    echo "$out" | tail -5
    lived=$((lived + 1))
  else
    echo "KILLED: $name"
    killed=$((killed + 1))
  fi
}

# 1) Backpressure skippable → silent data loss class
run_case "isTransient treats write rejected as skippable" \
  "internal/replication/reconnect.go" \
  'if strings.Contains(errStr, "key not found") {
		return true
	}
	return false' \
  'if strings.Contains(errStr, "key not found") {
		return true
	}
	if strings.Contains(errStr, "write rejected") {
		return true
	}
	return false' \
  "TestIsTransientReplicationError|TestReplicationApplyErrorDisposition" \
  "./internal/replication/"

# 2) SPOP generic-propagated → double-pop class
run_case "shouldPropagate SPOP true" \
  "internal/server/replication_helper.go" \
  'case "REPLICAOF", "PSYNC", "REPLCONF", "SPOP":
		return false' \
  'case "REPLICAOF", "PSYNC", "REPLCONF":
		return false' \
  "TestShouldPropagateCommand|TestProcessRequestPropagateGate|TestReplicationWriteCommandChecklist" \
  "./internal/server/"

# 3) ASKING sticky (never clear) → weakened redirect safety
run_case "ASKING sticky (no clear)" \
  "internal/server/handler_core.go" \
  'asking := state.clusterAsking
	if asking {
		state.clusterAsking = false
	}' \
  'asking := state.clusterAsking
	// MUTATION: sticky ASKING
	_ = asking' \
  "TestCheckAndHandleRedirect_ImportingWriteFence" \
  "./internal/server/"

# 4) IMPORTING fence removed for writes
run_case "IMPORTING write fence disabled" \
  "internal/server/handler_core.go" \
  'if cmd != "" && isWriteCommand(cmd) && cmd != "RESTORE" {
				return proto.NewError(fmt.Sprintf(
					"ERR slot %d is IMPORTING: client writes fenced during migration (RESTORE allowed)",
					slot))
			}' \
  'if false && cmd != "" && isWriteCommand(cmd) && cmd != "RESTORE" {
				return proto.NewError(fmt.Sprintf(
					"ERR slot %d is IMPORTING: client writes fenced during migration (RESTORE allowed)",
					slot))
			}' \
  "TestCheckAndHandleRedirect_ImportingWriteFence" \
  "./internal/server/"

# 5) MIGRATING source write fence disabled → concurrent SETs corrupt Phase-1
run_case "MIGRATING write fence disabled" \
  "internal/server/handler_core.go" \
  'if cmd != "" && isWriteCommand(cmd) && cmd != "RESTORE" &&
					h.Cluster.IsMigratingSlot(slot) {
					return proto.NewError(fmt.Sprintf(
						"ERR slot %d is MIGRATING: client writes fenced during migration",
						slot))
				}' \
  'if false && cmd != "" && isWriteCommand(cmd) && cmd != "RESTORE" &&
					h.Cluster.IsMigratingSlot(slot) {
					return proto.NewError(fmt.Sprintf(
						"ERR slot %d is MIGRATING: client writes fenced during migration",
						slot))
				}' \
  "TestCheckAndHandleRedirect_MigratingWriteFence" \
  "./internal/server/"

# 6) isErrorResponse always false → failed writes enter backlog
run_case "isErrorResponse always false" \
  "internal/server/replication_helper.go" \
  'func isErrorResponse(resp proto.RESP) bool {
	if resp == nil {
		return true
	}
	switch resp.(type) {
	case *proto.Error, proto.Error:
		return true
	default:
		return false
	}
}' \
  'func isErrorResponse(resp proto.RESP) bool {
	return false
}' \
  "TestIsErrorResponse|TestProcessRequestPropagateGate" \
  "./internal/server/"

# 7) isTransient always true → every apply error silently skipped
run_case "isTransient always true" \
  "internal/replication/reconnect.go" \
  'func isTransientReplicationError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// 键不存在（主从短暂不一致时的正常现象，对 DEL/SREM 等幂等）
	if strings.Contains(errStr, "key not found") {
		return true
	}
	return false
}' \
  'func isTransientReplicationError(err error) bool {
	return err != nil
}' \
  "TestIsTransientReplicationError|TestReplicationApplyErrorDisposition" \
  "./internal/replication/"

# 8) MIGRATE not excluded from propagate → thrash class
run_case "MIGRATE propagates" \
  "internal/server/replication_helper.go" \
  'if _, excluded := replication.IsReplicationExcluded(cmd); excluded {
		return false
	}
	return true' \
  'if _, excluded := replication.IsReplicationExcluded(cmd); excluded {
		return true // MUTATION: excluded cmds still propagate
	}
	return true' \
  "TestShouldPropagateCommand|TestProcessRequestPropagateGate" \
  "./internal/server/"

# 9) Drop error gate in processRequest → WRONGTYPE enters backlog
run_case "processRequest drops isErrorResponse gate" \
  "internal/server/handler_core.go" \
  'if h.Replication != nil && h.Replication.IsMaster() && isWriteCommand(cmd) &&
		shouldPropagateCommand(cmd) && !isErrorResponse(resp) {
		h.Replication.PropagateCommand(propagateArgs)
	}' \
  'if h.Replication != nil && h.Replication.IsMaster() && isWriteCommand(cmd) &&
		shouldPropagateCommand(cmd) {
		h.Replication.PropagateCommand(propagateArgs)
	}' \
  "TestProcessRequest_WrongTypeDoesNotAdvanceOffset" \
  "./internal/server/"

# 10) XREADGROUP not a write command → PEL never propagates
run_case "XREADGROUP removed from isWriteCommand" \
  "internal/server/replication_helper.go" \
  '		// XREADGROUP mutates PEL / LastDeliveredID — must replicate or XCLAIM/XACK diverge
		"XREADGROUP": true,' \
  '		// MUTATION: XREADGROUP not write
		// "XREADGROUP": true,' \
  "TestShouldPropagateCommand|TestReplicationWriteCommandChecklist|TestIsWriteCommand" \
  "./internal/server/"

echo ""
echo "=== Targeted mutation summary ==="
echo "killed=$killed lived=$lived skipped=$skipped"
if [[ "$lived" -gt 0 ]]; then
  echo "FAIL: one or more dangerous mutations lived (tests did not catch them)"
  exit 1
fi
if [[ "$killed" -eq 0 ]]; then
  echo "WARN: no mutations applied/killed (patterns may have drifted)"
  exit 2
fi
echo "OK: all applied mutations killed by existing tests"
exit 0

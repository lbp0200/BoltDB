# Mutation Test Baseline

Initial mutation test baseline for BoltDB.

| Date | Target | Score | Killed | Lived | Total | Notes |
|------|--------|-------|--------|-------|-------|-------|
| 2026-06-26 | internal/store/base.go:ttl>0→ttl>=0 | 0% | 0 | 2 | boundary test gap found |
| 2026-07-16 | targeted server/replication (4 cases) | 100% | 4 | 0 | 4 | initial suite |
| 2026-07-16 | targeted expanded (8 cases) | 100% | 8 | 0 | 8 | + MIGRATING fence / isErrorResponse / isTransient always / MIGRATE prop |
| 2026-07-16 | targeted expanded (10 cases) | 100% | 10 | 0 | 10 | + processRequest error gate / XREADGROUP isWrite |

## Targeted mutation suite (2026-07-16)

```bash
bash scripts/targeted-mutation-check.sh local   # unit packages, no -race
bash scripts/targeted-mutation-check.sh remote  # via remote-test.sh
```

| Mutation | File | Expected killer tests |
|----------|------|----------------------|
| `write rejected` treated as skippable | `reconnect.go` | `TestIsTransient*` / disposition |
| SPOP generic-propagate | `replication_helper.go` | `TestShouldPropagateCommand` / checklist |
| ASKING sticky (never clear flag) | `handler_core.go` | ImportingWriteFence |
| IMPORTING write fence disabled | `handler_core.go` | ImportingWriteFence |
| MIGRATING write fence disabled | `handler_core.go` | MigratingWriteFence |
| `isErrorResponse` always false | `replication_helper.go` | `TestIsErrorResponse` / propagate gate |
| `isTransient` always true | `reconnect.go` | `TestIsTransient*` / disposition |
| MIGRATE still propagates | `replication_helper.go` | ShouldPropagate / gate |
| processRequest drops `!isErrorResponse` | `handler_core.go` | `TestProcessRequest_WrongTypeDoesNotAdvanceOffset` |
| XREADGROUP removed from isWriteCommand | `replication_helper.go` | checklist / IsWriteCommand |

**Result (local 2026-07-16):** killed=10 lived=0.

## Findings

### 2026-06-26: `ttl > 0` → `ttl >= 0` in base.go

Mutation changed `ttl > 0` to `ttl >= 0` in TTL-related code paths.
Tests that passed despite mutation:
- `TestExpire_TTL_NonexistentKey_Coverage`
- `TestExpire_PExpire_ExpireAt_PExpireAt_Coverage`

**Root cause:** These coverage tests only check the happy/error path but never
exercise the `ttl == 0` boundary. The boundary `ttl == 0` (key has exactly 0
second TTL, meaning already expired) is not covered.

**To fix:** Add a test case where TTL is explicitly set to 0 (or an already-expired
value) and verify the key is not accessible.

### 2026-06-26: Follow-up — RENAME TTL tests added + dual-format bug fix

#### Tests added

Added `TestRename_WithTTL` and `TestRename_WithExpiredTTL` to cover both branches
of the `ttl > 0` check in the `Rename` function (base.go:724):

- `TestRename_WithTTL`: RENAME with valid TTL → verifies TTL is preserved
  on the renamed key (exercises `ttl > 0` = true branch)
- `TestRename_WithExpiredTTL`: RENAME after TTL expiry → verifies renamed key
  has no TTL (exercises `else` branch at line 729-734)

The exact `ttl == 0` boundary remains impractical to test deterministically
(requires wall-clock precision), but both code paths are now covered.

#### Bug fix: `Rename` / `copyKeysByPrefix` TTL dual-format

While writing the expired-TTL test, discovered that both `Rename` (base.go:720)
and `copyKeysByPrefix` (base.go:854) read `ExpiresAt()` from Badger but treated
the value as **Unix seconds** via `time.Unix(int64(expiresAt), 0)`, while
`Expire()` writes `ExpiresAt()` in **nanoseconds**.

This caused:
- RENAME of an expired key → key got a ~55-billion-year TTL instead of no TTL
- The bug applied to all key types (string via Rename, list/hash/set/zset/geo
  via copyKeysByPrefix)

**Fix:** Added the same dual-format heuristic (`expiresAt > nowUnix*100`) used
in the `TTL()` function to detect nanosecond vs second format, computing the
correct remaining duration in both cases.

### 2026-06-26: Batch fix — 4 additional TTL dual-format bugs (PTTL, EXPIRETIME/PEXPIRETIME, DUMP, BITFIELD)

A systematic audit of all `.ExpiresAt()` read sites uncovered 4 more locations
that assumed nanosecond-only format:

| file:line | function | impact |
|---|---|---|
| `base.go:465` | `PTTL()` | Seconds-format keys falsely reported as expired (-2) |
| `base.go:526` | `computeAbsoluteExpiry()` | `EXPIRETIME`/`PEXPIRETIME` returned wrong timestamps for seconds-format entries |
| `base.go:1225` | `Dump()` | `DUMP` dropped TTL on seconds-format keys |
| `string.go:1129` | `BitField()` | TTL silently lost after BITFIELD resize (also no WithTTL on re-write) |

**Fix applied to all 4:** Added the `expiresAt > nowUnix*100` heuristic to
detect format and compute the correct remaining duration.

**Tests added:** The existing `TestExpire_*` / `TestTTL_*` / `TestPTTL_*` / `TestDump_*`
suites cover the fixed paths. New `TestRename_WithTTL` / `TestRename_WithExpiredTTL`
cover the Rename code paths.

**Tests verifying the fix on remote (all pass):**
```bash
bash scripts/remote-test.sh -race -timeout 120s -run "Test(Expire|Persist|TTL|Rename|Dump)" ./internal/store/...
bash scripts/remote-test.sh -race -timeout 120s -run "Test(BitField|Set|Get|String)" -short ./internal/store/...
bash scripts/remote-test.sh -race -short ./internal/store/...
```

## Tool Compatibility

- **go-mutesting** (zimmski/go-mutesting): ❌ incompatible with Go 1.25
  (nil pointer in go/types.(*StdSizes).Sizeof due to stale golang.org/x/tools).
- **scripts/manual-mutation.sh**: ✅ simple sed-based mutation tester, compatible
  with any Go version.

## Commands

```bash
# Manual mutation test on a package
bash scripts/manual-mutation.sh ./internal/store/... 120

# Targeted manual mutation example:
#   cp file.go file.go.bak
#   sed -i 's/pattern/replacement/g' file.go
#   go test -count=1 -run TestName ./pkg/...
#   mv file.go.bak file.go
```

## Skipped Mutations (false positives)

Add blacklist checksums here as they are identified.

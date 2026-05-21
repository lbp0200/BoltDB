# Shutdown Race

## Symptom
- On SIGINT/SIGTERM, server hangs and doesn't exit
- Or: server exits but with goroutine leaks
- Or: panic on DB access after DB is closed
- Integration tests (TestGracefulShutdown, TestShutdownWithReplication) fail intermittently
- Test suite times out (total > 300s across many tests)

## Root Cause
- Shutdown sequence in `main.go` is non-trivial:
  ```
  close listener → ServeTCP returns
  → replMgr.Stop()       (close slave TCP connections → unblock reads)
  → cancel()             (cancel root context → all goroutines see Done)
  → handler.Shutdown()   (close all client TCP conns + WaitGroup.Wait)
  → db.CloseWithTimeout() (deferred — guaranteed: 0 goroutines accessing DB)
  ```
- Race scenarios:
  1. **PubSub/Monitor goroutine holds DB reference**: `runPubSubLoop` or `runMonitorLoop` doesn't see `ctx.Done()` fast enough, continues trying to write to DB
  2. **ReplConf ACK race**: `handleSlaveReplicationConnection` blocked on `ReadRESP`, slave connection not closed before `handler.Shutdown()` → `wg.Wait()` hangs
  3. **Double close**: `conn.Close()` called from both `handleConnection` (defer) and `Shutdown()` — `closeOnce` on conn prevents, but log surfaces spurious errors
  4. **WAL sync hang**: BadgerDB `Close()` blocks on `doWrites` drain — this is why `CloseTimeout` exists (2 seconds, then force-return)
  5. **Backup/GC race**: Background backup or retention goroutine still running when DB closes

## Invariant Violated
- **Zero goroutines access DB after handler.Shutdown() returns**
- **All goroutines are tracked**: every `go` spawn must be in `Handler.wg` or `SlaveReconnector.wg`
- **Close timeout must not be hit**: if `CloseTimeout` fires regularly, there's a leak

## Fix
1. Verify `handleSlaveReplicationConnection` is in `Handler.wg` — confirmed: `h.wg.Add(1)` in `handlePSyncWithRDB`
2. Verify `reconnectLoop` is in `SlaveReconnector.wg` — confirmed: `sr.wg.Add(1)` in `Start()`
3. Ensure `replMgr.Stop()` closes all slave connections before `handler.Shutdown()`:
   - `Stop()` calls `slave.Close()` which closes `slave.Conn`
   - This unblocks `ReadRESP` in `handleSlaveReplicationConnection`
   - goroutine sees error → returns → `wg.Done()`
4. Ensure `cancel()` is called after `replMgr.Stop()` but before `handler.Shutdown()`
5. Verify PubSub/Monitor goroutines exit via `ctx.Done()` — they use `state.ctx` (child of root ctx)
6. `CloseTimeout` (2s) prevents BadgerDB hang from cascading

## Prevention
- Each new goroutine must be WG-tracked — code review rule
- Shutdown integration tests verify goroutine count before/after (no leaks)
- `go test -race -timeout 60s ./cmd/integration/ -run "TestGracefulShutdown|TestShutdownWithReplication"`
- If a new goroutine doesn't fit the shutdown contract, update the contract in AGENTS.md

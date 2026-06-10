# Premature Write Deadline Destroys Replication Semantics

**Date:** 2026-06-02
**Scope:** `internal/replication/slave.go`, `internal/replication/psync.go`
**Root cause:** Misapplied defensive programming — `SetWriteDeadline` before `bufio.Writer.Flush()` breaks the replication connection rather than protecting it, causing a reconnect/FULLRESYNC storm.

## Summary

An attempt to prevent `Flush()` from blocking indefinitely during slave replication introduced a 10-second `SetWriteDeadline` on the slave's TCP connection before each `Flush()` call in `SendCommand` and `SendBacklogData`. This triggered a cascade:

1. Slave connects, FULLRESYNC starts, RDB sent
2. Slave begins loading RDB (CPU-bound, no TCP reads)
3. Backlog data written to TCP buffer → buffer fills → `Flush()` waits
4. Deadline fires after 10s → `Flush()` returns error
5. `bufio.Writer` is now in unknown state (Go spec: partial flush possible)
6. Connection is semantically broken — must be discarded
7. Slave gets removed from propagation, reconnects
8. New FULLRESYNC triggered, repeat from step 1

Result: instead of a bounded stall (~seconds), we got an unbounded reconnect storm that lost writes and inflated the duplicate window.

## Key Insight

**Replication write stalls during RDB loading are bounded by definition.** The slave will finish loading and drain the TCP buffer. This is not an indefinite block — it is a synchronous pipeline stall. Adding a timeout to "fix" it can only make things worse because:

- `bufio.Writer` after a failed `Flush()` cannot be reused (partial write may be in kernel buffer, remaining data in user buffer — no way to reconcile)
- A timed-out flush is not "retry the write" — it is "discard the connection"
- Discarding the connection mid-FULLRESYNC guarantees another FULLRESYNC
- The slave must fully drain the backlog anyway; timeout just defers the inevitable to a new connection

## What Went Wrong in the Analysis

| Step | Flaw |
|------|------|
| Identified `Flush()` can block | Correct |
| Concluded indefinite block is unacceptable | Partially wrong — the block is bounded (slave finishes RDB loading) |
| Added `SetWriteDeadline` as defense | Wrong — destroys `bufio.Writer`, breaks connection |
| Added slave removal on write error | Wrong — creates reconnect storm |
| Didn't check `bufio.Writer` post-Flush semantics | Missed critical Go contract |

## Correct Strategy

```
Normal replication writes:      wait patiently (bounded stall)
True interruption (disconnect): let handleSlaveReplicationConnection detect
                                via ReadRESP error, clean up naturally
Resource leaks (goroutines):    fix at source (conn.Close, h.Shutdown)
Reconnection storms:            fix by resetting backoff on success
```

## The Three Real Fixes (v8.19.0)

1. `CLIENT KILL TYPE NORMAL`: close TCP conn after `cancel()` — unblocks `ReadRESP`, goroutine exits
2. `soak_test.go`: add `h.Shutdown()` to cleanup — goroutine check no longer false-positive
3. `reconnect.go`: reset retries on any successful reconnect — backoff doesn't blow up under rapid CLIENT KILL

## Principle

> Do not use write deadlines to "fix" bounded replication stalls; a timed-out `bufio.Writer` must be discarded, and premature timeout amplifies reconnect/FULLRESYNC storms.

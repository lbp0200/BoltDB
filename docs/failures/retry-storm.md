# Retry Storm

## Symptom
- `ActiveRetries` spikes to 100+ (monotonic increase)
- `TotalRetries` grows rapidly
- `WritesBlocked` counter climbs (BadgerDB `doWrites` channel full)
- L0 score stays high (writes are retrying, adding more L0 pressure)
- Write latency spikes to seconds, then times out
- Degradation check: `ActiveRetries > 100` signals collapse

## Root Cause
- A burst of writes triggers BadgerDB conflict errors or "Writes are blocked" errors
- `retryUpdate` retries each failed write with exponential backoff (1ms..50ms for conflict, 1ms..2s for blocked)
- With high concurrency (e.g., 1000 clients all writing), retry goroutines multiply:
  - Each blocked write spawns a retry goroutine
  - Each retry goroutine holds a `writeSlot` (max 50 concurrent)
  - Retry goroutines compete for the same DB transaction slots
  - This makes compaction slower, L0 rises, more writes fail — **positive feedback loop**
- The semaphore (`writeSlot`) limits concurrent retryUpdate goroutines to 50, but:
  - Goroutines waiting on `Acquire()` are still alive and consuming resources
  - If clients don't back off at the application level, they keep sending new requests

## Invariant Violated
- **ActiveRetries must be bounded**: `ActiveRetries <= 100` at all times
- **Goroutine count must plateau**: not monotonic increase
- **Heap must sawtooth**: retry goroutines create allocations, GC must reclaim

## Fix
1. Verify `writeSlot.Acquire()` is called before entering the retry loop
2. Add client-level backpressure: if server is overloaded, send `-BUSY` or `-ERR` early (don't queue)
3. In `retryUpdate`, cap total retry time per write (e.g., max 5s total, fail fast)
4. Add retry budget: if system-wide retries exceed threshold in a window, shed load
5. Ensure clients implement their own backoff (go-redis already does this)

## Prevention
- Monitor `ActiveRetries` in real-time — it's the earliest indicator of collapse
- In soak tests, verify goroutine count plateaus within 30s of steady-state write load
- Set `MaxConcurrentWrites` lower for write-heavy workloads (e.g., 20 instead of 50)
- Use backpressure config to reject before retry storm starts
- Before scaling up write concurrency, verify L0 score stays below 5 under load

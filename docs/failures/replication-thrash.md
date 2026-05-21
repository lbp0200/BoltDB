# Replication Thrash

## Symptom
- Master and slave repeatedly perform FULLRESYNC instead of CONTINUE
- Reconnect count (`reconnectCount`) rises continuously
- Slave falls behind on offset, never catching up
- Network and CPU usage spikes from repeated RDB transfers
- Degradation check: `ReconnectCount > MaxReconnectCount`

## Root Cause
- Slave disconnects from master due to transient errors (L0 pressure, write rejections, network hiccups)
- On reconnect, slave sends PSYNC with old replId and offset
- If the offset is no longer in the master's backlog (`IsOffsetAvailable` returns false), master forces FULLRESYNC
- FULLRESYNC sends RDB snapshot (potentially gigabytes) — time-consuming and CPU-intensive
- During RDB transfer, writes continue on master, backlog advances further
- After RDB load, slave is already behind again — risk of repeat FULLRESYNC

## Invariant Violated
- **Backlog must cover the gap**: `slave_repl_offset >= backlog.AvailableStartOffset()`
- **Reconnect must converge**: after `N` reconnects, slave should stay connected long enough to catch up

## Fix
1. Increase `DefaultBacklogSize` from 1MB to 10MB+ to tolerate longer disconnects
2. Add adaptive backlog sizing based on write throughput
3. Implement `repl-backlog-ttl` — don't free backlog immediately when no slaves connected
4. On FULLRESYNC, master should serve a streaming RDB that doesn't block the backlog
5. Rate-limit reconnects: exponential backoff with jitter (already implemented in `reconnectLoop`, but verify `MaxBackoff` is large enough)

## Prevention
- Monitor `backlog.IsOffsetAvailable(slaveOffset)` proactively
- Alert when `reconnectCount` increases by >5 in 60s
- Soak test with periodic network partitions (use `iptables` or `tc` to simulate)
- Set `repl-backlog-size` proportional to expected write volume × max disconnect time

# State Machine Reference

## 1. Server Lifecycle

```
                  ┌──────────┐
                  │  START   │
                  └────┬─────┘
                       │ db.NewBotreonStore()
                       │ db.NextStartup()
                       │ replMgr (optional slave)
                       │ handler.ServeTCP()
                       v
                  ┌──────────┐
         ┌───────>│ RUNNING  │<───────── replication role switch
         │        └────┬─────┘
         │             │ signal (SIGINT/SIGTERM)
         │             v
         │        ┌──────────┐
         │        │SHUTDOWN  │  ln.Close() → ServeTCP returns
         │        └────┬─────┘  replMgr.Stop()
         │             │        cancel() → ctx.Done()
         │             │        handler.Shutdown()
         │             v
         │        ┌──────────┐
         └────────┤   DONE   │  db.Close() (deferred)
                  └──────────┘
```

**Invariant:** After `backupMgr.Wait()` returns, zero goroutines access the DB. The deferred `db.Close()` is the last operation.

**Ordering** (enforced by `main.go`):
```
close listener → ServeTCP returns
→ replMgr.Stop()       (close slave TCP connections → unblock reads)
→ cancel()             (cancel root context → all goroutines see Done)
→ handler.Shutdown()   (close all client TCP conns + WaitGroup.Wait)
→ backupMgr.Wait()     (wait for in-flight BGSAVE goroutine → no DB access after)
→ db.Close()           (deferred — guaranteed: 0 goroutines accessing DB)
```

---

## 2. Connection State Machine

Each client connection (`connState`) begins in **Normal** and may transition to one of four exclusive modes. Once in a push-based mode (Subscribed/Monitor), the main command loop exits and a specialized loop takes over.

```
                          ┌──────────────┐
              SUBSCRIBE   │              │  PSYNC (master)
         ┌───────────────>│  Subscribed  │─────────────┐
         │                │              │             │
         │                └──────────────┘             │
         │                                             │
         │         ┌──────────────┐                    │
    ┌────────┐     │              │     PSYNC (master) │
    │ Normal │────>│   Monitor    │────────────────────┼──┐
    └───┬────┘     │              │                    │  │
        │          └──────────────┘                    │  │
        │                                              │  │
        │          ┌──────────────┐                    v  v
        │  MULTI   │              │              ┌──────────┐
        ├─────────>│ Transaction  │              │  Repl    │
        │          │              │              │  Slave   │
        │          └──────────────┘              │          │
        │              │  EXEC/DISCARD           └──────────┘
        │              v
        │          (back to Normal)
        │
        v  QUIT / conn.Close / ctx.Done
   ┌──────────┐
   │  CLOSED  │
   └──────────┘
```

### 2.1 Normal → Subscribed

- **Command:** `SUBSCRIBE`, `PSUBSCRIBE`
- **Mechanism:** `state.subscriber = store.NewSubscriber(...)` set in `executeCommand`
- **Detection (handleConnection loop):** `state.subscriber != nil` check at end of each iteration
- **Allowed in Subscribed mode:** `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`, `PING`, `QUIT`
- **Rejected in Subscribed mode:** Everything else (error: "ERR only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context")
- **Exit:** `QUIT` command, connection close, or `ctx.Done()`
- **Cleanup:** `handler.PubSub.RemoveSubscriber(sub)` in defer

### 2.2 Normal → Monitor

- **Command:** `MONITOR`
- **Mechanism:** `state.monitoring = true` in `executeCommand`
- **Detection (handleConnection loop):** `state.monitoring` check at end of each iteration
- **Allowed in Monitor mode:** `PING`, `QUIT`
- **Rejected in Monitor mode:** Everything else (error: "ERR only PING / QUIT allowed in this context")
- **Exit:** `QUIT` command, connection close, or `ctx.Done()`
- **Push content:** Each command executed by any other connection is broadcast as a monitor frame

### 2.3 Normal → Transaction (non-exclusive)

- **Command:** `MULTI`
- **Mechanism:** `state.inTransaction = true`
- **NOT an exclusive mode** — connection stays in Normal for I/O; only command dispatch changes
- **Subsequent commands:** queued in `state.commands` (returns `QUEUED`)
- **Exceptions (not queued):** `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`, `PING`, `QUIT`
- **Exit:** `EXEC` (executes atomically), `DISCARD` (discards queue), connection close

### 2.4 Normal → ReplSlave (connection takeover)

- **Command:** `PSYNC` (on master, detected in `processRequest` before `executeCommand`)
- **Mechanism:** `replicationOwned = true` → `handleConnection` exits without closing conn
- **Not a connState mode** — the connection handle is passed to `handleSlaveReplicationConnection`
- **Lifecycle:** Only accepts `REPLCONF ACK` from the slave, or reads errors/EOF

### 2.5 Legal transition matrix

| From \ To | Normal | Subscribed | Monitor | Transaction | ReplSlave | Closed |
|-----------|--------|------------|---------|-------------|-----------|--------|
| Normal | — | SUBSCRIBE | MONITOR | MULTI | PSYNC | QUIT/err |
| Subscribed | UNSUBSCRIBE all | — | ❌ | ❌ | ❌ | QUIT/err |
| Monitor | ❌ | ❌ | — | ❌ | ❌ | QUIT/err |
| Transaction | EXEC/DISCARD | ❌ | ❌ | — | ❌ | err |
| ReplSlave | ❌ | ❌ | ❌ | ❌ | — | EOF/err |

**Legend:** ❌ = illegal transition (must quit and reconnect)

---

## 3. Replication Slave State Machine

**Package:** `internal/replication/reconnect.go`

Governs the slave-side reconnect loop (`SlaveReconnector.reconnectLoop`).

```
         ┌─────────────────┐
         │  Disconnected   │ <────┐
         └────────┬────────┘      │
                  │ reconnect      │ repl finished or error
                  v                │
         ┌─────────────────┐      │
         │  Connecting     │──────┘
         └────────┬────────┘  error
                  │ tryReplicate handshake
                  v
         ┌─────────────────┐
         │   Syncing       │  full resync (RDB load)
         └────────┬────────┘
                  │ RDB load complete
                  v
         ┌─────────────────┐
         │  Connected      │  readCommandLoop active
         └─────────────────┘
```

### States

| State | Value | Meaning |
|-------|-------|---------|
| `SlaveDisconnected` | 0 | Not connected, may backoff before retry |
| `SlaveConnecting` | 1 | Dial in progress or just connected |
| `SlaveSyncing` | 2 | Full RDB sync in progress |
| `SlaveConnected` | 3 | Streaming replication commands |

### Transitions

| From | To | Trigger | Function | Line |
|------|----|---------|----------|------|
| * | Disconnected | `Stop()` called | `reconnectLoop` | 111 |
| Disconnected | Connecting | Reconnect tick | `reconnectLoop` | 117 |
| Connecting | Syncing | `SendFullResync` → `+FULLRESYNC` | `tryReplicate` | 203 |
| Connecting | Connected | `PSYNC` → `+CONTINUE` (partial) | `tryReplicate` | 217 |
| Syncing | Connected | RDB loaded, `readCommandLoop` starts | `tryReplicate` | 217 |
| Connected | Disconnected | EOF/error on master connection | `reconnectLoop` | 130 |
| Syncing | Disconnected | RDB load error | `reconnectLoop` | 130 |

**Invariant:** Only one transition at a time per `SlaveReconnector`. The loop is sequential.

---

## 4. Sentinel Master State Machine

**Package:** `internal/sentinel/master.go`

```
                  ┌──────┐
        ┌────────>│  ok  │<────────┐
        │         └──┬───┘         │
        │            │ ping fail +  │ ping success
        │            │ downAfter   │ (recover)
        │            v             │
        │         ┌──────┐         │
        │         │sdown │─────────┘
        │         └──┬───┘
        │            │ sdownCount >= quorum
        │            v
        │         ┌──────┐
        │         │odown │
        │         └──┬───┘
        │            │ failover triggered
        │            v
        │         ┌──────────┐
        └─────────│ failover │  (after success → new master ok)
                  └──────────┘
```

| State | Meaning |
|-------|---------|
| `"ok"` | Master is responsive |
| `"sdown"` | Subjectively down (this sentinel can't reach it) |
| `"odown"` | Objectively down (quorum of sentinels agree) |
| `"failover"` | Failover in progress |

### Sentinel Slave State

- **Default:** `"online"`
- Only `"online"` slaves are eligible for promotion (`selectNewMaster`)

---

## 5. Cluster Slot States

**Package:** `internal/cluster/cluster.go`

| State | Meaning |
|-------|---------|
| `"stable"` | Slot is fully on this node |
| `"migrating"` | Slot is being moved to another node |
| `"importing"` | Slot is being received from another node |

---

## 6. State Interaction Legality

### 6.1 Server lifecycle vs connection state

| Server State | New Connections | Normal | Subscribed | Monitor | Transaction | ReplSlave |
|-------------|----------------|--------|------------|---------|-------------|-----------|
| START | ❌ | — | — | — | — | — |
| RUNNING | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| SHUTDOWN | ❌ (listener closed) | exiting | exiting | exiting | DISCARD | EOF |

### 6.2 Connection state combinations (per single connection)

| Normal | Subscribed | Monitor | Transaction | ReplSlave | Legal? |
|--------|------------|---------|-------------|-----------|--------|
| ✅ | ❌ | ❌ | ❌ | ❌ | Default |
| ❌ | ✅ | ❌ | ❌ | ❌ | Exclusive |
| ❌ | ❌ | ✅ | ❌ | ❌ | Exclusive |
| ✅ | ❌ | ❌ | ✅ | ❌ | Transaction stays Normal |
| ❌ | ❌ | ❌ | ❌ | ✅ | Exclusive (conn handle passed) |

**Invariant:** A connection is in exactly one of: Normal, Subscribed, Monitor, or ReplSlave. Transaction can overlap with Normal.

### 6.3 Server role vs connection state

| Role | Master | Slave |
|------|--------|-------|
| Accepts Subscribed | ✅ | ❌ (slave doesn't accept client connections) |
| Accepts Monitor | ✅ | ❌ |
| Accepts PSYNC | ✅ (creates ReplSlave) | ❌ |
| Can run Transaction | ✅ | ❌ |
| Propagates writes | ✅ (to slaves) | ❌ |

### 6.4 Command legality per connection state

```
+----------------+----------+------------+---------+-----------+-----------+
| Command Group  | Normal   | Subscribed | Monitor | Transactn | ReplSlave |
+----------------+----------+------------+---------+-----------+-----------+
| Read           | ✅       | ❌         | ❌      | QUEUED    | ❌        |
| Write          | ✅       | ❌         | ❌      | QUEUED    | ❌        |
| SUBSCRIBE      | →Subscr  | ✅         | ❌      | ❌        | ❌        |
| PSUBSCRIBE     | →Subscr  | ✅         | ❌      | ❌        | ❌        |
| UNSUBSCRIBE    | ✅(no-op)| ✅         | ❌      | ❌        | ❌        |
| PUNSUBSCRIBE   | ✅(no-op)| ✅         | ❌      | ❌        | ❌        |
| MONITOR        | →Monitor | ❌         | ❌      | ❌        | ❌        |
| PING           | ✅       | ✅         | ✅      | ✅(not Q) | ❌        |
| QUIT           | →Closed  | →Closed    | →Closed | →Closed   | ❌        |
| MULTI          | →Txn     | ❌         | ❌      | ❌        | ❌        |
| EXEC           | ❌       | ❌         | ❌      | execute   | ❌        |
| DISCARD        | ❌       | ❌         | ❌      | →Normal   | ❌        |
| WATCH          | ✅       | ❌         | ❌      | ✅        | ❌        |
| PSYNC          | →ReplSb  | ❌         | ❌      | ❌        | ❌        |
| REPLCONF ACK   | ❌       | ❌         | ❌      | ❌        | ✅        |
+----------------+----------+------------+---------+-----------+-----------+
```

QUEUED = command is queued by transaction, not executed immediately.
(not Q) = PING bypasses transaction queue.

---

## 7. Degradation State Machine

The soak harness tracks system health via `PressureMonitor`:

```
         ┌──────┐
         │  OK  │─────────────┐
         └──┬───┘             │
            │ soft threshold  │ hard threshold exceeded
            │ exceeded        │ (goroutine/L0/retries)
            v                 v
         ┌──────┐         ┌──────┐
         │ WARN │         │ FAIL │
         └──┬───┘         └──────┘
            │ multiple WARNs or
            │ L0 not recovering
            v
         ┌──────────┐
         │ DEGRADED │
         └──────────┘
```

See `DegradationAssertion` in `cmd/integration/pressure_monitor.go` for configurable thresholds.

---

## 8. Fuzz-Tested Transitions

The following transition classes are exercised by fuzz tests (`TestFuzzServerStateMachineChaos`):

| Chaos Class | Transitions Exercised |
|-------------|----------------------|
| PubSub | Normal → Subscribed → Normal (QUIT/UNSUBSCRIBE), parallel SUBSCRIBE |
| Monitor | Normal → Monitor → Normal (QUIT) |
| Transaction | Normal → Transaction → Normal (EXEC/DISCARD), WATCH/MULTI/EXEC |
| Client Kill | Normal → Closed (server-initiated), reconnect cycle |
| Connect/Disconnect | Normal → Closed → Normal (dial storm) |
| Blocking Ops | BLPOP with timeout → returns to Normal |
| Replication Slave | SlaveDisconnected ↔ SlaveConnecting ↔ SlaveSyncing ↔ SlaveConnected |
| Replication Lifecycle | CLIENT KILL TYPE slave → slave reconnects (Disconnected → ... → Connected) |
| Sentinel | ok → sdown → ok, ok → sdown → odown → failover |
| Degradation | OK → WARN → DEGRADED (backpressure), OK → FAIL (hard gate) |

## 9. Unformalized / Future

These state interactions are not yet formalized as invariants but are exercised in fuzz:

- **LOADING state:** RDB loading during full sync. Currently only the replication layer knows about it; there is no server-level LOADING state that blocks client commands.
- **ACL/auth state:** No authentication layer yet. When added, an `AUTH` state must gate all non-AUTH commands.
- **Read-only / degraded mode:** The server has no readonly mode for sentinel-replicated failover. After failover, the old master should reject writes.
- **Output buffer limit:** `OutputBufferLimit` forcibly closes connections; this is a `Normal → Closed` edge triggered by the server, not the client — not yet fuzz-tested explicitly.

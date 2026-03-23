# MONITOR Command Design

## Overview

Implement Redis-compatible `MONITOR` command that streams all executed commands to connected monitor clients in real-time.

## Output Format

Redis-compatible format:
```
1732222222.123456 [0 127.0.0.1:6337] "COMMAND" "arg1" "arg2"
```

- Unix timestamp with microseconds
- Client ID and address
- Full command with args as RESP strings

## Architecture

### Components

1. **CommandMonitor** (`internal/server/monitor.go`)
   - Manages set of monitor clients
   - `Add(client)` — register new monitor client
   - `Remove(client)` — unregister client
   - `Broadcast(cmd)` — send command to all monitors

2. **MonitorClient**
   - Per-client state: client connection, write channel
   - Channel buffered at 100 messages
   - Goroutine reads channel and writes to TCP

3. **MONITOR command handler**
   - Registers client in monitor set
   - Blocks until client disconnects (then auto-removes)

4. **Command interception**
   - Hook into command execution in handler.go
   - Call `CommandMonitor.Broadcast()` after each command

### Performance Strategy

1. **Async writes** — command execution never blocks on monitor writes
2. **Go channel per client** — buffered 100 messages, decouples execution from delivery
3. **Drop old messages** — if channel full, drop oldest (client is too slow)
4. **Disconnect slow clients** — if channel full for >1 second, close connection
5. **Fire-and-forget** — write to TCP without waiting for ACK

### Data Flow

```
Command Executed → CommandMonitor.Broadcast(cmd)
                 → for each MonitorClient:
                     select {
                       case client.ch <- formattedCmd: // sent
                       default: // channel full, drop message
                     }
                 → client goroutine: read ch → write TCP
```

## Implementation Details

### New File: internal/server/monitor.go

```go
type CommandMonitor struct {
    mu      sync.RWMutex
    clients map[*MonitorClient]struct{}
}

type MonitorClient struct {
    conn   net.Conn
    ch     chan string
    quit   chan struct{}
}

func NewCommandMonitor() *CommandMonitor
func (m *CommandMonitor) Add(client *MonitorClient)
func (m *CommandMonitor) Remove(client *MonitorClient)
func (m *CommandMonitor) Broadcast(format string, args ...interface{})
```

### Handler Integration

In `handler.go` command execution switch:

```go
case "MONITOR":
    return h.handleMonitor()
```

After each command in the main execution path (before return):

```go
// Broadcast to monitors
if h.monitor != nil {
    h.monitor.Broadcast(...)
}
```

### Handler Addition (internal/server/handler.go)

```go
func (h *Handler) handleMonitor() proto.Response {
    client := &MonitorClient{
        conn: h.conn,
        ch:   make(chan string, 100),
        quit: make(chan struct{}),
    }
    h.monitor.Add(client)

    // Start writer goroutine
    go client.writeLoop()

    // Block until client disconnects
    <-client.quit
    h.monitor.Remove(client)
    return nil // client disconnected
}
```

### MonitorClient writeLoop

```go
func (c *MonitorClient) writeLoop() {
    for {
        select {
        case msg := <-c.ch:
            c.conn.Write([]byte(msg))
        case <-c.quit:
            close(c.ch)
            return
        }
    }
}
```

## Files to Modify

| File | Change |
|------|--------|
| `internal/server/monitor.go` | New file |
| `internal/server/handler.go` | Add MONITOR case, add monitor field, call Broadcast |
| `internal/server/handler_test.go` | Add MONITOR tests |

## Testing

1. **Unit test**: `TestMonitor` — start server, connect monitor client, send commands, verify output format
2. **Integration test**: Full protocol test with go-redis client

## Error Handling

- If monitor channel full > 1s: close client connection, remove from monitor set
- If write fails: remove client from monitor set
- If handler.go already has `h.monitor`: check for nil before broadcast

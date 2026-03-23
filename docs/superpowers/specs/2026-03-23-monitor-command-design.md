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
   - Per-client state: client connection, write channel, remoteAddr, clientID
   - Channel buffered at 100 messages
   - Goroutine reads channel and writes to TCP

3. **MONITOR command handler**
   - Registers client in monitor set
   - Returns OK, then blocks until client disconnects (then auto-removes)

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
                 → for each MonitorClient (with lock):
                     select {
                       case client.ch <- formattedCmd: // sent
                       default: // channel full, drop message
                     }
                 → client goroutine: read ch → write TCP → on error close quit
```

## Implementation Details

### New File: internal/server/monitor.go

```go
type CommandMonitor struct {
    mu      sync.RWMutex
    clients map[*MonitorClient]struct{}
}

type MonitorClient struct {
    conn     net.Conn
    ch       chan string
    quit     chan struct{}
    remoteAddr string
    clientID int64
}

func NewCommandMonitor() *CommandMonitor
func (m *CommandMonitor) Add(client *MonitorClient)
func (m *CommandMonitor) Remove(client *MonitorClient)
func (m *CommandMonitor) Broadcast(format string, args ...interface{})
```

### Handler Changes (internal/server/handler.go)

#### 1. Add monitor field to Handler struct

```go
type Handler struct {
    // ... existing fields ...
    monitor *CommandMonitor
}
```

#### 2. Initialize monitor in newHandler

```go
h.monitor = NewCommandMonitor()
```

#### 3. Add MONITOR case in command switch

```go
case "MONITOR":
    return h.handleMonitor(conn, remoteAddr)
```

#### 4. handleMonitor implementation

```go
func (h *Handler) handleMonitor(conn net.Conn, remoteAddr string) proto.Response {
    clientID := int64(0)
    if h.clientInfo != nil {
        clientID = h.clientInfo.ID
    }

    client := &MonitorClient{
        conn:      conn,
        ch:        make(chan string, 100),
        quit:      make(chan struct{}),
        remoteAddr: remoteAddr,
        clientID:  clientID,
    }
    h.monitor.Add(client)

    // Start writer goroutine
    go client.writeLoop(h.monitor)

    // Return OK then block until client disconnects
    go func() {
        <-client.quit
        h.monitor.Remove(client)
    }()

    return proto.OK
}
```

#### 5. MonitorClient writeLoop with error handling

```go
func (c *MonitorClient) writeLoop(monitor *CommandMonitor) {
    for {
        select {
        case msg := <-c.ch:
            if _, err := c.conn.Write([]byte(msg)); err != nil {
                close(c.quit)
                return
            }
        case <-c.quit:
            close(c.ch)
            return
        }
    }
}
```

#### 6. Broadcast implementation (with lock protection)

```go
func (m *CommandMonitor) Broadcast(format string, args ...interface{}) {
    msg := fmt.Sprintf(format, args...)

    m.mu.RLock()
    for client := range m.clients {
        select {
        case client.ch <- msg:
        default:
            // Channel full, drop message (client is slow)
        }
    }
    m.mu.RUnlock()
}
```

### Slow Client Disconnect (1-second timeout)

Add to writeLoop:

```go
func (c *MonitorClient) writeLoop(monitor *CommandMonitor) {
    for {
        select {
        case msg := <-c.ch:
            if _, err := c.conn.Write([]byte(msg)); err != nil {
                close(c.quit)
                return
            }
        case <-time.After(1 * time.Second):
            // Check if channel is still full (slow client)
            if len(c.ch) == cap(c.ch) {
                close(c.quit)
                return
            }
        case <-c.quit:
            close(c.ch)
            return
        }
    }
}
```

Note: The 1-second check via `time.After` creates a timer that resets each time a message is successfully sent. This is acceptable for the slow-client detection use case.

### Command Broadcast Integration

After each command in the main execution path, before returning response:

```go
// Broadcast to monitors (after command executes, before returning)
if h.monitor != nil {
    // Format: timestamp [clientID addr] "COMMAND" "arg1" "arg2"
    ts := time.Now().UnixMicro()
    clientID := int64(0)
    addr := "127.0.0.1:6337"
    if h.clientInfo != nil {
        clientID = h.clientInfo.ID
        if h.clientInfo.Addr != "" {
            addr = h.clientInfo.Addr
        }
    }
    h.monitor.Broadcast("%d [%d %s] %s", ts, clientID, addr, cmd)
}
```

### RemoteAddr Handling

The `remoteAddr` is already passed to `processRequest()` in handler.go. Pass it through to `handleMonitor()`:

```go
// In processRequest, where handleMonitor is called:
case "MONITOR":
    return h.handleMonitor(conn, remoteAddr)
```

## Race Condition Prevention

1. **Broadcast** uses `m.mu.RLock()` to safely iterate clients
2. **Remove** uses `m.mu.Lock()` to safely delete from map
3. **writeLoop checks quit channel** before/after channel receive to avoid send on closed channel
4. **Close quit only once** - in error case or when Remove is called

## Files to Modify

| File | Change |
|------|--------|
| `internal/server/monitor.go` | New file with CommandMonitor and MonitorClient |
| `internal/server/handler.go` | Add `monitor` field, initialize it, add MONITOR case, add broadcast calls |
| `internal/server/handler_test.go` | Add MONITOR tests |

## Testing

1. **Unit test**: `TestMonitor` — connect monitor client, send commands, verify output format
2. **Integration test**: Full protocol test with go-redis client

## Error Handling

- If write fails: close quit channel (writeLoop exits, client removed)
- If channel full > 1s: close quit channel (disconnect slow client)
- If handler.go already has `h.monitor`: check for nil before broadcast
- On server shutdown: close all monitor connections gracefully

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
   - `Remove(client)` — unregister client and close connection
   - `Broadcast(cmd)` — send command to all monitors

2. **MonitorClient**
   - Per-client state: client connection, write channel, remoteAddr, clientID
   - Channel buffered at 100 messages
   - Goroutine reads channel and writes to TCP

3. **MONITOR command handler**
   - Registers client in monitor set
   - Returns proto.OK, then blocks until client disconnects
   - MONITOR hijacks the connection — no normal response writing

4. **Command interception**
   - Hook into command execution in handler.go
   - Call `CommandMonitor.Broadcast()` after each command

### Performance Strategy

1. **Async writes** — command execution never blocks on monitor writes
2. **Go channel per client** — buffered 100 messages, decouples execution from delivery
3. **Drop old messages** — if channel full, drop oldest (client is too slow)
4. **Disconnect slow clients** — if channel full for >1 second, close connection
5. **Fire-and-forget** — write to TCP without waiting for ACK

### Connection Hijacking

MONITOR hijacks the TCP connection. After MONITOR executes:
- The connection is removed from normal command-response handling
- All writes go through the monitor writeLoop directly to the TCP connection
- The main loop stops processing commands on this connection

Implementation: Return a special response type `proto.NewMonitorResponse()` that signals to `processRequest` to break out of the main loop without writing anything.

## Implementation Details

### New File: internal/server/monitor.go

```go
type CommandMonitor struct {
    mu      sync.RWMutex
    clients map[*MonitorClient]struct{}
}

type MonitorClient struct {
    conn       net.Conn
    ch         chan string
    quit       chan struct{}
    remoteAddr string
    clientID   int64
}

func NewCommandMonitor() *CommandMonitor

func (m *CommandMonitor) Add(client *MonitorClient)

func (m *CommandMonitor) Remove(client *MonitorClient) {
    m.mu.Lock()
    delete(m.clients, client)
    m.mu.Unlock()
    // Only close if not already closed (quit may be open or already closed by writeLoop)
    select {
    case <-client.quit:
    default:
        close(client.quit)
    }
    client.conn.Close()
}

func (m *CommandMonitor) Broadcast(format string, args ...interface{})
```

### MonitorClient writeLoop

```go
func (c *MonitorClient) writeLoop(monitor *CommandMonitor) {
    timer := time.NewTimer(1 * time.Second)
    defer timer.Stop()

    for {
        // Reset timer at start of each iteration when channel has messages
        if len(c.ch) > 0 {
            if !timer.Stop() {
                select {
                case <-timer.C:
                default:
                }
            }
            timer.Reset(1 * time.Second)
        }

        select {
        case msg := <-c.ch:
            if _, err := c.conn.Write([]byte(msg)); err != nil {
                monitor.Remove(c)
                return
            }
        case <-timer.C:
            // Timer fired - check if channel is still full
            if len(c.ch) == cap(c.ch) {
                monitor.Remove(c)
                return
            }
        case <-c.quit:
            // Drain remaining messages before exiting
            for {
                select {
                case msg := <-c.ch:
                    c.conn.Write([]byte(msg))
                default:
                    close(c.ch)
                    return
                }
            }
        }
    }
}
```

Note: `writeLoop` calls `Remove()` when it exits due to slow client or write error. `Remove()` closes `quit` and `conn` only if not already closed. The `c.ch` close happens in the quit case after draining.

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

#### 3. Add proto.MonitorResponse type (internal/proto/resp.go)

```go
type MonitorResponse struct{}

func NewMonitorResponse() *MonitorResponse
func (r *MonitorResponse) Encode() []byte { return nil }  // Nothing to write
```

#### 4. Add MONITOR case in command switch

```go
case "MONITOR":
    return h.handleMonitor(conn, remoteAddr)
```

#### 5. handleMonitor implementation

```go
func (h *Handler) handleMonitor(conn net.Conn, remoteAddr string) proto.Response {
    clientID := int64(0)
    addr := remoteAddr
    if h.clientInfo != nil {
        clientID = h.clientInfo.ID
    }

    client := &MonitorClient{
        conn:       conn,
        ch:         make(chan string, 100),
        quit:       make(chan struct{}),
        remoteAddr: addr,
        clientID:   clientID,
    }
    h.monitor.Add(client)

    // Start writer goroutine
    go client.writeLoop(h.monitor)

    // Return special response to signal connection hijack
    return proto.NewMonitorResponse()
}
```

#### 6. Broadcast implementation

```go
func (m *CommandMonitor) Broadcast(format string, args ...interface{}) {
    msg := fmt.Sprintf(format, args...)

    m.mu.RLock()
    defer m.mu.RUnlock()
    for client := range m.clients {
        select {
        case client.ch <- msg:
        default:
            // Channel full, drop message (client is slow)
        }
    }
}
```

### processRequest changes

In the main loop, after executeCommand returns:

```go
resp := h.executeCommand(cmd, args[1:], remoteAddr)

// Check for MONITOR hijack
if _, ok := resp.(*proto.MonitorResponse); ok {
    // Don't write response - connection is hijacked
    // Just return and stop processing this connection
    return
}

if err := writeResponse(conn, resp); err != nil {
    return
}
```

### Command Broadcast Integration

After each command in the main execution path:

```go
// Broadcast to monitors (after command executes)
if h.monitor != nil {
    ts := time.Now().UnixMicro()
    clientID := int64(0)
    addr := remoteAddr
    if h.clientInfo != nil {
        clientID = h.clientInfo.ID
    }
    h.monitor.Broadcast("%d [%d %s] %s", ts, clientID, addr, cmd)
}
```

Note: Broadcast uses `m.mu.RLock()` which allows concurrent reads but blocks Remove() from modifying the map. This is safe.

## Race Condition Prevention

1. **Quit channel ownership**: Only `Remove()` closes `quit`. writeLoop reads from it but never closes.
2. **Broadcast lock**: `RLock` for reading clients, `Lock` for removing
3. **writeLoop exit**: Returns when slow client detected; Remove() called by caller goroutine
4. **Timer management**: Use `time.Timer` with `Reset` to avoid accumulating timers

## Files to Modify

| File | Change |
|------|--------|
| `internal/server/monitor.go` | New file with CommandMonitor and MonitorClient |
| `internal/proto/resp.go` | Add MonitorResponse type |
| `internal/server/handler.go` | Add `monitor` field, MONITOR case, processRequest changes |
| `internal/server/handler_test.go` | Add MONITOR tests |

## Testing

1. **Unit test**: `TestMonitor` — connect monitor client, send commands, verify output format
2. **Integration test**: Full protocol test with go-redis client

## Error Handling

- If write fails: writeLoop returns, Remove() called (by whoever detects), connection closed
- If channel full > 1s: writeLoop returns, Remove() closes connection
- If handler exits: all monitor clients closed via server shutdown path
- On server shutdown: iterate and Remove all clients

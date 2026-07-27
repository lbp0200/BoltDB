# Backpressure (Write Throttling)

BoltDB implements write throttling to prevent L0 compaction from spiraling under heavy write load.

## Mechanism

- `retryUpdate` checks L0 score before each write, using `BotreonStore.preWriteCheck()`
- Soft threshold (default 8.0): delay proportional to score, max 1s
- Hard threshold (default 20.0): reject writes with `ErrWriteRejected`
- Global semaphore (`writeSlot`) limits concurrent `retryUpdate` goroutines (default 50)

## Threshold Interpretation

| L0 Score | Behavior |
|----------|----------|
| 1–5 | Normal |
| 5–10 | Mild pre-delay |
| 10–20 | Significant delay |
| >20 | Writes rejected |

## Metrics

Exported via `GetRetryMetrics()`:
- `ActiveRetries` — concurrent retry goroutines
- `TotalRetries` — cumulative retry count
- `L0Rejected` — writes rejected by hard threshold
- `L0Delayed` — writes delayed by soft threshold

## Configuration

```go
import "github.com/lbp0200/BoltDB/internal/store"
store.SetBackpressureConfig(store.BackpressureConfig{...})
```

## Related

- `internal/store/backpressure.go` — implementation
- `docs/failures/l0-collapse.md` — failure mode analysis
- `docs/monitoring.md` — Prometheus metrics for backpressure events

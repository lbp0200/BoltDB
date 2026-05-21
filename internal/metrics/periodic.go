package metrics

import (
	"context"
	"runtime"
	"strings"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

func StartPeriodicSnapshot(ctx context.Context, c *Collector, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		select {
		case <-ctx.Done():
			return
		default:
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				var memStats runtime.MemStats
				runtime.ReadMemStats(&memStats)

				s := c.Snapshot()

				logger.Logger.Info().
					Int("goroutines", s.Goroutines).
					Float64("l0_score", s.L0Score).
					Int64("active_retries", s.ActiveRetries).
					Int64("total_retries", s.TotalRetries).
					Int64("writes_blocked", s.WritesBlocked).
					Int64("l0_rejected", s.L0Rejected).
					Int64("reconnects", s.ReconnectCount).
					Int64("replication_lag", s.ReplicationLag).
					Int("active_clients", s.ActiveClients).
					Int("blocked_clients", s.BlockedClients).
					Int("pubsub_clients", s.PubSubClients).
					Int("slave_count", s.SlaveCount).
					Uint64("alloc_mb", memStats.Alloc/1024/1024).
					Uint64("heap_inuse_mb", memStats.HeapInuse/1024/1024).
					Uint64("stack_inuse_mb", memStats.StackInuse/1024/1024).
					Uint32("num_gc", memStats.NumGC).
					Msg("periodic runtime snapshot")
			}
		}
	}()
}

func WriteGoroutineStack() string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.TrimSpace(string(buf[:n]))
}

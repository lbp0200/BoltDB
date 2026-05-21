package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

func AttachHTTP(mux *http.ServeMux, c *Collector) {
	mux.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		s := c.Snapshot()
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(s)
	})

	mux.HandleFunc("/debug/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		s := c.Snapshot()
		_, _ = w.Write([]byte(s.String()))
	})
}

func ServeMetrics(ctx context.Context, addr string, c *Collector) error {
	mux := http.NewServeMux()
	AttachHTTP(mux, c)

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	server := &http.Server{
		Addr:        addr,
		Handler:     mux,
		ReadTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Logger.Warn().Err(err).Msg("metrics HTTP server shutdown error")
		}
	}()

	logger.Logger.Info().Str("addr", addr).Msg("metrics HTTP server starting")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("metrics HTTP server error: %w", err)
	}
	return nil
}

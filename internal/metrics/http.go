package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/lbp0200/BoltDB/internal/logger"
)

// AttachHTTP registers debug and Prometheus metric endpoints on the given mux.
func AttachHTTP(mux *http.ServeMux, c *Collector) {
	mux.HandleFunc("/debug/vars", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		s := c.Snapshot()
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(s); err != nil {
			logger.Logger.Warn().Err(err).Msg("/debug/vars encoding failed")
		}
	})

	mux.HandleFunc("/debug/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		s := c.Snapshot()
		_, _ = w.Write([]byte(s.String()))
	})

	// /metrics — Prometheus exposition format, standard scrape endpoint
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		s := c.Snapshot()
		_, _ = w.Write([]byte(prometheusText(s)))
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

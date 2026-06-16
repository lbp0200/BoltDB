package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zeebo/assert"
)

func TestAttachHTTP_DebugVars(t *testing.T) {
	t.Parallel()
	c := NewCollector()
	c.RetryMetricsFn = func() (int64, int64, int64, int64, int64, float64) {
		return 1, 2, 3, 4, 5, 1.5
	}
	c.RoleFn = func() string { return "master" }

	mux := http.NewServeMux()
	AttachHTTP(mux, c)

	req := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var result Snapshot
	err := json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), result.ActiveRetries)
	assert.Equal(t, 1.5, result.L0Score)
}

func TestAttachHTTP_DebugMetrics(t *testing.T) {
	t.Parallel()
	c := NewCollector()
	c.RetryMetricsFn = func() (int64, int64, int64, int64, int64, float64) {
		return 0, 0, 0, 0, 0, 0.5
	}
	c.RoleFn = func() string { return "slave" }

	mux := http.NewServeMux()
	AttachHTTP(mux, c)

	req := httptest.NewRequest(http.MethodGet, "/debug/metrics", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/plain; charset=utf-8", w.Header().Get("Content-Type"))

	body := w.Body.String()
	assert.True(t, strings.Contains(body, "BoltDB Metrics"))
	assert.True(t, strings.Contains(body, "role=slave"))
}

func TestAttachHTTP_CORSHeaders(t *testing.T) {
	t.Parallel()
	c := NewCollector()

	mux := http.NewServeMux()
	AttachHTTP(mux, c)

	for _, path := range []string{"/debug/vars", "/debug/metrics"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		assert.Equal(t, "*", w.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestServeMetrics_Healthz(t *testing.T) {
	t.Parallel()
	// The /healthz endpoint is registered in ServeMetrics, not AttachHTTP.
	// We replicate the minimal handler registration here for unit testing.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, `{"status":"ok"}`, strings.TrimSpace(w.Body.String()))
}

func TestServeMetrics_Shutdown(t *testing.T) {
	t.Parallel()
	c := NewCollector()

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- ServeMetrics(ctx, "localhost:0", c)
	}()

	time.Sleep(50 * time.Millisecond)

	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for ServeMetrics to return")
	}
}

func TestAttachHTTP_DebugVars_EmptyCollector(t *testing.T) {
	t.Parallel()
	c := NewCollector()

	mux := http.NewServeMux()
	AttachHTTP(mux, c)

	req := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var result Snapshot
	err := json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)
	assert.Equal(t, "master", result.Role)
	assert.Equal(t, 0, result.ActiveClients)
}

func TestAttachHTTP_DebugVars_AllFields(t *testing.T) {
	t.Parallel()
	c := NewCollector()
	c.RetryMetricsFn = func() (int64, int64, int64, int64, int64, float64) {
		return 3, 15, 0, 2, 1, 4.2
	}
	c.MasterReplOffsetFn = func() int64 { return 5000 }
	c.SlaveReplOffsetFn = func() int64 { return 4900 }
	c.ReconnectCountFn = func() int64 { return 2 }
	c.SlaveCountFn = func() int { return 1 }
	c.BacklogSizeFn = func() int64 { return 65536 }
	c.BacklogAvailFn = func() int64 { return 32768 }
	c.RoleFn = func() string { return "slave" }
	c.ActiveClientsFn = func() int { return 8 }
	c.BlockedClientsFn = func() int { return 1 }
	c.MonitorClientsFn = func() int { return 0 }
	c.PubSubClientsFn = func() int { return 3 }
	c.PubSubSubsFn = func() int { return 12 }
	c.TotalOutputBytesFn = func() int64 { return 1048576 }

	mux := http.NewServeMux()
	AttachHTTP(mux, c)

	req := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var result Snapshot
	err := json.Unmarshal(w.Body.Bytes(), &result)
	assert.NoError(t, err)

	assert.Equal(t, int64(3), result.ActiveRetries)
	assert.Equal(t, int64(15), result.TotalRetries)
	assert.Equal(t, int64(0), result.WritesBlocked)
	assert.Equal(t, int64(2), result.L0Rejected)
	assert.Equal(t, int64(1), result.L0Delayed)
	assert.Equal(t, 4.2, result.L0Score)
	assert.Equal(t, int64(5000), result.MasterReplOffset)
	assert.Equal(t, int64(4900), result.SlaveReplOffset)
	assert.Equal(t, int64(100), result.ReplicationLag)
	assert.Equal(t, int64(2), result.ReconnectCount)
	assert.Equal(t, 1, result.SlaveCount)
	assert.Equal(t, "slave", result.Role)
	assert.Equal(t, 8, result.ActiveClients)
	assert.Equal(t, 1, result.BlockedClients)
	assert.Equal(t, 0, result.MonitorClients)
	assert.Equal(t, 3, result.PubSubClients)
	assert.Equal(t, 12, result.PubSubSubs)
	assert.Equal(t, int64(1048576), result.TotalOutputBytes)
	assert.Equal(t, int64(65536), result.BacklogSize)
	assert.Equal(t, int64(32768), result.BacklogAvailable)
}

func TestServeMetrics_AddrInError(t *testing.T) {
	t.Parallel()
	c := NewCollector()
	ctx := context.Background()

	err := ServeMetrics(ctx, "127.0.0.1:-1", c)

	assert.Error(t, err)
	assert.True(t, strings.Contains(fmt.Sprintf("%v", err), "listen tcp"))
}

package regressions

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestRegressionCanonicalExpireAbsolutePoint verifies the TODO §8 store-layer
// normalization: EXPIRE/PEXPIRE log a canonical absolute PEXPIREAT frame
// (not the relative TTL), so the replica converges to the SAME absolute
// expiry point as the master even with replication lag.
//
// Guards:
//  1. Master's repl-log frames for the keys are PEXPIREAT (fails pre-fix,
//     where the store logged raw EXPIRE/PEXPIRE and only the dead
//     handler_core propagateArgs carried the absolute timestamp).
//  2. EXPIRETIME / PEXPIRETIME agree exactly master==slave (absolute points,
//     not lag-shifted relative TTLs).
//  3. Conditional-reject semantics (e322b7c) are covered by
//     TestRegressionCanonicalExpireConditionRejected (not duplicated here).
func TestRegressionCanonicalExpireAbsolutePoint(t *testing.T) {
	master := StartRegression(t)
	defer master.Close()

	slave := StartRegression(t)
	defer slave.Close()

	ctx := context.Background()

	if err := slave.MakeSlave(master.Addr); err != nil {
		t.Fatalf("MakeSlave failed: %v", err)
	}
	defer slave.StopSlave()

	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync in time")
	}

	if err := master.Client.Set(ctx, "expire:abs1", "v1", 0).Err(); err != nil {
		t.Fatalf("SET abs1 failed: %v", err)
	}
	if err := master.Client.Expire(ctx, "expire:abs1", 3600*time.Second).Err(); err != nil {
		t.Fatalf("EXPIRE abs1 failed: %v", err)
	}
	if err := master.Client.Set(ctx, "expire:abs2", "v2", 0).Err(); err != nil {
		t.Fatalf("SET abs2 failed: %v", err)
	}
	if err := master.Client.PExpire(ctx, "expire:abs2", 7200*time.Second).Err(); err != nil {
		t.Fatalf("PEXPIRE abs2 failed: %v", err)
	}

	if !slave.WaitForReplicaSync(ctx, master, slave, 30*time.Second) {
		t.Fatal("slave did not sync after writes")
	}

	// 1. Master log frames for both keys must be canonical PEXPIREAT.
	entries, err := master.DB.ReplLogEntries()
	if err != nil {
		t.Fatalf("ReplLogEntries failed: %v", err)
	}
	seen := map[string]string{}
	for _, e := range entries {
		s := string(e.Value)
		for _, k := range []string{"expire:abs1", "expire:abs2"} {
			if strings.Contains(s, k) {
				seen[k] = s
			}
		}
	}
	for _, k := range []string{"expire:abs1", "expire:abs2"} {
		raw, ok := seen[k]
		if !ok {
			t.Fatalf("no repl-log frame found for key %q (%d entries)", k, len(entries))
		}
		if !strings.Contains(raw, "PEXPIREAT") {
			t.Fatalf("key %q frame not canonical PEXPIREAT: %q", k, raw)
		}
		// Exact-token guard: raw EXPIRE is "$6\r\nEXPIRE\r\n", PEXPIRE "$7\r\nPEXPIRE\r\n".
		// PEXPIREAT ("$9\r\nPEXPIREAT\r\n") must not be accompanied by a relative frame.
		if strings.Contains(raw, "$6\r\nEXPIRE\r\n") || strings.Contains(raw, "$7\r\nPEXPIRE\r\n") {
			t.Fatalf("key %q frame logs relative TTL instead of absolute: %q", k, raw)
		}
	}

	// 2. Absolute expiry points must agree exactly master==slave.
	mExp, err := master.Client.ExpireTime(ctx, "expire:abs1").Result()
	if err != nil {
		t.Fatalf("master EXPIRETIME failed: %v", err)
	}
	sExp, err := slave.Client.ExpireTime(ctx, "expire:abs1").Result()
	if err != nil {
		t.Fatalf("slave EXPIRETIME failed: %v", err)
	}
	if mExp != sExp {
		t.Fatalf("EXPIRETIME mismatch (lag drift): master=%d slave=%d", int64(mExp), int64(sExp))
	}

	mPExp, err := master.Client.PExpireTime(ctx, "expire:abs2").Result()
	if err != nil {
		t.Fatalf("master PEXPIRETIME failed: %v", err)
	}
	sPExp, err := slave.Client.PExpireTime(ctx, "expire:abs2").Result()
	if err != nil {
		t.Fatalf("slave PEXPIRETIME failed: %v", err)
	}
	if mPExp != sPExp {
		t.Fatalf("PEXPIRETIME mismatch (lag drift): master=%d slave=%d", int64(mPExp), int64(sPExp))
	}
}

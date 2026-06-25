package main

import (
	"flag"
	"testing"
)

// TestSkipStartupCleanupFlag tests that the -skip-startup-cleanup flag is defined
func TestSkipStartupCleanupFlag(t *testing.T) {
	t.Parallel()
	// This test verifies the flag exists
	// It will fail if the flag is not defined in main
	f := flag.Lookup("skip-startup-cleanup")
	if f == nil {
		t.Error("expected -skip-startup-cleanup flag to be defined, but it was not found")
	}
	// Verify the flag has the correct default value
	if f.DefValue != "false" {
		t.Errorf("expected default value 'false', got '%s'", f.DefValue)
	}
}

// TestSkipStartupCleanupFlagUsage tests that the flag has proper usage text
func TestSkipStartupCleanupFlagUsage(t *testing.T) {
	t.Parallel()
	f := flag.Lookup("skip-startup-cleanup")
	if f == nil {
		t.Skip("flag not defined, skipping")
	}
	if f.Usage == "" {
		t.Error("expected flag usage to be defined")
	}
}

// TestReplBacklogSizeFlag tests that the -repl-backlog-size flag is defined
func TestReplBacklogSizeFlag(t *testing.T) {
	t.Parallel()
	f := flag.Lookup("repl-backlog-size")
	if f == nil {
		t.Error("expected -repl-backlog-size flag to be defined, but it was not found")
	}
	if f.DefValue != "" {
		t.Errorf("expected default value '', got '%s'", f.DefValue)
	}
}

// TestClientOutputBufferLimitFlag tests that the -client-output-buffer-limit flag is defined
func TestClientOutputBufferLimitFlag(t *testing.T) {
	t.Parallel()
	f := flag.Lookup("client-output-buffer-limit")
	if f == nil {
		t.Error("expected -client-output-buffer-limit flag to be defined, but it was not found")
	}
	if f.DefValue != "0" {
		t.Errorf("expected default value '0', got '%s'", f.DefValue)
	}
}

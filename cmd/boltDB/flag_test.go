package main

import (
	"flag"
	"testing"
)

// TestSkipStartupCleanupFlag tests that the -skip-startup-cleanup flag is defined
func TestSkipStartupCleanupFlag(t *testing.T) {
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
	f := flag.Lookup("skip-startup-cleanup")
	if f == nil {
		t.Skip("flag not defined, skipping")
	}
	if f.Usage == "" {
		t.Error("expected flag usage to be defined")
	}
}

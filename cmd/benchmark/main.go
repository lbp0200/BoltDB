package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	dbPath := flag.String("dir", "/tmp/bolt_bench", "badger dir")
	logLevel := flag.String("log-level", "ERROR", "log level")
	clients := flag.Int("c", 50, "number of concurrent clients")
	requests := flag.Int("n", 100000, "total number of requests")
	dataSize := flag.Int("d", 100, "data size in bytes")
	port := flag.String("port", "6388", "BoltDB server port")
	flag.Parse()

	fmt.Println("==============================================")
	fmt.Println("BoltDB Benchmark Results")
	fmt.Println("==============================================")

	boltBinary := "./build/boltDB"
	if _, err := os.Stat(boltBinary); err != nil {
		fmt.Println("Building BoltDB server...")
		cmd := exec.Command("go", "build", "-o", boltBinary, "./cmd/boltDB/")
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Printf("Failed to build BoltDB: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println("Starting BoltDB server...")
	boltCmd := exec.Command(boltBinary,
		"-addr", ":"+*port,
		"-dir", *dbPath,
		"-log-level", *logLevel,
	)

	var boltStdout, boltStderr bytes.Buffer
	boltCmd.Stdout = &boltStdout
	boltCmd.Stderr = &boltStderr

	if err := boltCmd.Start(); err != nil {
		fmt.Printf("Failed to start BoltDB: %v\n", err)
		os.Exit(1)
	}

	cleanup := func() {
		if boltCmd.Process != nil {
			_ = boltCmd.Process.Kill()
		}
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nInterrupt received, shutting down...")
		cleanup()
		os.Exit(1)
	}()

	time.Sleep(2 * time.Second)

	cmd := exec.Command("redis-cli", "-p", *port, "PING")
	if err := cmd.Run(); err != nil {
		fmt.Printf("Failed to connect to BoltDB on port %s: %v\n", *port, err)
		fmt.Printf("BoltDB stdout: %s\n", boltStdout.String())
		fmt.Printf("BoltDB stderr: %s\n", boltStderr.String())
		cleanup()
		os.Exit(1)
	}

	fmt.Printf("Server: BoltDB 127.0.0.1:%s\n", *port)
	fmt.Printf("Clients: %d | Data Size: %d bytes | Requests: %d\n", *clients, *dataSize, *requests)
	fmt.Println("==============================================")
	fmt.Println()

	testCommands := []string{"PING", "SET", "GET"}
	var totalRequests int64
	var totalTime time.Duration

	for _, testCmd := range testCommands {
		fmt.Printf("Testing %s...\n", testCmd)

		benchmarkCmd := exec.Command("redis-benchmark",
			"-h", "127.0.0.1",
			"-p", *port,
			"-t", testCmd,
			"-c", strconv.Itoa(*clients),
			"-d", strconv.Itoa(*dataSize),
			"-n", strconv.Itoa(*requests),
		)

		var benchStdout, benchStderr bytes.Buffer
		benchmarkCmd.Stdout = &benchStdout
		benchmarkCmd.Stderr = &benchStderr

		cmdStart := time.Now()
		if err := benchmarkCmd.Run(); err != nil {
			fmt.Printf("  %s test failed: %v\n", testCmd, err)
			fmt.Printf("  redis-benchmark stderr: %s\n", benchStderr.String())
			continue
		}
		cmdTime := time.Since(cmdStart)
		totalTime += cmdTime

		output := benchStdout.String()
		for _, line := range strings.Split(output, "\n") {
			if strings.Contains(line, "requests per second") {
				parts := strings.Fields(line)
				for i, part := range parts {
					if part == "requests" && i > 0 {
						if rps, err := strconv.ParseFloat(parts[i-1], 64); err == nil {
							fmt.Printf("  %s: %.2f requests/sec\n", testCmd, rps)
						}
						break
					}
				}
			}
			if strings.Contains(line, "completed") {
				parts := strings.Fields(line)
				for _, part := range parts {
					if n, err := strconv.ParseInt(part, 10, 64); err == nil {
						totalRequests += n
					}
				}
			}
		}
		fmt.Println()
	}

	fmt.Println("==============================================")
	fmt.Println("Benchmark Summary:")
	fmt.Println("----------------------------------------------")

	if totalRequests > 0 && totalTime > 0 {
		opsPerSec := float64(totalRequests) / totalTime.Seconds()
		fmt.Printf("Total requests: %d\n", totalRequests)
		fmt.Printf("Total time: %v\n", totalTime)
		fmt.Printf("Overall throughput: %.2f ops/sec\n", opsPerSec)
	}

	fmt.Println("\nShutting down BoltDB...")
	_ = exec.Command("redis-cli", "-p", *port, "SHUTDOWN").Run()

	shutdownCh := make(chan struct{})
	go func() {
		_, _ = boltCmd.Process.Wait()
		close(shutdownCh)
	}()
	select {
	case <-shutdownCh:
	case <-time.After(5 * time.Second):
		fmt.Println("Server did not shut down gracefully, killing...")
		_ = boltCmd.Process.Kill()
	}

	fmt.Println("\n==============================================")
	fmt.Println("Benchmark completed successfully!")
	fmt.Println("==============================================")

	if runtime.GOOS == "darwin" {
		fmt.Println("\nTip: Close 'Terminal' or 'iTerm' to avoid lingering processes.")
	}
}

package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"

	"github.com/lbp0200/BoltDB/internal/proto"
)

// 简单的调试客户端，用于测试服务器响应
func main() {
	addr := flag.String("addr", "127.0.0.1:6337", "BoltDB server address")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("Usage: go run debug_client.go [-addr host:port] <command> [args...]")
		fmt.Println("Example: go run debug_client.go -addr 127.0.0.1:6337 CONFIG GET *")
		os.Exit(1)
	}

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Printf("Error connecting: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	// 构建命令
	cmdArgs := make([][]byte, len(args))
	for i, arg := range args {
		cmdArgs[i] = []byte(arg)
	}
	req := &proto.Array{Args: cmdArgs}

	// 发送命令
	fmt.Printf("Sending: %s\n", req.String())
	if err := proto.WriteRESP(conn, req); err != nil {
		fmt.Printf("Error writing: %v\n", err)
		os.Exit(1)
	}

	// 读取响应
	reader := bufio.NewReader(conn)
	resp, err := proto.ReadRESP(reader)
	if err != nil {
		fmt.Printf("Error reading: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Response: %s\n", resp.String())
}

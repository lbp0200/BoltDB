//go:build ignore
// +build ignore

package main

import (
	"bufio"
	"fmt"
	"net"
	"time"

	"github.com/lbp0200/BoltDB/internal/proto"
)

// 模拟 redis-benchmark 的行为
func main() {
	// 连接服务器
	conn, err := net.DialTimeout("tcp", "127.0.0.1:6337", 5*time.Second)
	if err != nil {
		fmt.Printf("❌ 连接失败: %v\n", err)
		return
	}
	defer conn.Close()

	// 设置超时
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	// 发送 CONFIG GET *
	cmd := &proto.Array{
		Args: [][]byte{
			[]byte("CONFIG"),
			[]byte("GET"),
			[]byte("*"),
		},
	}

	fmt.Printf("发送命令: %s\n", cmd.String())
	if err := proto.WriteRESP(conn, cmd); err != nil {
		fmt.Printf("❌ 写入失败: %v\n", err)
		return
	}

	// 读取响应
	reader := bufio.NewReader(conn)
	resp, err := proto.ReadRESP(reader)
	if err != nil {
		fmt.Printf("❌ 读取失败: %v\n", err)
		return
	}

	fmt.Printf("✅ 响应成功: %s\n", resp.String())
	fmt.Printf("响应元素数量: %d\n", len(resp.Args))

	// 检查响应格式
	if len(resp.Args) > 0 {
		fmt.Printf("第一个元素: %s\n", string(resp.Args[0]))
	}

	// 尝试发送 PING（模拟 redis-benchmark 的后续行为）
	pingCmd := &proto.Array{
		Args: [][]byte{
			[]byte("PING"),
		},
	}

	fmt.Printf("\n发送 PING: %s\n", pingCmd.String())
	if err := proto.WriteRESP(conn, pingCmd); err != nil {
		fmt.Printf("❌ PING 写入失败: %v\n", err)
		return
	}

	// 读取 PING 响应
	resp2, err := proto.ReadRESP(reader)
	if err != nil {
		fmt.Printf("❌ PING 读取失败: %v\n", err)
		return
	}

	fmt.Printf("✅ PING 响应: %s\n", resp2.String())
}

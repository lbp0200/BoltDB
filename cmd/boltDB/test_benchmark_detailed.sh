#!/bin/bash

# 详细的测试脚本，模拟 redis-benchmark 的精确行为

SERVER_ADDR="127.0.0.1"
SERVER_PORT="6337"

echo "=== 测试 1: 使用 telnet 测试 CONFIG GET * ==="
{
    echo -e "*3\r\n\$6\r\nCONFIG\r\n\$3\r\nGET\r\n\$1\r\n*\r\n"
    sleep 0.5
} | telnet $SERVER_ADDR $SERVER_PORT 2>/dev/null | grep -v "^Escape"

echo ""
echo "=== 测试 2: 使用 nc 测试 CONFIG GET * (立即关闭) ==="
echo -e "*3\r\n\$6\r\nCONFIG\r\n\$3\r\nGET\r\n\$1\r\n*\r\n" | timeout 1 nc $SERVER_ADDR $SERVER_PORT

echo ""
echo "=== 测试 3: 使用 Python 脚本测试（最接近 redis-benchmark） ==="
python3 << 'EOF'
import socket
import sys

def test_config_get():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect(('127.0.0.1', 6337))
        
        # 发送 CONFIG GET *
        cmd = b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$1\r\n*\r\n"
        print(f"Sending: {cmd}")
        s.sendall(cmd)
        
        # 读取响应
        response = b""
        while True:
            data = s.recv(4096)
            if not data:
                break
            response += data
            if b"\r\n" in response:
                # 尝试解析响应
                lines = response.split(b"\r\n")
                if len(lines) > 0 and lines[0].startswith(b"*"):
                    # 数组响应
                    count = int(lines[0][1:])
                    print(f"Response is array with {count} elements")
                    print(f"Full response: {response[:200]}...")
                    break
        
        s.close()
        print("✅ Connection closed normally")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    test_config_get()
EOF

echo ""
echo "=== 测试完成 ==="


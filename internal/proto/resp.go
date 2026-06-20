// 简化 RESP，只支持 basics（Array/Bulk/Simple/Error/Integer）
package proto

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/lbp0200/BoltDB/internal/logger"
)

const MaxBulkLen = 512 * 1024 * 1024 // 512MB — Redis convention
const MaxArrayLen = 1024 * 1024      // 1M elements

type RESP interface {
	String() string
}

type Array struct {
	Args [][]byte
}

func (a *Array) String() string {
	return "*" + strconv.Itoa(len(a.Args)) + "\r\n" + joinBulkStrings(a.Args)
}

type BulkString []byte

func (b *BulkString) String() string {
	if b == nil {
		return "$-1\r\n"
	}
	if *b == nil {
		return "$-1\r\n"
	}
	return "$" + strconv.Itoa(len(*b)) + "\r\n" + string(*b) + "\r\n"
}

// NilArray represents a nil RESP array (*-1\r\n), used for EXEC watch failure
type NilArray struct{}

func (n NilArray) String() string { return "*-1\r\n" }

type SimpleString string

func (s SimpleString) String() string { return "+" + string(s) + "\r\n" }

type Error string

func (e Error) String() string { return "-" + string(e) + "\r\n" }

type Integer int64

func (i Integer) String() string { return ":" + strconv.FormatInt(int64(i), 10) + "\r\n" }

// NestedArray represents a RESP array that can contain other nested arrays
type NestedArray struct {
	Elems []RESP
}

func (n *NestedArray) String() string {
	var b strings.Builder
	b.WriteString("*")
	b.WriteString(strconv.Itoa(len(n.Elems)))
	b.WriteString("\r\n")
	for _, elem := range n.Elems {
		b.WriteString(elem.String())
	}
	return b.String()
}

func ReadRESP(r *bufio.Reader) (*Array, error) {
	line, err := readLine(r)
	if err != nil {
		logger.Logger.Debug().Err(err).Msg("ReadRESP readLine 失败")
		return nil, err
	}

	if len(line) == 0 {
		logger.Logger.Debug().Msg("ReadRESP 收到空行")
		return nil, fmt.Errorf("empty line")
	}

	logger.Logger.Debug().
		Str("line", string(line)).
		Str("type", string(line[0])).
		Msg("ReadRESP 读取到")

	switch line[0] {
	case '*': // Array
		if len(line) == 1 {
			return nil, fmt.Errorf("invalid array prefix")
		}
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid array length: %s", line[1:])
		}
		if n > MaxArrayLen {
			return nil, fmt.Errorf("array length too large: %d", n)
		}
		args := make([][]byte, n)
		for i := 0; i < n; i++ {
			line, err := readLine(r)
			if err != nil {
				return nil, err
			}
			if len(line) == 0 {
				return nil, fmt.Errorf("empty element in array")
			}
			switch line[0] {
			case '$':
				bulkLen, err := strconv.Atoi(string(line[1:]))
				if err != nil {
					return nil, err
				}
				if bulkLen < -1 {
					return nil, fmt.Errorf("invalid bulk string length: %s", line[1:])
				}
				if bulkLen == -1 {
					args[i] = nil
					continue
				}
				if bulkLen > MaxBulkLen {
					return nil, fmt.Errorf("bulk string length too large: %d", bulkLen)
				}
				data := make([]byte, bulkLen+2)
				_, err = io.ReadFull(r, data)
				if err != nil {
					return nil, err
				}
				args[i] = data[:bulkLen]
			case ':':
				args[i] = line[1:]
			case '+':
				args[i] = line[1:]
			default:
				return nil, fmt.Errorf("unexpected element type %q in array", line[0])
			}
		}
		return &Array{Args: args}, nil
	case '+': // Simple String (用于响应，如 PING 返回 PONG)
		// line 已经是 "+PONG" 格式，readLine 已经去掉了 \r\n
		// 所以 line[1:] 就是内容
		// 不需要再次调用 readLine，因为 readLine 已经读取了整行
		if len(line) < 2 {
			return nil, fmt.Errorf("invalid simple string format")
		}
		// 将简单字符串转换为单元素数组
		return &Array{Args: [][]byte{line[1:]}}, nil
	case '$': // Bulk String (单独发送，redis-benchmark 不使用)
		// 这不应该出现在命令中，但为了健壮性处理
		bulkLen, err := strconv.Atoi(string(line[1:]))
		if err != nil || bulkLen < -1 {
			return nil, fmt.Errorf("invalid bulk string length: %s", line[1:])
		}
		if bulkLen == -1 {
			return nil, fmt.Errorf("null bulk string not supported as command")
		}
		if bulkLen > MaxBulkLen {
			return nil, fmt.Errorf("bulk string length too large: %d", bulkLen)
		}
		data := make([]byte, bulkLen+2)
		_, err = io.ReadFull(r, data)
		if err != nil {
			return nil, err
		}
		return &Array{Args: [][]byte{data[:bulkLen]}}, nil
	default:
		// 内联命令格式（Inline Command）
		// Redis 支持内联命令，格式为: "PING\r\n" 或 "GET key\r\n"
		// 命令和参数用空格分隔，以 \r\n 结尾
		// readLine 已经去掉了 \r\n，所以 line 就是完整的命令
		return parseInlineCommand(line)
	}
}

// parseInlineCommand 解析内联命令
// 内联命令格式: "PING" 或 "GET key" 或 "SET key value"
// 参数用空格分隔
func parseInlineCommand(line []byte) (*Array, error) {
	// 将字节数组转换为字符串，然后按空格分割
	cmdStr := strings.TrimSpace(string(line))
	if cmdStr == "" {
		return nil, fmt.Errorf("empty inline command")
	}

	// 按空格分割命令和参数
	// 注意：Redis 内联命令中，参数中的空格需要用引号包裹，但大多数客户端不使用这个特性
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty inline command")
	}

	// 转换为字节数组
	args := make([][]byte, len(parts))
	for i, part := range parts {
		args[i] = []byte(part)
	}

	logger.Logger.Debug().
		Str("inline_command", cmdStr).
		Int("arg_count", len(args)).
		Msg("解析内联命令")

	return &Array{Args: args}, nil
}

func WriteRESP(w io.Writer, resp RESP) error {
	if resp == nil {
		_, err := fmt.Fprint(w, "$-1\r\n")
		return err
	}
	switch v := resp.(type) {
	case *BulkString:
		if v == nil || *v == nil {
			if _, err := w.Write([]byte("$-1\r\n")); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintf(w, "$%d\r\n", len(*v)); err != nil {
				return err
			}
			if _, err := w.Write(*v); err != nil {
				return err
			}
			if _, err := w.Write([]byte("\r\n")); err != nil {
				return err
			}
		}
	case *Array:
		if _, err := fmt.Fprintf(w, "*%d\r\n", len(v.Args)); err != nil {
			return err
		}
		for _, arg := range v.Args {
			if arg == nil {
				if _, err := w.Write([]byte("$-1\r\n")); err != nil {
					return err
				}
			} else {
				if _, err := fmt.Fprintf(w, "$%d\r\n", len(arg)); err != nil {
					return err
				}
				if _, err := w.Write(arg); err != nil {
					return err
				}
				if _, err := w.Write([]byte("\r\n")); err != nil {
					return err
				}
			}
		}
	case *NestedArray:
		if _, err := fmt.Fprintf(w, "*%d\r\n", len(v.Elems)); err != nil {
			return err
		}
		for _, elem := range v.Elems {
			if err := WriteRESP(w, elem); err != nil {
				return err
			}
		}
	case *SimpleString:
		if _, err := fmt.Fprintf(w, "+%s\r\n", string(*v)); err != nil {
			return err
		}
	case Error:
		if _, err := fmt.Fprintf(w, "-%s\r\n", string(v)); err != nil {
			return err
		}
	case Integer:
		if _, err := fmt.Fprintf(w, ":%d\r\n", int64(v)); err != nil {
			return err
		}
	case NilArray:
		if _, err := w.Write([]byte("*-1\r\n")); err != nil {
			return err
		}
	case RawString:
		if _, err := w.Write([]byte(string(v))); err != nil {
			return err
		}
	default:
		respStr := resp.String()
		if _, err := fmt.Fprint(w, respStr); err != nil {
			return err
		}
	}
	if flusher, ok := w.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	return nil
}

// helpers
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	// 去掉 \r\n
	if len(line) > 0 && line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line, nil
}

func joinBulkStrings(args [][]byte) string {
	var b strings.Builder
	for _, arg := range args {
		if arg == nil {
			b.WriteString("$-1\r\n")
		} else {
			b.WriteString("$")
			b.WriteString(strconv.Itoa(len(arg)))
			b.WriteString("\r\n")
			b.Write(arg)
			b.WriteString("\r\n")
		}
	}
	return b.String()
}

// RawString 用于直接返回RESP格式的字符串
type RawString string

func (r RawString) String() string {
	return string(r)
}

// NewScanResponse 创建SCAN命令的响应格式 [cursor, [keys...]]
func NewScanResponse(cursor uint64, keys []string) RESP {
	var b strings.Builder
	// 外层数组: 2个元素 (cursor, keys数组)
	b.WriteString("*2\r\n")
	// cursor 作为 bulk string
	cursorStr := strconv.FormatUint(cursor, 10)
	b.WriteString("$")
	b.WriteString(strconv.Itoa(len(cursorStr)))
	b.WriteString("\r\n")
	b.WriteString(cursorStr)
	b.WriteString("\r\n")
	// keys 数组
	b.WriteString("*")
	b.WriteString(strconv.Itoa(len(keys)))
	b.WriteString("\r\n")
	for _, key := range keys {
		b.WriteString("$")
		b.WriteString(strconv.Itoa(len(key)))
		b.WriteString("\r\n")
		b.WriteString(key)
		b.WriteString("\r\n")
	}
	return RawString(b.String())
}

// 工厂
func NewSimpleString(s string) RESP { r := SimpleString(s); return &r }
func NewBulkString(b []byte) RESP {
	if b == nil {
		// 返回一个非 nil 的 *BulkString，但值为 nil
		var r BulkString = nil
		return &r
	}
	r := BulkString(b)
	return &r
}
func NewError(e string) RESP  { r := Error(e); return &r }
func NewInteger(i int64) RESP { r := Integer(i); return &r }

var (
	OK = NewSimpleString("OK")
)

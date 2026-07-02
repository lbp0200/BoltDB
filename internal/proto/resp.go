// RESP protocol types: Array, BulkString, SimpleString, Error, Integer,
// NestedArray, plus RESP3 types (Map, Set, Push, Null, Double, Boolean, BigNumber).
package proto

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/lbp0200/BoltDB/internal/logger"
)

var (
	// MaxBulkLen 是 RESP bulk string 的最大长度限制（默认 256MB）
	// 可通过 SetMaxBulkLen() 在启动时调整
	MaxBulkLen atomic.Int64
	// MaxArrayLen 是 RESP array 的最大元素数
	MaxArrayLen atomic.Int64
	// MaxLineLen 是行长度限制（默认 64MB，防止 OOM）
	MaxLineLen atomic.Int64
)

func init() {
	MaxBulkLen.Store(256 * 1024 * 1024)
	MaxArrayLen.Store(1024 * 1024)
	MaxLineLen.Store(64 * 1024 * 1024)
}

// SetMaxBulkLen 设置 RESP bulk string 最大长度（字节）
func SetMaxBulkLen(n int64) {
	if n > 0 {
		MaxBulkLen.Store(n)
	}
}

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

// RESP3 types

// Map represents a RESP3 Map type, wire prefix '%'
type Map struct {
	Elems []RESP // key1, val1, key2, val2, ...
}

func (m *Map) String() string {
	var b strings.Builder
	b.WriteString("%")
	b.WriteString(strconv.Itoa(len(m.Elems) / 2))
	b.WriteString("\r\n")
	for _, elem := range m.Elems {
		b.WriteString(elem.String())
	}
	return b.String()
}

// Set represents a RESP3 Set type, wire prefix '~'
type Set struct {
	Elems []RESP
}

func (s *Set) String() string {
	var b strings.Builder
	b.WriteString("~")
	b.WriteString(strconv.Itoa(len(s.Elems)))
	b.WriteString("\r\n")
	for _, elem := range s.Elems {
		b.WriteString(elem.String())
	}
	return b.String()
}

// Push represents a RESP3 Push type, wire prefix '>'
// Used for PubSub messages, monitor output, and other server-initiated pushes.
type Push struct {
	Elems []RESP
}

func (p *Push) String() string {
	var b strings.Builder
	b.WriteString(">")
	b.WriteString(strconv.Itoa(len(p.Elems)))
	b.WriteString("\r\n")
	for _, elem := range p.Elems {
		b.WriteString(elem.String())
	}
	return b.String()
}

// Null represents a RESP3 Null value, wire prefix '_'
type Null struct{}

func (n Null) String() string { return "_\r\n" }

// Double represents a RESP3 Double value, wire prefix ','
type Double struct {
	Value float64
}

func (d Double) String() string { return "," + strconv.FormatFloat(d.Value, 'g', -1, 64) + "\r\n" }

// Boolean represents a RESP3 Boolean value, wire prefix '#'
type Boolean struct {
	Value bool
}

func (b Boolean) String() string {
	if b.Value {
		return "#t\r\n"
	}
	return "#f\r\n"
}

// BigNumber represents a RESP3 Big number, wire prefix '('
type BigNumber struct {
	Value string
}

func (b BigNumber) String() string { return "(" + b.Value + "\r\n" }

// VerbatimString represents a RESP3 Verbatim string, wire prefix '='
// Contains a 3-byte encoding prefix (e.g. "txt") followed by ':' and the value.
type VerbatimString struct {
	Encoding string // 3-char encoding, e.g. "txt" or "mkd"
	Value    string
}

func (v VerbatimString) String() string {
	return "=" + strconv.Itoa(len(v.Encoding)+1+len(v.Value)) + "\r\n" + v.Encoding + ":" + v.Value + "\r\n"
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
		if int64(n) > MaxArrayLen.Load() {
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
				if int64(bulkLen) > MaxBulkLen.Load() {
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
	case '-': // Error
		if len(line) < 2 {
			return nil, fmt.Errorf("invalid error format")
		}
		return &Array{Args: [][]byte{line[1:]}}, nil
	case '$': // Bulk String (单独发送，redis-benchmark 不使用)
		// 这不应该出现在命令中，但为了健壮性处理
		bulkLen, err := strconv.Atoi(string(line[1:]))
		if err != nil || bulkLen < -1 {
			return nil, fmt.Errorf("invalid bulk string length: %s", line[1:])
		}
		if bulkLen == -1 {
			return &Array{Args: [][]byte{nil}}, nil
		}
		if int64(bulkLen) > MaxBulkLen.Load() {
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
// 参数用空格分隔，支持双引号包裹含空格的参数
func parseInlineCommand(line []byte) (*Array, error) {
	cmdStr := strings.TrimSpace(string(line))
	if cmdStr == "" {
		return nil, fmt.Errorf("empty inline command")
	}

	parts := parseInlineArgs(cmdStr)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty inline command")
	}

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

// parseInlineArgs splits an inline command string respecting double-quoted arguments.
// Unquoted tokens are split on whitespace; quoted tokens preserve internal spaces.
// Adjacent quotes produce empty strings (e.g. SET "" value → [SET, "", value]).
func parseInlineArgs(s string) []string {
	var result []string
	var current strings.Builder
	inQuote := false

	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '"':
			if inQuote {
				// Closing quote: emit token (even if empty)
				result = append(result, current.String())
				current.Reset()
				inQuote = false
			} else {
				// Opening quote: if we had accumulated chars before quote, emit them first
				if current.Len() > 0 {
					result = append(result, current.String())
					current.Reset()
				}
				inQuote = true
			}
		case ch == ' ' && !inQuote:
			if current.Len() > 0 {
				result = append(result, current.String())
				current.Reset()
			}
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		result = append(result, current.String())
	}
	return result
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
	case *Map:
		if _, err := fmt.Fprintf(w, "%%%d\r\n", len(v.Elems)/2); err != nil {
			return err
		}
		for _, elem := range v.Elems {
			if err := WriteRESP(w, elem); err != nil {
				return err
			}
		}
	case *Set:
		if _, err := fmt.Fprintf(w, "~%d\r\n", len(v.Elems)); err != nil {
			return err
		}
		for _, elem := range v.Elems {
			if err := WriteRESP(w, elem); err != nil {
				return err
			}
		}
	case *Push:
		if _, err := fmt.Fprintf(w, ">%d\r\n", len(v.Elems)); err != nil {
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
	case Null:
		if _, err := w.Write([]byte("_\r\n")); err != nil {
			return err
		}
	case Double:
		if _, err := fmt.Fprintf(w, ",%s\r\n", strconv.FormatFloat(v.Value, 'g', -1, 64)); err != nil {
			return err
		}
	case Boolean:
		if v.Value {
			if _, err := w.Write([]byte("#t\r\n")); err != nil {
				return err
			}
		} else {
			if _, err := w.Write([]byte("#f\r\n")); err != nil {
				return err
			}
		}
	case BigNumber:
		if _, err := fmt.Fprintf(w, "(%s\r\n", v.Value); err != nil {
			return err
		}
	case VerbatimString:
		payload := v.Encoding + ":" + v.Value
		if _, err := fmt.Fprintf(w, "=%d\r\n", len(payload)); err != nil {
			return err
		}
		if _, err := w.Write([]byte(payload)); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\r\n")); err != nil {
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
	if int64(len(line)) > MaxLineLen.Load() {
		return nil, fmt.Errorf("line too long: %d bytes (max %d)", len(line), MaxLineLen.Load())
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

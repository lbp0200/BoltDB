//go:build ignore

package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port(), 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	send := func(cmd string) {
		rw.WriteString(cmd)
		rw.Flush()
	}
	read := func() string {
		l, err := rw.ReadString('\n')
		if err != nil {
			panic(fmt.Sprintf("read error: %v", err))
		}
		return strings.TrimSuffix(l, "\r\n")
	}

	send("*1\r\n$4\r\nPING\r\n")
	ok("PING (RESP2)", read() == "+PONG")

	send("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
	line := read()
	ok("HELLO 3 Map", strings.HasPrefix(line, "%"))
	if line[0] == '%' {
		n, _ := strconv.Atoi(line[1:])
		for i := 0; i < n*2; i++ {
			readRESP(read)
		}
	}

	send("*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n")
	line = read()
	ok("HELLO 3 re", strings.HasPrefix(line, "%"))
	if line[0] == '%' {
		n, _ := strconv.Atoi(line[1:])
		for i := 0; i < n*2; i++ {
			readRESP(read)
		}
	}

	send("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	ok("SET", read() == "+OK")

	send("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n")
	line = read()
	ok("GET BulkString", strings.HasPrefix(line, "$"))
	ok("GET value", read() == "bar")

	send("*1\r\n$4\r\nPING\r\n")
	ok("PING (RESP3)", read() == "+PONG")

	send("*2\r\n$4\r\nINCR\r\n$6\r\ncounter\r\n")
	ok("INCR Integer", strings.HasPrefix(read(), ":"))

	send("*2\r\n$6\r\nEXISTS\r\n$3\r\nfoo\r\n")
	ok("EXISTS Integer", strings.HasPrefix(read(), ":"))

	send("*2\r\n$4\r\nTYPE\r\n$3\r\nfoo\r\n")
	ok("TYPE SimpleString", read() == "+string")

	send("*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n")
	line = read()
	ok("HELLO 2 Array", strings.HasPrefix(line, "*"))
	if line[0] == '*' {
		n, _ := strconv.Atoi(line[1:])
		for i := 0; i < n; i++ {
			readRESP(read)
		}
	}

	send("*1\r\n$4\r\nPING\r\n")
	ok("PING (back RESP2)", read() == "+PONG")

	summary()
}

func readRESP(read func() string) {
	line := read()
	if len(line) == 0 {
		return
	}
	switch line[0] {
	case '+', '-', ':', '_', '#', ',', '(':
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n >= 0 {
			read()
		}
	case '*', '~', '>', '%':
		n, _ := strconv.Atoi(line[1:])
		elems := n
		if line[0] == '%' {
			elems = n * 2
		}
		for i := 0; i < elems; i++ {
			readRESP(read)
		}
	}
}

func port() string {
	if len(os.Args) > 1 {
		return os.Args[1]
	}
	return "6337"
}

var results []bool

func ok(label string, passed bool) {
	results = append(results, passed)
	status := "PASS"
	if !passed {
		status = "FAIL"
	}
	fmt.Printf("%s %s\n", status, label)
}

func summary() {
	passed, failed := 0, 0
	for _, r := range results {
		if r {
			passed++
		} else {
			failed++
		}
	}
	fmt.Printf("\n%d/%d passed", passed, len(results))
	if failed > 0 {
		fmt.Printf(", %d FAILED", failed)
		os.Exit(1)
	}
	fmt.Println(" — ALL PASSED")
}

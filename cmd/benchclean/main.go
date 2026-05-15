package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	var currentName string

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "badger") ||
			strings.HasPrefix(trimmed, "Level") ||
			strings.HasPrefix(trimmed, "Discard") ||
			strings.HasPrefix(trimmed, "Set next") ||
			strings.HasPrefix(trimmed, "Lifetime") ||
			strings.HasPrefix(trimmed, "[ ]") ||
			strings.HasPrefix(trimmed, "[B]") {
			continue
		}

		if strings.HasPrefix(line, "goos:") ||
			strings.HasPrefix(line, "goarch:") ||
			strings.HasPrefix(line, "pkg:") ||
			strings.HasPrefix(line, "cpu:") ||
			strings.HasPrefix(line, "ok ") ||
			strings.HasPrefix(line, "FAIL") {
			fmt.Println(line)
			continue
		}

		if strings.HasPrefix(line, "Benchmark") {
			parts := strings.SplitN(line, "\t", 2)
			name := strings.TrimSpace(parts[0])
			if strings.Contains(name, "-") {
				currentName = name
			}
			continue
		}

		if currentName != "" && len(trimmed) > 0 && trimmed[0] >= '0' && trimmed[0] <= '9' {
			fields := strings.Fields(trimmed)
			if len(fields) >= 3 {
				fmt.Printf("%s\t%s\t%s\t%s\t%s\n",
					currentName, fields[0], fields[1], fields[2], strings.Join(fields[3:], " "))
				currentName = ""
			}
		}
	}
}

package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	f, _ := os.Open("cases")
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines) // strips newline characters

	for scanner.Scan() {
		line := scanner.Bytes()
		trunc := truncate(line[1:], int(uint8(line[0])))
		fmt.Printf("%s\n", trunc)
	}
}

func truncate(s []byte, n int) []byte {
	if n >= len(s) {
		return s
	}

	for n > 0 && (s[n]&0xc0) == 0x80 {
		n -= 1
	}

	return s[:n]
}

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
	scanner.Split(bufio.ScanLines) //strips newline characters

	for scanner.Scan() {
		s := scanner.Bytes()
		fmt.Printf("%s\n", truncate(s[1:], int(uint8(s[0]))))
	}
}

func truncate(s []byte, n int) []byte {
	if n > len(s) {
		return s
	}

	c := 0
	for n > 0 {
		byte := s[n-1]
		if c == 1 && (byte&0xc0) == 192 {
			n = n + c
			break
		} else if c == 2 && (byte&0xe0) == 224 {
			n = n + c
			break
		} else if c == 3 && (byte&0xf0) == 240 {
			n = n + c
			break
		} else if byte&0x80 == 128 {
			c++
			n -= 1
		} else {
			break
		}
	}

	return s[:n]
}

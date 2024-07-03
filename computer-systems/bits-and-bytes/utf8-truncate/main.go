package main

import (
	"bufio"
	"fmt"
	"os"
	"unicode/utf8"
)

func main() {
	f, _ := os.Open("cases")
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		line := scanner.Bytes()
		l, bytesToKeep := len(line), int(line[0])
		endIndex := bytesToKeep
		if bytesToKeep > l {
			endIndex = l
		}
		idx := 0

		var output []byte
		input := line[1:]

		for idx < endIndex {
			_, size := utf8.DecodeRune(input[idx:])
			if size == 0 {
				break
			}
			if len(output)+size <= endIndex {
				output = append(output, input[idx:idx+size]...)
			}
			idx += size
		}
		fmt.Printf("%s\n", output)
	}
}

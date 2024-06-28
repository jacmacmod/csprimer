package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// convert from hexidecimal to rgb

// take std out as in put
func charToN(ch string) int {
	if i, err := strconv.Atoi(ch); err == nil {
		return i
	}

	ch = strings.ToLower(ch)
	hexMap := map[string]int{
		"a": 10,
		"b": 11,
		"c": 12,
		"d": 13,
		"e": 14,
		"f": 15,
	}

	return hexMap[ch]
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Text()
		before, after, found := strings.Cut(line, "#")

		if found {
			var nums []string
			for i := 0; i < len(after)-1; i += 2 {
				sum := charToN(string(after[i]))*16 +
					charToN(string(after[i+1]))

				nums = append(nums, strconv.Itoa(sum))
			}

			fmt.Printf("%s: rgb(%s);\n", before[0:len(before)-2], strings.Join(nums, " "))
		} else {
			fmt.Println(line)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}
}

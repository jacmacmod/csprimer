package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		before, after, found := strings.Cut(line, ": #")

		if found {
			var nums []string

			step := 0
			if (len(after)-1)%8 == 0 || (len(after)-1)%6 == 0 {
				step = 1
			}
			for i := 0; i < len(after)-1; i++ {
				sum := charToN(string(after[i]))*16 +
					charToN(string(after[i+step]))
				nums = append(nums, strconv.Itoa(sum))
				i += step
			}

			if (len(after)-1)%4 == 0 {
				num1, _ := strconv.Atoi(nums[2])
				num2, _ := strconv.Atoi(nums[3])
				fmt.Printf("%s: rgba(%s / %.5f);\n", before, strings.Join(nums[0:3], " "), float64(num2)/float64(num1))
			} else {
				fmt.Printf("%s: rgb(%s);\n", before, strings.Join(nums, " "))
			}

		} else {
			fmt.Println(line)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}
}

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

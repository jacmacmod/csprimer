package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func main() {
	fmt.Println("Enter a number")
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("-> ")
		s, _ := reader.ReadString('\n')
		s = strings.TrimSpace(s)
		if i, err := strconv.Atoi(s); err == nil {
			for x := 0; x < i; x++ {
				os.Stdout.Write([]byte{7})
			}

			fmt.Println()
		} else {
			fmt.Println("Please enter a number")
		}
	}
}

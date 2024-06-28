package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var hexMap = make(map[string]int)
var corpus string = "0123456789abcdef"

func main() {
	// Zip hex map
	for i, c := range corpus {
		hexMap[string(c)] = i
	}

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Text()
		re := regexp.MustCompile(`#[0-9a-fA-F]+`)
		colorValue := re.FindString(line)

		if colorValue != "" {
			colorValue = strings.ToLower(colorValue[1:]) // Drop #
			var decimals []string
			hx := ""
			// normalize shorthand hex value i.e. fff or ffff
			if len(colorValue) <= 4 {
				for _, ch := range colorValue {
					hx += strings.Repeat(string(ch), 2)
				}
			} else {
				hx = colorValue
			}

			for i := 0; i < len(hx); i += 2 {
				sum := hexByteToDecimal(hx[i : i+2])
				decimals = append(decimals, strconv.Itoa(sum))
			}

			decimalRepresentation := ""
			if len(colorValue)%4 == 0 {
				num2, _ := strconv.Atoi(decimals[3])
				decimalRepresentation = fmt.Sprintf("rgba(%s / %.5f)", strings.Join(decimals[0:3], " "), float64(num2)/float64(255))
			} else {
				decimalRepresentation = fmt.Sprintf("rgb(%s)", strings.Join(decimals, " "))
			}
			line = re.ReplaceAllString(line, decimalRepresentation)
		}

		fmt.Println(line)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}
}

func hexByteToDecimal(hexByte string) int {
	return hexMap[string(hexByte[0])]<<4 + hexMap[(string(hexByte[1]))]
}

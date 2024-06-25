package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

func main() {
	data, err := os.ReadFile("150.uint64")

	if err != nil {
		panic(err)
	}

	val := unpack(data)

	fmt.Printf("result %s ", hex.Dump(encode(val)))
	fmt.Println(decode(encode(val)))
	return
}

func decode(bytes []byte) uint64 {
	var s []string
	for _, byte := range bytes {
		eightBits := fmt.Sprintf("%08b", uint64(byte))
		s = append(s, eightBits[1:])
	}

	slices.Reverse(s)
	joined := strings.Join(s, "")
	val, _ := strconv.ParseUint(joined, 2, 64)
	return val
}

func encode(n uint64) []byte {
	if n < 128 {
		b := strconv.FormatUint(n, 2)
		b = fmt.Sprintf("%s%s", strings.Repeat("0", (8-len(b))%8), b)
		v, _ := strconv.ParseUint(b, 2, 8)
		var bytes []byte
		return append(bytes, byte(v))
	}

	b := strconv.FormatUint(n, 2)
	var s string
	for i := 0; i < len(b); i += 7 {
		if (len(b)-i)-7 > 0 {
			split := b[(len(b)-i)-7 : len(b)-i]
			s = fmt.Sprintf("1%s %s", split, s)
		} else {
			s = fmt.Sprintf("0%s%s %s", strings.Repeat("0", 7-len(b)%i), b[0:len(b)%i], s)
		}
	}

	s = strings.TrimSpace(s)
	bytes := strings.Split(s, " ")
	slices.Reverse(bytes)

	var dump []byte
	for _, s := range bytes {
		v, _ := strconv.ParseUint(s, 2, 8)
		dump = append(dump, byte(v))
	}

	return dump
}

func unpack(bytes []byte) uint64 {
	s := fmt.Sprintf("%x", bytes)
	arr := strings.Split(s, " ")
	var total uint64

	for i := len(arr) - 1; i >= 0; i-- {
		item, _ := strconv.ParseUint(arr[i], 16, 64)
		total += item
	}
	return total
}

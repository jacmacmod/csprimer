package main

import (
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

func main() {
	files := []string{"150.uint64"}
	// files := []string{"1.uint64", "150.uint64", "maxint.uint64"}
	for _, fileName := range files {
		data, err := os.ReadFile(fileName)

		if err != nil {
			panic(err)
		}
		val := unpack(data)

		fmt.Printf("Protobuf encoded varint %d -> % x decoded -> %d \n\n", val, encode(val), decode(encode(val)))

	}
	return
}

func encode(n uint64) []byte {
	var out []byte

	for n > 0 {
		part := n & 0x7f
		n >>= 7
		if n > 0 {
			part |= 0x80
		}
		out = append(out, byte(part))
	}

	return out
}

func decode(varn []byte) uint64 {
	slices.Reverse(varn)

	var n uint64
	for _, byte := range varn {
		n <<= 7
		n |= uint64((byte & 0x7f))
	}
	return n
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

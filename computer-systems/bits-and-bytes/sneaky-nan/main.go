package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"slices"
)

func main() {
	s := "Hello!"
	fmt.Printf("%s\n", s)
	concealed := conceal(s)
	fmt.Printf("concealed\n")
	fmt.Printf("%v\n", concealed)
	fmt.Printf("Type %T\n", concealed)
	fmt.Printf("x + 5 -> %v\n", concealed+5)
	fmt.Printf("x / 5 -> %v\n", concealed/5)
	fmt.Printf("revealed %v\n", reveal(concealed))

	s = "jack"
	fmt.Printf("%s\n", s)
	concealed = conceal(s)
	fmt.Printf("concealed\n")
	fmt.Printf("%v\n", concealed)
	fmt.Printf("revealed -> %v\n", reveal(concealed))
}

func conceal(s string) float64 {

	if len(s) > 6 {
		log.Fatal("String cannot be more than 6 characters")
	}
	bytesSlice := []byte(s)

	for len(bytesSlice) < 6 {
		bytesSlice = append(bytesSlice, 0x00)
	}

	slices.Reverse([]byte(bytesSlice))
	bytesSlice = append(bytesSlice, []byte{0xF8, 0xFF}...)
	slices.Reverse(bytesSlice)

	return math.Float64frombits(binary.BigEndian.Uint64(bytesSlice))
}

func reveal(concealedNaN float64) string {
	n := math.Float64bits(concealedNaN)
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return string(b[2:])
}

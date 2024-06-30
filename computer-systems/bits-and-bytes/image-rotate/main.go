package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	data, err := os.ReadFile("teapot.bmp")
	if err != nil {
		log.Fatal(err)
	}
	w, h, o := le(data[10:14]), le(data[18:22]), le(data[22:26])
	fmt.Printf("Byte Offset: %d\nWidth: %d x Height %d\n", w, h, o)
	rotateRight(data)

}

func rotateRight(data []byte) {
	f, err := os.Create("right.bmp")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	width, height, offset := le(data[10:14]), le(data[18:22]), le(data[10:14])
	sourcePixels := data[offset:]
	f.Write(data[:offset])

	for sw := 3; sw <= width*3*3+20; sw += 3 {
		for sh := 3; sh <= height*3; sh += 3 {
			location := (sh * height) - (sw)
			fmt.Println(location)
			f.Write(sourcePixels[location : location+3])
		}
	}
	os.Open("right.bmp")
}

func blackFile(data []byte) {
	os.Remove("black.bmp")
	f, err := os.Create("black.bmp")
	if err != nil {
		panic(err)
	}
	offset := le(data[10:14])
	// copy header
	f.Write(data[:offset])
	fmt.Println("length")
	fmt.Println(len(data[offset:]))
	for i := 0; i < len(data[offset:]); i++ {
		f.Write([]byte{0x00})
	}
	defer f.Close()

	// reader := bufio.NewReader(os.Stdin)
	// b, _ := reader.ReadByte()
	// fmt.Printf("% x", b)
	// b, _ = reader.ReadByte()
	// fmt.Printf("% x", b)
	// le(data[10:14])
	// fmt.Printf("first bgr value: % x\n", data[int(data[10]):int(data[10])+3])
	// 4 byte slice to int binary.LittleEndian.Uint16
	// fmt.Printf("Width %d  x Height %d\n\n", binary.LittleEndian.Uint16(data[18:22]), binary.LittleEndian.Uint16(data[22:26]))
}

func le(bs []byte) int {
	var total int = 0
	for i, b := range bs {
		total += int(b) << (i * 8)
	}
	return total
}

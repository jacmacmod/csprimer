package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"syscall"
)

func main() {
	url := os.Args[1]

	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
	if err != nil {
		log.Fatalf("Error creating socket %v\n", err)
	}
	defer syscall.Close(fd)

	host := [4]byte{8, 8, 8, 8}
	port := 53
	addr := syscall.SockaddrInet4{Port: port, Addr: host}

	xid := rand.Intn(65535)

	query := make([]byte, 12)
	flags := 0x0100 // use recursive
	binary.BigEndian.PutUint16(query[0:2], uint16(xid))
	binary.BigEndian.PutUint16(query[2:4], uint16(flags))
	binary.BigEndian.PutUint16(query[4:6], uint16(1))

	// Question
	question := []byte{}
	for _, u := range strings.Split(url, ".") {
		question = append(question, uint8(len(u)))
		question = append(question, []byte(u)...)
	}
	question = append(question, []byte{0x00, 0x00, 0x01, 0x00, 0x01}...)
	query = append(query, question...)

	if err = syscall.Sendto(fd, query, 0, &addr); err != nil {
		log.Fatalf("Error sending message to DNS server: %v", err)
	}

	r := make([]byte, 512)
	for {
		_, from, err := syscall.Recvfrom(fd, r, 0)
		if err != nil {
			log.Fatalf("Error recieving DNS response: %v", err)
		}
		if v, ok := from.(*syscall.SockaddrInet4); v.Addr != host || !ok {
			continue
		}
		if binary.BigEndian.Uint16(r[0:2]) != uint16(xid) {
			log.Printf("X ID does not match")
			continue
		}
		break
	}

	if (r[2]&0x80)>>7 != 1 {
		log.Fatalln("QR must be 1")
	}

	fmt.Printf("Response\nID: % x\n\n", binary.BigEndian.Uint16(r[0:2]))
	fmt.Printf("QR: %d\nOPCODE: %d\nAA: %d\nTC: %d\nRD: %d\nRA: %d\nZ: %d\nRCODE: %d\n\n",
		(r[2]&0x80)>>7, (r[2]&0x78)>>3, (r[2]&0x04)>>2, (r[2]&0x02)>>1, r[2]&0x01, (r[3]&0x80)>>7, (r[3]&0x70)>>4, r[3]&0x10)
	fmt.Printf("QDCOUNT: %d\nANCOUNT: %d\nNSCOUNT: %d\nARCOUNT: %d\n\n",
		binary.BigEndian.Uint16(r[4:6]),
		binary.BigEndian.Uint16(r[6:8]),
		binary.BigEndian.Uint16(r[8:10]),
		binary.BigEndian.Uint16(r[10:12]),
	)
	offset := len(question) + 24
	fmt.Printf("IP %d.%d.%d.%d\n", r[offset], r[offset+1], r[offset+2], r[offset+3])
	os.Exit(0)
}

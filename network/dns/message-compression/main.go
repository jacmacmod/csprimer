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

const dnsPort = 53
const bufferSize = 4096

var googleHost = [4]byte{8, 8, 8, 8}

type DNSClient struct {
	address     syscall.SockaddrInet4
	fd          int
	fromAddress syscall.SockaddrInet4
	data        []byte
}

func main() {
	url := os.Args[1]

	dnsClient := newDNSClient(googleHost)
	dnsClient.createSocket()
	defer dnsClient.closeSocket()

	query, question, xid := prepareRequest(url)
	dnsClient.SendRequest(query)

	for {
		n := dnsClient.ReceiveMsg()

		if dnsClient.fromAddress.Addr != googleHost {
			continue
		}

		if binary.BigEndian.Uint16(dnsClient.data[0:2]) != uint16(xid) {
			log.Printf("X ID does not match")
			break
		}

		if n < bufferSize {
			break
		}
	}

	validateResponse(dnsClient.data)
	printAnswer(dnsClient.data, question)
}

func newDNSClient(host [4]byte) DNSClient {
	r := make([]byte, bufferSize)
	return DNSClient{address: syscall.SockaddrInet4{Port: dnsPort, Addr: host}, data: r}
}

func (d *DNSClient) createSocket() {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
	if err != nil {
		log.Fatalf("Error creating socket %v\n", err)
	}
	d.fd = fd
}

func (d *DNSClient) closeSocket() {
	syscall.Close(d.fd)
}

func (d *DNSClient) SendRequest(p []byte) {
	if err := syscall.Sendto(d.fd, p, 0, &d.address); err != nil {
		log.Fatalf("Error sending message to DNS server: %v", err)
	}
}

func (d *DNSClient) ReceiveMsg() int {
	n, from, err := syscall.Recvfrom(d.fd, d.data, 0)
	if err != nil {
		log.Fatalf("Error recieving DNS response: %v", err)
	}

	s, ok := from.(*syscall.SockaddrInet4)
	if !ok {
		log.Fatal("Unable to parse address")
	}
	d.fromAddress = *s

	return n
}

func prepareRequest(url string) ([]byte, []byte, int) {
	xid, flags := rand.Intn(65535), 0x0100 // recursive

	query := make([]byte, 12)
	binary.BigEndian.PutUint16(query[0:2], uint16(xid))
	binary.BigEndian.PutUint16(query[2:4], uint16(flags))
	binary.BigEndian.PutUint16(query[4:6], uint16(1))

	// Question
	question := []byte{}
	for _, u := range strings.Split(url, ".") {
		question = append(question, uint8(len(u)))
		question = append(question, []byte(u)...)
	}

	question = append(question, []byte{0x00, 0x00, 0x01, 0x00, 0x01}...) //end sequence
	query = append(query, question...)
	return query, question, xid
}

func validateResponse(b []byte) {
	if (b[2]&0x80)>>7 != 1 {
		log.Fatalln("QR must be 1")
	}

	if (b[3]&0x70)>>4 != 0 {
		log.Fatalln("unexpected z value", (b[3]&0x70)>>4)
	}
}

func printAnswer(b []byte, question []byte) {
	fmt.Println(";; Got answer")
	printHeader(b)
	printIP(b, question)
}

func printIP(b []byte, q []byte) {
	offset := len(q) + 24
	fmt.Printf("IP %d.%d.%d.%d\n", b[offset], b[offset+1], b[offset+2], b[offset+3])
}

func printHeader(b []byte) {
	id, aa, rcode, opcode := binary.BigEndian.Uint16(b[0:2]), b[2]&0x04, b[3]&0x10, (b[2]&0x78)>>3

	opcodeMap := map[int]string{
		0: "QUERY",
		1: "IQUERY",
		2: "STATUS",
	}

	rcodeMap := map[int]string{
		0: "NOERROR",
		1: "FORMATERROR",
		2: "SERVERERROR",
		3: "NAMERERROR",
		4: "NOTIMPLEMENTED",
		5: "REFUSED",
	}

	fmt.Printf(";; --HEADER-- opcode: %s, status: %s, id: %d\n", opcodeMap[int(opcode)], rcodeMap[int(rcode)], id)
	printFlags(b)

	if aa != 0 {
		aa = 1
	}

	fmt.Printf(" AA: %d\n", aa)

	fmt.Printf(";; QDCOUNT: %d ANCOUNT: %d NSCOUNT: %d ARCOUNT: %d\n\n",
		binary.BigEndian.Uint16(b[4:6]),
		binary.BigEndian.Uint16(b[6:8]),
		binary.BigEndian.Uint16(b[8:10]),
		binary.BigEndian.Uint16(b[10:12]),
	)
}

func printFlags(b []byte) {
	fmt.Printf("flags:")
	qr, tc, rd, ra, aa := b[2]&0x80, b[2]&0x02, b[2]&0x01, b[3]&0x80, b[2]&0x04

	if qr != 0 {
		fmt.Printf(" qr")
	}

	if tc != 0 {
		fmt.Printf(" tc")
	}

	if rd != 0 {
		fmt.Printf(" rd")
	}

	if ra != 0 {
		fmt.Printf(" ra")
	}

	if aa != 0 {
		fmt.Printf(" aa")
	}

	fmt.Printf("; ")
}

package main

import (
	"log"
	"syscall"
)

type RType int
type RClass int

const (
	dnsPort    = 53
	bufferSize = 4096

	A     RType = 1
	NS    RType = 2
	CNAME RType = 5  // canonical name for an alias
	SOA   RType = 6  // start of zone ability
	WKS   RType = 11 // well known service descriptor
	PTR   RType = 12
	HINFO RType = 13
	MINFO RType = 14
	MX    RType = 15
	TXT   RType = 16
	AAAA  RType = 28

	IN RClass = 1
	CS RClass = 2
	CH RClass = 3
	HS RClass = 4
)

var (
	googleHost = [4]byte{8, 8, 8, 8}

	TypesMap = map[RType]string{
		A:     "A",
		NS:    "NS",
		CNAME: "CNAME",
		SOA:   "SOA",
		WKS:   "WKS",
		PTR:   "PTR",
		HINFO: "HINFO",
		MINFO: "MINFO",
		MX:    "MX",
		TXT:   "TXT",
		AAAA:  "AAAA",
	}

	ClassMap = map[RClass]string{
		IN: "IN",
		CS: "CS",
		CH: "CH",
		HS: "HS",
	}

	opcodeMap = map[int]string{
		0: "QUERY",
		1: "IQUERY",
		2: "STATUS",
	}

	rcodeMap = map[int]string{
		0: "NOERROR",
		1: "FORMATERROR",
		2: "SERVERERROR",
		3: "NAMERERROR",
		4: "NOTIMPLEMENTED",
		5: "REFUSED",
	}
)

type DNSClient struct {
	address     syscall.SockaddrInet4
	fd          int
	fromAddress syscall.SockaddrInet4
	data        []byte
}

func newDNSClient(host [4]byte) DNSClient {
	r := make([]byte, bufferSize)
	return DNSClient{
		address: syscall.SockaddrInet4{
			Port: dnsPort,
			Addr: host,
		},
		data: r,
	}
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

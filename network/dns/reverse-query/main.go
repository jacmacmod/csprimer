package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/netip"
	"os"
	"slices"
	"strings"
	"syscall"
	"time"
)

const (
	dnsPort    = 53
	bufferSize = 4096
)

var (
	googleHost = [4]byte{8, 8, 8, 8}

	class = "IN"

	typesMap = map[string]int{
		"A":     1,
		"NS":    2,
		"CNAME": 5,  // canonical name for an alias
		"SOA":   6,  // start of zone ability
		"WKS":   11, // well known service descriptor
		"PTR":   12,
		"HINFO": 13,
		"MINFO": 14,
		"MX":    15,
		"TXT":   16,
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

func main() {
	if len(os.Args) <= 1 {
		log.Fatalln("no url or ip specified")
	}

	var ipAddress = flag.String("x", "", "ip address for reverse lookup")
	flag.Parse()

	url, rrtype := os.Args[1], "A"
	if len(os.Args) > 2 {
		rrtype = os.Args[2]
		if ipAddress == nil && rrtype != "A" && rrtype != "NS" && rrtype != "PTR" {
			log.Fatalln("type not supported")
		}
	}

	if *ipAddress != "" {
		url = *ipAddress
		if _, err := netip.ParseAddr(url); err == nil {
			rrtype = "PTR"
		} else {
			log.Fatalln("invalid ip address")
		}
	}

	dnsClient := newDNSClient(googleHost)
	dnsClient.createSocket()
	defer dnsClient.closeSocket()

	query, xid := prepareQuery(url, rrtype)
	start, n := time.Now(), 0 // bytes received
	dnsClient.SendRequest(query)

	for {
		n = dnsClient.ReceiveMsg()

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
	printResponse(dnsClient.data)

	t := time.Now()
	elapsed := t.Sub(start)
	serverIP := netip.AddrFrom4(dnsClient.address.Addr).String()

	fmt.Println()
	fmt.Printf(";; Query Time: %d msec\n", elapsed.Milliseconds())
	fmt.Printf(";; SERVER %s#%d(%s)\n", serverIP, dnsClient.address.Port, serverIP)
	fmt.Printf(";; WHEN: %s\n", start.Local().Format("Mon Jan 2 15:04:05 MST 2006"))
	fmt.Printf(";; MSG SIZE    rcvd: %d\n", n)
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

func prepareQuery(url string, rrtype string) ([]byte, int) {
	xid, flags := rand.Intn(65535), 0x0100 // recursive

	query := make([]byte, 12)
	binary.BigEndian.PutUint16(query[0:2], uint16(xid))
	binary.BigEndian.PutUint16(query[2:4], uint16(flags))
	binary.BigEndian.PutUint16(query[4:6], uint16(1))

	// Question
	qname, qtype, qclass := []byte{}, make([]byte, 2), []byte{0, 1}

	binary.BigEndian.PutUint16(qtype, uint16(typesMap[rrtype]))

	octets := strings.Split(url, ".")
	if rrtype == "PTR" {
		slices.Reverse(octets)
	}

	for _, o := range octets {
		qname = append(qname, uint8(len(o)))
		qname = append(qname, []byte(o)...)
	}

	if rrtype == "PTR" {
		qname = append(qname, uint8(7))
		qname = append(qname, []byte("in-addr")...)
		qname = append(qname, uint8(4))
		qname = append(qname, []byte("arpa")...)
	}
	qname = append(qname, uint8(0))

	query = append(query, qname...)
	query = append(query, qtype...)
	query = append(query, qclass...)

	return query, xid
}

func validateResponse(p []byte) {
	if (p[2]&0x80)>>7 != 1 {
		log.Fatalln("QR must be 1")
	}

	if (p[3]&0x70)>>4 != 0 {
		log.Fatalln("unexpected z value", (p[3]&0x70)>>4)
	}
}

func readName(p []byte, offset int) ([]string, int, int) {
	labels, i, ptr := []string{}, offset, 0

	for {
		ll := int(p[i])
		if ll == 0 {
			i++
			break
		}

		if p[i]&0xC0 > 0 {
			ptr = int(p[i+1])
			i += 2
			break
		}
		labels = append(labels, string(p[i+1:i+1+ll]))
		i += ll + 1
	}

	return labels, i, ptr
}

func printResponse(p []byte) {
	i := 0
	fmt.Println(";; Got answer")
	xid, aa, rcode, opcode := binary.BigEndian.Uint16(p[0:2]), p[2]&0x04, p[3]&0x10, (p[2]&0x78)>>3

	fmt.Printf(";; --HEADER-- opcode: %s, status: %s, id: %d\n", opcodeMap[int(opcode)], rcodeMap[int(rcode)], xid)
	printFlags(p)

	if aa != 0 {
		aa = 1
	}
	fmt.Printf(" AA: %d\n", aa)

	qdcount := binary.BigEndian.Uint16(p[4:6])
	ancount := binary.BigEndian.Uint16(p[6:8])
	nscount := binary.BigEndian.Uint16(p[8:10])
	arcount := binary.BigEndian.Uint16(p[10:12])

	fmt.Printf(";; QDCOUNT: %d ANCOUNT: %d NSCOUNT: %d ARCOUNT: %d\n\n",
		qdcount, ancount, nscount, arcount)

	qname, i, _ := readName(p, 12)
	fmt.Println(";; QUESTION SECTION")
	name := strings.Join(qname, ".")
	qtype := binary.BigEndian.Uint16(p[i : i+2])
	i += 2

	qclass := binary.BigEndian.Uint16(p[i : i+2])
	i += 2

	if qclass != 1 {
		log.Fatalln("invalid Class, must be 1", qclass)
	}

	fmt.Printf("%6s. %13s %6s \n\n", name, "IN", getQtype(int(qtype)))

	ancounter := 0

	fmt.Println(";; ANSWER")
	for ancounter < int(ancount) {
		labels, ptr := []string{}, 0
		i += 6 // skip name pointer, type and class
		ttl := binary.BigEndian.Uint32(p[i : i+4])
		i += 4

		dl := binary.BigEndian.Uint16(p[i : i+2])
		i += 2
		// ARPA 4 byte IPV4
		if int(dl) == 4 {
			ipaddr := netip.AddrFrom4([4]byte(p[i : i+4]))
			fmt.Printf("%6s. %6d %6s %6s    %s\n",
				name, ttl, "IN", getQtype(int(qtype)), ipaddr.String())
			break
		}

		// first sequence is a pointer 11--------
		if p[i]&0xC0 > 0 {
			labels, i, _ = readName(p, int(p[i+1]))
			i += 2
		} else {
			// regular sequence ending in 0 octect with no message compression
			labels, i, ptr = readName(p, i)

			// terminated by pointer
			if ptr > 0 {
				sublabels := []string{}
				sublabels, _, _ = readName(p, ptr)
				labels = append(labels, sublabels...)
			}
		}

		fmt.Printf("%6s %6d %6s %6s    %s\n", name+".", ttl, "IN", getQtype(int(qtype)), strings.Join(labels, ".")+".")

		ancounter++
	}
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

func getQtype(i int) string {
	for k, v := range typesMap {
		if v == i {
			return k
		}
	}
	log.Fatal("invalid qtype")
	return ""
}

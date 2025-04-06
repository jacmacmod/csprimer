package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/netip"
	"os"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

type qtypeAndClass struct {
	Qtype  uint16
	Qclass uint16
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

func prepareQuery(url string, rrtype string) ([]byte, int) {
	xid, flags := rand.Intn(65535), 0x0100
	var data struct {
		XID        uint16
		Flags      uint16
		Questions  uint16
		Answers    uint16
		Authority  uint16
		Additional uint16
	}
	buf := new(bytes.Buffer)
	data.XID, data.Flags, data.Questions, data.Additional = uint16(xid), uint16(flags), 1, 1
	if err := binary.Write(buf, binary.BigEndian, data); err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	query := buf.Bytes()

	octets := strings.Split(url, ".")
	if rrtype == "PTR" {
		slices.Reverse(octets)
	}

	qname := []byte{}
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

	buf = new(bytes.Buffer)
	qtypeAndClassdataStruct := qtypeAndClass{Qtype: uint16(typeStringToInt(rrtype)), Qclass: uint16(IN)}
	if err := binary.Write(buf, binary.BigEndian, qtypeAndClassdataStruct); err != nil {
		fmt.Println("binary.Write failed:", err)
	}

	qtypeAndClass := buf.Bytes()
	query = append(query, qtypeAndClass...)

	var OPTRR struct {
		Name  uint8
		Type  uint16
		Class uint16
		TTL   uint32
		Rdlen uint16
	}

	buf = new(bytes.Buffer)
	OPTRR.Name, OPTRR.Type, OPTRR.Class, OPTRR.TTL, OPTRR.Rdlen = 0, uint16(OPT), uint16(bufferSize), 0, 0
	if err := binary.Write(buf, binary.BigEndian, OPTRR); err != nil {
		fmt.Println("binary.Write failed:", err)
	}

	optrr := buf.Bytes()
	query = append(query, optrr...)
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

func readName(p []byte, offset int) (string, int) {
	labels, i, nextIdx, offsets := []string{}, offset, 0, make(map[int]struct{})

	for {
		b := p[i]
		if b&0xC0 > 0 {
			if nextIdx == 0 {
				nextIdx = i + 2
			}
			i = int(p[i+1])
			if _, exists := offsets[i]; exists {
				break
			}
			offsets[i] = struct{}{}
		} else if b == 0x00 { // at the end
			if nextIdx == 0 {
				nextIdx = i + 1
			}
			break
		} else {
			ll := int(b)
			labels = append(labels, string(p[i+1:i+1+ll]))
			i += ll + 1
		}
	}

	return strings.Join(labels, ".") + ".", nextIdx
}

func printResponse(p []byte) {
	i := 0
	fmt.Println(";; Got answer")
	xid, aa, rcode, opcode := binary.BigEndian.Uint16(p[0:2]), p[2]&0x04, p[3]&0x10, (p[2]&0x78)>>3
	i += 4
	fmt.Printf(";; --HEADER-- opcode: %s, status: %s, id: %d\n", opcodeMap[int(opcode)], rcodeMap[int(rcode)], xid)
	printFlags(p)

	if aa != 0 {
		aa = 1
	}
	fmt.Printf(" AA: %d\n", aa)

	var q1 struct {
		QDcount uint16
		Ancount uint16
		Nscount uint16
		Arcount uint16
	}
	r := bytes.NewReader(p[i : i+8])
	if err := binary.Read(r, binary.BigEndian, &q1); err != nil {
		fmt.Println("binary.Read err:", err)
	}
	i += 8

	fmt.Printf(";; QUERY: %d ANSWER: %d AUTHORITY: %d ADDITIONAL: %d\n\n",
		q1.QDcount, q1.Ancount, q1.Nscount, q1.Arcount)

	qname, i := readName(p, i)

	var q2 qtypeAndClass

	r2 := bytes.NewReader(p[i : i+4])
	if err := binary.Read(r2, binary.BigEndian, &q2); err != nil {
		fmt.Println("binary.Read err:", err)
	}
	i += 4

	qclass, qtype := getClass(RClass(q2.Qclass)), getResourceType(RType(q2.Qtype))

	fmt.Println(";; QUESTION SECTION")
	fmt.Printf("%6s. %13s %6s \n\n", qname, qclass, qtype)

	// answers
	for anIdx := range q1.Ancount {
		if anIdx == 0 {
			fmt.Println(";; ANSWER")
		}

		i = printRR(p, i)
		anIdx++
	}

	for nsIdx := range q1.Nscount {
		if nsIdx == 0 {
			fmt.Println(";; AUTHORITY ")
		}
		i = printRR(p, i)

		nsIdx++
	}

	// for arIdx := range q1.Arcount {
	// 	if arIdx == 0 {
	// 		fmt.Println(";; ADDITIONAL")
	// 	}
	// 	i = printRR(p, i)

	// 	arIdx++
	// }
}

func printRR(p []byte, i int) int {
	recordName, i := readName(p, i)
	var data struct {
		RecordType  uint16
		RecordClass uint16
		RecordTTL   uint32
		DataLength  uint16
	}
	r := bytes.NewReader(p[i : i+10])
	if err := binary.Read(r, binary.BigEndian, &data); err != nil {
		fmt.Println("binary.Read err: ", err)
	}
	i += 10

	recordType, dataLength, class := RType(data.RecordType), int(data.DataLength), getClass(RClass(data.RecordClass))
	recordData, i := formatRData(p, dataLength, recordType, i)

	fmt.Printf("%6s %6d %6s %6s    %s\n",
		recordName, int(data.RecordTTL), class, getResourceType(recordType), recordData)
	return i
}

func formatRData(p []byte, dl int, recordType RType, i int) (string, int) {
	mailPreference, rdata := 0, ""
	if dl == 4 && recordType == A {
		rdata = netip.AddrFrom4([4]byte(p[i : i+4])).String()
		i += dl
	}

	if dl == 16 && recordType == AAAA {
		rdata = netip.AddrFrom16([16]byte(p[i : i+16])).String()
		i += dl
	}

	if recordType == TXT {
		txt := p[i : i+dl]
		if utf8.Valid(txt) {
			rdata = string(txt)
		} else {
			fmt.Println("invalid txt field")
			os.Exit(1)
		}
		i += dl
	}

	if recordType == SOA {
		mname, i := readName(p, i)
		rname, i := readName(p, i)

		var data struct {
			Serial  uint32
			Refresh uint32
			Retry   uint32
			Expire  uint32
		}
		r := bytes.NewReader(p[i : i+16])
		if err := binary.Read(r, binary.BigEndian, &data); err != nil {
			fmt.Println("binary.Read failed:", err)
		}
		i += 16

		rdata = fmt.Sprintf("%s %s %d %d %d %d",
			mname, rname, int(data.Serial), int(data.Refresh), int(data.Retry), int(data.Expire))
	}

	if recordType == MX {
		mailPreference = int(binary.BigEndian.Uint16(p[i : i+2]))
		i += 2
	}

	if recordType == CNAME || recordType == NS || recordType == MX || recordType == PTR {
		rdata, i = readName(p, i)
	}

	if mailPreference != 0 {
		return fmt.Sprintf("%d %s", mailPreference, rdata), i
	}

	return rdata, i
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

func getResourceType(i RType) string {
	c := TypesMap[i]
	if c == "" {
		fmt.Println("invalid type")
		os.Exit(1)
	}
	return c
}

func getClass(i RClass) string {
	c := ClassMap[i]
	if c == "" || i != IN {
		fmt.Println("invalid class: ", c, i)
		os.Exit(1)
	}
	return c
}

func typeStringToInt(s string) int {
	for k, v := range TypesMap {
		if v == s {
			return int(k)
		}
	}
	log.Fatal("invalid qtype")
	return 0
}

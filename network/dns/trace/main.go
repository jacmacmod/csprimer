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

func main() {
	server := googleHost
	var trace = flag.Bool("trace", false, "flag to show full trace or not")
	var ipAddress = flag.String("x", "", "ip address for reverse lookup")

	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		log.Fatalln("no url or ip specified")
	}

	url, rtype := args[0], A
	if len(args) > 1 {
		// return error more go like
		rtype = typeStringToRtype(strings.ToUpper(args[1]))
	}

	if *ipAddress != "" {
		url = *ipAddress
		if _, err := netip.ParseAddr(url); err == nil {
			rtype = PTR
		} else {
			log.Fatalln("invalid ip address")
		}
	}

	if rtype != PTR && url[len(url)-1] != '.' {
		url = url + "."
	}

	if !*trace {
		DNSQuery(url, rtype, server, "google.com")
	} else {

		Trace(url, rtype, [4]byte{198, 97, 190, 53}, "h.root-servers.net.")
	}
}

// do a full trace with glue records
func Trace(hostname string, rtype RType, hostIP [4]byte, domain string) DNSMessage {
	res := DNSQuery(hostname, rtype, hostIP, domain)
	if len(res.Answers) > 0 || len(res.Authorities) == 0 {
		return res
	}

	auth := res.Authorities[0]

	for _, a := range res.Additional {
		if auth.RData == a.Name && a.Type == A {
			ip := netip.MustParseAddr(a.RData)
			return Trace(hostname, rtype, ip.As4(), a.Name)
		}
	}

	res = Trace(auth.RData[:len(auth.RData)-1], A, hostIP, domain)
	if len(res.Answers) == 0 {
		return res
	}

	ip := netip.MustParseAddr(res.Answers[0].RData)
	return Trace(hostname, rtype, ip.As4(), res.Answers[0].Name)
}

func prepareQuery(url string, rrtype RType) DNSMessage {
	var h Header
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
	h.ID, h.QR, h.RecursionDesired, h.QuestionCount, h.AdditionalRecordCount = int(data.XID), QRQuery, Flag(true), 1, 1
	if err := binary.Write(buf, binary.BigEndian, data); err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	b := buf.Bytes()

	octets := strings.Split(url, ".")
	if rrtype == PTR {
		slices.Reverse(octets)
	}

	qname := []byte{}
	for _, o := range octets {
		if o != "" {
			qname = append(qname, uint8(len(o)))
			qname = append(qname, []byte(o)...)
		}
	}

	if rrtype == PTR {
		qname = append(qname, uint8(7))
		qname = append(qname, []byte("in-addr")...)
		qname = append(qname, uint8(4))
		qname = append(qname, []byte("arpa")...)
	}
	qname = append(qname, uint8(0))

	b = append(b, qname...)

	buf = new(bytes.Buffer)
	qtypeAndClassdataStruct := qtypeAndClass{Qtype: uint16(rrtype), Qclass: uint16(IN)}
	if err := binary.Write(buf, binary.BigEndian, qtypeAndClassdataStruct); err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	qtypeAndClass := buf.Bytes()
	b = append(b, qtypeAndClass...)

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
	b = append(b, optrr...)

	return DNSMessage{
		Header: h,
		Questions: []Question{
			{Qtype: rrtype, Qclass: IN, Name: url},
		},
		Additional: []ResourceRecord{
			{Name: "0", Type: OPT, Class: OPTRRClass},
		},
		Data: b,
	}
}

func DNSQuery(url string, rtype RType, addr [4]byte, domain string) DNSMessage {
	dnsClient := newDNSClient(addr, domain)
	dnsClient.Connect()
	defer dnsClient.Close()
	q := prepareQuery(url, rtype)
	res, n, err := dnsClient.Query(q)
	if err != nil {
		log.Fatalf("DNS Query err: %v", err)
	}

	res.Print(time.Now(), dnsClient, n)

	return res
}

func parseResponse(p []byte) (DNSMessage, error) {
	res, i := DNSMessage{Data: p}, 0
	xid, rcode, opcode := binary.BigEndian.Uint16(p[0:2]), p[3]&0x10, (p[2]&0x78)>>3
	qr, tc, rd, ra, aa := p[2]&0x80, p[2]&0x02, p[2]&0x01, p[3]&0x80, p[2]&0x04
	i += 4

	var countData struct {
		Qcount  uint16
		Ancount uint16
		Nscount uint16
		Arcount uint16
	}
	r := bytes.NewReader(p[i : i+8])
	if err := binary.Read(r, binary.BigEndian, &countData); err != nil {
		fmt.Println("binary.Read err:", err)
	}
	i += 8

	res.Header = Header{
		ID:                    int(xid),
		RCode:                 (Rcode(rcode)),
		OpCode:                Opcode(opcode),
		AuthoritativeAnswer:   toFlag(aa),
		Truncation:            toFlag(tc),
		RecursionDesired:      toFlag(rd),
		RecursionAvailable:    toFlag(ra),
		QR:                    QR(qr),
		QuestionCount:         int(countData.Qcount),
		AnswerCount:           int(countData.Ancount),
		NameServerCount:       int(countData.Nscount),
		AdditionalRecordCount: int(countData.Arcount),
	}

	for qIdx := range countData.Qcount {
		var qname string
		qname, i = readName(p, i)

		var qc qtypeAndClass
		r2 := bytes.NewReader(p[i : i+4])
		if err := binary.Read(r2, binary.BigEndian, &qc); err != nil {
			fmt.Println("binary.Read err:", err)
		}

		q := Question{Name: qname, Qtype: RType(qc.Qtype), Qclass: RClass(qc.Qclass)}
		res.Questions = append(res.Questions, q)
		i += 4
		qIdx++
	}

	for anIdx := range countData.Ancount {
		var rr ResourceRecord
		rr, i = getRR(p, i)
		res.Answers = append(res.Answers, rr)
		anIdx++
	}

	for nsIdx := range countData.Nscount {
		var rr ResourceRecord
		rr, i = getRR(p, i)
		res.Authorities = append(res.Authorities, rr)
		nsIdx++
	}

	for arIdx := range countData.Arcount {
		var rr ResourceRecord
		rr, i = getRR(p, i)
		res.Additional = append(res.Additional, rr)
		arIdx++
	}

	return res, nil
}

func toFlag(b byte) Flag {
	if b > 0 {
		return Flag(true)
	}
	return Flag(false)
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

func getRR(p []byte, i int) (ResourceRecord, int) {
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

	recordType, dataLength, ttl := RType(data.RecordType), int(data.DataLength), int(data.RecordTTL)
	recordData, i := formatRData(p, dataLength, recordType, i)

	rr := ResourceRecord{
		Name:       recordName,
		Type:       recordType,
		TTL:        ttl,
		Class:      RClass(data.RecordClass),
		DataLength: dataLength,
		RData:      recordData,
	}

	return rr, i
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

func (m DNSMessage) Print(start time.Time, dnsClient DNSClient, n int) {
	if len(m.Answers) > 0 {
		fmt.Println(";; Got answer")
	}
	fmt.Printf(";; ->>HEADER<<- opcode: %s, status: %s, id: %d\n", opcodeMap[m.Header.OpCode], rcodeMap[m.Header.RCode], m.Header.ID)

	fmt.Printf(";; flags:")
	if m.Header.QR == QRResponse {
		fmt.Printf(" qr")
	}
	if m.Header.Truncation {
		fmt.Printf(" tc")
	}
	if m.Header.RecursionDesired {
		fmt.Printf(" rd")
	}
	if m.Header.RecursionAvailable {
		fmt.Printf(" ra")
	}
	if m.Header.AuthoritativeAnswer {
		fmt.Printf(" aa")
	}
	fmt.Printf("; ")

	fmt.Printf("QUERY: %d ANSWER: %d AUTHORITY: %d ADDITIONAL: %d\n\n",
		m.Header.QuestionCount, m.Header.AnswerCount, m.Header.NameServerCount, m.Header.AdditionalRecordCount)

	fmt.Println(";; QUESTION SECTION")
	for _, q := range m.Questions {
		fmt.Printf("%6s %11s %6s\n", q.Name, ClassMap[q.Qclass], TypesMap[q.Qtype])
	}

	for i, a := range m.Answers {
		if i == 0 {
			fmt.Println(";; ANSWER")
		}
		printRR(a)
	}

	for i, a := range m.Authorities {
		if i == 0 {
			fmt.Println("\n;; AUTHORITY")
		}
		printRR(a)
	}

	for i, a := range m.Additional {
		if i == 0 {
			fmt.Println("\n;; ADDITIONAL")
		}
		printRR(a)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	serverIP := netip.AddrFrom4(dnsClient.address.Addr).String()

	fmt.Println()
	fmt.Printf(";; Query Time: %d msec\n", elapsed.Milliseconds())
	fmt.Printf(";; SERVER %s#%d(%s)\n", serverIP, dnsClient.address.Port, dnsClient.domain)
	fmt.Printf(";; WHEN: %s\n", start.Local().Format("Mon Jan 2 15:04:05 MST 2006"))
	fmt.Printf(";; MSG SIZE    rcvd: %d\n\n\n", n)
}

func printRR(rr ResourceRecord) {
	fmt.Printf("%6s %6d %6s %6s    %s\n",
		rr.Name, rr.TTL, ClassMap[rr.Class], TypesMap[rr.Type], rr.RData)
}

func typeStringToRtype(s string) RType {
	for k, v := range TypesMap {
		if v == s {
			return k
		}
	}
	log.Fatal("invalid qtype")
	return 0
}

package main

import (
	"encoding/binary"
	"errors"
	"log"
	"syscall"
)

type RType int
type RClass int
type Opcode int
type Rcode int
type Flag bool
type QR int

type DNSMessage struct {
	Header      Header
	Questions   []Question
	Answers     []ResourceRecord
	Authorities []ResourceRecord
	Additional  []ResourceRecord
	Data        []byte
}

type Header struct {
	ID                    int
	QR                    QR
	OpCode                Opcode
	AuthoritativeAnswer   Flag
	Truncation            Flag
	RecursionDesired      Flag
	RecursionAvailable    Flag
	Z                     int
	RCode                 Rcode
	QuestionCount         int
	AnswerCount           int
	NameServerCount       int
	AdditionalRecordCount int
}

type Question struct {
	Name   string
	Qtype  RType
	Qclass RClass
}

type ResourceRecord struct {
	Name       string
	Type       RType
	Class      RClass
	TTL        int
	DataLength int
	RData      string
}

type qtypeAndClass struct {
	Qtype  uint16
	Qclass uint16
}

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
	OPT   RType = 41

	IN         RClass = 1
	CS         RClass = 2
	CH         RClass = 3
	HS         RClass = 4
	OPTRRClass RClass = bufferSize

	Query  Opcode = 0
	IQuery Opcode = 1
	Status Opcode = 2

	NoError        Rcode = 0
	FormatError    Rcode = 1
	ServerError    Rcode = 2
	NameError      Rcode = 3
	NotImplemented Rcode = 4
	Refused        Rcode = 5

	QRQuery    QR = 0
	QRResponse QR = 1
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

	opcodeMap = map[Opcode]string{
		Query:  "QUERY",
		IQuery: "IQUERY",
		Status: "STATUS",
	}

	rcodeMap = map[Rcode]string{
		NoError:        "NOERROR",
		FormatError:    "FORMATERROR",
		ServerError:    "SERVERERROR",
		NameError:      "NAMERERROR",
		NotImplemented: "NOTIMPLEMENTED",
		Refused:        "REFUSED",
	}
)

type DNSClient struct {
	address     syscall.SockaddrInet4
	fd          int
	fromAddress syscall.SockaddrInet4
	data        []byte
}

func newDNSClient(host [4]byte) DNSClient {
	return DNSClient{
		address: syscall.SockaddrInet4{
			Port: dnsPort,
			Addr: host,
		},
	}
}

func (d *DNSClient) Connect() {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
	if err != nil {
		log.Fatalf("Error creating socket %v\n", err)
	}
	d.fd = fd
}

func (d *DNSClient) Close() {
	syscall.Close(d.fd)
}

func (d *DNSClient) Query(q DNSMessage) (DNSMessage, int, error) {
	if err := syscall.Sendto(d.fd, q.Data, 0, &d.address); err != nil {
		return DNSMessage{}, 0, err
	}

	p, bytesRead := make([]byte, bufferSize), 0

	for {
		n, from, err := syscall.Recvfrom(d.fd, p, 0)
		bytesRead += n
		if err != nil {
			return DNSMessage{}, bytesRead, err
		}

		s, ok := from.(*syscall.SockaddrInet4)
		if !ok {
			return DNSMessage{}, bytesRead, errors.New("unable ro parse address")
		}

		if s.Addr != d.address.Addr {
			continue // ignore dns data from other servers
		}

		if int(binary.BigEndian.Uint16(p[0:2])) != q.Header.ID {
			return DNSMessage{}, bytesRead, errors.New("X ID does not match")
		}

		if n < bufferSize {
			break
		}
	}

	validateResponse(p)
	res, _ := parseResponse(p)
	return res, bytesRead, nil
}

func validateResponse(p []byte) {
	if (p[2]&0x80)>>7 != 1 {
		log.Fatalln("QR must be 1")
	}

	if (p[3]&0x70)>>4 != 0 {
		log.Fatalln("unexpected z value", (p[3]&0x70)>>4)
	}
}

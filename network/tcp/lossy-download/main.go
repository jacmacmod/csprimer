package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net/netip"
	"os"
	"sort"
	"time"
)

type PCAPH struct {
	Magicnumber         int
	MajorVersion        int
	MinorVersion        int
	TimeZoneOffset      int
	TimeStampAccuracy   int
	SnapShotLength      int
	LinkLayerHeaderType LinkType
}

type Packet struct {
	Header PacketHeader
	Data   []byte
}

type PacketHeader struct {
	TimeStampUNIX      uint32
	TimeStampMicroNano uint32
	PacketLength       uint32
	UntruncatedLength  uint32
}

type EthernetII struct {
	SourceMac []byte
	DestMac   []byte
	Type      uint16
}

type IPV4Header struct {
	Version       uint8
	IHL           uint8
	Length        uint16
	TTL           uint8
	ID            uint16
	Protocol      uint8
	SourceIP      netip.Addr
	DestinationIP netip.Addr
}

type TCPHeader struct {
	SourcePort           uint16
	DestinationPort      uint16
	SequenceNumber       uint32
	AcknowledgmentNumber uint32
	DataOffset           int
	Reserved             int
	CWR                  Flag
	ECE                  Flag
	URG                  Flag
	ACK                  Flag
	PSH                  Flag
	RST                  Flag
	SYN                  Flag
	FIN                  Flag
	WindowSize           [2]byte
	UrgentPointer        int
	Data                 []byte

	Options any
}

type LinkType int
type ProtocolType int
type Flag bool

const (
	LinkTypeNull     LinkType = 0
	LinkTypeEthernet LinkType = 1

	ProtocolTypeTCP ProtocolType = 6
	ProtocolTypeUDP ProtocolType = 17

	IPV4 uint16 = 0x0800
)

var (
	protocolMap = map[ProtocolType]string{
		ProtocolTypeTCP: "TCP",
		ProtocolTypeUDP: "UDP",
	}
)

func main() {
	f, err := os.Open("lossy.pcap")
	if err != nil {
		log.Fatal("Error opening file:", err)
	}

	pcapHeader, err := parsePCAPHeader(f)
	if err != nil {
		log.Fatal("Error parsing PCAP header:", err)
	}
	pcapHeader.Print()

	count, httpMsgs := 0, map[int][]byte{}

	for {
		p, err := parsePacket(f)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			break
		}

		_, n, err := ParseEthernetII(p.Data)
		if err != nil {
			fmt.Println(err)
			break
		}

		ip, n, err := ParseIPV4(p.Data, n)
		if err != nil {
			fmt.Println(err)
			break
		}

		tcp, err := ParseTCP(p.Data[14+int(ip.IHL)*4:])
		if err != nil {
			fmt.Println(err)
			break
		}

		if !tcp.SYN && tcp.SourcePort == 80 {
			// sp := seqPayload{Seq: int(tcp.SequenceNumber), Payload: tcp.Data}
			payload := tcp.Data
			if parts := bytes.SplitN(payload, []byte("\r\n\r\n"), 2); len(parts) == 2 {
				payload = parts[1]
			}
			httpMsgs[int(tcp.SequenceNumber)] = payload
		}

		count += 1
	}

	fmt.Printf("\n\nNumber of packets %d\n", count)

	jf, err := os.Create("file.jpeg")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer jf.Close()

	keys := make([]int, len(httpMsgs))
	for k := range httpMsgs {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, k := range keys {
		jf.Write(httpMsgs[k])
	}
}

// all fields are in the byte order written by the host
func parsePCAPHeader(f *os.File) (PCAPH, error) {
	p := make([]byte, 24)
	if _, err := f.Read(p); err != nil {
		log.Fatal("Error reading file:", err)
	}

	var pcaph struct {
		Magicnumber         uint32
		MajorVersion        uint16
		MinorVersion        uint16
		TimeZoneOffset      uint32
		TimeStampAccuracy   uint32
		SnapShotLength      uint32
		LinkLayerHeaderType uint32
	}

	r := bytes.NewReader(p)
	if err := binary.Read(r, binary.NativeEndian, &pcaph); err != nil {
		fmt.Println("binary.Read err parsing pcap header:", err)
	}

	if pcaph.Magicnumber != 0xa1b2c3d4 {
		return PCAPH{}, errors.New("magic number not correct")
	}

	if pcaph.LinkLayerHeaderType != uint32(LinkTypeEthernet) {
		return PCAPH{}, errors.New("LLH not ethernet")
	}

	pcaphHeader := PCAPH{
		Magicnumber:         int(pcaph.Magicnumber),
		MajorVersion:        int(pcaph.MajorVersion),
		MinorVersion:        int(pcaph.MinorVersion),
		TimeZoneOffset:      int(pcaph.TimeZoneOffset),
		TimeStampAccuracy:   int(pcaph.TimeStampAccuracy),
		SnapShotLength:      int(pcaph.SnapShotLength),
		LinkLayerHeaderType: LinkTypeEthernet,
	}

	return pcaphHeader, nil
}

func parsePacket(f *os.File) (Packet, error) {
	b := make([]byte, 16)
	if _, err := f.Read(b); err != nil {
		return Packet{}, err
	}

	var ph struct {
		TimeStampUNIX      uint32
		TimeStampMicroNano uint32
		PacketLength       uint32
		UntruncatedLength  uint32
	}

	r := bytes.NewReader(b)
	if err := binary.Read(r, binary.NativeEndian, &ph); err != nil {
		return Packet{}, errors.New(fmt.Sprintf("binary.Read err parsing packet header: %v", err))
	}

	if ph.PacketLength != ph.UntruncatedLength {
		return Packet{}, errors.New("untruncated length and packet length must be the same")
	}

	data := make([]byte, ph.PacketLength)
	if _, err := f.Read(data); err != nil {
		return Packet{}, errors.New(fmt.Sprintf("binary.Read err parsing packet data: %v", err))
	}

	packet := Packet{
		Header: PacketHeader{
			TimeStampUNIX:      ph.TimeStampUNIX,
			TimeStampMicroNano: ph.TimeStampMicroNano,
			PacketLength:       ph.PacketLength,
			UntruncatedLength:  ph.UntruncatedLength,
		},
		Data: data,
	}
	return packet, nil
}

func ParseEthernetII(b []byte) (EthernetII, int, error) {
	e := EthernetII{
		Type:      binary.BigEndian.Uint16(b[12:14]),
		SourceMac: b[:6],
		DestMac:   b[6:12],
	}

	if int(e.Type) < 1536 {
		return e, 0, errors.New("802.3 Ethernet frame type not supported")
	}

	if e.Type != IPV4 {
		return e, 0, errors.New("Ethernet frame type not supported")
	}

	return e, 14, nil
}

func ParseIPV4(b []byte, i int) (IPV4Header, int, error) {
	ip := IPV4Header{
		Version:  uint8(b[i] >> 4),
		IHL:      uint8(b[i] & 0b00001111),
		Length:   binary.BigEndian.Uint16(b[i : i+2]),
		ID:       binary.BigEndian.Uint16(b[i+2 : i+4]),
		TTL:      uint8(b[i+8]),
		Protocol: uint8(b[i+9]),
	}

	if ip.Version != 4 {
		fmt.Println(ip.Version)
		return ip, 0, errors.New("IPV4 Version must be 4")
	}

	if ProtocolType(ip.Protocol) != ProtocolTypeTCP {
		return ip, 0, errors.New("Protocol must be TCP")
	}

	sourceIP, ok := netip.AddrFromSlice(b[26:30])
	if !ok {
		return ip, 0, errors.New("Invalid source IP address")
	}
	ip.SourceIP = sourceIP

	destIP, ok := netip.AddrFromSlice(b[30:34])
	if !ok {
		return ip, 0, errors.New("Invalid destination IP address")
	}
	ip.DestinationIP = destIP

	return ip, 20, nil
}

func ParseTCP(b []byte) (TCPHeader, error) {
	var data struct {
		SourcePort           uint16
		DestinationPort      uint16
		SequenceNumber       uint32
		AcknowledgmentNumber uint32
		Flags                uint16
	}

	r := bytes.NewReader(b[0:14])
	if err := binary.Read(r, binary.BigEndian, &data); err != nil {
		return TCPHeader{}, errors.New(fmt.Sprintf("binary.Read err parsing tcp headers: %v", err))
	}
	dataOffset := int(data.Flags>>12) * 4
	ack := data.Flags&0b10000 > 0
	syn := data.Flags&0b10 > 0
	fin := data.Flags&0b1 > 0

	return TCPHeader{
		SourcePort:           data.SourcePort,
		DestinationPort:      data.DestinationPort,
		SequenceNumber:       data.SequenceNumber,
		AcknowledgmentNumber: data.AcknowledgmentNumber,
		DataOffset:           dataOffset,
		Data:                 b[dataOffset:],
		ACK:                  Flag(ack),
		SYN:                  Flag(syn),
		FIN:                  Flag(fin),
	}, nil
}

func (p PCAPH) Print() {
	fmt.Printf("Magic Number in Little Endian % x\nVersion Number %d.%d\nSnapShot Length %d\nLLH %d\n",
		p.Magicnumber,
		p.MajorVersion,
		p.MinorVersion,
		p.SnapShotLength,
		p.LinkLayerHeaderType,
	)
}

func (p Packet) Print() {
	t := time.Unix(int64(p.Header.TimeStampUNIX), int64(p.Header.TimeStampMicroNano))
	fmt.Printf("--Packet Header--\nTimeStamp: %v\nPacket Length: %d\nUntruncated Length: %d\n",
		t, p.Header.PacketLength, p.Header.UntruncatedLength)
}

func (e EthernetII) Print() {
	fmt.Printf("--Ethernet II Frame--\nDestination MAC % x\nSource MAC % x\nEtherType: IPV4\n", e.DestMac, e.SourceMac)
}

func (h IPV4Header) Print() {
	fmt.Printf("--IPV4 Header--\nIHL: %2d Length: %d ID: %d TTL: %d Protocol: %s SourceIP: %s DestIP: %s\n",
		h.IHL, h.Length, h.ID, h.TTL, protocolMap[ProtocolType(h.Protocol)], h.SourceIP.String(), h.DestinationIP.String())
}

func (tcph TCPHeader) Print() {
	fmt.Printf("--TCP Header--\nsrc: %d dest: %d\nSEQ=%d ACK=%d Offset %d",
		tcph.SourcePort,
		tcph.DestinationPort,
		tcph.SequenceNumber,
		tcph.AcknowledgmentNumber,
		tcph.DataOffset,
	)

	fmt.Printf("\nFlags: ")
	if tcph.SYN {
		fmt.Print("SYN ")
	}
	if tcph.ACK {
		fmt.Print("ACK ")
	}
	if tcph.FIN {
		fmt.Print("FIN ")
	}
	fmt.Println()
	fmt.Println()
}

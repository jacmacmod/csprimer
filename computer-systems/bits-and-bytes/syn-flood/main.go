package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"time"
)

type PCAP struct {
	Magicnumber         uint32
	MajorVersion        uint16
	MinorVersion        uint16
	TimeZoneOffset      uint32
	TimeStampAccuracy   uint32
	SnapShotLength      uint32
	LinkLayerHeaderType uint32
}

type PacketHeader struct {
	TimeStampUNIX      uint32
	TimeStampMicroNano uint32
	PacketLength       uint32
	UntruncatedLength  uint32
}

type TCPHeader struct {
	SourcePort           uint16
	DestinationPort      uint16
	SequenceNumber       uint32
	AcknowledgmentNumber uint32
	DataOffset           int // number of bytes
	flags                uint8
}

func main() {
	f, err := os.Open("synflood.pcap")
	if err != nil {
		panic(err)
	}

	b1 := make([]byte, 24)
	f.Read(b1)

	pcapHeader, err := parsePCAPHeader(b1)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Magic Number in Little Endian % x\nVersion Number %d.%d\nSnapShot Length %d\nLLH %d\n",
		pcapHeader.Magicnumber,
		pcapHeader.MajorVersion,
		pcapHeader.MinorVersion,
		pcapHeader.SnapShotLength,
		pcapHeader.LinkLayerHeaderType,
	)

	count := 0
	acked := 0
	syned := 0

	for {
		phb := make([]byte, 16)
		n, _ := f.Read(phb)
		ph := makePacketHeader(phb)
		if n == 0 {
			break
		}
		count += 1

		if ph.PacketLength != ph.UntruncatedLength {
			log.Fatal("Untruncated length and packet length must be the same")
		}
		p := make([]byte, ph.PacketLength)
		f.Read(p)
		llh := binary.LittleEndian.Uint32(p[:4]) // link layer header

		// https://www.tcpdump.org/linktypes/LINKTYPE_NULL.html
		// link layer header (loopback interface)
		if llh != 2 { // loopback, could be ethernet, etc.
			log.Fatal("ipv4 Version must be 2")
		}

		ihl := (p[4] & 0x0f) << 2
		if ihl != 20 { // assume no options
			log.Fatal("ipv4 header must not contain options")
		}

		tcph := makeTCPHeader(p[24:38])

		syn, ack := tcph.flags&0x0002 > 0, tcph.flags&0x0010 > 0
		sp, dp := int(tcph.SourcePort), int(tcph.DestinationPort)
		s := fmt.Sprintf("%d -> %d", int(tcph.SourcePort), int(tcph.DestinationPort))
		if syn {
			if dp == 80 {
				syned += 1
			}
			s += " SYN"
		}
		if ack {
			if sp == 80 {
				acked += 1
			}
			s += " ACK"
		}
		fmt.Println(s)
	}
	fmt.Printf("\n%d packets parsed Acknowledged %d Initiated %d\n%.2f%% Acked \n",
		count, acked, syned, (float64(acked)/float64(syned))*100)
}

func printPacketHeader(p PacketHeader) {
	t := time.Unix(int64(p.TimeStampUNIX), int64(p.TimeStampMicroNano))
	fmt.Printf("First Packet\nTimeStamp%v\nPacket Length: %d\nUntruncated Length: %d",
		t, p.PacketLength, p.UntruncatedLength)
}

func makePacketHeader(b []byte) PacketHeader {
	return PacketHeader{
		TimeStampUNIX:      binary.LittleEndian.Uint32(b[0:4]),
		TimeStampMicroNano: binary.LittleEndian.Uint32(b[4:8]),
		PacketLength:       binary.LittleEndian.Uint32(b[8:12]),
		UntruncatedLength:  binary.LittleEndian.Uint32(b[12:16]),
	}
}

func makeTCPHeader(b []byte) TCPHeader {
	return TCPHeader{
		SourcePort:           binary.BigEndian.Uint16(b[0:2]),
		DestinationPort:      binary.BigEndian.Uint16(b[2:4]),
		SequenceNumber:       binary.LittleEndian.Uint32(b[4:8]),
		AcknowledgmentNumber: binary.LittleEndian.Uint32(b[8:12]),
		DataOffset:           int((b[12]&0xf0)>>4) << 2,
		flags:                b[13],
	}
}

func printTCPHeader(tcph TCPHeader) {
	fmt.Printf("Source port %d Destination port %d\nsequence number %d\n Acknowledgment number %d\nOffset %d\n",
		tcph.SourcePort,
		tcph.DestinationPort,
		tcph.SequenceNumber,
		tcph.AcknowledgmentNumber,
		tcph.DataOffset,
	)

	fmt.Printf("Flags\n %08b\n", tcph.flags)
}

// all fields are in the byte order written by the host
func parsePCAPHeader(b []byte) (PCAP, error) {
	mn := b[0:4] // magic number -> 0xa1b2c3d4
	// this is big endian, hex literal
	if binary.LittleEndian.Uint32(mn) == 0xa1b2c3d4 {
		pcap := PCAP{
			Magicnumber:         binary.LittleEndian.Uint32(mn),
			MajorVersion:        binary.LittleEndian.Uint16(b[4:6]),
			MinorVersion:        binary.LittleEndian.Uint16(b[6:8]),
			TimeZoneOffset:      binary.LittleEndian.Uint32(b[8:12]),
			TimeStampAccuracy:   binary.LittleEndian.Uint32(b[12:16]),
			SnapShotLength:      binary.LittleEndian.Uint32(b[16:20]),
			LinkLayerHeaderType: binary.LittleEndian.Uint32(b[20:24]),
		}

		if pcap.LinkLayerHeaderType != 0 {
			return pcap, errors.New("LLH not loopback")
		}
		return pcap, nil
	}
	return PCAP{}, errors.New("Unable to parse pcap")
}

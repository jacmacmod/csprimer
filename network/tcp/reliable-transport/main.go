package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"slices"
	"time"
)

type Header struct {
	Seq uint16
	Ack uint16
}

type Message struct {
	Header Header
	Data   []byte
}

type ReliableDelivery struct {
	Seq    int
	Ack    int
	Port   int
	Conn   *net.UDPConn
	Target *net.UDPAddr
}

var loopback = [4]byte{127, 0, 0, 1}

const MaxPayloadSize = 2

func main() {
	var host = flag.Int("P", 9000, "port number")
	flag.Parse()

	isClient := len(os.Args) > 1
	rd := createReliableDelivery(*host)
	rd.Init(isClient)
	defer rd.Conn.Close()

	payload := []byte("hello")
	if len(os.Args) > 1 {
		rd.Send(payload)
	} else {
		for range slices.Chunk(payload, MaxPayloadSize) {
			var msg Message
			for {
				msg, _ = rd.Receive()
				if msg.Header.Ack == uint16(rd.Ack) {
					break
				}

				if msg.Header.Ack < uint16(rd.Ack) && msg.Header.Seq == uint16(rd.Seq) {
					m := createMessage(rd.Seq, rd.Ack, nil)
					n, err := rd.Conn.WriteTo(m, rd.Target)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Printf("sent %d bytes seq=%d ack=%d\n", n, rd.Seq, rd.Ack)
				}
			}

			rd.Ack += 1
			rd.Seq = int(msg.Header.Seq)
			m := createMessage(rd.Seq, rd.Ack, nil)

			n, err := rd.Conn.WriteTo(m, rd.Target)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("sent %d bytes seq=%d ack=%d\n", n, rd.Seq, rd.Ack)
		}
	}
}

func createReliableDelivery(port int) ReliableDelivery {
	return ReliableDelivery{Seq: 0, Ack: 0, Port: port}
}

func (rd *ReliableDelivery) Send(b []byte) {
	chunks := slices.Chunk(b, MaxPayloadSize)
	for c := range chunks {
		var err error
		for {
			if rd.Ack == rd.Seq {
				m := createMessage(rd.Seq, rd.Ack, c)
				n, err := rd.Conn.Write(m)
				if err != nil {
					log.Fatal(err)
				}
				fmt.Printf("sent %d bytes seq=%d ack=%d\n", n, rd.Seq, rd.Ack)
			}

			rd.Conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			var receivedMsg Message
			for (receivedMsg.Header.Ack == 0 && receivedMsg.Header.Seq == 0) ||
				(receivedMsg.Header.Ack != uint16(rd.Ack)+uint16(1) &&
					receivedMsg.Header.Seq != uint16(rd.Seq)) {
				fmt.Println("waiting on response")
				if receivedMsg, err = rd.Receive(); err != nil {
					break
				}
			}

			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Println("No message received in time — timed out")
			}
			rd.Conn.SetReadDeadline(time.Time{})
			if err == nil {
				break
			}
		}
		rd.Seq += 1
		rd.Ack += 1
	}
}

func (rd *ReliableDelivery) Receive() (Message, error) {
	p := make([]byte, MaxPayloadSize+4)
	for {
		n, addr, err := rd.Conn.ReadFromUDP(p)
		if err != nil {
			return Message{}, err
		}
		if rd.Target == nil {
			rd.Target = addr
		}
		if !addr.IP.Equal(rd.Target.IP) {
			continue
		}
		msg := readMsg(p, n)
		fmt.Printf("rcvd %d bytes seq=%d ack=%d msg=%s\n", n, msg.Header.Seq, msg.Header.Ack, string(msg.Data))
		return msg, nil
	}
}

func (rd *ReliableDelivery) Init(isClient bool) {
	addr := &net.UDPAddr{Port: rd.Port, IP: loopback[:]}
	if isClient {
		conn, err := net.DialUDP("udp", nil, addr)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("connect to ", conn.LocalAddr().String())
		rd.Conn = conn
	} else {
		conn, err := net.ListenUDP("udp", addr)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("listening on ", conn.LocalAddr().String())
		rd.Conn = conn
	}
}

// func runServer(s state, p int) {

// 	payload := []byte("hello")
// 	// final := []byte{}
// 	// handle duplicates on server
// 	for {
// 		if int(s.seq) == len(payload) {
// 			break
// 		}
// 		// sleep 1 second

// 		msg := Message{
// 			Header: Header{Seq: s.seq, Ack: s.ack},
// 			Data:   payload[int(s.seq) : int(s.seq)+1],
// 		}
// 		m := createMsg(msg)
// 		n, err = conn.WriteToUDP(m, addr)
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		fmt.Printf("sent %d bytes seq=%d ack=%d\n", n, msg.Header.Seq, msg.Header.Ack)

// 		var receivedMsg Message
// 		fmt.Println(receivedMsg, s)
// 		for receivedMsg.Header.Ack != s.ack+uint16(1) {
// 			n, _, err := conn.ReadFromUDP(b)
// 			if err != nil {
// 				log.Fatal(err)
// 			}
// 			receivedMsg = readMsg(b)
// 			fmt.Printf("rcvd %d bytes seq=%d ack=%d\n", n, msg.Header.Seq, msg.Header.Ack)
// 		}

// 		s.ack = receivedMsg.Header.Ack
// 		s.seq += uint16(1)
// 	}
// }

// func runClient(s state, p int) {
// 	b := make([]byte, 5)
// 	for {
// 		n, _, err := conn.ReadFromUDP(b)
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		msg := readMsg(b)
// 		fmt.Printf("rcvd %d bytes seq=%d ack=%d\n", n, msg.Header.Seq, msg.Header.Ack)

// 		s.ack = msg.Header.Seq + uint16(1)
// 		s.seq = msg.Header.Seq
// 		msg.Header.Ack = s.ack
// 		msg.Data = nil

// 		b = createMsg(msg)
// 		n, err = conn.Write(b)
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 		fmt.Printf("sent %d bytes seq=%d ack=%d\n", n, msg.Header.Seq, msg.Header.Ack)
// 	}
// }

func createMessage(seq, ack int, b []byte) []byte {
	m := make([]byte, 4)
	binary.BigEndian.PutUint16(m[0:2], uint16(seq))
	binary.BigEndian.PutUint16(m[2:4], uint16(ack))
	m = append(m, b...)
	return m
}

func readMsg(b []byte, n int) Message {
	return Message{
		Header: Header{
			Seq: binary.BigEndian.Uint16(b[0:2]),
			Ack: binary.BigEndian.Uint16(b[2:4]),
		},
		Data: b[4:n]}
}

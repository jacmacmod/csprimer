package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"time"
)

var loopback = [4]byte{127, 0, 0, 1}

type header struct {
	Seq uint16
	Ack uint16
}

type msg struct {
	Header header
	Data   []byte
}

type state struct {
	seq uint16
	ack uint16
}

func main() {
	// sender or receiver mode
	// send ACKS or NACKs
	// check sum for bad data
	// sequence number in state
	// send fin when the whole payload has been delivered
	// timeouts to handle dropped packets:wq:WQ

	var server = flag.Bool("server", true, "if udp is a server")
	var host = flag.Int("P", 9000, "port number")
	flag.Parse()

	if *server {
		runServer(state{}, *host)
	} else {
		runClient(state{}, *host)
	}
}

func runServer(s state, p int) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: p, IP: loopback[:]})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	fmt.Println("started server on ", conn.LocalAddr().String())

	// conn.SetDeadline(t time.Time)
	// client connected
	b := make([]byte, 5)
	n, addr, err := conn.ReadFromUDP(b)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("client connected: Recieved %d bytes from %s\n", n, addr)

	payload := []byte("hello")

	for {
		if int(s.seq) == len(payload) {
			break
		}
		// sleep 1 second
		time.Sleep(time.Second)
		m := createMsg(msg{Header: header{Seq: s.seq, Ack: s.ack}, Data: payload[int(s.seq) : int(s.seq)+1]})
		n, err = conn.WriteToUDP(m, addr)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Sent %d bytes msg: %+v\n", n, m)

		n, addr, err := conn.ReadFromUDP(b)
		if err != nil {
			log.Fatal(err)
		}
		recievedMsg := readMsg(b)
		fmt.Printf("Recieved %d bytes from %s %+v\n", n, addr, recievedMsg)

		s.ack = recievedMsg.Header.Ack
		s.seq += 1
	}
}

func runClient(s state, p int) {
	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{Port: p, IP: loopback[:]})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	fmt.Println("started client on ", conn.LocalAddr().String())

	n, err := conn.Write([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Sent hello", n, "bytes")

	b := make([]byte, 5)
	for {
		n, _, err = conn.ReadFromUDP(b)
		if err != nil {
			log.Fatal(err)
		}
		msg := readMsg(b)
		fmt.Printf("Recieved msg %+v: %s\n", msg, msg.Data)

		s.ack = msg.Header.Seq + 1
		s.seq = msg.Header.Seq
		msg.Header.Ack = s.ack
		b = createMsg(msg)

		n, err := conn.Write(b)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Sent %d bytes", n)
	}
}

func createMsg(msg msg) []byte {
	m := make([]byte, 4)
	binary.BigEndian.PutUint16(m[0:2], msg.Header.Seq)
	binary.BigEndian.PutUint16(m[2:4], msg.Header.Ack)
	m = append(m, msg.Data...)
	return m
}

func readMsg(b []byte) msg {
	seq := binary.BigEndian.Uint16(b[0:2])
	ack := binary.BigEndian.Uint16(b[2:4])
	data := b[4:]

	return msg{Header: header{Seq: seq, Ack: ack}, Data: data}
}

// for {
// 	n, from, err := syscall.Recvfrom(fd, b, 0)
// 	if err != nil {
// 		log.Fatalf("Error recieving DNS response: %v", err)
// 	}
// 	if v, ok := from.(*syscall.SockaddrInet4); !ok {
// 		log.Fatalf("Error converting sockaddr: %v", err)
// 	} else {
// 		log.Printf("Recieved %d bytes from port %d\n", n, v.Port)
// 	}

// 	if err = syscall.Sendto(fd, []byte{1}, 0, &from); err != nil {
// 		log.Fatalf("error sending message: %v", err)
// 	}
// 	break
// }

// fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
// if err != nil {
// 	log.Fatalf("error creating socket %v\n", err)
// }
// defer syscall.Close(fd)

// addr := syscall.SockaddrInet4{Addr: loopback, Port: p}
// if err := syscall.Bind(fd, &addr); err != nil {
// 	log.Fatalf("error binding socket %v\n", err)
// }

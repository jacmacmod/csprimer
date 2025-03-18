package main

import (
	"fmt"
	"log"
	"strings"
	"syscall"
)

func main() {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_UDP)
	  if err != nil {
		log.Fatal(err)
	}
	port := 8888
	// replace with ip of current device so that other devices in the local network
	// can connect
	host := [4]byte{127, 0, 0, 1}

	addr := &syscall.SockaddrInet4{Port: port, Addr: host}
	if err := syscall.Bind(fd, addr); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Listening on %d.%d.%d.%d:%d\n", host[0], host[1], host[2], host[3], port)

	b := make([]byte, 1500)
	for {
		n, from, err := syscall.Recvfrom(fd, b, 0)
		if err != nil {
			log.Fatal(err)
		}

		upper := []byte(strings.ToUpper(string(b[0:n])))
		if err = syscall.Sendto(fd, upper, 0, from); err != nil {
			log.Fatal(err)
		}
	}
}

// sender/receiver, ip address and port

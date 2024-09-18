package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"syscall"
)

func main() {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		log.Fatal(err)
	}

	port := 9012
	addr := syscall.SockaddrInet4{Port: port, Addr: [4]byte{0, 0, 0, 0}}

	if err = syscall.Bind(fd, &addr); err != nil {
		log.Fatal(err)
	}

	if err = syscall.Listen(fd, 10); err != nil {
		log.Fatal(err)
	}
	// select epoll block across any of these connections
	defer syscall.Close(fd)
	fmt.Printf("Listening for connections on port %d\n", port)

	for {
		nfd, sa, err := syscall.Accept(fd)
		fmt.Printf("New connection from %T\n", sa)
		if err != nil {
			log.Fatal(err)
		}

		p := make([]byte, 4096)
		n, nsa, err := syscall.Recvfrom(nfd, p, 0)
		if err != nil {
			fmt.Println("Error receiving msg")
			log.Fatal(err)
		}

		if n < 1 {
			break
		}

		b := headersToJson(p, n)
		syscall.Sendto(nfd, []byte("HTTP/1.1 200 ok \r\n\r\n"), 0, nsa)
		syscall.Sendto(nfd, b, 0, nsa)

		if err = syscall.Close(nfd); err != nil {
			log.Fatal(err)
		}
	}
}

func headersToJson(p []byte, n int) []byte {
	req := bytes.Split(p[0:n], []byte("\r\n"))
	reqjson := make(map[string]interface{})

	for _, header := range req[1 : len(req)-2] {
		splitHeader := bytes.Split(header, []byte(": "))
		fmt.Printf("split header %q", splitHeader)
		reqjson[string(splitHeader[0])] = string(splitHeader[1])
	}

	b, err := json.MarshalIndent(reqjson, "", "    ")
	if err != nil {
		log.Fatal("error marashalling json")
	}

	return b
}

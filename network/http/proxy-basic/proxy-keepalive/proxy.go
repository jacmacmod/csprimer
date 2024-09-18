package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"slices"
	"syscall"
)

type HTTPState string

const (
	START   HTTPState = "START"
	HEADERS HTTPState = "HEADERS"
	BODY    HTTPState = "BODY"
	END     HTTPState = "END"
)

type HTTPRequest struct {
	Headers  map[string]string
	State    HTTPState
	Residual []byte
	Method   string
	URI      string
	Version  string
	Body     []byte
}

func main() {
	proxyfd, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	defer syscall.Close(proxyfd)
	syscall.SetsockoptInt(proxyfd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	proxyAddr := syscall.SockaddrInet4{Port: 8001, Addr: [4]byte{0, 0, 0, 0}}
	syscall.Bind(proxyfd, &proxyAddr)
	syscall.Listen(proxyfd, 10)
	fmt.Printf("Accepting new connections on port %d\n", proxyAddr.Port)

	for {
		client, clientAddr, _ := syscall.Accept(proxyfd)
		sa, _ := clientAddr.(*syscall.SockaddrInet4)
		fmt.Printf("New Connection from %v\n", sa.Port)
		if err := handleClientConnection(client); err != nil {
			log.Printf("err: %v\n", err)
		}
	}
}

func handleClientConnection(client int) error {
	for {
		upstream, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
		upstreamAddr := syscall.SockaddrInet4{Port: 9000, Addr: [4]byte{127, 0, 0, 1}}
		syscall.Connect(upstream, &upstreamAddr)
		fmt.Printf("Connected to %v\n", upstreamAddr.Port)

		req := createHTTPRequest()
		close := false
		for req.State != END {
			data := make([]byte, 4096)
			n, _, err := syscall.Recvfrom(client, data, 0)
			fmt.Println(err)
			if err != nil {
				fmt.Println("Error receiving data:", err)
				syscall.Sendto(client, []byte("HTTP/1.1 400 Internal Server Error\r\n\r\n"), 0, nil)
				syscall.Close(upstream)
				syscall.Close(client)
				return errors.New("Error Recieving Data from client")
			}
			fmt.Printf("--> *     %dB\n", n)

			parse(data[:n], &req)
			if n == 0 {
				close = true
				break
			}

			if req.Version != "HTTP/1.1" && req.Version != "HTTP/1.0" {
				syscall.Sendto(client, []byte("HTTP/1.1 505 HTTP Version Not Supported\r\n\r\n"), 0, nil)
				syscall.Close(upstream)
				syscall.Close(client)
				return errors.New("HTTP Version not supported")
			}
			syscall.Sendto(upstream, data[:n], 0, nil)
			fmt.Printf("    * --> %dB\n", n)
			fmt.Printf("req state: %v", req.State)
		}

		if close {
			fmt.Println("closed")
			syscall.Close(upstream)
			return nil
		}

		for {
			res := make([]byte, 1500)
			n, _, _ := syscall.Recvfrom(upstream, res, 0)
			fmt.Printf("    * <-- %dB\n", n)
			if n == 0 {
				break
			}
			syscall.Sendto(client, res[:n], 0, nil)
			fmt.Printf("<-- *     %dB\n", n)
		}

		syscall.Close(upstream)

		if !keepAlive(&req) {
			return nil
		}
	}
}

func createHTTPRequest() HTTPRequest {
	return HTTPRequest{State: START, Headers: make(map[string]string)}
}

func keepAlive(req *HTTPRequest) bool {
	keepAlive := false
	if req.Version == "HTTP/1.1" {
		keepAlive = true
	}

	if req.Headers["Connection"] != "" {
		if req.Version == "HTTP/1.0" && req.Headers["Connection"] == "keep-alive" {
			return true
		}
		if req.Version == "HTTP/1.1" && req.Headers["Connection"] == "close" {
			return false
		}
	}
	fmt.Println(keepAlive)
	return keepAlive
}

func parse(b []byte, req *HTTPRequest) {
	byteString := slices.Concat(req.Residual, b)
	reader := bufio.NewReader(bytes.NewReader(byteString))

	if req.State == START {
		requestLine, err := reader.ReadBytes('\n')
		if err != nil {
			req.Residual = requestLine
			return
		}
		requestLine = bytes.TrimRight(requestLine, " \t\r\n")
		splitRequestLine := bytes.Split(requestLine, []byte(" "))
		req.Method, req.URI, req.Version = string(splitRequestLine[0]), string(splitRequestLine[1]), string(splitRequestLine[2])
		req.State = HEADERS
	}

	if req.State == HEADERS {
		for {
			fieldLine, err := reader.ReadBytes('\n')

			if fieldLine == nil {
				break
			}

			if err != nil {
				req.Residual = fieldLine
				return
			}

			if bytes.Equal(fieldLine, []byte("\r\n")) || bytes.Equal(fieldLine, []byte("\n")) {
				if req.Method == "GET" {
					req.State = END
				} else {
					req.State = BODY
				}
				break
			}

			fieldLine = bytes.TrimRight(fieldLine, " \t\r\n")
			splitHeader := bytes.Split(fieldLine, []byte(": "))
			req.Headers[string(splitHeader[0])] = string(splitHeader[1])
		}
	}

	if req.State == BODY {
		buf := make([]byte, 500)
		for {
			if n, _ := reader.Read(buf); n != 0 {
				req.Body = append(req.Body, buf[0:n]...)
			} else {
				break
			}
		}
	}
}

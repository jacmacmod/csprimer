xpackage main

import (
	"bufio"
	"bytes"
	"fmt"
	"slices"
	"strconv"
	"strings"
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
type HTTPError string

const (
	HTTPError400 HTTPError = "400 HTTP/1.1 Bad Request\r\n\r\n"
	HTTPError502 HTTPError = "502 HTTP/1.1 Bad Gateway\r\n\r\n"
	HTTPError505 HTTPError = "505 HTTP/1.1 HTTP Version Not Supported\r\n\r\n"
)

func main() {
	proxyfd, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	syscall.SetsockoptInt(proxyfd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	proxyAddr := syscall.SockaddrInet4{Port: 8010, Addr: [4]byte{0, 0, 0, 0}}
	syscall.Bind(proxyfd, &proxyAddr)
	syscall.Listen(proxyfd, 10)
	fmt.Printf("Accepting new connections on port %d\n", proxyAddr.Port)

	for {
		client, clientAddr, _ := syscall.Accept(proxyfd)
		sa, _ := clientAddr.(*syscall.SockaddrInet4)
		fmt.Printf("New Connection from %v\n", sa.Port)
		handleClientConnection(client)
	}
}

func handleClientConnection(client int) {
	
	for {
		upstream, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
		upstreamAddr := syscall.SockaddrInet4{Port: 9000, Addr: [4]byte{127, 0, 0, 1}}
		err := syscall.Connect(upstream, &upstreamAddr)
		defer syscall.Close(upstream)
		if err != nil {
			syscall.Sendto(client, []byte(HTTPError502), 0, nil)
			fmt.Printf("<-- *   %db\n", len(HTTPError502))
			fmt.Println("error connecting to upstream")
			break
		}
		fmt.Printf("Connected to %v\n", upstreamAddr.Port)

		req := createHTTPRequest()
		close := false
		for req.State != END {
			data := make([]byte, 4096)
			n, _, err := syscall.Recvfrom(client, data, 0)
			fmt.Printf("--> *    %dB\n", n)
			if err != nil {
				fmt.Printf("error recieving data from client: %s", err)
				return
			}

			if n <= 0 || (n == 1 && req.State == START && data[0] == '\n') {
				close = true
				break
			}
			parse(data[:n], &req)
			if req.Version != "HTTP/1.1" && req.Version != "HTTP/1.0" {
				syscall.Sendto(client, []byte(HTTPError505), 0, nil)
				fmt.Printf("<-- *   %dB\n", len(HTTPError505))
				fmt.Printf("HTTP Version Not Supported")
				break
			}
			syscall.Sendto(upstream, data[:n], 0, nil)
			fmt.Printf("    * --> %dB\n", n)
		}

		if close {
			return
		}

		res := make([]byte, 1500)
		n, _, _ := syscall.Recvfrom(upstream, res, 0)
		reshttp := createHTTPRequest()
		parse(res[:n], &reshttp)
		fmt.Printf("    * <-- %dB\n", n)
		contentLength, err := strconv.Atoi(reshttp.Headers["Content-Length"])
		if err != nil {
			syscall.Sendto(client, []byte(HTTPError400), 0, nil)
			fmt.Printf("<-- *   %dB\n", len(HTTPError400))
			fmt.Printf("unable to parse Content-Length")
			syscall.Close(upstream)
			break
		}

		bytesRead := n
		syscall.Sendto(client, res[:n], 0, nil)
		fmt.Printf("<-- *    %dB\n", n)
		for contentLength > bytesRead {
			n, _, _ = syscall.Recvfrom(upstream, res, 0)
			fmt.Printf("    * <-- %dB\n", n)
			if n <= 0 {
				break
			}
			bytesRead += n
			syscall.Sendto(client, res[:n], 0, nil)
			fmt.Printf("<-- *    %dB\n", n)
		}
		syscall.Close(upstream)

		if !keepAlive(&req) {
			syscall.Close(client)
			return
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
		if req.Version == "HTTP/1.0" && strings.ToLower(req.Headers["Connection"]) == "keep-alive" {
			keepAlive = true
		}
		if req.Version == "HTTP/1.1" && strings.ToLower(req.Headers["Connection"]) == "close" {
			keepAlive = false
		}
	}
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

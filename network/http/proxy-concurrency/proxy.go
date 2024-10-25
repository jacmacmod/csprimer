package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
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
type HTTPRedirect string

const (
	HTTPRedirect304 HTTPRedirect = "304 Not Modified"
	HTTPError400    HTTPError    = "400 HTTP/1.1 Bad Request\r\n\r\n"
	HTTPError502    HTTPError    = "502 HTTP/1.1 Bad Gateway\r\n\r\n"
	HTTPError505    HTTPError    = "505 HTTP/1.1 HTTP Version Not Supported\r\n\r\n"
)

func RunProxyServer() {
	proxyAddr := syscall.SockaddrInet4{Port: 8010, Addr: [4]byte{0, 0, 0, 0}}
	upstreamAddr := syscall.SockaddrInet4{Port: 9000, Addr: [4]byte{127, 0, 0, 1}}

	proxy, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	syscall.SetsockoptInt(proxy, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	syscall.SetNonblock(proxy, true)
	syscall.Bind(proxy, &proxyAddr)
	syscall.Listen(proxy, 10)
	fmt.Printf("Accepting new connections on port %d\n", proxyAddr.Port)

	var r unix.FdSet
	r.Zero()
	r.Set(proxy)
	upstreamForClient := make(map[int]int)
	reqForClient := make(map[int]*HTTPRequest)

	for {
		rReady := r
		if v, err := unix.Select(unix.FD_SETSIZE, &rReady, nil, nil, nil); err != nil {
			log.Fatal(v, err)
		}

		for s := 0; s < unix.FD_SETSIZE; s++ {
			if rReady.IsSet(s) {
				if s == proxy {
					// accept connection from client
					if client, clientAddr, err := syscall.Accept(s); err == nil {
						syscall.SetNonblock(client, true)
						sa, _ := clientAddr.(*syscall.SockaddrInet4)
						fmt.Printf("New Connection from %d with fd_%d\n", sa.Port, client)
						r.Set(client)
					}
				} else {
					// connect to upstream
					if _, ok := upstreamForClient[s]; !ok {
						upstream, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
						if err != nil {
							log.Fatal("3resource unavailable", err)
						}
						if err := syscall.Connect(upstream, &upstreamAddr); err != nil {
							log.Fatal("error connecting to upstrean", err)
						}
						upstreamForClient[s] = upstream
						fmt.Printf("fd_%d Connected to upstream %d\n", s, upstream)
					}
					upstream := upstreamForClient[s]

					if _, ok := reqForClient[s]; !ok {
						newReq := createHTTPRequest()
						reqForClient[s] = &newReq
					}
					req := reqForClient[s]

					// handle message from client
					data := make([]byte, 4096)
					n, _, err := syscall.Recvfrom(s, data, 0)
					fmt.Printf("--> * %dB\n", n)
					if err != nil {
						log.Fatal("error recieving data from client", err)
					}

					if n <= 0 || (n == 1 && req.State == START && data[0] == '\n') {
						syscall.Close(s)
						fmt.Printf("Closing client %d\n", s)
						r.Clear(s)
						delete(upstreamForClient, s)
						delete(reqForClient, s)
						break
					}

					if n > 0 {
						parse(data[:n], req)
						err := syscall.Sendto(upstream, data[:n], 0, nil)
						fmt.Printf("    * --> %dB\n", n)
						if err != nil {
							log.Fatalf("error sending to upstream fd_%d: %v", upstream, err)
						}
					}

					// handle message(s) from upstream
					if req.State == END {
						res := make([]byte, 1500)
						n, _, _ := syscall.Recvfrom(upstream, res, 0)
						fmt.Printf("    * <-- %dB\n", n)
						reshttp := createHTTPRequest()
						parse(res[:n], &reshttp)
						contentLength, err := strconv.Atoi(reshttp.Headers["Content-Length"])
						if strings.Contains(string(res[:n]), string(HTTPRedirect304)) {
							syscall.Sendto(s, res[:n], 0, nil)
						} else if err != nil {
							syscall.Sendto(s, []byte(HTTPError400), 0, nil)
							fmt.Printf("<-- *   %dB\n", len(HTTPError400))
							fmt.Printf("unable to parse Content-Length")
							syscall.Close(upstream)
							break
						} else {
							bytesRead := n
							syscall.Sendto(s, res[:n], 0, nil)
							fmt.Printf("<-- *    %dB\n", n)

							for contentLength > bytesRead {
								n, _, _ = syscall.Recvfrom(upstream, res, 0)
								fmt.Printf("    * <-- %dB\n", n)
								if n <= 0 {
									break
								}
								bytesRead += n
								syscall.Sendto(s, res[:n], 0, nil)
								fmt.Printf("<-- *    %dB\n", n)
							}
						}

						syscall.Close(upstream)
						delete(upstreamForClient, s)
						delete(reqForClient, s)

						if !keepAlive(req) {
							syscall.Close(s)
							r.Clear(s)
						}
					}
				}
			}
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

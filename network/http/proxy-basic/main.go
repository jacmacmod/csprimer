package main

import (
	"fmt"
	"syscall"
)

func main() {
	proxyfd, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	defer syscall.Close(proxyfd)
	syscall.SetsockoptInt(proxyfd, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)

	proxyAddr := syscall.SockaddrInet4{Port: 8001, Addr: [4]byte{0, 0, 0, 0}}
	syscall.Bind(proxyfd, &proxyAddr)
	syscall.Listen(proxyfd, 10)
	fmt.Printf("Accepting connections on port %d\n", proxyAddr.Port)

	for {
		client, clientAddr, _ := syscall.Accept(proxyfd)
		sa, _ := clientAddr.(*syscall.SockaddrInet4)
		fmt.Printf("New Connection from %v\n", sa.Port)

		buf := make([]byte, 1500)
		n, _, _ := syscall.Recvfrom(client, buf, 0)
		fmt.Printf("--> *     %dB\n", n)

		upstream, _ := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
		upstreamAddr := syscall.SockaddrInet4{Port: 9000, Addr: [4]byte{127, 0, 0, 1}}
		syscall.Connect(upstream, &upstreamAddr)
		fmt.Printf("Connected to %v\n", upstreamAddr.Port)
		syscall.Sendto(upstream, buf[:n], 0, nil)
		fmt.Printf("    * --> %dB\n", n)

		for {
			res := make([]byte, 1500)
			n, _, _ = syscall.Recvfrom(upstream, res, 0)
			if n == 0 {
				break
			}
			fmt.Printf("    * <-- %dB\n", n)
			syscall.Sendto(client, res[:n], 0, nil)
			fmt.Printf("<-- *     %dB\n", n)
		}
		syscall.Close(upstream)
		syscall.Close(client)
	}
}

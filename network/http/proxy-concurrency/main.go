package main

import (
	"fmt"
	"log"
	"syscall"

	"golang.org/x/sys/unix"
)

func main() {
	RunProxyServer()
}

func test_simple() {
	listener, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		log.Fatal(err)
	}

	syscall.SetNonblock(listener, true)
	syscall.Bind(listener, &syscall.SockaddrInet4{Port: 10000, Addr: [4]byte{0, 0, 0, 0}})
	syscall.Listen(listener, 10)
	fmt.Println("listening on port 10000")
	var r, w unix.FdSet
	r.Zero()
	w.Zero()
	r.Set(listener)
	toSend := make(map[int]syscall.Sockaddr)
	for {
		rReady, wReady := r, w
		if val, _ := unix.Select(unix.FD_SETSIZE, &rReady, &wReady, nil, nil); val < 0 {
			log.Panicln("val")
		}

		for i := 0; i < unix.FD_SETSIZE; i++ {
			if i == listener {
				client, _, _ := syscall.Accept(i)
				fmt.Printf("new connection with fd %d\n", client)
				syscall.SetNonblock(client, true)
				r.Set(client)
			} else {
				p := make([]byte, 4096)
				n, nsa, err := syscall.Recvfrom(i, p, 0)
				fmt.Printf("err: %v n: %d\n", err, n)
				if n > 0 {
					fmt.Printf("%s\n", p[:n])
					toSend[i] = nsa
					w.Set(i)
				} else {
					syscall.Close(i)
				}
				r.Clear(i)
			}
		}

		for i := 0; i < unix.FD_SETSIZE; i++ {
			if w.IsSet(i) {
				w.Clear(i)
				syscall.Sendto(i, []byte("HTTP/1.0 200 ok \r\n\r\nhi!"), 0, toSend[i])
				delete(toSend, i)
				syscall.Close(i)
			}
		}
	}
}

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/netip"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

const (
	maxHopCount    = 64
	startProbePort = 33435
)

func main() {
	printAS := flag.Bool("a", false, "show autonomous system information")
	flag.Parse()
	args := flag.CommandLine.Args()
	if len(args) < 1 {
		log.Fatal("no url provided")
	}

	host, destPort := args[0], startProbePort
	var sliceAS []AS
	if *printAS {
		sliceAS = loadAutonomousSystem()
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		log.Fatal(err)
	}
	var dst net.IPAddr
	for _, ip := range ips {
		if ip.To4() != nil {
			dst.IP = ip
			fmt.Printf("traceroute to %v (%s), %d hops max, 40 byte packets\n", host, dst.IP, maxHopCount)
			break
		}
	}
	if dst.IP == nil {
		log.Fatal("no A record found")
	}

	udpConn, err := net.ListenPacket("udp4", "0.0.0.0:")
	if err != nil {
		log.Fatal(err)
	}
	pudp := ipv4.NewPacketConn(udpConn)
	udpAddr := net.UDPAddr{IP: dst.IP, Port: destPort}
	defer pudp.Close()

	icmpConn, err := net.ListenPacket("ip4:icmp", "0.0.0.0") // ICMP for IPv4
	if err != nil {
		log.Fatal("could not connect ICMP ", err)
	}
	iConn := ipv4.NewPacketConn(icmpConn)
	defer iConn.Close()

	rb := make([]byte, 1500)
	var prevPeer net.Addr
	for i := 1; i <= maxHopCount; i++ {
		fmt.Printf("%-3d", i)
		for j := range 3 {
			if err := pudp.SetTTL(i); err != nil {
				log.Fatal(err)
			}
			begin := time.Now()
			if _, err := pudp.WriteTo(nil, nil, &udpAddr); err != nil {
				log.Fatal(err)
			}
			if err := iConn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
				log.Fatal(err)
			}
			n, _, peer, err := iConn.ReadFrom(rb)
			if err != nil {
				if err, ok := err.(net.Error); ok && err.Timeout() {
					fmt.Printf("* ")
					continue
				}
				log.Fatal(err)
			}
			rm, err := icmp.ParseMessage(1, rb[:n]) // 1 = ICMPv4
			if err != nil {
				log.Fatal(err)
			}

			rtt := time.Since(begin)
			rttMs := float64(rtt) / float64(time.Millisecond)
			host := peer.String()
			if names, _ := net.LookupAddr(peer.String()); len(names) > 0 {
				host = names[0]
			}

			if rm.Type == ipv4.ICMPTypeTimeExceeded ||
				rm.Type == ipv4.ICMPTypeDestinationUnreachable {
				if prevPeer != nil && prevPeer.String() == peer.String() {
					fmt.Printf(" %-2.3f ms", rttMs)
				} else {
					if j > 0 { // padding for formatting
						fmt.Print("\n   ")
					}
					if *printAS {
						asID := 0
						for _, as := range sliceAS {
							if as.CIDR.Contains(netip.MustParseAddr(peer.String())) && as.ID != 0 {
								asID = as.ID
								break
							}
						}
						fmt.Printf("[AS%d] ", asID)
					}
					fmt.Printf("%-2s (%s)  %2.3f ms ", host, peer.String(), rttMs)
				}
			}
			if rm.Type == ipv4.ICMPTypeDestinationUnreachable {
				return
			}
			prevPeer = peer
			destPort += 1
			udpAddr.Port = destPort
		}
		prevPeer = nil
		fmt.Println()
	}
}

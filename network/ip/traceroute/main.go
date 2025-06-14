package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"sort"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

const (
	maxHopCount = 64
)

func main() {
	printAS := flag.Bool("a", false, "show autonomous system information")
	flag.Parse()
	args := flag.CommandLine.Args()
	if len(args) < 1 {
		log.Fatal("no url provided")
	}

	host := args[0]
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

	c, err := net.ListenPacket("ip4:icmp", "0.0.0.0") // ICMP for IPv4
	if err != nil {
		log.Fatal("could not connect ICMP ", err)
	}
	defer c.Close()
	p := ipv4.NewPacketConn(c)

	wm := icmp.Message{
		Type: ipv4.ICMPTypeEcho,
		Code: 0,
		Body: &icmp.Echo{
			ID:   os.Getpid() & 0xffff,
			Data: []byte("HELLO-R-U-THERE"),
		},
	}

	rb := make([]byte, 1500)
	var prevPeer net.Addr
	for i := 1; i <= maxHopCount; i++ {
		fmt.Printf("%-3d", i)
		j := 0
		for {
			wm.Body.(*icmp.Echo).Seq = i
			wb, err := wm.Marshal(nil)
			if err != nil {
				log.Fatal(err)
			}
			if err := p.SetTTL(i); err != nil {
				log.Fatal(err)
			}
			begin := time.Now()
			if _, err := p.WriteTo(wb, nil, &dst); err != nil {
				log.Fatal(err)
			}
			if err := p.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
				log.Fatal(err)
			}
			n, _, peer, err := p.ReadFrom(rb)
			if err != nil {
				if err, ok := err.(net.Error); ok && err.Timeout() {
					fmt.Printf("* ")
					break
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
				rm.Type == ipv4.ICMPTypeEchoReply ||
				rm.Type == ipv4.ICMPTypeDestinationUnreachable {

				if prevPeer != nil && prevPeer.String() == peer.String() {
					fmt.Printf(" %-2.3f ms", rttMs)
				} else {
					if j > 0 { // padding for formatting
						fmt.Print("\n   ")
					}
					if *printAS {
						printAutonomousSystemInfo(sliceAS, peer)
					}
					fmt.Printf("%-2s (%s)  %2.3f ms ", host, peer.String(), rttMs)
				}

				if rm.Type == ipv4.ICMPTypeEchoReply {
					return
				}
			}

			prevPeer = peer
			j++
		}
		prevPeer = nil
		fmt.Println()
	}
}

func printAutonomousSystemInfo(sliceAS []AS, peer net.Addr) {
	asID, peerIPv4 := 0, netip.MustParseAddr(peer.String())
	i := sort.Search(len(sliceAS), func(i int) bool {
		as := sliceAS[i]
		return binary.BigEndian.Uint32(as.CIDR.Addr().AsSlice()) > binary.BigEndian.Uint32(peerIPv4.AsSlice())
	})

	if i > 0 && i < len(sliceAS) && sliceAS[i-1].CIDR.Contains(peerIPv4) {
		asID = sliceAS[i-1].ID
	}
	fmt.Printf("[AS%d] ", asID)
}

package main

import (
	"bufio"
	"log"
	"net/netip"
	"os"
	"strconv"
	"strings"
)

type AS struct {
	ID      int
	Start   netip.Addr
	End     netip.Addr
	CIDR    netip.Prefix
	Name    string
	Country string
	BitsLen int
}

func loadAutonomousSystem() []AS {
	asnDB := "ip2asn-v4.tsv"
	f, err := os.Open(asnDB)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	// asmap := make(map[netip.Prefix][]AS)
	var asSlice []AS
	r := bufio.NewReader(f)
	scanner := bufio.NewScanner(r)
	i := 0
	for scanner.Scan() {
		i++
		line := scanner.Text()
		lineSplit := strings.Split(line, "\t")
		startIP := netip.MustParseAddr(lineSplit[0])
		endIP := netip.MustParseAddr(lineSplit[1])
		startIPV4 := startIP.AsSlice()
		endIPV4 := endIP.AsSlice()
		bitsLen := 0
		for i, b := range startIPV4 {
			bitIdx := 7
			for bitIdx >= 0 {
				leftBit, rightBit := b>>byte(bitIdx), endIPV4[i]>>byte(bitIdx)
				if (leftBit ^ rightBit) == 0 {
					bitsLen++
				} else {
					break
				}
				bitIdx--
			}
		}

		ID, err := strconv.Atoi(lineSplit[2])
		if err != nil {
			log.Fatal(err)
		}
		as := AS{
			ID:      ID,
			Start:   startIP,
			End:     endIP,
			Country: lineSplit[3],
			Name:    lineSplit[4],
			BitsLen: bitsLen,
			CIDR:    netip.PrefixFrom(startIP, bitsLen),
		}
		//	asmap[ID] = append(asmap[ID], as)
		asSlice = append(asSlice, as)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal("reading standard input:", err)
	}
	return asSlice
}

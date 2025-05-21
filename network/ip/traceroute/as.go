package main

import (
	"bufio"
	"encoding/binary"
	"log"
	"net/netip"
	"os"
	"sort"
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
	MaskInt int // CIDR IP mask as int for fast lookup
}

func loadAutonomousSystem() []AS {
	asnDB := "ip2asn-v4.tsv" // https://iptoasn.com
	f, err := os.Open(asnDB)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	scanner := bufio.NewScanner(r)
	var asSlice []AS
	for scanner.Scan() {
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
		CIDR := netip.PrefixFrom(startIP, bitsLen)
		mask := int(binary.BigEndian.Uint32(CIDR.Addr().AsSlice()))
		as := AS{
			ID:      ID,
			Start:   startIP,
			End:     endIP,
			Country: lineSplit[3],
			Name:    lineSplit[4],
			CIDR:    CIDR,
			MaskInt: mask,
		}
		asSlice = append(asSlice, as)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal("reading standard input:", err)
	}

	sort.Slice(asSlice, func(i, j int) bool {
		return asSlice[i].MaskInt < asSlice[j].MaskInt
	})

	return asSlice
}

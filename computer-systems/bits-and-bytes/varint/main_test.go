package main

import (
	"testing"
)

func TestVarint(t *testing.T) {
	for i := uint64(1); i < uint64(30<<1); i++ {
		if val := decode(encode(i)); val != i {
			t.Errorf("wanted %d got %d", i, val)
		}
	}
}

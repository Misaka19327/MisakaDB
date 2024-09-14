package main

import (
	"math/rand"
	"testing"
	"time"
)

func TestGoto(t *testing.T) {
	var i int
Test:
	i = rand.New(rand.NewSource(time.Now().UnixNano())).Intn(50)
	if i < 5 {
		t.Log(i)
	} else {
		t.Log(i)
		goto Test
	}
}

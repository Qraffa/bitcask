package main

import (
	"fmt"
	"math"
	"testing"
)

func TestEntry(t *testing.T) {
	e := NewEntry([]byte("key"), []byte("value"))
	_, bs := e.Encode()
	re := Decode(bs)
	fmt.Println(*re)
	fmt.Println(string(re.key), string(re.value))

	fmt.Print(math.MaxInt64 / (1 << 30))
	fmt.Println("GB")

	fmt.Println(1 << 30)
}

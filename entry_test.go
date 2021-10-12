package main

import (
	"fmt"
	"testing"
)

func TestEntry(t *testing.T) {
	e := NewEntry([]byte("key"), []byte("value"), PUT)
	_, bs := e.Encode()
	fmt.Println(e.crc)
	re := Decode(bs)
	fmt.Println(*re)
	fmt.Println(string(re.key), string(re.value))
}

func TestMeta(t *testing.T) {
	e := NewEntry([]byte("key"), []byte("value"), PUT)
	_, bs := e.Encode()
	re := &Entry{}
	re.DecodeMeta(bs)
	fmt.Println(*re)
}

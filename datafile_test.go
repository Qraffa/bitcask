package main

import (
	"fmt"
	"os"
	"testing"
)

func TestDatafile(t *testing.T) {
	df, err := NewDataFile(99, true)
	if err != nil {
		panic(err)
	}

	e := NewEntry([]byte("key"), []byte("value"))

	if _, err := df.Write(e); err != nil {
		panic(err)
	}
	fmt.Println(e.crc)

	re, err := df.ReadAt(0)
	if err != nil {
		panic(err)
	}
	fmt.Println(re.crc, string(re.key), string(re.value))
}

func TestRead(t *testing.T) {
	df, err := NewDataFile(99, true)
	if err != nil {
		panic(err)
	}

	re, err := df.ReadAt(145)
	if err != nil {
		panic(err)
	}
	fmt.Println(re.crc, string(re.key), string(re.value))
}

func TestFileSize(t *testing.T) {
	fi, err := os.Stat("tmp")
	if err != nil {
		panic(err)
	}
	fmt.Println(fi.Size())
}

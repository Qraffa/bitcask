package main

import (
	"fmt"
	"os"
	"path"
	"testing"
)

func TestDatafile(t *testing.T) {
	df, err := NewDataFile(defaultDir, 99, true)
	if err != nil {
		panic(err)
	}

	e := NewEntry([]byte("key"), []byte("value"), PUT)

	if _, err := df.Write(e); err != nil {
		panic(err)
	}
	fmt.Println(e.crc)

	_, re, err := df.ReadAt(0)
	if err != nil {
		panic(err)
	}
	fmt.Println(re.crc, string(re.key), string(re.value))
}

func TestRead(t *testing.T) {
	df, err := NewDataFile(defaultDir, 99, true)
	fmt.Println(df.offset)
	if err != nil {
		panic(err)
	}

	_, re, err := df.ReadAt(0)
	if err != nil {
		panic(err)
	}
	fmt.Println(re.crc, string(re.key), string(re.value))
}

func TestFileSize(t *testing.T) {
	fi, err := os.Stat(path.Join(defaultDir, fmt.Sprintf(dataFilePrefix, 99)))
	if err != nil {
		panic(err)
	}
	fmt.Println(fi.Size())
}

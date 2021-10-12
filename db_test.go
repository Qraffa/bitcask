package main

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestDB(t *testing.T) {
	db, err := Open("")
	if err != nil {
		panic(err)
	}
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		panic(err)
	}
	if err := db.Put([]byte("key1"), []byte("value2")); err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("key"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))
	val, err = db.Get([]byte("key1"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))
}

func TestGlob(t *testing.T) {
	pattern := fmt.Sprintf("%s/%s", defaultDir, dataFilePattern)
	fmt.Println(pattern)
	fs, err := filepath.Glob(pattern)
	if err != nil {
		panic(err)
	}
	fmt.Println(fs)
}

func TestDBload(t *testing.T) {
	db, err := Open("")
	if err != nil {
		panic(err)
	}
	fmt.Println(db.Keys())

	val, err := db.Get([]byte("key"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))

	val, err = db.Get([]byte("key1"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))
}

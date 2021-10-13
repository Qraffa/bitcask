package main

import (
	"fmt"
	"os"
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

func TestFilename(t *testing.T) {
	file, _ := os.Open("")
	println(file.Name())
}

func TestMerge(t *testing.T) {
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

	if err := db.merge(); err != nil {
		panic(err)
	}
	fmt.Println(db.Keys())
	if err := db.Put([]byte("key3"), []byte("value3")); err != nil {
		panic(err)
	}
	if err := db.Put([]byte("key4"), []byte("value4")); err != nil {
		panic(err)
	}
	fmt.Println(db.Keys())
}

func TestNewFile(t *testing.T) {
	newfile := getNewFileName("/tmp/bitcask/tmp_db", "/tmp/bitcask", "/tmp/bitcask/tmp_db/bitcask.data.0", 3)
	fmt.Println(newfile)
}

func TestOpen(t *testing.T) {
	// open a not exist file with read-only
	_, err := os.OpenFile("not_found", os.O_RDONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
}

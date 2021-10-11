package main

import (
	"fmt"
	"testing"
)

func TestDB(t *testing.T) {
	db, err := Open()
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

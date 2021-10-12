package main

import (
	"fmt"
	"testing"
)

func TestFindid(t *testing.T) {
	id := getFileID("bitcask.data.99")
	fmt.Println(id)
}
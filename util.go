package main

import (
	"strconv"
	"strings"
)

func getFileID(file string) int64 {
	idx := strings.LastIndex(file, ".")
	fileID, _ := strconv.Atoi(file[idx+1:])
	return int64(fileID)
}

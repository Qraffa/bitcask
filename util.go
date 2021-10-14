package main

import (
	"path"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		ForceColors: true,
	})
}

func getFileID(file string) int64 {
	idx := strings.LastIndex(file, ".")
	fileID, _ := strconv.Atoi(file[idx+1:])
	return int64(fileID)
}

func getOriginFileName(dir, file string) string {
	return strings.TrimPrefix(file, dir)
}

func getNewFileName(oldDir, newDir, filename string, delta int64) string {
	file := getOriginFileName(oldDir, filename)
	id := getFileID(file)
	newfile := strings.Replace(file, strconv.Itoa(int(id)), strconv.Itoa(int(id+delta)), -1)
	return path.Join(newDir, newfile)
}

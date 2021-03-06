package main

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path"
)

const (
	dataFilePattern = "bitcask.data.*"
	dataFilePrefix  = "bitcask.data.%d"
)

type DataFile struct {
	f        *os.File
	fileID   int64
	offset   int64
	isActive bool
}

func NewDataFile(dir string, id int64, active bool) (*DataFile, error) {
	var flag int
	var perm os.FileMode
	if active {
		flag = os.O_CREATE | os.O_APPEND | os.O_RDWR
		perm = os.ModePerm
	} else {
		flag = os.O_RDONLY
		perm = 0
	}
	file := path.Join(dir, fmt.Sprintf(dataFilePrefix, id))
	fd, err := os.OpenFile(file, flag, perm)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		f:        fd,
		fileID:   id,
		offset:   0,
		isActive: active,
	}, nil
}

func newDataFile(file string, active bool) (*DataFile, error) {
	var flag int
	var perm os.FileMode
	if active {
		flag = os.O_CREATE | os.O_APPEND | os.O_RDWR
		perm = os.ModePerm
	} else {
		flag = os.O_RDONLY
		perm = 0
	}
	fd, err := os.OpenFile(file, flag, perm)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		f:        fd,
		offset:   0,
		isActive: true,
	}, nil
}

func (d *DataFile) Close() error {
	return d.f.Close()
}

func (d *DataFile) Size() int64 {
	return d.offset
}

func (d *DataFile) ReadAt(offset int64) (int64, *Entry, error) {
	if d.f == nil {

	}
	// read k-v meta
	metaBuf := make([]byte, metaLen)
	metaOffset, err := d.f.ReadAt(metaBuf, offset)
	if err != nil {
		return 0, nil, err
	}
	e := &Entry{}
	e.DecodeMeta(metaBuf)

	kvBuf := make([]byte, uint64(e.keySize)+e.valueSize)
	kvOffset, err := d.f.ReadAt(kvBuf, offset+metaLen)
	if err != nil {
		return 0, nil, err
	}
	e.DecodeKV(kvBuf)
	return int64(metaOffset + kvOffset), e, nil
}

// assumed the file size is much smaller than 1 << 64
// so offset never overflow
func (d *DataFile) Write(e *Entry) (int64, error) {
	if d.f == nil || !d.isActive {

	}
	n, buf := e.Encode()
	// 1<<64 file too large, don't consider

	if uint64(d.offset)+n > uint64(math.MaxInt64) {
		// k-v too large
		d.isActive = false
		return d.offset, errors.New("")
	}
	offset := d.offset
	d.f.Write(buf)
	d.offset += int64(n)
	return offset, nil
}

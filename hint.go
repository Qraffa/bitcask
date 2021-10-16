package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path"
)

const (
	hintFilePattern = "bitcask.hint.*"
	hintFilePrefix  = "bitcask.hint.%d"

	offsetLen = 8
)

type HintEntry struct {
	// keysize offset key mark
	keySize uint32
	offset  uint64
	key     []byte
}

type HintFile struct {
	f         *os.File
	fileID    int64
	offset    int64
	bufWriter *bufio.Writer
}

func NewHintFile() *HintFile {
	return &HintFile{}
}

func OpenHintFile(dir string, fileID int64) (*HintFile, error) {
	fd, err := os.OpenFile(path.Join(dir, fmt.Sprintf(hintFilePrefix, fileID)), os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return &HintFile{
		f:      fd,
		fileID: fileID,
		offset: 0,
	}, nil
}

func (h *HintFile) WriteHint(dir string, fileID int64, key []byte, offset int64) error {
	if h == nil {
		return errors.New("")
	}
	// write new file
	if h.f == nil || h.fileID != fileID {
		if h.f != nil {
			if err := h.f.Close(); err != nil {
				return err
			}
		}

		fd, err := os.OpenFile(path.Join(dir, fmt.Sprintf(hintFilePrefix, fileID)), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
		if err != nil {
			return err
		}
		h.f = fd
		h.fileID = fileID
		h.bufWriter = bufio.NewWriterSize(fd, 4096)
	}
	entry := newHintEntry(key, offset)
	size, entryBuf := entry.Encode()
	h.bufWriter.Write(entryBuf)
	h.offset += int64(size)
	return nil
}

func (h *HintFile) Flush() {
	if h.f == nil || h.bufWriter == nil {
		return
	}
	h.bufWriter.Flush()
}

func (h *HintFile) ReadAt(offset int64) (int64, *HintEntry, error) {
	if h.f == nil {
	}

	metaBuf := make([]byte, keySizeLen+offsetLen)
	metaOffset, err := h.f.ReadAt(metaBuf, offset)
	if err != nil {
		return 0, nil, err
	}

	he := &HintEntry{}
	he.keySize = binary.BigEndian.Uint32(metaBuf[:keySizeLen])
	he.offset = binary.BigEndian.Uint64(metaBuf[keySizeLen : keySizeLen+offsetLen])

	keyBuf := make([]byte, he.keySize)
	keyOffset, err := h.f.ReadAt(keyBuf, offset+keySizeLen+offsetLen)
	if err != nil {
		return 0, nil, err
	}
	copy(keyBuf, keyBuf)

	return int64(metaOffset + keyOffset), he, nil
}

func newHintEntry(key []byte, offset int64) *HintEntry {
	return &HintEntry{
		keySize: uint32(len(key)),
		offset:  uint64(offset),
		key:     key,
	}
}

func (h *HintEntry) Size() uint64 {
	return uint64(keySizeLen + offsetLen + h.keySize)
}

func (h *HintEntry) Encode() (uint64, []byte) {
	entryBuf := make([]byte, h.Size())

	binary.BigEndian.PutUint32(entryBuf[:keySizeLen], h.keySize)
	binary.BigEndian.PutUint64(entryBuf[keySizeLen:keySizeLen+offsetLen], h.offset)

	copy(entryBuf[keySizeLen+offsetLen:], h.key)

	return h.Size(), entryBuf
}

func (h *HintEntry) Decode(data []byte) {
	h.keySize = binary.BigEndian.Uint32(data[:keySizeLen])
	h.offset = binary.BigEndian.Uint64(data[keySizeLen : keySizeLen+offsetLen])

	copy(h.key, data[keySizeLen+offsetLen:])
}

package main

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	crcLen       = 4
	markLen      = 1
	keySizeLen   = 4
	valueSizeLen = 8
	metaLen      = crcLen + keySizeLen + valueSizeLen + markLen

	DEL = 0x1
	PUT = 0x2
)

type Entry struct {
	crc       uint32
	keySize   uint32
	valueSize uint64 // lt math.MaxUint64 - 4 - 4 - 8 - math.MaxUint32
	mark      uint8
	key       []byte
	value     []byte
}

func NewEntry(key, value []byte, mark uint8) *Entry {
	// crc := crc32.ChecksumIEEE()
	return &Entry{
		keySize:   uint32(len(key)),
		valueSize: uint64(len(value)),
		key:       key,
		value:     value,
		mark:      mark,
	}
}

func (e *Entry) Size() uint64 {
	return uint64(e.keySize) + e.valueSize + metaLen
}

func (e *Entry) Encode() (uint64, []byte) {
	entryBuf := make([]byte, e.Size())

	// meta info
	entryBuf[crcLen] = byte(e.mark)
	binary.BigEndian.PutUint32(entryBuf[crcLen+markLen:crcLen+markLen+keySizeLen], e.keySize)
	binary.BigEndian.PutUint64(entryBuf[crcLen+markLen+keySizeLen:metaLen], e.valueSize)

	// k-v
	copy(entryBuf[metaLen:metaLen+e.keySize], e.key)
	copy(entryBuf[metaLen+e.keySize:], e.value)

	// crc32
	e.crc = crc32.ChecksumIEEE(entryBuf[crcLen:])
	binary.BigEndian.PutUint32(entryBuf[:crcLen], e.crc)

	return e.Size(), entryBuf
}

func (e *Entry) DecodeMeta(data []byte) {
	e.crc = binary.BigEndian.Uint32(data[:crcLen])
	e.mark = uint8(data[crcLen])
	e.keySize = binary.BigEndian.Uint32(data[crcLen+markLen : crcLen+markLen+keySizeLen])
	e.valueSize = binary.BigEndian.Uint64(data[crcLen+markLen+keySizeLen : metaLen])
}

func (e *Entry) DecodeKV(data []byte) {
	e.key = make([]byte, e.keySize)
	e.value = make([]byte, e.valueSize)
	copy(e.key, data[:e.keySize])
	copy(e.value, data[e.keySize:])
}

func Decode(data []byte) *Entry {
	e := &Entry{}
	e.crc = binary.BigEndian.Uint32(data[:crcLen])
	e.mark = uint8(data[crcLen])
	e.keySize = binary.BigEndian.Uint32(data[crcLen+markLen : crcLen+markLen+keySizeLen])
	e.valueSize = binary.BigEndian.Uint64(data[crcLen+markLen+keySizeLen : metaLen])
	e.key = make([]byte, e.keySize)
	e.value = make([]byte, e.valueSize)
	copy(e.key, data[metaLen:metaLen+e.keySize])
	copy(e.value, data[metaLen+e.keySize:])
	return e
}

package main

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	crcLen       = 4
	keySizeLen   = 4
	valueSizeLen = 8
	metaLen      = crcLen + keySizeLen + valueSizeLen
)

type Entry struct {
	crc       uint32
	keySize   uint32
	valueSize uint64 // lt math.MaxUint64 - 4 - 4 - 8 - math.MaxUint32
	key       []byte
	value     []byte
}

func NewEntry(key, value []byte) *Entry {
	// crc := crc32.ChecksumIEEE()
	return &Entry{
		keySize:   uint32(len(key)),
		valueSize: uint64(len(value)),
		key:       key,
		value:     value,
	}
}

func (e *Entry) Size() uint64 {
	return uint64(e.keySize) + e.valueSize + metaLen
}

func (e *Entry) Encode() (uint64, []byte) {
	entryBuf := make([]byte, e.Size())
	binary.BigEndian.PutUint32(entryBuf[crcLen:crcLen+keySizeLen], e.keySize)
	binary.BigEndian.PutUint64(entryBuf[crcLen+keySizeLen:metaLen], e.valueSize)
	copy(entryBuf[metaLen:metaLen+e.keySize], e.key)
	copy(entryBuf[metaLen+e.keySize:], e.value)
	e.crc = crc32.ChecksumIEEE(entryBuf[crcLen:])
	binary.BigEndian.PutUint32(entryBuf[:crcLen], e.crc)
	return e.Size(), entryBuf
}

func (e *Entry) DecodeMeta(data []byte) {
	e.crc = binary.BigEndian.Uint32(data[:crcLen])
	e.keySize = binary.BigEndian.Uint32(data[crcLen : crcLen+keySizeLen])
	e.valueSize = binary.BigEndian.Uint64(data[crcLen+keySizeLen : metaLen])
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
	e.keySize = binary.BigEndian.Uint32(data[crcLen : crcLen+keySizeLen])
	e.valueSize = binary.BigEndian.Uint64(data[crcLen+keySizeLen : metaLen])
	e.key = make([]byte, e.keySize)
	e.value = make([]byte, e.valueSize)
	copy(e.key, data[metaLen:metaLen+e.keySize])
	copy(e.value, data[metaLen+e.keySize:])
	return e
}

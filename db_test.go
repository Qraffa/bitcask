package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestMergeDel(t *testing.T) {
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
	if err := db.Del([]byte("key")); err != nil {
		panic(err)
	}
	if err := db.Del([]byte("key1")); err != nil {
		panic(err)
	}

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

func TestOnlyMerge(t *testing.T) {
	db, err := Open("")
	if err != nil {
		panic(err)
	}
	db.merge()
}

func TestConcurrPut(t *testing.T) {
	db, err := Open("")
	if err != nil {
		panic(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			db.Put([]byte(fmt.Sprintf("gor-%d-key-%d", 1, i)), []byte(fmt.Sprintf("gor-%d-value-%d", 1, i)))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			db.Put([]byte(fmt.Sprintf("gor-%d-key-%d", 2, i)), []byte(fmt.Sprintf("gor-%d-value-%d", 2, i)))
		}
	}()
	wg.Wait()

	fmt.Println(db.Keys())
	for k := range db.index {
		val, err := db.Get([]byte(k))
		if err != nil {
			panic(err)
		}
		fmt.Println(string(val))
	}
}

func TestPutMany(t *testing.T) {
	db, err := Open("")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000; i++ {
		if err := db.Put([]byte(fmt.Sprintf("gor-%d-key-%d", 2, i)), []byte(fmt.Sprintf("gor-%d-value-%d", 2, i))); err != nil {
			fmt.Println(err)
			panic(err)
		}
	}
	fmt.Println(db.Keys())
	for k := range db.index {
		val, err := db.Get([]byte(k))
		if err != nil {
			panic(err)
		}
		fmt.Println(string(val))
	}
}

func TestConcurrMer(t *testing.T) {
	db, err := Open("")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 10; i++ {
		db.Put([]byte(fmt.Sprintf("gor-%d-key-%d", 1, i)), []byte(fmt.Sprintf("gor-%d-value-%d", 1, i)))
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := db.merge(); err != nil {
			fmt.Println(err)
			panic(err)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if err := db.Put([]byte(fmt.Sprintf("gor-%d-key-%d", 2, i)), []byte(fmt.Sprintf("gor-%d-value-%d", 2, i))); err != nil {
				fmt.Println(err)
				panic(err)
			}
			//time.Sleep(2 * time.Millisecond)
		}
	}()
	wg.Wait()
	fmt.Println(db.Keys())
	for k := range db.index {
		val, err := db.Get([]byte(k))
		if err != nil {
			panic(err)
		}
		_ = val
		//fmt.Println(string(val))
	}
}

var db *Bitcask

func init() {
	start := time.Now()
	d, err := Open("")
	duration := time.Since(start)
	log.Info("rebuild. ", duration)
	if err != nil {
		panic(err)
	}
	db = d
	rand.Seed(time.Now().Unix())
}

func GetKey(n int) []byte {
	return []byte("test_key_" + fmt.Sprintf("%09d", n))
}

func GetValue() []byte {
	return []byte("test_val-val-val-val-val-val-val-val-val-val-val-val-" + strconv.FormatInt(rand.Int63(), 10))
}

type benchmarkTestCase struct {
	name string
	size int
}

func BenchmarkPutKV(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		key := GetKey(n)
		val := GetValue()
		err := db.Put(key, val)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkPut(b *testing.B) {

	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.SetBytes(int64(tt.size))

			key := []byte("foo")
			value := []byte(strings.Repeat(" ", tt.size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := db.Put(key, value)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

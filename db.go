package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	defaultMaxFileSize = 30
	defaultDir         = "/tmp/bitcask"
)

type item struct {
	fileID      int64
	entryOffset int64
}

type Bitcask struct {
	index     map[string]*item
	currID    int64
	active    *DataFile
	datafiles map[int64]*DataFile
	dir       string
}

func Open(dir string) (*Bitcask, error) {
	if dir == "" {
		dir = defaultDir
	}
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	db := &Bitcask{
		currID:    -1,
		index:     make(map[string]*item, 0),
		datafiles: make(map[int64]*DataFile, 0),
		dir:       dir,
	}

	db.loadDataFiles(db.dir)
	db.loadIndex()
	db.currID = db.nextID()
	df, err := NewDataFile(db.dir, db.currID, true)
	if err != nil {
		return nil, err
	}
	db.active = df
	db.datafiles[db.currID] = df

	return db, nil
}

func (db *Bitcask) Put(key []byte, value []byte) error {
	// check key value size
	offset, err := db.put(key, value)
	if err != nil {
		return err
	}
	it := &item{
		fileID:      db.currID,
		entryOffset: offset,
	}
	db.index[string(key)] = it
	return nil
}

func (db *Bitcask) Get(key []byte) ([]byte, error) {
	it, ok := db.index[string(key)]
	if !ok {
		return nil, errors.New("")
	}
	df, ok := db.datafiles[it.fileID]
	if !ok {
		return nil, errors.New("")
	}
	e, err := db.get(df, it.entryOffset)
	if err != nil {
		return nil, err
	}
	return e.value, nil
}

func (db *Bitcask) Del(key []byte) error {
	_, ok := db.index[string(key)]
	// key not found
	if !ok {
		return errors.New("")
	}
	if err := db.del(key); err != nil {
		return err
	}
	delete(db.index, string(key))
	return nil
}

func (db *Bitcask) Keys() int {
	return len(db.index)
}

func (db *Bitcask) checkIfNeeded(add int64) error {
	size := db.active.Size()
	// can add entry
	if size+add < defaultMaxFileSize {
		return nil
	}
	// open new datafile
	// close active
	db.active.isActive = false
	if err := db.active.Close(); err != nil {
		return err
	}

	oldID := db.currID
	db.currID = db.nextID()
	active, err := NewDataFile(db.dir, db.currID, true)
	if err != nil {
		return err
	}
	db.active = active
	db.datafiles[db.currID] = active

	// reopen old file
	old, err := NewDataFile(db.dir, oldID, false)
	if err != nil {
		return err
	}
	db.datafiles[oldID] = old

	return nil
}

func (db *Bitcask) get(df *DataFile, offset int64) (*Entry, error) {
	_, entry, err := df.ReadAt(offset)
	return entry, err
}

func (db *Bitcask) put(key []byte, value []byte) (int64, error) {
	return db.append(key, value, PUT)
}

func (db *Bitcask) del(key []byte) error {
	_, err := db.append(key, nil, DEL)
	return err
}

func (db *Bitcask) append(key []byte, value []byte, mark uint8) (int64, error) {
	if !db.active.isActive {
		return 0, errors.New("")
	}
	e := NewEntry(key, value, mark)
	if err := db.checkIfNeeded(int64(e.Size())); err != nil {
		return 0, err
	}
	offset, err := db.active.Write(e)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (db *Bitcask) loadDataFiles(dir string) error {
	files, err := filepath.Glob(fmt.Sprintf("%s/%s", dir, dataFilePattern))
	if err != nil {
		return err
	}
	if db.datafiles == nil {
		db.datafiles = make(map[int64]*DataFile)
	}
	for _, file := range files {
		id := getFileID(file)
		df, err := NewDataFile(db.dir, id, false)
		if err != nil {
			return err
		}
		db.datafiles[id] = df
	}
	return nil
}

func (db *Bitcask) loadIndex() {
	for _, df := range db.datafiles {
		db.loadIndexFromFile(df)
	}
}

func (db *Bitcask) loadIndexFromFile(df *DataFile) {
	if df == nil {
		return
	}
	var offset int64 = 0
	for {
		n, entry, err := df.ReadAt(offset)
		// read finish
		if err == io.EOF {
			break
		}
		if err != nil || entry == nil {
			return
		}
		// means k-v deleted. pass
		if entry.mark == DEL {
			offset += n
			continue
		}
		it := &item{
			fileID:      df.fileID,
			entryOffset: offset,
		}
		// read next k-v
		offset += n
		db.index[string(entry.key)] = it
	}
}

func (db *Bitcask) nextID() int64 {
	// means no active file
	if db.currID == -1 {
		var start int64 = 0
		for k := range db.datafiles {
			if k >= start {
				start = k + 1
			}
		}
		return start
	}
	return db.currID + 1
}

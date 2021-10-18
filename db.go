package main

import (
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
)

const (
	defaultMaxFileSize = 1 << 30
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
	hintfiles map[int64]*HintFile
	dir       string
	isMerging bool
	mu        sync.RWMutex
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
		hintfiles: make(map[int64]*HintFile, 0),
		dir:       dir,
	}

	db.loadDataFiles(db.dir)
	db.loadHintFiles(db.dir)
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
	db.mu.Lock()
	defer db.mu.Unlock()
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
	db.mu.RLock()
	defer db.mu.RUnlock()
	it, ok := db.index[string(key)]
	if !ok {
		return nil, errors.New("not found")
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
	db.mu.Lock()
	defer db.mu.Unlock()
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

func (db *Bitcask) merge() error {
	db.mu.Lock()
	// merging
	if db.isMerging {
		return nil
	}
	db.isMerging = true
	db.mu.Unlock()
	// like copy-on-write
	tmpdir := path.Join(defaultDir, "tmp_db")
	// tmpdir no datafile, currid=0
	mdb, err := Open(tmpdir)
	if err != nil {
		return err
	}
	// copy index
	db.mu.Lock()
	mdb.index = make(map[string]*item, len(db.index))
	for k, v := range db.index {
		mdb.index[k] = v
	}
	lastid := db.currID
	// force to use new datafile
	err = db.checkIfNeeded(0, true)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	db.mu.Unlock()
	hf := NewHintFile()
	// mdb rebuild datafile
	for _, v := range mdb.index {
		db.mu.RLock()
		file := db.datafiles[v.fileID]
		db.mu.RUnlock()
		// 随机读
		_, entry, err := file.ReadAt(v.entryOffset)
		if err != nil {
			return err
		}
		// 顺序append
		if err := mdb.Put(entry.key, entry.value); err != nil {
			return err
		}
		// write hint file, the same as datafile fileid
		it := mdb.index[string(entry.key)]
		// 顺序append
		// ==> bufio write
		if err := hf.WriteHint(mdb.dir, it.fileID, entry.key, it.entryOffset); err != nil {
			return err
		}
	}
	hf.Flush()

	db.mu.Lock()
	defer db.mu.Unlock()
	startID := db.currID + 1
	// TODO bug-fix: deleted key
	for k := range mdb.index {
		// means k-v has newer value
		if db.index[k].fileID > db.currID {
			continue
		}
		// update origin db index
		mdb.index[k].fileID = mdb.index[k].fileID + startID
		db.index[k] = mdb.index[k]
	}
	// move hint file, don't need to open it
	files, err := filepath.Glob(path.Join(mdb.dir, hintFilePattern))
	if err != nil {
		return err
	}
	for _, file := range files {
		newfile := getNewFileName(tmpdir, db.dir, file, startID)
		if err := os.Rename(file, newfile); err != nil {
			return err
		}
	}
	// move tmp-datafile to db dir
	for _, file := range mdb.datafiles {
		newfile := getNewFileName(tmpdir, db.dir, file.f.Name(), startID)
		if err := os.Rename(file.f.Name(), newfile); err != nil {
			return err
		}
		fileid := getFileID(newfile)
		df, err := NewDataFile(db.dir, fileid, false)
		if err != nil {
			return err
		}
		db.datafiles[fileid] = df
	}
	// remove old datafile
	for _, v := range db.datafiles {
		if v.fileID > lastid {
			// update currID
			if v.fileID > db.currID {
				db.currID = v.fileID
			}
			continue
		}
		delete(db.datafiles, v.fileID)
		os.Remove(v.f.Name())
	}
	// force to use new datafile
	if err := db.checkIfNeeded(0, true); err != nil {
		return err
	}
	db.isMerging = false
	return nil
}

// force==true, means db must use new datafile
func (db *Bitcask) checkIfNeeded(add int64, force bool) error {
	if !force {
		size := db.active.Size()
		// can add entry
		if size+add < defaultMaxFileSize {
			return nil
		}
	}
	// open new datafile
	// close active
	db.active.isActive = false
	if err := db.active.Close(); err != nil {
		return err
	}

	oldID := db.active.fileID
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
	if err := db.checkIfNeeded(int64(e.Size()), false); err != nil {
		return 0, err
	}
	offset, err := db.active.Write(e)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

func (db *Bitcask) loadDataFiles(dir string) error {
	files, err := filepath.Glob(path.Join(dir, dataFilePattern))
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

func (db *Bitcask) loadHintFiles(dir string) error {
	files, err := filepath.Glob(path.Join(dir, hintFilePattern))
	if err != nil {
		return err
	}
	if db.hintfiles == nil {
		db.hintfiles = make(map[int64]*HintFile)
	}
	for _, file := range files {
		id := getFileID(file)
		hf, err := OpenHintFile(dir, id)
		if err != nil {
			return err
		}
		db.hintfiles[id] = hf
	}
	return nil
}

// rebuild index
func (db *Bitcask) loadIndex() {
	for _, df := range db.datafiles {
		// load from hint first
		if hf, ok := db.hintfiles[df.fileID]; ok {
			db.loadIndexFromHint(hf)
		} else {
			db.loadIndexFromFile(df)
		}
	}
}

func (db *Bitcask) loadIndexFromHint(hf *HintFile) {
	if hf == nil {
		return
	}
	var offset int64 = 0
	for {
		n, he, err := hf.ReadAt(offset)
		if err == io.EOF {
			break
		}
		if err != nil || he == nil {
			return
		}
		it := &item{
			fileID:      hf.fileID,
			entryOffset: int64(he.offset),
		}
		offset += n
		db.index[string(he.key)] = it
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
			if _, ok := db.index[string(entry.key)]; ok {
				delete(db.index, string(entry.key))
			}
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

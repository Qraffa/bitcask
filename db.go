package main

import "errors"

const (
	defaultMaxFileSize = 30
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
}

func Open() (*Bitcask, error) {
	Bitcask := &Bitcask{
		index:     make(map[string]*item, 0),
		currID:    0,
		datafiles: make(map[int64]*DataFile, 0),
	}

	df, err := NewDataFile(Bitcask.currID, true)
	if err != nil {
		return nil, err
	}
	Bitcask.active = df
	Bitcask.datafiles[Bitcask.currID] = df

	return Bitcask, nil
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
	db.currID++
	active, err := NewDataFile(db.currID, true)
	if err != nil {
		return err
	}
	db.active = active
	db.datafiles[db.currID] = active

	// reopen old file
	old, err := NewDataFile(oldID, false)
	if err != nil {
		return err
	}
	db.datafiles[oldID] = old

	return nil
}

func (db *Bitcask) get(df *DataFile, offset int64) (*Entry, error) {
	return df.ReadAt(offset)
}

func (db *Bitcask) put(key []byte, value []byte) (int64, error) {
	if !db.active.isActive {
		return 0, errors.New("")
	}
	e := NewEntry(key, value)
	if err := db.checkIfNeeded(int64(e.Size())); err != nil {
		return 0, err
	}
	offset, err := db.active.Write(e)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

package db

import (
	"encoding/binary"
	"io"
	"sort"
)

type Database struct {
	MemTable MemTable
	SSTables []SSTable
}

func NewTable() *Database {
	db := &Database{
		MemTable: MemTable{Table: map[string][]byte{}},
		SSTables: initSSTables(),
	}
	return db
}

// Flush the contents of the in-memory key/value database
// to `w` in the form of an SSTable.
func (m *Database) FlushSSTable(w io.Writer) error {

	var tempkeys []string
	for k := range m.MemTable.Table {
		tempkeys = append(tempkeys, k)
	}
	sort.Strings(tempkeys)

	tableCountBS := make([]byte, 2)
	entryCountBS := make([]byte, 2)
	entryCount := len(tempkeys)
	binary.BigEndian.PutUint16(entryCountBS, uint16(entryCount))
	binary.BigEndian.PutUint16(tableCountBS, uint16(len(m.SSTables)+1))
	startLetter := []byte{tempkeys[0][0]}
	endLetter := []byte{tempkeys[entryCount-1][0]}
	w.Write(tableCountBS)
	w.Write(entryCountBS)
	w.Write(startLetter)
	w.Write(endLetter)
	for _, key := range tempkeys {
		keyLenBS := make([]byte, 2)
		valLenBS := make([]byte, 2)
		val := m.MemTable.Table[key]
		binary.BigEndian.PutUint16(keyLenBS, uint16(len([]byte(key))))
		binary.BigEndian.PutUint16(valLenBS, uint16(len(val)))
		w.Write(keyLenBS)
		w.Write(valLenBS)
		w.Write([]byte(key))
		w.Write(val)
	}
	m.MemTable.Table = make(map[string][]byte)
	return nil
}

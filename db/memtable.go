package db

import (
	"encoding/binary"
	"errors"
	"io"
	"sort"
)

type MemTable struct {
	Table map[string][]byte
}

func (m MemTable) Get(key []byte) (value []byte, err error) {
	val, ok := m.Table[string(key)]
	if !ok {
		return nil, errors.New("key not found")
	}
	return val, nil
}

func (m MemTable) Has(key []byte) (ret bool, err error) {
	_, ok := m.Table[string(key)]
	if !ok {
		return false, errors.New("key not found")
	}
	return true, nil
}

func (m MemTable) Put(key, value []byte) error {
	m.Table[string(key)] = value
	return nil
}

func (m MemTable) Delete(key []byte) error {
	_, ok := m.Table[string(key)]
	if !ok {
		return errors.New("key not found")
	}
	delete(m.Table, string(key))
	return nil
}

func (m MemTable) RangeScan(start, limit []byte) (Iterator, error) {
	rangeScan := &RangeScanIterator{table: m.Table, rangeKeys: make([]string, 0, len(m.Table)), curIdx: 0}
	for k := range m.Table {
		if k >= string(start) && k <= string(limit) {
			rangeScan.rangeKeys = append(rangeScan.rangeKeys, k)
		}

	}
	sort.Strings(rangeScan.rangeKeys)
	return rangeScan, nil
}

// Flush the contents of the in-memory key/value database
// to `w` in the form of an SSTable.
func (m *MemTable) FlushSSTable(w io.Writer) error {

	var tempkeys []string
	for k := range m.Table {
		tempkeys = append(tempkeys, k)
	}
	sort.Strings(tempkeys)
	entryCountBS := make([]byte, 2)
	entryCount := len(tempkeys)
	binary.BigEndian.PutUint16(entryCountBS, uint16(entryCount))
	startLetter := []byte{tempkeys[0][0]}
	endLetter := []byte{tempkeys[entryCount-1][0]}
	w.Write(entryCountBS)
	w.Write(startLetter)
	w.Write(endLetter)
	for _, key := range tempkeys {
		keyLenBS := make([]byte, 2)
		valLenBS := make([]byte, 2)
		val := m.Table[key]
		binary.BigEndian.PutUint16(keyLenBS, uint16(len([]byte(key))))
		binary.BigEndian.PutUint16(valLenBS, uint16(len(val)))
		w.Write(keyLenBS)
		w.Write(valLenBS)
		w.Write([]byte(key))
		w.Write(val)
	}
	m.Table = make(map[string][]byte)
	return nil
}

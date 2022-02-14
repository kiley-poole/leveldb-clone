package db

import (
	"errors"
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

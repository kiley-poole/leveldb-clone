package main

import (
	"errors"
	"fmt"
	"sort"
)

type DB interface {
	// Get gets the value for the given key. It returns an error if the
	// DB does not contain the key.
	Get(key []byte) (value []byte, err error)

	// Has returns true if the DB contains the given key.
	Has(key []byte) (ret bool, err error)

	// Put sets the value for the given key. It overwrites any previous value
	// for that key; a DB is not a multi-map.
	Put(key, value []byte) error

	// Delete deletes the value for the given key.
	Delete(key []byte) error

	// RangeScan returns an Iterator (see below) for scanning through all
	// key-value pairs in the given range, ordered by key ascending.
	RangeScan(start, limit []byte) (Iterator, error)
}

type Iterator interface {
	// Next moves the iterator to the next key/value pair.
	// It returns false if the iterator is exhausted.
	Next() bool

	// Error returns any accumulated error. Exhausting all the key/value pairs
	// is not considered to be an error.
	Error() error

	// Key returns the key of the current key/value pair, or nil if done.
	Key() []byte

	// Value returns the value of the current key/value pair, or nil if done.
	Value() []byte
}

type memTable struct {
	table map[string]string
}

func (m memTable) Get(key []byte) (value []byte, err error) {
	val, ok := m.table[string(key)]
	if !ok {
		return nil, errors.New("key not found")
	}
	return []byte(val), nil
}

func (m memTable) Has(key []byte) (ret bool, err error) {
	_, ok := m.table[string(key)]
	if !ok {
		return false, errors.New("key not found")
	}
	return true, nil
}

//TODO: Add error handling
func (m memTable) Put(key, value []byte) error {
	m.table[string(key)] = string(value)
	return nil
}

func (m memTable) Delete(key []byte) error {
	_, ok := m.table[string(key)]
	if !ok {
		return errors.New("key not found")
	}
	delete(m.table, string(key))
	return nil
}

func (m memTable) RangeScan(start, limit []byte) (Iterator, error) {
	rangeScan := RangeScanIterator{table: m.table, rangeKeys: make([]string, len(m.table))}
	for k := range m.table {
		rangeScan.rangeKeys = append(rangeScan.rangeKeys, k)
	}
	sort.Strings(rangeScan.rangeKeys)
	//TODO
	return nil, nil
}

//TODO: flesh out iterator funcs - add incrementor to struct to handle position?
type RangeScanIterator struct {
	table     map[string]string
	rangeKeys []string
}

func main() {
	db := memTable{table: make(map[string]string)}

	db.Put([]byte("test"), []byte("ans"))
	testVar, _ := db.Get([]byte("test"))
	testHas, _ := db.Has([]byte("duh"))

	fmt.Printf("The key test has %s and the key duh is %t\n", testVar, testHas)

	db.Delete([]byte("test"))
	testHas, _ = db.Has([]byte("test"))
	fmt.Printf("Does the key test exist? %t\n", testHas)

}

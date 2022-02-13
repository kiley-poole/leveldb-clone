package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
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
	table map[string][]byte
}

func (m memTable) Get(key []byte) (value []byte, err error) {
	val, ok := m.table[string(key)]
	if !ok {
		return nil, errors.New("key not found")
	}
	return val, nil
}

func (m memTable) Has(key []byte) (ret bool, err error) {
	_, ok := m.table[string(key)]
	if !ok {
		return false, errors.New("key not found")
	}
	return true, nil
}

func (m memTable) Put(key, value []byte) error {
	m.table[string(key)] = value
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
	rangeScan := &RangeScanIterator{table: m.table, rangeKeys: make([]string, 0, len(m.table)), curIdx: 0}
	for k := range m.table {
		if k >= string(start) && k <= string(limit) {
			rangeScan.rangeKeys = append(rangeScan.rangeKeys, k)
		}

	}
	sort.Strings(rangeScan.rangeKeys)
	return rangeScan, nil
}

// Flush the contents of the in-memory key/value database
// to `w` in the form of an SSTable.
func (m *memTable) flushSSTable(w io.Writer) error {

	var tempkeys []string
	for k := range m.table {
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
		val := m.table[key]
		binary.BigEndian.PutUint16(keyLenBS, uint16(len([]byte(key))))
		binary.BigEndian.PutUint16(valLenBS, uint16(len(val)))
		w.Write(keyLenBS)
		w.Write(valLenBS)
		w.Write([]byte(key))
		w.Write(val)
	}
	m.table = make(map[string][]byte)
	return nil
}

type RangeScanIterator struct {
	table     map[string][]byte
	rangeKeys []string
	curIdx    int
}

func (r *RangeScanIterator) Next() bool {
	r.curIdx += 1
	return r.curIdx < len(r.rangeKeys)
}

func (r RangeScanIterator) Error() error {
	return nil
}

func (r RangeScanIterator) Value() []byte {
	if r.curIdx >= len(r.rangeKeys) {
		return nil
	}
	curVal := r.table[r.rangeKeys[r.curIdx]]
	return []byte(curVal)
}

func (r RangeScanIterator) Key() []byte {
	if r.curIdx >= len(r.rangeKeys) {
		return nil
	}
	curKey := r.rangeKeys[r.curIdx]
	return []byte(curKey)
}

func main() {
	db := memTable{table: make(map[string][]byte)}

	db.Put([]byte("ant eater"), []byte("ans1"))
	db.Put([]byte("cat"), []byte("ans1"))
	db.Put([]byte("bison"), []byte("ans1"))
	db.Put([]byte("fox"), []byte("ans2"))
	db.Put([]byte("Aardvark"), []byte("ans3"))
	db.Put([]byte("Dog"), []byte("ans4"))
	db.Put([]byte("Elephant"), []byte("ans5"))
	testVar, _ := db.Get([]byte("cat"))
	testHas, _ := db.Has([]byte("ant eater"))
	f, _ := os.Create("test.ldb")
	db.flushSSTable(f)
	iter, _ := db.RangeScan([]byte("a"), []byte("Z"))
	fmt.Println(iter)
	iter.Next()
	fmt.Println(string(iter.Key()))
	fmt.Println(string(iter.Value()))
	iter.Next()
	fmt.Println(string(iter.Key()))
	fmt.Println(string(iter.Value()))
	iter.Next()
	fmt.Println(string(iter.Key()))
	fmt.Println(string(iter.Value()))
	iter.Next()

	fmt.Printf("The key test has %s and the key duh is %t\n", testVar, testHas)

	db.Delete([]byte("test"))
	testHas, _ = db.Has([]byte("test"))
	fmt.Printf("Does the key test exist? %t\n", testHas)

}

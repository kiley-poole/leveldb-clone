package db

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

type SSTable_entry struct {
	KeyLength uint16
	ValLength uint16
	Key       []byte
	Value     []byte
}

type SSTable struct {
	TableIdx   uint16
	EntryCount uint16
	StartKey   byte
	EndKey     byte
	Entries    []SSTable_entry
}

func (s *SSTable) Get(key []byte) (value []byte, err error) {
	f, _ := os.Open("data/test.ldb")
	rs := io.ReadSeeker(f)
	rs.Seek(2, 1)
	ec := make([]byte, 2)
	sk := make([]byte, 1)
	ek := make([]byte, 1)
	rs.Read(ec)
	rs.Read(sk)
	rs.Read(ek)
	if key[0] < sk[0] || key[0] > ek[0] {
		return nil, nil
	}
	for range ec {
		kl := make([]byte, 2)
		vl := make([]byte, 2)
		rs.Read(kl)
		rs.Read(vl)
		klen := binary.BigEndian.Uint16(kl)
		vlen := binary.BigEndian.Uint16(vl)
		k := make([]byte, klen)
		val := make([]byte, vlen)
		rs.Read(k)
		rs.Read(val)
		if bytes.Equal(key, k) {
			return val, nil
		}
	}
	return nil, nil
}

func (s *SSTable) Has(key []byte) (ret bool, err error) {
	f, _ := os.Open("data/test.ldb")
	rs := io.ReadSeeker(f)
	rs.Seek(2, 1)
	ec := make([]byte, 2)
	sk := make([]byte, 1)
	ek := make([]byte, 1)
	rs.Read(ec)
	rs.Read(sk)
	rs.Read(ek)
	if key[0] < sk[0] || key[0] > ek[0] {
		return false, nil
	}
	for range ec {
		kl := make([]byte, 2)
		vl := make([]byte, 2)
		rs.Read(kl)
		rs.Read(vl)
		klen := binary.BigEndian.Uint16(kl)
		vlen := binary.BigEndian.Uint16(vl)
		k := make([]byte, klen)
		val := make([]byte, vlen)
		rs.Read(k)
		rs.Read(val)
		if bytes.Equal(key, k) {
			return true, nil
		}
	}
	return false, nil
}

// func (s *SSTable) RangeScan(start, limit []byte) (Iterator, error) {

// }

func initSSTables() []SSTable {
	d, _ := os.ReadDir("data")
	ssTables := make([]SSTable, 0, len(d))
	for _, f := range d {
		sstable := SSTable{}
		of, _ := os.Open(f.Name())
		rs := io.ReadSeeker(of)

		tableIdx := make([]byte, 2)
		rs.Read(tableIdx)
		sstable.TableIdx = binary.BigEndian.Uint16(tableIdx)

		entryCount := make([]byte, 2)
		rs.Read(entryCount)
		sstable.EntryCount = binary.BigEndian.Uint16(entryCount)

		startKey := make([]byte, 1)
		rs.Read(startKey)
		sstable.StartKey = startKey[0]

		endKey := make([]byte, 1)
		rs.Read(endKey)
		sstable.EndKey = endKey[0]

		ssTables = append(ssTables, sstable)
	}
	return ssTables
}

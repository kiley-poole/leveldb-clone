package sstable

type SSTable_entry struct {
	KeyLength uint16
	ValLength uint16
	Key       []byte
	Value     []byte
}

type SSTable struct {
	TableCount uint16
	EntryCount uint16
	StartKey   byte
	EndKey     byte
	TableSize  uint64
	Entries    []SSTable_entry
}

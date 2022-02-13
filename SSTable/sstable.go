package sstable

type SSTable_entry struct {
	KeyLength uint16
	ValLength uint16
	Key       []byte
	Value     []byte
}

type SSTable struct {
	EntryCount uint16
	StartKey   byte
	EndKey     byte
	Entries    []SSTable_entry
}

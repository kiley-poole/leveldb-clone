package db

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

package main

import (
	"fmt"

	"github.com/kiley-poole/leveldb-clone/db"
)

func main() {

	newDb := db.NewTable()

	// dbT := db.MemTable{Table: make(map[string][]byte)}

	// dbT.Put([]byte("ant eater"), []byte("ans1"))
	// dbT.Put([]byte("cat"), []byte("ans1"))
	// dbT.Put([]byte("bison"), []byte("ans1"))
	// dbT.Put([]byte("fox"), []byte("ans2"))
	// dbT.Put([]byte("Aardvark"), []byte("ans3"))
	// dbT.Put([]byte("Dog"), []byte("ans4"))
	// dbT.Put([]byte("Elephant"), []byte("ans5"))
	// testVar, _ := dbT.Get([]byte("cat"))
	// testHas, _ := dbT.Has([]byte("ant eater"))
	// f, _ := os.Create("data/test.ldb")
	// dbT.FlushSSTable(f)
	// f.Close()

	ssT := db.SSTable{}
	val, _ := ssT.Get([]byte("Dog"))
	fmt.Println(val)
	val2, _ := ssT.Has([]byte("Aardvark"))
	fmt.Println(val2)

}

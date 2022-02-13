package main

import (
	"fmt"
	"os"

	"github.com/kiley-poole/leveldb-clone/db"
)

func main() {
	db := db.MemTable{Table: make(map[string][]byte)}

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
	db.FlushSSTable(f)
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

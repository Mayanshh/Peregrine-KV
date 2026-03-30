package main

import (
	"fmt"
	"log"

	peregrinekv "github.com/Mayanshh/Peregrine-KV"
)

func main() {
	opts := peregrinekv.DefaultOptions()
	opts.DataDir = "./my_data"

	db, err := peregrinekv.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Put([]byte("name"), []byte("Mayansh")); err != nil {
		log.Fatal(err)
	}

	val, ok, err := db.Get([]byte("name"))
	if err != nil {
		log.Fatal(err)
	}
	if ok {
		fmt.Println("name =", string(val))
	}
}

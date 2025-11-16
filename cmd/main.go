package main

import (
	"flag"
	"fmt"
	"log"

	badger "github.com/dgraph-io/badger/v4"
)

var path string

func init() {
	flag.StringVar(&path, "path", "./badger", "path to the database")
	flag.Parse()
}

func main() {
	fmt.Println("Hello, World!")
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()
}

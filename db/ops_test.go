package db

import (
	"encoding/binary"
	"fmt"
	"log"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestDequeue(t *testing.T) {
	path := "./data"
	bdb, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		log.Fatal(err)
	}
	defer bdb.Close()

	l := zaptest.NewLogger(t)
	dbWrap, err := New(bdb, l)
	require.NoError(t, err)

	queue := []byte("hello")
	err = dbWrap.Enqueue(queue, []byte("world"))
	require.NoError(t, err)

	item, err := dbWrap.Dequeue(queue)
	require.NoError(t, err)
	require.Equal(t, []byte("world"), item)

	bdb.View(func(txn *badger.Txn) error {
		seqKey := keySeq(keyActiveQueue(queue))

		item, err := txn.Get(seqKey)
		require.NoError(t, err)

		_ = item.Value(func(val []byte) error {
			n := int(binary.BigEndian.Uint64(val))
			require.Greater(t, n, 0)
			fmt.Printf("seqKey=%s, value=%d\n", seqKey, n)
			return nil
		})

		seqKey = keySeq(keyPendingQueue(queue))
		item, err = txn.Get(seqKey)
		require.NoError(t, err)

		_ = item.Value(func(val []byte) error {
			n := int(binary.BigEndian.Uint64(val))
			require.Greater(t, n, 0)
			fmt.Printf("seqKey=%s, value=%d\n", seqKey, n)
			return nil
		})

		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := keyActiveQueue(queue)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	})

}

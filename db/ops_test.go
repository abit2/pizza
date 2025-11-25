package db

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/abit2/pizza/task/task/generated"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	"google.golang.org/protobuf/proto"
)

func TestDequeue(t *testing.T) {
	path := "./data"
	bdb, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := bdb.Close()
		require.NoError(t, err)
		// Remove all items from the queue
		err = os.RemoveAll(path)
		require.NoError(t, err)
	}()

	l := zaptest.NewLogger(t)
	dbWrap, err := New(bdb, l, nil)
	require.NoError(t, err)

	queue := []byte("hello")
	err = dbWrap.Enqueue(queue, []byte("world - 0"))
	require.NoError(t, err)

	err = dbWrap.Enqueue(queue, []byte("world - 1"))
	require.NoError(t, err)

	item, err := dbWrap.MoveToActiveFromPending(queue)
	require.NoError(t, err)
	require.Equal(t, []byte("world - 0"), item)

	item, err = dbWrap.MoveToActiveFromPending(queue)
	require.NoError(t, err)
	require.Equal(t, []byte("world - 1"), item)

	e := bdb.View(func(txn *badger.Txn) error {
		seqKey := keySeq(keyQueue(queue, []byte(generated.State_ACTIVE.String())))

		item, err := txn.Get(seqKey)
		require.NoError(t, err)

		_ = item.Value(func(val []byte) error {
			n := int(binary.BigEndian.Uint64(val))
			require.Greater(t, n, 0)
			fmt.Printf("seqKey=%s, value=%d\n", seqKey, n)
			return nil
		})

		seqKey = keySeq(keyQueue(queue, []byte(generated.State_PENDING.String())))
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

		prefixTask := []byte("task:")
		var taskKeys []string
		for it.Seek(prefixTask); it.ValidForPrefix(prefixTask); it.Next() {
			item := it.Item()
			k := item.Key()
			keys := strings.Split(string(k), ":")
			taskKeys = append(taskKeys, keys[len(keys)-1])
			err := item.Value(func(v []byte) error {
				item := &generated.Task{}
				err = proto.Unmarshal(v, item)
				if err != nil {
					return err
				}
				require.Equal(t, item.GetState(), generated.State_ACTIVE, "Task state should be ACTIVE %s", item.GetState().String())
				return nil
			})
			require.NoError(t, err)
		}

		prefix := keyQueue(queue, []byte(generated.State_PENDING.String()))
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			err := item.Value(func(v []byte) error {
				require.Contains(t, taskKeys, string(v), fmt.Sprintf("taskKeys=%v, key=%s", taskKeys, v))
				return nil
			})
			if err != nil {
				return err
			}
		}

		leasePrefix := []byte(fmt.Sprintf("pizza:lease:%s:", queue))
		for it.Seek(leasePrefix); it.ValidForPrefix(leasePrefix); it.Next() {
			item := it.Item()
			k := item.Key()
			l := strings.Split(string(k), ":")
			require.Contains(t, taskKeys, l[len(l)-1])
		}

		return nil
	})
	require.NoError(t, e)

}

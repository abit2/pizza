package db

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/abit2/pizza/log"
	"github.com/abit2/pizza/task/task/generated"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"
)

const path = "./data"

type OpstTestSuite struct {
	suite.Suite
	bdb *badger.DB
}

func TestOpstTestSuite(t *testing.T) {
	suite.Run(t, new(OpstTestSuite))
}

func (suite *OpstTestSuite) SetupSuite() {
	var err error
	if suite.bdb != nil {
		return
	}
	suite.bdb, err = badger.Open(badger.DefaultOptions(path))
	require.NoError(suite.T(), err)
}

func (suite *OpstTestSuite) TearDownSuite() {
	err := suite.bdb.Close()
	require.NoError(suite.T(), err)
	// Remove all items from the queue
	err = os.RemoveAll(path)
	require.NoError(suite.T(), err)
}

func (suite *OpstTestSuite) TestDequeue() {
	t := suite.T()
	bdb := suite.bdb
	l := log.NewLogger(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	dbWrap, err := New(bdb, l, nil)
	require.NoError(t, err)

	taskKeys := make(map[string]string)

	payload := []string{"world - 0", "world - 1"}
	queue := []byte("hello")
	headers := []byte("headers")
	maxRetryCount := uint32(2)

	for _, p := range payload {
		taskID, err := dbWrap.Enqueue(queue, []byte(p), headers, maxRetryCount)
		require.NoError(t, err)
		taskKeys[string(taskID)] = p
	}

	// checking pending
	seqCount := 0
	for tID, load := range taskKeys {
		e := bdb.View(func(txn *badger.Txn) error {
			seqKey := keySeq(keyQueue(queue, []byte(generated.State_PENDING.String())))

			item, err := txn.Get(seqKey)
			require.NoError(t, err)
			_ = item.Value(func(val []byte) error {
				n := int(binary.BigEndian.Uint64(val))
				require.Greater(t, n, 0)
				seqCount += 1
				fmt.Printf("seqKey=%s, value=%d\n", seqKey, n)
				return nil
			})

			taskItem := getTask(t, txn, tID, headers, maxRetryCount)
			require.Equal(t, generated.State_PENDING, taskItem.GetState())
			require.Equal(t, load, string(taskItem.Payload))
			require.Equal(t, uint32(0), taskItem.RetryCount)

			taskRefItem := getRefItem(t, txn, queue, tID)
			require.Empty(t, taskRefItem.LeaseKey)
			pendingQBytes, err := txn.Get([]byte(taskRefItem.Key))
			require.NoError(t, err)
			require.NotNil(t, pendingQBytes)
			return nil
		})
		require.NoError(t, e)

	}
	require.Equal(t, len(taskKeys), seqCount)

	// now move the task to active
	for _, _ = range taskKeys {
		_, err = dbWrap.Dequeue(queue)
		require.NoError(t, err)
	}

	// checking active tasks
	activeSeqCount := 0
	for tID, load := range taskKeys {
		e := bdb.View(func(txn *badger.Txn) error {
			seqKey := keySeq(keyQueue(queue, []byte(generated.State_ACTIVE.String())))

			item, err := txn.Get(seqKey)
			require.NoError(t, err)

			_ = item.Value(func(val []byte) error {
				n := int(binary.BigEndian.Uint64(val))
				require.Greater(t, n, 0)
				activeSeqCount += 1
				fmt.Printf("seqKey=%s, value=%d\n", seqKey, n)
				return nil
			})
			taskItem := getTask(t, txn, tID, headers, maxRetryCount)
			require.Equal(t, generated.State_ACTIVE, taskItem.GetState())
			require.Equal(t, load, string(taskItem.Payload))
			require.Equal(t, uint32(1), taskItem.RetryCount)

			taskRefItem := getRefItem(t, txn, queue, tID)
			require.NotEmpty(t, taskRefItem.LeaseKey)
			leaseBytes, err := txn.Get([]byte(taskRefItem.LeaseKey))
			require.NoError(t, err)
			require.NotNil(t, leaseBytes)
			leaseTs := getTsFromKey(t, taskRefItem.LeaseKey)
			fmt.Println("lease ts ", leaseTs, " time now ", time.Now())
			require.True(t, time.Until(leaseTs) <= defaultLeaseDuration && time.Until(leaseTs) > 0)
			// require.Equal(t, defaultLeaseDuration, time.Until(leaseTs))

			activeQBytes, err := txn.Get([]byte(taskRefItem.Key))
			require.NoError(t, err)
			require.NotNil(t, activeQBytes)
			return nil
		})
		require.NoError(t, e)
	}
	require.Equal(t, len(taskKeys), activeSeqCount)

	// now move to retry queue
	for taskID := range taskKeys {
		err := dbWrap.MoveToRetryFromActive(queue, taskID)
		require.NoError(t, err)
	}

	// check retry task
	for tID, load := range taskKeys {
		e := bdb.View(func(txn *badger.Txn) error {
			taskItem := getTask(t, txn, tID, headers, maxRetryCount)
			require.Equal(t, generated.State_RETRY, taskItem.GetState())
			require.Equal(t, load, string(taskItem.Payload))
			require.Equal(t, uint32(1), taskItem.RetryCount)

			taskRefItem := getRefItem(t, txn, queue, tID)
			require.Empty(t, taskRefItem.LeaseKey)
			retryTs := getTsFromKey(t, taskRefItem.Key)
			fmt.Println("retry ts ", retryTs, " time now ", time.Now())
			require.True(t, time.Until(defaultRetryFn(time.Now())) >= time.Until(retryTs) && time.Until(retryTs) > 0)

			retryQBytes, err := txn.Get([]byte(taskRefItem.Key))
			require.NoError(t, err)
			require.NotNil(t, retryQBytes)
			return nil
		})
		require.NoError(t, e)

	}
	require.Equal(t, len(taskKeys), activeSeqCount)
}

func (suite *OpstTestSuite) TestMoveToPendingFromRetry() {
	t := suite.T()
	bdb := suite.bdb
	l := log.NewLogger(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	dbWrap, err := New(bdb, l, nil)
	require.NoError(t, err)

	taskKeys := make(map[string]string)

	payload := []string{"world - 0", "world - 1"}
	queue := []byte("hello")
	headers := []byte("headers")
	maxRetryCount := uint32(3)

	for _, p := range payload {
		taskID, err := dbWrap.Enqueue(queue, []byte(p), headers, maxRetryCount)
		require.NoError(t, err)
		taskKeys[string(taskID)] = p
	}

	for taskID, payload := range taskKeys {
		_, err := dbWrap.Dequeue(queue)
		require.NoError(t, err)

		err = dbWrap.MoveToRetryFromActive(queue, taskID)
		require.NoError(t, err)

		err = dbWrap.MoveToPendingFromRetry(queue, taskID)
		require.NoError(t, err)

		e := bdb.View(func(txn *badger.Txn) error {
			pendingQ := keyQueue(queue, []byte(generated.State_PENDING.String()))
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(pendingQ); it.ValidForPrefix(pendingQ); it.Next() {
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

			taskItem := getTask(t, txn, taskID, headers, maxRetryCount)
			require.Equal(t, generated.State_PENDING, taskItem.GetState())
			require.Equal(t, payload, string(taskItem.Payload))
			require.Equal(t, uint32(1), taskItem.RetryCount)

			taskRefItem := getRefItem(t, txn, queue, taskID)
			require.Empty(t, taskRefItem.LeaseKey)
			fmt.Println("pending ref", taskRefItem.Key)

			pendingQBytes, err := txn.Get([]byte(taskRefItem.Key))
			require.NoError(t, err)
			require.NotNil(t, pendingQBytes)
			return nil
		})
		require.NoError(t, e)
	}
}

func getRefItem(t *testing.T, txn *badger.Txn, queue []byte, taskID string) *generated.TaskReference {
	t.Helper()
	refKey := keyReference(queue, taskID)
	refBytes, err := txn.Get(refKey)
	require.NoError(t, err)

	resp, err := refBytes.ValueCopy(nil)
	require.NoError(t, err)

	refItem := &generated.TaskReference{}
	err = proto.Unmarshal(resp, refItem)
	require.NoError(t, err)

	require.Equal(t, taskID, refItem.Id)
	return refItem
}

func getTask(t *testing.T, txn *badger.Txn, taskID string, headers []byte, maxRetryCount uint32) *generated.Task {
	t.Helper()
	taskKey := keyTask(taskID)
	taskBytes, err := txn.Get(taskKey)
	require.NoError(t, err)

	resp, err := taskBytes.ValueCopy(nil)
	require.NoError(t, err)

	taskItem := &generated.Task{}
	err = proto.Unmarshal(resp, taskItem)
	require.NoError(t, err)

	require.Equal(t, taskID, taskItem.Id)
	require.Equal(t, taskItem.MaxRetries, maxRetryCount)
	require.Equal(t, taskItem.Headers, headers)
	return taskItem
}

func getTsFromKey(t *testing.T, key string) time.Time {

	// var queueZSetTemplate = "pizza:%s:%s:%020d:%s"
	info := strings.Split(key, ":")
	unixTsStr := info[len(info)-2]
	ts, err := strconv.ParseInt(unixTsStr, 10, 64)
	require.NoError(t, err)

	return time.Unix(ts, 0)
}

// TODO: Implement TestMoveToArchivedFromActive
func (suite *OpstTestSuite) TestMoveToArchivedFromActive() {
	t := suite.T()
	bdb := suite.bdb
	l := log.NewLogger(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	dbWrap, err := New(bdb, l, nil)
	require.NoError(t, err)

	taskKeys := make(map[string]string)

	payload := []string{"world - 0", "world - 1"}
	queue := []byte("hello")
	headers := []byte("headers")
	maxRetryCount := uint32(3)

	for _, p := range payload {
		taskID, err := dbWrap.Enqueue(queue, []byte(p), headers, maxRetryCount)
		require.NoError(t, err)
		taskKeys[string(taskID)] = p
	}

	for taskID, payload := range taskKeys {
		_, err := dbWrap.Dequeue(queue)
		require.NoError(t, err)

		err = dbWrap.MoveToArchivedFromActive(queue, taskID)
		require.NoError(t, err)

		e := bdb.View(func(txn *badger.Txn) error {
			archivedQ := keyQueue(queue, []byte(generated.State_ARCHIVED.String()))
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(archivedQ); it.ValidForPrefix(archivedQ); it.Next() {
				item := it.Item()
				k := item.Key()
				err := item.Value(func(v []byte) error {
					fmt.Printf("archived key=%s, value=%s\n", k, v)
					return nil
				})
				if err != nil {
					return err
				}
			}

			taskItem := getTask(t, txn, taskID, headers, maxRetryCount)
			require.Equal(t, generated.State_ARCHIVED, taskItem.GetState())
			require.Equal(t, payload, string(taskItem.Payload))
			require.Equal(t, uint32(1), taskItem.RetryCount)

			taskRefItem := getRefItem(t, txn, queue, taskID)
			require.Empty(t, taskRefItem.LeaseKey)
			fmt.Println("archived ref", taskRefItem.Key)

			pendingQBytes, err := txn.Get([]byte(taskRefItem.Key))
			require.NoError(t, err)
			require.NotNil(t, pendingQBytes)
			return nil
		})
		require.NoError(t, e)
	}
}

// TODO: Implement TestMoveToCompletedFromActive
func (suite *OpstTestSuite) TestMoveToCompletedFromActive() {
	t := suite.T()
	bdb := suite.bdb
	l := log.NewLogger(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	dbWrap, err := New(bdb, l, nil)
	require.NoError(t, err)

	taskKeys := make(map[string]string)

	payload := []string{"world - 0", "world - 1"}
	queue := []byte("hello")
	headers := []byte("headers")
	maxRetryCount := uint32(3)

	for _, p := range payload {
		taskID, err := dbWrap.Enqueue(queue, []byte(p), headers, maxRetryCount)
		require.NoError(t, err)
		taskKeys[string(taskID)] = p
	}

	for taskID, payload := range taskKeys {
		_, err := dbWrap.Dequeue(queue)
		require.NoError(t, err)

		err = dbWrap.MoveToCompletedFromActive(queue, taskID)
		require.NoError(t, err)

		e := bdb.View(func(txn *badger.Txn) error {
			archivedQ := keyQueue(queue, []byte(generated.State_COMPLETED.String()))
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(archivedQ); it.ValidForPrefix(archivedQ); it.Next() {
				item := it.Item()
				k := item.Key()
				err := item.Value(func(v []byte) error {
					fmt.Printf("completed key=%s, value=%s\n", k, v)
					return nil
				})
				if err != nil {
					return err
				}
			}

			taskItem := getTask(t, txn, taskID, headers, maxRetryCount)
			require.Equal(t, generated.State_COMPLETED, taskItem.GetState())
			require.Equal(t, payload, string(taskItem.Payload))
			require.Equal(t, uint32(1), taskItem.RetryCount)

			taskRefItem := getRefItem(t, txn, queue, taskID)
			require.Empty(t, taskRefItem.LeaseKey)
			fmt.Println("archived ref", taskRefItem.Key)

			pendingQBytes, err := txn.Get([]byte(taskRefItem.Key))
			require.NoError(t, err)
			require.NotNil(t, pendingQBytes)
			return nil
		})
		require.NoError(t, e)
	}
}

func (suite *OpstTestSuite) TestForward() {
	t := suite.T()
	bdb := suite.bdb
	l := log.NewLogger(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	dbWrap, err := New(bdb, l, nil)
	require.NoError(t, err)

	taskKeys := make(map[string]string)

	payload := []string{"world - 0", "world - 1"}
	queue := []byte("hello")
	headers := []byte("headers")
	maxRetryCount := uint32(3)

	for _, p := range payload {
		taskID, err := dbWrap.Enqueue(queue, []byte(p), headers, maxRetryCount)
		require.NoError(t, err)
		taskKeys[string(taskID)] = p
	}

	for taskID, _ := range taskKeys {
		_, err := dbWrap.Dequeue(queue)
		require.NoError(t, err)

		err = dbWrap.MoveToRetryFromActive(queue, taskID)
		require.NoError(t, err)
	}

	require.NoError(t, dbWrap.Forward(queue, time.Now().Add(2 *time.Hour).Unix()))

	for taskID, payload := range taskKeys {
		e := bdb.View(func(txn *badger.Txn) error {
			pendingQ := keyQueue(queue, []byte(generated.State_PENDING.String()))
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(pendingQ); it.ValidForPrefix(pendingQ); it.Next() {
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

			taskItem := getTask(t, txn, taskID, headers, maxRetryCount)
			require.Equal(t, generated.State_PENDING, taskItem.GetState())
			require.Equal(t, payload, string(taskItem.Payload))
			require.Equal(t, uint32(1), taskItem.RetryCount)

			taskRefItem := getRefItem(t, txn, queue, taskID)
			require.Empty(t, taskRefItem.LeaseKey)
			fmt.Println("pending ref", taskRefItem.Key)

			pendingQBytes, err := txn.Get([]byte(taskRefItem.Key))
			require.NoError(t, err)
			require.NotNil(t, pendingQBytes)
			return nil
		})
		require.NoError(t, e)
	}
}

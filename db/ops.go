package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/abit2/pizza/task/task/generated"
	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

/*
 * Types of queues
 * Pending
 * Active
 * Retry
 * Archived
 * Completed
 */
/*
 * pause key should be empty when not present
 *
 | Redis         | Embedded DB Equivalent        |
 | ------------- | ----------------------------- |
 | List          | KV with incrementing sequence |
 | Hash          | KV encoding JSON / protobuf   |
 | ZSET          | Sorted keys by timestamp      |
 | Lua atomicity | DB WriteBatch                 |
*/

const (
	defaultLeaseDuration = 30 * time.Second
)

type Config struct {
	LeaseDuration time.Duration
}

type DB struct {
	db     *badger.DB
	logger *zap.Logger
	config *Config
}

func New(db *badger.DB, logger *zap.Logger, config *Config) (*DB, error) {
	if config == nil {
		config = &Config{
			LeaseDuration: defaultLeaseDuration,
		}
	}
	return &DB{db: db, logger: logger, config: config}, nil
}

var ErrQueueEmpty = errors.New("queue is empty")
var ErrInvalidQueue = errors.New("invalid queue")

func isValidQueue(queue []byte) bool {
	return strings.Contains(string(queue), "pending") ||
		strings.Contains(string(queue), "active") ||
		strings.Contains(string(queue), "retry") ||
		strings.Contains(string(queue), "archived") ||
		strings.Contains(string(queue), "completed")
}

func (db *DB) Enqueue(queue, value []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		taskID := uuid.New().String()

		taskBytes, err := db.marshalTask(&generated.Task{
			Id:      taskID,
			Payload: value,
			State:   generated.State_PENDING,
		})
		if err != nil {
			return err
		}

		err = txn.Set(keyTask(taskID), taskBytes)
		if err != nil {
			return err
		}

		err = pushToList(txn, keyPendingQueue(queue), taskID)
		if err != nil {
			return err
		}

		return nil
	})
}

func (db *DB) marshalTask(task *generated.Task) ([]byte, error) {
	return proto.Marshal(task)
}

func (db *DB) unmarshalTask(data []byte) (*generated.Task, error) {
	task := &generated.Task{}
	if err := proto.Unmarshal(data, task); err != nil {
		return nil, err
	}
	return task, nil
}

/*
 // Input:
 // KEYS[1] -> asynq:{<qname>}:pending
 // KEYS[2] -> asynq:{<qname>}:paused
 // KEYS[3] -> asynq:{<qname>}:active
 // KEYS[4] -> asynq:{<qname>}:lease
 // --
 // ARGV[1] -> initial lease expiration Unix time
 // ARGV[2] -> task key prefix
 //
 // Output:
 // Returns nil if no processable task is found in the given queue.
 // Returns an encoded TaskMessage.
 //
 // Note: dequeueCmd checks whether a queue is paused first, before
 // calling RPOPLPUSH to pop a task from the queue.
 * var dequeueCmd = redis.NewScript(`
 if redis.call("EXISTS", KEYS[2]) == 0 then
	local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
	if id then
		local key = ARGV[2] .. id
		redis.call("HSET", key, "state", "active")
		redis.call("HDEL", key, "pending_since")
		redis.call("ZADD", KEYS[4], ARGV[1], id)
		return redis.call("HGET", key, "msg")
	end
 end
 return nil`)
*/

func (db *DB) Dequeue(queue []byte) ([]byte, error) {
	var resp []byte
	err := db.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(keyPauseQueue(queue))
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}

		taskID, ok := popFromList(txn, keyPendingQueue(queue))
		if !ok {
			return ErrQueueEmpty
		}

		db.logger.Info("Dequeued task", zap.String("taskID", taskID))

		// push to active queue
		if err := pushToList(txn, keyActiveQueue(queue), taskID); err != nil {
			return err
		}

		// push to lease zset
		if err := pushToZSet(txn, keyLeaseQueue(time.Now().Add(db.config.LeaseDuration).Unix(), queue, taskID)); err != nil {
			return err
		}

		taskKey := keyTask(taskID)
		taskItem, err := txn.Get(taskKey)
		if err != nil {
			return err
		}

		var msg []byte
		msg, err = taskItem.ValueCopy(msg)
		if err != nil {
			return err
		}

		task, err := db.unmarshalTask(msg)
		if err != nil {
			return err
		}

		// Update the state of the task to active
		task.State = generated.State_ACTIVE
		updatedStateTaskBytes, err := db.marshalTask(task)
		if err != nil {
			return err
		}

		err = txn.Set(taskKey, updatedStateTaskBytes)
		if err != nil {
			return err
		}

		resp = task.GetPayload()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func pushToZSet(txn *badger.Txn, key []byte) error {
	return txn.Set(key, []byte{})
}

func pushToList(txn *badger.Txn, queueWithState []byte, taskID string) error {
	// check if queue is valid
	if !isValidQueue(queueWithState) {
		return ErrInvalidQueue
	}

	seqKey := keySeq(queueWithState)
	item, err := txn.Get(seqKey)
	if err != nil && err != badger.ErrKeyNotFound {
		return err
	}

	seq := 0
	if item != nil {
		seqBytes, _ := item.ValueCopy(nil)
		seq = int(binary.BigEndian.Uint64(seqBytes))
	}

	seq++
	seqBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBytes, uint64(seq))
	err = txn.Set(seqKey, seqBytes)
	if err != nil {
		return err
	}

	// --- write queue entry: queue:<name>:<seq> → jobID ---
	key := fmt.Sprintf("%s:%020d", queueWithState, seq)
	return txn.Set([]byte(key), []byte(taskID))
}

func popFromList(txn *badger.Txn, prefix []byte) (string, bool) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 1
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()

	it.Rewind()
	if !it.Valid() {
		return "", false
	}

	item := it.Item()
	key := item.KeyCopy(nil)

	jobIDBytes, _ := item.ValueCopy(nil)
	err := txn.Delete(key)
	if err != nil {
		return "", false
	}
	return string(jobIDBytes), true
}

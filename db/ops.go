package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/abit2/pizza/task/task/generated"
	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"go.uber.org/multierr"
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

var (
	ErrQueuePaused = errors.New("queue is paused")
	ErrNotFound    = errors.New("not found")
)

type Config struct {
	LeaseDuration time.Duration
	RetryFn       func(t time.Time) time.Time
}

type DB struct {
	db     *badger.DB
	logger *zap.Logger
	config *Config
}

func defaultRetryFn(t time.Time) time.Time {
	return t.Add(time.Minute)
}

func New(db *badger.DB, logger *zap.Logger, config *Config) (*DB, error) {
	if config == nil {
		config = &Config{
			LeaseDuration: defaultLeaseDuration,
			RetryFn:       defaultRetryFn,
		}
	}
	return &DB{db: db, logger: logger, config: config}, nil
}

var ErrQueueEmpty = errors.New("queue is empty")
var ErrInvalidQueue = errors.New("invalid queue")

func isValidQueue(queue []byte) bool {
	q := strings.ToLower(string(queue))
	return strings.Contains(q, "pending") ||
		strings.Contains(q, "active") ||
		strings.Contains(q, "retry") ||
		strings.Contains(q, "archived") ||
		strings.Contains(q, "completed")
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

		err = db.pushToList(txn, queue, []byte(generated.State_PENDING.String()), taskID)
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
	if data == nil {
		return nil, ErrNotFound
	}
	task := &generated.Task{}
	if err := proto.Unmarshal(data, task); err != nil {
		return nil, err
	}
	return task, nil
}

func (db *DB) unmarshalTaskReference(data []byte) (*generated.TaskReference, error) {
	if data == nil {
		return nil, ErrNotFound
	}
	task := &generated.TaskReference{}
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

func (db *DB) MoveToActiveFromPending(queue []byte) ([]byte, error) {
	var resp []byte
	err := db.db.Update(func(txn *badger.Txn) error {
		v, err := txn.Get(keyPauseQueue(queue))
		if err != nil && err != badger.ErrKeyNotFound {
			return multierr.Append(ErrQueuePaused, err)
		}
		if v != nil {
			var paused []byte
			paused, err = v.ValueCopy(paused)
			if err != nil {
				return err
			}
			isPaused, err := strconv.ParseBool(string(paused))
			if err != nil {
				return err
			}
			if isPaused {
				return ErrQueuePaused
			}
		}

		taskID, ok := popFromList(txn, keyQueue(queue, []byte(generated.State_PENDING.String())))
		if !ok {
			return ErrQueueEmpty
		}

		db.logger.Info("Dequeued task", zap.String("taskID", taskID))

		// push to active queue
		if err := db.pushToList(txn, queue, []byte(generated.State_ACTIVE.String()), taskID); err != nil {
			return err
		}

		// lease task
		if err := db.leaseTask(txn, queue, taskID); err != nil {
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

func (db *DB) pushToZSet(txn *badger.Txn, key []byte) error {
	return txn.Set(key, []byte{})
}

func (db *DB) pushToList(txn *badger.Txn, queue, state []byte, taskID string) error {
	queueWithState := keyQueue(queue, state)
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
	keyForQueue := fmt.Sprintf("%s:%020d", queueWithState, seq)
	err = txn.Set([]byte(keyForQueue), []byte(taskID))
	if err != nil {
		return err
	}

	taskRef, err := db.getReference(txn, queue, taskID)
	if err != nil && !(errors.Is(err, badger.ErrKeyNotFound) && string(state) == generated.State_PENDING.String()) {
		return err
	}
	if taskRef != nil {
		taskRef.Key = keyForQueue
	} else {
		taskRef = &generated.TaskReference{
			Key: keyForQueue,
			Id:  taskID,
		}
	}
	val, err := proto.Marshal(taskRef)
	if err != nil {
		return err
	}
	err = txn.Set(keyReference(queue, taskID), val)
	if err != nil {
		return err
	}

	return nil
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

func (db *DB) leaseTask(txn *badger.Txn, queue []byte, taskID string) error {
	timeTillLease := time.Now().Add(db.config.LeaseDuration).Unix()
	keyToZSet := keyLeaseQueue(timeTillLease, queue, taskID)

	taskRef, err := db.getReference(txn, queue, taskID)

	taskRef.LeaseKey = string(keyToZSet)
	taskRefBytes, err := proto.Marshal(taskRef)
	if err != nil {
		return err
	}

	// push to lease reference
	keyReference := keyReference(queue, taskID)
	err = txn.Set(keyReference, taskRefBytes)
	if err != nil {
		return err
	}

	// push to lease zset
	if err := db.pushToZSet(txn, keyToZSet); err != nil {
		return err
	}
	return nil
}

func (db *DB) MoveToRetryFromActive(queue []byte, taskID string) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		v, err := txn.Get(keyPauseQueue(queue))
		if err != nil && err != badger.ErrKeyNotFound {
			return multierr.Append(ErrQueuePaused, err)
		}

		if v != nil {
			var paused []byte
			paused, err = v.ValueCopy(paused)
			if err != nil {
				return err
			}
			isPaused, err := strconv.ParseBool(string(paused))
			if err != nil {
				return err
			}
			if isPaused {
				return ErrQueuePaused
			}
		}

		// always delete before overwriting the reference
		// change the state of the task to retry state
		// delete from the active queue
		refKey := keyReference(queue, taskID)
		refItem, err := txn.Get(refKey)
		if err != nil {
			return err
		}
		var refBytes []byte
		refBytes, err = refItem.ValueCopy(refBytes)
		if err != nil {
			return err
		}

		taskRef, err := db.unmarshalTaskReference(refBytes)
		if err != nil {
			return err
		}

		// delete from the active queue
		err = txn.Delete([]byte(taskRef.Key))
		if err != nil {
			return err
		}

		taskItem, err := txn.Get(keyTask(taskID))
		if err != nil {
			return err
		}

		var taskBytes []byte
		taskBytes, err = taskItem.ValueCopy(taskBytes)
		if err != nil {
			return err
		}

		task, err := db.unmarshalTask(taskBytes)
		if err != nil {
			return err
		}

		task.State = generated.State_RETRY
		taskBytes, err = db.marshalTask(task)
		if err != nil {
			return err
		}

		err = txn.Set(keyTask(taskID), taskBytes)
		if err == nil {
			return err
		}

		retryTime := db.config.RetryFn(time.Now())
		keyToZSet := keyZSet(retryTime.Unix(), queue, []byte(generated.State_RETRY.String()), taskID)

		err = db.pushToZSet(txn, keyToZSet)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) getReference(txn *badger.Txn, queue []byte, taskID string) (*generated.TaskReference, error) {
	refKey := keyReference(queue, taskID)
	refItem, err := txn.Get(refKey)
	if err != nil {
		return nil, err
	}
	var refBytes []byte
	refBytes, err = refItem.ValueCopy(refBytes)
	if err != nil {
		return nil, err
	}

	taskRef, err := db.unmarshalTaskReference(refBytes)
	if err != nil {
		return nil, err
	}

	return taskRef, nil
}

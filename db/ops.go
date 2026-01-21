package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/abit2/pizza/log"
	"github.com/abit2/pizza/task/task/generated"
	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/multierr"
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

// ExtendLease extends the lease for an active task by pushing a new lease key
// with the configured lease duration and deleting the previous lease key.
func (db *DB) ExtendLease(_ context.Context, qname, id string) (int64, error) {
	queue := []byte(qname)
	taskID := id
	var leaseTill int64

	if err := db.db.Update(func(txn *badger.Txn) error {
		if pausedErr := db.checkPausedQueue(txn, queue); pausedErr != nil {
			return errors.Wrap(pausedErr, "failed to check paused queue")
		}

		taskRef, err := db.getReference(txn, queue, taskID)
		if err != nil {
			return errors.Wrap(err, "failed to get reference")
		}

		// delete the previous lease key (if any)
		if taskRef.LeaseKey != "" {
			if err := txn.Delete([]byte(taskRef.LeaseKey)); err != nil {
				return errors.Wrap(err, "failed to delete previous lease")
			}
		}

		// lease task
		leaseTill, err = db.leaseTask(txn, queue, taskID)
		if err != nil {
			return errors.Wrap(err, "failed to lease task")
		}

		return nil
	}); err != nil {
		return 0, errors.Wrap(err, "ExtendLease")
	}
	return leaseTill, nil
}

var (
	ErrQueuePaused = errors.New("queue is paused")
	ErrNotFound    = errors.New("not found")
	ErrTaskRef     = errors.New("task ref cannot be nil")
)

type Config struct {
	LeaseDuration time.Duration
	RetryFn       func(t time.Time) time.Time
}

type Clock interface {
	Now() time.Time
}

type DB struct {
	db     *badger.DB
	logger *log.Logger
	config *Config
	clock  Clock
}

func defaultRetryFn(t time.Time) time.Time {
	return t.Add(time.Minute)
}

func New(db *badger.DB, logger *log.Logger, config *Config, cl Clock) (*DB, error) {
	if config == nil {
		config = &Config{
			LeaseDuration: defaultLeaseDuration,
			RetryFn:       defaultRetryFn,
		}
	}
	if config.LeaseDuration < defaultLeaseDuration {
		config.LeaseDuration = defaultLeaseDuration
	}
	return &DB{db: db, logger: logger, config: config, clock: cl}, nil
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

func (db *DB) Enqueue(queue, payload, headers []byte, maxRetry uint32) ([]byte, error) {
	taskID := uuid.New().String()
	err := db.db.Update(func(txn *badger.Txn) error {
		taskBytes, err := db.marshalTask(&generated.Task{
			Id:         taskID,
			Payload:    payload,
			State:      generated.State_PENDING,
			Headers:    headers,
			RetryCount: 0,
			MaxRetries: maxRetry,
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
	if err != nil {
		return nil, err
	}
	return []byte(taskID), nil
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

// Dequeue = MoveToActiveFromPending moves a task from pending queue to active queue.
func (db *DB) Dequeue(queue []byte) (*generated.Task, error) {
	var resp *generated.Task
	err := db.db.Update(func(txn *badger.Txn) error {
		if pausedErr := db.checkPausedQueue(txn, queue); pausedErr != nil {
			return pausedErr
		}

		taskID, ok := popFromList(txn, keyQueue(queue, []byte(generated.State_PENDING.String())))
		if !ok {
			return ErrQueueEmpty
		}

		db.logger.Info("Dequeued task", "taskID", taskID)

		// push to active queue
		if err := db.pushToList(txn, queue, []byte(generated.State_ACTIVE.String()), taskID); err != nil {
			return err
		}

		// lease task
		_, err := db.leaseTask(txn, queue, taskID)
		if err != nil {
			return err
		}

		task, err := db.getTask(txn, taskID)
		if err != nil {
			return err
		}

		// Update the state of the task to active
		task.State = generated.State_ACTIVE
		task.RetryCount += 1
		updatedStateTaskBytes, err := db.marshalTask(task)
		if err != nil {
			return err
		}

		err = txn.Set(keyTask(taskID), updatedStateTaskBytes)
		if err != nil {
			return err
		}

		resp = task
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
		taskRef.LeaseKey = ""
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

func (db *DB) pushToZSetQueue(txn *badger.Txn, timeTill int64, queue []byte, taskID string, state []byte, isLeaseQueue bool) error {
	taskRef, err := db.getReference(txn, queue, taskID)
	if err != nil {
		return err
	}

	// it should not be possible that the taskRef is nil
	if taskRef == nil {
		return ErrTaskRef
	}

	var keyToZSet []byte
	if isLeaseQueue {
		keyToZSet = keyLeaseQueue(timeTill, queue, taskID)
		taskRef.LeaseKey = string(keyToZSet)
	} else {
		keyToZSet = keyZSet(timeTill, queue, state, taskID)
		taskRef.Key = string(keyToZSet)
		taskRef.LeaseKey = ""
	}

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

func (db *DB) leaseTask(txn *badger.Txn, queue []byte, taskID string) (int64, error) {
	timeTillLease := db.clock.Now().Add(db.config.LeaseDuration).Unix()
	err := db.pushToZSetQueue(txn, timeTillLease, queue, taskID, []byte(leaseState), true)
	if err != nil {
		return 0, err
	}

	return timeTillLease, nil
}

func (db *DB) MoveToArchivedFromActive(queue []byte, taskID string) error {
	return db.db.Update(func(txn *badger.Txn) error {
		if pausedErr := db.checkPausedQueue(txn, queue); pausedErr != nil {
			return pausedErr
		}

		// always delete before overwriting the reference
		// change the state of the task to retry state
		// delete from the active queue
		taskRef, err := db.getReference(txn, queue, taskID)
		if err != nil {
			return err
		}

		// delete from the existing queue - active
		err = txn.Delete([]byte(taskRef.Key))
		if err != nil {
			return err
		}

		// delete the lease
		err = txn.Delete([]byte(taskRef.LeaseKey))
		if err != nil {
			return err
		}

		task, err := db.getTask(txn, taskID)
		if err != nil {
			return err
		}
		if task == nil {
			return ErrNotFound
		}

		stateToMove := generated.State_ARCHIVED

		task.State = stateToMove
		taskBytes, err := db.marshalTask(task)
		if err != nil {
			return err
		}

		if setErr := txn.Set(keyTask(taskID), taskBytes); setErr != nil {
			return setErr
		}

		if pushErr := db.pushToList(txn, queue, []byte(stateToMove.String()), taskID); pushErr != nil {
			return pushErr
		}
		return nil
	})
}

func (db *DB) MoveToCompletedFromActive(queue []byte, taskID string) error {
	return db.db.Update(func(txn *badger.Txn) error {
		if pausedErr := db.checkPausedQueue(txn, queue); pausedErr != nil {
			return pausedErr
		}

		// always delete before overwriting the reference
		// change the state of the task to retry state
		// delete from the active queue
		taskRef, err := db.getReference(txn, queue, taskID)
		if err != nil {
			return err
		}

		// delete from the existing queue - active
		err = txn.Delete([]byte(taskRef.Key))
		if err != nil {
			return err
		}

		// delete the lease
		err = txn.Delete([]byte(taskRef.LeaseKey))
		if err != nil {
			return err
		}

		task, err := db.getTask(txn, taskID)
		if err != nil {
			return err
		}
		if task == nil {
			return ErrNotFound
		}

		stateToMove := generated.State_COMPLETED
		task.State = stateToMove
		taskBytes, err := db.marshalTask(task)
		if err != nil {
			return err
		}

		if setErr := txn.Set(keyTask(taskID), taskBytes); setErr != nil {
			return setErr
		}

		if pushErr := db.pushToList(txn, queue, []byte(stateToMove.String()), taskID); pushErr != nil {
			return pushErr
		}
		return nil
	})
}

func (db *DB) MoveToRetryFromActive(queue []byte, taskID string) error {
	err := db.db.Update(func(txn *badger.Txn) error {
		if pausedErr := db.checkPausedQueue(txn, queue); pausedErr != nil {
			return errors.Wrap(pausedErr, "failed to check paused queue")
		}

		// always delete before overwriting the reference
		// change the state of the task to retry state
		// delete from the active queue
		taskRef, err := db.getReference(txn, queue, taskID)
		if err != nil {
			return errors.Wrap(err, "failed to get reference")
		}

		db.logger.Debug("task ref", "reference", taskRef)
		// delete from the existing queue - active
		err = txn.Delete([]byte(taskRef.Key))
		if err != nil {
			return errors.Wrap(err, "failed to delete from active queue")
		}

		// delete the lease
		err = txn.Delete([]byte(taskRef.LeaseKey))
		if err != nil {
			return errors.Wrap(err, "failed to delete lease")
		}

		task, err := db.getTask(txn, taskID)
		if err != nil {
			return errors.Wrap(err, "failed to get task")
		}
		if task == nil {
			return errors.Wrap(ErrNotFound, "task not found")
		}

		task.State = generated.State_RETRY
		taskBytes, err := db.marshalTask(task)
		if err != nil {
			return errors.Wrap(err, "failed to marshal task")
		}

		err = txn.Set(keyTask(taskID), taskBytes)
		if err != nil {
			return errors.Wrap(err, "failed to set task")
		}

		retryTime := db.config.RetryFn(db.clock.Now())
		err = db.pushToZSetQueue(txn, retryTime.Unix(), queue, taskID, []byte(generated.State_RETRY.String()), false)
		if err != nil {
			return errors.Wrap(err, "failed to push to zset")
		}

		return nil
	})
	if err != nil {
		return errors.Wrap(err, "MoveToRetryFromActive")
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

func (db *DB) getTask(txn *badger.Txn, taskID string) (*generated.Task, error) {
	taskItem, err := txn.Get(keyTask(taskID))
	if err != nil {
		return nil, err
	}

	var taskBytes []byte
	taskBytes, err = taskItem.ValueCopy(taskBytes)
	if err != nil {
		return nil, err
	}

	task, err := db.unmarshalTask(taskBytes)
	if err != nil {
		return nil, err
	}
	return task, nil
}

func (db *DB) checkPausedQueue(txn *badger.Txn, queue []byte) error {
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
	return nil
}

func (db *DB) MoveToPendingFromRetry(queue []byte, taskID string) error {
	if err := db.db.Update(func(txn *badger.Txn) error {
		return db.processRetryQueue(txn, queue, taskID)
	}); err != nil {
		return errors.Wrap(err, "MoveToPendingFromRetry")
	}
	return nil
}

func (db *DB) processRetryQueue(txn *badger.Txn, queue []byte, taskID string) error {
	if pausedErr := db.checkPausedQueue(txn, queue); pausedErr != nil {
		return pausedErr
	}

	taskRef, err := db.getReference(txn, queue, taskID)
	if err != nil {
		return err
	}

	deleteErr := txn.Delete([]byte(taskRef.Key))
	if deleteErr != nil {
		return deleteErr
	}

	task, err := db.getTask(txn, taskID)
	if err != nil {
		return err
	}
	if task == nil {
		return ErrNotFound
	}

	task.State = generated.State_PENDING
	taskBytes, err := db.marshalTask(task)
	if err != nil {
		return err
	}

	if setErr := txn.Set(keyTask(taskID), taskBytes); setErr != nil {
		return setErr
	}

	if pushErr := db.pushToList(txn, queue, []byte(generated.State_PENDING.String()), taskID); pushErr != nil {
		return pushErr
	}
	return nil
}

// Foward moves tasks from retry state to pending state
func (db *DB) Forward(queue []byte, now int64) error {
	if err := db.db.Update(func(txn *badger.Txn) error {
		// iterate and foward all which are behind the timestamp in zset
		prefix := fmt.Sprintf("pizza:%s:%s:", queue, generated.State_RETRY.String())
		upperBound := fmt.Sprintf("%s%020d", prefix, now)

		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			if bytes.Compare(key, []byte(upperBound)) > 0 {
				break
			}

			taskID := db.taskIDFromKeyOfZset(key)
			if err := db.processRetryQueue(txn, queue, string(taskID)); err != nil {
				return err
			}

			db.logger.Debug("items pushed to pending", "key", string(key))
		}
		return nil
	}); err != nil {
		return errors.Wrap(err, "Forward")
	}
	return nil
}

func (db *DB) taskIDFromKeyOfZset(key []byte) []byte {
	idx := bytes.LastIndexByte(key, ':')
	if idx == -1 || idx+1 >= len(key) {
		return nil
	}
	return key[idx+1:]
}

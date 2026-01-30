package pizza

import (
	"context"
	"runtime/debug"
	"sync"
	"time"

	"github.com/abit2/pizza/log"
	"golang.org/x/sync/errgroup"
)

const (
	defaultJitter = 2 * time.Second
)

type recoverDB interface {
	RecoverExpiredTasks(queue []byte, timeStamp int64) error
	GetAllQueues() ([]string, error)
}

type Recovery struct {
	logger      *log.Logger
	avgInterval time.Duration

	expiredTasks <-chan *taskInfoHeartBeat

	done chan struct{}
	db   recoverDB

	lock           sync.RWMutex
	tasksToRecover []*taskInfoHeartBeat
}

func NewRecovery(l *log.Logger, interval time.Duration, claimed, finished <-chan *taskInfoHeartBeat, expiredTasks <-chan *taskInfoHeartBeat, db recoverDB, durationBeforeLeaseExpiry time.Duration) *Recovery {
	return &Recovery{
		logger:       l,
		avgInterval:  interval,
		expiredTasks: expiredTasks,
		db:           db,
		done:         make(chan struct{}),
	}
}

func (r *Recovery) start(ctx context.Context) {
	timer := time.NewTimer(5 * time.Millisecond)
	for {
		select {
		case <-timer.C:
			if err := r.exec(ctx); err != nil {
				r.logger.Error("err exec", "err", err.Error())
			}
			timer.Reset(r.avgInterval)
			// case <-ctx.Done():
			return
		case <-r.done:
			r.logger.Info("heartbeat: done")
			return
		case info := <-r.expiredTasks:
			r.lock.Lock()
			r.tasksToRecover = append(r.tasksToRecover, info)
			r.lock.Unlock()
		}
	}
}

func (r *Recovery) stop() {
	close(r.done)
}

func (r *Recovery) exec(ctx context.Context) error {
	defer func() {
		if x := recover(); x != nil {
			r.logger.Error("recovering from panic. See the stack trace below for details", "stack", string(debug.Stack()))
		}
	}()

	r.lock.Lock()
	defer r.lock.Unlock()

	// TODO: recover all the task not just the ones in the queue
	// AllQueues = queues
	queues, err := r.db.GetAllQueues()
	if err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(ctx)
	for _, queue := range queues {
		queue := queue
		g.Go(func() error {
			if err := r.db.RecoverExpiredTasks([]byte(queue), time.Now().Add(-1*defaultJitter).Unix()); err != nil {
				r.logger.Error("failed to recover tasks for queue", "queue", queue, "err", err.Error())
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

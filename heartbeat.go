package pizza

import (
	"context"
	"sync"
	"time"

	"github.com/abit2/pizza/log"
)

type heartBeatDB interface {
	ExtendLease(ctx context.Context, qname, id string) (int64, error)
}

type HeartBeat struct {
	logger      *log.Logger
	avgInterval time.Duration

	claimed      <-chan *taskInfoHeartBeat
	finished     <-chan *taskInfoHeartBeat
	expiredTasks chan<- *taskInfoHeartBeat

	done chan struct{}

	tasks sync.Map

	durationBeforeLeaseExpiry time.Duration
	db                        heartBeatDB
}

type taskInfoHeartBeat struct {
	ID          string
	QueueName   string
	LeaseTill   time.Time
	StartTime   time.Time
	FinishTime  time.Time
	ExecTimeout time.Time
}

func NewHearBeater(l *log.Logger, interval time.Duration, claimed, finished <-chan *taskInfoHeartBeat, expiredTasks chan<- *taskInfoHeartBeat, db heartBeatDB, durationBeforeLeaseExpiry time.Duration) *HeartBeat {
	return &HeartBeat{
		logger:                    l,
		avgInterval:               interval,
		claimed:                   claimed,
		finished:                  finished,
		expiredTasks:              expiredTasks,
		durationBeforeLeaseExpiry: durationBeforeLeaseExpiry,
		db:                        db,
		done:                      make(chan struct{}),
	}
}

func (hb *HeartBeat) start(ctx context.Context) {
	timer := time.NewTimer(5 * time.Millisecond)
	for {
		select {
		case <-timer.C:
			if err := hb.exec(ctx); err != nil {
				hb.logger.Error("err exec", "err", err.Error())
			}
			timer.Reset(hb.avgInterval)
			// case <-ctx.Done():
			// 	hb.logger.Info("heartbeat: ctx cancelled")
			return
		case <-hb.done:
			hb.logger.Info("heartbeat: done")
			return
		case info := <-hb.claimed:
			hb.tasks.Store(info.ID, info)
		case info := <-hb.finished:
			hb.tasks.Delete(info.ID)
		}
	}
}

func (hb *HeartBeat) stop() {
	close(hb.done)
}

func (hb *HeartBeat) exec(ctx context.Context) error {
	now := time.Now()
	var firstErr error

	hb.tasks.Range(func(key, value any) bool {
		taskInfo, ok := value.(*taskInfoHeartBeat)
		if !ok || taskInfo == nil {
			return true
		}

		// if the lease is already expired, do not extend; requeue for recovery
		if taskInfo.LeaseTill.Before(now) {
			// send expired task event to channel
			select {
			case hb.expiredTasks <- taskInfo:
			case <-ctx.Done():
				return false
			default:
				// channel full or not ready, log but continue
				hb.logger.Warn("expiredTasks channel full, dropping expired task event", "taskID", taskInfo.ID)
			}
			return true
		}

		// extend lease when it's about to expire (but not yet expired)
		if taskInfo.LeaseTill.Sub(now) <= hb.durationBeforeLeaseExpiry {
			// check if exec ttl is expired already

			leaseTill, err := hb.db.ExtendLease(ctx, taskInfo.QueueName, taskInfo.ID)
			if err != nil {
				hb.logger.Error("err extend lease", "err", err.Error())
				if firstErr == nil {
					firstErr = err
				}
			}
			taskInfo.LeaseTill = time.Unix(leaseTill, 0)
		}
		return true
	})
	return firstErr
}

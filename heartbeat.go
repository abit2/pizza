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

type heartBeat struct {
	logger      *log.Logger
	avgInterval time.Duration

	claimed      <-chan *taskInfoHeartBeat
	finished     <-chan *taskInfoHeartBeat
	expiredTasks chan<- *taskInfoHeartBeat

	tasks sync.Map

	durationBeforeLeaseExpiry time.Duration
	db                        heartBeatDB
}

type taskInfoHeartBeat struct {
	ID        string
	QueueName string
	LeaseTill time.Time
	StartTime time.Time
}

func NewHearBeater(l *log.Logger, interval time.Duration, claimed, finished <-chan *taskInfoHeartBeat, expiredTasks chan<- *taskInfoHeartBeat, db heartBeatDB, durationBeforeLeaseExpiry time.Duration) *heartBeat {
	return &heartBeat{
		logger:                    l,
		avgInterval:               interval,
		claimed:                   claimed,
		finished:                  finished,
		expiredTasks:              expiredTasks,
		durationBeforeLeaseExpiry: durationBeforeLeaseExpiry,
		db:                        db,
	}
}

func (hb *heartBeat) start(ctx context.Context) {
	timer := time.NewTimer(5 * time.Millisecond)
	for {
		select {
		case <-timer.C:
			if err := hb.exec(ctx); err != nil {
				hb.logger.Error("err exec", "err", err.Error())
			}
			timer.Reset(hb.avgInterval)
		case <-ctx.Done():
			hb.logger.Info("promise job: ctx cancelled")
			return
		case info := <-hb.claimed:
			hb.tasks.Store(info.ID, info)
		case info := <-hb.finished:
			hb.tasks.Delete(info.ID)
		}
	}
}

func (hb *heartBeat) exec(ctx context.Context) error {
	now := time.Now()
	var firstErr error

	hb.tasks.Range(func(key, value any) bool {
		info, ok := value.(*taskInfoHeartBeat)
		if !ok || info == nil {
			return true
		}

		// if the lease is already expired, do not extend; requeue for recovery
		if info.LeaseTill.Before(now) {
			// send expired task event to channel
			select {
			case hb.expiredTasks <- info:
			case <-ctx.Done():
				return false
			default:
				// channel full or not ready, log but continue
				hb.logger.Warn("expiredTasks channel full, dropping expired task event", "taskID", info.ID)
			}
			return true
		}

		// extend lease when it's about to expire (but not yet expired)
		if info.LeaseTill.Sub(now) <= hb.durationBeforeLeaseExpiry {
			leaseTill, err := hb.db.ExtendLease(ctx, info.QueueName, info.ID)
			if err != nil {
				hb.logger.Error("err extend lease", "err", err.Error())
				if firstErr == nil {
					firstErr = err
				}
			}
			info.LeaseTill = time.Unix(leaseTill, 0)
		}
		return true
	})
	return firstErr
}

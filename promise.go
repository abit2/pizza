package pizza

import (
	"context"
	"time"

	"github.com/abit2/pizza/log"
	"go.uber.org/multierr"
)

// TODO: write tests for promise/forwarder
type Forwarder interface {
	Forward(queue []byte, now int64) error
}

type Clock interface {
	Now() time.Time
}

// Promise is responsible to move tasks from retry to pending queue
type Promise struct {
	logger      *log.Logger
	avgInterval time.Duration
	queues      []string
	forwarder   Forwarder
	clock       Clock
	done        chan struct{}
}

func NewPromise(l *log.Logger, interval time.Duration, queues []string, f Forwarder, cl Clock) *Promise {
	return &Promise{
		logger:      l,
		avgInterval: interval,
		queues:      queues,
		forwarder:   f,
		clock:       cl,
		done:        make(chan struct{}),
	}
}

func (prom *Promise) start(ctx context.Context) {
	timer := time.NewTimer(5 * time.Millisecond)
	for {
		select {
		case <-timer.C:
			if err := prom.exec(ctx); err != nil {
				prom.logger.Error("err exec", "err", err.Error())
			}
			timer.Reset(prom.avgInterval)
		case <-prom.done:
			timer.Stop()
			prom.logger.Info("promise job: ctx cancelled")
			return
		}
	}
}

func (prom *Promise) stop() {
	close(prom.done)
}

func (prom *Promise) exec(ctx context.Context) error {
	prom.logger.Debug("exec promise")
	var err error
	for _, queue := range prom.queues {
		timeUntil := prom.clock.Now().UTC()
		if e := prom.forwarder.Forward([]byte(queue), timeUntil.Unix()); e != nil {
			multierr.Append(err, e)
		}
	}
	return err
}

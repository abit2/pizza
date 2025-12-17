package utils

import (
	"sync"
	"time"

	"github.com/abit2/pizza/log"
)

type RealClock struct {
}

func NewRealClock() *RealClock {
	return &RealClock{}
}

func (_ *RealClock) Now() time.Time {
	return time.Now()
}

type FakeClock struct {
	lock     sync.RWMutex
	t        time.Time
	interval time.Duration
	l        *log.Logger
}

func NewFakeClock(t time.Time, l *log.Logger, interval time.Duration) *FakeClock {
	return &FakeClock{
		t:        t,
		l:        l,
		interval: interval,
	}
}

func (fcl *FakeClock) Now() time.Time {
	fcl.lock.Lock()
	defer fcl.lock.Unlock()

	old := fcl.t
	fcl.t = fcl.t.Add(fcl.interval)
	fcl.l.Debug("moving forward", "curr", old.UTC(), "new", fcl.t.UTC())
	return old
}

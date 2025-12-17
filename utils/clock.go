package utils

import (
	"sync"
	"time"
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
	lock sync.RWMutex
	t    time.Time
}

func NewFakeClock(t time.Time) *FakeClock {
	return &FakeClock{
		t: t,
	}
}

func (fcl *FakeClock) Now() time.Time {
	fcl.lock.Lock()
	defer fcl.lock.Unlock()

	return fcl.t
}

package pizza

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abit2/pizza/log"
	"github.com/abit2/pizza/utils"
)

type fakeHeartBeatDB struct {
	calls []struct {
		qname string
		id    string
	}
	returnErr error
}

func (f *fakeHeartBeatDB) ExtendLease(_ context.Context, qname, id string) (int64, error) {
	f.calls = append(f.calls, struct {
		qname string
		id    string
	}{qname: qname, id: id})
	return time.Now().Unix(), f.returnErr
}

func newTestHeartBeat(t *testing.T, db heartBeatDB, durationBeforeExpiry time.Duration, expiredCh chan<- *taskInfoHeartBeat) *heartBeat {
	t.Helper()

	if db == nil {
		db = &fakeHeartBeatDB{}
	}
	if expiredCh == nil {
		expiredCh = make(chan *taskInfoHeartBeat, 1)
	}

	return NewHearBeater(
		log.NewLogger(&utils.NoopLogger{}),
		time.Second,
		nil,
		nil,
		expiredCh,
		db,
		durationBeforeExpiry,
	)
}

func TestHeartBeatExec_DoesNotExtendOrEmit_WhenLeaseFarFromExpiry(t *testing.T) {
	fakeDB := &fakeHeartBeatDB{}
	expiredCh := make(chan *taskInfoHeartBeat, 1)
	hb := newTestHeartBeat(t, fakeDB, time.Second, expiredCh)

	now := time.Now()
	info := &taskInfoHeartBeat{
		ID:        "task-1",
		QueueName: "q1",
		LeaseTill: now.Add(5 * time.Second), // far in the future
	}
	hb.tasks.Store(info.ID, info)

	if err := hb.exec(context.Background()); err != nil {
		t.Fatalf("exec returned error: %v", err)
	}

	if len(fakeDB.calls) != 0 {
		t.Fatalf("expected no ExtendLease calls, got %d", len(fakeDB.calls))
	}

	select {
	case <-expiredCh:
		t.Fatalf("did not expect expired task event")
	default:
	}
}

func TestHeartBeatExec_ExtendsLease_WhenCloseToExpiry(t *testing.T) {
	fakeDB := &fakeHeartBeatDB{}
	expiredCh := make(chan *taskInfoHeartBeat, 1)
	hb := newTestHeartBeat(t, fakeDB, time.Second, expiredCh)

	now := time.Now()
	info := &taskInfoHeartBeat{
		ID:        "task-2",
		QueueName: "q2",
		LeaseTill: now.Add(200 * time.Millisecond), // within durationBeforeLeaseExpiry
	}
	hb.tasks.Store(info.ID, info)

	if err := hb.exec(context.Background()); err != nil {
		t.Fatalf("exec returned error: %v", err)
	}

	if len(fakeDB.calls) != 1 {
		t.Fatalf("expected 1 ExtendLease call, got %d", len(fakeDB.calls))
	}
	if fakeDB.calls[0].qname != "q2" || fakeDB.calls[0].id != "task-2" {
		t.Fatalf("unexpected ExtendLease args: %+v", fakeDB.calls[0])
	}

	select {
	case <-expiredCh:
		t.Fatalf("did not expect expired task event")
	default:
	}
}

func TestHeartBeatExec_EmitsExpiredEvent_WhenLeaseExpired(t *testing.T) {
	fakeDB := &fakeHeartBeatDB{}
	expiredCh := make(chan *taskInfoHeartBeat, 1)
	hb := newTestHeartBeat(t, fakeDB, time.Second, expiredCh)

	now := time.Now()
	info := &taskInfoHeartBeat{
		ID:        "task-3",
		QueueName: "q3",
		LeaseTill: now.Add(-time.Second), // already expired
	}
	hb.tasks.Store(info.ID, info)

	if err := hb.exec(context.Background()); err != nil {
		t.Fatalf("exec returned error: %v", err)
	}

	// should not attempt to extend lease
	if len(fakeDB.calls) != 0 {
		t.Fatalf("expected no ExtendLease calls for expired task, got %d", len(fakeDB.calls))
	}

	select {
	case got := <-expiredCh:
		if got.ID != info.ID || got.QueueName != info.QueueName {
			t.Fatalf("unexpected expired task info: %+v", got)
		}
	default:
		t.Fatalf("expected expired task event to be sent")
	}
}

func TestHeartBeatExec_PropagatesFirstExtendLeaseError(t *testing.T) {
	fakeErr := errors.New("extend failed")
	fakeDB := &fakeHeartBeatDB{returnErr: fakeErr}
	expiredCh := make(chan *taskInfoHeartBeat, 1)
	hb := newTestHeartBeat(t, fakeDB, time.Second, expiredCh)

	now := time.Now()
	info := &taskInfoHeartBeat{
		ID:        "task-4",
		QueueName: "q4",
		LeaseTill: now.Add(100 * time.Millisecond), // will cause ExtendLease
	}
	hb.tasks.Store(info.ID, info)

	err := hb.exec(context.Background())
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, fakeErr) {
		t.Fatalf("expected error %v, got %v", fakeErr, err)
	}
}

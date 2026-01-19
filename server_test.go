package pizza_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abit2/pizza"
	"github.com/abit2/pizza/db"
	"github.com/abit2/pizza/log"
	"github.com/abit2/pizza/task/task/generated"
	"github.com/abit2/pizza/utils"
	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestServer(t *testing.T) {
	l := testSlogLogger()
	path := "./server"
	defer func() {
		l.Info("removing os.RemoveAll")
		require.NoError(t, os.RemoveAll(path))
	}()

	bdb, err := badger.Open(badger.DefaultOptions(path))
	require.NoError(t, err)
	defer func() {
		l.Info("closing badger db")
		require.NoError(t, bdb.Close())
	}()

	staticTime := time.Now()
	fakeClock := utils.NewFakeClock(staticTime, l, 15*time.Second)
	dbWrap, err := db.New(bdb, l, &db.Config{
		LeaseDuration: 20 * time.Second,
		RetryFn: func(t time.Time) time.Time {
			return t.Add(500 * time.Millisecond)
		},
	}, fakeClock)
	require.NoError(t, err)
	taskServer := pizza.NewServer(l, dbWrap, &pizza.ServerConfig{
		Queues:          []string{"q"},
		Concurrency:     10,
		PromiseInterval: 1 * time.Second,
	}, fakeClock)

	ctx, cancel := context.WithCancel(context.TODO())
	var wg sync.WaitGroup

	wg.Go(func() {
		l.Debug("inserting task")
		payload := []byte("hello world")
		headers := map[string]string{
			pizza.TaskType: "task_1",
		}
		marshaledHeader, err := json.Marshal(headers)
		require.NoError(t, err)
		maxRetry := 3
		_, err = dbWrap.Enqueue([]byte("q"), payload, marshaledHeader, uint32(maxRetry))
		require.NoError(t, err)
	})

	l.Debug("running the taskserver")
	taskServer.Run(ctx, &wg)

	l.Debug("setting up handlers")
	dh := &DummyHandler{
		l: l,
	}
	taskServer.SetupHandler(map[string]pizza.Handler{
		"task_1": dh,
	})

	l.Debug("starting to sleep")
	time.Sleep(2 * time.Second)
	cancel()

	wg.Wait()

	require.Equal(t, int32(2), dh.count.Load())
}

type DummyHandler struct {
	l     *log.Logger
	count atomic.Int32
}

func (dh *DummyHandler) Process(ctx context.Context, task *generated.Task) error {
	dh.count.Add(1)
	if task.GetRetryCount() > 1 {
		dh.l.Debug("success")
		return nil
	}
	dh.l.Debug("failure")
	return errors.New("try again")
}

func testSlogLogger() *log.Logger {
	lvl := new(slog.LevelVar)
	lvl.Set(slog.LevelDebug)
	return log.NewLogger(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: lvl,
	})))
}

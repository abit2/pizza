package pizza

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abit2/pizza/db"
	"github.com/abit2/pizza/log"
	"github.com/abit2/pizza/task/task/generated"
	"github.com/abit2/pizza/utils"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type ProcessorTestSuite struct {
	suite.Suite
	path string
	bdb  *badger.DB
}

func TestProcessorTestSuite(t *testing.T) {
	suite.Run(t, new(ProcessorTestSuite))
}

func insertSomeData(t *testing.T, db *db.DB, queues []string, taskTypeMapping map[string]string) {
	t.Helper()

	for _, queue := range queues {
		headers := map[string]string{TaskType: taskTypeMapping[queue]}
		hbyte, err := json.Marshal(headers)
		require.NoError(t, err)
		taskId, err := db.Enqueue([]byte(queue), []byte("hello world"), hbyte, 3)
		require.NoError(t, err)
		require.NotNil(t, taskId)
	}
}

func (suite *ProcessorTestSuite) SetupSuite() {
	var err error
	if suite.bdb != nil {
		return
	}
	suite.path = "./processor_data"
	suite.bdb, err = badger.Open(badger.DefaultOptions(suite.path))
	require.NoError(suite.T(), err)

}

func (suite *ProcessorTestSuite) TearDownSuite() {
	err := suite.bdb.Close()
	require.NoError(suite.T(), err)
	// Remove all items from the queue
	err = os.RemoveAll(suite.path)
	require.NoError(suite.T(), err)
}

func (suite *ProcessorTestSuite) Test_Start() {
	t := suite.T()
	lvl := new(slog.LevelVar)
	lvl.Set(slog.LevelDebug)
	l := log.NewLogger(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: lvl,
	})))
	bdb := suite.bdb

	queues := []string{"q_1", "q_2", "q_3"}
	taskTypeMapping := map[string]string{
		"q_1": "task_1",
		"q_2": "task_2",
		"q_3": "task_3",
	}

	db, err := db.New(bdb, l, nil, utils.NewRealClock())
	require.NoError(t, err)
	p := NewProcessor(l, db, &ProcessorConfig{
		MaxConcurrency: 5,
		Queues:         queues,
	}, make(chan *taskInfoHeartBeat, 100), make(chan *taskInfoHeartBeat, 100))

	insertSomeData(t, db, queues, taskTypeMapping)

	d := dummy{l: l}
	d.count.Store(0)

	p.setupHandlers(map[string]Handler{
		"task_1": &d,
		"task_2": &d,
		"task_3": &d,
	})

	ctx := context.Background()
	go func() {
		p.start(ctx)
	}()

	time.Sleep(300 * time.Millisecond)
	l.Info("cancelling")
	p.stop()
	require.Equal(t, len(queues), int(d.count.Load()))
}

type dummy struct {
	l     *log.Logger
	count atomic.Int32
}

func (d *dummy) Process(ctx context.Context, req *generated.Task) error {
	d.count.Add(1)

	d.l.Info("hello", "payload", string(req.Payload), "count", d.count.Load())
	return nil
}

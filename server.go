package pizza

import (
	"context"
	"sync"
	"time"

	"github.com/abit2/pizza/db"
	"github.com/abit2/pizza/log"
	"github.com/abit2/pizza/utils"
)

type Server struct {
	logger    *log.Logger
	serverCfg *ServerConfig
	db        *db.DB
}

type ServerConfig struct {
	queues          []string
	concurrency     int
	promiseInterval time.Duration
}

func NewServer(l *log.Logger, db *db.DB, cfg *ServerConfig) *Server {
	return &Server{
		logger:    l,
		db:        db,
		serverCfg: cfg,
	}
}

func (s *Server) Run(ctx context.Context, wg *sync.WaitGroup) {
	processor := NewProcessor(s.logger, s.db, &ProcessorConfig{
		MaxConcurrency: s.serverCfg.concurrency,
		Queues:         s.serverCfg.queues,
	})

	promise := NewPromise(s.logger, s.serverCfg.promiseInterval, s.serverCfg.queues, s.db, utils.NewRealClock())

	wg.Go(func() {
		processor.start(ctx)
	})

	wg.Go(func() {
		promise.start(ctx)
	})
}

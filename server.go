package pizza

import (
	"context"
	"sync"
	"time"

	"github.com/abit2/pizza/db"
	"github.com/abit2/pizza/log"
)

type Server struct {
	logger    *log.Logger
	serverCfg *ServerConfig
	db        *db.DB
	cl        Clock

	// internals
	processor *Processor
}

type ServerConfig struct {
	Queues          []string
	Concurrency     int
	PromiseInterval time.Duration
}

func NewServer(l *log.Logger, db *db.DB, cfg *ServerConfig, cl Clock) *Server {
	return &Server{
		logger:    l,
		db:        db,
		serverCfg: cfg,
		cl:        cl,
	}
}

func (s *Server) Run(ctx context.Context, wg *sync.WaitGroup) {
	s.processor = NewProcessor(s.logger, s.db, &ProcessorConfig{
		MaxConcurrency: s.serverCfg.Concurrency,
		Queues:         s.serverCfg.Queues,
	})

	promise := NewPromise(s.logger, s.serverCfg.PromiseInterval, s.serverCfg.Queues, s.db, s.cl)

	s.logger.Debug("starting promise & processor jobs")
	wg.Go(func() {
		s.processor.start(ctx)
	})

	wg.Go(func() {
		promise.start(ctx)
	})
	s.logger.Debug("started promise & processor jobs")
}

func (s *Server) SetupHandler(handleMapping map[string]Handler) {
	s.processor.setupHandlers(handleMapping)
}

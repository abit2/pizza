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
	heartBeat *HeartBeat
	promise   *Promise

	claimedTasks  chan *taskInfoHeartBeat
	finishedTasks chan *taskInfoHeartBeat
	expiredTasks  chan *taskInfoHeartBeat
}

type ServerConfig struct {
	Queues          []string
	Concurrency     int
	PromiseInterval time.Duration

	GracefulShutdownTimeout time.Duration
}

func NewServer(l *log.Logger, db *db.DB, cfg *ServerConfig, cl Clock) *Server {
	if cfg != nil && cfg.GracefulShutdownTimeout <= 0 {
		cfg.GracefulShutdownTimeout = 2 * time.Second
	}
	return &Server{
		logger:        l,
		db:            db,
		serverCfg:     cfg,
		cl:            cl,
		claimedTasks:  make(chan *taskInfoHeartBeat, heartBeatChannelBufferSize),
		finishedTasks: make(chan *taskInfoHeartBeat, heartBeatChannelBufferSize),
		expiredTasks:  make(chan *taskInfoHeartBeat, heartBeatChannelBufferSize),
	}
}

func (s *Server) Run(ctx context.Context, wg *sync.WaitGroup) {
	s.processor = NewProcessor(s.logger, s.db, &ProcessorConfig{
		MaxConcurrency: s.serverCfg.Concurrency,
		Queues:         s.serverCfg.Queues,
	}, s.claimedTasks, s.finishedTasks, s.cl)

	s.promise = NewPromise(s.logger, s.serverCfg.PromiseInterval, s.serverCfg.Queues, s.db, s.cl)

	// Heartbeat is responsible for extending active task leases and emitting events
	// when leases have expired (to be recovered/requeued).
	s.heartBeat = NewHearBeater(
		s.logger,
		heartBeatInterval,
		s.claimedTasks,
		s.finishedTasks,
		s.expiredTasks,
		s.db,
		heartBeatExtendBeforeExpr,
	)

	s.logger.Debug("starting promise & processor jobs")
	wg.Go(func() {
		s.processor.start(ctx)
	})

	wg.Go(func() {
		s.promise.start(ctx)
	})

	wg.Go(func() {
		s.heartBeat.start(ctx)
	})
	s.logger.Debug("started promise, heartbeat & processor jobs")
}

func (s *Server) Stop() {
	s.processor.stop()
	s.promise.stop()

	s.wait()

	s.heartBeat.stop()
}

func (s *Server) wait() {
	time.Sleep(s.serverCfg.GracefulShutdownTimeout)
}

func (s *Server) SetupHandler(handleMapping map[string]Handler) {
	s.processor.setupHandlers(handleMapping)
}

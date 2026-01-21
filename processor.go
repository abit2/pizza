package pizza

import (
	"context"
	"encoding/json"
	"time"

	"github.com/abit2/pizza/log"
	"github.com/abit2/pizza/task/task/generated"
	"github.com/pkg/errors"

	"github.com/abit2/pizza/db"
)

// TODO: add a debounce process to prioritise the queues that have msgs over that doesn't
// TODO: add heartbeat/lease mechanism to recover orphaned tasks

type Handler interface {
	Process(ctx context.Context, req *generated.Task) error
}

type DB interface {
	Dequeue(queue []byte) (*generated.Task, error)
	MoveToRetryFromActive(queue []byte, taskID string) error
	MoveToArchivedFromActive(queue []byte, taskID string) error
	MoveToCompletedFromActive(queue []byte, taskID string) error
}

type Processor struct {
	logger *log.Logger
	db     DB

	sema chan struct{}
	cfg  *ProcessorConfig

	handler       map[string]Handler
	claimedTasks  chan *taskInfoHeartBeat
	finishedTasks chan *taskInfoHeartBeat
}

type ProcessorConfig struct {
	MaxConcurrency int
	Queues         []string
}

func NewProcessor(l *log.Logger, db DB, cfg *ProcessorConfig, claimedTasks chan *taskInfoHeartBeat, finishedTasks chan *taskInfoHeartBeat) *Processor {
	return &Processor{
		logger:        l,
		db:            db,
		sema:          make(chan struct{}, cfg.MaxConcurrency),
		cfg:           cfg,
		claimedTasks:  claimedTasks,
		finishedTasks: finishedTasks,
	}
}

func (p *Processor) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("processor: ctx done")
			return

		default:
			for _, queue := range p.cfg.Queues {
				select {
				case <-ctx.Done():
					p.logger.Info("processor: ctx done")
					return
				case p.sema <- struct{}{}:
					task, dequeueErr := p.db.Dequeue([]byte(queue))
					if dequeueErr != nil && !errors.Is(dequeueErr, db.ErrQueueEmpty) {
						p.logger.Error("dequeue err", "err", dequeueErr.Error())
					}

					if dequeueErr == nil {
						// Emit claimed task event
						p.claimedTasks <- &taskInfoHeartBeat{
							ID:        task.GetId(),
							QueueName: queue,
							LeaseTill: time.Now().Add(defaultLeaseDuration),
							StartTime: time.Now(),
						}

						err := p.handleExecResult(ctx, p.exec(ctx, task), task, queue)
						if err != nil {
							p.logger.Error("exec err", "err", err)
						} else {
							// Emit finished task event
							p.finishedTasks <- &taskInfoHeartBeat{
								ID:        task.GetId(),
								QueueName: queue,
								LeaseTill: time.Now(),
								StartTime: time.Now(),
							}
						}
					}

					<-p.sema
				default:
				}
			}
		}
	}
}

func (p *Processor) exec(ctx context.Context, task *generated.Task) error {
	if task == nil {
		return nil
	}

	headers := make(map[string]string)
	err := json.Unmarshal(task.GetHeaders(), &headers)
	if err != nil {
		return err
	}

	if p.handler == nil {
		return errors.New("handlers not setup")
	}

	err = p.handler[headers[TaskType]].Process(ctx, task)
	if err != nil {
		return err
	}
	return nil
}

func (p *Processor) setupHandlers(handlersMapping map[string]Handler) {
	p.handler = handlersMapping
}

func (p *Processor) handleExecResult(_ context.Context, err error, task *generated.Task, queue string) error {
	if err == nil {
		return errors.Wrap(p.db.MoveToCompletedFromActive([]byte(queue), task.GetId()), "failed to move task to completed")
	}

	if errors.Is(err, context.Canceled) {
		p.logger.Info("ctx canceled")
		return err
	}

	if errors.Is(err, db.ErrQueueEmpty) {
		p.logger.Info("queue empty")
		return nil
	}

	// move to archived if retry limit is reached
	// move to completed if err is nil
	// move to retry if retry limit is not reached
	retryCount := task.GetRetryCount()
	maxRetryCount := task.GetMaxRetries()

	if retryCount >= maxRetryCount {
		return errors.Wrap(p.db.MoveToArchivedFromActive([]byte(queue), task.GetId()), "failed to move task to archived")
	}
	return errors.Wrap(p.db.MoveToRetryFromActive([]byte(queue), task.GetId()), "failed to move task to retry")
}

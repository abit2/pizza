package pizza

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/abit2/pizza/log"
	"github.com/abit2/pizza/task/task/generated"

	"github.com/abit2/pizza/db"
)

type Handler interface {
	Process(ctx context.Context, req *generated.Task) error
}

type Processor struct {
	logger *log.Logger
	db     *db.DB

	sema chan struct{}
	cfg  *ProcessorConfig

	handler map[string]Handler
}

type ProcessorConfig struct {
	MaxConcurrency int
	Queues         []string
}

func NewProcessor(l *log.Logger, db *db.DB, cfg *ProcessorConfig) *Processor {
	return &Processor{
		logger: l,
		db:     db,
		sema:   make(chan struct{}, cfg.MaxConcurrency),
		cfg:    cfg,
	}
}

func (p *Processor) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("ctx done")
			return

		default:
			for _, queue := range p.cfg.Queues {
				select {
				case <-ctx.Done():
					p.logger.Info("ctx done")
					return
				case p.sema <- struct{}{}:
					err := p.exec(ctx, queue)
					if err != nil {
						// TODO: add a debounce process to prioritise the queues that have msgs over that doesn't
						if !errors.Is(err, db.ErrQueueEmpty) {
							p.logger.Error("exec err", err.Error())
						}
					}
					<-p.sema
				default:
				}
			}
		}
	}

}

func (p *Processor) exec(ctx context.Context, queue string) error {
	task, err := p.db.Dequeue([]byte(queue))
	if err != nil {
		return err
	}

	if task == nil {
		return nil
	}

	headers := make(map[string]string)
	err = json.Unmarshal(task.GetHeaders(), &headers)
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

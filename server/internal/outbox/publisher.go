package outbox

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/driver/postgres"
)

type Publisher struct {
	repo   *postgres.OutboxRepo
	driver core.QueueEventDriver
	done   chan struct{}
}

func NewPublisher(repo *postgres.OutboxRepo, driver core.QueueEventDriver) *Publisher {
	return &Publisher{
		repo:   repo,
		driver: driver,
		done:   make(chan struct{}),
	}
}

func (p *Publisher) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.done:
			return
		case <-ticker.C:
			p.publishBatch(ctx)
		}
	}
}

func (p *Publisher) Stop() {
	close(p.done)
}

func (p *Publisher) publishBatch(ctx context.Context) {
	entries, err := p.repo.FetchPending(ctx, 100)
	if err != nil {
		log.Printf("outbox fetch: %v", err)
		return
	}

	for _, e := range entries {
		if err := p.publishOne(ctx, e); err != nil {
			log.Printf("outbox publish %s: %v", e.ID, err)
			_ = p.repo.MarkFailed(ctx, e.ID)
			continue
		}
		_ = p.repo.MarkPublished(ctx, e.ID)
	}
}

func (p *Publisher) publishOne(ctx context.Context, e postgres.OutboxEntry) error {
	switch e.Kind {
	case "task_publish":
		var msg core.TaskMessage
		if err := json.Unmarshal(e.Payload, &msg); err != nil {
			return err
		}
		return p.driver.PublishTask(ctx, msg)
	case "event_publish":
		var event core.JanusEvent
		if err := json.Unmarshal(e.Payload, &event); err != nil {
			return err
		}
		return p.driver.PublishEvent(ctx, event)
	default:
		return nil
	}
}

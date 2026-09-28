package service

import (
	"github.com/agentium-lab/Janus/server/internal/metrics"
	"log"
	"sync"
	"time"

	"context"
	"fmt"

	"github.com/agentium-lab/Janus/core"
)

type MailboxService struct {
	mailboxRepo MailboxRepo
	queueDriver QueueDriver

	pendMu  sync.Mutex
	pending []pendingReconcile
}

func NewMailboxService(mailboxRepo MailboxRepo, queueDriver QueueDriver) *MailboxService {
	return &MailboxService{
		mailboxRepo: mailboxRepo,
		queueDriver: queueDriver,
	}
}

func (s *MailboxService) Create(ctx context.Context, mailbox core.Mailbox) error {
	if mailbox.TenantID == "" {
		return fmt.Errorf("tenant id is required")
	}
	if mailbox.ID == "" {
		return fmt.Errorf("mailbox id is required")
	}
	if mailbox.AgentID == "" {
		return fmt.Errorf("agent id is required")
	}
	if mailbox.MaxConcurrency <= 0 {
		mailbox.MaxConcurrency = 1
	}
	if mailbox.ACKWaitSeconds <= 0 {
		mailbox.ACKWaitSeconds = 300
	}
	if mailbox.MaxDeliver <= 0 {
		mailbox.MaxDeliver = 5
	}
	if mailbox.RetentionSeconds <= 0 {
		mailbox.RetentionSeconds = 604800
	}
	if mailbox.RetryPolicy.MaxAttempts == 0 {
		mailbox.RetryPolicy = core.DefaultRetryPolicy()
	}
	if mailbox.Status == "" {
		mailbox.Status = core.MailboxStatusActive
	}

	if err := s.mailboxRepo.Create(ctx, mailbox); err != nil {
		return fmt.Errorf("create mailbox: %w", err)
	}

	if err := s.queueDriver.EnsureTenant(ctx, mailbox.TenantID); err != nil {
		return fmt.Errorf("ensure tenant streams: %w", err)
	}

	if err := s.queueDriver.EnsureMailbox(ctx, core.MailboxSpec{
		TenantID:         mailbox.TenantID,
		MailboxID:        mailbox.ID,
		AgentID:          mailbox.AgentID,
		MaxConcurrency:   mailbox.MaxConcurrency,
		ACKWaitSeconds:   mailbox.ACKWaitSeconds,
		MaxDeliver:       mailbox.MaxDeliver,
		RetentionSeconds: mailbox.RetentionSeconds,
	}); err != nil {
		return fmt.Errorf("ensure queue mailbox: %w", err)
	}

	return s.queueDriver.EnsureConsumer(ctx, core.ConsumerSpec{
		TenantID:       mailbox.TenantID,
		MailboxID:      mailbox.ID,
		ACKWaitSeconds: mailbox.ACKWaitSeconds,
		MaxDeliver:     mailbox.MaxDeliver,
		MaxACKPending:  mailbox.MaxConcurrency * 2,
	})
}

func (s *MailboxService) Get(ctx context.Context, tenantID, mailboxID string) (*core.Mailbox, error) {
	if tenantID == "" || mailboxID == "" {
		return nil, fmt.Errorf("tenant id and mailbox id are required")
	}
	return s.mailboxRepo.Get(ctx, tenantID, mailboxID)
}

func (s *MailboxService) ListByAgent(ctx context.Context, tenantID, agentID string) ([]*core.Mailbox, error) {
	if tenantID == "" || agentID == "" {
		return nil, fmt.Errorf("tenant id and agent id are required")
	}
	return s.mailboxRepo.ListByAgent(ctx, tenantID, agentID)
}

func (s *MailboxService) Backlog(ctx context.Context, tenantID, mailboxID string) (int, error) {
	if tenantID == "" || mailboxID == "" {
		return 0, fmt.Errorf("tenant id and mailbox id are required")
	}
	return s.mailboxRepo.Backlog(ctx, tenantID, mailboxID)
}

func (s *MailboxService) Pause(ctx context.Context, tenantID, mailboxID string) error {
	if tenantID == "" || mailboxID == "" {
		return fmt.Errorf("tenant id and mailbox id are required")
	}
	return s.mailboxRepo.UpdateStatus(ctx, tenantID, mailboxID, core.MailboxStatusPaused)
}

func (s *MailboxService) Resume(ctx context.Context, tenantID, mailboxID string) error {
	if tenantID == "" || mailboxID == "" {
		return fmt.Errorf("tenant id and mailbox id are required")
	}
	return s.mailboxRepo.UpdateStatus(ctx, tenantID, mailboxID, core.MailboxStatusActive)
}

// consumerReconciler is implemented by queue drivers whose consumer config
// can drift from PG (NATS). PG remains the source of truth; reconcile is
// best-effort so drivers without reconcile support (pgqueue) stay valid.
type consumerReconciler interface {
	ReconcileConsumer(ctx context.Context, spec core.ConsumerSpec) error
}

// pendingReconcile is a config change committed to PG whose NATS consumer
// update failed; the retry loop keeps trying until the broker matches.
type pendingReconcile struct {
	spec core.ConsumerSpec
}

func (s *MailboxService) UpdateConfig(ctx context.Context, tenantID, mailboxID string, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds int) error {
	if tenantID == "" || mailboxID == "" {
		return fmt.Errorf("tenant id and mailbox id are required")
	}
	if err := s.mailboxRepo.UpdateConfig(ctx, tenantID, mailboxID, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds); err != nil {
		return err
	}
	spec := core.ConsumerSpec{
		TenantID:       tenantID,
		MailboxID:      mailboxID,
		DurableName:    mailboxID,
		ACKWaitSeconds: ackWaitSeconds,
		MaxDeliver:     maxDeliver,
	}
	if rc, ok := s.queueDriver.(consumerReconciler); ok {
		if err := rc.ReconcileConsumer(ctx, spec); err != nil {
			// The durable PG config is committed; park the spec for the
			// retry loop so the broker eventually converges instead of
			// silently running stale ack_wait/max_deliver forever.
			s.pendMu.Lock()
			s.pending = append(s.pending, pendingReconcile{spec: spec})
			s.pendMu.Unlock()
			log.Printf("mailbox %s/%s: consumer reconcile failed (pg committed, will retry): %v", tenantID, mailboxID, err)
		}
	}
	return nil
}

// StartReconcileRetryLoop drains pending consumer reconciles until the
// broker accepts them. JanusMetric janus_mailbox_reconcile_pending tracks
// the backlog so operators can alert on prolonged drift.
func (s *MailboxService) StartReconcileRetryLoop(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.retryPending(ctx)
			}
		}
	}()
}

func (s *MailboxService) retryPending(ctx context.Context) {
	s.pendMu.Lock()
	pending := s.pending
	s.pending = nil
	s.pendMu.Unlock()

	rc, ok := s.queueDriver.(consumerReconciler)
	if !ok {
		return
	}
	for _, p := range pending {
		if err := rc.ReconcileConsumer(ctx, p.spec); err != nil {
			s.pendMu.Lock()
			s.pending = append(s.pending, p)
			s.pendMu.Unlock()
		}
	}
	if n := len(s.pending); n > 0 {
		log.Printf("mailbox reconcile: %d consumer config(s) still pending broker sync", n)
	}
	metrics.MailboxReconcilePending.Set(float64(len(s.pending)))
}

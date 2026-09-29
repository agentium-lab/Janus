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
	pending map[string]core.ConsumerSpec
	// applyMu serializes every broker consumer write (config change AND
	// retry): a retry holding a stale spec and a concurrent UpdateConfig
	// used to race, letting the STALE spec land last and overwrite the
	// newer revision on the broker.
	applyMu sync.Mutex
}

func NewMailboxService(mailboxRepo MailboxRepo, queueDriver QueueDriver) *MailboxService {
	return &MailboxService{
		mailboxRepo: mailboxRepo,
		queueDriver: queueDriver,
		pending:     make(map[string]core.ConsumerSpec),
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

// reconcileKey dedupes pending specs: a NEWER config change for the same
// mailbox replaces the parked older one, so a stale retry can never
// overwrite a newer revision that already applied.
func reconcileKey(tenantID, mailboxID string) string {
	return tenantID + "/" + mailboxID
}

func (s *MailboxService) UpdateConfig(ctx context.Context, tenantID, mailboxID string, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds int) error {
	if tenantID == "" || mailboxID == "" {
		return fmt.Errorf("tenant id and mailbox id are required")
	}
	if err := s.mailboxRepo.UpdateConfig(ctx, tenantID, mailboxID, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds); err != nil {
		return err
	}
	// MaxAckPending must mirror the CREATE path (maxConcurrency*2); an
	// unset value makes CreateOrUpdateConsumer fall back to 100, silently
	// widening or narrowing the broker's in-flight window.
	ackPending := maxConcurrency * 2
	if ackPending <= 0 {
		ackPending = 100
	}
	spec := core.ConsumerSpec{
		TenantID:       tenantID,
		MailboxID:      mailboxID,
		DurableName:    mailboxID,
		ACKWaitSeconds: ackWaitSeconds,
		MaxDeliver:     maxDeliver,
		MaxACKPending:  ackPending,
	}
	if rc, ok := s.queueDriver.(consumerReconciler); ok {
		s.applyMu.Lock()
		err := rc.ReconcileConsumer(ctx, spec)
		if err == nil {
			s.pendMu.Lock()
			delete(s.pending, reconcileKey(tenantID, mailboxID))
			s.pendMu.Unlock()
		}
		s.applyMu.Unlock()
		if err != nil {
			// The durable PG config is committed; park the spec (keyed so a
			// newer revision replaces an older parked one) for the retry
			// loop — the broker converges instead of silently running stale
			// ack_wait/max_deliver forever.
			s.pendMu.Lock()
			s.pending[reconcileKey(tenantID, mailboxID)] = spec
			s.pendMu.Unlock()
			log.Printf("mailbox %s/%s: consumer reconcile failed (pg committed, will retry): %v", tenantID, mailboxID, err)
		}
	}
	return nil
}

// ReconcileAllConsumers re-applies every mailbox's durable PG config to
// the broker. Called at startup: the in-memory pending map does not
// survive restarts, so PG (the source of truth) is replayed wholesale.
func (s *MailboxService) ReconcileAllConsumers(ctx context.Context) {
	rc, ok := s.queueDriver.(consumerReconciler)
	if !ok {
		return
	}
	mailboxes, err := s.mailboxRepo.ListAll(ctx)
	if err != nil {
		log.Printf("mailbox reconcile-all: list: %v", err)
		return
	}
	for _, mb := range mailboxes {
		spec := consumerSpecFor(*mb)
		s.applyMu.Lock()
		err := rc.ReconcileConsumer(ctx, spec)
		s.applyMu.Unlock()
		if err != nil {
			s.pendMu.Lock()
			s.pending[reconcileKey(mb.TenantID, mb.ID)] = spec
			s.pendMu.Unlock()
		}
	}
	s.pendMu.Lock()
	n := len(s.pending)
	s.pendMu.Unlock()
	if n > 0 {
		log.Printf("mailbox reconcile-all: %d consumer config(s) parked for retry", n)
	}
}

func consumerSpecFor(mb core.Mailbox) core.ConsumerSpec {
	ackPending := mb.MaxConcurrency * 2
	if ackPending <= 0 {
		ackPending = 100
	}
	return core.ConsumerSpec{
		TenantID:       mb.TenantID,
		MailboxID:      mb.ID,
		DurableName:    mb.ID,
		ACKWaitSeconds: mb.ACKWaitSeconds,
		MaxDeliver:     mb.MaxDeliver,
		MaxACKPending:  ackPending,
	}
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
	rc, ok := s.queueDriver.(consumerReconciler)
	if !ok {
		return
	}
	// The read-apply-clear sequence runs entirely under applyMu: reading
	// the spec outside the lock let a concurrent UpdateConfig apply a
	// NEWER revision and clear the entry while this retry held the older
	// spec — the stale apply then landed last on the broker.
	for {
		s.applyMu.Lock()
		s.pendMu.Lock()
		var key string
		var spec core.ConsumerSpec
		for k, v := range s.pending {
			key, spec = k, v
			break
		}
		if key == "" {
			n := len(s.pending)
			s.pendMu.Unlock()
			s.applyMu.Unlock()
			if n > 0 {
				log.Printf("mailbox reconcile: %d consumer config(s) still pending broker sync", n)
			}
			metrics.MailboxReconcilePending.Set(float64(n))
			return
		}
		err := rc.ReconcileConsumer(ctx, spec)
		if err == nil {
			// Delete only if not replaced by an even newer revision.
			if cur, ok := s.pending[key]; ok && cur == spec {
				delete(s.pending, key)
			}
		}
		n := len(s.pending)
		s.pendMu.Unlock()
		s.applyMu.Unlock()
		if err != nil {
			s.pendMu.Lock()
			n = len(s.pending)
			s.pendMu.Unlock()
			log.Printf("mailbox reconcile: %d consumer config(s) still pending broker sync", n)
			metrics.MailboxReconcilePending.Set(float64(n))
			return
		}
	}
}

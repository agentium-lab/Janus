package service

import (
	"strings"

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
	pending map[string]parkedSpec
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
		pending:     make(map[string]parkedSpec),
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
	version, err := s.mailboxRepo.UpdateConfig(ctx, tenantID, mailboxID, maxConcurrency, ackWaitSeconds, maxDeliver, retentionSeconds)
	if err != nil {
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
		// Cross-instance interleave guard: re-read the durable version
		// right before the broker write. Another replica may have committed
		// a NEWER revision between our commit and this write — writing ours
		// now would leave the broker on the older config with nothing
		// pending (the newer replica's write already happened or will park
		// itself). Skip; the newer revision owns convergence.
		current, gerr := s.mailboxRepo.Get(ctx, tenantID, mailboxID)
		if gerr != nil {
			// Version unreadable: park for the retry loop (which also skips
			// while unreadable) instead of returning success with nothing
			// queued — that left the broker permanently unconverged.
			s.pendMu.Lock()
			s.pending[reconcileKey(tenantID, mailboxID)] = parkedSpec{version: version, spec: spec}
			s.pendMu.Unlock()
			s.applyMu.Unlock()
			log.Printf("mailbox %s/%s: consumer sync deferred for v%d (version unreadable, parked)", tenantID, mailboxID, version)
			return nil
		}
		if current == nil || current.ConfigVersion != version {
			s.applyMu.Unlock()
			log.Printf("mailbox %s/%s: skipping consumer sync for v%d (superseded)", tenantID, mailboxID, version)
			return nil
		}
		err := rc.ReconcileConsumer(ctx, spec)
		if err == nil {
			s.pendMu.Lock()
			delete(s.pending, reconcileKey(tenantID, mailboxID))
			s.pendMu.Unlock()
		} else {
			// The durable PG config is committed; park the spec WITH its PG
			// version for the retry loop — the broker converges instead of
			// silently running stale ack_wait/max_deliver forever, and the
			// version check refuses to resurrect superseded revisions.
			s.pendMu.Lock()
			s.pending[reconcileKey(tenantID, mailboxID)] = parkedSpec{version: version, spec: spec}
			s.pendMu.Unlock()
			log.Printf("mailbox %s/%s: consumer reconcile failed (pg committed v%d, will retry): %v", tenantID, mailboxID, version, err)
		}
		s.applyMu.Unlock()
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
		// Write-time version gate: the ListAll snapshot can be stale by the
		// time we write; re-read the durable version and skip if a newer
		// revision has landed (writing the snapshot would roll the broker
		// back until the next successful replay).
		current, gerr := s.mailboxRepo.Get(ctx, mb.TenantID, mb.ID)
		if gerr != nil || current == nil || current.ConfigVersion != mb.ConfigVersion {
			s.applyMu.Unlock()
			if gerr == nil && current != nil && current.ConfigVersion > mb.ConfigVersion {
				// Re-park with the CURRENT version so convergence continues.
				s.pendMu.Lock()
				s.pending[reconcileKey(mb.TenantID, mb.ID)] = parkedSpec{version: current.ConfigVersion, spec: consumerSpecFor(*current)}
				s.pendMu.Unlock()
			}
			continue
		}
		err := rc.ReconcileConsumer(ctx, spec)
		s.applyMu.Unlock()
		if err != nil {
			s.pendMu.Lock()
			s.pending[reconcileKey(mb.TenantID, mb.ID)] = parkedSpec{version: mb.ConfigVersion, spec: spec}
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

// parkedSpec couples a failed reconcile with the PG config_version it was
// read from: retries verify the version BEFORE applying so a superseded
// revision can never be written back to the broker (multi-instance safe —
// the version lives in PG, not in process memory).
type parkedSpec struct {
	version int
	spec    core.ConsumerSpec
}

// StartReconcileRetryLoop drains pending consumer reconciles until the
// broker accepts them, and periodically replays the durable PG configs
// wholesale (reconcileInterval) as a convergence backstop — any drift the
// version gates miss (e.g. a broker wipe) heals within one reconcile pass.
// janus_mailbox_reconcile_pending tracks the backlog so operators can
// alert on prolonged drift.
func (s *MailboxService) StartReconcileRetryLoop(ctx context.Context, interval, reconcileInterval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		reconcile := time.NewTicker(reconcileInterval)
		defer ticker.Stop()
		defer reconcile.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.retryPending(ctx)
			case <-reconcile.C:
				s.ReconcileAllConsumers(ctx)
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
		var parked parkedSpec
		for k, v := range s.pending {
			key, parked = k, v
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
		// Version gate: PG is the authority. If the durable version has
		// moved past what this parked spec was read from, the spec is
		// superseded — drop it instead of writing stale config back. If PG
		// cannot answer, SKIP: writing with an unknown version can put a
		// stale config on the broker (leave parked for the next tick).
		parts := strings.SplitN(key, "/", 2)
		current, gerr := s.mailboxRepo.Get(ctx, parts[0], parts[1])
		if gerr != nil {
			s.pendMu.Unlock()
			s.applyMu.Unlock()
			s.reportPendingCount()
			return
		}
		if current == nil || current.ConfigVersion > parked.version {
			delete(s.pending, key)
			s.pendMu.Unlock()
			s.applyMu.Unlock()
			continue
		}
		err := rc.ReconcileConsumer(ctx, parked.spec)
		if err == nil {
			if cur, ok := s.pending[key]; ok && cur == parked {
				delete(s.pending, key)
			}
		}
		s.pendMu.Unlock()
		s.applyMu.Unlock()
		if err != nil {
			s.reportPendingCount()
			return
		}
	}
}

// reportPendingCount logs and exports the pending-consumer backlog size
// under the pending lock (the gauge read must never race a parked/cleared
// entry).
func (s *MailboxService) reportPendingCount() {
	s.pendMu.Lock()
	n := len(s.pending)
	s.pendMu.Unlock()
	if n > 0 {
		log.Printf("mailbox reconcile: %d consumer config(s) still pending broker sync", n)
	}
	metrics.MailboxReconcilePending.Set(float64(n))
}

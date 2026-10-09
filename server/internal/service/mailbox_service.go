package service

import (
	"errors"

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
	// applyMu is the IN-PROCESS fallback for repos without cross-replica
	// advisory locks (unit tests). In production the per-mailbox PG
	// advisory lock is the serializer; this mutex never runs there.
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

// mailboxWriteLocker serializes per-mailbox config writes across replicas
// (pg advisory lock). Repos without it degrade to the in-process mutex.
type mailboxWriteLocker interface {
	AcquireMailboxLock(ctx context.Context, tenantID, mailboxID string) (release func(), ok bool, err error)
}

// ErrMailboxLockBusy reports a broker write skipped because another
// replica holds the per-mailbox cross-instance lock; callers park/defer
// instead of writing without mutual exclusion.
var ErrMailboxLockBusy = errors.New("mailbox write lock held by another replica")

// withMailboxLock runs fn under the strongest available per-mailbox lock:
// cross-replica advisory when the repo supports it, otherwise applyMu. A
// busy or failed lock returns without running fn — the write must not
// degrade to an unlocked write (that reintroduces the stale-write
// interleave the lock exists to prevent).
func (s *MailboxService) withMailboxLock(ctx context.Context, tenantID, mailboxID string, fn func()) error {
	if locker, ok := s.mailboxRepo.(mailboxWriteLocker); ok {
		release, locked, err := locker.AcquireMailboxLock(ctx, tenantID, mailboxID)
		if err != nil {
			// Lock infrastructure failure: do not write blind.
			return fmt.Errorf("mailbox lock: %w", err)
		}
		if !locked {
			return ErrMailboxLockBusy
		}
		defer release()
		fn()
		return nil
	}
	s.applyMu.Lock()
	defer s.applyMu.Unlock()
	fn()
	return nil
}

// tenantReconciler restores broker-level tenant resources (streams) that
// were lost at runtime; implemented by the NATS driver. ReconcileMailboxDLQ
// heals per-mailbox DLQ streams the same way.
type tenantReconciler interface {
	ReconcileTenant(ctx context.Context, tenantID string) error
	ReconcileMailboxDLQ(ctx context.Context, tenantID, mailboxID string) error
}

// ReconcileAllTenants verifies/recreates every tenant's broker streams.
// Called by the periodic loop next to the consumer reconcile: the
// in-process tenant cache meant a wiped JetStream never healed.
func (s *MailboxService) ReconcileAllTenants(ctx context.Context) {
	tr, ok := s.queueDriver.(tenantReconciler)
	if !ok {
		return
	}
	mailboxes, err := s.mailboxRepo.ListAll(ctx)
	if err != nil {
		log.Printf("tenant reconcile: list: %v", err)
		return
	}
	// ONE listing, tenant streams reconciled ONCE per distinct tenant, then
	// per-mailbox DLQ checks — the previous shape did ~5N broker queries
	// under the driver's global lock (a second listing, per-mailbox tenant
	// stream checks, and a doubled healthy-DLQ probe), delaying pulls on
	// large tenants during every reconcile pass.
	tenants := make(map[string]struct{}, len(mailboxes))
	for _, mb := range mailboxes {
		tenants[mb.TenantID] = struct{}{}
	}
	for tenantID := range tenants {
		if err := tr.ReconcileTenant(ctx, tenantID); err != nil {
			log.Printf("tenant reconcile: %s: %v", tenantID, err)
		}
	}
	for _, mb := range mailboxes {
		if err := tr.ReconcileMailboxDLQ(ctx, mb.TenantID, mb.ID); err != nil {
			log.Printf("mailbox dlq reconcile: %s/%s: %v", mb.TenantID, mb.ID, err)
		}
	}
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
		syncErr := s.withMailboxLock(ctx, tenantID, mailboxID, func() {
			// Cross-instance interleave guard, now serialized per mailbox
			// across replicas: re-read the durable version right before the
			// broker write. A NEWER revision committed elsewhere between our
			// commit and this write makes ours superseded — skip; the newer
			// revision owns convergence.
			current, gerr := s.mailboxRepo.Get(ctx, tenantID, mailboxID)
			if gerr != nil {
				// Version unreadable: park for the retry loop (which also
				// skips while unreadable) instead of returning success with
				// nothing queued.
				s.pendMu.Lock()
				s.pending[reconcileKey(tenantID, mailboxID)] = parkedSpec{version: version, spec: spec}
				s.pendMu.Unlock()
				log.Printf("mailbox %s/%s: consumer sync deferred for v%d (version unreadable, parked)", tenantID, mailboxID, version)
				return
			}
			if current == nil || current.ConfigVersion != version {
				log.Printf("mailbox %s/%s: skipping consumer sync for v%d (superseded)", tenantID, mailboxID, version)
				return
			}
			if err := rc.ReconcileConsumer(ctx, spec); err != nil {
				s.pendMu.Lock()
				s.pending[reconcileKey(tenantID, mailboxID)] = parkedSpec{version: version, spec: spec}
				s.pendMu.Unlock()
				log.Printf("mailbox %s/%s: consumer reconcile failed (pg committed v%d, will retry): %v", tenantID, mailboxID, version, err)
			} else {
				s.pendMu.Lock()
				delete(s.pending, reconcileKey(tenantID, mailboxID))
				s.pendMu.Unlock()
			}
		})
		if syncErr != nil {
			// Lock busy (another replica is mid-write for this mailbox) or
			// lock infrastructure failed: park for the retry loop instead
			// of writing without mutual exclusion.
			s.pendMu.Lock()
			s.pending[reconcileKey(tenantID, mailboxID)] = parkedSpec{version: version, spec: spec}
			s.pendMu.Unlock()
			log.Printf("mailbox %s/%s: consumer sync deferred (lock busy/error): %v", tenantID, mailboxID, syncErr)
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
		lerr := s.withMailboxLock(ctx, mb.TenantID, mb.ID, func() {
			// Write-time version gate (cross-replica serialized): the
			// ListAll snapshot can be stale by write time; a newer durable
			// revision skips the write and re-parks the CURRENT version so
			// convergence continues.
			current, gerr := s.mailboxRepo.Get(ctx, mb.TenantID, mb.ID)
			if gerr != nil || current == nil || current.ConfigVersion != mb.ConfigVersion {
				if gerr == nil && current != nil && current.ConfigVersion > mb.ConfigVersion {
					s.pendMu.Lock()
					s.pending[reconcileKey(mb.TenantID, mb.ID)] = parkedSpec{version: current.ConfigVersion, spec: consumerSpecFor(*current)}
					s.pendMu.Unlock()
				}
				return
			}
			if err := rc.ReconcileConsumer(ctx, spec); err != nil {
				s.pendMu.Lock()
				s.pending[reconcileKey(mb.TenantID, mb.ID)] = parkedSpec{version: mb.ConfigVersion, spec: spec}
				s.pendMu.Unlock()
			}
		})
		if lerr != nil {
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
				s.ReconcileAllTenants(ctx)
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
	for {
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
			if n > 0 {
				log.Printf("mailbox reconcile: %d consumer config(s) still pending broker sync", n)
			}
			metrics.MailboxReconcilePending.Set(float64(n))
			return
		}
		s.pendMu.Unlock()

		parts := strings.SplitN(key, "/", 2)
		// The version check and broker write run as ONE cross-replica
		// serialized unit per mailbox: the advisory (or local) lock closes
		// the read-to-write window in which a concurrent replica could
		// commit a newer revision that this stale apply would overwrite.
		var applyErr error
		var skip bool
		lockErr := s.withMailboxLock(ctx, parts[0], parts[1], func() {
			s.pendMu.Lock()
			cur, still := s.pending[key]
			s.pendMu.Unlock()
			if !still || cur != parked {
				skip = true // superseded or drained mid-flight
				return
			}
			// Version gate: PG is the authority. A parked spec older than
			// the durable version is superseded — drop it. If PG cannot
			// answer, SKIP: writing with an unknown version can put a stale
			// config on the broker (leave parked for the next tick).
			current, gerr := s.mailboxRepo.Get(ctx, parts[0], parts[1])
			if gerr != nil {
				applyErr = gerr
				return
			}
			if current == nil || current.ConfigVersion > parked.version {
				s.pendMu.Lock()
				delete(s.pending, key)
				s.pendMu.Unlock()
				skip = true
				return
			}
			applyErr = rc.ReconcileConsumer(ctx, parked.spec)
			if applyErr == nil {
				s.pendMu.Lock()
				if cur, ok := s.pending[key]; ok && cur == parked {
					delete(s.pending, key)
				}
				s.pendMu.Unlock()
			}
		})
		if lockErr != nil {
			// Lock busy or failed: the spec stays parked; try again next tick.
			s.reportPendingCount()
			return
		}
		if skip {
			continue
		}
		if applyErr != nil {
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

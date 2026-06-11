package service

import (
	"context"
	"fmt"

	"github.com/agentium-lab/Janus/core"
)

type MailboxService struct {
	mailboxRepo MailboxRepo
	queueDriver QueueDriver
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

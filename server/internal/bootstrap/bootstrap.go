package bootstrap

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/agentium-lab/Janus/core"
)

type TenantLister interface {
	ListIDs(ctx context.Context) ([]string, error)
}

type QueueEnsurer interface {
	EnsureTenant(ctx context.Context, tenantID string) error
}

type MailboxLister interface {
	ListByTenant(ctx context.Context, tenantID string) ([]*core.Mailbox, error)
}

type MailboxEnsurer interface {
	EnsureMailbox(ctx context.Context, spec core.MailboxSpec) error
	EnsureConsumer(ctx context.Context, spec core.ConsumerSpec) error
}

type MailboxRepo interface {
	ListByTenant(ctx context.Context, tenantID string) ([]*core.Mailbox, error)
}

type Options struct {
	Pool            *pgxpool.Pool
	TenantLister    TenantLister
	QueueEnsurer    QueueEnsurer
	MailboxEnsurer  MailboxEnsurer
}

type Result struct {
	TenantsEnsured int
	MailboxesSeen  int
	Errors         []error
}

func Run(ctx context.Context, opts Options) (*Result, error) {
	if opts.TenantLister == nil {
		return nil, fmt.Errorf("tenant lister is required")
	}
	if opts.QueueEnsurer == nil {
		return nil, fmt.Errorf("queue ensurer is required")
	}

	tenantIDs, err := opts.TenantLister.ListIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}

	result := &Result{}

	for _, tenantID := range tenantIDs {
		if err := opts.QueueEnsurer.EnsureTenant(ctx, tenantID); err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("ensure tenant %s: %w", tenantID, err))
			continue
		}
		result.TenantsEnsured++
	}

	if len(result.Errors) > 0 {
		log.Printf("bootstrap: ensured %d tenants, %d errors", result.TenantsEnsured, len(result.Errors))
	} else {
		log.Printf("bootstrap: ensured %d tenants", result.TenantsEnsured)
	}

	return result, nil
}

func EnsureMailboxConsumer(ctx context.Context, ensurer MailboxEnsurer, mb *core.Mailbox) error {
	if ensurer == nil || mb == nil {
		return nil
	}

	spec := core.MailboxSpec{
		TenantID:         mb.TenantID,
		MailboxID:        mb.ID,
		AgentID:          mb.AgentID,
		MaxConcurrency:   mb.MaxConcurrency,
		ACKWaitSeconds:   mb.ACKWaitSeconds,
		MaxDeliver:       mb.MaxDeliver,
		RetentionSeconds: mb.RetentionSeconds,
	}
	if err := ensurer.EnsureMailbox(ctx, spec); err != nil {
		return fmt.Errorf("ensure mailbox %s: %w", mb.ID, err)
	}

	consumerSpec := core.ConsumerSpec{
		TenantID:       mb.TenantID,
		MailboxID:      mb.ID,
		DurableName:    mb.ID,
		ACKWaitSeconds: mb.ACKWaitSeconds,
		MaxDeliver:     mb.MaxDeliver,
	}
	if err := ensurer.EnsureConsumer(ctx, consumerSpec); err != nil {
		return fmt.Errorf("ensure consumer %s: %w", mb.ID, err)
	}

	return nil
}

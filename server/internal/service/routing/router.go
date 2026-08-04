package routing

import (
	"context"
	"fmt"
	"strings"

	"github.com/agentium-lab/Janus/core"
	"github.com/agentium-lab/Janus/server/internal/metrics"
)

type AgentCandidate struct {
	AgentID        string
	MailboxID      string
	Backlog        int
	RunningCount   int
	MaxConcurrency int
	Capabilities   []core.AgentCapability
	Status         string
	DataClass      string
	ModelClasses   []string
}

type FilteredCandidate struct {
	AgentID string
	Reason  string
}

type RouterResult struct {
	TargetType  core.TargetType
	MailboxID   string
	AgentID     string
	Reason      string
	Score       int
	FilteredOut []FilteredCandidate
}

type AgentLookup interface {
	ListOnlineByCapability(ctx context.Context, tenantID, capability string) ([]AgentCandidate, error)
	GetAgentMailbox(ctx context.Context, tenantID, agentID string) (mailboxID string, err error)
	ValidateMailbox(ctx context.Context, tenantID, mailboxID string) (active bool, err error)
	GetGroupMailboxes(ctx context.Context, tenantID, groupID string) ([]string, error)
	GetHumanMailboxes(ctx context.Context, tenantID, humanID string) ([]string, error)
}

type PolicyChecker interface {
	CheckRoute(ctx context.Context, tenantID, agentID, dataClass string) (allowed bool, err error)
}

type BudgetChecker interface {
	CheckCapacity(ctx context.Context, tenantID, agentID string, running, maxConcurrency int) (ok bool, err error)
}

type Router struct {
	lookup   AgentLookup
	policy   PolicyChecker
	budget   BudgetChecker
}

func NewRouter(lookup AgentLookup, policy PolicyChecker, budget BudgetChecker) *Router {
	return &Router{lookup: lookup, policy: policy, budget: budget}
}

func (r *Router) Route(ctx context.Context, tenantID string, target core.Target, envelope core.TaskEnvelope) (result *RouterResult, err error) {
	defer func() {
		metrics.RoutingDecisions.WithLabelValues(classifyRouteOutcome(result, err)).Inc()
	}()
	switch target.Type {
	case core.TargetTypeAgent:
		return r.routeAgent(ctx, tenantID, target.Value)
	case core.TargetTypeMailbox:
		return r.routeMailbox(ctx, tenantID, target.Value)
	case core.TargetTypeCapability:
		return r.routeCapability(ctx, tenantID, target.Value, envelope)
	case core.TargetTypeGroup:
		return r.routeGroup(ctx, tenantID, target.Value)
	case core.TargetTypeHuman:
		return r.routeHuman(ctx, tenantID, target.Value)
	default:
		return nil, fmt.Errorf("unsupported target type: %s", target.Type)
	}
}

func classifyRouteOutcome(result *RouterResult, err error) string {
	if err == nil {
		return "success"
	}
	if result != nil && result.Reason == "all_candidates_filtered" {
		return "all_filtered"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "no online agents"):
		return "no_candidates"
	case strings.Contains(msg, "not found"),
		strings.Contains(msg, "no active mailbox"),
		strings.Contains(msg, "no group mailbox"),
		strings.Contains(msg, "no human mailbox"):
		return "not_found"
	}
	return "error"
}

func (r *Router) routeAgent(ctx context.Context, tenantID, agentID string) (*RouterResult, error) {
	mailboxID, err := r.lookup.GetAgentMailbox(ctx, tenantID, agentID)
	if err != nil {
		return nil, fmt.Errorf("agent %s not found: %w", agentID, err)
	}
	return &RouterResult{
		TargetType: core.TargetTypeAgent,
		MailboxID:  mailboxID,
		AgentID:    agentID,
		Reason:     "direct agent target",
	}, nil
}

func (r *Router) routeMailbox(ctx context.Context, tenantID, mailboxID string) (*RouterResult, error) {
	active, err := r.lookup.ValidateMailbox(ctx, tenantID, mailboxID)
	if err != nil {
		return nil, fmt.Errorf("mailbox %s validation failed: %w", mailboxID, err)
	}
	if !active {
		return nil, fmt.Errorf("mailbox %s is not active", mailboxID)
	}
	return &RouterResult{
		TargetType: core.TargetTypeMailbox,
		MailboxID:  mailboxID,
		Reason:     "direct mailbox target",
	}, nil
}

func (r *Router) routeCapability(ctx context.Context, tenantID, capability string, envelope core.TaskEnvelope) (*RouterResult, error) {
	candidates, err := r.lookup.ListOnlineByCapability(ctx, tenantID, capability)
	if err != nil {
		return nil, fmt.Errorf("list capability candidates: %w", err)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no online agents with capability %s", capability)
	}

	dataClass := ""
	if envelope.Policy != nil {
		dataClass = envelope.Policy.DataClassification
	}

	var filtered []FilteredCandidate
	var passed []AgentCandidate

	for _, c := range candidates {
		if c.Status != "online" && c.Status != "active" {
			filtered = append(filtered, FilteredCandidate{AgentID: c.AgentID, Reason: "offline"})
			continue
		}
		if dataClass != "" && c.DataClass != "" && c.DataClass != dataClass {
			filtered = append(filtered, FilteredCandidate{AgentID: c.AgentID, Reason: "data_classification_mismatch"})
			continue
		}
		if r.policy != nil {
			allowed, err := r.policy.CheckRoute(ctx, tenantID, c.AgentID, dataClass)
			if err == nil && !allowed {
				filtered = append(filtered, FilteredCandidate{AgentID: c.AgentID, Reason: "policy_denied"})
				continue
			}
		}
		if c.MaxConcurrency > 0 && c.RunningCount >= c.MaxConcurrency {
			filtered = append(filtered, FilteredCandidate{AgentID: c.AgentID, Reason: "capacity_exceeded"})
			continue
		}
		if r.budget != nil {
			ok, err := r.budget.CheckCapacity(ctx, tenantID, c.AgentID, c.RunningCount, c.MaxConcurrency)
			if err == nil && !ok {
				filtered = append(filtered, FilteredCandidate{AgentID: c.AgentID, Reason: "budget_exceeded"})
				continue
			}
		}
		if envelope.Budget != nil && len(envelope.Budget.ModelClasses) > 0 && len(c.ModelClasses) > 0 {
			if !hasIntersection(envelope.Budget.ModelClasses, c.ModelClasses) {
				filtered = append(filtered, FilteredCandidate{AgentID: c.AgentID, Reason: "model_class_mismatch"})
				continue
			}
		}
		passed = append(passed, c)
	}

	if len(passed) == 0 {
		return &RouterResult{
			TargetType:  core.TargetTypeCapability,
			FilteredOut: filtered,
			Reason:      "all_candidates_filtered",
		}, fmt.Errorf("all candidates filtered for capability %s", capability)
	}

	best := passed[0]
	bestScore := 0
	for _, c := range passed {
		score := scoreCandidate(c, capability, envelope)
		if score > bestScore || (score == bestScore && c.Backlog < best.Backlog) {
			best = c
			bestScore = score
		}
	}

	reason := "capability_semantic"
	if bestScore == 0 {
		reason = "capability_filtered"
	}

	return &RouterResult{
		TargetType:  core.TargetTypeCapability,
		MailboxID:   best.MailboxID,
		AgentID:     best.AgentID,
		Reason:      reason,
		Score:       bestScore,
		FilteredOut: filtered,
	}, nil
}

func (r *Router) routeGroup(ctx context.Context, tenantID, groupID string) (*RouterResult, error) {
	mailboxes, err := r.lookup.GetGroupMailboxes(ctx, tenantID, groupID)
	if err != nil || len(mailboxes) == 0 {
		return nil, fmt.Errorf("no group mailbox mapping for %s", groupID)
	}
	for _, mb := range mailboxes {
		active, _ := r.lookup.ValidateMailbox(ctx, tenantID, mb)
		if active {
			return &RouterResult{
				TargetType: core.TargetTypeGroup,
				MailboxID:  mb,
				Reason:     "group_mailbox:" + groupID,
			}, nil
		}
	}
	return nil, fmt.Errorf("no active mailbox in group %s", groupID)
}

func (r *Router) routeHuman(ctx context.Context, tenantID, humanID string) (*RouterResult, error) {
	return nil, fmt.Errorf("human routing is not supported")
}

func hasIntersection(a, b []string) bool {
	set := make(map[string]bool, len(a))
	for _, s := range a {
		set[s] = true
	}
	for _, s := range b {
		if set[s] {
			return true
		}
	}
	return false
}

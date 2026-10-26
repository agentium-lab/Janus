package routing

import (
	"context"
	"fmt"
	"log"
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
	TeamID         string
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
	GetAgent(ctx context.Context, tenantID, agentID string) (*AgentCandidate, error)
	ListOnlineByCapability(ctx context.Context, tenantID, capability string) ([]AgentCandidate, error)
	GetAgentMailbox(ctx context.Context, tenantID, agentID string) (mailboxID string, err error)
	ValidateMailbox(ctx context.Context, tenantID, mailboxID string) (active bool, err error)
	GetMailboxOwner(ctx context.Context, tenantID, mailboxID string) (*AgentCandidate, error)
	GetGroupMailboxes(ctx context.Context, tenantID, groupID string) ([]string, error)
	GetHumanMailboxes(ctx context.Context, tenantID, humanID string) ([]string, error)
}

type PolicyChecker interface {
	CheckRoute(ctx context.Context, tenantID, agentID, dataClass, teamID string) (allowed bool, err error)
}

type BudgetChecker interface {
	CheckCapacity(ctx context.Context, tenantID, agentID string, running, maxConcurrency int) (ok bool, err error)
}

type Router struct {
	lookup AgentLookup
	policy PolicyChecker
	budget BudgetChecker
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
		return r.routeAgent(ctx, tenantID, target.Value, envelope)
	case core.TargetTypeMailbox:
		return r.routeMailbox(ctx, tenantID, target.Value, envelope)
	case core.TargetTypeCapability:
		return r.routeCapability(ctx, tenantID, target.Value, envelope)
	case core.TargetTypeGroup:
		return r.routeGroup(ctx, tenantID, target.Value, envelope)
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

func (r *Router) routeAgent(ctx context.Context, tenantID, agentID string, envelope core.TaskEnvelope) (*RouterResult, error) {
	agent, err := r.lookup.GetAgent(ctx, tenantID, agentID)
	if err != nil {
		return nil, fmt.Errorf("agent %s not found: %w", agentID, err)
	}
	// Direct agent target must pass the same policy gate as capability routing.
	// A denied agent should not be routable just because the caller specified
	// it by ID rather than by capability.
	dataClass := ""
	if envelope.Policy != nil {
		dataClass = envelope.Policy.DataClassification
	}
	// TargetTeamID is a SELECTION constraint, not a policy fact override.
	// If the task specifies a target team and the agent's actual team doesn't
	// match, reject — the caller cannot flip policy by claiming a different team.
	if envelope.Policy != nil && envelope.Policy.TargetTeamID != "" && envelope.Policy.TargetTeamID != agent.TeamID {
		return nil, fmt.Errorf("agent %s team %q does not match requested target team %q", agentID, agent.TeamID, envelope.Policy.TargetTeamID)
	}
	if r.policy != nil {
		// Policy fact is ALWAYS the agent's actual team, never the caller's claim.
		allowed, perr := r.policy.CheckRoute(ctx, tenantID, agentID, dataClass, agent.TeamID)
		if perr != nil {
			return nil, fmt.Errorf("policy check for agent %s: %w", agentID, perr)
		}
		if !allowed {
			return nil, fmt.Errorf("agent %s denied by policy", agentID)
		}
	}
	mailboxID, err := r.lookup.GetAgentMailbox(ctx, tenantID, agentID)
	if err != nil {
		return nil, fmt.Errorf("agent %s mailbox not found: %w", agentID, err)
	}
	return &RouterResult{
		TargetType: core.TargetTypeAgent,
		MailboxID:  mailboxID,
		AgentID:    agentID,
		Reason:     "direct agent target",
	}, nil
}

func (r *Router) routeMailbox(ctx context.Context, tenantID, mailboxID string, envelope core.TaskEnvelope) (*RouterResult, error) {
	active, err := r.lookup.ValidateMailbox(ctx, tenantID, mailboxID)
	if err != nil {
		return nil, fmt.Errorf("mailbox %s validation failed: %w", mailboxID, err)
	}
	if !active {
		return nil, fmt.Errorf("mailbox %s is not active", mailboxID)
	}
	// Same final admission gate as agent/capability routing: a direct
	// mailbox target must not bypass policy just because the caller named
	// the mailbox instead of the agent. r.policy == nil keeps the dev
	// permissive active-check-only path.
	agentID := ""
	if r.policy != nil {
		owner, _, aerr := r.admitMailbox(ctx, tenantID, mailboxID, envelope)
		if aerr != nil {
			return nil, aerr
		}
		if owner != nil {
			agentID = owner.AgentID
		}
	}
	return &RouterResult{
		TargetType: core.TargetTypeMailbox,
		MailboxID:  mailboxID,
		AgentID:    agentID,
		Reason:     "direct mailbox target",
	}, nil
}

// admitMailbox resolves a mailbox's owning agent and runs the unified
// admission gate: TargetTeamID selection constraint plus policy CheckRoute
// with the OWNER's team as the policy fact, fail-closed on checker errors.
// A mailbox with no resolvable owner cannot be governed and is rejected.
// The returned reason is the FilteredCandidate reason on failure.
func (r *Router) admitMailbox(ctx context.Context, tenantID, mailboxID string, envelope core.TaskEnvelope) (*AgentCandidate, string, error) {
	owner, err := r.lookup.GetMailboxOwner(ctx, tenantID, mailboxID)
	if err != nil {
		return nil, "owner_lookup_error", fmt.Errorf("mailbox %s owner lookup: %w", mailboxID, err)
	}
	if owner == nil {
		return nil, "ownerless", fmt.Errorf("mailbox %s has no resolvable owner agent, cannot govern routing", mailboxID)
	}
	dataClass := ""
	targetTeam := ""
	if envelope.Policy != nil {
		dataClass = envelope.Policy.DataClassification
		targetTeam = envelope.Policy.TargetTeamID
	}
	// TargetTeamID is a SELECTION constraint, never a policy fact override.
	if targetTeam != "" && targetTeam != owner.TeamID {
		return nil, "target_team_mismatch", fmt.Errorf("mailbox %s owner agent %s team %q does not match requested target team %q", mailboxID, owner.AgentID, owner.TeamID, targetTeam)
	}
	if r.policy != nil {
		allowed, perr := r.policy.CheckRoute(ctx, tenantID, owner.AgentID, dataClass, owner.TeamID)
		if perr != nil {
			return nil, "policy_check_error", fmt.Errorf("policy check for mailbox %s owner agent %s: %w", mailboxID, owner.AgentID, perr)
		}
		if !allowed {
			return nil, "policy_denied", fmt.Errorf("mailbox %s owner agent %s denied by policy", mailboxID, owner.AgentID)
		}
	}
	return owner, "", nil
}

// CheckMailboxAdmission re-runs the mailbox admission gate for republish
// paths (Replay, DLQ replay): owner resolution + TargetTeamID + policy.
// A nil policy checker keeps the dev-permissive path (no governance).
func (r *Router) CheckMailboxAdmission(ctx context.Context, tenantID, mailboxID string, envelope core.TaskEnvelope) error {
	if r.policy == nil {
		return nil
	}
	_, _, err := r.admitMailbox(ctx, tenantID, mailboxID, envelope)
	return err
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
	targetTeam := "" // task-specified target team (selection constraint only)
	if envelope.Policy != nil {
		dataClass = envelope.Policy.DataClassification
		targetTeam = envelope.Policy.TargetTeamID
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
		// TargetTeamID is a SELECTION constraint: filter candidates whose
		// actual team doesn't match. Never override the policy fact.
		if targetTeam != "" && c.TeamID != targetTeam {
			filtered = append(filtered, FilteredCandidate{AgentID: c.AgentID, Reason: "target_team_mismatch"})
			continue
		}
		if r.policy != nil {
			// Policy fact is ALWAYS the candidate's actual team.
			allowed, err := r.policy.CheckRoute(ctx, tenantID, c.AgentID, dataClass, c.TeamID)
			if err != nil {
				// Fail-closed by doctrine: policy check errors must reject
				// the candidate, not silently allow it. A broken policy
				// store is not a reason to bypass governance.
				filtered = append(filtered, FilteredCandidate{AgentID: c.AgentID, Reason: "policy_check_error"})
				log.Printf("routing: policy check for candidate agent %s failed, rejecting candidate (fail-closed): %v", c.AgentID, err)
				continue
			} else if !allowed {
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
		if envelope.Budget != nil && len(envelope.Budget.ModelClasses) > 0 {
			if len(c.ModelClasses) == 0 {
				// Task requires model classes but candidate has none - hard constraint violation
				filtered = append(filtered, FilteredCandidate{AgentID: c.AgentID, Reason: "model_class_required"})
				continue
			}
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

func (r *Router) routeGroup(ctx context.Context, tenantID, groupID string, envelope core.TaskEnvelope) (*RouterResult, error) {
	mailboxes, err := r.lookup.GetGroupMailboxes(ctx, tenantID, groupID)
	if err != nil || len(mailboxes) == 0 {
		return nil, fmt.Errorf("no group mailbox mapping for %s", groupID)
	}
	var filtered []FilteredCandidate
	for _, mb := range mailboxes {
		active, _ := r.lookup.ValidateMailbox(ctx, tenantID, mb)
		if !active {
			filtered = append(filtered, FilteredCandidate{AgentID: mb, Reason: "inactive"})
			continue
		}
		if r.policy == nil {
			return &RouterResult{
				TargetType: core.TargetTypeGroup,
				MailboxID:  mb,
				Reason:     "group_mailbox:" + groupID,
			}, nil
		}
		// Same admission gate per mailbox as direct mailbox routing: the
		// first admissible mailbox wins, denials collect reasons.
		owner, reason, aerr := r.admitMailbox(ctx, tenantID, mb, envelope)
		if aerr != nil {
			filtered = append(filtered, FilteredCandidate{AgentID: mb, Reason: reason})
			continue
		}
		return &RouterResult{
			TargetType: core.TargetTypeGroup,
			MailboxID:  mb,
			AgentID:    owner.AgentID,
			Reason:     "group_mailbox:" + groupID,
		}, nil
	}
	if r.policy != nil && len(filtered) > 0 {
		return &RouterResult{
				TargetType:  core.TargetTypeGroup,
				FilteredOut: filtered,
				Reason:      "all_candidates_filtered",
		}, fmt.Errorf("all candidates filtered for group %s", groupID)
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

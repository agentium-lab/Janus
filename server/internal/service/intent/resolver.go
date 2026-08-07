package intent

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/agentium-lab/Janus/core"
)

type IntentResolver struct {
	lookup AgentLookup
	llm    LLMCompleter
}

type AgentLookup interface {
	ListOnlineAgents(ctx context.Context, tenantID string) ([]core.Agent, error)
}

type LLMCompleter interface {
	Complete(ctx context.Context, systemPrompt, userPrompt string) (string, error)
}

type ResolveResult struct {
	ResolvedCapability string
	Confidence         float64
	Reason             string
	Candidates         []CandidateSummary
}

type CandidateSummary struct {
	AgentID    string
	Capability string
	Score      float64
}

func NewResolver(lookup AgentLookup) *IntentResolver {
	return &IntentResolver{lookup: lookup}
}

func (r *IntentResolver) WithLLM(client LLMCompleter) *IntentResolver {
	r.llm = client
	return r
}

func (r *IntentResolver) Resolve(ctx context.Context, tenantID, intentValue string, payload core.Payload, contextRefs []core.ContextRef, policyHints []string) (*ResolveResult, error) {
	agents, err := r.lookup.ListOnlineAgents(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list online agents: %w", err)
	}

	if r.llm != nil {
		catalogSet := make(map[string]bool)
		var catalogLines []string
		for _, agent := range agents {
			for _, cap := range agent.Capabilities {
				if !catalogSet[cap.Capability] {
					catalogSet[cap.Capability] = true
					line := "- " + cap.Capability
					if cap.Description != "" {
						line += ": " + cap.Description
					}
					catalogLines = append(catalogLines, line)
				}
			}
		}
		if len(catalogSet) > 0 {
			catalog := strings.Join(catalogLines, "\n")
			systemPrompt := "You are a task router. Select the best matching capability for the user's request from the catalog below. Respond with ONLY the capability name, or NONE if nothing matches."
			userPrompt := "User request: " + intentValue + "\n\nAvailable capabilities:\n" + catalog
			if resp, err := r.llm.Complete(ctx, systemPrompt, userPrompt); err == nil {
				matched := strings.TrimSpace(resp)
				if idx := strings.Index(matched, "\n"); idx > 0 {
					matched = matched[:idx]
				}
				matched = strings.TrimSpace(strings.Trim(matched, "`\"'* "))
				if catalogSet[matched] {
					return &ResolveResult{
						ResolvedCapability: matched,
						Confidence:         1.0,
						Reason:             "llm-resolved",
					}, nil
				}
			} else {
				log.Printf("intent llm: %v (falling back to keyword)", err)
			}
		}
	}

	intentLower := strings.ToLower(intentValue)
	payloadContent := strings.ToLower(payload.Content)
	var candidates []CandidateSummary

	for _, agent := range agents {
		for _, cap := range agent.Capabilities {
			score := scoreCapability(cap, intentLower, payloadContent, policyHints)
			if score > 0 {
				candidates = append(candidates, CandidateSummary{
					AgentID:    agent.ID,
					Capability: cap.Capability,
					Score:      score,
				})
			}
		}
	}

	if len(candidates) == 0 {
		return &ResolveResult{
			ResolvedCapability: "",
			Confidence:         0,
			Reason:             "no matching capability found",
		}, nil
	}

	sortCandidates(candidates)

	best := candidates[0]
	if best.Score < 0.3 {
		return &ResolveResult{
			ResolvedCapability: "",
			Confidence:         best.Score,
			Reason:             "low confidence match",
			Candidates:         candidates,
		}, nil
	}

	if len(candidates) > 1 {
		bestScore := candidates[0].Score
		secondScore := candidates[1].Score
		if bestScore > 0 && secondScore/bestScore > 0.85 {
			return &ResolveResult{
				ResolvedCapability: "",
				Confidence:         best.Score,
				Reason:             "ambiguous: multiple high-scoring capabilities",
				Candidates:         candidates,
			}, nil
		}
	}

	return &ResolveResult{
		ResolvedCapability: best.Capability,
		Confidence:         best.Score,
		Reason:             fmt.Sprintf("matched capability %s for agent %s", best.Capability, best.AgentID),
		Candidates:         candidates,
	}, nil
}

func scoreCapability(cap core.AgentCapability, intent, payloadContent string, hints []string) float64 {
	capName := strings.ToLower(cap.Capability)
	capDesc := strings.ToLower(cap.Description)
	score := 0.0

	if capName == intent {
		score += 1.0
	} else if strings.Contains(capName, intent) || strings.Contains(intent, capName) {
		score += 0.7
	}

	if capDesc != "" {
		descWords := strings.Fields(capDesc)
		for _, word := range descWords {
			if len(word) > 3 && strings.Contains(intent, strings.ToLower(word)) {
				score += 0.2
			}
		}
	}

	if payloadContent != "" {
		if strings.Contains(payloadContent, capName) {
			score += 0.3
		}
	}

	for _, hint := range hints {
		hintLower := strings.ToLower(hint)
		if strings.EqualFold(capName, hintLower) {
			score += 0.5
		}
	}

	if score > 1.0 {
		score = 1.0
	}
	return score
}

func sortCandidates(candidates []CandidateSummary) {
	for i := 0; i < len(candidates)-1; i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].Score > candidates[i].Score {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}
}

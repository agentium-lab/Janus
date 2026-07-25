package routing

import (
	"strings"

	"github.com/agentium-lab/Janus/core"
)

func scoreCandidate(c AgentCandidate, capability string, envelope core.TaskEnvelope) int {
	score := 0

	for _, cap := range c.Capabilities {
		if strings.EqualFold(cap.Capability, capability) {
			score += 10
		}
		if cap.Description != "" && strings.Contains(strings.ToLower(cap.Description), strings.ToLower(capability)) {
			score += 5
		}
	}

	if envelope.Policy != nil {
		for _, alias := range envelope.Policy.AllowedTools {
			for _, cap := range c.Capabilities {
				if strings.EqualFold(cap.Capability, alias) {
					score += 3
				}
			}
		}
	}

	score -= c.Backlog

	if score < 0 {
		score = 0
	}
	return score
}

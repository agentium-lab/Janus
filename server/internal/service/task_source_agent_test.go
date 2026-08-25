package service

import (
	"context"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
)

type fakeAgentExistence struct {
	exists map[string]bool
	called int
}

func (f *fakeAgentExistence) AgentExists(_ context.Context, tenantID, agentID string) (bool, error) {
	f.called++
	return f.exists[tenantID+"/"+agentID], nil
}

func sourceAgentTask(tenant, agent string) core.Task {
	return core.Task{
		TenantID:    tenant,
		ID:          "t-" + agent,
		SourceAgent: agent,
		TargetType:  "mailbox",
	}
}

func TestCreate_RejectsUnregisteredSourceAgent(t *testing.T) {
	checker := &fakeAgentExistence{exists: map[string]bool{"acme/reviewer": true}}
	s := NewTaskService(nil, nil, nil, nil).WithAgentExistence(checker)

	if _, err := s.Create(context.Background(), sourceAgentTask("acme", "ghost")); err == nil {
		t.Fatal("unregistered source_agent must be rejected")
	} else if !strings.Contains(err.Error(), "unknown source_agent") {
		t.Fatalf("wrong error: %v", err)
	}
	if checker.called != 1 {
		t.Fatalf("checker must be consulted once, got %d", checker.called)
	}

	if _, err := s.Create(context.Background(), sourceAgentTask("acme", "reviewer")); err != nil {
		if strings.Contains(err.Error(), "unknown source_agent") {
			t.Fatalf("registered agent must pass the ownership check, got: %v", err)
		}
	}
}

func TestCreate_NilCheckerKeepsLegacyBehavior(t *testing.T) {
	s := NewTaskService(nil, nil, nil, nil)
	_, err := s.Create(context.Background(), sourceAgentTask("acme", "anything"))
	if err != nil && strings.Contains(err.Error(), "unknown source_agent") {
		t.Fatal("nil checker must not introduce ownership errors")
	}
}

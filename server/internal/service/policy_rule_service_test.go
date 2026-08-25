package service

import (
	"context"
	"strings"
	"testing"

	"github.com/agentium-lab/Janus/core"
)

type fakePolicyRuleWriter struct{ created []core.PolicyRule }

func (f *fakePolicyRuleWriter) Create(_ context.Context, rule core.PolicyRule) error {
	f.created = append(f.created, rule)
	return nil
}

func (f *fakePolicyRuleWriter) ListActive(_ context.Context, _ string) ([]*core.PolicyRule, error) {
	return nil, nil
}

func TestPolicyRuleService_Validation(t *testing.T) {
	repo := &fakePolicyRuleWriter{}
	s := NewPolicyRuleService(repo)

	cases := []struct {
		name    string
		rule    core.PolicyRule
		wantErr string
	}{
		{"missing id", core.PolicyRule{Name: "n", Condition: raw(`{}`), Action: raw(`{}`)}, "id is required"},
		{"missing name", core.PolicyRule{ID: "r1", Condition: raw(`{}`), Action: raw(`{}`)}, "name is required"},
		{"empty condition", core.PolicyRule{ID: "r1", Name: "n", Action: raw(`{}`)}, "condition"},
		{"empty action", core.PolicyRule{ID: "r1", Name: "n", Condition: raw(`{}`)}, "action"},
		{"bad status", core.PolicyRule{ID: "r1", Name: "n", Status: "archived", Condition: raw(`{}`), Action: raw(`{}`)}, "active or disabled"},
	}
	for _, tc := range cases {
		if _, err := s.Create(context.Background(), "acme", tc.rule); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Fatalf("%s: want error containing %q, got %v", tc.name, tc.wantErr, err)
		}
	}

	ok, err := s.Create(context.Background(), "acme", core.PolicyRule{
		ID: "r1", Name: "n", Priority: 10,
		Condition: raw(`{"effect":"deny"}`),
		Action:    raw(`{"type":"deny"}`),
	})
	if err != nil {
		t.Fatalf("valid rule rejected: %v", err)
	}
	if ok.Status != "active" || ok.TenantID != "acme" || ok.CreatedAt.IsZero() || ok.UpdatedAt.IsZero() {
		t.Fatalf("defaults not applied: %+v", ok)
	}
	if len(repo.created) != 1 {
		t.Fatalf("repo Create called %d times", len(repo.created))
	}
}

func TestBudgetSpecService_UpsertValidation(t *testing.T) {
	repo := &fakeBudgetSpecRepo{}
	s := NewBudgetSpecService(repo)

	if _, err := s.Upsert(context.Background(), "acme", "galaxy", "", 0, 0, 0, 0, 0); err == nil {
		t.Fatal("unknown scope_type must be rejected")
	}
	if _, err := s.Upsert(context.Background(), "acme", "agent", "", 0, 0, 0, 0, 0); err == nil {
		t.Fatal("non-tenant scope without scope_id must be rejected")
	}
	spec, err := s.Upsert(context.Background(), "acme", "tenant", "", 60, 100000, 4, 5, 50)
	if err != nil {
		t.Fatalf("tenant-scope upsert rejected: %v", err)
	}
	if spec.ScopeType != core.BudgetScopeTenant || spec.RPM != 60 {
		t.Fatalf("spec not propagated: %+v", spec)
	}
}

type fakeBudgetSpecRepo struct{ upserts int }

func (f *fakeBudgetSpecRepo) Upsert(_ context.Context, _ core.BudgetSpec) error {
	f.upserts++
	return nil
}

func (f *fakeBudgetSpecRepo) Get(_ context.Context, _ string, _ core.BudgetScopeType, _ string) (*core.BudgetSpec, error) {
	return nil, nil
}

func (f *fakeBudgetSpecRepo) ListByTenant(_ context.Context, _ string) ([]*core.BudgetSpec, error) {
	return nil, nil
}

func raw(s string) []byte { return []byte(s) }

package postgres

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/agentium-lab/Janus/core"
)

func TestPolicyRuleRepo_CreateAndListActive(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewPolicyRuleRepository(pool)
	ctx := context.Background()

	rule := core.PolicyRule{
		TenantID: "acme",
		ID:       "rule-001",
		Name:     "Deny External Access",
		Status:   "active",
		Priority: 100,
		Condition: json.RawMessage(`{"actor.type":"external"}`),
		Action:   json.RawMessage(`{"decision":"deny"}`),
	}
	require.NoError(t, repo.Create(ctx, rule))

	rules, err := repo.ListActive(ctx, "acme")
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, "rule-001", rules[0].ID)
	assert.Equal(t, "Deny External Access", rules[0].Name)
	assert.Equal(t, 100, rules[0].Priority)
	assert.Contains(t, string(rules[0].Condition), "external")
	assert.Contains(t, string(rules[0].Action), "deny")
}

func TestPolicyRuleRepo_InactiveNotListed(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewPolicyRuleRepository(pool)
	ctx := context.Background()

	require.NoError(t, repo.Create(ctx, core.PolicyRule{
		TenantID: "acme", ID: "rule-active", Name: "Active Rule", Status: "active", Priority: 100,
		Condition: json.RawMessage(`{}`), Action: json.RawMessage(`{}`),
	}))
	require.NoError(t, repo.Create(ctx, core.PolicyRule{
		TenantID: "acme", ID: "rule-inactive", Name: "Inactive Rule", Status: "inactive", Priority: 200,
		Condition: json.RawMessage(`{}`), Action: json.RawMessage(`{}`),
	}))

	rules, err := repo.ListActive(ctx, "acme")
	require.NoError(t, err)
	assert.Len(t, rules, 1)
	assert.Equal(t, "rule-active", rules[0].ID)
}

func TestPolicyRuleRepo_ListActiveEmpty(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewPolicyRuleRepository(pool)
	ctx := context.Background()

	rules, err := repo.ListActive(ctx, "acme")
	require.NoError(t, err)
	assert.Len(t, rules, 0)
}

func TestPolicyRuleRepo_DuplicateID(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewPolicyRuleRepository(pool)
	ctx := context.Background()

	rule := core.PolicyRule{
		TenantID: "acme", ID: "rule-dup", Name: "Rule", Status: "active", Priority: 100,
		Condition: json.RawMessage(`{}`), Action: json.RawMessage(`{}`),
	}
	require.NoError(t, repo.Create(ctx, rule))
	err := repo.Create(ctx, rule)
	assert.Error(t, err, "duplicate policy rule ID should fail")
}

func TestPolicyRuleRepo_MultipleActiveSortedByPriority(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	insertTestTenant(t, pool, "acme")
	repo := NewPolicyRuleRepository(pool)
	ctx := context.Background()

	for _, r := range []core.PolicyRule{
		{TenantID: "acme", ID: "r3", Name: "Low Prio", Status: "active", Priority: 300, Condition: json.RawMessage(`{}`), Action: json.RawMessage(`{}`)},
		{TenantID: "acme", ID: "r1", Name: "High Prio", Status: "active", Priority: 100, Condition: json.RawMessage(`{}`), Action: json.RawMessage(`{}`)},
		{TenantID: "acme", ID: "r2", Name: "Mid Prio", Status: "active", Priority: 200, Condition: json.RawMessage(`{}`), Action: json.RawMessage(`{}`)},
	} {
		require.NoError(t, repo.Create(ctx, r))
	}

	rules, err := repo.ListActive(ctx, "acme")
	require.NoError(t, err)
	require.Len(t, rules, 3)
	assert.Equal(t, "r1", rules[0].ID)
	assert.Equal(t, "r2", rules[1].ID)
	assert.Equal(t, "r3", rules[2].ID)
}

func TestPolicyRuleRepo_GetNotFoundReturnsEmpty(t *testing.T) {
	pool := openTestDB(t)
	runMigration(t, pool)
	repo := NewPolicyRuleRepository(pool)
	ctx := context.Background()

	rules, err := repo.ListActive(ctx, "nonexistent")
	require.NoError(t, err)
	assert.Len(t, rules, 0)
}

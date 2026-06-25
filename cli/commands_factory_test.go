package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAPIKeyCmd(t *testing.T) {
	cmd := apiKeyCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, "api-key", cmd.Name())
	assert.True(t, cmd.HasSubCommands())
}

func TestPolicyCmd(t *testing.T) {
	cmd := policyCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, "policy", cmd.Name())
}

func TestPolicyCapabilityCmd(t *testing.T) {
	cmd := policyCapabilityCmd("allow", "Allow capability", "capability_allow", false)
	assert.NotNil(t, cmd)
	assert.Equal(t, "allow", cmd.Name())
}

func TestPolicyRequireApprovalCmd(t *testing.T) {
	cmd := policyRequireApprovalCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, "require-approval", cmd.Name())
}

func TestPolicyClassificationCmd(t *testing.T) {
	cmd := policyClassificationCmd("allow-classification", "Allow classification", true)
	assert.NotNil(t, cmd)
}

func TestPolicyToolCmd(t *testing.T) {
	cmd := policyToolCmd("allow-tool", "Allow tool", true)
	assert.NotNil(t, cmd)
}

func TestDLQCmd(t *testing.T) {
	cmd := dlqCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, "dlq", cmd.Name())
}

func TestProjectCmd(t *testing.T) {
	cmd := projectCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, "project", cmd.Name())
}

func TestProjectInitCmd(t *testing.T) {
	cmd := projectInitCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, "init", cmd.Name())
}

func TestProjectValidateCmd(t *testing.T) {
	cmd := projectValidateCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, "validate", cmd.Name())
}

func TestProjectDiffCmd(t *testing.T) {
	cmd := projectDiffCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, "diff", cmd.Name())
}

func TestProjectApplyCmd(t *testing.T) {
	cmd := projectApplyCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, "apply", cmd.Name())
}

func TestProjectSyncCmd(t *testing.T) {
	cmd := projectSyncCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, "sync", cmd.Name())
}

func TestTenantCmd(t *testing.T) {
	cmd := tenantCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, "tenant", cmd.Name())
}

func TestTenantAddCmd(t *testing.T) {
	cmd := tenantAddCmd()
	assert.NotNil(t, cmd)
	assert.Equal(t, "add", cmd.Name())
}

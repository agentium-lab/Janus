package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRootCmd_RegistersSubcommands(t *testing.T) {
	cmd := buildRootCmd()
	assert.Equal(t, "janus", cmd.Name())
	names := map[string]bool{}
	for _, sub := range cmd.Commands() {
		names[sub.Name()] = true
	}
	for _, want := range []string{"agent", "task", "mailbox", "dlq", "api-key", "policy", "project", "tenant", "dashboard"} {
		assert.True(t, names[want], "expected subcommand %q", want)
	}
}

func TestRunCLI_HelpSucceeds(t *testing.T) {
	require.NoError(t, runCLI([]string{"--help"}))
}

func TestRunCLI_UnknownSubcommandFails(t *testing.T) {
	err := runCLI([]string{"definitely-not-a-command"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown command")
}

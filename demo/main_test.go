package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnvOr(t *testing.T) {
	assert.Equal(t, "fallback", envOr("NONEXISTENT_KEY_XYZ", "fallback"))
	t.Setenv("TEST_ENV_OR_KEY", "value")
	assert.Equal(t, "value", envOr("TEST_ENV_OR_KEY", "fallback"))
}

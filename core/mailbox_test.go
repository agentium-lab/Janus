package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	assert.Equal(t, 5, p.MaxAttempts)
	assert.Equal(t, "exponential", p.BackoffType)
	assert.Equal(t, 10, p.InitialSeconds)
	assert.Equal(t, 900, p.MaxSeconds)
	assert.True(t, p.Jitter)
}

func TestRetryPolicy_BackoffCalculation(t *testing.T) {
	p := DefaultRetryPolicy()

	t.Run("first retry", func(t *testing.T) {
		d := p.BackoffDuration(1)
		assert.Equal(t, 10, int(d.Seconds()))
	})

	t.Run("second retry", func(t *testing.T) {
		d := p.BackoffDuration(2)
		assert.Equal(t, 20, int(d.Seconds()))
	})

	t.Run("third retry", func(t *testing.T) {
		d := p.BackoffDuration(3)
		assert.Equal(t, 40, int(d.Seconds()))
	})

	t.Run("caps at max_seconds", func(t *testing.T) {
		d := p.BackoffDuration(100)
		assert.Equal(t, 900, int(d.Seconds()))
	})

	t.Run("attempt 0 returns initial", func(t *testing.T) {
		d := p.BackoffDuration(0)
		assert.Equal(t, 10, int(d.Seconds()))
	})
}

func TestRetryPolicy_BackoffWithZeroInitial(t *testing.T) {
	p := RetryPolicy{
		MaxAttempts:    3,
		BackoffType:    "exponential",
		InitialSeconds: 1,
		MaxSeconds:     60,
		Jitter:         false,
	}

	d := p.BackoffDuration(3)
	assert.Equal(t, 4, int(d.Seconds()))
}

func TestRetryPolicy_ExceedsMaxAttempts(t *testing.T) {
	p := DefaultRetryPolicy()
	assert.True(t, p.ExceedsMaxAttempts(5))
	assert.True(t, p.ExceedsMaxAttempts(6))
	assert.False(t, p.ExceedsMaxAttempts(4))
	assert.False(t, p.ExceedsMaxAttempts(0))
}

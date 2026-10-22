package service

import (
	"context"
	"sync"
	"testing"

	"github.com/agentium-lab/Janus/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrent_StartCancelReplay(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	tenantID := "acme"
	taskID := "concurrent-start-cancel-replay"

	task := core.Task{
		ID:        taskID,
		TenantID:  tenantID,
		Status:    core.TaskStatusClaimed,
		MailboxID: "mb-1",
		Envelope: core.TaskEnvelope{
			TaskID:      taskID,
			TenantID:    tenantID,
			SourceAgent: "agent-1",
			Target:      core.Target{Type: core.TargetTypeAgent, Value: "agent-2"},
		},
	}
	err := env.taskRepo.Create(ctx, task)
	require.NoError(t, err)

	attempt := core.TaskAttempt{
		TenantID: tenantID,
		TaskID:   taskID,
		Attempt:  1,
		AgentID:  "agent-1",
		LeaseID:  "lease-1",
		Status:   "claimed",
	}
	err = env.attemptRepo.Create(ctx, attempt)
	require.NoError(t, err)

	var wg sync.WaitGroup
	var startErr, cancelErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		startErr = env.dispatch.StartTask(ctx, tenantID, taskID, "lease-1")
	}()

	go func() {
		defer wg.Done()
		cancelErr = env.taskSvc.Cancel(ctx, tenantID, taskID)
	}()

	wg.Wait()

	got, err := env.taskRepo.Get(ctx, tenantID, taskID)
	require.NoError(t, err)

	if startErr == nil && cancelErr == nil {
		if got.Status != core.TaskStatusCancelled {
			t.Errorf("Both succeeded but task is not cancelled: %s", got.Status)
		}
	} else if startErr == nil {
		if got.Status != core.TaskStatusRunning {
			t.Errorf("Start succeeded but task is not running: %s", got.Status)
		}
	} else if cancelErr == nil {
		if got.Status != core.TaskStatusCancelled {
			t.Errorf("Cancel succeeded but task is not cancelled: %s", got.Status)
		}
	}
}

func TestConcurrent_BlockACK(t *testing.T) {
	env := setupServiceTestEnv(t)
	ctx := context.Background()
	tenantID := "acme"
	taskID := "concurrent-block-ack"

	task := core.Task{
		ID:        taskID,
		TenantID:  tenantID,
		Status:    core.TaskStatusRunning,
		MailboxID: "mb-1",
		Envelope: core.TaskEnvelope{
			TaskID:      taskID,
			TenantID:    tenantID,
			SourceAgent: "agent-1",
			Target:      core.Target{Type: core.TargetTypeAgent, Value: "agent-2"},
		},
	}
	err := env.taskRepo.Create(ctx, task)
	require.NoError(t, err)

	attempt := core.TaskAttempt{
		TenantID: tenantID,
		TaskID:   taskID,
		Attempt:  1,
		AgentID:  "agent-1",
		LeaseID:  "lease-1",
		Status:   "running",
	}
	err = env.attemptRepo.Create(ctx, attempt)
	require.NoError(t, err)

	var wg sync.WaitGroup
	var blockErr, ackErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		blockErr = env.taskSvc.Block(ctx, tenantID, taskID, "test")
	}()

	go func() {
		defer wg.Done()
		ackErr = env.dispatch.AckTask(ctx, tenantID, taskID, "lease-1", "", nil)
	}()

	wg.Wait()

	got, err := env.taskRepo.Get(ctx, tenantID, taskID)
	require.NoError(t, err)

	if blockErr == nil && ackErr == nil {
		if got.Status != core.TaskStatusBlocked && got.Status != core.TaskStatusCompleted {
			t.Errorf("Both succeeded but task is in unexpected state: %s", got.Status)
		}
	} else if blockErr == nil {
		assert.Equal(t, core.TaskStatusBlocked, got.Status)
	} else if ackErr == nil {
		assert.Equal(t, core.TaskStatusCompleted, got.Status)
	}
}

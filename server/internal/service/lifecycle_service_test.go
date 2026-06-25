package service

import "testing"

// LifecycleService.ApplyTx is a thin wrapper over pool.Begin/Commit/Rollback.
// Its transactional commit/rollback semantics can only be meaningfully tested
// with a real PG pool, which is exercised by the PG-backed dispatch
// integration tests (see dispatch_service_test.go). These placeholders
// document the intent and keep the package testable without PG.

func TestLifecycleService_ApplyTx_Commits(t *testing.T) {
	t.Skip("ApplyTx commit semantics are covered by PG-backed dispatch integration tests")
}

func TestLifecycleService_ApplyTx_PropagatesFnError(t *testing.T) {
	t.Skip("ApplyTx rollback semantics are covered by PG-backed dispatch integration tests")
}

-- Placeholder kept for chain stability: the replay-ledger fix ships
-- entirely in code (ResetForReplayTx deletes the task's prior ledger
-- rows in the same transaction). Keeping the five-column ledger PK is
-- deliberate: the originally-proposed generation column would have
-- broken ON CONFLICT for v1.9.0 binaries during the rolling-upgrade
-- window (migrate-then-restart order).
SELECT 1;

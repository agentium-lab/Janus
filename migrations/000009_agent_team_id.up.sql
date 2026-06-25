-- Add team_id column to agents for team-scoped policy rules and routing.
ALTER TABLE agents ADD COLUMN IF NOT EXISTS team_id text;

-- Index to support team-scoped agent lookups and policy evaluation.
CREATE INDEX IF NOT EXISTS agents_team_idx ON agents (tenant_id, team_id) WHERE team_id IS NOT NULL;

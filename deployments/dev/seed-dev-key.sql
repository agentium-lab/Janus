-- Dev-only quickstart credentials. These values are committed to the public
-- repository and provide tenant-scoped API access on any stack seeded with
-- this file. NEVER use them against a production or internet-exposed Janus.
--
-- Key:    janus_e12ebdab09a71c856666756ab26fd12db630041a96559b74066f66820c1d6d48
-- Prefix: janus_e1   (first 8 chars, indexed for lookup)
-- Hash:   sha256(key) — matches server/internal/auth/apikey.go#hashKey
-- Usage:  export JANUS_API_KEY=<key above>, or pass api_key=<key> to the SDKs.

INSERT INTO tenants (id, name)
VALUES ('acme', 'Acme Corp (dev)')
ON CONFLICT (id) DO NOTHING;

INSERT INTO api_keys (tenant_id, key_hash, name, prefix)
VALUES (
    'acme',
    'fb828cbdd1e6a8023d7975d9305a959e785b3c40baae761a1f093dd5fa237d7b',
    'dev-quickstart',
    'janus_e1'
)
ON CONFLICT (tenant_id, key_hash) DO NOTHING;

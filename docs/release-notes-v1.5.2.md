# v1.5.2 — Release Pipeline Fixes

Hotfix release to restore a complete four-channel rollout (v1.5.1 published
npm only; PyPI was blocked by a test-dependency gap, GitHub Release/ghcr by
the driver-test issue fixed in v1.5.1's tag).

## Fixes

- **Python CI now installs declared dev deps**: `pip install -e ".[dev]"`
  brings `respx` and `pytest-asyncio` that `tests/test_client.py` imports —
  the hand-maintained `pip install httpx pydantic pytest` line was missing
  them, so the publish-python gate failed on a fresh runner while passing on
  dev machines that had the modules installed.
- **Driver integration tests skip when the server binary is absent**
  (carried from the v1.5.1 tag): redis/nats driver tests now `exec.LookPath`
  before forking `redis-server`/`nats-server` and `t.Skip` when neither the
  binary nor `JANUS_REDIS_ADDR`/`JANUS_NATS_URL` is available. The release
  verify job runs without infra containers and previously failed hard.

## Note on version numbering

v1.5.1 partially shipped (npm `@agentium-lab/janus-sdk@1.5.1` is live and
kept). v1.5.2 re-synchronizes all four channels — GitHub Release, ghcr,
PyPI, npm — with identical source.

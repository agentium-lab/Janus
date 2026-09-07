# v1.6.1 — Official-Client Interoperability Fixes

The new black-box interop suite (the official a2a-go v2.0.0 client driving
Janus over real HTTP) caught three wire-format bugs present in v1.6.0 —
our own unit tests had asserted our own shapes and could not see them:

1. **Agent Card securitySchemes**: emitted OpenAPI-style `{type,in,name}`;
   the official parser expects the oneof shape `{apiKey:{location,name}}`.
2. **securityRequirements**: scopes must be wrapped in a `{schemes:{...}}`
   object.
3. **GetTask / CancelTask**: must return the bare Task object; the official
   transport decodes directly into `a2a.Task`, so v1.6.0's StreamResponse
   `{"task":...}` wrapper yielded tasks with empty IDs.

With these fixes the official client completes all five conformance
scenarios: card resolution, non-streaming send, streaming lifecycle,
Get/List (cursor pagination)/Cancel, and terminal-subscribe rejection.
The suite (`server/tests/interop`) runs in CI.

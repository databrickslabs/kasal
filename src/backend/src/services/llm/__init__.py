"""
LLM configuration: turning a model KEY into a configured, credentialed client.

``manager`` (``LLMManager``) resolves a catalogue entry to an endpoint, applies
per-endpoint parameter rules, and attaches per-tenant credentials.
``embeddings`` does the same for embedding models.

This is a SERVICE, not core, and the split is the one `backend/CLAUDE.md`
already described: `core/llm/transport` is model- and tenant-agnostic; anything
that reads the model catalogue or a tenant's API key is per-tenant, DB-backed
business logic. Keeping it in `core/` meant `core` imported `services` at module
level for `ModelConfigService` and `ApiKeysService`.

What stayed in `core/llm/`: the transport, the endpoint handlers, context-limit
phrases, JSON extraction and the subprocess token — none of which touch the
database.
"""

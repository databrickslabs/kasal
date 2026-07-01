# CrewAI Tools CLAUDE.md

Instructions for `src/backend/src/engines/crewai/tools/`. This is the agent tool
system: the `tool_factory.py` registry plus `custom/` implementations.

## A tool is only usable when registered everywhere

Adding a `BaseTool` subclass is not enough. A tool must be wired through the whole
chain or it will not resolve at runtime ("tool not found"):

1. **Implementation** — `custom/<name>_tool.py` (a `crewai.tools.BaseTool` subclass).
2. **Factory import + registry** — a guarded import and an entry in
   `tool_factory.py`'s `self._tool_implementations` dict, keyed by the **display
   title**.
3. **DB seed** — a `(id, title, description, icon)` tuple in `seeds/tools.py`
   whose `title` **exactly matches** the factory key. Re-run `python run_seeders.py`.
4. **Optional config** — defaults in `get_tool_configs()` (keyed by the string id).

The `#1 cause of "tool not found"` is a mismatch between the seed `title` and the
factory dict key — keep them byte-identical.

## Implementation conventions (match `custom/perplexity_tool.py`)

- Subclass `BaseTool`; set `name`, `description`, and `args_schema` (a Pydantic
  `*Input(BaseModel)` with `Field(..., description=...)`).
- Store credentials/config in `PrivateAttr`, accept in `__init__`, call
  `super().__init__()`.
- Implement `_run(...) -> str`; catch external I/O errors and return a readable
  message instead of raising into the agent loop.

## Security (non-negotiable)

- **Mask secrets before logging.** Never log a raw API key/token/password. Use
  `from src.utils.sensitive_data_utils import mask_sensitive_fields` on any config
  dict you log.
- Credentials in `tool_configs` are encrypted at rest by the agent/service layer;
  they are decrypted only in-memory for tool construction. Do not write decrypted
  values back to the DB or into logs/traces.
- If the tool touches sensitive capabilities, declare it in
  `security/tool_capability_manifest.py` so the scanner pipeline can reason about it.

## Databricks telemetry (required for Databricks callers)

Every Databricks REST/SDK call needs the Kasal User-Agent (Partner
Well-Architected). Do NOT add it to non-Databricks calls.
- REST: merge `get_user_agent_header(KasalProduct.X)` into headers.
- SDK: `with_product(f"{KASAL_BASE}_{KasalProduct.X}", VERSION)` before building the client.
- Add a new `KasalProduct` constant in `utils/telemetry.py` if none fits.

## MCP tools

Real MCP integration lives here (`mcp_handler.py`, `mcp_integration.py`). There is
no top-level `mcp/` package anymore — it was removed in the engine refactor.

## Notes

- `custom/` is large and includes the flat PowerBI → UCMV → Genie pipeline
  (~20+ files + util subpackages). When adding to that vertical, keep related
  helpers together rather than scattering them at the top level.
- Use the `new-crewai-tool` skill to scaffold all of the above in one pass.

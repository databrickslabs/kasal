---
name: new-crewai-tool
description: Scaffold and fully register a new custom CrewAI tool in Kasal — BaseTool subclass, tool factory registration, DB seed entry, optional config, secret masking, and Databricks User-Agent telemetry. Use when adding a new agent tool to the CrewAI engine.
---

# new-crewai-tool

Add a custom tool that agents can use, wired through Kasal's full registration
chain. A tool is not usable until it is registered in **all** the places below —
a class alone does nothing. Model your output on
`engines/crewai/tools/custom/perplexity_tool.py` and the `PerplexityTool`
registration path.

All paths are under `src/backend/src/`.

## Inputs to gather

- **Display title** (what users pick in the UI, stored as `tool.title`):
  e.g. `"Weather Tool"`.
- **Class name**: `WeatherTool`.
- **A unique numeric tool ID** for the seed (pick an unused integer — check
  `seeds/tools.py`).
- **Inputs** the tool takes (become the args schema).
- Whether it calls a **Databricks API** (decides telemetry) and whether it needs
  **credentials** (decides secret masking + config).

## Steps

### 1. Implement the tool — `engines/crewai/tools/custom/<name>_tool.py`
- Subclass `crewai.tools.BaseTool`.
- Class attributes: `name: str`, `description: str`, `args_schema: Type[BaseModel]`.
- Define a Pydantic `*Input(BaseModel)` for arguments with `Field(..., description=...)`.
- Hold credentials/config in `PrivateAttr` (e.g. `_api_key: Optional[str] = PrivateAttr(default=None)`),
  accept them in `__init__`, call `super().__init__()`.
- Implement `_run(self, ...) -> str`. Wrap external I/O in try/except and return a
  legible error string rather than raising into the agent loop.
- **Secret masking (required if it logs config/secrets):** before logging any
  config dict, mask it:
  ```python
  from src.utils.sensitive_data_utils import mask_sensitive_fields
  logger.info("tool config: %s", mask_sensitive_fields(config))
  ```
  Never log a raw API key / token / password.

### 2. Databricks User-Agent telemetry (required for Databricks callers only)
If the tool calls a Databricks REST API or SDK, attach the Kasal User-Agent
(Partner Well-Architected requirement). Do NOT add telemetry to non-Databricks
calls (e.g. weather/PowerBI/external APIs).
- REST (httpx/requests):
  ```python
  from src.utils.telemetry import get_user_agent_header, KasalProduct
  headers = {"Authorization": f"Bearer {token}", **get_user_agent_header(KasalProduct.YOUR_PRODUCT)}
  ```
- SDK:
  ```python
  from databricks.sdk.useragent import with_product
  from src.utils.telemetry import KASAL_BASE, VERSION, KasalProduct
  with_product(f"{KASAL_BASE}_{KasalProduct.YOUR_PRODUCT}", VERSION)
  w = WorkspaceClient()
  ```
- If no existing `KasalProduct` fits, add a new constant in `utils/telemetry.py`.

### 3. Register in the tool factory — `engines/crewai/tools/tool_factory.py`
- Add a guarded import near the top (mirror the existing try/except blocks so a
  missing optional dep degrades gracefully):
  ```python
  try:
      from .custom.weather_tool import WeatherTool
  except ImportError:
      WeatherTool = None
      logging.warning("Could not import WeatherTool")
  ```
- Add it to the `self._tool_implementations` dict (keyed by the display title, and
  optionally a class-name alias):
  ```python
  "Weather Tool": WeatherTool,
  ```
- If it needs per-call configuration/credentials, handle its branch in
  `create_tool` where other tools (e.g. `"PerplexityTool"`) are instantiated with
  their config.

### 4. Seed the tool — `seeds/tools.py`
- Add a tuple to `tools_data`: `(id, title, description, icon)` — e.g.
  `(<id>, "Weather Tool", "…detailed description…", "web")`. The `title` MUST match
  the factory dict key; otherwise `_load_available_tools` won't resolve it.
- If it has default config, add an entry keyed by the **string** id in
  `get_tool_configs()` (e.g. `"<id>": {...}`).
- Re-seed: `cd src/backend && python run_seeders.py`.

### 5. (If capability-gated) security manifest
- If the tool touches sensitive capabilities, register it in
  `engines/crewai/security/tool_capability_manifest.py` so the scanner pipeline
  knows its surface.

## Validation before done

```bash
cd src/backend
.venv/bin/python -c "from src.engines.crewai.tools.custom.weather_tool import WeatherTool; print('import ok')"
python run_seeders.py
python run_tests.py --type unit -k weather
.venv/bin/python -m black src && .venv/bin/python -m isort src
```

Confirm the tool appears in `_tool_implementations` and that its seed `title`
matches the factory key exactly (title mismatch is the #1 "tool not found" cause).
See `src/backend/src/engines/crewai/tools/CLAUDE.md` and `../CLAUDE.md`.

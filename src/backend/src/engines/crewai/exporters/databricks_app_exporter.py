"""Databricks App exporter for CrewAI crews (template-driven).

Renders a CrewAI-adapted copy of Databricks' official agent-app template
(bundled under ``./templates/databricks_app``) into a deployable project:
MLflow ``AgentServer`` + ``ResponsesAgent``, ``app.yaml`` (``uv run start-app``),
``databricks.yml`` (DABs), ``pyproject.toml`` with script entrypoints,
``scripts/`` and ``.claude/skills``. The crew runs *behind* the ResponsesAgent
interface, reading ``config/agents.yaml`` + ``config/tasks.yaml`` at runtime.

The template files carry ``{{TOKEN}}`` placeholders; this exporter substitutes
them and appends the generated crew config so the output is deploy-ready. The
returned ``files`` list keeps the existing export contract (the router zips it).
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles

from .base_exporter import BaseExporter
from .secret_hints import SECRET_KEY_HINTS as _SECRET_KEY_HINTS
from .yaml_generator import YAMLGenerator

TEMPLATE_DIR = Path(__file__).parent / "templates" / "databricks_app"
# The ONE portable A2UI composer (stdlib-only), shared with the live app and
# vendored verbatim into every export so generative UI never forks into a second
# implementation. ``parents[3]`` is ``src/backend/src``.
SHARED_A2UI_DIR = Path(__file__).parents[3] / "shared" / "a2ui"
# The shared frontend A2UI renderer (self-contained React+TS module), vendored
# verbatim into the export so the deployed UI draws surfaces with the SAME renderer
# as Kasal chat. It lives IN the template tree (not the frontend source) so it
# ships with the backend and an export from the DEPLOYED app — which does NOT ship
# the frontend source — still gets it. The canonical source is
# ``src/frontend/src/shared/a2ui``; this committed copy is kept in sync by a test
# (test_a2ui_frontend_vendor_in_sync). See _a2ui_frontend_files().
SHARED_A2UI_FRONTEND_DIR = TEMPLATE_DIR / "frontend" / "src" / "a2ui"
# Standalone tool implementations bundled into the app (no Kasal `src.*` deps).
BUNDLED_TOOLS_DIR = Path(__file__).parent / "templates" / "_app_bundled_tools"
# Kasal's runtime custom-tool implementations (some are self-contained).
CUSTOM_TOOLS_DIR = Path(__file__).parent.parent / "tools" / "custom"

# Tools we can faithfully reproduce inside a standalone Databricks App, keyed by
# the tool's Kasal title (what `config/agents.yaml` carries). Each spec declares:
#   import      — import line for the tool class
#   class       — the class name to instantiate
#   config_keys — non-secret config keys baked into the factory from the crew's
#                 tool config (api keys/secrets are NEVER baked; they come via env)
#   env         — env vars the tool needs (surfaced in `.env.example` + app.yaml)
#   deps        — extra pip deps beyond the template's base set
#   bundle      — (filename, source) self-contained impl to ship under `tools/`;
#                 source is "bundled" (BUNDLED_TOOLS_DIR) or "custom" (CUSTOM_TOOLS_DIR)
#   special     — custom factory builder key (e.g. "genie")
_BUNDLEABLE_TOOLS: Dict[str, Dict[str, Any]] = {
    "SerperDevTool": {
        "import": "from crewai_tools import SerperDevTool",
        "class": "SerperDevTool",
        "config_keys": ["n_results", "country", "locale", "location", "search_type"],
        "env": ["SERPER_API_KEY"],
    },
    "ScrapeWebsiteTool": {
        "import": "from crewai_tools import ScrapeWebsiteTool",
        "class": "ScrapeWebsiteTool",
        "config_keys": ["website_url"],
        "deps": ['"beautifulsoup4>=4.12.0"'],
    },
    "Dall-E Tool": {
        "import": "from crewai_tools import DallETool",
        "class": "DallETool",
        "config_keys": ["model", "size", "quality", "n"],
        "env": ["OPENAI_API_KEY"],
    },
    "PerplexityTool": {
        "import": "from tools.perplexity_tool import PerplexitySearchTool",
        "class": "PerplexitySearchTool",
        "config_keys": [
            "model",
            "max_tokens",
            "temperature",
            "top_p",
            "top_k",
            "presence_penalty",
            "frequency_penalty",
            "search_recency_filter",
            "search_domain_filter",
            "return_images",
            "return_related_questions",
            "web_search_options",
        ],
        "env": ["PERPLEXITY_API_KEY"],
        "deps": ['"requests>=2.31.0"'],
        "bundle": ("perplexity_tool.py", "custom"),
    },
    "GenieTool": {
        "import": "from tools.genie_tool import GenieTool",
        "class": "GenieTool",
        # space_id is baked as the default; GENIE_SPACE_ID env overrides it.
        "config_keys": ["max_result_rows"],
        "env": ["GENIE_SPACE_ID"],
        "bundle": ("genie_tool.py", "bundled"),
        "special": "genie",
    },
}

# Class-name aliases that may appear instead of the canonical Kasal title.
_TOOL_ALIASES = {"DallETool": "Dall-E Tool", "GmailTool": "Gmail"}

# Config keys that look like secrets are never baked into the export — the
# deployed app reads them from env vars / OBO instead. ``_SECRET_KEY_HINTS`` is
# imported at the top from the shared ``secret_hints`` single source of truth.

# OS/editor junk that must never be emitted into the exported project (and which
# would crash the UTF-8 template read if walked).
_SKIP_FILES = {".DS_Store", "Thumbs.db", ".pyc", "CLAUDE.md"}

_EXT_TYPE = {
    ".py": "python",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".md": "markdown",
    ".toml": "toml",
    ".json": "json",
    ".txt": "text",
}


class DatabricksAppExporter(BaseExporter):
    """Export a crew as a Databricks App project (template-driven)."""

    def __init__(self):
        super().__init__()
        self.yaml_generator = YAMLGenerator()

    async def export(
        self, crew_data: Dict[str, Any], options: Dict[str, Any]
    ) -> Dict[str, Any]:
        crew_name = crew_data.get("name", "crew")
        agents = crew_data.get("agents", [])
        tasks = crew_data.get("tasks", [])
        mcp_servers = crew_data.get("mcp_servers", [])
        # Non-secret config per tool title (e.g. GenieTool space_id, Serper
        # n_results), so the deployed crew's tools are configured like Kasal's.
        tool_configs = crew_data.get("tool_configs", {}) or {}
        # Tools referenced by the crew that can't run standalone — populated by
        # _tool_map and surfaced in metadata so they're never silently dropped.
        self._unsupported_tools: List[str] = []

        include_custom_tools = options.get("include_custom_tools", True)
        include_obo = options.get("include_obo_auth", True)
        include_comments = options.get("include_comments", True)
        model_override = options.get("model_override") or None

        # Deploy-time selections (set by the one-click deploy); fall back to the
        # workspace's configured catalog/schema for plain downloads.
        experiment_id = options.get("experiment_id") or ""
        catalog = (
            options.get("databricks_catalog")
            or crew_data.get("databricks_catalog")
            or ""
        )
        schema = (
            options.get("databricks_schema") or crew_data.get("databricks_schema") or ""
        )

        tools = self._get_unique_tools(agents, tasks)
        sanitized = self._sanitize_name(crew_name)
        app_name = self._app_name(crew_name)
        bundle_name = sanitized
        display_name = crew_name.replace("_", " ").title()
        input_key = self._detect_input_key(agents, tasks)

        tokens = {
            "{{APP_NAME}}": app_name,
            "{{BUNDLE_NAME}}": bundle_name,
            "{{DISPLAY_NAME}}": display_name,
            "{{DESCRIPTION}}": (
                f"CrewAI crew '{display_name}' deployed as a Databricks App."
            ),
            "{{NAME}}": app_name,
            "{{INPUT_KEY}}": input_key,
            "{{CREW_PURPOSE}}": self._crew_purpose(display_name, tasks),
            # Optional bias for the A2UI composer's surface kind, sniffed from
            # the crew's purpose/task output specs (empty = composer decides).
            "{{A2UI_HINT}}": self._a2ui_hint(crew_data, tasks),
            # Crew-relevant example prompts for the UI empty state, derived from
            # THIS crew's tasks (crews are generated dynamically — no static
            # examples). JSON array literal; "[]" hides the suggestion row.
            "{{STARTER_PROMPTS_JSON}}": self._starter_prompts(crew_data, tasks),
            # Workspace deck/quiz theme palettes (deliverable -> palette) from the
            # UIConfigurator, so the deployed app's themes match this workspace's
            # live chat. JSON object literal ("{}" = use the built-in themes only).
            "{{WORKSPACE_THEMES_JSON}}": json.dumps(
                crew_data.get("a2ui_themes") or {}, ensure_ascii=False
            ),
            "{{MODEL_OVERRIDE}}": repr(model_override),
            "{{ENABLE_OBO}}": "True" if include_obo else "False",
            "{{MCP_SERVERS}}": self._mcp_block(mcp_servers),
            "{{TOOL_IMPORTS}}": self._tool_imports(tools, include_custom_tools),
            "{{TOOL_MAP}}": self._tool_map(tools, tool_configs, include_custom_tools),
            "{{EXTRA_DEPENDENCIES}}": self._extra_deps(
                tools, include_custom_tools, has_mcp=bool(mcp_servers)
            ),
            "{{ENV_TOOL_KEYS}}": self._env_keys(tools, mcp_servers),
            # Crew execution settings (mirror Kasal's runtime).
            "{{PROCESS}}": crew_data.get("process") or "sequential",
            "{{PLANNING}}": "True" if crew_data.get("planning") else "False",
            "{{PLANNING_LLM}}": repr(crew_data.get("planning_llm") or None),
            "{{REASONING}}": "True" if crew_data.get("reasoning") else "False",
            "{{MANAGER_LLM}}": repr(crew_data.get("manager_llm") or None),
            # CrewAI's built-in memory embeds + extracts via OpenAI by default,
            # which fails on a Databricks-only app (OPENAI_API_KEY required). Keep
            # it OFF until the deploy wires a Databricks-backed memory backend
            # (Lakebase / Vector Search) so the app runs purely on Databricks.
            "{{MEMORY}}": "False",
            # Deploy-time env values written into app.yaml.
            "{{EXPERIMENT_ID}}": experiment_id,
            # The app creates this MLflow experiment UC-bound at startup.
            "{{MLFLOW_EXPERIMENT_NAME}}": options.get("mlflow_experiment_name") or "",
            "{{DATABRICKS_CATALOG}}": catalog,
            "{{DATABRICKS_SCHEMA}}": schema,
            # Lakebase instance attached at deploy time (empty for plain export).
            "{{LAKEBASE_INSTANCE}}": options.get("lakebase_instance") or "",
            "{{LAKEBASE_DATABASE}}": "databricks_postgres",
            # SQL warehouse the app uses to provision its UC trace tables. Deploy
            # selection first, else the workspace's configured warehouse.
            "{{DATABRICKS_WAREHOUSE_ID}}": (
                options.get("databricks_warehouse_id")
                or crew_data.get("databricks_warehouse_id")
                or ""
            ),
        }

        files: List[Dict[str, str]] = []

        # 1. Render the bundled template tree with token substitution.
        for path in sorted(TEMPLATE_DIR.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(TEMPLATE_DIR).as_posix()
            # A `*.template` file is emitted with its `.template` suffix stripped.
            # In particular the app manifest ships as `app.yaml.template`, NOT a
            # literal `app.yaml`: the Databricks Marketplace resolver recursively
            # scans a listing's source tree for app manifests, and a nested
            # placeholder `app.yaml` (full of {{TOKEN}}s) makes it stall for 30s
            # (DEADLINE_EXCEEDED). Storing it as `.template` hides it from that
            # scan while the exported project still gets a real `app.yaml`.
            out_rel = rel[: -len(".template")] if rel.endswith(".template") else rel
            # Skip OS/editor junk, Python caches, and frontend deps/build artifacts
            # (node_modules/dist/...) so they're never templated or shipped — they'd
            # bloat the export and could crash the UTF-8 read.
            parts = set(rel.split("/"))
            if (
                path.name in _SKIP_FILES
                or "__pycache__" in parts
                or parts & {"node_modules", "dist", ".vite", "build", ".turbo"}
                # The vendored A2UI frontend is emitted VERBATIM by
                # _a2ui_frontend_files() (no token substitution — TS must not have
                # {{...}} tokens rewritten). Skip it here to avoid a double emission.
                or rel.startswith("frontend/src/a2ui/")
            ):
                continue
            try:
                async with aiofiles.open(path, "r", encoding="utf-8") as f:
                    content = await f.read()
            except (UnicodeDecodeError, ValueError):
                # Non-text/binary file (e.g. .DS_Store, an image) — can't be
                # templated, so leave it out rather than crashing the export.
                self.logger.warning(f"Skipping non-text template file: {rel}")
                continue
            for token, value in tokens.items():
                content = content.replace(token, value)
            files.append(
                {"path": out_rel, "content": content, "type": self._ftype(out_rel)}
            )

        # 1b. Vendor the shared A2UI composer + bake this workspace's resolved UI
        #     config (catalog/directives/enabled) under agent_server/a2ui/ so the
        #     deployed generative UI uses the SAME composer as live Kasal chat.
        files.extend(await self._a2ui_vendor_files(crew_data))

        # 1c. Vendor the shared FRONTEND renderer verbatim under frontend/src/a2ui/
        #     so the deployed UI draws surfaces with the SAME renderer as live chat.
        files.extend(await self._a2ui_frontend_files())

        # 2. Generated crew config (read at runtime by agent_server/agent.py).
        files.append(
            {
                "path": "config/agents.yaml",
                "content": self.yaml_generator.generate_agents_yaml(
                    agents, model_override=None, include_comments=include_comments
                ),
                "type": "yaml",
            }
        )
        files.append(
            {
                "path": "config/tasks.yaml",
                "content": self.yaml_generator.generate_tasks_yaml(
                    tasks,
                    agents,
                    include_comments=include_comments,
                    include_guardrails=True,
                ),
                "type": "yaml",
            }
        )

        # 3. Bundle self-contained tool implementations the crew uses (e.g.
        #    PerplexityTool, GenieTool) under tools/ so they run in the app.
        if include_custom_tools:
            bundled = await self._bundle_tool_files(tools)
            if bundled:
                files.append(
                    {"path": "tools/__init__.py", "content": "", "type": "python"}
                )
                files.extend(bundled)

        return {
            "crew_id": str(crew_data.get("id", "")),
            "crew_name": crew_name,
            "export_format": "databricks_app",
            "files": files,
            "metadata": {
                "agents_count": len(agents),
                "tasks_count": len(tasks),
                "tools_count": len(tools),
                "sanitized_name": sanitized,
                "app_name": app_name,
                "bundle_name": bundle_name,
                "include_obo_auth": include_obo,
                "input_key": input_key,
                # Tools the crew uses that can't run standalone (Kasal-internal);
                # attach these via MCP or add an implementation under tools/.
                "unsupported_tools": sorted(set(self._unsupported_tools)),
            },
            "generated_at": self._get_timestamp(),
            "size_bytes": sum(len(f["content"]) for f in files),
        }

    # ── Token builders ─────────────────────────────────────────────────

    def _app_name(self, crew_name: str) -> str:
        """Databricks App name: lowercase, hyphenated, 2-30 chars, no edge hyphens."""
        slug = re.sub(r"[^a-z0-9]+", "-", crew_name.lower()).strip("-")
        if not slug or not slug[0].isalpha():
            slug = f"agent-{slug}".strip("-")
        slug = slug[:30].strip("-")
        return slug or "agent-crew"

    def _detect_input_key(
        self, agents: List[Dict[str, Any]], tasks: List[Dict[str, Any]]
    ) -> str:
        """Infer the crew input key from ``{placeholder}`` tokens, default ``topic``."""
        pattern = re.compile(r"\{(\w+)\}")
        scan = [
            (tasks, ("description", "expected_output")),
            (agents, ("goal", "backstory", "role")),
        ]
        for collection, fields in scan:
            for item in collection:
                for field in fields:
                    match = pattern.search(str(item.get(field) or ""))
                    if match:
                        return match.group(1)
        return "topic"

    def _crew_purpose(self, display_name: str, tasks: List[Dict[str, Any]]) -> str:
        """A short description of what the crew does, for the conversation layer."""
        parts = [display_name]
        for task in tasks:
            text = (
                task.get("description") or task.get("expected_output") or ""
            ).strip()
            if text:
                parts.append(text)
        purpose = " ".join(parts)
        # Keep it safe to embed in a triple-quoted Python string and bounded.
        purpose = purpose.replace('"""', "'''").replace("\\", " ")
        return purpose[:600]

    # Keyword -> preferred A2UI surface kind. Ordered: first match wins.
    _A2UI_KEYWORDS = [
        (
            ("presentation", "slides", "slide deck", "deck", "powerpoint", "pitch"),
            "presentation",
        ),
        (
            (
                "dashboard",
                "kpi",
                "metrics",
                "chart",
                "charts",
                "graph",
                "plot",
                "analytics",
            ),
            "dashboard",
        ),
        (("mindmap", "mind map", "concept map", "tree diagram"), "mindmap"),
        (("quiz", "quizzes", "assessment", "trivia", "exam"), "quiz"),
        (
            ("report", "document", "article", "whitepaper", "summary", "brief"),
            "document",
        ),
    ]

    def _a2ui_hint(self, crew_data: Dict[str, Any], tasks: List[Dict[str, Any]]) -> str:
        """Bias the A2UI composer's surfaceKind from crew/task metadata.

        Sniffs the crew name + task descriptions/expected_output for content-type
        keywords; structured task output (output_pydantic/output_json) implies a
        dashboard. Returns a one-line hint, or "" to let the composer decide.
        """
        haystack = " ".join(
            [str(crew_data.get("name", ""))]
            + [str(t.get("expected_output", "")) for t in tasks]
            + [str(t.get("description", "")) for t in tasks]
        ).lower()
        for words, kind in self._A2UI_KEYWORDS:
            if any(w in haystack for w in words):
                hint = f"Prefer surfaceKind '{kind}'."
                break
        else:
            structured = any(
                t.get("output_pydantic") or t.get("output_json") for t in tasks
            )
            hint = (
                "The output is structured data; prefer a 'dashboard' with Table/Chart/KeyValue."
                if structured
                else ""
            )
        # Safe to embed in a triple-quoted Python string.
        return hint.replace('"""', "'''").replace("\\", " ")

    def _starter_prompts(
        self, crew_data: Dict[str, Any], tasks: List[Dict[str, Any]]
    ) -> str:
        """JSON array of crew-relevant example prompts for the UI empty state.

        Derived from the crew's own task descriptions so the suggestions reflect
        what THIS crew actually does (crews are generated dynamically, so static
        examples would be misleading). Each prompt is the first sentence of a
        task, cleaned to a single line and dropped if it contains template
        placeholders. Returns a JSON array literal (possibly "[]") that is safe
        to inline directly into the React source.
        """
        prompts: List[str] = []
        seen = set()
        for task in tasks:
            text = (
                task.get("description") or task.get("expected_output") or ""
            ).strip()
            if not text:
                continue
            # First sentence, collapsed to a single line.
            text = re.split(r"(?<=[.!?])\s", text.replace("\n", " "))[0].strip()
            text = re.sub(r"\s+", " ", text)
            if not text or "{" in text or "}" in text or len(text) < 12:
                continue
            if len(text) > 100:
                text = text[:100].rsplit(" ", 1)[0].strip() + "…"
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            prompts.append(text)
            if len(prompts) >= 3:
                break
        return json.dumps(prompts, ensure_ascii=False)

    def _mcp_block(self, mcp_servers: List[Dict[str, Any]]) -> str:
        """Emit (name, url, transport) tuples for the crew's MCP servers.

        The transport is explicit so the app's MCPServerAdapter connects
        correctly: Databricks-managed MCP is streamable-HTTP (the adapter would
        otherwise default to SSE and time out after ~30s); third-party servers
        keep their configured transport.
        """
        lines = []
        for server in mcp_servers or []:
            name = str(server.get("name", "mcp")).replace('"', "'")
            url = server.get("server_url") or ""
            if "/api/2.0/mcp/" in url:
                # Databricks-managed: keep the host-relative path; always HTTP.
                url = "/api/2.0/mcp/" + url.split("/api/2.0/mcp/", 1)[1]
                transport = "streamable-http"
            else:
                transport = (
                    "streamable-http"
                    if server.get("server_type") == "streamable"
                    else "sse"
                )
            lines.append(f'    ("{name}", "{url}", "{transport}"),')
        return ("\n".join(lines) + "\n") if lines else ""

    def _spec_for(self, title: str) -> Optional[Dict[str, Any]]:
        """Return the bundle spec for a tool title (resolving class-name aliases)."""
        return _BUNDLEABLE_TOOLS.get(title) or _BUNDLEABLE_TOOLS.get(
            _TOOL_ALIASES.get(title, "")
        )

    def _tool_imports(self, tools: List[str], include_custom: bool) -> str:
        imports: List[str] = []
        for tool in tools:
            spec = self._spec_for(tool)
            if not spec:
                continue
            # Bundled (custom) tools are only importable when bundling is enabled.
            if spec.get("bundle") and not include_custom:
                continue
            stmt = spec["import"]
            if stmt not in imports:
                imports.append(stmt)
        return ("\n".join(imports) + "\n") if imports else ""

    def _clean_config(self, cfg: Dict[str, Any], allowed: List[str]) -> Dict[str, Any]:
        """Keep only allowed, non-secret, non-empty config keys for a factory."""
        clean: Dict[str, Any] = {}
        for key in allowed:
            if key not in cfg:
                continue
            if any(hint in key.lower() for hint in _SECRET_KEY_HINTS):
                continue
            value = cfg[key]
            if value in (None, "", [], {}):
                continue
            clean[key] = value
        return clean

    def _factory_expr(
        self, title: str, spec: Dict[str, Any], cfg: Dict[str, Any]
    ) -> str:
        """Build the tool constructor call, baking in the crew's non-secret config."""
        cls = spec["class"]
        if spec.get("special") == "genie":
            space = cfg.get("space_id") or cfg.get("spaceId") or ""
            if isinstance(space, list):
                space = space[0] if space else ""
            kwargs = [f'space_id=os.environ.get("GENIE_SPACE_ID") or {space!r}']
            rows = cfg.get("max_result_rows")
            if isinstance(rows, int) and rows > 0:
                kwargs.append(f"max_result_rows={rows}")
            return f"{cls}({', '.join(kwargs)})"
        # SerperDevTool: Kasal stores the search kind as `endpoint_type`.
        if title == "SerperDevTool" and cfg.get("endpoint_type") in ("search", "news"):
            cfg = {**cfg, "search_type": cfg["endpoint_type"]}
        clean = self._clean_config(cfg, spec.get("config_keys", []))
        return f"{cls}(**{clean!r})" if clean else f"{cls}()"

    def _tool_map(
        self,
        tools: List[str],
        tool_configs: Dict[str, Any],
        include_custom: bool,
    ) -> str:
        lines: List[str] = []
        for tool in tools:
            spec = self._spec_for(tool)
            cfg = tool_configs.get(tool, {}) or {}
            if spec and (include_custom or not spec.get("bundle")):
                expr = self._factory_expr(tool, spec, cfg)
                # Key by the title the crew's agents.yaml carries.
                lines.append(f'    "{tool}": lambda: {expr},')
            else:
                # Not reproducible standalone (Kasal-internal tool). Flag it
                # rather than emit a call that NameErrors at runtime.
                self._unsupported_tools.append(tool)
                lines.append(
                    f'    # "{tool}": unsupported standalone — attach via MCP '
                    f"or add an implementation under tools/."
                )
        return ("\n".join(lines) + "\n") if lines else ""

    def _extra_deps(
        self, tools: List[str], include_custom: bool, has_mcp: bool = False
    ) -> str:
        seen: set = set()
        lines: List[str] = []
        deps: List[str] = []
        for tool in tools:
            spec = self._spec_for(tool)
            if not spec:
                continue
            if spec.get("bundle") and not include_custom:
                continue
            deps.extend(spec.get("deps", []))
        if has_mcp:
            # crewai_tools' MCPServerAdapter needs the `crewai-tools[mcp]` extra,
            # which is BOTH `mcp` AND `mcpadapt` (the adapter imports
            # `mcpadapt.core`). Shipping only `mcp` makes the adapter fail at
            # import with a misleading "missing the 'mcp' package" prompt and skip
            # every server. Pin `mcp` to the range crewai 1.14.5 requires; add
            # `mcpadapt` (matching Kasal's tested combo) so the adapter loads.
            deps.append('"mcp>=1.26.0,<1.27.0"')
            deps.append('"mcpadapt>=0.1.9,<0.2.0"')
        for dep in deps:
            if dep not in seen:
                seen.add(dep)
                lines.append(f"    {dep},")
        return ("\n".join(lines) + "\n") if lines else ""

    def _mcp_env_key(self, name: str) -> str:
        """Env var a third-party MCP server reads its bearer from (mirrors agent.py)."""
        slug = "".join(c if c.isalnum() else "_" for c in name.upper()).strip("_")
        return f"{slug}_MCP_TOKEN"

    def _env_keys(
        self, tools: List[str], mcp_servers: Optional[List[Dict[str, Any]]] = None
    ) -> str:
        keys: List[str] = []
        for tool in tools:
            spec = self._spec_for(tool)
            if not spec:
                continue
            for env in spec.get("env", []):
                if env not in keys:
                    keys.append(env)
        # Third-party (non-Databricks-managed) MCP servers need their own token.
        for server in mcp_servers or []:
            url = server.get("server_url") or ""
            if "/api/2.0/mcp/" in url:
                continue
            key = self._mcp_env_key(str(server.get("name", "mcp")))
            if key not in keys:
                keys.append(key)
        lines = [f"# {key}=" for key in keys]
        return (
            ("\n".join(lines) + "\n") if lines else "# (none required for this crew)\n"
        )

    def _ftype(self, rel_path: str) -> str:
        return _EXT_TYPE.get(Path(rel_path).suffix, "text")

    async def _a2ui_vendor_files(
        self, crew_data: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """Vendor the ONE shared A2UI composer + bake the workspace's resolved config.

        Ships ``agent_server/a2ui/`` = the portable composer (``__init__.py`` +
        ``compose.py``, copied verbatim from ``src.shared.a2ui``) plus two baked
        data files: ``catalog.json`` (the catalog the composer may use, resolved
        from this workspace's UIConfig) and ``config.json`` ({enabled, directives}).
        The deployed ``agent.py`` imports this module instead of carrying its own
        copy, so live Kasal chat and the exported app share one implementation.
        Resolution happens in CrewExportService via the shared resolvers; here we
        only fall back to the full bundled catalog for plain exports.
        """
        files: List[Dict[str, str]] = []
        for name in ("__init__.py", "compose.py"):
            src = SHARED_A2UI_DIR / name
            try:
                async with aiofiles.open(src, "r", encoding="utf-8") as f:
                    code = await f.read()
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(f"A2UI shared source missing: {src} ({exc})")
                continue
            files.append(
                {"path": f"agent_server/a2ui/{name}", "content": code, "type": "python"}
            )
        catalog = crew_data.get("a2ui_catalog")
        if not catalog:
            try:
                catalog = json.loads(
                    (SHARED_A2UI_DIR / "catalog.json").read_text(encoding="utf-8")
                )
            except Exception:  # noqa: BLE001
                catalog = {}
        config = {
            "enabled": bool(crew_data.get("a2ui_enabled", True)),
            "directives": crew_data.get("a2ui_directives") or {},
        }
        files.append(
            {
                "path": "agent_server/a2ui/catalog.json",
                "content": json.dumps(catalog, indent=2, ensure_ascii=False),
                "type": "json",
            }
        )
        files.append(
            {
                "path": "agent_server/a2ui/config.json",
                "content": json.dumps(config, indent=2, ensure_ascii=False),
                "type": "json",
            }
        )
        return files

    async def _a2ui_frontend_files(self) -> List[Dict[str, str]]:
        """Vendor the frontend A2UI renderer verbatim into the export.

        Copies the self-contained A2UI module (its own ``lib/`` + ``ui/``, relative
        imports only) to ``frontend/src/a2ui/`` so the deployed app renders surfaces
        with the SAME components Kasal chat uses. The source is the committed copy in
        the template tree (``SHARED_A2UI_FRONTEND_DIR``) — NOT the frontend source —
        so this works even when the export runs inside the DEPLOYED Kasal app, which
        ships the backend but not the frontend source tree. The copy is kept in sync
        with the canonical ``src/frontend/src/shared/a2ui`` by a test. Emitted
        VERBATIM (the token-substituting template walk skips this subtree).
        """
        files: List[Dict[str, str]] = []
        base = SHARED_A2UI_FRONTEND_DIR
        if not base.is_dir():
            self.logger.warning(f"Shared frontend A2UI module missing: {base}")
            return files
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            if ".test." in path.name or path.name in _SKIP_FILES:
                continue
            rel = path.relative_to(base).as_posix()
            try:
                async with aiofiles.open(path, "r", encoding="utf-8") as f:
                    content = await f.read()
            except (UnicodeDecodeError, ValueError):
                self.logger.warning(f"Skipping non-text A2UI frontend file: {rel}")
                continue
            files.append(
                {
                    "path": f"frontend/src/a2ui/{rel}",
                    "content": content,
                    "type": self._ftype(rel),
                }
            )
        return files

    async def _bundle_tool_files(self, tools: List[str]) -> List[Dict[str, str]]:
        """Emit self-contained impls for the bundled tools the crew uses."""
        files: List[Dict[str, str]] = []
        emitted: set = set()
        for tool in tools:
            spec = self._spec_for(tool)
            if not spec or not spec.get("bundle"):
                continue
            filename, source = spec["bundle"]
            if filename in emitted:
                continue
            base = BUNDLED_TOOLS_DIR if source == "bundled" else CUSTOM_TOOLS_DIR
            tool_path = base / filename
            if not tool_path.exists():
                self.logger.warning(f"Bundled tool source missing: {tool_path}")
                continue
            try:
                async with aiofiles.open(tool_path, "r", encoding="utf-8") as f:
                    code = await f.read()
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(f"Could not read bundled tool {filename}: {exc}")
                continue
            files.append(
                {"path": f"tools/{filename}", "content": code, "type": "python"}
            )
            emitted.add(filename)
        return files

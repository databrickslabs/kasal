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

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles

from .base_exporter import BaseExporter
from .yaml_generator import YAMLGenerator

TEMPLATE_DIR = Path(__file__).parent / "templates" / "databricks_app"

# crewai_tools we know how to import + instantiate directly.
_STANDARD_TOOLS = {
    "SerperDevTool",
    "ScrapeWebsiteTool",
    "DallETool",
    "FileReadTool",
    "DirectoryReadTool",
}

_TOOL_IMPORTS = {
    "SerperDevTool": "from crewai_tools import SerperDevTool",
    "ScrapeWebsiteTool": "from crewai_tools import ScrapeWebsiteTool",
    "DallETool": "from crewai_tools import DallETool",
    "FileReadTool": "from crewai_tools import FileReadTool",
    "DirectoryReadTool": "from crewai_tools import DirectoryReadTool",
}

_TOOL_MAP_ENTRIES = {
    "SerperDevTool": '"SerperDevTool": lambda: SerperDevTool(),',
    "ScrapeWebsiteTool": '"ScrapeWebsiteTool": lambda: ScrapeWebsiteTool(),',
    "DallETool": '"DallETool": lambda: DallETool(),',
    "FileReadTool": '"FileReadTool": lambda: FileReadTool(),',
    "DirectoryReadTool": '"DirectoryReadTool": lambda: DirectoryReadTool(),',
    "GenieTool": (
        '"GenieTool": lambda: GenieTool('
        'space_id=os.environ.get("GENIE_SPACE_ID", "")),'
    ),
}

# Extra pip deps a tool needs beyond the template's base set.
_TOOL_EXTRA_DEPS = {
    "PerplexityTool": ['"requests>=2.31.0"'],
    "ScrapeWebsiteTool": ['"beautifulsoup4>=4.12.0"'],
}

# Env vars a tool expects (surfaced as commented hints in .env.example).
_TOOL_ENV_KEYS = {
    "SerperDevTool": "SERPER_API_KEY",
    "PerplexityTool": "PERPLEXITY_API_KEY",
    "GenieTool": "GENIE_SPACE_ID",
}

# Custom tool name -> implementation file under crewai/tools/custom/.
_CUSTOM_TOOL_FILES = {
    "PerplexityTool": "perplexity_tool.py",
    "GenieTool": "genie_tool.py",
}

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

        custom_tools = [t for t in tools if t not in _STANDARD_TOOLS]
        has_custom = include_custom_tools and bool(custom_tools)

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
            "{{MODEL_OVERRIDE}}": repr(model_override),
            "{{ENABLE_OBO}}": "True" if include_obo else "False",
            "{{MCP_SERVERS}}": self._mcp_block(mcp_servers),
            "{{TOOL_IMPORTS}}": self._tool_imports(tools, has_custom),
            "{{TOOL_MAP}}": self._tool_map(tools),
            "{{EXTRA_DEPENDENCIES}}": self._extra_deps(
                tools, has_mcp=bool(mcp_servers)
            ),
            "{{ENV_TOOL_KEYS}}": self._env_keys(tools),
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
            "{{DATABRICKS_CATALOG}}": catalog,
            "{{DATABRICKS_SCHEMA}}": schema,
        }

        files: List[Dict[str, str]] = []

        # 1. Render the bundled template tree with token substitution.
        for path in sorted(TEMPLATE_DIR.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(TEMPLATE_DIR).as_posix()
            async with aiofiles.open(path, "r", encoding="utf-8") as f:
                content = await f.read()
            for token, value in tokens.items():
                content = content.replace(token, value)
            files.append({"path": rel, "content": content, "type": self._ftype(rel)})

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
                    tasks, agents, include_comments=include_comments
                ),
                "type": "yaml",
            }
        )

        # 3. Custom tool implementations (only when requested and present).
        if has_custom:
            tools_code = await self._generate_custom_tools(custom_tools)
            files.append({"path": "tools/__init__.py", "content": "", "type": "python"})
            files.append(
                {
                    "path": "tools/custom_tools.py",
                    "content": tools_code
                    or "# No bundled implementation found for the custom tools.\n",
                    "type": "python",
                }
            )

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

    def _mcp_block(self, mcp_servers: List[Dict[str, Any]]) -> str:
        lines = []
        for server in mcp_servers or []:
            name = str(server.get("name", "mcp")).replace('"', "'")
            url = server.get("server_url") or ""
            if "/api/2.0/mcp/" in url:
                url = "/api/2.0/mcp/" + url.split("/api/2.0/mcp/", 1)[1]
            lines.append(f'    ("{name}", "{url}"),')
        return ("\n".join(lines) + "\n") if lines else ""

    def _tool_imports(self, tools: List[str], has_custom: bool) -> str:
        imports: List[str] = []
        for tool in tools:
            stmt = _TOOL_IMPORTS.get(tool)
            if stmt and stmt not in imports:
                imports.append(stmt)
        if has_custom:
            imports.append("from tools.custom_tools import *  # noqa: F401,F403")
        return ("\n".join(imports) + "\n") if imports else ""

    def _tool_map(self, tools: List[str]) -> str:
        lines: List[str] = []
        for tool in tools:
            entry = _TOOL_MAP_ENTRIES.get(tool)
            if entry:
                lines.append("    " + entry)
            elif tool not in _STANDARD_TOOLS:
                lines.append(f'    "{tool}": lambda: {tool}(),')
        return ("\n".join(lines) + "\n") if lines else ""

    def _extra_deps(self, tools: List[str], has_mcp: bool = False) -> str:
        seen: set = set()
        lines: List[str] = []
        deps: List[str] = []
        for tool in tools:
            deps.extend(_TOOL_EXTRA_DEPS.get(tool, []))
        if has_mcp:
            # crewai_tools' MCPServerAdapter imports `mcp`; ship it so it never
            # tries to interactively prompt-install (which aborts in a server).
            deps.append('"mcp>=1.0.0"')
        for dep in deps:
            if dep not in seen:
                seen.add(dep)
                lines.append(f"    {dep},")
        return ("\n".join(lines) + "\n") if lines else ""

    def _env_keys(self, tools: List[str]) -> str:
        lines = [f"# {_TOOL_ENV_KEYS[t]}=" for t in tools if t in _TOOL_ENV_KEYS]
        return (
            ("\n".join(lines) + "\n") if lines else "# (none required for this crew)\n"
        )

    def _ftype(self, rel_path: str) -> str:
        return _EXT_TYPE.get(Path(rel_path).suffix, "text")

    async def _generate_custom_tools(self, custom_tools: List[str]) -> Optional[str]:
        """Read bundled custom tool implementations from crewai/tools/custom/."""
        tools_dir = Path(__file__).parent.parent / "tools" / "custom"
        parts: List[str] = []
        for tool_name in custom_tools:
            tool_file = _CUSTOM_TOOL_FILES.get(tool_name)
            if not tool_file:
                continue
            tool_path = tools_dir / tool_file
            if not tool_path.exists():
                continue
            try:
                async with aiofiles.open(tool_path, "r", encoding="utf-8") as f:
                    code = await f.read()
                parts.append(f"# {tool_name} implementation\n{code}")
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(f"Could not read tool {tool_file}: {exc}")
        return "\n\n".join(parts) if parts else None

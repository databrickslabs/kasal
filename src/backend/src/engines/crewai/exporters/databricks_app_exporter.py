"""
Databricks App exporter for CrewAI crews.

Generates a complete FastAPI application deployable to the Databricks Apps
platform via `databricks apps deploy`. The generated app follows service
pattern architecture, includes a simple frontend, and supports OBO
(on-behalf-of) authentication.
"""

from typing import Dict, Any, List, Optional
import logging
import aiofiles
from pathlib import Path

from .base_exporter import BaseExporter
from .yaml_generator import YAMLGenerator

logger = logging.getLogger(__name__)


class DatabricksAppExporter(BaseExporter):
    """Export crew as a Databricks App (FastAPI project in a zip)"""

    def __init__(self):
        super().__init__()
        self.yaml_generator = YAMLGenerator()

    async def export(
        self, crew_data: Dict[str, Any], options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Export crew as a Databricks App project.

        Args:
            crew_data: Crew configuration data
            options: Export options

        Returns:
            Dictionary with files list and metadata
        """
        crew_name = crew_data.get("name", "crew")
        sanitized_name = self._sanitize_name(crew_name)
        agents = crew_data.get("agents", [])
        tasks = crew_data.get("tasks", [])

        include_custom_tools = options.get("include_custom_tools", True)
        include_static = options.get("include_static_frontend", True)
        include_obo = options.get("include_obo_auth", True)
        include_tracing = options.get("include_tracing", True)
        model_override = options.get("model_override")

        tools = self._get_unique_tools(agents, tasks)

        # Generate YAML configs
        agents_yaml = self.yaml_generator.generate_agents_yaml(
            agents, model_override=model_override, include_comments=True
        )
        tasks_yaml = self.yaml_generator.generate_tasks_yaml(
            tasks, agents, include_comments=True
        )

        # Build file list
        files: List[Dict[str, str]] = []

        # Core files
        files.append(
            {
                "path": "app.yaml",
                "content": self._generate_app_yaml(),
                "type": "yaml",
            }
        )
        files.append(
            {
                "path": "app.py",
                "content": self._generate_app_py(sanitized_name, include_static),
                "type": "python",
            }
        )
        files.append(
            {
                "path": "requirements.txt",
                "content": self._generate_requirements(tools),
                "type": "text",
            }
        )
        files.append(
            {
                "path": ".env.example",
                "content": self._generate_env_example(tools),
                "type": "text",
            }
        )
        files.append(
            {
                "path": "README.md",
                "content": self._generate_readme(sanitized_name, agents, tasks),
                "type": "markdown",
            }
        )

        # Config
        files.append(
            {"path": "config/agents.yaml", "content": agents_yaml, "type": "yaml"}
        )
        files.append(
            {"path": "config/tasks.yaml", "content": tasks_yaml, "type": "yaml"}
        )

        # Services
        files.append(
            {"path": "services/__init__.py", "content": "", "type": "python"}
        )
        files.append(
            {
                "path": "services/crew_service.py",
                "content": self._generate_crew_service(
                    sanitized_name, agents, tasks, tools, model_override,
                    include_obo, include_tracing,
                ),
                "type": "python",
            }
        )

        # Routes
        files.append(
            {
                "path": "routes/__init__.py",
                "content": self._generate_routes_init(),
                "type": "python",
            }
        )
        files.append(
            {"path": "routes/v1/__init__.py",
             "content": self._generate_routes_v1_init(),
             "type": "python"}
        )
        files.append(
            {
                "path": "routes/v1/healthcheck.py",
                "content": self._generate_healthcheck_routes(sanitized_name),
                "type": "python",
            }
        )
        files.append(
            {
                "path": "routes/v1/crew.py",
                "content": self._generate_crew_routes(sanitized_name),
                "type": "python",
            }
        )

        # Models
        files.append(
            {"path": "models/__init__.py", "content": "", "type": "python"}
        )
        files.append(
            {
                "path": "models/crew.py",
                "content": self._generate_models_crew(),
                "type": "python",
            }
        )

        # Tools
        files.append(
            {"path": "tools/__init__.py", "content": "", "type": "python"}
        )
        if include_custom_tools and tools:
            custom_tools_content = await self._generate_custom_tools(tools)
            if custom_tools_content:
                files.append(
                    {
                        "path": "tools/custom_tools.py",
                        "content": custom_tools_content,
                        "type": "python",
                    }
                )

        # Static frontend
        if include_static:
            files.append(
                {
                    "path": "static/index.html",
                    "content": self._generate_static_html(crew_name),
                    "type": "html",
                }
            )
            files.append(
                {
                    "path": "static/styles.css",
                    "content": self._generate_static_css(),
                    "type": "css",
                }
            )
            files.append(
                {
                    "path": "static/app.js",
                    "content": self._generate_static_js(sanitized_name),
                    "type": "javascript",
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
                "sanitized_name": sanitized_name,
                "include_static_frontend": include_static,
                "include_obo_auth": include_obo,
            },
            "generated_at": self._get_timestamp(),
            "size_bytes": sum(len(f["content"]) for f in files),
        }

    # ── Template Methods ───────────────────────────────────────────────

    def _generate_app_yaml(self, port: int = 8000) -> str:
        return (
            f'command: ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "{port}"]\n'
        )

    def _generate_app_py(self, crew_name: str, include_static: bool) -> str:
        static_mount = ""
        static_import = ""
        if include_static:
            static_import = (
                "from fastapi.staticfiles import StaticFiles\n"
                "from fastapi.responses import FileResponse\n"
            )
            static_mount = '''
# Serve static frontend
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def serve_frontend():
    """Serve the frontend UI"""
    return FileResponse("static/index.html")
'''

        return f'''"""
{crew_name} - Databricks App

A FastAPI application wrapping a CrewAI crew, deployable to Databricks Apps.
"""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
{static_import}
from routes import api_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown lifecycle."""
    # Set up MLflow experiment for tracing
    try:
        import mlflow
        experiment_name = os.environ.get(
            "MLFLOW_EXPERIMENT_NAME", "/Shared/{crew_name}"
        )
        mlflow.set_experiment(experiment_name)
        print(f"MLflow experiment set to: {{experiment_name}}")
    except Exception:
        pass

    print("Starting {crew_name} app...")
    yield
    print("Shutting down {crew_name} app...")


app = FastAPI(
    title="{crew_name}",
    description="CrewAI crew deployed as a Databricks App",
    version="1.0.0",
    lifespan=lifespan,
)

# Include API routes (must be under /api for Databricks OAuth2)
app.include_router(api_router)
{static_mount}'''

    def _generate_requirements(self, tools: List[str]) -> str:
        # fastapi and uvicorn are pre-installed in Databricks Apps runtime
        lines = [
            "crewai",
            "crewai-tools",
            "pyyaml",
            "python-dotenv",
            "litellm",
            "mlflow",
            "chromadb",
        ]

        # Add tool-specific deps
        if "PerplexityTool" in tools:
            lines.append("requests")
        if "ScrapeWebsiteTool" in tools:
            lines.append("beautifulsoup4")
        if "GenieTool" in tools or "DatabricksQueryTool" in tools:
            lines.append("databricks-sdk")

        return "\n".join(lines) + "\n"

    def _generate_env_example(self, tools: List[str]) -> str:
        lines = [
            "# Databricks environment (auto-set by Databricks Apps runtime)",
            "# DATABRICKS_HOST=https://your-workspace.cloud.databricks.com",
            "# DATABRICKS_CLIENT_ID=",
            "# DATABRICKS_CLIENT_SECRET=",
            "",
            "# LLM model (optional override)",
            "# MODEL_OVERRIDE=databricks-llama-4-maverick",
            "",
            "# MLflow experiment name (optional - defaults to /Shared/<crew_name>)",
            "# MLFLOW_EXPERIMENT_NAME=/Shared/my_crew",
            "",
        ]
        if "SerperDevTool" in tools:
            lines.append("# SERPER_API_KEY=your-serper-key")
        if "PerplexityTool" in tools:
            lines.append("# PERPLEXITY_API_KEY=your-perplexity-key")
        if "GenieTool" in tools:
            lines.append("# GENIE_SPACE_ID=your-genie-space-id")

        return "\n".join(lines) + "\n"

    def _generate_crew_service(
        self,
        crew_name: str,
        agents: List[Dict[str, Any]],
        tasks: List[Dict[str, Any]],
        tools: List[str],
        model_override: Optional[str],
        include_obo: bool,
        include_tracing: bool = True,
    ) -> str:
        obo_param = ""
        obo_doc = ""
        obo_logic = ""
        if include_obo:
            obo_param = ", user_token: str | None = None"
            obo_doc = (
                '        user_token: Optional user OBO token from '
                'x-forwarded-access-token header.\n'
            )
            obo_logic = '''
        # If an OBO token is available, set it for the current request
        if user_token:
            os.environ["DATABRICKS_TOKEN"] = user_token
'''

        # Build tool imports
        standard_tools = {
            "SerperDevTool": "from crewai_tools import SerperDevTool",
            "ScrapeWebsiteTool": "from crewai_tools import ScrapeWebsiteTool",
            "DallETool": "from crewai_tools import DallETool",
            "FileReadTool": "from crewai_tools import FileReadTool",
            "DirectoryReadTool": "from crewai_tools import DirectoryReadTool",
        }
        tool_imports = []
        for t in tools:
            if t in standard_tools:
                tool_imports.append(standard_tools[t])

        custom_tools = [
            t for t in tools
            if t not in standard_tools
        ]
        if custom_tools:
            tool_imports.append("from tools.custom_tools import *  # noqa: F403")

        tool_import_block = "\n".join(tool_imports) + "\n" if tool_imports else ""

        # MLflow init block (conditional)
        mlflow_init = ""
        if include_tracing:
            mlflow_init = '''
        # Enable MLflow tracing for CrewAI
        try:
            import mlflow
            mlflow.crewai.autolog()
        except Exception:
            pass
'''

        # Tool map for instantiation
        tool_map_entries = {
            "SerperDevTool": '"SerperDevTool": lambda: SerperDevTool()',
            "ScrapeWebsiteTool": '"ScrapeWebsiteTool": lambda: ScrapeWebsiteTool()',
            "DallETool": '"DallETool": lambda: DallETool()',
            "FileReadTool": '"FileReadTool": lambda: FileReadTool()',
            "DirectoryReadTool": '"DirectoryReadTool": lambda: DirectoryReadTool()',
            "GenieTool": '"GenieTool": lambda: GenieTool(space_id=os.environ.get("GENIE_SPACE_ID", ""))',
        }

        tool_map_lines = []
        for t in tools:
            if t in tool_map_entries:
                tool_map_lines.append(f"        {tool_map_entries[t]},")
            elif t not in standard_tools:
                # Custom tools - use class name directly
                tool_map_lines.append(f'        "{t}": lambda: {t}(),')

        tool_map_block = "\n".join(tool_map_lines) if tool_map_lines else ""

        return f'''"""
Crew service - builds and runs the CrewAI crew.
"""

import os
import logging
from pathlib import Path
from typing import Any, Dict, List

import yaml
from crewai import Agent, Crew, Task, Process, LLM

{tool_import_block}

CONFIG_DIR = Path(__file__).parent.parent / "config"
logger = logging.getLogger(__name__)

# Mapping of tool names to their instantiation functions
TOOL_MAP = {{
{tool_map_block}
}}


class CrewService:
    """Builds and executes the crew from YAML configuration."""

    def __init__(self):
        self._agents_config = self._load_yaml("agents.yaml")
        self._tasks_config = self._load_yaml("tasks.yaml")
{mlflow_init}
    @staticmethod
    def _load_yaml(filename: str) -> Dict[str, Any]:
        path = CONFIG_DIR / filename
        with open(path, "r") as f:
            return yaml.safe_load(f)

    @staticmethod
    def _build_tools(tool_names: List[str]) -> list:
        """Instantiate tools from a list of tool names using TOOL_MAP."""
        tools = []
        for name in tool_names:
            factory = TOOL_MAP.get(name)
            if factory:
                try:
                    tools.append(factory())
                except Exception as exc:
                    logger.warning("Could not instantiate tool %s: %s", name, exc)
            else:
                logger.warning("Unknown tool: %s - skipping", name)
        return tools

    def get_info(self) -> Dict[str, Any]:
        """Return crew metadata."""
        return {{
            "name": "{crew_name}",
            "agents": list(self._agents_config.keys()),
            "tasks": list(self._tasks_config.keys()),
            "tools": list(TOOL_MAP.keys()),
            "agent_count": len(self._agents_config),
            "task_count": len(self._tasks_config),
        }}

    def execute(self, inputs: Dict[str, Any]{obo_param}) -> Dict[str, Any]:
        """
        Build the crew from config and kick it off.

        Args:
            inputs: Runtime inputs for the crew (e.g. {{"topic": "AI trends"}}).
{obo_doc}
        Returns:
            Execution result with output and task details.
        """
{obo_logic}
        agents = self._build_agents()
        tasks = self._build_tasks(agents)

        crew = Crew(
            agents=list(agents.values()),
            tasks=tasks,
            process=Process.sequential,
            memory=True,
            verbose=True,
        )

        result = crew.kickoff(inputs=inputs)

        return {{
            "result": str(result),
            "task_outputs": [
                {{"task": t.description[:80], "output": str(t.output)}}
                for t in crew.tasks
                if t.output
            ],
        }}

    def _build_agents(self) -> Dict[str, Agent]:
        agents: Dict[str, Agent] = {{}}
        model_override = os.environ.get("MODEL_OVERRIDE")

        for name, cfg in self._agents_config.items():
            llm_model = model_override or cfg.get("llm", "databricks-llama-4-maverick")
            if not llm_model.startswith("databricks/"):
                llm_model = f"databricks/{{llm_model}}"

            llm = LLM(model=llm_model, temperature=cfg.get("temperature", 0.7))

            # Build tools for this agent
            agent_tools = self._build_tools(cfg.get("tools", []))

            agents[name] = Agent(
                role=cfg["role"],
                goal=cfg["goal"],
                backstory=cfg["backstory"],
                llm=llm,
                tools=agent_tools,
                verbose=cfg.get("verbose", True),
                allow_delegation=cfg.get("allow_delegation", False),
                max_iter=cfg.get("max_iter", 25),
            )
        return agents

    def _build_tasks(self, agents: Dict[str, Agent]) -> list:
        tasks = []
        agent_names = list(agents.keys())

        for name, cfg in self._tasks_config.items():
            agent_key = cfg.get("agent", agent_names[0] if agent_names else None)
            agent = agents.get(agent_key)
            if agent is None and agents:
                agent = list(agents.values())[0]

            # Build tools for this task
            task_tools = self._build_tools(cfg.get("tools", []))

            tasks.append(
                Task(
                    description=cfg["description"],
                    expected_output=cfg["expected_output"],
                    agent=agent,
                    tools=task_tools,
                )
            )
        return tasks
'''

    def _generate_crew_routes(self, crew_name: str) -> str:
        return f'''"""
Crew execution routes.
"""

from fastapi import APIRouter, Header, HTTPException
from typing import Optional

from models.crew import CrewExecuteRequest, CrewExecuteResponse, CrewInfoResponse
from services.crew_service import CrewService

router = APIRouter(prefix="/crew", tags=["crew"])

crew_service = CrewService()


@router.get("/info", response_model=CrewInfoResponse)
async def get_crew_info():
    """Return metadata about the configured crew."""
    return crew_service.get_info()


@router.post("/execute", response_model=CrewExecuteResponse)
async def execute_crew(
    request: CrewExecuteRequest,
    x_forwarded_access_token: Optional[str] = Header(None),
):
    """
    Execute the crew with the provided inputs.

    The user OBO token is automatically extracted from the
    x-forwarded-access-token header injected by Databricks Apps.
    """
    try:
        result = crew_service.execute(
            inputs=request.inputs,
            user_token=x_forwarded_access_token,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
'''

    def _generate_healthcheck_routes(self, crew_name: str) -> str:
        return f'''"""
Health check routes.
"""

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check():
    """Basic health check endpoint."""
    return {{"status": "healthy", "app": "{crew_name}"}}
'''

    def _generate_routes_init(self) -> str:
        return '''"""
Route assembly - all routes are served under /api prefix.
"""

from fastapi import APIRouter
from routes.v1 import v1_router

api_router = APIRouter(prefix="/api")
api_router.include_router(v1_router)
'''

    def _generate_routes_v1_init(self) -> str:
        return '''"""
V1 route assembly.
"""

from fastapi import APIRouter
from routes.v1.healthcheck import router as healthcheck_router
from routes.v1.crew import router as crew_router

v1_router = APIRouter(prefix="/v1")
v1_router.include_router(healthcheck_router)
v1_router.include_router(crew_router)
'''

    def _generate_models_crew(self) -> str:
        return '''"""
Pydantic models for crew requests and responses.
"""

from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional


class CrewExecuteRequest(BaseModel):
    """Request body for crew execution."""

    inputs: Dict[str, Any] = Field(
        default_factory=dict,
        description="Runtime inputs for the crew (e.g. {\'topic\': \'AI trends\'})",
    )


class TaskOutput(BaseModel):
    """Individual task output."""

    task: str
    output: str


class CrewExecuteResponse(BaseModel):
    """Response from crew execution."""

    result: str
    task_outputs: Optional[List[TaskOutput]] = None


class CrewInfoResponse(BaseModel):
    """Metadata about the configured crew."""

    name: str
    agents: List[str]
    tasks: List[str]
    tools: List[str] = []
    agent_count: int
    task_count: int
'''

    async def _generate_custom_tools(self, tools: List[str]) -> Optional[str]:
        """Read custom tool implementations from disk."""
        standard_tools = {
            "SerperDevTool", "ScrapeWebsiteTool", "DallETool",
            "FileReadTool", "DirectoryReadTool",
        }
        custom_tools = [t for t in tools if t not in standard_tools]
        if not custom_tools:
            return None

        backend_path = Path(__file__).parent.parent  # crewai directory
        tools_dir = backend_path / "tools" / "custom"

        tool_file_mapping = {
            "PerplexityTool": "perplexity_tool.py",
            "GenieTool": "genie_tool.py",
        }

        parts: List[str] = []
        for tool_name in custom_tools:
            tool_file = tool_file_mapping.get(tool_name)
            if tool_file:
                tool_path = tools_dir / tool_file
                if tool_path.exists():
                    try:
                        async with aiofiles.open(tool_path, "r") as f:
                            code = await f.read()
                            parts.append(f"# {tool_name} Implementation\n{code}")
                    except Exception as e:
                        logger.warning(f"Could not read tool {tool_file}: {e}")

        return "\n\n".join(parts) if parts else None

    # ── Static Frontend ────────────────────────────────────────────────

    def _generate_static_html(self, crew_name: str) -> str:
        display_name = crew_name.replace("_", " ").title()
        return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{display_name}</title>
    <link rel="stylesheet" href="/static/styles.css">
</head>
<body>
    <div class="container">
        <header>
            <h1>{display_name}</h1>
            <p class="subtitle">CrewAI Agent Crew</p>
        </header>

        <section id="info-section">
            <h2>Crew Info</h2>
            <div id="crew-info"><em>Loading...</em></div>
        </section>

        <section id="execute-section">
            <h2>Execute Crew</h2>
            <form id="execute-form">
                <label for="topic-input">Topic / Input:</label>
                <input
                    type="text"
                    id="topic-input"
                    placeholder="e.g. AI trends in 2025"
                    required
                />
                <button type="submit" id="run-btn">Run Crew</button>
            </form>
            <div id="status" class="hidden"></div>
        </section>

        <section id="result-section" class="hidden">
            <h2>Result</h2>
            <pre id="result-output"></pre>
            <div id="task-outputs"></div>
        </section>
    </div>

    <script src="/static/app.js"></script>
</body>
</html>
'''

    def _generate_static_css(self) -> str:
        return '''*,
*::before,
*::after {
    box-sizing: border-box;
    margin: 0;
    padding: 0;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen,
        Ubuntu, Cantarell, sans-serif;
    line-height: 1.6;
    color: #1a1a2e;
    background: #f0f2f5;
}

.container {
    max-width: 800px;
    margin: 2rem auto;
    padding: 0 1rem;
}

header {
    text-align: center;
    margin-bottom: 2rem;
}

header h1 {
    font-size: 1.8rem;
    color: #16213e;
}

.subtitle {
    color: #6c757d;
    font-size: 0.95rem;
}

section {
    background: #fff;
    border-radius: 8px;
    padding: 1.5rem;
    margin-bottom: 1.5rem;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
}

section h2 {
    font-size: 1.1rem;
    margin-bottom: 1rem;
    color: #16213e;
}

label {
    display: block;
    margin-bottom: 0.35rem;
    font-weight: 500;
    font-size: 0.9rem;
}

input[type="text"] {
    width: 100%;
    padding: 0.6rem 0.8rem;
    border: 1px solid #ced4da;
    border-radius: 6px;
    font-size: 0.95rem;
    margin-bottom: 1rem;
}

input[type="text"]:focus {
    outline: none;
    border-color: #4361ee;
    box-shadow: 0 0 0 3px rgba(67, 97, 238, 0.15);
}

button {
    background: #4361ee;
    color: #fff;
    border: none;
    padding: 0.6rem 1.4rem;
    border-radius: 6px;
    font-size: 0.95rem;
    cursor: pointer;
    transition: background 0.2s;
}

button:hover {
    background: #3a56d4;
}

button:disabled {
    background: #adb5bd;
    cursor: not-allowed;
}

#status {
    margin-top: 1rem;
    padding: 0.6rem 1rem;
    border-radius: 6px;
    font-size: 0.9rem;
}

#status.running {
    background: #e8f4fd;
    color: #0c5460;
}

#status.error {
    background: #f8d7da;
    color: #721c24;
}

#status.success {
    background: #d4edda;
    color: #155724;
}

.hidden {
    display: none;
}

pre {
    background: #f8f9fa;
    padding: 1rem;
    border-radius: 6px;
    overflow-x: auto;
    white-space: pre-wrap;
    word-wrap: break-word;
    font-size: 0.85rem;
    line-height: 1.5;
}

#task-outputs {
    margin-top: 1rem;
}

.task-card {
    background: #f8f9fa;
    border-left: 3px solid #4361ee;
    padding: 0.8rem 1rem;
    margin-bottom: 0.75rem;
    border-radius: 0 6px 6px 0;
}

.task-card h4 {
    font-size: 0.85rem;
    color: #495057;
    margin-bottom: 0.3rem;
}

.task-card p {
    font-size: 0.85rem;
    color: #212529;
}

#crew-info ul {
    list-style: none;
    padding: 0;
}

#crew-info li {
    padding: 0.25rem 0;
    font-size: 0.9rem;
}
'''

    def _generate_static_js(self, crew_name: str) -> str:
        return '''(function () {
    "use strict";

    const API_BASE = "/api/v1";

    async function loadCrewInfo() {
        try {
            const res = await fetch(`${API_BASE}/crew/info`);
            if (!res.ok) throw new Error(res.statusText);
            const data = await res.json();
            const el = document.getElementById("crew-info");
            el.innerHTML = `
                <ul>
                    <li><strong>Name:</strong> ${data.name}</li>
                    <li><strong>Agents:</strong> ${data.agents.join(", ")}</li>
                    <li><strong>Tasks:</strong> ${data.tasks.join(", ")}</li>
                </ul>
            `;
        } catch (err) {
            document.getElementById("crew-info").textContent =
                "Could not load crew info: " + err.message;
        }
    }

    function setStatus(msg, cls) {
        const el = document.getElementById("status");
        el.textContent = msg;
        el.className = cls;
    }

    document.getElementById("execute-form").addEventListener("submit", async (e) => {
        e.preventDefault();
        const topic = document.getElementById("topic-input").value.trim();
        if (!topic) return;

        const btn = document.getElementById("run-btn");
        btn.disabled = true;
        setStatus("Running crew... this may take a few minutes.", "running");
        document.getElementById("result-section").classList.add("hidden");

        try {
            const res = await fetch(`${API_BASE}/crew/execute`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ inputs: { topic } }),
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({}));
                throw new Error(err.detail || res.statusText);
            }
            const data = await res.json();

            document.getElementById("result-output").textContent = data.result;

            const taskContainer = document.getElementById("task-outputs");
            taskContainer.innerHTML = "";
            if (data.task_outputs && data.task_outputs.length) {
                data.task_outputs.forEach((t) => {
                    const card = document.createElement("div");
                    card.className = "task-card";
                    card.innerHTML = `<h4>${t.task}</h4><p>${t.output}</p>`;
                    taskContainer.appendChild(card);
                });
            }

            document.getElementById("result-section").classList.remove("hidden");
            setStatus("Crew finished successfully.", "success");
        } catch (err) {
            setStatus("Error: " + err.message, "error");
        } finally {
            btn.disabled = false;
        }
    });

    loadCrewInfo();
})();
'''

    def _generate_readme(
        self,
        crew_name: str,
        agents: List[Dict[str, Any]],
        tasks: List[Dict[str, Any]],
    ) -> str:
        display_name = crew_name.replace("_", " ").title()
        agent_list = "\n".join(
            f"- **{a.get('name', 'Agent')}** - {a.get('role', 'N/A')}"
            for a in agents
        )
        task_list = "\n".join(
            f"- **{t.get('name', 'Task')}** - {t.get('description', 'N/A')[:80]}"
            for t in tasks
        )

        return f'''# {display_name}

A CrewAI crew deployed as a Databricks App (FastAPI).

## Crew Details

### Agents
{agent_list}

### Tasks
{task_list}

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Also install FastAPI and Uvicorn locally (pre-installed on Databricks Apps)
pip install fastapi uvicorn

# Run the app
uvicorn app:app --reload
```

Open http://localhost:8000 in your browser to access the frontend.

## Deploy to Databricks Apps

```bash
# Authenticate with Databricks CLI
databricks auth login --host https://your-workspace.cloud.databricks.com

# Sync files to workspace
databricks sync . /Workspace/Users/your-email/{crew_name}_app

# Create the app (first time only)
databricks apps create {crew_name.replace("_", "-")}-app

# Deploy
databricks apps deploy {crew_name.replace("_", "-")}-app \\
    --source-code-path /Workspace/Users/your-email/{crew_name}_app
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/health` | Health check |
| GET | `/api/v1/crew/info` | Crew metadata |
| POST | `/api/v1/crew/execute` | Execute the crew |

### Execute Request

```json
{{
    "inputs": {{
        "topic": "AI trends in 2025"
    }}
}}
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABRICKS_HOST` | Workspace URL (auto-set by Databricks Apps) |
| `DATABRICKS_CLIENT_ID` | Service principal ID (auto-set) |
| `DATABRICKS_CLIENT_SECRET` | Service principal secret (auto-set) |
| `MODEL_OVERRIDE` | Optional LLM model override |

## Generated by Kasal
'''

"""
Python code generator for CrewAI crew.py and main.py files.
"""

from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)


class CodeGenerator:
    """Generate Python code for CrewAI crews"""

    @staticmethod
    def _is_codex_model(model_name: Optional[str]) -> bool:
        """Whether a model requires the Databricks Responses API (gpt-5-3-codex).

        Mirrors the runtime check in ``llm_manager.configure_crewai_llm`` so the
        exported notebook routes these models the same way Kasal does.
        """
        return bool(model_name) and "gpt-5-3-codex" in model_name.lower()

    @staticmethod
    def _generate_codex_llm_helper() -> str:
        """Emit a notebook helper that builds Responses-API LLMs for codex models.

        gpt-5-3-codex on Databricks only works through the OpenAI Responses API,
        served under a different base path than chat completions:
          - AI Gateway on:  <host>/ai-gateway/openai/v1
          - AI Gateway off: <host>/serving-endpoints   (default)
        The OpenAI client appends ``/responses`` to that base.
        """
        return (
            "# gpt-5-3-codex ONLY supports the Databricks Responses API; the default\n"
            "# Chat Completions route returns 404 \"Supervisor API is not enabled\".\n"
            "from crewai.llms.providers.openai.completion import OpenAICompletion\n"
            "\n"
            "def make_codex_llm(model_name):\n"
            "    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()\n"
            "    host = ctx.apiUrl().getOrElse(None).rstrip(\"/\")\n"
            "    token = ctx.apiToken().getOrElse(None)\n"
            "    # AI Gateway on -> /ai-gateway/openai/v1 ; off -> /serving-endpoints (default)\n"
            "    gateway_on = os.environ.get(\"DATABRICKS_AI_GATEWAY_ENABLED\", \"false\").lower() in (\"1\", \"true\", \"yes\")\n"
            "    base_path = \"ai-gateway/openai/v1\" if gateway_on else \"serving-endpoints\"\n"
            "    return OpenAICompletion(\n"
            "        model=model_name,\n"
            "        api=\"responses\",\n"
            "        base_url=f\"{host}/{base_path}\",\n"
            "        api_key=token,\n"
            "        timeout=300,\n"
            "    )\n"
            "\n"
        )

    @staticmethod
    def _generate_mcp_server_params(mcp_servers: List[Dict[str, Any]]) -> str:
        """Emit a ``mcp_server_params`` list from the crew's configured MCP servers.

        Databricks-managed MCP servers (URL contains ``/api/2.0/mcp/``) are made
        portable by resolving host + token from the notebook runtime context and
        keeping only the URL path. Third-party MCP servers keep their full URL and
        read their bearer token from an environment variable the user must set.
        """
        import re

        lines = ["# MCP servers configured on this crew (auto-attached at runtime).\n"]
        needs_databricks = any(
            "/api/2.0/mcp/" in (s.get("server_url") or "") for s in mcp_servers
        )
        if needs_databricks:
            lines.append('_mcp_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()\n')
            lines.append('_mcp_host = _mcp_ctx.apiUrl().getOrElse(None).rstrip("/")\n')
            lines.append('_mcp_token = _mcp_ctx.apiToken().getOrElse(None)\n')

        lines.append("mcp_server_params = [\n")
        for server in mcp_servers:
            url = server.get("server_url") or ""
            name = server.get("name", "mcp")
            if "/api/2.0/mcp/" in url:
                # Databricks-managed MCP is served over streamable-HTTP, not SSE
                # (the mcpadapt default) — without this the adapter connects via
                # SSE and times out after 30s.
                path = "/api/2.0/mcp/" + url.split("/api/2.0/mcp/", 1)[1]
                lines.append("    {\n")
                lines.append(f'        "url": f"{{_mcp_host}}{path}",  # {name}\n')
                lines.append('        "transport": "streamable-http",\n')
                lines.append('        "headers": {"Authorization": f"Bearer {_mcp_token}"},\n')
                lines.append("    },\n")
            else:
                # Map the stored server_type to an mcpadapt transport.
                transport = "streamable-http" if server.get("server_type") == "streamable" else "sse"
                env_key = re.sub(r"[^A-Z0-9]+", "_", name.upper()).strip("_") + "_MCP_TOKEN"
                lines.append("    {\n")
                lines.append(f'        "url": "{url}",  # {name}\n')
                lines.append(f'        "transport": "{transport}",\n')
                lines.append(
                    f'        "headers": {{"Authorization": f"Bearer {{os.environ.get(\'{env_key}\', \'\')}}"}},  # TODO: set {env_key}\n'
                )
                lines.append("    },\n")
        lines.append("]\n")
        lines.append('print(f"MCP configured: {len(mcp_server_params)} server(s)")\n')
        lines.append("\n")
        return "".join(lines)

    def generate_crew_code(
        self,
        crew_name: str,
        agents: List[Dict[str, Any]],
        tasks: List[Dict[str, Any]],
        tools: List[str],
        process_type: str = "sequential",
        include_comments: bool = True,
        for_notebook: bool = False,
        mcp_servers: Optional[List[Dict[str, Any]]] = None
    ) -> str:
        """Generate crew code - uses direct instantiation for notebooks, class-based for standalone"""
        if for_notebook:
            return self._generate_notebook_crew_code(crew_name, agents, tasks, process_type, include_comments, mcp_servers)
        else:
            return self._generate_class_based_crew_code(crew_name, agents, tasks, tools, process_type, include_comments)

    def _generate_class_based_crew_code(
        self,
        crew_name: str,
        agents: List[Dict[str, Any]],
        tasks: List[Dict[str, Any]],
        tools: List[str],
        process_type: str = "sequential",
        include_comments: bool = True,
        for_notebook: bool = False
    ) -> str:
        """
        Generate crew.py content

        Args:
            crew_name: Name of the crew
            agents: List of agent configurations
            tasks: List of task configurations
            tools: List of tool names used
            process_type: Process type (sequential, hierarchical, etc.)
            include_comments: Whether to include explanatory comments
            for_notebook: Whether this is for a notebook (affects imports)

        Returns:
            Python code for crew definition
        """
        # Sanitize crew name for class name
        class_name = ''.join(word.capitalize() for word in crew_name.split('_'))
        if not class_name.endswith('Crew'):
            class_name += 'Crew'

        # Generate imports
        imports = self._generate_crew_imports(tools, for_notebook)

        # Generate agent methods
        agent_methods = []
        for agent in agents:
            agent_name = agent.get('name', 'agent').lower().replace(' ', '_')
            agent_tools = agent.get('tools', [])
            method_code = self._generate_agent_method(agent_name, agent_tools, include_comments, for_notebook)
            agent_methods.append(method_code)

        # Generate task methods
        task_methods = []
        for task in tasks:
            task_name = task.get('name', 'task').lower().replace(' ', '_')
            method_code = self._generate_task_method(task_name, include_comments, for_notebook)
            task_methods.append(method_code)

        # Generate crew method
        process_map = {
            'sequential': 'Process.sequential',
            'hierarchical': 'Process.hierarchical',
        }
        process = process_map.get(process_type, 'Process.sequential')
        crew_method = self._generate_crew_method(process, include_comments)

        # Assemble the code
        code_parts = []

        # Header comment
        if include_comments:
            header = (
                '"""\n'
                f'{crew_name.replace("_", " ").title()} - CrewAI Implementation\n'
                '"""\n\n'
            )
            code_parts.append(header)

        # Imports
        code_parts.append(imports)
        code_parts.append('\n\n')

        # Class definition - skip @CrewBase decorator in notebook mode
        if for_notebook:
            class_definition = f'class {class_name}:\n'
        else:
            class_definition = f'@CrewBase\nclass {class_name}:\n'

        if include_comments:
            class_definition += f'    """{crew_name.replace("_", " ").title()} for task execution"""\n\n'
        else:
            class_definition += '\n'

        # Config paths (only for non-notebook mode)
        if not for_notebook:
            class_definition += "    agents_config = 'config/agents.yaml'\n"
            class_definition += "    tasks_config = 'config/tasks.yaml'\n\n"
        else:
            # For notebooks, use __init__ to set instance attributes from outer scope
            class_definition += "    \n"
            class_definition += "    def __init__(self):\n"
            class_definition += "        # Set config attributes from outer scope\n"
            class_definition += "        self.agents_config = agents_config\n"
            class_definition += "        self.tasks_config = tasks_config\n\n"

        code_parts.append(class_definition)

        # Agent methods
        for i, method in enumerate(agent_methods):
            code_parts.append(method)
            code_parts.append('\n')

        # Task methods
        for method in task_methods:
            code_parts.append(method)
            code_parts.append('\n')

        # Crew method
        code_parts.append(crew_method)

        # Final print statement for notebook mode
        if for_notebook:
            code_parts.append(f'\n\nprint("{class_name} class defined")\n')

        return ''.join(code_parts)

    def generate_main_code(
        self,
        crew_name: str,
        sample_inputs: Optional[Dict[str, Any]] = None,
        include_comments: bool = True,
        for_notebook: bool = False,
        include_tracing: bool = True,
        mcp_servers: Optional[List[Dict[str, Any]]] = None
    ) -> str:
        """
        Generate main.py content

        Args:
            crew_name: Name of the crew
            sample_inputs: Sample input parameters
            include_comments: Whether to include explanatory comments
            for_notebook: Whether this is for a notebook
            include_tracing: Whether to include MLflow tracing (for_notebook only)

        Returns:
            Python code for main execution
        """
        # Sanitize crew name for class name
        class_name = ''.join(word.capitalize() for word in crew_name.split('_'))
        if not class_name.endswith('Crew'):
            class_name += 'Crew'

        # Default sample inputs
        if not sample_inputs:
            sample_inputs = {'topic': 'Artificial Intelligence trends in 2025'}

        code_parts = []

        if for_notebook:
            # Notebook execution code
            if include_comments:
                code_parts.append('"""\nExecute the Crew\n"""\n\n')

            # Loop-safe kickoff: Databricks notebooks run inside an already-running
            # asyncio event loop, so a plain crew.kickoff() raises "invoked
            # synchronously from within a running event loop". Detect that and use
            # the async API via nest_asyncio; otherwise fall back to sync kickoff.
            code_parts.append('import asyncio\n\n')
            code_parts.append('def _kickoff(crew_obj, inputs):\n')
            code_parts.append('    """Run a crew, tolerating an already-running notebook event loop."""\n')
            code_parts.append('    try:\n')
            code_parts.append('        loop = asyncio.get_running_loop()\n')
            code_parts.append('    except RuntimeError:\n')
            code_parts.append('        loop = None\n')
            code_parts.append('    if loop is not None:\n')
            code_parts.append('        import nest_asyncio\n')
            code_parts.append('        nest_asyncio.apply()\n')
            code_parts.append('        return loop.run_until_complete(crew_obj.kickoff_async(inputs=inputs))\n')
            code_parts.append('    return crew_obj.kickoff(inputs=inputs)\n\n')

            # Function definition
            code_parts.append('def run_crew(**inputs):\n')
            code_parts.append('    """\n')
            code_parts.append('    Run the crew with specified inputs\n')
            code_parts.append('    \n')
            code_parts.append('    Args:\n')
            code_parts.append('        **inputs: Input parameters for the crew\n')
            code_parts.append('    \n')
            code_parts.append('    Returns:\n')
            code_parts.append('        Crew execution result\n')
            code_parts.append('    """\n')
            code_parts.append('    \n')
            code_parts.append('    # Print execution header\n')
            code_parts.append('    print("=" * 70)\n')
            code_parts.append(f'    print("{crew_name.replace("_", " ").upper()} - STARTING EXECUTION")\n')
            code_parts.append('    print("=" * 70)\n')
            code_parts.append('    for key, value in inputs.items():\n')
            code_parts.append('        print(f"{key}: {value}")\n')
            code_parts.append("    print(f\"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\")\n")
            code_parts.append('    print()\n')
            code_parts.append('    \n')
            code_parts.append('    try:\n')

            mcp_servers = mcp_servers or []
            has_mcp = bool(mcp_servers)

            # Build the execution body (tracking + kickoff) at base indent 0.
            # `active_crew` is the crew to run — either the prebuilt `crew` or one
            # created with MCP tools injected via create_crew(mcp_tools=...).
            exec_lines = []
            if include_tracing:
                exec_lines.append('# Execute crew within MLflow run for tracking\n')
                exec_lines.append('active_run = mlflow.active_run()\n')
                exec_lines.append('if active_run:\n')
                exec_lines.append('    print(f"Using existing MLflow run: {active_run.info.run_id}")\n')
                exec_lines.append('    mlflow.log_params(inputs)\n')
                exec_lines.append('    result = _kickoff(active_crew, inputs)\n')
                exec_lines.append('    mlflow.log_text(str(result), "crew_output.txt")\n')
                exec_lines.append('    run_id = active_run.info.run_id\n')
                exec_lines.append('else:\n')
                exec_lines.append('    with mlflow.start_run() as run:\n')
                exec_lines.append('        mlflow.log_params(inputs)\n')
                exec_lines.append('        result = _kickoff(active_crew, inputs)\n')
                exec_lines.append('        mlflow.log_text(str(result), "crew_output.txt")\n')
                exec_lines.append('        run_id = run.info.run_id\n')
                exec_lines.append('print(f"\\nMLflow Run ID: {run_id}")\n')
            else:
                exec_lines.append('result = _kickoff(active_crew, inputs)\n')

            if has_mcp:
                # Keep MCP tools alive for the whole kickoff via the adapter context.
                code_parts.append('        print("Executing crew tasks with MCP tools...")\n')
                code_parts.append('        with MCPServerAdapter(mcp_server_params) as mcp_tools:\n')
                code_parts.append('            print(f"MCP: {len(mcp_tools)} tool(s) available")\n')
                code_parts.append('            active_crew = create_crew(mcp_tools=mcp_tools)\n')
                for line in exec_lines:
                    code_parts.append(('            ' + line) if line.strip() else line)
            else:
                code_parts.append('        print("Executing crew tasks...")\n')
                code_parts.append('        active_crew = crew\n')
                for line in exec_lines:
                    code_parts.append(('        ' + line) if line.strip() else line)

            code_parts.append('        \n')
            code_parts.append('        # Print results\n')
            code_parts.append('        print()\n')
            code_parts.append('        print("=" * 70)\n')
            code_parts.append('        print("EXECUTION COMPLETED SUCCESSFULLY")\n')
            code_parts.append('        print("=" * 70)\n')
            code_parts.append("        print(f\"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\")\n")
            code_parts.append('        print()\n')
            code_parts.append('        print("RESULT:")\n')
            code_parts.append('        print("-" * 70)\n')
            code_parts.append('        print(result)\n')
            code_parts.append('        print("-" * 70)\n')
            code_parts.append('        \n')
            code_parts.append('        return result\n')
            code_parts.append('        \n')
            code_parts.append('    except Exception as e:\n')
            code_parts.append('        print()\n')
            code_parts.append('        print("=" * 70)\n')
            code_parts.append('        print("EXECUTION FAILED")\n')
            code_parts.append('        print("=" * 70)\n')
            code_parts.append('        print(f"Error: {str(e)}")\n')
            code_parts.append('        raise\n\n')

            # Sample execution
            code_parts.append('# Execute with sample inputs (modify as needed)\n')
            inputs_str = ', '.join(f'{k}="{v}"' for k, v in sample_inputs.items())
            code_parts.append(f'result = run_crew({inputs_str})\n')

        else:
            # Standalone main.py code
            if include_comments:
                code_parts.append('#!/usr/bin/env python\n')
                code_parts.append('"""\n')
                code_parts.append(f'{crew_name.replace("_", " ").title()} - Main Entry Point\n')
                code_parts.append('"""\n\n')

            # Imports
            code_parts.append('import os\n')
            code_parts.append('from pathlib import Path\n')
            code_parts.append('from dotenv import load_dotenv\n')
            code_parts.append(f'from {crew_name}.crew import {class_name}\n\n')

            # Main function
            code_parts.append('def main():\n')
            code_parts.append(f'    """{crew_name.replace("_", " ").title()} execution"""\n')
            code_parts.append('    \n')
            code_parts.append('    # Load environment variables\n')
            code_parts.append('    load_dotenv()\n')
            code_parts.append('    \n')
            code_parts.append('    # Define inputs\n')
            code_parts.append(f'    inputs = {{\n')
            for key, value in sample_inputs.items():
                code_parts.append(f"        '{key}': '{value}',\n")
            code_parts.append('    }\n')
            code_parts.append('    \n')
            code_parts.append('    # Initialize and run crew\n')
            code_parts.append('    print("=" * 50)\n')
            code_parts.append(f'    print("{crew_name.replace("_", " ").upper()} - STARTING EXECUTION")\n')
            code_parts.append('    print("=" * 50)\n')
            code_parts.append('    for key, value in inputs.items():\n')
            code_parts.append('        print(f"{key}: {value}")\n')
            code_parts.append('    print()\n')
            code_parts.append('    \n')
            code_parts.append(f'    crew = {class_name}()\n')
            code_parts.append('    result = crew.crew().kickoff(inputs=inputs)\n')
            code_parts.append('    \n')
            code_parts.append('    print()\n')
            code_parts.append('    print("=" * 50)\n')
            code_parts.append('    print("EXECUTION COMPLETED")\n')
            code_parts.append('    print("=" * 50)\n')
            code_parts.append('    print(result)\n')
            code_parts.append('    \n')
            code_parts.append('    return result\n\n')

            # Entry point
            code_parts.append("if __name__ == '__main__':\n")
            code_parts.append('    main()\n')

        return ''.join(code_parts)

    def _generate_crew_imports(self, tools: List[str], for_notebook: bool) -> str:
        """Generate import statements"""
        imports = []

        imports.append('from crewai import Agent, Crew, Task, Process')
        imports.append('from crewai.project import CrewBase, agent, crew, task')

        # Add tool imports (crewai.tools since v1.0.0)
        standard_tool_imports = {
            'SerperDevTool': 'from crewai_tools import SerperDevTool',
            'ScrapeWebsiteTool': 'from crewai_tools import ScrapeWebsiteTool',
            'DallETool': 'from crewai_tools import DallETool',
        }

        tool_imports = set()
        for tool in tools:
            if tool in standard_tool_imports:
                tool_imports.add(standard_tool_imports[tool])

        imports.extend(sorted(tool_imports))

        if for_notebook:
            imports.append('from datetime import datetime')

        return '\n'.join(imports)

    def _generate_agent_method(
        self,
        agent_name: str,
        agent_tools: List[str],
        include_comments: bool,
        for_notebook: bool = False
    ) -> str:
        """Generate agent method"""
        code = f'    @agent\n'
        code += f'    def {agent_name}(self) -> Agent:\n'

        if include_comments:
            code += f'        """Create {agent_name} agent"""\n'

        code += f'        return Agent(\n'
        code += f"            config=self.agents_config['{agent_name}'],\n"

        # Add tools if any
        if agent_tools:
            tool_instances = ', '.join(f'{tool}()' for tool in agent_tools if tool in ['SerperDevTool', 'ScrapeWebsiteTool', 'DallETool'])
            if tool_instances:
                code += f'            tools=[{tool_instances}],\n'

        code += f'            verbose=True\n'
        code += f'        )\n'

        return code

    def _generate_task_method(self, task_name: str, include_comments: bool, for_notebook: bool = False) -> str:
        """Generate task method"""
        code = f'    @task\n'
        code += f'    def {task_name}(self) -> Task:\n'

        if include_comments:
            code += f'        """Create {task_name} task"""\n'

        code += f'        return Task(\n'
        code += f"            config=self.tasks_config['{task_name}']\n"
        code += f'        )\n'

        return code

    def _generate_crew_method(self, process: str, include_comments: bool) -> str:
        """Generate crew method"""
        code = '    @crew\n'
        code += '    def crew(self) -> Crew:\n'

        if include_comments:
            code += '        """Assemble the crew"""\n'

        code += '        return Crew(\n'
        code += '            agents=self.agents,\n'
        code += '            tasks=self.tasks,\n'
        code += f'            process={process},\n'
        code += '            verbose=True\n'
        code += '        )\n'

        return code

    def _get_tool_instantiation(self, tool_name: str) -> Optional[str]:
        """
        Get the instantiation code for a tool by its name.

        Args:
            tool_name: Name of the tool (e.g., "PerplexityTool", "SerperDevTool")

        Returns:
            Instantiation code string (e.g., "PerplexitySearchTool()") or None if unknown
        """
        # Map tool names to their instantiation code
        tool_mapping = {
            "PerplexityTool": "PerplexitySearchTool()",
            "SerperDevTool": "SerperDevTool()",
            "ScrapeWebsiteTool": "ScrapeWebsiteTool()",
            "DallETool": "DallETool()",
            "GenieTool": "GenieTool()",
        }

        instantiation = tool_mapping.get(tool_name)
        if instantiation:
            logger.info(f"Mapped tool '{tool_name}' to instantiation: {instantiation}")
            return instantiation
        else:
            logger.warning(f"Unknown tool name '{tool_name}' - no instantiation mapping found")
            return None

    def _generate_notebook_crew_code(
        self,
        crew_name: str,
        agents: List[Dict[str, Any]],
        tasks: List[Dict[str, Any]],
        process_type: str = "sequential",
        include_comments: bool = True,
        mcp_servers: Optional[List[Dict[str, Any]]] = None
    ) -> str:
        """
        Generate notebook-friendly crew code using direct instantiation (no decorators)

        When ``mcp_servers`` are configured, the crew is built inside a
        ``create_crew(mcp_tools=None)`` function so MCP tools (resolved at runtime
        via ``MCPServerAdapter``) can be injected into the agents at execution time.

        Args:
            crew_name: Name of the crew
            agents: List of agent configurations
            tasks: List of task configurations
            process_type: Process type (sequential, hierarchical, etc.)
            include_comments: Whether to include explanatory comments
            mcp_servers: MCP servers configured on the crew (optional)

        Returns:
            Python code for direct crew instantiation
        """
        code_parts = []

        if include_comments:
            code_parts.append('"""\n')
            code_parts.append('Create Crew with Agents and Tasks\n')
            code_parts.append('"""\n\n')

        # Collect unique LLM models used
        llm_models = set()
        agent_llm_map = {}
        for agent in agents:
            agent_name = agent.get('name', 'agent').lower().replace(' ', '_')
            llm = agent.get('llm')
            if llm:
                llm_models.add(llm)
                agent_llm_map[agent_name] = llm

        # Create LLM instances
        llm_var_map = {}
        if llm_models:
            if include_comments:
                code_parts.append('# Create LLM instances\n')

            # gpt-5-3-codex on Databricks ONLY supports the OpenAI Responses API;
            # the default Chat Completions route returns 404 "Supervisor API is
            # not enabled". Emit a helper so those models are built correctly.
            if any(self._is_codex_model(m) for m in llm_models):
                code_parts.append(self._generate_codex_llm_helper())

            for idx, llm_model in enumerate(sorted(llm_models)):
                # Create a safe variable name from the model name
                var_name = f"llm_{idx + 1}"
                llm_var_map[llm_model] = var_name
                if self._is_codex_model(llm_model):
                    code_parts.append(f'{var_name} = make_codex_llm("{llm_model}")\n')
                else:
                    code_parts.append(f'{var_name} = LLM(model="databricks/{llm_model}")\n')

            code_parts.append('\n')

        # MCP servers configured on the crew: emit the connection params and build
        # the crew inside a create_crew(mcp_tools=None) function so the MCP tools
        # (resolved at runtime via MCPServerAdapter) can be injected into agents.
        mcp_servers = mcp_servers or []
        has_mcp = bool(mcp_servers)
        if has_mcp:
            code_parts.append(self._generate_mcp_server_params(mcp_servers))

        # Build the agent + task creation body as a list of '\n'-terminated lines so
        # it can be emitted flat OR indented inside create_crew().
        body = []

        # Create agents
        if include_comments:
            body.append('# Create agents\n')

        agent_vars = []
        for agent in agents:
            agent_name = agent.get('name', 'agent').lower().replace(' ', '_')
            agent_vars.append(agent_name)

            # Get the LLM variable for this agent
            llm_model = agent_llm_map.get(agent_name)

            body.append(f'{agent_name}_config = dict(agents_config[\'{agent_name}\'])\n')

            if llm_model and llm_model in llm_var_map:
                body.append(f'{agent_name}_config["llm"] = {llm_var_map[llm_model]}\n')

            if has_mcp:
                body.append('if mcp_tools:\n')
                body.append(f'    {agent_name}_config["tools"] = list(mcp_tools)\n')

            body.append(f'{agent_name} = Agent(**{agent_name}_config)\n')

        body.append('\n')

        # Create tasks
        if include_comments:
            body.append('# Create tasks\n')

        # Initialize task map for context resolution
        body.append('task_map = {}\n')
        body.append('\n')

        task_vars = []
        for task in tasks:
            task_name = task.get('name', 'task').lower().replace(' ', '_')
            task_vars.append(task_name)

            body.append(f'{task_name}_config = dict(tasks_config[\'{task_name}\'])\n')

            # Map agent name string to agent instance
            body.append(f'if "agent" in {task_name}_config and isinstance({task_name}_config["agent"], str):\n')
            body.append(f'    agent_name = {task_name}_config["agent"]\n')
            body.append(f'    # Map agent name to agent variable\n')

            # Build agent map string outside f-string to avoid backslash in expression
            agent_items = []
            for a in agents:
                agent_name = a.get('name', 'agent').lower().replace(' ', '_')
                agent_items.append(f'"{agent_name}": {agent_name}')
            agent_map_str = ", ".join(agent_items)

            body.append(f'    agent_map = {{{agent_map_str}}}\n')
            body.append(f'    {task_name}_config["agent"] = agent_map.get(agent_name)\n')
            body.append(f'\n')

            # Map context strings to task instances
            body.append(f'if "context" in {task_name}_config and isinstance({task_name}_config["context"], list):\n')
            body.append(f'    context_tasks = []\n')
            body.append(f'    for ctx_task_name in {task_name}_config["context"]:\n')
            body.append(f'        if isinstance(ctx_task_name, str) and ctx_task_name in task_map:\n')
            body.append(f'            context_tasks.append(task_map[ctx_task_name])\n')
            body.append(f'    {task_name}_config["context"] = context_tasks\n')
            body.append(f'\n')

            # Instantiate tools if present
            task_tools = task.get('tools', [])
            if task_tools:
                body.append(f'# Instantiate tools for {task_name}\n')
                body.append(f'{task_name}_tools = []\n')
                for tool_name in task_tools:
                    tool_instance = self._get_tool_instantiation(tool_name)
                    if tool_instance:
                        body.append(f'{task_name}_tools.append({tool_instance})\n')
                body.append(f'{task_name}_config["tools"] = {task_name}_tools\n')
                body.append(f'\n')

            body.append(f'{task_name} = Task(**{task_name}_config)\n')
            body.append(f'task_map[\'{task_name}\'] = {task_name}\n')

        body.append('\n')

        # Crew constructor lines (no leading assignment) so they can be prefixed
        # with "crew = " (flat) or "return " (inside create_crew()).
        process_map = {
            'sequential': 'Process.sequential',
            'hierarchical': 'Process.hierarchical',
        }
        process = process_map.get(process_type, 'Process.sequential')
        crew_ctor = [
            'Crew(\n',
            f'    agents=[{", ".join(agent_vars)}],\n',
            f'    tasks=[{", ".join(task_vars)}],\n',
            f'    process={process},\n',
            '    verbose=True\n',
            ')\n',
        ]

        def _indent(line: str) -> str:
            return line if line.strip() == '' else '    ' + line

        if has_mcp:
            code_parts.append('def create_crew(mcp_tools=None):\n')
            code_parts.append('    """Create the crew; inject MCP tools into the agents when provided."""\n')
            for line in body:
                code_parts.append(_indent(line))
            code_parts.append('    return ' + crew_ctor[0])
            for line in crew_ctor[1:]:
                code_parts.append(_indent(line))
            code_parts.append('\n')
            code_parts.append('crew = create_crew()\n')
        else:
            code_parts.extend(body)
            if include_comments:
                code_parts.append('# Create crew\n')
            code_parts.append('crew = ' + crew_ctor[0])
            code_parts.extend(crew_ctor[1:])
            code_parts.append('\n')

        code_parts.append('print("Crew created successfully")\n')

        return ''.join(code_parts)

"""
Databricks Notebook exporter for CrewAI crews.
"""

from typing import Dict, Any, List, Optional
import json
import logging
import aiofiles
from .base_exporter import BaseExporter
from .yaml_generator import YAMLGenerator
from .code_generator import CodeGenerator

logger = logging.getLogger(__name__)


class DatabricksNotebookExporter(BaseExporter):
    """Export crew as a Databricks notebook (.ipynb format)"""

    def __init__(self):
        super().__init__()
        self.yaml_generator = YAMLGenerator()
        self.code_generator = CodeGenerator()

    async def export(self, crew_data: Dict[str, Any], options: Dict[str, Any]) -> Dict[str, Any]:
        """
        Export crew as Databricks notebook

        Args:
            crew_data: Crew configuration data
            options: Export options

        Returns:
            Dictionary with notebook structure and metadata
        """
        crew_name = crew_data.get('name', 'crew')
        sanitized_name = self._sanitize_name(crew_name)
        agents = crew_data.get('agents', [])
        tasks = crew_data.get('tasks', [])

        # Extract options
        include_custom_tools = options.get('include_custom_tools', True)
        include_comments = options.get('include_comments', True)
        include_tracing = options.get('include_tracing', True)  # MLflow autolog
        include_evaluation = options.get('include_evaluation', True)
        include_deployment = options.get('include_deployment', True)
        model_override = options.get('model_override')

        # Log options for debugging
        logger.info(f"[Export Options] include_tracing={include_tracing}, include_evaluation={include_evaluation}, include_deployment={include_deployment}")
        logger.info(f"[Export Options] Raw options dict: {options}")

        # Get all tools used
        tools = self._get_unique_tools(agents, tasks)
        logger.info(f"[Export Debug] All tools found: {tools}")

        # Determine if this is a deployment-only export
        deployment_only = include_deployment and not include_evaluation and not include_tracing
        logger.info(f"[Export Logic] Deployment-only mode: {deployment_only}")

        # Generate notebook cells
        cells = []

        # Always include title and basic setup
        # 1. Title cell (markdown)
        cells.append(self._create_markdown_cell(
            self._generate_title_markdown(crew_name, agents, tasks)
        ))

        if deployment_only:
            # For deployment-only, include minimal cells but need crew definitions for deployment
            # 2. Setup instructions (markdown) - minimal
            cells.append(self._create_markdown_cell(
                "## 🚀 Deployment Setup\n\n"
                "This notebook contains the deployment code for your CrewAI agent."
            ))

            # 3. Environment Configuration (for API keys like Perplexity)
            cells.append(self._create_markdown_cell(
                "## ⚙️ Environment Configuration\n\n"
                "Configure API keys and environment variables needed by your crew."
            ))

            cells.append(self._create_code_cell(
                self._generate_env_config_code(tools)
            ))

            # 4. Crew Definition Variables (needed by deployment code)
            cells.append(self._create_markdown_cell(
                "## 📋 Crew Definition\n\n"
                "Define your crew configuration as YAML strings."
            ))

            cells.append(self._create_code_cell(
                self._generate_crew_yaml_vars(agents, tasks, model_override, include_comments)
            ))

            # 5. Deployment section
            cells.append(self._create_markdown_cell(
                "## 🚀 Deploy to Model Serving Endpoint\n\n"
                "Deploy your crew as a production endpoint for API access."
            ))

            cells.append(self._create_code_cell(
                await self._generate_deployment_code(sanitized_name, tools)
            ))

        else:
            # Full export with all cells
            
            # 2. Setup instructions (markdown)
            cells.append(self._create_markdown_cell(
                self._generate_setup_instructions()
            ))

            # 3. Install dependencies (code)
            cells.append(self._create_code_cell(
                self._generate_install_code(tools)
            ))

            # 5. Import libraries (code)
            cells.append(self._create_code_cell(
                self._generate_imports_code()
            ))

            # 5b. MLflow configuration (code) - only if tracing enabled
            if include_tracing:
                cells.append(self._create_code_cell(
                    self._generate_mlflow_config()
                ))

            # 6. Environment configuration (code)
            cells.append(self._create_code_cell(
                self._generate_environment_config()
            ))

            # 7. Agents configuration header (markdown)
            cells.append(self._create_markdown_cell(
                "## 👥 Agent Configuration"
            ))

            # 8. Agents YAML definition (code)
            agents_yaml = self.yaml_generator.generate_agents_yaml(
                agents,
                model_override=model_override,
                include_comments=False  # Comments in markdown instead
            )
            cells.append(self._create_code_cell(
                self._generate_agents_yaml_code(agents_yaml)
            ))

            # 9. Tasks configuration header (markdown)
            cells.append(self._create_markdown_cell(
                "## 📋 Task Configuration"
            ))

            # 10. Tasks YAML definition (code)
            tasks_yaml = self.yaml_generator.generate_tasks_yaml(
                tasks,
                agents,
                include_comments=False
            )
            cells.append(self._create_code_cell(
                self._generate_tasks_yaml_code(tasks_yaml)
            ))

            # 11. Custom tools (if any)
            if include_custom_tools:
                logger.info(f"[Export Debug] All tools before filtering: {tools}")
                custom_tools = [t for t in tools if t not in ['SerperDevTool', 'ScrapeWebsiteTool', 'DallETool']]
                logger.info(f"[Export Debug] Custom tools after filtering: {custom_tools}")
                if custom_tools:
                    cells.append(self._create_markdown_cell(
                        "## 🛠️ Custom Tools"
                    ))
                    cells.append(self._create_code_cell(
                        await self._generate_custom_tools_placeholder(custom_tools)
                    ))

            # 12. Crew definition header (markdown)
            cells.append(self._create_markdown_cell(
                "## 🎯 Crew Definition"
            ))

            # 13. Crew class implementation (code)
            crew_code = self.code_generator.generate_crew_code(
                sanitized_name,
                agents,
                tasks,
                tools,
                process_type='sequential',
                include_comments=False,
                for_notebook=True
            )
            cells.append(self._create_code_cell(crew_code))

            # 14. Execution instructions (markdown)
            cells.append(self._create_markdown_cell(
                "## ▶️ Execute the Crew"
            ))

            # 15. Main execution logic (code)
            main_code = self.code_generator.generate_main_code(
                sanitized_name,
                sample_inputs={'topic': 'Artificial Intelligence trends in 2025'},
                include_comments=False,
                for_notebook=True,
                include_tracing=include_tracing
            )
            cells.append(self._create_code_cell(main_code))

            # 16. MLflow tracking info (markdown) - only if tracing enabled
            if include_tracing:
                cells.append(self._create_markdown_cell(
                    "## 📊 MLflow Tracking\n\n"
                    "Click the **Experiment** icon in the notebook toolbar to view tracked runs, metrics, and artifacts."
                ))

            # 17. Evaluation section - only if evaluation enabled
            if include_evaluation:
                cells.append(self._create_markdown_cell(
                    "## 📈 Evaluation\n\n"
                    "Evaluate your crew's performance using MLflow evaluation metrics."
                ))

                cells.append(self._create_code_cell(
                    self._generate_evaluation_code(sanitized_name)
                ))

            # 18. Deployment section - only if deployment enabled
            if include_deployment:
                cells.append(self._create_markdown_cell(
                    "## 🚀 Deploy to Model Serving Endpoint\n\n"
                    "Deploy your crew as a production endpoint for API access."
                ))

                cells.append(self._create_code_cell(
                    await self._generate_deployment_code(sanitized_name, tools)
                ))

        # Create notebook structure
        notebook = {
            "cells": cells,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3"
                },
                "language_info": {
                    "codemirror_mode": {
                        "name": "ipython",
                        "version": 3
                    },
                    "file_extension": ".py",
                    "mimetype": "text/x-python",
                    "name": "python",
                    "nbconvert_exporter": "python",
                    "pygments_lexer": "ipython3",
                    "version": "3.9.0"
                },
                "application/vnd.databricks.v1+notebook": {
                    "notebookName": f"{crew_name}",
                    "dashboards": [],
                    "language": "python",
                    "widgets": {},
                    "notebookMetadata": {
                        "pythonIndentUnit": 4
                    }
                }
            },
            "nbformat": 4,
            "nbformat_minor": 0
        }

        # Convert notebook to JSON string for download
        notebook_content = json.dumps(notebook, indent=2)

        return {
            'crew_id': str(crew_data.get('id', '')),
            'crew_name': crew_name,
            'export_format': 'databricks_notebook',
            'notebook': notebook,
            'notebook_content': notebook_content,
            'metadata': {
                'agents_count': len(agents),
                'tasks_count': len(tasks),
                'tools_count': len(tools),
                'cells_count': len(cells),
                'sanitized_name': sanitized_name,
            },
            'generated_at': self._get_timestamp(),
            'size_bytes': len(notebook_content)
        }

    def _create_markdown_cell(self, content: str) -> Dict[str, Any]:
        """Create a markdown cell with proper line formatting"""
        # Split content into lines, preserving newlines for proper notebook format
        lines = content.splitlines(keepends=True)
        # If no lines have newlines, add them (except last line)
        if lines and not any('\n' in line for line in lines):
            lines = [line + '\n' for line in lines[:-1]] + ([lines[-1]] if lines else [])

        return {
            "cell_type": "markdown",
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": False,
                    "inputWidgets": {},
                    "nuid": ""
                }
            },
            "source": lines if lines else [""]
        }

    def _create_code_cell(self, content: str) -> Dict[str, Any]:
        """Create a code cell with proper line formatting"""
        # Split content into lines, preserving newlines for proper notebook format
        lines = content.splitlines(keepends=True)
        # If no lines have newlines, add them (except last line)
        if lines and not any('\n' in line for line in lines):
            lines = [line + '\n' for line in lines[:-1]] + ([lines[-1]] if lines else [])

        return {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {
                "application/vnd.databricks.v1+cell": {
                    "title": "",
                    "showTitle": False,
                    "inputWidgets": {},
                    "nuid": ""
                }
            },
            "outputs": [],
            "source": lines if lines else [""]
        }

    def _generate_title_markdown(
        self,
        crew_name: str,
        agents: List[Dict[str, Any]],
        tasks: List[Dict[str, Any]]
    ) -> str:
        """Generate title markdown"""
        return f"""# 🤖 {crew_name.replace('_', ' ').title()} - Databricks Notebook

**Exported from Kasal Platform**

---

## Overview

This notebook contains a complete CrewAI agent setup exported from Kasal.

### Crew Details
- **Name:** {crew_name}
- **Generated:** {self._get_timestamp()}
- **Agents:** {len(agents)} ({', '.join(a.get('name', 'Agent') for a in agents[:3])}{'...' if len(agents) > 3 else ''})
- **Tasks:** {len(tasks)} ({', '.join(t.get('name', 'Task') for t in tasks[:3])}{'...' if len(tasks) > 3 else ''})
- **Process:** Sequential

### Architecture
```
{'→ '.join(a.get('name', 'Agent') for a in agents[:3])}
```
"""

    def _generate_setup_instructions(self) -> str:
        """Generate setup instructions"""
        return """## 🚀 Setup

1. Run installation cell and restart Python kernel
2. Configure API keys in environment cell (use Databricks secrets)
3. Run all cells sequentially
"""


    def _generate_install_code(self, tools: List[str]) -> str:
        """Generate installation code"""
        code = '"""\n'
        code += 'Install Required Packages\n'
        code += '"""\n\n'

        code += '# Install LiteLLM (required by CrewAI)\n'
        code += '%pip install litellm\n'

        code += '# Install MLflow with latest features\n'
        code += '%pip install mlflow --upgrade --pre\n'

        code += '# Install Databricks LangChain integration\n'
        code += '%pip install databricks-langchain\n'

        code += '# Install Unity Catalog CrewAI integration\n'
        code += '%pip install unitycatalog-crewai -U --quiet\n'

        code += '# Install CrewAI\n'
        code += '%pip install crewai\n'

        code += '# Restart Python kernel\n'
        code += 'dbutils.library.restartPython()'

        return code

    def _generate_imports_code(self) -> str:
        """Generate imports code"""
        return '''"""
Import Required Libraries
"""

from crewai import Agent, Crew, Task, Process, LLM
from crewai.project import CrewBase, agent, crew, task
from crewai_tools import SerperDevTool
import yaml
import os
import mlflow
from typing import Dict, Any, List
from datetime import datetime

print("✅ All libraries imported successfully")
print(f"Execution started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")'''

    def _generate_installation_verification(self) -> str:
        """Generate installation verification code"""
        return '''"""
Verify Installation and Check for Conflicts
"""

import importlib
import sys

print("\\n🔍 Verifying CrewAI Installation...\\n")

# Check CrewAI version
try:
    import crewai
    print(f"✅ CrewAI version: {crewai.__version__}")
except Exception as e:
    print(f"❌ CrewAI import failed: {e}")

# Check critical dependencies
dependencies = {
    "pydantic": "Pydantic (data validation)",
    "yaml": "PyYAML (configuration)",
    "langchain_core": "LangChain Core (LLM integration)",
}

for module, description in dependencies.items():
    try:
        mod = importlib.import_module(module)
        version = getattr(mod, "__version__", "unknown")
        print(f"✅ {description}: {version}")
    except ImportError:
        print(f"⚠️  {description}: Not installed (some features may not work)")

# Check for potential conflicts
print("\\n🔍 Checking for Potential Conflicts...\\n")

conflict_checks = {
    "mlflow": "MLflow (model tracking)",
    "databricks.sql": "Databricks SQL Connector",
    "dbt": "dbt (data transformation)",
}

conflicts_found = False
for module, description in conflict_checks.items():
    try:
        mod = importlib.import_module(module)
        version = getattr(mod, "__version__", "unknown")
        print(f"✅ {description}: {version} (available)")
    except ImportError:
        print(f"ℹ️  {description}: Not installed (optional)")
    except Exception as e:
        print(f"⚠️  {description}: Error - {e}")
        conflicts_found = True

if not conflicts_found:
    print("\\n✅ No obvious conflicts detected. You can proceed with the notebook.")
else:
    print("\\n⚠️  Some conflicts detected. Basic CrewAI functionality should work, but some Databricks features may have issues.")

print("\\n" + "="*70)'''

    def _generate_environment_config(self) -> str:
        """Generate environment configuration code"""
        return '''"""
Configure Environment Variables

⚠️ REQUIRED CONFIGURATION ⚠️
You MUST update the secret scope name before running this notebook!
"""

# Option 1: Using Databricks Secrets (Recommended for production)
try:
    # ⚠️ CHANGE THIS: Replace 'YOUR-SECRET-SCOPE-NAME' with your actual Databricks secret scope
    secret_scope = 'YOUR-SECRET-SCOPE-NAME'  # TODO: Update this with your secret scope name

    # Verify scope name was changed
    if secret_scope == 'YOUR-SECRET-SCOPE-NAME':
        raise ValueError(
            "\\n\\n❌ CONFIGURATION ERROR: You must update 'secret_scope' with your actual Databricks secret scope name!\\n"
            "   1. Go to your Databricks workspace Settings -> Secrets\\n"
            "   2. Find your secret scope name\\n"
            "   3. Replace 'YOUR-SECRET-SCOPE-NAME' above with your actual scope name\\n"
        )

    os.environ['DATABRICKS_HOST'] = dbutils.secrets.get(scope=secret_scope, key='databricks-host')
    os.environ['DATABRICKS_TOKEN'] = dbutils.secrets.get(scope=secret_scope, key='databricks-token')

    # Optional: Add API keys for tools you're using
    try:
        os.environ['SERPER_API_KEY'] = dbutils.secrets.get(scope=secret_scope, key='serper-api-key')
    except:
        pass  # SerperDevTool not used

    try:
        os.environ['PERPLEXITY_API_KEY'] = dbutils.secrets.get(scope=secret_scope, key='perplexity-api-key')
    except:
        pass  # PerplexityTool not used

    print("✅ Environment configured using Databricks Secrets")
except Exception as e:
    print(f"⚠️  Warning: Could not load secrets: {e}")
    print("   Please configure secrets or use Option 2 below")

# Option 2: Direct configuration (For testing only - NOT RECOMMENDED for production)
# SECURITY WARNING: Never commit notebooks with hardcoded credentials!
# Uncomment and replace with actual values only for local testing:
# os.environ['DATABRICKS_HOST'] = 'https://example.cloud.databricks.com'  # TODO: Replace with your workspace URL
# os.environ['DATABRICKS_TOKEN'] = 'dapi...'  # TODO: Replace with your token
# os.environ['SERPER_API_KEY'] = 'your-key'  # TODO: Replace if using SerperDevTool
# os.environ['PERPLEXITY_API_KEY'] = 'your-key'  # TODO: Replace if using PerplexityTool

# Verify configuration
print("\\n📋 Current Configuration:")
print(f"   - DATABRICKS_HOST: {'✅ Set' if os.getenv('DATABRICKS_HOST') else '❌ Not set'}")
print(f"   - DATABRICKS_TOKEN: {'✅ Set' if os.getenv('DATABRICKS_TOKEN') else '❌ Not set'}")
print(f"   - SERPER_API_KEY: {'✅ Set' if os.getenv('SERPER_API_KEY') else '❌ Not set'}")
print(f"   - PERPLEXITY_API_KEY: {'✅ Set' if os.getenv('PERPLEXITY_API_KEY') else '❌ Not set'}")'''

    def _generate_mlflow_config(self) -> str:
        """Generate MLflow configuration and autologging setup"""
        return '''# Enable MLflow autologging for automatic experiment tracking
mlflow.crewai.autolog()
print("✅ MLflow autologging enabled - all executions will be tracked")'''

    def _generate_mlflow_experiment_viewing(self) -> str:
        """Generate code to view MLflow experiment"""
        return '''"""
View MLflow Experiment Information
"""

# Get current experiment
experiment = mlflow.get_experiment_by_name(mlflow.active_run().info.experiment_id if mlflow.active_run() else None)

if experiment:
    print("📊 MLflow Experiment Details:")
    print(f"   Experiment ID: {experiment.experiment_id}")
    print(f"   Experiment Name: {experiment.name}")
    print(f"   Artifact Location: {experiment.artifact_location}")

    # Get recent runs
    runs = mlflow.search_runs(experiment_ids=[experiment.experiment_id], order_by=["start_time DESC"], max_results=5)

    if len(runs) > 0:
        print(f"\\n   Recent Runs: {len(runs)}")
        for idx, run in runs.iterrows():
            print(f"      - Run {idx+1}: Status={run['status']}, Duration={run.get('metrics.duration', 'N/A')}s")
    else:
        print("\\n   No runs found yet. Execute the crew above to create your first run!")
else:
    print("⚠️  No active experiment found")
    print("   Run the crew execution cell above to start tracking with MLflow")

# Display link to MLflow UI
print("\\n🔗 To view detailed metrics and artifacts:")
print("   Click the 'Experiment' icon in the notebook toolbar")
print("   Or navigate to the MLflow UI in your workspace")'''

    def _generate_agents_yaml_code(self, agents_yaml: str) -> str:
        """Generate agents YAML code"""
        # Escape backslashes and triple quotes in YAML content for proper Python string formatting
        escaped_yaml = agents_yaml.replace('\\', '\\\\').replace('"""', r'\"\"\"')

        code = '"""\nAgent Definitions (YAML Format)\n"""\n\n'
        code += f'agents_yaml = """{escaped_yaml}"""\n\n'
        code += '# Parse YAML configuration\n'
        code += 'agents_config = yaml.safe_load(agents_yaml)\n\n'
        code += 'print("✅ Agent configuration loaded:")\n'
        code += 'for agent_name in agents_config.keys():\n'
        code += '    print(f"   - {agent_name}: {agents_config[agent_name][\'role\'][:50]}...")'

        return code

    def _generate_tasks_yaml_code(self, tasks_yaml: str) -> str:
        """Generate tasks YAML code"""
        # Escape backslashes and triple quotes in YAML content for proper Python string formatting
        escaped_yaml = tasks_yaml.replace('\\', '\\\\').replace('"""', r'\"\"\"')

        code = '"""\nTask Definitions (YAML Format)\n"""\n\n'
        code += f'tasks_yaml = """{escaped_yaml}"""\n\n'
        code += '# Parse YAML configuration\n'
        code += 'tasks_config = yaml.safe_load(tasks_yaml)\n\n'
        code += 'print("✅ Task configuration loaded:")\n'
        code += 'for task_name in tasks_config.keys():\n'
        code += '    print(f"   - {task_name}: {tasks_config[task_name][\'description\'][:50]}...")'

        return code

    def _generate_env_config_code(self, tools: List[str]) -> str:
        """Generate environment configuration code for API keys"""
        code = '"""\nEnvironment Configuration\n\nConfigure API keys for custom tools.\n"""\n\nimport os\n\n'

        # Check which custom tools need API keys
        custom_tools = [t for t in tools if t not in ['SerperDevTool', 'ScrapeWebsiteTool', 'DallETool']]

        if 'PerplexityTool' in custom_tools:
            code += '# Perplexity API Key (required for PerplexityTool)\n'
            code += '# Option 1: Set as environment variable in Databricks workspace secrets\n'
            code += '# Option 2: Set directly here (not recommended for production)\n'
            code += 'if "PERPLEXITY_API_KEY" not in os.environ:\n'
            code += '    # IMPORTANT: Replace with your actual API key or use Databricks secrets\n'
            code += '    # Get your API key from: https://www.perplexity.ai/settings/api\n'
            code += '    os.environ["PERPLEXITY_API_KEY"] = "pplx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"  # Replace this!\n'
            code += '    print("⚠️  Using hardcoded Perplexity API key (not recommended for production)")\n'
            code += '    print("   Consider using Databricks secrets: dbutils.secrets.get(scope=\'my-scope\', key=\'perplexity-api-key\')")\n'
            code += 'else:\n'
            code += '    print("✅ Perplexity API key loaded from environment")\n\n'

        if 'GenieTool' in custom_tools:
            code += '# Genie configuration (if needed)\n'
            code += '# os.environ["GENIE_CONFIG"] = "your-config"\n\n'

        if not custom_tools:
            code += '# No custom tools requiring API keys\nprint("✅ No additional API keys required")\n'

        return code

    def _generate_crew_yaml_vars(self, agents: List[Dict], tasks: List[Dict], model_override: Optional[str], include_comments: bool) -> str:
        """Generate crew definition as YAML variables"""
        # Use the existing YAMLGenerator instance
        yaml_gen = YAMLGenerator()

        # Generate YAML configurations
        agents_yaml = yaml_gen.generate_agents_yaml(agents, model_override, include_comments=False)
        tasks_yaml = yaml_gen.generate_tasks_yaml(tasks, agents, include_comments=False)

        # Escape for embedding in Python strings
        escaped_agents_yaml = agents_yaml.replace('\\', '\\\\').replace('"""', r'\"\"\"')
        escaped_tasks_yaml = tasks_yaml.replace('\\', '\\\\').replace('"""', r'\"\"\"')

        code = ''
        if include_comments:
            code += '"""\nCrew Configuration (YAML Format)\n\nDefine agents and tasks as YAML strings.\n"""\n\nimport yaml\n\n'
        else:
            code += 'import yaml\n\n'

        code += f'# Agents configuration\nagents_yaml = """{escaped_agents_yaml}"""\n\n'
        code += f'# Tasks configuration\ntasks_yaml = """{escaped_tasks_yaml}"""\n\n'
        code += 'print("✅ Crew configuration loaded")\n'
        code += 'print(f"   Agents: {len(yaml.safe_load(agents_yaml))}")\n'
        code += 'print(f"   Tasks: {len(yaml.safe_load(tasks_yaml))}")'

        return code

    async def _generate_custom_tools_placeholder(self, custom_tools: List[str]) -> str:
        """Generate custom tools with real implementations"""
        from pathlib import Path

        logger.info(f"[Tool Export] Custom tools detected: {custom_tools}")

        # Read the actual tool implementations
        tools_code = []
        # This file is in: engines/crewai/exporters/databricks_notebook_exporter.py
        # We need to go up to: engines/crewai/tools/custom/
        backend_path = Path(__file__).parent.parent  # Go up to crewai directory
        tools_dir = backend_path / "tools" / "custom"

        logger.info(f"[Tool Export] Looking for tool files in: {tools_dir}")
        logger.info(f"[Tool Export] Tools directory exists: {tools_dir.exists()}")

        tool_file_mapping = {
            "PerplexityTool": "perplexity_tool.py",
            "GenieTool": "genie_tool.py",
        }

        for tool_name in custom_tools:
            logger.info(f"[Tool Export] Processing tool: {tool_name}")
            tool_file = tool_file_mapping.get(tool_name)
            logger.info(f"[Tool Export] Mapped to file: {tool_file}")

            if tool_file:
                tool_path = tools_dir / tool_file
                logger.info(f"[Tool Export] Full path: {tool_path}")
                logger.info(f"[Tool Export] File exists: {tool_path.exists()}")

                if tool_path.exists():
                    try:
                        async with aiofiles.open(tool_path, 'r') as f:
                            tool_code = await f.read()
                            logger.info(f"[Tool Export] Successfully read {len(tool_code)} characters from {tool_file}")
                            tools_code.append(f"# {tool_name} Implementation\n{tool_code}")
                    except Exception as e:
                        logger.error(f"[Tool Export] Could not read tool file {tool_file}: {e}", exc_info=True)
                else:
                    logger.warning(f"[Tool Export] Tool file not found: {tool_path}")
            else:
                logger.warning(f"[Tool Export] No file mapping found for tool: {tool_name}")

        logger.info(f"[Tool Export] Total tool implementations found: {len(tools_code)}")

        if tools_code:
            logger.info(f"[Tool Export] ✅ Including {len(tools_code)} tool implementation(s) in notebook")
            return f'''"""
Custom Tool Implementations

The following custom tools are used in this crew: {', '.join(custom_tools)}
"""

{chr(10).join(tools_code)}

print("✅ Custom tools loaded: {', '.join(custom_tools)}")'''
        else:
            logger.warning(f"[Tool Export] ⚠️ No tool implementations found, using placeholder")
            # Fallback to placeholder if no tool implementations found
            return f'''"""
Custom Tool Implementations

The following custom tools are used in this crew: {', '.join(custom_tools)}

TODO: Add custom tool implementations here
"""

from crewai.tools import BaseTool
from typing import Type
from pydantic import BaseModel, Field

print("⚠️  Custom tools detected but not implemented. Please add implementations above.")'''

    def _generate_evaluation_code(self, crew_name: str) -> str:
        """Generate MLflow evaluation code"""
        return f'''"""
Evaluate the Crew's Output

This cell demonstrates how to evaluate your crew's performance using MLflow.
"""

import pandas as pd
from mlflow.metrics import genai

# Search for the most recent run across all experiments
print("🔍 Searching for recent crew executions...")
runs_df = mlflow.search_runs(
    filter_string="",  # No filter - search all
    order_by=["start_time DESC"],
    max_results=5  # Get last 5 runs to show options
)

if not runs_df.empty:
    print(f"\\n✅ Found {{len(runs_df)}} recent runs:")
    for idx, row in runs_df.head().iterrows():
        print(f"   {{idx+1}}. Run ID: {{row['run_id'][:8]}}... | Started: {{row['start_time']}}")

    # Use the most recent run
    latest_run_id = runs_df.iloc[0]["run_id"]
    latest_run = mlflow.get_run(latest_run_id)

    print(f"\\n📊 Using latest run: {{latest_run_id}}")
    print(f"   - Experiment: {{latest_run.info.experiment_id}}")
    print(f"   - Status: {{latest_run.info.status}}")

    # Create evaluation dataset
    # You can customize the ground truth and expected outputs based on your use case
    eval_data = pd.DataFrame({{
        "inputs": [
            "Artificial Intelligence trends in 2025"
        ],
        "ground_truth": [
            "A comprehensive analysis covering AI trends, including generative AI, large language models, multimodal AI, AI safety, and practical applications across industries."
        ]
    }})

    # Define a function to get predictions from the crew
    def crew_model(inputs):
        """Wrapper function to run crew and return results"""
        results = []
        for input_text in inputs["inputs"]:
            result = run_crew(topic=input_text)
            results.append(str(result))
        return results

    # Evaluate with MLflow
    print("\\n🔄 Running evaluation...")
    print("   This will execute the crew with the evaluation dataset...")

    try:
        # Define metrics for LLM evaluation
        # Relevancy metrics
        relevancy_metrics = [
            genai.answer_relevance(),      # Measures if answer is relevant to the question
            genai.answer_correctness(),    # Evaluates correctness against ground truth
            genai.faithfulness(),           # Measures faithfulness to provided context
        ]

        # Safety metrics
        safety_metrics = [
            mlflow.metrics.toxicity(),     # Detects toxic or harmful content
        ]

        # Combine all metrics
        all_metrics = relevancy_metrics + safety_metrics

        # Run evaluation
        eval_results = mlflow.evaluate(
            model=crew_model,
            data=eval_data,
            targets="ground_truth",
            model_type="text",
            evaluators="default",
            extra_metrics=all_metrics
        )

        print("\\n✅ Evaluation complete!")
        print(f"\\n📊 Evaluation Results:")

        # Display relevancy metrics
        print("\\n🎯 Relevancy Assessment:")
        print(f"   - Answer Relevance: {{eval_results.metrics.get('answer_relevance/v1/mean', 'N/A')}}")
        print(f"   - Answer Correctness: {{eval_results.metrics.get('answer_correctness/v1/mean', 'N/A')}}")
        print(f"   - Faithfulness: {{eval_results.metrics.get('faithfulness/v1/mean', 'N/A')}}")

        # Display safety metrics
        print("\\n🛡️ Safety Assessment:")
        print(f"   - Toxicity Score: {{eval_results.metrics.get('toxicity/v1/mean', 'N/A')}}")
        print(f"     (Lower is better - scores >0.5 indicate potentially toxic content)")

        # Display evaluation results table
        print("\\n📈 Detailed Results:")
        display(eval_results.tables['eval_results_table'])

        # Log comprehensive metrics to the original run
        with mlflow.start_run(run_id=latest_run_id):
            # Log relevancy metrics
            mlflow.log_metrics({{
                "eval_answer_relevance": eval_results.metrics.get('answer_relevance/v1/mean', 0.0),
                "eval_answer_correctness": eval_results.metrics.get('answer_correctness/v1/mean', 0.0),
                "eval_faithfulness": eval_results.metrics.get('faithfulness/v1/mean', 0.0),
                "eval_toxicity": eval_results.metrics.get('toxicity/v1/mean', 0.0),
            }})
            print("\\n✅ Evaluation metrics logged to MLflow run")

    except Exception as e:
        print(f"\\n❌ Evaluation failed: {{str(e)}}")
        print("\\nNote: Make sure you have the required evaluation dependencies:")
        print("   %pip install mlflow[genai] openai")

else:
    print("❌ No runs found in MLflow.")
    print("\\nPlease execute the crew first by running the 'Execute the Crew' cell above.")
    print("\\nIf you just executed it, the run might still be registering. Wait a moment and try again.")

print("\\n💡 Tip: You can view detailed results in the MLflow UI")
print("   Click the 'Experiment' icon in the notebook toolbar")'''

    async def _generate_deployment_code(self, crew_name: str, tools: List[Dict]) -> str:
        """Generate Databricks agent deployment code using MLflow 3.x ResponsesAgent with custom tools"""
        
        has_tools = len(tools) > 0
        custom_tools = [t for t in tools if t not in ['SerperDevTool', 'ScrapeWebsiteTool', 'DallETool']]
        has_custom_tools = len(custom_tools) > 0
        
        # Get custom tool implementations if any
        custom_tools_code = ""
        custom_tools_init_code = ""
        custom_tools_assignment_code = ""

        if has_custom_tools:
            logger.info(f"[Deployment] Including custom tools: {custom_tools}")
            from pathlib import Path
            import aiofiles

            backend_path = Path(__file__).parent.parent
            tools_dir = backend_path / "tools" / "custom"

            tool_file_mapping = {
                "PerplexityTool": "perplexity_tool.py",
                "GenieTool": "genie_tool.py",
            }

            tools_code_parts = []
            for tool_name in custom_tools:
                tool_file = tool_file_mapping.get(tool_name)
                if tool_file:
                    tool_path = tools_dir / tool_file
                    if tool_path.exists():
                        try:
                            async with aiofiles.open(tool_path, 'r') as f:
                                tool_code = await f.read()
                                tools_code_parts.append(f"# {tool_name} Implementation\n{tool_code}")
                        except Exception as e:
                            logger.error(f"[Deployment] Could not read tool file {tool_file}: {e}")

            if tools_code_parts:
                custom_tools_code = "\n\n".join(tools_code_parts)

                # Build custom tools initialization code
                custom_tools_init_lines = [
                    "            # Initialize custom tools dictionary",
                    "            custom_tools_dict = {}",
                    "            # Import custom tool classes (they are defined above)",
                ]
                for tool_name in custom_tools:
                    custom_tools_init_lines.append(f"            custom_tools_dict['{tool_name}'] = {tool_name}()")
                custom_tools_init_code = "\n".join(custom_tools_init_lines)

                # Build custom tools assignment code
                custom_tools_assignment_code = """            # Add custom tools to agent if specified
            agent_tools = []
            if 'tools' in agent_data and agent_data['tools']:
                for tool_name in agent_data["tools"]:
                    if tool_name in custom_tools_dict:
                        agent_tools.append(custom_tools_dict[tool_name])"""

        # Build the agent Python code that will be written to crew_agent_responses.py
        agent_code_lines = []
        
        # Header and imports
        agent_code_lines.append(f"# Agent deployment file for {crew_name}")
        agent_code_lines.append("# This file can be logged to MLflow using models-from-code approach")
        agent_code_lines.append("")
        agent_code_lines.append("import mlflow")
        agent_code_lines.append("from mlflow.pyfunc import ResponsesAgent")
        agent_code_lines.append("from mlflow.models import set_model")
        agent_code_lines.append("from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse")
        agent_code_lines.append("import yaml")
        agent_code_lines.append("from typing import Dict, Any, List")
        agent_code_lines.append("import sys")
        agent_code_lines.append("")
        agent_code_lines.append("# CRITICAL FIX: Patch sys.stdout and sys.stderr to add isatty() method")
        agent_code_lines.append("# This prevents AttributeError when CrewAI imports in MLflow serving environment")
        agent_code_lines.append("if not hasattr(sys.stdout, 'isatty'):")
        agent_code_lines.append("    sys.stdout.isatty = lambda: False")
        agent_code_lines.append("if not hasattr(sys.stderr, 'isatty'):")
        agent_code_lines.append("    sys.stderr.isatty = lambda: False")
        agent_code_lines.append("")
        agent_code_lines.append("# NOTE: CrewAI imports are DEFERRED to load_context() to avoid module-level import issues")
        agent_code_lines.append("")
        
        # Add custom tools code if any
        if has_custom_tools:
            agent_code_lines.append("# ===== Custom Tools Implementations =====")
            agent_code_lines.append(custom_tools_code)
            agent_code_lines.append("# ===== End Custom Tools =====")
            agent_code_lines.append("")
        
        # Embedded YAML configurations
        agent_code_lines.append("# Embedded configuration (from notebook crew definition)")
        agent_code_lines.append('AGENTS_YAML = """')
        agent_code_lines.append("___AGENTS_YAML_PLACEHOLDER___")
        agent_code_lines.append('"""')
        agent_code_lines.append("")
        agent_code_lines.append('TASKS_YAML = """')
        agent_code_lines.append("___TASKS_YAML_PLACEHOLDER___")
        agent_code_lines.append('"""')
        agent_code_lines.append("")
        
        # ResponsesAgent class
        agent_code_lines.append("class CrewAgentWrapper(ResponsesAgent):")
        agent_code_lines.append('    """')
        agent_code_lines.append("    MLflow 3.x ResponsesAgent wrapper for CrewAI deployment.")
        agent_code_lines.append("    Implements Databricks Agent Framework ResponsesAgentRequest/Response schema.")
        agent_code_lines.append('    """')
        agent_code_lines.append("")
        agent_code_lines.append("    def __init__(self):")
        agent_code_lines.append('        """Initialize wrapper without loading crew"""')
        agent_code_lines.append("        self.crew = None")
        agent_code_lines.append("")
        agent_code_lines.append("    def load_context(self, context):")
        agent_code_lines.append('        """')
        agent_code_lines.append("        Load crew from embedded YAML configuration.")
        agent_code_lines.append("        Called once when the model is loaded on the serving endpoint.")
        agent_code_lines.append("        ")
        agent_code_lines.append("        IMPORTANT: CrewAI imports happen HERE, not at module level.")
        agent_code_lines.append("        sys.stdout/stderr are patched above to add isatty() method.")
        agent_code_lines.append('        """')
        agent_code_lines.append('        print("🔄 Initializing crew from configuration...")')
        agent_code_lines.append("        ")
        agent_code_lines.append("        # Import CrewAI here (sys.stdout/stderr already patched above)")
        agent_code_lines.append("        from crewai import Agent, Crew, Task, Process, LLM")
        agent_code_lines.append("        ")
        agent_code_lines.append("        try:")
        agent_code_lines.append("            # Parse YAML configurations")
        agent_code_lines.append("            agents_config = yaml.safe_load(AGENTS_YAML)")
        agent_code_lines.append("            tasks_config = yaml.safe_load(TASKS_YAML)")
        agent_code_lines.append("            ")
        agent_code_lines.append("            # Validate parsing succeeded")
        agent_code_lines.append("            if agents_config is None or not isinstance(agents_config, dict):")
        agent_code_lines.append('                raise ValueError(f"Failed to parse agents YAML. Got type: {type(agents_config)}")')
        agent_code_lines.append("            if tasks_config is None or not isinstance(tasks_config, dict):")
        agent_code_lines.append('                raise ValueError(f"Failed to parse tasks YAML. Got type: {type(tasks_config)}")')
        agent_code_lines.append("            ")
        
        # Add custom tools initialization if needed
        if has_custom_tools:
            agent_code_lines.append(custom_tools_init_code)
            agent_code_lines.append("            ")
        
        agent_code_lines.append("            # Create agents")
        agent_code_lines.append("            agents = []")
        agent_code_lines.append("            for agent_name, agent_data in agents_config.items():")
        agent_code_lines.append("                if agent_data is None or not isinstance(agent_data, dict):")
        agent_code_lines.append('                    raise ValueError(f"Invalid agent data for {agent_name}: {agent_data}")')
        agent_code_lines.append("                ")
        agent_code_lines.append("                llm_model = agent_data.get('llm')")
        agent_code_lines.append("                if not llm_model:")
        agent_code_lines.append('                    raise ValueError(f"No LLM model specified for agent {agent_name}")')
        agent_code_lines.append("                ")
        agent_code_lines.append("                llm = LLM(")
        agent_code_lines.append("                    model=llm_model,")
        agent_code_lines.append("                    temperature=agent_data.get('temperature', 0.7)")
        agent_code_lines.append("                )")
        agent_code_lines.append("                ")
        
        # Add custom tools assignment if needed
        if has_custom_tools:
            agent_code_lines.append(custom_tools_assignment_code)
            agent_code_lines.append("                ")
        
        agent_code_lines.append("                agent = Agent(")
        agent_code_lines.append("                    role=agent_data['role'],")
        agent_code_lines.append("                    goal=agent_data['goal'],")
        agent_code_lines.append("                    backstory=agent_data['backstory'],")
        agent_code_lines.append("                    llm=llm,")
        
        # Add tools parameter if custom tools are used
        if has_custom_tools:
            agent_code_lines.append("                    tools=agent_tools if agent_tools else None,")
        
        agent_code_lines.append("                    verbose=agent_data.get('verbose', True),")
        agent_code_lines.append("                    allow_delegation=agent_data.get('allow_delegation', False)")
        agent_code_lines.append("                )")
        agent_code_lines.append("                agents.append(agent)")
        agent_code_lines.append("            ")
        agent_code_lines.append("            # Create tasks")
        agent_code_lines.append("            tasks = []")
        agent_code_lines.append("            for task_name, task_data in tasks_config.items():")
        agent_code_lines.append("                agent_name = task_data['agent']")
        agent_code_lines.append("                agent_idx = list(agents_config.keys()).index(agent_name)")
        agent_code_lines.append("                ")
        agent_code_lines.append("                task = Task(")
        agent_code_lines.append("                    description=task_data['description'],")
        agent_code_lines.append("                    expected_output=task_data['expected_output'],")
        agent_code_lines.append("                    agent=agents[agent_idx]")
        agent_code_lines.append("                )")
        agent_code_lines.append("                tasks.append(task)")
        agent_code_lines.append("            ")
        agent_code_lines.append("            # Create crew")
        agent_code_lines.append("            self.crew = Crew(")
        agent_code_lines.append("                agents=agents,")
        agent_code_lines.append("                tasks=tasks,")
        agent_code_lines.append("                process=Process.sequential,")
        agent_code_lines.append("                verbose=True")
        agent_code_lines.append("            )")
        agent_code_lines.append("            ")
        agent_code_lines.append('            print("✅ Crew initialized successfully")')
        agent_code_lines.append("            ")
        agent_code_lines.append("        except Exception as err:")
        agent_code_lines.append('            print(f"❌ Failed to initialize crew: {err}")')
        agent_code_lines.append('            raise RuntimeError(f"Crew initialization failed: {err}")')
        agent_code_lines.append("")
        agent_code_lines.append("    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:")
        agent_code_lines.append('        """')
        agent_code_lines.append("        Run the crew with ResponsesAgentRequest input.")
        agent_code_lines.append("")
        agent_code_lines.append("        Args:")
        agent_code_lines.append("            request: ResponsesAgentRequest with messages")
        agent_code_lines.append("")
        agent_code_lines.append("        Returns:")
        agent_code_lines.append("            ResponsesAgentResponse with agent output")
        agent_code_lines.append('        """')
        agent_code_lines.append("        if self.crew is None:")
        agent_code_lines.append('            raise RuntimeError("Crew not initialized. Load context first.")')
        agent_code_lines.append("        ")
        agent_code_lines.append("        # Extract last user message as topic")
        agent_code_lines.append("        user_message = ''")
        agent_code_lines.append("        for msg in reversed(request.messages):")
        agent_code_lines.append("            if msg.role == 'user':")
        agent_code_lines.append("                user_message = msg.content")
        agent_code_lines.append("                break")
        agent_code_lines.append("        ")
        agent_code_lines.append("        if not user_message:")
        agent_code_lines.append('            user_message = "Please provide guidance on the topic."')
        agent_code_lines.append("        ")
        agent_code_lines.append("        try:")
        agent_code_lines.append("            # Execute crew")
        agent_code_lines.append(f"            result = self.crew.kickoff(inputs={{'topic': user_message}})")
        agent_code_lines.append("            ")
        agent_code_lines.append("            # Return ResponsesAgentResponse")
        agent_code_lines.append("            return ResponsesAgentResponse(")
        agent_code_lines.append("                content=str(result),")
        agent_code_lines.append(f"                metadata={{'crew_name': '{crew_name}'}}")
        agent_code_lines.append("            )")
        agent_code_lines.append("        except Exception as err:")
        agent_code_lines.append("            # Return error in ResponsesAgentResponse format")
        agent_code_lines.append("            return ResponsesAgentResponse(")
        agent_code_lines.append("                content=f'Error executing crew: {str(err)}',")
        agent_code_lines.append("                metadata={'error': True}")
        agent_code_lines.append("            )")
        agent_code_lines.append("")
        agent_code_lines.append("# Set the model for MLflow to use (models-from-code pattern)")
        agent_code_lines.append("set_model(CrewAgentWrapper())")
        
        # Join the agent code template
        agent_code_template = "\\n".join(agent_code_lines)

        # Now build the notebook cell code that creates this file
        custom_tools_message = f'print(f"   Includes {len(custom_tools)} custom tool(s): {", ".join(custom_tools)}")' if has_custom_tools else ""
        custom_tools_comment = f"# Note: Custom tools detected: {', '.join(custom_tools)}" if has_custom_tools else "# No custom tools detected"
        custom_tools_note = "# These tools are embedded in the deployment file and will be available to agents" if has_custom_tools else ""
        custom_tools_pip = '"requests",  # Required for custom tools' if has_custom_tools else ""

        # Return the complete deployment cell code with embedded agent code
        # Use .format() instead of f-string to avoid interpreting f-strings in agent_code_template
        return '''"""
Deploy Crew as Model Serving Endpoint

This cell demonstrates how to deploy your crew as a production endpoint
for API access via Databricks Model Serving using MLflow 3.x ResponsesAgent.
"""

from databricks import agents
import os
import mlflow
import yaml as yaml_lib

# Configuration for Unity Catalog
CATALOG_NAME = os.getenv("CATALOG_NAME", "main")
SCHEMA_NAME = os.getenv("SCHEMA_NAME", "agents")
MODEL_NAME = "{crew_name}_agent"

print("🚀 Preparing agent for deployment...")
print(f"   Target: {{CATALOG_NAME}}.{{SCHEMA_NAME}}.{{MODEL_NAME}}")

# Step 1: Fix model names in YAML to use databricks/ prefix
# This ensures LiteLLM correctly routes to Databricks models
print("\\n🔧 Fixing model names in configuration...")

agents_config = yaml_lib.safe_load(agents_yaml)
tasks_config = yaml_lib.safe_load(tasks_yaml)

for agent_name, agent_data in agents_config.items():
    if 'llm' in agent_data and agent_data['llm'].startswith('databricks-'):
        original_model = agent_data['llm']
        agent_data['llm'] = f"databricks/{{agent_data['llm']}}"
        print(f"   Fixed model name: {{original_model}} -> {{agent_data['llm']}}")

fixed_agents_yaml = yaml_lib.dump(agents_config, default_flow_style=False, sort_keys=False)
fixed_tasks_yaml = yaml_lib.dump(tasks_config, default_flow_style=False, sort_keys=False)

print("✅ Model names fixed")

# Step 2: Write ResponsesAgent wrapper to a Python file (models-from-code approach)
print("\\n📝 Creating agent Python file...")

# Agent code template with placeholders for YAML
agent_code_template = \'\'\'{agent_code}\'\'\'

# Replace placeholders with actual YAML content (using FIXED YAML with databricks/ prefix)
agent_code = agent_code_template.replace("___AGENTS_YAML_PLACEHOLDER___", fixed_agents_yaml)
agent_code = agent_code.replace("___TASKS_YAML_PLACEHOLDER___", fixed_tasks_yaml)

# Write the agent code to a file
# Use local directory instead of /tmp for Databricks compatibility
agent_file_path = os.path.join(os.getcwd(), 'crew_agent_responses.py')
with open(agent_file_path, 'w') as f:
    f.write(agent_code)

print(f"✅ Agent file created: {{agent_file_path}}")
{custom_tools_message}

# Step 3: Log the model using the Python file (not an instance)
print("\\n📦 Logging model to MLflow...")

# Note: ResponsesAgent has built-in signature inference for agent frameworks
# No need to manually create signatures

{custom_tools_comment}
{custom_tools_note}

with mlflow.start_run(run_name=f"{{MODEL_NAME}}_deployment") as run:
    # Log the model using the Python file (models-from-code approach)
    # ResponsesAgent has built-in signature inference
    model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=agent_file_path,  # Pass file path, not instance
        pip_requirements=[
            "crewai",
            "mlflow>=3.0.0",  # Minimum version for ResponsesAgent
            "databricks-sdk",
            "litellm",
            "pyyaml",
            "pydantic>=2",  # Required for ResponsesAgent types
            {custom_tools_pip}
        ]
    )

    print(f"✅ Model logged: {{model_info.model_uri}}")
    model_uri = model_info.model_uri

# Step 4: Register to Unity Catalog
print("\\n🏷️  Registering model to Unity Catalog...")

uc_model_name = f"{{CATALOG_NAME}}.{{SCHEMA_NAME}}.{{MODEL_NAME}}"

try:
    registered_model = mlflow.register_model(
        model_uri=model_uri,
        name=uc_model_name
    )

    model_version = registered_model.version
    print(f"✅ Model registered: {{uc_model_name}} (version {{model_version}})")

    # Step 5: Deploy to Model Serving
    print("\\n🚢 Deploying to Model Serving endpoint...")

    deployment = agents.deploy(
        model_name=uc_model_name,
        model_version=model_version,
        scale_to_zero=True  # Enable auto-scaling to zero when idle
    )

    endpoint_name = deployment.endpoint_name  # Use attribute, not dict access
    print(f"\\n✅ Deployment successful!")
    print(f"   Endpoint: {{endpoint_name}}")

    # Step 6: Example API query
    print("\\n💡 Example API Query:")
    print(f"""
# Using Databricks SDK (ResponsesAgentRequest format)
from databricks.sdk import WorkspaceClient
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentMessage

w = WorkspaceClient()

request = ResponsesAgentRequest(
    messages=[ResponsesAgentMessage(role="user", content="Quantum Computing applications")]
)

response = w.serving_endpoints.query(
    name="{{endpoint_name}}",
    inputs=request.to_dict()
)

print(response.predictions)

# Or using REST API with token
import requests

DATABRICKS_HOST = "https://your-workspace.cloud.databricks.com"
DATABRICKS_TOKEN = "your-token"

response = requests.post(
    f"{{{{DATABRICKS_HOST}}}}/serving-endpoints/{{{{endpoint_name}}}}/invocations",
    headers={{{{
        "Authorization": f"Bearer {{{{DATABRICKS_TOKEN}}}}",
        "Content-Type": "application/json"
    }}}},
    json={{{{
        "messages": [
            {{{{"role": "user", "content": "Quantum Computing applications"}}}}
        ]
    }}}}
)

print(response.json())
""")

except Exception as e:
    print(f"\\n❌ Deployment failed: {{str(e)}}")
    print("\\nTroubleshooting:")
    print("   1. Ensure you have CREATE MODEL permissions in the Unity Catalog")
    print("   2. Verify the catalog and schema exist")
    print("   3. Check that required resources (serving endpoints) are accessible")
    print("   4. Review the error message for specific issues")

print("\\n📚 Documentation:")
print("   - Databricks Agent Framework: https://docs.databricks.com/en/generative-ai/agent-framework/deploy-agent.html")
print("   - Unity Catalog Models: https://docs.databricks.com/en/machine-learning/manage-model-lifecycle/index.html")
print("   - MLflow 3.x ResponsesAgent: https://mlflow.org/docs/latest/python_api/mlflow.pyfunc.html#mlflow.pyfunc.ResponsesAgent")
print("   - Agent Schema: https://docs.databricks.com/en/generative-ai/agent-framework/agent-schema.html")
'''.format(
            crew_name=crew_name,
            agent_code=agent_code_template,
            custom_tools_message=custom_tools_message,
            custom_tools_comment=custom_tools_comment,
            custom_tools_note=custom_tools_note,
            custom_tools_pip=custom_tools_pip
        )

    def _generate_usage_examples(self, crew_name: str) -> str:
        """Generate usage examples"""
        return f'''## 📖 Usage Examples

### Run with Different Inputs
```python
# Example 1: Different topic
result = run_crew(topic="Quantum Computing applications")

# Example 2: Multiple executions
topics = [
    "Machine Learning in Healthcare",
    "Blockchain Technology",
    "Sustainable Energy Solutions"
]

for topic in topics:
    print(f"\\n{{'='*70}}")
    print(f"Processing: {{topic}}")
    print(f"{{'='*70}}\\n")
    result = run_crew(topic=topic)
```

### Access Task Outputs
```python
# Get individual task outputs
crew_instance = {crew_name.title().replace('_', '')}Crew()
executed_crew = crew_instance.crew()
result = executed_crew.kickoff(inputs={{'topic': 'Your Topic'}})

# Access task outputs
for task_output in executed_crew.tasks_output:
    print(f"Task: {{task_output.description[:50]}}")
    print(f"Output: {{task_output.raw}}\\n")
```

### Save Output to Databricks Table
```python
# Save results to Delta table
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

results_df = spark.createDataFrame([
    {{"topic": inputs['topic'], "result": str(result), "timestamp": datetime.now()}}
])

results_df.write.mode("append").saveAsTable("crew_execution_results")
```

---

**Generated by Kasal Platform** | [Documentation](https://github.com/your-org/kasal)'''

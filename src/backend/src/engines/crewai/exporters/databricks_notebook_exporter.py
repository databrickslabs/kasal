"""
Databricks Notebook exporter for CrewAI crews.
"""

from typing import Dict, Any, List
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
        model_override = options.get('model_override')

        # Get all tools used
        tools = self._get_unique_tools(agents, tasks)
        logger.info(f"[Export Debug] All tools found: {tools}")

        # Generate notebook cells
        cells = []

        # 1. Title cell (markdown)
        cells.append(self._create_markdown_cell(
            self._generate_title_markdown(crew_name, agents, tasks)
        ))

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

        # 5b. MLflow configuration (code)
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
            for_notebook=True
        )
        cells.append(self._create_code_cell(main_code))

        # 16. MLflow tracking info (markdown)
        cells.append(self._create_markdown_cell(
            "## 📊 MLflow Tracking\n\n"
            "Click the **Experiment** icon in the notebook toolbar to view tracked runs, metrics, and artifacts."
        ))

        # 17. Evaluation section (markdown)
        cells.append(self._create_markdown_cell(
            "## 📈 Evaluation\n\n"
            "Evaluate your crew's performance using MLflow evaluation metrics."
        ))

        # 18. Evaluation code (code)
        cells.append(self._create_code_cell(
            self._generate_evaluation_code(sanitized_name)
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
    print(f"\\n✅ Found {{{{len(runs_df)}}}} recent runs:")
    for idx, row in runs_df.head().iterrows():
        print(f"   {{{{idx+1}}}}. Run ID: {{{{row['run_id'][:8]}}}}... | Started: {{{{row['start_time']}}}}")

    # Use the most recent run
    latest_run_id = runs_df.iloc[0]["run_id"]
    latest_run = mlflow.get_run(latest_run_id)

    print(f"\\n📊 Using latest run: {{{{latest_run_id}}}}")
    print(f"   - Experiment: {{{{latest_run.info.experiment_id}}}}")
    print(f"   - Status: {{{{latest_run.info.status}}}}")

    # Create evaluation dataset
    # You can customize the ground truth and expected outputs based on your use case
    eval_data = pd.DataFrame({{{{
        "inputs": [
            "Artificial Intelligence trends in 2025"
        ],
        "ground_truth": [
            "A comprehensive analysis covering AI trends, including generative AI, large language models, multimodal AI, AI safety, and practical applications across industries."
        ]
    }}}})

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
        metrics = [
            genai.answer_relevance(),
            genai.answer_correctness(),
        ]

        # Run evaluation
        eval_results = mlflow.evaluate(
            model=crew_model,
            data=eval_data,
            targets="ground_truth",
            model_type="text",
            evaluators="default",
            extra_metrics=metrics
        )

        print("\\n✅ Evaluation complete!")
        print(f"\\n📊 Evaluation Results:")
        print(f"   - Metrics: {{{{eval_results.metrics}}}}")

        # Display evaluation results table
        print("\\n📈 Detailed Results:")
        display(eval_results.tables['eval_results_table'])

        # Log custom metrics to the original run
        with mlflow.start_run(run_id=latest_run_id):
            mlflow.log_metrics({{{{
                "custom_evaluation_score": 0.95,  # Replace with your actual scoring logic
                "task_completion_rate": 1.0
            }}}})
            print("\\n✅ Custom metrics logged to MLflow run")

    except Exception as e:
        print(f"\\n❌ Evaluation failed: {{{{str(e)}}}}")
        print("\\nNote: Make sure you have the required evaluation dependencies:")
        print("   %pip install mlflow[genai] openai")

else:
    print("❌ No runs found in MLflow.")
    print("\\nPlease execute the crew first by running the 'Execute the Crew' cell above.")
    print("\\nIf you just executed it, the run might still be registering. Wait a moment and try again.")

print("\\n💡 Tip: You can view detailed results in the MLflow UI")
print("   Click the 'Experiment' icon in the notebook toolbar")'''

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

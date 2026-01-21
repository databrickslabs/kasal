"""
Python Project exporter for CrewAI crews.
"""

from typing import Dict, Any, List
from .base_exporter import BaseExporter
from .yaml_generator import YAMLGenerator
from .code_generator import CodeGenerator


class PythonProjectExporter(BaseExporter):
    """Export crew as a standalone Python project"""

    def __init__(self):
        super().__init__()
        self.yaml_generator = YAMLGenerator()
        self.code_generator = CodeGenerator()

    async def export(self, crew_data: Dict[str, Any], options: Dict[str, Any]) -> Dict[str, Any]:
        """
        Export crew as Python project with standard CrewAI structure

        Args:
            crew_data: Crew configuration data
            options: Export options

        Returns:
            Dictionary with files list and metadata
        """
        crew_name = crew_data.get('name', 'crew')
        sanitized_name = self._sanitize_name(crew_name)
        agents = crew_data.get('agents', [])
        tasks = crew_data.get('tasks', [])

        # Extract options
        include_custom_tools = options.get('include_custom_tools', True)
        include_comments = options.get('include_comments', True)
        include_tests = options.get('include_tests', True)
        model_override = options.get('model_override')

        # Get all tools used
        tools = self._get_unique_tools(agents, tasks)

        # Generate files
        files = []

        # 1. README.md
        readme = self._generate_readme(crew_name, sanitized_name, agents, tasks)
        files.append({
            'path': 'README.md',
            'content': readme,
            'type': 'markdown'
        })

        # 2. requirements.txt
        requirements = self._generate_requirements(tools)
        files.append({
            'path': 'requirements.txt',
            'content': requirements,
            'type': 'text'
        })

        # 3. .env.example
        env_example = self._generate_env_example()
        files.append({
            'path': '.env.example',
            'content': env_example,
            'type': 'text'
        })

        # 4. .gitignore
        gitignore = self._generate_gitignore()
        files.append({
            'path': '.gitignore',
            'content': gitignore,
            'type': 'text'
        })

        # 5. src/{crew_name}/__init__.py
        init_py = f'"""{crew_name.replace("_", " ").title()} CrewAI Project"""\n\n__version__ = "0.1.0"\n'
        files.append({
            'path': f'src/{sanitized_name}/__init__.py',
            'content': init_py,
            'type': 'python'
        })

        # 6. src/{crew_name}/config/agents.yaml
        agents_yaml = self.yaml_generator.generate_agents_yaml(
            agents,
            model_override=model_override,
            include_comments=include_comments
        )
        files.append({
            'path': f'src/{sanitized_name}/config/agents.yaml',
            'content': agents_yaml,
            'type': 'yaml'
        })

        # 7. src/{crew_name}/config/tasks.yaml
        tasks_yaml = self.yaml_generator.generate_tasks_yaml(
            tasks,
            agents,
            include_comments=include_comments
        )
        files.append({
            'path': f'src/{sanitized_name}/config/tasks.yaml',
            'content': tasks_yaml,
            'type': 'yaml'
        })

        # 8. src/{crew_name}/crew.py
        crew_code = self.code_generator.generate_crew_code(
            sanitized_name,
            agents,
            tasks,
            tools,
            process_type='sequential',
            include_comments=include_comments,
            for_notebook=False
        )
        files.append({
            'path': f'src/{sanitized_name}/crew.py',
            'content': crew_code,
            'type': 'python'
        })

        # 9. src/{crew_name}/main.py
        main_code = self.code_generator.generate_main_code(
            sanitized_name,
            sample_inputs={'topic': 'Your research topic here'},
            include_comments=include_comments,
            for_notebook=False
        )
        files.append({
            'path': f'src/{sanitized_name}/main.py',
            'content': main_code,
            'type': 'python'
        })

        # 10. src/{crew_name}/tools/__init__.py (if custom tools)
        if include_custom_tools and any(tool not in ['SerperDevTool', 'ScrapeWebsiteTool', 'DallETool'] for tool in tools):
            tools_init = '"""Custom tools for the crew"""\n\n# Import your custom tools here\n'
            files.append({
                'path': f'src/{sanitized_name}/tools/__init__.py',
                'content': tools_init,
                'type': 'python'
            })

        # 11. tests/test_crew.py (if include_tests)
        if include_tests:
            test_code = self._generate_test_code(sanitized_name)
            files.append({
                'path': 'tests/test_crew.py',
                'content': test_code,
                'type': 'python'
            })

            # tests/__init__.py
            files.append({
                'path': 'tests/__init__.py',
                'content': '"""Tests for the crew"""\n',
                'type': 'python'
            })

        # 12. output/.gitkeep (to preserve output directory)
        files.append({
            'path': 'output/.gitkeep',
            'content': '',
            'type': 'text'
        })

        return {
            'crew_id': str(crew_data.get('id', '')),
            'crew_name': crew_name,
            'export_format': 'python_project',
            'files': files,
            'metadata': {
                'agents_count': len(agents),
                'tasks_count': len(tasks),
                'tools_count': len(tools),
                'sanitized_name': sanitized_name,
            },
            'generated_at': self._get_timestamp()
        }

    def _generate_readme(
        self,
        crew_name: str,
        sanitized_name: str,
        agents: List[Dict[str, Any]],
        tasks: List[Dict[str, Any]]
    ) -> str:
        """Generate README.md content"""
        readme = f"""# {crew_name.replace('_', ' ').title()}

AI agent crew for task execution using CrewAI framework.

## Overview

This project contains a complete CrewAI crew implementation with:
- **{len(agents)} Agent(s)**: {', '.join(a.get('name', 'Agent') for a in agents[:3])}{'...' if len(agents) > 3 else ''}
- **{len(tasks)} Task(s)**: {', '.join(t.get('name', 'Task') for t in tasks[:3])}{'...' if len(tasks) > 3 else ''}

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Copy the example environment file and configure your API keys:

```bash
cp .env.example .env
# Edit .env with your API keys
```

### 3. Run the Crew

```bash
python src/{sanitized_name}/main.py
```

## Project Structure

```
{sanitized_name}/
├── src/{sanitized_name}/
│   ├── config/
│   │   ├── agents.yaml      # Agent configurations
│   │   └── tasks.yaml       # Task configurations
│   ├── tools/               # Custom tools (if any)
│   ├── crew.py              # Crew definition
│   └── main.py              # Entry point
├── tests/                   # Unit tests
├── output/                  # Crew execution output
├── requirements.txt         # Python dependencies
└── .env                     # Environment variables (not in git)
```

## Configuration

### Agents

Agents are defined in `src/{sanitized_name}/config/agents.yaml`. Each agent has:
- **Role**: The function the agent performs
- **Goal**: What the agent aims to achieve
- **Backstory**: Context that shapes agent behavior
- **LLM**: The language model to use

### Tasks

Tasks are defined in `src/{sanitized_name}/config/tasks.yaml`. Each task has:
- **Description**: What needs to be done
- **Expected Output**: Format and content of the result
- **Agent**: Which agent handles this task
- **Context**: Dependencies on other tasks

## Customization

### Modify Agent Behavior

Edit `src/{sanitized_name}/config/agents.yaml` to change agent roles, goals, or backstories.

### Change Task Flow

Edit `src/{sanitized_name}/config/tasks.yaml` to modify task descriptions or dependencies.

### Add Custom Tools

1. Create tool class in `src/{sanitized_name}/tools/`
2. Import in `crew.py`
3. Add to agent's tools list

## Running Tests

```bash
pytest tests/
```

## Output

Crew execution results are saved to the `output/` directory.

## Exported from Kasal

- **Generated**: {self._get_timestamp()}
- **Source**: Kasal Platform

## Documentation

- [CrewAI Documentation](https://docs.crewai.com/)
- [CrewAI GitHub](https://github.com/crewaiinc/crewai)

## License

This project is exported from Kasal and follows your organization's licensing terms.
"""
        return readme

    def _generate_requirements(self, tools: List[str]) -> str:
        """Generate requirements.txt content"""
        requirements = [
            'crewai>=0.80.0',
            'crewai-tools>=0.12.0',
            'pydantic>=2.0.0',
            'python-dotenv>=1.0.0',
        ]

        # Add tool-specific dependencies
        if 'SerperDevTool' in tools:
            requirements.append('# SerperDevTool (already included in crewai-tools)')

        return '\n'.join(requirements) + '\n'

    def _generate_env_example(self) -> str:
        """Generate .env.example content"""
        return """# LLM Configuration
# Databricks Configuration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token

# Tool API Keys
SERPER_API_KEY=your-serper-dev-api-key
OPENAI_API_KEY=your-openai-api-key

# Optional: Override default LLM model
# DEFAULT_LLM_MODEL=databricks-llama-4-maverick
"""

    def _generate_gitignore(self) -> str:
        """Generate .gitignore content"""
        return """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual Environment
venv/
ENV/
env/

# Environment Variables
.env

# IDE
.vscode/
.idea/
*.swp
*.swo

# Output
output/*
!output/.gitkeep

# Testing
.pytest_cache/
.coverage
htmlcov/

# OS
.DS_Store
Thumbs.db
"""

    def _generate_test_code(self, sanitized_name: str) -> str:
        """Generate test_crew.py content"""
        class_name = ''.join(word.capitalize() for word in sanitized_name.split('_'))
        if not class_name.endswith('Crew'):
            class_name += 'Crew'

        return f"""\"\"\"
Unit tests for {sanitized_name}
\"\"\"

import pytest
from {sanitized_name}.crew import {class_name}


def test_crew_initialization():
    \"\"\"Test that the crew initializes correctly\"\"\"
    crew_instance = {class_name}()
    assert crew_instance is not None


def test_agents_defined():
    \"\"\"Test that all agents are properly defined\"\"\"
    crew_instance = {class_name}()
    crew = crew_instance.crew()
    assert len(crew.agents) > 0


def test_tasks_defined():
    \"\"\"Test that all tasks are properly defined\"\"\"
    crew_instance = {class_name}()
    crew = crew_instance.crew()
    assert len(crew.tasks) > 0


@pytest.mark.integration
def test_crew_execution():
    \"\"\"Test full crew execution (integration test)\"\"\"
    crew_instance = {class_name}()
    inputs = {{'topic': 'Test topic'}}

    # This test requires valid API keys
    # Skip if not available
    try:
        result = crew_instance.crew().kickoff(inputs=inputs)
        assert result is not None
    except Exception as e:
        pytest.skip(f"Integration test skipped: {{e}}")
"""

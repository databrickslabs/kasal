# Database Seeding

This document explains the database seeding functionality in Kasal, which allows for automatic population of predefined data essential for AI agent workflow orchestration into database tables.

## Overview

Database seeding is the process of initializing a database with a set of predefined data essential for Kasal's operation. This is particularly useful for:

- Populating tool configurations for AI agents (search tools, file operations, APIs)
- Setting up default LLM model configurations (OpenAI, Anthropic, Databricks, etc.)
- Providing prompt templates for agent and task generation
- Initializing JSON schemas for data validation
- Setting up development and testing environments with realistic data
- Ensuring required reference data is available for AI engine operations

The seeding system is designed to be:

- **Idempotent**: Can be run multiple times without creating duplicates
- **Modular**: Each type of data has its own seeder module
- **Configurable**: Can be enabled/disabled through environment variables
- **Flexible**: Supports both sync and async execution patterns
- **Resilient**: Continues seeding even if one seeder fails

## Available Seeders

Kasal includes the following seeders that provide essential data for AI agent operations:

### 1. Tools Seeder

Populates the `tools` table with default tool configurations available to AI agents.

- **Module**: `backend/src/seeds/tools.py`
- **Command**: `python -m src.seeds.seed_runner --tools`
- **Data**: Includes CrewAI built-in tools, custom Kasal tools (Genie, Python PPTX, Perplexity), and MCP integrations
- **Key tools**: Search tools, file operations, web scraping, data analysis, presentation generation

### 2. Schemas Seeder

Populates the `schemas` table with predefined JSON schemas for data validation.

- **Module**: `backend/src/seeds/schemas.py`
- **Command**: `python -m src.seeds.seed_runner --schemas`
- **Data**: Validation schemas for agent configurations, task definitions, workflow structures

### 3. Prompt Templates Seeder

Populates the `template` table with default prompt templates for AI generation tasks.

- **Module**: `backend/src/seeds/prompt_templates.py`
- **Command**: `python -m src.seeds.seed_runner --prompt_templates`
- **Data**: Templates for agent generation, task creation, crew planning, and workflow optimization

### 4. Model Configurations Seeder

Populates the `model_config` table with configurations for various LLM models from different providers.

- **Module**: `backend/src/seeds/model_configs.py`
- **Command**: `python -m src.seeds.seed_runner --model_configs`
- **Data**: OpenAI models (GPT-4, GPT-3.5), Anthropic models (Claude), Databricks models, and local model configurations

### 5. Roles Seeder

Populates the `roles` and `privileges` tables with default RBAC configuration.

- **Module**: `backend/src/seeds/roles.py`
- **Command**: `python -m src.seeds.seed_runner --roles`
- **Data**: Admin, Technical, and Regular user roles with appropriate privileges

### 6. Documentation Seeder

Populates the `documentation_embedding` table with vectorized documentation for AI agent reference.

- **Module**: `backend/src/seeds/documentation.py`
- **Command**: `python -m src.seeds.seed_runner --documentation`
- **Data**: Embedded documentation chunks for context-aware agent assistance

## Running Seeders

There are multiple ways to run the seeders in Kasal:

### Using the Seed Runner Script

The dedicated seed runner script provides the simplest interface:

```bash
# Navigate to backend directory
cd backend

# Run all seeders using the convenience script
python run_seeders.py

# Or use the module directly
python -m src.seeds.seed_runner --all

# Run specific seeders
python -m src.seeds.seed_runner --tools
python -m src.seeds.seed_runner --schemas
python -m src.seeds.seed_runner --prompt_templates
python -m src.seeds.seed_runner --model_configs
python -m src.seeds.seed_runner --roles
python -m src.seeds.seed_runner --documentation

# Run multiple specific seeders
python -m src.seeds.seed_runner --tools --schemas --model_configs

# Run with debug mode enabled
python -m src.seeds.seed_runner --all --debug
```

### Automatic Seeding on Application Startup

Kasal automatically runs all seeders during the FastAPI application startup process. This ensures that all required data (tools, models, templates) is available before the application starts serving requests.

Automatic seeding is enabled by default but can be controlled through the application configuration:

```python
# In backend/src/config/settings.py
class Settings(BaseSettings):
    # Database seeding configuration
    AUTO_SEED_DATABASE: bool = True
    SEED_DEBUG: bool = False
```

You can override these settings using environment variables:

```bash
# Enable/disable automatic seeding
AUTO_SEED_DATABASE=true   # Enable (default)
AUTO_SEED_DATABASE=false  # Disable

# Enable debug logging for seeding
SEED_DEBUG=true
```

#### Seeding Process Flow

1. When Kasal starts, the FastAPI lifespan context manager initializes the database connection
2. After successful database initialization, it checks if `AUTO_SEED_DATABASE` is enabled
3. If enabled, it imports the seed runner and executes all registered seeders in order:
   - Roles and privileges (for RBAC)
   - Model configurations (for LLM providers)
   - Tools (for agent capabilities)
   - Prompt templates (for generation)
   - Schemas (for validation)
   - Documentation embeddings (for context)
4. Each seeder runs independently - if one fails, others will still be executed
5. Detailed logs are generated throughout the process for monitoring and debugging
6. The application only starts serving requests after successful seeding

### Debugging Seeding

To enable detailed debug logging for the seeding process, set the `SEED_DEBUG` environment variable:

```bash
SEED_DEBUG=true  # Enable detailed seeding debug logs
```

This will output comprehensive information about:
- Which seeders are being loaded and executed
- Data being inserted or updated (tools, models, templates)
- When each seeder starts and completes
- Any errors that occur during the seeding process
- Database connection and transaction details

You can also enable debug mode programmatically:

```python
# In backend/src/main.py
import os
os.environ["SEED_DEBUG"] = "true"

# Or in the settings configuration
class Settings(BaseSettings):
    SEED_DEBUG: bool = True
```

## How Seeders Work

Each Kasal seeder follows a consistent pattern designed for reliability and idempotency:

1. Define the default data to be seeded (tools, models, templates, etc.)
2. Provide both async and sync implementations for different execution contexts
3. Check for existing records to avoid duplicates (using unique keys like tool names, model IDs)
4. Insert new records and update existing ones as needed
5. Handle Kasal-specific data structures (JSON configurations, YAML templates)
6. Log the results of the seeding operation with detailed statistics
7. Ensure data integrity with proper transaction handling

### Example Seeder Structure

```python
from datetime import datetime
from sqlalchemy import select
from src.db.session import async_session_factory
from src.models.tool import Tool
from src.core.logger import logger

# Define default tools for AI agents
DEFAULT_TOOLS = {
    "web_search": {
        "name": "Web Search",
        "description": "Search the web for information",
        "tool_type": "search",
        "configuration": {
            "provider": "serper",
            "max_results": 10
        },
        "requires_api_key": True,
        "is_active": True
    },
    "genie_tool": {
        "name": "Databricks Genie",
        "description": "Query Databricks data using natural language",
        "tool_type": "custom",
        "configuration": {
            "space_id": None,  # To be configured by user
            "timeout": 30
        },
        "requires_api_key": True,
        "is_active": True
    }
}

async def seed_async():
    """Seed tools into the database using async session."""
    async with async_session_factory() as session:
        # Check existing tools
        result = await session.execute(select(Tool.name))
        existing_tools = {row[0] for row in result.fetchall()}
        
        # Insert/update tools
        tools_added = 0
        tools_updated = 0
        
        for tool_key, tool_data in DEFAULT_TOOLS.items():
            if tool_data["name"] not in existing_tools:
                # Add new tool
                tool = Tool(
                    id=generate_uuid(),
                    **tool_data,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                session.add(tool)
                tools_added += 1
                logger.debug(f"Adding new tool: {tool_data['name']}")
            else:
                # Update existing tool if needed
                result = await session.execute(
                    select(Tool).filter(Tool.name == tool_data["name"])
                )
                existing_tool = result.scalars().first()
                if existing_tool:
                    # Update configuration if changed
                    existing_tool.configuration = tool_data["configuration"]
                    existing_tool.updated_at = datetime.utcnow()
                    tools_updated += 1
                    logger.debug(f"Updated tool: {tool_data['name']}")
        
        # Commit changes
        if tools_added > 0 or tools_updated > 0:
            await session.commit()
            logger.info(f"Tools seeding completed: {tools_added} added, {tools_updated} updated")
        else:
            logger.info("No tools needed to be added or updated")

# Main entry point for the seeder
async def seed():
    """Main entry point for tools seeding."""
    try:
        await seed_async()
        logger.info("Tools seeding completed successfully")
    except Exception as e:
        logger.error(f"Error in tools seeding: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Continue with other seeders even if this one fails
```

## Creating a New Seeder

To create a new seeder for additional Kasal data:

1. Create a new file in the `backend/src/seeds/` directory (e.g., `custom_tools.py`)
2. Implement the `seed_async()` function following the established pattern
3. Provide a main `seed()` function that handles errors gracefully
4. Define your data structure with proper Kasal-specific fields
5. Update the `seed_runner.py` file to include your new seeder

Example of adding a new seeder to `seed_runner.py`:

```python
# In backend/src/seeds/seed_runner.py

# Add your import
from src.seeds import (
    tools, 
    schemas, 
    prompt_templates, 
    model_configs, 
    roles,
    documentation,
    your_custom_seeder  # Your new seeder
)

# Add to the SEEDERS dictionary
try:
    SEEDERS["custom_data"] = your_custom_seeder.seed
    debug_log("Added your_custom_seeder.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding your_custom_seeder: {e}")

# Add command line argument
parser.add_argument('--custom_data', action='store_true', help='Seed custom data')
```

## Best Practices

When using or extending the Kasal seeding functionality:

1. **Maintain idempotency**: Always check for existing records before inserting (use unique names/keys)
2. **Use appropriate timestamps**: Set created_at and updated_at fields with UTC time
3. **Handle errors gracefully**: Use try/except blocks and continue seeding other data on failure
4. **Validate data**: Ensure seeded data follows Kasal's schema requirements
5. **Keep seed data manageable**: Split large datasets into logical modules (tools, models, templates)
6. **Document seed data**: Include comments explaining the purpose and usage of seeded items
7. **Test seeders**: Ensure they run correctly in both SQLite (dev) and PostgreSQL (prod) environments
8. **Add proper logging**: Use structured logging to track the seeding process and results
9. **Use proper UUIDs**: Generate proper UUID strings for entity IDs
10. **Consider dependencies**: Seed data that other seeders depend on first (roles before users)
11. **Handle JSON data**: Properly serialize complex configurations and tool parameters
12. **Version your seed data**: Consider versioning when updating existing seed data

## Troubleshooting

If you encounter issues with the Kasal seeding process:

1. **Enable debug mode**: Set `SEED_DEBUG=true` to get more detailed logs
2. **Check database connectivity**: Ensure the database is accessible and properly configured
3. **Verify model definitions**: Make sure SQLAlchemy model definitions match the data being seeded
4. **Check data integrity**: Ensure seeded data follows required constraints (unique names, valid JSON)
5. **Inspect logs**: Check `backend/logs/` directory for specific error messages
6. **Run seeders manually**: Try running individual seeders to isolate issues
7. **Validate JSON configurations**: Ensure tool configurations and model configs are valid JSON
8. **Check foreign key constraints**: Ensure referenced entities exist before seeding dependent data
9. **Database migrations**: Ensure all migrations are applied before seeding
10. **Environment variables**: Verify all required environment variables are set for external services

## Environment-Specific Considerations

### Development Environment
- **Enable auto-seeding**: Set `AUTO_SEED_DATABASE=true` for convenience during development
- **Use SQLite**: Seeders work with both SQLite and PostgreSQL
- **Enable debug mode**: Set `SEED_DEBUG=true` to see detailed seeding information

### Testing Environment
- **Use seeders for test data**: Create isolated test databases with seeded data
- **Reset between tests**: Clear and re-seed data for consistent test environments
- **Mock external dependencies**: Use test configurations for external API tools

### Production Environment (Databricks Apps)
- **Use seeders carefully**: Typically only for initial setup or essential reference data
- **Disable debug mode**: Set `SEED_DEBUG=false` to reduce log noise
- **Monitor seeding performance**: Large seed datasets can impact startup time
- **Version control seed data**: Track changes to production seed data

### CI/CD Pipeline
- **Include seeding in deployment**: Run seeders as part of your deployment process
- **Handle seeding failures**: Ensure deployment continues even if non-critical seeding fails
- **Environment-specific data**: Use different seed data for different deployment environments
- **Database migrations first**: Always run migrations before seeding

## Integration with Kasal Components

### Tools Integration
Seeded tools are immediately available in:
- Agent configuration dialogs
- Tool selection interfaces
- CrewAI engine tool registry
- MCP server configurations

### Model Configuration Integration
Seeded model configurations provide:
- Default LLM options in UI dropdowns
- Fallback configurations for agent execution
- Provider-specific parameter templates
- Cost and performance baselines

### Template Integration
Seeded prompt templates enable:
- AI-powered agent generation
- Task creation assistance
- Workflow optimization suggestions
- Context-aware prompt improvements

### RBAC Integration
Seeded roles and privileges establish:
- Default user permission structure
- Admin capabilities for system management
- Technical user access for workflow creation
- Regular user permissions for execution only 
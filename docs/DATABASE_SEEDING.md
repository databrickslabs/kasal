# Database Seeding

This document explains the database seeding functionality in the application, which allows for automatic population of predefined data into database tables.

## Overview

Database seeding is the process of initializing a database with a set of predefined data. This is particularly useful for:

- Populating lookup tables with standard values
- Setting up development and testing environments
- Ensuring required reference data is available
- Initializing the application with demo data

The seeding system is designed to be:

- **Idempotent**: Can be run multiple times without creating duplicates
- **Modular**: Each type of data has its own seeder module
- **Configurable**: Can be enabled/disabled through environment variables
- **Flexible**: Supports both sync and async execution patterns
- **Resilient**: Continues seeding even if one seeder fails

## Available Seeders

The application includes the following seeders:

### 1. Tools Seeder

Populates the `tools` table with default tool configurations for AI agents.

- **Module**: `src/seeds/tools.py`
- **Command**: `python -m src.seeds.seed_runner --tools`

### 2. Schemas Seeder

Populates the `schemas` table with predefined JSON schemas.

- **Module**: `src/seeds/schemas.py`
- **Command**: `python -m src.seeds.seed_runner --schemas`

### 3. Prompt Templates Seeder

Populates the `prompt_templates` table with default prompt templates for various generation tasks.

- **Module**: `src/seeds/prompt_templates.py`
- **Command**: `python -m src.seeds.seed_runner --prompt_templates`

### 4. Model Configurations Seeder

Populates the `model_configs` table with configurations for various LLM models from different providers.

- **Module**: `src/seeds/model_configs.py`
- **Command**: `python -m src.seeds.seed_runner --model_configs`

## Running Seeders

There are multiple ways to run the seeders:

### Using the Seed Runner

The `seed_runner` module provides a command-line interface for running seeders:

```bash
# Run all seeders
python -m src.seeds.seed_runner --all

# Run a specific seeder
python -m src.seeds.seed_runner --tools
python -m src.seeds.seed_runner --schemas
python -m src.seeds.seed_runner --prompt_templates
python -m src.seeds.seed_runner --model_configs

# Run multiple specific seeders
python -m src.seeds.seed_runner --tools --schemas

# Run with debug mode enabled
python -m src.seeds.seed_runner --all --debug
```

### Automatic Seeding on Application Startup

The application automatically runs all seeders during the application startup process in the FastAPI lifespan context manager. This ensures that all required data is available before the application starts serving requests.

Seeding is controlled by the `AUTO_SEED_DATABASE` setting in `src/config/settings.py`:

```python
# Add the following setting to control database seeding
AUTO_SEED_DATABASE: bool = True
```

You can override this setting using environment variables:

```
AUTO_SEED_DATABASE=true   # Enable automatic seeding
AUTO_SEED_DATABASE=false  # Disable automatic seeding
```

#### Seeding Process Flow

1. When the application starts, the lifespan context manager initializes the database
2. After successful database initialization, it checks if seeding is enabled
3. If enabled, it imports the seed runner and executes all registered seeders
4. Each seeder runs independently - if one fails, others will still be executed
5. Detailed logs are generated throughout the process

### Debugging Seeding

To enable detailed debug logging for the seeding process, set the `SEED_DEBUG` environment variable:

```
SEED_DEBUG=true  # Enable detailed seeding debug logs
```

This will output comprehensive information about:
- Which seeders are being loaded
- When each seeder starts and completes
- Any errors that occur during the seeding process

You can also enable debug mode in the application code:

```python
# In main.py
import os
os.environ["SEED_DEBUG"] = "True"
```

## How Seeders Work

Each seeder follows a consistent pattern:

1. Define the default data to be seeded
2. Provide both async and sync implementations
3. Check for existing records to avoid duplicates
4. Insert new records and update existing ones as needed
5. Log the results of the seeding operation

### Example Seeder Structure

```python
async def seed_async():
    """Seed data into the database using async session."""
    async with async_session_factory() as session:
        # Check existing records
        result = await session.execute(select(Model.key))
        existing_keys = {row[0] for row in result.scalars().all()}
        
        # Insert/update records
        items_added = 0
        items_updated = 0
        
        for item_key, item_data in DEFAULT_ITEMS.items():
            if item_key not in existing_keys:
                # Add new item
                session.add(Model(**item_data))
                items_added += 1
            else:
                # Update existing item
                result = await session.execute(
                    select(Model).filter(Model.key == item_key)
                )
                existing_item = result.scalars().first()
                # Update fields...
                items_updated += 1
        
        # Commit changes
        if items_added > 0 or items_updated > 0:
            await session.commit()
            
def seed_sync():
    """Sync version of the seeder"""
    # Similar implementation using synchronous session

# Main entry point for the seeder
async def seed():
    """Main entry point for seeding."""
    try:
        await seed_async()
        logger.info("Seeding completed successfully")
    except Exception as e:
        logger.error(f"Error in seeding: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        # You can choose to raise the exception or not depending on your strategy

## Creating a New Seeder

To create a new seeder for additional data:

1. Create a new file in the `src/seeds/` directory
2. Implement the `seed_async()` and `seed_sync()` functions
3. Provide a main `seed()` function that calls the appropriate implementation
4. Update the `seed_runner.py` file to include your new seeder

Example of adding a new seeder to `seed_runner.py`:

```python
# Add your import
from src.seeds import tools, schemas, prompt_templates, model_configs, your_new_seeder

# Add to the SEEDERS dictionary
try:
    SEEDERS["your_new_seeder_name"] = your_new_seeder.seed
    debug_log("Added your_new_seeder.seed to SEEDERS")
except (NameError, AttributeError) as e:
    logger.error(f"Error adding your_new_seeder: {e}")
```

## Best Practices

When using or extending the seeding functionality:

1. **Maintain idempotency**: Always check for existing records before inserting
2. **Use appropriate timestamps**: Set created_at and updated_at fields with UTC time
3. **Handle errors gracefully**: Use try/except blocks and log errors
4. **Keep seed data manageable**: Split large datasets into logical modules
5. **Document seed data**: Include comments explaining the purpose of the seeded data
6. **Test seeders**: Ensure they run correctly in different environments
7. **Add proper logging**: Use logging to track the seeding process

## Troubleshooting

If you encounter issues with the seeding process:

1. **Enable debug mode**: Set `SEED_DEBUG=true` to get more detailed logs
2. **Check database connectivity**: Ensure the database is accessible
3. **Verify model definitions**: Make sure model definitions match the data being seeded
4. **Inspect logs**: Check system.log and application logs for specific error messages
5. **Run seeders manually**: Try running the seeders manually to isolate issues

## Environment-Specific Considerations

- **Development**: Enable auto-seeding for convenience
- **Testing**: Use seeders to create test data in a controlled manner
- **Production**: Use seeders with caution, typically only for initial setup or specific reference data
- **CI/CD**: Consider running seeders as part of your deployment pipeline 
"""
Seed the model_configs table with default model configuration definitions.
"""
import logging
from datetime import datetime
from sqlalchemy import select

from src.db.session import async_session_factory, SessionLocal
from src.models.model_config import ModelConfig

# Configure logging
logger = logging.getLogger(__name__)

# Define default model configurations
DEFAULT_MODELS = {
    "gpt-4-turbo": {
        "name": "gpt-4-turbo-preview",
        "temperature": 0.7,
        "provider": "openai",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "gpt-4o-mini": {
        "name": "gpt-4o-mini",
        "temperature": 0.7,
        "provider": "openai",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "o1-preview": {
        "name": "o1-preview",
        "temperature": 1,
        "provider": "openai",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "gpt-4o": {
        "name": "gpt-4o",
        "temperature": 0.7,
        "provider": "openai",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "gpt-4": {
        "name": "gpt-4",
        "temperature": 0.7,
        "provider": "openai",
        "context_window": 8192,
        "max_output_tokens": 4096
    },
    "gpt-3.5-turbo": {
        "name": "gpt-3.5-turbo",
        "temperature": 0.7,
        "provider": "openai",
        "context_window": 16385,
        "max_output_tokens": 4096
    },
    "gemini-2.0-flash": {
        "name": "gemini-2.0-flash",
        "temperature": 0.7,
        "provider": "gemini",
        "context_window": 1000000,
        "max_output_tokens": 4096
    },
    "claude-3-5-sonnet-20241022": {
        "name": "claude-3-5-sonnet-20241022",
        "temperature": 0.7,
        "provider": "anthropic",
        "context_window": 200000,
        "max_output_tokens": 4096
    },
    "claude-3-5-haiku-20241022": {
        "name": "claude-3-5-haiku-20241022",
        "temperature": 0.7,
        "provider": "anthropic",
        "context_window": 200000,
        "max_output_tokens": 4096
    },
    "claude-3-7-sonnet-20250219": {
        "name": "claude-3-7-sonnet-20250219",
        "temperature": 0.7,
        "provider": "anthropic",
        "context_window": 200000,
        "max_output_tokens": 4096
    },
    "claude-3-7-sonnet-20250219-thinking": {
        "name": "claude-3-7-sonnet-20250219",
        "temperature": 0.7,
        "provider": "anthropic",
        "extended_thinking": True,
        "context_window": 200000,
        "max_output_tokens": 4096
    },
    "claude-3-opus-20240229": {
        "name": "claude-3-opus-20240229",
        "temperature": 0.7,
        "provider": "anthropic",
        "context_window": 200000,
        "max_output_tokens": 4096
    },
    "llama3.2:latest": {
        "name": "llama3.2:latest",
        "temperature": 0.7,
        "provider": "ollama",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "databricks-llama-4-maverick": {
        "name": "databricks-llama-4-maverick",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "databricks-claude-sonnet-4": {
        "name": "databricks-claude-sonnet-4",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 200000,
        "max_output_tokens": 4096
    },
    "llama2:13b": {
        "name": "llama2:13b",
        "temperature": 0.7,
        "provider": "ollama",
        "context_window": 4096,
        "max_output_tokens": 4096
    },
    "qwen2.5:32b": {
        "name": "qwen2.5:32b",
        "temperature": 0.7,
        "provider": "ollama",
        "context_window": 32768,
        "max_output_tokens": 4096
    },
    "mistral-nemo:12b-instruct-2407-q2_K": {
        "name": "mistral-nemo:12b-instruct-2407-q2_K",
        "temperature": 0.7,
        "provider": "ollama",
        "context_window": 8192,
        "max_output_tokens": 4096
    },
    "databricks-meta-llama-3-3-70b-instruct": {
        "name": "databricks-meta-llama-3-3-70b-instruct",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "databricks-meta-llama-3-1-405b-instruct": {
        "name": "databricks-meta-llama-3-1-405b-instruct",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "databricks-claude-3-7-sonnet": {
        "name": "databricks-claude-3-7-sonnet",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 200000,
        "max_output_tokens": 4096
    },
    "llama3.2:3b-text-q8_0": {
        "name": "llama3.2:3b-text-q8_0",
        "temperature": 0.7,
        "provider": "ollama",
        "context_window": 8192,
        "max_output_tokens": 4096
    },
    "gemma2:27b": {
        "name": "gemma2:27b",
        "temperature": 0.7,
        "provider": "ollama",
        "context_window": 32768,
        "max_output_tokens": 4096
    },
    "deepseek-r1:32b": {
        "name": "deepseek-r1:32b",
        "temperature": 0.7,
        "provider": "ollama",
        "context_window": 32768,
        "max_output_tokens": 4096
    },
    "milkey/QwQ-32B-0305:q4_K_M": {
        "name": "milkey/QwQ-32B-0305:q4_K_M",
        "temperature": 0.7,
        "provider": "ollama",
        "context_window": 32768,
        "max_output_tokens": 4096
    },
    "deepseek-chat": {
        "name": "deepseek-chat",
        "temperature": 0.7,
        "provider": "deepseek",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "deepseek-reasoner": {
        "name": "deepseek-reasoner",
        "temperature": 0.7,
        "provider": "deepseek",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "deepseek-coder-v2": {
        "name": "deepseek-coder-v2",
        "temperature": 0.7,
        "provider": "deepseek",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "deepseek-v3": {
        "name": "deepseek-v3",
        "temperature": 0.7,
        "provider": "deepseek",
        "context_window": 128000,
        "max_output_tokens": 4096
    }
}

async def seed_async():
    """Seed model configurations into the database using async session."""
    logger.info("Seeding model_configs table (async)...")
    
    # Get existing model keys to avoid duplicates (outside the loop to reduce DB queries)
    async with async_session_factory() as session:
        result = await session.execute(select(ModelConfig.key))
        existing_keys = {row[0] for row in result.scalars().all()}
    
    # Insert new models
    models_added = 0
    models_updated = 0
    models_skipped = 0
    models_error = 0
    
    # Required fields for a valid model config
    required_fields = ["name", "temperature", "provider", "context_window", "max_output_tokens"]
    
    # Process each model individually with its own session to avoid transaction problems
    for model_key, model_data in DEFAULT_MODELS.items():
        try:
            # Validate model data structure
            missing_fields = [field for field in required_fields if field not in model_data]
            if missing_fields:
                logger.error(f"Model {model_key} is missing required fields: {missing_fields}")
                models_error += 1
                continue
                
            # Validate data types
            if not isinstance(model_data["temperature"], (int, float)):
                logger.error(f"Model {model_key}: temperature must be a number")
                models_error += 1
                continue
                
            if not isinstance(model_data["context_window"], int):
                logger.error(f"Model {model_key}: context_window must be an integer")
                models_error += 1
                continue
                
            if not isinstance(model_data["max_output_tokens"], int):
                logger.error(f"Model {model_key}: max_output_tokens must be an integer")
                models_error += 1
                continue
            
            # Create a fresh session for each model to avoid transaction conflicts
            async with async_session_factory() as session:
                if model_key not in existing_keys:
                    # Check again to be extra sure - this helps with race conditions
                    check_result = await session.execute(
                        select(ModelConfig).filter(ModelConfig.key == model_key)
                    )
                    existing_model = check_result.scalars().first()
                    
                    if existing_model:
                        # If it exists now (race condition), update it instead
                        existing_model.name = model_data["name"]
                        existing_model.provider = model_data["provider"]
                        existing_model.temperature = model_data["temperature"]
                        existing_model.context_window = model_data["context_window"]
                        existing_model.max_output_tokens = model_data["max_output_tokens"]
                        existing_model.extended_thinking = model_data.get("extended_thinking", False)
                        existing_model.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing model: {model_key}")
                        models_updated += 1
                    else:
                        # Add new model config
                        model_config = ModelConfig(
                            key=model_key,
                            name=model_data["name"],
                            provider=model_data["provider"],
                            temperature=model_data["temperature"],
                            context_window=model_data["context_window"],
                            max_output_tokens=model_data["max_output_tokens"],
                            extended_thinking=model_data.get("extended_thinking", False),
                            enabled=True,
                            created_at=datetime.now().replace(tzinfo=None),
                            updated_at=datetime.now().replace(tzinfo=None)
                        )
                        session.add(model_config)
                        logger.debug(f"Adding new model: {model_key}")
                        models_added += 1
                else:
                    # Update existing model config
                    result = await session.execute(
                        select(ModelConfig).filter(ModelConfig.key == model_key)
                    )
                    existing_model = result.scalars().first()
                    
                    if existing_model:
                        existing_model.name = model_data["name"]
                        existing_model.provider = model_data["provider"]
                        existing_model.temperature = model_data["temperature"]
                        existing_model.context_window = model_data["context_window"]
                        existing_model.max_output_tokens = model_data["max_output_tokens"]
                        existing_model.extended_thinking = model_data.get("extended_thinking", False)
                        existing_model.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing model: {model_key}")
                        models_updated += 1
                
                # Commit the session for this model
                try:
                    await session.commit()
                except Exception as e:
                    await session.rollback()
                    if "UNIQUE constraint failed" in str(e):
                        logger.warning(f"Model {model_key} already exists, skipping insert")
                        models_skipped += 1
                    else:
                        logger.error(f"Failed to commit model config for {model_key}: {str(e)}")
                        models_error += 1
        except Exception as e:
            logger.error(f"Error processing model {model_key}: {str(e)}")
            models_error += 1
    
    logger.info(f"Model configs seeding summary: Added {models_added}, Updated {models_updated}, Skipped {models_skipped}, Errors {models_error}")

def seed_sync():
    """Seed model configurations into the database using sync session."""
    logger.info("Seeding model_configs table (sync)...")
    
    # Get existing model keys to avoid duplicates (outside the loop to reduce DB queries)
    with SessionLocal() as session:
        result = session.execute(select(ModelConfig.key))
        existing_keys = {row[0] for row in result.scalars().all()}
    
    # Insert new models
    models_added = 0
    models_updated = 0
    models_skipped = 0
    models_error = 0
    
    # Required fields for a valid model config
    required_fields = ["name", "temperature", "provider", "context_window", "max_output_tokens"]
    
    # Process each model individually with its own session to avoid transaction problems
    for model_key, model_data in DEFAULT_MODELS.items():
        try:
            # Validate model data structure
            missing_fields = [field for field in required_fields if field not in model_data]
            if missing_fields:
                logger.error(f"Model {model_key} is missing required fields: {missing_fields}")
                models_error += 1
                continue
                
            # Validate data types
            if not isinstance(model_data["temperature"], (int, float)):
                logger.error(f"Model {model_key}: temperature must be a number")
                models_error += 1
                continue
                
            if not isinstance(model_data["context_window"], int):
                logger.error(f"Model {model_key}: context_window must be an integer")
                models_error += 1
                continue
                
            if not isinstance(model_data["max_output_tokens"], int):
                logger.error(f"Model {model_key}: max_output_tokens must be an integer")
                models_error += 1
                continue
            
            # Create a fresh session for each model to avoid transaction conflicts
            with SessionLocal() as session:
                if model_key not in existing_keys:
                    # Check again to be extra sure - this helps with race conditions
                    check_result = session.execute(
                        select(ModelConfig).filter(ModelConfig.key == model_key)
                    )
                    existing_model = check_result.scalars().first()
                    
                    if existing_model:
                        # If it exists now (race condition), update it instead
                        existing_model.name = model_data["name"]
                        existing_model.provider = model_data["provider"]
                        existing_model.temperature = model_data["temperature"]
                        existing_model.context_window = model_data["context_window"]
                        existing_model.max_output_tokens = model_data["max_output_tokens"]
                        existing_model.extended_thinking = model_data.get("extended_thinking", False)
                        existing_model.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing model: {model_key}")
                        models_updated += 1
                    else:
                        # Add new model config
                        model_config = ModelConfig(
                            key=model_key,
                            name=model_data["name"],
                            provider=model_data["provider"],
                            temperature=model_data["temperature"],
                            context_window=model_data["context_window"],
                            max_output_tokens=model_data["max_output_tokens"],
                            extended_thinking=model_data.get("extended_thinking", False),
                            enabled=True,
                            created_at=datetime.now().replace(tzinfo=None),
                            updated_at=datetime.now().replace(tzinfo=None)
                        )
                        session.add(model_config)
                        logger.debug(f"Adding new model: {model_key}")
                        models_added += 1
                else:
                    # Update existing model config
                    result = session.execute(
                        select(ModelConfig).filter(ModelConfig.key == model_key)
                    )
                    existing_model = result.scalars().first()
                    
                    if existing_model:
                        existing_model.name = model_data["name"]
                        existing_model.provider = model_data["provider"]
                        existing_model.temperature = model_data["temperature"]
                        existing_model.context_window = model_data["context_window"]
                        existing_model.max_output_tokens = model_data["max_output_tokens"]
                        existing_model.extended_thinking = model_data.get("extended_thinking", False)
                        existing_model.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing model: {model_key}")
                        models_updated += 1
                
                # Commit the session for this model
                try:
                    session.commit()
                except Exception as e:
                    session.rollback()
                    if "UNIQUE constraint failed" in str(e):
                        logger.warning(f"Model {model_key} already exists, skipping insert")
                        models_skipped += 1
                    else:
                        logger.error(f"Failed to commit model config for {model_key}: {str(e)}")
                        models_error += 1
        except Exception as e:
            logger.error(f"Error processing model {model_key}: {str(e)}")
            models_error += 1
    
    logger.info(f"Model configs seeding summary: Added {models_added}, Updated {models_updated}, Skipped {models_skipped}, Errors {models_error}")

# Main entry point for seeding - can be called directly or by seed_runner
async def seed():
    """Main entry point for seeding model configurations."""
    logger.info("Starting model configs seeding process...")
    try:
        await seed_async()
        logger.info("Model configs seeding completed successfully")
    except Exception as e:
        logger.error(f"Error seeding model configs: {str(e)}")
        import traceback
        logger.error(f"Model configs seeding traceback: {traceback.format_exc()}")
        # Don't re-raise - allow other seeds to run

# For backwards compatibility or direct command-line usage
if __name__ == "__main__":
    import asyncio
    asyncio.run(seed()) 
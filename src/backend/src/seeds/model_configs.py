"""
Seed the model_configs table with default model configuration definitions.
"""
import logging
from datetime import datetime
from sqlalchemy import select

from src.db.session import async_session_factory
from src.models.model_config import ModelConfig
from src.core.unit_of_work import UnitOfWork

# Configure logging
logger = logging.getLogger(__name__)

# Define default model configurations
DEFAULT_MODELS = {
    # --- OpenAI ---
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
    "gpt-5": {
        "name": "gpt-5",
        "temperature": 0.7,
        "provider": "openai",
        "context_window": 400000,
        "max_output_tokens": 128000
    },
    "gpt-5.2": {
        "name": "openai/gpt-5.2",
        "temperature": 0.7,
        "provider": "openai",
        "context_window": 400000,
        "max_output_tokens": 128000
    },
    "o3-deep-research": {
        "name": "o3-deep-research-2025-06-26",
        "temperature": 1,
        "provider": "openai",
        "context_window": 200000,
        "max_output_tokens": 100000
    },
    "o4-mini-deep-research": {
        "name": "o4-mini-deep-research-2025-06-26",
        "temperature": 1,
        "provider": "openai",
        "context_window": 200000,
        "max_output_tokens": 100000
    },
    # --- Gemini ---
    "gemini-2.0-flash": {
        "name": "gemini-2.0-flash",
        "temperature": 0.7,
        "provider": "gemini",
        "context_window": 1000000,
        "max_output_tokens": 4096
    },
    "gemini-3-pro": {
        "name": "gemini-3-pro-preview",
        "temperature": 1,
        "provider": "gemini",
        "context_window": 1000000,
        "max_output_tokens": 64000
    },
    "gemini-3-flash": {
        "name": "gemini-3-flash-preview",
        "temperature": 1,
        "provider": "gemini",
        "context_window": 1000000,
        "max_output_tokens": 64000
    },
    # --- Anthropic (Claude 3 models removed — retired/superseded by Claude 4.x) ---
    "claude-opus-4-20250514": {
        "name": "claude-opus-4-20250514",
        "temperature": 0.7,
        "provider": "anthropic",
        "context_window": 200000,
        "max_output_tokens": 32000
    },
    "claude-sonnet-4": {
        "name": "claude-sonnet-4-20250514",
        "temperature": 0.7,
        "provider": "anthropic",
        "context_window": 200000,
        "max_output_tokens": 8192
    },
    # --- Ollama ---
    "llama3.2:latest": {
        "name": "llama3.2:latest",
        "temperature": 0.7,
        "provider": "ollama",
        "context_window": 128000,
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
    # --- DeepSeek ---
    "deepseek-chat": {
        "name": "deepseek-chat",
        "temperature": 0.7,
        "provider": "deepseek",
        "context_window": 128000,
        "max_output_tokens": 8000
    },
    "deepseek-reasoner": {
        "name": "deepseek-reasoner",
        "temperature": 0.7,
        "provider": "deepseek",
        "context_window": 128000,
        "max_output_tokens": 64000
    },
    "deepseek-v3.1-non-thinking": {
        "name": "deepseek-chat",
        "temperature": 0.7,
        "provider": "deepseek",
        "context_window": 128000,
        "max_output_tokens": 8000
    },
    "deepseek-v3.1-thinking": {
        "name": "deepseek-reasoner",
        "temperature": 0.7,
        "provider": "deepseek",
        "context_window": 128000,
        "max_output_tokens": 64000
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
    },
    # --- vLLM (self-hosted, OpenAI-compatible) ---
    "deepseek-r1-70b": {
        "name": "deepseek-r1-70b",
        "temperature": 0.6,
        "provider": "vllm",
        "context_window": 32768,
        "max_output_tokens": 4096
    },
    "Qwen3-Coder-30B-A3B-Instruct": {
        # Self-hosted via vLLM (provider=vllm, endpoint from VLLM_BASE_URL).
        # `name` is BOTH the UI label and the model id sent to the serving endpoint,
        # so vLLM --served-model-name must match it exactly.
        "name": "Qwen3-Coder-30B-A3B-Instruct",
        "temperature": 0.6,
        "provider": "vllm",
        # Qwen3-Coder-30B-A3B is a MoE (~3.3B active): newer, faster and more
        # reliable at the A2UI JSON format than dense 32B/14B models. Served from the
        # AWQ-4bit build (cpatonn/...-AWQ-4bit). context_window=28672 MUST match vLLM
        # --max-model-len: vLLM enforces prompt_tokens + max_tokens <= max-model-len
        # and 400s "passed N input and requested M output" otherwise, so the window
        # has to cover a large prompt PLUS max_output_tokens (8192). At 0.85 util the
        # GPU (shared with Ollama's embedder) fits ~31.8K of KV, so 28672 leaves
        # margin. vLLM MUST be launched with --enable-auto-tool-choice
        # --tool-call-parser qwen3_coder, else CrewAI planning/reasoning (which force
        # tool_choice="function") 400 with "requires --tool-call-parser to be set".
        # vLLM keeps legacy served-name aliases so older runs still resolve.
        "context_window": 28672,
        "max_output_tokens": 8192
    },
    # --- Databricks (sorted alphabetically) ---
    "databricks-claude-haiku-4-5": {
        "name": "databricks-claude-haiku-4-5",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 200000,
        "max_output_tokens": 64000
    },
    "databricks-claude-opus-4-1": {
        "name": "databricks-claude-opus-4-1",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 200000,
        "max_output_tokens": 32000
    },
    "databricks-claude-opus-4-5": {
        "name": "databricks-claude-opus-4-5",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 200000,
        "max_output_tokens": 64000
    },
    "databricks-claude-opus-4-6": {
        "name": "databricks-claude-opus-4-6",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 200000,
        "max_output_tokens": 32000
    },
    "databricks-claude-opus-4-7": {
        "name": "databricks-claude-opus-4-7",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 1000000,
        "max_output_tokens": 64000
    },
    "databricks-claude-opus-4-8": {
        "name": "databricks-claude-opus-4-8",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 1000000,
        "max_output_tokens": 64000
    },
    "databricks-claude-sonnet-4": {
        "name": "databricks-claude-sonnet-4",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 200000,
        "max_output_tokens": 64000
    },
    "databricks-claude-sonnet-4-5": {
        "name": "databricks-claude-sonnet-4-5",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 200000,
        "max_output_tokens": 64000
    },
    "databricks-claude-sonnet-4-6": {
        "name": "databricks-claude-sonnet-4-6",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 200000,
        "max_output_tokens": 64000
    },
    "databricks-gemini-2-5-flash": {
        "name": "databricks-gemini-2-5-flash",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 1048576,
        "max_output_tokens": 65536
    },
    "databricks-gemini-3-1-flash-lite": {
        "name": "databricks-gemini-3-1-flash-lite",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 1000000,
        "max_output_tokens": 65536
    },
    "databricks-gemini-3-5-flash": {
        "name": "databricks-gemini-3-5-flash",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 1000000,
        "max_output_tokens": 65536
    },
    "databricks-gemma-3-12b": {
        "name": "databricks-gemma-3-12b",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 128000,
        "max_output_tokens": 8192
    },
    "databricks-gpt-5": {
        "name": "databricks-gpt-5",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 400000,
        "max_output_tokens": 128000
    },
    "databricks-gpt-5-1": {
        "name": "databricks-gpt-5-1",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 400000,
        "max_output_tokens": 128000
    },
    "databricks-gpt-5-2": {
        "name": "databricks-gpt-5-2",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 400000,
        "max_output_tokens": 128000
    },
    "databricks-gpt-5-3-codex": {
        "name": "databricks-gpt-5-3-codex",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 400000,
        "max_output_tokens": 128000
    },
    "databricks-gpt-5-4": {
        "name": "databricks-gpt-5-4",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 400000,
        "max_output_tokens": 128000
    },
    "databricks-gpt-5-4-mini": {
        "name": "databricks-gpt-5-4-mini",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 400000,
        "max_output_tokens": 64000
    },
    "databricks-gpt-5-4-nano": {
        "name": "databricks-gpt-5-4-nano",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 400000,
        "max_output_tokens": 32000
    },
    "databricks-gpt-5-mini": {
        "name": "databricks-gpt-5-mini",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 200000,
        "max_output_tokens": 64000
    },
    "databricks-gpt-5-nano": {
        "name": "databricks-gpt-5-nano",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 128000,
        "max_output_tokens": 32000
    },
    "databricks-llama-4-maverick": {
        "name": "databricks-llama-4-maverick",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 128000,
        "max_output_tokens": 8000
    },
    "databricks-meta-llama-3-1-8b-instruct": {
        "name": "databricks-meta-llama-3-1-8b-instruct",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "databricks-meta-llama-3-3-70b-instruct": {
        "name": "databricks-meta-llama-3-3-70b-instruct",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 128000,
        "max_output_tokens": 4096
    },
    "databricks-qwen3-next-80b-a3b-instruct": {
        "name": "databricks-qwen3-next-80b-a3b-instruct",
        "temperature": 0.7,
        "provider": "databricks",
        "context_window": 262144,
        # Endpoint caps output at 10000 ("max_tokens cannot exceed 10000"); a
        # higher value 400s on real crew runs.
        "max_output_tokens": 10000
    },
}

# Alias for backwards compatibility - some modules import MODEL_CONFIGS
MODEL_CONFIGS = DEFAULT_MODELS

# Model keys that have been removed from the catalog and must be pruned from the
# DB on seed (the upsert loop alone never deletes). Claude 3 is retired/superseded
# by Claude 4.x (Databricks no longer serves Claude 3.7 Sonnet either).
REMOVED_MODEL_KEYS = [
    "claude-3-5-sonnet-20241022",
    "claude-3-5-haiku-20241022",
    "claude-3-7-sonnet-20250219",
    "claude-3-7-sonnet-20250219-thinking",
    "claude-3-opus-20240229",
    "databricks-claude-3-7-sonnet",
    # Anthropic suspended Claude Fable 5 on 2026-06-12 (US export-control
    # directive); the endpoint returns TEMPORARILY_UNAVAILABLE for all callers.
    # Prune it from any DB it was seeded/added to so it leaves the model picker.
    # Re-add to DEFAULT_MODELS (and drop from here) if the suspension is lifted.
    "databricks-claude-fable-5",
    # Audited 2026-06-20 against the fevm-serverless-stable workspace with a
    # hello-world run through Kasal's own LLM path: these Databricks endpoints no
    # longer work — removed/renamed in the workspace, or only support the
    # Responses API (which Kasal's chat-completions path can't use). Pruned so
    # they also leave the model picker on already-seeded DBs.
    "databricks-gemini-3-flash",                 # endpoint removed -> use gemini-3-5-flash / gemini-3-1-flash-lite
    "databricks-gemini-3-pro",                   # endpoint removed
    "databricks-gpt-5-1-codex-max",              # endpoint removed
    "databricks-gpt-5-1-codex-mini",             # endpoint removed
    "databricks-gpt-5-5-pro",                    # Responses-API-only (unsupported via chat completions)
    "databricks-gpt-5-5",                        # function tools unsupported via chat completions (Responses API only) — breaks tool crews
    "databricks-gemini-2-5-pro",                 # removed per request (superseded by gemini-3-5-flash / gemini-3-1-flash-lite)
    "databricks-meta-llama-3-1-405b-instruct",   # NOT_FOUND (pay-per-token disabled)
    # Reasoning model: answers the JSON-only planning prompt with a "thinking"
    # preamble ("1. Analyze the Request: ..."), so crew planning fails with
    # "Could not parse response as JSON". Not suited to one-shot JSON generation.
    "databricks-qwen35-122b-a10b",
    # GPT-OSS reasoning models fail crew runs (reasoning-block / JSON-planning
    # quirks). Pruned per audit.
    "databricks-gpt-oss-120b",
    "databricks-gpt-oss-20b",
]

async def seed_async():
    """Seed model configurations into the database using async session."""
    logger.info("Seeding model_configs table (async)...")
    
    # Counters for summary
    models_added = 0
    models_updated = 0
    models_skipped = 0
    models_error = 0
    
    # Required fields for a valid model config
    required_fields = ["name", "temperature", "provider", "context_window", "max_output_tokens"]
    
    # Use single session for all models to improve performance
    async with async_session_factory() as session:
        try:
            # Process each model configuration using upsert approach
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
                    
                    # Use upsert approach: try to find existing model first
                    result = await session.execute(
                        select(ModelConfig).filter(ModelConfig.key == model_key)
                    )
                    existing_model = result.scalars().first()
                    
                    if existing_model:
                        # Update existing model config
                        existing_model.name = model_data["name"]
                        existing_model.provider = model_data["provider"]
                        existing_model.temperature = model_data["temperature"]
                        existing_model.context_window = model_data["context_window"]
                        existing_model.max_output_tokens = model_data["max_output_tokens"]
                        existing_model.extended_thinking = model_data.get("extended_thinking", False)
                        existing_model.enabled = (model_key == "Qwen3-Coder-30B-A3B-Instruct")  # Only enable the self-hosted vllm model
                        existing_model.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing model: {model_key}")
                        models_updated += 1
                    else:
                        # Add new model config - only vLLM model is enabled by default
                        model_config = ModelConfig(
                            key=model_key,
                            name=model_data["name"],
                            provider=model_data["provider"],
                            temperature=model_data["temperature"],
                            context_window=model_data["context_window"],
                            max_output_tokens=model_data["max_output_tokens"],
                            extended_thinking=model_data.get("extended_thinking", False),
                            enabled=(model_key == "Qwen3-Coder-30B-A3B-Instruct"),  # Only enable the self-hosted vllm model
                            created_at=datetime.now().replace(tzinfo=None),
                            updated_at=datetime.now().replace(tzinfo=None)
                        )
                        session.add(model_config)
                        logger.debug(f"Adding new model: {model_key}")
                        models_added += 1

                except Exception as model_error:
                    logger.error(f"Error processing model {model_key}: {str(model_error)}")
                    models_error += 1

            # Prune retired models (e.g. Claude 3) from existing installations —
            # the upsert loop above never deletes, so removed keys would linger.
            # Resilient per-key: a prune failure must never break seeding.
            models_removed = 0
            for removed_key in REMOVED_MODEL_KEYS:
                try:
                    result = await session.execute(
                        select(ModelConfig).filter(ModelConfig.key == removed_key)
                    )
                    stale = result.scalars().first()
                    if stale is not None:
                        await session.delete(stale)
                        models_removed += 1
                        logger.debug(f"Removed retired model: {removed_key}")
                except Exception as prune_error:
                    logger.warning(f"Could not prune model {removed_key}: {str(prune_error)}")

            # Commit all changes at once
            await session.commit()
            logger.info(f"Model configs seeding summary: Added {models_added}, Updated {models_updated}, Removed {models_removed}, Skipped {models_skipped}, Errors {models_error}")
            
        except Exception as e:
            logger.error(f"Error seeding model configs: {str(e)}")
            await session.rollback()
            raise

def seed_sync():
    """Seed model configurations into the database using sync session."""
    logger.info("Seeding model_configs table (sync)...")
    
    # Counters for summary
    models_added = 0
    models_updated = 0
    models_skipped = 0
    models_error = 0
    
    # Required fields for a valid model config
    required_fields = ["name", "temperature", "provider", "context_window", "max_output_tokens"]
    
    # Use single session for all models to improve performance
    with SessionLocal() as session:
        try:
            # Process each model configuration using upsert approach
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
                    
                    # Use upsert approach: try to find existing model first
                    result = session.execute(
                        select(ModelConfig).filter(ModelConfig.key == model_key)
                    )
                    existing_model = result.scalars().first()
                    
                    if existing_model:
                        # Update existing model config
                        existing_model.name = model_data["name"]
                        existing_model.provider = model_data["provider"]
                        existing_model.temperature = model_data["temperature"]
                        existing_model.context_window = model_data["context_window"]
                        existing_model.max_output_tokens = model_data["max_output_tokens"]
                        existing_model.extended_thinking = model_data.get("extended_thinking", False)
                        existing_model.enabled = (model_key == "Qwen3-Coder-30B-A3B-Instruct")  # Only enable the self-hosted vllm model
                        existing_model.updated_at = datetime.now().replace(tzinfo=None)
                        logger.debug(f"Updating existing model: {model_key}")
                        models_updated += 1
                    else:
                        # Add new model config - only vLLM model is enabled by default
                        model_config = ModelConfig(
                            key=model_key,
                            name=model_data["name"],
                            provider=model_data["provider"],
                            temperature=model_data["temperature"],
                            context_window=model_data["context_window"],
                            max_output_tokens=model_data["max_output_tokens"],
                            extended_thinking=model_data.get("extended_thinking", False),
                            enabled=(model_key == "Qwen3-Coder-30B-A3B-Instruct"),  # Only enable the self-hosted vllm model
                            created_at=datetime.now().replace(tzinfo=None),
                            updated_at=datetime.now().replace(tzinfo=None)
                        )
                        session.add(model_config)
                        logger.debug(f"Adding new model: {model_key}")
                        models_added += 1
                        
                except Exception as model_error:
                    # Handle unique constraint violations gracefully
                    if "UNIQUE constraint failed" in str(model_error) or "IntegrityError" in str(type(model_error).__name__):
                        logger.warning(f"Model {model_key} already exists (concurrent insertion), skipping")
                        models_skipped += 1
                    else:
                        logger.error(f"Error processing model {model_key}: {str(model_error)}")
                        models_error += 1

            # Prune retired models (e.g. Claude 3) from existing installations.
            # Resilient per-key: a prune failure must never break seeding.
            models_removed = 0
            for removed_key in REMOVED_MODEL_KEYS:
                try:
                    result = session.execute(
                        select(ModelConfig).filter(ModelConfig.key == removed_key)
                    )
                    stale = result.scalars().first()
                    if stale is not None:
                        session.delete(stale)
                        models_removed += 1
                        logger.debug(f"Removed retired model: {removed_key}")
                except Exception as prune_error:
                    logger.warning(f"Could not prune model {removed_key}: {str(prune_error)}")

            # Commit all changes at once
            session.commit()
            logger.info(f"Model configs seeding summary: Added {models_added}, Updated {models_updated}, Removed {models_removed}, Skipped {models_skipped}, Errors {models_error}")
            
        except Exception as e:
            logger.error(f"Error seeding model configs: {str(e)}")
            session.rollback()
            raise

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
if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(seed())
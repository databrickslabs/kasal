import logging
from typing import List, Dict
from datetime import datetime

from src.db.session import async_session_factory
from src.repositories.api_key_repository import ApiKeyRepository

logger = logging.getLogger(__name__)

# Define placeholder API keys that tools may require (no secrets, empty values)
PLACEHOLDER_API_KEYS: List[Dict[str, str]] = [
    {"name": "SERPER_API_KEY", "description": "API Key for Serper.dev web search"},
    {"name": "PERPLEXITY_API_KEY", "description": "API Key for Perplexity AI search"},
    {"name": "FIRECRAWL_API_KEY", "description": "API Key for Firecrawl web crawling"},
    {"name": "EXA_API_KEY", "description": "API Key for EXA semantic search"},
    {"name": "LINKUP_API_KEY", "description": "API Key for Linkup search"},
    {"name": "COMPOSIO_API_KEY", "description": "API Key for Composio integration"},
    # Model/provider keys are managed elsewhere but we can ensure placeholders exist too
    {"name": "OPENAI_API_KEY", "description": "OpenAI API Key"},
    {"name": "ANTHROPIC_API_KEY", "description": "Anthropic API Key"},
    {"name": "DEEPSEEK_API_KEY", "description": "DeepSeek API Key"},
    {"name": "GEMINI_API_KEY", "description": "Google Gemini API Key"},
    {"name": "DATABRICKS_API_KEY", "description": "Databricks personal access token (if used)"},
]

async def seed():
    logger.info("Seeding placeholder API keys (no secret values)...")
    async with async_session_factory() as session:
        repo = ApiKeyRepository(session)
        created = 0

        for entry in PLACEHOLDER_API_KEYS:
            name = entry["name"].strip().upper()
            desc = entry.get("description", "")
            # Check if exists (global, group_id=None)
            existing = await repo.find_by_name(name, group_id=None)
            if existing:
                continue
            # Create placeholder with empty encrypted_value (interpreted as Not set)
            model_dict = {
                "name": name,
                "encrypted_value": "",  # empty means Not set
                "description": desc,
                "group_id": None,
                "created_by_email": None,
            }
            await repo.create(model_dict)
            created += 1
        logger.info(f"API Keys seeder completed. Created {created} placeholder keys.")


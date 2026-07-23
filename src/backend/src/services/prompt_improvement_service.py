"""
Service for prompt improvement operations.

Business logic behind the form "Improve with AI" buttons: takes an
agent's or task's current prompt fields, rewrites them with an LLM using
the group-overridable ``improve_prompt`` template, and returns the
improved texts. On-demand only — never called during generation or
execution.
"""

import json
import logging
import os
from typing import Any, Dict, Optional

from src.core.llm_manager import LLMManager
from src.repositories.log_repository import LLMLogRepository
from src.services.log_service import LLMLogService
from src.services.template_service import TemplateService
from src.utils.prompt_utils import robust_json_parser
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

# Default model for prompt improvement (same fallback as task generation)
DEFAULT_IMPROVE_MODEL = os.getenv("DEFAULT_IMPROVE_MODEL", "databricks-gpt-5-3-codex")


class PromptImprovementService:
    """Service for improving agent/task prompt fields with an LLM."""

    def __init__(self, session: Any):
        """
        Initialize the service with database session.

        Args:
            session: Database session from dependency injection
        """
        self.session = session
        self.log_service = LLMLogService(LLMLogRepository(session))

    async def _log_llm_interaction(self, prompt: str, response: str, model: str,
                                   status: str = 'success', error_message: Optional[str] = None,
                                   group_context: Optional[GroupContext] = None):
        """Log the LLM interaction; never let logging failures break the request."""
        try:
            await self.log_service.create_log(
                endpoint='improve-prompt',
                prompt=prompt,
                response=response,
                model=model,
                status=status,
                error_message=error_message,
                group_context=group_context,
            )
        except Exception as e:
            logger.error(f"Failed to log LLM interaction: {str(e)}")

    async def improve_prompt(
        self,
        target: str,
        fields: Dict[str, str],
        instructions: Optional[str] = None,
        model: Optional[str] = None,
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, str]:
        """Improve the given prompt fields as one coherent set.

        Args:
            target: "agent", "task", or "template" — what the fields belong to
            fields: current field texts keyed by field name
            instructions: optional user guidance for the rewrite
            model: optional LLM model override
            group_context: group context for template overrides and log isolation

        Returns:
            Dict with the same keys as ``fields`` and improved text values.
            A field the LLM omits or returns non-string falls back to its
            original text, so the caller always gets a complete set back.
        """
        model = model or os.getenv("PROMPT_IMPROVE_MODEL", DEFAULT_IMPROVE_MODEL)
        system = await TemplateService.get_effective_template_content("improve_prompt", group_context)
        user = json.dumps(
            {"target": target, "fields": fields, "instructions": instructions},
            ensure_ascii=False,
        )
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        try:
            from src.utils.telemetry import get_user_agent_header, KasalProduct
            content = await LLMManager.completion(
                messages=messages,
                model=model,
                temperature=0.4,
                max_tokens=2000,
                extra_headers=get_user_agent_header(KasalProduct.PROMPT_IMPROVEMENT),
            )
            parsed = robust_json_parser(content or "")
            if not isinstance(parsed, dict):
                raise ValueError("Prompt improvement response is not a JSON object")
            improved = {
                key: parsed[key].strip() if isinstance(parsed.get(key), str) and parsed[key].strip() else original
                for key, original in fields.items()
            }
            await self._log_llm_interaction(
                prompt=f"System: {system}\nUser: {user}",
                response=content or "",
                model=model,
                group_context=group_context,
            )
            return improved
        except Exception as e:
            error_msg = f"Error improving prompt: {str(e)}"
            logger.error(error_msg)
            await self._log_llm_interaction(
                prompt=f"System: {system}\nUser: {user}",
                response="",
                model=model,
                status='error',
                error_message=str(e),
                group_context=group_context,
            )
            raise

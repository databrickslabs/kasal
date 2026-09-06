"""Compatibility imports for legacy prompt utilities.

New parsing callers use core.llm.robust_json; template lookups use
services.catalog.templates.TemplateService.get_template_content directly.
"""

from typing import Optional

from sqlalchemy.orm import Session

from src.core.llm.robust_json import robust_json_parser

__all__ = ["get_prompt_template", "robust_json_parser"]


async def get_prompt_template(
    db: Session, name: str, default_template: str = None
) -> Optional[str]:
    """
    Legacy wrapper for TemplateService.get_template_content.

    This function is kept for backward compatibility with existing code.
    New code should use TemplateService.get_template_content directly.

    Args:
        db: Database session
        name: The name of the prompt template to retrieve
        default_template: A default template to use if the database lookup fails

    Returns:
        The template as a string, the default template if provided and the template wasn't found,
        or None if no template was found and no default was provided
    """
    # Import inside function to avoid circular imports
    from src.services.catalog.templates import TemplateService

    return await TemplateService.get_template_content(name, default_template)

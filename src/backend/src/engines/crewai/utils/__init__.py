"""
Utility modules for CrewAI engine.
"""

from .agent_utils import extract_agent_name_from_event, extract_agent_name_from_object

__all__ = [
    "extract_agent_name_from_event",
    "extract_agent_name_from_object",
]
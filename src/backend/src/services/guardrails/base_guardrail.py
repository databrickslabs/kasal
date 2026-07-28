"""
Base class for guardrails that validate task output.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

logger = logging.getLogger(__name__)

class BaseGuardrail(ABC):
    """
    Abstract base class for guardrails that validate task output.
    
    All guardrails should inherit from this class and implement the validate method.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the guardrail with configuration.
        
        Args:
            config: Configuration dictionary for the guardrail
        """
        self.config = config
    
    @abstractmethod
    def validate(self, output: str) -> Dict[str, Any]:
        """
        Validate the task output.
        
        Args:
            output: The task output to validate
            
        Returns:
            Dictionary with validation result and feedback:
                - valid (bool): Whether the output is valid
                - feedback (str): Feedback message if invalid
        """
        pass


def is_task_output(value: Any) -> bool:
    """Does this look like a task's output object (rather than a str or dict)?

    Structural on purpose. These guardrails used ``isinstance(output,
    TaskOutput)``, which bound a validation POLICY to the orchestration engine
    that happens to call it first — so a guardrail could not be reused anywhere
    a crew was not already running (crew generation, a planning pass, an
    exported app). Nothing here needs the class: every branch below it reaches
    for attributes (``raw``, ``results``, ``content``) and falls back to
    ``str(output)``, so "not a string, not a dict, and carries one of those
    attributes" is the real condition being tested.
    """
    if value is None or isinstance(value, (str, bytes, dict, list, tuple, int, float, bool)):
        return False
    # Anything else that reaches a guardrail IS the task's output object. Naming
    # the attributes instead would silently reject a future output type that
    # carries the text under a different name — and the branches below already
    # fall back to str(output), so being wrong here is recoverable while being
    # narrow is not.
    return True

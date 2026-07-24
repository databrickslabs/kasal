"""
Base class for guardrails that validate task output.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any

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
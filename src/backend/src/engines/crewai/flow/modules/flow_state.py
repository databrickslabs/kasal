"""
Flow state management module for CrewAI flow execution.

This module handles state initialization, updates, and crew output parsing.
"""
import logging
import json
from typing import Dict, Any, Optional

from src.core.logger import LoggerManager

# Initialize logger - use flow logger for flow execution
logger = LoggerManager.get_instance().flow


class FlowStateManager:
    """
    Manager for flow state operations including initialization, updates, and parsing.
    """

    @staticmethod
    def initialize_state(inputs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Initialize flow state with optional inputs.

        Args:
            inputs: Optional dictionary of initial state values

        Returns:
            Initialized state dictionary
        """
        state = {}
        if inputs:
            state.update(inputs)
            logger.info(f"Initialized flow state with inputs: {list(inputs.keys())}")
        else:
            logger.info("Initialized empty flow state")
        return state

    @staticmethod
    def update_state(
        current_state: Dict[str, Any],
        updates: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Update flow state with new values.

        Args:
            current_state: Current state dictionary
            updates: Dictionary of updates to merge into state

        Returns:
            Updated state dictionary
        """
        if updates:
            current_state.update(updates)
            logger.info(f"Updated flow state with keys: {list(updates.keys())}")
        return current_state

    @staticmethod
    def parse_crew_output(crew_output: str) -> Dict[str, Any]:
        """
        Parse crew output and extract state updates.

        CrewAI crews can emit JSON in their output which should be parsed
        and merged into flow state.

        Args:
            crew_output: Raw output string from crew execution

        Returns:
            Dictionary of parsed state values
        """
        state_updates = {}

        try:
            # Try to parse the entire output as JSON first
            try:
                parsed = json.loads(crew_output)
                if isinstance(parsed, dict):
                    state_updates = parsed
                    logger.info(f"Parsed crew output as JSON dict with keys: {list(parsed.keys())}")
                    return state_updates
            except json.JSONDecodeError:
                pass

            # Look for JSON blocks in the output
            # Common patterns: {...} or specific markers
            import re

            # Find all JSON-like blocks
            json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
            matches = re.finditer(json_pattern, crew_output)

            for match in matches:
                try:
                    json_str = match.group(0)
                    parsed = json.loads(json_str)
                    if isinstance(parsed, dict):
                        state_updates.update(parsed)
                        logger.info(f"Extracted JSON from crew output: {list(parsed.keys())}")
                except json.JSONDecodeError:
                    continue

            if state_updates:
                logger.info(f"Total state updates from crew output: {list(state_updates.keys())}")
            else:
                logger.debug("No JSON state updates found in crew output")

        except Exception as e:
            logger.error(f"Error parsing crew output: {e}", exc_info=True)

        return state_updates

    @staticmethod
    def evaluate_condition(
        state: Dict[str, Any],
        condition: Optional[str]
    ) -> bool:
        """
        Evaluate a condition expression against current state.

        Args:
            state: Current flow state
            condition: Python expression string to evaluate (e.g., "state.get('x') > 5")

        Returns:
            Boolean result of condition evaluation, True if no condition provided
        """
        if not condition:
            return True

        try:
            # Create a safe evaluation context with state
            eval_context = {
                'state': state,
                '__builtins__': {
                    'True': True,
                    'False': False,
                    'None': None,
                    'str': str,
                    'int': int,
                    'float': float,
                    'bool': bool,
                    'len': len,
                }
            }

            # Evaluate the condition
            result = eval(condition, eval_context)
            logger.info(f"Condition '{condition}' evaluated to: {result}")
            return bool(result)

        except Exception as e:
            logger.error(f"Error evaluating condition '{condition}': {e}", exc_info=True)
            return False

    @staticmethod
    def get_state_value(
        state: Dict[str, Any],
        key: str,
        default: Any = None
    ) -> Any:
        """
        Safely get a value from state with optional default.

        Args:
            state: Current flow state
            key: Key to retrieve
            default: Default value if key not found

        Returns:
            Value from state or default
        """
        return state.get(key, default)

    @staticmethod
    def set_state_value(
        state: Dict[str, Any],
        key: str,
        value: Any
    ) -> Dict[str, Any]:
        """
        Set a value in state.

        Args:
            state: Current flow state
            key: Key to set
            value: Value to set

        Returns:
            Updated state dictionary
        """
        state[key] = value
        logger.debug(f"Set state['{key}'] = {value}")
        return state

    @staticmethod
    def merge_state(
        state: Dict[str, Any],
        merge_dict: Dict[str, Any],
        prefix: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Merge a dictionary into state, optionally with a key prefix.

        Args:
            state: Current flow state
            merge_dict: Dictionary to merge
            prefix: Optional prefix for all keys (e.g., "crew_output_")

        Returns:
            Updated state dictionary
        """
        if prefix:
            for key, value in merge_dict.items():
                state[f"{prefix}{key}"] = value
        else:
            state.update(merge_dict)

        logger.debug(f"Merged {len(merge_dict)} items into state{' with prefix: ' + prefix if prefix else ''}")
        return state

    @staticmethod
    def get_state_snapshot(state: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get a snapshot of current state (deep copy).

        Args:
            state: Current flow state

        Returns:
            Deep copy of state
        """
        import copy
        return copy.deepcopy(state)

    @staticmethod
    def log_state(state: Dict[str, Any], message: str = "Current state"):
        """
        Log the current state for debugging.

        Args:
            state: Current flow state
            message: Log message prefix
        """
        logger.info(f"{message}: {list(state.keys())}")
        for key, value in state.items():
            # Truncate long values for logging
            value_str = str(value)
            if len(value_str) > 100:
                value_str = value_str[:100] + "..."
            logger.debug(f"  {key}: {value_str}")

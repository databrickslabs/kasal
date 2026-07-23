"""
Callable wrapper class for guardrails that supports inspect.getsource().

CrewAI's LLMGuardrailStartedEvent.__init__ calls getsource() on the guardrail function.
This fails for closures/lambdas with "OSError: could not get source code".
By using a class defined in a source file, getsource() can inspect the __call__ method.
"""
import datetime
import os
import traceback

from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
from src.core.logger import LoggerManager


class GuardrailWrapper:
    """
    A callable wrapper for BaseGuardrail instances.

    This class exists because CrewAI's guardrail events call inspect.getsource()
    on the guardrail function, which fails for closures/lambdas. By defining
    the __call__ method in a source file, getsource() can inspect it properly,
    enabling guardrail events to be emitted.

    Attributes:
        guardrail: The BaseGuardrail instance to wrap
        task_key: The task identifier for logging
        logger: Logger instance for guardrail operations
        log_dir: Directory for debug log files
    """

    def __init__(self, guardrail: BaseGuardrail, task_key: str):
        """
        Initialize the guardrail wrapper.

        Args:
            guardrail: The BaseGuardrail instance to wrap
            task_key: The task identifier for logging purposes
        """
        self.guardrail = guardrail
        self.task_key = task_key
        self.logger = LoggerManager.get_instance().guardrails

        # Get log directory from the logger manager
        logger_manager = LoggerManager.get_instance()
        self.log_dir = logger_manager._log_dir
        os.makedirs(self.log_dir, exist_ok=True)

    def __call__(self, output) -> tuple[bool, str]:
        """
        Validate task output with the wrapped guardrail.

        This method is called by CrewAI's guardrail mechanism. It must return
        a tuple of (bool, str) where:
        - (True, output) indicates validation passed
        - (False, feedback) indicates validation failed with feedback message

        Args:
            output: The task output to validate

        Returns:
            A tuple of (success: bool, result_or_feedback: str)
        """
        # Write to debug log
        with open(os.path.join(self.log_dir, "guardrail_debug.log"), "a") as f:
            f.write(f"\n\n{'='*50}\n")
            f.write(f"VALIDATION CALLBACK CALLED at {datetime.datetime.now().isoformat()}\n")
            f.write(f"Task: {self.task_key}\n")
            f.write(f"Output type: {type(output)}\n")
            f.write(f"Output: {str(output)[:1000]}\n")
            f.write(f"{'='*50}\n")

        self.logger.info("=" * 80)
        self.logger.info(f"VALIDATING TASK {self.task_key} OUTPUT WITH GUARDRAIL")
        self.logger.info("=" * 80)
        self.logger.info(f"Task output type: {type(output)}")
        self.logger.info(f"Task output: {output}")

        # Call the guardrail's validate method
        try:
            result = self.guardrail.validate(output)

            if result.get("valid", False):
                self.logger.info(f"Task {self.task_key} output passed guardrail validation")
                self.logger.info(f"Validation result: {result}")
                # Write to debug log
                with open(os.path.join(self.log_dir, "guardrail_debug.log"), "a") as f:
                    f.write(f"Validation PASSED\n")

                # Return a tuple indicating success (True, output)
                return (True, output)
            else:
                # If validation fails, return a tuple for CrewAI guardrail mechanism
                feedback = result.get("feedback", "Output does not meet requirements. Please try again.")
                self.logger.warning(f"Task {self.task_key} output failed guardrail validation")
                self.logger.warning(f"Validation feedback: {feedback}")
                self.logger.info(f"Full validation result: {result}")
                # Write to debug log
                with open(os.path.join(self.log_dir, "guardrail_debug.log"), "a") as f:
                    f.write(f"Validation FAILED: {feedback}\n")

                # Return a tuple indicating failure (False, error_message)
                return (False, feedback)
        except Exception as e:
            self.logger.error(f"Exception during guardrail validation: {str(e)}")
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
            # Write to debug log
            with open(os.path.join(self.log_dir, "guardrail_debug.log"), "a") as f:
                f.write(f"Validation ERROR: {str(e)}\n")

            # Return a tuple indicating failure (False, error_message)
            return (False, f"Validation error: {str(e)}")

    def __repr__(self) -> str:
        """Return a string representation of the wrapper."""
        return f"GuardrailWrapper(task={self.task_key}, guardrail={type(self.guardrail).__name__})"

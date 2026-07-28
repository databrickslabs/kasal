"""Context-window exceptions (crewAI 1.15.5 fidelity — kasal matches on these)."""

from typing import Final

CONTEXT_LIMIT_ERRORS: Final[list[str]] = [
    "expected a string with maximum length",
    "maximum context length",
    "context length exceeded",
    "context_length_exceeded",
    "context window full",
    "too many tokens",
    "input is too long",
    "exceeds token limit",
]


class LLMContextLengthExceededError(Exception):
    """Raised when the context length of a language model is exceeded."""

    def __init__(self, error_message: str) -> None:
        self.original_error_message = error_message
        super().__init__(self._get_error_message(error_message))

    def _is_context_limit_error(self, error_message: str) -> bool:
        return any(
            phrase.lower() in error_message.lower() for phrase in CONTEXT_LIMIT_ERRORS
        )

    def _get_error_message(self, error_message: str) -> str:
        return (
            f"LLM context length exceeded. Original error: {error_message}\n"
            "Consider using a smaller input or implementing a text splitting strategy."
        )


def is_context_length_exceeded(error: Exception) -> bool:
    message = str(error)
    return any(phrase.lower() in message.lower() for phrase in CONTEXT_LIMIT_ERRORS)


class ExecutionBudgetExceededError(RuntimeError):
    """An agent execution budget was breached (tool rounds or wall clock).

    Subclasses RuntimeError so callers that caught the engine's previous
    round-cap RuntimeError keep working. When this propagates out of
    LLM.call(), the standard failure path emits LLMCallFailedEvent, so the
    breach is visible in traces/logs like any other terminal LLM failure.
    """


class ToolExecutionBlockedError(Exception):
    """A pre-execution tool hook blocked this tool call.

    The message is surfaced to the LLM as the tool result (via the normal error
    path), so the agent can explain the denial instead of crashing.

    Lives here, not with the runtime that raises it: the tool loop in
    ``transport/base`` has to CATCH it, and an exception the LLM layer catches
    cannot be defined in a layer above the LLM layer.
    """

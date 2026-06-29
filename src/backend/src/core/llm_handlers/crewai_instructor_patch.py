"""Make CrewAI's InternalInstructor forward LLM credentials to litellm.

CrewAI routes every structured-output call (``llm.call(..., response_model=X)``)
through ``crewai.utilities.internal_instructor.InternalInstructor``. For
litellm-backed LLMs its ``to_pydantic()`` calls
``instructor.from_litellm(litellm.completion)`` with ONLY ``model`` and
``messages`` — silently dropping the ``api_key`` / ``api_base`` /
``extra_headers`` that Kasal attaches to every LLM instance
(OBO token, group-scoped PAT, telemetry User-Agent).

With no credentials, litellm's Databricks provider falls back to SDK
environment auth (``WorkspaceClient()``), which:

  - breaks tenant isolation (the call authenticates as whatever the process
    env holds — typically the app service principal — instead of the
    user/group the execution belongs to), and
  - hard-fails inside Databricks Apps whenever the env carries both OAuth
    client credentials (injected by the platform) and a DATABRICKS_TOKEN
    (exported for the MLflow exporter):
        litellm.APIConnectionError: validate: more than one authorization
        method configured: oauth and pat

This is the root cause of CrewAI memory-analysis / structured-output calls
failing with ``<failed_attempts>`` retry storms in deployed apps while
working fine locally.

We patch ``InternalInstructor.to_pydantic`` to forward the credential
parameters from the wrapped LLM instance into the instructor call.
kwargs pass straight through instructor to ``litellm.completion``, which
accepts all of them. Non-litellm clients (``instructor.from_provider``)
already receive ``base_url``/``api_key`` at client construction and are
left untouched.

Importing this module applies the patch (same convention as the other early
monkey patches imported by ``llm_manager``). Fully defensive: any failure
leaves CrewAI's original behavior untouched.
"""
import logging

logger = logging.getLogger(__name__)


def _instructor_mode_for_llm(llm):
    """Return the instructor ``Mode`` to use for this LLM's structured-output
    coercion, or ``None`` to keep instructor's default (TOOLS) mode.

    CrewAI coerces ``output_pydantic`` for litellm models via a separate
    instructor call (``InternalInstructor.to_pydantic``). Its default TOOLS mode
    sends the schema as one function and asserts the model replies with exactly
    ONE tool call. Databricks chat models (Claude, Llama, …) emit multiple /
    parallel tool calls, so that assertion fails:

        Instructor does not support multiple tool calls, use List[Model] instead

    MD_JSON coerces the schema from JSON-in-text instead of the tool channel:
    no tool-call collision (works alongside the agent's real tools), still
    validated + retried by instructor, and no ``response_format`` dependency
    (Databricks Claude only accepts ``json_schema``, not ``json_object``).

    Only affects structured-output calls — non-pydantic calls never reach
    instructor. codex never reaches here either (is_litellm=False; it enforces
    structured output natively via the Responses API). Returns None for
    non-Databricks litellm models so their instructor behavior is unchanged.
    """
    try:
        if llm is None or isinstance(llm, str) or not getattr(llm, "is_litellm", False):
            return None
        if "databricks" not in str(getattr(llm, "model", "")).lower():
            return None
        import instructor

        return instructor.Mode.MD_JSON
    except Exception:  # noqa: BLE001 — never break a call over mode selection
        return None


try:
    from crewai.utilities.internal_instructor import InternalInstructor

    _original_to_pydantic = InternalInstructor.to_pydantic

    def _to_pydantic_with_credentials(self):
        llm = self.llm
        # Only the litellm client path drops credentials; from_provider
        # clients are constructed with them and reject unknown kwargs.
        is_litellm = (
            llm is not None
            and not isinstance(llm, str)
            and getattr(llm, "is_litellm", False)
        )
        if not is_litellm:
            return _original_to_pydantic(self)

        credential_kwargs = {}
        for attr in ("api_key", "api_base", "base_url", "extra_headers", "timeout"):
            value = getattr(llm, attr, None)
            if value is not None:
                credential_kwargs[attr] = value

        # Databricks models must coerce via MD_JSON (see _instructor_mode_for_llm)
        # to avoid the "multiple tool calls" crash; build a mode-specific client.
        mode = _instructor_mode_for_llm(llm)

        # No credentials to forward AND no mode override → original behavior.
        if not credential_kwargs and mode is None:
            return _original_to_pydantic(self)

        client = self._client
        if mode is not None:
            try:
                import instructor
                from litellm import completion as _litellm_completion

                client = instructor.from_litellm(_litellm_completion, mode=mode)
            except Exception as mode_err:  # noqa: BLE001
                logger.warning(
                    "Could not build %s instructor client for Databricks; "
                    "falling back to default mode: %s", mode, mode_err
                )
                client = self._client

        messages = [{"role": "user", "content": self.content}]
        model_name = llm.model
        return client.chat.completions.create(
            model=model_name,
            response_model=self.model,
            messages=messages,
            **credential_kwargs,
        )

    InternalInstructor.to_pydantic = _to_pydantic_with_credentials
    logger.info(
        "Patched InternalInstructor.to_pydantic (credential forwarding + "
        "MD_JSON coercion for Databricks)"
    )

except Exception as patch_err:  # noqa: BLE001 — never break startup over a patch
    logger.warning(
        "Could not patch InternalInstructor credential forwarding: %s", patch_err
    )


# ---------------------------------------------------------------------------
# Databricks JSON-schema sanitizer for structured output
# ---------------------------------------------------------------------------
#
# The Databricks AI Gateway rejects JSON schemas that put numeric range
# keywords on number/integer types:
#     litellm.BadRequestError: databricksException -
#       {"error_code":"BAD_REQUEST","message":"Invalid JSON schema - number
#        types do not support minimum"}
# Structured-output callers express constraints with Pydantic Field(ge=, le=)
# (e.g. CrewAI memory save-analysis: ``importance: float = Field(ge=0, le=1)``).
# instructor turns those into ``minimum``/``maximum`` in the tool /
# response_format schema → the call 400s, retries 3×, and the feature silently
# falls back ("Memory save analysis failed, using defaults").
#
# These keywords are advisory only (the model isn't bound by them server-side;
# validation still happens locally when the response is parsed into the model),
# so stripping them costs nothing and makes structured output work on Databricks.
# We wrap ``litellm.completion``/``acompletion`` and, ONLY for Databricks models,
# recursively remove the numeric range keywords from ``tools`` and
# ``response_format``. ``InternalInstructor`` does ``from litellm import
# completion`` at construction time (per call), so wrapping the module attribute
# here — at import, before any run — is picked up by every structured call.

# JSON-schema keywords Databricks rejects on number/integer types.
_NUMERIC_RANGE_KEYS = (
    "minimum",
    "maximum",
    "exclusiveMinimum",
    "exclusiveMaximum",
    "multipleOf",
)


def strip_numeric_range_keywords(obj):
    """Recursively delete Databricks-unsupported numeric range keywords from a
    JSON-schema-shaped structure (dicts/lists). Mutates ``obj`` in place."""
    if isinstance(obj, dict):
        for key in _NUMERIC_RANGE_KEYS:
            obj.pop(key, None)
        for value in obj.values():
            strip_numeric_range_keywords(value)
    elif isinstance(obj, list):
        for item in obj:
            strip_numeric_range_keywords(item)
    return obj


def sanitize_databricks_request_kwargs(kwargs):
    """Strip unsupported numeric constraints from a litellm request's structured
    schema, but ONLY for Databricks models. No-op otherwise. Best-effort."""
    try:
        model = kwargs.get("model")
        if not isinstance(model, str) or not model.startswith("databricks"):
            return
        tools = kwargs.get("tools")
        if tools:
            strip_numeric_range_keywords(tools)
        response_format = kwargs.get("response_format")
        if isinstance(response_format, (dict, list)):
            strip_numeric_range_keywords(response_format)
    except Exception:  # noqa: BLE001 — never break a real LLM call over sanitization
        pass


try:
    import litellm as _litellm

    if not getattr(_litellm, "_kasal_schema_sanitizer_applied", False):
        _orig_completion = _litellm.completion
        _orig_acompletion = _litellm.acompletion

        def _completion_sanitized(*args, **kwargs):
            sanitize_databricks_request_kwargs(kwargs)
            return _orig_completion(*args, **kwargs)

        async def _acompletion_sanitized(*args, **kwargs):
            sanitize_databricks_request_kwargs(kwargs)
            return await _orig_acompletion(*args, **kwargs)

        _litellm.completion = _completion_sanitized
        _litellm.acompletion = _acompletion_sanitized
        setattr(_litellm, "_kasal_schema_sanitizer_applied", True)
        logger.info(
            "Patched litellm.completion/acompletion to strip Databricks-unsupported "
            "numeric JSON-schema constraints (minimum/maximum/…)"
        )

except Exception as schema_patch_err:  # noqa: BLE001 — never break startup over a patch
    logger.warning(
        "Could not patch litellm schema sanitizer: %s", schema_patch_err
    )

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
        if not credential_kwargs:
            return _original_to_pydantic(self)

        messages = [{"role": "user", "content": self.content}]
        model_name = llm.model
        return self._client.chat.completions.create(
            model=model_name,
            response_model=self.model,
            messages=messages,
            **credential_kwargs,
        )

    InternalInstructor.to_pydantic = _to_pydantic_with_credentials
    logger.info("Patched InternalInstructor.to_pydantic to forward LLM credentials")

except Exception as patch_err:  # noqa: BLE001 — never break startup over a patch
    logger.warning(
        "Could not patch InternalInstructor credential forwarding: %s", patch_err
    )

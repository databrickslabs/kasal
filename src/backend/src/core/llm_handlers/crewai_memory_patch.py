"""
Make CrewAI's cognitive-memory structured-output models tolerant of the shapes
Databricks/Bedrock-hosted models actually return.

CrewAI's memory analysis (``crewai/memory/analyze.py``) asks the LLM for several
structured objects. Databricks/Bedrock-served models (Opus *and* Haiku) return
them in shapes Pydantic rejects:

  - ``MemoryAnalysis.extracted_metadata`` (a nested ``ExtractedMetadata`` object,
    ``extra="forbid"``) comes back as a STRINGIFIED JSON blob, often with extra
    keys (``metrics`` / ``key_findings``):
        1 validation error for MemoryAnalysis
        extracted_metadata
          Input should be an object [input_value='{...}', input_type=str]

  - ``QueryAnalysis`` / ``ExtractedMemories`` come back as the WHOLE object
    serialized to a JSON string:
        1 validation error for QueryAnalysis
          Input should be a valid dictionary or instance of QueryAnalysis
          [input_value='{"keywords":[...]}', input_type=str]

On each, CrewAI retries the LLM 3x, all fail, logs a scary
``LLM Error: <failed_attempts> ...`` (MemoryAnalysis) or
``... failed, using defaults`` (QueryAnalysis/ExtractedMemories), and falls back
to defaults. Non-fatal, but it spams the logs, wastes three LLM round-trips per
call, AND degrades recall quality (a defaulted QueryAnalysis recalls poorly).

We patch the models in place so the FIRST attempt validates:
  - parse a whole-object value that arrived as a JSON string, and
  - for MemoryAnalysis, also coerce the nested ``extracted_metadata`` string and
    ignore extra keys.

Importing this module applies the patch (same convention as the other early
monkey patches imported by ``llm_manager``). Fully defensive: any failure leaves
CrewAI's original (self-healing) behavior untouched.
"""
import json
import logging

logger = logging.getLogger(__name__)


def _maybe_json(data):
    """If the LLM serialized the whole object to a JSON string, parse it back."""
    if isinstance(data, str):
        try:
            return json.loads(data)
        except (ValueError, TypeError):
            return data  # let normal validation raise a clear, original error
    return data


try:
    from pydantic import ConfigDict, Field, field_validator, model_validator
    import crewai.memory.analyze as _analyze

    class _TolerantExtractedMetadata(_analyze.ExtractedMetadata):
        # LLMs frequently include extra keys (metrics, key_findings, ...) inside
        # the metadata object; ignore them instead of failing the whole save.
        model_config = ConfigDict(extra="ignore")

    class _TolerantMemoryAnalysis(_analyze.MemoryAnalysis):
        extracted_metadata: _TolerantExtractedMetadata = Field(
            default_factory=_TolerantExtractedMetadata,
            description="Entities, dates, topics extracted from the content.",
        )

        @model_validator(mode="before")
        @classmethod
        def _parse_stringified(cls, data):
            return _maybe_json(data)

        @field_validator("extracted_metadata", mode="before")
        @classmethod
        def _coerce_metadata(cls, v):
            # The model often serializes this nested object as a JSON string.
            v = _maybe_json(v)
            return v if isinstance(v, (dict, _analyze.ExtractedMetadata)) else {}

    class _TolerantQueryAnalysis(_analyze.QueryAnalysis):
        @model_validator(mode="before")
        @classmethod
        def _parse_stringified(cls, data):
            return _maybe_json(data)

    class _TolerantExtractedMemories(_analyze.ExtractedMemories):
        @model_validator(mode="before")
        @classmethod
        def _parse_stringified(cls, data):
            return _maybe_json(data)

    # analyze_for_save() / analyze_query() / extract_memories_from_content()
    # resolve these names as module globals at call time, so reassigning them
    # here makes the tolerant versions take effect on the whole memory path
    # without touching the installed package.
    _analyze.ExtractedMetadata = _TolerantExtractedMetadata
    _analyze.MemoryAnalysis = _TolerantMemoryAnalysis
    _analyze.QueryAnalysis = _TolerantQueryAnalysis
    _analyze.ExtractedMemories = _TolerantExtractedMemories

    logger.info(
        "[crewai-memory-patch] MemoryAnalysis/QueryAnalysis/ExtractedMemories now "
        "tolerate stringified-JSON output + extra keys"
    )
except Exception as e:  # noqa: BLE001 — a patch failure must never break startup
    logger.warning("[crewai-memory-patch] could not patch CrewAI memory models: %s", e)

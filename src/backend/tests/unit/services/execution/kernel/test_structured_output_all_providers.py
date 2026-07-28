"""``response_model`` must work on every provider, not just Databricks.

The engine accepted ``response_model`` in ``call()`` and ignored it, returning a
JSON string. Callers do ``isinstance(r, Model) or Model.model_validate(r)``, and
``model_validate(<str>)`` raises — so they fell back silently (long-term-memory
consolidation logging "analysis failed, defaulting to insert"). Only
DatabricksRetryLLM compensated, with a private coercion of its own, so the
feature quietly worked on one provider out of six.
"""

from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from src.core.llm.transport import LLM


class _Plan(BaseModel):
    keep: bool
    note: str = ""


def _llm_returning(text):
    """An LLM whose endpoint answers with ``text``."""
    llm = LLM(model="test-structured", api_key="k")
    response = MagicMock()
    response.choices = [MagicMock(message=MagicMock(content=text, tool_calls=None))]
    response.usage = None
    client = MagicMock()
    client.chat.completions.create.return_value = response
    return llm, client


def _call(llm, client, **kwargs):
    with patch.object(type(llm), "client", property(lambda self: client)):
        return llm.call([{"role": "user", "content": "hi"}], **kwargs)


class TestResponseModelIsHonoured:
    def test_json_string_is_parsed_into_the_model(self):
        llm, client = _llm_returning('{"keep": true, "note": "hi"}')
        out = _call(llm, client, response_model=_Plan)
        assert isinstance(out, _Plan)
        assert out.keep is True and out.note == "hi"

    def test_markdown_fenced_json_is_parsed(self):
        llm, client = _llm_returning('```json\n{"keep": false}\n```')
        out = _call(llm, client, response_model=_Plan)
        assert isinstance(out, _Plan)
        assert out.keep is False

    def test_unparseable_output_returns_the_text_unchanged(self):
        """The caller's own fallback must stay available."""
        llm, client = _llm_returning("not json at all")
        out = _call(llm, client, response_model=_Plan)
        assert out == "not json at all"

    def test_without_response_model_a_string_is_still_returned(self):
        llm, client = _llm_returning('{"keep": true}')
        out = _call(llm, client)
        assert out == '{"keep": true}'
        assert not isinstance(out, _Plan)


class TestSharedWithTheDatabricksWrapper:
    def test_wrapper_delegates_to_the_engine_helper(self):
        """One implementation: the wrapper adapts kwargs, it does not re-parse."""
        with patch("src.services.llm.handlers.databricks_retry_llm.litellm"):
            from src.services.llm.handlers.databricks_retry_llm import DatabricksRetryLLM

            llm = DatabricksRetryLLM(model="databricks/x", api_key="k")

        with patch.object(
            llm, "_validate_structured_output", return_value="sentinel"
        ) as helper:
            out = llm._coerce_to_response_model('{"keep": true}', {"response_model": _Plan})

        assert out == "sentinel"
        helper.assert_called_once_with('{"keep": true}', _Plan)

    @pytest.mark.parametrize("result,kwargs", [("x", {}), (_Plan(keep=True), {"response_model": _Plan})])
    def test_adapter_passes_through_when_there_is_nothing_to_parse(self, result, kwargs):
        with patch("src.services.llm.handlers.databricks_retry_llm.litellm"):
            from src.services.llm.handlers.databricks_retry_llm import DatabricksRetryLLM

            llm = DatabricksRetryLLM(model="databricks/x", api_key="k")

        assert llm._coerce_to_response_model(result, kwargs) is result

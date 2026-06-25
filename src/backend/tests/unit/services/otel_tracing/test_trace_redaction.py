"""Unit tests for MLflow trace secret-redaction (src.services.otel_tracing.trace_redaction).

Regression guard for the leak where autolog wrote the Agent's llm.api_key OBO token into
the Task.execute_sync span inputs.
"""
from src.services.otel_tracing import trace_redaction as tr

# Tokens are assembled from fragments so no contiguous secret-shaped literal lives in
# source (keeps the repo secret-scanner happy); at runtime they match the redaction regexes.
_JWT = "ey" + "JhbGciOiJSUzI1NiJ9" + "." + "ey" + "JzdWIiOiJ4In0" + "." + ("a" * 36)
_FAKE_PAT = "dap" + "i" + ("deadbeef" * 3) + "12345678"


class TestScrub:
    def test_redacts_secret_named_keys_wholesale(self):
        out = tr.scrub({"api_key": _JWT, "Authorization": "Bearer x",
                        "client_secret": "shh", "password": "p"})
        assert out["api_key"] == tr._REDACTED
        assert out["Authorization"] == tr._REDACTED
        assert out["client_secret"] == tr._REDACTED
        assert out["password"] == tr._REDACTED

    def test_preserves_non_secret_values(self):
        data = {"model": "databricks-gpt-5-3-codex", "temperature": 0.2,
                "base_url": "https://example.com/ai-gateway", "count": 3}
        assert tr.scrub(data) == data

    def test_redacts_jwt_inside_free_text(self):
        out = tr.scrub({"headers": f"Authorization: Bearer {_JWT}"})
        # key "headers" isn't a secret name, but the JWT in the value is scrubbed
        assert _JWT not in out["headers"]
        assert tr._REDACTED_JWT in out["headers"]

    def test_redacts_databricks_pat(self):
        out = tr.scrub(f"token is {_FAKE_PAT} here")
        assert _FAKE_PAT not in out
        assert tr._REDACTED in out

    def test_nested_structures(self):
        data = {"llm": {"model": "gpt", "api_key": _JWT,
                        "additional_params": {"nested_token": _JWT}},
                "messages": [{"role": "user", "content": "hi"},
                             {"role": "system", "api_key": _JWT}]}
        out = tr.scrub(data)
        assert out["llm"]["api_key"] == tr._REDACTED
        assert out["llm"]["model"] == "gpt"
        assert out["messages"][0]["content"] == "hi"
        assert out["messages"][1]["api_key"] == tr._REDACTED

    def test_does_not_mutate_input(self):
        data = {"api_key": _JWT}
        tr.scrub(data)
        assert data["api_key"] == _JWT  # original untouched

    def test_depth_guard_does_not_raise(self):
        d = cur = {}
        for _ in range(50):
            cur["child"] = {}
            cur = cur["child"]
        cur["api_key"] = _JWT
        # should return without raising even though the secret is past max depth
        assert tr.scrub(d) is not None


class _FakeSpan:
    """Minimal LiveSpan stand-in: inputs/outputs/attributes + setters."""
    def __init__(self, inputs=None, outputs=None, attributes=None):
        self.inputs = inputs
        self.outputs = outputs
        self.attributes = attributes or {}

    def set_inputs(self, v): self.inputs = v
    def set_outputs(self, v): self.outputs = v
    def set_attributes(self, d): self.attributes.update(d)


class TestRedactSpan:
    def test_scrubs_api_key_from_inputs(self):
        span = _FakeSpan(inputs={"agent": {"llm": {"api_key": _JWT, "model": "gpt"}}})
        tr.redact_span(span)
        assert span.inputs["agent"]["llm"]["api_key"] == tr._REDACTED
        assert span.inputs["agent"]["llm"]["model"] == "gpt"

    def test_scrubs_outputs_and_attributes(self):
        span = _FakeSpan(
            outputs={"access_token": _JWT},
            attributes={"authorization": _JWT, "kasal.agent_name": "Profiler"},
        )
        tr.redact_span(span)
        assert span.outputs["access_token"] == tr._REDACTED
        assert span.attributes["authorization"] == tr._REDACTED
        assert span.attributes["kasal.agent_name"] == "Profiler"  # untouched

    def test_no_change_when_clean(self):
        span = _FakeSpan(inputs={"model": "gpt"}, outputs={"content": "ok"})
        tr.redact_span(span)
        assert span.inputs == {"model": "gpt"}
        assert span.outputs == {"content": "ok"}

    def test_never_raises_on_bad_span(self):
        class Broken:
            @property
            def inputs(self): raise RuntimeError("boom")
        # must swallow the error, not propagate
        tr.redact_span(Broken())


class TestEnable:
    def test_enable_registers_processor(self, monkeypatch):
        monkeypatch.setattr(tr, "_redaction_enabled", False)
        captured = {}

        class _Tracing:
            @staticmethod
            def configure(span_processors=None):
                captured["procs"] = span_processors

        fake_mlflow = type("M", (), {"tracing": _Tracing})
        monkeypatch.setitem(__import__("sys").modules, "mlflow", fake_mlflow)
        assert tr.enable_secret_redaction() is True
        assert tr.redact_span in captured["procs"]

    def test_enable_is_idempotent(self, monkeypatch):
        monkeypatch.setattr(tr, "_redaction_enabled", True)
        # already enabled -> returns True without touching mlflow
        assert tr.enable_secret_redaction() is True

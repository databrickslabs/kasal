"""The two free gates: schema parse, then declarative acceptance rule."""

from types import SimpleNamespace

from src.schemas.deep_research import (
    DEEP_RESEARCH_ENVELOPE_SCHEMA,
    DEFAULT_DEEP_GATE,
)
from src.services.agent_builder.schema_converter import build_model_from_schema
from src.services.guardrails.core.detection_rule_guardrail import (
    DetectionRuleGuardrail,
)
from src.services.guardrails.core.schema_gate_guardrail import SchemaGateGuardrail

ENVELOPE_MODEL = build_model_from_schema(
    "DeepResearchEnvelope", DEEP_RESEARCH_ENVELOPE_SCHEMA
)


def _envelope(findings=None, summary="A summary long enough to clear the gate rule."):
    return {
        "summary": summary,
        "findings": (
            findings
            if findings is not None
            else [
                {
                    "claim": f"claim {n}",
                    "source": f"https://example.com/{n}",
                    "confidence": 0.8,
                }
                for n in range(3)
            ]
        ),
        "open_questions": [],
        "limitations": "none",
    }


def _output(raw="", json_dict=None):
    return SimpleNamespace(raw=raw, json_dict=json_dict, pydantic=None)


class TestSchemaGate:
    def test_accepts_valid_json(self):
        import json

        gate = SchemaGateGuardrail(ENVELOPE_MODEL)
        assert gate.validate(_output(raw=json.dumps(_envelope())))["valid"] is True

    def test_rejects_prose(self):
        """Before this gate, prose from a task told to emit JSON simply went
        downstream — a warning in the log and nothing else."""
        gate = SchemaGateGuardrail(ENVELOPE_MODEL)
        result = gate.validate(_output(raw="Here is what I found: batteries are good."))
        assert result["valid"] is False
        assert "not valid JSON" in result["feedback"]
        assert "Required schema" in result["feedback"]

    def test_rejects_empty_output(self):
        gate = SchemaGateGuardrail(ENVELOPE_MODEL)
        assert gate.validate(_output(raw="   "))["valid"] is False

    def test_names_the_missing_field(self):
        import json

        gate = SchemaGateGuardrail(ENVELOPE_MODEL)
        payload = _envelope()
        del payload["summary"]
        result = gate.validate(_output(raw=json.dumps(payload)))
        assert result["valid"] is False
        assert "summary" in result["feedback"]

    def test_tolerates_a_markdown_fence(self):
        import json

        gate = SchemaGateGuardrail(ENVELOPE_MODEL)
        fenced = f"```json\n{json.dumps(_envelope())}\n```"
        assert gate.validate(_output(raw=fenced))["valid"] is True

    def test_tolerates_a_preamble_sentence(self):
        import json

        gate = SchemaGateGuardrail(ENVELOPE_MODEL)
        chatty = f"Sure! Here you go:\n{json.dumps(_envelope())}"
        assert gate.validate(_output(raw=chatty))["valid"] is True


class TestDetectionRule:
    def test_accepts_a_well_formed_envelope(self):
        gate = DetectionRuleGuardrail(DEFAULT_DEEP_GATE)
        assert gate.validate(_output(json_dict=_envelope()))["valid"] is True

    def test_rejects_too_few_findings_and_says_how_many(self):
        gate = DetectionRuleGuardrail(DEFAULT_DEEP_GATE)
        result = gate.validate(_output(json_dict=_envelope(findings=[])))
        assert result["valid"] is False
        assert "0 items, needs at least 3" in result["feedback"]

    def test_rejects_an_uncited_finding_and_names_it(self):
        findings = [
            {"claim": "a", "source": "https://example.com", "confidence": 0.9},
            {"claim": "b", "source": "", "confidence": 0.9},
            {"claim": "c", "source": "https://example.com", "confidence": 0.9},
        ]
        gate = DetectionRuleGuardrail(DEFAULT_DEEP_GATE)
        result = gate.validate(_output(json_dict=_envelope(findings=findings)))
        assert result["valid"] is False
        assert "findings[1].source is empty" in result["feedback"]

    def test_rejects_low_confidence(self):
        findings = [
            {"claim": "a", "source": "https://example.com", "confidence": 0.1}
        ] * 3
        gate = DetectionRuleGuardrail(DEFAULT_DEEP_GATE)
        result = gate.validate(_output(json_dict=_envelope(findings=findings)))
        assert result["valid"] is False
        assert "below the required 0.6" in result["feedback"]

    def test_rejects_a_thin_summary(self):
        gate = DetectionRuleGuardrail(DEFAULT_DEEP_GATE)
        result = gate.validate(_output(json_dict=_envelope(summary="Batteries.")))
        assert result["valid"] is False
        assert "needs at least 40" in result["feedback"]

    def test_rejects_unparsed_output_rather_than_deferring(self):
        """Each guardrail runs its own retry loop to completion, so by the time
        this gate runs the schema gate has finished and is no longer watching.
        Passing unparsed output here let a task escape BOTH gates by emitting
        valid JSON first and prose on the retry — found by a live run, not by
        the unit tests that existed at the time."""
        gate = DetectionRuleGuardrail(DEFAULT_DEEP_GATE)
        result = gate.validate(_output(raw="prose", json_dict=None))
        assert result["valid"] is False
        assert "not a JSON object" in result["feedback"]

    def test_boolean_is_not_a_number(self):
        findings = [
            {"claim": "a", "source": "https://example.com", "confidence": True}
        ] * 3
        gate = DetectionRuleGuardrail(DEFAULT_DEEP_GATE)
        result = gate.validate(_output(json_dict=_envelope(findings=findings)))
        assert result["valid"] is False
        assert "not a number" in result["feedback"]

    def test_missing_path_is_reported(self):
        gate = DetectionRuleGuardrail(
            {"require": [{"path": "nope", "not_empty": True}]}
        )
        result = gate.validate(_output(json_dict=_envelope()))
        assert result["valid"] is False
        assert "nope is missing" in result["feedback"]

    def test_one_of_and_matches(self):
        gate = DetectionRuleGuardrail(
            {
                "require": [
                    {"path": "status", "one_of": ["ok", "partial"]},
                    {"path": "id", "matches": r"^ID-\d+$"},
                ]
            }
        )
        assert gate.validate(_output(json_dict={"status": "ok", "id": "ID-42"}))[
            "valid"
        ]
        bad = gate.validate(_output(json_dict={"status": "weird", "id": "nope"}))
        assert bad["valid"] is False

    def test_invalid_regex_in_a_rule_does_not_fail_the_task(self):
        """The rule author made the mistake; the agent cannot fix it by
        retrying, so a broken rule must not gate the run."""
        gate = DetectionRuleGuardrail(
            {"require": [{"path": "id", "matches": "([unclosed"}]}
        )
        assert gate.validate(_output(json_dict={"id": "x"}))["valid"] is True

    def test_no_requirements_is_a_pass(self):
        assert (
            DetectionRuleGuardrail({}).validate(_output(json_dict={}))["valid"] is True
        )

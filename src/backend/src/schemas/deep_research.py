"""The deep-research output contract: one envelope, one gate vocabulary.

Deep mode asks every task for the same shape rather than a bespoke schema per
task. That choice is what makes the downstream machinery mechanical instead of
per-task bespoke:

* the gate rules are one rule set ("at least three findings, every finding
  cited"), not one per task;
* a citation check has a fixed place to look — ``findings[*].source``;
* contradiction detection compares ``claim`` fields across sibling tasks instead
  of diffing free prose;
* ``open_questions`` is a ready-made input for a gap loop;
* synthesis receives typed findings rather than concatenated paragraphs.

Tasks whose deliverable genuinely is not a research answer (a table, a config, a
dashboard spec) can carry their own ``output_schema``. The envelope is the
default, not a cage.

**The schema states shape; the gate states policy.** Deliberately, the schema
carries no ``minItems`` on ``findings`` and no ``minLength`` on ``summary`` even
though the converter supports both: a thin-but-well-formed answer must fail the
DETECTION rule, which can say "findings has 2 items, needs 3", rather than the
parse gate, which can only say the JSON did not validate. Specific feedback is
the whole reason a retry does better than the first attempt.
"""

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

#: JSON Schema for the envelope. A dict rather than a Pydantic class because the
#: task config is serialized to JSON on its way into the crew subprocess — a
#: class cannot make that trip. ``schema_converter`` rebuilds it on the far side.
DEEP_RESEARCH_ENVELOPE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "description": "A deep-research task's answer.",
    "properties": {
        "summary": {
            "type": "string",
            "description": "A 2-4 sentence answer to this task's question.",
        },
        "findings": {
            "type": "array",
            "description": "The individual factual claims this answer rests on.",
            "items": {"$ref": "#/$defs/Finding"},
        },
        "open_questions": {
            "type": "array",
            "description": "What this task could not resolve.",
            "items": {"type": "string"},
        },
        "limitations": {
            "type": "string",
            "description": "What would make this answer wrong.",
        },
    },
    "required": ["summary", "findings"],
    "$defs": {
        "Finding": {
            "type": "object",
            "properties": {
                "claim": {
                    "type": "string",
                    "description": "A single factual assertion.",
                },
                "evidence": {
                    "type": "string",
                    "description": "The supporting quote or data point.",
                },
                "source": {
                    "type": "string",
                    "description": "URL or identifier of where this came from.",
                },
                "confidence": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                    "description": "How sure you are, 0 to 1.",
                },
            },
            "required": ["claim", "source", "confidence"],
        }
    },
}

#: The default acceptance rule for a deep-research task. Deterministic and free:
#: a gate that costs a judge call will not survive being run on every task.
DEFAULT_DEEP_GATE: Dict[str, Any] = {
    "require": [
        {"path": "summary", "min_length": 40},
        {"path": "findings", "min_items": 3},
        {"path": "findings[*].source", "not_empty": True},
        {"path": "findings[*].confidence", "min": 0.6},
    ],
    "on_fail": "retry",
}


class GateRequirement(BaseModel):
    """One check against the parsed output.

    ``path`` is a dotted path into the object, where ``[*]`` fans out over a
    list and applies the check to every element — ``findings[*].source`` means
    "every finding has a non-empty source".
    """

    path: str = Field(description="Dotted path; [*] iterates a list.")
    min_items: Optional[int] = None
    max_items: Optional[int] = None
    not_empty: Optional[bool] = None
    min: Optional[float] = None
    max: Optional[float] = None
    min_length: Optional[int] = None
    one_of: Optional[List[Any]] = None
    matches: Optional[str] = Field(
        default=None, description="Regex the value must match."
    )


class GateRule(BaseModel):
    """A task's acceptance rule.

    ``on_fail`` is per task on purpose: ``retry`` for investigation work,
    ``degrade`` for anything the run can survive without, ``halt`` for a task
    whose failure makes everything downstream meaningless.
    """

    require: List[GateRequirement] = Field(default_factory=list)
    on_fail: Literal["retry", "degrade", "halt"] = "retry"

"""Backend adapter injected into the portable memory engine, never imported by it."""

import asyncio
from typing import Any

from src.services.decisions.policies import classify_sync, question, rank
from src.services.decisions.runtime import decide_sync


class MemoryDecisions:
    def __init__(self, group_id: str):
        self.group_id = group_id

    def label(self, content: Any, analysis: Any) -> Any:
        kind = classify_sync(
            "memory_classification",
            {"content": content},
            "Classify the memory. An account of a past run is episodic, not a durable fact.",
            {
                "episodic": "An event or account of a past experience",
                "semantic": "A durable fact or preference that remains true",
                "procedural": "Reusable instructions for how to perform a task",
            },
            group_id=self.group_id,
        )
        return analysis.model_copy(update={"kind": kind}) if kind else analysis

    def rank(self, query: Any, records: list[Any]) -> list[Any]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                rank(
                    "memory_ranking",
                    query,
                    records,
                    [r.content for r in records],
                    group_id=self.group_id,
                )
            )
        return records

    def supersession(self, records: list[Any]) -> list[dict[str, Any]] | None:
        if not 2 <= len(records) <= 40:
            return None
        questions = {
            str(i): question(
                f"Which newer record directly replaces the same fact asserted by record {i}? "
                "Different subjects, historical events and compatible details are not contradictions. "
                "Choose none if there is no clear replacement.",
                {**{str(j): f"Record {j}" for j in range(i)}, "none": "No replacement"},
            )
            for i in range(1, len(records))
        }
        answers = decide_sync(
            "memory_supersession",
            {"records": [r.content for r in records], "order": "newest first"},
            questions,
            group_id=self.group_id,
        )
        if answers is None:
            return None
        return [
            {"current": int(a.selected), "outdated": [int(i)]}
            for i, a in answers.items()
            if a.selected != "none"
        ]

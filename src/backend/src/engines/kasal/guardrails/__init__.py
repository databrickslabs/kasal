"""Engine-side guardrail glue.

The guardrails themselves — the contract, the registry, and every policy —
moved to ``src.services.guardrails``: validating an output is a capability, and
tying it to the engine made it unreachable from crew generation, a planning
pass, or an exported app.

What remains is the one piece that IS orchestration: ``GuardrailWrapper`` gives
a built guardrail a stable class identity so the engine can label it in events
and trace rows (``Task._guardrail_label`` reads the wrapper's inner type).
"""

from src.engines.kasal.guardrails.guardrail_wrapper import GuardrailWrapper

__all__ = ["GuardrailWrapper"]

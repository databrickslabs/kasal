"""Security policy: what may be sent to a model, and what may come back out.

Scanning a prompt for injection, redacting secrets from tool output and judging
whether a set of tools makes a task destructive are POLICY, not orchestration.
They were under ``engines/kasal/`` for the accident that a crew run was the
first caller, with no dependency on the engine at all — the whole package
imports nothing but the standard library and itself, and its tests already lived
in ``tests/unit/security/`` rather than beside the engine.

That accident cost reuse. The same checks belong on chat input, on knowledge
uploads and on exported apps, none of which run a crew, and none of which should
have to import an orchestration engine to scan a string.
"""

from src.services.security.prompt_injection_detector import (
    DetectionResult,
    PromptInjectionDetector,
)
from src.services.security.scanner_pipeline import ScanResult, security_scanner
from src.services.security.secret_leak_detector import (
    SecretLeakResult,
    detect,
    redact,
)
from src.services.security.tool_capability_manifest import (
    ToolCapability,
    apply_spotlighting_wrappers,
    assess_destructive_risk,
    assess_mixed_task,
    assess_trifecta,
    run_crew_security_checks,
)

__all__ = [
    "DetectionResult",
    "PromptInjectionDetector",
    "ScanResult",
    "SecretLeakResult",
    "ToolCapability",
    "apply_spotlighting_wrappers",
    "assess_destructive_risk",
    "assess_mixed_task",
    "assess_trifecta",
    "detect",
    "redact",
    "run_crew_security_checks",
    "security_scanner",
]

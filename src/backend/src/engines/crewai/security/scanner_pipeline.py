"""
Unified security scanner pipeline.

Centralises all security scanning (prompt injection detection, secret leak
detection) behind a single entry point with:
  - Singleton detector instances (avoids repeated object creation)
  - Configurable severity thresholds
  - Structured audit logging

This module is log-only — it never blocks execution.

Usage:
    from src.engines.crewai.security.scanner_pipeline import security_scanner

    # Scan text for all known threats
    result = security_scanner.scan(text, context="task_callback")

    # Scan for injection only
    inj = security_scanner.scan_injection(text)

    # Scan for secrets only
    sec = security_scanner.scan_secrets(text)
"""

import logging
from dataclasses import dataclass

from src.engines.crewai.security.prompt_injection_detector import (
    DetectionResult,
    PromptInjectionDetector,
)
from src.engines.crewai.security.secret_leak_detector import (
    SecretLeakResult,
    detect as _detect_secrets,
)

logger = logging.getLogger(__name__)


@dataclass
class ScanResult:
    """Combined result from all security scanners."""
    injection: DetectionResult
    secrets: SecretLeakResult
    has_findings: bool = False
    context: str = ""

    def __post_init__(self):
        self.has_findings = self.injection.detected or self.secrets.detected


class SecurityScannerPipeline:
    """
    Unified security scanner that holds singleton detector instances and
    provides a single entry point for all heuristic security checks.

    Thread-safe: the underlying detectors are stateless after init (compiled
    regex patterns are immutable).  The pipeline itself holds no mutable state
    beyond the detector references.
    """

    def __init__(
        self,
        min_injection_severity: str = "high",
    ) -> None:
        self._injection_detector = PromptInjectionDetector()
        self._min_severity = min_injection_severity
        # Severity ordering for threshold comparison
        self._severity_rank = {"none": 0, "low": 1, "medium": 2, "high": 3}

    # ------------------------------------------------------------------
    #  Public API
    # ------------------------------------------------------------------

    def scan(self, text: str, context: str = "") -> ScanResult:
        """
        Run all scanners on *text* and return a combined result.

        Args:
            text: Content to scan (agent output, task result, etc.)
            context: Label for audit logging (e.g. "step_callback", "flow_state")

        Returns:
            ScanResult with injection and secret findings.
        """
        inj = self.scan_injection(text)
        sec = self.scan_secrets(text)
        result = ScanResult(injection=inj, secrets=sec, context=context)

        if result.has_findings:
            self._audit_log(result)

        return result

    def scan_injection(self, text: str) -> DetectionResult:
        """Scan text for prompt injection patterns using the singleton detector."""
        return self._injection_detector.detect(text)

    def scan_secrets(self, text: str) -> SecretLeakResult:
        """Scan text for leaked credentials / secrets."""
        return _detect_secrets(text)

    def meets_severity_threshold(self, result: DetectionResult) -> bool:
        """Return True if *result* meets or exceeds the configured minimum severity."""
        return (
            self._severity_rank.get(result.severity, 0)
            >= self._severity_rank.get(self._min_severity, 0)
        )

    # ------------------------------------------------------------------
    #  Audit logging
    # ------------------------------------------------------------------

    def _audit_log(self, result: ScanResult) -> None:
        """Emit structured audit log entries for any findings."""
        ctx = f" [{result.context}]" if result.context else ""

        if result.injection.detected and self.meets_severity_threshold(result.injection):
            logger.warning(
                "[SECURITY]%s Injection detected: severity=%s patterns=%s excerpt=%.200r",
                ctx,
                result.injection.severity,
                result.injection.patterns_matched,
                result.injection.flagged_excerpt,
            )

        if result.secrets.detected:
            logger.warning(
                "[SECURITY]%s Secret leakage detected: types=%s",
                ctx,
                result.secrets.secret_types,
            )


# Module-level singleton — import this instead of creating new instances.
security_scanner = SecurityScannerPipeline()

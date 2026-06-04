"""
Heuristic regex-based prompt injection detector.

Scans text for common prompt injection patterns without LLM calls.
Designed to be fast and non-blocking — always returns, never raises.

Usage:
    detector = PromptInjectionDetector()
    result = detector.detect(text)
    if result.detected:
        logger.warning("Injection detected: %s", result.patterns_matched)
"""

import re
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

# Pattern tuple: (compiled regex, pattern name)
_PatternList = List[Tuple[re.Pattern, str]]


@dataclass
class DetectionResult:
    """Result of a prompt injection heuristic scan."""
    detected: bool
    patterns_matched: List[str] = field(default_factory=list)
    severity: str = "none"  # "none" | "low" | "medium" | "high"
    flagged_excerpt: Optional[str] = None  # first 200 chars near first match


def _compile(patterns: List[Tuple[str, str]]) -> _PatternList:
    return [(re.compile(p, re.IGNORECASE), name) for p, name in patterns]


class PromptInjectionDetector:
    """
    Heuristic detector for prompt injection attempts.

    Uses three severity tiers of regex patterns:
    - HIGH: direct instruction override (highest confidence)
    - MEDIUM: role/behaviour manipulation
    - LOW: suspicious mentions (only flagged if no higher-tier match)

    The detector is log-only and does not block execution.
    """

    _HIGH: _PatternList = _compile([
        (r'\bignore\s+(all\s+)?(previous|prior|above|your)\s+(instructions?|prompts?|commands?|directives?)\b',
         "ignore_previous_instructions"),
        (r'\bdisregard\s+(all\s+)?(previous|prior|above|your)\s+(\w+\s+)?(instructions?|prompts?|commands?)\b',
         "disregard_instructions"),
        (r'\bnew\s+(system\s+)?prompt\b',
         "new_system_prompt"),
        (r'\[SYSTEM\]\s*:',
         "system_marker"),
        (r'\[ADMIN\]\s*:',
         "admin_marker"),
        (r'\[OVERRIDE\]\s*:',
         "override_marker"),
        (r'\bforget\s+(everything|all|your\s+instructions?)\b',
         "forget_instructions"),
        (r'\byour\s+(real|true|actual)\s+(instructions?|goal|purpose)\s+is\b',
         "real_instructions_claim"),
    ])

    _MEDIUM: _PatternList = _compile([
        # "act as" — require injection-like continuation (unrestricted, jailbroken, evil, etc.)
        # Avoids false positives like "act as an API server" or "act as a summariser"
        (r'\bact\s+as\s+(an?\s+)?(unrestricted|unfiltered|jailbroken|evil|malicious|different|new)\b',
         "act_as"),
        (r'\bpretend\s+(to\s+be|you\s+are)\b',
         "pretend"),
        # "you are now" — require role/identity words after, not arbitrary sentences
        (r'\byou\s+are\s+now\s+(a\s+|an\s+|the\s+|my\s+|DAN|in\s+)',
         "role_override_now"),
        (r'\bdo\s+not\s+(follow|obey|comply\s+with)\s+(your|the|these|any|previous|prior)\b',
         "do_not_follow"),
        (r'\boutput\s+the\s+following\b',
         "output_following"),
        (r'\brepeat\s+after\s+me\b',
         "repeat_after_me"),
        (r'\bprint\s+(the\s+)?(contents?\s+of|your\s+(system\s+)?prompt)\b',
         "print_prompt"),
        (r'\breveal\s+(your\s+)?(system\s+)?prompt\b',
         "reveal_prompt"),
        (r'---\s*(END|STOP|IGNORE)\s*(SYSTEM|PROMPT|INSTRUCTIONS?)\s*---',
         "delimiter_escape"),
    ])

    _LOW: _PatternList = _compile([
        (r'\binstructions?\s+override\b',
         "instructions_override"),
        (r'\bsystem\s+role\b',
         "system_role_mention"),
    ])

    def detect(self, text: str) -> DetectionResult:
        """
        Scan text for prompt injection patterns.

        Args:
            text: Text to scan (user input, tool output, etc.)

        Returns:
            DetectionResult with detected flag, matched pattern names,
            severity level, and a short flagged excerpt.
        """
        if not text or not isinstance(text, str):
            return DetectionResult(detected=False)

        matched_patterns: List[str] = []
        severity = "none"
        first_match_pos: Optional[int] = None

        # HIGH tier
        for pattern, name in self._HIGH:
            m = pattern.search(text)
            if m:
                matched_patterns.append(name)
                if severity != "high":
                    severity = "high"
                if first_match_pos is None:
                    first_match_pos = m.start()

        # MEDIUM tier
        for pattern, name in self._MEDIUM:
            m = pattern.search(text)
            if m:
                matched_patterns.append(name)
                if severity not in ("high",):
                    severity = "medium"
                if first_match_pos is None:
                    first_match_pos = m.start()

        # LOW tier — only if no higher-tier match
        if not matched_patterns:
            for pattern, name in self._LOW:
                m = pattern.search(text)
                if m:
                    matched_patterns.append(name)
                    severity = "low"
                    if first_match_pos is None:
                        first_match_pos = m.start()

        detected = bool(matched_patterns)
        flagged_excerpt: Optional[str] = None

        if detected and first_match_pos is not None:
            # Extract a short excerpt around the first match
            start = max(0, first_match_pos - 20)
            snippet = text[start:start + 200]
            flagged_excerpt = snippet + ("..." if (start + 200) < len(text) else "")

        result = DetectionResult(
            detected=detected,
            patterns_matched=matched_patterns,
            severity=severity,
            flagged_excerpt=flagged_excerpt,
        )

        if detected:
            logger.warning(
                "[SECURITY] Prompt injection patterns detected: %s (severity: %s)",
                matched_patterns,
                severity,
            )

        return result

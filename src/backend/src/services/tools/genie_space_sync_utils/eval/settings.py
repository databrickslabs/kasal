"""Parsing of the eval action's numeric settings (pure)."""

from __future__ import annotations

DEFAULT_MAX_QUESTIONS = 50
MAX_QUESTIONS_CEILING = 200


def parse_limit(value: object) -> int:
    """``max_questions``: a whole number from 1 to MAX_QUESTIONS_CEILING; the default when unset."""
    if value is None or value == "":
        return DEFAULT_MAX_QUESTIONS
    if isinstance(value, bool):
        raise ValueError("max_questions must be a whole number")
    try:
        number = int(str(value).strip())
    except ValueError:
        raise ValueError("max_questions must be a whole number") from None
    if not 1 <= number <= MAX_QUESTIONS_CEILING:
        raise ValueError(f"max_questions must be between 1 and {MAX_QUESTIONS_CEILING}")
    return number


def parse_price(value: object) -> float | None:
    """``price_per_1k_tokens``: a non-negative number, or None when no price was given."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("price_per_1k_tokens must be a number")
    try:
        price = float(str(value).strip())
    except ValueError:
        raise ValueError("price_per_1k_tokens must be a number") from None
    if price < 0 or price != price:
        raise ValueError("price_per_1k_tokens must be zero or more")
    return price

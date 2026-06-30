"""Tests for pick_legacy_route — the no-expression router value matcher.

This is the branch that caused the has_results "true"/"True" flakiness: it did
raw `condition_value is True` / `== route_name` with no coercion, so a crew that
emitted the field as a string silently failed to route. pick_legacy_route now
coerces first.
"""

from src.engines.crewai.paths.flow.modules.flow_builder import pick_legacy_route


def test_string_true_routes_like_bool_to_success():
    # The exact regression: "true"/"True" must hit the success/true route.
    assert pick_legacy_route("true", ["failed", "success"]) == "success"
    assert pick_legacy_route("True", ["success", "failed"]) == "success"


def test_bool_true_routes_to_success():
    assert pick_legacy_route(True, ["failed", "success"]) == "success"


def test_string_false_routes_to_failure():
    assert pick_legacy_route("false", ["failure", "success"]) == "failure"
    assert pick_legacy_route(False, ["failed", "success"]) == "failed"


def test_exact_route_name_match_wins():
    assert pick_legacy_route("route_x", ["route_x", "route_y"]) == "route_x"


def test_falls_back_to_first_route_when_no_match():
    # No name match, value isn't a bool mapping to these names → first route.
    assert pick_legacy_route("something", ["route_a", "route_b"]) == "route_a"


def test_empty_routes_returns_default():
    assert pick_legacy_route(True, []) == "default"

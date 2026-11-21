"""A measure that is only arithmetic over other measures of its view takes the weakest verdict of
the measures it reads (transitively). Names here are made up."""

from src.services.tools.metric_view_validation_utils.composed_verdicts import (
    COMPOSED_DIFFERENCE,
    resolve_composed_verdicts,
)
from src.services.tools.metric_view_validation_utils.expression_validator import (
    ExpressionValidator,
)


def _entry(name: str, status: str, refs=None, diffs=None) -> dict:
    return {
        "measure_eval": "matched",
        "measure_name": name,
        "measure_eval_result": {
            "measure_name": name,
            "status": status,
            "is_valid": status == "VALID",
            "confidence": "medium",
            "differences": list(diffs or []),
            "similarities": [],
            "recommendations": [],
            "composed_refs": list(refs or []),
        },
    }


def _view(*names: str) -> set:
    return {n.replace("_", "").lower() for n in names}


def _status(entries, name):
    return next(e for e in entries if e["measure_name"] == name)["measure_eval_result"]


def _composed(name, refs):
    return _entry(
        name, "REVIEW", refs=refs, diffs=[f"{COMPOSED_DIFFERENCE}: judged by its rows"]
    )


def test_all_referenced_valid_gives_valid_and_names_them():
    entries = [
        _entry("alpha_part", "VALID"),
        _entry("beta_part", "VALID"),
        _composed("gamma_total", ["alphapart", "betapart"]),
    ]
    resolve_composed_verdicts(entries, _view("alpha_part", "beta_part", "gamma_total"))
    verdict = _status(entries, "gamma_total")
    assert verdict["status"] == "VALID" and verdict["is_valid"] is True
    assert verdict["differences"] == []
    assert any(
        "alpha_part (VALID)" in s and "beta_part (VALID)" in s
        for s in verdict["similarities"]
    )


def test_any_equivalent_and_nothing_worse_gives_equivalent():
    entries = [
        _entry("alpha_part", "VALID"),
        _entry("beta_part", "EQUIVALENT"),
        _composed("gamma_total", ["alphapart", "betapart"]),
    ]
    resolve_composed_verdicts(entries, _view("alpha_part", "beta_part", "gamma_total"))
    assert _status(entries, "gamma_total")["status"] == "EQUIVALENT"


def test_a_review_referenced_measure_keeps_review_and_names_it():
    entries = [
        _entry("alpha_part", "REVIEW"),
        _entry("beta_part", "VALID"),
        _composed("gamma_total", ["alphapart", "betapart"]),
    ]
    resolve_composed_verdicts(entries, _view("alpha_part", "beta_part", "gamma_total"))
    verdict = _status(entries, "gamma_total")
    assert verdict["status"] == "REVIEW"
    assert any("alpha_part (REVIEW)" in r for r in verdict["recommendations"])
    assert any(d.startswith(COMPOSED_DIFFERENCE) for d in verdict["differences"])


def test_an_invalid_referenced_measure_keeps_review_not_invalid():
    entries = [_entry("alpha_part", "INVALID"), _composed("gamma_total", ["alphapart"])]
    resolve_composed_verdicts(entries, _view("alpha_part", "gamma_total"))
    assert _status(entries, "gamma_total")["status"] == "REVIEW"


def test_a_referenced_name_outside_the_view_is_not_judged_and_keeps_review():
    entries = [
        _entry("alpha_part", "VALID"),
        _composed("gamma_total", ["alphapart", "outsidename"]),
    ]
    resolve_composed_verdicts(entries, _view("alpha_part", "gamma_total"))
    verdict = _status(entries, "gamma_total")
    assert verdict["status"] == "REVIEW"
    assert any("outsidename (not judged)" in r for r in verdict["recommendations"])


def test_resolution_is_transitive_through_a_composed_measure():
    entries = [
        _entry("alpha_part", "VALID"),
        _entry("beta_part", "VALID"),
        _composed("middle_total", ["alphapart", "betapart"]),
        _composed("outer_total", ["middletotal"]),
    ]
    resolve_composed_verdicts(
        entries, _view("alpha_part", "beta_part", "middle_total", "outer_total")
    )
    assert _status(entries, "middle_total")["status"] == "VALID"
    assert _status(entries, "outer_total")["status"] == "VALID"


def test_a_cycle_keeps_review():
    entries = [
        _composed("first_total", ["secondtotal"]),
        _composed("second_total", ["firsttotal"]),
    ]
    resolve_composed_verdicts(entries, _view("first_total", "second_total"))
    assert _status(entries, "first_total")["status"] == "REVIEW"
    assert _status(entries, "second_total")["status"] == "REVIEW"


def test_a_measure_without_composed_refs_is_not_changed():
    entries = [
        _entry("alpha_part", "VALID"),
        _entry("beta_part", "REVIEW", diffs=["Aggregation mismatch: made-up"]),
    ]
    resolve_composed_verdicts(entries, _view("alpha_part", "beta_part"))
    assert _status(entries, "beta_part")["status"] == "REVIEW"


def test_validate_marks_a_same_shape_composed_measure_and_not_a_shape_difference():
    validator = ExpressionValidator(
        data_handler=None, table_mappings={}, column_mappings={}
    )
    same = validator.validate(
        "MEASURE(part_one) - MEASURE(part_two)", "[Part One] - [Part Two]"
    )
    assert same["composed_refs"] == ["partone", "parttwo"]
    other = validator.validate(
        "MEASURE(part_one) - MEASURE(part_two)", "[Part Two] - [Part One]"
    )
    assert other["composed_refs"] == []
    assert any(d.startswith("Structure mismatch") for d in other["differences"])

"""NORM.S.DIST / NORM.S.INV translation, checked numerically.

The generated SQL is evaluated in an in-memory SQLite database. SQLite lacks SQRT, EXP, LN,
SIGN and PI in some builds, so they are registered as Python UDFs with the same semantics as
Spark / Databricks SQL. Reference values come from ``statistics.NormalDist``.
"""

import math
import sqlite3
import statistics

import pytest

from src.services.tools.metric_view_utils.dax_normal_sql import (
    normal_cdf_sql,
    normal_inv_sql,
    normal_pdf_sql,
)
from src.services.tools.metric_view_utils.dax_patterns import match_norm_s
from src.services.tools.metric_view_utils.dax_translator import DaxTranslator

REF = statistics.NormalDist()
Z_POINTS = [-6.0, -3.2, -1.96, -1.5, -0.3, 0.0, 0.0001, 0.7, 1.645, 2.5, 3.9, 6.0]
P_POINTS = [
    1e-10,
    1e-6,
    0.001,
    0.02424,
    0.02426,
    0.05,
    0.3,
    0.5,
    0.9,
    0.975,
    0.999,
    1 - 1e-9,
]
#: Measured length guard: one NORM.S.INV over a short column reference stays well under this.
MAX_INV_CHARS = 8000


def _sqlite() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")

    def _null_safe(fn):
        def wrapper(*args):
            if any(a is None for a in args):
                return None
            return fn(*args)

        return wrapper

    conn.create_function("SQRT", 1, _null_safe(math.sqrt), deterministic=True)
    conn.create_function("EXP", 1, _null_safe(math.exp), deterministic=True)
    conn.create_function(
        "LN",
        1,
        _null_safe(lambda x: math.log(x) if x > 0 else None),
        deterministic=True,
    )
    conn.create_function(
        "SIGN",
        1,
        _null_safe(lambda x: float((x > 0) - (x < 0))),
        deterministic=True,
    )
    conn.create_function("PI", 0, lambda: math.pi, deterministic=True)
    return conn


def _evaluate(sql_expr: str, value: float | None) -> float | None:
    conn = _sqlite()
    try:
        row = conn.execute(
            f"SELECT {sql_expr} FROM (SELECT ? AS p) AS source", (value,)
        ).fetchone()
    finally:
        conn.close()
    return row[0]


def _cdf(z: float) -> float:
    return _evaluate(normal_cdf_sql("source.p"), z)


class TestCdf:
    @pytest.mark.parametrize("z", Z_POINTS)
    def test_matches_normal_cdf_within_approximation_bound(self, z):
        # A&S 7.1.26: |error(erf)| < 1.5e-7, so |error(Phi)| < 7.5e-8.
        assert abs(_cdf(z) - REF.cdf(z)) < 1e-7

    def test_symmetry(self):
        assert _cdf(1.3) + _cdf(-1.3) == pytest.approx(1.0, abs=2e-7)

    def test_null_propagates(self):
        assert _evaluate(normal_cdf_sql("source.p"), None) is None

    def test_standard_critical_value(self):
        assert _cdf(1.959963984540054) == pytest.approx(0.975, abs=1e-7)


class TestPdf:
    @pytest.mark.parametrize("z", Z_POINTS)
    def test_matches_normal_pdf(self, z):
        assert _evaluate(normal_pdf_sql("source.p"), z) == pytest.approx(
            REF.pdf(z), rel=1e-12, abs=1e-15
        )


class TestInv:
    @pytest.mark.parametrize("p", P_POINTS)
    def test_matches_normal_inv_within_acklam_bound(self, p):
        expected = REF.inv_cdf(p)
        got = _evaluate(normal_inv_sql("source.p"), p)
        # Acklam: relative error < 1.15e-9; allow a little headroom for float rounding.
        assert got == pytest.approx(expected, rel=2e-9, abs=2e-9)

    @pytest.mark.parametrize("p", [0.0, 1.0, -0.2, 1.5, None])
    def test_outside_open_unit_interval_is_null(self, p):
        assert _evaluate(normal_inv_sql("source.p"), p) is None

    def test_round_trip_through_cdf(self):
        for p in (0.01, 0.2, 0.8, 0.995):
            x = _evaluate(normal_inv_sql("source.p"), p)
            assert _cdf(x) == pytest.approx(p, abs=2e-7)

    def test_expression_length_is_bounded(self):
        assert len(normal_inv_sql("SUM(source.p)")) < MAX_INV_CHARS


class TestMatcher:
    @pytest.mark.parametrize(
        "dax,fn,kind",
        [
            ("NORM.S.DIST(x, TRUE)", "DIST", "CDF"),
            ("NORM.S.DIST(x, FALSE())", "DIST", "PDF"),
            ("NORM.S.DIST(x, 1)", "DIST", "CDF"),
            ("NORM.S.DIST(x, flag)", "DIST", None),
        ],
    )
    def test_dist_forms(self, dax, fn, kind):
        match = match_norm_s(dax, "m")
        assert match["fn"] == fn and match["kind"] == kind

    def test_inv_form(self):
        assert match_norm_s("NORM.S.INV(SUM(T[p]))", "m") == {
            "fn": "INV",
            "arg": "SUM(T[p])",
        }

    @pytest.mark.parametrize(
        "dax",
        [
            "NORM.S.INV(a) + NORM.S.INV(b)",
            "NORM.S.DIST(a, TRUE) * 2",
            "1 - NORM.S.DIST(a, TRUE)",
            "NORM.S.INV(a, b)",
            "NORM.S.DIST(a)",
            "SUM(T[x])",
        ],
    )
    def test_non_whole_calls_do_not_match(self, dax):
        assert match_norm_s(dax, "m") is None


class TestTranslatorIntegration:
    @pytest.fixture
    def translator(self):
        return DaxTranslator()

    def _translate(self, translator, dax):
        return translator.translate(
            {"measure_name": "m", "dax_expression": dax, "original_name": "m"},
            "fact",
        )

    def test_cdf_measure_translates_and_evaluates(self, translator):
        result = self._translate(translator, "NORM.S.DIST(SUM(Sales[Z]), TRUE)")
        assert result.is_translatable is True
        assert "SQRT" in result.sql_expr and "SIGN" in result.sql_expr
        # SUM over a single row of z: the expression must evaluate to Phi(z).
        conn = _sqlite()
        conn.execute("CREATE TABLE source (z REAL)")
        conn.execute("INSERT INTO source VALUES (1.25)")
        value = conn.execute(f"SELECT {result.sql_expr} FROM source").fetchone()[0]
        conn.close()
        assert value == pytest.approx(REF.cdf(1.25), abs=1e-7)

    def test_pdf_measure_translates(self, translator):
        result = self._translate(translator, "NORM.S.DIST(SUM(Sales[Z]), FALSE)")
        assert result.is_translatable is True
        assert "PI()" in result.sql_expr

    def test_inv_measure_translates(self, translator):
        result = self._translate(translator, "NORM.S.INV(SUM(Sales[P]))")
        assert result.is_translatable is True
        assert "CASE WHEN" in result.sql_expr

    def test_non_literal_cumulative_flag_is_untranslatable_with_reason(
        self, translator
    ):
        result = self._translate(translator, "NORM.S.DIST(SUM(Sales[Z]), Flag)")
        assert result.is_translatable is False
        assert "TRUE or FALSE" in result.skip_reason

    def test_untranslatable_argument_gives_reason(self, translator):
        result = self._translate(translator, "NORM.S.INV([Other Measure])")
        assert result.is_translatable is False
        assert "argument not translatable" in result.skip_reason

    def test_norm_s_is_on_the_deterministic_fast_path(self):
        from src.services.tools.metric_view_utils import dax_patterns

        assert "norm_s_dist_inv" in dax_patterns.DEFECT_FAST_PATH

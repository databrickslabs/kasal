"""Inline Spark SQL for the standard normal distribution (DAX NORM.S.DIST / NORM.S.INV).

Databricks SQL has no normal CDF or quantile builtin, so the functions are expanded inline:

- CDF  Phi(z) = 1/2 * (1 + sign(z) * erf(|z| / sqrt 2)), with erf from the Abramowitz and
  Stegun 7.1.26 rational approximation: |error| < 1.5e-7 on erf, hence < 7.5e-8 on Phi.
- PDF  phi(z) = exp(-z^2 / 2) / sqrt(2 pi). Exact.
- Quantile Phi^-1(p): Acklam's rational approximation, relative error < 1.15e-9 on (0, 1).
  Outside the open interval the result is NULL (DAX NORM.S.INV raises there).

The argument is an aggregate SQL expression and is repeated in the output. Each copy is the
same group aggregate, so the value is unchanged, but the expression grows (see
``MAX_INV_CHARS`` in the tests). Only SQL functions that Spark, Databricks SQL and DuckDB all
provide are used: ABS, SQRT, EXP, LN, SIGN, PI, CASE.
"""

from __future__ import annotations

# Abramowitz & Stegun 7.1.26: erf(x) ~ 1 - (a1 t + ... + a5 t^5) exp(-x^2), t = 1/(1 + p x).
_ERF_P = 0.3275911
_ERF_COEFFS = (0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429)

# Acklam: central region rational in r = q^2 (q = p - 1/2); tails rational in q = sqrt(-2 ln p).
_ACKLAM_A = (
    -3.969683028665376e01,
    2.209460984245205e02,
    -2.759285104469687e02,
    1.383577518672690e02,
    -3.066479806614716e01,
    2.506628277459239e00,
)
_ACKLAM_B = (
    -5.447609879822406e01,
    1.615858368580409e02,
    -1.556989798598866e02,
    6.680131188771972e01,
    -1.328068155288572e01,
)
_ACKLAM_C = (
    -7.784894002430293e-03,
    -3.223964580411365e-01,
    -2.400758277161838e00,
    -2.549732539343734e00,
    4.374664141464968e00,
    2.938163982698783e00,
)
_ACKLAM_D = (
    7.784695709041462e-03,
    3.224671290700398e-01,
    2.445134137142996e00,
    3.754408661907416e00,
)
_P_LOW = 0.02425
_P_HIGH = 0.97575


def _horner(x: str, coeffs: tuple[float, ...]) -> str:
    """((c0 * x + c1) * x + c2) ... as SQL. ``x`` must be a parenthesised expression."""
    expr = repr(coeffs[0])
    for coeff in coeffs[1:]:
        expr = f"({expr} * {x} + ({coeff!r}))"
    return expr


def normal_pdf_sql(z: str) -> str:
    """phi(z) = exp(-z^2 / 2) / sqrt(2 pi)."""
    return f"(EXP(-(({z}) * ({z})) / 2) / SQRT(2 * PI()))"


def normal_cdf_sql(z: str) -> str:
    """Phi(z) via the A&S 7.1.26 erf approximation (|error| < 7.5e-8)."""
    x = f"(ABS({z}) / SQRT(2))"
    t = f"(1 / (1 + {_ERF_P!r} * {x}))"
    # p(t) = t * (a1 + t * (a2 + t * (a3 + t * (a4 + t * a5))))
    polynomial = repr(_ERF_COEFFS[4])
    for coeff in reversed(_ERF_COEFFS[:4]):
        polynomial = f"({coeff!r} + {t} * {polynomial})"
    erf_abs = f"(1 - {t} * {polynomial} * EXP(-(({z}) * ({z})) / 2))"
    return f"(0.5 * (1 + SIGN({z}) * {erf_abs}))"


def _acklam_central(p: str) -> str:
    q = f"({p} - 0.5)"
    r = f"({q} * {q})"
    numerator = f"({_horner(r, _ACKLAM_A)} * {q})"
    denominator = f"({_horner(r, _ACKLAM_B)} * {r} + 1)"
    return f"({numerator} / {denominator})"


def _acklam_lower(p: str) -> str:
    q = f"SQRT(-2 * LN({p}))"
    return f"({_horner(q, _ACKLAM_C)} / ({_horner(q, _ACKLAM_D)} * {q} + 1))"


def _acklam_upper(p: str) -> str:
    q = f"SQRT(-2 * LN(1 - {p}))"
    return f"(-{_horner(q, _ACKLAM_C)} / ({_horner(q, _ACKLAM_D)} * {q} + 1))"


def normal_inv_sql(p: str) -> str:
    """Phi^-1(p) via Acklam's approximation; NULL unless 0 < p < 1."""
    pe = f"({p})"
    return (
        f"(CASE WHEN {pe} IS NULL OR {pe} <= 0 OR {pe} >= 1 THEN NULL "
        f"WHEN {pe} < {_P_LOW!r} THEN {_acklam_lower(pe)} "
        f"WHEN {pe} > {_P_HIGH!r} THEN {_acklam_upper(pe)} "
        f"ELSE {_acklam_central(pe)} END)"
    )

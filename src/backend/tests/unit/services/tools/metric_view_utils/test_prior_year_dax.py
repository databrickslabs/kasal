"""Prior-year measures: the year shift is found in the DAX and taken out."""

import pytest

from src.services.tools.metric_view_utils.prior_year_dax import unshifted


@pytest.mark.parametrize(
    "dax, expected",
    [
        ("CALCULATE([Sales], SAMEPERIODLASTYEAR(Calendar[Date]))", "[Sales]"),
        ("Calculate([Sales],SAMEPERIODLASTYEAR('My Calendar'[Date]))", "[Sales]"),
        ("CALCULATE([Sales], DATEADD(Calendar[Date], -1, YEAR))", "[Sales]"),
        ("CALCULATE([Sales], DATEADD(Calendar[Date], -4, QUARTER))", "[Sales]"),
        ("CALCULATE([Sales], PARALLELPERIOD(Calendar[Date], -12, MONTH))", "[Sales]"),
        (
            'CALCULATE([Sales], DATEADD(Calendar[Date], -12, MONTH), T[a] = "x")',
            'CALCULATE([Sales], T[a] = "x")',
        ),
        (
            "CALCULATE([Sales], ALL(Calendar), Calendar[Date] IN VALUES(Calendar[Date_PY]))",
            "[Sales]",
        ),
        (
            "CALCULATE(SUM(F[v]) - SUM(F[w]), SAMEPERIODLASTYEAR(Calendar[Date])) // [x]",
            "(SUM(F[v]) - SUM(F[w]))",
        ),
    ],
)
def test_the_shift_is_taken_out(dax, expected):
    found = unshifted(dax)
    assert found is not None and found.dax == expected
    assert found.column == "Date"


def test_the_calendar_is_returned_as_the_measure_spells_it():
    found = unshifted("CALCULATE([S], SAMEPERIODLASTYEAR('Fiscal Calendar'[Day]))")
    assert (found.calendar, found.column) == ("Fiscal Calendar", "Day")


def test_a_variable_that_is_the_result_becomes_the_measure():
    dax = (
        'var a = CALCULATE( SUMX(FILTER(F,F[v]="0000" && F[c] in {"A","B"}),F[value]),'
        "SAMEPERIODLASTYEAR(C[date_id])) return a"
    )
    assert (
        unshifted(dax).dax
        == 'SUMX(FILTER(F,F[v]="0000" && F[c] in {"A","B"}),F[value])'
    )


def test_variables_the_result_does_not_use_are_left_out():
    dax = (
        "var x = SELECTEDVALUE(M[Display]) var y = SELECTEDVALUE(M[Name]) "
        "return CALCULATE([Q],SAMEPERIODLASTYEAR(C[date_id]))"
    )
    assert unshifted(dax).dax == "[Q]"


def test_every_part_shifted_is_a_prior_year_measure():
    dax = (
        'var a = CALCULATE(SUM(F[v]), F[p]="1", SAMEPERIODLASTYEAR(C[d])) '
        'var b = CALCULATE(SUM(F[v]), F[p]="2", SAMEPERIODLASTYEAR(C[d])) return a - b'
    )
    assert unshifted(dax).dax == (
        'VAR a = CALCULATE(SUM(F[v]), F[p]="1")\n'
        'VAR b = CALCULATE(SUM(F[v]), F[p]="2")\n'
        "RETURN a - b"
    )
    ratio = "DIVIDE(CALCULATE([A], SAMEPERIODLASTYEAR(C[d])), CALCULATE([B], SAMEPERIODLASTYEAR(C[d])))"
    assert unshifted(ratio).dax == "DIVIDE([A], [B])"


@pytest.mark.parametrize(
    "dax",
    [
        "SUM(F[v])",
        "",
        None,
        # part of the result is this year's value
        "var a = CALCULATE([X],SAMEPERIODLASTYEAR(C[d])) var b = [X] return (b-a)*100",
        "If(ISBLANK([X]),BLANK(), CALCULATE([X],SAMEPERIODLASTYEAR(C[d])))",
        "CALCULATE(DIVIDE([A],[B]),SAMEPERIODLASTYEAR(C[d])) * [W]",
        # shifted twice, or on two calendars
        "CALCULATE(CALCULATE([A], SAMEPERIODLASTYEAR(C[d])), SAMEPERIODLASTYEAR(C[d]))",
        "CALCULATE([A], SAMEPERIODLASTYEAR(C[d])) + CALCULATE([A], SAMEPERIODLASTYEAR(D[d]))",
        # not the same period one year back
        "CALCULATE([Sales], PARALLELPERIOD(Calendar[Date], -1, YEAR))",
        "CALCULATE([Sales], DATEADD(Calendar[Date], -1, MONTH))",
        # the lookup needs the calendar's filter lifted, and a prior-year column
        "CALCULATE([Sales], Calendar[Date] IN VALUES(Calendar[Date_PY]))",
        "CALCULATE([Sales], ALL(Calendar), Calendar[Date] IN VALUES(Calendar[Holiday]))",
        # the function name inside a string, and a block inside a call
        'CALCULATE(SUM(F[v]), F[t] = "a, SAMEPERIODLASTYEAR(C[d])")',
        "SUMX(F, VAR q = F[a] RETURN CALCULATE(q, SAMEPERIODLASTYEAR(C[d])))",
    ],
)
def test_not_a_prior_year_measure_as_a_whole(dax):
    assert unshifted(dax) is None

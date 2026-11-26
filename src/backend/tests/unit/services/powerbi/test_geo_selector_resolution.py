"""A reference to a dimension-conditional SWITCH measure takes branch B, never branch A.

``SWITCH(TRUE(), Or(ISFILTERED(T[a]), HASONEVALUE(T[b])), <A>, <B>)`` picks A only when a
query filters or groups by ``T``. A metric view cannot test that, so the reference used to
hard-code A (the plant branch) in every view, including views grouped by something else.
"""

from src.services.powerbi.pipeline_config import derive_measure_resolutions
from src.services.powerbi.switch_decomposition import (
    geo_selector_split,
    is_presence_switch,
)

PRESENT = 'CALCULATE(SUM(Fact[v]), Fact[ver] = "0000", Fact[kind] = "Plant")'
ABSENT = 'CALCULATE(SUM(Fact[v]), Fact[ver] = "0000", Fact[kind] = "Company Code")'

GEO = f"""Switch(TRUE(),
        Or(ISFILTERED(Dim_Plant[plant_desc]),HASONEVALUE(Dim_Plant[plant])),
    {PRESENT},
    {ABSENT})"""


class TestGeoSelectorSplit:
    def test_splits_into_dimension_present_and_absent_branches(self):
        split = geo_selector_split(GEO)
        assert split is not None
        assert split["dimension"] == "Dim_Plant"
        assert split["present"]["base_expr"] == "SUM(source.v)"
        assert "kind = 'Plant'" in split["present"]["base_filters"]
        assert "kind = 'Company Code'" in split["absent"]["base_filters"]
        assert "kind = 'Plant'" not in split["absent"]["base_filters"]

    def test_var_block_is_stripped_before_the_switch(self):
        dax = f"var std = CALCULATE([F_Start_date])\nreturn\n{GEO}"
        split = geo_selector_split(dax)
        assert split is not None
        assert "kind = 'Company Code'" in split["absent"]["base_filters"]

    def test_slicer_condition_is_not_split(self):
        # A condition that also reads a slicer cannot be decided from the view.
        dax = GEO.replace(
            "HASONEVALUE(Dim_Plant[plant])",
            'SELECTEDVALUE(Sel[Selector])="Plant"',
        )
        assert geo_selector_split(dax) is None

    def test_condition_on_two_tables_is_not_split(self):
        dax = GEO.replace("HASONEVALUE(Dim_Plant[plant])", "HASONEVALUE(Other[x])")
        assert geo_selector_split(dax) is None

    def test_branch_that_does_not_resolve_is_not_split(self):
        dax = GEO.replace(PRESENT, "CALCULATE(Fact[v])")
        assert geo_selector_split(dax) is None

    def test_plain_measure_is_not_a_split(self):
        assert geo_selector_split("SUM(Fact[v])") is None
        assert is_presence_switch("SUM(Fact[v])") is False
        assert is_presence_switch(GEO) is True


class TestReferenceResolution:
    def _resolutions(self, referenced_dax: str, referrer_dax: str) -> dict:
        measures = [
            {"measure_name": "Geo", "expression": referenced_dax, "table_name": "Fact"},
            {"measure_name": "Ref", "expression": referrer_dax, "table_name": "Fact"},
        ]
        return derive_measure_resolutions(measures)

    def test_reference_takes_branch_b_not_branch_a(self):
        res = self._resolutions(GEO, 'CALCULATE([Geo], Fact[x] = "y")')
        entry = res["Geo"]
        assert "kind = 'Company Code'" in entry["base_filters"]
        assert "kind = 'Plant'" not in entry["base_filters"]
        assert entry["base_expr"] == "SUM(source.v)"

    def test_unsplittable_presence_switch_is_not_resolved_to_branch_a(self):
        dax = GEO.replace("HASONEVALUE(Dim_Plant[plant])", 'SELECTEDVALUE(Sel[S])="P"')
        res = self._resolutions(dax, "[Geo] * 2")
        assert res["Geo"]["base_expr"].startswith("TODO")
        assert res["Geo"]["base_filters"] == []

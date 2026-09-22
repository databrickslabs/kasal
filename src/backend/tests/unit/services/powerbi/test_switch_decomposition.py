"""Regression coverage for switch_decomposition.py's fact-table re-homing.

Real bug found on otc_management: 8 genuine SWITCH() KPI measures (`ACT vs
Target`, `Val Format %`, ...) were being decomposed correctly but parked
under the measure-holder table (`Measures_Table`) instead of the real fact
table their referenced measures actually draw from — because the raw PBI
`table_name` of a referenced measure is its *definition* table, which in a
holder-pattern model is the shared holder, not a fact. `table_processor.py`'s
Step 6 only reads `switch_decompositions[table_key]` for the table it is
CURRENTLY processing, and a holder table is never itself processed, so every
one of these 8 measures was silently dropped from every emitted view.
"""

from src.services.powerbi.switch_decomposition import (
    derive_geo_switch_decompositions,
    derive_switch_decompositions,
)

FACT_TABLES = {"Fact_OTC"}


class TestSwitchDecompositionRehoming:
    def test_single_passthrough_rehomes_to_the_referenced_measures_real_fact(self):
        """`ACT vs Target` shape: SWITCH picks between two already-defined
        measures, both of which are themselves defined on the shared holder
        table but whose DAX references Fact_OTC columns."""
        measures = [
            {
                "measure_name": "ACT vs Target",
                "table_name": "Measures_Table",
                "expression": (
                    'VAR _sel = SELECTEDVALUE(Slicer[Value])\n'
                    'VAR _KPI = [ACT]\nVAR _Target = [BP]\n'
                    'RETURN SWITCH(TRUE(), _condition, _KPI, _Target)'
                ),
            },
            {
                "measure_name": "ACT",
                # Defined on the holder table, like every other business
                # measure in this model — NOT a fact table.
                "table_name": "Measures_Table",
                "expression": "SUM(Fact_OTC[act_value])",
            },
            {
                "measure_name": "BP",
                "table_name": "Measures_Table",
                "expression": "SUM(Fact_OTC[bp_value])",
            },
        ]

        without_fact_tables = derive_switch_decompositions(measures)
        assert "Measures_Table" in without_fact_tables

        decomps = derive_switch_decompositions(measures, FACT_TABLES)
        assert "Fact_OTC" in decomps
        assert "Measures_Table" not in decomps
        entry = decomps["Fact_OTC"][0]
        assert entry["original_name"] == "ACT vs Target"

    def test_composite_branch_rehomes_using_either_operand(self):
        measures = [
            {
                "measure_name": "Net Change",
                "table_name": "Measures_Table",
                "expression": (
                    'VAR _sel = SELECTEDVALUE(Slicer[Value])\n'
                    'VAR _A = [Gross]\nVAR _B = [Adjustment]\n'
                    'RETURN SWITCH(TRUE(), _cond, _A - _B)'
                ),
            },
            {
                "measure_name": "Gross",
                "table_name": "Measures_Table",
                "expression": "SUM(Fact_OTC[gross_value])",
            },
            {
                "measure_name": "Adjustment",
                "table_name": "Measures_Table",
                "expression": "SUM(Fact_OTC[adj_value])",
            },
        ]
        decomps = derive_switch_decompositions(measures, FACT_TABLES)
        assert "Fact_OTC" in decomps
        assert "Measures_Table" not in decomps

    def test_falls_back_to_home_table_when_no_fact_reference_resolves(self):
        """A measure that genuinely has nothing to do with a known fact table
        keeps its home table — never invent a fact-table attribution."""
        measures = [
            {
                "measure_name": "Display Toggle",
                "table_name": "Measures_Table",
                "expression": (
                    'VAR _sel = SELECTEDVALUE(Slicer[Value])\n'
                    'VAR _A = [SomeUnresolvable]\n'
                    'RETURN SWITCH(TRUE(), _cond, _A)'
                ),
            },
        ]
        decomps = derive_switch_decompositions(measures, FACT_TABLES)
        # Nothing resolves (SomeUnresolvable isn't in measure_by_name), so no
        # decomposition is emitted at all — not a false attribution either way.
        assert decomps == {} or "Fact_OTC" not in decomps

    def test_no_fact_tables_arg_preserves_prior_behavior(self):
        """Backward compatible: omitting fact_tables keeps the old naive
        home-table attribution (existing callers, existing tests)."""
        measures = [
            {
                "measure_name": "ACT vs Target",
                "table_name": "Measures_Table",
                "expression": (
                    'VAR _sel = SELECTEDVALUE(Slicer[Value])\n'
                    'VAR _KPI = [ACT]\nRETURN SWITCH(TRUE(), _c, _KPI)'
                ),
            },
            {
                "measure_name": "ACT",
                "table_name": "Measures_Table",
                "expression": "SUM(Fact_OTC[act_value])",
            },
        ]
        decomps = derive_switch_decompositions(measures)
        assert "Measures_Table" in decomps


class TestGeoSwitchDecompositionRehoming:
    def test_geo_selector_rehomes_to_its_own_real_fact_table(self):
        measures = [
            {
                "measure_name": "Plant_Comp KBI_Value_Actual",
                "original_name": "Plant_Comp KBI_Value_Actual",
                "table_name": "Measures_Table",
                "expression": (
                    "SWITCH(TRUE(),\n"
                    "  OR(ISFILTERED(Dim_Plant[plant_desc]), HASONEVALUE(Dim_Plant[plant])),\n"
                    "  CALCULATE(SUM(Fact_OTC[kbi_value]), Fact_OTC[creg_type]=\"Plant\"),\n"
                    "  CALCULATE(SUM(Fact_OTC[kbi_value]), Fact_OTC[creg_type]=\"Company Code\"))"
                ),
            },
        ]
        decomps = derive_geo_switch_decompositions(measures, FACT_TABLES)
        assert "Fact_OTC" in decomps
        assert "Measures_Table" not in decomps
        assert len(decomps["Fact_OTC"]) == 2

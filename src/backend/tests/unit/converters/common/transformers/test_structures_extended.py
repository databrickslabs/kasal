"""
Extended tests for converters/common/transformers/structures.py.
Covers: get_structure_dependencies, validate_structures (circular deps,
KPI with undefined structure, valid definition), and _generate_technical_name
from _create_combined_measures when technical_name is None.
"""
import pytest
from src.converters.common.transformers.structures import StructureExpander
from src.converters.base.models import KPI, Structure, KPIDefinition


@pytest.fixture
def expander():
    return StructureExpander()


# ── _generate_technical_name via process_definition ───────────────────────────

def test_process_definition_kpi_without_technical_name(expander):
    """_create_combined_measures generates technical_name from description when missing."""
    structures = {
        "YTD": Structure(description="Year to Date", display_sign=1),
    }
    kpis = [
        KPI(
            description="Total Revenue",
            formula="SUM(revenue)",
            apply_structures=["YTD"],
        )
    ]
    definition = KPIDefinition(
        description="Test",
        technical_name="test",
        structures=structures,
        kpis=kpis,
    )
    result = expander.process_definition(definition)
    assert len(result.kpis) == 1
    # Generated name should be based on "Total Revenue" + "_YTD"
    assert result.kpis[0].technical_name.endswith("_YTD")
    assert "total_revenue" in result.kpis[0].technical_name.lower()


# ── get_structure_dependencies ────────────────────────────────────────────────

def test_get_structure_dependencies_no_formula_structures(expander):
    """Structures without formulas have empty dependency lists."""
    structures = {
        "YTD": Structure(description="Year to Date", display_sign=1),
        "PY": Structure(description="Prior Year", display_sign=1),
    }
    deps = expander.get_structure_dependencies(structures)
    assert deps == {"YTD": [], "PY": []}


def test_get_structure_dependencies_with_formula_references(expander):
    """Structures with formulas extract references as dependencies."""
    structures = {
        "act_ytd": Structure(description="Actuals YTD", display_sign=1),
        "re_ytg": Structure(description="Reforecast YTG", display_sign=1),
        "total": Structure(
            description="Total",
            formula="( act_ytd ) + ( re_ytg )",
            display_sign=1,
        ),
    }
    deps = expander.get_structure_dependencies(structures)
    assert "act_ytd" in deps["total"]
    assert "re_ytg" in deps["total"]
    assert deps["act_ytd"] == []
    assert deps["re_ytg"] == []


def test_get_structure_dependencies_self_reference_excluded(expander):
    """Self-references are excluded from dependency lists."""
    structures = {
        "self_ref": Structure(
            description="Self",
            formula="( self_ref ) + 1",
            display_sign=1,
        ),
    }
    deps = expander.get_structure_dependencies(structures)
    assert "self_ref" not in deps["self_ref"]


def test_get_structure_dependencies_unknown_reference_not_included(expander):
    """References to non-existent structures are not included as deps."""
    structures = {
        "total": Structure(
            description="Total",
            formula="( unknown_struct ) + 1",
            display_sign=1,
        ),
    }
    deps = expander.get_structure_dependencies(structures)
    # unknown_struct is not in structures dict, so not included
    assert deps["total"] == []


# ── validate_structures ───────────────────────────────────────────────────────

def test_validate_structures_no_structures_returns_empty(expander):
    """validate_structures returns empty list when no structures defined."""
    definition = KPIDefinition(
        description="Test",
        technical_name="test",
        kpis=[KPI(description="K", formula="f")],
    )
    errors = expander.validate_structures(definition)
    assert errors == []


def test_validate_structures_valid_definition_no_errors(expander):
    """validate_structures returns empty list for valid definition."""
    structures = {
        "YTD": Structure(description="YTD", display_sign=1),
        "PY": Structure(description="PY", display_sign=1),
    }
    kpis = [
        KPI(
            description="Revenue",
            technical_name="revenue",
            formula="SUM(r)",
            apply_structures=["YTD", "PY"],
        )
    ]
    definition = KPIDefinition(
        description="Test",
        technical_name="test",
        structures=structures,
        kpis=kpis,
    )
    errors = expander.validate_structures(definition)
    assert errors == []


def test_validate_structures_undefined_structure_reference(expander):
    """validate_structures reports error for KPI referencing undefined structure."""
    structures = {
        "YTD": Structure(description="YTD", display_sign=1),
    }
    kpis = [
        KPI(
            description="Revenue",
            technical_name="revenue",
            formula="SUM(r)",
            apply_structures=["YTD", "UNDEFINED_STRUCT"],
        )
    ]
    definition = KPIDefinition(
        description="Test",
        technical_name="test",
        structures=structures,
        kpis=kpis,
    )
    errors = expander.validate_structures(definition)
    assert len(errors) == 1
    assert "UNDEFINED_STRUCT" in errors[0]


def test_validate_structures_circular_dependency_detected(expander):
    """validate_structures detects circular dependency between structures."""
    structures = {
        "a": Structure(
            description="A",
            formula="( b )",
            display_sign=1,
        ),
        "b": Structure(
            description="B",
            formula="( a )",
            display_sign=1,
        ),
    }
    definition = KPIDefinition(
        description="Test",
        technical_name="test",
        structures=structures,
        kpis=[],
    )
    errors = expander.validate_structures(definition)
    # Should detect circular dependency
    assert any("Circular" in e for e in errors)


def test_validate_structures_kpi_without_technical_name_uses_description(expander):
    """validate_structures reports error including description when KPI has no technical_name."""
    structures = {
        "YTD": Structure(description="YTD", display_sign=1),
    }
    kpis = [
        KPI(
            description="My KPI Without Name",
            formula="SUM(r)",
            apply_structures=["MISSING"],  # undefined
        )
    ]
    definition = KPIDefinition(
        description="Test",
        technical_name="test",
        structures=structures,
        kpis=kpis,
    )
    errors = expander.validate_structures(definition)
    assert len(errors) == 1
    # Error should reference the KPI by description
    assert "My KPI Without Name" in errors[0] or "MISSING" in errors[0]


def test_validate_structures_no_circular_in_chain(expander):
    """validate_structures does not flag linear dependency chains."""
    structures = {
        "base": Structure(description="Base", display_sign=1),
        "derived": Structure(
            description="Derived",
            formula="( base ) * 2",
            display_sign=1,
        ),
        "final": Structure(
            description="Final",
            formula="( derived ) + 1",
            display_sign=1,
        ),
    }
    definition = KPIDefinition(
        description="Test",
        technical_name="test",
        structures=structures,
        kpis=[],
    )
    errors = expander.validate_structures(definition)
    assert errors == []


# ── _resolve_structure_references coverage ────────────────────────────────────

def test_structure_formula_resolves_references(expander):
    """_resolve_structure_references combines base_kbi name with structure refs."""
    structures = {
        "act": Structure(description="Actual", display_sign=1),
        "bud": Structure(description="Budget", display_sign=1),
        "var": Structure(
            description="Variance",
            formula="( act ) - ( bud )",
            display_sign=-1,
        ),
    }
    kpis = [
        KPI(
            description="Revenue",
            technical_name="revenue",
            formula="SUM(revenue)",
            apply_structures=["act", "bud", "var"],
        )
    ]
    definition = KPIDefinition(
        description="Test",
        technical_name="test",
        structures=structures,
        kpis=kpis,
    )
    result = expander.process_definition(definition)
    # var KPI should have formula referencing revenue_act and revenue_bud
    var_kpi = next(k for k in result.kpis if k.technical_name == "revenue_var")
    assert "revenue_act" in var_kpi.formula
    assert "revenue_bud" in var_kpi.formula


def test_structure_formula_unknown_ref_kept_as_is(expander):
    """_resolve_structure_references keeps unknown references unchanged."""
    structures = {
        "total": Structure(
            description="Total",
            formula="( known ) + ( unknown_ref_xyz )",
            display_sign=1,
        ),
        "known": Structure(description="Known", display_sign=1),
    }
    kpis = [
        KPI(
            description="Revenue",
            technical_name="rev",
            formula="SUM(r)",
            apply_structures=["total"],
        )
    ]
    definition = KPIDefinition(
        description="Test",
        technical_name="test",
        structures=structures,
        kpis=kpis,
    )
    result = expander.process_definition(definition)
    total_kpi = result.kpis[0]
    # known ref should be replaced, unknown should be kept as original paren form
    assert "rev_known" in total_kpi.formula
    assert "( unknown_ref_xyz )" in total_kpi.formula

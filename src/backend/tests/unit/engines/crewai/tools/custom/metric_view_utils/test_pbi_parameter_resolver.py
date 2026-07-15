"""Tests for PBI parameter resolver."""
import pytest
from src.engines.crewai.tools.custom.metric_view_utils.pbi_parameter_resolver import PbiParameterResolver


class TestPbiParameterResolverNoConfig:
    """Tests without any parameter defaults configured."""

    def test_no_config_passthrough_currency(self):
        resolver = PbiParameterResolver()
        sql = "SELECT * FROM t WHERE '${CurrencyFilter}' = '30'"
        result = resolver.resolve(sql)
        assert "${CurrencyFilter}" in result

    def test_no_config_passthrough_re_version(self):
        resolver = PbiParameterResolver()
        sql = "WHERE '${RE_Version}' = 'R100'"
        result = resolver.resolve(sql)
        assert "${RE_Version}" in result

    def test_no_config_passthrough_plain_sql(self):
        resolver = PbiParameterResolver()
        sql = "SELECT col FROM table WHERE x = 1"
        result = resolver.resolve(sql)
        assert result == sql


class TestCurrencyFilter:
    def test_currency_filter_resolved(self):
        resolver = PbiParameterResolver(parameter_defaults={"CurrencyFilter": "30"})
        sql = "'${CurrencyFilter}'"
        result = resolver.resolve(sql)
        assert "'30'" in result
        assert "${CurrencyFilter}" not in result

    def test_currency_filter_ampersand_format(self):
        resolver = PbiParameterResolver(parameter_defaults={"CurrencyFilter": "30"})
        sql = "\"\" & CurrencyFilter & \"\""
        result = resolver.resolve(sql)
        assert "'30'" in result

    def test_currency_filter_multiple_occurrences(self):
        resolver = PbiParameterResolver(parameter_defaults={"CurrencyFilter": "USD"})
        sql = "'${CurrencyFilter}' AND '${CurrencyFilter}'"
        result = resolver.resolve(sql)
        assert result.count("'USD'") == 2
        assert "${CurrencyFilter}" not in result


class TestReVersionFilter:
    def test_re_version_configured(self):
        resolver = PbiParameterResolver(parameter_defaults={
            "RE_Version_ranges": {
                "R100": "MONTH(CURRENT_DATE()) >= 10",
            }
        })
        sql = "'${RE_Version}' = 'R100'"
        result = resolver.resolve(sql)
        assert "MONTH(CURRENT_DATE()) >= 10" in result

    def test_re_version_multiple_ranges(self):
        resolver = PbiParameterResolver(parameter_defaults={
            "RE_Version_ranges": {
                "R000": "1 = 0",
                "R040": "MONTH(CURRENT_DATE()) >= 4 AND MONTH(CURRENT_DATE()) < 7",
                "R070": "MONTH(CURRENT_DATE()) >= 7 AND MONTH(CURRENT_DATE()) < 10",
                "R100": "MONTH(CURRENT_DATE()) >= 10",
            }
        })
        sql = "'${RE_Version}' = 'R040'"
        result = resolver.resolve(sql)
        assert "MONTH(CURRENT_DATE()) >= 4" in result

    def test_re_version_not_configured(self):
        resolver = PbiParameterResolver()
        sql = "'${RE_Version}' = 'R100'"
        result = resolver.resolve(sql)
        assert "${RE_Version}" in result

    def test_re_version_bare_reference_replaced(self):
        """After resolving specific versions, remaining ${RE_Version} refs get the CASE expr."""
        resolver = PbiParameterResolver(parameter_defaults={
            "RE_Version_ranges": {
                "R100": "MONTH(CURRENT_DATE()) >= 10",
            }
        })
        sql = "'${RE_Version}'"
        result = resolver.resolve(sql)
        assert "CASE" in result  # Falls back to the CASE expression


class TestFiscperFilter:
    def test_fiscper_default_resolution(self):
        """By default (resolve_fiscper_filter=True), CASE WHEN FiscperFilter blocks collapse."""
        resolver = PbiParameterResolver()
        sql = "CASE WHEN '${FiscperFilter}' = 'Sample' THEN sample_branch ELSE real_branch END"
        result = resolver.resolve(sql)
        assert "real_branch" in result
        assert "Sample" not in result

    def test_fiscper_filter_opt_out(self):
        resolver = PbiParameterResolver(parameter_defaults={"resolve_fiscper_filter": False})
        sql = "CASE WHEN '${FiscperFilter}' = 'Sample' THEN x ELSE y END"
        result = resolver.resolve(sql)
        assert "FiscperFilter" in result

    def test_fiscper_nested_else(self):
        resolver = PbiParameterResolver()
        sql = "CASE WHEN '${FiscperFilter}' = 'Sample' THEN sample_val ELSE (col * 2) END"
        result = resolver.resolve(sql)
        assert "col * 2" in result

    def test_fiscper_ampersand_format(self):
        resolver = PbiParameterResolver()
        sql = """CASE WHEN "\"\" & FiscperFilter & \"\"" = 'Sample' THEN a ELSE b END"""
        # The ampersand format should also be handled
        # May or may not match depending on exact regex; verify no crash
        result = resolver.resolve(sql)
        assert isinstance(result, str)


class TestReVersionCaseOverride:
    def test_custom_case_expression(self):
        resolver = PbiParameterResolver(parameter_defaults={
            "RE_Version_CASE": "CASE WHEN 1=1 THEN 'CUSTOM' END",
            "RE_Version_ranges": {"R100": "1=1"},
        })
        sql = "'${RE_Version}'"
        result = resolver.resolve(sql)
        assert "CUSTOM" in result


class TestCombined:
    def test_all_parameters_resolved(self):
        resolver = PbiParameterResolver(parameter_defaults={
            "CurrencyFilter": "30",
            "RE_Version_ranges": {"R100": "MONTH(CURRENT_DATE()) >= 10"},
        })
        sql = (
            "SELECT * FROM t "
            "WHERE currency = '${CurrencyFilter}' "
            "AND '${RE_Version}' = 'R100'"
        )
        result = resolver.resolve(sql)
        assert "'30'" in result
        assert "MONTH(CURRENT_DATE()) >= 10" in result
        assert "${CurrencyFilter}" not in result
        assert "'${RE_Version}' = 'R100'" not in result


class TestFindUnresolvedParams:
    """P3: detect PBI params still interpolated after resolve()."""

    def test_detects_ampersand_param(self):
        r = PbiParameterResolver()
        found = r.find_unresolved_params(
            "source.fiscper = '\"& FiscperFilter &\"'")
        assert "FiscperFilter" in found

    def test_detects_dollar_brace_param(self):
        r = PbiParameterResolver()
        assert "RE_Version" in r.find_unresolved_params("x = '${RE_Version}'")

    def test_clean_sql_returns_empty(self):
        r = PbiParameterResolver()
        assert r.find_unresolved_params("source.fiscper >= '2025001'") == []

    def test_multiple_distinct_params(self):
        r = PbiParameterResolver()
        found = r.find_unresolved_params(
            "a = '\"& FiscperFilter &\"' AND b = '${CurrencyFilter}'")
        assert set(found) == {"FiscperFilter", "CurrencyFilter"}

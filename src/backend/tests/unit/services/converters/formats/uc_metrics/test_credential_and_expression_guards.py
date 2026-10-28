"""UC-metrics conversion: credentials go only to Databricks, expressions stay
expressions.

Apps security scan follow-up (2026-10-28):

* ``DatabricksAuthService`` POSTed the service-principal client secret, and
  ``DatabricksConnector.validate_connection`` sent the workspace credential, to
  whatever ``workspace_url`` it was given — https or not, Databricks or not.
* KPI formulas and filter conditions were interpolated into metric-view SQL
  (``FILTER (WHERE …)``) that is deployed and executed, so a definition could
  smuggle a subquery reading another table under the deployer's identity.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.services.converters.formats.uc_metrics.authentication import (
    DatabricksAuthService,
    require_databricks_workspace,
)
from src.services.converters.formats.uc_metrics.connector import DatabricksConnector
from src.services.converters.formats.uc_metrics.helpers.uc_metrics_aggregations import (
    UCMetricsAggregationBuilder,
    require_safe_expression,
)
from src.utils.url_security import UnsafeUrlError

UNTRUSTED = [
    "https://attacker.example.com",
    "http://example.cloud.databricks.com",
    "https://example.cloud.databricks.com.attacker.example.com",
    "https://169.254.169.254",
]


class TestCredentialsGoOnlyToDatabricks:
    def test_a_databricks_workspace_passes(self):
        url = "https://example.cloud.databricks.com"
        assert require_databricks_workspace(url) == url

    @pytest.mark.parametrize("url", UNTRUSTED)
    def test_the_client_secret_is_never_sent_elsewhere(self, url):
        service = DatabricksAuthService(
            workspace_url=url, client_id="cid", client_secret="secret"
        )
        with (
            patch("requests.post") as post,
            pytest.raises(UnsafeUrlError),
        ):
            service._acquire_token_with_service_principal()
        post.assert_not_called()

    @pytest.mark.parametrize("url", UNTRUSTED)
    def test_the_connector_never_probes_elsewhere(self, url):
        connector = DatabricksConnector(workspace_url=url, api_key="dapi0123456789")
        with patch("requests.get") as get:
            assert connector.validate_connection() is False
        get.assert_not_called()


def _kpi(formula="amount", aggregation_type="SUM"):
    return SimpleNamespace(
        formula=formula,
        aggregation_type=aggregation_type,
        display_sign=1,
        exceptions=None,
    )


HOSTILE = [
    "1 = 1) OR (SELECT COUNT(*) FROM secrets) > 0 OR (1 = 1",
    "region = 'EU'; DROP TABLE x",
    "region = 'EU' -- the rest is gone",
    "region = 'EU' /* hidden */",
    "x $$ y",
]


class TestExpressionsStayExpressions:
    @pytest.mark.parametrize("text", HOSTILE)
    def test_hostile_text_is_refused(self, text):
        with pytest.raises(ValueError, match="Unsafe"):
            require_safe_expression(text)

    @pytest.mark.parametrize(
        "text",
        [
            "region = 'EU' AND status <> 'closed'",
            "EXTRACT(YEAR FROM order_date) = 2024",
            "TRIM(BOTH ' ' FROM code) = 'A'",
            "note = 'imported from SAP; select all -- ok'",
        ],
    )
    def test_real_expressions_pass(self, text):
        assert require_safe_expression(text) == text

    @pytest.mark.parametrize("text", HOSTILE)
    def test_the_filter_builder_refuses_a_hostile_filter(self, text):
        builder = UCMetricsAggregationBuilder()
        with pytest.raises(ValueError):
            builder.build_measure_expression_with_filter(_kpi(), text)

    def test_a_hostile_formula_is_refused_by_both_builders(self):
        builder = UCMetricsAggregationBuilder()
        kpi = _kpi(formula="(SELECT MAX(salary) FROM hr.pay)")
        with pytest.raises(ValueError):
            builder.build_measure_expression(kpi)
        with pytest.raises(ValueError):
            builder.build_measure_expression_with_filter(kpi, None)

    def test_a_legitimate_filter_is_built_unchanged(self):
        builder = UCMetricsAggregationBuilder()
        expr = builder.build_measure_expression_with_filter(_kpi(), "region = 'EU'")
        assert expr == "SUM(amount) FILTER (WHERE region = 'EU')"


def _constant_selection_kpi(formula="amount", filters=()):
    return SimpleNamespace(
        technical_name="m",
        source_table="FactSales",
        aggregation_type="SUM",
        formula=formula,
        display_sign=1,
        filters=list(filters),
        fields_for_constant_selection=["region"],
        exceptions=None,
    )


class TestYamlToUcMetricsGuardsItsOwnBuild:
    def _build(self, kpi):
        from src.services.converters.formats.uc_metrics.yaml_to_uc_metrics import (
            UCMetricsGenerator,
        )

        converter = UCMetricsGenerator()
        return converter._build_consolidated_constant_selection_uc_metrics([kpi], {})

    @pytest.mark.parametrize("text", HOSTILE)
    def test_a_hostile_kbi_filter_is_refused(self, text):
        with pytest.raises(ValueError, match="Unsafe filter"):
            self._build(_constant_selection_kpi(filters=[text]))

    def test_a_hostile_formula_is_refused(self):
        kpi = _constant_selection_kpi(formula="(SELECT 1 FROM hr.pay)")
        with pytest.raises(ValueError, match="Unsafe formula"):
            self._build(kpi)

    def test_a_legitimate_filter_still_builds(self):
        result = self._build(_constant_selection_kpi(filters=["region = 'EU'"]))
        assert "WHERE region = 'EU'" in str(result)

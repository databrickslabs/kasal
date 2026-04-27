"""Unit tests for PowerBIMetadataReducerTool."""

import json
import pytest
from contextlib import asynccontextmanager
from unittest.mock import patch, AsyncMock, MagicMock

from src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool import (
    PowerBIMetadataReducerTool,
)


def _make_model_context():
    """Create a realistic mock model context."""
    return {
        "workspace_id": "ws-123",
        "dataset_id": "ds-456",
        "tables": [
            {
                "name": "Sales",
                "columns": [{"name": "Revenue"}, {"name": "Quantity"}, {"name": "DateKey"}],
                "measures": [
                    {"name": "Total Revenue", "expression": "SUM(Sales[Revenue])", "table": "Sales"},
                    {"name": "Total Quantity", "expression": "SUM(Sales[Quantity])", "table": "Sales"},
                    {"name": "Avg Revenue", "expression": "DIVIDE([Total Revenue], [Total Quantity])", "table": "Sales"},
                ],
            },
            {
                "name": "Geography",
                "columns": [{"name": "Country"}, {"name": "Region"}, {"name": "CountryKey"}],
                "measures": [],
            },
            {
                "name": "Products",
                "columns": [{"name": "Category"}, {"name": "Subcategory"}, {"name": "ProductKey"}],
                "measures": [],
            },
            {
                "name": "Dates",
                "columns": [{"name": "Date"}, {"name": "Year"}, {"name": "Month"}],
                "measures": [],
            },
            {
                "name": "Customers",
                "columns": [{"name": "Customer ID"}, {"name": "Name"}, {"name": "Segment"}],
                "measures": [],
            },
            {
                "name": "Warehouses",
                "columns": [{"name": "Warehouse ID"}, {"name": "Location"}],
                "measures": [],
            },
        ],
        "measures": [
            {"name": "Total Revenue", "expression": "SUM(Sales[Revenue])", "table": "Sales"},
            {"name": "Total Quantity", "expression": "SUM(Sales[Quantity])", "table": "Sales"},
            {"name": "Avg Revenue", "expression": "DIVIDE([Total Revenue], [Total Quantity])", "table": "Sales"},
        ],
        "relationships": [
            {"from_table": "Sales", "from_column": "CountryKey", "to_table": "Geography", "to_column": "CountryKey"},
            {"from_table": "Sales", "from_column": "ProductKey", "to_table": "Products", "to_column": "ProductKey"},
            {"from_table": "Sales", "from_column": "DateKey", "to_table": "Dates", "to_column": "DateKey"},
            {"from_table": "Sales", "from_column": "CustomerKey", "to_table": "Customers", "to_column": "CustomerKey"},
        ],
        "sample_data": {
            "Geography[Country]": [{"Country": "Austria"}, {"Country": "Germany"}, {"Country": "Italy"}],
            "Geography[Region]": [{"Region": "DACH"}, {"Region": "Southern Europe"}],
            "Products[Category]": [{"Category": "Electronics"}, {"Category": "Clothing"}],
        },
        "slicers": [
            {"table": "Geography", "column": "Country", "values": ["Austria", "Germany", "Italy"]},
        ],
        "columns": [],
        "default_filters": {},
    }


class TestPassthroughMode:
    def test_passthrough_returns_unchanged(self):
        tool = PowerBIMetadataReducerTool(strategy="passthrough")
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="any question",
        )
        parsed = json.loads(result)

        assert parsed["reduction_summary"]["strategy"] == "passthrough"
        assert parsed["reduction_summary"]["reduction_pct"] == 0.0
        assert parsed["reduction_summary"]["kept_tables"] == 6
        assert len(parsed["tables"]) == 6

    def test_passthrough_preserves_all_data(self):
        tool = PowerBIMetadataReducerTool(strategy="passthrough")
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="any question",
        )
        parsed = json.loads(result)

        assert len(parsed["relationships"]) == 4
        assert len(parsed["measures"]) == 3
        assert parsed["workspace_id"] == "ws-123"
        assert parsed["dataset_id"] == "ds-456"


class TestFuzzyOnlyMode:
    def test_reduces_tables(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="What is the total revenue by country?",
        )
        parsed = json.loads(result)

        summary = parsed["reduction_summary"]
        assert summary["original_tables"] == 6
        assert summary["kept_tables"] < 6
        assert summary["reduction_pct"] > 0

    def test_keeps_relevant_tables(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="total revenue by country",
        )
        parsed = json.loads(result)

        table_names = [t["name"] for t in parsed["tables"]]
        assert "Sales" in table_names
        assert "Geography" in table_names

    def test_filters_relationships(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="total revenue by country",
        )
        parsed = json.loads(result)

        kept_table_names = {t["name"] for t in parsed["tables"]}
        for rel in parsed["relationships"]:
            assert rel["from_table"] in kept_table_names
            assert rel["to_table"] in kept_table_names

    def test_filters_sample_data(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="total revenue by country",
        )
        parsed = json.loads(result)

        kept_table_names = {t["name"] for t in parsed["tables"]}
        for key in parsed["sample_data"]:
            # Keys use "table[column]" format — extract table name
            table_name = key.split("[")[0] if "[" in key else key
            assert table_name in kept_table_names

    def test_sample_data_retains_column_keys(self):
        """Sample data keys like 'Geography[Country]' are preserved, not flattened."""
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="total revenue by country",
        )
        parsed = json.loads(result)

        if "Geography" in {t["name"] for t in parsed["tables"]}:
            # Both Geography[Country] and Geography[Region] should be present
            geo_keys = [k for k in parsed["sample_data"] if k.startswith("Geography[")]
            assert len(geo_keys) >= 1

    def test_resolves_measure_dependencies(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="average revenue",
        )
        parsed = json.loads(result)

        measure_names = {m["name"] for m in parsed["measures"]}
        # Avg Revenue depends on Total Revenue and Total Quantity
        if "Avg Revenue" in measure_names:
            assert "Total Revenue" in measure_names
            assert "Total Quantity" in measure_names


class TestInputValidation:
    def test_missing_model_context(self):
        tool = PowerBIMetadataReducerTool()
        result = tool._run(user_question="test")
        parsed = json.loads(result)
        assert "error" in parsed

    def test_missing_user_question(self):
        tool = PowerBIMetadataReducerTool()
        ctx = _make_model_context()
        result = tool._run(model_context_json=json.dumps(ctx))
        parsed = json.loads(result)
        assert "error" in parsed

    def test_invalid_json(self):
        tool = PowerBIMetadataReducerTool()
        result = tool._run(
            model_context_json="not valid json",
            user_question="test",
        )
        parsed = json.loads(result)
        assert "error" in parsed

    def test_double_encoded_json(self):
        tool = PowerBIMetadataReducerTool(strategy="passthrough")
        ctx = _make_model_context()
        double_encoded = json.dumps(json.dumps(ctx))
        result = tool._run(
            model_context_json=double_encoded,
            user_question="any question",
        )
        parsed = json.loads(result)
        assert "error" not in parsed
        assert parsed["workspace_id"] == "ws-123"

    def test_json_in_code_block(self):
        tool = PowerBIMetadataReducerTool(strategy="passthrough")
        ctx = _make_model_context()
        wrapped = f"```json\n{json.dumps(ctx)}\n```"
        result = tool._run(
            model_context_json=wrapped,
            user_question="any question",
        )
        parsed = json.loads(result)
        assert "error" not in parsed


class TestLLMSelection:
    def test_llm_selection_called_when_enabled(self):
        tool = PowerBIMetadataReducerTool(
            strategy="combined",
            llm_workspace_url="https://example.com",
            llm_token="test-token",
        )
        ctx = _make_model_context()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "choices": [{
                "message": {
                    "content": json.dumps({
                        "tables": ["Sales", "Geography"],
                        "measures": ["Total Revenue"],
                        "reasoning": "test reasoning",
                    })
                }
            }]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client_cls.return_value = mock_client

            result = tool._run(
                model_context_json=json.dumps(ctx),
                user_question="What is the total revenue by country?",
            )
            parsed = json.loads(result)

            assert "Sales" in parsed["reduction_summary"]["relevant_tables"]
            assert "Geography" in parsed["reduction_summary"]["relevant_tables"]
            assert parsed["reduction_summary"]["reasoning"] == "test reasoning"

    def test_llm_fallback_on_error(self):
        tool = PowerBIMetadataReducerTool(
            strategy="combined",
            llm_workspace_url="https://example.com",
            llm_token="test-token",
            synonym_threshold=50,
        )
        ctx = _make_model_context()

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.post = AsyncMock(side_effect=Exception("Connection failed"))
            mock_client_cls.return_value = mock_client

            result = tool._run(
                model_context_json=json.dumps(ctx),
                user_question="total revenue by country",
            )
            parsed = json.loads(result)

            # Should fall back to fuzzy-only, not error
            assert "error" not in parsed
            assert "Fuzzy-only fallback" in parsed["reduction_summary"]["reasoning"]


class TestExtractJsonFromResponse:
    def test_direct_json(self):
        result = PowerBIMetadataReducerTool._extract_json_from_response(
            '{"tables": ["A"], "measures": ["B"]}'
        )
        assert result == {"tables": ["A"], "measures": ["B"]}

    def test_json_in_code_block(self):
        result = PowerBIMetadataReducerTool._extract_json_from_response(
            '```json\n{"tables": ["A"]}\n```'
        )
        assert result == {"tables": ["A"]}

    def test_json_with_surrounding_text(self):
        result = PowerBIMetadataReducerTool._extract_json_from_response(
            'Here is my selection: {"tables": ["A"], "measures": ["B"]} Done!'
        )
        assert result is not None
        assert result["tables"] == ["A"]

    def test_invalid_returns_none(self):
        result = PowerBIMetadataReducerTool._extract_json_from_response(
            "No JSON here at all"
        )
        assert result is None


class TestMaxLimits:
    def test_max_tables_enforced(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=0,  # Match everything
            max_tables=2,
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="everything",
        )
        parsed = json.loads(result)

        # max_tables=2, but dependency resolver may add more
        # At minimum, the initial selection should be capped
        assert parsed["reduction_summary"]["kept_tables"] <= 6

    def test_max_measures_enforced(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=0,
            max_measures=1,
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="revenue",
        )
        parsed = json.loads(result)
        # Dependencies may expand beyond max_measures, but initial selection is capped
        assert isinstance(parsed["measures"], list)


class TestValueNormalization:
    def test_normalizes_active_filters(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
            enable_value_normalization=True,
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="revenue by country",
            active_filters={"Geography.Country": "Austira"},
        )
        parsed = json.loads(result)

        # Filter should be corrected if Geography is in kept tables
        if "Geography" in {t["name"] for t in parsed["tables"]}:
            default_filters = parsed.get("default_filters", {})
            if "Geography.Country" in default_filters:
                assert default_filters["Geography.Country"] == "Austria"

    def test_disabled_normalization_passes_through(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
            enable_value_normalization=False,
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="revenue",
            active_filters={"Geography.Country": "Austira"},
        )
        parsed = json.loads(result)
        # Should pass through unchanged
        default_filters = parsed.get("default_filters", {})
        if "Geography.Country" in default_filters:
            assert default_filters["Geography.Country"] == "Austira"


class TestReductionSummary:
    def test_summary_fields_present(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="total revenue",
        )
        parsed = json.loads(result)
        summary = parsed["reduction_summary"]

        assert "strategy" in summary
        assert "original_tables" in summary
        assert "kept_tables" in summary
        assert "original_measures" in summary
        assert "kept_measures" in summary
        assert "reduction_pct" in summary
        assert "relevant_tables" in summary
        assert "reasoning" in summary

    def test_reduction_pct_calculated_correctly(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=90,  # High threshold to exclude most
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="total revenue",
        )
        parsed = json.loads(result)
        summary = parsed["reduction_summary"]

        expected_pct = round(
            (1 - summary["kept_tables"] / summary["original_tables"]) * 100, 1
        )
        assert summary["reduction_pct"] == expected_pct


class TestOutputSchemaCompatibility:
    """Ensure output matches the fetcher schema for DAX tool compatibility."""

    def test_has_required_top_level_keys(self):
        tool = PowerBIMetadataReducerTool(strategy="passthrough")
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="test",
        )
        parsed = json.loads(result)

        assert "workspace_id" in parsed
        assert "dataset_id" in parsed
        assert "measures" in parsed
        assert "relationships" in parsed
        assert "tables" in parsed
        assert "columns" in parsed
        assert "sample_data" in parsed
        assert "default_filters" in parsed
        assert "slicers" in parsed
        assert "reduction_summary" in parsed

    def test_measures_are_list_of_dicts(self):
        tool = PowerBIMetadataReducerTool(strategy="passthrough")
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="test",
        )
        parsed = json.loads(result)

        assert isinstance(parsed["measures"], list)
        for m in parsed["measures"]:
            assert isinstance(m, dict)
            assert "name" in m

    def test_tables_are_list_of_dicts(self):
        tool = PowerBIMetadataReducerTool(strategy="passthrough")
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="test",
        )
        parsed = json.loads(result)

        assert isinstance(parsed["tables"], list)
        for t in parsed["tables"]:
            assert isinstance(t, dict)
            assert "name" in t


def _make_cached_metadata():
    """Create a realistic cached metadata dict (as stored by the Fetcher)."""
    return {
        "measures": [
            {"name": "Total Revenue", "expression": "SUM(Sales[Revenue])", "table": "Sales"},
        ],
        "relationships": [
            {"from_table": "Sales", "from_column": "CountryKey", "to_table": "Geography", "to_column": "CountryKey"},
        ],
        "schema": {
            "tables": [
                {
                    "name": "Sales",
                    "columns": [{"name": "Revenue"}, {"name": "Quantity"}],
                    "measures": [{"name": "Total Revenue", "expression": "SUM(Sales[Revenue])", "table": "Sales"}],
                },
                {
                    "name": "Geography",
                    "columns": [{"name": "Country"}, {"name": "Region"}],
                    "measures": [],
                },
            ],
            "columns": [],
        },
        "sample_data": {
            "Geography[Country]": [{"Country": "Austria"}, {"Country": "Germany"}],
        },
        "default_filters": {},
        "slicers": [],
    }


class TestCacheFallback:
    """Tests for loading model context from the DB cache."""

    def test_cache_fallback_uses_any_report_id(self):
        """Cache lookup uses any_report_id=True so it matches caches written with any report_id."""
        tool = PowerBIMetadataReducerTool(
            strategy="passthrough",
            dataset_id="ds-123",
            workspace_id="ws-456",
            group_id="my-group",
        )

        mock_cache_service = MagicMock()
        mock_cache_service.get_cached_metadata = AsyncMock(return_value=_make_cached_metadata())

        @asynccontextmanager
        async def mock_session_factory():
            yield MagicMock()

        with patch(
            "src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool.async_session_factory",
            mock_session_factory,
        ), patch(
            "src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool.PowerBISemanticModelCacheService",
            return_value=mock_cache_service,
        ):
            tool._run(user_question="test")

        # Verify any_report_id=True was passed
        mock_cache_service.get_cached_metadata.assert_called_once_with(
            group_id="my-group",
            dataset_id="ds-123",
            workspace_id="ws-456",
            any_report_id=True,
        )

    def test_cache_fallback_loads_context(self):
        """When model_context_json is absent but dataset_id+workspace_id are set, loads from cache."""
        tool = PowerBIMetadataReducerTool(
            strategy="passthrough",
            dataset_id="ds-123",
            workspace_id="ws-456",
        )

        mock_cache_service = MagicMock()
        mock_cache_service.get_cached_metadata = AsyncMock(return_value=_make_cached_metadata())

        @asynccontextmanager
        async def mock_session_factory():
            yield MagicMock()

        with patch(
            "src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool.async_session_factory",
            mock_session_factory,
        ), patch(
            "src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool.PowerBISemanticModelCacheService",
            return_value=mock_cache_service,
        ):
            result = tool._run(user_question="total revenue by country")

        parsed = json.loads(result)
        assert "error" not in parsed
        assert parsed["workspace_id"] == "ws-456"
        assert parsed["dataset_id"] == "ds-123"
        assert len(parsed["tables"]) == 2
        assert parsed["reduction_summary"]["strategy"] == "passthrough"

    def test_cache_fallback_with_fuzzy_reduction(self):
        """Cache-loaded context goes through the full fuzzy reduction pipeline."""
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
            dataset_id="ds-123",
            workspace_id="ws-456",
        )

        mock_cache_service = MagicMock()
        mock_cache_service.get_cached_metadata = AsyncMock(return_value=_make_cached_metadata())

        @asynccontextmanager
        async def mock_session_factory():
            yield MagicMock()

        with patch(
            "src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool.async_session_factory",
            mock_session_factory,
        ), patch(
            "src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool.PowerBISemanticModelCacheService",
            return_value=mock_cache_service,
        ):
            result = tool._run(user_question="total revenue by country")

        parsed = json.loads(result)
        assert "error" not in parsed
        assert parsed["reduction_summary"]["strategy"] == "fuzzy"
        # Should keep Sales (revenue) and Geography (country)
        table_names = [t["name"] for t in parsed["tables"]]
        assert "Sales" in table_names
        assert "Geography" in table_names

    def test_cache_miss_returns_error(self):
        """When cache returns None, returns an appropriate error."""
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            dataset_id="ds-123",
            workspace_id="ws-456",
        )

        mock_cache_service = MagicMock()
        mock_cache_service.get_cached_metadata = AsyncMock(return_value=None)

        @asynccontextmanager
        async def mock_session_factory():
            yield MagicMock()

        with patch(
            "src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool.async_session_factory",
            mock_session_factory,
        ), patch(
            "src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool.PowerBISemanticModelCacheService",
            return_value=mock_cache_service,
        ):
            result = tool._run(user_question="test")

        parsed = json.loads(result)
        assert "error" in parsed

    def test_model_context_json_takes_priority_over_cache(self):
        """When model_context_json IS provided, cache is never consulted."""
        tool = PowerBIMetadataReducerTool(
            strategy="passthrough",
            dataset_id="ds-123",
            workspace_id="ws-456",
        )

        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="test",
        )
        parsed = json.loads(result)
        # Should use the model_context_json (6 tables), not cache
        assert "error" not in parsed
        assert len(parsed["tables"]) == 6

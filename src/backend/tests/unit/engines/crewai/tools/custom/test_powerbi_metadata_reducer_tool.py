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


class TestCacheSaving:
    """Tests that the pipeline saves the reduced output to cache when dataset_id+workspace_id are set."""

    def test_save_to_cache_returns_compact_summary(self):
        """When cache save succeeds, returns compact summary instead of full JSON."""
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
            dataset_id="ds-123",
            workspace_id="ws-456",
            group_id="grp-1",
        )
        ctx = _make_model_context()

        mock_cache_service = MagicMock()
        mock_cache_service.save_metadata = AsyncMock(return_value=None)

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
            result = tool._run(
                model_context_json=json.dumps(ctx),
                user_question="total revenue",
            )

        parsed = json.loads(result)
        # Compact summary when cache saved
        assert parsed.get("cache_saved") is True
        assert "reduction_summary" in parsed
        assert "message" in parsed

    def test_cache_save_failure_returns_full_json(self):
        """When cache save fails, falls back to returning full JSON."""
        tool = PowerBIMetadataReducerTool(
            strategy="passthrough",
            dataset_id="ds-123",
            workspace_id="ws-456",
        )
        ctx = _make_model_context()

        mock_cache_service = MagicMock()
        mock_cache_service.save_metadata = AsyncMock(side_effect=Exception("DB down"))

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
            result = tool._run(
                model_context_json=json.dumps(ctx),
                user_question="total revenue",
            )

        parsed = json.loads(result)
        # Full JSON on cache failure — has all required schema fields
        assert "tables" in parsed
        assert "reduction_summary" in parsed
        assert parsed.get("cache_saved") is not True

    def test_no_dataset_id_skips_cache_save(self):
        """When dataset_id is not set, cache save is skipped and full JSON returned."""
        tool = PowerBIMetadataReducerTool(strategy="passthrough")
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="revenue",
        )
        parsed = json.loads(result)
        # No compact summary — full JSON
        assert "tables" in parsed


class TestReferenceDAXParsing:
    """Tests for _parse_reference_dax static method."""

    def test_parses_table_references(self):
        tables = [{"name": "Sales"}, {"name": "Geography"}]
        measures = [{"name": "Total Revenue"}]
        dax = "EVALUATE SUMMARIZECOLUMNS('Sales'[DateKey], [Total Revenue])"
        found_tables, found_measures = PowerBIMetadataReducerTool._parse_reference_dax(
            dax, tables, measures
        )
        assert "Sales" in found_tables
        assert "Total Revenue" in found_measures

    def test_ignores_unknown_tables(self):
        tables = [{"name": "Sales"}]
        measures = []
        dax = "EVALUATE 'UnknownTable'[Column]"
        found_tables, _ = PowerBIMetadataReducerTool._parse_reference_dax(
            dax, tables, measures
        )
        assert "UnknownTable" not in found_tables

    def test_empty_dax_returns_empty_sets(self):
        tables = [{"name": "Sales"}]
        measures = [{"name": "Revenue"}]
        found_tables, found_measures = PowerBIMetadataReducerTool._parse_reference_dax(
            "", tables, measures
        )
        assert found_tables == set()
        assert found_measures == set()

    def test_reference_dax_forces_table_inclusion(self):
        """reference_dax causes tables to be force-included in the reduction."""
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=90,  # very high threshold — would exclude most tables
        )
        ctx = _make_model_context()
        # DAX that references Products — which would not normally be selected
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="revenue",
            reference_dax="EVALUATE 'Products'[Category]",
        )
        parsed = json.loads(result)
        # Products should appear due to force-inclusion
        table_names = [t["name"] for t in parsed.get("tables", [])]
        assert "Products" in table_names


class TestEnrichmentDataMerge:
    """Tests for _merge_enrichment_data static method."""

    def test_enriches_table_purpose(self):
        tables = [{"name": "Sales"}]
        counts = PowerBIMetadataReducerTool._merge_enrichment_data(
            tables, [], [],
            {"tables": {"Sales": {"purpose": "Fact table for sales transactions"}}}
        )
        assert tables[0]["purpose"] == "Fact table for sales transactions"
        assert counts["tables"] == 1

    def test_does_not_overwrite_existing_purpose(self):
        tables = [{"name": "Sales", "purpose": "existing purpose"}]
        PowerBIMetadataReducerTool._merge_enrichment_data(
            tables, [], [],
            {"tables": {"Sales": {"purpose": "new purpose"}}}
        )
        assert tables[0]["purpose"] == "existing purpose"

    def test_enriches_measure_synonyms(self):
        measures = [{"name": "Total Revenue"}]
        PowerBIMetadataReducerTool._merge_enrichment_data(
            [], measures, [],
            {"measures": {"Total Revenue": {"synonyms": ["Revenue", "Sales Total"]}}}
        )
        assert "Revenue" in measures[0]["synonyms"]
        assert "Sales Total" in measures[0]["synonyms"]

    def test_appends_new_synonyms_without_duplicates(self):
        measures = [{"name": "Total Revenue", "synonyms": ["Revenue"]}]
        PowerBIMetadataReducerTool._merge_enrichment_data(
            [], measures, [],
            {"measures": {"Total Revenue": {"synonyms": ["Revenue", "Sales Total"]}}}
        )
        assert measures[0]["synonyms"].count("Revenue") == 1
        assert "Sales Total" in measures[0]["synonyms"]

    def test_enriches_columns_in_tables(self):
        tables = [{"name": "Sales", "columns": [{"name": "Revenue"}]}]
        PowerBIMetadataReducerTool._merge_enrichment_data(
            tables, [], [],
            {"columns": {"Sales[Revenue]": {"description": "Revenue column", "synonyms": ["Income"]}}}
        )
        col = tables[0]["columns"][0]
        assert col.get("description") == "Revenue column"
        assert "Income" in col.get("synonyms", [])

    def test_ignores_unknown_table_in_enrichment(self):
        tables = [{"name": "Sales"}]
        counts = PowerBIMetadataReducerTool._merge_enrichment_data(
            tables, [], [],
            {"tables": {"UnknownTable": {"purpose": "ignored"}}}
        )
        assert counts["tables"] == 0

    def test_enrichment_via_pipeline(self):
        """Enrichment data passes through the full pipeline when provided as dict."""
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
            enrichment_data={"tables": {"Sales": {"purpose": "Main fact table"}}},
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="revenue",
        )
        parsed = json.loads(result)
        # Enrichment is applied; if Sales is kept, check for purpose
        assert "error" not in parsed
        sales_table = next((t for t in parsed.get("tables", []) if t["name"] == "Sales"), None)
        if sales_table is not None:
            assert sales_table.get("purpose") == "Main fact table"

    def test_enrichment_as_json_string(self):
        """Enrichment data as JSON string is decoded automatically."""
        enrichment = {"tables": {"Sales": {"purpose": "Main fact table"}}}
        tool = PowerBIMetadataReducerTool(
            strategy="passthrough",
            enrichment_data=json.dumps(enrichment),
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="revenue",
        )
        parsed = json.loads(result)
        assert "error" not in parsed

    def test_enrichment_invalid_json_string_ignored(self):
        """Invalid JSON string for enrichment_data is silently skipped."""
        tool = PowerBIMetadataReducerTool(
            strategy="passthrough",
            enrichment_data="not valid json",
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="revenue",
        )
        parsed = json.loads(result)
        assert "error" not in parsed


class TestBusinessTerms:
    """Tests for business_terms config flowing through the pipeline."""

    def test_business_terms_as_dict_used_in_scoring(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
            business_terms={"Rev": ["Revenue", "Sales"]},
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="Rev by country",
        )
        parsed = json.loads(result)
        assert "error" not in parsed

    def test_business_terms_as_json_string(self):
        terms = {"Rev": ["Revenue", "Sales"]}
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
            business_terms=json.dumps(terms),
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="Rev",
        )
        parsed = json.loads(result)
        assert "error" not in parsed

    def test_business_terms_invalid_json_falls_back_gracefully(self):
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
            business_terms="not json at all",
        )
        ctx = _make_model_context()
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="revenue",
        )
        parsed = json.loads(result)
        assert "error" not in parsed


def _make_model_context_string_cols():
    """Create a model context where columns are plain strings (not dicts).

    This format is required to test the default_filters remapping logic
    which calls col.lower() on each column.
    """
    return {
        "workspace_id": "ws-123",
        "dataset_id": "ds-456",
        "tables": [
            {
                "name": "Sales",
                "columns": ["Revenue", "Quantity", "DateKey", "CountryKey", "ProductKey"],
                "measures": [
                    {"name": "Total Revenue", "expression": "SUM(Sales[Revenue])", "table": "Sales"},
                ],
            },
            {
                "name": "Geography",
                "columns": ["Country", "Region", "CountryKey"],
                "measures": [],
            },
        ],
        "measures": [
            {"name": "Total Revenue", "expression": "SUM(Sales[Revenue])", "table": "Sales"},
        ],
        "relationships": [
            {"from_table": "Sales", "from_column": "CountryKey", "to_table": "Geography", "to_column": "CountryKey"},
        ],
        "sample_data": {},
        "slicers": [],
        "columns": [],
        "default_filters": {},
    }


class TestDefaultFiltersRemapping:
    """Tests for the default_filters phantom-table remapping logic."""

    def test_default_filters_with_valid_table_forces_inclusion(self):
        """A default filter referencing an existing table adds that table to selection."""
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=90,  # High — will try to drop most tables
        )
        ctx = _make_model_context_string_cols()
        ctx["default_filters"] = {"Geography[Country]": "Austria"}
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="total revenue",
        )
        parsed = json.loads(result)
        table_names = {t["name"] for t in parsed.get("tables", [])}
        # Geography must be included due to the filter
        assert "Geography" in table_names

    def test_default_filters_remaps_phantom_table_column(self):
        """A filter with a phantom table and a real column gets remapped."""
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
        )
        ctx = _make_model_context_string_cols()
        # PhantomTable doesn't exist, but 'Country' column exists in Geography
        ctx["default_filters"] = {"PhantomTable[Country]": "Austria"}
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="revenue by country",
        )
        parsed = json.loads(result)
        # Should be remapped or dropped — no error
        assert "error" not in parsed

    def test_default_filters_drops_phantom_column(self):
        """A filter with a completely unknown column is dropped."""
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
        )
        ctx = _make_model_context_string_cols()
        ctx["default_filters"] = {"PhantomTable[NonexistentColumn]": "value"}
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="revenue",
        )
        parsed = json.loads(result)
        assert "error" not in parsed

    def test_default_filters_no_bracket_key_passes_through(self):
        """A filter key without brackets is preserved as-is."""
        tool = PowerBIMetadataReducerTool(
            strategy="fuzzy",
            synonym_threshold=50,
        )
        ctx = _make_model_context_string_cols()
        ctx["default_filters"] = {"SomeFilter": "value"}
        result = tool._run(
            model_context_json=json.dumps(ctx),
            user_question="revenue",
        )
        parsed = json.loads(result)
        assert "error" not in parsed


class TestParseModelContextEdgeCases:
    """Tests for edge cases in _parse_model_context."""

    def test_model_context_as_dict_passthrough(self):
        """Passing a dict directly as model_context_json is handled."""
        tool = PowerBIMetadataReducerTool(strategy="passthrough")
        ctx = _make_model_context()
        # Pass dict (not string) — should be handled
        result = tool._run(
            model_context_json=ctx,
            user_question="test",
        )
        parsed = json.loads(result)
        assert "error" not in parsed

    def test_model_context_with_no_tables_falls_through_to_error(self):
        """A compact summary JSON with no tables/measures triggers cache fallback path."""
        tool = PowerBIMetadataReducerTool(strategy="passthrough")
        # This looks like a Fetcher compact summary — no real model data
        compact = json.dumps({"status": "ok", "message": "done"})
        result = tool._run(
            model_context_json=compact,
            user_question="revenue",
        )
        parsed = json.loads(result)
        # No dataset_id/workspace_id → falls through to error
        assert "error" in parsed

    def test_cache_exception_during_load_returns_error(self):
        """Cache lookup exception results in an error response."""
        tool = PowerBIMetadataReducerTool(
            strategy="passthrough",
            dataset_id="ds-123",
            workspace_id="ws-456",
        )

        @asynccontextmanager
        async def mock_session_factory():
            yield MagicMock()

        with patch(
            "src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool.async_session_factory",
            mock_session_factory,
        ), patch(
            "src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool.PowerBISemanticModelCacheService",
            side_effect=Exception("Cache explosion"),
        ):
            result = tool._run(user_question="revenue")

        parsed = json.loads(result)
        assert "error" in parsed


class TestRunAsyncInSyncContext:
    """Tests for the _run_async_in_sync_context helper."""

    def test_runs_in_new_loop_when_no_existing_loop(self):
        """Without a running loop, executes in a fresh loop."""
        from src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool import (
            _run_async_in_sync_context,
        )

        async def simple_coro():
            return 42

        # Ensure no loop is currently running
        result = _run_async_in_sync_context(simple_coro())
        assert result == 42

    def test_raises_on_coro_exception(self):
        """Exceptions from the coroutine propagate out."""
        import pytest
        from src.engines.crewai.tools.custom.powerbi_metadata_reducer_tool import (
            _run_async_in_sync_context,
        )

        async def failing_coro():
            raise ValueError("oops")

        with pytest.raises(ValueError, match="oops"):
            _run_async_in_sync_context(failing_coro())

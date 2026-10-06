"""Unit tests for A/B domain_context test harness metrics logic."""

import json
import sys
from pathlib import Path

import pytest

# Add scripts to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "scripts"))


class TestExtractMetrics:
    """Test metrics extraction from UCMV output."""

    def test_extract_metrics_from_valid_output(self):
        """Test extracting metrics from a valid UCMV output."""
        # Import here to avoid import errors in other tests
        from ab_domain_context import extract_metrics

        output = {
            "stats": {
                "total_measures": 10,
                "translated_count": 8,
                "untranslatable_count": 2,
                "dax_class_breakdown": {"base": 5, "dax": 3, "switch": 2},
            },
            "specs": {
                "fact_sales": {"measures": [], "joins": []},
                "fact_costs": {"measures": [], "joins": []},
            },
        }

        metrics = extract_metrics(output)

        assert metrics["total_measures"] == 10
        assert metrics["translated_count"] == 8
        assert metrics["untranslatable_count"] == 2
        assert metrics["views_count"] == 2
        assert metrics["dax_class_breakdown"]["base"] == 5

    def test_extract_metrics_empty_output(self):
        """Test extracting metrics from empty output."""
        from ab_domain_context import extract_metrics

        output = {"stats": {}, "specs": {}}

        metrics = extract_metrics(output)

        assert metrics["total_measures"] == 0
        assert metrics["translated_count"] == 0
        assert metrics["untranslatable_count"] == 0
        assert metrics["views_count"] == 0

    def test_extract_metrics_with_error(self):
        """Test extracting metrics when output contains error."""
        from ab_domain_context import extract_metrics

        output = {"error": "Tool execution failed"}

        metrics = extract_metrics(output)

        assert metrics["error"] == "Tool execution failed"
        assert metrics["total_measures"] == 0

    def test_extract_metrics_missing_stats(self):
        """Test extracting metrics when stats key is missing."""
        from ab_domain_context import extract_metrics

        output = {"specs": {"fact_sales": {}}}

        metrics = extract_metrics(output)

        # Should degrade gracefully
        assert metrics["total_measures"] == 0
        assert metrics["error"] is None


class TestComputeDiffMetrics:
    """Test diff metrics computation."""

    def test_compute_diff_newly_translated(self):
        """Test computing diff when domain_context enables new translations."""
        from ab_domain_context import compute_diff_metrics

        output_a = {
            "specs": {
                "fact_sales": {
                    "measures": [
                        {
                            "name": "total_sales",
                            "sql_expr": "",
                            "confidence": 0,
                        },
                        {
                            "name": "avg_price",
                            "sql_expr": "SELECT AVG(price) FROM sales",
                            "confidence": 0.8,
                        },
                    ]
                }
            }
        }

        output_b = {
            "specs": {
                "fact_sales": {
                    "measures": [
                        {
                            "name": "total_sales",
                            "sql_expr": "SELECT SUM(amount) FROM sales",
                            "confidence": 0.9,
                        },
                        {
                            "name": "avg_price",
                            "sql_expr": "SELECT AVG(price) FROM sales",
                            "confidence": 0.8,
                        },
                    ]
                }
            }
        }

        diff = compute_diff_metrics(output_a, output_b)

        assert diff["newly_translated"] == 1
        assert diff["expressions_changed"] == 1
        assert diff["measures_improved"] == 1
        assert "total_sales" in str(diff["details"])

    def test_compute_diff_confidence_improved(self):
        """Test computing diff when confidence improves with SQL change."""
        from ab_domain_context import compute_diff_metrics

        output_a = {
            "specs": {
                "fact_sales": {
                    "measures": [
                        {
                            "name": "metric1",
                            "sql_expr": "SELECT X",
                            "confidence": 0.5,
                        }
                    ]
                }
            }
        }

        output_b = {
            "specs": {
                "fact_sales": {
                    "measures": [
                        {
                            "name": "metric1",
                            "sql_expr": "SELECT Y",  # SQL changed
                            "confidence": 0.9,
                        }
                    ]
                }
            }
        }

        diff = compute_diff_metrics(output_a, output_b)

        # measures_improved is tracked only when SQL changes too
        assert diff["expressions_changed"] == 1
        assert diff["measures_improved"] == 1
        assert "confidence 0.5 → 0.9" in str(diff["details"])

    def test_compute_diff_no_change(self):
        """Test computing diff when nothing changes."""
        from ab_domain_context import compute_diff_metrics

        output_a = {
            "specs": {
                "fact_sales": {
                    "measures": [
                        {
                            "name": "metric1",
                            "sql_expr": "SELECT X",
                            "confidence": 0.8,
                        }
                    ]
                }
            }
        }

        output_b = {
            "specs": {
                "fact_sales": {
                    "measures": [
                        {
                            "name": "metric1",
                            "sql_expr": "SELECT X",
                            "confidence": 0.8,
                        }
                    ]
                }
            }
        }

        diff = compute_diff_metrics(output_a, output_b)

        assert diff["newly_translated"] == 0
        assert diff["expressions_changed"] == 0
        assert diff["measures_improved"] == 0

    def test_compute_diff_multiple_measures(self):
        """Test computing diff with multiple measures."""
        from ab_domain_context import compute_diff_metrics

        output_a = {
            "specs": {
                "fact_sales": {
                    "measures": [
                        {"name": "m1", "sql_expr": "", "confidence": 0},
                        {"name": "m2", "sql_expr": "", "confidence": 0},
                        {"name": "m3", "sql_expr": "SELECT X", "confidence": 0.5},
                    ]
                },
                "fact_costs": {
                    "measures": [
                        {"name": "m4", "sql_expr": "", "confidence": 0},
                    ]
                },
            }
        }

        output_b = {
            "specs": {
                "fact_sales": {
                    "measures": [
                        {"name": "m1", "sql_expr": "SELECT A", "confidence": 0.9},
                        {"name": "m2", "sql_expr": "", "confidence": 0},
                        {"name": "m3", "sql_expr": "SELECT X", "confidence": 0.95},
                    ]
                },
                "fact_costs": {
                    "measures": [
                        {"name": "m4", "sql_expr": "SELECT B", "confidence": 0.8},
                    ]
                },
            }
        }

        diff = compute_diff_metrics(output_a, output_b)

        # m1: newly translated + confidence improved
        # m3: confidence improved (same sql)
        # m4: newly translated
        assert diff["newly_translated"] == 2
        assert diff["expressions_changed"] == 2
        assert diff["measures_improved"] >= 2

    def test_compute_diff_with_empty_specs(self):
        """Test computing diff with empty specs."""
        from ab_domain_context import compute_diff_metrics

        output_a = {"specs": {}}
        output_b = {"specs": {}}

        diff = compute_diff_metrics(output_a, output_b)

        assert diff["newly_translated"] == 0
        assert diff["expressions_changed"] == 0


class TestFormatComparisonTable:
    """Test comparison table formatting."""

    def test_format_comparison_table_basic(self):
        """Test formatting a basic comparison table."""
        from ab_domain_context import format_comparison_table

        metrics_a = {
            "total_measures": 10,
            "translated_count": 6,
            "untranslatable_count": 4,
            "views_count": 2,
            "dax_class_breakdown": {"base": 4, "dax": 2},
            "error": None,
        }

        metrics_b = {
            "total_measures": 10,
            "translated_count": 8,
            "untranslatable_count": 2,
            "views_count": 2,
            "dax_class_breakdown": {"base": 4, "dax": 4},
            "error": None,
        }

        diff = {
            "newly_translated": 2,
            "expressions_changed": 4,
            "measures_improved": 2,
            "details": ["  ✓ measure1: newly translated with domain_context"],
        }

        table = format_comparison_table(metrics_a, metrics_b, diff)

        # Check for key elements
        assert "Total Measures" in table
        assert "Translated Measures" in table
        assert "60.0%" in table  # 6/10
        assert "80.0%" in table  # 8/10
        assert "VERDICT" in table
        assert "newly translated" in table

    def test_format_comparison_table_error_run_a(self):
        """Test formatting when Run A has error."""
        from ab_domain_context import format_comparison_table

        metrics_a = {
            "total_measures": 0,
            "translated_count": 0,
            "untranslatable_count": 0,
            "views_count": 0,
            "dax_class_breakdown": {},
            "error": "No Databricks credentials",
        }

        metrics_b = {
            "total_measures": 10,
            "translated_count": 8,
            "untranslatable_count": 2,
            "views_count": 2,
            "dax_class_breakdown": {},
            "error": None,
        }

        diff = {}

        table = format_comparison_table(metrics_a, metrics_b, diff)

        assert "FAILED" in table
        assert "No Databricks credentials" in table

    def test_format_comparison_table_no_improvement(self):
        """Test formatting when there's no improvement."""
        from ab_domain_context import format_comparison_table

        metrics_a = {
            "total_measures": 10,
            "translated_count": 8,
            "untranslatable_count": 2,
            "views_count": 2,
            "dax_class_breakdown": {},
            "error": None,
        }

        metrics_b = {
            "total_measures": 10,
            "translated_count": 8,
            "untranslatable_count": 2,
            "views_count": 2,
            "dax_class_breakdown": {},
            "error": None,
        }

        diff = {
            "newly_translated": 0,
            "expressions_changed": 0,
            "measures_improved": 0,
            "details": [],
        }

        table = format_comparison_table(metrics_a, metrics_b, diff)

        assert "No measurable improvement" in table


class TestLoadFunctions:
    """Test file loading functions."""

    def test_load_file_success(self, tmp_path):
        """Test loading a file successfully."""
        from ab_domain_context import load_file

        test_file = tmp_path / "test.txt"
        test_file.write_text("Hello, World!")

        content = load_file(test_file)

        assert content == "Hello, World!"

    def test_load_file_not_found(self):
        """Test loading a non-existent file."""
        from ab_domain_context import load_file

        with pytest.raises(FileNotFoundError):
            load_file("/nonexistent/path/file.txt")

    def test_load_json_file_success(self, tmp_path):
        """Test loading a JSON file successfully."""
        from ab_domain_context import load_json_file

        test_file = tmp_path / "test.json"
        test_file.write_text(json.dumps({"key": "value"}))

        data = load_json_file(test_file)

        assert data == {"key": "value"}

    def test_load_json_file_invalid(self, tmp_path):
        """Test loading invalid JSON."""
        from ab_domain_context import load_json_file

        test_file = tmp_path / "test.json"
        test_file.write_text("not valid json{")

        with pytest.raises(ValueError):
            load_json_file(test_file)

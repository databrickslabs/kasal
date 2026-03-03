"""
Unit tests for llm_handlers __init__.py module.

Verifies that all expected symbols are exported correctly.
Uses AST parsing to avoid triggering the heavy import chain (litellm/openai).
"""

import ast
import pathlib
import pytest


_INIT_PATH = (
    pathlib.Path(__file__).resolve().parents[4]
    / "src"
    / "core"
    / "llm_handlers"
    / "__init__.py"
)


@pytest.fixture(scope="module")
def init_source():
    """Parse the __init__.py AST without importing it."""
    return _INIT_PATH.read_text()


@pytest.fixture(scope="module")
def init_tree(init_source):
    return ast.parse(init_source)


def _get_all_list(tree):
    """Extract the __all__ list from the AST."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__all__":
                    if isinstance(node.value, ast.List):
                        return [elt.value for elt in node.value.elts if isinstance(elt, ast.Constant)]
    return []


def _get_imported_names(tree):
    """Extract all imported names from the AST."""
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                names.add(alias.asname if alias.asname else alias.name)
    return names


class TestLLMHandlersExports:
    """Test that llm_handlers __init__.py exports the expected symbols."""

    def test_exports_databricks_gpt_oss_handler(self, init_tree):
        names = _get_imported_names(init_tree)
        assert "DatabricksGPTOSSHandler" in names

    def test_exports_databricks_retry_llm(self, init_tree):
        names = _get_imported_names(init_tree)
        assert "DatabricksRetryLLM" in names

    def test_exports_databricks_codex_completion(self, init_tree):
        names = _get_imported_names(init_tree)
        assert "DatabricksCodexCompletion" in names

    def test_all_list_contains_expected_names(self, init_tree):
        all_list = _get_all_list(init_tree)
        assert "DatabricksGPTOSSHandler" in all_list
        assert "DatabricksRetryLLM" in all_list
        assert "DatabricksCodexCompletion" in all_list

    def test_all_list_length(self, init_tree):
        all_list = _get_all_list(init_tree)
        assert len(all_list) == 3

    def test_docstring_present(self, init_source):
        assert "LLM Handlers" in init_source

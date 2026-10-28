"""The judge registry's MLflow search filter keeps catalog/schema inside quotes.

Apps security scan (2026-10-28): ``_search_filter`` interpolated the catalog
and schema split off a registry prompt name into ``catalog = '…'`` with no
check, so a quote in either rewrote the filter.
"""

from unittest.mock import MagicMock

import pytest

from src.services.prompt_optimization.judge_registry import (
    PROMPT_PREFIX,
    JudgeRegistry,
)


def _registry(uc_schema):
    return JudgeRegistry("databricks-uc", uc_schema=uc_schema, client=MagicMock())


def test_a_quote_in_the_schema_is_refused():
    with pytest.raises(ValueError, match="Invalid Unity Catalog schema"):
        _registry("main.kasal' OR '1'='1")._search_filter()


def test_plain_names_build_the_filter():
    assert (
        _registry("main.kasal-dev")._search_filter()
        == "catalog = 'main' AND schema = 'kasal-dev'"
    )


def test_a_local_registry_filters_by_the_constant_prefix():
    assert _registry(None)._search_filter() == f"name LIKE '{PROMPT_PREFIX}%'"

"""
Unit tests for DaxRagRetriever.

Tests retrieval and storage of Q→DAX few-shot examples via mocked
Databricks Vector Search HTTP calls.
"""
import hashlib
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.services.dax_rag_retriever import DaxRagRetriever


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def retriever():
    return DaxRagRetriever()


@pytest.fixture
def valid_config():
    return {
        "dax_rag_enabled": True,
        "llm_workspace_url": "https://example.databricks.com",
        "llm_token": "secret-token",
        "dax_rag_index_name": "cat.schema.index",
        "dax_rag_endpoint_name": "my-endpoint",
    }


def _vs_response(pairs):
    """Build a fake Vector Search JSON response for the given (question, dax, score) triples."""
    rows = [[q, d, s] for q, d, s in pairs]
    return {
        "result": {
            "columns": ["question", "dax"],
            "data_array": rows,
        }
    }


def _mock_httpx_client(json_response=None, status_code=200, raise_exc=None):
    """Return (context-manager mock, inner-client mock) for httpx.AsyncClient."""
    mock_response = MagicMock()
    mock_response.json.return_value = json_response or {}
    if raise_exc:
        mock_response.raise_for_status.side_effect = raise_exc
    else:
        mock_response.raise_for_status.return_value = None

    mock_inner = AsyncMock()
    mock_inner.post.return_value = mock_response

    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_inner)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    return mock_ctx, mock_inner


# ---------------------------------------------------------------------------
# retrieve — disabled / missing config
# ---------------------------------------------------------------------------

class TestRetrieveDisabled:
    @pytest.mark.asyncio
    async def test_returns_empty_when_disabled(self, retriever):
        config = {"dax_rag_enabled": False}
        result = await retriever.retrieve("any question", config)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_flag_absent(self, retriever):
        config = {}
        result = await retriever.retrieve("any question", config)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_workspace_url_missing(self, retriever):
        config = {
            "dax_rag_enabled": True,
            "llm_token": "tok",
            "dax_rag_index_name": "idx",
            "dax_rag_endpoint_name": "ep",
            # no llm_workspace_url
        }
        result = await retriever.retrieve("question", config)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_token_missing(self, retriever, valid_config):
        cfg = {**valid_config, "llm_token": ""}
        result = await retriever.retrieve("question", cfg)
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_index_name_missing(self, retriever, valid_config):
        cfg = {**valid_config, "dax_rag_index_name": ""}
        result = await retriever.retrieve("question", cfg)
        assert result == []


# ---------------------------------------------------------------------------
# retrieve — HTTP success paths
# ---------------------------------------------------------------------------

class TestRetrieveSuccess:
    @pytest.mark.asyncio
    async def test_returns_examples_above_threshold(self, retriever, valid_config):
        pairs = [
            ("q1", "EVALUATE 'Table'", 0.95),
            ("q2", "CALCULATE(SUM([Sales]))", 0.80),
        ]
        mock_ctx, _ = _mock_httpx_client(_vs_response(pairs))

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            result = await retriever.retrieve("q1", valid_config, n=3, threshold=0.75)

        assert len(result) == 2
        assert result[0]["question"] == "q1"
        assert result[0]["dax"] == "EVALUATE 'Table'"
        assert result[0]["score"] == 0.95

    @pytest.mark.asyncio
    async def test_filters_out_results_below_threshold(self, retriever, valid_config):
        pairs = [
            ("good", "EVALUATE 'A'", 0.90),
            ("bad", "EVALUATE 'B'", 0.50),  # below default 0.75
        ]
        mock_ctx, _ = _mock_httpx_client(_vs_response(pairs))

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            result = await retriever.retrieve("good", valid_config, n=3, threshold=0.75)

        assert len(result) == 1
        assert result[0]["question"] == "good"

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_rows_match_threshold(self, retriever, valid_config):
        pairs = [("q", "d", 0.30)]
        mock_ctx, _ = _mock_httpx_client(_vs_response(pairs))

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            result = await retriever.retrieve("q", valid_config, threshold=0.75)

        assert result == []

    @pytest.mark.asyncio
    async def test_score_is_rounded_to_4_dp(self, retriever, valid_config):
        pairs = [("q", "d", 0.912345)]
        mock_ctx, _ = _mock_httpx_client(_vs_response(pairs))

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            result = await retriever.retrieve("q", valid_config, threshold=0.75)

        assert result[0]["score"] == round(0.912345, 4)

    @pytest.mark.asyncio
    async def test_url_is_built_correctly(self, retriever, valid_config):
        mock_ctx, mock_inner = _mock_httpx_client(
            _vs_response([("q", "d", 0.9)])
        )

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            await retriever.retrieve("q", valid_config)

        url_called = mock_inner.post.call_args[0][0]
        assert "cat.schema.index" in url_called
        assert "vector-search/indexes" in url_called

    @pytest.mark.asyncio
    async def test_authorization_header_sent(self, retriever, valid_config):
        mock_ctx, mock_inner = _mock_httpx_client(
            _vs_response([("q", "d", 0.9)])
        )

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            await retriever.retrieve("q", valid_config)

        headers = mock_inner.post.call_args.kwargs.get("headers", {})
        assert headers.get("Authorization") == "Bearer secret-token"


# ---------------------------------------------------------------------------
# retrieve — failure / parse error paths (fail-open)
# ---------------------------------------------------------------------------

class TestRetrieveFailOpen:
    @pytest.mark.asyncio
    async def test_returns_empty_on_http_error(self, retriever, valid_config):
        import httpx
        mock_ctx, mock_inner = _mock_httpx_client()
        mock_inner.post.side_effect = httpx.HTTPStatusError(
            "500 Server Error", request=MagicMock(), response=MagicMock()
        )

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            result = await retriever.retrieve("q", valid_config)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_network_error(self, retriever, valid_config):
        import httpx
        mock_ctx, mock_inner = _mock_httpx_client()
        mock_inner.post.side_effect = httpx.ConnectError("refused")

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            result = await retriever.retrieve("q", valid_config)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_missing_columns_in_response(self, retriever, valid_config):
        bad_response = {"result": {"columns": ["only_one"], "data_array": [["val", 0.9]]}}
        mock_ctx, _ = _mock_httpx_client(bad_response)

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            result = await retriever.retrieve("q", valid_config)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_empty_data_array(self, retriever, valid_config):
        response = {"result": {"columns": ["question", "dax"], "data_array": []}}
        mock_ctx, _ = _mock_httpx_client(response)

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            result = await retriever.retrieve("q", valid_config)

        assert result == []

    @pytest.mark.asyncio
    async def test_skips_rows_shorter_than_two_columns(self, retriever, valid_config):
        response = {"result": {"columns": ["question", "dax"], "data_array": [["only_one"]]}}
        mock_ctx, _ = _mock_httpx_client(response)

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            result = await retriever.retrieve("q", valid_config)

        assert result == []


# ---------------------------------------------------------------------------
# store — disabled / missing config
# ---------------------------------------------------------------------------

class TestStoreDisabled:
    @pytest.mark.asyncio
    async def test_no_op_when_disabled(self, retriever):
        config = {"dax_rag_enabled": False}
        # Should not raise and should not call any HTTP
        with patch("src.services.dax_rag_retriever.httpx.AsyncClient") as mock_cls:
            await retriever.store("question", "DAX", config)
            mock_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_op_when_missing_config_keys(self, retriever):
        config = {"dax_rag_enabled": True}  # missing workspace_url etc.
        with patch("src.services.dax_rag_retriever.httpx.AsyncClient") as mock_cls:
            await retriever.store("question", "DAX", config)
            mock_cls.assert_not_called()


# ---------------------------------------------------------------------------
# store — success & deterministic ID
# ---------------------------------------------------------------------------

class TestStoreSuccess:
    @pytest.mark.asyncio
    async def test_posts_to_upsert_url(self, retriever, valid_config):
        mock_ctx, mock_inner = _mock_httpx_client({})

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            await retriever.store("question", "EVALUATE 'T'", valid_config)

        url_called = mock_inner.post.call_args[0][0]
        assert "upsert" in url_called
        assert "cat.schema.index" in url_called

    @pytest.mark.asyncio
    async def test_payload_contains_question_and_dax(self, retriever, valid_config):
        mock_ctx, mock_inner = _mock_httpx_client({})

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            await retriever.store("my question", "MY DAX", valid_config)

        body = mock_inner.post.call_args.kwargs.get("json", {})
        records = json.loads(body["inputs_json"])
        assert records[0]["question"] == "my question"
        assert records[0]["dax"] == "MY DAX"

    @pytest.mark.asyncio
    async def test_record_id_is_deterministic(self, retriever, valid_config):
        question = "what are total sales?"
        dataset_id = "ds-1"
        expected_id = hashlib.sha256(
            f"{dataset_id}:{question}".encode()
        ).hexdigest()[:32]

        mock_ctx, mock_inner = _mock_httpx_client({})

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            await retriever.store(question, "DAX", valid_config, dataset_id=dataset_id)

        body = mock_inner.post.call_args.kwargs.get("json", {})
        records = json.loads(body["inputs_json"])
        assert records[0]["id"] == expected_id

    @pytest.mark.asyncio
    async def test_store_fails_silently_on_http_error(self, retriever, valid_config):
        import httpx
        mock_ctx, mock_inner = _mock_httpx_client()
        mock_inner.post.side_effect = httpx.ConnectError("refused")

        # Should not raise
        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            await retriever.store("q", "d", valid_config)

    @pytest.mark.asyncio
    async def test_store_fails_silently_on_raise_for_status(self, retriever, valid_config):
        import httpx
        mock_ctx, _ = _mock_httpx_client(
            raise_exc=httpx.HTTPStatusError("400", request=MagicMock(), response=MagicMock())
        )

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            # Should not raise
            await retriever.store("q", "d", valid_config)

    @pytest.mark.asyncio
    async def test_dataset_id_included_in_record(self, retriever, valid_config):
        mock_ctx, mock_inner = _mock_httpx_client({})

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            await retriever.store("q", "d", valid_config, dataset_id="my-ds")

        body = mock_inner.post.call_args.kwargs.get("json", {})
        records = json.loads(body["inputs_json"])
        assert records[0]["dataset_id"] == "my-ds"

    @pytest.mark.asyncio
    async def test_dataset_id_defaults_to_empty_string(self, retriever, valid_config):
        mock_ctx, mock_inner = _mock_httpx_client({})

        with patch("src.services.dax_rag_retriever.httpx.AsyncClient", return_value=mock_ctx):
            await retriever.store("q", "d", valid_config)  # no dataset_id

        body = mock_inner.post.call_args.kwargs.get("json", {})
        records = json.loads(body["inputs_json"])
        assert records[0]["dataset_id"] == ""

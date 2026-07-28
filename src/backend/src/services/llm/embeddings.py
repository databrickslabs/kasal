"""Embeddings — a separate protocol from chat completions, and separate here too.

These calls do not go through the engine's LLM at all: they are direct HTTP POSTs
to an embeddings endpoint (Databricks serving / Ollama / Google / OpenAI), with
their own auth resolution, batching and circuit breaker. They shared a module
with chat-completion configuration only because both lived on ``LLMManager``,
which made llm_manager a third longer for readers who never touch embeddings.

Five knowledge/RAG services use them, all through ``LLMManager.get_embedding`` /
``get_embeddings`` — those remain the public entry points and delegate here.
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional

from src.core.logger import LoggerManager
from src.schemas.model_provider import ModelProvider
from src.services.settings.api_keys import ApiKeysService
from src.utils.databricks_url_utils import DatabricksURLUtils

logger = logging.getLogger(__name__)
embedding_logger = LoggerManager.get_instance().documentation_embedding

# Circuit breaker: after this many consecutive failures for a provider, fail fast
# instead of hammering a broken endpoint; reset after the cooldown.
_embedding_failures: Dict[str, Dict[str, float]] = {}
_EMBEDDING_FAILURE_THRESHOLD = 3
_CIRCUIT_RESET_SECONDS = 300


def _get_group_id_from_context(required: bool = True) -> Optional[str]:
    """Group id for multi-tenant isolation (see LLMManager._get_group_id_from_context)."""
    from src.services.llm.manager import LLMManager

    return LLMManager._get_group_id_from_context(required=required)


async def get_embeddings(
    texts: List[str],
    model: str = "databricks-gte-large-en",
    embedder_config: Optional[Dict[str, Any]] = None,
    batch_size: Optional[int] = None,
) -> List[Optional[List[float]]]:
    """
    Get embedding vectors for many texts efficiently.

    Resolves Databricks auth ONCE and sends texts in batched requests, instead
    of one auth lookup + one HTTP round-trip per text. Returns a list aligned
    with ``texts`` (None for any text that failed). For non-Databricks
    providers, falls back to sequential ``get_embedding`` calls.
    """
    if not texts:
        return []

    provider = "databricks"
    embedding_model = model
    if embedder_config:
        provider = embedder_config.get("provider", "databricks")
        embedding_model = embedder_config.get("config", {}).get("model", model)

    # Only the Databricks serving endpoint supports the batched payload here;
    # other providers fall back to the existing per-text path.
    if not (provider == "databricks" or "databricks" in embedding_model):
        return [
            await get_embedding(t, model=model, embedder_config=embedder_config)
            for t in texts
        ]

    if batch_size is None:
        batch_size = int(os.getenv("EMBEDDING_BATCH_SIZE", "32"))

    try:
        from src.utils.databricks_auth import get_auth_context
        from src.utils.user_context import UserContext

        # Resolve auth ONCE for the whole file (this is the per-chunk cost we
        # are eliminating — each get_auth_context() opens a DB session).
        user_token = UserContext.get_user_token()
        emb_group_id = _get_group_id_from_context(required=False)
        auth = await get_auth_context(user_token=user_token, group_id=emb_group_id)
        if not auth:
            embedding_logger.warning(
                "No Databricks auth available for batch embeddings"
            )
            return [None] * len(texts)

        if auth.auth_method in ("OBO", "OAuth"):
            request_headers = auth.get_headers().copy()
            request_headers.setdefault("Content-Type", "application/json")
        else:
            request_headers = {
                "Authorization": f"Bearer {auth.token}",
                "Content-Type": "application/json",
            }

        # AI Gateway on  -> /ai-gateway/mlflow/v1/embeddings (model in body)
        # AI Gateway off -> /serving-endpoints/<model>/invocations (model in path)
        endpoint_url, body_model = DatabricksURLUtils.construct_embeddings_url(
            auth.workspace_url, embedding_model
        )

        import aiohttp

        timeout = aiohttp.ClientTimeout(
            total=float(os.getenv("EMBEDDING_HTTP_TIMEOUT_SECONDS", "60"))
        )
        results: List[Optional[List[float]]] = []
        from src.utils.aiohttp_session import shared_client_session

        async with shared_client_session() as session:
            for start in range(0, len(texts), batch_size):
                batch = texts[start : start + batch_size]
                payload = {"input": batch}
                if body_model:
                    payload["model"] = body_model
                try:
                    async with session.post(
                        endpoint_url,
                        headers=request_headers,
                        json=payload,
                        timeout=timeout,
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            data = result.get("data", [])
                            try:
                                data = sorted(data, key=lambda d: d.get("index", 0))
                            except Exception:
                                pass
                            if len(data) == len(batch):
                                results.extend([d.get("embedding", d) for d in data])
                            else:
                                embedding_logger.warning(
                                    f"Batch embedding size mismatch: got {len(data)} for {len(batch)}"
                                )
                                results.extend(
                                    [
                                        (
                                            data[i].get("embedding")
                                            if i < len(data)
                                            else None
                                        )
                                        for i in range(len(batch))
                                    ]
                                )
                        else:
                            error_text = await response.text()
                            embedding_logger.error(
                                f"Batch embedding API error {response.status}: {error_text}"
                            )
                            results.extend([None] * len(batch))
                except Exception as batch_err:
                    embedding_logger.error(
                        f"Batch embedding request failed: {batch_err}"
                    )
                    results.extend([None] * len(batch))
        return results

    except Exception as e:
        embedding_logger.error(f"Error in batch embeddings: {e}")
        return [None] * len(texts)


async def get_embedding(
    text: str,
    model: str = "databricks-gte-large-en",
    embedder_config: Optional[Dict[str, Any]] = None,
) -> Optional[List[float]]:
    """
    Get an embedding vector for the given text using configurable embedder.

    Args:
        text: The text to create an embedding for
        model: The embedding model to use (can be overridden by embedder_config)
        embedder_config: Optional embedder configuration with provider and model settings

    Returns:
        List[float]: The embedding vector or None if creation fails
    """
    provider = "databricks"  # Default provider
    try:
        # Determine provider and model from embedder_config or defaults
        if embedder_config:
            provider = embedder_config.get("provider", "databricks")
            config = embedder_config.get("config", {})
            embedding_model = config.get("model", model)
        else:
            provider = "databricks"
            embedding_model = model

        # Check circuit breaker for this provider
        current_time = time.time()
        if provider in _embedding_failures:
            failure_info = _embedding_failures[provider]
            failure_count = failure_info.get("count", 0)
            last_failure_time = failure_info.get("last_failure", 0)

            # If circuit is open, check if it should be reset
            if failure_count >= _EMBEDDING_FAILURE_THRESHOLD:
                if current_time - last_failure_time < _CIRCUIT_RESET_SECONDS:
                    embedding_logger.warning(
                        f"Circuit breaker OPEN for {provider} embeddings. Failing fast."
                    )
                    return None
                else:
                    # Reset circuit after timeout
                    embedding_logger.info(
                        f"Resetting circuit breaker for {provider} embeddings"
                    )
                    _embedding_failures[provider] = {"count": 0, "last_failure": 0}

        embedding_logger.info(
            f"Creating embedding using provider: {provider}, model: {embedding_model}"
        )

        # Handle different embedding providers
        if provider == "databricks" or "databricks" in embedding_model:
            # Use unified Databricks authentication for embeddings
            try:
                from src.utils.databricks_auth import get_auth_context
                from src.utils.user_context import UserContext

                # Get user token from context for OBO authentication
                user_token = UserContext.get_user_token()

                # Use unified authentication (OBO → OAuth → PAT)
                embedding_logger.info(
                    "Attempting unified Databricks authentication for embeddings"
                )
                emb_group_id = _get_group_id_from_context(required=False)
                auth = await get_auth_context(
                    user_token=user_token, group_id=emb_group_id
                )
                if auth:
                    embedding_logger.info(
                        f"Using Databricks {auth.auth_method} authentication for embeddings"
                    )
                    # For OAuth/OBO, use headers approach
                    if auth.auth_method in ["OBO", "OAuth"]:
                        headers = auth.get_headers()
                        api_key = None
                    else:
                        # For PAT, use API key approach
                        api_key = auth.token
                        headers = None
                    api_base = DatabricksURLUtils.construct_llm_base_url(
                        auth.workspace_url
                    )
                else:
                    embedding_logger.warning(
                        "No Databricks authentication available for embeddings"
                    )
                    return None

            except ImportError:
                # SECURITY: databricks_auth module is required - no fallback allowed
                embedding_logger.error(
                    "Unified Databricks auth module not available for embeddings"
                )
                raise ImportError(
                    "databricks_auth module is required for Databricks authentication"
                )

            # Check if we have either OAuth headers or API key + base URL
            if not ((headers and api_base) or (api_key and api_base)):
                logger.warning(
                    f"Missing Databricks credentials - OAuth headers: {bool(headers)}, API key: {bool(api_key)}, API base: {bool(api_base)}"
                )
                return None

            # Ensure model has databricks prefix
            if not embedding_model.startswith("databricks/"):
                embedding_model = f"databricks/{embedding_model}"

            # Use direct HTTP request to avoid config file issues
            import aiohttp

            try:
                # Construct the direct API endpoint using centralized utility.
                # AI Gateway on  -> /ai-gateway/mlflow/v1/embeddings (model in body)
                # AI Gateway off -> /serving-endpoints/<model>/invocations (model in path)
                workspace_url = DatabricksURLUtils.extract_workspace_from_endpoint(
                    api_base
                )
                endpoint_url, body_model = DatabricksURLUtils.construct_embeddings_url(
                    workspace_url, embedding_model
                )

                # Use OAuth headers if available, otherwise fall back to API key
                if headers:
                    request_headers = headers.copy()
                    if "Content-Type" not in request_headers:
                        request_headers["Content-Type"] = "application/json"
                else:
                    request_headers = {
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    }

                payload = {"input": [text] if isinstance(text, str) else text}
                if body_model:
                    payload["model"] = body_model

                timeout = aiohttp.ClientTimeout(
                    total=float(os.getenv("EMBEDDING_HTTP_TIMEOUT_SECONDS", "30"))
                )
                from src.utils.aiohttp_session import shared_client_session

                async with shared_client_session() as session:
                    async with session.post(
                        endpoint_url,
                        headers=request_headers,
                        json=payload,
                        timeout=timeout,
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            # Databricks embedding API returns embeddings in 'data' field
                            if "data" in result and len(result["data"]) > 0:
                                embedding = result["data"][0].get(
                                    "embedding", result["data"][0]
                                )
                                embedding_logger.info(
                                    f"Successfully created embedding with {len(embedding)} dimensions using direct Databricks API"
                                )
                                return embedding
                            else:
                                embedding_logger.warning(
                                    "No embedding data found in Databricks response"
                                )
                                return None
                        elif response.status == 401:
                            # Token expired, try to refresh and retry once
                            embedding_logger.warning(
                                "Received 401 error, attempting to refresh token and retry"
                            )
                            try:
                                # Re-resolve auth to pick up a refreshed token.
                                #
                                # This called an undefined `get_databricks_auth_headers()`
                                # — never imported here or in llm_manager, where this code
                                # used to live — so the retry raised NameError, was
                                # swallowed by the except below, and every 401 simply
                                # returned None. The refresh has never actually run.
                                # get_auth_context is the same resolver used above.
                                refreshed = await get_auth_context(
                                    user_token=UserContext.get_user_token(),
                                    group_id=_get_group_id_from_context(required=False),
                                )
                                headers_result = (
                                    refreshed.get_headers() if refreshed else None
                                )
                                error = (
                                    None
                                    if refreshed
                                    else "no Databricks auth available"
                                )
                                if headers_result and not error:
                                    # Update request headers with refreshed token
                                    if headers_result:
                                        request_headers = headers_result.copy()
                                        if "Content-Type" not in request_headers:
                                            request_headers["Content-Type"] = (
                                                "application/json"
                                            )

                                    # Retry the request with new token
                                    async with session.post(
                                        endpoint_url,
                                        headers=request_headers,
                                        json=payload,
                                        timeout=timeout,
                                    ) as retry_response:
                                        if retry_response.status == 200:
                                            result = await retry_response.json()
                                            if (
                                                "data" in result
                                                and len(result["data"]) > 0
                                            ):
                                                embedding = result["data"][0].get(
                                                    "embedding", result["data"][0]
                                                )
                                                embedding_logger.info(
                                                    f"Successfully created embedding after token refresh"
                                                )
                                                return embedding
                                            else:
                                                embedding_logger.warning(
                                                    "No embedding data found in Databricks response after retry"
                                                )
                                                return None
                                        else:
                                            error_text = await retry_response.text()
                                            embedding_logger.error(
                                                f"Databricks embedding API error after retry {retry_response.status}: {error_text}"
                                            )
                                            return None
                                else:
                                    embedding_logger.error(
                                        f"Failed to refresh token: {error}"
                                    )
                                    return None
                            except Exception as refresh_error:
                                embedding_logger.error(
                                    f"Error refreshing token: {refresh_error}"
                                )
                                return None
                        else:
                            error_text = await response.text()
                            embedding_logger.error(
                                f"Databricks embedding API error {response.status}: {error_text}"
                            )
                            return None

            except Exception as e:
                embedding_logger.error(
                    f"Error calling Databricks embedding API directly: {str(e)}"
                )
                return None

        elif provider == "ollama":
            # Use Ollama for embeddings via direct HTTP
            import aiohttp

            api_base = os.getenv("OLLAMA_API_BASE", "http://localhost:11434")
            # Strip ollama/ prefix if present for the raw API call
            raw_model = embedding_model.removeprefix("ollama/")

            timeout_val = aiohttp.ClientTimeout(
                total=float(os.getenv("EMBEDDING_TIMEOUT_SECONDS", "60"))
            )
            from src.utils.aiohttp_session import shared_client_session

            async with shared_client_session() as http_session:
                async with http_session.post(
                    f"{api_base}/api/embed",
                    json={"model": raw_model, "input": text},
                    timeout=timeout_val,
                ) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        embedding_logger.error(
                            f"Ollama embedding API error {resp.status}: {error_text}"
                        )
                        return None
                    result = await resp.json()
                    embeddings_list = result.get("embeddings", [])
                    if embeddings_list:
                        embedding = embeddings_list[0]
                        embedding_logger.info(
                            f"Successfully created embedding with {len(embedding)} dimensions using Ollama"
                        )
                        if provider in _embedding_failures:
                            _embedding_failures[provider] = {
                                "count": 0,
                                "last_failure": 0,
                            }
                        return embedding
                    embedding_logger.warning("No embedding data in Ollama response")
                    return None

        elif provider == "google":
            # Use Google AI for embeddings via direct HTTP
            import aiohttp

            group_id = _get_group_id_from_context()
            api_key = await ApiKeysService.get_provider_api_key(
                ModelProvider.GEMINI, group_id=group_id
            )

            if not api_key:
                embedding_logger.warning(
                    "No Google API key found for creating embeddings"
                )
                return None

            # Strip gemini/ prefix if present
            raw_model = embedding_model.removeprefix("gemini/")
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{raw_model}:embedContent?key={api_key}"
            payload = {
                "model": f"models/{raw_model}",
                "content": {"parts": [{"text": text}]},
            }

            timeout_val = aiohttp.ClientTimeout(
                total=float(os.getenv("EMBEDDING_TIMEOUT_SECONDS", "60"))
            )
            from src.utils.aiohttp_session import shared_client_session

            async with shared_client_session() as http_session:
                async with http_session.post(
                    url, json=payload, timeout=timeout_val
                ) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        embedding_logger.error(
                            f"Google embedding API error {resp.status}: {error_text}"
                        )
                        return None
                    result = await resp.json()
                    embedding_data = result.get("embedding", {})
                    values = embedding_data.get("values", [])
                    if values:
                        embedding_logger.info(
                            f"Successfully created embedding with {len(values)} dimensions using Google"
                        )
                        if provider in _embedding_failures:
                            _embedding_failures[provider] = {
                                "count": 0,
                                "last_failure": 0,
                            }
                        return values
                    embedding_logger.warning("No embedding data in Google response")
                    return None

        else:
            # Default to OpenAI for embeddings via direct HTTP
            import aiohttp

            group_id = _get_group_id_from_context()
            api_key = await ApiKeysService.get_provider_api_key(
                ModelProvider.OPENAI, group_id=group_id
            )

            if not api_key:
                embedding_logger.warning(
                    f"No OpenAI API key found for creating embeddings with group_id: {group_id}"
                )
                return None

            url = "https://api.openai.com/v1/embeddings"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
            payload = {"model": embedding_model, "input": text}

            timeout_val = aiohttp.ClientTimeout(
                total=float(os.getenv("EMBEDDING_TIMEOUT_SECONDS", "60"))
            )
            from src.utils.aiohttp_session import shared_client_session

            async with shared_client_session() as http_session:
                async with http_session.post(
                    url, headers=headers, json=payload, timeout=timeout_val
                ) as resp:
                    if resp.status != 200:
                        error_text = await resp.text()
                        embedding_logger.error(
                            f"OpenAI embedding API error {resp.status}: {error_text}"
                        )
                        return None
                    result = await resp.json()
                    data = result.get("data", [])
                    if data:
                        embedding = data[0].get("embedding", [])
                        embedding_logger.info(
                            f"Successfully created embedding with {len(embedding)} dimensions using OpenAI"
                        )
                        if provider in _embedding_failures:
                            _embedding_failures[provider] = {
                                "count": 0,
                                "last_failure": 0,
                            }
                        return embedding
                    embedding_logger.warning("No embedding data in OpenAI response")
                    return None

    except Exception as e:
        embedding_logger.error(f"Error creating embedding: {str(e)}")
        # Track failure for circuit breaker
        if provider not in _embedding_failures:
            _embedding_failures[provider] = {"count": 0, "last_failure": 0}
        _embedding_failures[provider]["count"] += 1
        _embedding_failures[provider]["last_failure"] = time.time()

        # Log circuit breaker status
        failure_count = _embedding_failures[provider]["count"]
        if failure_count >= _EMBEDDING_FAILURE_THRESHOLD:
            embedding_logger.error(
                f"Circuit breaker tripped for {provider} embeddings after {failure_count} failures"
            )

        return None

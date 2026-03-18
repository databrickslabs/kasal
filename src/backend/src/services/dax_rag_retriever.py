"""
DAX RAG Retriever — few-shot Q→DAX example retrieval.

Retrieves similar past Q→DAX pairs from a Databricks Vector Search index
and stores successful pairs for future retrieval.

Fails open: returns [] on any error (index not configured, auth failure).
Opt-in via 'dax_rag_enabled' config key.

Author: Kasal Team
Date: 2026
"""

import hashlib
import json
import logging
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


class DaxRagRetriever:
    """Retrieve and store Q→DAX few-shot examples via Databricks Vector Search."""

    async def retrieve(
        self,
        question: str,
        config: Dict[str, Any],
        n: int = 3,
        threshold: float = 0.75,
    ) -> List[Dict[str, Any]]:
        """Retrieve similar Q→DAX examples from the vector search index.

        Args:
            question: User natural language question.
            config: Tool config dict. Must contain:
                - llm_workspace_url: Databricks workspace URL.
                - llm_token: Bearer token for authentication.
                - dax_rag_index_name: Full VS index name (catalog.schema.index).
                - dax_rag_endpoint_name: VS endpoint name.
                Optional:
                - dax_rag_enabled (bool, default False): Must be True to activate.
            n: Number of examples to retrieve.
            threshold: Minimum similarity score (0–1) to include a result.

        Returns:
            List of dicts with keys: question, dax, score.
            Empty list on any failure or when RAG is disabled.
        """
        if not config.get("dax_rag_enabled", False):
            return []

        workspace_url = (config.get("llm_workspace_url") or "").rstrip("/")
        token = config.get("llm_token", "")
        index_name = config.get("dax_rag_index_name", "")
        endpoint_name = config.get("dax_rag_endpoint_name", "")

        if not all([workspace_url, token, index_name, endpoint_name]):
            logger.debug("[DaxRagRetriever] Missing config — skipping retrieval")
            return []

        url = f"{workspace_url}/api/2.0/vector-search/indexes/{index_name}/query"
        payload = {
            "endpoint_name": endpoint_name,
            "query_text": question,
            "num_results": n,
            "columns": ["question", "dax"],
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()
        except Exception as exc:
            logger.debug(f"[DaxRagRetriever] Retrieval failed (fail-open): {exc}")
            return []

        results = []
        try:
            result_data = data.get("result", {})
            column_names = result_data.get("columns", [])
            rows = result_data.get("data_array", [])

            # Build column index
            try:
                q_idx = column_names.index("question")
                dax_idx = column_names.index("dax")
                score_idx = len(column_names)  # score appended as last column by VS
            except ValueError:
                logger.debug("[DaxRagRetriever] Unexpected column schema in VS response")
                return []

            for row in rows:
                if len(row) < 2:
                    continue
                score_val = float(row[score_idx]) if score_idx < len(row) else 0.0
                if score_val < threshold:
                    continue
                results.append({
                    "question": str(row[q_idx]),
                    "dax": str(row[dax_idx]),
                    "score": round(score_val, 4),
                })
        except Exception as exc:
            logger.debug(f"[DaxRagRetriever] Response parse error (fail-open): {exc}")
            return []

        logger.info(
            f"[DaxRagRetriever] Retrieved {len(results)}/{n} examples "
            f"(threshold={threshold}) for: '{question[:80]}'"
        )
        return results

    async def store(
        self,
        question: str,
        dax: str,
        config: Dict[str, Any],
        dataset_id: Optional[str] = None,
    ) -> None:
        """Store a successful Q→DAX pair in the vector search index.

        Fails silently on any error.

        Args:
            question: User question that produced this DAX.
            dax: The validated, successfully-executed DAX query.
            config: Tool config dict (same keys as retrieve()).
            dataset_id: Optional dataset ID for namespacing.
        """
        if not config.get("dax_rag_enabled", False):
            return

        workspace_url = (config.get("llm_workspace_url") or "").rstrip("/")
        token = config.get("llm_token", "")
        index_name = config.get("dax_rag_index_name", "")
        endpoint_name = config.get("dax_rag_endpoint_name", "")

        if not all([workspace_url, token, index_name, endpoint_name]):
            return

        # Deterministic ID from question hash
        record_id = hashlib.sha256(
            f"{dataset_id or ''}:{question}".encode()
        ).hexdigest()[:32]

        url = f"{workspace_url}/api/2.0/vector-search/indexes/{index_name}/upsert"
        payload = {
            "inputs_json": json.dumps([
                {
                    "id": record_id,
                    "question": question,
                    "dax": dax,
                    "dataset_id": dataset_id or "",
                }
            ])
        }

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
                response.raise_for_status()
            logger.info(
                f"[DaxRagRetriever] Stored Q→DAX pair (id={record_id}) "
                f"for: '{question[:60]}'"
            )
        except Exception as exc:
            logger.debug(f"[DaxRagRetriever] Store failed (fail-open): {exc}")
